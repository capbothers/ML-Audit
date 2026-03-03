"""Authentication service — password hashing, sessions, user management"""
import logging
import secrets
from datetime import datetime, timedelta

from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.models.user import User, UserSession, UserInvite
from app.config import get_settings
from app.services.authorization_service import (
    ROLE_ADMIN,
    ROLE_USER,
    normalize_dashboard_access,
    normalize_role,
)

logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class InviteEmailError(Exception):
    """Raised when invite email cannot be sent."""


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def authenticate(db: Session, email: str, password: str) -> User | None:
    """Verify credentials and return user, or None."""
    user = db.query(User).filter(User.email == email, User.is_active == True).first()
    if not user or not verify_password(password, user.password_hash):
        return None
    user.last_login = datetime.utcnow()
    db.commit()
    return user


def create_session(db: Session, user_id: int) -> str:
    """Create a new session token for the user."""
    settings = get_settings()
    token = secrets.token_hex(32)
    session = UserSession(
        user_id=user_id,
        token=token,
        expires_at=datetime.utcnow() + timedelta(hours=settings.session_duration_hours),
    )
    db.add(session)
    db.commit()
    return token


def validate_session(db: Session, token: str) -> User | None:
    """Return the user for a valid, non-expired session token."""
    session = (
        db.query(UserSession)
        .filter(UserSession.token == token, UserSession.expires_at > datetime.utcnow())
        .first()
    )
    if not session:
        return None
    user = db.query(User).filter(User.id == session.user_id, User.is_active == True).first()
    return user


def delete_session(db: Session, token: str) -> None:
    """Remove a session (logout)."""
    db.query(UserSession).filter(UserSession.token == token).delete()
    db.commit()


def cleanup_expired(db: Session) -> int:
    """Delete expired sessions. Returns count removed."""
    count = db.query(UserSession).filter(UserSession.expires_at <= datetime.utcnow()).delete()
    db.commit()
    return count


def create_user(
    db: Session,
    email: str,
    password: str,
    display_name: str | None = None,
    role: str = ROLE_USER,
    dashboard_access: list[str] | None = None,
) -> User:
    """Create a new user account."""
    normalized_role = normalize_role(role)
    normalized_dashboards = normalize_dashboard_access(dashboard_access)
    user = User(
        email=email.lower().strip(),
        password_hash=hash_password(password),
        display_name=display_name,
        role=normalized_role,
        dashboard_access=normalized_dashboards,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def seed_initial_user(db: Session) -> None:
    """Create or repair admin access from env vars.

    Safety net: if no admin exists at all, promote the oldest active user
    even if env vars are not configured — prevents total lockout after
    the role column is added to an existing deployment.
    """
    from app.utils.logger import log
    settings = get_settings()
    has_env = bool(settings.initial_admin_email and settings.initial_admin_password)

    # First run: no users yet, create configured admin (requires env vars).
    if has_env and not db.query(User).first():
        create_user(
            db,
            settings.initial_admin_email,
            settings.initial_admin_password,
            "Admin",
            role=ROLE_ADMIN,
            dashboard_access=None,
        )
        log.info(f"Seeded initial admin user: {settings.initial_admin_email}")
        return

    # Existing deployments: ensure at least one active admin exists.
    has_admin = (
        db.query(User)
        .filter(User.is_active == True, User.role == ROLE_ADMIN)  # noqa: E712
        .first()
    )
    if has_admin:
        return

    # No admin exists — promote someone to prevent lockout.
    preferred = None
    if has_env:
        preferred = (
            db.query(User)
            .filter(User.email == settings.initial_admin_email.lower().strip(), User.is_active == True)  # noqa: E712
            .first()
        )
    fallback = db.query(User).filter(User.is_active == True).order_by(User.created_at.asc()).first()  # noqa: E712
    promoted = preferred or fallback
    if promoted:
        promoted.role = ROLE_ADMIN
        promoted.dashboard_access = None
        db.commit()
        log.warning(f"No admin found — promoted {promoted.email} to admin to prevent lockout")


# ── Invite system ────────────────────────────────────────────

def create_invite(
    db: Session,
    email: str,
    invited_by: int,
    role: str = ROLE_USER,
    dashboard_access: list[str] | None = None,
) -> UserInvite:
    """Create a new invite token for the given email (48h expiry)."""
    normalized_role = normalize_role(role)
    normalized_dashboards = normalize_dashboard_access(dashboard_access)
    token = secrets.token_urlsafe(32)
    invite = UserInvite(
        email=email.lower().strip(),
        token=token,
        invited_by=invited_by,
        role=normalized_role,
        dashboard_access=normalized_dashboards,
        expires_at=datetime.utcnow() + timedelta(hours=48),
    )
    db.add(invite)
    db.commit()
    db.refresh(invite)
    return invite


def validate_invite(db: Session, token: str) -> UserInvite | None:
    """Return a valid, unused, non-expired invite — or None."""
    invite = (
        db.query(UserInvite)
        .filter(
            UserInvite.token == token,
            UserInvite.expires_at > datetime.utcnow(),
            UserInvite.accepted_at.is_(None),
        )
        .first()
    )
    return invite


def accept_invite(db: Session, token: str, password: str) -> User:
    """Accept an invite: create the user account and mark invite used."""
    invite = validate_invite(db, token)
    if not invite:
        raise ValueError("Invite is invalid, expired, or already used")

    # Check if email already registered
    existing = db.query(User).filter(User.email == invite.email).first()
    if existing:
        raise ValueError("An account with this email already exists")

    user = User(
        email=invite.email,
        password_hash=hash_password(password),
        role=invite.role or ROLE_USER,
        dashboard_access=invite.dashboard_access,
    )
    db.add(user)
    invite.accepted_at = datetime.utcnow()
    db.commit()
    db.refresh(user)
    return user


def send_invite_email(email: str, token: str) -> bool:
    """Send invite email and raise InviteEmailError on failure."""
    settings = get_settings()
    if not settings.resend_api_key:
        raise InviteEmailError("RESEND_API_KEY is missing")

    try:
        import resend
    except Exception as exc:
        raise InviteEmailError("Resend client is not available in runtime") from exc

    resend.api_key = settings.resend_api_key

    invite_url = f"{settings.effective_base_url.rstrip('/')}/auth/accept-invite?token={token}"

    try:
        resend.Emails.send({
            "from": settings.invite_from_email,
            "to": [email],
            "subject": "You've been invited to Cass Brothers Intelligence",
            "html": (
                f"<div style='font-family:sans-serif;max-width:480px;margin:0 auto;padding:40px 20px'>"
                f"<h2 style='color:#1b1b1b'>You're invited</h2>"
                f"<p style='color:#555;line-height:1.6'>You've been invited to the "
                f"<strong>Cass Brothers Intelligence Platform</strong>.</p>"
                f"<p style='color:#555;line-height:1.6'>Click the button below to set your password and activate your account. "
                f"This link expires in 48 hours.</p>"
                f"<a href='{invite_url}' style='display:inline-block;background:#1b1b1b;color:#f7f1e8;"
                f"padding:14px 28px;border-radius:10px;text-decoration:none;font-weight:600;"
                f"margin:20px 0'>Set your password</a>"
                f"<p style='color:#999;font-size:13px;margin-top:30px'>"
                f"If the button doesn't work, copy this link:<br>"
                f"<a href='{invite_url}' style='color:#c49a4a'>{invite_url}</a></p>"
                f"</div>"
            ),
        })
        logger.info(f"Invite email sent to {email}")
        return True
    except Exception as exc:
        logger.error(f"Failed to send invite email to {email}: {exc}")
        raise InviteEmailError(str(exc)) from exc
