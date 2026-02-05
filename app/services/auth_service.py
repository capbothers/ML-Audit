"""Authentication service — password hashing, sessions, user management"""
import secrets
from datetime import datetime, timedelta

from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.models.user import User, UserSession
from app.config import get_settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


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


def create_user(db: Session, email: str, password: str, display_name: str | None = None) -> User:
    """Create a new user account."""
    user = User(
        email=email.lower().strip(),
        password_hash=hash_password(password),
        display_name=display_name,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def seed_initial_user(db: Session) -> None:
    """Create the first admin user from env vars if no users exist."""
    settings = get_settings()
    if not settings.initial_admin_email or not settings.initial_admin_password:
        return
    # Skip if any users already exist
    if db.query(User).first():
        return
    create_user(db, settings.initial_admin_email, settings.initial_admin_password, "Admin")
    from app.utils.logger import log
    log.info(f"Seeded initial admin user: {settings.initial_admin_email}")
