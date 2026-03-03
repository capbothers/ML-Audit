"""Authentication API - login, logout, user management, invites."""
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models.base import get_db
from app.models.user import User
from app.services import auth_service
from app.services.authorization_service import (
    ALL_DASHBOARD_KEYS,
    DASHBOARDS,
    ROLE_ADMIN,
    ROLE_USER,
    default_dashboard_for_user,
    normalize_dashboard_access,
    normalize_role,
    serialize_dashboard_access,
    user_is_admin,
)

router = APIRouter(prefix="/auth", tags=["auth"])

static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")


def _require_authenticated(request: Request) -> User:
    """Dependency: raise 401 if no authenticated user on request."""
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def _require_admin(current_user: User = Depends(_require_authenticated)) -> User:
    """Dependency: require admin role."""
    if not user_is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# Schemas

class LoginRequest(BaseModel):
    email: str
    password: str


class CreateUserRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None
    role: str = ROLE_USER
    dashboard_access: list[str] | None = None


class UserOut(BaseModel):
    id: int
    email: str
    display_name: str | None
    is_active: bool
    role: str
    dashboard_access: list[str] | None
    default_dashboard: str
    created_at: str | None
    last_login: str | None


class InviteRequest(BaseModel):
    email: str
    role: str = ROLE_USER
    dashboard_access: list[str] | None = None


class AcceptInviteRequest(BaseModel):
    token: str
    password: str


class UpdateUserPermissionsRequest(BaseModel):
    role: str | None = None
    dashboard_access: list[str] | None = None


def _user_out(u: User) -> dict:
    return {
        "id": u.id,
        "email": u.email,
        "display_name": u.display_name,
        "is_active": u.is_active,
        "role": (u.role or ROLE_USER),
        "dashboard_access": serialize_dashboard_access(u.dashboard_access),
        "default_dashboard": default_dashboard_for_user(u),
        "created_at": u.created_at.isoformat() if u.created_at else None,
        "last_login": u.last_login.isoformat() if u.last_login else None,
    }


# Login page

@router.get("/login")
async def login_page():
    """Serve the login HTML page."""
    return FileResponse(os.path.join(static_dir, "login.html"))


# Auth endpoints

@router.post("/login")
async def login(body: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate and return a session cookie."""
    user = auth_service.authenticate(db, body.email, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = auth_service.create_session(db, user.id)
    response = JSONResponse(content={"success": True, "user": _user_out(user)})
    is_prod = get_settings().environment != "development"
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=is_prod,
        samesite="lax",
        max_age=60 * 60 * 72,
        path="/",
    )
    return response


@router.post("/logout")
async def logout(request: Request, db: Session = Depends(get_db)):
    """Clear session and cookie."""
    token = request.cookies.get("session_token")
    if token:
        auth_service.delete_session(db, token)
    response = JSONResponse(content={"success": True})
    response.delete_cookie("session_token", path="/")
    return response


@router.get("/me")
async def me(request: Request):
    """Return current authenticated user."""
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return _user_out(user)


# User management

@router.get("/users")
async def list_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(_require_admin),
):
    """List all users (admin only)."""
    users = db.query(User).order_by(User.created_at).all()
    return [_user_out(u) for u in users]


@router.post("/users", status_code=201)
async def create_user(
    body: CreateUserRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(_require_admin),
):
    """Create a new user account (admin only)."""
    existing = db.query(User).filter(User.email == body.email.lower().strip()).first()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    try:
        role = normalize_role(body.role)
        normalized_dashboards = normalize_dashboard_access(body.dashboard_access)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    user = auth_service.create_user(
        db,
        body.email,
        body.password,
        body.display_name,
        role=role,
        dashboard_access=None if normalized_dashboards is None else normalized_dashboards.split(","),
    )
    return _user_out(user)


@router.patch("/users/{user_id}/permissions")
async def update_user_permissions(
    user_id: int,
    body: UpdateUserPermissionsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(_require_admin),
):
    """Update user role and dashboard access (admin only)."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if "role" in body.model_fields_set:
        if body.role is None:
            raise HTTPException(status_code=400, detail="Role cannot be null")
        try:
            role = normalize_role(body.role)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if user.id == current_user.id and role != ROLE_ADMIN:
            raise HTTPException(status_code=400, detail="Cannot remove your own admin role")
        user.role = role

    if "dashboard_access" in body.model_fields_set:
        if body.dashboard_access is None:
            user.dashboard_access = None
        else:
            try:
                user.dashboard_access = normalize_dashboard_access(body.dashboard_access)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

    if user.role != ROLE_ADMIN:
        active_admins = (
            db.query(User)
            .filter(User.is_active == True, User.role == ROLE_ADMIN)  # noqa: E712
            .count()
        )
        if active_admins == 0:
            raise HTTPException(status_code=400, detail="At least one active admin is required")

    db.commit()
    db.refresh(user)
    return {"success": True, "user": _user_out(user)}


@router.delete("/users/{user_id}")
async def deactivate_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(_require_admin),
):
    """Deactivate a user account (cannot deactivate yourself/last admin)."""
    if current_user.id == user_id:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user_is_admin(user):
        active_admins = (
            db.query(User)
            .filter(User.is_active == True, User.role == ROLE_ADMIN)  # noqa: E712
            .count()
        )
        if active_admins <= 1:
            raise HTTPException(status_code=400, detail="Cannot deactivate the last active admin")

    user.is_active = False
    db.commit()
    return {"success": True, "message": f"User {user.email} deactivated"}


@router.get("/dashboards")
async def list_dashboards(current_user: User = Depends(_require_admin)):
    """List supported dashboard keys for permission management."""
    return {
        "dashboards": [
            {"key": key, "label": DASHBOARDS[key].label}
            for key in ALL_DASHBOARD_KEYS
        ]
    }


# Invite system

@router.post("/invite")
async def send_invite(
    body: InviteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(_require_admin),
):
    """Send an invite email to a new user (admin only)."""
    email = body.email.lower().strip()

    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(status_code=409, detail="A user with this email already exists")

    try:
        role = normalize_role(body.role)
        normalized_dashboards = normalize_dashboard_access(body.dashboard_access)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    invite = auth_service.create_invite(
        db,
        email,
        current_user.id,
        role=role,
        dashboard_access=None if normalized_dashboards is None else normalized_dashboards.split(","),
    )
    try:
        auth_service.send_invite_email(email, invite.token)
    except auth_service.InviteEmailError as exc:
        raise HTTPException(status_code=500, detail=f"Failed to send invite email: {exc}")

    return {"success": True, "message": f"Invite sent to {email}"}


@router.get("/invite-config")
async def get_invite_config_status(current_user: User = Depends(_require_admin)):
    """Safe runtime diagnostics for invite delivery configuration."""
    settings = get_settings()
    from_email = (settings.invite_from_email or "").strip()
    domain = from_email.split("@")[-1] if "@" in from_email else ""
    return {
        "has_resend_api_key": bool(settings.resend_api_key),
        "invite_from_email": from_email,
        "invite_sender_domain": domain or None,
        "app_base_url": settings.app_base_url,
        "notes": [
            "If sender domain is not 'resend.dev', it must be verified in Resend.",
            "If has_resend_api_key is false, set RESEND_API_KEY in your deployment environment.",
        ],
    }


@router.get("/accept-invite")
async def accept_invite_page(token: str = "", db: Session = Depends(get_db)):
    """Serve the set-password page if token is valid, or error page."""
    if not token:
        return FileResponse(os.path.join(static_dir, "invite_expired.html"))

    invite = auth_service.validate_invite(db, token)
    if not invite:
        return FileResponse(os.path.join(static_dir, "invite_expired.html"))

    return FileResponse(os.path.join(static_dir, "accept_invite.html"))


@router.post("/accept-invite")
async def accept_invite(body: AcceptInviteRequest, db: Session = Depends(get_db)):
    """Accept an invite: set password and create the user account."""
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    try:
        user = auth_service.accept_invite(db, body.token, body.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"success": True, "message": "Account created. You can now sign in.", "email": user.email}
