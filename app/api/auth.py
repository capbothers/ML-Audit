"""Authentication API — login, logout, user management."""
import os

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.models.base import get_db
from app.models.user import User
from app.services import auth_service

router = APIRouter(prefix="/auth", tags=["auth"])

static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")


# ── Schemas ──────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: str
    password: str


class CreateUserRequest(BaseModel):
    email: str
    password: str
    display_name: str | None = None


class UserOut(BaseModel):
    id: int
    email: str
    display_name: str | None
    is_active: bool
    created_at: str | None
    last_login: str | None


def _user_out(u: User) -> dict:
    return {
        "id": u.id,
        "email": u.email,
        "display_name": u.display_name,
        "is_active": u.is_active,
        "created_at": u.created_at.isoformat() if u.created_at else None,
        "last_login": u.last_login.isoformat() if u.last_login else None,
    }


# ── Login page ───────────────────────────────────────────

@router.get("/login")
async def login_page():
    """Serve the login HTML page."""
    return FileResponse(os.path.join(static_dir, "login.html"))


# ── Auth endpoints ───────────────────────────────────────

@router.post("/login")
async def login(body: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate and return a session cookie."""
    user = auth_service.authenticate(db, body.email, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = auth_service.create_session(db, user.id)
    response = JSONResponse(content={"success": True, "user": _user_out(user)})
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 72,  # 72 hours
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


# ── User management ─────────────────────────────────────

@router.get("/users")
async def list_users(db: Session = Depends(get_db)):
    """List all users."""
    users = db.query(User).order_by(User.created_at).all()
    return [_user_out(u) for u in users]


@router.post("/users", status_code=201)
async def create_user(body: CreateUserRequest, db: Session = Depends(get_db)):
    """Create a new user account."""
    existing = db.query(User).filter(User.email == body.email.lower().strip()).first()
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    user = auth_service.create_user(db, body.email, body.password, body.display_name)
    return _user_out(user)


@router.delete("/users/{user_id}")
async def deactivate_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    """Deactivate a user account (cannot deactivate yourself)."""
    current_user = getattr(request.state, "user", None)
    if current_user and current_user.id == user_id:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = False
    db.commit()
    return {"success": True, "message": f"User {user.email} deactivated"}
