"""Authentication middleware — protects all routes except public paths."""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from app.models.base import SessionLocal
from app.services import auth_service

# Paths that never require authentication
PUBLIC_PREFIXES = (
    "/auth/login",
    "/auth/accept-invite",
    "/health",
    "/robots.txt",
    "/docs",
    "/openapi.json",
    "/redoc",
)

# Dashboard (HTML) paths — unauthenticated users get redirected to login
DASHBOARD_PATHS = {
    "/", "/dashboard", "/inventory", "/seo-dashboard", "/performance",
    "/pricing-intel", "/customer-intelligence", "/merchant-center-intel",
    "/strategic-intelligence", "/finance-dashboard", "/ads-intelligence",
    "/site-intelligence", "/brand-intelligence",
}


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Allow public paths through
        if any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)

        # Allow static assets through (CSS/JS/images needed by login page)
        if path.startswith("/static/"):
            return await call_next(request)

        # Check session cookie
        token = request.cookies.get("session_token")
        user = None
        if token:
            db = SessionLocal()
            try:
                user = auth_service.validate_session(db, token)
            finally:
                db.close()

        if user:
            # Attach user to request state for downstream use
            request.state.user = user
            return await call_next(request)

        # Not authenticated — decide response type
        if path in DASHBOARD_PATHS:
            return RedirectResponse(url="/auth/login", status_code=302)

        # API endpoints return 401
        return JSONResponse(
            status_code=401,
            content={"detail": "Not authenticated"},
        )
