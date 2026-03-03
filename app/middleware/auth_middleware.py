"""Authentication middleware — protects all routes except public paths."""
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

from app.models.base import SessionLocal
from app.services import auth_service
from app.services.authorization_service import (
    PAGE_PATH_TO_DASHBOARD,
    default_dashboard_for_user,
    required_dashboard_for_path,
    user_has_dashboard_access,
)

# Paths that never require authentication
PUBLIC_PREFIXES = (
    "/auth/login",
    "/auth/accept-invite",
    "/health",
    "/robots.txt",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/site-health/track",   # RUM telemetry from Shopify storefront (no cookie)
    "/sync",                # Sync endpoints — protected by Basic Auth only
)

# Dashboard (HTML) paths - unauthenticated users get redirected to login
DASHBOARD_PATHS = {"/", *PAGE_PATH_TO_DASHBOARD.keys()}


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

            dashboard_key = required_dashboard_for_path(path)
            if dashboard_key and not user_has_dashboard_access(user, dashboard_key):
                if path in PAGE_PATH_TO_DASHBOARD:
                    target = default_dashboard_for_user(user)
                    if target != path:
                        return RedirectResponse(url=target, status_code=302)
                return JSONResponse(
                    status_code=403,
                    content={"detail": "You do not have access to this dashboard"},
                )

            return await call_next(request)

        # Not authenticated — decide response type
        if path in DASHBOARD_PATHS:
            return RedirectResponse(url="/auth/login", status_code=302)

        # API endpoints return 401
        return JSONResponse(
            status_code=401,
            content={"detail": "Not authenticated"},
        )
