"""Security middleware — Basic Auth gate, anti-crawl headers, cache control."""
import base64
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.config import get_settings

# Paths exempt from Basic Auth
OPEN_PATHS = ("/health", "/robots.txt")


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        path = request.url.path

        # --- Basic Auth gate (skip for /health and /robots.txt) ---
        if settings.dash_user and settings.dash_pass:
            if not any(path.startswith(p) for p in OPEN_PATHS):
                if not self._check_basic_auth(request, settings):
                    return Response(
                        content="Unauthorized",
                        status_code=401,
                        headers={"WWW-Authenticate": 'Basic realm="ML-Audit"'},
                    )

        response: Response = await call_next(request)

        # --- Anti-crawl header on every response ---
        response.headers["X-Robots-Tag"] = "noindex, nofollow"

        # --- Cache-Control ---
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            # Dashboard HTML rarely changes; cache 10min for instant back/forward nav
            response.headers["Cache-Control"] = "private, max-age=600"
        elif "application/json" in content_type:
            # API data: browser may store but must revalidate each time
            response.headers["Cache-Control"] = "private, no-cache"

        return response

    @staticmethod
    def _check_basic_auth(request: Request, settings) -> bool:
        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
            user, password = decoded.split(":", 1)
        except Exception:
            return False
        user_ok = secrets.compare_digest(user, settings.dash_user)
        pass_ok = secrets.compare_digest(password, settings.dash_pass)
        return user_ok and pass_ok
