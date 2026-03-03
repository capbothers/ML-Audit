"""Role and dashboard access helpers."""
from __future__ import annotations

from dataclasses import dataclass


ROLE_ADMIN = "admin"
ROLE_USER = "user"
VALID_ROLES = {ROLE_ADMIN, ROLE_USER}


@dataclass(frozen=True)
class DashboardDefinition:
    key: str
    label: str
    page_paths: tuple[str, ...]
    api_prefixes: tuple[str, ...]
    api_paths: tuple[str, ...] = ()


DASHBOARDS: dict[str, DashboardDefinition] = {
    "overview": DashboardDefinition(
        key="overview",
        label="Overview",
        page_paths=("/dashboard",),
        api_prefixes=("/monitor",),
    ),
    "inventory": DashboardDefinition(
        key="inventory",
        label="Inventory",
        page_paths=("/inventory",),
        api_prefixes=(),
        api_paths=("/ml/inventory-dashboard",),
    ),
    "seo": DashboardDefinition(
        key="seo",
        label="SEO",
        page_paths=("/seo-dashboard",),
        api_prefixes=("/seo",),
    ),
    "performance": DashboardDefinition(
        key="performance",
        label="Performance",
        page_paths=("/performance",),
        api_prefixes=("/performance",),
        api_paths=("/ml/forecast", "/ml/anomalies", "/ml/drivers", "/ml/tracking-health"),
    ),
    "pricing": DashboardDefinition(
        key="pricing",
        label="Pricing Intelligence",
        page_paths=("/pricing-intel",),
        api_prefixes=("/pricing",),
    ),
    "customer": DashboardDefinition(
        key="customer",
        label="Customer Intelligence",
        page_paths=("/customer-intelligence",),
        api_prefixes=("/customers",),
    ),
    "merchant_center": DashboardDefinition(
        key="merchant_center",
        label="Merchant Center",
        page_paths=("/merchant-center-intel",),
        api_prefixes=("/merchant-center",),
    ),
    "strategic": DashboardDefinition(
        key="strategic",
        label="Strategic Intelligence",
        page_paths=("/strategic-intelligence",),
        api_prefixes=("/intelligence",),
    ),
    "finance": DashboardDefinition(
        key="finance",
        label="Finance",
        page_paths=("/finance-dashboard",),
        api_prefixes=("/finance",),
    ),
    "ads": DashboardDefinition(
        key="ads",
        label="Ads Intelligence",
        page_paths=("/ads-intelligence",),
        api_prefixes=("/ads",),
    ),
    "site_intelligence": DashboardDefinition(
        key="site_intelligence",
        label="Site Intelligence",
        page_paths=("/site-intelligence",),
        api_prefixes=("/site-health", "/data-quality", "/redirects", "/code"),
    ),
    "brand_intelligence": DashboardDefinition(
        key="brand_intelligence",
        label="Brand Intelligence",
        page_paths=("/brand-intelligence",),
        api_prefixes=("/brands",),
    ),
    "stock_worthiness": DashboardDefinition(
        key="stock_worthiness",
        label="Stock Worthiness",
        page_paths=("/stock-worthiness",),
        api_prefixes=("/stock-worthiness",),
    ),
    "brand_portal": DashboardDefinition(
        key="brand_portal",
        label="Brand Portal",
        page_paths=("/brand-portal",),
        api_prefixes=("/brand-portal",),
    ),
    "caprice_upload": DashboardDefinition(
        key="caprice_upload",
        label="Caprice Upload",
        page_paths=("/caprice-upload",),
        api_prefixes=(),
    ),
    "admin": DashboardDefinition(
        key="admin",
        label="Admin",
        page_paths=("/admin",),
        api_prefixes=(),
    ),
}

ALL_DASHBOARD_KEYS = tuple(sorted(DASHBOARDS.keys()))
DEFAULT_PAGE_PATH = "/dashboard"

PAGE_PATH_TO_DASHBOARD: dict[str, str] = {}
API_PATH_TO_DASHBOARD: dict[str, str] = {}
API_PREFIX_TO_DASHBOARD: list[tuple[str, str]] = []
for _key, _defn in DASHBOARDS.items():
    for _path in _defn.page_paths:
        PAGE_PATH_TO_DASHBOARD[_path] = _key
    for _path in _defn.api_paths:
        API_PATH_TO_DASHBOARD[_path] = _key
    for _prefix in _defn.api_prefixes:
        API_PREFIX_TO_DASHBOARD.append((_prefix, _key))
API_PREFIX_TO_DASHBOARD.sort(key=lambda x: len(x[0]), reverse=True)


def normalize_role(value: str | None) -> str:
    role = (value or ROLE_USER).strip().lower()
    if role not in VALID_ROLES:
        raise ValueError(f"Invalid role '{value}'. Valid roles: {', '.join(sorted(VALID_ROLES))}")
    return role


def normalize_dashboard_access(dashboard_access: list[str] | None) -> str | None:
    """Store as comma-separated keys; None means unrestricted access."""
    if dashboard_access is None:
        return None

    cleaned = []
    seen = set()
    for item in dashboard_access:
        key = (item or "").strip()
        if not key:
            continue
        if key not in DASHBOARDS:
            raise ValueError(f"Unknown dashboard key '{key}'")
        if key not in seen:
            cleaned.append(key)
            seen.add(key)
    if not cleaned:
        return ""
    cleaned.sort()
    return ",".join(cleaned)


def parse_dashboard_access(raw: str | None) -> set[str]:
    if raw is None:
        return set(ALL_DASHBOARD_KEYS)
    if raw == "":
        return set()
    return {key for key in raw.split(",") if key in DASHBOARDS}


def serialize_dashboard_access(raw: str | None) -> list[str] | None:
    """Expose `None` as unrestricted access for APIs."""
    if raw is None:
        return None
    return sorted(parse_dashboard_access(raw))


def user_is_admin(user) -> bool:
    return normalize_role(getattr(user, "role", ROLE_USER)) == ROLE_ADMIN


def user_has_dashboard_access(user, dashboard_key: str) -> bool:
    if dashboard_key not in DASHBOARDS:
        return False
    if user_is_admin(user):
        return True
    allowed = parse_dashboard_access(getattr(user, "dashboard_access", None))
    return dashboard_key in allowed


def required_dashboard_for_path(path: str) -> str | None:
    if path in PAGE_PATH_TO_DASHBOARD:
        return PAGE_PATH_TO_DASHBOARD[path]
    if path in API_PATH_TO_DASHBOARD:
        return API_PATH_TO_DASHBOARD[path]
    for prefix, key in API_PREFIX_TO_DASHBOARD:
        if path.startswith(prefix):
            return key
    return None


def default_dashboard_for_user(user) -> str:
    if user_is_admin(user):
        return DEFAULT_PAGE_PATH

    allowed = parse_dashboard_access(getattr(user, "dashboard_access", None))
    if not allowed:
        return "/auth/login"

    for key in ALL_DASHBOARD_KEYS:
        if key in allowed:
            definition = DASHBOARDS[key]
            if definition.page_paths:
                return definition.page_paths[0]
    return DEFAULT_PAGE_PATH
