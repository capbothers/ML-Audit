"""
Site Health & Real User Monitoring API

Privacy-safe endpoints for client-side error and performance tracking.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pydantic import BaseModel, Field, validator
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models.base import get_db
from app.models.site_health import SiteHealthEvent
from app.utils.logger import log

router = APIRouter(prefix="/site-health", tags=["site-health"])

# ── Web Vitals thresholds (good, poor) ───────────────────────────────
_WV_THRESHOLDS = {
    "LCP":  (2500, 4000),   # ms
    "CLS":  (0.1, 0.25),    # score
    "INP":  (200, 500),     # ms
    "TTFB": (800, 1800),    # ms
}


def _rate(metric: str, value: float) -> str:
    good, poor = _WV_THRESHOLDS.get(metric, (0, 0))
    if value <= good:
        return "good"
    if value <= poor:
        return "needs-improvement"
    return "poor"


def _p75(values: list) -> Optional[float]:
    """Compute the 75th-percentile of a sorted list (Python-side, SQLite-safe)."""
    if not values:
        return None
    s = sorted(values)
    idx = int(len(s) * 0.75)
    return s[min(idx, len(s) - 1)]


# ── Request models ───────────────────────────────────────────────────

class EventPayload(BaseModel):
    event_type: str
    page_url: str
    session_id: str
    device_type: str = "desktop"
    client_timestamp: datetime

    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None
    user_agent: Optional[str] = None

    # Error
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    error_stack: Optional[str] = None
    error_source_file: Optional[str] = None
    error_line_number: Optional[int] = None
    error_column_number: Optional[int] = None
    is_unhandled_rejection: bool = False

    # Web Vitals
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    metric_rating: Optional[str] = None
    metric_navigation_type: Optional[str] = None

    # Slow resource
    resource_url: Optional[str] = None
    resource_type: Optional[str] = None
    resource_duration: Optional[float] = None
    resource_transfer_size: Optional[int] = None

    # Long task
    task_duration: Optional[float] = None
    task_attribution: Optional[str] = None

    @validator("event_type")
    def _valid_type(cls, v):
        allowed = {"error", "web_vital", "slow_resource", "long_task"}
        if v not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return v


class TrackRequest(BaseModel):
    events: List[EventPayload] = Field(..., max_items=50)


# ── Endpoints ────────────────────────────────────────────────────────

@router.post("/track")
async def track_events(body: TrackRequest, db: Session = Depends(get_db)):
    """
    Ingest site health events from the client tracker.

    Accepts a batch of up to 50 events per request.
    Privacy-safe: no PII, random session IDs only.
    """
    try:
        for ev in body.events:
            page_path = urlparse(ev.page_url).path or "/"
            db.add(SiteHealthEvent(
                event_type=ev.event_type,
                page_url=ev.page_url,
                page_path=page_path,
                session_id=ev.session_id,
                device_type=ev.device_type,
                client_timestamp=ev.client_timestamp,
                viewport_width=ev.viewport_width,
                viewport_height=ev.viewport_height,
                user_agent=ev.user_agent[:500] if ev.user_agent else None,
                error_message=ev.error_message,
                error_type=ev.error_type,
                error_stack=ev.error_stack[:4000] if ev.error_stack else None,
                error_source_file=ev.error_source_file,
                error_line_number=ev.error_line_number,
                error_column_number=ev.error_column_number,
                is_unhandled_rejection=ev.is_unhandled_rejection,
                metric_name=ev.metric_name,
                metric_value=ev.metric_value,
                metric_rating=ev.metric_rating,
                metric_navigation_type=ev.metric_navigation_type,
                resource_url=ev.resource_url,
                resource_type=ev.resource_type,
                resource_duration=ev.resource_duration,
                resource_transfer_size=ev.resource_transfer_size,
                task_duration=ev.task_duration,
                task_attribution=ev.task_attribution,
            ))
        db.commit()
        return {"status": "ok", "events_saved": len(body.events)}
    except Exception as e:
        db.rollback()
        log.error(f"Site-health track error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_summary(
    hours: int = Query(24, ge=1, le=720, description="Look-back window in hours"),
    db: Session = Depends(get_db),
):
    """
    Site health summary dashboard.

    Returns top error pages, top error types, Core Web Vitals (p75),
    slowest pages by LCP, and top slow resources.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # ── Total event counts ──
        total_counts = dict(
            db.query(
                SiteHealthEvent.event_type,
                func.count(SiteHealthEvent.id),
            )
            .filter(SiteHealthEvent.created_at >= cutoff)
            .group_by(SiteHealthEvent.event_type)
            .all()
        )

        # ── Top error pages ──
        error_pages = (
            db.query(
                SiteHealthEvent.page_path,
                func.count(SiteHealthEvent.id).label("error_count"),
                func.max(SiteHealthEvent.created_at).label("last_error"),
            )
            .filter(
                SiteHealthEvent.event_type == "error",
                SiteHealthEvent.created_at >= cutoff,
            )
            .group_by(SiteHealthEvent.page_path)
            .order_by(desc("error_count"))
            .limit(10)
            .all()
        )

        # ── Top error types ──
        error_types = (
            db.query(
                SiteHealthEvent.error_message,
                SiteHealthEvent.error_type,
                func.count(SiteHealthEvent.id).label("count"),
                func.max(SiteHealthEvent.created_at).label("last_seen"),
            )
            .filter(
                SiteHealthEvent.event_type == "error",
                SiteHealthEvent.created_at >= cutoff,
            )
            .group_by(SiteHealthEvent.error_message, SiteHealthEvent.error_type)
            .order_by(desc("count"))
            .limit(10)
            .all()
        )

        # ── Core Web Vitals (p75, computed in Python for SQLite) ──
        web_vitals = {}
        for metric in ("LCP", "CLS", "INP", "TTFB"):
            rows = (
                db.query(SiteHealthEvent.metric_value)
                .filter(
                    SiteHealthEvent.event_type == "web_vital",
                    SiteHealthEvent.metric_name == metric,
                    SiteHealthEvent.metric_value.isnot(None),
                    SiteHealthEvent.created_at >= cutoff,
                )
                .all()
            )
            if rows:
                vals = [r[0] for r in rows]
                p = _p75(vals)
                web_vitals[metric] = {
                    "p75": round(p, 2),
                    "rating": _rate(metric, p),
                    "sample_size": len(vals),
                }
            else:
                web_vitals[metric] = None

        # ── Slowest pages by LCP p75 ──
        lcp_rows = (
            db.query(
                SiteHealthEvent.page_path,
                SiteHealthEvent.metric_value,
            )
            .filter(
                SiteHealthEvent.event_type == "web_vital",
                SiteHealthEvent.metric_name == "LCP",
                SiteHealthEvent.metric_value.isnot(None),
                SiteHealthEvent.created_at >= cutoff,
            )
            .all()
        )
        # Group by page_path and compute p75 per page
        page_lcps: Dict[str, list] = {}
        for path, val in lcp_rows:
            page_lcps.setdefault(path, []).append(val)
        slowest_pages = sorted(
            [
                {
                    "page_path": path,
                    "lcp_p75": round(_p75(vals), 0),
                    "rating": _rate("LCP", _p75(vals)),
                    "sample_size": len(vals),
                }
                for path, vals in page_lcps.items()
            ],
            key=lambda x: x["lcp_p75"],
            reverse=True,
        )[:10]

        # ── Top slow resources ──
        slow_resources = (
            db.query(
                SiteHealthEvent.resource_url,
                SiteHealthEvent.resource_type,
                func.count(SiteHealthEvent.id).label("count"),
                func.avg(SiteHealthEvent.resource_duration).label("avg_duration"),
            )
            .filter(
                SiteHealthEvent.event_type == "slow_resource",
                SiteHealthEvent.created_at >= cutoff,
            )
            .group_by(SiteHealthEvent.resource_url, SiteHealthEvent.resource_type)
            .order_by(desc("count"))
            .limit(15)
            .all()
        )

        return {
            "period_hours": hours,
            "timestamp": datetime.utcnow().isoformat(),
            "total_events": total_counts,
            "error_pages": [
                {
                    "page_path": r.page_path,
                    "error_count": r.error_count,
                    "last_error": r.last_error.isoformat() if r.last_error else None,
                }
                for r in error_pages
            ],
            "error_types": [
                {
                    "error_message": (r.error_message or "Unknown")[:200],
                    "error_type": r.error_type,
                    "count": r.count,
                    "last_seen": r.last_seen.isoformat() if r.last_seen else None,
                }
                for r in error_types
            ],
            "core_web_vitals": web_vitals,
            "slowest_pages": slowest_pages,
            "slow_resources": [
                {
                    "resource_url": (r.resource_url or "")[:200],
                    "resource_type": r.resource_type,
                    "count": r.count,
                    "avg_duration_ms": round(r.avg_duration, 0) if r.avg_duration else 0,
                }
                for r in slow_resources
            ],
        }
    except Exception as e:
        log.error(f"Site-health summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
