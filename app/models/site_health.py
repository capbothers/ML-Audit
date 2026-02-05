"""
Site Health & Real User Monitoring Models

Privacy-safe tracking of client-side errors and performance.
No PII: no cookies, emails, names, IPs, or customer IDs.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, Index
from sqlalchemy.sql import func
from app.models.base import Base


class SiteHealthEvent(Base):
    """
    Privacy-safe client-side event.

    event_type discriminator:
      error         – JS errors (window.onerror, unhandledrejection)
      web_vital     – Core Web Vitals (LCP, CLS, INP, TTFB)
      slow_resource – Resources loading > 2 s
      long_task     – Main-thread tasks > 50 ms
    """
    __tablename__ = "site_health_events"

    id = Column(Integer, primary_key=True, index=True)

    # Event classification
    event_type = Column(String, index=True, nullable=False)

    # Page context
    page_url = Column(String, nullable=False)
    page_path = Column(String, index=True)

    # Session context (random, non-PII)
    session_id = Column(String, index=True)

    # Device context
    device_type = Column(String, index=True)          # mobile / tablet / desktop
    user_agent = Column(Text, nullable=True)
    viewport_width = Column(Integer, nullable=True)
    viewport_height = Column(Integer, nullable=True)

    # ── Error fields ──
    error_message = Column(Text, nullable=True)
    error_type = Column(String, nullable=True)         # Error, TypeError, …
    error_stack = Column(Text, nullable=True)
    error_source_file = Column(String, nullable=True)
    error_line_number = Column(Integer, nullable=True)
    error_column_number = Column(Integer, nullable=True)
    is_unhandled_rejection = Column(Boolean, default=False)

    # ── Web Vitals fields ──
    metric_name = Column(String, index=True, nullable=True)   # LCP, CLS, INP, TTFB
    metric_value = Column(Float, nullable=True)
    metric_rating = Column(String, nullable=True)              # good / needs-improvement / poor
    metric_navigation_type = Column(String, nullable=True)     # navigate / reload / back_forward

    # ── Slow resource fields ──
    resource_url = Column(String, nullable=True)
    resource_type = Column(String, nullable=True)              # script, img, css, fetch
    resource_duration = Column(Float, nullable=True)           # ms
    resource_transfer_size = Column(Integer, nullable=True)    # bytes

    # ── Long task fields ──
    task_duration = Column(Float, nullable=True)               # ms
    task_attribution = Column(String, nullable=True)

    # Timestamps
    client_timestamp = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), index=True)

    __table_args__ = (
        Index("ix_sh_page_type_time", "page_path", "event_type", "created_at"),
        Index("ix_sh_error_msg_time", "event_type", "error_message", "created_at"),
        Index("ix_sh_metric_time", "metric_name", "created_at"),
    )
