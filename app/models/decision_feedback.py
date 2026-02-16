"""Decision feedback & outcome tracking model."""
from sqlalchemy import Column, Integer, String, Float, DateTime, Numeric, Text
from datetime import datetime

from app.models.base import Base


class DecisionSnapshot(Base):
    """
    Captures each campaign decision + context at decision time.
    Outcomes are filled in later (7d/30d) by the feedback service.
    """
    __tablename__ = "decision_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(String, index=True, nullable=False)
    campaign_name = Column(String)
    strategy_type = Column(String)

    # Decision at time of snapshot
    decided_at = Column(DateTime, index=True, default=datetime.utcnow)
    action = Column(String)                     # scale, reduce, investigate, fix_landing_page, etc.
    confidence = Column(String)                 # high, medium, low
    decision_score = Column(Integer, nullable=True)
    primary_cause = Column(String, nullable=True)
    why_now = Column(Text, nullable=True)

    # Context at decision time
    true_roas = Column(Float, nullable=True)
    total_spend = Column(Numeric(10, 2), nullable=True)
    true_profit = Column(Numeric(10, 2), nullable=True)

    # Outcomes (filled in by score_outcomes)
    outcome_7d_roas = Column(Float, nullable=True)
    outcome_7d_profit = Column(Numeric(10, 2), nullable=True)
    outcome_30d_roas = Column(Float, nullable=True)
    outcome_30d_profit = Column(Numeric(10, 2), nullable=True)

    # Verdict
    outcome_verdict = Column(String, nullable=True)  # correct, wrong, neutral
    outcome_scored_at = Column(DateTime, nullable=True)

    # Human feedback
    user_action = Column(String, nullable=True)       # accepted, rejected, modified
    user_override_to = Column(String, nullable=True)
    user_feedback_at = Column(DateTime, nullable=True)
