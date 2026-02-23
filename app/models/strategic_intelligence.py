"""
Strategic Intelligence Brief Models

The crown jewel: stores daily and weekly intelligence briefs
with LLM-generated analysis, cross-module correlations,
and tracked recommendations.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, Numeric, Date, ForeignKey
from datetime import datetime

from app.models.base import Base


class StrategicBrief(Base):
    """
    Daily or weekly strategic intelligence brief.

    Stores the complete output of the 5-LLM-call pipeline:
    executive pulse, priorities, CRO analysis, issue triage,
    strategic insights, and growth playbook.
    """
    __tablename__ = "strategic_briefs"

    id = Column(Integer, primary_key=True, index=True)

    # Cadence
    cadence = Column(String, index=True, nullable=False)  # 'daily' | 'weekly'
    brief_date = Column(Date, index=True, nullable=False)
    week_start_date = Column(Date, nullable=True)
    week_end_date = Column(Date, nullable=True)

    # Data collection metadata
    modules_queried = Column(JSON, nullable=True)
    modules_succeeded = Column(JSON, nullable=True)
    modules_failed = Column(JSON, nullable=True)
    data_quality_score = Column(Integer, default=0)  # 0-100

    # Core KPI snapshot (for trend computation)
    kpi_snapshot = Column(JSON, nullable=True)

    # LLM-generated sections
    executive_pulse = Column(Text, nullable=True)
    health_status = Column(String, nullable=True)  # thriving|stable|at_risk|critical
    todays_priorities = Column(JSON, nullable=True)
    conversion_analysis = Column(Text, nullable=True)
    growth_playbook = Column(Text, nullable=True)
    cross_module_correlations = Column(JSON, nullable=True)
    issue_command_center = Column(JSON, nullable=True)
    ai_strategic_insights = Column(Text, nullable=True)
    whats_working = Column(JSON, nullable=True)
    watch_list = Column(JSON, nullable=True)

    # Quick wins (persisted so they reload from DB)
    quick_wins = Column(JSON, nullable=True)

    # LLM-generated issue triage markdown
    issue_command_center_triage = Column(Text, nullable=True)

    # Revenue impact totals
    total_opportunity_value = Column(Numeric(12, 2), default=0)
    total_issues_identified = Column(Integer, default=0)
    total_quick_wins = Column(Integer, default=0)

    # Degradation state (Req 6: failure-state transparency)
    is_degraded = Column(Boolean, default=False)
    stale_modules = Column(JSON, nullable=True)
    module_freshness = Column(JSON, nullable=True)

    # Status
    is_current = Column(Boolean, default=True, index=True)
    generation_time_seconds = Column(Float, nullable=True)
    llm_calls_made = Column(Integer, default=0)
    llm_tokens_used = Column(Integer, default=0)

    # Timestamps
    generated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<StrategicBrief {self.cadence} {self.brief_date}>"


class BriefRecommendation(Base):
    """
    Individual recommendation from a strategic brief.

    Each recommendation has a pinpoint solution with implementation steps
    and a tracked status for follow-through.
    """
    __tablename__ = "strategic_brief_recommendations"

    id = Column(Integer, primary_key=True, index=True)
    brief_id = Column(Integer, ForeignKey("strategic_briefs.id"), index=True)

    # Classification
    category = Column(String, index=True)  # cro|growth|issue_fix|cost_saving|quick_win
    priority_rank = Column(Integer)
    priority_level = Column(String)  # critical|high|medium|low

    # Content (LLM-generated pinpoint solution)
    title = Column(String, nullable=False)
    problem_statement = Column(Text, nullable=True)
    root_cause = Column(Text, nullable=True)
    specific_solution = Column(Text, nullable=True)
    implementation_steps = Column(JSON, nullable=True)

    # Impact
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)
    estimated_cost_savings = Column(Numeric(10, 2), default=0)
    impact_timeframe = Column(String, nullable=True)  # immediate|this_week|this_month|quarterly
    confidence_score = Column(Float, default=0.5)

    # Source tracing
    source_modules = Column(JSON, nullable=True)  # ["ad_spend", "behavior"]
    evidence_data = Column(JSON, nullable=True)

    # Effort
    effort_hours = Column(Float, nullable=True)
    effort_level = Column(String, nullable=True)  # trivial|low|medium|high
    responsible_team = Column(String, nullable=True)  # marketing|dev|ops|content

    # Action-first (Req 1: owner, due date)
    due_date = Column(Date, nullable=True)

    # Algorithmic ranking (Req 5: impact x confidence x urgency)
    priority_score = Column(Float, default=0)
    urgency_weight = Column(Float, default=1.0)

    # Data guardrails (Req 2: data_as_of per recommendation)
    data_as_of = Column(JSON, nullable=True)

    # Dedup (Req 3: cross-module deduplication)
    dedup_hash = Column(String(32), nullable=True, index=True)
    is_cross_functional = Column(Boolean, default=True)

    # Outcome tracking (Req 4: baseline → target → actual)
    baseline_metric_name = Column(String, nullable=True)
    baseline_metric_value = Column(Float, nullable=True)
    target_metric_value = Column(Float, nullable=True)
    impact_7d = Column(Float, nullable=True)
    impact_30d = Column(Float, nullable=True)

    # Tracking
    status = Column(String, default='new', index=True)  # new|in_progress|completed|deferred
    completed_at = Column(DateTime, nullable=True)
    actual_impact = Column(Numeric(10, 2), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<BriefRecommendation #{self.priority_rank}: {self.title}>"


class BriefCorrelation(Base):
    """
    Cross-module correlation detected by algorithmic analysis.

    Represents causal chains like:
    'Pricing dropped on Brand X -> Conversion fell 12% -> Revenue down $3,400/week'
    """
    __tablename__ = "strategic_brief_correlations"

    id = Column(Integer, primary_key=True, index=True)
    brief_id = Column(Integer, ForeignKey("strategic_briefs.id"), index=True)

    # The correlation
    correlation_type = Column(String)  # causal_chain|co_occurrence|inverse|amplifying
    modules_involved = Column(JSON, nullable=True)
    title = Column(String, nullable=True)
    narrative = Column(Text, nullable=True)
    evidence = Column(JSON, nullable=True)
    confidence = Column(Float, default=0.5)
    revenue_impact = Column(Numeric(10, 2), default=0)

    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<BriefCorrelation: {self.title}>"
