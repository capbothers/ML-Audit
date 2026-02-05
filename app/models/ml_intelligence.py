"""
ML Intelligence Models - Phase 1

Stores outputs from forecasting, anomaly detection, and inventory analysis.
Designed for lightweight, explainable ML baselines.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Date, UniqueConstraint
from datetime import datetime

from app.models.base import Base


class MLForecast(Base):
    """Daily forecasts for key business metrics (revenue, orders, sessions)"""
    __tablename__ = "ml_forecasts"

    id = Column(Integer, primary_key=True, index=True)

    date = Column(Date, index=True, nullable=False)
    metric = Column(String, index=True, nullable=False)
    horizon_days = Column(Integer, nullable=False)

    predicted_value = Column(Float, nullable=False)
    lower_bound = Column(Float, nullable=True)
    upper_bound = Column(Float, nullable=True)

    model_type = Column(String, nullable=False)
    training_window_days = Column(Integer, default=90)

    generated_at = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint('date', 'metric', 'generated_at', name='uq_ml_forecast_date_metric_gen'),
    )


class MLAnomaly(Base):
    """Detected anomalies in business metrics"""
    __tablename__ = "ml_anomalies"

    id = Column(Integer, primary_key=True, index=True)

    date = Column(Date, index=True, nullable=False)
    metric = Column(String, index=True, nullable=False)

    actual_value = Column(Float, nullable=False)
    expected_value = Column(Float, nullable=False)
    deviation_pct = Column(Float, nullable=False)
    z_score = Column(Float, nullable=False)
    direction = Column(String, nullable=False)

    severity = Column(String, index=True, nullable=False)
    baseline_window = Column(Integer, nullable=False)

    is_acknowledged = Column(Boolean, default=False, index=True)
    acknowledged_at = Column(DateTime, nullable=True)

    generated_at = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint('date', 'metric', 'baseline_window', name='uq_ml_anomaly_date_metric_window'),
    )


class MLInventorySuggestion(Base):
    """Inventory reorder suggestions based on sales velocity"""
    __tablename__ = "ml_inventory_suggestions"

    id = Column(Integer, primary_key=True, index=True)

    sku = Column(String, index=True, nullable=False)
    brand = Column(String, index=True, nullable=True)
    title = Column(String, nullable=True)

    units_on_hand = Column(Integer, nullable=False)
    daily_sales_velocity = Column(Float, nullable=False)
    velocity_trend = Column(String, nullable=True)
    days_of_cover = Column(Float, nullable=False)

    suggestion = Column(String, index=True, nullable=False)
    reorder_quantity = Column(Integer, nullable=True)
    urgency = Column(String, index=True, nullable=False)

    oversold = Column(Boolean, default=False, nullable=False)
    cost_missing = Column(Boolean, default=False, nullable=False)
    offline_units_30d = Column(Float, default=0, nullable=False)

    generated_at = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint('sku', 'generated_at', name='uq_ml_inventory_sku_gen'),
    )


class InventoryDailySnapshot(Base):
    """Daily inventory level snapshots per SKU for offline sales inference."""
    __tablename__ = "inventory_daily_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, index=True, nullable=False)
    snapshot_date = Column(Date, index=True, nullable=False)
    quantity = Column(Integer, nullable=False)
    synced_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint('sku', 'snapshot_date', name='uq_inv_snapshot_sku_date'),
    )
