"""
ML Intelligence Service - Phase 1

Lightweight, explainable ML baselines:
1. Forecasting (Holt's Linear Exponential Smoothing)
2. Anomaly Detection (Rolling Z-Score)
3. Revenue Driver Analysis (Multiplicative Decomposition)
4. Tracking Health (GA4 vs Shopify Gap)
5. Inventory Suggestions (Sales Velocity + Days of Cover)
"""
import math
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal

from sqlalchemy import func, text, and_, cast, Date
from sqlalchemy.sql.expression import case
from sqlalchemy.orm import Session

from app.models.ml_intelligence import MLForecast, MLAnomaly, MLInventorySuggestion, InventoryDailySnapshot
from app.models.ga4_data import GA4DailySummary, GA4DailyEcommerce
from app.models.shopify import ShopifyOrder, ShopifyOrderItem, ShopifyInventory, ShopifyProduct
from app.models.product_cost import ProductCost

logger = logging.getLogger(__name__)


def _ensure_date(val) -> date:
    """Convert a value to a Python date object if it's a string."""
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        return date.fromisoformat(val)
    return val


class MLIntelligenceService:
    def __init__(self, db: Session):
        self.db = db

    # ─────────────────────────────────────────────
    # DATA HELPERS
    # ─────────────────────────────────────────────

    def _fetch_daily_metric_history(
        self, metric: str, days: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Fetch daily metric history from GA4 + Shopify sources.

        Returns list of {date, value} dicts sorted by date ascending.
        """
        cutoff = date.today() - timedelta(days=days)

        if metric == "sessions":
            rows = (
                self.db.query(
                    GA4DailySummary.date,
                    GA4DailySummary.sessions.label("value"),
                )
                .filter(GA4DailySummary.date >= cutoff)
                .order_by(GA4DailySummary.date)
                .all()
            )
            return [{"date": r.date, "value": float(r.value or 0)} for r in rows]

        if metric in ("revenue", "orders", "aov", "conversion_rate"):
            # Aggregate from shopify_orders per day
            rows = (
                self.db.query(
                    func.date(ShopifyOrder.created_at).label("day"),
                    func.sum(func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)).label("revenue"),
                    func.count(ShopifyOrder.id).label("orders"),
                )
                .filter(
                    func.date(ShopifyOrder.created_at) >= cutoff,
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                    ShopifyOrder.cancelled_at.is_(None),
                )
                .group_by(func.date(ShopifyOrder.created_at))
                .order_by(func.date(ShopifyOrder.created_at))
                .all()
            )

            if metric == "revenue":
                return [{"date": _ensure_date(r.day), "value": float(r.revenue or 0)} for r in rows]
            if metric == "orders":
                return [{"date": _ensure_date(r.day), "value": float(r.orders or 0)} for r in rows]

            # For aov and conversion_rate, we also need sessions
            if metric == "aov":
                return [
                    {
                        "date": _ensure_date(r.day),
                        "value": float(r.revenue or 0) / max(float(r.orders or 1), 1),
                    }
                    for r in rows
                ]

            if metric == "conversion_rate":
                # Build a date->sessions lookup from GA4
                session_rows = (
                    self.db.query(
                        GA4DailySummary.date,
                        GA4DailySummary.sessions,
                    )
                    .filter(GA4DailySummary.date >= cutoff)
                    .all()
                )
                sessions_by_date = {r.date: float(r.sessions or 0) for r in session_rows}
                result = []
                for r in rows:
                    day = _ensure_date(r.day)
                    sessions = sessions_by_date.get(day, 0)
                    cr = (float(r.orders or 0) / sessions * 100) if sessions > 0 else 0
                    result.append({"date": day, "value": cr})
                return result

        return []

    def _fetch_shopify_daily_aggregates(
        self, days: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Fetch daily Shopify aggregates (revenue, orders) for driver analysis.
        """
        cutoff = date.today() - timedelta(days=days)
        rows = (
            self.db.query(
                func.date(ShopifyOrder.created_at).label("day"),
                func.sum(func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)).label("revenue"),
                func.count(ShopifyOrder.id).label("orders"),
            )
            .filter(
                func.date(ShopifyOrder.created_at) >= cutoff,
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrder.cancelled_at.is_(None),
            )
            .group_by(func.date(ShopifyOrder.created_at))
            .order_by(func.date(ShopifyOrder.created_at))
            .all()
        )
        return [
            {
                "date": _ensure_date(r.day),
                "revenue": float(r.revenue or 0),
                "orders": int(r.orders or 0),
            }
            for r in rows
        ]

    # ─────────────────────────────────────────────
    # 1. FORECASTING - Holt's Linear Exponential Smoothing
    # ─────────────────────────────────────────────

    def _holt_forecast(
        self,
        values: List[float],
        horizon: int,
        alpha: float = 0.3,
        beta: float = 0.1,
    ) -> Tuple[List[float], float]:
        """
        Holt's Linear Exponential Smoothing.

        Returns (predictions, residual_std) where predictions is a list
        of horizon forecast values.
        """
        if len(values) < 2:
            avg = values[0] if values else 0.0
            return [avg] * horizon, 0.0

        # Initialize level and trend
        level = values[0]
        trend = values[1] - values[0]

        # Fit on historical data, collect residuals
        residuals = []
        for i in range(1, len(values)):
            forecast_i = level + trend
            residuals.append(values[i] - forecast_i)

            new_level = alpha * values[i] + (1 - alpha) * (level + trend)
            new_trend = beta * (new_level - level) + (1 - beta) * trend
            level = new_level
            trend = new_trend

        # Residual standard deviation for confidence intervals
        if len(residuals) > 1:
            mean_r = sum(residuals) / len(residuals)
            var_r = sum((r - mean_r) ** 2 for r in residuals) / (len(residuals) - 1)
            residual_std = math.sqrt(var_r)
        else:
            residual_std = 0.0

        # Generate forecasts
        predictions = []
        for h in range(1, horizon + 1):
            predictions.append(level + h * trend)

        return predictions, residual_std

    def _moving_average_forecast(
        self, values: List[float], horizon: int, window: int = 7
    ) -> Tuple[List[float], float]:
        """Fallback: simple moving average forecast for short history."""
        if not values:
            return [0.0] * horizon, 0.0

        recent = values[-window:] if len(values) >= window else values
        avg = sum(recent) / len(recent)

        # Residual std from recent window
        if len(recent) > 1:
            var_r = sum((v - avg) ** 2 for v in recent) / (len(recent) - 1)
            residual_std = math.sqrt(var_r)
        else:
            residual_std = 0.0

        return [avg] * horizon, residual_std

    def _forecast_metric(
        self, metric: str, horizon: int = 7, training_window: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Generate forecast for a single metric.

        Returns list of forecast dicts ready for DB insertion.
        """
        history = self._fetch_daily_metric_history(metric, days=training_window)

        if not history:
            logger.warning(f"No history for metric '{metric}', skipping forecast")
            return []

        values = [h["value"] for h in history]

        # Choose method based on history length
        if len(values) >= 14:
            predictions, residual_std = self._holt_forecast(values, horizon)
            model_type = "holt_linear"
        else:
            predictions, residual_std = self._moving_average_forecast(values, horizon)
            model_type = "moving_average_7d"

        now = datetime.utcnow()
        last_date = history[-1]["date"]
        forecasts = []

        for i, pred in enumerate(predictions):
            forecast_date = last_date + timedelta(days=i + 1)
            # 80% confidence interval: z=1.28
            ci_width = 1.28 * residual_std * math.sqrt(i + 1)

            # Don't allow negative lower bounds for non-negative metrics
            lower = max(0, pred - ci_width) if metric != "conversion_rate" else pred - ci_width

            forecasts.append(
                {
                    "date": forecast_date,
                    "metric": metric,
                    "horizon_days": i + 1,
                    "predicted_value": round(pred, 2),
                    "lower_bound": round(lower, 2),
                    "upper_bound": round(pred + ci_width, 2),
                    "model_type": model_type,
                    "training_window_days": training_window,
                    "generated_at": now,
                }
            )

        return forecasts

    def generate_forecasts(self, horizon: int = 7) -> Dict[str, Any]:
        """
        Generate forecasts for all tracked metrics and persist to DB.
        """
        metrics = ["revenue", "orders", "sessions"]
        total_inserted = 0
        results = {}

        for metric in metrics:
            forecasts = self._forecast_metric(metric, horizon=horizon)
            for f in forecasts:
                self.db.add(MLForecast(**f))
            total_inserted += len(forecasts)
            results[metric] = len(forecasts)
            logger.info(f"Generated {len(forecasts)} forecasts for {metric}")

        self.db.commit()
        return {"forecasts_generated": total_inserted, "by_metric": results}

    def get_forecasts(
        self, metric: Optional[str] = None, days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the most recent forecasts from DB.
        """
        # Find the most recent generated_at timestamp
        latest_q = self.db.query(func.max(MLForecast.generated_at))
        if metric:
            latest_q = latest_q.filter(MLForecast.metric == metric)
        latest_gen = latest_q.scalar()

        if not latest_gen:
            return []

        query = (
            self.db.query(MLForecast)
            .filter(MLForecast.generated_at == latest_gen)
        )
        if metric:
            query = query.filter(MLForecast.metric == metric)

        query = query.filter(MLForecast.horizon_days <= days)
        query = query.order_by(MLForecast.metric, MLForecast.date)

        rows = query.all()
        return [
            {
                "date": str(r.date),
                "metric": r.metric,
                "horizon_days": r.horizon_days,
                "predicted_value": r.predicted_value,
                "lower_bound": r.lower_bound,
                "upper_bound": r.upper_bound,
                "model_type": r.model_type,
                "generated_at": str(r.generated_at),
            }
            for r in rows
        ]

    # ─────────────────────────────────────────────
    # 2. ANOMALY DETECTION - Rolling Z-Score
    # ─────────────────────────────────────────────

    def _detect_metric_anomalies(
        self,
        metric: str,
        baseline_window: int,
        lookback_days: int = 14,
        history_days: int = 90,
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies for a single metric using rolling z-score.

        Args:
            metric: Metric name
            baseline_window: Rolling window size (7 or 30)
            lookback_days: How many recent days to check for anomalies
            history_days: Total history to fetch for computing baselines
        """
        history = self._fetch_daily_metric_history(metric, days=history_days)

        if len(history) < baseline_window + lookback_days:
            logger.warning(
                f"Insufficient history for {metric} anomaly detection "
                f"({len(history)} < {baseline_window + lookback_days})"
            )
            return []

        now = datetime.utcnow()
        anomalies = []

        # Check the last lookback_days points
        start_idx = max(baseline_window, len(history) - lookback_days)

        for i in range(start_idx, len(history)):
            # Compute rolling mean and std from preceding window
            window_values = [
                history[j]["value"] for j in range(i - baseline_window, i)
            ]
            mean_val = sum(window_values) / len(window_values)
            if len(window_values) > 1:
                var_val = sum((v - mean_val) ** 2 for v in window_values) / (
                    len(window_values) - 1
                )
                std_val = math.sqrt(var_val)
            else:
                std_val = 0.0

            actual = history[i]["value"]

            if std_val == 0:
                continue

            z_score = (actual - mean_val) / std_val

            # Severity thresholds
            abs_z = abs(z_score)
            if abs_z < 2.5:
                continue  # Not anomalous enough

            if abs_z >= 4:
                severity = "critical"
            elif abs_z >= 3:
                severity = "high"
            else:
                severity = "medium"

            deviation_pct = ((actual - mean_val) / mean_val * 100) if mean_val != 0 else 0
            direction = "spike" if z_score > 0 else "drop"

            anomalies.append(
                {
                    "date": history[i]["date"],
                    "metric": metric,
                    "actual_value": round(actual, 2),
                    "expected_value": round(mean_val, 2),
                    "deviation_pct": round(deviation_pct, 2),
                    "z_score": round(z_score, 2),
                    "direction": direction,
                    "severity": severity,
                    "baseline_window": baseline_window,
                    "generated_at": now,
                }
            )

        return anomalies

    def detect_anomalies(self, history_days: int = 90) -> Dict[str, Any]:
        """
        Detect anomalies across all metrics and persist to DB.

        Uses both 7-day and 30-day baselines for each metric.
        """
        metrics = ["revenue", "orders", "sessions", "conversion_rate", "aov"]
        baselines = [7, 30]
        total_upserted = 0
        results = {}

        for metric in metrics:
            metric_count = 0
            for window in baselines:
                anomalies = self._detect_metric_anomalies(
                    metric, baseline_window=window, history_days=history_days
                )
                for a in anomalies:
                    # Upsert: check if exists
                    existing = (
                        self.db.query(MLAnomaly)
                        .filter(
                            MLAnomaly.date == a["date"],
                            MLAnomaly.metric == a["metric"],
                            MLAnomaly.baseline_window == a["baseline_window"],
                        )
                        .first()
                    )
                    if existing:
                        # Update
                        for k, v in a.items():
                            if k != "date" and k != "metric" and k != "baseline_window":
                                setattr(existing, k, v)
                    else:
                        self.db.add(MLAnomaly(**a))
                    metric_count += 1

                total_upserted += len(anomalies)

            results[metric] = metric_count

        self.db.commit()
        return {"anomalies_upserted": total_upserted, "by_metric": results}

    def get_anomalies(
        self,
        days: int = 30,
        severity: Optional[str] = None,
        metric: Optional[str] = None,
        unacknowledged_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Retrieve recent anomalies from DB."""
        cutoff = date.today() - timedelta(days=days)
        query = self.db.query(MLAnomaly).filter(MLAnomaly.date >= cutoff)

        if severity:
            query = query.filter(MLAnomaly.severity == severity)
        if metric:
            query = query.filter(MLAnomaly.metric == metric)
        if unacknowledged_only:
            query = query.filter(MLAnomaly.is_acknowledged == False)

        query = query.order_by(MLAnomaly.date.desc(), MLAnomaly.severity.desc())
        rows = query.all()

        return [
            {
                "id": r.id,
                "date": str(r.date),
                "metric": r.metric,
                "actual_value": r.actual_value,
                "expected_value": r.expected_value,
                "deviation_pct": r.deviation_pct,
                "z_score": r.z_score,
                "direction": r.direction,
                "severity": r.severity,
                "baseline_window": r.baseline_window,
                "is_acknowledged": r.is_acknowledged,
                "generated_at": str(r.generated_at),
            }
            for r in rows
        ]

    def acknowledge_anomaly(self, anomaly_id: int) -> Optional[Dict[str, Any]]:
        """Mark an anomaly as acknowledged."""
        anomaly = self.db.query(MLAnomaly).filter(MLAnomaly.id == anomaly_id).first()
        if not anomaly:
            return None

        anomaly.is_acknowledged = True
        anomaly.acknowledged_at = datetime.utcnow()
        self.db.commit()

        return {
            "id": anomaly.id,
            "metric": anomaly.metric,
            "date": str(anomaly.date),
            "is_acknowledged": True,
            "acknowledged_at": str(anomaly.acknowledged_at),
        }

    # ─────────────────────────────────────────────
    # 3. REVENUE DRIVER ANALYSIS
    # ─────────────────────────────────────────────

    def get_revenue_drivers(self, days: int = 7) -> Dict[str, Any]:
        """
        Decompose revenue change into Sessions, CR, AOV contributions.

        Revenue = Sessions x CR x AOV

        Compares current N days vs preceding N days.
        Uses first-order Taylor decomposition with proportional
        interaction redistribution.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        prev_start = start_date - timedelta(days=days)
        prev_end = start_date - timedelta(days=1)

        # Fetch GA4 sessions for both periods
        def _get_period_sessions(start: date, end: date) -> float:
            result = (
                self.db.query(func.sum(GA4DailySummary.sessions))
                .filter(
                    GA4DailySummary.date >= start,
                    GA4DailySummary.date <= end,
                )
                .scalar()
            )
            return float(result or 0)

        # Fetch Shopify orders/revenue for both periods
        def _get_period_shopify(start: date, end: date) -> Tuple[float, int]:
            result = (
                self.db.query(
                    func.sum(func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)),
                    func.count(ShopifyOrder.id),
                )
                .filter(
                    func.date(ShopifyOrder.created_at) >= start,
                    func.date(ShopifyOrder.created_at) <= end,
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                    ShopifyOrder.cancelled_at.is_(None),
                )
                .first()
            )
            return (float(result[0] or 0), int(result[1] or 0))

        # Current period
        curr_sessions = _get_period_sessions(start_date, end_date)
        curr_revenue, curr_orders = _get_period_shopify(start_date, end_date)
        curr_cr = (curr_orders / curr_sessions * 100) if curr_sessions > 0 else 0
        curr_aov = (curr_revenue / curr_orders) if curr_orders > 0 else 0

        # Previous period
        prev_sessions = _get_period_sessions(prev_start, prev_end)
        prev_revenue, prev_orders = _get_period_shopify(prev_start, prev_end)
        prev_cr = (prev_orders / prev_sessions * 100) if prev_sessions > 0 else 0
        prev_aov = (prev_revenue / prev_orders) if prev_orders > 0 else 0

        # Decomposition
        delta_revenue = curr_revenue - prev_revenue

        if prev_revenue == 0:
            return {
                "period_days": days,
                "current": {
                    "revenue": round(curr_revenue, 2),
                    "sessions": int(curr_sessions),
                    "conversion_rate": round(curr_cr, 2),
                    "aov": round(curr_aov, 2),
                    "orders": curr_orders,
                },
                "previous": {
                    "revenue": round(prev_revenue, 2),
                    "sessions": int(prev_sessions),
                    "conversion_rate": round(prev_cr, 2),
                    "aov": round(prev_aov, 2),
                    "orders": prev_orders,
                },
                "delta_revenue": round(delta_revenue, 2),
                "delta_pct": None,
                "drivers": [],
                "narrative": "No previous period data for comparison.",
            }

        delta_pct = (delta_revenue / prev_revenue) * 100

        # First-order Taylor decomposition
        # R = S * CR * AOV
        # dR ≈ dS * CR0 * AOV0 + S0 * dCR * AOV0 + S0 * CR0 * dAOV + interactions
        dS = curr_sessions - prev_sessions
        dCR = (curr_cr / 100) - (prev_cr / 100)  # As fraction
        dAOV = curr_aov - prev_aov

        S0 = prev_sessions
        CR0 = prev_cr / 100  # As fraction
        AOV0 = prev_aov

        # First-order contributions
        session_contribution = dS * CR0 * AOV0
        cr_contribution = S0 * dCR * AOV0
        aov_contribution = S0 * CR0 * dAOV

        # Interaction term (redistribute proportionally)
        first_order_total = session_contribution + cr_contribution + aov_contribution
        interaction = delta_revenue - first_order_total

        if first_order_total != 0:
            session_share = abs(session_contribution) / (
                abs(session_contribution) + abs(cr_contribution) + abs(aov_contribution)
            )
            cr_share = abs(cr_contribution) / (
                abs(session_contribution) + abs(cr_contribution) + abs(aov_contribution)
            )
            aov_share = abs(aov_contribution) / (
                abs(session_contribution) + abs(cr_contribution) + abs(aov_contribution)
            )

            session_contribution += interaction * session_share
            cr_contribution += interaction * cr_share
            aov_contribution += interaction * aov_share

        # Build drivers list
        drivers = [
            {
                "driver": "sessions",
                "previous": int(prev_sessions),
                "current": int(curr_sessions),
                "change_pct": round(
                    (dS / prev_sessions * 100) if prev_sessions > 0 else 0, 1
                ),
                "revenue_impact": round(session_contribution, 2),
                "revenue_impact_pct": round(
                    (session_contribution / prev_revenue * 100)
                    if prev_revenue > 0
                    else 0,
                    1,
                ),
            },
            {
                "driver": "conversion_rate",
                "previous": round(prev_cr, 2),
                "current": round(curr_cr, 2),
                "change_pct": round(
                    (dCR / CR0 * 100) if CR0 > 0 else 0, 1
                ),
                "revenue_impact": round(cr_contribution, 2),
                "revenue_impact_pct": round(
                    (cr_contribution / prev_revenue * 100)
                    if prev_revenue > 0
                    else 0,
                    1,
                ),
            },
            {
                "driver": "aov",
                "previous": round(prev_aov, 2),
                "current": round(curr_aov, 2),
                "change_pct": round(
                    (dAOV / prev_aov * 100) if prev_aov > 0 else 0, 1
                ),
                "revenue_impact": round(aov_contribution, 2),
                "revenue_impact_pct": round(
                    (aov_contribution / prev_revenue * 100)
                    if prev_revenue > 0
                    else 0,
                    1,
                ),
            },
        ]

        # Sort by absolute impact
        drivers.sort(key=lambda d: abs(d["revenue_impact"]), reverse=True)

        # Generate narrative
        # Primary driver = largest contributor aligned with the revenue direction
        # If revenue is down, primary = biggest negative impact
        # If revenue is up, primary = biggest positive impact
        if delta_revenue < 0:
            negative_drivers = [d for d in drivers if d["revenue_impact"] < 0]
            positive_drivers = [d for d in drivers if d["revenue_impact"] > 0]
            # Sort negatives by most negative first
            negative_drivers.sort(key=lambda d: d["revenue_impact"])
            primary = negative_drivers[0] if negative_drivers else drivers[0]
        else:
            positive_drivers = [d for d in drivers if d["revenue_impact"] > 0]
            negative_drivers = [d for d in drivers if d["revenue_impact"] < 0]
            # Sort positives by most positive first
            positive_drivers.sort(key=lambda d: d["revenue_impact"], reverse=True)
            primary = positive_drivers[0] if positive_drivers else drivers[0]

        direction = "up" if delta_revenue > 0 else "down"

        # Build a richer narrative when drivers conflict
        if delta_revenue < 0 and positive_drivers:
            # Revenue down but some drivers are positive — explain the offset
            offsetting = ", ".join(
                f"{d['driver'].replace('_', ' ')} ({d['change_pct']:+.1f}%)"
                for d in positive_drivers
            )
            dragging = ", ".join(
                f"{d['driver'].replace('_', ' ')} ({d['change_pct']:+.1f}%)"
                for d in negative_drivers
            )
            narrative = (
                f"Revenue is down ${abs(delta_revenue):,.0f} "
                f"({delta_pct:+.1f}%) vs prior {days} days. "
                f"{dragging} dragged revenue down "
                f"despite {offsetting} improving. "
                f"The largest drag is {primary['driver'].replace('_', ' ')}, "
                f"costing ${abs(primary['revenue_impact']):,.0f}."
            )
        elif delta_revenue > 0 and negative_drivers:
            # Revenue up but some drivers are negative
            helping = ", ".join(
                f"{d['driver'].replace('_', ' ')} ({d['change_pct']:+.1f}%)"
                for d in positive_drivers
            )
            limiting = ", ".join(
                f"{d['driver'].replace('_', ' ')} ({d['change_pct']:+.1f}%)"
                for d in negative_drivers
            )
            narrative = (
                f"Revenue is up ${abs(delta_revenue):,.0f} "
                f"({delta_pct:+.1f}%) vs prior {days} days. "
                f"{helping} drove the gain "
                f"despite {limiting} declining. "
                f"The biggest contributor is {primary['driver'].replace('_', ' ')}, "
                f"adding ${abs(primary['revenue_impact']):,.0f}."
            )
        else:
            # All drivers moving in the same direction
            narrative = (
                f"Revenue is {direction} ${abs(delta_revenue):,.0f} "
                f"({delta_pct:+.1f}%) vs prior {days} days. "
                f"The primary driver is {primary['driver'].replace('_', ' ')} "
                f"({primary['change_pct']:+.1f}%), contributing "
                f"${abs(primary['revenue_impact']):,.0f} of the change."
            )

        return {
            "period_days": days,
            "current": {
                "revenue": round(curr_revenue, 2),
                "sessions": int(curr_sessions),
                "conversion_rate": round(curr_cr, 2),
                "aov": round(curr_aov, 2),
                "orders": curr_orders,
            },
            "previous": {
                "revenue": round(prev_revenue, 2),
                "sessions": int(prev_sessions),
                "conversion_rate": round(prev_cr, 2),
                "aov": round(prev_aov, 2),
                "orders": prev_orders,
            },
            "delta_revenue": round(delta_revenue, 2),
            "delta_pct": round(delta_pct, 1),
            "drivers": drivers,
            "narrative": narrative,
        }

    # ─────────────────────────────────────────────
    # 4. TRACKING HEALTH - GA4 vs Shopify Gap
    # ─────────────────────────────────────────────

    def get_tracking_health(self, days: int = 7) -> Dict[str, Any]:
        """
        Compare GA4 ecommerce data vs Shopify orders to detect tracking gaps.

        Thresholds:
        - < 5% gap: ok
        - 5-15% gap: warning
        - > 15% gap: critical
        """
        cutoff = date.today() - timedelta(days=days)

        # GA4 daily ecommerce
        ga4_rows = (
            self.db.query(
                GA4DailyEcommerce.date,
                GA4DailyEcommerce.ecommerce_purchases,
                GA4DailyEcommerce.total_revenue,
            )
            .filter(GA4DailyEcommerce.date >= cutoff)
            .order_by(GA4DailyEcommerce.date)
            .all()
        )
        ga4_by_date = {
            r.date: {"orders": int(r.ecommerce_purchases or 0), "revenue": float(r.total_revenue or 0)}
            for r in ga4_rows
        }

        # Shopify daily aggregates
        shopify_rows = (
            self.db.query(
                func.date(ShopifyOrder.created_at).label("day"),
                func.count(ShopifyOrder.id).label("orders"),
                func.sum(func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)).label("revenue"),
            )
            .filter(
                func.date(ShopifyOrder.created_at) >= cutoff,
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrder.cancelled_at.is_(None),
            )
            .group_by(func.date(ShopifyOrder.created_at))
            .order_by(func.date(ShopifyOrder.created_at))
            .all()
        )
        shopify_by_date = {
            _ensure_date(r.day): {"orders": int(r.orders or 0), "revenue": float(r.revenue or 0)}
            for r in shopify_rows
        }

        # Compare per day
        all_dates = sorted(set(list(ga4_by_date.keys()) + list(shopify_by_date.keys())))
        daily_comparison = []
        tracking_break_days = []
        total_ga4_orders = 0
        total_shopify_orders = 0
        total_ga4_revenue = 0.0
        total_shopify_revenue = 0.0

        for d in all_dates:
            ga4 = ga4_by_date.get(d, {"orders": 0, "revenue": 0})
            shopify = shopify_by_date.get(d, {"orders": 0, "revenue": 0})

            # Detect tracking breaks: GA4 reports 0 orders but Shopify has orders
            is_tracking_break = ga4["orders"] == 0 and shopify["orders"] > 0

            order_gap = ga4["orders"] - shopify["orders"]
            order_gap_pct = (
                (order_gap / shopify["orders"] * 100) if shopify["orders"] > 0 else 0
            )
            revenue_gap = ga4["revenue"] - shopify["revenue"]
            revenue_gap_pct = (
                (revenue_gap / shopify["revenue"] * 100)
                if shopify["revenue"] > 0
                else 0
            )

            if is_tracking_break:
                status = "tracking_break"
                tracking_break_days.append(str(d))
            else:
                abs_order_gap = abs(order_gap_pct)
                if abs_order_gap < 5:
                    status = "ok"
                elif abs_order_gap < 15:
                    status = "warning"
                else:
                    status = "critical"

            total_ga4_orders += ga4["orders"]
            total_shopify_orders += shopify["orders"]
            total_ga4_revenue += ga4["revenue"]
            total_shopify_revenue += shopify["revenue"]

            daily_comparison.append(
                {
                    "date": str(d),
                    "ga4_orders": ga4["orders"],
                    "shopify_orders": shopify["orders"],
                    "order_gap": order_gap,
                    "order_gap_pct": round(order_gap_pct, 1),
                    "ga4_revenue": round(ga4["revenue"], 2),
                    "shopify_revenue": round(shopify["revenue"], 2),
                    "revenue_gap": round(revenue_gap, 2),
                    "revenue_gap_pct": round(revenue_gap_pct, 1),
                    "status": status,
                }
            )

        # Overall summary
        overall_order_gap_pct = (
            ((total_ga4_orders - total_shopify_orders) / total_shopify_orders * 100)
            if total_shopify_orders > 0
            else 0
        )
        overall_revenue_gap_pct = (
            ((total_ga4_revenue - total_shopify_revenue) / total_shopify_revenue * 100)
            if total_shopify_revenue > 0
            else 0
        )

        abs_overall = abs(overall_order_gap_pct)
        if abs_overall < 5:
            overall_status = "ok"
        elif abs_overall < 15:
            overall_status = "warning"
        else:
            overall_status = "critical"

        # Escalate: any tracking break days → force critical
        if tracking_break_days:
            overall_status = "critical"

        critical_days = sum(1 for d in daily_comparison if d["status"] == "critical")
        warning_days = sum(1 for d in daily_comparison if d["status"] == "warning")

        return {
            "period_days": days,
            "overall_status": overall_status,
            "summary": {
                "ga4_total_orders": total_ga4_orders,
                "shopify_total_orders": total_shopify_orders,
                "order_gap_pct": round(overall_order_gap_pct, 1),
                "ga4_total_revenue": round(total_ga4_revenue, 2),
                "shopify_total_revenue": round(total_shopify_revenue, 2),
                "revenue_gap_pct": round(overall_revenue_gap_pct, 1),
            },
            "critical_days": critical_days,
            "warning_days": warning_days,
            "tracking_break_days": tracking_break_days,
            "tracking_break_count": len(tracking_break_days),
            "daily": daily_comparison,
        }

    # ─────────────────────────────────────────────
    # 5. INVENTORY SUGGESTIONS
    # ─────────────────────────────────────────────

    def _capture_inventory_snapshot(self) -> int:
        """Capture today's inventory levels per SKU into daily snapshots."""
        today = date.today()
        now = datetime.utcnow()

        active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
            ShopifyProduct.status == 'active'
        ).subquery()

        inventory_rows = (
            self.db.query(
                ShopifyInventory.sku,
                func.sum(ShopifyInventory.inventory_quantity).label("qty"),
            )
            .filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.sku != "",
            )
            .group_by(ShopifyInventory.sku)
            .all()
        )

        upserted = 0
        for row in inventory_rows:
            existing = (
                self.db.query(InventoryDailySnapshot)
                .filter(
                    InventoryDailySnapshot.sku == row.sku,
                    InventoryDailySnapshot.snapshot_date == today,
                )
                .first()
            )
            if existing:
                existing.quantity = int(row.qty or 0)
                existing.synced_at = now
            else:
                self.db.add(InventoryDailySnapshot(
                    sku=row.sku,
                    snapshot_date=today,
                    quantity=int(row.qty or 0),
                    synced_at=now,
                ))
            upserted += 1

        self.db.flush()
        logger.info(f"Captured inventory snapshot for {upserted} SKUs on {today}")
        return upserted

    def _compute_offline_units(self, sku: str, days: int = 30) -> float:
        """Infer offline (showroom) sales from inventory drops that exceed online orders."""
        cutoff = date.today() - timedelta(days=days)

        snapshots = (
            self.db.query(InventoryDailySnapshot)
            .filter(
                func.upper(InventoryDailySnapshot.sku) == sku.upper(),
                InventoryDailySnapshot.snapshot_date >= cutoff,
            )
            .order_by(InventoryDailySnapshot.snapshot_date)
            .all()
        )

        if len(snapshots) < 2:
            return 0.0

        offline_total = 0.0
        for i in range(1, len(snapshots)):
            prev_day = snapshots[i - 1]
            curr_day = snapshots[i]

            inventory_delta = prev_day.quantity - curr_day.quantity
            if inventory_delta <= 0:
                continue  # Stock went up (restock) or unchanged

            # Online units sold that day
            online_sold = (
                self.db.query(func.sum(ShopifyOrderItem.quantity))
                .join(
                    ShopifyOrder,
                    ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
                )
                .filter(
                    func.upper(ShopifyOrderItem.sku) == sku.upper(),
                    func.date(ShopifyOrderItem.order_date) == curr_day.snapshot_date,
                    ShopifyOrder.cancelled_at.is_(None),
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                )
                .scalar()
            ) or 0

            if inventory_delta > online_sold:
                offline_total += inventory_delta - online_sold

        return round(offline_total, 1)

    def _build_cost_map(self) -> Dict[str, float]:
        """Build SKU->cost lookup from ProductCost + ShopifyInventory.cost."""
        # ProductCost (nett master) keyed by upper SKU
        pc_rows = (
            self.db.query(
                func.upper(ProductCost.vendor_sku).label("sku"),
                ProductCost.nett_nett_cost_inc_gst,
            )
            .filter(
                ProductCost.vendor_sku.isnot(None),
                ProductCost.vendor_sku != "",
            )
            .all()
        )
        cost_map = {}
        for r in pc_rows:
            if r.nett_nett_cost_inc_gst is not None:
                cost_map[r.sku] = float(r.nett_nett_cost_inc_gst)

        # Fallback: ShopifyInventory.cost for SKUs not in ProductCost
        inv_cost_rows = (
            self.db.query(
                func.upper(ShopifyInventory.sku).label("sku"),
                ShopifyInventory.cost,
            )
            .filter(
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.sku != "",
                ShopifyInventory.cost.isnot(None),
            )
            .all()
        )
        for r in inv_cost_rows:
            if r.sku not in cost_map and r.cost is not None:
                cost_map[r.sku] = float(r.cost)

        return cost_map

    def generate_inventory_suggestions(self) -> Dict[str, Any]:
        """
        Generate inventory reorder suggestions based on sales velocity.

        Uses 30-day velocity for primary assessment and 7-day for trend detection.
        Delete-and-replace pattern (captures current state, not history).
        """
        # Delete existing suggestions (current-state snapshot)
        self.db.query(MLInventorySuggestion).delete()

        now = datetime.utcnow()
        cutoff_30d = date.today() - timedelta(days=30)
        cutoff_7d = date.today() - timedelta(days=7)

        # 30-day sales velocity by SKU
        velocity_30d = (
            self.db.query(
                ShopifyOrderItem.sku,
                ShopifyOrderItem.vendor,
                ShopifyOrderItem.title,
                func.sum(ShopifyOrderItem.quantity).label("units_sold_30d"),
            )
            .join(
                ShopifyOrder,
                ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
            )
            .filter(
                ShopifyOrderItem.order_date >= cutoff_30d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrderItem.sku.isnot(None),
                ShopifyOrderItem.sku != "",
            )
            .group_by(ShopifyOrderItem.sku)
            .all()
        )

        # 7-day sales velocity by SKU (for trend)
        velocity_7d_rows = (
            self.db.query(
                ShopifyOrderItem.sku,
                func.sum(ShopifyOrderItem.quantity).label("units_sold_7d"),
            )
            .join(
                ShopifyOrder,
                ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
            )
            .filter(
                ShopifyOrderItem.order_date >= cutoff_7d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrderItem.sku.isnot(None),
                ShopifyOrderItem.sku != "",
            )
            .group_by(ShopifyOrderItem.sku)
            .all()
        )
        velocity_7d = {r.sku.upper(): float(r.units_sold_7d or 0) for r in velocity_7d_rows}

        # Current inventory by SKU (active products only)
        active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
            ShopifyProduct.status == 'active'
        ).subquery()
        inventory_rows = (
            self.db.query(
                ShopifyInventory.sku,
                ShopifyInventory.vendor,
                ShopifyInventory.title,
                func.sum(ShopifyInventory.inventory_quantity).label("on_hand"),
            )
            .filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.sku != "",
            )
            .group_by(ShopifyInventory.sku)
            .all()
        )
        inventory_by_sku = {
            r.sku.upper(): {
                "on_hand": int(r.on_hand or 0),
                "vendor": r.vendor,
                "title": r.title,
            }
            for r in inventory_rows
        }

        # Build cost lookup for cost_missing flag
        cost_map = self._build_cost_map()

        # Check if we have enough snapshot days for offline inference
        snapshot_day_count = (
            self.db.query(func.count(func.distinct(InventoryDailySnapshot.snapshot_date)))
            .scalar()
        ) or 0
        offline_data_available = snapshot_day_count >= 2

        suggestions = []
        for row in velocity_30d:
            sku = row.sku.upper() if row.sku else row.sku
            if not sku:
                continue

            inv = inventory_by_sku.get(sku, {"on_hand": 0, "vendor": None, "title": None})
            units_on_hand = inv["on_hand"]
            units_sold_30d = float(row.units_sold_30d or 0)

            # Offline units inference
            offline_units = self._compute_offline_units(sku, 30) if offline_data_available else 0.0
            total_units_sold = units_sold_30d + offline_units
            daily_velocity_30d = total_units_sold / 30.0

            # 7-day velocity for trend
            units_sold_7d = velocity_7d.get(sku, 0)
            daily_velocity_7d = units_sold_7d / 7.0

            # Use higher of 30d/7d velocity for safety
            effective_velocity = max(daily_velocity_30d, daily_velocity_7d)

            # Trend detection
            if daily_velocity_30d > 0:
                ratio = daily_velocity_7d / daily_velocity_30d
                if ratio > 1.25:
                    velocity_trend = "increasing"
                elif ratio < 0.75:
                    velocity_trend = "decreasing"
                else:
                    velocity_trend = "stable"
            else:
                velocity_trend = "none"

            # Oversold flag — strictly negative inventory only
            oversold = units_on_hand < 0

            # Cost missing flag
            cost_missing = (sku.upper() not in cost_map)

            # Days of cover + suggestion logic
            if oversold:
                days_of_cover = 0.0
                suggestion = "reorder_now"
                urgency = "critical"
                reorder_qty = max(1, int(math.ceil(effective_velocity * 30))) if effective_velocity > 0 else 1
            elif effective_velocity == 0:
                # No demand — no_sales regardless of stock level
                days_of_cover = 999.0 if units_on_hand > 0 else 0.0
                suggestion = "no_sales"
                urgency = "ok"
                reorder_qty = None
            else:
                # effective_velocity > 0 guaranteed here
                days_of_cover = units_on_hand / effective_velocity

                if days_of_cover < 7:
                    suggestion = "reorder_now"
                    urgency = "critical"
                    reorder_qty = max(1, int(math.ceil(effective_velocity * 30 - units_on_hand)))
                elif days_of_cover < 14:
                    suggestion = "reorder_soon"
                    urgency = "warning"
                    reorder_qty = max(1, int(math.ceil(effective_velocity * 30 - units_on_hand)))
                elif days_of_cover <= 60:
                    suggestion = "adequate"
                    urgency = "ok"
                    reorder_qty = None
                else:
                    suggestion = "overstock"
                    urgency = "ok"
                    reorder_qty = None

            suggestions.append(
                MLInventorySuggestion(
                    sku=sku,
                    brand=row.vendor or inv["vendor"],
                    title=row.title or inv["title"],
                    units_on_hand=units_on_hand,
                    daily_sales_velocity=round(effective_velocity, 2),
                    velocity_trend=velocity_trend,
                    days_of_cover=round(days_of_cover, 1),
                    suggestion=suggestion,
                    reorder_quantity=reorder_qty,
                    urgency=urgency,
                    oversold=oversold,
                    cost_missing=cost_missing,
                    offline_units_30d=round(offline_units, 1),
                    generated_at=now,
                )
            )

        # --- Dead stock: inventory SKUs with zero 30d sales ---
        processed_skus = {s.sku for s in suggestions}

        for inv_sku, inv_data in inventory_by_sku.items():
            if inv_sku in processed_skus or not inv_sku:
                continue

            units_on_hand = inv_data["on_hand"]
            if units_on_hand == 0:
                continue  # zero stock + zero sales = not actionable

            inv_oversold = units_on_hand < 0
            inv_cost_missing = (inv_sku not in cost_map)

            if inv_oversold:
                ds_doc = 0.0
                ds_suggestion = "reorder_now"
                ds_urgency = "critical"
                ds_reorder_qty = 1
            else:
                ds_doc = 999.0
                ds_suggestion = "no_sales"
                ds_urgency = "ok"
                ds_reorder_qty = None

            suggestions.append(
                MLInventorySuggestion(
                    sku=inv_sku,
                    brand=inv_data["vendor"],
                    title=inv_data["title"],
                    units_on_hand=units_on_hand,
                    daily_sales_velocity=0.0,
                    velocity_trend="none",
                    days_of_cover=ds_doc,
                    suggestion=ds_suggestion,
                    reorder_quantity=ds_reorder_qty,
                    urgency=ds_urgency,
                    oversold=inv_oversold,
                    cost_missing=inv_cost_missing,
                    offline_units_30d=0.0,
                    generated_at=now,
                )
            )

        self.db.add_all(suggestions)
        self.db.commit()

        # Summary counts
        counts = {}
        for s in suggestions:
            counts[s.suggestion] = counts.get(s.suggestion, 0) + 1

        return {
            "total_skus_analyzed": len(suggestions),
            "by_suggestion": counts,
            "critical_count": sum(1 for s in suggestions if s.urgency == "critical"),
            "warning_count": sum(1 for s in suggestions if s.urgency == "warning"),
        }

    def get_inventory_suggestions(
        self,
        brand: Optional[str] = None,
        urgency: Optional[str] = None,
        suggestion: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve inventory suggestions from DB (scoped to latest run)."""
        latest_gen = self.db.query(func.max(MLInventorySuggestion.generated_at)).scalar()
        query = self.db.query(MLInventorySuggestion)
        if latest_gen:
            query = query.filter(MLInventorySuggestion.generated_at == latest_gen)

        if brand:
            query = query.filter(
                func.upper(MLInventorySuggestion.brand) == brand.upper()
            )
        if urgency:
            query = query.filter(MLInventorySuggestion.urgency == urgency)
        if suggestion:
            query = query.filter(MLInventorySuggestion.suggestion == suggestion)

        query = query.order_by(
            MLInventorySuggestion.days_of_cover.asc(),
            MLInventorySuggestion.daily_sales_velocity.desc(),
        )
        rows = query.all()

        return [
            {
                "sku": r.sku,
                "brand": r.brand,
                "title": r.title,
                "units_on_hand": r.units_on_hand,
                "daily_sales_velocity": r.daily_sales_velocity,
                "velocity_trend": r.velocity_trend,
                "days_of_cover": r.days_of_cover,
                "suggestion": r.suggestion,
                "reorder_quantity": r.reorder_quantity,
                "urgency": r.urgency,
                "generated_at": str(r.generated_at),
            }
            for r in rows
        ]

    # ─────────────────────────────────────────────
    # PIPELINE ORCHESTRATOR
    # ─────────────────────────────────────────────

    def run_daily_ml_pipeline(self) -> Dict[str, Any]:
        """
        Run all ML jobs in sequence.
        Called by the scheduler at 3am daily or manually via POST /ml/run.
        """
        results = {}

        try:
            logger.info("ML Pipeline: Starting forecasts...")
            results["forecasts"] = self.generate_forecasts(horizon=30)
        except Exception as e:
            logger.error(f"ML Pipeline: Forecasting failed: {e}")
            self.db.rollback()
            results["forecasts"] = {"error": str(e)}

        try:
            logger.info("ML Pipeline: Starting anomaly detection...")
            results["anomalies"] = self.detect_anomalies(history_days=90)
        except Exception as e:
            logger.error(f"ML Pipeline: Anomaly detection failed: {e}")
            self.db.rollback()
            results["anomalies"] = {"error": str(e)}

        try:
            logger.info("ML Pipeline: Capturing inventory snapshot...")
            self._capture_inventory_snapshot()
            self.db.commit()
        except Exception as e:
            logger.error(f"ML Pipeline: Inventory snapshot failed: {e}")
            self.db.rollback()

        try:
            logger.info("ML Pipeline: Starting inventory suggestions...")
            results["inventory"] = self.generate_inventory_suggestions()
        except Exception as e:
            logger.error(f"ML Pipeline: Inventory suggestions failed: {e}")
            self.db.rollback()
            results["inventory"] = {"error": str(e)}

        logger.info(f"ML Pipeline: Complete. Results: {results}")
        return results
