"""
Performance summary API
"""
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.base import get_db
from app.models.shopify import ShopifyOrder
from app.models.ga4_data import GA4DailySummary, GA4DailyEcommerce

router = APIRouter(prefix="/performance", tags=["performance"])


def _period_bounds(days: int):
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def _date_to_dt(d):
    return datetime.combine(d, datetime.min.time())


@router.get("/summary")
async def performance_summary(
    days: int = Query(7, description="Lookback window in days"),
    db: Session = Depends(get_db),
):
    """Return revenue/orders/sessions/CR for current and prior period."""
    start_date, end_date = _period_bounds(days)
    prior_start = start_date - timedelta(days=days)
    prior_end = start_date - timedelta(days=1)

    start_dt = _date_to_dt(start_date)
    end_dt = _date_to_dt(end_date + timedelta(days=1))
    prior_start_dt = _date_to_dt(prior_start)
    prior_end_dt = _date_to_dt(prior_end + timedelta(days=1))

    price_expr = func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)

    def shopify_metrics(start_dt, end_dt):
        revenue = db.query(func.sum(price_expr)).filter(
            ShopifyOrder.created_at >= start_dt,
            ShopifyOrder.created_at < end_dt,
            ShopifyOrder.cancelled_at.is_(None),
            ShopifyOrder.financial_status != 'voided'
        ).scalar() or 0
        orders = db.query(func.count(ShopifyOrder.id)).filter(
            ShopifyOrder.created_at >= start_dt,
            ShopifyOrder.created_at < end_dt,
            ShopifyOrder.cancelled_at.is_(None),
            ShopifyOrder.financial_status != 'voided'
        ).scalar() or 0
        aov = (revenue / orders) if orders else 0
        return float(revenue), int(orders), float(aov)

    def ga4_sessions(start_d, end_d):
        sessions = db.query(func.sum(GA4DailySummary.sessions)).filter(
            GA4DailySummary.date >= start_d,
            GA4DailySummary.date <= end_d
        ).scalar() or 0
        return int(sessions)

    def ga4_purchases(start_d, end_d):
        purchases = db.query(func.sum(GA4DailyEcommerce.ecommerce_purchases)).filter(
            GA4DailyEcommerce.date >= start_d,
            GA4DailyEcommerce.date <= end_d
        ).scalar() or 0
        return int(purchases)

    cur_rev, cur_orders, cur_aov = shopify_metrics(start_dt, end_dt)
    prev_rev, prev_orders, prev_aov = shopify_metrics(prior_start_dt, prior_end_dt)

    cur_sessions = ga4_sessions(start_date, end_date)
    prev_sessions = ga4_sessions(prior_start, prior_end)

    cur_purchases = ga4_purchases(start_date, end_date)
    prev_purchases = ga4_purchases(prior_start, prior_end)

    cur_cr = (cur_purchases / cur_sessions * 100) if cur_sessions else 0
    prev_cr = (prev_purchases / prev_sessions * 100) if prev_sessions else 0

    def pct_change(current, previous):
        if previous == 0:
            return None
        return round(((current - previous) / previous) * 100, 1)

    return {
        "period_days": days,
        "period_start": start_date.isoformat(),
        "period_end": end_date.isoformat(),
        "kpis": {
            "revenue": round(cur_rev, 2),
            "orders": cur_orders,
            "aov": round(cur_aov, 2),
            "sessions": cur_sessions,
            "conversion_rate": round(cur_cr, 2),
        },
        "deltas": {
            "revenue_pct": pct_change(cur_rev, prev_rev),
            "orders_pct": pct_change(cur_orders, prev_orders),
            "aov_pct": pct_change(cur_aov, prev_aov),
            "sessions_pct": pct_change(cur_sessions, prev_sessions),
            "conversion_rate_pct": pct_change(cur_cr, prev_cr),
        },
        "previous": {
            "revenue": round(prev_rev, 2),
            "orders": prev_orders,
            "aov": round(prev_aov, 2),
            "sessions": prev_sessions,
            "conversion_rate": round(prev_cr, 2),
        }
    }
