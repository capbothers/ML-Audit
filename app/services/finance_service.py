"""
Finance Service — Monthly P&L Calculation

Pulls revenue from Shopify, COGS from ProductCost/OrderItems,
ad spend from Google Ads, and operating expenses from BusinessExpense
to produce a full monthly P&L statement.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, extract, and_, or_

from app.models.business_expense import BusinessExpense, MonthlyPL
from app.models.shopify import ShopifyOrder, ShopifyOrderItem
from app.models.google_ads_data import GoogleAdsCampaign
from app.utils.logger import log


class FinanceService:
    def __init__(self, db: Session):
        self.db = db

    def calculate_monthly_pl(self, month: date) -> Dict:
        """
        Calculate and store P&L for a given month.

        Args:
            month: First day of the month (e.g., date(2026, 1, 1))

        Returns:
            Dict with full P&L breakdown
        """
        # Normalize to first of month
        month = date(month.year, month.month, 1)

        # Calculate next month for date range
        if month.month == 12:
            next_month = date(month.year + 1, 1, 1)
        else:
            next_month = date(month.year, month.month + 1, 1)

        log.info(f"Calculating P&L for {month.strftime('%Y-%m')}")

        # ── Revenue from Shopify ──
        revenue_data = self._calculate_revenue(month, next_month)

        # ── COGS from ShopifyOrderItem ──
        cogs = self._calculate_cogs(month, next_month)

        # ── Ad Spend from Google Ads ──
        ad_spend = self._calculate_ad_spend(month, next_month)

        # ── Operating Expenses from BusinessExpense ──
        expenses = self._get_expense_breakdown(month)

        # ── Compute P&L ──
        gross_revenue = revenue_data['gross_revenue']
        refunds = revenue_data['refunds']
        net_revenue = gross_revenue - refunds
        total_orders = revenue_data['total_orders']

        gross_margin = net_revenue - cogs
        gross_margin_pct = (gross_margin / net_revenue * 100) if net_revenue > 0 else Decimal('0')

        # Sum all operating expenses
        expense_total = (
            ad_spend
            + expenses.get('payroll', Decimal('0'))
            + expenses.get('rent', Decimal('0'))
            + expenses.get('shipping', Decimal('0'))
            + expenses.get('utilities', Decimal('0'))
            + expenses.get('insurance', Decimal('0'))
            + expenses.get('software', Decimal('0'))
            + expenses.get('marketing_other', Decimal('0'))
            + expenses.get('professional_services', Decimal('0'))
            + expenses.get('other', Decimal('0'))
        )

        operating_profit = gross_margin - expense_total
        operating_margin_pct = (operating_profit / net_revenue * 100) if net_revenue > 0 else Decimal('0')

        net_profit = operating_profit  # Same for now (no interest/tax)
        net_margin_pct = operating_margin_pct

        # Overhead per order = (all expenses except COGS) / total orders
        # This is used by the ads dashboard to allocate overhead to campaigns
        overhead_expenses = expense_total - ad_spend  # Exclude ad spend (already in campaign cost)
        overhead_per_order = (overhead_expenses / total_orders) if total_orders > 0 else None

        avg_order_value = (net_revenue / total_orders) if total_orders > 0 else None

        # ── Upsert MonthlyPL ──
        existing = self.db.query(MonthlyPL).filter(MonthlyPL.month == month).first()
        if not existing:
            existing = MonthlyPL(month=month)
            self.db.add(existing)

        existing.gross_revenue = gross_revenue
        existing.refunds = refunds
        existing.net_revenue = net_revenue
        existing.cogs = cogs
        existing.gross_margin = gross_margin
        existing.gross_margin_pct = gross_margin_pct
        existing.ad_spend = ad_spend
        existing.payroll = expenses.get('payroll', Decimal('0'))
        existing.rent = expenses.get('rent', Decimal('0'))
        existing.shipping = expenses.get('shipping', Decimal('0'))
        existing.utilities = expenses.get('utilities', Decimal('0'))
        existing.insurance = expenses.get('insurance', Decimal('0'))
        existing.software = expenses.get('software', Decimal('0'))
        existing.marketing_other = expenses.get('marketing_other', Decimal('0'))
        existing.professional_services = expenses.get('professional_services', Decimal('0'))
        existing.other_expenses = expenses.get('other', Decimal('0'))
        existing.total_expenses = expense_total
        existing.operating_profit = operating_profit
        existing.operating_margin_pct = operating_margin_pct
        existing.net_profit = net_profit
        existing.net_margin_pct = net_margin_pct
        existing.total_orders = total_orders
        existing.avg_order_value = avg_order_value
        existing.overhead_per_order = overhead_per_order
        existing.generated_at = datetime.utcnow()
        existing.updated_at = datetime.utcnow()

        self.db.commit()

        log.info(f"P&L for {month.strftime('%Y-%m')}: revenue=${net_revenue}, profit=${net_profit}, orders={total_orders}")

        return self._pl_to_dict(existing)

    def get_pl_summary(self, months: int = 6) -> List[Dict]:
        """Get monthly P&L data for the last N months."""
        results = (
            self.db.query(MonthlyPL)
            .order_by(MonthlyPL.month.desc())
            .limit(months)
            .all()
        )
        return [self._pl_to_dict(r) for r in reversed(results)]

    def get_pl_for_month(self, month: date) -> Optional[Dict]:
        """Get P&L for a specific month."""
        month = date(month.year, month.month, 1)
        result = self.db.query(MonthlyPL).filter(MonthlyPL.month == month).first()
        if not result:
            return None
        return self._pl_to_dict(result)

    def get_overhead_per_order(self, month: date) -> Optional[Decimal]:
        """
        Get overhead cost per order for a given month.

        Used by AdSpendProcessor to calculate fully-loaded campaign profitability.
        Returns None if no P&L data exists for the month.
        """
        month = date(month.year, month.month, 1)
        result = self.db.query(MonthlyPL.overhead_per_order).filter(
            MonthlyPL.month == month
        ).scalar()
        return Decimal(str(result)) if result is not None else None

    def get_latest_overhead_per_order(self) -> Optional[Decimal]:
        """Get the most recent overhead per order with actual expenses.

        Skips months with $0 overhead (no expenses uploaded).
        """
        result = (
            self.db.query(MonthlyPL)
            .filter(
                MonthlyPL.overhead_per_order.isnot(None),
                MonthlyPL.overhead_per_order > 0,
            )
            .order_by(MonthlyPL.month.desc())
            .first()
        )
        if result:
            return Decimal(str(result.overhead_per_order))
        return None

    def get_finance_summary(self) -> Dict:
        """High-level finance summary: current month vs prior month."""
        today = date.today()
        current_month = date(today.year, today.month, 1)

        if current_month.month == 1:
            prior_month = date(current_month.year - 1, 12, 1)
        else:
            prior_month = date(current_month.year, current_month.month - 1, 1)

        current = self.get_pl_for_month(current_month)
        prior = self.get_pl_for_month(prior_month)

        def _pct_change(current_val, prior_val):
            if not prior_val or prior_val == 0:
                return None
            return round((current_val - prior_val) / abs(prior_val) * 100, 1)

        summary = {
            "current_month": current_month.strftime('%Y-%m'),
            "prior_month": prior_month.strftime('%Y-%m'),
            "current": current,
            "prior": prior,
            "has_expenses": self._has_expenses(current_month),
        }

        if current and prior:
            summary["changes"] = {
                "revenue_change_pct": _pct_change(
                    current.get('net_revenue', 0), prior.get('net_revenue', 0)
                ),
                "profit_change_pct": _pct_change(
                    current.get('operating_profit', 0), prior.get('operating_profit', 0)
                ),
                "orders_change_pct": _pct_change(
                    current.get('total_orders', 0), prior.get('total_orders', 0)
                ),
                "ad_spend_change_pct": _pct_change(
                    current.get('ad_spend', 0), prior.get('ad_spend', 0)
                ),
            }

        return summary

    def get_overhead_trend(self, months: int = 6) -> List[Dict]:
        """Get overhead per order trend for the last N months."""
        results = (
            self.db.query(
                MonthlyPL.month,
                MonthlyPL.overhead_per_order,
                MonthlyPL.total_orders,
                MonthlyPL.total_expenses,
                MonthlyPL.ad_spend
            )
            .filter(MonthlyPL.overhead_per_order.isnot(None))
            .order_by(MonthlyPL.month.desc())
            .limit(months)
            .all()
        )
        return [
            {
                "month": r.month.strftime('%Y-%m'),
                "overhead_per_order": float(r.overhead_per_order) if r.overhead_per_order else None,
                "total_orders": r.total_orders,
                "operating_expenses": float((r.total_expenses or 0) - (r.ad_spend or 0)),
            }
            for r in reversed(results)
        ]

    # ── Private methods ──

    def _calculate_revenue(self, month_start: date, month_end: date) -> Dict:
        """Calculate revenue from Shopify orders for a month."""
        # Filter valid orders
        valid_statuses = ['paid', 'partially_refunded', 'partially_paid']

        result = self.db.query(
            func.count(ShopifyOrder.id).label('total_orders'),
            func.coalesce(func.sum(ShopifyOrder.total_price), 0).label('gross_revenue'),
            func.coalesce(func.sum(ShopifyOrder.total_refunded), 0).label('refunds'),
        ).filter(
            ShopifyOrder.created_at >= datetime.combine(month_start, datetime.min.time()),
            ShopifyOrder.created_at < datetime.combine(month_end, datetime.min.time()),
            ShopifyOrder.cancelled_at.is_(None),
            ShopifyOrder.financial_status.in_(valid_statuses),
        ).first()

        return {
            'total_orders': result.total_orders or 0,
            'gross_revenue': Decimal(str(result.gross_revenue or 0)),
            'refunds': Decimal(str(result.refunds or 0)),
        }

    def _calculate_cogs(self, month_start: date, month_end: date) -> Decimal:
        """Calculate COGS from ShopifyOrderItem cost_per_item."""
        result = self.db.query(
            func.coalesce(
                func.sum(ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                0
            )
        ).filter(
            ShopifyOrderItem.order_date >= datetime.combine(month_start, datetime.min.time()),
            ShopifyOrderItem.order_date < datetime.combine(month_end, datetime.min.time()),
            ShopifyOrderItem.cost_per_item.isnot(None),
            ShopifyOrderItem.cost_per_item > 0,
        ).scalar()

        return Decimal(str(result or 0))

    def _calculate_ad_spend(self, month_start: date, month_end: date) -> Decimal:
        """Calculate Google Ads spend for a month."""
        result = self.db.query(
            func.coalesce(func.sum(GoogleAdsCampaign.cost_micros), 0)
        ).filter(
            GoogleAdsCampaign.date >= month_start.isoformat(),
            GoogleAdsCampaign.date < month_end.isoformat(),
        ).scalar()

        # Convert from micros to dollars
        return Decimal(str(result or 0)) / Decimal('1000000')

    def _get_expense_breakdown(self, month: date) -> Dict[str, Decimal]:
        """Get expenses grouped by category for a month."""
        results = self.db.query(
            BusinessExpense.category,
            func.sum(BusinessExpense.amount).label('total')
        ).filter(
            BusinessExpense.month == month
        ).group_by(
            BusinessExpense.category
        ).all()

        return {r.category: Decimal(str(r.total or 0)) for r in results}

    def _has_expenses(self, month: date) -> bool:
        """Check if any expenses exist for a month."""
        count = self.db.query(func.count(BusinessExpense.id)).filter(
            BusinessExpense.month == month
        ).scalar()
        return (count or 0) > 0

    def _pl_to_dict(self, pl: MonthlyPL) -> Dict:
        """Convert MonthlyPL model to dict."""
        def _safe_float(val):
            if val is None:
                return None
            return float(val)

        return {
            "month": pl.month.strftime('%Y-%m'),
            "gross_revenue": _safe_float(pl.gross_revenue),
            "refunds": _safe_float(pl.refunds),
            "net_revenue": _safe_float(pl.net_revenue),
            "cogs": _safe_float(pl.cogs),
            "gross_margin": _safe_float(pl.gross_margin),
            "gross_margin_pct": _safe_float(pl.gross_margin_pct),
            "ad_spend": _safe_float(pl.ad_spend),
            "payroll": _safe_float(pl.payroll),
            "rent": _safe_float(pl.rent),
            "shipping": _safe_float(pl.shipping),
            "utilities": _safe_float(pl.utilities),
            "insurance": _safe_float(pl.insurance),
            "software": _safe_float(pl.software),
            "marketing_other": _safe_float(pl.marketing_other),
            "professional_services": _safe_float(pl.professional_services),
            "other_expenses": _safe_float(pl.other_expenses),
            "total_expenses": _safe_float(pl.total_expenses),
            "operating_profit": _safe_float(pl.operating_profit),
            "operating_margin_pct": _safe_float(pl.operating_margin_pct),
            "net_profit": _safe_float(pl.net_profit),
            "net_margin_pct": _safe_float(pl.net_margin_pct),
            "total_orders": pl.total_orders,
            "avg_order_value": _safe_float(pl.avg_order_value),
            "overhead_per_order": _safe_float(pl.overhead_per_order),
            "generated_at": pl.generated_at.isoformat() if pl.generated_at else None,
        }
