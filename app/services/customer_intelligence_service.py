"""
Customer Intelligence Service

ML-powered customer analytics: RFM scoring, cohort retention,
churn risk, product affinity, geo distribution.
All computed on-the-fly from ShopifyCustomer + ShopifyOrder data.
"""
import logging
import math
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import func, case, distinct, and_, or_, desc, asc

from app.models.shopify import ShopifyCustomer, ShopifyOrder, ShopifyOrderItem, ShopifyRefund

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RFM segment definitions
# ---------------------------------------------------------------------------
SEGMENT_DEFINITIONS = {
    "Champions": {
        "color": "#c49a4a",
        "description": "Best customers. Bought recently, buy often, spend the most.",
        "action": "Reward them. Offer exclusive early access and VIP programs.",
    },
    "Loyal": {
        "color": "#1a7a3a",
        "description": "Spend good money often. Responsive to promotions.",
        "action": "Upsell higher-value products. Ask for reviews and referrals.",
    },
    "Potential Loyalist": {
        "color": "#1f6f6b",
        "description": "Recent customers with above-average frequency.",
        "action": "Offer loyalty programs and recommend related products.",
    },
    "Promising": {
        "color": "#3a8fd6",
        "description": "Recent shoppers who haven't bought much yet.",
        "action": "Create brand awareness. Offer free trials or samples.",
    },
    "New Customers": {
        "color": "#6b5ce7",
        "description": "Bought recently for the first time.",
        "action": "Provide onboarding support. Start building the relationship.",
    },
    "Need Attention": {
        "color": "#c49a4a",
        "description": "Above-average recency, frequency and monetary but slipping.",
        "action": "Reactivate with limited-time offers and personalised recommendations.",
    },
    "About to Sleep": {
        "color": "#e88c3a",
        "description": "Below-average recency and frequency. Losing them.",
        "action": "Share valuable resources. Recommend popular products. Offer discounts.",
    },
    "At Risk": {
        "color": "#b5342a",
        "description": "Spent big money, purchased often — but long time ago.",
        "action": "Send personalised reactivation campaigns. Offer renewals or new products.",
    },
    "Hibernating": {
        "color": "#8b5e3c",
        "description": "Last purchase was long ago. Low frequency and spend.",
        "action": "Offer deep discounts. Recreate brand value. Likely to lose if not engaged.",
    },
    "Lost": {
        "color": "#6b7280",
        "description": "Lowest recency, frequency, and monetary scores.",
        "action": "Revive with aggressive win-back campaign or accept and focus elsewhere.",
    },
}


class CustomerIntelligenceService:
    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # RFM scoring
    # ------------------------------------------------------------------

    def _compute_rfm_scores(self):
        """
        Assign R / F / M quintile scores (1-5) to every customer
        who has placed at least one order.

        Recency is computed from ShopifyOrder.created_at (last order date)
        since ShopifyCustomer.days_since_last_order is not populated.

        Returns list of dicts:
            [{email, first_name, last_name, orders_count, total_spent,
              days_since_last_order, r, f, m, segment}, ...]
        """
        try:
            # Build recency map from orders (days since last order per customer)
            now = datetime.utcnow()
            last_order_sub = (
                self.db.query(
                    ShopifyOrder.customer_email,
                    func.max(ShopifyOrder.created_at).label("last_order_date"),
                )
                .filter(ShopifyOrder.customer_email.isnot(None))
                .filter(ShopifyOrder.customer_email != "")
                .group_by(ShopifyOrder.customer_email)
                .subquery()
            )

            rows = (
                self.db.query(
                    ShopifyCustomer.email,
                    ShopifyCustomer.first_name,
                    ShopifyCustomer.last_name,
                    ShopifyCustomer.orders_count,
                    ShopifyCustomer.total_spent,
                    ShopifyCustomer.created_at,
                    ShopifyCustomer.default_address_city,
                    ShopifyCustomer.default_address_province,
                    ShopifyCustomer.default_address_country,
                    last_order_sub.c.last_order_date,
                )
                .outerjoin(last_order_sub, ShopifyCustomer.email == last_order_sub.c.customer_email)
                .filter(ShopifyCustomer.orders_count > 0)
                .filter(ShopifyCustomer.total_spent > 0)
                .all()
            )

            if not rows:
                return []

            customers = []
            for r in rows:
                last_order = r.last_order_date
                if last_order:
                    if isinstance(last_order, str):
                        last_order = datetime.strptime(last_order[:19], "%Y-%m-%d %H:%M:%S")
                    days_since = max(0, (now - last_order).days)
                    last_order_str = str(last_order)[:10]
                else:
                    days_since = 9999
                    last_order_str = None

                customers.append({
                    "email": r.email or "",
                    "first_name": r.first_name or "",
                    "last_name": r.last_name or "",
                    "orders_count": int(r.orders_count or 0),
                    "total_spent": float(r.total_spent or 0),
                    "days_since_last_order": days_since,
                    "last_order_date": last_order_str,
                    "created_at": str(r.created_at)[:10] if r.created_at else None,
                    "city": r.default_address_city or "",
                    "province": r.default_address_province or "",
                    "country": r.default_address_country or "",
                })

            # Assign quintiles
            self._assign_quintiles(customers, "days_since_last_order", "r", reverse=True)
            self._assign_quintiles(customers, "orders_count", "f", reverse=False)
            self._assign_quintiles(customers, "total_spent", "m", reverse=False)

            # Assign segments
            for c in customers:
                c["segment"] = self._assign_rfm_segment(c["r"], c["f"], c["m"])

            return customers
        except Exception as e:
            logger.error(f"RFM scoring failed: {e}")
            return []

    def _assign_quintiles(self, customers, field, score_key, reverse=False):
        """
        Sort customers by `field` and assign quintile scores 1-5.
        reverse=True means lower values get HIGHER scores (for recency).
        """
        sorted_custs = sorted(customers, key=lambda c: c[field], reverse=reverse)
        n = len(sorted_custs)
        for i, c in enumerate(sorted_custs):
            quintile = min(int(i / n * 5) + 1, 5)
            c[score_key] = quintile

    @staticmethod
    def _assign_rfm_segment(r, f, m):
        """Map R/F/M quintile scores to a named segment."""
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        if f >= 3 and m >= 3:
            return "Loyal"
        if r >= 3 and f >= 2 and m >= 2:
            return "Potential Loyalist"
        if r >= 4 and f <= 2:
            return "Promising"
        if r >= 4 and f == 1:
            return "New Customers"
        if r >= 2 and r <= 3 and f >= 2 and m >= 2:
            return "Need Attention"
        if r >= 2 and r <= 3 and f <= 2:
            return "About to Sleep"
        if r <= 2 and f >= 3:
            return "At Risk"
        if r <= 2 and f <= 2 and m >= 2:
            return "Hibernating"
        return "Lost"

    # ------------------------------------------------------------------
    # Dashboard orchestrator
    # ------------------------------------------------------------------

    def get_dashboard(self):
        """Return the complete payload for all 4 tabs."""
        try:
            rfm_data = self._compute_rfm_scores()
            self._rfm_cache = rfm_data  # cache for sub-methods
            kpis = self._compute_overview_kpis(rfm_data)
            rfm_distribution = self._compute_rfm_distribution(rfm_data)
            revenue_by_segment = self._compute_revenue_by_segment(rfm_data)
            acquisition_trend = self._compute_acquisition_trend()
            top_customers = self._get_top_customers(rfm_data, limit=20)

            rfm_segments = self._get_rfm_segment_summary(rfm_data)

            cohort_retention = self._compute_cohort_retention()
            repeat_curve = self._compute_repeat_curve()
            days_between = self._compute_days_between_distribution()
            retention_kpis = self._compute_retention_kpis(rfm_data)

            gateway_products = self._compute_gateway_products()
            brand_affinity = self._compute_brand_affinity()
            geo_distribution = self._compute_geo_distribution(rfm_data)

            pulse = self._compute_pulse(rfm_data, kpis, rfm_distribution)

            return {
                "pulse": pulse,
                "overview_kpis": kpis,
                "rfm_distribution": rfm_distribution,
                "revenue_by_segment": revenue_by_segment,
                "acquisition_trend": acquisition_trend,
                "top_customers": top_customers,
                "rfm_segments": rfm_segments,
                "cohort_retention": cohort_retention,
                "repeat_curve": repeat_curve,
                "days_between_distribution": days_between,
                "retention_kpis": retention_kpis,
                "gateway_products": gateway_products,
                "brand_affinity": brand_affinity,
                "geo_distribution": geo_distribution,
            }
        except Exception as e:
            logger.error(f"Dashboard generation failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Pulse narrative
    # ------------------------------------------------------------------

    def _compute_pulse(self, rfm_data, kpis, rfm_dist):
        """Generate narrative sentence + status chip."""
        try:
            total = kpis.get("total_customers", 0)
            active = kpis.get("active_customers", 0)
            at_risk = sum(1 for c in rfm_data if c["segment"] in ("At Risk", "Hibernating", "Lost"))
            champions = sum(1 for c in rfm_data if c["segment"] == "Champions")
            champ_rev = sum(c["total_spent"] for c in rfm_data if c["segment"] == "Champions")
            total_rev = sum(c["total_spent"] for c in rfm_data) or 1
            champ_pct = round(champ_rev / total_rev * 100)

            risk_pct = round(at_risk / len(rfm_data) * 100) if rfm_data else 0

            if risk_pct >= 40:
                status = "Critical"
            elif risk_pct >= 20:
                status = "At Risk"
            elif risk_pct >= 10:
                status = "Stable"
            else:
                status = "Thriving"

            narrative = (
                f"{active:,} active customers out of {total:,} total — "
                f"{champions:,} Champions driving {champ_pct}% of revenue"
            )
            if at_risk > 0:
                narrative += f", but {at_risk:,} customers at risk of churning"

            return {
                "narrative": narrative,
                "status": status,
                "pro_narrative": (
                    f"Customer base: {total:,} total, {active:,} active (90d). "
                    f"RFM analysis identifies {champions:,} Champions ({champ_pct}% of revenue). "
                    f"{at_risk:,} customers classified At Risk/Hibernating/Lost ({risk_pct}% of purchasers). "
                    f"Repeat rate: {kpis.get('repeat_rate', 0)}%. "
                    f"Avg LTV: ${kpis.get('avg_ltv', 0):,.2f}."
                ),
            }
        except Exception as e:
            logger.error(f"Pulse computation failed: {e}")
            return {"narrative": "Customer intelligence data loading…", "status": "Loading", "pro_narrative": ""}

    # ------------------------------------------------------------------
    # KPIs
    # ------------------------------------------------------------------

    def _compute_overview_kpis(self, rfm_data):
        """8 KPI values for the pulse tab."""
        try:
            total_customers = self.db.query(func.count(ShopifyCustomer.id)).scalar() or 0

            # Active = ordered within last 90 days (computed from rfm_data since
            # days_since_last_order is derived from ShopifyOrder)
            active_customers = sum(1 for c in rfm_data if c["days_since_last_order"] <= 90)

            with_orders = [c for c in rfm_data if c["orders_count"] > 0]
            avg_orders = round(sum(c["orders_count"] for c in with_orders) / len(with_orders), 1) if with_orders else 0
            avg_ltv = round(sum(c["total_spent"] for c in with_orders) / len(with_orders), 2) if with_orders else 0

            # New this month
            now = datetime.utcnow()
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            new_this_month = (
                self.db.query(func.count(ShopifyCustomer.id))
                .filter(ShopifyCustomer.created_at >= month_start)
                .scalar()
            ) or 0

            # Repeat rate
            repeat_customers = (
                self.db.query(func.count(ShopifyCustomer.id))
                .filter(ShopifyCustomer.orders_count >= 2)
                .scalar()
            ) or 0
            total_with_orders = (
                self.db.query(func.count(ShopifyCustomer.id))
                .filter(ShopifyCustomer.orders_count >= 1)
                .scalar()
            ) or 0
            repeat_rate = round(repeat_customers / total_with_orders * 100, 1) if total_with_orders else 0

            # At-risk count
            at_risk_count = sum(1 for c in rfm_data if c["segment"] in ("At Risk", "Hibernating"))

            # Avg days between orders (for repeat customers, exclude 9999 sentinel)
            repeat_data = [c for c in rfm_data if c["orders_count"] >= 2 and c["days_since_last_order"] < 9999]
            if repeat_data:
                avg_days_between = round(
                    sum(c["days_since_last_order"] for c in repeat_data) / len(repeat_data), 0
                )
            else:
                avg_days_between = 0

            return {
                "total_customers": total_customers,
                "active_customers": active_customers,
                "avg_orders": avg_orders,
                "avg_ltv": avg_ltv,
                "new_this_month": new_this_month,
                "repeat_rate": repeat_rate,
                "at_risk_count": at_risk_count,
                "avg_days_between": int(avg_days_between),
            }
        except Exception as e:
            logger.error(f"KPI computation failed: {e}")
            return {k: 0 for k in [
                "total_customers", "active_customers", "avg_orders", "avg_ltv",
                "new_this_month", "repeat_rate", "at_risk_count", "avg_days_between"
            ]}

    # ------------------------------------------------------------------
    # RFM distribution + segment summary
    # ------------------------------------------------------------------

    def _compute_rfm_distribution(self, rfm_data):
        """Segment counts and percentages for stacked bar."""
        segment_counts = defaultdict(int)
        for c in rfm_data:
            segment_counts[c["segment"]] += 1

        total = len(rfm_data) or 1
        segments = []
        for name, defn in SEGMENT_DEFINITIONS.items():
            count = segment_counts.get(name, 0)
            segments.append({
                "segment": name,
                "count": count,
                "pct": round(count / total * 100, 1),
                "color": defn["color"],
            })

        return sorted(segments, key=lambda s: s["count"], reverse=True)

    def _compute_revenue_by_segment(self, rfm_data):
        """Revenue totals per segment for bar chart."""
        rev = defaultdict(float)
        for c in rfm_data:
            rev[c["segment"]] += c["total_spent"]

        results = []
        for name, defn in SEGMENT_DEFINITIONS.items():
            results.append({
                "segment": name,
                "revenue": round(rev.get(name, 0), 2),
                "color": defn["color"],
            })
        return sorted(results, key=lambda s: s["revenue"], reverse=True)

    def _get_rfm_segment_summary(self, rfm_data):
        """Detailed per-segment metrics for the RFM tab table."""
        segments = defaultdict(list)
        for c in rfm_data:
            segments[c["segment"]].append(c)

        total_customers = len(rfm_data) or 1
        result = []
        for name, defn in SEGMENT_DEFINITIONS.items():
            custs = segments.get(name, [])
            count = len(custs)
            if count == 0:
                result.append({
                    "segment": name,
                    "count": 0,
                    "pct": 0,
                    "avg_orders": 0,
                    "avg_spend": 0,
                    "avg_recency": 0,
                    "total_revenue": 0,
                    "color": defn["color"],
                    "description": defn["description"],
                    "action": defn["action"],
                })
                continue

            result.append({
                "segment": name,
                "count": count,
                "pct": round(count / total_customers * 100, 1),
                "avg_orders": round(sum(c["orders_count"] for c in custs) / count, 1),
                "avg_spend": round(sum(c["total_spent"] for c in custs) / count, 2),
                "avg_recency": round(sum(c["days_since_last_order"] for c in custs) / count, 0),
                "total_revenue": round(sum(c["total_spent"] for c in custs), 2),
                "color": defn["color"],
                "description": defn["description"],
                "action": defn["action"],
            })

        return sorted(result, key=lambda s: s["total_revenue"], reverse=True)

    # ------------------------------------------------------------------
    # Acquisition trend
    # ------------------------------------------------------------------

    def _compute_acquisition_trend(self):
        """New customers per month for last 12 months."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=365)
            rows = (
                self.db.query(
                    func.strftime("%Y-%m", ShopifyCustomer.created_at).label("month"),
                    func.count(ShopifyCustomer.id).label("count"),
                )
                .filter(ShopifyCustomer.created_at >= cutoff)
                .group_by(func.strftime("%Y-%m", ShopifyCustomer.created_at))
                .order_by(func.strftime("%Y-%m", ShopifyCustomer.created_at))
                .all()
            )
            return [{"month": r.month, "count": r.count} for r in rows]
        except Exception as e:
            logger.error(f"Acquisition trend failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Top customers
    # ------------------------------------------------------------------

    def _get_top_customers(self, rfm_data, limit=20):
        """Top N customers by total spend."""
        sorted_custs = sorted(rfm_data, key=lambda c: c["total_spent"], reverse=True)[:limit]
        return [
            {
                "name": f"{c['first_name']} {c['last_name']}".strip() or c["email"],
                "email": c["email"],
                "orders": c["orders_count"],
                "total_spent": c["total_spent"],
                "last_order": c["last_order_date"],
                "segment": c["segment"],
                "days_since": c["days_since_last_order"],
            }
            for c in sorted_custs
        ]

    # ------------------------------------------------------------------
    # Cohort retention
    # ------------------------------------------------------------------

    def _compute_cohort_retention(self):
        """
        Monthly cohort retention heatmap.
        Cohort = month of customer's first order.
        For each cohort month, count distinct customers who ordered
        in month+0, month+1, ..., month+11.
        """
        try:
            # Get first order month per customer
            first_order_sub = (
                self.db.query(
                    ShopifyOrder.customer_email,
                    func.min(ShopifyOrder.created_at).label("first_order"),
                )
                .filter(ShopifyOrder.customer_email.isnot(None))
                .filter(ShopifyOrder.customer_email != "")
                .group_by(ShopifyOrder.customer_email)
                .subquery()
            )

            # Get all orders with cohort month info
            rows = (
                self.db.query(
                    func.strftime("%Y-%m", first_order_sub.c.first_order).label("cohort"),
                    ShopifyOrder.customer_email,
                    func.strftime("%Y-%m", ShopifyOrder.created_at).label("order_month"),
                )
                .join(first_order_sub, ShopifyOrder.customer_email == first_order_sub.c.customer_email)
                .filter(ShopifyOrder.customer_email.isnot(None))
                .filter(ShopifyOrder.customer_email != "")
                .all()
            )

            if not rows:
                return {"cohorts": [], "max_months": 0}

            # Build cohort data
            cohort_orders = defaultdict(lambda: defaultdict(set))
            for r in rows:
                cohort_orders[r.cohort][r.order_month].add(r.customer_email)

            # Convert to retention percentages
            cohorts = []
            sorted_cohort_keys = sorted(cohort_orders.keys())[-12:]  # Last 12 cohorts

            for cohort_month in sorted_cohort_keys:
                months_data = cohort_orders[cohort_month]
                cohort_size = len(months_data.get(cohort_month, set()))
                if cohort_size == 0:
                    continue

                # Parse cohort month
                cy, cm = int(cohort_month[:4]), int(cohort_month[5:7])

                retention = []
                for offset in range(12):
                    # Calculate target month
                    m = cm + offset
                    y = cy + (m - 1) // 12
                    m = ((m - 1) % 12) + 1
                    target = f"{y:04d}-{m:02d}"

                    active = len(months_data.get(target, set()))
                    pct = round(active / cohort_size * 100, 1)
                    retention.append(pct)

                cohorts.append({
                    "cohort": cohort_month,
                    "size": cohort_size,
                    "retention": retention,
                })

            return {"cohorts": cohorts, "max_months": 12}
        except Exception as e:
            logger.error(f"Cohort retention failed: {e}")
            return {"cohorts": [], "max_months": 0}

    # ------------------------------------------------------------------
    # Repeat purchase curve
    # ------------------------------------------------------------------

    def _compute_repeat_curve(self):
        """
        % of customers who made at least N orders (N=1..10).
        Uses ShopifyCustomer.orders_count.
        """
        try:
            total = (
                self.db.query(func.count(ShopifyCustomer.id))
                .filter(ShopifyCustomer.orders_count >= 1)
                .scalar()
            ) or 1

            curve = []
            for n in range(1, 11):
                count = (
                    self.db.query(func.count(ShopifyCustomer.id))
                    .filter(ShopifyCustomer.orders_count >= n)
                    .scalar()
                ) or 0
                curve.append({
                    "order_number": n,
                    "customers": count,
                    "pct": round(count / total * 100, 1),
                })

            return curve
        except Exception as e:
            logger.error(f"Repeat curve failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Days between orders distribution
    # ------------------------------------------------------------------

    def _compute_days_between_distribution(self):
        """
        Histogram of days since last order for customers with 2+ orders.
        Uses rfm_data (computed from ShopifyOrder) instead of empty DB field.
        Called via get_dashboard which passes rfm_data; standalone fallback too.
        """
        # This is called by get_dashboard; we cache rfm_data on self for reuse
        rfm_data = getattr(self, '_rfm_cache', None)
        if rfm_data is None:
            rfm_data = self._compute_rfm_scores()

        try:
            buckets = [
                {"label": "0-30", "min": 0, "max": 30, "count": 0},
                {"label": "31-60", "min": 31, "max": 60, "count": 0},
                {"label": "61-90", "min": 61, "max": 90, "count": 0},
                {"label": "91-180", "min": 91, "max": 180, "count": 0},
                {"label": "181-365", "min": 181, "max": 365, "count": 0},
                {"label": "365+", "min": 366, "max": 999999, "count": 0},
            ]

            for c in rfm_data:
                if c["orders_count"] < 2:
                    continue
                days = c["days_since_last_order"]
                if days >= 9999:
                    continue
                for b in buckets:
                    if b["min"] <= days <= b["max"]:
                        b["count"] += 1
                        break

            return [{"label": b["label"], "count": b["count"]} for b in buckets]
        except Exception as e:
            logger.error(f"Days between distribution failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Retention KPIs
    # ------------------------------------------------------------------

    def _compute_retention_kpis(self, rfm_data):
        """30d/90d retention, churn rate, avg orders to loyal."""
        try:
            total_with_orders = len(rfm_data) or 1

            # Use rfm_data which has computed days_since_last_order from ShopifyOrder
            retained_30d = sum(
                1 for c in rfm_data
                if c["orders_count"] >= 2 and c["days_since_last_order"] <= 30
            )
            retained_90d = sum(
                1 for c in rfm_data
                if c["orders_count"] >= 2 and c["days_since_last_order"] <= 90
            )

            churned = sum(
                1 for c in rfm_data
                if c["segment"] in ("Lost", "Hibernating")
            )
            churn_rate = round(churned / len(rfm_data) * 100, 1) if rfm_data else 0

            # Avg orders for loyal customers (3+ orders)
            loyal = [c for c in rfm_data if c["orders_count"] >= 3]
            avg_orders_loyal = round(
                sum(c["orders_count"] for c in loyal) / len(loyal), 1
            ) if loyal else 0

            return {
                "retained_30d": retained_30d,
                "retention_30d_pct": round(retained_30d / total_with_orders * 100, 1),
                "retained_90d": retained_90d,
                "retention_90d_pct": round(retained_90d / total_with_orders * 100, 1),
                "churn_rate": churn_rate,
                "avg_orders_loyal": avg_orders_loyal,
            }
        except Exception as e:
            logger.error(f"Retention KPIs failed: {e}")
            return {k: 0 for k in [
                "retained_30d", "retention_30d_pct", "retained_90d",
                "retention_90d_pct", "churn_rate", "avg_orders_loyal"
            ]}

    # ------------------------------------------------------------------
    # Gateway products
    # ------------------------------------------------------------------

    def _compute_gateway_products(self, limit=20):
        """
        Products most commonly purchased in a customer's first order.
        Plus repeat-purchase rate for those customers.
        """
        try:
            # Find first order per customer
            first_order_sub = (
                self.db.query(
                    ShopifyOrder.customer_email,
                    func.min(ShopifyOrder.shopify_order_id).label("first_order_id"),
                )
                .filter(ShopifyOrder.customer_email.isnot(None))
                .filter(ShopifyOrder.customer_email != "")
                .group_by(ShopifyOrder.customer_email)
                .subquery()
            )

            # Get items from first orders
            first_items = (
                self.db.query(
                    ShopifyOrderItem.title,
                    ShopifyOrderItem.vendor,
                    ShopifyOrderItem.sku,
                    first_order_sub.c.customer_email,
                )
                .join(
                    first_order_sub,
                    ShopifyOrderItem.shopify_order_id == first_order_sub.c.first_order_id,
                )
                .all()
            )

            if not first_items:
                return []

            # Count products in first orders
            product_customers = defaultdict(set)
            for item in first_items:
                key = item.title or item.sku or "Unknown"
                product_customers[key].add(item.customer_email)

            # Get repeat buyers (orders_count >= 2) set
            repeat_emails = set(
                r.email for r in
                self.db.query(ShopifyCustomer.email)
                .filter(ShopifyCustomer.orders_count >= 2)
                .all()
                if r.email
            )

            # Get avg LTV for customers of each product
            customer_ltv = {}
            ltv_rows = (
                self.db.query(ShopifyCustomer.email, ShopifyCustomer.total_spent)
                .filter(ShopifyCustomer.orders_count >= 1)
                .all()
            )
            for r in ltv_rows:
                if r.email:
                    customer_ltv[r.email] = float(r.total_spent or 0)

            results = []
            for product, customers in product_customers.items():
                count = len(customers)
                if count < 3:
                    continue
                repeat_count = len(customers & repeat_emails)
                repeat_rate = round(repeat_count / count * 100, 1)
                avg_ltv = round(
                    sum(customer_ltv.get(e, 0) for e in customers) / count, 2
                )
                results.append({
                    "product": product,
                    "first_order_count": count,
                    "repeat_rate": repeat_rate,
                    "avg_customer_ltv": avg_ltv,
                })

            return sorted(results, key=lambda x: x["first_order_count"], reverse=True)[:limit]
        except Exception as e:
            logger.error(f"Gateway products failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Brand affinity (co-purchase)
    # ------------------------------------------------------------------

    def _compute_brand_affinity(self, limit=20):
        """
        Brands commonly purchased by the same customer.
        Uses ShopifyOrderItem.vendor grouped by customer email.
        """
        try:
            # Get brands per customer
            rows = (
                self.db.query(
                    ShopifyOrder.customer_email,
                    ShopifyOrderItem.vendor,
                )
                .join(ShopifyOrderItem, ShopifyOrder.shopify_order_id == ShopifyOrderItem.shopify_order_id)
                .filter(ShopifyOrder.customer_email.isnot(None))
                .filter(ShopifyOrder.customer_email != "")
                .filter(ShopifyOrderItem.vendor.isnot(None))
                .filter(ShopifyOrderItem.vendor != "")
                .all()
            )

            if not rows:
                return []

            # Build customer → brands mapping
            customer_brands = defaultdict(set)
            brand_customers = defaultdict(set)
            for r in rows:
                customer_brands[r.customer_email].add(r.vendor)
                brand_customers[r.vendor].add(r.customer_email)

            total_customers = len(customer_brands)
            if total_customers == 0:
                return []

            # Count co-purchases
            pair_counts = defaultdict(int)
            for email, brands in customer_brands.items():
                brands_list = sorted(brands)
                for i in range(len(brands_list)):
                    for j in range(i + 1, len(brands_list)):
                        pair_counts[(brands_list[i], brands_list[j])] += 1

            # Compute lift
            results = []
            for (brand_a, brand_b), count in pair_counts.items():
                if count < 3:
                    continue
                prob_a = len(brand_customers[brand_a]) / total_customers
                prob_b = len(brand_customers[brand_b]) / total_customers
                expected = prob_a * prob_b * total_customers
                lift = round(count / expected, 2) if expected > 0 else 0

                results.append({
                    "brand_a": brand_a,
                    "brand_b": brand_b,
                    "co_purchase_count": count,
                    "lift": lift,
                })

            return sorted(results, key=lambda x: x["co_purchase_count"], reverse=True)[:limit]
        except Exception as e:
            logger.error(f"Brand affinity failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Geographic distribution
    # ------------------------------------------------------------------

    def _compute_geo_distribution(self, rfm_data):
        """Top cities/states from ShopifyCustomer address fields."""
        try:
            rows = (
                self.db.query(
                    ShopifyCustomer.default_address_city,
                    ShopifyCustomer.default_address_province,
                    ShopifyCustomer.default_address_country,
                    func.count(ShopifyCustomer.id).label("customer_count"),
                    func.sum(ShopifyCustomer.total_spent).label("total_revenue"),
                    func.avg(ShopifyCustomer.orders_count).label("avg_orders"),
                )
                .filter(ShopifyCustomer.default_address_city.isnot(None))
                .filter(ShopifyCustomer.default_address_city != "")
                .group_by(
                    ShopifyCustomer.default_address_city,
                    ShopifyCustomer.default_address_province,
                    ShopifyCustomer.default_address_country,
                )
                .order_by(desc("total_revenue"))
                .limit(25)
                .all()
            )

            return [
                {
                    "city": r.default_address_city or "",
                    "state": r.default_address_province or "",
                    "country": r.default_address_country or "",
                    "customer_count": r.customer_count,
                    "total_revenue": round(float(r.total_revenue or 0), 2),
                    "avg_orders": round(float(r.avg_orders or 0), 1),
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Geo distribution failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Customer detail (drill-down modal)
    # ------------------------------------------------------------------

    def get_customer_detail(self, email: str):
        """Full customer drill-down for modal."""
        try:
            customer = (
                self.db.query(ShopifyCustomer)
                .filter(ShopifyCustomer.email == email)
                .first()
            )
            if not customer:
                return None

            # RFM scores for this customer
            rfm_data = self._compute_rfm_scores()
            cust_rfm = next((c for c in rfm_data if c["email"] == email), None)

            # Recent orders
            orders = (
                self.db.query(
                    ShopifyOrder.order_number,
                    ShopifyOrder.total_price,
                    ShopifyOrder.created_at,
                    ShopifyOrder.financial_status,
                    ShopifyOrder.fulfillment_status,
                )
                .filter(ShopifyOrder.customer_email == email)
                .order_by(desc(ShopifyOrder.created_at))
                .limit(10)
                .all()
            )

            # Compute ML flags
            flags = []
            if cust_rfm:
                if cust_rfm["segment"] == "Champions":
                    flags.append("Champion")
                if cust_rfm["segment"] == "Loyal":
                    flags.append("Loyal")
                if cust_rfm["segment"] in ("At Risk", "Hibernating"):
                    flags.append("At Risk")
                if cust_rfm["segment"] == "Lost":
                    flags.append("Churned")
                if cust_rfm["total_spent"] > 1000:
                    flags.append("High Value")
                if cust_rfm["segment"] in ("New Customers", "Promising"):
                    flags.append("New")

            total_spent = float(customer.total_spent or 0)
            orders_count = int(customer.orders_count or 0)
            aov = round(total_spent / orders_count, 2) if orders_count > 0 else 0

            months_since = 0
            if customer.created_at:
                delta = datetime.utcnow() - customer.created_at
                months_since = max(1, delta.days // 30)

            return {
                "email": customer.email,
                "name": f"{customer.first_name or ''} {customer.last_name or ''}".strip(),
                "segment": cust_rfm["segment"] if cust_rfm else "Unknown",
                "r_score": cust_rfm["r"] if cust_rfm else 0,
                "f_score": cust_rfm["f"] if cust_rfm else 0,
                "m_score": cust_rfm["m"] if cust_rfm else 0,
                "total_orders": orders_count,
                "total_spent": total_spent,
                "avg_order_value": aov,
                "days_since_last_order": int(customer.days_since_last_order or 0),
                "first_order_date": str(customer.created_at)[:10] if customer.created_at else None,
                "last_order_date": str(customer.last_order_date)[:10] if customer.last_order_date else None,
                "customer_since_months": months_since,
                "city": customer.default_address_city or "",
                "state": customer.default_address_province or "",
                "country": customer.default_address_country or "",
                "flags": flags,
                "orders": [
                    {
                        "order_number": o.order_number,
                        "total": float(o.total_price or 0),
                        "date": str(o.created_at)[:10] if o.created_at else "",
                        "status": o.financial_status or "",
                        "fulfillment": o.fulfillment_status or "",
                    }
                    for o in orders
                ],
            }
        except Exception as e:
            logger.error(f"Customer detail failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_customers(self, query: str, limit: int = 20):
        """Search customers by name or email."""
        try:
            pattern = f"%{query}%"
            rows = (
                self.db.query(
                    ShopifyCustomer.email,
                    ShopifyCustomer.first_name,
                    ShopifyCustomer.last_name,
                    ShopifyCustomer.orders_count,
                    ShopifyCustomer.total_spent,
                    ShopifyCustomer.days_since_last_order,
                    ShopifyCustomer.last_order_date,
                )
                .filter(
                    or_(
                        ShopifyCustomer.email.ilike(pattern),
                        ShopifyCustomer.first_name.ilike(pattern),
                        ShopifyCustomer.last_name.ilike(pattern),
                    )
                )
                .order_by(desc(ShopifyCustomer.total_spent))
                .limit(limit)
                .all()
            )

            return [
                {
                    "email": r.email or "",
                    "name": f"{r.first_name or ''} {r.last_name or ''}".strip(),
                    "orders": int(r.orders_count or 0),
                    "total_spent": float(r.total_spent or 0),
                    "days_since": int(r.days_since_last_order or 0),
                    "last_order": str(r.last_order_date)[:10] if r.last_order_date else None,
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Customer search failed: {e}")
            return []

    # ------------------------------------------------------------------
    # RFM segments endpoint
    # ------------------------------------------------------------------

    def get_rfm_segments(self):
        """Detailed RFM segment data with actions."""
        rfm_data = self._compute_rfm_scores()
        return self._get_rfm_segment_summary(rfm_data)

    # ------------------------------------------------------------------
    # Cohort data endpoint
    # ------------------------------------------------------------------

    def get_cohort_data(self):
        """Cohort retention heatmap data."""
        return self._compute_cohort_retention()

    # ------------------------------------------------------------------
    # Brand affinity endpoint
    # ------------------------------------------------------------------

    def get_brand_affinity(self):
        """Brand co-purchase analysis."""
        return self._compute_brand_affinity()
