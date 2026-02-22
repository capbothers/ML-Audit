"""
Stock Worthiness Service

Identifies order-in products (inventory_quantity <= 0 on active SKUs) that
should be moved into warehouse stock, ranked by a 0-100 composite score.
Also flags currently-stocked items that should be destocked.
"""
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from decimal import Decimal

from sqlalchemy import func, desc, and_, case
from sqlalchemy.orm import Session

from app.models.shopify import (
    ShopifyInventory, ShopifyOrderItem, ShopifyOrder, ShopifyProduct,
)
from app.models.ml_intelligence import MLInventorySuggestion, InventoryDailySnapshot
from app.models.product_cost import ProductCost

logger = logging.getLogger(__name__)

_BUCKET_LABELS = {
    "strong": "Strong candidate",
    "consider": "Worth considering",
    "marginal": "Marginal",
    "not_recommended": "Not recommended",
}


class StockWorthinessService:
    def __init__(self, db: Session):
        self.db = db

    # ── helpers ──────────────────────────────────────

    def _cost_subqueries(self):
        """Pre-aggregated cost per SKU (one row each) — same pattern as
        InventoryIntelligenceService._cost_subqueries()."""
        pc_cost = (
            self.db.query(
                func.upper(ProductCost.vendor_sku).label("sku"),
                func.max(ProductCost.nett_nett_cost_inc_gst).label("cost"),
            )
            .filter(ProductCost.vendor_sku.isnot(None), ProductCost.vendor_sku != "")
            .group_by(func.upper(ProductCost.vendor_sku))
            .subquery()
        )
        inv_cost = (
            self.db.query(
                func.upper(ShopifyInventory.sku).label("sku"),
                func.max(ShopifyInventory.cost).label("cost"),
            )
            .filter(ShopifyInventory.sku.isnot(None), ShopifyInventory.sku != "")
            .group_by(func.upper(ShopifyInventory.sku))
            .subquery()
        )
        return pc_cost, inv_cost

    def _has_offline_snapshot_data(self) -> bool:
        """Offline inference requires at least two distinct snapshot days."""
        snapshot_day_count = (
            self.db.query(func.count(func.distinct(InventoryDailySnapshot.snapshot_date)))
            .scalar()
        ) or 0
        return snapshot_day_count >= 2

    def _compute_offline_units_map(self, skus_upper: List[str], days: int = 30) -> Dict[str, float]:
        """Infer offline units from inventory depletion that exceeds online sales."""
        if not skus_upper:
            return {}

        cutoff = date.today() - timedelta(days=days)
        sku_set = sorted({s.strip().upper() for s in skus_upper if s and s.strip()})
        if not sku_set:
            return {}

        snapshots = (
            self.db.query(
                func.upper(InventoryDailySnapshot.sku).label("sku_upper"),
                InventoryDailySnapshot.snapshot_date,
                InventoryDailySnapshot.quantity,
            )
            .filter(
                func.upper(InventoryDailySnapshot.sku).in_(sku_set),
                InventoryDailySnapshot.snapshot_date >= cutoff,
            )
            .order_by(func.upper(InventoryDailySnapshot.sku), InventoryDailySnapshot.snapshot_date)
            .all()
        )
        if not snapshots:
            return {sku: 0.0 for sku in sku_set}

        online_rows = (
            self.db.query(
                func.upper(ShopifyOrderItem.sku).label("sku_upper"),
                func.date(ShopifyOrderItem.order_date).label("order_day"),
                func.sum(ShopifyOrderItem.quantity).label("qty"),
            )
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                func.upper(ShopifyOrderItem.sku).in_(sku_set),
                ShopifyOrderItem.order_date >= cutoff,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
            )
            .group_by(func.upper(ShopifyOrderItem.sku), func.date(ShopifyOrderItem.order_date))
            .all()
        )
        online_by_day = {
            (r.sku_upper, r.order_day): float(r.qty or 0)
            for r in online_rows
        }

        offline_map: Dict[str, float] = {sku: 0.0 for sku in sku_set}
        prev_by_sku: Dict[str, Any] = {}
        for row in snapshots:
            prev = prev_by_sku.get(row.sku_upper)
            if prev is not None:
                inventory_delta = int(prev.quantity or 0) - int(row.quantity or 0)
                if inventory_delta > 0:
                    online_sold = online_by_day.get((row.sku_upper, row.snapshot_date), 0.0)
                    if inventory_delta > online_sold:
                        offline_map[row.sku_upper] += inventory_delta - online_sold
            prev_by_sku[row.sku_upper] = row

        return {sku: round(units, 1) for sku, units in offline_map.items()}

    @staticmethod
    def _p90(values: List[float]) -> float:
        """Return the 90th-percentile value (or 1.0 to avoid division by zero)."""
        if not values:
            return 1.0
        s = sorted(values)
        idx = int(len(s) * 0.9)
        idx = min(idx, len(s) - 1)
        return max(s[idx], 0.001)

    @staticmethod
    def _trend_label(vel_7d: float, vel_30d: float) -> str:
        if vel_30d <= 0:
            return "none" if vel_7d <= 0 else "increasing"
        ratio = vel_7d / vel_30d
        if ratio > 1.25:
            return "increasing"
        if ratio < 0.75:
            return "decreasing"
        return "stable"

    @staticmethod
    def _trend_score(label: str) -> float:
        return {"increasing": 10, "stable": 5, "decreasing": 2, "none": 0}.get(label, 0)

    @staticmethod
    def _bucket(score: float) -> str:
        if score >= 75:
            return "strong"
        if score >= 50:
            return "consider"
        if score >= 25:
            return "marginal"
        return "not_recommended"

    # ── main dashboard ───────────────────────────────

    def get_dashboard(
        self,
        min_score: int = 0,
        vendor: Optional[str] = None,
    ) -> Dict[str, Any]:
        # 1. Active product IDs
        active_pids = (
            self.db.query(ShopifyProduct.shopify_product_id)
            .filter(ShopifyProduct.status == "active")
        )

        # 2. Order-in SKUs (inventory <= 0 on active products)
        order_in_rows = (
            self.db.query(
                ShopifyInventory.sku,
                ShopifyInventory.vendor,
                ShopifyInventory.title,
            )
            .filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.inventory_quantity <= 0,
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.sku != "",
            )
            .all()
        )
        # Build lookup by upper SKU
        oi_map: Dict[str, Dict] = {}
        for r in order_in_rows:
            key = r.sku.strip().upper()
            if key and key not in oi_map:
                oi_map[key] = {"sku": r.sku, "vendor": r.vendor or "", "title": r.title or ""}

        total_order_in = len(oi_map)
        if total_order_in == 0:
            return {
                "kpis": {
                    "total_order_in": 0,
                    "worth_stocking": 0,
                    "strong_candidates": 0,
                    "estimated_capital": 0,
                    "revenue_at_risk": 0,
                },
                "candidates": [],
                "score_distribution": {"strong": 0, "consider": 0, "marginal": 0, "not_recommended": 0},
                "vendors": [],
            }

        # 3. 30-day online sales per SKU
        cutoff_30d = date.today() - timedelta(days=30)
        sales_30d = (
            self.db.query(
                func.upper(ShopifyOrderItem.sku).label("sku_upper"),
                func.sum(ShopifyOrderItem.quantity).label("units_sold"),
                func.sum(ShopifyOrderItem.total_price).label("revenue"),
                func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label("order_count"),
                func.avg(ShopifyOrderItem.price).label("avg_price"),
            )
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                ShopifyOrderItem.order_date >= cutoff_30d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrderItem.sku.isnot(None),
                ShopifyOrderItem.sku != "",
            )
            .group_by(func.upper(ShopifyOrderItem.sku))
            .all()
        )
        sales_map = {r.sku_upper: r for r in sales_30d}

        # 4. 7-day online sales per SKU (for velocity trend)
        cutoff_7d = date.today() - timedelta(days=7)
        sales_7d = (
            self.db.query(
                func.upper(ShopifyOrderItem.sku).label("sku_upper"),
                func.sum(ShopifyOrderItem.quantity).label("units_sold_7d"),
            )
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                ShopifyOrderItem.order_date >= cutoff_7d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                ShopifyOrderItem.sku.isnot(None),
                ShopifyOrderItem.sku != "",
            )
            .group_by(func.upper(ShopifyOrderItem.sku))
            .all()
        )
        vel7_map = {r.sku_upper: float(r.units_sold_7d or 0) / 7.0 for r in sales_7d}

        # 4b. Offline movement inferred from snapshot depletion (showrooms/counter)
        offline_data_available = self._has_offline_snapshot_data()
        offline_30_map: Dict[str, float] = {}
        offline_7_map: Dict[str, float] = {}
        if offline_data_available:
            sku_keys = list(oi_map.keys())
            offline_30_map = self._compute_offline_units_map(sku_keys, days=30)
            offline_7_map = self._compute_offline_units_map(sku_keys, days=7)

        # 5. Cost lookup
        cost_rows = (
            self.db.query(
                func.upper(ProductCost.vendor_sku).label("sku_upper"),
                func.max(ProductCost.nett_nett_cost_inc_gst).label("cost"),
            )
            .filter(ProductCost.vendor_sku.isnot(None), ProductCost.vendor_sku != "")
            .group_by(func.upper(ProductCost.vendor_sku))
            .all()
        )
        cost_map: Dict[str, float] = {r.sku_upper: float(r.cost or 0) for r in cost_rows if r.cost}

        # Fallback: ShopifyInventory.cost
        inv_cost_rows = (
            self.db.query(
                func.upper(ShopifyInventory.sku).label("sku_upper"),
                func.max(ShopifyInventory.cost).label("cost"),
            )
            .filter(
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.sku != "",
                ShopifyInventory.cost.isnot(None),
                ShopifyInventory.cost > 0,
            )
            .group_by(func.upper(ShopifyInventory.sku))
            .all()
        )
        for r in inv_cost_rows:
            if r.sku_upper not in cost_map and r.cost:
                cost_map[r.sku_upper] = float(r.cost)

        # 6. Build raw candidate list (order-in SKUs with online and/or inferred offline demand)
        raw: List[Dict] = []
        for sku_upper, info in oi_map.items():
            s = sales_map.get(sku_upper)
            online_units = float(s.units_sold or 0) if s else 0.0
            offline_units = float(offline_30_map.get(sku_upper, 0.0)) if offline_data_available else 0.0
            total_units = online_units + offline_units
            if total_units <= 0:
                continue
            revenue = float(s.revenue or 0) if s else 0.0
            orders = int(s.order_count or 0) if s else 0
            avg_price = float(s.avg_price or 0) if s else 0.0
            vel_30 = total_units / 30.0
            vel_7_online = vel7_map.get(sku_upper, 0.0)
            vel_7_offline = (float(offline_7_map.get(sku_upper, 0.0)) / 7.0) if offline_data_available else 0.0
            vel_7 = vel_7_online + vel_7_offline
            trend = self._trend_label(vel_7, vel_30)
            unit_cost = cost_map.get(sku_upper, 0)
            cost_missing = unit_cost <= 0
            margin_pct = ((avg_price - unit_cost) / avg_price * 100) if avg_price > 0 and unit_cost > 0 else None
            capital_30d = unit_cost * vel_30 * 30 if unit_cost > 0 else 0

            raw.append({
                "sku": info["sku"],
                "vendor": info["vendor"],
                "title": info["title"],
                "online_units_30d": int(online_units),
                "offline_units_30d": round(offline_units, 1),
                "velocity_30d": round(vel_30, 3),
                "velocity_7d": round(vel_7, 3),
                "units_sold_30d": int(round(total_units)),
                "revenue_30d": round(revenue, 2),
                "order_count_30d": orders,
                "avg_price": round(avg_price, 2),
                "unit_cost": round(unit_cost, 2),
                "cost_missing": cost_missing,
                "margin_pct": round(margin_pct, 1) if margin_pct is not None else None,
                "capital_30d": round(capital_30d, 2),
                "trend": trend,
            })

        if not raw:
            return {
                "kpis": {
                    "total_order_in": total_order_in,
                    "worth_stocking": 0,
                    "strong_candidates": 0,
                    "estimated_capital": 0,
                    "revenue_at_risk": 0,
                },
                "candidates": [],
                "score_distribution": {"strong": 0, "consider": 0, "marginal": 0, "not_recommended": 0},
                "vendors": sorted(set(info["vendor"] for info in oi_map.values() if info["vendor"])),
            }

        # 7. Compute p90 normalization values
        p90_vel = self._p90([c["velocity_30d"] for c in raw if c["velocity_30d"] > 0])
        p90_freq = self._p90([float(c["order_count_30d"]) for c in raw if c["order_count_30d"] > 0])
        p90_rev = self._p90([c["revenue_30d"] for c in raw if c["revenue_30d"] > 0])
        capitals = [c["capital_30d"] for c in raw if c["capital_30d"] > 0]
        p90_cap = self._p90(capitals) if capitals else 1.0

        # 8. Score each candidate
        for c in raw:
            pts = 0.0
            # Velocity (25 pts)
            vel_pts = min(25, (c["velocity_30d"] / p90_vel) * 25) if c["velocity_30d"] > 0 else 0
            # Margin (20 pts)
            if c["margin_pct"] is not None:
                margin_pts = min(20, max(0, (c["margin_pct"] / 50) * 20))
            else:
                margin_pts = 10  # neutral when cost unknown
            # Frequency (20 pts)
            freq_pts = min(20, (c["order_count_30d"] / p90_freq) * 20) if c["order_count_30d"] > 0 else 0
            # Revenue (15 pts)
            rev_pts = min(15, (c["revenue_30d"] / p90_rev) * 15) if c["revenue_30d"] > 0 else 0
            # Trend (10 pts)
            trend_pts = self._trend_score(c["trend"])
            # Capital efficiency (10 pts) — lower capital = higher score
            if c["capital_30d"] > 0:
                cap_pts = min(10, max(0, (1 - c["capital_30d"] / p90_cap) * 10))
            else:
                cap_pts = 5  # neutral when cost unknown

            pts = vel_pts + margin_pts + freq_pts + rev_pts + trend_pts + cap_pts
            c["score"] = round(pts, 1)
            c["score_breakdown"] = {
                "velocity": round(vel_pts, 1),
                "margin": round(margin_pts, 1),
                "frequency": round(freq_pts, 1),
                "revenue": round(rev_pts, 1),
                "trend": round(trend_pts, 1),
                "capital_efficiency": round(cap_pts, 1),
            }
            c["bucket"] = self._bucket(pts)

        # 9. Filter — KPIs and score_distribution are intentionally computed
        # from the full unfiltered `raw` list so they represent global context.
        # Only the `candidates` list respects min_score/vendor filters.
        candidates = [c for c in raw if c["score"] >= min_score]
        if vendor:
            v_upper = vendor.upper()
            candidates = [c for c in candidates if c["vendor"].upper() == v_upper]

        candidates.sort(key=lambda c: c["score"], reverse=True)

        # Add rank
        for i, c in enumerate(candidates, 1):
            c["rank"] = i

        # 10. KPIs + distribution (global scope — across ALL raw, before filters)
        dist = {"strong": 0, "consider": 0, "marginal": 0, "not_recommended": 0}
        for c in raw:
            dist[c["bucket"]] += 1

        worth_stocking = [c for c in raw if c["score"] >= 50]
        strong = [c for c in raw if c["score"] >= 75]

        kpis = {
            "total_order_in": total_order_in,
            "worth_stocking": len(worth_stocking),
            "strong_candidates": len(strong),
            "estimated_capital": round(sum(c["capital_30d"] for c in worth_stocking), 2),
            "revenue_at_risk": round(sum(c["revenue_30d"] for c in worth_stocking), 2),
        }

        vendors = sorted(set(c["vendor"] for c in raw if c["vendor"]))

        return {
            "kpis": kpis,
            "candidates": candidates,
            "score_distribution": dist,
            "vendors": vendors,
            "offline_data_available": offline_data_available,
        }

    # ── destock review ───────────────────────────────

    def get_destock_review(self) -> Dict[str, Any]:
        """Stocked items that should be reconsidered (no sales / extreme overstock)."""
        pc_cost, inv_cost = self._cost_subqueries()
        effective_cost = func.coalesce(pc_cost.c.cost, inv_cost.c.cost)

        base_q = (
            self.db.query(
                MLInventorySuggestion.sku,
                MLInventorySuggestion.brand,
                MLInventorySuggestion.title,
                MLInventorySuggestion.units_on_hand,
                MLInventorySuggestion.daily_sales_velocity,
                MLInventorySuggestion.days_of_cover,
                MLInventorySuggestion.suggestion,
                effective_cost.label("unit_cost"),
            )
            .outerjoin(pc_cost, func.upper(MLInventorySuggestion.sku) == pc_cost.c.sku)
            .outerjoin(inv_cost, func.upper(MLInventorySuggestion.sku) == inv_cost.c.sku)
            .filter(
                MLInventorySuggestion.units_on_hand > 0,
                MLInventorySuggestion.suggestion.in_(["no_sales", "overstock"]),
            )
            .order_by(desc(
                func.coalesce(MLInventorySuggestion.units_on_hand, 0)
                * func.coalesce(effective_cost, 0)
            ))
        )

        # Fetch ALL rows for accurate KPI aggregation
        rows = base_q.all()

        offline_data_available = self._has_offline_snapshot_data()
        offline_30_map: Dict[str, float] = {}
        if offline_data_available:
            offline_30_map = self._compute_offline_units_map(
                [r.sku.strip().upper() for r in rows if r.sku],
                days=30,
            )

        # Build full items list + compute KPIs from complete dataset
        items = []
        total_capital = 0.0
        total_doc = 0.0
        excluded_offline_active = 0
        for r in rows:
            sku_upper = r.sku.strip().upper() if r.sku else ""
            offline_units_30d = float(offline_30_map.get(sku_upper, 0.0)) if offline_data_available else 0.0
            if offline_units_30d > 0:
                excluded_offline_active += 1
                continue
            uc = float(r.unit_cost or 0)
            on_hand = int(r.units_on_hand or 0)
            value = round(on_hand * uc, 2)
            total_capital += value
            doc = float(r.days_of_cover or 0)
            total_doc += doc
            items.append({
                "sku": r.sku,
                "brand": r.brand or "",
                "title": r.title or "",
                "on_hand": on_hand,
                "velocity": round(float(r.daily_sales_velocity or 0), 3),
                "days_cover": round(doc, 1),
                "suggestion": r.suggestion,
                "offline_units_30d": round(offline_units_30d, 1),
                "unit_cost": round(uc, 2),
                "value_tied_up": value,
            })

        # KPIs reflect the full dataset (not capped)
        kpis = {
            "destock_candidates": len(items),
            "capital_locked": round(total_capital, 2),
            "avg_days_cover": round(total_doc / len(items), 1) if items else 0,
            "excluded_offline_active": excluded_offline_active,
            "offline_data_available": offline_data_available,
        }

        return {"kpis": kpis, "items": items}

    # ── SKU detail ───────────────────────────────────

    def get_sku_detail(self, sku: str) -> Dict[str, Any]:
        """Deep-dive on a single SKU for the modal."""
        sku_upper = sku.strip().upper()

        # Inventory info
        inv = (
            self.db.query(ShopifyInventory)
            .filter(func.upper(ShopifyInventory.sku) == sku_upper)
            .first()
        )

        # Cost
        pc = (
            self.db.query(ProductCost)
            .filter(func.upper(ProductCost.vendor_sku) == sku_upper)
            .first()
        )
        unit_cost = float(pc.get_active_cost()) if pc and pc.get_active_cost() else 0
        if not unit_cost and inv and inv.cost:
            unit_cost = float(inv.cost)

        # 30-day daily sales for sparkline
        cutoff_30d = date.today() - timedelta(days=30)
        daily_sales = (
            self.db.query(
                func.date(ShopifyOrderItem.order_date).label("day"),
                func.sum(ShopifyOrderItem.quantity).label("units"),
                func.sum(ShopifyOrderItem.total_price).label("revenue"),
            )
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                func.upper(ShopifyOrderItem.sku) == sku_upper,
                ShopifyOrderItem.order_date >= cutoff_30d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
            )
            .group_by(func.date(ShopifyOrderItem.order_date))
            .order_by(func.date(ShopifyOrderItem.order_date))
            .all()
        )

        # Build sparkline (fill gaps with 0)
        spark_map = {str(r.day): {"units": int(r.units or 0), "revenue": float(r.revenue or 0)} for r in daily_sales}
        sparkline = []
        for i in range(30):
            d = str(date.today() - timedelta(days=29 - i))
            entry = spark_map.get(d, {"units": 0, "revenue": 0})
            sparkline.append({"date": d, **entry})

        # Offline movement inferred from inventory snapshots
        offline_data_available = self._has_offline_snapshot_data()
        offline_30 = 0.0
        offline_7 = 0.0
        if offline_data_available:
            offline_30 = float(self._compute_offline_units_map([sku_upper], days=30).get(sku_upper, 0.0))
            offline_7 = float(self._compute_offline_units_map([sku_upper], days=7).get(sku_upper, 0.0))

        # Aggregates
        total_units = sum(d["units"] for d in sparkline)
        total_revenue = sum(d["revenue"] for d in sparkline)
        total_units_all_channels = total_units + offline_30
        vel_30 = total_units_all_channels / 30.0
        vel_7 = (sum(d["units"] for d in sparkline[-7:]) + offline_7) / 7.0
        avg_price = total_revenue / total_units if total_units > 0 else 0
        margin_pct = ((avg_price - unit_cost) / avg_price * 100) if avg_price > 0 and unit_cost > 0 else None
        capital_30d = unit_cost * vel_30 * 30 if unit_cost > 0 else 0
        trend = self._trend_label(vel_7, vel_30)

        # Order count
        order_count = (
            self.db.query(func.count(func.distinct(ShopifyOrderItem.shopify_order_id)))
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                func.upper(ShopifyOrderItem.sku) == sku_upper,
                ShopifyOrderItem.order_date >= cutoff_30d,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
            )
            .scalar()
        ) or 0

        # Recent orders (last 10, paid only — consistent with 30d metrics)
        recent = (
            self.db.query(
                ShopifyOrderItem.order_date,
                ShopifyOrder.order_number,
                ShopifyOrderItem.quantity,
                ShopifyOrderItem.price,
                ShopifyOrderItem.total_price,
            )
            .join(ShopifyOrder, ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id)
            .filter(
                func.upper(ShopifyOrderItem.sku) == sku_upper,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
            )
            .order_by(desc(ShopifyOrderItem.order_date))
            .limit(10)
            .all()
        )

        # Build recommendation text
        rec_parts = []
        if vel_30 > 0:
            rec_parts.append(
                f"Sells {vel_30:.1f} units/day ({int(round(total_units_all_channels))} in 30 days across channels)."
            )
        if offline_data_available and offline_30 > 0:
            rec_parts.append(f"Estimated {offline_30:.0f} units sold offline (showrooms/counter) in 30 days.")
        if margin_pct is not None:
            rec_parts.append(f"Gross margin {margin_pct:.0f}%.")
        if capital_30d > 0:
            rec_parts.append(f"Stocking 30 days requires ${capital_30d:,.0f} in capital.")
        if order_count > 0:
            rec_parts.append(f"Ordered {order_count} times in the last 30 days.")
        if trend == "increasing":
            rec_parts.append("Demand is accelerating.")
        elif trend == "decreasing":
            rec_parts.append("Demand is slowing.")

        return {
            "sku": inv.sku if inv else sku,
            "title": inv.title if inv else (pc.description if pc else ""),
            "vendor": inv.vendor if inv else (pc.vendor if pc else ""),
            "inventory_quantity": inv.inventory_quantity if inv else None,
            "unit_cost": round(unit_cost, 2),
            "cost_missing": unit_cost <= 0,
            "avg_price": round(avg_price, 2),
            "margin_pct": round(margin_pct, 1) if margin_pct is not None else None,
            "velocity_30d": round(vel_30, 3),
            "velocity_7d": round(vel_7, 3),
            "trend": trend,
            "units_sold_30d": int(round(total_units_all_channels)),
            "online_units_30d": int(total_units),
            "offline_units_30d": round(offline_30, 1),
            "offline_data_available": offline_data_available,
            "revenue_30d": round(total_revenue, 2),
            "order_count_30d": order_count,
            "capital_30d": round(capital_30d, 2),
            "recommendation": " ".join(rec_parts),
            "sparkline": sparkline,
            "recent_orders": [
                {
                    "date": str(r.order_date)[:10] if r.order_date else "",
                    "order_number": r.order_number,
                    "quantity": int(r.quantity or 0),
                    "price": float(r.price or 0),
                    "total": float(r.total_price or 0),
                }
                for r in recent
            ],
        }
