"""
Inventory Intelligence Service

Powers the 4-tab Inventory Intelligence Dashboard:
  Tab 1 – Inventory Pulse (executive overview)
  Tab 2 – Reorder Queue (action list with cost estimates)
  Tab 3 – Stock Health (overstock, dead stock, brand health)
  Tab 4 – SKU Deep Dive (search + drill-down)

Data joins:
  MLInventorySuggestion.sku → ShopifyInventory.sku
  MLInventorySuggestion.sku → ProductCost.vendor_sku (UPPER match)
  MLInventorySuggestion.sku → ShopifyOrderItem.sku
"""
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal

from sqlalchemy import func, desc, asc, and_, case
from sqlalchemy.orm import Session

from app.models.ml_intelligence import MLInventorySuggestion, InventoryDailySnapshot
from app.models.shopify import ShopifyInventory, ShopifyOrderItem, ShopifyOrder, ShopifyProduct
from app.models.product_cost import ProductCost

logger = logging.getLogger(__name__)


class InventoryIntelligenceService:
    def __init__(self, db: Session):
        self.db = db

    def _cost_subqueries(self):
        """Pre-aggregated cost subqueries (one row per SKU) to avoid join multiplication."""
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

    # ─────────────────────────────────────────────
    # TAB 1: INVENTORY PULSE
    # ─────────────────────────────────────────────

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Main dashboard payload for Tab 1 (Pulse)."""
        snapshot = self.get_executive_snapshot()
        distribution = self.get_stock_distribution()
        brand_summary = self.get_brand_summary(limit=20)
        brand_value_chart = self.get_brand_value_chart(limit=10)
        velocity_summary = self.get_velocity_summary()

        pulse = self._build_pulse_narrative(snapshot, distribution)

        return {
            "pulse": pulse,
            "snapshot": snapshot,
            "distribution": distribution,
            "brand_summary": brand_summary,
            "brand_value_chart": brand_value_chart,
            "velocity_summary": velocity_summary,
        }

    def get_executive_snapshot(self) -> Dict[str, Any]:
        """KPI calculations for the executive overview."""
        try:
            # Counts by suggestion category
            cat_counts = (
                self.db.query(
                    MLInventorySuggestion.suggestion,
                    func.count().label("cnt"),
                    func.sum(MLInventorySuggestion.units_on_hand).label("units"),
                )
                .group_by(MLInventorySuggestion.suggestion)
                .all()
            )

            counts = {}
            total_skus = 0
            total_units = 0
            for row in cat_counts:
                counts[row.suggestion] = {"count": row.cnt, "units": int(row.units or 0)}
                total_skus += row.cnt
                total_units += int(row.units or 0)

            # Urgency counts
            urgency_counts = (
                self.db.query(
                    MLInventorySuggestion.urgency,
                    func.count().label("cnt"),
                )
                .group_by(MLInventorySuggestion.urgency)
                .all()
            )
            urgency = {r.urgency: r.cnt for r in urgency_counts}

            # Velocity trend counts
            trend_counts = (
                self.db.query(
                    MLInventorySuggestion.velocity_trend,
                    func.count().label("cnt"),
                )
                .group_by(MLInventorySuggestion.velocity_trend)
                .all()
            )
            trends = {r.velocity_trend: r.cnt for r in trend_counts}

            # Average days of cover (weighted by velocity)
            avg_doc = (
                self.db.query(
                    func.avg(MLInventorySuggestion.days_of_cover).label("avg_doc"),
                )
                .filter(
                    MLInventorySuggestion.days_of_cover < 999,
                    MLInventorySuggestion.daily_sales_velocity > 0,
                )
                .scalar()
            )

            # Total reorder quantity — use suggestion filter for consistency with reorder queue
            reorder_totals = (
                self.db.query(
                    func.sum(MLInventorySuggestion.reorder_quantity).label("total_qty"),
                    func.count().label("cnt"),
                )
                .filter(MLInventorySuggestion.suggestion.in_(["reorder_now", "reorder_soon"]))
                .first()
            )

            # Oversold count
            oversold_count = (
                self.db.query(func.count())
                .select_from(MLInventorySuggestion)
                .filter(MLInventorySuggestion.oversold == True)
                .scalar()
            ) or 0

            # Cost missing in reorder queue
            reorder_cost_missing_count = (
                self.db.query(func.count())
                .select_from(MLInventorySuggestion)
                .filter(
                    MLInventorySuggestion.cost_missing == True,
                    MLInventorySuggestion.suggestion.in_(["reorder_now", "reorder_soon"]),
                )
                .scalar()
            ) or 0

            # Total offline units estimated
            offline_total = (
                self.db.query(func.sum(MLInventorySuggestion.offline_units_30d))
                .scalar()
            ) or 0

            # Check if offline data is available (≥2 snapshot days)
            snapshot_day_count = (
                self.db.query(func.count(func.distinct(InventoryDailySnapshot.snapshot_date)))
                .scalar()
            ) or 0
            offline_data_available = snapshot_day_count >= 2

            # Inventory value calculation
            inv_value = self._calculate_total_inventory_value()

            # Turnover rate (30d)
            turnover = self._calculate_turnover_rate(days=30)

            # Last generated timestamp
            last_gen = (
                self.db.query(func.max(MLInventorySuggestion.generated_at)).scalar()
            )

            # ----- All-inventory KPIs (full ShopifyInventory, not just ML tracked) -----
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()

            # Oversold across all inventory (quantity < 0)
            oversold_all_inventory = (
                self.db.query(func.count())
                .select_from(ShopifyInventory)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity < 0,
                )
                .scalar()
            ) or 0

            # Not stocked: active product variants with no inventory record or quantity = 0
            not_stocked_count = (
                self.db.query(func.count())
                .select_from(ShopifyInventory)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity == 0,
                )
                .scalar()
            ) or 0

            return {
                "total_skus": total_skus,
                "total_units": total_units,
                "inventory_value": inv_value.get("total_value", 0),
                "inventory_value_coverage_pct": inv_value.get("coverage_pct", 0),
                "avg_days_of_cover": round(float(avg_doc or 0), 1),
                "reorder_now": counts.get("reorder_now", {}).get("count", 0),
                "reorder_soon": counts.get("reorder_soon", {}).get("count", 0),
                "adequate": counts.get("adequate", {}).get("count", 0),
                "overstock": counts.get("overstock", {}).get("count", 0),
                "no_sales": counts.get("no_sales", {}).get("count", 0),
                "critical_count": urgency.get("critical", 0),
                "warning_count": urgency.get("warning", 0),
                "total_reorder_qty": int(reorder_totals.total_qty or 0) if reorder_totals else 0,
                "reorder_sku_count": reorder_totals.cnt if reorder_totals else 0,
                "oversold_count": oversold_count,
                "oversold_all_inventory": oversold_all_inventory,
                "not_stocked_count": not_stocked_count,
                "reorder_cost_missing_count": reorder_cost_missing_count,
                "offline_units_estimated_30d": round(float(offline_total), 1),
                "offline_data_available": offline_data_available,
                "velocity_increasing": trends.get("increasing", 0),
                "velocity_stable": trends.get("stable", 0),
                "velocity_decreasing": trends.get("decreasing", 0),
                "turnover_rate": turnover,
                "last_generated": str(last_gen) if last_gen else None,
            }
        except Exception as e:
            logger.error(f"Error in executive snapshot: {e}")
            return {}

    def _calculate_total_inventory_value(self) -> Dict[str, Any]:
        """Calculate total inventory value joining ShopifyInventory + ProductCost (active products only)."""
        try:
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()

            # Pre-aggregated cost per SKU to avoid join multiplication
            pc_cost = (
                self.db.query(
                    func.upper(ProductCost.vendor_sku).label("sku"),
                    func.max(ProductCost.nett_nett_cost_inc_gst).label("cost"),
                )
                .filter(ProductCost.vendor_sku.isnot(None), ProductCost.vendor_sku != "")
                .group_by(func.upper(ProductCost.vendor_sku))
                .subquery()
            )

            effective_cost = func.coalesce(
                pc_cost.c.cost, ShopifyInventory.cost
            )

            result = (
                self.db.query(
                    func.sum(ShopifyInventory.inventory_quantity * effective_cost).label("total_value"),
                    func.count().label("total_rows"),
                    func.sum(
                        case(
                            (effective_cost.isnot(None), 1),
                            else_=0,
                        )
                    ).label("with_cost"),
                )
                .outerjoin(
                    pc_cost,
                    func.upper(ShopifyInventory.sku) == pc_cost.c.sku,
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity.isnot(None),
                    ShopifyInventory.inventory_quantity > 0,
                )
                .first()
            )

            total_value = float(result.total_value or 0) if result else 0
            total_rows = int(result.total_rows or 0) if result else 0
            with_cost = int(result.with_cost or 0) if result else 0
            coverage_pct = round((with_cost / total_rows) * 100, 1) if total_rows > 0 else 0

            return {
                "total_value": round(total_value, 2),
                "total_rows": total_rows,
                "with_cost": with_cost,
                "coverage_pct": coverage_pct,
            }
        except Exception as e:
            logger.error(f"Error calculating inventory value: {e}")
            return {"total_value": 0, "coverage_pct": 0}

    def _calculate_turnover_rate(self, days: int = 30) -> float:
        """Inventory turnover: units sold in period / current inventory."""
        try:
            cutoff = date.today() - timedelta(days=days)

            sold = (
                self.db.query(func.sum(ShopifyOrderItem.quantity))
                .join(
                    ShopifyOrder,
                    ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
                )
                .filter(
                    ShopifyOrderItem.order_date >= cutoff,
                    ShopifyOrder.cancelled_at.is_(None),
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                )
                .scalar()
            ) or 0

            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()
            inventory = (
                self.db.query(func.sum(ShopifyInventory.inventory_quantity))
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity > 0,
                )
                .scalar()
            ) or 0

            return round(float(sold) / float(inventory), 3) if inventory > 0 else 0
        except Exception as e:
            logger.error(f"Error calculating turnover: {e}")
            return 0

    def get_stock_distribution(self) -> List[Dict[str, Any]]:
        """Count + value per suggestion bucket for the stacked bar."""
        try:
            # Get counts and raw units per suggestion (no capping)
            rows = (
                self.db.query(
                    MLInventorySuggestion.suggestion,
                    func.count().label("count"),
                    func.sum(MLInventorySuggestion.units_on_hand).label("units"),
                )
                .group_by(MLInventorySuggestion.suggestion)
                .all()
            )

            # Build value estimates per category using deduplicated cost subqueries
            pc_cost, inv_cost = self._cost_subqueries()
            value_rows = (
                self.db.query(
                    MLInventorySuggestion.suggestion,
                    func.sum(
                        MLInventorySuggestion.units_on_hand
                        * func.coalesce(pc_cost.c.cost, inv_cost.c.cost, 0)
                    ).label("value"),
                )
                .outerjoin(pc_cost, func.upper(MLInventorySuggestion.sku) == pc_cost.c.sku)
                .outerjoin(inv_cost, func.upper(MLInventorySuggestion.sku) == inv_cost.c.sku)
                .group_by(MLInventorySuggestion.suggestion)
                .all()
            )
            value_map = {r.suggestion: float(r.value or 0) for r in value_rows}

            order = ["reorder_now", "reorder_soon", "adequate", "overstock", "no_sales"]
            colors = {
                "reorder_now": "#b5342a",
                "reorder_soon": "#c49a4a",
                "adequate": "#1a7a3a",
                "overstock": "#1f6f6b",
                "no_sales": "#9e9e9e",
            }
            labels = {
                "reorder_now": "Reorder Now",
                "reorder_soon": "Reorder Soon",
                "adequate": "Adequate",
                "overstock": "Overstock",
                "no_sales": "No Sales",
            }

            result = []
            row_map = {r.suggestion: r for r in rows}
            for key in order:
                r = row_map.get(key)
                result.append({
                    "key": key,
                    "label": labels.get(key, key),
                    "count": r.count if r else 0,
                    "units": int(r.units or 0) if r else 0,
                    "value": round(value_map.get(key, 0), 2),
                    "color": colors.get(key, "#999"),
                })

            return result
        except Exception as e:
            logger.error(f"Error in stock distribution: {e}")
            return []

    def get_brand_summary(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Brand-level aggregations for the overview table."""
        try:
            rows = (
                self.db.query(
                    MLInventorySuggestion.brand,
                    func.count().label("sku_count"),
                    func.sum(MLInventorySuggestion.units_on_hand).label("total_units"),
                    func.avg(MLInventorySuggestion.daily_sales_velocity).label("avg_velocity"),
                    func.avg(
                        case(
                            (MLInventorySuggestion.days_of_cover < 999, MLInventorySuggestion.days_of_cover),
                            else_=None,
                        )
                    ).label("avg_days_cover"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.urgency == "critical", 1),
                            else_=0,
                        )
                    ).label("critical_count"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.urgency == "warning", 1),
                            else_=0,
                        )
                    ).label("warning_count"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.suggestion == "overstock", 1),
                            else_=0,
                        )
                    ).label("overstock_count"),
                )
                .filter(MLInventorySuggestion.brand.isnot(None))
                .group_by(MLInventorySuggestion.brand)
                .order_by(desc("sku_count"))
                .limit(limit)
                .all()
            )

            return [
                {
                    "brand": r.brand,
                    "sku_count": r.sku_count,
                    "total_units": int(r.total_units or 0),
                    "avg_velocity": round(float(r.avg_velocity or 0), 2),
                    "avg_days_cover": round(float(r.avg_days_cover or 0), 1) if r.avg_days_cover else None,
                    "critical_count": int(r.critical_count or 0),
                    "warning_count": int(r.warning_count or 0),
                    "overstock_count": int(r.overstock_count or 0),
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Error in brand summary: {e}")
            return []

    def get_brand_value_chart(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Top brands by inventory value for the bar chart (active products only)."""
        try:
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()

            effective_cost = func.coalesce(
                ProductCost.nett_nett_cost_inc_gst, ShopifyInventory.cost
            )

            rows = (
                self.db.query(
                    func.coalesce(ShopifyInventory.vendor, "Unknown").label("brand"),
                    func.sum(ShopifyInventory.inventory_quantity * effective_cost).label("value"),
                    func.sum(ShopifyInventory.inventory_quantity).label("units"),
                )
                .outerjoin(
                    ProductCost,
                    func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku),
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity > 0,
                    effective_cost.isnot(None),
                )
                .group_by("brand")
                .order_by(desc("value"))
                .limit(limit)
                .all()
            )

            return [
                {
                    "brand": r.brand,
                    "value": round(float(r.value or 0), 2),
                    "units": int(r.units or 0),
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Error in brand value chart: {e}")
            return []

    def get_velocity_summary(self) -> Dict[str, Any]:
        """Velocity trend summary for overview."""
        try:
            rows = (
                self.db.query(
                    MLInventorySuggestion.velocity_trend,
                    func.count().label("cnt"),
                    func.avg(MLInventorySuggestion.daily_sales_velocity).label("avg_vel"),
                )
                .filter(MLInventorySuggestion.velocity_trend.isnot(None))
                .group_by(MLInventorySuggestion.velocity_trend)
                .all()
            )

            return {
                r.velocity_trend: {
                    "count": r.cnt,
                    "avg_velocity": round(float(r.avg_vel or 0), 3),
                }
                for r in rows
            }
        except Exception as e:
            logger.error(f"Error in velocity summary: {e}")
            return {}

    def _build_pulse_narrative(
        self, snapshot: Dict, distribution: List[Dict]
    ) -> Dict[str, Any]:
        """Generate the pulse banner narrative."""
        critical = snapshot.get("reorder_now", 0)
        warning = snapshot.get("reorder_soon", 0)
        overstock = snapshot.get("overstock", 0)
        no_sales = snapshot.get("no_sales", 0)
        total = snapshot.get("total_skus", 0)
        inv_value = snapshot.get("inventory_value", 0)
        turnover = snapshot.get("turnover_rate", 0)

        # Determine overall status
        attention = critical + warning
        if critical > 20:
            status = "critical"
            chip = "Urgent Action"
        elif critical > 5 or attention > 30:
            status = "warning"
            chip = "Needs Attention"
        elif overstock > total * 0.3:
            status = "caution"
            chip = "Capital Tied Up"
        else:
            status = "healthy"
            chip = "Healthy"

        # Overstock value
        overstock_val = 0
        for d in distribution:
            if d["key"] == "overstock":
                overstock_val = d.get("value", 0)
            if d["key"] == "no_sales":
                overstock_val += d.get("value", 0)

        # Plain narrative
        parts = []
        parts.append(f"{total} SKUs tracked across ${inv_value:,.0f} in inventory value.")
        if critical > 0:
            parts.append(f"{critical} SKUs need immediate reorder.")
        if overstock > 0:
            parts.append(f"{overstock} overstocked SKUs tying up ${overstock_val:,.0f}.")
        if no_sales > 0:
            parts.append(f"{no_sales} SKUs with zero velocity (dead stock).")

        plain = " ".join(parts)

        # Pro narrative
        pro_parts = []
        pro_parts.append(
            f"Tracking {total} active SKUs with ${inv_value:,.0f} total inventory value "
            f"and a {turnover:.1%} 30-day turnover rate."
        )
        if critical > 0:
            pro_parts.append(
                f"{critical} critical reorder SKUs risk stockout within 7 days at current velocity."
            )
        if overstock_val > 0:
            pro_parts.append(
                f"${overstock_val:,.0f} capital locked in {overstock + no_sales} overstock/dead-stock SKUs — "
                f"consider markdowns or supplier returns."
            )
        if snapshot.get("velocity_increasing", 0) > snapshot.get("velocity_decreasing", 0):
            pro_parts.append("Overall demand trend is accelerating — review reorder quantities.")
        elif snapshot.get("velocity_decreasing", 0) > snapshot.get("velocity_increasing", 0):
            pro_parts.append("Demand is slowing across the portfolio — watch for emerging overstock.")

        pro = " ".join(pro_parts)

        return {
            "status": status,
            "chip": chip,
            "plain": plain,
            "pro": pro,
        }

    # ─────────────────────────────────────────────
    # TAB 2: REORDER QUEUE
    # ─────────────────────────────────────────────

    def get_reorder_queue(
        self,
        page: int = 1,
        per_page: int = 25,
        brand: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Paginated reorder queue with cost estimates."""
        try:
            pc_cost, inv_cost = self._cost_subqueries()
            effective_cost = func.coalesce(pc_cost.c.cost, inv_cost.c.cost)

            base = (
                self.db.query(
                    MLInventorySuggestion.sku,
                    MLInventorySuggestion.brand,
                    MLInventorySuggestion.title,
                    MLInventorySuggestion.units_on_hand,
                    MLInventorySuggestion.daily_sales_velocity,
                    MLInventorySuggestion.velocity_trend,
                    MLInventorySuggestion.days_of_cover,
                    MLInventorySuggestion.suggestion,
                    MLInventorySuggestion.reorder_quantity,
                    MLInventorySuggestion.urgency,
                    MLInventorySuggestion.oversold,
                    MLInventorySuggestion.cost_missing,
                    MLInventorySuggestion.offline_units_30d,
                    effective_cost.label("unit_cost"),
                )
                .outerjoin(pc_cost, func.upper(MLInventorySuggestion.sku) == pc_cost.c.sku)
                .outerjoin(inv_cost, func.upper(MLInventorySuggestion.sku) == inv_cost.c.sku)
                .filter(
                    MLInventorySuggestion.suggestion.in_(["reorder_now", "reorder_soon"])
                )
            )

            if brand:
                base = base.filter(
                    func.upper(MLInventorySuggestion.brand) == brand.upper()
                )

            # Total count for pagination
            total_count = base.count()

            # KPIs (no joins needed — from MLInventorySuggestion only)
            kpi_rows = (
                self.db.query(
                    func.sum(
                        case(
                            (MLInventorySuggestion.urgency == "critical", 1),
                            else_=0,
                        )
                    ).label("critical"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.urgency == "warning", 1),
                            else_=0,
                        )
                    ).label("warning"),
                    func.sum(MLInventorySuggestion.reorder_quantity).label("total_qty"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.oversold == True, 1),
                            else_=0,
                        )
                    ).label("oversold_count"),
                    func.sum(
                        case(
                            (MLInventorySuggestion.cost_missing == True, 1),
                            else_=0,
                        )
                    ).label("cost_missing_count"),
                )
                .filter(
                    MLInventorySuggestion.suggestion.in_(["reorder_now", "reorder_soon"])
                )
                .first()
            )

            # Estimated reorder cost using deduplicated cost subqueries
            pc_cost2, inv_cost2 = self._cost_subqueries()
            effective_cost2 = func.coalesce(pc_cost2.c.cost, inv_cost2.c.cost)
            est_cost_row = (
                self.db.query(
                    func.sum(
                        MLInventorySuggestion.reorder_quantity * effective_cost2
                    ).label("est_cost"),
                )
                .outerjoin(pc_cost2, func.upper(MLInventorySuggestion.sku) == pc_cost2.c.sku)
                .outerjoin(inv_cost2, func.upper(MLInventorySuggestion.sku) == inv_cost2.c.sku)
                .filter(
                    MLInventorySuggestion.suggestion.in_(["reorder_now", "reorder_soon"]),
                    MLInventorySuggestion.reorder_quantity.isnot(None),
                    effective_cost2.isnot(None),
                )
                .first()
            )

            # Paginated items
            items = (
                base.order_by(
                    asc(MLInventorySuggestion.days_of_cover),
                    desc(MLInventorySuggestion.daily_sales_velocity),
                )
                .offset((page - 1) * per_page)
                .limit(per_page)
                .all()
            )

            return {
                "kpis": {
                    "critical_skus": int(kpi_rows.critical or 0) if kpi_rows else 0,
                    "warning_skus": int(kpi_rows.warning or 0) if kpi_rows else 0,
                    "total_reorder_qty": int(kpi_rows.total_qty or 0) if kpi_rows else 0,
                    "estimated_reorder_cost": round(float(est_cost_row.est_cost or 0), 2) if est_cost_row else 0,
                    "oversold_count": int(kpi_rows.oversold_count or 0) if kpi_rows else 0,
                    "cost_missing_count": int(kpi_rows.cost_missing_count or 0) if kpi_rows else 0,
                },
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total_count,
                    "pages": max(1, -(-total_count // per_page)),
                },
                "items": [
                    {
                        "sku": r.sku,
                        "brand": r.brand,
                        "title": r.title,
                        "units_on_hand": r.units_on_hand,
                        "velocity": round(float(r.daily_sales_velocity or 0), 2),
                        "velocity_trend": r.velocity_trend,
                        "days_of_cover": round(float(r.days_of_cover or 0), 1),
                        "suggestion": r.suggestion,
                        "reorder_quantity": r.reorder_quantity,
                        "urgency": r.urgency,
                        "oversold": r.oversold,
                        "cost_missing": r.cost_missing,
                        "offline_units_30d": round(float(r.offline_units_30d or 0), 1),
                        "unit_cost": round(float(r.unit_cost or 0), 2) if r.unit_cost else None,
                        "est_reorder_cost": round(float(r.unit_cost or 0) * (r.reorder_quantity or 0), 2) if r.unit_cost else None,
                    }
                    for r in items
                ],
            }
        except Exception as e:
            logger.error(f"Error in reorder queue: {e}")
            return {"kpis": {}, "pagination": {}, "items": []}

    def get_velocity_movers(self, limit: int = 20) -> Dict[str, Any]:
        """SKUs with biggest velocity changes (accelerating demand)."""
        try:
            increasing = (
                self.db.query(MLInventorySuggestion)
                .filter(MLInventorySuggestion.velocity_trend == "increasing")
                .order_by(desc(MLInventorySuggestion.daily_sales_velocity))
                .limit(limit)
                .all()
            )

            decreasing = (
                self.db.query(MLInventorySuggestion)
                .filter(MLInventorySuggestion.velocity_trend == "decreasing")
                .order_by(desc(MLInventorySuggestion.daily_sales_velocity))
                .limit(limit)
                .all()
            )

            def to_dict(r):
                return {
                    "sku": r.sku,
                    "brand": r.brand,
                    "title": r.title,
                    "velocity": round(float(r.daily_sales_velocity or 0), 2),
                    "units_on_hand": r.units_on_hand,
                    "days_of_cover": round(float(r.days_of_cover or 0), 1),
                    "suggestion": r.suggestion,
                    "urgency": r.urgency,
                }

            return {
                "increasing": [to_dict(r) for r in increasing],
                "decreasing": [to_dict(r) for r in decreasing],
            }
        except Exception as e:
            logger.error(f"Error in velocity movers: {e}")
            return {"increasing": [], "decreasing": []}

    # ─────────────────────────────────────────────
    # TAB 3: STOCK HEALTH
    # ─────────────────────────────────────────────

    def get_stock_health(self) -> Dict[str, Any]:
        """Overstock, dead stock, and brand health analysis."""
        try:
            pc_cost, inv_cost = self._cost_subqueries()
            effective_cost = func.coalesce(pc_cost.c.cost, inv_cost.c.cost)

            # Overstock items
            overstock = (
                self.db.query(
                    MLInventorySuggestion.sku,
                    MLInventorySuggestion.brand,
                    MLInventorySuggestion.title,
                    MLInventorySuggestion.units_on_hand,
                    MLInventorySuggestion.daily_sales_velocity,
                    MLInventorySuggestion.days_of_cover,
                    effective_cost.label("unit_cost"),
                )
                .outerjoin(pc_cost, func.upper(MLInventorySuggestion.sku) == pc_cost.c.sku)
                .outerjoin(inv_cost, func.upper(MLInventorySuggestion.sku) == inv_cost.c.sku)
                .filter(MLInventorySuggestion.suggestion == "overstock")
                .order_by(desc(MLInventorySuggestion.days_of_cover))
                .limit(100)
                .all()
            )

            # No sales items (need fresh subqueries for separate join context)
            pc_cost2, inv_cost2 = self._cost_subqueries()
            effective_cost2 = func.coalesce(pc_cost2.c.cost, inv_cost2.c.cost)
            no_sales = (
                self.db.query(
                    MLInventorySuggestion.sku,
                    MLInventorySuggestion.brand,
                    MLInventorySuggestion.title,
                    MLInventorySuggestion.units_on_hand,
                    MLInventorySuggestion.days_of_cover,
                    effective_cost2.label("unit_cost"),
                )
                .outerjoin(pc_cost2, func.upper(MLInventorySuggestion.sku) == pc_cost2.c.sku)
                .outerjoin(inv_cost2, func.upper(MLInventorySuggestion.sku) == inv_cost2.c.sku)
                .filter(MLInventorySuggestion.suggestion == "no_sales")
                .order_by(desc(MLInventorySuggestion.units_on_hand))
                .limit(100)
                .all()
            )

            # Oversold items — strictly negative inventory only
            oversold_items = (
                self.db.query(
                    MLInventorySuggestion.sku,
                    MLInventorySuggestion.brand,
                    MLInventorySuggestion.title,
                    MLInventorySuggestion.units_on_hand,
                    MLInventorySuggestion.daily_sales_velocity,
                    MLInventorySuggestion.days_of_cover,
                    MLInventorySuggestion.urgency,
                )
                .filter(
                    MLInventorySuggestion.oversold == True,
                    MLInventorySuggestion.units_on_hand < 0,
                )
                .order_by(desc(MLInventorySuggestion.daily_sales_velocity))
                .limit(100)
                .all()
            )

            # KPIs
            overstock_value = sum(
                float(r.unit_cost or 0) * int(r.units_on_hand or 0) for r in overstock
            )
            no_sales_value = sum(
                float(r.unit_cost or 0) * int(r.units_on_hand or 0) for r in no_sales
            )

            # Brand health breakdown
            brand_health = (
                self.db.query(
                    MLInventorySuggestion.brand,
                    func.count().label("total"),
                    func.sum(case((MLInventorySuggestion.suggestion == "reorder_now", 1), else_=0)).label("reorder_now"),
                    func.sum(case((MLInventorySuggestion.suggestion == "reorder_soon", 1), else_=0)).label("reorder_soon"),
                    func.sum(case((MLInventorySuggestion.suggestion == "adequate", 1), else_=0)).label("adequate"),
                    func.sum(case((MLInventorySuggestion.suggestion == "overstock", 1), else_=0)).label("overstock"),
                    func.sum(case((MLInventorySuggestion.suggestion == "no_sales", 1), else_=0)).label("no_sales"),
                )
                .filter(MLInventorySuggestion.brand.isnot(None))
                .group_by(MLInventorySuggestion.brand)
                .order_by(desc("total"))
                .limit(20)
                .all()
            )

            def item_dict(r, include_velocity=True):
                d = {
                    "sku": r.sku,
                    "brand": r.brand,
                    "title": r.title,
                    "units_on_hand": r.units_on_hand,
                    "unit_cost": round(float(r.unit_cost or 0), 2) if r.unit_cost else None,
                    "value_tied_up": round(float(r.unit_cost or 0) * int(r.units_on_hand or 0), 2) if r.unit_cost else None,
                }
                if include_velocity:
                    d["velocity"] = round(float(r.daily_sales_velocity or 0), 2)
                    d["days_of_cover"] = round(float(r.days_of_cover or 0), 1)
                return d

            # ----- All-inventory: Not Stocked products (active, quantity = 0) -----
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()
            not_stocked_items = (
                self.db.query(
                    ShopifyInventory.sku,
                    ShopifyInventory.vendor,
                    ShopifyInventory.title,
                )
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity == 0,
                )
                .order_by(ShopifyInventory.title)
                .limit(20)
                .all()
            )

            # All-inventory oversold count (quantity < 0)
            oversold_all_inventory = (
                self.db.query(func.count())
                .select_from(ShopifyInventory)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity < 0,
                )
                .scalar()
            ) or 0

            not_stocked_count = (
                self.db.query(func.count())
                .select_from(ShopifyInventory)
                .filter(
                    ShopifyInventory.shopify_product_id.in_(active_pids),
                    ShopifyInventory.inventory_quantity == 0,
                )
                .scalar()
            ) or 0

            return {
                "kpis": {
                    "overstock_skus": len(overstock),
                    "overstock_value": round(overstock_value, 2),
                    "no_sales_skus": len(no_sales),
                    "no_sales_value": round(no_sales_value, 2),
                    "total_tied_up": round(overstock_value + no_sales_value, 2),
                    "oversold_count": len(oversold_items),
                    "oversold_all_inventory": oversold_all_inventory,
                    "not_stocked_count": not_stocked_count,
                },
                "oversold": [
                    {
                        "sku": r.sku,
                        "brand": r.brand,
                        "title": r.title,
                        "units_on_hand": r.units_on_hand,
                        "velocity": round(float(r.daily_sales_velocity or 0), 2),
                        "days_of_cover": round(float(r.days_of_cover or 0), 1),
                        "urgency": r.urgency,
                    }
                    for r in oversold_items
                ],
                "not_stocked": [
                    {
                        "sku": r.sku or "-",
                        "vendor": r.vendor or "-",
                        "title": r.title or "-",
                    }
                    for r in not_stocked_items
                ],
                "overstock": [item_dict(r) for r in overstock],
                "no_sales": [item_dict(r, include_velocity=False) for r in no_sales],
                "brand_health": [
                    {
                        "brand": r.brand,
                        "total": r.total,
                        "reorder_now": int(r.reorder_now or 0),
                        "reorder_soon": int(r.reorder_soon or 0),
                        "adequate": int(r.adequate or 0),
                        "overstock": int(r.overstock or 0),
                        "no_sales": int(r.no_sales or 0),
                    }
                    for r in brand_health
                ],
            }
        except Exception as e:
            logger.error(f"Error in stock health: {e}")
            return {"kpis": {}, "overstock": [], "no_sales": [], "not_stocked": [], "brand_health": []}

    # ─────────────────────────────────────────────
    # TAB 4: SKU DEEP DIVE
    # ─────────────────────────────────────────────

    def get_sku_detail(self, sku: str) -> Dict[str, Any]:
        """Full SKU drill-down for the modal."""
        try:
            # ML suggestion data
            suggestion = (
                self.db.query(MLInventorySuggestion)
                .filter(func.upper(MLInventorySuggestion.sku) == sku.upper())
                .first()
            )

            # Inventory data
            inventory = (
                self.db.query(ShopifyInventory)
                .filter(func.upper(ShopifyInventory.sku) == sku.upper())
                .first()
            )

            # Cost data
            cost_data = (
                self.db.query(ProductCost)
                .filter(func.upper(ProductCost.vendor_sku) == sku.upper())
                .first()
            )

            # Recent orders (last 10)
            cutoff_90d = date.today() - timedelta(days=90)
            recent_orders = (
                self.db.query(
                    ShopifyOrderItem.order_date,
                    ShopifyOrderItem.quantity,
                    ShopifyOrderItem.price,
                    ShopifyOrderItem.total_price,
                    ShopifyOrderItem.order_number,
                )
                .filter(
                    func.upper(ShopifyOrderItem.sku) == sku.upper(),
                    ShopifyOrderItem.order_date >= cutoff_90d,
                )
                .order_by(desc(ShopifyOrderItem.order_date))
                .limit(10)
                .all()
            )

            # Daily sales for last 30 days (for sparkline)
            cutoff_30d = date.today() - timedelta(days=30)
            daily_sales = (
                self.db.query(
                    func.date(ShopifyOrderItem.order_date).label("day"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                )
                .join(
                    ShopifyOrder,
                    ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
                )
                .filter(
                    func.upper(ShopifyOrderItem.sku) == sku.upper(),
                    ShopifyOrderItem.order_date >= cutoff_30d,
                    ShopifyOrder.cancelled_at.is_(None),
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                )
                .group_by("day")
                .order_by("day")
                .all()
            )

            # Build sparkline data (fill missing days with 0)
            spark_data = []
            if daily_sales:
                day_map = {str(r.day): int(r.units or 0) for r in daily_sales}
                for i in range(30):
                    d = date.today() - timedelta(days=29 - i)
                    spark_data.append(day_map.get(str(d), 0))

            # Revenue from this SKU (30d)
            revenue_30d = (
                self.db.query(
                    func.sum(ShopifyOrderItem.total_price).label("revenue"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                )
                .join(
                    ShopifyOrder,
                    ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id,
                )
                .filter(
                    func.upper(ShopifyOrderItem.sku) == sku.upper(),
                    ShopifyOrderItem.order_date >= cutoff_30d,
                    ShopifyOrder.cancelled_at.is_(None),
                    ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                )
                .first()
            )

            # Build result
            result = {
                "sku": sku,
                "found": suggestion is not None or inventory is not None,
            }

            if suggestion:
                result["suggestion"] = {
                    "brand": suggestion.brand,
                    "title": suggestion.title,
                    "units_on_hand": suggestion.units_on_hand,
                    "velocity": round(float(suggestion.daily_sales_velocity or 0), 2),
                    "velocity_trend": suggestion.velocity_trend,
                    "days_of_cover": round(float(suggestion.days_of_cover or 0), 1),
                    "suggestion": suggestion.suggestion,
                    "reorder_quantity": suggestion.reorder_quantity,
                    "urgency": suggestion.urgency,
                    "oversold": suggestion.oversold,
                    "cost_missing": suggestion.cost_missing,
                    "offline_units_30d": round(float(suggestion.offline_units_30d or 0), 1),
                }

            if inventory:
                result["inventory"] = {
                    "quantity": inventory.inventory_quantity,
                    "shopify_cost": round(float(inventory.cost or 0), 2) if inventory.cost else None,
                    "vendor": inventory.vendor,
                    "inventory_policy": inventory.inventory_policy,
                }

            if cost_data:
                # Compute margin if we have both cost and recent price
                selling_price = None
                if recent_orders:
                    selling_price = float(recent_orders[0].price or 0)

                active_cost = float(cost_data.get_active_cost() or 0) if cost_data.get_active_cost() else None
                margin_pct = None
                if active_cost and selling_price and selling_price > 0:
                    margin_pct = round(((selling_price - active_cost) / selling_price) * 100, 1)

                result["cost_data"] = {
                    "nett_nett_cost": round(float(cost_data.nett_nett_cost_inc_gst or 0), 2) if cost_data.nett_nett_cost_inc_gst else None,
                    "rrp": round(float(cost_data.rrp_inc_gst or 0), 2) if cost_data.rrp_inc_gst else None,
                    "invoice_price": round(float(cost_data.invoice_price_inc_gst or 0), 2) if cost_data.invoice_price_inc_gst else None,
                    "has_special": cost_data.has_active_special,
                    "special_cost": round(float(cost_data.special_cost_inc_gst or 0), 2) if cost_data.special_cost_inc_gst else None,
                    "special_end_date": str(cost_data.special_end_date) if cost_data.special_end_date else None,
                    "active_cost": round(active_cost, 2) if active_cost else None,
                    "margin_pct": margin_pct,
                    "min_margin_pct": round(float(cost_data.min_margin_pct or 0), 1) if cost_data.min_margin_pct else None,
                }

            # Inventory value
            unit_cost = None
            if cost_data and cost_data.get_active_cost():
                unit_cost = float(cost_data.get_active_cost())
            elif inventory and inventory.cost:
                unit_cost = float(inventory.cost)

            on_hand = suggestion.units_on_hand if suggestion else (inventory.inventory_quantity if inventory else 0)
            result["inventory_value"] = round(unit_cost * on_hand, 2) if unit_cost and on_hand else None
            result["unit_cost"] = round(unit_cost, 2) if unit_cost else None

            result["revenue_30d"] = round(float(revenue_30d.revenue or 0), 2) if revenue_30d and revenue_30d.revenue else 0
            result["units_sold_30d"] = int(revenue_30d.units or 0) if revenue_30d and revenue_30d.units else 0

            result["spark_data"] = spark_data

            result["recent_orders"] = [
                {
                    "date": str(r.order_date.date() if hasattr(r.order_date, 'date') else r.order_date) if r.order_date else None,
                    "order_number": r.order_number,
                    "quantity": r.quantity,
                    "price": round(float(r.price or 0), 2),
                    "total": round(float(r.total_price or 0), 2),
                }
                for r in recent_orders
            ]

            # ML flags
            result["flags"] = self._compute_sku_flags(result)

            return result
        except Exception as e:
            logger.error(f"Error in SKU detail for {sku}: {e}")
            return {"sku": sku, "found": False, "error": str(e)}

    def _compute_sku_flags(self, detail: Dict) -> List[Dict[str, str]]:
        """Compute ML flags for a SKU based on its data."""
        flags = []
        sug = detail.get("suggestion", {})

        if not sug:
            return flags

        days_cover = sug.get("days_of_cover", 999)
        velocity = sug.get("velocity", 0)
        trend = sug.get("velocity_trend", "")
        on_hand = sug.get("units_on_hand", 0)
        suggestion = sug.get("suggestion", "")
        oversold = sug.get("oversold", False)
        cost_missing = sug.get("cost_missing", False)
        offline_units = sug.get("offline_units_30d", 0)

        # Oversold
        if oversold:
            flags.append({
                "type": "critical",
                "label": "Oversold",
                "detail": f"Inventory is {on_hand} units — below zero, needs immediate restock",
            })

        # Cost Missing
        if cost_missing:
            flags.append({
                "type": "warning",
                "label": "No Cost Data",
                "detail": "No cost found in ProductCost or ShopifyInventory — reorder cost cannot be estimated",
            })

        # Offline Sales Detected
        if offline_units and offline_units > 0:
            flags.append({
                "type": "info",
                "label": "Offline Sales",
                "detail": f"{offline_units:.0f} units estimated sold offline (showroom) in 30 days",
            })

        # Stockout Risk
        if not oversold and days_cover < 7 and velocity > 0:
            flags.append({
                "type": "critical",
                "label": "Stockout Risk",
                "detail": f"Only {days_cover:.0f} days of cover at current velocity",
            })

        # Velocity Spike (increasing trend + low cover)
        if trend == "increasing" and days_cover < 14:
            flags.append({
                "type": "warning",
                "label": "Velocity Spike",
                "detail": "Demand is accelerating while stock is low",
            })

        # Overstock Alert
        if suggestion == "overstock" and on_hand > 0:
            flags.append({
                "type": "info",
                "label": "Overstock",
                "detail": f"{days_cover:.0f} days of cover — consider markdown or return",
            })

        # Dead Stock
        if suggestion == "no_sales" and on_hand > 0:
            flags.append({
                "type": "info",
                "label": "Dead Stock",
                "detail": "No sales in 30 days — review or clearance",
            })

        # Declining Velocity
        if trend == "decreasing" and velocity > 0 and suggestion in ("adequate", "reorder_soon"):
            flags.append({
                "type": "warning",
                "label": "Slowing Demand",
                "detail": "7-day velocity dropped below 75% of 30-day average",
            })

        # Margin alert
        cost_info = detail.get("cost_data", {})
        if cost_info.get("margin_pct") is not None and cost_info.get("min_margin_pct"):
            if cost_info["margin_pct"] < cost_info["min_margin_pct"]:
                flags.append({
                    "type": "warning",
                    "label": "Below Min Margin",
                    "detail": f"Margin {cost_info['margin_pct']}% < minimum {cost_info['min_margin_pct']}%",
                })

        return flags

    # ─────────────────────────────────────────────
    # SKU SEARCH
    # ─────────────────────────────────────────────

    def search_skus(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search SKUs by partial match on sku, brand, or title."""
        try:
            pattern = f"%{query.upper()}%"
            rows = (
                self.db.query(
                    MLInventorySuggestion.sku,
                    MLInventorySuggestion.brand,
                    MLInventorySuggestion.title,
                    MLInventorySuggestion.units_on_hand,
                    MLInventorySuggestion.daily_sales_velocity,
                    MLInventorySuggestion.days_of_cover,
                    MLInventorySuggestion.suggestion,
                    MLInventorySuggestion.urgency,
                )
                .filter(
                    (func.upper(MLInventorySuggestion.sku).like(pattern))
                    | (func.upper(MLInventorySuggestion.brand).like(pattern))
                    | (func.upper(MLInventorySuggestion.title).like(pattern))
                )
                .order_by(asc(MLInventorySuggestion.days_of_cover))
                .limit(limit)
                .all()
            )

            return [
                {
                    "sku": r.sku,
                    "brand": r.brand,
                    "title": r.title,
                    "units_on_hand": r.units_on_hand,
                    "velocity": round(float(r.daily_sales_velocity or 0), 2),
                    "days_of_cover": round(float(r.days_of_cover or 0), 1),
                    "suggestion": r.suggestion,
                    "urgency": r.urgency,
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Error searching SKUs: {e}")
            return []
