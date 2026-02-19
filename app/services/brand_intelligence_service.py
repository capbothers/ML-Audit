"""
Brand Intelligence Service

Analyses brand (vendor) performance from ShopifyOrderItem data,
enriched with cost data, ad spend, competitive pricing.
Produces YoY comparisons, WHY analysis, and actionable recommendations.
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict
import json
import re as _re

from sqlalchemy.orm import Session
from sqlalchemy import func, case, literal_column, String, and_, or_, extract

from app.models.shopify import ShopifyOrderItem, ShopifyProduct, ShopifyInventory, ShopifyRefundLineItem
from app.models.google_ads_data import GoogleAdsProductPerformance, GoogleAdsCampaign, GoogleAdsSearchTerm
from app.models.ga4_data import GA4ProductPerformance
from app.models.search_console_data import SearchConsoleQuery
from app.models.competitive_pricing import CompetitivePricing
from app.models.product_cost import ProductCost
from app.models.business_expense import MonthlyPL
from app.models.shippit import ShippitOrder
from app.models.shopify import ShopifyOrder
from app.models.ml_intelligence import MLInventorySuggestion
from app.config import get_settings
from app.utils.logger import log

# Recommendation → data source dependency map (for Feature 7)
_REC_DATA_DEPS = {
    "range": ["shopify_orders"],
    "root_cause": ["shopify_orders", "search_console", "shopify_inventory"],
    "ads": ["google_ads"],
    "pricing": ["competitive_pricing", "product_costs"],
    "demand": ["search_console"],
    "conversion": ["ga4"],
    "margin": ["product_costs", "shopify_orders"],
}


def _dec(v):
    """Convert Decimal/None to float."""
    if v is None:
        return 0.0
    return float(v)


def _pct_change(curr, prev):
    """Percentage change, safe for zero."""
    if not prev:
        return None  # can't compute
    return round((curr - prev) / prev * 100, 1)


class BrandIntelligenceService:
    def __init__(self, db: Session):
        self.db = db
        # Business rule: stockouts are not treated as a lead cause of brand decline
        # because "we always sell when out of stock."
        self._stockout_root_cause = False

    # ── public ────────────────────────────────────────────────────

    def _anchored_now(self):
        """Period end anchored to latest order-item date (avoids empty-day skew when data lags utcnow)."""
        latest = self.db.query(func.max(ShopifyOrderItem.order_date)).scalar()
        return latest if latest else datetime.utcnow()

    def get_dashboard(self, period_days: int = 30) -> Dict:
        now = self._anchored_now()
        cur_end = now
        cur_start = now - timedelta(days=period_days)
        yoy_end = cur_end - timedelta(days=365)
        yoy_start = cur_start - timedelta(days=365)

        current = self._brand_aggregates(cur_start, cur_end)
        previous = self._brand_aggregates(yoy_start, yoy_end)

        prev_map = {b["brand"]: b for b in previous}
        ads_product = self._get_ads_product_summary(cur_start, cur_end)
        ads_campaign_rows = self._get_ads_campaign_rows(cur_start, cur_end)

        brands = []
        for b in current:
            p = prev_map.get(b["brand"], {})
            rev_prev = p.get("revenue", 0)
            units_prev = p.get("units", 0)

            yoy_rev = _pct_change(b["revenue"], rev_prev)
            yoy_units = _pct_change(b["units"], units_prev)

            tier = "stable"
            if yoy_rev is not None:
                if yoy_rev > 10:
                    tier = "growing"
                elif yoy_rev < -10:
                    tier = "declining"
            elif rev_prev == 0 and b["revenue"] > 0:
                tier = "growing"

            ads = ads_product.get(b["brand"], {})
            ads_metrics = self._get_ads_campaign_metrics(b["brand"], ads_campaign_rows)
            # Use max of campaign-name and product-level spend (product captures catch-all campaigns)
            spend = max(ads_metrics.get("spend", 0), ads.get("spend", 0))
            roas = ads.get("roas") if ads.get("spend") else ads_metrics.get("roas")
            ads_status = self._compute_ads_status(spend, roas, ads_metrics)

            brands.append({
                "brand": b["brand"],
                "revenue": b["revenue"],
                "refunds": b.get("refunds", 0),
                "revenue_prev": rev_prev,
                "revenue_yoy_pct": yoy_rev,
                "units": b["units"],
                "units_prev": units_prev,
                "units_yoy_pct": yoy_units,
                "orders": b["orders"],
                "product_count": b["product_count"],
                "gross_margin_pct": b["gross_margin_pct"],
                "estimated_margin_pct": b.get("estimated_margin_pct"),
                "cost_coverage_pct": b.get("cost_coverage_pct", 0),
                "has_cost_data": b.get("has_cost_data", False),
                "total_cogs": b.get("total_cogs", 0),
                "avg_selling_price": b["avg_selling_price"],
                "tier": tier,
                "ads_spend": spend,
                "ads_roas": roas,
                "ads_imp_share": ads_metrics.get("imp_share"),
                "ads_budget_lost": ads_metrics.get("budget_lost"),
                "ads_rank_lost": ads_metrics.get("rank_lost"),
                "ads_status": ads_status,
            })

        brands.sort(key=lambda x: x["revenue"], reverse=True)

        # Compute health scores
        brands_at_risk = 0
        for b in brands:
            health = self._compute_brand_health(b)
            b["health_score"] = health["score"]
            b["health_grade"] = health["grade"]
            b["flags"] = health["flags"]
            b["revenue_at_risk"] = health["revenue_at_risk"]
            if health["severity"] == "critical":
                brands_at_risk += 1

        tier_counts = {"growing": 0, "stable": 0, "declining": 0}
        for b in brands:
            tier_counts[b["tier"]] += 1

        total_rev = sum(b["revenue"] for b in brands)
        total_rev_prev = sum(b["revenue_prev"] for b in brands)

        # Best / worst among brands with meaningful revenue
        meaningful = [b for b in brands if b["revenue"] > 500 and b["revenue_yoy_pct"] is not None]
        best = max(meaningful, key=lambda b: b["revenue_yoy_pct"]) if meaningful else None
        worst = min(meaningful, key=lambda b: b["revenue_yoy_pct"]) if meaningful else None

        costed_brands = [b for b in brands if b.get("has_cost_data")]
        costed_rev = sum(b["revenue"] for b in costed_brands)
        weighted_margin = sum(b["revenue"] * b["gross_margin_pct"] for b in costed_brands)
        avg_margin = round(weighted_margin / costed_rev, 1) if costed_rev else 0

        total_refunds = sum(b.get("refunds", 0) for b in brands)
        kpis = {
            "total_brands": len(brands),
            "total_revenue": round(total_rev, 2),
            "total_refunds": round(total_refunds, 2),
            "total_revenue_prev": round(total_rev_prev, 2),
            "revenue_yoy_pct": _pct_change(total_rev, total_rev_prev),
            "avg_margin_pct": avg_margin,
            "best_brand": {"brand": best["brand"], "yoy_pct": best["revenue_yoy_pct"]} if best else None,
            "worst_brand": {"brand": worst["brand"], "yoy_pct": worst["revenue_yoy_pct"]} if worst else None,
            "brands_at_risk": brands_at_risk,
        }

        # Data-coverage metadata so the UI can show freshness warnings
        cur_active_days = self._count_active_days(cur_start, cur_end)
        yoy_active_days = self._count_active_days(yoy_start, yoy_end)

        # Ads freshness
        ads_latest = None
        try:
            ads_latest_dt = self.db.query(func.max(GoogleAdsCampaign.date)).scalar()
            if ads_latest_dt:
                ads_latest = str(ads_latest_dt)
        except Exception:
            pass

        # YoY baseline warning
        yoy_warnings = []
        if yoy_active_days < cur_active_days * 0.5:
            yoy_warnings.append(
                f"YoY baseline has only {yoy_active_days} active days vs "
                f"{cur_active_days} current — growth figures may be overstated"
            )
        if ads_latest and cur_end:
            ads_lag = (cur_end.date() if hasattr(cur_end, 'date') else cur_end) - (
                datetime.strptime(ads_latest, "%Y-%m-%d").date() if isinstance(ads_latest, str) else ads_latest
            )
            if ads_lag.days > 3:
                yoy_warnings.append(
                    f"Google Ads data lags by {ads_lag.days} days — "
                    f"ads spend/ROAS may be understated for recent period"
                )

        return {
            "period_days": period_days,
            "current_start": cur_start.isoformat(),
            "current_end": cur_end.isoformat(),
            "kpis": kpis,
            "brands": brands,
            "tier_counts": tier_counts,
            "data_coverage": {
                "current_active_days": cur_active_days,
                "yoy_active_days": yoy_active_days,
                "shopify_latest": cur_end.isoformat(),
                "ads_latest": ads_latest,
                "warnings": yoy_warnings,
            },
        }

    def get_brand_detail(self, brand_name: str, period_days: int = 30) -> Dict:
        now = self._anchored_now()
        cur_start = now - timedelta(days=period_days)
        cur_end = now
        yoy_start = cur_start - timedelta(days=365)
        yoy_end = cur_end - timedelta(days=365)

        # Monthly comparison (24 months)
        monthly = self._monthly_breakdown(brand_name, now - timedelta(days=730), now)

        # Product breakdowns
        cur_products = self._product_breakdown(brand_name, cur_start, cur_end)
        yoy_products = self._product_breakdown(brand_name, yoy_start, yoy_end)

        cur_map = {p["product_id"]: p for p in cur_products}
        yoy_map = {p["product_id"]: p for p in yoy_products}

        top_products = sorted(cur_products, key=lambda p: p["revenue"], reverse=True)[:10]

        declining = []
        for pid, cp in cur_map.items():
            yp = yoy_map.get(pid)
            if yp and yp["revenue"] > 100:
                chg = _pct_change(cp["revenue"], yp["revenue"])
                if chg is not None and chg < -20:
                    declining.append({**cp, "yoy_pct": chg, "revenue_prev": yp["revenue"]})
        declining.sort(key=lambda p: p.get("yoy_pct", 0))

        new_products = [p for p in cur_products if p["product_id"] not in yoy_map and p["revenue"] > 0]
        new_products.sort(key=lambda p: p["revenue"], reverse=True)

        lost_products = [p for p in yoy_products if p["product_id"] not in cur_map and p["revenue"] > 0]
        lost_products.sort(key=lambda p: p["revenue"], reverse=True)

        # Feature 1: Classify lost product recoverability
        lost_pids = [p["product_id"] for p in lost_products]
        sku_statuses = self._classify_sku_status(lost_pids, brand_name)
        for p in lost_products:
            st = sku_statuses.get(p["product_id"], {})
            p["sku_status"] = st.get("status", "unknown")
            p["sku_status_reason"] = st.get("reason", "")
            p["recoverable"] = st.get("recoverable", True)

        # Feature 5: Cannibalization check for new products
        new_pids = [p["product_id"] for p in new_products]
        cannibalization = None
        try:
            cannibalization = self._detect_cannibalization(brand_name, new_pids, cur_map, yoy_map)
        except Exception:
            pass

        # Totals for WHY
        cur_totals = self._brand_totals(brand_name, cur_start, cur_end)
        yoy_totals = self._brand_totals(brand_name, yoy_start, yoy_end)

        why = self._analyze_brand_drivers(
            brand_name, cur_map, yoy_map, cur_totals, yoy_totals,
            cur_start, cur_end, yoy_start, yoy_end,
        )

        # Add confidence to WHY drivers
        shared_count = len(set(cur_map.keys()) & set(yoy_map.keys()))
        cogs_with = sum(1 for p in cur_products if p.get("cogs", 0) > 0)
        cogs_coverage = cogs_with / len(cur_products) if cur_products else 0
        for d in why.get("drivers", []):
            d["confidence"] = self._compute_driver_confidence(d, shared_count, cogs_coverage)

        # Enrich WHY drivers with SKU status breakdown (Feature 1) and cannibalization (Feature 5)
        for d in why.get("drivers", []):
            if d["driver"] == "lost_products" and sku_statuses:
                recoverable_ids = [pid for pid in lost_pids if sku_statuses.get(pid, {}).get("recoverable", True)]
                recoverable_rev = sum(yoy_map.get(p, {}).get("revenue", 0) for p in recoverable_ids)
                total_lost = abs(d["impact_dollars"])
                d["recoverable_dollars"] = round(recoverable_rev, 2)
                d["permanent_dollars"] = round(total_lost - recoverable_rev, 2)
                d["sku_breakdown"] = {
                    s: sum(1 for pid in lost_pids if sku_statuses.get(pid, {}).get("status") == s)
                    for s in ("permanent_delist", "supplier_discontinued", "relaunch_candidate", "active")
                }
            elif d["driver"] == "new_products" and cannibalization and cannibalization.get("cannibalization_detected"):
                d["cannibalization_risk"] = {
                    "pct": cannibalization["estimated_cannibalized_pct"],
                    "dollars": cannibalization["estimated_cannibalized_dollars"],
                }

        # Diagnostics
        diagnostics = {}
        try:
            diagnostics["pricing"] = self._get_pricing_diagnostic(brand_name)
        except Exception:
            diagnostics["pricing"] = None
        try:
            diagnostics["stock"] = self._get_stock_health(brand_name, period_days=period_days)
        except Exception:
            diagnostics["stock"] = None
        try:
            diagnostics["ads"] = self._get_ads_diagnostic(brand_name, cur_start, cur_end)
        except Exception:
            diagnostics["ads"] = None
        try:
            diagnostics["demand"] = self._get_demand_signals(brand_name, cur_start, cur_end, yoy_start, yoy_end)
        except Exception:
            diagnostics["demand"] = None
        try:
            diagnostics["conversion"] = self._get_conversion_signals(brand_name, cur_start, cur_end)
        except Exception:
            diagnostics["conversion"] = None

        try:
            diagnostics["ads_yoy"] = self._get_brand_ad_data(
                brand_name, cur_start, cur_end, yoy_start, yoy_end
            )
        except Exception:
            diagnostics["ads_yoy"] = None

        # Feature 3: Demand quality (branded vs non-branded)
        try:
            diagnostics["demand_quality"] = self._get_demand_quality(
                brand_name, cur_start, cur_end, yoy_start, yoy_end
            )
        except Exception:
            diagnostics["demand_quality"] = None

        # Feature 6: Price elasticity
        try:
            diagnostics["price_elasticity"] = self._get_price_change_events(brand_name)
        except Exception:
            diagnostics["price_elasticity"] = None

        insights = self._synthesize_insights(
            brand_name,
            cur_totals,
            yoy_totals,
            why,
            diagnostics,
        )

        # Recommendations — generated AFTER diagnostics so we can cross-reference
        recs = self._generate_recommendations(
            brand_name, why, cur_products, yoy_map, cur_totals, yoy_totals,
            diagnostics=diagnostics,
        )

        ads_spend = 0
        if diagnostics.get("ads") and diagnostics["ads"].get("campaign_spend") is not None:
            ads_spend = diagnostics["ads"]["campaign_spend"]

        total_rev = self._total_revenue(cur_start, cur_end)
        overhead_total = self._get_period_overhead_ex_shipping(cur_start, cur_end)
        overhead_alloc = 0
        if total_rev > 0 and overhead_total > 0:
            overhead_alloc = round(overhead_total * (cur_totals["revenue"] / total_rev), 2)

        # Shipping cost from Shippit (per-order, allocated by brand revenue share)
        shipping_data = self._get_brand_shipping_cost(brand_name, cur_start, cur_end)
        shipping_cost = shipping_data["total_cost"]

        # Customer-paid shipping revenue (what customers paid at checkout)
        shipping_revenue = self._get_brand_shipping_revenue(brand_name, cur_start, cur_end)

        net_margin = None
        contribution_margin = None
        total_rev_incl_shipping = cur_totals["revenue"] + shipping_revenue
        if total_rev_incl_shipping > 0:
            contribution_margin = round(
                total_rev_incl_shipping - cur_totals["total_cogs"] - ads_spend
                - overhead_alloc - shipping_cost, 2
            )
            net_margin = round(contribution_margin / total_rev_incl_shipping * 100, 1)

        # Feature 2: Contribution margin waterfall
        gross_margin_abs = round(cur_totals["revenue"] - cur_totals["total_cogs"], 2)
        waterfall = {
            "gross_revenue": cur_totals.get("gross_revenue", 0),
            "less_discounts": cur_totals.get("discounts", 0),
            "less_refunds": cur_totals.get("refunds", 0),
            "net_revenue": cur_totals["revenue"],
            "less_cogs": cur_totals["total_cogs"],
            "gross_margin": gross_margin_abs,
            "gross_margin_pct": cur_totals["gross_margin_pct"],
            "less_ads": round(ads_spend, 2),
            "less_shipping_cost": round(shipping_cost, 2),
            "plus_shipping_revenue": round(shipping_revenue, 2),
            "less_overhead": round(overhead_alloc, 2),
            "contribution_margin": contribution_margin or 0,
            "contribution_margin_pct": net_margin,
            "cost_coverage_pct": cur_totals.get("cost_coverage_pct", 0),
        }

        # Data coverage (compute before rec enrichment)
        data_coverage = {
            "current_active_days": self._count_active_days(cur_start, cur_end),
            "yoy_active_days": self._count_active_days(yoy_start, yoy_end),
            "shopify_latest": cur_end.isoformat(),
            "current_start": cur_start.isoformat(),
            "current_end": cur_end.isoformat(),
        }

        # Feature 7: Enrich recs with confidence + data freshness
        recs = self._enrich_rec_confidence(recs, data_coverage, diagnostics)

        # Feature 4: Operational feasibility on recommendations
        # Note: operational_feasibility (OOS badges) removed — OOS is not
        # treated as a constraint for this business.

        return {
            "brand": brand_name,
            "period_days": period_days,
            "monthly_comparison": monthly,
            "top_products": top_products[:10],
            "declining_products": declining[:10],
            "new_products": new_products[:10],
            "lost_products": lost_products[:10],
            "cannibalization": cannibalization,
            "why_analysis": why,
            "insights": insights,
            "recommendations": recs,
            "diagnostics": diagnostics,
            "summary": {
                "current_revenue": cur_totals["revenue"],
                "prev_revenue": yoy_totals["revenue"],
                "revenue_yoy_pct": _pct_change(cur_totals["revenue"], yoy_totals["revenue"]),
                "current_units": cur_totals["units"],
                "prev_units": yoy_totals["units"],
                "gross_margin_pct": cur_totals["gross_margin_pct"],
                "cost_coverage_pct": cur_totals.get("cost_coverage_pct", 0),
                "estimated_margin_pct": cur_totals.get("estimated_margin_pct"),
                "has_cost_data": cur_totals.get("has_cost_data", False),
                "ads_spend": ads_spend,
                "overhead_allocated": overhead_alloc,
                "shipping_revenue": shipping_revenue,
                "shipping_cost": shipping_cost,
                "shipping_net": round(shipping_revenue - shipping_cost, 2),
                "shipping_coverage_pct": round(
                    shipping_data["orders_with_cost"]
                    / max(shipping_data["orders_total"], 1)
                    * 100,
                    1,
                ),
                "has_shipping_cost_data": shipping_data["orders_with_cost"] > 0,
                "net_margin_pct": net_margin,
                "contribution_waterfall": waterfall,
            },
            "data_coverage": data_coverage,
        }

    def get_brand_comparison(self, brand_names: List[str], period_days: int = 30) -> Dict:
        now = self._anchored_now()
        start_24m = now - timedelta(days=730)

        comparison = []
        for name in brand_names:
            monthly = self._monthly_breakdown(name, start_24m, now)
            cur_start = now - timedelta(days=period_days)
            totals = self._brand_totals(name, cur_start, now)
            yoy_totals = self._brand_totals(name, cur_start - timedelta(days=365), now - timedelta(days=365))

            comparison.append({
                "brand": name,
                "monthly": monthly,
                "revenue": totals["revenue"],
                "units": totals["units"],
                "gross_margin_pct": totals["gross_margin_pct"],
                "cost_coverage_pct": totals.get("cost_coverage_pct", 0),
                "estimated_margin_pct": totals.get("estimated_margin_pct"),
                "has_cost_data": totals.get("has_cost_data", False),
                "revenue_prev": yoy_totals["revenue"],
                "revenue_yoy_pct": _pct_change(totals["revenue"], yoy_totals["revenue"]),
            })

        return {"brands": comparison, "period_days": period_days}

    # ── private helpers ───────────────────────────────────────────

    def _base_filters(self, q, start, end):
        return q.filter(
            ShopifyOrderItem.order_date >= start,
            ShopifyOrderItem.order_date < end,
            ShopifyOrderItem.vendor.isnot(None),
            ShopifyOrderItem.vendor != '',
            ShopifyOrderItem.financial_status.notin_(['voided']),
        )

    def _count_active_days(self, start, end) -> int:
        """Count distinct days with at least one order item in the window."""
        result = self.db.query(
            func.count(func.distinct(func.date(ShopifyOrderItem.order_date)))
        ).filter(
            ShopifyOrderItem.order_date >= start,
            ShopifyOrderItem.order_date < end,
            ShopifyOrderItem.financial_status.notin_(['voided']),
        ).scalar()
        return int(result or 0)

    def _diagnose_no_sale_product_issues(self, brand: str, lost_products: List[Dict]) -> Dict:
        """
        Diagnose likely causes for products sold LY but with zero sales this period.

        This intentionally avoids assuming delist/discontinuation as the default cause.
        """
        if not lost_products:
            return {
                "count": 0,
                "lost_revenue": 0.0,
                "recoverable_estimate": 0.0,
                "causes": {},
            }

        product_ids = [p["product_id"] for p in lost_products if p.get("product_id") is not None]
        sku_status = self._classify_sku_status(product_ids, brand) if product_ids else {}

        # Catalog status lookup (to detect taken-down listings)
        product_meta = {}
        if product_ids:
            prod_rows = (
                self.db.query(
                    ShopifyProduct.shopify_product_id,
                    ShopifyProduct.status,
                    ShopifyProduct.published_at,
                )
                .filter(ShopifyProduct.shopify_product_id.in_(product_ids))
                .all()
            )
            for r in prod_rows:
                if r.shopify_product_id is None:
                    continue
                product_meta[int(r.shopify_product_id)] = {
                    "status": (r.status or "").lower(),
                    "published_at": r.published_at,
                }

        # Pricing snapshot by SKU (to detect uncompetitive price)
        skus_upper = {
            (p.get("sku") or "").strip().upper()
            for p in lost_products
            if p.get("sku")
        }
        pricing_map = {}
        latest_pricing_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
        if latest_pricing_date and skus_upper:
            pr_rows = (
                self.db.query(
                    CompetitivePricing.variant_sku,
                    CompetitivePricing.current_price,
                    CompetitivePricing.lowest_competitor_price,
                )
                .filter(
                    CompetitivePricing.vendor == brand,
                    CompetitivePricing.pricing_date == latest_pricing_date,
                    func.upper(CompetitivePricing.variant_sku).in_(list(skus_upper)),
                )
                .all()
            )
            for r in pr_rows:
                sku_u = (r.variant_sku or "").strip().upper()
                pricing_map[sku_u] = {
                    "current_price": _dec(r.current_price),
                    "lowest_competitor_price": _dec(r.lowest_competitor_price),
                }

        causes = {
            "pricing_pressure": {"label": "Pricing pressure vs competitors", "count": 0, "revenue": 0.0},
            "listing_not_live": {"label": "Listing not live (draft/archived/unpublished)", "count": 0, "revenue": 0.0},
            "intentional_or_supplier": {"label": "Likely intentional delist / supplier issue", "count": 0, "revenue": 0.0},
            "unknown_other": {"label": "Unknown / no recent sales (demand/merchandising/tracking)", "count": 0, "revenue": 0.0},
        }

        total_lost = 0.0
        for p in lost_products:
            rev = float(p.get("revenue") or 0)
            total_lost += rev
            pid = p.get("product_id")
            sku_u = (p.get("sku") or "").strip().upper()

            st = sku_status.get(pid, {}).get("status", "unknown")
            meta = product_meta.get(pid, {})
            status = meta.get("status", "")
            published_at = meta.get("published_at")
            pr = pricing_map.get(sku_u)

            # Cause precedence (OOS is not treated as a root cause)
            if status in {"draft", "archived"} or (status == "active" and published_at is None):
                cause = "listing_not_live"
            elif st in {"permanent_delist", "supplier_discontinued"}:
                cause = "intentional_or_supplier"
            elif pr and pr.get("current_price", 0) > 0 and pr.get("lowest_competitor_price", 0) > 0 and pr["current_price"] > pr["lowest_competitor_price"] * 1.03:
                cause = "pricing_pressure"
            else:
                cause = "unknown_other"

            causes[cause]["count"] += 1
            causes[cause]["revenue"] += rev

        # Weighted recoverable estimate by cause confidence
        # (OOS is not treated as a separate cause — merged into unknown_other)
        recoverable = (
            causes["pricing_pressure"]["revenue"] * 0.50
            + causes["unknown_other"]["revenue"] * 0.25
            + causes["listing_not_live"]["revenue"] * 0.20
            + causes["intentional_or_supplier"]["revenue"] * 0.05
        )

        for c in causes.values():
            c["revenue"] = round(c["revenue"], 2)

        return {
            "count": len(lost_products),
            "lost_revenue": round(total_lost, 2),
            "recoverable_estimate": round(recoverable, 2),
            "causes": causes,
            "pricing_snapshot_date": str(latest_pricing_date) if latest_pricing_date else None,
        }

    def _get_brand_term_filters(self, brand: str):
        """Return include/exclude term lists for brand matching."""
        settings = get_settings()
        allowlist = {}
        denylist = {}
        if settings.brand_term_allowlist:
            try:
                allowlist = json.loads(settings.brand_term_allowlist)
            except Exception as e:
                log.debug(f"brand_term_allowlist parse failed: {e}")
        if settings.brand_term_denylist:
            try:
                denylist = json.loads(settings.brand_term_denylist)
            except Exception as e:
                log.debug(f"brand_term_denylist parse failed: {e}")

        key = (brand or "").strip().lower()
        include_terms = [t for t in (allowlist.get(key) or []) if t]
        exclude_terms = [t for t in (denylist.get(key) or []) if t]
        allowlist_used = len(include_terms) > 0

        return include_terms, exclude_terms, allowlist_used

    def _get_ads_product_summary(self, start, end) -> Dict[str, Dict]:
        """Product-level ad spend proportionally allocated by vendor.

        PMax product performance data is an attribution metric (inflated 10-30x
        vs actual campaign cost). We use product-level ratios to allocate
        actual campaign spend proportionally across vendors.
        """
        try:
            start_d = start.date() if hasattr(start, "date") else start
            end_d = end.date() if hasattr(end, "date") else end

            # Build product_id → vendor lookup
            vendor_map = {}
            for r in self.db.query(ShopifyProduct.shopify_product_id, ShopifyProduct.vendor).filter(
                ShopifyProduct.vendor.isnot(None), ShopifyProduct.vendor != '',
            ).all():
                vendor_map[str(r.shopify_product_id)] = r.vendor

            # Get actual campaign-level spend
            camp_rows = self._get_ads_campaign_rows(start, end)
            campaign_actual = {}
            for row in camp_rows:
                name = row.get("name", "")
                campaign_actual.setdefault(name, 0)
                campaign_actual[name] += (row.get("cost_micros", 0) or 0) / 1_000_000

            # Get product spend grouped by campaign for ratio calculation
            rows = (
                self.db.query(
                    GoogleAdsProductPerformance.product_item_id,
                    GoogleAdsProductPerformance.campaign_name,
                    func.sum(GoogleAdsProductPerformance.cost_micros).label("cost"),
                    func.sum(GoogleAdsProductPerformance.conversions_value).label("conv_val"),
                )
                .filter(
                    GoogleAdsProductPerformance.date >= start_d,
                    GoogleAdsProductPerformance.date < end_d,
                )
                .group_by(
                    GoogleAdsProductPerformance.product_item_id,
                    GoogleAdsProductPerformance.campaign_name,
                )
                .all()
            )

            # Per campaign: total product attribution $ and per-vendor breakdown
            camp_total_prod = {}    # campaign -> total product-level $
            camp_vendor_prod = {}   # campaign -> {vendor: product-level $}

            for r in rows:
                camp = r.campaign_name or ""
                cost = int(r.cost or 0)
                camp_total_prod.setdefault(camp, 0)
                camp_total_prod[camp] += cost

                m = _re.match(r'shopify_au_(\d+)_\d+', r.product_item_id or '')
                if not m:
                    continue
                vendor = vendor_map.get(m.group(1))
                if not vendor:
                    continue
                camp_vendor_prod.setdefault(camp, {})
                camp_vendor_prod[camp].setdefault(vendor, 0)
                camp_vendor_prod[camp][vendor] += cost

            # Proportionally allocate actual campaign spend to vendors
            vendor_spend = {}
            for camp, vendor_breakdown in camp_vendor_prod.items():
                total_prod = camp_total_prod.get(camp, 0)
                actual = campaign_actual.get(camp, 0)
                if total_prod <= 0 or actual <= 0:
                    continue
                for vendor, vendor_prod in vendor_breakdown.items():
                    share = vendor_prod / total_prod
                    vendor_spend.setdefault(vendor, 0)
                    vendor_spend[vendor] += actual * share

            result = {}
            for vendor, spend in vendor_spend.items():
                roas = None  # ROAS less meaningful at this level with proportional allocation
                result[vendor] = {"spend": round(spend, 2), "roas": roas}
            return result
        except Exception as e:
            log.debug(f"Ads product summary skipped: {e}")
            return {}

    def _get_ads_campaign_rows(self, start, end) -> List[Dict]:
        """Fetch campaign rows for name-based brand matching."""
        try:
            rows = (
                self.db.query(
                    GoogleAdsCampaign.campaign_name,
                    GoogleAdsCampaign.impressions,
                    GoogleAdsCampaign.cost_micros,
                    GoogleAdsCampaign.conversions_value,
                    GoogleAdsCampaign.search_impression_share,
                    GoogleAdsCampaign.search_budget_lost_impression_share,
                    GoogleAdsCampaign.search_rank_lost_impression_share,
                )
                .filter(
                    GoogleAdsCampaign.date >= start.date() if hasattr(start, "date") else start,
                    GoogleAdsCampaign.date < end.date() if hasattr(end, "date") else end,
                )
                .all()
            )
            return [
                {
                    "name": r.campaign_name or "",
                    "impr": int(r.impressions or 0),
                    "cost_micros": int(r.cost_micros or 0),
                    "conv_val": _dec(r.conversions_value),
                    "imp_share": _dec(r.search_impression_share) if r.search_impression_share is not None else None,
                    "budget_lost": _dec(r.search_budget_lost_impression_share) if r.search_budget_lost_impression_share is not None else None,
                    "rank_lost": _dec(r.search_rank_lost_impression_share) if r.search_rank_lost_impression_share is not None else None,
                }
                for r in rows
            ]
        except Exception as e:
            log.debug(f"Ads campaign rows skipped: {e}")
            return []

    def _get_ads_campaign_metrics(self, brand: str, rows: List[Dict]) -> Dict:
        """Compute spend, ROAS, impression share, and lost IS for a brand via campaign name matching."""
        empty = {"spend": 0, "roas": None, "imp_share": None, "budget_lost": None, "rank_lost": None, "matched_campaigns": set()}
        include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
        if not brand or (len(brand) <= 2 and not allowlist_used):
            return empty

        brand_norm = brand.strip().lower()
        # Always match on brand name in campaign name (word-boundary regex)
        brand_pattern = _re.compile(r"\b" + _re.escape(brand_norm) + r"\b", _re.IGNORECASE)
        include_patterns = [
            _re.compile(r"\b" + _re.escape(term) + r"\b", _re.IGNORECASE)
            for term in include_terms if term
        ] if allowlist_used else []
        exclude_patterns = [
            _re.compile(r"\b" + _re.escape(term) + r"\b", _re.IGNORECASE)
            for term in exclude_terms if term
        ]
        total_impr = 0
        total_cost_micros = 0
        total_conv_val = 0.0
        imp_share_num = 0.0
        budget_lost_num = 0.0
        rank_lost_num = 0.0
        matched_campaigns = set()

        for row in rows:
            name = row.get("name", "")
            if not name:
                continue
            # Explicit campaign exclusions
            if brand_norm == "zip" and "zip search" in name.lower():
                continue
            if exclude_patterns and any(p.search(name) for p in exclude_patterns):
                continue
            # Match if brand name appears in campaign name
            if brand_pattern.search(name):
                pass
            elif include_patterns and any(p.search(name) for p in include_patterns):
                pass
            else:
                continue
            matched_campaigns.add(name)
            impr = row["impr"] or 0
            total_impr += impr
            total_cost_micros += row.get("cost_micros", 0) or 0
            total_conv_val += row.get("conv_val", 0) or 0
            if row["imp_share"] is not None:
                imp_share_num += row["imp_share"] * impr
            if row["budget_lost"] is not None:
                budget_lost_num += row["budget_lost"] * impr
            if row["rank_lost"] is not None:
                rank_lost_num += row["rank_lost"] * impr

        if total_impr == 0:
            return {**empty, "matched_campaigns": matched_campaigns}

        spend = total_cost_micros / 1_000_000
        roas = round(total_conv_val / spend, 1) if spend > 0 else None

        def _pct_safe(val):
            if val is None:
                return None
            v = _dec(val)
            if 0 < v <= 1:
                v *= 100
            return round(v, 1)

        return {
            "spend": round(spend, 2),
            "roas": roas,
            "imp_share": _pct_safe(imp_share_num / total_impr) if imp_share_num else None,
            "budget_lost": _pct_safe(budget_lost_num / total_impr) if budget_lost_num else None,
            "rank_lost": _pct_safe(rank_lost_num / total_impr) if rank_lost_num else None,
            "matched_campaigns": matched_campaigns,
        }

    def _compute_ads_status(self, spend, roas, metrics) -> str:
        """Return a compact ads status label."""
        if not spend or spend <= 0:
            return "No Ads"
        imp_share = metrics.get("imp_share")
        budget_lost = metrics.get("budget_lost")
        rank_lost = metrics.get("rank_lost")
        if roas is not None and roas >= 3 and imp_share is not None and imp_share < 80:
            return "Scale"
        if budget_lost is not None and (rank_lost is None or budget_lost >= rank_lost):
            return "Budget-limited"
        if rank_lost is not None and (budget_lost is None or rank_lost > budget_lost):
            return "Rank-limited"
        return "Active"

    def _refund_per_item_subquery(self):
        """Total refunds & refunded qty per line_item_id (all time).

        Joined to order-item queries so refunds are scoped by *order* date,
        not refund date — fixes temporal mismatch where refunds for prior-
        period orders were incorrectly subtracted from current-period revenue.
        """
        return (
            self.db.query(
                ShopifyRefundLineItem.line_item_id,
                func.sum(ShopifyRefundLineItem.subtotal).label("refund_amount"),
                func.sum(ShopifyRefundLineItem.quantity).label("refund_qty"),
            )
            .group_by(ShopifyRefundLineItem.line_item_id)
            .subquery()
        )

    def _brand_aggregates(self, start, end) -> List[Dict]:
        rpi = self._refund_per_item_subquery()
        q = self.db.query(
            ShopifyOrderItem.vendor,
            func.sum(ShopifyOrderItem.total_price).label('revenue'),
            func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))).label('discounts'),
            func.sum(func.coalesce(rpi.c.refund_amount, literal_column('0'))).label('refunds'),
            func.sum(ShopifyOrderItem.quantity).label('units'),
            func.sum(func.coalesce(rpi.c.refund_qty, literal_column('0'))).label('refund_units'),
            func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label('orders'),
            func.count(func.distinct(ShopifyOrderItem.shopify_product_id)).label('product_count'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                    else_=literal_column('0')
                )
            ).label('total_cogs'),
            func.sum(
                case(
                    (and_(ShopifyOrderItem.cost_per_item.isnot(None),
                          rpi.c.refund_qty.isnot(None)),
                     ShopifyOrderItem.cost_per_item * rpi.c.refund_qty),
                    else_=literal_column('0')
                )
            ).label('refund_cogs'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.quantity),
                    else_=literal_column('0')
                )
            ).label('units_with_cost'),
            # Revenue/discounts/refunds for costed items only (for accurate margin)
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.total_price),
                    else_=literal_column('0')
                )
            ).label('costed_revenue'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))),
                    else_=literal_column('0')
                )
            ).label('costed_discounts'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     func.coalesce(rpi.c.refund_amount, literal_column('0'))),
                    else_=literal_column('0')
                )
            ).label('costed_refunds'),
        )
        q = q.outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
        q = self._base_filters(q, start, end)
        rows = q.group_by(ShopifyOrderItem.vendor).all()

        results = []
        for r in rows:
            gross_rev = _dec(r.revenue)
            discounts = _dec(r.discounts)
            refunds = _dec(r.refunds)
            net_rev = gross_rev - discounts - refunds
            gross_cogs = _dec(r.total_cogs)
            refund_cogs = _dec(r.refund_cogs)
            net_cogs = gross_cogs - refund_cogs
            units = r.units or 0
            refund_units = int(r.refund_units or 0)
            net_units = units - refund_units
            units_costed = int(r.units_with_cost or 0)
            cost_coverage = round(units_costed / units * 100, 1) if units > 0 else 0
            asp = round(net_rev / net_units, 2) if net_units > 0 else 0

            # Margin: use only costed-item revenue vs costed-item COGS
            # so we compare like-for-like instead of partial cost / full revenue.
            costed_net_rev = _dec(r.costed_revenue) - _dec(r.costed_discounts) - _dec(r.costed_refunds)
            margin = round((costed_net_rev - net_cogs) / costed_net_rev * 100, 1) if costed_net_rev > 0 and net_cogs > 0 else 0
            estimated_margin = margin if 0 < cost_coverage < 100 else None

            results.append({
                "brand": r.vendor,
                "revenue": round(net_rev, 2),
                "refunds": round(refunds, 2),
                "units": net_units,
                "orders": r.orders or 0,
                "product_count": r.product_count or 0,
                "total_cogs": round(net_cogs, 2),
                "gross_margin_pct": margin,
                "cost_coverage_pct": cost_coverage,
                "estimated_margin_pct": estimated_margin,
                "has_cost_data": net_cogs > 0,
                "avg_selling_price": asp,
            })
        return results

    def _brand_totals(self, brand: str, start, end) -> Dict:
        rpi = self._refund_per_item_subquery()
        q = self.db.query(
            func.sum(ShopifyOrderItem.total_price).label('revenue'),
            func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))).label('discounts'),
            func.sum(func.coalesce(rpi.c.refund_amount, literal_column('0'))).label('refunds'),
            func.sum(ShopifyOrderItem.quantity).label('units'),
            func.sum(func.coalesce(rpi.c.refund_qty, literal_column('0'))).label('refund_units'),
            func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label('orders'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                    else_=literal_column('0')
                )
            ).label('total_cogs'),
            func.sum(
                case(
                    (and_(ShopifyOrderItem.cost_per_item.isnot(None),
                          rpi.c.refund_qty.isnot(None)),
                     ShopifyOrderItem.cost_per_item * rpi.c.refund_qty),
                    else_=literal_column('0')
                )
            ).label('refund_cogs'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.quantity),
                    else_=literal_column('0')
                )
            ).label('units_with_cost'),
            # Revenue/discounts/refunds for costed items only (for accurate margin)
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.total_price),
                    else_=literal_column('0')
                )
            ).label('costed_revenue'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))),
                    else_=literal_column('0')
                )
            ).label('costed_discounts'),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     func.coalesce(rpi.c.refund_amount, literal_column('0'))),
                    else_=literal_column('0')
                )
            ).label('costed_refunds'),
        ).outerjoin(
            rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id
        ).filter(
            ShopifyOrderItem.vendor == brand,
            ShopifyOrderItem.order_date >= start,
            ShopifyOrderItem.order_date < end,
            ShopifyOrderItem.financial_status.notin_(['voided']),
        )
        r = q.first()
        gross_rev = _dec(r.revenue) if r else 0
        discounts = _dec(r.discounts) if r else 0
        refunds = _dec(r.refunds) if r else 0
        net_rev = gross_rev - discounts - refunds
        gross_cogs = _dec(r.total_cogs) if r else 0
        refund_cogs = _dec(r.refund_cogs) if r else 0
        net_cogs = gross_cogs - refund_cogs
        units = (r.units or 0) if r else 0
        refund_units = int(r.refund_units or 0) if r else 0
        net_units = units - refund_units
        units_costed = int(r.units_with_cost or 0) if r else 0
        cost_coverage = round(units_costed / units * 100, 1) if units > 0 else 0
        # Margin: use costed-item revenue vs costed-item COGS (like-for-like)
        costed_net_rev = _dec(r.costed_revenue) - _dec(r.costed_discounts) - _dec(r.costed_refunds) if r else 0
        margin = round((costed_net_rev - net_cogs) / costed_net_rev * 100, 1) if costed_net_rev > 0 and net_cogs > 0 else 0
        estimated_margin = margin if 0 < cost_coverage < 100 else None

        return {
            "revenue": round(net_rev, 2),
            "gross_revenue": round(gross_rev, 2),
            "discounts": round(discounts, 2),
            "refunds": round(refunds, 2),
            "units": net_units,
            "orders": (r.orders or 0) if r else 0,
            "total_cogs": round(net_cogs, 2),
            "gross_margin_pct": margin,
            "cost_coverage_pct": cost_coverage,
            "estimated_margin_pct": estimated_margin,
            "has_cost_data": net_cogs > 0,
        }

    def _total_revenue(self, start, end) -> float:
        """Net revenue across all brands for a period (for allocation)."""
        rpi = self._refund_per_item_subquery()
        r = (
            self.db.query(
                func.sum(ShopifyOrderItem.total_price).label('revenue'),
                func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))).label('discounts'),
                func.sum(func.coalesce(rpi.c.refund_amount, literal_column('0'))).label('refunds'),
            )
            .outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
            .filter(
                ShopifyOrderItem.order_date >= start,
                ShopifyOrderItem.order_date < end,
                ShopifyOrderItem.financial_status.notin_(['voided']),
            )
            .first()
        )
        gross = _dec(r.revenue) if r else 0
        discounts = _dec(r.discounts) if r else 0
        refunds = _dec(r.refunds) if r else 0
        return round(gross - discounts - refunds, 2)

    def _get_period_overhead_ex_shipping(self, start, end) -> float:
        """Operating expenses excluding ads and shipping, summed across months."""
        # Normalize to month starts
        start_month = datetime(start.year, start.month, 1)
        end_month = datetime(end.year, end.month, 1)
        months = []
        cur = start_month
        while cur <= end_month:
            months.append(cur.date())
            if cur.month == 12:
                cur = datetime(cur.year + 1, 1, 1)
            else:
                cur = datetime(cur.year, cur.month + 1, 1)

        if not months:
            return 0.0

        total = (
            self.db.query(
                func.sum(
                    func.coalesce(MonthlyPL.payroll, 0)
                    + func.coalesce(MonthlyPL.rent, 0)
                    + func.coalesce(MonthlyPL.utilities, 0)
                    + func.coalesce(MonthlyPL.insurance, 0)
                    + func.coalesce(MonthlyPL.software, 0)
                    + func.coalesce(MonthlyPL.marketing_other, 0)
                    + func.coalesce(MonthlyPL.professional_services, 0)
                    + func.coalesce(MonthlyPL.other_expenses, 0)
                )
            )
            .filter(MonthlyPL.month.in_(months))
            .scalar()
        )
        return float(total or 0)

    def _get_brand_shipping_cost(self, brand: str, start, end) -> Dict:
        """
        Get shipping costs allocated to a brand by revenue share per order.

        For each order with Shippit data, calculates the brand's share of
        shipping cost proportional to the brand's revenue in that order.
        This avoids double-counting for multi-brand orders and inflation
        from multiple line items.
        """
        try:
            # Count total brand orders in period
            total_brand_orders = (
                self.db.query(
                    func.count(func.distinct(ShopifyOrderItem.shopify_order_id))
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                    ShopifyOrderItem.financial_status.in_(
                        ["paid", "partially_refunded", "refunded"]
                    ),
                )
                .scalar()
                or 0
            )

            # Get distinct orders with Shippit cost that contain this brand
            order_ids_with_brand = (
                self.db.query(
                    func.distinct(ShopifyOrderItem.shopify_order_id)
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                    ShopifyOrderItem.financial_status.in_(
                        ["paid", "partially_refunded", "refunded"]
                    ),
                )
            )

            shippit_orders = (
                self.db.query(ShippitOrder)
                .filter(
                    ShippitOrder.shopify_order_id.in_(order_ids_with_brand),
                    ShippitOrder.shipping_cost.isnot(None),
                )
                .all()
            )

            total_cost = 0.0
            orders_with_cost = 0

            for so in shippit_orders:
                ship_cost = float(so.shipping_cost)
                # Get all items in this order to calculate brand's revenue share
                items = (
                    self.db.query(
                        ShopifyOrderItem.vendor,
                        func.sum(
                            ShopifyOrderItem.price * ShopifyOrderItem.quantity
                        ).label("rev"),
                    )
                    .filter(
                        ShopifyOrderItem.shopify_order_id == so.shopify_order_id
                    )
                    .group_by(ShopifyOrderItem.vendor)
                    .all()
                )
                order_rev = sum(float(i.rev or 0) for i in items)
                brand_rev = sum(
                    float(i.rev or 0) for i in items if i.vendor == brand
                )
                share = brand_rev / order_rev if order_rev > 0 else 0
                total_cost += ship_cost * share
                orders_with_cost += 1

            return {
                "total_cost": round(total_cost, 2),
                "orders_with_cost": orders_with_cost,
                "orders_total": total_brand_orders,
            }
        except Exception as e:
            log.debug(f"Shipping cost lookup failed for {brand}: {e}")
            return {"total_cost": 0, "orders_with_cost": 0, "orders_total": 0}

    def _get_brand_shipping_revenue(self, brand: str, start, end) -> float:
        """
        Get customer-paid shipping revenue allocated to a brand by revenue share.

        Uses ShopifyOrder.total_shipping (what customer paid at checkout),
        allocated proportionally by brand's product revenue in each order.
        """
        try:
            order_ids_with_brand = (
                self.db.query(
                    func.distinct(ShopifyOrderItem.shopify_order_id)
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                    ShopifyOrderItem.financial_status.in_(
                        ["paid", "partially_refunded", "refunded"]
                    ),
                )
            ).subquery()

            # Get orders with shipping charges
            orders_with_shipping = (
                self.db.query(
                    ShopifyOrder.shopify_order_id,
                    ShopifyOrder.total_shipping,
                )
                .filter(
                    ShopifyOrder.shopify_order_id.in_(
                        self.db.query(order_ids_with_brand)
                    ),
                    ShopifyOrder.total_shipping > 0,
                )
                .all()
            )

            total_shipping_rev = 0.0
            for order in orders_with_shipping:
                shipping_charged = float(order.total_shipping or 0)
                # Allocate by brand revenue share in this order
                items = (
                    self.db.query(
                        ShopifyOrderItem.vendor,
                        func.sum(
                            ShopifyOrderItem.price * ShopifyOrderItem.quantity
                        ).label("rev"),
                    )
                    .filter(
                        ShopifyOrderItem.shopify_order_id == order.shopify_order_id
                    )
                    .group_by(ShopifyOrderItem.vendor)
                    .all()
                )
                order_rev = sum(float(i.rev or 0) for i in items)
                brand_rev = sum(
                    float(i.rev or 0) for i in items if i.vendor == brand
                )
                share = brand_rev / order_rev if order_rev > 0 else 0
                total_shipping_rev += shipping_charged * share

            return round(total_shipping_rev, 2)
        except Exception as e:
            log.debug(f"Shipping revenue lookup failed for {brand}: {e}")
            return 0.0

    def _monthly_breakdown(self, brand: str, start, end) -> List[Dict]:
        rpi = self._refund_per_item_subquery()
        yr_col = extract('year', ShopifyOrderItem.order_date).label('yr')
        mo_col = extract('month', ShopifyOrderItem.order_date).label('mo')
        rows = (
            self.db.query(
                yr_col,
                mo_col,
                func.sum(ShopifyOrderItem.total_price).label('revenue'),
                func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))).label('discounts'),
                func.sum(func.coalesce(rpi.c.refund_amount, literal_column('0'))).label('refunds'),
                func.sum(ShopifyOrderItem.quantity).label('units'),
                func.sum(func.coalesce(rpi.c.refund_qty, literal_column('0'))).label('refund_units'),
            )
            .outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
            .filter(
                ShopifyOrderItem.vendor == brand,
                ShopifyOrderItem.order_date >= start,
                ShopifyOrderItem.order_date < end,
                ShopifyOrderItem.financial_status.notin_(['voided']),
            )
            .group_by(yr_col, mo_col)
            .order_by(yr_col, mo_col)
            .all()
        )

        # Build {(year, month): {revenue, units}}
        by_ym = {}
        for r in rows:
            yr = str(int(r.yr))
            mo = f"{int(r.mo):02d}"
            gross = _dec(r.revenue)
            discounts = _dec(r.discounts)
            refunds = _dec(r.refunds)
            net_units = (r.units or 0) - int(r.refund_units or 0)
            by_ym[(yr, mo)] = {"revenue": gross - discounts - refunds, "units": net_units}

        # Determine the two years we're comparing
        this_year = str(end.year)
        last_year = str(end.year - 1)
        months = [f"{m:02d}" for m in range(1, 13)]

        result = []
        for mo in months:
            result.append({
                "month": mo,
                "month_label": f"{_month_name(mo)}",
                "this_year": round(by_ym.get((this_year, mo), {}).get("revenue", 0), 2),
                "this_year_units": by_ym.get((this_year, mo), {}).get("units", 0),
                "last_year": round(by_ym.get((last_year, mo), {}).get("revenue", 0), 2),
                "last_year_units": by_ym.get((last_year, mo), {}).get("units", 0),
            })
        return result

    def _product_breakdown(self, brand: str, start, end) -> List[Dict]:
        rpi = self._refund_per_item_subquery()
        rows = (
            self.db.query(
                ShopifyOrderItem.shopify_product_id,
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku,
                func.sum(ShopifyOrderItem.total_price).label('revenue'),
                func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column('0'))).label('discounts'),
                func.sum(func.coalesce(rpi.c.refund_amount, literal_column('0'))).label('refunds'),
                func.sum(ShopifyOrderItem.quantity).label('units'),
                func.sum(func.coalesce(rpi.c.refund_qty, literal_column('0'))).label('refund_units'),
                func.avg(ShopifyOrderItem.price).label('avg_price'),
                func.sum(
                    case(
                        (ShopifyOrderItem.cost_per_item.isnot(None),
                         ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                        else_=literal_column('0')
                    )
                ).label('cogs'),
                func.sum(
                    case(
                        (and_(ShopifyOrderItem.cost_per_item.isnot(None),
                              rpi.c.refund_qty.isnot(None)),
                         ShopifyOrderItem.cost_per_item * rpi.c.refund_qty),
                        else_=literal_column('0')
                    )
                ).label('refund_cogs'),
            )
            .outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
            .filter(
                ShopifyOrderItem.vendor == brand,
                ShopifyOrderItem.order_date >= start,
                ShopifyOrderItem.order_date < end,
                ShopifyOrderItem.financial_status.notin_(['voided']),
            )
            .group_by(
                ShopifyOrderItem.shopify_product_id,
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku,
            )
            .all()
        )
        results = []
        for r in rows:
            gross_rev = _dec(r.revenue)
            discounts = _dec(r.discounts)
            refunds = _dec(r.refunds)
            net_rev = gross_rev - discounts - refunds
            gross_cogs = _dec(r.cogs)
            refund_cogs = _dec(r.refund_cogs)
            net_cogs = gross_cogs - refund_cogs
            net_units = (r.units or 0) - int(r.refund_units or 0)
            margin = round((net_rev - net_cogs) / net_rev * 100, 1) if net_rev > 0 and net_cogs > 0 else 0
            results.append({
                "product_id": r.shopify_product_id,
                "title": r.title or "Unknown",
                "sku": r.sku or "",
                "revenue": round(net_rev, 2),
                "refunds": round(refunds, 2),
                "units": net_units,
                "avg_price": round(_dec(r.avg_price), 2),
                "cogs": round(net_cogs, 2),
                "gross_margin_pct": margin,
            })
        return results

    # ── WHY analysis ─────────────────────────────────────────────

    def _analyze_brand_drivers(self, brand, cur_map, yoy_map, cur_totals, yoy_totals,
                               cur_start=None, cur_end=None, yoy_start=None, yoy_end=None) -> Dict:
        total_change = cur_totals["revenue"] - yoy_totals["revenue"]
        total_change_pct = _pct_change(cur_totals["revenue"], yoy_totals["revenue"])

        drivers = []

        # Shared products
        shared_ids = set(cur_map.keys()) & set(yoy_map.keys())

        # 1. Volume effect
        volume_effect = 0.0
        for pid in shared_ids:
            c = cur_map[pid]
            y = yoy_map[pid]
            volume_effect += (c["units"] - y["units"]) * y["avg_price"]
        if abs(volume_effect) > 0:
            direction = "positive" if volume_effect > 0 else "negative"
            unit_change = sum(cur_map[p]["units"] - yoy_map[p]["units"] for p in shared_ids)
            # Explanation must reflect the dollar sign, not the unit sign,
            # because product mix can make +units produce -$ (high-ASP losses outweigh low-ASP gains).
            if volume_effect > 0:
                vol_expl = f"Unit volume shifts on existing products added ${abs(volume_effect):,.0f} (net {unit_change:+,} units)"
            else:
                vol_expl = f"Unit volume shifts on existing products reduced revenue by ${abs(volume_effect):,.0f} (net {unit_change:+,} units)"
            drivers.append({
                "driver": "volume",
                "label": "Sales Volume",
                "impact_dollars": round(volume_effect, 2),
                "direction": direction,
                "explanation": vol_expl,
            })

        # 2. Price effect
        price_effect = 0.0
        for pid in shared_ids:
            c = cur_map[pid]
            y = yoy_map[pid]
            price_effect += (c["avg_price"] - y["avg_price"]) * y["units"]
        if abs(price_effect) > 0:
            direction = "positive" if price_effect > 0 else "negative"
            drivers.append({
                "driver": "price",
                "label": "Pricing Changes",
                "impact_dollars": round(price_effect, 2),
                "direction": direction,
                "explanation": f"Average selling price changes contributed ${abs(price_effect):,.0f} {'gain' if price_effect > 0 else 'loss'}",
            })

        # 3. New products
        new_ids = set(cur_map.keys()) - set(yoy_map.keys())
        new_rev = sum(cur_map[p]["revenue"] for p in new_ids)
        if new_rev > 0:
            drivers.append({
                "driver": "new_products",
                "label": "New Products",
                "impact_dollars": round(new_rev, 2),
                "direction": "positive",
                "explanation": f"{len(new_ids)} new product{'s' if len(new_ids) != 1 else ''} contributed ${new_rev:,.0f} in revenue",
            })

        # 4. Lost products
        lost_ids = set(yoy_map.keys()) - set(cur_map.keys())
        lost_rev = sum(yoy_map[p]["revenue"] for p in lost_ids)
        if lost_rev > 0:
            drivers.append({
                "driver": "lost_products",
                "label": "Discontinued Products",
                "impact_dollars": round(-lost_rev, 2),
                "direction": "negative",
                "explanation": f"{len(lost_ids)} product{'s' if len(lost_ids) != 1 else ''} no longer selling, losing ${lost_rev:,.0f}",
            })

        # 5. Mix effect (residual)
        explained = volume_effect + price_effect + new_rev - lost_rev
        mix_effect = total_change - explained
        if abs(mix_effect) > 50:
            direction = "positive" if mix_effect > 0 else "negative"
            drivers.append({
                "driver": "mix",
                "label": "Product Mix Shift",
                "impact_dollars": round(mix_effect, 2),
                "direction": direction,
                "explanation": f"Shift in product mix contributed ${abs(mix_effect):,.0f} {'gain' if mix_effect > 0 else 'loss'}",
            })

        # 6. Margin effect
        cur_margin = cur_totals["gross_margin_pct"]
        yoy_margin = yoy_totals["gross_margin_pct"]
        margin_change = cur_margin - yoy_margin
        if abs(margin_change) > 1:
            direction = "positive" if margin_change > 0 else "negative"
            drivers.append({
                "driver": "margin",
                "label": "Cost / Margin",
                "impact_dollars": round(margin_change * cur_totals["revenue"] / 100, 2),
                "direction": direction,
                "explanation": f"Gross margin moved from {yoy_margin:.1f}% to {cur_margin:.1f}% ({'+' if margin_change > 0 else ''}{margin_change:.1f}pp)",
            })

        # 7. Ad spend effect (YoY change in spend + ROAS)
        try:
            ad_data = self._get_brand_ad_data(brand, cur_start, cur_end, yoy_start, yoy_end)
            if ad_data:
                drivers.append(ad_data)
        except Exception as e:
            log.debug(f"Ad spend analysis skipped for {brand}: {e}")

        # 8. Competitive effect
        try:
            comp_data = self._get_competitive_pressure(brand)
            if comp_data:
                drivers.append(comp_data)
        except Exception as e:
            log.debug(f"Competitive analysis skipped for {brand}: {e}")

        # Sort by absolute impact
        drivers.sort(key=lambda d: abs(d["impact_dollars"]), reverse=True)

        summary = self._build_summary(brand, total_change_pct, drivers)

        return {
            "summary": summary,
            "total_change_dollars": round(total_change, 2),
            "total_change_pct": total_change_pct,
            "drivers": drivers,
        }

    def _get_brand_ad_data(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        """Get ad spend driver — compares current vs prior period spend and ROAS."""
        if not cur_start or not cur_end:
            return None

        # Get product IDs for this brand
        brand_pids = (
            self.db.query(func.cast(ShopifyProduct.shopify_product_id, String))
            .filter(ShopifyProduct.vendor == brand)
            .all()
        )
        if not brand_pids:
            return None

        pid_list = [str(r[0]) for r in brand_pids]

        def _ad_agg(start, end):
            row = (
                self.db.query(
                    func.sum(GoogleAdsProductPerformance.cost_micros).label('cost'),
                    func.sum(GoogleAdsProductPerformance.conversions).label('conv'),
                    func.sum(GoogleAdsProductPerformance.conversions_value).label('conv_val'),
                    func.sum(GoogleAdsProductPerformance.clicks).label('clicks'),
                    func.sum(GoogleAdsProductPerformance.impressions).label('impr'),
                )
                .filter(
                    GoogleAdsProductPerformance.product_item_id.in_(pid_list),
                    GoogleAdsProductPerformance.date >= start.date() if hasattr(start, 'date') else start,
                    GoogleAdsProductPerformance.date < end.date() if hasattr(end, 'date') else end,
                )
                .first()
            )
            spend = _dec(row.cost) / 1_000_000 if row and row.cost else 0
            conv = _dec(row.conv) if row else 0
            conv_val = _dec(row.conv_val) if row else 0
            clicks = int(row.clicks or 0) if row else 0
            impr = int(row.impr or 0) if row else 0
            roas = conv_val / spend if spend > 0 else 0
            return {"spend": spend, "conv": conv, "conv_val": conv_val, "clicks": clicks, "impr": impr, "roas": roas}

        cur = _ad_agg(cur_start, cur_end)
        prev = _ad_agg(yoy_start, yoy_end) if yoy_start and yoy_end else {"spend": 0, "conv": 0, "conv_val": 0, "roas": 0}

        if cur["spend"] == 0 and prev["spend"] == 0:
            return None

        spend_change = cur["spend"] - prev["spend"]
        conv_val_change = cur["conv_val"] - prev["conv_val"]
        spend_change_pct = _pct_change(cur["spend"], prev["spend"])

        # The impact is the change in ad-driven revenue minus the change in cost
        impact = conv_val_change - spend_change

        parts = []
        if cur["spend"] > 0:
            parts.append(f"${cur['spend']:,.0f} ad spend (ROAS {cur['roas']:.1f}x)")
        if prev["spend"] > 0:
            spend_chg_pct = _pct_change(cur["spend"], prev["spend"])
            if spend_chg_pct is not None:
                parts.append(f"spend {'up' if spend_chg_pct > 0 else 'down'} {abs(spend_chg_pct):.0f}% vs last year")
        if cur["conv"] > 0:
            parts.append(f"{cur['conv']:.0f} conversions worth ${cur['conv_val']:,.0f}")

        direction = "positive" if impact > 0 else ("negative" if impact < 0 else "neutral")

        return {
            "driver": "ad_spend",
            "label": "Advertising",
            "metric_scope": "product-level",
            "impact_dollars": round(impact, 2),
            "direction": direction,
            "explanation": ". ".join(parts) if parts else f"${cur['spend']:,.0f} ad spend",
            "cur_spend": round(cur["spend"], 2),
            "prev_spend": round(prev["spend"], 2),
            "spend_change_pct": spend_change_pct,
            "cur_roas": round(cur["roas"], 2) if cur["roas"] is not None else None,
            "prev_roas": round(prev["roas"], 2) if prev["roas"] is not None else None,
        }

    def _get_competitive_pressure(self, brand) -> Optional[Dict]:
        """Check competitive pricing pressure for a brand."""
        latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
        if not latest_date:
            return None

        total = (
            self.db.query(func.count(CompetitivePricing.id))
            .filter(CompetitivePricing.vendor == brand, CompetitivePricing.pricing_date == latest_date)
            .scalar()
        ) or 0

        if total == 0:
            return None

        undercut = (
            self.db.query(func.count(CompetitivePricing.id))
            .filter(
                CompetitivePricing.vendor == brand,
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.is_below_minimum == True,
            )
            .scalar()
        ) or 0

        losing = (
            self.db.query(func.count(CompetitivePricing.id))
            .filter(
                CompetitivePricing.vendor == brand,
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.is_losing_money == True,
            )
            .scalar()
        ) or 0

        if undercut == 0 and losing == 0:
            return None

        parts = []
        if undercut:
            parts.append(f"{undercut} of {total} SKUs priced below minimum")
        if losing:
            parts.append(f"{losing} SKUs selling at a loss")

        return {
            "driver": "competitive",
            "label": "Competitive Pressure",
            "impact_dollars": 0,
            "direction": "negative",
            "explanation": ". ".join(parts),
        }

    def _build_summary(self, brand, total_change_pct, drivers) -> str:
        if total_change_pct is None:
            if not drivers:
                return f"{brand} is a new brand with no prior year comparison data."
            return f"{brand} has no prior year data for comparison. {drivers[0]['explanation']}."

        direction = "increased" if total_change_pct > 0 else "decreased"
        parts = [f"{brand} revenue {direction} {abs(total_change_pct):.0f}% year-on-year"]

        for d in drivers[:3]:
            if abs(d["impact_dollars"]) > 0:
                parts.append(d["explanation"])

        return ". ".join(parts) + "."

    def _synthesize_insights(self, brand, cur_totals, yoy_totals, why, diagnostics) -> List[Dict]:
        """Synthesize cross-signal insights across demand, pricing, ads, stock, and funnel."""
        insights = []

        def _add(title, severity, evidence, impact_estimate=0, category=None):
            insights.append({
                "title": title,
                "severity": severity,
                "evidence": evidence,
                "impact_estimate": round(impact_estimate, 2) if isinstance(impact_estimate, (int, float)) else None,
                "category": category,
                "sort_weight": abs(impact_estimate) if isinstance(impact_estimate, (int, float)) else 0,
            })

        yoy_pct = why.get("total_change_pct")
        revenue_down = yoy_pct is not None and yoy_pct < -5
        revenue_up = yoy_pct is not None and yoy_pct > 5
        cur_rev = cur_totals.get("revenue", 0)

        demand = diagnostics.get("demand") or {}
        if demand.get("clicks_yoy_pct") is not None:
            clicks_chg = demand["clicks_yoy_pct"]
            if revenue_down and clicks_chg <= -10:
                rev_per_click = cur_rev / max(demand.get("cur_clicks", 1), 1)
                impact = (demand.get("cur_clicks", 0) - demand.get("yoy_clicks", 0)) * rev_per_click
                _add(
                    "Search demand down",
                    "high",
                    f"Branded clicks down {abs(clicks_chg):.0f}% YoY.",
                    impact_estimate=impact,
                    category="demand",
                )
            elif revenue_down and clicks_chg >= 15:
                _add(
                    "Demand up but revenue down",
                    "critical",
                    f"Branded clicks up {clicks_chg:.0f}% YoY while revenue fell.",
                    impact_estimate=cur_rev * 0.2,
                    category="demand",
                )
            elif revenue_up and clicks_chg >= 15:
                _add(
                    "Demand growth tailwind",
                    "medium",
                    f"Branded clicks up {clicks_chg:.0f}% YoY supporting revenue growth.",
                    impact_estimate=cur_rev * 0.1,
                    category="demand",
                )

        stock = diagnostics.get("stock") or {}
        if self._stockout_root_cause:
            if stock.get("oos_rate", 0) >= 30 and stock.get("stockout_revenue_risk", 0) > 0:
                _add(
                    "Stockouts limiting sales",
                    "high" if revenue_down else "medium",
                    f"{stock.get('oos_count', 0)} of {stock.get('total_skus', 0)} SKUs out of stock "
                    f"({stock.get('oos_rate', 0):.0f}% OOS).",
                    impact_estimate=stock.get("stockout_revenue_risk", 0),
                    category="stock",
                )

        pricing = diagnostics.get("pricing") or {}
        price_idx = pricing.get("price_index")
        if price_idx is not None and price_idx > 1.1 and revenue_down:
            _add(
                "Above-competitor pricing",
                "high",
                f"Price index {price_idx:.2f} vs lowest competitor; likely suppressing conversion.",
                impact_estimate=cur_rev * 0.15,
                category="pricing",
            )
        if pricing.get("losing_money", 0) > 0:
            _add(
                "Selling below cost",
                "critical",
                f"{pricing.get('losing_money', 0)} SKUs priced below cost.",
                impact_estimate=cur_rev * 0.1,
                category="pricing",
            )

        ads_yoy = diagnostics.get("ads_yoy") or {}
        spend_chg = ads_yoy.get("spend_change_pct")
        if spend_chg is not None and revenue_down and spend_chg <= -20:
            _add(
                "Ad spend down",
                "high",
                f"Brand ad spend down {abs(spend_chg):.0f}% YoY.",
                impact_estimate=cur_rev * 0.1,
                category="ads",
            )
        ads = diagnostics.get("ads") or {}
        if ads.get("campaign_spend", 0) > 0 and ads.get("campaign_roas", 0) >= 3 and ads.get("impression_share") is not None:
            imp_share = ads.get("impression_share")
            if imp_share < 70:
                _add(
                    "Ad auctions leaving revenue on table",
                    "medium",
                    f"ROAS {ads.get('campaign_roas', 0):.1f}x with only {imp_share:.0f}% impression share.",
                    impact_estimate=cur_rev * 0.08,
                    category="ads",
                )

        conversion = diagnostics.get("conversion") or {}
        total_views = conversion.get("total_views", 0)
        total_carts = conversion.get("total_add_to_cart", 0)
        if total_views > 500 and conversion.get("view_to_cart_pct", 0) < 5:
            _add(
                "Low view-to-cart rate",
                "medium",
                f"Only {conversion.get('view_to_cart_pct', 0):.1f}% of viewers add to cart.",
                impact_estimate=cur_rev * 0.1,
                category="funnel",
            )
        if total_carts > 200 and conversion.get("cart_to_purchase_pct", 0) < 3:
            _add(
                "Checkout friction",
                "high" if revenue_down else "medium",
                f"Only {conversion.get('cart_to_purchase_pct', 0):.1f}% of carts convert to purchase.",
                impact_estimate=cur_rev * 0.12,
                category="funnel",
            )

        insights.sort(key=lambda x: x["sort_weight"], reverse=True)
        return insights[:5]

    # ── Recommendations ──────────────────────────────────────────

    def _generate_recommendations(self, brand, why, cur_products, yoy_map,
                                  cur_totals, yoy_totals, diagnostics=None) -> List[Dict]:
        """Generate specific, data-backed recommendations from diagnostics + WHY analysis."""
        diagnostics = diagnostics or {}
        recs = []  # each: {priority, category, action, expected_impact, sort_weight}
        rev = cur_totals.get("revenue", 0)
        rev_prev = yoy_totals.get("revenue", 0)
        yoy_pct = why.get("total_change_pct")
        is_declining = yoy_pct is not None and yoy_pct < -10

        stock = diagnostics.get("stock")
        ads = diagnostics.get("ads")
        pricing = diagnostics.get("pricing")
        demand = diagnostics.get("demand")
        conversion = diagnostics.get("conversion")

        # ── 1. Cross-signal: demand up + revenue down = supply/conversion problem ──
        demand_growing = demand and demand.get("clicks_yoy_pct") is not None and demand["clicks_yoy_pct"] > 15
        high_oos = stock and stock.get("oos_rate", 0) > 50

        if self._stockout_root_cause and is_declining and demand_growing and high_oos:
            recs.append({
                "priority": "critical",
                "category": "root_cause",
                "action": (
                    f"{brand} search demand up {demand['clicks_yoy_pct']:.0f}% but revenue down "
                    f"{abs(yoy_pct):.0f}% — {stock['oos_count']}/{stock['total_skus']} SKUs "
                    f"out of stock. Restock to capture the growing demand."
                ),
                "expected_impact": f"${stock.get('stockout_revenue_risk', 0):,.0f} revenue at risk from stockouts",
                "sort_weight": stock.get("stockout_revenue_risk", 0),
            })
        elif self._stockout_root_cause and is_declining and high_oos:
            recs.append({
                "priority": "critical",
                "category": "root_cause",
                "action": (
                    f"{stock['oos_count']} of {stock['total_skus']} {brand} SKUs out of stock "
                    f"({stock['oos_rate']:.0f}% OOS rate). This is likely the primary cause "
                    f"of the {abs(yoy_pct):.0f}% revenue decline."
                ),
                "expected_impact": f"${stock.get('stockout_revenue_risk', 0):,.0f} revenue at risk from stockouts",
                "sort_weight": stock.get("stockout_revenue_risk", 0),
            })

        # ── 2. Cross-signal: demand up + revenue down + low conversion ──
        low_cart_to_purchase = conversion and conversion.get("cart_to_purchase_pct", 100) < 3
        if is_declining and demand_growing and low_cart_to_purchase and not high_oos:
            v2c = conversion.get("view_to_cart_pct", 0)
            c2p = conversion.get("cart_to_purchase_pct", 0)
            recs.append({
                "priority": "critical",
                "category": "root_cause",
                "action": (
                    f"{brand} search demand up {demand['clicks_yoy_pct']:.0f}% but only "
                    f"{c2p:.1f}% of add-to-carts convert to purchase "
                    f"(view-to-cart {v2c:.0f}% is healthy). "
                    f"Investigate checkout friction, pricing, or shipping costs."
                ),
                "expected_impact": (
                    f"Improving cart-to-purchase from {c2p:.1f}% to {c2p * 1.5:.1f}% "
                    f"could add ~${rev * 0.5 * (c2p * 0.5 / max(c2p, 0.1)):,.0f} in revenue"
                ),
                "sort_weight": rev * 0.3,
            })

        # ── 4. Ads: scaling opportunity ──
        if ads and ads.get("campaign_spend", 0) > 0:
            spend = ads["campaign_spend"]
            roas = ads.get("campaign_roas", 0)
            imp_share = ads.get("impression_share")
            budget_lost = ads.get("budget_lost_share", 0) or 0
            rank_lost = ads.get("rank_lost_share", 0) or 0

            if roas >= 3 and imp_share is not None and imp_share < 70:
                missed_pct = 100 - imp_share
                est_missed_rev = spend * roas * (missed_pct / max(imp_share, 1))
                reason_parts = []
                if budget_lost > 5:
                    reason_parts.append(f"{budget_lost:.0f}% lost to budget")
                if rank_lost > 10:
                    reason_parts.append(f"{rank_lost:.0f}% lost to rank")
                reason = " — " + ", ".join(reason_parts) if reason_parts else ""
                recs.append({
                    "priority": "high",
                    "category": "ads",
                    "action": (
                        f"Scale {brand} ads — {roas:.1f}x ROAS with only {imp_share:.0f}% "
                        f"impression share{reason}. Capturing the missing "
                        f"{missed_pct:.0f}% could drive ~${est_missed_rev:,.0f} in revenue."
                    ),
                    "expected_impact": f"${spend:,.0f} spend → ${spend * roas:,.0f} revenue at {roas:.1f}x ROAS",
                    "sort_weight": est_missed_rev,
                })
            elif roas < 2 and spend > 500:
                recs.append({
                    "priority": "high",
                    "category": "ads",
                    "action": (
                        f"{brand} ads underperforming — ${spend:,.0f} spend at only "
                        f"{roas:.1f}x ROAS. Review keyword targeting and negative keywords."
                    ),
                    "expected_impact": f"Improving ROAS from {roas:.1f}x to 3x could save ${spend * (1 - roas / 3):,.0f} in ad spend",
                    "sort_weight": spend,
                })
            # Wasted products
            wasted = ads.get("wasted_spend_products", [])
            if wasted:
                total_wasted = sum(w.get("spend", 0) for w in wasted)
                if total_wasted > 100:
                    names = ", ".join(w.get("title", "?")[:40] for w in wasted[:3])
                    recs.append({
                        "priority": "medium",
                        "category": "ads",
                        "action": (
                            f"${total_wasted:,.0f} ad spend on {len(wasted)} {brand} products "
                            f"with no attributed conversions — review targeting: {names}"
                        ),
                        "expected_impact": f"Reallocating spend from low-converting products could save up to ${total_wasted:,.0f}",
                        "sort_weight": total_wasted,
                    })

        # ── 5. Pricing issues ──
        if pricing:
            below_min = pricing.get("below_minimum", 0)
            total_skus = pricing.get("total_skus", 0)
            losing = pricing.get("losing_money", 0)
            price_idx = pricing.get("price_index")

            if losing > 0:
                recs.append({
                    "priority": "critical",
                    "category": "pricing",
                    "action": (
                        f"{losing} {brand} SKUs selling below cost (losing money on every sale). "
                        f"Raise prices or discontinue."
                    ),
                    "expected_impact": "Stop margin losses immediately",
                    "sort_weight": rev * 0.2,
                })
            if below_min > 0 and below_min > total_skus * 0.1:
                recs.append({
                    "priority": "high",
                    "category": "pricing",
                    "action": (
                        f"{below_min} of {total_skus} {brand} SKUs priced below minimum "
                        f"({below_min / max(total_skus, 1) * 100:.0f}% non-compliant). "
                        f"Review MAP pricing compliance."
                    ),
                    "expected_impact": "Protect brand relationship and margin",
                    "sort_weight": below_min * 10,
                })
            if price_idx is not None and price_idx > 1.15:
                recs.append({
                    "priority": "medium",
                    "category": "pricing",
                    "action": (
                        f"{brand} price index is {price_idx:.2f} — we're "
                        f"{(price_idx - 1) * 100:.0f}% above cheapest competitor on average. "
                        f"This may be driving customers elsewhere."
                    ),
                    "expected_impact": "Selective price matching on top sellers could win back volume",
                    "sort_weight": rev * 0.1,
                })

        # ── 6. Conversion funnel issues (standalone) ──
        if conversion and not low_cart_to_purchase:
            views = conversion.get("total_views", 0)
            v2c = conversion.get("view_to_cart_pct", 0)
            c2p = conversion.get("cart_to_purchase_pct", 0)
            if views > 500 and v2c < 8:
                recs.append({
                    "priority": "medium",
                    "category": "conversion",
                    "action": (
                        f"{brand} has {views:,} product views but only {v2c:.1f}% add to cart. "
                        f"Improve product pages — better images, descriptions, or reviews."
                    ),
                    "expected_impact": (
                        f"Doubling view-to-cart rate from {v2c:.1f}% to {v2c * 2:.1f}% "
                        f"could add ~${rev * v2c / 100:,.0f} in revenue"
                    ),
                    "sort_weight": rev * 0.15,
                })

        # ── 7. Lost products driving decline ──
        lost_driver = next((d for d in why.get("drivers", []) if d["driver"] == "lost_products"), None)
        if lost_driver and abs(lost_driver["impact_dollars"]) > 2000:
            lost_rev = abs(lost_driver["impact_dollars"])
            cur_pids = {p["product_id"] for p in cur_products}
            lost_ids = set(yoy_map.keys()) - cur_pids
            lost_count = len(lost_ids)
            lost_products = [p for pid, p in yoy_map.items() if pid in lost_ids]
            lost_products.sort(key=lambda p: p.get("revenue", 0), reverse=True)

            causes = self._diagnose_no_sale_product_issues(brand, lost_products)
            by_cause = causes.get("causes", {})
            pricing_c = by_cause.get("pricing_pressure", {})
            listing_c = by_cause.get("listing_not_live", {})
            intent_c = by_cause.get("intentional_or_supplier", {})
            unknown_c = by_cause.get("unknown_other", {})
            recoverable = causes.get("recoverable_estimate", lost_rev * 0.35)
            top_lost = ", ".join(
                f"{(p.get('sku') or 'N/A')} (${p.get('revenue', 0):,.0f})"
                for p in lost_products[:3]
            )
            recs.append({
                "priority": "high",
                "category": "range",
                "action": (
                    f"{brand} has ${lost_rev:,.0f} YoY gap from {lost_count} SKU"
                    f"{'s' if lost_count != 1 else ''} with no sales this period (not necessarily delisted). "
                    f"Top lost: {top_lost}. "
                    f"Likely drivers: pricing pressure {pricing_c.get('count', 0)} "
                    f"(${pricing_c.get('revenue', 0):,.0f}), listing not live {listing_c.get('count', 0)} "
                    f"(${listing_c.get('revenue', 0):,.0f}), intentional/supplier {intent_c.get('count', 0)} "
                    f"(${intent_c.get('revenue', 0):,.0f}), unknown/no recent sales {unknown_c.get('count', 0)} "
                    f"(${unknown_c.get('revenue', 0):,.0f})."
                ),
                "expected_impact": (
                    f"Estimated recoverable upside ~${recoverable:,.0f} after cause-weighting; "
                    f"validate intentional delists and listing status first"
                ),
                "sort_weight": recoverable,
            })

        # ── 8. Margin erosion ──
        cur_margin = cur_totals.get("gross_margin_pct", 0)
        prev_margin = yoy_totals.get("gross_margin_pct", 0)
        if cur_margin > 0 and prev_margin > 0 and (prev_margin - cur_margin) > 5:
            margin_drop = prev_margin - cur_margin
            recs.append({
                "priority": "high",
                "category": "margin",
                "action": (
                    f"{brand} margin dropped {margin_drop:.1f}pp "
                    f"(from {prev_margin:.1f}% to {cur_margin:.1f}%). "
                    f"Check if costs increased or if discounting drove volume."
                ),
                "expected_impact": f"Each 1pp margin recovery = ~${rev * 0.01:,.0f}",
                "sort_weight": rev * margin_drop / 100,
            })

        # ── 9. New product success (keep momentum) ──
        new_driver = next((d for d in why.get("drivers", []) if d["driver"] == "new_products"), None)
        if new_driver and new_driver["impact_dollars"] > 2000:
            new_rev = new_driver["impact_dollars"]
            new_count = len({p["product_id"] for p in cur_products} - set(yoy_map.keys()))
            recs.append({
                "priority": "low",
                "category": "range",
                "action": (
                    f"{new_count} new {brand} product{'s' if new_count != 1 else ''} "
                    f"generated ${new_rev:,.0f} revenue — range expansion is working."
                ),
                "expected_impact": "Continue onboarding new lines to maintain momentum",
                "sort_weight": new_rev,
            })

        # ── 10. Demand signals (standalone positive) ──
        if demand_growing and not is_declining:
            clicks_yoy = demand.get("clicks_yoy_pct", 0)
            recs.append({
                "priority": "low",
                "category": "demand",
                "action": (
                    f"{brand} branded search up {clicks_yoy:.0f}% YoY — "
                    f"brand awareness is growing. Ensure stock and ad coverage keep pace."
                ),
                "expected_impact": "Sustain growth trajectory",
                "sort_weight": rev * 0.05,
            })

        # Sort by priority weight then impact
        priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        recs.sort(key=lambda r: (priority_order.get(r["priority"], 9), -r.get("sort_weight", 0)))

        # ── Next Actions enrichment ──
        _metric_map = {
            "root_cause": "revenue", "range": "revenue",
            "demand": "revenue", "ads": "spend_efficiency",
            "pricing": "margin", "margin": "margin", "conversion": "conversion",
        }
        _dep_map = {
            "root_cause": 1, "pricing": 1,
            "ads": 3,
            "conversion": 4, "margin": 4, "range": 4, "demand": 4,
        }
        for r in recs:
            r["impacted_metric"] = _metric_map.get(r.get("category", ""), "revenue")
            r["expected_impact_dollars"] = round(r.get("sort_weight", 0), 2)
            r["dependency_order"] = _dep_map.get(r.get("category", ""), 4)
            r.pop("sort_weight", None)
        return recs[:8]


    # ── Feature 1: SKU Status Classification ──────────────────

    def _classify_sku_status(self, product_ids: List[int], brand: str) -> Dict[int, Dict]:
        """Batch-classify lost products by recoverability status."""
        if not product_ids:
            return {}
        try:
            now = datetime.utcnow()
            pid_list = list(product_ids)

            # 1. Product status lookup
            prod_rows = (
                self.db.query(ShopifyProduct.shopify_product_id, ShopifyProduct.status)
                .filter(ShopifyProduct.shopify_product_id.in_(pid_list))
                .all()
            )
            prod_status = {int(r.shopify_product_id): r.status for r in prod_rows}

            # 2. Last sale date per product
            sale_rows = (
                self.db.query(
                    ShopifyOrderItem.shopify_product_id,
                    func.max(ShopifyOrderItem.order_date).label("last_sale"),
                )
                .filter(ShopifyOrderItem.shopify_product_id.in_(pid_list))
                .group_by(ShopifyOrderItem.shopify_product_id)
                .all()
            )
            last_sale = {int(r.shopify_product_id): r.last_sale for r in sale_rows}

            # 3. Competitive pricing presence (last 90 days)
            cutoff_90 = (now - timedelta(days=90)).date()
            cp_rows = (
                self.db.query(func.distinct(CompetitivePricing.variant_sku))
                .filter(
                    CompetitivePricing.vendor == brand,
                    CompetitivePricing.pricing_date >= cutoff_90,
                )
                .all()
            )
            cp_skus = {r[0] for r in cp_rows}

            # 4. SKU → product mapping for competitive pricing check
            sku_rows = (
                self.db.query(ShopifyOrderItem.shopify_product_id, ShopifyOrderItem.sku)
                .filter(ShopifyOrderItem.shopify_product_id.in_(pid_list), ShopifyOrderItem.sku.isnot(None))
                .distinct().all()
            )
            pid_skus = defaultdict(set)
            for r in sku_rows:
                pid_skus[int(r.shopify_product_id)].add(r.sku)

            # 5. ProductCost presence
            all_skus = set()
            for s in pid_skus.values():
                all_skus.update(s)
            cost_rows = (
                self.db.query(ProductCost.vendor_sku)
                .filter(ProductCost.vendor_sku.in_(list(all_skus)))
                .all()
            ) if all_skus else []
            cost_skus = {r.vendor_sku for r in cost_rows}

            # 6. Inventory check
            inv_rows = (
                self.db.query(ShopifyInventory.shopify_product_id, ShopifyInventory.inventory_quantity)
                .filter(ShopifyInventory.shopify_product_id.in_(pid_list))
                .all()
            )
            inv_qty = {}
            for r in inv_rows:
                pid = int(r.shopify_product_id) if r.shopify_product_id else None
                if pid:
                    inv_qty[pid] = max(inv_qty.get(pid, 0), r.inventory_quantity or 0)

            result = {}
            for pid in pid_list:
                status = prod_status.get(pid)
                ls = last_sale.get(pid)
                has_cp = bool(pid_skus.get(pid, set()) & cp_skus)
                has_cost = bool(pid_skus.get(pid, set()) & cost_skus)
                inv = inv_qty.get(pid)
                days_since_sale = (now - ls).days if ls else 999

                # Note: OOS is not treated as a constraint (we sell when out of stock)
                if status == "archived" and days_since_sale > 180 and not has_cp:
                    s, reason, rec = "permanent_delist", "Archived, no sales 180d+, no competitive pricing", False
                elif status == "archived" and days_since_sale < 365 and not has_cost:
                    s, reason, rec = "supplier_discontinued", "Archived, had recent sales but no cost record", False
                elif status == "archived" and has_cp and days_since_sale < 365:
                    s, reason, rec = "relaunch_candidate", "Archived but still in competitive pricing data", True
                elif status == "active":
                    s, reason, rec = "active", "Active product", True
                else:
                    s, reason, rec = "unknown", "Insufficient data to classify", True

                result[pid] = {"status": s, "reason": reason, "recoverable": rec}
            return result
        except Exception as e:
            log.debug(f"SKU status classification failed for {brand}: {e}")
            return {}

    # ── Feature 3: Demand Quality ────────────────────────────

    def _get_demand_quality(self, brand, cur_start, cur_end, yoy_start, yoy_end):
        """Split search demand into branded vs non-branded using Google Ads search terms."""
        try:
            include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
            brand_norm = (brand or "").strip().lower()
            if not brand_norm:
                return None

            # Get matched campaign names
            ads_campaign_rows = self._get_ads_campaign_rows(cur_start, cur_end)
            metrics = self._get_ads_campaign_metrics(brand, ads_campaign_rows)
            campaign_names = metrics.get("matched_campaigns", set())
            if not campaign_names:
                return None

            # Resolve campaign IDs
            cid_rows = (
                self.db.query(func.distinct(GoogleAdsCampaign.campaign_id))
                .filter(GoogleAdsCampaign.campaign_name.in_(list(campaign_names)))
                .all()
            )
            cids = [r[0] for r in cid_rows]
            if not cids:
                return None

            brand_pattern = _re.compile(r"\b" + _re.escape(brand_norm) + r"\b", _re.IGNORECASE)
            include_patterns = [
                _re.compile(r"\b" + _re.escape(t) + r"\b", _re.IGNORECASE)
                for t in include_terms if t
            ] if allowlist_used else []

            def _classify(start, end):
                start_d = start.date() if hasattr(start, "date") else start
                end_d = end.date() if hasattr(end, "date") else end
                rows = (
                    self.db.query(
                        GoogleAdsSearchTerm.search_term,
                        func.sum(GoogleAdsSearchTerm.clicks).label("clicks"),
                        func.sum(GoogleAdsSearchTerm.impressions).label("impr"),
                    )
                    .filter(
                        GoogleAdsSearchTerm.campaign_id.in_(cids),
                        GoogleAdsSearchTerm.date >= start_d,
                        GoogleAdsSearchTerm.date < end_d,
                    )
                    .group_by(GoogleAdsSearchTerm.search_term)
                    .all()
                )
                b_clicks, b_impr, nb_clicks, nb_impr = 0, 0, 0, 0
                nb_terms = []
                for r in rows:
                    term = r.search_term or ""
                    clicks = int(r.clicks or 0)
                    impr = int(r.impr or 0)
                    is_branded = bool(brand_pattern.search(term)) or any(p.search(term) for p in include_patterns)
                    if is_branded:
                        b_clicks += clicks
                        b_impr += impr
                    else:
                        nb_clicks += clicks
                        nb_impr += impr
                        if clicks > 0:
                            nb_terms.append({"query": r.search_term, "clicks": clicks, "impressions": impr})
                nb_terms.sort(key=lambda x: x["clicks"], reverse=True)
                return {"branded": {"clicks": b_clicks, "impressions": b_impr},
                        "non_branded": {"clicks": nb_clicks, "impressions": nb_impr},
                        "top_non_branded": nb_terms[:5]}

            cur = _classify(cur_start, cur_end)
            prev = _classify(yoy_start, yoy_end)

            total = cur["branded"]["clicks"] + cur["non_branded"]["clicks"]
            if total == 0:
                return None  # no search term data available
            branded_pct = round(cur["branded"]["clicks"] / total * 100, 1)
            nb_yoy = _pct_change(cur["non_branded"]["clicks"], prev["non_branded"]["clicks"])
            b_yoy = _pct_change(cur["branded"]["clicks"], prev["branded"]["clicks"])
            acq = "growing" if nb_yoy and nb_yoy > 10 else ("declining" if nb_yoy and nb_yoy < -10 else "flat")

            return {
                "branded": {**cur["branded"], "clicks_yoy_pct": b_yoy},
                "non_branded": {**cur["non_branded"], "clicks_yoy_pct": nb_yoy},
                "branded_pct": branded_pct,
                "acquisition_signal": acq,
                "top_non_branded": cur["top_non_branded"],
            }
        except Exception as e:
            log.debug(f"Demand quality skipped for {brand}: {e}")
            return None

    # ── Feature 4: Operational Feasibility ────────────────────

    def _get_operational_feasibility(self, brand: str, product_ids: List[int]) -> Dict[int, Dict]:
        """Get stock level and ML reorder data for a set of product IDs."""
        if not product_ids:
            return {}
        try:
            pid_list = list(product_ids)

            # Inventory quantities
            inv_rows = (
                self.db.query(ShopifyInventory.shopify_product_id, ShopifyInventory.inventory_quantity)
                .filter(ShopifyInventory.shopify_product_id.in_(pid_list))
                .all()
            )
            inv_by_pid = {}
            for r in inv_rows:
                pid = int(r.shopify_product_id) if r.shopify_product_id else None
                if pid:
                    inv_by_pid[pid] = max(inv_by_pid.get(pid, 0), r.inventory_quantity or 0)

            # SKU → product mapping
            sku_rows = (
                self.db.query(ShopifyOrderItem.shopify_product_id, ShopifyOrderItem.sku)
                .filter(ShopifyOrderItem.shopify_product_id.in_(pid_list), ShopifyOrderItem.sku.isnot(None))
                .distinct().all()
            )
            sku_to_pid = {}
            for r in sku_rows:
                sku_to_pid[r.sku] = int(r.shopify_product_id)

            # ML suggestions (latest generation)
            ml_by_pid = {}
            if sku_to_pid:
                latest_gen = self.db.query(func.max(MLInventorySuggestion.generated_at)).scalar()
                if latest_gen:
                    ml_rows = (
                        self.db.query(MLInventorySuggestion)
                        .filter(
                            MLInventorySuggestion.sku.in_(list(sku_to_pid.keys())),
                            MLInventorySuggestion.generated_at == latest_gen,
                        )
                        .all()
                    )
                    for r in ml_rows:
                        pid = sku_to_pid.get(r.sku)
                        if pid:
                            ml_by_pid[pid] = r

            result = {}
            for pid in pid_list:
                qty = inv_by_pid.get(pid)
                ml = ml_by_pid.get(pid)
                if qty is None:
                    feas = "unknown"
                elif qty == 0:
                    feas = "oos"
                elif qty <= 3:
                    feas = "low_stock"
                else:
                    feas = "stock_ok"
                result[pid] = {
                    "feasibility": feas,
                    "inventory_quantity": qty,
                    "days_of_cover": round(ml.days_of_cover, 1) if ml and ml.days_of_cover else None,
                    "daily_velocity": round(ml.daily_sales_velocity, 2) if ml and ml.daily_sales_velocity else None,
                }
            return result
        except Exception as e:
            log.debug(f"Operational feasibility failed for {brand}: {e}")
            return {}

    # ── Feature 5: Cannibalization Detection ──────────────────

    def _detect_cannibalization(self, brand, new_product_ids, cur_map, yoy_map):
        """Check if new products cannibalized existing products of the same product_type."""
        if not new_product_ids:
            return None
        try:
            all_pids = list(set(new_product_ids) | set(cur_map.keys()) | set(yoy_map.keys()))
            type_rows = (
                self.db.query(ShopifyProduct.shopify_product_id, ShopifyProduct.product_type)
                .filter(ShopifyProduct.shopify_product_id.in_(all_pids), ShopifyProduct.product_type.isnot(None))
                .all()
            )
            pid_type = {int(r.shopify_product_id): r.product_type for r in type_rows if r.product_type}

            # Group new product revenue by product_type
            type_new_rev = defaultdict(float)
            for pid in new_product_ids:
                pt = pid_type.get(pid)
                if pt and pid in cur_map:
                    type_new_rev[pt] += cur_map[pid]["revenue"]

            if not type_new_rev:
                return None

            shared_ids = set(cur_map.keys()) & set(yoy_map.keys())
            categories = []
            total_cannibalized = 0
            total_new_rev = 0

            for pt, new_rev in type_new_rev.items():
                same_type_shared = [pid for pid in shared_ids if pid_type.get(pid) == pt]
                if not same_type_shared:
                    categories.append({"product_type": pt, "new_product_revenue": round(new_rev, 2),
                                       "existing_decline": 0, "net_category_growth": round(new_rev, 2),
                                       "cannibalization_pct": 0})
                    total_new_rev += new_rev
                    continue

                cur_existing = sum(cur_map[p]["revenue"] for p in same_type_shared)
                yoy_existing = sum(yoy_map[p]["revenue"] for p in same_type_shared)
                existing_decline = yoy_existing - cur_existing  # positive = decline
                cannibalized = max(existing_decline, 0)
                cannibal_pct = round(min(cannibalized / new_rev * 100, 100), 1) if new_rev > 0 else 0

                categories.append({
                    "product_type": pt, "new_product_revenue": round(new_rev, 2),
                    "existing_decline": round(existing_decline, 2),
                    "net_category_growth": round(new_rev - cannibalized, 2),
                    "cannibalization_pct": cannibal_pct,
                })
                total_cannibalized += cannibalized
                total_new_rev += new_rev

            if not categories:
                return None

            return {
                "cannibalization_detected": total_cannibalized > total_new_rev * 0.2,
                "estimated_cannibalized_pct": round(total_cannibalized / total_new_rev * 100, 1) if total_new_rev > 0 else 0,
                "estimated_cannibalized_dollars": round(total_cannibalized, 2),
                "category_analysis": sorted(categories, key=lambda c: c["new_product_revenue"], reverse=True),
            }
        except Exception as e:
            log.debug(f"Cannibalization analysis failed for {brand}: {e}")
            return None

    # ── Feature 6: Price Elasticity ───────────────────────────

    def _get_price_change_events(self, brand: str, lookback_days: int = 180):
        """Detect price changes and measure sales impact (pre/post 14-day windows)."""
        try:
            since = (datetime.utcnow() - timedelta(days=lookback_days)).date()

            rows = (
                self.db.query(
                    CompetitivePricing.variant_sku, CompetitivePricing.title,
                    CompetitivePricing.pricing_date, CompetitivePricing.current_price,
                )
                .filter(
                    CompetitivePricing.vendor == brand,
                    CompetitivePricing.pricing_date >= since,
                    CompetitivePricing.current_price.isnot(None),
                    CompetitivePricing.current_price > 0,
                )
                .order_by(CompetitivePricing.variant_sku, CompetitivePricing.pricing_date)
                .all()
            )
            if not rows:
                return None

            # Detect changes > 5%
            events = []
            by_sku = defaultdict(list)
            for r in rows:
                by_sku[r.variant_sku].append(r)

            for sku, sku_rows in by_sku.items():
                for i in range(1, len(sku_rows)):
                    prev_p = float(sku_rows[i - 1].current_price)
                    curr_p = float(sku_rows[i].current_price)
                    if prev_p <= 0:
                        continue
                    chg_pct = (curr_p - prev_p) / prev_p * 100
                    if abs(chg_pct) > 5:
                        events.append({
                            "sku": sku, "title": sku_rows[i].title,
                            "change_date": sku_rows[i].pricing_date,
                            "price_before": prev_p, "price_after": curr_p,
                            "change_pct": round(chg_pct, 1),
                            "direction": "increase" if chg_pct > 0 else "decrease",
                        })

            if not events:
                return None

            # Batch daily sales for affected SKUs
            all_skus = list({e["sku"] for e in events})
            earliest = min(e["change_date"] for e in events) - timedelta(days=14)
            latest = max(e["change_date"] for e in events) + timedelta(days=14)

            sales_rows = (
                self.db.query(
                    ShopifyOrderItem.sku,
                    func.date(ShopifyOrderItem.order_date).label("day"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                )
                .filter(
                    ShopifyOrderItem.sku.in_(all_skus),
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= earliest,
                    ShopifyOrderItem.order_date <= latest,
                    ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded"]),
                )
                .group_by(ShopifyOrderItem.sku, func.date(ShopifyOrderItem.order_date))
                .all()
            )

            daily_sales = defaultdict(lambda: defaultdict(int))
            for sr in sales_rows:
                daily_sales[sr.sku][sr.day] = int(sr.units or 0)

            enriched = []
            for e in events:
                cd = e["change_date"]
                sku = e["sku"]
                before = [daily_sales[sku].get(cd - timedelta(days=d), 0) for d in range(1, 15)]
                after = [daily_sales[sku].get(cd + timedelta(days=d), 0) for d in range(0, 14)]
                avg_b = sum(before) / 14
                avg_a = sum(after) / 14
                if avg_b < 0.1 and avg_a < 0.1:
                    continue
                enriched.append({
                    **e, "change_date": str(e["change_date"]),
                    "sales_before_avg": round(avg_b, 2), "sales_after_avg": round(avg_a, 2),
                    "lift_pct": _pct_change(avg_a, avg_b),
                })

            enriched.sort(key=lambda x: x["change_date"], reverse=True)

            sensitivities = []
            for e in enriched:
                if e["lift_pct"] is not None and abs(e["change_pct"]) > 0:
                    sensitivities.append(abs(e["lift_pct"]) / abs(e["change_pct"]))

            return {
                "events": enriched[:10], "event_count": len(enriched),
                "avg_price_sensitivity": round(sum(sensitivities) / len(sensitivities), 2) if sensitivities else None,
            }
        except Exception as e:
            log.debug(f"Price elasticity skipped for {brand}: {e}")
            return None

    # ── Feature 7: Per-Recommendation Confidence ──────────────

    def _get_data_freshness(self) -> Dict[str, str]:
        """Query max dates for each data source (cached on instance)."""
        if hasattr(self, "_freshness_cache"):
            return self._freshness_cache
        f = {}
        try:
            v = self.db.query(func.max(GoogleAdsCampaign.date)).scalar()
            if v:
                f["google_ads"] = str(v)
        except Exception:
            pass
        try:
            v = self.db.query(func.max(SearchConsoleQuery.date)).scalar()
            if v:
                f["search_console"] = str(v)
        except Exception:
            pass
        try:
            v = self.db.query(func.max(GA4ProductPerformance.date)).scalar()
            if v:
                f["ga4"] = str(v)
        except Exception:
            pass
        try:
            v = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if v:
                f["competitive_pricing"] = str(v)
        except Exception:
            pass
        self._freshness_cache = f
        return f

    def _enrich_rec_confidence(self, recs, data_coverage, diagnostics):
        """Add confidence, data_freshness, and relies_on to each recommendation."""
        freshness = self._get_data_freshness()
        # Add shopify/product_costs from data_coverage
        shopify_latest = data_coverage.get("shopify_latest", "")
        if shopify_latest:
            freshness["shopify_orders"] = shopify_latest[:10]
            freshness["shopify_inventory"] = shopify_latest[:10]
            freshness["product_costs"] = shopify_latest[:10]

        now = datetime.utcnow().date()
        stale_days = {
            "shopify_orders": 1, "google_ads": 2, "search_console": 4,
            "ga4": 3, "competitive_pricing": 7, "product_costs": 30,
            "shopify_inventory": 1,
        }

        for rec in recs:
            cat = rec.get("category", "")
            deps = _REC_DATA_DEPS.get(cat, ["shopify_orders"])
            rec["relies_on"] = deps

            rec_fresh = {}
            stale = 0
            missing = 0
            for src in deps:
                latest = freshness.get(src)
                if latest:
                    rec_fresh[src] = latest
                    try:
                        latest_d = datetime.strptime(latest[:10], "%Y-%m-%d").date()
                        if (now - latest_d).days > stale_days.get(src, 7):
                            stale += 1
                    except Exception:
                        pass
                else:
                    missing += 1
                    rec_fresh[src] = None

            rec["data_freshness"] = rec_fresh
            if missing > 0:
                rec["confidence"] = "low"
            elif stale > 0:
                rec["confidence"] = "medium"
            else:
                rec["confidence"] = "high"

        return recs

    # ── Diagnostic methods ─────────────────────────────────────

    def _compute_brand_health(self, brand_data, why_analysis=None) -> Dict:
        """Deterministic 0-100 health score from weighted signals."""
        score = 50  # baseline

        # Revenue YoY (30 weight)
        yoy = brand_data.get("revenue_yoy_pct")
        if yoy is not None:
            if yoy > 20:
                score += 30
            elif yoy > 10:
                score += 20
            elif yoy > 0:
                score += 10
            elif yoy > -10:
                score += 0
            elif yoy > -20:
                score -= 15
            else:
                score -= 30

        # Margin (20 weight) — use best available margin signal
        coverage = brand_data.get("cost_coverage_pct", 0)
        margin = brand_data.get("gross_margin_pct", 0)
        est_margin = brand_data.get("estimated_margin_pct")
        # Pick effective margin: full margin at >=50% coverage, estimated if partial, skip if none
        if coverage >= 50:
            eff_margin = margin
        elif est_margin is not None:
            eff_margin = est_margin
        else:
            eff_margin = None

        if eff_margin is not None:
            if eff_margin > 30:
                score += 15
            elif eff_margin > 15:
                score += 10
            elif eff_margin > 0:
                score += 5
            else:
                score -= 15

        # WHY driver negativity (20 weight)
        if why_analysis:
            drivers = why_analysis.get("drivers", [])
            neg = sum(1 for d in drivers if d.get("direction") == "negative")
            if neg == 0:
                score += 10
            elif neg <= 2:
                score += 0
            else:
                score -= 10

        # Product diversity (15 weight) — approx from units YoY
        units_yoy = brand_data.get("units_yoy_pct")
        if units_yoy is not None:
            if units_yoy > 10:
                score += 10
            elif units_yoy > -10:
                score += 5
            else:
                score -= 10

        score = max(0, min(100, score))

        # Grade
        if score >= 80:
            grade = "A"
        elif score >= 65:
            grade = "B"
        elif score >= 50:
            grade = "C"
        elif score >= 35:
            grade = "D"
        else:
            grade = "F"

        # Severity
        if score >= 65:
            severity = "healthy"
        elif score >= 40:
            severity = "warning"
        else:
            severity = "critical"

        # Flags
        flags = []
        if yoy is not None and yoy < -10:
            flags.append("revenue_declining")
        if units_yoy is not None and units_yoy < -15:
            flags.append("volume_decline")
        if eff_margin is not None and eff_margin < 5:
            flags.append("margin_erosion")

        # Revenue at risk (revenue * severity factor)
        rev = brand_data.get("revenue", 0)
        if severity == "critical":
            revenue_at_risk = round(rev * 0.5, 2)
        elif severity == "warning":
            revenue_at_risk = round(rev * 0.2, 2)
        else:
            revenue_at_risk = 0

        return {
            "score": score,
            "grade": grade,
            "severity": severity,
            "flags": flags,
            "revenue_at_risk": revenue_at_risk,
        }

    def _get_pricing_diagnostic(self, brand) -> Optional[Dict]:
        """Competitive pricing analysis for a brand."""
        try:
            latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if not latest_date:
                return None

            rows = (
                self.db.query(CompetitivePricing, ProductCost)
                .outerjoin(
                    ProductCost,
                    func.upper(CompetitivePricing.variant_sku) == func.upper(ProductCost.vendor_sku),
                )
                .filter(
                    CompetitivePricing.vendor == brand,
                    CompetitivePricing.pricing_date == latest_date,
                )
                .all()
            )
            if not rows:
                return None

            total = len(rows)
            below_min = 0
            losing = 0
            above_rrp = 0
            no_cost = 0

            # Price index
            price_ratios = []
            for cp, pc in rows:
                if cp.is_losing_money:
                    losing += 1
                if cp.is_above_rrp:
                    above_rrp += 1
                if cp.has_no_cost:
                    no_cost += 1

                floor = None
                if pc and pc.minimum_price:
                    floor = float(pc.minimum_price)
                elif cp.minimum_price:
                    floor = float(cp.minimum_price)

                if (
                    floor is not None
                    and cp.lowest_competitor_price
                    and float(cp.lowest_competitor_price) > 0
                    and float(cp.lowest_competitor_price) < floor
                ):
                    below_min += 1

                if cp.current_price and cp.lowest_competitor_price and float(cp.lowest_competitor_price) > 0:
                    price_ratios.append(float(cp.current_price) / float(cp.lowest_competitor_price))
            price_index = round(sum(price_ratios) / len(price_ratios), 2) if price_ratios else None

            # Avg margin
            margins = [float(cp.profit_margin_pct) for cp, _pc in rows if cp.profit_margin_pct is not None]
            avg_margin = round(sum(margins) / len(margins), 1) if margins else None

            # Margin distribution
            buckets = {"negative": 0, "0_10": 0, "10_20": 0, "20_30": 0, "30_plus": 0}
            for m in margins:
                if m < 0:
                    buckets["negative"] += 1
                elif m < 10:
                    buckets["0_10"] += 1
                elif m < 20:
                    buckets["10_20"] += 1
                elif m < 30:
                    buckets["20_30"] += 1
                else:
                    buckets["30_plus"] += 1

            # Worst margin SKUs
            with_margin = [(cp, float(cp.profit_margin_pct)) for cp, _pc in rows if cp.profit_margin_pct is not None]
            with_margin.sort(key=lambda x: x[1])
            worst = []
            for cp, m in with_margin[:5]:
                worst.append({
                    "title": cp.title or "Unknown",
                    "sku": cp.variant_sku or "",
                    "margin_pct": m,
                    "current_price": float(cp.current_price) if cp.current_price else None,
                    "lowest_competitor": float(cp.lowest_competitor_price) if cp.lowest_competitor_price else None,
                    "nett_cost": float(cp.nett_cost) if cp.nett_cost else None,
                })

            return {
                "total_skus": total,
                "below_minimum": below_min,
                "below_minimum_pct": round(below_min / total * 100, 1) if total else 0,
                "losing_money": losing,
                "losing_money_pct": round(losing / total * 100, 1) if total else 0,
                "above_rrp": above_rrp,
                "no_cost": no_cost,
                "price_index": price_index,
                "avg_margin": avg_margin,
                "margin_distribution": buckets,
                "worst_skus": worst,
            }
        except Exception as e:
            log.debug(f"Pricing diagnostic skipped for {brand}: {e}")
            return None

    def _get_stock_health(self, brand, period_days: int = 90) -> Optional[Dict]:
        """Inventory health for a brand."""
        try:
            inv_rows = (
                self.db.query(ShopifyInventory)
                .filter(ShopifyInventory.vendor == brand)
                .all()
            )
            if not inv_rows:
                return None

            # Separate rows with known inventory from NULL (unknown)
            known = [r for r in inv_rows if r.inventory_quantity is not None]
            unknown_count = len(inv_rows) - len(known)
            total = len(known)
            if total == 0:
                return None

            oos = [r for r in known if r.inventory_quantity == 0]
            oos_count = len(oos)
            low_stock = sum(1 for r in known if 0 < r.inventory_quantity <= 3)
            total_units = sum(r.inventory_quantity for r in known)
            oos_rate = round(oos_count / total * 100, 1) if total else 0

            # Stockout revenue risk — check revenue in the selected period for OOS products
            stockout_revenue_risk = 0
            oos_products = []
            if oos:
                oos_pids = [r.shopify_product_id for r in oos if r.shopify_product_id]
                if oos_pids:
                    since = datetime.utcnow() - timedelta(days=period_days)
                    rev_rows = (
                        self.db.query(
                            ShopifyOrderItem.shopify_product_id,
                            ShopifyOrderItem.title,
                            func.sum(ShopifyOrderItem.total_price).label("rev"),
                            func.sum(ShopifyOrderItem.quantity).label("units"),
                        )
                        .filter(
                            ShopifyOrderItem.shopify_product_id.in_(oos_pids),
                            ShopifyOrderItem.order_date >= since,
                            ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded"]),
                        )
                        .group_by(ShopifyOrderItem.shopify_product_id, ShopifyOrderItem.title)
                        .all()
                    )
                    for rr in rev_rows:
                        rev = _dec(rr.rev)
                        stockout_revenue_risk += rev
                        oos_products.append({
                            "product_id": rr.shopify_product_id,
                            "title": rr.title or "Unknown",
                            "recent_revenue": round(rev, 2),
                            "recent_units": rr.units or 0,
                        })
                    oos_products.sort(key=lambda x: x["recent_revenue"], reverse=True)

            return {
                "scope": f"{brand} inventory",
                "total_skus": total,
                "unknown_inventory": unknown_count,
                "oos_count": oos_count,
                "oos_rate": oos_rate,
                "low_stock_count": low_stock,
                "total_units": total_units,
                "stockout_revenue_risk": round(stockout_revenue_risk, 2),
                "oos_products": oos_products[:10],
            }
        except Exception as e:
            log.debug(f"Stock health skipped for {brand}: {e}")
            return None

    def _get_stocking_priorities(self, brand: str, period_days: int = 90) -> Optional[Dict]:
        """Rank OOS/low-stock products by revenue recovery potential.

        Scoring model:
          0.40 * normalized_recent_revenue
        + 0.25 * normalized_view_to_cart_intent
        + 0.20 * normalized_margin
        + 0.15 * normalized_daily_velocity
        """
        try:
            # 1. Find candidates: OOS or low stock (<=3 units)
            inv_rows = (
                self.db.query(ShopifyInventory)
                .filter(ShopifyInventory.vendor == brand)
                .all()
            )
            if not inv_rows:
                return None
            known = [r for r in inv_rows if r.inventory_quantity is not None]
            candidates = [r for r in known if r.inventory_quantity <= 3]
            if not candidates:
                return None

            candidate_pids = list({r.shopify_product_id for r in candidates if r.shopify_product_id})
            if not candidate_pids:
                return None
            inv_by_pid = {}
            for r in candidates:
                if r.shopify_product_id:
                    inv_by_pid.setdefault(r.shopify_product_id, r)

            # 2. Recent revenue + velocity + COGS per product (net of discounts/refunds)
            since = datetime.utcnow() - timedelta(days=period_days)
            rpi = self._refund_per_item_subquery()
            rev_rows = (
                self.db.query(
                    ShopifyOrderItem.shopify_product_id,
                    ShopifyOrderItem.title,
                    ShopifyOrderItem.sku,
                    func.sum(ShopifyOrderItem.total_price).label("rev"),
                    func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column("0"))).label("discounts"),
                    func.sum(func.coalesce(rpi.c.refund_amount, literal_column("0"))).label("refunds"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                    func.sum(func.coalesce(rpi.c.refund_qty, literal_column("0"))).label("refund_units"),
                    func.avg(ShopifyOrderItem.price).label("avg_price"),
                    func.sum(
                        case(
                            (ShopifyOrderItem.cost_per_item.isnot(None),
                             ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                            else_=literal_column("0"),
                        )
                    ).label("cogs"),
                    func.sum(
                        case(
                            (and_(ShopifyOrderItem.cost_per_item.isnot(None),
                                  rpi.c.refund_qty.isnot(None)),
                             ShopifyOrderItem.cost_per_item * rpi.c.refund_qty),
                            else_=literal_column("0"),
                        )
                    ).label("refund_cogs"),
                    func.sum(
                        case(
                            (ShopifyOrderItem.cost_per_item.isnot(None),
                             ShopifyOrderItem.quantity),
                            else_=literal_column("0"),
                        )
                    ).label("units_costed"),
                )
                .outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
                .filter(
                    ShopifyOrderItem.shopify_product_id.in_(candidate_pids),
                    ShopifyOrderItem.order_date >= since,
                    ShopifyOrderItem.financial_status.notin_(["voided"]),
                )
                .group_by(
                    ShopifyOrderItem.shopify_product_id,
                    ShopifyOrderItem.title,
                    ShopifyOrderItem.sku,
                )
                .all()
            )

            # Build product data — only products with revenue > 0
            products = []
            for rr in rev_rows:
                gross_rev = _dec(rr.rev)
                discounts = _dec(rr.discounts)
                refunds = _dec(rr.refunds)
                net_rev = gross_rev - discounts - refunds
                if net_rev <= 0:
                    continue
                units = int(rr.units or 0) - int(rr.refund_units or 0)
                if units <= 0:
                    continue
                avg_price = net_rev / units if units > 0 else 0
                cogs = _dec(rr.cogs)
                refund_cogs = _dec(rr.refund_cogs)
                net_cogs = cogs - refund_cogs
                units_costed = int(rr.units_costed or 0)

                # Margin from order-level COGS (extrapolate when partial coverage)
                margin_pct = 0.0
                if net_cogs > 0 and net_rev > 0:
                    if 0 < units_costed < units:
                        avg_cost = cogs / units_costed
                        est_cogs = avg_cost * units
                        margin_pct = round((net_rev - est_cogs) / net_rev * 100, 1)
                    else:
                        margin_pct = round((net_rev - net_cogs) / net_rev * 100, 1)
                elif rr.sku:
                    # Fallback: ProductCost lookup
                    cost_row = (
                        self.db.query(ProductCost)
                        .filter(ProductCost.vendor_sku == rr.sku)
                        .first()
                    )
                    if cost_row:
                        active_cost = cost_row.get_active_cost()
                        if active_cost and net_rev > 0:
                            est_cogs = float(active_cost) * units
                            margin_pct = round((net_rev - est_cogs) / net_rev * 100, 1)

                inv_item = inv_by_pid.get(rr.shopify_product_id)
                products.append({
                    "product_id": rr.shopify_product_id,
                    "title": rr.title or "Unknown",
                    "sku": rr.sku or "",
                    "recent_revenue": round(net_rev, 2),
                    "recent_units": units,
                    "daily_velocity": round(units / max(period_days, 1), 2),
                    "avg_price": round(avg_price, 2),
                    "margin_pct": max(margin_pct, 0),
                    "view_to_cart_pct": 0.0,  # filled in step 3
                    "inventory_quantity": inv_item.inventory_quantity if inv_item else 0,
                })

            if not products:
                return None

            # 3. GA4 intent signals — batch query
            try:
                active_pids = [p["product_id"] for p in products if p["product_id"]]
                # Build GA4 item ID mapping per product
                ga4_map = {}  # shopify_product_id -> set of ga4 item_ids
                product_rows = (
                    self.db.query(
                        ShopifyProduct.shopify_product_id,
                        ShopifyProduct.handle,
                        ShopifyProduct.variants,
                    )
                    .filter(ShopifyProduct.shopify_product_id.in_(active_pids))
                    .all()
                )
                country_prefixes = ("shopify_AU", "shopify_US", "shopify_CA", "shopify_NZ", "shopify_GB")
                for pr in product_rows:
                    pid = str(pr.shopify_product_id)
                    ids = {pid}
                    if pr.handle:
                        ids.add(pr.handle)
                    variants = pr.variants or []
                    if isinstance(variants, list):
                        for v in variants:
                            vid = v.get("id") if isinstance(v, dict) else None
                            if vid:
                                vid = str(vid)
                                ids.add(vid)
                                for pfx in country_prefixes:
                                    ids.add(f"{pfx}_{pid}_{vid}")
                    ga4_map[pr.shopify_product_id] = ids

                # Add SKUs from order items
                sku_rows = (
                    self.db.query(
                        ShopifyOrderItem.shopify_product_id,
                        func.distinct(ShopifyOrderItem.sku),
                    )
                    .filter(
                        ShopifyOrderItem.shopify_product_id.in_(active_pids),
                        ShopifyOrderItem.sku.isnot(None),
                        ShopifyOrderItem.sku != "",
                    )
                    .all()
                )
                for row in sku_rows:
                    if row[0] in ga4_map and row[1]:
                        ga4_map[row[0]].add(row[1])

                # Batch query GA4 data
                all_ga4_ids = set()
                for id_set in ga4_map.values():
                    all_ga4_ids.update(id_set)

                if all_ga4_ids:
                    ga4_since = (datetime.utcnow() - timedelta(days=90)).date()
                    ga4_rows = (
                        self.db.query(
                            GA4ProductPerformance.item_id,
                            func.sum(GA4ProductPerformance.items_viewed).label("views"),
                            func.sum(GA4ProductPerformance.items_added_to_cart).label("carts"),
                        )
                        .filter(
                            GA4ProductPerformance.item_id.in_(list(all_ga4_ids)),
                            GA4ProductPerformance.date >= ga4_since,
                        )
                        .group_by(GA4ProductPerformance.item_id)
                        .all()
                    )
                    ga4_by_id = {r.item_id: r for r in ga4_rows}

                    # Aggregate per product
                    for p in products:
                        pid_ids = ga4_map.get(p["product_id"], set())
                        total_views = 0
                        total_carts = 0
                        for gid in pid_ids:
                            row = ga4_by_id.get(gid)
                            if row:
                                total_views += int(row.views or 0)
                                total_carts += int(row.carts or 0)
                        if total_views > 0:
                            p["view_to_cart_pct"] = round(total_carts / total_views * 100, 1)
            except Exception as ga4_err:
                log.debug(f"GA4 intent lookup failed for {brand}: {ga4_err}")

            # 4. Scoring — normalize and weight
            max_rev = max((p["recent_revenue"] for p in products), default=0.01) or 0.01
            max_intent = max((p["view_to_cart_pct"] for p in products), default=0.01) or 0.01
            max_margin = max((p["margin_pct"] for p in products), default=0.01) or 0.01
            max_velocity = max((p["daily_velocity"] for p in products), default=0.01) or 0.01

            for p in products:
                p["priority_score"] = round(
                    (0.40 * (p["recent_revenue"] / max_rev)
                     + 0.25 * (p["view_to_cart_pct"] / max_intent)
                     + 0.20 * (p["margin_pct"] / max_margin)
                     + 0.15 * (p["daily_velocity"] / max_velocity)
                     ) * 100,
                    1,
                )
                # Revenue protected estimate: 30 days of velocity at avg price
                p["revenue_protected"] = round(p["daily_velocity"] * 30 * p["avg_price"], 2)

            products.sort(key=lambda x: x["priority_score"], reverse=True)
            top = products[:10]

            margins_on_top = [p["margin_pct"] for p in top if p["margin_pct"] > 0]
            return {
                "total_candidates": len(products),
                "total_revenue_at_risk": round(sum(p["recent_revenue"] for p in products), 2),
                "avg_margin_on_priorities": round(
                    sum(margins_on_top) / len(margins_on_top), 1
                ) if margins_on_top else 0,
                "priorities": [
                    {k: v for k, v in p.items() if k != "avg_price"}
                    for p in top
                ],
            }
        except Exception as e:
            log.debug(f"Stocking priorities skipped for {brand}: {e}")
            return None

    def _get_ads_diagnostic(self, brand, cur_start, cur_end) -> Optional[Dict]:
        """Ad performance diagnostic for a brand.

        Spend is computed in two parts:
        1. Brand-specific campaigns (name contains brand) → campaign-level spend
        2. Catch-all campaigns → actual campaign spend allocated by brand's
           product-level share (PMax product spend is inflated 10-30x so we
           only use it for proportional allocation, never absolute spend)
        """
        try:
            ads_campaign_rows = self._get_ads_campaign_rows(cur_start, cur_end)
            metrics = self._get_ads_campaign_metrics(brand, ads_campaign_rows)

            camp_spend = metrics.get("spend", 0)
            camp_roas = metrics.get("roas", 0) or 0
            imp_share = metrics.get("imp_share")
            budget_lost = metrics.get("budget_lost")
            rank_lost = metrics.get("rank_lost")
            matched_campaigns = metrics.get("matched_campaigns", set())

            start_d = cur_start.date() if hasattr(cur_start, "date") else cur_start
            end_d = cur_end.date() if hasattr(cur_end, "date") else cur_end

            # Use pre-computed product summary for catch-all allocation
            product_summary = self._get_ads_product_summary(cur_start, cur_end)
            brand_product_spend = product_summary.get(brand, {}).get("spend", 0)

            # shared_spend = brand's proportional allocation minus what's
            # already counted in brand-specific campaigns
            shared_spend = max(brand_product_spend - camp_spend, 0)

            total_spend = camp_spend + shared_spend

            # Product-level top performers (only for this brand's products)
            product_perf = []
            wasted = []
            zero_conv_clicks = []
            brand_pids = (
                self.db.query(ShopifyProduct.shopify_product_id)
                .filter(ShopifyProduct.vendor == brand)
                .all()
            )
            brand_pid_set = {str(r[0]) for r in brand_pids} if brand_pids else set()

            if brand_pid_set:
                # Build LIKE patterns for top products only (limit query scope)
                like_patterns = [f"shopify_au_{pid}_%" for pid in brand_pid_set]

                # Query only this brand's products (much faster than all 238k rows)
                from sqlalchemy import or_
                brand_prod_rows = (
                    self.db.query(
                        GoogleAdsProductPerformance.product_item_id,
                        GoogleAdsProductPerformance.product_title,
                        func.sum(GoogleAdsProductPerformance.clicks).label("clicks"),
                        func.sum(GoogleAdsProductPerformance.cost_micros).label("cost_micros"),
                        func.sum(GoogleAdsProductPerformance.conversions).label("conv"),
                        func.sum(GoogleAdsProductPerformance.conversions_value).label("conv_val"),
                    )
                    .filter(
                        GoogleAdsProductPerformance.date >= start_d,
                        GoogleAdsProductPerformance.date < end_d,
                        or_(*[GoogleAdsProductPerformance.product_item_id.like(p) for p in like_patterns[:50]]),
                    )
                    .group_by(
                        GoogleAdsProductPerformance.product_item_id,
                        GoogleAdsProductPerformance.product_title,
                    )
                    .order_by(func.sum(GoogleAdsProductPerformance.cost_micros).desc())
                    .limit(10)
                    .all()
                )

                for pr in brand_prod_rows:
                    clicks = int(pr.clicks or 0)
                    spend = int(pr.cost_micros or 0) / 1_000_000
                    conv = float(pr.conv or 0)
                    conv_val = float(pr.conv_val or 0)
                    roas = round(conv_val / spend, 2) if spend > 0 else None
                    entry = {
                        "product_id": pr.product_item_id,
                        "title": pr.product_title or "Unknown",
                        "spend": round(spend, 2),
                        "clicks": clicks,
                        "conversions": round(conv, 1),
                        "roas": roas,
                    }
                    product_perf.append(entry)
                    if spend > 50 and conv == 0:
                        wasted.append(entry)
                    # Separate non-monetary signal: high click volume but zero conversions.
                    if clicks > 50 and conv == 0:
                        zero_conv_clicks.append({
                            "product_id": pr.product_item_id,
                            "title": pr.product_title or "Unknown",
                            "clicks": clicks,
                        })

            if total_spend == 0 and not product_perf:
                return None

            scaling = (imp_share is not None and imp_share < 80 and camp_roas > 3)

            return {
                "metric_scope": "campaign+product",
                "time_window": f"{cur_start.strftime('%Y-%m-%d')} to {cur_end.strftime('%Y-%m-%d')}",
                "campaign_spend": round(total_spend, 2),
                "campaign_spend_branded": round(camp_spend, 2),
                "campaign_spend_shared": round(shared_spend, 2),
                "campaign_roas": camp_roas,
                "impression_share": imp_share,
                "budget_lost_share": budget_lost,
                "rank_lost_share": rank_lost,
                "product_performance": product_perf[:5],
                "wasted_spend_products": wasted[:5],
                "zero_conversion_high_click_products": zero_conv_clicks[:5],
                "scaling_opportunity": scaling,
            }
        except Exception as e:
            log.debug(f"Ads diagnostic skipped for {brand}: {e}")
            return None

    def _get_demand_signals(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        """Branded search demand from GSC."""
        try:
            include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
            brand_norm = (brand or "").strip().lower()
            brand_clause = SearchConsoleQuery.query.ilike(f"%{brand}%") if brand else None
            exact_brand = func.lower(SearchConsoleQuery.query) == brand_norm if brand_norm else None
            term_clauses = [SearchConsoleQuery.query.ilike(f"%{t}%") for t in include_terms]
            if allowlist_used and brand_clause is not None and term_clauses:
                include_expr = or_(exact_brand, and_(brand_clause, or_(*term_clauses)))
            elif brand_clause is not None:
                include_expr = or_(exact_brand, brand_clause)
            else:
                include_expr = exact_brand

            def _gsc_agg(start, end):
                q = (
                    self.db.query(
                        func.sum(SearchConsoleQuery.clicks).label("clicks"),
                        func.sum(SearchConsoleQuery.impressions).label("impr"),
                    )
                    .filter(
                        include_expr,
                        SearchConsoleQuery.date >= start.date() if hasattr(start, "date") else start,
                        SearchConsoleQuery.date < end.date() if hasattr(end, "date") else end,
                    )
                )
                for term in exclude_terms:
                    q = q.filter(~SearchConsoleQuery.query.ilike(f"%{term}%"))
                row = q.first()
                return {
                    "clicks": int(row.clicks or 0) if row else 0,
                    "impressions": int(row.impr or 0) if row else 0,
                }

            cur = _gsc_agg(cur_start, cur_end)
            prev = _gsc_agg(yoy_start, yoy_end) if yoy_start and yoy_end else {"clicks": 0, "impressions": 0}

            if cur["clicks"] == 0 and prev["clicks"] == 0:
                return None

            # Top queries
            top_q_base = (
                self.db.query(
                    SearchConsoleQuery.query,
                    func.sum(SearchConsoleQuery.clicks).label("clicks"),
                    func.sum(SearchConsoleQuery.impressions).label("impr"),
                    func.avg(SearchConsoleQuery.position).label("pos"),
                )
                .filter(
                    include_expr,
                    SearchConsoleQuery.date >= cur_start.date() if hasattr(cur_start, "date") else cur_start,
                    SearchConsoleQuery.date < cur_end.date() if hasattr(cur_end, "date") else cur_end,
                )
            )
            for term in exclude_terms:
                top_q_base = top_q_base.filter(~SearchConsoleQuery.query.ilike(f"%{term}%"))

            top_q = (
                top_q_base
                .group_by(SearchConsoleQuery.query)
                .order_by(func.sum(SearchConsoleQuery.clicks).desc())
                .limit(5)
                .all()
            )

            queries = [{
                "query": q.query,
                "clicks": int(q.clicks or 0),
                "impressions": int(q.impr or 0),
                "avg_position": round(_dec(q.pos), 1),
            } for q in top_q]

            return {
                "cur_clicks": cur["clicks"],
                "cur_impressions": cur["impressions"],
                "yoy_clicks": prev["clicks"],
                "yoy_impressions": prev["impressions"],
                "clicks_yoy_pct": _pct_change(cur["clicks"], prev["clicks"]),
                "impressions_yoy_pct": _pct_change(cur["impressions"], prev["impressions"]),
                "top_queries": queries,
            }
        except Exception as e:
            log.debug(f"Demand signals skipped for {brand}: {e}")
            return None

    def _get_conversion_signals(self, brand, cur_start, cur_end) -> Optional[Dict]:
        """GA4 product funnel: views -> cart -> purchase."""
        try:
            # Build a broad set of identifiers that GA4 item_id might match:
            # 1. shopify_product_id (as string)
            # 2. handle (URL slug)
            # 3. SKUs from order items
            # 4. variant_id (as string)
            # 5. shopify_{country}_{product_id}_{variant_id}
            brand_products = (
                self.db.query(
                    func.cast(ShopifyProduct.shopify_product_id, String),
                    ShopifyProduct.handle,
                    ShopifyProduct.variants,
                )
                .filter(ShopifyProduct.vendor == brand)
                .all()
            )
            if not brand_products:
                return None
            id_set = set()
            variant_ids = set()
            country_prefixes = ("shopify_AU", "shopify_US", "shopify_CA", "shopify_NZ", "shopify_GB")
            for r in brand_products:
                pid = str(r[0]) if r[0] else None
                if pid:
                    id_set.add(pid)
                if r[1]:
                    id_set.add(r[1])
                variants = r[2] or []
                if isinstance(variants, list):
                    for v in variants:
                        vid = v.get("id") if isinstance(v, dict) else None
                        if not vid:
                            continue
                        vid = str(vid)
                        variant_ids.add(vid)
                        # Shopify GA4 item_id format: shopify_{country}_{product_id}_{variant_id}
                        if pid:
                            for prefix in country_prefixes:
                                id_set.add(f"{prefix}_{pid}_{vid}")
            # Also gather distinct SKUs for this vendor
            sku_rows = (
                self.db.query(func.distinct(ShopifyOrderItem.sku))
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.sku.isnot(None),
                    ShopifyOrderItem.sku != "",
                )
                .all()
            )
            for r in sku_rows:
                if r[0]:
                    id_set.add(r[0])

            # Gather variant IDs from order items (covers cases not in product variants JSON)
            variant_rows = (
                self.db.query(func.distinct(ShopifyOrderItem.shopify_variant_id))
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.shopify_variant_id.isnot(None),
                )
                .all()
            )
            for r in variant_rows:
                if r[0]:
                    variant_ids.add(str(r[0]))

            for vid in variant_ids:
                id_set.add(vid)

            # Add shopify_{country}_{product_id}_{variant_id} from actual order items
            pair_rows = (
                self.db.query(
                    func.distinct(ShopifyOrderItem.shopify_product_id),
                    ShopifyOrderItem.shopify_variant_id,
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.shopify_product_id.isnot(None),
                    ShopifyOrderItem.shopify_variant_id.isnot(None),
                )
                .all()
            )
            for pid, vid in pair_rows:
                pid = str(pid)
                vid = str(vid)
                for prefix in country_prefixes:
                    id_set.add(f"{prefix}_{pid}_{vid}")
            pid_list = list(id_set)

            agg = (
                self.db.query(
                    func.sum(GA4ProductPerformance.items_viewed).label("views"),
                    func.sum(GA4ProductPerformance.items_added_to_cart).label("carts"),
                    func.sum(GA4ProductPerformance.items_purchased).label("purchases"),
                    func.sum(GA4ProductPerformance.item_revenue).label("revenue"),
                )
                .filter(
                    GA4ProductPerformance.item_id.in_(pid_list),
                    GA4ProductPerformance.date >= cur_start.date() if hasattr(cur_start, "date") else cur_start,
                    GA4ProductPerformance.date < cur_end.date() if hasattr(cur_end, "date") else cur_end,
                )
                .first()
            )

            views = int(agg.views or 0) if agg else 0
            carts = int(agg.carts or 0) if agg else 0
            purchases = int(agg.purchases or 0) if agg else 0
            revenue = _dec(agg.revenue) if agg else 0

            if views == 0 and purchases == 0:
                return None

            v2c = round(carts / views * 100, 1) if views > 0 else 0
            c2p = round(purchases / carts * 100, 1) if carts > 0 else 0
            overall = round(purchases / views * 100, 2) if views > 0 else 0

            # Per-product funnel (top 5 by views)
            prod_rows = (
                self.db.query(
                    GA4ProductPerformance.item_id,
                    GA4ProductPerformance.item_name,
                    func.sum(GA4ProductPerformance.items_viewed).label("views"),
                    func.sum(GA4ProductPerformance.items_added_to_cart).label("carts"),
                    func.sum(GA4ProductPerformance.items_purchased).label("purchases"),
                    func.sum(GA4ProductPerformance.item_revenue).label("revenue"),
                )
                .filter(
                    GA4ProductPerformance.item_id.in_(pid_list),
                    GA4ProductPerformance.date >= cur_start.date() if hasattr(cur_start, "date") else cur_start,
                    GA4ProductPerformance.date < cur_end.date() if hasattr(cur_end, "date") else cur_end,
                )
                .group_by(GA4ProductPerformance.item_id, GA4ProductPerformance.item_name)
                .order_by(func.sum(GA4ProductPerformance.items_viewed).desc())
                .limit(5)
                .all()
            )

            funnels = []
            for pr in prod_rows:
                pv = int(pr.views or 0)
                pc = int(pr.carts or 0)
                pp = int(pr.purchases or 0)
                funnels.append({
                    "product_id": pr.item_id,
                    "title": pr.item_name or "Unknown",
                    "views": pv,
                    "add_to_cart": pc,
                    "purchases": pp,
                    "view_to_cart_pct": round(pc / pv * 100, 1) if pv > 0 else 0,
                    "cart_to_purchase_pct": round(pp / pc * 100, 1) if pc > 0 else 0,
                })

            return {
                "total_views": views,
                "total_add_to_cart": carts,
                "total_purchases": purchases,
                "total_revenue": round(revenue, 2),
                "view_to_cart_pct": v2c,
                "cart_to_purchase_pct": c2p,
                "overall_conversion_pct": overall,
                "product_funnels": funnels,
            }
        except Exception as e:
            log.debug(f"Conversion signals skipped for {brand}: {e}")
            return None

    def _compute_driver_confidence(self, driver, shared_count=0, cogs_coverage=0) -> str:
        """Assign confidence level to a WHY driver."""
        d = driver.get("driver", "")
        if d in ("volume", "price"):
            if shared_count > 5:
                return "high"
            elif shared_count >= 2:
                return "medium"
            return "low"
        if d in ("new_products", "lost_products"):
            return "high"
        if d == "mix":
            return "low"
        if d == "margin":
            return "high" if cogs_coverage > 0.5 else "medium"
        if d == "ad_spend":
            return "medium"
        if d == "competitive":
            return "medium"
        return "medium"

    # ── Executive summary ─────────────────────────────────────

    def get_executive_summary(self, period_days: int = 30) -> Dict:
        """High-level executive view: at-risk, watchlist, overperformers."""
        dashboard = self.get_dashboard(period_days)
        brands = dashboard.get("brands", [])

        at_risk = []
        watchlist = []
        overperformers = []
        total_recoverable = 0

        for b in brands:
            health = self._compute_brand_health(b)
            b_entry = {
                "brand": b["brand"],
                "revenue": b["revenue"],
                "revenue_yoy_pct": b["revenue_yoy_pct"],
                "gross_margin_pct": b["gross_margin_pct"],
                "cost_coverage_pct": b.get("cost_coverage_pct", 0),
                "estimated_margin_pct": b.get("estimated_margin_pct"),
                "has_cost_data": b.get("has_cost_data", False),
                "tier": b["tier"],
                "health_score": health["score"],
                "health_grade": health["grade"],
                "severity": health["severity"],
                "flags": health["flags"],
                "revenue_at_risk": health["revenue_at_risk"],
            }

            # Diagnosis string
            if health["flags"]:
                flag_labels = {
                    "revenue_declining": "Revenue declining YoY",
                    "volume_decline": "Unit volumes falling",
                    "margin_erosion": "Margins under pressure",
                    "competitive_pressure": "Competitive undercuts detected",
                    "range_shrinking": "Product range shrinking",
                }
                b_entry["diagnosis"] = flag_labels.get(health["flags"][0], health["flags"][0])
            else:
                b_entry["diagnosis"] = "No issues detected"

            if health["severity"] == "critical" and b["revenue"] > 5000:
                at_risk.append(b_entry)
                total_recoverable += health["revenue_at_risk"]
            elif health["severity"] == "warning" or (b["tier"] == "declining" and b["revenue"] > 1000):
                watchlist.append(b_entry)
            elif b["tier"] == "growing" and (b.get("estimated_margin_pct") or b["gross_margin_pct"]) > 10:
                overperformers.append(b_entry)

        at_risk.sort(key=lambda x: x["revenue_at_risk"], reverse=True)
        watchlist.sort(key=lambda x: x["revenue"], reverse=True)
        overperformers.sort(key=lambda x: x["revenue"], reverse=True)

        return {
            "period_days": period_days,
            "total_brands": len(brands),
            "at_risk": at_risk,
            "watchlist": watchlist[:15],
            "overperformers": overperformers[:15],
            "recoverable_revenue": round(total_recoverable, 2),
        }

    # ── Opportunity ranking ───────────────────────────────────

    def get_opportunity_ranking(self, period_days: int = 30, limit: int = 10) -> Dict:
        """Rank brands by forward-looking growth opportunity score."""
        dashboard = self.get_dashboard(period_days)
        brands = dashboard.get("brands", [])

        if not brands:
            return {"period_days": period_days, "opportunities": []}

        now = self._anchored_now()
        cur_start = now - timedelta(days=period_days)
        cur_end = now
        yoy_start = cur_start - timedelta(days=365)
        yoy_end = cur_end - timedelta(days=365)

        # Phase 1: preliminary score from dashboard data (free)
        prelim = []
        for b in brands:
            rev = b.get("revenue") or 0
            if rev < 500:
                continue
            yoy = b.get("revenue_yoy_pct") or 0
            roas = b.get("ads_roas") or 0
            imp_share = b.get("ads_imp_share") or 100

            momentum = max(min(yoy, 100), 0) / 100
            ads_scale = 0
            if roas >= 2:
                ads_scale = min(roas, 10) / 10 * max(0, 1 - imp_share / 100)
            p_score = 0.45 * momentum + 0.30 * ads_scale + 0.25 * min(rev, 500000) / 500000
            prelim.append({"brand_data": b, "prelim": p_score})

        prelim.sort(key=lambda x: x["prelim"], reverse=True)

        # Phase 2: enrich top 15 with demand and pricing
        enriched = []
        scored_items = []

        for item in prelim[:15]:
            b = item["brand_data"]
            brand = b["brand"]

            # Demand
            try:
                demand = self._get_demand_signals(brand, cur_start, cur_end, yoy_start, yoy_end)
            except Exception:
                demand = None

            # Price index (lightweight)
            try:
                latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
                if latest_date:
                    rows = (
                        self.db.query(
                            CompetitivePricing.current_price,
                            CompetitivePricing.lowest_competitor_price,
                        )
                        .filter(
                            CompetitivePricing.vendor == brand,
                            CompetitivePricing.pricing_date == latest_date,
                            CompetitivePricing.current_price.isnot(None),
                            CompetitivePricing.lowest_competitor_price > 0,
                        )
                        .all()
                    )
                    ratios = [float(r[0]) / float(r[1]) for r in rows if r[0] and r[1]]
                    price_index = round(sum(ratios) / len(ratios), 2) if ratios else None
                else:
                    price_index = None
            except Exception:
                price_index = None

            scored_items.append({
                "brand_data": b,
                "demand": demand,
                "price_index": price_index,
            })

        # Compute final scores
        for item in scored_items:
            b = item["brand_data"]
            demand = item["demand"]
            pi = item["price_index"]

            # 1. Demand tailwind (0.25)
            clicks_yoy = (demand or {}).get("clicks_yoy_pct") or 0
            demand_score = max(min(clicks_yoy, 200), 0) / 200

            # 2. Ads scalability (0.20)
            roas = b.get("ads_roas") or 0
            imp_share = b.get("ads_imp_share") or 100
            ads_score = 0
            if roas >= 2:
                ads_score = min(roas, 10) / 10 * max(0, 1 - imp_share / 100)

            # 3. Pricing edge (0.20)
            if pi is not None:
                pricing_score = max(min(1.20 - pi, 0.30), 0) / 0.30
            else:
                pricing_score = 0.5  # neutral default

            # 4. Momentum (0.25)
            yoy = b.get("revenue_yoy_pct") or 0
            momentum_score = max(min(yoy, 100), 0) / 100

            opp_score = round((
                0.30 * demand_score
                + 0.25 * ads_score
                + 0.20 * pricing_score
                + 0.25 * momentum_score
            ) * 100, 1)

            if opp_score <= 0:
                continue

            # Top action: build a short summary from highest-scoring signal
            best_signal = max(
                [("demand_tailwind", demand_score), ("ads_scalability", ads_score),
                 ("pricing_edge", pricing_score), ("momentum", momentum_score)],
                key=lambda x: x[1],
            )
            top_action = self._opportunity_action_summary(
                best_signal[0], b, demand, None, pi,
            )

            enriched.append({
                "brand": b["brand"],
                "opportunity_score": opp_score,
                "revenue": b.get("revenue", 0),
                "revenue_yoy_pct": b.get("revenue_yoy_pct"),
                "signals": {
                    "demand_tailwind": {"value": clicks_yoy, "score": round(demand_score, 3)},
                    "ads_scalability": {"roas": roas, "imp_share": imp_share, "score": round(ads_score, 3)},
                    "pricing_edge": {"price_index": pi, "score": round(pricing_score, 3)},
                    "momentum": {"revenue_yoy_pct": yoy, "score": round(momentum_score, 3)},
                },
                "top_action": top_action,
            })

        enriched.sort(key=lambda x: x["opportunity_score"], reverse=True)

        return {
            "period_days": period_days,
            "opportunities": enriched[:limit],
        }

    def _opportunity_action_summary(self, signal_name, brand_data, demand, stocking, price_index):
        """Generate a short action summary based on the strongest opportunity signal."""
        brand = brand_data["brand"]
        if signal_name == "demand_tailwind" and demand:
            yoy = demand.get("clicks_yoy_pct", 0)
            return f"Demand up {yoy:.0f}% — ensure stock and ad coverage for {brand}"
        elif signal_name == "ads_scalability":
            roas = brand_data.get("ads_roas", 0)
            imp = brand_data.get("ads_imp_share", 0)
            return f"Scale {brand} ads (ROAS {roas:.1f}x, IS {imp:.0f}%) — capture missing auctions"
        elif signal_name == "pricing_edge" and price_index is not None:
            return f"{brand} priced {(1 - price_index) * 100:+.0f}% vs competitors — competitive advantage"
        elif signal_name == "momentum":
            yoy = brand_data.get("revenue_yoy_pct", 0)
            return f"{brand} revenue up {yoy:.0f}% YoY — invest to sustain growth"
        return f"Growth opportunity identified for {brand}"


def _month_name(mo: str) -> str:
    names = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
    }
    return names.get(mo, mo)
