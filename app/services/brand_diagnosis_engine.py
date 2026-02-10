"""
Brand Diagnosis Engine

Produces an ML-ready structured decomposition of brand performance.
Every driver is quantified and cross-signal confirmed.  Stock is
informational-only unless strict gating criteria are met.

This engine consumes data already collected by BrandIntelligenceService
(totals, product breakdowns, diagnostics) and re-synthesises it into the
strict contract required by downstream ML pipelines.
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import func, case, literal_column, String, and_

from app.models.shopify import (
    ShopifyOrderItem, ShopifyProduct, ShopifyInventory,
    ShopifyRefundLineItem,
)
from app.models.google_ads_data import GoogleAdsProductPerformance, GoogleAdsCampaign
from app.models.ga4_data import GA4ProductPerformance
from app.models.search_console_data import SearchConsoleQuery
from app.models.competitive_pricing import CompetitivePricing
from app.utils.logger import log


def _f(v):
    """Decimal/None → float."""
    if v is None:
        return 0.0
    return float(v)


def _pct(cur, prev):
    if not prev:
        return None
    return round((cur - prev) / prev * 100, 2)


def _safe_div(a, b, default=0.0):
    return round(a / b, 4) if b else default


# ────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────

class BrandDiagnosisEngine:
    """
    Pure rule-based decomposition engine.  No LLM calls.

    Inputs: a brand name + period → queries the DB for all signals.
    Output: the strict ML contract dict.
    """

    def __init__(self, db: Session):
        self.db = db

    # ── public entry point ─────────────────────────────────────

    def diagnose(self, brand: str, period_days: int = 30) -> Dict:
        now = datetime.utcnow()
        cur_end = now
        cur_start = now - timedelta(days=period_days)
        yoy_end = cur_end - timedelta(days=365)
        yoy_start = cur_start - timedelta(days=365)

        period_label = f"{cur_start.strftime('%Y-%m-%d')} to {cur_end.strftime('%Y-%m-%d')}"

        # ── gather raw data ──
        cur_totals = self._brand_totals(brand, cur_start, cur_end)
        yoy_totals = self._brand_totals(brand, yoy_start, yoy_end)
        cur_products = self._product_breakdown(brand, cur_start, cur_end)
        yoy_products = self._product_breakdown(brand, yoy_start, yoy_end)
        cur_map = {p["product_id"]: p for p in cur_products}
        yoy_map = {p["product_id"]: p for p in yoy_products}

        # ── diagnostics (each wrapped in try/except) ──
        pricing_diag = self._safe(self._pricing_diagnostic, brand)
        ads_diag = self._safe(self._ads_diagnostic, brand, cur_start, cur_end, yoy_start, yoy_end)
        demand_diag = self._safe(self._demand_diagnostic, brand, cur_start, cur_end, yoy_start, yoy_end)
        funnel_diag = self._safe(self._funnel_diagnostic, brand, cur_start, cur_end, yoy_start, yoy_end)
        fulfilment_diag = self._safe(self._fulfilment_diagnostic, brand, cur_start, cur_end, yoy_start, yoy_end)
        stock_diag = self._safe(self._stock_diagnostic, brand, cur_start, cur_end, yoy_start, yoy_end, funnel_diag)

        # ── performance decomposition (the core) ──
        decomposition = self._decompose(
            brand, cur_totals, yoy_totals, cur_map, yoy_map,
            ads_diag, demand_diag, funnel_diag, fulfilment_diag,
        )

        # ── anomaly detection ──
        anomalies = self._detect_anomalies(
            cur_totals, yoy_totals, pricing_diag, ads_diag,
            demand_diag, funnel_diag, fulfilment_diag,
        )

        # ── momentum score ──
        momentum = self._momentum_score(
            cur_totals, yoy_totals, demand_diag, ads_diag, funnel_diag,
        )

        return {
            "brand": brand,
            "period": period_label,
            "performance_decomposition": decomposition,
            "anomalies": anomalies,
            "pricing_model": pricing_diag or self._empty_pricing(),
            "ads_model": ads_diag or self._empty_ads(),
            "funnel_model": funnel_diag or self._empty_funnel(),
            "fulfilment_model": fulfilment_diag or self._empty_fulfilment(),
            "stock_model": stock_diag or self._empty_stock(),
            "momentum_score": momentum,
        }

    # ── performance decomposition ──────────────────────────────

    def _decompose(self, brand, cur, yoy, cur_map, yoy_map,
                   ads_diag, demand_diag, funnel_diag, fulfilment_diag) -> Dict:
        """
        Decompose revenue change into additive drivers that sum to ~100%.

        Method: direct attribution on shared products (volume × price),
        new/lost product deltas, then allocate residual across ads,
        demand, conversion, and fulfilment friction proportionally.
        """
        cur_rev = cur["revenue"]
        yoy_rev = yoy["revenue"]
        delta = cur_rev - yoy_rev
        delta_pct = _pct(cur_rev, yoy_rev)

        shared_ids = set(cur_map.keys()) & set(yoy_map.keys())

        # ── 1. Volume effect (unit Δ × prior ASP) ──
        volume_effect = 0.0
        for pid in shared_ids:
            c, y = cur_map[pid], yoy_map[pid]
            volume_effect += (c["units"] - y["units"]) * y["avg_price"]

        # ── 2. Price effect (ASP Δ × prior units) ──
        price_effect = 0.0
        for pid in shared_ids:
            c, y = cur_map[pid], yoy_map[pid]
            price_effect += (c["avg_price"] - y["avg_price"]) * y["units"]

        # ── 3. Product mix (new product revenue - lost product revenue) ──
        new_ids = set(cur_map.keys()) - set(yoy_map.keys())
        lost_ids = set(yoy_map.keys()) - set(cur_map.keys())
        new_rev = sum(cur_map[p]["revenue"] for p in new_ids)
        lost_rev = sum(yoy_map[p]["revenue"] for p in lost_ids)
        product_mix_effect = new_rev - lost_rev

        # ── 4. Residual (what volume + price + mix doesn't explain) ──
        mechanical_explained = volume_effect + price_effect + product_mix_effect
        residual = delta - mechanical_explained

        # Allocate residual to soft signals proportionally by evidence strength
        ads_signal = self._ads_signal_strength(ads_diag)
        demand_signal = self._demand_signal_strength(demand_diag)
        conversion_signal = self._conversion_signal_strength(funnel_diag)
        fulfilment_signal = self._fulfilment_signal_strength(fulfilment_diag)

        total_signal = ads_signal + demand_signal + conversion_signal + fulfilment_signal

        if total_signal > 0 and abs(residual) > 0:
            ads_alloc = residual * (ads_signal / total_signal)
            demand_alloc = residual * (demand_signal / total_signal)
            conversion_alloc = residual * (conversion_signal / total_signal)
            fulfilment_alloc = residual * (fulfilment_signal / total_signal)
        else:
            # If no soft signals, residual stays in product_mix
            product_mix_effect += residual
            ads_alloc = 0.0
            demand_alloc = 0.0
            conversion_alloc = 0.0
            fulfilment_alloc = 0.0

        # ── Build contributions (% of total delta) ──
        abs_delta = abs(delta) if abs(delta) > 0 else 1.0  # avoid div-by-zero

        def _contrib_pct(val):
            return round(val / abs_delta * 100, 2)

        contributions = {
            "volume": {
                "dollars": round(volume_effect, 2),
                "pct_of_change": _contrib_pct(volume_effect),
                "direction": "positive" if volume_effect >= 0 else "negative",
                "confidence": "high" if len(shared_ids) > 5 else ("medium" if len(shared_ids) >= 2 else "low"),
            },
            "price": {
                "dollars": round(price_effect, 2),
                "pct_of_change": _contrib_pct(price_effect),
                "direction": "positive" if price_effect >= 0 else "negative",
                "confidence": "high" if len(shared_ids) > 5 else ("medium" if len(shared_ids) >= 2 else "low"),
            },
            "product_mix": {
                "dollars": round(product_mix_effect, 2),
                "pct_of_change": _contrib_pct(product_mix_effect),
                "direction": "positive" if product_mix_effect >= 0 else "negative",
                "new_products": len(new_ids),
                "lost_products": len(lost_ids),
                "confidence": "high",
            },
            "ads_effectiveness": {
                "dollars": round(ads_alloc, 2),
                "pct_of_change": _contrib_pct(ads_alloc),
                "direction": "positive" if ads_alloc >= 0 else "negative",
                "confidence": "high" if ads_signal > 0.6 else ("medium" if ads_signal > 0.2 else "low"),
            },
            "demand": {
                "dollars": round(demand_alloc, 2),
                "pct_of_change": _contrib_pct(demand_alloc),
                "direction": "positive" if demand_alloc >= 0 else "negative",
                "confidence": "high" if demand_signal > 0.6 else ("medium" if demand_signal > 0.2 else "low"),
            },
            "conversion": {
                "dollars": round(conversion_alloc, 2),
                "pct_of_change": _contrib_pct(conversion_alloc),
                "direction": "positive" if conversion_alloc >= 0 else "negative",
                "confidence": "high" if conversion_signal > 0.6 else ("medium" if conversion_signal > 0.2 else "low"),
            },
            "fulfilment_friction": {
                "dollars": round(fulfilment_alloc, 2),
                "pct_of_change": _contrib_pct(fulfilment_alloc),
                "direction": "positive" if fulfilment_alloc >= 0 else "negative",
                "confidence": "high" if fulfilment_signal > 0.6 else ("medium" if fulfilment_signal > 0.2 else "low"),
            },
        }

        # Verify sum ≈ 100%
        total_pct = sum(c["pct_of_change"] for c in contributions.values())

        return {
            "revenue_current": round(cur_rev, 2),
            "revenue_prior": round(yoy_rev, 2),
            "revenue_delta": round(delta, 2),
            "revenue_delta_pct": delta_pct,
            "driver_contributions": contributions,
            "decomposition_coverage_pct": round(total_pct, 2),
        }

    # ── signal strength estimators ──────────────────────────────
    # Each returns 0.0-1.0 indicating how much evidence the signal
    # provides for explaining residual revenue change.

    def _ads_signal_strength(self, diag) -> float:
        if not diag:
            return 0.0
        spend_chg = abs(diag.get("spend_change_pct") or 0)
        roas_chg = abs(diag.get("roas_change_pct") or 0)
        has_data = diag.get("cur_spend", 0) > 0 or diag.get("prev_spend", 0) > 0
        if not has_data:
            return 0.0
        # Strong signal if spend or ROAS changed materially
        score = min(1.0, (spend_chg / 50) * 0.5 + (roas_chg / 50) * 0.5)
        return round(score, 3)

    def _demand_signal_strength(self, diag) -> float:
        if not diag:
            return 0.0
        clicks_chg = abs(diag.get("clicks_yoy_pct") or 0)
        has_data = (diag.get("cur_clicks", 0) + diag.get("prev_clicks", 0)) > 0
        if not has_data:
            return 0.0
        return round(min(1.0, clicks_chg / 40), 3)

    def _conversion_signal_strength(self, diag) -> float:
        if not diag:
            return 0.0
        v2c_chg = abs(diag.get("view_to_cart_change_pp") or 0)
        c2p_chg = abs(diag.get("cart_to_purchase_change_pp") or 0)
        has_data = diag.get("cur_views", 0) > 100
        if not has_data:
            return 0.0
        return round(min(1.0, (v2c_chg / 5) * 0.5 + (c2p_chg / 3) * 0.5), 3)

    def _fulfilment_signal_strength(self, diag) -> float:
        if not diag:
            return 0.0
        refund_chg = abs(diag.get("refund_rate_change_pp") or 0)
        cancel_chg = abs(diag.get("cancellation_rate_change_pp") or 0)
        return round(min(1.0, (refund_chg / 5) * 0.6 + (cancel_chg / 3) * 0.4), 3)

    # ── stock gating (NON-NEGOTIABLE) ──────────────────────────

    def _stock_diagnostic(self, brand, cur_start, cur_end,
                          yoy_start, yoy_end, funnel_diag) -> Optional[Dict]:
        """
        Stock is INFORMATIONAL ONLY.  Weight → near zero unless
        at least one gating criterion is met.
        """
        inv_rows = (
            self.db.query(ShopifyInventory)
            .filter(ShopifyInventory.vendor == brand)
            .all()
        )
        if not inv_rows:
            return self._empty_stock()

        known = [r for r in inv_rows if r.inventory_quantity is not None]
        if not known:
            return self._empty_stock()

        total = len(known)
        oos = [r for r in known if r.inventory_quantity == 0]
        oos_count = len(oos)
        low_stock = sum(1 for r in known if 0 < r.inventory_quantity <= 3)
        oos_rate = round(oos_count / total * 100, 1) if total else 0

        # ── Gating checks ──
        gate_passed = False
        gate_reasons = []

        oos_pids = [r.shopify_product_id for r in oos if r.shopify_product_id]

        # Gate 1: is_purchasable = false
        # A product is NOT purchasable if:
        #   (a) status is not 'active' (draft, archived, unlisted), OR
        #   (b) status is 'active' but published_at is NULL (not visible to customers)
        if oos_pids:
            from sqlalchemy import or_
            not_purchasable = (
                self.db.query(func.count(ShopifyProduct.id))
                .filter(
                    ShopifyProduct.shopify_product_id.in_(oos_pids),
                    or_(
                        ShopifyProduct.status != 'active',
                        ShopifyProduct.published_at.is_(None),
                    ),
                )
                .scalar()
            ) or 0
            if not_purchasable > 0:
                gate_passed = True
                gate_reasons.append(f"{not_purchasable} OOS products are inactive/unpublished")

        # Gate 2: Product removed from ads (had ads previously, now zero)
        if oos_pids:
            oos_pid_strs = [str(p) for p in oos_pids]
            prev_ads = (
                self.db.query(func.count(func.distinct(GoogleAdsProductPerformance.product_item_id)))
                .filter(
                    GoogleAdsProductPerformance.product_item_id.in_(oos_pid_strs),
                    GoogleAdsProductPerformance.date >= yoy_start.date(),
                    GoogleAdsProductPerformance.date < yoy_end.date(),
                    GoogleAdsProductPerformance.impressions > 0,
                )
                .scalar()
            ) or 0
            cur_ads = (
                self.db.query(func.count(func.distinct(GoogleAdsProductPerformance.product_item_id)))
                .filter(
                    GoogleAdsProductPerformance.product_item_id.in_(oos_pid_strs),
                    GoogleAdsProductPerformance.date >= cur_start.date(),
                    GoogleAdsProductPerformance.date < cur_end.date(),
                    GoogleAdsProductPerformance.impressions > 0,
                )
                .scalar()
            ) or 0
            if prev_ads > 0 and cur_ads == 0:
                gate_passed = True
                gate_reasons.append(f"{prev_ads} OOS products removed from ads")

        # Gate 3: GA4 add-to-cart collapses for OOS SKUs
        if oos_pids and funnel_diag:
            atc_change = funnel_diag.get("view_to_cart_change_pp") or 0
            if atc_change < -5:
                gate_passed = True
                gate_reasons.append(
                    f"Add-to-cart rate collapsed by {abs(atc_change):.1f}pp"
                )

        # Gate 4: Refund/cancellation spikes align with inventory zero
        if oos_pids:
            oos_refunds = (
                self.db.query(func.count(ShopifyRefundLineItem.id))
                .filter(
                    ShopifyRefundLineItem.shopify_product_id.in_(oos_pids),
                    ShopifyRefundLineItem.processed_at >= cur_start,
                    ShopifyRefundLineItem.processed_at < cur_end,
                )
                .scalar()
            ) or 0
            prev_refunds = (
                self.db.query(func.count(ShopifyRefundLineItem.id))
                .filter(
                    ShopifyRefundLineItem.shopify_product_id.in_(oos_pids),
                    ShopifyRefundLineItem.processed_at >= yoy_start,
                    ShopifyRefundLineItem.processed_at < yoy_end,
                )
                .scalar()
            ) or 0
            if cur_start != yoy_start and prev_refunds > 0:
                refund_spike_pct = _pct(oos_refunds, prev_refunds)
                if refund_spike_pct is not None and refund_spike_pct > 50:
                    gate_passed = True
                    gate_reasons.append(
                        f"Refunds on OOS products spiked {refund_spike_pct:.0f}% YoY"
                    )

        # ── Weight assignment ──
        if gate_passed:
            stock_weight = min(0.8, oos_rate / 100)
        else:
            stock_weight = 0.01  # near-zero

        return {
            "total_skus": total,
            "oos_count": oos_count,
            "oos_rate": oos_rate,
            "low_stock_count": low_stock,
            "gate_passed": gate_passed,
            "gate_reasons": gate_reasons,
            "stock_weight": round(stock_weight, 4),
            "is_top_reason_eligible": gate_passed,
        }

    # ── diagnostic builders ────────────────────────────────────

    def _pricing_diagnostic(self, brand) -> Optional[Dict]:
        latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
        if not latest_date:
            return None

        rows = (
            self.db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.vendor == brand,
                CompetitivePricing.pricing_date == latest_date,
            )
            .all()
        )
        if not rows:
            return None

        total = len(rows)
        below_min = sum(1 for r in rows if r.is_below_minimum)
        losing = sum(1 for r in rows if r.is_losing_money)

        price_ratios = []
        for r in rows:
            if r.current_price and r.lowest_competitor_price and float(r.lowest_competitor_price) > 0:
                price_ratios.append(float(r.current_price) / float(r.lowest_competitor_price))
        price_index = round(sum(price_ratios) / len(price_ratios), 4) if price_ratios else None

        margins = [float(r.profit_margin_pct) for r in rows if r.profit_margin_pct is not None]
        avg_margin = round(sum(margins) / len(margins), 2) if margins else None
        min_margin = round(min(margins), 2) if margins else None
        max_margin = round(max(margins), 2) if margins else None

        return {
            "snapshot_date": str(latest_date),
            "total_skus": total,
            "below_minimum_count": below_min,
            "below_minimum_pct": round(below_min / total * 100, 2) if total else 0,
            "losing_money_count": losing,
            "losing_money_pct": round(losing / total * 100, 2) if total else 0,
            "price_index_vs_cheapest": price_index,
            "avg_margin_pct": avg_margin,
            "min_margin_pct": min_margin,
            "max_margin_pct": max_margin,
            "competitive_pressure_score": self._competitive_pressure_score(price_index, below_min, losing, total),
        }

    def _competitive_pressure_score(self, price_index, below_min, losing, total) -> float:
        """0-1 composite score of competitive pricing pressure."""
        score = 0.0
        if price_index is not None:
            if price_index > 1.2:
                score += 0.4
            elif price_index > 1.1:
                score += 0.25
            elif price_index > 1.0:
                score += 0.1
        if total > 0:
            score += min(0.3, (below_min / total) * 0.5)
            score += min(0.3, (losing / total) * 0.8)
        return round(min(1.0, score), 3)

    def _ads_diagnostic(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        brand_pids = (
            self.db.query(func.cast(ShopifyProduct.shopify_product_id, String))
            .filter(ShopifyProduct.vendor == brand)
            .all()
        )
        pid_list = [str(r[0]) for r in brand_pids] if brand_pids else []

        def _agg(start, end):
            if not pid_list:
                return {"spend": 0, "conv": 0, "conv_val": 0, "clicks": 0, "impr": 0, "roas": 0}
            row = (
                self.db.query(
                    func.sum(GoogleAdsProductPerformance.cost_micros).label("cost"),
                    func.sum(GoogleAdsProductPerformance.conversions).label("conv"),
                    func.sum(GoogleAdsProductPerformance.conversions_value).label("conv_val"),
                    func.sum(GoogleAdsProductPerformance.clicks).label("clicks"),
                    func.sum(GoogleAdsProductPerformance.impressions).label("impr"),
                )
                .filter(
                    GoogleAdsProductPerformance.product_item_id.in_(pid_list),
                    GoogleAdsProductPerformance.date >= start.date() if hasattr(start, "date") else start,
                    GoogleAdsProductPerformance.date < end.date() if hasattr(end, "date") else end,
                )
                .first()
            )
            spend = _f(row.cost) / 1_000_000 if row and row.cost else 0
            conv = _f(row.conv) if row else 0
            conv_val = _f(row.conv_val) if row else 0
            clicks = int(row.clicks or 0) if row else 0
            impr = int(row.impr or 0) if row else 0
            roas = conv_val / spend if spend > 0 else 0
            return {"spend": spend, "conv": conv, "conv_val": conv_val, "clicks": clicks, "impr": impr, "roas": roas}

        cur = _agg(cur_start, cur_end)
        prev = _agg(yoy_start, yoy_end)

        if cur["spend"] == 0 and prev["spend"] == 0:
            return None

        spend_chg_pct = _pct(cur["spend"], prev["spend"])
        roas_chg_pct = _pct(cur["roas"], prev["roas"])

        # Efficiency: revenue per $ of spend
        cur_eff = cur["conv_val"] / cur["spend"] if cur["spend"] > 0 else 0
        prev_eff = prev["conv_val"] / prev["spend"] if prev["spend"] > 0 else 0

        return {
            "cur_spend": round(cur["spend"], 2),
            "prev_spend": round(prev["spend"], 2),
            "spend_change_pct": spend_chg_pct,
            "cur_roas": round(cur["roas"], 3),
            "prev_roas": round(prev["roas"], 3),
            "roas_change_pct": roas_chg_pct,
            "cur_conversions": round(cur["conv"], 1),
            "prev_conversions": round(prev["conv"], 1),
            "cur_clicks": cur["clicks"],
            "prev_clicks": prev["clicks"],
            "cur_impressions": cur["impr"],
            "prev_impressions": prev["impr"],
            "efficiency_current": round(cur_eff, 3),
            "efficiency_prior": round(prev_eff, 3),
            "ad_driven_revenue_delta": round(cur["conv_val"] - prev["conv_val"], 2),
        }

    def _demand_diagnostic(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        def _gsc(start, end):
            row = (
                self.db.query(
                    func.sum(SearchConsoleQuery.clicks).label("clicks"),
                    func.sum(SearchConsoleQuery.impressions).label("impr"),
                )
                .filter(
                    SearchConsoleQuery.query.ilike(f"%{brand}%"),
                    SearchConsoleQuery.date >= start.date() if hasattr(start, "date") else start,
                    SearchConsoleQuery.date < end.date() if hasattr(end, "date") else end,
                )
                .first()
            )
            return {
                "clicks": int(row.clicks or 0) if row else 0,
                "impressions": int(row.impr or 0) if row else 0,
            }

        cur = _gsc(cur_start, cur_end)
        prev = _gsc(yoy_start, yoy_end)

        if cur["clicks"] == 0 and prev["clicks"] == 0:
            return None

        return {
            "cur_clicks": cur["clicks"],
            "prev_clicks": prev["clicks"],
            "clicks_yoy_pct": _pct(cur["clicks"], prev["clicks"]),
            "cur_impressions": cur["impressions"],
            "prev_impressions": prev["impressions"],
            "impressions_yoy_pct": _pct(cur["impressions"], prev["impressions"]),
        }

    def _funnel_diagnostic(self, brand, cur_start, cur_end,
                           yoy_start, yoy_end) -> Optional[Dict]:
        """GA4 product funnel: views → cart → purchase, current and prior."""
        pid_list = self._ga4_product_ids(brand)
        if not pid_list:
            return None

        def _ga4_agg(start, end):
            row = (
                self.db.query(
                    func.sum(GA4ProductPerformance.items_viewed).label("views"),
                    func.sum(GA4ProductPerformance.items_added_to_cart).label("carts"),
                    func.sum(GA4ProductPerformance.items_purchased).label("purchases"),
                    func.sum(GA4ProductPerformance.item_revenue).label("revenue"),
                )
                .filter(
                    GA4ProductPerformance.item_id.in_(pid_list),
                    GA4ProductPerformance.date >= start.date() if hasattr(start, "date") else start,
                    GA4ProductPerformance.date < end.date() if hasattr(end, "date") else end,
                )
                .first()
            )
            views = int(row.views or 0) if row else 0
            carts = int(row.carts or 0) if row else 0
            purchases = int(row.purchases or 0) if row else 0
            revenue = _f(row.revenue) if row else 0
            return {
                "views": views,
                "carts": carts,
                "purchases": purchases,
                "revenue": revenue,
                "v2c": round(carts / views * 100, 2) if views > 0 else 0,
                "c2p": round(purchases / carts * 100, 2) if carts > 0 else 0,
                "overall": round(purchases / views * 100, 3) if views > 0 else 0,
            }

        cur = _ga4_agg(cur_start, cur_end)
        prev = _ga4_agg(yoy_start, yoy_end)

        if cur["views"] == 0 and prev["views"] == 0:
            return None

        return {
            "cur_views": cur["views"],
            "cur_carts": cur["carts"],
            "cur_purchases": cur["purchases"],
            "cur_revenue": round(cur["revenue"], 2),
            "cur_view_to_cart_pct": cur["v2c"],
            "cur_cart_to_purchase_pct": cur["c2p"],
            "cur_overall_conversion_pct": cur["overall"],
            "prev_views": prev["views"],
            "prev_carts": prev["carts"],
            "prev_purchases": prev["purchases"],
            "prev_view_to_cart_pct": prev["v2c"],
            "prev_cart_to_purchase_pct": prev["c2p"],
            "view_to_cart_change_pp": round(cur["v2c"] - prev["v2c"], 2),
            "cart_to_purchase_change_pp": round(cur["c2p"] - prev["c2p"], 2),
        }

    def _fulfilment_diagnostic(self, brand, cur_start, cur_end,
                               yoy_start, yoy_end) -> Optional[Dict]:
        """Refund rate and cancellation rate changes."""
        def _ref_agg(start, end):
            # Total orders + revenue
            order_row = (
                self.db.query(
                    func.sum(ShopifyOrderItem.total_price).label("revenue"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                    func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label("orders"),
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                    ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded"]),
                )
                .first()
            )
            revenue = _f(order_row.revenue) if order_row else 0
            units = int(order_row.units or 0) if order_row else 0
            orders = int(order_row.orders or 0) if order_row else 0

            # Refund totals (by order date scope via join)
            rpi = (
                self.db.query(
                    func.sum(ShopifyRefundLineItem.subtotal).label("ref_amount"),
                    func.sum(ShopifyRefundLineItem.quantity).label("ref_qty"),
                    func.count(ShopifyRefundLineItem.id).label("ref_count"),
                )
                .join(
                    ShopifyOrderItem,
                    ShopifyRefundLineItem.line_item_id == ShopifyOrderItem.line_item_id,
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                )
                .first()
            )
            ref_amount = _f(rpi.ref_amount) if rpi else 0
            ref_qty = int(rpi.ref_qty or 0) if rpi else 0
            ref_count = int(rpi.ref_count or 0) if rpi else 0

            # Cancellation count (financial_status = 'voided' or 'refunded' fully)
            cancel_count = (
                self.db.query(func.count(func.distinct(ShopifyOrderItem.shopify_order_id)))
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= start,
                    ShopifyOrderItem.order_date < end,
                    ShopifyOrderItem.financial_status == "voided",
                )
                .scalar()
            ) or 0

            refund_rate = round(ref_amount / revenue * 100, 2) if revenue > 0 else 0
            cancel_rate = round(cancel_count / orders * 100, 2) if orders > 0 else 0

            return {
                "revenue": revenue, "units": units, "orders": orders,
                "refund_amount": ref_amount, "refund_qty": ref_qty,
                "refund_count": ref_count, "refund_rate": refund_rate,
                "cancel_count": cancel_count, "cancel_rate": cancel_rate,
            }

        cur = _ref_agg(cur_start, cur_end)
        prev = _ref_agg(yoy_start, yoy_end)

        return {
            "cur_refund_rate_pct": cur["refund_rate"],
            "prev_refund_rate_pct": prev["refund_rate"],
            "refund_rate_change_pp": round(cur["refund_rate"] - prev["refund_rate"], 2),
            "cur_refund_amount": round(cur["refund_amount"], 2),
            "prev_refund_amount": round(prev["refund_amount"], 2),
            "cur_cancellation_rate_pct": cur["cancel_rate"],
            "prev_cancellation_rate_pct": prev["cancel_rate"],
            "cancellation_rate_change_pp": round(cur["cancel_rate"] - prev["cancel_rate"], 2),
            "cur_orders": cur["orders"],
            "prev_orders": prev["orders"],
        }

    # ── anomaly detection ──────────────────────────────────────

    def _detect_anomalies(self, cur, yoy, pricing, ads, demand, funnel, fulfilment) -> Dict:
        """Flag signals that are statistical outliers (simple threshold-based)."""
        anomalies = []

        rev_pct = _pct(cur["revenue"], yoy["revenue"])
        if rev_pct is not None and abs(rev_pct) > 40:
            anomalies.append({
                "signal": "revenue",
                "value": rev_pct,
                "threshold": 40,
                "description": f"Revenue {'surged' if rev_pct > 0 else 'collapsed'} {abs(rev_pct):.1f}% YoY",
            })

        if pricing and pricing.get("losing_money_count", 0) > 3:
            anomalies.append({
                "signal": "pricing_loss",
                "value": pricing["losing_money_count"],
                "threshold": 3,
                "description": f"{pricing['losing_money_count']} SKUs selling below cost",
            })

        if ads:
            roas_chg = ads.get("roas_change_pct")
            if roas_chg is not None and abs(roas_chg) > 50:
                anomalies.append({
                    "signal": "ads_roas",
                    "value": roas_chg,
                    "threshold": 50,
                    "description": f"ROAS {'surged' if roas_chg > 0 else 'collapsed'} {abs(roas_chg):.1f}% YoY",
                })

        if demand:
            clicks_chg = demand.get("clicks_yoy_pct")
            if clicks_chg is not None and abs(clicks_chg) > 40:
                anomalies.append({
                    "signal": "branded_demand",
                    "value": clicks_chg,
                    "threshold": 40,
                    "description": f"Branded search clicks {'surged' if clicks_chg > 0 else 'collapsed'} {abs(clicks_chg):.1f}% YoY",
                })

        if funnel:
            v2c_chg = funnel.get("view_to_cart_change_pp") or 0
            if abs(v2c_chg) > 5:
                anomalies.append({
                    "signal": "view_to_cart",
                    "value": v2c_chg,
                    "threshold": 5,
                    "description": f"View-to-cart rate {'improved' if v2c_chg > 0 else 'dropped'} {abs(v2c_chg):.1f}pp",
                })

        if fulfilment:
            ref_chg = fulfilment.get("refund_rate_change_pp") or 0
            if abs(ref_chg) > 3:
                anomalies.append({
                    "signal": "refund_rate",
                    "value": ref_chg,
                    "threshold": 3,
                    "description": f"Refund rate {'increased' if ref_chg > 0 else 'decreased'} {abs(ref_chg):.1f}pp",
                })

        return {
            "count": len(anomalies),
            "items": anomalies,
        }

    # ── momentum score ─────────────────────────────────────────

    def _momentum_score(self, cur, yoy, demand, ads, funnel) -> Dict:
        """
        Forward-looking 0-100 composite score.

        Weights:
          - Revenue trajectory: 30%
          - Demand trend: 25%
          - Ads efficiency trend: 20%
          - Conversion trend: 25%
        """
        score = 50.0  # neutral baseline
        components = {}

        # Revenue trajectory (30 pts)
        rev_pct = _pct(cur["revenue"], yoy["revenue"])
        if rev_pct is not None:
            rev_score = max(-30, min(30, rev_pct * 0.6))
        else:
            rev_score = 0
        components["revenue_trajectory"] = round(rev_score, 2)
        score += rev_score

        # Demand trend (25 pts)
        if demand:
            clicks_chg = demand.get("clicks_yoy_pct") or 0
            demand_score = max(-25, min(25, clicks_chg * 0.5))
        else:
            demand_score = 0
        components["demand_trend"] = round(demand_score, 2)
        score += demand_score

        # Ads efficiency (20 pts)
        if ads:
            roas_chg = ads.get("roas_change_pct") or 0
            ads_score = max(-20, min(20, roas_chg * 0.3))
        else:
            ads_score = 0
        components["ads_efficiency"] = round(ads_score, 2)
        score += ads_score

        # Conversion trend (25 pts)
        if funnel:
            v2c_chg = funnel.get("view_to_cart_change_pp") or 0
            c2p_chg = funnel.get("cart_to_purchase_change_pp") or 0
            conv_score = max(-25, min(25, (v2c_chg * 2 + c2p_chg * 3)))
        else:
            conv_score = 0
        components["conversion_trend"] = round(conv_score, 2)
        score += conv_score

        score = max(0, min(100, score))

        if score >= 70:
            label = "accelerating"
        elif score >= 55:
            label = "stable_positive"
        elif score >= 45:
            label = "neutral"
        elif score >= 30:
            label = "decelerating"
        else:
            label = "declining"

        return {
            "score": round(score, 1),
            "label": label,
            "components": components,
        }

    # ── data access helpers ────────────────────────────────────

    def _refund_subquery(self):
        return (
            self.db.query(
                ShopifyRefundLineItem.line_item_id,
                func.sum(ShopifyRefundLineItem.subtotal).label("refund_amount"),
                func.sum(ShopifyRefundLineItem.quantity).label("refund_qty"),
            )
            .group_by(ShopifyRefundLineItem.line_item_id)
            .subquery()
        )

    def _brand_totals(self, brand: str, start, end) -> Dict:
        rpi = self._refund_subquery()
        q = self.db.query(
            func.sum(ShopifyOrderItem.total_price).label("revenue"),
            func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column("0"))).label("discounts"),
            func.sum(func.coalesce(rpi.c.refund_amount, literal_column("0"))).label("refunds"),
            func.sum(ShopifyOrderItem.quantity).label("units"),
            func.sum(func.coalesce(rpi.c.refund_qty, literal_column("0"))).label("refund_units"),
            func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label("orders"),
            func.sum(
                case(
                    (ShopifyOrderItem.cost_per_item.isnot(None),
                     ShopifyOrderItem.cost_per_item * ShopifyOrderItem.quantity),
                    else_=literal_column("0"),
                )
            ).label("total_cogs"),
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
            ).label("units_with_cost"),
        ).outerjoin(
            rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id,
        ).filter(
            ShopifyOrderItem.vendor == brand,
            ShopifyOrderItem.order_date >= start,
            ShopifyOrderItem.order_date < end,
            ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded", "refunded"]),
        )
        r = q.first()

        gross_rev = _f(r.revenue) if r else 0
        discounts = _f(r.discounts) if r else 0
        refunds = _f(r.refunds) if r else 0
        net_rev = gross_rev - discounts - refunds
        gross_cogs = _f(r.total_cogs) if r else 0
        refund_cogs = _f(r.refund_cogs) if r else 0
        net_cogs = gross_cogs - refund_cogs
        units = (r.units or 0) if r else 0
        refund_units = int(r.refund_units or 0) if r else 0
        net_units = units - refund_units
        units_costed = int(r.units_with_cost or 0) if r else 0
        cost_coverage = round(units_costed / units * 100, 1) if units > 0 else 0
        margin = round((net_rev - net_cogs) / net_rev * 100, 1) if net_rev > 0 and net_cogs > 0 else 0

        return {
            "revenue": round(net_rev, 2),
            "refunds": round(refunds, 2),
            "units": net_units,
            "orders": (r.orders or 0) if r else 0,
            "total_cogs": round(net_cogs, 2),
            "gross_margin_pct": margin,
            "cost_coverage_pct": cost_coverage,
        }

    def _product_breakdown(self, brand: str, start, end) -> List[Dict]:
        rpi = self._refund_subquery()
        rows = (
            self.db.query(
                ShopifyOrderItem.shopify_product_id,
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku,
                func.sum(ShopifyOrderItem.total_price).label("revenue"),
                func.sum(func.coalesce(ShopifyOrderItem.total_discount, literal_column("0"))).label("discounts"),
                func.sum(func.coalesce(rpi.c.refund_amount, literal_column("0"))).label("refunds"),
                func.sum(ShopifyOrderItem.quantity).label("units"),
                func.sum(func.coalesce(rpi.c.refund_qty, literal_column("0"))).label("refund_units"),
                func.avg(ShopifyOrderItem.price).label("avg_price"),
            )
            .outerjoin(rpi, rpi.c.line_item_id == ShopifyOrderItem.line_item_id)
            .filter(
                ShopifyOrderItem.vendor == brand,
                ShopifyOrderItem.order_date >= start,
                ShopifyOrderItem.order_date < end,
                ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded", "refunded"]),
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
            gross = _f(r.revenue)
            disc = _f(r.discounts)
            ref = _f(r.refunds)
            net = gross - disc - ref
            net_units = (r.units or 0) - int(r.refund_units or 0)
            results.append({
                "product_id": r.shopify_product_id,
                "title": r.title or "Unknown",
                "sku": r.sku or "",
                "revenue": round(net, 2),
                "units": net_units,
                "avg_price": round(_f(r.avg_price), 2),
            })
        return results

    def _ga4_product_ids(self, brand: str) -> List[str]:
        """Build broad set of GA4 item_id candidates for a brand."""
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
            return []

        id_set = set()
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
                    id_set.add(vid)
                    if pid:
                        for prefix in country_prefixes:
                            id_set.add(f"{prefix}_{pid}_{vid}")

        # SKUs
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

        # Variant IDs from order items
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
                id_set.add(str(r[0]))

        # product_id × variant_id combos
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
            for prefix in country_prefixes:
                id_set.add(f"{prefix}_{pid}_{vid}")

        return list(id_set)

    # ── empty stubs (for partial-data resilience) ──────────────

    def _empty_pricing(self) -> Dict:
        return {
            "snapshot_date": None, "total_skus": 0,
            "below_minimum_count": 0, "below_minimum_pct": 0,
            "losing_money_count": 0, "losing_money_pct": 0,
            "price_index_vs_cheapest": None,
            "avg_margin_pct": None, "min_margin_pct": None, "max_margin_pct": None,
            "competitive_pressure_score": 0,
        }

    def _empty_ads(self) -> Dict:
        return {
            "cur_spend": 0, "prev_spend": 0, "spend_change_pct": None,
            "cur_roas": 0, "prev_roas": 0, "roas_change_pct": None,
            "cur_conversions": 0, "prev_conversions": 0,
            "cur_clicks": 0, "prev_clicks": 0,
            "cur_impressions": 0, "prev_impressions": 0,
            "efficiency_current": 0, "efficiency_prior": 0,
            "ad_driven_revenue_delta": 0,
        }

    def _empty_funnel(self) -> Dict:
        return {
            "cur_views": 0, "cur_carts": 0, "cur_purchases": 0,
            "cur_revenue": 0,
            "cur_view_to_cart_pct": 0, "cur_cart_to_purchase_pct": 0,
            "cur_overall_conversion_pct": 0,
            "prev_views": 0, "prev_carts": 0, "prev_purchases": 0,
            "prev_view_to_cart_pct": 0, "prev_cart_to_purchase_pct": 0,
            "view_to_cart_change_pp": 0, "cart_to_purchase_change_pp": 0,
        }

    def _empty_fulfilment(self) -> Dict:
        return {
            "cur_refund_rate_pct": 0, "prev_refund_rate_pct": 0,
            "refund_rate_change_pp": 0,
            "cur_refund_amount": 0, "prev_refund_amount": 0,
            "cur_cancellation_rate_pct": 0, "prev_cancellation_rate_pct": 0,
            "cancellation_rate_change_pp": 0,
            "cur_orders": 0, "prev_orders": 0,
        }

    def _empty_stock(self) -> Dict:
        return {
            "total_skus": 0, "oos_count": 0, "oos_rate": 0,
            "low_stock_count": 0,
            "gate_passed": False, "gate_reasons": [],
            "stock_weight": 0.01,
            "is_top_reason_eligible": False,
        }

    def _safe(self, fn, *args):
        try:
            return fn(*args)
        except Exception as e:
            log.debug(f"Diagnosis engine: {fn.__name__} failed for args={args[:1]}: {e}")
            return None
