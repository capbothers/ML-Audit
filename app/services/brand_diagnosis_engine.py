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
import json

from sqlalchemy.orm import Session
from sqlalchemy import func, case, literal_column, String, and_, or_

from app.models.shopify import (
    ShopifyOrderItem, ShopifyProduct, ShopifyInventory,
    ShopifyRefundLineItem,
)
from app.models.google_ads_data import GoogleAdsProductPerformance, GoogleAdsCampaign
from app.models.ga4_data import GA4ProductPerformance
from app.models.search_console_data import SearchConsoleQuery
from app.models.competitive_pricing import CompetitivePricing
from app.config import get_settings
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

        # ── weekly trends ──
        weekly_trends = self._safe(self._weekly_trends, brand, cur_end)

        # ── performance decomposition (the core) ──
        decomposition = self._decompose(
            brand, cur_totals, yoy_totals, cur_map, yoy_map,
            ads_diag, demand_diag, funnel_diag, fulfilment_diag,
            cur_end=cur_end,
        )

        # ── anomaly detection ──
        anomalies = self._detect_anomalies(
            cur_totals, yoy_totals, pricing_diag, ads_diag,
            demand_diag, funnel_diag, fulfilment_diag,
            weekly_trends=weekly_trends,
        )

        # ── momentum score ──
        momentum = self._momentum_score(
            cur_totals, yoy_totals, demand_diag, ads_diag, funnel_diag,
            weekly_trends=weekly_trends,
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
            "weekly_trends": weekly_trends,
        }

    # ── performance decomposition ──────────────────────────────

    def _decompose(self, brand, cur, yoy, cur_map, yoy_map,
                   ads_diag, demand_diag, funnel_diag, fulfilment_diag,
                   cur_end=None) -> Dict:
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

        # Classify lost products: structural vs statistical noise
        variance_info = self._classify_product_variance(brand, lost_ids, yoy_map, cur_end)

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
                "structural_mix_dollars": round(new_rev - variance_info["structural_dollars"], 2),
                "variance_mix_dollars": round(-variance_info["variance_dollars"], 2),
                "structural_products": len(variance_info["structural_ids"]),
                "variance_products": len(variance_info["variance_ids"]),
                "confidence": self._product_mix_confidence(variance_info, product_mix_effect),
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

        # Promote product_mix confidence if it's a top-2 driver by dollar impact.
        # A top driver shouldn't be labeled "low" even if most lost products are
        # low-volume — the aggregate impact is still the primary explanation.
        ranked = sorted(contributions.items(),
                        key=lambda kv: abs(kv[1]["dollars"]), reverse=True)
        top2_names = {kv[0] for kv in ranked[:2]}
        if "product_mix" in top2_names and contributions["product_mix"]["confidence"] == "low":
            contributions["product_mix"]["confidence"] = "medium"

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
        score = min(1.0, (spend_chg / 50) * 0.4 + (roas_chg / 50) * 0.4)
        # Boost if impression share data shows constraint
        budget_lost = diag.get("cur_budget_lost") or 0
        rank_lost = diag.get("cur_rank_lost") or 0
        if budget_lost > 10 or rank_lost > 10:
            score = min(1.0, score + 0.2)
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

    # ── product mix variance detection ─────────────────────────

    def _classify_product_variance(self, brand, lost_ids, yoy_map, cur_end) -> Dict:
        """
        For each 'lost' product, check trailing 12-month sales rate.
        If P(0 sales in 30 days) > 20%, classify as statistical noise.
        """
        import math
        NOISE_THRESHOLD = 0.20

        empty = {"structural_ids": set(), "variance_ids": set(),
                 "structural_dollars": 0.0, "variance_dollars": 0.0}

        if not lost_ids or cur_end is None:
            return empty

        trailing_start = cur_end - timedelta(days=365)
        trailing_rows = (
            self.db.query(
                ShopifyOrderItem.shopify_product_id,
                func.sum(ShopifyOrderItem.quantity).label("units"),
            )
            .filter(
                ShopifyOrderItem.vendor == brand,
                ShopifyOrderItem.shopify_product_id.in_(list(lost_ids)),
                ShopifyOrderItem.order_date >= trailing_start,
                ShopifyOrderItem.order_date < cur_end,
                ShopifyOrderItem.financial_status.in_(
                    ["paid", "partially_refunded", "refunded"]
                ),
            )
            .group_by(ShopifyOrderItem.shopify_product_id)
            .all()
        )

        trailing_map = {r.shopify_product_id: int(r.units or 0) for r in trailing_rows}

        structural_ids = set()
        variance_ids = set()
        structural_dollars = 0.0
        variance_dollars = 0.0

        for pid in lost_ids:
            units_12m = trailing_map.get(pid, 0)
            monthly_rate = units_12m / 12.0
            p_zero = math.exp(-monthly_rate) if monthly_rate > 0 else 1.0
            lost_rev = yoy_map[pid]["revenue"] if pid in yoy_map else 0

            if p_zero > NOISE_THRESHOLD:
                variance_ids.add(pid)
                variance_dollars += lost_rev
            else:
                structural_ids.add(pid)
                structural_dollars += lost_rev

        return {
            "structural_ids": structural_ids,
            "variance_ids": variance_ids,
            "structural_dollars": round(structural_dollars, 2),
            "variance_dollars": round(variance_dollars, 2),
        }

    def _product_mix_confidence(self, variance_info, product_mix_effect) -> str:
        """Low confidence when most dollar impact comes from noise products."""
        if abs(product_mix_effect) < 1:
            return "low"
        noise_pct = abs(variance_info["variance_dollars"]) / abs(product_mix_effect) if abs(product_mix_effect) > 0 else 0
        if noise_pct > 0.6:
            return "low"
        elif noise_pct > 0.3:
            return "medium"
        return "high"

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
            "scope": f"{brand} inventory",
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

    def _ads_diagnostic(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        """Campaign-level ads diagnostic using name-based brand matching."""
        import re as _re

        include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
        brand_norm = (brand or "").strip().lower()
        if not brand_norm:
            return None

        # Build include/exclude patterns for campaign name matching.
        # Campaign names typically contain the brand name (e.g. "PM-AU Zip Taps"),
        # so ALWAYS include a brand-name pattern regardless of allowlist config.
        brand_pattern = _re.compile(r"\b" + _re.escape(brand_norm) + r"\b", _re.IGNORECASE)
        include_patterns = [
            _re.compile(r"\b" + _re.escape(t) + r"\b", _re.IGNORECASE)
            for t in include_terms if t
        ] if allowlist_used else []
        exclude_patterns = [
            _re.compile(r"\b" + _re.escape(t) + r"\b", _re.IGNORECASE)
            for t in exclude_terms if t
        ]

        def _matches_brand(campaign_name):
            name = (campaign_name or "").strip()
            if not name:
                return False
            if exclude_patterns and any(p.search(name) for p in exclude_patterns):
                return False
            # Match if brand name appears in campaign name
            if brand_pattern.search(name):
                return True
            # Also match via allowlist terms
            if include_patterns and any(p.search(name) for p in include_patterns):
                return True
            return False

        def _fetch_and_aggregate(start, end):
            rows = (
                self.db.query(
                    GoogleAdsCampaign.campaign_id,
                    GoogleAdsCampaign.campaign_name,
                    GoogleAdsCampaign.date,
                    GoogleAdsCampaign.impressions,
                    GoogleAdsCampaign.clicks,
                    GoogleAdsCampaign.cost_micros,
                    GoogleAdsCampaign.conversions,
                    GoogleAdsCampaign.conversions_value,
                    GoogleAdsCampaign.search_impression_share,
                    GoogleAdsCampaign.search_budget_lost_impression_share,
                    GoogleAdsCampaign.search_rank_lost_impression_share,
                )
                .filter(
                    GoogleAdsCampaign.date >= (start.date() if hasattr(start, "date") else start),
                    GoogleAdsCampaign.date < (end.date() if hasattr(end, "date") else end),
                )
                .all()
            )

            total_impr = 0
            total_clicks = 0
            total_cost_micros = 0
            total_conv = 0.0
            total_conv_val = 0.0
            imp_share_num = 0.0
            budget_lost_num = 0.0
            rank_lost_num = 0.0
            campaign_ids = set()
            # Per-campaign tracking for breakdown
            camp_stats = {}  # campaign_name → {spend_micros, conv_val, clicks, last_date}

            for r in rows:
                if not _matches_brand(r.campaign_name):
                    continue
                impr = int(r.impressions or 0)
                total_impr += impr
                total_clicks += int(r.clicks or 0)
                total_cost_micros += int(r.cost_micros or 0)
                total_conv += _f(r.conversions)
                total_conv_val += _f(r.conversions_value)
                campaign_ids.add(r.campaign_id)
                # Accumulate per-campaign
                cname = (r.campaign_name or "").strip()
                if cname:
                    cs = camp_stats.setdefault(cname, {"spend_micros": 0, "conv_val": 0.0, "clicks": 0, "last_date": None})
                    cs["spend_micros"] += int(r.cost_micros or 0)
                    cs["conv_val"] += _f(r.conversions_value)
                    cs["clicks"] += int(r.clicks or 0)
                    row_date = r.date
                    if row_date and (cs["last_date"] is None or row_date > cs["last_date"]):
                        cs["last_date"] = row_date
                if r.search_impression_share is not None:
                    v = _f(r.search_impression_share)
                    if 0 < v <= 1:
                        v *= 100
                    imp_share_num += v * impr
                if r.search_budget_lost_impression_share is not None:
                    v = _f(r.search_budget_lost_impression_share)
                    if 0 < v <= 1:
                        v *= 100
                    budget_lost_num += v * impr
                if r.search_rank_lost_impression_share is not None:
                    v = _f(r.search_rank_lost_impression_share)
                    if 0 < v <= 1:
                        v *= 100
                    rank_lost_num += v * impr

            spend = total_cost_micros / 1_000_000
            roas = total_conv_val / spend if spend > 0 else 0

            def _share(num):
                if total_impr == 0 or num == 0:
                    return None
                return round(num / total_impr, 1)

            # Build per-campaign breakdown (top 5 by spend)
            # Mark campaigns as "paused" if their last activity is before
            # the latest data date across all matched campaigns in this period.
            all_last_dates = [cs["last_date"] for cs in camp_stats.values() if cs["last_date"]]
            latest_data_date = max(all_last_dates) if all_last_dates else None

            camp_list = []
            for cname, cs in camp_stats.items():
                cs_spend = cs["spend_micros"] / 1_000_000
                cs_roas = cs["conv_val"] / cs_spend if cs_spend > 0 else 0
                ld = cs["last_date"]
                if ld and latest_data_date and ld < latest_data_date:
                    status = "paused"
                else:
                    status = "active"
                camp_list.append({
                    "name": cname, "spend": round(cs_spend, 2),
                    "roas": round(cs_roas, 2), "clicks": cs["clicks"],
                    "last_date": str(ld) if ld else None,
                    "status": status,
                })
            camp_list.sort(key=lambda x: x["spend"], reverse=True)

            return {
                "spend": spend, "conv": total_conv, "conv_val": total_conv_val,
                "clicks": total_clicks, "impr": total_impr, "roas": roas,
                "imp_share": _share(imp_share_num),
                "budget_lost": _share(budget_lost_num),
                "rank_lost": _share(rank_lost_num),
                "campaign_ids": campaign_ids,
                "campaigns": camp_list[:5],
            }

        cur = _fetch_and_aggregate(cur_start, cur_end)
        prev = _fetch_and_aggregate(yoy_start, yoy_end)

        if cur["spend"] == 0 and prev["spend"] == 0:
            return None

        new_campaign_ids = cur["campaign_ids"] - prev["campaign_ids"]
        lost_campaign_ids = prev["campaign_ids"] - cur["campaign_ids"]

        spend_chg_pct = _pct(cur["spend"], prev["spend"])
        roas_chg_pct = _pct(cur["roas"], prev["roas"])

        cur_eff = cur["conv_val"] / cur["spend"] if cur["spend"] > 0 else 0
        prev_eff = prev["conv_val"] / prev["spend"] if prev["spend"] > 0 else 0

        return {
            "metric_scope": "campaign-level YoY",
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
            "cur_imp_share": cur["imp_share"],
            "prev_imp_share": prev["imp_share"],
            "cur_budget_lost": cur["budget_lost"],
            "cur_rank_lost": cur["rank_lost"],
            "efficiency_current": round(cur_eff, 3),
            "efficiency_prior": round(prev_eff, 3),
            "ad_driven_revenue_delta": round(cur["conv_val"] - prev["conv_val"], 2),
            "new_campaigns": len(new_campaign_ids),
            "lost_campaigns": len(lost_campaign_ids),
            "top_campaigns": cur.get("campaigns", []),
        }

    def _demand_diagnostic(self, brand, cur_start, cur_end, yoy_start, yoy_end) -> Optional[Dict]:
        include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
        brand_norm = (brand or "").strip().lower()
        brand_clause = SearchConsoleQuery.query.ilike(f"%{brand}%") if brand else None
        exact_brand = func.lower(SearchConsoleQuery.query) == brand_norm if brand_norm else None

        def _gsc(start, end):
            term_clauses = [SearchConsoleQuery.query.ilike(f"%{t}%") for t in include_terms]
            if allowlist_used and brand_clause is not None and term_clauses:
                include_expr = or_(exact_brand, and_(brand_clause, or_(*term_clauses)))
            elif brand_clause is not None:
                include_expr = or_(exact_brand, brand_clause)
            else:
                include_expr = exact_brand
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

    def _detect_anomalies(self, cur, yoy, pricing, ads, demand, funnel, fulfilment,
                          weekly_trends=None) -> Dict:
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
                prev_r = ads.get("prev_roas", 0)
                cur_r = ads.get("cur_roas", 0)
                anomalies.append({
                    "signal": "ads_roas",
                    "value": roas_chg,
                    "threshold": 50,
                    "description": (
                        f"ROAS {'surged' if roas_chg > 0 else 'collapsed'} "
                        f"{abs(roas_chg):.1f}% YoY ({prev_r:.1f}x → {cur_r:.1f}x)"
                    ),
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

        # Trend-based anomalies
        if weekly_trends and weekly_trends.get("trends"):
            trends = weekly_trends["trends"]
            if trends.get("ads_roas") in ("accelerating_decline", "declining"):
                roas_vals = [w["roas"] for w in (weekly_trends.get("ads") or []) if w.get("roas") is not None]
                recent = roas_vals[-4:] if len(roas_vals) >= 4 else roas_vals
                if recent:
                    anomalies.append({
                        "signal": "ads_roas_trend",
                        "value": recent[-1] if recent else None,
                        "threshold": None,
                        "description": f"ROAS declining week-over-week: {' → '.join(str(r) for r in recent)}",
                    })
            if trends.get("revenue") == "accelerating_decline":
                anomalies.append({
                    "signal": "revenue_trend",
                    "value": None,
                    "threshold": None,
                    "description": "Revenue in accelerating week-over-week decline",
                })

        return {
            "count": len(anomalies),
            "items": anomalies,
        }

    # ── weekly trend analysis ──────────────────────────────────

    def _weekly_trends(self, brand, cur_end, num_weeks=8) -> Dict:
        """Compute week-over-week trends for revenue, ads, and search."""
        import re as _re

        weeks = []
        for i in range(num_weeks):
            week_end = cur_end - timedelta(weeks=i)
            week_start = week_end - timedelta(days=7)
            weeks.append((week_start, week_end))
        weeks.reverse()  # oldest first

        # ── Revenue + orders per week ──
        revenue_weeks = []
        for ws, we in weeks:
            row = (
                self.db.query(
                    func.sum(ShopifyOrderItem.total_price).label("revenue"),
                    func.sum(ShopifyOrderItem.quantity).label("units"),
                    func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label("orders"),
                )
                .filter(
                    ShopifyOrderItem.vendor == brand,
                    ShopifyOrderItem.order_date >= ws,
                    ShopifyOrderItem.order_date < we,
                    ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded"]),
                )
                .first()
            )
            revenue_weeks.append({
                "week_start": ws.strftime("%Y-%m-%d"),
                "revenue": round(_f(row.revenue), 2) if row else 0,
                "units": int(row.units or 0) if row else 0,
                "orders": int(row.orders or 0) if row else 0,
            })

        # ── Ads ROAS + spend per week (campaign-level, one bulk query) ──
        include_terms, exclude_terms, allowlist_used = self._get_brand_term_filters(brand)
        brand_norm = (brand or "").strip().lower()
        brand_pat = _re.compile(r"\b" + _re.escape(brand_norm) + r"\b", _re.IGNORECASE)
        al_patterns = [
            _re.compile(r"\b" + _re.escape(t) + r"\b", _re.IGNORECASE)
            for t in include_terms if t
        ] if allowlist_used else []
        ex_patterns = [
            _re.compile(r"\b" + _re.escape(t) + r"\b", _re.IGNORECASE)
            for t in exclude_terms if t
        ]

        def _camp_matches(name):
            name = (name or "").strip()
            if not name:
                return False
            if ex_patterns and any(p.search(name) for p in ex_patterns):
                return False
            if brand_pat.search(name):
                return True
            if al_patterns and any(p.search(name) for p in al_patterns):
                return True
            return False

        span_start = weeks[0][0]
        span_end = weeks[-1][1]
        all_camp_rows = (
            self.db.query(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.date,
                GoogleAdsCampaign.cost_micros,
                GoogleAdsCampaign.conversions_value,
            )
            .filter(
                GoogleAdsCampaign.date >= (span_start.date() if hasattr(span_start, "date") else span_start),
                GoogleAdsCampaign.date < (span_end.date() if hasattr(span_end, "date") else span_end),
            )
            .all()
        )
        brand_camp_rows = [r for r in all_camp_rows if _camp_matches(r.campaign_name)]

        ads_weeks = []
        for ws, we in weeks:
            ws_d = ws.date() if hasattr(ws, "date") else ws
            we_d = we.date() if hasattr(we, "date") else we
            period_rows = [r for r in brand_camp_rows if ws_d <= r.date < we_d]
            spend = sum(int(r.cost_micros or 0) for r in period_rows) / 1_000_000
            conv_val = sum(_f(r.conversions_value) for r in period_rows)
            roas = round(conv_val / spend, 2) if spend > 0 else None
            ads_weeks.append({
                "week_start": ws.strftime("%Y-%m-%d"),
                "spend": round(spend, 2),
                "roas": roas,
            })

        # ── Branded search clicks per week ──
        brand_clause = SearchConsoleQuery.query.ilike(f"%{brand}%") if brand else None
        exact_brand = func.lower(SearchConsoleQuery.query) == brand_norm if brand_norm else None

        search_weeks = []
        for ws, we in weeks:
            term_clauses = [SearchConsoleQuery.query.ilike(f"%{t}%") for t in include_terms]
            if allowlist_used and brand_clause is not None and term_clauses:
                include_expr = or_(exact_brand, and_(brand_clause, or_(*term_clauses)))
            elif brand_clause is not None:
                include_expr = or_(exact_brand, brand_clause)
            else:
                include_expr = exact_brand

            q = (
                self.db.query(func.sum(SearchConsoleQuery.clicks).label("clicks"))
                .filter(
                    include_expr,
                    SearchConsoleQuery.date >= (ws.date() if hasattr(ws, "date") else ws),
                    SearchConsoleQuery.date < (we.date() if hasattr(we, "date") else we),
                )
            )
            for t in exclude_terms:
                q = q.filter(~SearchConsoleQuery.query.ilike(f"%{t}%"))
            row = q.first()
            search_weeks.append({
                "week_start": ws.strftime("%Y-%m-%d"),
                "clicks": int(row.clicks or 0) if row and row.clicks else 0,
            })

        # ── Trend classification ──
        rev_trend = self._classify_trend([w["revenue"] for w in revenue_weeks])
        ads_roas_values = [w["roas"] for w in ads_weeks if w["roas"] is not None]
        ads_trend = self._classify_trend(ads_roas_values) if len(ads_roas_values) >= 4 else "insufficient_data"
        search_trend = self._classify_trend([w["clicks"] for w in search_weeks])

        return {
            "num_weeks": num_weeks,
            "revenue": revenue_weeks,
            "ads": ads_weeks,
            "ads_metric_scope": "campaign-level weekly",
            "search": search_weeks,
            "trends": {
                "revenue": rev_trend,
                "ads_roas": ads_trend,
                "search_clicks": search_trend,
            },
        }

    def _classify_trend(self, values) -> str:
        """Classify a series of weekly values into a trend direction."""
        if len(values) < 4:
            return "insufficient_data"

        changes = []
        for i in range(1, len(values)):
            prev_val = values[i - 1]
            curr_val = values[i]
            if prev_val and prev_val > 0:
                changes.append((curr_val - prev_val) / prev_val)
            else:
                changes.append(0)

        recent = changes[-4:]
        neg_count = sum(1 for c in recent if c < -0.05)
        pos_count = sum(1 for c in recent if c > 0.05)

        if neg_count >= 3:
            if len(recent) >= 2 and recent[-1] < recent[-2]:
                return "accelerating_decline"
            return "declining"
        elif pos_count >= 3:
            earlier = changes[:-4] if len(changes) > 4 else []
            if any(c < -0.05 for c in earlier):
                return "recovering"
            return "accelerating_growth"
        elif neg_count >= 2 and pos_count >= 1:
            return "stabilizing"
        elif abs(sum(recent)) < 0.1:
            return "flat"
        return "mixed"

    # ── momentum score ─────────────────────────────────────────

    def _momentum_score(self, cur, yoy, demand, ads, funnel,
                        weekly_trends=None) -> Dict:
        """
        Forward-looking 0-100 composite score.

        Weights:
          - Revenue trajectory: 25%
          - Demand trend: 20%
          - Ads efficiency trend: 15%
          - Conversion trend: 20%
          - Weekly momentum: 20%
        """
        score = 50.0  # neutral baseline
        components = {}

        # Revenue trajectory (25 pts)
        rev_pct = _pct(cur["revenue"], yoy["revenue"])
        if rev_pct is not None:
            rev_score = max(-25, min(25, rev_pct * 0.5))
        else:
            rev_score = 0
        components["revenue_trajectory"] = round(rev_score, 2)
        score += rev_score

        # Demand trend (20 pts)
        if demand:
            clicks_chg = demand.get("clicks_yoy_pct") or 0
            demand_score = max(-20, min(20, clicks_chg * 0.4))
        else:
            demand_score = 0
        components["demand_trend"] = round(demand_score, 2)
        score += demand_score

        # Ads efficiency (15 pts)
        if ads:
            roas_chg = ads.get("roas_change_pct") or 0
            ads_score = max(-15, min(15, roas_chg * 0.25))
        else:
            ads_score = 0
        components["ads_efficiency"] = round(ads_score, 2)
        score += ads_score

        # Conversion trend (20 pts)
        if funnel:
            v2c_chg = funnel.get("view_to_cart_change_pp") or 0
            c2p_chg = funnel.get("cart_to_purchase_change_pp") or 0
            conv_score = max(-20, min(20, (v2c_chg * 1.5 + c2p_chg * 2.5)))
        else:
            conv_score = 0
        components["conversion_trend"] = round(conv_score, 2)
        score += conv_score

        # Weekly momentum (20 pts)
        weekly_score = 0
        if weekly_trends and weekly_trends.get("trends"):
            trends = weekly_trends["trends"]
            trend_scores = {
                "accelerating_decline": -20, "declining": -15,
                "stabilizing": -5, "flat": 0, "mixed": 0,
                "recovering": 10, "accelerating_growth": 20,
                "insufficient_data": 0,
            }
            rev_t = trend_scores.get(trends.get("revenue", ""), 0) * 0.5
            ads_t = trend_scores.get(trends.get("ads_roas", ""), 0) * 0.3
            search_t = trend_scores.get(trends.get("search_clicks", ""), 0) * 0.2
            weekly_score = rev_t + ads_t + search_t
        components["weekly_momentum"] = round(weekly_score, 2)
        score += weekly_score

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
            "cur_imp_share": None, "prev_imp_share": None,
            "cur_budget_lost": None, "cur_rank_lost": None,
            "efficiency_current": 0, "efficiency_prior": 0,
            "ad_driven_revenue_delta": 0,
            "new_campaigns": 0, "lost_campaigns": 0,
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
