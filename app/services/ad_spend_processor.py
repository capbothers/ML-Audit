"""
Ad Spend Processing Pipeline

Transforms raw google_ads_campaigns daily data into:
- campaign_performance (aggregated metrics with derived indicators)
- ad_waste (detected waste patterns)
- ad_spend_optimizations (budget reallocation recommendations)

Designed to be idempotent: re-running overwrites previous results for
the same period without creating duplicates.
"""
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.models.google_ads_data import GoogleAdsCampaign, GoogleAdsProductPerformance
from app.services.campaign_strategy import classify, score as strategy_score, decide as strategy_decide, STRATEGY_THRESHOLDS
from app.models.ad_spend import (
    CampaignPerformance,
    AdWaste,
    AdSpendOptimization,
    ProductAdPerformance,
)
from app.models.product_cost import ProductCost
from app.models.product import Product
from app.services.shopify_revenue_attribution import ShopifyRevenueAttributionService
from app.services.finance_service import FinanceService

logger = logging.getLogger(__name__)


class AdSpendProcessor:
    """
    Processes raw Google Ads campaign data into derived analytics tables.

    Usage:
        processor = AdSpendProcessor(db)
        result = processor.process(days=30)
    """

    # Thresholds (matching AdSpendService)
    PROFITABLE_ROAS = 2.0
    HIGH_PERFORMER_ROAS = 3.0
    SCALING_OPPORTUNITY_ROAS = 3.5
    WASTE_ROAS = 1.5
    BUDGET_CAPPED_IMPRESSION_LOSS = 10.0  # 0-100 scale

    ZERO_CONV_MIN_SPEND = 50.0
    LOW_ROAS_THRESHOLD = 1.0

    def __init__(self, db: Session):
        self.db = db

    def process(self, days: int = 30) -> Dict:
        """Main pipeline entry point. Idempotent."""
        logger.info(f"AdSpendProcessor: starting for last {days} days")
        start_time = datetime.utcnow()

        # Anchor to latest ads data row — never fall back to calendar today
        max_date = self.db.query(func.max(GoogleAdsCampaign.date)).scalar()
        if max_date is None:
            logger.warning("No Google Ads data found — skipping processing")
            return {
                "campaigns_processed": 0,
                "campaign_performance": {"upserted": 0, "created": 0, "updated": 0},
                "waste_detected": {"created": 0, "cleared": 0},
                "optimizations_generated": {"created": 0, "cleared": 0},
                "product_performance": {"processed": 0, "profitable": 0, "losing_money": 0},
                "period": {"start": None, "end": None, "days": days},
                "message": "No Google Ads data in database",
                "duration_seconds": round((datetime.utcnow() - start_time).total_seconds(), 2),
            }
        period_end = max_date
        period_start = period_end - timedelta(days=days - 1)

        # Step 1: Aggregate
        aggregated = self._aggregate_campaigns(period_start, period_end)
        logger.info(f"  Aggregated {len(aggregated)} campaigns")

        if not aggregated:
            return {
                "campaigns_processed": 0,
                "campaign_performance": {"upserted": 0, "created": 0, "updated": 0},
                "waste_detected": {"created": 0, "cleared": 0},
                "optimizations_generated": {"created": 0, "cleared": 0},
                "product_performance": {"processed": 0, "profitable": 0, "losing_money": 0},
                "period": {"start": period_start.isoformat(), "end": period_end.isoformat(), "days": days},
                "message": "No campaign data found for the specified period",
                "duration_seconds": round((datetime.utcnow() - start_time).total_seconds(), 2),
            }

        # Step 1.5: Shopify revenue attribution
        attribution_service = ShopifyRevenueAttributionService(self.db)
        campaign_ids = [agg["campaign_id"] for agg in aggregated]
        shopify_data = attribution_service.get_campaign_revenue(
            campaign_ids, period_start, period_end
        )

        # Step 1.6: Get overhead per order from finance service
        finance_service = FinanceService(self.db)
        overhead_per_order = finance_service.get_latest_overhead_per_order()

        # Step 2: Derive metrics (with Shopify data where available)
        performance_rows = self._build_performance_rows(
            aggregated, period_start, period_end, days, shopify_data, overhead_per_order
        )

        # Step 3: Upsert campaign_performance
        perf_counts = self._upsert_campaign_performance(performance_rows)

        # Step 4: Detect waste
        waste_counts = self._detect_waste(performance_rows, days)

        # Step 5: Generate optimizations
        opt_counts = self._generate_optimizations(performance_rows)

        # Step 6: Process product performance
        product_counts = self._process_product_performance(period_start, period_end, days)

        duration = (datetime.utcnow() - start_time).total_seconds()

        attribution_stats = {
            "campaigns_with_shopify_data": sum(1 for v in shopify_data.values() if v is not None),
            "campaigns_using_google_fallback": sum(1 for v in shopify_data.values() if v is None),
            "total_shopify_orders_attributed": sum(
                v["order_count"] for v in shopify_data.values() if v is not None
            ),
        }

        result = {
            "campaigns_processed": len(aggregated),
            "campaign_performance": perf_counts,
            "waste_detected": waste_counts,
            "optimizations_generated": opt_counts,
            "product_performance": product_counts,
            "attribution": attribution_stats,
            "period": {"start": period_start.isoformat(), "end": period_end.isoformat(), "days": days},
            "duration_seconds": round(duration, 2),
        }

        logger.info(
            f"AdSpendProcessor: done in {duration:.1f}s — "
            f"{perf_counts['upserted']} campaigns, "
            f"{waste_counts['created']} waste, "
            f"{opt_counts['created']} optimizations"
        )
        return result

    # ── Step 1: Aggregate ────────────────────────────────────────────

    def _aggregate_campaigns(self, period_start: date, period_end: date) -> List[Dict]:
        rows = (
            self.db.query(
                GoogleAdsCampaign.campaign_id,
                func.max(GoogleAdsCampaign.campaign_name).label("campaign_name"),
                func.max(GoogleAdsCampaign.campaign_type).label("campaign_type"),
                func.sum(GoogleAdsCampaign.impressions).label("sum_impressions"),
                func.sum(GoogleAdsCampaign.clicks).label("sum_clicks"),
                func.sum(GoogleAdsCampaign.cost_micros).label("sum_cost_micros"),
                func.sum(GoogleAdsCampaign.conversions).label("sum_conversions"),
                func.sum(GoogleAdsCampaign.conversions_value).label("sum_conversions_value"),
                func.avg(GoogleAdsCampaign.search_budget_lost_impression_share).label("avg_budget_lost_is"),
                func.min(GoogleAdsCampaign.date).label("min_date"),
                func.max(GoogleAdsCampaign.date).label("max_date"),
                func.count(GoogleAdsCampaign.id).label("row_count"),
            )
            .filter(
                GoogleAdsCampaign.date >= period_start,
                GoogleAdsCampaign.date <= period_end,
            )
            .group_by(GoogleAdsCampaign.campaign_id)
            .all()
        )

        latest_statuses = self._get_latest_campaign_statuses(period_start, period_end)

        return [
            {
                "campaign_id": r.campaign_id,
                "campaign_name": r.campaign_name,
                "campaign_type": r.campaign_type,
                "campaign_status": latest_statuses.get(r.campaign_id, "UNKNOWN"),
                "sum_impressions": int(r.sum_impressions or 0),
                "sum_clicks": int(r.sum_clicks or 0),
                "sum_cost_micros": int(r.sum_cost_micros or 0),
                "sum_conversions": float(r.sum_conversions or 0),
                "sum_conversions_value": float(r.sum_conversions_value or 0),
                "avg_budget_lost_is": float(r.avg_budget_lost_is) if r.avg_budget_lost_is is not None else None,
                "min_date": r.min_date,
                "max_date": r.max_date,
                "row_count": int(r.row_count),
            }
            for r in rows
        ]

    def _get_latest_campaign_statuses(self, period_start: date, period_end: date) -> Dict[str, str]:
        max_date_sub = (
            self.db.query(
                GoogleAdsCampaign.campaign_id,
                func.max(GoogleAdsCampaign.date).label("max_date"),
            )
            .filter(
                GoogleAdsCampaign.date >= period_start,
                GoogleAdsCampaign.date <= period_end,
            )
            .group_by(GoogleAdsCampaign.campaign_id)
            .subquery()
        )

        rows = (
            self.db.query(
                GoogleAdsCampaign.campaign_id,
                GoogleAdsCampaign.campaign_status,
            )
            .join(
                max_date_sub,
                and_(
                    GoogleAdsCampaign.campaign_id == max_date_sub.c.campaign_id,
                    GoogleAdsCampaign.date == max_date_sub.c.max_date,
                ),
            )
            .all()
        )

        return {r.campaign_id: (r.campaign_status or "UNKNOWN") for r in rows}

    # ── Step 2: Derive metrics ───────────────────────────────────────

    def _build_performance_rows(
        self,
        aggregated: List[Dict],
        period_start: date,
        period_end: date,
        days: int,
        shopify_data: Optional[Dict] = None,
        overhead_per_order: Optional[Decimal] = None,
    ) -> List[Dict]:
        results = []
        shopify_data = shopify_data or {}

        for agg in aggregated:
            total_spend = agg["sum_cost_micros"] / 1_000_000.0
            total_clicks = agg["sum_clicks"]
            total_impressions = agg["sum_impressions"]
            google_conversions = int(agg["sum_conversions"])
            google_conv_value = agg["sum_conversions_value"]

            # Safe division
            avg_cpc = (total_spend / total_clicks) if total_clicks > 0 else None
            ctr = (total_clicks / total_impressions) if total_impressions > 0 else None
            google_roas = (google_conv_value / total_spend) if total_spend > 0 else None

            # Budget analysis (avg_budget_lost_is is 0-100 scale)
            avg_lost_is = agg["avg_budget_lost_is"]
            budget_capped = avg_lost_is is not None and avg_lost_is > self.BUDGET_CAPPED_IMPRESSION_LOSS
            lost_is_fraction = avg_lost_is / 100.0 if avg_lost_is is not None else None

            avg_daily_spend = total_spend / days if days > 0 else 0

            # ── Shopify attribution (use real data if available) ──
            shopify = shopify_data.get(agg["campaign_id"])

            if shopify is not None:
                actual_conversions = shopify["order_count"]
                actual_revenue = Decimal(str(round(float(shopify["revenue"]), 2)))
                actual_product_costs = Decimal(str(round(float(shopify["product_costs"]), 2)))
                true_profit_val = float(shopify["revenue"]) - float(shopify["product_costs"]) - total_spend
                true_roas_val = (
                    (float(shopify["revenue"]) - float(shopify["product_costs"])) / total_spend
                    if total_spend > 0 else None
                )
                revenue_roas_val = float(shopify["revenue"]) / total_spend if total_spend > 0 else None
                products_advertised = shopify["products_advertised"]
                avg_product_margin = shopify["avg_product_margin"]
                unprofitable_products_count = shopify["unprofitable_products_count"]
            else:
                # Fallback to Google reported values
                actual_conversions = google_conversions
                actual_revenue = Decimal(str(round(google_conv_value, 2)))
                actual_product_costs = Decimal("0")
                true_profit_val = google_conv_value - total_spend
                true_roas_val = google_roas
                revenue_roas_val = google_roas
                products_advertised = 0
                avg_product_margin = None
                unprofitable_products_count = 0

            # Flags (based on true ROAS — Shopify when available)
            is_active = (agg["campaign_status"] or "").upper() == "ENABLED"
            is_profitable = true_roas_val is not None and true_roas_val >= self.PROFITABLE_ROAS
            is_high_performer = true_roas_val is not None and true_roas_val >= self.HIGH_PERFORMER_ROAS
            is_scaling_opportunity = (
                true_roas_val is not None
                and true_roas_val >= self.SCALING_OPPORTUNITY_ROAS
                and budget_capped
            )
            is_wasting_budget = total_spend > 0 and (true_roas_val is None or true_roas_val < self.WASTE_ROAS)

            # Waste reasons
            waste_reasons = []
            if total_spend > 0 and google_conversions == 0:
                waste_reasons.append("Zero conversions with active spend")
            if true_roas_val is not None and true_roas_val < 1.0:
                waste_reasons.append(f"ROAS below 1.0 ({true_roas_val:.2f})")
            if true_roas_val is not None and true_roas_val < self.WASTE_ROAS:
                waste_reasons.append(f"ROAS below waste threshold ({self.WASTE_ROAS})")

            # Estimated waste
            estimated_waste = 0.0
            if true_roas_val is not None and true_roas_val < 1.0 and total_spend > 0:
                estimated_waste = total_spend
            elif is_wasting_budget and true_roas_val is not None and total_spend > 0:
                actual_rev = float(actual_revenue)
                ideal_spend = actual_rev / self.PROFITABLE_ROAS if actual_rev > 0 else 0
                estimated_waste = max(0, total_spend - ideal_spend)

            # Recommended action
            action = self._determine_action(true_roas_val, is_scaling_opportunity, is_wasting_budget, total_spend, google_conversions)
            rec_budget = self._calc_recommended_budget(action, avg_daily_spend, true_roas_val)
            impact = self._estimate_impact(action, total_spend, true_roas_val, rec_budget, days)

            # ── Fully-loaded overhead allocation ──
            allocated_overhead = None
            fully_loaded_profit = None
            fully_loaded_roas = None
            is_profitable_fully_loaded = None

            if overhead_per_order is not None and actual_conversions > 0:
                overhead_f = float(overhead_per_order)
                allocated_overhead = Decimal(str(round(overhead_f * actual_conversions, 2)))
                fully_loaded_profit = Decimal(str(round(
                    true_profit_val - float(allocated_overhead), 2
                )))
                if total_spend > 0:
                    fully_loaded_roas = round(
                        (float(actual_revenue) - float(actual_product_costs) - float(allocated_overhead)) / total_spend,
                        4
                    )
                is_profitable_fully_loaded = float(fully_loaded_profit) > 0

            # ── Strategy-aware decision layer ──
            aov = float(actual_revenue) / actual_conversions if actual_conversions > 0 else None
            strat_type = classify(agg["campaign_name"], agg["campaign_type"], aov)
            strat_thresholds = STRATEGY_THRESHOLDS.get(strat_type, STRATEGY_THRESHOLDS['unknown'])

            strat_data = {
                'true_roas': true_roas_val,
                'cpa': float(total_spend) / actual_conversions if actual_conversions > 0 else None,
                'impression_share': float(avg_lost_is) if avg_lost_is else None,
                'fully_loaded_roas': fully_loaded_roas,
                'total_spend': total_spend,
                'days': days,
            }
            d_score = strategy_score(strat_data, strat_type, strat_thresholds)
            decision = strategy_decide(d_score, true_roas_val, strat_type, strat_thresholds, total_spend, days)

            # Guardrail: waste signal overrides scale
            if is_wasting_budget and decision['action'] in ('scale', 'scale_aggressively'):
                decision['action'] = 'investigate'

            # ── Attribution confidence (Capability 2) ──
            # Tier based on Shopify ground-truth match when available,
            # else fall back to Google conversion volume as a proxy.
            if shopify is not None and google_conversions > 0:
                conv_ratio = actual_conversions / google_conversions
                if conv_ratio >= 0.5:
                    attr_confidence = 'high'
                elif conv_ratio >= 0.2:
                    attr_confidence = 'medium'
                else:
                    attr_confidence = 'low'
                attr_gap_pct = round((1 - conv_ratio) * 100, 1)
            elif shopify is None and google_conversions >= 10:
                # No Shopify verification but sufficient Google volume —
                # Google attribution is unverified, not unreliable.
                attr_confidence = 'medium'
                attr_gap_pct = None
            elif shopify is None and google_conversions >= 3:
                attr_confidence = 'medium'
                attr_gap_pct = None
            else:
                # No Shopify AND very low Google volume — truly low confidence
                attr_confidence = 'low'
                attr_gap_pct = None

            # Attribution gate: low confidence cannot trigger hard cut
            if attr_confidence == 'low' and decision['action'] in ('reduce', 'pause'):
                decision['action'] = 'investigate'

            # ── Causal triage (Capability 1) — only for non-scaling campaigns ──
            primary_cause = None
            cause_confidence = None
            cause_evidence = None
            lp_cvr_change = None
            lp_bounce_change = None
            lp_is_friction = None

            if decision['action'] not in ('scale', 'scale_aggressively'):
                try:
                    from app.services.causal_triage import CausalTriageService
                    triage_svc = CausalTriageService(self.db)
                    triage = triage_svc.diagnose(
                        agg["campaign_id"], period_start, period_end,
                        google_conversions, actual_conversions,
                        campaign_name=agg["campaign_name"],
                    )
                    primary_cause = triage['primary_cause']
                    cause_confidence = triage['confidence']
                    cause_evidence = triage['causes']

                    # Extract LP metrics for storage
                    lp_cause = next(
                        (c for c in triage['causes'] if c['cause'] == 'landing_page'), None
                    )
                    if lp_cause:
                        lp_cvr_change = lp_cause.get('cvr_change')
                        lp_bounce_change = lp_cause.get('bounce_change')
                        lp_is_friction = lp_cause['score'] >= 0.7
                except Exception as e:
                    logger.warning(f"Triage failed for {agg['campaign_id']}: {e}")

            results.append({
                "campaign_id": agg["campaign_id"],
                "campaign_name": agg["campaign_name"],
                "campaign_type": (agg["campaign_type"] or "").lower() or None,
                "is_active": is_active,
                "budget_status": "limited" if budget_capped else "standard",
                "total_spend": Decimal(str(round(total_spend, 2))),
                "daily_budget": Decimal(str(round(avg_daily_spend, 2))),
                "avg_daily_spend": Decimal(str(round(avg_daily_spend, 2))),
                "budget_capped": budget_capped,
                "avg_cap_time": None,
                "lost_impression_share": lost_is_fraction,
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "avg_cpc": Decimal(str(round(avg_cpc, 2))) if avg_cpc is not None else None,
                "click_through_rate": round(ctr, 6) if ctr is not None else None,
                "avg_position": None,
                "google_conversions": google_conversions,
                "google_conversion_value": Decimal(str(round(google_conv_value, 2))),
                "google_roas": round(google_roas, 4) if google_roas is not None else None,
                "actual_conversions": actual_conversions,
                "actual_revenue": actual_revenue,
                "actual_product_costs": actual_product_costs,
                "true_profit": Decimal(str(round(true_profit_val, 2))),
                "true_roas": round(true_roas_val, 4) if true_roas_val is not None else None,
                "revenue_roas": round(revenue_roas_val, 4) if revenue_roas_val is not None else None,
                "is_profitable": is_profitable,
                "is_high_performer": is_high_performer,
                "is_scaling_opportunity": is_scaling_opportunity,
                "is_wasting_budget": is_wasting_budget,
                "waste_reasons": waste_reasons if waste_reasons else None,
                "estimated_waste": Decimal(str(round(estimated_waste, 2))),
                "products_advertised": products_advertised,
                "avg_product_margin": avg_product_margin,
                "unprofitable_products_count": unprofitable_products_count,
                "allocated_overhead": allocated_overhead,
                "fully_loaded_profit": fully_loaded_profit,
                "fully_loaded_roas": fully_loaded_roas,
                "is_profitable_fully_loaded": is_profitable_fully_loaded,
                "recommended_action": action,
                "recommended_budget": rec_budget,
                "expected_impact": impact,
                "strategy_type": strat_type,
                "decision_score": d_score,
                "short_term_status": decision['short_term'],
                "strategic_value": decision['strategic_value'],
                "strategy_action": decision['action'],
                "strategy_confidence": decision['confidence'],
                "primary_cause": primary_cause,
                "cause_confidence": cause_confidence,
                "cause_evidence": cause_evidence,
                "attribution_confidence": attr_confidence,
                "attribution_gap_pct": attr_gap_pct,
                "lp_cvr_change": lp_cvr_change,
                "lp_bounce_change": lp_bounce_change,
                "lp_is_friction": lp_is_friction,
                "period_start": datetime.combine(period_start, datetime.min.time()),
                "period_end": datetime.combine(period_end, datetime.min.time()),
                "period_days": days,
                "analyzed_at": datetime.utcnow(),
            })

        return results

    def _determine_action(self, true_roas, is_scaling, is_wasting, total_spend, conversions) -> str:
        if is_scaling:
            return "scale"
        if total_spend > self.ZERO_CONV_MIN_SPEND and conversions == 0:
            return "pause"
        if true_roas is not None and true_roas < 0.5:
            return "pause"
        if is_wasting:
            return "reduce"
        return "optimize"

    def _calc_recommended_budget(self, action, current_daily, true_roas) -> Optional[Decimal]:
        if action == "scale" and true_roas and true_roas >= self.SCALING_OPPORTUNITY_ROAS:
            return Decimal(str(round(current_daily * 1.5 * 30, 2)))
        elif action == "reduce":
            return Decimal(str(round(current_daily * 0.5 * 30, 2)))
        elif action == "pause":
            return Decimal("0")
        return None

    def _estimate_impact(self, action, total_spend, true_roas, rec_budget, days) -> Optional[Decimal]:
        if rec_budget is None or true_roas is None:
            return None
        monthly_current = (total_spend / days * 30) if days > 0 else 0
        monthly_new = float(rec_budget)
        delta = monthly_new - monthly_current
        if action == "scale":
            return Decimal(str(round(delta * (true_roas - 1), 2)))
        elif action in ("reduce", "pause"):
            if true_roas < 1.0:
                return Decimal(str(round(abs(delta) * (1 - true_roas), 2)))
            return Decimal(str(round(max(0, abs(delta) * (1 - true_roas / self.PROFITABLE_ROAS)), 2)))
        return None

    # ── Step 3: Upsert campaign_performance ──────────────────────────

    def _upsert_campaign_performance(self, performance_rows: List[Dict]) -> Dict:
        created = 0
        updated = 0

        for row in performance_rows:
            existing = self.db.query(CampaignPerformance).filter(
                CampaignPerformance.campaign_id == row["campaign_id"]
            ).first()

            if existing:
                for key, value in row.items():
                    if key != "campaign_id":
                        setattr(existing, key, value)
                existing.updated_at = datetime.utcnow()
                updated += 1
            else:
                self.db.add(CampaignPerformance(**row))
                created += 1

            if (created + updated) % 50 == 0:
                self.db.commit()

        self.db.commit()
        return {"upserted": created + updated, "created": created, "updated": updated}

    # ── Step 4: Detect waste ─────────────────────────────────────────

    def _detect_waste(self, performance_rows: List[Dict], days: int) -> Dict:
        # Clear existing active waste for idempotency
        cleared = self.db.query(AdWaste).filter(
            AdWaste.status == "active",
            AdWaste.period_days == days,
        ).delete()
        self.db.commit()

        created = 0

        for row in performance_rows:
            total_spend = float(row["total_spend"])
            convs = row["google_conversions"]
            roas = row["true_roas"]
            name = row["campaign_name"]
            cid = row["campaign_id"]

            # Pattern 1: Zero conversions with significant spend
            if total_spend >= self.ZERO_CONV_MIN_SPEND and convs < 1:
                monthly_waste = total_spend / days * 30 if days > 0 else 0
                self.db.add(AdWaste(
                    waste_type="no_conversion_keywords",
                    waste_description=(
                        f"Campaign '{name}' spent ${total_spend:,.2f} over {days} days "
                        f"with zero conversions"
                    ),
                    campaign_id=cid,
                    campaign_name=name,
                    monthly_waste_spend=Decimal(str(round(monthly_waste, 2))),
                    actual_conversion_rate=0.0,
                    severity="critical" if total_spend > 500 else "high",
                    monthly_impact=Decimal(str(round(monthly_waste, 2))),
                    recommended_action=(
                        f"Pause campaign '{name}' or review targeting. "
                        f"${total_spend:,.2f} spent with zero conversions."
                    ),
                    expected_savings=Decimal(str(round(monthly_waste, 2))),
                    implementation_difficulty="easy",
                    status="active",
                    period_days=days,
                    evidence={
                        "total_spend": round(total_spend, 2),
                        "conversions": convs,
                        "clicks": row["total_clicks"],
                        "period_days": days,
                    },
                    identified_at=datetime.utcnow(),
                ))
                created += 1

            # Pattern 2: ROAS < 1.0 with meaningful spend (losing money)
            elif (
                roas is not None
                and roas < self.LOW_ROAS_THRESHOLD
                and total_spend >= self.ZERO_CONV_MIN_SPEND
                and convs >= 1
            ):
                # Use actual_revenue consistently — same source as true_roas
                actual_rev = float(row["actual_revenue"])
                monthly_loss = (total_spend - actual_rev) / days * 30 if days > 0 else 0
                if monthly_loss <= 0:
                    continue  # Revenue exceeds spend — not truly wasting
                self.db.add(AdWaste(
                    waste_type="budget_fragmentation",
                    waste_description=(
                        f"Campaign '{name}' has true ROAS of {roas:.2f}x — "
                        f"spending ${total_spend:,.2f} to generate ${actual_rev:,.2f} revenue."
                    ),
                    campaign_id=cid,
                    campaign_name=name,
                    monthly_waste_spend=Decimal(str(round(monthly_loss, 2))),
                    actual_conversion_rate=(
                        convs / row["total_clicks"] if row["total_clicks"] > 0 else 0.0
                    ),
                    severity="high" if roas < 0.5 else "medium",
                    monthly_impact=Decimal(str(round(monthly_loss, 2))),
                    recommended_action=(
                        f"Reduce budget for '{name}' or restructure targeting. "
                        f"Current true ROAS {roas:.2f}x (need {self.PROFITABLE_ROAS}x to be profitable)."
                    ),
                    expected_savings=Decimal(str(round(monthly_loss * 0.7, 2))),
                    implementation_difficulty="medium",
                    status="active",
                    period_days=days,
                    evidence={
                        "total_spend": round(total_spend, 2),
                        "actual_revenue": round(actual_rev, 2),
                        "true_roas": round(roas, 2),
                        "conversions": convs,
                        "period_days": days,
                    },
                    identified_at=datetime.utcnow(),
                ))
                created += 1

        self.db.commit()
        return {"created": created, "cleared": cleared}

    # ── Step 5: Generate optimizations ───────────────────────────────

    def _generate_optimizations(self, performance_rows: List[Dict]) -> Dict:
        # Clear existing recommendations for idempotency
        cleared = self.db.query(AdSpendOptimization).filter(
            AdSpendOptimization.status == "recommended"
        ).delete()
        self.db.commit()

        # Sources: worst performers
        sources = sorted(
            [r for r in performance_rows if r["is_wasting_budget"] and float(r["total_spend"]) > 0],
            key=lambda r: r["true_roas"] or 0,
        )

        # Targets: best budget-capped performers
        targets = sorted(
            [r for r in performance_rows if r["is_scaling_opportunity"]],
            key=lambda r: r["true_roas"] or 0,
            reverse=True,
        )

        created = 0

        for i, target in enumerate(targets):
            if i >= len(sources):
                break

            source = sources[i]
            src_daily = float(source["avg_daily_spend"])
            tgt_daily = float(target["avg_daily_spend"])
            budget_to_move = Decimal(str(round(src_daily * 0.5 * 30, 2)))
            src_roas = source["true_roas"] or 0
            tgt_roas = target["true_roas"] or 0

            current_profit = float(source["true_profit"]) + float(target["true_profit"])
            projected_delta = float(budget_to_move) * (tgt_roas - src_roas)

            self.db.add(AdSpendOptimization(
                optimization_name=f"Reallocate from '{source['campaign_name']}' to '{target['campaign_name']}'",
                optimization_type="reallocation",
                source_campaign_id=source["campaign_id"],
                source_campaign_name=source["campaign_name"],
                current_source_budget=Decimal(str(round(src_daily * 30, 2))),
                recommended_source_budget=Decimal(str(round(src_daily * 0.5 * 30, 2))),
                budget_to_move=budget_to_move,
                target_campaign_id=target["campaign_id"],
                target_campaign_name=target["campaign_name"],
                current_target_budget=Decimal(str(round(tgt_daily * 30, 2))),
                recommended_target_budget=Decimal(str(round(tgt_daily * 30 + float(budget_to_move), 2))),
                budget_to_add=budget_to_move,
                current_total_spend=Decimal(str(round(float(source["total_spend"]) + float(target["total_spend"]), 2))),
                current_total_revenue=Decimal(str(round(float(source["actual_revenue"]) + float(target["actual_revenue"]), 2))),
                current_total_profit=Decimal(str(round(current_profit, 2))),
                projected_total_spend=Decimal(str(round(float(source["total_spend"]) + float(target["total_spend"]), 2))),
                projected_total_revenue=Decimal(str(round(
                    float(source["actual_revenue"]) + float(target["actual_revenue"]) + projected_delta, 2
                ))),
                projected_total_profit=Decimal(str(round(current_profit + projected_delta, 2))),
                revenue_impact=Decimal(str(round(projected_delta, 2))),
                profit_impact=Decimal(str(round(projected_delta, 2))),
                spend_change=Decimal("0"),
                confidence_level="medium" if tgt_roas >= 4.0 else "low",
                confidence_score=min(0.9, tgt_roas / 10) if tgt_roas else 0.3,
                rationale=(
                    f"Move ${float(budget_to_move):,.0f}/mo from "
                    f"'{source['campaign_name']}' (ROAS {src_roas:.1f}x) to "
                    f"'{target['campaign_name']}' (ROAS {tgt_roas:.1f}x). "
                    f"Expected +${projected_delta:,.0f}/mo."
                ),
                supporting_data={
                    "source_roas": round(src_roas, 2),
                    "target_roas": round(tgt_roas, 2),
                },
                priority="high" if projected_delta > 500 else "medium",
                impact_score=min(100, int(projected_delta / 10)) if projected_delta > 0 else 0,
                status="recommended",
                created_at=datetime.utcnow(),
            ))
            created += 1

        self.db.commit()
        return {"created": created, "cleared": cleared}

    # ── Step 6: Process product performance ──────────────────────────

    def _process_product_performance(self, period_start: date, period_end: date, days: int) -> Dict:
        """
        Aggregate product-level ad performance from google_ads_products,
        join with product costs, and upsert into product_ad_performance.

        Returns:
            {"processed": count, "profitable": profitable_count, "losing_money": losing_count}
        """
        logger.info("  Step 6: Processing product-level ad performance")

        # ── 1. Aggregate product data grouped by product_item_id ──
        rows = (
            self.db.query(
                GoogleAdsProductPerformance.product_item_id,
                func.sum(GoogleAdsProductPerformance.impressions).label("sum_impressions"),
                func.sum(GoogleAdsProductPerformance.clicks).label("sum_clicks"),
                func.sum(GoogleAdsProductPerformance.cost_micros).label("sum_cost_micros"),
                func.sum(GoogleAdsProductPerformance.conversions).label("sum_conversions"),
                func.sum(GoogleAdsProductPerformance.conversions_value).label("sum_conversions_value"),
                func.max(GoogleAdsProductPerformance.product_title).label("product_title"),
                func.max(GoogleAdsProductPerformance.campaign_name).label("campaign_name"),
                func.group_concat(GoogleAdsProductPerformance.campaign_id.distinct()).label("campaign_ids_str"),
            )
            .filter(
                GoogleAdsProductPerformance.date >= period_start,
                GoogleAdsProductPerformance.date <= period_end,
            )
            .group_by(GoogleAdsProductPerformance.product_item_id)
            .all()
        )

        logger.info(f"    Found {len(rows)} products in google_ads_products")

        # ── Clear existing product performance for idempotency ──
        cleared = self.db.query(ProductAdPerformance).filter(
            ProductAdPerformance.period_days == days,
        ).delete()
        self.db.commit()
        logger.info(f"    Cleared {cleared} existing product performance rows")

        processed = 0
        profitable_count = 0
        losing_count = 0

        for row in rows:
            product_item_id = row.product_item_id
            clicks = int(row.sum_clicks or 0)
            impressions = int(row.sum_impressions or 0)
            cost_micros = int(row.sum_cost_micros or 0)
            conversions = float(row.sum_conversions or 0)
            conversions_value = float(row.sum_conversions_value or 0)
            product_title = row.product_title or ""
            campaign_name = row.campaign_name or ""
            campaign_ids_str = row.campaign_ids_str or ""
            campaign_ids = list(set(campaign_ids_str.split(","))) if campaign_ids_str else []

            # ── Skip ultra-low-traffic products (< 10 clicks) ──
            if clicks < 10:
                continue

            # ── Extract Shopify IDs from product_item_id ──
            # Format: shopify_au_6770211422252_39981455441964
            shopify_product_id = None
            shopify_variant_id = None
            parts = product_item_id.split("_") if product_item_id else []
            if len(parts) >= 4:
                try:
                    shopify_product_id = int(parts[2])
                    shopify_variant_id = int(parts[3])
                except (ValueError, IndexError):
                    pass

            # ── Look up product cost via Product -> ProductCost bridge ──
            product_cost_value = None
            if shopify_product_id:
                # Find the Product by shopify_product_id to get its SKU
                product_record = (
                    self.db.query(Product)
                    .filter(Product.shopify_product_id == str(shopify_product_id))
                    .first()
                )
                if product_record and product_record.sku:
                    cost_record = (
                        self.db.query(ProductCost)
                        .filter(ProductCost.vendor_sku == product_record.sku)
                        .first()
                    )
                    if cost_record and cost_record.nett_nett_cost_inc_gst is not None:
                        product_cost_value = float(cost_record.nett_nett_cost_inc_gst)

            # ── Calculate metrics ──
            total_ad_spend = cost_micros / 1_000_000.0
            avg_cpc = (total_ad_spend / clicks) if clicks > 0 else None
            ad_conversion_rate = (conversions / clicks) if clicks > 0 else None
            ad_revenue = conversions_value
            ad_units_sold = int(conversions)

            if product_cost_value is not None:
                total_product_costs = product_cost_value * ad_units_sold
                gross_profit = ad_revenue - total_product_costs
                net_profit = ad_revenue - total_product_costs - total_ad_spend
            else:
                gross_profit = ad_revenue - total_ad_spend
                net_profit = gross_profit
                total_product_costs = 0.0

            profit_margin = (net_profit / ad_revenue) if ad_revenue > 0 else None
            revenue_roas = (ad_revenue / total_ad_spend) if total_ad_spend > 0 else None
            profit_roas = (net_profit / total_ad_spend) if total_ad_spend > 0 else None

            # ── Set flags ──
            is_profitable_to_advertise = net_profit > 0 and (profit_roas is not None and profit_roas >= 1.5)
            is_high_performer = profit_roas is not None and profit_roas >= 3.0
            is_losing_money = net_profit < 0

            # ── Set recommendation ──
            if is_high_performer:
                recommended_action = "scale"
            elif is_profitable_to_advertise:
                recommended_action = "continue"
            elif not is_profitable_to_advertise and net_profit > -total_ad_spend * 0.5:
                recommended_action = "reduce"
            elif is_losing_money and (profit_roas is not None and profit_roas < 0.5):
                recommended_action = "exclude"
            else:
                recommended_action = "reduce"

            # ── Calculate recommended_max_cpc ──
            recommended_max_cpc = None
            if product_cost_value is not None and conversions > 0 and ad_conversion_rate:
                margin_per_sale = (ad_revenue / conversions) - product_cost_value
                if margin_per_sale > 0:
                    recommended_max_cpc = margin_per_sale * ad_conversion_rate * 0.5

            # ── Build product_id from hash of product_item_id ──
            product_id_hash = abs(hash(product_item_id)) % (2**31)

            self.db.add(ProductAdPerformance(
                product_id=product_id_hash,
                shopify_product_id=shopify_product_id,
                product_title=product_title,
                product_sku=None,
                campaign_ids=campaign_ids,
                total_campaigns=len(campaign_ids),
                total_ad_spend=Decimal(str(round(total_ad_spend, 2))),
                avg_cpc=Decimal(str(round(avg_cpc, 2))) if avg_cpc is not None else None,
                ad_clicks=clicks,
                ad_conversions=ad_units_sold,
                ad_conversion_rate=round(ad_conversion_rate, 6) if ad_conversion_rate is not None else None,
                ad_revenue=Decimal(str(round(ad_revenue, 2))),
                ad_units_sold=ad_units_sold,
                product_cost=Decimal(str(round(product_cost_value, 2))) if product_cost_value is not None else None,
                total_product_costs=Decimal(str(round(total_product_costs, 2))),
                gross_profit=Decimal(str(round(gross_profit, 2))),
                net_profit=Decimal(str(round(net_profit, 2))),
                profit_margin=round(profit_margin, 6) if profit_margin is not None else None,
                revenue_roas=round(revenue_roas, 4) if revenue_roas is not None else None,
                profit_roas=round(profit_roas, 4) if profit_roas is not None else None,
                is_profitable_to_advertise=is_profitable_to_advertise,
                is_high_performer=is_high_performer,
                is_losing_money=is_losing_money,
                recommended_action=recommended_action,
                recommended_max_cpc=Decimal(str(round(recommended_max_cpc, 2))) if recommended_max_cpc is not None else None,
                period_start=datetime.combine(period_start, datetime.min.time()),
                period_end=datetime.combine(period_end, datetime.min.time()),
                period_days=days,
                analyzed_at=datetime.utcnow(),
            ))

            processed += 1
            if is_profitable_to_advertise:
                profitable_count += 1
            if is_losing_money:
                losing_count += 1

            if processed % 50 == 0:
                self.db.commit()

        self.db.commit()

        logger.info(
            f"    Product performance: {processed} processed, "
            f"{profitable_count} profitable, {losing_count} losing money"
        )

        return {"processed": processed, "profitable": profitable_count, "losing_money": losing_count}
