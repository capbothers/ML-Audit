"""
Strategic Intelligence Service — The Crown Jewel

Orchestrates ALL 16 intelligence modules, detects cross-module
correlations, and makes 5 targeted LLM calls to produce daily
and weekly strategic intelligence briefs.

Architecture:
  1. Collect data from all 16 modules (try/except per module)
  2. Compute KPI snapshot from raw DB queries
  3. Detect cross-module correlations algorithmically
  4. Build curated context packages for each LLM call
  5. Make 5 targeted LLM calls (executive pulse, CRO, issues, insights, growth)
  6. Assemble and persist the complete brief
"""
import asyncio
import hashlib
import json
import logging
import re as _re
import time
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple

from sqlalchemy import func, desc, and_
from sqlalchemy.orm import Session

from app.models.base import Base
from app.models.shopify import ShopifyOrder, ShopifyCustomer, ShopifyOrderItem, ShopifyProduct, ShopifyInventory
from app.models.ga4_data import GA4DailySummary, GA4DailyEcommerce, GA4LandingPage, GA4ProductPerformance, GA4PagePerformance
from app.models.search_console_data import SearchConsoleQuery, SearchConsolePage
from app.models.competitive_pricing import CompetitivePricing
from app.models.product_cost import ProductCost
from app.models.ml_intelligence import MLForecast, MLAnomaly, MLInventorySuggestion
from app.models.strategic_intelligence import StrategicBrief, BriefRecommendation, BriefCorrelation
from app.models.data_quality import DataSyncStatus

from app.services.llm_service import LLMService

logger = logging.getLogger(__name__)

# ── Decision-layer constants ─────────────────────────────────
# Staleness thresholds (hours) per data source
_STALE_THRESHOLDS = {
    'shopify': 6, 'ga4': 72, 'search_console': 96,
    'google_ads': 48, 'merchant_center': 48,
    'competitive_pricing': 168, 'product_costs': 720,
    'google_sheets_costs': 720,
}
# Module → underlying data sources
_MODULE_DATA_DEPS = {
    'customer': ['shopify'], 'pricing': ['competitive_pricing', 'product_costs'],
    'merchant_center': ['merchant_center'], 'seo': ['search_console'],
    'ad_spend': ['google_ads'], 'behavior': ['ga4'], 'inventory': ['shopify'],
    'ml_intelligence': ['shopify', 'ga4'], 'profitability': ['shopify', 'product_costs'],
    'content_gap': ['search_console', 'ga4'], 'redirect_health': ['ga4'],
    'journey': ['ga4'], 'attribution': ['ga4', 'google_ads'],
    'email': ['shopify'], 'data_quality': [], 'code_health': [],
}
# Urgency → weight for priority scoring
_URGENCY_WEIGHTS = {
    'immediate': 4, 'today': 4, 'this_week': 3,
    'this_month': 2, 'ongoing': 1, 'quarterly': 1,
}


def _dec(v):
    """Safely convert Decimal/None to float."""
    if v is None:
        return 0.0
    return float(v)


class StrategicIntelligenceService:
    """
    Orchestrates all 16 intelligence modules into daily and weekly
    strategic briefs with deep LLM analysis.
    """

    def __init__(self, db: Session):
        self.db = db
        self.llm = LLMService()

    # ------------------------------------------------------------------
    # PUBLIC: Generate briefs
    # ------------------------------------------------------------------

    def generate_daily_brief(self, target_date: Optional[date] = None) -> Dict:
        """Generate the daily intelligence brief."""
        start_time = time.time()
        target = target_date or date.today()

        # 1. Collect from all modules
        module_data, module_meta = self._collect_all_module_data()

        # 2. Compute KPI snapshot
        kpi_snapshot = self._compute_kpi_snapshot(target)

        # 3. Extract & normalize insights from every module
        all_insights = []
        for mod_name, mod_data in module_data.items():
            try:
                insights = self._extract_module_insights(mod_name, mod_data)
                all_insights.extend(insights)
            except Exception as e:
                logger.warning(f"Failed to extract insights from {mod_name}: {e}")

        # 4a. Module freshness & degraded state (Req 6)
        freshness = self._get_module_freshness()
        is_degraded, stale_modules = self._check_degraded_state(module_meta, freshness)

        # 4b. Score by impact × confidence × urgency (Req 5)
        for insight in all_insights:
            insight['priority_score'] = self._compute_priority_score(insight)

        # 4c. Sort by priority_score (replaces raw revenue_impact sort)
        all_insights.sort(key=lambda x: x.get('priority_score', 0), reverse=True)

        # 4d. Dedup against Brand Intelligence (Req 3)
        all_insights = self._dedup_against_brand_intel(all_insights)

        # 5. Detect cross-module correlations
        correlations = self._detect_correlations(module_data, kpi_snapshot)

        # 6. Build LLM context packages
        context_pkg = self._build_llm_context_package(
            'daily', all_insights, kpi_snapshot, correlations, module_meta
        )

        # 7. Make 4 targeted LLM calls (daily = no growth playbook)
        llm_calls = 0
        llm_tokens = 0

        pulse_result = self._llm_call_executive_pulse(context_pkg.get('priorities_context', ''), 'daily')
        llm_calls += 1

        cro_result = self._llm_call_cro_analysis(context_pkg.get('cro_context', ''))
        llm_calls += 1

        issues_result = self._llm_call_issue_triage(context_pkg.get('issues_context', ''))
        llm_calls += 1

        insights_result = self._llm_call_strategic_insights(context_pkg.get('strategic_context', ''))
        llm_calls += 1

        # 8. Assemble brief
        elapsed = time.time() - start_time

        # Categorize insights
        whats_working = [i for i in all_insights if i.get('insight_type') == 'positive'][:8]
        watch_items = [i for i in all_insights if i.get('insight_type') == 'risk'][:8]
        issue_items = [i for i in all_insights if i.get('insight_type') in ('issue', 'risk')]
        quick_wins = [i for i in all_insights
                      if _dec(i.get('effort_hours', 99)) <= 1 and _dec(i.get('revenue_impact', 0)) > 0][:10]

        total_opp = sum(_dec(i.get('revenue_impact', 0)) for i in all_insights if _dec(i.get('revenue_impact', 0)) > 0)

        brief_data = {
            'cadence': 'daily',
            'brief_date': target.isoformat(),
            'kpi_snapshot': kpi_snapshot,
            'modules_queried': module_meta.get('queried', []),
            'modules_succeeded': module_meta.get('succeeded', []),
            'modules_failed': module_meta.get('failed', []),
            'data_quality_score': self._compute_data_quality(module_meta),
            'executive_pulse': pulse_result.get('pulse', ''),
            'health_status': pulse_result.get('health_status', 'stable'),
            'todays_priorities': pulse_result.get('priorities', []),
            'conversion_analysis': cro_result,
            'growth_playbook': None,
            'cross_module_correlations': correlations,
            'issue_command_center': issue_items[:30],
            'ai_strategic_insights': insights_result,
            'whats_working': whats_working,
            'watch_list': watch_items,
            'protect': pulse_result.get('protect', []),
            'total_opportunity_value': round(total_opp, 2),
            'total_issues_identified': len(issue_items),
            'total_quick_wins': len(quick_wins),
            'quick_wins': quick_wins,
            'all_insights_count': len(all_insights),
            'generation_time_seconds': round(elapsed, 1),
            'llm_calls_made': llm_calls,
            'llm_tokens_used': llm_tokens,
            # Decision-layer fields (Req 6)
            'is_degraded': is_degraded,
            'stale_modules': stale_modules,
            'module_freshness': {k: v.get('last_sync') for k, v in freshness.items()},
        }

        # 9. Persist
        self._save_brief(brief_data)

        return brief_data

    def generate_weekly_brief(self, week_start: Optional[date] = None) -> Dict:
        """Generate the weekly intelligence brief (adds growth playbook)."""
        start_time = time.time()
        today = date.today()
        if week_start:
            ws = week_start
        else:
            ws = today - timedelta(days=today.weekday())  # Monday
        we = ws + timedelta(days=6)

        # Same pipeline as daily
        module_data, module_meta = self._collect_all_module_data()
        kpi_snapshot = self._compute_kpi_snapshot(today)

        all_insights = []
        for mod_name, mod_data in module_data.items():
            try:
                insights = self._extract_module_insights(mod_name, mod_data)
                all_insights.extend(insights)
            except Exception as e:
                logger.warning(f"Failed to extract insights from {mod_name}: {e}")

        # Decision-layer pipeline (same as daily)
        freshness = self._get_module_freshness()
        is_degraded, stale_modules = self._check_degraded_state(module_meta, freshness)

        for insight in all_insights:
            insight['priority_score'] = self._compute_priority_score(insight)
        all_insights.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        all_insights = self._dedup_against_brand_intel(all_insights)

        correlations = self._detect_correlations(module_data, kpi_snapshot)

        context_pkg = self._build_llm_context_package(
            'weekly', all_insights, kpi_snapshot, correlations, module_meta
        )

        # 5 LLM calls for weekly (includes growth playbook)
        llm_calls = 0
        pulse_result = self._llm_call_executive_pulse(context_pkg.get('priorities_context', ''), 'weekly')
        llm_calls += 1
        cro_result = self._llm_call_cro_analysis(context_pkg.get('cro_context', ''))
        llm_calls += 1
        issues_result = self._llm_call_issue_triage(context_pkg.get('issues_context', ''))
        llm_calls += 1
        insights_result = self._llm_call_strategic_insights(context_pkg.get('strategic_context', ''))
        llm_calls += 1
        growth_result = self._llm_call_growth_playbook(context_pkg.get('growth_context', ''))
        llm_calls += 1

        elapsed = time.time() - start_time

        whats_working = [i for i in all_insights if i.get('insight_type') == 'positive'][:8]
        watch_items = [i for i in all_insights if i.get('insight_type') == 'risk'][:8]
        issue_items = [i for i in all_insights if i.get('insight_type') in ('issue', 'risk')]
        quick_wins = [i for i in all_insights
                      if _dec(i.get('effort_hours', 99)) <= 1 and _dec(i.get('revenue_impact', 0)) > 0][:10]
        total_opp = sum(_dec(i.get('revenue_impact', 0)) for i in all_insights if _dec(i.get('revenue_impact', 0)) > 0)

        brief_data = {
            'cadence': 'weekly',
            'brief_date': today.isoformat(),
            'week_start_date': ws.isoformat(),
            'week_end_date': we.isoformat(),
            'kpi_snapshot': kpi_snapshot,
            'modules_queried': module_meta.get('queried', []),
            'modules_succeeded': module_meta.get('succeeded', []),
            'modules_failed': module_meta.get('failed', []),
            'data_quality_score': self._compute_data_quality(module_meta),
            'executive_pulse': pulse_result.get('pulse', ''),
            'health_status': pulse_result.get('health_status', 'stable'),
            'todays_priorities': pulse_result.get('priorities', []),
            'conversion_analysis': cro_result,
            'growth_playbook': growth_result,
            'cross_module_correlations': correlations,
            'issue_command_center': issue_items[:30],
            'ai_strategic_insights': insights_result,
            'whats_working': whats_working,
            'watch_list': watch_items,
            'protect': pulse_result.get('protect', []),
            'total_opportunity_value': round(total_opp, 2),
            'total_issues_identified': len(issue_items),
            'total_quick_wins': len(quick_wins),
            'quick_wins': quick_wins,
            'all_insights_count': len(all_insights),
            'generation_time_seconds': round(elapsed, 1),
            'llm_calls_made': llm_calls,
            'llm_tokens_used': 0,
            'is_degraded': is_degraded,
            'stale_modules': stale_modules,
            'module_freshness': {k: v.get('last_sync') for k, v in freshness.items()},
        }

        self._save_brief(brief_data)
        return brief_data

    # ------------------------------------------------------------------
    # PUBLIC: Retrieve existing briefs
    # ------------------------------------------------------------------

    def get_current_brief(self, cadence: str = 'daily') -> Optional[Dict]:
        """Get the most recent brief for the given cadence."""
        brief = (
            self.db.query(StrategicBrief)
            .filter(StrategicBrief.cadence == cadence, StrategicBrief.is_current == True)
            .order_by(desc(StrategicBrief.generated_at))
            .first()
        )
        if not brief:
            return None
        return self._brief_to_dict(brief)

    def get_brief_history(self, cadence: str = 'weekly', limit: int = 12) -> List[Dict]:
        """Get historical briefs."""
        briefs = (
            self.db.query(StrategicBrief)
            .filter(StrategicBrief.cadence == cadence)
            .order_by(desc(StrategicBrief.generated_at))
            .limit(limit)
            .all()
        )
        return [self._brief_to_dict(b) for b in briefs]

    def get_recommendations(self, status: Optional[str] = None, category: Optional[str] = None) -> List[Dict]:
        """Get recommendations with optional filters."""
        q = self.db.query(BriefRecommendation)
        if status:
            q = q.filter(BriefRecommendation.status == status)
        if category:
            q = q.filter(BriefRecommendation.category == category)
        recs = q.order_by(desc(BriefRecommendation.created_at)).limit(50).all()
        return [self._rec_to_dict(r) for r in recs]

    def update_recommendation_status(self, rec_id: int, status: str, actual_impact: Optional[float] = None) -> Dict:
        """Update a recommendation's status."""
        rec = self.db.query(BriefRecommendation).filter(BriefRecommendation.id == rec_id).first()
        if not rec:
            return {'error': 'Not found'}
        rec.status = status
        if status == 'completed':
            rec.completed_at = datetime.utcnow()
        if actual_impact is not None:
            rec.actual_impact = actual_impact
        self.db.commit()
        return self._rec_to_dict(rec)

    # ------------------------------------------------------------------
    # DATA COLLECTION: All 16 modules
    # ------------------------------------------------------------------

    def _collect_all_module_data(self) -> tuple:
        """
        Collect data from all 16 modules. Each module is called in a
        try/except so one failure doesn't break the brief.
        """
        module_data = {}
        meta = {'queried': [], 'succeeded': [], 'failed': []}

        modules = [
            ('customer', self._collect_customer),
            ('pricing', self._collect_pricing),
            ('merchant_center', self._collect_merchant_center),
            ('seo', self._collect_seo),
            ('ad_spend', self._collect_ad_spend),
            ('email', self._collect_email),
            ('behavior', self._collect_behavior),
            ('ml_intelligence', self._collect_ml),
            ('content_gap', self._collect_content),
            ('profitability', self._collect_profitability),
            ('data_quality', self._collect_data_quality),
            ('code_health', self._collect_code_health),
            ('redirect_health', self._collect_redirect_health),
            ('inventory', self._collect_inventory),
            ('journey', self._collect_journey),
            ('attribution', self._collect_attribution),
        ]

        for name, collector in modules:
            meta['queried'].append(name)
            try:
                data = collector()
                if data:
                    module_data[name] = data
                    meta['succeeded'].append(name)
                else:
                    meta['failed'].append({'module': name, 'reason': 'No data returned'})
            except Exception as e:
                logger.warning(f"Module {name} failed: {e}")
                meta['failed'].append({'module': name, 'reason': str(e)[:200]})

        return module_data, meta

    # -- Individual module collectors --

    def _collect_customer(self) -> Optional[Dict]:
        from app.services.customer_intelligence_service import CustomerIntelligenceService
        svc = CustomerIntelligenceService(self.db)
        return svc.get_dashboard()

    def _collect_pricing(self) -> Optional[Dict]:
        from app.services.pricing_intelligence_service import PricingIntelligenceService
        svc = PricingIntelligenceService(self.db)
        sku_data = asyncio.run(svc.get_sku_pricing_sensitivity(days=30, limit=50))
        brand_data = asyncio.run(svc.get_brand_pricing_impact(days=30))
        return {'sku_sensitivity': sku_data, 'brand_impact': brand_data}

    def _collect_merchant_center(self) -> Optional[Dict]:
        from app.services.merchant_center_intelligence_service import MerchantCenterIntelligenceService
        svc = MerchantCenterIntelligenceService(self.db)
        return svc.get_dashboard()

    def _collect_seo(self) -> Optional[Dict]:
        from app.services.seo_service import SEOService
        svc = SEOService(self.db)
        return asyncio.run(svc.get_seo_dashboard())

    def _collect_ad_spend(self) -> Optional[Dict]:
        from app.services.ad_spend_service import AdSpendService
        svc = AdSpendService(self.db)
        return asyncio.run(svc.get_ad_dashboard())

    def _collect_email(self) -> Optional[Dict]:
        from app.services.email_service import EmailService
        svc = EmailService(self.db)
        return asyncio.run(svc.get_email_dashboard())

    def _collect_behavior(self) -> Optional[Dict]:
        from app.services.user_behavior_service import UserBehaviorService
        svc = UserBehaviorService(self.db)
        return asyncio.run(svc.get_behavior_dashboard())

    def _collect_ml(self) -> Optional[Dict]:
        from app.services.ml_intelligence_service import MLIntelligenceService
        svc = MLIntelligenceService(self.db)
        forecasts = svc.generate_forecasts()
        anomalies = svc.detect_anomalies()
        drivers = svc.get_revenue_drivers()
        return {
            'forecasts': forecasts,
            'anomalies': anomalies,
            'drivers': drivers,
        }

    def _collect_content(self) -> Optional[Dict]:
        from app.services.content_gap_service import ContentGapService
        svc = ContentGapService(self.db)
        return asyncio.run(svc.get_content_dashboard())

    def _collect_profitability(self) -> Optional[Dict]:
        from app.services.profitability_service import ProfitabilityService
        svc = ProfitabilityService(self.db)
        end = datetime.now()
        start = end - timedelta(days=30)
        summary = asyncio.run(svc.get_profitability_summary(start, end))
        hidden = asyncio.run(svc.get_hidden_gems(start, end))
        losing = asyncio.run(svc.get_losing_products(start, end))
        return {'summary': summary, 'hidden_gems': hidden, 'losing_products': losing}

    def _collect_data_quality(self) -> Optional[Dict]:
        from app.services.data_quality_service import DataQualityService
        svc = DataQualityService(self.db)
        return asyncio.run(svc.run_full_data_quality_check())

    def _collect_code_health(self) -> Optional[Dict]:
        from app.services.code_health_service import CodeHealthService
        svc = CodeHealthService(self.db)
        # get_code_dashboard requires repo_name — try to find from recent data
        from app.models.code_health import CodeRepository
        recent = self.db.query(CodeRepository.repo_name).first()
        repo_name = recent[0] if recent else None
        if not repo_name:
            logger.info("No code health repo found — skipping module")
            return None
        return asyncio.run(svc.get_code_dashboard(repo_name))

    def _collect_redirect_health(self) -> Optional[Dict]:
        from app.services.redirect_health_service import RedirectHealthService
        svc = RedirectHealthService(self.db)
        return asyncio.run(svc.get_404_dashboard())

    def _collect_inventory(self) -> Optional[Dict]:
        from app.services.inventory_intelligence_service import InventoryIntelligenceService
        svc = InventoryIntelligenceService(self.db)
        return svc.get_dashboard_data()

    def _collect_journey(self) -> Optional[Dict]:
        from app.services.journey_service import JourneyService
        svc = JourneyService(self.db)
        return asyncio.run(svc.get_journey_dashboard())

    def _collect_attribution(self) -> Optional[Dict]:
        from app.services.attribution_service import AttributionService
        svc = AttributionService(self.db)
        end = datetime.now()
        start = end - timedelta(days=30)
        return asyncio.run(svc.get_attribution_insights(start, end))

    # ------------------------------------------------------------------
    # KPI SNAPSHOT: Direct DB queries for ground truth
    # ------------------------------------------------------------------

    def _compute_kpi_snapshot(self, target: date) -> Dict:
        """Direct DB queries for core business KPIs."""
        snapshot = {}

        try:
            # GA4: find latest available date (data may lag behind today)
            latest_ga4_date = self.db.query(func.max(GA4DailySummary.date)).scalar()
            effective_date = min(target, latest_ga4_date) if latest_ga4_date else target
            snapshot['_ga4_latest_date'] = str(latest_ga4_date) if latest_ga4_date else None

            today_ga4 = self.db.query(GA4DailySummary).filter(
                GA4DailySummary.date == effective_date
            ).first()
            yesterday_ga4 = self.db.query(GA4DailySummary).filter(
                GA4DailySummary.date == effective_date - timedelta(days=1)
            ).first()

            # Last 7 days GA4 — use effective_date so we always compare full windows
            week_ago = effective_date - timedelta(days=7)
            ga4_7d = self.db.query(
                func.sum(GA4DailySummary.sessions).label('sessions'),
                func.sum(GA4DailySummary.active_users).label('users'),
                func.avg(GA4DailySummary.bounce_rate).label('bounce'),
                func.sum(GA4DailySummary.total_revenue).label('revenue'),
                func.sum(GA4DailySummary.total_conversions).label('conversions'),
                func.count(GA4DailySummary.id).label('day_count'),
            ).filter(GA4DailySummary.date > week_ago, GA4DailySummary.date <= effective_date).first()

            # Previous 7 days for comparison (same window size)
            prev_week_start = week_ago - timedelta(days=7)
            ga4_prev_7d = self.db.query(
                func.sum(GA4DailySummary.sessions).label('sessions'),
                func.sum(GA4DailySummary.total_revenue).label('revenue'),
                func.sum(GA4DailySummary.total_conversions).label('conversions'),
                func.count(GA4DailySummary.id).label('day_count'),
            ).filter(GA4DailySummary.date > prev_week_start, GA4DailySummary.date <= week_ago).first()

            snapshot['sessions_today'] = today_ga4.sessions if today_ga4 else 0
            snapshot['sessions_yesterday'] = yesterday_ga4.sessions if yesterday_ga4 else 0
            snapshot['users_today'] = today_ga4.active_users if today_ga4 else 0
            snapshot['bounce_rate_today'] = round(today_ga4.bounce_rate or 0, 2) if today_ga4 else 0
            snapshot['revenue_ga4_today'] = _dec(today_ga4.total_revenue) if today_ga4 else 0
            snapshot['conversions_today'] = today_ga4.total_conversions if today_ga4 else 0

            snapshot['sessions_7d'] = int(ga4_7d.sessions or 0) if ga4_7d else 0
            snapshot['revenue_ga4_7d'] = _dec(ga4_7d.revenue) if ga4_7d else 0
            snapshot['conversions_7d'] = int(ga4_7d.conversions or 0) if ga4_7d else 0

            # Normalize to daily averages for fair WoW comparison
            days_current = int(ga4_7d.day_count or 0) if ga4_7d else 0
            days_prev = int(ga4_prev_7d.day_count or 0) if ga4_prev_7d else 0
            raw_prev_sessions = int(ga4_prev_7d.sessions or 0) if ga4_prev_7d else 0
            raw_prev_revenue = _dec(ga4_prev_7d.revenue) if ga4_prev_7d else 0
            raw_prev_conversions = int(ga4_prev_7d.conversions or 0) if ga4_prev_7d else 0

            if days_current > 0 and days_prev > 0 and days_current != days_prev:
                # Scale previous period to match current period day count
                scale = days_current / days_prev
                snapshot['sessions_prev_7d'] = int(raw_prev_sessions * scale)
                snapshot['revenue_prev_7d'] = round(raw_prev_revenue * scale, 2)
                snapshot['conversions_prev_7d'] = int(raw_prev_conversions * scale)
                snapshot['_wow_note'] = f"Previous period normalized: {days_prev}d scaled to {days_current}d"
            else:
                snapshot['sessions_prev_7d'] = raw_prev_sessions
                snapshot['revenue_prev_7d'] = raw_prev_revenue
                snapshot['conversions_prev_7d'] = raw_prev_conversions

        except Exception as e:
            logger.warning(f"GA4 KPI query failed: {e}")

        try:
            # Shopify orders — use latest order date if target has no data
            latest_order_raw = self.db.query(func.max(func.date(ShopifyOrder.created_at))).scalar()
            # SQLite returns date strings; parse to date object
            if isinstance(latest_order_raw, str):
                latest_order_date = date.fromisoformat(latest_order_raw)
            else:
                latest_order_date = latest_order_raw
            shopify_effective = target
            if latest_order_date and latest_order_date < target:
                shopify_effective = latest_order_date
            snapshot['_shopify_latest_date'] = str(latest_order_date) if latest_order_date else None

            today_start = datetime(shopify_effective.year, shopify_effective.month, shopify_effective.day)
            today_end = today_start + timedelta(days=1)

            orders_today = self.db.query(
                func.count(ShopifyOrder.id).label('count'),
                func.sum(ShopifyOrder.total_price).label('revenue'),
                func.avg(ShopifyOrder.total_price).label('aov'),
            ).filter(
                ShopifyOrder.created_at >= today_start,
                ShopifyOrder.created_at < today_end,
            ).first()

            # 7-day orders
            week_start = today_start - timedelta(days=7)
            orders_7d = self.db.query(
                func.count(ShopifyOrder.id).label('count'),
                func.sum(ShopifyOrder.total_price).label('revenue'),
                func.avg(ShopifyOrder.total_price).label('aov'),
            ).filter(
                ShopifyOrder.created_at >= week_start,
                ShopifyOrder.created_at < today_end,
            ).first()

            # 30-day orders
            month_start = today_start - timedelta(days=30)
            orders_30d = self.db.query(
                func.count(ShopifyOrder.id).label('count'),
                func.sum(ShopifyOrder.total_price).label('revenue'),
                func.avg(ShopifyOrder.total_price).label('aov'),
            ).filter(
                ShopifyOrder.created_at >= month_start,
                ShopifyOrder.created_at < today_end,
            ).first()

            snapshot['orders_today'] = int(orders_today.count or 0) if orders_today else 0
            snapshot['revenue_today'] = _dec(orders_today.revenue) if orders_today else 0
            snapshot['aov_today'] = round(_dec(orders_today.aov), 2) if orders_today and orders_today.aov else 0

            snapshot['orders_7d'] = int(orders_7d.count or 0) if orders_7d else 0
            snapshot['revenue_7d'] = _dec(orders_7d.revenue) if orders_7d else 0
            snapshot['aov_7d'] = round(_dec(orders_7d.aov), 2) if orders_7d and orders_7d.aov else 0

            snapshot['orders_30d'] = int(orders_30d.count or 0) if orders_30d else 0
            snapshot['revenue_30d'] = _dec(orders_30d.revenue) if orders_30d else 0
            snapshot['aov_30d'] = round(_dec(orders_30d.aov), 2) if orders_30d and orders_30d.aov else 0

            # Conversion rate (orders / sessions)
            sessions_7d = snapshot.get('sessions_7d', 0)
            if sessions_7d > 0:
                snapshot['conversion_rate_7d'] = round(snapshot['orders_7d'] / sessions_7d * 100, 2)
            else:
                snapshot['conversion_rate_7d'] = 0

        except Exception as e:
            logger.warning(f"Shopify KPI query failed: {e}")

        try:
            # GA4 ecommerce funnel
            ecom_7d = self.db.query(
                func.sum(GA4DailyEcommerce.items_viewed).label('views'),
                func.sum(GA4DailyEcommerce.add_to_carts).label('atc'),
                func.sum(GA4DailyEcommerce.checkouts).label('checkouts'),
                func.sum(GA4DailyEcommerce.ecommerce_purchases).label('purchases'),
            ).filter(
                GA4DailyEcommerce.date >= target - timedelta(days=7),
                GA4DailyEcommerce.date <= target,
            ).first()

            if ecom_7d:
                snapshot['items_viewed_7d'] = int(ecom_7d.views or 0)
                snapshot['add_to_carts_7d'] = int(ecom_7d.atc or 0)
                snapshot['checkouts_7d'] = int(ecom_7d.checkouts or 0)
                snapshot['purchases_7d'] = int(ecom_7d.purchases or 0)
                if snapshot['items_viewed_7d'] > 0:
                    snapshot['view_to_cart_rate'] = round(snapshot['add_to_carts_7d'] / snapshot['items_viewed_7d'] * 100, 2)
                if snapshot['add_to_carts_7d'] > 0:
                    snapshot['cart_abandonment_rate'] = round(
                        (1 - snapshot['purchases_7d'] / snapshot['add_to_carts_7d']) * 100, 2
                    )

        except Exception as e:
            logger.warning(f"Ecommerce funnel query failed: {e}")

        try:
            # Customer counts
            total_customers = self.db.query(func.count(ShopifyCustomer.id)).scalar() or 0

            # At-risk: use RFM-based calculation (the is_at_risk boolean is not populated)
            # Count customers whose last order was 90-365 days ago (at-risk window)
            from sqlalchemy import distinct
            cutoff_at_risk = datetime.now() - timedelta(days=90)
            cutoff_lost = datetime.now() - timedelta(days=365)
            at_risk = self.db.query(func.count(distinct(ShopifyOrder.customer_id))).filter(
                ShopifyOrder.customer_id.isnot(None),
            ).filter(
                ~ShopifyOrder.customer_id.in_(
                    self.db.query(ShopifyOrder.customer_id).filter(
                        ShopifyOrder.created_at >= cutoff_at_risk
                    )
                ),
                ShopifyOrder.customer_id.in_(
                    self.db.query(ShopifyOrder.customer_id).filter(
                        ShopifyOrder.created_at >= cutoff_lost
                    )
                ),
            ).scalar() or 0

            snapshot['total_customers'] = total_customers
            snapshot['at_risk_customers'] = at_risk

            # New customers this month
            month_start_dt = datetime(target.year, target.month, 1)
            new_this_month = self.db.query(func.count(ShopifyCustomer.id)).filter(
                ShopifyCustomer.created_at >= month_start_dt
            ).scalar() or 0
            snapshot['new_customers_month'] = new_this_month

        except Exception as e:
            logger.warning(f"Customer KPI query failed: {e}")

        try:
            # Inventory health — only count active, deny-oversell products
            # Join to ShopifyProduct to filter by status='active'
            # Exclude inventory_policy='continue' (oversell-allowed, qty<=0 is expected)
            active_product_ids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()

            base_q = self.db.query(ShopifyInventory).filter(
                ShopifyInventory.shopify_product_id.in_(active_product_ids),
                ShopifyInventory.inventory_policy != 'continue',
            )
            total_skus = base_q.count()
            oos = base_q.filter(ShopifyInventory.inventory_quantity <= 0).count()

            snapshot['total_skus'] = total_skus
            snapshot['out_of_stock_skus'] = oos
            snapshot['oos_rate'] = round(oos / total_skus * 100, 1) if total_skus > 0 else 0
            # Also track full catalog size for context
            snapshot['total_catalog_skus'] = self.db.query(func.count(ShopifyInventory.id)).scalar() or 0
        except Exception as e:
            logger.warning(f"Inventory KPI query failed: {e}")

        try:
            # Pricing health — filter to LATEST pricing date only
            # CompetitivePricing stores daily snapshots; without date filter, counts are inflated ~89x
            latest_pricing_date = self.db.query(
                func.max(CompetitivePricing.pricing_date)
            ).scalar()

            if latest_pricing_date:
                below_min = self.db.query(func.count(CompetitivePricing.id)).filter(
                    CompetitivePricing.pricing_date == latest_pricing_date,
                    CompetitivePricing.is_below_minimum == True,
                ).scalar() or 0
                losing_money = self.db.query(func.count(CompetitivePricing.id)).filter(
                    CompetitivePricing.pricing_date == latest_pricing_date,
                    CompetitivePricing.is_losing_money == True,
                ).scalar() or 0
                total_priced = self.db.query(func.count(CompetitivePricing.id)).filter(
                    CompetitivePricing.pricing_date == latest_pricing_date,
                ).scalar() or 0
            else:
                below_min = 0
                losing_money = 0
                total_priced = 0

            snapshot['below_minimum_skus'] = below_min
            snapshot['losing_money_skus'] = losing_money
            snapshot['total_priced_skus'] = total_priced
            snapshot['_pricing_date'] = str(latest_pricing_date) if latest_pricing_date else None
        except Exception as e:
            logger.warning(f"Pricing KPI query failed: {e}")

        try:
            # ML forecasts
            latest_forecast = self.db.query(MLForecast).order_by(desc(MLForecast.id)).first()
            if latest_forecast:
                snapshot['forecast_revenue_next_7d'] = _dec(getattr(latest_forecast, 'predicted_value', 0))

            active_anomalies = self.db.query(func.count(MLAnomaly.id)).filter(
                MLAnomaly.is_acknowledged == False
            ).scalar() or 0
            snapshot['active_anomalies'] = active_anomalies
        except Exception as e:
            logger.warning(f"ML KPI query failed: {e}")

        return snapshot

    # ------------------------------------------------------------------
    # INSIGHT EXTRACTION: Normalize module outputs
    # ------------------------------------------------------------------

    def _extract_module_insights(self, module_name: str, data: Dict) -> List[Dict]:
        """Extract normalized insights from any module's output."""
        insights = []

        try:
            if module_name == 'customer':
                insights.extend(self._extract_customer_insights(data))
            elif module_name == 'pricing':
                insights.extend(self._extract_pricing_insights(data))
            elif module_name == 'merchant_center':
                insights.extend(self._extract_merchant_insights(data))
            elif module_name == 'seo':
                insights.extend(self._extract_seo_insights(data))
            elif module_name == 'ml_intelligence':
                insights.extend(self._extract_ml_insights(data))
            elif module_name == 'inventory':
                insights.extend(self._extract_inventory_insights(data))
            elif module_name == 'behavior':
                insights.extend(self._extract_behavior_insights(data))
            elif module_name == 'content_gap':
                insights.extend(self._extract_content_insights(data))
            elif module_name == 'redirect_health':
                insights.extend(self._extract_redirect_insights(data))
            elif module_name == 'ad_spend':
                insights.extend(self._extract_ad_insights(data))
            elif module_name == 'email':
                insights.extend(self._extract_email_insights(data))
            elif module_name == 'profitability':
                insights.extend(self._extract_profitability_insights(data))
            elif module_name == 'code_health':
                insights.extend(self._extract_code_insights(data))
            elif module_name == 'data_quality':
                insights.extend(self._extract_dq_insights(data))
            elif module_name == 'journey':
                insights.extend(self._extract_journey_insights(data))
            elif module_name == 'attribution':
                insights.extend(self._extract_attribution_insights(data))
        except Exception as e:
            logger.warning(f"Insight extraction failed for {module_name}: {e}")

        return insights

    def _make_insight(self, source: str, itype: str, title: str, desc: str,
                      revenue: float = 0, cost: float = 0, confidence: float = 0.7,
                      action: str = '', effort: float = 2.0, urgency: str = 'this_week',
                      evidence: dict = None) -> Dict:
        """Helper to create a normalized insight dict."""
        return {
            'source_module': source,
            'insight_type': itype,  # opportunity|issue|risk|positive
            'title': title,
            'description': desc,
            'revenue_impact': revenue,
            'cost_impact': cost,
            'confidence': confidence,
            'data_evidence': evidence or {},
            'suggested_action': action,
            'effort_hours': effort,
            'urgency': urgency,
        }

    def _extract_customer_insights(self, data: Dict) -> List[Dict]:
        insights = []
        kpis = data.get('overview_kpis', {})
        rfm = data.get('rfm_distribution', [])

        at_risk_count = 0
        champions_count = 0
        for seg in rfm:
            name = (seg.get('segment') or '').lower()
            if 'risk' in name or 'lost' in name or 'hibernat' in name:
                at_risk_count += seg.get('count', 0)
            if 'champion' in name:
                champions_count += seg.get('count', 0)

        if at_risk_count > 100:
            insights.append(self._make_insight(
                'customer', 'risk',
                f'{at_risk_count:,} customers at risk of churning',
                f'RFM analysis identified {at_risk_count:,} customers in At-Risk, Hibernating, or Lost segments.',
                revenue=at_risk_count * 50,  # est $50 avg recovery value
                action=f'Launch a targeted win-back email campaign to the {at_risk_count:,} at-risk customers with a 10% discount offer.',
                effort=4.0, urgency='this_week',
                evidence={'at_risk_count': at_risk_count, 'champions': champions_count}
            ))

        repeat_rate = kpis.get('repeat_rate', 0)
        if repeat_rate and repeat_rate < 25:
            insights.append(self._make_insight(
                'customer', 'opportunity',
                f'Repeat purchase rate is {repeat_rate}% — below 25% benchmark',
                'Improving repeat rate drives compounding revenue. Industry benchmark is 25-30%.',
                revenue=float(kpis.get('total_customers', 0)) * 0.05 * float(kpis.get('avg_ltv', 100)),
                action='Implement a post-purchase email sequence: Day 3 thank you, Day 14 related products, Day 30 replenishment reminder.',
                effort=6.0, urgency='this_week'
            ))

        if champions_count > 0:
            insights.append(self._make_insight(
                'customer', 'positive',
                f'{champions_count:,} Champion customers driving revenue',
                f'Top-tier RFM segment contributing disproportionate revenue.',
                action='Protect this segment — do not change email frequency or discount strategy for Champions.',
                effort=0, urgency='ongoing',
                evidence={'champions': champions_count}
            ))

        return insights

    def _extract_pricing_insights(self, data: Dict) -> List[Dict]:
        insights = []
        kpis = data.get('kpis', {})
        brands = data.get('brand_summary', [])

        below_min = kpis.get('below_minimum_count', 0)
        if below_min > 0:
            insights.append(self._make_insight(
                'pricing', 'issue',
                f'{below_min} SKUs priced below minimum — margin erosion',
                f'These products are selling below the minimum price set by suppliers.',
                revenue=below_min * 15,  # est $15 margin loss per SKU/month
                action=f'Review and increase prices for {below_min} below-minimum SKUs in Shopify admin.',
                effort=2.0, urgency='immediate',
                evidence={'below_min_count': below_min}
            ))

        losing = kpis.get('losing_money_count', 0)
        if losing > 0:
            insights.append(self._make_insight(
                'pricing', 'issue',
                f'{losing} SKUs losing money on every sale',
                f'Cost exceeds selling price — every sale is a net loss.',
                revenue=losing * 25,
                action=f'Immediately raise prices or delist the {losing} loss-making SKUs.',
                effort=1.0, urgency='immediate',
                evidence={'losing_count': losing}
            ))

        # Top undercut brands
        for brand in (brands or [])[:3]:
            name = brand.get('vendor', 'Unknown')
            undercut_pct = brand.get('avg_price_gap_pct', 0)
            if undercut_pct and undercut_pct < -5:
                insights.append(self._make_insight(
                    'pricing', 'risk',
                    f'{name} — competitors undercut by {abs(undercut_pct):.0f}%',
                    f'Brand {name} products are priced {abs(undercut_pct):.0f}% above cheapest competitor.',
                    revenue=abs(undercut_pct) * 100,
                    action=f'Review pricing for {name} products. Consider matching competitors or adding value bundles.',
                    effort=3.0, urgency='this_week',
                    evidence={'brand': name, 'gap_pct': undercut_pct}
                ))

        return insights

    def _extract_merchant_insights(self, data: Dict) -> List[Dict]:
        insights = []
        kpis = data.get('kpis', {})
        issues = data.get('issues', [])

        health_score = kpis.get('health_score', 100)
        if health_score < 80:
            insights.append(self._make_insight(
                'merchant_center', 'issue',
                f'Merchant Center health score: {health_score}/100',
                'Low feed health reduces product visibility on Google Shopping.',
                revenue=kpis.get('revenue_at_risk', 0),
                action='Fix top feed issues: missing GTINs, out-of-stock products, pricing violations.',
                effort=4.0, urgency='this_week',
            ))

        for issue in (issues or [])[:3]:
            itype = issue.get('type', '')
            count = issue.get('count', 0)
            if count > 50:
                insights.append(self._make_insight(
                    'merchant_center', 'issue',
                    f'Feed issue: {itype} — {count:,} products affected',
                    issue.get('description', f'{count} products have {itype} issue'),
                    revenue=count * 5,
                    action=issue.get('fix', f'Resolve {itype} for {count:,} products'),
                    effort=2.0, urgency='this_week',
                    evidence={'issue_type': itype, 'count': count}
                ))

        return insights

    def _extract_seo_insights(self, data: Dict) -> List[Dict]:
        insights = []
        opps = data.get('opportunities', [])
        technical = data.get('technical_issues', [])

        for opp in (opps or [])[:3]:
            insights.append(self._make_insight(
                'seo', 'opportunity',
                opp.get('title', 'SEO Opportunity'),
                opp.get('description', ''),
                revenue=_dec(opp.get('estimated_monthly_value', 0)),
                action=opp.get('recommendation', ''),
                effort=2.0, urgency='this_week',
                evidence={'query': opp.get('query', ''), 'position': opp.get('position', 0)}
            ))

        for issue in (technical or [])[:2]:
            insights.append(self._make_insight(
                'seo', 'issue',
                issue.get('title', 'Technical SEO Issue'),
                issue.get('description', ''),
                revenue=_dec(issue.get('traffic_impact', 0)) * 2,
                action=issue.get('fix', ''),
                effort=3.0, urgency='this_week',
            ))

        return insights

    def _extract_ml_insights(self, data: Dict) -> List[Dict]:
        insights = []
        anomalies = data.get('anomalies', [])
        forecasts = data.get('forecasts', {})
        drivers = data.get('drivers', {})

        for a in (anomalies if isinstance(anomalies, list) else [])[:3]:
            insights.append(self._make_insight(
                'ml_intelligence', 'risk',
                f"Anomaly: {a.get('metric', 'metric')} — {a.get('severity', 'moderate')}",
                a.get('description', f"Unusual {a.get('direction', 'change')} detected"),
                revenue=abs(_dec(a.get('deviation_value', 0))) * 100,
                action=a.get('suggested_action', 'Investigate the root cause'),
                effort=1.0, urgency='immediate',
                evidence=a
            ))

        if isinstance(drivers, dict):
            primary = drivers.get('primary_driver', '')
            if primary:
                insights.append(self._make_insight(
                    'ml_intelligence', 'opportunity',
                    f'Primary revenue driver: {primary}',
                    drivers.get('explanation', f'{primary} is the biggest lever for revenue'),
                    action=f'Focus optimization efforts on improving {primary}.',
                    effort=4.0, urgency='this_week',
                    evidence=drivers
                ))

        return insights

    def _extract_inventory_insights(self, data: Dict) -> List[Dict]:
        insights = []
        snapshot = data.get('snapshot', data.get('executive_snapshot', {}))

        dead_stock = snapshot.get('dead_stock_count', 0)
        if dead_stock > 50:
            insights.append(self._make_insight(
                'inventory', 'issue',
                f'{dead_stock:,} dead stock SKUs tying up capital',
                f'Products with zero sales velocity consuming warehouse space.',
                cost=dead_stock * 20,
                action=f'Create a clearance sale or bundle dead stock items. Consider liquidation for lowest-value items.',
                effort=4.0, urgency='this_week',
                evidence={'dead_stock': dead_stock}
            ))

        reorder_urgent = snapshot.get('reorder_now_count', 0)
        if reorder_urgent > 0:
            insights.append(self._make_insight(
                'inventory', 'risk',
                f'{reorder_urgent} SKUs need reorder NOW — stock running out',
                'These products will go out of stock within days at current velocity.',
                revenue=reorder_urgent * 200,
                action=f'Place immediate reorder for {reorder_urgent} critical SKUs.',
                effort=2.0, urgency='immediate',
                evidence={'reorder_now': reorder_urgent}
            ))

        return insights

    def _extract_behavior_insights(self, data: Dict) -> List[Dict]:
        insights = []
        friction = data.get('friction_pages', data.get('friction', []))
        checkout = data.get('checkout_funnel', {})

        for page in (friction if isinstance(friction, list) else [])[:2]:
            insights.append(self._make_insight(
                'behavior', 'issue',
                f"High friction: {page.get('page_path', page.get('url', 'unknown page'))}",
                f"Friction score {page.get('friction_score', 'high')} — {page.get('issues', 'multiple UX issues')}",
                revenue=_dec(page.get('estimated_revenue_loss', 500)),
                action=page.get('fix', 'Review page UX and reduce friction points'),
                effort=3.0, urgency='this_week',
                evidence=page
            ))

        if isinstance(checkout, dict) and checkout.get('biggest_leak'):
            leak = checkout['biggest_leak']
            insights.append(self._make_insight(
                'behavior', 'issue',
                f"Checkout leak: {leak.get('step', 'unknown')} — {leak.get('drop_off_pct', 0)}% drop-off",
                f"Biggest checkout funnel leak losing potential revenue.",
                revenue=_dec(leak.get('revenue_impact', 1000)),
                action=leak.get('fix', 'Optimize checkout step to reduce abandonment'),
                effort=4.0, urgency='this_week',
                evidence=leak
            ))

        return insights

    def _extract_content_insights(self, data: Dict) -> List[Dict]:
        insights = []
        gaps = data.get('content_gaps', data.get('gaps', []))

        for gap in (gaps if isinstance(gaps, list) else [])[:3]:
            insights.append(self._make_insight(
                'content_gap', 'opportunity',
                gap.get('title', 'Content Gap'),
                gap.get('description', ''),
                revenue=_dec(gap.get('estimated_revenue', 0)),
                action=gap.get('recommendation', 'Create missing content'),
                effort=4.0, urgency='this_month',
                evidence=gap
            ))
        return insights

    def _extract_redirect_insights(self, data: Dict) -> List[Dict]:
        insights = []
        errors = data.get('top_404s', data.get('errors', []))
        revenue_impact = data.get('revenue_impact', {})

        total_lost = _dec(revenue_impact.get('monthly_revenue_loss', 0)) if isinstance(revenue_impact, dict) else 0
        if total_lost > 100:
            insights.append(self._make_insight(
                'redirect_health', 'issue',
                f'404 errors costing ${total_lost:,.0f}/month in lost revenue',
                f"Broken URLs losing traffic and sales.",
                revenue=total_lost,
                action='Set up redirects for the top 404 URLs to relevant product or category pages.',
                effort=2.0, urgency='this_week',
                evidence={'monthly_loss': total_lost}
            ))

        for err in (errors if isinstance(errors, list) else [])[:2]:
            url = err.get('url', err.get('page_path', 'unknown'))
            hits = err.get('hit_count', err.get('hits', 0))
            if hits > 10:
                insights.append(self._make_insight(
                    'redirect_health', 'issue',
                    f"404: {url} — {hits} hits/month",
                    f'Page returning 404 with significant traffic.',
                    revenue=hits * 2,
                    action=f'Create a 301 redirect from {url} to the closest matching live page.',
                    effort=0.5, urgency='this_week',
                    evidence={'url': url, 'hits': hits}
                ))

        return insights

    def _extract_ad_insights(self, data: Dict) -> List[Dict]:
        insights = []
        campaigns = data.get('campaigns', [])
        waste = data.get('waste', data.get('ad_waste', []))
        scaling = data.get('scaling_opportunities', data.get('scaling', []))

        for s in (scaling if isinstance(scaling, list) else [])[:2]:
            insights.append(self._make_insight(
                'ad_spend', 'opportunity',
                f"Scale: {s.get('campaign_name', 'Campaign')} — {s.get('roas', 0):.1f}x ROAS",
                f"High-performing campaign with room to scale budget.",
                revenue=_dec(s.get('potential_revenue', 500)),
                action=f"Increase budget by 20% for '{s.get('campaign_name', '')}'",
                effort=0.5, urgency='this_week',
                evidence=s
            ))

        for w in (waste if isinstance(waste, list) else [])[:2]:
            insights.append(self._make_insight(
                'ad_spend', 'issue',
                f"Ad waste: {w.get('type', w.get('waste_type', 'unknown'))} — ${_dec(w.get('monthly_waste', 0)):,.0f}/mo",
                w.get('description', 'Spend not generating returns'),
                cost=_dec(w.get('monthly_waste', 0)),
                action=w.get('recommendation', 'Reduce or reallocate this spend'),
                effort=1.0, urgency='this_week',
                evidence=w
            ))

        return insights

    def _extract_email_insights(self, data: Dict) -> List[Dict]:
        insights = []
        missing_flows = data.get('missing_flows', [])
        underperforming = data.get('underperforming_flows', data.get('underperforming', []))

        for flow in (missing_flows if isinstance(missing_flows, list) else [])[:2]:
            name = flow if isinstance(flow, str) else flow.get('name', 'Unknown Flow')
            insights.append(self._make_insight(
                'email', 'opportunity',
                f'Missing email flow: {name}',
                f'Standard e-commerce flow not set up — leaving revenue on the table.',
                revenue=500,
                action=f'Create a {name} email flow in Klaviyo with 3-email sequence.',
                effort=4.0, urgency='this_week',
            ))

        for flow in (underperforming if isinstance(underperforming, list) else [])[:2]:
            insights.append(self._make_insight(
                'email', 'issue',
                f"Underperforming: {flow.get('flow_name', flow.get('name', 'Flow'))}",
                f"Open rate or click rate below industry benchmark.",
                revenue=_dec(flow.get('revenue_gap', 200)),
                action=flow.get('recommendation', 'A/B test subject lines and CTAs'),
                effort=2.0, urgency='this_week',
                evidence=flow
            ))

        return insights

    def _extract_profitability_insights(self, data: Dict) -> List[Dict]:
        insights = []
        losing = data.get('losing_products', [])
        gems = data.get('hidden_gems', [])

        for p in (losing if isinstance(losing, list) else [])[:2]:
            insights.append(self._make_insight(
                'profitability', 'issue',
                f"Loss-maker: {p.get('title', p.get('product_name', 'Product'))}",
                f"This product loses money after COGS + ad spend + returns.",
                cost=abs(_dec(p.get('net_loss', p.get('net_profit', 0)))),
                action=f"Increase price, reduce ad spend, or delist '{p.get('title', '')}'",
                effort=1.0, urgency='this_week',
                evidence=p
            ))

        for g in (gems if isinstance(gems, list) else [])[:2]:
            insights.append(self._make_insight(
                'profitability', 'opportunity',
                f"Hidden gem: {g.get('title', g.get('product_name', 'Product'))}",
                f"High-margin product with low visibility — increase promotion.",
                revenue=_dec(g.get('potential_revenue', 300)),
                action=f"Promote '{g.get('title', '')}' in ad campaigns and email flows.",
                effort=2.0, urgency='this_week',
                evidence=g
            ))

        return insights

    def _extract_code_insights(self, data: Dict) -> List[Dict]:
        insights = []
        security = data.get('security_issues', data.get('security', []))
        debt = data.get('technical_debt', data.get('debt', []))

        for s in (security if isinstance(security, list) else [])[:2]:
            insights.append(self._make_insight(
                'code_health', 'issue',
                f"Security: {s.get('title', s.get('type', 'vulnerability'))}",
                s.get('description', 'Security vulnerability detected'),
                action=s.get('fix', 'Review and patch security issue'),
                effort=2.0, urgency='this_week',
                evidence=s
            ))
        return insights

    def _extract_dq_insights(self, data: Dict) -> List[Dict]:
        insights = []
        # DataQualityService returns 'quality_score' (not 'overall_score' or 'score')
        score = data.get('quality_score', data.get('overall_score', data.get('score', None)))
        if score is None:
            # No score available — don't generate a false "0/100" alarm
            return insights
        if isinstance(score, (int, float)) and score < 70:
            insights.append(self._make_insight(
                'data_quality', 'risk',
                f'Data quality score: {score}/100 — unreliable analytics',
                'Low data quality means insights may be inaccurate.',
                action='Fix tracking discrepancies between GA4, Google Ads, and Shopify.',
                effort=8.0, urgency='this_week',
                evidence={'score': score}
            ))
        return insights

    def _extract_journey_insights(self, data: Dict) -> List[Dict]:
        insights = []
        gateway = data.get('gateway_products', [])
        dead_ends = data.get('dead_end_products', [])

        for g in (gateway if isinstance(gateway, list) else [])[:2]:
            insights.append(self._make_insight(
                'journey', 'opportunity',
                f"Gateway product: {g.get('title', g.get('product_title', 'Product'))}",
                f"Customers who buy this first have high repeat rate.",
                revenue=_dec(g.get('estimated_ltv_gain', 300)),
                action=f"Feature '{g.get('title', '')}' in first-time buyer campaigns and homepage.",
                effort=2.0, urgency='this_week',
                evidence=g
            ))

        for d in (dead_ends if isinstance(dead_ends, list) else [])[:1]:
            insights.append(self._make_insight(
                'journey', 'risk',
                f"Dead-end product: {d.get('title', d.get('product_title', 'Product'))}",
                'Customers who buy this rarely return.',
                action=f"Add cross-sell recommendations to '{d.get('title', '')}' product page.",
                effort=1.0, urgency='this_month',
                evidence=d
            ))

        return insights

    def _extract_attribution_insights(self, data: Dict) -> List[Dict]:
        insights = []
        channels = data.get('channel_performance', data.get('channels', []))

        for ch in (channels if isinstance(channels, list) else [])[:2]:
            roas = _dec(ch.get('true_roas', ch.get('roas', 0)))
            name = ch.get('channel', ch.get('name', 'Channel'))
            if roas > 3:
                insights.append(self._make_insight(
                    'attribution', 'positive',
                    f'{name} — {roas:.1f}x true ROAS',
                    f'High-performing channel worth protecting.',
                    action=f'Maintain or increase investment in {name}.',
                    effort=0, urgency='ongoing',
                    evidence=ch
                ))
            elif roas < 1 and roas > 0:
                insights.append(self._make_insight(
                    'attribution', 'issue',
                    f'{name} — {roas:.1f}x ROAS (below breakeven)',
                    f'Channel not generating sufficient returns.',
                    cost=_dec(ch.get('spend', 0)) * (1 - roas),
                    action=f'Reduce spend on {name} or improve targeting.',
                    effort=2.0, urgency='this_week',
                    evidence=ch
                ))

        return insights

    # ------------------------------------------------------------------
    # CORRELATION DETECTION: Algorithmic cross-module patterns
    # ------------------------------------------------------------------

    def _detect_correlations(self, module_data: Dict, kpi: Dict) -> List[Dict]:
        """Detect cross-module correlations algorithmically."""
        correlations = []

        # 1. Pricing → Conversion chain
        pricing = module_data.get('pricing', {})
        below_min = kpi.get('below_minimum_skus', 0)
        losing = kpi.get('losing_money_skus', 0)
        conv_rate = kpi.get('conversion_rate_7d', 0)

        if below_min > 50 and conv_rate > 0:
            correlations.append({
                'correlation_type': 'causal_chain',
                'modules_involved': ['pricing', 'ga4'],
                'title': f'{below_min} below-minimum SKUs may be suppressing Google Shopping visibility',
                'narrative': (
                    f'With {below_min} products priced below supplier minimums and {losing} losing money, '
                    f'Google Merchant Center may be suppressing listings. This correlates with the current '
                    f'{conv_rate}% conversion rate.'
                ),
                'evidence': {'below_min': below_min, 'losing': losing, 'conv_rate': conv_rate},
                'confidence': 0.7,
                'revenue_impact': below_min * 10,
            })

        # 2. Inventory → Lost Sales
        oos = kpi.get('out_of_stock_skus', 0)
        oos_rate = kpi.get('oos_rate', 0)
        # Guardrail: only deny-policy SKUs should be counted. If rate exceeds 90%
        # and count exceeds 1000, the data is likely stale/unfiltered — skip.
        if oos_rate > 90 and oos > 1000:
            logger.warning(f"OOS guardrail: skipping inflated count {oos:,} ({oos_rate}%)")
            oos = 0
            oos_rate = 0
        if oos > 100:
            correlations.append({
                'correlation_type': 'causal_chain',
                'modules_involved': ['inventory', 'shopify'],
                'title': f'{oos:,} out-of-stock SKUs ({oos_rate}%) → lost sales opportunity',
                'narrative': (
                    f'{oos:,} SKUs are out of stock ({oos_rate}% of catalog). '
                    f'Customers landing on these pages cannot purchase, leading to lost revenue and poor SEO signals.'
                ),
                'evidence': {'oos_skus': oos, 'oos_rate': oos_rate},
                'confidence': 0.85,
                'revenue_impact': oos * 15,
            })

        # 3. Sessions → Revenue trend
        sessions_7d = kpi.get('sessions_7d', 0)
        sessions_prev = kpi.get('sessions_prev_7d', 0)
        rev_7d = kpi.get('revenue_7d', 0)
        rev_prev = kpi.get('revenue_prev_7d', 0)

        if sessions_prev > 0 and rev_prev > 0:
            session_change = (sessions_7d - sessions_prev) / sessions_prev * 100
            rev_change = (rev_7d - rev_prev) / rev_prev * 100 if rev_prev > 0 else 0

            if abs(session_change) > 5 or abs(rev_change) > 5:
                direction = 'up' if session_change > 0 else 'down'
                correlations.append({
                    'correlation_type': 'co_occurrence',
                    'modules_involved': ['ga4', 'shopify'],
                    'title': f'Traffic {direction} {abs(session_change):.0f}% WoW → Revenue {"up" if rev_change > 0 else "down"} {abs(rev_change):.0f}%',
                    'narrative': (
                        f'Sessions changed {session_change:+.0f}% ({sessions_prev:,} → {sessions_7d:,}) '
                        f'while revenue changed {rev_change:+.0f}% (${rev_prev:,.0f} → ${rev_7d:,.0f}).'
                    ),
                    'evidence': {
                        'sessions_7d': sessions_7d, 'sessions_prev': sessions_prev,
                        'revenue_7d': rev_7d, 'revenue_prev': rev_prev,
                        'session_change_pct': round(session_change, 1),
                        'revenue_change_pct': round(rev_change, 1),
                    },
                    'confidence': 0.9,
                    'revenue_impact': abs(rev_7d - rev_prev),
                })

        # 4. Customer risk → Revenue threat
        at_risk = kpi.get('at_risk_customers', 0)
        total_cust = kpi.get('total_customers', 0)
        if at_risk > 500 and total_cust > 0:
            risk_pct = at_risk / total_cust * 100
            correlations.append({
                'correlation_type': 'amplifying',
                'modules_involved': ['customer', 'shopify'],
                'title': f'{at_risk:,} at-risk customers ({risk_pct:.1f}%) threaten recurring revenue',
                'narrative': (
                    f'{at_risk:,} of {total_cust:,} customers are flagged as at-risk. '
                    f'Without intervention, this threatens ${at_risk * 50:,.0f} in annual revenue.'
                ),
                'evidence': {'at_risk': at_risk, 'total': total_cust, 'risk_pct': round(risk_pct, 1)},
                'confidence': 0.75,
                'revenue_impact': at_risk * 50,
            })

        # 5. Anomalies correlating with revenue
        active_anomalies = kpi.get('active_anomalies', 0)
        if active_anomalies > 0:
            correlations.append({
                'correlation_type': 'causal_chain',
                'modules_involved': ['ml_intelligence', 'shopify'],
                'title': f'{active_anomalies} active anomalies detected — potential revenue impact',
                'narrative': f'ML anomaly detection found {active_anomalies} unacknowledged anomalies that may be affecting performance.',
                'evidence': {'active_anomalies': active_anomalies},
                'confidence': 0.65,
                'revenue_impact': active_anomalies * 500,
            })

        return correlations

    # ------------------------------------------------------------------
    # LLM CONTEXT BUILDING
    # ------------------------------------------------------------------

    def _build_llm_context_package(self, cadence: str, all_insights: List[Dict],
                                    kpi: Dict, correlations: List[Dict],
                                    module_meta: Dict) -> Dict[str, str]:
        """Build curated context strings for each LLM call."""

        # --- Context 1: Executive Pulse + Priorities ---
        priorities_lines = [f"BUSINESS SNAPSHOT ({cadence} brief):"]
        priorities_lines.append(f"  Revenue (7d): ${kpi.get('revenue_7d', 0):,.0f}")
        priorities_lines.append(f"  Orders (7d): {kpi.get('orders_7d', 0):,}")
        priorities_lines.append(f"  AOV (7d): ${kpi.get('aov_7d', 0):,.2f}")
        priorities_lines.append(f"  Conversion Rate (7d): {kpi.get('conversion_rate_7d', 0):.2f}%")
        priorities_lines.append(f"  Sessions (7d): {kpi.get('sessions_7d', 0):,}")
        priorities_lines.append(f"  Cart Abandonment: {kpi.get('cart_abandonment_rate', 0):.1f}%")
        priorities_lines.append(f"  At-Risk Customers: {kpi.get('at_risk_customers', 0):,}")
        priorities_lines.append(f"  Out-of-Stock SKUs: {kpi.get('out_of_stock_skus', 0):,}")
        priorities_lines.append(f"  Below-Minimum Price SKUs: {kpi.get('below_minimum_skus', 0)}")
        priorities_lines.append(f"  Active Anomalies: {kpi.get('active_anomalies', 0)}")

        # WoW changes
        s7 = kpi.get('sessions_7d', 0)
        sp = kpi.get('sessions_prev_7d', 0)
        if sp > 0:
            priorities_lines.append(f"\nWEEK-OVER-WEEK CHANGES:")
            priorities_lines.append(f"  Sessions: {(s7-sp)/sp*100:+.1f}%")
        r7 = kpi.get('revenue_7d', 0)
        rp = kpi.get('revenue_prev_7d', 0)
        if rp > 0:
            priorities_lines.append(f"  Revenue: {(r7-rp)/rp*100:+.1f}%")

        # Top insights by revenue impact
        priorities_lines.append(f"\nTOP ISSUES & OPPORTUNITIES (ranked by revenue impact):")
        for i, ins in enumerate(all_insights[:15], 1):
            rev = _dec(ins.get('revenue_impact', 0))
            cost = _dec(ins.get('cost_impact', 0))
            impact = f"${rev:,.0f} revenue" if rev > 0 else f"${cost:,.0f} cost"
            priorities_lines.append(
                f"  {i}. [{ins['source_module']}] {ins['title']} — {impact}"
            )
            if ins.get('suggested_action'):
                priorities_lines.append(f"     Action: {ins['suggested_action'][:150]}")

        # Correlations
        if correlations:
            priorities_lines.append(f"\nCROSS-MODULE CORRELATIONS:")
            for c in correlations[:5]:
                priorities_lines.append(f"  - {c['title']}")
                priorities_lines.append(f"    {c['narrative'][:200]}")

        priorities_lines.append(f"\nModules reporting: {len(module_meta.get('succeeded', []))}/16")
        failed = module_meta.get('failed', [])
        if failed:
            priorities_lines.append(f"Modules failed: {', '.join(f.get('module', str(f)) if isinstance(f, dict) else str(f) for f in failed)}")

        # Stale data source warnings (Req 6 — LLM should factor these into confidence)
        if hasattr(self, '_freshness_cache'):
            stale_sources = [k for k, v in self._freshness_cache.items() if v.get('is_stale')]
            if stale_sources:
                priorities_lines.append(f"\nSTALE DATA SOURCES (reduce confidence for affected recommendations): {', '.join(stale_sources)}")

        priorities_context = '\n'.join(priorities_lines)

        # --- Context 2: CRO Analysis ---
        cro_lines = ["CONVERSION RATE OPTIMIZATION DATA:"]
        cro_lines.append(f"\nFUNNEL (last 7 days):")
        cro_lines.append(f"  Sessions: {kpi.get('sessions_7d', 0):,}")
        cro_lines.append(f"  Items Viewed: {kpi.get('items_viewed_7d', 0):,}")
        cro_lines.append(f"  Add to Cart: {kpi.get('add_to_carts_7d', 0):,}")
        cro_lines.append(f"  Checkouts: {kpi.get('checkouts_7d', 0):,}")
        cro_lines.append(f"  Purchases: {kpi.get('purchases_7d', 0):,}")
        cro_lines.append(f"  Conversion Rate: {kpi.get('conversion_rate_7d', 0):.2f}%")
        cro_lines.append(f"  View-to-Cart Rate: {kpi.get('view_to_cart_rate', 0):.2f}%")
        cro_lines.append(f"  Cart Abandonment: {kpi.get('cart_abandonment_rate', 0):.1f}%")
        cro_lines.append(f"  AOV: ${kpi.get('aov_7d', 0):,.2f}")
        cro_lines.append(f"  Bounce Rate: {kpi.get('bounce_rate_today', 0):.1f}%")

        # CRO-related insights
        cro_insights = [i for i in all_insights if i['source_module'] in ('behavior', 'content_gap', 'redirect_health', 'pricing')]
        if cro_insights:
            cro_lines.append(f"\nCRO-RELEVANT ISSUES:")
            for ins in cro_insights[:10]:
                cro_lines.append(f"  - [{ins['source_module']}] {ins['title']}")
                if ins.get('suggested_action'):
                    cro_lines.append(f"    Fix: {ins['suggested_action'][:150]}")

        cro_context = '\n'.join(cro_lines)

        # --- Context 3: Issue Triage ---
        issues_lines = ["ALL DETECTED ISSUES ACROSS MODULES:"]
        issue_insights = [i for i in all_insights if i['insight_type'] in ('issue', 'risk')]
        for i, ins in enumerate(issue_insights[:20], 1):
            rev = _dec(ins.get('revenue_impact', 0))
            cost = _dec(ins.get('cost_impact', 0))
            issues_lines.append(
                f"\n{i}. [{ins['source_module'].upper()}] {ins['title']}"
            )
            issues_lines.append(f"   Description: {ins['description'][:200]}")
            if rev > 0:
                issues_lines.append(f"   Revenue at risk: ${rev:,.0f}")
            if cost > 0:
                issues_lines.append(f"   Cost waste: ${cost:,.0f}")
            if ins.get('suggested_action'):
                issues_lines.append(f"   Suggested fix: {ins['suggested_action'][:200]}")
            issues_lines.append(f"   Effort: {ins.get('effort_hours', '?')} hours | Urgency: {ins.get('urgency', 'this_week')}")

        issues_context = '\n'.join(issues_lines)

        # --- Context 4: Strategic Insights ---
        strat_lines = ["CROSS-MODULE STRATEGIC DATA:"]
        strat_lines.append(f"\nKPI SNAPSHOT:")
        for k, v in kpi.items():
            if isinstance(v, (int, float)):
                strat_lines.append(f"  {k}: {v:,.2f}" if isinstance(v, float) else f"  {k}: {v:,}")

        if correlations:
            strat_lines.append(f"\nDETECTED CORRELATIONS:")
            for c in correlations:
                strat_lines.append(f"  [{c['correlation_type']}] {c['title']}")
                strat_lines.append(f"    {c['narrative'][:300]}")
                strat_lines.append(f"    Confidence: {c['confidence']:.0%} | Revenue impact: ${_dec(c.get('revenue_impact', 0)):,.0f}")

        # Positive signals
        positives = [i for i in all_insights if i['insight_type'] == 'positive']
        if positives:
            strat_lines.append(f"\nWHAT'S WORKING WELL:")
            for p in positives[:5]:
                strat_lines.append(f"  - [{p['source_module']}] {p['title']}")

        # Top opportunities
        opps = [i for i in all_insights if i['insight_type'] == 'opportunity']
        if opps:
            strat_lines.append(f"\nTOP OPPORTUNITIES:")
            for o in opps[:5]:
                strat_lines.append(f"  - [{o['source_module']}] {o['title']} — ${_dec(o.get('revenue_impact', 0)):,.0f}")

        strategic_context = '\n'.join(strat_lines)

        # --- Context 5: Growth Playbook (weekly only) ---
        growth_lines = ["GROWTH & SALES DATA:"]
        growth_lines.append(f"\nREVENUE SUMMARY:")
        growth_lines.append(f"  Last 7 days: ${kpi.get('revenue_7d', 0):,.0f}")
        growth_lines.append(f"  Last 30 days: ${kpi.get('revenue_30d', 0):,.0f}")
        growth_lines.append(f"  Orders (30d): {kpi.get('orders_30d', 0):,}")
        growth_lines.append(f"  AOV (30d): ${kpi.get('aov_30d', 0):,.2f}")
        growth_lines.append(f"  New customers this month: {kpi.get('new_customers_month', 0):,}")
        growth_lines.append(f"  Total customers: {kpi.get('total_customers', 0):,}")

        # Revenue opportunities
        rev_opps = sorted(all_insights, key=lambda x: _dec(x.get('revenue_impact', 0)), reverse=True)
        growth_lines.append(f"\nTOP REVENUE OPPORTUNITIES:")
        total_opp = 0
        for o in rev_opps[:10]:
            rev = _dec(o.get('revenue_impact', 0))
            if rev > 0:
                total_opp += rev
                growth_lines.append(f"  - [{o['source_module']}] {o['title']} — ${rev:,.0f}")
                if o.get('suggested_action'):
                    growth_lines.append(f"    Action: {o['suggested_action'][:150]}")
        growth_lines.append(f"\n  TOTAL OPPORTUNITY: ${total_opp:,.0f}")

        growth_context = '\n'.join(growth_lines)

        return {
            'priorities_context': priorities_context,
            'cro_context': cro_context,
            'issues_context': issues_context,
            'strategic_context': strategic_context,
            'growth_context': growth_context,
        }

    # ------------------------------------------------------------------
    # 5 TARGETED LLM CALLS
    # ------------------------------------------------------------------

    def _llm_call_executive_pulse(self, context: str, cadence: str) -> Dict:
        """LLM Call 1: Executive Pulse + Today's Priorities."""
        if not self.llm.enabled:
            return self._fallback_pulse(context)

        prompt = f"""You are the Chief Revenue Officer of a bathroom & kitchen appliances e-commerce business (Cass Brothers).
You are delivering the {cadence} intelligence briefing to the business owner.

{context}

Generate a JSON response with this EXACT structure (valid JSON only, no markdown):
{{
  "pulse": "2-3 sentence executive summary of the business state right now. Be specific with numbers.",
  "health_status": "thriving|stable|at_risk|critical",
  "priorities": [
    {{
      "rank": 1,
      "title": "Short decision title",
      "why": "Why this matters NOW — specific data evidence and time pressure",
      "action": "The EXACT steps to execute. Name SKUs, URLs, campaigns, dollar amounts",
      "estimated_impact": "$X,XXX/week or /month",
      "effort": "X hours",
      "responsible_team": "marketing|dev|ops|content",
      "urgency": "immediate|this_week|this_month",
      "source_modules": ["module1", "module2"],
      "confidence_note": "What data this relies on and any caveats (stale sources, missing data)"
    }}
  ],
  "protect": ["Things working well that should NOT be changed"],
  "watch": ["Emerging concerns to monitor this week"]
}}

RULES:
1. Return ONLY valid JSON — no markdown, no explanation
2. Include exactly 3 priorities — the TOP 3 DECISIONS the owner must make, ranked by impact x confidence x urgency
3. Every decision must name specific products, URLs, campaigns, or SKUs — never say "improve X"
4. Every decision must have a dollar estimate based on the actual data provided
5. Every decision must specify responsible_team and source_modules
6. If any data source is marked STALE above, note this in confidence_note for affected decisions
7. The pulse must tell a story: what happened, what it means, what to do
8. These are DECISIONS requiring action, not observations or narratives"""

        try:
            response = self.llm.client.messages.create(
                model='claude-sonnet-4-20250514',
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            # Parse JSON from response
            return self._parse_json_response(text)
        except Exception as e:
            logger.error(f"LLM executive pulse failed: {e}")
            return self._fallback_pulse(context)

    def _llm_call_cro_analysis(self, context: str) -> str:
        """LLM Call 2: Conversion Rate Optimization deep-dive."""
        if not self.llm.enabled:
            return self._fallback_cro(context)

        prompt = f"""You are a world-class Conversion Rate Optimization consultant analyzing Cass Brothers, a bathroom & kitchen appliances e-commerce site.

{context}

Produce a detailed CRO analysis in markdown with these sections:

## Conversion Scorecard
Grade each funnel stage A-F based on the data. Format as a simple table.

## The 3 Biggest Conversion Killers
For each:
- **What the data shows**: Specific numbers
- **Why it's happening**: Root cause analysis
- **Exact fix**: Step-by-step implementation (name specific URLs, page elements, etc.)
- **Expected lift**: % improvement and $ impact

## Quick Wins (fix in under 1 hour)
3-5 specific changes that can be made immediately. Each must name the page/element.

## Mobile-Specific Fixes
Dedicated mobile CRO recommendations based on the data.

## A/B Test Recommendations
What to test, hypothesis, expected impact.

RULES:
- Be specific: name URLs, page elements, button text, form fields
- Every recommendation must have a dollar estimate
- Focus on the biggest revenue opportunities first
- Use the actual numbers from the data provided"""

        try:
            response = self.llm.client.messages.create(
                model='claude-sonnet-4-20250514',
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"LLM CRO analysis failed: {e}")
            return self._fallback_cro(context)

    def _llm_call_issue_triage(self, context: str) -> str:
        """LLM Call 3: Issue Command Center triage."""
        if not self.llm.enabled:
            return self._fallback_issues(context)

        prompt = f"""You are the Operations Commander for Cass Brothers e-commerce. Your job is to triage EVERY issue found across the platform and provide specific fixes.

{context}

Produce a markdown issue triage. For EACH issue:

### [SEVERITY] Issue Title
- **Module**: Where this was detected
- **Problem**: What is wrong (with data evidence)
- **Root Cause**: Why it's happening
- **Fix**: Exact steps to resolve (be specific — name the SKU, URL, setting, or configuration)
- **Revenue Impact**: $ estimate
- **Effort**: Hours and who (dev/marketing/ops)
- **Priority**: Do today / This week / This month

SEVERITY LEVELS: CRITICAL (fix today), HIGH (fix this week), MEDIUM (fix this month), LOW (backlog)

Sort issues by severity then by revenue impact. Be specific and actionable for every fix."""

        try:
            response = self.llm.client.messages.create(
                model='claude-sonnet-4-20250514',
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"LLM issue triage failed: {e}")
            return self._fallback_issues(context)

    def _llm_call_strategic_insights(self, context: str) -> str:
        """LLM Call 4: Cross-module strategic insights."""
        if not self.llm.enabled:
            return self._fallback_strategic(context)

        prompt = f"""You are a senior strategic advisor with access to every data source in the Cass Brothers e-commerce business.
Your job is to find insights that CROSS module boundaries — things no single dashboard would show.

{context}

Produce exactly 5 strategic insights in markdown. For each:

### Insight: [Title]
**The Pattern:** What data from which modules reveals this
**Why It Matters:** Business impact, quantified in dollars
**The Play:** Specific multi-step action plan (not vague — name products, campaigns, segments)
**Expected Outcome:** $ impact and timeline

Focus on:
1. Hidden revenue the business is leaving on the table
2. Emerging threats about to become problems
3. Customer lifecycle optimization (moving customers up the value ladder)
4. Cross-channel synergies (how one channel can amplify another)
5. Competitive positioning moves (pricing/product strategies)

RULES: Be specific. Name products, brands, customer segments, dollar amounts. No vague platitudes."""

        try:
            response = self.llm.client.messages.create(
                model='claude-sonnet-4-20250514',
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"LLM strategic insights failed: {e}")
            return self._fallback_strategic(context)

    def _llm_call_growth_playbook(self, context: str) -> str:
        """LLM Call 5: Growth playbook (weekly only)."""
        if not self.llm.enabled:
            return self._fallback_growth(context)

        prompt = f"""You are the Head of Growth for Cass Brothers, a bathroom & kitchen appliances e-commerce business.
You have a quarterly revenue growth target. Design this week's growth playbook.

{context}

Produce a markdown growth playbook with:

## This Week's Growth Target
Based on the data, what's a realistic revenue goal for this week?

## 7-Day Action Plan
For each day (Monday through Sunday):
- **Focus**: What to work on
- **Specific Action**: Exact steps (name campaigns, products, segments)
- **Expected Impact**: $ estimate

## Top 5 Revenue Opportunities
Ranked by potential impact. Each with:
- What it is
- Specific implementation steps
- Expected revenue lift
- Timeline to see results

## Sales Growth Levers
Which levers (traffic, conversion, AOV, frequency) offer the most upside? Quantify each.

## Quick Revenue Wins
3 things that can generate additional revenue within 48 hours.

RULES: Every recommendation must have specific product names, dollar amounts, and implementation steps. No vague advice."""

        try:
            response = self.llm.client.messages.create(
                model='claude-sonnet-4-20250514',
                max_tokens=2500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            logger.error(f"LLM growth playbook failed: {e}")
            return self._fallback_growth(context)

    # ------------------------------------------------------------------
    # FALLBACKS: When LLM is unavailable
    # ------------------------------------------------------------------

    def _fallback_pulse(self, context: str) -> Dict:
        return {
            'pulse': 'LLM analysis unavailable. Review the data-driven insights below for priorities.',
            'health_status': 'stable',
            'priorities': [],
            'protect': [],
            'watch': [],
        }

    def _fallback_cro(self, context: str) -> str:
        return '## CRO Analysis\n\nLLM analysis unavailable. Review the conversion funnel data in the KPI section above.'

    def _fallback_issues(self, context: str) -> str:
        return '## Issue Triage\n\nLLM analysis unavailable. Issues are listed by module in the data sections below.'

    def _fallback_strategic(self, context: str) -> str:
        return '## Strategic Insights\n\nLLM analysis unavailable. Review cross-module correlations in the data section.'

    def _fallback_growth(self, context: str) -> str:
        return '## Growth Playbook\n\nLLM analysis unavailable. Review revenue opportunities in the data section.'

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _parse_json_response(self, text: str) -> Dict:
        """Parse JSON from LLM response, handling markdown wrapping."""
        # Strip markdown code fences if present
        if '```json' in text:
            text = text.split('```json')[1].split('```')[0].strip()
        elif '```' in text:
            text = text.split('```')[1].split('```')[0].strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to find JSON object in the text
            start = text.find('{')
            end = text.rfind('}')
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    pass
            logger.warning("Failed to parse LLM JSON response")
            return self._fallback_pulse('')

    def _compute_data_quality(self, meta: Dict) -> int:
        """Compute data quality score from module success rate."""
        total = len(meta.get('queried', []))
        succeeded = len(meta.get('succeeded', []))
        if total == 0:
            return 0
        return int(succeeded / total * 100)

    # ------------------------------------------------------------------
    # DECISION-LAYER HELPERS (Reqs 1-6)
    # ------------------------------------------------------------------

    def _get_module_freshness(self) -> Dict[str, Dict]:
        """Query DataSyncStatus for per-source freshness (cached on instance)."""
        if hasattr(self, '_freshness_cache'):
            return self._freshness_cache

        freshness: Dict[str, Dict] = {}
        now = datetime.utcnow()
        try:
            statuses = self.db.query(DataSyncStatus).all()
            for s in statuses:
                name = s.source_name
                last = s.last_successful_sync
                lag = (now - last).total_seconds() / 3600 if last else None
                threshold = _STALE_THRESHOLDS.get(name, 48)
                freshness[name] = {
                    'last_sync': last.isoformat() if last else None,
                    'lag_hours': round(lag, 1) if lag is not None else None,
                    'is_stale': lag > threshold if lag is not None else True,
                    'health_score': s.health_score or 0,
                }
        except Exception as e:
            logger.warning(f"Failed to query DataSyncStatus: {e}")

        self._freshness_cache = freshness
        return freshness

    def _check_degraded_state(self, module_meta: Dict,
                              freshness: Dict) -> Tuple[bool, List[Dict]]:
        """Determine if the brief is degraded (>30% modules stale/failed)."""
        stale: List[Dict] = []
        failed_names = set()
        for f in module_meta.get('failed', []):
            failed_names.add(f['module'] if isinstance(f, dict) else f)

        for name in module_meta.get('succeeded', []):
            deps = _MODULE_DATA_DEPS.get(name, [])
            for dep in deps:
                info = freshness.get(dep, {})
                if info.get('is_stale', True) and dep in freshness:
                    stale.append({
                        'module': name, 'source': dep,
                        'lag_hours': info.get('lag_hours'),
                        'last_sync': info.get('last_sync'),
                    })
                    break

        total_queried = len(module_meta.get('queried', []))
        stale_or_failed = len({s['module'] for s in stale}) + len(failed_names)
        is_degraded = total_queried > 0 and (stale_or_failed / total_queried) > 0.30
        return is_degraded, stale

    def _compute_priority_score(self, insight: Dict) -> float:
        """Compute priority_score = impact * confidence * urgency_weight."""
        impact = _dec(insight.get('revenue_impact', 0)) + _dec(insight.get('cost_impact', 0))
        confidence = float(insight.get('confidence', 0.5))
        urgency = insight.get('urgency', 'this_week')
        urgency_weight = _URGENCY_WEIGHTS.get(urgency, 1)
        return round(impact * confidence * urgency_weight, 2)

    def _generate_dedup_hash(self, category: str, title: str) -> str:
        """SHA256 hash from category + normalized title keywords."""
        stop_words = {'the', 'a', 'an', 'is', 'are', 'to', 'for', 'and', 'or', 'in', 'on', 'of', 'at'}
        words = [w.lower() for w in (title or '').split() if w.lower() not in stop_words]
        key_words = sorted(words[:6])
        content = f"{category}:{' '.join(key_words)}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def _is_cross_functional(self, insight: Dict) -> bool:
        """True if the insight is cross-functional (strategic), False if single-brand operational."""
        action = (insight.get('suggested_action', '') or '').lower()
        title = (insight.get('title', '') or '').lower()
        # Single-brand operational actions belong in Brand Intelligence
        single_brand = ['restock', 'fix pricing for', 'raise price on', 'delist',
                        'discontinue', 'reprice', 'relist']
        if any(sig in action for sig in single_brand):
            return False
        return True

    def _dedup_against_brand_intel(self, insights: List[Dict]) -> List[Dict]:
        """Filter out insights that duplicate Brand Intelligence recommendations."""
        try:
            from app.services.brand_intelligence_service import BrandIntelligenceService
            bi = BrandIntelligenceService(self.db)
            dashboard = bi.get_dashboard(period_days=30)
            bi_brands = dashboard.get('brands', [])
            # Collect recommendation actions from all brands' detail views would be
            # too expensive — instead, hash brand names + known patterns
            brand_names = {b['brand'].lower() for b in bi_brands if b.get('brand')}

            deduped = []
            for insight in insights:
                is_cross = self._is_cross_functional(insight)
                insight['_is_cross_functional'] = is_cross
                insight['_dedup_hash'] = self._generate_dedup_hash(
                    insight.get('source_module', ''), insight.get('title', ''))

                # Keep if cross-functional; skip single-brand-specific ops
                if is_cross:
                    deduped.append(insight)
                else:
                    logger.debug(f"Deduped (single-brand): {insight.get('title')}")
            return deduped
        except Exception as e:
            logger.warning(f"Brand Intel dedup skipped: {e}")
            for insight in insights:
                insight['_is_cross_functional'] = True
                insight['_dedup_hash'] = self._generate_dedup_hash(
                    insight.get('source_module', ''), insight.get('title', ''))
            return insights

    def _derive_due_date(self, urgency: str) -> date:
        """Convert urgency to a concrete due date."""
        today = date.today()
        days_map = {
            'immediate': 1, 'today': 1, 'this_week': 7,
            'this_month': 30, 'ongoing': 90, 'quarterly': 90,
        }
        return today + timedelta(days=days_map.get(urgency, 7))

    @staticmethod
    def _parse_dollar_amount(s: str) -> float:
        """Parse '$1,234/week' or '$5,000/month' to float."""
        match = _re.search(r'\$([\d,]+)', str(s or ''))
        if match:
            return float(match.group(1).replace(',', ''))
        return 0.0

    @staticmethod
    def _parse_effort_hours(s: str) -> float:
        """Parse '4 hours' or '2h' to float."""
        match = _re.search(r'([\d.]+)', str(s or ''))
        if match:
            return float(match.group(1))
        return 2.0

    def _save_brief(self, data: Dict) -> None:
        """Persist the brief to the database."""
        try:
            # Mark previous briefs of same cadence as not current
            self.db.query(StrategicBrief).filter(
                StrategicBrief.cadence == data['cadence'],
                StrategicBrief.is_current == True
            ).update({'is_current': False})

            brief = StrategicBrief(
                cadence=data['cadence'],
                brief_date=date.fromisoformat(data['brief_date']),
                week_start_date=date.fromisoformat(data['week_start_date']) if data.get('week_start_date') else None,
                week_end_date=date.fromisoformat(data['week_end_date']) if data.get('week_end_date') else None,
                modules_queried=data.get('modules_queried'),
                modules_succeeded=data.get('modules_succeeded'),
                modules_failed=data.get('modules_failed'),
                data_quality_score=data.get('data_quality_score', 0),
                kpi_snapshot=data.get('kpi_snapshot'),
                executive_pulse=data.get('executive_pulse', ''),
                health_status=data.get('health_status', 'stable'),
                todays_priorities=data.get('todays_priorities'),
                conversion_analysis=data.get('conversion_analysis'),
                growth_playbook=data.get('growth_playbook'),
                cross_module_correlations=data.get('cross_module_correlations'),
                issue_command_center=data.get('issue_command_center'),
                ai_strategic_insights=data.get('ai_strategic_insights'),
                whats_working=data.get('whats_working'),
                watch_list=data.get('watch_list'),
                total_opportunity_value=data.get('total_opportunity_value', 0),
                total_issues_identified=data.get('total_issues_identified', 0),
                total_quick_wins=data.get('total_quick_wins', 0),
                is_current=True,
                generation_time_seconds=data.get('generation_time_seconds', 0),
                llm_calls_made=data.get('llm_calls_made', 0),
                llm_tokens_used=data.get('llm_tokens_used', 0),
                # Decision-layer fields (Req 6)
                is_degraded=data.get('is_degraded', False),
                stale_modules=data.get('stale_modules'),
                module_freshness=data.get('module_freshness'),
            )
            self.db.add(brief)
            self.db.flush()

            # Save recommendations from priorities — properly populated
            freshness = getattr(self, '_freshness_cache', {})
            kpi_snapshot = data.get('kpi_snapshot', {})

            # Hard cap to 3 decisions (LLM sometimes returns more)
            raw_priorities = (data.get('todays_priorities') or [])[:3]
            enriched_priorities = []

            for p in raw_priorities:
                if isinstance(p, dict):
                    urgency = p.get('urgency', 'this_week')
                    due = self._derive_due_date(urgency)
                    impact_val = self._parse_dollar_amount(p.get('estimated_impact', '$0'))
                    effort_h = self._parse_effort_hours(p.get('effort', '2 hours'))

                    # Source modules from LLM or fallback extraction
                    source_mods = p.get('source_modules', [])
                    if not source_mods:
                        text_blob = (p.get('why', '') + ' ' + p.get('action', '')).lower()
                        for mod_name in _MODULE_DATA_DEPS:
                            if mod_name.replace('_', ' ') in text_blob or mod_name in text_blob:
                                source_mods.append(mod_name)

                    # Data-as-of per recommendation (Req 2)
                    data_as_of = {}
                    for mod in source_mods:
                        for dep in _MODULE_DATA_DEPS.get(mod, []):
                            info = freshness.get(dep, {})
                            if info.get('last_sync'):
                                data_as_of[dep] = info['last_sync']

                    # Confidence from data staleness (Req 2)
                    stale_count = sum(1 for dep in data_as_of
                                      if freshness.get(dep, {}).get('is_stale', True))
                    missing_count = sum(1 for mod in source_mods
                                        for dep in _MODULE_DATA_DEPS.get(mod, [])
                                        if dep not in freshness)
                    if missing_count > 0:
                        conf = 0.4
                    elif stale_count > 0:
                        conf = 0.6
                    else:
                        conf = 0.9

                    # Priority score (Req 5)
                    urgency_w = _URGENCY_WEIGHTS.get(urgency, 1)
                    pscore = round(impact_val * conf * urgency_w, 2)

                    # Baseline metric (Req 4)
                    baseline_name = 'revenue_7d'
                    baseline_val = float(kpi_snapshot.get('revenue_7d', 0) or 0)
                    target_val = round(baseline_val + impact_val, 2)

                    team = p.get('responsible_team', 'marketing')
                    due_iso = due.isoformat() if due else None

                    rec = BriefRecommendation(
                        brief_id=brief.id,
                        category='decision',
                        priority_rank=p.get('rank', 0),
                        priority_level='critical' if p.get('rank', 99) == 1 else (
                            'high' if p.get('rank', 99) <= 2 else 'medium'),
                        title=p.get('title', ''),
                        problem_statement=p.get('why', ''),
                        specific_solution=p.get('action', ''),
                        estimated_revenue_impact=impact_val,
                        impact_timeframe=urgency,
                        confidence_score=conf,
                        source_modules=source_mods,
                        effort_hours=effort_h,
                        effort_level=p.get('effort', 'medium'),
                        responsible_team=team,
                        status='new',
                        due_date=due,
                        data_as_of=data_as_of,
                        priority_score=pscore,
                        urgency_weight=urgency_w,
                        baseline_metric_name=baseline_name,
                        baseline_metric_value=baseline_val,
                        target_metric_value=target_val,
                        dedup_hash=self._generate_dedup_hash(urgency, p.get('title', '')),
                        is_cross_functional=True,
                    )
                    self.db.add(rec)

                    # Enrich the priority dict so frontend can render
                    # all decision-layer fields without a DB join
                    enriched = dict(p)
                    enriched.update({
                        'source_modules': source_mods,
                        'data_as_of': data_as_of,
                        'confidence_score': conf,
                        'priority_score': pscore,
                        'responsible_team': team,
                        'due_date': due_iso,
                        'baseline_metric_name': baseline_name,
                        'baseline_metric_value': baseline_val,
                        'target_metric_value': target_val,
                        'status': 'new',
                    })
                    enriched_priorities.append(enriched)

            # Overwrite todays_priorities on the brief with enriched version
            brief.todays_priorities = enriched_priorities

            # Save correlations
            for c in (data.get('cross_module_correlations') or []):
                if isinstance(c, dict):
                    corr = BriefCorrelation(
                        brief_id=brief.id,
                        correlation_type=c.get('correlation_type', 'co_occurrence'),
                        modules_involved=c.get('modules_involved', []),
                        title=c.get('title', ''),
                        narrative=c.get('narrative', ''),
                        evidence=c.get('evidence'),
                        confidence=c.get('confidence', 0.5),
                        revenue_impact=c.get('revenue_impact', 0),
                    )
                    self.db.add(corr)

            self.db.commit()
            logger.info(f"Saved {data['cadence']} brief for {data['brief_date']}")

        except Exception as e:
            logger.error(f"Failed to save brief: {e}")
            self.db.rollback()

    def _brief_to_dict(self, brief: StrategicBrief) -> Dict:
        """Convert a StrategicBrief model to a response dict."""
        # Get recommendations
        recs = self.db.query(BriefRecommendation).filter(
            BriefRecommendation.brief_id == brief.id
        ).order_by(BriefRecommendation.priority_rank).all()

        corrs = self.db.query(BriefCorrelation).filter(
            BriefCorrelation.brief_id == brief.id
        ).all()

        return {
            'id': brief.id,
            'cadence': brief.cadence,
            'brief_date': brief.brief_date.isoformat() if brief.brief_date else None,
            'week_start_date': brief.week_start_date.isoformat() if brief.week_start_date else None,
            'week_end_date': brief.week_end_date.isoformat() if brief.week_end_date else None,
            'modules_queried': brief.modules_queried,
            'modules_succeeded': brief.modules_succeeded,
            'modules_failed': brief.modules_failed,
            'data_quality_score': brief.data_quality_score,
            'kpi_snapshot': brief.kpi_snapshot,
            'executive_pulse': brief.executive_pulse,
            'health_status': brief.health_status,
            'todays_priorities': brief.todays_priorities,
            'conversion_analysis': brief.conversion_analysis,
            'growth_playbook': brief.growth_playbook,
            'cross_module_correlations': [self._corr_to_dict(c) for c in corrs],
            'issue_command_center': brief.issue_command_center,
            'ai_strategic_insights': brief.ai_strategic_insights,
            'whats_working': brief.whats_working,
            'watch_list': brief.watch_list,
            'total_opportunity_value': _dec(brief.total_opportunity_value),
            'total_issues_identified': brief.total_issues_identified,
            'total_quick_wins': brief.total_quick_wins,
            'recommendations': [self._rec_to_dict(r) for r in recs],
            'generation_time_seconds': brief.generation_time_seconds,
            'llm_calls_made': brief.llm_calls_made,
            'generated_at': brief.generated_at.isoformat() if brief.generated_at else None,
            # Decision-layer fields
            'is_degraded': brief.is_degraded,
            'stale_modules': brief.stale_modules,
            'module_freshness': brief.module_freshness,
        }

    def _rec_to_dict(self, rec: BriefRecommendation) -> Dict:
        return {
            'id': rec.id,
            'category': rec.category,
            'priority_rank': rec.priority_rank,
            'priority_level': rec.priority_level,
            'title': rec.title,
            'problem_statement': rec.problem_statement,
            'root_cause': rec.root_cause,
            'specific_solution': rec.specific_solution,
            'implementation_steps': rec.implementation_steps,
            'estimated_revenue_impact': _dec(rec.estimated_revenue_impact),
            'estimated_cost_savings': _dec(rec.estimated_cost_savings),
            'impact_timeframe': rec.impact_timeframe,
            'confidence_score': rec.confidence_score,
            'source_modules': rec.source_modules,
            'effort_hours': rec.effort_hours,
            'effort_level': rec.effort_level,
            'responsible_team': rec.responsible_team,
            'status': rec.status,
            'completed_at': rec.completed_at.isoformat() if rec.completed_at else None,
            'actual_impact': _dec(rec.actual_impact) if rec.actual_impact else None,
            # Decision-layer fields
            'due_date': rec.due_date.isoformat() if rec.due_date else None,
            'priority_score': rec.priority_score,
            'urgency_weight': rec.urgency_weight,
            'data_as_of': rec.data_as_of,
            'dedup_hash': rec.dedup_hash,
            'is_cross_functional': rec.is_cross_functional,
            'baseline_metric_name': rec.baseline_metric_name,
            'baseline_metric_value': rec.baseline_metric_value,
            'target_metric_value': rec.target_metric_value,
            'impact_7d': rec.impact_7d,
            'impact_30d': rec.impact_30d,
        }

    def _corr_to_dict(self, corr: BriefCorrelation) -> Dict:
        return {
            'id': corr.id,
            'correlation_type': corr.correlation_type,
            'modules_involved': corr.modules_involved,
            'title': corr.title,
            'narrative': corr.narrative,
            'evidence': corr.evidence,
            'confidence': corr.confidence,
            'revenue_impact': _dec(corr.revenue_impact),
        }
