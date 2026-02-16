"""
Causal Triage Service — classifies root cause before action recommendation.

Six cause categories:
  demand           — organic search demand declining
  auction_pressure — competitive CPC / impression share pressure
  landing_page     — LP conversion or bounce degradation
  attribution_lag  — Google vs Shopify conversion gap
  catalog_feed     — Merchant Center disapprovals
  measurement      — stale or missing data sources
"""
from datetime import date, timedelta
from typing import Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.google_ads_data import GoogleAdsCampaign
from app.models.ga4_data import GA4LandingPage, GA4DeviceBreakdown
from app.models.search_console_data import SearchConsoleQuery
from app.models.merchant_center_data import MerchantCenterDisapproval
from app.utils.logger import log


class CausalTriageService:
    def __init__(self, db: Session):
        self.db = db

    def diagnose(
        self,
        campaign_id: str,
        period_start: date,
        period_end: date,
        google_conversions: int = 0,
        actual_conversions: int = 0,
    ) -> Dict:
        """
        Classify root cause for a campaign's underperformance.

        Returns:
            {
                'primary_cause': str,
                'confidence': float,      # 0-1, gap between #1 and #2
                'causes': [               # all causes sorted by score
                    {'cause': str, 'score': float, 'evidence': str},
                ],
            }
        """
        causes = []

        demand = self._score_demand(period_start, period_end)
        causes.append(demand)

        auction = self._score_auction_pressure(campaign_id, period_start, period_end)
        causes.append(auction)

        lp = self._score_landing_page(period_start, period_end)
        causes.append(lp)

        attr = self._score_attribution(google_conversions, actual_conversions)
        causes.append(attr)

        feed = self._score_catalog_feed(period_start, period_end)
        causes.append(feed)

        meas = self._score_measurement()
        causes.append(meas)

        # Sort by score descending
        causes.sort(key=lambda c: c['score'], reverse=True)

        primary = causes[0]
        second = causes[1]['score'] if len(causes) > 1 else 0
        gap = primary['score'] - second

        # Confidence: high gap between #1 and #2 = high confidence
        # If top score is 0, no cause identified
        if primary['score'] == 0:
            confidence = 0
        else:
            confidence = min(1.0, gap / max(primary['score'], 0.01) + 0.3)

        return {
            'primary_cause': primary['cause'] if primary['score'] > 0.1 else None,
            'confidence': round(confidence, 2),
            'causes': causes,
        }

    # ------------------------------------------------------------------
    # Demand: Search Console query volume trends
    # ------------------------------------------------------------------

    def _score_demand(self, start: date, end: date) -> Dict:
        """Detect demand decline via Search Console click/impression trends."""
        mid = start + (end - start) // 2

        def _period_totals(s, e):
            row = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label('clicks'),
                func.sum(SearchConsoleQuery.impressions).label('impr'),
            ).filter(
                SearchConsoleQuery.date >= s,
                SearchConsoleQuery.date <= e,
            ).first()
            return (row.clicks or 0, row.impr or 0)

        prev_clicks, prev_impr = _period_totals(start, mid - timedelta(days=1))
        curr_clicks, curr_impr = _period_totals(mid, end)

        if prev_clicks == 0:
            return {'cause': 'demand', 'score': 0, 'evidence': 'No prior SC data'}

        click_change = (curr_clicks - prev_clicks) / prev_clicks
        impr_change = (curr_impr - prev_impr) / prev_impr if prev_impr > 0 else 0

        score = 0
        evidence_parts = []

        # Clicks declining is a strong demand signal
        if click_change < -0.15:
            score = min(1.0, abs(click_change))
            evidence_parts.append(f"SC clicks {click_change:+.0%}")
        if impr_change < -0.10:
            score = max(score, min(0.8, abs(impr_change)))
            evidence_parts.append(f"SC impressions {impr_change:+.0%}")

        evidence = '; '.join(evidence_parts) if evidence_parts else 'Demand stable'
        return {'cause': 'demand', 'score': round(score, 2), 'evidence': evidence}

    # ------------------------------------------------------------------
    # Auction pressure: impression share + CPC trends
    # ------------------------------------------------------------------

    def _score_auction_pressure(self, campaign_id: str, start: date, end: date) -> Dict:
        """Detect competitive pressure via impression share and CPC changes."""
        mid = start + (end - start) // 2

        def _period_avg(s, e):
            row = self.db.query(
                func.avg(GoogleAdsCampaign.search_rank_lost_impression_share).label('rank_lost'),
                func.avg(GoogleAdsCampaign.search_budget_lost_impression_share).label('budget_lost'),
                func.avg(GoogleAdsCampaign.avg_cpc).label('cpc'),
                func.avg(GoogleAdsCampaign.ctr).label('ctr'),
            ).filter(
                GoogleAdsCampaign.campaign_id == campaign_id,
                GoogleAdsCampaign.date >= s,
                GoogleAdsCampaign.date <= e,
            ).first()
            return {
                'rank_lost': float(row.rank_lost or 0),
                'budget_lost': float(row.budget_lost or 0),
                'cpc': float(row.cpc or 0),
                'ctr': float(row.ctr or 0),
            }

        prev = _period_avg(start, mid - timedelta(days=1))
        curr = _period_avg(mid, end)

        score = 0
        evidence_parts = []

        # Rank-lost IS increase = auction pressure
        rank_delta = curr['rank_lost'] - prev['rank_lost']
        if rank_delta > 5:
            score = min(1.0, rank_delta / 20)
            evidence_parts.append(f"Rank-lost IS +{rank_delta:.0f}pp")

        # CPC increase while CTR stable = competitive bidding
        if prev['cpc'] > 0:
            cpc_change = (curr['cpc'] - prev['cpc']) / prev['cpc']
            ctr_change = (curr['ctr'] - prev['ctr']) / prev['ctr'] if prev['ctr'] > 0 else 0
            if cpc_change > 0.20 and abs(ctr_change) < 0.15:
                score = max(score, min(1.0, cpc_change))
                evidence_parts.append(f"CPC {cpc_change:+.0%} while CTR stable")

        # Budget-lost IS increase = budget constraint (related but different)
        budget_delta = curr['budget_lost'] - prev['budget_lost']
        if budget_delta > 10:
            score = max(score, min(0.6, budget_delta / 30))
            evidence_parts.append(f"Budget-lost IS +{budget_delta:.0f}pp")

        evidence = '; '.join(evidence_parts) if evidence_parts else 'Auction pressure stable'
        return {'cause': 'auction_pressure', 'score': round(score, 2), 'evidence': evidence}

    # ------------------------------------------------------------------
    # Landing page: GA4 CVR/bounce for google/cpc traffic
    # ------------------------------------------------------------------

    def _score_landing_page(self, start: date, end: date) -> Dict:
        """Detect LP friction via google/cpc CVR and bounce rate changes."""
        mid = start + (end - start) // 2

        def _period_lp(s, e):
            row = self.db.query(
                func.sum(GA4LandingPage.sessions).label('sessions'),
                func.sum(GA4LandingPage.conversions).label('conversions'),
                func.avg(GA4LandingPage.bounce_rate).label('bounce'),
            ).filter(
                GA4LandingPage.session_source == 'google',
                GA4LandingPage.session_medium == 'cpc',
                GA4LandingPage.date >= s,
                GA4LandingPage.date <= e,
            ).first()
            sessions = row.sessions or 0
            convs = row.conversions or 0
            cvr = convs / sessions if sessions > 0 else 0
            return {'sessions': sessions, 'cvr': cvr, 'bounce': float(row.bounce or 0)}

        prev = _period_lp(start, mid - timedelta(days=1))
        curr = _period_lp(mid, end)

        score = 0
        evidence_parts = []
        cvr_change = 0
        bounce_change = 0

        if prev['cvr'] > 0:
            cvr_change = (curr['cvr'] - prev['cvr']) / prev['cvr']
            if cvr_change < -0.20:
                # CVR dropped — check if sessions are stable (ad traffic is fine)
                session_change = (
                    (curr['sessions'] - prev['sessions']) / prev['sessions']
                    if prev['sessions'] > 0 else 0
                )
                if session_change > -0.15:  # Sessions didn't drop much
                    score = min(1.0, abs(cvr_change))
                    evidence_parts.append(
                        f"LP CVR {cvr_change:+.0%} while sessions {session_change:+.0%}"
                    )

        if prev['bounce'] > 0:
            bounce_change = (curr['bounce'] - prev['bounce']) / prev['bounce']
            if bounce_change > 0.15:
                score = max(score, min(0.7, bounce_change))
                evidence_parts.append(f"Bounce rate {bounce_change:+.0%}")

        evidence = '; '.join(evidence_parts) if evidence_parts else 'LP metrics stable'
        return {
            'cause': 'landing_page',
            'score': round(score, 2),
            'evidence': evidence,
            'cvr_change': round(cvr_change, 3),
            'bounce_change': round(bounce_change, 3),
        }

    # ------------------------------------------------------------------
    # Attribution lag: Google vs Shopify conversion gap
    # ------------------------------------------------------------------

    def _score_attribution(
        self, google_conversions: int, actual_conversions: int
    ) -> Dict:
        """Detect attribution uncertainty from Google/Shopify conversion gap."""
        if google_conversions == 0:
            return {'cause': 'attribution_lag', 'score': 0, 'evidence': 'No Google conversions'}

        ratio = actual_conversions / google_conversions
        gap_pct = (1 - ratio) * 100

        score = 0
        if ratio < 0.2:
            score = 0.8
        elif ratio < 0.5:
            score = 0.5
        elif ratio < 0.7:
            score = 0.2

        evidence = (
            f"Shopify matches {ratio:.0%} of Google conversions "
            f"(gap: {gap_pct:.0f}%)"
        )
        return {'cause': 'attribution_lag', 'score': round(score, 2), 'evidence': evidence}

    # ------------------------------------------------------------------
    # Catalog/feed: Merchant Center disapprovals
    # ------------------------------------------------------------------

    def _score_catalog_feed(self, start: date, end: date) -> Dict:
        """Detect product feed issues from Merchant Center disapprovals."""
        try:
            active_disapprovals = self.db.query(
                func.count(MerchantCenterDisapproval.id)
            ).filter(
                MerchantCenterDisapproval.issue_severity == 'disapproved',
                MerchantCenterDisapproval.is_resolved == False,
            ).scalar() or 0

            new_in_period = self.db.query(
                func.count(MerchantCenterDisapproval.id)
            ).filter(
                MerchantCenterDisapproval.issue_severity == 'disapproved',
                MerchantCenterDisapproval.first_seen_date >= start,
                MerchantCenterDisapproval.first_seen_date <= end,
            ).scalar() or 0
        except Exception:
            # Table may not exist or have different schema
            return {'cause': 'catalog_feed', 'score': 0, 'evidence': 'MC data unavailable'}

        score = 0
        evidence_parts = []

        if new_in_period > 10:
            score = min(0.8, new_in_period / 50)
            evidence_parts.append(f"{new_in_period} new disapprovals in period")
        if active_disapprovals > 50:
            score = max(score, min(0.6, active_disapprovals / 200))
            evidence_parts.append(f"{active_disapprovals} active disapprovals")

        evidence = '; '.join(evidence_parts) if evidence_parts else 'Feed health OK'
        return {'cause': 'catalog_feed', 'score': round(score, 2), 'evidence': evidence}

    # ------------------------------------------------------------------
    # Measurement: data freshness
    # ------------------------------------------------------------------

    def _score_measurement(self) -> Dict:
        """Check if any data sources are stale."""
        from app.models.google_ads_data import GoogleAdsCampaign
        from app.models.ga4_data import GA4DailySummary

        stale_sources = []
        today = date.today()

        # Check ads freshness (threshold: 48h)
        try:
            max_ads = self.db.query(func.max(GoogleAdsCampaign.date)).scalar()
            if max_ads and (today - max_ads).days > 2:
                stale_sources.append(f"Ads data {(today - max_ads).days}d old")
        except Exception:
            pass

        # Check GA4 freshness (threshold: 72h)
        try:
            max_ga4 = self.db.query(func.max(GA4DailySummary.date)).scalar()
            if max_ga4 and (today - max_ga4).days > 3:
                stale_sources.append(f"GA4 data {(today - max_ga4).days}d old")
        except Exception:
            pass

        score = min(1.0, len(stale_sources) * 0.4)
        evidence = '; '.join(stale_sources) if stale_sources else 'All sources fresh'
        return {'cause': 'measurement', 'score': round(score, 2), 'evidence': evidence}
