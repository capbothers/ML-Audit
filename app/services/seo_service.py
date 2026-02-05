"""
SEO Intelligence Service

Analyzes Search Console data to find SEO opportunities.
Answers: "Where are my easy SEO wins?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from collections import defaultdict

from app.models.search_console_data import (
    SearchConsoleQuery, SearchConsolePage, SearchConsoleIndexCoverage, SearchConsoleSitemap
)
from app.models.ga4_data import GA4TrafficSource
from app.config import get_settings
from app.utils.logger import log
from app.services.seo_utils import expected_ctr_for_position, classify_url, shorten_url


class SEOService:
    """Service for SEO intelligence and opportunity analysis"""

    def __init__(self, db: Session):
        self.db = db
        settings = get_settings()
        brand_terms = settings.gsc_brand_terms or []
        if isinstance(brand_terms, str):
            brand_terms = [t.strip() for t in brand_terms.split(",")]
        self.brand_terms = [t.lower() for t in brand_terms if t.strip()]
        self.spam_fragments = [
            "slot", "casino", "bet", "poker", "porn", "xxx", "adult", "escort",
            "apk", "download", "login", "free", "bonus", "crypto", "forex"
        ]
        self.value_per_click_default = 1.0

        # Opportunity thresholds (configurable)
        self.high_impression_threshold = 500  # Min impressions/month to consider
        self.low_ctr_threshold = 0.03  # 3% - CTR below this with high impressions = opportunity
        self.page_one_position_max = 7.5  # Position < 7.5 = page 1
        self.close_to_page_one_min = 8.0  # Position 8-15 = close to page 1
        self.close_to_page_one_max = 15.0
        self.declining_threshold = -0.15  # -15% traffic = declining

    async def identify_all_opportunities(
        self,
        days: int = 30
    ) -> Dict:
        """
        Identify all SEO opportunities

        Returns categorized opportunities with priority scores
        """
        log.info(f"Identifying SEO opportunities (last {days} days)")

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        results = {
            'period_days': days,
            'period_start': start_date.isoformat(),
            'period_end': end_date.isoformat(),
            'opportunities': {
                'quick_wins': [],
                'close_to_page_one': [],
                'declining_pages': [],
                'technical_issues': []
            },
            'summary': {}
        }

        # 1. Find quick wins (high impression, low CTR)
        quick_wins = await self.find_high_impression_low_ctr(start_date, end_date, limit=100)
        results['opportunities']['quick_wins'] = quick_wins

        # 2. Find close-to-page-one opportunities
        close_to_page_one = await self.find_close_to_page_one(start_date, end_date, limit=50)
        results['opportunities']['close_to_page_one'] = close_to_page_one

        # 3. Find declining pages
        declining = await self.find_declining_pages(start_date, end_date)
        results['opportunities']['declining_pages'] = declining

        # 4. Get technical issues
        technical = await self.get_technical_issues()
        results['opportunities']['technical_issues'] = technical

        # Calculate summary
        total_opportunities = (
            len(quick_wins) +
            len(close_to_page_one) +
            len(declining) +
            len(technical)
        )

        results['summary'] = {
            'total_opportunities': total_opportunities,
            'quick_wins_count': len(quick_wins),
            'close_to_page_one_count': len(close_to_page_one),
            'declining_pages_count': len(declining),
            'technical_issues_count': len(technical)
        }

        log.info(f"Found {total_opportunities} SEO opportunities")

        return results

    async def find_high_impression_low_ctr(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20
    ) -> List[Dict]:
        """
        Find queries with high impressions but low CTR

        These are RANKING but not getting clicks = title/meta issue
        """
        log.info("Finding high impression, low CTR queries")

        # Aggregate Search Console queries over the period
        queries = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= self.high_impression_threshold,
            func.avg(SearchConsoleQuery.ctr) < self.low_ctr_threshold,
            func.avg(SearchConsoleQuery.position) <= self.page_one_position_max
        ).order_by(
            desc(func.sum(SearchConsoleQuery.impressions))
        ).limit(limit * 5).all()

        opportunities = []
        prev_ctr_map = self._get_query_prev_ctr_map([q.query for q in queries], start_date, end_date)

        value_per_click = self._get_value_per_click(start_date, end_date)
        for query in queries:
            if query.ctr is None or query.position is None:
                continue
            if self._is_brand_query(query.query) or self._is_spam_query(query.query):
                continue
            top_page = self._get_top_page_for_query(query.query, start_date, end_date)
            suggested_title, suggested_meta = self._build_snippet_suggestions(query.query)
            # Calculate potential clicks if CTR improved
            # Assume improving to average CTR for position (rough estimate)
            expected_ctr = self._estimate_expected_ctr(query.position)
            potential_additional_clicks = int(query.impressions * (expected_ctr - query.ctr))

            if potential_additional_clicks > 10:  # Only if meaningful gain
                opportunity = {
                    'query': query.query,
                    'current_impressions': query.impressions,
                    'current_clicks': query.clicks,
                    'current_ctr': round(query.ctr * 100, 1),  # Convert to %
                    'current_position': round(query.position, 1),

                    'impressions': query.impressions,
                    'clicks': query.clicks,
                    'ctr': round(query.ctr * 100, 1),
                    'position': round(query.position, 1),

                    'expected_ctr': round(expected_ctr * 100, 1),
                    'potential_additional_clicks': potential_additional_clicks,

                    'issue': f"CTR of {query.ctr * 100:.1f}% is low for position {query.position:.1f}",
                    'probable_cause': 'Title tag or meta description not compelling enough',

                    'recommended_action': f"Update title/meta to include '{query.query}' and make more compelling",

                    'impact_score': self._calculate_impact_score(
                        potential_clicks=potential_additional_clicks,
                        current_impressions=query.impressions,
                        difficulty='low'  # Title/meta changes are easy
                    ),
                    'estimated_revenue_gain': round(potential_additional_clicks * value_per_click, 2),
                    'plain_explanation': "People are seeing this page but not clicking. Improving the title/description should win more clicks.",
                    'pro_explanation': "High impressions with low CTR indicate snippet under‑performance. Optimize title/meta to capture demand.",
                    'page': top_page,
                    'previous_ctr': prev_ctr_map.get(query.query),
                    'suggested_title': suggested_title,
                    'suggested_meta': suggested_meta,
                    'owner': 'Unassigned',
                    'due_date': None,

                    'effort': 'low',
                    'priority': 'high' if potential_additional_clicks > 50 else 'medium'
                }

                opportunities.append(opportunity)

        # Sort by impact score
        opportunities.sort(key=lambda x: x['impact_score'], reverse=True)

        log.info(f"Found {len(opportunities)} high impression, low CTR opportunities")

        return opportunities[:limit]

    async def find_close_to_page_one(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20
    ) -> List[Dict]:
        """
        Find queries at position 8-15 (close to page 1)

        These are worth pushing with content improvements
        """
        log.info("Finding queries close to page 1")

        queries = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query
        ).having(
            func.avg(SearchConsoleQuery.position) >= self.close_to_page_one_min,
            func.avg(SearchConsoleQuery.position) <= self.close_to_page_one_max,
            func.sum(SearchConsoleQuery.impressions) >= 100
        ).order_by(
            desc(func.sum(SearchConsoleQuery.impressions))
        ).limit(limit * 5).all()

        opportunities = []

        value_per_click = self._get_value_per_click(start_date, end_date)
        for query in queries:
            if query.position is None:
                continue
            if self._is_brand_query(query.query) or self._is_spam_query(query.query):
                continue
            # Estimate traffic gain from reaching page 1
            page_one_ctr = self._estimate_expected_ctr(5.0)  # Assume position 5
            potential_clicks = int(query.impressions * page_one_ctr) - query.clicks

            if potential_clicks > 20:
                opportunity = {
                    'query': query.query,
                    'current_position': round(query.position, 1),
                    'current_impressions': query.impressions,
                    'current_clicks': query.clicks,

                    'impressions': query.impressions,
                    'clicks': query.clicks,
                    'ctr': round((query.ctr or 0) * 100, 1) if query.ctr is not None else None,
                    'position': round(query.position, 1),

                    'target_position': '5-7 (page 1)',
                    'potential_additional_clicks': potential_clicks,

                    'issue': f"Currently position {query.position:.1f} (page 2)",
                    'opportunity': 'Push to page 1 with content improvements',

                    'recommended_action': 'Add comprehensive content (500+ words), FAQ section, schema markup',
                    'specific_steps': [
                        f"Research top-ranking content for '{query.query}'",
                        "Add 500-800 words of relevant, helpful content",
                        "Include FAQ section addressing common questions",
                        "Add schema markup (FAQ/HowTo)",
                        "Improve internal linking to this page"
                    ],

                    'impact_score': self._calculate_impact_score(
                        potential_clicks=potential_clicks,
                        current_impressions=query.impressions,
                        difficulty='medium'
                    ),
                    'estimated_revenue_gain': round(potential_clicks * value_per_click, 2),
                    'plain_explanation': "This query is almost on page 1. A content upgrade can pull it up and unlock more traffic.",
                    'pro_explanation': "Position 8–15 queries are page‑2 opportunities. Content + internal links can push into top 7.",

                    'effort': 'medium',
                    'timeline': '1-3 months',
                    'priority': 'high' if query.impressions > 500 else 'medium'
                }

                opportunities.append(opportunity)

        opportunities.sort(key=lambda x: x['impact_score'], reverse=True)

        log.info(f"Found {len(opportunities)} close-to-page-one opportunities")

        return opportunities[:limit]

    async def find_declining_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20
    ) -> List[Dict]:
        """
        Find pages with declining organic traffic

        These need attention before they lose more traffic
        """
        log.info("Finding declining pages")

        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        current = self.db.query(
            SearchConsolePage.page.label("page"),
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).group_by(
            SearchConsolePage.page
        ).subquery()

        previous = self.db.query(
            SearchConsolePage.page.label("page"),
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= prev_start,
            SearchConsolePage.date <= prev_end,
        ).group_by(
            SearchConsolePage.page
        ).subquery()

        rows = self.db.query(
            current.c.page,
            current.c.clicks,
            current.c.impressions,
            current.c.position,
            previous.c.clicks.label("prev_clicks"),
            previous.c.impressions.label("prev_impressions"),
            previous.c.position.label("prev_position"),
        ).outerjoin(
            previous, previous.c.page == current.c.page
        ).all()

        opportunities = []

        value_per_click = self._get_value_per_click(start_date, end_date)
        for row in rows:
            prev_clicks = row.prev_clicks or 0
            if prev_clicks < 20:
                continue

            clicks_lost = prev_clicks - (row.clicks or 0)
            if prev_clicks == 0:
                continue
            decline_pct = (row.clicks - prev_clicks) / prev_clicks
            if decline_pct > self.declining_threshold:
                continue

            position_change = None
            if row.prev_position is not None and row.position is not None:
                position_change = row.position - row.prev_position

            top_queries = await self._get_top_queries_for_page(row.page, start_date, end_date)
            opportunity = {
                'url': row.page,
                'page_type': None,

                'current_clicks': row.clicks or 0,
                'previous_clicks': prev_clicks,
                'clicks_lost': clicks_lost,
                'decline_pct': round(decline_pct * 100, 1),

                'current_position': round(row.position, 1) if row.position is not None else None,
                'previous_position': round(row.prev_position, 1) if row.prev_position is not None else None,
                'position_change': round(position_change, 1) if position_change is not None else None,

                'issue': f"Traffic down {abs(decline_pct * 100):.0f}% in last {(end_date - start_date).days} days",
                'severity': 'critical' if abs(decline_pct) > 0.30 else 'high',

                'top_queries': top_queries,

                'probable_cause': self._diagnose_decline_cause(position_change, row.position, row.prev_position),

                'recommended_action': 'Content refresh, check for technical issues, improve on-page SEO',
                'specific_steps': [
                    "Check for technical issues (indexing, speed, mobile)",
                    "Analyze top-ranking competitor content",
                    "Refresh content with new information",
                    "Update images/media",
                    "Improve internal linking",
                    "Add schema markup if missing"
                ],

                'impact_score': self._calculate_impact_score(
                    potential_clicks=clicks_lost,
                    current_impressions=row.impressions or 0,
                    difficulty='medium'
                ),
                'estimated_revenue_loss': round(clicks_lost * value_per_click, 2),
                'plain_explanation': "This page is losing visits. Refresh it before the drop gets worse.",
                'pro_explanation': "Clicks declined meaningfully vs the prior period. Likely ranking decay or intent mismatch.",

                'effort': 'medium',
                'priority': 'critical' if abs(decline_pct) > 0.30 else 'high'
            }

            opportunities.append(opportunity)

        opportunities.sort(key=lambda x: x['decline_pct'])

        log.info(f"Found {len(opportunities)} declining pages")

        return opportunities[:limit]

    async def get_technical_issues(self) -> List[Dict]:
        """
        Get technical SEO issues

        Returns indexing issues, Core Web Vitals failures, etc.
        """
        log.info("Getting technical SEO issues")

        issues = []

        # 1. Index coverage issues
        error_count = self.db.query(SearchConsoleIndexCoverage).filter(
            SearchConsoleIndexCoverage.coverage_state == 'Error'
        ).count()
        if error_count > 0:
            sample_errors = self.db.query(SearchConsoleIndexCoverage.url).filter(
                SearchConsoleIndexCoverage.coverage_state == 'Error'
            ).limit(5).all()
            issues.append({
                'type': 'indexing_errors',
                'severity': 'critical',
                'count': error_count,
                'issue': f"{error_count} pages not indexed due to errors",
                'sample_urls': [r[0] for r in sample_errors],
                'impact': 'These pages are invisible in search',
                'fix': 'Fix crawl errors, server errors, and 404s'
            })

        warning_count = self.db.query(SearchConsoleIndexCoverage).filter(
            SearchConsoleIndexCoverage.coverage_state == 'Valid with warnings'
        ).count()
        if warning_count > 0:
            issues.append({
                'type': 'indexing_warnings',
                'severity': 'high',
                'count': warning_count,
                'issue': f"{warning_count} pages with indexing warnings",
                'impact': 'May not rank properly'
            })

        # 2. Sitemap issues
        sitemap_errors = self.db.query(SearchConsoleSitemap).filter(
            SearchConsoleSitemap.errors > 0
        ).all()
        if sitemap_errors:
            issues.append({
                'type': 'sitemap_errors',
                'severity': 'high',
                'count': sum(s.errors for s in sitemap_errors),
                'issue': f"{len(sitemap_errors)} sitemaps reporting errors",
                'sample_urls': [s.sitemap_url for s in sitemap_errors[:5]],
                'impact': 'Search engines may miss pages in affected sitemaps'
            })

        sitemap_warnings = self.db.query(SearchConsoleSitemap).filter(
            SearchConsoleSitemap.warnings > 0
        ).all()
        if sitemap_warnings:
            issues.append({
                'type': 'sitemap_warnings',
                'severity': 'medium',
                'count': sum(s.warnings for s in sitemap_warnings),
                'issue': f"{len(sitemap_warnings)} sitemaps reporting warnings",
                'sample_urls': [s.sitemap_url for s in sitemap_warnings[:5]],
                'impact': 'Potential issues with sitemap health'
            })

        log.info(f"Found {len(issues)} technical SEO issues")

        return issues

    def _estimate_expected_ctr(self, position: float) -> float:
        """
        Estimate expected CTR based on position

        Based on industry averages
        """
        if position <= 1.5:
            return 0.30  # Position 1: ~30%
        elif position <= 2.5:
            return 0.15  # Position 2: ~15%
        elif position <= 3.5:
            return 0.10  # Position 3: ~10%
        elif position <= 5.0:
            return 0.07  # Positions 4-5: ~7%
        elif position <= 7.0:
            return 0.05  # Positions 6-7: ~5%
        elif position <= 10.0:
            return 0.03  # Positions 8-10: ~3%
        else:
            return 0.01  # Page 2+: ~1%

    def _calculate_impact_score(
        self,
        potential_clicks: int,
        current_impressions: int,
        difficulty: str
    ) -> int:
        """
        Calculate opportunity impact score (0-100)

        Higher = better opportunity
        """
        # Base score from potential clicks
        click_score = min(potential_clicks / 5, 50)  # Max 50 points from clicks

        # Impression volume score
        impression_score = min(current_impressions / 100, 30)  # Max 30 points

        # Effort/difficulty modifier
        if difficulty == 'low':
            difficulty_multiplier = 1.5  # Boost easy wins
        elif difficulty == 'medium':
            difficulty_multiplier = 1.0
        else:  # high
            difficulty_multiplier = 0.7

        total_score = (click_score + impression_score) * difficulty_multiplier

        return min(int(total_score), 100)

    def _diagnose_decline_cause(
        self,
        position_change: Optional[float],
        current_position: Optional[float],
        previous_position: Optional[float]
    ) -> str:
        """
        Diagnose why a page is declining

        Returns probable cause
        """
        if position_change is not None and current_position is not None and previous_position is not None:
            if position_change > 3:
                return f"Position dropped from ~{previous_position:.1f} to {current_position:.1f} - likely lost rankings to competitors"

        return "Declining traffic - needs content refresh and on-page SEO review"

    async def _get_top_queries_for_page(
        self,
        page_url: str,
        start_date: date,
        end_date: date,
        limit: int = 3
    ) -> List[Dict]:
        base = self._normalize_url(page_url)
        if not base:
            return []
        variants = {base, base.rstrip("/"), base.rstrip("/") + "/"}

        rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.position).label("position"),
            SearchConsoleQuery.page.label("page"),
        ).filter(
            SearchConsoleQuery.page.isnot(None),
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
            SearchConsoleQuery.page.like(f"{base}%")
        ).group_by(
            SearchConsoleQuery.query, SearchConsoleQuery.page
        ).order_by(
            desc(func.sum(SearchConsoleQuery.clicks))
        ).limit(limit * 2).all()

        results = []
        for row in rows:
            if self._normalize_url(row.page) not in variants:
                continue
            if self._is_brand_query(row.query) or self._is_spam_query(row.query):
                continue
            results.append({
                "query": row.query,
                "clicks": row.clicks,
                "impressions": row.impressions,
                "position": round(row.position, 1) if row.position is not None else None
            })
            if len(results) >= limit:
                break

        return results

    def _normalize_url(self, url: Optional[str]) -> str:
        if not url:
            return ""
        cleaned = url.split("?")[0].strip()
        if cleaned.endswith("/") and len(cleaned) > len("https://"):
            cleaned = cleaned[:-1]
        return cleaned

    def _is_brand_query(self, query: str) -> bool:
        if not query:
            return False
        q = query.lower()
        return any(term in q for term in self.brand_terms)

    def _is_spam_query(self, query: str) -> bool:
        if not query:
            return False
        q = query.lower()
        if any(fragment in q for fragment in self.spam_fragments):
            return True
        if "http" in q or ".com" in q or ".net" in q or ".xyz" in q:
            return True
        if any(ch in q for ch in ["🔥", "💰", "🤑"]):
            return True
        return False

    async def get_seo_dashboard(
        self,
        days: int = 30
    ) -> Dict:
        """
        Complete SEO intelligence dashboard

        Everything you need to know about SEO opportunities
        """
        log.info("Generating SEO dashboard")

        # Get all opportunities
        opportunities = await self.identify_all_opportunities(days)
        metrics = self._get_seo_metrics(days)

        # Get top opportunities across all categories
        all_opps = (
            opportunities['opportunities']['quick_wins'][:5] +
            opportunities['opportunities']['close_to_page_one'][:5] +
            opportunities['opportunities']['declining_pages'][:5]
        )

        # Sort by impact score
        all_opps.sort(key=lambda x: x.get('impact_score', 0), reverse=True)

        alerts = self._build_alerts(opportunities, metrics, days)
        action_stack = self._build_action_stack(opportunities, metrics)

        # NEW: Enhanced data for 4-tab dashboard
        executive_snapshot = self.get_executive_snapshot(days)
        click_gap_top30 = self.get_click_gap_analysis(days, limit=30)
        category_breakdown = self.get_category_template_breakdown(days)
        monthly_trends = self.get_monthly_trends(months=6)
        position_distribution = self.get_position_distribution(days)

        return {
            'period_days': days,
            'generated_at': datetime.utcnow().isoformat(),
            'metrics': metrics,

            'summary': opportunities['summary'],
            'alerts': alerts,
            'action_stack': action_stack,
            'ctr_drag_details': opportunities['opportunities'].get('quick_wins', []),

            'top_opportunities': all_opps[:10],  # Top 10 across all categories

            'by_category': opportunities['opportunities'],

            'technical_health': {
                'issues_count': len(opportunities['opportunities']['technical_issues']),
                'issues': opportunities['opportunities']['technical_issues']
            },

            'recommendations': {
                'immediate_actions': [
                    opp.get('recommended_action')
                    for opp in all_opps[:3]
                ],
                'estimated_traffic_gain': sum(
                    opp.get('potential_additional_clicks', 0)
                    for opp in all_opps[:10]
                )
            },
            'fix_tracker': {
                'enabled': False,
                'items': []
            },

            # NEW keys for the 4-tab layout
            'executive_snapshot': executive_snapshot,
            'click_gap_top30': click_gap_top30,
            'category_breakdown': category_breakdown,
            'monthly_trends': monthly_trends,
            'position_distribution': position_distribution,
        }

    def _build_alerts(self, opportunities: Dict, metrics: Dict, days: int) -> List[Dict]:
        alerts = []
        summary = opportunities.get('summary', {})

        if metrics.get('ctr_drag_count', 0) > 0:
            alerts.append({
                'type': 'ctr_drag',
                'severity': 'high',
                'title': 'CTR drag on high‑impression queries',
                'message': f"{metrics.get('ctr_drag_count', 0)} queries have >5k impressions with CTR under 0.5%.",
                'action': 'Rewrite titles/meta for the top offenders.',
                'plain': "Lots of people see these pages, but almost nobody clicks. Update the titles and descriptions.",
                'pro': "CTR drag indicates snippet under‑performance; prioritize title/meta rewrites."
            })

        if metrics.get('ranking_drop_pages', 0) > 0:
            alerts.append({
                'type': 'ranking_drop',
                'severity': 'critical',
                'title': 'Ranking drops detected',
                'message': f"{metrics.get('ranking_drop_pages', 0)} pages dropped >2.5 positions.",
                'action': 'Audit those pages and refresh content.',
                'plain': "Some important pages slipped down in Google. Refresh and strengthen them.",
                'pro': "Ranking decay detected; prioritize content refresh + internal linking."
            })

        if summary.get('quick_wins_count', 0) > 0:
            alerts.append({
                'type': 'quick_wins',
                'severity': 'medium',
                'title': 'Quick wins available',
                'message': f"{summary.get('quick_wins_count', 0)} high‑impression queries with low CTR.",
                'action': 'Prioritize title/meta updates.',
                'plain': "Fast wins: small title changes could bring in more clicks.",
                'pro': "High‑impression, low‑CTR queries are low‑effort wins."
            })

        if summary.get('close_to_page_one_count', 0) > 0:
            alerts.append({
                'type': 'page_two',
                'severity': 'medium',
                'title': 'Page‑2 opportunities',
                'message': f"{summary.get('close_to_page_one_count', 0)} queries sitting in positions 8‑15.",
                'action': 'Add content + internal links to push to page 1.',
                'plain': "Several topics are close to page one. A content boost can lift them.",
                'pro': "Queries in positions 8–15 need content depth + internal links."
            })

        if summary.get('declining_pages_count', 0) > 0:
            alerts.append({
                'type': 'declines',
                'severity': 'high',
                'title': 'Declining pages',
                'message': f"{summary.get('declining_pages_count', 0)} pages losing clicks vs prior period.",
                'action': 'Refresh content and check technical health.',
                'plain': "Some pages are losing visits. Refresh content before it worsens.",
                'pro': "Declining pages need refresh and technical review."
            })

        if summary.get('technical_issues_count', 0) > 0:
            alerts.append({
                'type': 'technical',
                'severity': 'critical',
                'title': 'Technical SEO issues',
                'message': f"{summary.get('technical_issues_count', 0)} issues in index coverage or sitemaps.",
                'action': 'Fix crawl/indexing issues immediately.',
                'plain': "Google is having trouble indexing some pages. Fixing this is urgent.",
                'pro': "Indexing/sitemap issues detected; resolve immediately."
            })

        cannibal = self._get_cannibalization(days)
        if cannibal:
            alerts.append({
                'type': 'cannibalization',
                'severity': 'medium',
                'title': 'Keyword cannibalization',
                'message': f"{len(cannibal)} queries have multiple pages competing.",
                'action': 'Consolidate or clarify the primary page.',
                'plain': "Multiple pages are fighting for the same keyword. Pick a winner.",
                'pro': "Cannibalization detected; consolidate pages or adjust internal linking."
            })

        serp_risk = self._get_serp_risk(days)
        if serp_risk:
            alerts.append({
                'type': 'serp_risk',
                'severity': 'high',
                'title': 'SERP risk detected',
                'message': f"{len(serp_risk)} pages lost clicks while impressions stayed flat.",
                'action': 'Improve titles/snippets to regain CTR.',
                'plain': "People still see your page but aren’t clicking. Fix the snippet.",
                'pro': "CTR decay with stable impressions suggests snippet fatigue."
            })

        return alerts

    def _build_action_stack(self, opportunities: Dict, metrics: Dict) -> List[Dict]:
        action_stack = []
        quick_wins = opportunities.get('opportunities', {}).get('quick_wins', [])
        close_to_page = opportunities.get('opportunities', {}).get('close_to_page_one', [])
        declining = opportunities.get('opportunities', {}).get('declining_pages', [])

        for item in quick_wins[:3]:
            action_stack.append({
                'priority': 'high',
                'task': f"Rewrite title/meta for '{item.get('query')}'",
                'impact': f"${item.get('estimated_revenue_gain', 0):,.0f} / month",
                'plain': "Quick win: improve click‑through on a high‑demand query.",
                'pro': "CTR optimization on high‑impression query."
            })

        for item in close_to_page[:3]:
            action_stack.append({
                'priority': 'medium',
                'task': f"Expand content for '{item.get('query')}'",
                'impact': f"${item.get('estimated_revenue_gain', 0):,.0f} / month",
                'plain': "Push this topic onto page 1 with more content.",
                'pro': "Content depth + internal links to lift page‑2 query."
            })

        for item in declining[:3]:
            action_stack.append({
                'priority': 'high',
                'task': f"Refresh {item.get('url', '')}",
                'impact': f"${item.get('estimated_revenue_loss', 0):,.0f} at risk",
                'plain': "Traffic is dropping. Refresh before it worsens.",
                'pro': "Ranking decay; refresh content + resolve causes."
            })

        if metrics.get('ctr_drag_count', 0) > 0:
            action_stack.append({
                'priority': 'medium',
                'task': "Fix CTR drag queries with >5k impressions",
                'impact': "High upside",
                'plain': "Big visibility but low clicks. Fix titles now.",
                'pro': "CTR drag remediation on high‑impression queries."
            })

        if metrics.get('ranking_drop_pages', 0) > 0:
            action_stack.append({
                'priority': 'high',
                'task': "Audit pages with ranking drops >2.5",
                'impact': "High risk",
                'plain': "Important pages slipping. Audit and fix.",
                'pro': "Ranking decay audit."
            })

        return action_stack[:6]

    def _get_cannibalization(self, days: int) -> List[Dict]:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.count(func.distinct(SearchConsoleQuery.page)).label("page_count"),
            func.sum(SearchConsoleQuery.clicks).label("clicks")
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
            SearchConsoleQuery.page.isnot(None)
        ).group_by(
            SearchConsoleQuery.query
        ).having(
            func.count(func.distinct(SearchConsoleQuery.page)) >= 2,
            func.sum(SearchConsoleQuery.clicks) >= 10
        ).order_by(
            desc(func.sum(SearchConsoleQuery.clicks))
        ).limit(10).all()

        results = []
        for row in rows:
            if self._is_brand_query(row.query) or self._is_spam_query(row.query):
                continue
            results.append({
                'query': row.query,
                'page_count': row.page_count,
                'clicks': row.clicks
            })
        return results

    def _get_serp_risk(self, days: int) -> List[Dict]:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        current = self.db.query(
            SearchConsolePage.page.label("page"),
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).group_by(SearchConsolePage.page).subquery()

        previous = self.db.query(
            SearchConsolePage.page.label("page"),
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= prev_start,
            SearchConsolePage.date <= prev_end,
        ).group_by(SearchConsolePage.page).subquery()

        rows = self.db.query(
            current.c.page,
            current.c.clicks,
            current.c.impressions,
            current.c.position,
            previous.c.clicks.label("prev_clicks"),
            previous.c.impressions.label("prev_impressions"),
            previous.c.position.label("prev_position"),
        ).outerjoin(
            previous, previous.c.page == current.c.page
        ).all()

        results = []
        for row in rows:
            if row.prev_clicks is None or row.prev_impressions is None:
                continue
            if row.prev_impressions == 0:
                continue
            click_change = (row.clicks or 0) - row.prev_clicks
            impression_change = ((row.impressions or 0) - row.prev_impressions) / row.prev_impressions
            position_change = 0
            if row.prev_position is not None and row.position is not None:
                position_change = row.position - row.prev_position
            if click_change < 0 and abs(impression_change) < 0.1 and abs(position_change) < 0.5:
                results.append({
                    'page': row.page,
                    'click_change': click_change,
                    'impression_change_pct': round(impression_change * 100, 1)
                })

        return results[:10]

    def _get_seo_metrics(self, days: int = 30) -> Dict:
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        totals = self.db.query(
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).first()

        clicks = totals.clicks or 0
        impressions = totals.impressions or 0
        avg_position = round(totals.position or 0, 1)
        ctr = round((clicks / impressions * 100), 2) if impressions else 0

        unique_queries = self.db.query(
            func.count(func.distinct(SearchConsoleQuery.query))
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).scalar() or 0

        unique_pages = self.db.query(
            func.count(func.distinct(SearchConsolePage.page))
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).scalar() or 0

        non_brand_clicks = 0
        query_rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks")
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query
        ).all()
        for row in query_rows:
            if self._is_brand_query(row.query) or self._is_spam_query(row.query):
                continue
            non_brand_clicks += row.clicks or 0

        ctr_drag_count = self.db.query(
            func.count(func.distinct(SearchConsoleQuery.query))
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= 5000,
            func.avg(SearchConsoleQuery.ctr) < 0.005
        ).all()
        ctr_drag_total = len(ctr_drag_count)

        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)
        current_pos = self.db.query(
            SearchConsolePage.page.label("page"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).group_by(SearchConsolePage.page).subquery()
        previous_pos = self.db.query(
            SearchConsolePage.page.label("page"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= prev_start,
            SearchConsolePage.date <= prev_end,
        ).group_by(SearchConsolePage.page).subquery()
        drop_rows = self.db.query(
            current_pos.c.page,
            current_pos.c.position.label("current_position"),
            previous_pos.c.position.label("previous_position"),
        ).outerjoin(
            previous_pos, previous_pos.c.page == current_pos.c.page
        ).all()
        ranking_drop = 0
        for row in drop_rows:
            if row.previous_position is None or row.current_position is None:
                continue
            if (row.current_position - row.previous_position) > 2.5:
                ranking_drop += 1

        organic_revenue = self._get_organic_revenue(start_date, end_date)
        value_per_click = round((organic_revenue / clicks), 2) if clicks else self.value_per_click_default

        return {
            'total_clicks': int(clicks),
            'total_impressions': int(impressions),
            'avg_ctr': ctr,
            'avg_position': avg_position,
            'unique_queries': unique_queries,
            'unique_pages': unique_pages,
            'non_brand_clicks': int(non_brand_clicks),
            'non_brand_share': round((non_brand_clicks / clicks * 100), 1) if clicks else 0,
            'ctr_drag_count': ctr_drag_total,
            'ranking_drop_pages': ranking_drop,
            'organic_revenue': round(organic_revenue, 2),
            'value_per_click': value_per_click
        }

    def _get_organic_revenue(self, start_date: date, end_date: date) -> float:
        revenue = self.db.query(
            func.sum(GA4TrafficSource.total_revenue)
        ).filter(
            GA4TrafficSource.date >= start_date,
            GA4TrafficSource.date <= end_date,
            GA4TrafficSource.session_medium.ilike('%organic%')
        ).scalar()
        return float(revenue or 0.0)

    def _get_value_per_click(self, start_date: date, end_date: date) -> float:
        clicks = self.db.query(
            func.sum(SearchConsoleQuery.clicks)
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date
        ).scalar() or 0
        organic_revenue = self._get_organic_revenue(start_date, end_date)
        if clicks == 0:
            return self.value_per_click_default
        return max(round(organic_revenue / clicks, 4), 0.01)

    def _get_query_prev_ctr_map(
        self,
        queries: List[str],
        start_date: date,
        end_date: date
    ) -> Dict[str, float]:
        if not queries:
            return {}
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
        ).filter(
            SearchConsoleQuery.date >= prev_start,
            SearchConsoleQuery.date <= prev_end,
            SearchConsoleQuery.query.in_(queries)
        ).group_by(
            SearchConsoleQuery.query
        ).all()

        prev = {}
        for row in rows:
            if row.impressions and row.impressions > 0:
                prev[row.query] = round((row.clicks / row.impressions) * 100, 2)
            elif row.ctr is not None:
                prev[row.query] = round(row.ctr * 100, 2)
        return prev

    def _get_top_page_for_query(self, query: str, start_date: date, end_date: date) -> Optional[str]:
        row = self.db.query(
            SearchConsoleQuery.page.label("page"),
            func.sum(SearchConsoleQuery.clicks).label("clicks")
        ).filter(
            SearchConsoleQuery.query == query,
            SearchConsoleQuery.page.isnot(None),
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.page
        ).order_by(
            desc(func.sum(SearchConsoleQuery.clicks))
        ).first()
        return row.page if row else None

    def _build_snippet_suggestions(self, query: str) -> (str, str):
        base = query.strip().title()
        title = f"{base} | Shop Online | Cass Brothers"
        meta = f"Shop {query} at Cass Brothers. Fast delivery Australia, trusted brands, expert advice. View the range now."
        if len(meta) > 155:
            meta = meta[:152] + "..."
        return title, meta

    # ==================================================================
    #  NEW: Executive Snapshot (Performance Tab KPIs + WoW deltas)
    # ==================================================================
    def get_executive_snapshot(self, days: int = 30) -> Dict:
        """All KPIs with weighted CTR/position + WoW deltas."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        def _period_totals(sd, ed):
            t = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label("clicks"),
                func.sum(SearchConsoleQuery.impressions).label("impressions"),
            ).filter(
                SearchConsoleQuery.date >= sd,
                SearchConsoleQuery.date <= ed,
            ).first()
            clicks = int(t.clicks or 0)
            impressions = int(t.impressions or 0)
            return clicks, impressions

        def _weighted_metrics(sd, ed):
            """Weighted CTR (clicks/impressions) and impression-weighted position."""
            t = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label("clicks"),
                func.sum(SearchConsoleQuery.impressions).label("impressions"),
            ).filter(
                SearchConsoleQuery.date >= sd,
                SearchConsoleQuery.date <= ed,
            ).first()
            clicks = int(t.clicks or 0)
            impressions = int(t.impressions or 0)
            wctr = clicks / impressions if impressions else 0  # decimal

            # Impression-weighted position
            rows = self.db.query(
                SearchConsoleQuery.position,
                SearchConsoleQuery.impressions,
            ).filter(
                SearchConsoleQuery.date >= sd,
                SearchConsoleQuery.date <= ed,
                SearchConsoleQuery.impressions > 0,
                SearchConsoleQuery.position.isnot(None),
            ).all()
            total_imp = 0
            w_pos = 0.0
            for r in rows:
                imp = r.impressions or 0
                total_imp += imp
                w_pos += (r.position or 0) * imp
            wpos = w_pos / total_imp if total_imp else 0
            return wctr, wpos

        cur_clicks, cur_imp = _period_totals(start_date, end_date)
        prev_clicks, prev_imp = _period_totals(prev_start, prev_end)
        cur_wctr, cur_wpos = _weighted_metrics(start_date, end_date)
        prev_wctr, prev_wpos = _weighted_metrics(prev_start, prev_end)

        def _delta(cur, prev):
            if prev == 0:
                return None
            return round((cur - prev) / prev * 100, 1)

        def _abs_delta(cur, prev):
            if prev is None or prev == 0:
                return None
            return round(cur - prev, 2)

        # Click gap total
        click_gap = self._compute_total_click_gap(start_date, end_date)

        # URLs analyzed, gaining, losing
        gaining, losing = self._rank_movers(start_date, end_date, prev_start, prev_end)

        unique_urls = self.db.query(
            func.count(func.distinct(SearchConsolePage.page))
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).scalar() or 0

        return {
            "total_clicks": cur_clicks,
            "clicks_wow": _delta(cur_clicks, prev_clicks),
            "total_impressions": cur_imp,
            "impressions_wow": _delta(cur_imp, prev_imp),
            "weighted_ctr": round(cur_wctr * 100, 2),
            "weighted_ctr_wow": _abs_delta(cur_wctr * 100, prev_wctr * 100),
            "weighted_position": round(cur_wpos, 1),
            "position_wow": _abs_delta(cur_wpos, prev_wpos),
            "click_gap_total": click_gap,
            "urls_analyzed": unique_urls,
            "gaining_rank": gaining,
            "losing_rank": losing,
            "status": self._trend_status(cur_clicks, prev_clicks, losing),
        }

    def _compute_total_click_gap(self, start_date: date, end_date: date) -> int:
        """Sum of (impressions * expected_ctr - actual_clicks) across all queries."""
        rows = self.db.query(
            func.avg(SearchConsoleQuery.position).label("position"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= 50,
        ).all()

        total = 0
        for r in rows:
            exp = expected_ctr_for_position(r.position or 50)
            gap = int((r.impressions or 0) * exp) - (r.clicks or 0)
            if gap > 0:
                total += gap
        return total

    def _rank_movers(self, sd, ed, psd, ped):
        """Count pages gaining and losing rank."""
        cur = self.db.query(
            SearchConsolePage.page.label("page"),
            func.avg(SearchConsolePage.position).label("pos"),
        ).filter(
            SearchConsolePage.date >= sd,
            SearchConsolePage.date <= ed,
        ).group_by(SearchConsolePage.page).subquery()

        prev = self.db.query(
            SearchConsolePage.page.label("page"),
            func.avg(SearchConsolePage.position).label("pos"),
        ).filter(
            SearchConsolePage.date >= psd,
            SearchConsolePage.date <= ped,
        ).group_by(SearchConsolePage.page).subquery()

        rows = self.db.query(
            cur.c.page,
            cur.c.pos.label("cur_pos"),
            prev.c.pos.label("prev_pos"),
        ).join(prev, prev.c.page == cur.c.page).all()

        gaining = 0
        losing = 0
        for r in rows:
            if r.prev_pos is None or r.cur_pos is None:
                continue
            diff = r.cur_pos - r.prev_pos
            if diff < -0.5:
                gaining += 1
            elif diff > 0.5:
                losing += 1
        return gaining, losing

    def _trend_status(self, cur_clicks, prev_clicks, losing):
        if prev_clicks == 0:
            return "Stable"
        change = (cur_clicks - prev_clicks) / prev_clicks
        if change > 0.05:
            return "Growing"
        elif change < -0.05 or losing > 50:
            return "Declining"
        return "Stable"

    # ==================================================================
    #  NEW: Click Gap Analysis (Query Intelligence Tab)
    # ==================================================================
    def get_click_gap_analysis(self, days: int = 30, limit: int = 30) -> List[Dict]:
        """Top queries by click gap with expected vs actual CTR."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Group by query (aggregate across pages) for broader coverage
        rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.avg(SearchConsoleQuery.position).label("position"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query,
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= 10,
        ).order_by(
            desc(func.sum(SearchConsoleQuery.impressions))
        ).limit(limit * 10).all()

        value_per_click = self._get_value_per_click(start_date, end_date)
        results = []
        for r in rows:
            if self._is_brand_query(r.query) or self._is_spam_query(r.query):
                continue
            exp_ctr = expected_ctr_for_position(r.position or 50)
            actual_clicks = r.clicks or 0
            actual_ctr = actual_clicks / (r.impressions or 1)
            gap = max(0, int((r.impressions or 0) * exp_ctr) - actual_clicks)
            if gap < 2:
                continue
            # Get top page for this query
            top_page = self._get_top_page_for_query(r.query, start_date, end_date) if len(results) < limit else None
            sparkline = self._compute_sparkline_query(r.query, months=6) if len(results) < limit else []
            results.append({
                "query": r.query,
                "page": top_page,
                "short_page": shorten_url(top_page) if top_page else "-",
                "position": round(r.position, 1) if r.position else None,
                "impressions": int(r.impressions or 0),
                "clicks": int(actual_clicks),
                "actual_ctr": round(actual_ctr * 100, 2),
                "expected_ctr": round(exp_ctr * 100, 2),
                "click_gap": gap,
                "revenue_gap": round(gap * value_per_click, 2),
                "sparkline": sparkline,
            })
            if len(results) >= limit:
                break
        results.sort(key=lambda x: x["click_gap"], reverse=True)
        return results[:limit]

    # ==================================================================
    #  NEW: Category / Template Breakdown
    # ==================================================================
    def get_category_template_breakdown(self, days: int = 30) -> List[Dict]:
        """Aggregate search metrics by URL category/template."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        rows = self.db.query(
            SearchConsolePage.page.label("page"),
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).group_by(SearchConsolePage.page).all()

        buckets = defaultdict(lambda: {"clicks": 0, "impressions": 0, "positions": [], "pages": 0})
        for r in rows:
            info = classify_url(r.page)
            cat = info["template"]
            buckets[cat]["clicks"] += int(r.clicks or 0)
            buckets[cat]["impressions"] += int(r.impressions or 0)
            buckets[cat]["positions"].append(r.position or 0)
            buckets[cat]["pages"] += 1

        results = []
        for template, d in buckets.items():
            avg_pos = sum(d["positions"]) / len(d["positions"]) if d["positions"] else 0
            ctr = round(d["clicks"] / d["impressions"] * 100, 2) if d["impressions"] else 0
            results.append({
                "template": template,
                "pages": d["pages"],
                "clicks": d["clicks"],
                "impressions": d["impressions"],
                "ctr": ctr,
                "avg_position": round(avg_pos, 1),
            })
        results.sort(key=lambda x: x["clicks"], reverse=True)
        return results

    # ==================================================================
    #  NEW: Monthly Trends (6 months)
    # ==================================================================
    def get_monthly_trends(self, months: int = 6) -> List[Dict]:
        """Monthly rollups for chart + table."""
        today = date.today()
        results = []
        for i in range(months - 1, -1, -1):
            # First day of month i months ago
            m = today.month - i
            y = today.year
            while m <= 0:
                m += 12
                y -= 1
            month_start = date(y, m, 1)
            # Last day of month
            if m == 12:
                month_end = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = date(y, m + 1, 1) - timedelta(days=1)
            # Don't exceed today
            if month_end > today:
                month_end = today

            totals = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label("clicks"),
                func.sum(SearchConsoleQuery.impressions).label("impressions"),
            ).filter(
                SearchConsoleQuery.date >= month_start,
                SearchConsoleQuery.date <= month_end,
            ).first()

            clicks = int(totals.clicks or 0)
            impressions = int(totals.impressions or 0)
            ctr = round(clicks / impressions * 100, 2) if impressions else 0

            avg_pos = self.db.query(
                func.avg(SearchConsoleQuery.position)
            ).filter(
                SearchConsoleQuery.date >= month_start,
                SearchConsoleQuery.date <= month_end,
                SearchConsoleQuery.impressions > 0,
            ).scalar() or 0

            # Click gap for this month
            gap = self._compute_total_click_gap(month_start, month_end)

            results.append({
                "month": month_start.strftime("%b %Y"),
                "month_key": month_start.isoformat(),
                "clicks": clicks,
                "impressions": impressions,
                "ctr": ctr,
                "avg_position": round(float(avg_pos), 1),
                "click_gap": gap,
            })
        return results

    # ==================================================================
    #  NEW: Position Distribution
    # ==================================================================
    def get_position_distribution(self, days: int = 30) -> Dict:
        """Position bucket counts (1-3, 4-10, 11-20, 20+)."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(SearchConsoleQuery.query).all()

        buckets = {"1-3": 0, "4-10": 0, "11-20": 0, "20+": 0}
        for r in rows:
            p = r.position or 50
            if p <= 3:
                buckets["1-3"] += 1
            elif p <= 10:
                buckets["4-10"] += 1
            elif p <= 20:
                buckets["11-20"] += 1
            else:
                buckets["20+"] += 1

        total = sum(buckets.values()) or 1
        return {
            "buckets": buckets,
            "percentages": {k: round(v / total * 100, 1) for k, v in buckets.items()},
            "total_queries": total,
        }

    # ==================================================================
    #  NEW: Underperformers (Action Plan Tab)
    # ==================================================================
    def get_underperformers(self, days: int = 30, limit: int = 50) -> List[Dict]:
        """Priority-scored action list with all ML flags."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        # Current period query-level aggregation (group by query only for broader coverage)
        cur_rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.avg(SearchConsoleQuery.position).label("position"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
        ).filter(
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(
            SearchConsoleQuery.query,
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= 10,
        ).order_by(
            desc(func.sum(SearchConsoleQuery.impressions))
        ).limit(limit * 8).all()

        # Previous period for ML flags
        prev_map = {}
        prev_rows = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.avg(SearchConsoleQuery.position).label("position"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
        ).filter(
            SearchConsoleQuery.date >= prev_start,
            SearchConsoleQuery.date <= prev_end,
        ).group_by(
            SearchConsoleQuery.query,
        ).having(
            func.sum(SearchConsoleQuery.impressions) >= 5,
        ).all()
        for p in prev_rows:
            prev_map[p.query] = p

        value_per_click = self._get_value_per_click(start_date, end_date)
        results = []
        max_gap = 1
        max_imp = 1

        # First pass: compute raw values
        raw = []
        for r in cur_rows:
            if self._is_brand_query(r.query) or self._is_spam_query(r.query):
                continue
            exp_ctr = expected_ctr_for_position(r.position or 50)
            gap = max(0, int((r.impressions or 0) * exp_ctr) - (r.clicks or 0))
            if gap < 3:
                continue
            raw.append((r, gap, exp_ctr))
            if gap > max_gap:
                max_gap = gap
            if (r.impressions or 0) > max_imp:
                max_imp = r.impressions

        for r, gap, exp_ctr in raw:
            prev = prev_map.get(r.query)
            flags = self._compute_ml_flags(r, prev)
            top_page = self._get_top_page_for_query(r.query, start_date, end_date)
            url_info = classify_url(top_page)

            # Priority score: (norm_gap×0.5 + norm_impressions×0.3) × effort_weight + revenue×0.2
            norm_gap = gap / max_gap if max_gap else 0
            norm_imp = (r.impressions or 0) / max_imp if max_imp else 0
            effort = url_info["effort_weight"]
            rev_score = min(gap * value_per_click / 500, 1.0)  # Normalize revenue contribution
            priority = (norm_gap * 0.5 + norm_imp * 0.3) * effort + rev_score * 0.2
            priority_score = min(int(priority * 100), 100)

            sparkline = self._compute_sparkline_query(r.query, months=6)

            actual_ctr = (r.clicks or 0) / (r.impressions or 1)
            results.append({
                "query": r.query,
                "page": top_page,
                "short_page": shorten_url(top_page),
                "template": url_info["template"],
                "position": round(r.position, 1) if r.position else None,
                "impressions": int(r.impressions or 0),
                "clicks": int(r.clicks or 0),
                "actual_ctr": round(actual_ctr * 100, 2),
                "expected_ctr": round(exp_ctr * 100, 2),
                "click_gap": gap,
                "revenue_opportunity": round(gap * value_per_click, 2),
                "priority_score": priority_score,
                "serp_risk": flags["serp_risk"],
                "content_decay": flags["content_decay"],
                "fix_first": flags["fix_first"],
                "sparkline": sparkline,
            })

        results.sort(key=lambda x: x["priority_score"], reverse=True)
        return results[:limit]

    def _compute_ml_flags(self, current, previous) -> Dict:
        """Compute ML flags: serp_risk, content_decay, fix_first."""
        serp_risk = False
        content_decay = False
        fix_first = "Optimize snippet"

        if previous is not None:
            cur_pos = current.position or 0
            prev_pos = previous.position or 0
            cur_clicks = current.clicks or 0
            prev_clicks = previous.clicks or 0
            cur_imp = current.impressions or 0
            prev_imp = previous.impressions or 0

            # Compute CTR from clicks/impressions (stored ctr field uses percentage scale)
            cur_ctr = cur_clicks / cur_imp if cur_imp else 0
            prev_ctr = prev_clicks / prev_imp if prev_imp else 0

            pos_change = cur_pos - prev_pos

            # SERP Risk: position worsened >0.3 AND CTR dropped AND impressions stable (±15%)
            imp_change_pct = abs(cur_imp - prev_imp) / prev_imp if prev_imp else 1.0
            if pos_change > 0.3 and cur_ctr < prev_ctr and imp_change_pct < 0.15:
                serp_risk = True

            # Content Decay: clicks down >15% AND position flat (±1.0) AND impressions >100
            if prev_clicks > 0 and cur_imp > 100:
                click_decline = (prev_clicks - cur_clicks) / prev_clicks
                if click_decline > 0.15 and abs(pos_change) < 1.0:
                    content_decay = True

        # Fix First recommendation
        if serp_risk and content_decay:
            fix_first = "Rewrite title + refresh content"
        elif serp_risk:
            fix_first = "Rewrite title/meta"
        elif content_decay:
            fix_first = "Refresh content"
        else:
            # Check cannibalization (multiple pages for same query handled separately)
            fix_first = "Optimize snippet"

        return {
            "serp_risk": serp_risk,
            "content_decay": content_decay,
            "fix_first": fix_first,
        }

    # ==================================================================
    #  NEW: Query Drill-Down
    # ==================================================================
    def get_query_drill_down(self, query: str, days: int = 30) -> Dict:
        """Full query detail for modal."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        # Current period
        cur = self.db.query(
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.query == query,
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).first()

        # Previous period
        prev = self.db.query(
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
            func.avg(SearchConsoleQuery.position).label("position"),
        ).filter(
            SearchConsoleQuery.query == query,
            SearchConsoleQuery.date >= prev_start,
            SearchConsoleQuery.date <= prev_end,
        ).first()

        # Pages ranking for this query
        pages = self.db.query(
            SearchConsoleQuery.page.label("page"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.position).label("position"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
        ).filter(
            SearchConsoleQuery.query == query,
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
            SearchConsoleQuery.page.isnot(None),
        ).group_by(SearchConsoleQuery.page).order_by(
            desc(func.sum(SearchConsoleQuery.clicks))
        ).limit(10).all()

        # Monthly history
        monthly = self._compute_monthly_for_query(query)

        cur_clicks = int(cur.clicks or 0)
        cur_imp = int(cur.impressions or 0)
        prev_clicks = int(prev.clicks or 0) if prev and prev.clicks else 0
        prev_imp = int(prev.impressions or 0) if prev and prev.impressions else 0
        cur_ctr = round(cur_clicks / cur_imp * 100, 2) if cur_imp else 0
        prev_ctr = round(prev_clicks / prev_imp * 100, 2) if prev_imp else 0

        exp_ctr = expected_ctr_for_position(cur.position or 50) if cur.position else 0
        click_gap = max(0, int(cur_imp * exp_ctr) - cur_clicks)
        is_cannibalized = len(pages) > 1

        # ML flags
        class _Row:
            pass
        cr = _Row()
        cr.position = cur.position
        cr.clicks = cur_clicks
        cr.impressions = cur_imp
        pr = None
        if prev and prev.clicks is not None:
            pr = _Row()
            pr.position = prev.position
            pr.clicks = prev_clicks
            pr.impressions = prev_imp
        flags = self._compute_ml_flags(cr, pr)

        return {
            "query": query,
            "current": {
                "clicks": cur_clicks,
                "impressions": cur_imp,
                "ctr": cur_ctr,
                "position": round(cur.position, 1) if cur.position else None,
            },
            "previous": {
                "clicks": prev_clicks,
                "impressions": prev_imp,
                "ctr": prev_ctr,
                "position": round(prev.position, 1) if prev and prev.position else None,
            },
            "click_gap": click_gap,
            "expected_ctr": round(exp_ctr * 100, 2),
            "is_cannibalized": is_cannibalized,
            "pages": [
                {
                    "page": p.page,
                    "short_page": shorten_url(p.page),
                    "clicks": int(p.clicks or 0),
                    "impressions": int(p.impressions or 0),
                    "position": round(p.position, 1) if p.position else None,
                    "ctr": round((p.clicks or 0) / (p.impressions or 1) * 100, 2),
                }
                for p in pages
            ],
            "monthly": monthly,
            "flags": flags,
        }

    # ==================================================================
    #  NEW: Page Drill-Down
    # ==================================================================
    def get_page_drill_down(self, url: str, days: int = 30) -> Dict:
        """Full page detail for modal."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        period_days = (end_date - start_date).days + 1
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days - 1)

        # Current period
        cur = self.db.query(
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.ctr).label("ctr"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.page == url,
            SearchConsolePage.date >= start_date,
            SearchConsolePage.date <= end_date,
        ).first()

        # Previous period
        prev = self.db.query(
            func.sum(SearchConsolePage.clicks).label("clicks"),
            func.sum(SearchConsolePage.impressions).label("impressions"),
            func.avg(SearchConsolePage.ctr).label("ctr"),
            func.avg(SearchConsolePage.position).label("position"),
        ).filter(
            SearchConsolePage.page == url,
            SearchConsolePage.date >= prev_start,
            SearchConsolePage.date <= prev_end,
        ).first()

        # Top queries for this page
        queries = self.db.query(
            SearchConsoleQuery.query.label("query"),
            func.sum(SearchConsoleQuery.clicks).label("clicks"),
            func.sum(SearchConsoleQuery.impressions).label("impressions"),
            func.avg(SearchConsoleQuery.position).label("position"),
            func.avg(SearchConsoleQuery.ctr).label("ctr"),
        ).filter(
            SearchConsoleQuery.page == url,
            SearchConsoleQuery.date >= start_date,
            SearchConsoleQuery.date <= end_date,
        ).group_by(SearchConsoleQuery.query).order_by(
            desc(func.sum(SearchConsoleQuery.clicks))
        ).limit(15).all()

        # Monthly history for page
        monthly = self._compute_monthly_for_page(url)

        url_info = classify_url(url)

        cur_c = int(cur.clicks or 0) if cur else 0
        cur_i = int(cur.impressions or 0) if cur else 0
        prev_c = int(prev.clicks or 0) if prev and prev.clicks else 0
        prev_i = int(prev.impressions or 0) if prev and prev.impressions else 0

        return {
            "url": url,
            "short_url": shorten_url(url),
            "template": url_info["template"],
            "category": url_info["category"],
            "current": {
                "clicks": cur_c,
                "impressions": cur_i,
                "ctr": round(cur_c / cur_i * 100, 2) if cur_i else 0,
                "position": round(cur.position, 1) if cur and cur.position else None,
            },
            "previous": {
                "clicks": prev_c,
                "impressions": prev_i,
                "ctr": round(prev_c / prev_i * 100, 2) if prev_i else 0,
                "position": round(prev.position, 1) if prev and prev.position else None,
            },
            "queries": [
                {
                    "query": q.query,
                    "clicks": int(q.clicks or 0),
                    "impressions": int(q.impressions or 0),
                    "position": round(q.position, 1) if q.position else None,
                    "ctr": round((q.clicks or 0) / (q.impressions or 1) * 100, 2),
                }
                for q in queries
            ],
            "monthly": monthly,
        }

    # ==================================================================
    #  Sparkline / Monthly Helpers
    # ==================================================================
    def _compute_sparkline_query(self, query: str, months: int = 6) -> List[int]:
        """6-month click totals for a query."""
        today = date.today()
        values = []
        for i in range(months - 1, -1, -1):
            m = today.month - i
            y = today.year
            while m <= 0:
                m += 12
                y -= 1
            ms = date(y, m, 1)
            if m == 12:
                me = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                me = date(y, m + 1, 1) - timedelta(days=1)
            if me > today:
                me = today

            total = self.db.query(
                func.sum(SearchConsoleQuery.clicks)
            ).filter(
                SearchConsoleQuery.query == query,
                SearchConsoleQuery.date >= ms,
                SearchConsoleQuery.date <= me,
            ).scalar() or 0
            values.append(int(total))
        return values

    def _compute_monthly_for_query(self, query: str, months: int = 6) -> List[Dict]:
        today = date.today()
        results = []
        for i in range(months - 1, -1, -1):
            m = today.month - i
            y = today.year
            while m <= 0:
                m += 12
                y -= 1
            ms = date(y, m, 1)
            if m == 12:
                me = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                me = date(y, m + 1, 1) - timedelta(days=1)
            if me > today:
                me = today

            t = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label("clicks"),
                func.sum(SearchConsoleQuery.impressions).label("impressions"),
                func.avg(SearchConsoleQuery.position).label("position"),
            ).filter(
                SearchConsoleQuery.query == query,
                SearchConsoleQuery.date >= ms,
                SearchConsoleQuery.date <= me,
            ).first()
            clicks = int(t.clicks or 0)
            impressions = int(t.impressions or 0)
            results.append({
                "month": ms.strftime("%b %Y"),
                "clicks": clicks,
                "impressions": impressions,
                "position": round(float(t.position or 0), 1),
            })
        return results

    def _compute_monthly_for_page(self, url: str, months: int = 6) -> List[Dict]:
        today = date.today()
        results = []
        for i in range(months - 1, -1, -1):
            m = today.month - i
            y = today.year
            while m <= 0:
                m += 12
                y -= 1
            ms = date(y, m, 1)
            if m == 12:
                me = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                me = date(y, m + 1, 1) - timedelta(days=1)
            if me > today:
                me = today

            t = self.db.query(
                func.sum(SearchConsolePage.clicks).label("clicks"),
                func.sum(SearchConsolePage.impressions).label("impressions"),
                func.avg(SearchConsolePage.position).label("position"),
            ).filter(
                SearchConsolePage.page == url,
                SearchConsolePage.date >= ms,
                SearchConsolePage.date <= me,
            ).first()
            clicks = int(t.clicks or 0)
            impressions = int(t.impressions or 0)
            results.append({
                "month": ms.strftime("%b %Y"),
                "clicks": clicks,
                "impressions": impressions,
                "position": round(float(t.position or 0), 1),
            })
        return results
