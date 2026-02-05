"""
404 & Redirect Intelligence Service

Tracks broken links, analyzes redirect health, and calculates lost revenue.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from decimal import Decimal

from app.models.redirect_health import (
    NotFoundError, RedirectRule, RedirectChain,
    LostRevenue, RedirectInsight, BrokenLink
)
from app.utils.logger import log


class RedirectHealthService:
    """Service for analyzing 404 errors and redirects"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds
        self.high_traffic_404_threshold = 100  # Monthly sessions
        self.site_avg_conversion_rate = 0.025  # 2.5%
        self.site_avg_order_value = 85.00
        self.max_redirect_chain_length = 2

    async def analyze_404_health(self, days: int = 30) -> Dict:
        """
        Complete 404 and redirect analysis

        Returns:
        - Top 404 errors by traffic
        - Revenue impact from 404s
        - Redirect chain issues
        - Broken internal links
        - Recommended fixes
        """
        try:
            # Get 404 errors
            not_found_errors = await self.get_404_errors(days)

            # Calculate revenue impact
            revenue_impact = await self.calculate_revenue_impact(not_found_errors)

            # Check redirects
            redirect_issues = await self.analyze_redirects()

            # Detect redirect chains
            redirect_chains = await self.detect_redirect_chains()

            # Find broken internal links
            broken_links = await self.find_broken_internal_links()

            # Generate recommendations
            recommendations = self._generate_recommendations(
                not_found_errors, redirect_issues, redirect_chains, broken_links
            )

            # Calculate summary
            summary = self._calculate_summary(
                not_found_errors, revenue_impact, redirect_issues,
                redirect_chains, broken_links
            )

            return {
                "not_found_errors": not_found_errors,
                "revenue_impact": revenue_impact,
                "redirect_issues": redirect_issues,
                "redirect_chains": redirect_chains,
                "broken_links": broken_links,
                "recommendations": recommendations,
                "summary": summary
            }

        except Exception as e:
            log.error(f"Error analyzing 404 health: {str(e)}")
            raise

    async def get_404_errors(self, days: int = 30) -> List[Dict]:
        """
        Get 404 errors

        Returns errors sorted by:
        1. Revenue impact
        2. Traffic volume
        """
        errors = []

        try:
            # In production, would query GA4 for 404 errors
            # For now, return structured example data

            errors = [
                {
                    "requested_url": "/products/discontinued-bathroom-sink",
                    "url_type": "product_page",
                    "traffic": {
                        "total_hits": 840,
                        "unique_visitors": 620,
                        "estimated_monthly_sessions": 840
                    },
                    "referrers": {
                        "top_referrers": [
                            {"referrer": "google.com", "count": 450},
                            {"referrer": "pinterest.com", "count": 180},
                            {"referrer": "instagram.com", "count": 120}
                        ],
                        "external_links_count": 12,
                        "internal_links_count": 3
                    },
                    "user_behavior": {
                        "bounce_rate_after_404": 0.72,
                        "avg_session_duration_before_404": 145
                    },
                    "revenue_impact": {
                        "estimated_conversion_rate": 0.028,
                        "estimated_monthly_revenue_loss": 1974.00,
                        "confidence_level": "high"
                    },
                    "likely_cause": "deleted_product",
                    "recommended_action": "create_redirect",
                    "redirect_to_url": "/collections/bathroom-sinks",
                    "status": "active"
                },
                {
                    "requested_url": "/blogs/old-blog-post",
                    "url_type": "blog_post",
                    "traffic": {
                        "total_hits": 420,
                        "unique_visitors": 380,
                        "estimated_monthly_sessions": 420
                    },
                    "referrers": {
                        "top_referrers": [
                            {"referrer": "google.com", "count": 320},
                            {"referrer": "facebook.com", "count": 60}
                        ],
                        "external_links_count": 8,
                        "internal_links_count": 0
                    },
                    "user_behavior": {
                        "bounce_rate_after_404": 0.85,
                        "avg_session_duration_before_404": 12
                    },
                    "revenue_impact": {
                        "estimated_conversion_rate": 0.015,
                        "estimated_monthly_revenue_loss": 535.00,
                        "confidence_level": "medium"
                    },
                    "likely_cause": "old_url_structure",
                    "recommended_action": "create_redirect",
                    "redirect_to_url": "/blogs/news/new-blog-post",
                    "status": "active"
                },
                {
                    "requested_url": "/collections/clearance",
                    "url_type": "collection_page",
                    "traffic": {
                        "total_hits": 280,
                        "unique_visitors": 240,
                        "estimated_monthly_sessions": 280
                    },
                    "referrers": {
                        "top_referrers": [
                            {"referrer": "direct", "count": 120},
                            {"referrer": "email", "count": 90}
                        ],
                        "external_links_count": 2,
                        "internal_links_count": 5
                    },
                    "user_behavior": {
                        "bounce_rate_after_404": 0.65,
                        "avg_session_duration_before_404": 240
                    },
                    "revenue_impact": {
                        "estimated_conversion_rate": 0.032,
                        "estimated_monthly_revenue_loss": 761.00,
                        "confidence_level": "high"
                    },
                    "likely_cause": "deleted_product",
                    "recommended_action": "restore_page",
                    "redirect_to_url": "/collections/sale",
                    "status": "active"
                }
            ]

            # Sort by revenue impact
            errors.sort(key=lambda x: x['revenue_impact']['estimated_monthly_revenue_loss'], reverse=True)

            return errors

        except Exception as e:
            log.error(f"Error getting 404 errors: {str(e)}")
            return []

    async def calculate_revenue_impact(self, not_found_errors: List[Dict]) -> Dict:
        """
        Calculate total revenue impact from 404s

        Uses:
        - Traffic volume to 404
        - Expected conversion rate (from similar pages)
        - Average order value
        """
        try:
            total_lost_revenue = sum(
                error['revenue_impact']['estimated_monthly_revenue_loss']
                for error in not_found_errors
            )

            total_sessions = sum(
                error['traffic']['estimated_monthly_sessions']
                for error in not_found_errors
            )

            high_impact_404s = [
                error for error in not_found_errors
                if error['revenue_impact']['estimated_monthly_revenue_loss'] >= 500
            ]

            return {
                "total_lost_revenue_monthly": float(total_lost_revenue),
                "total_404_sessions_monthly": total_sessions,
                "high_impact_404s_count": len(high_impact_404s),
                "top_revenue_losses": not_found_errors[:5]
            }

        except Exception as e:
            log.error(f"Error calculating revenue impact: {str(e)}")
            return {}

    async def analyze_redirects(self) -> Dict:
        """
        Analyze redirect health

        Checks for:
        - Redirects to 404s (broken redirects)
        - Slow redirects
        - Redirect chains
        - 302 instead of 301
        """
        issues = {}

        try:
            # Example redirect issues
            issues = {
                "total_redirects": 87,
                "redirects_with_issues": 12,

                "broken_redirects": [
                    {
                        "source_url": "/old-product-page",
                        "destination_url": "/products/deleted-product",
                        "redirect_type": 301,
                        "destination_status_code": 404,
                        "total_hits": 120,
                        "issue": "Redirect points to 404",
                        "recommendation": "Update redirect to point to /collections/all-products"
                    }
                ],

                "redirect_chains": [
                    {
                        "source_url": "/old-url",
                        "destination_url": "/intermediate-url",
                        "final_destination": "/final-url",
                        "chain_length": 3,
                        "total_redirect_time_ms": 450,
                        "issue": "Redirect chain (3 hops)",
                        "recommendation": "Create direct 301 from /old-url to /final-url"
                    }
                ],

                "temporary_redirects": [
                    {
                        "source_url": "/temporary-redirect",
                        "destination_url": "/destination",
                        "redirect_type": 302,
                        "total_hits": 240,
                        "issue": "Using 302 (temporary) instead of 301 (permanent)",
                        "recommendation": "Change to 301 redirect for SEO"
                    }
                ],

                "slow_redirects": [
                    {
                        "source_url": "/slow-redirect",
                        "destination_url": "/destination",
                        "avg_redirect_time_ms": 850,
                        "threshold_ms": 300,
                        "issue": "Redirect takes 850ms (threshold: 300ms)",
                        "recommendation": "Investigate server response time"
                    }
                ]
            }

            return issues

        except Exception as e:
            log.error(f"Error analyzing redirects: {str(e)}")
            return {}

    async def detect_redirect_chains(self) -> List[Dict]:
        """
        Detect multi-hop redirect chains

        Redirect chains hurt SEO and performance:
        - Google may not follow chains >3 hops
        - Each hop adds latency
        """
        chains = []

        try:
            # Example redirect chains
            chains = [
                {
                    "initial_url": "/very-old-product",
                    "final_url": "/products/current-product",
                    "chain_length": 4,
                    "chain_path": [
                        {"url": "/very-old-product", "status": 301},
                        {"url": "/old-product", "status": 301},
                        {"url": "/product-v2", "status": 302},
                        {"url": "/products/current-product", "status": 200}
                    ],
                    "total_redirect_time_ms": 680,
                    "severity": "high",
                    "contains_302": True,
                    "ends_in_404": False,
                    "recommended_fix": "Create direct 301 redirect from /very-old-product to /products/current-product",
                    "status": "active"
                },
                {
                    "initial_url": "/old-collection",
                    "final_url": "/deleted-page",
                    "chain_length": 3,
                    "chain_path": [
                        {"url": "/old-collection", "status": 301},
                        {"url": "/collection-v2", "status": 301},
                        {"url": "/deleted-page", "status": 404}
                    ],
                    "total_redirect_time_ms": 420,
                    "severity": "high",
                    "contains_302": False,
                    "ends_in_404": True,
                    "recommended_fix": "Fix redirect chain - destination returns 404. Redirect to /collections/all instead.",
                    "status": "active"
                }
            ]

            return chains

        except Exception as e:
            log.error(f"Error detecting redirect chains: {str(e)}")
            return []

    async def find_broken_internal_links(self) -> List[Dict]:
        """
        Find internal broken links

        These are links from your own site to 404 pages.
        Easier to fix than external links.
        """
        broken_links = []

        try:
            # Example broken internal links
            broken_links = [
                {
                    "source_page": "/pages/about-us",
                    "broken_link": "/products/discontinued-sink",
                    "link_text": "View our best-selling bathroom sink",
                    "link_type": "text_link",
                    "source_page_traffic": 1200,
                    "estimated_monthly_clicks": 60,
                    "priority": "high",
                    "recommended_fix": "update_link",
                    "suggested_replacement": "/collections/bathroom-sinks",
                    "status": "active"
                },
                {
                    "source_page": "/",
                    "broken_link": "/collections/clearance",
                    "link_text": "Clearance Sale",
                    "link_type": "nav_link",
                    "source_page_traffic": 15000,
                    "estimated_monthly_clicks": 750,
                    "priority": "high",
                    "recommended_fix": "update_link",
                    "suggested_replacement": "/collections/sale",
                    "status": "active"
                }
            ]

            return broken_links

        except Exception as e:
            log.error(f"Error finding broken internal links: {str(e)}")
            return []

    def _generate_recommendations(
        self,
        not_found_errors: List[Dict],
        redirect_issues: Dict,
        redirect_chains: List[Dict],
        broken_links: List[Dict]
    ) -> List[Dict]:
        """Generate prioritized recommendations"""

        recommendations = []

        # High-revenue 404s (create redirects)
        for error in not_found_errors:
            if error['revenue_impact']['estimated_monthly_revenue_loss'] >= 500:
                recommendations.append({
                    "priority": "high",
                    "type": "create_redirect",
                    "title": f"Create redirect for {error['requested_url']}",
                    "description": f"404 error causing ${error['revenue_impact']['estimated_monthly_revenue_loss']:.0f}/month revenue loss",
                    "action": f"Create 301 redirect: {error['requested_url']} → {error['redirect_to_url']}",
                    "impact": f"${error['revenue_impact']['estimated_monthly_revenue_loss']:.0f}/month",
                    "effort": "Low (5 minutes)"
                })

        # Broken redirects (update destination)
        for redirect in redirect_issues.get('broken_redirects', []):
            recommendations.append({
                "priority": "high",
                "type": "fix_redirect",
                "title": f"Fix broken redirect: {redirect['source_url']}",
                "description": f"Redirect points to 404 ({redirect['destination_url']})",
                "action": redirect['recommendation'],
                "impact": f"{redirect['total_hits']} monthly sessions affected",
                "effort": "Low (5 minutes)"
            })

        # Redirect chains (create direct redirects)
        for chain in redirect_chains:
            if chain['chain_length'] >= 3:
                recommendations.append({
                    "priority": "medium",
                    "type": "fix_redirect_chain",
                    "title": f"Fix redirect chain: {chain['initial_url']}",
                    "description": f"{chain['chain_length']}-hop redirect chain adds {chain['total_redirect_time_ms']}ms latency",
                    "action": chain['recommended_fix'],
                    "impact": f"Improve SEO and reduce latency by {chain['total_redirect_time_ms'] - 100}ms",
                    "effort": "Low (5 minutes)"
                })

        # High-traffic broken internal links
        for link in broken_links:
            if link['priority'] == 'high':
                recommendations.append({
                    "priority": "high",
                    "type": "fix_internal_link",
                    "title": f"Fix broken link on {link['source_page']}",
                    "description": f"High-traffic page ({link['source_page_traffic']:,} sessions/month) has broken link",
                    "action": f"Update link from {link['broken_link']} to {link['suggested_replacement']}",
                    "impact": f"{link['estimated_monthly_clicks']} clicks to 404 prevented",
                    "effort": "Low (5 minutes)"
                })

        # Sort by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 3))

        return recommendations[:15]  # Top 15

    def _calculate_summary(
        self,
        not_found_errors: List[Dict],
        revenue_impact: Dict,
        redirect_issues: Dict,
        redirect_chains: List[Dict],
        broken_links: List[Dict]
    ) -> Dict:
        """Calculate summary metrics"""

        return {
            "total_404_errors": len(not_found_errors),
            "high_traffic_404s": len([
                e for e in not_found_errors
                if e['traffic']['estimated_monthly_sessions'] >= self.high_traffic_404_threshold
            ]),
            "total_monthly_revenue_loss": revenue_impact.get('total_lost_revenue_monthly', 0),
            "total_404_sessions": revenue_impact.get('total_404_sessions_monthly', 0),

            "total_redirects": redirect_issues.get('total_redirects', 0),
            "redirects_with_issues": redirect_issues.get('redirects_with_issues', 0),
            "broken_redirects": len(redirect_issues.get('broken_redirects', [])),
            "redirect_chains": len(redirect_chains),

            "broken_internal_links": len(broken_links),
            "high_priority_broken_links": len([l for l in broken_links if l['priority'] == 'high'])
        }

    async def get_404_dashboard(self) -> Dict:
        """Get complete 404 & redirect dashboard"""
        analysis = await self.analyze_404_health()

        return {
            "overview": {
                "total_404_errors": analysis['summary']['total_404_errors'],
                "high_traffic_404s": analysis['summary']['high_traffic_404s'],
                "total_revenue_loss": analysis['summary']['total_monthly_revenue_loss'],
                "total_redirect_issues": analysis['summary']['redirects_with_issues']
            },

            "top_priorities": analysis['recommendations'][:5],

            "404_summary": {
                "count": len(analysis['not_found_errors']),
                "top_errors": analysis['not_found_errors'][:5]
            },

            "revenue_impact_summary": {
                "total_lost_revenue": analysis['revenue_impact']['total_lost_revenue_monthly'],
                "total_sessions": analysis['revenue_impact']['total_404_sessions_monthly'],
                "high_impact_404s": analysis['revenue_impact']['high_impact_404s_count']
            },

            "redirect_issues_summary": {
                "total_redirects": analysis['redirect_issues']['total_redirects'],
                "issues_count": analysis['redirect_issues']['redirects_with_issues'],
                "broken_redirects": len(analysis['redirect_issues'].get('broken_redirects', [])),
                "redirect_chains": len(analysis['redirect_chains'])
            },

            "broken_links_summary": {
                "count": len(analysis['broken_links']),
                "high_priority": analysis['summary']['high_priority_broken_links'],
                "top_links": analysis['broken_links'][:5]
            }
        }
