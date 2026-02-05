"""
Content & Merchandising Gap Analysis Service

Identifies missing content, merchandising issues, and optimization opportunities.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from decimal import Decimal

from app.models.content_gap import (
    ContentGap, MerchandisingGap, ContentOpportunity,
    ContentPerformance, ContentInsight, CategoryContentHealth
)
from app.utils.logger import log


class ContentGapService:
    """Service for analyzing content and merchandising gaps"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds
        self.min_description_length = 200
        self.optimal_description_length = 500
        self.min_images = 3
        self.optimal_images = 5
        self.high_traffic_threshold = 500  # Monthly sessions
        self.low_conversion_threshold = 0.02  # 2%
        self.good_conversion_threshold = 0.04  # 4%

    async def analyze_all_content(self, days: int = 30) -> Dict:
        """
        Complete content gap analysis

        Returns comprehensive analysis of:
        - Content gaps (missing descriptions, images, etc.)
        - Merchandising gaps (cross-sells, categorization)
        - Content opportunities (guides, videos to create)
        - Performance issues (high traffic, low conversion)
        - Category health scores
        """
        try:
            # Detect gaps
            content_gaps = await self.detect_content_gaps()
            merchandising_gaps = await self.detect_merchandising_gaps()

            # Find opportunities
            content_opportunities = await self.find_content_opportunities()

            # Analyze performance
            underperforming_content = await self.find_underperforming_content()

            # Category health
            category_health = await self.analyze_category_health()

            # Identify quick wins
            quick_wins = self._identify_quick_wins(
                content_gaps, merchandising_gaps, content_opportunities
            )

            # Calculate summary metrics
            summary = self._calculate_summary(
                content_gaps, merchandising_gaps,
                content_opportunities, underperforming_content
            )

            return {
                "content_gaps": content_gaps,
                "merchandising_gaps": merchandising_gaps,
                "content_opportunities": content_opportunities,
                "underperforming_content": underperforming_content,
                "category_health": category_health,
                "quick_wins": quick_wins,
                "summary": summary
            }

        except Exception as e:
            log.error(f"Error in content gap analysis: {str(e)}")
            raise

    async def detect_content_gaps(self) -> List[Dict]:
        """
        Detect content gaps on product pages

        Identifies:
        - Missing or short descriptions
        - Insufficient images
        - Missing videos
        - Missing size guides
        - Missing product specs
        - Poor SEO metadata
        """
        gaps = []

        try:
            # In production, would query Shopify products
            # For now, return structured example data

            # Example: Products with short descriptions
            gaps.append({
                "gap_type": "short_description",
                "gap_severity": "high",
                "product_handle": "modern-bathroom-sink",
                "product_title": "Modern Bathroom Sink",
                "category": "Bathroom Sinks",
                "current_state": {
                    "description_length": 87,
                    "has_bullet_points": False,
                    "has_specifications": False
                },
                "expected_state": {
                    "description_length": 500,
                    "has_bullet_points": True,
                    "has_specifications": True,
                    "sections": ["Overview", "Features", "Specifications", "Installation"]
                },
                "impact": {
                    "monthly_traffic": 1240,
                    "current_conversion_rate": 0.018,
                    "expected_conversion_rate": 0.035,
                    "estimated_revenue_impact": 892.00
                },
                "effort": {
                    "hours": 1.5,
                    "resources": ["copywriter"],
                    "priority_score": 594.67  # 892 / 1.5
                },
                "priority_level": "high"
            })

            # Example: Missing images
            gaps.append({
                "gap_type": "missing_images",
                "gap_severity": "critical",
                "product_handle": "corner-bathroom-vanity",
                "product_title": "Corner Bathroom Vanity",
                "category": "Vanities",
                "current_state": {
                    "image_count": 1,
                    "has_lifestyle_images": False,
                    "has_dimension_images": False,
                    "has_installation_images": False
                },
                "expected_state": {
                    "image_count": 5,
                    "has_lifestyle_images": True,
                    "has_dimension_images": True,
                    "has_installation_images": True
                },
                "impact": {
                    "monthly_traffic": 2180,
                    "current_conversion_rate": 0.012,
                    "expected_conversion_rate": 0.032,
                    "estimated_revenue_impact": 1560.00
                },
                "effort": {
                    "hours": 3.0,
                    "resources": ["photographer", "photo_editor"],
                    "priority_score": 520.00  # 1560 / 3
                },
                "priority_level": "critical"
            })

            # Example: Missing size guide
            gaps.append({
                "gap_type": "missing_size_guide",
                "gap_severity": "medium",
                "product_handle": "rainfall-shower-head",
                "product_title": "Rainfall Shower Head",
                "category": "Shower Heads",
                "current_state": {
                    "has_size_guide": False,
                    "has_dimensions": True,
                    "dimension_format": "text_only"
                },
                "expected_state": {
                    "has_size_guide": True,
                    "has_dimensions": True,
                    "dimension_format": "visual_diagram"
                },
                "impact": {
                    "monthly_traffic": 890,
                    "current_conversion_rate": 0.024,
                    "expected_conversion_rate": 0.031,
                    "estimated_revenue_impact": 310.00
                },
                "effort": {
                    "hours": 2.0,
                    "resources": ["designer"],
                    "priority_score": 155.00  # 310 / 2
                },
                "priority_level": "medium"
            })

            # Example: Missing video
            gaps.append({
                "gap_type": "missing_video",
                "gap_severity": "medium",
                "product_handle": "kitchen-faucet-pulldown",
                "product_title": "Kitchen Faucet with Pull-Down Sprayer",
                "category": "Kitchen Faucets",
                "current_state": {
                    "has_video": False,
                    "image_count": 4
                },
                "expected_state": {
                    "has_video": True,
                    "video_types": ["product_demo", "installation_guide"]
                },
                "impact": {
                    "monthly_traffic": 1450,
                    "current_conversion_rate": 0.028,
                    "expected_conversion_rate": 0.039,
                    "estimated_revenue_impact": 680.00
                },
                "effort": {
                    "hours": 8.0,
                    "resources": ["videographer", "video_editor"],
                    "priority_score": 85.00  # 680 / 8
                },
                "priority_level": "low"
            })

            # Sort by priority score
            gaps.sort(key=lambda x: x['effort']['priority_score'], reverse=True)

            return gaps

        except Exception as e:
            log.error(f"Error detecting content gaps: {str(e)}")
            return []

    async def detect_merchandising_gaps(self) -> List[Dict]:
        """
        Detect merchandising gaps

        Identifies:
        - Missing cross-sells
        - Missing upsells
        - Poor categorization
        - No bundle opportunities
        - Missing related products
        """
        gaps = []

        try:
            # Example: Missing cross-sells
            gaps.append({
                "gap_type": "missing_cross_sells",
                "product_handle": "bathroom-sink",
                "product_title": "Modern Bathroom Sink",
                "category": "Bathroom Sinks",
                "description": "No cross-sell recommendations. Customers who buy sinks typically also buy faucets (67% attach rate).",
                "current_state": {
                    "cross_sell_count": 0,
                    "has_recommended_products": False
                },
                "recommended_state": {
                    "cross_sell_count": 3,
                    "recommended_products": [
                        "Single-Handle Faucet",
                        "Drain Assembly",
                        "P-Trap Kit"
                    ],
                    "expected_attach_rate": 0.67
                },
                "impact": {
                    "monthly_product_sales": 48,
                    "current_cross_sell_revenue": 0,
                    "potential_cross_sell_revenue": 2240.00,
                    "estimated_impact": 2240.00
                },
                "effort": {
                    "level": "low",
                    "hours": 0.5
                },
                "priority_score": 4480.00,  # 2240 / 0.5
                "priority_level": "critical"
            })

            # Example: Poor categorization
            gaps.append({
                "gap_type": "poor_categorization",
                "product_handle": "modern-vanity-light",
                "product_title": "Modern Vanity Light Fixture",
                "category": "Other",
                "description": "Product is in 'Other' category instead of 'Bathroom Lighting'. Reduces discoverability.",
                "current_state": {
                    "category": "Other",
                    "collection_count": 0,
                    "in_navigation": False
                },
                "recommended_state": {
                    "category": "Bathroom Lighting",
                    "collections": ["Modern Lighting", "Vanity Lights", "New Arrivals"],
                    "in_navigation": True
                },
                "impact": {
                    "monthly_revenue": 580.00,
                    "estimated_traffic_increase": 1.45,  # 45% more traffic
                    "estimated_impact": 340.00
                },
                "effort": {
                    "level": "low",
                    "hours": 0.25
                },
                "priority_score": 1360.00,  # 340 / 0.25
                "priority_level": "high"
            })

            # Example: Missing bundle opportunity
            gaps.append({
                "gap_type": "no_bundle_opportunities",
                "product_handle": "complete-bathroom-set",
                "product_title": "Complete Bathroom Renovation Set",
                "category": "Bundles",
                "description": "High-value bundle opportunity: sink + faucet + vanity bought together 34% of the time, but no bundle exists.",
                "current_state": {
                    "is_bundle": False,
                    "bundle_exists": False
                },
                "recommended_state": {
                    "is_bundle": True,
                    "bundle_products": ["Modern Sink", "Single-Handle Faucet", "24-inch Vanity"],
                    "bundle_discount": 0.15,
                    "bundle_price": 599.00,
                    "individual_price": 705.00
                },
                "impact": {
                    "purchase_together_count": 16,  # Per month
                    "current_bundle_revenue": 0,
                    "potential_bundle_revenue": 9584.00,  # 16 × $599
                    "estimated_impact": 1200.00  # Conservative estimate
                },
                "effort": {
                    "level": "medium",
                    "hours": 3.0
                },
                "priority_score": 400.00,  # 1200 / 3
                "priority_level": "high"
            })

            # Sort by priority score
            gaps.sort(key=lambda x: x['priority_score'], reverse=True)

            return gaps

        except Exception as e:
            log.error(f"Error detecting merchandising gaps: {str(e)}")
            return []

    async def find_content_opportunities(self) -> List[Dict]:
        """
        Find high-impact content to create

        Identifies opportunities for:
        - Buying guides
        - How-to videos
        - Comparison charts
        - Installation guides
        - Blog posts
        """
        opportunities = []

        try:
            # Example: Buying guide
            opportunities.append({
                "opportunity_type": "buying_guide",
                "topic": "Bathroom Sink Buying Guide",
                "target_audience": "first-time home buyers, renovators",
                "description": "Comprehensive guide to choosing the right bathroom sink (size, material, style, installation type).",
                "current_state": {
                    "exists": False,
                    "competitors_have_it": True,
                    "competitor_count": 7
                },
                "opportunity_metrics": {
                    "search_volume": 2400,  # Monthly
                    "keyword_difficulty": 32,
                    "estimated_monthly_traffic": 480,
                    "estimated_conversion_rate": 0.05,
                    "estimated_monthly_revenue": 1680.00
                },
                "effort": {
                    "hours": 12.0,
                    "resources": ["content_writer", "SEO_specialist", "designer"],
                    "estimated_cost": 850.00
                },
                "priority_score": 140.00,  # 1680 / 12
                "priority_level": "high",
                "recommended_format": ["long_form_blog_post", "infographic", "video_summary"],
                "target_keywords": [
                    "bathroom sink buying guide",
                    "how to choose bathroom sink",
                    "best bathroom sink types",
                    "bathroom sink size guide"
                ]
            })

            # Example: How-to video
            opportunities.append({
                "opportunity_type": "how_to_video",
                "topic": "How to Install a Bathroom Faucet",
                "target_audience": "DIY homeowners, contractors",
                "description": "Step-by-step video guide for installing bathroom faucets.",
                "current_state": {
                    "exists": False,
                    "competitors_have_it": True,
                    "competitor_count": 12
                },
                "opportunity_metrics": {
                    "search_volume": 5400,  # Monthly (YouTube + Google)
                    "keyword_difficulty": 28,
                    "estimated_monthly_traffic": 860,
                    "estimated_conversion_rate": 0.03,
                    "estimated_monthly_revenue": 940.00
                },
                "effort": {
                    "hours": 16.0,
                    "resources": ["videographer", "video_editor", "installer"],
                    "estimated_cost": 1200.00
                },
                "priority_score": 58.75,  # 940 / 16
                "priority_level": "medium",
                "recommended_format": ["youtube_video", "embedded_product_page", "blog_post_transcript"],
                "target_keywords": [
                    "how to install bathroom faucet",
                    "bathroom faucet installation",
                    "replace bathroom faucet",
                    "faucet installation tutorial"
                ]
            })

            # Example: Comparison chart
            opportunities.append({
                "opportunity_type": "comparison_chart",
                "topic": "Bathroom Vanity Material Comparison",
                "target_audience": "homeowners comparing vanity options",
                "description": "Side-by-side comparison of vanity materials (wood, MDF, plywood, etc.) with pros/cons.",
                "current_state": {
                    "exists": False,
                    "competitors_have_it": False,
                    "competitor_count": 2
                },
                "opportunity_metrics": {
                    "search_volume": 720,
                    "keyword_difficulty": 18,
                    "estimated_monthly_traffic": 290,
                    "estimated_conversion_rate": 0.08,
                    "estimated_monthly_revenue": 1160.00
                },
                "effort": {
                    "hours": 6.0,
                    "resources": ["content_writer", "designer"],
                    "estimated_cost": 400.00
                },
                "priority_score": 193.33,  # 1160 / 6
                "priority_level": "high",
                "recommended_format": ["interactive_table", "infographic", "downloadable_pdf"],
                "target_keywords": [
                    "bathroom vanity materials",
                    "best vanity material",
                    "wood vs MDF vanity",
                    "vanity material comparison"
                ]
            })

            # Sort by priority score
            opportunities.sort(key=lambda x: x['priority_score'], reverse=True)

            return opportunities

        except Exception as e:
            log.error(f"Error finding content opportunities: {str(e)}")
            return []

    async def find_underperforming_content(self) -> List[Dict]:
        """
        Find content with high traffic but low conversion

        These are optimization opportunities - the traffic is there,
        but content isn't converting.
        """
        underperforming = []

        try:
            # Example: High traffic, low conversion product page
            underperforming.append({
                "page_url": "/products/modern-bathroom-sink",
                "page_title": "Modern Bathroom Sink",
                "content_type": "product_page",
                "performance": {
                    "monthly_sessions": 3240,
                    "monthly_pageviews": 4180,
                    "avg_time_on_page": 47,  # seconds
                    "bounce_rate": 0.68,
                    "scroll_depth_avg": 0.42,  # Only scrolling 42%
                    "conversion_rate": 0.014,  # 1.4% - very low
                    "monthly_conversions": 45,
                    "monthly_revenue": 3150.00
                },
                "content_quality": {
                    "word_count": 92,
                    "image_count": 2,
                    "video_count": 0,
                    "has_cta": True,
                    "readability_score": 65
                },
                "performance_category": "high_traffic_low_conversion",
                "issues_identified": [
                    "Description too short (92 words, should be 300+)",
                    "Only 2 images (need 5+)",
                    "No product video",
                    "High bounce rate (68%) suggests poor content fit",
                    "Low scroll depth (42%) - content not engaging"
                ],
                "optimization_potential": {
                    "current_conversion_rate": 0.014,
                    "benchmark_conversion_rate": 0.035,
                    "estimated_optimized_conversion_rate": 0.032,
                    "estimated_revenue_gain": 4536.00  # Per month
                },
                "recommended_fixes": [
                    "Expand product description to 500+ words",
                    "Add 3 more lifestyle images",
                    "Create product demonstration video",
                    "Add customer reviews section",
                    "Improve mobile layout (check mobile bounce rate)"
                ]
            })

            # Example: Category page with traffic but poor conversion
            underperforming.append({
                "page_url": "/collections/kitchen-faucets",
                "page_title": "Kitchen Faucets",
                "content_type": "category_page",
                "performance": {
                    "monthly_sessions": 2180,
                    "monthly_pageviews": 3420,
                    "avg_time_on_page": 89,
                    "bounce_rate": 0.52,
                    "scroll_depth_avg": 0.67,
                    "conversion_rate": 0.019,
                    "monthly_conversions": 41,
                    "monthly_revenue": 2870.00
                },
                "content_quality": {
                    "word_count": 0,  # No description
                    "image_count": 0,  # No banner
                    "has_cta": False,
                    "has_filters": True,
                    "filter_usage_rate": 0.23
                },
                "performance_category": "high_traffic_low_conversion",
                "issues_identified": [
                    "No category description or buying guide",
                    "No hero banner or featured products",
                    "Missing filters (finish, style, features)",
                    "No educational content to help buyers choose"
                ],
                "optimization_potential": {
                    "current_conversion_rate": 0.019,
                    "benchmark_conversion_rate": 0.038,
                    "estimated_optimized_conversion_rate": 0.034,
                    "estimated_revenue_gain": 2139.00
                },
                "recommended_fixes": [
                    "Add category description with buying tips",
                    "Create hero banner with featured products",
                    "Add more filter options (finish, mount type, features)",
                    "Embed 'How to Choose' guide",
                    "Add customer favorite badges"
                ]
            })

            # Sort by revenue opportunity
            underperforming.sort(
                key=lambda x: x['optimization_potential']['estimated_revenue_gain'],
                reverse=True
            )

            return underperforming

        except Exception as e:
            log.error(f"Error finding underperforming content: {str(e)}")
            return []

    async def analyze_category_health(self) -> List[Dict]:
        """
        Analyze content health by category

        Returns health scores (0-100) for each category across:
        - Description completeness
        - Image quality
        - SEO optimization
        - Merchandising completeness
        """
        categories = []

        try:
            # Example: Bathroom Sinks category
            categories.append({
                "category_name": "Bathroom Sinks",
                "product_count": 47,
                "health_scores": {
                    "description_score": 62,  # 62% have good descriptions
                    "image_score": 45,  # 45% have 4+ images
                    "seo_score": 71,  # 71% have meta descriptions
                    "merchandising_score": 38,  # 38% have cross-sells
                    "overall_health_score": 54  # Weighted average
                },
                "gaps": {
                    "total_gaps": 89,
                    "critical_gaps": 12,
                    "by_type": {
                        "short_description": 18,
                        "missing_images": 26,
                        "missing_cross_sells": 29,
                        "poor_seo": 16
                    }
                },
                "performance": {
                    "avg_conversion_rate": 0.026,
                    "total_monthly_revenue": 14280.00
                },
                "opportunity": {
                    "estimated_revenue_if_optimized": 22840.00,
                    "revenue_opportunity": 8560.00
                },
                "top_priorities": [
                    "Add cross-sells to 29 products (biggest revenue impact)",
                    "Improve images on 26 products",
                    "Expand descriptions on 18 products"
                ]
            })

            # Example: Kitchen Faucets category
            categories.append({
                "category_name": "Kitchen Faucets",
                "product_count": 34,
                "health_scores": {
                    "description_score": 79,
                    "image_score": 68,
                    "seo_score": 85,
                    "merchandising_score": 56,
                    "overall_health_score": 72
                },
                "gaps": {
                    "total_gaps": 42,
                    "critical_gaps": 5,
                    "by_type": {
                        "short_description": 7,
                        "missing_images": 11,
                        "missing_cross_sells": 15,
                        "poor_seo": 9
                    }
                },
                "performance": {
                    "avg_conversion_rate": 0.034,
                    "total_monthly_revenue": 18420.00
                },
                "opportunity": {
                    "estimated_revenue_if_optimized": 22190.00,
                    "revenue_opportunity": 3770.00
                },
                "top_priorities": [
                    "Add cross-sells to 15 products",
                    "Improve images on 11 products",
                    "Expand descriptions on 7 products"
                ]
            })

            # Sort by opportunity
            categories.sort(key=lambda x: x['opportunity']['revenue_opportunity'], reverse=True)

            return categories

        except Exception as e:
            log.error(f"Error analyzing category health: {str(e)}")
            return []

    def _identify_quick_wins(
        self,
        content_gaps: List[Dict],
        merchandising_gaps: List[Dict],
        content_opportunities: List[Dict]
    ) -> List[Dict]:
        """Identify low-effort, high-impact improvements"""
        quick_wins = []

        # Quick wins from content gaps (effort < 2 hours, impact > $500)
        for gap in content_gaps:
            if gap['effort']['hours'] <= 2.0 and gap['impact']['estimated_revenue_impact'] >= 500:
                quick_wins.append({
                    "type": "content_gap",
                    "title": f"Fix {gap['gap_type']} on {gap['product_title']}",
                    "effort": f"{gap['effort']['hours']} hours",
                    "impact": f"${gap['impact']['estimated_revenue_impact']:.0f}/month",
                    "action": self._get_gap_action(gap),
                    "priority_score": gap['effort']['priority_score']
                })

        # Quick wins from merchandising (effort = low, impact > $500)
        for gap in merchandising_gaps:
            if gap['effort']['level'] == 'low' and gap['impact']['estimated_impact'] >= 500:
                quick_wins.append({
                    "type": "merchandising_gap",
                    "title": f"Fix {gap['gap_type']} on {gap['product_title']}",
                    "effort": f"{gap['effort']['hours']} hours",
                    "impact": f"${gap['impact']['estimated_impact']:.0f}/month",
                    "action": self._get_merchandising_action(gap),
                    "priority_score": gap['priority_score']
                })

        # Sort by priority score
        quick_wins.sort(key=lambda x: x['priority_score'], reverse=True)

        return quick_wins[:10]  # Top 10

    def _get_gap_action(self, gap: Dict) -> str:
        """Get recommended action for content gap"""
        gap_type = gap['gap_type']

        actions = {
            'short_description': 'Expand product description to 500+ words',
            'missing_images': 'Add 3-4 lifestyle and detail images',
            'missing_video': 'Create product demonstration video',
            'missing_size_guide': 'Add visual size guide or dimension diagram'
        }

        return actions.get(gap_type, f"Fix {gap_type}")

    def _get_merchandising_action(self, gap: Dict) -> str:
        """Get recommended action for merchandising gap"""
        gap_type = gap['gap_type']

        actions = {
            'missing_cross_sells': 'Add 3 cross-sell product recommendations',
            'poor_categorization': 'Move to correct category and collections',
            'no_bundle_opportunities': 'Create product bundle with discount'
        }

        return actions.get(gap_type, f"Fix {gap_type}")

    def _calculate_summary(
        self,
        content_gaps: List[Dict],
        merchandising_gaps: List[Dict],
        content_opportunities: List[Dict],
        underperforming: List[Dict]
    ) -> Dict:
        """Calculate summary metrics"""
        return {
            "total_gaps": len(content_gaps) + len(merchandising_gaps),
            "content_gaps_count": len(content_gaps),
            "merchandising_gaps_count": len(merchandising_gaps),
            "opportunities_count": len(content_opportunities),
            "underperforming_pages_count": len(underperforming),

            "total_revenue_opportunity": sum([
                sum(g['impact']['estimated_revenue_impact'] for g in content_gaps),
                sum(g['impact']['estimated_impact'] for g in merchandising_gaps),
                sum(o['opportunity_metrics']['estimated_monthly_revenue'] for o in content_opportunities),
                sum(u['optimization_potential']['estimated_revenue_gain'] for u in underperforming)
            ]),

            "critical_gaps": len([
                g for g in content_gaps if g['gap_severity'] == 'critical'
            ]) + len([
                g for g in merchandising_gaps if g.get('priority_level') == 'critical'
            ]),

            "top_category_by_opportunity": "Bathroom Sinks",
            "avg_content_health_score": 63
        }

    async def get_content_dashboard(self, days: int = 30) -> Dict:
        """Get complete content & merchandising dashboard"""
        analysis = await self.analyze_all_content(days)

        return {
            "overview": {
                "total_gaps": analysis['summary']['total_gaps'],
                "critical_gaps": analysis['summary']['critical_gaps'],
                "total_opportunity": analysis['summary']['total_revenue_opportunity'],
                "avg_health_score": analysis['summary']['avg_content_health_score']
            },
            "top_priorities": analysis['quick_wins'][:5],
            "content_gaps_summary": {
                "count": len(analysis['content_gaps']),
                "top_gaps": analysis['content_gaps'][:5]
            },
            "merchandising_gaps_summary": {
                "count": len(analysis['merchandising_gaps']),
                "top_gaps": analysis['merchandising_gaps'][:5]
            },
            "content_opportunities_summary": {
                "count": len(analysis['content_opportunities']),
                "top_opportunities": analysis['content_opportunities'][:3]
            },
            "underperforming_content_summary": {
                "count": len(analysis['underperforming_content']),
                "top_pages": analysis['underperforming_content'][:3]
            },
            "category_health": analysis['category_health']
        }
