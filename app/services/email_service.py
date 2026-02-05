"""
Email & Retention Intelligence Service

Analyzes email marketing performance to find revenue opportunities.
Answers: "Am I emailing enough? What flows are underperforming?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from decimal import Decimal

from app.models.email import (
    EmailCampaign,
    EmailFlow,
    EmailSegment,
    EmailSendFrequency,
    EmailRevenueOpportunity
)
from app.utils.logger import log


class EmailService:
    """Service for email marketing intelligence"""

    def __init__(self, db: Session):
        self.db = db

        # Flow benchmarks (industry averages)
        self.flow_benchmarks = {
            'welcome': {
                'open_rate': 0.50,  # 50%
                'click_rate': 0.15,
                'conversion_rate': 0.08
            },
            'abandoned_cart': {
                'open_rate': 0.45,
                'click_rate': 0.20,
                'conversion_rate': 0.18  # 15-20%
            },
            'browse_abandonment': {
                'open_rate': 0.40,
                'click_rate': 0.12,
                'conversion_rate': 0.05
            },
            'post_purchase': {
                'open_rate': 0.45,
                'click_rate': 0.12,
                'conversion_rate': 0.10
            },
            'winback': {
                'open_rate': 0.35,
                'click_rate': 0.10,
                'conversion_rate': 0.06
            },
            'default': {
                'open_rate': 0.35,
                'click_rate': 0.10,
                'conversion_rate': 0.05
            }
        }

        # Thresholds
        self.high_value_threshold = Decimal('200.00')  # Avg customer value > $200
        self.under_contacted_days = 30  # No send in 30+ days = under-contacted
        self.engagement_threshold = 0.15  # 15% open/click rate = engaged

    async def analyze_all_opportunities(self, days: int = 30) -> Dict:
        """
        Identify all email revenue opportunities

        Returns comprehensive analysis of email program
        """
        log.info(f"Analyzing email opportunities (last {days} days)")

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        results = {
            'period_days': days,
            'period_start': start_date.isoformat(),
            'period_end': end_date.isoformat(),
            'opportunities': {
                'underperforming_flows': [],
                'under_contacted_segments': [],
                'frequency_optimization': None,
                'missing_flows': []
            },
            'summary': {}
        }

        # 1. Analyze flow performance
        flow_opps = await self.find_underperforming_flows(days)
        results['opportunities']['underperforming_flows'] = flow_opps

        # 2. Find under-contacted segments
        segment_opps = await self.find_under_contacted_segments()
        results['opportunities']['under_contacted_segments'] = segment_opps

        # 3. Analyze send frequency
        frequency_analysis = await self.analyze_send_frequency(days)
        results['opportunities']['frequency_optimization'] = frequency_analysis

        # 4. Identify missing flows
        missing_flows = await self.identify_missing_flows()
        results['opportunities']['missing_flows'] = missing_flows

        # Calculate total opportunity
        total_revenue_opportunity = (
            sum(f.get('estimated_revenue_gap', 0) for f in flow_opps) +
            sum(s.get('revenue_opportunity', 0) for s in segment_opps) +
            (frequency_analysis.get('estimated_revenue_impact', 0) if frequency_analysis else 0) +
            sum(f.get('estimated_monthly_revenue', 0) for f in missing_flows)
        )

        results['summary'] = {
            'total_revenue_opportunity': float(total_revenue_opportunity),
            'underperforming_flows_count': len(flow_opps),
            'under_contacted_segments_count': len(segment_opps),
            'missing_flows_count': len(missing_flows),
            'can_send_more': frequency_analysis.get('can_send_more', False) if frequency_analysis else False
        }

        log.info(f"Found ${total_revenue_opportunity:,.0f}/month in email opportunities")

        return results

    async def find_underperforming_flows(self, days: int = 30) -> List[Dict]:
        """
        Find email flows performing below benchmarks

        Returns flows with improvement opportunities
        """
        log.info("Finding underperforming email flows")

        flows = self.db.query(EmailFlow).filter(
            EmailFlow.is_active == True,
            EmailFlow.period_days == days
        ).all()

        opportunities = []

        for flow in flows:
            # Get benchmark for this flow type
            benchmark = self.flow_benchmarks.get(
                flow.flow_type,
                self.flow_benchmarks['default']
            )

            # Calculate performance vs benchmark
            open_rate_gap = (benchmark['open_rate'] - flow.open_rate) if flow.open_rate else benchmark['open_rate']
            click_rate_gap = (benchmark['click_rate'] - flow.click_rate) if flow.click_rate else benchmark['click_rate']
            conversion_rate_gap = (benchmark['conversion_rate'] - flow.conversion_rate) if flow.conversion_rate else benchmark['conversion_rate']

            # Flag if significantly underperforming (>20% below benchmark)
            is_underperforming = (
                open_rate_gap > (benchmark['open_rate'] * 0.20) or
                click_rate_gap > (benchmark['click_rate'] * 0.20) or
                conversion_rate_gap > (benchmark['conversion_rate'] * 0.20)
            )

            if is_underperforming and flow.total_entered > 50:  # Min volume threshold
                # Estimate revenue gap
                if flow.conversion_rate and benchmark['conversion_rate']:
                    potential_conversions = flow.total_entered * benchmark['conversion_rate']
                    actual_conversions = flow.total_conversions
                    missed_conversions = potential_conversions - actual_conversions

                    # Estimate revenue per conversion
                    revenue_per_conversion = (
                        float(flow.revenue_per_recipient) / flow.conversion_rate
                        if flow.conversion_rate > 0
                        else 50.0  # Default estimate
                    )

                    estimated_revenue_gap = missed_conversions * revenue_per_conversion
                else:
                    estimated_revenue_gap = 0

                # Diagnose issues
                issues = self._diagnose_flow_issues(flow, benchmark)

                opportunity = {
                    'flow_name': flow.flow_name,
                    'flow_type': flow.flow_type,
                    'is_active': flow.is_active,
                    'total_emails': flow.total_emails,

                    'current_performance': {
                        'open_rate': round(flow.open_rate * 100, 1),
                        'click_rate': round(flow.click_rate * 100, 1),
                        'conversion_rate': round(flow.conversion_rate * 100, 1),
                        'revenue': float(flow.total_revenue)
                    },

                    'benchmark': {
                        'open_rate': round(benchmark['open_rate'] * 100, 1),
                        'click_rate': round(benchmark['click_rate'] * 100, 1),
                        'conversion_rate': round(benchmark['conversion_rate'] * 100, 1)
                    },

                    'gaps': {
                        'open_rate': round(open_rate_gap * 100, 1),
                        'click_rate': round(click_rate_gap * 100, 1),
                        'conversion_rate': round(conversion_rate_gap * 100, 1)
                    },

                    'estimated_revenue_gap': round(estimated_revenue_gap, 2),

                    'issues': issues,
                    'recommended_actions': self._recommend_flow_fixes(flow, issues, benchmark),

                    'priority': 'critical' if estimated_revenue_gap > 2000 else 'high'
                }

                opportunities.append(opportunity)

        # Sort by revenue gap
        opportunities.sort(key=lambda x: x['estimated_revenue_gap'], reverse=True)

        log.info(f"Found {len(opportunities)} underperforming flows")

        return opportunities

    async def find_under_contacted_segments(self) -> List[Dict]:
        """
        Find high-value segments that aren't being contacted enough

        Returns segments with revenue opportunities
        """
        log.info("Finding under-contacted segments")

        segments = self.db.query(EmailSegment).filter(
            EmailSegment.is_high_value == True,
            EmailSegment.is_under_contacted == True,
            EmailSegment.total_profiles > 100  # Min size threshold
        ).order_by(
            desc(EmailSegment.revenue_opportunity)
        ).limit(10).all()

        opportunities = []

        for segment in segments:
            opportunity = {
                'segment_name': segment.segment_name,
                'total_profiles': segment.total_profiles,
                'engaged_profiles': segment.engaged_profiles,

                'value_indicators': {
                    'avg_customer_value': float(segment.avg_customer_value),
                    'total_segment_value': float(segment.total_segment_value),
                    'avg_orders_per_customer': round(segment.avg_orders_per_customer, 1)
                },

                'contact_history': {
                    'days_since_last_send': segment.days_since_last_send,
                    'sends_last_30_days': segment.sends_last_30_days,
                    'avg_sends_per_week': round(segment.avg_sends_per_week, 1)
                },

                'engagement_metrics': {
                    'open_rate_90d': round(segment.open_rate_90d * 100, 1),
                    'click_rate_90d': round(segment.click_rate_90d * 100, 1),
                    'conversion_rate_90d': round(segment.conversion_rate_90d * 100, 1)
                },

                'issue': f"High-value segment ({segment.total_profiles} customers) not contacted in {segment.days_since_last_send} days",
                'opportunity': 'These are engaged, valuable customers going cold',

                'estimated_response_rate': round(segment.estimated_response_rate * 100, 1) if segment.estimated_response_rate else None,
                'revenue_opportunity': float(segment.revenue_opportunity),

                'recommended_action': f"Send targeted campaign to {segment.segment_name}",
                'recommended_frequency': segment.recommended_frequency,

                'priority': 'high' if segment.revenue_opportunity > 2000 else 'medium'
            }

            opportunities.append(opportunity)

        log.info(f"Found {len(opportunities)} under-contacted segments")

        return opportunities

    async def analyze_send_frequency(self, days: int = 30) -> Optional[Dict]:
        """
        Analyze overall send frequency

        Answers: Are we sending too much or too little?
        """
        log.info("Analyzing email send frequency")

        # Get latest frequency analysis
        freq_analysis = self.db.query(EmailSendFrequency).filter(
            EmailSendFrequency.period_days == days
        ).order_by(
            desc(EmailSendFrequency.calculated_at)
        ).first()

        if not freq_analysis:
            log.warning("No send frequency data available")
            return None

        result = {
            'current_frequency': {
                'emails_per_week': round(freq_analysis.avg_emails_per_subscriber_week, 1),
                'emails_per_month': round(freq_analysis.avg_emails_per_subscriber_month, 1),
                'total_campaigns': freq_analysis.total_campaigns_sent,
                'total_flow_emails': freq_analysis.total_flow_emails_sent
            },

            'engagement_dropoff_threshold': round(freq_analysis.engagement_dropoff_threshold, 1) if freq_analysis.engagement_dropoff_threshold else None,

            'can_send_more': freq_analysis.can_send_more,
            'optimal_frequency': round(freq_analysis.optimal_frequency, 1) if freq_analysis.optimal_frequency else None,
            'recommended_increase_pct': round(freq_analysis.recommended_increase_pct, 1) if freq_analysis.recommended_increase_pct else None,

            'estimated_revenue_impact': float(freq_analysis.estimated_revenue_from_frequency_change),

            'engagement_by_frequency': freq_analysis.engagement_by_frequency,

            'recommendation': self._generate_frequency_recommendation(freq_analysis)
        }

        return result

    async def identify_missing_flows(self) -> List[Dict]:
        """
        Identify standard flows that don't exist or aren't set up

        Returns missing flow opportunities
        """
        log.info("Identifying missing email flows")

        # Standard flows every ecommerce business should have
        standard_flows = [
            {
                'type': 'welcome',
                'name': 'Welcome Series',
                'description': 'Onboard new subscribers',
                'estimated_conversion_rate': 0.08,
                'estimated_monthly_revenue': 1500
            },
            {
                'type': 'abandoned_cart',
                'name': 'Abandoned Cart',
                'description': 'Recover abandoned checkouts',
                'estimated_conversion_rate': 0.18,
                'estimated_monthly_revenue': 3000
            },
            {
                'type': 'browse_abandonment',
                'name': 'Browse Abandonment',
                'description': 'Re-engage browsers who didn\'t add to cart',
                'estimated_conversion_rate': 0.05,
                'estimated_monthly_revenue': 800
            },
            {
                'type': 'post_purchase',
                'name': 'Post-Purchase',
                'description': 'Review requests, cross-sells, replenishment',
                'estimated_conversion_rate': 0.10,
                'estimated_monthly_revenue': 1800
            },
            {
                'type': 'winback',
                'name': 'Win-Back',
                'description': 'Re-engage churned customers',
                'estimated_conversion_rate': 0.06,
                'estimated_monthly_revenue': 1200
            }
        ]

        # Check which flows exist
        existing_flows = self.db.query(EmailFlow.flow_type).filter(
            EmailFlow.is_active == True
        ).all()
        existing_flow_types = {f[0] for f in existing_flows}

        missing = []

        for flow in standard_flows:
            if flow['type'] not in existing_flow_types:
                missing.append({
                    'flow_type': flow['type'],
                    'flow_name': flow['name'],
                    'description': flow['description'],
                    'issue': f"{flow['name']} flow not set up",
                    'estimated_monthly_revenue': flow['estimated_monthly_revenue'],
                    'estimated_conversion_rate': round(flow['estimated_conversion_rate'] * 100, 1),
                    'recommended_action': f"Set up {flow['name']} flow",
                    'priority': 'critical' if flow['type'] in ['abandoned_cart', 'post_purchase'] else 'high'
                })

        log.info(f"Found {len(missing)} missing flows")

        return missing

    def _diagnose_flow_issues(self, flow: EmailFlow, benchmark: Dict) -> List[str]:
        """
        Diagnose specific issues with a flow

        Returns list of issues found
        """
        issues = []

        # Low open rate
        if flow.open_rate < (benchmark['open_rate'] * 0.8):
            issues.append(f"Open rate ({flow.open_rate * 100:.1f}%) below benchmark ({benchmark['open_rate'] * 100:.1f}%)")

        # Low click rate
        if flow.click_rate < (benchmark['click_rate'] * 0.8):
            issues.append(f"Click rate ({flow.click_rate * 100:.1f}%) below benchmark ({benchmark['click_rate'] * 100:.1f}%)")

        # Low conversion rate
        if flow.conversion_rate < (benchmark['conversion_rate'] * 0.8):
            issues.append(f"Conversion rate ({flow.conversion_rate * 100:.1f}%) below benchmark ({benchmark['conversion_rate'] * 100:.1f}%)")

        # Single email flow (should have sequence)
        if flow.total_emails == 1 and flow.flow_type in ['abandoned_cart', 'browse_abandonment', 'winback']:
            issues.append(f"Only {flow.total_emails} email in flow - add follow-up sequence")

        # Not enough emails in sequence
        if flow.total_emails < 3 and flow.flow_type in ['welcome', 'post_purchase']:
            issues.append(f"Only {flow.total_emails} emails - consider adding more to sequence")

        return issues

    def _recommend_flow_fixes(self, flow: EmailFlow, issues: List[str], benchmark: Dict) -> List[str]:
        """
        Generate specific fix recommendations for a flow

        Returns list of actionable recommendations
        """
        recommendations = []

        # Based on issues, recommend specific fixes
        if flow.open_rate < (benchmark['open_rate'] * 0.8):
            recommendations.append("Improve subject lines - test urgency, personalization, or benefit-driven copy")

        if flow.click_rate < (benchmark['click_rate'] * 0.8):
            recommendations.append("Strengthen CTAs - make buttons more prominent and action-oriented")

        if flow.conversion_rate < (benchmark['conversion_rate'] * 0.8):
            if flow.flow_type == 'abandoned_cart':
                recommendations.append("Add incentive in 2nd email (10-15% discount)")
            elif flow.flow_type == 'browse_abandonment':
                recommendations.append("Add social proof (reviews, bestsellers)")
            else:
                recommendations.append("Optimize landing page - ensure seamless path to purchase")

        if flow.total_emails == 1 and flow.flow_type in ['abandoned_cart', 'browse_abandonment']:
            recommendations.append(f"Add 2nd email at 24hrs, 3rd email at 72hrs")

        if flow.total_emails < 3 and flow.flow_type == 'welcome':
            recommendations.append("Expand to 3-5 emails: 1) Welcome, 2) Brand story, 3) Bestsellers, 4) Review/social proof")

        if flow.total_emails < 2 and flow.flow_type == 'post_purchase':
            recommendations.append("Add review request at 7 days, cross-sell at 14 days, replenishment reminder at 30 days")

        return recommendations

    def _generate_frequency_recommendation(self, freq_analysis: EmailSendFrequency) -> str:
        """
        Generate human-readable frequency recommendation

        Returns recommendation text
        """
        if freq_analysis.can_send_more:
            return (
                f"OPPORTUNITY: You can send {freq_analysis.recommended_increase_pct:.0f}% more emails "
                f"before engagement drops. "
                f"Current: {freq_analysis.avg_emails_per_subscriber_week:.1f}/week, "
                f"Optimal: {freq_analysis.optimal_frequency:.1f}/week. "
                f"Expected impact: +${freq_analysis.estimated_revenue_from_frequency_change:,.0f}/month"
            )
        else:
            return (
                f"Current frequency ({freq_analysis.avg_emails_per_subscriber_week:.1f}/week) "
                f"is near optimal. Engagement drops off at {freq_analysis.engagement_dropoff_threshold:.1f}/week."
            )

    async def get_email_dashboard(self, days: int = 30) -> Dict:
        """
        Complete email intelligence dashboard

        Everything you need to know about email performance
        """
        log.info("Generating email dashboard")

        # Get all opportunities
        opportunities = await self.analyze_all_opportunities(days)

        # Get top opportunities across all categories
        all_opps = (
            opportunities['opportunities']['underperforming_flows'][:3] +
            opportunities['opportunities']['under_contacted_segments'][:3] +
            opportunities['opportunities']['missing_flows'][:3]
        )

        # Sort by estimated revenue
        all_opps.sort(
            key=lambda x: x.get('estimated_revenue_gap', x.get('revenue_opportunity', x.get('estimated_monthly_revenue', 0))),
            reverse=True
        )

        return {
            'period_days': days,
            'generated_at': datetime.utcnow().isoformat(),

            'summary': opportunities['summary'],

            'top_opportunities': all_opps[:5],  # Top 5 across all categories

            'by_category': opportunities['opportunities'],

            'frequency_analysis': opportunities['opportunities']['frequency_optimization'],

            'recommendations': {
                'immediate_actions': [
                    opp.get('recommended_action', opp.get('recommended_actions', ['No action specified'])[0] if opp.get('recommended_actions') else 'No action specified')
                    for opp in all_opps[:3]
                ],
                'total_revenue_opportunity': opportunities['summary']['total_revenue_opportunity']
            }
        }
