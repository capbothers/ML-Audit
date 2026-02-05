"""
Recommendation Engine
Generates actionable recommendations based on insights from all data sources
"""
from typing import Dict, List, Optional
from datetime import datetime
from app.utils.logger import log
from app.utils.helpers import calculate_percentage_change, format_currency


class RecommendationEngine:
    """
    Generates prioritized, actionable recommendations for growth optimization
    """

    def __init__(self):
        self.recommendations = []

    def generate_recommendations(
        self,
        churn_data: Optional[List[Dict]] = None,
        anomalies: Optional[List[Dict]] = None,
        seo_issues: Optional[List[Dict]] = None,
        campaign_data: Optional[List[Dict]] = None,
        disapproved_ads: Optional[List[Dict]] = None,
        abandoned_checkouts: Optional[List[Dict]] = None,
        email_campaigns: Optional[List[Dict]] = None
    ) -> List[Dict]:
        """
        Generate comprehensive recommendations from all data sources
        """
        log.info("Generating recommendations...")

        recommendations = []

        # Churn-related recommendations
        if churn_data:
            recommendations.extend(self._generate_churn_recommendations(churn_data))

        # Anomaly-based recommendations
        if anomalies:
            recommendations.extend(self._generate_anomaly_recommendations(anomalies))

        # SEO recommendations
        if seo_issues:
            recommendations.extend(self._generate_seo_recommendations(seo_issues))

        # Campaign optimization recommendations
        if campaign_data:
            recommendations.extend(self._generate_campaign_recommendations(campaign_data))

        # Disapproved ads recommendations
        if disapproved_ads:
            recommendations.extend(self._generate_ad_approval_recommendations(disapproved_ads))

        # Abandoned checkout recommendations
        if abandoned_checkouts:
            recommendations.extend(self._generate_checkout_recommendations(abandoned_checkouts))

        # Email campaign recommendations
        if email_campaigns:
            recommendations.extend(self._generate_email_recommendations(email_campaigns))

        # Sort by priority and impact
        recommendations = self._prioritize_recommendations(recommendations)

        log.info(f"Generated {len(recommendations)} recommendations")
        return recommendations

    def _generate_churn_recommendations(self, churn_data: List[Dict]) -> List[Dict]:
        """Generate recommendations for at-risk customers"""
        recommendations = []

        high_risk = [c for c in churn_data if c.get('churn_risk_level') == 'HIGH']

        if len(high_risk) > 0:
            total_value_at_risk = sum(c.get('total_spent', 0) for c in high_risk)

            recommendations.append({
                'type': 'churn_prevention',
                'priority': 'critical',
                'title': f'{len(high_risk)} High-Risk Customers Need Immediate Attention',
                'description': f'You have {len(high_risk)} customers at high risk of churning, representing {format_currency(total_value_at_risk)} in lifetime value.',
                'impact': total_value_at_risk,
                'impact_type': 'revenue_at_risk',
                'recommendations': [
                    'Launch a win-back email campaign targeting these customers',
                    'Offer personalized discounts or incentives',
                    'Send product recommendations based on purchase history',
                    'Create a VIP re-engagement program'
                ],
                'action_items': [
                    {
                        'action': 'Export high-risk customer list to Klaviyo',
                        'priority': 'immediate',
                        'estimated_time': '15 minutes'
                    },
                    {
                        'action': 'Create automated win-back flow',
                        'priority': 'high',
                        'estimated_time': '2 hours'
                    }
                ],
                'metrics_to_track': ['churn_rate', 'reactivation_rate', 'recovered_revenue']
            })

        # Medium risk customers
        medium_risk = [c for c in churn_data if c.get('churn_risk_level') == 'MEDIUM']

        if len(medium_risk) > 50:
            recommendations.append({
                'type': 'churn_prevention',
                'priority': 'high',
                'title': f'{len(medium_risk)} Customers Showing Early Churn Signals',
                'description': f'{len(medium_risk)} customers are showing signs of disengagement. Act now before they become high-risk.',
                'impact': sum(c.get('total_spent', 0) for c in medium_risk) * 0.5,  # 50% potential save rate
                'impact_type': 'potential_recovery',
                'recommendations': [
                    'Send personalized product recommendations',
                    'Create educational content about product usage',
                    'Implement loyalty program incentives',
                    'Survey for feedback on their experience'
                ],
                'action_items': [
                    {
                        'action': 'Set up engagement monitoring alerts',
                        'priority': 'high',
                        'estimated_time': '30 minutes'
                    }
                ]
            })

        return recommendations

    def _generate_anomaly_recommendations(self, anomalies: List[Dict]) -> List[Dict]:
        """Generate recommendations based on detected anomalies"""
        recommendations = []

        # Group anomalies by type
        traffic_drops = [a for a in anomalies if a.get('type') == 'traffic_anomaly' and a.get('direction') == 'drop']
        revenue_drops = [a for a in anomalies if a.get('type') == 'revenue_anomaly' and a.get('direction') == 'drop']
        campaign_spikes = [a for a in anomalies if a.get('type') == 'campaign_anomaly' and a.get('metric') == 'cost']

        # Traffic drop recommendations
        if traffic_drops:
            for anomaly in traffic_drops[:3]:  # Top 3
                recommendations.append({
                    'type': 'traffic_anomaly',
                    'priority': 'critical' if abs(anomaly.get('deviation_pct', 0)) > 50 else 'high',
                    'title': f'Unusual {anomaly.get("traffic_metric", "traffic")} Drop Detected',
                    'description': f'{anomaly.get("traffic_metric")} dropped by {abs(anomaly.get("deviation_pct", 0)):.1f}% on {anomaly.get("date")}',
                    'impact': anomaly.get('value', 0),
                    'impact_type': 'traffic_loss',
                    'recommendations': [
                        'Check if there were technical issues on the site',
                        'Verify Google Analytics tracking is working correctly',
                        'Review any recent changes to SEO or paid campaigns',
                        'Check Search Console for manual actions or indexing issues',
                        'Analyze traffic sources to identify which channel dropped'
                    ],
                    'action_items': [
                        {
                            'action': 'Run technical site audit',
                            'priority': 'immediate',
                            'estimated_time': '30 minutes'
                        },
                        {
                            'action': 'Review recent site changes',
                            'priority': 'immediate',
                            'estimated_time': '20 minutes'
                        }
                    ]
                })

        # Revenue drop recommendations
        if revenue_drops:
            for anomaly in revenue_drops[:2]:
                recommendations.append({
                    'type': 'revenue_anomaly',
                    'priority': 'critical',
                    'title': f'Significant Revenue Drop on {anomaly.get("date")}',
                    'description': f'Revenue dropped {abs(anomaly.get("deviation_pct", 0)):.1f}% below expected levels',
                    'impact': abs(anomaly.get('value', 0) - anomaly.get('expected_value', 0)),
                    'impact_type': 'revenue_loss',
                    'recommendations': [
                        'Investigate checkout process for technical issues',
                        'Review pricing changes or shipping costs',
                        'Check if payment gateway is functioning properly',
                        'Analyze customer feedback for complaints',
                        'Review abandoned cart rate for unusual patterns'
                    ],
                    'action_items': [
                        {
                            'action': 'Test checkout process end-to-end',
                            'priority': 'immediate',
                            'estimated_time': '15 minutes'
                        },
                        {
                            'action': 'Analyze abandoned checkout data',
                            'priority': 'immediate',
                            'estimated_time': '20 minutes'
                        }
                    ]
                })

        # Campaign cost spikes
        if campaign_spikes:
            for anomaly in campaign_spikes[:2]:
                recommendations.append({
                    'type': 'cost_anomaly',
                    'priority': 'high',
                    'title': f'Unusual Ad Spend Spike in {anomaly.get("campaign_name", "Campaign")}',
                    'description': f'Campaign cost increased by {anomaly.get("deviation_pct", 0):.1f}%',
                    'impact': abs(anomaly.get('value', 0) - anomaly.get('expected_value', 0)),
                    'impact_type': 'cost_increase',
                    'recommendations': [
                        'Review campaign bidding strategy',
                        'Check for bid adjustments or automated rules',
                        'Verify daily budget settings',
                        'Analyze if ROAS is still acceptable despite higher spend',
                        'Check for seasonal trends or competitor activity'
                    ],
                    'action_items': [
                        {
                            'action': 'Review campaign settings in Google Ads',
                            'priority': 'high',
                            'estimated_time': '20 minutes'
                        }
                    ]
                })

        return recommendations

    def _generate_seo_recommendations(self, seo_issues: List[Dict]) -> List[Dict]:
        """Generate SEO improvement recommendations"""
        recommendations = []

        critical_issues = [i for i in seo_issues if i.get('severity') == 'critical']

        if critical_issues:
            recommendations.append({
                'type': 'seo_critical',
                'priority': 'high',
                'title': f'{len(critical_issues)} Critical SEO Issues Found',
                'description': 'Critical SEO problems are preventing your site from ranking properly',
                'impact': len(critical_issues) * 1000,  # Estimated traffic impact
                'impact_type': 'seo_traffic_potential',
                'recommendations': [
                    issue.get('message') for issue in critical_issues[:5]
                ],
                'action_items': [
                    {
                        'action': 'Fix critical SEO issues',
                        'priority': 'high',
                        'estimated_time': '1-2 hours'
                    }
                ]
            })

        return recommendations

    def _generate_campaign_recommendations(self, campaign_data: List[Dict]) -> List[Dict]:
        """Generate campaign optimization recommendations"""
        recommendations = []

        # Find underperforming campaigns
        campaigns_with_roas = [c for c in campaign_data if c.get('roas', 0) > 0]

        if campaigns_with_roas:
            avg_roas = sum(c['roas'] for c in campaigns_with_roas) / len(campaigns_with_roas)

            low_roas_campaigns = [c for c in campaigns_with_roas if c['roas'] < avg_roas * 0.5]

            if low_roas_campaigns:
                total_wasted_spend = sum(c.get('cost', 0) for c in low_roas_campaigns)

                recommendations.append({
                    'type': 'campaign_optimization',
                    'priority': 'high',
                    'title': f'{len(low_roas_campaigns)} Underperforming Campaigns',
                    'description': f'{len(low_roas_campaigns)} campaigns have ROAS below 50% of average, wasting {format_currency(total_wasted_spend)}',
                    'impact': total_wasted_spend * 0.7,  # Potential savings
                    'impact_type': 'cost_savings',
                    'recommendations': [
                        'Pause or reduce budget for low ROAS campaigns',
                        'Reallocate budget to top-performing campaigns',
                        'Audit targeting and keyword selection',
                        'Review ad creative and messaging',
                        'Test new ad variations'
                    ],
                    'action_items': [
                        {
                            'action': 'Review and pause underperforming campaigns',
                            'priority': 'high',
                            'estimated_time': '1 hour'
                        }
                    ],
                    'campaigns': [c.get('name') for c in low_roas_campaigns[:5]]
                })

        return recommendations

    def _generate_ad_approval_recommendations(self, disapproved_ads: List[Dict]) -> List[Dict]:
        """Generate recommendations for disapproved ads"""
        recommendations = []

        if disapproved_ads:
            recommendations.append({
                'type': 'ad_disapproval',
                'priority': 'critical',
                'title': f'{len(disapproved_ads)} Ads Disapproved by Google',
                'description': 'These ads are not running and need immediate attention to restore your campaigns',
                'impact': len(disapproved_ads) * 100,  # Estimated daily revenue impact per ad
                'impact_type': 'lost_opportunity',
                'recommendations': [
                    'Review policy violations for each ad',
                    'Fix policy issues and request re-review',
                    'Create compliant alternative ads',
                    'Update landing pages if needed'
                ],
                'action_items': [
                    {
                        'action': 'Review and fix disapproved ads',
                        'priority': 'immediate',
                        'estimated_time': '2-3 hours'
                    }
                ],
                'affected_ads': [
                    {
                        'name': ad.get('name'),
                        'campaign': ad.get('campaign'),
                        'violations': ad.get('violations', [])
                    }
                    for ad in disapproved_ads[:10]
                ]
            })

        return recommendations

    def _generate_checkout_recommendations(self, abandoned_checkouts: List[Dict]) -> List[Dict]:
        """Generate recommendations for abandoned checkouts"""
        recommendations = []

        recent_abandoned = [c for c in abandoned_checkouts if not c.get('recovered')]

        if len(recent_abandoned) > 20:
            total_value = sum(c.get('total_price', 0) for c in recent_abandoned)

            recommendations.append({
                'type': 'abandoned_checkout',
                'priority': 'high',
                'title': f'{len(recent_abandoned)} Abandoned Checkouts Worth {format_currency(total_value)}',
                'description': f'Significant revenue is being left on the table from abandoned carts',
                'impact': total_value * 0.3,  # 30% typical recovery rate
                'impact_type': 'recovery_opportunity',
                'recommendations': [
                    'Set up automated abandoned cart email sequence',
                    'Offer small discount (5-10%) for cart completion',
                    'Add exit-intent popup with incentive',
                    'Simplify checkout process to reduce friction',
                    'Add trust badges and security assurances',
                    'Optimize for mobile checkout'
                ],
                'action_items': [
                    {
                        'action': 'Create abandoned cart flow in Klaviyo',
                        'priority': 'high',
                        'estimated_time': '1-2 hours'
                    },
                    {
                        'action': 'Analyze checkout drop-off points in GA4',
                        'priority': 'medium',
                        'estimated_time': '30 minutes'
                    }
                ]
            })

        return recommendations

    def _generate_email_recommendations(self, email_campaigns: List[Dict]) -> List[Dict]:
        """Generate email campaign recommendations"""
        recommendations = []

        if email_campaigns:
            avg_open_rate = sum(c.get('open_rate', 0) for c in email_campaigns) / len(email_campaigns)

            low_performance = [c for c in email_campaigns if c.get('open_rate', 0) < avg_open_rate * 0.5]

            if low_performance:
                recommendations.append({
                    'type': 'email_optimization',
                    'priority': 'medium',
                    'title': f'{len(low_performance)} Email Campaigns Underperforming',
                    'description': 'Several email campaigns have unusually low open rates',
                    'impact': len(low_performance) * 500,  # Estimated missed revenue
                    'impact_type': 'engagement_opportunity',
                    'recommendations': [
                        'Test different subject lines (A/B testing)',
                        'Segment audience for more targeted messaging',
                        'Clean email list to remove inactive subscribers',
                        'Optimize send time based on engagement data',
                        'Personalize email content',
                        'Improve preview text'
                    ],
                    'action_items': [
                        {
                            'action': 'Set up A/B tests for subject lines',
                            'priority': 'medium',
                            'estimated_time': '1 hour'
                        }
                    ]
                })

        return recommendations

    def _prioritize_recommendations(self, recommendations: List[Dict]) -> List[Dict]:
        """Sort recommendations by priority and impact"""

        # Priority weights
        priority_weights = {
            'critical': 4,
            'high': 3,
            'medium': 2,
            'low': 1
        }

        # Sort by priority first, then by impact
        sorted_recs = sorted(
            recommendations,
            key=lambda x: (
                -priority_weights.get(x.get('priority', 'low'), 0),
                -abs(x.get('impact', 0))
            )
        )

        return sorted_recs

    def generate_executive_summary(self, recommendations: List[Dict]) -> Dict:
        """
        Generate executive summary of all recommendations
        """
        total_impact = sum(r.get('impact', 0) for r in recommendations)
        critical_items = len([r for r in recommendations if r.get('priority') == 'critical'])
        high_items = len([r for r in recommendations if r.get('priority') == 'high'])

        # Group by type
        by_type = {}
        for rec in recommendations:
            rec_type = rec.get('type', 'other')
            if rec_type not in by_type:
                by_type[rec_type] = []
            by_type[rec_type].append(rec)

        return {
            'total_recommendations': len(recommendations),
            'critical_actions': critical_items,
            'high_priority_actions': high_items,
            'estimated_total_impact': total_impact,
            'recommendations_by_type': {k: len(v) for k, v in by_type.items()},
            'top_recommendations': recommendations[:5],
            'generated_at': datetime.utcnow().isoformat()
        }
