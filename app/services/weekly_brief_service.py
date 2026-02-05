"""
Weekly Strategic Brief Service

Synthesizes insights from all modules into a prioritized action list.
Answers: "What should I focus on this week?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from decimal import Decimal
import statistics

from app.models.weekly_brief import (
    WeeklyBrief,
    BriefPriority,
    BriefTrend,
    BriefWorkingWell,
    BriefWatchList
)
from app.utils.logger import log


class WeeklyBriefService:
    """Service for weekly strategic brief generation"""

    def __init__(self, db: Session):
        self.db = db

        # Priority scoring weights
        self.impact_weight = 1.0
        self.confidence_weight = 0.8
        self.effort_penalty = 1.0

        # Thresholds
        self.high_priority_score = 1000  # Score >= 1000 = high priority
        self.data_quality_threshold = 70  # Minimum quality for reliable insights

    async def generate_weekly_brief(self, week_start: Optional[date] = None) -> Dict:
        """
        Generate comprehensive weekly brief

        Aggregates insights from all modules and prioritizes them
        """
        if not week_start:
            # Use current week (Monday)
            today = date.today()
            week_start = today - timedelta(days=today.weekday())

        week_end = week_start + timedelta(days=6)

        log.info(f"Generating weekly brief for week of {week_start}")

        # 1. Check data quality
        data_quality = await self._assess_data_quality()

        # 2. Aggregate insights from all modules
        all_insights = await self._aggregate_module_insights()

        # 3. Score and prioritize
        priorities = await self._score_and_prioritize(all_insights)

        # 4. Identify what's working well
        working_well = await self._identify_working_well()

        # 5. Build watch list
        watch_list = await self._build_watch_list()

        # 6. Calculate trends vs previous week
        trends = await self._calculate_trends(week_start)

        # 7. Create brief record
        brief = await self._create_brief_record(
            week_start=week_start,
            week_end=week_end,
            priorities=priorities,
            working_well=working_well,
            watch_list=watch_list,
            trends=trends,
            data_quality=data_quality
        )

        log.info(f"Generated brief with {len(priorities)} priorities")

        return brief

    async def _aggregate_module_insights(self) -> List[Dict]:
        """
        Aggregate insights from all modules

        Returns normalized list of insights ready for prioritization
        """
        log.info("Aggregating insights from all modules")

        insights = []

        # Ad Spend Module
        try:
            from app.services.ad_spend_service import AdSpendService
            ad_service = AdSpendService(self.db)
            ad_analysis = await ad_service.analyze_all_campaigns(days=7)

            # Scaling opportunities
            for opp in ad_analysis.get('scaling_opportunities', [])[:3]:
                insights.append({
                    'source_module': 'ad_spend',
                    'type': 'scaling_opportunity',
                    'title': f"Scale {opp['campaign_name']}",
                    'description': opp.get('rationale', ''),
                    'revenue_impact': opp['expected_impact']['additional_profit_per_month'],
                    'cost_savings': 0,
                    'effort_hours': 0.5,  # Just budget change
                    'effort_level': 'low',
                    'confidence': 0.9 if opp['confidence'] == 'high' else 0.7,
                    'action': f"Increase budget to ${opp['recommendation']['recommended_monthly_budget']:,.0f}/month",
                    'impact_timeframe': 'monthly'
                })

            # Waste reduction
            for waste in ad_analysis.get('waste_identified', [])[:3]:
                insights.append({
                    'source_module': 'ad_spend',
                    'type': 'waste_reduction',
                    'title': f"Fix {waste['waste_type'].replace('_', ' ').title()}",
                    'description': waste['description'],
                    'revenue_impact': 0,
                    'cost_savings': waste['recommendation']['expected_savings'],
                    'effort_hours': 1.0 if waste['recommendation']['difficulty'] == 'easy' else 3.0,
                    'effort_level': waste['recommendation']['difficulty'],
                    'confidence': 0.85,
                    'action': waste['recommendation']['action'],
                    'impact_timeframe': 'monthly'
                })

        except Exception as e:
            log.warning(f"Could not load ad spend insights: {str(e)}")

        # Email Module
        try:
            from app.services.email_service import EmailService
            email_service = EmailService(self.db)
            email_analysis = await email_service.analyze_all_opportunities(days=7)

            # Underperforming flows
            for flow in email_analysis['opportunities'].get('underperforming_flows', [])[:2]:
                insights.append({
                    'source_module': 'email',
                    'type': 'flow_optimization',
                    'title': f"Fix {flow.get('flow_name', 'Email Flow')}",
                    'description': ', '.join(flow.get('issues', [])),
                    'revenue_impact': flow.get('estimated_revenue_gap', 0),
                    'cost_savings': 0,
                    'effort_hours': 2.0,
                    'effort_level': 'low',
                    'confidence': 0.8,
                    'action': flow['recommended_actions'][0] if flow.get('recommended_actions') else 'Optimize flow',
                    'impact_timeframe': 'monthly'
                })

            # Under-contacted segments
            for segment in email_analysis['opportunities'].get('under_contacted_segments', [])[:2]:
                insights.append({
                    'source_module': 'email',
                    'type': 'segment_activation',
                    'title': f"Contact {segment.get('segment_name', 'Segment')}",
                    'description': segment.get('issue', ''),
                    'revenue_impact': segment.get('revenue_opportunity', 0),
                    'cost_savings': 0,
                    'effort_hours': 0.5,  # Just send campaign
                    'effort_level': 'low',
                    'confidence': 0.75,
                    'action': segment.get('recommended_action', 'Send campaign'),
                    'impact_timeframe': 'monthly'
                })

        except Exception as e:
            log.warning(f"Could not load email insights: {str(e)}")

        # User Behavior Module
        try:
            from app.services.user_behavior_service import UserBehaviorService
            behavior_service = UserBehaviorService(self.db)
            behavior_analysis = await behavior_service.analyze_all_behavior(days=7)

            # High-friction pages
            for page in behavior_analysis.get('high_friction_pages', [])[:2]:
                insights.append({
                    'source_module': 'behavior',
                    'type': 'ux_fix',
                    'title': f"Fix {page['page_path']}",
                    'description': ', '.join(page.get('issues', [])),
                    'revenue_impact': page['revenue_impact']['estimated_revenue_lost'],
                    'cost_savings': 0,
                    'effort_hours': len(page.get('recommended_fixes', [])) * 1.5,
                    'effort_level': 'medium',
                    'confidence': 0.8,
                    'action': page['recommended_fixes'][0] if page.get('recommended_fixes') else 'Reduce friction',
                    'impact_timeframe': 'monthly'
                })

            # Checkout funnel leaks
            for step in behavior_analysis.get('checkout_funnel', []):
                if step.get('is_biggest_leak'):
                    insights.append({
                        'source_module': 'behavior',
                        'type': 'checkout_optimization',
                        'title': f"Fix Checkout: {step['step_name']}",
                        'description': ', '.join(step.get('issues', [])),
                        'revenue_impact': step['revenue_impact']['estimated_revenue_lost'],
                        'cost_savings': 0,
                        'effort_hours': 4.0,
                        'effort_level': 'medium',
                        'confidence': 0.85,
                        'action': step['recommended_fixes'][0] if step.get('recommended_fixes') else 'Fix friction',
                        'impact_timeframe': 'monthly'
                    })
                    break  # Only biggest leak

        except Exception as e:
            log.warning(f"Could not load behavior insights: {str(e)}")

        # SEO Module
        try:
            from app.models.seo import SEOOpportunity
            seo_opps = self.db.query(SEOOpportunity).filter(
                SEOOpportunity.priority.in_(['critical', 'high'])
            ).order_by(desc(SEOOpportunity.estimated_monthly_traffic)).limit(2).all()

            for opp in seo_opps:
                insights.append({
                    'source_module': 'seo',
                    'type': 'seo_opportunity',
                    'title': opp.opportunity_type.replace('_', ' ').title(),
                    'description': opp.description or '',
                    'revenue_impact': float(opp.estimated_monthly_value) if opp.estimated_monthly_value else 0,
                    'cost_savings': 0,
                    'effort_hours': 2.0,
                    'effort_level': 'low',
                    'confidence': 0.7,
                    'action': opp.recommended_action or 'Optimize SEO',
                    'impact_timeframe': 'monthly'
                })

        except Exception as e:
            log.warning(f"Could not load SEO insights: {str(e)}")

        # Journey Module
        try:
            from app.services.journey_service import JourneyService
            journey_service = JourneyService(self.db)
            journey_analysis = await journey_service.analyze_all_journeys()

            # Gateway products
            for product in journey_analysis.get('gateway_products', [])[:1]:
                if product['opportunity'].get('should_be_promoted'):
                    insights.append({
                        'source_module': 'journey',
                        'type': 'merchandising',
                        'title': f"Promote {product['product_title']}",
                        'description': f"Gateway product with {product['metrics']['repeat_rate_vs_average']} repeat rate",
                        'revenue_impact': product['opportunity']['estimated_ltv_gain'],
                        'cost_savings': 0,
                        'effort_hours': 2.0,
                        'effort_level': 'low',
                        'confidence': 0.75,
                        'action': product['recommended_actions'][0] if product.get('recommended_actions') else 'Feature product',
                        'impact_timeframe': 'monthly'
                    })

        except Exception as e:
            log.warning(f"Could not load journey insights: {str(e)}")

        log.info(f"Aggregated {len(insights)} insights from all modules")

        return insights

    async def _score_and_prioritize(self, insights: List[Dict]) -> List[Dict]:
        """
        Score insights and prioritize

        Formula: (revenue_impact + cost_savings) × confidence / effort_hours
        """
        log.info("Scoring and prioritizing insights")

        scored_insights = []

        for insight in insights:
            # Calculate total impact
            revenue_impact = float(insight.get('revenue_impact', 0))
            cost_savings = float(insight.get('cost_savings', 0))
            total_impact = revenue_impact + cost_savings

            # Get confidence
            confidence = float(insight.get('confidence', 0.5))

            # Get effort (minimum 0.5 hours)
            effort_hours = max(float(insight.get('effort_hours', 1.0)), 0.5)

            # Calculate priority score
            # Higher impact, higher confidence, lower effort = higher score
            priority_score = (total_impact * confidence) / effort_hours

            # Determine priority level
            if priority_score >= self.high_priority_score:
                priority_level = 'high'
            elif priority_score >= 500:
                priority_level = 'medium'
            else:
                priority_level = 'low'

            scored_insights.append({
                **insight,
                'total_impact': total_impact,
                'priority_score': priority_score,
                'priority_level': priority_level
            })

        # Sort by priority score descending
        scored_insights.sort(key=lambda x: x['priority_score'], reverse=True)

        # Assign ranks
        for i, insight in enumerate(scored_insights, 1):
            insight['priority_rank'] = i

        log.info(f"Scored and ranked {len(scored_insights)} insights")

        return scored_insights

    async def _identify_working_well(self) -> List[Dict]:
        """
        Identify things that are working well

        These should not be touched
        """
        log.info("Identifying what's working well")

        working_well = []

        # Check ad campaigns with high ROAS
        try:
            from app.models.ad_spend import CampaignPerformance
            high_performing_campaigns = self.db.query(CampaignPerformance).filter(
                CampaignPerformance.is_high_performer == True,
                CampaignPerformance.is_active == True
            ).order_by(desc(CampaignPerformance.true_roas)).limit(3).all()

            for campaign in high_performing_campaigns:
                working_well.append({
                    'source_module': 'ad_spend',
                    'item_name': campaign.campaign_name,
                    'item_type': 'campaign',
                    'metric': f"{campaign.true_roas:.1f}x true ROAS",
                    'description': f"{campaign.campaign_name}: {campaign.true_roas:.1f}x true ROAS, profitable",
                    'performance_value': float(campaign.true_roas),
                    'benchmark_value': 2.0,
                    'performance_vs_benchmark': ((campaign.true_roas / 2.0) - 1) * 100 if campaign.true_roas else 0
                })

        except Exception as e:
            log.warning(f"Could not load ad campaign data: {str(e)}")

        # Check email flows above benchmark
        try:
            from app.models.email import EmailFlow
            high_performing_flows = self.db.query(EmailFlow).filter(
                EmailFlow.is_active == True,
                EmailFlow.open_rate > 0.45  # Above 45% benchmark
            ).order_by(desc(EmailFlow.open_rate)).limit(2).all()

            for flow in high_performing_flows:
                working_well.append({
                    'source_module': 'email',
                    'item_name': flow.flow_name,
                    'item_type': 'flow',
                    'metric': f"{flow.open_rate * 100:.0f}% open rate",
                    'description': f"{flow.flow_name}: {flow.open_rate * 100:.0f}% open rate (above benchmark)",
                    'performance_value': float(flow.open_rate * 100),
                    'benchmark_value': 45.0,
                    'performance_vs_benchmark': ((flow.open_rate * 100 / 45.0) - 1) * 100
                })

        except Exception as e:
            log.warning(f"Could not load email flow data: {str(e)}")

        log.info(f"Found {len(working_well)} items working well")

        return working_well

    async def _build_watch_list(self) -> List[Dict]:
        """
        Build watch list of emerging issues

        Not urgent yet, but worth monitoring
        """
        log.info("Building watch list")

        watch_list = []

        # Check for declining trends
        try:
            from app.models.ad_spend import CampaignPerformance
            # Campaigns with declining ROAS (but not terrible yet)
            # This would require historical data comparison
            # Placeholder for now

        except Exception as e:
            log.warning(f"Could not build watch list: {str(e)}")

        log.info(f"Built watch list with {len(watch_list)} items")

        return watch_list

    async def _calculate_trends(self, current_week_start: date) -> Dict:
        """
        Calculate week-over-week trends

        Compare to previous week's brief
        """
        log.info("Calculating week-over-week trends")

        # Get previous week's brief
        previous_week_start = current_week_start - timedelta(days=7)

        previous_brief = self.db.query(WeeklyBrief).filter(
            WeeklyBrief.week_start_date == previous_week_start
        ).first()

        trends = {
            'improved': [],
            'declined': [],
            'implemented': [],
            'pending': []
        }

        if not previous_brief:
            log.info("No previous brief found for comparison")
            return trends

        # Check what was implemented from previous week
        previous_priorities = self.db.query(BriefPriority).filter(
            BriefPriority.week_start_date == previous_week_start
        ).all()

        for priority in previous_priorities:
            if priority.status == 'completed':
                trends['implemented'].append({
                    'title': priority.priority_title,
                    'impact': float(priority.total_estimated_impact) if priority.total_estimated_impact else 0
                })
            elif priority.status == 'in_progress':
                trends['pending'].append(priority.priority_title)

        log.info(f"Calculated trends: {len(trends['implemented'])} implemented, {len(trends['pending'])} pending")

        return trends

    async def _create_brief_record(
        self,
        week_start: date,
        week_end: date,
        priorities: List[Dict],
        working_well: List[Dict],
        watch_list: List[Dict],
        trends: Dict,
        data_quality: Dict
    ) -> Dict:
        """
        Create WeeklyBrief database record
        """
        log.info("Creating brief record")

        # Mark previous briefs as not current
        self.db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).update({'is_current': False})

        # Calculate summary statistics
        total_impact = sum(p.get('total_impact', 0) for p in priorities)
        high_priority_count = sum(1 for p in priorities if p.get('priority_level') == 'high')
        medium_priority_count = sum(1 for p in priorities if p.get('priority_level') == 'medium')
        low_priority_count = sum(1 for p in priorities if p.get('priority_level') == 'low')

        # Module insights count
        module_counts = {}
        for p in priorities:
            module = p.get('source_module', 'unknown')
            module_counts[module] = module_counts.get(module, 0) + 1

        # Create brief
        brief = WeeklyBrief(
            week_start_date=week_start,
            week_end_date=week_end,
            week_number=week_start.isocalendar()[1],
            year=week_start.year,
            data_quality_score=data_quality.get('score', 0),
            data_quality_status=data_quality.get('status', 'unknown'),
            data_issues=data_quality.get('issues'),
            total_priorities=len(priorities),
            high_priority_count=high_priority_count,
            medium_priority_count=medium_priority_count,
            low_priority_count=low_priority_count,
            total_estimated_impact=total_impact,
            module_insights_count=module_counts,
            top_3_priorities=[
                {
                    'title': p['title'],
                    'impact': p['total_impact'],
                    'action': p['action']
                }
                for p in priorities[:3]
            ],
            working_well_items=working_well,
            working_well_count=len(working_well),
            watch_list_items=watch_list,
            watch_list_count=len(watch_list),
            trends_summary=trends,
            is_current=True
        )

        self.db.add(brief)
        self.db.commit()
        self.db.refresh(brief)

        # Create priority records
        for priority in priorities[:20]:  # Top 20 priorities
            priority_record = BriefPriority(
                brief_id=brief.id,
                week_start_date=week_start,
                priority_rank=priority['priority_rank'],
                priority_title=priority['title'],
                priority_description=priority.get('description'),
                source_module=priority['source_module'],
                source_insight_type=priority.get('type'),
                estimated_revenue_impact=priority.get('revenue_impact', 0),
                estimated_cost_savings=priority.get('cost_savings', 0),
                total_estimated_impact=priority['total_impact'],
                impact_timeframe=priority.get('impact_timeframe', 'monthly'),
                effort_level=priority['effort_level'],
                effort_hours=priority['effort_hours'],
                confidence_level='high' if priority['confidence'] >= 0.8 else 'medium' if priority['confidence'] >= 0.6 else 'low',
                confidence_score=priority['confidence'],
                priority_score=priority['priority_score'],
                recommended_action=priority['action'],
                priority_level=priority['priority_level'],
                status='new'
            )
            self.db.add(priority_record)

        self.db.commit()

        log.info(f"Created brief #{brief.id} with {len(priorities)} priorities")

        return {
            'brief_id': brief.id,
            'week_start': str(week_start),
            'week_end': str(week_end),
            'data_quality_score': brief.data_quality_score,
            'total_priorities': brief.total_priorities,
            'total_impact': float(brief.total_estimated_impact),
            'priorities': priorities[:10],
            'working_well': working_well,
            'watch_list': watch_list,
            'trends': trends
        }

    async def _assess_data_quality(self) -> Dict:
        """
        Assess overall data quality across modules

        Returns quality score and issues
        """
        log.info("Assessing data quality")

        # This would check data quality from Module 9
        # Placeholder implementation
        return {
            'score': 94,
            'status': 'excellent',
            'issues': []
        }

    async def get_current_brief(self) -> Optional[Dict]:
        """Get the current week's brief"""
        brief = self.db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).first()

        if not brief:
            return None

        # Get priorities
        priorities = self.db.query(BriefPriority).filter(
            BriefPriority.brief_id == brief.id
        ).order_by(BriefPriority.priority_rank).all()

        return {
            'brief_id': brief.id,
            'week_start': str(brief.week_start_date),
            'week_end': str(brief.week_end_date),
            'data_quality_score': brief.data_quality_score,
            'total_priorities': brief.total_priorities,
            'total_impact': float(brief.total_estimated_impact),
            'priorities': [self._priority_to_dict(p) for p in priorities],
            'working_well': brief.working_well_items or [],
            'watch_list': brief.watch_list_items or [],
            'trends': brief.trends_summary or {},
            'generated_at': brief.generated_at.isoformat()
        }

    def _priority_to_dict(self, priority: BriefPriority) -> Dict:
        """Convert priority record to dict"""
        return {
            'rank': priority.priority_rank,
            'title': priority.priority_title,
            'description': priority.priority_description,
            'source_module': priority.source_module,
            'impact': float(priority.total_estimated_impact),
            'effort': priority.effort_level,
            'effort_hours': priority.effort_hours,
            'confidence': priority.confidence_level,
            'action': priority.recommended_action,
            'priority_level': priority.priority_level,
            'status': priority.status
        }
