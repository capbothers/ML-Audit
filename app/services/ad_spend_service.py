"""
Ad Spend Optimization Intelligence Service

Analyzes Google Ads performance with true ROAS calculations.
Answers: "Where am I wasting ad spend? Where should I scale?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from decimal import Decimal
import statistics
import math

from app.models.ad_spend import (
    CampaignPerformance,
    AdSpendOptimization,
    AdWaste,
    ProductAdPerformance
)
from app.models.google_ads_data import GoogleAdsCampaign
from app.services.finance_service import FinanceService
from app.services.campaign_strategy import format_why_now, STRATEGY_THRESHOLDS
from app.utils.logger import log


class AdSpendService:
    """Service for ad spend optimization intelligence"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds
        self.profitable_roas_threshold = 2.0  # ROAS >= 2.0 = profitable
        self.high_performer_threshold = 3.0  # ROAS >= 3.0 = high performer
        self.scaling_opportunity_threshold = 3.5  # High ROAS + budget capped
        self.waste_threshold = 1.5  # ROAS < 1.5 = wasting money

        # Product profitability
        self.min_product_margin = 0.30  # 30% minimum margin after ads
        self.min_profit_roas = 1.5  # Minimum profit ROAS

    def _get_ads_data_end_date(self) -> date:
        """Use latest Google Ads row date as analysis boundary to avoid trailing empty days."""
        max_date = self.db.query(func.max(GoogleAdsCampaign.date)).scalar()
        return max_date or datetime.utcnow().date()

    def _get_campaigns_for_period(self, days: int) -> List[CampaignPerformance]:
        """
        Return active campaign snapshots for the requested period, with fallback
        to latest available period to avoid empty responses.
        """
        campaigns = self.db.query(CampaignPerformance).filter(
            CampaignPerformance.is_active == True,
            CampaignPerformance.period_days == days
        ).all()
        if campaigns:
            return campaigns

        latest_period = self.db.query(func.max(CampaignPerformance.period_days)).filter(
            CampaignPerformance.is_active == True
        ).scalar()
        if not latest_period:
            return []

        return self.db.query(CampaignPerformance).filter(
            CampaignPerformance.is_active == True,
            CampaignPerformance.period_days == latest_period
        ).all()

    async def analyze_all_campaigns(self, days: int = 30) -> Dict:
        """
        Complete ad spend analysis

        Returns comprehensive insights on waste, scaling, reallocation
        """
        log.info(f"Analyzing ad spend for last {days} days")

        results = {
            'campaigns': [],
            'scaling_opportunities': [],
            'waste_identified': [],
            'budget_reallocations': [],
            'product_performance': [],
            'summary': {}
        }

        # 1. Get all campaigns with performance
        campaigns = await self.get_campaign_performance(days)
        results['campaigns'] = campaigns

        # 2. Identify scaling opportunities
        scaling_opps = await self.find_scaling_opportunities(days)
        results['scaling_opportunities'] = scaling_opps

        # 3. Detect waste
        waste = await self.detect_ad_waste(days)
        results['waste_identified'] = waste

        # 4. Generate budget reallocation recommendations
        reallocations = await self.calculate_budget_reallocations(days)
        results['budget_reallocations'] = reallocations

        # 5. Product-level performance
        product_perf = await self.get_product_ad_performance(days)
        results['product_performance'] = product_perf

        # Summary
        total_spend = sum(float(c.get('spend', 0)) for c in campaigns)
        total_waste = sum(float(w.get('monthly_waste', 0)) for w in waste)
        total_opportunity = sum(float(s.get('expected_profit_increase', 0)) for s in scaling_opps)

        results['summary'] = {
            'total_campaigns': len(campaigns),
            'total_spend': total_spend,
            'total_waste_identified': total_waste,
            'scaling_opportunities_count': len(scaling_opps),
            'total_scaling_opportunity': total_opportunity,
            'budget_reallocations_count': len(reallocations),
            'period_days': days
        }

        log.info(f"Ad spend analysis complete: ${total_waste:,.0f} waste, ${total_opportunity:,.0f} opportunity")

        return results

    async def get_campaign_performance(self, days: int = 30) -> List[Dict]:
        """
        Get all campaigns with true ROAS vs Google ROAS

        Returns campaigns sorted by true profit
        """
        log.info("Getting campaign performance")

        # Try exact period match first, then fall back to any active campaigns
        campaigns = self._get_campaigns_for_period(days)
        campaigns = sorted(campaigns, key=lambda c: c.true_profit or 0, reverse=True)

        results = []

        for campaign in campaigns:
            result = {
                'campaign_id': campaign.campaign_id,
                'campaign_name': campaign.campaign_name,
                'campaign_type': campaign.campaign_type,

                'spend': float(campaign.total_spend),
                'daily_budget': float(campaign.daily_budget) if campaign.daily_budget else None,

                'google_metrics': {
                    'conversions': campaign.google_conversions,
                    'conversion_value': float(campaign.google_conversion_value),
                    'roas': round(campaign.google_roas, 1) if campaign.google_roas else None
                },

                'true_metrics': {
                    'conversions': campaign.actual_conversions,
                    'revenue': float(campaign.actual_revenue),
                    'product_costs': float(campaign.actual_product_costs),
                    'profit': float(campaign.true_profit),
                    'true_roas': round(campaign.true_roas, 1) if campaign.true_roas else None,
                    'revenue_roas': round(campaign.revenue_roas, 1) if campaign.revenue_roas else None,
                    'allocated_overhead': float(campaign.allocated_overhead) if campaign.allocated_overhead else None,
                    'fully_loaded_profit': float(campaign.fully_loaded_profit) if campaign.fully_loaded_profit else None,
                    'fully_loaded_roas': round(campaign.fully_loaded_roas, 1) if campaign.fully_loaded_roas else None,
                    'is_profitable_fully_loaded': campaign.is_profitable_fully_loaded,
                },

                'roas_difference': {
                    'google_roas': round(campaign.google_roas, 1) if campaign.google_roas else None,
                    'true_roas': round(campaign.true_roas, 1) if campaign.true_roas else None,
                    'inflated_by': round(campaign.google_roas - campaign.true_roas, 1) if campaign.google_roas and campaign.true_roas else None
                },

                'performance': {
                    'clicks': campaign.total_clicks,
                    'avg_cpc': float(campaign.avg_cpc) if campaign.avg_cpc else None,
                    'ctr': round(campaign.click_through_rate * 100, 2) if campaign.click_through_rate else None
                },

                'budget_status': {
                    'status': campaign.budget_status,
                    'is_capped': campaign.budget_capped,
                    'cap_time': campaign.avg_cap_time,
                    'lost_impression_share': round(campaign.lost_impression_share * 100, 1) if campaign.lost_impression_share else None
                },

                'indicators': {
                    'is_profitable': campaign.is_profitable,
                    'is_high_performer': campaign.is_high_performer,
                    'is_scaling_opportunity': campaign.is_scaling_opportunity,
                    'is_wasting_budget': campaign.is_wasting_budget
                },

                'recommendation': {
                    'action': campaign.recommended_action,
                    'recommended_budget': float(campaign.recommended_budget) if campaign.recommended_budget else None,
                    'expected_impact': float(campaign.expected_impact) if campaign.expected_impact else None
                },

                'strategy': {
                    'type': campaign.strategy_type,
                    'decision_score': campaign.decision_score,
                    'short_term_status': campaign.short_term_status,
                    'strategic_value': campaign.strategic_value,
                    'action': campaign.strategy_action,
                    'confidence': campaign.strategy_confidence,
                    'why_now': format_why_now(
                        campaign.strategy_action,
                        campaign.true_roas,
                        campaign.strategy_type,
                        STRATEGY_THRESHOLDS.get(campaign.strategy_type or 'unknown'),
                        campaign.decision_score,
                    ) if campaign.strategy_type else None,
                } if campaign.strategy_type else None,
            }

            results.append(result)

        log.info(f"Found {len(results)} active campaigns")

        return results

    async def find_scaling_opportunities(self, days: int = 30) -> List[Dict]:
        """
        Find campaigns with high ROAS that are budget-capped

        These should get more budget
        """
        log.info("Finding scaling opportunities")

        scaling_campaigns = self.db.query(CampaignPerformance).filter(
            CampaignPerformance.is_scaling_opportunity == True,
            CampaignPerformance.period_days == days
        ).order_by(
            desc(CampaignPerformance.true_roas)
        ).all()

        results = []

        for campaign in scaling_campaigns:
            # Calculate potential impact of budget increase
            current_budget = float(campaign.daily_budget) if campaign.daily_budget else 0
            current_monthly = current_budget * 30

            # Recommend 50-100% increase for high performers
            if campaign.true_roas and campaign.true_roas >= self.scaling_opportunity_threshold:
                recommended_increase = 1.5  # 50% increase
            else:
                recommended_increase = 1.25  # 25% increase

            recommended_budget = current_monthly * recommended_increase
            budget_increase = recommended_budget - current_monthly

            # Estimate profit increase (conservative)
            expected_profit_increase = budget_increase * (campaign.true_roas if campaign.true_roas else 0)

            result = {
                'campaign_name': campaign.campaign_name,
                'campaign_type': campaign.campaign_type,

                'current_performance': {
                    'monthly_spend': current_monthly,
                    'daily_budget': current_budget,
                    'true_roas': round(campaign.true_roas, 1) if campaign.true_roas else None,
                    'monthly_profit': float(campaign.true_profit) * (30 / days) if campaign.true_profit else 0
                },

                'budget_constraint': {
                    'is_capped': campaign.budget_capped,
                    'caps_at': campaign.avg_cap_time,
                    'lost_impression_share': round(campaign.lost_impression_share * 100, 1) if campaign.lost_impression_share else None
                },

                'recommendation': {
                    'recommended_monthly_budget': round(recommended_budget, 0),
                    'budget_increase': round(budget_increase, 0),
                    'increase_percentage': round((recommended_increase - 1) * 100, 0)
                },

                'expected_impact': {
                    'additional_profit_per_month': round(expected_profit_increase, 0),
                    'new_monthly_profit': round(float(campaign.true_profit) * (30 / days) + expected_profit_increase, 0)
                },

                'confidence': 'high' if campaign.true_roas and campaign.true_roas >= 4.0 else 'medium',
                'priority': 'high' if expected_profit_increase > 2000 else 'medium',

                'rationale': self._generate_scaling_rationale(campaign, expected_profit_increase)
            }

            results.append(result)

        log.info(f"Found {len(results)} scaling opportunities")

        return results

    async def detect_ad_waste(self, days: int = 30) -> List[Dict]:
        """
        Detect where ad spend is being wasted

        Brand cannibalization, below-margin products, etc.
        """
        log.info("Detecting ad waste")

        waste_instances = self.db.query(AdWaste).filter(
            AdWaste.status == 'active',
            AdWaste.period_days == days
        ).order_by(
            desc(AdWaste.monthly_waste_spend)
        ).all()

        results = []

        for waste in waste_instances:
            result = {
                'waste_type': waste.waste_type,
                'description': waste.waste_description,

                'affected': {
                    'campaign_name': waste.campaign_name,
                    'product_title': waste.product_title,
                    'keyword': waste.keyword
                },

                'waste_metrics': {
                    'monthly_waste': float(waste.monthly_waste_spend),
                    'severity': waste.severity
                },

                'evidence': waste.evidence or {},

                'recommendation': {
                    'action': waste.recommended_action,
                    'expected_savings': float(waste.expected_savings),
                    'difficulty': waste.implementation_difficulty
                },

                'priority': self._calculate_waste_priority(
                    float(waste.monthly_waste_spend),
                    waste.severity,
                    waste.implementation_difficulty
                )
            }

            # Add type-specific details
            if waste.waste_type == 'brand_cannibalization':
                result['organic_metrics'] = {
                    'organic_conversion_rate': round(waste.organic_conversion_rate * 100, 1) if waste.organic_conversion_rate else None,
                    'estimated_organic_conversions': waste.evidence.get('estimated_organic_conversions') if waste.evidence else None
                }

            elif waste.waste_type == 'below_margin_products':
                result['product_metrics'] = {
                    'actual_margin': round(waste.product_margin * 100, 1) if waste.product_margin else None,
                    'required_margin': round(waste.margin_threshold * 100, 1) if waste.margin_threshold else None,
                    'cost_per_acquisition': float(waste.cost_per_acquisition) if waste.cost_per_acquisition else None
                }

            results.append(result)

        log.info(f"Found {len(results)} waste instances")

        return results

    async def calculate_budget_reallocations(self, days: int = 30) -> List[Dict]:
        """
        Generate budget reallocation recommendations

        Move budget from low ROAS to high ROAS campaigns
        """
        log.info("Calculating budget reallocations")

        optimizations = self.db.query(AdSpendOptimization).filter(
            AdSpendOptimization.status == 'recommended'
        ).order_by(
            desc(AdSpendOptimization.profit_impact)
        ).limit(5).all()

        results = []

        for opt in optimizations:
            result = {
                'optimization_name': opt.optimization_name,
                'type': opt.optimization_type,

                'from_campaign': {
                    'name': opt.source_campaign_name,
                    'current_budget': float(opt.current_source_budget) if opt.current_source_budget else None,
                    'recommended_budget': float(opt.recommended_source_budget) if opt.recommended_source_budget else None,
                    'budget_reduction': float(opt.budget_to_move) if opt.budget_to_move else None
                },

                'to_campaign': {
                    'name': opt.target_campaign_name,
                    'current_budget': float(opt.current_target_budget) if opt.current_target_budget else None,
                    'recommended_budget': float(opt.recommended_target_budget) if opt.recommended_target_budget else None,
                    'budget_increase': float(opt.budget_to_add) if opt.budget_to_add else None
                },

                'current_performance': {
                    'total_spend': float(opt.current_total_spend),
                    'total_revenue': float(opt.current_total_revenue),
                    'total_profit': float(opt.current_total_profit)
                },

                'projected_performance': {
                    'total_spend': float(opt.projected_total_spend),
                    'total_revenue': float(opt.projected_total_revenue),
                    'total_profit': float(opt.projected_total_profit)
                },

                'expected_impact': {
                    'additional_revenue': float(opt.revenue_impact),
                    'additional_profit': float(opt.profit_impact),
                    'spend_change': float(opt.spend_change)
                },

                'confidence': opt.confidence_level,
                'priority': opt.priority,
                'rationale': opt.rationale
            }

            results.append(result)

        log.info(f"Generated {len(results)} budget reallocation recommendations")

        return results

    async def get_product_ad_performance(self, days: int = 30, limit: int = 20) -> List[Dict]:
        """
        Get product-level ad performance

        Which products are profitable to advertise?
        """
        log.info("Getting product ad performance")

        # Get both profitable and unprofitable products
        # Spend floor ($200) + conversions floor (3) prevents low-confidence outliers
        # Sort by spend * ROAS to prioritize actionable scale candidates
        profitable_products = self.db.query(ProductAdPerformance).filter(
            ProductAdPerformance.is_profitable_to_advertise == True,
            ProductAdPerformance.period_days == days,
            ProductAdPerformance.total_ad_spend >= 200,
            ProductAdPerformance.ad_conversions >= 3,
        ).order_by(
            desc(ProductAdPerformance.total_ad_spend * ProductAdPerformance.profit_roas)
        ).limit(limit // 2).all()

        unprofitable_products = self.db.query(ProductAdPerformance).filter(
            ProductAdPerformance.is_losing_money == True,
            ProductAdPerformance.period_days == days
        ).order_by(
            ProductAdPerformance.net_profit  # Ascending (most negative first)
        ).limit(limit // 2).all()

        all_products = list(profitable_products) + list(unprofitable_products)

        results = []

        for product in all_products:
            result = {
                'product_title': product.product_title,
                'product_sku': product.product_sku,

                'ad_spend': {
                    'total_spend': float(product.total_ad_spend),
                    'campaigns': product.total_campaigns,
                    'avg_cpc': float(product.avg_cpc) if product.avg_cpc else None
                },

                'performance': {
                    'clicks': product.ad_clicks,
                    'conversions': product.ad_conversions,
                    'conversion_rate': round(product.ad_conversion_rate * 100, 1) if product.ad_conversion_rate else None,
                    'units_sold': product.ad_units_sold
                },

                'revenue': {
                    'ad_revenue': float(product.ad_revenue),
                    'product_cost_per_unit': float(product.product_cost) if product.product_cost else None,
                    'total_product_costs': float(product.total_product_costs)
                },

                'profitability': {
                    'gross_profit': float(product.gross_profit),
                    'net_profit': float(product.net_profit),
                    'profit_margin': round(product.profit_margin * 100, 1) if product.profit_margin else None
                },

                'roas': {
                    'revenue_roas': round(product.revenue_roas, 1) if product.revenue_roas else None,
                    'profit_roas': round(product.profit_roas, 1) if product.profit_roas else None
                },

                'indicators': {
                    'is_profitable': product.is_profitable_to_advertise,
                    'is_high_performer': product.is_high_performer,
                    'is_losing_money': product.is_losing_money,
                    'meets_margin_threshold': product.margin_threshold_met
                },

                'sample_quality': {
                    'conversions': product.ad_conversions,
                    'is_low_sample': (product.ad_conversions or 0) < 5,
                },

                'recommendation': {
                    'action': product.recommended_action,
                    'max_cpc': float(product.recommended_max_cpc) if product.recommended_max_cpc else None
                }
            }

            results.append(result)

        # DB query already sorts by spend * ROAS (actionability); don't re-sort by ROAS alone

        log.info(f"Found {len(results)} products with ad performance data")

        return results

    async def get_ad_dashboard(self, days: int = 30) -> Dict:
        """
        Complete ad spend dashboard

        Everything you need to know about ad spend optimization
        """
        log.info("Generating ad spend dashboard")

        # Get all analyses
        analysis = await self.analyze_all_campaigns(days)

        # Top priorities
        top_priorities = self._identify_top_priorities(analysis)

        # Quick wins
        quick_wins = self._identify_quick_wins(analysis)

        return {
            'generated_at': datetime.utcnow().isoformat(),

            'summary': analysis['summary'],

            'top_priorities': top_priorities,

            'campaigns': {
                'all': analysis['campaigns'][:10],
                'top_performers': [c for c in analysis['campaigns'] if c['indicators']['is_high_performer']][:5],
                'underperformers': [c for c in analysis['campaigns'] if c['indicators']['is_wasting_budget']][:5]
            },

            'scaling_opportunities': analysis['scaling_opportunities'][:3],
            'waste_identified': analysis['waste_identified'][:5],
            'budget_reallocations': analysis['budget_reallocations'][:3],

            'product_performance': {
                'most_profitable': [p for p in analysis['product_performance'] if p['indicators']['is_profitable']][:5],
                'losing_money': [p for p in analysis['product_performance'] if p['indicators']['is_losing_money']][:5]
            },

            'quick_wins': quick_wins,

            'period_days': days
        }

    def _generate_scaling_rationale(self, campaign: CampaignPerformance, expected_impact: float) -> str:
        """Generate rationale for scaling recommendation"""
        rationale_parts = []

        if campaign.true_roas and campaign.true_roas >= 4.0:
            rationale_parts.append(f"Strong ROAS ({campaign.true_roas:.1f}x)")

        if campaign.budget_capped:
            rationale_parts.append(f"Budget-capped (runs out at {campaign.avg_cap_time})")

        if campaign.lost_impression_share and campaign.lost_impression_share > 0.2:
            rationale_parts.append(f"Losing {campaign.lost_impression_share*100:.0f}% impression share")

        if expected_impact > 2000:
            rationale_parts.append(f"High profit opportunity (+${expected_impact:,.0f}/month)")

        return ", ".join(rationale_parts) if rationale_parts else "Scaling recommended"

    def _calculate_waste_priority(self, monthly_waste: float, severity: str, difficulty: str) -> str:
        """Calculate priority for waste fix"""
        if monthly_waste > 1000 and difficulty == 'easy':
            return 'critical'
        elif monthly_waste > 500 and severity in ['critical', 'high']:
            return 'high'
        elif monthly_waste > 200:
            return 'medium'
        else:
            return 'low'

    def _identify_top_priorities(self, analysis: Dict) -> List[Dict]:
        """Identify top 3 priorities from ad spend analysis"""
        priorities = []

        # Scaling opportunities
        if analysis['scaling_opportunities']:
            top_scale = analysis['scaling_opportunities'][0]
            priorities.append({
                'type': 'scaling',
                'title': f"Scale {top_scale['campaign_name']}",
                'impact': f"+${top_scale['expected_impact']['additional_profit_per_month']:,.0f}/month",
                'action': f"Increase budget to ${top_scale['recommendation']['recommended_monthly_budget']:,.0f}/month"
            })

        # Waste reduction
        if analysis['waste_identified']:
            top_waste = analysis['waste_identified'][0]
            priorities.append({
                'type': 'waste_reduction',
                'title': f"Fix {top_waste['waste_type'].replace('_', ' ').title()}",
                'impact': f"Save ${top_waste['recommendation']['expected_savings']:,.0f}/month",
                'action': top_waste['recommendation']['action']
            })

        # Budget reallocation
        if analysis['budget_reallocations']:
            top_realloc = analysis['budget_reallocations'][0]
            priorities.append({
                'type': 'reallocation',
                'title': top_realloc['optimization_name'],
                'impact': f"+${top_realloc['expected_impact']['additional_profit']:,.0f}/month",
                'action': f"Move ${top_realloc['from_campaign']['budget_reduction']:,.0f} from {top_realloc['from_campaign']['name']} to {top_realloc['to_campaign']['name']}"
            })

        return sorted(priorities, key=lambda x: float(x['impact'].replace('+$', '').replace('Save $', '').replace('/month', '').replace(',', '')), reverse=True)[:3]

    def _identify_quick_wins(self, analysis: Dict) -> List[Dict]:
        """Identify low-effort, high-impact fixes"""
        quick_wins = []

        # Easy waste fixes
        for waste in analysis['waste_identified']:
            if waste['recommendation']['difficulty'] == 'easy' and waste['waste_metrics']['monthly_waste'] > 200:
                quick_wins.append({
                    'type': waste['waste_type'],
                    'description': waste['description'],
                    'action': waste['recommendation']['action'],
                    'savings': f"${waste['recommendation']['expected_savings']:,.0f}/month",
                    'effort': 'Low (< 1 hour)'
                })

        return quick_wins[:3]

    # ── Campaign Intelligence Analytics ──

    async def get_campaign_deep_metrics(self, days: int = 30) -> List[Dict]:
        """
        Per-campaign CPC, CTR, conversion rate, CPA, AOV with week-over-week trends.

        Queries google_ads_campaigns for weekly aggregates and compares
        current vs prior period to determine trend direction.
        """
        log.info(f"Calculating deep metrics for last {days} days")

        end_date = self._get_ads_data_end_date()
        cutoff_current = end_date - timedelta(days=days - 1)
        cutoff_prior = end_date - timedelta(days=(days * 2) - 1)

        # Get active campaigns from campaign_performance
        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return []

        results = []

        for campaign in campaigns:
            cid = campaign.campaign_id

            # Current period daily rows
            current_rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= cutoff_current
            ).all()

            # Prior period daily rows
            prior_rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= cutoff_prior,
                GoogleAdsCampaign.date < cutoff_current
            ).all()

            def _aggregate(rows):
                total_cost = sum((r.cost_micros or 0) for r in rows) / 1_000_000
                total_clicks = sum((r.clicks or 0) for r in rows)
                total_impressions = sum((r.impressions or 0) for r in rows)
                total_conversions = sum((r.conversions or 0) for r in rows)
                total_conv_value = sum((r.conversions_value or 0) for r in rows)

                avg_cpc = total_cost / total_clicks if total_clicks > 0 else 0
                avg_ctr = total_clicks / total_impressions if total_impressions > 0 else 0
                conv_rate = total_conversions / total_clicks if total_clicks > 0 else 0
                cpa = total_cost / total_conversions if total_conversions > 0 else 0
                aov = total_conv_value / total_conversions if total_conversions > 0 else 0

                return {
                    'avg_cpc': round(avg_cpc, 2),
                    'avg_ctr': round(avg_ctr, 4),
                    'conv_rate': round(conv_rate, 4),
                    'cpa': round(cpa, 2),
                    'aov': round(aov, 2),
                }

            current_metrics = _aggregate(current_rows)
            prior_metrics = _aggregate(prior_rows)

            def _trend(current_val, prior_val):
                if prior_val == 0:
                    return "stable"
                if current_val > prior_val * 1.1:
                    return "rising"
                if current_val < prior_val * 0.9:
                    return "falling"
                return "stable"

            trends = {
                'cpc_trend': _trend(current_metrics['avg_cpc'], prior_metrics['avg_cpc']),
                'ctr_trend': _trend(current_metrics['avg_ctr'], prior_metrics['avg_ctr']),
                'conv_rate_trend': _trend(current_metrics['conv_rate'], prior_metrics['conv_rate']),
                'cpa_trend': _trend(current_metrics['cpa'], prior_metrics['cpa']),
                'aov_trend': _trend(current_metrics['aov'], prior_metrics['aov']),
            }

            # Weekly data points for charting
            weekly_data = []
            week_buckets = {}
            for row in current_rows:
                week_key = row.date.isocalendar()[1]
                if week_key not in week_buckets:
                    week_buckets[week_key] = []
                week_buckets[week_key].append(row)

            for week_num in sorted(week_buckets.keys()):
                week_agg = _aggregate(week_buckets[week_num])
                weekly_data.append({
                    'week': week_num,
                    **week_agg
                })

            results.append({
                'campaign_id': cid,
                'campaign_name': campaign.campaign_name,
                'metrics': current_metrics,
                'trends': trends,
                'weekly_data': weekly_data,
            })

        log.info(f"Deep metrics calculated for {len(results)} campaigns")
        return results

    async def calculate_health_scores(self, days: int = 30) -> List[Dict]:
        """
        Composite 0-100 health score per campaign.

        Components: ROAS (0-30), CTR percentile (0-20), CPC trend (0-15),
        Impression share (0-20), Conversion rate percentile (0-15).
        """
        log.info(f"Calculating health scores for last {days} days")

        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return []

        # Collect CTR and conversion rate values for percentile ranking
        ctr_values = sorted([c.click_through_rate or 0 for c in campaigns])
        conv_rates = []
        for c in campaigns:
            if c.actual_conversions and c.total_clicks and c.total_clicks > 0:
                conv_rates.append(c.actual_conversions / c.total_clicks)
            else:
                conv_rates.append(0)
        conv_values_sorted = sorted(conv_rates)

        def _percentile_rank(value, sorted_list):
            if not sorted_list:
                return 0
            count_below = sum(1 for v in sorted_list if v < value)
            return count_below / len(sorted_list)

        end_date = self._get_ads_data_end_date()
        cutoff = end_date - timedelta(days=13)

        results = []

        for idx, campaign in enumerate(campaigns):
            cid = campaign.campaign_id

            # ROAS score (0-30): min(30, true_roas * 6) -- caps at 5x = 30 points
            true_roas = campaign.true_roas or 0
            roas_score = min(30, true_roas * 6)

            # CTR score (0-20): percentile rank among campaigns * 20
            ctr_val = campaign.click_through_rate or 0
            ctr_score = _percentile_rank(ctr_val, ctr_values) * 20

            # CPC trend score (0-15): based on last 2 weeks of daily data
            recent_rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= cutoff
            ).order_by(GoogleAdsCampaign.date).all()

            cpc_trend_score = 10  # default stable
            if len(recent_rows) >= 7:
                mid = len(recent_rows) // 2
                week1 = recent_rows[:mid]
                week2 = recent_rows[mid:]

                w1_cost = sum((r.cost_micros or 0) for r in week1) / 1_000_000
                w1_clicks = sum((r.clicks or 0) for r in week1)
                w2_cost = sum((r.cost_micros or 0) for r in week2) / 1_000_000
                w2_clicks = sum((r.clicks or 0) for r in week2)

                w1_cpc = w1_cost / w1_clicks if w1_clicks > 0 else 0
                w2_cpc = w2_cost / w2_clicks if w2_clicks > 0 else 0

                if w1_cpc > 0:
                    cpc_change = (w2_cpc - w1_cpc) / w1_cpc
                    if cpc_change < -0.05:
                        cpc_trend_score = 15  # falling CPC is good
                    elif cpc_change > 0.20:
                        cpc_trend_score = 0  # sharply rising
                    elif cpc_change > 0.05:
                        cpc_trend_score = 5  # rising
                    else:
                        cpc_trend_score = 10  # stable

            # Impression share score (0-20)
            is_cutoff = end_date - timedelta(days=days - 1)
            is_rows = self.db.query(
                func.avg(GoogleAdsCampaign.search_impression_share)
            ).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= is_cutoff,
                GoogleAdsCampaign.search_impression_share.isnot(None)
            ).scalar()

            avg_is = float(is_rows) if is_rows else 0
            impression_share_score = (avg_is / 100.0) * 20

            # Conversion rate score (0-15): percentile rank * 15
            cr_val = conv_rates[idx]
            conversion_rate_score = _percentile_rank(cr_val, conv_values_sorted) * 15

            health_score = round(
                roas_score + ctr_score + cpc_trend_score
                + impression_share_score + conversion_rate_score, 1
            )

            if health_score >= 80:
                grade = 'A'
            elif health_score >= 65:
                grade = 'B'
            elif health_score >= 50:
                grade = 'C'
            elif health_score >= 35:
                grade = 'D'
            else:
                grade = 'F'

            if health_score >= 65:
                color = 'green'
            elif health_score >= 35:
                color = 'amber'
            else:
                color = 'red'

            results.append({
                'campaign_id': cid,
                'campaign_name': campaign.campaign_name,
                'health_score': health_score,
                'grade': grade,
                'components': {
                    'roas': round(roas_score, 1),
                    'ctr': round(ctr_score, 1),
                    'cpc_trend': round(cpc_trend_score, 1),
                    'impression_share': round(impression_share_score, 1),
                    'conversion_rate': round(conversion_rate_score, 1),
                },
                'color': color,
            })

        # Sort by health_score ascending (worst first)
        results.sort(key=lambda x: x['health_score'])

        log.info(f"Health scores calculated for {len(results)} campaigns")
        return results

    async def calculate_concentration_risk(self, days: int = 30) -> Dict:
        """
        Revenue concentration using the Herfindahl-Hirschman Index (HHI).

        HHI ranges from 0 (perfect competition) to 10000 (single source).
        Identifies how dependent the business is on a few campaigns.
        """
        log.info(f"Calculating concentration risk for last {days} days")

        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return {
                'hhi_score': 0, 'risk_level': 'low',
                'top_2_share_pct': 0, 'top_5_share_pct': 0,
                'campaigns': []
            }

        total_revenue = sum(float(c.actual_revenue or 0) for c in campaigns)

        if total_revenue <= 0:
            return {
                'hhi_score': 0, 'risk_level': 'low',
                'top_2_share_pct': 0, 'top_5_share_pct': 0,
                'campaigns': []
            }

        campaign_shares = []
        for c in campaigns:
            rev = float(c.actual_revenue or 0)
            share = rev / total_revenue
            campaign_shares.append({
                'name': c.campaign_name,
                'revenue': round(rev, 2),
                'share_pct': round(share * 100, 1),
            })

        # Sort by share descending
        campaign_shares.sort(key=lambda x: x['share_pct'], reverse=True)

        shares = [float(c.actual_revenue or 0) / total_revenue for c in campaigns]
        hhi_score = round(sum(s ** 2 for s in shares) * 10000, 1)

        top_2_share = sum(cs['share_pct'] for cs in campaign_shares[:2]) / 100.0
        top_5_share = sum(cs['share_pct'] for cs in campaign_shares[:5]) / 100.0

        if top_2_share > 0.8:
            risk_level = 'critical'
        elif top_2_share > 0.6:
            risk_level = 'high'
        elif top_2_share > 0.4:
            risk_level = 'medium'
        else:
            risk_level = 'low'

        result = {
            'hhi_score': hhi_score,
            'risk_level': risk_level,
            'top_2_share_pct': round(top_2_share * 100, 1),
            'top_5_share_pct': round(top_5_share * 100, 1),
            'campaigns': campaign_shares,
        }

        log.info(f"Concentration risk: HHI={hhi_score}, risk={risk_level}")
        return result

    async def calculate_break_even(self, days: int = 30) -> List[Dict]:
        """
        Break-even ROAS per campaign.

        Uses overhead per order from FinanceService to determine the
        true break-even point for each campaign.
        """
        log.info(f"Calculating break-even ROAS for last {days} days")

        overhead_per_order = FinanceService(self.db).get_latest_overhead_per_order()

        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return []

        results = []

        for campaign in campaigns:
            actual_conversions = campaign.actual_conversions or 0
            actual_revenue = float(campaign.actual_revenue or 0)
            total_spend = float(campaign.total_spend or 0)
            true_roas = campaign.true_roas

            aov = actual_revenue / actual_conversions if actual_conversions > 0 else 0

            if overhead_per_order and aov > 0:
                break_even_roas = 1 + (float(overhead_per_order) / aov)
            else:
                break_even_roas = 1.0

            if break_even_roas > 0 and true_roas is not None:
                margin_of_safety = (true_roas - break_even_roas) / break_even_roas
            else:
                margin_of_safety = 0

            if true_roas is not None and total_spend > 0:
                headroom_dollars = (true_roas - break_even_roas) * total_spend
            else:
                headroom_dollars = 0

            above_break_even = (true_roas >= break_even_roas) if true_roas is not None else False

            results.append({
                'campaign_id': campaign.campaign_id,
                'campaign_name': campaign.campaign_name,
                'actual_roas': round(true_roas, 2) if true_roas is not None else None,
                'break_even_roas': round(break_even_roas, 2),
                'margin_of_safety': round(margin_of_safety, 2),
                'headroom_dollars': round(headroom_dollars, 2),
                'above_break_even': above_break_even,
                'overhead_per_order': float(overhead_per_order) if overhead_per_order else None,
            })

        log.info(f"Break-even calculated for {len(results)} campaigns")
        return results

    async def analyze_diminishing_returns(self, days: int = 90) -> List[Dict]:
        """
        Spend vs ROAS curves to find optimal daily spend.

        Buckets daily spend into quartiles and finds the range with
        the highest average ROAS (the optimal spend level).
        """
        log.info(f"Analyzing diminishing returns for last {days} days")

        end_date = self._get_ads_data_end_date()
        cutoff = end_date - timedelta(days=days - 1)

        # Get distinct campaign IDs that have data
        campaign_ids = self.db.query(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).filter(
            GoogleAdsCampaign.date >= cutoff
        ).group_by(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).all()

        results = []

        for cid, cname in campaign_ids:
            rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= cutoff
            ).all()

            if len(rows) < 14:
                continue

            # Build daily data points
            daily_data = []
            for row in rows:
                daily_spend = (row.cost_micros or 0) / 1_000_000
                if daily_spend > 0:
                    daily_roas = (row.conversions_value or 0) / daily_spend
                else:
                    daily_roas = 0
                daily_data.append({'spend': daily_spend, 'roas': daily_roas})

            if not daily_data:
                continue

            # Sort by spend to calculate quartiles
            spends = sorted(d['spend'] for d in daily_data)
            n = len(spends)

            if n < 4:
                continue

            # Calculate quartile boundaries
            q1 = spends[n // 4]
            q2 = spends[n // 2]
            q3 = spends[(3 * n) // 4]
            boundaries = [0, q1, q2, q3, spends[-1] * 1.01]  # slight overshoot for inclusion

            buckets = []
            for i in range(len(boundaries) - 1):
                lo = boundaries[i]
                hi = boundaries[i + 1]
                bucket_data = [d for d in daily_data if lo <= d['spend'] < hi]

                if not bucket_data:
                    continue

                avg_roas = sum(d['roas'] for d in bucket_data) / len(bucket_data)
                buckets.append({
                    'range_label': f"${lo:.0f}-${hi:.0f}",
                    'min_spend': round(lo, 2),
                    'max_spend': round(hi, 2),
                    'avg_roas': round(avg_roas, 2),
                    'days_count': len(bucket_data),
                })

            if not buckets:
                continue

            # Find optimal bucket (highest avg ROAS)
            optimal_bucket = max(buckets, key=lambda b: b['avg_roas'])
            optimal_midpoint = (optimal_bucket['min_spend'] + optimal_bucket['max_spend']) / 2

            current_daily_spend = sum(d['spend'] for d in daily_data) / len(daily_data)

            overspend = 0
            if current_daily_spend > optimal_bucket['max_spend']:
                overspend = current_daily_spend - optimal_midpoint
                status = 'overspending'
            elif current_daily_spend < optimal_bucket['min_spend']:
                status = 'underspending'
            else:
                status = 'optimal'

            # DR curve confidence: need enough active days and stable buckets
            active_days = len(daily_data)
            min_bucket_days = min(b['days_count'] for b in buckets)
            if active_days >= 21 and min_bucket_days >= 5:
                dr_confidence = 'high'
            elif active_days >= 14 and min_bucket_days >= 3:
                dr_confidence = 'medium'
            else:
                dr_confidence = 'low'

            results.append({
                'campaign_id': cid,
                'campaign_name': cname,
                'buckets': buckets,
                'optimal_daily_spend': round(optimal_midpoint, 2),
                'current_daily_spend': round(current_daily_spend, 2),
                'overspend_per_day': round(overspend, 2),
                'status': status,
                'active_days': active_days,
                'min_bucket_days': min_bucket_days,
                'dr_confidence': dr_confidence,
            })

        log.info(f"Diminishing returns analyzed for {len(results)} campaigns")
        return results

    async def calculate_competitor_pressure(self, days: int = 90) -> List[Dict]:
        """
        Competitive pressure index per campaign.

        Compares CPC and rank-lost impression share between current
        and prior 30-day windows to gauge competitive activity.
        """
        log.info(f"Calculating competitor pressure for last {days} days")

        end_date = self._get_ads_data_end_date()
        window_days = max(14, days // 2)
        current_start = end_date - timedelta(days=window_days - 1)
        prior_end = current_start
        prior_start = prior_end - timedelta(days=window_days)

        # Get campaigns with search_rank_lost_impression_share data
        campaign_ids = self.db.query(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).filter(
            GoogleAdsCampaign.date >= prior_start,
            GoogleAdsCampaign.search_rank_lost_impression_share.isnot(None)
        ).group_by(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).all()

        results = []

        for cid, cname in campaign_ids:
            # Current 30d
            current_rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= current_start
            ).all()

            # Prior 30d
            prior_rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= prior_start,
                GoogleAdsCampaign.date < prior_end
            ).all()

            if not current_rows or not prior_rows:
                continue

            def _calc_cpc(rows):
                total_cost = sum((r.cost_micros or 0) for r in rows) / 1_000_000
                total_clicks = sum((r.clicks or 0) for r in rows)
                return total_cost / total_clicks if total_clicks > 0 else 0

            def _avg_rank_lost(rows):
                vals = [r.search_rank_lost_impression_share for r in rows
                        if r.search_rank_lost_impression_share is not None]
                return sum(vals) / len(vals) if vals else 0

            def _avg_is(rows):
                vals = [r.search_impression_share for r in rows
                        if r.search_impression_share is not None]
                return sum(vals) / len(vals) if vals else 0

            cpc_current = _calc_cpc(current_rows)
            cpc_prior = _calc_cpc(prior_rows)
            rank_lost_current = _avg_rank_lost(current_rows)
            rank_lost_prior = _avg_rank_lost(prior_rows)
            is_current = _avg_is(current_rows)
            is_prior = _avg_is(prior_rows)

            cpc_change_pct = ((cpc_current - cpc_prior) / cpc_prior * 100) if cpc_prior > 0 else 0
            rank_lost_change = rank_lost_current - rank_lost_prior

            # Classify pressure type
            if cpc_change_pct > 5 and rank_lost_change > 3:
                pressure_type = 'competitors_increasing'
                interpretation = (
                    f"CPC up {cpc_change_pct:.0f}% and rank-lost IS increased by "
                    f"{rank_lost_change:.1f} points - competitors likely bidding more aggressively"
                )
            elif cpc_change_pct < -5 and is_current < is_prior:
                pressure_type = 'demand_declining'
                interpretation = (
                    f"CPC down {abs(cpc_change_pct):.0f}% and impression share declining - "
                    f"market demand may be shrinking"
                )
            elif rank_lost_change < -3:
                pressure_type = 'improving'
                interpretation = (
                    f"Rank-lost IS improved by {abs(rank_lost_change):.1f} points - "
                    f"competitive position strengthening"
                )
            else:
                pressure_type = 'stable'
                interpretation = "No significant competitive shifts detected"

            pressure_score = min(100, abs(cpc_change_pct) * 2 + abs(rank_lost_change) * 3)

            results.append({
                'campaign_id': cid,
                'campaign_name': cname,
                'pressure_score': round(pressure_score, 1),
                'pressure_type': pressure_type,
                'cpc_current': round(cpc_current, 2),
                'cpc_prior': round(cpc_prior, 2),
                'cpc_change_pct': round(cpc_change_pct, 1),
                'rank_lost_current': round(rank_lost_current, 1),
                'rank_lost_prior': round(rank_lost_prior, 1),
                'rank_lost_change': round(rank_lost_change, 1),
                'interpretation': interpretation,
            })

        log.info(f"Competitor pressure calculated for {len(results)} campaigns")
        return results

    async def compare_campaign_types(self, days: int = 30) -> Dict:
        """
        Compare performance across campaign types (PMAX vs Search vs Shopping etc.).

        Groups active campaigns by type and computes weighted averages.
        """
        log.info(f"Comparing campaign types for last {days} days")

        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return {'types': [], 'best_type': None, 'recommendation': 'No campaign data available.'}

        type_groups = {}
        for c in campaigns:
            ctype = c.campaign_type or 'unknown'
            if ctype not in type_groups:
                type_groups[ctype] = []
            type_groups[ctype].append(c)

        type_results = []
        for ctype, group in type_groups.items():
            total_spend = sum(float(c.total_spend or 0) for c in group)
            total_revenue = sum(float(c.actual_revenue or 0) for c in group)
            total_profit = sum(float(c.true_profit or 0) for c in group)
            total_clicks = sum((c.total_clicks or 0) for c in group)
            total_impressions = sum((c.total_impressions or 0) for c in group)
            total_conversions = sum((c.actual_conversions or 0) for c in group)

            avg_roas = total_revenue / total_spend if total_spend > 0 else 0
            avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
            avg_ctr = total_clicks / total_impressions if total_impressions > 0 else 0

            type_results.append({
                'type': ctype,
                'campaign_count': len(group),
                'total_spend': round(total_spend, 2),
                'total_revenue': round(total_revenue, 2),
                'total_profit': round(total_profit, 2),
                'avg_roas': round(avg_roas, 2),
                'avg_cpc': round(avg_cpc, 2),
                'avg_ctr': round(avg_ctr, 4),
                'total_conversions': total_conversions,
            })

        type_results.sort(key=lambda x: x['avg_roas'], reverse=True)
        best_type = type_results[0]['type'] if type_results else None

        # Generate recommendation
        if best_type and len(type_results) > 1:
            worst = type_results[-1]
            recommendation = (
                f"{best_type} campaigns deliver the best ROAS ({type_results[0]['avg_roas']:.1f}x). "
                f"Consider shifting budget from {worst['type']} "
                f"({worst['avg_roas']:.1f}x ROAS) to {best_type}."
            )
        elif best_type:
            recommendation = f"Only {best_type} campaigns are active. Diversify campaign types for comparison."
        else:
            recommendation = 'No campaign data available.'

        result = {
            'types': type_results,
            'best_type': best_type,
            'recommendation': recommendation,
        }

        log.info(f"Campaign type comparison complete: {len(type_results)} types")
        return result

    async def detect_anomalies(self, days: int = 30) -> List[Dict]:
        """
        Week-over-week anomaly detection.

        Compares the latest complete week to the previous week across
        CPC, CTR, conversion rate, and spend.
        """
        log.info(f"Detecting anomalies for last {days} days")

        end_date = self._get_ads_data_end_date()
        cutoff = end_date - timedelta(days=days - 1)

        campaign_ids = self.db.query(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).filter(
            GoogleAdsCampaign.date >= cutoff
        ).group_by(
            GoogleAdsCampaign.campaign_id,
            GoogleAdsCampaign.campaign_name
        ).all()

        results = []

        for cid, cname in campaign_ids:
            rows = self.db.query(GoogleAdsCampaign).filter(
                GoogleAdsCampaign.campaign_id == cid,
                GoogleAdsCampaign.date >= cutoff
            ).order_by(GoogleAdsCampaign.date).all()

            if not rows:
                continue

            # Group by ISO week
            week_buckets = {}
            for row in rows:
                wk = row.date.isocalendar()[1]
                if wk not in week_buckets:
                    week_buckets[wk] = []
                week_buckets[wk].append(row)

            sorted_weeks = sorted(week_buckets.keys())
            if len(sorted_weeks) < 2:
                continue

            # Latest complete week vs previous week
            prev_week_rows = week_buckets[sorted_weeks[-2]]
            curr_week_rows = week_buckets[sorted_weeks[-1]]

            def _week_metrics(wk_rows):
                cost = sum((r.cost_micros or 0) for r in wk_rows) / 1_000_000
                clicks = sum((r.clicks or 0) for r in wk_rows)
                impressions = sum((r.impressions or 0) for r in wk_rows)
                conversions = sum((r.conversions or 0) for r in wk_rows)

                cpc = cost / clicks if clicks > 0 else 0
                ctr = clicks / impressions if impressions > 0 else 0
                conv_rate = conversions / clicks if clicks > 0 else 0

                return {
                    'cpc': cpc,
                    'ctr': ctr,
                    'conversion_rate': conv_rate,
                    'spend': cost,
                }

            prev = _week_metrics(prev_week_rows)
            curr = _week_metrics(curr_week_rows)

            # Thresholds: CPC 30%, CTR 30%, conv_rate 40%, spend 50%
            checks = [
                ('CPC', prev['cpc'], curr['cpc'], 0.30),
                ('CTR', prev['ctr'], curr['ctr'], 0.30),
                ('Conversion Rate', prev['conversion_rate'], curr['conversion_rate'], 0.40),
                ('Spend', prev['spend'], curr['spend'], 0.50),
            ]

            anomalies_found = []
            for metric_name, prev_val, curr_val, threshold in checks:
                if prev_val == 0:
                    continue
                change_pct = (curr_val - prev_val) / prev_val
                if abs(change_pct) > threshold:
                    direction = "increased" if change_pct > 0 else "decreased"

                    # Sentiment: is this change good, bad, or neutral?
                    _good_dir = {'CPC': 'down', 'CTR': 'up', 'Conversion Rate': 'up'}
                    dir_key = 'up' if change_pct > 0 else 'down'
                    good_dir = _good_dir.get(metric_name)
                    sentiment = 'positive' if good_dir == dir_key else ('negative' if good_dir and good_dir != dir_key else 'neutral')

                    # Negative interpretations
                    if metric_name == 'CPC' and change_pct > 0:
                        interp = f"CPC increased by {abs(change_pct)*100:.0f}% \u2014 competitors may be bidding more aggressively"
                    elif metric_name == 'CTR' and change_pct < 0:
                        interp = f"CTR decreased by {abs(change_pct)*100:.0f}% \u2014 ad relevance or search intent may have shifted"
                    elif metric_name == 'Conversion Rate' and change_pct < 0:
                        interp = f"Conversion rate dropped by {abs(change_pct)*100:.0f}% \u2014 check landing pages and offer relevance"
                    elif metric_name == 'Spend' and change_pct > 0:
                        interp = f"Spend surged by {abs(change_pct)*100:.0f}% \u2014 review budget settings and bid strategy"
                    # Positive interpretations
                    elif metric_name == 'CPC' and change_pct < 0:
                        interp = f"CPC decreased by {abs(change_pct)*100:.0f}% \u2014 competitive pressure may be easing"
                    elif metric_name == 'CTR' and change_pct > 0:
                        interp = f"CTR improved by {abs(change_pct)*100:.0f}% \u2014 ad relevance may be strengthening"
                    elif metric_name == 'Conversion Rate' and change_pct > 0:
                        interp = f"Conversion rate improved by {abs(change_pct)*100:.0f}% \u2014 verify sustainability"
                    elif metric_name == 'Spend' and change_pct < 0:
                        interp = f"Spend decreased by {abs(change_pct)*100:.0f}% \u2014 check if intentional"
                    else:
                        interp = f"{metric_name} {direction} by {abs(change_pct)*100:.0f}%"

                    anomalies_found.append({
                        'metric': metric_name,
                        'previous_value': round(prev_val, 4),
                        'current_value': round(curr_val, 4),
                        'change_pct': round(change_pct * 100, 1),
                        'interpretation': interp,
                        'sentiment': sentiment,
                    })

            if not anomalies_found:
                continue

            severity = 'critical' if len(anomalies_found) >= 2 else 'warning'

            weekly_spend = curr['spend']
            for anomaly in anomalies_found:
                # Impact score: magnitude * spend (higher = more material)
                impact = abs(anomaly['change_pct']) * weekly_spend / 100
                results.append({
                    'campaign_id': cid,
                    'campaign_name': cname,
                    'metric': anomaly['metric'],
                    'previous_value': anomaly['previous_value'],
                    'current_value': anomaly['current_value'],
                    'change_pct': anomaly['change_pct'],
                    'severity': severity,
                    'interpretation': anomaly['interpretation'],
                    'sentiment': anomaly['sentiment'],
                    'weekly_spend': round(weekly_spend, 2),
                    'impact_score': round(impact, 2),
                })

        # Look up strategy types to deprioritize unknown/zombie campaigns
        from app.models.ad_spend import CampaignPerformance as CP
        strat_map = {
            r.campaign_id: r.strategy_type
            for r in self.db.query(CP.campaign_id, CP.strategy_type).filter(
                CP.strategy_type.isnot(None)
            ).all()
        }
        for a in results:
            a['strategy_type'] = strat_map.get(a['campaign_id'])
            a['is_material'] = (
                a['sentiment'] != 'positive'
                and a['impact_score'] >= 5
                and a.get('strategy_type') != 'unknown'
            )

        # Sort: material negative first (by impact), then non-material, then positive
        def _sort_key(a):
            if a['is_material']:
                return (0, -a['impact_score'])
            elif a['sentiment'] != 'positive':
                return (1, -a['impact_score'])
            else:
                return (2, -a['impact_score'])
        results.sort(key=_sort_key)

        log.info(f"Detected {len(results)} anomalies ({sum(1 for a in results if a['is_material'])} material)")
        return results

    async def forecast_performance(self, days: int = 90) -> Dict:
        """
        Simple linear regression forecast of spend, revenue, and conversions.

        Uses weekly totals over the specified period and projects forward 4 weeks.
        Includes R-squared confidence measure.
        """
        log.info(f"Forecasting performance from last {days} days")

        end_date = self._get_ads_data_end_date()
        cutoff = end_date - timedelta(days=days - 1)

        rows = self.db.query(GoogleAdsCampaign).filter(
            GoogleAdsCampaign.date >= cutoff
        ).order_by(GoogleAdsCampaign.date).all()

        if not rows:
            return {
                'historical': [], 'projected': [],
                'trend_direction': 'stable', 'confidence': 'low', 'r_squared': 0
            }

        # Group by ISO week
        week_data = {}
        for row in rows:
            wk = row.date.isocalendar()[1]
            yr = row.date.isocalendar()[0]
            key = (yr, wk)
            if key not in week_data:
                week_data[key] = {'spend': 0, 'revenue': 0, 'conversions': 0}
            week_data[key]['spend'] += (row.cost_micros or 0) / 1_000_000
            week_data[key]['revenue'] += (row.conversions_value or 0)
            week_data[key]['conversions'] += (row.conversions or 0)

        sorted_keys = sorted(week_data.keys())
        if len(sorted_keys) < 3:
            return {
                'historical': [], 'projected': [],
                'trend_direction': 'stable', 'confidence': 'low', 'r_squared': 0
            }

        historical = []
        for i, key in enumerate(sorted_keys):
            d = week_data[key]
            roas = d['revenue'] / d['spend'] if d['spend'] > 0 else 0
            historical.append({
                'week': i,
                'week_label': f"{key[0]}-W{key[1]:02d}",
                'spend': round(d['spend'], 2),
                'revenue': round(d['revenue'], 2),
                'conversions': round(d['conversions'], 1),
                'roas': round(roas, 2),
            })

        def _linear_regression(x_vals, y_vals):
            n = len(x_vals)
            if n < 2:
                return 0, 0, 0

            sum_x = sum(x_vals)
            sum_y = sum(y_vals)
            sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
            sum_x2 = sum(x * x for x in x_vals)

            denom = n * sum_x2 - sum_x * sum_x
            if denom == 0:
                return 0, sum_y / n if n > 0 else 0, 0

            m = (n * sum_xy - sum_x * sum_y) / denom
            b = (sum_y - m * sum_x) / n

            # R-squared
            mean_y = sum_y / n
            ss_tot = sum((y - mean_y) ** 2 for y in y_vals)
            ss_res = sum((y - (m * x + b)) ** 2 for x, y in zip(x_vals, y_vals))
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

            return m, b, r_squared

        x = [h['week'] for h in historical]
        rev_y = [h['revenue'] for h in historical]
        spend_y = [h['spend'] for h in historical]
        conv_y = [h['conversions'] for h in historical]

        m_rev, b_rev, r2_rev = _linear_regression(x, rev_y)
        m_spend, b_spend, _ = _linear_regression(x, spend_y)
        m_conv, b_conv, _ = _linear_regression(x, conv_y)

        # Project next 4 weeks
        last_week = x[-1]
        projected = []
        for j in range(1, 5):
            future_x = last_week + j
            proj_spend = max(0, m_spend * future_x + b_spend)
            proj_revenue = max(0, m_rev * future_x + b_rev)
            proj_conversions = max(0, m_conv * future_x + b_conv)
            proj_roas = proj_revenue / proj_spend if proj_spend > 0 else 0

            projected.append({
                'week': future_x,
                'week_label': f"Projected +{j}",
                'spend': round(proj_spend, 2),
                'revenue': round(proj_revenue, 2),
                'conversions': round(proj_conversions, 1),
                'roas': round(proj_roas, 2),
                'is_projection': True,
            })

        # Trend direction based on revenue slope
        mean_rev = sum(rev_y) / len(rev_y) if rev_y else 1
        if mean_rev > 0 and abs(m_rev / mean_rev) < 0.05:
            trend_direction = 'stable'
        elif m_rev > 0:
            trend_direction = 'growing'
        else:
            trend_direction = 'declining'

        # Confidence from R-squared
        if r2_rev > 0.7:
            confidence = 'high'
        elif r2_rev > 0.4:
            confidence = 'medium'
        else:
            confidence = 'low'

        result = {
            'historical': historical,
            'projected': projected,
            'trend_direction': trend_direction,
            'confidence': confidence,
            'r_squared': round(r2_rev, 3),
        }

        log.info(f"Forecast complete: trend={trend_direction}, confidence={confidence}, R2={r2_rev:.3f}")
        return result

    async def get_google_vs_reality(self, days: int = 30) -> Dict:
        """
        Compare Google's reported metrics vs actual Shopify revenue.

        Calculates inflation percentage per campaign and overall totals
        to show how much Google over-reports.
        """
        log.info(f"Calculating Google vs reality for last {days} days")

        campaigns = self._get_campaigns_for_period(days)

        if not campaigns:
            return {
                'campaigns': [],
                'totals': {
                    'google_total_revenue': 0, 'actual_total_revenue': 0,
                    'inflation_pct': 0, 'avg_google_roas': 0, 'avg_true_roas': 0
                }
            }

        campaign_results = []
        total_google_revenue = 0
        total_actual_revenue = 0
        total_spend = 0

        for c in campaigns:
            google_roas = c.google_roas
            true_roas = c.true_roas
            google_revenue = float(c.google_conversion_value or 0)
            actual_revenue = float(c.actual_revenue or 0)
            spend = float(c.total_spend or 0)

            if true_roas and true_roas > 0:
                inflation_pct = ((google_roas - true_roas) / true_roas * 100) if google_roas else 0
            else:
                inflation_pct = 0

            campaign_results.append({
                'campaign_id': c.campaign_id,
                'campaign_name': c.campaign_name,
                'google_roas': round(google_roas, 2) if google_roas else None,
                'true_roas': round(true_roas, 2) if true_roas else None,
                'google_revenue': round(google_revenue, 2),
                'actual_revenue': round(actual_revenue, 2),
                'inflation_pct': round(inflation_pct, 1),
            })

            total_google_revenue += google_revenue
            total_actual_revenue += actual_revenue
            total_spend += spend

        if total_actual_revenue > 0:
            total_inflation_pct = ((total_google_revenue - total_actual_revenue)
                                   / total_actual_revenue * 100)
        else:
            total_inflation_pct = 0

        avg_google_roas = total_google_revenue / total_spend if total_spend > 0 else 0
        avg_true_roas = total_actual_revenue / total_spend if total_spend > 0 else 0

        result = {
            'campaigns': campaign_results,
            'totals': {
                'google_total_revenue': round(total_google_revenue, 2),
                'actual_total_revenue': round(total_actual_revenue, 2),
                'inflation_pct': round(total_inflation_pct, 1),
                'avg_google_roas': round(avg_google_roas, 2),
                'avg_true_roas': round(avg_true_roas, 2),
            }
        }

        log.info(f"Google vs reality: {total_inflation_pct:.1f}% inflation across {len(campaign_results)} campaigns")
        return result
