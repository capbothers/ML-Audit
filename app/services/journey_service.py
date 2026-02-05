"""
Customer Journey Intelligence Service

Analyzes customer behavior patterns to understand what creates high-LTV customers.
Answers: "What separates repeat customers from one-and-done buyers?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from decimal import Decimal
import statistics

from app.models.journey import (
    CustomerLTV,
    JourneyPattern,
    GatewayProduct,
    DeadEndProduct,
    ChurnRiskTiming
)
from app.models.shopify import ShopifyOrder, ShopifyCustomer
from app.utils.logger import log


class JourneyService:
    """Service for customer journey intelligence"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds
        self.min_customers_for_analysis = 100  # Min customers to detect patterns
        self.gateway_product_threshold = 1.5  # X times higher repeat rate than average
        self.dead_end_product_threshold = 0.7  # Lower repeat rate than this = dead-end

    async def analyze_all_journeys(self) -> Dict:
        """
        Complete customer journey analysis

        Returns comprehensive insights on customer patterns
        """
        log.info("Analyzing customer journeys")

        results = {
            'ltv_segments': {},
            'gateway_products': [],
            'dead_end_products': [],
            'journey_patterns': [],
            'churn_risk_timing': {},
            'summary': {}
        }

        # 1. Calculate LTV segments
        ltv_segments = await self.calculate_ltv_segments()
        results['ltv_segments'] = ltv_segments

        # 2. Identify gateway products
        gateway_products = await self.identify_gateway_products()
        results['gateway_products'] = gateway_products

        # 3. Identify dead-end products
        dead_end_products = await self.identify_dead_end_products()
        results['dead_end_products'] = dead_end_products

        # 4. Analyze journey patterns
        patterns = await self.analyze_journey_patterns()
        results['journey_patterns'] = patterns

        # 5. Calculate churn risk timing
        churn_timing = await self.calculate_churn_risk_timing()
        results['churn_risk_timing'] = churn_timing

        # Summary
        results['summary'] = {
            'total_customers': sum(ltv_segments.values(), start=0) if isinstance(ltv_segments, dict) else 0,
            'gateway_products_count': len(gateway_products),
            'dead_end_products_count': len(dead_end_products),
            'patterns_identified': len(patterns),
            'customers_at_risk': churn_timing.get('customers_at_risk', 0)
        }

        log.info(f"Journey analysis complete: {results['summary']['total_customers']} customers analyzed")

        return results

    async def calculate_ltv_segments(self) -> Dict:
        """
        Calculate LTV segments (top 20%, middle 60%, bottom 20%)

        Returns segment breakdown with characteristics
        """
        log.info("Calculating LTV segments")

        # Get all customers with LTV
        customers = self.db.query(CustomerLTV).filter(
            CustomerLTV.total_ltv > 0
        ).order_by(desc(CustomerLTV.total_ltv)).all()

        if not customers:
            log.warning("No customer LTV data available")
            return {}

        total_customers = len(customers)

        # Calculate percentile thresholds
        top_20_idx = int(total_customers * 0.20)
        bottom_20_idx = int(total_customers * 0.80)

        # Segment customers
        top_20 = customers[:top_20_idx]
        middle_60 = customers[top_20_idx:bottom_20_idx]
        bottom_20 = customers[bottom_20_idx:]

        # Calculate segment characteristics
        def segment_stats(segment: List[CustomerLTV], name: str) -> Dict:
            if not segment:
                return {}

            return {
                'segment_name': name,
                'customer_count': len(segment),
                'percentage': (len(segment) / total_customers) * 100,

                'avg_ltv': float(statistics.mean([c.total_ltv for c in segment])),
                'median_ltv': float(statistics.median([c.total_ltv for c in segment])),
                'min_ltv': float(min([c.total_ltv for c in segment])),
                'max_ltv': float(max([c.total_ltv for c in segment])),

                'avg_orders': statistics.mean([c.total_orders for c in segment]),
                'avg_aov': float(statistics.mean([c.avg_order_value for c in segment])),

                'repeat_customer_rate': sum(1 for c in segment if c.is_repeat_customer) / len(segment) * 100,

                'avg_days_to_second_order': statistics.mean([
                    c.days_to_second_order for c in segment
                    if c.days_to_second_order is not None
                ]) if any(c.days_to_second_order for c in segment) else None,

                'email_subscriber_rate': sum(1 for c in segment if c.email_subscriber) / len(segment) * 100,
                'subscribed_before_purchase_rate': sum(1 for c in segment if c.subscribed_before_first_purchase) / len(segment) * 100,
            }

        result = {
            'total_customers': total_customers,
            'segments': {
                'top_20': segment_stats(top_20, 'Top 20% (High LTV)'),
                'middle_60': segment_stats(middle_60, 'Middle 60%'),
                'bottom_20': segment_stats(bottom_20, 'Bottom 20% (Low LTV)')
            },

            'key_differences': self._calculate_segment_differences(
                segment_stats(top_20, 'top_20'),
                segment_stats(bottom_20, 'bottom_20')
            )
        }

        log.info(f"Segmented {total_customers} customers into LTV tiers")

        return result

    async def identify_gateway_products(self, min_first_purchases: int = 50) -> List[Dict]:
        """
        Identify products that lead to repeat purchases

        Gateway products: First purchase → high repeat rate → high LTV

        Args:
            min_first_purchases: Minimum customers for statistical significance

        Returns:
            List of gateway products sorted by impact
        """
        log.info("Identifying gateway products")

        gateway_products = self.db.query(GatewayProduct).filter(
            GatewayProduct.total_first_purchases >= min_first_purchases,
            GatewayProduct.ltv_multiplier >= self.gateway_product_threshold
        ).order_by(
            desc(GatewayProduct.promotion_opportunity_score)
        ).limit(10).all()

        results = []

        for product in gateway_products:
            result = {
                'product_title': product.product_title,
                'product_sku': product.product_sku,
                'product_category': product.product_category,

                'metrics': {
                    'total_first_purchases': product.total_first_purchases,
                    'repeat_purchase_rate': round(product.repeat_purchase_rate * 100, 1),
                    'repeat_rate_vs_average': f"{product.repeat_rate_lift:.1f}x higher",
                    'avg_ltv': float(product.avg_ltv_from_this_product),
                    'ltv_vs_average': f"{product.ltv_multiplier:.1f}x higher"
                },

                'journey_impact': {
                    'avg_days_to_second_purchase': product.avg_days_to_second_purchase,
                    'avg_total_orders': product.avg_total_orders
                },

                'current_promotion': {
                    'is_featured': product.is_featured,
                    'is_in_ads': product.is_in_ads,
                    'is_in_email_flows': product.is_in_email_flows,
                    'promotion_score': product.current_promotion_score
                },

                'opportunity': {
                    'should_be_promoted': product.should_be_promoted,
                    'opportunity_score': product.promotion_opportunity_score,
                    'estimated_ltv_gain': float(product.estimated_ltv_gain)
                },

                'recommended_actions': product.recommended_actions or []
            }

            results.append(result)

        log.info(f"Found {len(results)} gateway products")

        return results

    async def identify_dead_end_products(self, min_first_purchases: int = 50) -> List[Dict]:
        """
        Identify products that correlate with customer churn

        Dead-end products: First purchase → low/no repeat rate → one-and-done

        Args:
            min_first_purchases: Minimum customers for statistical significance

        Returns:
            List of dead-end products sorted by severity
        """
        log.info("Identifying dead-end products")

        dead_end_products = self.db.query(DeadEndProduct).filter(
            DeadEndProduct.total_first_purchases >= min_first_purchases,
            DeadEndProduct.one_time_rate >= self.dead_end_product_threshold
        ).order_by(
            desc(DeadEndProduct.estimated_ltv_lost)
        ).limit(10).all()

        results = []

        for product in dead_end_products:
            result = {
                'product_title': product.product_title,
                'product_sku': product.product_sku,
                'product_category': product.product_category,

                'metrics': {
                    'total_first_purchases': product.total_first_purchases,
                    'one_time_rate': round(product.one_time_rate * 100, 1),
                    'one_time_rate_vs_average': f"{product.one_time_rate_difference:+.1f}% points",
                    'return_rate': round(product.return_rate * 100, 1) if product.return_rate else None,
                    'avg_ltv': float(product.avg_ltv_from_this_product),
                    'ltv_vs_average': f"{product.ltv_penalty:.1f}x lower"
                },

                'why_customers_dont_return': {
                    'high_return_rate': product.return_rate > 0.15 if product.return_rate else False,
                    'attracts_bargain_hunters': product.price_sensitivity_score > 0.7 if product.price_sensitivity_score else False,
                    'avg_discount_used': round(product.avg_discount_used * 100, 1) if product.avg_discount_used else None
                },

                'current_promotion': {
                    'is_featured': product.is_featured,
                    'is_in_ads': product.is_in_ads,
                    'current_ad_spend': float(product.current_ad_spend) if product.current_ad_spend else 0,
                    'is_actively_promoted': product.is_actively_promoted
                },

                'problem_severity': {
                    'severity': product.severity,
                    'estimated_ltv_lost': float(product.estimated_ltv_lost),
                    'should_stop_promoting': product.should_stop_promoting
                },

                'recommended_actions': product.recommended_actions or []
            }

            results.append(result)

        log.info(f"Found {len(results)} dead-end products")

        return results

    async def analyze_journey_patterns(self) -> List[Dict]:
        """
        Identify common journey patterns

        Analyzes what high-LTV customers do differently
        """
        log.info("Analyzing journey patterns")

        patterns = self.db.query(JourneyPattern).filter(
            JourneyPattern.customer_count >= self.min_customers_for_analysis
        ).order_by(
            desc(JourneyPattern.avg_ltv)
        ).limit(10).all()

        results = []

        for pattern in patterns:
            result = {
                'pattern_name': pattern.pattern_name,
                'pattern_type': pattern.pattern_type,

                'characteristics': {
                    'first_product_category': pattern.first_product_category,
                    'first_channel': pattern.first_channel,
                    'avg_days_to_second_purchase': pattern.avg_days_to_second_purchase,
                    'email_subscribed_first': pattern.email_subscribed_first
                },

                'prevalence': {
                    'customer_count': pattern.customer_count,
                    'percentage_of_segment': round(pattern.percentage_of_segment, 1) if pattern.percentage_of_segment else None
                },

                'outcomes': {
                    'avg_ltv': float(pattern.avg_ltv),
                    'avg_orders': pattern.avg_orders,
                    'avg_aov': float(pattern.avg_aov) if pattern.avg_aov else None,
                    'repeat_purchase_rate': round(pattern.repeat_purchase_rate * 100, 1) if pattern.repeat_purchase_rate else None
                },

                'vs_baseline': {
                    'ltv_difference': f"{pattern.ltv_vs_baseline:+.1f}%" if pattern.ltv_vs_baseline else None,
                    'repeat_rate_difference': f"{pattern.repeat_rate_vs_baseline:+.1f}%" if pattern.repeat_rate_vs_baseline else None
                },

                'description': pattern.description,
                'key_characteristics': pattern.key_characteristics,
                'is_desirable_pattern': pattern.is_desirable_pattern,
                'recommended_actions': pattern.recommended_actions or []
            }

            results.append(result)

        log.info(f"Found {len(results)} journey patterns")

        return results

    async def calculate_churn_risk_timing(self) -> Dict:
        """
        Calculate optimal timing for customer reactivation

        When do customers churn? When should we reach out?
        """
        log.info("Calculating churn risk timing")

        # Get timing for each LTV segment
        timings = self.db.query(ChurnRiskTiming).all()

        if not timings:
            log.warning("No churn timing data available")
            return {}

        result = {
            'by_segment': {},
            'overall': {},
            'customers_at_risk': 0,
            'total_ltv_at_risk': 0
        }

        for timing in timings:
            segment_data = {
                'segment': timing.ltv_segment,

                'timing_metrics': {
                    'avg_days_between_purchases': timing.avg_days_between_purchases,
                    'median_days_between_purchases': timing.median_days_between_purchases,
                    'std_dev_days': timing.std_dev_days
                },

                'risk_thresholds': {
                    'at_risk_days': timing.at_risk_threshold_days,
                    'critical_risk_days': timing.critical_risk_threshold_days
                },

                'reactivation_window': {
                    'optimal_start_day': timing.optimal_reactivation_day_min,
                    'optimal_end_day': timing.optimal_reactivation_day_max,
                    'success_rate': round(timing.reactivation_success_rate * 100, 1) if timing.reactivation_success_rate else None
                },

                'current_at_risk': {
                    'customers_at_risk': timing.customers_at_risk,
                    'customers_critical_risk': timing.customers_critical_risk,
                    'total_ltv_at_risk': float(timing.total_ltv_at_risk)
                },

                'winback_effectiveness': {
                    'open_rate': round(timing.winback_open_rate * 100, 1) if timing.winback_open_rate else None,
                    'conversion_rate': round(timing.winback_conversion_rate * 100, 1) if timing.winback_conversion_rate else None,
                    'avg_order_value': float(timing.avg_winback_order_value) if timing.avg_winback_order_value else None
                }
            }

            result['by_segment'][timing.ltv_segment] = segment_data

            # Aggregate
            result['customers_at_risk'] += timing.customers_at_risk
            result['total_ltv_at_risk'] += float(timing.total_ltv_at_risk)

        log.info(f"{result['customers_at_risk']} customers at churn risk")

        return result

    def _calculate_segment_differences(self, top_20: Dict, bottom_20: Dict) -> Dict:
        """
        Calculate key differences between high and low LTV segments

        Returns specific metrics showing what separates them
        """
        if not top_20 or not bottom_20:
            return {}

        differences = {}

        # LTV difference
        if 'avg_ltv' in top_20 and 'avg_ltv' in bottom_20:
            ltv_multiplier = top_20['avg_ltv'] / bottom_20['avg_ltv'] if bottom_20['avg_ltv'] > 0 else 0
            differences['ltv_multiplier'] = f"{ltv_multiplier:.1f}x higher"

        # Days to second order
        if top_20.get('avg_days_to_second_order') and bottom_20.get('avg_days_to_second_order'):
            days_diff = top_20['avg_days_to_second_order'] - bottom_20['avg_days_to_second_order']
            differences['days_to_second_order_diff'] = f"{days_diff:+.0f} days"
            differences['second_order_speed'] = "faster" if days_diff < 0 else "slower"

        # Email subscription
        if 'subscribed_before_purchase_rate' in top_20 and 'subscribed_before_purchase_rate' in bottom_20:
            email_diff = top_20['subscribed_before_purchase_rate'] - bottom_20['subscribed_before_purchase_rate']
            differences['email_subscription_diff'] = f"{email_diff:+.1f}% points"

        # AOV
        if 'avg_aov' in top_20 and 'avg_aov' in bottom_20:
            aov_diff = top_20['avg_aov'] - bottom_20['avg_aov']
            differences['aov_diff'] = f"${aov_diff:+,.2f}"

        return differences

    async def get_journey_dashboard(self) -> Dict:
        """
        Complete customer journey dashboard

        Everything you need to know about customer patterns
        """
        log.info("Generating journey dashboard")

        # Get all analyses
        analysis = await self.analyze_all_journeys()

        # Top gateway products
        top_gateways = analysis['gateway_products'][:3]

        # Worst dead-end products
        worst_dead_ends = analysis['dead_end_products'][:3]

        return {
            'generated_at': datetime.utcnow().isoformat(),

            'summary': analysis['summary'],

            'ltv_segments': analysis['ltv_segments'],

            'top_gateway_products': top_gateways,
            'worst_dead_end_products': worst_dead_ends,

            'journey_patterns': analysis['journey_patterns'][:5],

            'churn_risk': analysis['churn_risk_timing'],

            'key_insights': {
                'high_ltv_characteristics': self._extract_high_ltv_characteristics(analysis),
                'biggest_opportunities': self._identify_biggest_opportunities(analysis)
            }
        }

    def _extract_high_ltv_characteristics(self, analysis: Dict) -> List[str]:
        """Extract key characteristics of high-LTV customers"""
        characteristics = []

        ltv_data = analysis.get('ltv_segments', {})
        if ltv_data and 'key_differences' in ltv_data:
            diffs = ltv_data['key_differences']

            if 'days_to_second_order_diff' in diffs:
                characteristics.append(f"Buy again {diffs['days_to_second_order_diff']} {diffs.get('second_order_speed', '')}")

            if 'email_subscription_diff' in diffs:
                characteristics.append(f"Email subscription rate {diffs['email_subscription_diff']} higher")

        return characteristics

    def _identify_biggest_opportunities(self, analysis: Dict) -> List[Dict]:
        """Identify top 3 opportunities from analysis"""
        opportunities = []

        # Gateway products
        gateways = analysis.get('gateway_products', [])
        if gateways:
            top_gateway = gateways[0]
            if top_gateway['opportunity']['should_be_promoted']:
                opportunities.append({
                    'type': 'gateway_product',
                    'title': f"Promote {top_gateway['product_title']}",
                    'impact': f"+${top_gateway['opportunity']['estimated_ltv_gain']:,.0f} LTV",
                    'action': top_gateway['recommended_actions'][0] if top_gateway['recommended_actions'] else "Feature prominently"
                })

        # Dead-end products
        dead_ends = analysis.get('dead_end_products', [])
        if dead_ends:
            worst_dead_end = dead_ends[0]
            if worst_dead_end['problem_severity']['should_stop_promoting']:
                opportunities.append({
                    'type': 'dead_end_product',
                    'title': f"Stop promoting {worst_dead_end['product_title']}",
                    'impact': f"Avoid ${worst_dead_end['problem_severity']['estimated_ltv_lost']:,.0f} LTV loss",
                    'action': worst_dead_end['recommended_actions'][0] if worst_dead_end['recommended_actions'] else "Reduce ad spend"
                })

        # Churn risk
        churn_data = analysis.get('churn_risk_timing', {})
        if churn_data.get('customers_at_risk', 0) > 0:
            opportunities.append({
                'type': 'churn_prevention',
                'title': f"Reactivate {churn_data['customers_at_risk']} at-risk customers",
                'impact': f"${churn_data['total_ltv_at_risk']:,.0f} LTV at risk",
                'action': "Send win-back campaign"
            })

        return opportunities[:3]
