"""
Attribution Analysis Service

Builds customer journeys and calculates multi-touch attribution
to show which channels are really driving conversions.

Answers: "Where should I actually spend my next dollar?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from collections import defaultdict
import math

from app.models.attribution import (
    CustomerTouchpoint, CustomerJourney, ChannelAttribution, AttributionInsight
)
from app.utils.logger import log


class AttributionService:
    """Service for multi-touch attribution analysis"""

    def __init__(self, db: Session):
        self.db = db

    async def build_customer_journeys(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict]:
        """
        Build customer journeys from touchpoints

        Groups touchpoints by user and creates journey paths
        """
        log.info(f"Building customer journeys from {start_date} to {end_date}")

        # Get all touchpoints in period, grouped by user
        touchpoints = self.db.query(CustomerTouchpoint).filter(
            CustomerTouchpoint.timestamp >= start_date,
            CustomerTouchpoint.timestamp <= end_date
        ).order_by(CustomerTouchpoint.user_id, CustomerTouchpoint.timestamp).all()

        if not touchpoints:
            log.warning("No touchpoints found in period")
            return []

        # Group by user
        journeys_by_user = defaultdict(list)
        for tp in touchpoints:
            journeys_by_user[tp.user_id].append(tp)

        # Build journey for each user
        journeys = []
        for user_id, user_touchpoints in journeys_by_user.items():
            journey = await self._build_single_journey(user_id, user_touchpoints)
            journeys.append(journey)

            # Save to database
            self._save_journey(journey)

        log.info(f"Built {len(journeys)} customer journeys")
        return journeys

    async def _build_single_journey(
        self,
        user_id: str,
        touchpoints: List[CustomerTouchpoint]
    ) -> Dict:
        """Build a single customer journey from touchpoints"""

        if not touchpoints:
            return {}

        # Sort by timestamp
        touchpoints = sorted(touchpoints, key=lambda tp: tp.timestamp)

        first_touch = touchpoints[0]
        last_touch = touchpoints[-1]

        # Check if journey led to conversion
        # (In real implementation, would check against orders table)
        converted = any(tp.attributed_revenue > 0 for tp in touchpoints)
        conversion_date = last_touch.timestamp if converted else None

        # Calculate journey metrics
        touchpoint_count = len(touchpoints)
        days_to_conversion = None
        if converted and conversion_date:
            days_to_conversion = (conversion_date - first_touch.timestamp).days

        # Count touchpoints by channel
        channel_touchpoints = defaultdict(int)
        for tp in touchpoints:
            channel_touchpoints[tp.channel] += 1

        # Get unique channels that assisted
        assisted_channels = list(set(tp.channel for tp in touchpoints))

        # Build journey path string
        journey_path = " -> ".join([tp.channel for tp in touchpoints])

        # Total revenue from conversion
        total_revenue = sum(tp.attributed_revenue for tp in touchpoints)

        # Calculate attribution models
        linear_attr = self._calculate_linear_attribution(touchpoints, total_revenue)
        time_decay_attr = self._calculate_time_decay_attribution(touchpoints, total_revenue)
        position_attr = self._calculate_position_based_attribution(touchpoints, total_revenue)

        journey = {
            'user_id': user_id,
            'customer_id': first_touch.customer_id,

            'first_touch_date': first_touch.timestamp,
            'last_touch_date': last_touch.timestamp,
            'conversion_date': conversion_date,

            'touchpoint_count': touchpoint_count,
            'days_to_conversion': days_to_conversion,

            'first_touch_channel': first_touch.channel,
            'first_touch_source': first_touch.source,
            'first_touch_campaign': first_touch.campaign,

            'last_touch_channel': last_touch.channel,
            'last_touch_source': last_touch.source,
            'last_touch_campaign': last_touch.campaign,

            'assisted_channels': assisted_channels,
            'channel_touchpoints': dict(channel_touchpoints),

            'converted': converted,
            'revenue': total_revenue,

            'linear_attribution': linear_attr,
            'time_decay_attribution': time_decay_attr,
            'position_based_attribution': position_attr,

            'journey_path': journey_path,
            'is_first_purchase': True  # Would check against customer table
        }

        return journey

    def _calculate_linear_attribution(
        self,
        touchpoints: List[CustomerTouchpoint],
        total_revenue: float
    ) -> Dict[str, float]:
        """
        Linear attribution: Equal credit to all touchpoints

        If 4 touchpoints, each gets 25% credit
        """
        if not touchpoints or total_revenue == 0:
            return {}

        credit_per_touchpoint = total_revenue / len(touchpoints)

        attribution = defaultdict(float)
        for tp in touchpoints:
            attribution[tp.channel] += credit_per_touchpoint

        return dict(attribution)

    def _calculate_time_decay_attribution(
        self,
        touchpoints: List[CustomerTouchpoint],
        total_revenue: float,
        half_life_days: int = 7
    ) -> Dict[str, float]:
        """
        Time decay attribution: More recent touchpoints get more credit

        Uses exponential decay with 7-day half-life
        """
        if not touchpoints or total_revenue == 0:
            return {}

        if len(touchpoints) == 1:
            return {touchpoints[0].channel: total_revenue}

        # Calculate decay weights
        last_touchpoint_time = touchpoints[-1].timestamp
        weights = []

        for tp in touchpoints:
            days_before_conversion = (last_touchpoint_time - tp.timestamp).days
            # Exponential decay: weight = 2^(-days / half_life)
            weight = math.pow(2, -days_before_conversion / half_life_days)
            weights.append(weight)

        total_weight = sum(weights)

        # Distribute revenue based on weights
        attribution = defaultdict(float)
        for tp, weight in zip(touchpoints, weights):
            credit = (weight / total_weight) * total_revenue
            attribution[tp.channel] += credit

        return dict(attribution)

    def _calculate_position_based_attribution(
        self,
        touchpoints: List[CustomerTouchpoint],
        total_revenue: float,
        first_credit: float = 0.4,
        last_credit: float = 0.4
    ) -> Dict[str, float]:
        """
        Position-based attribution (U-shaped):
        - 40% credit to first touch
        - 40% credit to last touch
        - 20% split among middle touches
        """
        if not touchpoints or total_revenue == 0:
            return {}

        if len(touchpoints) == 1:
            return {touchpoints[0].channel: total_revenue}

        if len(touchpoints) == 2:
            return {
                touchpoints[0].channel: total_revenue * 0.5,
                touchpoints[1].channel: total_revenue * 0.5
            }

        attribution = defaultdict(float)

        # First touch: 40%
        attribution[touchpoints[0].channel] += total_revenue * first_credit

        # Last touch: 40%
        attribution[touchpoints[-1].channel] += total_revenue * last_credit

        # Middle touches: 20% split equally
        middle_credit = 1.0 - first_credit - last_credit
        middle_touchpoints = touchpoints[1:-1]

        if middle_touchpoints:
            credit_per_middle = (total_revenue * middle_credit) / len(middle_touchpoints)
            for tp in middle_touchpoints:
                attribution[tp.channel] += credit_per_middle

        return dict(attribution)

    def _save_journey(self, journey: Dict):
        """Save journey to database"""
        try:
            journey_record = CustomerJourney(**journey)
            self.db.add(journey_record)
            self.db.commit()
        except Exception as e:
            log.error(f"Error saving journey: {str(e)}")
            self.db.rollback()

    async def calculate_channel_attribution(
        self,
        start_date: datetime,
        end_date: datetime,
        period_type: str = "monthly"
    ) -> List[Dict]:
        """
        Calculate aggregated attribution by channel

        Compares different attribution models to show
        which channels are over/under-credited
        """
        log.info(f"Calculating channel attribution from {start_date} to {end_date}")

        # Get all completed journeys in period
        journeys = self.db.query(CustomerJourney).filter(
            CustomerJourney.conversion_date >= start_date,
            CustomerJourney.conversion_date <= end_date,
            CustomerJourney.converted == True
        ).all()

        if not journeys:
            log.warning("No converted journeys found in period")
            return []

        # Aggregate by channel
        channel_data = defaultdict(lambda: {
            'last_click_conversions': 0,
            'last_click_revenue': 0.0,
            'first_click_conversions': 0,
            'first_click_revenue': 0.0,
            'linear_conversions': 0.0,
            'linear_revenue': 0.0,
            'time_decay_conversions': 0.0,
            'time_decay_revenue': 0.0,
            'position_conversions': 0.0,
            'position_revenue': 0.0,
            'assisted_conversions': 0,
            'total_spend': 0.0  # Would pull from ad platforms
        })

        # Process each journey
        for journey in journeys:
            # Last-click attribution (Google's default)
            last_channel = journey.last_touch_channel
            channel_data[last_channel]['last_click_conversions'] += 1
            channel_data[last_channel]['last_click_revenue'] += journey.revenue

            # First-click attribution
            first_channel = journey.first_touch_channel
            channel_data[first_channel]['first_click_conversions'] += 1
            channel_data[first_channel]['first_click_revenue'] += journey.revenue

            # Linear attribution
            if journey.linear_attribution:
                for channel, revenue in journey.linear_attribution.items():
                    channel_data[channel]['linear_revenue'] += revenue
                    # Fractional conversions
                    channel_data[channel]['linear_conversions'] += revenue / journey.revenue if journey.revenue > 0 else 0

            # Time decay attribution
            if journey.time_decay_attribution:
                for channel, revenue in journey.time_decay_attribution.items():
                    channel_data[channel]['time_decay_revenue'] += revenue
                    channel_data[channel]['time_decay_conversions'] += revenue / journey.revenue if journey.revenue > 0 else 0

            # Position-based attribution
            if journey.position_based_attribution:
                for channel, revenue in journey.position_based_attribution.items():
                    channel_data[channel]['position_revenue'] += revenue
                    channel_data[channel]['position_conversions'] += revenue / journey.revenue if journey.revenue > 0 else 0

            # Assisted conversions (touched but not last)
            for channel in journey.assisted_channels:
                if channel != journey.last_touch_channel:
                    channel_data[channel]['assisted_conversions'] += 1

        # Calculate totals and percentages
        total_last_click_revenue = sum(data['last_click_revenue'] for data in channel_data.values())
        total_linear_revenue = sum(data['linear_revenue'] for data in channel_data.values())

        results = []
        for channel, data in channel_data.items():
            # Calculate credit percentages
            last_click_pct = (data['last_click_revenue'] / total_last_click_revenue * 100) if total_last_click_revenue > 0 else 0
            linear_pct = (data['linear_revenue'] / total_linear_revenue * 100) if total_linear_revenue > 0 else 0

            # The truth: how much difference between models?
            credit_difference = linear_pct - last_click_pct

            # Determine if over/under-credited
            is_overcredited = credit_difference < -5  # Last-click gives 5%+ more credit
            is_undercredited = credit_difference > 5  # Multi-touch gives 5%+ more credit

            # Calculate ROAS (if spend data available)
            true_roas = (data['linear_revenue'] / data['total_spend']) if data['total_spend'] > 0 else None
            reported_roas = (data['last_click_revenue'] / data['total_spend']) if data['total_spend'] > 0 else None

            # Assist ratio
            assist_ratio = (data['assisted_conversions'] / data['last_click_conversions']) if data['last_click_conversions'] > 0 else 0

            result = {
                'channel': channel,

                'last_click_conversions': data['last_click_conversions'],
                'last_click_revenue': round(data['last_click_revenue'], 2),
                'last_click_credit_pct': round(last_click_pct, 1),

                'linear_conversions': round(data['linear_conversions'], 2),
                'linear_revenue': round(data['linear_revenue'], 2),
                'linear_credit_pct': round(linear_pct, 1),

                'time_decay_conversions': round(data['time_decay_conversions'], 2),
                'time_decay_revenue': round(data['time_decay_revenue'], 2),

                'position_conversions': round(data['position_conversions'], 2),
                'position_revenue': round(data['position_revenue'], 2),

                'assisted_conversions': data['assisted_conversions'],
                'assist_ratio': round(assist_ratio, 2),

                'credit_difference_pct': round(credit_difference, 1),
                'is_overcredited': is_overcredited,
                'is_undercredited': is_undercredited,

                'total_spend': data['total_spend'],
                'true_roas': round(true_roas, 2) if true_roas else None,
                'reported_roas': round(reported_roas, 2) if reported_roas else None,

                'period_start': start_date,
                'period_end': end_date,
                'period_type': period_type
            }

            results.append(result)

            # Save to database
            self._save_channel_attribution(result)

        # Sort by linear revenue (true value)
        results.sort(key=lambda x: x['linear_revenue'], reverse=True)

        log.info(f"Calculated attribution for {len(results)} channels")
        return results

    def _save_channel_attribution(self, data: Dict):
        """Save channel attribution to database"""
        try:
            record = ChannelAttribution(**data)
            self.db.add(record)
            self.db.commit()
        except Exception as e:
            log.error(f"Error saving channel attribution: {str(e)}")
            self.db.rollback()

    async def get_attribution_insights(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict:
        """
        Get high-level attribution insights

        Shows which channels are over/under-credited
        and budget reallocation opportunities
        """
        # Get channel attribution
        attribution = await self.calculate_channel_attribution(start_date, end_date)

        if not attribution:
            return {
                'message': 'No attribution data available for this period',
                'overcredited_channels': [],
                'undercredited_channels': [],
                'budget_recommendations': []
            }

        # Find overcredited channels (last-click gives too much credit)
        overcredited = [
            ch for ch in attribution
            if ch['is_overcredited']
        ]
        overcredited.sort(key=lambda x: abs(x['credit_difference_pct']), reverse=True)

        # Find undercredited channels (multi-touch shows more value)
        undercredited = [
            ch for ch in attribution
            if ch['is_undercredited']
        ]
        undercredited.sort(key=lambda x: x['credit_difference_pct'], reverse=True)

        # Calculate budget reallocation opportunities
        budget_recs = self._calculate_budget_reallocation(attribution)

        return {
            'period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            },

            'summary': {
                'total_channels': len(attribution),
                'overcredited_count': len(overcredited),
                'undercredited_count': len(undercredited)
            },

            'overcredited_channels': overcredited[:5],  # Top 5
            'undercredited_channels': undercredited[:5],

            'budget_recommendations': budget_recs,

            'all_channels': attribution
        }

    def _calculate_budget_reallocation(self, attribution: List[Dict]) -> List[Dict]:
        """
        Calculate budget reallocation recommendations

        Move money from overcredited to undercredited channels
        """
        recommendations = []

        overcredited = [ch for ch in attribution if ch['is_overcredited'] and ch['total_spend'] > 0]
        undercredited = [ch for ch in attribution if ch['is_undercredited']]

        for over_ch in overcredited[:3]:  # Top 3 overcredited
            for under_ch in undercredited[:3]:  # Top 3 undercredited
                # Calculate potential reallocation
                amount_to_move = min(
                    over_ch['total_spend'] * 0.2,  # Max 20% of current spend
                    1000  # Max $1000 to start
                )

                # Estimate impact (rough)
                expected_revenue_loss = amount_to_move * (over_ch['true_roas'] or 0)
                expected_revenue_gain = amount_to_move * (under_ch['true_roas'] or 0)
                net_impact = expected_revenue_gain - expected_revenue_loss

                if net_impact > 0:
                    recommendations.append({
                        'from_channel': over_ch['channel'],
                        'to_channel': under_ch['channel'],
                        'amount': round(amount_to_move, 2),
                        'reason': f"{over_ch['channel']} is overcredited by {abs(over_ch['credit_difference_pct'])}%, {under_ch['channel']} is undercredited by {under_ch['credit_difference_pct']}%",
                        'expected_net_impact': round(net_impact, 2)
                    })

        # Sort by expected impact
        recommendations.sort(key=lambda x: x['expected_net_impact'], reverse=True)

        return recommendations[:5]  # Top 5 recommendations

    async def get_journey_analysis(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict:
        """
        Analyze common customer journey patterns

        Shows typical paths to conversion
        """
        journeys = self.db.query(CustomerJourney).filter(
            CustomerJourney.conversion_date >= start_date,
            CustomerJourney.conversion_date <= end_date,
            CustomerJourney.converted == True
        ).all()

        if not journeys:
            return {'message': 'No converted journeys found'}

        # Analyze journey patterns
        journey_paths = defaultdict(int)
        touchpoint_counts = defaultdict(int)
        days_to_conversion_list = []

        for journey in journeys:
            if journey.journey_path:
                journey_paths[journey.journey_path] += 1

            if journey.touchpoint_count:
                touchpoint_counts[journey.touchpoint_count] += 1

            if journey.days_to_conversion is not None:
                days_to_conversion_list.append(journey.days_to_conversion)

        # Most common paths
        common_paths = sorted(journey_paths.items(), key=lambda x: x[1], reverse=True)[:10]

        # Average metrics
        avg_touchpoints = sum(j.touchpoint_count for j in journeys if j.touchpoint_count) / len(journeys)
        avg_days = sum(days_to_conversion_list) / len(days_to_conversion_list) if days_to_conversion_list else 0

        return {
            'total_journeys': len(journeys),
            'average_touchpoints': round(avg_touchpoints, 1),
            'average_days_to_conversion': round(avg_days, 1),

            'most_common_paths': [
                {
                    'path': path,
                    'count': count,
                    'percentage': round(count / len(journeys) * 100, 1)
                }
                for path, count in common_paths
            ],

            'touchpoint_distribution': dict(touchpoint_counts)
        }
