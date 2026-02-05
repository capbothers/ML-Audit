"""
User Behavior Intelligence Service

Analyzes user behavior patterns from Hotjar/Clarity to identify friction points.
Answers: "Where are users getting stuck? What's breaking the experience?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from decimal import Decimal
import statistics

from app.models.user_behavior import (
    PageFriction,
    CheckoutFunnel,
    DeviceComparison,
    SessionInsight
)
from app.utils.logger import log


class UserBehaviorService:
    """Service for user behavior intelligence"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds
        self.high_friction_threshold = 60  # Friction score >= 60 = high friction
        self.rage_click_threshold = 10  # >= 10 rage clicks = problem
        self.conversion_gap_threshold = 1.0  # 1% points below average = underperforming
        self.mobile_desktop_gap_threshold = 10.0  # 10% points = significant gap

        # Benchmarks
        self.avg_site_conversion_rate = 0.024  # 2.4% (will be calculated from actual data)
        self.avg_checkout_completion = 0.55  # 55% complete checkout

    async def analyze_all_behavior(self, days: int = 30) -> Dict:
        """
        Complete user behavior analysis

        Returns comprehensive insights on friction, checkout, mobile issues
        """
        log.info(f"Analyzing user behavior for last {days} days")

        results = {
            'high_friction_pages': [],
            'checkout_funnel': [],
            'mobile_issues': [],
            'rage_click_pages': [],
            'session_patterns': [],
            'summary': {}
        }

        # 1. High-friction pages
        friction_pages = await self.find_high_friction_pages(days)
        results['high_friction_pages'] = friction_pages

        # 2. Checkout funnel analysis
        checkout_funnel = await self.analyze_checkout_funnel(days)
        results['checkout_funnel'] = checkout_funnel

        # 3. Mobile vs desktop issues
        mobile_issues = await self.find_mobile_issues(days)
        results['mobile_issues'] = mobile_issues

        # 4. Rage click pages
        rage_pages = await self.find_rage_click_pages(days)
        results['rage_click_pages'] = rage_pages

        # 5. Session patterns
        patterns = await self.analyze_session_patterns(days)
        results['session_patterns'] = patterns

        # Summary
        total_revenue_impact = (
            sum(float(p.get('estimated_revenue_lost', 0)) for p in friction_pages) +
            sum(float(s.get('estimated_revenue_lost', 0)) for s in checkout_funnel) +
            sum(float(m.get('estimated_revenue_lost', 0)) for m in mobile_issues)
        )

        results['summary'] = {
            'high_friction_pages_count': len(friction_pages),
            'checkout_steps_analyzed': len(checkout_funnel),
            'mobile_issues_count': len(mobile_issues),
            'rage_click_pages_count': len(rage_pages),
            'total_estimated_revenue_impact': total_revenue_impact,
            'period_days': days
        }

        log.info(f"Behavior analysis complete: ${total_revenue_impact:,.0f} revenue impact identified")

        return results

    async def find_high_friction_pages(self, days: int = 30) -> List[Dict]:
        """
        Find pages with high friction (rage clicks, dead clicks, low conversion)

        Returns pages sorted by revenue impact
        """
        log.info("Finding high-friction pages")

        friction_pages = self.db.query(PageFriction).filter(
            PageFriction.is_high_friction == True,
            PageFriction.period_days == days
        ).order_by(
            desc(PageFriction.estimated_monthly_revenue_lost)
        ).limit(10).all()

        results = []

        for page in friction_pages:
            result = {
                'page_path': page.page_path,
                'page_title': page.page_title,
                'page_type': page.page_type,

                'traffic': {
                    'monthly_sessions': page.total_sessions,
                    'page_views': page.page_views
                },

                'conversion': {
                    'conversion_rate': round(page.conversion_rate * 100, 1),
                    'site_average': round(page.avg_conversion_rate * 100, 1) if page.avg_conversion_rate else None,
                    'gap': round(page.conversion_rate_gap * 100, 1) if page.conversion_rate_gap else None
                },

                'friction_signals': {
                    'rage_clicks': page.rage_click_count,
                    'rage_click_sessions': page.rage_click_sessions,
                    'rage_click_rate': round(page.rage_click_rate * 100, 1) if page.rage_click_rate else None,
                    'dead_clicks': page.dead_click_count,
                    'dead_click_sessions': page.dead_click_sessions
                },

                'engagement': {
                    'median_scroll_depth': round(page.median_scroll_depth, 1) if page.median_scroll_depth else None,
                    'percent_reach_cta': round(page.percent_reach_cta, 1) if page.percent_reach_cta else None,
                    'percent_reach_specs': round(page.percent_reach_specs, 1) if page.percent_reach_specs else None,
                    'avg_time_on_page': round(page.avg_time_on_page) if page.avg_time_on_page else None
                },

                'mobile_vs_desktop': {
                    'mobile_conversion': round(page.mobile_conversion_rate * 100, 1) if page.mobile_conversion_rate else None,
                    'desktop_conversion': round(page.desktop_conversion_rate * 100, 1) if page.desktop_conversion_rate else None,
                    'gap': round(page.mobile_desktop_gap * 100, 1) if page.mobile_desktop_gap else None
                },

                'friction_elements': page.friction_elements or [],

                'severity': {
                    'friction_score': page.friction_score,
                    'severity_level': page.severity,
                    'priority': page.priority
                },

                'revenue_impact': {
                    'estimated_revenue_lost': float(page.estimated_monthly_revenue_lost),
                    'monthly_traffic': page.estimated_monthly_traffic
                },

                'issues': page.issues_detected or [],
                'recommended_fixes': page.recommended_fixes or []
            }

            results.append(result)

        log.info(f"Found {len(results)} high-friction pages")

        return results

    async def analyze_checkout_funnel(self, days: int = 30) -> List[Dict]:
        """
        Analyze checkout funnel step-by-step

        Identifies the biggest leaks and friction points
        """
        log.info("Analyzing checkout funnel")

        funnel_steps = self.db.query(CheckoutFunnel).filter(
            CheckoutFunnel.period_days == days
        ).order_by(
            CheckoutFunnel.step_number
        ).all()

        if not funnel_steps:
            log.warning("No checkout funnel data available")
            return []

        results = []
        biggest_leak = None
        max_drop_rate = 0

        for step in funnel_steps:
            drop_rate = step.drop_off_rate or 0

            # Track biggest leak
            if drop_rate > max_drop_rate:
                max_drop_rate = drop_rate
                biggest_leak = step.step_name

            result = {
                'step_number': step.step_number,
                'step_name': step.step_name,
                'step_url': step.step_url,

                'metrics': {
                    'sessions_entered': step.sessions_entered,
                    'sessions_completed': step.sessions_completed,
                    'sessions_dropped': step.sessions_dropped,
                    'completion_rate': round(step.completion_rate * 100, 1) if step.completion_rate else None,
                    'drop_off_rate': round(drop_rate * 100, 1)
                },

                'timing': {
                    'avg_time_on_step': round(step.avg_time_on_step) if step.avg_time_on_step else None,
                    'median_time': round(step.median_time_on_step) if step.median_time_on_step else None
                },

                'friction_signals': {
                    'rage_clicks': step.rage_click_count,
                    'stuck_sessions': step.stuck_sessions,
                    'back_button_clicks': step.back_button_clicks,
                    'page_reloads': step.reload_count,
                    'error_messages': step.error_message_count
                },

                'mobile_vs_desktop': {
                    'mobile_completion': round(step.mobile_completion_rate * 100, 1) if step.mobile_completion_rate else None,
                    'desktop_completion': round(step.desktop_completion_rate * 100, 1) if step.desktop_completion_rate else None,
                    'gap': round(step.mobile_desktop_gap * 100, 1) if step.mobile_desktop_gap else None
                },

                'revenue_impact': {
                    'estimated_revenue_lost': float(step.estimated_revenue_lost),
                    'avg_order_value': float(step.avg_order_value) if step.avg_order_value else None,
                    'monthly_sessions': step.estimated_sessions_per_month
                },

                'is_biggest_leak': step.is_biggest_leak,
                'issues': step.issues_detected or [],
                'recommended_fixes': step.recommended_fixes or [],
                'priority': step.priority
            }

            results.append(result)

        # Mark biggest leak
        for result in results:
            if result['step_name'] == biggest_leak:
                result['is_biggest_leak'] = True

        log.info(f"Analyzed {len(results)} checkout steps, biggest leak: {biggest_leak}")

        return results

    async def find_mobile_issues(self, days: int = 30) -> List[Dict]:
        """
        Find pages with significant mobile vs desktop performance gaps

        Returns mobile-specific UX problems
        """
        log.info("Finding mobile issues")

        mobile_issues = self.db.query(DeviceComparison).filter(
            DeviceComparison.mobile_underperforming == True,
            DeviceComparison.period_days == days
        ).order_by(
            desc(DeviceComparison.estimated_mobile_revenue_lost)
        ).limit(10).all()

        results = []

        for issue in mobile_issues:
            result = {
                'page_path': issue.page_path,
                'page_type': issue.page_type,

                'traffic_split': {
                    'mobile_sessions': issue.mobile_sessions,
                    'desktop_sessions': issue.desktop_sessions,
                    'mobile_percentage': round(issue.mobile_traffic_percentage * 100, 1) if issue.mobile_traffic_percentage else None
                },

                'conversion_comparison': {
                    'mobile_conversion': round(issue.mobile_conversion_rate * 100, 1) if issue.mobile_conversion_rate else None,
                    'desktop_conversion': round(issue.desktop_conversion_rate * 100, 1) if issue.desktop_conversion_rate else None,
                    'gap': round(issue.conversion_rate_gap * 100, 1) if issue.conversion_rate_gap else None
                },

                'engagement_comparison': {
                    'mobile_avg_time': round(issue.mobile_avg_time) if issue.mobile_avg_time else None,
                    'desktop_avg_time': round(issue.desktop_avg_time) if issue.desktop_avg_time else None,
                    'mobile_bounce_rate': round(issue.mobile_bounce_rate * 100, 1) if issue.mobile_bounce_rate else None,
                    'desktop_bounce_rate': round(issue.desktop_bounce_rate * 100, 1) if issue.desktop_bounce_rate else None
                },

                'mobile_friction': {
                    'rage_clicks': issue.mobile_rage_clicks,
                    'dead_clicks': issue.mobile_dead_clicks,
                    'scroll_issues': issue.mobile_scroll_issues,
                    'content_cut_off': issue.content_cut_off_mobile,
                    'horizontal_scroll': issue.horizontal_scroll_mobile
                },

                'mobile_specific_problems': issue.mobile_specific_problems or [],
                'small_touch_targets': issue.small_touch_targets or [],

                'revenue_impact': {
                    'estimated_revenue_lost': float(issue.estimated_mobile_revenue_lost),
                    'severity': issue.severity,
                    'priority': issue.priority
                },

                'recommended_fixes': issue.recommended_fixes or []
            }

            results.append(result)

        log.info(f"Found {len(results)} mobile issues")

        return results

    async def find_rage_click_pages(self, days: int = 30) -> List[Dict]:
        """
        Find pages with high rage click rates (user frustration)

        Returns pages sorted by rage click count
        """
        log.info("Finding rage click pages")

        rage_pages = self.db.query(PageFriction).filter(
            PageFriction.rage_click_count >= self.rage_click_threshold,
            PageFriction.period_days == days
        ).order_by(
            desc(PageFriction.rage_click_count)
        ).limit(10).all()

        results = []

        for page in rage_pages:
            # Extract most clicked element
            top_element = None
            if page.friction_elements:
                top_element = max(
                    page.friction_elements,
                    key=lambda x: x.get('click_count', 0)
                ) if page.friction_elements else None

            result = {
                'page_path': page.page_path,
                'page_title': page.page_title,

                'rage_clicks': {
                    'total_rage_clicks': page.rage_click_count,
                    'sessions_with_rage': page.rage_click_sessions,
                    'rage_click_rate': round(page.rage_click_rate * 100, 1) if page.rage_click_rate else None
                },

                'top_frustration_element': top_element,
                'all_friction_elements': page.friction_elements or [],

                'traffic': page.total_sessions,
                'conversion_rate': round(page.conversion_rate * 100, 1),

                'diagnosis': self._diagnose_rage_clicks(page),

                'recommended_fix': page.recommended_fixes[0] if page.recommended_fixes else None
            }

            results.append(result)

        log.info(f"Found {len(results)} pages with rage clicks")

        return results

    async def analyze_session_patterns(self, days: int = 30) -> List[Dict]:
        """
        Analyze common session behavior patterns

        Identifies frustration patterns and abandonment sequences
        """
        log.info("Analyzing session patterns")

        patterns = self.db.query(SessionInsight).filter(
            SessionInsight.period_days == days
        ).order_by(
            desc(SessionInsight.sessions_with_pattern)
        ).limit(10).all()

        results = []

        for pattern in patterns:
            result = {
                'pattern_name': pattern.pattern_name,
                'pattern_type': pattern.pattern_type,
                'description': pattern.description,

                'prevalence': {
                    'sessions_with_pattern': pattern.sessions_with_pattern,
                    'percentage_of_total': round(pattern.percentage_of_total, 1) if pattern.percentage_of_total else None
                },

                'common_pages': pattern.common_pages or [],
                'common_devices': pattern.common_devices,

                'outcomes': {
                    'conversion_rate': round(pattern.conversion_rate * 100, 1) if pattern.conversion_rate else None,
                    'avg_session_value': float(pattern.avg_session_value) if pattern.avg_session_value else None,
                    'bounce_rate': round(pattern.bounce_rate * 100, 1) if pattern.bounce_rate else None
                },

                'timing': {
                    'avg_time_before_pattern': round(pattern.avg_time_before_pattern) if pattern.avg_time_before_pattern else None,
                    'time_to_conversion_if_overcome': round(pattern.time_to_conversion_if_overcome) if pattern.time_to_conversion_if_overcome else None
                },

                'event_sequence': pattern.event_sequence or [],

                'revenue_impact': {
                    'estimated_impact': float(pattern.estimated_revenue_impact),
                    'monthly_sessions': pattern.estimated_sessions_per_month
                },

                'is_fixable': pattern.is_fixable,
                'recommended_actions': pattern.recommended_actions or [],
                'priority': pattern.priority
            }

            results.append(result)

        log.info(f"Found {len(results)} session patterns")

        return results

    async def get_page_analysis(self, page_path: str, days: int = 30) -> Optional[Dict]:
        """
        Detailed analysis for a specific page

        Returns comprehensive friction analysis
        """
        log.info(f"Analyzing page: {page_path}")

        page = self.db.query(PageFriction).filter(
            PageFriction.page_path == page_path,
            PageFriction.period_days == days
        ).first()

        if not page:
            return None

        # Get device comparison
        device_data = self.db.query(DeviceComparison).filter(
            DeviceComparison.page_path == page_path,
            DeviceComparison.period_days == days
        ).first()

        result = {
            'page_path': page.page_path,
            'page_title': page.page_title,
            'page_type': page.page_type,

            'traffic': {
                'total_sessions': page.total_sessions,
                'unique_visitors': page.unique_visitors,
                'page_views': page.page_views
            },

            'conversion': {
                'conversion_rate': round(page.conversion_rate * 100, 1),
                'site_average': round(page.avg_conversion_rate * 100, 1) if page.avg_conversion_rate else None,
                'underperforming': page.conversion_rate < (page.avg_conversion_rate or 0)
            },

            'engagement': {
                'avg_time_on_page': round(page.avg_time_on_page) if page.avg_time_on_page else None,
                'median_scroll_depth': round(page.median_scroll_depth, 1) if page.median_scroll_depth else None,
                'bounce_rate': round(page.bounce_rate * 100, 1) if page.bounce_rate else None,
                'exit_rate': round(page.exit_rate * 100, 1) if page.exit_rate else None
            },

            'friction_analysis': {
                'friction_score': page.friction_score,
                'severity': page.severity,
                'rage_clicks': page.rage_click_count,
                'dead_clicks': page.dead_click_count,
                'friction_elements': page.friction_elements or []
            },

            'scroll_depth': {
                'median_depth': round(page.median_scroll_depth, 1) if page.median_scroll_depth else None,
                'reach_cta': round(page.percent_reach_cta, 1) if page.percent_reach_cta else None,
                'reach_specs': round(page.percent_reach_specs, 1) if page.percent_reach_specs else None,
                'content_below_fold': page.content_below_fold
            },

            'device_comparison': None,

            'revenue_impact': {
                'estimated_monthly_lost': float(page.estimated_monthly_revenue_lost)
            },

            'diagnosis': {
                'issues': page.issues_detected or [],
                'recommended_fixes': page.recommended_fixes or [],
                'priority': page.priority
            }
        }

        # Add device comparison if available
        if device_data:
            result['device_comparison'] = {
                'mobile_conversion': round(device_data.mobile_conversion_rate * 100, 1) if device_data.mobile_conversion_rate else None,
                'desktop_conversion': round(device_data.desktop_conversion_rate * 100, 1) if device_data.desktop_conversion_rate else None,
                'gap': round(device_data.conversion_rate_gap * 100, 1) if device_data.conversion_rate_gap else None,
                'mobile_issues': device_data.mobile_specific_problems or []
            }

        return result

    async def get_behavior_dashboard(self, days: int = 30) -> Dict:
        """
        Complete user behavior dashboard

        Everything you need to know about UX friction
        """
        log.info("Generating behavior dashboard")

        # Get all analyses
        analysis = await self.analyze_all_behavior(days)

        # Top 3 priorities
        top_priorities = self._identify_top_priorities(analysis)

        return {
            'generated_at': datetime.utcnow().isoformat(),

            'summary': analysis['summary'],

            'top_priorities': top_priorities,

            'high_friction_pages': analysis['high_friction_pages'][:5],
            'checkout_funnel': analysis['checkout_funnel'],
            'mobile_issues': analysis['mobile_issues'][:5],
            'rage_click_pages': analysis['rage_click_pages'][:3],

            'quick_wins': self._identify_quick_wins(analysis),

            'period_days': days
        }

    def _diagnose_rage_clicks(self, page: PageFriction) -> str:
        """Diagnose why users are rage clicking"""
        if not page.friction_elements:
            return "Users clicking repeatedly but specific element unknown"

        top_element = max(
            page.friction_elements,
            key=lambda x: x.get('click_count', 0)
        )

        issue_type = top_element.get('issue', 'unknown')

        diagnoses = {
            'not_clickable': f"Users clicking '{top_element.get('element')}' expecting it to be clickable",
            'appears_broken': f"Element '{top_element.get('element')}' appears broken or unresponsive",
            'slow_response': f"Element '{top_element.get('element')}' has slow response time",
            'unclear_function': f"Users confused by '{top_element.get('element')}' functionality"
        }

        return diagnoses.get(issue_type, f"Repeated clicks on '{top_element.get('element')}'")

    def _identify_top_priorities(self, analysis: Dict) -> List[Dict]:
        """Identify top 3 priorities from behavior analysis"""
        priorities = []

        # Checkout funnel leaks
        checkout_steps = analysis.get('checkout_funnel', [])
        if checkout_steps:
            biggest_leak = max(checkout_steps, key=lambda x: x['metrics']['drop_off_rate'])
            if biggest_leak['metrics']['drop_off_rate'] > 30:  # > 30% drop
                priorities.append({
                    'type': 'checkout_leak',
                    'title': f"Fix {biggest_leak['step_name']} (Biggest Leak)",
                    'impact': f"+${biggest_leak['revenue_impact']['estimated_revenue_lost']:,.0f}/month",
                    'fix': biggest_leak['recommended_fixes'][0] if biggest_leak['recommended_fixes'] else "Investigate friction points"
                })

        # High friction pages
        friction_pages = analysis.get('high_friction_pages', [])
        if friction_pages:
            top_friction = friction_pages[0]
            priorities.append({
                'type': 'page_friction',
                'title': f"Fix {top_friction['page_path']}",
                'impact': f"+${top_friction['revenue_impact']['estimated_revenue_lost']:,.0f}/month",
                'fix': top_friction['recommended_fixes'][0] if top_friction['recommended_fixes'] else "Reduce friction"
            })

        # Mobile issues
        mobile_issues = analysis.get('mobile_issues', [])
        if mobile_issues:
            top_mobile = mobile_issues[0]
            priorities.append({
                'type': 'mobile_issue',
                'title': f"Mobile Fix: {top_mobile['page_path']}",
                'impact': f"+${top_mobile['revenue_impact']['estimated_revenue_lost']:,.0f}/month",
                'fix': top_mobile['recommended_fixes'][0] if top_mobile['recommended_fixes'] else "Optimize for mobile"
            })

        return sorted(priorities, key=lambda x: float(x['impact'].replace('+$', '').replace('/month', '').replace(',', '')), reverse=True)[:3]

    def _identify_quick_wins(self, analysis: Dict) -> List[Dict]:
        """Identify low-effort, high-impact fixes"""
        quick_wins = []

        # Rage click fixes (usually simple)
        rage_pages = analysis.get('rage_click_pages', [])
        for page in rage_pages[:2]:
            if page.get('top_frustration_element'):
                quick_wins.append({
                    'page': page['page_path'],
                    'issue': f"Rage clicks on {page['top_frustration_element'].get('element')}",
                    'fix': page.get('recommended_fix', 'Make element clickable'),
                    'effort': 'Low (< 1 hour)'
                })

        return quick_wins
