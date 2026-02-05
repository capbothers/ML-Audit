"""
Continuous Monitoring Service
The safety net that catches everything before it becomes a problem
"""
import asyncio
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import hashlib
import json

from sqlalchemy import func
from sqlalchemy.sql import and_

from app.services.data_sync_service import DataSyncService
from app.services.analysis_service import AnalysisService
from app.services.alert_service import AlertService
from app.services.llm_service import LLMService
from app.models.base import SessionLocal
from app.models.data_quality import TrackingAlert, DataSyncStatus
from app.models.analytics import DataSyncLog
from app.models.shopify import ShopifyOrder
from app.models.ga4_data import GA4TrafficSource
from app.models.google_ads_data import GoogleAdsCampaign
from app.models.klaviyo_data import KlaviyoCampaign
from app.models.transaction import AbandonedCheckout
from app.utils.logger import log
from app.config import get_settings

settings = get_settings()


class MonitoringService:
    """
    Continuous monitoring service that acts as a safety net
    Catches problems and opportunities before they become critical
    """

    def __init__(self):
        self.data_sync = DataSyncService()
        self.analysis_service = AnalysisService()
        self.alert_service = AlertService()
        self.llm_service = LLMService()

        # Track baselines for comparison
        self.baselines = {}
        self.last_check = {}
        # Note: cooldowns are now persisted to database (TrackingAlert.created_at)

        # Control flag for stopping the monitoring loop
        self._running = False

        # Define what metrics to monitor
        self.monitored_metrics = {
            'revenue': {'threshold': 0.15, 'cooldown_hours': 4},
            'conversion_rate': {'threshold': 0.20, 'cooldown_hours': 6},
            'roas': {'threshold': 0.25, 'cooldown_hours': 4},
            'traffic': {'threshold': 0.30, 'cooldown_hours': 6},
            'cart_abandonment': {'threshold': 0.15, 'cooldown_hours': 8},
            'email_open_rate': {'threshold': 0.20, 'cooldown_hours': 12},
            'ad_disapprovals': {'threshold': 0, 'cooldown_hours': 1},  # Alert immediately
        }

    async def start_continuous_monitoring(self):
        """
        Main monitoring loop - runs continuously in the background
        """
        log.info("Starting continuous monitoring service...")
        self._running = True

        while self._running:
            try:
                log.info("Running monitoring check...")

                # 1. Sync latest data
                await self._sync_latest_data()

                # 2. Check all metrics
                issues = await self._check_all_metrics()

                # 3. For each issue, diagnose and alert
                for issue in issues:
                    if not self._running:
                        break
                    await self._diagnose_and_alert(issue)

                # 4. Check for opportunities
                opportunities = await self._detect_opportunities()

                for opp in opportunities:
                    if not self._running:
                        break
                    await self._alert_opportunity(opp)

                # 5. Check connector health (stale/failed syncs)
                connector_issues = await self._check_connector_health()

                for conn_issue in connector_issues:
                    if not self._running:
                        break
                    await self._alert_connector_issue(conn_issue)

                log.info(
                    f"Monitoring check complete. Found {len(issues)} issues, "
                    f"{len(opportunities)} opportunities, {len(connector_issues)} connector issues"
                )

                # Wait before next check (default: 15 minutes)
                # Check running flag periodically during sleep
                for _ in range(90):  # 90 * 10s = 15 minutes
                    if not self._running:
                        break
                    await asyncio.sleep(10)

            except Exception as e:
                log.error(f"Error in monitoring loop: {str(e)}")
                # Wait 5 min on error, but check running flag
                for _ in range(30):  # 30 * 10s = 5 minutes
                    if not self._running:
                        break
                    await asyncio.sleep(10)

        log.info("Monitoring service stopped")

    def stop(self):
        """Stop the continuous monitoring loop"""
        log.info("Stopping monitoring service...")
        self._running = False

    def is_running(self) -> bool:
        """Check if monitoring is currently running"""
        return self._running

    async def _sync_latest_data(self):
        """Sync latest data from all sources"""
        try:
            # Quick sync of last 24 hours only
            await self.data_sync.sync_all(days=1)
        except Exception as e:
            log.error(f"Error syncing data: {str(e)}")

    async def _check_all_metrics(self) -> List[Dict]:
        """
        Check all monitored metrics against baselines
        Returns list of issues detected
        """
        issues = []

        # Get latest metrics (you'd pull this from your database in production)
        current_metrics = await self._get_current_metrics()
        baseline_metrics = await self._get_baseline_metrics()

        for metric_name, config in self.monitored_metrics.items():
            threshold = config['threshold']

            current = current_metrics.get(metric_name)
            baseline = baseline_metrics.get(metric_name)

            if current is None or baseline is None:
                continue

            # Guard against division by zero - skip metrics with zero baseline
            # (can't meaningfully calculate percentage change from zero)
            if baseline == 0:
                # Special case: if current > 0 but baseline is 0, that's new activity (not an issue)
                # If both are 0, nothing to alert on
                log.debug(f"Skipping {metric_name}: baseline is 0")
                continue

            # Check if metric dropped below threshold
            if metric_name == 'cart_abandonment':
                # Higher is bad for cart abandonment
                if current > baseline * (1 + threshold):
                    issues.append({
                        'metric': metric_name,
                        'current': current,
                        'baseline': baseline,
                        'change_pct': ((current - baseline) / baseline) * 100,
                        'severity': self._calculate_severity(metric_name, current, baseline),
                        'threshold': threshold,
                        'direction': 'increase'
                    })
            else:
                # Lower is bad for most metrics
                if current < baseline * (1 - threshold):
                    issues.append({
                        'metric': metric_name,
                        'current': current,
                        'baseline': baseline,
                        'change_pct': ((current - baseline) / baseline) * 100,
                        'severity': self._calculate_severity(metric_name, current, baseline),
                        'threshold': threshold,
                        'direction': 'decrease'
                    })

        # Special check: Ad disapprovals (always alert)
        disapproved_count = await self._check_disapproved_ads()
        if disapproved_count > 0:
            issues.append({
                'metric': 'ad_disapprovals',
                'current': disapproved_count,
                'baseline': 0,
                'change_pct': 0,
                'severity': 'critical',
                'threshold': 0,
                'direction': 'new'
            })

        return issues

    async def _diagnose_and_alert(self, issue: Dict):
        """
        When an issue is detected:
        1. Gather all related context
        2. Use LLM to diagnose WHY
        3. Send smart alert with diagnosis and recommendations
        """
        metric = issue['metric']

        # Check cooldown to prevent alert spam
        if self._is_in_cooldown(metric):
            log.info(f"Skipping alert for {metric} - in cooldown period")
            return

        log.warning(f"Issue detected: {metric} changed by {issue['change_pct']:.1f}%")

        # 1. Gather related context
        context = await self._gather_diagnostic_context(issue)

        # 2. Use LLM to diagnose
        diagnosis = None
        if self.llm_service.is_available():
            diagnosis = await self._llm_diagnose_issue(issue, context)

        # 3. Send alert (cooldown is now tracked via persisted alert in database)
        await self._send_diagnostic_alert(issue, context, diagnosis)

    async def _llm_diagnose_issue(self, issue: Dict, context: Dict) -> str:
        """
        Use LLM to diagnose why a metric changed and what to do
        """
        metric = issue['metric']
        change_pct = issue['change_pct']

        prompt = f"""You are diagnosing a significant drop in {metric} for an e-commerce business.

ISSUE:
- {metric} dropped {abs(change_pct):.1f}%
- Current: {issue['current']:.2f}
- Baseline (7-day avg): {issue['baseline']:.2f}

RELATED DATA:
{self._format_context_for_llm(context)}

Provide a diagnosis in this format:

**Most Likely Cause:**
[One specific, testable hypothesis]

**Supporting Evidence:**
- [Data point 1 that supports this]
- [Data point 2 that supports this]

**What To Test First:**
1. [Specific action to verify/fix]
2. [Second action]
3. [Third action]

**Expected Timeline:**
[How long to see results]

Be specific and actionable. Focus on the most likely root cause based on the data.
"""

        try:
            response = self.llm_service.client.messages.create(
                model=settings.llm_model,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            return response.content[0].text

        except Exception as e:
            log.error(f"LLM diagnosis error: {str(e)}")
            return None

    async def _gather_diagnostic_context(self, issue: Dict) -> Dict:
        """
        Gather all related data for diagnosis
        Example: If ROAS is down, pull CTR, CPC, conversion rate, audience data, etc.
        """
        metric = issue['metric']
        context = {}

        # Get related metrics based on what's being monitored
        if metric == 'revenue':
            context['traffic'] = await self._get_metric_trend('traffic', days=7)
            context['conversion_rate'] = await self._get_metric_trend('conversion_rate', days=7)
            context['avg_order_value'] = await self._get_metric_trend('avg_order_value', days=7)
            context['top_products'] = await self._get_top_products_change()
            context['traffic_sources'] = await self._get_traffic_source_breakdown()

        elif metric == 'roas':
            context['ctr'] = await self._get_metric_trend('ctr', days=7)
            context['cpc'] = await self._get_metric_trend('cpc', days=7)
            context['conversion_rate'] = await self._get_metric_trend('conversion_rate', days=7)
            context['campaigns'] = await self._get_campaign_performance()
            context['audience_breakdown'] = await self._get_audience_data()
            context['creative_performance'] = await self._get_ad_creative_stats()

        elif metric == 'conversion_rate':
            context['traffic_quality'] = await self._get_traffic_quality()
            context['checkout_abandonment'] = await self._get_metric_trend('checkout_abandonment', days=7)
            context['page_load_time'] = await self._get_metric_trend('page_load_time', days=7)
            context['device_breakdown'] = await self._get_device_stats()

        elif metric == 'traffic':
            context['organic'] = await self._get_metric_trend('organic_traffic', days=7)
            context['paid'] = await self._get_metric_trend('paid_traffic', days=7)
            context['social'] = await self._get_metric_trend('social_traffic', days=7)
            context['campaigns'] = await self._get_campaign_status()
            context['rankings'] = await self._get_ranking_changes()

        elif metric == 'cart_abandonment':
            context['checkout_errors'] = await self._get_checkout_errors()
            context['shipping_costs'] = await self._get_shipping_stats()
            context['abandonment_by_stage'] = await self._get_funnel_breakdown()

        return context

    async def _detect_opportunities(self) -> List[Dict]:
        """
        Detect positive trends and opportunities
        - Products selling unusually well
        - Campaigns performing above average
        - Traffic sources driving high conversion
        - etc.
        """
        opportunities = []

        # Check for breakout products
        breakout_products = await self._find_breakout_products()
        if breakout_products:
            opportunities.append({
                'type': 'breakout_product',
                'data': breakout_products,
                'priority': 'high'
            })

        # Check for high-performing campaigns
        top_campaigns = await self._find_top_performing_campaigns()
        if top_campaigns:
            opportunities.append({
                'type': 'high_roas_campaign',
                'data': top_campaigns,
                'priority': 'medium'
            })

        # Check for traffic spikes from new sources
        traffic_spikes = await self._find_traffic_opportunities()
        if traffic_spikes:
            opportunities.append({
                'type': 'traffic_opportunity',
                'data': traffic_spikes,
                'priority': 'medium'
            })

        return opportunities

    async def _send_diagnostic_alert(self, issue: Dict, context: Dict, diagnosis: Optional[str]):
        """Send alert with full diagnostic information and persist to database"""
        metric = issue['metric']
        change_pct = issue['change_pct']

        # Build alert title and message
        title = f"⚠️ {metric.replace('_', ' ').title()} {issue['direction']} {abs(change_pct):.1f}%"

        message = f"""
ALERT: {metric.replace('_', ' ').title()} Issue Detected

Current Value: {issue['current']:.2f}
Expected: {issue['baseline']:.2f}
Change: {change_pct:+.1f}%
Severity: {issue['severity'].upper()}

"""

        if diagnosis:
            message += f"\n🤖 AI DIAGNOSIS:\n{diagnosis}\n"
        else:
            message += "\nRelated Metrics:\n"
            for key, value in list(context.items())[:5]:
                message += f"- {key}: {value}\n"

        # 1. Persist alert to database FIRST (creates audit trail)
        # Use metric's cooldown_hours as the deduplication window
        cooldown_hours = self.monitored_metrics.get(metric, {}).get('cooldown_hours', 24)
        alert_id = self._persist_alert(
            alert_type='metric_anomaly',
            severity=issue['severity'],
            title=title,
            description=message,
            metric=metric,
            issue_data=issue,
            diagnosis=diagnosis,
            max_age_hours=cooldown_hours
        )

        # 2. Send via configured channels and track delivery
        delivery_result = None

        try:
            if issue['severity'] == 'critical':
                # send_critical_alert returns dict with success, results, total_attempts, total_delay
                delivery_result = await self.alert_service.send_critical_alert(title, message, issue)
            else:
                # send_email_alert returns DeliveryResult object
                email_result = await self.alert_service.send_email_alert(title, message, issue, priority=issue['severity'])
                delivery_result = {
                    'success': email_result.success,
                    'results': {'email': {
                        'success': email_result.success,
                        'attempts': email_result.attempts,
                        'total_delay_seconds': email_result.total_delay_seconds,
                        'errors': email_result.errors[:5],
                        'final_error': email_result.final_error
                    }},
                    'total_attempts': email_result.attempts,
                    'total_delay_seconds': email_result.total_delay_seconds
                }
        except Exception as e:
            log.error(f"Alert delivery failed for {metric}: {e}")
            delivery_result = {
                'success': False,
                'results': {},
                'total_attempts': 0,
                'total_delay_seconds': 0.0,
                'error': str(e)
            }

        # 3. Update delivery status in database with full audit trail
        if alert_id and delivery_result:
            self._update_alert_delivery(alert_id, delivery_result)

    async def _alert_opportunity(self, opportunity: Dict):
        """Alert about positive opportunities with predictions"""
        opp_type = opportunity['type']
        data = opportunity['data']

        if opp_type == 'breakout_product':
            # Provide predictions, not prescriptive actions
            product = data.get('product_name', 'Unknown')
            current_rate = data.get('current_daily_sales', 0)
            baseline_rate = data.get('baseline_daily_sales', 0)
            increase_pct = ((current_rate - baseline_rate) / baseline_rate * 100) if baseline_rate > 0 else 0

            # Predict future sales
            predicted_7_day = current_rate * 7
            predicted_30_day = current_rate * 30

            title = f"💡 Breakout Product: {product} (+{increase_pct:.0f}%)"
            message = f"""
Product Performance Spike Detected

Product: {product}
Current Sales Rate: {current_rate} units/day (up from {baseline_rate})
Increase: {increase_pct:.1f}%

PREDICTIONS:
- Next 7 days: ~{predicted_7_day:.0f} units at current rate
- Next 30 days: ~{predicted_30_day:.0f} units at current rate
- Current inventory: {data.get('current_inventory', 'Unknown')} units
- Estimated stock-out: {data.get('stockout_estimate', 'Calculate from current inventory')}

MARKETING OPPORTUNITIES:
- High demand indicates strong product-market fit
- Consider featuring in email campaigns
- Good candidate for dedicated ad campaign
- Potential homepage/collection feature

Revenue opportunity: ${current_rate * data.get('unit_price', 0) * 7:,.0f} in next 7 days at current velocity
"""

        elif opp_type == 'high_roas_campaign':
            campaign = data.get('campaign_name', 'Unknown')
            roas = data.get('roas', 0)
            spend = data.get('daily_spend', 0)

            title = f"💡 High-Performing Campaign: {campaign} ({roas:.1f}x ROAS)"
            message = f"""
Exceptional Campaign Performance

Campaign: {campaign}
ROAS: {roas:.1f}x (well above average)
Daily Spend: ${spend:,.2f}
Daily Revenue: ${spend * roas:,.2f}

SCALING POTENTIAL:
If you doubled budget to ${spend * 2:,.2f}/day:
- Expected revenue: ${spend * 2 * roas:,.2f}/day
- Additional profit: ${(spend * 2 * roas) - (spend * 2):,.2f}/day
- ROI: {((roas - 1) * 100):.0f}%

Note: ROAS typically decreases with scale. Conservative estimate at 2x budget: {roas * 0.85:.1f}x ROAS
"""

        elif opp_type == 'traffic_opportunity':
            source = data.get('source', 'Unknown')
            increase = data.get('traffic_increase_pct', 0)
            conversion = data.get('conversion_rate', 0)

            title = f"💡 Traffic Source Opportunity: {source} (+{increase}%)"
            message = f"""
Organic Traffic Spike from {source}

Traffic Increase: {increase}%
Conversion Rate: {conversion:.1f}%
Current Spend: $0 (organic)

PAID EXPANSION OPPORTUNITY:
If you started paid campaigns on {source}:
- Test budget: $50-100/day recommended
- Target similar audiences converting organically
- Expected ROAS: 3-5x (if paid matches organic quality)

Estimated additional revenue: $150-500/day if paid performs at 50% of organic conversion rate
"""

        else:
            title = f"💡 Opportunity: {opp_type.replace('_', ' ').title()}"
            message = f"Detected opportunity: {opp_type}\n\nData: {opportunity['data']}"

        # 1. Persist opportunity alert to database FIRST
        alert_id = self._persist_alert(
            alert_type='opportunity',
            severity='low',  # Opportunities are positive, not urgent
            title=title,
            description=message,
            metric=opp_type,
            issue_data=opportunity
        )

        # 2. Send via email and track delivery
        delivery_result = None

        try:
            # send_email_alert returns DeliveryResult object
            email_result = await self.alert_service.send_email_alert(
                title,
                message,
                opportunity,
                priority='medium'  # Opportunities are medium priority, not low
            )
            delivery_result = {
                'success': email_result.success,
                'results': {'email': {
                    'success': email_result.success,
                    'attempts': email_result.attempts,
                    'total_delay_seconds': email_result.total_delay_seconds,
                    'errors': email_result.errors[:5],
                    'final_error': email_result.final_error
                }},
                'total_attempts': email_result.attempts,
                'total_delay_seconds': email_result.total_delay_seconds
            }
        except Exception as e:
            log.error(f"Opportunity alert delivery failed for {opp_type}: {e}")
            delivery_result = {
                'success': False,
                'results': {},
                'total_attempts': 0,
                'total_delay_seconds': 0.0,
                'error': str(e)
            }

        # 3. Update delivery status in database with full audit trail
        if alert_id and delivery_result:
            self._update_alert_delivery(alert_id, delivery_result)

    def _calculate_severity(self, metric: str, current: float, baseline: float) -> str:
        """Calculate how severe the issue is"""
        # Guard against division by zero
        if baseline == 0:
            # If baseline is 0 and current is non-zero, treat as critical
            return 'critical' if current != 0 else 'low'

        pct_change = abs((current - baseline) / baseline)

        if pct_change > 0.5:  # 50%+ change
            return 'critical'
        elif pct_change > 0.3:  # 30%+ change
            return 'high'
        elif pct_change > 0.15:  # 15%+ change
            return 'medium'
        else:
            return 'low'

    def _is_in_cooldown(self, metric: str) -> bool:
        """
        Check if we're in cooldown period for this metric.

        Queries the database for recent alerts, making cooldowns survive service restarts.
        """
        cooldown_hours = self.monitored_metrics.get(metric, {}).get('cooldown_hours', 24)
        cooldown_cutoff = datetime.utcnow() - timedelta(hours=cooldown_hours)

        db = SessionLocal()
        try:
            # Check if any alert was created for this metric within the cooldown period
            # Include all statuses - even resolved alerts count toward cooldown
            recent_alert = db.query(TrackingAlert).filter(
                TrackingAlert.alert_type == 'metric_anomaly',
                TrackingAlert.source_name == metric,
                TrackingAlert.created_at >= cooldown_cutoff
            ).first()

            if recent_alert:
                log.debug(
                    f"Cooldown active for {metric}: alert {recent_alert.id} "
                    f"created at {recent_alert.created_at}"
                )
                return True

            return False

        except Exception as e:
            log.error(f"Error checking cooldown for {metric}: {e}")
            # On error, allow the alert (fail open)
            return False
        finally:
            db.close()

    def _format_context_for_llm(self, context: Dict) -> str:
        """Format context data for LLM consumption"""
        lines = []
        for key, value in context.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)

    def _generate_alert_hash(self, alert_type: str, metric: str, issue_data: Dict) -> str:
        """
        Generate a hash for deduplication.
        Same metric + alert_type within cooldown = same alert.
        """
        # Include only stable fields in hash (not current values which change)
        hash_content = f"{alert_type}:{metric}"
        return hashlib.sha256(hash_content.encode()).hexdigest()[:32]

    def _find_active_alert(self, alert_type: str, metric: str) -> Optional[TrackingAlert]:
        """
        Check if there's already an active alert for this metric/type.
        Returns the existing alert or None.
        """
        db = SessionLocal()
        try:
            existing = db.query(TrackingAlert).filter(
                TrackingAlert.alert_type == alert_type,
                TrackingAlert.source_name == metric,
                TrackingAlert.status.in_(['active', 'acknowledged', 'investigating'])
            ).first()
            return existing
        finally:
            db.close()

    def _persist_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        description: str,
        metric: str,
        issue_data: Dict,
        diagnosis: Optional[str] = None,
        recommended_actions: Optional[List[str]] = None,
        max_age_hours: int = 24
    ) -> Optional[int]:
        """
        Persist an alert to the database.

        Args:
            max_age_hours: If an existing alert is older than this, auto-resolve it
                          and create a new one (prevents stale alerts blocking new ones)

        Returns the alert ID if created/updated, None if failed.
        """
        db = SessionLocal()
        try:
            # Generate dedup hash for efficient lookup
            dedup_hash = self._generate_alert_hash(alert_type, metric, issue_data)

            # Check for existing active alert using the dedup hash (fast index lookup)
            existing = db.query(TrackingAlert).filter(
                TrackingAlert.dedup_hash == dedup_hash,
                TrackingAlert.status.in_(['active', 'acknowledged', 'investigating'])
            ).first()

            # Fallback to alert_type + source_name lookup for backwards compatibility
            # (alerts created before dedup_hash was added)
            if not existing:
                existing = db.query(TrackingAlert).filter(
                    TrackingAlert.alert_type == alert_type,
                    TrackingAlert.source_name == metric,
                    TrackingAlert.dedup_hash.is_(None),  # Only match old alerts without hash
                    TrackingAlert.status.in_(['active', 'acknowledged', 'investigating'])
                ).first()

            if existing:
                # Check if alert is stale (older than max_age_hours)
                alert_age = datetime.utcnow() - (existing.created_at or datetime.utcnow())
                if alert_age > timedelta(hours=max_age_hours):
                    # Auto-resolve stale alert and create new one
                    existing.status = 'resolved'
                    existing.resolved_at = datetime.utcnow()
                    existing.resolution_notes = f"Auto-resolved: superseded by new alert after {max_age_hours}h"
                    log.info(f"Auto-resolved stale alert {existing.id} for {metric} (age: {alert_age})")
                    db.commit()
                    # Fall through to create new alert
                else:
                    # Update existing alert with new data
                    existing.updated_at = datetime.utcnow()
                    existing.issue_data = issue_data
                    existing.severity = severity  # May have changed
                    if diagnosis:
                        existing.probable_cause = diagnosis
                    log.debug(f"Updated existing alert {existing.id} for {metric}")
                    db.commit()
                    return existing.id

            # Create new alert
            alert = TrackingAlert(
                dedup_hash=dedup_hash,
                alert_type=alert_type,
                severity=severity,
                title=title,
                description=description,
                source_name=metric,
                affected_date=datetime.utcnow(),
                issue_data=issue_data,
                probable_cause=diagnosis,
                recommended_actions=recommended_actions,
                status='active',
                notification_sent=False,
                notification_channels=None
            )
            db.add(alert)
            db.commit()
            db.refresh(alert)
            log.info(f"Created alert {alert.id}: {title}")
            return alert.id

        except Exception as e:
            db.rollback()
            log.error(f"Failed to persist alert: {e}")
            return None
        finally:
            db.close()

    def _update_alert_delivery(
        self,
        alert_id: int,
        delivery_result: Dict
    ) -> bool:
        """
        Update alert with delivery status and attempt details.

        Args:
            alert_id: The alert ID to update
            delivery_result: Dict with keys:
                - success: bool - overall delivery success
                - results: dict - per-channel delivery details
                - total_attempts: int - total delivery attempts
                - total_delay_seconds: float - total retry delay
        """
        if not alert_id:
            return False

        db = SessionLocal()
        try:
            alert = db.query(TrackingAlert).filter(TrackingAlert.id == alert_id).first()
            if alert:
                # Basic status
                alert.notification_sent = delivery_result.get('success', False)
                alert.notification_channels = list(delivery_result.get('results', {}).keys())

                # Delivery attempt tracking (for audit trail)
                alert.delivery_attempts = delivery_result.get('total_attempts', 0)
                alert.delivery_total_delay_seconds = delivery_result.get('total_delay_seconds', 0.0)
                alert.delivery_results = delivery_result.get('results', {})

                alert.updated_at = datetime.utcnow()
                db.commit()
                log.debug(
                    f"Alert {alert_id} delivery: success={delivery_result.get('success')}, "
                    f"attempts={delivery_result.get('total_attempts')}, "
                    f"channels={list(delivery_result.get('results', {}).keys())}"
                )
                return True
            return False
        except Exception as e:
            db.rollback()
            log.error(f"Failed to update alert delivery status: {e}")
            return False
        finally:
            db.close()

    async def _get_current_metrics(self) -> Dict:
        """
        Get current metric values (last 24 hours) from database.

        Returns dict with keys: revenue, conversion_rate, roas, traffic,
        cart_abandonment, email_open_rate
        """
        db = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(hours=24)
            cutoff_date = cutoff.date()
            metrics = {}

            # 1. Revenue from Shopify orders (last 24h)
            revenue_result = db.query(func.sum(ShopifyOrder.total_price)).filter(
                ShopifyOrder.created_at >= cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar()
            metrics['revenue'] = float(revenue_result) if revenue_result else 0

            # 2. Traffic from GA4 daily totals (last 24h)
            # Use only (all)/(all) rows to avoid double-counting with per-channel aggregates
            traffic_result = db.query(
                func.sum(GA4TrafficSource.sessions).label('sessions')
            ).filter(
                GA4TrafficSource.date >= cutoff_date,
                GA4TrafficSource.session_source == '(all)',
                GA4TrafficSource.session_medium == '(all)'
            ).scalar()
            sessions = traffic_result or 0
            metrics['traffic'] = int(sessions)

            # Conversions: Use Shopify orders as source of truth
            # This ensures apples-to-apples comparison with baseline (which also uses orders)
            current_orders = db.query(func.count(ShopifyOrder.id)).filter(
                ShopifyOrder.created_at >= cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar() or 0
            metrics['conversion_rate'] = (current_orders / sessions * 100) if sessions > 0 else 0

            # 3. ROAS from Google Ads (last 24h)
            ads_result = db.query(
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue'),
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros')
            ).filter(
                GoogleAdsCampaign.date >= cutoff_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED'
            ).first()

            ad_revenue = ads_result.revenue or 0
            ad_cost_micros = ads_result.cost_micros or 0
            ad_cost = ad_cost_micros / 1_000_000 if ad_cost_micros else 0
            metrics['roas'] = (ad_revenue / ad_cost) if ad_cost > 0 else 0

            # 4. Cart abandonment rate (last 24h)
            # Count abandoned checkouts vs completed orders
            abandoned_count = db.query(func.count(AbandonedCheckout.id)).filter(
                AbandonedCheckout.created_at >= cutoff,
                AbandonedCheckout.recovered == False
            ).scalar() or 0

            completed_count = db.query(func.count(ShopifyOrder.id)).filter(
                ShopifyOrder.created_at >= cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar() or 0

            total_checkouts = abandoned_count + completed_count
            metrics['cart_abandonment'] = (abandoned_count / total_checkouts) if total_checkouts > 0 else 0

            # 5. Email open rate from Klaviyo (campaigns sent in last 24h)
            email_result = db.query(
                func.avg(KlaviyoCampaign.open_rate)
            ).filter(
                KlaviyoCampaign.send_time >= cutoff,
                KlaviyoCampaign.status == 'sent',
                KlaviyoCampaign.recipients > 0
            ).scalar()
            metrics['email_open_rate'] = float(email_result) if email_result else 0

            log.debug(f"Current metrics (24h): {metrics}")
            return metrics

        except Exception as e:
            log.error(f"Error fetching current metrics: {e}")
            # Return None values to indicate data unavailable
            return {
                'revenue': None,
                'conversion_rate': None,
                'roas': None,
                'traffic': None,
                'cart_abandonment': None,
                'email_open_rate': None
            }
        finally:
            db.close()

    async def _get_baseline_metrics(self) -> Dict:
        """
        Get baseline metric values (7-day average, excluding last 24h).

        This provides the comparison point for anomaly detection.
        Excludes the last 24h to ensure we're comparing to historical norms.
        """
        db = SessionLocal()
        try:
            # 7 days ago to 24 hours ago (excludes current period)
            end_cutoff = datetime.utcnow() - timedelta(hours=24)
            start_cutoff = datetime.utcnow() - timedelta(days=7)
            start_date = start_cutoff.date()
            end_date = end_cutoff.date()
            metrics = {}

            # 1. Average daily revenue from Shopify orders
            revenue_result = db.query(func.sum(ShopifyOrder.total_price)).filter(
                ShopifyOrder.created_at >= start_cutoff,
                ShopifyOrder.created_at < end_cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar()
            # Convert to daily average (divide by 6 days since we exclude last 24h)
            total_revenue = float(revenue_result) if revenue_result else 0
            metrics['revenue'] = total_revenue / 6 if total_revenue > 0 else 0

            # 2. Average daily traffic from GA4 daily totals
            # Use only (all)/(all) rows to avoid double-counting
            traffic_result = db.query(
                func.sum(GA4TrafficSource.sessions).label('sessions')
            ).filter(
                GA4TrafficSource.date >= start_date,
                GA4TrafficSource.date < end_date,
                GA4TrafficSource.session_source == '(all)',
                GA4TrafficSource.session_medium == '(all)'
            ).scalar()
            sessions = traffic_result or 0
            # Daily average traffic
            metrics['traffic'] = int(sessions / 6) if sessions > 0 else 0

            # Conversions: Use Shopify orders as source of truth for historical conversions
            # GA4 aggregated rows only have conversion data for today, so we use
            # actual order counts which are reliable across the historical period
            baseline_orders = db.query(func.count(ShopifyOrder.id)).filter(
                ShopifyOrder.created_at >= start_cutoff,
                ShopifyOrder.created_at < end_cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar() or 0
            metrics['conversion_rate'] = (baseline_orders / sessions * 100) if sessions > 0 else 0

            # 3. Average ROAS from Google Ads
            ads_result = db.query(
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue'),
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros')
            ).filter(
                GoogleAdsCampaign.date >= start_date,
                GoogleAdsCampaign.date < end_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED'
            ).first()

            ad_revenue = ads_result.revenue or 0
            ad_cost_micros = ads_result.cost_micros or 0
            ad_cost = ad_cost_micros / 1_000_000 if ad_cost_micros else 0
            metrics['roas'] = (ad_revenue / ad_cost) if ad_cost > 0 else 0

            # 4. Average cart abandonment rate
            abandoned_count = db.query(func.count(AbandonedCheckout.id)).filter(
                AbandonedCheckout.created_at >= start_cutoff,
                AbandonedCheckout.created_at < end_cutoff,
                AbandonedCheckout.recovered == False
            ).scalar() or 0

            completed_count = db.query(func.count(ShopifyOrder.id)).filter(
                ShopifyOrder.created_at >= start_cutoff,
                ShopifyOrder.created_at < end_cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar() or 0

            total_checkouts = abandoned_count + completed_count
            metrics['cart_abandonment'] = (abandoned_count / total_checkouts) if total_checkouts > 0 else 0

            # 5. Average email open rate from Klaviyo
            email_result = db.query(
                func.avg(KlaviyoCampaign.open_rate)
            ).filter(
                KlaviyoCampaign.send_time >= start_cutoff,
                KlaviyoCampaign.send_time < end_cutoff,
                KlaviyoCampaign.status == 'sent',
                KlaviyoCampaign.recipients > 0
            ).scalar()
            metrics['email_open_rate'] = float(email_result) if email_result else 0

            log.debug(f"Baseline metrics (7-day avg): {metrics}")
            return metrics

        except Exception as e:
            log.error(f"Error fetching baseline metrics: {e}")
            return {
                'revenue': None,
                'conversion_rate': None,
                'roas': None,
                'traffic': None,
                'cart_abandonment': None,
                'email_open_rate': None
            }
        finally:
            db.close()

    async def _check_disapproved_ads(self) -> int:
        """
        Check for potentially disapproved Google Ads.

        Since the data model doesn't track ad-level approval status,
        we check for ENABLED campaigns with 0 impressions in the last 24h
        as a proxy for potential disapproval issues.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(hours=24)).date()

            # Find ENABLED campaigns with 0 impressions (possible disapproval)
            problematic_campaigns = db.query(func.count(GoogleAdsCampaign.id)).filter(
                GoogleAdsCampaign.date >= cutoff_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED',
                GoogleAdsCampaign.impressions == 0
            ).scalar() or 0

            if problematic_campaigns > 0:
                log.warning(f"Found {problematic_campaigns} enabled campaigns with 0 impressions")

            return problematic_campaigns

        except Exception as e:
            log.error(f"Error checking disapproved ads: {e}")
            return 0
        finally:
            db.close()

    async def _get_metric_trend(self, metric: str, days: int) -> List[Dict]:
        """
        Get metric trend over time (daily values).

        Returns list of {date, value} dicts for the specified metric.
        """
        db = SessionLocal()
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days)
            trend = []

            if metric == 'traffic':
                # Daily traffic from GA4
                results = db.query(
                    GA4TrafficSource.date,
                    func.sum(GA4TrafficSource.sessions).label('value')
                ).filter(
                    GA4TrafficSource.date >= start_date.date(),
                    GA4TrafficSource.session_source == '(all)',
                    GA4TrafficSource.session_medium == '(all)'
                ).group_by(GA4TrafficSource.date).order_by(GA4TrafficSource.date).all()

                for row in results:
                    trend.append({'date': row.date.isoformat(), 'value': row.value or 0})

            elif metric == 'revenue':
                # Daily revenue from Shopify orders
                # Use current_total_price (post-refund) with fallback to total_price for old records
                from sqlalchemy import cast, Date, func as sqla_func
                results = db.query(
                    cast(ShopifyOrder.created_at, Date).label('date'),
                    func.sum(
                        sqla_func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)
                    ).label('value')
                ).filter(
                    ShopifyOrder.created_at >= start_date,
                    ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
                ).group_by(cast(ShopifyOrder.created_at, Date)).order_by(cast(ShopifyOrder.created_at, Date)).all()

                for row in results:
                    trend.append({'date': row.date.isoformat(), 'value': float(row.value or 0)})

            elif metric == 'conversion_rate':
                # Need both sessions and orders per day
                from sqlalchemy import cast, Date

                # Get daily sessions
                sessions_by_day = {}
                session_results = db.query(
                    GA4TrafficSource.date,
                    func.sum(GA4TrafficSource.sessions).label('sessions')
                ).filter(
                    GA4TrafficSource.date >= start_date.date(),
                    GA4TrafficSource.session_source == '(all)',
                    GA4TrafficSource.session_medium == '(all)'
                ).group_by(GA4TrafficSource.date).all()

                for row in session_results:
                    sessions_by_day[row.date] = row.sessions or 0

                # Get daily orders
                order_results = db.query(
                    cast(ShopifyOrder.created_at, Date).label('date'),
                    func.count(ShopifyOrder.id).label('orders')
                ).filter(
                    ShopifyOrder.created_at >= start_date,
                    ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
                ).group_by(cast(ShopifyOrder.created_at, Date)).all()

                for row in order_results:
                    sessions = sessions_by_day.get(row.date, 0)
                    rate = (row.orders / sessions * 100) if sessions > 0 else 0
                    trend.append({'date': row.date.isoformat(), 'value': round(rate, 2)})

            elif metric in ['avg_order_value', 'aov']:
                # Average order value per day
                # Use current_total_price (post-refund) with fallback to total_price for old records
                from sqlalchemy import cast, Date, func as sqla_func
                results = db.query(
                    cast(ShopifyOrder.created_at, Date).label('date'),
                    func.avg(
                        sqla_func.coalesce(ShopifyOrder.current_total_price, ShopifyOrder.total_price)
                    ).label('value')
                ).filter(
                    ShopifyOrder.created_at >= start_date,
                    ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
                ).group_by(cast(ShopifyOrder.created_at, Date)).order_by(cast(ShopifyOrder.created_at, Date)).all()

                for row in results:
                    trend.append({'date': row.date.isoformat(), 'value': float(row.value or 0)})

            elif metric in ['organic_traffic', 'paid_traffic', 'social_traffic']:
                # Traffic by medium type
                medium_map = {
                    'organic_traffic': 'organic',
                    'paid_traffic': 'cpc',
                    'social_traffic': 'social'
                }
                target_medium = medium_map.get(metric, 'organic')

                results = db.query(
                    GA4TrafficSource.date,
                    func.sum(GA4TrafficSource.sessions).label('value')
                ).filter(
                    GA4TrafficSource.date >= start_date.date(),
                    GA4TrafficSource.session_medium == target_medium
                ).group_by(GA4TrafficSource.date).order_by(GA4TrafficSource.date).all()

                for row in results:
                    trend.append({'date': row.date.isoformat(), 'value': row.value or 0})

            elif metric == 'checkout_abandonment':
                # Daily checkout abandonment rate
                from sqlalchemy import cast, Date

                # Get daily abandoned checkouts
                abandoned = db.query(
                    cast(AbandonedCheckout.created_at, Date).label('date'),
                    func.count(AbandonedCheckout.id).label('abandoned')
                ).filter(
                    AbandonedCheckout.created_at >= start_date,
                    AbandonedCheckout.recovered == False
                ).group_by(cast(AbandonedCheckout.created_at, Date)).all()

                # Get daily completed orders
                completed = db.query(
                    cast(ShopifyOrder.created_at, Date).label('date'),
                    func.count(ShopifyOrder.id).label('completed')
                ).filter(
                    ShopifyOrder.created_at >= start_date,
                    ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
                ).group_by(cast(ShopifyOrder.created_at, Date)).all()

                completed_by_day = {row.date: row.completed for row in completed}

                for row in abandoned:
                    total = row.abandoned + completed_by_day.get(row.date, 0)
                    rate = (row.abandoned / total * 100) if total > 0 else 0
                    trend.append({'date': row.date.isoformat(), 'value': round(rate, 1)})

            elif metric == 'ctr':
                # Daily CTR from Google Ads
                results = db.query(
                    GoogleAdsCampaign.date,
                    func.sum(GoogleAdsCampaign.clicks).label('clicks'),
                    func.sum(GoogleAdsCampaign.impressions).label('impressions')
                ).filter(
                    GoogleAdsCampaign.date >= start_date.date(),
                    GoogleAdsCampaign.campaign_status == 'ENABLED'
                ).group_by(GoogleAdsCampaign.date).order_by(GoogleAdsCampaign.date).all()

                for row in results:
                    ctr = (row.clicks / row.impressions * 100) if row.impressions > 0 else 0
                    trend.append({'date': row.date.isoformat(), 'value': round(ctr, 2)})

            elif metric == 'cpc':
                # Daily CPC from Google Ads
                results = db.query(
                    GoogleAdsCampaign.date,
                    func.sum(GoogleAdsCampaign.clicks).label('clicks'),
                    func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros')
                ).filter(
                    GoogleAdsCampaign.date >= start_date.date(),
                    GoogleAdsCampaign.campaign_status == 'ENABLED'
                ).group_by(GoogleAdsCampaign.date).order_by(GoogleAdsCampaign.date).all()

                for row in results:
                    cost = (row.cost_micros or 0) / 1_000_000
                    cpc = (cost / row.clicks) if row.clicks > 0 else 0
                    trend.append({'date': row.date.isoformat(), 'value': round(cpc, 2)})

            return trend

        except Exception as e:
            log.error(f"Error getting metric trend for {metric}: {e}")
            return []
        finally:
            db.close()

    async def _get_top_products_change(self) -> Dict:
        """
        Get change in top product sales: current period vs previous period.

        Returns dict with top products and their sales change.
        """
        db = SessionLocal()
        try:
            now = datetime.utcnow()
            current_start = now - timedelta(days=7)
            previous_start = now - timedelta(days=14)

            # Helper to extract product sales from line_items JSON
            # Since line_items is stored as JSON, we need to process in Python
            current_orders = db.query(ShopifyOrder.line_items).filter(
                ShopifyOrder.created_at >= current_start,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).all()

            previous_orders = db.query(ShopifyOrder.line_items).filter(
                ShopifyOrder.created_at >= previous_start,
                ShopifyOrder.created_at < current_start,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).all()

            # Aggregate sales by product
            def aggregate_products(orders):
                products = {}
                for (line_items,) in orders:
                    if not line_items:
                        continue
                    for item in line_items:
                        product_id = str(item.get('product_id', 'unknown'))
                        title = item.get('title', 'Unknown Product')
                        quantity = item.get('quantity', 0)
                        price = float(item.get('price', 0))

                        if product_id not in products:
                            products[product_id] = {
                                'title': title,
                                'units': 0,
                                'revenue': 0
                            }
                        products[product_id]['units'] += quantity
                        products[product_id]['revenue'] += quantity * price
                return products

            current_products = aggregate_products(current_orders)
            previous_products = aggregate_products(previous_orders)

            # Calculate changes for top current products
            changes = {}
            for product_id, current_data in sorted(
                current_products.items(),
                key=lambda x: x[1]['revenue'],
                reverse=True
            )[:10]:
                prev_data = previous_products.get(product_id, {'units': 0, 'revenue': 0})
                prev_revenue = prev_data['revenue']
                change_pct = ((current_data['revenue'] - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 100

                changes[current_data['title']] = {
                    'current_revenue': round(current_data['revenue'], 2),
                    'previous_revenue': round(prev_revenue, 2),
                    'change_pct': round(change_pct, 1),
                    'current_units': current_data['units'],
                    'previous_units': prev_data['units']
                }

            return changes

        except Exception as e:
            log.error(f"Error getting top products change: {e}")
            return {}
        finally:
            db.close()

    async def _get_traffic_source_breakdown(self) -> Dict:
        """Get traffic breakdown by source/medium for last 24h"""
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(hours=24)).date()

            results = db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.conversions).label('conversions')
            ).filter(
                GA4TrafficSource.date >= cutoff_date
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).order_by(func.sum(GA4TrafficSource.sessions).desc()).limit(10).all()

            breakdown = {}
            for row in results:
                source_medium = f"{row.session_source or 'direct'}/{row.session_medium or '(none)'}"
                sessions = row.sessions or 0
                conversions = row.conversions or 0
                breakdown[source_medium] = {
                    'sessions': sessions,
                    'conversions': conversions,
                    'conversion_rate': (conversions / sessions * 100) if sessions > 0 else 0
                }

            return breakdown

        except Exception as e:
            log.error(f"Error getting traffic source breakdown: {e}")
            return {}
        finally:
            db.close()

    async def _get_campaign_performance(self) -> List[Dict]:
        """Get Google Ads campaign performance for last 24h"""
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(hours=24)).date()

            results = db.query(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type,
                func.sum(GoogleAdsCampaign.impressions).label('impressions'),
                func.sum(GoogleAdsCampaign.clicks).label('clicks'),
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue'),
                func.sum(GoogleAdsCampaign.conversions).label('conversions')
            ).filter(
                GoogleAdsCampaign.date >= cutoff_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED'
            ).group_by(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type
            ).order_by(func.sum(GoogleAdsCampaign.cost_micros).desc()).limit(10).all()

            campaigns = []
            for row in results:
                cost = (row.cost_micros or 0) / 1_000_000
                revenue = row.revenue or 0
                clicks = row.clicks or 0
                impressions = row.impressions or 0

                campaigns.append({
                    'campaign_name': row.campaign_name,
                    'campaign_type': row.campaign_type,
                    'impressions': impressions,
                    'clicks': clicks,
                    'cost': round(cost, 2),
                    'revenue': round(revenue, 2),
                    'roas': round(revenue / cost, 2) if cost > 0 else 0,
                    'ctr': round(clicks / impressions * 100, 2) if impressions > 0 else 0,
                    'conversions': row.conversions or 0
                })

            return campaigns

        except Exception as e:
            log.error(f"Error getting campaign performance: {e}")
            return []
        finally:
            db.close()

    async def _get_audience_data(self) -> Dict:
        """
        Get audience breakdown by source/medium with engagement metrics.

        Returns audience segments with their conversion rates.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=7)).date()

            results = db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.total_users).label('users'),
                func.sum(GA4TrafficSource.new_users).label('new_users'),
                func.sum(GA4TrafficSource.engaged_sessions).label('engaged_sessions'),
                func.sum(GA4TrafficSource.conversions).label('conversions'),
                func.avg(GA4TrafficSource.bounce_rate).label('avg_bounce_rate')
            ).filter(
                GA4TrafficSource.date >= cutoff_date
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).order_by(func.sum(GA4TrafficSource.sessions).desc()).limit(15).all()

            audience = {}
            for row in results:
                source = row.session_source or 'direct'
                medium = row.session_medium or '(none)'
                key = f"{source}/{medium}"

                sessions = row.sessions or 0
                conversions = row.conversions or 0
                engaged = row.engaged_sessions or 0

                audience[key] = {
                    'sessions': sessions,
                    'users': row.users or 0,
                    'new_users': row.new_users or 0,
                    'conversion_rate': round(conversions / sessions * 100, 2) if sessions > 0 else 0,
                    'engagement_rate': round(engaged / sessions * 100, 2) if sessions > 0 else 0,
                    'bounce_rate': round(row.avg_bounce_rate or 0, 2)
                }

            return audience

        except Exception as e:
            log.error(f"Error getting audience data: {e}")
            return {}
        finally:
            db.close()

    async def _get_ad_creative_stats(self) -> Dict:
        """
        Get ad performance by campaign type (as a proxy for creative groupings).

        Note: The data model stores campaign-level data, not ad/creative level.
        This returns performance grouped by campaign_type as a useful proxy.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=7)).date()

            results = db.query(
                GoogleAdsCampaign.campaign_type,
                func.sum(GoogleAdsCampaign.impressions).label('impressions'),
                func.sum(GoogleAdsCampaign.clicks).label('clicks'),
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
                func.sum(GoogleAdsCampaign.conversions).label('conversions'),
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue')
            ).filter(
                GoogleAdsCampaign.date >= cutoff_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED'
            ).group_by(GoogleAdsCampaign.campaign_type).all()

            stats = {}
            for row in results:
                campaign_type = row.campaign_type or 'Unknown'
                impressions = row.impressions or 0
                clicks = row.clicks or 0
                cost = (row.cost_micros or 0) / 1_000_000
                revenue = row.revenue or 0

                stats[campaign_type] = {
                    'impressions': impressions,
                    'clicks': clicks,
                    'ctr': round(clicks / impressions * 100, 2) if impressions > 0 else 0,
                    'cost': round(cost, 2),
                    'cpc': round(cost / clicks, 2) if clicks > 0 else 0,
                    'conversions': row.conversions or 0,
                    'revenue': round(revenue, 2),
                    'roas': round(revenue / cost, 2) if cost > 0 else 0
                }

            return stats

        except Exception as e:
            log.error(f"Error getting ad creative stats: {e}")
            return {}
        finally:
            db.close()

    async def _get_traffic_quality(self) -> Dict:
        """
        Get traffic quality metrics by source/medium.

        Quality indicators: engagement rate, bounce rate, conversion rate.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=7)).date()

            results = db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.engaged_sessions).label('engaged'),
                func.sum(GA4TrafficSource.conversions).label('conversions'),
                func.avg(GA4TrafficSource.bounce_rate).label('bounce_rate'),
                func.avg(GA4TrafficSource.avg_session_duration).label('avg_duration')
            ).filter(
                GA4TrafficSource.date >= cutoff_date,
                GA4TrafficSource.sessions > 0  # Only sources with traffic
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).order_by(func.sum(GA4TrafficSource.sessions).desc()).limit(10).all()

            quality = {}
            for row in results:
                source = row.session_source or 'direct'
                medium = row.session_medium or '(none)'
                key = f"{source}/{medium}"

                sessions = row.sessions or 0
                engaged = row.engaged or 0
                conversions = row.conversions or 0

                engagement_rate = (engaged / sessions * 100) if sessions > 0 else 0
                conversion_rate = (conversions / sessions * 100) if sessions > 0 else 0

                # Quality score: weighted combination of engagement and conversion
                # Higher is better (0-100 scale)
                quality_score = min(100, (engagement_rate * 0.5) + (conversion_rate * 5))

                quality[key] = {
                    'sessions': sessions,
                    'engagement_rate': round(engagement_rate, 1),
                    'bounce_rate': round(row.bounce_rate or 0, 1),
                    'conversion_rate': round(conversion_rate, 2),
                    'avg_duration_seconds': round(row.avg_duration or 0, 0),
                    'quality_score': round(quality_score, 1)
                }

            return quality

        except Exception as e:
            log.error(f"Error getting traffic quality: {e}")
            return {}
        finally:
            db.close()

    async def _get_device_stats(self) -> Dict:
        """
        Get traffic breakdown by device type from Search Console data.

        Note: GA4 model doesn't have device dimension, so we use Search Console
        which tracks device (DESKTOP, MOBILE, TABLET).
        """
        db = SessionLocal()
        try:
            from app.models.search_console_data import SearchConsoleQuery

            cutoff_date = (datetime.utcnow() - timedelta(days=7)).date()

            results = db.query(
                SearchConsoleQuery.device,
                func.sum(SearchConsoleQuery.clicks).label('clicks'),
                func.sum(SearchConsoleQuery.impressions).label('impressions'),
                func.avg(SearchConsoleQuery.ctr).label('avg_ctr'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= cutoff_date,
                SearchConsoleQuery.device.isnot(None)
            ).group_by(SearchConsoleQuery.device).all()

            devices = {}
            total_clicks = sum(r.clicks or 0 for r in results)

            for row in results:
                device = row.device or 'Unknown'
                clicks = row.clicks or 0
                impressions = row.impressions or 0

                devices[device] = {
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': round((row.avg_ctr or 0) * 100, 2),  # Convert to percentage
                    'avg_position': round(row.avg_position or 0, 1),
                    'share_pct': round(clicks / total_clicks * 100, 1) if total_clicks > 0 else 0
                }

            return devices

        except Exception as e:
            log.error(f"Error getting device stats: {e}")
            return {}
        finally:
            db.close()

    async def _get_campaign_status(self) -> List[Dict]:
        """
        Get current status of all active campaigns.

        Returns list of campaigns with their performance status.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=1)).date()

            results = db.query(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type,
                GoogleAdsCampaign.campaign_status,
                func.sum(GoogleAdsCampaign.impressions).label('impressions'),
                func.sum(GoogleAdsCampaign.clicks).label('clicks'),
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue')
            ).filter(
                GoogleAdsCampaign.date >= cutoff_date
            ).group_by(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type,
                GoogleAdsCampaign.campaign_status
            ).order_by(func.sum(GoogleAdsCampaign.cost_micros).desc()).limit(20).all()

            campaigns = []
            for row in results:
                cost = (row.cost_micros or 0) / 1_000_000
                revenue = row.revenue or 0
                impressions = row.impressions or 0

                # Determine health status
                if row.campaign_status != 'ENABLED':
                    health = 'paused'
                elif impressions == 0:
                    health = 'no_delivery'
                elif cost > 0 and revenue / cost < 1:
                    health = 'underperforming'
                elif cost > 0 and revenue / cost >= 3:
                    health = 'excellent'
                else:
                    health = 'healthy'

                campaigns.append({
                    'name': row.campaign_name,
                    'type': row.campaign_type,
                    'status': row.campaign_status,
                    'health': health,
                    'impressions': impressions,
                    'clicks': row.clicks or 0,
                    'cost': round(cost, 2),
                    'revenue': round(revenue, 2),
                    'roas': round(revenue / cost, 2) if cost > 0 else 0
                })

            return campaigns

        except Exception as e:
            log.error(f"Error getting campaign status: {e}")
            return []
        finally:
            db.close()

    async def _get_ranking_changes(self) -> Dict:
        """
        Get SEO ranking changes for top queries.

        Compares current week's average position to previous week.
        """
        db = SessionLocal()
        try:
            from app.models.search_console_data import SearchConsoleQuery

            now = datetime.utcnow()
            current_start = (now - timedelta(days=7)).date()
            previous_start = (now - timedelta(days=14)).date()

            # Current period positions
            current_results = db.query(
                SearchConsoleQuery.query,
                func.avg(SearchConsoleQuery.position).label('avg_position'),
                func.sum(SearchConsoleQuery.clicks).label('clicks'),
                func.sum(SearchConsoleQuery.impressions).label('impressions')
            ).filter(
                SearchConsoleQuery.date >= current_start
            ).group_by(SearchConsoleQuery.query).having(
                func.sum(SearchConsoleQuery.impressions) >= 100  # Filter for significant queries
            ).order_by(func.sum(SearchConsoleQuery.clicks).desc()).limit(20).all()

            # Previous period positions
            previous_results = db.query(
                SearchConsoleQuery.query,
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= previous_start,
                SearchConsoleQuery.date < current_start
            ).group_by(SearchConsoleQuery.query).all()

            previous_positions = {r.query: r.avg_position for r in previous_results}

            changes = {}
            for row in current_results:
                query = row.query
                current_pos = row.avg_position or 0
                previous_pos = previous_positions.get(query)

                if previous_pos is not None:
                    # Negative change = improved ranking (lower position is better)
                    position_change = current_pos - previous_pos
                    direction = 'improved' if position_change < -1 else ('declined' if position_change > 1 else 'stable')
                else:
                    position_change = 0
                    direction = 'new'

                changes[query] = {
                    'current_position': round(current_pos, 1),
                    'previous_position': round(previous_pos, 1) if previous_pos else None,
                    'change': round(position_change, 1),
                    'direction': direction,
                    'clicks': row.clicks or 0,
                    'impressions': row.impressions or 0
                }

            return changes

        except Exception as e:
            log.error(f"Error getting ranking changes: {e}")
            return {}
        finally:
            db.close()

    async def _get_checkout_errors(self) -> List[Dict]:
        """
        Analyze abandoned checkouts to identify potential issues.

        Looks at abandonment patterns by time, value, and recovery status.
        """
        db = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(days=7)

            # Get abandoned checkouts with their details
            results = db.query(AbandonedCheckout).filter(
                AbandonedCheckout.created_at >= cutoff,
                AbandonedCheckout.recovered == False
            ).order_by(AbandonedCheckout.total_price.desc()).limit(100).all()

            # Analyze patterns
            total_abandoned = len(results)
            total_value = sum(float(r.total_price or 0) for r in results)

            # Group by value ranges
            value_ranges = {
                'under_50': 0,
                '50_to_100': 0,
                '100_to_200': 0,
                '200_to_500': 0,
                'over_500': 0
            }

            for checkout in results:
                value = float(checkout.total_price or 0)
                if value < 50:
                    value_ranges['under_50'] += 1
                elif value < 100:
                    value_ranges['50_to_100'] += 1
                elif value < 200:
                    value_ranges['100_to_200'] += 1
                elif value < 500:
                    value_ranges['200_to_500'] += 1
                else:
                    value_ranges['over_500'] += 1

            # Identify high-value abandoned checkouts (potential issues)
            high_value_abandoned = [
                {
                    'email': r.customer_email[:3] + '***' if r.customer_email else 'anonymous',
                    'value': float(r.total_price or 0),
                    'created_at': r.created_at.isoformat() if r.created_at else None,
                    'items_count': len(r.line_items) if r.line_items else 0
                }
                for r in results[:10]  # Top 10 by value
            ]

            return [{
                'summary': {
                    'total_abandoned': total_abandoned,
                    'total_value_lost': round(total_value, 2),
                    'avg_abandoned_value': round(total_value / total_abandoned, 2) if total_abandoned > 0 else 0
                },
                'by_value_range': value_ranges,
                'high_value_abandoned': high_value_abandoned
            }]

        except Exception as e:
            log.error(f"Error getting checkout errors: {e}")
            return []
        finally:
            db.close()

    async def _get_shipping_stats(self) -> Dict:
        """
        Get shipping-related statistics from orders.

        Analyzes shipping costs and geographic distribution.
        """
        db = SessionLocal()
        try:
            cutoff = datetime.utcnow() - timedelta(days=7)

            # Get recent orders with shipping data
            results = db.query(
                ShopifyOrder.shipping_country,
                ShopifyOrder.shipping_province,
                func.count(ShopifyOrder.id).label('orders'),
                func.sum(ShopifyOrder.total_shipping).label('total_shipping'),
                func.sum(ShopifyOrder.total_price).label('total_revenue'),
                func.avg(ShopifyOrder.total_shipping).label('avg_shipping')
            ).filter(
                ShopifyOrder.created_at >= cutoff,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).group_by(
                ShopifyOrder.shipping_country,
                ShopifyOrder.shipping_province
            ).order_by(func.count(ShopifyOrder.id).desc()).limit(20).all()

            # Summary stats
            total_orders = sum(r.orders for r in results)
            total_shipping_cost = sum(float(r.total_shipping or 0) for r in results)
            total_revenue = sum(float(r.total_revenue or 0) for r in results)

            # By region
            by_region = {}
            for row in results:
                region = f"{row.shipping_country or 'Unknown'}/{row.shipping_province or 'Unknown'}"
                by_region[region] = {
                    'orders': row.orders,
                    'total_shipping': round(float(row.total_shipping or 0), 2),
                    'avg_shipping': round(float(row.avg_shipping or 0), 2),
                    'total_revenue': round(float(row.total_revenue or 0), 2)
                }

            return {
                'summary': {
                    'total_orders': total_orders,
                    'total_shipping_cost': round(total_shipping_cost, 2),
                    'avg_shipping_per_order': round(total_shipping_cost / total_orders, 2) if total_orders > 0 else 0,
                    'shipping_as_pct_of_revenue': round(total_shipping_cost / total_revenue * 100, 1) if total_revenue > 0 else 0
                },
                'by_region': by_region
            }

        except Exception as e:
            log.error(f"Error getting shipping stats: {e}")
            return {}
        finally:
            db.close()

    async def _get_funnel_breakdown(self) -> Dict:
        """
        Get checkout funnel breakdown.

        Analyzes: sessions → add to cart → checkout initiated → purchase
        """
        db = SessionLocal()
        try:
            from app.models.ga4_data import GA4ProductPerformance

            cutoff_date = (datetime.utcnow() - timedelta(days=7)).date()

            # Get sessions from GA4 traffic
            sessions_result = db.query(
                func.sum(GA4TrafficSource.sessions)
            ).filter(
                GA4TrafficSource.date >= cutoff_date,
                GA4TrafficSource.session_source == '(all)',
                GA4TrafficSource.session_medium == '(all)'
            ).scalar() or 0

            # Get product funnel metrics from GA4
            product_results = db.query(
                func.sum(GA4ProductPerformance.items_viewed).label('views'),
                func.sum(GA4ProductPerformance.items_added_to_cart).label('add_to_cart'),
                func.sum(GA4ProductPerformance.items_purchased).label('purchased')
            ).filter(
                GA4ProductPerformance.date >= cutoff_date
            ).first()

            # Get abandoned checkouts (initiated but not completed)
            checkouts_initiated = db.query(func.count(AbandonedCheckout.id)).filter(
                AbandonedCheckout.created_at >= datetime.utcnow() - timedelta(days=7)
            ).scalar() or 0

            # Get completed orders
            completed_orders = db.query(func.count(ShopifyOrder.id)).filter(
                ShopifyOrder.created_at >= datetime.utcnow() - timedelta(days=7),
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).scalar() or 0

            views = product_results.views or 0 if product_results else 0
            add_to_cart = product_results.add_to_cart or 0 if product_results else 0
            total_checkouts = checkouts_initiated + completed_orders

            funnel = {
                'sessions': sessions_result,
                'product_views': views,
                'add_to_cart': add_to_cart,
                'checkouts_initiated': total_checkouts,
                'purchases': completed_orders,
                'conversion_rates': {
                    'view_to_cart': round(add_to_cart / views * 100, 2) if views > 0 else 0,
                    'cart_to_checkout': round(total_checkouts / add_to_cart * 100, 2) if add_to_cart > 0 else 0,
                    'checkout_to_purchase': round(completed_orders / total_checkouts * 100, 2) if total_checkouts > 0 else 0,
                    'overall': round(completed_orders / sessions_result * 100, 2) if sessions_result > 0 else 0
                },
                'drop_off': {
                    'view_to_cart': views - add_to_cart if views > add_to_cart else 0,
                    'cart_to_checkout': add_to_cart - total_checkouts if add_to_cart > total_checkouts else 0,
                    'checkout_abandoned': checkouts_initiated
                }
            }

            return funnel

        except Exception as e:
            log.error(f"Error getting funnel breakdown: {e}")
            return {}
        finally:
            db.close()

    async def _find_breakout_products(self) -> Optional[Dict]:
        """
        Find products selling unusually well compared to their baseline.

        A "breakout" product has sales velocity 2x+ above its historical average.
        Returns the top breakout product or None if none found.
        """
        db = SessionLocal()
        try:
            now = datetime.utcnow()
            current_start = now - timedelta(days=3)  # Last 3 days
            baseline_start = now - timedelta(days=10)  # 7 days before that

            # Get current period sales by product (from line items)
            current_orders = db.query(ShopifyOrder.line_items).filter(
                ShopifyOrder.created_at >= current_start,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).all()

            # Get baseline period sales
            baseline_orders = db.query(ShopifyOrder.line_items).filter(
                ShopifyOrder.created_at >= baseline_start,
                ShopifyOrder.created_at < current_start,
                ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
            ).all()

            # Aggregate by product
            def aggregate_sales(orders, days):
                products = {}
                for (line_items,) in orders:
                    if not line_items:
                        continue
                    for item in line_items:
                        product_id = str(item.get('product_id', 'unknown'))
                        title = item.get('title', 'Unknown')
                        quantity = item.get('quantity', 0)
                        price = float(item.get('price', 0))

                        if product_id not in products:
                            products[product_id] = {'title': title, 'units': 0, 'revenue': 0}
                        products[product_id]['units'] += quantity
                        products[product_id]['revenue'] += quantity * price

                # Normalize to daily rate
                for pid in products:
                    products[pid]['daily_units'] = products[pid]['units'] / days
                    products[pid]['daily_revenue'] = products[pid]['revenue'] / days

                return products

            current_products = aggregate_sales(current_orders, 3)
            baseline_products = aggregate_sales(baseline_orders, 7)

            # Find breakouts: products with 2x+ velocity increase
            breakouts = []
            for product_id, current_data in current_products.items():
                baseline_data = baseline_products.get(product_id)

                if baseline_data and baseline_data['daily_units'] > 0:
                    velocity_ratio = current_data['daily_units'] / baseline_data['daily_units']

                    if velocity_ratio >= 2.0 and current_data['units'] >= 3:  # 2x+ and at least 3 sales
                        breakouts.append({
                            'product_id': product_id,
                            'product_name': current_data['title'],
                            'current_daily_sales': round(current_data['daily_units'], 1),
                            'baseline_daily_sales': round(baseline_data['daily_units'], 1),
                            'velocity_increase': round(velocity_ratio, 1),
                            'unit_price': round(current_data['revenue'] / current_data['units'], 2) if current_data['units'] > 0 else 0,
                            'total_units_sold': current_data['units'],
                            'total_revenue': round(current_data['revenue'], 2)
                        })

            # Return top breakout by velocity increase
            if breakouts:
                breakouts.sort(key=lambda x: x['velocity_increase'], reverse=True)
                return breakouts[0]

            return None

        except Exception as e:
            log.error(f"Error finding breakout products: {e}")
            return None
        finally:
            db.close()

    async def _find_top_performing_campaigns(self) -> Optional[Dict]:
        """
        Find campaigns with exceptional ROAS (>5x) that could be scaled.

        Returns the top performing campaign data or None if none found.
        """
        db = SessionLocal()
        try:
            cutoff_date = (datetime.utcnow() - timedelta(hours=24)).date()
            min_spend_micros = 10_000_000  # $10 minimum spend to qualify

            results = db.query(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type,
                func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
                func.sum(GoogleAdsCampaign.conversions_value).label('revenue')
            ).filter(
                GoogleAdsCampaign.date >= cutoff_date,
                GoogleAdsCampaign.campaign_status == 'ENABLED'
            ).group_by(
                GoogleAdsCampaign.campaign_name,
                GoogleAdsCampaign.campaign_type
            ).having(
                func.sum(GoogleAdsCampaign.cost_micros) >= min_spend_micros
            ).all()

            # Find campaigns with ROAS > 5x
            for row in results:
                cost = (row.cost_micros or 0) / 1_000_000
                revenue = row.revenue or 0

                if cost > 0:
                    roas = revenue / cost
                    if roas >= 5.0:  # Exceptional ROAS threshold
                        return {
                            'campaign_name': row.campaign_name,
                            'campaign_type': row.campaign_type,
                            'roas': round(roas, 2),
                            'daily_spend': round(cost, 2),
                            'daily_revenue': round(revenue, 2)
                        }

            return None

        except Exception as e:
            log.error(f"Error finding top performing campaigns: {e}")
            return None
        finally:
            db.close()

    async def _find_traffic_opportunities(self) -> Optional[Dict]:
        """
        Find traffic sources with unusual growth that could be scaled.

        Identifies organic traffic spikes that might indicate:
        - Viral content
        - New referral sources
        - SEO wins
        """
        db = SessionLocal()
        try:
            now = datetime.utcnow()
            current_date = (now - timedelta(days=1)).date()
            previous_start = (now - timedelta(days=8)).date()

            # Get current traffic by source/medium
            current_results = db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.conversions).label('conversions')
            ).filter(
                GA4TrafficSource.date >= current_date,
                GA4TrafficSource.session_source != '(all)'  # Exclude aggregate rows
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).having(func.sum(GA4TrafficSource.sessions) >= 10).all()  # Min 10 sessions

            # Get baseline traffic
            baseline_results = db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.conversions).label('conversions')
            ).filter(
                GA4TrafficSource.date >= previous_start,
                GA4TrafficSource.date < current_date,
                GA4TrafficSource.session_source != '(all)'
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).all()

            # Build baseline lookup (daily average)
            baseline_map = {}
            for row in baseline_results:
                key = f"{row.session_source}/{row.session_medium}"
                # 7 days of baseline
                baseline_map[key] = {
                    'daily_sessions': (row.sessions or 0) / 7,
                    'daily_conversions': (row.conversions or 0) / 7
                }

            # Find opportunities: sources with 50%+ traffic increase
            opportunities = []
            for row in current_results:
                key = f"{row.session_source}/{row.session_medium}"
                baseline = baseline_map.get(key, {'daily_sessions': 0, 'daily_conversions': 0})

                current_sessions = row.sessions or 0
                baseline_sessions = baseline['daily_sessions']

                if baseline_sessions > 0:
                    increase_pct = ((current_sessions - baseline_sessions) / baseline_sessions) * 100

                    if increase_pct >= 50:  # 50%+ increase
                        conversion_rate = (row.conversions / current_sessions * 100) if current_sessions > 0 else 0

                        opportunities.append({
                            'source': f"{row.session_source}/{row.session_medium}",
                            'current_sessions': current_sessions,
                            'baseline_daily_sessions': round(baseline_sessions, 1),
                            'traffic_increase_pct': round(increase_pct, 0),
                            'conversions': row.conversions or 0,
                            'conversion_rate': round(conversion_rate, 2)
                        })

            # Return best opportunity by conversion rate (quality traffic)
            if opportunities:
                opportunities.sort(key=lambda x: x['conversion_rate'], reverse=True)
                return opportunities[0]

            return None

        except Exception as e:
            log.error(f"Error finding traffic opportunities: {e}")
            return None
        finally:
            db.close()

    # ==================== CONNECTOR HEALTH MONITORING ====================

    async def _check_connector_health(self) -> List[Dict]:
        """
        Check health of all data connectors.

        Detects:
        - ≥3 consecutive failures → critical alert
        - Data stale >24h → medium alert
        - Data stale >48h → high alert
        """
        issues = []

        # List of connectors to check
        connectors = ['shopify', 'klaviyo', 'ga4', 'google_ads', 'merchant_center', 'search_console']

        for connector in connectors:
            connector_issues = await self._analyze_connector_health(connector)
            issues.extend(connector_issues)

        return issues

    async def _analyze_connector_health(self, connector: str) -> List[Dict]:
        """
        Analyze health of a specific connector.

        Returns list of issues detected for this connector.
        """
        db = SessionLocal()
        issues = []

        try:
            # Get recent sync logs for this connector
            recent_logs = db.query(DataSyncLog).filter(
                DataSyncLog.source == connector
            ).order_by(DataSyncLog.started_at.desc()).limit(10).all()

            if not recent_logs:
                # No sync logs = connector never ran (might be intentional)
                return []

            # Check for consecutive failures
            consecutive_failures = 0
            for log_entry in recent_logs:
                if log_entry.status == 'failed':
                    consecutive_failures += 1
                else:
                    break  # Stop counting on first success

            if consecutive_failures >= 3:
                issues.append({
                    'type': 'consecutive_failures',
                    'connector': connector,
                    'failure_count': consecutive_failures,
                    'severity': 'critical',
                    'last_error': recent_logs[0].error_message if recent_logs else None,
                    'last_attempt': recent_logs[0].started_at.isoformat() if recent_logs else None
                })

            # Check for data staleness
            latest_success = next(
                (log_entry for log_entry in recent_logs if log_entry.status in ['success', 'partial']),
                None
            )

            if latest_success and latest_success.completed_at:
                hours_since_sync = (datetime.utcnow() - latest_success.completed_at).total_seconds() / 3600

                if hours_since_sync > 48:
                    issues.append({
                        'type': 'stale_data',
                        'connector': connector,
                        'hours_stale': round(hours_since_sync, 1),
                        'severity': 'high',
                        'last_success': latest_success.completed_at.isoformat()
                    })
                elif hours_since_sync > 24:
                    issues.append({
                        'type': 'stale_data',
                        'connector': connector,
                        'hours_stale': round(hours_since_sync, 1),
                        'severity': 'medium',
                        'last_success': latest_success.completed_at.isoformat()
                    })

            elif not latest_success:
                # No successful syncs in recent history
                issues.append({
                    'type': 'no_successful_sync',
                    'connector': connector,
                    'severity': 'high',
                    'message': f"No successful syncs found in last {len(recent_logs)} attempts"
                })

            return issues

        except Exception as e:
            log.error(f"Error analyzing connector health for {connector}: {e}")
            return []
        finally:
            db.close()

    async def _alert_connector_issue(self, issue: Dict):
        """
        Send alert for connector health issues.
        """
        connector = issue.get('connector', 'unknown')
        issue_type = issue.get('type', 'unknown')
        severity = issue.get('severity', 'medium')

        # Check cooldown
        cooldown_key = f"connector_{connector}_{issue_type}"
        if self._is_connector_alert_in_cooldown(connector, issue_type):
            log.debug(f"Skipping connector alert for {connector} - in cooldown")
            return

        # Build alert content
        if issue_type == 'consecutive_failures':
            title = f"🔴 Connector Failure: {connector.title()} ({issue['failure_count']} consecutive failures)"
            description = f"""
CRITICAL: Data Connector Failing

Connector: {connector}
Consecutive Failures: {issue['failure_count']}
Last Error: {issue.get('last_error', 'Unknown')}
Last Attempt: {issue.get('last_attempt', 'Unknown')}

Impact:
- Data from {connector} is not being updated
- Business metrics may be inaccurate
- Alerts based on {connector} data may be missed

Recommended Actions:
1. Check API credentials for {connector}
2. Verify API rate limits haven't been exceeded
3. Check for service outages at {connector}
4. Review error logs for detailed stack trace
"""

        elif issue_type == 'stale_data':
            title = f"⚠️ Stale Data: {connector.title()} ({issue['hours_stale']:.0f}h old)"
            description = f"""
WARNING: Data Connector Stale

Connector: {connector}
Hours Since Last Sync: {issue['hours_stale']:.1f}
Last Successful Sync: {issue.get('last_success', 'Unknown')}

Impact:
- {connector} data is {issue['hours_stale']:.0f} hours out of date
- Recent changes may not be reflected in dashboards
- Alerts may be delayed or inaccurate

Recommended Actions:
1. Check if sync service is running
2. Trigger a manual sync
3. Review sync scheduler configuration
"""

        elif issue_type == 'no_successful_sync':
            title = f"🔴 No Data: {connector.title()} has no successful syncs"
            description = f"""
CRITICAL: No Successful Data Syncs

Connector: {connector}
Status: {issue.get('message', 'No successful syncs found')}

Impact:
- No data from {connector} is available
- Dashboards and alerts cannot function properly

Recommended Actions:
1. Verify connector configuration
2. Check API credentials
3. Run initial sync manually
"""

        else:
            title = f"⚠️ Connector Issue: {connector.title()}"
            description = f"Issue detected with {connector} connector: {issue}"

        # Persist alert
        alert_id = self._persist_alert(
            alert_type='connector_health',
            severity=severity,
            title=title,
            description=description,
            metric=connector,
            issue_data=issue,
            max_age_hours=12  # Connector alerts refresh more frequently
        )

        # Send notification for critical/high issues
        if severity in ['critical', 'high'] and alert_id:
            try:
                delivery_result = await self.alert_service.send_critical_alert(
                    title,
                    description,
                    issue
                )
                self._update_alert_delivery(alert_id, delivery_result)
            except Exception as e:
                log.error(f"Failed to send connector alert: {e}")

    def _is_connector_alert_in_cooldown(self, connector: str, issue_type: str) -> bool:
        """
        Check if connector alert is in cooldown.

        Uses 6-hour cooldown for connector alerts.
        Cooldown is now keyed by BOTH connector AND issue_type to prevent
        a stale_data alert from suppressing a consecutive_failures alert.
        """
        cooldown_hours = 6
        cooldown_cutoff = datetime.utcnow() - timedelta(hours=cooldown_hours)

        db = SessionLocal()
        try:
            # Query for recent alerts of the same connector AND issue_type
            # issue_type is stored in the issue_data JSON field
            # Use json_extract for SQLite compatibility (works with PostgreSQL too via cast)
            recent_alert = db.query(TrackingAlert).filter(
                TrackingAlert.alert_type == 'connector_health',
                TrackingAlert.source_name == connector,
                TrackingAlert.created_at >= cooldown_cutoff,
                func.json_extract(TrackingAlert.issue_data, '$.type') == issue_type
            ).first()

            return recent_alert is not None

        except Exception as e:
            log.error(f"Error checking connector alert cooldown: {e}")
            return False
        finally:
            db.close()

    def get_connector_health_summary(self) -> Dict:
        """
        Get a summary of all connector health status.

        Useful for dashboard display.
        """
        db = SessionLocal()
        try:
            connectors = ['shopify', 'klaviyo', 'ga4', 'google_ads', 'merchant_center', 'search_console']
            summary = {}

            for connector in connectors:
                # Get latest sync log
                latest = db.query(DataSyncLog).filter(
                    DataSyncLog.source == connector
                ).order_by(DataSyncLog.started_at.desc()).first()

                if not latest:
                    summary[connector] = {
                        'status': 'unknown',
                        'last_sync': None,
                        'message': 'No sync history'
                    }
                    continue

                # Get last successful sync
                latest_success = db.query(DataSyncLog).filter(
                    DataSyncLog.source == connector,
                    DataSyncLog.status.in_(['success', 'partial'])
                ).order_by(DataSyncLog.completed_at.desc()).first()

                hours_stale = None
                if latest_success and latest_success.completed_at:
                    hours_stale = (datetime.utcnow() - latest_success.completed_at).total_seconds() / 3600

                # Count recent failures
                recent_failures = db.query(func.count(DataSyncLog.id)).filter(
                    DataSyncLog.source == connector,
                    DataSyncLog.status == 'failed',
                    DataSyncLog.started_at >= datetime.utcnow() - timedelta(hours=24)
                ).scalar() or 0

                # Determine status
                if hours_stale and hours_stale > 48:
                    status = 'critical'
                elif recent_failures >= 3:
                    status = 'critical'
                elif hours_stale and hours_stale > 24:
                    status = 'warning'
                elif recent_failures >= 1:
                    status = 'warning'
                elif latest.status == 'success':
                    status = 'healthy'
                else:
                    status = 'unknown'

                summary[connector] = {
                    'status': status,
                    'last_sync': latest.completed_at.isoformat() if latest.completed_at else None,
                    'last_status': latest.status,
                    'hours_since_sync': round(hours_stale, 1) if hours_stale else None,
                    'recent_failures_24h': recent_failures,
                    'records_processed': latest.records_processed
                }

            return summary

        except Exception as e:
            log.error(f"Error getting connector health summary: {e}")
            return {}
        finally:
            db.close()
