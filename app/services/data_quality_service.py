"""
Data Quality & Tracking Validation Service

Validates data integrity across all sources.
Ensures you can trust the insights from other modules.

Answers: "Can I trust this data?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from collections import defaultdict

from app.models.data_quality import (
    DataSyncStatus, TrackingDiscrepancy, UTMHealthCheck,
    MerchantCenterHealth, GoogleAdsLinkHealth,
    DataQualityScore, TrackingAlert
)
from app.utils.logger import log


class DataQualityService:
    """Service for data quality validation and monitoring"""

    def __init__(self, db: Session):
        self.db = db

        # Thresholds (configurable)
        self.discrepancy_warning_threshold = 0.10  # 10%
        self.discrepancy_critical_threshold = 0.20  # 20%
        self.sync_freshness_warning_hours = 24
        self.sync_freshness_critical_hours = 48
        self.utm_coverage_minimum = 0.80  # 80%

    async def run_full_data_quality_check(self) -> Dict:
        """
        Run comprehensive data quality check

        Checks all aspects of data quality and returns overall health
        """
        log.info("Running full data quality check")

        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'checks_run': [],
            'issues_found': [],
            'overall_health': 'unknown'
        }

        # 1. Check data sync status
        try:
            sync_health = await self.check_data_sync_health()
            results['sync_health'] = sync_health
            results['checks_run'].append('sync_health')

            if not sync_health['is_healthy']:
                results['issues_found'].extend(sync_health.get('issues', []))
        except Exception as e:
            log.error(f"Error checking sync health: {str(e)}")
            results['issues_found'].append({'check': 'sync_health', 'error': str(e)})

        # 2. Check conversion tracking accuracy
        try:
            tracking_health = await self.check_conversion_tracking()
            results['tracking_health'] = tracking_health
            results['checks_run'].append('tracking_accuracy')

            if tracking_health.get('has_discrepancies'):
                results['issues_found'].extend(tracking_health.get('discrepancies', []))
        except Exception as e:
            log.error(f"Error checking conversion tracking: {str(e)}")
            results['issues_found'].append({'check': 'tracking_accuracy', 'error': str(e)})

        # 3. Check UTM health
        try:
            utm_health = await self.check_utm_health()
            results['utm_health'] = utm_health
            results['checks_run'].append('utm_health')

            if not utm_health['is_healthy']:
                results['issues_found'].extend(utm_health.get('issues', []))
        except Exception as e:
            log.error(f"Error checking UTM health: {str(e)}")
            results['issues_found'].append({'check': 'utm_health', 'error': str(e)})

        # 4. Check Merchant Center health
        try:
            feed_health = await self.check_merchant_center_health()
            results['feed_health'] = feed_health
            results['checks_run'].append('feed_health')

            if not feed_health['is_healthy']:
                results['issues_found'].extend(feed_health.get('issues', []))
        except Exception as e:
            log.error(f"Error checking feed health: {str(e)}")
            results['issues_found'].append({'check': 'feed_health', 'error': str(e)})

        # 5. Check Google Ads/Analytics linking
        try:
            link_health = await self.check_google_ads_link_health()
            results['link_health'] = link_health
            results['checks_run'].append('link_health')

            if not link_health['is_healthy']:
                results['issues_found'].extend(link_health.get('issues', []))
        except Exception as e:
            log.error(f"Error checking link health: {str(e)}")
            results['issues_found'].append({'check': 'link_health', 'error': str(e)})

        # Calculate overall data quality score
        quality_score = self._calculate_quality_score(results)
        results['quality_score'] = quality_score
        results['overall_health'] = self._determine_health_status(quality_score)

        # Generate alerts for critical issues
        await self._generate_alerts(results)

        log.info(f"Data quality check complete. Score: {quality_score}/100")

        return results

    async def check_data_sync_health(self) -> Dict:
        """
        Check health of all data source syncs

        Returns sync status for each source
        """
        log.info("Checking data sync health")

        # Get all data sources
        sources = self.db.query(DataSyncStatus).all()

        if not sources:
            return {
                'is_healthy': False,
                'message': 'No data sources configured',
                'sources': []
            }

        now = datetime.utcnow()
        issues = []
        healthy_sources = 0
        total_sources = len(sources)

        source_statuses = []

        for source in sources:
            source_data = {
                'source_name': source.source_name,
                'source_type': source.source_type,
                'last_successful_sync': source.last_successful_sync.isoformat() if source.last_successful_sync else None,
                'sync_status': source.sync_status,
                'is_healthy': source.is_healthy,
                'health_score': source.health_score
            }

            # Check freshness
            if source.last_successful_sync:
                hours_since_sync = (now - source.last_successful_sync).total_seconds() / 3600
                source_data['hours_since_sync'] = round(hours_since_sync, 1)

                if hours_since_sync > self.sync_freshness_critical_hours:
                    issues.append({
                        'source': source.source_name,
                        'severity': 'critical',
                        'issue': f'No sync in {hours_since_sync:.0f} hours (critical)',
                        'expected': f'< {self.sync_freshness_critical_hours}h'
                    })
                    source_data['freshness_status'] = 'critical'

                elif hours_since_sync > self.sync_freshness_warning_hours:
                    issues.append({
                        'source': source.source_name,
                        'severity': 'warning',
                        'issue': f'No sync in {hours_since_sync:.0f} hours',
                        'expected': f'< {self.sync_freshness_warning_hours}h'
                    })
                    source_data['freshness_status'] = 'warning'

                else:
                    source_data['freshness_status'] = 'healthy'
                    healthy_sources += 1

            # Check error count
            if source.error_count > 0:
                source_data['consecutive_errors'] = source.error_count
                source_data['last_error'] = source.last_error

                if source.error_count >= 3:
                    issues.append({
                        'source': source.source_name,
                        'severity': 'critical',
                        'issue': f'{source.error_count} consecutive sync failures',
                        'error': source.last_error
                    })

            source_statuses.append(source_data)

        return {
            'is_healthy': len(issues) == 0,
            'total_sources': total_sources,
            'healthy_sources': healthy_sources,
            'health_percentage': round((healthy_sources / total_sources * 100) if total_sources > 0 else 0, 1),
            'sources': source_statuses,
            'issues': issues,
            'summary': f"{healthy_sources}/{total_sources} sources healthy"
        }

    async def check_conversion_tracking(self, days: int = 7) -> Dict:
        """
        Compare conversion tracking across platforms

        Shopify (truth) vs GA4 vs Google Ads
        """
        log.info(f"Checking conversion tracking accuracy (last {days} days)")

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)

        # Get recent discrepancy records
        discrepancies = self.db.query(TrackingDiscrepancy).filter(
            TrackingDiscrepancy.date >= start_date,
            TrackingDiscrepancy.date <= end_date
        ).order_by(desc(TrackingDiscrepancy.date)).all()

        if not discrepancies:
            # In real implementation, would calculate this from actual data
            # For now, return structure showing what it would look like
            return {
                'has_discrepancies': False,
                'message': 'No tracking data available for comparison',
                'period_days': days
            }

        # Aggregate metrics
        total_shopify_orders = sum(d.shopify_orders for d in discrepancies)
        total_shopify_revenue = sum(d.shopify_revenue for d in discrepancies)

        total_ga4_conversions = sum(d.ga4_conversions or 0 for d in discrepancies)
        total_ga4_revenue = sum(d.ga4_revenue or 0 for d in discrepancies)

        total_ads_conversions = sum(d.google_ads_conversions or 0 for d in discrepancies)
        total_ads_revenue = sum(d.google_ads_revenue or 0 for d in discrepancies)

        # Calculate discrepancies
        ga4_discrepancy = self._calculate_discrepancy_pct(total_shopify_orders, total_ga4_conversions)
        ads_discrepancy = self._calculate_discrepancy_pct(total_shopify_orders, total_ads_conversions)

        # Identify issues
        has_critical = abs(ga4_discrepancy) > (self.discrepancy_critical_threshold * 100) or \
                       abs(ads_discrepancy) > (self.discrepancy_critical_threshold * 100)

        has_warning = abs(ga4_discrepancy) > (self.discrepancy_warning_threshold * 100) or \
                      abs(ads_discrepancy) > (self.discrepancy_warning_threshold * 100)

        discrepancy_details = []

        if abs(ga4_discrepancy) > (self.discrepancy_warning_threshold * 100):
            missing = total_shopify_orders - total_ga4_conversions
            discrepancy_details.append({
                'platform': 'GA4',
                'discrepancy_pct': round(ga4_discrepancy, 1),
                'missing_conversions': missing,
                'severity': 'critical' if abs(ga4_discrepancy) > 20 else 'warning',
                'message': f"GA4 tracking {abs(missing)} conversions ({abs(ga4_discrepancy):.1f}%) compared to Shopify"
            })

        if abs(ads_discrepancy) > (self.discrepancy_warning_threshold * 100):
            missing = total_shopify_orders - total_ads_conversions
            discrepancy_details.append({
                'platform': 'Google Ads',
                'discrepancy_pct': round(ads_discrepancy, 1),
                'missing_conversions': missing,
                'severity': 'critical' if abs(ads_discrepancy) > 20 else 'warning',
                'message': f"Google Ads tracking {abs(missing)} conversions ({abs(ads_discrepancy):.1f}%) compared to Shopify"
            })

        return {
            'period_days': days,
            'period_start': start_date.isoformat(),
            'period_end': end_date.isoformat(),

            'shopify': {
                'orders': total_shopify_orders,
                'revenue': round(total_shopify_revenue, 2)
            },

            'ga4': {
                'conversions': total_ga4_conversions,
                'revenue': round(total_ga4_revenue, 2),
                'discrepancy_pct': round(ga4_discrepancy, 1),
                'missing_conversions': total_shopify_orders - total_ga4_conversions
            },

            'google_ads': {
                'conversions': total_ads_conversions,
                'revenue': round(total_ads_revenue, 2),
                'discrepancy_pct': round(ads_discrepancy, 1),
                'missing_conversions': total_shopify_orders - total_ads_conversions
            },

            'has_discrepancies': has_critical or has_warning,
            'has_critical_discrepancies': has_critical,
            'discrepancies': discrepancy_details,

            'health_status': 'critical' if has_critical else ('warning' if has_warning else 'healthy')
        }

    def _calculate_discrepancy_pct(self, truth: int, measured: int) -> float:
        """Calculate percentage discrepancy"""
        if truth == 0:
            return 0.0

        return ((measured - truth) / truth) * 100

    async def check_utm_health(self, days: int = 7) -> Dict:
        """
        Check UTM parameter tracking health

        Returns coverage percentage and issues
        """
        log.info("Checking UTM tracking health")

        # Get recent UTM health checks
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        checks = self.db.query(UTMHealthCheck).filter(
            UTMHealthCheck.date >= start_date
        ).order_by(desc(UTMHealthCheck.date)).all()

        if not checks:
            return {
                'is_healthy': False,
                'message': 'No UTM health data available',
                'coverage_pct': 0
            }

        # Get most recent check
        latest = checks[0]

        issues = []
        is_healthy = True

        # Check coverage
        if latest.utm_coverage_pct < (self.utm_coverage_minimum * 100):
            issues.append({
                'severity': 'warning',
                'issue': f'UTM coverage is {latest.utm_coverage_pct:.1f}%',
                'expected': f'>= {self.utm_coverage_minimum * 100}%',
                'message': 'Low UTM parameter coverage means attribution data is incomplete'
            })
            is_healthy = False

        # Check for quality issues
        if latest.malformed_utms and latest.malformed_utms > 0:
            issues.append({
                'severity': 'warning',
                'issue': f'{latest.malformed_utms} sessions with malformed UTM parameters',
                'message': 'Malformed UTMs prevent proper attribution'
            })

        return {
            'is_healthy': is_healthy,
            'coverage_pct': round(latest.utm_coverage_pct, 1),
            'total_sessions': latest.total_sessions,
            'sessions_with_utm': latest.sessions_with_utm,
            'missing_utms': latest.total_sessions - latest.sessions_with_utm,

            'utm_by_source': latest.utm_by_source,
            'missing_utm_campaigns': latest.missing_utm_campaigns,
            'malformed_utms': latest.malformed_utms,

            'issues': issues,
            'health_score': latest.health_score,
            'checked_at': latest.date.isoformat()
        }

    async def check_merchant_center_health(self) -> Dict:
        """
        Check Google Merchant Center feed health

        Returns feed status and issues
        """
        log.info("Checking Merchant Center feed health")

        # Get most recent check
        latest = self.db.query(MerchantCenterHealth).order_by(
            desc(MerchantCenterHealth.checked_at)
        ).first()

        if not latest:
            return {
                'is_healthy': False,
                'message': 'No Merchant Center health data available'
            }

        issues = []
        is_healthy = True

        # Check for disapprovals
        if latest.disapproved_products > 0:
            severity = 'critical' if latest.disapproved_products > 10 else 'warning'
            issues.append({
                'severity': severity,
                'issue': f'{latest.disapproved_products} products disapproved',
                'details': latest.disapproval_reasons,
                'message': 'Disapproved products cannot appear in Shopping ads'
            })
            is_healthy = False

        # Check for new disapprovals
        if latest.new_disapprovals > 0:
            issues.append({
                'severity': 'high',
                'issue': f'{latest.new_disapprovals} NEW products disapproved since last check',
                'products': latest.new_disapproval_products,
                'message': 'These products just stopped showing in Shopping'
            })

        # Check feed processing status
        if latest.feed_processing_status != 'success':
            issues.append({
                'severity': 'critical',
                'issue': f'Feed processing status: {latest.feed_processing_status}',
                'message': 'Feed is not processing successfully'
            })
            is_healthy = False

        return {
            'is_healthy': is_healthy,

            'total_products': latest.total_products,
            'approved_products': latest.approved_products,
            'disapproved_products': latest.disapproved_products,
            'pending_products': latest.pending_products,

            'approval_rate_pct': round((latest.approved_products / latest.total_products * 100) if latest.total_products > 0 else 0, 1),

            'feed_status': latest.feed_processing_status,
            'last_feed_upload': latest.last_feed_upload.isoformat() if latest.last_feed_upload else None,

            'disapproval_reasons': latest.disapproval_reasons,
            'new_disapprovals': latest.new_disapprovals,

            'issues': issues,
            'health_score': latest.health_score,
            'checked_at': latest.checked_at.isoformat()
        }

    async def check_google_ads_link_health(self) -> Dict:
        """
        Check Google Ads / Analytics linking health

        Verifies proper setup
        """
        log.info("Checking Google Ads/Analytics link health")

        # Get most recent check
        latest = self.db.query(GoogleAdsLinkHealth).order_by(
            desc(GoogleAdsLinkHealth.checked_at)
        ).first()

        if not latest:
            return {
                'is_healthy': False,
                'message': 'No link health data available'
            }

        issues = []
        is_healthy = True

        # Critical checks
        if not latest.auto_tagging_enabled:
            issues.append({
                'severity': 'critical',
                'issue': 'Auto-tagging not enabled',
                'impact': 'Google Ads clicks not properly tracked',
                'fix': 'Enable auto-tagging in Google Ads settings'
            })
            is_healthy = False

        if not latest.ga4_linked:
            issues.append({
                'severity': 'critical',
                'issue': 'GA4 not linked to Google Ads',
                'impact': 'Cannot import GA4 conversions or share audiences',
                'fix': 'Link GA4 property in Google Ads'
            })
            is_healthy = False

        if not latest.conversion_import_enabled:
            issues.append({
                'severity': 'high',
                'issue': 'Conversion import not enabled',
                'impact': 'Google Ads Smart Bidding lacks conversion data',
                'fix': 'Set up conversion import from GA4'
            })

        if not latest.conversions_importing:
            issues.append({
                'severity': 'high',
                'issue': 'Conversions not importing',
                'impact': 'Smart Bidding has no data to optimize on',
                'fix': 'Check conversion import configuration'
            })

        return {
            'is_healthy': is_healthy,

            'auto_tagging_enabled': latest.auto_tagging_enabled,
            'gclid_present': latest.gclid_present_in_urls,

            'ga4_linked': latest.ga4_linked,
            'ga4_property_id': latest.ga4_property_id,

            'conversion_import_enabled': latest.conversion_import_enabled,
            'conversions_importing': latest.conversions_importing,
            'last_conversion_import': latest.last_conversion_import.isoformat() if latest.last_conversion_import else None,

            'audience_sharing_enabled': latest.audience_sharing_enabled,
            'remarketing_enabled': latest.remarketing_enabled,

            'issues': issues,
            'critical_issues_count': latest.critical_issues_count,
            'health_score': latest.health_score,
            'checked_at': latest.checked_at.isoformat()
        }

    def _calculate_quality_score(self, results: Dict) -> int:
        """
        Calculate overall data quality score (0-100)

        Weighted average of component scores
        """
        scores = []
        weights = []

        # Sync health (25%)
        if 'sync_health' in results:
            score = results['sync_health'].get('health_percentage', 0)
            scores.append(score)
            weights.append(0.25)

        # Tracking accuracy (30%) - most important
        if 'tracking_health' in results:
            tracking = results['tracking_health']
            if tracking.get('health_status') == 'healthy':
                score = 100
            elif tracking.get('health_status') == 'warning':
                score = 70
            elif tracking.get('health_status') == 'critical':
                score = 30
            else:
                score = 50
            scores.append(score)
            weights.append(0.30)

        # UTM health (15%)
        if 'utm_health' in results:
            score = results['utm_health'].get('health_score', 0)
            scores.append(score)
            weights.append(0.15)

        # Feed health (15%)
        if 'feed_health' in results:
            score = results['feed_health'].get('health_score', 0)
            scores.append(score)
            weights.append(0.15)

        # Link health (15%)
        if 'link_health' in results:
            score = results['link_health'].get('health_score', 0)
            scores.append(score)
            weights.append(0.15)

        if not scores:
            return 0

        # Weighted average
        total_weight = sum(weights)
        weighted_score = sum(s * w for s, w in zip(scores, weights)) / total_weight

        return round(weighted_score)

    def _determine_health_status(self, score: int) -> str:
        """Determine health status from score"""
        if score >= 90:
            return 'excellent'
        elif score >= 70:
            return 'good'
        elif score >= 50:
            return 'warning'
        else:
            return 'critical'

    async def _generate_alerts(self, results: Dict):
        """
        Generate alerts for critical issues

        Creates TrackingAlert records
        """
        issues = results.get('issues_found', [])

        for issue in issues:
            # Check if this issue already has an active alert
            check_type = issue.get('check', '')
            existing_alert = self.db.query(TrackingAlert).filter(
                TrackingAlert.alert_type == check_type,
                TrackingAlert.status == 'active'
            ).first()

            if existing_alert:
                # Update existing alert
                existing_alert.updated_at = datetime.utcnow()
                existing_alert.issue_data = issue
            else:
                # Create new alert
                severity = issue.get('severity', 'medium')

                alert = TrackingAlert(
                    alert_type=check_type,
                    severity=severity,
                    title=issue.get('issue', 'Data quality issue detected'),
                    description=issue.get('message', ''),
                    issue_data=issue,
                    status='active'
                )

                self.db.add(alert)

        self.db.commit()

    async def get_active_alerts(self) -> List[Dict]:
        """Get all active data quality alerts"""
        alerts = self.db.query(TrackingAlert).filter(
            TrackingAlert.status == 'active'
        ).order_by(
            desc(TrackingAlert.created_at)
        ).all()

        return [
            {
                'id': alert.id,
                'alert_type': alert.alert_type,
                'severity': alert.severity,
                'title': alert.title,
                'description': alert.description,
                'issue_data': alert.issue_data,
                'created_at': alert.created_at.isoformat(),
                'recommended_actions': alert.recommended_actions
            }
            for alert in alerts
        ]

    async def acknowledge_alert(self, alert_id: int) -> Dict:
        """Acknowledge an alert"""
        alert = self.db.query(TrackingAlert).filter(
            TrackingAlert.id == alert_id
        ).first()

        if not alert:
            return {'error': 'Alert not found'}

        alert.status = 'acknowledged'
        alert.acknowledged_at = datetime.utcnow()
        self.db.commit()

        return {
            'success': True,
            'alert_id': alert_id,
            'status': 'acknowledged'
        }

    async def resolve_alert(self, alert_id: int, resolution_notes: str = None) -> Dict:
        """Resolve an alert"""
        alert = self.db.query(TrackingAlert).filter(
            TrackingAlert.id == alert_id
        ).first()

        if not alert:
            return {'error': 'Alert not found'}

        alert.status = 'resolved'
        alert.resolved_at = datetime.utcnow()
        if resolution_notes:
            alert.resolution_notes = resolution_notes

        self.db.commit()

        return {
            'success': True,
            'alert_id': alert_id,
            'status': 'resolved'
        }
