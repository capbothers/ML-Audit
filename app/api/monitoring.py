"""
Monitoring and Dashboard endpoints
Real-time safety net that catches everything
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from app.services.monitoring_service import MonitoringService
from app.utils.logger import log
from app.utils.response_cache import response_cache

router = APIRouter(prefix="/monitor", tags=["monitoring"])

# Global monitoring service instance
monitoring_service = None
monitoring_task = None


@router.post("/start")
async def start_monitoring(background_tasks: BackgroundTasks):
    """
    Start continuous monitoring in the background
    This is your safety net - it watches everything 24/7
    """
    global monitoring_service, monitoring_task

    if monitoring_service is not None:
        return {
            "status": "already_running",
            "message": "Monitoring service is already active"
        }

    monitoring_service = MonitoringService()

    # Start monitoring in background
    background_tasks.add_task(monitoring_service.start_continuous_monitoring)

    log.info("Monitoring service started")

    return {
        "status": "started",
        "message": "Continuous monitoring is now active",
        "monitored_metrics": list(monitoring_service.monitored_metrics.keys()),
        "check_interval": "15 minutes"
    }


@router.post("/stop")
async def stop_monitoring():
    """Stop continuous monitoring"""
    global monitoring_service

    if monitoring_service is None:
        return {
            "status": "not_running",
            "message": "Monitoring service is not active"
        }

    # Actually stop the monitoring loop
    monitoring_service.stop()
    monitoring_service = None

    return {
        "status": "stopped",
        "message": "Monitoring service stopped"
    }


@router.get("/status")
async def get_monitoring_status():
    """Get monitoring service status"""
    global monitoring_service

    if monitoring_service is None:
        return {
            "active": False,
            "message": "Monitoring service is not running",
            "start_endpoint": "POST /monitor/start"
        }

    # Get recent alert counts from database (cooldowns are now DB-backed)
    from app.models.base import SessionLocal
    from app.models.data_quality import TrackingAlert

    recent_alerts = {}
    db = SessionLocal()
    try:
        for metric in monitoring_service.monitored_metrics.keys():
            cooldown_hours = monitoring_service.monitored_metrics[metric].get('cooldown_hours', 24)
            cooldown_cutoff = datetime.utcnow() - timedelta(hours=cooldown_hours)

            recent = db.query(TrackingAlert).filter(
                TrackingAlert.alert_type == 'metric_anomaly',
                TrackingAlert.source_name == metric,
                TrackingAlert.created_at >= cooldown_cutoff
            ).first()

            recent_alerts[metric] = {
                'in_cooldown': recent is not None,
                'last_alert_at': recent.created_at.isoformat() if recent else None,
                'cooldown_hours': cooldown_hours
            }
    except Exception as e:
        log.error(f"Error fetching alert status: {e}")
    finally:
        db.close()

    return {
        "active": monitoring_service.is_running(),
        "monitored_metrics": list(monitoring_service.monitored_metrics.keys()),
        "last_checks": monitoring_service.last_check,
        "alert_status": recent_alerts
    }


@router.get("/dashboard")
async def get_dashboard_data():
    """
    Get dashboard data that surfaces what matters
    Returns insights, not raw data
    """
    cached = response_cache.get("monitor:dashboard")
    if cached:
        return cached
    from app.models.base import SessionLocal
    from app.models.data_quality import TrackingAlert
    from app.models.shopify import ShopifyOrder, ShopifyRefund
    from app.models.ga4_data import GA4TrafficSource
    from app.models.google_ads_data import GoogleAdsCampaign
    from app.models.transaction import AbandonedCheckout
    from sqlalchemy import func

    db = SessionLocal()
    try:
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_48h = now - timedelta(hours=48)
        today_date = now.date()
        yesterday_date = (now - timedelta(days=1)).date()

        # Get active issues from TrackingAlert
        active_issues_query = db.query(TrackingAlert).filter(
            TrackingAlert.alert_type == 'metric_anomaly',
            TrackingAlert.status.in_(['active', 'acknowledged', 'investigating'])
        ).order_by(TrackingAlert.created_at.desc()).limit(10).all()

        active_issues = []
        for alert in active_issues_query:
            issue_data = alert.issue_data or {}
            active_issues.append({
                "id": alert.id,
                "metric": alert.source_name,
                "status": alert.severity,
                "current": issue_data.get('current'),
                "expected": issue_data.get('baseline'),
                "change_pct": issue_data.get('change_pct'),
                "diagnosis": alert.probable_cause[:300] if alert.probable_cause else None,
                "recommended_actions": alert.recommended_actions,
                "detected_at": alert.created_at.isoformat() if alert.created_at else None
            })

        # Get opportunities from TrackingAlert
        opportunities_query = db.query(TrackingAlert).filter(
            TrackingAlert.alert_type == 'opportunity',
            TrackingAlert.status.in_(['active', 'acknowledged'])
        ).order_by(TrackingAlert.created_at.desc()).limit(10).all()

        opportunities = []
        for opp in opportunities_query:
            opportunities.append({
                "id": opp.id,
                "type": opp.source_name,
                "title": opp.title,
                "description": opp.description[:300] if opp.description else None,
                "data": opp.issue_data,
                "detected_at": opp.created_at.isoformat() if opp.created_at else None
            })

        # Calculate key metrics
        # Today's revenue
        today_revenue = db.query(func.sum(ShopifyOrder.total_price)).filter(
            ShopifyOrder.created_at >= last_24h,
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).scalar() or 0

        # Yesterday's revenue (for comparison)
        yesterday_revenue = db.query(func.sum(ShopifyOrder.total_price)).filter(
            ShopifyOrder.created_at >= last_48h,
            ShopifyOrder.created_at < last_24h,
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).scalar() or 0

        revenue_change = ((today_revenue - yesterday_revenue) / yesterday_revenue * 100) if yesterday_revenue > 0 else 0

        # Traffic (sessions)
        today_sessions = db.query(func.sum(GA4TrafficSource.sessions)).filter(
            GA4TrafficSource.date >= today_date,
            GA4TrafficSource.session_source == '(all)',
            GA4TrafficSource.session_medium == '(all)'
        ).scalar() or 0

        yesterday_sessions = db.query(func.sum(GA4TrafficSource.sessions)).filter(
            GA4TrafficSource.date == yesterday_date,
            GA4TrafficSource.session_source == '(all)',
            GA4TrafficSource.session_medium == '(all)'
        ).scalar() or 0

        traffic_change = ((today_sessions - yesterday_sessions) / yesterday_sessions * 100) if yesterday_sessions > 0 else 0

        # Conversion rate
        today_orders = db.query(func.count(ShopifyOrder.id)).filter(
            ShopifyOrder.created_at >= last_24h,
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).scalar() or 0

        conversion_rate = (today_orders / today_sessions * 100) if today_sessions > 0 else 0

        # ROAS
        ads_today = db.query(
            func.sum(GoogleAdsCampaign.conversions_value).label('revenue'),
            func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros')
        ).filter(
            GoogleAdsCampaign.date >= today_date
        ).first()

        ad_cost = (ads_today.cost_micros or 0) / 1_000_000
        ad_revenue = ads_today.revenue or 0
        roas = (ad_revenue / ad_cost) if ad_cost > 0 else 0

        # Refund stats (all-time)
        refunded_orders = db.query(func.count(func.distinct(ShopifyRefund.shopify_order_id))).scalar() or 0
        refund_records = db.query(func.count(ShopifyRefund.id)).scalar() or 0

        # Cart abandonment
        abandoned = db.query(func.count(AbandonedCheckout.id)).filter(
            AbandonedCheckout.created_at >= last_24h,
            AbandonedCheckout.recovered == False
        ).scalar() or 0

        total_checkouts = abandoned + today_orders
        cart_abandonment = (abandoned / total_checkouts * 100) if total_checkouts > 0 else 0

        # Determine overall status
        critical_count = len([i for i in active_issues if i['status'] == 'critical'])
        high_count = len([i for i in active_issues if i['status'] == 'high'])

        if critical_count > 0:
            overall_status = "critical"
        elif high_count > 0:
            overall_status = "warning"
        else:
            overall_status = "healthy"

        def get_metric_status(change: float, higher_is_bad: bool = False) -> str:
            if higher_is_bad:
                if change > 15:
                    return "critical"
                elif change > 5:
                    return "warning"
                return "healthy"
            else:
                if change < -25:
                    return "critical"
                elif change < -10:
                    return "warning"
                return "healthy"

        result = {
            "summary": {
                "status": overall_status,
                "active_issues": len(active_issues),
                "active_opportunities": len(opportunities),
                "last_updated": now.isoformat()
            },
            "active_issues": active_issues,
            "opportunities": opportunities,
            "key_metrics": {
                "revenue": {
                    "value": f"${today_revenue:,.2f}",
                    "change_24h": round(revenue_change, 1),
                    "trend": "up" if revenue_change > 0 else "down",
                    "status": get_metric_status(revenue_change)
                },
                "traffic": {
                    "value": f"{today_sessions:,}",
                    "change_24h": round(traffic_change, 1),
                    "trend": "up" if traffic_change > 0 else "down",
                    "status": get_metric_status(traffic_change)
                },
                "conversion_rate": {
                    "value": f"{conversion_rate:.1f}%",
                    "change_24h": None,  # Would need more historical data
                    "trend": None,
                    "status": "healthy" if conversion_rate > 2 else "warning"
                },
                "roas": {
                    "value": f"{roas:.1f}x",
                    "change_24h": None,
                    "trend": None,
                    "status": "healthy" if roas >= 3 else ("warning" if roas >= 2 else "critical")
                },
                "cart_abandonment": {
                    "value": f"{cart_abandonment:.0f}%",
                    "change_24h": None,
                    "trend": None,
                    "status": "healthy" if cart_abandonment < 65 else ("warning" if cart_abandonment < 75 else "critical")
                },
                "refunds": {
                    "value": f"{refunded_orders:,} orders",
                    "records": refund_records,
                    "change_24h": None,
                    "trend": None,
                    "status": "healthy"
                }
            }
        }
        response_cache.set("monitor:dashboard", result, ttl=120)
        return result

    except Exception as e:
        log.error(f"Error fetching dashboard data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/alerts/history")
async def get_alert_history(hours: int = 24):
    """Get alert history for last N hours"""
    from app.models.base import SessionLocal
    from app.models.data_quality import TrackingAlert
    from sqlalchemy import func

    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # Query alerts from database
        alerts = db.query(TrackingAlert).filter(
            TrackingAlert.created_at >= cutoff
        ).order_by(TrackingAlert.created_at.desc()).limit(100).all()

        # Count by severity
        severity_counts = db.query(
            TrackingAlert.severity,
            func.count(TrackingAlert.id)
        ).filter(
            TrackingAlert.created_at >= cutoff
        ).group_by(TrackingAlert.severity).all()

        by_severity = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for severity, count in severity_counts:
            if severity in by_severity:
                by_severity[severity] = count

        # Format alerts
        alert_list = []
        for alert in alerts:
            alert_list.append({
                "id": alert.id,
                "timestamp": alert.created_at.isoformat() if alert.created_at else None,
                "severity": alert.severity,
                "metric": alert.source_name,
                "title": alert.title,
                "message": alert.description[:200] if alert.description else None,
                "diagnosis": alert.probable_cause[:200] if alert.probable_cause else None,
                "status": alert.status,
                "notification_sent": alert.notification_sent,
                "notification_channels": alert.notification_channels,
                "resolved": alert.status == 'resolved'
            })

        return {
            "period": f"Last {hours} hours",
            "total_alerts": len(alerts),
            "by_severity": by_severity,
            "alerts": alert_list
        }

    except Exception as e:
        log.error(f"Error fetching alert history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/check/manual")
async def manual_check():
    """
    Manually trigger a monitoring check
    Useful for testing or on-demand checks
    """
    service = MonitoringService()

    # Run one check cycle
    issues = await service._check_all_metrics()
    opportunities = await service._detect_opportunities()

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "issues_found": len(issues),
        "opportunities_found": len(opportunities),
        "issues": issues,
        "opportunities": opportunities
    }


@router.get("/metrics/live")
async def get_live_metrics():
    """
    Get live metric values for dashboard
    Updated every few minutes
    """
    from app.models.base import SessionLocal
    from app.models.shopify import ShopifyOrder
    from app.models.ga4_data import GA4TrafficSource
    from app.models.google_ads_data import GoogleAdsCampaign
    from app.models.transaction import AbandonedCheckout
    from sqlalchemy import func

    db = SessionLocal()
    try:
        now = datetime.utcnow()
        last_hour = now - timedelta(hours=1)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_date = now.date()

        # Revenue and orders in last hour
        hour_stats = db.query(
            func.count(ShopifyOrder.id).label('orders'),
            func.sum(ShopifyOrder.total_price).label('revenue')
        ).filter(
            ShopifyOrder.created_at >= last_hour,
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).first()

        # Revenue today
        today_stats = db.query(
            func.count(ShopifyOrder.id).label('orders'),
            func.sum(ShopifyOrder.total_price).label('revenue')
        ).filter(
            ShopifyOrder.created_at >= today_start,
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).first()

        # Ad spend today
        ads_stats = db.query(
            func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
            func.sum(GoogleAdsCampaign.conversions_value).label('ad_revenue')
        ).filter(
            GoogleAdsCampaign.date >= today_date
        ).first()

        ad_spend_today = (ads_stats.cost_micros or 0) / 1_000_000
        ad_revenue_today = ads_stats.ad_revenue or 0
        roas_today = (ad_revenue_today / ad_spend_today) if ad_spend_today > 0 else 0

        # Abandoned checkouts today
        abandoned_today = db.query(func.count(AbandonedCheckout.id)).filter(
            AbandonedCheckout.created_at >= today_start,
            AbandonedCheckout.recovered == False
        ).scalar() or 0

        return {
            "timestamp": now.isoformat(),
            "metrics": {
                "revenue_last_hour": round(float(hour_stats.revenue or 0), 2),
                "orders_last_hour": hour_stats.orders or 0,
                "revenue_today": round(float(today_stats.revenue or 0), 2),
                "orders_today": today_stats.orders or 0,
                "ad_spend_today": round(ad_spend_today, 2),
                "ad_revenue_today": round(ad_revenue_today, 2),
                "roas_today": round(roas_today, 2),
                "abandoned_checkouts_today": abandoned_today
            }
        }

    except Exception as e:
        log.error(f"Error fetching live metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/metrics/timeseries")
async def get_metric_timeseries(days: int = 7):
    """
    Get daily time series for key metrics.
    Defaults to last 7 days including today.
    """
    from app.models.base import SessionLocal
    from app.models.shopify import ShopifyOrder
    from app.models.ga4_data import GA4DailySummary
    from sqlalchemy import func

    db = SessionLocal()
    try:
        now = datetime.utcnow()
        start_date = (now - timedelta(days=days - 1)).date()
        end_date = now.date()

        # Shopify revenue/orders by day
        shopify_rows = db.query(
            func.date(ShopifyOrder.created_at).label("date"),
            func.count(ShopifyOrder.id).label("orders"),
            func.sum(ShopifyOrder.total_price).label("revenue")
        ).filter(
            ShopifyOrder.created_at >= datetime.combine(start_date, datetime.min.time()),
            ShopifyOrder.financial_status.in_(['paid', 'partially_refunded'])
        ).group_by(func.date(ShopifyOrder.created_at)).all()

        shopify_by_date = {
            row.date: {
                "orders": int(row.orders or 0),
                "revenue": float(row.revenue or 0)
            }
            for row in shopify_rows
        }

        # GA4 sessions by day
        ga4_rows = db.query(
            GA4DailySummary.date,
            GA4DailySummary.sessions
        ).filter(
            GA4DailySummary.date >= start_date,
            GA4DailySummary.date <= end_date
        ).all()

        ga4_by_date = {row.date: int(row.sessions or 0) for row in ga4_rows}

        labels = []
        revenue_series = []
        sessions_series = []
        conversion_series = []
        aov_series = []

        current = start_date
        while current <= end_date:
            labels.append(current.isoformat())
            shopify = shopify_by_date.get(current, {"orders": 0, "revenue": 0.0})
            sessions = ga4_by_date.get(current, 0)

            revenue_series.append(round(shopify["revenue"], 2))
            sessions_series.append(sessions)

            conversion_rate = (shopify["orders"] / sessions * 100) if sessions > 0 else 0
            conversion_series.append(round(conversion_rate, 2))

            aov = (shopify["revenue"] / shopify["orders"]) if shopify["orders"] > 0 else 0
            aov_series.append(round(aov, 2))

            current += timedelta(days=1)

        return {
            "period": f"Last {days} days",
            "dates": labels,
            "series": {
                "revenue": revenue_series,
                "sessions": sessions_series,
                "conversion_rate": conversion_series,
                "aov": aov_series
            }
        }

    except Exception as e:
        log.error(f"Error fetching metric timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/health/dashboard")
async def get_system_health_dashboard():
    """
    Consolidated system health dashboard.

    Returns a single view of:
    - Overall status (healthy/degraded/critical)
    - Connector sync health
    - Validation failure rates
    - Alert backlog (critical/high/unacked)
    - Recent sync success rate

    This is the "can I trust this system right now?" endpoint.
    """
    from app.models.base import SessionLocal
    from app.models.data_quality import TrackingAlert, ValidationFailure
    from app.models.analytics import DataSyncLog
    from sqlalchemy import func, case

    db = SessionLocal()
    try:
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)

        # ========== 1. CONNECTOR HEALTH ==========
        connectors = ['shopify', 'klaviyo', 'ga4', 'google_ads', 'merchant_center', 'search_console']
        connector_health = {}
        connectors_critical = 0
        connectors_warning = 0

        for connector in connectors:
            # Get latest sync
            latest = db.query(DataSyncLog).filter(
                DataSyncLog.source == connector
            ).order_by(DataSyncLog.started_at.desc()).first()

            if not latest:
                connector_health[connector] = {
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
                hours_stale = (now - latest_success.completed_at).total_seconds() / 3600

            # Count recent consecutive failures
            recent_logs = db.query(DataSyncLog).filter(
                DataSyncLog.source == connector
            ).order_by(DataSyncLog.started_at.desc()).limit(5).all()

            consecutive_failures = 0
            for log_entry in recent_logs:
                if log_entry.status == 'failed':
                    consecutive_failures += 1
                else:
                    break

            # Determine status
            if consecutive_failures >= 3 or (hours_stale and hours_stale > 48):
                status = 'critical'
                connectors_critical += 1
            elif consecutive_failures >= 1 or (hours_stale and hours_stale > 24):
                status = 'warning'
                connectors_warning += 1
            elif latest.status == 'success':
                status = 'healthy'
            else:
                status = 'unknown'

            connector_health[connector] = {
                'status': status,
                'last_sync': latest.completed_at.isoformat() if latest.completed_at else None,
                'last_status': latest.status,
                'hours_since_sync': round(hours_stale, 1) if hours_stale else None,
                'consecutive_failures': consecutive_failures
            }

        # ========== 2. SYNC SUCCESS RATE (last 24h) ==========
        sync_stats = db.query(
            DataSyncLog.status,
            func.count(DataSyncLog.id).label('count')
        ).filter(
            DataSyncLog.started_at >= last_24h
        ).group_by(DataSyncLog.status).all()

        total_syncs = sum(s.count for s in sync_stats)
        successful_syncs = sum(s.count for s in sync_stats if s.status in ['success', 'partial'])

        # Handle zero syncs case - don't falsely report 100% success
        if total_syncs == 0:
            sync_success_rate = None  # Unknown, not 100%
            no_syncs_24h = True
        else:
            sync_success_rate = round(successful_syncs / total_syncs * 100, 1)
            no_syncs_24h = False

        # ========== 3. VALIDATION FAILURE RATE ==========
        validation_stats = db.query(
            func.count(ValidationFailure.id).label('total'),
            func.sum(case((ValidationFailure.severity == 'error', 1), else_=0)).label('errors'),
            func.sum(case((ValidationFailure.severity == 'warning', 1), else_=0)).label('warnings')
        ).filter(
            ValidationFailure.created_at >= last_24h
        ).first()

        validation_failures = {
            'total': validation_stats.total or 0,
            'errors': validation_stats.errors or 0,
            'warnings': validation_stats.warnings or 0
        }

        # Get total records synced for failure rate calculation
        total_records = db.query(func.sum(DataSyncLog.records_processed)).filter(
            DataSyncLog.started_at >= last_24h
        ).scalar() or 0

        validation_failure_rate = (
            (validation_failures['errors'] / total_records * 100)
            if total_records > 0 else 0
        )

        # ========== 4. ALERT BACKLOG ==========
        alert_backlog = db.query(
            TrackingAlert.severity,
            func.count(TrackingAlert.id).label('count')
        ).filter(
            TrackingAlert.status.in_(['active', 'acknowledged', 'investigating'])
        ).group_by(TrackingAlert.severity).all()

        backlog = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        for row in alert_backlog:
            if row.severity in backlog:
                backlog[row.severity] = row.count

        total_unresolved = sum(backlog.values())

        # Recent alerts (last 24h)
        recent_alert_count = db.query(func.count(TrackingAlert.id)).filter(
            TrackingAlert.created_at >= last_24h
        ).scalar() or 0

        # ========== 5. DETERMINE OVERALL STATUS ==========
        # Note: sync_success_rate is None when no syncs occurred
        low_sync_success = sync_success_rate is not None and sync_success_rate < 80

        if connectors_critical > 0 or backlog['critical'] > 0:
            overall_status = 'critical'
            status_message = 'System has critical issues requiring immediate attention'
        elif connectors_warning > 1 or backlog['high'] > 2 or low_sync_success:
            overall_status = 'degraded'
            status_message = 'System is experiencing some issues but operational'
        elif no_syncs_24h:
            overall_status = 'warning'
            status_message = 'No data syncs in 24 hours - data freshness unknown'
        elif connectors_warning > 0 or backlog['high'] > 0 or validation_failure_rate > 5:
            overall_status = 'warning'
            status_message = 'Minor issues detected, monitoring recommended'
        else:
            overall_status = 'healthy'
            status_message = 'All systems operational'

        # can_trust_data is False if no syncs occurred (data freshness unknown)
        can_trust_data = overall_status in ['healthy', 'warning'] and not no_syncs_24h

        return {
            "timestamp": now.isoformat(),
            "overall_status": overall_status,
            "status_message": status_message,
            "can_trust_data": can_trust_data,

            "connector_health": {
                "summary": {
                    "healthy": len([c for c in connector_health.values() if c['status'] == 'healthy']),
                    "warning": connectors_warning,
                    "critical": connectors_critical,
                    "unknown": len([c for c in connector_health.values() if c['status'] == 'unknown'])
                },
                "connectors": connector_health
            },

            "sync_health": {
                "success_rate_24h": sync_success_rate,  # None if no syncs
                "total_syncs_24h": total_syncs,
                "successful_syncs_24h": successful_syncs,
                "failed_syncs_24h": total_syncs - successful_syncs,
                "no_syncs_warning": no_syncs_24h
            },

            "validation_health": {
                "failure_rate_24h": round(validation_failure_rate, 2),
                "total_failures_24h": validation_failures['total'],
                "blocking_errors_24h": validation_failures['errors'],
                "warnings_24h": validation_failures['warnings'],
                "total_records_processed_24h": total_records
            },

            "alert_backlog": {
                "total_unresolved": total_unresolved,
                "by_severity": backlog,
                "new_alerts_24h": recent_alert_count
            },

            "recommendations": _generate_health_recommendations(
                overall_status,
                connector_health,
                sync_success_rate,
                validation_failure_rate,
                backlog
            )
        }

    except Exception as e:
        log.error(f"Error fetching system health dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


def _generate_health_recommendations(
    overall_status: str,
    connector_health: Dict,
    sync_success_rate: Optional[float],  # None if no syncs
    validation_failure_rate: float,
    alert_backlog: Dict
) -> List[str]:
    """
    Generate actionable recommendations based on system health.
    """
    recommendations = []

    # Check connectors
    for connector, health in connector_health.items():
        if health['status'] == 'critical':
            if health.get('consecutive_failures', 0) >= 3:
                recommendations.append(
                    f"URGENT: {connector} has {health['consecutive_failures']} consecutive sync failures. "
                    f"Check API credentials and service status."
                )
            elif health.get('hours_since_sync') and health['hours_since_sync'] > 48:
                recommendations.append(
                    f"URGENT: {connector} data is {health['hours_since_sync']:.0f} hours stale. "
                    f"Investigate sync failures immediately."
                )
        elif health['status'] == 'warning':
            if health.get('hours_since_sync') and health['hours_since_sync'] > 24:
                recommendations.append(
                    f"WARNING: {connector} data is {health['hours_since_sync']:.0f} hours old. "
                    f"Consider triggering a manual sync."
                )

    # Check sync success rate
    if sync_success_rate is None:
        recommendations.append(
            "No data syncs have occurred in the last 24 hours. "
            "Data freshness cannot be verified. Check that sync jobs are running."
        )
    elif sync_success_rate < 80:
        recommendations.append(
            f"Sync success rate is only {sync_success_rate:.0f}%. "
            f"Review error logs and connector configurations."
        )
    elif sync_success_rate < 95:
        recommendations.append(
            f"Sync success rate is {sync_success_rate:.0f}%. "
            f"Some intermittent issues may be occurring."
        )

    # Check validation failures
    if validation_failure_rate > 10:
        recommendations.append(
            f"Data validation error rate is {validation_failure_rate:.1f}%. "
            f"Review data sources for quality issues."
        )
    elif validation_failure_rate > 5:
        recommendations.append(
            f"Elevated validation failure rate ({validation_failure_rate:.1f}%). "
            f"Monitor for patterns in failing records."
        )

    # Check alert backlog
    if alert_backlog['critical'] > 0:
        recommendations.append(
            f"{alert_backlog['critical']} critical alerts require immediate attention."
        )
    if alert_backlog['high'] > 3:
        recommendations.append(
            f"{alert_backlog['high']} high-priority alerts are pending review."
        )

    if not recommendations and overall_status == 'healthy':
        recommendations.append("All systems are operating normally. No action required.")

    return recommendations
