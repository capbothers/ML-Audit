"""
Data Quality & Tracking Validation API Endpoints

Validates data integrity across all sources.
Answers: "Can I trust this data?"
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
from sqlalchemy import func

from app.services.data_quality_service import DataQualityService
from app.services.llm_service import LLMService
from app.models.base import get_db, SessionLocal
from app.models.data_quality import ValidationFailure, DataSyncStatus
from app.models.shopify import ShopifyOrder, ShopifyOrderItem
from app.models.ga4_data import GA4DailySummary, GA4TrafficSource
from app.models.search_console_data import SearchConsoleQuery, SearchConsolePage
from app.models.google_ads_data import GoogleAdsCampaign, GoogleAdsAdGroup, GoogleAdsSearchTerm
from app.models.merchant_center_data import MerchantCenterProductStatus, MerchantCenterAccountStatus
from app.config import get_settings
from app.utils.logger import log

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


@router.get("/dashboard")
async def get_data_quality_dashboard(
    db = Depends(get_db)
):
    """
    Complete data quality dashboard

    Shows overall health across all data sources:
    - Sync status for each source
    - Conversion tracking accuracy
    - UTM parameter coverage
    - Merchant Center feed health
    - Google Ads/Analytics linking

    This is your single source of truth for "can I trust my data?"
    """
    service = DataQualityService(db)

    try:
        results = await service.run_full_data_quality_check()

        response = {
            "timestamp": results['timestamp'],
            "quality_score": results['quality_score'],
            "overall_health": results['overall_health'],

            "sync_health": results.get('sync_health'),
            "tracking_health": results.get('tracking_health'),
            "utm_health": results.get('utm_health'),
            "feed_health": results.get('feed_health'),
            "link_health": results.get('link_health'),

            "issues_found": len(results['issues_found']),
            "checks_run": results['checks_run'],

            "summary": {
                "score": results['quality_score'],
                "status": results['overall_health'],
                "issues": len(results['issues_found']),
                "message": f"Data quality score: {results['quality_score']}/100 ({results['overall_health']})"
            }
        }

        # Include freshness report inline
        freshness = _get_freshness_report(db)
        response['freshness'] = freshness
        if not freshness['all_fresh']:
            response['data_warning'] = {
                'warning': True,
                'stale_count': freshness['stale_count'],
                'empty_count': freshness['empty_count'],
                'message': f"{freshness['stale_count']} data source(s) are stale or empty",
                'stale_sources': freshness['stale_sources'],
                'empty_sources': freshness['empty_sources'],
            }

        return response

    except Exception as e:
        log.error(f"Error generating data quality dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-check")
async def run_manual_check(
    db = Depends(get_db)
):
    """
    Manually trigger a full data quality check

    Runs all validation checks and returns results
    """
    service = DataQualityService(db)

    try:
        results = await service.run_full_data_quality_check()

        return {
            "status": "check_completed",
            "timestamp": results['timestamp'],
            "quality_score": results['quality_score'],
            "overall_health": results['overall_health'],
            "issues_found": len(results['issues_found']),
            "results": results
        }

    except Exception as e:
        log.error(f"Error running data quality check: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync-status")
async def get_sync_status(
    db = Depends(get_db)
):
    """
    Get data sync status for all sources

    Shows:
    - Last successful sync for each source
    - Hours since last sync
    - Error counts
    - Sync health indicators

    Critical for knowing if data is flowing properly
    """
    service = DataQualityService(db)

    try:
        sync_health = await service.check_data_sync_health()

        return sync_health

    except Exception as e:
        log.error(f"Error getting sync status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discrepancies")
async def get_conversion_discrepancies(
    days: int = Query(7, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get conversion tracking discrepancies

    Compares Shopify (truth) vs GA4 vs Google Ads

    Critical for knowing if conversion tracking is accurate.
    If discrepancy > 10%, your Smart Bidding is optimizing on wrong data.
    """
    service = DataQualityService(db)

    try:
        tracking_health = await service.check_conversion_tracking(days=days)

        return tracking_health

    except Exception as e:
        log.error(f"Error checking conversion tracking: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/utm-health")
async def get_utm_health(
    days: int = Query(7, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get UTM parameter tracking health

    Shows:
    - % of sessions with UTM parameters
    - Coverage by traffic source
    - Missing UTM campaigns
    - Malformed UTM strings

    Low UTM coverage = incomplete attribution data
    """
    service = DataQualityService(db)

    try:
        utm_health = await service.check_utm_health(days=days)

        return utm_health

    except Exception as e:
        log.error(f"Error checking UTM health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/feed-health")
async def get_merchant_center_health(
    db = Depends(get_db)
):
    """
    Get Google Merchant Center feed health

    Shows:
    - Approved vs disapproved products
    - New disapprovals (critical - these just stopped showing)
    - Disapproval reasons
    - Feed processing status

    Disapproved products = lost Shopping impressions = lost revenue
    """
    service = DataQualityService(db)

    try:
        feed_health = await service.check_merchant_center_health()

        return feed_health

    except Exception as e:
        log.error(f"Error checking feed health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/link-health")
async def get_google_ads_link_health(
    db = Depends(get_db)
):
    """
    Get Google Ads / Analytics link health

    Verifies:
    - Auto-tagging enabled (GCLID in URLs)
    - GA4 linked to Google Ads
    - Conversion import working
    - Audience sharing enabled

    Broken linking = Smart Bidding can't optimize properly
    """
    service = DataQualityService(db)

    try:
        link_health = await service.check_google_ads_link_health()

        return link_health

    except Exception as e:
        log.error(f"Error checking link health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_active_alerts(
    db = Depends(get_db)
):
    """
    Get all active data quality alerts

    Returns alerts that need attention
    """
    service = DataQualityService(db)

    try:
        alerts = await service.get_active_alerts()

        return {
            "total_alerts": len(alerts),
            "alerts": alerts
        }

    except Exception as e:
        log.error(f"Error getting alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: int,
    db = Depends(get_db)
):
    """
    Acknowledge an alert

    Marks alert as seen/acknowledged
    """
    service = DataQualityService(db)

    try:
        result = await service.acknowledge_alert(alert_id)

        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])

        return result

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error acknowledging alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: int,
    resolution_notes: Optional[str] = None,
    db = Depends(get_db)
):
    """
    Resolve an alert

    Marks alert as resolved with optional notes
    """
    service = DataQualityService(db)

    try:
        result = await service.resolve_alert(alert_id, resolution_notes)

        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])

        return result

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error resolving alert: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-diagnosis")
async def get_llm_diagnosis(
    days: int = Query(7, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    LLM-Powered Data Quality Diagnosis

    Claude analyzes data quality issues and provides:
    - Diagnosis of why discrepancies exist
    - When the issue started
    - Probable cause
    - Specific fix recommendations
    - Expected impact

    This is the "why is this broken and how do I fix it?"
    """
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    service = DataQualityService(db)

    try:
        # Get full data quality check
        results = await service.run_full_data_quality_check()

        if not results.get('issues_found'):
            return {
                "message": "No data quality issues detected",
                "quality_score": results['quality_score'],
                "overall_health": results['overall_health']
            }

        # Generate LLM diagnosis
        diagnosis = llm_service.diagnose_data_quality_issues(
            quality_score=results['quality_score'],
            overall_health=results['overall_health'],
            issues=results['issues_found'],
            sync_health=results.get('sync_health'),
            tracking_health=results.get('tracking_health'),
            utm_health=results.get('utm_health'),
            feed_health=results.get('feed_health'),
            link_health=results.get('link_health')
        )

        return {
            "quality_score": results['quality_score'],
            "overall_health": results['overall_health'],
            "issues_count": len(results['issues_found']),

            "llm_diagnosis": diagnosis,

            "issues_summary": results['issues_found'][:5]  # Top 5 issues
        }

    except Exception as e:
        log.error(f"Error generating LLM diagnosis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discrepancy-diagnosis")
async def diagnose_tracking_discrepancy(
    days: int = Query(7, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Deep-dive diagnosis of conversion tracking discrepancies

    When Shopify vs GA4 vs Google Ads don't match:
    - When did the gap start?
    - What changed around that time?
    - What's the probable cause?
    - How to fix it?
    - What's the impact?

    This is critical when Smart Bidding is optimizing on wrong data
    """
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    service = DataQualityService(db)

    try:
        # Get conversion tracking data
        tracking = await service.check_conversion_tracking(days=days)

        if not tracking.get('has_discrepancies'):
            return {
                "message": "No significant tracking discrepancies detected",
                "health_status": tracking.get('health_status')
            }

        # Generate LLM diagnosis
        diagnosis = llm_service.diagnose_tracking_discrepancy(
            shopify_data=tracking.get('shopify'),
            ga4_data=tracking.get('ga4'),
            google_ads_data=tracking.get('google_ads'),
            discrepancies=tracking.get('discrepancies'),
            period_days=days
        )

        return {
            "period_days": days,
            "tracking_data": {
                "shopify": tracking.get('shopify'),
                "ga4": tracking.get('ga4'),
                "google_ads": tracking.get('google_ads')
            },

            "discrepancies": tracking.get('discrepancies'),
            "health_status": tracking.get('health_status'),

            "llm_diagnosis": diagnosis
        }

    except Exception as e:
        log.error(f"Error diagnosing tracking discrepancy: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validation/failures")
async def get_validation_failures(
    source: Optional[str] = Query(None, description="Filter by source (shopify, klaviyo, ga4, search_console)"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type (order, campaign, flow, etc.)"),
    failure_type: Optional[str] = Query(None, description="Filter by failure type (missing_required, invalid_type, etc.)"),
    severity: Optional[str] = Query(None, description="Filter by severity (error, warning)"),
    hours: int = Query(24, description="Hours of history to retrieve"),
    limit: int = Query(100, description="Maximum number of failures to return")
):
    """
    Get validation failures from data sync operations.

    Use this to:
    - Identify patterns of bad data from sources
    - Debug why records failed to save
    - Track data quality over time
    """
    db = SessionLocal()
    try:
        query = db.query(ValidationFailure)

        # Apply filters
        if source:
            query = query.filter(ValidationFailure.source == source)
        if entity_type:
            query = query.filter(ValidationFailure.entity_type == entity_type)
        if failure_type:
            query = query.filter(ValidationFailure.failure_type == failure_type)
        if severity:
            query = query.filter(ValidationFailure.severity == severity)

        # Time filter
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        query = query.filter(ValidationFailure.created_at >= cutoff)

        # Order and limit
        failures = query.order_by(ValidationFailure.created_at.desc()).limit(limit).all()

        return {
            "count": len(failures),
            "filters": {
                "source": source,
                "entity_type": entity_type,
                "failure_type": failure_type,
                "severity": severity,
                "hours": hours
            },
            "failures": [
                {
                    "id": f.id,
                    "sync_log_id": f.sync_log_id,
                    "source": f.source,
                    "entity_type": f.entity_type,
                    "entity_id": f.entity_id,
                    "field_name": f.field_name,
                    "failure_type": f.failure_type,
                    "failure_message": f.failure_message,
                    "raw_value": f.raw_value[:200] if f.raw_value else None,
                    "expected_format": f.expected_format,
                    "severity": f.severity,
                    "created_at": f.created_at.isoformat() if f.created_at else None
                }
                for f in failures
            ]
        }
    except Exception as e:
        log.error(f"Get validation failures error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/validation/summary")
async def get_validation_summary(
    hours: int = Query(24, description="Hours of history to analyze")
):
    """
    Get summary statistics of validation failures.

    Shows:
    - Failure rates by source
    - Most common failure types
    - Most problematic fields
    - Error vs warning breakdown

    Useful for identifying systemic data quality issues.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # Get total failures
        failures = db.query(ValidationFailure).filter(
            ValidationFailure.created_at >= cutoff
        ).all()

        if not failures:
            return {
                "period_hours": hours,
                "total_failures": 0,
                "message": "No validation failures in this period"
            }

        # Group by source
        by_source = {}
        by_failure_type = {}
        by_field = {}
        by_severity = {"error": 0, "warning": 0}

        for f in failures:
            # By source
            if f.source not in by_source:
                by_source[f.source] = {"total": 0, "entities": set()}
            by_source[f.source]["total"] += 1
            by_source[f.source]["entities"].add(f.entity_id)

            # By failure type
            if f.failure_type not in by_failure_type:
                by_failure_type[f.failure_type] = 0
            by_failure_type[f.failure_type] += 1

            # By field
            field_key = f"{f.source}.{f.entity_type}.{f.field_name}"
            if field_key not in by_field:
                by_field[field_key] = 0
            by_field[field_key] += 1

            # By severity
            if f.severity in by_severity:
                by_severity[f.severity] += 1

        # Convert sets to counts
        for source in by_source:
            by_source[source]["unique_entities"] = len(by_source[source]["entities"])
            del by_source[source]["entities"]

        # Sort by field to get top problematic fields
        top_fields = sorted(by_field.items(), key=lambda x: x[1], reverse=True)[:10]

        # Get total records processed and failed from sync logs to calculate actual failure rate
        from app.models.analytics import DataSyncLog
        sync_logs = db.query(DataSyncLog).filter(
            DataSyncLog.started_at >= cutoff
        ).all()
        total_records_processed = sum(entry.records_processed or 0 for entry in sync_logs)
        total_records_failed = sum(entry.records_failed or 0 for entry in sync_logs)

        # Calculate actual failure rate (failed records / total records)
        record_failure_rate = (
            total_records_failed / total_records_processed * 100
            if total_records_processed > 0 else 0
        )

        return {
            "period_hours": hours,
            "total_validation_issues": len(failures),
            "total_records_processed": total_records_processed,
            "total_records_failed": total_records_failed,
            "by_severity": by_severity,
            "by_source": by_source,
            "by_failure_type": by_failure_type,
            "top_problematic_fields": [
                {"field": field, "count": count}
                for field, count in top_fields
            ],
            # % of validation issues that are blocking errors (vs warnings)
            "blocking_error_percent": round(by_severity["error"] / len(failures) * 100, 2) if failures else 0,
            # % of all processed records that failed to save
            "record_failure_rate": round(record_failure_rate, 2)
        }
    except Exception as e:
        log.error(f"Get validation summary error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ── Data Freshness Audit ─────────────────────────────────────────────

# Stale thresholds (hours) per source type — how old data can be before it's stale
_STALE_THRESHOLDS = {
    'shopify_orders': 6,
    'shopify_order_items': 6,
    'ga4': 72,                  # GA4 has 24-48h processing delay
    'search_console_queries': 96,  # 3-day delay is normal
    'search_console_pages': 96,
    'google_ads_campaigns': 48,
    'google_ads_ad_groups': 48,
    'google_ads_search_terms': 48,
    'merchant_center': 48,
}


def _get_freshness_report(db) -> dict:
    """
    Build a freshness report by querying max dates in each key table.

    Returns a dict with per-source status, consumed by both the
    GET /data-quality/freshness endpoint and the stale-data warning banner.
    """
    now = datetime.utcnow()
    sources = {}

    # Helper: query max date column and format result
    def _check(label, model, date_col, threshold_hours):
        try:
            max_date = db.query(func.max(date_col)).scalar()
            row_count = db.query(func.count(model.id)).scalar() or 0
        except Exception:
            max_date = None
            row_count = 0

        if max_date is None:
            return {
                'max_date': None,
                'rows': row_count,
                'lag_hours': None,
                'stale': True,
                'status': 'empty',
                'threshold_hours': threshold_hours,
            }

        # Normalize to datetime
        if not isinstance(max_date, datetime):
            max_date = datetime.combine(max_date, datetime.min.time())

        lag = now - max_date
        lag_hours = round(lag.total_seconds() / 3600, 1)
        is_stale = lag_hours > threshold_hours

        return {
            'max_date': max_date.isoformat(),
            'rows': row_count,
            'lag_hours': lag_hours,
            'stale': is_stale,
            'status': 'stale' if is_stale else 'ok',
            'threshold_hours': threshold_hours,
        }

    sources['shopify_orders'] = _check(
        'shopify_orders', ShopifyOrder, ShopifyOrder.created_at, _STALE_THRESHOLDS['shopify_orders'])
    sources['shopify_order_items'] = _check(
        'shopify_order_items', ShopifyOrderItem, ShopifyOrderItem.order_date, _STALE_THRESHOLDS['shopify_order_items'])
    sources['ga4_daily_summary'] = _check(
        'ga4_daily_summary', GA4DailySummary, GA4DailySummary.date, _STALE_THRESHOLDS['ga4'])
    sources['ga4_traffic_sources'] = _check(
        'ga4_traffic_sources', GA4TrafficSource, GA4TrafficSource.date, _STALE_THRESHOLDS['ga4'])
    sources['search_console_queries'] = _check(
        'search_console_queries', SearchConsoleQuery, SearchConsoleQuery.date, _STALE_THRESHOLDS['search_console_queries'])
    sources['search_console_pages'] = _check(
        'search_console_pages', SearchConsolePage, SearchConsolePage.date, _STALE_THRESHOLDS['search_console_pages'])
    sources['google_ads_campaigns'] = _check(
        'google_ads_campaigns', GoogleAdsCampaign, GoogleAdsCampaign.date, _STALE_THRESHOLDS['google_ads_campaigns'])

    # In Sheets-only mode (google_ads_sheet_id is set), ad_groups and search_terms
    # are not imported — the Google Ads Sheets export only covers campaign-level and
    # product-level data. Skip these checks to avoid false "empty" warnings.
    _settings = get_settings()
    if not _settings.google_ads_sheet_id:
        # Direct API mode — ad_groups and search_terms should be populated
        sources['google_ads_ad_groups'] = _check(
            'google_ads_ad_groups', GoogleAdsAdGroup, GoogleAdsAdGroup.date, _STALE_THRESHOLDS['google_ads_ad_groups'])
        sources['google_ads_search_terms'] = _check(
            'google_ads_search_terms', GoogleAdsSearchTerm, GoogleAdsSearchTerm.date, _STALE_THRESHOLDS['google_ads_search_terms'])
    sources['merchant_center_statuses'] = _check(
        'merchant_center_statuses', MerchantCenterProductStatus, MerchantCenterProductStatus.snapshot_date, _STALE_THRESHOLDS['merchant_center'])

    stale_sources = [k for k, v in sources.items() if v['stale']]
    empty_sources = [k for k, v in sources.items() if v['status'] == 'empty']

    return {
        'checked_at': now.isoformat(),
        'all_fresh': len(stale_sources) == 0,
        'stale_count': len(stale_sources),
        'empty_count': len(empty_sources),
        'stale_sources': stale_sources,
        'empty_sources': empty_sources,
        'sources': sources,
    }


def get_stale_data_warning(db) -> Optional[dict]:
    """
    Returns a warning banner dict if any source is stale, or None if all are fresh.

    Call this from any dashboard endpoint to inject a warning header.
    """
    report = _get_freshness_report(db)
    if report['all_fresh']:
        return None

    messages = []
    for src in report['stale_sources']:
        info = report['sources'][src]
        if info['status'] == 'empty':
            messages.append(f"{src}: NO DATA (0 rows)")
        else:
            messages.append(f"{src}: {info['lag_hours']}h behind (threshold: {info['threshold_hours']}h)")

    return {
        'warning': True,
        'stale_count': report['stale_count'],
        'empty_count': report['empty_count'],
        'message': f"{report['stale_count']} data source(s) are stale or empty",
        'details': messages,
    }


@router.get("/freshness")
async def get_data_freshness(db=Depends(get_db)):
    """
    Data freshness audit.

    Queries max dates and row counts for every key table and flags stale sources.
    Use this to quickly see which data is behind and by how much.

    Returns per-source: max_date, rows, lag_hours, stale (bool), threshold_hours.
    """
    try:
        report = _get_freshness_report(db)
        return {
            "success": True,
            **report,
        }
    except Exception as e:
        log.error(f"Data freshness check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
