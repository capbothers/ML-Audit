"""
Ads Intelligence regression tests.

Guards against two bugs found during the 2026-02-15 audit:
1. CTR double-scaling (click_through_rate is 0-1 in DB, *100 in service, UI must NOT *100 again)
2. Period boundary drift (all analytics must anchor to latest ads data date, not utcnow())
"""
import asyncio
from pathlib import Path
from datetime import date, timedelta

from app.models.base import SessionLocal
from app.models.ad_spend import CampaignPerformance
from app.models.google_ads_data import GoogleAdsCampaign
from app.services.ad_spend_service import AdSpendService


def _run(coro):
    """Run an async coroutine in a sync test."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Test 1 — CTR rendering contract
# ---------------------------------------------------------------------------

def test_ctr_percent_contract():
    """
    CampaignPerformance.click_through_rate is stored as a decimal (0-1).
    get_campaign_performance() must return perf.ctr as a percentage (0-100).
    The frontend must display perf.ctr directly via fmtPct() — no further *100.

    This test guards against the 100x inflation bug where the value was
    multiplied by 100 twice (once in service, once in HTML).
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)

        # Pick a campaign that has a non-null click_through_rate
        campaign = (
            db.query(CampaignPerformance)
            .filter(CampaignPerformance.click_through_rate.isnot(None))
            .filter(CampaignPerformance.click_through_rate > 0)
            .first()
        )
        if not campaign:
            # No campaigns with CTR — skip gracefully
            return

        raw_ctr = float(campaign.click_through_rate)  # 0-1 decimal
        assert 0 < raw_ctr < 1, f"click_through_rate should be 0-1 decimal, got {raw_ctr}"

        # Service method scales it
        result = _run(svc.get_campaign_performance())
        match = next(
            (c for c in result if c['campaign_id'] == campaign.campaign_id),
            None,
        )
        assert match is not None, f"Campaign {campaign.campaign_id} not found in performance results"

        api_ctr = match['performance']['ctr']
        expected_ctr = round(raw_ctr * 100, 2)  # single *100
        assert api_ctr == expected_ctr, (
            f"API CTR should be raw*100={expected_ctr}, got {api_ctr}. "
            f"Double-scaling bug if {api_ctr} ≈ {raw_ctr * 10000}"
        )

        # Verify the HTML does NOT multiply perf.ctr by 100
        html = Path("app/static/ads_intelligence.html").read_text()
        assert "fmtPct(perf.ctr)" in html, (
            "Scorecard should render perf.ctr via fmtPct() directly (no *100)"
        )
        # deep_metrics.ctr IS a decimal and SHOULD be multiplied by 100
        assert "dm.metrics.ctr * 100" in html or "dm?.metrics?.ctr" in html, (
            "Deep metrics CTR fallback should multiply by 100 (it's a decimal)"
        )
    finally:
        db.close()


def test_deep_metrics_ctr_is_decimal():
    """
    get_campaign_deep_metrics() returns avg_ctr as a 0-1 decimal.
    The HTML multiplies it by 100 before calling fmtPct().
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        metrics = _run(svc.get_campaign_deep_metrics(days=30))
        if not metrics:
            return  # no campaigns

        for m in metrics:
            ctr = m['metrics']['avg_ctr']
            assert 0 <= ctr <= 1, (
                f"Deep metrics avg_ctr should be 0-1 decimal, "
                f"got {ctr} for campaign {m.get('campaign_name', '?')}"
            )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 2 — Period boundary contract
# ---------------------------------------------------------------------------

def test_ads_data_end_date_anchored_to_latest_row():
    """
    _get_ads_data_end_date() must return the max date from google_ads_campaigns,
    not datetime.utcnow().date(). This prevents trailing empty days from
    distorting trend calculations.
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)

        max_date_db = db.query(
            __import__('sqlalchemy').func.max(GoogleAdsCampaign.date)
        ).scalar()

        if max_date_db is None:
            return  # no ads data

        end_date = svc._get_ads_data_end_date()
        assert end_date == max_date_db, (
            f"_get_ads_data_end_date() returned {end_date}, "
            f"but latest row is {max_date_db}. "
            f"Possibly using utcnow() instead of data boundary."
        )
    finally:
        db.close()


def test_deep_metrics_window_bounded_by_data_end_date():
    """
    Deep metrics current-period window must end at _get_ads_data_end_date(),
    not at today. Verify by checking that no query reaches beyond the data end.
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        end_date = svc._get_ads_data_end_date()

        if end_date is None:
            return

        # The cutoff for a 30-day window should be end_date - 29 days
        expected_cutoff = end_date - timedelta(days=29)

        # Verify the method uses the correct boundary by checking
        # the source code doesn't contain utcnow() or datetime.now()
        source = Path("app/services/ad_spend_service.py").read_text()

        # All period methods should use _get_ads_data_end_date(), not utcnow()
        methods_to_check = [
            'get_campaign_deep_metrics',
            'calculate_health_scores',
            'analyze_diminishing_returns',
            'calculate_competitor_pressure',
            'detect_anomalies',
            'forecast_performance',
        ]

        for method_name in methods_to_check:
            # Find the method body
            start = source.find(f'def {method_name}')
            if start == -1:
                continue
            # Find the next def (approximate method boundary)
            next_def = source.find('\n    def ', start + 1)
            if next_def == -1:
                next_def = len(source)
            body = source[start:next_def]

            assert 'utcnow()' not in body, (
                f"{method_name}() still uses utcnow() — should use "
                f"_get_ads_data_end_date() for period boundary"
            )
            assert '_get_ads_data_end_date' in body or '_get_campaigns_for_period' in body, (
                f"{method_name}() should call _get_ads_data_end_date() or "
                f"_get_campaigns_for_period() for data-anchored windowing"
            )
    finally:
        db.close()


def test_type_comparison_ctr_is_decimal():
    """
    compare_campaign_types() returns avg_ctr as a 0-1 decimal.
    The HTML multiplies by 100: fmtPct((t.avg_ctr || 0) * 100).
    Guard against someone changing it to already-percent.
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        result = _run(svc.compare_campaign_types(days=30))
        types = result.get('types', []) if isinstance(result, dict) else result
        if not types:
            return

        for t in types:
            ctr = t.get('avg_ctr', 0)
            assert 0 <= ctr <= 1, (
                f"Type comparison avg_ctr should be 0-1 decimal, "
                f"got {ctr} for type {t.get('type', '?')}"
            )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 3 — Processor date anchoring
# ---------------------------------------------------------------------------

def test_processor_anchors_to_data_not_calendar():
    """
    AdSpendProcessor.process() must query max(GoogleAdsCampaign.date) to anchor
    period_end, not rely solely on date.today().
    """
    source = Path("app/services/ad_spend_processor.py").read_text()
    start = source.find('def process(')
    assert start != -1
    next_def = source.find('\n    def ', start + 1)
    body = source[start:next_def] if next_def != -1 else source[start:]

    assert 'GoogleAdsCampaign.date' in body or 'max(' in body, (
        "Processor must query max(GoogleAdsCampaign.date) to anchor "
        "period_end to actual data boundary"
    )
    # date.today() is acceptable only as a fallback (or pattern)
    assert 'period_end = date.today()' not in body, (
        "period_end must not be unconditionally set to date.today()"
    )


# ---------------------------------------------------------------------------
# Test 4 — Conversion rate scaling contract
# ---------------------------------------------------------------------------

def test_deep_metrics_conv_rate_is_decimal():
    """
    deep_metrics conv_rate is 0-1 decimal.
    Frontend must multiply by 100 before display.
    """
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        metrics = _run(svc.get_campaign_deep_metrics(days=30))
        if not metrics:
            return

        for m in metrics:
            cr = m['metrics']['conv_rate']
            assert 0 <= cr <= 1, (
                f"Deep metrics conv_rate should be 0-1 decimal, "
                f"got {cr} for campaign {m.get('campaign_name', '?')}"
            )

        # Frontend must multiply by 100
        html = Path("app/static/ads_intelligence.html").read_text()
        assert 'conv_rate * 100' in html or 'conv_rate *100' in html, (
            "Frontend must multiply conv_rate by 100 (it's a decimal)"
        )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 5 — Forecast trend semantics
# ---------------------------------------------------------------------------

def test_forecast_trend_values_handled_in_html():
    """
    Backend emits growing/declining/stable.
    Frontend must map these correctly (not just up/down).
    """
    html = Path("app/static/ads_intelligence.html").read_text()
    assert "'growing'" in html or '"growing"' in html, (
        "Frontend must handle 'growing' trend direction from backend"
    )
    assert "'declining'" in html or '"declining"' in html, (
        "Frontend must handle 'declining' trend direction from backend"
    )
