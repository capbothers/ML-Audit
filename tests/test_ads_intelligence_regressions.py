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
    assert 'period_end = date.today()' not in body, (
        "period_end must not be unconditionally set to date.today()"
    )
    # date.today() should not appear even as fallback — return early when no data
    assert 'date.today()' not in body, (
        "process() must return early when no data, not fall back to date.today()"
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


# ---------------------------------------------------------------------------
# Test 6 — Waste-conflict guardrail
# ---------------------------------------------------------------------------

def test_waste_conflict_downgrades_scale():
    """If is_wasting_budget=True, strategy_action cannot be scale_what_works."""
    db = SessionLocal()
    try:
        campaigns = db.query(CampaignPerformance).filter(
            CampaignPerformance.is_wasting_budget == True,
            CampaignPerformance.strategy_action.isnot(None),
        ).all()
        for c in campaigns:
            assert c.strategy_action != 'scale_what_works', (
                f"Campaign '{c.campaign_name}' is wasting budget but has "
                f"strategy_action='{c.strategy_action}' — should be investigate or lower"
            )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 7 — Anomaly sentiment
# ---------------------------------------------------------------------------

def test_anomaly_has_sentiment_field():
    """All anomalies must include a 'sentiment' field."""
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        anomalies = _run(svc.detect_anomalies(days=30))
        for a in anomalies:
            assert 'sentiment' in a, (
                f"Anomaly for {a.get('campaign_name')} / {a.get('metric')} "
                f"missing 'sentiment' field"
            )
            assert a['sentiment'] in ('positive', 'negative', 'neutral'), (
                f"Invalid sentiment '{a['sentiment']}' — must be positive/negative/neutral"
            )
    finally:
        db.close()


def test_anomaly_positive_sentiment():
    """CTR increase and CPC decrease must have 'positive' sentiment."""
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        anomalies = _run(svc.detect_anomalies(days=30))
        ctr_ups = [a for a in anomalies if a['metric'] == 'CTR' and a['change_pct'] > 0]
        for a in ctr_ups:
            assert a['sentiment'] == 'positive', (
                f"CTR increase should have positive sentiment, got {a['sentiment']}"
            )
        cpc_downs = [a for a in anomalies if a['metric'] == 'CPC' and a['change_pct'] < 0]
        for a in cpc_downs:
            assert a['sentiment'] == 'positive', (
                f"CPC decrease should have positive sentiment, got {a['sentiment']}"
            )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 8 — Product confidence gating
# ---------------------------------------------------------------------------

def test_products_spend_floor_200():
    """Profitable products must have at least $200 in ad spend."""
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        products = _run(svc.get_product_ad_performance(days=30))
        profitable = [p for p in products if p['indicators']['is_profitable']]
        for p in profitable:
            assert p['ad_spend']['total_spend'] >= 200, (
                f"Product '{p['product_title']}' has only ${p['ad_spend']['total_spend']:.0f} "
                f"spend — below minimum floor of $200"
            )
    finally:
        db.close()


def test_products_min_conversions_3():
    """Profitable products must have at least 3 conversions."""
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        products = _run(svc.get_product_ad_performance(days=30))
        profitable = [p for p in products if p['indicators']['is_profitable']]
        for p in profitable:
            assert p['performance']['conversions'] >= 3, (
                f"Product '{p['product_title']}' has only {p['performance']['conversions']} "
                f"conversions — below minimum floor of 3"
            )
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 9 — DR confidence gating
# ---------------------------------------------------------------------------

def test_dr_output_includes_confidence():
    """analyze_diminishing_returns() must emit dr_confidence, active_days, min_bucket_days."""
    db = SessionLocal()
    try:
        svc = AdSpendService(db)
        results = _run(svc.analyze_diminishing_returns(days=90))
        for r in results:
            assert 'dr_confidence' in r, f"Missing dr_confidence for {r['campaign_name']}"
            assert r['dr_confidence'] in ('high', 'medium', 'low')
            assert 'active_days' in r, f"Missing active_days for {r['campaign_name']}"
            assert 'min_bucket_days' in r, f"Missing min_bucket_days for {r['campaign_name']}"
            assert r['active_days'] >= 14, "DR should only include campaigns with ≥14 days"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Test 10 — Period window consistency
# ---------------------------------------------------------------------------

def test_period_windows_consistent():
    """
    Processor and service must use days-1 for inclusive date ranges.
    Verify source code does not use timedelta(days=days) with >= filters.
    """
    proc_src = Path("app/services/ad_spend_processor.py").read_text()
    # The processor's period_start line
    assert 'period_start = period_end - timedelta(days=days - 1)' in proc_src, (
        "Processor must use days-1 for inclusive period_start"
    )


# ---------------------------------------------------------------------------
# Test 11 — why_now / action coherence contract
# ---------------------------------------------------------------------------

def test_why_now_action_coherence_in_source():
    """
    Enhanced dashboard must regenerate why_now after DR overrides.
    Verify the post-override regeneration pass exists.
    """
    source = Path("app/api/ad_spend.py").read_text()
    assert 'original_action' in source, "DR override must track original_action"
    assert 'Regenerate why_now' in source or 'regenerate why_now' in source, (
        "Must have a why_now regeneration pass after overrides"
    )
    # Ensure format_why_now is imported for non-DR fallback
    assert 'from app.services.campaign_strategy import format_why_now' in source, (
        "format_why_now must be imported for fallback regeneration"
    )


# ---------------------------------------------------------------------------
# Test 12 — Causal Triage: demand signal
# ---------------------------------------------------------------------------

def test_triage_demand_signal():
    """SC clicks down 30% → primary_cause='demand'."""
    from unittest.mock import MagicMock, patch
    from app.services.causal_triage import CausalTriageService

    mock_db = MagicMock()

    svc = CausalTriageService(mock_db)

    # Mock _score_demand to return high score
    with patch.object(svc, '_score_demand', return_value={'cause': 'demand', 'score': 0.8, 'evidence': 'SC clicks -30%'}), \
         patch.object(svc, '_score_auction_pressure', return_value={'cause': 'auction_pressure', 'score': 0.1, 'evidence': 'Stable'}), \
         patch.object(svc, '_score_landing_page', return_value={'cause': 'landing_page', 'score': 0.0, 'evidence': 'LP OK', 'cvr_change': 0, 'bounce_change': 0}), \
         patch.object(svc, '_score_attribution', return_value={'cause': 'attribution_lag', 'score': 0.0, 'evidence': 'OK'}), \
         patch.object(svc, '_score_catalog_feed', return_value={'cause': 'catalog_feed', 'score': 0.0, 'evidence': 'Feed OK'}), \
         patch.object(svc, '_score_measurement', return_value={'cause': 'measurement', 'score': 0.0, 'evidence': 'Fresh'}):
        result = svc.diagnose('campaign_1', date(2026, 1, 1), date(2026, 1, 30))

    assert result['primary_cause'] == 'demand', f"Expected demand, got {result['primary_cause']}"
    assert result['confidence'] > 0.5, f"Expected high confidence, got {result['confidence']}"


def test_triage_auction_pressure():
    """Rank-lost IS up 10pp → primary_cause='auction_pressure'."""
    from unittest.mock import MagicMock, patch
    from app.services.causal_triage import CausalTriageService

    mock_db = MagicMock()
    svc = CausalTriageService(mock_db)

    with patch.object(svc, '_score_demand', return_value={'cause': 'demand', 'score': 0.1, 'evidence': 'Stable'}), \
         patch.object(svc, '_score_auction_pressure', return_value={'cause': 'auction_pressure', 'score': 0.75, 'evidence': 'Rank-lost IS +10pp'}), \
         patch.object(svc, '_score_landing_page', return_value={'cause': 'landing_page', 'score': 0.0, 'evidence': 'LP OK', 'cvr_change': 0, 'bounce_change': 0}), \
         patch.object(svc, '_score_attribution', return_value={'cause': 'attribution_lag', 'score': 0.0, 'evidence': 'OK'}), \
         patch.object(svc, '_score_catalog_feed', return_value={'cause': 'catalog_feed', 'score': 0.0, 'evidence': 'Feed OK'}), \
         patch.object(svc, '_score_measurement', return_value={'cause': 'measurement', 'score': 0.0, 'evidence': 'Fresh'}):
        result = svc.diagnose('campaign_1', date(2026, 1, 1), date(2026, 1, 30))

    assert result['primary_cause'] == 'auction_pressure'


def test_triage_landing_page():
    """LP CVR down 25% → primary_cause='landing_page'."""
    from unittest.mock import MagicMock, patch
    from app.services.causal_triage import CausalTriageService

    mock_db = MagicMock()
    svc = CausalTriageService(mock_db)

    with patch.object(svc, '_score_demand', return_value={'cause': 'demand', 'score': 0.1, 'evidence': 'Stable'}), \
         patch.object(svc, '_score_auction_pressure', return_value={'cause': 'auction_pressure', 'score': 0.0, 'evidence': 'Stable'}), \
         patch.object(svc, '_score_landing_page', return_value={'cause': 'landing_page', 'score': 0.85, 'evidence': 'CVR -25%', 'cvr_change': -0.25, 'bounce_change': 0.1}), \
         patch.object(svc, '_score_attribution', return_value={'cause': 'attribution_lag', 'score': 0.0, 'evidence': 'OK'}), \
         patch.object(svc, '_score_catalog_feed', return_value={'cause': 'catalog_feed', 'score': 0.0, 'evidence': 'Feed OK'}), \
         patch.object(svc, '_score_measurement', return_value={'cause': 'measurement', 'score': 0.0, 'evidence': 'Fresh'}):
        result = svc.diagnose('campaign_1', date(2026, 1, 1), date(2026, 1, 30))

    assert result['primary_cause'] == 'landing_page'


# ---------------------------------------------------------------------------
# Test 13 — Attribution confidence tiers
# ---------------------------------------------------------------------------

def test_attribution_confidence_tiers():
    """gclid match >50% → 'high', 20-50% → 'medium', <20% → 'low'."""
    source = Path("app/services/ad_spend_processor.py").read_text()
    assert 'attr_confidence' in source, "Processor must compute attribution confidence"
    assert "conv_ratio >= 0.5" in source, "High tier should be >= 0.5"
    assert "conv_ratio >= 0.2" in source, "Medium tier should be >= 0.2"


def test_low_attribution_blocks_review():
    """Low attr conf + review → investigate (in processor source)."""
    source = Path("app/services/ad_spend_processor.py").read_text()
    # The attribution gate should exist
    assert "attr_confidence == 'low'" in source, "Attribution gate missing in processor"
    assert "'review', 'pause'" in source or "('review', 'pause')" in source, (
        "Attribution gate must block review/pause actions"
    )


# ---------------------------------------------------------------------------
# Test 14 — LP friction overrides reduce
# ---------------------------------------------------------------------------

def test_lp_friction_overrides_review():
    """When LP CVR drops and action is review → fix (arbitrator rule 2)."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    campaign = {
        'true_metrics': {'true_roas': 1.8},
    }
    evidence = {
        'strategy': {
            'action': 'review',
            'confidence': 'medium',
            'type': 'fast_turn',
            'decision_score': 35,
            'short_term_status': 'Underperforming',
            'strategic_value': 'Low Value',
        },
        'diminishing_returns': None,
        'causal_triage': {
            'primary_cause': 'landing_page',
            'confidence': 0.8,
            'causes': [
                {'cause': 'landing_page', 'score': 0.85, 'evidence': 'CVR -30%'},
                {'cause': 'demand', 'score': 0.1, 'evidence': 'Stable'},
            ],
        },
        'attribution': {'confidence': 'high', 'gap_pct': 10},
        'waste': {'is_wasting': False},
    }

    result = arbitrator.arbitrate(campaign, evidence)
    assert result['final_action'] == 'fix', (
        f"LP friction should override review to fix, got {result['final_action']}"
    )
    assert any(o['module'] == 'landing_page' for o in result['overrides'])


# ---------------------------------------------------------------------------
# Test 15 — Arbitrator: profitability protection
# ---------------------------------------------------------------------------

def test_arbitrator_profitability_protection():
    """High-conf profitable + low-conf negative → no downgrade below maintain."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    campaign = {
        'true_metrics': {'true_roas': 5.0},
    }
    evidence = {
        'strategy': {
            'action': 'review',  # incorrectly classified
            'confidence': 'high',
            'type': 'fast_turn',
            'decision_score': 65,
            'short_term_status': 'Performing',
            'strategic_value': 'High Value',
        },
        'diminishing_returns': None,
        'causal_triage': None,
        'attribution': {'confidence': 'high', 'gap_pct': 5},
        'waste': {'is_wasting': False},
    }

    result = arbitrator.arbitrate(campaign, evidence)
    assert result['final_action'] == 'maintain', (
        f"Profitable campaign at 5x ROAS should floor at maintain, got {result['final_action']}"
    )
    assert any(o['module'] == 'profitability_protection' for o in result['overrides'])


# ---------------------------------------------------------------------------
# Test 16 — Arbitrator: high-conf DR downgrades scale
# ---------------------------------------------------------------------------

def test_arbitrator_high_conf_dr_downgrades_scale():
    """High-conf DR + scale_what_works → investigate."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    # Use medium confidence so profitability protection doesn't override the DR finding
    campaign = {
        'true_metrics': {'true_roas': 3.5},
    }
    evidence = {
        'strategy': {
            'action': 'scale_what_works',
            'confidence': 'medium',
            'type': 'high_consideration',
            'decision_score': 80,
            'short_term_status': 'Performing',
            'strategic_value': 'High Value',
        },
        'diminishing_returns': {
            'overspend_per_day': 120,
            'optimal_daily_spend': 200,
            'current_daily_spend': 320,
            'dr_confidence': 'high',
            'active_days': 28,
        },
        'causal_triage': None,
        'attribution': {'confidence': 'high', 'gap_pct': 5},
        'waste': {'is_wasting': False},
    }

    result = arbitrator.arbitrate(campaign, evidence)
    assert result['final_action'] == 'investigate', (
        f"High-conf DR overspend should downgrade scale_what_works to investigate, got {result['final_action']}"
    )
    assert any(o['module'] == 'diminishing_returns' for o in result['overrides'])


# ---------------------------------------------------------------------------
# Test 17 — Arbitrator: why_now matches final action
# ---------------------------------------------------------------------------

def test_arbitrator_why_now_matches_final_action():
    """why_now must be generated from the final action, not the original."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    # Use medium confidence to prevent profitability protection from overriding DR
    campaign = {'true_metrics': {'true_roas': 4.0}}
    evidence = {
        'strategy': {
            'action': 'scale_what_works',
            'confidence': 'medium',
            'type': 'fast_turn',
            'decision_score': 85,
            'short_term_status': 'Performing',
            'strategic_value': 'High Value',
        },
        'diminishing_returns': {
            'overspend_per_day': 80,
            'optimal_daily_spend': 150,
            'current_daily_spend': 230,
            'dr_confidence': 'high',
            'active_days': 25,
        },
        'causal_triage': None,
        'attribution': {'confidence': 'medium', 'gap_pct': 5},
        'waste': {'is_wasting': False},
    }

    result = arbitrator.arbitrate(campaign, evidence)
    assert result['final_action'] == 'investigate'
    # why_now should NOT mention budget prescriptions
    why = result['why_now'] or ''
    assert 'Scale budget' not in why, f"why_now has budget prescription: {why}"
    assert 'Increase budget' not in why, f"why_now has budget prescription: {why}"
    assert 'Cut budget' not in why, f"why_now has budget prescription: {why}"


# ---------------------------------------------------------------------------
# Test 18 — Feedback snapshot model
# ---------------------------------------------------------------------------

def test_snapshot_model_has_required_fields():
    """DecisionSnapshot must have all required fields for feedback loop."""
    from app.models.decision_feedback import DecisionSnapshot
    required = [
        'campaign_id', 'campaign_name', 'strategy_type', 'decided_at',
        'action', 'confidence', 'decision_score', 'primary_cause', 'why_now',
        'true_roas', 'total_spend', 'true_profit',
        'outcome_7d_roas', 'outcome_7d_profit', 'outcome_30d_roas', 'outcome_30d_profit',
        'outcome_verdict', 'outcome_scored_at',
        'user_action', 'user_override_to', 'user_feedback_at',
    ]
    for field in required:
        assert hasattr(DecisionSnapshot, field), f"DecisionSnapshot missing field: {field}"


# ---------------------------------------------------------------------------
# Test 19 — Feedback API endpoint exists
# ---------------------------------------------------------------------------

def test_feedback_endpoint_exists():
    """POST /ads/feedback/{campaign_id} must exist."""
    source = Path("app/api/ad_spend.py").read_text()
    assert '/feedback/{campaign_id}' in source, "Missing feedback endpoint"
    assert 'record_feedback' in source, "Missing record_feedback call"


# ---------------------------------------------------------------------------
# Test 20 — Decision Arbitrator uses all evidence modules
# ---------------------------------------------------------------------------

def test_arbitrator_uses_enhanced_dashboard():
    """Enhanced dashboard must use DecisionArbitrator, not ad-hoc overrides."""
    source = Path("app/api/ad_spend.py").read_text()
    assert 'DecisionArbitrator' in source, (
        "Enhanced dashboard must use DecisionArbitrator"
    )
    assert 'arbitrator.arbitrate' in source, (
        "Enhanced dashboard must call arbitrator.arbitrate()"
    )


# ---------------------------------------------------------------------------
# Test 21 — New diagnostic action vocabulary
# ---------------------------------------------------------------------------

def test_new_action_vocabulary():
    """campaign_strategy.py must use diagnostic vocabulary: scale_what_works, fix, review."""
    source = Path("app/services/campaign_strategy.py").read_text()
    assert "'scale_what_works'" in source, "Missing scale_what_works in strategy"
    assert "'fix'" in source, "Missing fix action in strategy"
    assert "'review'" in source, "Missing review action in strategy"
    # Old budget-prescriptive actions must NOT exist
    assert "'scale_aggressively'" not in source, "Old scale_aggressively action still present"
    assert "'optimize'" not in source, "Old optimize action still present"


# ---------------------------------------------------------------------------
# Test 22 — Scheduler has outcome scoring jobs
# ---------------------------------------------------------------------------

def test_scheduler_has_outcome_scoring_jobs():
    """Scheduler must have 7d and 30d outcome scoring jobs."""
    source = Path("app/scheduler.py").read_text()
    assert 'score_decision_outcomes_7d' in source, "Missing 7d outcome scoring job"
    assert 'score_decision_outcomes_30d' in source, "Missing 30d outcome scoring job"
    assert 'decision_outcomes_7d' in source, "Missing job ID for 7d scoring"
    assert 'decision_outcomes_30d' in source, "Missing job ID for 30d scoring"


# ---------------------------------------------------------------------------
# Test 23 — No budget prescriptions in templates
# ---------------------------------------------------------------------------

def test_no_budget_prescriptions_in_templates():
    """WHY_NOW_TEMPLATES must not contain budget-prescriptive language."""
    source = Path("app/services/campaign_strategy.py").read_text()
    # Extract only the templates section
    start = source.find('_WHY_NOW_TEMPLATES')
    end = source.find('\n}', start)
    templates = source[start:end] if start != -1 else ''
    for phrase in ['Scale budget', 'Cut budget', 'Increase budget', 'Reduce 50%', 'Scale 50%', 'Scale 25%']:
        assert phrase not in templates, (
            f"Budget prescription '{phrase}' found in WHY_NOW_TEMPLATES"
        )


# ---------------------------------------------------------------------------
# Test 24 — DR notes never prescribe budget targets
# ---------------------------------------------------------------------------

def test_dr_no_budget_target():
    """DR override text must not contain 'set to' or 'reduce to' budget prescriptions."""
    from app.services.decision_arbitration import DecisionArbitrator
    arbitrator = DecisionArbitrator()
    campaign = {'true_metrics': {'true_roas': 4.0}}
    evidence = {
        'strategy': {
            'action': 'scale_what_works', 'confidence': 'medium',
            'type': 'fast_turn', 'decision_score': 80,
            'short_term_status': 'strong', 'strategic_value': 'high',
        },
        'diminishing_returns': {
            'overspend_per_day': 100, 'optimal_daily_spend': 150,
            'current_daily_spend': 250, 'dr_confidence': 'high', 'active_days': 30,
        },
        'causal_triage': None,
        'attribution': {'confidence': 'high'},
        'waste': {'is_wasting': False},
    }
    result = arbitrator.arbitrate(campaign, evidence)
    why = result['why_now'] or ''
    for phrase in ['set to $', 'reduce to $', 'increase to $', 'Set to $', 'Reduce to $']:
        assert phrase not in why, f"DR why_now contains budget target: {why}"


# ---------------------------------------------------------------------------
# Test 25 — Diagnostics service exists and has required analyzers
# ---------------------------------------------------------------------------

def test_diagnostics_service_structure():
    """CampaignDiagnosticsService must have all 5 analyzers + scoping."""
    source = Path("app/services/campaign_diagnostics.py").read_text()
    assert 'class CampaignDiagnosticsService' in source
    assert '_analyze_search_terms' in source, "Missing search terms analyzer"
    assert '_analyze_landing_pages' in source, "Missing landing pages analyzer"
    assert '_analyze_feed' in source, "Missing feed analyzer"
    assert '_analyze_device' in source, "Missing device analyzer"
    assert '_analyze_auction' in source, "Missing auction analyzer"
    assert 'diagnose_all' in source, "Missing diagnose_all method"
    # Scoping: must have campaign-aware data fetchers
    assert '_build_product_campaign_bridge' in source, "Missing product-campaign bridge"
    assert '_batch_product_revenue' in source, "Missing batch product revenue"
    assert '_extract_brand_keywords' in source, "Missing brand keyword extraction"
    assert '_normalize_mc_product_id' in source, "Missing MC product_id normalizer"
    assert 'has_blockers' in source, "Missing has_blockers flag"
    assert '[site-wide]' in source, "Missing [site-wide] label for global signals"


# ---------------------------------------------------------------------------
# Test 26 — Diagnostics integrated into enhanced dashboard
# ---------------------------------------------------------------------------

def test_diagnostics_in_enhanced_dashboard():
    """Enhanced dashboard must call CampaignDiagnosticsService."""
    source = Path("app/api/ad_spend.py").read_text()
    assert 'CampaignDiagnosticsService' in source, (
        "Enhanced dashboard must use CampaignDiagnosticsService"
    )
    assert 'diagnose_all' in source, (
        "Enhanced dashboard must call diagnose_all()"
    )
    assert "'diagnostics'" in source or '"diagnostics"' in source, (
        "Campaigns must have diagnostics key attached"
    )


# ---------------------------------------------------------------------------
# Test 27 — Diagnostics search term analyzer works
# ---------------------------------------------------------------------------

def test_diagnostics_search_terms():
    """Search term analyzer identifies non-converting terms with $ impact."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    terms = [
        {'term': 'billi tap', 'clicks': 50, 'impressions': 500, 'cost': 200, 'conversions': 5, 'conv_value': 1000},
        {'term': 'repair manual', 'clicks': 30, 'impressions': 300, 'cost': 120, 'conversions': 0, 'conv_value': 0},
        {'term': 'how to fix', 'clicks': 20, 'impressions': 200, 'cost': 80, 'conversions': 0, 'conv_value': 0},
        {'term': 'buy billi', 'clicks': 10, 'impressions': 100, 'cost': 40, 'conversions': 2, 'conv_value': 400},
    ]

    svc._analyze_search_terms(diag, terms, campaign_spend=500)

    # Should identify converting terms as working
    assert any('billi tap' in w for w in diag['working']), "Should identify top converting terms"
    # Should show revenue in working
    assert any('$' in w for w in diag['working']), "Working terms should show $ revenue"
    # Should identify non-converting spend as not working
    assert any('non-converting' in n for n in diag['not_working']), "Should flag wasted spend"
    # Should suggest negatives
    assert any('repair manual' in a for a in diag['actions']), "Should suggest negatives for wasted terms"


# ---------------------------------------------------------------------------
# Test 28 — Arbitrator uses new vocabulary
# ---------------------------------------------------------------------------

def test_arbitrator_new_vocabulary():
    """Arbitrator must use new action names: scale_what_works, fix, review."""
    source = Path("app/services/decision_arbitration.py").read_text()
    assert "'scale_what_works'" in source, "Arbitrator missing scale_what_works"
    assert "'fix'" in source, "Arbitrator missing fix action"
    assert "'review'" in source, "Arbitrator missing review action"
    # Old actions should NOT be referenced
    assert "'scale_aggressively'" not in source, "Arbitrator still has scale_aggressively"
    assert "'optimize'" not in source, "Arbitrator still has optimize"
    assert "'reduce'" not in source, "Arbitrator still has reduce"


# ---------------------------------------------------------------------------
# Test 29 — Frontend uses new action labels
# ---------------------------------------------------------------------------

def test_frontend_new_action_labels():
    """Frontend must use new diagnostic action labels."""
    html = Path("app/static/ads_intelligence.html").read_text()
    assert 'scale_what_works' in html, "Frontend missing scale_what_works label"
    assert "Scale 50%" not in html, "Frontend still has old 'Scale 50%' label"
    assert "Reduce 50%" not in html, "Frontend still has old 'Reduce 50%' label"
    assert "Scale 25%" not in html, "Frontend still has old 'Scale 25%' label"
    # Should have diagnostics section
    assert "What\\'s Working" in html or "What's Working" in html, "Missing Working section"
    assert "What\\'s Not Working" in html or "What's Not Working" in html, "Missing Not Working section"


# ---------------------------------------------------------------------------
# Test 30 — Diagnostics campaign scoping
# ---------------------------------------------------------------------------

def test_diagnostics_campaign_scoped_feed():
    """Feed analyzer must scope disapprovals to campaign's advertised products."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService, _normalize_mc_product_id
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    feed_issues = {
        'disapproved': [
            {'product_id': 'online:en:AU:shopify_AU_111_222', 'offer_id': None,
             'title': 'Product A', 'issue_code': 'missing_gtin', 'description': 'GTIN missing',
             'normalized_pid': 'shopify_au_111_222'},
            {'product_id': 'online:en:AU:shopify_AU_333_444', 'offer_id': None,
             'title': 'Product B', 'issue_code': 'image_too_small', 'description': 'Image issue',
             'normalized_pid': 'shopify_au_333_444'},
        ],
        'count': 2,
    }
    # Only Product A is advertised in campaign_1
    mc_to_campaigns = {
        'shopify_au_111_222': {'campaign_1'},
        'shopify_au_333_444': {'campaign_2'},
    }
    product_revenue = {
        'shopify_au_111_222': {'title': 'Product A', 'revenue': 5000, 'cost': 500, 'conversions': 10},
    }

    svc._analyze_feed(diag, feed_issues, 'campaign_1', mc_to_campaigns, product_revenue, 1000)

    # Should only flag Product A (campaign_1's product), not Product B
    assert len(diag['not_working']) == 1, f"Expected 1 issue, got {len(diag['not_working'])}"
    assert 'Product A' in diag['actions'][0] or 'missing_gtin' in diag['not_working'][0]
    assert 'Product B' not in str(diag), "Product B should not appear for campaign_1"
    # Should show revenue at risk
    assert '$5,000' in diag['not_working'][0], f"Should show revenue at risk: {diag['not_working']}"
    assert diag['has_blockers'] is True


# ---------------------------------------------------------------------------
# Test 31 — Diagnostics LP scoping via brand keywords
# ---------------------------------------------------------------------------

def test_diagnostics_lp_brand_scoping():
    """LP analyzer scopes to campaign's brand keywords when available."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService, _extract_brand_keywords
    from unittest.mock import MagicMock

    # Test keyword extraction
    assert 'franke' in _extract_brand_keywords('PM-AU Franke')
    assert 'zip' in _extract_brand_keywords('PM-AU Zip Taps')
    assert 'billi' in _extract_brand_keywords('PM-AU Billi')
    assert _extract_brand_keywords('PM-AU Hardware All') == []  # all generic words

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    lp_health = {
        'prev': {
            '/collections/franke': {'sessions': 100, 'conversions': 10, 'cvr': 0.10, 'bounce': 0.40},
            '/collections/zip-taps': {'sessions': 100, 'conversions': 10, 'cvr': 0.10, 'bounce': 0.40},
        },
        'curr': {
            '/collections/franke': {'sessions': 120, 'conversions': 5, 'cvr': 0.042, 'bounce': 0.55},
            '/collections/zip-taps': {'sessions': 120, 'conversions': 12, 'cvr': 0.10, 'bounce': 0.38},
        },
        'cwv': {},
        'total_sessions': 500,
    }

    # For a Franke campaign, should flag /collections/franke
    svc._analyze_landing_pages(diag, lp_health, ['franke'], 'PM-AU Franke')
    assert any('/collections/franke' in n for n in diag['not_working']), (
        f"Should flag franke LP for Franke campaign, got: {diag['not_working']}"
    )
    # Should NOT flag zip-taps LP for Franke campaign
    assert not any('zip-taps' in n for n in diag['not_working']), (
        "Should NOT flag zip-taps LP for Franke campaign"
    )
    # Site-wide label should NOT appear since we matched brand
    assert not any('[site-wide]' in n for n in diag['not_working']), (
        "Scoped LP issue should not have [site-wide] label"
    )


# ---------------------------------------------------------------------------
# Test 31b — LP fallback avoids repeated tiny site-wide messages
# ---------------------------------------------------------------------------

def test_diagnostics_lp_fallback_suppresses_tiny_sitewide():
    """Fallback LP analyzer should skip tiny site-wide issues to reduce repetition."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    # No brand keywords => site-wide fallback mode.
    # Franke page has tiny share (1%) and low lost convs (<2), so it should be suppressed.
    lp_health = {
        'prev': {
            '/collections/franke': {'sessions': 20, 'conversions': 2, 'cvr': 0.10, 'bounce': 0.40},
            '/collections/zip': {'sessions': 400, 'conversions': 12, 'cvr': 0.03, 'bounce': 0.40},
        },
        'curr': {
            '/collections/franke': {'sessions': 20, 'conversions': 0, 'cvr': 0.00, 'bounce': 0.80},
            '/collections/zip': {'sessions': 400, 'conversions': 14, 'cvr': 0.035, 'bounce': 0.39},
        },
        'cwv': {},
        'total_sessions': 2000,
    }

    svc._analyze_landing_pages(diag, lp_health, [], 'PMAX Zombie Campaign')

    assert not any('/collections/franke' in n for n in diag['not_working']), (
        f"Tiny site-wide LP issue should be suppressed, got: {diag['not_working']}"
    )


# ---------------------------------------------------------------------------
# Test 32 — Global signals labelled [site-wide]
# ---------------------------------------------------------------------------

def test_diagnostics_global_signals_labelled():
    """Device gaps and fallback LP issues must be labelled [site-wide]."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    device_gaps = {
        'desktop': {'sessions': 5000, 'conversions': 100, 'cvr': 0.02, 'bounce': 0.4},
        'mobile': {'sessions': 8000, 'conversions': 40, 'cvr': 0.005, 'bounce': 0.6},
    }
    svc._analyze_device(diag, device_gaps, 50)

    assert any('[site-wide]' in n for n in diag['not_working']), (
        f"Device gap must be labelled [site-wide], got: {diag['not_working']}"
    )
    assert any('[site-wide]' in a for a in diag['actions']), (
        f"Device action must be labelled [site-wide], got: {diag['actions']}"
    )
    # Should include conversion impact estimate
    assert any('conversions' in n for n in diag['not_working']), (
        "Device gap should mention conversions lost"
    )


# ---------------------------------------------------------------------------
# Test 33 — DR text has no budget-target framing
# ---------------------------------------------------------------------------

def test_dr_text_no_budget_target_framing():
    """DR text must not contain $/day, optimal, or budget-target language."""
    source = Path("app/services/decision_arbitration.py").read_text()
    # Extract DR override section
    start = source.find('Rule 4: Diminishing returns')
    end = source.find('Rule 5:', start) if start != -1 else -1
    dr_section = source[start:end] if start != -1 and end != -1 else ''

    for phrase in ['optimal ~$', 'overspend ~$', '/day', 'set to $', 'reduce to $']:
        assert phrase not in dr_section, (
            f"DR section still contains budget-target text: '{phrase}'"
        )

    # Also check conflict_note in ad_spend.py
    api_source = Path("app/api/ad_spend.py").read_text()
    cn_start = api_source.find('conflict_note')
    cn_end = api_source.find('\n\n', cn_start) if cn_start != -1 else -1
    cn_section = api_source[cn_start:cn_end] if cn_start != -1 and cn_end != -1 else ''

    for phrase in ['$/day', 'over optimal', 'optimal)', '$overspend']:
        assert phrase not in cn_section, (
            f"conflict_note still contains budget-target text: '{phrase}'"
        )


# ---------------------------------------------------------------------------
# Test 34 — Blocker precedence prevents Scale
# ---------------------------------------------------------------------------

def test_blocker_precedence_prevents_scale():
    """When diagnostics.has_blockers is True, action cannot be scale_what_works."""
    source = Path("app/api/ad_spend.py").read_text()
    assert 'blocker_precedence' in source, (
        "Missing blocker_precedence rule in enhanced dashboard"
    )
    assert "has_blockers" in source, (
        "Missing has_blockers check in enhanced dashboard"
    )
    # Verify the rule downgrades to fix, not maintain or investigate
    assert "'to_action': 'fix'" in source or '"to_action": "fix"' in source, (
        "Blocker precedence should downgrade scale_what_works to fix"
    )


# ---------------------------------------------------------------------------
# Test 35 — Impact ranking: $ at risk in feed issues
# ---------------------------------------------------------------------------

def test_feed_impact_ranking_with_revenue():
    """Feed issues sorted by revenue impact, showing $ at risk."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    feed_issues = {
        'disapproved': [
            {'product_id': 'p1', 'offer_id': None, 'title': 'Low Rev Product',
             'issue_code': 'missing_gtin', 'description': 'GTIN', 'normalized_pid': 'p1'},
            {'product_id': 'p2', 'offer_id': None, 'title': 'High Rev Product',
             'issue_code': 'image_issue', 'description': 'Image', 'normalized_pid': 'p2'},
        ],
        'count': 2,
    }
    mc_to_campaigns = {'p1': {'c1'}, 'p2': {'c1'}}
    product_revenue = {
        'p1': {'title': 'Low Rev Product', 'revenue': 100, 'cost': 50, 'conversions': 1},
        'p2': {'title': 'High Rev Product', 'revenue': 10000, 'cost': 500, 'conversions': 20},
    }

    svc._analyze_feed(diag, feed_issues, 'c1', mc_to_campaigns, product_revenue, 1000)

    # Actions should list High Rev Product first (sorted by revenue)
    assert 'High Rev Product' in diag['actions'][0], (
        f"Should prioritize high-revenue SKU first, got: {diag['actions']}"
    )
    # Not working should show total $ at risk
    assert '$10,100' in diag['not_working'][0], (
        f"Should show total revenue at risk: {diag['not_working']}"
    )


# ---------------------------------------------------------------------------
# Test 35b — Feed action fallback avoids arbitrary repeated SKU names
# ---------------------------------------------------------------------------

def test_feed_action_fallback_without_campaign_revenue():
    """When campaign-revenue linkage is weak, action should use issue summary (not arbitrary SKU names)."""
    from app.services.campaign_diagnostics import CampaignDiagnosticsService
    from unittest.mock import MagicMock

    svc = CampaignDiagnosticsService(MagicMock())
    diag = {'working': [], 'not_working': [], 'actions': [], 'has_blockers': False}

    feed_issues = {
        'disapproved': [
            {'product_id': 'p1', 'offer_id': None, 'title': 'SKU One',
             'issue_code': 'price_mismatch', 'description': 'Price issue', 'normalized_pid': 'p1'},
            {'product_id': 'p2', 'offer_id': None, 'title': 'SKU Two',
             'issue_code': 'price_mismatch', 'description': 'Price issue', 'normalized_pid': 'p2'},
        ],
        'count': 2,
    }
    mc_to_campaigns = {'p1': {'c1'}, 'p2': {'c1'}}
    # No revenue linkage for either SKU in this campaign period
    product_revenue = {
        'p1': {'title': 'SKU One', 'revenue': 0, 'cost': 0, 'conversions': 0},
        'p2': {'title': 'SKU Two', 'revenue': 0, 'cost': 0, 'conversions': 0},
    }

    svc._analyze_feed(diag, feed_issues, 'c1', mc_to_campaigns, product_revenue, 500)

    assert diag['actions'], "Expected feed action to be generated"
    assert 'Fix 2 disapproved products (top issue: price_mismatch)' == diag['actions'][0]


# ---------------------------------------------------------------------------
# Test 36 — MC product_id normalization
# ---------------------------------------------------------------------------

def test_mc_product_id_normalization():
    """MC product_id must normalize to match Ads product_item_id format."""
    from app.services.campaign_diagnostics import _normalize_mc_product_id

    assert _normalize_mc_product_id('online:en:AU:shopify_AU_111_222') == 'shopify_au_111_222'
    assert _normalize_mc_product_id('online:en:AU:shopify_AU_6770211422252_39981455441964') == \
        'shopify_au_6770211422252_39981455441964'
    assert _normalize_mc_product_id('') == ''
    assert _normalize_mc_product_id('simple_id') == 'simple_id'
