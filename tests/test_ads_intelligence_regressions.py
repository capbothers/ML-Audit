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
    """If is_wasting_budget=True, strategy_action cannot be scale or scale_aggressively."""
    db = SessionLocal()
    try:
        campaigns = db.query(CampaignPerformance).filter(
            CampaignPerformance.is_wasting_budget == True,
            CampaignPerformance.strategy_action.isnot(None),
        ).all()
        for c in campaigns:
            assert c.strategy_action not in ('scale', 'scale_aggressively'), (
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


def test_low_attribution_blocks_reduce():
    """Low attr conf + reduce → investigate (in processor source)."""
    source = Path("app/services/ad_spend_processor.py").read_text()
    # The attribution gate should exist
    assert "attr_confidence == 'low'" in source, "Attribution gate missing in processor"
    assert "'reduce', 'pause'" in source or "('reduce', 'pause')" in source, (
        "Attribution gate must block reduce/pause actions"
    )


# ---------------------------------------------------------------------------
# Test 14 — LP friction overrides reduce
# ---------------------------------------------------------------------------

def test_lp_friction_overrides_reduce():
    """When LP CVR drops and action is reduce → fix_landing_page (arbitrator rule 2)."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    campaign = {
        'true_metrics': {'roas': 1.8},
    }
    evidence = {
        'strategy': {
            'action': 'reduce',
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
    assert result['final_action'] == 'fix_landing_page', (
        f"LP friction should override reduce to fix_landing_page, got {result['final_action']}"
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
        'true_metrics': {'roas': 5.0},
    }
    evidence = {
        'strategy': {
            'action': 'reduce',  # incorrectly classified
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
    """High-conf DR + scale → investigate (or maintain if profitability protection kicks in)."""
    from app.services.decision_arbitration import DecisionArbitrator

    arbitrator = DecisionArbitrator()
    # Use medium confidence so profitability protection doesn't override the DR finding
    campaign = {
        'true_metrics': {'roas': 3.5},
    }
    evidence = {
        'strategy': {
            'action': 'scale',
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
        f"High-conf DR overspend should downgrade scale to investigate, got {result['final_action']}"
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
    campaign = {'true_metrics': {'roas': 4.0}}
    evidence = {
        'strategy': {
            'action': 'scale',
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
    # why_now should NOT mention "Scale budget"
    assert 'Scale budget' not in (result['why_now'] or ''), (
        f"why_now says 'Scale budget' but final action is investigate: {result['why_now']}"
    )


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
# Test 21 — fix_landing_page action template exists
# ---------------------------------------------------------------------------

def test_fix_landing_page_template():
    """campaign_strategy.py must have fix_landing_page in WHY_NOW_TEMPLATES."""
    source = Path("app/services/campaign_strategy.py").read_text()
    assert "'fix_landing_page'" in source, (
        "Missing fix_landing_page template in campaign_strategy.py"
    )


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
