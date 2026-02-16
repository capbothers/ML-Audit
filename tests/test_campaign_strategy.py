"""
Campaign Strategy classification, scoring, and decision matrix tests.

Guards against:
1. Misclassification of campaign strategy types
2. Score out of bounds
3. Action matrix violations
4. Confidence gating bypass
5. Brand-defense campaigns being unfairly penalized
"""
from app.services.campaign_strategy import (
    classify,
    score,
    decide,
    format_why_now,
    STRATEGY_THRESHOLDS,
)


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------

def test_classify_search_is_brand_defense():
    assert classify("Zip Search Campaign", "SEARCH", 400) == "brand_defense"


def test_classify_search_by_name_is_brand_defense():
    """Even PMAX campaigns with 'search' in the name are brand_defense."""
    assert classify("PM-SYD Toto Search", "PERFORMANCE_MAX", 600) == "brand_defense"


def test_classify_demand_gen_is_prospecting():
    assert classify("Demand Gen Campaign", "DEMAND_GEN", None) == "prospecting"


def test_classify_display_is_prospecting():
    assert classify("Summer Display", "DISPLAY", 200) == "prospecting"


def test_classify_local_store_visit_is_prospecting():
    assert classify("PM-SYD Local - Store Visits", "PERFORMANCE_MAX", 300) == "prospecting"


def test_classify_zombie_is_unknown():
    assert classify("PM Zombie Campaign", "PERFORMANCE_MAX", 500) == "unknown"


def test_classify_old_campaign_is_unknown():
    assert classify("PM Rheem Old", "PERFORMANCE_MAX", 400) == "unknown"


def test_classify_filter_is_fast_turn():
    assert classify("PM-AU Zip Filters", "PERFORMANCE_MAX", 80) == "fast_turn"


def test_classify_hardware_all_is_fast_turn():
    assert classify("PM-AU Hardware All", "PERFORMANCE_MAX", None) == "fast_turn"


def test_classify_pmax_high_aov_is_high_consideration():
    assert classify("PM-AU Billi", "PERFORMANCE_MAX", 500) == "high_consideration"


def test_classify_pmax_low_aov_is_fast_turn():
    assert classify("PM-AU Generic", "PERFORMANCE_MAX", 50) == "fast_turn"


def test_classify_pmax_no_aov_defaults_to_high_consideration():
    """PMAX with no AOV data falls to rule 5 (PMAX → high_consideration)."""
    assert classify("PM-AU Caroma", "PERFORMANCE_MAX", None) == "high_consideration"


def test_classify_shopping_is_high_consideration():
    assert classify("Toto Sydney $1.00", "SHOPPING", None) == "high_consideration"


# ---------------------------------------------------------------------------
# Score tests
# ---------------------------------------------------------------------------

def test_score_range_0_100():
    """Score must always be in [0, 100] regardless of inputs."""
    for strategy in STRATEGY_THRESHOLDS:
        thresholds = STRATEGY_THRESHOLDS[strategy]
        # Extreme high
        data = {'true_roas': 20.0, 'cpa': 0.5, 'impression_share': 0, 'fully_loaded_roas': 15.0, 'total_spend': 5000, 'days': 30}
        s = score(data, strategy, thresholds)
        assert 0 <= s <= 100, f"Score {s} out of range for {strategy} (high inputs)"

        # Extreme low / nulls
        data2 = {'true_roas': None, 'cpa': None, 'impression_share': None, 'fully_loaded_roas': None, 'total_spend': 0, 'days': 0}
        s2 = score(data2, strategy, thresholds)
        assert 0 <= s2 <= 100, f"Score {s2} out of range for {strategy} (null inputs)"

        # Zero everything
        data3 = {'true_roas': 0, 'cpa': 0, 'impression_share': 0, 'fully_loaded_roas': 0, 'total_spend': 0, 'days': 30}
        s3 = score(data3, strategy, thresholds)
        assert 0 <= s3 <= 100, f"Score {s3} out of range for {strategy} (zero inputs)"


def test_score_higher_roas_yields_higher_score():
    """Campaign with 5x ROAS should score higher than 1x ROAS, all else equal."""
    thresholds = STRATEGY_THRESHOLDS['high_consideration']
    base = {'cpa': 50, 'impression_share': 20, 'fully_loaded_roas': None, 'total_spend': 500, 'days': 30}

    low = score({**base, 'true_roas': 1.0}, 'high_consideration', thresholds)
    high = score({**base, 'true_roas': 5.0}, 'high_consideration', thresholds)
    assert high > low


# ---------------------------------------------------------------------------
# Action matrix tests
# ---------------------------------------------------------------------------

def test_action_matrix_strong_high_scales_aggressively():
    result = decide(85, 5.0, 'high_consideration', STRATEGY_THRESHOLDS['high_consideration'], 1000, 30)
    assert result['short_term'] == 'strong'
    assert result['strategic_value'] == 'high'
    assert result['action'] == 'scale_aggressively'


def test_action_matrix_healthy_moderate_maintains():
    result = decide(55, 3.0, 'high_consideration', STRATEGY_THRESHOLDS['high_consideration'], 500, 30)
    assert result['short_term'] == 'healthy'
    assert result['strategic_value'] == 'moderate'
    assert result['action'] == 'maintain'


def test_action_matrix_weak_low_pauses():
    result = decide(20, 0.3, 'high_consideration', STRATEGY_THRESHOLDS['high_consideration'], 500, 30)
    assert result['short_term'] == 'weak'
    assert result['strategic_value'] == 'low'
    assert result['action'] == 'pause'


def test_action_matrix_weak_high_investigates():
    """Weak ROAS + high strategic value = investigate, not pause."""
    result = decide(80, 0.5, 'high_consideration', STRATEGY_THRESHOLDS['high_consideration'], 500, 30)
    assert result['short_term'] == 'weak'
    assert result['strategic_value'] == 'high'
    assert result['action'] == 'investigate'


# ---------------------------------------------------------------------------
# Confidence gating tests
# ---------------------------------------------------------------------------

def test_confidence_gating_low_spend_clamps():
    """Low spend → low confidence → action clamped to max 'maintain'."""
    thresholds = STRATEGY_THRESHOLDS['high_consideration']
    # min_spend_for_eval is 200 — use spend of 50
    result = decide(90, 8.0, 'high_consideration', thresholds, 50, 30)
    assert result['confidence'] == 'low'
    # scale_aggressively (rank 6) should be clamped to maintain (rank 4)
    assert result['action'] == 'maintain'


def test_confidence_high_when_sufficient_spend_and_days():
    thresholds = STRATEGY_THRESHOLDS['fast_turn']
    result = decide(60, 5.0, 'fast_turn', thresholds, 500, 30)
    assert result['confidence'] == 'high'


def test_confidence_medium_for_short_period():
    thresholds = STRATEGY_THRESHOLDS['fast_turn']
    result = decide(60, 5.0, 'fast_turn', thresholds, 500, 5)
    assert result['confidence'] == 'medium'


# ---------------------------------------------------------------------------
# Why-now template tests
# ---------------------------------------------------------------------------

def test_why_now_template_populated():
    text = format_why_now('scale', 3.5, 'high_consideration', STRATEGY_THRESHOLDS['high_consideration'], 72)
    assert text is not None
    assert '3.5x' in text
    assert '72/100' in text
    assert 'High Consideration' in text


def test_why_now_investigate_mentions_attribution():
    text = format_why_now('investigate', 0.8, 'brand_defense', STRATEGY_THRESHOLDS['brand_defense'], 75)
    assert 'attribution' in text.lower()


def test_why_now_returns_none_for_missing_action():
    assert format_why_now(None, 2.0, 'fast_turn', None, 50) is None


# ---------------------------------------------------------------------------
# Threshold relationship tests
# ---------------------------------------------------------------------------

def test_thresholds_brand_defense_roas_floor_lower():
    """Brand defense should have a lower ROAS floor than fast_turn (different goals)."""
    assert STRATEGY_THRESHOLDS['brand_defense']['roas_floor'] < STRATEGY_THRESHOLDS['fast_turn']['roas_floor']


def test_thresholds_prospecting_roas_floor_lowest():
    """Prospecting should have the lowest ROAS floor (top-of-funnel)."""
    pr_floor = STRATEGY_THRESHOLDS['prospecting']['roas_floor']
    for name, t in STRATEGY_THRESHOLDS.items():
        if name != 'prospecting':
            assert pr_floor <= t['roas_floor'], f"Prospecting floor {pr_floor} not <= {name} floor {t['roas_floor']}"


# ---------------------------------------------------------------------------
# Regression: brand defense not unfairly penalized
# ---------------------------------------------------------------------------

def test_brand_defense_roas_1_not_paused():
    """A brand_defense campaign with ROAS 1.0 (above its 0.8 floor) should NOT get pause."""
    thresholds = STRATEGY_THRESHOLDS['brand_defense']
    data = {'true_roas': 1.0, 'cpa': 10, 'impression_share': 15, 'fully_loaded_roas': 0.8, 'total_spend': 200, 'days': 30}
    s = score(data, 'brand_defense', thresholds)
    result = decide(s, 1.0, 'brand_defense', thresholds, 200, 30)
    assert result['action'] not in ('pause', 'reduce'), (
        f"Brand defense with ROAS 1.0 got '{result['action']}' — should be maintain/optimize/investigate"
    )


def test_brand_defense_roas_1_classified_marginal_not_weak():
    """ROAS 1.0 for brand defense (floor=0.8, good=1.5) is marginal, not weak."""
    thresholds = STRATEGY_THRESHOLDS['brand_defense']
    result = decide(50, 1.0, 'brand_defense', thresholds, 200, 30)
    assert result['short_term'] == 'marginal', f"Expected marginal, got {result['short_term']}"
