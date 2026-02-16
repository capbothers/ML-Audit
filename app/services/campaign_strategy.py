"""
Campaign Strategy Classification & Decision Layer

Deterministic (no LLM) strategy classification that applies strategy-specific
benchmarks, computes a composite decision score, and produces dual-status
verdicts (short-term efficiency + strategic value).

5 strategy types: high_consideration, fast_turn, brand_defense, prospecting, unknown
"""

# ---------------------------------------------------------------------------
# Strategy-specific thresholds
# ---------------------------------------------------------------------------

STRATEGY_THRESHOLDS = {
    'high_consideration': {
        'label': 'High Consideration',
        'short': 'HC',
        'description': 'High-AOV products with longer sales cycles',
        'roas_floor': 1.5,
        'roas_good': 2.5,
        'roas_great': 4.0,
        'cvr_floor': 0.005,
        'cpa_ceiling': 120,
        'min_spend_for_eval': 200,
        'weights': {
            'roas_vs_threshold': 30,
            'efficiency': 20,
            'volume_trend': 15,
            'impression_share': 10,
            'margin_health': 25,
        },
    },
    'fast_turn': {
        'label': 'Fast Turn',
        'short': 'FT',
        'description': 'Low-AOV consumables/accessories with quick purchase cycles',
        'roas_floor': 2.5,
        'roas_good': 4.0,
        'roas_great': 6.0,
        'cvr_floor': 0.02,
        'cpa_ceiling': 25,
        'min_spend_for_eval': 100,
        'weights': {
            'roas_vs_threshold': 40,
            'efficiency': 25,
            'volume_trend': 15,
            'impression_share': 5,
            'margin_health': 15,
        },
    },
    'brand_defense': {
        'label': 'Brand Defense',
        'short': 'BD',
        'description': 'Protecting brand position in search results',
        'roas_floor': 0.8,
        'roas_good': 1.5,
        'roas_great': 2.5,
        'cvr_floor': 0.03,
        'cpa_ceiling': 15,
        'min_spend_for_eval': 50,
        'weights': {
            'roas_vs_threshold': 15,
            'efficiency': 15,
            'volume_trend': 10,
            'impression_share': 35,
            'margin_health': 25,
        },
    },
    'prospecting': {
        'label': 'Prospecting',
        'short': 'PR',
        'description': 'Top-of-funnel awareness and demand generation',
        'roas_floor': 0.5,
        'roas_good': 1.2,
        'roas_great': 2.0,
        'cvr_floor': 0.002,
        'cpa_ceiling': 80,
        'min_spend_for_eval': 300,
        'weights': {
            'roas_vs_threshold': 10,
            'efficiency': 15,
            'volume_trend': 25,
            'impression_share': 20,
            'margin_health': 30,
        },
    },
    'unknown': {
        'label': 'Unknown',
        'short': '??',
        'description': 'Insufficient data or zombie campaigns',
        'roas_floor': 2.0,
        'roas_good': 3.0,
        'roas_great': 4.0,
        'cvr_floor': 0.01,
        'cpa_ceiling': 50,
        'min_spend_for_eval': 100,
        'weights': {
            'roas_vs_threshold': 30,
            'efficiency': 20,
            'volume_trend': 15,
            'impression_share': 15,
            'margin_health': 20,
        },
    },
}

# ---------------------------------------------------------------------------
# Action matrix: (short_term_efficiency, strategic_value) -> action
# ---------------------------------------------------------------------------

_ACTION_MATRIX = {
    ('strong', 'high'):     'scale_aggressively',
    ('strong', 'moderate'): 'scale',
    ('strong', 'low'):      'maintain',
    ('healthy', 'high'):    'scale',
    ('healthy', 'moderate'): 'maintain',
    ('healthy', 'low'):     'optimize',
    ('marginal', 'high'):   'maintain',
    ('marginal', 'moderate'): 'optimize',
    ('marginal', 'low'):    'reduce',
    ('weak', 'high'):       'investigate',
    ('weak', 'moderate'):   'reduce',
    ('weak', 'low'):        'pause',
}

# Actions ordered from most aggressive to most conservative (for confidence gating)
_ACTION_RANK = {
    'scale_aggressively': 6,
    'scale': 5,
    'maintain': 4,
    'investigate': 3,
    'optimize': 2,
    'reduce': 1,
    'pause': 0,
}
_RANK_TO_ACTION = {v: k for k, v in _ACTION_RANK.items()}

# Confidence gating: low confidence clamps action to at most this rank
_CONFIDENCE_CLAMP = {
    'high': 6,    # no clamp
    'medium': 5,  # max = scale
    'low': 4,     # max = maintain
}

# ---------------------------------------------------------------------------
# Why-now templates
# ---------------------------------------------------------------------------

_WHY_NOW_TEMPLATES = {
    'scale_aggressively': (
        "ROAS {roas:.1f}x exceeds {strategy} target ({target}x) "
        "with {score}/100 decision score. Scale budget by 50%."
    ),
    'scale': (
        "ROAS {roas:.1f}x above {strategy} good ({target}x). "
        "Decision score {score}/100. Increase budget 25%."
    ),
    'maintain': (
        "ROAS {roas:.1f}x near {strategy} target. "
        "Decision score {score}/100. Hold current budget."
    ),
    'optimize': (
        "ROAS {roas:.1f}x below {strategy} target ({target}x). "
        "Decision score {score}/100. Review targeting and bids."
    ),
    'reduce': (
        "ROAS {roas:.1f}x below {strategy} floor ({floor}x). "
        "Decision score {score}/100. Cut budget 50%."
    ),
    'pause': (
        "ROAS {roas:.1f}x well below {strategy} floor ({floor}x). "
        "Decision score {score}/100. Pause and review."
    ),
    'investigate': (
        "Low ROAS ({roas:.1f}x) but high strategic signals "
        "(score {score}/100). Check attribution lag before cutting."
    ),
    'fix_landing_page': (
        "Ad traffic is healthy but landing page conversion degraded. "
        "Fix LP/checkout before adjusting budget."
    ),
}

# Name patterns for classification (lowercased)
_UNKNOWN_PATTERNS = ['zombie', ' old', 'test campaign']
_PROSPECTING_PATTERNS = ['demand gen', 'prospecting', 'awareness', 'local', 'store visit']
_FAST_TURN_PATTERNS = ['filter', 'accessori', 'part', 'hardware all']
_PROSPECTING_TYPES = {'DEMAND_GEN', 'DISPLAY', 'VIDEO'}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify(campaign_name: str, campaign_type: str | None, aov: float | None) -> str:
    """
    Classify a campaign into a strategy type.

    Priority order (first match wins):
    1. Name contains zombie/old/test → unknown
    2. campaign_type in (DEMAND_GEN, DISPLAY, VIDEO) or name matches prospecting → prospecting
    3. campaign_type == SEARCH or name contains "search" → brand_defense
    4. Name contains filter/hardware all or AOV < $150 → fast_turn
    5. AOV >= $300 or PMAX with brand name → high_consideration
    6. Else → unknown
    """
    name_lower = (campaign_name or '').lower()
    ctype_upper = (campaign_type or '').upper()

    # 1. Zombie / old / test
    for pat in _UNKNOWN_PATTERNS:
        if pat in name_lower:
            return 'unknown'

    # 2. Prospecting (demand gen, display, video)
    if ctype_upper in _PROSPECTING_TYPES:
        return 'prospecting'
    for pat in _PROSPECTING_PATTERNS:
        if pat in name_lower:
            return 'prospecting'

    # 3. Brand defense (search campaigns)
    if ctype_upper == 'SEARCH' or 'search' in name_lower:
        return 'brand_defense'

    # 4. Fast turn (filters, accessories, low AOV)
    for pat in _FAST_TURN_PATTERNS:
        if pat in name_lower:
            return 'fast_turn'
    if aov is not None and aov < 150:
        return 'fast_turn'

    # 5. High consideration (high AOV or PMAX brand campaigns)
    if aov is not None and aov >= 300:
        return 'high_consideration'
    if ctype_upper in ('PERFORMANCE_MAX', 'SHOPPING'):
        return 'high_consideration'

    # 6. Fallback
    return 'unknown'


def score(campaign_data: dict, strategy: str, thresholds: dict | None = None) -> int:
    """
    Compute composite decision score (0-100) for a campaign.

    campaign_data keys:
        true_roas (float|None), cpa (float|None), impression_share (float|None 0-100),
        fully_loaded_roas (float|None), total_spend (float), days (int)
    """
    if thresholds is None:
        thresholds = STRATEGY_THRESHOLDS.get(strategy, STRATEGY_THRESHOLDS['unknown'])

    weights = thresholds['weights']
    components = {}

    true_roas = campaign_data.get('true_roas') or 0
    roas_good = thresholds['roas_good']

    # 1. ROAS vs threshold (0-100 pct)
    components['roas_vs_threshold'] = min(100, (true_roas / roas_good) * 100) if roas_good > 0 else 0

    # 2. Efficiency: CPA vs ceiling (lower is better)
    cpa = campaign_data.get('cpa')
    cpa_ceiling = thresholds['cpa_ceiling']
    if cpa is not None and cpa_ceiling > 0:
        # CPA at ceiling = 50%, CPA at 0 = 100%, CPA at 2x ceiling = 0%
        components['efficiency'] = max(0, min(100, (1 - cpa / (2 * cpa_ceiling)) * 100))
    else:
        components['efficiency'] = 50  # neutral if no data

    # 3. Volume trend (placeholder — WoW conversion growth not available at process time)
    # Use conversion rate as proxy: higher conv rate = healthier volume
    components['volume_trend'] = 50  # neutral default

    # 4. Impression share (0-100 input → 0-100 score)
    imp_share = campaign_data.get('impression_share')
    if imp_share is not None:
        # impression_share comes as 0-100 (percentage of impressions won)
        # Actually in the processor, lost_impression_share is what we have
        # A low lost IS means high won IS
        components['impression_share'] = max(0, min(100, 100 - float(imp_share)))
    else:
        components['impression_share'] = 50  # neutral

    # 5. Margin health: fully loaded ROAS
    fl_roas = campaign_data.get('fully_loaded_roas')
    if fl_roas is not None:
        components['margin_health'] = max(0, min(100, fl_roas * 40))  # 2.5x FL ROAS = 100
    elif true_roas > 0:
        components['margin_health'] = max(0, min(100, true_roas * 30))  # fallback to true ROAS
    else:
        components['margin_health'] = 0

    # Weighted sum
    total_score = 0
    total_weight = 0
    for component, weight in weights.items():
        pct = components.get(component, 50)
        total_score += pct * weight
        total_weight += weight

    final = round(total_score / total_weight) if total_weight > 0 else 0
    return max(0, min(100, final))


def decide(
    decision_score: int,
    true_roas: float | None,
    strategy: str,
    thresholds: dict | None = None,
    total_spend: float = 0,
    days: int = 30,
) -> dict:
    """
    Produce dual-status decision: short_term_efficiency + strategic_value → action.

    Returns dict with: short_term, strategic_value, action, confidence
    """
    if thresholds is None:
        thresholds = STRATEGY_THRESHOLDS.get(strategy, STRATEGY_THRESHOLDS['unknown'])

    roas = true_roas if true_roas is not None else 0

    # Short-term efficiency
    if roas >= thresholds['roas_great']:
        short_term = 'strong'
    elif roas >= thresholds['roas_good']:
        short_term = 'healthy'
    elif roas >= thresholds['roas_floor']:
        short_term = 'marginal'
    else:
        short_term = 'weak'

    # Strategic value
    if decision_score >= 70:
        strategic_val = 'high'
    elif decision_score >= 45:
        strategic_val = 'moderate'
    else:
        strategic_val = 'low'

    # Action from matrix
    action = _ACTION_MATRIX.get((short_term, strategic_val), 'optimize')

    # Confidence assessment
    min_spend = thresholds.get('min_spend_for_eval', 100)
    if total_spend < min_spend:
        confidence = 'low'
    elif days >= 7:
        confidence = 'high'
    elif days >= 3:
        confidence = 'medium'
    else:
        confidence = 'low'

    # Confidence gating: clamp action
    max_rank = _CONFIDENCE_CLAMP.get(confidence, 6)
    # Strategy-type gating: unknown/zombie campaigns never scale
    if strategy == 'unknown':
        max_rank = min(max_rank, _CONFIDENCE_CLAMP['low'])  # cap at maintain
    action_rank = _ACTION_RANK.get(action, 2)
    if action_rank > max_rank:
        action = _RANK_TO_ACTION.get(max_rank, action)

    return {
        'short_term': short_term,
        'strategic_value': strategic_val,
        'action': action,
        'confidence': confidence,
    }


def format_why_now(
    action: str | None,
    roas: float | None,
    strategy: str | None,
    thresholds: dict | None = None,
    decision_score: int | None = None,
) -> str | None:
    """Generate deterministic why-now text from metrics."""
    if not action or not strategy:
        return None

    if thresholds is None:
        thresholds = STRATEGY_THRESHOLDS.get(strategy, STRATEGY_THRESHOLDS['unknown'])

    template = _WHY_NOW_TEMPLATES.get(action)
    if not template:
        return None

    label = thresholds.get('label', strategy.replace('_', ' ').title())

    return template.format(
        roas=roas or 0,
        strategy=label,
        target=thresholds.get('roas_good', 0),
        floor=thresholds.get('roas_floor', 0),
        score=decision_score or 0,
    )
