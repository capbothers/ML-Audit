"""
Decision Arbitration Engine — final policy layer.

Replaces the ad-hoc override chain with an ordered, confidence-weighted
set of policy rules. Each rule can promote or demote the action, but only
if its evidence meets the confidence threshold.

Policy rules (priority order):
  1. Measurement gate          — stale data → investigate
  2. Landing page override     — LP friction → fix
  3. Attribution gate          — low attr conf → can't review/pause
  4. DR override               — high-conf overspend → can't scale
  5. Waste override            — wasting + scale → investigate
  6. Profitability protection  — high-conf profitable → floor at maintain
"""
from typing import Dict, List, Optional

from app.services.campaign_strategy import format_why_now, STRATEGY_THRESHOLDS


# Action rank for comparison (new diagnostic vocabulary)
_ACTION_RANK = {
    'scale_what_works': 5, 'maintain': 4, 'fix': 3,
    'investigate': 2, 'review': 1, 'pause': 0,
}


class DecisionArbitrator:
    """
    Final policy engine that weighs all evidence modules before emitting
    a decision.

    Invariants:
      - High-confidence negative evidence can downgrade actions
      - Low-confidence modules cannot override high-confidence profitability
      - Explanation always references diagnostic actions, never budget amounts
    """

    def arbitrate(self, campaign: Dict, evidence: Dict) -> Dict:
        """
        Apply policy rules and return final decision.

        Args:
            campaign: Full campaign dict from get_campaign_performance
            evidence: {
                'strategy':   {action, confidence, decision_score, type, ...},
                'diminishing_returns': {overspend_per_day, dr_confidence, ...} or None,
                'causal_triage': {primary_cause, confidence, causes} or None,
                'attribution': {confidence, gap_pct} or None,
                'waste':       {is_wasting, reasons} or None,
            }
        """
        strat = evidence.get('strategy') or {}
        action = strat.get('action', 'investigate')
        strat_confidence = strat.get('confidence', 'low')
        stype = strat.get('type', 'unknown')
        roas = campaign.get('true_metrics', {}).get('true_roas')
        score = strat.get('decision_score')

        overrides: List[Dict] = []
        evidence_chain: List[Dict] = []

        # Record base strategy evidence
        evidence_chain.append({
            'module': 'strategy',
            'signal': f"{strat.get('short_term_status', '?')}/{strat.get('strategic_value', '?')}",
            'confidence': strat_confidence,
            'direction': action,
        })

        # --- Rule 1: Measurement gate ---
        triage = evidence.get('causal_triage')
        if triage:
            meas = next((c for c in triage.get('causes', []) if c['cause'] == 'measurement'), None)
            if meas and meas['score'] > 0.5:
                overrides.append({
                    'from_action': action, 'to_action': 'investigate',
                    'reason': f"Data quality issue: {meas['evidence']}",
                    'module': 'measurement',
                })
                action = 'investigate'
                evidence_chain.append({
                    'module': 'measurement', 'signal': meas['evidence'],
                    'confidence': 'high', 'direction': 'investigate',
                })

        # --- Rule 2: Landing page override ---
        if triage:
            lp = next((c for c in triage.get('causes', []) if c['cause'] == 'landing_page'), None)
            if lp and lp['score'] >= 0.7 and action in ('review', 'pause', 'fix'):
                overrides.append({
                    'from_action': action, 'to_action': 'fix',
                    'reason': f"LP issue detected: {lp['evidence']}",
                    'module': 'landing_page',
                })
                action = 'fix'
                evidence_chain.append({
                    'module': 'landing_page', 'signal': lp['evidence'],
                    'confidence': 'high', 'direction': 'fix',
                })

        # --- Rule 3: Attribution confidence gate ---
        attr = evidence.get('attribution') or {}
        attr_conf = attr.get('confidence', 'high')
        if attr_conf == 'low' and action in ('review', 'pause'):
            gap = attr.get('gap_pct')
            gap_str = f" (gap: {gap:.0f}%)" if gap is not None else ""
            overrides.append({
                'from_action': action, 'to_action': 'investigate',
                'reason': f"Low attribution confidence{gap_str} — verify conversions before cutting",
                'module': 'attribution',
            })
            action = 'investigate'
            evidence_chain.append({
                'module': 'attribution', 'signal': f'confidence={attr_conf}',
                'confidence': 'low', 'direction': 'investigate',
            })

        # --- Rule 4: Diminishing returns override ---
        dr = evidence.get('diminishing_returns')
        if dr and action == 'scale_what_works':
            overspend = dr.get('overspend_per_day', 0)
            optimal = dr.get('optimal_daily_spend', 0)
            current = dr.get('current_daily_spend', 0)
            dr_conf = dr.get('dr_confidence', 'low')
            pct_over = (overspend / optimal) if optimal > 0 else 0
            is_material = current >= 50 and (overspend > 50 or pct_over > 0.20)

            # DR description: flag efficiency concern, never prescribe budget target
            dr_desc = (
                f"spend efficiency declining — "
                f"marginal ROAS dropping after {dr.get('active_days', '?')}d of data"
            )

            if is_material and dr_conf == 'high':
                overrides.append({
                    'from_action': action, 'to_action': 'investigate',
                    'reason': (
                        f"DR (high conf, {dr.get('active_days', '?')}d): {dr_desc}"
                    ),
                    'module': 'diminishing_returns',
                })
                action = 'investigate'
                evidence_chain.append({
                    'module': 'diminishing_returns',
                    'signal': dr_desc,
                    'confidence': dr_conf, 'direction': 'investigate',
                })
            elif is_material:
                evidence_chain.append({
                    'module': 'diminishing_returns',
                    'signal': f'{dr_desc} (low conf)',
                    'confidence': dr_conf, 'direction': 'monitor',
                })

        # --- Rule 5: Waste override ---
        waste = evidence.get('waste') or {}
        if waste.get('is_wasting') and action == 'scale_what_works':
            overrides.append({
                'from_action': action, 'to_action': 'investigate',
                'reason': 'Waste signals detected — investigate before scaling',
                'module': 'waste',
            })
            action = 'investigate'

        # --- Rule 6: Profitability protection ---
        thresholds = STRATEGY_THRESHOLDS.get(stype, STRATEGY_THRESHOLDS.get('unknown', {}))
        roas_good = thresholds.get('roas_good', 2.5)
        if (strat_confidence == 'high'
                and roas is not None and roas >= roas_good
                and _ACTION_RANK.get(action, 4) < _ACTION_RANK.get('maintain', 4)):
            # High-confidence profitable campaign — floor at maintain
            overrides.append({
                'from_action': action, 'to_action': 'maintain',
                'reason': (
                    f"ROAS {roas:.1f}x above {stype} target ({roas_good}x) "
                    f"with high confidence — floor at maintain"
                ),
                'module': 'profitability_protection',
            })
            action = 'maintain'

        # --- Generate final explanation ---
        why_now = self._generate_why_now(
            action, roas, stype, thresholds, score, overrides, triage
        )

        return {
            'final_action': action,
            'final_confidence': strat_confidence,
            'why_now': why_now,
            'evidence_chain': evidence_chain,
            'overrides': overrides,
        }

    def _generate_why_now(
        self,
        action: str,
        roas: Optional[float],
        stype: str,
        thresholds: Dict,
        score: Optional[int],
        overrides: List[Dict],
        triage: Optional[Dict],
    ) -> Optional[str]:
        """Generate why_now — always diagnostic, never budget-prescriptive."""
        # If there was an override, explain it
        if overrides:
            last = overrides[-1]
            if last['module'] == 'landing_page':
                return (
                    f"LP conversion degraded ({last.get('reason', '')}). "
                    f"Fix page issues before adjusting spend."
                )
            elif action == 'investigate' and last['module'] == 'diminishing_returns':
                return (
                    f"ROAS {roas:.1f}x looks strong but {last['reason']}. "
                    f"Investigate spend efficiency."
                ) if roas else last['reason']
            elif action == 'investigate' and last['module'] == 'attribution':
                return last['reason']
            elif action == 'investigate' and last['module'] == 'measurement':
                return f"Data quality concern: {last['reason']}. Resolve before acting."
            elif action == 'investigate' and last['module'] == 'waste':
                return last['reason']
            elif last['module'] == 'profitability_protection':
                return (
                    f"ROAS {roas:.1f}x is profitable — holding at maintain "
                    f"despite low-confidence signals."
                ) if roas else last['reason']

        # If triage identified a primary cause, incorporate it
        if triage and triage.get('primary_cause'):
            cause = triage['primary_cause']
            cause_evidence = next(
                (c['evidence'] for c in triage.get('causes', []) if c['cause'] == cause),
                ''
            )
            base = format_why_now(action, roas, stype, thresholds, score)
            neutral_causes = (
                'Demand stable', 'LP metrics stable', 'All sources fresh',
                'Feed health OK', 'Auction pressure stable',
            )
            if base and cause_evidence and cause_evidence not in neutral_causes:
                return f"{base} Root cause: {cause_evidence}."
            return base

        # Default: standard template
        return format_why_now(action, roas, stype, thresholds, score)
