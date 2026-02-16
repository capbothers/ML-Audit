"""
Decision Feedback Service — snapshot decisions, score outcomes, record feedback.

Lifecycle:
  1. snapshot_decisions() — called after each process() run
  2. score_outcomes(7)    — scheduled daily, fills 7-day outcomes
  3. score_outcomes(30)   — scheduled daily, fills 30-day outcomes
  4. record_feedback()    — called from API when user accepts/rejects
  5. get_accuracy_by_type() — for dashboard and threshold calibration
"""
from datetime import datetime, timedelta
from typing import Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.ad_spend import CampaignPerformance
from app.models.decision_feedback import DecisionSnapshot
from app.utils.logger import log


class DecisionFeedbackService:
    def __init__(self, db: Session):
        self.db = db

    def snapshot_decisions(self) -> int:
        """
        Capture current campaign decisions as snapshots.
        Called after AdSpendProcessor.process().
        Returns count of snapshots created.
        """
        campaigns = self.db.query(CampaignPerformance).filter(
            CampaignPerformance.strategy_action.isnot(None),
            CampaignPerformance.is_active == True,
        ).all()

        count = 0
        for c in campaigns:
            snap = DecisionSnapshot(
                campaign_id=c.campaign_id,
                campaign_name=c.campaign_name,
                strategy_type=c.strategy_type,
                decided_at=datetime.utcnow(),
                action=c.strategy_action,
                confidence=c.strategy_confidence,
                decision_score=c.decision_score,
                primary_cause=c.primary_cause,
                true_roas=c.true_roas,
                total_spend=c.total_spend,
                true_profit=c.true_profit,
            )
            self.db.add(snap)
            count += 1

        self.db.commit()
        log.info(f"Snapshotted {count} campaign decisions")
        return count

    def score_outcomes(self, lookback_days: int = 7) -> int:
        """
        Score decisions made `lookback_days` ago against current performance.
        Returns count of outcomes scored.
        """
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        # Snapshots from lookback_days ago (± 1 day window)
        window_start = cutoff - timedelta(days=1)

        if lookback_days <= 7:
            snapshots = self.db.query(DecisionSnapshot).filter(
                DecisionSnapshot.decided_at >= window_start,
                DecisionSnapshot.decided_at <= cutoff,
                DecisionSnapshot.outcome_7d_roas.is_(None),
            ).all()
        else:
            snapshots = self.db.query(DecisionSnapshot).filter(
                DecisionSnapshot.decided_at >= window_start,
                DecisionSnapshot.decided_at <= cutoff,
                DecisionSnapshot.outcome_30d_roas.is_(None),
            ).all()

        scored = 0
        for snap in snapshots:
            current = self.db.query(CampaignPerformance).filter_by(
                campaign_id=snap.campaign_id,
            ).first()
            if not current:
                continue

            if lookback_days <= 7:
                snap.outcome_7d_roas = current.true_roas
                snap.outcome_7d_profit = current.true_profit
            else:
                snap.outcome_30d_roas = current.true_roas
                snap.outcome_30d_profit = current.true_profit

            snap.outcome_verdict = self._evaluate(snap, current)
            snap.outcome_scored_at = datetime.utcnow()
            scored += 1

        self.db.commit()
        log.info(f"Scored {scored} decision outcomes ({lookback_days}d)")
        return scored

    def _evaluate(self, snap: DecisionSnapshot, current: CampaignPerformance) -> str:
        """Determine if the decision was correct given the outcome."""
        if snap.action in ('scale', 'scale_aggressively'):
            # Scale was correct if ROAS held within 10%
            if current.true_roas and snap.true_roas:
                return 'correct' if current.true_roas >= snap.true_roas * 0.9 else 'wrong'
            return 'neutral'

        elif snap.action in ('reduce', 'pause'):
            # Reduce was correct if the campaign was actually losing money
            if snap.true_roas is not None:
                return 'correct' if snap.true_roas < 1.5 else 'wrong'
            return 'neutral'

        elif snap.action == 'fix_landing_page':
            # LP fix was correct if CVR improved
            return 'neutral'  # Can't easily measure LP changes

        else:
            # maintain, optimize, investigate — neutral by default
            return 'neutral'

    def record_feedback(
        self,
        campaign_id: str,
        user_action: str,
        override_to: Optional[str] = None,
    ) -> Optional[DecisionSnapshot]:
        """
        Record user acceptance/rejection of a recommendation.
        Updates the most recent snapshot for this campaign.
        """
        snap = self.db.query(DecisionSnapshot).filter_by(
            campaign_id=campaign_id,
        ).order_by(DecisionSnapshot.decided_at.desc()).first()

        if not snap:
            return None

        snap.user_action = user_action
        snap.user_override_to = override_to
        snap.user_feedback_at = datetime.utcnow()
        self.db.commit()

        log.info(
            f"Recorded feedback for {campaign_id}: {user_action}"
            f"{' → ' + override_to if override_to else ''}"
        )
        return snap

    def get_accuracy_by_type(self) -> Dict:
        """
        Accuracy breakdown per strategy_type for threshold calibration.
        Returns {strategy_type: {total, correct, wrong, neutral, accuracy}}.
        """
        results = {}
        rows = self.db.query(
            DecisionSnapshot.strategy_type,
            DecisionSnapshot.outcome_verdict,
            func.count(DecisionSnapshot.id),
        ).filter(
            DecisionSnapshot.outcome_verdict.isnot(None),
        ).group_by(
            DecisionSnapshot.strategy_type,
            DecisionSnapshot.outcome_verdict,
        ).all()

        for stype, verdict, count in rows:
            if stype not in results:
                results[stype] = {'total': 0, 'correct': 0, 'wrong': 0, 'neutral': 0}
            results[stype][verdict] = count
            results[stype]['total'] += count

        for stype, data in results.items():
            evaluated = data['correct'] + data['wrong']
            data['accuracy'] = (
                round(data['correct'] / evaluated, 2) if evaluated > 0 else None
            )

        return results
