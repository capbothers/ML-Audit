"""
Brand Decision Engine

Orchestrates existing diagnostics into a unified WHY / HOW / WHAT-IF contract.
"""
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.services.brand_intelligence_service import BrandIntelligenceService
from app.services.brand_diagnosis_engine import BrandDiagnosisEngine
from app.utils.logger import log


class BrandDecisionEngine:
    def __init__(self, db: Session):
        self.db = db
        self.intel = BrandIntelligenceService(db)
        self.diagnosis = BrandDiagnosisEngine(db)

    def decide(self, brand: str, period_days: int = 30) -> Dict:
        now = datetime.utcnow()
        cur_start = now - timedelta(days=period_days)
        cur_end = now
        period_label = f"{cur_start.strftime('%Y-%m-%d')} to {cur_end.strftime('%Y-%m-%d')}"

        detail = self.intel.get_brand_detail(brand_name=brand, period_days=period_days)
        try:
            diag = self.diagnosis.diagnose(brand, period_days=period_days)
        except Exception as e:
            log.error(f"Decision engine diagnosis failed for {brand}: {e}")
            diag = None

        state, state_label = self._determine_state(detail, diag)
        why = self._build_why(detail, diag, state)
        how = self._build_how(detail, state)
        what_if = self._build_what_if(detail, diag)
        guardrails = self._apply_guardrails(detail, diag, how)
        confidence = self._build_confidence(detail, diag)

        return {
            "brand": brand,
            "period": period_label,
            "state": state,
            "state_label": state_label,
            "revenue_current": (detail.get("summary") or {}).get("current_revenue"),
            "revenue_yoy_pct": (detail.get("summary") or {}).get("revenue_yoy_pct"),
            "why": why,
            "how": how,
            "what_if": what_if,
            "confidence": confidence,
            "guardrails": guardrails,
        }

    # ── helpers ────────────────────────────────────────────────

    def _determine_state(self, detail: Dict, diag: Optional[Dict]):
        yoy = (detail.get("summary") or {}).get("revenue_yoy_pct")
        if yoy is None:
            return "stable", "Stable (no prior year)"
        if yoy < -10:
            return "down", f"Declining {yoy:.1f}% YoY"
        if yoy > 10:
            return "up", f"Growing {yoy:.1f}% YoY"
        return "stable", f"Stable {yoy:.1f}% YoY"

    def _build_why(self, detail: Dict, diag: Optional[Dict], state: str) -> Dict:
        why_detail = detail.get("why_analysis") or {}
        drivers = []

        diag_drivers = (diag or {}).get("performance_decomposition", {}).get("driver_contributions", {})
        expl_map = {d.get("driver"): d for d in (why_detail.get("drivers") or [])}

        for name, d in diag_drivers.items():
            expl = expl_map.get(name, {})
            drivers.append({
                "driver": name,
                "dollars": d.get("dollars"),
                "pct_of_change": d.get("pct_of_change"),
                "direction": d.get("direction"),
                "confidence": d.get("confidence"),
                "explanation": expl.get("explanation") or expl.get("label") or "",
            })

        drivers.sort(key=lambda x: abs(x.get("dollars") or 0), reverse=True)

        # primary driver: avoid low confidence if possible
        primary = None
        for d in drivers:
            if d.get("confidence") != "low":
                primary = d.get("driver")
                break
        if not primary and drivers:
            primary = drivers[0].get("driver")

        # summary: state + top 2 drivers
        summary_bits = []
        summary_bits.append((why_detail.get("summary") or "").strip())
        top_expl = [d.get("explanation") for d in drivers[:2] if d.get("explanation")]
        summary = ". ".join([b for b in summary_bits + top_expl if b])

        return {
            "summary": summary,
            "primary_driver": primary,
            "drivers": drivers,
            "anomalies": (diag or {}).get("anomalies") or [],
            "momentum": (diag or {}).get("momentum_score") or {},
        }

    def _build_how(self, detail: Dict, state: str) -> Dict:
        recs = detail.get("recommendations") or []
        diag = detail.get("diagnostics") or {}

        category_map = {
            "root_cause": "assortment",
            "stock": "assortment",
            "range": "assortment",
            "ads": "ads",
            "demand": "ads",
            "pricing": "pricing",
            "margin": "pricing",
            "conversion": "funnel",
        }

        actions = []
        for idx, r in enumerate(recs, 1):
            cat = category_map.get(r.get("category", ""), r.get("category") or "assortment")
            evidence = self._build_evidence(r, diag)
            actions.append({
                "id": idx,
                "category": cat,
                "priority": r.get("priority"),
                "action": r.get("action"),
                "expected_impact": r.get("expected_impact"),
                "expected_impact_dollars": r.get("expected_impact_dollars"),
                "impacted_metric": r.get("impacted_metric"),
                "dependency_order": r.get("dependency_order"),
                "evidence": evidence,
            })

        strategy = "activation"
        if state == "down":
            strategy = "recovery"
        elif state == "up":
            strategy = "scaling"

        deps = [
            "Fix pricing or margin violations before scaling spend",
            "Secure supply for high-velocity products before advertising pushes",
            "Resolve conversion friction before scaling traffic",
        ]

        return {
            "strategy": strategy,
            "actions": actions,
            "dependencies": deps,
        }

    def _build_what_if(self, detail: Dict, diag: Optional[Dict]) -> Dict:
        scenarios = []
        diagnostics = detail.get("diagnostics") or {}

        # Scenario 1: Scale ads +20%
        ads = diagnostics.get("ads") or {}
        if ads.get("campaign_spend", 0) > 0:
            spend = ads.get("campaign_spend", 0)
            roas = ads.get("campaign_roas", 0) or 0
            add_spend = spend * 0.2
            scenarios.append({
                "scenario": "Scale ads +20%",
                "description": "Increase brand spend by 20% at current ROAS",
                "impact_low": add_spend * roas * 0.7,
                "impact_mid": add_spend * roas * 1.0,
                "impact_high": add_spend * roas * 1.2,
                "confidence": "high" if spend > 500 else "medium",
                "assumptions": ["ROAS holds", "Inventory coverage sufficient"],
                "time_horizon": "30 days",
            })

        # Scenario 3: Fix pricing violations
        pricing = diagnostics.get("pricing") or {}
        losing = pricing.get("losing_money", 0) or 0
        if losing > 0:
            avg_margin = pricing.get("avg_margin", 0) or 0
            scenarios.append({
                "scenario": "Fix pricing violations",
                "description": "Lift below-cost SKUs to minimum margin",
                "impact_low": losing * avg_margin * 0.5,
                "impact_mid": losing * avg_margin * 0.75,
                "impact_high": losing * avg_margin * 1.0,
                "confidence": "medium",
                "assumptions": ["No volume collapse from price lift"],
                "time_horizon": "90 days",
            })

        # Scenario 4: Improve conversion +1pp
        conv = diagnostics.get("conversion") or {}
        views = conv.get("total_views", 0) or 0
        if views > 0:
            aov = (detail.get("summary") or {}).get("current_revenue", 0) / max((detail.get("summary") or {}).get("current_units", 1), 1)
            scenarios.append({
                "scenario": "Improve conversion +1pp",
                "description": "Lift overall conversion by 1pp",
                "impact_low": views * 0.01 * aov * 0.5,
                "impact_mid": views * 0.01 * aov * 1.0,
                "impact_high": views * 0.01 * aov * 1.5,
                "confidence": "medium" if views > 1000 else "low",
                "assumptions": ["Traffic quality stable"],
                "time_horizon": "30 days",
            })

        total_upside = round(sum(s["impact_mid"] for s in scenarios), 2) if scenarios else 0
        return {"scenarios": scenarios, "total_addressable_upside": total_upside}

    def _apply_guardrails(self, detail: Dict, diag: Optional[Dict], how: Dict) -> Dict:
        pricing = (detail.get("diagnostics") or {}).get("pricing") or {}
        violations = []

        below_min = pricing.get("below_minimum", 0) or 0
        losing = pricing.get("losing_money", 0) or 0

        if below_min > 0:
            violations.append({
                "type": "below_map",
                "count": below_min,
                "action_blocked": "Lowering prices further",
            })
        if losing > 0:
            violations.append({
                "type": "below_cost",
                "count": losing,
                "action_blocked": "Discounting below cost",
            })

        driver_contribs = (diag or {}).get("performance_decomposition", {}).get("driver_contributions", {})
        low_conf = [name for name, d in driver_contribs.items() if d.get("confidence") == "low"]

        return {
            "pricing_violations": violations,
            "low_confidence_signals": low_conf,
        }

    def _build_confidence(self, detail: Dict, diag: Optional[Dict]) -> Dict:
        diagnostics = detail.get("diagnostics") or {}
        summary = detail.get("summary") or {}
        decomp_section = (diag or {}).get("performance_decomposition", {})
        coverage = decomp_section.get("decomposition_coverage_pct", 0)
        if coverage:
            coverage = round(abs(coverage), 2)

        sources = {
            "orders": summary.get("current_revenue") is not None,
            "cogs": summary.get("cost_coverage_pct", 0),
            "ads": diagnostics.get("ads") is not None,
            "demand": diagnostics.get("demand") is not None and (diagnostics.get("demand") or {}).get("cur_clicks") is not None,
            "conversion": diagnostics.get("conversion") is not None,
            "pricing": diagnostics.get("pricing") is not None,
            "customer": False,
        }

        available = sum(1 for k, v in sources.items() if (k == "cogs" and v > 0) or (k != "cogs" and v))
        overall = "high" if available >= 4 else "medium" if available >= 2 else "low"

        return {
            "overall": overall,
            "data_sources": sources,
            "decomposition_coverage_pct": coverage,
        }

    def _build_evidence(self, rec: Dict, diagnostics: Dict) -> List[Dict]:
        category = rec.get("category", "")
        evidence = []

        if category == "ads":
            ads = diagnostics.get("ads") or {}
            if ads:
                evidence.append({"source": "ads_diagnostic", "metric": "roas", "value": ads.get("campaign_roas")})
                evidence.append({"source": "ads_diagnostic", "metric": "impression_share", "value": ads.get("impression_share")})
        elif category == "pricing":
            pricing = diagnostics.get("pricing") or {}
            if pricing:
                evidence.append({"source": "pricing", "metric": "price_index", "value": pricing.get("price_index")})
                evidence.append({"source": "pricing", "metric": "losing_money", "value": pricing.get("losing_money")})
        elif category == "conversion":
            conv = diagnostics.get("conversion") or {}
            if conv:
                evidence.append({"source": "conversion", "metric": "view_to_cart_pct", "value": conv.get("view_to_cart_pct")})

        return evidence[:3]
