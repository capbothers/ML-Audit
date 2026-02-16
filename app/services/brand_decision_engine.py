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
        how = self._build_how(detail, state, diag=diag)
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

        # primary driver: prefer high-confidence, largest dollar impact
        primary = None
        for d in drivers:
            if d.get("confidence") != "low":
                primary = d.get("driver")
                break
        if not primary and drivers:
            primary = drivers[0].get("driver")

        # Build concise 1-2 sentence executive summary
        summary = self._executive_summary(detail, diag, drivers, state)

        return {
            "summary": summary,
            "primary_driver": primary,
            "drivers": drivers,
            "anomalies": (diag or {}).get("anomalies") or [],
            "momentum": (diag or {}).get("momentum_score") or {},
            "weekly_trends": (diag or {}).get("weekly_trends"),
        }

    def _executive_summary(self, detail: Dict, diag: Optional[Dict],
                           drivers: list, state: str) -> str:
        """Build a 1-2 sentence executive summary from drivers + trends."""
        summary = detail.get("summary") or {}
        brand = summary.get("brand") or (detail.get("brand") or "Brand")
        yoy = summary.get("revenue_yoy_pct")
        cur_rev = summary.get("current_revenue")
        decomp = (diag or {}).get("performance_decomposition", {}).get("driver_contributions", {})

        # Sentence 1: "{Brand} is {up/down} X% YoY driven by {top 2 directional drivers}"
        if yoy is not None:
            direction = "up" if yoy >= 0 else "down"
            s1 = f"{brand} is {direction} {abs(yoy):.0f}% YoY"
        elif cur_rev is not None:
            s1 = f"{brand} revenue is ${cur_rev:,.0f}"
        else:
            s1 = f"{brand} performance"

        # Pick top 2 drivers that match the direction of change
        # Declining brand → focus on biggest negative drivers
        # Growing brand → focus on biggest positive drivers
        is_declining = yoy is not None and yoy < 0
        directional = [d for d in drivers if
                       (is_declining and (d.get("dollars") or 0) < 0) or
                       (not is_declining and (d.get("dollars") or 0) > 0)]
        # Fallback: if fewer than 2 same-direction drivers, take largest by abs
        if len(directional) < 2:
            directional = sorted(drivers, key=lambda x: abs(x.get("dollars") or 0), reverse=True)
        top2 = sorted(directional, key=lambda x: abs(x.get("dollars") or 0), reverse=True)[:2]

        driver_parts = []
        for d in top2:
            name = d.get("driver") or ""
            dollars = d.get("dollars") or 0
            # Use rich labels for product_mix (show lost/new breakdown)
            if name == "product_mix" and decomp.get("product_mix"):
                pm = decomp["product_mix"]
                if dollars < 0:
                    lost_n = pm.get("lost_products", 0)
                    driver_parts.append(f"lost range (-${abs(dollars):,.0f}, {lost_n} products)")
                else:
                    new_n = pm.get("new_products", 0)
                    driver_parts.append(f"new range (+${abs(dollars):,.0f}, {new_n} products)")
            else:
                label = name.replace("_", " ")
                driver_parts.append(f"{label} (-${abs(dollars):,.0f})" if dollars < 0
                                    else f"{label} (+${abs(dollars):,.0f})")

        if driver_parts:
            s1 += " driven by " + " and ".join(driver_parts)
        s1 += "."

        # Sentence 2: Critical trend or operational signal
        trend_narrative = self._build_trend_narrative(diag)
        if trend_narrative:
            return f"{s1} {trend_narrative}"

        # Fallback: mention ads efficiency if ROAS changed significantly
        ads = (diag or {}).get("ads_model") or {}
        roas_chg = ads.get("roas_change_pct")
        if roas_chg is not None and abs(roas_chg) > 30:
            direction = "up" if roas_chg > 0 else "down"
            return f"{s1} Ads efficiency {direction} {abs(roas_chg):.0f}% YoY ({ads.get('prev_roas', 0):.1f}x → {ads.get('cur_roas', 0):.1f}x)."

        return s1

    def _build_how(self, detail: Dict, state: str, diag: Optional[Dict] = None) -> Dict:
        recs = detail.get("recommendations") or []
        detail_diag = detail.get("diagnostics") or {}

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

        # Check if ROAS is declining — suppress BIS "scale" ads recs if so
        roas_declining = False
        if diag:
            _wt = ((diag or {}).get("weekly_trends") or {}).get("trends") or {}
            roas_declining = _wt.get("ads_roas", "") in ("accelerating_decline", "declining")

        # Pre-build enrichment data for concrete actions
        lost_products = detail.get("lost_products") or []
        top_lost = lost_products[:3]
        lost_sku_lines = []
        for p in top_lost:
            sku = p.get("sku") or "?"
            rev = p.get("revenue") or 0
            lost_sku_lines.append(f"{sku} (${rev:,.0f})")

        actions = []
        for idx, r in enumerate(recs, 1):
            cat = category_map.get(r.get("category", ""), r.get("category") or "assortment")
            # Skip BIS ads "scale" recommendations when ROAS is declining
            if roas_declining and cat == "ads":
                action_text = (r.get("action") or "").lower()
                if "scale" in action_text or "increase" in action_text:
                    continue
            evidence = self._build_evidence(r, detail_diag)
            action = r.get("action") or ""

            # Enrich "lost products" action with top SKUs (skip if already present)
            if cat == "assortment" and "lost" in action.lower() and lost_sku_lines and "Top lost:" not in action:
                action += " Top lost: " + ", ".join(lost_sku_lines) + "."

            actions.append({
                "id": idx,
                "category": cat,
                "priority": r.get("priority"),
                "action": action,
                "expected_impact": r.get("expected_impact"),
                "expected_impact_dollars": r.get("expected_impact_dollars"),
                "impacted_metric": r.get("impacted_metric"),
                "dependency_order": r.get("dependency_order"),
                "evidence": evidence,
            })

        # Inject ads-specific actions from diagnosis engine data
        if diag:
            ads_model = (diag or {}).get("ads_model") or {}
            weekly = (diag or {}).get("weekly_trends") or {}
            trends = weekly.get("trends") or {}

            if ads_model.get("cur_spend", 0) > 0:
                roas_trend = trends.get("ads_roas", "")

                if roas_trend in ("accelerating_decline", "declining"):
                    # Build concrete action with campaign names (active only)
                    top_camps = ads_model.get("top_campaigns") or []
                    active_camps = [c for c in top_camps if c.get("status") != "paused"]
                    paused_camps = [c for c in top_camps if c.get("status") == "paused"]
                    camp_lines = []
                    for c in active_camps[:3]:
                        camp_lines.append(
                            f"{c['name']} (${c['spend']:,.0f} spend, {c['roas']:.1f}x ROAS)"
                        )
                    action_text = "Audit declining ROAS campaigns"
                    if camp_lines:
                        action_text += ": " + "; ".join(camp_lines)
                    else:
                        action_text += " — pause or restructure worst performers"
                    if paused_camps:
                        paused_names = ", ".join(c["name"] for c in paused_camps[:2])
                        action_text += f". Already paused: {paused_names}"

                    actions.append({
                        "id": len(actions) + 1,
                        "category": "ads",
                        "priority": "critical" if roas_trend == "accelerating_decline" else "high",
                        "action": action_text,
                        "expected_impact": "Prevent further ROAS erosion",
                        "expected_impact_dollars": None,
                        "impacted_metric": "spend_efficiency",
                        "dependency_order": 1,
                        "evidence": [
                            {"source": "diagnosis", "metric": "roas_trend", "label": f"ROAS Trend: {roas_trend}", "value": roas_trend},
                            {"source": "diagnosis", "metric": "cur_roas", "label": f"Current ROAS: {ads_model.get('cur_roas', 0):.1f}x", "value": ads_model.get("cur_roas")},
                            {"source": "diagnosis", "metric": "prev_roas", "label": f"Prior-Year ROAS: {ads_model.get('prev_roas', 0):.1f}x", "value": ads_model.get("prev_roas")},
                        ],
                    })

                budget_lost = ads_model.get("cur_budget_lost") or 0
                if budget_lost > 15:
                    actions.append({
                        "id": len(actions) + 1,
                        "category": "ads",
                        "priority": "medium",
                        "action": f"Increase budget — {budget_lost:.0f}% impression share lost to budget",
                        "expected_impact": "Capture additional impression share",
                        "expected_impact_dollars": None,
                        "impacted_metric": "revenue",
                        "dependency_order": 3,
                        "evidence": [
                            {"source": "diagnosis", "metric": "budget_lost_share", "label": f"Budget Lost Share: {budget_lost:.0f}%", "value": budget_lost},
                        ],
                    })

                new_camps = ads_model.get("new_campaigns", 0)
                if new_camps > 0:
                    actions.append({
                        "id": len(actions) + 1,
                        "category": "ads",
                        "priority": "medium",
                        "action": f"Review {new_camps} new campaign(s) — verify targeting and ROAS trajectory",
                        "expected_impact": "Ensure new campaigns are optimizing correctly",
                        "expected_impact_dollars": None,
                        "impacted_metric": "spend_efficiency",
                        "dependency_order": 3,
                        "evidence": [],
                    })

        # When ROAS is the critical issue, demote assortment actions
        # so ads efficiency leads the action list
        if roas_declining:
            for a in actions:
                if a["category"] == "assortment":
                    # Push assortment to later steps
                    if (a.get("dependency_order") or 4) < 3:
                        a["dependency_order"] = 3
                    # Downgrade priority so ads [critical] dominates visually
                    if a.get("priority") == "high":
                        a["priority"] = "medium"

        strategy = "activation"
        if state == "down":
            strategy = "recovery"
        elif state == "up":
            strategy = "scaling"

        # Build context-aware dependency chain (only include what's relevant)
        deps = []
        pricing_diag = (detail.get("diagnostics") or {}).get("pricing") or {}
        losing = pricing_diag.get("losing_money", 0) or 0
        below_min = pricing_diag.get("below_minimum", 0) or 0
        if losing > 0 or below_min > 0:
            deps.append(f"Fix {losing + below_min} pricing violation(s) before scaling spend")

        if roas_declining:
            deps.append("Fix ads efficiency before scaling spend")

        conv_diag = (detail.get("diagnostics") or {}).get("conversion") or {}
        v2c = conv_diag.get("view_to_cart_pct")
        if v2c is not None and v2c < 3:
            deps.append(f"Resolve conversion friction ({v2c:.1f}% add-to-cart) before scaling traffic")

        # Note: OOS dependency removed — business rule is "we sell when out of stock"

        return {
            "strategy": strategy,
            "actions": actions,
            "dependencies": deps,
        }

    def _build_what_if(self, detail: Dict, diag: Optional[Dict]) -> Dict:
        scenarios = []
        diagnostics = detail.get("diagnostics") or {}
        weekly_trends = ((diag or {}).get("weekly_trends") or {}).get("trends") or {}

        # Scenario 1a: Restore ROAS (when declining) or Scale ads +20% (when stable)
        ads = diagnostics.get("ads") or {}
        ads_roas_trend = weekly_trends.get("ads_roas", "")
        diag_ads = (diag or {}).get("ads_model") or {}
        if ads.get("campaign_spend", 0) > 0:
            spend = ads.get("campaign_spend", 0)
            roas = ads.get("campaign_roas", 0) or 0
            add_spend = spend * 0.2

            if ads_roas_trend in ("accelerating_decline", "declining"):
                # Blocked: Scale ads
                scenarios.append({
                    "scenario": "Scale ads +20%",
                    "description": f"ROAS is in decline — fix campaign efficiency first (current ${spend:,.0f} at {roas:.1f}x)",
                    "additional_spend": round(add_spend, 2),
                    "new_total_spend": round(spend + add_spend, 2),
                    "impact_low": 0,
                    "impact_mid": 0,
                    "impact_high": 0,
                    "confidence": "low",
                    "assumptions": [f"ROAS trend: {ads_roas_trend} — scaling would amplify losses"],
                    "time_horizon": "30 days",
                    "blocked": True,
                    "blocked_reason": f"ROAS trend: {ads_roas_trend}",
                })

                # Priority scenario: Restore ROAS to prior-year level
                prev_roas = diag_ads.get("prev_roas", 0) or 0
                cur_roas = diag_ads.get("cur_roas", 0) or 0
                if prev_roas > cur_roas and spend > 0:
                    # Revenue gained = current spend × (prior ROAS - current ROAS)
                    roas_gap = prev_roas - cur_roas
                    recovery_mid = spend * roas_gap
                    scenarios.append({
                        "scenario": "Restore ROAS to prior level",
                        "description": f"Fix campaign efficiency from {cur_roas:.1f}x back to {prev_roas:.1f}x at current spend",
                        "impact_low": round(recovery_mid * 0.5, 2),
                        "impact_mid": round(recovery_mid, 2),
                        "impact_high": round(recovery_mid * 1.2, 2),
                        "confidence": "high" if spend > 500 else "medium",
                        "assumptions": [
                            f"Current ROAS: {cur_roas:.1f}x → Target: {prev_roas:.1f}x",
                            "Campaign restructuring stops efficiency bleed",
                            "Inventory and demand remain stable",
                        ],
                        "time_horizon": "60 days",
                    })
            else:
                scenarios.append({
                    "scenario": "Scale ads +20%",
                    "description": (
                        f"Add ${add_spend:,.0f}/mo to current ${spend:,.0f} spend "
                        f"(→ ${spend + add_spend:,.0f}) at {roas:.1f}x ROAS"
                    ),
                    "additional_spend": round(add_spend, 2),
                    "new_total_spend": round(spend + add_spend, 2),
                    "impact_low": round(add_spend * roas * 0.7),
                    "impact_mid": round(add_spend * roas * 1.0),
                    "impact_high": round(add_spend * roas * 1.2),
                    "confidence": "high" if spend > 500 else "medium",
                    "assumptions": [
                        f"Current spend: ${spend:,.0f}, ROAS: {roas:.1f}x",
                        "ROAS holds at incremental spend",
                        "Inventory coverage sufficient",
                    ],
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
                "impact_low": round(losing * avg_margin * 0.5),
                "impact_mid": round(losing * avg_margin * 0.75),
                "impact_high": round(losing * avg_margin * 1.0),
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
                "impact_low": round(views * 0.01 * aov * 0.5),
                "impact_mid": round(views * 0.01 * aov * 1.0),
                "impact_high": round(views * 0.01 * aov * 1.5),
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

        # Check ads from both BIS diagnostics and diagnosis engine
        diag_ads = (diag or {}).get("ads_model") or {}
        has_ads = diagnostics.get("ads") is not None or diag_ads.get("cur_spend", 0) > 0

        sources = {
            "orders": summary.get("current_revenue") is not None,
            "cogs": summary.get("cost_coverage_pct", 0),
            "ads": has_ads,
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

    def _build_trend_narrative(self, diag: Optional[Dict]) -> str:
        """Build human-readable trend narrative from weekly trends."""
        weekly = (diag or {}).get("weekly_trends") or {}
        trends = weekly.get("trends") or {}
        if not trends:
            return ""

        parts = []
        ads_trend = trends.get("ads_roas", "")
        if ads_trend in ("accelerating_decline", "declining"):
            ads_data = weekly.get("ads") or []
            roas_vals = [w["roas"] for w in ads_data if w.get("roas") is not None]
            recent = roas_vals[-4:] if len(roas_vals) >= 4 else roas_vals
            if recent:
                parts.append(
                    f"ROAS declining week-over-week ({' → '.join(str(r) for r in recent)})"
                )
            else:
                parts.append("Ads ROAS in sustained decline")

        rev_trend = trends.get("revenue", "")
        if rev_trend == "accelerating_decline":
            parts.append("Revenue decline is accelerating week-over-week")
        elif rev_trend == "recovering":
            parts.append("Revenue showing signs of week-over-week recovery")

        search_trend = trends.get("search_clicks", "")
        if search_trend in ("accelerating_decline", "declining"):
            parts.append("Branded search clicks softening in recent weeks (may differ from YoY trend)")
        elif search_trend in ("recovering", "accelerating_growth"):
            parts.append("Branded search clicks strengthening in recent weeks")

        return ". ".join(parts)

    def _build_evidence(self, rec: Dict, diagnostics: Dict) -> List[Dict]:
        category = rec.get("category", "")
        evidence = []

        if category == "ads":
            ads = diagnostics.get("ads") or {}
            if ads:
                roas_val = ads.get("campaign_roas")
                imp_val = ads.get("impression_share")
                evidence.append({
                    "source": "ads_diagnostic",
                    "metric": "campaign_roas",
                    "label": f"Campaign ROAS: {roas_val:.1f}x" if roas_val else "Campaign ROAS: N/A",
                    "value": roas_val,
                })
                evidence.append({
                    "source": "ads_diagnostic",
                    "metric": "impression_share",
                    "label": f"Impression Share: {imp_val:.0f}%" if imp_val is not None else "Impression Share: N/A",
                    "value": imp_val,
                })
        elif category == "pricing":
            pricing = diagnostics.get("pricing") or {}
            if pricing:
                pi_val = pricing.get("price_index")
                losing_val = pricing.get("losing_money")
                evidence.append({
                    "source": "pricing",
                    "metric": "price_index",
                    "label": f"Price Index: {pi_val:.2f}" if pi_val else "Price Index: N/A",
                    "value": pi_val,
                })
                evidence.append({
                    "source": "pricing",
                    "metric": "losing_money",
                    "label": f"SKUs Below Cost: {losing_val}" if losing_val else "SKUs Below Cost: 0",
                    "value": losing_val,
                })
        elif category == "conversion":
            conv = diagnostics.get("conversion") or {}
            if conv:
                v2c = conv.get("view_to_cart_pct")
                evidence.append({
                    "source": "conversion",
                    "metric": "view_to_cart_pct",
                    "label": f"View-to-Cart: {v2c:.1f}%" if v2c is not None else "View-to-Cart: N/A",
                    "value": v2c,
                })

        return evidence[:3]
