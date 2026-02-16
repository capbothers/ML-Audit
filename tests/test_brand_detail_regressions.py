from pathlib import Path

from app.models.base import SessionLocal
from app.services.brand_decision_engine import BrandDecisionEngine
from app.services.brand_diagnosis_engine import BrandDiagnosisEngine
from app.services.brand_intelligence_service import BrandIntelligenceService


def test_decision_period_aligned_with_brand_detail_window():
    """Decision period label must align with Brand Detail's anchored window."""
    db = SessionLocal()
    try:
        bis = BrandIntelligenceService(db)
        de = BrandDecisionEngine(db)

        detail = bis.get_brand_detail("Zip", 30)
        decision = de.decide("Zip", 30)

        start_iso = detail["data_coverage"]["current_start"]
        end_iso = detail["data_coverage"]["current_end"]
        expected_period = f"{start_iso[:10]} to {end_iso[:10]}"

        assert decision["period"] == expected_period
    finally:
        db.close()


def test_diagnosis_engine_is_null_safe_for_all_dashboard_brands():
    """
    Regression guard: diagnosis should not crash on dashboard brands due to
    None comparisons in decomposition logic.
    """
    db = SessionLocal()
    try:
        bis = BrandIntelligenceService(db)
        diag = BrandDiagnosisEngine(db)

        brands = [b["brand"] for b in bis.get_dashboard(30).get("brands", [])]
        failures = []

        for brand in brands:
            try:
                result = diag.diagnose(brand, 30)
                assert isinstance(result, dict)
            except Exception as exc:  # pragma: no cover - only for failure reporting
                failures.append((brand, str(exc)))

        assert not failures, f"Diagnosis failures: {failures}"
    finally:
        db.close()


def test_wasted_spend_semantics_and_labels():
    """
    Wasted-spend rows should only exist when spend is known and > $50.
    UI copy should remain qualified (not absolute 'wasted spend').
    """
    db = SessionLocal()
    try:
        bis = BrandIntelligenceService(db)
        dashboard_brands = [b["brand"] for b in bis.get_dashboard(30).get("brands", [])[:30]]

        saw_wasted = False
        for brand in dashboard_brands:
            detail = bis.get_brand_detail(brand, 30)
            ads = (detail.get("diagnostics") or {}).get("ads") or {}
            wasted = ads.get("wasted_spend_products") or []

            for row in wasted:
                saw_wasted = True
                spend = row.get("spend")
                assert spend is not None
                assert float(spend) > 50
                assert float(row.get("conversions") or 0) == 0

        # Ensure test actually validates at least one real example in current data.
        assert saw_wasted

        html = Path("app/static/brand_intelligence.html").read_text()
        service_py = Path("app/services/brand_intelligence_service.py").read_text()
        assert "Low-Converting Products (spend > $50, no attributed conversions)" in html
        assert "no attributed conversions — review targeting" in service_py
        assert "could save ${spend * (1 - roas / 3):,.0f} in ad spend" in service_py
    finally:
        db.close()
