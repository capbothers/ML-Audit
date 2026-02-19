"""
Brand Intelligence API Routes

Endpoints for brand performance analysis:
- Dashboard overview (KPIs + scorecard)
- Brand detail (deep dive with WHY analysis)
- Brand comparison
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.models.base import get_db
from app.services.brand_intelligence_service import BrandIntelligenceService
from app.services.brand_diagnosis_engine import BrandDiagnosisEngine
from app.services.brand_decision_engine import BrandDecisionEngine
from app.utils.logger import log
from app.utils.cache import get_cached, set_cached, _MISS

router = APIRouter(prefix="/brands", tags=["brands"])


@router.get("/dashboard")
async def get_brand_dashboard(
    days: int = Query(30, ge=1, le=730, description="Period in days (30, 90, 365)"),
    db: Session = Depends(get_db),
):
    """Brand Intelligence dashboard — KPIs + scorecard for all brands."""
    cache_key = f"brand_dashboard|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        service = BrandIntelligenceService(db)
        data = service.get_dashboard(period_days=days)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail")
async def get_brand_detail(
    brand: str = Query(..., description="Brand/vendor name"),
    days: int = Query(30, ge=1, le=730, description="Period in days"),
    db: Session = Depends(get_db),
):
    """Deep dive into a single brand with WHY analysis and recommendations."""
    cache_key = f"brand_detail|{brand}|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        service = BrandIntelligenceService(db)
        data = service.get_brand_detail(brand_name=brand, period_days=days)

        # Attach ML-ready diagnosis — use the same anchored period end
        # as get_brand_detail() so all panels describe the same window.
        try:
            from datetime import datetime as _dt
            anchored_end = _dt.fromisoformat(data["data_coverage"]["current_end"])
            engine = BrandDiagnosisEngine(db)
            data["diagnosis"] = engine.diagnose(brand, period_days=days, cur_end=anchored_end)
        except Exception as diag_err:
            log.error(f"Diagnosis engine failed for {brand}: {diag_err}")
            data["diagnosis"] = None

        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnosis")
async def get_brand_diagnosis(
    brand: str = Query(..., description="Brand/vendor name"),
    days: int = Query(30, ge=1, le=730, description="Period in days (30, 90, 365)"),
    db: Session = Depends(get_db),
):
    """ML-ready brand diagnosis — structured decomposition with stock gating."""
    cache_key = f"brand_diagnosis|{brand}|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        engine = BrandDiagnosisEngine(db)
        data = engine.diagnose(brand, period_days=days)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/diagnosis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executive")
async def get_executive_summary(
    days: int = Query(30, ge=1, le=730, description="Period in days"),
    db: Session = Depends(get_db),
):
    """Executive view — brands at risk, watchlist, overperformers."""
    cache_key = f"brand_executive|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        service = BrandIntelligenceService(db)
        data = service.get_executive_summary(period_days=days)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/executive: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_opportunities(
    days: int = Query(30, ge=1, le=730, description="Period in days"),
    db: Session = Depends(get_db),
):
    """Ranked brands by growth opportunity score."""
    cache_key = f"brand_opportunities|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        service = BrandIntelligenceService(db)
        data = service.get_opportunity_ranking(period_days=days)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/decision")
async def get_brand_decision(
    brand: str = Query(..., description="Brand/vendor name"),
    days: int = Query(30, ge=1, le=730, description="Period in days"),
    db: Session = Depends(get_db),
):
    """Unified WHY / HOW / WHAT-IF decision contract."""
    cache_key = f"brand_decision|{brand}|{days}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached

    try:
        engine = BrandDecisionEngine(db)
        data = engine.decide(brand=brand, period_days=days)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /brands/decision: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare")
async def compare_brands(
    brands: str = Query(..., description="Comma-separated brand names"),
    days: int = Query(30, ge=1, le=730, description="Period in days"),
    db: Session = Depends(get_db),
):
    """Compare 2-5 brands side by side."""
    brand_list = [b.strip() for b in brands.split(',') if b.strip()]
    if len(brand_list) < 2 or len(brand_list) > 5:
        raise HTTPException(status_code=400, detail="Provide 2-5 brand names")

    try:
        service = BrandIntelligenceService(db)
        data = service.get_brand_comparison(brand_list, period_days=days)
        return {"success": True, "data": data}
    except Exception as e:
        log.error(f"Error in /brands/compare: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
