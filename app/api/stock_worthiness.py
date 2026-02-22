"""
Stock Worthiness API Routes

Endpoints for identifying order-in products worth stocking in-warehouse.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.stock_worthiness_service import StockWorthinessService
from app.utils.cache import get_cached, set_cached, _MISS
from app.utils.logger import log

router = APIRouter(prefix="/stock-worthiness", tags=["stock-worthiness"])


@router.get("/dashboard")
async def get_stock_worthiness_dashboard(
    min_score: int = Query(0, ge=0, le=100, description="Minimum stock worthiness score"),
    vendor: Optional[str] = Query(None, description="Filter by vendor/brand"),
    db: Session = Depends(get_db),
):
    """Stock worthiness dashboard — order-in products ranked by stocking score."""
    cache_key = f"sw_dashboard|{min_score}|{vendor or ''}"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    try:
        svc = StockWorthinessService(db)
        data = svc.get_dashboard(min_score=min_score, vendor=vendor)
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /stock-worthiness/dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/destock-review")
async def get_destock_review(
    db: Session = Depends(get_db),
):
    """Destock review — stocked items that should be reconsidered."""
    cache_key = "sw_destock_review"
    cached = get_cached(cache_key)
    if cached is not _MISS:
        return cached
    try:
        svc = StockWorthinessService(db)
        data = svc.get_destock_review()
        result = {"success": True, "data": data}
        set_cached(cache_key, result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /stock-worthiness/destock-review: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sku-detail")
async def get_sku_detail(
    sku: str = Query(..., min_length=1, description="SKU to analyze"),
    db: Session = Depends(get_db),
):
    """Deep-dive on a single SKU — score breakdown, sparkline, recommendation."""
    try:
        svc = StockWorthinessService(db)
        data = svc.get_sku_detail(sku)
        return {"success": True, "data": data}
    except Exception as e:
        log.error(f"Error in /stock-worthiness/sku-detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))
