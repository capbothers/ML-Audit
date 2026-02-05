"""
Merchant Center Intelligence API

Endpoints for product visibility, feed health, issue breakdown,
category risk, GTIN coverage, price drift, and revenue-at-risk analysis.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.models.base import get_db
from app.services.merchant_center_intelligence_service import (
    MerchantCenterIntelligenceService,
)

router = APIRouter(prefix="/merchant-center", tags=["merchant-center"])


@router.get("/dashboard")
async def get_merchant_center_dashboard(
    db: Session = Depends(get_db),
):
    """Full merchant center intelligence dashboard payload (all tabs)."""
    try:
        service = MerchantCenterIntelligenceService(db)
        data = service.get_dashboard()
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/product-detail")
async def get_product_detail(
    product_id: str = Query(..., description="Shopify product ID"),
    db: Session = Depends(get_db),
):
    """Product drill-down modal data."""
    try:
        service = MerchantCenterIntelligenceService(db)
        data = service.get_product_detail(product_id)
        if not data:
            raise HTTPException(status_code=404, detail="Product not found")
        return {"success": True, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_products(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, description="Max results"),
    db: Session = Depends(get_db),
):
    """Search products by title or SKU."""
    try:
        service = MerchantCenterIntelligenceService(db)
        results = service.search_products(query=q, limit=limit)
        return {"success": True, "count": len(results), "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
