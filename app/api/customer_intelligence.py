"""
Customer Intelligence API

Endpoints for RFM analysis, cohort retention, customer drill-down,
brand affinity, and geographic distribution.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.customer_intelligence_service import CustomerIntelligenceService
from app.utils.response_cache import response_cache

router = APIRouter(prefix="/customers", tags=["customers"])


@router.get("/dashboard")
async def get_customer_dashboard(
    db: Session = Depends(get_db),
):
    """Full customer intelligence dashboard payload (all 4 tabs)."""
    cached = response_cache.get("customers:dashboard")
    if cached:
        return cached
    try:
        service = CustomerIntelligenceService(db)
        data = service.get_dashboard()
        result = {"success": True, "data": data}
        response_cache.set("customers:dashboard", result, ttl=300)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail")
async def get_customer_detail(
    email: str = Query(..., description="Customer email"),
    db: Session = Depends(get_db),
):
    """Customer drill-down modal data."""
    try:
        service = CustomerIntelligenceService(db)
        data = service.get_customer_detail(email)
        if not data:
            raise HTTPException(status_code=404, detail="Customer not found")
        return {"success": True, "data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_customers(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Max results (1-100)"),
    db: Session = Depends(get_db),
):
    """Search customers by name or email."""
    try:
        service = CustomerIntelligenceService(db)
        results = service.search_customers(query=q, limit=limit)
        return {"success": True, "count": len(results), "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rfm-segments")
async def get_rfm_segments(
    db: Session = Depends(get_db),
):
    """Detailed RFM segment data with action recommendations."""
    try:
        service = CustomerIntelligenceService(db)
        data = service.get_rfm_segments()
        return {"success": True, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cohort-data")
async def get_cohort_data(
    db: Session = Depends(get_db),
):
    """Cohort retention heatmap data."""
    try:
        service = CustomerIntelligenceService(db)
        data = service.get_cohort_data()
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brand-affinity")
async def get_brand_affinity(
    db: Session = Depends(get_db),
):
    """Brand co-purchase analysis."""
    try:
        service = CustomerIntelligenceService(db)
        data = service.get_brand_affinity()
        return {"success": True, "count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
