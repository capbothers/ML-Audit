"""
ML Intelligence API

Endpoints for forecasting, anomaly detection, revenue drivers,
tracking health, inventory suggestions, and inventory intelligence.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.ml_intelligence_service import MLIntelligenceService
from app.services.inventory_intelligence_service import InventoryIntelligenceService

router = APIRouter(prefix="/ml", tags=["ml"])


@router.get("/forecast")
async def get_forecast(
    metric: Optional[str] = Query(None, description="Filter by metric: revenue, orders, sessions"),
    days: int = Query(7, description="Forecast horizon in days"),
    db: Session = Depends(get_db),
):
    """Get the most recent forecasts."""
    try:
        service = MLIntelligenceService(db)
        forecasts = service.get_forecasts(metric=metric, days=days)

        return {
            "success": True,
            "count": len(forecasts),
            "data": forecasts,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies")
async def get_anomalies(
    days: int = Query(30, description="Lookback days"),
    severity: Optional[str] = Query(None, description="Filter: low, medium, high, critical"),
    metric: Optional[str] = Query(None, description="Filter by metric"),
    unacknowledged_only: bool = Query(False, description="Only unacknowledged"),
    db: Session = Depends(get_db),
):
    """Get detected anomalies."""
    try:
        service = MLIntelligenceService(db)
        anomalies = service.get_anomalies(
            days=days,
            severity=severity,
            metric=metric,
            unacknowledged_only=unacknowledged_only,
        )

        return {
            "success": True,
            "count": len(anomalies),
            "data": anomalies,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/anomalies/{anomaly_id}/acknowledge")
async def acknowledge_anomaly(
    anomaly_id: int,
    db: Session = Depends(get_db),
):
    """Mark an anomaly as acknowledged."""
    try:
        service = MLIntelligenceService(db)
        result = service.acknowledge_anomaly(anomaly_id)

        if not result:
            raise HTTPException(status_code=404, detail="Anomaly not found")

        return {"success": True, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drivers")
async def get_revenue_drivers(
    days: int = Query(7, description="Comparison period in days"),
    db: Session = Depends(get_db),
):
    """
    Revenue driver decomposition: Sessions x CR x AOV.

    Compares current N days vs preceding N days.
    """
    try:
        service = MLIntelligenceService(db)
        drivers = service.get_revenue_drivers(days=days)

        return {"success": True, "data": drivers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracking-health")
async def get_tracking_health(
    days: int = Query(7, description="Lookback days"),
    db: Session = Depends(get_db),
):
    """
    GA4 vs Shopify tracking gap analysis.

    Compares daily order counts and revenue between GA4 and Shopify.
    """
    try:
        service = MLIntelligenceService(db)
        health = service.get_tracking_health(days=days)

        return {"success": True, "data": health}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-suggestions")
async def get_inventory_suggestions(
    brand: Optional[str] = Query(None, description="Filter by brand"),
    urgency: Optional[str] = Query(None, description="Filter: critical, warning, ok"),
    suggestion: Optional[str] = Query(None, description="Filter: reorder_now, reorder_soon, adequate, overstock, no_sales"),
    db: Session = Depends(get_db),
):
    """Get inventory reorder suggestions based on sales velocity."""
    try:
        service = MLIntelligenceService(db)
        suggestions = service.get_inventory_suggestions(
            brand=brand, urgency=urgency, suggestion=suggestion
        )

        return {
            "success": True,
            "count": len(suggestions),
            "data": suggestions,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-dashboard")
async def get_inventory_dashboard(
    db: Session = Depends(get_db),
):
    """Full inventory intelligence dashboard payload (Tab 1: Pulse)."""
    try:
        service = InventoryIntelligenceService(db)
        data = service.get_dashboard_data()
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-reorder-queue")
async def get_inventory_reorder_queue(
    page: int = Query(1, description="Page number"),
    per_page: int = Query(25, description="Items per page"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    db: Session = Depends(get_db),
):
    """Paginated reorder queue with cost estimates (Tab 2)."""
    try:
        service = InventoryIntelligenceService(db)
        data = service.get_reorder_queue(page=page, per_page=per_page, brand=brand)
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-stock-health")
async def get_inventory_stock_health(
    db: Session = Depends(get_db),
):
    """Stock health analysis: overstock, dead stock, brand health (Tab 3)."""
    try:
        service = InventoryIntelligenceService(db)
        data = service.get_stock_health()
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-sku-detail")
async def get_inventory_sku_detail(
    sku: str = Query(..., description="SKU to look up"),
    db: Session = Depends(get_db),
):
    """Full SKU drill-down for modal (Tab 4)."""
    try:
        service = InventoryIntelligenceService(db)
        data = service.get_sku_detail(sku)
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-sku-search")
async def search_inventory_skus(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, description="Max results"),
    db: Session = Depends(get_db),
):
    """Search SKUs by partial match on sku, brand, or title."""
    try:
        service = InventoryIntelligenceService(db)
        results = service.search_skus(query=q, limit=limit)
        return {"success": True, "count": len(results), "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run")
async def run_ml_pipeline(
    db: Session = Depends(get_db),
):
    """Manually trigger the full ML pipeline."""
    try:
        service = MLIntelligenceService(db)
        results = service.run_daily_ml_pipeline()

        return {"success": True, "data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
