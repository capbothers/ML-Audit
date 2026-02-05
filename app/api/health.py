"""
Health check and status endpoints
"""
from fastapi import APIRouter
from datetime import datetime
from app.config import get_settings
from app import __version__

settings = get_settings()

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": __version__
    }


@router.get("/status")
async def get_status():
    """Get system status"""
    return {
        "app_name": settings.app_name,
        "version": __version__,
        "environment": settings.environment,
        "features": {
            "churn_prediction": settings.enable_churn_prediction,
            "anomaly_detection": settings.enable_anomaly_detection,
            "seo_analysis": settings.enable_seo_analysis,
            "ad_monitoring": settings.enable_ad_monitoring,
            "auto_alerts": settings.enable_auto_alerts
        },
        "timestamp": datetime.utcnow().isoformat()
    }
