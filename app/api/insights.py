"""
Insights and analysis endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel

from app.services.analysis_service import AnalysisService
from app.services.data_sync_service import DataSyncService
from app.utils.logger import log

router = APIRouter(prefix="/insights", tags=["insights"])

analysis_service = AnalysisService()
data_sync = DataSyncService()


class ChurnRequest(BaseModel):
    customer_data: List[dict]


class AnomalyRequest(BaseModel):
    data: List[dict]
    metric_name: str
    method: str = "zscore"


class SEOAuditRequest(BaseModel):
    urls: List[str]


class FullAnalysisRequest(BaseModel):
    customer_data: Optional[List[dict]] = None
    campaign_data: Optional[List[dict]] = None
    traffic_data: Optional[List[dict]] = None
    revenue_data: Optional[List[dict]] = None
    disapproved_ads: Optional[List[dict]] = None
    abandoned_checkouts: Optional[List[dict]] = None
    email_campaigns: Optional[List[dict]] = None
    urls_to_audit: Optional[List[str]] = None


@router.post("/analyze")
async def run_full_analysis(request: FullAnalysisRequest):
    """
    Run comprehensive analysis across all modules
    """
    try:
        results = await analysis_service.run_full_analysis(
            customer_data=request.customer_data,
            campaign_data=request.campaign_data,
            traffic_data=request.traffic_data,
            revenue_data=request.revenue_data,
            disapproved_ads=request.disapproved_ads,
            abandoned_checkouts=request.abandoned_checkouts,
            email_campaigns=request.email_campaigns,
            urls_to_audit=request.urls_to_audit
        )
        return results
    except Exception as e:
        log.error(f"Full analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/churn/predict")
async def predict_churn(request: ChurnRequest):
    """
    Predict customer churn probability
    """
    try:
        predictions = await analysis_service.predict_churn(request.customer_data)
        return {
            "predictions": predictions,
            "total_customers": len(predictions),
            "high_risk_count": len([p for p in predictions if p['churn_risk_level'] == 'HIGH']),
            "medium_risk_count": len([p for p in predictions if p['churn_risk_level'] == 'MEDIUM']),
            "low_risk_count": len([p for p in predictions if p['churn_risk_level'] == 'LOW'])
        }
    except Exception as e:
        log.error(f"Churn prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/churn/train")
async def train_churn_model(request: ChurnRequest):
    """
    Train churn prediction model with new data
    """
    try:
        result = await analysis_service.train_churn_model(request.customer_data)
        return result
    except Exception as e:
        log.error(f"Model training error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anomalies/detect")
async def detect_anomalies(request: AnomalyRequest):
    """
    Detect anomalies in a specific metric
    """
    try:
        anomalies = await analysis_service.detect_anomalies(
            request.data,
            request.metric_name,
            request.method
        )
        return {
            "anomalies": anomalies,
            "total_anomalies": len(anomalies),
            "critical_anomalies": len([a for a in anomalies if a.get('severity') == 'critical'])
        }
    except Exception as e:
        log.error(f"Anomaly detection error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seo/audit")
async def audit_seo(request: SEOAuditRequest):
    """
    Run SEO audit on specified URLs
    """
    try:
        results = await analysis_service.audit_seo(request.urls)
        return results
    except Exception as e:
        log.error(f"SEO audit error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations")
async def get_recommendations(
    include_churn: bool = Query(True),
    include_anomalies: bool = Query(True),
    include_seo: bool = Query(True),
    include_campaigns: bool = Query(True)
):
    """
    Get AI-generated recommendations
    (This would typically pull from database after a full analysis)
    """
    return {
        "message": "Run a full analysis first using POST /insights/analyze to generate recommendations",
        "endpoint": "/insights/analyze"
    }
