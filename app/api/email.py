"""
Email & Retention Intelligence API

Endpoints for email marketing performance analysis.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.email_service import EmailService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/email", tags=["email"])


@router.get("/dashboard")
async def get_email_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Complete email intelligence dashboard

    Returns:
    - Overall email performance
    - Top revenue opportunities
    - Flow performance
    - Segment health
    - Frequency analysis
    """
    try:
        service = EmailService(db)
        dashboard = await service.get_email_dashboard(days)

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating email dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_all_opportunities(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    All email revenue opportunities

    Returns:
    - Underperforming flows
    - Under-contacted segments
    - Frequency optimization
    - Missing flows
    """
    try:
        service = EmailService(db)
        opportunities = await service.analyze_all_opportunities(days)

        return {
            "success": True,
            "data": opportunities
        }

    except Exception as e:
        log.error(f"Error getting email opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flows")
async def get_flow_performance(
    days: int = Query(30, description="Number of days to analyze"),
    underperforming_only: bool = Query(False, description="Show only underperforming flows"),
    db: Session = Depends(get_db)
):
    """
    Email flow performance vs benchmarks

    Shows which flows are underperforming and need attention
    """
    try:
        service = EmailService(db)
        flows = await service.find_underperforming_flows(days)

        return {
            "success": True,
            "data": {
                "flows": flows,
                "total_count": len(flows),
                "total_revenue_gap": sum(f.get('estimated_revenue_gap', 0) for f in flows)
            }
        }

    except Exception as e:
        log.error(f"Error getting flow performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/segments")
async def get_segment_health(
    db: Session = Depends(get_db)
):
    """
    Email segment health analysis

    Identifies high-value segments that aren't being contacted enough
    """
    try:
        service = EmailService(db)
        segments = await service.find_under_contacted_segments()

        return {
            "success": True,
            "data": {
                "segments": segments,
                "total_count": len(segments),
                "total_revenue_opportunity": sum(s.get('revenue_opportunity', 0) for s in segments)
            }
        }

    except Exception as e:
        log.error(f"Error getting segment health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/frequency")
async def get_frequency_analysis(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Email send frequency analysis

    Answers: Are we sending too much or too little?
    """
    try:
        service = EmailService(db)
        frequency = await service.analyze_send_frequency(days)

        if not frequency:
            raise HTTPException(status_code=404, detail="No frequency data available")

        return {
            "success": True,
            "data": frequency
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting frequency analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/missing-flows")
async def get_missing_flows(
    db: Session = Depends(get_db)
):
    """
    Identify missing standard flows

    Returns flows that should exist but don't
    """
    try:
        service = EmailService(db)
        missing = await service.identify_missing_flows()

        return {
            "success": True,
            "data": {
                "missing_flows": missing,
                "total_count": len(missing),
                "total_revenue_opportunity": sum(f.get('estimated_monthly_revenue', 0) for f in missing)
            }
        }

    except Exception as e:
        log.error(f"Error getting missing flows: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_email_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    AI-powered email insights and prioritization

    Uses Claude to analyze email performance and provide strategic recommendations
    """
    try:
        # Get email data
        email_service = EmailService(db)
        opportunities = await email_service.analyze_all_opportunities(days)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": opportunities
            }

        analysis = llm_service.analyze_email_performance(
            underperforming_flows=opportunities['opportunities']['underperforming_flows'],
            under_contacted_segments=opportunities['opportunities']['under_contacted_segments'],
            frequency_analysis=opportunities['opportunities']['frequency_optimization'],
            missing_flows=opportunities['opportunities']['missing_flows'],
            summary=opportunities['summary']
        )

        return {
            "success": True,
            "data": {
                "opportunities": opportunities,
                "llm_analysis": analysis
            }
        }

    except Exception as e:
        log.error(f"Error generating LLM insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flow/{flow_id}")
async def get_flow_details(
    flow_id: str,
    db: Session = Depends(get_db)
):
    """
    Detailed analysis for a specific flow

    Returns performance metrics and recommendations
    """
    try:
        from app.models.email import EmailFlow

        flow = db.query(EmailFlow).filter(
            EmailFlow.flow_id == flow_id
        ).first()

        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow {flow_id} not found")

        # Get benchmark
        service = EmailService(db)
        benchmark = service.flow_benchmarks.get(
            flow.flow_type,
            service.flow_benchmarks['default']
        )

        return {
            "success": True,
            "data": {
                "flow_id": flow.flow_id,
                "flow_name": flow.flow_name,
                "flow_type": flow.flow_type,
                "is_active": flow.is_active,
                "total_emails": flow.total_emails,

                "performance": {
                    "total_entered": flow.total_entered,
                    "open_rate": round(flow.open_rate * 100, 1),
                    "click_rate": round(flow.click_rate * 100, 1),
                    "conversion_rate": round(flow.conversion_rate * 100, 1),
                    "total_revenue": float(flow.total_revenue),
                    "revenue_per_recipient": float(flow.revenue_per_recipient)
                },

                "benchmark": {
                    "open_rate": round(benchmark['open_rate'] * 100, 1),
                    "click_rate": round(benchmark['click_rate'] * 100, 1),
                    "conversion_rate": round(benchmark['conversion_rate'] * 100, 1)
                },

                "vs_benchmark": {
                    "open_rate": round((flow.open_rate - benchmark['open_rate']) * 100, 1),
                    "click_rate": round((flow.click_rate - benchmark['click_rate']) * 100, 1),
                    "conversion_rate": round((flow.conversion_rate - benchmark['conversion_rate']) * 100, 1)
                },

                "is_underperforming": flow.is_underperforming,
                "issues": flow.issues_detected,
                "recommended_actions": flow.recommended_actions
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting flow details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
