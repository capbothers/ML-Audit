"""
Attribution API Endpoints

Provides honest multi-touch attribution analysis.
Answers: "Where should I actually spend my next dollar?"
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel

from app.services.attribution_service import AttributionService
from app.services.llm_service import LLMService
from app.models.base import get_db
from app.utils.logger import log

router = APIRouter(prefix="/attribution", tags=["attribution"])


class AttributionAnalysisRequest(BaseModel):
    """Request for attribution analysis"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


@router.post("/build-journeys")
async def build_customer_journeys(
    request: AttributionAnalysisRequest,
    db = Depends(get_db)
):
    """
    Build customer journeys from touchpoint data

    Groups touchpoints by user to create complete journey paths
    """
    end_date = request.end_date or datetime.utcnow()
    start_date = request.start_date or (end_date - timedelta(days=30))

    service = AttributionService(db)

    try:
        journeys = await service.build_customer_journeys(start_date, end_date)

        return {
            "status": "success",
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "journeys_built": len(journeys),
            "sample_journeys": journeys[:5]  # First 5 as examples
        }

    except Exception as e:
        log.error(f"Error building journeys: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate")
async def calculate_channel_attribution(
    request: AttributionAnalysisRequest,
    db = Depends(get_db)
):
    """
    Calculate multi-touch attribution by channel

    Compares different attribution models:
    - Last-click (Google's default)
    - First-click
    - Linear (equal credit)
    - Time decay (recent touchpoints get more credit)
    - Position-based (40-20-40)

    Returns which channels are over/under-credited by last-click
    """
    end_date = request.end_date or datetime.utcnow()
    start_date = request.start_date or (end_date - timedelta(days=30))

    service = AttributionService(db)

    try:
        attribution = await service.calculate_channel_attribution(
            start_date=start_date,
            end_date=end_date
        )

        return {
            "status": "success",
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "channels_analyzed": len(attribution),
            "attribution_by_channel": attribution
        }

    except Exception as e:
        log.error(f"Error calculating attribution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights")
async def get_attribution_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get attribution insights showing over/under-credited channels

    This is the key endpoint - shows you:
    - Which channels Google Ads over-credits
    - Which channels are under-credited (doing more than Google shows)
    - Budget reallocation recommendations
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        insights = await service.get_attribution_insights(start_date, end_date)

        return insights

    except Exception as e:
        log.error(f"Error getting attribution insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overcredited")
async def get_overcredited_channels(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get channels that are OVERCREDITED by last-click attribution

    These channels get too much credit in Google's reporting.
    Usually includes:
    - Brand search campaigns (customers would convert anyway)
    - Direct traffic (last touch but influenced earlier)

    Recommendation: Reduce spend on these, shift to prospecting
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        insights = await service.get_attribution_insights(start_date, end_date)

        return {
            "period_days": days,
            "overcredited_channels": insights.get('overcredited_channels', []),
            "message": "These channels get too much credit from last-click attribution"
        }

    except Exception as e:
        log.error(f"Error getting overcredited channels: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/undercredited")
async def get_undercredited_channels(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get channels that are UNDERCREDITED by last-click attribution

    These channels are doing more work than Google's reporting shows.
    Usually includes:
    - Email marketing (assists but doesn't get last click)
    - Display/awareness campaigns
    - Social media (starts journey, doesn't finish it)

    Recommendation: These deserve more budget
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        insights = await service.get_attribution_insights(start_date, end_date)

        return {
            "period_days": days,
            "undercredited_channels": insights.get('undercredited_channels', []),
            "message": "These channels are doing more work than last-click shows"
        }

    except Exception as e:
        log.error(f"Error getting undercredited channels: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/budget-recommendations")
async def get_budget_recommendations(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get budget reallocation recommendations

    Based on multi-touch attribution, shows:
    - Which channels to reduce spend on
    - Which channels to increase spend on
    - Expected impact

    This answers: "Where should I move my ad budget?"
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        insights = await service.get_attribution_insights(start_date, end_date)

        return {
            "period_days": days,
            "recommendations": insights.get('budget_recommendations', []),
            "message": "Budget reallocation opportunities based on multi-touch attribution"
        }

    except Exception as e:
        log.error(f"Error getting budget recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/journey-analysis")
async def get_journey_analysis(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Analyze customer journey patterns

    Shows:
    - Most common paths to conversion
    - Average number of touchpoints before converting
    - Average time to conversion
    - Touchpoint distribution

    Helps understand typical customer behavior
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        analysis = await service.get_journey_analysis(start_date, end_date)

        return {
            "period_days": days,
            **analysis
        }

    except Exception as e:
        log.error(f"Error analyzing journeys: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_attribution_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    LLM-Powered Attribution Analysis

    Claude analyzes the attribution data and tells you:
    - Why channels are over/under-credited
    - Specific budget moves to make
    - Expected impact
    - Priority actions

    This is the "so what?" - what the data actually means
    """
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        # Get attribution insights
        insights = await service.get_attribution_insights(start_date, end_date)

        if 'message' in insights and 'no' in insights['message'].lower():
            return {
                "message": "No attribution data available for LLM analysis",
                "recommendation": "Ensure touchpoint data is being collected from GA4 and ad platforms"
            }

        # Generate LLM analysis
        analysis = llm_service.analyze_attribution(
            overcredited=insights.get('overcredited_channels', []),
            undercredited=insights.get('undercredited_channels', []),
            budget_recommendations=insights.get('budget_recommendations', []),
            summary=insights.get('summary', {})
        )

        return {
            "period_days": days,
            "data_summary": insights.get('summary'),
            "llm_analysis": analysis,
            "budget_recommendations": insights.get('budget_recommendations', [])[:3]
        }

    except Exception as e:
        log.error(f"Error generating LLM attribution insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
async def get_attribution_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Complete attribution dashboard

    Everything you need to understand true channel performance:
    - All channels with multi-touch attribution
    - Over/under-credited channels
    - Budget recommendations
    - Journey analysis
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        # Get all data
        insights = await service.get_attribution_insights(start_date, end_date)
        journey_analysis = await service.get_journey_analysis(start_date, end_date)

        return {
            "period_days": days,
            "generated_at": datetime.utcnow().isoformat(),

            "summary": insights.get('summary'),
            "all_channels": insights.get('all_channels', []),

            "overcredited_channels": insights.get('overcredited_channels', []),
            "undercredited_channels": insights.get('undercredited_channels', []),

            "budget_recommendations": insights.get('budget_recommendations', []),

            "journey_patterns": journey_analysis,

            "key_insights": {
                "biggest_overcredit": insights['overcredited_channels'][0] if insights.get('overcredited_channels') else None,
                "biggest_undercredit": insights['undercredited_channels'][0] if insights.get('undercredited_channels') else None,
                "top_budget_move": insights['budget_recommendations'][0] if insights.get('budget_recommendations') else None
            }
        }

    except Exception as e:
        log.error(f"Error generating attribution dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare-models")
async def compare_attribution_models(
    channel: str = Query(..., description="Channel to analyze"),
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Compare attribution models for a specific channel

    Shows how different models credit the same channel:
    - Last-click (Google's version)
    - First-click
    - Linear
    - Time decay
    - Position-based

    Helps understand if channel is over/under valued
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = AttributionService(db)

    try:
        attribution = await service.calculate_channel_attribution(start_date, end_date)

        # Find the specific channel
        channel_data = next((ch for ch in attribution if ch['channel'] == channel), None)

        if not channel_data:
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel}' not found in attribution data"
            )

        comparison = {
            "channel": channel,
            "period_days": days,

            "attribution_models": {
                "last_click": {
                    "conversions": channel_data['last_click_conversions'],
                    "revenue": channel_data['last_click_revenue'],
                    "credit_pct": channel_data['last_click_credit_pct']
                },
                "linear": {
                    "conversions": channel_data['linear_conversions'],
                    "revenue": channel_data['linear_revenue'],
                    "credit_pct": channel_data['linear_credit_pct']
                },
                "time_decay": {
                    "conversions": channel_data['time_decay_conversions'],
                    "revenue": channel_data['time_decay_revenue']
                },
                "position_based": {
                    "conversions": channel_data['position_conversions'],
                    "revenue": channel_data['position_revenue']
                }
            },

            "analysis": {
                "credit_difference": channel_data['credit_difference_pct'],
                "is_overcredited": channel_data['is_overcredited'],
                "is_undercredited": channel_data['is_undercredited'],
                "assist_ratio": channel_data['assist_ratio']
            },

            "explanation": self._explain_credit_difference(channel_data)
        }

        return comparison

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error comparing attribution models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def _explain_credit_difference(channel_data: Dict) -> str:
    """Explain what the credit difference means"""
    diff = channel_data['credit_difference_pct']

    if channel_data['is_overcredited']:
        return f"Last-click gives {abs(diff):.1f}% more credit than multi-touch models. This channel gets too much credit in Google's reporting. Consider reducing spend."

    elif channel_data['is_undercredited']:
        return f"Multi-touch gives {diff:.1f}% more credit than last-click. This channel is doing more work than Google's reporting shows. Consider increasing spend."

    else:
        return "Attribution models are aligned. This channel is fairly credited."
