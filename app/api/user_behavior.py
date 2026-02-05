"""
User Behavior Intelligence API

Endpoints for analyzing user behavior friction points and UX issues.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.user_behavior_service import UserBehaviorService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/behavior", tags=["behavior"])


@router.get("/dashboard")
async def get_behavior_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Complete user behavior intelligence dashboard

    Returns:
    - Top priority UX issues
    - High-friction pages
    - Checkout funnel analysis
    - Mobile vs desktop issues
    - Quick wins
    """
    try:
        service = UserBehaviorService(db)
        dashboard = await service.get_behavior_dashboard(days)

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating behavior dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/friction")
async def get_high_friction_pages(
    days: int = Query(30, description="Number of days to analyze"),
    min_traffic: int = Query(100, description="Minimum monthly sessions"),
    db: Session = Depends(get_db)
):
    """
    High-friction pages analysis

    Pages with:
    - High traffic but low conversion
    - Rage clicks
    - Dead clicks
    - Poor scroll depth
    """
    try:
        service = UserBehaviorService(db)
        friction_pages = await service.find_high_friction_pages(days)

        # Filter by min traffic
        if min_traffic:
            friction_pages = [
                p for p in friction_pages
                if p['traffic']['monthly_sessions'] >= min_traffic
            ]

        return {
            "success": True,
            "data": {
                "pages": friction_pages,
                "total_count": len(friction_pages),
                "total_revenue_impact": sum(
                    p['revenue_impact']['estimated_revenue_lost']
                    for p in friction_pages
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting high-friction pages: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/checkout-funnel")
async def get_checkout_funnel_analysis(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Checkout funnel step-by-step analysis

    Shows:
    - Drop-off rate at each step
    - Biggest leak in funnel
    - Mobile vs desktop completion
    - Friction signals (rage clicks, errors)
    """
    try:
        service = UserBehaviorService(db)
        funnel = await service.analyze_checkout_funnel(days)

        if not funnel:
            raise HTTPException(status_code=404, detail="No checkout funnel data available")

        # Find biggest leak
        biggest_leak = max(funnel, key=lambda x: x['metrics']['drop_off_rate']) if funnel else None

        return {
            "success": True,
            "data": {
                "funnel_steps": funnel,
                "total_steps": len(funnel),
                "biggest_leak": {
                    "step_name": biggest_leak['step_name'],
                    "step_number": biggest_leak['step_number'],
                    "drop_off_rate": biggest_leak['metrics']['drop_off_rate'],
                    "revenue_lost": biggest_leak['revenue_impact']['estimated_revenue_lost']
                } if biggest_leak else None,
                "total_revenue_impact": sum(
                    s['revenue_impact']['estimated_revenue_lost']
                    for s in funnel
                )
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error analyzing checkout funnel: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mobile-issues")
async def get_mobile_issues(
    days: int = Query(30, description="Number of days to analyze"),
    min_gap: float = Query(5.0, description="Minimum conversion gap (% points)"),
    db: Session = Depends(get_db)
):
    """
    Mobile vs desktop performance issues

    Identifies pages where mobile significantly underperforms:
    - Lower conversion rates
    - Rage clicks
    - Touch target issues
    - Layout problems
    """
    try:
        service = UserBehaviorService(db)
        mobile_issues = await service.find_mobile_issues(days)

        # Filter by minimum gap
        if min_gap:
            mobile_issues = [
                i for i in mobile_issues
                if i['conversion_comparison']['gap'] and abs(i['conversion_comparison']['gap']) >= min_gap
            ]

        return {
            "success": True,
            "data": {
                "issues": mobile_issues,
                "total_count": len(mobile_issues),
                "total_revenue_impact": sum(
                    i['revenue_impact']['estimated_revenue_lost']
                    for i in mobile_issues
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting mobile issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rage-clicks")
async def get_rage_click_pages(
    days: int = Query(30, description="Number of days to analyze"),
    min_rage_clicks: int = Query(10, description="Minimum rage clicks to report"),
    db: Session = Depends(get_db)
):
    """
    Pages with rage clicks (user frustration)

    Rage clicks indicate:
    - Non-clickable elements users expect to work
    - Broken functionality
    - Unclear UI
    """
    try:
        service = UserBehaviorService(db)
        rage_pages = await service.find_rage_click_pages(days)

        # Filter by minimum rage clicks
        if min_rage_clicks:
            rage_pages = [
                p for p in rage_pages
                if p['rage_clicks']['total_rage_clicks'] >= min_rage_clicks
            ]

        return {
            "success": True,
            "data": {
                "pages": rage_pages,
                "total_count": len(rage_pages),
                "total_rage_clicks": sum(
                    p['rage_clicks']['total_rage_clicks']
                    for p in rage_pages
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting rage click pages: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/session-patterns")
async def get_session_patterns(
    days: int = Query(30, description="Number of days to analyze"),
    pattern_type: Optional[str] = Query(None, description="Filter by pattern type (frustration, abandonment, success)"),
    db: Session = Depends(get_db)
):
    """
    Common session behavior patterns

    Identifies:
    - Frustration patterns
    - Abandonment sequences
    - Success patterns
    """
    try:
        service = UserBehaviorService(db)
        patterns = await service.analyze_session_patterns(days)

        # Filter by pattern type if specified
        if pattern_type:
            patterns = [
                p for p in patterns
                if p['pattern_type'] == pattern_type
            ]

        return {
            "success": True,
            "data": {
                "patterns": patterns,
                "total_count": len(patterns),
                "total_sessions_affected": sum(
                    p['prevalence']['sessions_with_pattern']
                    for p in patterns
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting session patterns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/page/{page_path:path}")
async def get_page_analysis(
    page_path: str,
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Detailed analysis for a specific page

    Returns:
    - Traffic and conversion metrics
    - Friction analysis
    - Scroll depth
    - Mobile vs desktop
    - Specific issues and fixes
    """
    try:
        service = UserBehaviorService(db)

        # Ensure page_path starts with /
        if not page_path.startswith('/'):
            page_path = f"/{page_path}"

        analysis = await service.get_page_analysis(page_path, days)

        if not analysis:
            raise HTTPException(
                status_code=404,
                detail=f"No behavior data found for page: {page_path}"
            )

        return {
            "success": True,
            "data": analysis
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error analyzing page {page_path}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_behavior_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    AI-powered user behavior insights

    Uses Claude to analyze UX friction and provide strategic recommendations:
    - Prioritize fixes by revenue impact
    - Diagnose root causes
    - Recommend specific implementation steps
    - Estimate effort and impact
    """
    try:
        # Get behavior data
        behavior_service = UserBehaviorService(db)
        analysis = await behavior_service.analyze_all_behavior(days)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_user_behavior(
            high_friction_pages=analysis['high_friction_pages'],
            checkout_funnel=analysis['checkout_funnel'],
            mobile_issues=analysis['mobile_issues'],
            rage_click_pages=analysis['rage_click_pages'],
            session_patterns=analysis['session_patterns'],
            summary=analysis['summary']
        )

        return {
            "success": True,
            "data": {
                "analysis": analysis,
                "llm_insights": llm_analysis
            }
        }

    except Exception as e:
        log.error(f"Error generating LLM behavior insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quick-wins")
async def get_quick_wins(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Low-effort, high-impact UX fixes

    Returns simple fixes that can be implemented quickly:
    - Rage click fixes
    - Mobile layout issues
    - Content positioning
    """
    try:
        service = UserBehaviorService(db)
        analysis = await service.analyze_all_behavior(days)

        quick_wins = service._identify_quick_wins(analysis)

        return {
            "success": True,
            "data": {
                "quick_wins": quick_wins,
                "total_count": len(quick_wins)
            }
        }

    except Exception as e:
        log.error(f"Error getting quick wins: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
