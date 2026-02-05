"""
Weekly Strategic Brief API

Endpoints for weekly prioritized action lists.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import date, datetime, timedelta

from app.models.base import get_db
from app.services.weekly_brief_service import WeeklyBriefService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/brief", tags=["brief"])


@router.get("/current")
async def get_current_brief(
    db: Session = Depends(get_db)
):
    """
    Get the current week's strategic brief

    Returns:
    - Top 3-5 priorities
    - What's working well
    - Watch list
    - Week-over-week trends
    - Data quality status
    """
    try:
        service = WeeklyBriefService(db)
        brief = await service.get_current_brief()

        if not brief:
            raise HTTPException(
                status_code=404,
                detail="No brief available. Generate one with POST /brief/generate"
            )

        return {
            "success": True,
            "data": brief
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting current brief: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate")
async def generate_brief(
    week_start: Optional[str] = Query(None, description="Week start date (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    Generate a new weekly brief

    Aggregates insights from all modules and creates prioritized action list
    """
    try:
        service = WeeklyBriefService(db)

        # Parse week_start if provided
        week_start_date = None
        if week_start:
            week_start_date = datetime.strptime(week_start, "%Y-%m-%d").date()

        brief = await service.generate_weekly_brief(week_start=week_start_date)

        return {
            "success": True,
            "message": "Weekly brief generated successfully",
            "data": brief
        }

    except Exception as e:
        log.error(f"Error generating brief: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_brief_history(
    limit: int = Query(10, description="Number of briefs to return"),
    db: Session = Depends(get_db)
):
    """
    Get previous weekly briefs

    Returns historical briefs for trend analysis
    """
    try:
        from app.models.weekly_brief import WeeklyBrief

        briefs = db.query(WeeklyBrief).order_by(
            WeeklyBrief.week_start_date.desc()
        ).limit(limit).all()

        results = []
        for brief in briefs:
            results.append({
                'brief_id': brief.id,
                'week_start': str(brief.week_start_date),
                'week_end': str(brief.week_end_date),
                'total_priorities': brief.total_priorities,
                'total_impact': float(brief.total_estimated_impact),
                'data_quality_score': brief.data_quality_score,
                'is_current': brief.is_current,
                'generated_at': brief.generated_at.isoformat()
            })

        return {
            "success": True,
            "data": {
                "briefs": results,
                "total_count": len(results)
            }
        }

    except Exception as e:
        log.error(f"Error getting brief history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/priorities")
async def get_priorities(
    priority_level: Optional[str] = Query(None, description="Filter by priority level (high, medium, low)"),
    status: Optional[str] = Query(None, description="Filter by status (new, in_progress, completed)"),
    db: Session = Depends(get_db)
):
    """
    Get priority list from current brief

    Just the actionable priorities, no other context
    """
    try:
        from app.models.weekly_brief import WeeklyBrief, BriefPriority

        # Get current brief
        brief = db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).first()

        if not brief:
            raise HTTPException(status_code=404, detail="No current brief available")

        # Get priorities
        query = db.query(BriefPriority).filter(
            BriefPriority.brief_id == brief.id
        )

        if priority_level:
            query = query.filter(BriefPriority.priority_level == priority_level)

        if status:
            query = query.filter(BriefPriority.status == status)

        priorities = query.order_by(BriefPriority.priority_rank).all()

        results = []
        for priority in priorities:
            results.append({
                'rank': priority.priority_rank,
                'title': priority.priority_title,
                'description': priority.priority_description,
                'source_module': priority.source_module,
                'impact': {
                    'total': float(priority.total_estimated_impact),
                    'revenue': float(priority.estimated_revenue_impact),
                    'savings': float(priority.estimated_cost_savings),
                    'timeframe': priority.impact_timeframe
                },
                'effort': {
                    'level': priority.effort_level,
                    'hours': priority.effort_hours
                },
                'confidence': priority.confidence_level,
                'action': priority.recommended_action,
                'priority_level': priority.priority_level,
                'status': priority.status
            })

        return {
            "success": True,
            "data": {
                "priorities": results,
                "total_count": len(results),
                "total_impact": sum(float(p.total_estimated_impact) for p in priorities)
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting priorities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/working")
async def get_working_well(
    db: Session = Depends(get_db)
):
    """
    Get things that are working well

    Don't touch these - they're performing above benchmarks
    """
    try:
        from app.models.weekly_brief import WeeklyBrief

        brief = db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).first()

        if not brief:
            raise HTTPException(status_code=404, detail="No current brief available")

        return {
            "success": True,
            "data": {
                "working_well": brief.working_well_items or [],
                "total_count": brief.working_well_count
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting working well items: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/watch")
async def get_watch_list(
    db: Session = Depends(get_db)
):
    """
    Get watch list

    Emerging issues to monitor (not urgent yet)
    """
    try:
        from app.models.weekly_brief import WeeklyBrief

        brief = db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).first()

        if not brief:
            raise HTTPException(status_code=404, detail="No current brief available")

        return {
            "success": True,
            "data": {
                "watch_list": brief.watch_list_items or [],
                "total_count": brief.watch_list_count
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting watch list: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def get_trends(
    db: Session = Depends(get_db)
):
    """
    Get week-over-week trends

    What improved, what declined, what was implemented
    """
    try:
        from app.models.weekly_brief import WeeklyBrief

        brief = db.query(WeeklyBrief).filter(
            WeeklyBrief.is_current == True
        ).first()

        if not brief:
            raise HTTPException(status_code=404, detail="No current brief available")

        return {
            "success": True,
            "data": brief.trends_summary or {
                'improved': [],
                'declined': [],
                'implemented': [],
                'pending': []
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting trends: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-summary")
async def get_llm_summary(
    db: Session = Depends(get_db)
):
    """
    AI-powered executive summary

    High-level strategic analysis and recommendations
    """
    try:
        # Get current brief
        service = WeeklyBriefService(db)
        brief = await service.get_current_brief()

        if not brief:
            raise HTTPException(status_code=404, detail="No current brief available")

        # Generate LLM summary
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": brief
            }

        summary = llm_service.generate_weekly_brief(
            priorities=brief['priorities'][:5],
            working_well=brief['working_well'],
            watch_list=brief['watch_list'],
            trends=brief['trends'],
            data_quality_score=brief['data_quality_score'],
            total_impact=brief['total_impact']
        )

        return {
            "success": True,
            "data": {
                "brief": brief,
                "executive_summary": summary
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error generating LLM summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/priorities/{priority_id}/status")
async def update_priority_status(
    priority_id: int,
    status: str = Query(..., description="New status: in_progress, completed, deferred"),
    db: Session = Depends(get_db)
):
    """
    Update priority status

    Mark priorities as in_progress, completed, or deferred
    """
    try:
        from app.models.weekly_brief import BriefPriority

        priority = db.query(BriefPriority).filter(
            BriefPriority.id == priority_id
        ).first()

        if not priority:
            raise HTTPException(status_code=404, detail=f"Priority {priority_id} not found")

        if status not in ['new', 'in_progress', 'completed', 'deferred']:
            raise HTTPException(status_code=400, detail="Invalid status")

        priority.status = status

        if status == 'completed':
            priority.completed_at = datetime.utcnow()

        db.commit()

        return {
            "success": True,
            "message": f"Priority status updated to {status}",
            "data": {
                'priority_id': priority.id,
                'title': priority.priority_title,
                'status': priority.status
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating priority status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
