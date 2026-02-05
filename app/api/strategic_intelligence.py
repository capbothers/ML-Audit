"""
Strategic Intelligence API

Daily and weekly intelligence briefs with LLM-powered analysis,
cross-module correlations, CRO deep-dives, and growth playbooks.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.strategic_intelligence_service import StrategicIntelligenceService

router = APIRouter(prefix="/intelligence", tags=["intelligence"])


# === DAILY BRIEF ===

@router.get("/daily/current")
async def get_current_daily_brief(db: Session = Depends(get_db)):
    """Get today's daily intelligence brief."""
    try:
        service = StrategicIntelligenceService(db)
        data = service.get_current_brief('daily')
        if not data:
            return {"success": True, "data": None, "message": "No daily brief generated yet. Click Generate to create one."}
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/daily/generate")
def generate_daily_brief(
    target_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    db: Session = Depends(get_db),
):
    """Generate a fresh daily intelligence brief (runs in threadpool for async module calls)."""
    try:
        from datetime import date as d
        td = d.fromisoformat(target_date) if target_date else None
        service = StrategicIntelligenceService(db)
        data = service.generate_daily_brief(td)
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === WEEKLY BRIEF ===

@router.get("/weekly/current")
async def get_current_weekly_brief(db: Session = Depends(get_db)):
    """Get this week's strategic brief."""
    try:
        service = StrategicIntelligenceService(db)
        data = service.get_current_brief('weekly')
        if not data:
            return {"success": True, "data": None, "message": "No weekly brief generated yet. Click Generate to create one."}
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/weekly/generate")
def generate_weekly_brief(
    week_start: Optional[str] = Query(None, description="YYYY-MM-DD (Monday)"),
    db: Session = Depends(get_db),
):
    """Generate a fresh weekly intelligence brief (runs in threadpool for async module calls)."""
    try:
        from datetime import date as d
        ws = d.fromisoformat(week_start) if week_start else None
        service = StrategicIntelligenceService(db)
        data = service.generate_weekly_brief(ws)
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === SECTION DEEP-DIVES ===

@router.get("/cro-analysis")
async def get_cro_analysis(db: Session = Depends(get_db)):
    """Get the CRO analysis section from the current brief."""
    try:
        service = StrategicIntelligenceService(db)
        brief = service.get_current_brief('daily') or service.get_current_brief('weekly')
        if not brief:
            return {"success": True, "data": None}
        return {"success": True, "data": {
            "conversion_analysis": brief.get("conversion_analysis", ""),
            "kpi_snapshot": brief.get("kpi_snapshot", {}),
        }}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/growth-playbook")
async def get_growth_playbook(db: Session = Depends(get_db)):
    """Get the growth playbook from the current weekly brief."""
    try:
        service = StrategicIntelligenceService(db)
        brief = service.get_current_brief('weekly')
        if not brief:
            return {"success": True, "data": None}
        return {"success": True, "data": {
            "growth_playbook": brief.get("growth_playbook", ""),
            "kpi_snapshot": brief.get("kpi_snapshot", {}),
        }}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/issues")
async def get_issues(
    severity: Optional[str] = Query(None),
    module: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get all issues across modules with optional filtering."""
    try:
        service = StrategicIntelligenceService(db)
        brief = service.get_current_brief('daily') or service.get_current_brief('weekly')
        if not brief:
            return {"success": True, "data": [], "count": 0}
        issues = brief.get("issue_command_center", [])
        if isinstance(issues, list):
            if module:
                issues = [i for i in issues if i.get('source_module') == module]
        return {"success": True, "data": issues, "count": len(issues) if isinstance(issues, list) else 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations")
async def get_correlations(db: Session = Depends(get_db)):
    """Get cross-module correlations."""
    try:
        service = StrategicIntelligenceService(db)
        brief = service.get_current_brief('daily') or service.get_current_brief('weekly')
        if not brief:
            return {"success": True, "data": []}
        return {"success": True, "data": brief.get("cross_module_correlations", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights")
async def get_ai_insights(db: Session = Depends(get_db)):
    """Get AI strategic insights."""
    try:
        service = StrategicIntelligenceService(db)
        brief = service.get_current_brief('daily') or service.get_current_brief('weekly')
        if not brief:
            return {"success": True, "data": None}
        return {"success": True, "data": {
            "ai_strategic_insights": brief.get("ai_strategic_insights", ""),
            "whats_working": brief.get("whats_working", []),
            "watch_list": brief.get("watch_list", []),
        }}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === HISTORY ===

@router.get("/history")
async def get_brief_history(
    cadence: str = Query("weekly"),
    limit: int = Query(12),
    db: Session = Depends(get_db),
):
    """Get historical briefs for trend analysis."""
    try:
        service = StrategicIntelligenceService(db)
        briefs = service.get_brief_history(cadence, limit)
        return {"success": True, "data": briefs, "count": len(briefs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# === RECOMMENDATION TRACKING ===

@router.put("/recommendations/{rec_id}/status")
async def update_recommendation_status(
    rec_id: int,
    status: str = Query(..., description="new|in_progress|completed|deferred"),
    actual_impact: Optional[float] = Query(None),
    db: Session = Depends(get_db),
):
    """Track recommendation implementation."""
    try:
        service = StrategicIntelligenceService(db)
        result = service.update_recommendation_status(rec_id, status, actual_impact)
        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])
        return {"success": True, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/track")
async def get_recommendation_tracking(
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get all recommendations with implementation status."""
    try:
        service = StrategicIntelligenceService(db)
        recs = service.get_recommendations(status, category)
        return {"success": True, "data": recs, "count": len(recs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
