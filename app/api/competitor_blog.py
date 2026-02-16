"""
Competitor Blog Intelligence API

Endpoints for viewing, searching, and managing competitor blog content.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.competitor_blog_service import CompetitorBlogService

router = APIRouter(prefix="/competitor-blogs", tags=["competitor-blogs"])
service = CompetitorBlogService()


@router.get("/dashboard")
def get_dashboard(db: Session = Depends(get_db)):
    """Get competitor blog monitoring dashboard"""
    return service.get_dashboard(db)


@router.get("/articles")
def get_articles(
    domain: Optional[str] = Query(None, description="Filter by site domain"),
    site_type: Optional[str] = Query(None, description="Filter by site type: competitor, supplier, industry"),
    search: Optional[str] = Query(None, description="Search in title/content"),
    flagged_only: bool = Query(False, description="Only show flagged articles"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Get filtered list of competitor articles"""
    return service.get_articles(
        db,
        domain=domain,
        site_type=site_type,
        search=search,
        flagged_only=flagged_only,
        limit=limit,
        offset=offset,
    )


@router.post("/sync")
async def sync_competitor_blogs(
    background_tasks: BackgroundTasks,
    days: int = Query(90, ge=1, le=365, description="How many days back to look"),
):
    """Trigger a competitor blog scrape (runs in background)"""
    async def _run_sync():
        result = await service.sync_competitor_blogs(days=days)
        if result.get("success"):
            from app.utils.logger import log
            log.info(
                f"Competitor blog sync done: {result['new_articles']} new, "
                f"{result['updated_articles']} updated from {result['sites_scraped']} sites"
            )

    background_tasks.add_task(_run_sync)
    return {"status": "started", "message": "Competitor blog sync started in background"}


@router.post("/articles/{article_id}/flag")
def flag_article(
    article_id: int,
    reason: Optional[str] = Query(None),
    notes: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Flag an article as interesting/inspirational"""
    result = service.flag_article(db, article_id, reason=reason, notes=notes)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return result


@router.delete("/articles/{article_id}/flag")
def unflag_article(article_id: int, db: Session = Depends(get_db)):
    """Remove flag from an article"""
    result = service.unflag_article(db, article_id)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return result


@router.post("/sites")
def add_site(
    name: str = Query(..., description="Display name"),
    domain: str = Query(..., description="Domain e.g. example.com.au"),
    site_type: str = Query("competitor", description="competitor, supplier, or industry"),
    blog_url: Optional[str] = Query(None, description="Blog listing page URL"),
    feed_url: Optional[str] = Query(None, description="RSS/Atom feed URL"),
    feed_type: Optional[str] = Query("scrape", description="rss, atom, or scrape"),
    db: Session = Depends(get_db),
):
    """Add a new competitor/supplier site to monitor"""
    result = service.add_site(db, {
        "name": name,
        "domain": domain,
        "site_type": site_type,
        "blog_url": blog_url,
        "feed_url": feed_url,
        "feed_type": feed_type,
    })
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.delete("/sites/{site_id}")
def remove_site(site_id: int, db: Session = Depends(get_db)):
    """Deactivate a competitor site (soft delete)"""
    if not service.remove_site(db, site_id):
        raise HTTPException(status_code=404, detail="Site not found")
    return {"status": "removed"}
