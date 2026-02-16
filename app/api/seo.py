"""
SEO Intelligence API Endpoints

Exposes Search Console insights and SEO opportunities.
Answers: "Where are my easy SEO wins?"
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel

from app.services.seo_service import SEOService
from app.services.llm_service import LLMService
from app.services.blog_draft_service import BlogDraftService
from app.models.base import get_db
from app.utils.logger import log
from app.utils.cache import get_cached, set_cached, _MISS


class GenerateBlogDraftRequest(BaseModel):
    query: str
    opportunity_type: str = "manual"
    days: int = 30


class UpdateDraftStatusRequest(BaseModel):
    status: str
    reviewer_notes: Optional[str] = None

router = APIRouter(prefix="/seo", tags=["seo"])


@router.get("/dashboard")
async def get_seo_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Complete SEO intelligence dashboard

    Shows all SEO opportunities categorized and prioritized:
    - Quick wins (high impression, low CTR)
    - Close to page 1 (position 8-15)
    - Declining pages (losing traffic)
    - Technical issues

    This is your SEO action plan
    """
    cached = get_cached(f"seo_dashboard|{days}")
    if cached is not _MISS:
        return cached

    service = SEOService(db)

    try:
        dashboard = await service.get_seo_dashboard(days=days)
        set_cached(f"seo_dashboard|{days}", dashboard, 300)
        return dashboard

    except Exception as e:
        log.error(f"Error generating SEO dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_all_opportunities(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get all SEO opportunities

    Returns categorized opportunities with impact scores
    """
    cached = get_cached(f"seo_opportunities|{days}")
    if cached is not _MISS:
        return cached

    service = SEOService(db)

    try:
        opportunities = await service.identify_all_opportunities(days=days)
        set_cached(f"seo_opportunities|{days}", opportunities, 300)
        return opportunities

    except Exception as e:
        log.error(f"Error identifying opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/low-ctr")
async def get_low_ctr_opportunities(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(20, description="Maximum results to return"),
    db = Depends(get_db)
):
    """
    Get high impression, low CTR queries

    These are QUICK WINS:
    - Already ranking (on page 1)
    - Getting impressions
    - But not getting clicks

    Fix: Update title tag and meta description to be more compelling

    Expected timeline: 1-2 weeks to see improvement
    Effort: Low (just title/meta changes)
    """
    service = SEOService(db)

    try:
        from datetime import date
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        opportunities = await service.find_high_impression_low_ctr(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        total_potential_clicks = sum(o.get('potential_additional_clicks', 0) for o in opportunities)

        return {
            "period_days": days,
            "opportunities_found": len(opportunities),
            "total_potential_clicks": total_potential_clicks,
            "message": f"Found {len(opportunities)} quick win opportunities",
            "opportunities": opportunities
        }

    except Exception as e:
        log.error(f"Error finding low CTR opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/close-to-page-1")
async def get_close_to_page_one(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(20, description="Maximum results to return"),
    db = Depends(get_db)
):
    """
    Get queries close to page 1 (position 8-15)

    These are WORTH PUSHING:
    - Currently page 2
    - Close to page 1
    - Meaningful search volume

    Fix: Add comprehensive content, FAQ section, schema markup

    Expected timeline: 1-3 months
    Effort: Medium (content work required)
    """
    service = SEOService(db)

    try:
        from datetime import date
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        opportunities = await service.find_close_to_page_one(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        total_potential_clicks = sum(o.get('potential_additional_clicks', 0) for o in opportunities)

        return {
            "period_days": days,
            "opportunities_found": len(opportunities),
            "total_potential_clicks": total_potential_clicks,
            "message": f"Found {len(opportunities)} close-to-page-1 opportunities",
            "opportunities": opportunities
        }

    except Exception as e:
        log.error(f"Error finding close-to-page-1 opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/declining")
async def get_declining_pages(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(20, description="Maximum results to return"),
    db = Depends(get_db)
):
    """
    Get pages with declining organic traffic

    These need IMMEDIATE ATTENTION:
    - Traffic dropping
    - Position dropping
    - Revenue impact

    Fix: Content refresh, technical check, on-page SEO review

    Priority: High (losing traffic = losing revenue)
    """
    service = SEOService(db)

    try:
        from datetime import date
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        opportunities = await service.find_declining_pages(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        total_clicks_lost = sum(o.get('clicks_lost', 0) for o in opportunities)

        return {
            "period_days": days,
            "pages_found": len(opportunities),
            "total_clicks_lost": total_clicks_lost,
            "message": f"Found {len(opportunities)} declining pages - needs attention",
            "pages": opportunities
        }

    except Exception as e:
        log.error(f"Error finding declining pages: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/technical-issues")
async def get_technical_issues(
    db = Depends(get_db)
):
    """
    Get technical SEO issues

    Includes:
    - Indexing errors (pages not indexed)
    - Core Web Vitals failures
    - Duplicate title tags
    - Mobile usability issues

    These block rankings - fix first
    """
    service = SEOService(db)

    try:
        issues = await service.get_technical_issues()

        critical_count = sum(1 for i in issues if i.get('severity') == 'critical')

        return {
            "total_issues": len(issues),
            "critical_issues": critical_count,
            "message": f"Found {len(issues)} technical SEO issues ({critical_count} critical)",
            "issues": issues
        }

    except Exception as e:
        log.error(f"Error getting technical issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_seo_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    LLM-Powered SEO Analysis

    Claude analyzes all SEO opportunities and provides:
    - Which opportunities to prioritize
    - Expected impact of each
    - Specific action plan
    - Timeline estimates

    This is the "what should I do first?" answer
    """
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    seo_service = SEOService(db)

    try:
        # Get all opportunities
        opportunities = await seo_service.identify_all_opportunities(days=days)

        # Generate LLM analysis
        analysis = llm_service.analyze_seo_opportunities(
            quick_wins=opportunities['opportunities']['quick_wins'][:10],
            close_to_page_one=opportunities['opportunities']['close_to_page_one'][:10],
            declining=opportunities['opportunities']['declining_pages'][:10],
            technical=opportunities['opportunities']['technical_issues'],
            summary=opportunities['summary']
        )

        return {
            "period_days": days,
            "opportunities_analyzed": opportunities['summary']['total_opportunities'],

            "llm_analysis": analysis,

            "opportunity_counts": {
                "quick_wins": len(opportunities['opportunities']['quick_wins']),
                "close_to_page_one": len(opportunities['opportunities']['close_to_page_one']),
                "declining_pages": len(opportunities['opportunities']['declining_pages']),
                "technical_issues": len(opportunities['opportunities']['technical_issues'])
            }
        }

    except Exception as e:
        log.error(f"Error generating LLM SEO insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/underperformers")
async def get_underperformers(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(50, description="Maximum results to return"),
    db = Depends(get_db)
):
    """
    Top underperforming queries ranked by ML priority score.

    Each row includes: click gap, priority score, SERP risk badge,
    content decay badge, fix-first recommendation, and sparkline data.
    """
    cached = get_cached(f"seo_underperformers|{days}|{limit}")
    if cached is not _MISS:
        return cached

    service = SEOService(db)
    try:
        results = service.get_underperformers(days=days, limit=limit)
        total_opp = sum(r.get("revenue_opportunity", 0) for r in results)
        serp_risks = sum(1 for r in results if r.get("serp_risk"))
        content_decays = sum(1 for r in results if r.get("content_decay"))
        result = {
            "period_days": days,
            "count": len(results),
            "total_revenue_opportunity": round(total_opp, 2),
            "serp_risks": serp_risks,
            "content_decays": content_decays,
            "items": results,
        }
        set_cached(f"seo_underperformers|{days}|{limit}", result, 300)
        return result
    except Exception as e:
        log.error(f"Error getting underperformers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query-detail")
async def get_query_detail(
    query: str = Query(..., description="Search query to analyze"),
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Full query drill-down: current vs previous period, pages ranking for it,
    monthly history, and ML flags.
    """
    service = SEOService(db)
    try:
        result = service.get_query_drill_down(query=query, days=days)
        return result
    except Exception as e:
        log.error(f"Error getting query detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/page-detail")
async def get_page_detail(
    url: str = Query(..., description="Full URL to analyze"),
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Full page drill-down: current vs previous period, top queries,
    monthly history, and ML flags.
    """
    service = SEOService(db)
    try:
        result = service.get_page_drill_down(url=url, days=days)
        return result
    except Exception as e:
        log.error(f"Error getting page detail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/query/{query}")
async def analyze_specific_query(
    query: str,
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Deep-dive analysis for a specific search query

    Shows:
    - Current performance (impressions, clicks, CTR, position)
    - Historical trends
    - Opportunity assessment
    - Specific recommendations
    """
    from app.models.seo import SearchQuery
    from datetime import date

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    # Get query data
    query_data = db.query(SearchQuery).filter(
        SearchQuery.query == query,
        SearchQuery.date >= start_date,
        SearchQuery.date <= end_date
    ).order_by(SearchQuery.date.desc()).all()

    if not query_data:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for query: {query}"
        )

    # Get most recent data
    latest = query_data[0]

    # Calculate averages
    avg_position = sum(q.position for q in query_data) / len(query_data)
    avg_ctr = sum(q.ctr for q in query_data) / len(query_data)
    total_impressions = sum(q.impressions for q in query_data)
    total_clicks = sum(q.clicks for q in query_data)

    return {
        "query": query,
        "period_days": days,
        "data_points": len(query_data),

        "current": {
            "position": round(latest.position, 1),
            "ctr": round(latest.ctr * 100, 1),
            "impressions": latest.impressions,
            "clicks": latest.clicks
        },

        "averages": {
            "position": round(avg_position, 1),
            "ctr": round(avg_ctr * 100, 1),
            "total_impressions": total_impressions,
            "total_clicks": total_clicks
        },

        "opportunity_flags": {
            "high_impression_low_ctr": latest.is_high_impression_low_ctr,
            "close_to_page_one": latest.is_close_to_page_one,
            "declining": latest.is_declining
        },

        "historical_data": [
            {
                "date": q.date.isoformat(),
                "position": round(q.position, 1),
                "ctr": round(q.ctr * 100, 1),
                "impressions": q.impressions,
                "clicks": q.clicks
            }
            for q in query_data[-30:]  # Last 30 data points
        ]
    }


@router.get("/page-analysis")
async def analyze_page_performance(
    url: str = Query(..., description="Full URL to analyze"),
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Analyze performance of a specific page

    Shows:
    - Traffic trends
    - Top queries driving traffic
    - Technical issues
    - Optimization opportunities
    """
    from app.models.seo import PageSEO
    from datetime import date

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    # Get page data
    page_data = db.query(PageSEO).filter(
        PageSEO.url == url,
        PageSEO.date >= start_date,
        PageSEO.date <= end_date
    ).order_by(PageSEO.date.desc()).all()

    if not page_data:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for URL: {url}"
        )

    latest = page_data[0]

    return {
        "url": url,
        "page_type": latest.page_type,
        "period_days": days,

        "current_performance": {
            "clicks": latest.clicks,
            "impressions": latest.impressions,
            "ctr": round(latest.ctr * 100, 1),
            "position": round(latest.position, 1)
        },

        "trend": {
            "clicks_change_pct": round(latest.clicks_change_pct or 0, 1),
            "position_change": round(latest.position_change or 0, 1),
            "is_declining": latest.is_declining
        },

        "top_queries": latest.top_queries,

        "technical": {
            "is_indexed": latest.is_indexed,
            "indexing_issues": latest.indexing_issues,
            "mobile_usable": latest.mobile_usable,
            "core_web_vitals_pass": latest.core_web_vitals_pass
        },

        "content": {
            "title_tag": latest.title_tag,
            "meta_description": latest.meta_description,
            "h1_tag": latest.h1_tag,
            "word_count": latest.word_count
        },

        "opportunities": {
            "has_technical_issues": latest.has_technical_issues,
            "has_content_gaps": latest.has_content_gaps,
            "opportunity_score": latest.opportunity_score
        }
    }


# ── Blog Draft Endpoints ──────────────────────────────────────────


@router.get("/blog-drafts/suggest")
async def suggest_blog_topics(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(5, description="Number of topics to suggest"),
    db=Depends(get_db),
):
    """
    Auto-suggest the best blog post topics based on SEO underperformers.

    Analyzes declining pages, close-to-page-1 queries, and content decay
    to recommend which queries would benefit most from blog content.
    """
    service = BlogDraftService(db)

    if not service.llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env",
        )

    try:
        suggestions = await service.suggest_topics(days=days, limit=limit)

        return {
            "success": True,
            "data": {"suggestions": suggestions, "count": len(suggestions)},
        }

    except Exception as e:
        log.error(f"Error suggesting blog topics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/blog-drafts/generate")
async def generate_blog_draft(
    request: GenerateBlogDraftRequest,
    db=Depends(get_db),
):
    """
    Generate a full LLM blog post draft for a specific underperforming query.

    Takes a search query from the SEO dashboard and generates a complete
    SEO-optimized blog post draft with title, meta, HTML content, and internal links.
    """
    service = BlogDraftService(db)

    if not service.llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env",
        )

    try:
        draft = await service.generate_draft_for_query(
            query=request.query,
            opportunity_type=request.opportunity_type,
            days=request.days,
        )

        if not draft:
            raise HTTPException(
                status_code=500, detail="Failed to generate blog draft"
            )

        return {"success": True, "data": draft}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error generating blog draft: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


class GenerateFromIdeaRequest(BaseModel):
    """Request body for generating a blog post from a chosen competitor idea."""
    idea_title: str
    idea_angle: str
    idea_keywords: List[str] = []


@router.get("/blog-drafts/competitor-ideas/{article_id}")
def get_competitor_ideas(
    article_id: int,
    num_ideas: int = Query(4, ge=2, le=6),
    db=Depends(get_db),
):
    """
    Analyse a competitor article and suggest original content ideas.

    Step 1: Returns a list of blog post ideas inspired by the competitor article,
    each with a unique angle. The user picks one, then calls the generate endpoint.
    """
    service = BlogDraftService(db)

    if not service.llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env",
        )

    try:
        ideas = service.get_ideas_from_competitor(
            article_id=article_id, num_ideas=num_ideas
        )

        if ideas is None:
            raise HTTPException(
                status_code=500, detail="Failed to generate ideas"
            )

        return {"success": True, "data": {"ideas": ideas, "article_id": article_id}}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting competitor ideas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/blog-drafts/competitor-ideas/{article_id}/generate")
async def generate_from_competitor_idea(
    article_id: int,
    request: GenerateFromIdeaRequest,
    db=Depends(get_db),
):
    """
    Generate a full blog post from a chosen competitor-inspired idea.

    Step 2: Takes the chosen idea (title, angle, keywords) and generates a
    complete blog draft saved with opportunity_type='competitor_spin'.
    """
    service = BlogDraftService(db)

    if not service.llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env",
        )

    try:
        draft = await service.generate_from_competitor_idea(
            article_id=article_id,
            idea_title=request.idea_title,
            idea_angle=request.idea_angle,
            idea_keywords=request.idea_keywords,
        )

        if not draft:
            raise HTTPException(
                status_code=500, detail="Failed to generate blog draft from idea"
            )

        return {"success": True, "data": draft}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error generating from competitor idea: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/blog-drafts")
async def list_blog_drafts(
    status: Optional[str] = Query(
        None,
        description="Filter by status: draft, reviewed, approved, published, rejected",
    ),
    limit: int = Query(20, description="Max results"),
    offset: int = Query(0, description="Offset for pagination"),
    db=Depends(get_db),
):
    """List all blog drafts with optional status filter."""
    try:
        service = BlogDraftService(db)
        result = service.list_drafts(
            status=status, limit=limit, offset=offset
        )
        return {"success": True, "data": result}

    except Exception as e:
        log.error(f"Error listing blog drafts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/blog-drafts/{draft_id}")
async def get_blog_draft(draft_id: int, db=Depends(get_db)):
    """Get a specific blog draft by ID with full content."""
    try:
        service = BlogDraftService(db)
        draft = service.get_draft(draft_id)

        if not draft:
            raise HTTPException(
                status_code=404, detail="Blog draft not found"
            )

        return {"success": True, "data": draft}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting blog draft: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/blog-drafts/{draft_id}/status")
async def update_blog_draft_status(
    draft_id: int,
    request: UpdateDraftStatusRequest,
    db=Depends(get_db),
):
    """Update draft status (reviewed, approved, published, rejected)."""
    valid_statuses = {"draft", "reviewed", "approved", "published", "rejected"}
    if request.status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {', '.join(sorted(valid_statuses))}",
        )

    try:
        service = BlogDraftService(db)
        draft = service.update_draft_status(
            draft_id=draft_id,
            status=request.status,
            reviewer_notes=request.reviewer_notes,
        )

        if not draft:
            raise HTTPException(
                status_code=404, detail="Blog draft not found"
            )

        return {"success": True, "data": draft}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error updating blog draft status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
