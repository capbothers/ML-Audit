"""
404 & Redirect Intelligence API

Endpoints for tracking broken links and redirect health.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.redirect_health_service import RedirectHealthService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/redirects", tags=["redirects"])


@router.get("/dashboard")
async def get_redirect_dashboard(
    db: Session = Depends(get_db)
):
    """
    Complete 404 & redirect dashboard

    Returns:
    - Overview (total 404s, revenue loss, redirect issues)
    - Top priorities
    - 404 errors summary
    - Revenue impact
    - Redirect issues
    - Broken links
    """
    try:
        service = RedirectHealthService(db)
        dashboard = await service.get_404_dashboard()

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating redirect dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/404-errors")
async def get_404_errors(
    days: int = Query(30, description="Number of days to analyze"),
    min_traffic: int = Query(0, description="Minimum monthly sessions"),
    url_type: Optional[str] = Query(None, description="Filter by URL type: product_page, collection_page, blog_post"),
    db: Session = Depends(get_db)
):
    """
    All 404 errors

    Shows:
    - Traffic to 404 pages
    - Referrers
    - Revenue impact
    - Recommended fixes
    """
    try:
        service = RedirectHealthService(db)
        errors = await service.get_404_errors(days)

        # Apply filters
        if min_traffic:
            errors = [e for e in errors if e['traffic']['estimated_monthly_sessions'] >= min_traffic]

        if url_type:
            errors = [e for e in errors if e.get('url_type') == url_type]

        return {
            "success": True,
            "data": {
                "errors": errors,
                "total_count": len(errors)
            }
        }

    except Exception as e:
        log.error(f"Error getting 404 errors: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/revenue-impact")
async def get_revenue_impact(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Revenue impact from 404 errors

    Shows:
    - Total lost revenue
    - High-impact 404s
    - Top revenue losses
    """
    try:
        service = RedirectHealthService(db)
        errors = await service.get_404_errors(days)
        revenue_impact = await service.calculate_revenue_impact(errors)

        return {
            "success": True,
            "data": revenue_impact
        }

    except Exception as e:
        log.error(f"Error getting revenue impact: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/redirect-issues")
async def get_redirect_issues(
    db: Session = Depends(get_db)
):
    """
    Redirect health issues

    Shows:
    - Broken redirects (point to 404)
    - Redirect chains
    - Temporary redirects (302 instead of 301)
    - Slow redirects
    """
    try:
        service = RedirectHealthService(db)
        issues = await service.analyze_redirects()

        return {
            "success": True,
            "data": issues
        }

    except Exception as e:
        log.error(f"Error getting redirect issues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/redirect-chains")
async def get_redirect_chains(
    min_chain_length: int = Query(2, description="Minimum chain length"),
    db: Session = Depends(get_db)
):
    """
    Multi-hop redirect chains

    Shows redirect chains that:
    - Have multiple hops (bad for SEO)
    - Add latency
    - May end in 404
    """
    try:
        service = RedirectHealthService(db)
        chains = await service.detect_redirect_chains()

        # Filter by chain length
        chains = [c for c in chains if c['chain_length'] >= min_chain_length]

        return {
            "success": True,
            "data": {
                "chains": chains,
                "total_count": len(chains)
            }
        }

    except Exception as e:
        log.error(f"Error getting redirect chains: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/broken-links")
async def get_broken_links(
    priority: Optional[str] = Query(None, description="Filter by priority: high, medium, low"),
    db: Session = Depends(get_db)
):
    """
    Broken internal links

    Shows links from your site to 404 pages.
    These are easier to fix than external links.
    """
    try:
        service = RedirectHealthService(db)
        broken_links = await service.find_broken_internal_links()

        # Filter by priority
        if priority:
            broken_links = [l for l in broken_links if l['priority'] == priority]

        return {
            "success": True,
            "data": {
                "broken_links": broken_links,
                "total_count": len(broken_links)
            }
        }

    except Exception as e:
        log.error(f"Error getting broken links: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations")
async def get_recommendations(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Prioritized fix recommendations

    Shows:
    - Create redirects for high-revenue 404s
    - Fix broken redirects
    - Fix redirect chains
    - Fix broken internal links
    """
    try:
        service = RedirectHealthService(db)
        analysis = await service.analyze_404_health(days)

        return {
            "success": True,
            "data": {
                "recommendations": analysis['recommendations'],
                "total_count": len(analysis['recommendations'])
            }
        }

    except Exception as e:
        log.error(f"Error getting recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_redirect_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    AI-powered 404 & redirect insights

    Uses Claude to analyze 404s and provide strategic recommendations:
    - Which 404s to fix first (by revenue impact)
    - Recommended redirect mappings
    - Internal link fixes
    - SEO impact analysis
    """
    try:
        # Get redirect analysis
        redirect_service = RedirectHealthService(db)
        analysis = await redirect_service.analyze_404_health(days)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_404_health(
            not_found_errors=analysis['not_found_errors'],
            revenue_impact=analysis['revenue_impact'],
            redirect_issues=analysis['redirect_issues'],
            redirect_chains=analysis['redirect_chains'],
            broken_links=analysis['broken_links'],
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
        log.error(f"Error generating LLM redirect insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
