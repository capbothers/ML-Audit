"""
Content & Merchandising Gap Analysis API

Endpoints for identifying content gaps and optimization opportunities.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.content_gap_service import ContentGapService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/content", tags=["content"])


@router.get("/dashboard")
async def get_content_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Complete content & merchandising dashboard

    Returns:
    - Overview (total gaps, opportunities, health score)
    - Top priorities
    - Content gaps summary
    - Merchandising gaps summary
    - Content opportunities
    - Underperforming content
    - Category health scores
    """
    try:
        service = ContentGapService(db)
        dashboard = await service.get_content_dashboard(days)

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating content dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gaps")
async def get_content_gaps(
    gap_type: Optional[str] = Query(None, description="Filter by gap type"),
    gap_severity: Optional[str] = Query(None, description="Filter by severity: critical, high, medium, low"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    db: Session = Depends(get_db)
):
    """
    All content gaps

    Types:
    - short_description: Description too short
    - missing_images: Not enough images
    - missing_video: No product video
    - missing_size_guide: No size guide
    - missing_specs: No specifications
    - poor_seo: Missing meta descriptions, alt text
    """
    try:
        service = ContentGapService(db)
        gaps = await service.detect_content_gaps()

        # Apply filters
        if gap_type:
            gaps = [g for g in gaps if g['gap_type'] == gap_type]

        if gap_severity:
            gaps = [g for g in gaps if g['gap_severity'] == gap_severity]

        if category:
            gaps = [g for g in gaps if g.get('category') == category]

        return {
            "success": True,
            "data": {
                "gaps": gaps,
                "total_count": len(gaps),
                "total_impact": sum(g['impact']['estimated_revenue_impact'] for g in gaps)
            }
        }

    except Exception as e:
        log.error(f"Error getting content gaps: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/merchandising-gaps")
async def get_merchandising_gaps(
    gap_type: Optional[str] = Query(None, description="Filter by gap type"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    min_impact: float = Query(0, description="Minimum monthly impact ($)"),
    db: Session = Depends(get_db)
):
    """
    Merchandising gaps

    Types:
    - missing_cross_sells: No cross-sell recommendations
    - missing_upsells: No upsell opportunities
    - poor_categorization: Wrong category
    - no_bundle_opportunities: Should be bundled
    - missing_related_products: No related products
    """
    try:
        service = ContentGapService(db)
        gaps = await service.detect_merchandising_gaps()

        # Apply filters
        if gap_type:
            gaps = [g for g in gaps if g['gap_type'] == gap_type]

        if category:
            gaps = [g for g in gaps if g.get('category') == category]

        if min_impact:
            gaps = [g for g in gaps if g['impact']['estimated_impact'] >= min_impact]

        return {
            "success": True,
            "data": {
                "gaps": gaps,
                "total_count": len(gaps),
                "total_impact": sum(g['impact']['estimated_impact'] for g in gaps)
            }
        }

    except Exception as e:
        log.error(f"Error getting merchandising gaps: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_content_opportunities(
    opportunity_type: Optional[str] = Query(None, description="Filter by type"),
    min_traffic: int = Query(0, description="Minimum estimated monthly traffic"),
    db: Session = Depends(get_db)
):
    """
    High-impact content to create

    Types:
    - buying_guide: Product buying guides
    - how_to_video: Installation/usage videos
    - comparison_chart: Product comparisons
    - size_guide: Size/dimension guides
    - installation_guide: Installation instructions
    - blog_post: SEO blog content
    - landing_page: Category landing pages
    """
    try:
        service = ContentGapService(db)
        opportunities = await service.find_content_opportunities()

        # Apply filters
        if opportunity_type:
            opportunities = [o for o in opportunities if o['opportunity_type'] == opportunity_type]

        if min_traffic:
            opportunities = [
                o for o in opportunities
                if o['opportunity_metrics']['estimated_monthly_traffic'] >= min_traffic
            ]

        return {
            "success": True,
            "data": {
                "opportunities": opportunities,
                "total_count": len(opportunities),
                "total_estimated_revenue": sum(
                    o['opportunity_metrics']['estimated_monthly_revenue']
                    for o in opportunities
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting content opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/underperforming")
async def get_underperforming_content(
    content_type: Optional[str] = Query(None, description="Filter by content type"),
    min_traffic: int = Query(500, description="Minimum monthly sessions"),
    db: Session = Depends(get_db)
):
    """
    Content with high traffic but low conversion

    Shows pages that get traffic but don't convert well.
    These are optimization opportunities - fix the content, keep the traffic.
    """
    try:
        service = ContentGapService(db)
        underperforming = await service.find_underperforming_content()

        # Apply filters
        if content_type:
            underperforming = [u for u in underperforming if u['content_type'] == content_type]

        if min_traffic:
            underperforming = [
                u for u in underperforming
                if u['performance']['monthly_sessions'] >= min_traffic
            ]

        return {
            "success": True,
            "data": {
                "pages": underperforming,
                "total_count": len(underperforming),
                "total_revenue_opportunity": sum(
                    u['optimization_potential']['estimated_revenue_gain']
                    for u in underperforming
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting underperforming content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/category-health")
async def get_category_health(
    min_health_score: Optional[int] = Query(None, description="Filter by minimum health score"),
    db: Session = Depends(get_db)
):
    """
    Content health score by category

    Shows health scores (0-100) for:
    - Description completeness
    - Image quality
    - SEO optimization
    - Merchandising completeness
    """
    try:
        service = ContentGapService(db)
        categories = await service.analyze_category_health()

        # Apply filters
        if min_health_score is not None:
            categories = [
                c for c in categories
                if c['health_scores']['overall_health_score'] >= min_health_score
            ]

        return {
            "success": True,
            "data": {
                "categories": categories,
                "total_count": len(categories),
                "avg_health_score": sum(
                    c['health_scores']['overall_health_score'] for c in categories
                ) / len(categories) if categories else 0
            }
        }

    except Exception as e:
        log.error(f"Error getting category health: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/missing-content-types")
async def get_missing_content_types(
    db: Session = Depends(get_db)
):
    """
    What types of content are missing across the site

    Returns count of products missing:
    - Videos
    - Size guides
    - Specifications
    - Installation guides
    - Reviews
    """
    try:
        service = ContentGapService(db)
        gaps = await service.detect_content_gaps()

        # Aggregate by gap type
        gap_summary = {}
        for gap in gaps:
            gap_type = gap['gap_type']
            if gap_type not in gap_summary:
                gap_summary[gap_type] = {
                    "count": 0,
                    "total_impact": 0,
                    "affected_products": []
                }

            gap_summary[gap_type]["count"] += 1
            gap_summary[gap_type]["total_impact"] += gap['impact']['estimated_revenue_impact']
            gap_summary[gap_type]["affected_products"].append(gap['product_handle'])

        # Convert to list and sort by impact
        missing_types = [
            {
                "content_type": gap_type,
                "missing_count": data["count"],
                "total_impact": data["total_impact"],
                "example_products": data["affected_products"][:5]
            }
            for gap_type, data in gap_summary.items()
        ]

        missing_types.sort(key=lambda x: x['total_impact'], reverse=True)

        return {
            "success": True,
            "data": {
                "missing_content_types": missing_types,
                "total_types": len(missing_types)
            }
        }

    except Exception as e:
        log.error(f"Error getting missing content types: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quick-wins")
async def get_quick_wins(
    max_effort_hours: float = Query(2.0, description="Maximum effort hours"),
    db: Session = Depends(get_db)
):
    """
    Low-effort, high-impact content improvements

    Returns simple fixes that can be done quickly:
    - Add cross-sells (30 min)
    - Fix categorization (15 min)
    - Expand short descriptions (1-2 hours)
    """
    try:
        service = ContentGapService(db)
        analysis = await service.analyze_all_content()

        quick_wins = analysis['quick_wins']

        # Filter by max effort (if specified)
        # Note: effort is a string like "0.5 hours", need to parse
        if max_effort_hours:
            filtered_wins = []
            for win in quick_wins:
                effort_str = win['effort']
                try:
                    hours = float(effort_str.split()[0])
                    if hours <= max_effort_hours:
                        filtered_wins.append(win)
                except:
                    filtered_wins.append(win)  # Include if can't parse
            quick_wins = filtered_wins

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


@router.get("/llm-insights")
async def get_llm_content_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    AI-powered content & merchandising insights

    Uses Claude to analyze content gaps and provide strategic recommendations:
    - Top content priorities
    - Merchandising improvements
    - Content opportunities to pursue
    - Category-specific recommendations
    - Expected revenue impact
    """
    try:
        # Get content analysis
        content_service = ContentGapService(db)
        analysis = await content_service.analyze_all_content(days)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_content_gaps(
            content_gaps=analysis['content_gaps'],
            merchandising_gaps=analysis['merchandising_gaps'],
            content_opportunities=analysis['content_opportunities'],
            underperforming_content=analysis['underperforming_content'],
            category_health=analysis['category_health'],
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
        log.error(f"Error generating LLM content insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
