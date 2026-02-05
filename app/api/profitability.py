"""
Product Profitability API Endpoints

Answers the critical question: "Which products are actually making me money?"
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel

from app.services.profitability_service import ProfitabilityService
from app.services.llm_service import LLMService
from app.models.base import get_db
from app.api.data_quality import get_stale_data_warning
from app.utils.logger import log

router = APIRouter(prefix="/profitability", tags=["profitability"])


class ProfitabilityAnalysisRequest(BaseModel):
    """Request for profitability analysis"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    period_type: str = "monthly"  # daily, weekly, monthly


@router.post("/analyze")
async def analyze_profitability(
    request: ProfitabilityAnalysisRequest,
    db = Depends(get_db)
):
    """
    Calculate true profitability for all products

    Returns which products make money and which lose money
    after COGS, ad spend, and returns
    """
    # Default to last 30 days if not specified
    end_date = request.end_date or datetime.utcnow()
    start_date = request.start_date or (end_date - timedelta(days=30))

    service = ProfitabilityService(db)

    try:
        results = await service.calculate_product_profitability(
            start_date=start_date,
            end_date=end_date,
            period_type=request.period_type
        )

        return {
            "status": "success",
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "type": request.period_type
            },
            "products_analyzed": len(results),
            "results": results
        }

    except Exception as e:
        log.error(f"Error analyzing profitability: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_profitability_summary(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Get overall profitability summary

    Shows:
    - Total profit vs revenue
    - How many products are profitable vs losing money
    - Blended ROAS
    - Top performers and biggest losers
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        summary = await service.get_profitability_summary(start_date, end_date)
        return summary

    except Exception as e:
        log.error(f"Error getting profitability summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profitable")
async def get_profitable_products(
    days: int = Query(30, description="Number of days to analyze"),
    min_profit: float = Query(0, description="Minimum profit threshold"),
    limit: int = Query(50, description="Maximum number of products to return"),
    db = Depends(get_db)
):
    """
    Get most profitable products

    These are your winners - push them harder
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        products = await service.get_profitable_products(
            start_date=start_date,
            end_date=end_date,
            min_profit=min_profit,
            limit=limit
        )

        return {
            "period_days": days,
            "min_profit_threshold": min_profit,
            "products_found": len(products),
            "products": products
        }

    except Exception as e:
        log.error(f"Error getting profitable products: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/losing")
async def get_losing_products(
    days: int = Query(30, description="Number of days to analyze"),
    limit: int = Query(50, description="Maximum number of products to return"),
    db = Depends(get_db)
):
    """
    Get products that are LOSING money

    CRITICAL: These might have high revenue but negative profit
    after ad spend and returns.

    Your "best sellers" might be killing your profit.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        products = await service.get_losing_products(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        if products:
            total_loss = sum(p['net_profit'] for p in products)
            total_revenue = sum(p['revenue'] for p in products)

            return {
                "period_days": days,
                "products_found": len(products),
                "total_revenue_from_losers": round(total_revenue, 2),
                "total_loss": round(total_loss, 2),
                "message": f"These {len(products)} products generated ${total_revenue:,.0f} in revenue but lost ${abs(total_loss):,.0f}",
                "products": products
            }
        else:
            return {
                "period_days": days,
                "products_found": 0,
                "message": "No losing products found - all products are profitable!"
            }

    except Exception as e:
        log.error(f"Error getting losing products: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hidden-gems")
async def get_hidden_gems(
    days: int = Query(30, description="Number of days to analyze"),
    min_roas: float = Query(4.0, description="Minimum ROAS threshold"),
    max_revenue: float = Query(5000, description="Maximum revenue (to find 'hidden' gems)"),
    db = Depends(get_db)
):
    """
    Find "hidden gems" - low revenue but highly profitable products

    These are products you should push harder.
    High ROAS but low visibility/budget.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        products = await service.get_hidden_gems(
            start_date=start_date,
            end_date=end_date,
            min_roas=min_roas,
            max_revenue=max_revenue
        )

        return {
            "period_days": days,
            "min_roas": min_roas,
            "max_revenue": max_revenue,
            "products_found": len(products),
            "message": f"Found {len(products)} hidden gems with ROAS >= {min_roas}x and revenue <= ${max_revenue:,.0f}",
            "recommendation": "Consider increasing ad spend on these products - they're proven performers",
            "products": products
        }

    except Exception as e:
        log.error(f"Error finding hidden gems: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/{product_id}")
async def get_product_trends(
    product_id: int,
    lookback_days: int = Query(90, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Analyze profitability trends for a specific product over time

    Shows if profitability is improving or declining
    """
    service = ProfitabilityService(db)

    try:
        trends = await service.analyze_profitability_trends(
            product_id=product_id,
            lookback_days=lookback_days
        )

        return trends

    except Exception as e:
        log.error(f"Error getting product trends: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
async def get_profitability_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Complete profitability dashboard

    Everything you need to know about product profitability:
    - Overall summary
    - Top performers
    - Biggest losers
    - Hidden gems
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        # Get all the data in parallel
        summary = await service.get_profitability_summary(start_date, end_date)
        top_performers = await service.get_profitable_products(start_date, end_date, min_profit=0, limit=10)
        losers = await service.get_losing_products(start_date, end_date, limit=10)
        gems = await service.get_hidden_gems(start_date, end_date, min_roas=4.0, max_revenue=5000)

        response = {
            "period_days": days,
            "generated_at": datetime.utcnow().isoformat(),

            "summary": summary,

            "top_performers": {
                "count": len(top_performers),
                "products": top_performers
            },

            "losing_products": {
                "count": len(losers),
                "total_loss": sum(p['net_profit'] for p in losers) if losers else 0,
                "products": losers
            },

            "hidden_gems": {
                "count": len(gems),
                "products": gems
            },

            "recommendations": {
                "push_harder": [p['title'] for p in gems[:5]] if gems else [],
                "reduce_or_fix": [p['title'] for p in losers[:5]] if losers else [],
                "double_down": [p['title'] for p in top_performers[:5]] if top_performers else []
            }
        }

        # Inject stale-data warning if any source is behind
        stale_warning = get_stale_data_warning(db)
        if stale_warning:
            response['data_warning'] = stale_warning

        return response

    except Exception as e:
        log.error(f"Error generating profitability dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare-products")
async def compare_products(
    product_ids: str = Query(..., description="Comma-separated product IDs"),
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Compare profitability of multiple products side-by-side

    Useful for deciding which products to push
    """
    try:
        ids = [int(pid.strip()) for pid in product_ids.split(',')]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid product IDs format")

    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    comparisons = []
    for product_id in ids:
        try:
            trends = await service.analyze_profitability_trends(product_id, days)
            comparisons.append(trends)
        except Exception as e:
            log.warning(f"Error analyzing product {product_id}: {str(e)}")

    return {
        "period_days": days,
        "products_compared": len(comparisons),
        "comparisons": comparisons
    }


@router.get("/insights")
async def get_profitability_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    LLM-Powered Profitability Insights

    Get AI-powered analysis answering:
    - Which products should I push harder?
    - Which products are killing profitability?
    - Where should I reallocate budget?

    This is the "so what?" - what these numbers actually mean for your business
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    try:
        # Get profitability data
        summary = await service.get_profitability_summary(start_date, end_date)
        top_performers = await service.get_profitable_products(start_date, end_date, min_profit=0, limit=10)
        losers = await service.get_losing_products(start_date, end_date, limit=10)
        gems = await service.get_hidden_gems(start_date, end_date, min_roas=4.0, max_revenue=5000)

        # Generate LLM analysis
        analysis = llm_service.analyze_product_profitability(
            profitable_products=top_performers,
            losing_products=losers,
            hidden_gems=gems,
            summary=summary
        )

        return {
            "period_days": days,
            "generated_at": datetime.utcnow().isoformat(),

            "data_summary": {
                "total_products": summary.get('total_products', 0),
                "profitable_count": summary.get('profitable_products', 0),
                "losing_count": summary.get('losing_products', 0),
                "total_profit": summary.get('total_profit', 0)
            },

            "llm_analysis": analysis,

            "top_performers": [p['title'] for p in top_performers[:5]],
            "losing_products": [p['title'] for p in losers[:5]] if losers else [],
            "hidden_gems": [p['title'] for p in gems[:5]] if gems else []
        }

    except Exception as e:
        log.error(f"Error generating profitability insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/product/{product_id}")
async def get_product_insights(
    product_id: int,
    days: int = Query(30, description="Number of days to analyze"),
    db = Depends(get_db)
):
    """
    Deep-dive LLM analysis for a specific product

    Critical for understanding why a product is losing money
    and what to do about it
    """
    llm_service = LLMService()

    if not llm_service.is_available():
        raise HTTPException(
            status_code=503,
            detail="LLM service not available. Configure ANTHROPIC_API_KEY in .env"
        )

    # Get product profitability data
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)

    service = ProfitabilityService(db)

    try:
        trends = await service.analyze_profitability_trends(product_id, days)

        if 'error' in trends:
            raise HTTPException(status_code=404, detail=trends['error'])

        # Get latest snapshot for detailed analysis
        from app.models.product import ProductProfitability
        latest_snapshot = db.query(ProductProfitability).filter(
            ProductProfitability.product_id == product_id
        ).order_by(ProductProfitability.period_start.desc()).first()

        if not latest_snapshot:
            raise HTTPException(
                status_code=404,
                detail="No profitability data found for this product"
            )

        product_data = {
            'title': trends.get('title'),
            'revenue': latest_snapshot.total_revenue,
            'cogs': latest_snapshot.total_cogs,
            'gross_margin': latest_snapshot.gross_margin_dollars,
            'gross_margin_pct': latest_snapshot.gross_margin_pct,
            'ad_spend': latest_snapshot.attributed_ad_spend,
            'ad_spend_by_channel': latest_snapshot.ad_spend_by_channel,
            'refunds': latest_snapshot.total_refunded,
            'return_rate': latest_snapshot.return_rate_pct,
            'net_profit': latest_snapshot.net_profit_dollars,
            'roas': latest_snapshot.roas,
            'units_sold': latest_snapshot.units_sold
        }

        # Generate LLM explanation
        explanation = llm_service.explain_losing_product(product_data)

        return {
            "product_id": product_id,
            "title": trends.get('title'),
            "period_days": days,

            "financial_summary": product_data,
            "trends": trends,
            "llm_explanation": explanation
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error generating product insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
