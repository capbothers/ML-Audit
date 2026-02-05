"""
Customer Journey Intelligence API

Endpoints for analyzing customer behavior patterns and LTV.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.services.journey_service import JourneyService
from app.services.llm_service import LLMService
from app.utils.logger import log

router = APIRouter(prefix="/journey", tags=["journey"])


@router.get("/dashboard")
async def get_journey_dashboard(
    db: Session = Depends(get_db)
):
    """
    Complete customer journey intelligence dashboard

    Returns:
    - LTV segments breakdown
    - Gateway products (create repeat customers)
    - Dead-end products (correlate with churn)
    - Journey patterns
    - Churn risk analysis
    """
    try:
        service = JourneyService(db)
        dashboard = await service.get_journey_dashboard()

        return {
            "success": True,
            "data": dashboard
        }

    except Exception as e:
        log.error(f"Error generating journey dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/segments")
async def get_ltv_segments(
    db: Session = Depends(get_db)
):
    """
    LTV segmentation analysis

    Returns breakdown of top 20%, middle 60%, bottom 20% customers
    Shows what separates high-LTV from low-LTV customers
    """
    try:
        service = JourneyService(db)
        segments = await service.calculate_ltv_segments()

        return {
            "success": True,
            "data": segments
        }

    except Exception as e:
        log.error(f"Error getting LTV segments: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gateway-products")
async def get_gateway_products(
    min_purchases: int = Query(50, description="Minimum first purchases for statistical significance"),
    db: Session = Depends(get_db)
):
    """
    Gateway products - create repeat customers

    Products that when purchased first lead to:
    - Higher repeat purchase rates
    - Higher lifetime value
    - Faster second purchases

    These should be promoted as entry points
    """
    try:
        service = JourneyService(db)
        gateway_products = await service.identify_gateway_products(min_first_purchases=min_purchases)

        return {
            "success": True,
            "data": {
                "gateway_products": gateway_products,
                "total_count": len(gateway_products),
                "summary": {
                    "avg_repeat_rate_lift": sum(
                        float(p['metrics']['repeat_rate_vs_average'].replace('x higher', ''))
                        for p in gateway_products
                    ) / len(gateway_products) if gateway_products else 0,
                    "total_estimated_ltv_gain": sum(
                        p['opportunity']['estimated_ltv_gain']
                        for p in gateway_products
                    )
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting gateway products: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dead-end-products")
async def get_dead_end_products(
    min_purchases: int = Query(50, description="Minimum first purchases for statistical significance"),
    db: Session = Depends(get_db)
):
    """
    Dead-end products - correlate with churn

    Products that when purchased first lead to:
    - Low/no repeat purchases
    - High one-time customer rate
    - Lower lifetime value

    These should NOT be promoted as entry points
    """
    try:
        service = JourneyService(db)
        dead_end_products = await service.identify_dead_end_products(min_first_purchases=min_purchases)

        return {
            "success": True,
            "data": {
                "dead_end_products": dead_end_products,
                "total_count": len(dead_end_products),
                "summary": {
                    "total_estimated_ltv_lost": sum(
                        p['problem_severity']['estimated_ltv_lost']
                        for p in dead_end_products
                    ),
                    "actively_promoted_count": sum(
                        1 for p in dead_end_products
                        if p['current_promotion']['is_actively_promoted']
                    )
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting dead-end products: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns")
async def get_journey_patterns(
    db: Session = Depends(get_db)
):
    """
    Common customer journey patterns

    Identifies patterns that lead to high vs low LTV:
    - First product category
    - First channel
    - Time to second purchase
    - Email subscription timing
    """
    try:
        service = JourneyService(db)
        patterns = await service.analyze_journey_patterns()

        return {
            "success": True,
            "data": {
                "patterns": patterns,
                "total_count": len(patterns),
                "summary": {
                    "desirable_patterns": sum(1 for p in patterns if p['is_desirable_pattern']),
                    "highest_ltv_pattern": patterns[0] if patterns else None
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting journey patterns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/churn-risk")
async def get_churn_risk_analysis(
    db: Session = Depends(get_db)
):
    """
    Churn risk timing analysis

    Shows:
    - When customers typically become at-risk
    - Optimal reactivation window
    - Current at-risk customers
    - Win-back effectiveness
    """
    try:
        service = JourneyService(db)
        churn_timing = await service.calculate_churn_risk_timing()

        if not churn_timing:
            raise HTTPException(status_code=404, detail="No churn timing data available")

        return {
            "success": True,
            "data": churn_timing
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting churn risk analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimal-timing")
async def get_optimal_timing(
    ltv_segment: Optional[str] = Query(None, description="Filter by LTV segment (top_20, middle_60, bottom_20)"),
    db: Session = Depends(get_db)
):
    """
    Optimal customer reactivation timing

    Answers:
    - How many days between purchases is normal?
    - When does a customer become at-risk?
    - Best time window to send win-back campaign
    """
    try:
        service = JourneyService(db)
        churn_timing = await service.calculate_churn_risk_timing()

        if not churn_timing:
            raise HTTPException(status_code=404, detail="No timing data available")

        # Filter by segment if requested
        if ltv_segment:
            segment_data = churn_timing.get('by_segment', {}).get(ltv_segment)
            if not segment_data:
                raise HTTPException(status_code=404, detail=f"Segment {ltv_segment} not found")

            return {
                "success": True,
                "data": {
                    "segment": ltv_segment,
                    **segment_data
                }
            }

        return {
            "success": True,
            "data": churn_timing
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting optimal timing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_journey_insights(
    db: Session = Depends(get_db)
):
    """
    AI-powered customer journey insights

    Uses Claude to analyze journey data and provide strategic recommendations:
    - What makes high-LTV customers different
    - Which products to promote/deprioritize
    - Churn prevention strategies
    - Specific action items with expected impact
    """
    try:
        # Get journey data
        journey_service = JourneyService(db)
        analysis = await journey_service.analyze_all_journeys()

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_customer_journeys(
            ltv_segments=analysis['ltv_segments'],
            gateway_products=analysis['gateway_products'],
            dead_end_products=analysis['dead_end_products'],
            journey_patterns=analysis['journey_patterns'],
            churn_timing=analysis['churn_risk_timing'],
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
        log.error(f"Error generating LLM journey insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/customer/{customer_id}")
async def get_customer_journey(
    customer_id: int,
    db: Session = Depends(get_db)
):
    """
    Detailed journey for a specific customer

    Returns:
    - LTV metrics
    - Journey characteristics
    - Churn risk
    - Product affinity
    """
    try:
        from app.models.journey import CustomerLTV

        customer = db.query(CustomerLTV).filter(
            CustomerLTV.customer_id == customer_id
        ).first()

        if not customer:
            raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

        return {
            "success": True,
            "data": {
                "customer_id": customer.customer_id,
                "email": customer.email,

                "ltv_metrics": {
                    "total_ltv": float(customer.total_ltv),
                    "historical_ltv": float(customer.historical_ltv),
                    "predicted_ltv": float(customer.predicted_ltv) if customer.predicted_ltv else None,
                    "ltv_segment": customer.ltv_segment,
                    "ltv_percentile": customer.ltv_percentile
                },

                "order_metrics": {
                    "total_orders": customer.total_orders,
                    "avg_order_value": float(customer.avg_order_value),
                    "total_revenue": float(customer.total_revenue)
                },

                "journey_timing": {
                    "first_order_date": customer.first_order_date.isoformat() if customer.first_order_date else None,
                    "last_order_date": customer.last_order_date.isoformat() if customer.last_order_date else None,
                    "days_as_customer": customer.days_as_customer,
                    "days_since_last_order": customer.days_since_last_order,
                    "days_to_second_order": customer.days_to_second_order,
                    "avg_days_between_orders": customer.avg_days_between_orders
                },

                "first_purchase": {
                    "product_title": customer.first_product_title,
                    "product_sku": customer.first_product_sku,
                    "channel": customer.first_order_channel,
                    "order_value": float(customer.first_order_value) if customer.first_order_value else None
                },

                "engagement": {
                    "is_repeat_customer": customer.is_repeat_customer,
                    "email_subscriber": customer.email_subscriber,
                    "subscribed_before_first_purchase": customer.subscribed_before_first_purchase
                },

                "churn_risk": {
                    "is_at_risk": customer.is_at_risk,
                    "churn_probability": customer.churn_probability,
                    "expected_next_purchase_date": customer.expected_next_purchase_date.isoformat() if customer.expected_next_purchase_date else None
                },

                "product_indicators": {
                    "bought_gateway_product": customer.bought_gateway_product,
                    "bought_dead_end_product": customer.bought_dead_end_product,
                    "favorite_category": customer.favorite_product_category
                }
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting customer journey: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
