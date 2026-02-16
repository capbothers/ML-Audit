"""
Ad Spend Optimization Intelligence API

Endpoints for analyzing ad spend efficiency and waste.
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.models.base import get_db
from app.models.google_ads_data import GoogleAdsCampaign
from app.services.ad_spend_service import AdSpendService
from app.services.ad_spend_processor import AdSpendProcessor
from app.services.llm_service import LLMService
from app.api.data_quality import get_stale_data_warning
from app.utils.logger import log

from sqlalchemy import func
from datetime import date, timedelta

router = APIRouter(prefix="/ads", tags=["ads"])


def _get_ads_period_end(db: Session) -> date:
    """Anchor ad analysis windows to latest ingested Google Ads date."""
    latest = db.query(func.max(GoogleAdsCampaign.date)).scalar()
    return latest or date.today()


@router.post("/process")
def process_ad_spend_data(
    days: int = Query(30, description="Number of trailing days to process"),
    db: Session = Depends(get_db),
):
    """
    Process raw Google Ads data into derived analytics tables.

    Aggregates google_ads_campaigns rows, computes ROAS / waste / optimizations,
    and populates campaign_performance, ad_waste, ad_spend_optimizations.

    Idempotent — safe to re-run.
    """
    try:
        processor = AdSpendProcessor(db)
        result = processor.process(days=days)
        return {"success": True, "data": result}
    except Exception as e:
        log.error(f"Error processing ad spend data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
async def get_ad_spend_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Complete ad spend intelligence dashboard

    Returns:
    - Top priority optimizations
    - Campaign performance (true ROAS vs Google ROAS)
    - Scaling opportunities
    - Waste detection
    - Budget reallocation recommendations
    """
    try:
        service = AdSpendService(db)
        dashboard = await service.get_ad_dashboard(days)

        response = {
            "success": True,
            "data": dashboard
        }

        # Inject stale-data warning if any source is behind
        stale_warning = get_stale_data_warning(db)
        if stale_warning:
            response['data_warning'] = stale_warning

        return response

    except Exception as e:
        log.error(f"Error generating ad spend dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns")
async def get_campaign_performance(
    days: int = Query(30, description="Number of days to analyze"),
    sort_by: str = Query("profit", description="Sort by: profit, roas, spend"),
    db: Session = Depends(get_db)
):
    """
    All campaigns with true ROAS analysis

    Shows:
    - Google ROAS vs True ROAS (with product costs)
    - Actual profit per campaign
    - Performance indicators
    - Budget recommendations
    """
    try:
        service = AdSpendService(db)
        campaigns = await service.get_campaign_performance(days)

        # Sort campaigns
        if sort_by == "roas":
            campaigns.sort(key=lambda x: x['true_metrics'].get('true_roas') or 0, reverse=True)
        elif sort_by == "spend":
            campaigns.sort(key=lambda x: x.get('spend') or 0, reverse=True)
        # Default is already sorted by profit

        return {
            "success": True,
            "data": {
                "campaigns": campaigns,
                "total_count": len(campaigns),
                "summary": {
                    "total_spend": sum(c.get('spend', 0) for c in campaigns),
                    "total_profit": sum(c['true_metrics'].get('profit', 0) for c in campaigns),
                    "avg_true_roas": (
                        sum(c['true_metrics'].get('true_roas', 0) for c in campaigns if c['true_metrics'].get('true_roas'))
                        / len([c for c in campaigns if c['true_metrics'].get('true_roas')])
                    ) if [c for c in campaigns if c['true_metrics'].get('true_roas')] else 0
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting campaign performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scaling-opportunities")
async def get_scaling_opportunities(
    days: int = Query(30, description="Number of days to analyze"),
    min_roas: float = Query(3.0, description="Minimum ROAS to consider"),
    db: Session = Depends(get_db)
):
    """
    Campaigns to scale (high ROAS + budget capped)

    Identifies campaigns that:
    - Have strong ROAS (>3x)
    - Are budget-capped (run out of budget early)
    - Would benefit from increased budget
    """
    try:
        service = AdSpendService(db)
        opportunities = await service.find_scaling_opportunities(days)

        # Filter by minimum ROAS
        if min_roas:
            opportunities = [
                o for o in opportunities
                if o['current_performance'].get('true_roas', 0) >= min_roas
            ]

        return {
            "success": True,
            "data": {
                "opportunities": opportunities,
                "total_count": len(opportunities),
                "total_opportunity": sum(
                    o['expected_impact']['additional_profit_per_month']
                    for o in opportunities
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting scaling opportunities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/waste")
async def get_ad_waste(
    days: int = Query(30, description="Number of days to analyze"),
    waste_type: Optional[str] = Query(None, description="Filter by waste type"),
    min_waste: float = Query(100.0, description="Minimum monthly waste ($)"),
    db: Session = Depends(get_db)
):
    """
    Identified ad spend waste

    Types of waste:
    - brand_cannibalization: Brand campaigns capturing organic traffic
    - below_margin_products: Advertising unprofitable products
    - no_conversion_keywords: Keywords with zero conversions
    - duplicate_targeting: Multiple campaigns targeting same audience
    - poor_quality_score: High CPC due to low quality score
    """
    try:
        service = AdSpendService(db)
        waste = await service.detect_ad_waste(days)

        # Filter by waste type
        if waste_type:
            waste = [w for w in waste if w['waste_type'] == waste_type]

        # Filter by minimum waste
        if min_waste:
            waste = [w for w in waste if w['waste_metrics']['monthly_waste'] >= min_waste]

        return {
            "success": True,
            "data": {
                "waste": waste,
                "total_count": len(waste),
                "total_monthly_waste": sum(
                    w['waste_metrics']['monthly_waste']
                    for w in waste
                ),
                "total_potential_savings": sum(
                    w['recommendation']['expected_savings']
                    for w in waste
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting ad waste: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/budget-reallocation")
async def get_budget_reallocation_recommendations(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Budget reallocation recommendations

    Shows what happens if you move budget:
    - From low-ROAS campaigns → to high-ROAS campaigns
    - Expected revenue and profit impact
    - Confidence level
    """
    try:
        service = AdSpendService(db)
        reallocations = await service.calculate_budget_reallocations(days)

        return {
            "success": True,
            "data": {
                "reallocations": reallocations,
                "total_count": len(reallocations),
                "total_profit_impact": sum(
                    r['expected_impact']['additional_profit']
                    for r in reallocations
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting budget reallocations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/product-performance")
async def get_product_ad_performance(
    days: int = Query(30, description="Number of days to analyze"),
    filter_by: Optional[str] = Query(None, description="Filter: profitable, unprofitable, all"),
    limit: int = Query(20, description="Max products to return"),
    db: Session = Depends(get_db)
):
    """
    Product-level ad performance

    Shows which products:
    - Are profitable to advertise
    - Lose money on every ad-driven sale
    - Should be excluded from campaigns
    """
    try:
        service = AdSpendService(db)
        products = await service.get_product_ad_performance(days, limit)

        # Filter products
        if filter_by == 'profitable':
            products = [p for p in products if p['indicators']['is_profitable']]
        elif filter_by == 'unprofitable':
            products = [p for p in products if p['indicators']['is_losing_money']]

        return {
            "success": True,
            "data": {
                "products": products,
                "total_count": len(products),
                "summary": {
                    "profitable_count": sum(1 for p in products if p['indicators']['is_profitable']),
                    "unprofitable_count": sum(1 for p in products if p['indicators']['is_losing_money']),
                    "total_ad_spend": sum(p['ad_spend']['total_spend'] for p in products),
                    "total_net_profit": sum(p['profitability']['net_profit'] for p in products)
                }
            }
        }

    except Exception as e:
        log.error(f"Error getting product ad performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_ad_spend_insights(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    AI-powered ad spend optimization insights

    Uses Claude to analyze ad spend and provide strategic recommendations:
    - Where to scale
    - Where to cut
    - Budget reallocation strategy
    - Product exclusion recommendations
    - Expected profit impact
    """
    try:
        # Get ad spend data
        ad_service = AdSpendService(db)
        analysis = await ad_service.analyze_all_campaigns(days)

        # Generate LLM insights
        llm_service = LLMService()

        if not llm_service.is_available():
            return {
                "success": False,
                "error": "LLM service not available",
                "data": analysis
            }

        llm_analysis = llm_service.analyze_ad_spend(
            campaigns=analysis['campaigns'],
            scaling_opportunities=analysis['scaling_opportunities'],
            waste_identified=analysis['waste_identified'],
            budget_reallocations=analysis['budget_reallocations'],
            product_performance=analysis['product_performance'],
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
        log.error(f"Error generating LLM ad spend insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quick-wins")
async def get_quick_wins(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Low-effort, high-impact ad optimizations

    Returns simple fixes:
    - Exclude unprofitable products
    - Reduce brand campaign spend
    - Pause zero-conversion keywords
    """
    try:
        service = AdSpendService(db)
        analysis = await service.analyze_all_campaigns(days)

        quick_wins = service._identify_quick_wins(analysis)

        return {
            "success": True,
            "data": {
                "quick_wins": quick_wins,
                "total_count": len(quick_wins),
                "total_savings": sum(
                    float(qw['savings'].replace('$', '').replace('/month', '').replace(',', ''))
                    for qw in quick_wins
                )
            }
        }

    except Exception as e:
        log.error(f"Error getting quick wins: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def get_campaign_trends(
    days: int = Query(90, description="Number of days of trend data"),
    campaign_id: Optional[str] = Query(None, description="Filter to a single campaign"),
    db: Session = Depends(get_db)
):
    """
    Weekly campaign performance trends for charting.

    Returns spend, impressions, clicks, conversions, conversion value,
    and impression share data grouped by week.
    """
    try:
        period_end = _get_ads_period_end(db)
        period_start = period_end - timedelta(days=days)

        query = db.query(
            func.strftime('%Y-W%W', GoogleAdsCampaign.date).label('week'),
            func.min(GoogleAdsCampaign.date).label('week_start'),
            func.max(GoogleAdsCampaign.date).label('week_end'),
            func.sum(GoogleAdsCampaign.impressions).label('impressions'),
            func.sum(GoogleAdsCampaign.clicks).label('clicks'),
            func.sum(GoogleAdsCampaign.cost_micros).label('cost_micros'),
            func.sum(GoogleAdsCampaign.conversions).label('conversions'),
            func.sum(GoogleAdsCampaign.conversions_value).label('conversions_value'),
            func.avg(GoogleAdsCampaign.search_impression_share).label('avg_impression_share'),
            func.avg(GoogleAdsCampaign.search_budget_lost_impression_share).label('avg_budget_lost_is'),
            func.avg(GoogleAdsCampaign.search_rank_lost_impression_share).label('avg_rank_lost_is'),
        ).filter(
            GoogleAdsCampaign.date >= period_start,
            GoogleAdsCampaign.date <= period_end,
        )

        if campaign_id:
            query = query.filter(GoogleAdsCampaign.campaign_id == campaign_id)

        query = query.group_by(
            func.strftime('%Y-W%W', GoogleAdsCampaign.date)
        ).order_by(func.min(GoogleAdsCampaign.date))

        rows = query.all()

        trends = []
        for r in rows:
            spend = float(r.cost_micros or 0) / 1_000_000
            clicks = int(r.clicks or 0)
            conversions = float(r.conversions or 0)
            conv_value = float(r.conversions_value or 0)
            roas = conv_value / spend if spend > 0 else None

            trends.append({
                "week": r.week,
                "week_start": str(r.week_start),
                "week_end": str(r.week_end),
                "impressions": int(r.impressions or 0),
                "clicks": clicks,
                "spend": round(spend, 2),
                "conversions": round(conversions, 1),
                "conversions_value": round(conv_value, 2),
                "roas": round(roas, 2) if roas else None,
                "cpc": round(spend / clicks, 2) if clicks > 0 else None,
                "ctr": round(clicks / int(r.impressions or 1) * 100, 2) if int(r.impressions or 0) > 0 else None,
                "conversion_rate": round(conversions / clicks * 100, 2) if clicks > 0 else None,
                "avg_impression_share": round(float(r.avg_impression_share or 0), 1),
                "avg_budget_lost_is": round(float(r.avg_budget_lost_is or 0), 1),
                "avg_rank_lost_is": round(float(r.avg_rank_lost_is or 0), 1),
            })

        return {
            "success": True,
            "data": {
                "trends": trends,
                "total_weeks": len(trends),
                "period": {
                    "start": period_start.isoformat(),
                    "end": period_end.isoformat(),
                    "days": days,
                },
                "campaign_id": campaign_id,
            }
        }

    except Exception as e:
        log.error(f"Error getting campaign trends: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/impression-share")
async def get_impression_share_analysis(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Campaign impression share analysis.

    Shows current impression share, budget lost IS, rank lost IS per campaign,
    with month-over-month comparison and flags for declining share.
    """
    try:
        period_end = _get_ads_period_end(db)
        period_start = period_end - timedelta(days=days)

        # Also get prior period for comparison
        prior_end = period_start
        prior_start = prior_end - timedelta(days=days)

        def _get_is_data(start, end):
            rows = db.query(
                GoogleAdsCampaign.campaign_id,
                func.max(GoogleAdsCampaign.campaign_name).label('campaign_name'),
                func.avg(GoogleAdsCampaign.search_impression_share).label('avg_is'),
                func.avg(GoogleAdsCampaign.search_budget_lost_impression_share).label('avg_budget_lost'),
                func.avg(GoogleAdsCampaign.search_rank_lost_impression_share).label('avg_rank_lost'),
                func.sum(GoogleAdsCampaign.impressions).label('total_impressions'),
                func.sum(GoogleAdsCampaign.cost_micros).label('total_cost'),
            ).filter(
                GoogleAdsCampaign.date >= start,
                GoogleAdsCampaign.date <= end,
                GoogleAdsCampaign.search_impression_share > 0,
            ).group_by(
                GoogleAdsCampaign.campaign_id
            ).all()

            return {
                r.campaign_id: {
                    "campaign_name": r.campaign_name,
                    "impression_share": round(float(r.avg_is or 0), 1),
                    "budget_lost_is": round(float(r.avg_budget_lost or 0), 1),
                    "rank_lost_is": round(float(r.avg_rank_lost or 0), 1),
                    "impressions": int(r.total_impressions or 0),
                    "spend": round(float(r.total_cost or 0) / 1_000_000, 2),
                }
                for r in rows
            }

        current_data = _get_is_data(period_start, period_end)
        prior_data = _get_is_data(prior_start, prior_end)

        campaigns = []
        for cid, current in current_data.items():
            prior = prior_data.get(cid, {})
            prior_is = prior.get('impression_share', 0)
            is_change = current['impression_share'] - prior_is if prior_is > 0 else None

            campaigns.append({
                "campaign_id": cid,
                "campaign_name": current['campaign_name'],
                "current": current,
                "prior": prior if prior else None,
                "impression_share_change": round(is_change, 1) if is_change is not None else None,
                "is_declining": is_change is not None and is_change < -5,
                "is_budget_constrained": current['budget_lost_is'] > 10,
                "is_rank_constrained": current['rank_lost_is'] > 50,
                "primary_constraint": (
                    "budget" if current['budget_lost_is'] > current['rank_lost_is']
                    else "rank"
                ),
            })

        # Sort by impression share change (worst first)
        campaigns.sort(key=lambda x: x.get('impression_share_change') or 0)

        return {
            "success": True,
            "data": {
                "campaigns": campaigns,
                "total_count": len(campaigns),
                "declining_count": sum(1 for c in campaigns if c['is_declining']),
                "budget_constrained_count": sum(1 for c in campaigns if c['is_budget_constrained']),
                "rank_constrained_count": sum(1 for c in campaigns if c['is_rank_constrained']),
                "period": {
                    "current": {"start": period_start.isoformat(), "end": period_end.isoformat()},
                    "prior": {"start": prior_start.isoformat(), "end": prior_end.isoformat()},
                },
            }
        }

    except Exception as e:
        log.error(f"Error getting impression share: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enhanced-dashboard")
async def get_enhanced_dashboard(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Master endpoint returning all analytics sections for the enhanced dashboard."""
    try:
        service = AdSpendService(db)

        # Fetch all data sections in parallel-ish (they share db session so sequential)
        campaigns = await service.get_campaign_performance(days)
        health_scores = await service.calculate_health_scores(days)
        deep_metrics = await service.get_campaign_deep_metrics(days)
        concentration = await service.calculate_concentration_risk(days)
        break_even = await service.calculate_break_even(days)
        diminishing = await service.analyze_diminishing_returns(days * 3)
        competitor = await service.calculate_competitor_pressure(days * 3)
        type_comparison = await service.compare_campaign_types(days)
        anomalies = await service.detect_anomalies(days)
        forecast = await service.forecast_performance(days * 3)
        google_vs_reality = await service.get_google_vs_reality(days)
        waste = await service.detect_ad_waste(days)
        reallocations = await service.calculate_budget_reallocations(days)
        products = await service.get_product_ad_performance(days)
        quick_wins = service._identify_quick_wins({
            'waste_identified': waste,
            'scaling_opportunities': [],
            'budget_reallocations': reallocations,
        })

        # Summary
        total_spend = sum(c.get('spend', 0) for c in campaigns)
        total_profit = sum(c['true_metrics'].get('profit', 0) for c in campaigns)
        total_waste = sum(w['waste_metrics']['monthly_waste'] for w in waste)

        return {
            "success": True,
            "data": {
                "summary": {
                    "total_spend": total_spend,
                    "total_profit": total_profit,
                    "total_waste": total_waste,
                    "campaign_count": len(campaigns),
                    "profitable_count": sum(1 for c in campaigns if c['indicators']['is_profitable']),
                    "wasting_count": sum(1 for c in campaigns if c['indicators']['is_wasting_budget']),
                },
                "campaigns": campaigns,
                "health_scores": health_scores,
                "deep_metrics": deep_metrics,
                "concentration": concentration,
                "break_even": break_even,
                "diminishing_returns": diminishing,
                "competitor_pressure": competitor,
                "type_comparison": type_comparison,
                "anomalies": anomalies,
                "forecast": forecast,
                "google_vs_reality": google_vs_reality,
                "waste": waste,
                "reallocations": reallocations,
                "products": products,
                "quick_wins": quick_wins,
                "period_days": days,
            }
        }
    except Exception as e:
        log.error(f"Error generating enhanced dashboard: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health-scores")
async def get_health_scores(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Campaign health scores combining ROAS, trend, waste, and impression share factors."""
    try:
        service = AdSpendService(db)
        scores = await service.calculate_health_scores(days)
        return {
            "success": True,
            "data": {
                "scores": scores,
                "count": len(scores),
            }
        }
    except Exception as e:
        log.error(f"Error calculating health scores: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deep-metrics")
async def get_deep_metrics(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Deep campaign metrics including day-of-week and hourly performance breakdowns."""
    try:
        service = AdSpendService(db)
        metrics = await service.get_campaign_deep_metrics(days)
        return {
            "success": True,
            "data": {
                "metrics": metrics,
                "count": len(metrics),
            }
        }
    except Exception as e:
        log.error(f"Error getting deep metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/concentration")
async def get_concentration_risk(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Spend and revenue concentration risk analysis across campaigns."""
    try:
        service = AdSpendService(db)
        concentration = await service.calculate_concentration_risk(days)
        return {
            "success": True,
            "data": concentration,
        }
    except Exception as e:
        log.error(f"Error calculating concentration risk: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/break-even")
async def get_break_even(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Break-even analysis per campaign showing days and spend needed to reach profitability."""
    try:
        service = AdSpendService(db)
        campaigns = await service.calculate_break_even(days)
        return {
            "success": True,
            "data": {
                "campaigns": campaigns,
                "count": len(campaigns),
            }
        }
    except Exception as e:
        log.error(f"Error calculating break-even: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diminishing-returns")
async def get_diminishing_returns(
    days: int = Query(90, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Diminishing returns analysis identifying campaigns past their optimal spend level."""
    try:
        service = AdSpendService(db)
        campaigns = await service.analyze_diminishing_returns(days)
        return {
            "success": True,
            "data": {
                "campaigns": campaigns,
                "count": len(campaigns),
            }
        }
    except Exception as e:
        log.error(f"Error analyzing diminishing returns: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/competitor-pressure")
async def get_competitor_pressure(
    days: int = Query(90, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Competitor pressure indicators based on impression share and CPC trends."""
    try:
        service = AdSpendService(db)
        campaigns = await service.calculate_competitor_pressure(days)
        return {
            "success": True,
            "data": {
                "campaigns": campaigns,
                "count": len(campaigns),
            }
        }
    except Exception as e:
        log.error(f"Error calculating competitor pressure: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/type-comparison")
async def get_type_comparison(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Compare performance across campaign types (Search, Shopping, Display, etc.)."""
    try:
        service = AdSpendService(db)
        comparison = await service.compare_campaign_types(days)
        return {
            "success": True,
            "data": comparison,
        }
    except Exception as e:
        log.error(f"Error comparing campaign types: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies")
async def get_anomalies(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Detect anomalies in campaign performance metrics (spend spikes, CTR drops, etc.)."""
    try:
        service = AdSpendService(db)
        anomalies = await service.detect_anomalies(days)
        return {
            "success": True,
            "data": {
                "anomalies": anomalies,
                "count": len(anomalies),
                "critical_count": sum(1 for a in anomalies if a.get('severity') == 'critical'),
            }
        }
    except Exception as e:
        log.error(f"Error detecting anomalies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast")
async def get_forecast(
    days: int = Query(90, description="Number of days of historical data for forecasting"),
    db: Session = Depends(get_db)
):
    """Forecast future campaign performance based on historical trends."""
    try:
        service = AdSpendService(db)
        forecast = await service.forecast_performance(days)
        return {
            "success": True,
            "data": forecast,
        }
    except Exception as e:
        log.error(f"Error forecasting performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/google-vs-reality")
async def get_google_vs_reality(
    days: int = Query(30, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """Compare Google Ads reported metrics against actual tracked revenue and profit."""
    try:
        service = AdSpendService(db)
        comparison = await service.get_google_vs_reality(days)
        return {
            "success": True,
            "data": comparison,
        }
    except Exception as e:
        log.error(f"Error comparing Google vs reality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
