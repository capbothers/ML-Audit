"""
Pricing Impact API Routes

Endpoints for pricing intelligence:
- SKU pricing sensitivity analysis
- Brand-level pricing impact summary
- Unmatchable revenue risk report
- LLM-powered pricing insights
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from datetime import datetime, date, timedelta
from typing import Optional, List
from collections import defaultdict, Counter
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.base import get_db
from app.services.pricing_intelligence_service import PricingIntelligenceService, COMPETITOR_COLUMNS
from app.models.competitive_pricing import CompetitivePricing
from app.models.product_cost import ProductCost
from app.models.shopify import ShopifyOrderItem
from app.utils.logger import log
from app.utils.cache import get_cached, set_cached, _MISS

router = APIRouter(prefix="/pricing", tags=["pricing"])


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _latest_pricing_date(db: Session) -> date | None:
    """Cached latest pricing date — changes at most once per day."""
    cached = get_cached("pricing_latest_date")
    if cached is not _MISS:
        return cached
    val = db.query(func.max(CompetitivePricing.pricing_date)).scalar()
    set_cached("pricing_latest_date", val, 300)
    return val


def _competitor_prices(cp: CompetitivePricing):
    prices = []
    for name, col in COMPETITOR_COLUMNS:
        val = getattr(cp, col)
        if val is None:
            continue
        try:
            price_val = float(val)
        except Exception:
            continue
        prices.append({
            "competitor": name,
            "price": price_val,
        })
    return prices


def _cheapest_competitor(prices):
    if not prices:
        return None, None
    cheapest = min(prices, key=lambda x: x["price"])
    return cheapest["competitor"], cheapest["price"]


def _following_competitors(prices, our_price: float, tolerance: float = 1.0):
    if our_price is None:
        return []
    following = []
    for item in prices:
        if abs(item["price"] - our_price) <= tolerance:
            following.append(item)
    return following


def _parse_category(title: str) -> str:
    """Derive product category from title via keyword matching."""
    t = (title or "").lower()
    if any(w in t for w in ("mixer", "tap ", "tapware", "spout", "diverter", "valve")):
        return "Tapware"
    if any(w in t for w in ("basin", "sink", "vanity")):
        return "Basins"
    if any(w in t for w in ("toilet", " pan ", "pan only", "suite", "cistern", "bidet", "seat")):
        return "Toilets"
    if "shower" in t:
        return "Showers"
    if "bath" in t and "bathroom" not in t:
        return "Baths"
    if any(w in t for w in ("towel", "rail", "ring", "hook", "holder", "soap",
                             "accessory", "robe", "shelf", "bracket", "tray", "mirror")):
        return "Accessories"
    return "Other"


def _parse_collection(title: str, vendor: str) -> str:
    """Extract collection name from title by stripping the brand prefix."""
    if not title or not vendor:
        return "Uncategorised"
    stripped = title.strip()
    # Strip full vendor name first (e.g. "Nero Tapware")
    if stripped.startswith(vendor):
        stripped = stripped[len(vendor):].strip()
    else:
        # Try first word only (e.g. "Nero" from "Nero Tapware")
        first_word = vendor.split()[0]
        if stripped.startswith(first_word):
            stripped = stripped[len(first_word):].strip()
    words = stripped.split()
    if not words:
        return "Uncategorised"
    # Include second word if it's a Roman numeral or qualifier
    if len(words) >= 2 and words[1] in ("II", "III", "IV", "V", "Plus", "Pro", "Mini"):
        return f"{words[0]} {words[1]}"
    return words[0]


def _analyze_snapshot(rows: list, vendor_filter: str = None) -> dict:
    """
    Analyze a list of CompetitivePricing rows.
    Returns per-SKU analysis dicts with category, collection, discount, floor gap, cheapest competitor.
    """
    analyzed = []
    for cp in rows:
        if vendor_filter and cp.vendor != vendor_filter:
            continue
        prices = _competitor_prices(cp)
        cheapest_name, cheapest_price = _cheapest_competitor(prices)
        rrp = float(cp.rrp) if cp.rrp else None
        min_price = float(cp.minimum_price) if cp.minimum_price else None

        discount_pct = None
        if rrp and rrp > 0 and cheapest_price is not None:
            discount_pct = round((rrp - cheapest_price) / rrp * 100, 1)

        below_floor = False
        gap_below = 0.0
        if min_price and cheapest_price is not None and cheapest_price < min_price:
            below_floor = True
            gap_below = round(min_price - cheapest_price, 2)

        analyzed.append({
            "sku": cp.variant_sku,
            "title": cp.title or "",
            "vendor": cp.vendor or "",
            "category": _parse_category(cp.title),
            "collection": _parse_collection(cp.title, cp.vendor),
            "rrp": rrp,
            "our_min": min_price,
            "market_lowest": cheapest_price,
            "discount_pct": discount_pct,
            "below_floor": below_floor,
            "gap_below": gap_below,
            "cheapest_competitor": cheapest_name,
            "competitor_prices": prices,
        })
    return analyzed


# ─────────────────────────────────────────────
# NEW ENDPOINTS: Overview + Brand Report
# ─────────────────────────────────────────────

@router.get("/brands")
async def get_brands(db: Session = Depends(get_db)):
    """List all brands with SKU counts from the latest pricing snapshot."""
    try:
        latest_date = _latest_pricing_date(db)
        if not latest_date:
            return {"success": True, "data": []}
        rows = (
            db.query(CompetitivePricing.vendor, func.count(CompetitivePricing.variant_sku))
            .filter(CompetitivePricing.pricing_date == latest_date)
            .group_by(CompetitivePricing.vendor)
            .order_by(CompetitivePricing.vendor)
            .all()
        )
        return {
            "success": True,
            "data": [{"brand": r[0], "sku_count": r[1]} for r in rows if r[0]],
        }
    except Exception as e:
        log.error(f"Error in /pricing/brands: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overview")
async def get_overview(db: Session = Depends(get_db)):
    """
    Market-wide pricing overview.
    All-brand KPIs, brand pressure table, competitor leaderboard, category pressure.
    """
    cached = get_cached("pricing_overview")
    if cached is not _MISS:
        return cached

    try:
        latest_date = _latest_pricing_date(db)
        if not latest_date:
            return {"success": True, "data": {"snapshot_date": None, "total_skus": 0}}

        all_rows = (
            db.query(CompetitivePricing)
            .filter(CompetitivePricing.pricing_date == latest_date)
            .all()
        )
        analyzed = _analyze_snapshot(all_rows)

        # KPIs
        with_discount = [a for a in analyzed if a["discount_pct"] is not None]
        avg_discount = round(sum(a["discount_pct"] for a in with_discount) / len(with_discount), 1) if with_discount else 0
        below_floor = [a for a in analyzed if a["below_floor"]]
        total_gap = round(sum(a["gap_below"] for a in below_floor), 2)

        # Brand pressure
        brand_map = defaultdict(list)
        for a in analyzed:
            brand_map[a["vendor"]].append(a)
        brand_pressure = []
        for brand, items in brand_map.items():
            if not brand:
                continue
            wd = [i for i in items if i["discount_pct"] is not None]
            bf = [i for i in items if i["below_floor"]]
            # Most aggressive competitor for this brand
            comp_counter = Counter(i["cheapest_competitor"] for i in bf if i["cheapest_competitor"])
            most_agg = comp_counter.most_common(1)[0][0] if comp_counter else None
            brand_pressure.append({
                "brand": brand,
                "total_skus": len(items),
                "undercut_count": len(bf),
                "avg_discount_pct": round(sum(i["discount_pct"] for i in wd) / len(wd), 1) if wd else 0,
                "avg_gap_when_below": round(sum(i["gap_below"] for i in bf) / len(bf), 2) if bf else 0,
                "most_aggressive": most_agg,
            })
        brand_pressure.sort(key=lambda x: x["undercut_count"], reverse=True)

        # Competitor leaderboard
        comp_cheapest = Counter()
        comp_below = Counter()
        comp_gaps = defaultdict(list)
        for a in analyzed:
            cn = a["cheapest_competitor"]
            if cn:
                comp_cheapest[cn] += 1
                if a["below_floor"]:
                    comp_below[cn] += 1
                    comp_gaps[cn].append(a["gap_below"])
        competitor_leaderboard = []
        for comp, cnt in comp_cheapest.most_common(20):
            gaps = comp_gaps.get(comp, [])
            competitor_leaderboard.append({
                "competitor": comp,
                "times_cheapest": cnt,
                "times_below_floor": comp_below.get(comp, 0),
                "avg_gap_when_below": round(sum(gaps) / len(gaps), 2) if gaps else 0,
            })

        # Category pressure
        cat_map = defaultdict(list)
        for a in analyzed:
            cat_map[a["category"]].append(a)
        category_pressure = []
        for cat, items in cat_map.items():
            wd = [i for i in items if i["discount_pct"] is not None]
            bf = [i for i in items if i["below_floor"]]
            category_pressure.append({
                "category": cat,
                "sku_count": len(items),
                "avg_discount_pct": round(sum(i["discount_pct"] for i in wd) / len(wd), 1) if wd else 0,
                "skus_below_floor": len(bf),
                "avg_gap_below": round(sum(i["gap_below"] for i in bf) / len(bf), 2) if bf else 0,
            })
        category_pressure.sort(key=lambda x: x["avg_discount_pct"], reverse=True)

        result = {
            "success": True,
            "data": {
                "snapshot_date": str(latest_date),
                "total_skus": len(analyzed),
                "kpis": {
                    "avg_market_discount_pct": avg_discount,
                    "skus_below_floor": len(below_floor),
                    "skus_below_floor_pct": round(len(below_floor) / len(analyzed) * 100, 1) if analyzed else 0,
                    "total_gap_below_floor": total_gap,
                },
                "brand_pressure": brand_pressure,
                "competitor_leaderboard": competitor_leaderboard,
                "category_pressure": category_pressure,
            },
        }
        set_cached("pricing_overview", result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /pricing/overview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brand-report")
async def get_brand_report(
    brand: str = Query(..., description="Brand/vendor name"),
    db: Session = Depends(get_db),
):
    """
    Per-brand pricing report: category breakdown, collection breakdown,
    competitor activity, monthly trends, heavily discounted SKUs.
    """
    cached = get_cached(f"pricing_brand|{brand}")
    if cached is not _MISS:
        return cached

    try:
        latest_date = _latest_pricing_date(db)
        if not latest_date:
            return {"success": True, "data": {"brand": brand, "error": "No pricing data"}}

        # Latest snapshot for this brand
        brand_rows = (
            db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.pricing_date == latest_date,
                CompetitivePricing.vendor == brand,
            )
            .all()
        )
        if not brand_rows:
            return {"success": True, "data": {"brand": brand, "error": "Brand not found"}}

        analyzed = _analyze_snapshot(brand_rows)

        # KPIs
        with_discount = [a for a in analyzed if a["discount_pct"] is not None]
        avg_discount = round(sum(a["discount_pct"] for a in with_discount) / len(with_discount), 1) if with_discount else 0
        below_floor = [a for a in analyzed if a["below_floor"]]
        total_gap = round(sum(a["gap_below"] for a in below_floor), 2)

        # Category breakdown
        cat_map = defaultdict(list)
        for a in analyzed:
            cat_map[a["category"]].append(a)
        category_breakdown = []
        for cat, items in cat_map.items():
            wd = [i for i in items if i["discount_pct"] is not None]
            bf = [i for i in items if i["below_floor"]]
            comp_counter = Counter(i["cheapest_competitor"] for i in bf if i["cheapest_competitor"])
            most_agg = comp_counter.most_common(1)[0][0] if comp_counter else None
            category_breakdown.append({
                "category": cat,
                "sku_count": len(items),
                "avg_discount_pct": round(sum(i["discount_pct"] for i in wd) / len(wd), 1) if wd else 0,
                "max_discount_pct": round(max((i["discount_pct"] for i in wd), default=0), 1),
                "skus_below_floor": len(bf),
                "avg_gap_below": round(sum(i["gap_below"] for i in bf) / len(bf), 2) if bf else 0,
                "most_aggressive": most_agg,
            })
        category_breakdown.sort(key=lambda x: x["avg_discount_pct"], reverse=True)

        # Collection breakdown
        col_map = defaultdict(list)
        for a in analyzed:
            col_map[a["collection"]].append(a)
        collection_breakdown = []
        for col, items in col_map.items():
            wd = [i for i in items if i["discount_pct"] is not None]
            bf = [i for i in items if i["below_floor"]]
            comp_counter = Counter(i["cheapest_competitor"] for i in bf if i["cheapest_competitor"])
            most_agg = comp_counter.most_common(1)[0][0] if comp_counter else None
            collection_breakdown.append({
                "collection": col,
                "sku_count": len(items),
                "avg_discount_pct": round(sum(i["discount_pct"] for i in wd) / len(wd), 1) if wd else 0,
                "skus_below_floor": len(bf),
                "avg_gap_below": round(sum(i["gap_below"] for i in bf) / len(bf), 2) if bf else 0,
                "most_aggressive": most_agg,
            })
        collection_breakdown.sort(key=lambda x: x["avg_discount_pct"], reverse=True)

        # Competitor activity
        comp_data = defaultdict(lambda: {"below": 0, "gaps": [], "cats": [], "cols": []})
        for a in analyzed:
            if not a["below_floor"]:
                continue
            for cp in a["competitor_prices"]:
                if a["our_min"] and cp["price"] < a["our_min"]:
                    c = comp_data[cp["competitor"]]
                    c["below"] += 1
                    c["gaps"].append(a["our_min"] - cp["price"])
                    c["cats"].append(a["category"])
                    c["cols"].append(a["collection"])
        competitor_activity = []
        for comp, info in comp_data.items():
            top_cat = Counter(info["cats"]).most_common(1)
            top_col = Counter(info["cols"]).most_common(1)
            competitor_activity.append({
                "competitor": comp,
                "times_below_floor": info["below"],
                "avg_gap_when_below": round(sum(info["gaps"]) / len(info["gaps"]), 2) if info["gaps"] else 0,
                "max_gap": round(max(info["gaps"], default=0), 2),
                "top_category": top_cat[0][0] if top_cat else None,
                "top_collection": top_col[0][0] if top_col else None,
            })
        competitor_activity.sort(key=lambda x: x["times_below_floor"], reverse=True)

        # Monthly trends - fetch ALL dates for this brand
        monthly_rows = (
            db.query(CompetitivePricing)
            .filter(CompetitivePricing.vendor == brand)
            .order_by(CompetitivePricing.pricing_date)
            .all()
        )
        # Group by month, use latest snapshot per month
        month_snapshots = defaultdict(dict)  # month -> {date -> [rows]}
        for r in monthly_rows:
            m = str(r.pricing_date)[:7]  # "YYYY-MM"
            d = str(r.pricing_date)
            if d not in month_snapshots[m]:
                month_snapshots[m][d] = []
            month_snapshots[m][d].append(r)

        MONTH_NAMES = {
            "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
            "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
            "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
        }

        monthly_trends = []
        for month in sorted(month_snapshots.keys()):
            dates = month_snapshots[month]
            # Use latest date in the month
            latest_in_month = max(dates.keys())
            snap = _analyze_snapshot(dates[latest_in_month])
            wd = [a for a in snap if a["discount_pct"] is not None]
            bf = [a for a in snap if a["below_floor"]]
            mm = month.split("-")[1]
            yyyy = month.split("-")[0]
            monthly_trends.append({
                "month": month,
                "month_name": f"{MONTH_NAMES.get(mm, mm)} {yyyy}",
                "avg_discount_pct": round(sum(a["discount_pct"] for a in wd) / len(wd), 1) if wd else 0,
                "skus_below_floor": len(bf),
                "avg_gap_below": round(sum(a["gap_below"] for a in bf) / len(bf), 2) if bf else 0,
                "total_skus": len(snap),
                "snapshot_count": len(dates),
            })

        # Heavily discounted SKUs (top 50)
        heavily_discounted = sorted(
            [a for a in analyzed if a["discount_pct"] is not None],
            key=lambda x: x["discount_pct"],
            reverse=True,
        )[:50]

        result = {
            "success": True,
            "data": {
                "brand": brand,
                "snapshot_date": str(latest_date),
                "total_skus": len(analyzed),
                "kpis": {
                    "avg_market_discount_pct": avg_discount,
                    "skus_below_floor": len(below_floor),
                    "skus_with_rrp": len(with_discount),
                    "total_gap_below_floor": total_gap,
                },
                "category_breakdown": category_breakdown,
                "collection_breakdown": collection_breakdown,
                "competitor_activity": competitor_activity,
                "monthly_trends": monthly_trends,
                "heavily_discounted_skus": [{
                    "sku": a["sku"],
                    "title": a["title"],
                    "collection": a["collection"],
                    "category": a["category"],
                    "rrp": a["rrp"],
                    "our_min": a["our_min"],
                    "market_lowest": a["market_lowest"],
                    "market_discount_pct": a["discount_pct"],
                    "below_floor": a["below_floor"],
                    "gap": a["gap_below"],
                    "cheapest_competitor": a["cheapest_competitor"],
                } for a in heavily_discounted],
            },
        }
        set_cached(f"pricing_brand|{brand}", result, 300)
        return result
    except Exception as e:
        log.error(f"Error in /pricing/brand-report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brand-report/pdf")
async def brand_report_pdf(
    brand: str = Query(..., description="Brand/vendor name"),
    db: Session = Depends(get_db),
):
    """Generate a downloadable PDF report for a brand."""
    try:
        # Re-use the brand-report logic
        report_response = await get_brand_report(brand=brand, db=db)
        data = report_response.get("data", {})
        if data.get("error"):
            raise HTTPException(status_code=404, detail=data["error"])

        from app.services.brand_report_pdf import generate_brand_report_pdf
        pdf_buf = generate_brand_report_pdf(data)

        safe_brand = brand.replace(" ", "_").replace("/", "_")
        filename = f"Pricing_Report_{safe_brand}_{date.today().isoformat()}.pdf"

        return StreamingResponse(
            pdf_buf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error in /pricing/brand-report/pdf: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
# EXISTING ENDPOINTS
# ─────────────────────────────────────────────

@router.get("/impact")
async def get_pricing_impact(
    days: int = Query(30, description="Lookback window in days for sales data"),
    decline_threshold: float = Query(10.0, description="% unit decline to flag as price-sensitive"),
    limit: int = Query(100, description="Max SKUs to return"),
    db: Session = Depends(get_db),
):
    """
    SKU Pricing Sensitivity List.

    Returns SKUs with their price gap to the cheapest competitor,
    sales trends (30/60/90 day), and a price_sensitive boolean flag.
    """
    try:
        service = PricingIntelligenceService(db)
        result = await service.get_sku_pricing_sensitivity(
            days=days, decline_threshold=decline_threshold, limit=limit
        )
        return {"success": True, "data": result}
    except Exception as e:
        log.error(f"Error in /pricing/impact: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/brand-summary")
async def get_brand_summary(
    days: int = Query(30, description="Lookback window in days"),
    decline_threshold: float = Query(10.0, description="% unit decline threshold"),
    db: Session = Depends(get_db),
):
    """
    Brand Pricing Impact Summary.

    Aggregated view per brand: # SKUs undercut, avg price gap,
    % unit decline vs prior period, and revenue at risk.
    """
    try:
        service = PricingIntelligenceService(db)
        result = await service.get_brand_pricing_impact(
            days=days, decline_threshold=decline_threshold
        )
        return {"success": True, "data": result}
    except Exception as e:
        log.error(f"Error in /pricing/brand-summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/unmatchable")
async def get_unmatchable(
    days: int = Query(30, description="Lookback window in days"),
    db: Session = Depends(get_db),
):
    """
    Unmatchable Revenue Risk.

    SKUs where the cheapest competitor price is below our minimum floor price.
    Shows total revenue at risk and affected orders.
    """
    try:
        service = PricingIntelligenceService(db)
        result = await service.get_unmatchable_revenue_risk(days=days)
        return {"success": True, "data": result}
    except Exception as e:
        log.error(f"Error in /pricing/unmatchable: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/llm-insights")
async def get_llm_insights(
    days: int = Query(30, description="Lookback window in days"),
    db: Session = Depends(get_db),
):
    """
    LLM-powered pricing impact analysis.

    Generates a natural-language summary of pricing competitiveness,
    price-sensitive SKUs, and revenue at risk.
    """
    try:
        service = PricingIntelligenceService(db)

        sku_data = await service.get_sku_pricing_sensitivity(days=days, limit=50)
        brand_data = await service.get_brand_pricing_impact(days=days)
        unmatchable_data = await service.get_unmatchable_revenue_risk(days=days)

        from app.services.llm_service import LLMService
        llm = LLMService()

        if not llm.is_available():
            return {
                "success": True,
                "data": {
                    "sku_sensitivity": sku_data,
                    "brand_impact": brand_data,
                    "unmatchable_risk": unmatchable_data,
                    "llm_analysis": None,
                    "llm_note": "LLM service not available"
                }
            }

        analysis = llm.analyze_pricing_impact({
            'sku_sensitivity': sku_data,
            'brand_impact': brand_data,
            'unmatchable_risk': unmatchable_data,
        })

        return {
            "success": True,
            "data": {
                "sku_sensitivity": sku_data,
                "brand_impact": brand_data,
                "unmatchable_risk": unmatchable_data,
                "llm_analysis": analysis,
            }
        }
    except Exception as e:
        log.error(f"Error in /pricing/llm-insights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_skus(
    query: str = Query(..., min_length=2, description="SKU, title, or vendor search"),
    limit: int = Query(20, description="Max results"),
    db: Session = Depends(get_db),
):
    """
    Search SKU/title/vendor in competitive pricing data.
    Fast-path: tries exact SKU match first (index hit), then falls back to
    LIKE search scoped to the latest snapshot only.
    """
    try:
        query_clean = query.strip()

        # Fast-path: exact SKU match (uses variant_sku index)
        exact = (
            db.query(CompetitivePricing.variant_sku, CompetitivePricing.title, CompetitivePricing.vendor)
            .filter(func.lower(CompetitivePricing.variant_sku) == query_clean.lower())
            .order_by(CompetitivePricing.pricing_date.desc())
            .first()
        )
        if exact:
            return {
                "success": True,
                "data": [{"sku": exact[0], "title": exact[1] or "", "vendor": exact[2] or ""}],
            }

        # Slow-path: LIKE search, but only on the latest snapshot (not all history)
        latest_date = _latest_pricing_date(db)
        if not latest_date:
            return {"success": True, "data": []}

        q = f"%{query_clean.lower()}%"
        rows = (
            db.query(CompetitivePricing.variant_sku, CompetitivePricing.title, CompetitivePricing.vendor)
            .filter(
                CompetitivePricing.pricing_date == latest_date,
                func.lower(CompetitivePricing.variant_sku).like(q)
                | func.lower(CompetitivePricing.title).like(q)
                | func.lower(CompetitivePricing.vendor).like(q),
            )
            .order_by(CompetitivePricing.variant_sku)
            .distinct()
            .limit(limit)
            .all()
        )
        return {
            "success": True,
            "data": [
                {"sku": r[0], "title": r[1] or "", "vendor": r[2] or ""}
                for r in rows
            ],
        }
    except Exception as e:
        log.error(f"Error in /pricing/search: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sku")
async def get_sku_pricing(
    sku: str = Query(..., description="Variant SKU"),
    days: int = Query(30, description="History window in days"),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    db: Session = Depends(get_db),
):
    """
    SKU pricing intelligence: current snapshot + history.
    Cached for 60s per SKU+days combination.
    """
    try:
        sku_norm = sku.strip()
        if start_date and end_date:
            start_dt = _parse_date(start_date)
            end_dt = _parse_date(end_date)
        else:
            end_dt = date.today()
            start_dt = end_dt - timedelta(days=days)

        cache_key = f"pricing_sku|{sku_norm.lower()}|{start_dt}|{end_dt}"
        cached = get_cached(cache_key)
        if cached is not _MISS:
            return cached

        latest = (
            db.query(CompetitivePricing)
            .filter(func.lower(CompetitivePricing.variant_sku) == sku_norm.lower())
            .order_by(CompetitivePricing.pricing_date.desc())
            .first()
        )
        if not latest:
            return {"success": True, "data": {"sku": sku_norm, "error": "SKU not found"}}

        cost = (
            db.query(ProductCost)
            .filter(func.lower(ProductCost.vendor_sku) == sku_norm.lower())
            .first()
        )

        prices = _competitor_prices(latest)
        cheapest_name, cheapest_price = _cheapest_competitor(prices)
        our_price = float(latest.current_price) if latest.current_price is not None else None
        following = _following_competitors(prices, our_price)

        history_rows = (
            db.query(CompetitivePricing)
            .filter(
                func.lower(CompetitivePricing.variant_sku) == sku_norm.lower(),
                CompetitivePricing.pricing_date >= start_dt,
                CompetitivePricing.pricing_date <= end_dt,
            )
            .order_by(CompetitivePricing.pricing_date.asc())
            .all()
        )

        # Daily sales — query ShopifyOrderItem directly (has order_date + financial_status)
        sales_rows = (
            db.query(
                func.date(ShopifyOrderItem.order_date).label("day"),
                func.sum(ShopifyOrderItem.quantity).label("units"),
                func.sum(ShopifyOrderItem.quantity * ShopifyOrderItem.price).label("revenue"),
            )
            .filter(
                func.lower(ShopifyOrderItem.sku) == sku_norm.lower(),
                func.date(ShopifyOrderItem.order_date) >= start_dt,
                func.date(ShopifyOrderItem.order_date) <= end_dt,
                ShopifyOrderItem.financial_status.in_(["paid", "partially_refunded"]),
            )
            .group_by(func.date(ShopifyOrderItem.order_date))
            .all()
        )
        sales_by_date = {
            str(r.day): {"units": int(r.units or 0), "revenue": float(r.revenue or 0)}
            for r in sales_rows
        }

        # Sales summary
        total_units = sum(s["units"] for s in sales_by_date.values())
        total_revenue = sum(s["revenue"] for s in sales_by_date.values())

        history = []
        for row in history_rows:
            row_prices = _competitor_prices(row)
            row_cheapest_name, row_cheapest_price = _cheapest_competitor(row_prices)
            day_str = str(row.pricing_date)
            day_sales = sales_by_date.get(day_str, {"units": 0, "revenue": 0})
            history.append({
                "date": day_str,
                "our_price": float(row.current_price) if row.current_price is not None else None,
                "lowest_competitor": row_cheapest_price,
                "lowest_competitor_name": row_cheapest_name,
                "minimum_price": float(row.minimum_price) if row.minimum_price is not None else None,
                "rrp": float(row.rrp) if row.rrp is not None else None,
                "competitors": row_prices,
                "units_sold": day_sales["units"],
                "day_revenue": day_sales["revenue"],
            })

        result = {
            "success": True,
            "data": {
                "sku": sku_norm,
                "title": latest.title or "",
                "vendor": latest.vendor or (cost.vendor if cost else ""),
                "pricing_date": str(latest.pricing_date),
                "current_price": our_price,
                "minimum_price": float(latest.minimum_price) if latest.minimum_price is not None else None,
                "nett_cost": float(latest.nett_cost) if latest.nett_cost is not None else (
                    float(cost.nett_nett_cost_inc_gst) if cost and cost.nett_nett_cost_inc_gst is not None else None
                ),
                "rrp": float(latest.rrp) if latest.rrp is not None else (
                    float(cost.rrp_inc_gst) if cost and cost.rrp_inc_gst is not None else None
                ),
                "set_price": float(latest.set_price) if latest.set_price is not None else (
                    float(cost.set_price) if cost and cost.set_price is not None else None
                ),
                "match_rule": latest.match_rule,
                "do_not_follow": bool(cost.do_not_follow) if cost else False,
                "min_margin_pct": float(cost.min_margin_pct) if cost and cost.min_margin_pct is not None else None,
                "discount_off_rrp_pct": float(cost.discount_off_rrp_pct) if cost and cost.discount_off_rrp_pct is not None else None,
                "comments": cost.comments if cost else None,
                "cost_breakdown": {
                    "invoice_price_inc_gst": float(cost.invoice_price_inc_gst) if cost and cost.invoice_price_inc_gst is not None else None,
                    "special_cost_inc_gst": float(cost.special_cost_inc_gst) if cost and cost.special_cost_inc_gst is not None else None,
                    "special_end_date": str(cost.special_end_date) if cost and cost.special_end_date else None,
                    "nett_nett_cost_inc_gst": float(cost.nett_nett_cost_inc_gst) if cost and cost.nett_nett_cost_inc_gst is not None else None,
                    "discount": float(cost.discount) if cost and cost.discount is not None else None,
                    "additional_discount": float(cost.additional_discount) if cost and cost.additional_discount is not None else None,
                    "extra_discount": float(cost.extra_discount) if cost and cost.extra_discount is not None else None,
                    "rebate": float(cost.rebate) if cost and cost.rebate is not None else None,
                    "extra": float(cost.extra) if cost and cost.extra is not None else None,
                    "settlement": float(cost.settlement) if cost and cost.settlement is not None else None,
                    "crf": float(cost.crf) if cost and cost.crf is not None else None,
                    "loyalty": float(cost.loyalty) if cost and cost.loyalty is not None else None,
                    "advertising": float(cost.advertising) if cost and cost.advertising is not None else None,
                    "timed_settlement_fee": float(cost.timed_settlement_fee) if cost and cost.timed_settlement_fee is not None else None,
                    "other": float(cost.other) if cost and cost.other is not None else None,
                } if cost else None,
                "cheapest_competitor": cheapest_name,
                "cheapest_competitor_price": cheapest_price,
                "following_competitors": following,
                "competitor_prices": prices,
                "sales_summary": {
                    "total_units": total_units,
                    "total_revenue": round(total_revenue, 2),
                    "days_with_sales": sum(1 for s in sales_by_date.values() if s["units"] > 0),
                    "period_days": days,
                },
                "history": history,
            },
        }
        set_cached(cache_key, result, 60)
        return result
    except Exception as e:
        log.error(f"Error in /pricing/sku: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
