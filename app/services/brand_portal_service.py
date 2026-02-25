"""
Brand Portal Service — Supplier-facing pricing intelligence.

Strict data firewall: queries CompetitivePricing only, never exposes
costs, margins, floor prices, or internal pricing policies.
"""
from datetime import date, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.models.competitive_pricing import CompetitivePricing
from app.services.pricing_intelligence_service import COMPETITOR_COLUMNS
from app.utils.logger import log


class BrandPortalService:

    def __init__(self, db: Session):
        self.db = db

    # ── public ──────────────────────────────────────────────────────

    def get_brands(self) -> List[Dict]:
        """List all brands with SKU counts from the latest pricing snapshot."""
        latest = self._latest_snapshot_date()
        if not latest:
            return []
        rows = (
            self.db.query(
                CompetitivePricing.vendor,
                func.count(CompetitivePricing.id),
            )
            .filter(
                CompetitivePricing.pricing_date == latest,
                CompetitivePricing.vendor.isnot(None),
            )
            .group_by(CompetitivePricing.vendor)
            .order_by(CompetitivePricing.vendor)
            .all()
        )
        return [{"brand": r[0], "sku_count": r[1]} for r in rows]

    def get_brand_overview(self, brand: str) -> Dict:
        """KPIs + category breakdown + competitor activity + monthly trends."""
        latest = self._latest_snapshot_date()
        if not latest:
            return {"kpis": {}, "categories": [], "competitors": [], "monthly_trends": []}

        # Fetch all rows for this brand on latest snapshot
        rows = (
            self.db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.pricing_date == latest,
                func.upper(CompetitivePricing.vendor) == brand.upper(),
            )
            .all()
        )
        if not rows:
            return {"kpis": {}, "categories": [], "competitors": [], "monthly_trends": []}

        # ── KPIs ──
        total_skus = len(rows)
        prices = [float(r.current_price) for r in rows if r.current_price]
        avg_price = round(sum(prices) / len(prices), 2) if prices else 0

        discounts = []
        for r in rows:
            if r.rrp and r.current_price and float(r.rrp) > 0:
                discounts.append(round((1 - float(r.current_price) / float(r.rrp)) * 100, 1))
        avg_discount = round(sum(discounts) / len(discounts), 1) if discounts else 0

        # Unique competitors stocking this brand
        competitors_stocking = set()
        for r in rows:
            for name, col in COMPETITOR_COLUMNS:
                val = getattr(r, col, None)
                if val is not None and float(val) > 0:
                    competitors_stocking.add(name)

        kpis = {
            "total_skus": total_skus,
            "avg_selling_price": avg_price,
            "avg_discount_off_rrp": avg_discount,
            "competitors_stocking": len(competitors_stocking),
            "snapshot_date": str(latest),
        }

        # ── Category breakdown ──
        cat_map = defaultdict(list)
        for r in rows:
            cat = _parse_category(r.title)
            cat_map[cat].append(r)

        categories = []
        for cat, cat_rows in sorted(cat_map.items()):
            cat_prices = [float(r.current_price) for r in cat_rows if r.current_price]
            cat_discounts = []
            cat_competitors = set()
            for r in cat_rows:
                if r.rrp and r.current_price and float(r.rrp) > 0:
                    cat_discounts.append((1 - float(r.current_price) / float(r.rrp)) * 100)
                for name, col in COMPETITOR_COLUMNS:
                    val = getattr(r, col, None)
                    if val is not None and float(val) > 0:
                        cat_competitors.add(name)
            categories.append({
                "category": cat,
                "sku_count": len(cat_rows),
                "avg_price": round(sum(cat_prices) / len(cat_prices), 2) if cat_prices else 0,
                "avg_discount_pct": round(sum(cat_discounts) / len(cat_discounts), 1) if cat_discounts else 0,
                "num_competitors": len(cat_competitors),
            })

        # ── Competitor activity ──
        comp_stats = defaultdict(lambda: {"count": 0, "prices": [], "times_cheapest": 0})
        for r in rows:
            cheapest_price = None
            cheapest_name = None
            for name, col in COMPETITOR_COLUMNS:
                val = getattr(r, col, None)
                if val is None:
                    continue
                price = float(val)
                if price <= 0:
                    continue
                comp_stats[name]["count"] += 1
                comp_stats[name]["prices"].append(price)
                if cheapest_price is None or price < cheapest_price:
                    cheapest_price = price
                    cheapest_name = name
            if cheapest_name:
                comp_stats[cheapest_name]["times_cheapest"] += 1

        competitors = []
        for name, s in sorted(comp_stats.items(), key=lambda x: x[1]["count"], reverse=True):
            avg_comp_discount = 0
            rrp_vals = [float(r.rrp) for r in rows if r.rrp and float(r.rrp) > 0]
            if s["prices"] and rrp_vals:
                # average competitor discount from average RRP
                avg_rrp = sum(rrp_vals) / len(rrp_vals)
                avg_comp_price = sum(s["prices"]) / len(s["prices"])
                if avg_rrp > 0:
                    avg_comp_discount = round((1 - avg_comp_price / avg_rrp) * 100, 1)
            competitors.append({
                "competitor": name,
                "products_stocked": s["count"],
                "avg_price": round(sum(s["prices"]) / len(s["prices"]), 2) if s["prices"] else 0,
                "avg_discount_from_rrp": avg_comp_discount,
                "times_cheapest": s["times_cheapest"],
            })

        # ── Monthly trends (last 12 months) ──
        twelve_months_ago = latest - timedelta(days=365)
        trend_rows = (
            self.db.query(
                func.strftime("%Y-%m", CompetitivePricing.pricing_date).label("month"),
                func.avg(CompetitivePricing.current_price),
                func.avg(CompetitivePricing.discount_off_rrp_pct),
                func.count(CompetitivePricing.id),
            )
            .filter(
                func.upper(CompetitivePricing.vendor) == brand.upper(),
                CompetitivePricing.pricing_date >= twelve_months_ago,
            )
            .group_by("month")
            .order_by("month")
            .all()
        )
        monthly_trends = [
            {
                "month": r[0],
                "avg_selling_price": round(float(r[1] or 0), 2),
                "avg_discount_pct": round(abs(float(r[2] or 0)), 1),
                "snapshot_count": r[3],
            }
            for r in trend_rows
        ]

        return {
            "kpis": kpis,
            "categories": categories,
            "competitors": competitors,
            "monthly_trends": monthly_trends,
        }

    def get_brand_products(
        self,
        brand: str,
        category: Optional[str] = None,
        search: Optional[str] = None,
        page: int = 1,
        per_page: int = 50,
    ) -> Dict:
        """Paginated product list with pricing for one brand."""
        latest = self._latest_snapshot_date()
        if not latest:
            return {"items": [], "total": 0, "page": 1, "pages": 0}

        q = (
            self.db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.pricing_date == latest,
                func.upper(CompetitivePricing.vendor) == brand.upper(),
            )
        )
        if search:
            term = f"%{search}%"
            q = q.filter(
                or_(
                    CompetitivePricing.variant_sku.ilike(term),
                    CompetitivePricing.title.ilike(term),
                )
            )

        # Fetch all for in-Python category filtering (category is derived)
        all_rows = q.order_by(CompetitivePricing.title).all()

        # Category filter (post-query since category is title-derived)
        if category:
            all_rows = [r for r in all_rows if _parse_category(r.title) == category]

        total = len(all_rows)
        pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, pages))
        start = (page - 1) * per_page
        page_rows = all_rows[start:start + per_page]

        items = []
        for r in page_rows:
            comp_prices = self._competitor_prices(r)
            market_low = min(cp["price"] for cp in comp_prices) if comp_prices else None
            cheapest = min(comp_prices, key=lambda x: x["price"])["competitor"] if comp_prices else None
            discount_pct = 0
            if r.rrp and r.current_price and float(r.rrp) > 0:
                discount_pct = round((1 - float(r.current_price) / float(r.rrp)) * 100, 1)
            items.append({
                "sku": r.variant_sku,
                "title": r.title,
                "category": _parse_category(r.title),
                "rrp": float(r.rrp) if r.rrp else None,
                "our_price": float(r.current_price) if r.current_price else None,
                "discount_pct": discount_pct,
                "market_low": round(market_low, 2) if market_low else None,
                "cheapest_competitor": cheapest,
                "num_competitors": len(comp_prices),
            })

        return {"items": items, "total": total, "page": page, "pages": pages}

    def get_sku_detail(self, brand: str, sku: str, days: int = 90) -> Optional[Dict]:
        """Full SKU detail with competitor map + price history."""
        latest = self._latest_snapshot_date()
        if not latest:
            return None

        # Current snapshot — validate brand ownership
        current = (
            self.db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.pricing_date == latest,
                func.upper(CompetitivePricing.variant_sku) == sku.upper(),
            )
            .first()
        )
        if not current:
            return None
        if current.vendor and current.vendor.upper() != brand.upper():
            return None  # Cross-brand protection

        comp_prices = self._competitor_prices(current)
        avg_market = (
            round(sum(cp["price"] for cp in comp_prices) / len(comp_prices), 2)
            if comp_prices else None
        )

        sku_info = {
            "sku": current.variant_sku,
            "title": current.title,
            "vendor": current.vendor,
            "category": _parse_category(current.title),
            "our_price": float(current.current_price) if current.current_price else None,
            "rrp": float(current.rrp) if current.rrp else None,
            "lowest_competitor": round(min(cp["price"] for cp in comp_prices), 2) if comp_prices else None,
            "avg_market_price": avg_market,
            "pricing_date": str(latest),
        }

        # Competitor map
        competitor_map = []
        our_price = float(current.current_price) if current.current_price else 0
        rrp_val = float(current.rrp) if current.rrp else 0
        for cp in sorted(comp_prices, key=lambda x: x["price"]):
            gap_vs_us = round(cp["price"] - our_price, 2) if our_price else None
            gap_vs_rrp = round(cp["price"] - rrp_val, 2) if rrp_val else None
            competitor_map.append({
                "competitor": cp["competitor"],
                "price": cp["price"],
                "gap_vs_us": gap_vs_us,
                "gap_vs_rrp": gap_vs_rrp,
            })

        # Price history
        since = latest - timedelta(days=days)
        history_rows = (
            self.db.query(CompetitivePricing)
            .filter(
                func.upper(CompetitivePricing.variant_sku) == sku.upper(),
                CompetitivePricing.pricing_date >= since,
            )
            .order_by(CompetitivePricing.pricing_date.asc())
            .all()
        )
        price_history = []
        for h in history_rows:
            h_comp = self._competitor_prices(h)
            lowest = min(cp["price"] for cp in h_comp) if h_comp else None
            lowest_name = min(h_comp, key=lambda x: x["price"])["competitor"] if h_comp else None
            price_history.append({
                "date": str(h.pricing_date),
                "our_price": float(h.current_price) if h.current_price else None,
                "rrp": float(h.rrp) if h.rrp else None,
                "lowest_competitor": round(lowest, 2) if lowest else None,
                "lowest_competitor_name": lowest_name,
            })

        return {
            "sku_info": sku_info,
            "competitor_map": competitor_map,
            "price_history": price_history,
        }

    def search_skus(self, brand: str, query: str, limit: int = 20) -> List[Dict]:
        """Search within a brand's products on the latest snapshot."""
        latest = self._latest_snapshot_date()
        if not latest or not query:
            return []

        # Fast path — exact SKU match
        exact = (
            self.db.query(CompetitivePricing)
            .filter(
                CompetitivePricing.pricing_date == latest,
                func.upper(CompetitivePricing.vendor) == brand.upper(),
                func.upper(CompetitivePricing.variant_sku) == query.upper(),
            )
            .first()
        )
        if exact:
            return [{"sku": exact.variant_sku, "title": exact.title}]

        # Fallback — LIKE search
        term = f"%{query}%"
        rows = (
            self.db.query(
                CompetitivePricing.variant_sku,
                CompetitivePricing.title,
            )
            .filter(
                CompetitivePricing.pricing_date == latest,
                func.upper(CompetitivePricing.vendor) == brand.upper(),
                or_(
                    CompetitivePricing.variant_sku.ilike(term),
                    CompetitivePricing.title.ilike(term),
                ),
            )
            .limit(limit)
            .all()
        )
        return [{"sku": r[0], "title": r[1]} for r in rows]

    # ── private ─────────────────────────────────────────────────────

    def _latest_snapshot_date(self) -> Optional[date]:
        return self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()

    def _competitor_prices(self, cp: CompetitivePricing) -> List[Dict]:
        """Extract non-null competitor prices from a row."""
        prices = []
        for name, col in COMPETITOR_COLUMNS:
            val = getattr(cp, col, None)
            if val is None:
                continue
            price = float(val)
            if price > 0:
                prices.append({"competitor": name, "price": round(price, 2)})
        return prices


# ── helpers (module-level, no class) ────────────────────────────────

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
