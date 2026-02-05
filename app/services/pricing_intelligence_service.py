"""
Pricing Intelligence Service - Stage 1 MVP

Quantifies how pricing vs competitors is affecting sales.
Joins Shopify sales data with Caprice competitor pricing and NETT cost data
to identify price-sensitive SKUs, brand-level impact, and unmatchable revenue risk.
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date
from decimal import Decimal
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.models.competitive_pricing import CompetitivePricing
from app.models.product_cost import ProductCost
from app.models.shopify import ShopifyOrderItem, ShopifyInventory, ShopifyProduct
from app.utils.logger import log


# All competitor price column names on CompetitivePricing
COMPETITOR_COLUMNS = [
    ('8appliances', 'price_8appliances'),
    ('appliancesonline', 'price_appliancesonline'),
    ('austpek', 'price_austpek'),
    ('binglee', 'price_binglee'),
    ('blueleafbath', 'price_blueleafbath'),
    ('brandsdirect', 'price_brandsdirect'),
    ('buildmat', 'price_buildmat'),
    ('cookandbathe', 'price_cookandbathe'),
    ('designerbathware', 'price_designerbathware'),
    ('harveynorman', 'price_harveynorman'),
    ('idealbathroom', 'price_idealbathroom'),
    ('justbathroomware', 'price_justbathroomware'),
    ('thebluespace', 'price_thebluespace'),
    ('wellsons', 'price_wellsons'),
    ('winnings', 'price_winnings'),
    ('agcequipment', 'price_agcequipment'),
    ('berloniapp', 'price_berloniapp'),
    ('eands', 'price_eands'),
    ('plumbingsales', 'price_plumbingsales'),
    ('powerland', 'price_powerland'),
    ('saappliances', 'price_saappliances'),
    ('sameday', 'price_sameday'),
    ('shire', 'price_shire'),
    ('vogue', 'price_vogue'),
]


class PricingIntelligenceService:
    """
    Analyses pricing competitiveness and its impact on sales.

    Combines data from:
    - CompetitivePricing (Caprice competitor prices)
    - ProductCost (NETT master: cost, margins, floor price, do_not_follow/set_price flags)
    - ShopifyOrderItem (units sold, revenue)
    - ShopifyInventory (stockout control)
    """

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def get_sku_pricing_sensitivity(
        self,
        days: int = 30,
        decline_threshold: float = 10.0,
        limit: int = 100,
    ) -> Dict:
        """
        SKU Pricing Sensitivity List.

        For every SKU with competitor pricing data, compute the price gap,
        recent sales trend, and flag price-sensitive items.

        Args:
            days: lookback window for sales (also used for prior-period comparison)
            decline_threshold: % unit decline to trigger price_sensitive flag
            limit: max SKUs to return (sorted by price_gap desc)

        Returns:
            Dict with total_skus_analyzed, price_sensitive_count, and skus list.
        """
        try:
            latest_date = self._get_latest_snapshot_date()
            if not latest_date:
                return {'total_skus_analyzed': 0, 'price_sensitive_count': 0, 'skus': [],
                        'error': 'No competitive pricing data available'}

            # Fetch pricing records joined with cost data
            pricing_rows = self._get_pricing_with_costs(latest_date)

            end_date = date.today()
            start_current = end_date - timedelta(days=days)
            start_prior = start_current - timedelta(days=days)

            # Pre-fetch stockout SKUs to exclude
            stockout_skus = self._get_stockout_skus()

            skus: List[Dict] = []
            for cp, pc in pricing_rows:
                sku = cp.variant_sku
                if not sku:
                    continue

                # Exclude stockout SKUs
                if sku.upper() in stockout_skus:
                    continue

                our_price = float(cp.current_price) if cp.current_price else None
                if our_price is None or our_price == 0:
                    continue

                cheapest_name, cheapest_price = self._get_cheapest_competitor(cp)
                if cheapest_price is None:
                    continue

                price_gap = round(our_price - cheapest_price, 2)
                price_gap_pct = round((price_gap / our_price) * 100, 2) if our_price else 0.0

                # Sales: current period
                units_current, rev_current = self._get_units_sold(sku, start_current, end_date)
                # Sales: prior period
                units_prior, _ = self._get_units_sold(sku, start_prior, start_current)

                # Extended windows (60d, 90d)
                units_60d, _ = self._get_units_sold(sku, end_date - timedelta(days=60), end_date)
                units_90d, _ = self._get_units_sold(sku, end_date - timedelta(days=90), end_date)

                # Percentage change
                if units_prior > 0:
                    pct_change = round(((units_current - units_prior) / units_prior) * 100, 1)
                elif units_current > 0:
                    pct_change = 100.0  # new sales, no prior
                else:
                    pct_change = 0.0

                price_sensitive = price_gap > 0 and pct_change < -decline_threshold

                skus.append({
                    'sku': sku,
                    'title': cp.title or '',
                    'vendor': cp.vendor or '',
                    'current_price': our_price,
                    'lowest_competitor_price': cheapest_price,
                    'price_gap': price_gap,
                    'price_gap_pct': price_gap_pct,
                    'units_30d': units_current,
                    'units_60d': units_60d,
                    'units_90d': units_90d,
                    'units_prior_30d': units_prior,
                    'pct_change_30d': pct_change,
                    'revenue_30d': round(rev_current, 2),
                    'price_sensitive': price_sensitive,
                    'cheapest_competitor': cheapest_name,
                })

            # Sort by price_gap descending, then price_sensitive first
            skus.sort(key=lambda x: (not x['price_sensitive'], -x['price_gap']))
            skus = skus[:limit]

            return {
                'total_skus_analyzed': len(skus),
                'price_sensitive_count': sum(1 for s in skus if s['price_sensitive']),
                'analysis_period_days': days,
                'decline_threshold_pct': decline_threshold,
                'pricing_snapshot_date': str(latest_date),
                'skus': skus,
            }
        except Exception as e:
            log.error(f"Error in get_sku_pricing_sensitivity: {str(e)}")
            return {'total_skus_analyzed': 0, 'price_sensitive_count': 0, 'skus': [],
                    'error': str(e)}

    async def get_brand_pricing_impact(
        self,
        days: int = 30,
        decline_threshold: float = 10.0,
    ) -> Dict:
        """
        Brand Pricing Impact summary.

        Aggregates SKU-level sensitivity data by brand.

        Returns:
            Dict with brands list, each containing undercut counts, avg gap,
            revenue at risk, and unit decline percentage.
        """
        try:
            sku_data = await self.get_sku_pricing_sensitivity(
                days=days, decline_threshold=decline_threshold, limit=5000
            )

            if sku_data.get('error') and not sku_data['skus']:
                return {'brands': [], 'error': sku_data.get('error')}

            brand_map: Dict[str, Dict] = defaultdict(lambda: {
                'brand': '',
                'total_skus': 0,
                'undercut_skus': 0,
                'price_gaps': [],
                'units_current': 0,
                'units_prior': 0,
                'revenue_current': 0.0,
                'revenue_at_risk': 0.0,
                'price_sensitive_skus': 0,
            })

            for s in sku_data['skus']:
                brand = s['vendor'] or 'Unknown'
                b = brand_map[brand]
                b['brand'] = brand
                b['total_skus'] += 1

                if s['price_gap'] > 0:
                    b['undercut_skus'] += 1
                    b['price_gaps'].append(s['price_gap'])
                    # Revenue at risk: units * gap
                    b['revenue_at_risk'] += s['units_30d'] * s['price_gap']

                b['units_current'] += s['units_30d']
                b['units_prior'] += s['units_prior_30d']
                b['revenue_current'] += s['revenue_30d']

                if s['price_sensitive']:
                    b['price_sensitive_skus'] += 1

            brands = []
            for b in brand_map.values():
                gaps = b.pop('price_gaps')
                avg_gap = round(sum(gaps) / len(gaps), 2) if gaps else 0.0

                if b['units_prior'] > 0:
                    pct_decline = round(
                        ((b['units_current'] - b['units_prior']) / b['units_prior']) * 100, 1
                    )
                elif b['units_current'] > 0:
                    pct_decline = 100.0
                else:
                    pct_decline = 0.0

                brands.append({
                    **b,
                    'avg_price_gap': avg_gap,
                    'pct_units_decline': pct_decline,
                    'revenue_at_risk': round(b['revenue_at_risk'], 2),
                    'revenue_current': round(b['revenue_current'], 2),
                })

            # Sort by revenue at risk descending
            brands.sort(key=lambda x: -x['revenue_at_risk'])

            return {
                'total_brands': len(brands),
                'analysis_period_days': days,
                'pricing_snapshot_date': sku_data.get('pricing_snapshot_date'),
                'brands': brands,
            }
        except Exception as e:
            log.error(f"Error in get_brand_pricing_impact: {str(e)}")
            return {'brands': [], 'error': str(e)}

    async def get_unmatchable_revenue_risk(self, days: int = 30) -> Dict:
        """
        Unmatchable Revenue Risk.

        Identifies SKUs where the lowest competitor price is below our minimum
        price floor, meaning we cannot match even if we wanted to.

        Returns:
            Dict with total unmatchable SKUs, total revenue at risk, and SKU list.
        """
        try:
            latest_date = self._get_latest_snapshot_date()
            if not latest_date:
                return {'total_unmatchable_skus': 0, 'total_revenue_at_risk': 0.0,
                        'total_orders_affected': 0, 'skus': [],
                        'error': 'No competitive pricing data available'}

            # Query pricing + costs where competitor < our floor
            rows = (
                self.db.query(CompetitivePricing, ProductCost)
                .outerjoin(
                    ProductCost,
                    func.upper(CompetitivePricing.variant_sku) == func.upper(ProductCost.vendor_sku)
                )
                .filter(
                    CompetitivePricing.pricing_date == latest_date,
                    CompetitivePricing.lowest_competitor_price.isnot(None),
                    CompetitivePricing.current_price.isnot(None),
                )
                .all()
            )

            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            stockout_skus = self._get_stockout_skus()

            skus = []
            total_revenue_at_risk = 0.0
            total_orders_affected = 0

            for cp, pc in rows:
                sku = cp.variant_sku
                if not sku or sku.upper() in stockout_skus:
                    continue

                # Skip do_not_follow and set_price SKUs
                if pc and pc.do_not_follow:
                    continue
                if pc and pc.set_price is not None:
                    continue

                our_price = float(cp.current_price)
                # Determine floor: use ProductCost.minimum_price or CompetitivePricing.minimum_price
                floor = None
                if pc and pc.minimum_price:
                    floor = float(pc.minimum_price)
                elif cp.minimum_price:
                    floor = float(cp.minimum_price)

                if floor is None:
                    continue

                cheapest_name, cheapest_price = self._get_cheapest_competitor(cp)
                if cheapest_price is None:
                    continue

                # Only include if competitor is below our floor
                if cheapest_price >= floor:
                    continue

                gap_below_floor = round(floor - cheapest_price, 2)

                units_30d, revenue_30d = self._get_units_sold(sku, start_date, end_date)
                rev_at_risk = round(units_30d * (our_price - cheapest_price), 2)

                total_revenue_at_risk += rev_at_risk
                total_orders_affected += units_30d

                skus.append({
                    'sku': sku,
                    'title': cp.title or '',
                    'vendor': cp.vendor or '',
                    'our_price': our_price,
                    'our_floor': floor,
                    'competitor_price': cheapest_price,
                    'gap_below_floor': gap_below_floor,
                    'units_30d': units_30d,
                    'revenue_30d': round(revenue_30d, 2),
                    'revenue_at_risk': rev_at_risk,
                    'cheapest_competitor': cheapest_name,
                })

            # Sort by revenue at risk descending
            skus.sort(key=lambda x: -x['revenue_at_risk'])

            return {
                'total_unmatchable_skus': len(skus),
                'total_revenue_at_risk': round(total_revenue_at_risk, 2),
                'total_orders_affected': total_orders_affected,
                'analysis_period_days': days,
                'pricing_snapshot_date': str(latest_date),
                'skus': skus,
            }
        except Exception as e:
            log.error(f"Error in get_unmatchable_revenue_risk: {str(e)}")
            return {'total_unmatchable_skus': 0, 'total_revenue_at_risk': 0.0,
                    'total_orders_affected': 0, 'skus': [], 'error': str(e)}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_latest_snapshot_date(self) -> Optional[date]:
        """Get the most recent pricing_date in competitive_pricing."""
        return self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()

    def _get_pricing_with_costs(self, snapshot_date: date) -> List[Tuple]:
        """
        Fetch CompetitivePricing rows for the given date,
        left-joined with ProductCost on SKU.
        Excludes do_not_follow and set_price SKUs.
        """
        rows = (
            self.db.query(CompetitivePricing, ProductCost)
            .outerjoin(
                ProductCost,
                func.upper(CompetitivePricing.variant_sku) == func.upper(ProductCost.vendor_sku)
            )
            .filter(
                CompetitivePricing.pricing_date == snapshot_date,
                CompetitivePricing.current_price.isnot(None),
            )
            .all()
        )

        # Filter out do_not_follow and set_price in Python (simpler with outer join)
        filtered = []
        for cp, pc in rows:
            if pc and pc.do_not_follow:
                continue
            if pc and pc.set_price is not None:
                continue
            filtered.append((cp, pc))

        return filtered

    def _get_cheapest_competitor(self, record: CompetitivePricing) -> Tuple[str, Optional[float]]:
        """
        Find the cheapest competitor from the 24 price_* columns.

        Returns:
            (competitor_name, price) or ('', None) if no competitor data.
        """
        best_name = ''
        best_price = None

        for name, attr in COMPETITOR_COLUMNS:
            val = getattr(record, attr, None)
            if val is not None:
                price = float(val)
                if price > 0 and (best_price is None or price < best_price):
                    best_price = price
                    best_name = name

        return best_name, best_price

    def _get_units_sold(
        self, sku: str, start_date: date, end_date: date
    ) -> Tuple[int, float]:
        """
        Query ShopifyOrderItem for total units and revenue for a SKU in a date range.

        Returns:
            (total_units, total_revenue)
        """
        result = (
            self.db.query(
                func.coalesce(func.sum(ShopifyOrderItem.quantity), 0),
                func.coalesce(func.sum(ShopifyOrderItem.total_price), 0),
            )
            .filter(
                func.upper(ShopifyOrderItem.sku) == sku.upper(),
                ShopifyOrderItem.order_date >= datetime.combine(start_date, datetime.min.time()),
                ShopifyOrderItem.order_date < datetime.combine(end_date, datetime.min.time()),
            )
            .first()
        )

        units = int(result[0]) if result[0] else 0
        revenue = float(result[1]) if result[1] else 0.0
        return units, revenue

    def _get_stockout_skus(self) -> set:
        """
        Return a set of upper-cased SKUs that currently have zero inventory.
        Used to exclude them from decline analysis (stockout ≠ price sensitivity).
        Active products only.
        """
        active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
            ShopifyProduct.status == 'active'
        ).subquery()
        rows = (
            self.db.query(ShopifyInventory.sku)
            .filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.sku.isnot(None),
                ShopifyInventory.inventory_quantity <= 0,
            )
            .all()
        )
        return {r[0].upper() for r in rows if r[0]}
