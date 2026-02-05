"""
Product Profitability Analysis Service

Calculates true product-level profitability after:
- Cost of goods sold (COGS)
- Attributed ad spend
- Returns and refunds
- All marketing costs

Answers: "Which products are actually making you money?"
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.models.product import (
    Product, ProductSale, ProductProfitability, ProductAdSpendAllocation
)
from app.utils.logger import log


class ProfitabilityService:
    """Service for calculating and analyzing product profitability"""

    def __init__(self, db: Session):
        self.db = db

    async def calculate_product_profitability(
        self,
        start_date: datetime,
        end_date: datetime,
        period_type: str = "monthly"
    ) -> List[Dict]:
        """
        Calculate profitability for all products in the period

        Returns the truth: which products make money, which lose money
        """
        log.info(f"Calculating product profitability from {start_date} to {end_date}")

        # Get all products with sales in this period
        products = self._get_products_with_sales(start_date, end_date)

        profitability_results = []

        for product in products:
            profitability = await self._calculate_single_product_profitability(
                product, start_date, end_date, period_type
            )
            profitability_results.append(profitability)

        # Rank by net profit
        profitability_results.sort(key=lambda x: x['net_profit_dollars'], reverse=True)
        for idx, result in enumerate(profitability_results, 1):
            result['profit_rank'] = idx

        # Classify into tiers
        self._classify_profitability_tiers(profitability_results)

        # Save to database
        self._save_profitability_snapshots(profitability_results, start_date, end_date, period_type)

        log.info(f"Calculated profitability for {len(profitability_results)} products")

        return profitability_results

    def _get_products_with_sales(self, start_date: datetime, end_date: datetime) -> List[Product]:
        """Get all products that had sales in the period"""
        return self.db.query(Product).join(ProductSale).filter(
            ProductSale.order_date >= start_date,
            ProductSale.order_date <= end_date
        ).distinct().all()

    async def _calculate_single_product_profitability(
        self,
        product: Product,
        start_date: datetime,
        end_date: datetime,
        period_type: str
    ) -> Dict:
        """Calculate comprehensive profitability for a single product"""

        # Get all sales for this product in the period
        sales = self.db.query(ProductSale).filter(
            ProductSale.product_id == product.id,
            ProductSale.order_date >= start_date,
            ProductSale.order_date <= end_date
        ).all()

        if not sales:
            return self._empty_profitability_result(product)

        # Calculate volume metrics
        units_sold = sum(s.quantity for s in sales)
        units_returned = sum(s.quantity for s in sales if s.was_returned)
        orders_count = len(set(s.order_id for s in sales))

        # Calculate revenue
        total_revenue = sum(s.total_revenue for s in sales)
        total_cogs = sum(s.total_cost for s in sales)
        gross_margin_dollars = total_revenue - total_cogs
        gross_margin_pct = (gross_margin_dollars / total_revenue * 100) if total_revenue > 0 else 0

        # Calculate marketing costs (attributed ad spend)
        attributed_ad_spend = sum(s.attributed_ad_spend for s in sales)
        ad_spend_by_channel = self._calculate_ad_spend_by_channel(sales)

        # Calculate returns impact
        total_refunded = sum(s.refund_amount for s in sales)
        return_rate_pct = (units_returned / units_sold * 100) if units_sold > 0 else 0

        # THE TRUTH: Net profitability after everything
        net_revenue = total_revenue - total_refunded
        net_profit_dollars = gross_margin_dollars - attributed_ad_spend - total_refunded
        net_profit_margin_pct = (net_profit_dollars / total_revenue * 100) if total_revenue > 0 else 0

        # Calculate ROAS (Return on Ad Spend)
        roas = (net_revenue / attributed_ad_spend) if attributed_ad_spend > 0 else None

        # Calculate efficiency metrics
        cpa = (attributed_ad_spend / orders_count) if orders_count > 0 else None

        # Determine profitability status
        is_profitable = net_profit_dollars > 0
        is_losing_money = net_profit_dollars < 0
        is_breakeven = abs(net_profit_dollars) < (total_revenue * 0.05)  # Within 5%

        result = {
            'product_id': product.id,
            'shopify_product_id': product.shopify_product_id,
            'title': product.title,
            'sku': product.sku,

            # Volume
            'units_sold': units_sold,
            'units_returned': units_returned,
            'return_rate_pct': round(return_rate_pct, 2),
            'orders_count': orders_count,

            # Revenue
            'total_revenue': round(total_revenue, 2),
            'average_unit_price': round(total_revenue / units_sold, 2) if units_sold > 0 else 0,

            # Gross margin (before marketing)
            'total_cogs': round(total_cogs, 2),
            'gross_margin_dollars': round(gross_margin_dollars, 2),
            'gross_margin_pct': round(gross_margin_pct, 2),

            # Marketing costs
            'attributed_ad_spend': round(attributed_ad_spend, 2),
            'ad_spend_by_channel': ad_spend_by_channel,

            # Returns impact
            'total_refunded': round(total_refunded, 2),

            # NET PROFITABILITY (The Truth)
            'net_revenue': round(net_revenue, 2),
            'net_profit_dollars': round(net_profit_dollars, 2),
            'net_profit_margin_pct': round(net_profit_margin_pct, 2),
            'roas': round(roas, 2) if roas else None,

            # Efficiency
            'cost_per_acquisition': round(cpa, 2) if cpa else None,

            # Status
            'is_profitable': is_profitable,
            'is_losing_money': is_losing_money,
            'is_breakeven': is_breakeven,

            # Period
            'period_start': start_date,
            'period_end': end_date,
            'period_type': period_type
        }

        return result

    def _calculate_ad_spend_by_channel(self, sales: List[ProductSale]) -> Dict[str, float]:
        """Break down ad spend by channel"""
        by_channel = {}

        for sale in sales:
            if sale.traffic_source and sale.attributed_ad_spend > 0:
                source = sale.traffic_source
                by_channel[source] = by_channel.get(source, 0) + sale.attributed_ad_spend

        return {k: round(v, 2) for k, v in by_channel.items()}

    def _empty_profitability_result(self, product: Product) -> Dict:
        """Return empty result for products with no sales"""
        return {
            'product_id': product.id,
            'shopify_product_id': product.shopify_product_id,
            'title': product.title,
            'sku': product.sku,
            'units_sold': 0,
            'total_revenue': 0,
            'net_profit_dollars': 0,
            'is_profitable': False
        }

    def _classify_profitability_tiers(self, results: List[Dict]):
        """Classify products into profitability tiers"""
        if not results:
            return

        # Calculate quartiles based on net profit
        profits = [r['net_profit_dollars'] for r in results]
        profits_sorted = sorted(profits, reverse=True)

        if len(profits_sorted) >= 4:
            q1 = profits_sorted[len(profits_sorted) // 4]
            q3 = profits_sorted[3 * len(profits_sorted) // 4]
        else:
            q1 = max(profits)
            q3 = min(profits)

        for result in results:
            profit = result['net_profit_dollars']

            if profit > q1:
                result['profitability_tier'] = 'gold'
            elif profit > 0:
                result['profitability_tier'] = 'silver'
            elif profit > q3:
                result['profitability_tier'] = 'bronze'
            else:
                result['profitability_tier'] = 'losing_money'

    def _save_profitability_snapshots(
        self,
        results: List[Dict],
        start_date: datetime,
        end_date: datetime,
        period_type: str
    ):
        """Save profitability snapshots to database"""
        for result in results:
            snapshot = ProductProfitability(
                product_id=result['product_id'],
                period_start=start_date,
                period_end=end_date,
                period_type=period_type,
                units_sold=result['units_sold'],
                orders_count=result.get('orders_count', 0),
                total_revenue=result['total_revenue'],
                average_unit_price=result.get('average_unit_price', 0),
                total_cogs=result.get('total_cogs', 0),
                gross_margin_dollars=result.get('gross_margin_dollars', 0),
                gross_margin_pct=result.get('gross_margin_pct', 0),
                attributed_ad_spend=result.get('attributed_ad_spend', 0),
                ad_spend_by_channel=result.get('ad_spend_by_channel'),
                units_returned=result.get('units_returned', 0),
                return_rate_pct=result.get('return_rate_pct', 0),
                total_refunded=result.get('total_refunded', 0),
                net_revenue=result.get('net_revenue', 0),
                net_profit_dollars=result['net_profit_dollars'],
                net_profit_margin_pct=result.get('net_profit_margin_pct', 0),
                roas=result.get('roas'),
                cost_per_acquisition=result.get('cost_per_acquisition'),
                profit_rank=result.get('profit_rank'),
                profitability_tier=result.get('profitability_tier'),
                is_profitable=result.get('is_profitable', False),
                is_breakeven=result.get('is_breakeven', False),
                is_losing_money=result.get('is_losing_money', False)
            )

            self.db.add(snapshot)

        self.db.commit()

    async def get_profitable_products(
        self,
        start_date: datetime,
        end_date: datetime,
        min_profit: float = 0,
        limit: int = 50
    ) -> List[Dict]:
        """Get most profitable products in period"""
        snapshots = self.db.query(ProductProfitability).filter(
            ProductProfitability.period_start >= start_date,
            ProductProfitability.period_end <= end_date,
            ProductProfitability.net_profit_dollars >= min_profit
        ).order_by(desc(ProductProfitability.net_profit_dollars)).limit(limit).all()

        return [self._snapshot_to_dict(s) for s in snapshots]

    async def get_losing_products(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get products that are LOSING money

        These are the killers - high revenue but negative profit
        after ad spend and returns
        """
        snapshots = self.db.query(ProductProfitability).filter(
            ProductProfitability.period_start >= start_date,
            ProductProfitability.period_end <= end_date,
            ProductProfitability.is_losing_money == True
        ).order_by(ProductProfitability.net_profit_dollars).limit(limit).all()

        return [self._snapshot_to_dict(s) for s in snapshots]

    async def get_hidden_gems(
        self,
        start_date: datetime,
        end_date: datetime,
        min_roas: float = 4.0,
        max_revenue: float = 5000
    ) -> List[Dict]:
        """
        Find "hidden gems" - low revenue but high profitability

        These are products you should push harder
        """
        snapshots = self.db.query(ProductProfitability).filter(
            ProductProfitability.period_start >= start_date,
            ProductProfitability.period_end <= end_date,
            ProductProfitability.total_revenue <= max_revenue,
            ProductProfitability.roas >= min_roas,
            ProductProfitability.is_profitable == True
        ).order_by(desc(ProductProfitability.roas)).all()

        return [self._snapshot_to_dict(s) for s in snapshots]

    async def get_profitability_summary(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict:
        """Get overall profitability summary"""

        snapshots = self.db.query(ProductProfitability).filter(
            ProductProfitability.period_start >= start_date,
            ProductProfitability.period_end <= end_date
        ).all()

        if not snapshots:
            return {
                'total_products': 0,
                'total_revenue': 0,
                'total_profit': 0,
                'message': 'No profitability data available for this period'
            }

        total_revenue = sum(s.total_revenue for s in snapshots)
        total_profit = sum(s.net_profit_dollars for s in snapshots)
        total_ad_spend = sum(s.attributed_ad_spend for s in snapshots)

        profitable_count = sum(1 for s in snapshots if s.is_profitable)
        losing_count = sum(1 for s in snapshots if s.is_losing_money)
        breakeven_count = sum(1 for s in snapshots if s.is_breakeven)

        # Calculate blended ROAS
        blended_roas = (total_revenue / total_ad_spend) if total_ad_spend > 0 else None

        # Find biggest winners and losers
        top_performer = max(snapshots, key=lambda s: s.net_profit_dollars)
        biggest_loser = min(snapshots, key=lambda s: s.net_profit_dollars)

        return {
            'period_start': start_date.isoformat(),
            'period_end': end_date.isoformat(),

            'total_products': len(snapshots),
            'profitable_products': profitable_count,
            'losing_products': losing_count,
            'breakeven_products': breakeven_count,

            'total_revenue': round(total_revenue, 2),
            'total_ad_spend': round(total_ad_spend, 2),
            'total_profit': round(total_profit, 2),
            'profit_margin_pct': round((total_profit / total_revenue * 100) if total_revenue > 0 else 0, 2),
            'blended_roas': round(blended_roas, 2) if blended_roas else None,

            'top_performer': {
                'title': top_performer.product.title,
                'profit': round(top_performer.net_profit_dollars, 2),
                'revenue': round(top_performer.total_revenue, 2)
            } if top_performer else None,

            'biggest_loser': {
                'title': biggest_loser.product.title,
                'loss': round(biggest_loser.net_profit_dollars, 2),
                'revenue': round(biggest_loser.total_revenue, 2)
            } if biggest_loser else None
        }

    def _snapshot_to_dict(self, snapshot: ProductProfitability) -> Dict:
        """Convert snapshot to dictionary"""
        return {
            'product_id': snapshot.product_id,
            'title': snapshot.product.title if snapshot.product else 'Unknown',
            'sku': snapshot.product.sku if snapshot.product else None,

            'units_sold': snapshot.units_sold,
            'revenue': snapshot.total_revenue,
            'cogs': snapshot.total_cogs,
            'gross_margin': snapshot.gross_margin_dollars,
            'gross_margin_pct': snapshot.gross_margin_pct,

            'ad_spend': snapshot.attributed_ad_spend,
            'ad_spend_by_channel': snapshot.ad_spend_by_channel,

            'refunds': snapshot.total_refunded,
            'return_rate': snapshot.return_rate_pct,

            'net_profit': snapshot.net_profit_dollars,
            'net_margin_pct': snapshot.net_profit_margin_pct,
            'roas': snapshot.roas,

            'profitability_tier': snapshot.profitability_tier,
            'profit_rank': snapshot.profit_rank,

            'is_profitable': snapshot.is_profitable,
            'is_losing_money': snapshot.is_losing_money
        }

    async def analyze_profitability_trends(
        self,
        product_id: int,
        lookback_days: int = 90
    ) -> Dict:
        """Analyze profitability trends for a specific product over time"""

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)

        snapshots = self.db.query(ProductProfitability).filter(
            ProductProfitability.product_id == product_id,
            ProductProfitability.period_start >= start_date
        ).order_by(ProductProfitability.period_start).all()

        if not snapshots:
            return {'error': 'No profitability data found for this product'}

        product = self.db.query(Product).filter(Product.id == product_id).first()

        # Calculate trend
        profits = [s.net_profit_dollars for s in snapshots]
        roas_values = [s.roas for s in snapshots if s.roas]

        avg_profit = sum(profits) / len(profits) if profits else 0
        avg_roas = sum(roas_values) / len(roas_values) if roas_values else 0

        # Determine trend direction
        if len(profits) >= 2:
            recent_avg = sum(profits[-3:]) / len(profits[-3:])
            older_avg = sum(profits[:3]) / len(profits[:3]) if len(profits) >= 6 else profits[0]
            trend = 'improving' if recent_avg > older_avg else 'declining'
        else:
            trend = 'stable'

        return {
            'product_id': product_id,
            'title': product.title if product else 'Unknown',
            'lookback_days': lookback_days,

            'snapshots_count': len(snapshots),
            'average_profit': round(avg_profit, 2),
            'average_roas': round(avg_roas, 2) if avg_roas > 0 else None,
            'trend': trend,

            'historical_data': [
                {
                    'date': s.period_start.isoformat(),
                    'profit': round(s.net_profit_dollars, 2),
                    'revenue': round(s.total_revenue, 2),
                    'roas': round(s.roas, 2) if s.roas else None
                }
                for s in snapshots
            ]
        }
