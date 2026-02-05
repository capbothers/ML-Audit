"""
Shopify Revenue Attribution Service

Matches Shopify orders to Google Ads campaigns and calculates real
revenue and product costs per campaign.

Attribution hierarchy:
1. gad_campaign_id (parsed from landing_site URL) — exact campaign ID match
2. utm_campaign — normalized name match against campaign names
3. No match — caller falls back to Google's reported numbers
"""
import logging
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional
from urllib.parse import unquote

from sqlalchemy import or_, func
from sqlalchemy.orm import Session

from app.models.shopify import ShopifyOrder, ShopifyOrderItem
from app.models.google_ads_data import GoogleAdsCampaign

logger = logging.getLogger(__name__)


class ShopifyRevenueAttributionService:
    """Attributes Shopify orders to Google Ads campaigns."""

    def __init__(self, db: Session):
        self.db = db

    def get_campaign_revenue(
        self,
        campaign_ids: List[str],
        period_start: date,
        period_end: date,
    ) -> Dict[str, Optional[Dict]]:
        """
        Returns revenue, COGS, and order count per campaign_id.

        Returns:
            {campaign_id: {order_count, revenue, product_costs, ...} or None}
            None means no Shopify data — caller should use Google fallback.
        """
        if not campaign_ids:
            return {}

        # Normalize campaign IDs (strip trailing .0 from CSV import format)
        normalized_ids = {self._normalize_campaign_id(cid): cid for cid in campaign_ids}

        # Build campaign name → normalized ID mapping for UTM fallback
        name_to_id = self._build_name_to_id_map(campaign_ids, period_start, period_end)

        # Fetch attributable orders in the period
        period_start_dt = datetime.combine(period_start, datetime.min.time())
        period_end_dt = datetime.combine(period_end, datetime.max.time())

        orders = (
            self.db.query(ShopifyOrder)
            .filter(
                ShopifyOrder.created_at >= period_start_dt,
                ShopifyOrder.created_at <= period_end_dt,
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.financial_status.in_(["paid", "partially_refunded"]),
                or_(
                    ShopifyOrder.gad_campaign_id.isnot(None),
                    ShopifyOrder.utm_campaign.isnot(None),
                ),
            )
            .all()
        )

        logger.info(f"Attribution: {len(orders)} attributable orders in period")

        # Match orders to campaigns
        campaign_orders: Dict[str, List] = defaultdict(list)
        stats = {"gad_matched": 0, "utm_matched": 0, "unmatched": 0}

        for order in orders:
            matched_id = None

            # Tier 1: gad_campaign_id (exact match)
            if order.gad_campaign_id:
                norm_gad = self._normalize_campaign_id(order.gad_campaign_id)
                if norm_gad in normalized_ids:
                    matched_id = normalized_ids[norm_gad]
                    stats["gad_matched"] += 1

            # Tier 2: utm_campaign (name match)
            if matched_id is None and order.utm_campaign:
                norm_name = self._normalize_name(order.utm_campaign)
                if norm_name in name_to_id:
                    matched_id = name_to_id[norm_name]
                    stats["utm_matched"] += 1

            if matched_id:
                campaign_orders[matched_id].append(order)
            else:
                stats["unmatched"] += 1

        logger.info(
            f"Attribution: {stats['gad_matched']} via gad_campaign_id, "
            f"{stats['utm_matched']} via utm_campaign, "
            f"{stats['unmatched']} unmatched"
        )

        # Calculate revenue per campaign
        result: Dict[str, Optional[Dict]] = {}
        for cid in campaign_ids:
            orders_for_campaign = campaign_orders.get(cid, [])
            if not orders_for_campaign:
                result[cid] = None
                continue
            result[cid] = self._calculate_campaign_metrics(orders_for_campaign)

        matched_campaigns = sum(1 for v in result.values() if v is not None)
        logger.info(
            f"Attribution: {matched_campaigns}/{len(campaign_ids)} campaigns "
            f"have Shopify revenue data"
        )

        return result

    def _build_name_to_id_map(
        self, campaign_ids: List[str], period_start: date, period_end: date,
    ) -> Dict[str, str]:
        """Build {normalized_campaign_name: campaign_id} for UTM matching."""
        rows = (
            self.db.query(
                GoogleAdsCampaign.campaign_id,
                GoogleAdsCampaign.campaign_name,
            )
            .filter(
                GoogleAdsCampaign.date >= period_start,
                GoogleAdsCampaign.date <= period_end,
            )
            .distinct()
            .all()
        )

        mapping = {}
        target_ids = set(campaign_ids)
        for r in rows:
            if r.campaign_id in target_ids:
                norm = self._normalize_name(r.campaign_name)
                mapping[norm] = r.campaign_id
        return mapping

    def _calculate_campaign_metrics(self, orders: List[ShopifyOrder]) -> Dict:
        """Calculate revenue, COGS, and product metrics for a set of orders."""
        total_revenue = Decimal("0")
        total_cogs = Decimal("0")
        all_skus = set()
        unprofitable_count = 0

        order_ids = [o.shopify_order_id for o in orders]

        # Batch-load order items for all matched orders
        items_by_order: Dict[int, List] = defaultdict(list)
        if order_ids:
            items = (
                self.db.query(ShopifyOrderItem)
                .filter(ShopifyOrderItem.shopify_order_id.in_(order_ids))
                .all()
            )
            for item in items:
                items_by_order[item.shopify_order_id].append(item)

        for order in orders:
            # Revenue: net of refunds, excluding tax/shipping
            revenue = (
                order.current_subtotal_price
                or order.subtotal_price
                or order.total_price
                or Decimal("0")
            )
            total_revenue += revenue

            # COGS from order items
            for item in items_by_order.get(order.shopify_order_id, []):
                if item.sku:
                    all_skus.add(item.sku)

                if item.cost_per_item is not None and item.quantity:
                    item_cogs = item.cost_per_item * item.quantity
                    total_cogs += item_cogs

                    # Check if this product is unprofitable
                    if item.price and item.cost_per_item > item.price:
                        unprofitable_count += 1

        avg_margin = None
        if total_revenue > 0 and total_cogs > 0:
            avg_margin = round(float((total_revenue - total_cogs) / total_revenue), 4)

        return {
            "order_count": len(orders),
            "revenue": total_revenue,
            "product_costs": total_cogs,
            "products_advertised": len(all_skus),
            "avg_product_margin": avg_margin,
            "unprofitable_products_count": unprofitable_count,
        }

    @staticmethod
    def _normalize_campaign_id(campaign_id: str) -> str:
        """
        Normalize campaign ID format.
        CSV import stores '12345678.0', URLs have '12345678'.
        """
        s = str(campaign_id).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    @staticmethod
    def _normalize_name(name: str) -> str:
        """
        Normalize campaign name for fuzzy matching.
        'Summer Sale - Search' → 'summer sale search'
        'summer+sale+-+search' → 'summer sale search'
        """
        name = unquote(str(name))
        name = name.lower()
        name = name.replace("-", " ").replace("_", " ").replace("+", " ")
        return " ".join(name.split()).strip()
