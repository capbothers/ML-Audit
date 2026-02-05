"""
Chat Data Service - Smart data loading for the chat interface

Queries historical data from the database for comprehensive questions,
and uses quick API sync for real-time questions.
"""
import re
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, extract, and_, or_, case
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
from decimal import Decimal

from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrder, ShopifyProduct, ShopifyCustomer, ShopifyOrderItem, ShopifyRefund, ShopifyInventory
from app.models.search_console_data import SearchConsoleQuery, SearchConsolePage
from app.models.ga4_data import (
    GA4TrafficSource, GA4LandingPage, GA4DailySummary, GA4DeviceBreakdown,
    GA4GeoBreakdown, GA4UserType, GA4PagePerformance, GA4DailyEcommerce, GA4Event
)
from app.models.competitive_pricing import CompetitivePricing
from app.models.product_cost import ProductCost
from app.utils.logger import log


class ChatDataService:
    """Service for loading data intelligently for chat context"""

    def __init__(self):
        self.db: Session = SessionLocal()

    def __del__(self):
        if hasattr(self, 'db') and self.db:
            self.db.close()

    def _date_bounds(self, start_date: date, end_date: date) -> tuple[datetime, datetime]:
        """
        Convert date range to datetime bounds (inclusive start, exclusive end).
        Ensures end_date includes the full day.
        """
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        return start_dt, end_dt

    def get_database_stats(self) -> Dict[str, Any]:
        """Get statistics about what's in the database"""
        try:
            stats = {}

            # Orders count and date range
            order_count = self.db.query(func.count(ShopifyOrder.id)).scalar() or 0
            if order_count > 0:
                min_date = self.db.query(func.min(ShopifyOrder.created_at)).scalar()
                max_date = self.db.query(func.max(ShopifyOrder.created_at)).scalar()
                # Use current_total_price (net after refunds) when available, fallback to total_price (gross)
                revenue_result = self.db.query(
                    func.sum(ShopifyOrder.total_price).label('gross_revenue'),
                    func.sum(
                        case(
                            (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                            else_=ShopifyOrder.total_price
                        )
                    ).label('net_revenue')
                ).filter(
                    ShopifyOrder.financial_status != 'voided',
                    ShopifyOrder.cancelled_at.is_(None)
                ).first()
                stats['orders'] = {
                    'count': order_count,
                    'date_range': f"{min_date.strftime('%Y-%m-%d') if min_date else 'N/A'} to {max_date.strftime('%Y-%m-%d') if max_date else 'N/A'}",
                    'total_revenue': float(revenue_result.net_revenue or 0),
                    'gross_revenue': float(revenue_result.gross_revenue or 0),
                    'refunds_amount': float((revenue_result.gross_revenue or 0) - (revenue_result.net_revenue or 0))
                }
            else:
                stats['orders'] = {'count': 0}

            # Search Console queries
            query_count = self.db.query(func.count(SearchConsoleQuery.id)).scalar() or 0
            if query_count > 0:
                unique_queries = self.db.query(func.count(func.distinct(SearchConsoleQuery.query))).scalar() or 0
                min_date = self.db.query(func.min(SearchConsoleQuery.date)).scalar()
                max_date = self.db.query(func.max(SearchConsoleQuery.date)).scalar()
                months = ((max_date - min_date).days // 30) + 1 if min_date and max_date else 0
                stats['search_console'] = {
                    'total_rows': query_count,
                    'unique_queries': unique_queries,
                    'months': months,
                    'date_range': f"{min_date} to {max_date}" if min_date else 'N/A'
                }
            else:
                stats['search_console'] = {'total_rows': 0}

            # GA4 traffic
            traffic_count = self.db.query(func.count(GA4TrafficSource.id)).scalar() or 0
            if traffic_count > 0:
                min_date = self.db.query(func.min(GA4TrafficSource.date)).scalar()
                max_date = self.db.query(func.max(GA4TrafficSource.date)).scalar()
                months = ((max_date - min_date).days // 30) + 1 if min_date and max_date else 0
                stats['ga4'] = {
                    'total_rows': traffic_count,
                    'months': months,
                    'date_range': f"{min_date} to {max_date}" if min_date else 'N/A'
                }
            else:
                stats['ga4'] = {'total_rows': 0}

            # Products
            product_count = self.db.query(func.count(ShopifyProduct.id)).scalar() or 0
            stats['products'] = {'count': product_count}

            # Customers
            customer_count = self.db.query(func.count(ShopifyCustomer.id)).scalar() or 0
            stats['customers'] = {'count': customer_count}

            return stats

        except Exception as e:
            log.error(f"Error getting database stats: {str(e)}")
            return {'error': str(e)}

    def get_refund_counts(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Get refund counts using Sidekick methodology.

        Returns:
            refunded_orders: COUNT from shopify_orders where financial_status IN (refunded, partially_refunded)
                             Date filter applied to ORDER created_at (Sidekick-style)
            refund_records: COUNT(*) from shopify_refunds (refund events)
            total_refund_amount: SUM(total_refunded) from shopify_refunds
        """
        try:
            date_filter_applied = None

            # ===== REFUNDED ORDERS (Sidekick-style) =====
            # Uses shopify_orders table with order created_at date filter
            orders_query = self.db.query(
                func.count(ShopifyOrder.id).label('refunded_orders'),
                func.sum(ShopifyOrder.total_refunded).label('total_refunded_from_orders')
            ).filter(
                ShopifyOrder.financial_status.in_(['refunded', 'partially_refunded'])
            )

            # Apply date filter to ORDER created_at
            if start_date and end_date:
                start_dt, end_dt = self._date_bounds(start_date, end_date)
                orders_query = orders_query.filter(
                    ShopifyOrder.created_at >= start_dt,
                    ShopifyOrder.created_at < end_dt
                )
                date_filter_applied = f"{start_date} to {end_date}"
            elif start_date:
                start_dt, _ = self._date_bounds(start_date, start_date)
                orders_query = orders_query.filter(ShopifyOrder.created_at >= start_dt)
                date_filter_applied = f"since {start_date}"
            elif end_date:
                _, end_dt = self._date_bounds(end_date, end_date)
                orders_query = orders_query.filter(ShopifyOrder.created_at < end_dt)
                date_filter_applied = f"until {end_date}"

            orders_result = orders_query.first()

            # ===== REFUND RECORDS (from refunds table) =====
            # Total count of refund events (no date filter - shows all refund records)
            refunds_query = self.db.query(
                func.count(ShopifyRefund.id).label('refund_records'),
                func.sum(ShopifyRefund.total_refunded).label('total_refund_amount'),
                func.min(ShopifyRefund.created_at).label('earliest_refund'),
                func.max(ShopifyRefund.created_at).label('latest_refund')
            )
            refunds_result = refunds_query.first()

            return {
                'refunded_orders': orders_result.refunded_orders or 0,
                'refunded_orders_method': 'Sidekick-style: orders with financial_status IN (refunded, partially_refunded), filtered by order created_at',
                'refund_records': refunds_result.refund_records or 0,
                'refund_records_method': 'Total refund events from shopify_refunds table',
                'total_refund_amount': float(refunds_result.total_refund_amount or 0),
                'earliest_refund': refunds_result.earliest_refund.isoformat() if refunds_result.earliest_refund else None,
                'latest_refund': refunds_result.latest_refund.isoformat() if refunds_result.latest_refund else None,
                'date_filter': date_filter_applied,
                'date_filter_applies_to': 'order created_at (for refunded_orders count)'
            }
        except Exception as e:
            log.error(f"Error getting refund counts: {str(e)}")
            return {
                'error': str(e),
                'refunded_orders': 0,
                'refund_records': 0,
                'total_refund_amount': 0
            }

    def get_revenue_by_year(self) -> Dict[int, Dict]:
        """Get revenue breakdown by year"""
        try:
            # Use current_total_price (net after refunds) when available
            net_revenue_expr = case(
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )

            results = self.db.query(
                extract('year', ShopifyOrder.created_at).label('year'),
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(ShopifyOrder.total_price).label('gross_revenue'),
                func.sum(net_revenue_expr).label('net_revenue')
            ).filter(
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).group_by(
                extract('year', ShopifyOrder.created_at)
            ).order_by('year').all()

            return {
                int(r.year): {
                    'orders': r.order_count,
                    'revenue': float(r.net_revenue or 0),
                    'gross_revenue': float(r.gross_revenue or 0)
                } for r in results
            }
        except Exception as e:
            log.error(f"Error getting revenue by year: {str(e)}")
            return {}

    def get_top_products(self, limit: int = 20, start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Dict]:
        """Get top products by revenue, optionally filtered by date range"""
        try:
            # Build base query
            query = self.db.query(ShopifyOrder).filter(
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.line_items.isnot(None)
            )

            # Apply date filter if provided
            if start_date:
                start_dt, _ = self._date_bounds(start_date, start_date)
                query = query.filter(ShopifyOrder.created_at >= start_dt)
            if end_date:
                _, end_dt = self._date_bounds(end_date, end_date)
                query = query.filter(ShopifyOrder.created_at < end_dt)

            orders = query.all()

            # Aggregate by product
            product_sales = {}
            for order in orders:
                if not order.line_items:
                    continue
                for item in order.line_items:
                    title = item.get('title', 'Unknown')
                    sku = item.get('sku', '')
                    key = f"{title}|{sku}"
                    if key not in product_sales:
                        product_sales[key] = {
                            'title': title,
                            'sku': sku,
                            'quantity': 0,
                            'revenue': 0,
                            'orders': 0
                        }
                    product_sales[key]['quantity'] += item.get('quantity', 0)
                    price = float(item.get('price', 0))
                    product_sales[key]['revenue'] += price * item.get('quantity', 0)
                    product_sales[key]['orders'] += 1

            # Sort by revenue and return top N
            sorted_products = sorted(
                product_sales.values(),
                key=lambda x: x['revenue'],
                reverse=True
            )[:limit]

            return sorted_products

        except Exception as e:
            log.error(f"Error getting top products: {str(e)}")
            return []

    def get_top_products_for_period(self, days: int, limit: int = 10) -> Dict:
        """Get top products for last N days with summary"""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        products = self.get_top_products(limit=limit, start_date=start_date, end_date=end_date)
        total_revenue = sum(p['revenue'] for p in products)

        return {
            'period': f"Last {days} days",
            'start_date': str(start_date),
            'end_date': str(end_date),
            'top_products': products,
            'total_top_products_revenue': total_revenue,
            'count': len(products)
        }

    def get_revenue_by_date_range(self, start_date: date, end_date: date) -> Dict:
        """Get detailed revenue breakdown for a specific date range"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            net_revenue_expr = case(
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )

            results = self.db.query(
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(ShopifyOrder.total_price).label('gross_revenue'),
                func.sum(net_revenue_expr).label('net_revenue'),
                func.avg(net_revenue_expr).label('avg_order'),
                func.min(ShopifyOrder.created_at).label('first_order'),
                func.max(ShopifyOrder.created_at).label('last_order')
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).first()

            gross = float(results.gross_revenue or 0)
            net = float(results.net_revenue or 0)

            return {
                'start_date': str(start_date),
                'end_date': str(end_date),
                'orders': results.order_count or 0,
                'revenue': net,
                'gross_revenue': gross,
                'refunds_amount': gross - net,
                'avg_order': float(results.avg_order or 0)
            }
        except Exception as e:
            log.error(f"Error getting revenue by date range: {str(e)}")
            return {'orders': 0, 'revenue': 0}

    def compare_periods(self, period1_start: date, period1_end: date,
                       period2_start: date, period2_end: date,
                       label1: str = "Period 1", label2: str = "Period 2") -> Dict:
        """Compare two date periods (e.g., 2024 vs 2025)"""
        p1 = self.get_revenue_by_date_range(period1_start, period1_end)
        p2 = self.get_revenue_by_date_range(period2_start, period2_end)

        # Calculate changes
        revenue_change = p2['revenue'] - p1['revenue']
        revenue_change_pct = (revenue_change / p1['revenue'] * 100) if p1['revenue'] > 0 else 0
        orders_change = p2['orders'] - p1['orders']
        orders_change_pct = (orders_change / p1['orders'] * 100) if p1['orders'] > 0 else 0

        return {
            label1: p1,
            label2: p2,
            'comparison': {
                'revenue_change': revenue_change,
                'revenue_change_pct': round(revenue_change_pct, 1),
                'orders_change': orders_change,
                'orders_change_pct': round(orders_change_pct, 1),
                'avg_order_change': p2['avg_order'] - p1['avg_order']
            }
        }

    def get_top_customers(self, limit: int = 20) -> List[Dict]:
        """Get top customers by total spent"""
        try:
            results = self.db.query(
                ShopifyOrder.customer_email,
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(ShopifyOrder.total_price).label('total_spent')
            ).filter(
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None),
                ShopifyOrder.customer_email.isnot(None)
            ).group_by(
                ShopifyOrder.customer_email
            ).order_by(
                desc('total_spent')
            ).limit(limit).all()

            return [
                {
                    'email': r.customer_email,
                    'orders': r.order_count,
                    'total_spent': float(r.total_spent or 0)
                } for r in results
            ]
        except Exception as e:
            log.error(f"Error getting top customers: {str(e)}")
            return []

    def get_traffic_sources_summary(self) -> Dict:
        """Get traffic sources summary from GA4"""
        try:
            results = self.db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('total_sessions'),
                func.sum(GA4TrafficSource.total_revenue).label('total_revenue'),
                func.sum(GA4TrafficSource.conversions).label('total_conversions')
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).order_by(
                desc('total_sessions')
            ).limit(20).all()

            return [
                {
                    'source': r.session_source or '(direct)',
                    'medium': r.session_medium or '(none)',
                    'sessions': r.total_sessions or 0,
                    'revenue': float(r.total_revenue or 0),
                    'conversions': r.total_conversions or 0
                } for r in results
            ]
        except Exception as e:
            log.error(f"Error getting traffic sources: {str(e)}")
            return []

    def get_top_search_queries(self, limit: int = 50) -> List[Dict]:
        """Get top search queries from Search Console"""
        try:
            results = self.db.query(
                SearchConsoleQuery.query,
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).group_by(
                SearchConsoleQuery.query
            ).order_by(
                desc('total_clicks')
            ).limit(limit).all()

            return [
                {
                    'query': r.query,
                    'clicks': r.total_clicks or 0,
                    'impressions': r.total_impressions or 0,
                    'avg_position': round(float(r.avg_position or 0), 1)
                } for r in results
            ]
        except Exception as e:
            log.error(f"Error getting top search queries: {str(e)}")
            return []

    def get_brand_terms(self) -> List[str]:
        """Get brand terms to exclude from non-brand query analysis"""
        from app.config import get_settings
        settings = get_settings()
        brand_terms_str = getattr(settings, 'gsc_brand_terms', '')
        if not brand_terms_str:
            return []
        return [term.strip().lower() for term in brand_terms_str.split(',') if term.strip()]

    def get_search_console_queries_filtered(
        self,
        days: int = 28,
        exclude_brand: bool = True,
        limit: int = 10,
        order_by: str = 'clicks'
    ) -> Dict[str, Any]:
        """
        Get Search Console queries with date filtering and brand exclusion.

        Args:
            days: Number of days to look back (default 28)
            exclude_brand: Whether to exclude brand terms (default True)
            limit: Max number of queries to return (default 10)
            order_by: Sort field - 'clicks', 'impressions', or 'ctr' (default 'clicks')

        Returns:
            Dict with queries, metadata, and summary
        """
        try:
            from sqlalchemy import not_, literal

            # Calculate date range
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Build base query
            query = self.db.query(
                SearchConsoleQuery.query.label('query'),
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )

            # Exclude brand terms if requested
            brand_terms = self.get_brand_terms()
            excluded_terms = []
            if exclude_brand and brand_terms:
                for term in brand_terms:
                    query = query.filter(
                        not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                    )
                excluded_terms = brand_terms

            # Group and order
            query = query.group_by(SearchConsoleQuery.query)

            if order_by == 'impressions':
                query = query.order_by(desc('total_impressions'))
            elif order_by == 'ctr':
                # Calculate CTR for ordering
                query = query.order_by(
                    desc(func.sum(SearchConsoleQuery.clicks) / func.nullif(func.sum(SearchConsoleQuery.impressions), 0))
                )
            else:  # default: clicks
                query = query.order_by(desc('total_clicks'))

            results = query.limit(limit).all()

            # Format results with CTR calculation
            queries = []
            for r in results:
                clicks = r.total_clicks or 0
                impressions = r.total_impressions or 0
                ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0

                queries.append({
                    'query': r.query,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'avg_position': round(float(r.avg_position or 0), 1)
                })

            # Get totals for the period
            totals_query = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.count(func.distinct(SearchConsoleQuery.query)).label('unique_queries')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )

            # Apply same brand exclusion to totals
            if exclude_brand and brand_terms:
                for term in brand_terms:
                    totals_query = totals_query.filter(
                        not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                    )

            totals = totals_query.first()

            total_clicks = totals.total_clicks or 0
            total_impressions = totals.total_impressions or 0
            overall_ctr = round((total_clicks / total_impressions * 100), 2) if total_impressions > 0 else 0

            return {
                'period': f'Last {days} days',
                'start_date': str(start_date),
                'end_date': str(end_date),
                'filter': 'non-brand' if exclude_brand else 'all queries',
                'excluded_terms': excluded_terms,
                'order_by': order_by,
                'queries': queries,
                'summary': {
                    'total_clicks': total_clicks,
                    'total_impressions': total_impressions,
                    'overall_ctr': overall_ctr,
                    'unique_queries': totals.unique_queries or 0
                }
            }

        except Exception as e:
            log.error(f"Error getting filtered search queries: {str(e)}")
            return {
                'error': str(e),
                'queries': [],
                'summary': {}
            }

    def get_search_console_pages_filtered(
        self,
        days: int = 28,
        limit: int = 10,
        order_by: str = 'clicks'
    ) -> Dict[str, Any]:
        """
        Get Search Console page performance with date filtering.

        Args:
            days: Number of days to look back (default 28)
            limit: Max number of pages to return (default 10)
            order_by: Sort field - 'clicks', 'impressions', or 'ctr' (default 'clicks')

        Returns:
            Dict with pages, metadata, and summary
        """
        try:
            # Calculate date range
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Build query
            query = self.db.query(
                SearchConsolePage.page.label('page'),
                func.sum(SearchConsolePage.clicks).label('total_clicks'),
                func.sum(SearchConsolePage.impressions).label('total_impressions'),
                func.avg(SearchConsolePage.position).label('avg_position')
            ).filter(
                SearchConsolePage.date >= start_date,
                SearchConsolePage.date <= end_date
            ).group_by(SearchConsolePage.page)

            if order_by == 'impressions':
                query = query.order_by(desc('total_impressions'))
            elif order_by == 'ctr':
                query = query.order_by(
                    desc(func.sum(SearchConsolePage.clicks) / func.nullif(func.sum(SearchConsolePage.impressions), 0))
                )
            else:
                query = query.order_by(desc('total_clicks'))

            results = query.limit(limit).all()

            pages = []
            for r in results:
                clicks = r.total_clicks or 0
                impressions = r.total_impressions or 0
                ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0

                pages.append({
                    'page': r.page,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'avg_position': round(float(r.avg_position or 0), 1)
                })

            return {
                'period': f'Last {days} days',
                'start_date': str(start_date),
                'end_date': str(end_date),
                'order_by': order_by,
                'pages': pages
            }

        except Exception as e:
            log.error(f"Error getting filtered search pages: {str(e)}")
            return {'error': str(e), 'pages': []}

    def get_search_console_stats(self) -> Dict[str, Any]:
        """Get Search Console database statistics"""
        try:
            # Total rows and date range
            stats = self.db.query(
                func.count(SearchConsoleQuery.id).label('total_rows'),
                func.count(func.distinct(SearchConsoleQuery.query)).label('unique_queries'),
                func.min(SearchConsoleQuery.date).label('min_date'),
                func.max(SearchConsoleQuery.date).label('max_date'),
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions')
            ).first()

            if not stats or not stats.total_rows:
                return {'available': False, 'message': 'No Search Console data in database'}

            months = 0
            if stats.min_date and stats.max_date:
                days_diff = (stats.max_date - stats.min_date).days
                months = (days_diff // 30) + 1

            return {
                'available': True,
                'total_rows': stats.total_rows,
                'unique_queries': stats.unique_queries,
                'date_range': f'{stats.min_date} to {stats.max_date}',
                'months_of_data': months,
                'total_clicks': stats.total_clicks or 0,
                'total_impressions': stats.total_impressions or 0
            }

        except Exception as e:
            log.error(f"Error getting Search Console stats: {str(e)}")
            return {'available': False, 'error': str(e)}

    def get_low_ctr_high_impression_queries(
        self,
        days: int = 28,
        ctr_threshold: float = 1.0,
        min_impressions: int = 100,
        exclude_brand: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Find queries with high impressions but low CTR - optimization opportunities.

        Args:
            days: Number of days to look back (default 28)
            ctr_threshold: CTR threshold in percent (default 1.0 = 1%)
            min_impressions: Minimum impressions to qualify (default 100)
            exclude_brand: Whether to exclude brand terms (default True)
            limit: Max number of queries to return (default 10)

        Returns:
            Dict with queries sorted by impressions (highest first)
        """
        try:
            from sqlalchemy import not_

            # Calculate date range
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Build base query - aggregate by query
            query = self.db.query(
                SearchConsoleQuery.query.label('query'),
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )

            # Exclude brand terms if requested
            brand_terms = self.get_brand_terms()
            excluded_terms = []
            if exclude_brand and brand_terms:
                for term in brand_terms:
                    query = query.filter(
                        not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                    )
                excluded_terms = brand_terms

            # Group by query
            query = query.group_by(SearchConsoleQuery.query)

            # Filter for low CTR and high impressions using HAVING clause
            # CTR < threshold AND impressions >= min_impressions
            query = query.having(
                and_(
                    func.sum(SearchConsoleQuery.impressions) >= min_impressions,
                    (func.sum(SearchConsoleQuery.clicks) * 100.0 /
                     func.nullif(func.sum(SearchConsoleQuery.impressions), 0)) < ctr_threshold
                )
            )

            # Order by impressions descending (highest visibility first)
            query = query.order_by(desc('total_impressions'))
            results = query.limit(limit).all()

            # Format results
            queries = []
            for r in results:
                clicks = r.total_clicks or 0
                impressions = r.total_impressions or 0
                ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0

                queries.append({
                    'query': r.query,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'avg_position': round(float(r.avg_position or 0), 1),
                    'opportunity': f"High visibility ({impressions:,} impressions) but only {ctr}% CTR"
                })

            return {
                'period': f'Last {days} days',
                'start_date': str(start_date),
                'end_date': str(end_date),
                'filter': f'CTR < {ctr_threshold}%, impressions >= {min_impressions}',
                'excluded_terms': excluded_terms,
                'queries': queries,
                'analysis': {
                    'total_queries_found': len(queries),
                    'ctr_threshold': ctr_threshold,
                    'min_impressions': min_impressions,
                    'recommendation': 'These queries have visibility but poor click-through. Consider improving titles/meta descriptions.'
                }
            }

        except Exception as e:
            log.error(f"Error getting low CTR queries: {str(e)}")
            return {'error': str(e), 'queries': []}

    def get_search_console_queries_brand_only(
        self,
        days: int = 28,
        limit: int = 10,
        order_by: str = 'clicks'
    ) -> Dict[str, Any]:
        """
        Get Search Console queries that MATCH brand terms only.

        Args:
            days: Number of days to look back (default 28)
            limit: Max number of queries to return (default 10)
            order_by: Sort field - 'clicks', 'impressions', or 'ctr' (default 'clicks')

        Returns:
            Dict with brand queries, metadata, and summary
        """
        try:
            from sqlalchemy import or_

            # Calculate date range
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            brand_terms = self.get_brand_terms()
            if not brand_terms:
                return {
                    'error': 'No brand terms configured. Set GSC_BRAND_TERMS in .env',
                    'queries': []
                }

            # Build base query
            query = self.db.query(
                SearchConsoleQuery.query.label('query'),
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )

            # Filter for queries MATCHING any brand term
            brand_filters = [
                func.lower(SearchConsoleQuery.query).like(f'%{term}%')
                for term in brand_terms
            ]
            query = query.filter(or_(*brand_filters))

            # Group and order
            query = query.group_by(SearchConsoleQuery.query)

            if order_by == 'impressions':
                query = query.order_by(desc('total_impressions'))
            elif order_by == 'ctr':
                query = query.order_by(
                    desc(func.sum(SearchConsoleQuery.clicks) / func.nullif(func.sum(SearchConsoleQuery.impressions), 0))
                )
            else:  # default: clicks
                query = query.order_by(desc('total_clicks'))

            results = query.limit(limit).all()

            # Format results
            queries = []
            for r in results:
                clicks = r.total_clicks or 0
                impressions = r.total_impressions or 0
                ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0

                queries.append({
                    'query': r.query,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'avg_position': round(float(r.avg_position or 0), 1)
                })

            # Get totals for brand queries
            totals_query = self.db.query(
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.count(func.distinct(SearchConsoleQuery.query)).label('unique_queries')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )
            totals_query = totals_query.filter(or_(*brand_filters))
            totals = totals_query.first()

            total_clicks = totals.total_clicks or 0
            total_impressions = totals.total_impressions or 0
            overall_ctr = round((total_clicks / total_impressions * 100), 2) if total_impressions > 0 else 0

            return {
                'period': f'Last {days} days',
                'start_date': str(start_date),
                'end_date': str(end_date),
                'filter': 'brand queries only',
                'brand_terms': brand_terms,
                'order_by': order_by,
                'queries': queries,
                'summary': {
                    'total_clicks': total_clicks,
                    'total_impressions': total_impressions,
                    'overall_ctr': overall_ctr,
                    'unique_brand_queries': totals.unique_queries or 0
                }
            }

        except Exception as e:
            log.error(f"Error getting brand queries: {str(e)}")
            return {'error': str(e), 'queries': []}

    def get_search_console_queries_opportunities(
        self,
        days: int = 28,
        min_impressions: int = 1000,
        pos_min: float = 8.0,
        pos_max: float = 15.0,
        exclude_brand: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Find queries ranking positions 8-15 with high impressions - page 2 opportunities.

        These are keywords where you're close to page 1 but not quite there,
        and have good search volume. Small ranking improvements = big traffic gains.

        Args:
            days: Number of days to look back (default 28)
            min_impressions: Minimum impressions to qualify (default 1000)
            pos_min: Minimum position (default 8.0)
            pos_max: Maximum position (default 15.0)
            exclude_brand: Whether to exclude brand terms (default True)
            limit: Max number of queries to return (default 10)

        Returns:
            Dict with opportunity queries sorted by impressions (highest potential first)
        """
        try:
            from sqlalchemy import not_, and_

            # Calculate date range
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Build base query
            query = self.db.query(
                SearchConsoleQuery.query.label('query'),
                func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                func.avg(SearchConsoleQuery.position).label('avg_position')
            ).filter(
                SearchConsoleQuery.date >= start_date,
                SearchConsoleQuery.date <= end_date
            )

            # Exclude brand terms if requested
            brand_terms = self.get_brand_terms()
            excluded_terms = []
            if exclude_brand and brand_terms:
                for term in brand_terms:
                    query = query.filter(
                        not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                    )
                excluded_terms = brand_terms

            # Group by query
            query = query.group_by(SearchConsoleQuery.query)

            # Filter for position range and minimum impressions using HAVING
            query = query.having(
                and_(
                    func.avg(SearchConsoleQuery.position) >= pos_min,
                    func.avg(SearchConsoleQuery.position) <= pos_max,
                    func.sum(SearchConsoleQuery.impressions) >= min_impressions
                )
            )

            # Order by impressions descending (highest potential first)
            query = query.order_by(desc('total_impressions'))
            results = query.limit(limit).all()

            # Format results - NO potential/forecast calculations
            # LLM should report ONLY actual measured data
            queries = []
            for r in results:
                clicks = r.total_clicks or 0
                impressions = r.total_impressions or 0
                ctr = round((clicks / impressions * 100), 2) if impressions > 0 else 0
                position = round(float(r.avg_position or 0), 1)

                queries.append({
                    'query': r.query,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'avg_position': position
                    # NOTE: No "potential_clicks" - LLM must NOT forecast/estimate
                })

            return {
                'period': f'Last {days} days',
                'start_date': str(start_date),
                'end_date': str(end_date),
                'filter': f'Positions {pos_min}-{pos_max}, impressions >= {min_impressions}',
                'excluded_terms': excluded_terms,
                'queries': queries,
                'analysis': {
                    'total_opportunities': len(queries),
                    'position_range': f'{pos_min} to {pos_max}',
                    'min_impressions': min_impressions
                },
                'IMPORTANT': 'Report ONLY the actual metrics (clicks, impressions, ctr, position). Do NOT calculate or estimate potential clicks or traffic gains.'
            }

        except Exception as e:
            log.error(f"Error getting opportunity queries: {str(e)}")
            return {'error': str(e), 'queries': []}

    def get_search_console_pages_week_over_week(
        self,
        current_days: int = 7,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Compare Search Console page metrics between current period and previous period.

        Args:
            current_days: Days in each period (default 7 for week-over-week)
            limit: Number of pages to show in top movers

        Returns:
            Dict with period comparison and top movers (click gainers/losers)
        """
        try:
            # Calculate date ranges
            current_end = date.today()
            current_start = current_end - timedelta(days=current_days)
            previous_end = current_start - timedelta(days=1)
            previous_start = previous_end - timedelta(days=current_days)

            def get_period_data(start_dt: date, end_dt: date) -> Dict[str, Dict]:
                """Get aggregated page data for a period"""
                results = self.db.query(
                    SearchConsolePage.page.label('page'),
                    func.sum(SearchConsolePage.clicks).label('total_clicks'),
                    func.sum(SearchConsolePage.impressions).label('total_impressions'),
                    func.avg(SearchConsolePage.position).label('avg_position')
                ).filter(
                    SearchConsolePage.date >= start_dt,
                    SearchConsolePage.date <= end_dt
                ).group_by(SearchConsolePage.page).all()

                return {
                    r.page: {
                        'clicks': r.total_clicks or 0,
                        'impressions': r.total_impressions or 0,
                        'ctr': round((r.total_clicks / r.total_impressions * 100), 2) if r.total_impressions else 0,
                        'position': round(float(r.avg_position or 0), 1)
                    }
                    for r in results
                }

            def get_period_totals(start_dt: date, end_dt: date) -> Dict:
                """Get totals for a period"""
                result = self.db.query(
                    func.sum(SearchConsolePage.clicks).label('total_clicks'),
                    func.sum(SearchConsolePage.impressions).label('total_impressions'),
                    func.count(func.distinct(SearchConsolePage.page)).label('unique_pages')
                ).filter(
                    SearchConsolePage.date >= start_dt,
                    SearchConsolePage.date <= end_dt
                ).first()

                clicks = result.total_clicks or 0
                impressions = result.total_impressions or 0
                return {
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': round((clicks / impressions * 100), 2) if impressions > 0 else 0,
                    'unique_pages': result.unique_pages or 0
                }

            # Get data for both periods
            current_data = get_period_data(current_start, current_end)
            previous_data = get_period_data(previous_start, previous_end)
            current_totals = get_period_totals(current_start, current_end)
            previous_totals = get_period_totals(previous_start, previous_end)

            # Calculate changes for all pages
            all_pages = set(current_data.keys()) | set(previous_data.keys())
            changes = []

            for page in all_pages:
                curr = current_data.get(page, {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0})
                prev = previous_data.get(page, {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0})

                click_change = curr['clicks'] - prev['clicks']
                impression_change = curr['impressions'] - prev['impressions']

                # Only include pages that had meaningful traffic in at least one period
                if prev['clicks'] >= 5 or curr['clicks'] >= 5:
                    changes.append({
                        'page': page,
                        'current_clicks': curr['clicks'],
                        'previous_clicks': prev['clicks'],
                        'click_change': click_change,
                        'click_change_pct': round((click_change / prev['clicks'] * 100), 1) if prev['clicks'] > 0 else 0,
                        'current_impressions': curr['impressions'],
                        'previous_impressions': prev['impressions'],
                        'impression_change': impression_change,
                        'current_position': curr['position'],
                        'previous_position': prev['position']
                    })

            # Sort by click change
            click_gainers = sorted([c for c in changes if c['click_change'] > 0],
                                   key=lambda x: x['click_change'], reverse=True)[:limit]
            click_losers = sorted([c for c in changes if c['click_change'] < 0],
                                  key=lambda x: x['click_change'])[:limit]

            # Calculate overall changes
            overall_click_change = current_totals['clicks'] - previous_totals['clicks']
            overall_click_change_pct = round((overall_click_change / previous_totals['clicks'] * 100), 1) if previous_totals['clicks'] > 0 else 0

            return {
                'current_period': {
                    'start': str(current_start),
                    'end': str(current_end),
                    'label': f'Last {current_days} days',
                    'totals': current_totals
                },
                'previous_period': {
                    'start': str(previous_start),
                    'end': str(previous_end),
                    'label': f'Prior {current_days} days',
                    'totals': previous_totals
                },
                'overall_changes': {
                    'click_change': overall_click_change,
                    'click_change_pct': overall_click_change_pct,
                    'impression_change': current_totals['impressions'] - previous_totals['impressions']
                },
                'click_gainers': click_gainers,
                'click_losers': click_losers,
                'analysis': {
                    'summary': f"Page clicks {'up' if overall_click_change > 0 else 'down'} {abs(overall_click_change_pct):.1f}% ({overall_click_change:+d} clicks)."
                }
            }

        except Exception as e:
            log.error(f"Error getting pages week-over-week: {str(e)}")
            return {'error': str(e)}

    def get_search_console_week_over_week(
        self,
        current_days: int = 7,
        exclude_brand: bool = True,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Compare Search Console metrics between current period and previous period.

        Args:
            current_days: Days in each period (default 7 for week-over-week)
            exclude_brand: Whether to exclude brand terms (default True)
            limit: Number of queries to show in top movers

        Returns:
            Dict with period comparison and top movers (gainers/losers)
        """
        try:
            from sqlalchemy import not_

            # Calculate date ranges
            current_end = date.today()
            current_start = current_end - timedelta(days=current_days)
            previous_end = current_start - timedelta(days=1)
            previous_start = previous_end - timedelta(days=current_days)

            brand_terms = self.get_brand_terms()

            def get_period_data(start_dt: date, end_dt: date) -> Dict[str, Dict]:
                """Get aggregated data for a period"""
                query = self.db.query(
                    SearchConsoleQuery.query.label('query'),
                    func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                    func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                    func.avg(SearchConsoleQuery.position).label('avg_position')
                ).filter(
                    SearchConsoleQuery.date >= start_dt,
                    SearchConsoleQuery.date <= end_dt
                )

                if exclude_brand and brand_terms:
                    for term in brand_terms:
                        query = query.filter(
                            not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                        )

                results = query.group_by(SearchConsoleQuery.query).all()

                return {
                    r.query: {
                        'clicks': r.total_clicks or 0,
                        'impressions': r.total_impressions or 0,
                        'ctr': round((r.total_clicks / r.total_impressions * 100), 2) if r.total_impressions else 0,
                        'position': round(float(r.avg_position or 0), 1)
                    }
                    for r in results
                }

            def get_period_totals(start_dt: date, end_dt: date) -> Dict:
                """Get totals for a period"""
                query = self.db.query(
                    func.sum(SearchConsoleQuery.clicks).label('total_clicks'),
                    func.sum(SearchConsoleQuery.impressions).label('total_impressions'),
                    func.count(func.distinct(SearchConsoleQuery.query)).label('unique_queries')
                ).filter(
                    SearchConsoleQuery.date >= start_dt,
                    SearchConsoleQuery.date <= end_dt
                )

                if exclude_brand and brand_terms:
                    for term in brand_terms:
                        query = query.filter(
                            not_(func.lower(SearchConsoleQuery.query).like(f'%{term}%'))
                        )

                result = query.first()
                clicks = result.total_clicks or 0
                impressions = result.total_impressions or 0
                return {
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': round((clicks / impressions * 100), 2) if impressions > 0 else 0,
                    'unique_queries': result.unique_queries or 0
                }

            # Get data for both periods
            current_data = get_period_data(current_start, current_end)
            previous_data = get_period_data(previous_start, previous_end)
            current_totals = get_period_totals(current_start, current_end)
            previous_totals = get_period_totals(previous_start, previous_end)

            # Calculate changes for all queries
            all_queries = set(current_data.keys()) | set(previous_data.keys())
            changes = []

            for q in all_queries:
                curr = current_data.get(q, {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0})
                prev = previous_data.get(q, {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0})

                click_change = curr['clicks'] - prev['clicks']
                ctr_change = curr['ctr'] - prev['ctr']
                impression_change = curr['impressions'] - prev['impressions']

                changes.append({
                    'query': q,
                    'current_clicks': curr['clicks'],
                    'previous_clicks': prev['clicks'],
                    'click_change': click_change,
                    'current_ctr': curr['ctr'],
                    'previous_ctr': prev['ctr'],
                    'ctr_change': round(ctr_change, 2),
                    'current_impressions': curr['impressions'],
                    'previous_impressions': prev['impressions'],
                    'impression_change': impression_change,
                    'current_position': curr['position'],
                    'previous_position': prev['position']
                })

            # Filter for MEANINGFUL CTR changes:
            # - Must have had impressions in BOTH periods (min 50 each)
            # - Must have had at least 2 clicks in previous period (so CTR is meaningful)
            # - Must have previous_ctr > 0 (excludes new queries that had no CTR before)
            # This prevents "0 clicks -> 1 click = +100% CTR" noise
            meaningful_for_ctr = [
                c for c in changes
                if c['previous_impressions'] >= 50
                and c['current_impressions'] >= 50
                and c['previous_clicks'] >= 2
                and c['previous_ctr'] > 0
            ]

            # Sort by CTR change to find biggest movers (only meaningful ones)
            ctr_gainers = sorted([c for c in meaningful_for_ctr if c['ctr_change'] > 0],
                                  key=lambda x: x['ctr_change'], reverse=True)[:limit]
            ctr_losers = sorted([c for c in meaningful_for_ctr if c['ctr_change'] < 0],
                                 key=lambda x: x['ctr_change'])[:limit]

            # Sort by click change (use all changes - clicks are meaningful regardless)
            click_gainers = sorted([c for c in changes if c['click_change'] > 0],
                                    key=lambda x: x['click_change'], reverse=True)[:limit]
            click_losers = sorted([c for c in changes if c['click_change'] < 0],
                                   key=lambda x: x['click_change'])[:limit]

            # Calculate overall changes
            overall_click_change = current_totals['clicks'] - previous_totals['clicks']
            overall_click_change_pct = round((overall_click_change / previous_totals['clicks'] * 100), 1) if previous_totals['clicks'] > 0 else 0
            overall_ctr_change = current_totals['ctr'] - previous_totals['ctr']

            return {
                'current_period': {
                    'start': str(current_start),
                    'end': str(current_end),
                    'label': f'Last {current_days} days',
                    'totals': current_totals
                },
                'previous_period': {
                    'start': str(previous_start),
                    'end': str(previous_end),
                    'label': f'Prior {current_days} days',
                    'totals': previous_totals
                },
                'overall_changes': {
                    'click_change': overall_click_change,
                    'click_change_pct': overall_click_change_pct,
                    'ctr_change': round(overall_ctr_change, 2),
                    'impression_change': current_totals['impressions'] - previous_totals['impressions']
                },
                'ctr_gainers': ctr_gainers,
                'ctr_losers': ctr_losers,
                'click_gainers': click_gainers,
                'click_losers': click_losers,
                'excluded_terms': brand_terms if exclude_brand else [],
                'analysis': {
                    'summary': f"CTR {'improved' if overall_ctr_change > 0 else 'declined'} by {abs(overall_ctr_change):.2f}% points. "
                               f"Clicks {'up' if overall_click_change > 0 else 'down'} {abs(overall_click_change_pct):.1f}%."
                }
            }

        except Exception as e:
            log.error(f"Error getting week-over-week comparison: {str(e)}")
            return {'error': str(e)}

    def get_monthly_trends(self, months: int = 12) -> List[Dict]:
        """Get monthly revenue trends"""
        try:
            cutoff_date = datetime.now() - timedelta(days=months * 30)

            # Use current_total_price (net after refunds) when available
            net_revenue_expr = case(
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )

            results = self.db.query(
                extract('year', ShopifyOrder.created_at).label('year'),
                extract('month', ShopifyOrder.created_at).label('month'),
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(net_revenue_expr).label('revenue')
            ).filter(
                ShopifyOrder.created_at >= cutoff_date,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).group_by(
                extract('year', ShopifyOrder.created_at),
                extract('month', ShopifyOrder.created_at)
            ).order_by('year', 'month').all()

            return [
                {
                    'year': int(r.year),
                    'month': int(r.month),
                    'orders': r.order_count,
                    'revenue': float(r.revenue or 0)
                } for r in results
            ]
        except Exception as e:
            log.error(f"Error getting monthly trends: {str(e)}")
            return []

    def get_orders_for_period(self, start_date: date, end_date: date) -> Dict:
        """Get orders summary for a specific period"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            # Use current_total_price (net after refunds) when available
            net_revenue_expr = case(
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )

            results = self.db.query(
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(ShopifyOrder.total_price).label('gross_revenue'),
                func.sum(net_revenue_expr).label('net_revenue'),
                func.avg(net_revenue_expr).label('avg_order')
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).first()

            return {
                'orders': results.order_count or 0,
                'revenue': float(results.net_revenue or 0),
                'gross_revenue': float(results.gross_revenue or 0),
                'avg_order': float(results.avg_order or 0),
                'period': f"{start_date} to {end_date}"
            }
        except Exception as e:
            log.error(f"Error getting orders for period: {str(e)}")
            return {'orders': 0, 'revenue': 0, 'avg_order': 0}

    def get_orders_last_n_days(self, days: int) -> Dict:
        """Get orders summary for last N days from DATABASE"""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        start_dt, end_dt = self._date_bounds(start_date, end_date)

        try:
            # Use current_total_price (net after refunds) when available, fallback to total_price (gross)
            net_revenue_expr = case(
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )

            results = self.db.query(
                func.count(ShopifyOrder.id).label('order_count'),
                func.sum(ShopifyOrder.total_price).label('gross_revenue'),
                func.sum(net_revenue_expr).label('net_revenue'),
                func.avg(net_revenue_expr).label('avg_order'),
                func.min(ShopifyOrder.created_at).label('first_order'),
                func.max(ShopifyOrder.created_at).label('last_order')
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).first()

            # Get daily breakdown
            daily = self.db.query(
                func.date(ShopifyOrder.created_at).label('day'),
                func.count(ShopifyOrder.id).label('orders'),
                func.sum(net_revenue_expr).label('revenue')
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).group_by(
                func.date(ShopifyOrder.created_at)
            ).order_by('day').all()

            gross = float(results.gross_revenue or 0)
            net = float(results.net_revenue or 0)

            return {
                'period': f"Last {days} days",
                'start_date': str(start_date),
                'end_date': str(end_date),
                'orders': results.order_count or 0,
                'revenue': net,  # Net revenue (after refunds) - main number
                'gross_revenue': gross,  # Original order totals
                'refunds_amount': gross - net,  # Difference shows refunds
                'avg_order': float(results.avg_order or 0),
                'daily_breakdown': [
                    {'date': str(d.day), 'orders': d.orders, 'revenue': float(d.revenue or 0)}
                    for d in daily
                ]
            }
        except Exception as e:
            log.error(f"Error getting orders for last {days} days: {str(e)}")
            return {'orders': 0, 'revenue': 0, 'avg_order': 0, 'period': f"Last {days} days"}

    def get_sales_by_channel(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Sales by Shopify channel/source_name"""
        try:
            # Clamp to available data range
            max_date = self.db.query(func.max(ShopifyOrder.created_at)).scalar()
            if max_date:
                max_date = max_date.date()
                if end_date > max_date:
                    end_date = max_date
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            total_sales_expr = case(
                (ShopifyOrder.current_subtotal_price.isnot(None), ShopifyOrder.current_subtotal_price),
                (ShopifyOrder.current_total_price.isnot(None), ShopifyOrder.current_total_price),
                else_=ShopifyOrder.total_price
            )
            rows = self.db.query(
                ShopifyOrder.source_name,
                func.count(ShopifyOrder.id).label("orders"),
                func.sum(total_sales_expr).label("revenue")
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).group_by(ShopifyOrder.source_name).order_by(func.sum(total_sales_expr).desc()).all()

            channel_map = {
                "web": "Online Store",
                "shopify_draft_order": "Draft Orders",
            }

            total_orders = sum(r.orders or 0 for r in rows)
            total_revenue = sum(float(r.revenue or 0) for r in rows)

            return {
                "period": f"{start_date} to {end_date}",
                "channels": [
                    {
                        "channel": channel_map.get(r.source_name, f"App {r.source_name}" if (r.source_name or "").isdigit() else (r.source_name or "unknown")),
                        "orders": r.orders or 0,
                        "revenue": float(r.revenue or 0)
                    } for r in rows
                ],
                "total_orders": total_orders,
                "total_revenue": total_revenue,
                "data_end": str(end_date),
                "sales_basis": "current_subtotal_price if available, else current_total_price"
            }
        except Exception as e:
            log.error(f"Error getting sales by channel: {str(e)}")
            return {"error": str(e), "channels": []}

    def get_order_status_summary(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Order fulfillment + cancellation summary"""
        try:
            # Clamp to available data range
            max_date = self.db.query(func.max(ShopifyOrder.created_at)).scalar()
            if max_date:
                max_date = max_date.date()
                if end_date > max_date:
                    end_date = max_date
            start_dt, end_dt = self._date_bounds(start_date, end_date)

            base = self.db.query(ShopifyOrder).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt
            )
            total_orders = base.count()

            # Sidekick-style: Shopify often leaves unfulfilled as NULL
            cancelled = base.filter(ShopifyOrder.fulfillment_status == 'cancelled').count()
            fulfilled = base.filter(ShopifyOrder.fulfillment_status == 'fulfilled').count()
            partial = base.filter(ShopifyOrder.fulfillment_status == 'partial').count()
            unfulfilled_sidekick = base.filter(ShopifyOrder.fulfillment_status.is_(None)).count()

            eligible_total = total_orders - cancelled
            fulfillment_rate = (fulfilled / eligible_total * 100) if eligible_total > 0 else 0

            # Operational backlog: include NULL fulfillment_status
            unfulfilled_operational = base.filter(
                or_(
                    ShopifyOrder.fulfillment_status.is_(None),
                    ShopifyOrder.fulfillment_status == 'unfulfilled'
                )
            ).count()

            return {
                "period": f"{start_date} to {end_date}",
                "data_end": str(end_date),
                "total_orders": total_orders,
                "eligible_orders": eligible_total,
                "cancelled": cancelled,
                "fulfilled": fulfilled,
                "partial": partial,
                "unfulfilled": unfulfilled_sidekick,
                "unfulfilled_operational": unfulfilled_operational,
                "fulfillment_rate": round(fulfillment_rate, 2),
                "context": "Operational view includes all orders in period for fulfillment rate context."
            }
        except Exception as e:
            log.error(f"Error getting order status summary: {str(e)}")
            return {"error": str(e)}

    def get_discount_summary(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Discounted orders and discount code usage"""
        try:
            max_date = self.db.query(func.max(ShopifyOrder.created_at)).scalar()
            if max_date:
                max_date = max_date.date()
                if end_date > max_date:
                    end_date = max_date
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            discounted_orders = self.db.query(ShopifyOrder).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.total_discounts > 0
            ).all()

            total_discounted_orders = len(discounted_orders)
            total_discount_amount = sum(float(o.total_discounts or 0) for o in discounted_orders)
            total_discount_revenue = sum(float(o.total_price or 0) for o in discounted_orders)

            code_counts = {}
            for order in discounted_orders:
                if not order.discount_codes:
                    continue
                for code in order.discount_codes:
                    code_name = code.get('code') if isinstance(code, dict) else str(code)
                    if not code_name:
                        continue
                    code_counts[code_name] = code_counts.get(code_name, 0) + 1

            top_codes = sorted(
                [{"code": k, "orders": v} for k, v in code_counts.items()],
                key=lambda x: x["orders"],
                reverse=True
            )[:10]

            return {
                "period": f"{start_date} to {end_date}",
                "discounted_orders": total_discounted_orders,
                "discount_revenue": round(total_discount_revenue, 2),
                "total_discount_amount": round(total_discount_amount, 2),
                "top_codes": top_codes,
                "data_end": str(end_date)
            }
        except Exception as e:
            log.error(f"Error getting discount summary: {str(e)}")
            return {"error": str(e)}

    def get_shipping_tax_trends(self, days: int = 30) -> Dict[str, Any]:
        """Shipping and tax collections over time"""
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            max_date = self.db.query(func.max(ShopifyOrder.created_at)).scalar()
            if max_date:
                max_date = max_date.date()
                if end_date > max_date:
                    end_date = max_date
                    start_date = end_date - timedelta(days=days)
            start_dt, end_dt = self._date_bounds(start_date, end_date)

            rows = self.db.query(
                func.date(ShopifyOrder.created_at).label("day"),
                func.sum(ShopifyOrder.total_shipping).label("shipping"),
                func.sum(ShopifyOrder.total_tax).label("tax")
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.financial_status != 'voided',
                ShopifyOrder.cancelled_at.is_(None)
            ).group_by(func.date(ShopifyOrder.created_at)).order_by("day").all()

            return {
                "period": f"Last {days} days",
                "daily": [
                    {
                        "date": str(r.day),
                        "shipping": float(r.shipping or 0),
                        "tax": float(r.tax or 0)
                    } for r in rows
                ],
                "data_end": str(end_date)
            }
        except Exception as e:
            log.error(f"Error getting shipping/tax trends: {str(e)}")
            return {"error": str(e), "daily": []}

    def get_returns_by_product(self, start_date: date, end_date: date, limit: int = 20) -> Dict[str, Any]:
        """Returns by product (from refund line items)"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            refunds = self.db.query(ShopifyRefund).filter(
                ShopifyRefund.created_at >= start_dt,
                ShopifyRefund.created_at < end_dt,
                ShopifyRefund.refund_line_items.isnot(None)
            ).all()

            # Collect product IDs for title/type lookup
            product_ids = set()
            for refund in refunds:
                for item in (refund.refund_line_items or []):
                    pid = item.get('product_id')
                    if pid:
                        product_ids.add(pid)

            product_map = {}
            if product_ids:
                rows = self.db.query(
                    ShopifyProduct.shopify_product_id,
                    ShopifyProduct.title,
                    ShopifyProduct.product_type
                ).filter(
                    ShopifyProduct.shopify_product_id.in_(list(product_ids))
                ).all()
                product_map = {r.shopify_product_id: {"title": r.title, "product_type": r.product_type} for r in rows}

            totals = {}
            total_amount = Decimal("0")
            total_items = 0

            for refund in refunds:
                for item in (refund.refund_line_items or []):
                    sku = item.get('sku') or 'UNKNOWN'
                    pid = item.get('product_id')
                    quantity = int(item.get('quantity') or 0)
                    subtotal = Decimal(str(item.get('subtotal') or 0))
                    tax = Decimal(str(item.get('total_tax') or 0))
                    amount = subtotal + tax  # include tax to align with refunded totals
                    key = f"{sku}|{pid or ''}"
                    title = None
                    if pid in product_map:
                        title = product_map[pid].get("title")

                    if key not in totals:
                        totals[key] = {
                            "sku": sku,
                            "product_id": pid,
                            "title": title or "Unknown",
                            "refund_amount": Decimal("0"),
                            "refund_items": 0
                        }
                    totals[key]["refund_amount"] += amount
                    totals[key]["refund_items"] += quantity
                    total_amount += amount
                    total_items += quantity

            products = list(totals.values())
            products.sort(key=lambda x: x["refund_amount"], reverse=True)

            return {
                "period": f"{start_date} to {end_date}",
                "products": [
                    {
                        "sku": p["sku"],
                        "title": p["title"],
                        "refund_amount": float(p["refund_amount"] or 0) * -1,  # show as negative return value
                        "refund_items": p["refund_items"]
                    } for p in products[:limit]
                ],
                "total_refund_amount": float(total_amount or 0) * -1,
                "total_refund_items": total_items,
                "product_count": len(products),
                "data_end": str(end_date)
            }
        except Exception as e:
            log.error(f"Error getting returns by product: {str(e)}")
            return {"error": str(e), "products": []}

    def get_returns_by_product_type(self, start_date: date, end_date: date, limit: int = 10) -> Dict[str, Any]:
        """Returns by product type/category (from refund line items)"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            refunds = self.db.query(ShopifyRefund).filter(
                ShopifyRefund.created_at >= start_dt,
                ShopifyRefund.created_at < end_dt,
                ShopifyRefund.refund_line_items.isnot(None)
            ).all()

            product_ids = set()
            for refund in refunds:
                for item in (refund.refund_line_items or []):
                    pid = item.get('product_id')
                    if pid:
                        product_ids.add(pid)

            product_map = {}
            if product_ids:
                rows = self.db.query(
                    ShopifyProduct.shopify_product_id,
                    ShopifyProduct.product_type
                ).filter(
                    ShopifyProduct.shopify_product_id.in_(list(product_ids))
                ).all()
                product_map = {r.shopify_product_id: r.product_type for r in rows}

            totals = {}
            total_amount = Decimal("0")
            for refund in refunds:
                for item in (refund.refund_line_items or []):
                    pid = item.get('product_id')
                    product_type = product_map.get(pid) or "No product type assigned"
                    subtotal = Decimal(str(item.get('subtotal') or 0))
                    tax = Decimal(str(item.get('total_tax') or 0))
                    amount = subtotal + tax

                    if product_type not in totals:
                        totals[product_type] = Decimal("0")
                    totals[product_type] += amount
                    total_amount += amount

            categories = [
                {"product_type": k, "refund_amount": float(v) * -1}
                for k, v in totals.items()
            ]
            categories.sort(key=lambda x: x["refund_amount"])

            return {
                "period": f"{start_date} to {end_date}",
                "categories": categories[:limit],
                "total_refund_amount": float(total_amount or 0) * -1,
                "category_count": len(categories),
                "data_end": str(end_date)
            }
        except Exception as e:
            log.error(f"Error getting returns by product type: {str(e)}")
            return {"error": str(e), "categories": []}

    def get_product_variant_popularity(self, start_date: date, end_date: date, limit: int = 10) -> Dict[str, Any]:
        """Top product variants by units"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            rows = self.db.query(
                ShopifyOrderItem.sku,
                ShopifyOrderItem.title,
                func.sum(ShopifyOrderItem.quantity).label("units"),
                func.sum(ShopifyOrderItem.total_price).label("revenue")
            ).filter(
                ShopifyOrderItem.order_date >= start_dt,
                ShopifyOrderItem.order_date < end_dt
            ).group_by(ShopifyOrderItem.sku, ShopifyOrderItem.title).order_by(func.sum(ShopifyOrderItem.quantity).desc()).limit(limit).all()

            return {
                "period": f"{start_date} to {end_date}",
                "variants": [
                    {
                        "sku": r.sku,
                        "title": r.title,
                        "units": int(r.units or 0),
                        "revenue": float(r.revenue or 0)
                    } for r in rows
                ]
            }
        except Exception as e:
            log.error(f"Error getting variant popularity: {str(e)}")
            return {"error": str(e), "variants": []}

    def get_low_selling_products(self, start_date: date, end_date: date, limit: int = 10) -> Dict[str, Any]:
        """Products with lowest sales in a period (non-zero)"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            rows = self.db.query(
                ShopifyOrderItem.sku,
                ShopifyOrderItem.title,
                func.sum(ShopifyOrderItem.quantity).label("units"),
                func.sum(ShopifyOrderItem.total_price).label("revenue")
            ).filter(
                ShopifyOrderItem.order_date >= start_dt,
                ShopifyOrderItem.order_date < end_dt
            ).group_by(ShopifyOrderItem.sku, ShopifyOrderItem.title).having(func.sum(ShopifyOrderItem.quantity) > 0).order_by(func.sum(ShopifyOrderItem.quantity).asc()).limit(limit).all()

            return {
                "period": f"{start_date} to {end_date}",
                "products": [
                    {
                        "sku": r.sku,
                        "title": r.title,
                        "units": int(r.units or 0),
                        "revenue": float(r.revenue or 0)
                    } for r in rows
                ]
            }
        except Exception as e:
            log.error(f"Error getting low selling products: {str(e)}")
            return {"error": str(e), "products": []}

    def get_brand_sales(
        self,
        start_date: date,
        end_date: date,
        brand: Optional[str] = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get brand-level sales aggregated from Shopify orders.

        Uses SKU to join to NETT master (ProductCost.vendor) first for brand,
        falls back to ShopifyProduct.vendor if no match.

        Args:
            start_date: Start of date range
            end_date: End of date range
            brand: Optional brand name to filter (case-insensitive)
            limit: Number of brands/SKUs to return

        Returns:
            Dict with brand sales data, top brands or single brand details
        """
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)

            # Build a subquery to get vendor from NETT master (ProductCost) first,
            # fallback to ShopifyOrderItem.vendor or ShopifyProduct.vendor
            # We'll query order items and left join to product_costs to get authoritative vendor

            # Get all order items in the period with their SKUs
            items_query = self.db.query(
                ShopifyOrderItem.sku,
                ShopifyOrderItem.title,
                ShopifyOrderItem.quantity,
                ShopifyOrderItem.total_price,
                ShopifyOrderItem.shopify_order_id,
                ShopifyOrderItem.vendor.label('item_vendor')
            ).filter(
                ShopifyOrderItem.order_date >= start_dt,
                ShopifyOrderItem.order_date < end_dt,
                ShopifyOrderItem.sku.isnot(None),
                ShopifyOrderItem.sku != ''
            ).subquery()

            # Join to ProductCost to get NETT master vendor (authoritative)
            # Use COALESCE: ProductCost.vendor > ShopifyOrderItem.vendor
            vendor_label = func.coalesce(
                ProductCost.vendor,
                items_query.c.item_vendor
            ).label('brand')

            # Query with join
            results = self.db.query(
                vendor_label,
                items_query.c.sku,
                items_query.c.title,
                func.sum(items_query.c.quantity).label('units'),
                func.sum(items_query.c.total_price).label('revenue'),
                func.count(func.distinct(items_query.c.shopify_order_id)).label('orders')
            ).outerjoin(
                ProductCost,
                func.upper(ProductCost.vendor_sku) == func.upper(items_query.c.sku)
            )

            # Filter by brand if specified
            if brand:
                brand_upper = brand.upper()
                results = results.filter(
                    func.upper(func.coalesce(ProductCost.vendor, items_query.c.item_vendor)).like(f'%{brand_upper}%')
                )

            # Group by brand and SKU for detailed breakdown
            sku_results = results.group_by(
                vendor_label,
                items_query.c.sku,
                items_query.c.title
            ).order_by(
                func.sum(items_query.c.total_price).desc()
            ).all()

            # Aggregate by brand
            brand_totals = {}
            brand_skus = {}
            for row in sku_results:
                brand_name = row.brand or 'Unknown'
                if brand_name not in brand_totals:
                    brand_totals[brand_name] = {
                        'brand': brand_name,
                        'revenue': 0.0,
                        'units': 0,
                        'orders': 0,
                        'sku_count': 0
                    }
                    brand_skus[brand_name] = []

                brand_totals[brand_name]['revenue'] += float(row.revenue or 0)
                brand_totals[brand_name]['units'] += int(row.units or 0)
                brand_totals[brand_name]['orders'] += int(row.orders or 0)
                brand_totals[brand_name]['sku_count'] += 1
                brand_skus[brand_name].append({
                    'sku': row.sku,
                    'title': row.title,
                    'units': int(row.units or 0),
                    'revenue': round(float(row.revenue or 0), 2)
                })

            # Sort brands by revenue
            sorted_brands = sorted(
                brand_totals.values(),
                key=lambda x: x['revenue'],
                reverse=True
            )

            # Calculate totals
            total_revenue = sum(b['revenue'] for b in sorted_brands)
            total_units = sum(b['units'] for b in sorted_brands)
            total_orders = sum(b['orders'] for b in sorted_brands)

            # If specific brand requested, return detailed view
            if brand:
                matching_brands = [b for b in sorted_brands if brand.upper() in b['brand'].upper()]
                if matching_brands:
                    target_brand = matching_brands[0]
                    return {
                        'period': f"{start_date} to {end_date}",
                        'brand': target_brand['brand'],
                        'revenue': round(target_brand['revenue'], 2),
                        'units': target_brand['units'],
                        'orders': target_brand['orders'],
                        'sku_count': target_brand['sku_count'],
                        'top_skus': brand_skus.get(target_brand['brand'], [])[:limit],
                        'source': 'shopify_orders + nett_master'
                    }
                else:
                    return {
                        'period': f"{start_date} to {end_date}",
                        'brand': brand,
                        'error': f"No sales found for brand '{brand}'",
                        'source': 'shopify_orders + nett_master'
                    }

            # Return top brands
            top_brands = sorted_brands[:limit]
            for b in top_brands:
                b['revenue'] = round(b['revenue'], 2)
                b['revenue_pct'] = round((b['revenue'] / total_revenue * 100) if total_revenue > 0 else 0, 1)

            return {
                'period': f"{start_date} to {end_date}",
                'total_revenue': round(total_revenue, 2),
                'total_units': total_units,
                'total_orders': total_orders,
                'brand_count': len(sorted_brands),
                'top_brands': top_brands,
                'source': 'shopify_orders + nett_master'
            }

        except Exception as e:
            log.error(f"Error getting brand sales: {str(e)}")
            return {'error': str(e), 'source': 'shopify_orders + nett_master'}

    def get_new_vs_returning_customers(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """New vs returning customers in period"""
        try:
            start_dt, end_dt = self._date_bounds(start_date, end_date)
            # Customer IDs who ordered in period
            orders_in_period = self.db.query(
                ShopifyOrder.customer_id
            ).filter(
                ShopifyOrder.created_at >= start_dt,
                ShopifyOrder.created_at < end_dt,
                ShopifyOrder.customer_id.isnot(None)
            ).distinct().all()
            customer_ids = [o.customer_id for o in orders_in_period if o.customer_id]

            if not customer_ids:
                return {
                    "period": f"{start_date} to {end_date}",
                    "new_customers": 0,
                    "returning_customers": 0,
                    "total_customers": 0
                }

            # Determine first order date per customer (all-time)
            first_orders = self.db.query(
                ShopifyOrder.customer_id,
                func.min(ShopifyOrder.created_at).label("first_order_date")
            ).filter(
                ShopifyOrder.customer_id.in_(customer_ids)
            ).group_by(ShopifyOrder.customer_id).all()

            new_customers = sum(1 for r in first_orders if r.first_order_date and r.first_order_date.date() >= start_date)
            total_customers = len(customer_ids)
            returning_customers = total_customers - new_customers

            return {
                "period": f"{start_date} to {end_date}",
                "new_customers": new_customers,
                "returning_customers": returning_customers,
                "total_customers": total_customers
            }
        except Exception as e:
            log.error(f"Error getting new vs returning customers: {str(e)}")
            return {"error": str(e)}

    def get_inactive_customers(self, days: int = 30, limit: int = 20) -> Dict[str, Any]:
        """Customers who haven't purchased in N days"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            rows = self.db.query(
                ShopifyCustomer.shopify_customer_id,
                ShopifyCustomer.email,
                ShopifyCustomer.first_name,
                ShopifyCustomer.last_name,
                ShopifyCustomer.last_order_date,
                ShopifyCustomer.total_spent
            ).filter(
                ShopifyCustomer.last_order_date.isnot(None),
                ShopifyCustomer.last_order_date < cutoff
            ).order_by(ShopifyCustomer.last_order_date.asc()).limit(limit).all()

            return {
                "period": f"> {days} days",
                "customers": [
                    {
                        "id": r.shopify_customer_id,
                        "email": r.email,
                        "name": f"{r.first_name or ''} {r.last_name or ''}".strip(),
                        "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
                        "total_spent": float(r.total_spent or 0)
                    } for r in rows
                ]
            }
        except Exception as e:
            log.error(f"Error getting inactive customers: {str(e)}")
            return {"error": str(e), "customers": []}

    def get_customer_geo_breakdown(self, limit: int = 10) -> Dict[str, Any]:
        """Customers by city/region"""
        try:
            rows = self.db.query(
                ShopifyCustomer.default_address_city,
                ShopifyCustomer.default_address_province,
                ShopifyCustomer.default_address_country,
                func.count(ShopifyCustomer.id).label("count")
            ).group_by(
                ShopifyCustomer.default_address_city,
                ShopifyCustomer.default_address_province,
                ShopifyCustomer.default_address_country
            ).order_by(func.count(ShopifyCustomer.id).desc()).limit(limit).all()

            return {
                "top_locations": [
                    {
                        "city": r.default_address_city,
                        "region": r.default_address_province,
                        "country": r.default_address_country,
                        "customers": r.count
                    } for r in rows
                ]
            }
        except Exception as e:
            log.error(f"Error getting customer geo breakdown: {str(e)}")
            return {"error": str(e), "top_locations": []}

    def get_customer_retention_rate(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Retention rate: returning / total customers ordering in period"""
        base = self.get_new_vs_returning_customers(start_date, end_date)
        if "error" in base:
            return base
        total = base["total_customers"] or 0
        returning = base["returning_customers"] or 0
        retention = (returning / total * 100) if total > 0 else 0
        base["retention_rate"] = round(retention, 2)
        return base

    def get_inventory_status(self, threshold: int = 5, limit: int = 20) -> Dict[str, Any]:
        """Low and out-of-stock products (active products only)"""
        try:
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()
            low_rows = self.db.query(
                ShopifyInventory.sku,
                ShopifyInventory.title,
                ShopifyInventory.inventory_quantity,
                ShopifyInventory.vendor
            ).filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.inventory_quantity <= threshold
            ).order_by(ShopifyInventory.inventory_quantity.asc()).limit(limit).all()

            return {
                "threshold": threshold,
                "products": [
                    {
                        "sku": r.sku,
                        "title": r.title,
                        "vendor": r.vendor,
                        "quantity": r.inventory_quantity
                    } for r in low_rows
                ]
            }
        except Exception as e:
            log.error(f"Error getting inventory status: {str(e)}")
            return {"error": str(e), "products": []}

    def get_inventory_value_by_vendor(self, limit: int = 10) -> Dict[str, Any]:
        """Inventory value by vendor (cost * quantity) - active products only"""
        try:
            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()

            vendor_label = func.coalesce(ProductCost.vendor, ShopifyInventory.vendor).label("vendor")
            effective_cost = func.coalesce(ProductCost.nett_nett_cost_inc_gst, ShopifyInventory.cost)

            base_query = self.db.query(
                vendor_label,
                func.sum(ShopifyInventory.inventory_quantity).label("units"),
                func.sum(
                    case(
                        (ProductCost.nett_nett_cost_inc_gst.isnot(None), ShopifyInventory.inventory_quantity),
                        else_=0
                    )
                ).label("units_with_nett"),
                func.sum(ShopifyInventory.inventory_quantity * effective_cost).label("value")
            ).outerjoin(
                ProductCost,
                func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku)
            ).filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.inventory_quantity.isnot(None),
                effective_cost.isnot(None)
            ).group_by(vendor_label)

            rows = base_query.order_by(desc("value")).limit(limit).all()

            totals = self.db.query(
                func.sum(ShopifyInventory.inventory_quantity).label("units"),
                func.sum(
                    case(
                        (ProductCost.nett_nett_cost_inc_gst.isnot(None), ShopifyInventory.inventory_quantity),
                        else_=0
                    )
                ).label("units_with_nett")
            ).outerjoin(
                ProductCost,
                func.upper(ProductCost.vendor_sku) == func.upper(ShopifyInventory.sku)
            ).filter(
                ShopifyInventory.shopify_product_id.in_(active_pids),
                ShopifyInventory.inventory_quantity.isnot(None)
            ).first()

            total_units = int(totals.units or 0) if totals else 0
            units_with_nett = int(totals.units_with_nett or 0) if totals else 0
            coverage_pct = round((units_with_nett / total_units) * 100, 2) if total_units > 0 else 0

            return {
                "vendors": [
                    {
                        "vendor": r.vendor or "Unknown",
                        "inventory_value": float(r.value or 0),
                        "units": int(r.units or 0),
                        "units_with_nett_cost": int(r.units_with_nett or 0)
                    } for r in rows
                ],
                "coverage": {
                    "total_units": total_units,
                    "units_with_nett_cost": units_with_nett,
                    "coverage_pct": coverage_pct
                }
            }
        except Exception as e:
            log.error(f"Error getting inventory value: {str(e)}")
            return {"error": str(e), "vendors": []}

    def get_inventory_turnover(self, days: int = 30) -> Dict[str, Any]:
        """Inventory turnover approximation: units sold / current inventory (active products only)"""
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            sold = self.db.query(func.sum(ShopifyOrderItem.quantity)).filter(
                ShopifyOrderItem.order_date >= start_date,
                ShopifyOrderItem.order_date <= end_date
            ).scalar() or 0

            active_pids = self.db.query(ShopifyProduct.shopify_product_id).filter(
                ShopifyProduct.status == 'active'
            ).subquery()
            inventory = self.db.query(func.sum(ShopifyInventory.inventory_quantity)).filter(
                ShopifyInventory.shopify_product_id.in_(active_pids)
            ).scalar() or 0
            turnover = (sold / inventory) if inventory > 0 else 0

            return {
                "period": f"Last {days} days",
                "units_sold": int(sold or 0),
                "inventory_units": int(inventory or 0),
                "turnover_rate": round(turnover, 4)
            }
        except Exception as e:
            log.error(f"Error getting inventory turnover: {str(e)}")
            return {"error": str(e)}

    def parse_time_period(self, question: str) -> Optional[int]:
        """Extract number of days from question like 'last 30 days' or 'past week'"""
        question_lower = question.lower()

        # Match patterns like "last 30 days", "past 7 days"
        match = re.search(r'(?:last|past)\s+(\d+)\s+days?', question_lower)
        if match:
            return int(match.group(1))

        # Match "last week" = 7 days
        if 'last week' in question_lower or 'past week' in question_lower:
            return 7

        # Match "last month" = 30 days
        if 'last month' in question_lower or 'past month' in question_lower:
            return 30

        # Match "last 3 months" etc
        match = re.search(r'(?:last|past)\s+(\d+)\s+months?', question_lower)
        if match:
            return int(match.group(1)) * 30

        # Match "last quarter" = 90 days
        if 'last quarter' in question_lower or 'past quarter' in question_lower:
            return 90

        # Match "last year" = 365 days
        if 'last year' in question_lower or 'past year' in question_lower:
            return 365

        return None

    def parse_date_range(self, question: str) -> Optional[tuple]:
        """
        Parse date range from question. Returns (start_date, end_date, description) or None.

        Supports:
        - "last 7 days" → (today - 7 days, today)
        - "January 2026" → (Jan 1, Jan 31)
        - "2024" → (Jan 1 2024, Dec 31 2024)
        - "2024 vs 2025" → Returns first period only (use parse_comparison for both)
        """
        import calendar
        question_lower = question.lower()
        today = date.today()

        # Match explicit ISO date ranges: "2025-12-29 to 2026-01-26"
        iso_range = re.search(
            r'(?:from|between)?\s*(\d{4}-\d{2}-\d{2})\s*(?:to|and|–|-|—)\s*(\d{4}-\d{2}-\d{2})',
            question_lower
        )
        if iso_range:
            try:
                start = date.fromisoformat(iso_range.group(1))
                end = date.fromisoformat(iso_range.group(2))
                return (start, end, f"{start} to {end}")
            except ValueError:
                pass

        # Match "last N days"
        days = self.parse_time_period(question)
        if days:
            start = today - timedelta(days=days)
            return (start, today, f"Last {days} days")

        # Match specific month + year: "January 2026", "Jan 2025"
        month_names = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
            'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12
        }

        for month_name, month_num in month_names.items():
            match = re.search(rf'{month_name}\s+(\d{{4}})', question_lower)
            if match:
                year = int(match.group(1))
                last_day = calendar.monthrange(year, month_num)[1]
                start = date(year, month_num, 1)
                end = date(year, month_num, last_day)
                return (start, end, f"{month_name.capitalize()} {year}")

        # Match just year: "2024", "in 2025"
        match = re.search(r'\b(202[3-9])\b', question_lower)
        if match and 'vs' not in question_lower and 'versus' not in question_lower:
            year = int(match.group(1))
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            return (start, end, str(year))

        return None

    def parse_year_comparison(self, question: str) -> Optional[Dict]:
        """
        Parse year comparison from question like "2024 vs 2025".
        Returns dict with two periods or None.
        """
        question_lower = question.lower()

        # Match "2024 vs 2025" or "2024 versus 2025"
        match = re.search(r'(202[3-9])\s+(?:vs|versus|compared to|compared with)\s+(202[3-9])', question_lower)
        if match:
            year1, year2 = int(match.group(1)), int(match.group(2))
            return {
                'period1': {
                    'start': date(year1, 1, 1),
                    'end': date(year1, 12, 31),
                    'label': str(year1)
                },
                'period2': {
                    'start': date(year2, 1, 1),
                    'end': date(year2, 12, 31),
                    'label': str(year2)
                }
            }
        return None

    def get_full_historical_context(self) -> Dict[str, Any]:
        """
        Get comprehensive historical context for the LLM.
        Used when questions require full historical data.
        ALL DATA COMES FROM DATABASE - NO API CALLS.
        """
        context = {}

        # Database stats - shows what we have
        context['database_stats'] = self.get_database_stats()

        # Revenue by year
        context['revenue_by_year'] = self.get_revenue_by_year()

        # Top products (all time)
        context['top_products'] = self.get_top_products(limit=30)

        # Top customers
        context['top_customers'] = self.get_top_customers(limit=20)

        # Traffic sources
        context['traffic_sources'] = self.get_traffic_sources_summary()

        # Top search queries
        context['top_search_queries'] = self.get_top_search_queries(limit=50)

        # Monthly trends
        context['monthly_trends'] = self.get_monthly_trends(months=24)

        # Common time period summaries from DATABASE
        context['last_7_days'] = self.get_orders_last_n_days(7)
        context['last_30_days'] = self.get_orders_last_n_days(30)
        context['last_90_days'] = self.get_orders_last_n_days(90)

        return context

    # ==================== GA4 DATA METHODS ====================
    # Methods for querying the new GA4 tables for comprehensive analytics

    def get_ga4_daily_summary(self, days: int = 7) -> Dict[str, Any]:
        """
        Get GA4 daily summary metrics for the last N days.
        Returns sessions, users, revenue, and engagement metrics.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(GA4DailySummary).filter(
                GA4DailySummary.date >= start_date,
                GA4DailySummary.date <= end_date
            ).order_by(GA4DailySummary.date.desc()).all()

            if not results:
                return {'data': [], 'total': {}, 'period': f'Last {days} days', 'source': 'ga4_daily_summary'}

            # Aggregate totals
            total_sessions = sum(r.sessions or 0 for r in results)
            total_users = sum(r.active_users or 0 for r in results)
            total_new_users = sum(r.new_users or 0 for r in results)
            total_pageviews = sum(r.pageviews or 0 for r in results)
            total_revenue = sum(float(r.total_revenue or 0) for r in results)
            total_conversions = sum(r.total_conversions or 0 for r in results)

            avg_bounce_rate = sum(r.bounce_rate or 0 for r in results) / len(results) if results else 0
            avg_session_duration = sum(r.avg_session_duration or 0 for r in results) / len(results) if results else 0

            daily_data = [
                {
                    'date': str(r.date),
                    'sessions': r.sessions,
                    'active_users': r.active_users,
                    'new_users': r.new_users,
                    'pageviews': r.pageviews,
                    'bounce_rate': round(r.bounce_rate or 0, 2),
                    'avg_session_duration': round(r.avg_session_duration or 0, 1),
                    'conversions': r.total_conversions,
                    'revenue': float(r.total_revenue or 0)
                }
                for r in results
            ]

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_daily_summary',
                'total': {
                    'sessions': total_sessions,
                    'users': total_users,
                    'new_users': total_new_users,
                    'pageviews': total_pageviews,
                    'revenue': round(total_revenue, 2),
                    'conversions': total_conversions,
                    'avg_bounce_rate': round(avg_bounce_rate, 2),
                    'avg_session_duration_seconds': round(avg_session_duration, 1)
                },
                'daily': daily_data
            }
        except Exception as e:
            log.error(f"Error getting GA4 daily summary: {str(e)}")
            return {'error': str(e), 'source': 'ga4_daily_summary'}

    def get_ga4_channel_revenue(self, days: int = 28, limit: int = 15) -> Dict[str, Any]:
        """
        Get revenue by channel (source/medium) from GA4.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium,
                func.sum(GA4TrafficSource.sessions).label('sessions'),
                func.sum(GA4TrafficSource.total_users).label('users'),
                func.sum(GA4TrafficSource.conversions).label('conversions'),
                func.sum(GA4TrafficSource.total_revenue).label('revenue')
            ).filter(
                GA4TrafficSource.date >= start_date,
                GA4TrafficSource.date <= end_date
            ).group_by(
                GA4TrafficSource.session_source,
                GA4TrafficSource.session_medium
            ).order_by(
                desc('revenue')
            ).limit(limit).all()

            channels = [
                {
                    'source': r.session_source or '(direct)',
                    'medium': r.session_medium or '(none)',
                    'channel': f"{r.session_source or '(direct)'} / {r.session_medium or '(none)'}",
                    'sessions': r.sessions or 0,
                    'users': r.users or 0,
                    'conversions': r.conversions or 0,
                    'revenue': float(r.revenue or 0)
                }
                for r in results
            ]

            total_revenue = sum(c['revenue'] for c in channels)

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_traffic_sources',
                'total_revenue': round(total_revenue, 2),
                'channels': channels
            }
        except Exception as e:
            log.error(f"Error getting GA4 channel revenue: {str(e)}")
            return {'error': str(e), 'source': 'ga4_traffic_sources'}

    def get_ga4_top_pages(self, days: int = 28, limit: int = 20) -> Dict[str, Any]:
        """
        Get top pages by pageviews from GA4.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(
                GA4PagePerformance.page_path,
                GA4PagePerformance.page_title,
                func.sum(GA4PagePerformance.pageviews).label('pageviews'),
                func.sum(GA4PagePerformance.unique_pageviews).label('unique_pageviews'),
                func.avg(GA4PagePerformance.bounce_rate).label('bounce_rate'),
                func.avg(GA4PagePerformance.avg_time_on_page).label('avg_time')
            ).filter(
                GA4PagePerformance.date >= start_date,
                GA4PagePerformance.date <= end_date
            ).group_by(
                GA4PagePerformance.page_path,
                GA4PagePerformance.page_title
            ).order_by(
                desc('pageviews')
            ).limit(limit).all()

            pages = [
                {
                    'page_path': r.page_path,
                    'page_title': r.page_title or r.page_path,
                    'pageviews': r.pageviews or 0,
                    'unique_pageviews': r.unique_pageviews or 0,
                    'bounce_rate': round(float(r.bounce_rate or 0), 2),
                    'avg_time_on_page': round(float(r.avg_time or 0), 1)
                }
                for r in results
            ]

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_pages',
                'total_pages': len(pages),
                'pages': pages
            }
        except Exception as e:
            log.error(f"Error getting GA4 top pages: {str(e)}")
            return {'error': str(e), 'source': 'ga4_pages'}

    def get_ga4_top_landing_pages(
        self,
        days: int = 28,
        limit: int = 20,
        order_by: str = 'sessions',
        product_only: bool = False,
        min_sessions: int = 0
    ) -> Dict[str, Any]:
        """
        Get top landing pages from GA4.

        Args:
            days: Number of days to look back
            limit: Max results to return
            order_by: 'sessions' (default), 'conversions', 'conversion_rate_asc', 'conversion_rate_desc',
                      'high_sessions_low_conversion' (highest sessions with lowest conversion rate)
            product_only: If True, filter to only /products/ URLs
            min_sessions: Minimum sessions to include (useful for meaningful conversion rates)
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            query = self.db.query(
                GA4LandingPage.landing_page,
                func.sum(GA4LandingPage.sessions).label('sessions'),
                func.sum(GA4LandingPage.total_users).label('users'),
                func.sum(GA4LandingPage.conversions).label('conversions'),
                func.sum(GA4LandingPage.total_revenue).label('revenue'),
                func.avg(GA4LandingPage.bounce_rate).label('bounce_rate')
            ).filter(
                GA4LandingPage.date >= start_date,
                GA4LandingPage.date <= end_date
            )

            # Filter to product pages only if requested
            if product_only:
                query = query.filter(GA4LandingPage.landing_page.like('/products/%'))

            results = query.group_by(
                GA4LandingPage.landing_page
            ).order_by(
                desc('sessions')
            ).all()  # Get all, we'll sort/limit in Python for conversion_rate ordering

            # Compute conversion_rate from aggregated totals (not averaged daily rates)
            landing_pages = []
            for r in results:
                sessions = r.sessions or 0
                conversions = r.conversions or 0

                # Apply min_sessions filter
                if sessions < min_sessions:
                    continue

                # Conversion rate as percentage: (conversions / sessions) * 100
                conversion_rate = round((conversions / sessions) * 100, 2) if sessions > 0 else 0.0

                landing_pages.append({
                    'landing_page': r.landing_page,
                    'sessions': sessions,
                    'users': r.users or 0,
                    'conversions': conversions,
                    'revenue': float(r.revenue or 0),
                    'bounce_rate': round(float(r.bounce_rate or 0), 2),
                    'conversion_rate_pct': conversion_rate  # Already a percentage, e.g., 0.11 means 0.11%
                })

            # Sort based on order_by parameter
            if order_by == 'high_sessions_low_conversion':
                # Sort by sessions desc, then conversion_rate asc (highest traffic, lowest conversion)
                landing_pages = sorted(
                    landing_pages,
                    key=lambda x: (-x['sessions'], x['conversion_rate_pct'])
                )[:limit]
            elif order_by == 'conversion_rate_asc':
                # Bottom conversion rates - show pages with conversions > 0 first (meaningful low rates)
                # Then pages with 0 conversions, all with sessions > 100 for significance
                pages_with_conversions = sorted(
                    [p for p in landing_pages if p['sessions'] >= 100 and p['conversions'] > 0],
                    key=lambda x: x['conversion_rate_pct']
                )
                pages_zero_conversions = sorted(
                    [p for p in landing_pages if p['sessions'] >= 100 and p['conversions'] == 0],
                    key=lambda x: -x['sessions']  # Most sessions first among 0-conversion pages
                )
                landing_pages = (pages_with_conversions + pages_zero_conversions)[:limit]
            elif order_by == 'conversion_rate_desc':
                landing_pages = sorted(
                    landing_pages,
                    key=lambda x: x['conversion_rate_pct'],
                    reverse=True
                )[:limit]
            elif order_by == 'conversions':
                landing_pages = sorted(
                    landing_pages,
                    key=lambda x: x['conversions'],
                    reverse=True
                )[:limit]
            else:  # Default: by sessions
                landing_pages = landing_pages[:limit]

            result = {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_landing_pages',
                'total_landing_pages': len(landing_pages),
                'order_by': order_by,
                'note': 'conversion_rate_pct is already a percentage (e.g., 0.11 means 0.11%). Use this value directly.',
                'landing_pages': landing_pages
            }
            if product_only:
                result['filter'] = 'product_pages_only (/products/*)'
            if min_sessions > 0:
                result['min_sessions'] = min_sessions
            return result
        except Exception as e:
            log.error(f"Error getting GA4 top landing pages: {str(e)}")
            return {'error': str(e), 'source': 'ga4_landing_pages'}

    def get_ga4_device_breakdown(self, days: int = 28) -> Dict[str, Any]:
        """
        Get device breakdown (desktop, mobile, tablet) from GA4.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(
                GA4DeviceBreakdown.device_category,
                func.sum(GA4DeviceBreakdown.sessions).label('sessions'),
                func.sum(GA4DeviceBreakdown.active_users).label('users'),
                func.sum(GA4DeviceBreakdown.conversions).label('conversions'),
                func.sum(GA4DeviceBreakdown.total_revenue).label('revenue'),
                func.avg(GA4DeviceBreakdown.bounce_rate).label('bounce_rate')
            ).filter(
                GA4DeviceBreakdown.date >= start_date,
                GA4DeviceBreakdown.date <= end_date
            ).group_by(
                GA4DeviceBreakdown.device_category
            ).order_by(
                desc('sessions')
            ).all()

            total_sessions = sum(r.sessions or 0 for r in results)
            total_conversions = sum(r.conversions or 0 for r in results)

            devices = [
                {
                    'device': r.device_category,
                    'sessions': r.sessions or 0,
                    'session_share': round((r.sessions or 0) / total_sessions * 100, 1) if total_sessions > 0 else 0,
                    'users': r.users or 0,
                    'conversions': r.conversions or 0,
                    'conversion_share': round((r.conversions or 0) / total_conversions * 100, 1) if total_conversions > 0 else 0,
                    'revenue': float(r.revenue or 0),
                    'bounce_rate': round(float(r.bounce_rate or 0), 2)
                }
                for r in results
            ]

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_device_breakdown',
                'total_sessions': total_sessions,
                'total_conversions': total_conversions,
                'devices': devices
            }
        except Exception as e:
            log.error(f"Error getting GA4 device breakdown: {str(e)}")
            return {'error': str(e), 'source': 'ga4_device_breakdown'}

    def get_ga4_geo_revenue(self, days: int = 28, limit: int = 15) -> Dict[str, Any]:
        """
        Get geographic breakdown of revenue from GA4.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(
                GA4GeoBreakdown.country,
                func.sum(GA4GeoBreakdown.sessions).label('sessions'),
                func.sum(GA4GeoBreakdown.active_users).label('users'),
                func.sum(GA4GeoBreakdown.conversions).label('conversions'),
                func.sum(GA4GeoBreakdown.total_revenue).label('revenue')
            ).filter(
                GA4GeoBreakdown.date >= start_date,
                GA4GeoBreakdown.date <= end_date
            ).group_by(
                GA4GeoBreakdown.country
            ).order_by(
                desc('revenue')
            ).limit(limit).all()

            total_revenue = sum(float(r.revenue or 0) for r in results)

            countries = [
                {
                    'country': r.country,
                    'sessions': r.sessions or 0,
                    'users': r.users or 0,
                    'conversions': r.conversions or 0,
                    'revenue': float(r.revenue or 0),
                    'revenue_share': round(float(r.revenue or 0) / total_revenue * 100, 1) if total_revenue > 0 else 0
                }
                for r in results
            ]

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_geo_breakdown',
                'total_revenue': round(total_revenue, 2),
                'countries': countries
            }
        except Exception as e:
            log.error(f"Error getting GA4 geo revenue: {str(e)}")
            return {'error': str(e), 'source': 'ga4_geo_breakdown'}

    def get_ga4_ecommerce_summary(self, days: int = 28) -> Dict[str, Any]:
        """
        Get e-commerce summary from GA4 daily ecommerce table.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(GA4DailyEcommerce).filter(
                GA4DailyEcommerce.date >= start_date,
                GA4DailyEcommerce.date <= end_date
            ).order_by(GA4DailyEcommerce.date.desc()).all()

            if not results:
                return {'data': [], 'total': {}, 'period': f'Last {days} days', 'source': 'ga4_daily_ecommerce'}

            total_purchases = sum(r.ecommerce_purchases or 0 for r in results)
            total_revenue = sum(float(r.total_revenue or 0) for r in results)
            total_add_to_carts = sum(r.add_to_carts or 0 for r in results)
            total_checkouts = sum(r.checkouts or 0 for r in results)

            daily_data = [
                {
                    'date': str(r.date),
                    'purchases': r.ecommerce_purchases or 0,
                    'revenue': float(r.total_revenue or 0),
                    'add_to_carts': r.add_to_carts or 0,
                    'checkouts': r.checkouts or 0,
                    'cart_to_purchase_rate': round(r.cart_to_purchase_rate or 0, 4)
                }
                for r in results
            ]

            avg_cart_to_purchase = total_purchases / total_add_to_carts if total_add_to_carts > 0 else 0

            return {
                'period': f'Last {days} days',
                'date_range': f'{start_date} to {end_date}',
                'source': 'ga4_daily_ecommerce',
                'total': {
                    'purchases': total_purchases,
                    'revenue': round(total_revenue, 2),
                    'add_to_carts': total_add_to_carts,
                    'checkouts': total_checkouts,
                    'cart_to_purchase_rate': round(avg_cart_to_purchase, 4)
                },
                'daily': daily_data
            }
        except Exception as e:
            log.error(f"Error getting GA4 ecommerce summary: {str(e)}")
            return {'error': str(e), 'source': 'ga4_daily_ecommerce'}

    # ==================== CAPRICE COMPETITIVE PRICING ====================

    def get_competitor_undercuts(self, days: int = None, limit: int = 20, use_latest: bool = True) -> Dict[str, Any]:
        """
        Get products where competitors are undercutting our price.
        Returns products where lowest_competitor_price < current_price.

        By default uses latest snapshot only for performance.
        Set use_latest=False and provide days for date range query.
        """
        try:
            # Use latest snapshot by default for performance
            if use_latest or days is None:
                latest_date = self.get_latest_caprice_snapshot_date()
                if not latest_date:
                    return {'error': 'No pricing data available', 'count': 0, 'products': [], 'source': 'competitive_pricing'}
                date_filter = CompetitivePricing.pricing_date == latest_date
                period_desc = f'Snapshot: {latest_date}'
            else:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                date_filter = CompetitivePricing.pricing_date.between(start_date, end_date)
                period_desc = f'Last {days} days'

            # Query with undercut filter
            results = self.db.query(
                CompetitivePricing.variant_sku,
                CompetitivePricing.title,
                CompetitivePricing.vendor,
                CompetitivePricing.current_price,
                CompetitivePricing.lowest_competitor_price,
                CompetitivePricing.nett_cost,
                CompetitivePricing.profit_margin_pct,
                CompetitivePricing.pricing_date
            ).filter(
                date_filter,
                CompetitivePricing.lowest_competitor_price.isnot(None),
                CompetitivePricing.current_price.isnot(None),
                CompetitivePricing.lowest_competitor_price < CompetitivePricing.current_price
            ).order_by(
                # Order by price gap (biggest undercutting first)
                (CompetitivePricing.current_price - CompetitivePricing.lowest_competitor_price).desc()
            ).limit(limit).all()

            undercuts = []
            for r in results:
                price_gap = float(r.current_price) - float(r.lowest_competitor_price)
                gap_pct = (price_gap / float(r.current_price)) * 100 if r.current_price else 0
                undercuts.append({
                    'sku': r.variant_sku,
                    'title': r.title,
                    'vendor': r.vendor,
                    'our_price': float(r.current_price),
                    'competitor_price': float(r.lowest_competitor_price),
                    'price_gap': round(price_gap, 2),
                    'gap_pct': round(gap_pct, 2),
                    'cost': float(r.nett_cost) if r.nett_cost else None,
                    'margin_pct': float(r.profit_margin_pct) if r.profit_margin_pct else None,
                    'pricing_date': str(r.pricing_date)
                })

            return {
                'period': period_desc,
                'count': len(undercuts),
                'products': undercuts,
                'source': 'competitive_pricing'
            }
        except Exception as e:
            log.error(f"Error getting competitor undercuts: {str(e)}")
            return {'error': str(e), 'source': 'competitive_pricing'}

    def get_price_gap_by_competitor(self, days: int = None, limit: int = 10, use_latest: bool = True) -> Dict[str, Any]:
        """
        Aggregate price gaps by competitor to see which competitors undercut most often.

        By default uses latest snapshot only for performance.
        """
        try:
            # Use latest snapshot by default for performance
            if use_latest or days is None:
                latest_date = self.get_latest_caprice_snapshot_date()
                if not latest_date:
                    return {'error': 'No pricing data available', 'competitors': [], 'source': 'competitive_pricing'}
                date_filter = CompetitivePricing.pricing_date == latest_date
                period_desc = f'Snapshot: {latest_date}'
            else:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                date_filter = CompetitivePricing.pricing_date.between(start_date, end_date)
                period_desc = f'Last {days} days'

            # Get pricing records - only select needed columns for performance
            # Add row limit for safety on historical queries (50K max rows)
            MAX_ROWS = 50000
            query = self.db.query(
                CompetitivePricing.current_price,
                CompetitivePricing.price_8appliances,
                CompetitivePricing.price_appliancesonline,
                CompetitivePricing.price_austpek,
                CompetitivePricing.price_binglee,
                CompetitivePricing.price_blueleafbath,
                CompetitivePricing.price_brandsdirect,
                CompetitivePricing.price_buildmat,
                CompetitivePricing.price_cookandbathe,
                CompetitivePricing.price_designerbathware,
                CompetitivePricing.price_harveynorman,
                CompetitivePricing.price_idealbathroom,
                CompetitivePricing.price_justbathroomware,
                CompetitivePricing.price_thebluespace,
                CompetitivePricing.price_wellsons,
                CompetitivePricing.price_winnings,
            ).filter(date_filter)

            # For historical queries, apply row limit
            if not use_latest and days is not None:
                query = query.limit(MAX_ROWS)

            records = query.all()

            # Competitor names in same order as query columns (index 1-15, index 0 is current_price)
            competitor_names = [
                '8appliances', 'appliancesonline', 'austpek', 'binglee', 'blueleafbath',
                'brandsdirect', 'buildmat', 'cookandbathe', 'designerbathware', 'harveynorman',
                'idealbathroom', 'justbathroomware', 'thebluespace', 'wellsons', 'winnings'
            ]

            # Initialize stats for each competitor
            competitor_stats = {name: {'undercut_count': 0, 'total_gap': 0, 'products_tracked': 0} for name in competitor_names}

            # Process records - each record is a tuple (current_price, price_comp1, price_comp2, ...)
            for record in records:
                our_price = record[0]  # current_price is first column
                if our_price is None:
                    continue
                our_price_val = float(our_price)

                for i, comp_name in enumerate(competitor_names):
                    comp_price = record[i + 1]  # Competitor prices start at index 1
                    if comp_price is not None:
                        comp_price_val = float(comp_price)
                        competitor_stats[comp_name]['products_tracked'] += 1
                        if comp_price_val < our_price_val:
                            competitor_stats[comp_name]['undercut_count'] += 1
                            competitor_stats[comp_name]['total_gap'] += our_price_val - comp_price_val

            # Calculate final stats
            final_stats = {}
            for comp_name, stats in competitor_stats.items():
                if stats['products_tracked'] > 0:
                    final_stats[comp_name] = {
                        'undercut_count': stats['undercut_count'],
                        'products_tracked': stats['products_tracked'],
                        'undercut_pct': round((stats['undercut_count'] / stats['products_tracked']) * 100, 2),
                        'total_price_gap': round(stats['total_gap'], 2),
                        'avg_gap': round(stats['total_gap'] / stats['undercut_count'], 2) if stats['undercut_count'] > 0 else 0
                    }

            # Sort by undercut count
            sorted_stats = sorted(
                final_stats.items(),
                key=lambda x: x[1]['undercut_count'],
                reverse=True
            )[:limit]

            return {
                'period': period_desc,
                'competitors': [{'name': k, **v} for k, v in sorted_stats],
                'source': 'competitive_pricing'
            }
        except Exception as e:
            log.error(f"Error getting price gap by competitor: {str(e)}")
            return {'error': str(e), 'source': 'competitive_pricing'}

    def get_min_margin_breaches(self, days: int = 7, limit: int = 20) -> Dict[str, Any]:
        """
        Get products that are priced below minimum price or losing money.
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            results = self.db.query(
                CompetitivePricing.variant_sku,
                CompetitivePricing.title,
                CompetitivePricing.vendor,
                CompetitivePricing.current_price,
                CompetitivePricing.minimum_price,
                CompetitivePricing.nett_cost,
                CompetitivePricing.profit_margin_pct,
                CompetitivePricing.profit_amount,
                CompetitivePricing.is_losing_money,
                CompetitivePricing.is_below_minimum,
                CompetitivePricing.pricing_date
            ).filter(
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date,
                or_(
                    CompetitivePricing.is_losing_money == True,
                    CompetitivePricing.is_below_minimum == True
                )
            ).order_by(
                CompetitivePricing.profit_amount.asc()  # Most negative profit first
            ).limit(limit).all()

            breaches = []
            for r in results:
                breach_type = []
                if r.is_losing_money:
                    breach_type.append('losing_money')
                if r.is_below_minimum:
                    breach_type.append('below_minimum')

                breaches.append({
                    'sku': r.variant_sku,
                    'title': r.title,
                    'vendor': r.vendor,
                    'current_price': float(r.current_price) if r.current_price else None,
                    'minimum_price': float(r.minimum_price) if r.minimum_price else None,
                    'cost': float(r.nett_cost) if r.nett_cost else None,
                    'margin_pct': float(r.profit_margin_pct) if r.profit_margin_pct else None,
                    'profit_amount': float(r.profit_amount) if r.profit_amount else None,
                    'breach_type': breach_type,
                    'pricing_date': str(r.pricing_date)
                })

            return {
                'period': f'Last {days} days',
                'count': len(breaches),
                'products': breaches,
                'source': 'competitive_pricing'
            }
        except Exception as e:
            log.error(f"Error getting margin breaches: {str(e)}")
            return {'error': str(e), 'source': 'competitive_pricing'}

    def get_competitive_pricing_summary(self, days: int = None, use_latest: bool = True) -> Dict[str, Any]:
        """
        Get summary statistics from Caprice competitive pricing data.
        By default uses latest snapshot only for performance.
        """
        try:
            # Use latest snapshot by default for performance
            if use_latest or days is None:
                latest_date = self.get_latest_caprice_snapshot_date()
                if not latest_date:
                    return {'error': 'No pricing data available', 'source': 'competitive_pricing'}
                date_filter = CompetitivePricing.pricing_date == latest_date
                period_desc = f'Snapshot: {latest_date}'
            else:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                date_filter = CompetitivePricing.pricing_date.between(start_date, end_date)
                period_desc = f'Last {days} days'

            # Use SQL aggregation for performance
            from sqlalchemy import case

            result = self.db.query(
                func.count(CompetitivePricing.id).label('total'),
                func.count(CompetitivePricing.lowest_competitor_price).label('with_comp_data'),
                func.sum(case(
                    (CompetitivePricing.lowest_competitor_price < CompetitivePricing.current_price, 1),
                    else_=0
                )).label('undercut'),
                func.sum(case((CompetitivePricing.is_losing_money == True, 1), else_=0)).label('losing_money'),
                func.sum(case((CompetitivePricing.is_below_minimum == True, 1), else_=0)).label('below_min'),
                func.sum(case((CompetitivePricing.has_no_cost == True, 1), else_=0)).label('no_cost'),
                func.avg(CompetitivePricing.profit_margin_pct).label('avg_margin')
            ).filter(date_filter).first()

            if not result or result.total == 0:
                return {
                    'period': period_desc,
                    'error': 'No pricing data found for this period',
                    'source': 'competitive_pricing'
                }

            total = result.total or 0
            with_comp = result.with_comp_data or 0
            undercut = result.undercut or 0

            return {
                'period': period_desc,
                'total_products': total,
                'products_with_competitor_data': with_comp,
                'products_undercut_by_competitor': undercut,
                'undercut_pct': round((undercut / with_comp) * 100, 2) if with_comp > 0 else 0,
                'products_losing_money': result.losing_money or 0,
                'products_below_minimum_price': result.below_min or 0,
                'products_missing_cost': result.no_cost or 0,
                'average_margin_pct': round(float(result.avg_margin or 0), 2),
                'source': 'competitive_pricing'
            }
        except Exception as e:
            log.error(f"Error getting competitive pricing summary: {str(e)}")
            return {'error': str(e), 'source': 'competitive_pricing'}

    def get_caprice_sku_competitor_price(
        self,
        sku: str,
        target_price: float,
        start_date: date,
        end_date: date,
        tolerance: float = 1.0
    ) -> Dict[str, Any]:
        """
        Find which competitor has a specific price for a given SKU.

        Args:
            sku: The product SKU to look up
            target_price: The price to match
            start_date: Start of date range
            end_date: End of date range
            tolerance: Price tolerance for matching (default ±$1)

        Returns:
            Dict with matched competitor(s), dates, and prices
        """
        try:
            # Competitor column mapping
            competitor_columns = [
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
                # Additional competitors (added 2026-01-28)
                ('agcequipment', 'price_agcequipment'),
                ('berloniappliances', 'price_berloniapp'),
                ('eands', 'price_eands'),
                ('plumbingsales', 'price_plumbingsales'),
                ('powerland', 'price_powerland'),
                ('saappliancewarehouse', 'price_saappliances'),
                ('samedayhotwaterservice', 'price_sameday'),
                ('shireskylights', 'price_shire'),
                ('voguespas', 'price_vogue'),
            ]

            # Query records for this SKU in date range
            records = self.db.query(CompetitivePricing).filter(
                CompetitivePricing.variant_sku == sku,
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date
            ).order_by(CompetitivePricing.pricing_date.desc()).all()

            if not records:
                return {
                    'sku': sku,
                    'target_price': target_price,
                    'date_range': f'{start_date} to {end_date}',
                    'matches': [],
                    'message': f'No pricing data found for SKU {sku} in date range',
                    'source': 'competitive_pricing'
                }

            # Search for price matches
            matches = []
            product_title = None
            our_prices = []

            for record in records:
                if not product_title and record.title:
                    product_title = record.title

                if record.current_price:
                    our_prices.append({
                        'date': str(record.pricing_date),
                        'price': float(record.current_price)
                    })

                # Check each competitor column
                for comp_name, col_name in competitor_columns:
                    comp_price = getattr(record, col_name, None)
                    if comp_price is not None:
                        price_val = float(comp_price)
                        # Check if price matches within tolerance
                        if abs(price_val - target_price) <= tolerance:
                            matches.append({
                                'competitor': comp_name,
                                'price': price_val,
                                'pricing_date': str(record.pricing_date),
                                'our_price': float(record.current_price) if record.current_price else None
                            })

                # Also check lowest_competitor_price
                if record.lowest_competitor_price:
                    lowest_price = float(record.lowest_competitor_price)
                    if abs(lowest_price - target_price) <= tolerance:
                        # Check if we already have this match from a specific competitor
                        already_matched = any(
                            m['price'] == lowest_price and m['pricing_date'] == str(record.pricing_date)
                            for m in matches
                        )
                        if not already_matched:
                            matches.append({
                                'competitor': 'lowest_competitor (unspecified)',
                                'price': lowest_price,
                                'pricing_date': str(record.pricing_date),
                                'our_price': float(record.current_price) if record.current_price else None
                            })

            # Deduplicate and sort by date
            unique_matches = []
            seen = set()
            for m in matches:
                key = (m['competitor'], m['pricing_date'])
                if key not in seen:
                    seen.add(key)
                    unique_matches.append(m)

            unique_matches.sort(key=lambda x: x['pricing_date'], reverse=True)

            return {
                'sku': sku,
                'product_title': product_title,
                'target_price': target_price,
                'tolerance': tolerance,
                'date_range': f'{start_date} to {end_date}',
                'matches': unique_matches,
                'match_count': len(unique_matches),
                'our_prices': our_prices[:5],  # Last 5 pricing dates
                'message': f'Found {len(unique_matches)} competitor(s) with price ${target_price} (±${tolerance})' if unique_matches else f'No competitor price of ${target_price} found for SKU {sku}',
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting SKU competitor price: {str(e)}")
            return {'error': str(e), 'sku': sku, 'source': 'competitive_pricing'}

    def get_caprice_brand_competitive_gaps(self, days: int = None, limit: int = 10, use_latest: bool = True) -> Dict[str, Any]:
        """
        Get brand-level competitive analysis aggregated by vendor.

        Groups products by vendor (brand) and computes:
        - total_skus_tracked: Number of SKUs for this brand
        - skus_undercut_count: SKUs where lowest_competitor_price < current_price
        - undercut_rate: skus_undercut_count / total_skus_tracked
        - avg_price_gap: Average gap for undercut products
        - total_price_gap: Sum of all price gaps for undercut products

        Sorted by total_price_gap descending (brands with highest $ exposure first).

        Args:
            days: Days to look back (ignored if use_latest=True)
            limit: Max brands to return
            use_latest: If True, only use latest snapshot (much faster)
        """
        try:
            # Determine date filter - use latest snapshot by default for speed
            if use_latest or days is None:
                latest_date = self.get_latest_caprice_snapshot_date()
                if not latest_date:
                    return {'error': 'No pricing data available', 'source': 'competitive_pricing'}
                date_filter = CompetitivePricing.pricing_date == latest_date
                period_desc = f'Snapshot: {latest_date}'
            else:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                date_filter = and_(
                    CompetitivePricing.pricing_date >= start_date,
                    CompetitivePricing.pricing_date <= end_date
                )
                period_desc = f'Last {days} days'

            # Use SQL aggregation for performance
            # Calculate price gap only for undercut rows: current_price - lowest_competitor_price
            price_gap_expr = CompetitivePricing.current_price - CompetitivePricing.lowest_competitor_price

            results = self.db.query(
                CompetitivePricing.vendor,
                func.count(CompetitivePricing.id).label('total_skus'),
                func.count(CompetitivePricing.lowest_competitor_price).label('skus_with_comp_data'),
                func.sum(case(
                    (CompetitivePricing.lowest_competitor_price < CompetitivePricing.current_price, 1),
                    else_=0
                )).label('skus_undercut'),
                func.sum(case(
                    (CompetitivePricing.lowest_competitor_price < CompetitivePricing.current_price, price_gap_expr),
                    else_=0
                )).label('total_price_gap')
            ).filter(
                date_filter,
                CompetitivePricing.vendor.isnot(None),
                CompetitivePricing.vendor != ''
            ).group_by(CompetitivePricing.vendor).all()

            if not results:
                return {
                    'period': period_desc,
                    'brands': [],
                    'message': 'No pricing data found for this period',
                    'source': 'competitive_pricing'
                }

            # Build results list
            brands = []
            for row in results:
                vendor = row.vendor
                total_skus = row.total_skus or 0
                skus_with_comp = row.skus_with_comp_data or 0
                skus_undercut = row.skus_undercut or 0
                total_gap = float(row.total_price_gap or 0)

                if skus_with_comp > 0:
                    undercut_rate = (skus_undercut / skus_with_comp) * 100
                    avg_gap = total_gap / skus_undercut if skus_undercut > 0 else 0

                    brands.append({
                        'brand': vendor,
                        'total_skus_tracked': total_skus,
                        'skus_with_competitor_data': skus_with_comp,
                        'skus_undercut_count': skus_undercut,
                        'undercut_rate': round(undercut_rate, 2),
                        'avg_price_gap': round(avg_gap, 2),
                        'total_price_gap': round(total_gap, 2)
                    })

            # Sort by total_price_gap descending (highest exposure first)
            brands.sort(key=lambda x: x['total_price_gap'], reverse=True)
            brands = brands[:limit]

            return {
                'period': period_desc,
                'brands': brands,
                'total_brands_analyzed': len(results),
                'sort_order': 'total_price_gap descending (highest $ exposure first)',
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting brand competitive gaps: {str(e)}")
            return {'error': str(e), 'source': 'competitive_pricing'}

    def get_latest_caprice_snapshot_date(self) -> Optional[date]:
        """Get the most recent pricing_date in competitive_pricing table."""
        try:
            result = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            return result
        except Exception as e:
            log.error(f"Error getting latest Caprice snapshot date: {str(e)}")
            return None

    def get_caprice_sku_details_latest(self, sku: str) -> Dict[str, Any]:
        """
        Get detailed pricing info for a SKU from the latest snapshot.

        Returns:
            vendor, variant_sku, current_price, minimum_price (floor),
            nett_cost, profit_amount, profit_margin_pct, lowest_competitor_price,
            competitor_matches (competitors whose price matches current_price ±$1)
        """
        try:
            latest_date = self.get_latest_caprice_snapshot_date()
            if not latest_date:
                return {'error': 'No pricing data available', 'source': 'competitive_pricing'}

            # Get the record for this SKU on latest date
            record = self.db.query(CompetitivePricing).filter(
                CompetitivePricing.variant_sku == sku,
                CompetitivePricing.pricing_date == latest_date
            ).first()

            if not record:
                # Try case-insensitive search
                record = self.db.query(CompetitivePricing).filter(
                    func.upper(CompetitivePricing.variant_sku) == sku.upper(),
                    CompetitivePricing.pricing_date == latest_date
                ).first()

            if not record:
                return {
                    'sku': sku,
                    'snapshot_date': str(latest_date),
                    'error': f'SKU {sku} not found in latest snapshot',
                    'source': 'competitive_pricing'
                }

            # Competitor column mapping
            competitor_columns = [
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
                # Additional competitors (added 2026-01-28)
                ('agcequipment', 'price_agcequipment'),
                ('berloniappliances', 'price_berloniapp'),
                ('eands', 'price_eands'),
                ('plumbingsales', 'price_plumbingsales'),
                ('powerland', 'price_powerland'),
                ('saappliancewarehouse', 'price_saappliances'),
                ('samedayhotwaterservice', 'price_sameday'),
                ('shireskylights', 'price_shire'),
                ('voguespas', 'price_vogue'),
            ]

            # Find competitor matches (price within ±$1 of current_price)
            current_price = float(record.current_price) if record.current_price else None
            competitor_matches = []
            all_competitor_prices = {}

            for comp_name, col_name in competitor_columns:
                comp_price = getattr(record, col_name, None)
                if comp_price is not None:
                    price_val = float(comp_price)
                    all_competitor_prices[comp_name] = price_val
                    if current_price and abs(price_val - current_price) <= 1.0:
                        competitor_matches.append({
                            'competitor': comp_name,
                            'price': price_val,
                            'gap': round(price_val - current_price, 2)
                        })

            return {
                'sku': record.variant_sku,
                'title': record.title,
                'vendor': record.vendor,
                'snapshot_date': str(latest_date),
                'current_price': float(record.current_price) if record.current_price else None,
                'minimum_price': float(record.minimum_price) if record.minimum_price else None,
                'nett_cost': float(record.nett_cost) if record.nett_cost else None,
                'profit_amount': float(record.profit_amount) if record.profit_amount else None,
                'profit_margin_pct': float(record.profit_margin_pct) if record.profit_margin_pct else None,
                'lowest_competitor_price': float(record.lowest_competitor_price) if record.lowest_competitor_price else None,
                'competitor_matches': competitor_matches,
                'competitor_match_count': len(competitor_matches),
                'all_competitor_prices': all_competitor_prices,
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting SKU details: {str(e)}")
            return {'error': str(e), 'sku': sku, 'source': 'competitive_pricing'}

    def get_caprice_competitor_price_match_latest(
        self,
        sku: str,
        target_price: float,
        tolerance: float = 1.0
    ) -> Dict[str, Any]:
        """
        Find which competitor(s) are selling a SKU at a specific price (latest snapshot).

        Args:
            sku: Product SKU
            target_price: The price to match
            tolerance: Price tolerance (default ±$1)
        """
        try:
            latest_date = self.get_latest_caprice_snapshot_date()
            if not latest_date:
                return {'error': 'No pricing data available', 'source': 'competitive_pricing'}

            record = self.db.query(CompetitivePricing).filter(
                func.upper(CompetitivePricing.variant_sku) == sku.upper(),
                CompetitivePricing.pricing_date == latest_date
            ).first()

            if not record:
                return {
                    'sku': sku,
                    'target_price': target_price,
                    'snapshot_date': str(latest_date),
                    'matches': [],
                    'message': f'SKU {sku} not found in latest snapshot',
                    'source': 'competitive_pricing'
                }

            competitor_columns = [
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
            ]

            matches = []
            for comp_name, col_name in competitor_columns:
                comp_price = getattr(record, col_name, None)
                if comp_price is not None:
                    price_val = float(comp_price)
                    if abs(price_val - target_price) <= tolerance:
                        matches.append({
                            'competitor': comp_name,
                            'price': price_val
                        })

            return {
                'sku': sku,
                'title': record.title,
                'vendor': record.vendor,
                'target_price': target_price,
                'tolerance': tolerance,
                'snapshot_date': str(latest_date),
                'our_price': float(record.current_price) if record.current_price else None,
                'matches': matches,
                'match_count': len(matches),
                'message': f"Found {len(matches)} competitor(s) at ${target_price}" if matches else f"No competitor at ${target_price} for SKU {sku}",
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting competitor price match: {str(e)}")
            return {'error': str(e), 'sku': sku, 'source': 'competitive_pricing'}

    def get_caprice_brand_unmatchable(
        self,
        brand: str,
        use_latest_only: bool = True,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get SKUs for a brand where we can't match competitors because they're below our minimum.

        Args:
            brand: Vendor/brand name (case-insensitive)
            use_latest_only: If True, only check latest snapshot; if False, check last N days
            days: Days to look back if use_latest_only is False
        """
        try:
            if use_latest_only:
                latest_date = self.get_latest_caprice_snapshot_date()
                if not latest_date:
                    return {'error': 'No pricing data available', 'source': 'competitive_pricing'}
                start_date = latest_date
                end_date = latest_date
                period_desc = f'Snapshot: {latest_date}'
            else:
                end_date = date.today()
                start_date = end_date - timedelta(days=days)
                period_desc = f'Last {days} days'

            # Get all records for this brand
            records = self.db.query(CompetitivePricing).filter(
                func.upper(CompetitivePricing.vendor) == brand.upper(),
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date
            ).all()

            if not records:
                return {
                    'brand': brand,
                    'period': period_desc,
                    'error': f'No data found for brand "{brand}"',
                    'source': 'competitive_pricing'
                }

            total_skus = len(records)
            unmatchable_skus = []

            for record in records:
                # Can't match if lowest_competitor_price < minimum_price
                if (record.lowest_competitor_price is not None and
                    record.minimum_price is not None and
                    float(record.lowest_competitor_price) < float(record.minimum_price)):

                    gap = float(record.minimum_price) - float(record.lowest_competitor_price)
                    unmatchable_skus.append({
                        'sku': record.variant_sku,
                        'title': record.title,
                        'our_minimum': float(record.minimum_price),
                        'lowest_competitor': float(record.lowest_competitor_price),
                        'gap_below_minimum': round(gap, 2),
                        'current_price': float(record.current_price) if record.current_price else None,
                        'nett_cost': float(record.nett_cost) if record.nett_cost else None
                    })

            # Sort by gap (biggest gap first)
            unmatchable_skus.sort(key=lambda x: x['gap_below_minimum'], reverse=True)

            unmatchable_count = len(unmatchable_skus)
            unmatchable_pct = round((unmatchable_count / total_skus) * 100, 2) if total_skus > 0 else 0

            return {
                'brand': brand,
                'period': period_desc,
                'total_skus': total_skus,
                'unmatchable_count': unmatchable_count,
                'unmatchable_pct': unmatchable_pct,
                'unmatchable_skus': unmatchable_skus[:20],  # Limit to top 20
                'message': f"{unmatchable_count} of {total_skus} SKUs ({unmatchable_pct}%) cannot be matched - competitors below minimum",
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting brand unmatchable: {str(e)}")
            return {'error': str(e), 'brand': brand, 'source': 'competitive_pricing'}

    def get_caprice_competitor_trend(
        self,
        competitor: str,
        months: int = 12
    ) -> Dict[str, Any]:
        """
        Get 12-month trend for a specific competitor.

        For each month: count SKUs undercut, avg gap, total gap.
        Uses SQL aggregation for performance.
        """
        try:
            # Map competitor name to column
            competitor_column_map = {
                '8appliances': 'price_8appliances',
                'appliancesonline': 'price_appliancesonline',
                'austpek': 'price_austpek',
                'binglee': 'price_binglee',
                'blueleafbath': 'price_blueleafbath',
                'brandsdirect': 'price_brandsdirect',
                'buildmat': 'price_buildmat',
                'cookandbathe': 'price_cookandbathe',
                'designerbathware': 'price_designerbathware',
                'harveynorman': 'price_harveynorman',
                'idealbathroom': 'price_idealbathroom',
                'justbathroomware': 'price_justbathroomware',
                'thebluespace': 'price_thebluespace',
                'wellsons': 'price_wellsons',
                'winnings': 'price_winnings',
            }

            comp_lower = competitor.lower().replace(' ', '').replace('-', '')
            col_name = competitor_column_map.get(comp_lower)

            if not col_name:
                # Try partial match
                for name, col in competitor_column_map.items():
                    if comp_lower in name or name in comp_lower:
                        col_name = col
                        competitor = name
                        break

            if not col_name:
                return {
                    'competitor': competitor,
                    'error': f'Competitor "{competitor}" not found. Valid competitors: {list(competitor_column_map.keys())}',
                    'source': 'competitive_pricing'
                }

            end_date = date.today()
            start_date = end_date - timedelta(days=months * 30)

            # Get the competitor column reference
            comp_col = getattr(CompetitivePricing, col_name)

            # Price gap expression: current - competitor (positive when competitor is cheaper)
            price_gap_expr = CompetitivePricing.current_price - comp_col

            # Single SQL query with monthly aggregation using strftime for SQLite
            # Groups by year-month and computes all metrics in one query
            results = self.db.query(
                func.strftime('%Y-%m', CompetitivePricing.pricing_date).label('month'),
                func.count(CompetitivePricing.id).label('total_skus'),
                func.count(comp_col).label('skus_with_comp'),
                func.sum(case(
                    (and_(comp_col.isnot(None), comp_col < CompetitivePricing.current_price), 1),
                    else_=0
                )).label('skus_undercut'),
                func.sum(case(
                    (and_(comp_col.isnot(None), comp_col < CompetitivePricing.current_price), price_gap_expr),
                    else_=0
                )).label('total_gap')
            ).filter(
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date
            ).group_by(
                func.strftime('%Y-%m', CompetitivePricing.pricing_date)
            ).order_by(
                func.strftime('%Y-%m', CompetitivePricing.pricing_date)
            ).all()

            # Build monthly series from SQL results
            monthly_series = []
            for row in results:
                month = row.month
                total_skus = row.total_skus or 0
                skus_tracked = row.skus_with_comp or 0
                skus_undercut = row.skus_undercut or 0
                total_gap = float(row.total_gap or 0)

                undercut_rate = (skus_undercut / skus_tracked * 100) if skus_tracked > 0 else 0
                avg_gap = (total_gap / skus_undercut) if skus_undercut > 0 else 0

                monthly_series.append({
                    'month': month,
                    'total_skus': total_skus,
                    'skus_tracked': skus_tracked,
                    'skus_undercut': skus_undercut,
                    'undercut_rate': round(undercut_rate, 2),
                    'avg_gap': round(avg_gap, 2),
                    'total_gap': round(total_gap, 2)
                })

            # Calculate trend summary
            if len(monthly_series) >= 2:
                first_month = monthly_series[0]
                last_month = monthly_series[-1]
                undercut_trend = last_month['skus_undercut'] - first_month['skus_undercut']
                rate_trend = last_month['undercut_rate'] - first_month['undercut_rate']
            else:
                undercut_trend = 0
                rate_trend = 0

            return {
                'competitor': competitor,
                'period': f'Last {months} months',
                'monthly_series': monthly_series,
                'summary': {
                    'total_months': len(monthly_series),
                    'earliest_month': monthly_series[0]['month'] if monthly_series else None,
                    'latest_month': monthly_series[-1]['month'] if monthly_series else None,
                    'undercut_trend': undercut_trend,
                    'rate_trend': round(rate_trend, 2),
                    'trend_direction': 'increasing' if undercut_trend > 0 else 'decreasing' if undercut_trend < 0 else 'stable'
                },
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting competitor trend: {str(e)}")
            return {'error': str(e), 'competitor': competitor, 'source': 'competitive_pricing'}

    def get_caprice_sku_pricing_trend(self, sku: str, days: int = 30) -> Dict[str, Any]:
        """
        Get pricing trend for a specific SKU over the past N days.

        Returns:
            - date_range: start and end dates covered
            - days_with_data: number of distinct dates with pricing data
            - current_price: min/avg/max over period
            - lowest_competitor_price: min/avg/max over period
            - minimum_price (floor): min/avg/max over period
            - profit_margin_pct: min/avg/max over period
            - recent_snapshots: last 5 dates with prices
        """
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)

            # Get all records for this SKU in the date range
            records = self.db.query(CompetitivePricing).filter(
                func.upper(CompetitivePricing.variant_sku) == sku.upper(),
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date
            ).order_by(CompetitivePricing.pricing_date.desc()).all()

            if not records:
                return {
                    'sku': sku,
                    'period': f'Last {days} days',
                    'error': f'No pricing data found for SKU {sku}',
                    'source': 'competitive_pricing'
                }

            # Get product info from most recent record
            latest = records[0]
            product_title = latest.title
            vendor = latest.vendor

            # Calculate aggregates using SQL for performance
            agg_result = self.db.query(
                func.count(CompetitivePricing.id).label('count'),
                func.min(CompetitivePricing.current_price).label('price_min'),
                func.avg(CompetitivePricing.current_price).label('price_avg'),
                func.max(CompetitivePricing.current_price).label('price_max'),
                func.min(CompetitivePricing.lowest_competitor_price).label('comp_min'),
                func.avg(CompetitivePricing.lowest_competitor_price).label('comp_avg'),
                func.max(CompetitivePricing.lowest_competitor_price).label('comp_max'),
                func.min(CompetitivePricing.minimum_price).label('floor_min'),
                func.avg(CompetitivePricing.minimum_price).label('floor_avg'),
                func.max(CompetitivePricing.minimum_price).label('floor_max'),
                func.min(CompetitivePricing.profit_margin_pct).label('margin_min'),
                func.avg(CompetitivePricing.profit_margin_pct).label('margin_avg'),
                func.max(CompetitivePricing.profit_margin_pct).label('margin_max'),
                func.min(CompetitivePricing.pricing_date).label('earliest_date'),
                func.max(CompetitivePricing.pricing_date).label('latest_date')
            ).filter(
                func.upper(CompetitivePricing.variant_sku) == sku.upper(),
                CompetitivePricing.pricing_date >= start_date,
                CompetitivePricing.pricing_date <= end_date
            ).first()

            # Build recent snapshots (last 5 dates)
            recent_snapshots = []
            for r in records[:5]:
                recent_snapshots.append({
                    'date': str(r.pricing_date),
                    'current_price': float(r.current_price) if r.current_price else None,
                    'lowest_competitor': float(r.lowest_competitor_price) if r.lowest_competitor_price else None,
                    'minimum_price': float(r.minimum_price) if r.minimum_price else None,
                    'margin_pct': float(r.profit_margin_pct) if r.profit_margin_pct else None
                })

            # Determine price stability
            prices = [float(r.current_price) for r in records if r.current_price]
            price_change = 0
            if len(prices) >= 2:
                price_change = prices[0] - prices[-1]  # Latest minus earliest

            return {
                'sku': sku,
                'title': product_title,
                'vendor': vendor,
                'period': f'Last {days} days',
                'date_range': {
                    'start': str(agg_result.earliest_date),
                    'end': str(agg_result.latest_date)
                },
                'days_with_data': agg_result.count,
                'current_price': {
                    'min': float(agg_result.price_min) if agg_result.price_min else None,
                    'avg': round(float(agg_result.price_avg), 2) if agg_result.price_avg else None,
                    'max': float(agg_result.price_max) if agg_result.price_max else None,
                    'change': round(price_change, 2)
                },
                'lowest_competitor_price': {
                    'min': float(agg_result.comp_min) if agg_result.comp_min else None,
                    'avg': round(float(agg_result.comp_avg), 2) if agg_result.comp_avg else None,
                    'max': float(agg_result.comp_max) if agg_result.comp_max else None
                },
                'minimum_price_floor': {
                    'min': float(agg_result.floor_min) if agg_result.floor_min else None,
                    'avg': round(float(agg_result.floor_avg), 2) if agg_result.floor_avg else None,
                    'max': float(agg_result.floor_max) if agg_result.floor_max else None
                },
                'profit_margin_pct': {
                    'min': round(float(agg_result.margin_min), 2) if agg_result.margin_min else None,
                    'avg': round(float(agg_result.margin_avg), 2) if agg_result.margin_avg else None,
                    'max': round(float(agg_result.margin_max), 2) if agg_result.margin_max else None
                },
                'recent_snapshots': recent_snapshots,
                'source': 'competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting SKU pricing trend: {str(e)}")
            return {'error': str(e), 'sku': sku, 'source': 'competitive_pricing'}

    # ==================== NETT MASTER SHEET / PRODUCT COST METHODS ====================

    def get_do_not_follow_skus(self, vendor: str = None, limit: int = 100) -> Dict[str, Any]:
        """
        Get SKUs marked as Do Not Follow (excluded from competitor matching).

        Args:
            vendor: Optional vendor/brand filter
            limit: Max results to return

        Returns:
            Dict with list of do_not_follow SKUs
        """
        try:
            query = self.db.query(
                ProductCost.vendor_sku,
                ProductCost.vendor,
                ProductCost.description,
                ProductCost.nett_nett_cost_inc_gst,
                ProductCost.minimum_price,
                ProductCost.rrp_inc_gst,
                ProductCost.comments
            ).filter(ProductCost.do_not_follow == True)

            if vendor:
                query = query.filter(func.lower(ProductCost.vendor) == vendor.lower())

            results = query.order_by(ProductCost.vendor, ProductCost.vendor_sku).limit(limit).all()

            skus = []
            for r in results:
                skus.append({
                    'sku': r.vendor_sku,
                    'vendor': r.vendor,
                    'description': r.description,
                    'nett_cost': float(r.nett_nett_cost_inc_gst) if r.nett_nett_cost_inc_gst else None,
                    'minimum_price': float(r.minimum_price) if r.minimum_price else None,
                    'rrp': float(r.rrp_inc_gst) if r.rrp_inc_gst else None,
                    'comments': r.comments
                })

            # Get count by vendor
            vendor_counts = self.db.query(
                ProductCost.vendor,
                func.count(ProductCost.id).label('count')
            ).filter(
                ProductCost.do_not_follow == True
            ).group_by(ProductCost.vendor).all()

            return {
                'total_do_not_follow': len(skus),
                'skus': skus,
                'by_vendor': {v.vendor: v.count for v in vendor_counts if v.vendor},
                'filter': {'vendor': vendor} if vendor else None,
                'source': 'product_costs'
            }

        except Exception as e:
            log.error(f"Error getting do_not_follow SKUs: {str(e)}")
            return {'error': str(e), 'source': 'product_costs'}

    def get_set_price_skus(self, vendor: str = None, limit: int = 100) -> Dict[str, Any]:
        """
        Get SKUs with Set Price (fixed price, ignore competitor matching).
        Includes margin calculation at set price.

        Args:
            vendor: Optional vendor/brand filter
            limit: Max results to return

        Returns:
            Dict with list of set_price SKUs and their margins
        """
        try:
            query = self.db.query(
                ProductCost.vendor_sku,
                ProductCost.vendor,
                ProductCost.description,
                ProductCost.set_price,
                ProductCost.nett_nett_cost_inc_gst,
                ProductCost.minimum_price,
                ProductCost.rrp_inc_gst,
                ProductCost.min_margin_pct,
                ProductCost.comments
            ).filter(ProductCost.set_price.isnot(None))

            if vendor:
                query = query.filter(func.lower(ProductCost.vendor) == vendor.lower())

            results = query.order_by(ProductCost.vendor, ProductCost.vendor_sku).limit(limit).all()

            skus = []
            for r in results:
                set_price = float(r.set_price) if r.set_price else 0
                nett_cost = float(r.nett_nett_cost_inc_gst) if r.nett_nett_cost_inc_gst else 0

                # Calculate margin at set price: (price - cost) / price * 100
                margin_at_set_price = ((set_price - nett_cost) / set_price * 100) if set_price > 0 else 0
                min_margin = float(r.min_margin_pct) if r.min_margin_pct else None

                skus.append({
                    'sku': r.vendor_sku,
                    'vendor': r.vendor,
                    'description': r.description,
                    'set_price': set_price,
                    'nett_cost': nett_cost,
                    'margin_at_set_price': round(margin_at_set_price, 2),
                    'min_margin_required': min_margin,
                    'margin_ok': margin_at_set_price >= min_margin if min_margin else True,
                    'minimum_price': float(r.minimum_price) if r.minimum_price else None,
                    'rrp': float(r.rrp_inc_gst) if r.rrp_inc_gst else None,
                    'comments': r.comments
                })

            # Summary stats
            margins = [s['margin_at_set_price'] for s in skus if s['margin_at_set_price']]
            below_min_margin = [s for s in skus if not s['margin_ok']]

            return {
                'total_set_price': len(skus),
                'skus': skus,
                'summary': {
                    'avg_margin': round(sum(margins) / len(margins), 2) if margins else 0,
                    'min_margin': round(min(margins), 2) if margins else 0,
                    'max_margin': round(max(margins), 2) if margins else 0,
                    'below_min_margin_count': len(below_min_margin)
                },
                'below_min_margin': below_min_margin[:20],
                'filter': {'vendor': vendor} if vendor else None,
                'source': 'product_costs'
            }

        except Exception as e:
            log.error(f"Error getting set_price SKUs: {str(e)}")
            return {'error': str(e), 'source': 'product_costs'}

    def get_unmatchable_skus_by_brand(self, vendor: str = None, limit: int = 100) -> Dict[str, Any]:
        """
        Get SKUs where competitor price < our floor price (unmatchable).
        Joins ProductCost with CompetitivePricing to find gaps.

        Args:
            vendor: Optional vendor/brand filter
            limit: Max results to return

        Returns:
            Dict with unmatchable SKUs grouped by brand
        """
        try:
            # Get latest pricing date
            latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()
            if not latest_date:
                return {'error': 'No pricing data available', 'source': 'product_costs+competitive_pricing'}

            # Base join (case-insensitive match via UPPER - use indexed expression)
            base_query = self.db.query(
                ProductCost.vendor_sku,
                ProductCost.vendor,
                ProductCost.description,
                ProductCost.minimum_price,
                ProductCost.nett_nett_cost_inc_gst,
                CompetitivePricing.lowest_competitor_price,
                CompetitivePricing.current_price,
                (ProductCost.minimum_price - CompetitivePricing.lowest_competitor_price).label('gap')
            ).join(
                CompetitivePricing,
                func.upper(ProductCost.vendor_sku) == func.upper(CompetitivePricing.variant_sku)
            ).filter(
                CompetitivePricing.pricing_date == latest_date,
                ProductCost.minimum_price.isnot(None),
                CompetitivePricing.lowest_competitor_price.isnot(None),
                CompetitivePricing.lowest_competitor_price < ProductCost.minimum_price
            )

            if vendor:
                base_query = base_query.filter(func.lower(ProductCost.vendor) == vendor.lower())

            total_unmatchable = base_query.with_entities(func.count()).scalar() or 0

            # Vendor summary via aggregate query (fast)
            vendor_summary_rows = base_query.with_entities(
                ProductCost.vendor.label('vendor'),
                func.count().label('count'),
                func.avg(ProductCost.minimum_price - CompetitivePricing.lowest_competitor_price).label('avg_gap')
            ).group_by(ProductCost.vendor).all()

            vendor_summary = {}
            for row in vendor_summary_rows:
                v = row.vendor or 'Unknown'
                vendor_summary[v] = {
                    'count': int(row.count),
                    'avg_gap': float(row.avg_gap) if row.avg_gap is not None else 0
                }

            results = []
            if limit and limit > 0:
                results = base_query.order_by(desc('gap')).limit(limit).all()

            skus = [
                {
                    'sku': r.vendor_sku,
                    'vendor': r.vendor,
                    'description': r.description,
                    'our_floor': float(r.minimum_price) if r.minimum_price else None,
                    'competitor_price': float(r.lowest_competitor_price) if r.lowest_competitor_price else None,
                    'gap': float(r.gap) if r.gap else 0,
                    'our_current_price': float(r.current_price) if r.current_price else None,
                    'nett_cost': float(r.nett_nett_cost_inc_gst) if r.nett_nett_cost_inc_gst else None
                }
                for r in results
            ]

            return {
                'total_unmatchable': total_unmatchable,
                'pricing_date': str(latest_date),
                'skus': skus,
                'by_vendor': vendor_summary,
                'filter': {'vendor': vendor} if vendor else None,
                'source': 'product_costs+competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting unmatchable SKUs: {str(e)}")
            return {'error': str(e), 'source': 'product_costs+competitive_pricing'}

    def get_brand_cost_summary(self, vendor: str = None) -> Dict[str, Any]:
        """
        Get brand/vendor summary from NETT Master Sheet data.
        Shows: total SKUs, avg cost, avg margin, undercut stats.

        Args:
            vendor: Optional specific vendor filter

        Returns:
            Dict with brand summary statistics
        """
        try:
            # Base query for product costs by vendor
            base_query = self.db.query(
                ProductCost.vendor,
                func.count(ProductCost.id).label('total_skus'),
                func.avg(ProductCost.nett_nett_cost_inc_gst).label('avg_nett_cost'),
                func.avg(ProductCost.minimum_price).label('avg_floor'),
                func.avg(ProductCost.rrp_inc_gst).label('avg_rrp'),
                func.avg(ProductCost.min_margin_pct).label('avg_min_margin'),
                func.sum(case((ProductCost.do_not_follow == True, 1), else_=0)).label('do_not_follow_count'),
                func.sum(case((ProductCost.set_price.isnot(None), 1), else_=0)).label('set_price_count')
            ).filter(ProductCost.vendor.isnot(None))

            if vendor:
                base_query = base_query.filter(func.lower(ProductCost.vendor) == vendor.lower())

            results = base_query.group_by(ProductCost.vendor).order_by(desc('total_skus')).all()

            # Get latest competitive pricing date for undercut stats
            latest_date = self.db.query(func.max(CompetitivePricing.pricing_date)).scalar()

            brands = []
            for r in results:
                brand_data = {
                    'vendor': r.vendor,
                    'total_skus': r.total_skus,
                    'avg_nett_cost': round(float(r.avg_nett_cost), 2) if r.avg_nett_cost else None,
                    'avg_floor_price': round(float(r.avg_floor), 2) if r.avg_floor else None,
                    'avg_rrp': round(float(r.avg_rrp), 2) if r.avg_rrp else None,
                    'avg_min_margin_pct': round(float(r.avg_min_margin), 2) if r.avg_min_margin else None,
                    'do_not_follow_count': r.do_not_follow_count or 0,
                    'set_price_count': r.set_price_count or 0
                }

                # Get undercut stats for this brand if we have pricing data
                if latest_date:
                    undercut_query = self.db.query(
                        func.count(CompetitivePricing.id).label('total_priced'),
                        func.sum(case(
                            (CompetitivePricing.lowest_competitor_price < CompetitivePricing.minimum_price, 1),
                            else_=0
                        )).label('undercut_count'),
                        func.avg(
                            case(
                                (CompetitivePricing.lowest_competitor_price < CompetitivePricing.minimum_price,
                                 CompetitivePricing.minimum_price - CompetitivePricing.lowest_competitor_price),
                                else_=None
                            )
                        ).label('avg_undercut_gap')
                    ).filter(
                        CompetitivePricing.pricing_date == latest_date,
                        func.lower(CompetitivePricing.vendor) == r.vendor.lower() if r.vendor else False
                    ).first()

                    if undercut_query and undercut_query.total_priced:
                        brand_data['total_priced'] = undercut_query.total_priced
                        brand_data['undercut_count'] = undercut_query.undercut_count or 0
                        brand_data['undercut_pct'] = round(
                            (undercut_query.undercut_count or 0) / undercut_query.total_priced * 100, 1
                        )
                        brand_data['avg_undercut_gap'] = round(float(undercut_query.avg_undercut_gap), 2) if undercut_query.avg_undercut_gap else 0

                brands.append(brand_data)

            # Sort by undercut percentage (most undercut first)
            brands.sort(key=lambda x: x.get('undercut_pct', 0), reverse=True)

            # Overall summary
            total_skus = sum(b['total_skus'] for b in brands)
            total_undercut = sum(b.get('undercut_count', 0) for b in brands)

            return {
                'total_brands': len(brands),
                'total_skus': total_skus,
                'total_undercut': total_undercut,
                'pricing_date': str(latest_date) if latest_date else None,
                'brands': brands,
                'filter': {'vendor': vendor} if vendor else None,
                'source': 'product_costs+competitive_pricing'
            }

        except Exception as e:
            log.error(f"Error getting brand cost summary: {str(e)}")
            return {'error': str(e), 'source': 'product_costs'}

    def get_sku_cost_details(self, sku: str) -> Dict[str, Any]:
        """
        Get full cost details for a specific SKU from NETT Master Sheet.

        Args:
            sku: The SKU to look up

        Returns:
            Dict with all cost/pricing details for the SKU
        """
        try:
            result = self.db.query(ProductCost).filter(
                func.upper(ProductCost.vendor_sku) == sku.upper()
            ).first()

            if not result:
                return {'error': f'SKU {sku} not found in product costs', 'sku': sku, 'source': 'product_costs'}

            return {
                'sku': result.vendor_sku,
                'vendor': result.vendor,
                'description': result.description,
                'item_category': result.item_category,
                'ean': result.ean,
                'pricing': {
                    'nett_nett_cost_inc_gst': float(result.nett_nett_cost_inc_gst) if result.nett_nett_cost_inc_gst else None,
                    'rrp_inc_gst': float(result.rrp_inc_gst) if result.rrp_inc_gst else None,
                    'invoice_price_inc_gst': float(result.invoice_price_inc_gst) if result.invoice_price_inc_gst else None,
                    'minimum_price': float(result.minimum_price) if result.minimum_price else None,
                    'set_price': float(result.set_price) if result.set_price else None
                },
                'discounts': {
                    'discount': float(result.discount) if result.discount else None,
                    'additional_discount': float(result.additional_discount) if result.additional_discount else None,
                    'extra_discount': float(result.extra_discount) if result.extra_discount else None,
                    'rebate': float(result.rebate) if result.rebate else None,
                    'settlement': float(result.settlement) if result.settlement else None,
                    'crf': float(result.crf) if result.crf else None,
                    'loyalty': float(result.loyalty) if result.loyalty else None,
                    'advertising': float(result.advertising) if result.advertising else None
                },
                'margins': {
                    'min_margin_pct': float(result.min_margin_pct) if result.min_margin_pct else None,
                    'discount_off_rrp_pct': float(result.discount_off_rrp_pct) if result.discount_off_rrp_pct else None
                },
                'flags': {
                    'do_not_follow': result.do_not_follow or False,
                    'gst_free': result.gst_free or False
                },
                'comments': result.comments,
                'last_synced': result.last_synced.isoformat() if result.last_synced else None,
                'source': 'product_costs'
            }

        except Exception as e:
            log.error(f"Error getting SKU cost details: {str(e)}")
            return {'error': str(e), 'sku': sku, 'source': 'product_costs'}

    def get_context_for_question(self, question: str) -> Dict[str, Any]:
        """
        Get appropriate context based on the question.
        ALL DATA COMES FROM DATABASE.
        Applies date filters to all queries when a time period is specified.
        """
        context = {
            'data_source': 'DATABASE',
            'database_stats': self.get_database_stats()
        }

        question_lower = question.lower()

        # Parse date range from question
        date_range = self.parse_date_range(question)
        start_date = None
        end_date = None

        if date_range:
            start_date, end_date, period_desc = date_range
            context['date_filter'] = {
                'start_date': str(start_date),
                'end_date': str(end_date),
                'description': period_desc
            }
            # Get revenue for the specific period
            context['requested_period'] = self.get_revenue_by_date_range(start_date, end_date)
            context['requested_period']['period'] = period_desc

        # Check for year comparison (e.g., "2024 vs 2025")
        year_comparison = self.parse_year_comparison(question)
        if year_comparison:
            p1, p2 = year_comparison['period1'], year_comparison['period2']
            context['year_comparison'] = self.compare_periods(
                p1['start'], p1['end'],
                p2['start'], p2['end'],
                p1['label'], p2['label']
            )

        # ==================== SHOPIFY / COMMERCE QUESTIONS ====================
        # Determine date range or default window for commerce queries
        default_days = self.parse_time_period(question) or 30
        if not start_date or not end_date:
            end_date = date.today()
            start_date = end_date - timedelta(days=default_days)

        is_sales_channel_question = any(phrase in question_lower for phrase in [
            'sales by channel', 'sales channel', 'shop app', 'online store', 'channel performance'
        ])
        if is_sales_channel_question:
            context['SALES_BY_CHANNEL'] = self.get_sales_by_channel(start_date, end_date)
            context['SALES_CHANNEL_INSTRUCTIONS'] = (
                "Use SALES_BY_CHANNEL ONLY. Do NOT include GA4 traffic source attribution or GA4_CHANNEL_REVENUE data. "
                "Ignore requested_period totals if present."
            )
            # Avoid conflicting totals from requested_period for this question type
            context.pop('requested_period', None)

        is_order_status_question = any(phrase in question_lower for phrase in [
            'unfulfilled', 'partially fulfilled', 'cancelled', 'canceled',
            'fulfillment rate', 'fulfilled orders', 'order fulfillment'
        ])
        if is_order_status_question:
            context['ORDER_STATUS'] = self.get_order_status_summary(start_date, end_date)

        is_discount_question = any(phrase in question_lower for phrase in [
            'discount', 'discounts', 'discount code', 'promo code', 'coupon'
        ])
        if is_discount_question:
            context['DISCOUNTS'] = self.get_discount_summary(start_date, end_date)

        is_returns_question = any(phrase in question_lower for phrase in [
            'return', 'returns', 'refunded items', 'refunds by product', 'returns by product'
        ])
        if is_returns_question:
            if any(phrase in question_lower for phrase in ['product type', 'category', 'product_type']):
                context['RETURNS_BY_CATEGORY'] = self.get_returns_by_product_type(start_date, end_date)
            else:
                context['RETURNS_BY_PRODUCT'] = self.get_returns_by_product(start_date, end_date)

        is_shipping_tax_question = any(phrase in question_lower for phrase in [
            'shipping', 'tax', 'shipping charges', 'tax collections'
        ])
        if is_shipping_tax_question:
            context['SHIPPING_TAX'] = self.get_shipping_tax_trends(days=default_days)

        is_product_variant_question = any(phrase in question_lower for phrase in [
            'variant', 'variants', 'most popular variants'
        ])
        if is_product_variant_question:
            context['TOP_VARIANTS'] = self.get_product_variant_popularity(start_date, end_date, limit=10)

        is_low_sales_question = any(phrase in question_lower for phrase in [
            'lowest sales', 'not selling', 'low selling', 'least selling'
        ])
        if is_low_sales_question:
            context['LOW_SELLING_PRODUCTS'] = self.get_low_selling_products(start_date, end_date, limit=10)

        # ==================== BRAND SALES QUESTIONS ====================
        # "Franke sales last 30 days", "top brands by revenue", "brand sales"
        is_brand_sales_question = any(phrase in question_lower for phrase in [
            'brand sales', 'sales for brand', 'sales by brand', 'top brands',
            'brands by revenue', 'brand revenue', 'vendor sales', 'vendor revenue'
        ])

        # Also detect "<brand_name> sales" pattern (e.g., "Franke sales", "Zip sales")
        brand_name_match = None
        brand_patterns = [
            r'(?:what|how)\s+(?:have|has|are|were)\s+(?:our\s+)?sales\s+(?:been\s+)?(?:like\s+)?for\s+(?:the\s+)?(?:brand\s+)?([A-Za-z][A-Za-z0-9\s]{1,20}?)(?:\s+in|\s+over|\s+last|\s+past|\?|$)',
            r'([A-Za-z][A-Za-z0-9]{2,15})\s+sales\s+(?:in\s+)?(?:the\s+)?(?:last|past)',
            r'sales\s+(?:for|of)\s+([A-Za-z][A-Za-z0-9\s]{1,20}?)(?:\s+in|\s+over|\s+last|\s+past|\?|$)',
            r'(?:top|best)\s+(?:\d+\s+)?(?:selling\s+)?([A-Za-z][A-Za-z0-9]{2,15})\s+(?:products?|skus?|items?)',
        ]
        for pattern in brand_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                brand_name_match = match.group(1).strip()
                # Exclude common words that aren't brand names
                if brand_name_match.lower() not in ['the', 'our', 'your', 'all', 'top', 'best', 'total']:
                    is_brand_sales_question = True
                    break
                else:
                    brand_name_match = None

        if is_brand_sales_question:
            context['BRAND_SALES'] = self.get_brand_sales(
                start_date=start_date,
                end_date=end_date,
                brand=brand_name_match,
                limit=10
            )
            if brand_name_match:
                context['BRAND_SALES_INSTRUCTIONS'] = (
                    f"Use BRAND_SALES ONLY for this query about brand '{brand_name_match}'. "
                    f"Report: revenue, units sold, order count, and top SKUs for this brand. "
                    f"The vendor is sourced from NETT master (product_costs) with fallback to Shopify."
                )
            else:
                context['BRAND_SALES_INSTRUCTIONS'] = (
                    "Use BRAND_SALES ONLY for this query. "
                    "Report: top brands by revenue with revenue_pct, units, and orders. "
                    "The vendor is sourced from NETT master (product_costs) with fallback to Shopify."
                )

        is_top_customers_question = any(phrase in question_lower for phrase in [
            'top customers', 'best customers', 'highest spending customers'
        ])
        if is_top_customers_question:
            context['TOP_CUSTOMERS'] = self.get_top_customers(limit=10)

        is_new_returning_question = any(phrase in question_lower for phrase in [
            'new vs returning', 'new customers', 'returning customers'
        ])
        if is_new_returning_question:
            context['CUSTOMER_TYPES'] = self.get_new_vs_returning_customers(start_date, end_date)

        is_inactive_customers_question = any(phrase in question_lower for phrase in [
            'haven\'t purchased', 'not purchased', 'inactive customers', 'lapsed customers'
        ])
        if is_inactive_customers_question:
            context['INACTIVE_CUSTOMERS'] = self.get_inactive_customers(days=default_days, limit=20)

        is_customer_geo_question = any(phrase in question_lower for phrase in [
            'customers from', 'customers by city', 'customers by region', 'customer locations'
        ])
        if is_customer_geo_question:
            context['CUSTOMER_GEO'] = self.get_customer_geo_breakdown(limit=10)

        is_retention_question = any(phrase in question_lower for phrase in [
            'retention rate', 'customer retention'
        ])
        if is_retention_question:
            context['CUSTOMER_RETENTION'] = self.get_customer_retention_rate(start_date, end_date)

        is_inventory_question = any(phrase in question_lower for phrase in [
            'inventory', 'stock', 'out of stock', 'low stock'
        ])
        if is_inventory_question:
            context['INVENTORY_STATUS'] = self.get_inventory_status(threshold=5, limit=20)
            if 'value' in question_lower:
                context['INVENTORY_VALUE'] = self.get_inventory_value_by_vendor(limit=10)
            if 'turnover' in question_lower:
                context['INVENTORY_TURNOVER'] = self.get_inventory_turnover(days=default_days)

        # Normalize Unicode hyphens to ASCII for consistent detection
        # Handles: U+2010 HYPHEN, U+2011 NON-BREAKING HYPHEN, U+2012 FIGURE DASH,
        # U+2013 EN DASH, U+2014 EM DASH, U+2015 HORIZONTAL BAR, U+2212 MINUS SIGN
        normalized_question = question_lower
        for unicode_hyphen in '\u2010\u2011\u2012\u2013\u2014\u2015\u2212':
            normalized_question = normalized_question.replace(unicode_hyphen, '-')

        # Detect SEO/Search Console questions
        is_seo_question = any(word in normalized_question for word in [
            'seo', 'search', 'query', 'queries', 'keyword', 'keywords',
            'organic', 'ranking', 'position', 'impressions', 'ctr',
            'click-through', 'clicks', 'search console', 'google search',
            'non-brand', 'non brand', 'branded', 'brand terms'
        ])

        if is_seo_question:
            # Parse days from question, default to 28
            days = self.parse_time_period(question) or 28

            # Parse limit from question (e.g., "top 10", "top 5")
            limit_match = re.search(r'top\s+(\d+)', question_lower)
            limit = int(limit_match.group(1)) if limit_match else 10

            # Determine query type: brand-only, non-brand, or all
            is_brand_only_question = any(phrase in normalized_question for phrase in [
                'brand queries', 'brand keywords', 'branded queries', 'branded keywords',
                'brand terms', 'brand search', 'branded search', 'our brand',
                'show brand', 'top brand', 'brand performance'
            ]) and 'non-brand' not in normalized_question and 'non brand' not in normalized_question

            is_nonbrand_question = any(phrase in normalized_question for phrase in [
                'non-brand', 'non brand', 'nonbrand', 'excluding brand',
                'without brand', 'generic queries', 'generic keywords'
            ])

            # Default: exclude brand unless specifically asking for brand queries
            exclude_brand = not is_brand_only_question

            # Initialize instructions
            context['SEARCH_CONSOLE_STATS'] = self.get_search_console_stats()
            context['SEARCH_CONSOLE_INSTRUCTIONS'] = ""

            # BRAND-ONLY QUERIES
            if is_brand_only_question:
                context['SEARCH_CONSOLE_BRAND'] = self.get_search_console_queries_brand_only(
                    days=days,
                    limit=limit,
                    order_by='clicks'
                )
                context['SEARCH_CONSOLE_INSTRUCTIONS'] = (
                    f"USE ONLY the SEARCH_CONSOLE_BRAND data for this question. "
                    f"This contains ONLY queries matching brand terms: {context['SEARCH_CONSOLE_BRAND'].get('brand_terms', [])}. "
                    f"Data is for the last {days} days."
                )
            else:
                # Get filtered Search Console data (non-brand by default)
                context['SEARCH_CONSOLE_QUERIES'] = self.get_search_console_queries_filtered(
                    days=days,
                    exclude_brand=exclude_brand,
                    limit=limit,
                    order_by='clicks'
                )
                context['SEARCH_CONSOLE_INSTRUCTIONS'] = (
                    f"Use the SEARCH_CONSOLE_QUERIES data to answer. "
                    f"Data is filtered for the last {days} days. "
                    f"{'Brand terms excluded: ' + str(context['SEARCH_CONSOLE_QUERIES'].get('excluded_terms', [])) if exclude_brand else 'Includes all queries including brand.'}"
                )

            # Also include top pages if relevant
            if 'page' in question_lower or 'url' in question_lower:
                context['SEARCH_CONSOLE_PAGES'] = self.get_search_console_pages_filtered(
                    days=days,
                    limit=limit
                )

            # Detect OPPORTUNITY questions (positions 8-15, page 2)
            is_opportunity_question = any(phrase in normalized_question for phrase in [
                'opportunity', 'opportunities', 'position 8', 'positions 8',
                'page 2', 'page two', 'close to page 1', 'almost ranking',
                'could rank', 'potential', 'low hanging', 'low-hanging',
                '8-15', '8 to 15', '8 through 15'
            ])

            if is_opportunity_question:
                context['SEARCH_CONSOLE_OPPORTUNITIES'] = self.get_search_console_queries_opportunities(
                    days=days,
                    min_impressions=1000,
                    pos_min=8.0,
                    pos_max=15.0,
                    exclude_brand=exclude_brand,
                    limit=limit
                )
                context['SEARCH_CONSOLE_INSTRUCTIONS'] += (
                    f" USE SEARCH_CONSOLE_OPPORTUNITIES for this question - it contains queries "
                    f"ranking positions 8-15 with 1000+ impressions. These are page 2 opportunities."
                )

            # Detect LOW CTR questions (e.g., "CTR < 1%", "low CTR", "impressions > 5000")
            is_low_ctr_question = any(phrase in normalized_question for phrase in [
                'low ctr', 'poor ctr', 'ctr under', 'ctr below', 'ctr less than',
                'ctr <', 'ctr<',  # Direct comparison patterns
                'impressions but', 'high impressions low', 'visibility but',
                'not clicking', 'under 1%', 'under 2%', 'below 1%', 'below 2%',
                'impressions >', 'impressions>',  # Impressions threshold patterns
                'low ctr queries', 'queries with low ctr'
            ])

            if is_low_ctr_question:
                # Parse CTR threshold from question (default 1%)
                # Match patterns like "CTR < 1%", "CTR under 1%", "under 1% CTR", "ctr < 2%"
                ctr_match = re.search(r'ctr\s*[<≤]\s*(\d+(?:\.\d+)?)\s*%?', normalized_question)
                if not ctr_match:
                    ctr_match = re.search(r'(?:under|below|less than)\s*(\d+(?:\.\d+)?)\s*%', normalized_question)
                ctr_threshold = float(ctr_match.group(1)) if ctr_match else 1.0

                # Parse min impressions from question (default 5000 for low CTR queries)
                # Match patterns like "impressions > 5,000", "impressions > 5000", "> 10000 impressions"
                impressions_match = re.search(r'impressions?\s*[>≥]\s*([\d,]+)', normalized_question)
                if not impressions_match:
                    impressions_match = re.search(r'[>≥]\s*([\d,]+)\s*impressions?', normalized_question)
                if impressions_match:
                    min_impressions = int(impressions_match.group(1).replace(',', ''))
                else:
                    min_impressions = 5000  # Default for low CTR questions

                context['LOW_CTR_QUERIES'] = self.get_low_ctr_high_impression_queries(
                    days=days,
                    ctr_threshold=ctr_threshold,
                    min_impressions=min_impressions,
                    exclude_brand=exclude_brand,
                    limit=limit
                )
                context['SEARCH_CONSOLE_INSTRUCTIONS'] = (
                    f"CRITICAL: For this LOW CTR question, use ONLY the LOW_CTR_QUERIES dataset. "
                    f"This contains queries with CTR < {ctr_threshold}% AND impressions >= {min_impressions:,}. "
                    f"Queries are sorted by impressions (highest first). "
                    f"DO NOT use SEARCH_CONSOLE_QUERIES (top clicks) for low CTR questions."
                )

            # Detect WEEK-OVER-WEEK / CTR comparison questions
            is_ctr_comparison_question = any(phrase in normalized_question for phrase in [
                'ctr gains', 'ctr improvements', 'ctr change', 'improved ctr',
                'ctr week over week', 'ctr week-over-week', 'ctr wow',
                'biggest ctr', 'ctr gainers', 'ctr losers'
            ])

            is_click_comparison_question = any(phrase in normalized_question for phrase in [
                'click gains', 'click change', 'click week over week',
                'clicks week-over-week', 'click gainers', 'click losers',
                'traffic change', 'traffic gains'
            ])

            is_general_comparison_question = any(phrase in normalized_question for phrase in [
                'week over week', 'week-over-week', 'wow', 'compared to last',
                'vs last week', 'versus last week', 'change from last',
                'biggest gainers', 'biggest losers', 'trending up', 'trending down',
                'period over period', 'this week vs', 'compared to previous'
            ])

            if is_ctr_comparison_question or is_click_comparison_question or is_general_comparison_question:
                # Default to 7 days for week-over-week
                comparison_days = 7 if 'week' in normalized_question else days

                context['SEARCH_CONSOLE_WOW'] = self.get_search_console_week_over_week(
                    current_days=comparison_days,
                    exclude_brand=exclude_brand,
                    limit=limit
                )

                # Explicit instructions based on question type
                if is_ctr_comparison_question:
                    context['SEARCH_CONSOLE_INSTRUCTIONS'] += (
                        f" CRITICAL: For CTR changes, use ONLY ctr_gainers and ctr_losers from SEARCH_CONSOLE_WOW. "
                        f"These are filtered for meaningful CTR changes (previous_ctr > 0, min 50 impressions both periods). "
                        f"DO NOT use click_gainers for CTR questions."
                    )
                elif is_click_comparison_question:
                    context['SEARCH_CONSOLE_INSTRUCTIONS'] += (
                        f" For click changes, use click_gainers and click_losers from SEARCH_CONSOLE_WOW."
                    )
                else:
                    context['SEARCH_CONSOLE_INSTRUCTIONS'] += (
                        f" SEARCH_CONSOLE_WOW contains period-over-period comparison. "
                        f"Use ctr_gainers/ctr_losers for CTR changes, click_gainers/click_losers for click changes."
                    )

            # Detect PAGE LOSS questions
            is_page_loss_question = any(phrase in normalized_question for phrase in [
                'page loss', 'pages lost', 'lost clicks', 'losing clicks',
                'page decline', 'pages declining', 'page week over week',
                'pages week-over-week', 'page wow', 'which pages lost',
                'pages that lost', 'url losses', 'urls lost'
            ])

            if is_page_loss_question:
                comparison_days = 7 if 'week' in normalized_question else days

                context['SEARCH_CONSOLE_PAGES_WOW'] = self.get_search_console_pages_week_over_week(
                    current_days=comparison_days,
                    limit=limit
                )
                context['SEARCH_CONSOLE_INSTRUCTIONS'] += (
                    f" USE SEARCH_CONSOLE_PAGES_WOW for page-level changes. "
                    f"click_losers shows pages that lost the most clicks vs prior period."
                )

        # ==================== GA4 QUESTION DETECTION ====================
        # Detect GA4/Analytics questions and provide appropriate data
        is_ga4_question = any(word in normalized_question for word in [
            'sessions', 'users', 'pageviews', 'page views', 'bounce rate',
            'analytics', 'ga4', 'google analytics', 'traffic',
            'channels', 'channel', 'landing page', 'landing pages',
            'device', 'devices', 'mobile', 'desktop', 'tablet',
            'geo', 'geography', 'country', 'countries', 'region',
            'conversions', 'conversion rate', 'ecommerce'
        ]) and not is_seo_question  # Don't overlap with SEO questions

        if is_ga4_question:
            # Parse days from question, default to 28
            days = self.parse_time_period(question) or 28

            # Parse limit from question
            limit_match = re.search(r'top\s+(\d+)', question_lower)
            limit = int(limit_match.group(1)) if limit_match else 15

            context['GA4_INSTRUCTIONS'] = (
                "IMPORTANT: Use the GA4_* context blocks below for analytics questions. "
                "This data comes from Google Analytics 4 and is the authoritative source for "
                "sessions, users, pageviews, conversions, and channel performance. "
                "Do NOT use Shopify order data for traffic/session questions."
            )

            # Sessions/users/revenue questions → daily summary + ecommerce
            if any(word in normalized_question for word in ['sessions', 'users', 'pageviews', 'traffic overview', 'daily']):
                context['GA4_DAILY_SUMMARY'] = self.get_ga4_daily_summary(days=days)
                context['GA4_ECOMMERCE'] = self.get_ga4_ecommerce_summary(days=days)

            # Channel/source/medium revenue questions
            if any(word in normalized_question for word in ['channel', 'channels', 'source', 'medium', 'drove revenue', 'drove most']):
                context['GA4_CHANNEL_REVENUE'] = self.get_ga4_channel_revenue(days=days, limit=limit)

            # Product pages with sessions/conversion questions
            # Detect: "product" + ("sessions" or "conversion rate" or "traffic")
            is_product_session_question = (
                any(word in normalized_question for word in ['product', 'products', 'sku']) and
                any(word in normalized_question for word in ['sessions', 'conversion', 'traffic', 'converting'])
            )

            if is_product_session_question:
                # Determine ordering based on question
                is_low_conversion = any(phrase in normalized_question for phrase in [
                    'lowest conversion', 'low conversion', 'worst conversion',
                    'not converting', 'poor conversion', 'highest sessions and lowest',
                    'most sessions but low', 'high sessions low conversion'
                ])
                is_high_conversion = any(phrase in normalized_question for phrase in [
                    'highest conversion', 'best conversion', 'top conversion',
                    'most converting', 'good conversion'
                ])

                if is_low_conversion or 'highest sessions' in normalized_question:
                    order_by = 'high_sessions_low_conversion'
                elif is_high_conversion:
                    order_by = 'conversion_rate_desc'
                else:
                    order_by = 'sessions'

                context['GA4_PRODUCT_PAGES'] = self.get_ga4_top_landing_pages(
                    days=days,
                    limit=limit,
                    order_by=order_by,
                    product_only=True,
                    min_sessions=10  # Filter out low-traffic products for meaningful conversion rates
                )
                context['GA4_PRODUCT_INSTRUCTIONS'] = (
                    "CRITICAL: For product session/conversion questions, use GA4_PRODUCT_PAGES. "
                    "This contains ONLY /products/* URLs from GA4 landing pages. "
                    "conversion_rate_pct is pre-computed (e.g., 0.11 means 0.11%). "
                    "Format: /products/product-name — X sessions, Y conversions, Z% conversion rate"
                )

            # Landing pages questions
            if any(word in normalized_question for word in ['landing page', 'landing pages', 'entry page']):
                # Detect conversion rate ordering requests
                is_low_conversion = any(phrase in normalized_question for phrase in [
                    'low conversion', 'bottom conversion', 'worst conversion',
                    'lowest conversion', 'poor conversion', 'low conversions',
                    'high sessions but low', 'high traffic but low'
                ])
                is_high_conversion = any(phrase in normalized_question for phrase in [
                    'high conversion', 'best conversion', 'top conversion',
                    'highest conversion', 'good conversion'
                ])

                if is_low_conversion:
                    order_by = 'conversion_rate_asc'
                elif is_high_conversion:
                    order_by = 'conversion_rate_desc'
                else:
                    order_by = 'sessions'

                context['GA4_LANDING_PAGES'] = self.get_ga4_top_landing_pages(
                    days=days, limit=limit, order_by=order_by
                )
                context['GA4_LANDING_PAGES_INSTRUCTIONS'] = (
                    "CRITICAL: Use conversion_rate_pct EXACTLY as provided. Do NOT recalculate. "
                    "The value is already a percentage (e.g., 0.11 means 0.11%, not 0.11). "
                    "Format as: /page — X sessions, Y conversions, Z% conversion rate"
                )

            # Top pages by pageviews
            if any(word in normalized_question for word in ['top pages', 'page views', 'pageviews', 'most viewed']):
                context['GA4_TOP_PAGES'] = self.get_ga4_top_pages(days=days, limit=limit)

            # Device breakdown
            if any(word in normalized_question for word in ['device', 'devices', 'mobile', 'desktop', 'tablet']):
                context['GA4_DEVICE_BREAKDOWN'] = self.get_ga4_device_breakdown(days=days)

            # Geo breakdown
            if any(word in normalized_question for word in ['geo', 'geography', 'country', 'countries', 'region', 'location']):
                context['GA4_GEO_REVENUE'] = self.get_ga4_geo_revenue(days=days, limit=limit)

            # E-commerce summary (purchases, cart, checkout)
            if any(word in normalized_question for word in ['ecommerce', 'e-commerce', 'purchases', 'cart', 'checkout', 'add to cart']):
                context['GA4_ECOMMERCE'] = self.get_ga4_ecommerce_summary(days=days)

            # General revenue question from GA4 perspective
            if 'revenue' in normalized_question and 'channel' not in normalized_question and 'geo' not in normalized_question:
                # Include daily summary for general revenue questions
                if 'GA4_DAILY_SUMMARY' not in context:
                    context['GA4_DAILY_SUMMARY'] = self.get_ga4_daily_summary(days=days)
                if 'GA4_ECOMMERCE' not in context:
                    context['GA4_ECOMMERCE'] = self.get_ga4_ecommerce_summary(days=days)

        # ==================== PRICING IMPACT / SENSITIVITY QUESTIONS ====================
        is_pricing_impact_question = any(phrase in question_lower for phrase in [
            'price sensitive', 'price sensitivity', 'pricing impact', 'losing sales due to price',
            'losing sales to price', 'losing sales because of price', 'price gap impact',
            'are we losing sales', 'revenue at risk', 'unmatchable', 'price floor',
            'pricing intelligence', 'price erosion', 'competitor undercutting impact',
            'price vs competitor', 'sales decline price', 'price affecting sales',
        ])

        if is_pricing_impact_question:
            try:
                import asyncio
                from app.services.pricing_intelligence_service import PricingIntelligenceService
                svc = PricingIntelligenceService(self.db)
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        sku_result = pool.submit(asyncio.run, svc.get_sku_pricing_sensitivity(days=default_days, limit=30)).result()
                        brand_result = pool.submit(asyncio.run, svc.get_brand_pricing_impact(days=default_days)).result()
                        unmatchable_result = pool.submit(asyncio.run, svc.get_unmatchable_revenue_risk(days=default_days)).result()
                else:
                    sku_result = asyncio.run(svc.get_sku_pricing_sensitivity(days=default_days, limit=30))
                    brand_result = asyncio.run(svc.get_brand_pricing_impact(days=default_days))
                    unmatchable_result = asyncio.run(svc.get_unmatchable_revenue_risk(days=default_days))

                context['PRICING_IMPACT_SKU_LIST'] = sku_result
                context['PRICING_IMPACT_BRAND_SUMMARY'] = brand_result
                context['PRICING_IMPACT_UNMATCHABLE'] = unmatchable_result
                context['PRICING_IMPACT_INSTRUCTIONS'] = (
                    "CRITICAL: Use PRICING_IMPACT_SKU_LIST, PRICING_IMPACT_BRAND_SUMMARY, "
                    "and PRICING_IMPACT_UNMATCHABLE to answer this pricing impact question. "
                    "These contain SKU-level price sensitivity (price gap to cheapest competitor + sales trend), "
                    "brand-level aggregation (undercut counts, revenue at risk), "
                    "and unmatchable SKUs (competitor below our price floor). "
                    "Focus on revenue at risk and actionable recommendations."
                )
            except Exception as e:
                log.error(f"Error loading pricing impact context: {str(e)}")

        # ==================== CAPRICE SKU-LEVEL PRICE LOOKUP ====================
        # Detect patterns like: "competitor at $1799 for SKU HSNRT80B"
        # or "who is the competitor at 1799 for SKU HSNRT80B"

        sku_price_pattern = re.search(
            r'(?:competitor|who)\s+(?:at|is at|has|with)?\s*\$?([\d,]+(?:\.\d{2})?)\s+(?:for\s+)?(?:sku|product)?\s*([A-Z0-9\-]+)',
            question,
            re.IGNORECASE
        )
        # Also try: "SKU HSNRT80B at $1799"
        sku_price_alt = None
        if not sku_price_pattern:
            sku_price_alt = re.search(
                r'(?:sku|product)\s*([A-Z0-9\-]+)\s+(?:at|price|priced at)\s*\$?([\d,]+(?:\.\d{2})?)',
                question,
                re.IGNORECASE
            )

        if sku_price_pattern or sku_price_alt:
            if sku_price_pattern:
                target_price = float(sku_price_pattern.group(1).replace(',', ''))
                sku = sku_price_pattern.group(2).upper()
            else:
                sku = sku_price_alt.group(1).upper()
                target_price = float(sku_price_alt.group(2).replace(',', ''))

            # Parse explicit date range: "between Dec 15 and Jan 13"
            # or "from Dec 15 to Jan 13"
            month_names = {
                'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
                'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
                'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9,
                'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12
            }
            month_pattern = '|'.join(month_names.keys())

            date_range_match = re.search(
                rf'(?:between|from)\s+({month_pattern})\s+(\d{{1,2}})\s+(?:and|to)\s+({month_pattern})\s+(\d{{1,2}})',
                question_lower
            )

            if date_range_match:
                # Parse the dates (assume current year or infer from context)
                month1 = month_names[date_range_match.group(1).lower()]
                day1 = int(date_range_match.group(2))
                month2 = month_names[date_range_match.group(3).lower()]
                day2 = int(date_range_match.group(4))

                # Infer year - if month1 > month2, start is previous year
                current_year = date.today().year
                if month1 > month2:
                    start_year = current_year - 1
                    end_year = current_year
                else:
                    start_year = current_year
                    end_year = current_year

                sku_start_date = date(start_year, month1, day1)
                sku_end_date = date(end_year, month2, day2)
            else:
                # Default to last 30 days if no explicit range
                sku_end_date = date.today()
                sku_start_date = sku_end_date - timedelta(days=30)

            context['CAPRICE_SKU_PRICE_MATCH'] = self.get_caprice_sku_competitor_price(
                sku=sku,
                target_price=target_price,
                start_date=sku_start_date,
                end_date=sku_end_date
            )
            context['CAPRICE_INSTRUCTIONS'] = (
                f"CRITICAL: Use CAPRICE_SKU_PRICE_MATCH to answer this SKU-specific price question. "
                f"Looking for competitor with price ${target_price} for SKU {sku}. "
                f"If matches found, report the competitor name(s) and date(s). "
                f"If no matches, clearly state: 'No competitor price of ${target_price} found for SKU {sku} in the date range.'"
            )

        # ==================== CAPRICE SKU PRICING TREND QUESTIONS ====================
        # "what has the pricing been like on this sku the past 30 days? H51760Z01AU"

        # Check for SKU pricing trend questions FIRST (before SKU details)
        is_sku_trend_question = any(phrase in normalized_question for phrase in [
            'pricing been like', 'pricing trend', 'price trend', 'price history',
            'pricing history', 'price over', 'pricing over', 'how has the price',
            'how has pricing', 'price changed', 'pricing changed'
        ])

        if is_sku_trend_question:
            # Extract SKU pattern (letters+digits, at least 5 chars)
            sku_trend_match = re.search(r'\b([A-Z][A-Z0-9\-]*\d[A-Z0-9\-]*)\b', question, re.IGNORECASE)
            if sku_trend_match and len(sku_trend_match.group(1)) >= 5:
                sku = sku_trend_match.group(1).upper()
                # Extract days from question (default 30)
                days_match = re.search(r'(?:past|last)\s+(\d+)\s+days?', question, re.IGNORECASE)
                trend_days = int(days_match.group(1)) if days_match else 30

                context['CAPRICE_SKU_TREND'] = self.get_caprice_sku_pricing_trend(sku, days=trend_days)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"CRITICAL: Use CAPRICE_SKU_TREND to answer this SKU pricing trend question. "
                    f"This shows min/avg/max for current_price, lowest_competitor_price, minimum_price (floor), "
                    f"and profit_margin_pct over the past {trend_days} days. "
                    f"Also includes recent_snapshots with the last 5 dates of pricing data. "
                    f"Report the trend summary: days_with_data, price range, competitor price range, "
                    f"and whether price has changed (current_price.change)."
                )

        # ==================== CAPRICE SKU DETAILS / FOLLOWING / COST QUESTIONS ====================
        # "who are we following on SKU X", "nett cost for SKU X", "minimum price for SKU X"

        # Detect SKU detail questions (skip if already handling trend)
        # SKUs typically have format like HSNRT80B, CT1002-1, etc. - require at least one digit
        sku_detail_patterns = [
            r'(?:following|matching|tracking)\s+(?:on\s+)?(?:sku|product)\s+([A-Z0-9\-]+\d[A-Z0-9\-]*)',
            r'(?:following|follwing|folowing|matching|tracking)\s+(?:on|for)?\s*([A-Z0-9\-]+\d[A-Z0-9\-]*)',
            r'(?:nett|net|cost|floor|minimum)\s+(?:price|cost)?\s+(?:for\s+)?(?:sku|product)\s+([A-Z0-9\-]+\d[A-Z0-9\-]*)',
            r'(?:sku|product)\s+([A-Z0-9\-]+\d[A-Z0-9\-]*)',  # Generic "sku X" or "product X"
            r'(?:making|profit|margin)\s+(?:on|at|for)\s+(?:sku|product)\s+([A-Z0-9\-]+\d[A-Z0-9\-]*)',
            r'(?:how much)\s+(?:are we|do we)\s+(?:making|profit)\s+(?:on|at|for)?\s+(?:sku|product)\s+([A-Z0-9\-]+\d[A-Z0-9\-]*)',
        ]

        sku_detail_match = None
        for pattern in sku_detail_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                sku_detail_match = match.group(1).upper()
                break

        # Fallback: look for SKU-like pattern (letters+digits, at least 5 chars, contains digit)
        if not sku_detail_match and 'CAPRICE_SKU_PRICE_MATCH' not in context:
            has_cost_keywords = any(word in normalized_question for word in [
                'nett', 'cost', 'floor', 'minimum price', 'min price', 'following', 'follwing', 'folowing', 'matching',
                'how much are we making', 'profit on', 'margin on'
            ])
            if has_cost_keywords:
                # Match SKU-like patterns: must contain at least one letter AND one digit
                sku_match = re.search(r'\b([A-Z][A-Z0-9\-]*\d[A-Z0-9\-]*)\b', question, re.IGNORECASE)
                if sku_match and len(sku_match.group(1)) >= 5:
                    sku_detail_match = sku_match.group(1).upper()

        if sku_detail_match and 'CAPRICE_SKU_PRICE_MATCH' not in context and 'CAPRICE_SKU_TREND' not in context:
            context['CAPRICE_SKU_DETAILS'] = self.get_caprice_sku_details_latest(sku_detail_match)
            context['CAPRICE_INSTRUCTIONS'] = (
                f"CRITICAL: Use CAPRICE_SKU_DETAILS for SKU {sku_detail_match}. "
                f"This shows: vendor (brand), current_price, minimum_price (floor), nett_cost, "
                f"profit_amount, profit_margin_pct, lowest_competitor_price, and competitor_matches "
                f"(competitors at same price ±$1). Data is from the LATEST Caprice snapshot."
            )

        # ==================== CAPRICE BRAND UNMATCHABLE QUESTIONS ====================
        # "How many Zip SKUs can't be matched" / "below minimum for brand X"
        # Detect if this is an unmatchable/below-minimum question first
        is_unmatchable_question = any(phrase in normalized_question for phrase in [
            "can't match", "cannot match", "cant match", "unmatchable",
            "below minimum", "below floor", "under minimum", "below min"
        ])

        brand_unmatchable_match = None
        if is_unmatchable_question:
            # Look for brand name patterns - check most specific first
            unmatchable_patterns = [
                r'(?:how many|which)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:skus?|products?)',  # "How many Zip SKUs"
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:skus?|products?)\s+(?:are|can)',  # "Zip SKUs are/can"
                r'(?:for|brand)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',  # "for Zip" or "brand Zip"
            ]

            for pattern in unmatchable_patterns:
                match = re.search(pattern, question, re.IGNORECASE)
                if match:
                    brand_unmatchable_match = match.group(1)
                    break

        if brand_unmatchable_match and 'CAPRICE_SKU_DETAILS' not in context:
            context['CAPRICE_BRAND_UNMATCHABLE'] = self.get_caprice_brand_unmatchable(
                brand=brand_unmatchable_match,
                use_latest_only=True
            )
            context['CAPRICE_INSTRUCTIONS'] = (
                f"CRITICAL: Use CAPRICE_BRAND_UNMATCHABLE for brand '{brand_unmatchable_match}'. "
                f"This shows SKUs where lowest_competitor_price < our minimum_price (floor). "
                f"We cannot match these competitors without going below our minimum. "
                f"Report: unmatchable_count, unmatchable_pct, and list the SKUs with gaps."
            )

        # ==================== CAPRICE COMPETITOR TREND QUESTIONS ====================
        # "Over the past 12 months, what has brandsdirect been doing?"
        trend_patterns = [
            r'(?:past|last)\s+(\d+)\s+months?\s+(?:what\s+)?(?:has\s+)?(\w+)\s+(?:been\s+)?(?:doing|trend)',
            r'(\w+)\s+(?:trend|history|over time)\s+(?:past|last)?\s*(\d+)?\s*months?',
            r'(?:how has|what has)\s+(\w+)\s+(?:been\s+)?(?:doing|changed|trending)',
        ]

        competitor_trend_match = None
        trend_months = 12
        for pattern in trend_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                groups = match.groups()
                # Find which group is the competitor name (not a number)
                for g in groups:
                    if g and not g.isdigit():
                        competitor_trend_match = g
                    elif g and g.isdigit():
                        trend_months = int(g)
                break

        if competitor_trend_match and 'CAPRICE_SKU_DETAILS' not in context and 'CAPRICE_BRAND_UNMATCHABLE' not in context:
            context['CAPRICE_COMPETITOR_TREND'] = self.get_caprice_competitor_trend(
                competitor=competitor_trend_match,
                months=trend_months
            )
            context['CAPRICE_INSTRUCTIONS'] = (
                f"CRITICAL: Use CAPRICE_COMPETITOR_TREND for competitor '{competitor_trend_match}'. "
                f"This shows monthly data: skus_undercut, undercut_rate, avg_gap, total_gap. "
                f"The summary shows trend_direction (increasing/decreasing/stable). "
                f"Report the trend over {trend_months} months with key monthly highlights."
            )

        # ==================== CAPRICE COMPETITIVE PRICING QUESTIONS ====================
        # Skip general Caprice routing if SKU-specific match was already handled
        is_set_price_question = any(phrase in normalized_question for phrase in ['set price', 'fixed price'])
        is_caprice_question = any(word in normalized_question for word in [
            'competitor', 'competitors', 'undercut', 'undercutting', 'price gap',
            'min margin', 'minimum margin', 'losing money', 'below minimum',
            'caprice', 'pricing log', 'competitive pricing', 'competitor price',
            'margin breach', 'price match', 'price matching'
        ]) and not is_set_price_question

        if is_caprice_question and 'CAPRICE_SKU_PRICE_MATCH' not in context:
            # Parse days from question, default to 7
            days = self.parse_time_period(question) or 7

            # Parse limit from question
            limit_match = re.search(r'top\s+(\d+)', question_lower)
            limit = int(limit_match.group(1)) if limit_match else 20

            # Always include summary for context
            context['CAPRICE_SUMMARY'] = self.get_competitive_pricing_summary(days=days)

            # Brand-level competitive analysis (check FIRST - most specific)
            # Patterns: "which brand" + competitor/undercut/price gap keywords
            is_brand_analysis = (
                any(phrase in normalized_question for phrase in ['which brand', 'which brands', 'brand analysis', 'by brand', 'brands are']) and
                any(word in normalized_question for word in ['competitor', 'undercut', 'price gap', 'match', 'competitive', 'losing'])
            )

            # Competitor analysis questions (which COMPETITOR, not brand)
            is_competitor_analysis = any(phrase in normalized_question for phrase in [
                'which competitor', 'which competitors', 'competitor analysis',
                'most aggressive', 'price gap by', 'competitors undercut',
                'undercut us the most', 'undercutting us the most'
            ])

            if is_brand_analysis:
                context['CAPRICE_BRAND_GAPS'] = self.get_caprice_brand_competitive_gaps(days=days, limit=limit)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"CRITICAL: For brand-level competitive analysis, use CAPRICE_BRAND_GAPS. "
                    f"This shows aggregated competitive gaps BY BRAND (vendor), NOT individual SKUs. "
                    f"Fields: brand, total_skus_tracked, skus_undercut_count, undercut_rate (%), avg_price_gap ($), total_price_gap ($). "
                    f"Sorted by total_price_gap (highest $ exposure first). "
                    f"Answer using brand aggregates, NOT individual product details. "
                    f"Data is from Caprice competitive intelligence for the last {days} days."
                )

            elif is_competitor_analysis:
                context['CAPRICE_BY_COMPETITOR'] = self.get_price_gap_by_competitor(days=days, limit=limit)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"CRITICAL: For competitor analysis questions, use CAPRICE_BY_COMPETITOR. "
                    f"This shows which competitors undercut us most often and by how much. "
                    f"undercut_count = number of products where competitor is cheaper. "
                    f"Data is from Caprice competitive intelligence for the last {days} days."
                )

            # Undercut questions (which products are being undercut)
            elif any(word in normalized_question for word in ['undercut', 'undercutting', 'cheaper', 'lower price', 'beating']):
                context['CAPRICE_UNDERCUTS'] = self.get_competitor_undercuts(days=days, limit=limit)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"CRITICAL: For competitor undercut questions, use CAPRICE_UNDERCUTS. "
                    f"This shows products where competitors have lower prices than us. "
                    f"price_gap = our_price - competitor_price (positive means we're more expensive). "
                    f"Data is from Caprice competitive intelligence for the last {days} days."
                )

            # Margin breach / losing money questions
            elif any(word in normalized_question for word in ['losing money', 'margin breach', 'below minimum', 'min margin', 'negative margin']):
                context['CAPRICE_MARGIN_BREACHES'] = self.get_min_margin_breaches(days=days, limit=limit)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"CRITICAL: For margin breach questions, use CAPRICE_MARGIN_BREACHES. "
                    f"This shows products that are losing money or priced below minimum. "
                    f"breach_type indicates: 'losing_money' (profit < 0) or 'below_minimum' (price < min_price). "
                    f"Data is from Caprice competitive intelligence for the last {days} days."
                )

            # General competitive pricing / summary questions
            else:
                context['CAPRICE_UNDERCUTS'] = self.get_competitor_undercuts(days=days, limit=limit)
                context['CAPRICE_INSTRUCTIONS'] = (
                    f"Use CAPRICE_SUMMARY for overall competitive pricing stats. "
                    f"Use CAPRICE_UNDERCUTS to see specific products being undercut. "
                    f"Data is from Caprice competitive intelligence for the last {days} days."
                )

        # ==================== NETT MASTER SHEET / PRODUCT COST QUESTIONS ====================
        # Questions about do not follow, set price, brand costs, unmatchable SKUs

        is_nett_cost_question = any(phrase in normalized_question for phrase in [
            'do not follow', 'dont follow', "don't follow", 'excluded from matching',
            'set price', 'fixed price', 'nett cost', 'net cost', 'nett nett',
            'brand cost', 'brand summary', 'vendor cost', 'supplier cost',
            'unmatchable', 'cannot match', "can't match", 'below floor'
        ])

        if is_nett_cost_question:
            # Do Not Follow questions
            if any(phrase in normalized_question for phrase in ['do not follow', 'dont follow', "don't follow", 'excluded']):
                # Check for specific vendor
                vendor_match = re.search(r'(?:for|brand|vendor)\s+([A-Za-z]+)', question, re.IGNORECASE)
                vendor_filter = vendor_match.group(1) if vendor_match else None

                context['NETT_DO_NOT_FOLLOW'] = self.get_do_not_follow_skus(vendor=vendor_filter)
                context['NETT_INSTRUCTIONS'] = (
                    f"CRITICAL: Use NETT_DO_NOT_FOLLOW to answer this question. "
                    f"These SKUs are EXCLUDED from competitor price matching. "
                    f"Show the count by vendor and list the SKUs with their comments."
                )

            # Set Price questions
            elif any(phrase in normalized_question for phrase in ['set price', 'fixed price']):
                vendor_match = re.search(r'(?:for|brand|vendor)\s+([A-Za-z]+)', question, re.IGNORECASE)
                vendor_filter = vendor_match.group(1) if vendor_match else None

                context['NETT_SET_PRICE'] = self.get_set_price_skus(vendor=vendor_filter)
                context['NETT_INSTRUCTIONS'] = (
                    f"CRITICAL: Use NETT_SET_PRICE to answer this question. "
                    f"These SKUs have a FIXED price (ignore competitor matching). "
                    f"Show the set price, margin at that price, and whether it meets min margin requirement."
                )

            # Brand/vendor cost summary questions
            elif any(phrase in normalized_question for phrase in ['brand cost', 'brand summary', 'vendor cost', 'supplier']):
                # Extract specific vendor filter, but exclude common non-vendor words
                non_vendor_words = {'cost', 'summary', 'analysis', 'report', 'data', 'price', 'pricing', 'all', 'the'}
                vendor_match = re.search(r'(?:for|brand|vendor)\s+([A-Za-z]+)', question, re.IGNORECASE)
                vendor_filter = None
                if vendor_match:
                    potential_vendor = vendor_match.group(1).lower()
                    if potential_vendor not in non_vendor_words:
                        vendor_filter = vendor_match.group(1)

                context['NETT_BRAND_SUMMARY'] = self.get_brand_cost_summary(vendor=vendor_filter)
                context['NETT_INSTRUCTIONS'] = (
                    f"CRITICAL: Use NETT_BRAND_SUMMARY to answer this question. "
                    f"Shows brands with: total SKUs, avg nett cost, avg floor price, "
                    f"avg RRP, undercut count/percentage, and avg undercut gap."
                )

            # Unmatchable SKUs questions
            elif any(phrase in normalized_question for phrase in ['unmatchable', 'cannot match', "can't match", 'below floor']):
                vendor_match = re.search(r'(?:for|brand|vendor)\s+([A-Za-z]+)', question, re.IGNORECASE)
                vendor_filter = vendor_match.group(1) if vendor_match else None

                context['NETT_UNMATCHABLE'] = self.get_unmatchable_skus_by_brand(vendor=vendor_filter)
                context['NETT_INSTRUCTIONS'] = (
                    f"CRITICAL: Use NETT_UNMATCHABLE to answer this question. "
                    f"These SKUs have competitors priced BELOW our floor price - we cannot match them. "
                    f"Show the gap (our floor - competitor price) and group by vendor."
                )

            # Generic nett cost question - get SKU details if a SKU is mentioned
            else:
                sku_match = re.search(r'\b([A-Z][A-Z0-9\-]*\d[A-Z0-9\-]*)\b', question, re.IGNORECASE)
                if sku_match and len(sku_match.group(1)) >= 5:
                    sku = sku_match.group(1).upper()
                    context['NETT_SKU_DETAILS'] = self.get_sku_cost_details(sku)
                    context['NETT_INSTRUCTIONS'] = (
                        f"CRITICAL: Use NETT_SKU_DETAILS for SKU {sku}. "
                        f"Shows full cost breakdown from NETT Master Sheet: nett cost, RRP, minimum price, "
                        f"discounts (rebates, settlement, etc.), min margin %, and do_not_follow/set_price flags."
                    )

        # Only include traffic/search data if not asking about products
        is_product_question = any(word in question_lower for word in ['product', 'selling', 'items', 'sku'])
        if not is_product_question and not is_seo_question:
            context['traffic_sources'] = self.get_traffic_sources_summary()
            context['top_search_queries'] = self.get_top_search_queries(limit=20)

        # For revenue/sales questions, include more detail
        if any(word in question_lower for word in ['revenue', 'sales', 'orders', 'money', 'earned']):
            context['revenue_by_year'] = self.get_revenue_by_year()
            context['monthly_trends'] = self.get_monthly_trends(months=12)
            if not date_range:
                # Default to last 30 days if no specific period
                context['last_30_days'] = self.get_orders_last_n_days(30)

        # ==================== REFUND QUESTIONS ====================
        # Detect refund-related questions
        is_refund_question = any(phrase in normalized_question for phrase in [
            'refund', 'refunds', 'refunded', 'refunded orders', 'refund count',
            'how many refunds', 'refund records', 'returned', 'returns'
        ])

        if is_refund_question:

            # For refund questions, prioritize specific date patterns over year-based parsing
            # Handle "since YYYY-MM-DD" or "since 2025-02-06" pattern
            refund_start = None
            refund_end = None
            used_since_pattern = False

            # Pattern 1: "since YYYY-MM-DD" - NO end date for "since" queries
            since_match = re.search(r'since\s+(\d{4})[-‑](\d{1,2})[-‑](\d{1,2})', normalized_question)
            if since_match:
                try:
                    refund_start = date(
                        int(since_match.group(1)),
                        int(since_match.group(2)),
                        int(since_match.group(3))
                    )
                    refund_end = None  # Explicitly NO end date for "since" queries
                    used_since_pattern = True
                except ValueError:
                    pass

            # Pattern 2: If no specific "since" pattern found, use already parsed date range
            if not used_since_pattern:
                if not refund_start and start_date:
                    refund_start = start_date
                if end_date:
                    refund_end = end_date

            context['REFUND_COUNTS'] = self.get_refund_counts(
                start_date=refund_start,
                end_date=refund_end
            )
            context['REFUND_INSTRUCTIONS'] = (
                "CRITICAL: Use REFUND_COUNTS to answer this question. "
                "refunded_orders = distinct order IDs with refunds. "
                "refund_records = total number of refund records (one order can have multiple refunds). "
                "total_refund_amount = sum of all refunded amounts. "
                f"{'Date filter applied: ' + context['REFUND_COUNTS'].get('date_filter', '') if context['REFUND_COUNTS'].get('date_filter') else 'No date filter - showing all refunds.'}"
            )

        # For product questions - USE DATE FILTER
        if any(word in question_lower for word in ['product', 'selling', 'items', 'sku', 'top']):
            # Determine limit from question (e.g., "top 10" vs "top 5")
            limit_match = re.search(r'top\s+(\d+)', question_lower)
            limit = int(limit_match.group(1)) if limit_match else 20

            if start_date and end_date:
                # DATE-FILTERED PRODUCTS
                products = self.get_top_products(
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date
                )
                period_desc = context.get('date_filter', {}).get('description', 'filtered period')
                total_revenue = sum(p['revenue'] for p in products)
                context['TOP_PRODUCTS_FOR_REQUESTED_PERIOD'] = {
                    'PERIOD': period_desc,
                    'DATE_RANGE': f"{start_date} to {end_date}",
                    'PRODUCTS': products,
                    'TOTAL_REVENUE': total_revenue,
                    'INSTRUCTIONS': f'Use ONLY these {len(products)} products when answering. These are filtered for {period_desc}.'
                }
            else:
                # Fallback: check for time period in question
                days = self.parse_time_period(question)
                if days:
                    end = date.today()
                    start = end - timedelta(days=days)
                    products = self.get_top_products(
                        limit=limit,
                        start_date=start,
                        end_date=end
                    )
                    total_revenue = sum(p['revenue'] for p in products)
                    context['TOP_PRODUCTS_FOR_REQUESTED_PERIOD'] = {
                        'PERIOD': f"Last {days} days",
                        'DATE_RANGE': f"{start} to {end}",
                        'PRODUCTS': products,
                        'TOTAL_REVENUE': total_revenue,
                        'INSTRUCTIONS': f'Use ONLY these {len(products)} products when answering. These are filtered for the last {days} days.'
                    }
                else:
                    # No date filter - all time
                    context['top_products'] = self.get_top_products(limit=limit)
                    context['top_products_period'] = "All time"

        # For customer questions
        if any(word in question_lower for word in ['customer', 'buyer', 'client']):
            context['top_customers'] = self.get_top_customers(limit=20)

        return context

    # ==================== PRODUCT MIX ANALYTICS ====================
    # Fast queries using the normalized shopify_order_items table

    def get_product_mix_by_date(
        self,
        start_date: date,
        end_date: date,
        limit: int = 30
    ) -> List[Dict]:
        """
        Get product mix for a date range - fast query using order_items table.

        Returns top products by revenue with quantity sold.
        """
        try:
            results = self.db.query(
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku,
                ShopifyOrderItem.shopify_product_id,
                func.sum(ShopifyOrderItem.quantity).label('units_sold'),
                func.sum(ShopifyOrderItem.total_price).label('revenue'),
                func.count(func.distinct(ShopifyOrderItem.shopify_order_id)).label('order_count')
            ).filter(
                ShopifyOrderItem.order_date >= start_date,
                ShopifyOrderItem.order_date <= end_date,
                ShopifyOrderItem.financial_status.in_(['paid', 'partially_refunded'])
            ).group_by(
                ShopifyOrderItem.shopify_product_id,
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku
            ).order_by(
                desc('revenue')
            ).limit(limit).all()

            return [
                {
                    'title': r.title,
                    'sku': r.sku,
                    'product_id': r.shopify_product_id,
                    'units_sold': r.units_sold or 0,
                    'revenue': float(r.revenue or 0),
                    'order_count': r.order_count or 0
                }
                for r in results
            ]
        except Exception as e:
            log.error(f"Error getting product mix: {str(e)}")
            return []

    def get_daily_product_sales(
        self,
        product_id: int = None,
        sku: str = None,
        days: int = 30
    ) -> List[Dict]:
        """Get daily sales for a specific product or SKU."""
        try:
            start_date = datetime.now() - timedelta(days=days)

            query = self.db.query(
                func.date(ShopifyOrderItem.order_date).label('date'),
                func.sum(ShopifyOrderItem.quantity).label('units'),
                func.sum(ShopifyOrderItem.total_price).label('revenue')
            ).filter(
                ShopifyOrderItem.order_date >= start_date,
                ShopifyOrderItem.financial_status.in_(['paid', 'partially_refunded'])
            )

            if product_id:
                query = query.filter(ShopifyOrderItem.shopify_product_id == product_id)
            elif sku:
                query = query.filter(ShopifyOrderItem.sku == sku)

            results = query.group_by(
                func.date(ShopifyOrderItem.order_date)
            ).order_by('date').all()

            return [
                {
                    'date': str(r.date),
                    'units': r.units or 0,
                    'revenue': float(r.revenue or 0)
                }
                for r in results
            ]
        except Exception as e:
            log.error(f"Error getting daily product sales: {str(e)}")
            return []

    def get_product_trends(self, days: int = 30, limit: int = 20) -> Dict:
        """
        Compare product performance between two periods.
        Returns products that are growing or declining.
        """
        try:
            # Current period
            current_end = datetime.now()
            current_start = current_end - timedelta(days=days)

            # Previous period
            prev_end = current_start
            prev_start = prev_end - timedelta(days=days)

            # Get current period data
            current = self.db.query(
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku,
                ShopifyOrderItem.shopify_product_id,
                func.sum(ShopifyOrderItem.quantity).label('units'),
                func.sum(ShopifyOrderItem.total_price).label('revenue')
            ).filter(
                ShopifyOrderItem.order_date >= current_start,
                ShopifyOrderItem.order_date <= current_end,
                ShopifyOrderItem.financial_status.in_(['paid', 'partially_refunded'])
            ).group_by(
                ShopifyOrderItem.shopify_product_id,
                ShopifyOrderItem.title,
                ShopifyOrderItem.sku
            ).all()

            current_map = {
                r.shopify_product_id: {
                    'title': r.title,
                    'sku': r.sku,
                    'units': r.units or 0,
                    'revenue': float(r.revenue or 0)
                }
                for r in current
            }

            # Get previous period data
            previous = self.db.query(
                ShopifyOrderItem.shopify_product_id,
                func.sum(ShopifyOrderItem.quantity).label('units'),
                func.sum(ShopifyOrderItem.total_price).label('revenue')
            ).filter(
                ShopifyOrderItem.order_date >= prev_start,
                ShopifyOrderItem.order_date <= prev_end,
                ShopifyOrderItem.financial_status.in_(['paid', 'partially_refunded'])
            ).group_by(
                ShopifyOrderItem.shopify_product_id
            ).all()

            prev_map = {
                r.shopify_product_id: {
                    'units': r.units or 0,
                    'revenue': float(r.revenue or 0)
                }
                for r in previous
            }

            # Calculate changes
            trends = []
            for pid, curr in current_map.items():
                prev = prev_map.get(pid, {'units': 0, 'revenue': 0})
                revenue_change = curr['revenue'] - prev['revenue']
                revenue_change_pct = (revenue_change / prev['revenue'] * 100) if prev['revenue'] > 0 else (100 if curr['revenue'] > 0 else 0)

                trends.append({
                    'product_id': pid,
                    'title': curr['title'],
                    'sku': curr['sku'],
                    'current_revenue': curr['revenue'],
                    'previous_revenue': prev['revenue'],
                    'revenue_change': revenue_change,
                    'revenue_change_pct': round(revenue_change_pct, 1),
                    'current_units': curr['units'],
                    'previous_units': prev['units']
                })

            # Sort by absolute revenue change
            trends.sort(key=lambda x: abs(x['revenue_change']), reverse=True)

            growing = [t for t in trends if t['revenue_change'] > 0][:limit]
            declining = [t for t in trends if t['revenue_change'] < 0][:limit]

            return {
                'period': f'{days} days',
                'growing': growing,
                'declining': declining
            }
        except Exception as e:
            log.error(f"Error getting product trends: {str(e)}")
            return {'growing': [], 'declining': []}

    def get_top_products_by_month(self, months: int = 3, limit: int = 10) -> List[Dict]:
        """Get top products for each of the last N months."""
        try:
            results = []
            for i in range(months):
                month_end = datetime.now().replace(day=1) - timedelta(days=1) - timedelta(days=30*i)
                month_start = month_end.replace(day=1)

                top_products = self.get_product_mix_by_date(
                    start_date=month_start.date(),
                    end_date=month_end.date(),
                    limit=limit
                )

                results.append({
                    'month': month_start.strftime('%Y-%m'),
                    'products': top_products
                })

            return results
        except Exception as e:
            log.error(f"Error getting top products by month: {str(e)}")
            return []

    def is_historical_question(self, question: str) -> bool:
        """
        Determine if a question requires historical DATABASE data vs real-time API data.

        Returns True for questions about:
        - Time periods (last 30 days, past month, etc.)
        - Year comparisons (2024 vs 2025)
        - Top products/customers (all time)
        - Historical trends
        - Full SEO queries
        """
        question_lower = question.lower()

        # Check if there's a time period in the question
        if self.parse_time_period(question):
            return True

        # Historical keywords - use DATABASE
        historical_patterns = [
            'vs', 'versus', 'compare', 'comparison',
            '2023', '2024', '2025', '2026',
            'last year', 'this year', 'previous year',
            'last week', 'past week', 'last month', 'past month',
            'last quarter', 'past quarter',
            'last 7', 'last 14', 'last 30', 'last 60', 'last 90',
            'past 7', 'past 14', 'past 30', 'past 60', 'past 90',
            'all time', 'all-time', 'ever', 'total',
            'top products', 'top customers', 'best selling', 'best-selling',
            'historically', 'historical', 'trends', 'over time',
            'growth', 'year over year', 'yoy', 'monthly', 'weekly',
            'revenue breakdown', 'full', 'complete',
            'how much', 'how many orders', 'revenue',
            'seo', 'search queries', 'keywords', 'organic',
            'summary', 'performance', 'orders', 'sales',
        ]

        for pattern in historical_patterns:
            if pattern in question_lower:
                return True

        return False

    def is_realtime_question(self, question: str) -> bool:
        """Returns True only for questions needing live API data (today, right now)"""
        question_lower = question.lower()
        realtime_patterns = ['right now', 'currently', 'live', 'real-time', 'realtime']
        # "today" could be database if we have today's data synced
        for pattern in realtime_patterns:
            if pattern in question_lower:
                return True
        return False


# Convenience function to get an instance
def get_chat_data_service() -> ChatDataService:
    return ChatDataService()
