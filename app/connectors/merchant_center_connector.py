"""
Google Merchant Center data connector
Fetches product listings, performance data, and shopping feed status
"""
from typing import Any, Dict, List
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class MerchantCenterConnector(BaseConnector):
    """Connector for Google Merchant Center"""

    def __init__(self):
        super().__init__("Google Merchant Center")
        self.merchant_id = settings.merchant_center_id
        self.credentials_path = settings.merchant_center_credentials_path
        self.service = None

    async def connect(self) -> bool:
        """Establish connection to Merchant Center"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/content']
            )
            self.service = build('content', 'v2.1', credentials=credentials)
            log.info("Connected to Google Merchant Center")
            return True
        except Exception as e:
            log.error(f"Failed to connect to Merchant Center: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate Merchant Center connection"""
        try:
            if not self.service:
                await self.connect()

            # Test with a simple account info request
            result = self.service.accounts().get(
                merchantId=self.merchant_id,
                accountId=self.merchant_id
            ).execute()

            account_name = result.get('name', 'Unknown')
            log.info(f"Merchant Center account validated: {account_name}")
            return True
        except Exception as e:
            log.error(f"Merchant Center connection validation failed: {str(e)}")
            return False

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive Merchant Center data"""
        if not self.service:
            await self.connect()

        data = {
            "account_info": await self._fetch_account_info(),
            "products": await self._fetch_products(),
            "product_statuses": await self._fetch_product_statuses(),
            "account_status": await self._fetch_account_status(),
        }
        return data

    async def _fetch_account_info(self) -> Dict:
        """Fetch account information"""
        try:
            result = self.service.accounts().get(
                merchantId=self.merchant_id,
                accountId=self.merchant_id
            ).execute()

            log.info(f"Fetched Merchant Center account info: {result.get('name', 'Unknown')}")
            return {
                "id": result.get('id'),
                "name": result.get('name'),
                "website_url": result.get('websiteUrl'),
                "adult_content": result.get('adultContent', False),
                "seller_id": result.get('sellerId'),
            }
        except Exception as e:
            log.error(f"Error fetching Merchant Center account info: {str(e)}")
            return {}

    async def _fetch_products(self) -> List[Dict]:
        """Fetch all products from Merchant Center"""
        try:
            products = []
            page_token = None

            while True:
                request = self.service.products().list(
                    merchantId=self.merchant_id,
                    maxResults=250,
                    pageToken=page_token
                )
                result = request.execute()

                resources = result.get('resources', [])
                for product in resources:
                    products.append({
                        "id": product.get('id'),
                        "offer_id": product.get('offerId'),
                        "title": product.get('title'),
                        "description": product.get('description', '')[:200],  # Truncate
                        "link": product.get('link'),
                        "image_link": product.get('imageLink'),
                        "price": product.get('price', {}).get('value'),
                        "currency": product.get('price', {}).get('currency'),
                        "availability": product.get('availability'),
                        "condition": product.get('condition'),
                        "brand": product.get('brand'),
                        "gtin": product.get('gtin'),
                        "mpn": product.get('mpn'),
                        "google_product_category": product.get('googleProductCategory'),
                        "product_type": product.get('productTypes', [None])[0] if product.get('productTypes') else None,
                        "channel": product.get('channel'),
                        "content_language": product.get('contentLanguage'),
                        "target_country": product.get('targetCountry'),
                        "sale_price": product.get('salePrice', {}).get('value') if product.get('salePrice') else None,
                        "sale_price_effective_date": product.get('salePriceEffectiveDate'),
                    })

                page_token = result.get('nextPageToken')
                if not page_token:
                    break

            log.info(f"Fetched {len(products)} products from Merchant Center")
            return products

        except Exception as e:
            log.error(f"Error fetching Merchant Center products: {str(e)}")
            return []

    async def _fetch_product_statuses(self) -> Dict:
        """Fetch product statuses (approval status, issues, etc.)"""
        try:
            statuses = {
                "approved": 0,
                "disapproved": 0,
                "pending": 0,
                "expiring": 0,
                "issues": [],
                "products_with_issues": [],
                "all_products": []  # Track all products for status history
            }

            page_token = None

            while True:
                request = self.service.productstatuses().list(
                    merchantId=self.merchant_id,
                    maxResults=250,
                    pageToken=page_token
                )
                result = request.execute()

                resources = result.get('resources', [])
                for status in resources:
                    product_id = status.get('productId', '')
                    title = status.get('title', '')

                    # Determine approval status for Shopping destination
                    approval_status = 'pending'
                    dest_statuses = status.get('destinationStatuses', [])
                    for dest in dest_statuses:
                        if dest.get('destination') == 'Shopping':
                            approval_status = dest.get('status', 'pending')
                            if approval_status == 'approved':
                                statuses['approved'] += 1
                            elif approval_status == 'disapproved':
                                statuses['disapproved'] += 1
                            else:
                                statuses['pending'] += 1
                            break

                    # Collect issues
                    item_issues = status.get('itemLevelIssues', [])
                    issues_list = [
                        {
                            "code": issue.get('code'),
                            "severity": issue.get('servability'),
                            "description": issue.get('description'),
                            "detail": issue.get('detail'),
                            "documentation": issue.get('documentation'),
                            "destination": issue.get('destination'),
                            "attribute": issue.get('attributeName'),
                        }
                        for issue in item_issues
                    ]

                    # Track ALL products for status history (limit to essential fields)
                    statuses['all_products'].append({
                        "product_id": product_id,
                        "title": title,
                        "approval_status": approval_status,
                        "has_issues": len(item_issues) > 0,
                        "issue_count": len(item_issues)
                    })

                    if item_issues:
                        statuses['products_with_issues'].append({
                            "product_id": product_id,
                            "title": title,
                            "issues": issues_list
                        })

                        # Aggregate issue types
                        for issue in item_issues:
                            issue_code = issue.get('code', 'unknown')
                            existing = next((i for i in statuses['issues'] if i['code'] == issue_code), None)
                            if existing:
                                existing['count'] += 1
                            else:
                                statuses['issues'].append({
                                    "code": issue_code,
                                    "description": issue.get('description', ''),
                                    "severity": issue.get('servability', 'unknown'),
                                    "count": 1
                                })

                page_token = result.get('nextPageToken')
                if not page_token:
                    break

            # Sort issues by count
            statuses['issues'] = sorted(statuses['issues'], key=lambda x: x['count'], reverse=True)

            total = statuses['approved'] + statuses['disapproved'] + statuses['pending']
            log.info(f"Fetched Merchant Center product statuses: {statuses['approved']} approved, "
                    f"{statuses['disapproved']} disapproved, {statuses['pending']} pending out of {total}")

            return statuses

        except Exception as e:
            log.error(f"Error fetching Merchant Center product statuses: {str(e)}")
            return {"approved": 0, "disapproved": 0, "pending": 0, "issues": [], "all_products": []}

    async def _fetch_account_status(self) -> Dict:
        """Fetch account-level status and issues"""
        try:
            result = self.service.accountstatuses().get(
                merchantId=self.merchant_id,
                accountId=self.merchant_id
            ).execute()

            account_issues = []
            for issue in result.get('accountLevelIssues', []):
                account_issues.append({
                    "id": issue.get('id'),
                    "title": issue.get('title'),
                    "severity": issue.get('severity'),
                    "country": issue.get('country'),
                    "destination": issue.get('destination'),
                    "detail": issue.get('detail'),
                    "documentation": issue.get('documentation'),
                })

            # Get products statistics
            products_stats = result.get('products', [])
            stats_by_channel = {}
            for stat in products_stats:
                channel = stat.get('channel', 'unknown')
                stats_by_channel[channel] = {
                    "active": stat.get('statistics', {}).get('active', 0),
                    "pending": stat.get('statistics', {}).get('pending', 0),
                    "disapproved": stat.get('statistics', {}).get('disapproved', 0),
                    "expiring": stat.get('statistics', {}).get('expiring', 0),
                }

            log.info(f"Fetched Merchant Center account status with {len(account_issues)} account-level issues")

            return {
                "account_id": result.get('accountId'),
                "website_claimed": result.get('websiteClaimed', False),
                "account_issues": account_issues,
                "products_by_channel": stats_by_channel,
            }

        except Exception as e:
            log.error(f"Error fetching Merchant Center account status: {str(e)}")
            return {}

    async def fetch_products_quick(self) -> Dict[str, Any]:
        """
        Quick fetch - just product statuses summary (no full product list)
        Much faster for chat queries
        """
        if not self.service:
            await self.connect()

        data = {
            "account_info": await self._fetch_account_info(),
            "product_statuses": await self._fetch_product_statuses(),
            "account_status": await self._fetch_account_status(),
        }
        return data
