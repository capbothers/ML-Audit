"""
Shippit shipping cost connector.
Fetches fulfillment data from Shippit API v3.

API structure:
  - GET /orders/{tracking_number} — single order lookup (no list endpoint)
  - POST /quotes — get shipping rate quotes given parcel + destination

The Get Order endpoint doesn't expose actual billed shipping cost, so we use
the Quote API to estimate the cost based on parcel dimensions and destination.

Flow:
  1. DataSyncService gets tracking numbers from Shopify fulfillments
  2. This connector looks up each in Shippit to get parcel + destination data
  3. For each order, calls Quote API to get the estimated shipping cost
"""
from typing import Any, Dict, List, Optional
from datetime import datetime
import asyncio
import aiohttp
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class ShippitConnector(BaseConnector):
    """Connector for Shippit shipping platform — cost data only."""

    def __init__(self):
        super().__init__("Shippit")
        self.api_key = settings.shippit_api_key
        self.base_url = settings.shippit_api_base_url.rstrip("/")
        self.headers = {
            "Authorization": self.api_key,
            "Accept": "application/json",
        }

    async def connect(self) -> bool:
        """Test Shippit API connection by requesting a non-existent order."""
        if not self.api_key:
            log.warning("Shippit API key not configured, skipping")
            return False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/orders/__connection_test__",
                    headers=self.headers,
                ) as response:
                    if response.status == 404:
                        body = await response.text()
                        if "not_found" in body or "error" in body:
                            log.info("Connected to Shippit API (auth verified)")
                            return True
                        log.warning("Shippit returned 404 but unexpected body")
                        return False
                    elif response.status == 200:
                        log.info("Connected to Shippit API")
                        return True
                    elif response.status in (401, 403):
                        log.error("Shippit API authentication failed — check API key")
                        return False
                    else:
                        log.warning(f"Shippit API returned status {response.status}")
                        return False
        except Exception as e:
            log.error(f"Failed to connect to Shippit: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        return await self.connect()

    async def fetch_data(
        self,
        start_date: datetime,
        end_date: datetime,
        tracking_numbers: Optional[List[str]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Fetch shipping data from Shippit for the given tracking numbers.

        1. Look up each tracking number via GET /orders/{tn}
        2. For each order, estimate shipping cost via POST /quotes
        """
        if not tracking_numbers:
            log.info("No tracking numbers provided, nothing to fetch from Shippit")
            return {"orders": []}

        orders = await self._fetch_by_tracking_numbers(tracking_numbers)

        # Estimate shipping costs via Quote API
        quoted = 0
        async with aiohttp.ClientSession() as session:
            for order in orders:
                cost = await self._estimate_shipping_cost(session, order)
                if cost is not None:
                    order["shipping_cost"] = cost
                    quoted += 1

        log.info(
            f"Fetched {len(orders)} of {len(tracking_numbers)} orders from Shippit "
            f"({quoted} with cost estimates)"
        )
        return {"orders": orders}

    async def _fetch_by_tracking_numbers(
        self, tracking_numbers: List[str], concurrency: int = 5
    ) -> List[Dict]:
        """Fetch orders by tracking number with controlled concurrency."""
        sem = asyncio.Semaphore(concurrency)
        orders: List[Dict] = []

        async def _fetch_one(session: aiohttp.ClientSession, tn: str):
            async with sem:
                try:
                    async with session.get(
                        f"{self.base_url}/orders/{tn}",
                        headers=self.headers,
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            order = data.get("response", data)
                            if isinstance(order, dict):
                                orders.append(self._normalize_order(order))
                        elif response.status == 404:
                            log.debug(f"Shippit order {tn} not found")
                        else:
                            log.debug(f"Shippit order {tn} returned {response.status}")
                except Exception as e:
                    log.debug(f"Error fetching Shippit order {tn}: {e}")
                await asyncio.sleep(0.2)

        async with aiohttp.ClientSession() as session:
            tasks = [_fetch_one(session, tn) for tn in tracking_numbers]
            await asyncio.gather(*tasks)

        return orders

    async def _estimate_shipping_cost(
        self, session: aiohttp.ClientSession, order: Dict
    ) -> Optional[float]:
        """
        Use the Shippit Quote API to estimate shipping cost for an order.
        Matches on courier_type to get the rate the merchant would have paid.
        """
        raw = order.get("raw_response", {})
        parcels = raw.get("parcel_attributes", [])
        if not parcels:
            return None

        destination = {
            "dropoff_suburb": raw.get("delivery_suburb"),
            "dropoff_postcode": raw.get("delivery_postcode"),
            "dropoff_state": raw.get("delivery_state"),
            "dropoff_country_code": raw.get("delivery_country_code", "AU"),
        }
        if not destination["dropoff_postcode"]:
            return None

        parcel_attrs = []
        for p in parcels:
            parcel_attrs.append({
                "qty": 1,
                "weight": p.get("weight", 1.0),
                "length": p.get("length", 0.1),
                "width": p.get("width", 0.1),
                "depth": p.get("depth", 0.1),
            })

        payload = {"quote": {**destination, "parcel_attributes": parcel_attrs}}

        try:
            async with session.post(
                f"{self.base_url}/quotes",
                headers={**self.headers, "Content-Type": "application/json"},
                json=payload,
            ) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                quotes = data.get("response", [])

                # Match the courier_type used for this order
                order_courier = order.get("courier_type", "").lower()
                for q in quotes:
                    if not q.get("success"):
                        continue
                    qt = (q.get("courier_type") or "").lower()
                    if qt == order_courier or (
                        order_courier == "standard" and "eparcel" in qt.lower()
                    ):
                        quote_list = q.get("quotes", [])
                        if quote_list:
                            return float(quote_list[0]["price"])

                # Fallback: return cheapest successful quote
                for q in quotes:
                    if q.get("success") and q.get("quotes"):
                        return float(q["quotes"][0]["price"])

        except Exception as e:
            log.debug(f"Quote API error: {e}")

        await asyncio.sleep(0.2)
        return None

    def _normalize_order(self, raw: Dict) -> Dict:
        """Extract the fields we care about from a Shippit order response."""
        parcels = raw.get("parcel_attributes", [])
        tracking = None
        if parcels:
            tracking = parcels[0].get("label_number")

        # retailer_reference format: "shopify_graphql_id|shopify_order_id"
        retailer_ref = raw.get("retailer_reference", "")
        parts = retailer_ref.split("|") if "|" in retailer_ref else []
        shopify_order_id_str = parts[1] if len(parts) > 1 else ""

        return {
            "tracking_number": tracking,
            "retailer_order_number": raw.get("retailer_invoice"),
            "shopify_order_id_from_ref": shopify_order_id_str,
            "courier_name": raw.get("courier_allocation"),
            "courier_type": raw.get("courier_type"),
            "service_level": raw.get("service_level"),
            "shipping_cost": None,  # populated by _estimate_shipping_cost
            "state": raw.get("state"),
            "parcel_count": len(parcels) or 1,
            "created_at": raw.get("created_at"),
            "delivered_at": raw.get("delivered_at"),
            "raw_response": raw,
        }
