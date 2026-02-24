"""
Google Sheets Connector

Syncs product cost data from Google Sheets.
Critical for Product Profitability module.
"""
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import os
import time
import concurrent.futures

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import httplib2
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

from sqlalchemy.orm import Session

from app.connectors.base import BaseConnector
from app.models.product_cost import ProductCost
from app.utils.logger import log

# Timeout for individual Google Sheets API calls (seconds)
SHEETS_API_TIMEOUT = 30


class GoogleSheetsConnector(BaseConnector):
    """
    Connector for Google Sheets API

    Pulls product cost data from supplier pricing sheet.
    Automatically detects vendor tabs by validating header structure.
    """

    # Core required headers that identify a valid vendor pricing tab
    # These must all be present (normalized: lowercase, no punctuation, collapsed whitespace)
    # Flexible matching allows minor variations in header names
    CORE_REQUIRED_HEADERS = {
        "vendor",
        "sku",
        "nett nett cost inc gst",
        "min margin",          # Matches "min margin %", "min margin", etc.
        "minimum",
        "discount off rrp",    # Matches "discount off rrp %", "discount off rrp", etc.
        "do not follow",
        "set price",
    }

    def __init__(
        self,
        db: Session,
        credentials_path: str,
        sheet_id: str,
        sheet_range: str = "A:AZ",  # Extended to include all columns (Set Price may be in col AB)
        tab_prefix: str = ""  # Not used - tabs are detected by headers
    ):
        """
        Initialize Google Sheets connector

        Args:
            db: Database session
            credentials_path: Path to service account JSON credentials
            sheet_id: Google Sheets spreadsheet ID
            sheet_range: Column range to read (e.g., "A:AA")
            tab_prefix: Deprecated - tabs are now detected by header validation
        """
        super().__init__(db, source_name="google_sheets_costs", source_type="feed")

        if not GOOGLE_AVAILABLE:
            raise ImportError(
                "Google API libraries not installed. "
                "Install with: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client"
            )

        # Validate config upfront — fail fast instead of hanging
        if not credentials_path or not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Google Sheets credentials file not found: {credentials_path!r}. "
                f"Set GOOGLE_SHEETS_CREDENTIALS_PATH in .env"
            )
        if not sheet_id or sheet_id == "your_spreadsheet_id":
            raise ValueError(
                "COST_SHEET_ID is not configured. Set it in .env to the Google Sheets spreadsheet ID."
            )

        self.credentials_path = credentials_path
        self.sheet_id = sheet_id
        # If range includes a sheet name, only keep the cell range
        self.sheet_range = sheet_range.split("!", 1)[-1]
        self.service = None

        # Header mapping (normalized header -> field name)
        # Keys are normalized: lowercase, no punctuation, collapsed whitespace
        self.header_map = {
            "vendor": "vendor",
            "sku": "vendor_sku",
            "item category": "item_category",
            "description": "description",
            "ean": "ean",
            "gst free": "gst_free",
            "rrp inc gst": "rrp_inc_gst",
            "invoice price inc gst": "invoice_price_inc_gst",
            "special cost inc gst": "special_cost_inc_gst",
            "discount": "discount",
            "additional discount": "additional_discount",
            "extra discount": "extra_discount",
            "rebate": "rebate",
            "extra": "extra",
            "settlement": "settlement",
            "crf": "crf",
            "loyalty": "loyalty",
            "advertising": "advertising",
            "timed settlement fee": "timed_settlement_fee",
            "other": "other",
            "nett nett cost inc gst": "nett_nett_cost_inc_gst",
            "min margin": "min_margin_pct",           # Matches "min margin %", "min margin", etc.
            "minimum": "minimum_price",
            "discount off rrp": "discount_off_rrp_pct",  # Matches "discount off rrp %", etc.
            "do not follow": "do_not_follow",
            "comments": "comments",
            "set price": "set_price",
            # Supply chain parameters (optional enrichment)
            "lead time days": "lead_time_days",
            "service level": "service_level",
            "moq": "moq",
            "case pack": "case_pack",
        }

    def _execute_with_timeout(self, request, timeout: int = SHEETS_API_TIMEOUT):
        """
        Execute a Google API request with a timeout.

        The google-api-python-client uses blocking httplib2 calls with no
        built-in timeout, which can hang indefinitely on network issues.
        This wraps the call in a thread with a hard timeout.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(request.execute)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(
                    f"Google Sheets API call timed out after {timeout}s. "
                    "Check network connectivity and credentials."
                )

    async def authenticate(self) -> bool:
        """
        Authenticate with Google Sheets API using service account.

        Returns:
            True if authenticated successfully
        """
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )

            # Build with a timeout-aware http transport
            import google_auth_httplib2
            http = httplib2.Http(timeout=SHEETS_API_TIMEOUT)
            authed_http = google_auth_httplib2.AuthorizedHttp(credentials, http=http)
            self.service = build(
                'sheets', 'v4',
                http=authed_http,
            )

            # Test authentication with a timeout-guarded call
            result = self._execute_with_timeout(
                self.service.spreadsheets().get(spreadsheetId=self.sheet_id)
            )

            sheet_title = result.get('properties', {}).get('title', 'Unknown')

            log.info(f"Authenticated with Google Sheets: {sheet_title}")
            self._authenticated = True
            return True

        except FileNotFoundError:
            log.error(f"Google Sheets credentials file not found: {self.credentials_path}")
            return False
        except TimeoutError as e:
            log.error(f"Google Sheets authentication timed out: {e}")
            return False
        except Exception as e:
            log.error(f"Google Sheets authentication failed: {str(e)}")
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync product cost data from Google Sheets

        Automatically detects vendor tabs by checking if they have the required headers.
        Skips tabs that don't match the expected header structure.

        Args:
            start_date: Not used (sheet is always full sync)
            end_date: Not used

        Returns:
            Dict with sync results
        """
        sync_start = time.time()

        try:
            await self.log_sync_start()

            # Authenticate if not already
            if not self.is_authenticated():
                authenticated = await self.authenticate()
                if not authenticated:
                    await self.log_sync_failure("Authentication failed")
                    return {"success": False, "error": "Authentication failed"}

            log.info(f"Syncing cost data from Google Sheets (range: {self.sheet_range})")

            # Get all sheet titles
            all_titles = self._get_all_sheet_titles()
            if not all_titles:
                log.warning("No sheets found in spreadsheet")
                await self.log_sync_success(records_synced=0)
                return {"success": True, "records_synced": 0, "message": "No sheets found"}

            synced = 0
            errors = 0
            skipped = 0
            tabs_processed = 0
            tabs_skipped = 0

            for sheet_title in all_titles:
                # Read header row to check if this is a vendor tab
                sheet_range = f"'{sheet_title}'!{self.sheet_range}"
                try:
                    result = self._execute_with_timeout(
                        self.service.spreadsheets().values().get(
                            spreadsheetId=self.sheet_id,
                            range=sheet_range
                        ),
                        timeout=60,  # larger timeout for data-heavy tabs
                    )
                    # Rate limit: 60 requests/minute = 1 per second
                    time.sleep(1.1)
                except TimeoutError:
                    log.warning(f"Timeout reading sheet '{sheet_title}' — skipping")
                    tabs_skipped += 1
                    continue
                except Exception as e:
                    log.warning(f"Could not read sheet '{sheet_title}': {str(e)}")
                    tabs_skipped += 1
                    # On rate limit error, wait before continuing
                    if "429" in str(e) or "Quota exceeded" in str(e):
                        log.info("Rate limited, waiting 60 seconds...")
                        time.sleep(60)
                    continue

                rows = result.get('values', [])
                if not rows:
                    log.debug(f"Empty sheet: {sheet_title}")
                    tabs_skipped += 1
                    continue

                header_row = rows[0]

                # Validate headers - skip if not a vendor tab
                if not self._has_required_headers(header_row):
                    log.info(f"Skipping tab '{sheet_title}' - doesn't have required headers")
                    tabs_skipped += 1
                    continue

                # This is a valid vendor tab - process it
                log.info(f"Processing vendor tab: {sheet_title}")
                tabs_processed += 1

                col_map = self._build_column_map(header_row)
                data_rows = rows[1:] if len(rows) > 1 else []

                for row_idx, row in enumerate(data_rows, start=2):
                    try:
                        if not self._has_sku(row, col_map):
                            skipped += 1
                            continue

                        await self._save_product_cost(row, col_map, sheet_title)
                        synced += 1
                    except Exception as e:
                        # Rollback the session to recover from integrity errors
                        self.db.rollback()
                        log.error(f"Error processing {sheet_title} row {row_idx}: {str(e)}")
                        errors += 1

            sync_duration = time.time() - sync_start

            # Log success
            await self.log_sync_success(
                records_synced=synced,
                latest_data_timestamp=datetime.utcnow(),
                sync_duration_seconds=sync_duration
            )

            log.info(
                f"Google Sheets sync complete: {synced} products from {tabs_processed} vendor tabs, "
                f"{skipped} rows skipped, {errors} errors, {tabs_skipped} non-vendor tabs skipped "
                f"in {sync_duration:.1f}s"
            )

            return {
                "success": True,
                "records_synced": synced,
                "records_skipped": skipped,
                "errors": errors,
                "tabs_processed": tabs_processed,
                "tabs_skipped": tabs_skipped,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            log.error(f"Google Sheets sync failed: {str(e)}")
            await self.log_sync_failure(str(e))
            return {"success": False, "error": str(e)}

    async def _save_product_cost(self, row: List[str], col_map: Dict[int, str], sheet_title: str) -> ProductCost:
        """
        Save or update product cost from sheet row

        Args:
            row: Row data from sheet
            col_map: Column index to field name mapping
            sheet_title: Name of the sheet (used as fallback vendor)

        Returns:
            ProductCost record

        Raises:
            ValueError: If SKU or Nett Nett Cost is missing
        """
        vendor_sku = self._get_value(row, col_map, "vendor_sku")

        if not vendor_sku:
            raise ValueError("Missing vendor SKU")

        # Check nett cost BEFORE any database operations
        nett_cost_value = self._get_value(row, col_map, "nett_nett_cost_inc_gst")
        nett_cost = self._parse_decimal(nett_cost_value) if nett_cost_value else None

        if nett_cost is None:
            raise ValueError(f"Missing Nett Nett Cost Inc GST for SKU {vendor_sku}")

        # Check if product cost exists
        product_cost = self.db.query(ProductCost).filter(
            ProductCost.vendor_sku == vendor_sku
        ).first()

        if not product_cost:
            product_cost = ProductCost(vendor_sku=vendor_sku)
            self.db.add(product_cost)

        # Map columns to fields
        for col_idx, field_name in col_map.items():
            if col_idx >= len(row):
                continue  # Column not present in this row

            value = row[col_idx].strip() if row[col_idx] else None

            # Skip if empty
            if not value:
                continue

            # Type conversions
            try:
                if field_name in [
                    "rrp_inc_gst",
                    "invoice_price_inc_gst",
                    "special_cost_inc_gst",
                    "nett_nett_cost_inc_gst",
                    "minimum_price",
                    "discount",
                    "additional_discount",
                    "extra_discount",
                    "rebate",
                    "extra",
                    "settlement",
                    "crf",
                    "loyalty",
                    "advertising",
                    "timed_settlement_fee",
                    "other",
                    "set_price"
                ]:
                    # Decimal fields
                    setattr(product_cost, field_name, self._parse_decimal(value))

                elif field_name in [
                    "min_margin_pct",
                    "discount_off_rrp_pct"
                ]:
                    # Percentage fields
                    setattr(product_cost, field_name, self._parse_percentage(value))

                elif field_name in ["gst_free", "do_not_follow"]:
                    # Boolean field
                    setattr(product_cost, field_name, value.upper() in ["YES", "Y", "TRUE", "1", "X"])

                else:
                    # String fields
                    setattr(product_cost, field_name, value)

            except Exception as e:
                log.warning(f"Error parsing {field_name} = '{value}': {str(e)}")
                # Continue with other fields

        # Ensure vendor is set (fallback to tab name)
        if not product_cost.vendor:
            product_cost.vendor = sheet_title

        # Update special status
        product_cost.update_special_status()

        # Update sync timestamp
        product_cost.last_synced = datetime.utcnow()

        self.db.commit()

        return product_cost

    def _parse_decimal(self, value: str) -> Optional[Decimal]:
        """
        Parse decimal value from string

        Handles:
        - Currency symbols ($)
        - Commas (1,234.56)
        - Empty strings

        Args:
            value: String value

        Returns:
            Decimal or None
        """
        if not value:
            return None

        try:
            # Remove currency symbols and commas
            cleaned = value.replace("$", "").replace(",", "").strip()

            if not cleaned:
                return None

            return Decimal(cleaned)

        except (ValueError, InvalidOperation):
            log.warning(f"Could not parse decimal: {value}")
            return None

    def _parse_percentage(self, value: str) -> Optional[Decimal]:
        """
        Parse percentage value

        Handles:
        - Percentage symbols (15%)
        - Decimal percentages (0.15)

        Args:
            value: String value

        Returns:
            Decimal (as percentage, e.g., 15.0 for 15%)
        """
        if not value:
            return None

        try:
            # Remove % symbol
            cleaned = value.replace("%", "").strip()

            if not cleaned:
                return None

            decimal_val = Decimal(cleaned)

            # If value is 0-1 range, convert to percentage
            if decimal_val < 1:
                decimal_val = decimal_val * 100

            return decimal_val

        except (ValueError, InvalidOperation):
            log.warning(f"Could not parse percentage: {value}")
            return None

    def _normalize_header(self, header: str) -> str:
        """
        Normalize header for comparison.
        - lowercase
        - trim whitespace
        - collapse multiple spaces
        - remove punctuation (%, commas, etc.)
        """
        import re
        if not header:
            return ""
        # Lowercase and strip
        normalized = header.strip().lower()
        # Remove punctuation (keep alphanumeric and spaces)
        normalized = re.sub(r'[^\w\s]', '', normalized)
        # Collapse multiple spaces
        normalized = " ".join(normalized.split())
        return normalized

    def _has_required_headers(self, headers: List[str]) -> bool:
        """
        Check if the header row contains the core required headers.
        Uses flexible matching - headers just need to contain the core terms.

        Args:
            headers: List of header strings from the first row

        Returns:
            True if all core required headers are present
        """
        normalized_headers = {self._normalize_header(h) for h in headers if h}

        # For each core required header, check if ANY header contains it
        missing = []
        for required in self.CORE_REQUIRED_HEADERS:
            found = False
            for header in normalized_headers:
                # Check if required term is contained in the header
                if required in header:
                    found = True
                    break
            if not found:
                missing.append(required)

        if missing:
            log.debug(f"Missing core headers: {missing}")
            return False

        return True

    def _build_column_map(self, headers: List[str]) -> Dict[int, str]:
        """
        Build mapping from column index to field name.
        Uses flexible matching - header just needs to contain the key.
        """
        col_map: Dict[int, str] = {}
        for idx, header in enumerate(headers):
            normalized = self._normalize_header(header)
            if not normalized:
                continue

            # Try exact match first
            if normalized in self.header_map:
                col_map[idx] = self.header_map[normalized]
                continue

            # Try flexible match (header contains key)
            for key, field_name in self.header_map.items():
                if key in normalized:
                    col_map[idx] = field_name
                    break

        return col_map

    def _get_all_sheet_titles(self) -> List[str]:
        """Get all sheet titles in the spreadsheet (with timeout)."""
        if not self.service:
            return []

        result = self._execute_with_timeout(
            self.service.spreadsheets().get(spreadsheetId=self.sheet_id)
        )
        sheets = result.get("sheets", [])
        return [s.get("properties", {}).get("title", "") for s in sheets]

    def _get_value(self, row: List[str], col_map: Dict[int, str], field_name: str) -> Optional[str]:
        """Get value from row by field name."""
        for idx, field in col_map.items():
            if field == field_name and idx < len(row):
                return row[idx].strip() if row[idx] else None
        return None

    def _has_sku(self, row: List[str], col_map: Dict[int, str]) -> bool:
        """Check if row has a SKU value."""
        return bool(self._get_value(row, col_map, "vendor_sku"))

    def _parse_date(self, value: str) -> Optional[date]:
        """
        Parse date from various formats

        Args:
            value: Date string

        Returns:
            date object or None
        """
        if not value:
            return None

        # Try common formats
        formats = [
            "%d/%m/%Y",   # 31/12/2024
            "%d-%m-%Y",   # 31-12-2024
            "%Y-%m-%d",   # 2024-12-31
            "%d/%m/%y",   # 31/12/24
            "%d %b %Y",   # 31 Dec 2024
            "%d %B %Y",   # 31 December 2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

        log.warning(f"Could not parse date: {value}")
        return None
