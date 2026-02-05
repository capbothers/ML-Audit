"""
Google Ads Sheet Import Service

Reads campaign data from a Google Sheet (populated by Google Ads Scripts)
and upserts into the google_ads_campaigns table.

This is the automated alternative to manual CSV export/import.
The ads manager sets up a Google Ads Script that writes to a shared Sheet,
then our system reads from that Sheet on demand or on schedule.
"""
import logging
import re
import time
from datetime import datetime, date as date_type
from typing import Dict, List, Optional, Tuple

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import httplib2
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False

from sqlalchemy.orm import Session

from app.models.base import SessionLocal, Base, engine
from app.models.google_ads_data import GoogleAdsCampaign, GoogleAdsProductPerformance
from app.models.google_ads_import import GoogleAdsImportLog

logger = logging.getLogger(__name__)

# Timeout for Google Sheets API calls
SHEETS_API_TIMEOUT = 30

# Column mapping: sheet header (lowercase) → internal field name
SHEET_COLUMN_MAP = {
    "date": "date",
    "day": "date",
    "campaign id": "campaign_id",
    "campaign name": "campaign_name",
    "campaign": "campaign_name",
    "campaign type": "campaign_type",
    "campaign status": "campaign_status",
    "campaign state": "campaign_status",
    "status": "campaign_status",
    "impressions": "impressions",
    "impr": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conversions": "conversions",
    "conv": "conversions",
    "conv. value": "conversions_value",
    "conv value": "conversions_value",
    "conversion value": "conversions_value",
    "conversions value": "conversions_value",
    "total conv value": "conversions_value",
    "ctr": "ctr",
    "avg. cpc": "avg_cpc",
    "avg cpc": "avg_cpc",
    "average cpc": "avg_cpc",
    "conv. rate": "conversion_rate",
    "conv rate": "conversion_rate",
    "conversion rate": "conversion_rate",
    "search impr. share": "search_impression_share",
    "search impr share": "search_impression_share",
    "search impression share": "search_impression_share",
    "search lost is (budget)": "search_budget_lost_impression_share",
    "search lost abs top is (budget)": "search_budget_lost_impression_share",
    "search budget lost is": "search_budget_lost_impression_share",
    "search lost is (rank)": "search_rank_lost_impression_share",
    "search lost abs top is (rank)": "search_rank_lost_impression_share",
    "search rank lost is": "search_rank_lost_impression_share",
}

# Product tab column mapping (aggregated format — no daily Date column)
PRODUCT_COLUMN_MAP = {
    "date": "date",
    "day": "date",
    "period end": "period_end",
    "period start": "period_start",
    "product id": "product_item_id",
    "product item id": "product_item_id",
    "item id": "product_item_id",
    "offer id": "product_item_id",
    "product title": "product_title",
    "title": "product_title",
    "campaign id": "campaign_id",
    "campaign name": "campaign_name",
    "campaign": "campaign_name",
    "impressions": "impressions",
    "impr": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conversions": "conversions",
    "conv": "conversions",
    "conv. value": "conversions_value",
    "conv value": "conversions_value",
    "conversion value": "conversions_value",
    "conversions value": "conversions_value",
}


class GoogleAdsSheetImportService:
    """Reads Google Ads campaign data from a Google Sheet and upserts into DB."""

    def __init__(self, db: Session = None):
        self._external_db = db

    def import_from_sheet(
        self,
        sheet_id: str,
        credentials_path: str,
        tab_name: str = "Campaign Data",
    ) -> Dict:
        """
        Read campaign data from a Google Sheet and upsert into google_ads_campaigns.

        Args:
            sheet_id: Google Sheets spreadsheet ID
            credentials_path: Path to service account JSON credentials
            tab_name: Name of the tab containing campaign data

        Returns:
            Dict with import results
        """
        if not GOOGLE_AVAILABLE:
            return {
                "success": False,
                "error": (
                    "Google API libraries not installed. "
                    "Install with: pip install google-auth google-auth-httplib2 google-api-python-client"
                ),
            }

        db = self._external_db or SessionLocal()
        own_session = self._external_db is None

        # Ensure tables exist
        Base.metadata.create_all(bind=engine)

        try:
            # Authenticate
            service = self._authenticate(credentials_path)
            if not service:
                return {"success": False, "error": "Google Sheets authentication failed"}

            # Read data from sheet
            rows = self._read_sheet(service, sheet_id, tab_name)
            if rows is None:
                return {
                    "success": False,
                    "error": f"Could not read tab '{tab_name}' from sheet",
                }

            if len(rows) < 2:
                return {
                    "success": True,
                    "message": "Sheet has no data rows",
                    "rows_found": 0,
                }

            # Parse headers
            header_row = rows[0]
            col_map = self._build_column_map(header_row)

            if "campaign_id" not in col_map.values() or "date" not in col_map.values():
                return {
                    "success": False,
                    "error": (
                        f"Sheet is missing required columns. Found: {header_row}. "
                        "Need at least 'Campaign ID' and 'Date'."
                    ),
                }

            data_rows = rows[1:]
            logger.info(f"Google Ads Sheet import: {len(data_rows)} rows from '{tab_name}'")

            # Upsert
            counts = self._upsert_campaigns(data_rows, col_map, db)

            # Determine date range
            date_range = self._get_date_range(data_rows, col_map)

            # Log import
            self._log_import(
                db,
                sheet_id=sheet_id,
                tab_name=tab_name,
                status="success",
                counts=counts,
                date_range=date_range,
            )

            logger.info(
                f"Google Ads Sheet import done: "
                f"{counts['created']} created, {counts['updated']} updated, "
                f"{counts['skipped']} skipped, {counts['errored']} errors"
            )

            return {
                "success": True,
                "source": f"sheet:{sheet_id}/{tab_name}",
                "rows_found": len(data_rows),
                "rows_created": counts["created"],
                "rows_updated": counts["updated"],
                "rows_skipped": counts["skipped"],
                "rows_errored": counts["errored"],
                "date_range": date_range,
            }

        except Exception as e:
            logger.error(f"Google Ads Sheet import failed: {e}")
            try:
                self._log_import(
                    db,
                    sheet_id=sheet_id,
                    tab_name=tab_name,
                    status="failed",
                    error=str(e)[:500],
                )
            except Exception:
                pass
            return {"success": False, "error": str(e)}

        finally:
            if own_session:
                db.close()

    def import_products_from_sheet(
        self,
        sheet_id: str,
        credentials_path: str,
        tab_name: str = "Product Data",
    ) -> Dict:
        """
        Read product performance data from a Google Sheet and upsert into
        google_ads_products table.
        """
        if not GOOGLE_AVAILABLE:
            return {"success": False, "error": "Google API libraries not installed."}

        db = self._external_db or SessionLocal()
        own_session = self._external_db is None
        Base.metadata.create_all(bind=engine)

        try:
            service = self._authenticate(credentials_path)
            if not service:
                return {"success": False, "error": "Google Sheets authentication failed"}

            rows = self._read_sheet(service, sheet_id, tab_name)
            if rows is None:
                return {
                    "success": True,
                    "message": f"No '{tab_name}' tab found (product export may not have run yet)",
                    "rows_found": 0,
                }

            if len(rows) < 2:
                return {"success": True, "message": "Product tab has no data rows", "rows_found": 0}

            header_row = rows[0]
            col_map = self._build_column_map(header_row, column_map=PRODUCT_COLUMN_MAP)

            if "product_item_id" not in col_map.values():
                return {
                    "success": False,
                    "error": f"Product tab missing required columns. Found: {header_row}. Need 'Product ID'.",
                }

            data_rows = rows[1:]
            logger.info(f"Google Ads Product Sheet import: {len(data_rows)} rows from '{tab_name}'")

            counts = self._upsert_products(data_rows, col_map, db)
            date_range = self._get_date_range(data_rows, col_map)

            self._log_import(
                db, sheet_id=sheet_id, tab_name=tab_name,
                status="success", counts=counts, date_range=date_range,
                csv_type="products",
            )

            return {
                "success": True,
                "source": f"sheet:{sheet_id}/{tab_name}",
                "rows_found": len(data_rows),
                "rows_created": counts["created"],
                "rows_updated": counts["updated"],
                "rows_skipped": counts["skipped"],
                "rows_errored": counts["errored"],
                "date_range": date_range,
            }

        except Exception as e:
            logger.error(f"Google Ads Product Sheet import failed: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if own_session:
                db.close()

    def _upsert_products(
        self, rows: List[List[str]], col_map: Dict[int, str], db: Session,
    ) -> Dict[str, int]:
        """Upsert product rows into google_ads_products.

        Handles both daily format (has Date column) and aggregated format
        (has Period End column instead). For aggregated data, uses period_end
        as the date. Falls back to today's date if neither exists.
        """
        counts = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        # Determine date strategy: daily Date column, Period End, or today
        has_date_col = "date" in col_map.values()
        has_period_end = "period_end" in col_map.values()

        for row_idx, row in enumerate(rows):
            try:
                product_item_id = self._get_field(row, col_map, "product_item_id")
                campaign_id = self._get_field(row, col_map, "campaign_id")

                if not product_item_id:
                    counts["skipped"] += 1
                    continue

                campaign_id = str(campaign_id or "").strip()
                if campaign_id.endswith(".0"):
                    campaign_id = campaign_id[:-2]

                # Resolve date: prefer Date column, then Period End, then today
                date_str = None
                if has_date_col:
                    date_str = self._get_field(row, col_map, "date")
                if not date_str and has_period_end:
                    date_str = self._get_field(row, col_map, "period_end")

                row_date = _parse_date(date_str) if date_str else date_type.today()
                if not row_date:
                    row_date = date_type.today()

                existing = db.query(GoogleAdsProductPerformance).filter(
                    GoogleAdsProductPerformance.product_item_id == product_item_id,
                    GoogleAdsProductPerformance.campaign_id == campaign_id,
                    GoogleAdsProductPerformance.date == row_date,
                ).first()

                vals = dict(
                    campaign_id=campaign_id,
                    product_title=str(self._get_field(row, col_map, "product_title") or "").strip() or None,
                    campaign_name=str(self._get_field(row, col_map, "campaign_name") or "").strip() or None,
                    impressions=_parse_int(self._get_field(row, col_map, "impressions")),
                    clicks=_parse_int(self._get_field(row, col_map, "clicks")),
                    cost_micros=_parse_cost_to_micros(self._get_field(row, col_map, "cost")),
                    conversions=_parse_float(self._get_field(row, col_map, "conversions")),
                    conversions_value=_parse_float(self._get_field(row, col_map, "conversions_value")),
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    counts["updated"] += 1
                else:
                    obj = GoogleAdsProductPerformance(
                        product_item_id=product_item_id, date=row_date, **vals,
                    )
                    db.add(obj)
                    counts["created"] += 1

                if (counts["created"] + counts["updated"]) % 500 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"Product row {row_idx + 2} error: {e}")
                db.rollback()
                counts["errored"] += 1

        db.commit()
        return counts

    def _authenticate(self, credentials_path: str):
        """Authenticate with Google Sheets API via service account."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
            service = build(
                "sheets", "v4",
                credentials=credentials,
            )
            return service
        except Exception as e:
            logger.error(f"Google Sheets auth failed: {e}")
            return None

    def _read_sheet(self, service, sheet_id: str, tab_name: str) -> Optional[List[List[str]]]:
        """Read all rows from the specified tab."""
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=f"'{tab_name}'!A:P",
            ).execute()
            return result.get("values", [])
        except Exception as e:
            logger.error(f"Error reading sheet: {e}")
            return None

    def _build_column_map(self, headers: List[str], column_map: Dict = None) -> Dict[int, str]:
        """Map column index → internal field name."""
        mapping = column_map or SHEET_COLUMN_MAP
        col_map = {}
        for idx, header in enumerate(headers):
            norm = self._normalize_header(header)
            if norm in mapping:
                col_map[idx] = mapping[norm]
        return col_map

    def _upsert_campaigns(
        self, rows: List[List[str]], col_map: Dict[int, str], db: Session,
    ) -> Dict[str, int]:
        """Upsert campaign rows into google_ads_campaigns."""
        counts = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        for row_idx, row in enumerate(rows):
            try:
                # Extract required fields
                campaign_id = self._get_field(row, col_map, "campaign_id")
                date_str = self._get_field(row, col_map, "date")

                if not campaign_id or not date_str:
                    counts["skipped"] += 1
                    continue

                # Normalize campaign_id (strip .0 from float format)
                campaign_id = str(campaign_id).strip()
                if campaign_id.endswith(".0"):
                    campaign_id = campaign_id[:-2]

                row_date = _parse_date(date_str)
                if not row_date:
                    counts["skipped"] += 1
                    continue

                # Check for existing record
                existing = db.query(GoogleAdsCampaign).filter(
                    GoogleAdsCampaign.campaign_id == campaign_id,
                    GoogleAdsCampaign.date == row_date,
                ).first()

                vals = dict(
                    campaign_name=str(
                        self._get_field(row, col_map, "campaign_name") or "Unknown"
                    ).strip(),
                    campaign_type=self._get_field(row, col_map, "campaign_type") or None,
                    campaign_status=self._get_field(row, col_map, "campaign_status") or None,
                    impressions=_parse_int(self._get_field(row, col_map, "impressions")),
                    clicks=_parse_int(self._get_field(row, col_map, "clicks")),
                    cost_micros=_parse_cost_to_micros(
                        self._get_field(row, col_map, "cost")
                    ),
                    conversions=_parse_float(
                        self._get_field(row, col_map, "conversions")
                    ),
                    conversions_value=_parse_float(
                        self._get_field(row, col_map, "conversions_value")
                    ),
                    ctr=_parse_percentage(self._get_field(row, col_map, "ctr")),
                    avg_cpc=_parse_cost_dollars(
                        self._get_field(row, col_map, "avg_cpc")
                    ),
                    search_impression_share=_parse_percentage(
                        self._get_field(row, col_map, "search_impression_share")
                    ),
                    search_budget_lost_impression_share=_parse_percentage(
                        self._get_field(row, col_map, "search_budget_lost_impression_share")
                    ),
                    search_rank_lost_impression_share=_parse_percentage(
                        self._get_field(row, col_map, "search_rank_lost_impression_share")
                    ),
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    counts["updated"] += 1
                else:
                    obj = GoogleAdsCampaign(
                        campaign_id=campaign_id, date=row_date, **vals
                    )
                    db.add(obj)
                    counts["created"] += 1

                # Batch commit
                if (counts["created"] + counts["updated"]) % 500 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"Row {row_idx + 2} error: {e}")
                db.rollback()
                counts["errored"] += 1

        db.commit()
        return counts

    def _get_date_range(
        self, rows: List[List[str]], col_map: Dict[int, str],
    ) -> Optional[str]:
        """Get date range string from rows.

        Handles both daily format (Date column) and aggregated format
        (Period Start / Period End columns).
        """
        # Try daily Date column first
        dates = []
        for row in rows:
            d = _parse_date(self._get_field(row, col_map, "date"))
            if d:
                dates.append(d)
        if dates:
            return f"{min(dates)} to {max(dates)}"

        # Try Period Start / Period End (aggregated format)
        if "period_start" in col_map.values() or "period_end" in col_map.values():
            first_row = rows[0] if rows else None
            if first_row:
                start = _parse_date(self._get_field(first_row, col_map, "period_start"))
                end = _parse_date(self._get_field(first_row, col_map, "period_end"))
                if start and end:
                    return f"{start} to {end}"
                elif end:
                    return f"up to {end}"

        return None

    def _log_import(
        self,
        db: Session,
        sheet_id: str,
        tab_name: str,
        status: str,
        counts: Dict[str, int] = None,
        date_range: str = None,
        error: str = None,
        csv_type: str = "campaigns",
    ):
        """Log import to google_ads_import_log."""
        counts = counts or {}
        entry = GoogleAdsImportLog(
            filename=f"sheet:{sheet_id}/{tab_name}",
            checksum=f"sheet_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            status=status,
            csv_type=csv_type,
            rows_imported=counts.get("created", 0),
            rows_updated=counts.get("updated", 0),
            rows_skipped=counts.get("skipped", 0),
            rows_errored=counts.get("errored", 0),
            date_range=date_range,
            error=error,
            imported_at=datetime.utcnow(),
        )
        db.add(entry)
        db.commit()

    @staticmethod
    def _get_field(
        row: List[str], col_map: Dict[int, str], field_name: str,
    ) -> Optional[str]:
        """Get a field value from a row by field name."""
        for idx, fname in col_map.items():
            if fname == field_name and idx < len(row):
                val = row[idx].strip() if row[idx] else None
                return val if val else None
        return None

    @staticmethod
    def _normalize_header(header: str) -> str:
        """Normalize header for matching."""
        s = str(header).strip().lower()
        s = s.rstrip(".")
        s = re.sub(r"\s+", " ", s)
        return s


# ── Value parsers (same logic as CSV import) ─────────────────────

def _parse_int(value) -> int:
    if not value:
        return 0
    s = str(value).replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _parse_float(value) -> float:
    if not value:
        return 0.0
    s = str(value).replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse_cost_to_micros(cost_value) -> int:
    """Convert dollar cost to micros. $10.50 -> 10_500_000."""
    if not cost_value:
        return 0
    s = str(cost_value).replace("$", "").replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0
    try:
        return int(float(s) * 1_000_000)
    except (ValueError, TypeError):
        return 0


def _parse_cost_dollars(cost_value) -> Optional[float]:
    """Parse dollar value to float."""
    if not cost_value:
        return None
    s = str(cost_value).replace("$", "").replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_percentage(value) -> Optional[float]:
    """Parse percentage string. '45.2%' -> 45.2, '--' -> None."""
    if not value:
        return None
    s = str(value).strip()
    if not s or s == "--" or s.startswith("<"):
        return None
    if s.endswith("%"):
        try:
            return float(s.rstrip("%").replace(",", "").strip())
        except (ValueError, TypeError):
            return None
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_date(value) -> Optional[date_type]:
    """Parse date from various formats."""
    if not value:
        return None
    if isinstance(value, (datetime, date_type)):
        return value if isinstance(value, date_type) else value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None
