"""
Google Ads CSV Import Service

Imports Google Ads data from CSV files exported via the Google Ads web UI.
Scans imports/google-ads/new/ for CSV files, auto-detects the report type
(campaigns, ad_groups, products, search_terms), maps columns, upserts into
the existing google_ads_* tables, and logs results.

Follows the same scan → deduplicate → import → move → log pattern as
CapriceImportService.
"""
import hashlib
import logging
import re
import shutil
from datetime import datetime, date as date_type
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from app.models.base import SessionLocal, Base, engine
from app.models.google_ads_import import GoogleAdsImportLog
from app.models.google_ads_data import (
    GoogleAdsCampaign,
    GoogleAdsAdGroup,
    GoogleAdsProductPerformance,
    GoogleAdsSearchTerm,
)

logger = logging.getLogger(__name__)

# ── Directory layout ─────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent.parent
INBOX_DIR = BASE_DIR / "imports" / "google-ads" / "new"
PROCESSED_DIR = BASE_DIR / "imports" / "google-ads" / "processed"
FAILED_DIR = BASE_DIR / "imports" / "google-ads" / "failed"

# ── Column mappings ──────────────────────────────────────────────────
# Keys are normalised header strings (lowercase, stripped, no trailing dots).
# Values are internal field names used during processing.

CAMPAIGN_COLUMN_MAP = {
    "campaign": "campaign_name",
    "campaign name": "campaign_name",
    "campaign id": "campaign_id",
    "campaign type": "campaign_type",
    "campaign state": "campaign_status",
    "campaign status": "campaign_status",
    "status": "campaign_status",
    "day": "date",
    "date": "date",
    "impr": "impressions",
    "impressions": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conv": "conversions",
    "conversions": "conversions",
    "conv value": "conversions_value",
    "conversion value": "conversions_value",
    "conversions value": "conversions_value",
    "total conv value": "conversions_value",
    "all conv value": "conversions_value",
    "ctr": "ctr",
    "avg cpc": "avg_cpc",
    "average cpc": "avg_cpc",
    "search impr share": "search_impression_share",
    "search impression share": "search_impression_share",
    "search lost is (budget)": "search_budget_lost_impression_share",
    "search lost is (rank)": "search_rank_lost_impression_share",
    "search lost abs top is (budget)": "search_budget_lost_impression_share",
    "search lost abs top is (rank)": "search_rank_lost_impression_share",
    "conversion rate": "conversion_rate",
    "conv rate": "conversion_rate",
}

AD_GROUP_COLUMN_MAP = {
    "ad group": "ad_group_name",
    "ad group name": "ad_group_name",
    "ad group id": "ad_group_id",
    "ad group state": "ad_group_status",
    "ad group status": "ad_group_status",
    "campaign": "campaign_name",
    "campaign id": "campaign_id",
    "day": "date",
    "date": "date",
    "impr": "impressions",
    "impressions": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conv": "conversions",
    "conversions": "conversions",
    "conv value": "conversions_value",
    "conversions value": "conversions_value",
    "conversion value": "conversions_value",
}

PRODUCT_COLUMN_MAP = {
    "item id": "product_item_id",
    "product item id": "product_item_id",
    "product id": "product_item_id",
    "offer id": "product_item_id",
    "campaign id": "campaign_id",
    "campaign": "campaign_name",
    "ad group id": "ad_group_id",
    "day": "date",
    "date": "date",
    "impr": "impressions",
    "impressions": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conv": "conversions",
    "conversions": "conversions",
    "conv value": "conversions_value",
    "conversions value": "conversions_value",
    "conversion value": "conversions_value",
}

SEARCH_TERM_COLUMN_MAP = {
    "search term": "search_term",
    "campaign": "campaign_name",
    "campaign id": "campaign_id",
    "ad group": "ad_group_name",
    "ad group id": "ad_group_id",
    "day": "date",
    "date": "date",
    "impr": "impressions",
    "impressions": "impressions",
    "clicks": "clicks",
    "cost": "cost",
    "conv": "conversions",
    "conversions": "conversions",
    "conv value": "conversions_value",
    "conversions value": "conversions_value",
    "conversion value": "conversions_value",
    "match type": "keyword_match_type",
    "keyword match type": "keyword_match_type",
}

# Columns whose presence indicates a specific CSV type.
# Checked in priority order in _detect_csv_type.
_KNOWN_HEADERS = set()
for _m in (CAMPAIGN_COLUMN_MAP, AD_GROUP_COLUMN_MAP, PRODUCT_COLUMN_MAP, SEARCH_TERM_COLUMN_MAP):
    _KNOWN_HEADERS.update(_m.keys())


# ── Helpers ──────────────────────────────────────────────────────────

def _ensure_dirs():
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)


def _file_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_imported(checksum: str, db: Session) -> bool:
    return db.query(GoogleAdsImportLog).filter(
        GoogleAdsImportLog.checksum == checksum,
        GoogleAdsImportLog.status == "success",
    ).first() is not None


def _normalize_header(header: str) -> str:
    """Normalise a CSV header for matching: lowercase, strip, remove trailing dots, collapse spaces."""
    s = str(header).strip().lower()
    s = s.rstrip(".")
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_int(value) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0
    s = str(value).replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _parse_float(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse_cost_to_micros(cost_value) -> int:
    """Convert a dollar cost value to micros (BigInteger). $10.50 → 10_500_000."""
    if cost_value is None or (isinstance(cost_value, float) and pd.isna(cost_value)):
        return 0
    s = str(cost_value).replace("$", "").replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return 0
    try:
        return int(float(s) * 1_000_000)
    except (ValueError, TypeError):
        return 0


def _parse_cost_dollars(cost_value) -> Optional[float]:
    """Parse a dollar value to float (for avg_cpc which is stored in dollars)."""
    if cost_value is None or (isinstance(cost_value, float) and pd.isna(cost_value)):
        return None
    s = str(cost_value).replace("$", "").replace(",", "").replace(" ", "").strip()
    if not s or s == "--":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_percentage(value) -> Optional[float]:
    """Parse a percentage string to a float (0-100 scale, matching existing DB convention).
    '45.2%' → 45.2, '0.452' → 0.452 (left as-is if already decimal), '--' → None.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
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
    """Parse date from CSV. Handles: 2025-01-15, Jan 15 2025, 15/01/2025, 01/15/2025."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, date_type)):
        return value if isinstance(value, date_type) else value.date()
    if isinstance(value, pd.Timestamp):
        return value.date()
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _extract_date_from_filename(filename: str) -> Optional[date_type]:
    """Try to pull a date from the filename, e.g. campaigns_2025-01-15.csv."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        return _parse_date(m.group(1))
    m = re.search(r"(\d{2})(\d{2})(\d{4})", filename)
    if m:
        try:
            return date_type(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


# ── Service ──────────────────────────────────────────────────────────

class GoogleAdsImportService:
    """Orchestrates scan → deduplicate → parse → upsert → move → log."""

    def __init__(self, db: Session = None):
        self._external_db = db

    def run_import(self) -> Dict:
        _ensure_dirs()
        db = self._external_db or SessionLocal()
        own_session = self._external_db is None

        # Ensure log table exists
        Base.metadata.create_all(bind=engine)

        files = sorted(INBOX_DIR.glob("*.csv"))
        if not files:
            logger.info("Google Ads CSV import: no new files in inbox")
            return {"files_found": 0, "results": [], "message": "No CSV files found in imports/google-ads/new/"}

        logger.info(f"Google Ads CSV import: found {len(files)} file(s)")
        results: List[Dict] = []

        try:
            for fp in files:
                result = self._process_one(fp, db)
                results.append(result)

            summary = {
                "files_found": len(files),
                "imported": sum(1 for r in results if r["status"] == "success"),
                "skipped": sum(1 for r in results if r["status"] == "skipped"),
                "failed": sum(1 for r in results if r["status"] == "failed"),
                "total_rows": sum(r.get("rows_imported", 0) + r.get("rows_updated", 0) for r in results),
                "results": results,
            }
            logger.info(
                f"Google Ads CSV import done: {summary['imported']} imported, "
                f"{summary['skipped']} skipped, {summary['failed']} failed"
            )
            return summary
        finally:
            if own_session:
                db.close()

    # ── Per-file processing ──────────────────────────────────────────

    def _process_one(self, fp: Path, db: Session) -> Dict:
        filename = fp.name
        checksum = _file_checksum(fp)

        if _already_imported(checksum, db):
            logger.info(f"  Skipping {filename} (already imported)")
            self._log(db, filename, checksum, "skipped", note="Duplicate checksum")
            self._move(fp, PROCESSED_DIR)
            return {"filename": filename, "status": "skipped", "reason": "duplicate"}

        try:
            csv_type, df = self._parse_csv(fp)
        except Exception as exc:
            error_msg = str(exc)[:500]
            logger.error(f"  FAILED to parse {filename}: {error_msg}")
            self._log(db, filename, checksum, "failed", error=error_msg)
            self._move(fp, FAILED_DIR)
            return {"filename": filename, "status": "failed", "error": error_msg}

        if df.empty:
            self._log(db, filename, checksum, "failed", csv_type=csv_type, error="No data rows after parsing")
            self._move(fp, FAILED_DIR)
            return {"filename": filename, "status": "failed", "error": "No data rows"}

        # Determine date range for logging
        date_range = None
        if "date" in df.columns:
            dates = df["date"].dropna()
            if len(dates) > 0:
                parsed_dates = [_parse_date(d) for d in dates]
                valid_dates = [d for d in parsed_dates if d is not None]
                if valid_dates:
                    date_range = f"{min(valid_dates)} to {max(valid_dates)}"

        # Upsert
        try:
            upsert_fn = {
                "campaigns": self._upsert_campaigns,
                "ad_groups": self._upsert_ad_groups,
                "products": self._upsert_products,
                "search_terms": self._upsert_search_terms,
            }[csv_type]
            counts = upsert_fn(df, db, fp.name)
        except Exception as exc:
            db.rollback()
            error_msg = str(exc)[:500]
            logger.error(f"  FAILED upsert for {filename}: {error_msg}")
            self._log(db, filename, checksum, "failed", csv_type=csv_type, error=error_msg)
            self._move(fp, FAILED_DIR)
            return {"filename": filename, "status": "failed", "csv_type": csv_type, "error": error_msg}

        self._log(
            db, filename, checksum, "success",
            csv_type=csv_type,
            rows_imported=counts.get("created", 0),
            rows_updated=counts.get("updated", 0),
            rows_skipped=counts.get("skipped", 0),
            rows_errored=counts.get("errored", 0),
            date_range=date_range,
        )
        self._move(fp, PROCESSED_DIR)
        logger.info(
            f"  OK {filename} ({csv_type}): "
            f"{counts.get('created', 0)} created, {counts.get('updated', 0)} updated, "
            f"{counts.get('skipped', 0)} skipped, {counts.get('errored', 0)} errors"
        )
        return {
            "filename": filename,
            "status": "success",
            "csv_type": csv_type,
            "rows_imported": counts.get("created", 0),
            "rows_updated": counts.get("updated", 0),
            "rows_skipped": counts.get("skipped", 0),
            "rows_errored": counts.get("errored", 0),
            "date_range": date_range,
        }

    # ── CSV Parsing ──────────────────────────────────────────────────

    def _parse_csv(self, fp: Path) -> Tuple[str, pd.DataFrame]:
        """Read CSV, detect header row, detect type, map columns, filter summary rows."""
        header_row = self._find_header_row(fp)
        df = pd.read_csv(fp, skiprows=header_row, encoding="utf-8-sig")

        # Normalise column headers
        raw_cols = list(df.columns)
        norm_cols = [_normalize_header(c) for c in raw_cols]

        csv_type = self._detect_csv_type(norm_cols)
        col_map = {
            "campaigns": CAMPAIGN_COLUMN_MAP,
            "ad_groups": AD_GROUP_COLUMN_MAP,
            "products": PRODUCT_COLUMN_MAP,
            "search_terms": SEARCH_TERM_COLUMN_MAP,
        }[csv_type]

        # Rename columns to internal names
        rename = {}
        for raw, norm in zip(raw_cols, norm_cols):
            if norm in col_map:
                rename[raw] = col_map[norm]
        df = df.rename(columns=rename)

        # If no date column, try filename date or fall back to today
        if "date" not in df.columns:
            fallback_date = _extract_date_from_filename(fp.name) or date_type.today()
            df["date"] = fallback_date
            logger.info(f"  No date column found, using {fallback_date}")

        # Drop summary/total rows
        df = self._filter_summary_rows(df, csv_type)

        return csv_type, df

    @staticmethod
    def _find_header_row(fp: Path) -> int:
        """Find the row index containing actual column headers (skip Google Ads metadata rows)."""
        with open(fp, "r", encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i > 10:
                    break
                lower = line.lower()
                # Header row should contain several known column names
                matches = sum(1 for kw in ("campaign", "clicks", "impr", "cost", "ad group",
                                           "search term", "item id", "product")
                              if kw in lower)
                if matches >= 2:
                    return i
        return 0  # Assume first row is header

    @staticmethod
    def _detect_csv_type(norm_cols: List[str]) -> str:
        """Detect CSV report type from normalised column names."""
        col_set = set(norm_cols)

        if "search term" in col_set:
            return "search_terms"
        if col_set & {"item id", "product item id", "product id", "offer id"}:
            return "products"
        if col_set & {"ad group id", "ad group"}:
            # Could be ad_groups or campaigns with ad group breakdown
            # If there's a dedicated ad group ID, it's ad_groups
            if "ad group id" in col_set:
                return "ad_groups"
            return "ad_groups"
        if col_set & {"campaign id", "campaign"}:
            return "campaigns"

        raise ValueError(f"Cannot detect CSV type from columns: {', '.join(norm_cols)}")

    @staticmethod
    def _filter_summary_rows(df: pd.DataFrame, csv_type: str) -> pd.DataFrame:
        """Remove Total/summary rows that Google Ads appends at the bottom."""
        # Check the main ID column for "Total" values
        id_cols = {
            "campaigns": "campaign_id",
            "ad_groups": "ad_group_id",
            "products": "product_item_id",
            "search_terms": "search_term",
        }
        id_col = id_cols.get(csv_type)

        if id_col and id_col in df.columns:
            mask = df[id_col].astype(str).str.strip().str.lower() != "total"
            df = df[mask]

        # Also check campaign_name for "Total"
        if "campaign_name" in df.columns:
            mask = df["campaign_name"].astype(str).str.strip().str.lower() != "total"
            df = df[mask]

        # Drop fully empty rows
        df = df.dropna(how="all")

        return df

    # ── Upsert methods ───────────────────────────────────────────────

    def _upsert_campaigns(self, df: pd.DataFrame, db: Session, filename: str) -> Dict:
        result = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        for idx, row in df.iterrows():
            campaign_id = str(row.get("campaign_id", "")).strip()
            row_date = _parse_date(row.get("date"))

            if not campaign_id or not row_date:
                result["skipped"] += 1
                continue

            try:
                existing = db.query(GoogleAdsCampaign).filter(
                    GoogleAdsCampaign.campaign_id == campaign_id,
                    GoogleAdsCampaign.date == row_date,
                ).first()

                vals = dict(
                    campaign_name=str(row.get("campaign_name", "Unknown")).strip(),
                    campaign_type=str(row.get("campaign_type", "")).strip() or None,
                    campaign_status=str(row.get("campaign_status", "")).strip() or None,
                    impressions=_parse_int(row.get("impressions")),
                    clicks=_parse_int(row.get("clicks")),
                    cost_micros=_parse_cost_to_micros(row.get("cost")),
                    conversions=_parse_float(row.get("conversions")),
                    conversions_value=_parse_float(row.get("conversions_value")),
                    ctr=_parse_percentage(row.get("ctr")),
                    avg_cpc=_parse_cost_dollars(row.get("avg_cpc")),
                    search_impression_share=_parse_percentage(row.get("search_impression_share")),
                    search_budget_lost_impression_share=_parse_percentage(row.get("search_budget_lost_impression_share")),
                    search_rank_lost_impression_share=_parse_percentage(row.get("search_rank_lost_impression_share")),
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    result["updated"] += 1
                else:
                    obj = GoogleAdsCampaign(campaign_id=campaign_id, date=row_date, **vals)
                    db.add(obj)
                    result["created"] += 1

                if (result["created"] + result["updated"]) % 1000 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"  Row {idx} error (campaign {campaign_id}): {e}")
                db.rollback()
                result["errored"] += 1

        db.commit()
        return result

    def _upsert_ad_groups(self, df: pd.DataFrame, db: Session, filename: str) -> Dict:
        result = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        for idx, row in df.iterrows():
            ad_group_id = str(row.get("ad_group_id", "")).strip()
            campaign_id = str(row.get("campaign_id", "")).strip()
            row_date = _parse_date(row.get("date"))

            if not ad_group_id or not row_date:
                result["skipped"] += 1
                continue

            try:
                existing = db.query(GoogleAdsAdGroup).filter(
                    GoogleAdsAdGroup.ad_group_id == ad_group_id,
                    GoogleAdsAdGroup.date == row_date,
                ).first()

                vals = dict(
                    ad_group_name=str(row.get("ad_group_name", "Unknown")).strip(),
                    campaign_id=campaign_id or (existing.campaign_id if existing else ""),
                    ad_group_status=str(row.get("ad_group_status", "")).strip() or None,
                    impressions=_parse_int(row.get("impressions")),
                    clicks=_parse_int(row.get("clicks")),
                    cost_micros=_parse_cost_to_micros(row.get("cost")),
                    conversions=_parse_float(row.get("conversions")),
                    conversions_value=_parse_float(row.get("conversions_value")),
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    result["updated"] += 1
                else:
                    obj = GoogleAdsAdGroup(ad_group_id=ad_group_id, date=row_date, **vals)
                    db.add(obj)
                    result["created"] += 1

                if (result["created"] + result["updated"]) % 1000 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"  Row {idx} error (ad_group {ad_group_id}): {e}")
                db.rollback()
                result["errored"] += 1

        db.commit()
        return result

    def _upsert_products(self, df: pd.DataFrame, db: Session, filename: str) -> Dict:
        result = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        for idx, row in df.iterrows():
            product_item_id = str(row.get("product_item_id", "")).strip()
            campaign_id = str(row.get("campaign_id", "")).strip()
            row_date = _parse_date(row.get("date"))

            if not product_item_id or not row_date:
                result["skipped"] += 1
                continue

            try:
                existing = db.query(GoogleAdsProductPerformance).filter(
                    GoogleAdsProductPerformance.product_item_id == product_item_id,
                    GoogleAdsProductPerformance.campaign_id == campaign_id,
                    GoogleAdsProductPerformance.date == row_date,
                ).first()

                vals = dict(
                    campaign_id=campaign_id,
                    ad_group_id=str(row.get("ad_group_id", "")).strip() or None,
                    impressions=_parse_int(row.get("impressions")),
                    clicks=_parse_int(row.get("clicks")),
                    cost_micros=_parse_cost_to_micros(row.get("cost")),
                    conversions=_parse_float(row.get("conversions")),
                    conversions_value=_parse_float(row.get("conversions_value")),
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    result["updated"] += 1
                else:
                    obj = GoogleAdsProductPerformance(
                        product_item_id=product_item_id, date=row_date, **vals,
                    )
                    db.add(obj)
                    result["created"] += 1

                if (result["created"] + result["updated"]) % 1000 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"  Row {idx} error (product {product_item_id}): {e}")
                db.rollback()
                result["errored"] += 1

        db.commit()
        return result

    def _upsert_search_terms(self, df: pd.DataFrame, db: Session, filename: str) -> Dict:
        result = {"created": 0, "updated": 0, "skipped": 0, "errored": 0}

        for idx, row in df.iterrows():
            search_term = str(row.get("search_term", "")).strip()
            campaign_id = str(row.get("campaign_id", "")).strip()
            ad_group_id = str(row.get("ad_group_id", "")).strip()
            row_date = _parse_date(row.get("date"))

            if not search_term or not row_date:
                result["skipped"] += 1
                continue

            try:
                existing = db.query(GoogleAdsSearchTerm).filter(
                    GoogleAdsSearchTerm.search_term == search_term,
                    GoogleAdsSearchTerm.campaign_id == campaign_id,
                    GoogleAdsSearchTerm.ad_group_id == ad_group_id,
                    GoogleAdsSearchTerm.date == row_date,
                ).first()

                vals = dict(
                    campaign_id=campaign_id or (existing.campaign_id if existing else ""),
                    ad_group_id=ad_group_id or (existing.ad_group_id if existing else ""),
                    impressions=_parse_int(row.get("impressions")),
                    clicks=_parse_int(row.get("clicks")),
                    cost_micros=_parse_cost_to_micros(row.get("cost")),
                    conversions=_parse_float(row.get("conversions")),
                    conversions_value=_parse_float(row.get("conversions_value")),
                    keyword_match_type=str(row.get("keyword_match_type", "")).strip() or None,
                    synced_at=datetime.utcnow(),
                )

                if existing:
                    for k, v in vals.items():
                        setattr(existing, k, v)
                    result["updated"] += 1
                else:
                    obj = GoogleAdsSearchTerm(
                        search_term=search_term, date=row_date, **vals,
                    )
                    db.add(obj)
                    result["created"] += 1

                if (result["created"] + result["updated"]) % 1000 == 0:
                    db.commit()

            except Exception as e:
                logger.warning(f"  Row {idx} error (search_term '{search_term}'): {e}")
                db.rollback()
                result["errored"] += 1

        db.commit()
        return result

    # ── Shared helpers ───────────────────────────────────────────────

    @staticmethod
    def _move(fp: Path, dest_dir: Path):
        target = dest_dir / fp.name
        if target.exists():
            stem = fp.stem
            suffix = fp.suffix
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            target = dest_dir / f"{stem}_{ts}{suffix}"
        shutil.move(str(fp), str(target))
        logger.info(f"  Moved {fp.name} → {target.parent.name}/")

    @staticmethod
    def _log(
        db: Session,
        filename: str,
        checksum: str,
        status: str,
        csv_type: str = None,
        rows_imported: int = 0,
        rows_updated: int = 0,
        rows_skipped: int = 0,
        rows_errored: int = 0,
        date_range: str = None,
        error: str = None,
        note: str = None,
    ):
        entry = GoogleAdsImportLog(
            filename=filename,
            checksum=checksum,
            status=status,
            csv_type=csv_type,
            rows_imported=rows_imported,
            rows_updated=rows_updated,
            rows_skipped=rows_skipped,
            rows_errored=rows_errored,
            date_range=date_range,
            error=error or note,
            imported_at=datetime.utcnow(),
        )
        db.add(entry)
        db.commit()
