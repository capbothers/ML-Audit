"""
Caprice Import Service — automated daily import of pricing files.

Scans /imports/new-sheets for unprocessed .xlsx files, imports them
using the proven import_caprice_file() logic, logs results to
caprice_import_log, and moves files to processed/ or failed/.
"""
import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from sqlalchemy.orm import Session

from app.models.base import SessionLocal, Base, engine
from app.models.caprice_import import CapriceImportLog

logger = logging.getLogger(__name__)

# Directory layout (relative to project root)
BASE_DIR = Path(__file__).parent.parent.parent          # /workspaces/ML-Audit
INBOX_DIR = BASE_DIR / "imports" / "new-sheets"
PROCESSED_DIR = BASE_DIR / "imports" / "processed"
FAILED_DIR = BASE_DIR / "imports" / "failed"


def _ensure_dirs():
    """Create processed/failed dirs if they don't exist."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)


def _file_checksum(path: Path) -> str:
    """SHA-256 checksum of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_imported(checksum: str, db: Session) -> bool:
    """Check if a file with this checksum was already imported successfully."""
    return db.query(CapriceImportLog).filter(
        CapriceImportLog.checksum == checksum,
        CapriceImportLog.status == "success",
    ).first() is not None


class CapriceImportService:
    """Orchestrates the scan → deduplicate → import → move → log cycle."""

    def __init__(self, db: Session | None = None):
        self._external_db = db

    # ── public entry point ──────────────────────────────────────────
    def run_import(self) -> Dict:
        """
        Scan the inbox, import new files, log results.

        Returns a summary dict suitable for the API response.
        """
        _ensure_dirs()

        db = self._external_db or SessionLocal()
        own_session = self._external_db is None

        # Make sure the log table exists
        Base.metadata.create_all(bind=engine)

        files = sorted(INBOX_DIR.glob("*.xlsx"))
        if not files:
            logger.info("Caprice import: no new files in inbox")
            return {"files_found": 0, "results": [], "message": "No new files"}

        logger.info(f"Caprice import: found {len(files)} file(s) in inbox")
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
                f"Caprice import done: {summary['imported']} imported, "
                f"{summary['skipped']} skipped, {summary['failed']} failed"
            )
            return summary

        finally:
            if own_session:
                db.close()

    # ── per-file processing ─────────────────────────────────────────
    def _process_one(self, fp: Path, db: Session) -> Dict:
        filename = fp.name
        checksum = _file_checksum(fp)

        # Duplicate check
        if _already_imported(checksum, db):
            logger.info(f"  Skipping {filename} (checksum already imported)")
            self._log(db, filename, checksum, "skipped", note="Duplicate checksum")
            self._move(fp, PROCESSED_DIR)
            return {"filename": filename, "status": "skipped", "reason": "duplicate"}

        # Import via existing proven logic
        try:
            from scripts.import_data import import_caprice_file
            result = import_caprice_file(str(fp), db)
        except Exception as exc:
            error_msg = str(exc)[:500]
            logger.error(f"  FAILED {filename}: {error_msg}")
            self._log(db, filename, checksum, "failed", error=error_msg)
            self._move(fp, FAILED_DIR)
            return {"filename": filename, "status": "failed", "error": error_msg}

        if result.get("success"):
            self._log(
                db, filename, checksum, "success",
                rows_imported=result.get("imported", 0),
                rows_updated=result.get("updated", 0),
                rows_skipped=result.get("skipped", 0),
                rows_errored=result.get("errors", 0),
                pricing_date=str(result.get("pricing_date", "")),
            )
            self._move(fp, PROCESSED_DIR)
            return {
                "filename": filename,
                "status": "success",
                "pricing_date": str(result.get("pricing_date")),
                "rows_imported": result.get("imported", 0),
                "rows_updated": result.get("updated", 0),
            }
        else:
            error_msg = result.get("error", "Unknown error")
            self._log(db, filename, checksum, "failed", error=error_msg)
            self._move(fp, FAILED_DIR)
            return {"filename": filename, "status": "failed", "error": error_msg}

    # ── helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _move(fp: Path, dest_dir: Path):
        """Move file to destination, handling name collisions."""
        target = dest_dir / fp.name
        if target.exists():
            # Append timestamp to avoid overwrite
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
        rows_imported: int = 0,
        rows_updated: int = 0,
        rows_skipped: int = 0,
        rows_errored: int = 0,
        pricing_date: str = None,
        error: str = None,
        note: str = None,
    ):
        entry = CapriceImportLog(
            filename=filename,
            checksum=checksum,
            status=status,
            rows_imported=rows_imported,
            rows_updated=rows_updated,
            rows_skipped=rows_skipped,
            rows_errored=rows_errored,
            pricing_date=pricing_date,
            error=error or note,
            imported_at=datetime.utcnow(),
        )
        db.add(entry)
        db.commit()
