"""
Caprice Import Log — tracks every file import attempt for deduplication and auditing.
"""
from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime
from app.models.base import Base


class CapriceImportLog(Base):
    __tablename__ = "caprice_import_log"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False, index=True)
    checksum = Column(String(64), nullable=False, index=True)  # SHA-256 hex
    status = Column(String(20), nullable=False, index=True)     # success | failed | skipped
    rows_imported = Column(Integer, default=0)
    rows_updated = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    rows_errored = Column(Integer, default=0)
    pricing_date = Column(String, nullable=True)                # date extracted from filename
    error = Column(Text, nullable=True)
    imported_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<CapriceImportLog {self.filename} [{self.status}]>"
