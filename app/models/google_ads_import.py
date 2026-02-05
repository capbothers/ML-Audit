"""
Google Ads CSV Import Log — tracks every file import for deduplication and auditing.
"""
from sqlalchemy import Column, Integer, String, DateTime, Text
from datetime import datetime
from app.models.base import Base


class GoogleAdsImportLog(Base):
    __tablename__ = "google_ads_import_log"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False, index=True)
    checksum = Column(String(64), nullable=False, index=True)   # SHA-256
    status = Column(String(20), nullable=False, index=True)      # success | failed | skipped
    csv_type = Column(String(30), nullable=True)                 # campaigns | ad_groups | products | search_terms
    rows_imported = Column(Integer, default=0)
    rows_updated = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    rows_errored = Column(Integer, default=0)
    date_range = Column(String, nullable=True)                   # e.g. "2025-01-01 to 2025-01-31"
    error = Column(Text, nullable=True)
    imported_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<GoogleAdsImportLog {self.filename} [{self.status}]>"
