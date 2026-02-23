"""
Base database model and session management
"""
import logging
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# Resolve relative SQLite paths to absolute so cwd changes can't break it
_db_url = settings.database_url
if _db_url.startswith("sqlite:///") and not _db_url.startswith("sqlite:////"):
    rel_path = _db_url[len("sqlite:///"):]
    _db_url = "sqlite:///" + os.path.abspath(rel_path)

# Create database engine
if _db_url.startswith("sqlite"):
    # SQLite works best with a single connection in this app's workload.
    engine = create_engine(
        _db_url,
        connect_args={"check_same_thread": False, "timeout": 60},
        poolclass=NullPool,
        pool_pre_ping=True
    )
else:
    engine = create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=5,
        pool_recycle=300,
    )

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for all models
Base = declarative_base()


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.expire_all()
        db.close()


def _migrate_missing_columns():
    """Add columns defined in models but missing from existing DB tables.

    create_all() only creates missing *tables*; it cannot add new columns
    to tables that already exist.  This helper bridges the gap so that
    adding a Column to a model is sufficient — no manual ALTER TABLE needed.
    """
    inspector = inspect(engine)
    with engine.connect() as conn:
        for table_name, table in Base.metadata.tables.items():
            if not inspector.has_table(table_name):
                continue  # create_all will handle it
            existing = {c["name"] for c in inspector.get_columns(table_name)}
            for col in table.columns:
                if col.name not in existing:
                    # Build a minimal ALTER TABLE ADD COLUMN
                    col_type = col.type.compile(dialect=engine.dialect)
                    sql = f'ALTER TABLE {table_name} ADD COLUMN {col.name} {col_type}'
                    logger.info(f"Auto-migrating: {sql}")
                    conn.execute(text(sql))
        conn.commit()


def init_db():
    """Initialize database tables and auto-migrate new columns."""
    Base.metadata.create_all(bind=engine)
    _migrate_missing_columns()
