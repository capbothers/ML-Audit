"""
Base database model and session management
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import get_settings

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


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
