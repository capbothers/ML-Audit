"""
Base database model and session management
"""
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import get_settings

settings = get_settings()

# Create database engine
if settings.database_url.startswith("sqlite"):
    # SQLite works best with a single connection in this app's workload.
    engine = create_engine(
        settings.database_url,
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
