"""
Logging configuration
"""
from loguru import logger
import sys
from app.config import get_settings

settings = get_settings()


def setup_logger():
    """Configure logger with appropriate settings"""
    logger.remove()  # Remove default handler

    # Console logging
    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=settings.log_level
    )

    # File logging
    logger.add(
        "logs/ml_audit_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        compression="zip",
        level="INFO"
    )

    # Error file
    logger.add(
        "logs/errors_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="90 days",
        level="ERROR"
    )

    return logger


# Initialize logger
log = setup_logger()
