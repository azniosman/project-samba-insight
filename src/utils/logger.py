"""
Logging Configuration Module

Sets up structured logging with rotation and proper formatting.
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import structlog

from .config import get_config


def setup_logging(
    name: str = "samba_insight",
    log_level: Optional[str] = None,
    log_file: Optional[Path] = None,
) -> structlog.BoundLogger:
    """
    Set up structured logging with both console and file output.

    Args:
        name: Logger name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file. If None, uses default from config

    Returns:
        Configured structlog logger
    """
    config = get_config()

    # Determine log level
    if log_level is None:
        log_level = config.log_level

    level = getattr(logging, log_level.upper(), logging.INFO)

    # Determine log file path
    if log_file is None:
        log_file = config.logs_dir / f"{name}.log"

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    # Remove existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    root_logger.handlers = []

    # Console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Configure structlog
    renderer = structlog.processors.JSONRenderer() if log_file else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Create and return logger
    logger = structlog.get_logger(name)
    logger.info("logging_initialized", log_level=log_level, log_file=str(log_file))

    return logger


def get_logger(name: str = "samba_insight") -> structlog.BoundLogger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (usually module name)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


class LoggerMixin:
    """Mixin class that adds a logger property to any class."""

    @property
    def logger(self) -> structlog.BoundLogger:
        """Get logger for this class."""
        return get_logger(self.__class__.__name__)
