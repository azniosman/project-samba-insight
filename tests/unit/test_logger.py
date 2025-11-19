"""
Unit tests for logging module.
"""

import logging
from io import StringIO

import pytest
import structlog

from src.utils.logger import get_logger, setup_logging


class TestLogger:
    """Test suite for logger functionality."""

    def test_get_logger_returns_bound_logger(self):
        """Test that get_logger returns a structlog BoundLogger."""
        logger = get_logger("test_module")

        assert isinstance(logger, structlog.BoundLoggerBase)

    def test_get_logger_with_module_name(self):
        """Test logger creation with module name."""
        module_name = "test.module.name"
        logger = get_logger(module_name)

        assert logger is not None

    def test_setup_logging_configures_structlog(self):
        """Test that setup_logging configures structlog."""
        setup_logging("test_app")

        # Verify structlog is configured
        logger = get_logger("test")
        assert logger is not None

    def test_logger_info_level(self):
        """Test logging at INFO level."""
        logger = get_logger("test")

        # Should not raise exception
        logger.info("test_message", key="value")

    def test_logger_error_level(self):
        """Test logging at ERROR level."""
        logger = get_logger("test")

        # Should not raise exception
        logger.error("error_message", error_code=500)

    def test_logger_warning_level(self):
        """Test logging at WARNING level."""
        logger = get_logger("test")

        # Should not raise exception
        logger.warning("warning_message", status="pending")

    def test_logger_debug_level(self):
        """Test logging at DEBUG level."""
        logger = get_logger("test")

        # Should not raise exception
        logger.debug("debug_message", details={"key": "value"})

    def test_logger_with_context(self):
        """Test logger with bound context."""
        logger = get_logger("test")

        # Bind context
        logger_with_context = logger.bind(request_id="12345")

        # Should not raise exception
        logger_with_context.info("test_with_context")

    def test_multiple_loggers_are_independent(self):
        """Test that multiple loggers can be created independently."""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")

        assert logger1 is not None
        assert logger2 is not None


class TestLoggingIntegration:
    """Test logging integration scenarios."""

    def test_logger_handles_exceptions_in_messages(self):
        """Test that logger handles exceptions gracefully."""
        logger = get_logger("test")

        try:
            raise ValueError("Test exception")
        except ValueError as e:
            # Should not raise exception
            logger.error("caught_exception", exception=str(e))

    def test_logger_handles_none_values(self):
        """Test that logger handles None values in context."""
        logger = get_logger("test")

        # Should not raise exception
        logger.info("test_message", nullable_field=None)

    def test_logger_handles_complex_objects(self):
        """Test that logger handles complex objects."""
        logger = get_logger("test")

        complex_obj = {
            "nested": {"key": "value"},
            "list": [1, 2, 3],
            "number": 42,
        }

        # Should not raise exception
        logger.info("test_message", data=complex_obj)
