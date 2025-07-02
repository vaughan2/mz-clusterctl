"""
Structured logging configuration for mz-clusterctl

Provides consistent logging setup with structured output for both stdout and
audit tables.
"""

import logging
import sys

import structlog


def setup_logging(verbose: int = 0):
    """
    Configure structured logging for the application

    Args:
        verbose: Logging verbosity level (0=WARNING, 1=INFO, 2=DEBUG)
    """
    # Determine log level based on verbosity
    if verbose >= 2:
        log_level = logging.DEBUG
    elif verbose >= 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a structured logger instance

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)
