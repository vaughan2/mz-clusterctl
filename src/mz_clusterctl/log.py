"""
Structured logging configuration for mz-clusterctl

Provides consistent logging setup with structured output for both stdout and
audit tables.
"""

import logging
import sys

import structlog

# Define custom TRACE level between DEBUG and NOTSET
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


def trace_level_processor(logger, method_name, event_dict):
    """Custom processor to handle trace level logging"""
    # Skip trace messages if trace is not enabled
    import builtins

    if event_dict.get("_trace") and not getattr(builtins, "_trace_enabled", False):
        raise structlog.DropEvent

    # Remove the trace marker from the final log output
    event_dict.pop("_trace", None)
    return event_dict


def setup_logging(verbose: int = 0):
    """
    Configure structured logging for the application

    Args:
        verbose: Logging verbosity level (0=INFO, 1=DEBUG, 2=TRACE)
    """
    # Determine log level based on verbosity
    # For trace level, we'll use DEBUG level but set a global flag
    if verbose >= 2:
        log_level = logging.DEBUG
        # Store trace mode globally for our custom logger
        import builtins

        builtins._trace_enabled = True
    elif verbose >= 1:
        log_level = logging.DEBUG
        import builtins

        builtins._trace_enabled = False
    else:
        log_level = logging.INFO
        import builtins

        builtins._trace_enabled = False

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Configure structlog
    structlog.configure(
        processors=[
            trace_level_processor,
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


class TracingBoundLogger:
    """Wrapper around structlog.BoundLogger to add trace method"""

    def __init__(self, logger: structlog.BoundLogger):
        self._logger = logger

    def trace(self, msg: str, **kwargs):
        """Log at TRACE level (only when trace is enabled)"""
        import builtins

        if getattr(builtins, "_trace_enabled", False):
            # Add trace marker to the log entry
            kwargs["_trace"] = True
            self._logger.debug(msg, **kwargs)

    def __getattr__(self, name):
        """Delegate all other method calls to the wrapped logger"""
        return getattr(self._logger, name)


def get_logger(name: str) -> TracingBoundLogger:
    """
    Get a structured logger instance with trace method

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured structlog logger with trace method
    """
    base_logger = structlog.get_logger(name)
    return TracingBoundLogger(base_logger)
