"""
Structured logging configuration for mz-schedctl

Provides consistent logging setup with structured output for both stdout and audit tables.
"""

import logging
import sys
from typing import Any, Dict, Optional

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


class AuditLogger:
    """
    Helper class for audit logging

    Provides structured logging specifically for audit events that should
    be captured both in application logs and potentially in audit tables.
    """

    def __init__(self, cluster_id: Optional[str] = None):
        self.logger = get_logger("audit")
        self.cluster_id = cluster_id

    def log_decision(
        self,
        strategy_type: str,
        config: Dict[str, Any],
        signals: Dict[str, Any],
        actions: list,
        **kwargs,
    ):
        """Log a strategy decision"""
        self.logger.info(
            "Strategy decision made",
            cluster_id=self.cluster_id,
            strategy_type=strategy_type,
            config=config,
            signals=signals,
            actions_count=len(actions),
            actions=[str(action) for action in actions],
            **kwargs,
        )

    def log_action_start(self, action_sql: str, reason: str, **kwargs):
        """Log the start of an action execution"""
        self.logger.info(
            "Action execution started",
            cluster_id=self.cluster_id,
            action_sql=action_sql,
            reason=reason,
            **kwargs,
        )

    def log_action_success(self, action_sql: str, result: Dict[str, Any], **kwargs):
        """Log successful action execution"""
        self.logger.info(
            "Action executed successfully",
            cluster_id=self.cluster_id,
            action_sql=action_sql,
            result=result,
            **kwargs,
        )

    def log_action_failure(self, action_sql: str, error: str, **kwargs):
        """Log failed action execution"""
        self.logger.error(
            "Action execution failed",
            cluster_id=self.cluster_id,
            action_sql=action_sql,
            error=error,
            **kwargs,
        )

    def log_state_change(
        self, old_state: Dict[str, Any], new_state: Dict[str, Any], **kwargs
    ):
        """Log strategy state changes"""
        self.logger.info(
            "Strategy state updated",
            cluster_id=self.cluster_id,
            old_state=old_state,
            new_state=new_state,
            **kwargs,
        )


# Convenience functions for common logging patterns
def log_startup(version: str, config: Dict[str, Any]):
    """Log application startup"""
    logger = get_logger("startup")
    logger.info("mz-schedctl starting", version=version, config=config)


def log_shutdown(exit_code: int, reason: str = "normal"):
    """Log application shutdown"""
    logger = get_logger("shutdown")
    logger.info("mz-schedctl shutting down", exit_code=exit_code, reason=reason)


def log_error(error: Exception, context: Dict[str, Any] = None):
    """Log errors with context"""
    logger = get_logger("error")
    logger.error(
        "Error occurred",
        error=str(error),
        error_type=type(error).__name__,
        context=context or {},
        exc_info=True,
    )


def log_performance(operation: str, duration_ms: float, **kwargs):
    """Log performance metrics"""
    logger = get_logger("performance")
    logger.info(
        "Performance metric", operation=operation, duration_ms=duration_ms, **kwargs
    )
