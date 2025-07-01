"""
SQL execution engine for mz-schedctl

Handles execution of actions in apply mode with proper error handling and audit logging.
"""

from typing import Any, Dict, List
from uuid import UUID

from .db import Database
from .log import get_logger
from .models import Action

logger = get_logger(__name__)


class Executor:
    """
    Executes SQL actions and maintains audit trail

    Responsible for:
    1. Executing SQL commands in sequence
    2. Logging all actions to audit table
    3. Handling errors gracefully
    4. Providing execution feedback
    """

    def __init__(self, db: Database):
        self.db = db

    def execute_actions(
        self, cluster_id: UUID, actions: List[Action]
    ) -> Dict[str, Any]:
        """
        Execute a list of actions for a cluster

        Args:
            cluster_id: UUID of the cluster
            actions: List of actions to execute

        Returns:
            Dictionary with execution summary
        """
        if not actions:
            return {"total": 0, "executed": 0, "failed": 0, "errors": []}

        summary = {"total": len(actions), "executed": 0, "failed": 0, "errors": []}

        logger.info(
            "Starting action execution",
            extra={"cluster_id": str(cluster_id), "total_actions": len(actions)},
        )

        for i, action in enumerate(actions, 1):
            action_id = None
            executed = False
            error_message = None

            try:
                logger.info(
                    f"Executing action {i}/{len(actions)}",
                    extra={
                        "cluster_id": str(cluster_id),
                        "action_sql": action.sql,
                        "action_reason": action.reason,
                    },
                )

                # Create decision context for audit log
                decision_ctx = {
                    "action_index": i,
                    "total_actions": len(actions),
                    "reason": action.reason,
                    "expected_state_delta": action.expected_state_delta,
                }

                # Execute the SQL
                logger.debug(
                    "About to execute action SQL",
                    extra={"cluster_id": str(cluster_id), "action_sql": action.sql},
                )
                result = self.db.execute_sql(action.sql)
                executed = True
                summary["executed"] += 1

                # Update decision context with execution results
                decision_ctx["execution_result"] = result

                print(f"✓ {action.sql}")
                print(f"  Reason: {action.reason}")
                if result.get("rowcount", 0) > 0:
                    print(f"  Affected rows: {result['rowcount']}")
                print()

                logger.info(
                    "Action executed successfully",
                    extra={
                        "cluster_id": str(cluster_id),
                        "action_sql": action.sql,
                        "rowcount": result.get("rowcount", 0),
                    },
                )

            except Exception as e:
                executed = False
                error_message = str(e)
                summary["failed"] += 1
                summary["errors"].append(
                    {"action_index": i, "sql": action.sql, "error": error_message}
                )

                decision_ctx = {
                    "action_index": i,
                    "total_actions": len(actions),
                    "reason": action.reason,
                    "expected_state_delta": action.expected_state_delta,
                    "error": error_message,
                }

                print(f"✗ {action.sql}")
                print(f"  Reason: {action.reason}")
                print(f"  Error: {error_message}")
                print()

                logger.error(
                    "Action execution failed",
                    extra={
                        "cluster_id": str(cluster_id),
                        "action_sql": action.sql,
                        "error": error_message,
                    },
                    exc_info=True,
                )

            finally:
                # Always log to audit table
                try:
                    logger.debug(
                        "Logging action to audit table",
                        extra={"cluster_id": str(cluster_id), "executed": executed},
                    )
                    action_id = self.db.log_action(
                        cluster_id=cluster_id,
                        action_sql=action.sql,
                        decision_ctx=decision_ctx,
                        executed=executed,
                        error_message=error_message,
                    )
                    logger.debug(
                        "Action logged to audit table successfully",
                        extra={"cluster_id": str(cluster_id), "action_id": action_id},
                    )

                    logger.debug(
                        "Action logged to audit table",
                        extra={
                            "cluster_id": str(cluster_id),
                            "action_id": str(action_id),
                            "executed": executed,
                        },
                    )

                except Exception as audit_error:
                    # Don't fail the entire execution if audit logging fails
                    logger.error(
                        "Failed to log action to audit table",
                        extra={
                            "cluster_id": str(cluster_id),
                            "action_sql": action.sql,
                            "audit_error": str(audit_error),
                        },
                        exc_info=True,
                    )

                # Stop execution on first error (fail-fast approach)
                if not executed:
                    logger.warning(
                        "Stopping execution due to error",
                        extra={
                            "cluster_id": str(cluster_id),
                            "failed_action_index": i,
                            "remaining_actions": len(actions) - i,
                        },
                    )
                    break

        logger.info(
            "Action execution completed",
            extra={"cluster_id": str(cluster_id), "summary": summary},
        )

        # Print execution summary
        if summary["failed"] > 0:
            print(
                f"Execution completed with errors: {summary['executed']}/{summary['total']} actions succeeded"
            )
            print("Errors:")
            for error in summary["errors"]:
                print(f"  Action {error['action_index']}: {error['error']}")
        else:
            print(f"All {summary['executed']} actions executed successfully")

        return summary

    def validate_action(self, action: Action) -> bool:
        """
        Validate an action before execution

        Args:
            action: Action to validate

        Returns:
            True if action is valid, False otherwise
        """
        # Basic validation
        if not action.sql or not action.sql.strip():
            logger.error("Action has empty SQL", extra={"action": action})
            return False

        # Check for potentially dangerous operations
        sql_upper = action.sql.upper().strip()

        # Allow specific DDL operations
        allowed_operations = [
            "CREATE CLUSTER REPLICA",
            "DROP CLUSTER REPLICA",
            "ALTER CLUSTER",
        ]

        if not any(sql_upper.startswith(op) for op in allowed_operations):
            logger.warning(
                "Action contains potentially unsafe SQL",
                extra={"action_sql": action.sql},
            )
            # For now, we'll allow it but log a warning
            # In production, you might want to be more restrictive

        return True
