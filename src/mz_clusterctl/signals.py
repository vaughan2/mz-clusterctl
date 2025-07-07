"""
Signal queries for mz-clusterctl

Functions to query activity and hydration status from Materialize system tables.
"""

from datetime import datetime
from typing import Any

import psycopg

from .log import get_logger
from .models import Signals

logger = get_logger(__name__)


def get_cluster_signals(
    conn: psycopg.Connection, cluster_id: str, cluster_name: str
) -> Signals:
    """
    Get activity and hydration signals for a cluster

    Args:
        conn: Database connection
        cluster_id: ID of the cluster
        cluster_name: Name of the cluster

    Returns:
        Signals object with activity and hydration data
    """
    signals = Signals(cluster_id=cluster_id)

    # Get last activity timestamp
    signals.last_activity_ts = _get_last_activity(conn, cluster_id)

    # Get hydration status per replica
    signals.hydration_status = _get_hydration_status(conn, cluster_name)

    # Get replica crash information
    signals.replica_crash_info = _get_replica_crash_info(conn, cluster_name)

    return signals


def _get_last_activity(conn: psycopg.Connection, cluster_id: str) -> datetime | None:
    """
    Get timestamp of last activity on a cluster using
    mz_statement_execution_history_redacted

    Queries the statement execution history to find the most recent activity
    for the specified cluster.
    """
    with conn.cursor() as cur:
        sql = """
            SELECT MAX(finished_at) as last_activity
            FROM mz_internal.mz_statement_execution_history_redacted
            WHERE cluster_id = %s
            AND finished_at IS NOT NULL
        """
        params = (cluster_id,)
        logger.debug(
            "Executing SQL",
            extra={
                "sql": sql,
                "params": params,
                "param_types": [type(p).__name__ for p in params],
            },
        )
        try:
            cur.execute(sql, params)
            result = cur.fetchone()
            if result and result["last_activity"]:
                logger.debug(
                    "Last activity found",
                    extra={
                        "cluster_id": cluster_id,
                        "last_activity": result["last_activity"],
                    },
                )
                return result["last_activity"]

            logger.debug("No last activity found", extra={"cluster_id": cluster_id})
            return None
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise


def _get_hydration_status(
    conn: psycopg.Connection, cluster_name: str
) -> dict[str, bool]:
    """
    Get hydration status per replica for a cluster using mz_compute_hydration_statuses

    This queries the hydration status of compute objects on each replica in the cluster.

    Returns:
        Dict mapping replica names to their hydration status (True if hydrated,
        False otherwise)
    """
    with conn.cursor() as cur:
        sql = """
            SELECT
                cr.name as replica_name,
                COUNT(*) as total_objects,
                COUNT(*) FILTER (WHERE h.hydrated) as hydrated_objects
            FROM mz_clusters c
            JOIN mz_cluster_replicas cr ON cr.cluster_id = c.id
            JOIN mz_indexes i ON i.cluster_id = c.id
            LEFT JOIN mz_internal.mz_hydration_statuses h
                ON h.replica_id = cr.id AND h.object_id = i.id
            WHERE c.name = %s
            GROUP BY cr.name
        """
        params = (cluster_name,)
        logger.debug(
            "Executing SQL",
            extra={
                "sql": sql,
                "params": params,
                "param_types": [type(p).__name__ for p in params],
            },
        )
        try:
            cur.execute(sql, params)
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise

        results = cur.fetchall()
        hydration_status = {}

        for result in results:
            replica_name = result["replica_name"]
            total_objects = result["total_objects"]
            hydrated_objects = result["hydrated_objects"]

            # A replica is considered hydrated if all its objects are hydrated
            is_hydrated = total_objects > 0 and hydrated_objects == total_objects
            hydration_status[replica_name] = is_hydrated

        logger.debug(
            "Per-replica hydration status calculated",
            extra={
                "cluster_name": cluster_name,
                "hydration_status": hydration_status,
            },
        )

        return hydration_status


def _get_replica_crash_info(
    conn: psycopg.Connection, cluster_name: str, lookback_hours: int = 1
) -> dict[str, dict[str, Any]]:
    """
    Get crash information for replicas in a cluster using
    mz_cluster_replica_status_history

    Analyzes crash patterns over the specified lookback period to identify:
    - OOM conditions (reason contains 'oom')
    - Total crash count
    - Recent crash patterns

    Args:
        conn: Database connection
        cluster_name: Name of the cluster
        lookback_hours: Hours to look back for crash history (default: 24)

    Returns:
        Dict mapping replica names to crash information
    """
    with conn.cursor() as cur:
        sql = """
            SELECT
                cr.name as replica_name,
                h.reason,
                h.occurred_at,
                h.status
            FROM mz_cluster_replicas cr
            JOIN mz_clusters c ON cr.cluster_id = c.id
            JOIN mz_internal.mz_cluster_replica_status_history h ON h.replica_id = cr.id
            WHERE c.name = %s
            AND h.status = 'offline'
            AND h.occurred_at >= NOW() - INTERVAL '%s hours'
            AND h.reason IS NOT NULL
            ORDER BY cr.name, h.occurred_at DESC
        """
        params = (cluster_name, lookback_hours)
        logger.debug(
            "Executing SQL",
            extra={
                "sql": sql,
                "params": params,
                "param_types": [type(p).__name__ for p in params],
            },
        )

        try:
            cur.execute(sql, params)
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise

        results = cur.fetchall()
        crash_info = {}

        for result in results:
            replica_name = result["replica_name"]
            reason = result["reason"]
            occurred_at = result["occurred_at"]

            if replica_name not in crash_info:
                crash_info[replica_name] = {
                    "total_crashes": 0,
                    "oom_count": 0,
                    "recent_crashes": [],
                    "latest_crash_time": None,
                    "crash_reasons": {},
                }

            info = crash_info[replica_name]
            info["total_crashes"] += 1

            # Track OOM-specific crashes
            if reason and "oom" in reason.lower():
                info["oom_count"] += 1

            # Track crash reasons
            if reason:
                if reason not in info["crash_reasons"]:
                    info["crash_reasons"][reason] = 0
                info["crash_reasons"][reason] += 1

            # Track recent crashes (last 10)
            if len(info["recent_crashes"]) < 10:
                info["recent_crashes"].append(
                    {
                        "reason": reason,
                        "occurred_at": occurred_at,
                    }
                )

            # Update latest crash time
            if (
                info["latest_crash_time"] is None
                or occurred_at > info["latest_crash_time"]
            ):
                info["latest_crash_time"] = occurred_at

        logger.info(
            "Replica crash information collected",
            extra={
                "cluster_name": cluster_name,
                "replicas_with_crashes": len(crash_info),
                "crash_summary": {
                    name: {
                        "total_crashes": info["total_crashes"],
                        "oom_count": info["oom_count"],
                    }
                    for name, info in crash_info.items()
                },
            },
        )

        return crash_info


def get_cluster_metrics(conn: psycopg.Connection, cluster_name: str) -> dict:
    """
    Get additional metrics that might be useful for scaling decisions

    Returns a dictionary with various cluster metrics.
    """
    metrics = {}

    with conn.cursor() as cur:
        # Get cluster size information
        sql = """
            SELECT 
                cr.name as replica_name,
                cr.size,
                cr.availability_zone,
                cr.disk
            FROM mz_cluster_replicas cr
            JOIN mz_clusters c ON cr.cluster_id = c.id
            WHERE c.name = %s
        """
        params = (cluster_name,)
        logger.debug(
            "Executing SQL",
            extra={
                "sql": sql,
                "params": params,
                "param_types": [type(p).__name__ for p in params],
            },
        )
        try:
            cur.execute(sql, params)
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise

        replicas = cur.fetchall()
        metrics["replicas"] = replicas
        metrics["replica_count"] = len(replicas)

        # Get workload information (placeholder)
        # In a real implementation, you might query:
        # - Queue depths from mz_internal.mz_compute_operator_durations
        # - Memory usage from system tables
        # - CPU utilization metrics

        sql = """
            SELECT COUNT(*) as active_queries
            FROM mz_internal.mz_active_peeks
        """
        logger.debug("Executing SQL", extra={"sql": sql, "params": None})
        try:
            cur.execute(sql)
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": None, "error": str(e)},
                exc_info=True,
            )
            raise

        result = cur.fetchone()
        metrics["active_queries"] = result["active_queries"] if result else 0

    return metrics
