"""
Signal queries for mz-schedctl

Functions to query activity and hydration status from Materialize system tables.
"""

from datetime import datetime
from typing import Optional

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
        cluster_id: UUID of the cluster
        cluster_name: Name of the cluster

    Returns:
        Signals object with activity and hydration data
    """
    logger.debug(
        "Starting get_cluster_signals",
        extra={"cluster_id": cluster_id, "cluster_name": cluster_name},
    )
    signals = Signals(cluster_id=cluster_id)

    # Get current replica count
    signals.current_replicas = _get_replica_count(conn, cluster_name)

    # Get last activity timestamp
    signals.last_activity_ts = _get_last_activity(conn, cluster_id)

    # Get hydration status
    signals.hydration_status = _get_hydration_status(conn, cluster_name)

    logger.debug(
        "Retrieved signals for cluster",
        extra={
            "cluster_id": str(cluster_id),
            "cluster_name": cluster_name,
            "current_replicas": signals.current_replicas,
            "last_activity_ts": signals.last_activity_ts,
            "hydration_status": signals.hydration_status,
        },
    )

    return signals


def _get_replica_count(conn: psycopg.Connection, cluster_name: str) -> int:
    """Get current number of replicas for a cluster"""
    logger.debug("Starting _get_replica_count", extra={"cluster_name": cluster_name})
    with conn.cursor() as cur:
        sql = "SELECT COUNT(*) as count FROM mz_cluster_replicas WHERE cluster_id = (SELECT id FROM mz_clusters WHERE name = %s)"
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
            result = cur.fetchone()
            count = result["count"] if result else 0
            logger.debug(
                "Replica count retrieved",
                extra={"cluster_name": cluster_name, "count": count},
            )
            return count
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise


def _get_last_activity(conn: psycopg.Connection, cluster_id: str) -> Optional[datetime]:
    """
    Get timestamp of last activity on a cluster

    This is a placeholder implementation. In a real deployment, you would query
    appropriate system tables or metrics to determine cluster activity.

    Possible approaches:
    - Query mz_internal.mz_active_peeks for active queries
    - Look at mz_internal.mz_dataflow_operators for dataflow activity
    - Check mz_internal.mz_hydration_status for materialized view updates
    """
    logger.debug("Starting _get_last_activity", extra={"cluster_id": cluster_id})
    with conn.cursor() as cur:
        # Placeholder query - replace with actual activity detection logic
        # This example looks for recent queries that might have used the cluster
        sql = """
            SELECT MAX(finished_at) as last_activity
            FROM mz_internal.mz_statement_execution_history 
            WHERE status = 'success'
            AND finished_at > now() - INTERVAL '1 hour'
            LIMIT 1
        """
        logger.debug("Executing SQL", extra={"sql": sql, "params": None})
        try:
            cur.execute(sql)
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
                extra={"sql": sql, "params": None, "error": str(e)},
                exc_info=True,
            )
            raise


def _get_hydration_status(conn: psycopg.Connection, cluster_name: str) -> Optional[str]:
    """
    Get hydration status for a cluster

    This queries the hydration status of materialized views and indexes
    that might be using this cluster.
    """
    logger.debug("Starting _get_hydration_status", extra={"cluster_name": cluster_name})
    with conn.cursor() as cur:
        # Check if there are any non-hydrated objects on this cluster
        sql = """
            SELECT 
                COUNT(*) as total_objects,
                COUNT(*) FILTER (WHERE hydrated) as hydrated_objects
            FROM mz_internal.mz_hydration_status h
            JOIN mz_indexes i ON h.object_id = i.id
            JOIN mz_clusters c ON i.cluster_id = c.id
            WHERE c.name = %s
            
            UNION ALL
            
            SELECT 
                COUNT(*) as total_objects,
                COUNT(*) FILTER (WHERE hydrated) as hydrated_objects  
            FROM mz_internal.mz_hydration_status h
            JOIN mz_materialized_views mv ON h.object_id = mv.id
            JOIN mz_clusters c ON mv.cluster_id = c.id
            WHERE c.name = %s
        """
        params = (cluster_name, cluster_name)
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

        total_objects = sum(r["total_objects"] for r in results)
        hydrated_objects = sum(r["hydrated_objects"] for r in results)

        logger.debug(
            "Hydration status calculated",
            extra={
                "cluster_name": cluster_name,
                "total_objects": total_objects,
                "hydrated_objects": hydrated_objects,
            },
        )

        if total_objects == 0:
            return "no_objects"
        elif hydrated_objects == total_objects:
            return "hydrated"
        elif hydrated_objects == 0:
            return "not_hydrated"
        else:
            return "partially_hydrated"


def get_cluster_metrics(conn: psycopg.Connection, cluster_name: str) -> dict:
    """
    Get additional metrics that might be useful for scaling decisions

    Returns a dictionary with various cluster metrics.
    """
    logger.debug("Starting get_cluster_metrics", extra={"cluster_name": cluster_name})
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
