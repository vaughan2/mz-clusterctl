"""
Signal queries for mz-clusterctl

Functions to query activity and hydration status from Materialize system tables.
"""

from datetime import datetime, timedelta
from typing import Any

import psycopg

from .log import get_logger
from .models import Signals

logger = get_logger(__name__)


def get_cluster_signals(
    conn: psycopg.Connection,
    cluster_ids: list[str],
    max_activity_lookback_seconds: int | None = None,
) -> dict[str, Signals]:
    """
    Get activity and hydration signals for multiple clusters

    Args:
        conn: Database connection
        cluster_ids: List of cluster IDs
        max_activity_lookback_seconds: Maximum seconds to look back for activity data

    Returns:
        Dictionary mapping cluster IDs to their Signals objects
    """
    if not cluster_ids:
        return {}

    # Get data for all clusters in batch
    last_activities = _get_last_activity(conn, cluster_ids, max_activity_lookback_seconds)
    hydration_statuses = _get_hydration_status(conn, cluster_ids)
    replica_crash_infos = _get_replica_crash_info(conn, cluster_ids)

    # Build signals objects for each cluster
    signals_by_cluster = {}
    for cluster_id in cluster_ids:
        signals = Signals(cluster_id=cluster_id)
        signals.last_activity_ts = last_activities.get(cluster_id)
        signals.hydration_status = hydration_statuses.get(cluster_id, {})
        signals.replica_crash_info = replica_crash_infos.get(cluster_id, {})
        signals_by_cluster[cluster_id] = signals

    return signals_by_cluster


def _get_last_activity(
    conn: psycopg.Connection,
    cluster_ids: list[str],
    max_activity_lookback_seconds: int | None = None,
) -> dict[str, datetime | None]:
    """
    Get timestamp of last activity for multiple clusters using
    mz_statement_execution_history_redacted

    Queries the statement execution history to find the most recent activity
    for each specified cluster.

    Args:
        conn: Database connection
        cluster_ids: List of cluster IDs
        max_activity_lookback_seconds: Maximum seconds to look back for activity data
    """
    if not cluster_ids:
        return {}

    with conn.cursor() as cur:
        # Create placeholder string for IN clause
        placeholders = ",".join(["%s"] * len(cluster_ids))

        # Build WHERE clause with optional time filter
        where_conditions = [
            f"cluster_id IN ({placeholders})",
            "finished_at IS NOT NULL",
        ]
        params = list(cluster_ids)

        if max_activity_lookback_seconds is not None:
            where_conditions.append("finished_at >= NOW() - %s")
            params.append(timedelta(seconds=max_activity_lookback_seconds))

        where_clause = " AND ".join(where_conditions)

        sql = f"""
            SELECT cluster_id, MAX(finished_at) as last_activity
            FROM mz_internal.mz_statement_execution_history_redacted
            WHERE {where_clause}
            GROUP BY cluster_id
        """
        params = tuple(params)
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
            results = cur.fetchall()

            # Build result dictionary
            last_activities = {}
            for result in results:
                cluster_id = result["cluster_id"]
                last_activity = result["last_activity"]
                last_activities[cluster_id] = last_activity
                logger.debug(
                    "Last activity found",
                    extra={
                        "cluster_id": cluster_id,
                        "last_activity": last_activity,
                    },
                )

            # Add None entries for clusters with no activity
            for cluster_id in cluster_ids:
                if cluster_id not in last_activities:
                    last_activities[cluster_id] = None
                    logger.debug(
                        "No last activity found", extra={"cluster_id": cluster_id}
                    )

            return last_activities
        except Exception as e:
            logger.error(
                "Error executing SQL",
                extra={"sql": sql, "params": params, "error": str(e)},
                exc_info=True,
            )
            raise


def _get_hydration_status(
    conn: psycopg.Connection, cluster_ids: list[str]
) -> dict[str, dict[str, bool]]:
    """
    Get hydration status per replica for multiple clusters using mz_compute_hydration_statuses

    This queries the hydration status of compute objects on each replica in the clusters.

    Returns:
        Dict mapping cluster IDs to dicts mapping replica names to their hydration status
        (True if hydrated, False otherwise)
    """
    if not cluster_ids:
        return {}

    with conn.cursor() as cur:
        # Create placeholder string for IN clause
        placeholders = ",".join(["%s"] * len(cluster_ids))
        sql = f"""
            WITH index_status AS (
                SELECT
                    c.id as cluster_id,
                    cr.name as replica_name,
                    COUNT(*) as total_objects,
                    COUNT(*) FILTER (WHERE h.hydrated) as hydrated_objects
                FROM mz_clusters c
                JOIN mz_cluster_replicas cr ON cr.cluster_id = c.id
                JOIN mz_indexes i ON i.cluster_id = c.id
                LEFT JOIN mz_internal.mz_hydration_statuses h
                    ON h.replica_id = cr.id AND h.object_id = i.id
                GROUP BY c.id, cr.name
            ),
            source_status AS (
                SELECT
                    c.id as cluster_id,
                    cr.name as replica_name,
                    COUNT(*) as total_objects,
                    COUNT(*) FILTER (WHERE h.hydrated) as hydrated_objects
                FROM mz_clusters c
                JOIN mz_cluster_replicas cr ON cr.cluster_id = c.id
                JOIN mz_sources s ON s.cluster_id = c.id
                LEFT JOIN mz_internal.mz_hydration_statuses h
                    ON h.replica_id = cr.id AND h.object_id = s.id
                WHERE
                    -- currently, only Kafka sources require hydration
                    s.type IN ('kafka')
                GROUP BY c.id, cr.name
            ),
            combined_status AS (
                SELECT * FROM index_status UNION ALL SELECT * FROM source_status
            )
            SELECT
                cluster_id,
                replica_name,
                SUM(total_objects) as total_objects,
                SUM(hydrated_objects) as hydrated_objects
            FROM
                combined_status
            WHERE
                cluster_id IN ({placeholders})
            GROUP BY cluster_id, replica_name
        """
        params = tuple(cluster_ids)
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
        hydration_status_by_cluster = {}

        for result in results:
            cluster_id = result["cluster_id"]
            replica_name = result["replica_name"]
            total_objects = result["total_objects"]
            hydrated_objects = result["hydrated_objects"]

            # Initialize cluster entry if not exists
            if cluster_id not in hydration_status_by_cluster:
                hydration_status_by_cluster[cluster_id] = {}

            # A replica is considered hydrated if all its objects are hydrated
            is_hydrated = total_objects > 0 and hydrated_objects == total_objects
            hydration_status_by_cluster[cluster_id][replica_name] = is_hydrated

        # Ensure all requested clusters have an entry (even if empty)
        for cluster_id in cluster_ids:
            if cluster_id not in hydration_status_by_cluster:
                hydration_status_by_cluster[cluster_id] = {}

        logger.debug(
            "Per-replica hydration status calculated",
            extra={
                "cluster_count": len(cluster_ids),
                "hydration_status_summary": {
                    cluster_id: len(status)
                    for cluster_id, status in hydration_status_by_cluster.items()
                },
            },
        )

        return hydration_status_by_cluster


def _get_replica_crash_info(
    conn: psycopg.Connection, cluster_ids: list[str], lookback_hours: int = 1
) -> dict[str, dict[str, dict[str, Any]]]:
    """
    Get crash information for replicas in multiple clusters using
    mz_cluster_replica_status_history

    Analyzes crash patterns over the specified lookback period to identify:
    - OOM conditions (reason contains 'oom')
    - Total crash count
    - Recent crash patterns

    Args:
        conn: Database connection
        cluster_ids: List of cluster IDs
        lookback_hours: Hours to look back for crash history (default: 24)

    Returns:
        Dict mapping cluster IDs to dicts mapping replica names to crash information
    """
    if not cluster_ids:
        return {}

    with conn.cursor() as cur:
        # Create placeholder string for IN clause
        placeholders = ",".join(["%s"] * len(cluster_ids))
        sql = f"""
            SELECT
                c.id as cluster_id,
                cr.name as replica_name,
                h.reason,
                h.occurred_at,
                h.status
            FROM mz_cluster_replicas cr
            JOIN mz_clusters c ON cr.cluster_id = c.id
            JOIN mz_internal.mz_cluster_replica_status_history h ON h.replica_id = cr.id
            WHERE c.id IN ({placeholders})
            AND h.status = 'offline'
            AND h.occurred_at >= NOW() - %s
            AND h.reason IS NOT NULL
            ORDER BY c.id, cr.name, h.occurred_at DESC
        """
        params = tuple(cluster_ids) + (timedelta(hours=lookback_hours),)
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
        crash_info_by_cluster = {}

        for result in results:
            cluster_id = result["cluster_id"]
            replica_name = result["replica_name"]
            reason = result["reason"]
            occurred_at = result["occurred_at"]

            # Initialize cluster entry if not exists
            if cluster_id not in crash_info_by_cluster:
                crash_info_by_cluster[cluster_id] = {}

            crash_info = crash_info_by_cluster[cluster_id]

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

        # Ensure all requested clusters have an entry (even if empty)
        for cluster_id in cluster_ids:
            if cluster_id not in crash_info_by_cluster:
                crash_info_by_cluster[cluster_id] = {}

        logger.debug(
            "Replica crash information collected",
            extra={
                "cluster_count": len(cluster_ids),
                "crash_summary": {
                    cluster_id: {
                        "replicas_with_crashes": len(crash_info),
                        "replica_summary": {
                            name: {
                                "total_crashes": info["total_crashes"],
                                "oom_count": info["oom_count"],
                            }
                            for name, info in crash_info.items()
                        },
                    }
                    for cluster_id, crash_info in crash_info_by_cluster.items()
                },
            },
        )

        return crash_info_by_cluster
