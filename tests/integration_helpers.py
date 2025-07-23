"""
Shared helper functions for integration tests.

This module provides common functionality used across all integration tests,
including CLI command execution and database query helpers.
"""

import subprocess
from typing import Any

import psycopg


def run_clusterctl_command(
    command: str, materialize_url: str, extra_args: list[str] = None, timeout: int = 30
) -> subprocess.CompletedProcess:
    """
    Run a mz-clusterctl command using subprocess.

    Args:
        command: The command to run ('apply', 'dry-run', 'wipe-state')
        materialize_url: Database URL to pass to the CLI
        extra_args: Additional CLI arguments
        timeout: Timeout in seconds

    Returns:
        CompletedProcess result
    """
    cmd = ["uv", "run", "mz-clusterctl", command, "--postgres-url", materialize_url]
    if extra_args:
        cmd.extend(extra_args)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def get_cluster_replicas(conn, cluster_id: str) -> list[tuple[str, str]]:
    """Get current replicas for a cluster as (name, size) tuples."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, size FROM mz_cluster_replicas
            WHERE cluster_id = %s ORDER BY name
            """,
            (cluster_id,),
        )
        return [(row["name"], row["size"]) for row in cur.fetchall()]


def insert_strategy_config(
    conn: psycopg.Connection,
    cluster_id: str,
    strategy_type: str,
    config: dict[str, Any],
):
    """Insert a strategy configuration for testing."""
    import json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mz_cluster_strategies
            (cluster_id, strategy_type, config)
            VALUES (%s, %s, %s)
            """,
            (cluster_id, strategy_type, json.dumps(config)),
        )


def get_strategy_actions(conn, cluster_id: str) -> list[dict[str, Any]]:
    """Get recorded strategy actions for a cluster."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT action_id, action_sql, decision_ctx, executed,
                   error_message, created_at
            FROM mz_cluster_strategy_actions
            WHERE cluster_id = %s
            ORDER BY created_at
            """,
            (cluster_id,),
        )
        return [
            {
                "action_id": row["action_id"],
                "action_sql": row["action_sql"],
                "decision_ctx": row["decision_ctx"],
                "executed": row["executed"],
                "error_message": row["error_message"],
                "created_at": row["created_at"],
            }
            for row in cur.fetchall()
        ]


def execute_sql(conn: psycopg.Connection, sql: str, params=None):
    """Execute arbitrary SQL command."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.rowcount > 0:
            return [row for row in cur.fetchall()]
        else:
            return []


def get_cluster_name_from_id(conn, cluster_id: str) -> str:
    """Get cluster name from cluster ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM mz_clusters WHERE id = %s", (cluster_id,))
        result = cur.fetchone()
        if not result:
            raise ValueError(f"Cluster not found: {cluster_id}")
        return result[0]
