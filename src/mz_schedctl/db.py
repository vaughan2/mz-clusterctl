"""
Database connection and helper functions for mz-schedctl

Provides PostgreSQL connection pool and database schema management.
"""

import json
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .log import get_logger
from .models import ClusterInfo, ReplicaInfo, StrategyConfig, StrategyState

logger = get_logger(__name__)


class Database:
    """Database connection manager and helper methods"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = ConnectionPool(conninfo=database_url, min_size=1, max_size=10)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the connection pool"""
        self.pool.close()

    @contextmanager
    def get_connection(self) -> Iterator[psycopg.Connection]:
        """Get a connection from the pool"""
        with self.pool.connection() as conn:
            conn.row_factory = dict_row
            yield conn

    def ensure_tables(self):
        """Create required tables if they don't exist"""
        # Execute each CREATE TABLE in its own transaction
        tables = [
            (
                "mz_cluster_strategies",
                """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategies (
                    cluster_id    TEXT            NOT NULL,
                    strategy_type TEXT            NOT NULL,
                    config        JSONB           NOT NULL,
                    updated_at    TIMESTAMPTZ     DEFAULT now()
                )
            """,
            ),
            (
                "mz_cluster_strategy_state",
                """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategy_state (
                    cluster_id    TEXT            NOT NULL,
                    state_version INT             NOT NULL,
                    payload       JSONB           NOT NULL,
                    updated_at    TIMESTAMPTZ     DEFAULT now()
                )
            """,
            ),
            (
                "mz_cluster_strategy_actions",
                """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategy_actions (
                    action_id     TEXT            NOT NULL,
                    cluster_id    TEXT,
                    action_sql    TEXT,
                    decision_ctx  JSONB,
                    executed      BOOL,
                    error_message TEXT,
                    created_at    TIMESTAMPTZ     DEFAULT now()
                )
            """,
            ),
        ]

        for table_name, sql in tables:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    conn.commit()

        logger.info("Database tables ensured")

    def get_clusters(self, name_filter: Optional[str] = None) -> List[ClusterInfo]:
        """Get cluster information from mz_catalog.mz_clusters"""
        logger.debug("Starting get_clusters", extra={"name_filter": name_filter})
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                sql = "SELECT id, name FROM mz_catalog.mz_clusters"
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
                clusters = []

                for row in cur.fetchall():
                    logger.debug(
                        "Processing cluster row",
                        extra={
                            "row": row,
                            "row_types": {k: type(v).__name__ for k, v in row.items()},
                        },
                    )
                    cluster = ClusterInfo.from_db_row(row)
                    logger.debug(
                        "Created ClusterInfo",
                        extra={
                            "cluster_id": cluster.id,
                            "cluster_id_type": type(cluster.id).__name__,
                            "cluster_name": cluster.name,
                        },
                    )

                    # Apply name filter if provided
                    if name_filter and not self._matches_filter(
                        cluster.name, name_filter
                    ):
                        continue

                    # Get replicas for this cluster using catalog query instead of SHOW
                    sql = "SELECT name, size FROM mz_catalog.mz_cluster_replicas WHERE cluster_id = %s"
                    params = (cluster.id,)
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
                        replica_infos = [
                            ReplicaInfo(
                                name=replica_row["name"], size=replica_row["size"]
                            )
                            for replica_row in cur.fetchall()
                        ]
                        # ClusterInfo is frozen, so we need to create a new instance
                        cluster = ClusterInfo(
                            id=cluster.id,
                            name=cluster.name,
                            replicas=tuple(replica_infos),
                            managed=cluster.managed,
                        )
                    except Exception as e:
                        logger.error(
                            "Error executing SQL",
                            extra={"sql": sql, "params": params, "error": str(e)},
                            exc_info=True,
                        )
                        raise

                    clusters.append(cluster)

                return clusters

    def get_strategy_configs(
        self, cluster_id: Optional[UUID] = None
    ) -> List[StrategyConfig]:
        """Get strategy configurations from mz_cluster_strategies"""
        logger.debug(
            "Starting get_strategy_configs",
            extra={"cluster_id": str(cluster_id) if cluster_id else None},
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if cluster_id:
                    sql = "SELECT * FROM mz_cluster_strategies WHERE cluster_id = %s"
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
                    except Exception as e:
                        logger.error(
                            "Error executing SQL",
                            extra={"sql": sql, "params": params, "error": str(e)},
                            exc_info=True,
                        )
                        raise
                else:
                    sql = "SELECT * FROM mz_cluster_strategies"
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

                return [StrategyConfig.from_db_row(row) for row in cur.fetchall()]

    def get_strategy_state(self, cluster_id: str) -> Optional[StrategyState]:
        """Get strategy state for a cluster"""
        logger.debug(
            "Starting get_strategy_state",
            extra={
                "cluster_id": cluster_id,
                "cluster_id_type": type(cluster_id).__name__,
            },
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                sql = "SELECT * FROM mz_cluster_strategy_state WHERE cluster_id = %s"
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
                    row = cur.fetchone()
                except Exception as e:
                    logger.error(
                        "Error executing SQL",
                        extra={"sql": sql, "params": params, "error": str(e)},
                        exc_info=True,
                    )
                    raise

                if not row:
                    return None

                return StrategyState(
                    cluster_id=row["cluster_id"],
                    strategy_type="",  # Not stored in state table
                    state_version=row["state_version"],
                    payload=row["payload"],
                    updated_at=row["updated_at"],
                )

    def upsert_strategy_state(self, state: StrategyState):
        """Insert or update strategy state"""
        logger.debug(
            "Starting upsert_strategy_state",
            extra={
                "cluster_id": state.cluster_id,
                "state_version": state.state_version,
            },
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO mz_cluster_strategy_state 
                    (cluster_id, state_version, payload, updated_at)
                    VALUES (%s, %s, %s, now())
                """
                params = (state.cluster_id, state.state_version, json.dumps(state.payload))
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
                    conn.commit()
                except Exception as e:
                    logger.error(
                        "Error executing SQL",
                        extra={"sql": sql, "params": params, "error": str(e)},
                        exc_info=True,
                    )
                    raise

    def log_action(
        self,
        cluster_id: str,
        action_sql: str,
        decision_ctx: Dict[str, Any],
        executed: bool,
        error_message: Optional[str] = None,
    ) -> str:
        """Log an action to the audit table"""
        action_id = str(uuid4())
        logger.debug(
            "Starting log_action",
            extra={
                "action_id": action_id,
                "cluster_id": cluster_id,
                "executed": executed,
            },
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO mz_cluster_strategy_actions
                    (action_id, cluster_id, action_sql, decision_ctx, executed, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                params = (
                    action_id,
                    cluster_id,
                    action_sql,
                    json.dumps(decision_ctx),
                    executed,
                    error_message,
                )
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
                    conn.commit()
                    return action_id
                except Exception as e:
                    logger.error(
                        "Error executing SQL",
                        extra={"sql": sql, "params": params, "error": str(e)},
                        exc_info=True,
                    )
                    raise

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute arbitrary SQL and return result info"""
        logger.debug("Starting execute_sql", extra={"sql": sql})
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                logger.debug("Executing SQL", extra={"sql": sql, "params": None})
                try:
                    cur.execute(sql)
                    conn.commit()
                    return {
                        "rowcount": cur.rowcount,
                        "statusmessage": cur.statusmessage,
                    }
                except Exception as e:
                    logger.error(
                        "Error executing SQL",
                        extra={"sql": sql, "params": None, "error": str(e)},
                        exc_info=True,
                    )
                    raise

    def wipe_strategy_state(self, cluster_id: Optional[UUID] = None):
        """Clear strategy state table"""
        logger.debug(
            "Starting wipe_strategy_state",
            extra={"cluster_id": str(cluster_id) if cluster_id else None},
        )
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if cluster_id:
                    sql = "DELETE FROM mz_cluster_strategy_state WHERE cluster_id = %s"
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
                    except Exception as e:
                        logger.error(
                            "Error executing SQL",
                            extra={"sql": sql, "params": params, "error": str(e)},
                            exc_info=True,
                        )
                        raise
                else:
                    sql = "DELETE FROM mz_cluster_strategy_state"
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

                conn.commit()
                logger.info(f"Wiped strategy state for {cur.rowcount} clusters")

    def _matches_filter(self, name: str, pattern: str) -> bool:
        """Check if name matches filter pattern (simple regex)"""
        import re

        try:
            return bool(re.search(pattern, name))
        except re.error:
            # If regex is invalid, fall back to substring match
            return pattern.lower() in name.lower()
