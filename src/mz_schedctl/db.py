"""
Database connection and helper functions for mz-schedctl

Provides PostgreSQL connection pool and database schema management.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .models import ClusterInfo, StrategyConfig, StrategyState

logger = logging.getLogger(__name__)


class Database:
    """Database connection manager and helper methods"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = ConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=10
        )
    
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
            ("mz_cluster_strategies", """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategies (
                    cluster_id    UUID            NOT NULL,
                    strategy_type TEXT            NOT NULL,
                    config        JSONB           NOT NULL,
                    updated_at    TIMESTAMPTZ     DEFAULT now()
                )
            """),
            ("mz_cluster_strategy_state", """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategy_state (
                    cluster_id    UUID            NOT NULL,
                    state_version INT             NOT NULL,
                    payload       JSONB           NOT NULL,
                    updated_at    TIMESTAMPTZ     DEFAULT now()
                )
            """),
            ("mz_cluster_strategy_actions", """
                CREATE TABLE IF NOT EXISTS mz_cluster_strategy_actions (
                    action_id     UUID            NOT NULL,
                    cluster_id    UUID,
                    action_sql    TEXT,
                    decision_ctx  JSONB,
                    executed      BOOL,
                    error_message TEXT,
                    created_at    TIMESTAMPTZ     DEFAULT now()
                )
            """)
        ]
        
        for table_name, sql in tables:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    conn.commit()
        
        logger.info("Database tables ensured")
    
    def get_clusters(self, name_filter: Optional[str] = None) -> List[ClusterInfo]:
        """Get cluster information from mz_catalog.mz_clusters"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, name FROM mz_catalog.mz_clusters")
                clusters = []
                
                for row in cur.fetchall():
                    cluster = ClusterInfo.from_db_row(row)
                    
                    # Apply name filter if provided
                    if name_filter and not self._matches_filter(cluster.name, name_filter):
                        continue
                    
                    # Get replicas for this cluster
                    cur.execute(
                        "SHOW CLUSTER REPLICAS WHERE cluster = %s",
                        (cluster.name,)
                    )
                    cluster.replicas = [replica_row['replica'] for replica_row in cur.fetchall()]
                    
                    clusters.append(cluster)
                
                return clusters
    
    def get_strategy_configs(self, cluster_id: Optional[UUID] = None) -> List[StrategyConfig]:
        """Get strategy configurations from mz_cluster_strategies"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if cluster_id:
                    cur.execute(
                        "SELECT * FROM mz_cluster_strategies WHERE cluster_id = %s",
                        (cluster_id,)
                    )
                else:
                    cur.execute("SELECT * FROM mz_cluster_strategies")
                
                return [StrategyConfig.from_db_row(row) for row in cur.fetchall()]
    
    def get_strategy_state(self, cluster_id: UUID) -> Optional[StrategyState]:
        """Get strategy state for a cluster"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM mz_cluster_strategy_state WHERE cluster_id = %s",
                    (cluster_id,)
                )
                row = cur.fetchone()
                
                if not row:
                    return None
                
                return StrategyState(
                    cluster_id=UUID(row['cluster_id']),
                    strategy_type="",  # Not stored in state table
                    state_version=row['state_version'],
                    payload=row['payload'],
                    updated_at=row['updated_at']
                )
    
    def upsert_strategy_state(self, state: StrategyState):
        """Insert or update strategy state"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mz_cluster_strategy_state 
                    (cluster_id, state_version, payload, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (cluster_id) 
                    DO UPDATE SET
                        state_version = EXCLUDED.state_version,
                        payload = EXCLUDED.payload,
                        updated_at = EXCLUDED.updated_at
                """, (
                    state.cluster_id,
                    state.state_version,
                    state.payload
                ))
                conn.commit()
    
    def log_action(self, cluster_id: UUID, action_sql: str, decision_ctx: Dict[str, Any], 
                   executed: bool, error_message: Optional[str] = None) -> UUID:
        """Log an action to the audit table"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mz_cluster_strategy_actions
                    (cluster_id, action_sql, decision_ctx, executed, error_message)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING action_id
                """, (
                    cluster_id,
                    action_sql,
                    decision_ctx,
                    executed,
                    error_message
                ))
                action_id = cur.fetchone()['action_id']
                conn.commit()
                return UUID(action_id)
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute arbitrary SQL and return result info"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                return {
                    'rowcount': cur.rowcount,
                    'statusmessage': cur.statusmessage
                }
    
    def wipe_strategy_state(self, cluster_id: Optional[UUID] = None):
        """Clear strategy state table"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if cluster_id:
                    cur.execute(
                        "DELETE FROM mz_cluster_strategy_state WHERE cluster_id = %s",
                        (cluster_id,)
                    )
                else:
                    cur.execute("DELETE FROM mz_cluster_strategy_state")
                
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