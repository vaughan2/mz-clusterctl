"""
Signal queries for mz-schedctl

Functions to query activity and hydration status from Materialize system tables.
"""

import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from .models import Signals

logger = logging.getLogger(__name__)


def get_cluster_signals(conn: psycopg.Connection, cluster_id: UUID, cluster_name: str) -> Signals:
    """
    Get activity and hydration signals for a cluster
    
    Args:
        conn: Database connection
        cluster_id: UUID of the cluster
        cluster_name: Name of the cluster
        
    Returns:
        Signals object with activity and hydration data
    """
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
            'cluster_id': str(cluster_id),
            'cluster_name': cluster_name,
            'current_replicas': signals.current_replicas,
            'last_activity_ts': signals.last_activity_ts,
            'hydration_status': signals.hydration_status
        }
    )
    
    return signals


def _get_replica_count(conn: psycopg.Connection, cluster_name: str) -> int:
    """Get current number of replicas for a cluster"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) as count FROM mz_cluster_replicas WHERE cluster_id = (SELECT id FROM mz_clusters WHERE name = %s)",
            (cluster_name,)
        )
        result = cur.fetchone()
        return result['count'] if result else 0


def _get_last_activity(conn: psycopg.Connection, cluster_id: UUID) -> Optional[datetime]:
    """
    Get timestamp of last activity on a cluster
    
    This is a placeholder implementation. In a real deployment, you would query
    appropriate system tables or metrics to determine cluster activity.
    
    Possible approaches:
    - Query mz_internal.mz_active_peeks for active queries
    - Look at mz_internal.mz_dataflow_operators for dataflow activity
    - Check mz_internal.mz_hydration_status for materialized view updates
    """
    with conn.cursor() as cur:
        # Placeholder query - replace with actual activity detection logic
        # This example looks for recent queries that might have used the cluster
        cur.execute("""
            SELECT MAX(finished_at) as last_activity
            FROM mz_internal.mz_statement_execution_history 
            WHERE status = 'success'
            AND finished_at > now() - INTERVAL '1 hour'
            LIMIT 1
        """)
        
        result = cur.fetchone()
        if result and result['last_activity']:
            return result['last_activity']
        
        return None


def _get_hydration_status(conn: psycopg.Connection, cluster_name: str) -> Optional[str]:
    """
    Get hydration status for a cluster
    
    This queries the hydration status of materialized views and indexes
    that might be using this cluster.
    """
    with conn.cursor() as cur:
        # Check if there are any non-hydrated objects on this cluster
        cur.execute("""
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
        """, (cluster_name, cluster_name))
        
        results = cur.fetchall()
        
        total_objects = sum(r['total_objects'] for r in results)
        hydrated_objects = sum(r['hydrated_objects'] for r in results)
        
        if total_objects == 0:
            return 'no_objects'
        elif hydrated_objects == total_objects:
            return 'hydrated'
        elif hydrated_objects == 0:
            return 'not_hydrated'
        else:
            return 'partially_hydrated'


def get_cluster_metrics(conn: psycopg.Connection, cluster_name: str) -> dict:
    """
    Get additional metrics that might be useful for scaling decisions
    
    Returns a dictionary with various cluster metrics.
    """
    metrics = {}
    
    with conn.cursor() as cur:
        # Get cluster size information
        cur.execute("""
            SELECT 
                cr.name as replica_name,
                cr.size,
                cr.availability_zone,
                cr.disk
            FROM mz_cluster_replicas cr
            JOIN mz_clusters c ON cr.cluster_id = c.id
            WHERE c.name = %s
        """, (cluster_name,))
        
        replicas = cur.fetchall()
        metrics['replicas'] = replicas
        metrics['replica_count'] = len(replicas)
        
        # Get workload information (placeholder)
        # In a real implementation, you might query:
        # - Queue depths from mz_internal.mz_compute_operator_durations
        # - Memory usage from system tables
        # - CPU utilization metrics
        
        cur.execute("""
            SELECT COUNT(*) as active_queries
            FROM mz_internal.mz_active_peeks
        """)
        
        result = cur.fetchone()
        metrics['active_queries'] = result['active_queries'] if result else 0
    
    return metrics