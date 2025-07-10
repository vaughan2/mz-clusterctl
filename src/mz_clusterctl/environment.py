"""
Environment queries for mz-clusterctl

Functions to query environment information from Materialize system tables.
"""

from dataclasses import dataclass, field

import psycopg

from .log import get_logger
from .models import ReplicaSizeInfo

logger = get_logger(__name__)


@dataclass
class Environment:
    """Information about a Materialize environment"""

    # Replica sizes, ordered from smallest to largest.
    replica_sizes: list[ReplicaSizeInfo] = field(default_factory=list)


def get_environment_info(
    conn: psycopg.Connection, replica_sizes_override: list[str] | None = None
) -> Environment:
    """
    Get environment information from Materialize

    Args:
        conn: Database connection
        replica_sizes_override: Optional list of replica size names to use instead
            of querying Materialize. Intended for local testing.

    Returns:
        Environment object with environment information
    """
    if replica_sizes_override:
        # Create ReplicaSizeInfo objects from override list with dummy values
        replica_sizes = [
            ReplicaSizeInfo(
                size=size,
                processes=1,  # Dummy value for local testing
                workers=1,  # Dummy value for local testing
                credits_per_hour=1.0,  # Dummy value for local testing
            )
            for size in replica_sizes_override
        ]
        logger.info(
            "Using replica sizes override for local testing",
            extra={
                "replica_sizes_override": replica_sizes_override,
            },
        )
    else:
        replica_sizes = _get_replica_sizes(conn)

    return Environment(replica_sizes=replica_sizes)


def _get_replica_sizes(conn: psycopg.Connection) -> list[ReplicaSizeInfo]:
    """
    Get available replica sizes from mz_cluster_replica_sizes

    Queries the system table to find all available replica sizes, filtering for
    "modern" cloud compute sizes (ending with 'cc' or 'C').

    Returns:
        List of ReplicaSizeInfo objects containing replica size information,
        ordered by processes, workers, and credits per hour
    """
    with conn.cursor() as cur:
        sql = """
            SELECT DISTINCT ON (processes, workers)
                size, processes, workers, credits_per_hour
            FROM mz_cluster_replica_sizes
            WHERE size LIKE '%%cc' OR size LIKE '%%C'
            ORDER BY processes ASC, workers ASC, credits_per_hour ASC
        """
        params = ()
        logger.debug(
            "Executing SQL",
            extra={
                "sql": sql,
                "params": params,
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
        replica_sizes = []

        for result in results:
            replica_sizes.append(
                ReplicaSizeInfo(
                    size=result["size"],
                    processes=result["processes"],
                    workers=result["workers"],
                    credits_per_hour=result["credits_per_hour"],
                )
            )

        logger.debug(
            "Replica sizes retrieved",
            extra={
                "replica_sizes_count": len(replica_sizes),
                "replica_sizes": [
                    {
                        "size": rs.size,
                        "processes": rs.processes,
                        "workers": rs.workers,
                        "credits_per_hour": rs.credits_per_hour,
                    }
                    for rs in replica_sizes
                ],
            },
        )

        return replica_sizes
