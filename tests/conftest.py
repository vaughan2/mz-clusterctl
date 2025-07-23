"""
Pytest configuration and fixtures for mz-clusterctl tests.

This module provides test fixtures for database connections, strategy testing,
and common test utilities.
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from mz_clusterctl.db import Database
from mz_clusterctl.environment import Environment
from mz_clusterctl.models import (
    ClusterInfo,
    ReplicaInfo,
    Signals,
    StrategyState,
)


@pytest.fixture(scope="session")
def materialize_url() -> str:
    """Get the Materialize connection URL from environment."""
    url = os.environ.get("MATERIALIZE_URL")
    if not url:
        pytest.skip("MATERIALIZE_URL environment variable not set")
    return url


@pytest.fixture
def db_connection(materialize_url: str) -> Iterator[psycopg.Connection]:
    """Create a database connection for the test."""
    conn = psycopg.connect(materialize_url, autocommit=True)
    conn.row_factory = dict_row
    yield conn
    conn.close()


@pytest.fixture
def test_cluster_id() -> str:
    """Generate a unique cluster ID for testing."""
    return f"test_cluster_{uuid4().hex[:8]}"


@pytest.fixture
def test_cluster_name() -> str:
    """Generate a unique cluster name for testing."""
    return f"test_cluster_{uuid4().hex[:8]}"


@pytest.fixture
def clean_test_tables(db_connection: psycopg.Connection, materialize_url: str):
    """Clean up test data before and after tests."""
    # Clean up before test
    cleanup_tables = [
        "mz_cluster_strategies",
        "mz_cluster_strategy_state",
        "mz_cluster_strategy_actions",
    ]

    for table in cleanup_tables:
        try:
            with db_connection.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception:
            # Table might not exist, that's fine
            pass

    # Create tables needed for testing using the actual Database class
    db = Database(materialize_url)
    db.ensure_tables()

    yield

    # Clean up after test
    for table in cleanup_tables:
        try:
            with db_connection.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception:
            # Table might not exist, that's fine
            pass


@pytest.fixture
def environment() -> Environment:
    """Create a test environment."""
    from mz_clusterctl.models import ReplicaSizeInfo

    return Environment(
        replica_sizes=[
            ReplicaSizeInfo(size="25cc", processes=1, workers=1, credits_per_hour=0.25),
            ReplicaSizeInfo(size="50cc", processes=1, workers=2, credits_per_hour=0.5),
            ReplicaSizeInfo(size="100cc", processes=1, workers=4, credits_per_hour=1.0),
            ReplicaSizeInfo(size="200cc", processes=2, workers=4, credits_per_hour=2.0),
            ReplicaSizeInfo(size="400cc", processes=4, workers=4, credits_per_hour=4.0),
        ]
    )


@pytest.fixture
def test_cluster_info(test_cluster_id: str, test_cluster_name: str) -> ClusterInfo:
    """Create a test cluster info with some default replicas."""
    return ClusterInfo(
        id=test_cluster_id,
        name=test_cluster_name,
        replicas=(
            ReplicaInfo(name="r1", size="25cc"),
            ReplicaInfo(name="r2", size="50cc"),
        ),
        managed=True,
    )


@pytest.fixture
def empty_cluster_info(test_cluster_id: str, test_cluster_name: str) -> ClusterInfo:
    """Create a test cluster info with no replicas."""
    return ClusterInfo(
        id=test_cluster_id,
        name=test_cluster_name,
        replicas=(),
        managed=True,
    )


@pytest.fixture
def test_signals(test_cluster_id: str) -> Signals:
    """Create test signals with default hydration and activity."""
    return Signals(
        cluster_id=test_cluster_id,
        last_activity_ts=datetime.now(UTC),
        hydration_status={"r1": True, "r2": True},
        replica_crash_info={},
    )


@pytest.fixture
def idle_signals(test_cluster_id: str) -> Signals:
    """Create test signals indicating the cluster is idle."""
    return Signals(
        cluster_id=test_cluster_id,
        last_activity_ts=None,  # No activity
        hydration_status={"r1": True, "r2": True},
        replica_crash_info={},
    )


@pytest.fixture
def initial_strategy_state(test_cluster_id: str) -> StrategyState:
    """Create an initial strategy state for testing."""
    return StrategyState(
        cluster_id=test_cluster_id,
        strategy_type="test_strategy",
        state_version=1,
        payload={},
    )


@contextmanager
def create_test_cluster(
    conn: psycopg.Connection,
    cluster_name: str,
    replica_specs: list[tuple[str, str]] = None,
) -> Iterator[str]:
    """
    Context manager to create and cleanup a test cluster.

    Args:
        conn: Database connection
        cluster_name: Name of the cluster to create
        replica_specs: List of (replica_name, size) tuples

    Yields:
        The cluster ID
    """
    replica_specs = replica_specs or []

    with conn.cursor() as cur:
        # Create unmanaged cluster (must specify REPLICAS explicitly)
        replicas_sql = []

        # Add replicas from specs
        for replica_name, size in replica_specs:
            replicas_sql.append(f'{replica_name} (SIZE "{size}")')

        replicas_str = ", ".join(replicas_sql)
        cur.execute(f'CREATE CLUSTER "{cluster_name}" REPLICAS ({replicas_str})')

        # Get cluster ID
        cur.execute("SELECT id FROM mz_clusters WHERE name = %s", (cluster_name,))
        result = cur.fetchone()
        if not result:
            raise RuntimeError(f"Failed to get cluster ID for {cluster_name}")
        cluster_id = result["id"]

    try:
        yield cluster_id
    finally:
        # Clean up
        with conn.cursor() as cur:
            cur.execute(f'DROP CLUSTER "{cluster_name}" CASCADE')
