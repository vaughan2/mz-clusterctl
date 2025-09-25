"""
Unit tests for SQL ordering and deduplication behavior via get_strategy_configs().

These tests verify the SQL DISTINCT ON ordering behavior by testing
the database method directly.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from src.mz_clusterctl.db import Database
from src.mz_clusterctl.models import StrategyConfig


class TestSQLStrategyOrdering:
    """Unit tests for SQL strategy configuration ordering and deduplication."""

    def create_strategy_config(
        self, cluster_id: str, strategy_type: str, config: dict, updated_at: datetime
    ) -> StrategyConfig:
        """Helper to create StrategyConfig for testing."""
        return StrategyConfig(
            cluster_id=cluster_id,
            strategy_type=strategy_type,
            config=config,
            updated_at=updated_at,
        )

    def test_sql_orders_by_updated_at_desc(self):
        """Test SQL query orders by updated_at DESC and returns most recent
        per cluster_id/strategy_type."""
        from datetime import timedelta

        base_time = datetime.now(UTC)

        # Mock database rows - simulate what SQL would return with
        # DISTINCT ON and ORDER BY updated_at DESC
        mock_rows = [
            {
                "cluster_id": "u1",
                "strategy_type": "target_size",
                "config": {"target_size": "200cc"},
                "updated_at": base_time + timedelta(seconds=10),
            },
            {
                "cluster_id": "u1",
                "strategy_type": "burst",
                "config": {"burst_replica_size": "800cc"},
                "updated_at": base_time,
            },
            {
                "cluster_id": "u2",
                "strategy_type": "target_size",
                "config": {"target_size": "400cc"},
                "updated_at": base_time,
            },
        ]

        with patch.object(Database, "__init__", lambda x, y, z=None: None):
            db = Database.__new__(Database)

            # Mock the database connection and cursor
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = mock_rows
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            with patch.object(db, "get_connection") as mock_get_conn:
                mock_get_conn.return_value.__enter__.return_value = mock_conn

                result = db.get_strategy_configs()

        # Should have 3 configs (no duplicates due to DISTINCT ON)
        assert len(result) == 3

        # Check that we get the most recent config for u1/target_size
        # (200cc, not any older value)
        u1_target_size = next(
            c
            for c in result
            if c.cluster_id == "u1" and c.strategy_type == "target_size"
        )
        assert u1_target_size.config["target_size"] == "200cc"

    def test_sql_distinct_on_prevents_duplicates(self):
        """Test that SQL DISTINCT ON prevents duplicate
        cluster_id/strategy_type pairs."""
        from datetime import timedelta

        base_time = datetime.now(UTC)

        # Mock database rows - only most recent per cluster_id/strategy_type
        # should be returned by SQL
        mock_rows = [
            {
                "cluster_id": "u1",
                "strategy_type": "target_size",
                "config": {"target_size": "800cc"},
                "updated_at": base_time + timedelta(seconds=30),  # Most recent
            },
            {
                "cluster_id": "u1",
                "strategy_type": "burst",
                "config": {"burst_replica_size": "1600cc"},
                "updated_at": base_time + timedelta(seconds=25),
            },
            {
                "cluster_id": "u2",
                "strategy_type": "target_size",
                "config": {"target_size": "200cc"},
                "updated_at": base_time + timedelta(seconds=15),
            },
        ]

        with patch.object(Database, "__init__", lambda x, y, z=None: None):
            db = Database.__new__(Database)

            # Mock the database connection and cursor
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = mock_rows
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            with patch.object(db, "get_connection") as mock_get_conn:
                mock_get_conn.return_value.__enter__.return_value = mock_conn

                result = db.get_strategy_configs()

        # Should have exactly 3 configs:
        # u1 target_size (newest), u1 burst, u2 target_size
        assert len(result) == 3

        # Check that we have the right cluster/strategy combinations
        cluster_strategy_pairs = [(c.cluster_id, c.strategy_type) for c in result]
        expected_pairs = [("u1", "target_size"), ("u1", "burst"), ("u2", "target_size")]

        for expected_pair in expected_pairs:
            assert expected_pair in cluster_strategy_pairs

        # Check that u1's target_size is the most recent one (800cc)
        u1_target_size = next(
            c
            for c in result
            if c.cluster_id == "u1" and c.strategy_type == "target_size"
        )
        assert u1_target_size.config["target_size"] == "800cc"

    def test_sql_query_structure(self):
        """Test that the SQL query is correctly structured for deduplication."""
        with patch.object(Database, "__init__", lambda x, y, z=None: None):
            db = Database.__new__(Database)

            # Mock the database connection and cursor to capture the SQL
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            with patch.object(db, "get_connection") as mock_get_conn:
                mock_get_conn.return_value.__enter__.return_value = mock_conn

                db.get_strategy_configs()

        # Verify the SQL was executed
        mock_cursor.execute.assert_called_once()
        sql_call = mock_cursor.execute.call_args[0][0]

        # Check that the SQL contains the expected DISTINCT ON and ORDER BY clauses
        assert "DISTINCT ON (cluster_id, strategy_type)" in sql_call
        assert "ORDER BY cluster_id, strategy_type, updated_at DESC" in sql_call
        assert "FROM mz_cluster_strategies" in sql_call
