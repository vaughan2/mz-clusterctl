"""
Unit tests for duplicate strategy configuration deduplication logic.

These tests verify the deduplication behavior without requiring a full database setup.
"""

from datetime import UTC, datetime
from unittest.mock import patch

from src.mz_clusterctl.db import Database
from src.mz_clusterctl.models import StrategyConfig


class TestStrategyDeduplication:
    """Unit tests for strategy configuration deduplication."""

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

    def test_no_duplicates_all_configs_preserved(self):
        """Test that non-duplicate configs are all preserved."""
        base_time = datetime.now(UTC)
        configs = [
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "100cc"}, base_time
            ),
            self.create_strategy_config(
                "u1", "burst", {"burst_replica_size": "800cc"}, base_time
            ),
            self.create_strategy_config(
                "u2", "target_size", {"target_size": "200cc"}, base_time
            ),
        ]

        with patch.object(Database, '__init__', lambda x, y, z=None: None):
            db = Database.__new__(Database)
            result = db._deduplicate_strategy_configs(configs)

        assert len(result) == 3
        assert all(config in result for config in configs)

    def test_single_duplicate_target_size_keeps_most_recent(self):
        """Test that with 2 duplicate target_size configs, most recent is kept."""
        base_time = datetime.now(UTC)
        older_config = self.create_strategy_config(
            "u1", "target_size", {"target_size": "100cc"}, base_time
        )
        newer_config = self.create_strategy_config(
            "u1", "target_size", {"target_size": "200cc"},
            base_time.replace(second=base_time.second + 10)
        )

        # Order them as they would come from SQL (newer first due to ORDER BY DESC)
        configs = [newer_config, older_config]

        with patch.object(Database, '__init__', lambda x, y, z=None: None):
            db = Database.__new__(Database)
            result = db._deduplicate_strategy_configs(configs)

        assert len(result) == 1
        assert result[0] == newer_config
        assert result[0].config["target_size"] == "200cc"

    def test_multiple_duplicates_target_size_keeps_most_recent(self):
        """Test that with 4 duplicate target_size configs, only most recent is kept."""
        from datetime import timedelta
        base_time = datetime.now(UTC).replace(second=0)
        configs = [
            # Ordered as they would come from SQL (most recent first)
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "800cc"},
                base_time + timedelta(seconds=30)
            ),
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "400cc"},
                base_time + timedelta(seconds=20)
            ),
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "200cc"},
                base_time + timedelta(seconds=10)
            ),
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "100cc"}, base_time
            ),
        ]

        with patch.object(Database, '__init__', lambda x, y, z=None: None):
            db = Database.__new__(Database)
            result = db._deduplicate_strategy_configs(configs)

        assert len(result) == 1
        assert result[0] == configs[0]  # Most recent (first in ordered list)
        assert result[0].config["target_size"] == "800cc"

    def test_mixed_duplicates_and_unique_configs(self):
        """Test complex scenario with duplicates and unique configs."""
        from datetime import timedelta
        base_time = datetime.now(UTC).replace(second=0)
        configs = [
            # Most recent target_size for u1 (should be kept)
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "800cc"},
                base_time + timedelta(seconds=30)
            ),
            # Burst strategy for u1 (should be kept - not target_size)
            self.create_strategy_config(
                "u1", "burst", {"burst_replica_size": "1600cc"},
                base_time + timedelta(seconds=25)
            ),
            # Older target_size for u1 (should be removed)
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "400cc"},
                base_time + timedelta(seconds=20)
            ),
            # Target_size for different cluster u2 (should be kept)
            self.create_strategy_config(
                "u2", "target_size", {"target_size": "200cc"},
                base_time + timedelta(seconds=15)
            ),
            # Another older target_size for u1 (should be removed)
            self.create_strategy_config(
                "u1", "target_size", {"target_size": "100cc"},
                base_time + timedelta(seconds=10)
            ),
        ]

        with patch.object(Database, '__init__', lambda x, y, z=None: None):
            db = Database.__new__(Database)
            result = db._deduplicate_strategy_configs(configs)

        # Should have 3 configs: u1 target_size (newest), u1 burst, u2 target_size
        assert len(result) == 3

        # Check that we have the right configs
        cluster_strategy_pairs = [(c.cluster_id, c.strategy_type) for c in result]
        expected_pairs = [("u1", "target_size"), ("u1", "burst"), ("u2", "target_size")]

        for expected_pair in expected_pairs:
            assert expected_pair in cluster_strategy_pairs

        # Check that u1's target_size is the most recent one (800cc)
        u1_target_size = next(
            c for c in result
            if c.cluster_id == "u1" and c.strategy_type == "target_size"
        )
        assert u1_target_size.config["target_size"] == "800cc"

    def test_non_target_size_duplicates_all_preserved(self):
        """Test that non-target_size duplicates are all preserved."""
        from datetime import timedelta
        base_time = datetime.now(UTC).replace(second=0)
        configs = [
            self.create_strategy_config(
                "u1", "burst", {"burst_replica_size": "800cc"}, base_time
            ),
            self.create_strategy_config(
                "u1", "burst", {"burst_replica_size": "1600cc"},
                base_time + timedelta(seconds=10)
            ),
            self.create_strategy_config(
                "u1", "idle_suspend", {"idle_after_s": 1800},
                base_time + timedelta(seconds=5)
            ),
            self.create_strategy_config(
                "u1", "idle_suspend", {"idle_after_s": 3600},
                base_time + timedelta(seconds=15)
            ),
        ]

        with patch.object(Database, '__init__', lambda x, y, z=None: None):
            db = Database.__new__(Database)
            result = db._deduplicate_strategy_configs(configs)

        # All non-target_size configs should be preserved
        assert len(result) == 4
        assert all(config in result for config in configs)