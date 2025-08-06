"""
Smoke tests for the burst strategy.

Tests the basic functionality of creating burst replicas for fast hydration.
"""

import time
from datetime import UTC, datetime

import pytest

from mz_clusterctl.models import (
    ClusterInfo,
    ReplicaInfo,
    Signals,
    StrategyState,
)
from mz_clusterctl.strategies.burst import BurstStrategy
from src.mz_clusterctl.signals import _get_hydration_status
from tests.conftest import create_test_cluster
from tests.integration_helpers import (
    execute_sql,
    get_cluster_replicas,
    get_strategy_actions,
    insert_strategy_config,
    run_clusterctl_command,
)


class TestBurstStrategy:
    """Test cases for BurstStrategy."""

    def test_config_validation_missing_burst_replica_size(self, environment):
        """Test that config validation fails without burst_replica_size."""
        strategy = BurstStrategy()
        config = {}

        with pytest.raises(
            ValueError, match="Missing required config key: burst_replica_size"
        ):
            strategy.validate_config(config, environment)

    def test_config_validation_invalid_burst_replica_size(self, environment):
        """Test that config validation fails with invalid burst_replica_size."""
        strategy = BurstStrategy()

        # Non-string value
        with pytest.raises(ValueError, match="burst_replica_size must be a string"):
            strategy.validate_config({"burst_replica_size": 123}, environment)

    def test_config_validation_invalid_cooldown(self, environment):
        """Test that config validation fails with invalid cooldown."""
        strategy = BurstStrategy()

        config = {"burst_replica_size": "400cc", "cooldown_s": -60}
        with pytest.raises(ValueError, match="cooldown_s must be >= 0"):
            strategy.validate_config(config, environment)

    def test_config_validation_valid(self, environment):
        """Test that valid config passes validation."""
        strategy = BurstStrategy()

        # Minimal valid config
        strategy.validate_config({"burst_replica_size": "400cc"}, environment)

        # With cooldown
        strategy.validate_config(
            {"burst_replica_size": "400cc", "cooldown_s": 60}, environment
        )

    def test_create_burst_replica_when_no_hydrated_replicas(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test creating burst replica when no other replicas are hydrated."""
        strategy = BurstStrategy()
        config = {"burst_replica_size": "400cc"}

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r1", size="25cc"),  # Not hydrated
                ReplicaInfo(name="r2", size="50cc"),  # Not hydrated
            ),
            managed=True,
        )

        # No replicas are hydrated
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={"r1": False, "r2": False},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "burst"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should add the burst replica
        burst_name = f"{test_cluster_name}_burst"
        assert burst_name in desired.target_replicas
        assert desired.target_replicas[burst_name].size == "400cc"
        assert any("Creating burst replica" in reason for reason in desired.reasons)

    def test_remove_burst_replica_when_others_hydrated(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test removing burst replica when other replicas become hydrated."""
        strategy = BurstStrategy()
        config = {"burst_replica_size": "400cc"}

        burst_name = f"{test_cluster_name}_burst"
        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r1", size="25cc"),
                ReplicaInfo(name=burst_name, size="400cc"),  # Burst replica exists
            ),
            managed=True,
        )

        # Regular replica is hydrated, burst is not
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={"r1": True, burst_name: False},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "burst"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should remove the burst replica
        assert burst_name not in desired.target_replicas
        assert "r1" in desired.target_replicas
        assert any(
            "other replicas are now hydrated" in reason for reason in desired.reasons
        )

    def test_keep_burst_replica_when_no_others_hydrated(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test keeping burst replica when no other replicas are hydrated."""
        strategy = BurstStrategy()
        config = {"burst_replica_size": "400cc"}

        burst_name = f"{test_cluster_name}_burst"
        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r1", size="25cc"),
                ReplicaInfo(name=burst_name, size="400cc"),  # Burst replica exists
            ),
            managed=True,
        )

        # No replicas are hydrated yet
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={"r1": False, burst_name: False},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "burst"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should keep both replicas
        assert burst_name in desired.target_replicas
        assert "r1" in desired.target_replicas
        assert len(desired.reasons) == 0  # No changes needed

    def test_no_burst_replica_needed_when_others_already_hydrated(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test not creating burst replica when other replicas are already hydrated."""
        strategy = BurstStrategy()
        config = {"burst_replica_size": "400cc"}

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(ReplicaInfo(name="r1", size="25cc"),),  # No burst replica
            managed=True,
        )

        # Regular replica is already hydrated
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={"r1": True},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "burst"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should not create burst replica
        burst_name = f"{test_cluster_name}_burst"
        assert burst_name not in desired.target_replicas
        assert "r1" in desired.target_replicas
        assert len(desired.reasons) == 0

    def test_cooldown_prevents_changes(
        self, environment, test_cluster_id, test_cluster_name
    ):
        """Test that cooldown prevents strategy from making changes."""
        strategy = BurstStrategy()
        config = {"burst_replica_size": "400cc", "cooldown_s": 300}  # 5 minute cooldown

        # State with recent decision
        recent_decision_state = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="burst",
            state_version=1,
            payload={
                "last_decision_ts": datetime.now(UTC).isoformat(),
            },
        )

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(ReplicaInfo(name="r1", size="25cc"),),
            managed=True,
        )

        # No replicas hydrated (would normally trigger burst replica)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={"r1": False},
            replica_crash_info={},
        )

        desired, new_state = strategy.decide_desired_state(
            recent_decision_state, config, signals, environment, cluster_info
        )

        # Should not make changes due to cooldown
        burst_name = f"{test_cluster_name}_burst"
        assert burst_name not in desired.target_replicas
        assert len(desired.reasons) == 0

        # State should be unchanged
        assert new_state.payload == recent_decision_state.payload


@pytest.mark.integration
class TestBurstStrategyIntegration:
    """Integration tests using real Materialize connection."""

    def test_burst_strategy_lifecycle_with_rehydration(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test burst strategy creates and removes burst replica during rehydration."""

        config = {"burst_replica_size": "50cc"}
        burst_replica_name = f"{test_cluster_name}_burst"
        test_view_name = f"sales_large_tbl_{test_cluster_name}"

        with create_test_cluster(
            db_connection, test_cluster_name, [("regular_replica", "25cc")]
        ) as cluster_id:
            # Insert strategy config
            insert_strategy_config(db_connection, cluster_id, "burst", config)

            # Verify initial state - only regular replica exists and not hydrated
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            initial_replica_names = [name for name, _ in initial_replicas]
            assert "regular_replica" in initial_replica_names
            assert burst_replica_name not in initial_replica_names

            # Clean up any existing test objects first
            try:
                execute_sql(
                    db_connection, f"DROP INDEX IF EXISTS {test_view_name}_primary_idx"
                )
                execute_sql(db_connection, f"DROP VIEW IF EXISTS {test_view_name}")
            except Exception:
                pass  # Ignore cleanup errors

            # Set cluster context and create objects that need rehydration
            execute_sql(db_connection, f"SET cluster = {test_cluster_name}")
            execute_sql(
                db_connection,
                f"CREATE VIEW {test_view_name} AS SELECT generate_series(1,150000)",
            )
            execute_sql(db_connection, f"CREATE DEFAULT INDEX ON {test_view_name}")

            # First run - should attempt to create burst replica due to hydration needs
            run_clusterctl_command("apply", materialize_url)

            # Check strategy actions to see what happened
            actions_after_first = get_strategy_actions(db_connection, cluster_id)

            # The strategy should have attempted to create the burst replica
            create_attempts = [
                a
                for a in actions_after_first
                if "CREATE" in a["action_sql"] and burst_replica_name in a["action_sql"]
            ]
            assert len(create_attempts) > 0, (
                "Should have attempted to create burst replica"
            )

            # Verify replicas after first run
            replicas_after_first_run = get_cluster_replicas(db_connection, cluster_id)
            replica_names_after_first_run = [
                name for name, _ in replicas_after_first_run
            ]

            # Burst replica was successfully created
            assert burst_replica_name in replica_names_after_first_run

            # Wait for hydration to complete on the regular replica.
            hydration_complete = False
            for _ in range(60):
                hydration_status = _get_hydration_status(
                    db_connection, cluster_id
                )
                if hydration_status.get("regular_replica", False):
                    hydration_complete = True
                    break
                time.sleep(1)

            assert hydration_complete

            # Second run - should remove burst replica now that main replica is
            # hydrated
            run_clusterctl_command("apply", materialize_url)

            # Check final state
            final_replicas = get_cluster_replicas(db_connection, cluster_id)
            final_replica_names = [name for name, _ in final_replicas]

            # Check if burst replica was removed
            all_actions = get_strategy_actions(db_connection, cluster_id)
            drop_attempts = [
                a
                for a in all_actions
                if "DROP" in a["action_sql"] and burst_replica_name in a["action_sql"]
            ]
            assert len(drop_attempts) > 0, "Should have attempted to drop burst replica"

            # Verify the regular replica still exists
            assert "regular_replica" in final_replica_names
            assert burst_replica_name not in final_replica_names

            # Clean up test objects
            try:
                execute_sql(
                    db_connection, f"DROP INDEX IF EXISTS {test_view_name}_primary_idx"
                )
                execute_sql(db_connection, f"DROP VIEW IF EXISTS {test_view_name}")
            except Exception:
                pass  # Ignore cleanup errors
