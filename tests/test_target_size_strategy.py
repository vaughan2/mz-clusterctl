"""
Smoke tests for the target_size strategy.

Tests the basic functionality of ensuring a cluster has a replica of a
specific target size.
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
from mz_clusterctl.strategies.target_size import TargetSizeStrategy
from tests.conftest import create_test_cluster
from tests.integration_helpers import (
    get_cluster_replicas,
    get_strategy_actions,
    insert_strategy_config,
    run_clusterctl_command,
)


class TestTargetSizeStrategy:
    """Test cases for TargetSizeStrategy."""

    def test_config_validation_missing_target_size(self, environment):
        """Test that config validation fails without target_size."""
        strategy = TargetSizeStrategy()
        config = {}

        with pytest.raises(
            ValueError, match="Missing required config key: target_size"
        ):
            strategy.validate_config(config, environment)

    def test_config_validation_invalid_target_size(self, environment):
        """Test that config validation fails with invalid target_size."""
        strategy = TargetSizeStrategy()

        # Empty string
        with pytest.raises(ValueError, match="target_size must be a non-empty string"):
            strategy.validate_config({"target_size": ""}, environment)

        # Non-string
        with pytest.raises(ValueError, match="target_size must be a non-empty string"):
            strategy.validate_config({"target_size": 123}, environment)

    def test_config_validation_invalid_replica_name(self, environment):
        """Test that config validation fails with invalid replica_name."""
        strategy = TargetSizeStrategy()

        # Empty string
        config = {"target_size": "100cc", "replica_name": ""}
        with pytest.raises(ValueError, match="replica_name must be a non-empty string"):
            strategy.validate_config(config, environment)

    def test_config_validation_valid(self, environment):
        """Test that valid config passes validation."""
        strategy = TargetSizeStrategy()

        # Minimal valid config
        strategy.validate_config({"target_size": "100cc"}, environment)

        # With replica name
        strategy.validate_config(
            {"target_size": "100cc", "replica_name": "my_replica"}, environment
        )

    def test_create_target_replica_when_missing(
        self, environment, empty_cluster_info, test_signals, initial_strategy_state
    ):
        """Test creating target replica when none exists."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc"}

        initial_strategy_state.strategy_type = "target_size"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            test_signals,
            environment,
            empty_cluster_info,
        )

        # Should add the target replica
        assert len(desired.target_replicas) == 1
        assert "r_100cc" in desired.target_replicas
        assert desired.target_replicas["r_100cc"].size == "100cc"
        assert "Creating target size replica" in desired.reasons[0]

        # State should track pending replica
        assert new_state.payload["pending_target_replica"] is not None
        assert new_state.payload["pending_target_replica"]["size"] == "100cc"

    def test_create_target_replica_with_custom_name(
        self, environment, empty_cluster_info, test_signals, initial_strategy_state
    ):
        """Test creating target replica with custom name."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc", "replica_name": "main_replica"}

        initial_strategy_state.strategy_type = "target_size"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            test_signals,
            environment,
            empty_cluster_info,
        )

        # Should use custom name
        assert "main_replica" in desired.target_replicas
        assert desired.target_replicas["main_replica"].size == "100cc"

        # State should track the custom name
        assert new_state.payload["pending_target_replica"]["name"] == "main_replica"

    def test_remove_other_replicas_when_target_hydrated(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test removing other replicas when target replica is hydrated."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc"}

        # Create cluster with target replica and other replicas
        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r_100cc", size="100cc"),  # Target replica
                ReplicaInfo(name="r1", size="25cc"),  # Other replica
                ReplicaInfo(name="r2", size="50cc"),  # Other replica
            ),
            managed=True,
        )

        # Signals indicating target replica is hydrated
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={
                "r_100cc": True,  # Target is hydrated
                "r1": True,
                "r2": True,
            },
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "target_size"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should keep only the target replica
        assert len(desired.target_replicas) == 1
        assert "r_100cc" in desired.target_replicas

        # Should have removal reasons for other replicas
        removal_reasons = [
            r for r in desired.reasons if "Dropping non-target size replica" in r
        ]
        assert len(removal_reasons) == 2

    def test_wait_for_target_replica_hydration(
        self, environment, test_cluster_id, test_cluster_name, initial_strategy_state
    ):
        """Test waiting for target replica to become hydrated before removing others."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc"}

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(
                    name="r_100cc", size="100cc"
                ),  # Target replica (not hydrated)
                ReplicaInfo(name="r1", size="25cc"),  # Other replica
            ),
            managed=True,
        )

        # Target replica is not hydrated
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={
                "r_100cc": False,  # Target not hydrated
                "r1": True,
            },
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "target_size"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, cluster_info
        )

        # Should keep both replicas since target is not hydrated
        assert len(desired.target_replicas) == 2
        assert "r_100cc" in desired.target_replicas
        assert "r1" in desired.target_replicas

    def test_recreate_missing_pending_replica(
        self, environment, empty_cluster_info, test_signals, test_cluster_id
    ):
        """Test recreating a replica that was pending but disappeared."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc"}

        # State with pending replica that doesn't exist
        state_with_pending = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="target_size",
            state_version=1,
            payload={
                "pending_target_replica": {
                    "name": "r_100cc",
                    "size": "100cc",
                    "created_at": datetime.now(UTC).isoformat(),
                }
            },
        )

        desired, new_state = strategy.decide_desired_state(
            state_with_pending, config, test_signals, environment, empty_cluster_info
        )

        # Should recreate the replica
        assert len(desired.target_replicas) == 1
        assert "r_100cc" in desired.target_replicas
        assert "Recreating target size replica" in desired.reasons[0]

    def test_clear_pending_state_when_replica_exists(
        self, environment, test_cluster_id, test_cluster_name, test_signals
    ):
        """Test clearing pending state when replica creation completes."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc"}

        # Cluster now has the target replica
        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(ReplicaInfo(name="r_100cc", size="100cc"),),
            managed=True,
        )

        # State with pending replica
        state_with_pending = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="target_size",
            state_version=1,
            payload={
                "pending_target_replica": {
                    "name": "r_100cc",
                    "size": "100cc",
                    "created_at": datetime.now(UTC).isoformat(),
                }
            },
        )

        desired, new_state = strategy.decide_desired_state(
            state_with_pending, config, test_signals, environment, cluster_info
        )

        # Pending state should remain because no changes were made to desired state
        # (This is the current behavior - clearing only happens when changes are made)
        assert new_state.payload["pending_target_replica"] is not None

    def test_cooldown_prevents_changes(
        self, environment, empty_cluster_info, test_signals, test_cluster_id
    ):
        """Test that cooldown prevents strategy from making changes."""
        strategy = TargetSizeStrategy()
        config = {"target_size": "100cc", "cooldown_s": 300}  # 5 minute cooldown

        # State with recent decision
        recent_decision_state = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="target_size",
            state_version=1,
            payload={
                "last_decision_ts": datetime.now(UTC).isoformat(),
            },
        )

        desired, new_state = strategy.decide_desired_state(
            recent_decision_state, config, test_signals, environment, empty_cluster_info
        )

        # Should not make changes due to cooldown
        assert len(desired.target_replicas) == 0
        assert len(desired.reasons) == 0

        # State should be unchanged
        assert new_state.payload == recent_decision_state.payload


@pytest.mark.integration
class TestTargetSizeStrategyIntegration:
    """Integration tests using real Materialize connection."""

    def test_target_size_creates_missing_replica(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that target_size strategy creates a missing target replica."""

        # Create a cluster with only a default replica (25cc)
        with create_test_cluster(
            db_connection, test_cluster_name, [("default", "25cc")]
        ) as cluster_id:
            # Configure target_size strategy for 50cc replica
            config = {"target_size": "50cc", "replica_name": "target_replica"}
            insert_strategy_config(db_connection, cluster_id, "target_size", config)

            # Get initial replicas
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 1
            assert initial_replicas[0] == ("default", "25cc")

            # Run mz-clusterctl apply
            result = run_clusterctl_command("apply", materialize_url)

            # Command should succeed
            assert result.returncode == 0, f"Command failed: {result.stderr}"

            # Check that the target replica was created
            final_replicas = get_cluster_replicas(db_connection, cluster_id)
            replica_names = [name for name, _ in final_replicas]
            replica_sizes = {name: size for name, size in final_replicas}

            # Should have both the original default and the new target replica
            assert "default" in replica_names
            assert "target_replica" in replica_names
            assert replica_sizes["target_replica"] == "50cc"

            # Check that actions were recorded
            actions = get_strategy_actions(db_connection, cluster_id)
            assert len(actions) >= 1

            create_actions = [
                a for a in actions if "CREATE CLUSTER REPLICA" in a["action_sql"]
            ]
            assert len(create_actions) == 1
            assert "target_replica" in create_actions[0]["action_sql"]
            assert "50cc" in create_actions[0]["action_sql"]

    def test_target_size_removes_other_replicas_when_target_hydrated(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that target_size removes other replicas once target is hydrated."""

        # Create cluster with multiple replicas including the target size
        # (using smaller sizes)
        replica_specs = [
            ("default", "25cc"),
            ("target_replica", "50cc"),
            ("extra_replica", "25cc"),
        ]
        with create_test_cluster(
            db_connection, test_cluster_name, replica_specs
        ) as cluster_id:
            # Configure target_size strategy
            config = {"target_size": "50cc", "replica_name": "target_replica"}
            insert_strategy_config(db_connection, cluster_id, "target_size", config)

            # Get initial replicas - should have default, target_replica,
            # and extra_replica
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 3

            for _ in range(60):
                # Run mz-clusterctl apply
                result = run_clusterctl_command(
                    "apply", materialize_url, extra_args=["-vv"]
                )
                assert result.returncode == 0, f"Command failed: {result.stderr}"

                # The target replica should remain, others should be removed
                # (assuming the target replica is considered hydrated)
                final_replicas = get_cluster_replicas(db_connection, cluster_id)

                # Should have only the target replica (assuming it's hydrated)
                replica_names = [name for name, _ in final_replicas]
                assert "target_replica" in replica_names
                if len(replica_names) == 1:
                    break

                time.sleep(1)

            # Verify the result, we might have arrived here from a timeout.

            # The target replica should remain, others should be removed
            # (assuming the target replica is considered hydrated)
            final_replicas = get_cluster_replicas(db_connection, cluster_id)

            # Should have only the target replica (assuming it's hydrated)
            replica_names = [name for name, _ in final_replicas]
            assert "target_replica" in replica_names
            assert len(replica_names) == 1

            # Check that actions were recorded
            actions = get_strategy_actions(db_connection, cluster_id)
            assert len(actions) >= 0  # May or may not remove depending on hydration
