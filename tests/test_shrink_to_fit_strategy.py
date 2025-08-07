"""
Smoke tests for the shrink_to_fit strategy.

Tests the basic functionality of creating replicas of all sizes then shrinking
to the smallest that can handle the workload.
"""

import time
from datetime import UTC, datetime

import pytest

from mz_clusterctl.environment import Environment
from mz_clusterctl.models import (
    ClusterInfo,
    ReplicaInfo,
    ReplicaSizeInfo,
    Signals,
    StrategyState,
)
from mz_clusterctl.strategies.shrink_to_fit import ShrinkToFitStrategy
from tests.conftest import create_test_cluster
from tests.integration_helpers import (
    execute_sql,
    get_cluster_replicas,
    get_strategy_actions,
    insert_strategy_config,
    run_clusterctl_command,
)


class TestShrinkToFitStrategy:
    """Test cases for ShrinkToFitStrategy."""

    @pytest.fixture
    def environment_with_sizes(self):
        """Create environment with defined replica sizes."""
        return Environment(
            replica_sizes=[
                ReplicaSizeInfo(
                    size="25cc", processes=1, workers=1, credits_per_hour=0.25
                ),
                ReplicaSizeInfo(
                    size="50cc", processes=1, workers=2, credits_per_hour=0.5
                ),
                ReplicaSizeInfo(
                    size="100cc", processes=1, workers=4, credits_per_hour=1.0
                ),
                ReplicaSizeInfo(
                    size="200cc", processes=2, workers=4, credits_per_hour=2.0
                ),
                ReplicaSizeInfo(
                    size="400cc", processes=4, workers=4, credits_per_hour=4.0
                ),
            ]
        )

    def test_config_validation_missing_max_replica_size(self, environment_with_sizes):
        """Test that config validation fails without max_replica_size."""
        strategy = ShrinkToFitStrategy()
        config = {}

        with pytest.raises(
            ValueError, match="Missing required config key: max_replica_size"
        ):
            strategy.validate_config(config, environment_with_sizes)

    def test_config_validation_invalid_max_replica_size(self, environment_with_sizes):
        """Test that config validation fails with invalid max_replica_size."""
        strategy = ShrinkToFitStrategy()

        # Invalid size not in environment
        with pytest.raises(
            ValueError, match="max_replica_size must be a valid replica size"
        ):
            strategy.validate_config(
                {"max_replica_size": "1000cc"}, environment_with_sizes
            )

        # Non-string value
        with pytest.raises(
            ValueError, match="max_replica_size must be a valid replica size"
        ):
            strategy.validate_config({"max_replica_size": 123}, environment_with_sizes)

    def test_config_validation_invalid_cooldown(self, environment_with_sizes):
        """Test that config validation fails with invalid cooldown."""
        strategy = ShrinkToFitStrategy()

        config = {"max_replica_size": "200cc", "cooldown_s": -60}
        with pytest.raises(ValueError, match="cooldown_s must be >= 0"):
            strategy.validate_config(config, environment_with_sizes)

    def test_config_validation_valid(self, environment_with_sizes):
        """Test that valid config passes validation."""
        strategy = ShrinkToFitStrategy()

        # Minimal valid config
        strategy.validate_config({"max_replica_size": "200cc"}, environment_with_sizes)

        # With cooldown
        strategy.validate_config(
            {"max_replica_size": "200cc", "cooldown_s": 60}, environment_with_sizes
        )

    def test_create_all_sizes_when_no_replicas(
        self,
        environment_with_sizes,
        empty_cluster_info,
        test_signals,
        initial_strategy_state,
    ):
        """Test creating replicas of all sizes up to max when none exist."""
        strategy = ShrinkToFitStrategy()
        config = {"max_replica_size": "100cc"}

        initial_strategy_state.strategy_type = "shrink_to_fit"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            test_signals,
            environment_with_sizes,
            empty_cluster_info,
        )

        # Should create replicas for all sizes up to 100cc
        expected_sizes = ["25cc", "50cc", "100cc"]
        assert len(desired.target_replicas) == len(expected_sizes)

        for size in expected_sizes:
            replica_name = f"{empty_cluster_info.name}_{size}"
            assert replica_name in desired.target_replicas
            assert desired.target_replicas[replica_name].size == size

        # Should have creation reasons
        assert any("Creating replica size" in reason for reason in desired.reasons)

    def test_drop_larger_replicas_when_smaller_hydrated(
        self,
        environment_with_sizes,
        test_cluster_id,
        test_cluster_name,
        initial_strategy_state,
    ):
        """Test dropping larger replicas when smaller ones become hydrated."""
        strategy = ShrinkToFitStrategy()
        config = {"max_replica_size": "200cc"}

        # Cluster has all sizes up to 200cc
        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r_25cc", size="25cc"),
                ReplicaInfo(name="r_50cc", size="50cc"),
                ReplicaInfo(name="r_100cc", size="100cc"),
                ReplicaInfo(name="r_200cc", size="200cc"),
            ),
            managed=True,
        )

        # 50cc replica is hydrated, others are not
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={
                "r_25cc": False,
                "r_50cc": True,  # This one is hydrated
                "r_100cc": False,
                "r_200cc": False,
            },
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "shrink_to_fit"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            signals,
            environment_with_sizes,
            cluster_info,
        )

        # Should keep 25cc and 50cc, drop 100cc and 200cc
        assert "r_25cc" in desired.target_replicas
        assert "r_50cc" in desired.target_replicas
        assert "r_100cc" not in desired.target_replicas
        assert "r_200cc" not in desired.target_replicas

        # Should have drop reasons for larger replicas
        drop_reasons = [r for r in desired.reasons if "Dropping larger replica" in r]
        assert len(drop_reasons) == 2

    def test_keep_all_replicas_when_none_hydrated(
        self,
        environment_with_sizes,
        test_cluster_id,
        test_cluster_name,
        initial_strategy_state,
    ):
        """Test keeping all replicas when none are hydrated yet."""
        strategy = ShrinkToFitStrategy()
        config = {"max_replica_size": "100cc"}

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r_25cc", size="25cc"),
                ReplicaInfo(name="r_50cc", size="50cc"),
                ReplicaInfo(name="r_100cc", size="100cc"),
            ),
            managed=True,
        )

        # No replicas are hydrated yet
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={
                "r_25cc": False,
                "r_50cc": False,
                "r_100cc": False,
            },
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "shrink_to_fit"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            signals,
            environment_with_sizes,
            cluster_info,
        )

        # Should keep all replicas
        assert len(desired.target_replicas) == 3
        assert "r_25cc" in desired.target_replicas
        assert "r_50cc" in desired.target_replicas
        assert "r_100cc" in desired.target_replicas
        assert len(desired.reasons) == 0  # No changes needed

    def test_keep_smallest_hydrated_replica_only(
        self,
        environment_with_sizes,
        test_cluster_id,
        test_cluster_name,
        initial_strategy_state,
    ):
        """Test keeping only the smallest hydrated replica."""
        strategy = ShrinkToFitStrategy()
        config = {"max_replica_size": "200cc"}

        cluster_info = ClusterInfo(
            id=test_cluster_id,
            name=test_cluster_name,
            replicas=(
                ReplicaInfo(name="r_25cc", size="25cc"),
                ReplicaInfo(name="r_50cc", size="50cc"),
                ReplicaInfo(name="r_100cc", size="100cc"),
                ReplicaInfo(name="r_200cc", size="200cc"),
            ),
            managed=True,
        )

        # 25cc replica is hydrated (smallest)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=datetime.now(UTC),
            hydration_status={
                "r_25cc": True,  # Smallest hydrated
                "r_50cc": False,
                "r_100cc": False,
                "r_200cc": False,
            },
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "shrink_to_fit"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state,
            config,
            signals,
            environment_with_sizes,
            cluster_info,
        )

        # Should keep only the 25cc replica
        assert len(desired.target_replicas) == 1
        assert "r_25cc" in desired.target_replicas

        # Should drop all others
        drop_reasons = [r for r in desired.reasons if "Dropping larger replica" in r]
        assert len(drop_reasons) == 3

    def test_cooldown_prevents_changes(
        self, environment_with_sizes, empty_cluster_info, test_signals, test_cluster_id
    ):
        """Test that cooldown prevents strategy from making changes."""
        strategy = ShrinkToFitStrategy()
        config = {"max_replica_size": "100cc", "cooldown_s": 300}  # 5 minute cooldown

        # State with recent decision
        recent_decision_state = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="shrink_to_fit",
            state_version=1,
            payload={
                "last_decision_ts": datetime.now(UTC).isoformat(),
            },
        )

        desired, new_state = strategy.decide_desired_state(
            recent_decision_state,
            config,
            test_signals,
            environment_with_sizes,
            empty_cluster_info,
        )

        # Should not make changes due to cooldown
        assert len(desired.target_replicas) == 0
        assert len(desired.reasons) == 0

        # State should be unchanged
        assert new_state.payload == recent_decision_state.payload


@pytest.mark.integration
class TestShrinkToFitStrategyIntegration:
    """Integration tests using real Materialize connection."""

    def test_shrink_to_fit_creates_ladder_and_shrinks_when_hydrated(
        self,
        db_connection,
        clean_test_tables,
        test_cluster_name,
        materialize_url,
    ):
        """Test shrink_to_fit creates replica ladder then shrinks to smallest."""

        # Use larger max replica size to create a proper ladder
        config = {"max_replica_size": "400cc"}
        test_view_name = f"sales_large_tbl_{test_cluster_name}"

        with create_test_cluster(db_connection, test_cluster_name, []) as cluster_id:
            # Insert strategy config
            insert_strategy_config(db_connection, cluster_id, "shrink_to_fit", config)

            # Get initial replicas - should be empty initially
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 0

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

            # First run - should create replica ladder (25cc, 50cc, 100cc, 200cc, 400cc)
            result = run_clusterctl_command(
                "apply",
                materialize_url,
                extra_args=["--enable-experimental-strategies"],
            )
            assert result.returncode == 0, f"Command failed: {result.stderr}"

            # Check that replica ladder was created
            replicas_after_first = get_cluster_replicas(db_connection, cluster_id)
            replica_names_after_first = [name for name, _ in replicas_after_first]
            replica_sizes_after_first = {
                name: size for name, size in replicas_after_first
            }

            # Should have created replicas for all sizes up to max
            # Note: Using actual available sizes from environment
            expected_sizes = ["25cc", "100cc", "200cc", "300cc", "400cc"]
            expected_replica_names = [
                f"{test_cluster_name}_{size}" for size in expected_sizes
            ]

            # Verify exactly the expected replicas exist with correct sizes
            assert len(replicas_after_first) == len(expected_sizes), (
                f"Expected exactly {len(expected_sizes)} replicas, "
                f"got {len(replicas_after_first)}: {replicas_after_first}"
            )

            # Verify each expected replica exists with the correct size
            for expected_size in expected_sizes:
                expected_name = f"{test_cluster_name}_{expected_size}"
                assert expected_name in replica_names_after_first, (
                    f"Expected replica {expected_name} not found. "
                    f"Actual replicas: {replica_names_after_first}"
                )
                actual_size = replica_sizes_after_first[expected_name]
                assert actual_size == expected_size, (
                    f"Replica {expected_name} has wrong size. "
                    f"Expected: {expected_size}, got: {actual_size}"
                )

            # Verify no unexpected replicas exist
            for actual_name in replica_names_after_first:
                assert actual_name in expected_replica_names, (
                    f"Unexpected replica {actual_name} found. "
                    f"Expected only: {expected_replica_names}"
                )

            # Check strategy actions to verify replica creation
            actions_after_first = get_strategy_actions(db_connection, cluster_id)
            create_actions = [
                a
                for a in actions_after_first
                if "CREATE CLUSTER REPLICA" in a["action_sql"]
            ]
            assert len(create_actions) == len(expected_sizes), (
                f"Expected {len(expected_sizes)} CREATE actions, "
                f"got {len(create_actions)}"
            )

            # Wait for hydration to complete on smaller replicas
            from mz_clusterctl.signals import _get_hydration_status

            hydration_complete = False

            for _ in range(60):  # Wait up to 60 seconds
                hydration_status = _get_hydration_status(db_connection, [cluster_id])

                hydration_status = hydration_status.get(cluster_id)

                # Check if the smallest replica is hydrated
                size = expected_sizes[0]
                replica_name = f"{test_cluster_name}_{size}"
                if hydration_status.get(replica_name, False):
                    # Any replica becoming hydrated is sufficient
                    hydration_complete = True
                    break

                time.sleep(1)

            assert hydration_complete, (
                f"No replica became hydrated within timeout. Status: {hydration_status}"
            )

            # Second run - should shrink to smallest hydrated replica
            result = run_clusterctl_command(
                "apply",
                materialize_url,
                extra_args=["--enable-experimental-strategies"],
            )
            assert result.returncode == 0, f"Command failed: {result.stderr}"

            # Check final state - should have fewer replicas (shrunk to fit)
            final_replicas = get_cluster_replicas(db_connection, cluster_id)
            final_replica_names = [name for name, _ in final_replicas]
            final_replica_sizes = {name: size for name, size in final_replicas}

            # The shrink_to_fit strategy drops all larger replicas when ANY
            # replica becomes hydrated. The strategy aims to use the smallest
            # possible replica size that can handle the workload. We happen to
            # know that the workload fits on the smallest size.

            # The strategy should keep only the smallest replica once hydrated
            expected_final_size = expected_sizes[0]  # Smallest size (25cc)
            expected_final_replica = f"{test_cluster_name}_{expected_final_size}"

            # Verify only the smallest replica remains
            assert len(final_replicas) == 1, (
                f"Expected exactly 1 replica (smallest), "
                f"got {len(final_replicas)}: {final_replicas}"
            )

            assert expected_final_replica in final_replica_names, (
                f"Expected {expected_final_replica} to remain, "
                f"got: {final_replica_names}"
            )

            assert final_replica_sizes[expected_final_replica] == expected_final_size, (
                f"Final replica has wrong size. Expected: {expected_final_size}, "
                f"got: {final_replica_sizes[expected_final_replica]}"
            )

            # All larger replicas should have been dropped
            expected_dropped_sizes = expected_sizes[1:]  # All except the smallest

            # Verify that all larger replicas were dropped
            for dropped_size in expected_dropped_sizes:
                dropped_replica_name = f"{test_cluster_name}_{dropped_size}"
                assert dropped_replica_name not in final_replica_names, (
                    f"Replica {dropped_replica_name} should have been dropped "
                    f"but still exists"
                )

            # Check strategy actions to verify replica removal
            all_actions = get_strategy_actions(db_connection, cluster_id)
            drop_actions = [
                a for a in all_actions if "DROP CLUSTER REPLICA" in a["action_sql"]
            ]

            # Should have dropped the larger replicas (all except the smallest)
            assert len(drop_actions) == len(expected_dropped_sizes), (
                f"Expected {len(expected_dropped_sizes)} DROP actions "
                f"for sizes {expected_dropped_sizes}, "
                f"got {len(drop_actions)} DROP actions"
            )

            # Verify each expected drop action exists
            drop_sqls = [action["action_sql"] for action in drop_actions]
            for dropped_size in expected_dropped_sizes:
                dropped_replica_name = f"{test_cluster_name}_{dropped_size}"
                assert any(dropped_replica_name in sql for sql in drop_sqls), (
                    f"Expected DROP action for {dropped_replica_name} not found. "
                    f"Drop actions: {drop_sqls}"
                )

            # Clean up test objects
            try:
                execute_sql(
                    db_connection, f"DROP INDEX IF EXISTS {test_view_name}_primary_idx"
                )
                execute_sql(db_connection, f"DROP VIEW IF EXISTS {test_view_name}")
            except Exception:
                pass  # Ignore cleanup errors
