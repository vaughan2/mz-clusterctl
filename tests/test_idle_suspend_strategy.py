"""
Smoke tests for the idle_suspend strategy.

Tests the basic functionality of suspending cluster replicas after inactivity.
"""

import contextlib
import time
from datetime import UTC, datetime, timedelta

import pytest

from mz_clusterctl.models import (
    Signals,
    StrategyState,
)
from mz_clusterctl.strategies.idle_suspend import IdleSuspendStrategy
from tests.conftest import create_test_cluster
from tests.integration_helpers import (
    execute_sql,
    get_cluster_replicas,
    get_strategy_actions,
    insert_strategy_config,
    run_clusterctl_command,
)


class TestIdleSuspendStrategy:
    """Test cases for IdleSuspendStrategy."""

    def test_config_validation_missing_idle_after_s(self, environment):
        """Test that config validation fails without idle_after_s."""
        strategy = IdleSuspendStrategy()
        config = {}

        with pytest.raises(
            ValueError, match="Missing required config key: idle_after_s"
        ):
            strategy.validate_config(config, environment)

    def test_config_validation_invalid_idle_after_s(self, environment):
        """Test that config validation fails with invalid idle_after_s."""
        strategy = IdleSuspendStrategy()

        # Zero or negative values
        with pytest.raises(ValueError, match="idle_after_s must be > 0"):
            strategy.validate_config({"idle_after_s": 0}, environment)

        with pytest.raises(ValueError, match="idle_after_s must be > 0"):
            strategy.validate_config({"idle_after_s": -300}, environment)

    def test_config_validation_invalid_cooldown(self, environment):
        """Test that config validation fails with invalid cooldown."""
        strategy = IdleSuspendStrategy()

        config = {"idle_after_s": 300, "cooldown_s": -60}
        with pytest.raises(ValueError, match="cooldown_s must be >= 0"):
            strategy.validate_config(config, environment)

    def test_config_validation_valid(self, environment):
        """Test that valid config passes validation."""
        strategy = IdleSuspendStrategy()

        # Minimal valid config
        strategy.validate_config({"idle_after_s": 300}, environment)

        # With cooldown
        strategy.validate_config({"idle_after_s": 300, "cooldown_s": 60}, environment)

    def test_suspend_when_no_activity_data(
        self, environment, test_cluster_info, test_cluster_id, initial_strategy_state
    ):
        """Test suspending when no activity data is available."""
        strategy = IdleSuspendStrategy()
        config = {"idle_after_s": 300}

        # Signals with no activity data
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=None,  # No activity recorded
            hydration_status={"r1": True, "r2": True},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "idle_suspend"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, test_cluster_info
        )

        # Should remove all replicas
        assert len(desired.target_replicas) == 0
        assert any("No activity data available" in reason for reason in desired.reasons)

        # State should track the decision
        assert new_state.payload["last_decision_ts"] is not None

    def test_suspend_when_idle_too_long(
        self, environment, test_cluster_info, test_cluster_id, initial_strategy_state
    ):
        """Test suspending when cluster has been idle too long."""
        strategy = IdleSuspendStrategy()
        config = {"idle_after_s": 300}  # 5 minutes

        # Activity was 10 minutes ago
        old_activity = datetime.now(UTC) - timedelta(minutes=10)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=old_activity,
            hydration_status={"r1": True, "r2": True},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "idle_suspend"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, test_cluster_info
        )

        # Should remove all replicas
        assert len(desired.target_replicas) == 0
        assert any(
            "Idle for" in reason and "threshold: 300s" in reason
            for reason in desired.reasons
        )

    def test_no_suspension_when_recently_active(
        self, environment, test_cluster_info, test_cluster_id, initial_strategy_state
    ):
        """Test not suspending when cluster was recently active."""
        strategy = IdleSuspendStrategy()
        config = {"idle_after_s": 300}  # 5 minutes

        # Activity was 2 minutes ago (less than threshold)
        recent_activity = datetime.now(UTC) - timedelta(minutes=2)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=recent_activity,
            hydration_status={"r1": True, "r2": True},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "idle_suspend"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, test_cluster_info
        )

        # Should keep both replicas
        assert len(desired.target_replicas) == 2
        assert "r1" in desired.target_replicas
        assert "r2" in desired.target_replicas
        assert len(desired.reasons) == 0

    def test_no_suspension_when_no_replicas(
        self, environment, empty_cluster_info, test_cluster_id, initial_strategy_state
    ):
        """Test that strategy does nothing when there are no replicas."""
        strategy = IdleSuspendStrategy()
        config = {"idle_after_s": 300}

        # Old activity but no replicas
        old_activity = datetime.now(UTC) - timedelta(minutes=10)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=old_activity,
            hydration_status={},
            replica_crash_info={},
        )

        initial_strategy_state.strategy_type = "idle_suspend"

        desired, new_state = strategy.decide_desired_state(
            initial_strategy_state, config, signals, environment, empty_cluster_info
        )

        # Should do nothing
        assert len(desired.target_replicas) == 0
        assert len(desired.reasons) == 0

    def test_cooldown_prevents_suspension(
        self, environment, test_cluster_info, test_cluster_id
    ):
        """Test that cooldown prevents strategy from making changes."""
        strategy = IdleSuspendStrategy()
        config = {"idle_after_s": 300, "cooldown_s": 300}  # 5 minute cooldown

        # State with recent decision
        recent_decision_state = StrategyState(
            cluster_id=test_cluster_id,
            strategy_type="idle_suspend",
            state_version=1,
            payload={
                "last_decision_ts": datetime.now(UTC).isoformat(),
            },
        )

        # Old activity that should trigger suspension
        old_activity = datetime.now(UTC) - timedelta(minutes=10)
        signals = Signals(
            cluster_id=test_cluster_id,
            last_activity_ts=old_activity,
            hydration_status={"r1": True, "r2": True},
            replica_crash_info={},
        )

        desired, new_state = strategy.decide_desired_state(
            recent_decision_state, config, signals, environment, test_cluster_info
        )

        # Should not make changes due to cooldown
        assert len(desired.target_replicas) == 2
        assert len(desired.reasons) == 0

        # State should be unchanged
        assert new_state.payload == recent_decision_state.payload


@pytest.mark.integration
class TestIdleSuspendStrategyIntegration:
    """Integration tests using real Materialize connection."""

    def test_idle_suspend_no_action_on_active_cluster(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that idle_suspend doesn't act on active clusters."""

        with create_test_cluster(
            db_connection, test_cluster_name, [("default", "25cc")]
        ) as cluster_id:
            # Configure idle_suspend strategy with short idle time
            config = {"idle_after_s": 10}  # 10 seconds
            insert_strategy_config(db_connection, cluster_id, "idle_suspend", config)

            test_table_name = f"tbl_{test_cluster_name}"

            # Get initial replicas
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            initial_replica_names = [name for name, _ in initial_replicas]
            assert "default" in initial_replica_names

            # Create some activity to ensure the cluster has a last_activity_ts
            # This simulates the cluster being used recently
            execute_sql(db_connection, f"SET cluster = {test_cluster_name}")
            execute_sql(
                db_connection,
                f"CREATE TABLE IF NOT EXISTS {test_table_name} (hello int)",
            )

            # Produce activity over time to make sure it's captured.
            for _ in range(5):
                execute_sql(db_connection, f"SELECT * from {test_table_name}")
                time.sleep(1)

            # Run mz-clusterctl apply
            result = run_clusterctl_command("apply", materialize_url)
            assert result.returncode == 0, f"Command failed: {result.stderr}"

            # Wait for changes
            time.sleep(0.5)

            # Replicas should be unchanged (cluster has recent activity
            # or no activity data)
            final_replicas = get_cluster_replicas(db_connection, cluster_id)
            final_replica_names = [name for name, _ in final_replicas]
            assert "default" in final_replica_names

    def test_idle_suspend_drops_replicas_when_idle(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that idle_suspend actually drops replicas when cluster is idle."""

        with create_test_cluster(
            db_connection,
            test_cluster_name,
            [("default", "25cc"), ("secondary", "25cc")],
        ) as cluster_id:
            # Configure idle_suspend strategy with short idle time
            config = {"idle_after_s": 10}  # 10 seconds idle timeout
            insert_strategy_config(db_connection, cluster_id, "idle_suspend", config)

            test_table_name = f"tbl_{test_cluster_name}"

            # Get initial replicas - should have both replicas
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            initial_replica_names = [name for name, _ in initial_replicas]
            assert "default" in initial_replica_names
            assert "secondary" in initial_replica_names
            assert len(initial_replicas) == 2

            # Create some activity to ensure the cluster has a last_activity_ts
            # This simulates the cluster being used recently
            execute_sql(db_connection, f"SET cluster = {test_cluster_name}")
            execute_sql(
                db_connection,
                f"CREATE TABLE IF NOT EXISTS {test_table_name} (hello int)",
            )

            # Produce activity over time to make sure it's captured.
            for _ in range(5):
                execute_sql(db_connection, f"SELECT * from {test_table_name}")
                time.sleep(1)

            # First run immediately - should not drop replicas (activity is recent)
            result = run_clusterctl_command("apply", materialize_url)
            assert result.returncode == 0, f"Command failed: {result.stderr}"

            # Check that replicas are still there
            after_first_run = get_cluster_replicas(db_connection, cluster_id)
            after_first_names = [name for name, _ in after_first_run]
            assert "default" in after_first_names
            assert "secondary" in after_first_names
            assert len(after_first_run) == 2

            # Keep running apply until replicas are dropped.

            # Poll for replica removal with deadline
            deadline = time.time() + 30  # 30 second deadline
            final_replicas = []

            while time.time() < deadline:
                # Run clusterctl apply
                result = run_clusterctl_command("apply", materialize_url)
                assert result.returncode == 0, f"Command failed: {result.stderr}"

                # Check if replicas have been dropped
                final_replicas = get_cluster_replicas(db_connection, cluster_id)
                if len(final_replicas) == 0:
                    break

                # Wait a bit before next attempt
                time.sleep(1)

            # Verify replicas were actually dropped
            assert len(final_replicas) == 0, (
                f"Expected no replicas within 30s deadline, but found: {final_replicas}"
            )

            # Need replicas to actually run the query, so use quickstart
            # cluster.
            execute_sql(db_connection, "SET cluster = quickstart")

            # Check strategy actions to verify DROP commands were executed
            actions = get_strategy_actions(db_connection, cluster_id)
            drop_actions = [
                a
                for a in actions
                if "DROP" in a["action_sql"] and a["executed"] is True
            ]
            assert len(drop_actions) >= 2, (
                f"Expected at least 2 DROP actions, got: {len(drop_actions)}"
            )

            # Verify the DROP actions were for our replicas
            drop_sqls = [action["action_sql"] for action in drop_actions]
            assert any("default" in sql for sql in drop_sqls), (
                "Should have dropped 'default' replica"
            )
            assert any("secondary" in sql for sql in drop_sqls), (
                "Should have dropped 'secondary' replica"
            )

            # Clean up test objects
            with contextlib.suppress(Exception):
                execute_sql(db_connection, f"DROP TABLE IF EXISTS {test_table_name}")
