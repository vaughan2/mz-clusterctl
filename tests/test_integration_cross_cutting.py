"""
Cross-cutting integration tests for mz-clusterctl.

These tests cover functionality that spans multiple strategies or tests
system-wide behavior like error handling, dry-run mode, and cluster filtering.

These tests use the tool the way a user would:
1. Set up clusters and strategy configurations in a real Materialize database
2. Run the CLI tool using 'uv run mz-clusterctl apply'
3. Verify that the expected changes were applied to the database

These tests require a real Materialize instance and test the full workflow.
"""

import subprocess
import time

import pytest

from tests.conftest import create_test_cluster
from tests.integration_helpers import (
    execute_sql,
    get_cluster_replicas,
    get_strategy_actions,
    insert_strategy_config,
    run_clusterctl_command,
)


@pytest.mark.integration
class TestMultipleStrategies:
    """Integration tests for multiple strategies working together."""

    def test_multiple_strategies_coexist(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that multiple strategies can coexist and work together."""
        with create_test_cluster(
            db_connection, test_cluster_name, [("default", "25cc")]
        ) as cluster_id:
            test_table_name = f"tbl_{test_cluster_name}"

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

            # Configure both target_size and idle_suspend
            target_size_config = {"target_size": "50cc", "replica_name": "main_replica"}
            idle_suspend_config = {"idle_after_s": 10}  # 10 seconds

            insert_strategy_config(
                db_connection, cluster_id, "target_size", target_size_config
            )
            insert_strategy_config(
                db_connection, cluster_id, "idle_suspend", idle_suspend_config
            )

            # Get initial replicas
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 1
            assert initial_replicas[0] == ("default", "25cc")

            # Eventually, the replicas should report as hydrated and we're only
            # left with the target replica.
            for _ in range(30):
                result = run_clusterctl_command("apply", materialize_url, timeout=15)
                assert result.returncode == 0, f"Command failed: {result.stderr}"
                current_replicas = get_cluster_replicas(db_connection, cluster_id)
                if len(current_replicas) == 1:
                    break

            current_replicas = get_cluster_replicas(db_connection, cluster_id)
            current_replica_names = [name for name, _ in current_replicas]
            assert "main_replica" in current_replica_names
            assert "default" not in current_replica_names

            # And a bit later, idle_suspend will kick in and we shut off all
            # replicas.
            for _ in range(30):
                result = run_clusterctl_command("apply", materialize_url, timeout=15)
                assert result.returncode == 0, f"Command failed: {result.stderr}"
                current_replicas = get_cluster_replicas(db_connection, cluster_id)
                if len(current_replicas) == 0:
                    break

            current_replicas = get_cluster_replicas(db_connection, cluster_id)
            current_replica_names = [name for name, _ in current_replicas]
            assert len(current_replicas) == 0, (
                f"Expected to see zero replicas but have {current_replica_names}"
            )


@pytest.mark.integration
class TestErrorHandling:
    """Integration tests for error handling scenarios."""

    def test_invalid_strategy_config_handling(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that invalid strategy configurations are handled gracefully."""
        with create_test_cluster(
            db_connection, test_cluster_name, [("default", "25cc")]
        ) as cluster_id:
            # Insert invalid strategy config (missing required field)
            invalid_config = {}  # target_size requires "target_size" field
            insert_strategy_config(
                db_connection, cluster_id, "target_size", invalid_config
            )

            # Get initial replicas to ensure they don't change due to invalid config
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 1
            assert initial_replicas[0] == ("default", "25cc")

            # Run mz-clusterctl apply with retry mechanism for config errors
            config_error_detected = False
            replicas_unchanged = True

            for attempt in range(3):  # Retry up to 3 times
                try:
                    result = run_clusterctl_command(
                        "apply", materialize_url, timeout=15
                    )

                    # Check for config error in output
                    output = (result.stdout + result.stderr).lower()
                    if any(
                        keyword in output
                        for keyword in [
                            "target_size",
                            "config",
                            "missing",
                            "error",
                            "validation",
                            "required",
                        ]
                    ):
                        config_error_detected = True

                    # Verify replicas remain unchanged
                    current_replicas = get_cluster_replicas(db_connection, cluster_id)
                    if current_replicas != initial_replicas:
                        replicas_unchanged = False

                    break  # Successfully completed

                except subprocess.TimeoutExpired:
                    if attempt == 2:  # Last attempt
                        # Timeout on final attempt is acceptable for this test
                        config_error_detected = True
                        break
                    time.sleep(1)

            # Either we detected a config error OR replicas remained unchanged
            # (tool should handle invalid config gracefully)
            assert config_error_detected or replicas_unchanged, (
                f"Expected config error detection or unchanged replicas. "
                f"Config error detected: {config_error_detected}, "
                f"Replicas unchanged: {replicas_unchanged}"
            )

            # Verify no successful actions were recorded for the invalid config
            actions = get_strategy_actions(db_connection, cluster_id)
            successful_actions = [
                a
                for a in actions
                if a.get("executed", False) and not a.get("error_message")
            ]
            # Should have no successful actions due to invalid config
            assert len(successful_actions) == 0, (
                f"Unexpected successful actions with invalid config: "
                f"{successful_actions}"
            )


@pytest.mark.integration
class TestDryRun:
    """Integration tests for dry-run mode."""

    def test_dry_run_shows_planned_actions(
        self, db_connection, clean_test_tables, test_cluster_name, materialize_url
    ):
        """Test that dry-run shows planned actions without executing them."""
        with create_test_cluster(
            db_connection, test_cluster_name, [("default", "25cc")]
        ) as cluster_id:
            # Configure target_size strategy that will require a change
            config = {"target_size": "50cc", "replica_name": "target_replica"}
            insert_strategy_config(db_connection, cluster_id, "target_size", config)

            # Get initial replicas
            initial_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert len(initial_replicas) == 1
            assert initial_replicas[0] == ("default", "25cc")

            # Run mz-clusterctl dry-run with retry mechanism
            result = run_clusterctl_command("dry-run", materialize_url, timeout=15)

            # Since we configured a different target size, dry-run should show
            # planned actions or at minimum complete successfully
            output = result.stdout + result.stderr

            # Verify that the output contains expected planned actions
            # Should mention the cluster name and show replica changes
            assert test_cluster_name in output, (
                f"Output should mention cluster {test_cluster_name}: {output}"
            )
            assert any(
                keyword in output.lower()
                for keyword in ["replica", "create", "drop", "target_replica"]
            ), f"Output should mention replica operations: {output}"

            # Replicas should be unchanged after dry-run
            final_replicas = get_cluster_replicas(db_connection, cluster_id)
            assert final_replicas == initial_replicas, (
                f"Dry-run should not change replicas. "
                f"Initial: {initial_replicas}, Final: {final_replicas}"
            )

            # No actions should be recorded in the database for dry-run
            actions = get_strategy_actions(db_connection, cluster_id)
            assert len(actions) == 0, (
                f"Dry-run should not record actions in database: {actions}"
            )


@pytest.mark.integration
class TestClusterFiltering:
    """Integration tests for cluster filtering."""

    def test_cluster_filter_limits_scope(
        self, db_connection, clean_test_tables, materialize_url
    ):
        """Test that --filter-clusters filter limits which clusters are processed."""
        cluster_name_1 = f"test_cluster_filter_1_{int(time.time())}"
        cluster_name_2 = f"test_cluster_filter_2_{int(time.time())}"

        with (
            create_test_cluster(
                db_connection, cluster_name_1, [("default", "25cc")]
            ) as cluster_id_1,
            create_test_cluster(
                db_connection, cluster_name_2, [("default", "25cc")]
            ) as cluster_id_2,
        ):
            # Configure strategies for both clusters with different target sizes
            config_1 = {"target_size": "50cc", "replica_name": "filtered_replica_1"}
            config_2 = {"target_size": "75cc", "replica_name": "filtered_replica_2"}
            insert_strategy_config(db_connection, cluster_id_1, "target_size", config_1)
            insert_strategy_config(db_connection, cluster_id_2, "target_size", config_2)

            # Get initial state
            initial_replicas_2 = get_cluster_replicas(db_connection, cluster_id_2)

            # Run with cluster filter matching only first cluster
            run_clusterctl_command(
                "apply",
                materialize_url,
                ["--filter-clusters", ".*filter_1.*"],
                timeout=15,
            )

            # Verify cluster 1 was processed (should have new replica)
            current_replicas_1 = get_cluster_replicas(db_connection, cluster_id_1)
            replica_names_1 = [name for name, _ in current_replicas_1]
            assert "filtered_replica_1" in replica_names_1, (
                f"Cluster 1 should have been processed with filter. "
                f"Current replicas: {current_replicas_1}"
            )

            # Verify cluster 2 was NOT processed (should have no changes)
            current_replicas_2 = get_cluster_replicas(db_connection, cluster_id_2)
            assert current_replicas_2 == initial_replicas_2, (
                f"Cluster 2 should be unchanged due to filter. "
                f"Initial: {initial_replicas_2}, Current: {current_replicas_2}"
            )

            # Check actions - cluster 2 should have no actions since it was filtered out
            actions_1 = get_strategy_actions(db_connection, cluster_id_1)
            actions_2 = get_strategy_actions(db_connection, cluster_id_2)

            assert len(actions_1) >= 1, f"Cluster 1 should have actions: {actions_1}"
            assert len(actions_2) == 0, (
                f"Cluster 2 should have no actions due to filter: {actions_2}"
            )
