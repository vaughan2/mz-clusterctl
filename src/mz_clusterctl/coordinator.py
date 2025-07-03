"""
Strategy coordinator for mz-clusterctl

Coordinates multiple strategies by combining their desired states and resolving
conflicts.
"""

from enum import Enum
from typing import Any

from .log import get_logger
from .models import (
    Action,
    ClusterInfo,
    DesiredState,
    Signals,
    StrategyState,
)

logger = get_logger(__name__)


class ConflictResolution(Enum):
    """Conflict resolution strategies"""

    PRIORITY = "priority"  # Higher priority wins


class StateDiffer:
    """Converts desired state to actions by comparing with current state"""

    def generate_actions(
        self, desired: DesiredState, current_cluster: ClusterInfo
    ) -> list[Action]:
        """Generate actions to transition from current state to desired state"""
        actions = []

        # Get current replica names
        current_replicas = {r.name: r for r in current_cluster.replicas}
        desired_replicas = desired.target_replicas

        # Find replicas to remove (in current but not in desired)
        to_remove = set(current_replicas.keys()) - set(desired_replicas.keys())
        for replica_name in to_remove:
            actions.append(
                Action(
                    sql=f"DROP CLUSTER REPLICA {current_cluster.name}.{replica_name}",
                    expected_state_delta={"replicas_removed": 1},
                )
            )
            logger.debug(
                "Generating DROP action",
                extra={
                    "cluster_id": current_cluster.id,
                    "replica_name": replica_name,
                    "strategy_type": desired.strategy_type,
                },
            )

        # Find replicas to add (in desired but not in current)
        to_add = set(desired_replicas.keys()) - set(current_replicas.keys())
        for replica_name in to_add:
            replica_spec = desired_replicas[replica_name]
            actions.append(
                Action(
                    sql=replica_spec.to_create_sql(current_cluster.name),
                    expected_state_delta={"replicas_added": 1},
                )
            )
            logger.debug(
                "Generating CREATE action",
                extra={
                    "cluster_id": current_cluster.id,
                    "replica_name": replica_name,
                    "replica_size": replica_spec.size,
                    "strategy_type": desired.strategy_type,
                },
            )

        # Find replicas to update (different size or spec)
        to_update = set(current_replicas.keys()) & set(desired_replicas.keys())
        for replica_name in to_update:
            current_replica = current_replicas[replica_name]
            desired_replica = desired_replicas[replica_name]

            # Check if replica needs updating (different size)
            if current_replica.size != desired_replica.size:
                # Drop and recreate with new size
                actions.append(
                    Action(
                        sql=(
                            f"DROP CLUSTER REPLICA "
                            f"{current_cluster.name}.{replica_name}"
                        ),
                        expected_state_delta={"replicas_removed": 1},
                    )
                )
                actions.append(
                    Action(
                        sql=desired_replica.to_create_sql(current_cluster.name),
                        expected_state_delta={"replicas_added": 1},
                    )
                )
                logger.debug(
                    "Generating UPDATE actions",
                    extra={
                        "cluster_id": current_cluster.id,
                        "replica_name": replica_name,
                        "old_size": current_replica.size,
                        "new_size": desired_replica.size,
                        "strategy_type": desired.strategy_type,
                    },
                )

        return actions


class ConflictResolver:
    """Resolves conflicts between multiple desired states"""

    def resolve(
        self, desired_states: list[DesiredState], resolution: ConflictResolution
    ) -> DesiredState:
        """Resolve conflicts between multiple desired states"""
        if not desired_states:
            # Return empty desired state
            return DesiredState(cluster_id="", strategy_type="empty")

        if len(desired_states) == 1:
            return desired_states[0]

        cluster_id = desired_states[0].cluster_id

        if resolution == ConflictResolution.PRIORITY:
            return self._resolve_by_priority(desired_states, cluster_id)
        else:
            raise ValueError(f"Unknown conflict resolution: {resolution}")

    def _resolve_by_priority(
        self, desired_states: list[DesiredState], cluster_id: str
    ) -> DesiredState:
        """Resolve by priority - higher priority strategies win"""
        # Sort by priority (highest first)
        sorted_states = sorted(desired_states, key=lambda s: s.priority, reverse=True)

        result = DesiredState(
            cluster_id=cluster_id,
            strategy_type="priority_composite",
            priority=max(s.priority for s in desired_states),
        )

        # Start with highest priority strategy
        highest_priority = sorted_states[0]
        result.target_replicas = highest_priority.target_replicas.copy()
        result.reason = highest_priority.reason

        # Add replicas from lower priority strategies if they don't conflict
        for state in sorted_states[1:]:
            for replica_name, replica_spec in state.target_replicas.items():
                if replica_name not in result.target_replicas:
                    result.target_replicas[replica_name] = replica_spec

        logger.info(
            "Resolved conflicts using priority strategy",
            extra={
                "cluster_id": cluster_id,
                "strategies_count": len(desired_states),
                "final_replicas_count": len(result.target_replicas),
                "highest_priority": highest_priority.priority,
            },
        )

        return result


class StrategyCoordinator:
    """Coordinates multiple strategies for a single cluster"""

    def __init__(
        self, conflict_resolution: ConflictResolution = ConflictResolution.PRIORITY
    ):
        self.conflict_resolution = conflict_resolution
        self.resolver = ConflictResolver()
        self.differ = StateDiffer()

    def coordinate(
        self,
        strategies_and_configs: list[
            tuple[Any, dict[str, Any]]
        ],  # (strategy_instance, config)
        cluster_info: ClusterInfo,
        signals: Signals,
        strategy_states: dict[str, StrategyState],  # strategy_type -> state
    ) -> tuple[list[Action], dict[str, StrategyState]]:
        """
        Coordinate multiple strategies for a cluster

        Args:
            strategies_and_configs: List of (strategy_instance, config) tuples
            cluster_info: Information about the cluster
            signals: Activity and hydration signals
            strategy_states: Current state for each strategy type

        Returns:
            Tuple of (actions to execute, updated strategy states)
        """
        new_states = {}

        # Sort strategies by priority (lower number = lower priority, meaning
        # lower numbers get processed first)
        sorted_strategies = sorted(
            strategies_and_configs, key=lambda x: x[0].get_priority()
        )

        # Process strategies in priority order, feeding output to next strategy
        current_desired_state = None

        for strategy, config in sorted_strategies:
            strategy_type = config.get("strategy_type", strategy.__class__.__name__)
            current_state = strategy_states.get(strategy_type)

            if current_state is None:
                current_state = strategy.initial_state(cluster_info.id, strategy_type)

            try:
                desired_state, new_state = strategy.decide_desired_state(
                    current_state, config, signals, cluster_info, current_desired_state
                )
                current_desired_state = desired_state
                new_states[strategy_type] = new_state

                logger.debug(
                    "Strategy processed in priority order",
                    extra={
                        "strategy_type": strategy_type,
                        "cluster_id": cluster_info.id,
                        "priority": strategy.get_priority(),
                        "replicas_count": len(desired_state.target_replicas),
                    },
                )

            except Exception as e:
                logger.error(
                    "Error running strategy",
                    extra={
                        "strategy_type": strategy_type,
                        "cluster_id": cluster_info.id,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                continue

        if current_desired_state is None:
            return [], new_states

        # Generate actions from final desired state
        actions = self.differ.generate_actions(current_desired_state, cluster_info)

        logger.info(
            "Strategy coordination completed",
            extra={
                "cluster_id": cluster_info.id,
                "strategies_run": len(sorted_strategies),
                "actions_generated": len(actions),
                "final_replicas_count": len(current_desired_state.target_replicas),
            },
        )

        return actions, new_states
