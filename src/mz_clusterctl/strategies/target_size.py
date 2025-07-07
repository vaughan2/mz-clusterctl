"""
Target size strategy for mz-clusterctl

Strategy that ensures a cluster has a replica of a specific target size.
If the target size replica doesn't exist, it creates one.
If other size replicas exist when the target size replica is hydrated, it drops them.
"""

from datetime import datetime
from typing import Any

from ..log import get_logger
from ..models import ClusterInfo, DesiredState, ReplicaSpec, Signals, StrategyState
from .base import Strategy

logger = get_logger(__name__)


class TargetSizeStrategy(Strategy):
    """
    Target size strategy implementation

    This strategy:
    1. Ensures a cluster has a replica of a specific target size
    2. Creates a target size replica if it doesn't exist
    3. Drops other size replicas when the target size replica is hydrated
    4. Optionally uses a specific replica name for the target size replica
    """

    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate target size strategy configuration"""
        required_keys = ["target_size"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        if not isinstance(config["target_size"], str) or not config["target_size"]:
            raise ValueError("target_size must be a non-empty string")

        # Optional replica name validation
        if "replica_name" in config and (
            not isinstance(config["replica_name"], str) or not config["replica_name"]
        ):
            raise ValueError("replica_name must be a non-empty string")

    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> tuple[DesiredState, StrategyState]:
        """Make target size decisions"""
        self.validate_config(config)

        now = datetime.utcnow()
        target_size = config["target_size"]
        replica_name = config.get("replica_name", f"r_{target_size}")

        # Start with previous desired state if available, otherwise current replicas
        if current_desired_state:
            desired = current_desired_state
        else:
            desired = DesiredState(
                cluster_id=cluster_info.id,
                strategy_type=current_state.strategy_type,
                priority=self.get_priority(),
            )
            # Start with current replicas
            for replica in cluster_info.replicas:
                desired.add_replica(ReplicaSpec(name=replica.name, size=replica.size))

        # Find current replicas by size
        current_replicas = list(cluster_info.replicas)
        target_size_replicas = [r for r in current_replicas if r.size == target_size]
        other_size_replicas = [r for r in current_replicas if r.size != target_size]

        # Track what we've done in state
        pending_target_replica = current_state.payload.get("pending_target_replica")

        logger.debug(
            "Evaluating target size requirements",
            extra={
                "cluster_id": signals.cluster_id,
                "target_size": target_size,
                "replica_name": replica_name,
                "current_target_size_replicas": len(target_size_replicas),
                "other_size_replicas": len(other_size_replicas),
                "pending_target_replica": pending_target_replica,
                "is_hydrated": signals.is_hydrated,
                "hydration_status": signals.hydration_status,
            },
        )

        # Case 1: No target size replica exists and none is pending
        if not target_size_replicas and not pending_target_replica:
            # Add target size replica to desired state
            replica_spec = ReplicaSpec(name=replica_name, size=target_size)
            desired.add_replica(
                replica_spec, f"Creating target size replica ({target_size})"
            )

            logger.info(
                "Adding target size replica to desired state",
                extra={
                    "cluster_id": signals.cluster_id,
                    "target_size": target_size,
                    "replica_name": replica_name,
                },
            )

        # Case 2: Target size replica exists and is hydrated, drop other replicas
        elif target_size_replicas and other_size_replicas:
            # Check if the target size replica is hydrated
            target_replica_hydrated = any(
                signals.is_replica_hydrated(replica.name)
                for replica in target_size_replicas
            )

            if target_replica_hydrated:
                for replica in other_size_replicas:
                    desired.remove_replica(
                        replica.name,
                        f"Dropping non-target size replica ({replica.size}) - "
                        f"target size replica is hydrated",
                    )

                logger.info(
                    "Removing non-target size replicas from desired state",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "target_size": target_size,
                        "replicas_to_drop": len(other_size_replicas),
                    },
                )

        # Case 3: We have a pending target replica, check if it now exists
        elif pending_target_replica and target_size_replicas:
            # The pending replica now exists, we can clear the pending state
            logger.info(
                "Target size replica creation completed",
                extra={
                    "cluster_id": signals.cluster_id,
                    "target_size": target_size,
                    "replica_name": replica_name,
                },
            )

        # Case 4: We have a pending target replica but it doesn't exist -
        # need to recreate
        elif pending_target_replica and not target_size_replicas:
            # Add the target replica to desired state again
            replica_spec = ReplicaSpec(name=replica_name, size=target_size)
            desired.add_replica(
                replica_spec,
                f"Recreating target size replica ({target_size}) - "
                f"pending replica not found",
            )

            logger.info(
                "Adding target size replica to desired state "
                "(pending replica not found)",
                extra={
                    "cluster_id": signals.cluster_id,
                    "target_size": target_size,
                    "replica_name": replica_name,
                    "pending_replica": pending_target_replica,
                },
            )

        # Compute next state
        new_payload = current_state.payload.copy()

        # Check if we made any changes to the desired state
        current_replica_names = {r.name for r in cluster_info.replicas}
        desired_replica_names = desired.get_replica_names()
        changes_made = current_replica_names != desired_replica_names

        # Update state based on changes
        if changes_made:
            new_payload["last_decision_ts"] = now.isoformat()

            # Track replica changes
            replicas_added = len(desired_replica_names - current_replica_names)

            # Track pending target replica creation
            if replicas_added > 0 and replica_name in desired_replica_names:
                new_payload["pending_target_replica"] = {
                    "name": replica_name,
                    "size": target_size,
                    "created_at": now.isoformat(),
                }

            # Clear pending state when target replica is found
            if target_size_replicas and pending_target_replica:
                new_payload["pending_target_replica"] = None

        next_state = StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

        return desired, next_state

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """Create initial state for target size strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
            "pending_target_replica": None,  # Will contain name/size info when creating
        }
        return state

    @classmethod
    def get_priority(cls) -> int:
        """Target size strategy has lowest priority (0)"""
        return 0
