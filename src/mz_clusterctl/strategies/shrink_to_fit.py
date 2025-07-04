"""
Shrink to fit strategy for mz-clusterctl

Strategy that creates replicas of all possible sizes up to a configurable max size,
then drops larger replicas when smaller ones become hydrated, arriving at the
smallest replica size needed.
"""

from datetime import datetime
from typing import Any

from ..log import get_logger
from ..models import ClusterInfo, DesiredState, ReplicaSpec, Signals, StrategyState
from .base import Strategy

logger = get_logger(__name__)


class ShrinkToFitStrategy(Strategy):
    """
    Shrink to fit strategy implementation

    This strategy:
    1. Creates replicas of all possible sizes up to max_replica_size
    2. Drops larger replicas when smaller ones become hydrated
    3. Arrives at the smallest replica size that can handle the workload
    """

    # Standard Materialize replica sizes in order from smallest to largest, at
    # least for local testing...
    REPLICA_SIZES = ["1", "2", "4", "8", "16"]

    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate shrink to fit strategy configuration"""
        required_keys = ["max_replica_size"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        max_size = config["max_replica_size"]
        if not isinstance(max_size, str) or max_size not in self.REPLICA_SIZES:
            raise ValueError(
                f"max_replica_size must be a valid replica size: {max_size}"
            )

        # Optional cooldown period
        if "cooldown_s" in config and config["cooldown_s"] < 0:
            raise ValueError("cooldown_s must be >= 0")

    def _get_sizes_up_to_max(self, max_size: str) -> list[str]:
        """Get all replica sizes up to and including max_size"""
        try:
            max_index = self.REPLICA_SIZES.index(max_size)
            return self.REPLICA_SIZES[: max_index + 1]
        except ValueError as e:
            raise ValueError(f"Invalid max_replica_size: {max_size}") from e

    def _get_replica_size_index(self, size: str) -> int:
        """Get the index of a replica size in the ordered list"""
        try:
            return self.REPLICA_SIZES.index(size)
        except ValueError:
            # If size not in standard list, treat as unknown/maximum
            return len(self.REPLICA_SIZES)

    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> tuple[DesiredState, StrategyState]:
        """Make shrink to fit scaling decisions"""
        self.validate_config(config)

        now = datetime.utcnow()
        max_replica_size = config["max_replica_size"]

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

        # Check cooldown period
        cooldown_s = config.get("cooldown_s", 0)
        if cooldown_s > 0:
            last_decision_ts = current_state.payload.get("last_decision_ts")
            if last_decision_ts:
                last_decision = datetime.fromisoformat(last_decision_ts)
                if (now - last_decision).total_seconds() < cooldown_s:
                    logger.debug(
                        "Skipping decision due to cooldown",
                        extra={
                            "cluster_id": signals.cluster_id,
                            "cooldown_remaining": cooldown_s
                            - (now - last_decision).total_seconds(),
                        },
                    )
                    return desired, current_state

        # Get all valid sizes up to max
        valid_sizes = self._get_sizes_up_to_max(max_replica_size)

        # Find current replicas by size
        current_replicas = list(cluster_info.replicas)
        replicas_by_size = {}
        for replica in current_replicas:
            if replica.size not in replicas_by_size:
                replicas_by_size[replica.size] = []
            replicas_by_size[replica.size].append(replica)

        # Find which sizes are already present in desired state
        desired_replicas_by_size = {}
        for replica_spec in desired.target_replicas.values():
            if replica_spec.size not in desired_replicas_by_size:
                desired_replicas_by_size[replica_spec.size] = []
            desired_replicas_by_size[replica_spec.size].append(replica_spec)

        logger.debug(
            "Evaluating shrink to fit requirements",
            extra={
                "cluster_id": signals.cluster_id,
                "max_replica_size": max_replica_size,
                "valid_sizes": valid_sizes,
                "current_replicas_by_size": {
                    k: len(v) for k, v in replicas_by_size.items()
                },
                "desired_replicas_by_size": {
                    k: len(v) for k, v in desired_replicas_by_size.items()
                },
                "hydration_status": signals.hydration_status,
            },
        )

        # Phase 1: Ensure we have at least one replica of each valid size
        # (if we don't have any replicas at all)
        if not desired.target_replicas:
            for size in valid_sizes:
                replica_name = f"{cluster_info.name}_{size}"
                replica_spec = ReplicaSpec(name=replica_name, size=size)
                desired.add_replica(
                    replica_spec,
                    f"Creating replica size {size} for shrink-to-fit strategy",
                )

            logger.info(
                "Adding all valid size replicas to desired state",
                extra={
                    "cluster_id": signals.cluster_id,
                    "sizes_added": valid_sizes,
                },
            )

        # Phase 2: Drop larger replicas when smaller ones are hydrated
        else:
            # Find the smallest hydrated replica size
            smallest_hydrated_size = None
            smallest_hydrated_index = float("inf")

            for replica_name, is_hydrated in signals.hydration_status.items():
                if is_hydrated and replica_name in desired.target_replicas:
                    replica_spec = desired.target_replicas[replica_name]
                    size_index = self._get_replica_size_index(replica_spec.size)
                    if size_index < smallest_hydrated_index:
                        smallest_hydrated_index = size_index
                        smallest_hydrated_size = replica_spec.size

            if smallest_hydrated_size:
                # Drop all replicas larger than the smallest hydrated one
                replicas_to_drop = []
                for replica_name, replica_spec in desired.target_replicas.items():
                    replica_size_index = self._get_replica_size_index(replica_spec.size)
                    if replica_size_index > smallest_hydrated_index:
                        replicas_to_drop.append((replica_name, replica_spec.size))

                for replica_name, replica_size in replicas_to_drop:
                    desired.remove_replica(
                        replica_name,
                        f"Dropping larger replica ({replica_size}) - "
                        f"smaller replica ({smallest_hydrated_size}) is hydrated",
                    )

                if replicas_to_drop:
                    logger.info(
                        "Dropping larger replicas - smaller replica is hydrated",
                        extra={
                            "cluster_id": signals.cluster_id,
                            "smallest_hydrated_size": smallest_hydrated_size,
                            "replicas_dropped": len(replicas_to_drop),
                            "dropped_sizes": [size for _, size in replicas_to_drop],
                        },
                    )

        # Compute next state
        new_payload = current_state.payload.copy()

        # Check if we made any changes to the desired state
        initial_replica_names = (
            current_desired_state.get_replica_names()
            if current_desired_state
            else {r.name for r in cluster_info.replicas}
        )
        desired_replica_names = desired.get_replica_names()
        changes_made = initial_replica_names != desired_replica_names

        # Update last decision timestamp if any changes were made
        if changes_made:
            new_payload["last_decision_ts"] = now.isoformat()

        # Track replica changes
        replicas_added = len(desired_replica_names - initial_replica_names)
        replicas_removed = len(initial_replica_names - desired_replica_names)

        if replicas_added > 0 or replicas_removed > 0:
            new_payload["last_scale_action"] = {
                "timestamp": now.isoformat(),
                "replicas_added": replicas_added,
                "replicas_removed": replicas_removed,
            }

        next_state = StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

        return desired, next_state

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """Create initial state for shrink to fit strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
            "last_scale_action": None,
        }
        return state

    @classmethod
    def get_priority(cls) -> int:
        """Shrink to fit strategy has medium-low priority (1.5)"""
        return 1
