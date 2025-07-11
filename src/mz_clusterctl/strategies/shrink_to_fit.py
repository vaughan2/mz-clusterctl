"""
Shrink to fit strategy for mz-clusterctl

Strategy that creates replicas of all possible sizes up to a configurable max size,
then drops larger replicas when smaller ones become hydrated, arriving at the
smallest replica size needed.
"""

from datetime import datetime
from typing import Any

from ..environment import Environment
from ..log import get_logger
from ..models import (
    ClusterInfo,
    DesiredState,
    ReplicaSpec,
    Signals,
    StrategyState,
)
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

    def _get_replica_sizes_from_environment(
        self, environment: Environment, cluster_id: str
    ) -> list[str]:
        """
        Get replica sizes from environment

        Args:
            environment: Environment object containing replica_sizes data
            cluster_id: Cluster ID for logging

        Returns:
            List of replica size strings, ordered from smallest to largest
        """
        # Extract size names from the replica_sizes data
        sizes = []
        for size_info in environment.replica_sizes:
            sizes.append(size_info.size)

        logger.debug(
            "Available replica sizes",
            extra={
                "cluster_id": cluster_id,
                "sizes": sizes,
            },
        )
        return sizes

    def validate_config(self, config: dict[str, Any], environment: Environment) -> None:
        """Validate shrink to fit strategy configuration"""
        # Get replica sizes from environment
        replica_sizes = self._get_replica_sizes_from_environment(environment, "unknown")

        required_keys = ["max_replica_size"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        max_size = config["max_replica_size"]
        if not isinstance(max_size, str) or max_size not in replica_sizes:
            raise ValueError(
                f"max_replica_size must be a valid replica size: {max_size}, "
                f"available sizes: {replica_sizes}"
            )

        # Optional cooldown period
        if "cooldown_s" in config and config["cooldown_s"] < 0:
            raise ValueError("cooldown_s must be >= 0")

        # Optional crash detection thresholds
        if "min_oom_count" in config and config["min_oom_count"] < 1:
            raise ValueError("min_oom_count must be >= 1")
        if "min_crash_count" in config and config["min_crash_count"] < 1:
            raise ValueError("min_crash_count must be >= 1")

    def _get_sizes_up_to_max(
        self, max_size: str, replica_sizes: list[str]
    ) -> list[str]:
        """Get all replica sizes up to and including max_size"""
        try:
            max_index = replica_sizes.index(max_size)
            return replica_sizes[: max_index + 1]
        except ValueError as e:
            raise ValueError(f"Invalid max_replica_size: {max_size}") from e

    def _get_replica_size_index(self, size: str, replica_sizes: list[str]) -> int:
        """Get the index of a replica size in the ordered list"""
        try:
            return replica_sizes.index(size)
        except ValueError:
            # If size not in standard list, treat as unknown/maximum
            return len(replica_sizes)

    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        environment: Environment,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> tuple[DesiredState, StrategyState]:
        """Make shrink to fit scaling decisions"""

        self.validate_config(config, environment)

        desired = self._initialize_desired_state(
            current_state, cluster_info, current_desired_state
        )
        if self._check_cooldown(current_state, config, signals):
            return desired, current_state

        max_replica_size = config["max_replica_size"]

        replica_sizes = self._get_replica_sizes_from_environment(environment, "unknown")

        # Get all valid sizes up to max
        valid_sizes = self._get_sizes_up_to_max(max_replica_size, replica_sizes)

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

        # Get crash detection thresholds
        min_oom_count = config.get("min_oom_count", 1)
        min_crash_count = config.get("min_crash_count", 1)

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
                "crash_detection": {
                    "min_oom_count": min_oom_count,
                    "min_crash_count": min_crash_count,
                    "replicas_with_crashes": len(signals.replica_crash_info),
                },
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

        # Phase 2: Remove crash-looping replicas (they're definitively too small)
        if desired.target_replicas:
            crash_looping_replicas = []
            for replica_name, replica_spec in desired.target_replicas.items():
                if signals.is_replica_oom_looping(replica_name, min_oom_count):
                    crash_summary = signals.get_replica_crash_summary(replica_name)
                    crash_looping_replicas.append(
                        (
                            replica_name,
                            replica_spec.size,
                            f"OOM loops ({crash_summary.get('oom_count', 0)} OOMs)",
                        )
                    )
                elif signals.is_replica_crash_looping(replica_name, min_crash_count):
                    crash_summary = signals.get_replica_crash_summary(replica_name)
                    total_crashes = crash_summary.get("total_crashes", 0)
                    crash_looping_replicas.append(
                        (
                            replica_name,
                            replica_spec.size,
                            f"crash loops ({total_crashes} crashes)",
                        )
                    )

            for replica_name, replica_size, reason in crash_looping_replicas:
                desired.remove_replica(
                    replica_name,
                    f"Removing replica ({replica_size}) - {reason}",
                )

            if crash_looping_replicas:
                logger.info(
                    "Removed crash-looping replicas",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "replicas_removed": len(crash_looping_replicas),
                        "removed_details": [
                            {"name": name, "size": size, "reason": reason}
                            for name, size, reason in crash_looping_replicas
                        ],
                    },
                )

        # Phase 3: Drop larger replicas when smaller ones are healthy
        if desired.target_replicas:
            # Find the smallest healthy replica size (hydrated AND not crash-looping)
            smallest_healthy_size = None
            smallest_healthy_index = float("inf")

            for replica_name, is_hydrated in signals.hydration_status.items():
                is_oom_looping = signals.is_replica_oom_looping(
                    replica_name, min_oom_count
                )
                is_crash_looping = signals.is_replica_crash_looping(
                    replica_name, min_crash_count
                )
                if (
                    is_hydrated
                    and replica_name in desired.target_replicas
                    and not is_oom_looping
                    and not is_crash_looping
                ):
                    replica_spec = desired.target_replicas[replica_name]
                    size_index = self._get_replica_size_index(
                        replica_spec.size, replica_sizes
                    )
                    if size_index < smallest_healthy_index:
                        smallest_healthy_index = size_index
                        smallest_healthy_size = replica_spec.size

            if smallest_healthy_size:
                # Drop all replicas larger than the smallest healthy one
                replicas_to_drop = []
                for replica_name, replica_spec in desired.target_replicas.items():
                    replica_size_index = self._get_replica_size_index(
                        replica_spec.size, replica_sizes
                    )
                    if replica_size_index > smallest_healthy_index:
                        replicas_to_drop.append((replica_name, replica_spec.size))

                for replica_name, replica_size in replicas_to_drop:
                    desired.remove_replica(
                        replica_name,
                        f"Dropping larger replica ({replica_size}) - "
                        f"smaller replica ({smallest_healthy_size}) is healthy",
                    )

                if replicas_to_drop:
                    logger.info(
                        "Dropping larger replicas - smaller replica is healthy",
                        extra={
                            "cluster_id": signals.cluster_id,
                            "smallest_healthy_size": smallest_healthy_size,
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

        now = datetime.utcnow()

        # Update last decision timestamp if any changes were made
        if changes_made:
            new_payload["last_decision_ts"] = now.isoformat()

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
        }
        return state

    @classmethod
    def get_priority(cls) -> int:
        """Shrink to fit strategy has second lowest priority (1)"""
        return 1
