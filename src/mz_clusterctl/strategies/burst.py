"""
Burst scaling strategy for mz-clusterctl

Auto-scaling strategy that adds replicas when activity is high and removes them
during idle periods.
"""

from datetime import datetime
from typing import Any

from ..log import get_logger
from ..models import ClusterInfo, DesiredState, ReplicaSpec, Signals, StrategyState
from .base import Strategy

logger = get_logger(__name__)


class BurstStrategy(Strategy):
    """
    Burst scaling strategy implementation

    This strategy:
    1. Creates a large "burst" replica when no replicas are hydrated
    2. Drops the burst replica when any other replica becomes hydrated
    3. Respects cooldown periods to avoid thrashing
    """

    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate burst strategy configuration"""
        required_keys = [
            "burst_replica_size",
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        # cooldown_s is optional, default to 0 if not provided
        cooldown_s = config.get("cooldown_s", 0)
        if cooldown_s < 0:
            raise ValueError("cooldown_s must be >= 0")
        if not isinstance(config["burst_replica_size"], str):
            raise ValueError("burst_replica_size must be a string")

    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
    ) -> tuple[DesiredState, StrategyState]:
        """Make burst scaling decisions"""
        self.validate_config(config)

        now = datetime.utcnow()

        # Create desired state starting with current replicas
        desired = DesiredState(
            cluster_id=cluster_info.id,
            strategy_type=current_state.strategy_type,
            priority=1,  # Default priority
        )

        # Start with current replicas
        for replica in cluster_info.replicas:
            desired.add_replica(ReplicaSpec(name=replica.name, size=replica.size))

        # Check cooldown period
        last_decision_ts = current_state.payload.get("last_decision_ts")
        if last_decision_ts:
            last_decision = datetime.fromisoformat(last_decision_ts)
            cooldown_seconds = config.get("cooldown_s", 0)
            if (now - last_decision).total_seconds() < cooldown_seconds:
                logger.debug(
                    "Skipping decision due to cooldown",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "cooldown_remaining": cooldown_seconds
                        - (now - last_decision).total_seconds(),
                    },
                )
                desired.reason = "In cooldown period"
                return desired, current_state

        # Main burst logic: manage burst replica based on hydration status
        burst_replica_name = f"{cluster_info.name}_burst"
        burst_replica_size = config["burst_replica_size"]
        has_burst_replica = any(
            replica.name == burst_replica_name for replica in cluster_info.replicas
        )

        # Check if any non-burst replicas are hydrated
        other_replicas_hydrated = any(
            signals.is_replica_hydrated(replica.name)
            for replica in cluster_info.replicas
            if replica.name != burst_replica_name
        )

        # Check if any non-burst replicas exist
        has_other_replicas = any(
            replica.name != burst_replica_name for replica in cluster_info.replicas
        )

        if has_other_replicas and not other_replicas_hydrated and not has_burst_replica:
            # Add burst replica when other replicas exist but none are hydrated
            burst_spec = ReplicaSpec(name=burst_replica_name, size=burst_replica_size)
            desired.add_replica(
                burst_spec, "Creating burst replica - no other replicas are hydrated"
            )

            logger.info(
                "Adding burst replica to desired state",
                extra={
                    "cluster_id": signals.cluster_id,
                    "burst_replica_size": burst_replica_size,
                    "other_replicas_count": len(
                        [
                            r
                            for r in cluster_info.replicas
                            if r.name != burst_replica_name
                        ]
                    ),
                },
            )

        elif has_burst_replica and other_replicas_hydrated:
            # Remove burst replica when other replicas become hydrated
            desired.remove_replica(
                burst_replica_name,
                "Dropping burst replica - other replicas are now hydrated",
            )

            logger.info(
                "Removing burst replica from desired state",
                extra={
                    "cluster_id": signals.cluster_id,
                    "reason": "other replicas hydrated",
                },
            )

        # Compute next state
        new_payload = current_state.payload.copy()

        # Check if we made any changes to the desired state
        current_replica_names = {r.name for r in cluster_info.replicas}
        desired_replica_names = desired.get_replica_names()
        changes_made = current_replica_names != desired_replica_names

        # Update last decision timestamp if any changes were made
        if changes_made:
            new_payload["last_decision_ts"] = datetime.utcnow().isoformat()

        # Track replica changes
        replicas_added = len(desired_replica_names - current_replica_names)
        replicas_removed = len(current_replica_names - desired_replica_names)

        if replicas_added > 0 or replicas_removed > 0:
            new_payload["last_scale_action"] = {
                "timestamp": datetime.utcnow().isoformat(),
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
        """Create initial state for burst strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
            "last_scale_action": None,
            "cluster_name": None,  # Will be populated by engine
        }
        return state
