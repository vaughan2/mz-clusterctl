"""
Burst scaling strategy for mz-schedctl

Auto-scaling strategy that adds replicas when activity is high and removes them during idle periods.
"""

from datetime import datetime
from typing import Any, Dict, List

from .base import Strategy
from ..log import get_logger
from ..models import Action, ClusterInfo, ReplicaSpec, Signals, StrategyState

logger = get_logger(__name__)


class BurstStrategy(Strategy):
    """
    Burst scaling strategy implementation

    This strategy:
    1. Scales up when activity is detected and current capacity might be insufficient
    2. Scales down to 0 replicas after a configured idle period
    3. Respects cooldown periods to avoid thrashing
    4. Enforces maximum replica limits
    """

    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate burst strategy configuration"""
        required_keys = [
            "max_replicas",
            "scale_up_threshold_ms",
            "cooldown_s",
            "idle_after_s",
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        if config["max_replicas"] < 0:
            raise ValueError("max_replicas must be >= 0")
        if config["cooldown_s"] < 0:
            raise ValueError("cooldown_s must be >= 0")
        if config["idle_after_s"] < 0:
            raise ValueError("idle_after_s must be >= 0")

    def decide(
        self,
        current_state: StrategyState,
        config: Dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
    ) -> List[Action]:
        """Make burst scaling decisions"""
        self.validate_config(config)

        actions = []
        now = datetime.utcnow()

        # Check cooldown period
        last_decision_ts = current_state.payload.get("last_decision_ts")
        if last_decision_ts:
            last_decision = datetime.fromisoformat(last_decision_ts)
            cooldown_seconds = config["cooldown_s"]
            if (now - last_decision).total_seconds() < cooldown_seconds:
                logger.debug(
                    "Skipping decision due to cooldown",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "cooldown_remaining": cooldown_seconds
                        - (now - last_decision).total_seconds(),
                    },
                )
                return actions

        # Check for idle shutdown
        idle_after_s = config["idle_after_s"]
        current_replicas = len(cluster_info.replicas)
        if (
            signals.seconds_since_activity
            and signals.seconds_since_activity > idle_after_s
        ):
            if current_replicas > 0:
                for replica in cluster_info.replicas:
                    actions.append(
                        Action(
                            sql=f"DROP CLUSTER REPLICA {cluster_info.name}.{replica.name}",
                            reason=f"Idle for {signals.seconds_since_activity:.0f}s (threshold: {idle_after_s}s)",
                            expected_state_delta={"replicas_removed": 1},
                        )
                    )

                logger.info(
                    "Scaling down idle cluster",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "idle_seconds": signals.seconds_since_activity,
                        "threshold": idle_after_s,
                        "replicas_to_remove": current_replicas,
                    },
                )

        # Check for scale up conditions
        elif self._should_scale_up(config, signals, current_replicas):
            max_replicas = config["max_replicas"]
            if current_replicas < max_replicas:
                # Add one replica
                replica_name = f"{cluster_info.name}-replica-{current_replicas}"

                # Use default size, can be made configurable
                replica_size = config.get("replica_size", "xsmall")
                replica_spec = ReplicaSpec(name=replica_name, size=replica_size)

                actions.append(
                    Action(
                        sql=replica_spec.to_create_sql(cluster_info.name),
                        reason=f"Activity detected, scaling up (current: {current_replicas}, max: {max_replicas})",
                        expected_state_delta={"replicas_added": 1},
                    )
                )

                logger.info(
                    "Scaling up cluster",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "current_replicas": current_replicas,
                        "max_replicas": max_replicas,
                        "replica_size": replica_size,
                    },
                )

        return actions

    def next_state(
        self,
        current_state: StrategyState,
        config: Dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
        actions_taken: List[Action],
    ) -> StrategyState:
        """Compute next state after actions"""
        new_payload = current_state.payload.copy()

        # Update last decision timestamp if any actions were taken
        if actions_taken:
            new_payload["last_decision_ts"] = datetime.utcnow().isoformat()

        # Track replica changes
        replicas_added = sum(
            action.expected_state_delta.get("replicas_added", 0)
            for action in actions_taken
        )
        replicas_removed = sum(
            action.expected_state_delta.get("replicas_removed", 0)
            for action in actions_taken
        )

        if replicas_added > 0 or replicas_removed > 0:
            new_payload["last_scale_action"] = {
                "timestamp": datetime.utcnow().isoformat(),
                "replicas_added": replicas_added,
                "replicas_removed": replicas_removed,
            }

        return StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

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

    def _should_scale_up(
        self, config: Dict[str, Any], signals: Signals, current_replicas: int
    ) -> bool:
        """
        Determine if we should scale up based on current signals

        This is a simplified implementation. In practice, you might want to:
        - Check query queue depths
        - Monitor response times or latency
        - Look at resource utilization
        - Check for pending hydration work
        """
        # If there's recent activity and we have no replicas, we should scale up
        if current_replicas == 0 and signals.seconds_since_activity is not None:
            scale_up_threshold_ms = config["scale_up_threshold_ms"]
            if signals.seconds_since_activity * 1000 <= scale_up_threshold_ms:
                return True

        # Additional scale-up conditions could be added here
        # For example, checking if cluster is not fully hydrated
        if not signals.is_hydrated and current_replicas == 0:
            return True

        # If we have active queries but no replicas, scale up
        # (This would require additional signals from the signals module)

        return False
