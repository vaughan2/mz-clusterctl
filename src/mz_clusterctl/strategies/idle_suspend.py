"""
Idle suspend strategy for mz-clusterctl

Simple strategy that suspends cluster replicas after a period of inactivity.
Replica recreation is handled by the target_size strategy when combined.
"""

from datetime import UTC, datetime
from typing import Any

from ..environment import Environment
from ..log import get_logger
from ..models import ClusterInfo, DesiredState, Signals, StrategyState
from .base import Strategy

logger = get_logger(__name__)


class IdleSuspendStrategy(Strategy):
    """
    Idle suspend strategy implementation

    This strategy:
    1. Monitors cluster activity
    2. Suspends all replicas after a configured idle period
    3. Respects cooldown periods to avoid repeated suspend attempts

    Note: Replica recreation is handled by the target_size strategy when combined.
    """

    def validate_config(self, config: dict[str, Any], environment: Environment) -> None:
        """Validate idle suspend strategy configuration"""
        required_keys = ["idle_after_s"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        if config["idle_after_s"] <= 0:
            raise ValueError("idle_after_s must be > 0")

        # Optional cooldown period
        if "cooldown_s" in config and config["cooldown_s"] < 0:
            raise ValueError("cooldown_s must be >= 0")

    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        environment: Environment,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> tuple[DesiredState, StrategyState]:
        """Make idle suspend decisions"""
        self.validate_config(config, environment)

        desired = self._initialize_desired_state(
            current_state, cluster_info, current_desired_state
        )
        if self._check_cooldown(current_state, config, signals):
            return desired, current_state

        idle_after_s = config["idle_after_s"]
        current_replicas = len(desired.target_replicas)

        logger.debug(
            "Deciding on suspension",
            extra={
                "cluster_id": signals.cluster_id,
                "seconds_since_activity": signals.seconds_since_activity,
                "threshold": idle_after_s,
                "current_replicas": current_replicas,
            },
        )

        # Check if cluster is idle and has replicas to suspend
        if current_replicas > 0:
            should_suspend = False
            reason = ""

            if signals.seconds_since_activity is None:
                # No activity recorded - suspend as it indicates no recent activity
                should_suspend = True
                reason = "No activity data available - suspending cluster"
                logger.info(
                    "No activity data available, suspending cluster",
                    extra={"cluster_id": signals.cluster_id},
                )

            elif signals.seconds_since_activity > idle_after_s:
                should_suspend = True
                reason = (
                    f"Idle for {signals.seconds_since_activity:.0f}s "
                    f"(threshold: {idle_after_s}s)"
                )

            if should_suspend:
                # Remove all replicas from desired state
                for replica_name in list(desired.target_replicas.keys()):
                    desired.remove_replica(replica_name, reason)

                logger.info(
                    "Removing all replicas from desired state "
                    "(suspending idle cluster)",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "idle_seconds": signals.seconds_since_activity,
                        "threshold": idle_after_s,
                        "replicas_to_remove": current_replicas,
                        "reason": reason,
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
            new_payload["last_decision_ts"] = datetime.now(UTC).isoformat()

        next_state = StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

        return desired, next_state

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """Create initial state for idle suspend strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
        }
        return state

    @classmethod
    def get_priority(cls) -> int:
        """Idle suspend strategy has highest priority (3) -
        suspension trumps other strategies"""
        return 3
