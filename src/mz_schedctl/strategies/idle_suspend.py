"""
Idle suspend strategy for mz-schedctl

Simple strategy that suspends cluster replicas after a period of inactivity.
"""

from datetime import datetime
from typing import Any, Dict, List

from .base import Strategy
from ..log import get_logger
from ..models import Action, Signals, StrategyState

logger = get_logger(__name__)


class IdleSuspendStrategy(Strategy):
    """
    Idle suspend strategy implementation

    This strategy:
    1. Monitors cluster activity
    2. Suspends all replicas after a configured idle period
    3. Does not automatically scale back up (manual intervention required)
    4. Respects cooldown periods to avoid repeated suspend attempts
    """

    def validate_config(self, config: Dict[str, Any]) -> None:
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

    def decide(
        self, current_state: StrategyState, config: Dict[str, Any], signals: Signals
    ) -> List[Action]:
        """Make idle suspend decisions"""
        self.validate_config(config)

        actions = []
        now = datetime.utcnow()

        # Check cooldown period (optional)
        cooldown_s = config.get("cooldown_s", 0)
        if cooldown_s > 0:
            last_decision_ts = current_state.payload.get("last_decision_ts")
            if last_decision_ts:
                last_decision = datetime.fromisoformat(last_decision_ts)
                if (now - last_decision).total_seconds() < cooldown_s:
                    logger.debug(
                        "Skipping decision due to cooldown",
                        extra={
                            "cluster_id": str(signals.cluster_id),
                            "cooldown_remaining": cooldown_s
                            - (now - last_decision).total_seconds(),
                        },
                    )
                    return actions

        # Check if cluster is idle and has replicas to suspend
        idle_after_s = config["idle_after_s"]
        if signals.current_replicas > 0:
            should_suspend = False
            reason = ""

            if signals.seconds_since_activity is None:
                # No activity recorded - could mean cluster has never been used
                # or activity tracking isn't working. Be conservative and don't suspend.
                logger.debug(
                    "No activity data available, not suspending",
                    extra={"cluster_id": str(signals.cluster_id)},
                )
                return actions

            if signals.seconds_since_activity > idle_after_s:
                should_suspend = True
                reason = f"Idle for {signals.seconds_since_activity:.0f}s (threshold: {idle_after_s}s)"

            if should_suspend:
                # Get cluster name for DROP commands
                cluster_name = current_state.payload.get("cluster_name", "unknown")

                # Create actions to drop all replicas
                # Note: In practice, you'd need to query the actual replica names
                # This is simplified for the skeleton implementation
                for i in range(signals.current_replicas):
                    replica_name = f"{cluster_name}-replica-{i}"
                    actions.append(
                        Action(
                            sql=f"DROP CLUSTER REPLICA {cluster_name}.{replica_name}",
                            reason=reason,
                            expected_state_delta={"replicas_removed": 1},
                        )
                    )

                logger.info(
                    "Suspending idle cluster",
                    extra={
                        "cluster_id": str(signals.cluster_id),
                        "idle_seconds": signals.seconds_since_activity,
                        "threshold": idle_after_s,
                        "replicas_to_remove": signals.current_replicas,
                    },
                )

        return actions

    def next_state(
        self,
        current_state: StrategyState,
        config: Dict[str, Any],
        signals: Signals,
        actions_taken: List[Action],
    ) -> StrategyState:
        """Compute next state after actions"""
        new_payload = current_state.payload.copy()

        # Update last decision timestamp if any actions were taken
        if actions_taken:
            new_payload["last_decision_ts"] = datetime.utcnow().isoformat()

            # Record suspend event
            replicas_removed = sum(
                action.expected_state_delta.get("replicas_removed", 0)
                for action in actions_taken
            )
            if replicas_removed > 0:
                new_payload["last_suspend"] = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "replicas_removed": replicas_removed,
                    "reason": actions_taken[0].reason if actions_taken else "unknown",
                }

        return StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """Create initial state for idle suspend strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
            "last_suspend": None,
            "cluster_name": None,  # Will be populated by engine
        }
        return state
