"""
Idle suspend strategy for mz-schedctl

Simple strategy that suspends cluster replicas after a period of inactivity.
"""

from datetime import datetime
from typing import Any, Dict, List, Tuple

from .base import Strategy
from ..log import get_logger
from ..models import Action, ClusterInfo, Signals, StrategyState

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
        self,
        current_state: StrategyState,
        config: Dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
    ) -> Tuple[List[Action], StrategyState]:
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
                            "cluster_id": signals.cluster_id,
                            "cooldown_remaining": cooldown_s
                            - (now - last_decision).total_seconds(),
                        },
                    )
                    return actions, current_state

        # Check if cluster is idle and has replicas to suspend
        idle_after_s = config["idle_after_s"]
        current_replicas = len(cluster_info.replicas)
        if current_replicas > 0:
            should_suspend = False
            reason = ""

            if signals.seconds_since_activity is None:
                # No activity recorded - could mean cluster has never been used
                # or activity tracking isn't working. Be conservative and don't suspend.
                logger.info(
                    "No activity data available, not suspending",
                    extra={"cluster_id": signals.cluster_id},
                )
                return actions, current_state

            if signals.seconds_since_activity > idle_after_s:
                should_suspend = True
                reason = f"Idle for {signals.seconds_since_activity:.0f}s (threshold: {idle_after_s}s)"

            if should_suspend:
                for replica in cluster_info.replicas:
                    actions.append(
                        Action(
                            sql=f"DROP CLUSTER REPLICA {cluster_info.name}.{replica.name}",
                            reason=reason,
                            expected_state_delta={"replicas_removed": 1},
                        )
                    )

                logger.info(
                    "Suspending idle cluster",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "idle_seconds": signals.seconds_since_activity,
                        "threshold": idle_after_s,
                        "replicas_to_remove": current_replicas,
                    },
                )

        # Compute next state
        new_payload = current_state.payload.copy()

        # Update last decision timestamp if any actions were taken
        if actions:
            new_payload["last_decision_ts"] = datetime.utcnow().isoformat()

            # Record suspend event
            replicas_removed = sum(
                action.expected_state_delta.get("replicas_removed", 0)
                for action in actions
            )
            if replicas_removed > 0:
                # Store detailed information about each suspended replica
                suspended_replicas = []
                for action in actions:
                    if action.expected_state_delta.get("replicas_removed", 0) > 0:
                        # Extract replica name from DROP statement
                        # Format: "DROP CLUSTER REPLICA cluster_name.replica_name"
                        sql_parts = action.sql.split(".")
                        if len(sql_parts) >= 2:
                            replica_name = sql_parts[-1]
                            # Find the replica info from cluster_info to get the size
                            replica_size = None
                            for replica in cluster_info.replicas:
                                if replica.name == replica_name:
                                    replica_size = replica.size
                                    break

                            suspended_replicas.append(
                                {
                                    "name": replica_name,
                                    "size": replica_size,
                                }
                            )

                new_payload["last_suspend"] = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "replicas_removed": replicas_removed,
                    "reason": actions[0].reason if actions else "unknown",
                    "suspended_replicas": suspended_replicas,
                }

        next_state = StrategyState(
            cluster_id=current_state.cluster_id,
            strategy_type=current_state.strategy_type,
            state_version=self.CURRENT_STATE_VERSION,
            payload=new_payload,
        )

        return actions, next_state

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """Create initial state for idle suspend strategy"""
        state = super().initial_state(cluster_id, strategy_type)
        state.payload = {
            "last_decision_ts": None,
            "last_suspend": None,  # Will contain suspended_replicas list with name/size
            "cluster_name": None,  # Will be populated by engine
        }
        return state
