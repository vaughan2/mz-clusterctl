"""
Idle suspend strategy for mz-clusterqctl

Simple strategy that suspends cluster replicas after a period of inactivity.
"""

from datetime import datetime
from typing import Any

from ..log import get_logger
from ..models import Action, ClusterInfo, Signals, StrategyState
from .base import Strategy

logger = get_logger(__name__)


class IdleSuspendStrategy(Strategy):
    """
    Idle suspend strategy implementation

    This strategy:
    1. Monitors cluster activity
    2. Suspends all replicas after a configured idle period
    3. Automatically recreates replicas when recent activity is detected
    4. Respects cooldown periods to avoid repeated suspend attempts
    """

    def validate_config(self, config: dict[str, Any]) -> None:
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
        config: dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
    ) -> tuple[list[Action], StrategyState]:
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

        idle_after_s = config["idle_after_s"]
        current_replicas = len(cluster_info.replicas)
        last_suspend_info = current_state.payload.get("last_suspend")

        logger.debug(
            "Deciding on suspension or re-creation",
            extra={
                "cluster_id": signals.cluster_id,
                "seconds_since_activity": signals.seconds_since_activity,
                "threshold": idle_after_s,
                "last_suspended_info": last_suspend_info,
            },
        )

        # Check if we should recreate replicas due to recent activity
        if current_replicas == 0 and last_suspend_info:
            # Cluster is currently suspended, check if we should recreate replicas
            if (
                signals.seconds_since_activity is not None
                and signals.seconds_since_activity < idle_after_s
            ):
                # Recent activity detected, recreate the suspended replicas
                suspended_replicas = last_suspend_info.get("suspended_replicas", [])

                for replica_info in suspended_replicas:
                    replica_name = replica_info["name"]
                    replica_size = replica_info["size"]

                    if replica_size:  # Only recreate if we have size info
                        from ..models import ReplicaSpec

                        replica_spec = ReplicaSpec(name=replica_name, size=replica_size)
                        actions.append(
                            Action(
                                sql=replica_spec.to_create_sql(cluster_info.name),
                                reason=(
                                    f"Recreating replica due to recent activity "
                                    f"({signals.seconds_since_activity:.0f}s ago)"
                                ),
                                expected_state_delta={"replicas_added": 1},
                            )
                        )

                if actions:
                    logger.info(
                        "Recreating replicas due to recent activity",
                        extra={
                            "cluster_id": signals.cluster_id,
                            "seconds_since_activity": signals.seconds_since_activity,
                            "threshold": idle_after_s,
                            "replicas_to_recreate": len(actions),
                        },
                    )

        # Check if cluster is idle and has replicas to suspend
        elif current_replicas > 0:
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
                reason = (
                    f"Idle for {signals.seconds_since_activity:.0f}s "
                    f"(threshold: {idle_after_s}s)"
                )

            if should_suspend:
                for replica in cluster_info.replicas:
                    actions.append(
                        Action(
                            sql=(
                                f"DROP CLUSTER REPLICA {cluster_info.name}."
                                f"{replica.name}"
                            ),
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

            # Clear suspend info when replicas are recreated
            replicas_added = sum(
                action.expected_state_delta.get("replicas_added", 0)
                for action in actions
            )
            if replicas_added > 0:
                new_payload["last_suspend"] = None

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
