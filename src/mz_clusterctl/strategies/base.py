"""
Base strategy interface for mz-clusterctl

Defines the Strategy abstract base class that all scaling strategies must implement.
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
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

logger = get_logger(__name__)


class Strategy(ABC):
    """
    Abstract base class for all scaling strategies

    Strategies implement the core decision logic for cluster scaling.
    Each strategy receives the current state, the current desired state (as decided
    but potentially other strategies that come first in the priority order),
    configuration, and signals, then returns a list of actions to be executed.
    """

    CURRENT_STATE_VERSION = 1

    @abstractmethod
    def decide_desired_state(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
        environment: Environment,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> tuple[DesiredState, StrategyState]:
        """
        Make scaling decisions by returning desired state

        Args:
            current_state: The current state of this strategy for the cluster
            config: Strategy configuration from mz_cluster_strategies table
            signals: Activity and hydration signals for the cluster
            environment: Environment information about the running Materialize
                environment
            cluster_info: Information about the cluster including current replicas
            current_desired_state: The desired state from the previous strategy in
                                  priority order, or None if this is the first strategy

        Returns:
            Tuple of (desired state, new strategy state to be persisted)
        """
        pass

    @classmethod
    def initial_state(cls, cluster_id, strategy_type: str) -> StrategyState:
        """
        Create initial state for a new cluster/strategy combination

        Args:
            cluster_id: ID of the cluster
            strategy_type: Type of strategy (e.g., 'burst', 'idle_suspend')

        Returns:
            Initial StrategyState with default payload
        """
        return StrategyState(
            cluster_id=cluster_id,
            strategy_type=strategy_type,
            state_version=cls.CURRENT_STATE_VERSION,
            payload={},
        )

    def is_state_version_compatible(self, state: StrategyState) -> bool:
        """
        Check if the given state is compatible with this strategy version

        Args:
            state: The state to check

        Returns:
            True if compatible, False if state needs to be reset
        """
        return state.state_version == self.CURRENT_STATE_VERSION

    @abstractmethod
    def validate_config(self, config: dict[str, Any], environment: Environment) -> None:
        """
        Validate strategy configuration

        Args:
            config: Configuration dictionary to validate
            environment: Environment information about the running Materialize
                environment

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    @classmethod
    def get_priority(cls) -> int:
        """
        Get the priority for this strategy type

        Lower numbers = lower priority (processed first)
        Default priority is 0

        Returns:
            Priority value for this strategy
        """
        return 0

    def get_max_activity_lookback_seconds(self, config: dict[str, Any]) -> int | None:
        """
        Get the maximum lookback time in seconds this strategy needs for activity data

        Args:
            config: Strategy configuration

        Returns:
            Maximum lookback time in seconds, or None if no lookback is needed
        """
        return None

    def _initialize_desired_state(
        self,
        current_state: StrategyState,
        cluster_info: ClusterInfo,
        current_desired_state: DesiredState | None = None,
    ) -> DesiredState:
        """
        Initialize desired state from previous state or current replicas

        Args:
            current_state: Current strategy state
            cluster_info: Information about the cluster
            current_desired_state: Previous desired state from other strategies

        Returns:
            Initialized DesiredState
        """
        if current_desired_state:
            return current_desired_state
        else:
            desired = DesiredState(
                cluster_id=cluster_info.id,
                strategy_type=current_state.strategy_type,
                priority=self.get_priority(),
            )
            # Start with current replicas
            for replica in cluster_info.replicas:
                desired.add_replica(ReplicaSpec(name=replica.name, size=replica.size))
            return desired

    def _check_cooldown(
        self,
        current_state: StrategyState,
        config: dict[str, Any],
        signals: Signals,
    ) -> bool:
        """
        Check if strategy is in cooldown period

        Args:
            current_state: Current strategy state
            config: Strategy configuration
            signals: Activity and hydration signals

        Returns:
            True if in cooldown (should skip decision), False otherwise
        """
        cooldown_s = config.get("cooldown_s", 0)
        if cooldown_s <= 0:
            return False

        last_decision_ts = current_state.payload.get("last_decision_ts")
        if not last_decision_ts:
            return False

        try:
            last_decision = datetime.fromisoformat(last_decision_ts)
            now = datetime.now(UTC)
            time_since_last = (now - last_decision).total_seconds()

            if time_since_last < cooldown_s:
                logger.debug(
                    "Skipping decision due to cooldown",
                    extra={
                        "cluster_id": signals.cluster_id,
                        "strategy_type": current_state.strategy_type,
                        "cooldown_remaining": cooldown_s - time_since_last,
                    },
                )
                return True
        except (ValueError, TypeError) as e:
            logger.warning(
                "Invalid timestamp in strategy state, ignoring cooldown",
                extra={
                    "cluster_id": signals.cluster_id,
                    "strategy_type": current_state.strategy_type,
                    "timestamp": last_decision_ts,
                    "error": str(e),
                },
            )
            # Reset the invalid timestamp
            current_state.payload["last_decision_ts"] = None

        return False
