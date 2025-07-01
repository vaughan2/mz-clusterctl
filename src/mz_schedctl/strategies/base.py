"""
Base strategy interface for mz-schedctl

Defines the Strategy abstract base class that all scaling strategies must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

from ..models import Action, ClusterInfo, Signals, StrategyState


class Strategy(ABC):
    """
    Abstract base class for all scaling strategies

    Strategies implement the core decision logic for cluster scaling.
    Each strategy receives the current state, configuration, and signals,
    then returns a list of actions to be executed.
    """

    CURRENT_STATE_VERSION = 1

    @abstractmethod
    def decide(
        self,
        current_state: StrategyState,
        config: Dict[str, Any],
        signals: Signals,
        cluster_info: ClusterInfo,
    ) -> Tuple[List[Action], StrategyState]:
        """
        Make scaling decisions based on current state and signals

        Args:
            current_state: The current state of this strategy for the cluster
            config: Strategy configuration from mz_cluster_strategies table
            signals: Activity and hydration signals for the cluster
            cluster_info: Information about the cluster including current replicas

        Returns:
            Tuple of (actions to be executed, new strategy state to be persisted)
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

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate strategy configuration

        Args:
            config: Configuration dictionary to validate

        Raises:
            ValueError: If configuration is invalid
        """
        # Default implementation does no validation
        # Subclasses should override to add validation
        pass
