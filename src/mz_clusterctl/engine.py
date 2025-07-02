"""
Orchestration engine for mz-clusterctl

Coordinates the decision cycle: loading configurations, running strategies, and executing actions.
"""

from typing import Dict, List, Optional

from .db import Database
from .executor import Executor
from .log import get_logger
from .models import Action, ClusterInfo, StrategyConfig, StrategyState
from .signals import get_cluster_signals
from .strategies import STRATEGY_REGISTRY

logger = get_logger(__name__)


class Engine:
    """
    Main orchestration engine for mz-clusterctl

    Coordinates the full decision cycle:
    1. Bootstrap - load cluster and strategy configurations
    2. State hydration - restore previous state
    3. Run strategies - generate actions
    4. Plan/Apply - execute or display actions
    5. Persist state - save updated state
    """

    def __init__(self, database_url: str, cluster_filter: Optional[str] = None):
        self.database_url = database_url
        self.cluster_filter = cluster_filter
        self.db = Database(database_url)
        self.executor = Executor(self.db)

    def __enter__(self):
        self.db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(exc_type, exc_val, exc_tb)

    def plan(self):
        """Execute plan mode - dry run that shows what actions would be taken"""

        with self.db:
            self.db.ensure_tables()
            actions_by_cluster = self._run_decision_cycle(dry_run=True)

            if not any(actions_by_cluster.values()):
                print("No actions would be taken.")
                return

            print("Planned actions:")
            print("=" * 60)

            for cluster_info, actions in actions_by_cluster.items():
                if not actions:
                    continue

                print(f"\nCluster: {cluster_info.name} ({cluster_info.id})")
                print("-" * 40)

                for i, action in enumerate(actions, 1):
                    print(f"{i}. {action.sql}")
                    print(f"   Reason: {action.reason}")
                    if action.expected_state_delta:
                        print(f"   Expected changes: {action.expected_state_delta}")
                    print()

    def apply(self):
        """Execute apply mode - actually execute the actions"""

        with self.db:
            self.db.ensure_tables()
            actions_by_cluster = self._run_decision_cycle(dry_run=False)

            total_actions = sum(len(actions) for actions in actions_by_cluster.values())
            if total_actions == 0:
                print("No actions to execute.")
                return

            print(f"Executing {total_actions} actions...")

            for cluster_info, actions in actions_by_cluster.items():
                if not actions:
                    continue

                print(f"\nProcessing cluster: {cluster_info.name}")
                self.executor.execute_actions(cluster_info.id, actions)

    def wipe_state(self):
        """Clear strategy state table"""
        logger.info("Wiping strategy state")

        with self.db:
            self.db.ensure_tables()
            logger.debug("Database tables ensured, starting wipe state operation")

            if self.cluster_filter:
                # Find clusters matching filter and wipe only those
                clusters = self.db.get_clusters(self.cluster_filter)
                for cluster in clusters:
                    self.db.wipe_strategy_state(cluster.id)
                    print(f"Wiped state for cluster: {cluster.name}")
            else:
                # Wipe all state
                self.db.wipe_strategy_state()
                print("Wiped all strategy state.")

    def _run_decision_cycle(
        self, dry_run: bool = True
    ) -> Dict[ClusterInfo, List[Action]]:
        """
        Run the full decision cycle for all configured clusters

        Returns:
            Dictionary mapping ClusterInfo to list of actions
        """
        logger.trace(
            "Starting decision cycle",
            extra={"dry_run": dry_run, "cluster_filter": self.cluster_filter},
        )

        # 1. Bootstrap - load configurations
        clusters = self.db.get_clusters(self.cluster_filter)

        strategy_configs = self.db.get_strategy_configs()
        logger.info(
            "Strategy configs loaded", extra={"config_count": len(strategy_configs)}
        )

        # Create lookup for strategies by cluster
        config_by_cluster = {config.cluster_id: config for config in strategy_configs}

        actions_by_cluster = {}

        for cluster in clusters:
            actions_by_cluster[cluster] = []

            # Skip clusters without strategy configuration
            if cluster.id not in config_by_cluster:
                logger.debug(
                    "No strategy configured for cluster",
                    extra={"cluster_id": cluster.id, "cluster_name": cluster.name},
                )
                continue

            config = config_by_cluster[cluster.id]

            # Get strategy class
            if config.strategy_type not in STRATEGY_REGISTRY:
                logger.error(
                    "Unknown strategy type",
                    extra={
                        "cluster_id": cluster.id,
                        "strategy_type": config.strategy_type,
                    },
                )
                continue

            strategy_class = STRATEGY_REGISTRY[config.strategy_type]
            strategy = strategy_class()

            try:
                # Validate configuration
                strategy.validate_config(config.config)

                # 2. State hydration
                current_state = self._get_or_create_state(cluster, config, strategy)
                logger.trace(
                    "State hydrated",
                    extra={
                        "cluster_id": cluster.id,
                        "state_version": current_state.state_version,
                        "current_state": current_state,
                    },
                )

                # 3. Get signals
                with self.db.get_connection() as conn:
                    signals = get_cluster_signals(conn, cluster.id, cluster.name)
                logger.info(
                    "Cluster signals retrieved",
                    extra={"cluster_id": cluster.id, "signals": str(signals)},
                )

                # 4. Run strategy
                logger.info(
                    "Running strategy",
                    extra={
                        "cluster_id": cluster.id,
                        "strategy_type": config.strategy_type,
                    },
                )
                actions, new_state = strategy.decide(
                    current_state, config.config, signals, cluster
                )
                actions_by_cluster[cluster] = actions
                logger.debug(
                    "Strategy completed",
                    extra={
                        "cluster_id": cluster.id,
                        "actions_count": len(actions),
                    },
                )

                # 5. Persist state (if not dry run)
                if not dry_run:
                    logger.debug("Persisting state", extra={"cluster_id": cluster.id})
                    # Populate cluster name in state payload
                    new_state.payload["cluster_name"] = cluster.name
                    self.db.upsert_strategy_state(new_state)
                    logger.debug(
                        "State persisted",
                        extra={
                            "cluster_id": cluster.id,
                            "new_state_version": new_state.state_version,
                        },
                    )

                logger.debug(
                    "Processed cluster",
                    extra={
                        "cluster_id": cluster.id,
                        "cluster_name": cluster.name,
                        "strategy_type": config.strategy_type,
                        "actions_generated": len(actions),
                    },
                )

            except Exception as e:
                logger.error(
                    "Error processing cluster",
                    extra={
                        "cluster_id": cluster.id,
                        "cluster_name": cluster.name,
                        "strategy_type": config.strategy_type,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                continue

        return actions_by_cluster

    def _get_or_create_state(
        self, cluster: ClusterInfo, config: StrategyConfig, strategy
    ) -> StrategyState:
        """Get existing state or create initial state for a cluster/strategy"""
        logger.trace(
            "Getting or creating state",
            extra={
                "cluster_id": cluster.id,
                "strategy_type": config.strategy_type,
            },
        )
        existing_state = self.db.get_strategy_state(cluster.id)
        logger.trace(
            "State query completed",
            extra={
                "cluster_id": cluster.id,
                "has_existing_state": existing_state is not None,
            },
        )

        if existing_state is None:
            # No existing state, create initial state
            logger.debug(
                "Creating initial state",
                extra={
                    "cluster_id": cluster.id,
                    "strategy_type": config.strategy_type,
                },
            )
            state = strategy.initial_state(cluster.id, config.strategy_type)
            state.payload["cluster_name"] = cluster.name
            return state

        # Check if existing state is compatible
        if not strategy.is_state_version_compatible(existing_state):
            logger.warning(
                "State version incompatible, resetting to initial state",
                extra={
                    "cluster_id": cluster.id,
                    "existing_version": existing_state.state_version,
                    "expected_version": strategy.CURRENT_STATE_VERSION,
                },
            )
            state = strategy.initial_state(cluster.id, config.strategy_type)
            state.payload["cluster_name"] = cluster.name
            return state

        # Update strategy type in case it changed
        existing_state.strategy_type = config.strategy_type
        existing_state.payload["cluster_name"] = cluster.name

        return existing_state
