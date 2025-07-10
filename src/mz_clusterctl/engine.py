"""
Orchestration engine for mz-clusterctl

Coordinates the decision cycle: loading configurations, running strategies,
and executing actions.
"""

from .coordinator import StrategyCoordinator
from .db import Database
from .environment import get_environment_info
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
    4. Dry-Run/Apply - execute or display actions
    5. Persist state - save updated state
    """

    def __init__(
        self,
        database_url: str,
        cluster_filter: str | None = None,
        replica_sizes_override: list[str] | None = None,
    ):
        self.database_url = database_url
        self.cluster_filter = cluster_filter
        self.replica_sizes_override = replica_sizes_override
        self.db = Database(database_url)
        self.executor = Executor(self.db)
        self.coordinator = StrategyCoordinator()

    def __enter__(self):
        self.db.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.__exit__(exc_type, exc_val, exc_tb)

    def dry_run(self):
        """Execute dry-run mode - dry run that shows what actions would be
        taken"""

        with self.db:
            self.db.ensure_tables()
            actions_by_cluster = self._run_decision_cycle(dry_run=True)

            if not any(actions_by_cluster.values()):
                logger.info("No actions would be taken.")
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
                logger.info("No actions to execute.")
                return

            print(f"Executing {total_actions} actions...")

            for cluster_info, actions in actions_by_cluster.items():
                if not actions:
                    continue

                print(f"\nProcessing cluster: {cluster_info.name}")
                self.executor.execute_actions(cluster_info.id, actions)
                print()

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
    ) -> dict[ClusterInfo, list[Action]]:
        """
        Run the full decision cycle for all configured clusters

        Returns:
            Dictionary mapping ClusterInfo to list of actions
        """

        # 1. Bootstrap - load configurations
        clusters = self.db.get_clusters(self.cluster_filter)

        strategy_configs = self.db.get_strategy_configs()
        logger.info(
            "Strategy configs loaded", extra={"config_count": len(strategy_configs)}
        )

        # Create lookup for strategies by cluster (support multiple strategies
        # per cluster)
        configs_by_cluster = {}
        for config in strategy_configs:
            if config.cluster_id not in configs_by_cluster:
                configs_by_cluster[config.cluster_id] = []
            configs_by_cluster[config.cluster_id].append(config)

        actions_by_cluster = {}

        for cluster in clusters:
            actions_by_cluster[cluster] = []

            # Skip clusters without strategy configuration
            if cluster.id not in configs_by_cluster:
                logger.debug(
                    "No strategy configured for cluster",
                    extra={"cluster_id": cluster.id, "cluster_name": cluster.name},
                )
                continue

            configs = configs_by_cluster[cluster.id]

            # Always use coordinator approach
            actions_by_cluster[cluster] = self._run_strategies(
                cluster, configs, dry_run
            )

        return actions_by_cluster

    def _run_strategies(
        self, cluster: ClusterInfo, configs: list[StrategyConfig], dry_run: bool
    ) -> list[Action]:
        """Run strategies using the coordinator approach"""
        try:
            # Prepare strategies and their configurations
            strategies_and_configs = []
            strategy_states = {}

            # Get signals and environment
            with self.db.get_connection() as conn:
                signals = get_cluster_signals(conn, cluster.id, cluster.name)
                environment = get_environment_info(conn, self.replica_sizes_override)
            logger.info(
                "Cluster signals retrieved",
                extra={"cluster_id": cluster.id, "signals": str(signals)},
            )

            for config in configs:
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

                # Validate configuration
                strategy.validate_config(config.config, environment)

                # Prepare config dict
                strategy_config = config.config.copy()
                strategy_config["strategy_type"] = config.strategy_type

                strategies_and_configs.append((strategy, strategy_config))

                # Get or create state for this strategy
                current_state = self._get_or_create_state(cluster, config, strategy)
                strategy_states[config.strategy_type] = current_state

            if not strategies_and_configs:
                logger.warning(
                    "No valid strategies found for cluster",
                    extra={"cluster_id": cluster.id, "configs_count": len(configs)},
                )
                return []

            logger.info(
                "Running strategies",
                extra={
                    "cluster_id": cluster.id,
                    "strategies_count": len(strategies_and_configs),
                },
            )
            actions, new_states = self.coordinator.coordinate(
                strategies_and_configs, cluster, signals, environment, strategy_states
            )

            # Persist states (if not dry run)
            if not dry_run:
                for strategy_type, new_state in new_states.items():
                    logger.debug(
                        "Persisting state",
                        extra={
                            "cluster_id": cluster.id,
                            "strategy_type": strategy_type,
                        },
                    )
                    new_state.payload["cluster_name"] = cluster.name
                    self.db.upsert_strategy_state(new_state)

            return actions

        except Exception as e:
            logger.error(
                "Error processing strategies",
                extra={
                    "cluster_id": cluster.id,
                    "cluster_name": cluster.name,
                    "configs_count": len(configs),
                    "error": str(e),
                },
                exc_info=True,
            )
            return []

    def _get_or_create_state(
        self, cluster: ClusterInfo, config: StrategyConfig, strategy
    ) -> StrategyState:
        """Get existing state or create initial state for a cluster/strategy"""
        logger.debug(
            "Getting or creating state",
            extra={
                "cluster_id": cluster.id,
                "strategy_type": config.strategy_type,
            },
        )
        existing_state = self.db.get_strategy_state(cluster.id)
        logger.debug(
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
