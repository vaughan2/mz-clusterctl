"""
Constants for mz-clusterctl

Centralized definition of magic numbers and strings used throughout the project.
"""

# Database configuration
DEFAULT_POOL_MIN_SIZE = 1
DEFAULT_POOL_MAX_SIZE = 10

# Default crash detection thresholds
DEFAULT_MIN_OOM_COUNT = 1
DEFAULT_MIN_CRASH_COUNT = 1

# Default crash info lookback period (hours)
DEFAULT_CRASH_LOOKBACK_HOURS = 1

# Strategy types
STRATEGY_TYPE_BURST = "burst"
STRATEGY_TYPE_IDLE_SUSPEND = "idle_suspend"
STRATEGY_TYPE_SHRINK_TO_FIT = "shrink_to_fit"
STRATEGY_TYPE_TARGET_SIZE = "target_size"

# Table names
TABLE_CLUSTER_STRATEGIES = "mz_cluster_strategies"
TABLE_CLUSTER_STRATEGY_STATE = "mz_cluster_strategy_state"
TABLE_CLUSTER_STRATEGY_ACTIONS = "mz_cluster_strategy_actions"

# State version
CURRENT_STATE_VERSION = 1
