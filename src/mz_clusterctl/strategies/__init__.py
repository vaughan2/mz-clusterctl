from .burst import BurstStrategy
from .idle_suspend import IdleSuspendStrategy
from .target_size import TargetSizeStrategy

STRATEGY_REGISTRY = {
    "burst": BurstStrategy,
    "idle_suspend": IdleSuspendStrategy,
    "target_size": TargetSizeStrategy,
}
