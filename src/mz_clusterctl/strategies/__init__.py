from .burst import BurstStrategy
from .idle_suspend import IdleSuspendStrategy
from .shrink_to_fit import ShrinkToFitStrategy
from .target_size import TargetSizeStrategy

STRATEGY_REGISTRY = {
    "burst": BurstStrategy,
    "idle_suspend": IdleSuspendStrategy,
    "shrink_to_fit": ShrinkToFitStrategy,
    "target_size": TargetSizeStrategy,
}
