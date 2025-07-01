from .burst import BurstStrategy
from .idle_suspend import IdleSuspendStrategy

STRATEGY_REGISTRY = {
    "burst": BurstStrategy,
    "idle_suspend": IdleSuspendStrategy,
}
