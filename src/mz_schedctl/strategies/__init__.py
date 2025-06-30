from .burst import BurstStrategy
from .idle_shutdown import IdleShutdownStrategy

STRATEGY_REGISTRY = {
    'burst': BurstStrategy,
    'idle_shutdown': IdleShutdownStrategy,
}