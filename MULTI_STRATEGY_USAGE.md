# Multi-Strategy Usage Guide

## Overview

The Desired State + Coordinator approach allows multiple strategies to run together for a single cluster. Strategies declare their desired state (what replicas should exist), and a coordinator resolves conflicts between them.

## Key Components

1. **DesiredState** - Represents what replicas a strategy wants
2. **StrategyCoordinator** - Combines multiple desired states
3. **ConflictResolver** - Resolves conflicts between strategies 
4. **StateDiffer** - Converts desired state to SQL actions

## Conflict Resolution Modes

- **PRIORITY** - Higher priority strategies win conflicts

## Usage: Multiple Strategy Rows

Insert multiple rows for the same cluster to have multiple strategies active:

```sql
-- Multiple strategies for the same cluster
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'target_size', '{"target_size": "medium"}'),
('cluster-123', 'idle_suspend', '{"idle_after_s": 1800}');
```

The engine automatically detects multiple strategies per cluster and uses the coordinator.

## Example Scenario: Burst + Target Size

This combination provides:
- **Target Size Strategy**: Ensures a baseline "medium" replica exists
- **Burst Strategy**: Adds a large replica when other replicas aren't hydrated

**Behavior:**
1. If no replicas exist → Creates medium replica 
2. If medium replica is creating/not hydrated → Adds large burst replica
3. When medium replica is hydrated → Removes burst replica
4. Maintains medium replica as the steady state

## Example Configuration Files

### Aggressive Auto-scaling
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'target_size', '{"target_size": "small", "replica_name": "baseline"}'),
('cluster-123', 'burst', '{"burst_replica_size": "xlarge", "cooldown_s": 60}'),
('cluster-123', 'idle_suspend', '{"idle_after_s": 3600}');
```

### Conservative Approach
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'idle_suspend', '{"idle_after_s": 1800}'),
('cluster-123', 'target_size', '{"target_size": "medium"}');
```

## Migration from Single Strategy

### Current (Single Strategy)
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) 
VALUES ('cluster-123', 'burst', '{"burst_replica_size": "large"}');
```

### New (Multi-Strategy)
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'burst', '{"burst_replica_size": "large"}'),
('cluster-123', 'target_size', '{"target_size": "medium"}');
```

## Testing

Run the test script to verify the implementation:
```bash
uv run python test_multi_strategy_simple.py
```
