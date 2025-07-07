# mz-clusterctl

mz-clusterctl is an external cluster controller for Materialize that
automatically manages cluster replicas based on configurable scaling
strategies. It monitors cluster activity, replica hydration status, and
resource utilization to make intelligent decisions about scaling up, scaling
down, or suspending replicas to optimize performance and cost.

The tool operates as a stateless CLI that can be run periodically (e.g., via
cron or Github Actions) to continuously manage your Materialize clusters
according to your scaling requirements.

## Setup

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Configure database connection:**
   Create a `.env` file with your Materialize connection string:
   ```bash
   # For local Materialize instance
   DATABASE_URL=postgresql://materialize@localhost:6875/materialize
   
   # For Materialize Cloud instance
   DATABASE_URL=postgresql:<your-materialize-connection-string>
   ```

3. **Initialize the controller:**
   ```bash
   # This creates the necessary tables
   uv run mz-clusterctl dry-run
   ```

## User Interface

### Configuring Strategies

Strategies are configured by inserting records into the `mz_cluster_strategies`
table in your Materialize database. Each strategy is defined by:

- **cluster_id**: The ID of the cluster to manage
- **strategy_type**: The type of strategy (e.g., "target_size", "burst", "idle_suspend")
- **config**: A JSON object containing strategy-specific parameters

**Basic example:**
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES
  ('u1', 'target_size', '{"target_size": "200cc"}');
```

**Advanced multi-strategy example:**
```sql
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES
  ('u1', 'target_size', '{"target_size": "50cc"}'),
  ('u1', 'burst', '{"burst_replica_size": "800cc", "cooldown_s": 60}'),
  ('u1', 'idle_suspend', '{"idle_after_s": 1800}');
```

### Strategy Execution Order

When multiple strategies are configured for the same cluster, they execute in
priority order:

1. `shrink_to_fit` (priority 1)
2. `target_size` (priority 1)
3. `burst` (priority 2)
4. `idle_suspend` (priority 3)

Higher priority strategies can modify or override decisions from lower priority strategies.

### Managing Strategies

**View current strategies:**
```sql
SELECT * FROM mz_cluster_strategies;
```

**Update a strategy:**
```sql
UPDATE mz_cluster_strategies
  SET config = '{"target_size": "large"}'
  WHERE cluster_id = 'u1' AND strategy_type = 'target_size';
```

**Remove a strategy:**
```sql
DELETE FROM mz_cluster_strategies 
WHERE cluster_id = 'u1' AND strategy_type = 'idle_suspend';
```

## Strategies

### target_size

Ensures a cluster has exactly one replica of a specified size.

**Configuration:**
- `target_size` (required): The desired replica size (e.g., "25cc", "100cc", "200cc", "800cc")
- `replica_name` (optional): Custom name for the replica (default: "r_{target_size}")

**Example:**
```json
{
  "target_size": "200cc",
  "replica_name": "main_replica"
}
```

**Use case:** Maintain a consistent baseline replica size.

### burst

Adds large "burst" replicas when existing replicas are not hydrated (not ready
to serve queries), and removes them when other replicas become hydrated.

**Configuration:**
- `burst_replica_size` (required): Size of the burst replica (e.g., "800cc", "1600cc")
- `cooldown_s` (optional): Cooldown period in seconds between decisions (default: 0)

**Example:**
```json
{
  "burst_replica_size": "800cc",
  "cooldown_s": 60
}
```

**Use case:** Temporarily scale up during high-demand periods or when primary
replicas are starting up.

### idle_suspend

Suspends all cluster replicas after a configured period of inactivity.

**Configuration:**
- `idle_after_s` (required): Number of seconds of inactivity before suspending replicas
- `cooldown_s` (optional): Cooldown period in seconds between decisions (default: 0)

**Example:**
```json
{
  "idle_after_s": 1800,
  "cooldown_s": 300
}
```

**Use case:** Reduce costs by automatically suspending unused clusters.

### shrink_to_fit

Creates replicas of multiple sizes, then removes larger ones when smaller ones
can handle the workload.

**Configuration:**
- `max_replica_size` (required): Maximum replica size to create (for example, "800cc" or "1600cc")
- `cooldown_s` (optional): Cooldown period in seconds between decisions (default: 0)
- `min_oom_count` (optional): Minimum OOM count to consider a replica crash-looping (default: 1)
- `min_crash_count` (optional): Minimum crash count to consider a replica crash-looping (default: 1)

**Example:**
```json
{
  "max_replica_size": "1600cc",
  "cooldown_s": 120,
}
```

**Use case:** Optimize replica sizes based on actual resource requirements.

## Operations

### Environment Configuration

The tool requires a PostgreSQL connection to your Materialize instance:

```bash
# Set environment variable
export DATABASE_URL=postgresql://materialize@localhost:6875/materialize

# Or use a .env file
echo "DATABASE_URL=postgresql://materialize@localhost:6875/materialize" > .env
```

### Running the Controller

**Dry-run mode** (view planned actions without executing):
```bash
uv run mz-clusterctl dry-run
```

**Apply mode** (execute actions):
```bash
uv run mz-clusterctl apply
```

**Verbose output** (for debugging):
```bash
uv run mz-clusterctl dry-run --verbose
```

**Target specific clusters**:
```bash
uv run mz-clusterctl apply --cluster "production-.*"
```

### Periodic Operation

The tool is designed to run periodically. A typical setup might run it every 1-5 minutes:

**Using cron:**
```bash
# Run every 2 minutes
*/2 * * * * cd /path/to/mz-clusterctl && uv run mz-clusterctl apply
```

**Using a simple loop:**
```bash
while true; do
    uv run mz-clusterctl apply
    sleep 60
done
```

### Monitoring

**View audit trail:**
```sql
SELECT * FROM mz_cluster_strategy_actions ORDER BY created_at DESC LIMIT 10;
```

**View current state:**
```sql
SELECT * FROM mz_cluster_strategy_state;
```

**View failed actions:**
```sql
SELECT * FROM mz_cluster_strategy_actions WHERE executed = false;
```

### State Management

**Clear state for debugging:**
```bash
uv run mz-clusterctl wipe-state
```

**Clear state for specific cluster:**
```bash
uv run mz-clusterctl wipe-state --cluster "problematic-cluster"
```

## Quirks

### Reaction Time Limitations

The controller can only react to changes at the interval it is run. This means:

- **Delayed Response**: If you run the controller every 5 minutes, it may take
  up to 5 minutes to react to changes in cluster activity or replica status.
- **Idle Suspend Timing**: The `idle_suspend` strategy cannot immediately spin
  up replicas when activity resumes. It will only detect activity and create
  replicas on the next execution cycle.
- **Burst Scaling**: The `burst` strategy may not immediately respond to
  replica hydration changes, potentially keeping burst replicas active longer
  than necessary.

### Non-Atomic Operations

Changes to controller state and cluster replicas are not atomic. This differs
from what might be expected if the scheduling functionality were integrated
directly into Materialize:

- **State Persistence**: Controller state is persisted to the database
  separately from cluster replica changes.
- **Failure Recovery**: If the controller fails after updating state but before
  creating/destroying replicas, the next execution may result in duplicate
  actions or inconsistent state.
- **Concurrent Execution**: Running multiple instances of the controller
  simultaneously is not supported and may lead to conflicting actions.

### Resource Monitoring Limitations

The controller relies on Materialize's system tables for monitoring:

- **Activity Detection**: Based on `mz_statement_execution_history_redacted`,
  which may not capture all forms of cluster activity.
- **Hydration Status**: Based on `mz_hydration_statuses`, which reflects the
  current state but may not predict future readiness.
- **Crash Detection**: Based on `mz_cluster_replica_status_history`, which may
  have delays in reporting status changes.

### Configuration Persistence

Strategy configurations are stored in the database and persist across
controller restarts. However:

- **No Validation**: Invalid configurations are not validated until execution
  time.
- **JSON Format**: Configuration parameters must be valid JSON and match
  expected schema.
- **Manual Management**: There is no built-in UI for managing strategies; all
  configuration is done via SQL.
