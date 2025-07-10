# External Cluster Controller for Materialize

## 1. Scope & Assumptions

| Item                | Decision                                                                 |
| ------------------- | ------------------------------------------------------------------------ |
| Execution model     | **Stateless CLI**; invoked on‑demand (human or cron)                     |
| Target env.         | Local workstation first; later k8s or CronJob                            |
| Materialize version | Version‑locked to the current cluster                                    |
| Concurrency         | Assume **single instance**; no leader‑election                           |
| Signals in v0       | *Recent activity*, *hydration status*, *crash information*, static *strategy config*          |
| Safety              | Best‑effort; no cross‑statement transactions; failures abort current run |
| Permissions         | Super‑user connection via `DATABASE_URL` in `.env`                       |
| Extensibility       | No plug‑in model yet; single codebase                                    |

## 2. Database Artifacts

```sql
-- 1️⃣ User‑authored strategy definitions (source of truth)
CREATE TABLE IF NOT EXISTS mz_cluster_strategies (
    cluster_id    TEXT            NOT NULL,
    strategy_type TEXT            NOT NULL,
    config        JSONB           NOT NULL,
    updated_at    TIMESTAMPTZ     DEFAULT now()
);

-- 2️⃣ Controller internal state (one row per cluster/strategy)
CREATE TABLE IF NOT EXISTS mz_cluster_strategy_state (
    cluster_id    TEXT            NOT NULL,
    state_version INT             NOT NULL,
    payload       JSONB           NOT NULL,          -- serialized Python dataclass
    updated_at    TIMESTAMPTZ     DEFAULT now()
);

-- 3️⃣ Action log / audit trail
CREATE TABLE IF NOT EXISTS mz_cluster_strategy_actions (
    action_id     TEXT            NOT NULL,
    cluster_id    TEXT,
    action_sql    TEXT,
    decision_ctx  JSONB,
    executed      BOOL,
    error_message TEXT,
    created_at    TIMESTAMPTZ     DEFAULT now()
);
```

*All three live in the user's default database for now; migration = "drop & recreate" on version bumps.*

## 3. CLI Surface

```
mz‑clusterctl dry-run     # read-only dry‑run (prints SQL actions)
mz‑clusterctl apply       # executes actions, writes audit log
mz‑clusterctl wipe-state  # optional helper to clear mz_cluster_strategy_state
Common flags:
  --cluster <name-regex>   # limit to subset of clusters
  --verbose/-v             # debug logging (-v for info, -vv for debug)
  --postgres-url           # PostgreSQL connection URL (overrides DATABASE_URL env var)
  --replica-sizes          # comma-separated list of replica sizes for local testing
```

## 4. Python Package Layout

```
src/mz_clusterctl/
 ├─ __init__.py          # package exports
 ├─ __main__.py          # CLI entry point with argparse and mode dispatch
 ├─ db.py                # PostgreSQL connection pool + database helpers
 ├─ models.py            # @dataclass StrategyState, ReplicaSpec, Action, DesiredState, etc.
 ├─ constants.py         # application constants and defaults
 ├─ signals.py           # queries for activity, hydration, crash info, and cluster metrics
 ├─ environment.py       # environment detection and replica size queries
 ├─ coordinator.py       # multi-strategy coordination: StrategyCoordinator and StateDiffer
 ├─ strategies/
 │    ├─ __init__.py     # strategy registry: STRATEGY_REGISTRY dict
 │    ├─ base.py         # Strategy interface: decide_desired_state()
 │    ├─ target_size.py  # target size strategy implementation
 │    ├─ burst.py        # auto-scaling strategy with cooldown
 │    ├─ shrink_to_fit.py # shrink to fit strategy implementation
 │    └─ idle_suspend.py # idle cluster suspend/resume strategy
 ├─ engine.py            # orchestration: load config → run strategies → dry-run/apply → persist
 ├─ executor.py          # SQL execution engine with audit logging
 └─ log.py               # structured logging with structlog + verbosity levels
```

*No dynamic plug‑ins:* `strategies/__init__.py` exports `STRATEGY_REGISTRY` dict mapping strategy names to classes:
- `target_size`: TargetSizeStrategy (priority 0)
- `shrink_to_fit`: ShrinkToFitStrategy (priority 1)
- `burst`: BurstStrategy (priority 2)
- `idle_suspend`: IdleSuspendStrategy (priority 3)

## 5. Decision Cycle (per invocation)

1. **Bootstrap**

   * Load rows from `mz_cluster_strategies`.
   * Get environment information including available replica sizes.
   * For each cluster:

     * Fetch cluster + replica metadata from catalog tables.
     * Fetch recent‑activity, hydration status, and crash information (helpers in `signals.py`).

2. **State Hydration**

   * Pull prior row from `mz_cluster_strategy_state` ➜ deserialize to `StrategyState`.
   * If missing or `state_version` mismatch → start fresh.

3. **Run Strategies (Desired State + Conflict Resolution)**

   * **StrategyCoordinator** coordinates multiple strategies:
     * Sort strategies by priority (lowest first).
     * Each strategy implements `decide_desired_state(current_state, config, signals, environment, cluster_info, current_desired_state) -> (DesiredState, new_state)`
     * Pass the accumulated desired state from previous strategies to the next strategy.
     * Higher priority strategies can override or modify decisions from lower priority strategies.

   * **DesiredState** = dataclass containing:
     * `target_replicas: dict[str, ReplicaSpec]` - exact replicas that should exist
     * `cluster_id: str` - cluster identifier
     * `strategy_type: str` - strategy that created this state
     * `priority: int` - for conflict resolution
     * `reasons: list[str]` - human-readable explanations for changes
     * `metadata: dict[str, Any]` - strategy-specific data

   * **StateDiffer** converts desired state to actions by comparing with current cluster state.

4. **Dry-Run vs. Apply**

   * **dry-run**: pretty‑print ordered SQL with reasons.
   * **apply**: in sequence

     * `EXECUTE` each `action.sql` (autocommit).
     * Insert a row into `mz_cluster_strategy_actions`.
     * On first error → abort remaining actions but finish audit row (executed=false, error).

5. **Persist State**

   * Save updated `StrategyState` returned by strategy to `mz_cluster_strategy_state` table.

## 6. Reference Strategy – "Target Size"

> *Illustrative only; real parameters come from `config` JSON.*

| Config key      | Description                                    | Example        |
| --------------- | ---------------------------------------------- | -------------- |
| `target_size`   | Target replica size (required)                 | `"200cc"`     |
| `replica_name`  | Name for target size replica (optional)        | `"r_medium"`   |

Algorithm sketch (inside `target_size.decide_desired_state`):

1. Check for existing replicas matching the target size.
2. If no target size replica exists → emit `CREATE CLUSTER REPLICA ...` with target size.
3. If target size replica exists and is hydrated + other size replicas exist → emit `DROP CLUSTER REPLICA ...` for non-target replicas.
4. Track pending replica creation in state to handle async replica creation.
5. Clear pending state when target replica is confirmed to exist.

State stored: `{"last_decision_ts": "...", "pending_target_replica": {"name": "...", "size": "...", "created_at": "..."}}`.

## 7. Reference Strategy – "Shrink to Fit"

> *Creates replicas of all sizes up to a maximum, then drops larger replicas when smaller ones are hydrated.*

| Config key         | Description                                   | Example        |
| ------------------ | ----------------------------------------------| -------------- |
| `max_replica_size` | Maximum replica size to create (required)     | `"800cc"`      |
| `cooldown_s`       | Cooldown period in seconds (optional)         | `60`           |
| `min_oom_count`    | OOM count threshold for replica removal       | `2`            |
| `min_crash_count`  | Crash count threshold for replica removal     | `2`            |

Algorithm sketch (inside `shrink_to_fit.decide_desired_state`):

1. Create replicas of all sizes up to `max_replica_size` if no replicas exist.
2. Remove crash-looping replicas (OOM or general crash loops).
3. Drop larger replicas when smaller replicas become hydrated and healthy.
4. Track replica changes and decisions in state.

State stored: `{"last_decision_ts": "...", "last_scale_action": {"timestamp": "...", "replicas_added": 0, "replicas_removed": 1}}`.

## 8. Error Handling & Observability

* **Logging**: Python `structlog` with configurable verbosity levels (INFO, DEBUG).
* **Audit Table**: `mz_cluster_strategy_actions` provides complete audit trail.
* **Error Handling**: Fail-fast approach with detailed error logging and rollback capability.
* **State Versioning**: Handles schema evolution with version compatibility checks.
* **Crash Detection**: Monitors replica crash loops and OOM conditions.

## 9. Multi-Strategy Configuration

### Overview

The system supports multiple strategies per cluster using the **Desired State + Conflict Resolution** pattern. Strategies declare their desired replica configuration, and conflicts are resolved by priority.

### Configuration

Insert multiple rows in `mz_cluster_strategies` for the same cluster:

```sql
-- Example: Target size baseline + burst scaling + idle suspend
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'target_size', '{"target_size": "200cc"}'),
('cluster-123', 'burst', '{"burst_replica_size": "800cc", "cooldown_s": 60}'),
('cluster-123', 'idle_suspend', '{"idle_after_s": 1800}');

-- Example: Shrink to fit with crash detection
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-456', 'shrink_to_fit', '{"max_replica_size": "800cc", "min_oom_count": 3, "min_crash_count": 5}');
```

### Conflict Resolution

- **PRIORITY**: Higher priority (higher number) strategies override lower priority ones
- Strategies run in priority order (lowest number first):
  - Target Size (priority 0) → Shrink to Fit (priority 1) → Burst (priority 2) → Idle Suspend (priority 3)
- Later strategies receive the accumulated desired state from previous strategies
- Strategies can modify, extend, or completely override previous decisions

### Example Combinations

**Aggressive Auto-scaling:**
```sql
-- Baseline + burst + idle suspend
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'target_size', '{"target_size": "50cc"}'),
('cluster-123', 'burst', '{"burst_replica_size": "xlarge", "cooldown_s": 60}'),
('cluster-123', 'idle_suspend', '{"idle_after_s": 3600}');
```

**Shrink to Fit:**
```sql
-- Automatically find smallest viable replica size
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'shrink_to_fit', '{"max_replica_size": "xlarge", "min_oom_count": 2}');
```

**Conservative Approach:**
```sql
-- Just idle suspend + baseline size
INSERT INTO mz_cluster_strategies (cluster_id, strategy_type, config) VALUES 
('cluster-123', 'target_size', '{"target_size": "200cc"}'),
('cluster-123', 'idle_suspend', '{"idle_after_s": 1800}');
```

### Behavior Example: Target Size + Burst

1. **Target Size Strategy** (priority 0): Declares medium replica should exist
2. **Burst Strategy** (priority 2): Receives desired state with medium replica
   - If medium replica is not hydrated → adds large burst replica
   - If medium replica is hydrated → removes burst replica
3. **Result**: Maintains medium replica as baseline, adds burst when needed

### Behavior Example: Shrink to Fit

1. **Shrink to Fit Strategy** (priority 1): Creates replicas of all sizes up to max_replica_size
2. As smaller replicas become hydrated, drops larger replicas
3. Removes replicas experiencing crash loops (OOM or general crashes)
4. **Result**: Converges to smallest viable replica size for the workload

## 10. Local Development Workflow

1. `uv sync` (installs dependencies and sets up development environment).
2. Set up `DATABASE_URL` environment variable, in .env file
3. `uv run mz-clusterctl dry-run --verbose`
4. Verify SQL, then `uv run mz-clusterctl apply`

### Development Commands

```bash
# Format and lint
uv run ruff format         # Format code
uv run ruff check          # Lint code
uv run ruff check --fix    # Fix auto-fixable lint issues

# Run the CLI
uv run mz-clusterctl dry-run
uv run mz-clusterctl apply
uv run mz-clusterctl wipe-state

# Local testing with custom replica sizes
uv run mz-clusterctl dry-run --replica-sizes "1,2,4,8,16"
```
