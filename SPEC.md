# External Cluster‑Scheduling Controller for Materialize

## 1. Scope & Assumptions

| Item                | Decision                                                                 |
| ------------------- | ------------------------------------------------------------------------ |
| Execution model     | **Stateless CLI**; invoked on‑demand (human or cron)                     |
| Target env.         | Local workstation first; later k8s or CronJob                            |
| Materialize version | Version‑locked to the current cluster                                    |
| Concurrency         | Assume **single instance**; no leader‑election                           |
| Signals in v0       | *Recent activity*, *hydration status*, static *strategy config*          |
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
mz‑schedctl plan        # read-only dry‑run (prints SQL actions)
mz‑schedctl apply       # executes actions, writes audit log
mz‑schedctl wipe-state  # optional helper to clear mz_cluster_strategy_state
Common flags:
  --cluster <name-regex>   # limit to subset of clusters
  --verbose/-v             # debug logging (-v for debug, -vv for trace)
  --postgres-url           # PostgreSQL connection URL (overrides DATABASE_URL env var)
```

## 4. Python Package Layout

```
mz_schedctl/
 ├─ __main__.py          # CLI entry point with argparse and mode dispatch
 ├─ db.py                # PostgreSQL connection pool + database helpers
 ├─ models.py            # @dataclass StrategyState, ReplicaSpec, Action, etc.
 ├─ signals.py           # queries for activity, hydration, and cluster metrics
 ├─ strategies/
 │    ├─ __init__.py     # strategy registry: STRATEGY_REGISTRY dict
 │    ├─ base.py         # Strategy interface: decide(state, signals) -> (Action[], new_state)
 │    ├─ target_size.py  # aka. 0dt reconfiguration
 │    ├─ burst.py        # auto-scaling strategy with cooldown and idle shutdown
 │    └─ idle_suspend.py # idle cluster suspend/resume strategy
 ├─ engine.py            # orchestration: load config → run strategies → plan/apply → persist
 ├─ executor.py          # SQL execution engine with audit logging
 └─ log.py               # structured logging with structlog + verbosity levels
```

*No dynamic plug‑ins:* `strategies/__init__.py` exports `STRATEGY_REGISTRY` dict mapping strategy names to classes.

## 5. Decision Cycle (per invocation)

1. **Bootstrap**

   * Load rows from `mz_cluster_strategies`.
   * For each cluster:

     * Fetch cluster + replica metadata (`SHOW CLUSTERS`, `SHOW CLUSTER REPLICAS`).
     * Fetch recent‑activity and hydration signals (helpers in `signals.py`).

2. **State Hydration**

   * Pull prior row from `mz_cluster_strategy_state` ➜ deserialize to `StrategyState`.
   * If missing or `state_version` mismatch → start fresh.

3. **Run Strategies**

   * `strategy_cls.decide(current_state, config, signals, cluster_info) -> (List[Action], new_state)`

     * *Action* = dataclass (`sql:str`, `reason:str`, `cluster_id:str`).
   * Strategies run **independently**; engine coordinates execution and state updates.

4. **Plan vs. Apply**

   * **plan**: pretty‑print ordered SQL with reasons.
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
| `target_size`   | Target replica size (required)                 | `"medium"`     |
| `replica_name`  | Name for target size replica (optional)       | `"r_medium"`   |

Algorithm sketch (inside `target_size.decide`):

1. Check for existing replicas matching the target size.
2. If no target size replica exists → emit `CREATE CLUSTER REPLICA ...` with target size.
3. If target size replica exists and is hydrated + other size replicas exist → emit `DROP CLUSTER REPLICA ...` for non-target replicas.
4. Track pending replica creation in state to handle async replica creation.
5. Clear pending state when target replica is confirmed to exist.

State stored: `{"last_decision_ts": "...", "pending_target_replica": {"name": "...", "size": "...", "created_at": "..."}}`.

## 7. Error Handling & Observability

* **Logging**: Python `structlog` with configurable verbosity levels (INFO, DEBUG, TRACE).
* **Audit Table**: `mz_cluster_strategy_actions` provides complete audit trail.
* **Error Handling**: Fail-fast approach with detailed error logging and rollback capability.
* **State Versioning**: Handles schema evolution with version compatibility checks.

## 8. Local Development Workflow

1. `uv sync` (installs dependencies and sets up development environment).
2. `cp .env.example .env` ➜ adjust `DATABASE_URL`.
3. `uv run mz-schedctl plan --verbose`
4. Verify SQL, then `uv run mz-schedctl apply`
