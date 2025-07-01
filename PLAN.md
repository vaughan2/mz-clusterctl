# External Cluster‑Scheduling Controller for Materialize

## 1. Scope & Assumptions

| Item                | Decision                                                                 |
| ------------------- | ------------------------------------------------------------------------ |
| Execution model     | **Stateless CLI**; invoked on‑demand (human or cron)                     |
| Target env.         | Local workstation first; later k8s CronJob                               |
| Materialize version | Version‑locked to the current cluster                                    |
| Concurrency         | Assume **single instance**; no leader‑election                           |
| Signals in v0       | *Recent activity*, *hydration status*, static *strategy config*          |
| Safety              | Best‑effort; no cross‑statement transactions; failures abort current run |
| Permissions         | Super‑user connection via `DATABASE_URL` in `.env`                       |
| Extensibility       | No plug‑in model yet; single codebase                                    |

## 2. Database Artifacts

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

*All three live in the user’s default database for now; migration = “drop & recreate” on version bumps.*

## 3. CLI Surface

```
mz‑schedctl plan        # read-only dry‑run (prints SQL actions)
mz‑schedctl apply       # executes actions, writes audit log
mz‑schedctl wipe-state  # optional helper to clear mz_cluster_strategy_state
Common flags:
  --cluster <name-regex>   # limit to subset
  --verbose
  --postgres-url $DATABASE_URL  (fallback to env/.env)
```

## 4. Python Package Layout

```
src/mz_schedctl/
 ├─ __main__.py          # arg‑parse, mode dispatch
 ├─ db.py                # thin psycopg pool + helpers
 ├─ models.py            # @dataclass StrategyState, ReplicaSpec, etc.
 ├─ signals.py           # queries to recent_activity, hydration tables
 ├─ strategies/
 │    ├─ base.py         # Strategy interface: decide(curr_state, signals) -> Action[]
 │    ├─ burst.py        # reference implementation
 │    └─ idle_suspend.py
 ├─ engine.py            # orchestration: load cfg → run strategies → merge → render SQL
 ├─ executor.py          # optional execution (apply)
 └─ log.py               # structured logging → stdout + mz_cluster_strategy_actions
```

*No dynamic plug‑ins:* `strategies/__init__.py` exports a registry dict `{strategy_type: StrategyClass}`.

## 5. Decision Cycle (per invocation)

1. **Bootstrap**

   * Load rows from `mz_cluster_strategies`.
   * For each cluster:

     * Fetch cluster + replica metadata (`SHOW CLUSTERS`, `SHOW CLUSTER REPLICAS`).
     * Fetch recent‑activity and hydration signals (helpers in `signals.py`).

2. **State Hydration**

   * Pull prior row from `mz_cluster_strategy_state` ➜ deserialize to `StrategyState`.
   * If missing or `state_version` mismatch → start fresh.

3. **Run Strategies**

   * `strategy_cls.decide(current_state, config, signals) -> List[Action]`

     * *Action* = dataclass (`sql:str`, `reason:str`, `expected_state_delta`).
   * Strategies run **independently**; engine merges by simple priority (enum in code).

4. **Plan vs. Apply**

   * **plan**: pretty‑print ordered SQL with reasons.
   * **apply**: in sequence

     * `EXECUTE` each `action.sql` (autocommit).
     * Insert a row into `mz_cluster_strategy_actions`.
     * On first error → abort remaining actions but finish audit row (executed=false, error).

5. **Persist State**

   * Call `strategy_cls.next_state(...)` to compute new `StrategyState`; upsert row.

## 6. Reference Strategy – “Burst”

> *Illustrative only; real parameters come from `config` JSON.*

| Config key              | Description                         | Example |
| ----------------------- | ----------------------------------- | ------- |
| `max_replicas`          | hard ceiling                        | `3`     |
| `scale_up_threshold_ms` | inactivity ≤ this ⇒ add one replica | `500`   |
| `cooldown_s`            | min seconds between decisions       | `120`   |
| `idle_after_s`          | drop to 0 replicas if idle for N s  | `900`   |

Algorithm sketch (inside `burst.decide`):

1. If last decision < `cooldown_s` → return \[].
2. Read `signals.last_activity_ts`.
3. If `now – last_activity > idle_after_s` ⇒ emit `DROP CLUSTER REPLICA ...`.
4. Else if `workload latency > scale_up_threshold` and replicas < max ⇒ emit `CREATE REPLICA ...`.
5. Else no action.

State stored: `{"last_decision_ts": "...", "cooldown_s": 120}`.

## 7. Error Handling & Observability

* **Logging**: Python `structlog`; always include `cluster_id`, `strategy`, `action_id`.
* **Audit Table**: single source for “what happened & why”.
* **Metrics**: skipped in v0 (can be added via Prom‑client later).

## 8. Local Development Workflow

1. `uv sync` (installs dependencies and sets up development environment).
2. `cp .env.example .env` ➜ adjust `DATABASE_URL`.
3. `uv run mz-schedctl plan --verbose` or `uv run python -m mz_schedctl plan --verbose`.
4. Verify SQL, then `uv run mz-schedctl apply` or `uv run python -m mz_schedctl apply`.

## 9. Next‑Steps / Open Items

* Finish SQL queries for **activity** & **hydration** signals.
* Flesh out at least two concrete strategy classes (“idle\_shutdown”, “burst”).
* Add unit tests with a local Materialize instance or test‑containers.
* Prepare a follow‑up story for k8s CronJob & GitHub Action wrappers when prototype is validated.

**Hand‑off**: This document + the minimal code skeleton above are sufficient for an engineer to clone, fill in the details, and run against a sandbox Materialize environment.

