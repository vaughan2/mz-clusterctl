# mz-schedctl

External cluster-scheduling controller for Materialize. A Python CLI tool that
manages Materialize cluster replicas based on configurable strategies like
auto-scaling and idle shutdown.

## Quick Start

```bash
# Install dependencies
uv sync

# Run dry-run to see planned actions
uv run mz-schedctl plan

# Execute the actions
uv run mz-schedctl apply

# Format and lint
uv run ruff format && uv run ruff check
```

## Architecture

Stateless CLI execution model with three main commands:
- `plan` - read-only dry-run showing SQL actions
- `apply` - executes actions and logs to audit trail
- `wipe-state` - clears controller state

State is managed in Materialize tables (`mz_cluster_strategies`,
`mz_cluster_strategy_state`, `mz_cluster_strategy_actions`). Configuration is
database-driven via JSON in the strategies table.

## Documentation

- **[SPEC.md](SPEC.md)** - Complete technical specification (written for AI
  agents but useful for humans)
- **[CLAUDE.md](CLAUDE.md)** - Development guide and architecture overview
  (written for AI agents but useful for humans)

Both files provide comprehensive details about the system design, database
schema, strategy implementations, and development workflow.
