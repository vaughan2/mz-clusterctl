# mz-clusterctl

External cluster controller for Materialize. A Python CLI tool that manages
Materialize cluster replicas based on configurable strategies like auto-scaling
and idle shutdown.

## Quick Start

### Setup

Create a `.env` file in the project root with your database connection:

```bash
# For local Materialize instance
DATABASE_URL=postgresql://materialize@localhost:6875/materialize

# For Materialize Cloud instance
DATABASE_URL=postgresql:<materialize connection string>
```

### Running

```bash
# Install dependencies
uv sync

# Run dry-run to see planned actions
uv run mz-clusterctl dry-run

# Execute the actions
uv run mz-clusterctl apply

# Format and lint
uv run ruff format && uv run ruff check
```

You might want to run `apply` in a loop, like this:

```bash
while true; sleep 1; uv run mz-clusterctl apply; end;
```

## Architecture

Stateless CLI execution model with three main commands:
- `dry-run` - read-only dry-run showing SQL actions
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
