# Tests for mz-clusterctl

This directory contains smoke tests for all the mz-clusterctl scheduling
strategies.

## Overview

The tests are designed to verify the basic functionality of each strategy:

- **target_size**: Ensures clusters have replicas of a specific target size
- **idle_suspend**: Suspends cluster replicas after inactivity periods
- **burst**: Creates burst replicas for fast hydration when needed
- **shrink_to_fit**: Creates replicas of all sizes then shrinks to smallest viable

## Running Tests

### Prerequisites

1. **Python 3.12+** and **uv** package manager
2. **Docker** and **Docker Compose** for Materialize Emulator
3. **MATERIALIZE_URL** environment variable

### Setup

1. Install dependencies:
```bash
uv sync --group dev --group test
```

2. Start Materialize Emulator:
```bash
docker compose -f docker-compose.test.yml up -d
```

3. Wait for Materialize to be ready:
```bash
# Check if ready
docker compose -f docker-compose.test.yml exec materialize pg_isready -h localhost -p 6875 -U materialize
```

### Running Tests

```bash
# Set environment variable
export MATERIALIZE_URL="postgres://materialize@localhost:6875/materialize"

# Run all tests
uv run pytest tests/ -v

# Run unit tests only (no database required)
uv run pytest tests/ -v -m "not integration"

# Run integration tests only (requires database)
uv run pytest tests/ -v -m integration

# Run tests for specific strategy
uv run pytest tests/test_target_size_strategy.py -v

# Run with coverage
uv run pytest tests/ -v --cov=mz_clusterctl
```

### Cleanup

```bash
docker compose -f docker-compose.test.yml down -v
```

## Test Structure

Each strategy test file contains:

- **Unit tests**: Test strategy logic without database
- **Integration tests**: Test end-to-end with real Materialize connection
- **Configuration validation tests**: Ensure invalid configs are rejected
- **Edge case tests**: Test cooldowns, empty clusters, etc.

## Fixtures

The `conftest.py` file provides shared fixtures:

- `materialize_url`: Database connection URL from environment
- `db_connection`: Database connection for session
- `test_cluster_*`: Test cluster data and utilities
- `environment`: Mock environment with replica sizes
- `*_signals`: Various signal scenarios for testing
- `clean_test_tables`: Automatic test data cleanup

## Integration Tests

Integration tests marked with `@pytest.mark.integration` use a real Materialize database to:

1. Create test clusters with replicas
2. Insert strategy configurations
3. Execute strategy decision logic
4. Verify expected database state changes

These tests ensure strategies work correctly with the actual Materialize schema.

## Nightly Testing

A GitHub Action runs these tests nightly, but only if there have been code changes since the last successful run. The workflow:

1. Checks for changes in `src/`, `tests/`, `pyproject.toml`, or `docker-compose.test.yml`
2. Starts Materialize Emulator
3. Runs all tests including integration tests
4. Creates GitHub issues on failure
5. Uploads test artifacts on failure

## Troubleshooting

### Materialize Connection Issues

```bash
# Check if Materialize is running
docker compose -f docker-compose.test.yml ps

# Check logs
docker compose -f docker-compose.test.yml logs materialize

# Test connection manually
psql postgres://materialize@localhost:6875/materialize -c "SELECT 1"
```

### Test Database Cleanup

The tests automatically clean up their database tables, but if tests fail unexpectedly:

```bash
# Connect to Materialize and drop test tables
psql postgres://materialize@localhost:6875/materialize -c "
DROP TABLE IF EXISTS mz_cluster_strategies CASCADE;
DROP TABLE IF EXISTS mz_cluster_strategy_state CASCADE;
DROP TABLE IF EXISTS mz_cluster_strategy_actions CASCADE;
"
```

### Port Conflicts

If port 6875 is already in use:

```bash
# Check what's using the port
lsof -i :6875

# Stop other Materialize instances
docker stop $(docker ps -q --filter ancestor=materialize/materialized)
```
