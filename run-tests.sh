#!/bin/bash
set -e

# Script to run mz-clusterctl tests easily

echo "🚀 Starting mz-clusterctl test suite"

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
uv sync --group dev --group test

# Start Materialize Emulator
echo "🐳 Starting Materialize Emulator..."
docker compose -f docker-compose.test.yml up -d

# Wait for Materialize to be ready
echo "⏳ Waiting for Materialize to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if docker compose -f docker-compose.test.yml exec -T materialize pg_isready -h localhost -p 6875 -U materialize >/dev/null 2>&1; then
        echo "✅ Materialize is ready!"
        break
    fi
    echo "   Waiting... ($timeout seconds remaining)"
    sleep 2
    timeout=$((timeout-2))
done

if [ $timeout -le 0 ]; then
    echo "❌ Timeout waiting for Materialize to be ready"
    docker compose -f docker-compose.test.yml logs materialize
    exit 1
fi

# Set environment variable
export MATERIALIZE_URL="postgres://materialize@localhost:6875/materialize"

# Run tests
echo "🧪 Running tests..."
if [ "$#" -eq 0 ]; then
    # Run all tests by default
    uv run pytest tests/ -v
else
    # Pass through any arguments to pytest
    uv run pytest "$@"
fi

# Store the exit code
test_exit_code=$?

# Clean up
echo "🧹 Cleaning up..."
docker compose -f docker-compose.test.yml down -v

if [ $test_exit_code -eq 0 ]; then
    echo "🎉 All tests passed!"
else
    echo "❌ Some tests failed"
    exit $test_exit_code
fi