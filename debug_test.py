#!/usr/bin/env python3
"""
Debug test script to demonstrate the enhanced logging in mz-clusterctl.

This script shows how to use the enhanced debugging features to track down
the "there is no parameter $1" error.
"""

import os
import sys

# Add the source directory to Python path
sys.path.insert(0, "/Users/aljoscha/Dev/mz-schedctl/src")

from mz_clusterctl.log import setup_logging
from mz_clusterctl.engine import Engine


def test_debug_logging():
    """Test the debug logging with verbose output"""

    # Force verbose logging
    setup_logging(verbose=True)

    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("Error: DATABASE_URL environment variable required")
        sys.exit(1)

    print("Starting debug test with enhanced logging...")
    print("=" * 60)

    try:
        # Initialize engine
        engine = Engine(database_url=database_url)

        # Run plan mode to trigger the error
        print("Running plan mode to trigger database operations...")
        engine.plan()

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_debug_logging()
