#!/usr/bin/env python3
"""
mz-clusterctl: External cluster controller for Materialize

CLI interface for managing Materialize cluster replicas based on configurable
strategies.
"""

import argparse
import os
import sys

from dotenv import load_dotenv

from .engine import Engine
from .log import setup_logging


def main():
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="mz-clusterctl",
        description="External cluster controller for Materialize",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Common arguments
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--filter-clusters", type=str, help="Limit to clusters matching this name regex"
    )
    common_parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Enable verbose logging (-v for info, -vv for debug)",
    )
    common_parser.add_argument(
        "--postgres-url",
        type=str,
        help="PostgreSQL connection URL (defaults to DATABASE_URL env var)",
    )
    common_parser.add_argument(
        "--replica-sizes",
        type=str,
        help="Comma-separated list of replica sizes to use for local testing "
        "(e.g., '1,2,4,8'). Normally replica sizes are retrieved from Materialize "
        "itself.",
    )
    common_parser.add_argument(
        "--enable-experimental-strategies",
        action="store_true",
        help="Enable experimental strategies (e.g., shrink_to_fit)",
    )
    common_parser.add_argument(
        "--cluster",
        type=str,
        help="Cluster to use for executing commands (executes SET cluster = <cluster>)",
    )
    common_parser.add_argument(
        "--create-replica",
        nargs="?",
        const="25cc",
        metavar="SIZE",
        help="Create a temporary replica for the cluster with optional size (default: 25cc, requires --cluster)",
    )

    # dry-run command
    _ = subparsers.add_parser(
        "dry-run",
        parents=[common_parser],
        help="Read-only dry-run (prints SQL actions)",
    )

    # apply command
    _ = subparsers.add_parser(
        "apply", parents=[common_parser], help="Execute actions and write audit log"
    )

    # wipe-state command
    _ = subparsers.add_parser(
        "wipe-state",
        parents=[common_parser],
        help="Clear mz_cluster_strategy_state table",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Validate --create-replica requires --cluster
    if args.create_replica is not None and not args.cluster:
        print(
            "Error: --create-replica requires --cluster to be specified",
            file=sys.stderr,
        )
        sys.exit(1)

    # Set up logging
    setup_logging(verbose=args.verbose)

    # Get database URL
    database_url = args.postgres_url or os.getenv("DATABASE_URL")
    if not database_url:
        print(
            "Error: DATABASE_URL environment variable or --postgres-url required",
            file=sys.stderr,
        )
        sys.exit(1)

    # Parse replica sizes override if provided
    replica_sizes_override = None
    if args.replica_sizes:
        replica_sizes_override = [
            size.strip() for size in args.replica_sizes.split(",")
        ]
        if not replica_sizes_override:
            print("Error: --replica-sizes cannot be empty", file=sys.stderr)
            sys.exit(1)

    # Initialize engine
    engine = Engine(
        database_url=database_url,
        cluster_filter=args.filter_clusters,
        replica_sizes_override=replica_sizes_override,
        enable_experimental_strategies=args.enable_experimental_strategies,
        cluster=args.cluster,
        create_replica=args.create_replica is not None,
        create_replica_size=args.create_replica,
    )

    try:
        with engine:
            if args.command == "dry-run":
                engine.dry_run()
            elif args.command == "apply":
                engine.apply()
            elif args.command == "wipe-state":
                engine.wipe_state()
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
