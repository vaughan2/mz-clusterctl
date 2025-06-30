#!/usr/bin/env python3
"""
mz-schedctl: External cluster-scheduling controller for Materialize

CLI interface for managing Materialize cluster replicas based on configurable strategies.
"""

import argparse
import os
import sys
from typing import Optional

from dotenv import load_dotenv

from .engine import Engine
from .log import setup_logging


def main():
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        prog='mz-schedctl',
        description='External cluster-scheduling controller for Materialize'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Common arguments
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        '--cluster',
        type=str,
        help='Limit to clusters matching this name regex'
    )
    common_parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    common_parser.add_argument(
        '--postgres-url',
        type=str,
        help='PostgreSQL connection URL (defaults to DATABASE_URL env var)'
    )
    
    # plan command
    plan_parser = subparsers.add_parser(
        'plan',
        parents=[common_parser],
        help='Read-only dry-run (prints SQL actions)'
    )
    
    # apply command
    apply_parser = subparsers.add_parser(
        'apply',
        parents=[common_parser],
        help='Execute actions and write audit log'
    )
    
    # wipe-state command
    wipe_parser = subparsers.add_parser(
        'wipe-state',
        parents=[common_parser],
        help='Clear mz_cluster_strategy_state table'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Set up logging
    setup_logging(verbose=args.verbose)
    
    # Get database URL
    database_url = args.postgres_url or os.getenv('DATABASE_URL')
    if not database_url:
        print("Error: DATABASE_URL environment variable or --postgres-url required", file=sys.stderr)
        sys.exit(1)
    
    # Initialize engine
    engine = Engine(database_url=database_url, cluster_filter=args.cluster)
    
    try:
        if args.command == 'plan':
            engine.plan()
        elif args.command == 'apply':
            engine.apply()
        elif args.command == 'wipe-state':
            engine.wipe_state()
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()