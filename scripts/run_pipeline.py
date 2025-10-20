# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0


"""
Database Migration Pipeline Runner Script

This script runs the Luigi ETL pipeline for migrating Source data to analytics database with flattened tables.

Usage:
    python scripts/run_pipeline.py [--incremental] [--full-refresh] [--scheduled]

Examples:
    # Full migration (rebuild all tables)
    python scripts/run_pipeline.py --full-refresh

    # Incremental migration (update changed data)
    python scripts/run_pipeline.py --incremental

    # Scheduled incremental run
    python scripts/run_pipeline.py --scheduled

    # Custom incremental with specific last update timestamp
    python scripts/run_pipeline.py --incremental --last-updated 2024-01-01

    # Force full refresh (rerun all tasks)
    python scripts/run_pipeline.py --full-refresh --force
"""

import argparse
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.main_pipeline import DatabaseMigrationPipeline, ScheduledIncrementalPipeline, FullRefreshPipeline
from utils import setup_logging
import luigi

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description='Run Database Migration Pipeline for Source Analytics')
    parser.add_argument('--incremental', action='store_true',
                       help='Run incremental migration (only process changed data)')
    parser.add_argument('--full-refresh', action='store_true',
                       help='Run full refresh migration (rebuild all tables)')
    parser.add_argument('--scheduled', action='store_true',
                       help='Run scheduled incremental migration (optimized for cron jobs)')
    parser.add_argument('--last-updated', type=str,
                       help='Last update timestamp for incremental mode (YYYY-MM-DD format)')
    parser.add_argument('--workers', type=int, default=1,
                       help='Number of Luigi workers')
    parser.add_argument('--local-scheduler', action='store_true', default=True,
                       help='Use local scheduler instead of central scheduler (default: True)')
    parser.add_argument('--central-scheduler', action='store_true',
                       help='Force use of central scheduler (overrides local-scheduler)')
    parser.add_argument('--force', action='store_true',
                       help='Force all tasks to run even if outputs exist')

    args = parser.parse_args()

    mode_count = sum([args.incremental, args.full_refresh, args.scheduled])
    if mode_count > 1:
        parser.error("Cannot specify multiple modes (--incremental, --full-refresh, --scheduled)")

    if mode_count == 0:
        args.incremental = True

    last_updated = None
    if args.last_updated:
        try:
            last_updated = datetime.strptime(args.last_updated, '%Y-%m-%d').date()
        except ValueError:
            parser.error("Invalid date format for --last-updated. Use YYYY-MM-DD format.")

    if args.scheduled and not last_updated:
        last_updated = (datetime.now() - timedelta(days=1)).date()

    if args.force:
        import shutil
        from config import DATA_DIR
        if os.path.exists(DATA_DIR):
            shutil.rmtree(DATA_DIR)
        os.makedirs(DATA_DIR, exist_ok=True)

    try:
        if args.full_refresh:
            print("Running full refresh migration...")
            task = FullRefreshPipeline()
        elif args.scheduled:
            print("Running scheduled incremental migration...")
            task = ScheduledIncrementalPipeline(last_updated=last_updated)
        else:
            print("Running incremental migration...")
            task = DatabaseMigrationPipeline(
                incremental=True,
                last_updated=last_updated
            )

        use_local_scheduler = args.local_scheduler and not args.central_scheduler

        success = luigi.build(
            [task],
            workers=args.workers,
            local_scheduler=use_local_scheduler
        )

        if success:
            print("Database migration pipeline completed successfully!")
            return 0
        else:
            print("Database migration pipeline failed!")
            return 1

    except Exception as e:
        print(f"Error running database migration pipeline: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())