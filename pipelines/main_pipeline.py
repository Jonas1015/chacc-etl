# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

import luigi
from datetime import datetime
from tasks.dynamic_task_factory import create_dynamic_tasks
from services.progress_service import initialize_progress_tracking

_dynamic_tasks = create_dynamic_tasks()

for task_name, task_class in _dynamic_tasks.items():
    globals()[task_name] = task_class
from utils import setup_logging

setup_logging()

def initialize_pipeline_progress(pipeline_type):
    """Initialize progress tracking for the pipeline."""
    from web_ui import socketio
    initialize_progress_tracking(socketio, pipeline_type)

class DatabaseMigrationPipeline(luigi.Task):
    """
    Main ETL pipeline for migrating Source data to analytics database with flattened tables.
    """
    incremental = luigi.BoolParameter(default=False)
    last_updated = luigi.DateParameter(default=None)

    def requires(self):
        required_tasks = []
        for task_class in _dynamic_tasks.values():
            try:
                if hasattr(task_class, 'incremental'):
                    required_tasks.append(task_class(incremental=self.incremental, last_updated=self.last_updated))
                else:
                    required_tasks.append(task_class())
            except TypeError:
                required_tasks.append(task_class())
        return required_tasks

    def output(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        mode = 'incremental' if self.incremental else 'full'
        return luigi.LocalTarget(f'/tmp/db_migration_pipeline_{mode}_{timestamp}_complete.txt')

    def run(self):
        mode = 'incremental' if self.incremental else 'full'
        with self.output().open('w') as f:
            f.write(f'Database migration pipeline ({mode}) completed successfully at {datetime.now()}\n')

class ScheduledIncrementalPipeline(DatabaseMigrationPipeline):
    """
    Scheduled incremental pipeline that runs periodically to update analytics database.
    """
    def __init__(self, *args, **kwargs):
        kwargs['incremental'] = True
        super().__init__(*args, **kwargs)

class FullRefreshPipeline(DatabaseMigrationPipeline):
    """
    Full refresh pipeline that rebuilds all tables from scratch.
    """
    def __init__(self, *args, **kwargs):
        kwargs['incremental'] = False
        super().__init__(*args, **kwargs)
