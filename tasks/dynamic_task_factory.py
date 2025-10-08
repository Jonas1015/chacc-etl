"""
Dynamic task factory that creates Luigi tasks from SQL files.
Scans the sql/ directory and creates corresponding Luigi task classes.
"""

import os
import glob
import json
import fnmatch

from tasks.flattened_table_tasks import load_task_dependencies

# Store SQL file information
_sql_files_info = {}
_task_dependencies = {}
_dynamic_tasks = {}

def load_task_definitions():
    """Load task definitions from JSON configuration."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'task_definitions.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get('tasks', {}), config.get('task_dependencies', {})
    return {}, {}

def resolve_dependencies(task_name, all_tasks, dependencies_config, patterns_config):
    """Resolve dependencies for a task, including pattern matching."""
    deps = []

    # Check direct dependencies
    if task_name in dependencies_config:
        deps.extend(dependencies_config[task_name])

    # Check pattern dependencies
    for pattern, pattern_deps in patterns_config.items():
        if fnmatch.fnmatch(task_name, pattern):
            deps.extend(pattern_deps)

    # Expand wildcard patterns to actual task names
    expanded_deps = []
    for dep in deps:
        if '*' in dep:
            # Find all tasks that match the pattern
            matching_tasks = [t for t in all_tasks if fnmatch.fnmatch(t, dep)]
            expanded_deps.extend(matching_tasks)
        else:
            expanded_deps.append(dep)

    # Remove duplicates and filter to existing tasks
    return list(set(expanded_deps) & set(all_tasks))

def scan_sql_directory():
    """Scan sql/ directory and collect SQL file information."""
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')

    # Define dependency order
    dependency_order = ['init', 'tables', 'procedures']

    # Collect all SQL files by directory
    sql_files_by_dir = {}
    for dirname in dependency_order:
        dir_path = os.path.join(sql_dir, dirname)
        if os.path.exists(dir_path):
            sql_files = glob.glob(os.path.join(dir_path, '*.sql'))
            sql_files_by_dir[dirname] = sorted(sql_files)

    # Store file information with dependencies
    for dir_type in dependency_order:
        if dir_type not in sql_files_by_dir:
            continue

        sql_files = sql_files_by_dir[dir_type]

        for sql_file in sql_files:
            # Generate task name from file path
            rel_path = os.path.relpath(sql_file, sql_dir)
            task_name = rel_path.replace('/', '_').replace('\\', '_').replace('.sql', '').replace('.', '_')
            task_name = ''.join(word.capitalize() for word in task_name.split('_')) + 'Task'

            # Determine dependencies
            dependencies = []
            if dir_type == 'tables':
                # Tables depend on init tasks
                for init_file in sql_files_by_dir.get('init', []):
                    init_rel_path = os.path.relpath(init_file, sql_dir)
                    init_task_name = init_rel_path.replace('/', '_').replace('\\', '_').replace('.sql', '').replace('.', '_')
                    init_task_name = ''.join(word.capitalize() for word in init_task_name.split('_')) + 'Task'
                    dependencies.append(init_task_name)

                # Add inter-table dependencies based on foreign keys
                table_name = os.path.basename(sql_file).replace('.sql', '')
                if table_name == 'encounters':
                    # Encounters depends on patients
                    dependencies.append('TablesPatientsTask')
                elif table_name == 'observations':
                    # Observations depends on patients, encounters, locations
                    dependencies.extend(['TablesPatientsTask', 'TablesEncountersTask', 'TablesLocationsTask'])
                
            elif dir_type == 'procedures':
                # Procedures depend on tables
                for table_file in sql_files_by_dir.get('tables', []):
                    table_rel_path = os.path.relpath(table_file, sql_dir)
                    table_task_name = table_rel_path.replace('/', '_').replace('\\', '_').replace('.sql', '').replace('.', '_')
                    table_task_name = ''.join(word.capitalize() for word in task_name.split('_')) + 'Task'
                    dependencies.append(table_task_name)

            _sql_files_info[task_name] = {
                'sql_file_path': sql_file,
                'task_name': task_name,
                'dir_type': dir_type,
                'dependencies': dependencies
            }

    return _sql_files_info

_sql_files_info = scan_sql_directory()

def get_sql_files_info():
    """Get information about all SQL files."""
    return _sql_files_info

def get_task_names():
    """Get all task names that will be created."""
    return list(_sql_files_info.keys())

def create_dynamic_tasks():
    """Create Luigi task classes dynamically."""
    import luigi
    from tasks.base_tasks import TargetDatabaseTask
    from utils import log_task_start, log_task_complete, log_task_error, execute_query
    from datetime import datetime
    import pymysql
    from config import TARGET_DB_CONFIG, DATA_DIR

    def read_sql_file(filepath):
        """Read SQL content from a file."""
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read().strip()

    def update_task_status(conn, task_name, status, error_message=None):
        """Update task status in etl_metadata table."""
        try:
            sql = """
                INSERT INTO etl_metadata (table_name, status, error_message, last_update_timestamp, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    error_message = VALUES(error_message),
                    last_update_timestamp = VALUES(last_update_timestamp),
                    updated_at = VALUES(updated_at)
            """
            execute_query(conn, sql, (task_name, status, error_message, datetime.now(), datetime.now()))
        except Exception as e:
            print(f"Could not update task status for {task_name}: {e}")

    class BaseDynamicSQLTask(TargetDatabaseTask):
        """Base class for dynamically created SQL tasks."""

        sql_file_path = luigi.Parameter()

        def update_status(self, status, error_message=None):
            """Update task execution status."""
            try:
                with self.get_db_connection() as conn:
                    update_task_status(conn, self.__class__.__name__, status, error_message)
            except:
                pass

        def output(self):
            task_name = self.__class__.__name__
            return luigi.LocalTarget(os.path.join(DATA_DIR, f'{task_name.lower()}_completed.txt'))

        def run(self):
            try:
                log_task_start(self)
                self.update_status('running')

                # Read and execute SQL file
                sql = read_sql_file(self.sql_file_path)
                with self.get_db_connection() as conn:
                    # For procedures, handle DELIMITER
                    if self.__class__.__name__.startswith('Procedures'):
                        # Split SQL by DELIMITER statements
                        import re
                        # Remove DELIMITER statements and split by //
                        sql = re.sub(r'DELIMITER\s+//', '', sql)
                        sql = re.sub(r'DELIMITER\s+;', '', sql)
                        statements = sql.split('//')
                        for statement in statements:
                            statement = statement.strip()
                            if statement:
                                conn.query(statement)
                        conn.commit()
                    else:
                        execute_query(conn, sql)

                # Mark as complete
                with self.output().open('w') as f:
                    f.write(f'{self.__class__.__name__} completed successfully\n')

                self.update_status('completed')
                log_task_complete(self)
            except Exception as e:
                self.update_status('failed', str(e))
                log_task_error(self, e)
                raise

    # Load dependency configuration
    dependencies_config, patterns_config = load_task_dependencies()

    # Create task classes
    dynamic_tasks = {}

    for task_name, info in _sql_files_info.items():
        sql_file_path = info['sql_file_path']
        dir_type = info['dir_type']
        dependencies = info['dependencies']

        if dir_type == 'init':
            # Database creation tasks - special handling
            class DynamicDBTask(BaseDynamicSQLTask):
                def run(self):
                    try:
                        log_task_start(self)
                        self.update_status('running')

                        # Read database creation SQL
                        sql = read_sql_file(self.sql_file_path)

                        # Create database using connection without database specified
                        db_config_no_db = TARGET_DB_CONFIG.copy()
                        db_config_no_db.pop('database', None)

                        conn = pymysql.connect(**db_config_no_db)
                        try:
                            with conn.cursor() as cursor:
                                cursor.execute(sql)
                            conn.commit()
                        finally:
                            conn.close()

                        # Mark as complete
                        with self.output().open('w') as f:
                            f.write(f'{self.__class__.__name__} completed successfully\n')

                        self.update_status('completed')
                        log_task_complete(self)
                    except Exception as e:
                        self.update_status('failed', str(e))
                        log_task_error(self, e)
                        raise

            # Create a unique class for this task
            task_class = type(task_name, (DynamicDBTask,), {'sql_file_path': sql_file_path})

        else:
            # Table and procedure tasks
            task_class = type(task_name, (BaseDynamicSQLTask,), {'sql_file_path': sql_file_path})

        dynamic_tasks[task_name] = task_class

    # Get all task names (dynamic + static)
    all_task_names = list(dynamic_tasks.keys())

    # Add static task names that are imported in the main pipeline
    static_tasks = [
        'ExtractPatientsTask', 'ExtractEncountersTask', 'ExtractObservationsTask', 'ExtractLocationsTask',
        'LoadPatientsTask', 'LoadEncountersTask', 'LoadObservationsTask', 'LoadLocationsTask',
        'CreateFlattenedPatientEncountersTask', 'CreateFlattenedObservationsTask',
        'CreatePatientSummaryTableTask', 'CreateObservationSummaryTableTask', 'CreateLocationSummaryTableTask',
        'CreateETLMetadataTableTask'
    ]
    all_task_names.extend(static_tasks)

    # Set up dependencies for all tasks
    for task_name, task_class in dynamic_tasks.items():
        deps = resolve_dependencies(task_name, all_task_names, dependencies_config, patterns_config)
        task_class.requires = lambda self, deps=deps, tasks=dynamic_tasks: [tasks[dep]() for dep in deps if dep in tasks]

    return _dynamic_tasks

def setup_task_dependencies():
    """Set up dependencies for all tasks based on configuration."""
    global _dynamic_tasks
    dependencies_config, patterns_config = load_task_dependencies()

    # Get all dynamic tasks
    _dynamic_tasks = create_dynamic_tasks()
    all_task_names = list(_dynamic_tasks.keys())

    # Add static task names
    static_tasks = [
        'ExtractPatientsTask', 'ExtractEncountersTask', 'ExtractObservationsTask', 'ExtractLocationsTask',
        'LoadPatientsTask', 'LoadEncountersTask', 'LoadObservationsTask', 'LoadLocationsTask',
        'CreateFlattenedPatientEncountersTask', 'CreateFlattenedObservationsTask',
        'CreatePatientSummaryTableTask', 'CreateObservationSummaryTableTask', 'CreateLocationSummaryTableTask',
        'CreateETLMetadataTableTask'
    ]
    all_task_names.extend(static_tasks)

    # Set up dependencies for static tasks by importing and modifying them
    import importlib

    # Import static task modules
    task_modules = [
        ('tasks.extract_tasks', ['ExtractPatientsTask', 'ExtractEncountersTask', 'ExtractObservationsTask', 'ExtractLocationsTask']),
        ('tasks.load_tasks', ['LoadPatientsTask', 'LoadEncountersTask', 'LoadObservationsTask', 'LoadLocationsTask']),
        ('tasks.flattened_table_tasks', ['CreateFlattenedPatientEncountersTask', 'CreateFlattenedObservationsTask',
                                        'CreatePatientSummaryTableTask', 'CreateObservationSummaryTableTask', 'CreateLocationSummaryTableTask']),
        ('tasks.schema_tasks', ['CreateETLMetadataTableTask'])
    ]

    for module_name, task_names in task_modules:
        try:
            module = importlib.import_module(module_name)
            for task_name in task_names:
                if hasattr(module, task_name):
                    task_class = getattr(module, task_name)
                    deps = resolve_dependencies(task_name, all_task_names, dependencies_config, patterns_config)
                    # Filter to only dynamic tasks for now (static tasks handle their own dependencies)
                    dynamic_deps = [d for d in deps if d in _dynamic_tasks]
                    if dynamic_deps:
                        task_class.requires = lambda self, deps=dynamic_deps, tasks=_dynamic_tasks: [tasks[dep]() for dep in deps if dep in tasks]
        except ImportError:
            pass  # Module might not exist

    return _dynamic_tasks