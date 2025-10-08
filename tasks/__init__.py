# Try to import Luigi-dependent modules
try:
    from .base_tasks import BaseETLTask, SourceDatabaseTask, TargetDatabaseTask
    from .dynamic_task_factory_new import create_dynamic_tasks

    # Create all tasks dynamically from JSON configuration
    _dynamic_tasks = create_dynamic_tasks()

    # Import dynamic tasks into module namespace
    for task_name, task_class in _dynamic_tasks.items():
        globals()[task_name] = task_class

    # Build __all__ list with all tasks
    __all__ = [
        'BaseETLTask', 'SourceDatabaseTask', 'TargetDatabaseTask',
    ] + list(_dynamic_tasks.keys())

except ImportError:
    # Luigi not available, create minimal interface
    _dynamic_tasks = {}
    __all__ = []