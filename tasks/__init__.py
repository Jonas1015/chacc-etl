# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0


try:
    from .base_tasks import BaseETLTask, SourceDatabaseTask, TargetDatabaseTask
    from .dynamic_task_factory import create_dynamic_tasks

    _dynamic_tasks = create_dynamic_tasks()

    for task_name, task_class in _dynamic_tasks.items():
        globals()[task_name] = task_class

    __all__ = [
        'BaseETLTask', 'SourceDatabaseTask', 'TargetDatabaseTask',
    ] + list(_dynamic_tasks.keys())

except ImportError:
    _dynamic_tasks = {}
    __all__ = []