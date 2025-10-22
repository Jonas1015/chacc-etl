# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Progress Service Module

Handles progress monitoring for pipeline execution.
Now delegates to progress_parser for actual Luigi output tracking.
"""

from utils.progress_parser import get_current_progress, initialize_progress_tracking


def initialize_progress_tracking(socketio_instance, action: str):
    """Initialize progress tracking for a new pipeline run."""
    # Initialize the parser's progress tracking
    from utils.progress_parser import initialize_progress_tracking as parser_init_progress
    parser_init_progress(action)


def get_current_progress():
    """Get current progress state from the parser."""
    from utils.progress_parser import get_current_progress as parser_get_current_progress
    return parser_get_current_progress()


def update_task_progress(task_name: str, status: str):
    """Update progress for a specific task."""
    # This function is used by base_tasks.py for task lifecycle events
    # We can extend this later if needed for more detailed progress tracking
    pass


def initialize_task_status():
    """Initialize default task status."""
    return {
        'running': False,
        'current_task': None,
        'progress': 0,
        'message': '',
        'start_time': None
    }