# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Progress Service Module

Handles progress estimation and monitoring for pipeline execution.
"""

import time
import threading
from typing import Dict, Set, Optional

# Global progress tracking state
_progress_state = {
    'running': False,
    'current_task': None,
    'completed_tasks': set(),
    'total_tasks': 0,
    'start_time': None,
    'pipeline_type': None,
    'socketio': None
}

# Thread lock for state updates
_state_lock = threading.Lock()


def get_expected_tasks(action):
    """Get the expected tasks for a given pipeline action."""
    # Define task sets for different pipeline types
    full_tasks = [
        'TablesPatientsTask', 'TablesEncountersTask', 'TablesObservationsTask', 'TablesLocationsTask',
        'ExtractLoadPatientsTask', 'ExtractLoadEncountersTask', 'ExtractLoadObservationsTask', 'ExtractLoadLocationsTask',
        'ProceduresCreateFlattenedPatientEncountersTask', 'ProceduresCreateFlattenedObservationsTask',
        'CreateFlattenedPatientEncountersTask', 'CreateFlattenedObservationsTask'
    ]

    incremental_tasks = [
        'ExtractLoadPatientsTask', 'ExtractLoadEncountersTask', 'ExtractLoadObservationsTask', 'ExtractLoadLocationsTask',
        'ProceduresCreateFlattenedPatientEncountersTask', 'ProceduresCreateFlattenedObservationsTask',
        'CreateFlattenedPatientEncountersTask', 'CreateFlattenedObservationsTask'
    ]

    if action in ['full_refresh', 'force_refresh', 'scheduled_full']:
        return full_tasks
    elif action in ['incremental', 'scheduled_incremental', 'scheduled']:
        return incremental_tasks
    else:
        return full_tasks  # Default to full tasks


def initialize_progress_tracking(socketio_instance, action: str):
    """Initialize progress tracking for a new pipeline run."""
    global _progress_state

    with _state_lock:
        expected_tasks = get_expected_tasks(action)
        _progress_state.update({
            'running': True,
            'current_task': None,
            'completed_tasks': set(),
            'total_tasks': len(expected_tasks),
            'start_time': time.time(),
            'pipeline_type': action,
            'socketio': socketio_instance
        })


def update_task_progress(task_name: str, status: str):
    """Update progress when a task changes status."""
    global _progress_state

    with _state_lock:
        if status == 'completed':
            _progress_state['completed_tasks'].add(task_name)
        elif status == 'running':
            _progress_state['current_task'] = task_name

        # Calculate progress percentage
        completed_count = len(_progress_state['completed_tasks'])
        total_count = _progress_state['total_tasks']

        if total_count > 0:
            progress_percentage = min(95, (completed_count / total_count) * 100)
        else:
            progress_percentage = 0

        # Calculate elapsed time
        elapsed = time.time() - (_progress_state['start_time'] or time.time())

        # Prepare progress update
        progress_data = {
            'running': _progress_state['running'],
            'progress': int(progress_percentage),
            'current_task': _progress_state['current_task'],
            'message': f"Running {_progress_state['current_task'] or 'pipeline'}... ({completed_count}/{total_count} tasks completed, {int(elapsed)}s elapsed)"
        }

        # Emit to WebSocket clients
        if _progress_state['socketio']:
            try:
                _progress_state['socketio'].emit('task_update', progress_data)
            except Exception as e:
                print(f"Failed to emit progress update: {e}")


def finalize_progress_tracking(success: bool = True):
    """Finalize progress tracking when pipeline completes."""
    global _progress_state

    with _state_lock:
        elapsed = time.time() - (_progress_state['start_time'] or time.time())

        final_data = {
            'running': False,
            'progress': 100,
            'current_task': None,
            'message': f"Pipeline {'completed successfully' if success else 'failed'} in {int(elapsed)}s",
            'result': f"Pipeline {'completed successfully' if success else 'failed'} in {int(elapsed)}s"
        }

        if _progress_state['socketio']:
            try:
                _progress_state['socketio'].emit('task_update', final_data)
            except Exception as e:
                print(f"Failed to emit final progress update: {e}")

        # Reset state
        _progress_state.update({
            'running': False,
            'current_task': None,
            'completed_tasks': set(),
            'start_time': None,
            'pipeline_type': None
        })


def get_current_progress():
    """Get current progress state for status requests."""
    global _progress_state

    with _state_lock:
        if not _progress_state['running']:
            return {'running': False}

        elapsed = time.time() - (_progress_state['start_time'] or time.time())
        completed_count = len(_progress_state['completed_tasks'])
        total_count = _progress_state['total_tasks']

        return {
            'running': True,
            'progress': min(95, (completed_count / total_count * 100) if total_count > 0 else 0),
            'current_task': _progress_state['current_task'],
            'message': f"Running {_progress_state['current_task'] or 'pipeline'}... ({completed_count}/{total_count} tasks completed, {int(elapsed)}s elapsed)"
        }


def estimate_progress(elapsed_time, is_incremental=False):
    """Estimate progress percentage based on elapsed time (fallback only)."""
    if is_incremental:
        # Incremental pipelines are typically faster
        if elapsed_time < 3:
            return 10
        elif elapsed_time < 10:
            return 20 + (elapsed_time - 3) / 7 * 30  # 20-50%
        elif elapsed_time < 25:
            return 50 + (elapsed_time - 10) / 15 * 40  # 50-90%
        else:
            return min(95, 90 + (elapsed_time - 25) / 10)  # 90-95%
    else:
        # Full refresh pipelines take longer
        if elapsed_time < 5:
            return 5
        elif elapsed_time < 15:
            return 10 + (elapsed_time - 5) / 10 * 20  # 10-30%
        elif elapsed_time < 45:
            return 30 + (elapsed_time - 15) / 30 * 40  # 30-70%
        elif elapsed_time < 90:
            return 70 + (elapsed_time - 45) / 45 * 25  # 70-95%
        else:
            return min(98, 95 + (elapsed_time - 90) / 30)  # 95-98%


def monitor_pipeline_progress(process, task_name, socketio=None, task_status=None, action=None):
    """Monitor pipeline execution - now simplified since progress is event-driven."""
    # Initialize progress tracking
    initialize_progress_tracking(socketio, action or 'unknown')

    # Wait for process to complete
    process.wait()

    # Finalize progress tracking
    success = process.returncode == 0
    finalize_progress_tracking(success)

    return process


def initialize_task_status():
    """Initialize default task status."""
    return {
        'running': False,
        'current_task': None,
        'progress': 0,
        'message': '',
        'start_time': None
    }