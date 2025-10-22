#!/usr/bin/env python3
"""
Progress Parser Utility Module

Handles parsing of Luigi output for progress tracking.
Tracks actual task execution states from Luigi output.
"""

import re
import time
from typing import Dict, Optional

_luigi_progress_state = {
    'running_tasks': set(),
    'completed_tasks': set(),
    'failed_tasks': set(),
    'total_tasks': 0,
    'start_time': None,
    'current_task': None,
    'pipeline_type': None
}


def reset_progress_state():
    """Reset the progress state for a new pipeline run."""
    global _luigi_progress_state
    _luigi_progress_state = {
        'running_tasks': set(),
        'completed_tasks': set(),
        'failed_tasks': set(),
        'total_tasks': 0,
        'start_time': None,
        'current_task': None,
        'pipeline_type': None
    }


def initialize_progress_tracking(pipeline_type: str):
    """Initialize progress tracking for a new pipeline."""
    reset_progress_state()
    _luigi_progress_state['start_time'] = time.time()
    _luigi_progress_state['pipeline_type'] = pipeline_type


def parse_luigi_progress(line: str, action: str) -> Optional[Dict]:
    """Parse basic progress information from Luigi output."""

    if 'This progress looks :)' in line or 'completed successfully' in line.lower():
        elapsed = time.time() - (_luigi_progress_state['start_time'] or time.time())
        return {
            'running': False,
            'progress': 100,
            'message': f"{action.replace('_', ' ').title()} completed successfully in {int(elapsed)}s!",
            'current_task': None,
            'result': f"Pipeline completed successfully in {int(elapsed)}s"
        }

    if 'failed' in line.lower() and ('pipeline' in line.lower() or 'task' in line.lower()):
        elapsed = time.time() - (_luigi_progress_state['start_time'] or time.time())
        return {
            'running': False,
            'progress': 100,
            'message': f"Pipeline failed after {int(elapsed)}s",
            'current_task': None,
            'result': f"Pipeline failed after {int(elapsed)}s"
        }

    if any(keyword in line.lower() for keyword in ['starting', 'initializing', 'connecting to database']):
        return {
            'running': True,
            'progress': 5,
            'message': line.strip(),
            'current_task': action.replace('_', ' ').title()
        }

    return None


def check_luigi_daemon_status() -> bool:
    """Check if Luigi daemon is running by attempting to connect to it."""
    try:
        import requests
        from config.luigi_config import SCHEDULER_HOST, SCHEDULER_PORT
        url = f'http://{SCHEDULER_HOST}:{SCHEDULER_PORT}/api/task_list'
        response = requests.get(url, timeout=2)
        return response.status_code == 200
    except:
        return False


def get_luigi_task_status() -> Dict:
    """Get detailed task status from Luigi API."""
    try:
        import requests
        from config.luigi_config import SCHEDULER_HOST, SCHEDULER_PORT
        url = f'http://{SCHEDULER_HOST}:{SCHEDULER_PORT}/api/task_list'
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json().get('response', {})
    except:
        pass
    return {}


def get_current_progress() -> Dict:
    """Get current progress state from Luigi API."""
    if not check_luigi_daemon_status():
        return {
            'running': False,
            'progress': 0,
            'current_task': None,
            'message': 'Luigi daemon is not running. Start it with: luigid --background --logdir logs/',
            'luigi_status': 'not_running'
        }

    task_status = get_luigi_task_status()

    if not task_status:
        return {
            'running': False,
            'progress': 0,
            'current_task': None,
            'message': 'Luigi daemon is running. Ready to start pipelines.',
            'luigi_status': 'ready'
        }

    total_tasks = len(task_status)
    completed_tasks = sum(1 for task in task_status.values() if task.get('status') == 'DONE')
    running_tasks = [task for task in task_status.values() if task.get('status') == 'RUNNING']
    failed_tasks = sum(1 for task in task_status.values() if task.get('status') == 'FAILED')
    pending_tasks = sum(1 for task in task_status.values() if task.get('status') == 'PENDING')
    disabled_tasks = sum(1 for task in task_status.values() if task.get('status') == 'DISABLED')

    current_task = None
    if running_tasks:
        current_task = max(running_tasks, key=lambda t: t.get('start_time', 0))['name']

    if total_tasks > 0:
        progress = min(95, (completed_tasks / total_tasks) * 100)
    else:
        progress = 0

    is_running = len(running_tasks) > 0

    pipeline_start_time = None
    elapsed = 0
    if task_status:
        all_start_times = [task.get('start_time') for task in task_status.values() if task.get('start_time')]
        if all_start_times:
            pipeline_start_time = min(all_start_times)
            elapsed = time.time() - pipeline_start_time

    pipeline_type = 'unknown'
    if task_status:
        task_names = [task.get('name', '') for task in task_status.values()]
        if any('Incremental' in name for name in task_names):
            pipeline_type = 'incremental'
        elif any('FullRefresh' in name for name in task_names):
            pipeline_type = 'full_refresh'
        elif any(task.get('params', {}).get('incremental') == 'False' for task in task_status.values()):
            pipeline_type = 'full_refresh'
        else:
            pipeline_type = 'unknown'

    if not is_running and total_tasks > 0:
        success = failed_tasks == 0 and completed_tasks > 0 and pending_tasks == 0 and disabled_tasks == 0
        end_time = time.time()

        try:
            from services.history_service import get_last_pending_execution, update_pipeline_status
            last_pending = get_last_pending_execution()
            if last_pending:
                if last_pending.get('action') == pipeline_type or pipeline_type == 'unknown':
                    if success:
                        final_status = 'completed'
                    elif pending_tasks > 0:
                        final_status = 'pending'
                    elif failed_tasks > 0:
                        final_status = 'failed'
                    else:
                        final_status = 'done'

                    result_msg = f"Tasks: {completed_tasks} completed, {failed_tasks} failed, {pending_tasks} pending, {disabled_tasks} disabled."
                    update_pipeline_status(last_pending['id'], final_status, end_time, success, result_msg)
                else:
                    print(f"Pipeline type mismatch: expected {last_pending.get('action')}, got {pipeline_type}. Skipping update.")
        except Exception as e:
            print(f"Failed to update pipeline execution: {e}")

    if is_running:
        message = f"Running {current_task or 'pipeline'}... ({completed_tasks}/{total_tasks} tasks completed, {int(elapsed)}s elapsed)"
        if failed_tasks > 0:
            message += f", {failed_tasks} failed"
    else:
        if completed_tasks == total_tasks and total_tasks > 0:
            success_status = "successfully" if failed_tasks == 0 else f"with {failed_tasks} failures"
            message = f"Pipeline completed {success_status}! ({completed_tasks}/{total_tasks} tasks completed)"
        elif pending_tasks > 0:
            message = f"Pipeline pending... ({completed_tasks}/{total_tasks} tasks completed, {pending_tasks} pending)"
        else:
            message = f"Luigi daemon is running. Ready to start pipelines."

    return {
        'running': is_running,
        'progress': int(progress),
        'current_task': current_task,
        'message': message,
        'completed_tasks': completed_tasks,
        'total_tasks': total_tasks,
        'failed_tasks': failed_tasks,
        'pending_tasks': pending_tasks,
        'pipeline_type': pipeline_type,
        'pipeline_start_time': pipeline_start_time,
        'elapsed_time': int(elapsed),
        'luigi_status': 'running_pipeline' if is_running else ('pending' if pending_tasks > 0 else 'ready')
    }