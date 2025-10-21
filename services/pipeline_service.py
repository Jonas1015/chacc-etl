# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Pipeline Service Module

Handles pipeline execution, command building, and result processing.
"""

import subprocess
import sys
import multiprocessing
import os
import time


def get_optimal_worker_count():
    """Get optimal number of workers based on CPU cores."""
    num_cores = multiprocessing.cpu_count()
    return max(1, num_cores - 1)


def build_pipeline_command(action, workers):
    """Build the command to run the pipeline script."""
    cmd = [sys.executable, 'scripts/run_pipeline.py', '--central-scheduler', '--workers', str(workers)]

    if action == 'full_refresh':
        cmd.append('--full-refresh')
    elif action == 'incremental':
        cmd.append('--incremental')
    elif action == 'scheduled_incremental':
        cmd.append('--scheduled')
    elif action == 'scheduled_full':
        cmd.append('--scheduled')
        cmd.append('--full-refresh')
    elif action == 'force_refresh':
        cmd.append('--full-refresh')
        cmd.append('--force')

    return cmd


def execute_pipeline(action, socketio=None):
    """Execute a pipeline and return the subprocess."""
    workers = get_optimal_worker_count()
    try:
        if socketio:
            socketio.emit('task_update', {'message': f'Using {workers} workers on {multiprocessing.cpu_count()} CPU cores'})
    except:
        pass

    cmd = build_pipeline_command(action, workers)
    try:
        if socketio:
            socketio.emit('task_update', {'message': 'Executing pipeline...', 'progress': 5})
    except:
        pass

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,  # Keep as bytes for better output reading
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    # Give process a moment to start
    time.sleep(0.5)

    return process


def get_pipeline_result(process, task_name):
    """Get the final result from completed pipeline process."""
    stdout, stderr = process.communicate()

    # Decode bytes to string if needed
    if isinstance(stdout, bytes):
        stdout = stdout.decode('utf-8', errors='ignore')
    if isinstance(stderr, bytes):
        stderr = stderr.decode('utf-8', errors='ignore')

    # Check if the process actually completed successfully
    # Luigi returns 0 for success, non-zero for failure
    success = process.returncode == 0

    # Also check stdout for success indicators and error patterns
    if success:
        # Even if return code is 0, check if there were actual errors in output
        error_indicators = ['ERROR', 'FAILED', 'Exception', 'Traceback', 'error', 'failed']
        success_indicators = ['completed successfully', 'pipeline completed', 'successfully']

        has_errors = any(error in stdout or error in stderr for error in error_indicators)
        has_success = any(success_text in stdout.lower() for success_text in success_indicators)

        # If we have clear error indicators, mark as failed
        if has_errors and not has_success:
            success = False

    if success:
        return {
            'progress': 100,
            'message': f"{task_name} completed successfully!",
            'result': f"{task_name} completed successfully!\n\nOutput:\n{stdout}"
        }
    else:
        return {
            'progress': 100,
            'message': f"{task_name} failed!",
            'result': f"{task_name} failed with exit code {process.returncode}\n\nError:\n{stderr}\n\nOutput:\n{stdout}"
        }