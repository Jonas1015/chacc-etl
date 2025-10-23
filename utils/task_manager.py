# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at

#         http://www.apache.org/licenses/LICENSE-2.0

"""
Centralized Task Manager for Luigi Pipeline Execution
Provides robust task management with interruption, timeout, and monitoring capabilities.
"""

import luigi
import threading
import time
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)

class LuigiTaskManager:
    """
    Centralized task manager for Luigi pipeline execution with enhanced control and monitoring.
    """

    def __init__(self, scheduler_url: str = "http://localhost:8082",
                 max_concurrent_tasks: int = 3,
                 task_timeout: int = 3600):
        """
        Initialize the task manager.

        Args:
            scheduler_url: URL of the Luigi scheduler
            max_concurrent_tasks: Maximum number of tasks to run concurrently
            task_timeout: Default timeout for individual tasks in seconds
        """
        self.scheduler_url = scheduler_url
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_timeout = task_timeout

        self.running_tasks: Dict[str, Dict[str, Any]] = {}
        self.completed_tasks: Dict[str, Dict[str, Any]] = {}
        self.failed_tasks: Dict[str, Dict[str, Any]] = {}

        self.interrupt_flag = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_tasks, thread_name_prefix="luigi-task")

        self._lock = threading.RLock()
        logger.info(f"TaskManager initialized with max_concurrent={max_concurrent_tasks}, timeout={task_timeout}s")

    def run_pipeline(self, tasks: List[luigi.Task], action: str = "unknown") -> Dict[str, Any]:
        """
        Run a list of Luigi tasks with centralized management.

        Args:
            tasks: List of Luigi tasks to execute
            action: Pipeline action name for logging

        Returns:
            Dict containing execution results and statistics
        """
        logger.info(f"Starting pipeline execution: {action} with {len(tasks)} tasks")

        self.interrupt_flag.clear()
        pipeline_start_time = time.time()

        with self._lock:
            self.running_tasks.clear()
            self.completed_tasks.clear()
            self.failed_tasks.clear()

        try:
            future_to_task = {}
            for task in tasks:
                if self.interrupt_flag.is_set():
                    logger.info("Pipeline interrupted before task submission")
                    break

                future = self.executor.submit(self._execute_task_with_monitoring, task, action)
                future_to_task[future] = task

            results = []
            for future in as_completed(future_to_task, timeout=self.task_timeout * len(tasks)):
                if self.interrupt_flag.is_set():
                    logger.info("Pipeline interrupted during execution")
                    break

                try:
                    result = future.result(timeout=self.task_timeout)
                    results.append(result)
                except Exception as e:
                    task = future_to_task[future]
                    logger.error(f"Task {task.task_id} failed: {e}")
                    results.append({
                        'task_id': task.task_id,
                        'status': 'FAILED',
                        'error': str(e)
                    })

            pipeline_duration = time.time() - pipeline_start_time
            successful_tasks = len([r for r in results if r.get('status') == 'DONE'])
            failed_tasks = len([r for r in results if r.get('status') == 'FAILED'])

            pipeline_result = {
                'action': action,
                'total_tasks': len(tasks),
                'successful_tasks': successful_tasks,
                'failed_tasks': failed_tasks,
                'interrupted': self.interrupt_flag.is_set(),
                'duration': round(pipeline_duration, 2),
                'task_results': results
            }

            status = 'interrupted' if self.interrupt_flag.is_set() else ('completed' if failed_tasks == 0 else 'failed')
            logger.info(f"Pipeline {action} {status}: {successful_tasks}/{len(tasks)} tasks successful")

            return pipeline_result

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {
                'action': action,
                'error': str(e),
                'interrupted': self.interrupt_flag.is_set(),
                'duration': round(time.time() - pipeline_start_time, 2)
            }

    def _execute_task_with_monitoring(self, task: luigi.Task, pipeline_action: str) -> Dict[str, Any]:
        """
        Execute a single task with monitoring and error handling.

        Args:
            task: Luigi task to execute
            pipeline_action: Pipeline action name

        Returns:
            Dict containing task execution result
        """
        task_id = task.task_id
        task_name = task.__class__.__name__

        with self._lock:
            self.running_tasks[task_id] = {
                'task': task,
                'name': task_name,
                'start_time': time.time(),
                'pipeline_action': pipeline_action
            }

        logger.info(f"Starting task: {task_name} ({task_id})")

        try:
            result = luigi.build([task], local_scheduler=True)

            if result:
                status = 'DONE'
                logger.info(f"Task completed successfully: {task_name}")
            else:
                status = 'FAILED'
                logger.warning(f"Task failed: {task_name}")

            with self._lock:
                task_info = self.running_tasks.pop(task_id, {})
                task_info.update({
                    'end_time': time.time(),
                    'status': status,
                    'result': result
                })
                self.completed_tasks[task_id] = task_info

            return {
                'task_id': task_id,
                'task_name': task_name,
                'status': status,
                'duration': round(time.time() - task_info['start_time'], 2)
            }

        except Exception as e:
            logger.error(f"Task execution error: {task_name} - {e}")

            with self._lock:
                task_info = self.running_tasks.pop(task_id, {})
                task_info.update({
                    'end_time': time.time(),
                    'status': 'FAILED',
                    'error': str(e)
                })
                self.failed_tasks[task_id] = task_info

            return {
                'task_id': task_id,
                'task_name': task_name,
                'status': 'FAILED',
                'error': str(e),
                'duration': round(time.time() - task_info.get('start_time', time.time()), 2)
            }

    def interrupt_pipeline(self) -> bool:
        """
        Interrupt the currently running pipeline.

        Returns:
            True if interruption was successful
        """
        logger.info("Interrupting pipeline execution")

        self.interrupt_flag.set()

        try:
            response = requests.get(f"{self.scheduler_url}/api/task_list",
                                  params={"data": "running"}, timeout=5)
            if response.status_code == 200:
                running_tasks = response.json()

                for task_id in running_tasks.keys():
                    try:
                        cancel_response = requests.post(
                            f"{self.scheduler_url}/api/remove_task",
                            data={"task_id": task_id},
                            timeout=5
                        )
                        if cancel_response.status_code == 200:
                            logger.info(f"Cancelled task via scheduler: {task_id}")
                        else:
                            logger.warning(f"Failed to cancel task {task_id}: {cancel_response.status_code}")
                    except Exception as e:
                        logger.error(f"Error cancelling task {task_id}: {e}")

        except requests.exceptions.ConnectionError:
            logger.warning("Cannot connect to Luigi scheduler for task cancellation")
        except Exception as e:
            logger.error(f"Error during pipeline interruption: {e}")

        self.executor.shutdown(wait=False)

        return True

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline execution status.

        Returns:
            Dict containing current status information
        """
        with self._lock:
            return {
                'running_tasks': len(self.running_tasks),
                'completed_tasks': len(self.completed_tasks),
                'failed_tasks': len(self.failed_tasks),
                'interrupted': self.interrupt_flag.is_set(),
                'running_task_details': [
                    {
                        'task_id': task_id,
                        'name': info['name'],
                        'duration': round(time.time() - info['start_time'], 2)
                    }
                    for task_id, info in self.running_tasks.items()
                ]
            }

    def reset(self):
        """
        Reset the task manager for a new pipeline execution.
        """
        logger.info("Resetting task manager")

        self.interrupt_flag.clear()

        with self._lock:
            self.running_tasks.clear()
            self.completed_tasks.clear()
            self.failed_tasks.clear()

        if self.executor._shutdown:
            self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_tasks, thread_name_prefix="luigi-task")

    def __del__(self):
        """Cleanup resources on deletion."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)