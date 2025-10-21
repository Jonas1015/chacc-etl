#!/usr/bin/env python3
"""
Progress Parser Utility Module

Handles parsing of Luigi output for progress tracking.
"""

import re


def parse_luigi_progress(line, action):
    """Parse progress information from Luigi output"""
    patterns = [
        r'(\d+)% complete',
        r'progress: (\d+)%',
        r'(\d+)/(\d+) tasks?',
        r'Running (\w+)',
        r'Completed (\w+)',
        r'Scheduled (\w+)',
        r'INFO.*luigi.*: (.*)',
    ]

    for pattern in patterns:
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            if '%' in pattern:
                try:
                    progress = int(match.group(1))
                    return {
                        'running': True,
                        'progress': min(progress, 95),
                        'message': f"Pipeline running... {progress}% complete",
                        'current_task': action.replace('_', ' ').title()
                    }
                except (ValueError, IndexError):
                    continue
            elif 'tasks' in pattern.lower():
                try:
                    completed = int(match.group(1))
                    total = int(match.group(2))
                    progress = min(int((completed / total) * 100), 95)
                    return {
                        'running': True,
                        'progress': progress,
                        'message': f"Running pipeline... ({completed}/{total} tasks completed)",
                        'current_task': action.replace('_', ' ').title()
                    }
                except (ValueError, IndexError, ZeroDivisionError):
                    continue
            elif 'Running' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Running {task_name}...",
                    'current_task': task_name
                }
            elif 'Completed' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Completed {task_name}",
                    'current_task': task_name
                }
            elif 'Scheduled' in pattern:
                task_name = match.group(1)
                return {
                    'running': True,
                    'message': f"Scheduled {task_name}",
                    'current_task': task_name
                }
            elif 'INFO' in pattern:
                info_msg = match.group(1)
                return {
                    'running': True,
                    'message': info_msg,
                    'current_task': action.replace('_', ' ').title()
                }

    # Check for Luigi success indicators
    if 'This progress looks :)' in line or 'completed successfully' in line.lower():
        return {
            'running': False,
            'progress': 100,
            'message': f"{action.replace('_', ' ').title()} completed successfully!",
            'current_task': None
        }

    if any(keyword in line.lower() for keyword in ['starting', 'initializing', 'connecting', 'processing']):
        return {
            'running': True,
            'message': line.strip(),
            'current_task': action.replace('_', ' ').title()
        }

    return None