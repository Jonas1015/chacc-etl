# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Daemon Service Module

Handles Luigi daemon management and startup.
"""

import subprocess
import os
import time

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False


def ensure_luigi_daemon_running(socketio=None):
    """Ensure Luigi daemon is running, start it if necessary."""
    try:
        if socketio:
            socketio.emit('task_update', {'message': 'Checking Luigi daemon status...'})
    except:
        pass

    check_result = subprocess.run(['pgrep', '-f', 'luigid'], capture_output=True, text=True)
    luigid_running = check_result.returncode == 0

    if not luigid_running:
        try:
            if socketio:
                socketio.emit('task_update', {'message': 'Starting Luigi daemon...'})
        except:
            pass

        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(logs_dir, exist_ok=True)

        subprocess.Popen(['luigid', '--background', '--logdir', logs_dir])
        time.sleep(2)
    return True


def is_daemon_running():
    """Check if Luigi daemon is currently running."""
    check_result = subprocess.run(['pgrep', '-f', 'luigid'], capture_output=True, text=True)
    return check_result.returncode == 0


def stop_daemon():
    """Stop the Luigi daemon if it's running."""
    if is_daemon_running():
        subprocess.run(['pkill', '-f', 'luigid'])
        return True
    return False