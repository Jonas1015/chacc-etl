# Copyright 2025 Jonas G Mwambimbi
# Licensed under the Apache License, Version 2.0 (see LICENSE file for details)

"""
Socket Handlers Module

Handles WebSocket events for real-time communication.
"""

try:
    from flask_socketio import emit
    HAS_SOCKETIO = True
except ImportError:
    HAS_SOCKETIO = False
    def emit(*args, **kwargs):
        pass

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def handle_connect():
    """Send current status when client connects"""
    if HAS_SOCKETIO:
        from services.progress_service import get_current_progress
        current_progress = get_current_progress()

        # Always emit current progress state, whether running or not
        emit('task_update', current_progress)


def handle_get_status():
    """Handle status requests"""
    if HAS_SOCKETIO:
        from services.progress_service import get_current_progress
        current_progress = get_current_progress()

        # Always emit current progress state
        emit('task_update', current_progress)


def register_socket_handlers(socketio):
    """Register all socket event handlers"""
    if HAS_SOCKETIO and socketio:
        socketio.on_event('connect', handle_connect)
        socketio.on_event('get_status', handle_get_status)