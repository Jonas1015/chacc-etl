#!/usr/bin/env python3
"""
WSGI entry point for the ChaCC ETL Pipeline Web UI
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web_ui import app, socketio

application = app

if __name__ == "__main__":
    socketio.run(app, debug=False, host='0.0.0.0', port=5000)