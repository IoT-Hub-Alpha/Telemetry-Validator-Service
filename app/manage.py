#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import os
import sys
import threading
import uvicorn


def run_fastapi():
    uvicorn.run("web_app.api:app", host="0.0.0.0", port=8037)


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "telemetry_validator.settings")

    if len(sys.argv) > 1 and sys.argv[1] == "run_validator":
        fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
        fastapi_thread.start()

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
