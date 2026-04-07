import subprocess
import sys

if __name__ == "__main__":
    django_proc = subprocess.Popen([sys.executable, "manage.py", "run_validator"])

    fastapi_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "web_app.api:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8037",
        ]
    )

    django_proc.wait()
    fastapi_proc.wait()
