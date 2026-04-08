"""
Django settings for telemetry_validator project.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# --- Security ---

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY")
if not SECRET_KEY or "insecure" in SECRET_KEY.lower():
    raise ValueError("SECRET_KEY must be set to a secure value")

DEBUG = str(os.getenv("VALIDATOR_DEBUG", "false")).lower() in ("true", "yes", "1")

ALLOWED_HOSTS = []

# --- Application definition ---

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "validator",
]

# No middleware needed — this service does not serve HTTP requests.
MIDDLEWARE = []

ROOT_URLCONF = "telemetry_validator.urls"

# --- Database ---
# Stateless service — no database needed.
# Using dummy backend so Django doesn't complain.

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

# --- Internationalization ---

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = False
USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# --- Kafka ---

KAFKA_TOPIC_TELEMETRY_RAW = os.getenv("KAFKA_TOPIC_TELEMETRY_RAW", "telemetry.raw")
KAFKA_TOPIC_TELEMETRY_CLEAN = os.getenv(
    "KAFKA_TOPIC_TELEMETRY_CLEAN", "telemetry.clean"
)
KAFKA_TOPIC_TELEMETRY_DLQ = os.getenv("KAFKA_TOPIC_TELEMETRY_DLQ", "telemetry.dlq")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "telemetry-validator")
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))

# Consumer tuning
CONSUMER_BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
CONSUMER_POLL_TIMEOUT = float(os.getenv("CONSUMER_POLL_TIMEOUT", "1.0"))

# --- Redis ---

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SCHEMA_CACHE_TTL_SECONDS = int(os.getenv("SCHEMA_CACHE_TTL_SECONDS", "300"))
DEVICE_CACHE_TTL_SECONDS = int(os.getenv("DEVICE_CACHE_TTL_SECONDS", "600"))

# --- Device API ---

DEVICE_API_BASE_URL = os.getenv("DEVICE_API_BASE_URL", "http://api-device:8010")
DEVICE_API_TIMEOUT_SECONDS = float(os.getenv("DEVICE_API_TIMEOUT_SECONDS", "5.0"))

# --- Metrics ---

METRICS_PORT = int(os.getenv("METRICS_PORT", "9103"))

# --- Logging (iot_logging) ---

try:
    import iot_logging  # noqa: F401

    _log_formatter = {"()": "iot_logging.StructuredJsonFormatter"}
except ImportError:
    _log_formatter = {
        "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
    }

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": _log_formatter,
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG" if DEBUG else "INFO",
    },
}
