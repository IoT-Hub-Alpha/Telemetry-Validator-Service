"""Shared test fixtures for Telemetry Validator Service."""

import types
import pytest


@pytest.fixture()
def fake_settings():
    return types.SimpleNamespace(
        KAFKA_TOPIC_TELEMETRY_RAW="telemetry.raw",
        KAFKA_TOPIC_TELEMETRY_CLEAN="telemetry.clean",
        KAFKA_TOPIC_TELEMETRY_DLQ="telemetry.dlq",
        KAFKA_CONSUMER_GROUP="telemetry-validator",
        CONSUMER_BATCH_SIZE=100,
        CONSUMER_POLL_TIMEOUT=1.0,
        KAFKA_REQUEST_TIMEOUT_MS=30000,
        REDIS_URL="redis://localhost:6379/0",
        SCHEMA_CACHE_TTL_SECONDS=300,
        DEVICE_CACHE_TTL_SECONDS=600,
        DEVICE_API_BASE_URL="http://localhost:8000/api",
        DEVICE_API_TIMEOUT_SECONDS=5.0,
        METRICS_PORT=9103,
    )
