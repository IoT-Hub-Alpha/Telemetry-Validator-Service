"""Tests for schema cache (Redis + HTTP)."""

import json
import pytest
from unittest.mock import MagicMock, patch

from validator.services.schema_cache import SchemaCache


@pytest.fixture
def schema_data():
    return {
        "version": "1.0",
        "validation_schema": {
            "type": "object",
            "properties": {"value": {"type": "number"}},
            "required": ["value"],
        },
        "transformation_rules": {"rename": {"value": "temperature"}},
        "is_active": True,
    }


@pytest.fixture
def device_data():
    return {
        "id": "device-uuid-001",
        "serial_number": "SN-001",
        "is_active": True,
    }


class TestSchemaCacheGetSchema:
    """Tests for SchemaCache.get_schema."""

    @patch("validator.services.schema_cache.redis.Redis")
    @patch("validator.services.schema_cache.httpx.Client")
    def test_returns_schema_from_redis_cache(
        self, mock_http_cls, mock_redis_cls, schema_data
    ):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        mock_redis.get.return_value = json.dumps(schema_data)

        cache = SchemaCache()
        result = cache.get_schema("1.0")

        assert result is not None
        assert result["version"] == "1.0"
        mock_redis.get.assert_called_once_with("telemetry_schema:1.0")

    @patch("validator.services.schema_cache.redis.Redis")
    @patch("validator.services.schema_cache.httpx.Client")
    def test_fetches_from_api_on_cache_miss(
        self, mock_http_cls, mock_redis_cls, schema_data
    ):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        mock_redis.get.return_value = None

        mock_http = MagicMock()
        mock_http_cls.return_value = mock_http

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [schema_data]}
        mock_response.raise_for_status.return_value = None
        mock_http.get.return_value = mock_response

        cache = SchemaCache()
        result = cache.get_schema("1.0")

        assert result is not None
        assert result["version"] == "1.0"
        mock_redis.setex.assert_called_once()

    @patch("validator.services.schema_cache.redis.Redis")
    @patch("validator.services.schema_cache.httpx.Client")
    def test_returns_none_when_not_found(self, mock_http_cls, mock_redis_cls):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        mock_redis.get.return_value = None

        mock_http = MagicMock()
        mock_http_cls.return_value = mock_http

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_response.raise_for_status.return_value = None
        mock_http.get.return_value = mock_response

        cache = SchemaCache()
        result = cache.get_schema("99.0")

        assert result is None


class TestSchemaCacheGetDevice:
    """Tests for SchemaCache.get_device."""

    @patch("validator.services.schema_cache.redis.Redis")
    @patch("validator.services.schema_cache.httpx.Client")
    def test_returns_device_from_redis_cache(
        self, mock_http_cls, mock_redis_cls, device_data
    ):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        mock_redis.get.return_value = json.dumps(device_data)

        cache = SchemaCache()
        result = cache.get_device("SN-001")

        assert result is not None
        assert result["id"] == "device-uuid-001"
        mock_redis.get.assert_called_once_with("device:SN-001")

    @patch("validator.services.schema_cache.redis.Redis")
    @patch("validator.services.schema_cache.httpx.Client")
    def test_returns_none_for_unknown_device(self, mock_http_cls, mock_redis_cls):
        mock_redis = MagicMock()
        mock_redis_cls.from_url.return_value = mock_redis
        mock_redis.get.return_value = None

        mock_http = MagicMock()
        mock_http_cls.return_value = mock_http

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_response.raise_for_status.return_value = None
        mock_http.get.return_value = mock_response

        cache = SchemaCache()
        result = cache.get_device("SN-UNKNOWN")

        assert result is None
