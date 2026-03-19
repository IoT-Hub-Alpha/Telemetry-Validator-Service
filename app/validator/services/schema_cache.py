"""Redis cache + HTTP client for TelemetrySchema and Device lookups."""

from __future__ import annotations

import json
import logging

import httpx
import redis

from django.conf import settings

logger = logging.getLogger(__name__)


class SchemaCache:
    """
    Cache-aside pattern for TelemetrySchema and Device lookups.

    Flow: check Redis → miss → HTTP fetch from Device API → store in Redis.
    Graceful fallback to direct HTTP if Redis is unavailable.
    """

    def __init__(self) -> None:
        self._http = httpx.Client(
            base_url=settings.DEVICE_API_BASE_URL,
            timeout=settings.DEVICE_API_TIMEOUT_SECONDS,
        )
        try:
            self._redis = redis.Redis.from_url(
                settings.REDIS_URL, decode_responses=True
            )
            self._redis.ping()
            logger.info("Redis connection established for schema cache")
        except redis.ConnectionError:
            logger.warning(
                "Redis unavailable — schema cache will use direct HTTP only"
            )
            self._redis = None

    def get_schema(self, version: str) -> dict | None:
        """
        Get TelemetrySchema by version. Checks Redis first, then HTTP.

        Returns dict with 'validation_schema' and 'transformation_rules',
        or None if schema not found or inactive.
        """
        cache_key = f"telemetry_schema:{version}"

        # Try Redis
        if self._redis is not None:
            try:
                cached = self._redis.get(cache_key)
                if cached is not None:
                    return json.loads(cached)
            except redis.RedisError:
                logger.warning("Redis read error for schema %s", version)

        # Fetch from API
        schema = self._fetch_schema_from_api(version)
        if schema is None:
            return None

        # Store in Redis
        if self._redis is not None:
            try:
                self._redis.setex(
                    cache_key,
                    settings.SCHEMA_CACHE_TTL_SECONDS,
                    json.dumps(schema),
                )
            except redis.RedisError:
                logger.warning("Redis write error for schema %s", version)

        return schema

    def get_device(self, serial_number: str) -> dict | None:
        """
        Get Device by serial_number. Checks Redis first, then HTTP.

        Returns dict with 'id', 'serial_number', 'is_active',
        or None if device not found.
        """
        cache_key = f"device:{serial_number}"

        # Try Redis
        if self._redis is not None:
            try:
                cached = self._redis.get(cache_key)
                if cached is not None:
                    return json.loads(cached)
            except redis.RedisError:
                logger.warning("Redis read error for device %s", serial_number)

        # Fetch from API
        device = self._fetch_device_from_api(serial_number)
        if device is None:
            return None

        # Store in Redis
        if self._redis is not None:
            try:
                self._redis.setex(
                    cache_key,
                    settings.DEVICE_CACHE_TTL_SECONDS,
                    json.dumps(device),
                )
            except redis.RedisError:
                logger.warning("Redis write error for device %s", serial_number)

        return device

    def _fetch_schema_from_api(self, version: str) -> dict | None:
        """Fetch TelemetrySchema from Device API by version."""
        try:
            response = self._http.get(
                "/v1/telemetry-schemas/",
                params={"version": version, "is_active": "true"},
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                results = data
            elif isinstance(data, dict) and "results" in data:
                results = data["results"]
            else:
                results = [data]

            if not results:
                return None

            return results[0]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            logger.error("Schema API HTTP error: %s", e)
            return None
        except Exception as e:
            logger.error("Schema API request failed: %s", e)
            return None

    def _fetch_device_from_api(self, serial_number: str) -> dict | None:
        """Fetch Device from Device API by serial_number."""
        try:
            response = self._http.get(
                "/v1/devices/",
                params={"serial_number": serial_number},
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                results = data
            elif isinstance(data, dict) and "results" in data:
                results = data["results"]
            else:
                results = [data]

            if not results:
                return None

            return results[0]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            logger.error("Device API HTTP error: %s", e)
            return None
        except Exception as e:
            logger.error("Device API request failed: %s", e)
            return None

    def close(self) -> None:
        """Clean up connections."""
        self._http.close()
        if self._redis is not None:
            self._redis.close()
