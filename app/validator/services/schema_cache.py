"""Redis cache + HTTP client for TelemetrySchema and Device lookups."""

from __future__ import annotations

import json
import logging
import time

import httpx
import redis

from django.conf import settings

logger = logging.getLogger(__name__)


class SchemaCache:
    """
    Cache-aside pattern for TelemetrySchema and Device lookups.

    Flow: check memory → miss → check Redis → miss → HTTP fetch → store in both.
    Graceful fallback to direct HTTP if Redis is unavailable.
    """

    def __init__(self) -> None:
        self._mem_cache: dict[str, tuple[float, dict]] = {}
        self._mem_ttl = 30  # seconds
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
            logger.warning("Redis unavailable — schema cache will use direct HTTP only")
            self._redis = None

    def warm_cache(self) -> None:
        """
        Pre-load all active schemas and devices into Redis.

        Call once at startup to avoid cold-cache latency on first messages.
        Falls back silently if API or Redis is unavailable.
        """
        if self._redis is None:
            logger.warning("Skipping cache warm-up — Redis not available")
            return

        schemas_loaded = 0
        devices_loaded = 0

        # Warm schemas
        try:
            schemas = self._fetch_all_pages(
                "/v1/telemetry-schemas/", {"is_active": "true"}
            )
            for schema in schemas:
                version = schema.get("version")
                if not version:
                    continue
                cache_key = f"telemetry_schema:{version}"
                self._redis.setex(
                    cache_key,
                    settings.SCHEMA_CACHE_TTL_SECONDS,
                    json.dumps(schema),
                )
                schemas_loaded += 1
        except Exception as e:
            logger.warning("Cache warm-up failed for schemas: %s", e)

        # Warm devices
        try:
            devices = self._fetch_all_pages("/v1/devices/", {"is_active": "true"})
            for device in devices:
                serial = device.get("serial_number")
                if not serial:
                    continue
                cache_key = f"device:{serial}"
                self._redis.setex(
                    cache_key,
                    settings.DEVICE_CACHE_TTL_SECONDS,
                    json.dumps(device),
                )
                devices_loaded += 1
        except Exception as e:
            logger.warning("Cache warm-up failed for devices: %s", e)

        logger.info(
            "Cache warm-up complete: %d schemas, %d devices",
            schemas_loaded,
            devices_loaded,
        )

    def _mem_get(self, key: str) -> dict | None:
        """Get value from in-memory cache if not expired."""
        entry = self._mem_cache.get(key)
        if entry is not None:
            ts, value = entry
            if time.monotonic() - ts < self._mem_ttl:
                return value
            del self._mem_cache[key]
        return None

    def _mem_set(self, key: str, value: dict) -> None:
        """Store value in in-memory cache with current timestamp."""
        self._mem_cache[key] = (time.monotonic(), value)

    def get_schema(self, version: str) -> dict | None:
        """
        Get TelemetrySchema by version. Checks memory, then Redis, then HTTP.

        Returns dict with 'validation_schema' and 'transformation_rules',
        or None if schema not found or inactive.
        """
        cache_key = f"telemetry_schema:{version}"

        # Try memory
        mem_hit = self._mem_get(cache_key)
        if mem_hit is not None:
            return mem_hit

        # Try Redis
        if self._redis is not None:
            try:
                cached = self._redis.get(cache_key)
                if cached is not None:
                    result = json.loads(cached)
                    self._mem_set(cache_key, result)
                    return result
            except redis.RedisError:
                logger.warning("Redis read error for schema %s", version)

        # Fetch from API
        schema = self._fetch_schema_from_api(version)
        if schema is None:
            return None

        # Store in memory
        self._mem_set(cache_key, schema)

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
        Get Device by serial_number. Checks memory, then Redis, then HTTP.

        Returns dict with 'id', 'serial_number', 'is_active',
        or None if device not found.
        """
        cache_key = f"device:{serial_number}"

        # Try memory
        mem_hit = self._mem_get(cache_key)
        if mem_hit is not None:
            return mem_hit

        # Try Redis
        if self._redis is not None:
            try:
                cached = self._redis.get(cache_key)
                if cached is not None:
                    result = json.loads(cached)
                    self._mem_set(cache_key, result)
                    return result
            except redis.RedisError:
                logger.warning("Redis read error for device %s", serial_number)

        # Fetch from API
        device = self._fetch_device_from_api(serial_number)
        if device is None:
            return None

        # Store in memory
        self._mem_set(cache_key, device)

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

    def _fetch_all_pages(self, endpoint: str, params: dict) -> list[dict]:
        """Fetch all results from a paginated API endpoint."""
        results = []
        try:
            response = self._http.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                results = data
            elif isinstance(data, dict) and "results" in data:
                results = data["results"]
            else:
                results = [data]
        except Exception as e:
            logger.error("API fetch failed for %s: %s", endpoint, e)

        return results

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
