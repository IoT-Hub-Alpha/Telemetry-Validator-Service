"""Tests for the processing pipeline (MessageRouter)."""

import json
import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from validator.services.pipeline import MessageRouter


def _make_kafka_message(
    value_dict=None, key=None, topic="telemetry.raw", partition=0, offset=0
):
    """Create a mock Kafka message."""
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = key

    if value_dict is not None:
        msg.value.return_value = json.dumps(value_dict).encode("utf-8")
    else:
        msg.value.return_value = None

    return msg


def _make_raw_envelope(**overrides):
    """Build a valid raw telemetry.raw message body."""
    base = {
        "request_id": str(uuid4()),
        "ingest_protocol": "http",
        "serial_number": "SN-001",
        "payload": {
            "schema_version": "1.0",
            "value": 42,
        },
        "received_at": "2026-01-15T12:00:00+00:00",
        "ingest_index": 0,
    }
    base.update(overrides)
    return base


@pytest.fixture
def schema_data():
    return {
        "version": "1.0",
        "validation_schema": {
            "type": "object",
            "properties": {
                "schema_version": {"type": "string"},
                "serial_number": {"type": "string"},
                "value": {"type": "number"},
            },
            "required": ["schema_version", "value"],
        },
        "transformation_rules": {"rename": {"value": "temperature"}},
        "is_active": True,
    }


@pytest.fixture
def device_data():
    return {"id": "device-uuid-001", "serial_number": "SN-001", "is_active": True}


@pytest.fixture
def mock_cache(schema_data, device_data):
    cache = MagicMock()
    cache.get_schema.return_value = schema_data
    cache.get_device.return_value = device_data
    return cache


@pytest.fixture
def mock_producer():
    return MagicMock()


@pytest.fixture
def router(mock_cache, mock_producer):
    return MessageRouter(mock_cache, mock_producer)


class TestMessageRouterHappy:
    """Happy path: valid messages routed to telemetry.clean."""

    def test_valid_message_published_to_clean(self, router, mock_producer):
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "telemetry.clean"

    def test_clean_envelope_contains_device_id(self, router, mock_producer):
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        published_value = mock_producer.produce.call_args.kwargs["value"]
        assert published_value["device_id"] == "device-uuid-001"

    def test_clean_envelope_has_transformed_payload(self, router, mock_producer):
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        published_value = mock_producer.produce.call_args.kwargs["value"]
        assert "temperature" in published_value["payload"]

    def test_clean_key_is_serial_number(self, router, mock_producer):
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw, key=None)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["key"] == "SN-001"


class TestMessageRouterDLQ:
    """Error paths: invalid messages routed to telemetry.dlq."""

    def test_empty_payload_to_dlq(self, router, mock_producer):
        msg = _make_kafka_message(value_dict=None)
        msg.value.return_value = None

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "telemetry.dlq"
        published_value = call_kwargs["value"]
        assert published_value["error"]["code"] == "empty_payload"

    def test_malformed_json_to_dlq(self, router, mock_producer):
        msg = MagicMock()
        msg.topic.return_value = "telemetry.raw"
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        msg.key.return_value = None
        msg.value.return_value = b"not json{{"

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "telemetry.dlq"
        published_value = call_kwargs["value"]
        assert published_value["error"]["code"] == "malformed_json"

    def test_missing_schema_version_to_dlq(self, router, mock_producer):
        raw = _make_raw_envelope(payload={"no_version": True})
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "telemetry.dlq"

    def test_unknown_device_to_dlq(self, router, mock_cache, mock_producer):
        mock_cache.get_device.return_value = None
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "telemetry.dlq"
        published_value = call_kwargs["value"]
        assert published_value["error"]["code"] == "unknown_device"

    def test_schema_not_found_to_dlq(self, router, mock_cache, mock_producer):
        mock_cache.get_schema.return_value = None
        raw = _make_raw_envelope()
        msg = _make_kafka_message(raw)

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "telemetry.dlq"
        published_value = call_kwargs["value"]
        assert "not found" in str(published_value["error"]["detail"])

    def test_dlq_envelope_has_source_metadata(self, router, mock_producer):
        msg = _make_kafka_message(value_dict=None)
        msg.value.return_value = None

        with patch("validator.services.pipeline.settings") as mock_settings:
            mock_settings.KAFKA_TOPIC_TELEMETRY_CLEAN = "telemetry.clean"
            mock_settings.KAFKA_TOPIC_TELEMETRY_DLQ = "telemetry.dlq"
            router.process_message(msg)

        dlq_value = mock_producer.produce.call_args.kwargs["value"]
        assert dlq_value["source"]["topic"] == "telemetry.raw"
        assert dlq_value["source"]["partition"] == 0
        assert dlq_value["source"]["offset"] == 0


class TestHelperMethods:
    """Tests for static helper methods on MessageRouter."""

    def test_build_event_id_from_request(self):
        raw_obj = {"request_id": "abc-123", "ingest_index": 5}
        result = MessageRouter._build_event_id(raw_obj, "t", 0, 0)
        assert result == "abc-123:5"

    def test_build_event_id_fallback(self):
        result = MessageRouter._build_event_id(None, "topic", 2, 99)
        assert result == "topic:2:99"

    def test_resolve_dlq_key_from_source(self):
        result = MessageRouter._resolve_dlq_key(b"existing-key", {})
        assert result == b"existing-key"

    def test_resolve_dlq_key_from_serial(self):
        result = MessageRouter._resolve_dlq_key(None, {"serial_number": "SN-001"})
        assert result == b"SN-001"

    def test_resolve_dlq_key_none(self):
        result = MessageRouter._resolve_dlq_key(None, {})
        assert result is None

    def test_decode_raw_for_dlq_json(self):
        raw = json.dumps({"a": 1}).encode("utf-8")
        result = MessageRouter._decode_raw_for_dlq(raw)
        assert result == {"a": 1}

    def test_decode_raw_for_dlq_invalid_json(self):
        result = MessageRouter._decode_raw_for_dlq(b"not json")
        assert result == {"encoding": "utf-8", "data": "not json"}

    def test_decode_raw_for_dlq_binary(self):
        result = MessageRouter._decode_raw_for_dlq(b"\x80\x81\x82")
        assert result["encoding"] == "base64"

    def test_decode_raw_for_dlq_none(self):
        assert MessageRouter._decode_raw_for_dlq(None) is None

    def test_ensure_jsonable_passthrough(self):
        assert MessageRouter._ensure_jsonable("hello") == "hello"
        assert MessageRouter._ensure_jsonable({"a": 1}) == {"a": 1}

    def test_ensure_jsonable_fallback(self):
        obj = object()
        result = MessageRouter._ensure_jsonable(obj)
        assert isinstance(result, str)
