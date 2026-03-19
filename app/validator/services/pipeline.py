"""Main processing pipeline: validate → transform → route."""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone as dt_timezone
from typing import Any

from django.conf import settings

from IoTKafka import IoTKafkaProducer, IoTProducerException

from validator.services.envelope import RawContractError, to_utc, validate_raw_contract
from validator.services.schema_cache import SchemaCache
from validator.services.transform import process_telemetry_payload

logger = logging.getLogger(__name__)


class MessageRouter:
    """
    Routes a single Kafka message to telemetry.clean or telemetry.dlq.

    Orchestrates: decode → validate envelope → fetch schema →
    validate payload → transform → resolve device → route.
    """

    def __init__(
        self,
        schema_cache: SchemaCache,
        producer: IoTKafkaProducer,
    ) -> None:
        self._cache = schema_cache
        self._producer = producer

    def process_message(self, message) -> None:
        """
        Process a single Kafka message: validate, transform, and route.

        On success → publish to telemetry.clean.
        On failure → publish to telemetry.dlq with error details.
        """
        source_topic = message.topic()
        source_partition = message.partition()
        source_offset = message.offset()
        source_key = message.key()

        raw_obj: dict[str, Any] | None = None
        raw_payload_for_dlq = self._decode_raw_for_dlq(message.value())

        try:
            raw_obj = self._decode_message_value(message.value())
            raw_payload_for_dlq = raw_obj

            contract = validate_raw_contract(raw_obj)

            normalized = self._normalize_payload(contract)

            event_id = self._build_event_id(
                raw_obj, source_topic, source_partition, source_offset
            )
            clean_key = source_key or contract["serial_number"]

            clean_envelope = {
                "event_id": event_id,
                "request_id": contract["request_id"],
                "ingest_protocol": contract["ingest_protocol"],
                "serial_number": contract["serial_number"],
                "ingest_index": contract["ingest_index"],
                "received_at": contract["received_at"],
                "processed_at": datetime.now(dt_timezone.utc).isoformat(),
                "device_id": normalized["device_id"],
                "payload": normalized["payload"],
            }
            if normalized.get("timestamp"):
                clean_envelope["timestamp"] = normalized["timestamp"]

            self._publish(
                topic=settings.KAFKA_TOPIC_TELEMETRY_CLEAN,
                key=clean_key,
                value=clean_envelope,
            )
            return

        except RawContractError as exc:
            error_code, error_detail = exc.code, exc.detail
        except Exception as exc:
            error_code, error_detail = "unexpected_error", str(exc)

        # DLQ path
        event_id = self._build_event_id(
            raw_obj, source_topic, source_partition, source_offset
        )
        dlq_key = self._resolve_dlq_key(source_key=source_key, raw_obj=raw_obj)

        dlq_envelope = {
            "event_id": event_id,
            "failed_at": datetime.now(dt_timezone.utc).isoformat(),
            "error": {
                "code": error_code,
                "detail": self._ensure_jsonable(error_detail),
            },
            "source": {
                "topic": source_topic,
                "partition": source_partition,
                "offset": source_offset,
                "key": self._decode_key_for_logs(source_key),
            },
            "raw_message": raw_payload_for_dlq,
        }

        self._publish(
            topic=settings.KAFKA_TOPIC_TELEMETRY_DLQ,
            key=dlq_key,
            value=dlq_envelope,
        )

    def _publish(self, topic: str, key, value: dict) -> None:
        """Publish a message via IoTKafkaProducer.

        IoTKafkaProducer auto-serializes dict→JSON bytes and str→bytes.
        """
        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
            )
        except IoTProducerException:
            logger.critical(
                "Failed to produce message to %s", topic,
                extra={"event_id": value.get("event_id")},
            )
            raise

    def _normalize_payload(self, contract: dict[str, Any]) -> dict[str, Any]:
        """Validate payload against schema, transform, and resolve device."""
        payload_input = dict(contract["payload"])
        received_ts = contract["received_at"]
        payload_input["serial_number"] = contract["serial_number"]

        # Get schema version from payload
        schema_version = payload_input.get("schema_version")
        if not schema_version:
            raise RawContractError(
                "payload_validation_failed",
                "Missing field 'schema_version' in payload",
            )

        # Fetch schema from cache/API
        schema = self._cache.get_schema(schema_version)
        if schema is None:
            raise RawContractError(
                "payload_validation_failed",
                f"Active schema version '{schema_version}' not found",
            )

        # Validate and transform
        clean_payload = process_telemetry_payload(
            payload_input, schema, received_ts
        )

        # Resolve device
        device = self._cache.get_device(contract["serial_number"])
        if device is None:
            raise RawContractError(
                "unknown_device",
                f"Device SN: {contract['serial_number']} not found",
            )

        normalized: dict[str, Any] = {
            "device_id": str(device["id"]),
            "payload": clean_payload,
        }

        # Normalize timestamp
        timestamp_val = clean_payload.get("timestamp")
        if timestamp_val is not None:
            if isinstance(timestamp_val, str):
                normalized["payload"]["timestamp"] = timestamp_val
            else:
                normalized["payload"]["timestamp"] = (
                    to_utc(timestamp_val).isoformat()
                )

        return normalized


    @staticmethod
    def _decode_message_value(raw_bytes: bytes | None) -> dict[str, Any]:
        if raw_bytes is None:
            raise RawContractError("empty_payload", "Message payload is empty")
        try:
            parsed = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawContractError("malformed_json", str(exc)) from exc
        if not isinstance(parsed, dict):
            raise RawContractError(
                "invalid_contract", "Message payload must be a JSON object"
            )
        return parsed

    @staticmethod
    def _build_event_id(raw_obj, source_topic, source_partition, source_offset):
        if isinstance(raw_obj, dict):
            req_id = raw_obj.get("request_id")
            idx = raw_obj.get("ingest_index")
            if isinstance(req_id, str) and isinstance(idx, int):
                return f"{req_id}:{idx}"
        return f"{source_topic}:{source_partition}:{source_offset}"

    @staticmethod
    def _resolve_dlq_key(source_key, raw_obj):
        if source_key is not None:
            return source_key
        if (
            isinstance(raw_obj, dict)
            and isinstance(raw_obj.get("serial_number"), str)
            and raw_obj["serial_number"].strip()
        ):
            return raw_obj["serial_number"].encode("utf-8")
        return None

    @staticmethod
    def _decode_key_for_logs(key):
        if key is None:
            return None
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError:
            return key.hex()

    @staticmethod
    def _decode_raw_for_dlq(raw_bytes):
        if raw_bytes is None:
            return None
        try:
            payload_text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return {
                "encoding": "base64",
                "data": base64.b64encode(raw_bytes).decode("ascii"),
            }
        try:
            return json.loads(payload_text)
        except json.JSONDecodeError:
            return {
                "encoding": "utf-8",
                "data": payload_text,
            }

    @staticmethod
    def _ensure_jsonable(value):
        try:
            json.dumps(value, ensure_ascii=False)
            return value
        except (TypeError, ValueError):
            return str(value)
