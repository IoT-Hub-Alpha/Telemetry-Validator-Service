"""Raw contract envelope validation."""

from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any
from uuid import UUID

from jsonschema import validate, ValidationError

ENVELOPE_SCHEMA = {
    "type": "object",
    "properties": {
        "request_id": {"type": "string", "minLength": 1},
        "ingest_protocol": {"type": "string", "minLength": 1},
        "serial_number": {"type": "string", "minLength": 1},
        "payload": {"type": "object"},
        "received_at": {"type": "string", "format": "date-time"},
        "ingest_index": {"type": "integer", "minimum": 0},
    },
    "required": [
        "request_id",
        "ingest_protocol",
        "serial_number",
        "payload",
        "received_at",
        "ingest_index",
    ],
}


class RawContractError(Exception):
    """Raised when telemetry.raw message contract is invalid."""

    def __init__(self, code: str, detail: Any):
        super().__init__(str(detail))
        self.code = code
        self.detail = detail


def parse_datetime_iso(value: str) -> datetime | None:
    """Parse ISO 8601 datetime string without Django dependency."""
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None


def to_utc(value: datetime) -> datetime:
    """Convert datetime to UTC. Makes naive datetimes UTC-aware."""
    if value.tzinfo is None:
        return value.replace(tzinfo=dt_timezone.utc)
    return value.astimezone(dt_timezone.utc)


def validate_raw_contract(raw_obj: dict[str, Any]) -> dict[str, Any]:
    """
    Validate the external contract of a message from telemetry.raw.

    Args:
        raw_obj: Decoded JSON object from Kafka message.

    Returns:
        Validated contract dict with normalized fields.

    Raises:
        RawContractError: If the contract is invalid.
    """
    try:
        validate(instance=raw_obj, schema=ENVELOPE_SCHEMA)
    except ValidationError as e:
        error_field = e.json_path.replace("$.", "") if e.json_path != "$" else "root"
        raise RawContractError(
            "invalid_contract", f"Schema error at '{error_field}': {e.message}"
        )

    request_id = raw_obj["request_id"]
    try:
        UUID(request_id)
    except ValueError as exc:
        raise RawContractError("invalid_request_id", str(exc)) from exc

    serial_number = raw_obj["serial_number"]
    payload = raw_obj["payload"]

    payload_serial = payload.get("serial_number")
    if payload_serial and payload_serial != serial_number:
        raise RawContractError(
            "payload_serial_mismatch",
            "payload.serial_number must match top-level serial_number",
        )

    received_at_dt = parse_datetime_iso(raw_obj["received_at"])
    if received_at_dt is None:
        raise RawContractError(
            "invalid_received_at", "received_at must be ISO 8601 datetime string"
        )

    received_at_dt = to_utc(received_at_dt)

    return {
        "request_id": request_id,
        "ingest_protocol": raw_obj["ingest_protocol"],
        "serial_number": serial_number,
        "payload": payload,
        "received_at": received_at_dt.isoformat(),
        "ingest_index": raw_obj["ingest_index"],
    }
