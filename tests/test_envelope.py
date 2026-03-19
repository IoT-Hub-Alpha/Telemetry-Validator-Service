"""Tests for envelope validation (ported from monolith validators.py)."""

import pytest
from uuid import uuid4

from validator.services.envelope import (
    RawContractError,
    parse_datetime_iso,
    to_utc,
    validate_raw_contract,
)


def _make_raw_obj(**overrides):
    """Build a valid raw envelope for testing."""
    base = {
        "request_id": str(uuid4()),
        "ingest_protocol": "http",
        "serial_number": "SN-001",
        "payload": {"schema_version": "1.0", "value": 42},
        "received_at": "2026-01-15T12:00:00+00:00",
        "ingest_index": 0,
    }
    base.update(overrides)
    return base


class TestValidateRawContract:
    """Tests for validate_raw_contract."""

    def test_valid_contract(self):
        raw = _make_raw_obj()
        result = validate_raw_contract(raw)

        assert result["request_id"] == raw["request_id"]
        assert result["serial_number"] == "SN-001"
        assert result["ingest_protocol"] == "http"
        assert result["ingest_index"] == 0
        assert result["payload"] == raw["payload"]

    def test_missing_required_field(self):
        raw = _make_raw_obj()
        del raw["serial_number"]

        with pytest.raises(RawContractError) as exc_info:
            validate_raw_contract(raw)
        assert exc_info.value.code == "invalid_contract"

    def test_invalid_request_id(self):
        raw = _make_raw_obj(request_id="not-a-uuid")

        with pytest.raises(RawContractError) as exc_info:
            validate_raw_contract(raw)
        assert exc_info.value.code == "invalid_request_id"

    def test_payload_serial_mismatch(self):
        raw = _make_raw_obj(
            payload={"serial_number": "SN-999", "schema_version": "1.0"}
        )

        with pytest.raises(RawContractError) as exc_info:
            validate_raw_contract(raw)
        assert exc_info.value.code == "payload_serial_mismatch"

    def test_payload_serial_matches(self):
        raw = _make_raw_obj(
            payload={"serial_number": "SN-001", "schema_version": "1.0"}
        )
        result = validate_raw_contract(raw)
        assert result["serial_number"] == "SN-001"

    def test_invalid_received_at(self):
        raw = _make_raw_obj(received_at="not-a-date")

        with pytest.raises(RawContractError) as exc_info:
            validate_raw_contract(raw)
        assert exc_info.value.code in ("invalid_contract", "invalid_received_at")

    def test_negative_ingest_index(self):
        raw = _make_raw_obj(ingest_index=-1)

        with pytest.raises(RawContractError) as exc_info:
            validate_raw_contract(raw)
        assert exc_info.value.code == "invalid_contract"

    def test_received_at_converted_to_utc(self):
        raw = _make_raw_obj(received_at="2026-01-15T14:00:00+02:00")
        result = validate_raw_contract(raw)
        assert "12:00:00" in result["received_at"]
        assert "+00:00" in result["received_at"]


class TestParseDatetimeIso:
    """Tests for parse_datetime_iso."""

    def test_valid_iso(self):
        dt = parse_datetime_iso("2026-01-15T12:00:00+00:00")
        assert dt is not None
        assert dt.year == 2026

    def test_invalid_string(self):
        assert parse_datetime_iso("not-a-date") is None

    def test_none_input(self):
        assert parse_datetime_iso(None) is None


class TestToUtc:
    """Tests for to_utc."""

    def test_naive_datetime(self):
        from datetime import datetime
        dt = datetime(2026, 1, 15, 12, 0, 0)
        result = to_utc(dt)
        assert result.tzinfo is not None

    def test_aware_datetime_converted(self):
        from datetime import datetime, timezone, timedelta
        dt = datetime(2026, 1, 15, 14, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        result = to_utc(dt)
        assert result.hour == 12
