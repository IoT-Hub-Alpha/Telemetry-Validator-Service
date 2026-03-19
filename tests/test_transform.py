"""Tests for transformation engine (ported from monolith services.py)."""

import pytest

from validator.services.envelope import RawContractError
from validator.services.transform import apply_transformations, process_telemetry_payload


class TestApplyTransformations:
    """Tests for apply_transformations."""

    def test_rename(self):
        data = {"val": 42, "other": "ok"}
        rules = {"rename": {"val": "temperature"}}
        result = apply_transformations(data, rules)
        assert result == {"temperature": 42, "other": "ok"}

    def test_multiply(self):
        data = {"temperature": 10}
        rules = {"multiply": {"temperature": 2}}
        result = apply_transformations(data, rules)
        assert result["temperature"] == 20

    def test_divide(self):
        data = {"value": 100}
        rules = {"divide": {"value": 4}}
        result = apply_transformations(data, rules)
        assert result["value"] == 25.0

    def test_divide_by_zero_skipped(self):
        data = {"value": 100}
        rules = {"divide": {"value": 0}}
        result = apply_transformations(data, rules)
        assert result["value"] == 100

    def test_round(self):
        data = {"value": 3.14159}
        rules = {"round": {"value": 2}}
        result = apply_transformations(data, rules)
        assert result["value"] == 3.14

    def test_remove(self):
        data = {"keep": 1, "drop": 2}
        rules = {"remove": {"drop": True}}
        result = apply_transformations(data, rules)
        assert "drop" not in result
        assert result["keep"] == 1

    def test_remove_false_keeps_key(self):
        data = {"keep": 1}
        rules = {"remove": {"keep": False}}
        result = apply_transformations(data, rules)
        assert "keep" in result

    def test_timestamp_replacement(self):
        data = {"value": 1}
        rules = {"timestamp": True}
        result = apply_transformations(data, rules, received_ts="2026-01-15T12:00:00Z")
        assert result["timestamp"] == "2026-01-15T12:00:00Z"

    def test_combined_rules(self):
        data = {"val": 1000, "drop_me": "x"}
        rules = {
            "rename": {"val": "temperature"},
            "divide": {"temperature": 10},
            "round": {"temperature": 1},
            "remove": {"drop_me": True},
        }
        result = apply_transformations(data, rules)
        assert result == {"temperature": 100.0}

    def test_non_numeric_skipped_for_multiply(self):
        data = {"value": "not_a_number"}
        rules = {"multiply": {"value": 2}}
        result = apply_transformations(data, rules)
        assert result["value"] == "not_a_number"

    def test_missing_key_skipped(self):
        data = {"other": 1}
        rules = {"multiply": {"missing_key": 2}}
        result = apply_transformations(data, rules)
        assert result == {"other": 1}

    def test_empty_rules(self):
        data = {"a": 1, "b": 2}
        result = apply_transformations(data, {})
        assert result == {"a": 1, "b": 2}

    def test_original_data_not_mutated(self):
        data = {"val": 42}
        rules = {"rename": {"val": "temperature"}}
        apply_transformations(data, rules)
        assert "val" in data


class TestProcessTelemetryPayload:
    """Tests for process_telemetry_payload."""

    @pytest.fixture
    def schema(self):
        return {
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
        }

    def test_valid_payload(self, schema):
        payload = {"schema_version": "1.0", "serial_number": "SN-001", "value": 42}
        result = process_telemetry_payload(payload, schema)
        assert "temperature" in result
        assert result["temperature"] == 42

    def test_invalid_payload_raises(self, schema):
        payload = {"schema_version": "1.0", "serial_number": "SN-001"}
        with pytest.raises(RawContractError) as exc_info:
            process_telemetry_payload(payload, schema)
        assert exc_info.value.code == "payload_validation_failed"

    def test_transformation_with_received_ts(self, schema):
        schema_with_ts = {
            "validation_schema": schema["validation_schema"],
            "transformation_rules": {"timestamp": True},
        }
        payload = {"schema_version": "1.0", "serial_number": "SN-001", "value": 1}
        result = process_telemetry_payload(
            payload, schema_with_ts, received_ts="2026-01-15T12:00:00Z"
        )
        assert result["timestamp"] == "2026-01-15T12:00:00Z"
