"""Transformation engine (rename, multiply, divide, round, remove, timestamp).

Ported from monolith: backend/apps/telemetry/services.py
"""

from __future__ import annotations

from typing import Any

from jsonschema import validate
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from validator.services.envelope import RawContractError


def apply_transformations(
    data: dict, rules: dict, received_ts: str | None = None
) -> dict:
    """
    Apply transformations based on provided rules.

    Supported rules:
    - rename: {"old_key": "new_key"}
    - multiply: {"key": factor}
    - divide: {"key": divisor}
    - round: {"key": decimals}
    - remove: {"key": true}
    - timestamp: replaces timestamp with received_ts
    """
    result = data.copy()

    rename_rules = rules.get("rename", {})
    if rename_rules:
        intermediate_map = {}
        for old_key, new_key in rename_rules.items():
            if old_key in result:
                intermediate_map[new_key] = result.pop(old_key)

        result.update(intermediate_map)

    for key, factor in rules.get("multiply", {}).items():
        if key in result and isinstance(result[key], (int, float)):
            result[key] = result[key] * factor

    for key, divisor in rules.get("divide", {}).items():
        if key in result and isinstance(result[key], (int, float)):
            if divisor != 0:
                result[key] = result[key] / divisor

    for key, decimals in rules.get("round", {}).items():
        if key in result and isinstance(result[key], (int, float)):
            result[key] = round(result[key], decimals)

    for key, remover in rules.get("remove", {}).items():
        if key in result and remover:
            result.pop(key, None)

    if "timestamp" in rules.keys():
        result["timestamp"] = received_ts

    return result


def process_telemetry_payload(
    raw_payload: dict[str, Any],
    schema: dict[str, Any],
    received_ts: str | None = None,
) -> dict[str, Any]:
    """
    Validate payload against TelemetrySchema and apply transformations.

    Args:
        raw_payload: The telemetry payload dict.
        schema: Dict with 'validation_schema' and 'transformation_rules' keys.
        received_ts: ISO 8601 timestamp string from the envelope.

    Returns:
        Transformed (clean) payload.

    Raises:
        RawContractError: If validation or transformation fails.
    """
    try:
        validate(instance=raw_payload, schema=schema["validation_schema"])
    except JsonSchemaValidationError as e:
        error_msg = f"Validation error: field {e.json_path} -> {e.message}"
        raise RawContractError("payload_validation_failed", error_msg)

    try:
        clean_data = apply_transformations(
            raw_payload, schema.get("transformation_rules", {}), received_ts
        )
        return clean_data
    except Exception as e:
        raise RawContractError(
            "transformation_failed", f"Error applying transformations: {str(e)}"
        )
