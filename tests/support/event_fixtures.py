"""Load and execute deterministic synthetic Debezium event fixtures."""

from __future__ import annotations

import base64
import copy
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "consumer" / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from cdc_consumer.migrations import (  # pyright: ignore[reportMissingImports]
    migrate,
    open_database,
)
from cdc_consumer.processing import (  # pyright: ignore[reportMissingImports]
    KafkaRecord,
    ProcessingError,
    process_record,
)

EVENT_ROOT = PROJECT_ROOT / "fixtures" / "events"
ROW_FIELD_TYPES: dict[str, tuple[tuple[str, str], ...]] = {
    "customers": (
        ("id", "int64"),
        ("email", "string"),
        ("full_name", "string"),
        ("status", "string"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
    "orders": (
        ("id", "int64"),
        ("customer_id", "int64"),
        ("status", "string"),
        ("total_amount", "decimal"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
    "payments": (
        ("id", "int64"),
        ("order_id", "int64"),
        ("status", "string"),
        ("amount", "decimal"),
        ("provider_ref", "string"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ),
}
SOURCE_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("version", "string", False),
    ("connector", "string", False),
    ("name", "string", False),
    ("ts_ms", "int64", False),
    ("snapshot", "string", True),
    ("db", "string", False),
    ("sequence", "string", True),
    ("ts_us", "int64", True),
    ("ts_ns", "int64", True),
    ("schema", "string", False),
    ("table", "string", False),
    ("txId", "int64", True),
    ("lsn", "int64", True),
    ("xmin", "int64", True),
    ("origin", "string", True),
    ("origin_lsn", "int64", True),
)
PROJECTION_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": ("email", "full_name", "status", "created_at", "updated_at"),
    "orders": ("customer_id", "status", "total_amount", "created_at", "updated_at"),
    "payments": (
        "order_id",
        "status",
        "amount",
        "provider_ref",
        "created_at",
        "updated_at",
    ),
}


class FixtureError(ValueError):
    """A fixture document is malformed."""


class FixtureAssertionError(AssertionError):
    """Observed processing state does not match the independent fixture oracle."""


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def load_fixture(path: str | Path) -> dict[str, Any]:
    source = Path(path).resolve()
    try:
        source.relative_to(EVENT_ROOT.resolve())
    except ValueError as error:
        raise FixtureError("fixture path must remain under fixtures/events") from error
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureError(f"cannot read fixture JSON: {source}") from error
    if not isinstance(document, dict) or document.get("fixture_version") != 1:
        raise FixtureError("fixture_version must be 1")
    if "base" in document:
        allowed = {"fixture_version", "base", "purpose", "expected_override"}
        if set(document) != allowed:
            raise FixtureError("negative fixture has unexpected fields")
        base_path = (source.parent / document["base"]).resolve()
        try:
            base_path.relative_to(EVENT_ROOT.resolve())
        except ValueError as error:
            raise FixtureError("negative fixture base escapes fixtures/events") from error
        base = load_fixture(base_path)
        override = document.get("expected_override")
        if not isinstance(override, dict) or not override:
            raise FixtureError("negative fixture must define expected_override")
        return _deep_merge(base, {"expected": override})
    required = {
        "fixture_version",
        "ac_id",
        "source",
        "generation",
        "schema_version",
        "records",
        "expected",
    }
    if set(document) != required:
        raise FixtureError("event fixture fields differ from the version-1 contract")
    if document["ac_id"] not in {"AC-005", "AC-006", "AC-012"}:
        raise FixtureError("synthetic event fixture has unsupported AC")
    if document["generation"] != "g001":
        raise FixtureError("synthetic event fixtures must target g001")
    if document["schema_version"] not in {1, 2}:
        raise FixtureError("schema_version must be 1 or 2")
    if not isinstance(document["records"], list) or not document["records"]:
        raise FixtureError("records must be a non-empty array")
    if not isinstance(document["expected"], dict):
        raise FixtureError("expected must be an object")
    return document


def _row_schema_fields(table: str, row: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    field_types = list(ROW_FIELD_TYPES[table])
    if table == "customers" and row is not None and "phone" in row:
        field_types.append(("phone", "string_optional"))
    for name, field_type in field_types:
        field: dict[str, Any] = {
            "field": name,
            "optional": field_type == "string_optional",
        }
        if field_type == "decimal":
            field.update(
                {
                    "type": "bytes",
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {"scale": "2", "connect.decimal.precision": "12"},
                }
            )
        elif field_type == "timestamp":
            field.update(
                {"type": "string", "name": "io.debezium.time.ZonedTimestamp"}
            )
        else:
            field["type"] = "string" if field_type == "string_optional" else field_type
        fields.append(field)
    return fields


def _source_schema_fields() -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for name, field_type, optional in SOURCE_FIELDS:
        field: dict[str, Any] = {
            "field": name,
            "type": field_type,
            "optional": optional,
        }
        if name == "snapshot":
            field.update(
                {
                    "name": "io.debezium.data.Enum",
                    "parameters": {
                        "allowed": (
                            "true,first,first_in_data_collection,"
                            "last_in_data_collection,last,false,incremental"
                        )
                    },
                }
            )
        fields.append(field)
    return fields


def _normalize_fixture_row(
    table: str, row: Mapping[str, Any] | None
) -> dict[str, Any] | None:
    if row is None:
        return None
    normalized = dict(row)
    for name, field_type in ROW_FIELD_TYPES[table]:
        if field_type != "decimal":
            continue
        value = normalized[name]
        if not isinstance(value, str):
            raise FixtureError(f"{table}.{name} decimal fixture must be a string")
        negative = value.startswith("-")
        digits = value.removeprefix("-").replace(".", "")
        if not digits.isdigit():
            raise FixtureError(f"{table}.{name} decimal fixture is invalid")
        unscaled = int(digits) * (-1 if negative else 1)
        size = max(1, (unscaled.bit_length() + 8) // 8)
        normalized[name] = base64.b64encode(
            unscaled.to_bytes(size, "big", signed=True)
        ).decode("ascii")
    return normalized


def _key_bytes(topic: str, entity_id: int) -> bytes:
    document = {
        "schema": {
            "type": "struct",
            "optional": False,
            "name": f"{topic}.Key",
            "fields": [{"field": "id", "type": "int64", "optional": False}],
        },
        "payload": {"id": entity_id},
    }
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


def _record_from_fixture(raw: Mapping[str, Any], generation: str) -> KafkaRecord:
    required_coordinate = {"name", "topic", "partition", "offset", "key_id"}
    if not required_coordinate.issubset(raw):
        raise FixtureError("record is missing its name or Kafka coordinate")
    topic = raw["topic"]
    if not isinstance(topic, str) or not topic.startswith(f"cdc.{generation}.public."):
        raise FixtureError("record topic does not match generation")
    table = topic.rsplit(".", 1)[-1]
    if table not in ROW_FIELD_TYPES:
        raise FixtureError("record topic has an unsupported table")
    key_id = raw["key_id"]
    if not isinstance(key_id, int) or isinstance(key_id, bool) or key_id <= 0:
        raise FixtureError("record key_id must be a positive integer")
    if "raw_value_utf8" in raw:
        value = raw["raw_value_utf8"]
        if not isinstance(value, str):
            raise FixtureError("raw_value_utf8 must be a string")
        return KafkaRecord(
            topic=topic,
            partition=raw["partition"],
            offset=raw["offset"],
            key=_key_bytes(topic, key_id),
            value=value.encode("utf-8"),
            timestamp_ms=1_700_000_000_000 + raw["offset"],
        )
    required_event = {
        "operation",
        "source_lsn",
        "transaction_total_order",
        "transaction_data_collection_order",
        "before",
        "after",
    }
    if not required_event.issubset(raw):
        raise FixtureError("Debezium record is missing event fields")
    operation = raw["operation"]
    if operation not in {"c", "u", "d"}:
        raise FixtureError("fixture operation must be c, u, or d")
    before = _normalize_fixture_row(table, raw["before"])
    after = _normalize_fixture_row(table, raw["after"])
    row_for_schema = after if after is not None else before
    row_fields = _row_schema_fields(table, row_for_schema)
    source_lsn = raw["source_lsn"]
    total_order = raw["transaction_total_order"]
    collection_order = raw["transaction_data_collection_order"]
    source_timestamp = 1_700_000_000_000 + raw["offset"]
    source: dict[str, Any] = {name: None for name, _, _ in SOURCE_FIELDS}
    source.update(
        {
            "version": "3.6.3.Final",
            "connector": "postgresql",
            "name": f"cdc.{generation}",
            "ts_ms": source_timestamp,
            "snapshot": "false",
            "db": "cdc_lab",
            "sequence": f"[null,\"{source_lsn}\"]",
            "ts_us": source_timestamp * 1_000,
            "ts_ns": source_timestamp * 1_000_000,
            "schema": "public",
            "table": table,
            "txId": 42,
            "lsn": source_lsn,
        }
    )
    value = {
        "schema": {
            "type": "struct",
            "optional": False,
            "name": f"{topic}.Envelope",
            "fields": [
                {
                    "field": "before",
                    "type": "struct",
                    "optional": True,
                    "name": f"{topic}.Value",
                    "fields": copy.deepcopy(row_fields),
                },
                {
                    "field": "after",
                    "type": "struct",
                    "optional": True,
                    "name": f"{topic}.Value",
                    "fields": copy.deepcopy(row_fields),
                },
                {
                    "field": "source",
                    "type": "struct",
                    "optional": False,
                    "name": "io.debezium.connector.postgresql.Source",
                    "fields": _source_schema_fields(),
                },
                {
                    "field": "transaction",
                    "type": "struct",
                    "optional": True,
                    "name": "event.block",
                    "fields": [
                        {"field": "id", "type": "string", "optional": False},
                        {
                            "field": "total_order",
                            "type": "int64",
                            "optional": False,
                        },
                        {
                            "field": "data_collection_order",
                            "type": "int64",
                            "optional": False,
                        },
                    ],
                },
                {"field": "op", "type": "string", "optional": False},
                {"field": "ts_ms", "type": "int64", "optional": True},
                {"field": "ts_us", "type": "int64", "optional": True},
                {"field": "ts_ns", "type": "int64", "optional": True},
            ],
        },
        "payload": {
            "before": before,
            "after": after,
            "source": source,
            "transaction": {
                "id": f"fixture:{source_lsn}",
                "total_order": total_order,
                "data_collection_order": collection_order,
            },
            "op": operation,
            "ts_ms": source_timestamp + 1,
            "ts_us": (source_timestamp + 1) * 1_000,
            "ts_ns": (source_timestamp + 1) * 1_000_000,
        },
    }
    return KafkaRecord(
        topic=topic,
        partition=raw["partition"],
        offset=raw["offset"],
        key=_key_bytes(topic, key_id),
        value=json.dumps(value, separators=(",", ":")).encode("utf-8"),
        timestamp_ms=source_timestamp + 2,
    )


def _projection_state(
    connection: sqlite3.Connection, table: str, entity_id: int
) -> dict[str, Any] | None:
    columns = PROJECTION_COLUMNS[table]
    row = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table}_current WHERE id = ?",
        (entity_id,),
    ).fetchone()
    return None if row is None else dict(zip(columns, row, strict=True))


def _observed_state(
    connection: sqlite3.Connection,
    fixture: Mapping[str, Any],
    outcomes: list[dict[str, Any]],
) -> dict[str, Any]:
    records = fixture["records"]
    first = records[0]
    table = first["topic"].rsplit(".", 1)[-1]
    entity_id = first["key_id"]
    watermark_row = connection.execute(
        "SELECT source_table, entity_key, phase_rank, source_lsn, "
        "transaction_total_order, transaction_data_collection_order, last_was_delete "
        "FROM entity_watermarks WHERE generation = ? AND source_table = ? AND entity_key = ?",
        (fixture["generation"], table, str(entity_id)),
    ).fetchone()
    watermark = None
    if watermark_row is not None:
        watermark = dict(
            zip(
                (
                    "table",
                    "entity_key",
                    "phase_rank",
                    "source_lsn",
                    "transaction_total_order",
                    "transaction_data_collection_order",
                    "last_was_delete",
                ),
                watermark_row,
                strict=True,
            )
        )
    raw = [
        {
            "offset": row[0],
            "operation": row[1],
            "processing_status": row[2],
        }
        for row in connection.execute(
            "SELECT offset, operation, processing_status FROM raw_events ORDER BY offset"
        ).fetchall()
    ]
    errors = [
        {"offset": row[0], "error_class": row[1], "attempt_count": row[2]}
        for row in connection.execute(
            "SELECT offset, error_class, attempt_count FROM ingestion_errors ORDER BY offset"
        ).fetchall()
    ]
    projection_row = _projection_state(connection, table, entity_id)
    projection = (
        None
        if projection_row is None
        else {"table": table, "id": entity_id, "row": projection_row}
    )
    return {
        "outcomes": outcomes,
        "projection": projection,
        "watermark": watermark,
        "raw": raw,
        "errors": errors,
    }


def run_fixture(path: str | Path) -> dict[str, Any]:
    fixture = load_fixture(path)
    with tempfile.TemporaryDirectory() as temporary_directory:
        database = Path(temporary_directory) / "fixture.sqlite3"
        migrate(
            database,
            fixture["generation"],
            target_version=fixture["schema_version"],
        )
        connection = open_database(database)
        try:
            outcomes: list[dict[str, Any]] = []
            for raw_record in fixture["records"]:
                record = _record_from_fixture(raw_record, fixture["generation"])
                if "expected_error_class" in raw_record:
                    attempts = raw_record.get("attempts", 1)
                    if not isinstance(attempts, int) or attempts < 1:
                        raise FixtureError("error fixture attempts must be positive")
                    expected_class = raw_record["expected_error_class"]
                    expected_message = raw_record["expected_error_contains"]
                    for _ in range(attempts):
                        try:
                            process_record(connection, fixture["generation"], record)
                        except ProcessingError as error:
                            if type(error).__name__ != expected_class:
                                raise FixtureAssertionError(
                                    f"{raw_record['name']} raised {type(error).__name__}, "
                                    f"expected {expected_class}"
                                ) from error
                            if expected_message not in str(error):
                                raise FixtureAssertionError(
                                    f"{raw_record['name']} error did not contain "
                                    f"{expected_message!r}: {error}"
                                ) from error
                        else:
                            raise FixtureAssertionError(
                                f"{raw_record['name']} unexpectedly processed successfully"
                            )
                    outcomes.append(
                        {
                            "name": raw_record["name"],
                            "error_class": expected_class,
                            "attempt_count": attempts,
                        }
                    )
                else:
                    result = process_record(connection, fixture["generation"], record)
                    outcomes.append(
                        {
                            "name": raw_record["name"],
                            "status": result.status,
                            "duplicate": result.duplicate,
                        }
                    )
            observed = _observed_state(connection, fixture, outcomes)
        finally:
            connection.close()
    if observed != fixture["expected"]:
        raise FixtureAssertionError(
            "fixture oracle mismatch:\n"
            + json.dumps(
                {"expected": fixture["expected"], "observed": observed},
                indent=2,
                sort_keys=True,
            )
        )
    return observed
