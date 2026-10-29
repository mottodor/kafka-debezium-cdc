"""Validate and apply one Debezium record to the generation-bound SQLite sink."""

from __future__ import annotations

import base64
import binascii
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, NoReturn, Sequence


TABLES = ("customers", "orders", "payments")
INITIAL_SNAPSHOT_MARKERS = {
    "true",
    "first",
    "first_in_data_collection",
    "last_in_data_collection",
    "last",
}
EMPTY_STRING_ALLOWED_FIELDS = {
    ("customers", "email"),
    ("customers", "phone"),
    ("payments", "provider_ref"),
}
ROW_FIELD_TYPES: dict[str, dict[str, str]] = {
    "customers": {
        "id": "int64",
        "email": "string",
        "full_name": "string",
        "status": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "orders": {
        "id": "int64",
        "customer_id": "int64",
        "status": "string",
        "total_amount": "decimal",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "payments": {
        "id": "int64",
        "order_id": "int64",
        "status": "string",
        "amount": "decimal",
        "provider_ref": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
}
SOURCE_FIELD_SPECS: dict[str, tuple[str, bool]] = {
    "version": ("string", False),
    "connector": ("string", False),
    "name": ("string", False),
    "ts_ms": ("int64", False),
    "snapshot": ("string", True),
    "db": ("string", False),
    "sequence": ("string", True),
    "ts_us": ("int64", True),
    "ts_ns": ("int64", True),
    "schema": ("string", False),
    "table": ("string", False),
    "txId": ("int64", True),
    "lsn": ("int64", True),
    "xmin": ("int64", True),
    "origin": ("string", True),
    "origin_lsn": ("int64", True),
}
INT64_MAX = (1 << 63) - 1
ENVELOPE_FIELD_ORDER = (
    "before",
    "after",
    "source",
    "transaction",
    "op",
    "ts_ms",
    "ts_us",
    "ts_ns",
)


class ProcessingError(RuntimeError):
    """A record cannot be durably applied and its Kafka offset must not advance."""


class RecordValidationError(ProcessingError):
    """The key, envelope, metadata, or row does not satisfy the pinned contract."""


class OrderingCollisionError(ProcessingError):
    """A distinct Kafka coordinate reused an existing per-key ordering token."""


class ProjectionWriteError(ProcessingError):
    """Validated row data was rejected by the SQLite projection schema."""


@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()
    timestamp_ms: int | None = None


@dataclass(frozen=True, order=True)
class OrderingToken:
    phase_rank: int
    source_lsn: int
    transaction_total_order: int
    transaction_data_collection_order: int


@dataclass(frozen=True)
class ProcessResult:
    status: Literal["applied", "stale", "tombstone"]
    duplicate: bool
    token: OrderingToken | None


@dataclass(frozen=True)
class _ParsedRecord:
    table: str
    entity_id: int
    operation: Literal["r", "c", "u", "d"] | None
    snapshot_marker: str | None
    source_lsn: int | None
    source_sequence: str | None
    source_tx_id: int | None
    transaction_id: str | None
    transaction_total_order: int | None
    transaction_data_collection_order: int | None
    source_timestamp_ms: int | None
    token: OrderingToken | None
    row: dict[str, Any] | None


PROJECTION_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": ("id", "email", "full_name", "status", "created_at", "updated_at"),
    "orders": (
        "id",
        "customer_id",
        "status",
        "total_amount",
        "created_at",
        "updated_at",
    ),
    "payments": (
        "id",
        "order_id",
        "status",
        "amount",
        "provider_ref",
        "created_at",
        "updated_at",
    ),
}


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _raise(message: str) -> NoReturn:
    raise RecordValidationError(message)


def _require_object(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _raise(f"{description} must be a JSON object")
    return value


def _require_exact_names(
    value: dict[str, Any], expected: set[str], description: str
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        _raise(
            f"{description} fields differ: missing={missing}, unexpected={unexpected}"
        )


def _require_int(
    value: Any,
    description: str,
    *,
    minimum: int = 0,
    optional: bool = False,
) -> int | None:
    if value is None and optional:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > INT64_MAX
    ):
        qualifier = "non-negative" if minimum == 0 else f">= {minimum}"
        _raise(
            f"{description} must be a signed 64-bit integer {qualifier}"
        )
    return value


def _require_nonempty_string(
    value: Any, description: str, *, optional: bool = False
) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        _raise(f"{description} must be a non-empty string")
    return value


def _json_object(raw: bytes, description: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RecordValidationError(
                    f"{description} contains duplicate JSON field {key!r}"
                )
            result[key] = value
        return result

    try:
        decoded = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except ProcessingError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RecordValidationError(f"{description} is not valid UTF-8 JSON") from error
    return _require_object(decoded, description)


def _schema_fields(
    value: Any, expected_order: Sequence[str], description: str
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list):
        _raise(f"{description} must be a JSON array")
    fields: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for index, raw_field in enumerate(value):
        field = _require_object(raw_field, f"{description}[{index}]")
        name = _require_nonempty_string(
            field.get("field"), f"{description}[{index}].field"
        )
        assert name is not None
        if name in fields:
            _raise(f"{description} contains duplicate field {name!r}")
        fields[name] = field
        order.append(name)
    if tuple(order) != tuple(expected_order):
        _raise(f"{description} has unexpected field order {order}")
    return fields


def _validate_basic_field(
    field: dict[str, Any], expected_type: str, description: str
) -> None:
    if field.get("field") is None or field.get("type") != expected_type:
        _raise(f"{description} must have type {expected_type}")


def _validate_row_schema(
    table: str,
    topic: str,
    container: dict[str, Any],
    description: str,
    *,
    customers_phone_enabled: bool,
) -> dict[str, dict[str, Any]]:
    if container.get("type") != "struct" or container.get("optional") is not True:
        _raise(f"{description} must be an optional struct")
    if container.get("name") != f"{topic}.Value":
        _raise(f"{description} has an unexpected row schema name")
    expected = tuple(ROW_FIELD_TYPES[table])
    raw_fields = container.get("fields")
    has_phone = (
        table == "customers"
        and isinstance(raw_fields, list)
        and len(raw_fields) == len(expected) + 1
        and isinstance(raw_fields[-1], dict)
        and raw_fields[-1].get("field") == "phone"
    )
    if has_phone:
        if not customers_phone_enabled:
            _raise("customers.phone requires SQLite migration 0002_customers_phone")
        expected = (*expected, "phone")
    fields = _schema_fields(raw_fields, expected, f"{description}.fields")
    field_types = dict(ROW_FIELD_TYPES[table])
    if has_phone:
        field_types["phone"] = "string"
    for name, field_type in field_types.items():
        field = fields[name]
        expected_optional = name == "phone"
        if field.get("optional") is not expected_optional:
            requirement = "optional" if expected_optional else "non-optional"
            _raise(f"{description}.{name} must be {requirement}")
        if field_type == "decimal":
            _validate_basic_field(field, "bytes", f"{description}.{name}")
            if field.get("name") != "org.apache.kafka.connect.data.Decimal":
                _raise(f"{description}.{name} is not Connect Decimal")
            parameters = _require_object(
                field.get("parameters"), f"{description}.{name}.parameters"
            )
            if parameters.get("scale") != "2" or parameters.get(
                "connect.decimal.precision"
            ) != "12":
                _raise(f"{description}.{name} must be NUMERIC(12,2)")
        elif field_type == "timestamp":
            _validate_basic_field(field, "string", f"{description}.{name}")
            if field.get("name") != "io.debezium.time.ZonedTimestamp":
                _raise(f"{description}.{name} is not ZonedTimestamp")
        else:
            _validate_basic_field(field, field_type, f"{description}.{name}")
            if field.get("name") is not None:
                _raise(f"{description}.{name} has an unexpected named schema")
    return fields


def _validate_source_schema(field: dict[str, Any]) -> None:
    if field.get("type") != "struct" or field.get("optional") is not False:
        _raise("value.schema.source must be a non-optional struct")
    if field.get("name") != "io.debezium.connector.postgresql.Source":
        _raise("value.schema.source has an unexpected schema name")
    fields = _schema_fields(
        field.get("fields"), tuple(SOURCE_FIELD_SPECS), "value.schema.source.fields"
    )
    for name, (field_type, optional) in SOURCE_FIELD_SPECS.items():
        source_field = fields[name]
        _validate_basic_field(source_field, field_type, f"value.schema.source.{name}")
        if source_field.get("optional") is not optional:
            _raise(f"value.schema.source.{name}.optional must be {optional}")
        if name == "snapshot":
            if source_field.get("name") != "io.debezium.data.Enum":
                _raise("value.schema.source.snapshot has an unexpected schema name")
            parameters = _require_object(
                source_field.get("parameters"),
                "value.schema.source.snapshot.parameters",
            )
            if parameters.get("allowed") != (
                "true,first,first_in_data_collection,last_in_data_collection,"
                "last,false,incremental"
            ):
                _raise("value.schema.source.snapshot has unexpected allowed values")
        elif source_field.get("name") is not None:
            _raise(f"value.schema.source.{name} has an unexpected named schema")


def _validate_transaction_schema(field: dict[str, Any]) -> None:
    if field.get("type") != "struct" or field.get("optional") is not True:
        _raise("value.schema.transaction must be an optional struct")
    if field.get("name") != "event.block":
        _raise("value.schema.transaction has an unexpected schema name")
    expected = {
        "id": "string",
        "total_order": "int64",
        "data_collection_order": "int64",
    }
    fields = _schema_fields(
        field.get("fields"), tuple(expected), "value.schema.transaction.fields"
    )
    for name, field_type in expected.items():
        _validate_basic_field(
            fields[name], field_type, f"value.schema.transaction.{name}"
        )
        if fields[name].get("optional") is not False:
            _raise(f"value.schema.transaction.{name} must be non-optional")
        if fields[name].get("name") is not None:
            _raise(f"value.schema.transaction.{name} has an unexpected named schema")


def _validate_schema(
    topic: str,
    table: str,
    key: dict[str, Any],
    value: dict[str, Any],
    *,
    customers_phone_enabled: bool,
) -> dict[str, dict[str, Any]]:
    _require_exact_names(key, {"schema", "payload"}, "key")
    key_schema = _require_object(key.get("schema"), "key.schema")
    if (
        key_schema.get("type") != "struct"
        or key_schema.get("optional") is not False
        or key_schema.get("name") != f"{topic}.Key"
    ):
        _raise(f"unexpected key schema for {topic}")
    key_fields = _schema_fields(key_schema.get("fields"), ("id",), "key.schema.fields")
    id_field = key_fields["id"]
    _validate_basic_field(id_field, "int64", "key.schema.id")
    if id_field.get("optional") is not False or id_field.get("name") is not None:
        _raise("key.schema.id must be a non-optional unnamed int64")

    _require_exact_names(value, {"schema", "payload"}, "value")
    value_schema = _require_object(value.get("schema"), "value.schema")
    if (
        value_schema.get("type") != "struct"
        or value_schema.get("optional") is not False
        or value_schema.get("name") != f"{topic}.Envelope"
    ):
        _raise(f"unexpected value schema for {topic}")
    envelope = _schema_fields(
        value_schema.get("fields"), ENVELOPE_FIELD_ORDER, "value.schema.fields"
    )
    before_fields = _validate_row_schema(
        table,
        topic,
        envelope["before"],
        "value.schema.before",
        customers_phone_enabled=customers_phone_enabled,
    )
    after_fields = _validate_row_schema(
        table,
        topic,
        envelope["after"],
        "value.schema.after",
        customers_phone_enabled=customers_phone_enabled,
    )
    if before_fields != after_fields:
        _raise("before and after row schemas differ")
    _validate_source_schema(envelope["source"])
    _validate_transaction_schema(envelope["transaction"])
    _validate_basic_field(envelope["op"], "string", "value.schema.op")
    if envelope["op"].get("optional") is not False:
        _raise("value.schema.op must be non-optional")
    for name in ("ts_ms", "ts_us", "ts_ns"):
        _validate_basic_field(envelope[name], "int64", f"value.schema.{name}")
        if envelope[name].get("optional") is not True:
            _raise(f"value.schema.{name} must be optional")
    return after_fields


def _require_row_string(
    table: str, name: str, value: Any, description: str
) -> str:
    if not isinstance(value, str):
        _raise(f"{description} must be a string")
    if not value and (table, name) not in EMPTY_STRING_ALLOWED_FIELDS:
        _raise(f"{description} must be a non-empty string")
    if table == "customers" and name == "full_name" and not value.strip():
        _raise(f"{description} must contain a non-whitespace character")
    return value


def _decode_decimal(value: Any, field: dict[str, Any], description: str) -> str:
    if not isinstance(value, str):
        _raise(f"{description} must be a base64 string")
    try:
        encoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as error:
        raise RecordValidationError(f"{description} is not valid base64") from error
    if not encoded:
        _raise(f"{description} has an empty decimal payload")
    unscaled = int.from_bytes(encoded, byteorder="big", signed=True)
    if abs(unscaled) > 999_999_999_999:
        _raise(f"{description} exceeds NUMERIC(12,2)")
    decimal_value = Decimal(unscaled).scaleb(-2)
    return f"{decimal_value:.2f}"


def _normalize_row(
    table: str,
    row: Any,
    row_schema: dict[str, dict[str, Any]],
    entity_id: int,
) -> dict[str, Any]:
    values = _require_object(row, "value.payload.after")
    _require_exact_names(values, set(row_schema), "value.payload.after")
    field_types = dict(ROW_FIELD_TYPES[table])
    if "phone" in row_schema:
        field_types["phone"] = "string"
    normalized: dict[str, Any] = {}
    for name, field_type in field_types.items():
        value = values[name]
        if name == "phone" and value is None:
            normalized[name] = None
        elif field_type == "int64":
            parsed = _require_int(value, f"value.payload.after.{name}", minimum=1)
            assert parsed is not None
            normalized[name] = parsed
        elif field_type == "decimal":
            normalized[name] = _decode_decimal(
                value, row_schema[name], f"value.payload.after.{name}"
            )
        else:
            normalized[name] = _require_row_string(
                table, name, value, f"value.payload.after.{name}"
            )
    if normalized["id"] != entity_id:
        _raise("value.payload.after.id does not match key.payload.id")
    if table == "orders" and Decimal(normalized["total_amount"]) < 0:
        _raise("value.payload.after.total_amount must be non-negative")
    if table == "payments" and Decimal(normalized["amount"]) <= 0:
        _raise("value.payload.after.amount must be positive")
    return normalized


def _validate_before_row(
    table: str,
    row: Any,
    row_schema: dict[str, dict[str, Any]],
    entity_id: int,
    *,
    key_only: bool = False,
) -> None:
    values = _require_object(row, "value.payload.before")
    _require_exact_names(values, set(row_schema), "value.payload.before")
    if key_only:
        before_id = _require_int(values["id"], "value.payload.before.id", minimum=1)
        if before_id != entity_id:
            _raise("value.payload.before.id does not match key.payload.id")
        return
    field_types = dict(ROW_FIELD_TYPES[table])
    if "phone" in row_schema:
        field_types["phone"] = "string"
    for name, field_type in field_types.items():
        value = values[name]
        if value is None:
            if name == "id":
                _raise("value.payload.before.id must not be null")
            continue
        if field_type == "int64":
            _require_int(value, f"value.payload.before.{name}", minimum=1)
        elif field_type == "decimal":
            decimal_value = _decode_decimal(
                value, row_schema[name], f"value.payload.before.{name}"
            )
            if table == "orders" and Decimal(decimal_value) < 0:
                _raise("value.payload.before.total_amount must be non-negative")
            if table == "payments" and Decimal(decimal_value) <= 0:
                _raise("value.payload.before.amount must be positive")
        else:
            _require_row_string(table, name, value, f"value.payload.before.{name}")
    if values["id"] != entity_id:
        _raise("value.payload.before.id does not match key.payload.id")


def _parse_key(topic: str, raw_key: bytes | None) -> tuple[dict[str, Any], int]:
    if raw_key is None or not raw_key:
        _raise("Kafka record must have a non-empty key")
    key = _json_object(raw_key, "key")
    _require_exact_names(key, {"schema", "payload"}, "key")
    key_schema = _require_object(key.get("schema"), "key.schema")
    if (
        key_schema.get("type") != "struct"
        or key_schema.get("optional") is not False
        or key_schema.get("name") != f"{topic}.Key"
    ):
        _raise(f"unexpected key schema for {topic}")
    fields = _schema_fields(key_schema.get("fields"), ("id",), "key.schema.fields")
    _validate_basic_field(fields["id"], "int64", "key.schema.id")
    if fields["id"].get("optional") is not False or fields["id"].get("name") is not None:
        _raise("key.schema.id must be a non-optional unnamed int64")
    payload = _require_object(key.get("payload"), "key.payload")
    _require_exact_names(payload, {"id"}, "key.payload")
    entity_id = _require_int(payload["id"], "key.payload.id", minimum=1)
    assert entity_id is not None
    return key, entity_id


def _validate_coordinate(record: KafkaRecord) -> None:
    if not isinstance(record.topic, str) or not record.topic:
        raise ValueError("Kafka topic must be a non-empty string")
    if isinstance(record.partition, bool) or not isinstance(record.partition, int) or record.partition < 0:
        raise ValueError("Kafka partition must be a non-negative integer")
    if isinstance(record.offset, bool) or not isinstance(record.offset, int) or record.offset < 0:
        raise ValueError("Kafka offset must be a non-negative integer")
    if record.key is not None and not isinstance(record.key, bytes):
        raise ValueError("Kafka key must be bytes or None")
    if record.value is not None and not isinstance(record.value, bytes):
        raise ValueError("Kafka value must be bytes or None")


def _validate_record_metadata(record: KafkaRecord) -> None:
    if record.timestamp_ms is not None:
        _require_int(record.timestamp_ms, "Kafka timestamp", minimum=0)
    for index, header in enumerate(record.headers):
        if not isinstance(header, tuple) or len(header) != 2:
            raise ValueError(f"Kafka header {index} must be a (key, value) tuple")
        key, value = header
        if not isinstance(key, str) or not key:
            raise ValueError(f"Kafka header {index} key must be a non-empty string")
        if value is not None and not isinstance(value, bytes):
            raise ValueError(f"Kafka header {index} value must be bytes or None")


def _table_for_topic(topic: str, generation: str) -> str:
    prefix = f"cdc.{generation}.public."
    if not topic.startswith(prefix):
        _raise(f"unexpected topic {topic!r} for generation {generation}")
    table = topic.removeprefix(prefix)
    if table not in TABLES:
        _raise(f"unexpected source table {table!r}")
    return table


def _parse_record(
    record: KafkaRecord,
    generation: str,
    *,
    customers_phone_enabled: bool,
) -> _ParsedRecord:
    table = _table_for_topic(record.topic, generation)
    key, entity_id = _parse_key(record.topic, record.key)
    if record.value is None:
        return _ParsedRecord(
            table=table,
            entity_id=entity_id,
            operation=None,
            snapshot_marker=None,
            source_lsn=None,
            source_sequence=None,
            source_tx_id=None,
            transaction_id=None,
            transaction_total_order=None,
            transaction_data_collection_order=None,
            source_timestamp_ms=None,
            token=None,
            row=None,
        )

    value = _json_object(record.value, "value")
    row_schema = _validate_schema(
        record.topic,
        table,
        key,
        value,
        customers_phone_enabled=customers_phone_enabled,
    )
    payload = _require_object(value.get("payload"), "value.payload")
    _require_exact_names(payload, set(ENVELOPE_FIELD_ORDER), "value.payload")
    operation = payload.get("op")
    if operation not in {"r", "c", "u", "d"}:
        _raise(f"unsupported Debezium operation {operation!r}")

    source = _require_object(payload.get("source"), "value.payload.source")
    _require_exact_names(source, set(SOURCE_FIELD_SPECS), "value.payload.source")
    if source.get("version") != "3.6.3.Final":
        _raise("value.payload.source.version does not match the pinned connector")
    if source.get("connector") != "postgresql":
        _raise("value.payload.source.connector must be postgresql")
    if source.get("name") != f"cdc.{generation}":
        _raise("value.payload.source.name does not match the generation")
    if source.get("db") != "cdc_lab" or source.get("schema") != "public":
        _raise("value.payload.source database/schema do not match the lab contract")
    if source.get("table") != table:
        _raise("value.payload.source.table does not match the Kafka topic")

    source_timestamp = _require_int(
        source.get("ts_ms"), "value.payload.source.ts_ms", minimum=0
    )
    source_lsn = _require_int(
        source.get("lsn"), "value.payload.source.lsn", minimum=1
    )
    assert source_timestamp is not None and source_lsn is not None
    source_sequence = _require_nonempty_string(
        source.get("sequence"), "value.payload.source.sequence", optional=True
    )
    source_tx_id = _require_int(
        source.get("txId"), "value.payload.source.txId", minimum=0, optional=True
    )
    for name in ("ts_us", "ts_ns", "xmin", "origin_lsn"):
        _require_int(
            source.get(name), f"value.payload.source.{name}", minimum=0, optional=True
        )
    _require_nonempty_string(
        source.get("origin"), "value.payload.source.origin", optional=True
    )
    for name in ("ts_ms", "ts_us", "ts_ns"):
        _require_int(
            payload.get(name), f"value.payload.{name}", minimum=0, optional=True
        )

    snapshot_marker = source.get("snapshot")
    transaction_id: str | None
    transaction_total_order: int
    transaction_data_collection_order: int
    if operation == "r":
        if snapshot_marker == "incremental":
            _raise("incremental snapshots are not supported in an active generation")
        if snapshot_marker not in INITIAL_SNAPSHOT_MARKERS:
            _raise("snapshot/read event has an invalid initial snapshot marker")
        if payload.get("transaction") is not None:
            _raise("initial snapshot event must not contain transaction metadata")
        phase_rank = 0
        transaction_id = None
        transaction_total_order = 0
        transaction_data_collection_order = 0
    else:
        if snapshot_marker != "false":
            _raise("streaming event must have snapshot marker false")
        transaction = _require_object(
            payload.get("transaction"), "value.payload.transaction"
        )
        _require_exact_names(
            transaction,
            {"id", "total_order", "data_collection_order"},
            "value.payload.transaction",
        )
        transaction_id = _require_nonempty_string(
            transaction.get("id"), "value.payload.transaction.id"
        )
        total_order = _require_int(
            transaction.get("total_order"),
            "value.payload.transaction.total_order",
            minimum=1,
        )
        collection_order = _require_int(
            transaction.get("data_collection_order"),
            "value.payload.transaction.data_collection_order",
            minimum=1,
        )
        assert transaction_id is not None
        assert total_order is not None and collection_order is not None
        phase_rank = 1
        transaction_total_order = total_order
        transaction_data_collection_order = collection_order

    before = payload.get("before")
    after = payload.get("after")
    row: dict[str, Any] | None
    if operation in {"r", "c"}:
        if before is not None:
            _raise(f"{operation} event must have null before payload")
        row = _normalize_row(table, after, row_schema, entity_id)
    elif operation == "u":
        if before is not None:
            _validate_before_row(table, before, row_schema, entity_id)
        row = _normalize_row(table, after, row_schema, entity_id)
    else:
        if after is not None:
            _raise("delete event must have null after payload")
        _validate_before_row(table, before, row_schema, entity_id, key_only=True)
        row = None

    return _ParsedRecord(
        table=table,
        entity_id=entity_id,
        operation=operation,
        snapshot_marker=snapshot_marker,
        source_lsn=source_lsn,
        source_sequence=source_sequence,
        source_tx_id=source_tx_id,
        transaction_id=transaction_id,
        transaction_total_order=transaction_total_order,
        transaction_data_collection_order=transaction_data_collection_order,
        source_timestamp_ms=source_timestamp,
        token=OrderingToken(
            phase_rank,
            source_lsn,
            transaction_total_order,
            transaction_data_collection_order,
        ),
        row=row,
    )


def _existing_token_coordinate(
    connection: sqlite3.Connection,
    generation: str,
    parsed: _ParsedRecord,
) -> tuple[str, int, int] | None:
    if parsed.token is None:
        raise RuntimeError("ordering token is required for a Debezium envelope")
    phase_operation = (
        "operation = 'r'"
        if parsed.token.phase_rank == 0
        else "operation IN ('c', 'u', 'd')"
    )
    return connection.execute(
        "SELECT topic, partition, offset FROM raw_events "
        "WHERE generation = ? AND source_table = ? AND entity_key = ? "
        f"AND {phase_operation} AND source_lsn = ? "
        "AND transaction_total_order = ? "
        "AND transaction_data_collection_order = ? LIMIT 1",
        (
            generation,
            parsed.table,
            str(parsed.entity_id),
            parsed.token.source_lsn,
            parsed.token.transaction_total_order,
            parsed.token.transaction_data_collection_order,
        ),
    ).fetchone()


def _existing_token(
    connection: sqlite3.Connection, generation: str, parsed: _ParsedRecord
) -> OrderingToken | None:
    row = connection.execute(
        "SELECT phase_rank, source_lsn, transaction_total_order, "
        "transaction_data_collection_order FROM entity_watermarks "
        "WHERE generation = ? AND source_table = ? AND entity_key = ?",
        (generation, parsed.table, str(parsed.entity_id)),
    ).fetchone()
    return None if row is None else OrderingToken(*row)


def _set_raw_durable_commit_time(
    connection: sqlite3.Connection,
    record: KafkaRecord,
    committed_at: str,
) -> None:
    updated = connection.execute(
        "UPDATE raw_events SET durable_committed_at = ? "
        "WHERE topic = ? AND partition = ? AND offset = ?",
        (committed_at, record.topic, record.partition, record.offset),
    ).rowcount
    if updated != 1:
        raise RuntimeError("durable commit timestamp update did not match one raw event")


def _insert_raw(
    connection: sqlite3.Connection,
    generation: str,
    record: KafkaRecord,
    parsed: _ParsedRecord,
    status: Literal["applied", "stale", "tombstone"],
    committed_at: str,
) -> None:
    if record.key is None:
        raise RuntimeError("validated raw event has no Kafka key")
    connection.execute(
        "INSERT INTO raw_events ("
        "topic, partition, offset, generation, message_key, message_value, "
        "kafka_timestamp_ms, operation, source_table, entity_key, snapshot_marker, "
        "source_lsn, source_sequence, source_tx_id, transaction_id, "
        "transaction_total_order, transaction_data_collection_order, "
        "source_timestamp_ms, processing_status, durable_committed_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            record.topic,
            record.partition,
            record.offset,
            generation,
            sqlite3.Binary(record.key),
            None if record.value is None else sqlite3.Binary(record.value),
            record.timestamp_ms,
            parsed.operation,
            parsed.table,
            str(parsed.entity_id),
            parsed.snapshot_marker,
            parsed.source_lsn,
            parsed.source_sequence,
            parsed.source_tx_id,
            parsed.transaction_id,
            parsed.transaction_total_order,
            parsed.transaction_data_collection_order,
            parsed.source_timestamp_ms,
            status,
            committed_at,
        ),
    )
    connection.executemany(
        "INSERT INTO raw_headers "
        "(topic, partition, offset, header_index, header_key, header_value) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                record.topic,
                record.partition,
                record.offset,
                index,
                key,
                None if value is None else sqlite3.Binary(value),
            )
            for index, (key, value) in enumerate(record.headers)
        ],
    )


def _write_projection(
    connection: sqlite3.Connection,
    generation: str,
    parsed: _ParsedRecord,
) -> None:
    if parsed.operation == "d":
        connection.execute(
            f"DELETE FROM {parsed.table}_current WHERE id = ?",
            (parsed.entity_id,),
        )
        return
    if parsed.row is None:
        raise RuntimeError("non-delete event has no projection row")
    columns = PROJECTION_COLUMNS[parsed.table]
    if parsed.table == "customers" and "phone" in parsed.row:
        columns = (*columns, "phone")
    insert_columns = ("generation", *columns)
    placeholders = ", ".join("?" for _ in insert_columns)
    updates = ", ".join(
        f"{column} = excluded.{column}" for column in insert_columns if column != "id"
    )
    connection.execute(
        f"INSERT INTO {parsed.table}_current ({', '.join(insert_columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {updates}",
        (generation, *(parsed.row[column] for column in columns)),
    )


def _write_watermark(
    connection: sqlite3.Connection,
    generation: str,
    parsed: _ParsedRecord,
    committed_at: str,
) -> None:
    if parsed.token is None:
        raise RuntimeError("watermark write requires an ordering token")
    connection.execute(
        "INSERT INTO entity_watermarks ("
        "generation, source_table, entity_key, phase_rank, source_lsn, "
        "transaction_total_order, transaction_data_collection_order, "
        "last_was_delete, updated_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(generation, source_table, entity_key) DO UPDATE SET "
        "phase_rank = excluded.phase_rank, source_lsn = excluded.source_lsn, "
        "transaction_total_order = excluded.transaction_total_order, "
        "transaction_data_collection_order = excluded.transaction_data_collection_order, "
        "last_was_delete = excluded.last_was_delete, updated_at = excluded.updated_at",
        (
            generation,
            parsed.table,
            str(parsed.entity_id),
            parsed.token.phase_rank,
            parsed.token.source_lsn,
            parsed.token.transaction_total_order,
            parsed.token.transaction_data_collection_order,
            1 if parsed.operation == "d" else 0,
            committed_at,
        ),
    )


def _clear_ingestion_error(
    connection: sqlite3.Connection,
    record: KafkaRecord,
) -> None:
    connection.execute(
        "DELETE FROM ingestion_errors "
        "WHERE topic = ? AND partition = ? AND offset = ?",
        (record.topic, record.partition, record.offset),
    )


def _record_ingestion_error(
    connection: sqlite3.Connection,
    generation: str,
    record: KafkaRecord,
    error: ProcessingError,
    observed_at: str,
) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO ingestion_errors ("
            "topic, partition, offset, generation, message_key, message_value, "
            "kafka_timestamp_ms, error_class, error_message, first_seen_at, "
            "last_seen_at, attempt_count"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1) "
            "ON CONFLICT(topic, partition, offset) DO UPDATE SET "
            "message_key = excluded.message_key, message_value = excluded.message_value, "
            "kafka_timestamp_ms = excluded.kafka_timestamp_ms, "
            "error_class = excluded.error_class, error_message = excluded.error_message, "
            "last_seen_at = excluded.last_seen_at, attempt_count = attempt_count + 1",
            (
                record.topic,
                record.partition,
                record.offset,
                generation,
                None if record.key is None else sqlite3.Binary(record.key),
                None if record.value is None else sqlite3.Binary(record.value),
                record.timestamp_ms,
                type(error).__name__,
                str(error),
                observed_at,
                observed_at,
            ),
        )
        connection.execute("COMMIT")
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise


def process_record(
    connection: sqlite3.Connection,
    generation: str,
    record: KafkaRecord,
    *,
    observed_at: str | None = None,
) -> ProcessResult:
    """Apply one record atomically or persist an ingestion error and fail-stop.

    The caller may commit the corresponding Kafka offset only after this function
    returns successfully. A raised ``ProcessingError`` means the offset must not move.
    """

    _validate_coordinate(record)
    if not isinstance(generation, str) or not generation:
        raise ValueError("generation must be a non-empty string")
    if connection.in_transaction:
        raise RuntimeError("process_record requires a connection with no active transaction")
    binding = connection.execute(
        "SELECT generation FROM sink_metadata WHERE singleton = 1"
    ).fetchone()
    if binding is None:
        raise RuntimeError("SQLite sink has no generation binding")
    if binding[0] != generation:
        raise ValueError(
            "SQLite sink belongs to a different generation: "
            f"expected {generation}, found {binding[0]}"
        )
    timestamp = observed_at or _utc_now()
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError("observed_at must be a non-empty string")

    try:
        connection.execute("BEGIN IMMEDIATE")
        duplicate = connection.execute(
            "SELECT processing_status FROM raw_events "
            "WHERE topic = ? AND partition = ? AND offset = ?",
            (record.topic, record.partition, record.offset),
        ).fetchone()
        if duplicate is not None:
            connection.execute(
                "UPDATE raw_events SET delivery_count = delivery_count + 1 "
                "WHERE topic = ? AND partition = ? AND offset = ?",
                (record.topic, record.partition, record.offset),
            )
            _clear_ingestion_error(connection, record)
            connection.execute("COMMIT")
            return ProcessResult(duplicate[0], True, None)

        _validate_record_metadata(record)
        phone_migration = connection.execute(
            "SELECT 1 FROM schema_migrations "
            "WHERE version = 2 AND name = 'customers_phone'"
        ).fetchone()
        phone_column = any(
            row[1] == "phone"
            for row in connection.execute("PRAGMA table_info(customers_current)")
        )
        if (phone_migration is not None) != phone_column:
            raise RuntimeError(
                "SQLite customers.phone column and migration history are inconsistent"
            )
        parsed = _parse_record(
            record,
            generation,
            customers_phone_enabled=phone_column,
        )
        if parsed.token is None:
            _insert_raw(connection, generation, record, parsed, "tombstone", timestamp)
            _set_raw_durable_commit_time(
                connection, record, observed_at or _utc_now()
            )
            _clear_ingestion_error(connection, record)
            connection.execute("COMMIT")
            return ProcessResult("tombstone", False, None)

        equal_coordinate = _existing_token_coordinate(connection, generation, parsed)
        if equal_coordinate is not None:
            raise OrderingCollisionError(
                "distinct Kafka coordinate has the same ordering token for "
                f"{parsed.table}/{parsed.entity_id}; first seen at "
                f"{equal_coordinate[0]}:{equal_coordinate[1]}:{equal_coordinate[2]}"
            )
        previous = _existing_token(connection, generation, parsed)
        status: Literal["applied", "stale"] = (
            "stale" if previous is not None and parsed.token < previous else "applied"
        )
        _insert_raw(connection, generation, record, parsed, status, timestamp)
        if status == "applied":
            try:
                if parsed.operation == "d":
                    _write_watermark(connection, generation, parsed, timestamp)
                    _write_projection(connection, generation, parsed)
                else:
                    _write_projection(connection, generation, parsed)
                    _write_watermark(connection, generation, parsed, timestamp)
            except sqlite3.IntegrityError as error:
                raise ProjectionWriteError(
                    f"SQLite projection rejected {parsed.table}/{parsed.entity_id}: {error}"
                ) from error
        _set_raw_durable_commit_time(
            connection, record, observed_at or _utc_now()
        )
        _clear_ingestion_error(connection, record)
        connection.execute("COMMIT")
        return ProcessResult(status, False, parsed.token)
    except ProcessingError as error:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        _record_ingestion_error(connection, generation, record, error, timestamp)
        raise
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
