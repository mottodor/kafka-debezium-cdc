#!/usr/bin/env python3
"""Verify the exact generation g001 Debezium snapshot and streaming records."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, NoReturn


INITIAL_SNAPSHOT_MARKERS = {
    "true",
    "first",
    "first_in_data_collection",
    "last_in_data_collection",
    "last",
}
TABLES = ("customers", "orders", "payments")
GENERATION_ENV = {
    "CDC_GENERATION_ID": "g001",
    "KAFKA_TOPIC_PREFIX": "cdc.g001",
    "KAFKA_CUSTOMERS_TOPIC": "cdc.g001.public.customers",
    "KAFKA_ORDERS_TOPIC": "cdc.g001.public.orders",
    "KAFKA_PAYMENTS_TOPIC": "cdc.g001.public.payments",
    "POSTGRES_DATABASE": "cdc_lab",
    "POSTGRES_SCHEMA": "public",
    "DEBEZIUM_CONNECTOR_NAME": "postgres-cdc-g001",
    "POSTGRES_REPLICATION_SLOT": "postgres_cdc_g001",
    "POSTGRES_PUBLICATION": "postgres_cdc_g001_pub",
}
TOPICS = {
    "customers": GENERATION_ENV["KAFKA_CUSTOMERS_TOPIC"],
    "orders": GENERATION_ENV["KAFKA_ORDERS_TOPIC"],
    "payments": GENERATION_ENV["KAFKA_PAYMENTS_TOPIC"],
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
ENVELOPE_FIELDS = set(ENVELOPE_FIELD_ORDER)
SNAPSHOT_ROWS: dict[str, dict[int, dict[str, Any]]] = {
    "customers": {
        1: {
            "email": "alice@example.test",
            "full_name": "Alice Adams",
            "status": "active",
        },
        2: {
            "email": "bob@example.test",
            "full_name": "Bob Brown",
            "status": "active",
        },
    },
    "orders": {
        101: {"customer_id": 1, "status": "pending", "total_amount": "120.50"},
        102: {"customer_id": 2, "status": "paid", "total_amount": "49.99"},
    },
    "payments": {
        1001: {
            "order_id": 102,
            "status": "captured",
            "amount": "49.99",
            "provider_ref": "pay-1001",
        }
    },
}
STREAMING_ROWS: dict[str, dict[int, list[tuple[str, dict[str, Any]]]]] = {
    "customers": {
        1: [
            ("r", SNAPSHOT_ROWS["customers"][1]),
            (
                "u",
                {
                    "email": "alice@example.test",
                    "full_name": "Alice Cooper",
                    "status": "inactive",
                },
            ),
        ],
        2: [("r", SNAPSHOT_ROWS["customers"][2])],
        3: [
            (
                "c",
                {
                    "email": "carol@example.test",
                    "full_name": "Carol Clark",
                    "status": "active",
                },
            )
        ],
    },
    "orders": {
        101: [("r", SNAPSHOT_ROWS["orders"][101])],
        102: [("r", SNAPSHOT_ROWS["orders"][102])],
        103: [
            ("c", {"customer_id": 3, "status": "pending", "total_amount": "15.00"}),
            ("u", {"customer_id": 3, "status": "paid", "total_amount": "15.00"}),
        ],
    },
    "payments": {
        1001: [("r", SNAPSHOT_ROWS["payments"][1001])],
        1002: [
            (
                "c",
                {
                    "order_id": 103,
                    "status": "authorized",
                    "amount": "15.00",
                    "provider_ref": "pay-1002",
                },
            ),
            (
                "u",
                {
                    "order_id": 103,
                    "status": "captured",
                    "amount": "15.00",
                    "provider_ref": "pay-1002",
                },
            ),
        ],
    },
}
EXPECTED_TOTALS = {"snapshot": 5, "streaming": 11}


class ValidationError(RuntimeError):
    """The observed records do not satisfy the R-02 contract."""


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: Any
    value: Any


PartitionKey = tuple[str, int]
Watermarks = dict[PartitionKey, dict[str, int]]


def fail(message: str, code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def require_object(value: Any, description: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{description} must be a JSON object")
    return value


def require_positive_int(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationError(f"{description} must be a positive integer")
    return value


def validate_generation_environment(require_all: bool) -> None:
    missing: list[str] = []
    mismatches: dict[str, dict[str, str]] = {}
    for name, expected in GENERATION_ENV.items():
        actual = os.environ.get(name)
        if actual is None:
            if require_all:
                missing.append(name)
        elif actual != expected:
            mismatches[name] = {"expected": expected, "actual": actual}
    if missing:
        raise ValidationError(
            f"live g001 verification requires environment variables: {sorted(missing)}"
        )
    if mismatches:
        raise ValidationError(f"generation environment mismatch: {mismatches}")


def topic_contract() -> dict[str, str]:
    validate_generation_environment(require_all=False)
    return dict(TOPICS)


def decode_json(raw: bytes | None, description: str) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"{description} is not valid JSON: {error}") from error


def schema_fields(
    value: Any, expected_order: Iterable[str], description: str
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list):
        raise ValidationError(f"{description} must be a list")
    result: dict[str, dict[str, Any]] = {}
    actual_order: list[str] = []
    for index, item in enumerate(value):
        field = require_object(item, f"{description}[{index}]")
        name = field.get("field")
        if not isinstance(name, str) or not name:
            raise ValidationError(f"{description}[{index}].field must be non-empty")
        if name in result:
            raise ValidationError(f"{description} contains duplicate field {name}")
        actual_order.append(name)
        result[name] = field
    expected = list(expected_order)
    if actual_order != expected:
        raise ValidationError(
            f"{description} order mismatch: expected {expected}, got {actual_order}"
        )
    return result


def require_exact_names(
    actual: dict[str, Any], expected: AbstractSet[str], description: str
) -> None:
    if set(actual) != expected:
        raise ValidationError(
            f"{description} fields mismatch: expected {sorted(expected)}, got {sorted(actual)}"
        )


def validate_basic_field(
    field: dict[str, Any], expected_type: str, description: str
) -> None:
    if field.get("type") != expected_type:
        raise ValidationError(
            f"{description}.type must be {expected_type}, got {field.get('type')!r}"
        )


def validate_row_schema(
    table: str, topic: str, container: dict[str, Any], description: str
) -> dict[str, dict[str, Any]]:
    if container.get("type") != "struct" or container.get("optional") is not True:
        raise ValidationError(f"{description} must be an optional struct")
    if container.get("name") != f"{topic}.Value":
        raise ValidationError(f"{description} has unexpected row schema name")
    fields = schema_fields(
        container.get("fields"), ROW_FIELD_TYPES[table], f"{description}.fields"
    )
    require_exact_names(fields, set(ROW_FIELD_TYPES[table]), f"{description}.fields")
    for name, field_type in ROW_FIELD_TYPES[table].items():
        field = fields[name]
        if field.get("optional") is not False:
            raise ValidationError(f"{description}.{name} must be non-optional")
        if field_type == "decimal":
            validate_basic_field(field, "bytes", f"{description}.{name}")
            if field.get("name") != "org.apache.kafka.connect.data.Decimal":
                raise ValidationError(f"{description}.{name} is not Connect Decimal")
            parameters = require_object(
                field.get("parameters"), f"{description}.{name}.parameters"
            )
            if parameters.get("scale") != "2" or parameters.get(
                "connect.decimal.precision"
            ) != "12":
                raise ValidationError(
                    f"{description}.{name} must be NUMERIC(12,2)"
                )
        elif field_type == "timestamp":
            validate_basic_field(field, "string", f"{description}.{name}")
            if field.get("name") != "io.debezium.time.ZonedTimestamp":
                raise ValidationError(f"{description}.{name} is not ZonedTimestamp")
        else:
            validate_basic_field(field, field_type, f"{description}.{name}")
    return fields


def validate_source_schema(field: dict[str, Any]) -> None:
    if field.get("type") != "struct" or field.get("optional") is not False:
        raise ValidationError("value.schema.source must be a non-optional struct")
    if field.get("name") != "io.debezium.connector.postgresql.Source":
        raise ValidationError("value.schema.source has unexpected schema name")
    fields = schema_fields(
        field.get("fields"), SOURCE_FIELD_SPECS, "value.schema.source.fields"
    )
    for name, (field_type, optional) in SOURCE_FIELD_SPECS.items():
        source_field = fields[name]
        validate_basic_field(
            source_field, field_type, f"value.schema.source.{name}"
        )
        if source_field.get("optional") is not optional:
            raise ValidationError(
                f"value.schema.source.{name}.optional must be {optional}"
            )
        if name == "snapshot":
            if source_field.get("name") != "io.debezium.data.Enum":
                raise ValidationError("value.schema.source.snapshot has unexpected name")
            parameters = require_object(
                source_field.get("parameters"),
                "value.schema.source.snapshot.parameters",
            )
            if parameters.get("allowed") != (
                "true,first,first_in_data_collection,last_in_data_collection,"
                "last,false,incremental"
            ):
                raise ValidationError(
                    "value.schema.source.snapshot has unexpected allowed values"
                )
        elif source_field.get("name") is not None:
            raise ValidationError(
                f"value.schema.source.{name} has unexpected named schema"
            )


def validate_transaction_schema(field: dict[str, Any]) -> None:
    if field.get("type") != "struct" or field.get("optional") is not True:
        raise ValidationError("value.schema.transaction must be an optional struct")
    if field.get("name") != "event.block":
        raise ValidationError("value.schema.transaction has unexpected schema name")
    expected = {
        "id": "string",
        "total_order": "int64",
        "data_collection_order": "int64",
    }
    fields = schema_fields(
        field.get("fields"), expected, "value.schema.transaction.fields"
    )
    require_exact_names(fields, set(expected), "value.schema.transaction.fields")
    for name, field_type in expected.items():
        validate_basic_field(
            fields[name], field_type, f"value.schema.transaction.{name}"
        )
        if fields[name].get("optional") is not False:
            raise ValidationError(
                f"value.schema.transaction.{name} must be non-optional"
            )


def validate_schema(
    record: Record, table: str, key: dict[str, Any], value: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    key_schema = require_object(key.get("schema"), "key.schema")
    key_payload = require_object(key.get("payload"), "key.payload")
    if (
        key_schema.get("type") != "struct"
        or key_schema.get("optional") is not False
        or key_schema.get("name") != f"{record.topic}.Key"
    ):
        raise ValidationError(f"unexpected key schema for {record.topic}")
    key_fields = schema_fields(
        key_schema.get("fields"), ("id",), "key.schema.fields"
    )
    require_exact_names(key_fields, {"id"}, "key.schema.fields")
    validate_basic_field(key_fields["id"], "int64", "key.schema.id")
    if key_fields["id"].get("optional") is not False:
        raise ValidationError("key.schema.id must be non-optional")
    require_exact_names(key_payload, {"id"}, "key.payload")
    if isinstance(key_payload["id"], bool) or not isinstance(key_payload["id"], int):
        raise ValidationError(f"{record.topic} key payload id must be an integer")

    value_schema = require_object(value.get("schema"), "value.schema")
    if (
        value_schema.get("type") != "struct"
        or value_schema.get("optional") is not False
        or value_schema.get("name") != f"{record.topic}.Envelope"
    ):
        raise ValidationError(f"unexpected value schema for {record.topic}")
    envelope = schema_fields(
        value_schema.get("fields"), ENVELOPE_FIELD_ORDER, "value.schema.fields"
    )
    before_fields = validate_row_schema(
        table, record.topic, envelope["before"], "value.schema.before"
    )
    after_fields = validate_row_schema(
        table, record.topic, envelope["after"], "value.schema.after"
    )
    if before_fields != after_fields:
        raise ValidationError("before and after row schemas differ")
    validate_source_schema(envelope["source"])
    validate_transaction_schema(envelope["transaction"])
    validate_basic_field(envelope["op"], "string", "value.schema.op")
    if envelope["op"].get("optional") is not False:
        raise ValidationError("value.schema.op must be non-optional")
    for name in ("ts_ms", "ts_us", "ts_ns"):
        validate_basic_field(envelope[name], "int64", f"value.schema.{name}")
        if envelope[name].get("optional") is not True:
            raise ValidationError(f"value.schema.{name} must be optional")
    return after_fields


def decode_decimal(value: Any, field: dict[str, Any], description: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{description} must be a base64 string")
    try:
        encoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValidationError(f"{description} is not valid base64") from error
    if not encoded:
        raise ValidationError(f"{description} has an empty decimal payload")
    parameters = require_object(field.get("parameters"), f"{description}.parameters")
    scale = 2
    unscaled = int.from_bytes(encoded, byteorder="big", signed=True)
    decimal_value = Decimal(unscaled).scaleb(-scale)
    return f"{decimal_value:.{scale}f}"


def normalize_after(
    table: str, after: dict[str, Any], row_schema: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    require_exact_names(after, set(ROW_FIELD_TYPES[table]), "value.payload.after")
    normalized: dict[str, Any] = {}
    for name, field_type in ROW_FIELD_TYPES[table].items():
        value = after[name]
        if field_type == "int64":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationError(f"value.payload.after.{name} must be an integer")
            normalized[name] = value
        elif field_type == "decimal":
            normalized[name] = decode_decimal(
                value, row_schema[name], f"value.payload.after.{name}"
            )
        else:
            if not isinstance(value, str) or not value:
                raise ValidationError(
                    f"value.payload.after.{name} must be a non-empty string"
                )
            if field_type != "timestamp":
                normalized[name] = value
    return normalized


def validate_event(record: Record, table: str) -> tuple[int, str, dict[str, Any]]:
    key = require_object(record.key, "record key")
    value = require_object(record.value, "record value")
    row_schema = validate_schema(record, table, key, value)

    key_id = require_object(key["payload"], "key.payload")["id"]
    payload = require_object(value["payload"], "value.payload")
    require_exact_names(payload, ENVELOPE_FIELDS, "value.payload")
    operation = payload.get("op")
    if operation not in {"r", "c", "u"}:
        raise ValidationError(
            f"unexpected operation {operation!r} at "
            f"{record.topic}:{record.partition}:{record.offset}"
        )
    after = normalize_after(
        table,
        require_object(payload.get("after"), "value.payload.after"),
        row_schema,
    )
    if after.get("id") != key_id:
        raise ValidationError("message key id does not match after.id")
    if operation in {"r", "c"} and payload.get("before") is not None:
        raise ValidationError(f"{operation} event must have before=null")

    source = require_object(payload["source"], "value.payload.source")
    require_exact_names(source, set(SOURCE_FIELD_SPECS), "value.payload.source")
    expected_source = {
        "name": GENERATION_ENV["KAFKA_TOPIC_PREFIX"],
        "db": GENERATION_ENV["POSTGRES_DATABASE"],
        "schema": GENERATION_ENV["POSTGRES_SCHEMA"],
        "table": table,
        "connector": "postgresql",
        "version": "3.6.3.Final",
    }
    mismatches = {
        name: {"expected": expected, "actual": source.get(name)}
        for name, expected in expected_source.items()
        if source.get(name) != expected
    }
    if mismatches:
        raise ValidationError(f"source metadata mismatch: {mismatches}")
    require_positive_int(source.get("lsn"), "source.lsn")

    snapshot_marker = source.get("snapshot")
    transaction = payload.get("transaction")
    if operation == "r":
        if snapshot_marker not in INITIAL_SNAPSHOT_MARKERS:
            raise ValidationError(
                f"snapshot event has unexpected marker {snapshot_marker!r}"
            )
        if transaction is not None:
            raise ValidationError(
                "initial snapshot event must not contain transaction metadata"
            )
    else:
        if snapshot_marker != "false":
            raise ValidationError(
                f"streaming event has unexpected snapshot marker {snapshot_marker!r}"
            )
        transaction_object = require_object(
            transaction, "streaming value.payload.transaction"
        )
        require_exact_names(
            transaction_object,
            {"id", "total_order", "data_collection_order"},
            "value.payload.transaction",
        )
        if not transaction_object.get("id"):
            raise ValidationError("streaming transaction.id must be non-empty")
        require_positive_int(
            transaction_object.get("total_order"), "transaction.total_order"
        )
        require_positive_int(
            transaction_object.get("data_collection_order"),
            "transaction.data_collection_order",
        )

    return key_id, operation, after


def expected_sequences(
    phase: str,
) -> dict[str, dict[int, list[tuple[str, dict[str, Any]]]]]:
    if phase == "snapshot":
        return {
            table: {
                row_id: [("r", expected_fields)]
                for row_id, expected_fields in rows.items()
            }
            for table, rows in SNAPSHOT_ROWS.items()
        }
    return STREAMING_ROWS


def validate_watermarks(
    records: list[Record], partition_counts: dict[str, int], watermarks: Watermarks
) -> None:
    expected_keys = {
        (topic, partition)
        for topic, count in partition_counts.items()
        for partition in range(count)
    }
    if set(watermarks) != expected_keys:
        raise ValidationError("partition watermark metadata is incomplete")
    offsets: dict[PartitionKey, list[int]] = defaultdict(list)
    for record in records:
        offsets[(record.topic, record.partition)].append(record.offset)
    for key in sorted(expected_keys):
        state = watermarks[key]
        require_exact_names(state, {"low", "high", "position"}, "watermark")
        low, high, position = state["low"], state["high"], state["position"]
        if low != 0:
            raise ValidationError(f"partition {key} has nonzero low watermark {low}")
        if high < low or position != high:
            raise ValidationError(
                f"partition {key} is not fully consumed: "
                f"low={low} high={high} position={position}"
            )
        observed_offsets = sorted(offsets.get(key, []))
        if observed_offsets != list(range(low, high)):
            raise ValidationError(
                f"partition {key} offsets are incomplete: "
                f"expected {list(range(low, high))}, got {observed_offsets}"
            )


def validate_records(
    records: Iterable[Record],
    phase: str,
    partition_counts: dict[str, int],
    watermarks: Watermarks,
) -> dict[str, Any]:
    topics = topic_contract()
    expected_topic_set = set(topics.values())
    if set(partition_counts) != expected_topic_set:
        raise ValidationError(
            f"topic metadata mismatch: expected {sorted(expected_topic_set)}, "
            f"got {sorted(partition_counts)}"
        )
    wrong_partitions = {
        topic: count for topic, count in partition_counts.items() if count != 3
    }
    if wrong_partitions:
        raise ValidationError(
            f"table topics must have three partitions: {wrong_partitions}"
        )

    record_list = list(records)
    if len(record_list) != EXPECTED_TOTALS[phase]:
        raise ValidationError(
            f"{phase} phase expected {EXPECTED_TOTALS[phase]} records, "
            f"got {len(record_list)}"
        )
    validate_watermarks(record_list, partition_counts, watermarks)

    coordinates: set[tuple[str, int, int]] = set()
    observed: dict[
        str, dict[int, list[tuple[int, int, str, dict[str, Any]]]]
    ] = {table: defaultdict(list) for table in TABLES}
    operation_counts: Counter[str] = Counter()

    topic_to_table = {topic: table for table, topic in topics.items()}
    for record in record_list:
        if record.topic not in topic_to_table:
            raise ValidationError(f"unexpected topic {record.topic}")
        coordinate = (record.topic, record.partition, record.offset)
        if coordinate in coordinates:
            raise ValidationError(f"duplicate Kafka coordinate {coordinate}")
        coordinates.add(coordinate)
        if record.partition < 0 or record.partition >= partition_counts[record.topic]:
            raise ValidationError(f"invalid partition in coordinate {coordinate}")
        table = topic_to_table[record.topic]
        row_id, operation, after = validate_event(record, table)
        observed[table][row_id].append(
            (record.partition, record.offset, operation, after)
        )
        operation_counts[operation] += 1

    expected = expected_sequences(phase)
    for table in TABLES:
        if set(observed[table]) != set(expected[table]):
            raise ValidationError(
                f"{table} ids mismatch: expected {sorted(expected[table])}, "
                f"got {sorted(observed[table])}"
            )
        for row_id, expected_events in expected[table].items():
            events = observed[table][row_id]
            partitions = {event[0] for event in events}
            if len(partitions) != 1:
                raise ValidationError(
                    f"{table} id={row_id} records span partitions {sorted(partitions)}"
                )
            ordered = sorted(events, key=lambda event: event[1])
            actual_operations = [event[2] for event in ordered]
            expected_operations = [event[0] for event in expected_events]
            if actual_operations != expected_operations:
                raise ValidationError(
                    f"{table} id={row_id} operations mismatch: "
                    f"expected {expected_operations}, got {actual_operations}"
                )
            for index, (_, expected_fields) in enumerate(expected_events):
                actual_fields = dict(ordered[index][3])
                expected_row = {"id": row_id, **expected_fields}
                if actual_fields != expected_row:
                    raise ValidationError(
                        f"{table} id={row_id} event={index} fields mismatch: "
                        f"expected {expected_row}, got {actual_fields}"
                    )

    expected_operation_counts = (
        Counter({"r": 5})
        if phase == "snapshot"
        else Counter({"r": 5, "c": 3, "u": 3})
    )
    if operation_counts != expected_operation_counts:
        raise ValidationError(
            f"operation counts mismatch: expected {dict(expected_operation_counts)}, "
            f"got {dict(operation_counts)}"
        )

    return {
        "generation": "g001",
        "phase": phase,
        "records": len(record_list),
        "operations": dict(sorted(operation_counts.items())),
        "topics": {
            table: {
                "name": topic,
                "partitions": [
                    {
                        "partition": partition,
                        **watermarks[(topic, partition)],
                    }
                    for partition in range(partition_counts[topic])
                ],
                "records": sum(len(events) for events in observed[table].values()),
                "keys": sorted(observed[table]),
            }
            for table, topic in topics.items()
        },
    }


def collect_live_records(
    bootstrap_server: str, phase: str, timeout_seconds: float
) -> tuple[list[Record], dict[str, int], Watermarks]:
    try:
        from confluent_kafka import Consumer, TopicPartition  # type: ignore[import-not-found]
    except ImportError as error:
        raise ValidationError(
            "live verification requires confluent-kafka in the exact consumer runtime"
        ) from error

    validate_generation_environment(require_all=True)
    topics = topic_contract()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "group.id": "r02-verifier-g001",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
        }
    )
    records: dict[tuple[str, int, int], Record] = {}
    partition_counts: dict[str, int] = {}
    positions: dict[PartitionKey, int] = {}
    low_watermarks: dict[PartitionKey, int] = {}
    high_watermarks: dict[PartitionKey, int] = {}
    assignments: list[Any] = []

    def refresh_watermarks() -> None:
        for assignment in assignments:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(assignment.topic, assignment.partition),
                timeout=10,
                cached=False,
            )
            key = (assignment.topic, assignment.partition)
            low_watermarks[key] = low
            high_watermarks[key] = high

    def current_watermarks() -> Watermarks:
        return {
            key: {
                "low": low_watermarks[key],
                "high": high_watermarks[key],
                "position": positions[key],
            }
            for key in low_watermarks
        }

    try:
        for topic in topics.values():
            metadata = consumer.list_topics(topic, timeout=10).topics.get(topic)
            if metadata is None or metadata.error is not None:
                raise ValidationError(f"cannot read metadata for topic {topic}")
            partition_counts[topic] = len(metadata.partitions)
            for partition in sorted(metadata.partitions):
                topic_partition = TopicPartition(topic, partition)
                low, high = consumer.get_watermark_offsets(
                    topic_partition, timeout=10, cached=False
                )
                key = (topic, partition)
                assignments.append(TopicPartition(topic, partition, low))
                positions[key] = low
                low_watermarks[key] = low
                high_watermarks[key] = high
        nonzero_lows = {
            key: low for key, low in low_watermarks.items() if low != 0
        }
        if nonzero_lows:
            raise ValidationError(f"nonzero low watermarks: {nonzero_lows}")
        consumer.assign(assignments)

        deadline = time.monotonic() + timeout_seconds
        settled_since: float | None = None
        last_validation_error = "expected records have not arrived"
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None:
                if message.error():
                    raise ValidationError(f"Kafka consume error: {message.error()}")
                coordinate = (message.topic(), message.partition(), message.offset())
                records[coordinate] = Record(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=decode_json(message.key(), "message key"),
                    value=decode_json(message.value(), "message value"),
                )
                positions[(message.topic(), message.partition())] = message.offset() + 1
                settled_since = None

            if len(records) >= EXPECTED_TOTALS[phase]:
                refresh_watermarks()
                watermarks = current_watermarks()
                caught_up = all(
                    state["position"] == state["high"]
                    for state in watermarks.values()
                )
                if caught_up:
                    try:
                        validate_records(
                            records.values(), phase, partition_counts, watermarks
                        )
                    except ValidationError as error:
                        last_validation_error = str(error)
                        settled_since = None
                    else:
                        if settled_since is None:
                            settled_since = time.monotonic()
                        elif time.monotonic() - settled_since >= 1.0:
                            refresh_watermarks()
                            watermarks = current_watermarks()
                            validate_records(
                                records.values(), phase, partition_counts, watermarks
                            )
                            return list(records.values()), partition_counts, watermarks
                else:
                    settled_since = None

        raise ValidationError(
            f"timed out after {timeout_seconds:g}s: {last_validation_error}"
        )
    finally:
        consumer.close()


def row_schema_fields(table: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for name, field_type in ROW_FIELD_TYPES[table].items():
        field: dict[str, Any] = {"field": name, "optional": False}
        if field_type == "decimal":
            field.update(
                {
                    "type": "bytes",
                    "name": "org.apache.kafka.connect.data.Decimal",
                    "parameters": {
                        "connect.decimal.precision": "12",
                        "scale": "2",
                    },
                }
            )
        elif field_type == "timestamp":
            field.update(
                {
                    "type": "string",
                    "name": "io.debezium.time.ZonedTimestamp",
                }
            )
        else:
            field["type"] = field_type
        fields.append(field)
    return fields


def envelope_schema(topic: str, table: str) -> dict[str, Any]:
    row_fields = row_schema_fields(table)
    source_fields: list[dict[str, Any]] = []
    for name, (field_type, optional) in SOURCE_FIELD_SPECS.items():
        source_field: dict[str, Any] = {
            "field": name,
            "type": field_type,
            "optional": optional,
        }
        if name == "snapshot":
            source_field.update(
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
        source_fields.append(source_field)
    transaction_fields = [
        {"field": "id", "type": "string", "optional": False},
        {"field": "total_order", "type": "int64", "optional": False},
        {
            "field": "data_collection_order",
            "type": "int64",
            "optional": False,
        },
    ]
    return {
        "type": "struct",
        "optional": False,
        "name": f"{topic}.Envelope",
        "fields": [
            {
                "field": "before",
                "type": "struct",
                "optional": True,
                "name": f"{topic}.Value",
                "fields": row_fields,
            },
            {
                "field": "after",
                "type": "struct",
                "optional": True,
                "name": f"{topic}.Value",
                "fields": row_fields,
            },
            {
                "field": "source",
                "type": "struct",
                "optional": False,
                "name": "io.debezium.connector.postgresql.Source",
                "fields": source_fields,
            },
            {
                "field": "transaction",
                "type": "struct",
                "optional": True,
                "name": "event.block",
                "fields": transaction_fields,
            },
            {"field": "op", "type": "string", "optional": False},
            {"field": "ts_ms", "type": "int64", "optional": True},
            {"field": "ts_us", "type": "int64", "optional": True},
            {"field": "ts_ns", "type": "int64", "optional": True},
        ],
    }


def encode_decimal(value: str) -> str:
    unscaled = round(Decimal(value) * 100)
    length = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(
        unscaled.to_bytes(length, byteorder="big", signed=True)
    ).decode("ascii")


def synthetic_record(
    topic: str,
    table: str,
    partition: int,
    offset: int,
    row_id: int,
    operation: str,
    fields: dict[str, Any],
) -> Record:
    snapshot = operation == "r"
    after: dict[str, Any] = {
        "id": row_id,
        **fields,
        "created_at": "2026-01-01T00:00:00.000000Z",
        "updated_at": "2026-01-01T00:00:00.000000Z",
    }
    for name, field_type in ROW_FIELD_TYPES[table].items():
        if field_type == "decimal":
            after[name] = encode_decimal(after[name])
    source: dict[str, Any] = {name: None for name in SOURCE_FIELD_SPECS}
    source.update(
        {
            "version": "3.6.3.Final",
            "connector": "postgresql",
            "name": "cdc.g001",
            "ts_ms": 1,
            "snapshot": "last" if snapshot else "false",
            "db": "cdc_lab",
            "sequence": "[null,\"100\"]",
            "ts_us": 1000,
            "ts_ns": 1000000,
            "schema": "public",
            "table": table,
            "txId": 1,
            "lsn": 100 + offset,
        }
    )
    return Record(
        topic=topic,
        partition=partition,
        offset=offset,
        key={
            "schema": {
                "type": "struct",
                "optional": False,
                "name": f"{topic}.Key",
                "fields": [
                    {"field": "id", "type": "int64", "optional": False}
                ],
            },
            "payload": {"id": row_id},
        },
        value={
            "schema": envelope_schema(topic, table),
            "payload": {
                "before": None,
                "after": after,
                "source": source,
                "transaction": None
                if snapshot
                else {
                    "id": f"tx:{offset}",
                    "total_order": 1,
                    "data_collection_order": 1,
                },
                "op": operation,
                "ts_ms": 1,
                "ts_us": 1000,
                "ts_ns": 1000000,
            },
        },
    )


def synthetic_records(
    phase: str,
) -> tuple[list[Record], dict[str, int], Watermarks]:
    topics = topic_contract()
    records: list[Record] = []
    offsets: Counter[PartitionKey] = Counter()
    for table, rows in expected_sequences(phase).items():
        topic = topics[table]
        for row_id, events in rows.items():
            partition = row_id % 3
            for operation, fields in events:
                offset = offsets[(topic, partition)]
                offsets[(topic, partition)] += 1
                records.append(
                    synthetic_record(
                        topic, table, partition, offset, row_id, operation, fields
                    )
                )
    partition_counts = {topic: 3 for topic in topics.values()}
    watermarks = {
        (topic, partition): {
            "low": 0,
            "high": offsets[(topic, partition)],
            "position": offsets[(topic, partition)],
        }
        for topic in topics.values()
        for partition in range(3)
    }
    return records, partition_counts, watermarks


def expect_self_test_failure(
    records: list[Record],
    partitions: dict[str, int],
    watermarks: Watermarks,
    expected_message: str,
) -> None:
    try:
        validate_records(records, "streaming", partitions, watermarks)
    except ValidationError as error:
        if expected_message not in str(error):
            raise ValidationError(
                f"self-test mutation failed for the wrong reason: {error}"
            ) from error
    else:
        raise ValidationError("self-test mutation was not detected")


def run_self_test() -> None:
    snapshot_records, partitions, watermarks = synthetic_records("snapshot")
    validate_records(snapshot_records, "snapshot", partitions, watermarks)
    streaming_records, partitions, watermarks = synthetic_records("streaming")
    validate_records(streaming_records, "streaming", partitions, watermarks)

    missing_transaction, partitions, watermarks = synthetic_records("streaming")
    target = next(
        record for record in missing_transaction if record.value["payload"]["op"] == "u"
    )
    target.value["payload"]["transaction"] = None
    expect_self_test_failure(
        missing_transaction, partitions, watermarks, "transaction"
    )

    repeated_snapshot, partitions, watermarks = synthetic_records("streaming")
    target = next(
        record for record in repeated_snapshot if record.value["payload"]["op"] == "u"
    )
    target.value["payload"]["op"] = "r"
    target.value["payload"]["source"]["snapshot"] = "last"
    target.value["payload"]["transaction"] = None
    expect_self_test_failure(
        repeated_snapshot, partitions, watermarks, "operations mismatch"
    )

    wrong_partitions, partitions, watermarks = synthetic_records("streaming")
    partitions[TOPICS["customers"]] = 2
    expect_self_test_failure(
        wrong_partitions, partitions, watermarks, "three partitions"
    )

    missing_amount, partitions, watermarks = synthetic_records("streaming")
    target = next(
        record
        for record in missing_amount
        if record.topic == TOPICS["payments"]
        and record.value["payload"]["op"] == "c"
    )
    target.value["payload"]["after"].pop("amount")
    expect_self_test_failure(
        missing_amount, partitions, watermarks, "after fields mismatch"
    )

    wrong_amount, partitions, watermarks = synthetic_records("streaming")
    target = next(
        record
        for record in wrong_amount
        if record.topic == TOPICS["orders"]
        and record.value["payload"]["op"] == "c"
    )
    target.value["payload"]["after"]["total_amount"] = encode_decimal("16.00")
    expect_self_test_failure(wrong_amount, partitions, watermarks, "fields mismatch")

    missing_schema, partitions, watermarks = synthetic_records("streaming")
    missing_schema[0].value["schema"].pop("fields")
    expect_self_test_failure(
        missing_schema, partitions, watermarks, "value.schema.fields"
    )

    composite_key, partitions, watermarks = synthetic_records("streaming")
    composite_key[0].key["schema"]["fields"].append(
        {"field": "tenant_id", "type": "int64", "optional": False}
    )
    expect_self_test_failure(
        composite_key, partitions, watermarks, "key.schema.fields"
    )

    wrong_source_optional, partitions, watermarks = synthetic_records("streaming")
    source_schema = next(
        field
        for field in wrong_source_optional[0].value["schema"]["fields"]
        if field["field"] == "source"
    )
    source_schema["fields"][0]["optional"] = True
    expect_self_test_failure(
        wrong_source_optional, partitions, watermarks, ".optional"
    )

    optional_op, partitions, watermarks = synthetic_records("streaming")
    op_schema = next(
        field
        for field in optional_op[0].value["schema"]["fields"]
        if field["field"] == "op"
    )
    op_schema["optional"] = True
    expect_self_test_failure(optional_op, partitions, watermarks, "non-optional")

    reordered_source, partitions, watermarks = synthetic_records("streaming")
    source_schema = next(
        field
        for field in reordered_source[0].value["schema"]["fields"]
        if field["field"] == "source"
    )
    source_schema["fields"][0], source_schema["fields"][1] = (
        source_schema["fields"][1],
        source_schema["fields"][0],
    )
    expect_self_test_failure(
        reordered_source, partitions, watermarks, "order mismatch"
    )

    reordered_envelope, partitions, watermarks = synthetic_records("streaming")
    envelope_fields = reordered_envelope[0].value["schema"]["fields"]
    envelope_fields[0], envelope_fields[1] = envelope_fields[1], envelope_fields[0]
    expect_self_test_failure(
        reordered_envelope, partitions, watermarks, "order mismatch"
    )

    nonzero_low, partitions, watermarks = synthetic_records("streaming")
    watermarks[(TOPICS["customers"], 0)]["low"] = 1
    expect_self_test_failure(
        nonzero_low, partitions, watermarks, "nonzero low watermark"
    )
    print("R-02 verifier self-test passed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("snapshot", "streaming"))
    parser.add_argument(
        "--bootstrap-server",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVER", "kafka:9092"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test == (args.phase is not None):
        parser.error("choose exactly one of --self-test or --phase")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    return args


def main() -> None:
    args = parse_args()
    try:
        if args.self_test:
            run_self_test()
            return
        records, partition_counts, watermarks = collect_live_records(
            args.bootstrap_server, args.phase, args.timeout_seconds
        )
        print(
            json.dumps(
                validate_records(
                    records, args.phase, partition_counts, watermarks
                ),
                indent=2,
                sort_keys=True,
            )
        )
    except ValidationError as error:
        fail(f"R-02 verification failed: {error}")


if __name__ == "__main__":
    main()
