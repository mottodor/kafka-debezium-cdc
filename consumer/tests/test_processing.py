from __future__ import annotations

import base64
import copy
import json
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from cdc_consumer.migrations import migrate, open_database
from cdc_consumer.processing import (
    KafkaRecord,
    OrderingCollisionError,
    OrderingToken,
    ProcessingError,
    ProjectionWriteError,
    RecordValidationError,
    process_record,
)


ROW_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
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


def encode_decimal(value: str) -> str:
    unscaled = int(value.replace(".", ""))
    size = max(1, (unscaled.bit_length() + 8) // 8)
    return base64.b64encode(unscaled.to_bytes(size, "big", signed=True)).decode()


def row_schema_fields(table: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for name, field_type in ROW_FIELDS[table]:
        field: dict[str, Any] = {"field": name, "optional": False}
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
            field["type"] = field_type
        fields.append(field)
    return fields


def envelope_schema(topic: str, table: str) -> dict[str, Any]:
    source_fields: list[dict[str, Any]] = []
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
        source_fields.append(field)
    row_fields = row_schema_fields(table)
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
                "fields": source_fields,
            },
            {
                "field": "transaction",
                "type": "struct",
                "optional": True,
                "name": "event.block",
                "fields": [
                    {"field": "id", "type": "string", "optional": False},
                    {"field": "total_order", "type": "int64", "optional": False},
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
    }


def row_values(table: str, row_id: int, *, variant: str = "initial") -> dict[str, Any]:
    timestamp = "2026-01-01T00:00:00.000000Z"
    if table == "customers":
        return {
            "id": row_id,
            "email": f"customer-{row_id}@example.test",
            "full_name": "New Name" if variant == "new" else "Initial Name",
            "status": "inactive" if variant == "new" else "active",
            "created_at": timestamp,
            "updated_at": timestamp,
        }
    if table == "orders":
        return {
            "id": row_id,
            "customer_id": 999,
            "status": "paid" if variant == "new" else "pending",
            "total_amount": encode_decimal("15.00"),
            "created_at": timestamp,
            "updated_at": timestamp,
        }
    return {
        "id": row_id,
        "order_id": 777,
        "status": "captured" if variant == "new" else "authorized",
        "amount": encode_decimal("15.00"),
        "provider_ref": f"pay-{row_id}",
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def key_document(topic: str, row_id: int) -> dict[str, Any]:
    return {
        "schema": {
            "type": "struct",
            "optional": False,
            "name": f"{topic}.Key",
            "fields": [{"field": "id", "type": "int64", "optional": False}],
        },
        "payload": {"id": row_id},
    }


def make_record(
    *,
    table: str = "customers",
    row_id: int = 1,
    partition: int = 0,
    offset: int = 0,
    operation: str = "c",
    lsn: int = 100,
    total_order: int = 1,
    collection_order: int = 1,
    variant: str = "initial",
    snapshot_marker: str | None = None,
    headers: tuple[tuple[str, bytes | None], ...] = (),
) -> KafkaRecord:
    topic = f"cdc.g001.public.{table}"
    snapshot = operation == "r"
    marker = snapshot_marker or ("last" if snapshot else "false")
    values = row_values(table, row_id, variant=variant)
    source: dict[str, Any] = {name: None for name, _, _ in SOURCE_FIELDS}
    source.update(
        {
            "version": "3.6.3.Final",
            "connector": "postgresql",
            "name": "cdc.g001",
            "ts_ms": 10,
            "snapshot": marker,
            "db": "cdc_lab",
            "sequence": f"[null,\"{lsn}\"]",
            "ts_us": 10_000,
            "ts_ns": 10_000_000,
            "schema": "public",
            "table": table,
            "txId": None if snapshot else 20,
            "lsn": lsn,
        }
    )
    if operation in {"c", "r"}:
        before: dict[str, Any] | None = None
        after: dict[str, Any] | None = values
    elif operation == "u":
        before = row_values(table, row_id)
        after = values
    else:
        before = values
        after = None
    value = {
        "schema": envelope_schema(topic, table),
        "payload": {
            "before": before,
            "after": after,
            "source": source,
            "transaction": None
            if snapshot
            else {
                "id": f"tx:{lsn}",
                "total_order": total_order,
                "data_collection_order": collection_order,
            },
            "op": operation,
            "ts_ms": 11,
            "ts_us": 11_000,
            "ts_ns": 11_000_000,
        },
    }
    return KafkaRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        key=json.dumps(key_document(topic, row_id), separators=(",", ":")).encode(),
        value=json.dumps(value, separators=(",", ":")).encode(),
        headers=headers,
        timestamp_ms=12,
    )


def replace_value(record: KafkaRecord, value: dict[str, Any] | bytes | None) -> KafkaRecord:
    encoded = (
        value
        if isinstance(value, bytes) or value is None
        else json.dumps(value, separators=(",", ":")).encode()
    )
    return KafkaRecord(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        value=encoded,
        headers=record.headers,
        timestamp_ms=record.timestamp_ms,
    )


def decoded_value(record: KafkaRecord) -> dict[str, Any]:
    assert record.value is not None
    return json.loads(record.value)


def with_customers_phone(record: KafkaRecord, phone: str | None) -> KafkaRecord:
    value = decoded_value(record)
    phone_field = {"field": "phone", "type": "string", "optional": True}
    value["schema"]["fields"][0]["fields"].append(copy.deepcopy(phone_field))
    value["schema"]["fields"][1]["fields"].append(copy.deepcopy(phone_field))
    before = value["payload"]["before"]
    after = value["payload"]["after"]
    if before is not None:
        before["phone"] = None
    if after is not None:
        after["phone"] = phone
    return replace_value(record, value)


class ProcessingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database = Path(self.temporary_directory.name) / "cdc-g001.sqlite3"
        migrate(self.database, "g001")
        self.connection = open_database(self.database)
        self.addCleanup(self.connection.close)
        self.observed_at = "2026-01-01T00:00:01.000Z"

    def process(self, record: KafkaRecord):
        return process_record(
            self.connection, "g001", record, observed_at=self.observed_at
        )

    def test_durable_commit_timestamp_includes_projection_delay(self) -> None:
        delay_seconds = 0.15
        self.connection.create_function(
            "delay_before_commit", 0, lambda: time.sleep(delay_seconds)
        )
        self.connection.execute(
            "CREATE TRIGGER delay_customer_projection "
            "AFTER INSERT ON customers_current "
            "BEGIN SELECT delay_before_commit(); END"
        )
        started_at = datetime.now(timezone.utc)
        process_record(self.connection, "g001", make_record(offset=74))
        returned_at = datetime.now(timezone.utc)
        stored = self.connection.execute(
            "SELECT durable_committed_at FROM raw_events WHERE offset = 74"
        ).fetchone()[0]
        durable_boundary = datetime.fromisoformat(stored.replace("Z", "+00:00"))
        self.assertGreaterEqual(
            (durable_boundary - started_at).total_seconds(),
            delay_seconds * 0.8,
        )
        self.assertLessEqual(durable_boundary, returned_at)

    def test_applies_snapshot_and_independent_child_projections(self) -> None:
        order = make_record(table="orders", row_id=103, operation="r", lsn=50)
        payment = make_record(
            table="payments", row_id=1002, operation="r", lsn=50, offset=1
        )
        self.assertEqual(self.process(order).status, "applied")
        self.assertEqual(self.process(payment).status, "applied")
        self.assertEqual(
            self.connection.execute(
                "SELECT customer_id, status, total_amount FROM orders_current WHERE id = 103"
            ).fetchone(),
            (999, "pending", "15.00"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT order_id, status, amount FROM payments_current WHERE id = 1002"
            ).fetchone(),
            (777, "authorized", "15.00"),
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT id FROM customers_current WHERE id = 999"
            ).fetchone()
        )

    def test_allowed_empty_domain_strings_apply_in_after_and_before(self) -> None:
        customer = make_record(table="customers", row_id=4, offset=70)
        customer_value = decoded_value(customer)
        customer_value["payload"]["after"]["email"] = ""
        customer = replace_value(customer, customer_value)

        payment = make_record(table="payments", row_id=1003, offset=71)
        payment_value = decoded_value(payment)
        payment_value["payload"]["after"]["provider_ref"] = ""
        payment = replace_value(payment, payment_value)

        customer_update = make_record(
            table="customers", row_id=5, offset=72, operation="u", variant="new"
        )
        customer_update_value = decoded_value(customer_update)
        customer_update_value["payload"]["before"]["email"] = ""
        customer_update = replace_value(customer_update, customer_update_value)

        payment_delete = make_record(
            table="payments", row_id=1004, offset=73, operation="d"
        )
        payment_delete_value = decoded_value(payment_delete)
        payment_delete_value["payload"]["before"]["provider_ref"] = ""
        payment_delete = replace_value(payment_delete, payment_delete_value)

        for record in (customer, payment, customer_update, payment_delete):
            self.assertEqual(self.process(record).status, "applied")
        self.assertEqual(
            self.connection.execute(
                "SELECT email FROM customers_current WHERE id = 4"
            ).fetchone(),
            ("",),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT provider_ref FROM payments_current WHERE id = 1003"
            ).fetchone(),
            ("",),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT full_name FROM customers_current WHERE id = 5"
            ).fetchone(),
            ("New Name",),
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT id FROM payments_current WHERE id = 1004"
            ).fetchone()
        )

    def test_out_of_order_and_phase_rank_keep_newest_projection(self) -> None:
        newest = make_record(
            offset=2,
            operation="u",
            lsn=200,
            total_order=2,
            collection_order=2,
            variant="new",
        )
        older_same_lsn = make_record(
            offset=3,
            operation="u",
            lsn=200,
            total_order=1,
            collection_order=9,
        )
        late_snapshot = make_record(
            offset=4,
            operation="r",
            lsn=999,
            variant="initial",
        )
        self.assertEqual(self.process(newest).status, "applied")
        self.assertEqual(self.process(older_same_lsn).status, "stale")
        self.assertEqual(self.process(late_snapshot).status, "stale")
        self.assertEqual(
            self.connection.execute(
                "SELECT full_name, status FROM customers_current WHERE id = 1"
            ).fetchone(),
            ("New Name", "inactive"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT offset, processing_status FROM raw_events ORDER BY offset"
            ).fetchall(),
            [(2, "applied"), (3, "stale"), (4, "stale")],
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT phase_rank, source_lsn, transaction_total_order, "
                "transaction_data_collection_order FROM entity_watermarks"
            ).fetchone(),
            (1, 200, 2, 2),
        )

    def test_delete_tombstone_and_old_update_do_not_resurrect_row(self) -> None:
        create = make_record(offset=10, operation="c", lsn=100)
        delete = make_record(offset=11, operation="d", lsn=300)
        topic = create.topic
        tombstone = KafkaRecord(
            topic=topic,
            partition=0,
            offset=12,
            key=create.key,
            value=None,
            headers=(("trace", b"first"), ("trace", None)),
            timestamp_ms=12,
        )
        stale = make_record(
            offset=13, operation="u", lsn=200, variant="new"
        )
        self.assertEqual(self.process(create).status, "applied")
        self.connection.execute(
            "CREATE TRIGGER require_watermark_before_customer_delete "
            "BEFORE DELETE ON customers_current "
            "WHEN NOT EXISTS ("
            "SELECT 1 FROM entity_watermarks "
            "WHERE generation = 'g001' AND source_table = 'customers' "
            "AND entity_key = '1' AND source_lsn = 300 AND last_was_delete = 1"
            ") BEGIN SELECT RAISE(ABORT, 'delete watermark was not written first'); END"
        )
        self.assertEqual(self.process(delete).status, "applied")
        watermark_before_tombstone = self.connection.execute(
            "SELECT phase_rank, source_lsn, transaction_total_order, "
            "transaction_data_collection_order, last_was_delete "
            "FROM entity_watermarks"
        ).fetchone()
        self.assertEqual(self.process(tombstone).status, "tombstone")
        self.assertEqual(self.process(stale).status, "stale")
        self.assertIsNone(
            self.connection.execute(
                "SELECT id FROM customers_current WHERE id = 1"
            ).fetchone()
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT phase_rank, source_lsn, transaction_total_order, "
                "transaction_data_collection_order, last_was_delete "
                "FROM entity_watermarks"
            ).fetchone(),
            watermark_before_tombstone,
        )
        self.assertEqual(watermark_before_tombstone, (1, 300, 1, 1, 1))
        self.assertEqual(
            self.connection.execute(
                "SELECT offset, operation, processing_status FROM raw_events ORDER BY offset"
            ).fetchall(),
            [
                (10, "c", "applied"),
                (11, "d", "applied"),
                (12, None, "tombstone"),
                (13, "u", "stale"),
            ],
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT header_index, header_key, header_value FROM raw_headers "
                "WHERE offset = 12 ORDER BY header_index"
            ).fetchall(),
            [(0, "trace", b"first"), (1, "trace", None)],
        )

    def test_duplicate_coordinate_is_not_reapplied(self) -> None:
        record = make_record(offset=20, operation="c", lsn=100)
        first = self.process(record)
        duplicate = self.process(record)
        self.assertFalse(first.duplicate)
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(duplicate.status, "applied")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*), max(delivery_count) FROM raw_events"
            ).fetchone(),
            (1, 2),
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM entity_watermarks").fetchone(),
            (1,),
        )

    def test_generation_mismatch_is_rejected_before_duplicate_shortcut(self) -> None:
        record = make_record(offset=25, operation="c", lsn=100)
        self.process(record)
        with self.assertRaisesRegex(ValueError, "different generation"):
            process_record(
                self.connection,
                "g002",
                record,
                observed_at=self.observed_at,
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT delivery_count, generation FROM raw_events WHERE offset = 25"
            ).fetchone(),
            (1, "g001"),
        )

    def test_equal_token_collision_rolls_back_raw_and_upserts_error(self) -> None:
        first = make_record(offset=30, operation="c", lsn=500)
        collision = make_record(
            offset=31, operation="u", lsn=500, variant="new"
        )
        self.process(first)
        for expected_attempt in (1, 2):
            with self.assertRaises(OrderingCollisionError):
                self.process(collision)
            self.assertEqual(
                self.connection.execute(
                    "SELECT attempt_count, message_key, message_value, error_class "
                    "FROM ingestion_errors WHERE offset = 31"
                ).fetchone(),
                (
                    expected_attempt,
                    collision.key,
                    collision.value,
                    "OrderingCollisionError",
                ),
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 31"
            ).fetchone()
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT full_name FROM customers_current WHERE id = 1"
            ).fetchone(),
            ("Initial Name",),
        )

    def test_equal_token_collision_is_detected_in_stale_history(self) -> None:
        newest = make_record(offset=35, operation="u", lsn=300, variant="new")
        stale = make_record(offset=36, operation="u", lsn=200)
        repeated_stale_token = make_record(
            offset=37,
            operation="u",
            lsn=200,
            variant="new",
        )
        self.assertEqual(self.process(newest).status, "applied")
        self.assertEqual(self.process(stale).status, "stale")
        with self.assertRaises(OrderingCollisionError):
            self.process(repeated_stale_token)
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 37"
            ).fetchone()
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT error_class, attempt_count FROM ingestion_errors WHERE offset = 37"
            ).fetchone(),
            ("OrderingCollisionError", 1),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT full_name FROM customers_current WHERE id = 1"
            ).fetchone(),
            ("New Name",),
        )

    def test_missing_and_empty_keys_are_durable_fail_stop_errors(self) -> None:
        for offset, key in ((38, None), (39, b"")):
            base = make_record(offset=offset)
            record = KafkaRecord(
                topic=base.topic,
                partition=base.partition,
                offset=base.offset,
                key=key,
                value=base.value,
                headers=base.headers,
                timestamp_ms=base.timestamp_ms,
            )
            with self.subTest(offset=offset):
                for expected_attempt in (1, 2):
                    with self.assertRaisesRegex(
                        RecordValidationError, "non-empty key"
                    ):
                        self.process(record)
                    self.assertEqual(
                        self.connection.execute(
                            "SELECT message_key, attempt_count FROM ingestion_errors "
                            "WHERE offset = ?",
                            (offset,),
                        ).fetchone(),
                        (key, expected_attempt),
                    )
                self.assertIsNone(
                    self.connection.execute(
                        "SELECT offset FROM raw_events WHERE offset = ?",
                        (offset,),
                    ).fetchone()
                )

    def test_malformed_unknown_incremental_and_unsupported_schema_fail_stop(self) -> None:
        malformed = replace_value(
            make_record(offset=40), b'{"schema":{},"payload":{},"payload":{}}'
        )
        unknown = make_record(offset=41)
        unknown_value = decoded_value(unknown)
        unknown_value["payload"]["op"] = "x"
        unknown = replace_value(unknown, unknown_value)

        incremental = make_record(
            offset=42, operation="r", snapshot_marker="incremental"
        )

        missing_order = make_record(offset=43)
        missing_order_value = decoded_value(missing_order)
        del missing_order_value["payload"]["transaction"]["total_order"]
        missing_order = replace_value(missing_order, missing_order_value)

        unsupported = make_record(offset=44)
        unsupported_value = decoded_value(unsupported)
        phone_field = {"field": "phone", "type": "string", "optional": True}
        unsupported_value["schema"]["fields"][0]["fields"].append(
            copy.deepcopy(phone_field)
        )
        unsupported_value["schema"]["fields"][1]["fields"].append(
            copy.deepcopy(phone_field)
        )
        unsupported_value["payload"]["after"]["phone"] = "+15550000001"
        unsupported = replace_value(unsupported, unsupported_value)

        cases = (
            (malformed, "duplicate JSON field"),
            (unknown, "unsupported Debezium operation"),
            (incremental, "incremental snapshots are not supported"),
            (missing_order, "fields differ"),
            (unsupported, "customers.phone requires SQLite migration"),
        )
        for record, expected_message in cases:
            with self.subTest(offset=record.offset):
                with self.assertRaisesRegex(RecordValidationError, expected_message):
                    self.process(record)
                self.assertIsNone(
                    self.connection.execute(
                        "SELECT offset FROM raw_events WHERE offset = ?",
                        (record.offset,),
                    ).fetchone()
                )
                stored = self.connection.execute(
                    "SELECT error_class, attempt_count, message_value "
                    "FROM ingestion_errors WHERE offset = ?",
                    (record.offset,),
                ).fetchone()
                self.assertEqual(stored, ("RecordValidationError", 1, record.value))
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM customers_current").fetchone(),
            (0,),
        )

    def test_customers_phone_stops_before_migration_and_replays_same_coordinate(self) -> None:
        existing = make_record(row_id=2, offset=45)
        self.process(existing)
        changed = with_customers_phone(
            make_record(
                row_id=1,
                offset=46,
                operation="u",
                lsn=200,
                variant="new",
            ),
            "+15550000001",
        )

        with self.assertRaisesRegex(
            RecordValidationError,
            "customers.phone requires SQLite migration 0002_customers_phone",
        ):
            self.process(changed)
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 46"
            ).fetchone()
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT error_class, attempt_count, message_value "
                "FROM ingestion_errors WHERE offset = 46"
            ).fetchone(),
            ("RecordValidationError", 1, changed.value),
        )

        self.assertEqual(
            migrate(self.database, "g001", target_version=2),
            2,
        )
        result = self.process(changed)
        self.assertEqual((result.status, result.duplicate), ("applied", False))
        self.assertEqual(
            self.connection.execute(
                "SELECT topic, partition, offset, processing_status "
                "FROM raw_events WHERE offset = 46"
            ).fetchone(),
            (changed.topic, changed.partition, changed.offset, "applied"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT full_name, status, phone FROM customers_current WHERE id = 1"
            ).fetchone(),
            ("New Name", "inactive", "+15550000001"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT phone FROM customers_current WHERE id = 2"
            ).fetchone(),
            (None,),
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT attempt_count FROM ingestion_errors WHERE offset = 46"
            ).fetchone()
        )

        nullable = with_customers_phone(
            make_record(row_id=3, offset=47, lsn=300),
            None,
        )
        self.process(nullable)
        self.assertEqual(
            self.connection.execute(
                "SELECT phone FROM customers_current WHERE id = 3"
            ).fetchone(),
            (None,),
        )

    def test_oversized_update_before_int64_is_a_durable_validation_error(self) -> None:
        record = make_record(
            table="orders",
            row_id=103,
            offset=47,
            operation="u",
        )
        value = decoded_value(record)
        value["payload"]["before"]["customer_id"] = 1 << 63
        oversized = replace_value(record, value)
        for expected_attempt in (1, 2):
            with self.assertRaisesRegex(RecordValidationError, "signed 64-bit"):
                self.process(oversized)
            self.assertEqual(
                self.connection.execute(
                    "SELECT message_value, error_class, attempt_count "
                    "FROM ingestion_errors WHERE offset = 47"
                ).fetchone(),
                (
                    oversized.value,
                    "RecordValidationError",
                    expected_attempt,
                ),
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 47"
            ).fetchone()
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT id FROM orders_current WHERE id = 103"
            ).fetchone()
        )

    def test_delete_accepts_replica_identity_key_only_before_payload(self) -> None:
        record = make_record(
            table="customers",
            row_id=4,
            offset=48,
            operation="d",
        )
        value = decoded_value(record)
        before = value["payload"]["before"]
        for name in ("email", "full_name", "status"):
            before[name] = ""
        for name in ("created_at", "updated_at"):
            before[name] = "1970-01-01T00:00:00.000000Z"
        delete = replace_value(record, value)

        self.assertEqual(self.process(delete).status, "applied")
        self.assertEqual(
            self.connection.execute(
                "SELECT operation, processing_status FROM raw_events WHERE offset = 48"
            ).fetchone(),
            ("d", "applied"),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT last_was_delete FROM entity_watermarks "
                "WHERE source_table = 'customers' AND entity_key = '4'"
            ).fetchone(),
            (1,),
        )

    def test_oversized_connect_int64_is_a_durable_validation_error(self) -> None:
        record = make_record(table="orders", row_id=103, offset=49)
        value = decoded_value(record)
        value["payload"]["after"]["customer_id"] = 1 << 63
        oversized = replace_value(record, value)
        for expected_attempt in (1, 2):
            with self.assertRaisesRegex(RecordValidationError, "signed 64-bit"):
                self.process(oversized)
            self.assertEqual(
                self.connection.execute(
                    "SELECT message_value, error_class, attempt_count "
                    "FROM ingestion_errors WHERE offset = 49"
                ).fetchone(),
                (
                    oversized.value,
                    "RecordValidationError",
                    expected_attempt,
                ),
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 49"
            ).fetchone()
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT id FROM orders_current WHERE id = 103"
            ).fetchone()
        )

    def test_projection_rejection_rolls_back_raw_and_persists_error(self) -> None:
        self.connection.execute(
            "CREATE TRIGGER reject_customer_projection "
            "BEFORE INSERT ON customers_current "
            "BEGIN SELECT RAISE(ABORT, 'test projection rejection'); END"
        )
        record = make_record(offset=50)
        with self.assertRaisesRegex(ProjectionWriteError, "test projection rejection"):
            self.process(record)
        self.assertIsNone(
            self.connection.execute(
                "SELECT offset FROM raw_events WHERE offset = 50"
            ).fetchone()
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT error_class, attempt_count FROM ingestion_errors WHERE offset = 50"
            ).fetchone(),
            ("ProjectionWriteError", 1),
        )

    def test_ordering_token_comparison_is_lexicographic(self) -> None:
        self.assertLess(OrderingToken(0, 999, 0, 0), OrderingToken(1, 1, 1, 1))
        self.assertLess(OrderingToken(1, 10, 1, 99), OrderingToken(1, 10, 2, 1))
        self.assertLess(OrderingToken(1, 10, 2, 1), OrderingToken(1, 10, 2, 2))

    def test_process_record_requires_an_idle_connection(self) -> None:
        self.connection.execute("BEGIN")
        self.addCleanup(
            lambda: self.connection.execute("ROLLBACK")
            if self.connection.in_transaction
            else None
        )
        with self.assertRaisesRegex(RuntimeError, "no active transaction"):
            self.process(make_record(offset=60))


if __name__ == "__main__":
    unittest.main()
