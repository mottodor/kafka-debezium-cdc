from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cdc_consumer.migrations import (  # pyright: ignore[reportMissingImports]
    Migration,
    MigrationError,
    apply_migrations,
    load_migrations,
    migrate,
    open_database,
)

EXPECTED_TABLES = {
    "customers_current",
    "entity_watermarks",
    "ingestion_errors",
    "orders_current",
    "payments_current",
    "raw_events",
    "raw_headers",
    "schema_migrations",
    "sink_metadata",
}


class MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database = Path(self.temporary_directory.name) / "cdc-g001.sqlite3"

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database)
        connection.execute("PRAGMA foreign_keys = ON")
        self.addCleanup(connection.close)
        return connection

    def test_initial_schema_is_complete_and_idempotent(self) -> None:
        self.assertEqual(migrate(self.database, "g001"), 1)
        self.assertEqual(migrate(self.database, "g001"), 1)
        connection = self.connect()

        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        self.assertEqual(tables, EXPECTED_TABLES)
        migration = connection.execute(
            "SELECT version, name, length(checksum_sha256) FROM schema_migrations"
        ).fetchone()
        self.assertEqual(migration, (1, "initial", 64))
        self.assertEqual(
            connection.execute(
                "SELECT singleton, generation FROM sink_metadata"
            ).fetchall(),
            [(1, "g001")],
        )
        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        generation_triggers = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_schema "
                "WHERE type = 'trigger' AND name LIKE '%_generation_%'"
            )
        }
        self.assertEqual(len(generation_triggers), 12)

        raw_primary_key = {
            row[1]: row[5]
            for row in connection.execute("PRAGMA table_info(raw_events)")
            if row[5]
        }
        self.assertEqual(raw_primary_key, {"topic": 1, "partition": 2, "offset": 3})
        projection_foreign_keys = sum(
            (
                len(connection.execute("PRAGMA foreign_key_list(customers_current)").fetchall()),
                len(connection.execute("PRAGMA foreign_key_list(orders_current)").fetchall()),
                len(connection.execute("PRAGMA foreign_key_list(payments_current)").fetchall()),
            )
        )
        self.assertEqual(projection_foreign_keys, 0)

    def test_customers_phone_migration_is_manual_idempotent_and_non_destructive(self) -> None:
        self.assertEqual(migrate(self.database, "g001"), 1)
        connection = self.connect()
        connection.execute(
            "INSERT INTO customers_current "
            "(id, generation, email, full_name, status, created_at, updated_at) "
            "VALUES (1, 'g001', 'alice@example.test', 'Alice Adams', 'active', "
            "'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
        )
        connection.commit()
        self.assertNotIn(
            "phone",
            {row[1] for row in connection.execute("PRAGMA table_info(customers_current)")},
        )

        self.assertEqual(
            migrate(self.database, "g001", target_version=2),
            2,
        )
        self.assertEqual(
            migrate(self.database, "g001", target_version=2),
            2,
        )
        self.assertEqual(migrate(self.database, "g001"), 2)
        columns = {
            row[1]: (row[2], row[3])
            for row in connection.execute("PRAGMA table_info(customers_current)")
        }
        self.assertEqual(columns["phone"], ("TEXT", 0))
        self.assertEqual(
            connection.execute(
                "SELECT phone FROM customers_current WHERE id = 1"
            ).fetchone(),
            (None,),
        )
        self.assertEqual(
            connection.execute(
                "SELECT version, name FROM schema_migrations ORDER BY version"
            ).fetchall(),
            [(1, "initial"), (2, "customers_phone")],
        )

        for target in (0, 3):
            with self.subTest(target=target):
                with self.assertRaisesRegex(MigrationError, "target migration version"):
                    migrate(self.database, "g001", target_version=target)

    def test_raw_archive_preserves_blob_headers_and_rejects_duplicate_coordinate(self) -> None:
        migrate(self.database, "g001")
        connection = self.connect()
        raw_values = (
            "cdc.g001.public.customers",
            2,
            7,
            "g001",
            sqlite3.Binary(b"\x00key"),
            sqlite3.Binary(b"\xffvalue"),
            1234,
            "u",
            "customers",
            "1",
            "false",
            42,
            '["41","42"]',
            9,
            "tx-9",
            1,
            1,
            1200,
            "applied",
            "2026-01-01T00:00:00.000Z",
        )
        connection.execute(
            "INSERT INTO raw_events ("
            "topic, partition, offset, generation, message_key, message_value, "
            "kafka_timestamp_ms, operation, source_table, entity_key, snapshot_marker, "
            "source_lsn, source_sequence, source_tx_id, transaction_id, "
            "transaction_total_order, transaction_data_collection_order, "
            "source_timestamp_ms, processing_status, durable_committed_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            raw_values,
        )
        connection.executemany(
            "INSERT INTO raw_headers "
            "(topic, partition, offset, header_index, header_key, header_value) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (raw_values[0], 2, 7, 0, "trace", sqlite3.Binary(b"first")),
                (raw_values[0], 2, 7, 1, "trace", None),
            ],
        )
        stored = connection.execute(
            "SELECT message_key, message_value, delivery_count FROM raw_events"
        ).fetchone()
        self.assertEqual(stored, (b"\x00key", b"\xffvalue", 1))
        self.assertEqual(
            connection.execute(
                "SELECT header_index, header_key, header_value "
                "FROM raw_headers ORDER BY header_index"
            ).fetchall(),
            [(0, "trace", b"first"), (1, "trace", None)],
        )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO raw_events ("
                "topic, partition, offset, generation, message_key, source_table, "
                "entity_key, processing_status, durable_committed_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    raw_values[0],
                    2,
                    7,
                    "g001",
                    b"duplicate",
                    "customers",
                    "1",
                    "stale",
                    "2026-01-01T00:00:01.000Z",
                ),
            )

    def test_projection_tables_are_independent_and_watermark_survives_delete(self) -> None:
        migrate(self.database, "g001")
        connection = self.connect()
        connection.execute(
            "INSERT INTO orders_current "
            "(id, generation, customer_id, status, total_amount, created_at, updated_at) "
            "VALUES (103, 'g001', 999, 'pending', '15.00', 'created', 'updated')"
        )
        connection.execute(
            "INSERT INTO entity_watermarks ("
            "generation, source_table, entity_key, phase_rank, source_lsn, "
            "transaction_total_order, transaction_data_collection_order, "
            "last_was_delete, updated_at"
            ") VALUES ('g001', 'customers', '3', 1, 200, 7, 3, 1, 'updated')"
        )
        self.assertIsNone(
            connection.execute("SELECT id FROM customers_current WHERE id = 999").fetchone()
        )
        self.assertEqual(
            connection.execute(
                "SELECT phase_rank, source_lsn, transaction_total_order, "
                "transaction_data_collection_order, last_was_delete "
                "FROM entity_watermarks WHERE entity_key = '3'"
            ).fetchone(),
            (1, 200, 7, 3, 1),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE entity_watermarks SET phase_rank = 2 WHERE entity_key = '3'"
            )

    def test_ingestion_errors_keep_original_bytes_and_attempt_count(self) -> None:
        migrate(self.database, "g001")
        connection = self.connect()
        connection.execute(
            "INSERT INTO ingestion_errors ("
            "topic, partition, offset, generation, message_key, message_value, "
            "error_class, error_message, first_seen_at, last_seen_at"
            ") VALUES ('topic', 0, 4, 'g001', ?, ?, 'invalid_json', 'bad value', 'first', 'first')",
            (sqlite3.Binary(b"key"), sqlite3.Binary(b"\xffbad")),
        )
        connection.execute(
            "UPDATE ingestion_errors SET attempt_count = attempt_count + 1, "
            "last_seen_at = 'second' WHERE topic = 'topic' AND partition = 0 AND offset = 4"
        )
        self.assertEqual(
            connection.execute(
                "SELECT message_key, message_value, attempt_count, last_seen_at "
                "FROM ingestion_errors"
            ).fetchone(),
            (b"key", b"\xffbad", 2, "second"),
        )

    def test_generation_mismatch_is_rejected_without_rebinding_sink(self) -> None:
        migrate(self.database, "g001")
        with self.assertRaisesRegex(MigrationError, "different generation"):
            migrate(self.database, "g002")
        connection = self.connect()
        self.assertEqual(
            connection.execute("SELECT generation FROM sink_metadata").fetchone()[0],
            "g001",
        )

    def test_generation_scoped_tables_reject_mixed_rows_and_updates(self) -> None:
        migrate(self.database, "g001")
        connection = self.connect()
        mixed_generation_inserts = (
            (
                "raw_events",
                lambda: connection.execute(
                    "INSERT INTO raw_events ("
                    "topic, partition, offset, generation, message_key, source_table, "
                    "entity_key, processing_status, durable_committed_at"
                    ") VALUES ('topic', 0, 1, 'g002', X'01', 'customers', '1', "
                    "'applied', 'now')"
                ),
            ),
            (
                "ingestion_errors",
                lambda: connection.execute(
                    "INSERT INTO ingestion_errors ("
                    "topic, partition, offset, generation, error_class, error_message, "
                    "first_seen_at, last_seen_at"
                    ") VALUES ('topic', 0, 1, 'g002', 'invalid', 'bad', 'first', 'last')"
                ),
            ),
            (
                "customers_current",
                lambda: connection.execute(
                    "INSERT INTO customers_current "
                    "(id, generation, email, full_name, status, created_at, updated_at) "
                    "VALUES (1, 'g002', 'a@example.test', 'A', 'active', 'created', 'updated')"
                ),
            ),
            (
                "orders_current",
                lambda: connection.execute(
                    "INSERT INTO orders_current "
                    "(id, generation, customer_id, status, total_amount, created_at, updated_at) "
                    "VALUES (1, 'g002', 1, 'pending', '1.00', 'created', 'updated')"
                ),
            ),
            (
                "payments_current",
                lambda: connection.execute(
                    "INSERT INTO payments_current "
                    "(id, generation, order_id, status, amount, provider_ref, created_at, updated_at) "
                    "VALUES (1, 'g002', 1, 'captured', '1.00', 'ref', 'created', 'updated')"
                ),
            ),
            (
                "entity_watermarks",
                lambda: connection.execute(
                    "INSERT INTO entity_watermarks ("
                    "generation, source_table, entity_key, phase_rank, source_lsn, "
                    "transaction_total_order, transaction_data_collection_order, "
                    "last_was_delete, updated_at"
                    ") VALUES ('g002', 'customers', '1', 1, 1, 1, 1, 0, 'updated')"
                ),
            ),
        )
        for table, insert_mixed_generation in mixed_generation_inserts:
            with self.subTest(table=table):
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "generation does not match sink"
                ):
                    insert_mixed_generation()
        connection.execute(
            "INSERT INTO raw_events ("
            "topic, partition, offset, generation, message_key, source_table, "
            "entity_key, processing_status, durable_committed_at"
            ") VALUES ('topic', 0, 1, 'g001', X'01', 'customers', '1', "
            "'applied', 'now')"
        )
        connection.execute(
            "INSERT INTO ingestion_errors ("
            "topic, partition, offset, generation, error_class, error_message, "
            "first_seen_at, last_seen_at"
            ") VALUES ('topic', 0, 2, 'g001', 'invalid', 'bad', 'first', 'last')"
        )
        connection.execute(
            "INSERT INTO customers_current "
            "(id, generation, email, full_name, status, created_at, updated_at) "
            "VALUES (1, 'g001', 'a@example.test', 'A', 'active', 'created', 'updated')"
        )
        connection.execute(
            "INSERT INTO orders_current "
            "(id, generation, customer_id, status, total_amount, created_at, updated_at) "
            "VALUES (1, 'g001', 1, 'pending', '1.00', 'created', 'updated')"
        )
        connection.execute(
            "INSERT INTO payments_current "
            "(id, generation, order_id, status, amount, provider_ref, created_at, updated_at) "
            "VALUES (1, 'g001', 1, 'captured', '1.00', 'ref', 'created', 'updated')"
        )
        connection.execute(
            "INSERT INTO entity_watermarks ("
            "generation, source_table, entity_key, phase_rank, source_lsn, "
            "transaction_total_order, transaction_data_collection_order, "
            "last_was_delete, updated_at"
            ") VALUES ('g001', 'customers', '1', 1, 1, 1, 1, 0, 'updated')"
        )
        mixed_generation_updates = (
            (
                "raw_events",
                lambda: connection.execute(
                    "UPDATE raw_events SET generation = 'g002' "
                    "WHERE topic = 'topic' AND partition = 0 AND offset = 1"
                ),
            ),
            (
                "ingestion_errors",
                lambda: connection.execute(
                    "UPDATE ingestion_errors SET generation = 'g002' "
                    "WHERE topic = 'topic' AND partition = 0 AND offset = 2"
                ),
            ),
            (
                "customers_current",
                lambda: connection.execute(
                    "UPDATE customers_current SET generation = 'g002' WHERE id = 1"
                ),
            ),
            (
                "orders_current",
                lambda: connection.execute(
                    "UPDATE orders_current SET generation = 'g002' WHERE id = 1"
                ),
            ),
            (
                "payments_current",
                lambda: connection.execute(
                    "UPDATE payments_current SET generation = 'g002' WHERE id = 1"
                ),
            ),
            (
                "entity_watermarks",
                lambda: connection.execute(
                    "UPDATE entity_watermarks SET generation = 'g002' "
                    "WHERE generation = 'g001' AND source_table = 'customers' "
                    "AND entity_key = '1'"
                ),
            ),
        )
        for table, update_generation in mixed_generation_updates:
            with self.subTest(table=table):
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "generation does not match sink"
                ):
                    update_generation()
        binding_mutations = (
            lambda: connection.execute(
                "INSERT OR REPLACE INTO sink_metadata "
                "(singleton, generation, created_at) VALUES (1, 'g002', 'replacement')"
            ),
            lambda: connection.execute(
                "UPDATE sink_metadata SET generation = 'g002' WHERE singleton = 1"
            ),
            lambda: connection.execute("DELETE FROM sink_metadata WHERE singleton = 1"),
        )
        for mutate_binding in binding_mutations:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "binding is immutable"):
                mutate_binding()
        self.assertEqual(
            connection.execute("SELECT generation FROM sink_metadata").fetchone()[0],
            "g001",
        )
        preserved_generations = (
            connection.execute("SELECT generation FROM raw_events").fetchone()[0],
            connection.execute("SELECT generation FROM ingestion_errors").fetchone()[0],
            connection.execute("SELECT generation FROM customers_current").fetchone()[0],
            connection.execute("SELECT generation FROM orders_current").fetchone()[0],
            connection.execute("SELECT generation FROM payments_current").fetchone()[0],
            connection.execute("SELECT generation FROM entity_watermarks").fetchone()[0],
        )
        self.assertEqual(preserved_generations, ("g001",) * 6)

    def test_changed_applied_migration_is_rejected(self) -> None:
        migrate(self.database, "g001")
        connection = self.connect()
        connection.execute(
            "UPDATE schema_migrations SET checksum_sha256 = ? WHERE version = 1",
            ("0" * 64,),
        )
        connection.commit()
        with self.assertRaisesRegex(MigrationError, "does not match source"):
            migrate(self.database, "g001")

    def test_failed_migration_rolls_back_ddl_and_version_record(self) -> None:
        migrate(self.database, "g001")
        migrations = load_migrations()[:1]
        broken = Migration(
            version=2,
            name="broken_probe",
            sql=(
                "CREATE TABLE should_rollback (id INTEGER PRIMARY KEY) STRICT;\n"
                "INSERT INTO missing_table VALUES (1);"
            ),
            checksum_sha256="1" * 64,
        )
        connection = open_database(self.database)
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(MigrationError, "failed to apply"):
            apply_migrations(connection, (*migrations, broken), "g001")
        self.assertIsNone(
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name = 'should_rollback'"
            ).fetchone()
        )
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
            1,
        )

        inline_commit = Migration(
            version=2,
            name="inline_commit_probe",
            sql=(
                "CREATE TABLE inline_leak (id INTEGER PRIMARY KEY) STRICT; COMMIT;\n"
                "INSERT INTO missing_table VALUES (1);"
            ),
            checksum_sha256="2" * 64,
        )
        with self.assertRaisesRegex(MigrationError, "failed to apply"):
            apply_migrations(connection, (*migrations, inline_commit), "g001")
        self.assertIsNone(
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name = 'inline_leak'"
            ).fetchone()
        )
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
            1,
        )

    def test_wrong_generation_does_not_apply_pending_migration(self) -> None:
        migrate(self.database, "g001")
        pending = Migration(
            version=2,
            name="pending_probe",
            sql="CREATE TABLE pending_leak (id INTEGER PRIMARY KEY) STRICT;",
            checksum_sha256="3" * 64,
        )
        connection = open_database(self.database)
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(MigrationError, "different generation"):
            apply_migrations(connection, (*load_migrations()[:1], pending), "g002")
        self.assertIsNone(
            connection.execute(
                "SELECT name FROM sqlite_schema WHERE name = 'pending_leak'"
            ).fetchone()
        )
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
            1,
        )

    def test_initial_binding_and_schema_roll_back_together(self) -> None:
        connection = open_database(self.database)
        self.addCleanup(connection.close)
        failing_initial = Migration(
            version=1,
            name="failing_initial",
            sql=(
                "CREATE TABLE schema_migrations ("
                "version INTEGER PRIMARY KEY CHECK (version < 0), "
                "name TEXT NOT NULL, checksum_sha256 TEXT NOT NULL, applied_at TEXT NOT NULL"
                ") STRICT;"
                "CREATE TABLE sink_metadata ("
                "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
                "generation TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL"
                ") STRICT;"
            ),
            checksum_sha256="4" * 64,
        )
        with self.assertRaisesRegex(MigrationError, "failed to apply"):
            apply_migrations(connection, (failing_initial,), "g001")
        tables = connection.execute(
            "SELECT name FROM sqlite_schema "
            "WHERE name IN ('schema_migrations', 'sink_metadata')"
        ).fetchall()
        self.assertEqual(tables, [])

    def test_invalid_migration_set_is_rejected(self) -> None:
        migration = load_migrations()[0]
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        invalid_cases = (
            (replace(migration, version=2), "contiguous"),
            (replace(migration, name="unsafe-name"), "invalid name"),
            (replace(migration, checksum_sha256="not-a-checksum"), "invalid checksum"),
        )
        for invalid_migration, expected_error in invalid_cases:
            with self.subTest(expected_error=expected_error):
                with self.assertRaisesRegex(MigrationError, expected_error):
                    apply_migrations(connection, (invalid_migration,), "g001")


if __name__ == "__main__":
    unittest.main()
