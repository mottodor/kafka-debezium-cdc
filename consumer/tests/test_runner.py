from __future__ import annotations

import pathlib
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import dataclass
from typing import Callable

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "consumer" / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from cdc_consumer.processing import ProcessResult
from cdc_consumer.runner import (
    SQLITE_MAX_BYTES,
    SQLITE_STOP_HEADROOM_BYTES,
    FaultAfterDurableCommit,
    KafkaCoordinate,
    KafkaMessage,
    KafkaPollError,
    OffsetCommitError,
    RunnerConfig,
    RunnerError,
    SQLiteSizeLimitError,
    commit_message_synchronously,
    configure_sqlite_size_guard,
    kafka_record_from_message,
    process_message_and_commit,
    run_consumer,
)


@dataclass
class FakeTopicPartition:
    topic: str
    partition: int
    offset: int
    error: object | None = None


class FakeMessage:
    def __init__(
        self,
        *,
        topic: str = "cdc.g001.public.customers",
        partition: int = 1,
        offset: int = 7,
        key: bytes | None = b"key-bytes",
        value: bytes | None = b"value-bytes",
        headers: list[tuple[str, bytes | None]] | None = None,
        timestamp: tuple[int, int] = (1, 1234),
        error: object | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._headers = headers
        self._timestamp = timestamp
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return self._headers

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def error(self) -> object | None:
        return self._error


class FakeConsumer:
    def __init__(
        self,
        messages: list[KafkaMessage] | None = None,
        *,
        commit_hook: Callable[[KafkaMessage], None] | None = None,
    ) -> None:
        self.messages = list(messages or [])
        self.commit_hook = commit_hook
        self.subscriptions: list[list[str]] = []
        self.commits: list[tuple[KafkaMessage, bool]] = []
        self.commit_result_factory: Callable[[KafkaMessage], list[FakeTopicPartition] | None] = (
            lambda message: [
                FakeTopicPartition(
                    message.topic() or "", message.partition(), message.offset() + 1
                )
            ]
        )
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float) -> KafkaMessage | None:
        del timeout
        return self.messages.pop(0) if self.messages else None

    def commit(
        self, *, message: KafkaMessage, asynchronous: bool
    ) -> list[FakeTopicPartition] | None:
        self.commits.append((message, asynchronous))
        if self.commit_hook is not None:
            self.commit_hook(message)
        return self.commit_result_factory(message)

    def close(self) -> None:
        self.closed = True


def config(
    *,
    max_messages: int | None = 1,
    fault: KafkaCoordinate | None = None,
    sqlite_max_bytes: int = SQLITE_MAX_BYTES,
    sqlite_stop_headroom_bytes: int = SQLITE_STOP_HEADROOM_BYTES,
) -> RunnerConfig:
    return RunnerConfig(
        generation="g001",
        bootstrap_servers="kafka:9092",
        consumer_group="sqlite-projector-g001",
        topics=(
            "cdc.g001.public.customers",
            "cdc.g001.public.orders",
            "cdc.g001.public.payments",
        ),
        sqlite_path=pathlib.Path("/data/cdc-g001.sqlite3"),
        poll_timeout_seconds=0.1,
        max_messages=max_messages,
        fault_after_sqlite_commit_at=fault,
        sqlite_max_bytes=sqlite_max_bytes,
        sqlite_stop_headroom_bytes=sqlite_stop_headroom_bytes,
    )


class RunnerTests(unittest.TestCase):
    def test_compose_consumer_requires_explicit_restart_after_failure(self) -> None:
        compose_text = (PROJECT_ROOT / "compose.yaml").read_text()
        consumer_block = compose_text.split("\n  consumer:\n", 1)[1].split(
            "\nvolumes:\n", 1
        )[0]
        self.assertIn('    restart: "no"\n', consumer_block)
        self.assertIn("      CDC_MAX_MESSAGES: ${CDC_MAX_MESSAGES:-}\n", consumer_block)
        self.assertIn(
            "      CDC_FAULT_AFTER_SQLITE_COMMIT_AT: "
            "${CDC_FAULT_AFTER_SQLITE_COMMIT_AT:-}\n",
            consumer_block,
        )
        lab_text = (PROJECT_ROOT / "scripts" / "lab").read_text()
        self.assertIn("scripts/lab migrate g001", lab_text)
        self.assertIn('[[ "$(service_state consumer)" != "running" ]]', lab_text)
        self.assertIn("--network none", lab_text)
        self.assertIn("--target-version 2", lab_text)
        for heading in (
            "--- kafka_consumer_group_lag_by_partition ---",
            "--- durable_event_age_ms ---",
            "--- postgres_replication_slot_retained_wal ---",
            "--- kafka_topic_retention_bounds ---",
            "--- sqlite_sink_size_guard ---",
        ):
            self.assertIn(heading, lab_text)
        topic_script = (
            PROJECT_ROOT / "config" / "kafka" / "bootstrap-topics.sh"
        ).read_text()
        self.assertIn('[[ "$1" == "--verify-only" ]]', topic_script)
        self.assertIn('if [[ "$verify_only" == false ]]; then', topic_script)
        common_script = (PROJECT_ROOT / "scripts" / "lib" / "common.sh").read_text()
        self.assertIn("retained_wal_bytes >= 512 * 1024 * 1024", common_script)
        self.assertIn(
            '[[ "$threshold" == "WARNING" ]]',
            common_script,
        )
        runner_text = (SOURCE_ROOT / "cdc_consumer" / "runner.py").read_text()
        self.assertIn("BASE_SCHEMA_VERSION = 1", runner_text)
        self.assertIn("SQLITE_MAX_BYTES = 1024 * 1024 * 1024", runner_text)
        self.assertIn("SQLITE_STOP_HEADROOM_BYTES = 16 * 1024 * 1024", runner_text)
        self.assertIn("target_version=BASE_SCHEMA_VERSION", runner_text)

    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.connection = sqlite3.connect(
            pathlib.Path(temporary_directory.name) / "runner.sqlite3",
            isolation_level=None,
        )
        self.addCleanup(self.connection.close)
        self.connection.execute("CREATE TABLE durable_probe (value TEXT NOT NULL)")

    def test_environment_is_generation_scoped_and_disables_automatic_offsets(self) -> None:
        environment = {
            "CDC_GENERATION": "g001",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_CONSUMER_GROUP": "sqlite-projector-g001",
            "KAFKA_CUSTOMERS_TOPIC": "cdc.g001.public.customers",
            "KAFKA_ORDERS_TOPIC": "cdc.g001.public.orders",
            "KAFKA_PAYMENTS_TOPIC": "cdc.g001.public.payments",
            "SQLITE_SINK_PATH": "/data/cdc-g001.sqlite3",
            "CDC_POLL_TIMEOUT_SECONDS": "0.5",
            "CDC_MAX_MESSAGES": "2",
            "CDC_FAULT_AFTER_SQLITE_COMMIT_AT": (
                "cdc.g001.public.customers:1:7"
            ),
        }
        parsed = RunnerConfig.from_environment(environment)
        self.assertEqual(parsed.max_messages, 2)
        self.assertEqual(
            parsed.fault_after_sqlite_commit_at,
            KafkaCoordinate("cdc.g001.public.customers", 1, 7),
        )
        kafka_configuration = parsed.kafka_configuration()
        self.assertIs(kafka_configuration["enable.auto.commit"], False)
        self.assertIs(kafka_configuration["enable.auto.offset.store"], False)
        self.assertEqual(kafka_configuration["auto.offset.reset"], "earliest")
        self.assertIs(kafka_configuration["allow.auto.create.topics"], False)

        mutations = (
            {**environment, "CDC_GENERATION": "generation-one"},
            {**environment, "KAFKA_CONSUMER_GROUP": "other-group"},
            {**environment, "KAFKA_ORDERS_TOPIC": "cdc.g002.public.orders"},
            {**environment, "SQLITE_SINK_PATH": "/data/other.sqlite3"},
            {**environment, "CDC_MAX_MESSAGES": "0"},
            {
                **environment,
                "CDC_FAULT_AFTER_SQLITE_COMMIT_AT": "unknown:0:0",
            },
        )
        for mutated in mutations:
            with self.subTest(mutated=mutated):
                with self.assertRaises(RunnerError):
                    RunnerConfig.from_environment(mutated)

    def test_message_conversion_preserves_raw_bytes_headers_and_timestamp(self) -> None:
        message = FakeMessage(
            key=b"\x00key",
            value=b"\xffvalue",
            headers=[("trace", b"one"), ("trace", None)],
            timestamp=(1, 9876),
        )
        record = kafka_record_from_message(message)
        self.assertEqual(record.key, b"\x00key")
        self.assertEqual(record.value, b"\xffvalue")
        self.assertEqual(record.headers, (("trace", b"one"), ("trace", None)))
        self.assertEqual(record.timestamp_ms, 9876)

        no_timestamp = kafka_record_from_message(FakeMessage(timestamp=(0, -1)))
        self.assertIsNone(no_timestamp.timestamp_ms)

    def test_sqlite_size_guard_sets_hard_limit_and_stops_before_poll(self) -> None:
        page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
        page_count = self.connection.execute("PRAGMA page_count").fetchone()[0]
        max_bytes = (page_count + 2) * page_size
        headroom_bytes = 2 * page_size

        with self.assertRaisesRegex(SQLiteSizeLimitError, "controlled stop threshold"):
            configure_sqlite_size_guard(
                self.connection,
                max_bytes=max_bytes,
                stop_headroom_bytes=headroom_bytes,
            )
        self.assertEqual(
            self.connection.execute("PRAGMA max_page_count").fetchone(),
            (max_bytes // page_size,),
        )

        consumer = FakeConsumer([FakeMessage()])
        with self.assertRaisesRegex(SQLiteSizeLimitError, "controlled stop threshold"):
            run_consumer(
                consumer,
                self.connection,
                config(
                    sqlite_max_bytes=max_bytes,
                    sqlite_stop_headroom_bytes=headroom_bytes,
                ),
            )
        self.assertEqual(consumer.subscriptions, [])
        self.assertEqual(consumer.commits, [])

    def test_run_commits_only_after_durable_processor_return(self) -> None:
        trace: list[str] = []
        message = FakeMessage()

        def processor(connection, generation, record):
            self.assertEqual(generation, "g001")
            self.assertEqual(record.offset, 7)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("INSERT INTO durable_probe VALUES ('committed')")
            connection.execute("COMMIT")
            trace.append("sqlite-committed")
            return ProcessResult("applied", False, None)

        def commit_hook(_message: KafkaMessage) -> None:
            self.assertFalse(self.connection.in_transaction)
            self.assertEqual(
                self.connection.execute("SELECT value FROM durable_probe").fetchone(),
                ("committed",),
            )
            trace.append("kafka-committed")

        consumer = FakeConsumer([message], commit_hook=commit_hook)
        output: list[str] = []
        processed = run_consumer(
            consumer,
            self.connection,
            config(),
            processor=processor,
            emit=output.append,
        )
        self.assertEqual(processed, 1)
        self.assertEqual(trace, ["sqlite-committed", "kafka-committed"])
        self.assertEqual(consumer.commits, [(message, False)])
        self.assertEqual(consumer.subscriptions, [list(config().topics)])
        self.assertIn('"committed_offset": 8', output[0])

    def test_processing_failure_never_attempts_offset_commit(self) -> None:
        consumer = FakeConsumer()
        message = FakeMessage()

        def processor(_connection, _generation, _record):
            raise RuntimeError("processor failed")

        with self.assertRaisesRegex(RuntimeError, "processor failed"):
            process_message_and_commit(
                consumer,
                self.connection,
                config(),
                message,
                processor=processor,
            )
        self.assertEqual(consumer.commits, [])

    def test_fault_window_leaves_durable_state_without_offset_commit(self) -> None:
        message = FakeMessage()
        coordinate = KafkaCoordinate(message.topic(), message.partition(), message.offset())
        consumer = FakeConsumer()

        def first_delivery(connection, _generation, _record):
            connection.execute("INSERT INTO durable_probe VALUES ('first-delivery')")
            return ProcessResult("applied", False, None)

        with self.assertRaisesRegex(FaultAfterDurableCommit, "controlled fault"):
            process_message_and_commit(
                consumer,
                self.connection,
                config(fault=coordinate),
                message,
                processor=first_delivery,
            )
        self.assertEqual(
            self.connection.execute("SELECT value FROM durable_probe").fetchall(),
            [("first-delivery",)],
        )
        self.assertEqual(consumer.commits, [])

        def duplicate_delivery(_connection, _generation, _record):
            return ProcessResult("applied", True, None)

        _, duplicate, committed_offset = process_message_and_commit(
            consumer,
            self.connection,
            config(fault=coordinate),
            message,
            processor=duplicate_delivery,
        )
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(committed_offset, 8)
        self.assertEqual(len(consumer.commits), 1)

    def test_synchronous_commit_checks_partition_error_and_exact_offset(self) -> None:
        message = FakeMessage()
        consumer = FakeConsumer()
        self.assertEqual(commit_message_synchronously(consumer, message), 8)

        failure_cases = (
            [],
            [FakeTopicPartition(message.topic(), message.partition(), 8, "denied")],
            [FakeTopicPartition(message.topic(), message.partition(), 7)],
            [FakeTopicPartition(message.topic(), 2, 8)],
        )
        for result in failure_cases:
            with self.subTest(result=result):
                consumer.commit_result_factory = lambda _message, result=result: result
                with self.assertRaises(OffsetCommitError):
                    commit_message_synchronously(consumer, message)

        def raise_commit(_message: KafkaMessage):
            raise RuntimeError("broker unavailable")

        consumer.commit_result_factory = raise_commit
        with self.assertRaisesRegex(OffsetCommitError, "broker unavailable"):
            commit_message_synchronously(consumer, message)

    def test_poll_error_fails_before_processing_or_commit(self) -> None:
        consumer = FakeConsumer()
        called = False

        def processor(_connection, _generation, _record):
            nonlocal called
            called = True
            return ProcessResult("applied", False, None)

        with self.assertRaisesRegex(KafkaPollError, "partition unavailable"):
            process_message_and_commit(
                consumer,
                self.connection,
                config(),
                FakeMessage(error="partition unavailable"),
                processor=processor,
            )
        self.assertFalse(called)
        self.assertEqual(consumer.commits, [])


if __name__ == "__main__":
    unittest.main()
