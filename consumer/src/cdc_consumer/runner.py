"""Sequential Kafka consumer with a durable SQLite-before-offset boundary."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from cdc_consumer.migrations import migrate, open_database
from cdc_consumer.processing import KafkaRecord, ProcessResult, process_record

_GENERATION_PATTERN = re.compile(r"g[0-9]{3}")


class RunnerError(RuntimeError):
    """Base error for consumer-loop failures."""


class KafkaPollError(RunnerError):
    """A polled Kafka event was an error rather than a record."""


class OffsetCommitError(RunnerError):
    """The durable record's Kafka offset could not be confirmed."""


class FaultAfterDurableCommit(RunnerError):
    """Controlled AC-004 stop after SQLite commit and before Kafka commit."""


class KafkaMessage(Protocol):
    def topic(self) -> str | None:
        raise AssertionError("protocol method")

    def partition(self) -> int:
        raise AssertionError("protocol method")

    def offset(self) -> int:
        raise AssertionError("protocol method")

    def key(self) -> bytes | None:
        raise AssertionError("protocol method")

    def value(self) -> bytes | None:
        raise AssertionError("protocol method")

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        raise AssertionError("protocol method")

    def timestamp(self) -> tuple[int, int]:
        raise AssertionError("protocol method")

    def error(self) -> object | None:
        raise AssertionError("protocol method")


class KafkaConsumer(Protocol):
    def subscribe(self, topics: list[str]) -> None:
        raise AssertionError("protocol method")

    def poll(self, timeout: float) -> KafkaMessage | None:
        raise AssertionError("protocol method")

    def commit(
        self, *, message: KafkaMessage, asynchronous: bool
    ) -> Sequence[object] | None:
        raise AssertionError("protocol method")

    def close(self) -> None:
        raise AssertionError("protocol method")


@dataclass(frozen=True)
class KafkaCoordinate:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class RunnerConfig:
    generation: str
    bootstrap_servers: str
    consumer_group: str
    topics: tuple[str, str, str]
    sqlite_path: Path
    poll_timeout_seconds: float = 1.0
    max_messages: int | None = None
    fault_after_sqlite_commit_at: KafkaCoordinate | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> RunnerConfig:
        values = os.environ if environment is None else environment

        def required(name: str) -> str:
            value = values.get(name, "").strip()
            if not value:
                raise RunnerError(f"required environment variable is empty: {name}")
            return value

        generation = required("CDC_GENERATION")
        if _GENERATION_PATTERN.fullmatch(generation) is None:
            raise RunnerError("CDC_GENERATION must match gNNN")

        expected_topics = (
            f"cdc.{generation}.public.customers",
            f"cdc.{generation}.public.orders",
            f"cdc.{generation}.public.payments",
        )
        topics = (
            required("KAFKA_CUSTOMERS_TOPIC"),
            required("KAFKA_ORDERS_TOPIC"),
            required("KAFKA_PAYMENTS_TOPIC"),
        )
        if topics != expected_topics:
            raise RunnerError(
                "Kafka topics do not match the generation-scoped customers/orders/payments contract"
            )
        if len(set(topics)) != len(topics):
            raise RunnerError("Kafka topics must be distinct")

        consumer_group = required("KAFKA_CONSUMER_GROUP")
        if consumer_group != f"sqlite-projector-{generation}":
            raise RunnerError("Kafka consumer group does not match CDC_GENERATION")

        sqlite_path = Path(required("SQLITE_SINK_PATH"))
        expected_sink = Path(f"/data/cdc-{generation}.sqlite3")
        if sqlite_path != expected_sink:
            raise RunnerError("SQLite sink path does not match CDC_GENERATION")

        timeout_text = values.get("CDC_POLL_TIMEOUT_SECONDS", "1.0").strip()
        try:
            poll_timeout = float(timeout_text)
        except ValueError as error:
            raise RunnerError("CDC_POLL_TIMEOUT_SECONDS must be numeric") from error
        if not 0.0 < poll_timeout <= 60.0:
            raise RunnerError("CDC_POLL_TIMEOUT_SECONDS must be in (0, 60]")

        max_messages: int | None = None
        max_messages_text = values.get("CDC_MAX_MESSAGES", "").strip()
        if max_messages_text:
            try:
                max_messages = int(max_messages_text)
            except ValueError as error:
                raise RunnerError("CDC_MAX_MESSAGES must be a positive integer") from error
            if max_messages <= 0:
                raise RunnerError("CDC_MAX_MESSAGES must be a positive integer")

        fault_coordinate: KafkaCoordinate | None = None
        fault_text = values.get("CDC_FAULT_AFTER_SQLITE_COMMIT_AT", "").strip()
        if fault_text:
            try:
                topic, partition_text, offset_text = fault_text.rsplit(":", 2)
                fault_coordinate = KafkaCoordinate(
                    topic=topic,
                    partition=int(partition_text),
                    offset=int(offset_text),
                )
            except (ValueError, TypeError) as error:
                raise RunnerError(
                    "CDC_FAULT_AFTER_SQLITE_COMMIT_AT must be topic:partition:offset"
                ) from error
            if (
                fault_coordinate.topic not in topics
                or fault_coordinate.partition < 0
                or fault_coordinate.offset < 0
            ):
                raise RunnerError(
                    "CDC_FAULT_AFTER_SQLITE_COMMIT_AT must identify a configured topic "
                    "with non-negative partition and offset"
                )

        return cls(
            generation=generation,
            bootstrap_servers=required("KAFKA_BOOTSTRAP_SERVERS"),
            consumer_group=consumer_group,
            topics=topics,
            sqlite_path=sqlite_path,
            poll_timeout_seconds=poll_timeout,
            max_messages=max_messages,
            fault_after_sqlite_commit_at=fault_coordinate,
        )

    def kafka_configuration(self) -> dict[str, object]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group,
            "client.id": f"{self.consumer_group}-consumer",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "isolation.level": "read_committed",
        }


def kafka_record_from_message(message: KafkaMessage) -> KafkaRecord:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if not isinstance(topic, str) or not topic:
        raise RunnerError("Kafka message topic is empty")
    if isinstance(partition, bool) or not isinstance(partition, int) or partition < 0:
        raise RunnerError("Kafka message partition is invalid")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise RunnerError("Kafka message offset is invalid")

    timestamp_type, timestamp_value = message.timestamp()
    del timestamp_type
    timestamp_ms = (
        timestamp_value
        if isinstance(timestamp_value, int)
        and not isinstance(timestamp_value, bool)
        and timestamp_value >= 0
        else None
    )
    headers = message.headers()
    return KafkaRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        key=message.key(),
        value=message.value(),
        headers=tuple(headers or ()),
        timestamp_ms=timestamp_ms,
    )


def commit_message_synchronously(consumer: KafkaConsumer, message: KafkaMessage) -> int:
    expected_offset = message.offset() + 1
    try:
        committed = consumer.commit(message=message, asynchronous=False)
    except Exception as error:
        raise OffsetCommitError(
            f"synchronous Kafka commit failed for {message.topic()}:"
            f"{message.partition()}:{message.offset()}: {error}"
        ) from error
    if committed is None or len(committed) != 1:
        raise OffsetCommitError("synchronous Kafka commit returned no exact partition result")

    partition_result = committed[0]
    result_error = getattr(partition_result, "error", None)
    if result_error is not None:
        raise OffsetCommitError(
            f"synchronous Kafka commit reported a partition error: {result_error}"
        )
    actual = (
        getattr(partition_result, "topic", None),
        getattr(partition_result, "partition", None),
        getattr(partition_result, "offset", None),
    )
    expected = (message.topic(), message.partition(), expected_offset)
    if actual != expected:
        raise OffsetCommitError(
            f"synchronous Kafka commit result mismatch: expected {expected!r}, got {actual!r}"
        )
    return expected_offset


def process_message_and_commit(
    consumer: KafkaConsumer,
    connection: sqlite3.Connection,
    config: RunnerConfig,
    message: KafkaMessage,
    *,
    processor: Callable[[sqlite3.Connection, str, KafkaRecord], ProcessResult] = process_record,
) -> tuple[KafkaRecord, ProcessResult, int]:
    message_error = message.error()
    if message_error is not None:
        raise KafkaPollError(f"Kafka poll returned an error: {message_error}")

    record = kafka_record_from_message(message)
    result = processor(connection, config.generation, record)
    fault_coordinate = config.fault_after_sqlite_commit_at
    if (
        fault_coordinate is not None
        and not result.duplicate
        and fault_coordinate
        == KafkaCoordinate(record.topic, record.partition, record.offset)
    ):
        raise FaultAfterDurableCommit(
            "controlled fault after durable SQLite commit and before Kafka offset commit "
            f"at {record.topic}:{record.partition}:{record.offset}"
        )
    committed_offset = commit_message_synchronously(consumer, message)
    return record, result, committed_offset


def run_consumer(
    consumer: KafkaConsumer,
    connection: sqlite3.Connection,
    config: RunnerConfig,
    *,
    processor: Callable[[sqlite3.Connection, str, KafkaRecord], ProcessResult] = process_record,
    emit: Callable[[str], None] = print,
) -> int:
    consumer.subscribe(list(config.topics))
    committed_messages = 0
    while config.max_messages is None or committed_messages < config.max_messages:
        message = consumer.poll(config.poll_timeout_seconds)
        if message is None:
            continue
        record, result, committed_offset = process_message_and_commit(
            consumer,
            connection,
            config,
            message,
            processor=processor,
        )
        committed_messages += 1
        emit(
            json.dumps(
                {
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "status": result.status,
                    "duplicate": result.duplicate,
                    "committed_offset": committed_offset,
                },
                sort_keys=True,
            )
        )
    return committed_messages


def create_kafka_consumer(config: RunnerConfig) -> KafkaConsumer:
    try:
        from confluent_kafka import Consumer  # pyright: ignore[reportMissingImports]
    except ImportError as error:
        raise RunnerError("confluent-kafka is unavailable") from error
    return Consumer(config.kafka_configuration())


def main(environment: Mapping[str, str] | None = None) -> int:
    consumer: KafkaConsumer | None = None
    connection: sqlite3.Connection | None = None
    try:
        config = RunnerConfig.from_environment(environment)
        migrate(config.sqlite_path, config.generation)
        connection = open_database(config.sqlite_path)
        consumer = create_kafka_consumer(config)
        run_consumer(consumer, connection, config)
        return 0
    except FaultAfterDurableCommit as error:
        print(str(error), file=sys.stderr)
        return 86
    except KeyboardInterrupt:
        print("consumer interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"consumer stopped: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close()
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
