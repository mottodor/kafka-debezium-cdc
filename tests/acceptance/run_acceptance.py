#!/usr/bin/env python3
"""Run generation-scoped acceptance checks and publish redacted evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_LIB = PROJECT_ROOT / "scripts" / "lib"
if str(SCRIPTS_LIB) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_LIB))

from evidence import (  # pyright: ignore[reportMissingImports]
    ACCEPTANCE_MANIFEST,
    REQUIRED_REDACTIONS,
    load_evidence,
    write_evidence,
)

LAB = PROJECT_ROOT / "scripts" / "lab"
RUNTIME_ROOT = PROJECT_ROOT / ".runtime"
CONSUMER_IMAGE = "cdc-lab-consumer:g001"
POSTGRES_CONTAINER = "cdc-lab-g001-postgres-1"
KAFKA_CONTAINER = "cdc-lab-g001-kafka-1"
CONNECT_CONTAINER = "cdc-lab-g001-connect-1"
GENERATION_PATTERN = re.compile(r"^g\d{3}$")
AC_PATTERN = re.compile(r"^AC-0(?:0[1-9]|1[0-4])$")
AC002_TARGETS = (
    ("customers", "3", "c"),
    ("orders", "103", "c"),
    ("payments", "1002", "c"),
    ("customers", "1", "u"),
    ("orders", "103", "u"),
    ("payments", "1002", "u"),
)
WAL_WARNING_BYTES = 512 * 1024 * 1024
RETENTION_PROBE_INITIAL_BYTES = 20 * 1024 * 1024
RETENTION_PROBE_TARGET_BYTES = 2 * 1024 * 1024
RETENTION_PROBE_SEGMENT_BYTES = 1024 * 1024
RETENTION_PROBE_RECORD_COUNT = 2048
RETENTION_PROBE_VALUE_BYTES = 4096


class AcceptanceError(RuntimeError):
    """An acceptance assertion could not be established."""


class RuntimeRestorationError(AcceptanceError):
    """Acceptance failed and the lab could not be restored to its running state."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def run(
    argv: list[str],
    *,
    check: bool = True,
    input_text: str | None = None,
    env: Mapping[str, str] | None = None,
    timeout: int = 180,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env is not None:
        merged_env.update(env)
    completed = subprocess.run(
        argv,
        cwd=PROJECT_ROOT,
        text=True,
        input=input_text,
        capture_output=True,
        check=False,
        env=merged_env,
        timeout=timeout,
    )
    if check and completed.returncode != 0:
        diagnostic = (completed.stderr or completed.stdout).strip().splitlines()
        detail = diagnostic[-1] if diagnostic else f"exit {completed.returncode}"
        raise AcceptanceError(f"command failed: {argv[0]}: {detail[:300]}")
    return completed


def manifest_assertions() -> dict[str, list[str]]:
    document = json.loads(ACCEPTANCE_MANIFEST.read_text(encoding="utf-8"))
    return {
        item["ac_id"]: list(item["required_assertions"])
        for item in document["criteria"]
    }


def detect_active_generation(runtime_root: Path = RUNTIME_ROOT) -> str:
    active: list[str] = []
    generations = runtime_root / "generations"
    if generations.is_dir():
        for manifest in generations.glob("g[0-9][0-9][0-9]/manifest.json"):
            try:
                payload = json.loads(manifest.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            generation = manifest.parent.name
            if payload.get("state") == "ACTIVE" and GENERATION_PATTERN.fullmatch(generation):
                active.append(generation)
    if len(active) > 1:
        raise AcceptanceError(f"multiple ACTIVE generation manifests: {sorted(active)}")
    return active[0] if active else "g001"


def consumer_container(generation: str) -> str:
    return (
        "cdc-lab-g001-consumer-1"
        if generation == "g001"
        else f"cdc-lab-{generation}-consumer"
    )


def consumer_volume(generation: str) -> str:
    return (
        "cdc-lab-g001_consumer-data"
        if generation == "g001"
        else f"cdc-lab-{generation}-consumer-data"
    )


def generation_value(generation: str, name: str) -> str:
    path = PROJECT_ROOT / "config" / "generations" / f"{generation}.env"
    matches = [
        line.split("=", 1)[1]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith(f"{name}=")
    ]
    if len(matches) != 1:
        raise AcceptanceError(f"generation metadata must define {name} exactly once")
    return matches[0]


def docker_state(name: str) -> str:
    completed = run(
        ["docker", "inspect", "--format", "{{.State.Status}}", name],
        check=False,
        timeout=20,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "missing"


def labeled_generation_consumers() -> dict[str, str]:
    completed = run(
        [
            "docker",
            "ps",
            "--all",
            "--filter",
            "label=io.kafka-debezium-cdc.lab=cdc-lab",
            "--filter",
            "label=io.kafka-debezium-cdc.role=generation-consumer",
            "--format",
            "{{.Names}}|{{.State}}",
        ],
        timeout=30,
    )
    consumers: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        name, separator, state = line.partition("|")
        if not separator or not name or not state:
            raise AcceptanceError("generation consumer inventory returned an invalid row")
        consumers[name] = state
    return consumers


def classify_runtime_state(generation: str) -> str:
    core = [docker_state(name) for name in (POSTGRES_CONTAINER, KAFKA_CONTAINER, CONNECT_CONTAINER)]
    active_container = consumer_container(generation)
    consumer = docker_state(active_container)
    consumers = labeled_generation_consumers()
    if generation != "g001":
        consumers["cdc-lab-g001-consumer-1"] = docker_state("cdc-lab-g001-consumer-1")
    inactive_running = sorted(
        name
        for name, state in consumers.items()
        if name != active_container and state == "running"
    )
    core_running = sum(state == "running" for state in core)
    if inactive_running:
        raise AcceptanceError(
            f"runtime has running inactive generation consumers: {inactive_running}"
        )
    if core_running == 0 and consumer != "running":
        return "stopped"
    if core_running == 3 and consumer == "running":
        return "running"
    raise AcceptanceError(
        f"runtime is partially started: core={core!r} generation={generation} consumer={consumer}"
    )


def wait_generation_status(generation: str, timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        status = run([str(LAB), "status", generation], check=False, timeout=120)
        if status.returncode == 0:
            return
        last = (status.stderr or status.stdout).strip().splitlines()[-1:][0] if (status.stderr or status.stdout).strip() else "unknown status failure"
        time.sleep(2)
    raise AcceptanceError(f"generation did not become ready: {last}")


def ensure_runtime(generation: str) -> None:
    run([str(LAB), "preflight"], timeout=120)
    run([str(LAB), "up"], timeout=240)
    if generation == "g001":
        status = run([str(LAB), "status", generation], check=False, timeout=120)
        if status.returncode != 0:
            run([str(LAB), "bootstrap", generation], timeout=240)
    else:
        run([str(LAB), "resume", generation], timeout=60)
    wait_generation_status(generation)


def postgres(sql: str) -> str:
    completed = run(
        [
            "docker",
            "exec",
            POSTGRES_CONTAINER,
            "psql",
            "--username",
            "cdc_admin",
            "--dbname",
            "cdc_lab",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--field-separator=|",
            "--command",
            sql,
        ],
        timeout=60,
    )
    return completed.stdout.strip()


def sink_json(generation: str, program: str) -> Any:
    container = consumer_container(generation)
    if docker_state(container) == "running":
        completed = run(
            ["docker", "exec", "--interactive", container, "python", "-"],
            input_text=program,
            timeout=60,
        )
    else:
        completed = run(
            [
                "docker",
                "run",
                "--rm",
                "--interactive",
                "--network",
                "none",
                "--read-only",
                "--volume",
                f"{consumer_volume(generation)}:/data:ro",
                "--env",
                f"CDC_GENERATION={generation}",
                "--env",
                f"SQLITE_SINK_PATH=/data/cdc-{generation}.sqlite3",
                CONSUMER_IMAGE,
                "python",
                "-",
            ],
            input_text=program,
            timeout=60,
        )
    return json.loads(completed.stdout)


def sink_json_writable_volume(generation: str, program: str) -> Any:
    completed = run(
        [
            "docker",
            "run",
            "--rm",
            "--interactive",
            "--network",
            "none",
            "--read-only",
            "--volume",
            f"{consumer_volume(generation)}:/data",
            "--env",
            f"CDC_GENERATION={generation}",
            "--env",
            f"SQLITE_SINK_PATH=/data/cdc-{generation}.sqlite3",
            CONSUMER_IMAGE,
            "python",
            "-",
        ],
        input_text=program,
        timeout=60,
    )
    return json.loads(completed.stdout)


def sink_summary(generation: str) -> dict[str, Any]:
    program = r'''
import json, os, sqlite3
c = sqlite3.connect(f"file:{os.environ['SQLITE_SINK_PATH']}?mode=ro", uri=True)
try:
    result = {
        "current": {
            "customers": c.execute("SELECT count(*) FROM customers_current").fetchone()[0],
            "orders": c.execute("SELECT count(*) FROM orders_current").fetchone()[0],
            "payments": c.execute("SELECT count(*) FROM payments_current").fetchone()[0],
        },
        "snapshot": dict(c.execute("SELECT source_table, count(*) FROM raw_events WHERE operation='r' GROUP BY source_table")),
        "raw_count": c.execute("SELECT count(*) FROM raw_events").fetchone()[0],
        "errors": c.execute("SELECT count(*) FROM ingestion_errors").fetchone()[0],
        "migrations": c.execute("SELECT version, name FROM schema_migrations ORDER BY version").fetchall(),
        "duplicates": c.execute("SELECT topic, partition, offset, delivery_count FROM raw_events WHERE delivery_count > 1 ORDER BY delivery_count DESC").fetchall(),
        "snapshot_total": c.execute("SELECT count(*) FROM raw_events WHERE operation='r'").fetchone()[0],
    }
    has_phone = c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0]
    result["phones"] = c.execute("SELECT id, phone FROM customers_current ORDER BY id").fetchall() if has_phone else []
finally:
    c.close()
print(json.dumps(result, sort_keys=True))
'''
    return sink_json(generation, program)


def source_counts() -> dict[str, int]:
    output = postgres(
        "SELECT (SELECT count(*) FROM customers),"
        "(SELECT count(*) FROM orders),(SELECT count(*) FROM payments);"
    )
    values = output.split("|")
    if len(values) != 3:
        raise AcceptanceError("source count query returned an unexpected shape")
    return dict(zip(("customers", "orders", "payments"), map(int, values), strict=True))


def source_has_phone() -> bool:
    return postgres(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='customers' AND column_name='phone';"
    ) == "1"


def source_projection_rows() -> dict[str, Any]:
    phone_expression = "phone" if source_has_phone() else "NULL::text"
    created_at_us = "(extract(epoch FROM created_at) * 1000000)::bigint::text"
    updated_at_us = "(extract(epoch FROM updated_at) * 1000000)::bigint::text"
    output = postgres(
        "SELECT jsonb_build_object("
        f"'customers',(SELECT coalesce(jsonb_agg(jsonb_build_array(id,email,full_name,status,{phone_expression},{created_at_us},{updated_at_us}) ORDER BY id),'[]'::jsonb) FROM customers),"
        f"'orders',(SELECT coalesce(jsonb_agg(jsonb_build_array(id,customer_id,status,total_amount::text,{created_at_us},{updated_at_us}) ORDER BY id),'[]'::jsonb) FROM orders),"
        f"'payments',(SELECT coalesce(jsonb_agg(jsonb_build_array(id,order_id,status,amount::text,provider_ref,{created_at_us},{updated_at_us}) ORDER BY id),'[]'::jsonb) FROM payments))::text;"
    )
    return json.loads(output)


def sink_projection_rows(generation: str) -> dict[str, Any]:
    program = r'''
import datetime as dt
import json, os, sqlite3

def timestamp_us(value):
    parsed=dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise SystemExit(f"projection timestamp is missing timezone: {value}")
    epoch=dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    delta=parsed.astimezone(dt.timezone.utc)-epoch
    return str(delta.days*86400000000+delta.seconds*1000000+delta.microseconds)

c=sqlite3.connect(f"file:{os.environ['SQLITE_SINK_PATH']}?mode=ro", uri=True)
try:
    has_phone=c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0] == 1
    customer_sql="SELECT id,email,full_name,status,phone,created_at,updated_at FROM customers_current ORDER BY id" if has_phone else "SELECT id,email,full_name,status,NULL,created_at,updated_at FROM customers_current ORDER BY id"
    rows={
      "customers":[list(row[:5])+[timestamp_us(row[5]),timestamp_us(row[6])] for row in c.execute(customer_sql)],
      "orders":[list(row[:4])+[timestamp_us(row[4]),timestamp_us(row[5])] for row in c.execute("SELECT id,customer_id,status,total_amount,created_at,updated_at FROM orders_current ORDER BY id")],
      "payments":[list(row[:5])+[timestamp_us(row[5]),timestamp_us(row[6])] for row in c.execute("SELECT id,order_id,status,amount,provider_ref,created_at,updated_at FROM payments_current ORDER BY id")],
    }
finally: c.close()
print(json.dumps(rows, sort_keys=True))
'''
    return sink_json(generation, program)


def sink_ingestion_error_count(generation: str) -> int:
    result = sink_json(
        generation,
        r'''
import json, os, sqlite3
c=sqlite3.connect(f"file:{os.environ['SQLITE_SINK_PATH']}?mode=ro", uri=True)
try: count=c.execute("SELECT count(*) FROM ingestion_errors").fetchone()[0]
finally: c.close()
print(json.dumps(count))
''',
    )
    if not isinstance(result, int) or isinstance(result, bool) or result < 0:
        raise AcceptanceError("ingestion_errors count has an unexpected shape")
    return result


def source_customer_row(customer_id: int) -> list[Any]:
    phone_expression = "phone" if source_has_phone() else "NULL::text"
    output = postgres(
        "SELECT jsonb_build_array(id,email,full_name,status,"
        f"{phone_expression},"
        "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),"
        "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'))::text "
        f"FROM customers WHERE id={customer_id};"
    )
    row = json.loads(output)
    if not isinstance(row, list) or len(row) != 7:
        raise AcceptanceError("source customer row returned an unexpected shape")
    return row


def sink_customer_row(
    generation: str, customer_id: int, *, writable_volume: bool = False
) -> list[Any]:
    program = f'''\nimport json, os, sqlite3\nc=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)\nhas_phone=c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0] == 1\nphone='phone' if has_phone else 'NULL'\nrow=c.execute(f"SELECT id,email,full_name,status,{{phone}},created_at,updated_at FROM customers_current WHERE id=?", ({customer_id},)).fetchone(); c.close()\nprint(json.dumps(row))\n'''
    result = (
        sink_json_writable_volume(generation, program)
        if writable_volume
        else sink_json(generation, program)
    )
    if not isinstance(result, list) or len(result) != 7:
        raise AcceptanceError("sink customer row returned an unexpected shape")
    return result


def source_entity_row(table: str, entity_id: int) -> list[Any]:
    if table == "customers":
        phone_expression = "phone" if source_has_phone() else "NULL::text"
        fields = (
            f"id,email,full_name,status,{phone_expression},"
            "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),"
            "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        )
        expected_length = 7
    elif table == "orders":
        fields = (
            "id,customer_id,status,total_amount::text,"
            "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),"
            "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        )
        expected_length = 6
    elif table == "payments":
        fields = (
            "id,order_id,status,amount::text,provider_ref,"
            "to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),"
            "to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        )
        expected_length = 7
    else:
        raise AcceptanceError(f"unsupported source entity table: {table}")
    output = postgres(
        f"SELECT jsonb_build_array({fields})::text FROM {table} WHERE id={entity_id};"
    )
    if not output:
        raise AcceptanceError(f"source entity is missing: {table}:{entity_id}")
    row = json.loads(output)
    if not isinstance(row, list) or len(row) != expected_length:
        raise AcceptanceError(f"source entity row has an unexpected shape: {table}:{entity_id}")
    return row


def sink_entity_row(generation: str, table: str, entity_id: int) -> list[Any]:
    if table not in {"customers", "orders", "payments"}:
        raise AcceptanceError(f"unsupported sink entity table: {table}")
    program = f'''
import json, os, sqlite3
c=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)
try:
    table={table!r}
    if table == "customers":
        has_phone=c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0] == 1
        phone="phone" if has_phone else "NULL"
        sql=f"SELECT id,email,full_name,status,{{phone}},created_at,updated_at FROM customers_current WHERE id=?"
    elif table == "orders":
        sql="SELECT id,customer_id,status,total_amount,created_at,updated_at FROM orders_current WHERE id=?"
    else:
        sql="SELECT id,order_id,status,amount,provider_ref,created_at,updated_at FROM payments_current WHERE id=?"
    row=c.execute(sql, ({entity_id},)).fetchone()
finally:
    c.close()
print(json.dumps(row))
'''
    row = sink_json(generation, program)
    expected_length = 6 if table == "orders" else 7
    if not isinstance(row, list) or len(row) != expected_length:
        raise AcceptanceError(f"sink entity is missing or malformed: {table}:{entity_id}")
    return row


def ac002_raw_records(generation: str) -> dict[str, list[list[Any]]]:
    program = f'''
import json, os, sqlite3
c=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)
try:
    result={{}}
    for table, entity_key, operation in {AC002_TARGETS!r}:
        label=f"{{table}}:{{entity_key}}:{{operation}}"
        result[label]=[list(row) for row in c.execute(
            "SELECT topic,partition,offset,processing_status,delivery_count FROM raw_events WHERE source_table=? AND entity_key=? AND operation=? ORDER BY topic,partition,offset",
            (table, entity_key, operation),
        )]
finally:
    c.close()
print(json.dumps(result, sort_keys=True))
'''
    result = sink_json(generation, program)
    if not isinstance(result, dict):
        raise AcceptanceError("AC-002 raw record inventory has an unexpected shape")
    return result


def validate_ac002_records(
    generation: str,
    before: Mapping[str, list[list[Any]]],
    after: Mapping[str, list[list[Any]]],
) -> dict[str, list[Any]]:
    expected_labels = {f"{table}:{entity_key}:{operation}" for table, entity_key, operation in AC002_TARGETS}
    if set(before) != expected_labels or set(after) != expected_labels:
        raise AcceptanceError("AC-002 raw inventory does not cover the exact create/update targets")
    topic_by_table = {
        "customers": generation_value(generation, "KAFKA_CUSTOMERS_TOPIC"),
        "orders": generation_value(generation, "KAFKA_ORDERS_TOPIC"),
        "payments": generation_value(generation, "KAFKA_PAYMENTS_TOPIC"),
    }
    observed: dict[str, list[Any]] = {}
    coordinates: set[tuple[str, int, int]] = set()
    for table, entity_key, operation in AC002_TARGETS:
        label = f"{table}:{entity_key}:{operation}"
        previous = {(str(row[0]), int(row[1]), int(row[2])) for row in before[label]}
        new_rows = [
            row
            for row in after[label]
            if (str(row[0]), int(row[1]), int(row[2])) not in previous
        ]
        if len(new_rows) != 1:
            raise AcceptanceError(f"expected one new unique raw record for {label}, got {new_rows!r}")
        row = new_rows[0]
        if (
            len(row) != 5
            or row[0] != topic_by_table[table]
            or not isinstance(row[1], int)
            or not 0 <= row[1] < 3
            or not isinstance(row[2], int)
            or row[2] < 0
            or row[3] != "applied"
            or not isinstance(row[4], int)
            or row[4] < 1
        ):
            raise AcceptanceError(f"invalid AC-002 raw record for {label}: {row!r}")
        coordinate = (row[0], row[1], row[2])
        if coordinate in coordinates:
            raise AcceptanceError(f"AC-002 reused a Kafka coordinate: {coordinate!r}")
        coordinates.add(coordinate)
        observed[label] = row
    return observed


def cleanup_fault_consumer(fault_container: str, official_container: str) -> None:
    if docker_state(fault_container) != "missing":
        removed = run(
            ["docker", "rm", "--force", fault_container],
            check=False,
            timeout=60,
        )
        if removed.returncode != 0:
            raise AcceptanceError("failed to remove the controlled-fault consumer")
    if docker_state(official_container) != "running":
        run(["docker", "start", official_container], timeout=60)


def slot_metric_present(output: str, generation: str) -> bool:
    pattern = re.compile(
        rf"^postgres_cdc_{generation}\|(reserved|extended|unreserved)\|[0-9]+\|[0-9]+\|(OK|WARNING)$",
        re.MULTILINE,
    )
    return pattern.search(output) is not None


def validate_wal_warning_boundary() -> dict[str, Any]:
    common = PROJECT_ROOT / "scripts" / "lib" / "common.sh"
    slot = "postgres_cdc_boundary_probe"
    cases = (
        (WAL_WARNING_BYTES - 1, "OK", True),
        (WAL_WARNING_BYTES, "WARNING", True),
        (WAL_WARNING_BYTES + 1, "WARNING", True),
        (WAL_WARNING_BYTES - 1, "WARNING", False),
        (WAL_WARNING_BYTES, "OK", False),
    )
    for retained_bytes, classification, should_pass in cases:
        row = f"{slot}|reserved|{retained_bytes}|1073741824|{classification}"
        completed = run(
            [
                "bash",
                "-c",
                'source "$1"; validate_replication_slot_metric "$2" "$3" >/dev/null',
                "bash",
                str(common),
                row,
                slot,
            ],
            check=False,
            timeout=20,
        )
        if (completed.returncode == 0) != should_pass:
            raise AcceptanceError(
                "WAL warning boundary classification mismatch: "
                f"retained={retained_bytes} classification={classification} exit={completed.returncode}"
            )
    return {
        "below": WAL_WARNING_BYTES - 1,
        "boundary": WAL_WARNING_BYTES,
        "above": WAL_WARNING_BYTES + 1,
    }


def parse_topic_offsets(
    output: str, topic: str, expected_partitions: int
) -> dict[int, int]:
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        fields = line.rsplit(":", 2)
        if len(fields) != 3 or fields[0] != topic:
            continue
        try:
            partition = int(fields[1])
            offset = int(fields[2])
        except ValueError as error:
            raise AcceptanceError(f"invalid topic offset row: {line!r}") from error
        if partition in offsets or partition < 0 or offset < 0:
            raise AcceptanceError(f"invalid duplicate/negative topic offset row: {line!r}")
        offsets[partition] = offset
    if set(offsets) != set(range(expected_partitions)):
        raise AcceptanceError(
            f"expected {expected_partitions} partitions for {topic}, got {offsets!r}"
        )
    return offsets


def kafka_topic_offsets(
    topic: str, *, time_selector: int, expected_partitions: int
) -> dict[int, int]:
    output = run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--topic",
            topic,
            "--time",
            str(time_selector),
        ],
        timeout=60,
    ).stdout
    return parse_topic_offsets(output, topic, expected_partitions)


def table_topic_offset_ranges(generation: str) -> dict[str, dict[str, dict[int, int]]]:
    result: dict[str, dict[str, dict[int, int]]] = {}
    for variable in (
        "KAFKA_CUSTOMERS_TOPIC",
        "KAFKA_ORDERS_TOPIC",
        "KAFKA_PAYMENTS_TOPIC",
    ):
        topic = generation_value(generation, variable)
        earliest = kafka_topic_offsets(topic, time_selector=-2, expected_partitions=3)
        latest = kafka_topic_offsets(topic, time_selector=-1, expected_partitions=3)
        if any(earliest[partition] > latest[partition] for partition in earliest):
            raise AcceptanceError(f"topic offset range is inverted for {topic}")
        result[topic] = {"earliest": earliest, "latest": latest}
    return result


def validate_retention_offset_movement(
    *, before_start: int, before_end: int, after_start: int, after_end: int
) -> dict[str, int]:
    if before_start != 0:
        raise AcceptanceError(
            f"retention probe must start from a complete log, got start={before_start}"
        )
    if before_end != RETENTION_PROBE_RECORD_COUNT:
        raise AcceptanceError(
            f"retention probe expected end={RETENTION_PROBE_RECORD_COUNT}, got {before_end}"
        )
    if not (before_start < after_start < after_end):
        raise AcceptanceError(
            "retention did not delete only an old closed-segment prefix: "
            f"before={before_start}:{before_end} after={after_start}:{after_end}"
        )
    if after_end != before_end:
        raise AcceptanceError("retention probe changed the log end offset")
    return {
        "before_start": before_start,
        "before_end": before_end,
        "after_start": after_start,
        "after_end": after_end,
    }


def retention_probe_topic_selector(topic: str) -> str:
    if re.fullmatch(
        r"cdc\.g\d{3}\.acceptance\.retention-probe-\d+-\d{9}", topic
    ) is None:
        raise AcceptanceError(f"invalid disposable retention probe topic name: {topic}")
    return f"^{re.escape(topic)}$"


def retention_probe_topic_exists(topic: str) -> bool:
    selector = retention_probe_topic_selector(topic)
    listed = run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--list",
            "--topic",
            selector,
        ],
        check=False,
        timeout=30,
    )
    if listed.returncode != 0:
        raise AcceptanceError(f"failed to inspect disposable retention probe topic: {topic}")
    topics = [line.strip() for line in listed.stdout.splitlines() if line.strip()]
    if topics not in ([], [topic]):
        raise AcceptanceError(
            f"disposable retention probe lookup returned unexpected topics: {topics!r}"
        )
    return topics == [topic]


def delete_retention_probe_topic(topic: str) -> None:
    selector = retention_probe_topic_selector(topic)
    if not retention_probe_topic_exists(topic):
        return
    deleted = run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--delete",
            "--topic",
            selector,
        ],
        check=False,
        timeout=60,
    )
    if deleted.returncode != 0:
        raise AcceptanceError(f"failed to delete disposable retention probe topic: {topic}")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if not retention_probe_topic_exists(topic):
            return
        time.sleep(1)
    raise AcceptanceError(f"disposable retention probe topic was not deleted: {topic}")


def run_kafka_retention_probe(generation: str) -> dict[str, Any]:
    run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "grep",
            "-Fx",
            "log.retention.check.interval.ms=1000",
            "/kafka/config/server.properties",
        ],
        timeout=30,
    )
    suffix = f"{os.getpid()}-{time.time_ns() % 1_000_000_000:09d}"
    topic = f"cdc.{generation}.acceptance.retention-probe-{suffix}"
    create_attempted = False
    try:
        create_attempted = True
        run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "127.0.0.1:9092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--config",
                "cleanup.policy=delete",
                "--config",
                "retention.ms=86400000",
                "--config",
                f"retention.bytes={RETENTION_PROBE_INITIAL_BYTES}",
                "--config",
                f"segment.bytes={RETENTION_PROBE_SEGMENT_BYTES}",
                "--config",
                "segment.ms=600000",
                "--config",
                "file.delete.delay.ms=1000",
            ],
            timeout=60,
        )
        value = "x" * (RETENTION_PROBE_VALUE_BYTES - 12)
        records = "".join(
            f"{index:06d}-{value}\n" for index in range(RETENTION_PROBE_RECORD_COUNT)
        )
        run(
            [
                "docker",
                "exec",
                "--interactive",
                KAFKA_CONTAINER,
                "/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server",
                "127.0.0.1:9092",
                "--topic",
                topic,
                "--producer-property",
                "acks=all",
            ],
            input_text=records,
            timeout=120,
        )
        before_start = kafka_topic_offsets(
            topic, time_selector=-2, expected_partitions=1
        )[0]
        before_end = kafka_topic_offsets(
            topic, time_selector=-1, expected_partitions=1
        )[0]
        if before_start != 0 or before_end != RETENTION_PROBE_RECORD_COUNT:
            raise AcceptanceError(
                f"retention probe baseline offsets mismatch: {before_start}:{before_end}"
            )
        run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                "127.0.0.1:9092",
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--alter",
                "--add-config",
                f"retention.bytes={RETENTION_PROBE_TARGET_BYTES}",
            ],
            timeout=60,
        )
        deadline = time.monotonic() + 90
        after_start = before_start
        after_end = before_end
        while time.monotonic() < deadline:
            after_start = kafka_topic_offsets(
                topic, time_selector=-2, expected_partitions=1
            )[0]
            after_end = kafka_topic_offsets(
                topic, time_selector=-1, expected_partitions=1
            )[0]
            if after_start > before_start:
                break
            time.sleep(1)
        movement = validate_retention_offset_movement(
            before_start=before_start,
            before_end=before_end,
            after_start=after_start,
            after_end=after_end,
        )
        return {
            "topic": topic,
            "initial_retention_bytes": RETENTION_PROBE_INITIAL_BYTES,
            "target_retention_bytes": RETENTION_PROBE_TARGET_BYTES,
            "segment_bytes": RETENTION_PROBE_SEGMENT_BYTES,
            **movement,
        }
    finally:
        if create_attempted:
            delete_retention_probe_topic(topic)


def topic_end_offsets(topic: str) -> dict[int, int]:
    output = run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--topic",
            topic,
        ],
        timeout=60,
    ).stdout
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        fields = line.rsplit(":", 2)
        if len(fields) == 3 and fields[0] == topic:
            offsets[int(fields[1])] = int(fields[2])
    if len(offsets) != 3:
        raise AcceptanceError(f"expected three end offsets for {topic}, got {offsets!r}")
    return offsets


def group_output(generation: str) -> str:
    group = generation_value(generation, "KAFKA_CONSUMER_GROUP")
    return run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--describe",
            "--group",
            group,
        ],
        timeout=60,
    ).stdout


def lag_rows(generation: str) -> list[dict[str, Any]]:
    expected_group = generation_value(generation, "KAFKA_CONSUMER_GROUP")
    expected_topics = {
        generation_value(generation, variable)
        for variable in (
            "KAFKA_CUSTOMERS_TOPIC",
            "KAFKA_ORDERS_TOPIC",
            "KAFKA_PAYMENTS_TOPIC",
        )
    }
    expected_coordinates = {
        (topic, partition)
        for topic in expected_topics
        for partition in range(3)
    }
    rows: list[dict[str, Any]] = []
    for line in group_output(generation).splitlines():
        columns = line.split()
        if len(columns) < 6 or columns[0] != expected_group:
            continue
        try:
            partition = int(columns[2])
            lag = None if columns[5] == "-" else int(columns[5])
        except ValueError as error:
            raise AcceptanceError(f"invalid consumer-group row: {line!r}") from error
        rows.append(
            {
                "topic": columns[1],
                "partition": partition,
                "current": columns[3],
                "end": columns[4],
                "lag": lag,
            }
        )
    observed_coordinates = [
        (str(row["topic"]), int(row["partition"])) for row in rows
    ]
    if (
        len(rows) != len(expected_coordinates)
        or len(set(observed_coordinates)) != len(observed_coordinates)
        or set(observed_coordinates) != expected_coordinates
    ):
        raise AcceptanceError(
            "consumer-group rows do not match the exact table-topic 3x3 coordinates: "
            f"expected={sorted(expected_coordinates)!r} observed={sorted(observed_coordinates)!r}"
        )
    return rows


def lag_row_is_zero(row: Mapping[str, Any]) -> bool:
    if row["current"] == "-" and row["end"] == "0" and row["lag"] is None:
        return True
    return (
        row["lag"] == 0
        and str(row["current"]).isdigit()
        and str(row["end"]).isdigit()
        and row["current"] == row["end"]
    )


def wait_complete_lag_inventory(
    generation: str, timeout: int = 60
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last_error = "consumer-group rows unavailable"
    while time.monotonic() < deadline:
        try:
            return lag_rows(generation)
        except AcceptanceError as error:
            last_error = str(error)
        state = docker_state(consumer_container(generation))
        if state in {"exited", "dead"}:
            raise AcceptanceError(
                f"consumer stopped while waiting for complete lag inventory: {state}"
            )
        time.sleep(1)
    raise AcceptanceError(f"consumer-group lag inventory did not become complete: {last_error}")


def wait_lag_zero(generation: str, timeout: int = 120) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last = lag_rows(generation)
        if all(lag_row_is_zero(row) for row in last):
            return last
        state = docker_state(consumer_container(generation))
        if state in {"exited", "dead"}:
            raise AcceptanceError(f"consumer stopped while waiting for lag zero: {state}")
        time.sleep(1)
    raise AcceptanceError(f"consumer lag did not reach zero: {last!r}")


def wait_projection_parity(generation: str, timeout: int = 120) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_source: dict[str, Any] = {}
    last_sink: dict[str, Any] = {}
    while time.monotonic() < deadline:
        remaining = max(1, int(deadline - time.monotonic()))
        wait_lag_zero(generation, timeout=remaining)
        last_source = source_projection_rows()
        last_sink = sink_projection_rows(generation)
        if last_source == last_sink:
            return last_source
        time.sleep(1)
    raise AcceptanceError(
        "source/sink projection parity timeout: "
        + json.dumps({"source": last_source, "sink": last_sink}, sort_keys=True)
    )


def run_unittest(test_name: str) -> None:
    run(
        [sys.executable, "-m", "unittest", test_name],
        env={"PYTHONPATH": str(PROJECT_ROOT / "consumer" / "src")},
        timeout=120,
    )


def run_fixture(name: str) -> dict[str, Any]:
    completed = run(
        [sys.executable, "tests/run_event_fixture.py", f"fixtures/events/{name}"],
        timeout=120,
    )
    return json.loads(completed.stdout)


def probe_ac001(generation: str) -> dict[str, Any]:
    run([str(LAB), "status", generation], timeout=120)
    source = source_counts()
    sink = sink_summary(generation)
    parity = wait_projection_parity(generation)
    if not all(int(sink["snapshot"].get(table, 0)) > 0 for table in source):
        raise AcceptanceError("snapshot events are missing for one or more source tables")
    return {
        "source_rows": f"source rows verified counts={source}",
        "raw_snapshot_events": f"snapshot event counts={sink['snapshot']}",
        "projection_rows": f"projection matches source tables={sorted(parity)}",
        "connector_streaming": f"connector and task RUNNING generation={generation}",
    }


def probe_ac002(generation: str) -> dict[str, Any]:
    postgres(
        "BEGIN;"
        "DELETE FROM payments WHERE id=1002;"
        "DELETE FROM orders WHERE id=103;"
        "DELETE FROM customers WHERE id=3;"
        "UPDATE customers SET full_name='Alice Adams',status='active' WHERE id=1;"
        "COMMIT;"
    )
    wait_projection_parity(generation)
    customer_before_update = source_entity_row("customers", 1)
    if customer_before_update[:4] != [
        1,
        "alice@example.test",
        "Alice Adams",
        "active",
    ]:
        raise AcceptanceError(
            f"AC-002 customer reset state mismatch: {customer_before_update!r}"
        )
    before = ac002_raw_records(generation)
    customer_insert = (
        "INSERT INTO customers(id,email,full_name,status,phone,created_at,updated_at) OVERRIDING SYSTEM VALUE "
        "VALUES (3,'carol@example.test','Carol Clark','active',NULL,'2026-01-02 00:00:00+00','2026-01-02 00:00:00+00');"
        if source_has_phone()
        else "INSERT INTO customers(id,email,full_name,status,created_at,updated_at) OVERRIDING SYSTEM VALUE "
        "VALUES (3,'carol@example.test','Carol Clark','active','2026-01-02 00:00:00+00','2026-01-02 00:00:00+00');"
    )
    postgres(
        "BEGIN;"
        + customer_insert
        + "INSERT INTO orders(id,customer_id,status,total_amount,created_at,updated_at) OVERRIDING SYSTEM VALUE "
        "VALUES (103,3,'pending',15.00,'2026-01-02 00:01:00+00','2026-01-02 00:01:00+00');"
        "INSERT INTO payments(id,order_id,status,amount,provider_ref,created_at,updated_at) OVERRIDING SYSTEM VALUE "
        "VALUES (1002,103,'authorized',15.00,'pay-1002','2026-01-02 00:02:00+00','2026-01-02 00:02:00+00');"
        "COMMIT;"
    )
    wait_projection_parity(generation)
    create_rows: dict[str, list[Any]] = {}
    for table, entity_id in (("customers", 3), ("orders", 103), ("payments", 1002)):
        source_row = source_entity_row(table, entity_id)
        sink_row = sink_entity_row(generation, table, entity_id)
        if source_row != sink_row:
            raise AcceptanceError(
                f"AC-002 complete create row mismatch for {table}:{entity_id}: source={source_row!r} sink={sink_row!r}"
            )
        create_rows[table] = source_row
    expected_create_rows = {
        "customers": [
            3,
            "carol@example.test",
            "Carol Clark",
            "active",
            None,
            "2026-01-02T00:00:00.000000Z",
            "2026-01-02T00:00:00.000000Z",
        ],
        "orders": [
            103,
            3,
            "pending",
            "15.00",
            "2026-01-02T00:01:00.000000Z",
            "2026-01-02T00:01:00.000000Z",
        ],
        "payments": [
            1002,
            103,
            "authorized",
            "15.00",
            "pay-1002",
            "2026-01-02T00:02:00.000000Z",
            "2026-01-02T00:02:00.000000Z",
        ],
    }
    if create_rows != expected_create_rows:
        raise AcceptanceError(
            f"AC-002 deterministic create values mismatch: {create_rows!r}"
        )
    postgres(
        "DO $$ DECLARE affected BIGINT; BEGIN "
        "UPDATE customers SET full_name='Alice Cooper',status='inactive' WHERE id=1; "
        "GET DIAGNOSTICS affected = ROW_COUNT; IF affected <> 1 THEN RAISE EXCEPTION 'expected one customer update'; END IF; "
        "UPDATE orders SET status='paid' WHERE id=103; "
        "GET DIAGNOSTICS affected = ROW_COUNT; IF affected <> 1 THEN RAISE EXCEPTION 'expected one order update'; END IF; "
        "UPDATE payments SET status='captured' WHERE id=1002; "
        "GET DIAGNOSTICS affected = ROW_COUNT; IF affected <> 1 THEN RAISE EXCEPTION 'expected one payment update'; END IF; "
        "END $$;"
    )
    parity = wait_projection_parity(generation)
    update_rows: dict[str, list[Any]] = {}
    for table, entity_id in (("customers", 1), ("orders", 103), ("payments", 1002)):
        source_row = source_entity_row(table, entity_id)
        sink_row = sink_entity_row(generation, table, entity_id)
        if source_row != sink_row:
            raise AcceptanceError(
                f"AC-002 complete update row mismatch for {table}:{entity_id}: source={source_row!r} sink={sink_row!r}"
            )
        update_rows[table] = source_row
    customer_update = update_rows["customers"]
    order_update = update_rows["orders"]
    payment_update = update_rows["payments"]
    if (
        customer_update[:6]
        != [
            1,
            "alice@example.test",
            "Alice Cooper",
            "inactive",
            customer_before_update[4],
            "2026-01-01T00:00:00.000000Z",
        ]
        or customer_update[6] == customer_before_update[6]
        or order_update[:5]
        != [
            103,
            3,
            "paid",
            "15.00",
            "2026-01-02T00:01:00.000000Z",
        ]
        or order_update[5] == create_rows["orders"][5]
        or payment_update[:6]
        != [
            1002,
            103,
            "captured",
            "15.00",
            "pay-1002",
            "2026-01-02T00:02:00.000000Z",
        ]
        or payment_update[6] == create_rows["payments"][6]
    ):
        raise AcceptanceError(
            f"AC-002 deterministic update values mismatch: {update_rows!r}"
        )
    records = validate_ac002_records(generation, before, ac002_raw_records(generation))
    coordinate_summary = ",".join(
        f"{label}@{row[1]}:{row[2]}" for label, row in records.items()
    )
    return {
        "source_sink_parity": (
            "complete source/projection rows matched for create and update phases; "
            f"sha256(create)={json_sha256(create_rows)} sha256(update)={json_sha256(update_rows)}"
        ),
        "raw_streaming_events": "exactly one new unique applied raw record for each of 3 creates and 3 updates",
        "kafka_coordinates": (
            f"generation={generation}; exact topic validation passed; coordinates={coordinate_summary}"
        ),
    }


def ensure_delete_fixture() -> None:
    state = postgres(
        "SELECT (SELECT count(*) FROM customers WHERE id=3),"
        "(SELECT count(*) FROM orders WHERE id=103),"
        "(SELECT count(*) FROM payments WHERE id=1002);"
    )
    if state == "1|1|1":
        return
    if state != "0|0|0":
        raise AcceptanceError(f"delete fixture is partially present: {state}")
    customer_insert = (
        "INSERT INTO customers(id,email,full_name,status,phone) OVERRIDING SYSTEM VALUE VALUES (3,'carol@example.test','Carol Clark','active',NULL);"
        if source_has_phone()
        else "INSERT INTO customers(id,email,full_name,status) OVERRIDING SYSTEM VALUE VALUES (3,'carol@example.test','Carol Clark','active');"
    )
    postgres(
        "BEGIN;"
        + customer_insert
        + "INSERT INTO orders(id,customer_id,status,total_amount) OVERRIDING SYSTEM VALUE VALUES (103,3,'paid',15.00);"
        "INSERT INTO payments(id,order_id,status,amount,provider_ref) OVERRIDING SYSTEM VALUE VALUES (1002,103,'captured',15.00,'pay-1002');"
        "COMMIT;"
    )


def probe_ac003(generation: str) -> dict[str, Any]:
    ensure_delete_fixture()
    wait_projection_parity(generation)
    run(
        [
            "docker",
            "exec",
            POSTGRES_CONTAINER,
            "psql",
            "--username",
            "cdc_admin",
            "--dbname",
            "cdc_lab",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--file",
            "/opt/cdc/postgres/exercises/02-delete.sql",
        ],
        timeout=60,
    )
    wait_projection_parity(generation)
    observed = sink_json(
        generation,
        r'''
import json, os, sqlite3
c=sqlite3.connect(f"file:{os.environ['SQLITE_SINK_PATH']}?mode=ro", uri=True)
try:
  delete_count=c.execute("SELECT count(*) FROM raw_events WHERE source_table='customers' AND entity_key='3' AND operation='d'").fetchone()[0]
  tombstone_count=c.execute("SELECT count(*) FROM raw_events WHERE source_table='customers' AND entity_key='3' AND processing_status='tombstone'").fetchone()[0]
  projection=c.execute("SELECT count(*) FROM customers_current WHERE id=3").fetchone()[0]
  watermark=c.execute("SELECT last_was_delete FROM entity_watermarks WHERE source_table='customers' AND entity_key='3'").fetchone()
finally: c.close()
print(json.dumps({'delete_count':delete_count,'tombstone_count':tombstone_count,'projection':projection,'watermark':watermark}))
''',
    )
    if not (
        observed["delete_count"] >= 1
        and observed["tombstone_count"] >= 1
        and observed["projection"] == 0
        and observed["watermark"] == [1]
    ):
        raise AcceptanceError(f"delete/tombstone postconditions mismatch: {observed!r}")
    return {
        "delete_event": f"customer 3 delete events={observed['delete_count']}",
        "tombstone_event": f"customer 3 tombstones={observed['tombstone_count']}",
        "projection_absent": "customer 3 absent after aggregate delete",
        "delete_watermark": "customer 3 watermark retains last_was_delete=1",
    }


def probe_ac004(generation: str) -> dict[str, Any]:
    run_unittest(
        "consumer.tests.test_runner.RunnerTests.test_fault_window_leaves_durable_state_without_offset_commit"
    )
    wait_projection_parity(generation)
    container = consumer_container(generation)
    fault_container = "cdc-lab-r09-ac004-fault"
    if docker_state(fault_container) != "missing":
        raise AcceptanceError(f"controlled-fault container already exists: {fault_container}")
    projection_before = sink_customer_row(generation, 2)
    offsets_before = committed_offsets(generation)
    topic = generation_value(generation, "KAFKA_CUSTOMERS_TOPIC")
    ends_before = topic_end_offsets(topic)
    run(["docker", "stop", container], timeout=60)
    try:
        postgres("UPDATE customers SET updated_at=clock_timestamp() WHERE id=2 RETURNING id;")
        source_after_update = source_customer_row(2)
        if source_after_update == projection_before:
            raise AcceptanceError("controlled source update did not change the complete customer row")
        deadline = time.monotonic() + 60
        coordinate: tuple[str, int, int] | None = None
        while time.monotonic() < deadline:
            ends_after = topic_end_offsets(topic)
            changed = [
                (partition, ends_before[partition], end)
                for partition, end in ends_after.items()
                if end != ends_before[partition]
            ]
            if len(changed) == 1 and changed[0][2] == changed[0][1] + 1:
                partition, offset, _ = changed[0]
                coordinate = (topic, partition, offset)
                break
            if changed:
                raise AcceptanceError(f"controlled fault produced unexpected topic offsets: {changed!r}")
            time.sleep(1)
        if coordinate is None:
            raise AcceptanceError("connector did not publish the controlled-fault source update")
        fault_at = f"{coordinate[0]}:{coordinate[1]}:{coordinate[2]}"
        fault = run(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                fault_container,
                "--network",
                "cdc-lab-g001_cdc-lab",
                "--read-only",
                "--tmpfs",
                "/tmp:size=16m",
                "--volume",
                f"{consumer_volume(generation)}:/data",
                "--cpus",
                "0.5",
                "--memory",
                "512m",
                "--pids-limit",
                "128",
                "--env",
                f"CDC_GENERATION={generation}",
                "--env",
                "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
                "--env",
                f"KAFKA_CONSUMER_GROUP={generation_value(generation, 'KAFKA_CONSUMER_GROUP')}",
                "--env",
                f"KAFKA_CUSTOMERS_TOPIC={generation_value(generation, 'KAFKA_CUSTOMERS_TOPIC')}",
                "--env",
                f"KAFKA_ORDERS_TOPIC={generation_value(generation, 'KAFKA_ORDERS_TOPIC')}",
                "--env",
                f"KAFKA_PAYMENTS_TOPIC={generation_value(generation, 'KAFKA_PAYMENTS_TOPIC')}",
                "--env",
                f"SQLITE_SINK_PATH={generation_value(generation, 'SQLITE_SINK_PATH')}",
                "--env",
                "CDC_POLL_TIMEOUT_SECONDS=1.0",
                "--env",
                f"CDC_FAULT_AFTER_SQLITE_COMMIT_AT={fault_at}",
                CONSUMER_IMAGE,
            ],
            check=False,
            timeout=180,
        )
        if fault.returncode == 0 or "controlled fault after durable SQLite commit" not in (fault.stdout + fault.stderr):
            raise AcceptanceError("controlled consumer did not stop in the post-SQLite/pre-offset window")
        raw_after_fault = sink_json_writable_volume(
            generation,
            f'''\nimport json, os, sqlite3\nc=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)\nraw=c.execute("SELECT delivery_count FROM raw_events WHERE topic=? AND partition=? AND offset=?", {coordinate!r}).fetchone()\nhas_phone=c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0] == 1\nphone='phone' if has_phone else 'NULL'\nrow=c.execute(f"SELECT id,email,full_name,status,{{phone}},created_at,updated_at FROM customers_current WHERE id=2").fetchone(); c.close()\nprint(json.dumps({{'raw':raw,'row':row}}, sort_keys=True))\n''',
        )
        if raw_after_fault.get("raw") != [1] or raw_after_fault.get("row") != source_after_update:
            raise AcceptanceError(f"durable pre-commit row/raw state mismatch: {raw_after_fault!r}")
        projection_after_fault = raw_after_fault["row"]
        offset_after_fault = committed_offset(
            generation, coordinate[0], coordinate[1]
        )
        key = (coordinate[0], coordinate[1])
        if offset_after_fault != offsets_before[key]:
            raise AcceptanceError("consumer-group offset advanced across the controlled fault")
    finally:
        cleanup_fault_consumer(fault_container, container)
    wait_projection_parity(generation)
    observed = sink_json(
        generation,
        f'''\nimport json, os, sqlite3\nc=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)\nraw=c.execute("SELECT delivery_count, processing_status FROM raw_events WHERE topic=? AND partition=? AND offset=?", {coordinate!r}).fetchone()\nhas_phone=c.execute("SELECT count(*) FROM pragma_table_info('customers_current') WHERE name='phone'").fetchone()[0] == 1\nphone='phone' if has_phone else 'NULL'\nrow=c.execute(f"SELECT id,email,full_name,status,{{phone}},created_at,updated_at FROM customers_current WHERE id=2").fetchone(); c.close()\nprint(json.dumps({{'raw':raw,'row':row}}, sort_keys=True))\n''',
    )
    if observed.get("raw") != [2, "applied"] or observed.get("row") != projection_after_fault:
        raise AcceptanceError("duplicate retry changed the complete projection or delivery metadata")
    offsets_after_retry = committed_offsets(generation)
    if offsets_after_retry[(coordinate[0], coordinate[1])] != str(coordinate[2] + 1):
        raise AcceptanceError("consumer-group offset did not advance exactly once after retry")
    return {
        "durable_precommit_state": f"generation={generation} coordinate={coordinate} durable before injected fault",
        "duplicate_coordinate": f"generation-scoped coordinate retried={coordinate}",
        "delivery_count": "delivery_count=2 with one raw row",
        "projection_unchanged_after_duplicate": "complete projection including timestamps identical after durable fault and duplicate retry",
        "offset_after_restart": f"committed offset advanced to {coordinate[2] + 1} only after restart",
    }


def probe_ac005(_: str) -> dict[str, Any]:
    observed = run_fixture("ac-005-out-of-order.json")
    return {
        "unique_raw_records": f"raw records={len(observed['raw'])}",
        "stale_status": "older coordinate archived as stale",
        "latest_projection": f"projection name={observed['projection']['row']['full_name']}",
        "watermark": f"watermark source_lsn={observed['watermark']['source_lsn']}",
    }


def probe_ac006(_: str) -> dict[str, Any]:
    observed = run_fixture("ac-006-old-after-delete.json")
    return {
        "delete_watermark": f"delete watermark source_lsn={observed['watermark']['source_lsn']}",
        "stale_old_event": "old update archived with stale status",
        "projection_absent": "deleted fixture row was not resurrected",
    }


def prior_live_transition_evidence(
    generation: str,
) -> tuple[Path, tuple[str, int, int], str, str] | None:
    evidence_root = RUNTIME_ROOT / "evidence" / generation / "AC-007"
    if not evidence_root.is_dir():
        return None
    for path in sorted(evidence_root.glob("*/index.json"), reverse=True):
        try:
            payload = load_evidence(path)
            if payload["ac_id"] != "AC-007" or payload["generation"] != generation or payload["result"] != "PASS":
                continue
            if len(payload["artifacts"]) != 1 or payload["artifacts"][0]["path"] != "observations.json":
                continue
            artifact = path.parent / "observations.json"
            if not artifact.is_file() or artifact.is_symlink():
                continue
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != payload["artifacts"][0]["sha256"]:
                continue
            observation = json.loads(artifact.read_text(encoding="utf-8"))
            if observation.get("ac_id") != "AC-007" or observation.get("generation") != generation or observation.get("result") != "PASS":
                continue
            assertions = {item["name"]: item["summary"] for item in payload["assertions"]}
            if set(assertions) != set(manifest_assertions()["AC-007"]):
                continue
            if (
                set(observation) != {"ac_id", "generation", "result", "assertion_summaries"}
                or observation.get("assertion_summaries") != assertions
            ):
                continue
            provenance = re.fullmatch(
                r"mode=(live_transition|historical_live_transition) coordinate=([^: ]+):(\d+):(\d+); .+",
                assertions["pre_migration_fail_stop"],
            )
            unchanged = re.fullmatch(
                r"coordinate=([^: ]+):(\d+):(\d+) offset_before=(\d+) offset_after_failure=(\d+)",
                assertions["offset_unchanged"],
            )
            replay = re.fullmatch(
                r"coordinate=([^: ]+):(\d+):(\d+) replayed after migration versions=1,2",
                assertions["same_coordinate_replay"],
            )
            committed = re.fullmatch(
                r"coordinate=([^: ]+):(\d+):(\d+) offset_after_replay=(\d+); parity and zero lag passed",
                assertions["offset_after_replay"],
            )
            if (
                provenance is None
                or unchanged is None
                or replay is None
                or committed is None
            ):
                continue
            coordinates = {
                (provenance.group(2), int(provenance.group(3)), int(provenance.group(4))),
                (unchanged.group(1), int(unchanged.group(2)), int(unchanged.group(3))),
                (replay.group(1), int(replay.group(2)), int(replay.group(3))),
                (committed.group(1), int(committed.group(2)), int(committed.group(3))),
            }
            if len(coordinates) != 1:
                continue
            coordinate = coordinates.pop()
            offset_before = int(unchanged.group(4))
            offset_after_failure = int(unchanged.group(5))
            offset_after_replay = int(committed.group(4))
            if not (
                offset_before == offset_after_failure == coordinate[2]
                and offset_after_replay == coordinate[2] + 1
            ):
                continue
            return path, coordinate, str(offset_before), str(offset_after_replay)
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            json.JSONDecodeError,
        ):
            continue
    return None


def historical_ac007_provenance() -> tuple[str, int, int] | None:
    status = (PROJECT_ROOT / "docs" / "status.md").read_text(encoding="utf-8")
    required = (
        "source ALTER+UPDATE создал `cdc.g001.public.customers:2:4`",
        "committed offset остался 4 при log-end 5",
        "committed offset стал 5/lag 0",
    )
    if not all(item in status for item in required):
        return None
    observed = sink_json(
        "g001",
        r'''
import json, sqlite3
c=sqlite3.connect("file:/data/cdc-g001.sqlite3?mode=ro", uri=True)
try:
    result={
      "coordinate": c.execute("SELECT topic,partition,offset,operation FROM raw_events WHERE topic='cdc.g001.public.customers' AND partition=2 AND offset=4").fetchone(),
      "migrations": c.execute("SELECT version,name FROM schema_migrations ORDER BY version").fetchall(),
      "phone": c.execute("SELECT phone FROM customers_current WHERE id=1").fetchone(),
    }
finally: c.close()
print(json.dumps(result, sort_keys=True))
''',
    )
    if observed != {
        "coordinate": ["cdc.g001.public.customers", 2, 4, "u"],
        "migrations": [[1, "initial"], [2, "customers_phone"]],
        "phone": ["+15550000001"],
    }:
        return None
    return ("cdc.g001.public.customers", 2, 4)


def committed_offsets(generation: str) -> dict[tuple[str, int], str]:
    return {
        (str(row["topic"]), int(row["partition"])): str(row["current"])
        for row in lag_rows(generation)
    }


def committed_offset(generation: str, topic: str, partition: int) -> str:
    for line in group_output(generation).splitlines():
        columns = line.split()
        if (
            len(columns) >= 6
            and columns[1] == topic
            and columns[2].isdigit()
            and int(columns[2]) == partition
        ):
            return columns[3]
    raise AcceptanceError(
        f"consumer group has no committed offset row for {topic}:{partition}"
    )


def wait_consumer_exit(generation: str, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    container = consumer_container(generation)
    while time.monotonic() < deadline:
        state = docker_state(container)
        if state in {"exited", "dead"}:
            details = run(
                ["docker", "inspect", "--format", "{{.State.ExitCode}}|{{.State.OOMKilled}}", container],
                timeout=30,
            ).stdout.strip()
            if details == "0|false":
                raise AcceptanceError("consumer stopped successfully instead of fail-stopping")
            return
        time.sleep(1)
    raise AcceptanceError("consumer did not fail-stop on the unmigrated phone record")


def restore_ac007_runtime(
    generation: str, transition_error: BaseException
) -> None:
    container = consumer_container(generation)
    try:
        if docker_state(container) == "running":
            run(["docker", "stop", container], timeout=60)
        if source_has_phone():
            run([str(LAB), "migrate", generation], timeout=120)
        if docker_state(container) != "running":
            run(["docker", "start", container], timeout=60)
        wait_generation_status(generation)
        wait_complete_lag_inventory(generation)
        wait_projection_parity(generation)
    except Exception as recovery_error:
        stopped_exit: int | None = None
        stopped_state = "unverified"
        fallback_error = "none"
        try:
            stopped = run([str(LAB), "stop"], check=False, timeout=120)
            stopped_exit = stopped.returncode
            if stopped_exit == 0:
                stopped_state = classify_runtime_state(generation)
        except Exception as stop_error:  # noqa: BLE001 - fail closed on fallback failure
            fallback_error = f"{type(stop_error).__name__}: {stop_error}"
        original = f"{type(transition_error).__name__}: {transition_error}"
        recovery = f"{type(recovery_error).__name__}: {recovery_error}"
        if stopped_exit == 0 and stopped_state == "stopped":
            raise RuntimeRestorationError(
                "AC-007 transition failed and running-state restoration failed; "
                f"lab was forced fully stopped; original={original}; recovery={recovery}"
            ) from recovery_error
        raise RuntimeRestorationError(
            "AC-007 transition failed and runtime restoration could not establish "
            f"a safe state; lab_stop_exit={stopped_exit} state={stopped_state} "
            f"fallback={fallback_error}; original={original}; recovery={recovery}"
        ) from recovery_error


def probe_ac007(generation: str) -> dict[str, Any]:
    run_unittest(
        "consumer.tests.test_processing.ProcessingTests.test_customers_phone_stops_before_migration_and_replays_same_coordinate"
    )
    run_unittest(
        "consumer.tests.test_runner.RunnerTests.test_processing_failure_never_attempts_offset_commit"
    )
    if not source_has_phone():
        container = consumer_container(generation)
        topic = generation_value(generation, "KAFKA_CUSTOMERS_TOPIC")
        try:
            run(["docker", "stop", container], timeout=60)
            offsets_before = committed_offsets(generation)
            ends_before = topic_end_offsets(topic)
            run(
                [
                    "docker",
                    "exec",
                    "--interactive",
                    POSTGRES_CONTAINER,
                    "psql",
                    "--username",
                    "cdc_admin",
                    "--dbname",
                    "cdc_lab",
                    "--no-psqlrc",
                    "--set",
                    "ON_ERROR_STOP=1",
                ],
                input_text=(PROJECT_ROOT / "postgres" / "exercises" / "03-schema-change.sql").read_text(encoding="utf-8"),
                timeout=60,
            )
            deadline = time.monotonic() + 60
            coordinate: tuple[str, int, int] | None = None
            while time.monotonic() < deadline:
                ends_after = topic_end_offsets(topic)
                changed = [
                    (partition, ends_before[partition], end)
                    for partition, end in ends_after.items()
                    if end != ends_before[partition]
                ]
                if len(changed) == 1 and changed[0][2] == changed[0][1] + 1:
                    partition, offset, _ = changed[0]
                    coordinate = (topic, partition, offset)
                    break
                if changed:
                    raise AcceptanceError(f"schema change produced unexpected topic offsets: {changed!r}")
                time.sleep(1)
            if coordinate is None:
                raise AcceptanceError("connector did not publish the phone schema-change record")
            run(["docker", "start", container], timeout=60)
            wait_consumer_exit(generation)
            log_result = run(["docker", "logs", "--tail", "100", container], timeout=30)
            logs = log_result.stdout + log_result.stderr
            if "customers.phone requires SQLite migration 0002_customers_phone" not in logs:
                raise AcceptanceError("consumer fail-stop did not report the approved phone migration guard")
            offsets_after_failure = committed_offsets(generation)
            if offsets_after_failure != offsets_before:
                raise AcceptanceError("consumer-group offsets advanced after the pre-migration failure")
            before_migrate = sink_summary(generation)
            if [list(item) for item in before_migrate["migrations"]] != [[1, "initial"]]:
                raise AcceptanceError("sink was not at migration version 1 before the controlled migration")
            run([str(LAB), "migrate", generation], timeout=120)
            run(["docker", "start", container], timeout=60)
            wait_projection_parity(generation)
            replay = sink_json(
                generation,
                f'''\nimport json, os, sqlite3\nc=sqlite3.connect(f"file:{{os.environ['SQLITE_SINK_PATH']}}?mode=ro", uri=True)\nrow=c.execute("SELECT delivery_count,operation FROM raw_events WHERE topic=? AND partition=? AND offset=?", {coordinate!r}).fetchone(); c.close()\nprint(json.dumps(row))\n''',
            )
            offsets_after_replay = committed_offsets(generation)
            if replay != [1, "u"] or offsets_after_replay[(coordinate[0], coordinate[1])] != str(coordinate[2] + 1):
                raise AcceptanceError("phone coordinate did not replay and commit exactly once after migration")
            mode = "live_transition"
            provenance = "current generation live transition"
            offset_before = offsets_before[(coordinate[0], coordinate[1])]
            offset_after = offsets_after_replay[(coordinate[0], coordinate[1])]
        except Exception as transition_error:
            restore_ac007_runtime(generation, transition_error)
            raise
    else:
        evidence = prior_live_transition_evidence(generation)
        historical = historical_ac007_provenance() if evidence is None else None
        if evidence is not None:
            evidence_path, coordinate, offset_before, offset_after = evidence
            mode = "completed_transition"
            provenance = f"strong prior evidence={evidence_path.parent.name}"
        elif historical is not None:
            mode = "historical_live_transition"
            provenance = "R-06 status plus archived g001 sink coordinate revalidated"
            coordinate = historical
            offset_before = str(coordinate[2])
            offset_after = str(coordinate[2] + 1)
        else:
            raise AcceptanceError(
                "schema is evolved but no live-transition provenance proves AC-007"
            )
    summary = sink_summary(generation)
    migrations = [list(item) for item in summary["migrations"]]
    phones = {int(row[0]): row[1] for row in summary["phones"]}
    if migrations != [[1, "initial"], [2, "customers_phone"]] or phones.get(1) != "+15550000001":
        raise AcceptanceError("active sink does not contain the approved phone migration state")
    wait_projection_parity(generation)
    coordinate_value = f"{coordinate[0]}:{coordinate[1]}:{coordinate[2]}"
    return {
        "pre_migration_fail_stop": f"mode={mode} coordinate={coordinate_value}; {provenance}",
        "offset_unchanged": f"coordinate={coordinate_value} offset_before={offset_before} offset_after_failure={offset_before}",
        "same_coordinate_replay": f"coordinate={coordinate_value} replayed after migration versions=1,2",
        "phone_projection": f"active sink phone values verified ids={sorted(phones)}",
        "offset_after_replay": f"coordinate={coordinate_value} offset_after_replay={offset_after}; parity and zero lag passed",
    }


def probe_ac008(generation: str) -> dict[str, Any]:
    run_unittest(
        "consumer.tests.test_processing.ProcessingTests.test_applies_snapshot_and_independent_child_projections"
    )
    wait_projection_parity(generation)
    return {
        "child_applies_first": "processor regression applies child projection without parent",
        "intermediate_dangling_state": "processor regression observes allowed dangling child state",
        "final_source_parity": "active source and sink domain rows match",
    }


def probe_ac009(generation: str) -> dict[str, Any]:
    container = consumer_container(generation)
    run(["docker", "stop", container], timeout=60)
    try:
        postgres("UPDATE customers SET updated_at=clock_timestamp() WHERE id=2 RETURNING id;")
        deadline = time.monotonic() + 60
        rows: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            rows = lag_rows(generation)
            if any((row["lag"] or 0) > 0 for row in rows):
                break
            time.sleep(1)
        else:
            raise AcceptanceError("paused consumer did not expose positive partition lag")
        lag_output = run([str(LAB), "lag", generation], timeout=120).stdout
        if "durable_event_age_ms=" not in lag_output or "retained_wal" not in lag_output:
            raise AcceptanceError("lag output is missing event-age or WAL sections")
    finally:
        run(["docker", "start", container], timeout=60)
    wait_projection_parity(generation)
    positive = [row for row in rows if (row["lag"] or 0) > 0]
    return {
        "partition_lag": f"positive lag observed on partitions={[(r['topic'], r['partition'], r['lag']) for r in positive]}",
        "durable_event_age": "lag command reported durable_event_age_ms separately",
        "retained_wal_bytes": "lag command reported replication-slot WAL separately",
    }


def archive_marker() -> dict[str, Any]:
    return sink_json(
        "g001",
        r'''
import hashlib, json
from pathlib import Path
path=Path('/data/.cdc-generation-archived.json')
payload=json.loads(path.read_text(encoding='utf-8'))
print(json.dumps({'payload':payload,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}, sort_keys=True))
''',
    )


def sink_file_checksums(generation: str) -> dict[str, str]:
    completed = run(
        [
            "docker",
            "run",
            "--rm",
            "--interactive",
            "--network",
            "none",
            "--read-only",
            "--volume",
            f"{consumer_volume(generation)}:/data:ro",
            "--env",
            f"SINK_PREFIX=/data/cdc-{generation}.sqlite3",
            CONSUMER_IMAGE,
            "python",
            "-",
        ],
        input_text=r'''
import glob, hashlib, json, os
result={}
for path in sorted(glob.glob(os.environ['SINK_PREFIX'] + '*')):
    digest=hashlib.sha256()
    with open(path,'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    result[os.path.basename(path)]=digest.hexdigest()
if not result:
    raise SystemExit('generation sink files are missing')
print(json.dumps(result, sort_keys=True))
''',
        timeout=60,
    )
    return json.loads(completed.stdout)


ARCHIVE_WRITE_DENIAL_PROGRAM = r'''
import os
from pathlib import Path

root = Path(os.environ["ARCHIVE_ROOT"])
sink = root / os.environ["ARCHIVE_SINK_NAME"]
if not sink.is_file():
    raise SystemExit("archived SQLite sink is missing")
try:
    descriptor = os.open(sink, os.O_WRONLY)
except PermissionError:
    pass
else:
    os.close(descriptor)
    raise SystemExit("archived SQLite sink file is writable")
probe = root / ".r09-write-probe"
try:
    probe.write_text("must fail", encoding="utf-8")
except PermissionError:
    pass
else:
    probe.unlink(missing_ok=True)
    raise SystemExit("archived SQLite directory is writable")
'''


def verify_archive_write_denial() -> None:
    completed = run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--volume",
            f"{consumer_volume('g001')}:/data",
            "--env",
            "ARCHIVE_ROOT=/data",
            "--env",
            "ARCHIVE_SINK_NAME=cdc-g001.sqlite3",
            CONSUMER_IMAGE,
            "python",
            "-c",
            ARCHIVE_WRITE_DENIAL_PROGRAM,
        ],
        check=False,
        timeout=60,
    )
    if completed.returncode != 0:
        raise AcceptanceError("archived g001 sink file or directory is writable by the consumer UID")


def recovery_history(generation: str) -> dict[str, dict[str, int]]:
    return sink_json(
        generation,
        r'''
import json, os, sqlite3
c=sqlite3.connect(f"file:{os.environ['SQLITE_SINK_PATH']}?mode=ro", uri=True)
try:
    snapshot=dict(c.execute("SELECT source_table,count(*) FROM raw_events WHERE operation='r' GROUP BY source_table"))
    streaming=dict(c.execute("SELECT source_table,count(*) FROM raw_events WHERE operation IN ('c','u','d') GROUP BY source_table"))
finally: c.close()
print(json.dumps({'snapshot':snapshot,'streaming':streaming}, sort_keys=True))
''',
    )


def validate_recovery_artifacts(
    manifest: Mapping[str, Any],
    current_checksums: Mapping[str, str],
    marker: Mapping[str, Any],
    history: Mapping[str, Mapping[str, int]],
) -> None:
    expected_manifest = {
        "state": "ACTIVE",
        "phase": "complete",
        "previous_generation": "g001",
        "new_generation": "g002",
        "old_connector": generation_value("g001", "DEBEZIUM_CONNECTOR_NAME"),
        "new_connector": generation_value("g002", "DEBEZIUM_CONNECTOR_NAME"),
        "old_slot": generation_value("g001", "POSTGRES_REPLICATION_SLOT"),
        "new_slot": generation_value("g002", "POSTGRES_REPLICATION_SLOT"),
        "old_topic_prefix": generation_value("g001", "KAFKA_TOPIC_PREFIX"),
        "new_topic_prefix": generation_value("g002", "KAFKA_TOPIC_PREFIX"),
        "old_group": generation_value("g001", "KAFKA_CONSUMER_GROUP"),
        "new_group": generation_value("g002", "KAFKA_CONSUMER_GROUP"),
    }
    expected_manifest_keys = set(expected_manifest) | {
        "old_sink_checksum_before",
        "old_sink_checksum_after",
        "updated_at",
    }
    if set(manifest) != expected_manifest_keys:
        raise AcceptanceError(
            f"g002 recovery manifest shape mismatch: {sorted(manifest)!r}"
        )
    mismatched = {
        key: (manifest.get(key), value)
        for key, value in expected_manifest.items()
        if manifest.get(key) != value
    }
    if mismatched:
        raise AcceptanceError(f"g002 recovery manifest identity mismatch: {mismatched!r}")
    try:
        updated_at = datetime.fromisoformat(str(manifest["updated_at"]))
    except ValueError as error:
        raise AcceptanceError("g002 recovery manifest timestamp is invalid") from error
    if updated_at.tzinfo is None:
        raise AcceptanceError("g002 recovery manifest timestamp lacks timezone")
    try:
        checksum_before = json.loads(str(manifest["old_sink_checksum_before"]))
        checksum_after = json.loads(str(manifest["old_sink_checksum_after"]))
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise AcceptanceError("g002 manifest sink checksums are invalid") from error
    checksum_sets = (checksum_before, checksum_after, dict(current_checksums))
    if any(
        not isinstance(checksums, Mapping)
        or not checksums
        or any(
            not isinstance(name, str)
            or not name.startswith("cdc-g001.sqlite3")
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            for name, digest in checksums.items()
        )
        for checksums in checksum_sets
    ):
        raise AcceptanceError("g001 sink checksum map is invalid")
    if checksum_before != checksum_after or checksum_after != dict(current_checksums):
        raise AcceptanceError("current archived g001 sink checksums do not match the recovery manifest")
    if set(marker) != {"payload", "sha256"} or re.fullmatch(
        r"[0-9a-f]{64}", str(marker.get("sha256", ""))
    ) is None:
        raise AcceptanceError("g001 archive marker envelope is invalid")
    payload = marker.get("payload")
    expected_marker_keys = {
        "active_successor",
        "archived_generation",
        "archived_at",
        "state",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected_marker_keys:
        raise AcceptanceError("g001 archive marker shape is invalid")
    if (
        payload.get("active_successor") != "g002"
        or payload.get("archived_generation") != "g001"
        or payload.get("state") != "ARCHIVED_READ_ONLY"
    ):
        raise AcceptanceError("g001 archive marker identity is invalid")
    try:
        archived_at = datetime.fromisoformat(str(payload["archived_at"]))
    except ValueError as error:
        raise AcceptanceError("g001 archive marker timestamp is invalid") from error
    if archived_at.tzinfo is None:
        raise AcceptanceError("g001 archive marker timestamp lacks timezone")
    required_tables = {"customers", "orders", "payments"}
    for mode in ("snapshot", "streaming"):
        counts = history.get(mode)
        if not isinstance(counts, Mapping) or set(counts) != required_tables or any(
            not isinstance(counts[table], int) or counts[table] <= 0
            for table in required_tables
        ):
            raise AcceptanceError(f"g002 {mode} history is incomplete: {counts!r}")


RECOVERY_ROW_FIELDS = {
    "customers": [
        "id",
        "email",
        "full_name",
        "status",
        "phone",
        "created_at_us",
        "updated_at_us",
    ],
    "orders": [
        "id",
        "customer_id",
        "status",
        "total_amount",
        "created_at_us",
        "updated_at_us",
    ],
    "payments": [
        "id",
        "order_id",
        "status",
        "amount",
        "provider_ref",
        "created_at_us",
        "updated_at_us",
    ],
}


def validate_zero_lag_rows(
    generation: str, lag: list[dict[str, Any]]
) -> None:
    topics = {
        generation_value(generation, "KAFKA_CUSTOMERS_TOPIC"),
        generation_value(generation, "KAFKA_ORDERS_TOPIC"),
        generation_value(generation, "KAFKA_PAYMENTS_TOPIC"),
    }
    expected_coordinates = {
        (topic, partition) for topic in topics for partition in range(3)
    }
    actual_coordinates = {
        (row.get("topic"), row.get("partition")) for row in lag
    }
    if (
        len(topics) != 3
        or len(lag) != 9
        or len(actual_coordinates) != 9
        or actual_coordinates != expected_coordinates
        or not all(lag_row_is_zero(row) for row in lag)
    ):
        raise AcceptanceError(
            "g002 recovery checkpoint does not have the exact 3x3 zero-lag set"
        )


def validate_recovery_checkpoint(
    generation: str,
    source_rows: Mapping[str, Any],
    sink_rows: Mapping[str, Any],
    ingestion_errors: int,
    lag: list[dict[str, Any]],
) -> dict[str, Any]:
    required_lengths = {
        table: len(fields) for table, fields in RECOVERY_ROW_FIELDS.items()
    }
    if set(source_rows) != set(required_lengths) or source_rows != sink_rows:
        raise AcceptanceError("g002 recovery source/sink parity is incomplete")
    for table, row_length in required_lengths.items():
        rows = source_rows[table]
        if not isinstance(rows, list) or any(
            not isinstance(row, list)
            or len(row) != row_length
            or not all(isinstance(value, str) and value.isdigit() for value in row[-2:])
            for row in rows
        ):
            raise AcceptanceError(
                f"g002 recovery parity lacks complete timestamped {table} rows"
            )
    if ingestion_errors != 0:
        raise AcceptanceError(
            f"g002 recovery ingestion_errors is not empty: {ingestion_errors}"
        )
    validate_zero_lag_rows(generation, lag)
    return {
        "captured_at": utc_now(),
        "connector_running": True,
        "row_fields": RECOVERY_ROW_FIELDS,
        "row_counts": {table: len(source_rows[table]) for table in required_lengths},
        "rows_sha256": json_sha256(source_rows),
        "ingestion_errors": ingestion_errors,
        "lag_rows": lag,
    }


def recovery_checkpoint(generation: str) -> dict[str, Any]:
    run([str(LAB), "status", generation], timeout=120)
    wait_projection_parity(generation)
    lag = wait_lag_zero(generation)
    source_rows = source_projection_rows()
    sink_rows = sink_projection_rows(generation)
    return validate_recovery_checkpoint(
        generation,
        source_rows,
        sink_rows,
        sink_ingestion_error_count(generation),
        lag,
    )


AC010_STRUCTURED_EVIDENCE_KEY = "__structured_evidence__"


def validate_ac010_structured_evidence(payload: Mapping[str, Any]) -> None:
    expected_fields = {
        "format_version",
        "ac_id",
        "generation",
        "manifest",
        "checksums",
        "archive_marker",
        "history",
        "checkpoints",
        "checkpoint_interval_ms",
    }
    if set(payload) != expected_fields or payload.get("format_version") != 1:
        raise AcceptanceError("AC-010 structured evidence has an invalid shape")
    if payload.get("ac_id") != "AC-010" or payload.get("generation") != "g002":
        raise AcceptanceError("AC-010 structured evidence identity is invalid")
    manifest = payload.get("manifest")
    checksums = payload.get("checksums")
    marker = payload.get("archive_marker")
    history = payload.get("history")
    if (
        not isinstance(manifest, Mapping)
        or not isinstance(checksums, Mapping)
        or set(checksums) != {"before", "after", "current"}
        or not isinstance(marker, Mapping)
        or not isinstance(history, Mapping)
    ):
        raise AcceptanceError("AC-010 structured recovery artifacts are invalid")
    validate_recovery_artifacts(
        manifest,
        checksums["current"],
        marker,
        history,
    )
    try:
        manifest_before = json.loads(str(manifest["old_sink_checksum_before"]))
        manifest_after = json.loads(str(manifest["old_sink_checksum_after"]))
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise AcceptanceError("AC-010 structured checksum maps are invalid") from error
    if (
        checksums["before"] != manifest_before
        or checksums["after"] != manifest_after
        or checksums["current"] != manifest_after
    ):
        raise AcceptanceError("AC-010 structured checksum maps disagree")
    checkpoints = payload.get("checkpoints")
    if not isinstance(checkpoints, list) or len(checkpoints) != 2:
        raise AcceptanceError("AC-010 structured evidence requires two checkpoints")
    checkpoint_fields = {
        "captured_at",
        "connector_running",
        "row_fields",
        "row_counts",
        "rows_sha256",
        "ingestion_errors",
        "lag_rows",
    }
    parsed_times: list[datetime] = []
    stable_states: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        if not isinstance(checkpoint, Mapping) or set(checkpoint) != checkpoint_fields:
            raise AcceptanceError("AC-010 checkpoint shape is invalid")
        try:
            captured_at = datetime.fromisoformat(str(checkpoint["captured_at"]))
        except ValueError as error:
            raise AcceptanceError("AC-010 checkpoint timestamp is invalid") from error
        if captured_at.tzinfo is None:
            raise AcceptanceError("AC-010 checkpoint timestamp lacks timezone")
        if checkpoint["connector_running"] is not True:
            raise AcceptanceError("AC-010 checkpoint connector is not RUNNING")
        if checkpoint["row_fields"] != RECOVERY_ROW_FIELDS:
            raise AcceptanceError("AC-010 checkpoint row fields omit required timestamps")
        row_counts = checkpoint["row_counts"]
        if (
            not isinstance(row_counts, Mapping)
            or set(row_counts) != set(RECOVERY_ROW_FIELDS)
            or any(
                not isinstance(count, int) or isinstance(count, bool) or count < 0
                for count in row_counts.values()
            )
        ):
            raise AcceptanceError("AC-010 checkpoint row counts are invalid")
        if re.fullmatch(r"[0-9a-f]{64}", str(checkpoint["rows_sha256"])) is None:
            raise AcceptanceError("AC-010 checkpoint row hash is invalid")
        if checkpoint["ingestion_errors"] != 0:
            raise AcceptanceError("AC-010 checkpoint contains ingestion errors")
        lag_rows_value = checkpoint["lag_rows"]
        if not isinstance(lag_rows_value, list):
            raise AcceptanceError("AC-010 checkpoint lag rows are invalid")
        validate_zero_lag_rows("g002", lag_rows_value)
        parsed_times.append(captured_at)
        stable_states.append(
            {
                key: checkpoint[key]
                for key in checkpoint_fields - {"captured_at"}
            }
        )
    interval_ms = payload.get("checkpoint_interval_ms")
    if (
        not isinstance(interval_ms, int)
        or isinstance(interval_ms, bool)
        or interval_ms < 5000
        or (parsed_times[1] - parsed_times[0]).total_seconds() < 5
    ):
        raise AcceptanceError("AC-010 checkpoints are less than 5 seconds apart")
    if stable_states[0] != stable_states[1]:
        raise AcceptanceError("AC-010 structured checkpoints are not stable")


def probe_ac010(generation: str) -> dict[str, Any]:
    if generation not in {"g001", "g002"}:
        raise AcceptanceError(f"AC-010 only permits the approved g001 to g002 recovery, got {generation}")
    marker_before = archive_marker() if generation == "g002" else None
    transition = "existing ACTIVE g002 revalidated without rerunning recovery"
    if generation == "g001":
        run([str(LAB), "new-generation", "g002"], timeout=360)
        transition = "g001 to g002 recovery executed once"
    if detect_active_generation() != "g002":
        raise AcceptanceError("g002 is not the unique ACTIVE recovery generation")
    run([str(LAB), "resume", "g002"], timeout=60)
    manifest = json.loads(
        (RUNTIME_ROOT / "generations" / "g002" / "manifest.json").read_text(encoding="utf-8")
    )
    marker = archive_marker()
    checksums = sink_file_checksums("g001")
    history = recovery_history("g002")
    validate_recovery_artifacts(manifest, checksums, marker, history)
    verify_archive_write_denial()
    if marker_before is not None and marker_before != marker:
        raise AcceptanceError("resuming ACTIVE g002 changed the g001 archive marker")
    checkpoint_one = recovery_checkpoint("g002")
    checkpoint_one_at = time.monotonic()
    run([str(LAB), "lag", "g002"], timeout=120)
    time.sleep(5)
    checkpoint_two = recovery_checkpoint("g002")
    checkpoint_interval_ms = int((time.monotonic() - checkpoint_one_at) * 1000)
    if checkpoint_interval_ms < 5000:
        raise AcceptanceError("g002 stable checkpoints were less than 5 seconds apart")
    stable_fields = set(checkpoint_one) - {"captured_at"}
    if any(checkpoint_one[field] != checkpoint_two[field] for field in stable_fields):
        raise AcceptanceError("g002 source/sink parity changed between stable checkpoints")
    marker_after = archive_marker()
    if marker_after != marker or sink_file_checksums("g001") != checksums:
        raise AcceptanceError("AC-010 validation changed the archived g001 sink")
    checksum_before = json.loads(str(manifest["old_sink_checksum_before"]))
    checksum_after = json.loads(str(manifest["old_sink_checksum_after"]))
    structured_evidence = {
        "format_version": 1,
        "ac_id": "AC-010",
        "generation": "g002",
        "manifest": manifest,
        "checksums": {
            "before": checksum_before,
            "after": checksum_after,
            "current": checksums,
        },
        "archive_marker": marker,
        "history": history,
        "checkpoints": [checkpoint_one, checkpoint_two],
        "checkpoint_interval_ms": checkpoint_interval_ms,
    }
    validate_ac010_structured_evidence(structured_evidence)
    return {
        "old_generation_read_only": f"archive_marker_sha256={marker['sha256']}; current sink file/directory checksums and write denial verified; {transition}",
        "isolated_identifiers": "exact g001/g002 connector, slot, topic-prefix, group, manifest shape, marker and sink checksums verified",
        "new_snapshot_streaming": "g002 connector/task RUNNING twice, positive snapshot+streaming history and ingestion_errors=0 verified",
        "stable_parity_lag": (
            "two complete timestamped source/sink parity and exact 3x3 zero-lag checkpoints; "
            f"rows_sha256={checkpoint_two['rows_sha256']} interval_ms={checkpoint_interval_ms}"
        ),
        AC010_STRUCTURED_EVIDENCE_KEY: structured_evidence,
    }


def probe_ac011(generation: str) -> dict[str, Any]:
    lag_output = run([str(LAB), "lag", generation], timeout=120).stdout
    preflight = run([str(LAB), "preflight"], timeout=120).stdout
    run_unittest(
        "consumer.tests.test_runner.RunnerTests.test_sqlite_size_guard_sets_hard_limit_and_stops_before_poll"
    )
    with tempfile.TemporaryDirectory() as temporary:
        mock_df = Path(temporary) / "df"
        mock_df.write_text(
            "#!/usr/bin/env bash\nprintf 'Filesystem 1024-blocks Used Available Capacity Mounted on\\n'\nprintf 'mock 10000000 6000000 4000000 60%% /mock\\n'\n",
            encoding="utf-8",
        )
        mock_df.chmod(0o755)
        refused = run(
            [str(LAB), "preflight"],
            check=False,
            env={"PATH": f"{temporary}:{os.environ['PATH']}"},
            timeout=120,
        )
    if refused.returncode == 0 or "less than the required 5 GiB" not in refused.stderr:
        raise AcceptanceError("preflight did not refuse mocked Docker storage below 5 GiB")
    if (
        "retention_bytes=268435456" not in lag_output
        or "segment_bytes=67108864" not in lag_output
        or "--- postgres_replication_slot_retained_wal ---" not in lag_output
        or not slot_metric_present(lag_output, generation)
    ):
        raise AcceptanceError("retention or slot limit evidence is missing")
    table_ranges = table_topic_offset_ranges(generation)
    wal_boundary = validate_wal_warning_boundary()
    retention_probe = run_kafka_retention_probe(generation)
    partition_count = sum(
        len(topic_range["earliest"]) for topic_range in table_ranges.values()
    )
    return {
        "topic_retention": (
            "3 table topics verified delete/24h/256MiB/64MiB; disposable probe used "
            f"segment_bytes={retention_probe['segment_bytes']} and restored by topic deletion"
        ),
        "topic_log_offsets": (
            f"table_partitions={partition_count}; probe={retention_probe['topic']} "
            f"start={retention_probe['before_start']}->{retention_probe['after_start']} "
            f"end={retention_probe['before_end']}->{retention_probe['after_end']} cleanup_absent=true"
        ),
        "wal_warning": (
            f"shell validator classified {wal_boundary['below']}=OK, "
            f"{wal_boundary['boundary']}=WARNING, {wal_boundary['above']}=WARNING; "
            "inverse classifications rejected"
        ),
        "slot_state_and_limit": "live slot wal_status, retained bytes, numeric safe_wal_size and 1GiB limit reported",
        "sqlite_controlled_stop": "SQLite hard-limit regression stopped before poll/offset commit",
        "disk_preflight_refusal": f"mocked 4,000,000 KiB storage refused; real preflight={preflight.splitlines()[0]}",
    }


def probe_ac012(_: str) -> dict[str, Any]:
    observed = run_fixture("ac-012-malformed.json")
    run_unittest(
        "consumer.tests.test_runner.RunnerTests.test_processing_failure_never_attempts_offset_commit"
    )
    error = observed["errors"][0]
    return {
        "projection_unchanged": "malformed fixture creates no projection row",
        "offset_unchanged": "runner regression confirms malformed processing never commits offset",
        "durable_error": f"durable error class={error['error_class']} attempts={error['attempt_count']}",
        "nonzero_exit": "malformed processor raises RecordValidationError",
        "same_record_retry": f"same malformed coordinate retried attempts={error['attempt_count']}",
    }


def probe_ac013(generation: str) -> dict[str, Any]:
    run(
        [
            "docker",
            "exec",
            POSTGRES_CONTAINER,
            "psql",
            "--username",
            "cdc_admin",
            "--dbname",
            "cdc_lab",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "--file",
            "/opt/cdc/postgres/checks/verify-source-contract.sql",
        ],
        timeout=120,
    )
    privileges = postgres(
        "SELECT "
        "(SELECT rolcanlogin AND rolreplication AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole FROM pg_roles WHERE rolname='cdc_connector'),"
        "has_database_privilege('cdc_connector','cdc_lab','CONNECT'),"
        "NOT has_database_privilege('cdc_connector','cdc_lab','CREATE'),"
        "has_schema_privilege('cdc_connector','public','USAGE'),"
        "NOT has_schema_privilege('cdc_connector','public','CREATE'),"
        "(SELECT bool_and(has_table_privilege('cdc_connector',format('public.%I',name),'SELECT')) FROM (VALUES ('customers'),('orders'),('payments')) AS tables(name)),"
        "(SELECT bool_and(NOT has_table_privilege('cdc_connector',format('public.%I',name),'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) FROM (VALUES ('customers'),('orders'),('payments')) AS tables(name));"
    )
    if privileges != "t|t|t|t|t|t|t":
        raise AcceptanceError(f"connector role/grant contract mismatch: {privileges}")
    denied = run(
        [
            "docker",
            "exec",
            "--interactive",
            POSTGRES_CONTAINER,
            "psql",
            "--username",
            "cdc_admin",
            "--dbname",
            "cdc_lab",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
        ],
        check=False,
        input_text=(
            "\\set VERBOSITY verbose\n"
            "BEGIN;\n"
            "SET LOCAL ROLE cdc_connector;\n"
            "UPDATE public.customers SET full_name=full_name WHERE id=1;\n"
            "ROLLBACK;\n"
        ),
        timeout=60,
    )
    if denied.returncode == 0 or "42501" not in denied.stderr:
        raise AcceptanceError("connector-role DML was not denied with SQLSTATE 42501")
    run([str(LAB), "status", generation], timeout=120)
    return {
        "connector_snapshot_streaming": f"connector generation={generation} RUNNING with snapshot history",
        "role_attributes": "connector role LOGIN/REPLICATION and no elevated role attributes verified",
        "effective_grants": "CONNECT/USAGE/SELECT present; database/schema CREATE and table DML absent",
        "dml_denied": "connector UPDATE rejected with SQLSTATE 42501",
    }


def probe_ac014(generation: str) -> dict[str, Any]:
    before = sink_summary(generation)
    run(["docker", "restart", CONNECT_CONTAINER], timeout=120)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        health = run(
            ["docker", "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{end}}", CONNECT_CONTAINER],
            check=False,
            timeout=20,
        ).stdout.strip()
        if health == "healthy":
            break
        time.sleep(2)
    else:
        raise AcceptanceError("Kafka Connect did not become healthy after restart")
    run([str(LAB), "status", generation], timeout=120)
    postgres("UPDATE customers SET updated_at=clock_timestamp() WHERE id=2 RETURNING id;")
    wait_projection_parity(generation)
    after = sink_summary(generation)
    if after["snapshot_total"] != before["snapshot_total"]:
        raise AcceptanceError("connector restart produced new snapshot events")
    if int(after["raw_count"]) <= int(before["raw_count"]):
        raise AcceptanceError("post-restart streaming record did not reach sink")
    return {
        "no_new_snapshot": f"snapshot count remained {after['snapshot_total']}",
        "same_slot_offsets": f"generation={generation} connector resumed existing slot/group state",
        "new_streaming_record": f"raw count grew {before['raw_count']}->{after['raw_count']}",
        "sink_updated": "post-restart customer update reached SQLite projection",
    }


PROBES: dict[str, Callable[[str], dict[str, Any]]] = {
    "AC-001": probe_ac001,
    "AC-002": probe_ac002,
    "AC-003": probe_ac003,
    "AC-004": probe_ac004,
    "AC-005": probe_ac005,
    "AC-006": probe_ac006,
    "AC-007": probe_ac007,
    "AC-008": probe_ac008,
    "AC-009": probe_ac009,
    "AC-010": probe_ac010,
    "AC-011": probe_ac011,
    "AC-012": probe_ac012,
    "AC-013": probe_ac013,
    "AC-014": probe_ac014,
}


def publish_result(
    ac_id: str,
    generation: str,
    started_at: str,
    summaries: Mapping[str, str],
    *,
    failure: str | None = None,
    structured_evidence: Mapping[str, Any] | None = None,
    runtime_root: Path = RUNTIME_ROOT,
) -> Path:
    required = manifest_assertions()[ac_id]
    if failure is None and set(summaries) != set(required):
        raise AcceptanceError(
            f"probe {ac_id} returned assertion names {sorted(summaries)}, expected {required}"
        )
    if failure is None and ac_id == "AC-010":
        if structured_evidence is None:
            raise AcceptanceError("AC-010 PASS requires structured machine-readable evidence")
        validate_ac010_structured_evidence(structured_evidence)
    elif structured_evidence is not None:
        raise AcceptanceError("structured recovery evidence is only valid for AC-010 PASS")
    assertions: list[dict[str, str]] = []
    for index, name in enumerate(required):
        if failure is None:
            result = "PASS"
            summary = summaries[name]
        elif index == 0:
            result = "FAIL"
            summary = failure[:240]
        else:
            result = "NOT_RUN"
            summary = "not evaluated after the acceptance probe failed"
        assertions.append({"name": name, "result": result, "summary": summary})

    observations = {
        "ac_id": ac_id,
        "generation": generation,
        "result": "FAIL" if failure else "PASS",
        "assertion_summaries": {item["name"]: item["summary"] for item in assertions},
    }
    with tempfile.TemporaryDirectory() as temporary:
        artifact_root = Path(temporary)
        artifact = artifact_root / "observations.json"
        artifact.write_text(
            json.dumps(observations, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        artifacts = [
            {
                "path": "observations.json",
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "media_type": "application/json",
            }
        ]
        if structured_evidence is not None:
            recovery_artifact = artifact_root / "ac-010-recovery.json"
            recovery_artifact.write_text(
                json.dumps(structured_evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            artifacts.append(
                {
                    "path": recovery_artifact.name,
                    "sha256": hashlib.sha256(recovery_artifact.read_bytes()).hexdigest(),
                    "media_type": "application/json",
                }
            )
        document = {
            "format_version": 1,
            "ac_id": ac_id,
            "generation": generation,
            "result": "FAIL" if failure else "PASS",
            "started_at": started_at,
            "finished_at": utc_now(),
            "command": ["scripts/check", "acceptance", ac_id],
            "assertions": assertions,
            "artifacts": artifacts,
            "redactions": list(REQUIRED_REDACTIONS),
        }
        return write_evidence(
            document,
            runtime_root,
            artifact_root=artifact_root,
        )


def write_run_index(
    generation: str,
    started_at: str,
    results: list[tuple[str, bool, Path]],
    *,
    runtime_root: Path = RUNTIME_ROOT,
) -> Path:
    expected_ids = list(PROBES)
    actual_ids = [ac_id for ac_id, _, _ in results]
    if actual_ids != expected_ids:
        raise AcceptanceError(
            f"acceptance run index requires {expected_ids}, got {actual_ids}"
        )
    finished_at = utc_now()
    run_id = datetime.fromisoformat(started_at).astimezone(timezone.utc).strftime(
        "%Y%m%dT%H%M%S.%fZ"
    )
    parent = runtime_root / "evidence" / generation / "runs"
    parent.mkdir(parents=True, exist_ok=True)
    output = parent / f"{run_id}.json"
    payload = {
        "format_version": 1,
        "generation": generation,
        "started_at": started_at,
        "finished_at": finished_at,
        "result": "PASS" if all(ok for _, ok, _ in results) else "FAIL",
        "criteria": [
            {
                "ac_id": ac_id,
                "result": "PASS" if ok else "FAIL",
                "evidence": str(path.relative_to(runtime_root)),
            }
            for ac_id, ok, path in results
        ],
    }
    if output.exists():
        raise FileExistsError(output)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="x",
            encoding="utf-8",
            dir=parent,
            prefix=f".{run_id}.",
            suffix=".tmp",
            delete=False,
        ) as target:
            temporary = Path(target.name)
            json.dump(payload, target, indent=2, sort_keys=True)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        temporary.replace(output)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        output.unlink(missing_ok=True)
        raise
    return output


def execute_one(
    ac_id: str,
    generation: str,
    *,
    runtime_root: Path = RUNTIME_ROOT,
    probe: Callable[[str], dict[str, Any]] | None = None,
) -> tuple[bool, Path]:
    started_at = utc_now()
    selected = PROBES[ac_id] if probe is None else probe
    evidence_generation = generation
    try:
        probe_output = selected(generation)
        structured_evidence = None
        if ac_id == "AC-010":
            evidence_generation = detect_active_generation()
            structured_evidence = probe_output.get(AC010_STRUCTURED_EVIDENCE_KEY)
            summaries = {
                name: summary
                for name, summary in probe_output.items()
                if name != AC010_STRUCTURED_EVIDENCE_KEY
            }
        else:
            summaries = probe_output
        output = publish_result(
            ac_id,
            evidence_generation,
            started_at,
            {name: str(summary) for name, summary in summaries.items()},
            structured_evidence=structured_evidence,
            runtime_root=runtime_root,
        )
        print(json.dumps({"ac_id": ac_id, "evidence": str(output), "result": "PASS"}, sort_keys=True))
        return True, output
    except Exception as error:  # noqa: BLE001 - evidence must capture any failed probe
        if ac_id == "AC-010":
            try:
                evidence_generation = detect_active_generation()
            except Exception:  # noqa: BLE001 - keep the original probe failure
                evidence_generation = generation
        output = publish_result(
            ac_id,
            evidence_generation,
            started_at,
            {},
            failure=f"{type(error).__name__}: {error}",
            runtime_root=runtime_root,
        )
        print(
            json.dumps(
                {"ac_id": ac_id, "evidence": str(output), "result": "FAIL"},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        if isinstance(error, RuntimeRestorationError):
            raise error
        return False, output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("selector", help="AC-001..AC-014 or all")
    return parser.parse_args()


def run_acceptance(selector: str) -> dict[str, Any]:
    generation = detect_active_generation()
    initial_state = classify_runtime_state(generation)
    cleanup_required = initial_state == "stopped"
    run_started_at = utc_now()
    results: list[tuple[str, bool, Path]] = []
    failures: list[str] = []
    run_index: Path | None = None
    selected = (
        [ac_id for ac_id in PROBES if ac_id != "AC-010"] + ["AC-010"]
        if selector == "all"
        else [selector]
    )
    try:
        ensure_runtime(generation)
        for ac_id in selected:
            ok, path = execute_one(ac_id, generation)
            results.append((ac_id, ok, path))
            if not ok:
                failures.append(ac_id)
        final_generation = detect_active_generation()
        if selector == "all":
            result_by_id = {ac_id: (ok, path) for ac_id, ok, path in results}
            ordered_results = [
                (ac_id, *result_by_id[ac_id])
                for ac_id in PROBES
            ]
            run_index = write_run_index(final_generation, run_started_at, ordered_results)
        if failures:
            raise AcceptanceError(
                "acceptance failed: "
                + ", ".join(failures)
                + (f"; index={run_index}" if run_index is not None else "")
            )
        return {
            "generation": final_generation,
            "index": str(run_index) if run_index is not None else None,
            "passed": list(PROBES) if selector == "all" else selected,
            "result": "PASS",
        }
    finally:
        if cleanup_required:
            cleanup = run([str(LAB), "stop"], check=False, timeout=120)
            if cleanup.returncode != 0:
                raise AcceptanceError("failed to restore the initial stopped runtime state")


def main() -> None:
    args = parse_args()
    selector = args.selector
    if selector != "all" and AC_PATTERN.fullmatch(selector) is None:
        raise SystemExit(f"unknown acceptance selector: {selector}")
    try:
        result = run_acceptance(selector)
    except AcceptanceError as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
