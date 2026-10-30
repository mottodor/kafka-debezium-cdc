# PostgreSQL CDC Lab with Debezium, Kafka, and SQLite

A container-first learning project that demonstrates an end-to-end Change Data Capture (CDC) pipeline:

```text
PostgreSQL WAL → Debezium PostgreSQL Connector → Kafka → Python consumer → SQLite
```

The lab is designed for observing CDC behavior rather than presenting a production deployment template. It covers initial snapshots, streaming changes, deletes and tombstones, durable offset handling, duplicate delivery, stale-event protection, schema migration, lag diagnostics, and generation-based recovery.

The authoritative behavioral contract is [`docs/spec.md`](docs/spec.md). Detailed operating procedures are in [`docs/runbook.md`](docs/runbook.md).

## What This Project Demonstrates

The lab lets you explore:

- PostgreSQL logical replication with `pgoutput`;
- least-privilege access for the Debezium connector;
- Debezium initial snapshot and streaming lifecycle;
- create, update, delete, tombstone, and snapshot/read events;
- Kafka topics, partitions, offsets, and consumer groups;
- Kafka offset commits only after a durable SQLite transaction;
- at-least-once delivery with an idempotent sink;
- duplicate delivery after a crash between the SQLite and Kafka commits;
- protection against stale or out-of-order events;
- controlled schema evolution through stop, migrate, and replay;
- independent Kafka lag, durable event age, and retained WAL metrics;
- isolated CDC generations for bounded recovery.

The complete acceptance contract is defined by AC-001 through AC-014 in [`docs/spec.md`](docs/spec.md).

## Architecture

```text
┌──────────────────────────────┐
│ PostgreSQL 16                │
│ customers / orders / payments│
└──────────────┬───────────────┘
               │ WAL / pgoutput
               ▼
┌──────────────────────────────┐
│ Debezium PostgreSQL Connector│
└──────────────┬───────────────┘
               │ JSON with embedded schemas
               ▼
┌──────────────────────────────┐
│ Kafka KRaft                  │
│ one topic per source table   │
│ three partitions per topic   │
└──────────────┬───────────────┘
               │ sequential consumer processing
               ▼
┌──────────────────────────────┐
│ Python CDC consumer          │
│ confluent-kafka              │
└──────────────┬───────────────┘
               │ one durable SQLite transaction
               ▼
┌──────────────────────────────┐
│ SQLite sink                  │
│ - raw event archive          │
│ - current-state projections  │
│ - ordering watermarks        │
│ - ingestion errors           │
└──────────────────────────────┘
```

The source model contains three tables:

- `customers`;
- `orders`;
- `payments`.

Each table has a separate generation-scoped Kafka topic. The SQLite sink keeps both the original event history and the latest projected state.

## Pinned Stack

The main runtime uses exact versions and immutable image digests defined in [`docs/spec.md`](docs/spec.md):

- PostgreSQL `16.13`;
- Kafka KRaft `4.3.0`;
- Kafka Connect `4.3.0`;
- Debezium PostgreSQL Connector `3.6.3.Final`;
- Python `3.12.12`;
- `confluent-kafka==2.16.0` with `librdkafka 2.16.0`.

Floating `latest` tags are not used.

## Delivery Guarantees

The project implements **at-least-once delivery with an idempotent SQLite sink**.

For every Kafka record, the consumer:

1. starts a SQLite transaction;
2. archives the raw event and updates the projection atomically;
3. commits the SQLite transaction;
4. synchronously commits the Kafka offset for that record.

If the process stops after the SQLite commit but before the Kafka commit, Kafka can deliver the record again. The sink recognizes the existing `(topic, partition, offset)` coordinate and does not apply the logical change twice.

The project does not claim exactly-once processing. Ordering is assumed only within a Kafka partition, while the current-state projection additionally compares Debezium source-position tokens to reject stale events.

See [`docs/spec.md`](docs/spec.md) for the complete contract.

## Prerequisites

Install:

- Docker Engine or Docker Desktop;
- Docker Compose v2.

The configured services require at least:

- 6 CPUs available to Docker;
- 8 GiB of Docker memory;
- 5 GiB of free Docker storage before load or retention exercises.

The intended local environments are Linux, macOS, and WSL2, subject to availability of the pinned image platform. The repository does not require a host Python, Java, Kafka, or PostgreSQL installation for the main path.

## Quick Start

### 1. Create the local secrets file

```bash
cp .env.example .env
```

Replace both placeholder values in `.env`:

```dotenv
POSTGRES_ADMIN_PASSWORD=replace-with-a-local-secret
POSTGRES_CONNECTOR_PASSWORD=replace-with-a-local-secret
```

Use local passwords of your choice. Never commit `.env` or include its values in evidence or logs.

### 2. Run the read-only preflight check

```bash
scripts/lab preflight
```

Preflight validates Docker availability, Compose configuration, resource limits, storage capacity, generation metadata, local secrets, and the presence of pinned images. It does not create or start runtime resources.

### 3. Pull and build missing runtime images

If preflight reports missing images, explicitly run:

```bash
scripts/lab pull
```

This command uses the network. It pulls the digest-pinned service images and builds the consumer image from the pinned Python base and exact dependency version.

### 4. Start and bootstrap the initial generation

```bash
scripts/lab up
scripts/lab bootstrap g001
```

`scripts/lab up` starts the PostgreSQL, Kafka, and Kafka Connect services without an implicit pull or build. `bootstrap g001` verifies the source database, creates or verifies the Kafka topics, registers or verifies the Debezium connector, and starts the consumer.

### 5. Inspect the running lab

```bash
scripts/lab status g001
scripts/lab lag g001
```

A successful process start alone is not acceptance evidence. Use the checks below to verify behavior.

## Running Checks

Run the smallest relevant check first.

### Unit checks

```bash
scripts/check unit
```

Runs the offline consumer, migration, fixture, evidence-format, and acceptance-orchestration tests inside the pinned consumer image without network access.

### Runtime smoke check

```bash
scripts/check smoke
```

Verifies runtime health, exact PostgreSQL/Kafka/Debezium/Python versions, connector readiness, and the active generation. If the lab was stopped before the check, the command restores the stopped state afterward while preserving durable data.

### One acceptance criterion

```bash
scripts/check acceptance AC-001
```

Replace `AC-001` with any criterion from `AC-001` through `AC-014`.

### Complete acceptance suite

```bash
scripts/check acceptance all
```

Acceptance checks may create reversible source changes, Kafka history, SQLite events, and recovery metadata. Each completed criterion publishes redacted evidence under:

```text
.runtime/evidence/<generation>/<AC-ID>/<UTC-start>/
```

A failed or unavailable check is not reported as a pass. The `all` selector normally continues after an ordinary criterion failure so that all completed failures remain visible. It aborts if AC-007 cannot restore a safe and consistent runtime state.

## Inspecting the Lab

Use:

```bash
scripts/lab status <gNNN>
scripts/lab lag <gNNN>
```

For example:

```bash
scripts/lab status g001
scripts/lab lag g001
```

The lag command reports separate sections for:

1. Kafka consumer-group lag by topic partition;
2. event age at the durable SQLite commit boundary;
3. retained PostgreSQL replication-slot WAL;
4. Kafka topic retention configuration;
5. the SQLite size guard.

These measurements describe different parts of the pipeline and are never combined into one number.

## CDC Generations and Recovery

Every CDC generation has isolated identifiers for its:

- Debezium connector;
- PostgreSQL publication and replication slot;
- Kafka topic namespace;
- Kafka consumer group;
- SQLite sink.

The initial generation is `g001`. If connector offsets, the replication slot, required WAL, or the required Kafka history become unusable, do not reset or reuse the old generation in place. Create an isolated successor instead:

```bash
scripts/lab up
scripts/lab new-generation g002
```

The recovery flow preserves the old sink for diagnostics, snapshots the source into the new generation, waits for zero lag, verifies source/sink parity twice, and marks the successor active only after all gates pass.

To resume an existing active successor after a normal stop:

```bash
scripts/lab up
scripts/lab resume g002
scripts/lab status g002
scripts/lab lag g002
```

Do not rerun `new-generation` merely to resume an existing active successor. See [`docs/runbook.md`](docs/runbook.md#recovery-generation-ac-010) for the complete recovery procedure.

## Schema Change Exercise

The supported schema-change exercise adds nullable `customers.phone` data.

Before the corresponding SQLite migration, the consumer must:

- detect the unsupported field;
- roll back the main SQLite transaction;
- leave the Kafka offset uncommitted;
- record diagnostics;
- stop with a nonzero status.

After the stopped sink is migrated, the same Kafka record is replayed and applied.

Run the complete exercise with:

```bash
scripts/check acceptance AC-007
```

The manual migration command is:

```bash
scripts/lab migrate <gNNN>
```

The generation consumer must be stopped before migration. Detailed transition and recovery behavior is documented in [`docs/runbook.md`](docs/runbook.md#schema-change-exercise-ac-007).

## Stopping and Restarting

Stop the lab without deleting durable state:

```bash
scripts/lab stop
```

This preserves:

- PostgreSQL and Kafka volumes;
- Kafka Connect offsets;
- replication slots and publications;
- Kafka topics and records;
- SQLite sinks and acceptance evidence.

Restart an initial generation with:

```bash
scripts/lab up
scripts/lab bootstrap g001
```

If a successor generation is active, use `scripts/lab resume <gNNN>` instead of bootstrapping `g001`.

## Destructive Cleanup

Destruction is intentionally narrow and confirmation-gated:

```bash
scripts/lab destroy g001 --confirm g001
```

The command refuses unsafe cleanup when a successor generation or recovery manifest exists. It must not be replaced with broad Docker cleanup commands.

Do not use:

```text
docker compose down --volumes
docker volume prune
docker system prune
git clean
git reset --hard
```

Ordinary operation should use `scripts/lab stop`.

## Repository Layout

```text
compose.yaml              Docker Compose runtime
config/                   Kafka, Connect, and generation configuration
postgres/                 Source schema, seed, checks, and exercises
consumer/                 Python CDC consumer and SQLite migrations
fixtures/                 Deterministic Debezium event fixtures
scripts/lab               Safe lifecycle, migration, and diagnostic commands
scripts/check             Unit, smoke, and acceptance commands
tests/                    Offline and runtime verification
docs/spec.md              Approved behavioral contract
docs/runbook.md           Detailed operating procedures
docs/bootstrap.md         Implementation plan and design sequence
.runtime/                 Generated runtime state and evidence; not committed
```

## Documentation

- [`docs/spec.md`](docs/spec.md) — authoritative scope, invariants, versions, and AC-001–AC-014;
- [`docs/runbook.md`](docs/runbook.md) — detailed startup, inspection, migration, recovery, and cleanup procedures;
- [`docs/bootstrap.md`](docs/bootstrap.md) — approved implementation plan and validation strategy;
- [`docs/idea.md`](docs/idea.md) — original project brief.

## Safety Notes

- Never commit `.env`, `.runtime/`, SQLite files, or generated evidence.
- Do not print credentials or preserve full environment dumps as evidence.
- Do not replace digest-pinned images with floating tags.
- Do not reuse connector, slot, topic, consumer-group, or sink identifiers across generations.
- Do not commit Kafka offsets before the durable SQLite transaction.
- Do not describe the project as exactly-once.
- Do not use broad Docker cleanup commands for routine operation.
