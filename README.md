# PostgreSQL CDC Lab

A container-first learning project for observing an end-to-end Change Data Capture pipeline:

```text
PostgreSQL WAL → Debezium → Kafka → Python consumer → SQLite
```

The lab demonstrates initial snapshots, streaming changes, deletes and tombstones, durable offset handling, duplicate delivery, stale-event protection, schema migration, lag diagnostics, and isolated recovery generations. It is an educational lab, not a production deployment template.

The approved behavioral contract is [`docs/spec.md`](docs/spec.md). Detailed operating procedures are in [`docs/runbook.md`](docs/runbook.md).

## Key properties

- PostgreSQL logical replication through `pgoutput` and a least-privilege connector account.
- One three-partition Kafka topic per source table: `customers`, `orders`, and `payments`.
- A SQLite sink containing the raw event archive, current-state projections, ordering watermarks, and ingestion errors.
- Kafka offsets committed only after the corresponding SQLite transaction is durable.
- At-least-once delivery with idempotent handling of repeated `(topic, partition, offset)` coordinates.
- Source-position comparison that prevents stale events from overwriting newer state.
- Controlled schema evolution through stop, migrate, and replay.
- Generation-scoped connectors, slots, topics, consumer groups, and sinks for recovery.

The project does **not** claim exactly-once processing. Runtime images and dependencies are pinned to exact versions and immutable digests defined in the specification.

## Requirements

Install Docker Engine or Docker Desktop with Docker Compose v2. The main path does not require host installations of Python, Java, Kafka, or PostgreSQL.

Make available to Docker approximately:

- 6 CPUs;
- 8 GiB of memory;
- at least 5 GiB of free storage before load or retention exercises.

Linux, macOS, and WSL2 are supported subject to availability of the pinned image platform.

## Quick start

Create a local secrets file and replace both placeholders:

```bash
cp .env.example .env
```

```dotenv
POSTGRES_ADMIN_PASSWORD=replace-with-a-local-secret
POSTGRES_CONNECTOR_PASSWORD=replace-with-a-local-secret
```

Never commit `.env` or include its values in logs or evidence.

Run the read-only preflight check:

```bash
scripts/lab preflight
```

If exact images are missing, explicitly pull and build them. This command uses the network:

```bash
scripts/lab pull
```

Start the core services and bootstrap the initial generation:

```bash
scripts/lab up
scripts/lab bootstrap g001
```

Inspect the lab:

```bash
scripts/lab status g001
scripts/lab lag g001
```

`lag` reports Kafka consumer lag, durable event age, retained PostgreSQL WAL, Kafka retention settings, and the SQLite size guard as separate measurements.

## Verification

Run the smallest relevant check first:

```bash
scripts/check unit
scripts/check smoke
```

Run one acceptance criterion or the complete AC-001–AC-014 suite:

```bash
scripts/check acceptance AC-001
scripts/check acceptance all
```

Acceptance checks inspect real source data, Kafka records and offsets, connector state, and SQLite state. A failed, skipped, or unavailable check is not reported as a pass.

Redacted evidence is stored under:

```text
.runtime/evidence/<generation>/<AC-ID>/<UTC-start>/
```

Acceptance checks may create reversible source changes, Kafka history, SQLite events, and recovery metadata. See the [verification section of the runbook](docs/runbook.md#verification) before running the full suite.

## Common operations

Stop all project services without deleting durable state:

```bash
scripts/lab stop
```

Restart the initial generation:

```bash
scripts/lab up
scripts/lab bootstrap g001
```

An active successor generation is resumed instead:

```bash
scripts/lab up
scripts/lab resume g002
```

For the controlled schema-change exercise, run:

```bash
scripts/check acceptance AC-007
```

If connector offsets, required Kafka history, or the replication slot become unusable, create an isolated successor rather than resetting the existing generation:

```bash
scripts/lab up
scripts/lab new-generation g002
```

Recovery preserves the previous sink for diagnostics and activates the successor only after source parity and zero-lag checks pass. Detailed procedures are documented in:

- [schema-change exercise](docs/runbook.md#schema-change-exercise-ac-007);
- [recovery generation](docs/runbook.md#recovery-generation-ac-010);
- [restart and stop](docs/runbook.md#restart-and-stop).

## Safety and cleanup

Routine cleanup is always:

```bash
scripts/lab stop
```

Destruction is narrow and requires the exact confirmation argument:

```bash
scripts/lab destroy g001 --confirm g001
```

Do not use broad cleanup commands such as `docker compose down --volumes`, `docker volume prune`, `docker system prune`, `git clean`, or `git reset --hard`. Do not reuse connector, slot, topic, consumer-group, or sink identifiers across generations.

Generated runtime state, SQLite files, secrets, and evidence belong under ignored paths and must not be committed.

## Documentation

- [`docs/spec.md`](docs/spec.md) — authoritative scope, invariants, pinned stack, and AC-001–AC-014.
- [`docs/runbook.md`](docs/runbook.md) — startup, verification, migration, recovery, retention, and cleanup procedures.
- [`docs/bootstrap.md`](docs/bootstrap.md) — approved implementation plan and validation strategy.
- [`docs/idea.md`](docs/idea.md) — original project brief.
