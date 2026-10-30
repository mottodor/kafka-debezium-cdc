# CDC lab runbook

This runbook operates the local laboratory defined by [`docs/spec.md`](spec.md). It does not replace the behavioral contract or its AC-001–AC-014 criteria.

## Safety boundaries

- Use `scripts/lab` and `scripts/check`; do not run broad Docker cleanup commands.
- `scripts/lab preflight`, `status`, and `lag` are read-only.
- `scripts/lab pull` uses the network and may update the local image cache.
- `scripts/lab stop` preserves PostgreSQL/Kafka volumes, connector offsets, replication slots, topics, and SQLite sinks.
- `scripts/lab destroy g001 --confirm g001` is destructive and remains fail-closed when a successor generation exists.
- Never commit `.env`, `.runtime/`, SQLite files, or acceptance evidence.

## First start

1. Copy `.env.example` to `.env` and replace both password placeholders locally.
2. Verify the host and exact cached images:

   ```text
   scripts/lab preflight
   ```

3. If an exact image is not cached, explicitly authorize and run:

   ```text
   scripts/lab pull
   ```

4. Start and bootstrap the initial generation:

   ```text
   scripts/lab up
   scripts/lab bootstrap g001
   scripts/lab status g001
   scripts/lab lag g001
   ```

`bootstrap g001` is idempotent only when the existing generation still matches the approved source, publication, topic, connector, and access contracts.

## Verification

Run the smallest check first:

```text
scripts/check unit
scripts/check smoke
```

Run one acceptance criterion:

```text
scripts/check acceptance AC-001
```

Run the complete acceptance set:

```text
scripts/check acceptance all
```

Each acceptance invocation detects the active generation, accepts only a fully stopped or fully running lab, verifies or starts its runtime, executes the criterion-specific assertions, and atomically publishes redacted evidence below:

```text
.runtime/evidence/<generation>/<AC-ID>/<UTC-start>/
```

A failed criterion still writes a `FAIL` evidence bundle and returns nonzero. The `all` selector continues after ordinary criterion failures, reports every completed failure, and returns nonzero if any criterion failed. Any failure to restore the consistent running state after an interrupted AC-007 transition publishes AC-007 `FAIL` evidence and aborts the remaining probes immediately. A verified fully stopped fallback makes the lab safe but does not resume the run; an unverified fallback also fails closed. Evidence must not contain credentials, environment dumps, Docker inspect output, or authentication configuration.

If acceptance started a fully stopped lab, it stops the lab again in a `finally` path after success or failure; volumes, connector offsets, slots, topics, manifests, and sinks remain preserved. A lab that was already fully running remains running. Partial runtime state—including any running inactive g001 or successor consumer—is rejected before mutation, and multiple `ACTIVE` generation manifests fail closed.

The acceptance flow may create reversible source updates and CDC history. AC-003 deletes the deterministic `payment 1002 → order 103 → customer 3` aggregate in the approved order. AC-004 runs a named, bounded one-off consumer at an exact Kafka coordinate; its cleanup force-removes that container before the official consumer is restarted, including timeout/error paths. Persistent raw history is expected and is not automatically cleared.

## Inspecting the lab

```text
scripts/lab status g001
scripts/lab lag g001
```

For an active recovery generation, use its identifier:

```text
scripts/lab status g002
scripts/lab lag g002
```

`lag` reports separate sections for:

1. Kafka consumer-group lag by partition;
2. event age at the durable SQLite commit boundary;
3. replication-slot retained WAL bytes and `safe_wal_size`;
4. Kafka topic retention settings;
5. SQLite size guard state.

These values are not interchangeable and are never combined into one metric.

## Schema-change exercise (AC-007)

`scripts/check acceptance AC-007` performs the controlled transition on a schema-v1 generation:

1. Stop its consumer and record committed offsets.
2. Apply `postgres/exercises/03-schema-change.sql` to PostgreSQL.
3. Start the consumer and require fail-stop without offset advancement.
4. Migrate the stopped sink:

   ```text
   scripts/lab migrate g001
   ```

5. Restart the consumer. The same Kafka coordinate must replay successfully, `customers.phone` must be projected, and older rows must retain `NULL`.

On an already evolved generation, the check accepts only explicit live-transition provenance containing the Kafka coordinate and offsets before failure and after replay, then revalidates current migration, phone projection, parity, and lag. This repository's preserved R-06 live evidence is bootstrapped by matching its exact status record to the archived g001 coordinate; subsequent runs require the strong generation-scoped bundle. A legacy PASS containing only unit-test summaries is rejected. Do not edit the migration history or add columns automatically in the consumer.

## Recovery generation (AC-010)

When connector offsets, required Kafka records, or the replication slot are unusable, create a new isolated generation rather than reusing names:

```text
scripts/lab up
scripts/lab new-generation g002
scripts/lab status g002
scripts/lab lag g002
```

The command:

- stops the predecessor connector without dropping its slot;
- preserves and archives the old SQLite sink read-only;
- creates separate publication, slot, topic prefix, consumer group, and sink identifiers;
- snapshots the source into the new generation;
- requires source/sink parity and lag zero twice, at least five seconds apart;
- writes `.runtime/generations/<generation>/manifest.json` with `IN_PROGRESS`, `FAILED`, or `ACTIVE` state.

AC-010 recomputes the current archived sink-file checksum map, requires exact marker shape and non-root write denial, verifies positive snapshot and streaming history for all three tables, and checks source/sink parity plus zero lag twice at least five seconds apart. A failed recovery remains explicit and must be retried after fixing the stated cause. Never mix old and new sinks or offsets.

## Restart and stop

A normal service stop preserves all durable state:

```text
scripts/lab stop
```

Restart with:

```text
scripts/lab up
```

For `g001`, use `scripts/lab bootstrap g001` only while it is not archived. Resume an existing active successor with:

```text
scripts/lab up
scripts/lab resume g002
scripts/lab status g002
```

`resume` first requires the unique exact `ACTIVE`/`complete` g001→g002 recovery manifest; `FAILED`, `IN_PROGRESS`, malformed, or duplicate-active state is rejected. It then reuses the full recovery-consumer scope verifier: exact current image, generation labels, network, volume, environment, sink path, Kafka group/topics, read-only root, restart policy, CPU/memory/PID limits, and stale-image rejection are checked before and after start. `scripts/check smoke` and acceptance use this command. Do not rerun `new-generation` merely to resume an already `ACTIVE` successor: recovery updates archive metadata and is reserved for creating the successor or explicitly retrying a failed recovery.

## Disk and retention guards

Do not start load exercises unless `scripts/lab preflight` reports at least 5 GiB free Docker storage. Stop and investigate when:

- retained slot WAL reaches the 512 MiB warning threshold;
- slot `wal_status` becomes `lost`;
- the SQLite allocation approaches its controlled stop threshold near 1 GiB;
- exact topic retention differs from 24 hours, 256 MiB per partition, delete policy, and 64 MiB segments.

`scripts/check acceptance AC-011` keeps the three table topics at their exact 24-hour/256 MiB/64 MiB contract and uses a uniquely named disposable generation-scoped topic to prove actual closed-segment deletion and log-start movement with scaled byte limits. The probe verifies an unchanged log-end offset, deletes only an old prefix, and removes its topic before returning. It also executes the production shell validator at 512 MiB − 1 byte, exactly 512 MiB, and 512 MiB + 1 byte, including inverse-classification negative cases.

A lost slot or unavailable required Kafka range requires a new CDC generation; it is not repaired by resetting offsets in place.

## Cleanup

Ordinary cleanup is only:

```text
scripts/lab stop
```

Destruction requires the exact confirmation argument and is limited to the named initial lab scope:

```text
scripts/lab destroy g001 --confirm g001
```

Do not use `docker compose down --volumes`, `docker volume prune`, `docker system prune`, or manual deletion of unrelated resources. The destroy command intentionally refuses to remove shared infrastructure when a successor generation or recovery manifest exists.
