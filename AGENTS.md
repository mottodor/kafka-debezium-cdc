# Agent guide

These instructions apply to the whole repository.

## Source of truth

1. Read `docs/spec.md` first. It is the approved behavioral contract and defines scope, invariants, versions, and AC-001–AC-014.
2. Read `docs/bootstrap.md` for the approved implementation sequence, proposed layout, command interfaces, and validation strategy.
3. Use `docs/idea.md` only as the original project brief. Do not change `docs/spec.md` or `docs/idea.md` without an explicit contract decision.
4. Do not duplicate the full specification in code comments or new documentation; link to the relevant section instead.

## Planned command interface

The following interfaces are proposed by `docs/bootstrap.md`; verify that a command exists before invoking or documenting it as available:

```text
scripts/lab preflight
scripts/lab pull
scripts/lab up
scripts/lab bootstrap g001
scripts/lab status g001
scripts/lab lag g001
scripts/lab stop
scripts/lab new-generation g002
scripts/lab destroy g001 --confirm g001

scripts/check unit
scripts/check smoke
scripts/check acceptance AC-001
scripts/check acceptance all
```

Prefer these project interfaces once implemented. A successful process start, empty test selection, or Compose parse alone is not acceptance evidence.

## Implementation boundaries

- Keep the main path container-first with Docker Compose v2 and exact versions/digests from `docs/spec.md`; do not introduce floating `latest` tags or unbounded runtime dependencies.
- Do not change delete semantics, ordering tokens, durable-commit boundaries, generation recovery, retention limits, or expected AC results without explicit approval.
- Preserve the at-least-once design: commit Kafka offsets only after the durable SQLite transaction; do not claim exactly-once.
- Keep generations isolated. Never reuse or overwrite an old connector, slot, topic namespace, consumer group, or SQLite sink.
- Do not add project-level Pi settings, extensions, MCP servers, orchestration frameworks, or CI unless a separate approved task requires them.
- Keep secrets and generated runtime data out of Git. Do not print credentials or persist private environment dumps as evidence.

## Network and destructive actions

- Treat image pulls, dependency downloads, builds requiring network access, container startup, and external API calls as explicit actions; obtain the required authorization before running them.
- `preflight` must be read-only. Ordinary stop/restart must preserve PostgreSQL/Kafka volumes, connector offsets, replication slots, and SQLite sinks.
- Never run broad cleanup, `docker compose down --volumes`, volume deletion, `git reset --hard`, `git clean`, or force operations by default.
- Destruction must be limited to the named lab generation, require the exact confirmation argument, and must not touch unrelated containers, networks, volumes, caches, or source data.
- Before acceptance or load probes, verify the resource and disk guards in `docs/spec.md`; stop on version/digest mismatch, unsafe isolation, or insufficient capacity.

## Verification

Use the smallest relevant checks first, then the affected integration/acceptance command. Assertions must inspect real data, offsets, connector state, and failure behavior; skipped or unavailable checks are not passes. Keep evidence redacted and generation-scoped under the runtime path defined by the implementation.
