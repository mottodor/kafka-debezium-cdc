"""Apply and verify versioned SQLite schema migrations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Sequence

MIGRATION_PACKAGE = "cdc_consumer.sql"
MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")
GENERATION_ID = re.compile(r"^g[0-9]{3}$")
LOGICAL_NAME = re.compile(r"^[a-z0-9_]+$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


class MigrationError(RuntimeError):
    """The SQLite schema cannot be migrated safely."""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum_sha256: str


def _parse_integer(value: object, description: str) -> int:
    if isinstance(value, bool):
        raise MigrationError(f"{description} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 10)
        except ValueError as error:
            raise MigrationError(f"{description} must be an integer") from error
    raise MigrationError(f"{description} must be an integer")


def load_migrations() -> tuple[Migration, ...]:
    migrations: list[Migration] = []
    for entry in resources.files(MIGRATION_PACKAGE).iterdir():
        match = MIGRATION_NAME.fullmatch(entry.name)
        if match is None:
            continue
        sql = entry.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=_parse_integer(match.group("version"), "migration version"),
                name=match.group("name"),
                sql=sql,
                checksum_sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    migrations.sort(key=lambda migration: migration.version)
    _validate_migration_set(migrations)
    return tuple(migrations)


def open_database(database_path: str | Path) -> sqlite3.Connection:
    path = Path(database_path)
    if path.exists() and not path.is_file():
        raise MigrationError(f"SQLite path is not a regular file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            raise MigrationError(
                f"SQLite database did not enter WAL journal mode: {journal_mode!r}"
            )
        connection.execute("PRAGMA synchronous = FULL")
        return connection
    except Exception:
        connection.close()
        raise


def _applied_migrations(connection: sqlite3.Connection) -> dict[int, tuple[str, str]]:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if table_exists is None:
        return {}
    try:
        rows = connection.execute(
            "SELECT version, name, checksum_sha256 FROM schema_migrations ORDER BY version"
        ).fetchall()
    except sqlite3.DatabaseError as error:
        raise MigrationError("schema_migrations has an incompatible schema") from error
    return {
        _parse_integer(version, "applied migration version"): (
            str(name),
            str(checksum),
        )
        for version, name, checksum in rows
    }


def _validate_migration_set(migrations: Sequence[Migration]) -> None:
    expected_versions = list(range(1, len(migrations) + 1))
    actual_versions = [migration.version for migration in migrations]
    if not migrations or actual_versions != expected_versions:
        raise MigrationError(
            "SQLite migration set must be contiguous and start at version 0001"
        )
    if len({migration.name for migration in migrations}) != len(migrations):
        raise MigrationError("SQLite migration names must be unique")
    for migration in migrations:
        if LOGICAL_NAME.fullmatch(migration.name) is None:
            raise MigrationError(
                f"SQLite migration {migration.version:04d} has an invalid name"
            )
        if SHA256_HEX.fullmatch(migration.checksum_sha256) is None:
            raise MigrationError(
                f"SQLite migration {migration.version:04d} has an invalid checksum"
            )


def _validate_applied(
    applied: dict[int, tuple[str, str]], migrations: Sequence[Migration]
) -> None:
    known = {migration.version: migration for migration in migrations}
    for version, (name, checksum) in applied.items():
        migration = known.get(version)
        if migration is None:
            raise MigrationError(
                f"database has unknown SQLite migration version {version:04d}"
            )
        if name != migration.name or checksum != migration.checksum_sha256:
            raise MigrationError(
                f"SQLite migration {version:04d} metadata does not match source"
            )
    applied_versions = sorted(applied)
    if applied_versions != list(range(1, len(applied_versions) + 1)):
        raise MigrationError(
            f"database has non-contiguous migration history: {applied_versions}"
        )


def _validate_generation(generation: str) -> None:
    if GENERATION_ID.fullmatch(generation) is None:
        raise MigrationError(f"generation must match gNNN, got {generation!r}")


def _existing_generation(connection: sqlite3.Connection) -> str | None:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sink_metadata'"
    ).fetchone()
    if table_exists is None:
        return None
    try:
        rows = connection.execute(
            "SELECT singleton, generation FROM sink_metadata"
        ).fetchall()
    except sqlite3.DatabaseError as error:
        raise MigrationError("sink_metadata has an incompatible schema") from error
    if not rows:
        return None
    if len(rows) != 1 or rows[0][0] != 1 or not isinstance(rows[0][1], str):
        raise MigrationError("sink_metadata must contain one valid singleton row")
    return rows[0][1]


def _split_sql_statements(sql: str) -> tuple[str, ...]:
    statements: list[str] = []
    buffer: list[str] = []
    for character in sql:
        buffer.append(character)
        if character == ";" and sqlite3.complete_statement("".join(buffer)):
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer.clear()
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return tuple(statements)


def _deny_transaction_control(
    action_code: int,
    _parameter_one: str | None,
    _parameter_two: str | None,
    _database_name: str | None,
    _trigger_name: str | None,
) -> int:
    if action_code in {sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT}:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _apply_one(
    connection: sqlite3.Connection,
    migration: Migration,
    initial_generation: str | None,
) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.set_authorizer(_deny_transaction_control)
        for statement in _split_sql_statements(migration.sql):
            connection.execute(statement)
        connection.set_authorizer(None)
        if initial_generation is not None:
            connection.execute(
                "INSERT INTO sink_metadata (singleton, generation, created_at) "
                "VALUES (1, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                (initial_generation,),
            )
        connection.execute(
            "INSERT INTO schema_migrations "
            "(version, name, checksum_sha256, applied_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            (migration.version, migration.name, migration.checksum_sha256),
        )
        connection.commit()
    except sqlite3.DatabaseError as error:
        connection.set_authorizer(None)
        if connection.in_transaction:
            connection.rollback()
        raise MigrationError(
            f"failed to apply SQLite migration {migration.version:04d}_{migration.name}"
        ) from error
    finally:
        connection.set_authorizer(None)


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
    generation: str,
    *,
    target_version: int | None = None,
) -> int:
    migrations = tuple(migrations)
    _validate_migration_set(migrations)
    _validate_generation(generation)
    if target_version is None:
        target_version = migrations[-1].version
    target_version = _parse_integer(target_version, "target migration version")
    if not 1 <= target_version <= migrations[-1].version:
        raise MigrationError(
            f"target migration version must be between 1 and {migrations[-1].version}"
        )
    applied = _applied_migrations(connection)
    _validate_applied(applied, migrations)
    existing_generation = _existing_generation(connection)
    if applied and existing_generation is None:
        raise MigrationError("migrated SQLite sink has no generation binding")
    if existing_generation is not None and existing_generation != generation:
        raise MigrationError(
            "SQLite sink belongs to a different generation: "
            f"expected {generation}, found {existing_generation}"
        )
    for migration in migrations:
        if migration.version <= target_version and migration.version not in applied:
            _apply_one(
                connection,
                migration,
                initial_generation=generation if migration.version == 1 else None,
            )
    if _existing_generation(connection) != generation:
        raise MigrationError("SQLite sink generation binding was not established")
    applied_after = _applied_migrations(connection)
    return max(applied_after)


def migrate(
    database_path: str | Path,
    generation: str,
    *,
    target_version: int = 1,
) -> int:
    migrations = load_migrations()
    connection = open_database(database_path)
    try:
        return apply_migrations(
            connection,
            migrations,
            generation,
            target_version=target_version,
        )
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="SQLite sink path")
    parser.add_argument("--generation", required=True, help="CDC generation ID (gNNN)")
    parser.add_argument(
        "--target-version",
        type=int,
        default=1,
        help="highest migration version to apply (default: 1)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        version = migrate(
            args.database,
            args.generation,
            target_version=args.target_version,
        )
    except (MigrationError, sqlite3.DatabaseError, OSError) as error:
        raise SystemExit(f"SQLite migration failed: {error}") from error
    print(
        json.dumps(
            {
                "database": str(Path(args.database)),
                "generation": args.generation,
                "schema_version": version,
                "target_version": args.target_version,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
