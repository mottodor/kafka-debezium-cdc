#!/usr/bin/env python3
"""Validate and atomically publish redacted acceptance evidence bundles."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ACCEPTANCE_MANIFEST = PROJECT_ROOT / "fixtures" / "expected" / "acceptance-evidence.json"
FORMAT_VERSION = 1
AC_ID = re.compile(r"^AC-0(?:0[1-9]|1[0-4])$")
GENERATION_ID = re.compile(r"^g[0-9]{3}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
SENSITIVE_KEY = re.compile(
    r"(?:password|passwd|secret|credential|authorization|token|auth[_-]?token|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|private[_-]?key|secret[_-]?key|access[_-]?key(?:[_-]?id)?|environment|env[_-]?dump|auths|creds[_-]?store|cred[_-]?helpers)",
    re.IGNORECASE,
)
ORDERING_TOKEN_KEYS = {"ordering_token", "source_position_token"}
EXACT_SENSITIVE_KEYS = {
    "_auth",
    "access",
    "auth",
    "auth_config",
    "authentication",
}
NAME_FIELDS = {"key", "name", "variable"}
VALUE_FIELDS = {"setting", "value"}
ENVIRONMENT_KEY = re.compile(r"^[A-Z_][A-Z0-9_]*$")
COMMON_ENVIRONMENT_KEYS = {
    "HOME",
    "HOSTNAME",
    "LANG",
    "PATH",
    "PWD",
    "SHELL",
    "TERM",
    "USER",
}
DOCKER_INSPECT_KEYS = {"Config", "HostConfig", "Mounts", "NetworkSettings"}
SENSITIVE_ASSIGNMENT = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?)\s*[:=]\s*\S+",
    re.IGNORECASE,
)
IDENTIFIER_ASSIGNMENT = re.compile(
    r"(?<![A-Za-z0-9_.])(?P<name>[A-Za-z_][A-Za-z0-9_.-]*)\s*[:=]\s*\S+"
)
CREDENTIAL_URL = re.compile(r"[a-z][a-z0-9+.-]*://[^/@\s:]+:[^/@\s]+@", re.IGNORECASE)
PRIVATE_KEY_MARKER = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
ENVIRONMENT_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")
FORBIDDEN_ARTIFACT_NAME = re.compile(
    r"(?:^|[-_.])(?:docker[-_]?inspect|full[-_]?environment|environment[-_]?dump|auth[-_]?config|credentials?|secrets?)(?:[-_.]|$)",
    re.IGNORECASE,
)
RESULTS = {"PASS", "FAIL", "NOT_RUN"}
REQUIRED_REDACTIONS = [
    "authentication_configuration",
    "credentials",
    "docker_inspect",
    "environment_values",
]
ALLOWED_ARTIFACT_MEDIA_TYPES = {"application/json", "text/csv", "text/plain"}
MAX_ARTIFACT_BYTES = 10 * 1024 * 1024
TOP_LEVEL_FIELDS = {
    "format_version",
    "ac_id",
    "generation",
    "result",
    "started_at",
    "finished_at",
    "command",
    "assertions",
    "artifacts",
    "redactions",
}
ASSERTION_FIELDS = {"name", "result", "summary"}
ARTIFACT_FIELDS = {"path", "sha256", "media_type"}


class EvidenceError(ValueError):
    """An evidence document or bundle is incomplete, inconsistent, or unsafe."""


def _require_exact_fields(
    value: Mapping[str, Any], expected: set[str], description: str
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise EvidenceError(
            f"{description} fields differ: missing={missing}, unexpected={unexpected}"
        )


def _parse_timestamp(value: Any, description: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise EvidenceError(f"{description} must be a non-empty ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise EvidenceError(f"{description} must be valid ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidenceError(f"{description} must include a UTC offset")
    return parsed


def _reject_sensitive_string(value: str, path: str) -> None:
    if (
        SENSITIVE_ASSIGNMENT.search(value)
        or CREDENTIAL_URL.search(value)
        or PRIVATE_KEY_MARKER.search(value)
    ):
        raise EvidenceError(f"{path} contains credential-bearing content")
    for assignment in IDENTIFIER_ASSIGNMENT.finditer(value):
        identifier = assignment.group("name")
        normalized = identifier.strip().lower().replace("-", "_")
        if normalized not in ORDERING_TOKEN_KEYS and _is_sensitive_identifier(identifier):
            raise EvidenceError(f"{path} contains credential-bearing content")


def _is_sensitive_identifier(value: str) -> bool:
    value = value.strip()
    normalized = value.lower().replace("-", "_")
    return (
        normalized in EXACT_SENSITIVE_KEYS
        or SENSITIVE_KEY.search(value) is not None
        or (ENVIRONMENT_KEY.fullmatch(value) is not None and "_" in value)
    )


def _reject_name_value_pair(identifier: Any, setting: Any, path: str) -> None:
    if not isinstance(identifier, str) or setting in (None, ""):
        return
    normalized = identifier.strip().lower().replace("-", "_")
    if normalized in ORDERING_TOKEN_KEYS:
        return
    if _is_sensitive_identifier(identifier):
        raise EvidenceError(f"{path} contains credential-bearing key/value content")


def _reject_sensitive_content(value: Any, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise EvidenceError(f"{path} contains a non-string object key")
        normalized_fields = {key.lower().replace("-", "_"): key for key in value}
        name_field = next((normalized_fields[name] for name in NAME_FIELDS if name in normalized_fields), None)
        value_field = next((normalized_fields[name] for name in VALUE_FIELDS if name in normalized_fields), None)
        if name_field is not None and value_field is not None:
            _reject_name_value_pair(value[name_field], value[value_field], path)
        keys = set(value)
        environment_keys = COMMON_ENVIRONMENT_KEYS & keys
        variable_shaped_keys = {
            key for key in keys if ENVIRONMENT_KEY.fullmatch(key) and "_" in key
        }
        if len(environment_keys) >= 3 or len(variable_shaped_keys) >= 2:
            raise EvidenceError(f"{path} contains an environment-shaped object")
        if DOCKER_INSPECT_KEYS & keys:
            raise EvidenceError(f"{path} contains Docker inspect output")
        for key, nested in value.items():
            normalized_key = key.lower().replace("-", "_")
            if normalized_key not in ORDERING_TOKEN_KEYS and _is_sensitive_identifier(key):
                raise EvidenceError(f"{path} contains forbidden sensitive field {key!r}")
            _reject_sensitive_content(nested, f"{path}.{key}")
    elif isinstance(value, list):
        environment_values = [
            item
            for item in value
            if isinstance(item, str) and ENVIRONMENT_ASSIGNMENT.fullmatch(item)
        ]
        if len(environment_values) >= 3:
            raise EvidenceError(f"{path} contains an environment dump")
        for index, nested in enumerate(value):
            _reject_sensitive_content(nested, f"{path}[{index}]")
    elif isinstance(value, str):
        _reject_sensitive_string(value, path)


def _load_required_assertions(
    manifest_path: str | Path = ACCEPTANCE_MANIFEST,
) -> dict[str, list[str]]:
    source = Path(manifest_path)
    try:
        manifest = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read acceptance evidence manifest: {source}") from error
    if not isinstance(manifest, Mapping) or set(manifest) != {
        "format_version",
        "source",
        "criteria",
    }:
        raise EvidenceError("acceptance evidence manifest has an invalid shape")
    if manifest["format_version"] != FORMAT_VERSION:
        raise EvidenceError("acceptance evidence manifest version is incompatible")
    criteria = manifest["criteria"]
    if not isinstance(criteria, list):
        raise EvidenceError("acceptance evidence criteria must be an array")
    required: dict[str, list[str]] = {}
    for index, criterion in enumerate(criteria):
        if not isinstance(criterion, Mapping) or set(criterion) != {
            "ac_id",
            "required_assertions",
        }:
            raise EvidenceError(f"acceptance criterion {index} has an invalid shape")
        ac_id = criterion["ac_id"]
        names = criterion["required_assertions"]
        if not isinstance(ac_id, str) or AC_ID.fullmatch(ac_id) is None:
            raise EvidenceError(f"acceptance criterion {index} has an invalid AC ID")
        if ac_id in required:
            raise EvidenceError(f"duplicate acceptance criterion: {ac_id}")
        if (
            not isinstance(names, list)
            or not names
            or len(names) != len(set(names))
            or any(
                not isinstance(name, str)
                or re.fullmatch(r"[a-z][a-z0-9_]*", name) is None
                for name in names
            )
        ):
            raise EvidenceError(f"acceptance criterion {ac_id} assertions are invalid")
        required[ac_id] = list(names)
    expected_ids = {f"AC-{number:03d}" for number in range(1, 15)}
    if set(required) != expected_ids:
        raise EvidenceError("acceptance evidence manifest must cover AC-001 through AC-014")
    return required


def _validate_command(value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise EvidenceError("command must be a non-empty argv array")
    for index, argument in enumerate(value):
        if not isinstance(argument, str) or not argument:
            raise EvidenceError(f"command[{index}] must be a non-empty string")
        if "\x00" in argument or "\n" in argument or "\r" in argument:
            raise EvidenceError(f"command[{index}] contains a control separator")
        lowered = argument.lower()
        if lowered == ".env" or lowered.endswith("/.env"):
            raise EvidenceError("command must not expose a secrets file path")
        if ENVIRONMENT_ASSIGNMENT.fullmatch(argument):
            raise EvidenceError("command must not embed environment values")
        _reject_sensitive_string(argument, f"command[{index}]")
    for index, argument in enumerate(value[:-1]):
        identifier = argument.lstrip("-")
        normalized = identifier.lower().replace("-", "_")
        if normalized not in ORDERING_TOKEN_KEYS and _is_sensitive_identifier(identifier):
            raise EvidenceError(
                f"command[{index}:{index + 2}] contains a credential-bearing argument pair"
            )


def _validate_assertions(value: Any, required_names: list[str]) -> list[str]:
    if not isinstance(value, list) or not value:
        raise EvidenceError("assertions must be a non-empty array")
    names: list[str] = []
    results: list[str] = []
    for index, raw_assertion in enumerate(value):
        if not isinstance(raw_assertion, Mapping):
            raise EvidenceError(f"assertions[{index}] must be an object")
        _require_exact_fields(raw_assertion, ASSERTION_FIELDS, f"assertions[{index}]")
        name = raw_assertion["name"]
        result = raw_assertion["result"]
        summary = raw_assertion["summary"]
        if not isinstance(name, str) or re.fullmatch(r"[a-z][a-z0-9_]*", name) is None:
            raise EvidenceError(f"assertions[{index}].name is invalid")
        if name in names:
            raise EvidenceError(f"duplicate assertion name: {name}")
        if result not in RESULTS:
            raise EvidenceError(f"assertions[{index}].result is invalid")
        if not isinstance(summary, str) or not summary.strip():
            raise EvidenceError(f"assertions[{index}].summary must be non-empty")
        if len(summary) > 500:
            raise EvidenceError(f"assertions[{index}].summary is too long")
        _reject_sensitive_string(summary, f"assertions[{index}].summary")
        names.append(name)
        results.append(result)
    if names != required_names:
        raise EvidenceError(
            f"assertion names must exactly match the AC manifest: expected={required_names}, actual={names}"
        )
    return results


def _validate_artifacts(value: Any) -> None:
    if not isinstance(value, list):
        raise EvidenceError("artifacts must be an array")
    paths: set[str] = set()
    for index, raw_artifact in enumerate(value):
        if not isinstance(raw_artifact, Mapping):
            raise EvidenceError(f"artifacts[{index}] must be an object")
        _require_exact_fields(raw_artifact, ARTIFACT_FIELDS, f"artifacts[{index}]")
        path = raw_artifact["path"]
        digest = raw_artifact["sha256"]
        media_type = raw_artifact["media_type"]
        if not isinstance(path, str) or not path:
            raise EvidenceError(f"artifacts[{index}].path must be non-empty")
        pure_path = PurePosixPath(path)
        if pure_path.is_absolute() or ".." in pure_path.parts or path in paths:
            raise EvidenceError(f"artifacts[{index}].path must be unique and relative")
        if any(part == ".env" or FORBIDDEN_ARTIFACT_NAME.search(part) for part in pure_path.parts):
            raise EvidenceError(f"artifacts[{index}].path is a prohibited evidence class")
        if not isinstance(digest, str) or SHA256_HEX.fullmatch(digest) is None:
            raise EvidenceError(f"artifacts[{index}].sha256 is invalid")
        if media_type not in ALLOWED_ARTIFACT_MEDIA_TYPES:
            raise EvidenceError(f"artifacts[{index}].media_type is not allowed")
        paths.add(path)


def validate_evidence(document: Any) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise EvidenceError("evidence must be a JSON object")
    _reject_sensitive_content(document)
    _require_exact_fields(document, TOP_LEVEL_FIELDS, "evidence")
    if document["format_version"] != FORMAT_VERSION:
        raise EvidenceError(f"format_version must be {FORMAT_VERSION}")
    ac_id = document["ac_id"]
    if not isinstance(ac_id, str) or AC_ID.fullmatch(ac_id) is None:
        raise EvidenceError("ac_id must be AC-001 through AC-014")
    if (
        not isinstance(document["generation"], str)
        or GENERATION_ID.fullmatch(document["generation"]) is None
    ):
        raise EvidenceError("generation must match gNNN")
    result = document["result"]
    if result not in RESULTS:
        raise EvidenceError("result must be PASS, FAIL, or NOT_RUN")
    started = _parse_timestamp(document["started_at"], "started_at")
    finished = _parse_timestamp(document["finished_at"], "finished_at")
    if finished < started:
        raise EvidenceError("finished_at must not precede started_at")
    _validate_command(document["command"])
    required_assertions = _load_required_assertions()[ac_id]
    assertion_results = _validate_assertions(document["assertions"], required_assertions)
    _validate_artifacts(document["artifacts"])
    if document["redactions"] != REQUIRED_REDACTIONS:
        raise EvidenceError(
            f"redactions must exactly match format-version {FORMAT_VERSION} categories"
        )
    if result == "PASS" and any(item != "PASS" for item in assertion_results):
        raise EvidenceError("PASS evidence requires every assertion to PASS")
    if result == "FAIL" and "FAIL" not in assertion_results:
        raise EvidenceError("FAIL evidence requires at least one failed assertion")
    if result == "NOT_RUN" and (
        "FAIL" in assertion_results or "NOT_RUN" not in assertion_results
    ):
        raise EvidenceError(
            "NOT_RUN evidence requires at least one NOT_RUN and no failed assertion"
        )
    return dict(document)


def load_evidence(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceError(f"cannot read evidence JSON: {source}") from error
    return validate_evidence(document)


def evidence_run_directory(
    runtime_root: str | Path, document: Mapping[str, Any]
) -> Path:
    validated = validate_evidence(document)
    started = _parse_timestamp(validated["started_at"], "started_at")
    run_id = started.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return (
        Path(runtime_root)
        / "evidence"
        / validated["generation"]
        / validated["ac_id"]
        / run_id
    )


def _reject_sensitive_csv(
    text: str, description: str, *, require_valid: bool
) -> None:
    try:
        rows = csv.reader(io.StringIO(text), strict=True)
        header = next(rows, None)
        if header is None:
            return
        normalized_fields = {
            field.strip().lower().replace("-", "_"): index
            for index, field in enumerate(header)
        }
        name_index = next(
            (normalized_fields[name] for name in NAME_FIELDS if name in normalized_fields),
            None,
        )
        value_index = next(
            (normalized_fields[name] for name in VALUE_FIELDS if name in normalized_fields),
            None,
        )
        sensitive_columns = [
            index
            for index, field in enumerate(header)
            if field.strip().lower().replace("-", "_") not in ORDERING_TOKEN_KEYS
            and _is_sensitive_identifier(field)
        ]
        for row_number, row in enumerate(rows, start=2):
            for column_index in sensitive_columns:
                if column_index < len(row) and row[column_index].strip():
                    raise EvidenceError(
                        f"{description} row {row_number} contains a nonempty sensitive column"
                    )
            if (
                name_index is not None
                and value_index is not None
                and max(name_index, value_index) < len(row)
            ):
                _reject_name_value_pair(
                    row[name_index],
                    row[value_index],
                    f"{description} row {row_number}",
                )
    except csv.Error as error:
        if require_valid:
            raise EvidenceError(f"{description} is not valid CSV") from error


def _validate_artifact_content(content: bytes, media_type: str, description: str) -> None:
    if len(content) > MAX_ARTIFACT_BYTES:
        raise EvidenceError(f"{description} exceeds the {MAX_ARTIFACT_BYTES}-byte limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise EvidenceError(f"{description} must be UTF-8 text") from error
    parsed_json: Any | None = None
    try:
        parsed_json = json.loads(text)
    except json.JSONDecodeError as error:
        if media_type == "application/json":
            raise EvidenceError(f"{description} is not valid JSON") from error
    if parsed_json is not None:
        _reject_sensitive_content(parsed_json, description)
    _reject_sensitive_string(text, description)
    _reject_sensitive_csv(
        text, description, require_valid=media_type == "text/csv"
    )
    environment_lines = [
        line for line in text.splitlines() if ENVIRONMENT_ASSIGNMENT.fullmatch(line)
    ]
    if len(environment_lines) >= 3:
        raise EvidenceError(f"{description} contains an environment dump")


def _copy_verified_artifact(
    artifact: Mapping[str, Any], artifact_root: Path, staging_directory: Path
) -> None:
    relative = PurePosixPath(artifact["path"])
    source_root = artifact_root.resolve()
    source = (source_root / Path(*relative.parts)).resolve()
    try:
        source.relative_to(source_root)
    except ValueError as error:
        raise EvidenceError(f"artifact escapes artifact root: {relative}") from error
    candidate = source_root
    for part in relative.parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise EvidenceError(f"artifact path must not contain symlinks: {relative}")
    if not source.is_file():
        raise EvidenceError(f"artifact is missing or not a regular file: {relative}")
    content = source.read_bytes()
    _validate_artifact_content(content, artifact["media_type"], f"artifact {relative}")
    actual_digest = hashlib.sha256(content).hexdigest()
    if actual_digest != artifact["sha256"]:
        raise EvidenceError(f"artifact checksum mismatch: {relative}")
    target = staging_directory / Path(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_evidence(
    document: Mapping[str, Any],
    runtime_root: str | Path = ".runtime",
    *,
    artifact_root: str | Path | None = None,
) -> Path:
    validated = validate_evidence(document)
    artifacts = validated["artifacts"]
    if artifacts and artifact_root is None:
        raise EvidenceError("artifact_root is required when evidence lists artifacts")
    run_directory = evidence_run_directory(runtime_root, validated)
    parent = run_directory.parent
    parent.mkdir(parents=True, exist_ok=True)
    if run_directory.exists():
        raise FileExistsError(run_directory)
    staging = parent / f".{run_directory.name}.staging-{uuid.uuid4().hex}"
    staging.mkdir()
    published = False
    try:
        if artifacts:
            source_root = Path(artifact_root)  # type: ignore[arg-type]
            for artifact in artifacts:
                _copy_verified_artifact(artifact, source_root, staging)
        output = staging / "index.json"
        payload = json.dumps(validated, indent=2, sort_keys=True) + "\n"
        with output.open("x", encoding="utf-8") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        _fsync_directory(staging)
        staging.replace(run_directory)
        published = True
        _fsync_directory(parent)
    except Exception:
        if published:
            shutil.rmtree(run_directory, ignore_errors=True)
            try:
                _fsync_directory(parent)
            except OSError:
                pass
        else:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    return run_directory / "index.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path")
    write_parser = subparsers.add_parser("write")
    write_parser.add_argument("path")
    write_parser.add_argument("--runtime-root", default=".runtime")
    write_parser.add_argument("--artifact-root")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        document = load_evidence(args.path)
        if args.command == "validate":
            print(
                json.dumps(
                    {
                        "ac_id": document["ac_id"],
                        "generation": document["generation"],
                        "result": document["result"],
                        "valid": True,
                    },
                    sort_keys=True,
                )
            )
        else:
            print(
                write_evidence(
                    document,
                    args.runtime_root,
                    artifact_root=args.artifact_root,
                )
            )
    except (EvidenceError, OSError) as error:
        raise SystemExit(f"evidence validation failed: {error}") from error


if __name__ == "__main__":
    main()
