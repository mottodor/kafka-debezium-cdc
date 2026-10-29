#!/usr/bin/env python3
"""Register the generation-scoped Debezium connector and verify RUNNING state."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, NoReturn
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener


HTTP_OPENER = build_opener(ProxyHandler({}))


def fail(message: str, code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        fail(f"required environment variable is unset: {name}", 64)
    return value


def redact(value: Any) -> Any:
    secret = os.environ.get("POSTGRES_CONNECTOR_PASSWORD", "")
    if isinstance(value, dict):
        return {
            key: "<redacted>" if key == "database.password" else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str) and secret:
        return value.replace(secret, "<redacted>")
    return value


def request_json(method: str, url: str, payload: Any | None = None) -> tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=data, headers=headers, method=method)
    try:
        with HTTP_OPENER.open(request, timeout=10) as response:
            body = response.read()
            return response.status, json.loads(body) if body else None
    except HTTPError as error:
        body = error.read()
        try:
            decoded: Any = json.loads(body) if body else None
        except json.JSONDecodeError:
            decoded = body.decode("utf-8", errors="replace")
        return error.code, decoded
    except URLError as error:
        fail(f"Kafka Connect request failed: {error.reason}")


def load_expected_config(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read connector config {path}: {error}", 66)

    if not isinstance(payload, dict) or not isinstance(payload.get("config"), dict):
        fail(f"connector config has unexpected structure: {path}", 65)
    return payload


def verify_generation(payload: dict[str, Any]) -> None:
    config = payload["config"]
    schema = required_env("POSTGRES_SCHEMA")
    expected = {
        "name": required_env("DEBEZIUM_CONNECTOR_NAME"),
        "database.dbname": required_env("POSTGRES_DATABASE"),
        "database.user": required_env("POSTGRES_CONNECTOR_USER"),
        "database.password": "${env:POSTGRES_CONNECTOR_PASSWORD}",
        "slot.name": required_env("POSTGRES_REPLICATION_SLOT"),
        "publication.name": required_env("POSTGRES_PUBLICATION"),
        "topic.prefix": required_env("KAFKA_TOPIC_PREFIX"),
        "table.include.list": ",".join(
            f"{schema}.{table}" for table in ("customers", "orders", "payments")
        ),
    }

    if payload.get("name") != expected.pop("name"):
        fail("connector name does not match generation environment", 65)
    mismatches = {
        key: {"expected": value, "actual": config.get(key)}
        for key, value in expected.items()
        if config.get(key) != value
    }
    if mismatches:
        fail(f"connector config does not match generation environment: {redact(mismatches)}", 65)


def verify_registered_config(
    actual: Any, expected: dict[str, Any], connector_name: str
) -> None:
    if not isinstance(actual, dict):
        fail("registered connector config is not a JSON object")
    if "database.password" not in actual:
        fail("registered connector config is missing database.password")
    if actual["database.password"] != expected.get("database.password"):
        fail("registered connector config has unexpected database.password reference")

    actual_without_password = dict(actual)
    expected_without_password = dict(expected)
    actual_without_password.pop("database.password", None)
    expected_without_password.pop("database.password", None)
    returned_name = actual_without_password.pop("name", connector_name)
    if returned_name != connector_name:
        fail(f"registered connector returned unexpected name: {returned_name}")
    if actual_without_password != expected_without_password:
        print("registered connector config differs from expected config", file=sys.stderr)
        print(json.dumps(redact(actual_without_password), indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)


def status_summary(status: Any) -> dict[str, Any]:
    if not isinstance(status, dict):
        return {"response": status}
    connector = status.get("connector") or {}
    tasks = status.get("tasks") or []
    return {
        "name": status.get("name"),
        "connector": {
            "state": connector.get("state"),
            "worker_id": connector.get("worker_id"),
        },
        "tasks": [
            {
                "id": task.get("id"),
                "state": task.get("state"),
                "worker_id": task.get("worker_id"),
                **({"trace": task.get("trace")} if task.get("trace") else {}),
            }
            for task in tasks
            if isinstance(task, dict)
        ],
    }


def is_running(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    connector = status.get("connector") or {}
    tasks = status.get("tasks") or []
    return (
        connector.get("state") == "RUNNING"
        and len(tasks) == 1
        and tasks[0].get("id") == 0
        and tasks[0].get("state") == "RUNNING"
    )


def is_failed(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    connector = status.get("connector") or {}
    tasks = status.get("tasks") or []
    return connector.get("state") == "FAILED" or any(
        isinstance(task, dict) and task.get("state") == "FAILED" for task in tasks
    )


def main() -> None:
    connect_url = os.environ.get("KAFKA_CONNECT_URL", "http://127.0.0.1:8083").rstrip("/")
    connector_file = Path(
        os.environ.get(
            "CONNECTOR_CONFIG_FILE", "/opt/cdc/connect/g001/postgres-cdc.json"
        )
    )
    try:
        max_checks = int(os.environ.get("CONNECTOR_STATUS_MAX_CHECKS", "60"))
        interval = float(os.environ.get("CONNECTOR_STATUS_INTERVAL_SECONDS", "2"))
    except ValueError:
        fail("connector status retry settings must be numeric", 64)
    if max_checks < 1 or interval < 0:
        fail("connector status retry settings are outside the allowed range", 64)

    payload = load_expected_config(connector_file)
    verify_generation(payload)
    connector_name = payload["name"]
    expected_config = payload["config"]
    encoded_name = quote(connector_name, safe="")
    config_url = f"{connect_url}/connectors/{encoded_name}/config"
    status_url = f"{connect_url}/connectors/{encoded_name}/status"

    code, response = request_json("GET", config_url)
    if code == 200:
        verify_registered_config(response, expected_config, connector_name)
        print(f"connector already registered with matching config: {connector_name}")
    elif code == 404:
        code, response = request_json("POST", f"{connect_url}/connectors", payload)
        if code != 201:
            fail(
                "connector registration failed with HTTP "
                f"{code}: {json.dumps(redact(response), sort_keys=True)}"
            )
        print(f"registered connector: {connector_name}")
    else:
        fail(
            "connector config lookup failed with HTTP "
            f"{code}: {json.dumps(redact(response), sort_keys=True)}"
        )

    code, response = request_json("GET", config_url)
    if code != 200:
        fail(
            "connector config verification failed with HTTP "
            f"{code}: {json.dumps(redact(response), sort_keys=True)}"
        )
    verify_registered_config(response, expected_config, connector_name)

    last_status: Any = None
    for _ in range(max_checks):
        code, last_status = request_json("GET", status_url)
        if code == 200 and is_running(last_status):
            print(json.dumps(status_summary(last_status), indent=2, sort_keys=True))
            return
        if code == 200 and is_failed(last_status):
            fail(
                "connector or task entered FAILED state: "
                f"{json.dumps(redact(status_summary(last_status)), sort_keys=True)}"
            )
        time.sleep(interval)

    fail(
        f"connector did not reach RUNNING state after {max_checks} checks: "
        f"{json.dumps(redact(status_summary(last_status)), sort_keys=True)}"
    )


if __name__ == "__main__":
    main()
