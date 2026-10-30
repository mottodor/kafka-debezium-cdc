from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = PROJECT_ROOT / "tests" / "acceptance" / "run_acceptance.py"
SPEC = importlib.util.spec_from_file_location("run_acceptance", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load acceptance runner")
acceptance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(acceptance)


class AcceptanceRunnerTests(unittest.TestCase):
    def test_probe_registry_exactly_covers_manifest(self) -> None:
        required = acceptance.manifest_assertions()
        self.assertEqual(list(acceptance.PROBES), sorted(required))
        for ac_id, names in required.items():
            self.assertTrue(names, ac_id)
            self.assertEqual(len(names), len(set(names)), ac_id)

    def test_active_generation_uses_latest_active_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for generation, state in (
                ("g002", "FAILED"),
                ("g003", "FAILED"),
                ("g004", "IN_PROGRESS"),
                ("g005", "ACTIVE"),
            ):
                directory = root / "generations" / generation
                directory.mkdir(parents=True)
                (directory / "manifest.json").write_text(
                    json.dumps({"state": state}), encoding="utf-8"
                )
            self.assertEqual(acceptance.detect_active_generation(root), "g005")

    def test_active_generation_rejects_multiple_active_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for generation in ("g002", "g003"):
                directory = root / "generations" / generation
                directory.mkdir(parents=True)
                (directory / "manifest.json").write_text(
                    json.dumps({"state": "ACTIVE"}), encoding="utf-8"
                )
            with self.assertRaisesRegex(acceptance.AcceptanceError, "multiple ACTIVE"):
                acceptance.detect_active_generation(root)

    def test_runtime_state_rejects_running_inactive_consumer(self) -> None:
        states = {
            acceptance.POSTGRES_CONTAINER: "running",
            acceptance.KAFKA_CONTAINER: "running",
            acceptance.CONNECT_CONTAINER: "running",
            acceptance.consumer_container("g002"): "running",
            "cdc-lab-g001-consumer-1": "running",
        }
        with (
            mock.patch.object(
                acceptance, "docker_state", side_effect=lambda name: states.get(name, "missing")
            ),
            mock.patch.object(
                acceptance,
                "labeled_generation_consumers",
                return_value={acceptance.consumer_container("g002"): "running"},
            ),
        ):
            with self.assertRaisesRegex(acceptance.AcceptanceError, "inactive"):
                acceptance.classify_runtime_state("g002")

    def test_prior_ac007_evidence_requires_valid_consistent_live_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            required = acceptance.manifest_assertions()["AC-007"]
            legacy = {name: f"legacy regression {name}" for name in required}
            acceptance.publish_result(
                "AC-007", "g002", acceptance.utc_now(), legacy, runtime_root=root
            )
            strong = {
                "pre_migration_fail_stop": "mode=live_transition coordinate=topic:1:2; current generation live transition",
                "offset_unchanged": "coordinate=topic:1:2 offset_before=2 offset_after_failure=2",
                "same_coordinate_replay": "coordinate=topic:1:2 replayed after migration versions=1,2",
                "phone_projection": "active sink phone values verified ids=[1, 2]",
                "offset_after_replay": "coordinate=topic:1:2 offset_after_replay=3; parity and zero lag passed",
            }
            with mock.patch.object(acceptance, "RUNTIME_ROOT", root):
                self.assertIsNone(acceptance.prior_live_transition_evidence("g002"))
                path = acceptance.publish_result(
                    "AC-007", "g002", acceptance.utc_now(), strong, runtime_root=root
                )
                selected = acceptance.prior_live_transition_evidence("g002")
                self.assertIsNotNone(selected)
                assert selected is not None
                self.assertEqual(selected[:2], (path, ("topic", 1, 2)))
                document = json.loads(path.read_text(encoding="utf-8"))
                for item in document["assertions"]:
                    if item["name"] == "offset_after_replay":
                        item["summary"] = item["summary"].replace("=3;", "=9;")
                path.write_text(json.dumps(document), encoding="utf-8")
                self.assertIsNone(acceptance.prior_live_transition_evidence("g002"))

    def test_ac007_restoration_migrates_before_restarting_running_runtime(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            mock.patch.object(
                acceptance, "docker_state", side_effect=["running", "exited"]
            ),
            mock.patch.object(acceptance, "source_has_phone", return_value=True),
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
            mock.patch.object(acceptance, "wait_generation_status") as status_mock,
            mock.patch.object(
                acceptance, "wait_complete_lag_inventory"
            ) as inventory_mock,
            mock.patch.object(acceptance, "wait_projection_parity") as parity_mock,
        ):
            acceptance.restore_ac007_runtime(
                "g002", acceptance.AcceptanceError("transition failed")
            )
        self.assertEqual(
            [call.args[0] for call in run_mock.call_args_list],
            [
                ["docker", "stop", acceptance.consumer_container("g002")],
                [str(acceptance.LAB), "migrate", "g002"],
                ["docker", "start", acceptance.consumer_container("g002")],
            ],
        )
        status_mock.assert_called_once_with("g002")
        inventory_mock.assert_called_once_with("g002")
        parity_mock.assert_called_once_with("g002")

    def test_ac007_restoration_restarts_without_migration_before_schema_change(
        self,
    ) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            mock.patch.object(
                acceptance, "docker_state", side_effect=["exited", "exited"]
            ),
            mock.patch.object(acceptance, "source_has_phone", return_value=False),
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
            mock.patch.object(acceptance, "wait_generation_status"),
            mock.patch.object(acceptance, "wait_complete_lag_inventory"),
            mock.patch.object(acceptance, "wait_projection_parity"),
        ):
            acceptance.restore_ac007_runtime(
                "g002", acceptance.AcceptanceError("transition failed")
            )
        self.assertEqual(
            [call.args[0] for call in run_mock.call_args_list],
            [["docker", "start", acceptance.consumer_container("g002")]],
        )

    def test_ac007_restoration_failure_forces_fully_stopped_state(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")

        def fake_run(
            argv: list[str], **_: object
        ) -> subprocess.CompletedProcess[str]:
            if argv == [str(acceptance.LAB), "migrate", "g002"]:
                raise acceptance.AcceptanceError("migration failed")
            if argv == [str(acceptance.LAB), "stop"]:
                return completed
            self.fail(f"unexpected restoration command: {argv!r}")

        with (
            mock.patch.object(acceptance, "docker_state", return_value="exited"),
            mock.patch.object(acceptance, "source_has_phone", return_value=True),
            mock.patch.object(acceptance, "run", side_effect=fake_run),
            mock.patch.object(
                acceptance, "classify_runtime_state", return_value="stopped"
            ) as state_mock,
        ):
            with self.assertRaisesRegex(
                acceptance.RuntimeRestorationError, "forced fully stopped"
            ):
                acceptance.restore_ac007_runtime(
                    "g002", acceptance.AcceptanceError("transition failed")
                )
        state_mock.assert_called_once_with("g002")

    def test_ac007_restoration_stop_timeout_is_runtime_restoration_error(self) -> None:
        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if argv == [str(acceptance.LAB), "migrate", "g002"]:
                raise acceptance.AcceptanceError("migration failed")
            if argv == [str(acceptance.LAB), "stop"]:
                raise subprocess.TimeoutExpired(argv, 120)
            self.fail(f"unexpected restoration command: {argv!r}")

        with (
            mock.patch.object(acceptance, "docker_state", return_value="exited"),
            mock.patch.object(acceptance, "source_has_phone", return_value=True),
            mock.patch.object(acceptance, "run", side_effect=fake_run),
        ):
            with self.assertRaisesRegex(
                acceptance.RuntimeRestorationError, "fallback=TimeoutExpired"
            ):
                acceptance.restore_ac007_runtime(
                    "g002", acceptance.AcceptanceError("transition failed")
                )

    def test_fault_cleanup_removes_one_off_before_restarting_official(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")
        states = {"fault": "running", "official": "exited"}
        with (
            mock.patch.object(
                acceptance, "docker_state", side_effect=lambda name: states[name]
            ),
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
        ):
            acceptance.cleanup_fault_consumer("fault", "official")
        self.assertEqual(
            [call.args[0] for call in run_mock.call_args_list],
            [["docker", "rm", "--force", "fault"], ["docker", "start", "official"]],
        )

    def test_ac002_requires_create_and_update_for_every_table(self) -> None:
        expected_targets = {
            ("customers", "3", "c"),
            ("orders", "103", "c"),
            ("payments", "1002", "c"),
            ("customers", "1", "u"),
            ("orders", "103", "u"),
            ("payments", "1002", "u"),
        }
        self.assertEqual(set(acceptance.AC002_TARGETS), expected_targets)
        before = {
            f"{table}:{entity_key}:{operation}": []
            for table, entity_key, operation in acceptance.AC002_TARGETS
        }
        topics = {
            "customers": "cdc.g002.public.customers",
            "orders": "cdc.g002.public.orders",
            "payments": "cdc.g002.public.payments",
        }
        variable_topics = {
            "KAFKA_CUSTOMERS_TOPIC": topics["customers"],
            "KAFKA_ORDERS_TOPIC": topics["orders"],
            "KAFKA_PAYMENTS_TOPIC": topics["payments"],
        }
        after = {
            f"{table}:{entity_key}:{operation}": [
                [topics[table], index % 3, index, "applied", 1]
            ]
            for index, (table, entity_key, operation) in enumerate(
                acceptance.AC002_TARGETS
            )
        }
        with mock.patch.object(
            acceptance,
            "generation_value",
            side_effect=lambda _generation, name: variable_topics[name],
        ):
            observed = acceptance.validate_ac002_records("g002", before, after)
            self.assertEqual(set(observed), set(before))
            for mutation in (
                lambda payload: payload["payments:1002:u"].clear(),
                lambda payload: payload["orders:103:u"].clear(),
                lambda payload: payload["orders:103:c"].append(
                    [topics["orders"], 2, 99, "applied", 1]
                ),
                lambda payload: payload["customers:1:u"].__setitem__(
                    0, [topics["customers"], 0, 3, "stale", 1]
                ),
            ):
                changed = json.loads(json.dumps(after))
                mutation(changed)
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.validate_ac002_records("g002", before, changed)

    def test_recovery_artifacts_reject_changed_sink_marker_and_history(self) -> None:
        checksums = {"cdc-g001.sqlite3": "a" * 64}
        manifest = {
            "state": "ACTIVE",
            "phase": "complete",
            "previous_generation": "g001",
            "new_generation": "g002",
            "old_connector": "postgres-cdc-g001",
            "new_connector": "postgres-cdc-g002",
            "old_slot": "postgres_cdc_g001",
            "new_slot": "postgres_cdc_g002",
            "old_topic_prefix": "cdc.g001",
            "new_topic_prefix": "cdc.g002",
            "old_group": "sqlite-projector-g001",
            "new_group": "sqlite-projector-g002",
            "old_sink_checksum_before": json.dumps(checksums),
            "old_sink_checksum_after": json.dumps(checksums),
            "updated_at": "2026-10-30T12:05:00+00:00",
        }
        marker = {
            "payload": {
                "active_successor": "g002",
                "archived_generation": "g001",
                "archived_at": "2026-10-30T12:00:00+00:00",
                "state": "ARCHIVED_READ_ONLY",
            },
            "sha256": "c" * 64,
        }
        history = {
            "snapshot": {"customers": 1, "orders": 1, "payments": 1},
            "streaming": {"customers": 1, "orders": 1, "payments": 1},
        }
        acceptance.validate_recovery_artifacts(manifest, checksums, marker, history)
        for bad_manifest, bad_checksums, bad_marker, bad_history in (
            (manifest, {"cdc-g001.sqlite3": "b" * 64}, marker, history),
            (manifest, checksums, {**marker, "payload": {**marker["payload"], "extra": True}}, history),
            (manifest, checksums, marker, {**history, "snapshot": {"customers": 1}}),
            ({**manifest, "unexpected": True}, checksums, marker, history),
            (manifest, checksums, {**marker, "sha256": "not-a-digest"}, history),
        ):
            with self.subTest(
                manifest=bad_manifest,
                checksums=bad_checksums,
                marker=bad_marker,
                history=bad_history,
            ):
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.validate_recovery_artifacts(
                        bad_manifest, bad_checksums, bad_marker, bad_history
                    )

    def test_recovery_checkpoint_requires_complete_timestamps_zero_errors_and_exact_lag(
        self,
    ) -> None:
        source = {
            "customers": [[1, "a@example.test", "Alice", "active", None, "1000000", "2000000"]],
            "orders": [[101, 1, "paid", "12.50", "1000000", "2000000"]],
            "payments": [[1001, 101, "captured", "12.50", "pay-1001", "1000000", "2000000"]],
        }
        lag = [
            {
                "topic": f"cdc.g002.public.{table}",
                "partition": partition,
                "current": "4",
                "end": "4",
                "lag": 0,
            }
            for table in ("customers", "orders", "payments")
            for partition in range(3)
        ]
        topic_values = {
            "KAFKA_CUSTOMERS_TOPIC": "cdc.g002.public.customers",
            "KAFKA_ORDERS_TOPIC": "cdc.g002.public.orders",
            "KAFKA_PAYMENTS_TOPIC": "cdc.g002.public.payments",
        }
        with mock.patch.object(
            acceptance,
            "generation_value",
            side_effect=lambda _generation, name: topic_values[name],
        ):
            checkpoint = acceptance.validate_recovery_checkpoint(
                "g002", source, source, 0, lag
            )
            self.assertEqual(checkpoint["ingestion_errors"], 0)
            self.assertEqual(len(checkpoint["lag_rows"]), 9)
            mutations = []
            missing_timestamp = json.loads(json.dumps(source))
            missing_timestamp["orders"][0].pop()
            mutations.append((missing_timestamp, 0, lag))
            mutations.append((source, 1, lag))
            nonzero_lag = json.loads(json.dumps(lag))
            nonzero_lag[0].update({"current": "3", "end": "4", "lag": 1})
            mutations.append((source, 0, nonzero_lag))
            wrong_topic = json.loads(json.dumps(lag))
            wrong_topic[0]["topic"] = "cdc.g002.public.extra"
            mutations.append((source, 0, wrong_topic))
            duplicate_coordinate = json.loads(json.dumps(lag))
            duplicate_coordinate[0] = duplicate_coordinate[1]
            mutations.append((source, 0, duplicate_coordinate))
            mutations.append((source, 0, lag[:-1]))
            for sink, errors, observed_lag in mutations:
                with self.subTest(sink=sink, errors=errors, lag=observed_lag):
                    with self.assertRaises(acceptance.AcceptanceError):
                        acceptance.validate_recovery_checkpoint(
                            "g002", source, sink, errors, observed_lag
                        )

    def test_archive_write_probe_rejects_writable_existing_sink_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sink = root / "cdc-g001.sqlite3"
            sink.write_bytes(b"sqlite")
            root.chmod(0o555)
            try:
                sink.chmod(0o666)
                environment = {
                    **os.environ,
                    "ARCHIVE_ROOT": str(root),
                    "ARCHIVE_SINK_NAME": sink.name,
                }
                writable = subprocess.run(
                    [sys.executable, "-c", acceptance.ARCHIVE_WRITE_DENIAL_PROGRAM],
                    env=environment,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(writable.returncode, 0)
                self.assertIn("sink file is writable", writable.stderr)
                sink.chmod(0o444)
                readonly = subprocess.run(
                    [sys.executable, "-c", acceptance.ARCHIVE_WRITE_DENIAL_PROGRAM],
                    env=environment,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(readonly.returncode, 0, readonly.stderr)
            finally:
                root.chmod(0o755)
                sink.chmod(0o644)

    def test_recovery_checkpoint_requires_connector_status(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")
        rows = {table: [] for table in acceptance.RECOVERY_ROW_FIELDS}
        lag = [
            {
                "topic": f"cdc.g002.public.{table}",
                "partition": partition,
                "current": "0",
                "end": "0",
                "lag": 0,
            }
            for table in ("customers", "orders", "payments")
            for partition in range(3)
        ]
        topic_values = {
            "KAFKA_CUSTOMERS_TOPIC": "cdc.g002.public.customers",
            "KAFKA_ORDERS_TOPIC": "cdc.g002.public.orders",
            "KAFKA_PAYMENTS_TOPIC": "cdc.g002.public.payments",
        }
        with (
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
            mock.patch.object(acceptance, "wait_projection_parity"),
            mock.patch.object(acceptance, "wait_lag_zero", return_value=lag),
            mock.patch.object(acceptance, "source_projection_rows", return_value=rows),
            mock.patch.object(acceptance, "sink_projection_rows", return_value=rows),
            mock.patch.object(acceptance, "sink_ingestion_error_count", return_value=0),
            mock.patch.object(
                acceptance,
                "generation_value",
                side_effect=lambda _generation, name: topic_values[name],
            ),
        ):
            acceptance.recovery_checkpoint("g002")
        run_mock.assert_called_once_with(
            [str(acceptance.LAB), "status", "g002"], timeout=120
        )

    def test_ac010_pass_rejects_summary_only_bundle(self) -> None:
        required = acceptance.manifest_assertions()["AC-010"]

        def summary_only(_: str) -> dict[str, str]:
            return {name: f"verified {name}" for name in required}

        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(acceptance, "detect_active_generation", return_value="g002"),
        ):
            passed, path = acceptance.execute_one(
                "AC-010",
                "g002",
                runtime_root=Path(temporary),
                probe=summary_only,
            )
            self.assertFalse(passed)
            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(document["result"], "FAIL")
            self.assertIn("structured machine-readable evidence", document["assertions"][0]["summary"])

    def test_execute_one_publishes_pass_and_fail_evidence(self) -> None:
        required = acceptance.manifest_assertions()["AC-005"]
        with tempfile.TemporaryDirectory() as temporary:
            runtime_root = Path(temporary)

            def passing(_: str) -> dict[str, str]:
                return {name: f"verified {name}" for name in required}

            passed, pass_path = acceptance.execute_one(
                "AC-005",
                "g001",
                runtime_root=runtime_root,
                probe=passing,
            )
            self.assertTrue(passed)
            pass_document = json.loads(pass_path.read_text(encoding="utf-8"))
            self.assertEqual(pass_document["result"], "PASS")
            self.assertTrue(all(item["result"] == "PASS" for item in pass_document["assertions"]))

            def failing(_: str) -> dict[str, str]:
                raise acceptance.AcceptanceError("deliberate probe failure")

            failed, fail_path = acceptance.execute_one(
                "AC-005",
                "g002",
                runtime_root=runtime_root,
                probe=failing,
            )
            self.assertFalse(failed)
            fail_document = json.loads(fail_path.read_text(encoding="utf-8"))
            self.assertEqual(fail_document["result"], "FAIL")
            self.assertEqual(fail_document["assertions"][0]["result"], "FAIL")
            self.assertTrue(
                all(
                    item["result"] == "NOT_RUN"
                    for item in fail_document["assertions"][1:]
                )
            )

    def test_all_run_index_requires_every_ac_and_exposes_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime_root = Path(temporary)
            results = [
                (
                    ac_id,
                    ac_id != "AC-011",
                    runtime_root / "evidence" / "g002" / ac_id / "run" / "index.json",
                )
                for ac_id in acceptance.PROBES
            ]
            output = acceptance.write_run_index(
                "g002",
                "2026-10-30T11:00:00+00:00",
                results,
                runtime_root=runtime_root,
            )
            document = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(document["result"], "FAIL")
            self.assertEqual(len(document["criteria"]), 14)
            self.assertEqual(
                [item["ac_id"] for item in document["criteria"]],
                list(acceptance.PROBES),
            )
            self.assertEqual(
                next(
                    item["result"]
                    for item in document["criteria"]
                    if item["ac_id"] == "AC-011"
                ),
                "FAIL",
            )
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.write_run_index(
                    "g002",
                    "2026-10-30T11:00:01+00:00",
                    results[:-1],
                    runtime_root=runtime_root,
                )

    def test_complete_lag_inventory_retries_transient_missing_rows(self) -> None:
        rows = [
            {
                "topic": f"cdc.g002.public.{table}",
                "partition": partition,
                "current": "1",
                "end": "1",
                "lag": 0,
            }
            for table in ("customers", "orders", "payments")
            for partition in range(3)
        ]
        with (
            mock.patch.object(
                acceptance,
                "lag_rows",
                side_effect=[
                    acceptance.AcceptanceError("incomplete consumer-group rows"),
                    rows,
                ],
            ),
            mock.patch.object(acceptance, "docker_state", return_value="running"),
            mock.patch.object(acceptance.time, "sleep"),
        ):
            self.assertEqual(
                acceptance.wait_complete_lag_inventory("g002", timeout=1), rows
            )

    def test_lag_rows_requires_exact_table_topic_partition_coordinates(self) -> None:
        topics = {
            "KAFKA_CONSUMER_GROUP": "sqlite-projector-g002",
            "KAFKA_CUSTOMERS_TOPIC": "cdc.g002.public.customers",
            "KAFKA_ORDERS_TOPIC": "cdc.g002.public.orders",
            "KAFKA_PAYMENTS_TOPIC": "cdc.g002.public.payments",
        }
        table_topics = [
            topics["KAFKA_CUSTOMERS_TOPIC"],
            topics["KAFKA_ORDERS_TOPIC"],
            topics["KAFKA_PAYMENTS_TOPIC"],
        ]
        lines = [
            f"{topics['KAFKA_CONSUMER_GROUP']} {topic} {partition} 4 4 0 consumer host client"
            for topic in table_topics
            for partition in range(3)
        ]

        def parse(candidate: list[str]) -> list[dict[str, object]]:
            with (
                mock.patch.object(
                    acceptance, "group_output", return_value="\n".join(candidate)
                ),
                mock.patch.object(
                    acceptance,
                    "generation_value",
                    side_effect=lambda _generation, name: topics[name],
                ),
            ):
                return acceptance.lag_rows("g002")

        self.assertEqual(len(parse(lines)), 9)
        mutations = []
        wrong_topic = list(lines)
        wrong_topic[-1] = wrong_topic[-1].replace(
            topics["KAFKA_PAYMENTS_TOPIC"], "cdc.g002.public.extra"
        )
        mutations.append(wrong_topic)
        extra_foreign = list(lines)
        extra_foreign.append(
            f"{topics['KAFKA_CONSUMER_GROUP']} unrelated.topic 0 4 4 0 consumer host client"
        )
        mutations.append(extra_foreign)
        duplicate_missing = list(lines)
        duplicate_missing[-1] = duplicate_missing[0]
        mutations.append(duplicate_missing)
        out_of_range = list(lines)
        out_of_range[-1] = out_of_range[-1].replace(" 2 4 4 0 ", " 3 4 4 0 ")
        mutations.append(out_of_range)
        for candidate in mutations:
            with self.subTest(candidate=candidate[-1]):
                with self.assertRaises(acceptance.AcceptanceError):
                    parse(candidate)

    def test_lag_zero_requires_exact_empty_or_committed_end_state(self) -> None:
        self.assertTrue(
            acceptance.lag_row_is_zero(
                {"current": "-", "end": "0", "lag": None}
            )
        )
        self.assertTrue(
            acceptance.lag_row_is_zero(
                {"current": "12", "end": "12", "lag": 0}
            )
        )
        for row in (
            {"current": "-", "end": "12", "lag": None},
            {"current": "11", "end": "12", "lag": 1},
            {"current": "11", "end": "12", "lag": 0},
        ):
            with self.subTest(row=row):
                self.assertFalse(acceptance.lag_row_is_zero(row))

    def test_slot_metric_parser_requires_complete_numeric_row(self) -> None:
        valid = (
            "--- postgres_replication_slot_retained_wal ---\n"
            "postgres_cdc_g002|reserved|464|1080078768|OK\n"
        )
        self.assertTrue(acceptance.slot_metric_present(valid, "g002"))
        for invalid in (
            "postgres_cdc_g002|reserved|464||OK\n",
            "postgres_cdc_g002|lost|464|0|WARNING\n",
            "postgres_cdc_g001|reserved|464|1080078768|OK\n",
        ):
            with self.subTest(invalid=invalid):
                self.assertFalse(acceptance.slot_metric_present(invalid, "g002"))

    def test_wal_warning_boundary_executes_actual_shell_validator(self) -> None:
        observed = acceptance.validate_wal_warning_boundary()
        self.assertEqual(
            observed,
            {
                "below": 536870911,
                "boundary": 536870912,
                "above": 536870913,
            },
        )

    def test_retention_offset_movement_requires_deleted_prefix_and_stable_end(
        self,
    ) -> None:
        observed = acceptance.validate_retention_offset_movement(
            before_start=0,
            before_end=acceptance.RETENTION_PROBE_RECORD_COUNT,
            after_start=192,
            after_end=acceptance.RETENTION_PROBE_RECORD_COUNT,
        )
        self.assertEqual(observed["after_start"], 192)
        count = acceptance.RETENTION_PROBE_RECORD_COUNT
        for mutation in (
            {"before_start": 1, "before_end": count, "after_start": 192, "after_end": count},
            {"before_start": 0, "before_end": count - 1, "after_start": 192, "after_end": count - 1},
            {"before_start": 0, "before_end": count, "after_start": 0, "after_end": count},
            {"before_start": 0, "before_end": count, "after_start": count, "after_end": count},
            {"before_start": 0, "before_end": count, "after_start": 192, "after_end": count + 1},
        ):
            with self.subTest(mutation=mutation):
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.validate_retention_offset_movement(**mutation)

    def test_retention_probe_requires_fast_broker_cleanup_interval(self) -> None:
        compose = (PROJECT_ROOT / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn('KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: "1000"', compose)

    def test_retention_probe_cleans_after_create_timeout_with_side_effect(self) -> None:
        completed = subprocess.CompletedProcess([], 0, "", "")

        def fake_run(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if "grep" in argv:
                return completed
            if "--create" in argv:
                raise subprocess.TimeoutExpired(argv, 60)
            self.fail(f"unexpected command before cleanup: {argv!r}")

        with (
            mock.patch.object(acceptance, "run", side_effect=fake_run),
            mock.patch.object(
                acceptance, "delete_retention_probe_topic"
            ) as cleanup_mock,
        ):
            with self.assertRaises(subprocess.TimeoutExpired):
                acceptance.run_kafka_retention_probe("g002")
        cleanup_mock.assert_called_once()
        topic = cleanup_mock.call_args.args[0]
        self.assertRegex(topic, r"^cdc\.g002\.acceptance\.retention-probe-")

    def test_retention_probe_cleanup_is_idempotent_and_waits_for_absence(self) -> None:
        topic = "cdc.g002.acceptance.retention-probe-1234-123456789"
        responses = [
            subprocess.CompletedProcess([], 0, f"{topic}\n", ""),
            subprocess.CompletedProcess([], 0, "", ""),
            subprocess.CompletedProcess([], 0, "", ""),
        ]
        with mock.patch.object(
            acceptance, "run", side_effect=responses
        ) as run_mock:
            acceptance.delete_retention_probe_topic(topic)
        commands = [call.args[0] for call in run_mock.call_args_list]
        self.assertIn("--list", commands[0])
        self.assertIn("--delete", commands[1])
        self.assertIn("--list", commands[2])
        selector = acceptance.retention_probe_topic_selector(topic)
        self.assertEqual(commands[0][commands[0].index("--topic") + 1], selector)
        self.assertEqual(commands[1][commands[1].index("--topic") + 1], selector)
        self.assertIsNotNone(re.fullmatch(selector, topic))
        self.assertIsNone(re.fullmatch(selector, topic.replace(".", "x", 1)))
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.retention_probe_topic_selector(
                "cdc.g002.acceptance.retention-probe-unsafe.*"
            )
        with mock.patch.object(
            acceptance,
            "run",
            return_value=subprocess.CompletedProcess([], 0, "", ""),
        ) as absent_run:
            acceptance.delete_retention_probe_topic(topic)
        absent_run.assert_called_once()

    def test_topic_offset_parser_requires_exact_partition_set(self) -> None:
        valid = "probe:0:0\nprobe:1:4\nprobe:2:9\n"
        self.assertEqual(
            acceptance.parse_topic_offsets(valid, "probe", 3), {0: 0, 1: 4, 2: 9}
        )
        for invalid in (
            "probe:0:0\nprobe:1:4\n",
            "probe:0:0\nprobe:0:4\nprobe:2:9\n",
            "probe:0:-1\nprobe:1:4\nprobe:2:9\n",
            "probe:0:nope\nprobe:1:4\nprobe:2:9\n",
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.parse_topic_offsets(invalid, "probe", 3)

    def test_all_orchestration_runs_ac010_last_but_indexes_manifest_order(self) -> None:
        evidence = Path("/tmp/evidence/index.json")
        seen: list[str] = []

        def execute(ac_id: str, _: str) -> tuple[bool, Path]:
            seen.append(ac_id)
            return ac_id != "AC-011", evidence

        with (
            mock.patch.object(acceptance, "detect_active_generation", return_value="g002"),
            mock.patch.object(acceptance, "classify_runtime_state", return_value="running"),
            mock.patch.object(acceptance, "ensure_runtime"),
            mock.patch.object(acceptance, "execute_one", side_effect=execute),
            mock.patch.object(acceptance, "write_run_index", return_value=evidence) as index_mock,
        ):
            with self.assertRaisesRegex(acceptance.AcceptanceError, "AC-011"):
                acceptance.run_acceptance("all")
        self.assertEqual(seen[-1], "AC-010")
        self.assertEqual(set(seen), set(acceptance.PROBES))
        indexed = index_mock.call_args.args[2]
        self.assertEqual([item[0] for item in indexed], list(acceptance.PROBES))

    def test_execute_one_publishes_failure_then_rethrows_restoration_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime_root = Path(temporary)

            def unsafe(_: str) -> dict[str, str]:
                raise acceptance.RuntimeRestorationError(
                    "runtime restoration failed"
                )

            with self.assertRaisesRegex(
                acceptance.RuntimeRestorationError, "runtime restoration failed"
            ):
                acceptance.execute_one(
                    "AC-007",
                    "g002",
                    runtime_root=runtime_root,
                    probe=unsafe,
                )
            evidence = list(
                (runtime_root / "evidence" / "g002" / "AC-007").glob(
                    "*/index.json"
                )
            )
            self.assertEqual(len(evidence), 1)
            self.assertEqual(
                json.loads(evidence[0].read_text(encoding="utf-8"))["result"],
                "FAIL",
            )

    def test_all_orchestration_aborts_after_runtime_restoration_failure(self) -> None:
        evidence = Path("/tmp/evidence/index.json")
        seen: list[str] = []

        def execute(ac_id: str, _: str) -> tuple[bool, Path]:
            seen.append(ac_id)
            if ac_id == "AC-007":
                raise acceptance.RuntimeRestorationError(
                    "runtime restoration failed"
                )
            return True, evidence

        with (
            mock.patch.object(acceptance, "detect_active_generation", return_value="g002"),
            mock.patch.object(acceptance, "classify_runtime_state", return_value="running"),
            mock.patch.object(acceptance, "ensure_runtime"),
            mock.patch.object(acceptance, "execute_one", side_effect=execute),
            mock.patch.object(acceptance, "write_run_index") as index_mock,
        ):
            with self.assertRaisesRegex(
                acceptance.RuntimeRestorationError, "runtime restoration failed"
            ):
                acceptance.run_acceptance("all")
        self.assertEqual(seen[-1], "AC-007")
        self.assertNotIn("AC-008", seen)
        self.assertNotIn("AC-010", seen)
        index_mock.assert_not_called()

    def test_run_acceptance_restores_initial_stopped_state_after_success(self) -> None:
        evidence = Path("/tmp/evidence/index.json")
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            mock.patch.object(acceptance, "detect_active_generation", return_value="g002"),
            mock.patch.object(acceptance, "classify_runtime_state", return_value="stopped"),
            mock.patch.object(acceptance, "ensure_runtime"),
            mock.patch.object(acceptance, "execute_one", return_value=(True, evidence)),
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
        ):
            result = acceptance.run_acceptance("AC-005")
        self.assertEqual(result["result"], "PASS")
        run_mock.assert_called_once_with(
            [str(acceptance.LAB), "stop"], check=False, timeout=120
        )

    def test_run_acceptance_restores_initial_stopped_state_after_failure(self) -> None:
        evidence = Path("/tmp/evidence/index.json")
        completed = subprocess.CompletedProcess([], 0, "", "")
        with (
            mock.patch.object(acceptance, "detect_active_generation", return_value="g002"),
            mock.patch.object(acceptance, "classify_runtime_state", return_value="stopped"),
            mock.patch.object(acceptance, "ensure_runtime"),
            mock.patch.object(acceptance, "execute_one", return_value=(False, evidence)),
            mock.patch.object(acceptance, "run", return_value=completed) as run_mock,
        ):
            with self.assertRaisesRegex(acceptance.AcceptanceError, "AC-005"):
                acceptance.run_acceptance("AC-005")
        run_mock.assert_called_once_with(
            [str(acceptance.LAB), "stop"], check=False, timeout=120
        )

    def test_check_script_dispatches_to_acceptance_runner(self) -> None:
        script = (PROJECT_ROOT / "scripts" / "check").read_text(encoding="utf-8")
        lab = (PROJECT_ROOT / "scripts" / "lab").read_text(encoding="utf-8")
        self.assertIn("tests/acceptance/run_acceptance.py", script)
        self.assertIn("active_consumer_state", script)
        self.assertIn('"$LAB" resume "$active_generation"', script)
        self.assertIn("resume_generation()", lab)
        self.assertIn("require_active_recovery_manifest", lab)
        self.assertIn("expected unique ACTIVE recovery manifest", lab)
        self.assertIn("payload.get('phase') != 'complete'", lab)
        self.assertGreaterEqual(
            lab.count('verify_recovery_consumer_scope "$generation"'), 4
        )
        for exact_field in (
            ".Config.Image",
            ".HostConfig.NanoCpus",
            "KAFKA_CONSUMER_GROUP",
            "KAFKA_CUSTOMERS_TOPIC",
            "SQLITE_SINK_PATH",
            "uses a stale image",
        ):
            self.assertIn(exact_field, lab)
        self.assertNotIn("found $running_services/4 running", script)
        self.assertNotIn("acceptance $selector is unavailable until R-09", script)


if __name__ == "__main__":
    unittest.main()
