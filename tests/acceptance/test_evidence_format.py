from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_LIB = PROJECT_ROOT / "scripts" / "lib"
if str(SCRIPTS_LIB) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_LIB))

import evidence as evidence_module  # pyright: ignore[reportMissingImports]
from evidence import (  # pyright: ignore[reportMissingImports]
    EvidenceError,
    evidence_run_directory,
    load_evidence,
    validate_evidence,
    write_evidence,
)


class EvidenceFormatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.example_path = (
            PROJECT_ROOT / "fixtures" / "expected" / "evidence-example.json"
        )
        self.example = json.loads(self.example_path.read_text(encoding="utf-8"))

    def test_example_validates_and_writes_to_generation_scoped_runtime_path(self) -> None:
        validated = load_evidence(self.example_path)
        self.assertEqual(
            (validated["ac_id"], validated["generation"], validated["result"]),
            ("AC-005", "g001", "PASS"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = write_evidence(validated, temporary_directory)
            self.assertEqual(output.name, "index.json")
            self.assertEqual(
                output.parts[-4:-1],
                ("g001", "AC-005", "20261030T060000.000000Z"),
            )
            self.assertEqual(load_evidence(output), validated)
            with self.assertRaises(FileExistsError):
                write_evidence(validated, temporary_directory)

    def test_required_assertion_set_cannot_be_omitted_or_reordered(self) -> None:
        assertions = self.example["assertions"]
        for index, assertion in enumerate(assertions):
            with self.subTest(missing=assertion["name"]):
                mutated = copy.deepcopy(self.example)
                mutated["assertions"].pop(index)
                with self.assertRaisesRegex(EvidenceError, "exactly match"):
                    validate_evidence(mutated)
        reordered = copy.deepcopy(self.example)
        reordered["assertions"][0], reordered["assertions"][1] = (
            reordered["assertions"][1],
            reordered["assertions"][0],
        )
        with self.assertRaisesRegex(EvidenceError, "exactly match"):
            validate_evidence(reordered)

    def test_evidence_rejects_inconsistent_or_sensitive_documents(self) -> None:
        mutations: list[tuple[str, dict[str, object]]] = []

        inconsistent = copy.deepcopy(self.example)
        inconsistent["assertions"][0]["result"] = "FAIL"
        mutations.append(("pass_with_failed_assertion", inconsistent))

        secret_field = copy.deepcopy(self.example)
        secret_field["password"] = "must-not-persist"
        mutations.append(("sensitive_field", secret_field))

        secret_path = copy.deepcopy(self.example)
        secret_path["command"].append(".env")
        mutations.append(("secret_path", secret_path))

        credential_url = copy.deepcopy(self.example)
        credential_url["command"] = [
            "psql",
            "postgresql://cdc:hunter2@localhost/cdc_lab",
        ]
        mutations.append(("credential_url", credential_url))

        environment_assignment = copy.deepcopy(self.example)
        environment_assignment["command"].append("POSTGRES_PASSWORD=hunter2")
        mutations.append(("environment_assignment", environment_assignment))

        split_command_credential = copy.deepcopy(self.example)
        split_command_credential["command"] = [
            "example-client",
            "--password",
            "hunter2",
        ]
        mutations.append(("split_command_credential", split_command_credential))

        option_assignment_credential = copy.deepcopy(self.example)
        option_assignment_credential["command"] = [
            "example-client",
            "--access-key-id=AKIAEXAMPLE",
        ]
        mutations.append(("option_assignment_credential", option_assignment_credential))

        split_auth_credential = copy.deepcopy(self.example)
        split_auth_credential["command"] = [
            "example-client",
            "--auth",
            "dXNlcjpwYXNz",
        ]
        mutations.append(("split_auth_credential", split_auth_credential))

        sensitive_summary = copy.deepcopy(self.example)
        sensitive_summary["assertions"][0]["summary"] = "password=hunter2"
        mutations.append(("sensitive_summary", sensitive_summary))

        meaningless_redactions = copy.deepcopy(self.example)
        meaningless_redactions["redactions"] = ["none"]
        mutations.append(("meaningless_redactions", meaningless_redactions))

        for artifact_name in (
            "docker-inspect.json",
            "auth-config.json",
            "full-environment.json",
        ):
            forbidden_artifact = copy.deepcopy(self.example)
            forbidden_artifact["artifacts"] = [
                {
                    "path": artifact_name,
                    "sha256": "0" * 64,
                    "media_type": "application/json",
                }
            ]
            mutations.append((artifact_name, forbidden_artifact))

        for name, document in mutations:
            with self.subTest(name=name):
                with self.assertRaises(EvidenceError):
                    validate_evidence(document)

    def test_writer_verifies_and_atomically_publishes_artifact_bundle(self) -> None:
        document = copy.deepcopy(self.example)
        content = (
            b'{"ordering_token":[1,500,2,2],"status":"verified",'
            b'"name":"row_count","value":2}\n'
        )
        document["artifacts"] = [
            {
                "path": "queries/source.json",
                "sha256": hashlib.sha256(content).hexdigest(),
                "media_type": "application/json",
            }
        ]
        with (
            tempfile.TemporaryDirectory() as runtime_directory,
            tempfile.TemporaryDirectory() as artifact_directory,
        ):
            source = Path(artifact_directory) / "queries" / "source.json"
            source.parent.mkdir(parents=True)
            source.write_bytes(content)
            output = write_evidence(
                document,
                runtime_directory,
                artifact_root=artifact_directory,
            )
            self.assertEqual(
                (output.parent / "queries" / "source.json").read_bytes(), content
            )
            self.assertEqual(load_evidence(output), document)
            self.assertFalse(
                any(path.name.startswith(".") for path in output.parent.parent.iterdir())
            )

    def test_writer_rejects_missing_mismatched_or_sensitive_artifacts(self) -> None:
        content = b'{"rows":2}\n'
        document = copy.deepcopy(self.example)
        document["artifacts"] = [
            {
                "path": "queries/source.json",
                "sha256": hashlib.sha256(content).hexdigest(),
                "media_type": "application/json",
            }
        ]
        with (
            tempfile.TemporaryDirectory() as runtime_directory,
            tempfile.TemporaryDirectory() as artifact_directory,
        ):
            with self.assertRaisesRegex(EvidenceError, "missing"):
                write_evidence(
                    document,
                    runtime_directory,
                    artifact_root=artifact_directory,
                )
            source = Path(artifact_directory) / "queries" / "source.json"
            source.parent.mkdir(parents=True)
            source.write_bytes(content)
            mismatched = copy.deepcopy(document)
            mismatched["artifacts"][0]["sha256"] = "0" * 64
            with self.assertRaisesRegex(EvidenceError, "checksum mismatch"):
                write_evidence(
                    mismatched,
                    runtime_directory,
                    artifact_root=artifact_directory,
                )
            source.write_text('{"note":"password=hunter2"}\n', encoding="utf-8")
            sensitive = copy.deepcopy(document)
            sensitive["artifacts"][0]["sha256"] = hashlib.sha256(
                source.read_bytes()
            ).hexdigest()
            with self.assertRaisesRegex(EvidenceError, "credential-bearing"):
                write_evidence(
                    sensitive,
                    runtime_directory,
                    artifact_root=artifact_directory,
                )

            environment_dump = b"HOME=/home/test\nPATH=/usr/bin\nLANG=C.UTF-8\n"
            text_source = Path(artifact_directory) / "queries" / "source.txt"
            text_source.write_bytes(environment_dump)
            environment_document = copy.deepcopy(self.example)
            environment_document["artifacts"] = [
                {
                    "path": "queries/source.txt",
                    "sha256": hashlib.sha256(environment_dump).hexdigest(),
                    "media_type": "text/plain",
                }
            ]
            with self.assertRaisesRegex(EvidenceError, "environment dump"):
                write_evidence(
                    environment_document,
                    runtime_directory,
                    artifact_root=artifact_directory,
                )

            sensitive_json_documents = {
                "plain_token": b'{"token":"hunter2"}\n',
                "plain_access": b'{"access":"hunter2"}\n',
                "plain_auth": b'{"auth":"dXNlcjpwYXNz"}\n',
                "environment_object": (
                    b'{"HOME":"/home/test","PATH":"/usr/bin","LANG":"C",'
                    b'"SHELL":"/bin/sh"}\n'
                ),
                "service_environment_object": (
                    b'{"POSTGRES_HOST":"postgres","POSTGRES_DB":"cdc_lab",'
                    b'"CONNECT_BOOTSTRAP_SERVERS":"kafka:9092"}\n'
                ),
                "name_value_credential": (
                    b'{"name":"POSTGRES_PASSWORD","value":"hunter2"}\n'
                ),
                "environment_array": b'["HOME=/home/test","PATH=/usr/bin","LANG=C"]\n',
                "config_only_docker_inspect": b'{"Config":{"Env":[]}}\n',
            }
            for name, sensitive_content in sensitive_json_documents.items():
                with self.subTest(sensitive_json=name):
                    source.write_bytes(sensitive_content)
                    sensitive_json = copy.deepcopy(document)
                    sensitive_json["artifacts"][0]["sha256"] = hashlib.sha256(
                        sensitive_content
                    ).hexdigest()
                    with self.assertRaises(EvidenceError):
                        write_evidence(
                            sensitive_json,
                            runtime_directory,
                            artifact_root=artifact_directory,
                        )

            relabeled_sensitive_content = {
                "json_as_text": b'{"name":"POSTGRES_PASSWORD","value":"hunter2"}\n',
                "name_value_csv_as_text": b"name,value\nPOSTGRES_PASSWORD,hunter2\n",
                "password_column_csv_as_text": (
                    b"username,password\ncdc_connector,hunter2\n"
                ),
                "access_key_columns_csv_as_text": (
                    b"account,access_key_id,secret_access_key\n"
                    b"cdc_connector,AKIAEXAMPLE,hunter2\n"
                ),
                "single_line_access_key_assignment": (
                    b"AWS_ACCESS_KEY_ID=AKIAEXAMPLE\n"
                ),
                "single_line_auth_assignment": b"auth=dXNlcjpwYXNz\n",
            }
            for name, sensitive_content in relabeled_sensitive_content.items():
                with self.subTest(relabeled=name):
                    source.write_bytes(sensitive_content)
                    relabeled = copy.deepcopy(document)
                    relabeled["artifacts"][0]["sha256"] = hashlib.sha256(
                        sensitive_content
                    ).hexdigest()
                    relabeled["artifacts"][0]["media_type"] = "text/plain"
                    with self.assertRaises(EvidenceError):
                        write_evidence(
                            relabeled,
                            runtime_directory,
                            artifact_root=artifact_directory,
                        )

            sensitive_csv_documents = {
                "name_value": b"name,value\nPOSTGRES_PASSWORD,hunter2\n",
                "password_column": b"username,password\ncdc_connector,hunter2\n",
                "access_key_columns": (
                    b"account,access_key_id,secret_access_key\n"
                    b"cdc_connector,AKIAEXAMPLE,hunter2\n"
                ),
            }
            csv_source = Path(artifact_directory) / "queries" / "values.csv"
            for name, csv_content in sensitive_csv_documents.items():
                with self.subTest(sensitive_csv=name):
                    csv_source.write_bytes(csv_content)
                    csv_document = copy.deepcopy(self.example)
                    csv_document["artifacts"] = [
                        {
                            "path": "queries/values.csv",
                            "sha256": hashlib.sha256(csv_content).hexdigest(),
                            "media_type": "text/csv",
                        }
                    ]
                    with self.assertRaises(EvidenceError):
                        write_evidence(
                            csv_document,
                            runtime_directory,
                            artifact_root=artifact_directory,
                        )
            self.assertFalse(evidence_run_directory(runtime_directory, document).exists())

    def test_writer_allows_structural_ordering_token_evidence(self) -> None:
        json_content = b'{"name":"ordering_token","value":[1,500,2,2]}\n'
        csv_content = b"name,value\nsource_position_token,1:500:2:2\n"
        column_csv_content = (
            b"ordering_token,source_position_token\n"
            b"1:500:2:2,1:500:2:2\n"
        )
        document = copy.deepcopy(self.example)
        document["artifacts"] = [
            {
                "path": "queries/ordering.json",
                "sha256": hashlib.sha256(json_content).hexdigest(),
                "media_type": "application/json",
            },
            {
                "path": "queries/ordering.csv",
                "sha256": hashlib.sha256(csv_content).hexdigest(),
                "media_type": "text/csv",
            },
            {
                "path": "queries/ordering-columns.csv",
                "sha256": hashlib.sha256(column_csv_content).hexdigest(),
                "media_type": "text/csv",
            },
        ]
        with (
            tempfile.TemporaryDirectory() as runtime_directory,
            tempfile.TemporaryDirectory() as artifact_directory,
        ):
            query_directory = Path(artifact_directory) / "queries"
            query_directory.mkdir()
            (query_directory / "ordering.json").write_bytes(json_content)
            (query_directory / "ordering.csv").write_bytes(csv_content)
            (query_directory / "ordering-columns.csv").write_bytes(column_csv_content)
            output = write_evidence(
                document,
                runtime_directory,
                artifact_root=artifact_directory,
            )
            self.assertTrue(output.is_file())

    def test_interrupted_publish_cleans_staging_and_allows_retry(self) -> None:
        document = copy.deepcopy(self.example)
        with tempfile.TemporaryDirectory() as runtime_directory:
            final_directory = evidence_run_directory(runtime_directory, document)
            with mock.patch.object(
                Path,
                "replace",
                side_effect=OSError("injected publish failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected publish failure"):
                    write_evidence(document, runtime_directory)
            self.assertFalse(final_directory.exists())
            self.assertFalse(
                any(
                    path.name.startswith(f".{final_directory.name}.staging-")
                    for path in final_directory.parent.iterdir()
                )
            )
            output = write_evidence(document, runtime_directory)
            self.assertTrue(output.is_file())

    def test_post_rename_fsync_failure_removes_final_bundle_and_allows_retry(self) -> None:
        document = copy.deepcopy(self.example)
        with tempfile.TemporaryDirectory() as runtime_directory:
            final_directory = evidence_run_directory(runtime_directory, document)
            with mock.patch.object(
                evidence_module,
                "_fsync_directory",
                side_effect=[None, OSError("injected parent fsync failure"), None],
            ):
                with self.assertRaisesRegex(OSError, "injected parent fsync failure"):
                    write_evidence(document, runtime_directory)
            self.assertFalse(final_directory.exists())
            self.assertFalse(
                any(
                    path.name.startswith(f".{final_directory.name}.staging-")
                    for path in final_directory.parent.iterdir()
                )
            )
            output = write_evidence(document, runtime_directory)
            self.assertTrue(output.is_file())

    def test_deliberate_failing_fixture_exits_nonzero_with_oracle_mismatch(self) -> None:
        fixture = (
            PROJECT_ROOT
            / "fixtures"
            / "events"
            / "negative"
            / "ac-005-wrong-projection.json"
        )
        result = subprocess.run(
            [sys.executable, "tests/run_event_fixture.py", str(fixture)],
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fixture oracle mismatch", result.stderr)
        self.assertIn("Incorrect Expected Name", result.stderr)
        self.assertIn("Newest Name", result.stderr)


if __name__ == "__main__":
    unittest.main()
