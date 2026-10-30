from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests.support.event_fixtures import EVENT_ROOT, load_fixture

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FixtureContractTests(unittest.TestCase):
    def test_positive_fixture_set_is_non_empty_and_covers_synthetic_contracts(self) -> None:
        fixtures = sorted(EVENT_ROOT.glob("ac-*.json"))
        self.assertGreater(len(fixtures), 0)
        loaded = [load_fixture(path) for path in fixtures]
        self.assertEqual(
            {fixture["ac_id"] for fixture in loaded},
            {"AC-005", "AC-006", "AC-012"},
        )
        for fixture in loaded:
            with self.subTest(ac_id=fixture["ac_id"]):
                self.assertGreater(len(fixture["records"]), 0)
                self.assertGreater(len(fixture["expected"]["outcomes"]), 0)

    def test_deliberate_failure_fixture_extends_a_real_oracle(self) -> None:
        path = EVENT_ROOT / "negative" / "ac-005-wrong-projection.json"
        raw = json.loads(path.read_text(encoding="utf-8"))
        self.assertIn("deliberate failing fixture", raw["purpose"])
        mutated = load_fixture(path)
        self.assertEqual(mutated["ac_id"], "AC-005")
        self.assertEqual(
            mutated["expected"]["projection"]["row"]["full_name"],
            "Incorrect Expected Name",
        )

    def test_acceptance_manifest_covers_every_approved_ac_once(self) -> None:
        manifest = json.loads(
            (PROJECT_ROOT / "fixtures" / "expected" / "acceptance-evidence.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(manifest["format_version"], 1)
        criteria = manifest["criteria"]
        expected_ids = [f"AC-{number:03d}" for number in range(1, 15)]
        self.assertEqual([criterion["ac_id"] for criterion in criteria], expected_ids)
        assertions_by_ac = {
            criterion["ac_id"]: criterion["required_assertions"]
            for criterion in criteria
        }
        self.assertEqual(
            assertions_by_ac["AC-004"],
            [
                "durable_precommit_state",
                "duplicate_coordinate",
                "delivery_count",
                "projection_unchanged_after_duplicate",
                "offset_after_restart",
            ],
        )
        self.assertEqual(
            assertions_by_ac["AC-007"],
            [
                "pre_migration_fail_stop",
                "offset_unchanged",
                "same_coordinate_replay",
                "phone_projection",
                "offset_after_replay",
            ],
        )
        self.assertEqual(
            assertions_by_ac["AC-011"],
            [
                "topic_retention",
                "topic_log_offsets",
                "wal_warning",
                "slot_state_and_limit",
                "sqlite_controlled_stop",
                "disk_preflight_refusal",
            ],
        )
        for criterion in criteria:
            with self.subTest(ac_id=criterion["ac_id"]):
                assertions = criterion["required_assertions"]
                self.assertGreater(len(assertions), 0)
                self.assertEqual(len(assertions), len(set(assertions)))
                self.assertTrue(
                    all(
                        isinstance(name, str) and name and name == name.lower()
                        for name in assertions
                    )
                )


if __name__ == "__main__":
    unittest.main()
