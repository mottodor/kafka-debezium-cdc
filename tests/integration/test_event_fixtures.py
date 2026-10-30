from __future__ import annotations

import unittest

from tests.support.event_fixtures import EVENT_ROOT, run_fixture


class EventFixtureIntegrationTests(unittest.TestCase):
    def test_positive_event_fixtures_match_independent_oracles(self) -> None:
        fixture_paths = sorted(EVENT_ROOT.glob("ac-*.json"))
        self.assertGreater(len(fixture_paths), 0)
        for path in fixture_paths:
            with self.subTest(fixture=path.name):
                observed = run_fixture(path)
                self.assertGreater(len(observed["outcomes"]), 0)


if __name__ == "__main__":
    unittest.main()
