#!/usr/bin/env python3
"""Run the non-empty offline unit, integration, and acceptance-asset suites."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    suite = unittest.TestSuite()
    for start_directory in (
        PROJECT_ROOT / "consumer" / "tests",
        PROJECT_ROOT / "tests" / "unit",
        PROJECT_ROOT / "tests" / "integration",
        PROJECT_ROOT / "tests" / "acceptance",
    ):
        discovered = unittest.TestLoader().discover(
            str(start_directory), pattern="test_*.py"
        )
        if discovered.countTestCases() == 0:
            raise SystemExit(f"empty test selection: {start_directory.relative_to(PROJECT_ROOT)}")
        suite.addTests(discovered)
    if suite.countTestCases() == 0:
        raise SystemExit("offline suite is empty")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
