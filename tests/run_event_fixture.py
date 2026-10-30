#!/usr/bin/env python3
"""Execute one deterministic event fixture against the SQLite processor."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.support.event_fixtures import (
    FixtureAssertionError,
    FixtureError,
    run_fixture,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        observed = run_fixture(args.fixture)
    except (FixtureError, FixtureAssertionError) as error:
        raise SystemExit(f"fixture failed: {error}") from error
    print(json.dumps(observed, sort_keys=True))


if __name__ == "__main__":
    main()
