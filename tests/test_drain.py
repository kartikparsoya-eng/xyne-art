"""Regression tests for strict SIGTERM client accounting."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from drain_test import client_outcomes  # noqa: E402


def test_all_clients_must_be_clean_or_rehomed():
    results = [(0, "closed", 1001, ""), (1, "control", None, "Rehome")]
    counts, ok = client_outcomes(results, 2)
    assert ok
    assert counts == {"abrupt": 0, "clean": 1, "controlled": 1, "failed": 0,
                      "unnotified": 0}


def test_one_clean_client_cannot_mask_failed_connections():
    results = [(0, "closed", 1001, ""), (1, "connect-failed", None, "boom")]
    _, ok = client_outcomes(results, 2)
    assert not ok


def test_abrupt_close_never_passes():
    _, ok = client_outcomes([(0, "closed", 1006, "")], 1)
    assert not ok


def test_missing_results_are_counted_as_unnotified():
    counts, ok = client_outcomes([], 10)
    assert counts["unnotified"] == 10
    assert not ok
