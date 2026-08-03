"""Regression tests for cold-start split-poke hydration detection."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from cold_start import hydration_frame  # noqa: E402


def test_split_poke_counts_rows_and_completes_at_poke_end():
    assert hydration_frame(["pokePart", {"rowsPatch": [
        {"op": "put", "tableName": "users", "value": {"id": "1"}},
    ]}]) == (1, False, None)
    assert hydration_frame(["pokeEnd", {"cookie": "00:02"}]) == (0, True, None)


def test_empty_registration_poke_is_not_itself_evidentiary():
    assert hydration_frame(["pokeEnd", {"cookie": "00:01"}]) == (0, True, None)


def test_transform_error_is_not_reported_as_a_timeout():
    assert hydration_frame(["transformError", {"queryName": "badQuery"}]) == (
        0, False, "badQuery",
    )
