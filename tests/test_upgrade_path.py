"""Regression tests for the split-poke upgrade-path probe."""

import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from upgrade_path import _connect_and_drive  # noqa: E402


def test_connect_probe_uses_public_duration_and_explicit_client_group():
    params = inspect.signature(_connect_and_drive).parameters
    assert "duration_s" in params
    assert "client_group_id" in params
    assert "initial_state" in params


def test_upgrade_probe_no_longer_reads_missing_duration_s_namespace_field():
    source = inspect.getsource(sys.modules[_connect_and_drive.__module__].probe)
    assert "a.duration_s" not in source
    assert source.count("shared_cgid") >= 3
    assert "initial_state=mat_old" in source
    assert "a.seed, shared_cgid,\n        initial_state=mat_old" in source
