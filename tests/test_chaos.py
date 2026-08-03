"""Regression tests for chaos recovery on images without HEALTHCHECK."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from chaos import recovery_ok  # noqa: E402


def test_running_container_is_valid_recovery_without_healthcheck():
    assert recovery_ok(True, "running")


def test_configured_healthy_container_is_valid_recovery():
    assert recovery_ok(True, "healthy")


def test_unknown_stopped_or_unreverted_never_passes():
    assert not recovery_ok(True, "unknown")
    assert not recovery_ok(True, "stopped")
    assert not recovery_ok(False, "running")
