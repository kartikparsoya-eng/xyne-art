"""Pytest config: put the harness/ package on sys.path so tests can import
`workload` and `protocol` the same way the scripts do."""
import os
import sys

HARNESS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "harness")
if HARNESS not in sys.path:
    sys.path.insert(0, HARNESS)
