"""Unit tests for harness/protocol.py — the vendored Zero wire-protocol primitives.

These pin the byte-exact contract with mono's encodeSecProtocols /
PROTOCOL_VERSION so a mono upgrade that changes the wire format is caught
here before it silently breaks every live driver and oracle.
"""
from __future__ import annotations

import base64
import json
import urllib.parse

from protocol import DEFAULT_PROTOCOL_VERSION, encode_sec_protocols


def test_protocol_version_is_49():
    # packages/zero-protocol/src/protocol-version.ts. Bump this test when the
    # mono default moves; updating the constant without the test is exactly
    # the silent-drift this suite exists to catch.
    assert DEFAULT_PROTOCOL_VERSION == 49


def test_encode_sec_protocols_structure():
    sec = encode_sec_protocols(["initConnection", {"desiredQueriesPatch": []}], "tok")
    # outer layer is percent-encoding; decode it to reach the base64
    raw = urllib.parse.unquote(sec)
    payload = json.loads(base64.b64decode(raw))
    assert payload == {
        "initConnectionMessage": ["initConnection", {"desiredQueriesPatch": []}],
        "authToken": "tok",
    }


def test_encode_sec_protocols_compact_json():
    # compact separators (",", ":") — no spaces — matches mono's JSON.stringify
    sec = encode_sec_protocols(None, "tok")
    raw = urllib.parse.unquote(sec)
    # decode base64 to the raw JSON string and assert no whitespace
    json_bytes = base64.b64decode(raw)
    assert b" " not in json_bytes
    assert json.loads(json_bytes) == {"initConnectionMessage": None, "authToken": "tok"}


def test_encode_sec_protocols_deterministic():
    a = encode_sec_protocols(["initConnection", {}], "abc")
    b = encode_sec_protocols(["initConnection", {}], "abc")
    assert a == b


def test_encode_sec_protocols_distinct_inputs():
    a = encode_sec_protocols(None, "token-a")
    b = encode_sec_protocols(None, "token-b")
    assert a != b


def test_encode_sec_protocols_none_token():
    sec = encode_sec_protocols(None, None)
    raw = urllib.parse.unquote(sec)
    payload = json.loads(base64.b64decode(raw))
    assert payload == {"initConnectionMessage": None, "authToken": None}


def test_encode_sec_protocols_roundtrips_for_all_callers():
    # Every live driver/oracle calls this with (None, token) post-handshake.
    # Ensure that shape is stable and URL-safe.
    sec = encode_sec_protocols(None, "eyJhbGc.test-token")
    assert all(c not in sec for c in ' +"\\'), "must be URL-safe"
    raw = urllib.parse.unquote(sec)
    json.loads(base64.b64decode(raw))  # decodes without error
