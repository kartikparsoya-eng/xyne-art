"""
protocol.py — vendored Zero wire-protocol primitives.

Kept byte-for-byte compatible with the mono repo so zero-cache's
decodeSecProtocols round-trips. This module is stdlib-only (no `websockets`)
so it can be unit-tested without a live server, just like workload.py.

If any of these source files change in a mono upgrade, update here:
  packages/zero-protocol/src/protocol-version.ts   -> DEFAULT_PROTOCOL_VERSION
  packages/zero-protocol/src/connect.ts::encodeSecProtocols -> encode_sec_protocols
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from typing import Optional

# Current mono protocol version (packages/zero-protocol/src/protocol-version.ts).
DEFAULT_PROTOCOL_VERSION = 49


def encode_sec_protocols(init_connection_message: Optional[list],
                         auth_token: Optional[str]) -> str:
    """Port of packages/zero-protocol/src/connect.ts::encodeSecProtocols.
    base64(utf8(JSON)) then percent-encode. Kept byte-for-byte compatible so
    zero-cache's decodeSecProtocols round-trips. If that file changes, update here."""
    protocols = {"initConnectionMessage": init_connection_message, "authToken": auth_token}
    raw = json.dumps(protocols, separators=(",", ":")).encode("utf-8")
    return urllib.parse.quote(base64.b64encode(raw).decode("ascii"))
