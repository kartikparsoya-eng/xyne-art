import re

from tools.log_gate import (
    HARD_BLOCKING,
    _known_labeled,
    is_error_or_warn_level,
    normalize_signature,
)


def test_info_message_containing_error_is_not_error_level() -> None:
    line = (
        '{"level":"INFO","errorKind":"Unauthorized",'
        '"message":"Sending error on WebSocket"}'
    )
    assert not is_error_or_warn_level(line)


def test_structured_and_plain_warning_levels_are_detected() -> None:
    assert is_error_or_warn_level('{"level":"WARN","message":"new failure"}')
    assert is_error_or_warn_level("WARN: new failure")
    assert not is_error_or_warn_level('{"level":"INFO","message":"warning count 2"}')


def test_negative_suite_events_are_known_operational_events() -> None:
    assert _known_labeled('{"level":"WARN","errorKind":"AuthInvalidated"}')
    assert _known_labeled('{"level":"WARN","errorKind":"ClientNotFound"}')
    assert _known_labeled(
        '{"level":"WARN","errorBody":{"kind":"InvalidConnectionRequestBaseCookie"}}'
    )
    assert _known_labeled(
        '{"level":"WARN","errorBody":{"kind":"Internal",'
        '"message":"shut down before initialization completed"}}'
    )
    assert _known_labeled(
        '{"level":"WARN","errorBody":{"kind":"Rehome",'
        '"message":"CVR has been concurrently modified"}}'
    )
    assert _known_labeled(
        '{"level":"WARN","error":"alreadyProcessed",'
        '"message":"mutation was already processed"}'
    )
    assert _known_labeled(
        '{"level":"WARN","message":"Slow SQLite query 250.0"}'
    )


def test_unknown_warning_remains_unknown() -> None:
    line = '{"level":"WARN","message":"previously unseen data corruption"}'
    assert is_error_or_warn_level(line)
    assert _known_labeled(line) is None


def test_sqlite_corruption_is_hard_blocking() -> None:
    patterns = dict(HARD_BLOCKING)
    assert "sqlite-corrupt" in patterns
    assert re.search(
        patterns["sqlite-corrupt"],
        "advance failed: get_rows next: database disk image is malformed",
    )


def test_structured_signature_ignores_query_context_and_dynamic_ids() -> None:
    left = (
        '{"level":"WARN","worker":"syncer","workerIndex":1,'
        '"clientGroupID":"art-one","class":"Statement",'
        '"sql":"select * from a","message":"Slow SQLite query 121.4"}'
    )
    right = (
        '{"level":"WARN","worker":"syncer","workerIndex":3,'
        '"clientGroupID":"art-two","class":"Statement",'
        '"sql":"select * from unrelated","message":"Slow SQLite query 987.6"}'
    )
    assert normalize_signature(left) == normalize_signature(right)


def test_structured_signature_preserves_failure_message() -> None:
    corruption = normalize_signature(
        '{"level":"WARN","message":"database disk image is malformed"}'
    )
    timeout = normalize_signature(
        '{"level":"WARN","message":"request timed out after 500ms"}'
    )
    assert corruption != timeout
