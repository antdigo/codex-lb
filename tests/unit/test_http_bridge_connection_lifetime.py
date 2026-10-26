from collections import deque
from typing import Any, cast
from unittest.mock import Mock

import pytest

from app.core.openai.requests import ResponsesRequest
from app.modules.proxy._service.http_bridge.connection_lifetime import _HTTPBridgeAgeRotation
from app.modules.proxy._service.support import _HTTPBridgeSessionKey
from tests.unit.test_http_bridge_idle_leases import _make_bridge_session

pytestmark = pytest.mark.unit


def _candidate(session, items=None):
    session.key = _HTTPBridgeSessionKey("prompt_cache", "age-key", None)
    session.physical_opened_at = 10.0
    payload = ResponsesRequest.model_validate(
        {
            "model": "gpt-5.4",
            "instructions": "",
            "input": items or [{"role": "user", "content": "complete"}],
        }
    )
    return _HTTPBridgeAgeRotation(session.key, payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("elapsed,expected", [(2999.999, False), (3000, True), (3593.4, True)])
async def test_age_threshold_is_physical_and_not_idle(elapsed, expected):
    session = _make_bridge_session()
    candidate = _candidate(session)
    session.last_used_at = 10 + elapsed
    assert candidate.allows(session, now=10 + elapsed) is expected
    session.physical_opened_at = 10 + elapsed
    assert not candidate.allows(session, now=10 + elapsed)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner",
    [
        "pending",
        "draining",
        "queued_request_count",
        "admission_waiter_count",
        "unanchored_reservation_id",
        "handoff_in_progress",
        "handoff_future",
        "resource_close_task",
        "closed",
        "reader_processing",
        "codex_session",
        "liveness_settlement_owner",
        "reconnect_requested",
        "retire_after_drain",
        "pending_lock",
        "lifecycle_lock",
    ],
)
async def test_age_rotation_keeps_every_owned_generation(owner):
    session = _make_bridge_session()
    candidate = _candidate(session)
    if owner in {"pending", "draining"}:
        session.pending_requests = deque([Mock(downstream_disconnected=owner == "draining")])
    elif owner in {"pending_lock", "lifecycle_lock"}:
        await getattr(session, owner).acquire()
    elif owner in {"reconnect_requested", "retire_after_drain"}:
        setattr(session.upstream_control, owner, True)
    else:
        setattr(session, owner, True)
    try:
        assert not candidate.allows(session, now=4000)
    finally:
        if owner in {"pending_lock", "lifecycle_lock"}:
            getattr(session, owner).release()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [
        {"type": "item_reference", "id": "opaque"},
        {"type": "reasoning", "id": "reason"},
        {"type": "message", "id": "message", "role": "assistant"},
        {"type": "function_call", "call_id": "call", "name": "tool"},
        {"type": "function_call_output", "call_id": "call"},
        {"type": "unknown", "content": "opaque"},
        {"role": "user", "content": [{"type": "input_image", "file_id": "opaque"}]},
        {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}", "status": "in_progress"},
    ],
)
async def test_age_rotation_rejects_incomplete_or_opaque_original_items(item):
    session = _make_bridge_session()
    candidate = _candidate(session, [item])
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
async def test_age_rotation_preserves_native_items_and_binds_original_target():
    items = [
        {"role": "user", "content": "complete"},
        {"type": "reasoning", "id": "reason", "encrypted_content": "synthetic-ciphertext"},
        {"type": "function_call", "id": "fc", "call_id": "call", "name": "tool", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call", "output": "result"},
    ]
    session = _make_bridge_session()
    candidate = _candidate(session, items)
    assert candidate.allows(session, now=4000)
    assert candidate.payload.input[: len(items)] == items
    session.last_completed_input_prefix_fingerprint = "mismatch"
    assert candidate.allows(session, now=4000)
    session.last_completed_input_prefix_fingerprint = None
    session.key = _HTTPBridgeSessionKey("prompt_cache", "different-target", None)
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
@pytest.mark.parametrize("anchor", ["previous_response_id", "conversation"])
async def test_age_rotation_rejects_original_anchors(anchor):
    session = _make_bridge_session()
    candidate = _candidate(session)
    setattr(candidate.payload, anchor, "anchor")
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
async def test_age_rotation_accepts_complete_cancelled_tool_result_without_rewriting_status():
    session = _make_bridge_session()
    items = [
        {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call", "output": "cancelled", "status": "incomplete"},
    ]
    candidate = _candidate(session, items)
    assert candidate.allows(session, now=4000)
    assert candidate.payload.input[: len(items)] == items


@pytest.mark.asyncio
async def test_age_rotation_does_not_require_local_or_durable_continuity():
    session = _make_bridge_session()
    candidate = _candidate(session)
    assert session.last_completed_response_id is None
    assert session.durable_session_id is None
    assert candidate.allows(session, now=4000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "items",
    [
        [{"type": "function_call_output", "call_id": "call", "output": "orphan"}],
        [{"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"}],
        [{"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"}] * 2,
        [
            {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call", "output": "first"},
            {"type": "function_call_output", "call_id": "call", "output": "duplicate"},
        ],
    ],
)
async def test_age_rotation_requires_exactly_paired_tool_calls(items):
    session = _make_bridge_session()
    candidate = _candidate(session, items)
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
async def test_young_socket_never_walks_payload(monkeypatch):
    from app.modules.proxy._service.http_bridge import connection_lifetime

    session = _make_bridge_session()
    candidate = _candidate(session)
    checker = Mock(side_effect=AssertionError("unexpected payload walk"))
    monkeypatch.setattr(connection_lifetime, "_complete_original_items", checker)
    assert not candidate.allows(session, now=20)
    checker.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["encrypted_function_args", "encrypted_arguments"])
async def test_age_rotation_rejects_opaque_function_arguments(field):
    session = _make_bridge_session()
    candidate = _candidate(
        session,
        [
            {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "", field: "opaque"},
            {"type": "function_call_output", "call_id": "call", "output": "complete"},
        ],
    )
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
async def test_age_owner_is_required_at_central_reconnect_even_after_soft_preference_is_cleared(monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.modules.proxy import service as proxy_service

    service = proxy_service.ProxyService(cast(Any, nullcontext()))
    session = _make_bridge_session()
    session.key = _HTTPBridgeSessionKey("prompt_cache", "age-key", None)
    session.affinity = proxy_service._AffinityPolicy()
    request_state = proxy_service._WebSocketRequestState(
        request_id="age-reconnect",
        model="gpt-5.4",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=service._clock.monotonic(),
        bridge_age_required_account_id=session.account.id,
        preferred_account_id=None,
        file_required_preferred_account=False,
    )
    other = SimpleNamespace(id="other", status=proxy_service.AccountStatus.ACTIVE, plan_type="plus")
    selection = AsyncMock(
        return_value=proxy_service.AccountSelection(account=cast(Any, other), error_message=None, error_code=None)
    )
    connect = AsyncMock()
    monkeypatch.setattr(service, "_select_account_with_budget_for_stream", selection)
    monkeypatch.setattr(service, "_open_upstream_websocket_with_budget", connect)
    with pytest.raises(proxy_service.ProxyResponseError) as caught:
        await service._reconnect_http_bridge_session(
            session,
            request_state=request_state,
            require_same_account=False,
            require_preferred_account=False,
            dashboard_settings=cast(
                Any,
                SimpleNamespace(
                    prefer_earlier_reset_accounts=False,
                    prefer_earlier_reset_window="secondary",
                    routing_strategy="usage_weighted",
                ),
            ),
        )
    assert caught.value.payload["error"]["code"] == "previous_response_owner_unavailable"
    assert selection.await_args is not None
    assert selection.await_args.kwargs["preferred_account_id"] == session.account.id
    assert selection.await_args.kwargs["fallback_on_preferred_account_unavailable"] is False
    connect.assert_not_awaited()
