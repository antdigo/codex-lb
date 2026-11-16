import asyncio
import json
from collections import deque
from copy import deepcopy
from typing import Any, cast
from unittest.mock import Mock

import pytest

from app.core.openai.requests import ResponsesRequest
from app.modules.proxy._service.http_bridge.connection_lifetime import _HTTPBridgeAgeRotation
from app.modules.proxy._service.http_bridge.request_submit import _send_http_bridge_request_text_with_archive_id
from app.modules.proxy._service.http_bridge.streaming import _new_http_operation
from app.modules.proxy._service.support import _HTTPBridgeSessionKey, _WebSocketRequestState
from app.modules.proxy.http_operation import ReplayKind
from tests.simulation.virtual_time import VirtualClock
from tests.unit.test_http_bridge_idle_leases import _make_bridge_session

pytestmark = pytest.mark.unit


def _client_search_history():
    return [
        {"role": "user", "content": "discover a synthetic function"},
        {
            "type": "tool_search_call",
            "execution": "client",
            "call_id": "search_one",
            "status": "completed",
            "arguments": {"query": "verify"},
        },
        {
            "type": "tool_search_output",
            "execution": "client",
            "call_id": "search_one",
            "tools": [{"type": "function", "name": "verify", "parameters": {"type": "object"}}],
        },
    ]


@pytest.mark.parametrize(
    "tools,eligible",
    [
        ([], True),
        ([{"type": "function", "name": "verify", "parameters": {"type": "object"}}], True),
        ([{"type": "tool_search", "execution": "client", "parameters": {"type": "object"}}], True),
        ([{"type": "tool_search", "execution": "server"}], False),
        ([{"type": "tool_search"}], False),
        ([{"type": "web_search"}], False),
        ([{"type": "computer_use_preview"}], False),
        ([{"type": "function"}], False),
        ([{"type": "function", "name": "verify", "parameters": "opaque"}], False),
        ([{"type": "tool_search", "execution": "client", "parameters": "opaque"}], False),
    ],
)
def test_original_tool_catalog_controls_operation_replay_authority(tools, eligible):
    payload = ResponsesRequest.model_validate(
        {"model": "gpt-5.4", "instructions": "", "input": _client_search_history()}
    )
    frame = dict(payload.to_payload())
    frame["tools"] = tools
    operation = _new_http_operation(payload, frame, 100.0)
    operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="owner", generation=1)
    grant = operation.authorize_replay(ReplayKind.TRANSPORT, now=2.0, account_id="owner", complete_input_required=True)
    assert (grant is not None) is eligible
    if eligible:
        assert operation.pending_dispatch is not None
        assert operation.pending_dispatch.required_account_id == "owner"
        assert operation.authorize_replay(ReplayKind.CAPACITY, now=3.0, account_id="owner") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("execution,eligible", [("client", True), ("server", False)])
async def test_wire_created_operation_uses_the_original_client_tool_contract(execution, eligible):
    session = _make_bridge_session()
    sent = []

    class Socket:
        async def send_text(self, text):
            sent.append(json.loads(text))

    session.upstream = cast(Any, Socket())
    state = _WebSocketRequestState(
        request_id="synthetic-discovery-wire",
        model="gpt-5.4",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=1.0,
        transport="http",
        event_queue=asyncio.Queue(),
    )
    frame = {
        "type": "response.create",
        "model": "gpt-5.4",
        "instructions": "",
        "input": _client_search_history(),
        "tools": [{"type": "tool_search", "execution": execution, "parameters": {"type": "object"}}],
        "prompt_cache_key": "synthetic-unchanged-cache",
    }
    await _send_http_bridge_request_text_with_archive_id(
        session, state, json.dumps(frame), clock=VirtualClock(monotonic_value=1, epoch_value=1)
    )
    operation = state.http_operation
    assert operation is not None
    grant = operation.authorize_replay(
        ReplayKind.TRANSPORT, now=2.0, account_id=session.account.id, complete_input_required=True
    )
    assert (grant is not None) is eligible
    assert len(sent) == 1
    assert sent[0]["input"] == frame["input"]
    assert sent[0]["tools"] == frame["tools"]
    assert sent[0]["prompt_cache_key"] == "synthetic-unchanged-cache"


@pytest.mark.asyncio
async def test_age_rotation_preserves_complete_client_tool_search_history():
    session = _make_bridge_session()
    items = _client_search_history()
    original = deepcopy(items)
    candidate = _candidate(session, items)
    assert candidate.allows(session, now=4000)
    assert candidate.payload.input == original
    assert items == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "hosted_call",
        "hosted_output",
        "missing_execution",
        "missing_call_id",
        "blank_call_id",
        "unmatched_output",
        "missing_output",
        "duplicate_output",
        "duplicate_call",
        "cross_kind_output",
        "cross_kind_call",
        "in_progress_call",
        "incomplete_call",
        "malformed_arguments",
        "missing_arguments",
        "missing_tools",
        "non_list_tools",
        "hosted_loaded_tool",
        "unknown_loaded_tool",
        "missing_function_name",
        "malformed_function_parameters",
    ],
)
async def test_age_rotation_rejects_unproven_client_tool_search(fault):
    items = _client_search_history()
    call, output = items[1:]
    if fault == "hosted_call":
        call["execution"] = "server"
    elif fault == "hosted_output":
        output["execution"] = "server"
    elif fault == "missing_execution":
        del call["execution"]
    elif fault == "missing_call_id":
        del call["call_id"]
    elif fault == "blank_call_id":
        call["call_id"] = " "
    elif fault == "unmatched_output":
        output["call_id"] = "another_search"
    elif fault == "missing_output":
        items.pop()
    elif fault == "duplicate_output":
        items.append(deepcopy(output))
    elif fault == "duplicate_call":
        items.insert(2, deepcopy(call))
    elif fault == "cross_kind_output":
        output.update(type="function_call_output", output="different contract")
    elif fault == "cross_kind_call":
        call.update(type="function_call", name="verify", arguments="{}")
    elif fault in {"in_progress_call", "incomplete_call"}:
        call["status"] = "in_progress" if fault == "in_progress_call" else "incomplete"
    elif fault == "malformed_arguments":
        call["arguments"] = "not an object"
    elif fault == "missing_arguments":
        del call["arguments"]
    elif fault == "missing_tools":
        del output["tools"]
    elif fault == "non_list_tools":
        output["tools"] = {"type": "function", "name": "verify"}
    elif fault in {"hosted_loaded_tool", "unknown_loaded_tool"}:
        output["tools"] = [{"type": "web_search" if fault == "hosted_loaded_tool" else "unknown"}]
    elif fault == "missing_function_name":
        output["tools"] = [{"type": "function", "parameters": {"type": "object"}}]
    elif fault == "malformed_function_parameters":
        output["tools"] = [{"type": "function", "name": "verify", "parameters": "opaque"}]
    session = _make_bridge_session()
    candidate = _candidate(session, items)
    assert not candidate.allows(session, now=4000)


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
async def test_age_rotation_allows_own_unsent_pool_head_but_preserves_other_waiters():
    session = _make_bridge_session()
    candidate = _candidate(session)
    session.key = _HTTPBridgeSessionKey("internal_pool_lane", "group.3", None)
    candidate.key = session.key
    session.pool_waiter_count = 1
    assert not candidate.allows(session, now=4000)
    candidate.own_pool_waiters = 1
    assert candidate.allows(session, now=4000)
    session.pool_waiter_count = 2
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
@pytest.mark.parametrize(
    "output", ["", [{"type": "input_text", "text": "complete"}], [{"type": "input_text", "text": ""}]]
)
async def test_age_rotation_accepts_complete_native_text_tool_output(output):
    session = _make_bridge_session()
    items = [
        {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call", "output": output, "status": "incomplete"},
    ]
    candidate = _candidate(session, items)
    assert candidate.allows(session, now=4000)
    assert candidate.payload.input == items


@pytest.mark.asyncio
@pytest.mark.parametrize("encrypted", ["absent", None])
async def test_age_rotation_accepts_full_plaintext_reasoning_without_rewriting(encrypted):
    session = _make_bridge_session()
    item = {
        "type": "reasoning",
        "id": "reason_plain",
        "summary": [],
        "content": [{"type": "reasoning_text", "text": "complete native text"}],
    }
    if encrypted != "absent":
        item["encrypted_content"] = encrypted
    candidate = _candidate(session, [item])
    assert candidate.allows(session, now=4000)
    assert candidate.payload.input == [item]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        [],
        [{"type": "input_text"}],
        [{"type": "input_text", "text": 1}],
        [{"type": "input_image", "image_url": "https://example.invalid/image"}],
        [{"type": "tool_reference", "tool_name": "tool"}],
    ],
)
async def test_age_rotation_rejects_incomplete_or_opaque_tool_content(content):
    session = _make_bridge_session()
    items = [
        {"type": "function_call", "call_id": "call", "name": "tool", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call", "output": content},
    ]
    assert not _candidate(session, items).allows(session, now=4000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "patch",
    [
        {"id": ""},
        {"content": []},
        {"content": [{"type": "reasoning_text"}]},
        {"content": [{"type": "reasoning_text", "text": ""}]},
        {"content": [{"type": "summary_text", "text": "summary only"}]},
        {"encrypted_content": ""},
        {"encrypted_content": 7},
        {"status": "in_progress"},
    ],
)
async def test_age_rotation_rejects_incomplete_or_malformed_plaintext_reasoning(patch):
    session = _make_bridge_session()
    item = {
        "type": "reasoning",
        "id": "reason_plain",
        "content": [{"type": "reasoning_text", "text": "complete native text"}],
        **patch,
    }
    assert not _candidate(session, [item]).allows(session, now=4000)


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


@pytest.mark.asyncio
async def test_independent_pool_lane_rotates_at_safe_physical_boundary():
    session = _make_bridge_session()
    candidate = _candidate(session)
    session.key = _HTTPBridgeSessionKey("internal_pool_lane", "group.3", None)
    candidate.key = session.key
    assert candidate.allows(session, now=4000)
    session.queued_request_count = 1
    assert not candidate.allows(session, now=4000)


@pytest.mark.asyncio
async def test_header_pool_rotation_requires_independent_complete_input_proof():
    session = _make_bridge_session()
    candidate = _candidate(session)
    session.key = _HTTPBridgeSessionKey("internal_pool_lane", "broad-header.2", None)
    candidate.key = session.key
    session.codex_session = True
    assert not candidate.allows(session, now=4000)
    candidate.independent_pool_lane = True
    assert candidate.allows(session, now=4000)
    candidate.payload = candidate.payload.model_copy(update={"previous_response_id": "exact-owner"})
    assert not candidate.allows(session, now=4000)
    candidate.payload = candidate.payload.model_copy(
        update={
            "previous_response_id": None,
            "input": [
                {"role": "user", "content": [{"type": "input_file", "file_id": "owner-file"}]},
            ],
        }
    )
    assert not candidate.allows(session, now=4000)
