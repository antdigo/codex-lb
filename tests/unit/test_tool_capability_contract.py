from __future__ import annotations

from copy import deepcopy

import pytest

from app.core.types import JsonValue
from app.modules.proxy._service.http_bridge.upstream_events import (
    _durable_pending_tool_call_manifest,
    _record_http_bridge_tool_call_lifecycle,
    _response_completed_tool_call_types,
)
from app.modules.proxy._service.support import _WebSocketRequestState
from app.modules.proxy.replay_safety import (
    project_responses_input_for_account_neutral_fresh_replay,
    responses_input_suffix_matches_pending_tool_calls,
    responses_payload_is_account_neutral_fresh_replay,
)

pytestmark = pytest.mark.unit


def discovery_history(execution: str | None = "client") -> list[JsonValue]:
    return [
        {"role": "user", "content": "discover a synthetic tool"},
        {
            "type": "tool_search_call",
            "execution": execution,
            "call_id": "search_one" if execution == "client" else None,
            "arguments": {"query": "dynamic_function"},
            "status": "completed",
        },
        {
            "type": "tool_search_output",
            "execution": execution,
            "call_id": "search_one" if execution == "client" else None,
            "status": "completed",
            "tools": [
                {
                    "type": "function",
                    "name": "dynamic_function",
                    "parameters": {"type": "object", "properties": {}},
                    "defer_loading": True,
                }
            ],
        },
    ]


def test_portable_discovery_definitions_remain_meaningful_input():
    prefix = discovery_history()
    original = deepcopy(prefix)
    history: list[JsonValue] = [
        *prefix,
        {"role": "assistant", "content": [{"type": "output_text", "text": "prior answer"}]},
        {"role": "user", "content": "use it"},
    ]
    projection = project_responses_input_for_account_neutral_fresh_replay(history, stored_count=len(prefix))
    assert projection is not None
    assert projection.input_items == history
    assert projection.stored_prefix_count == len(prefix)
    assert responses_payload_is_account_neutral_fresh_replay({"input": projection.input_items})
    assert prefix == original


@pytest.mark.parametrize("execution", ["server", None, "unknown"])
def test_unproven_discovery_cannot_disappear_to_manufacture_portability(execution):
    prefix = discovery_history(execution)
    history = [*prefix, {"role": "assistant", "content": "prior answer"}, {"role": "user", "content": "use it"}]
    projection = project_responses_input_for_account_neutral_fresh_replay(history, stored_count=len(prefix))
    if projection is not None:
        assert projection.input_items == history
        assert not responses_payload_is_account_neutral_fresh_replay({"input": projection.input_items})


def _state():
    return _WebSocketRequestState(
        request_id="synthetic-client-obligations",
        model="gpt-6.1-sol",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=1.0,
    )


def _mixed_calls():
    return [
        {"type": "function_call", "call_id": "function_one", "name": "verify", "arguments": "{}"},
        {
            "type": "tool_search_call",
            "execution": "client",
            "call_id": "search_one",
            "arguments": {"query": "dynamic_function"},
            "status": "completed",
        },
    ]


def test_every_client_obligation_survives_terminal_manifest_classification():
    assert _response_completed_tool_call_types({"response": {"output": _mixed_calls()}}) == {
        "function_one": "function_call",
        "search_one": "tool_search_call",
    }


def test_mixed_durable_manifest_requires_the_search_result_too():
    state = _state()
    calls = _mixed_calls()
    for event in ("response.output_item.added", "response.output_item.done"):
        for call in calls:
            _record_http_bridge_tool_call_lifecycle(state, event_type=event, payload={"item": call})
    manifest = _durable_pending_tool_call_manifest(state, {"response": {"output": calls}})
    assert manifest == {"function_one": "function_call", "search_one": "tool_search_call"}
    prefix = [{"role": "user", "content": "synthetic task"}]
    function_result = {"type": "function_call_output", "call_id": "function_one", "output": "verified"}
    partial = [*prefix, calls[0], function_result]
    assert not responses_input_suffix_matches_pending_tool_calls(partial, stored_count=1, pending_tool_calls=manifest)
    complete = [*prefix, *calls, function_result, discovery_history()[2]]
    assert responses_input_suffix_matches_pending_tool_calls(complete, stored_count=1, pending_tool_calls=manifest)


def test_partial_manifest_cannot_authorize_a_resend_missing_parallel_search():
    state = _state()
    calls = _mixed_calls()
    for event in ("response.output_item.added", "response.output_item.done"):
        for call in calls:
            _record_http_bridge_tool_call_lifecycle(state, event_type=event, payload={"item": call})
    manifest = _durable_pending_tool_call_manifest(state, {"response": {"output": calls}})
    partial = [
        {"role": "user", "content": "synthetic task"},
        calls[0],
        {"type": "function_call_output", "call_id": "function_one", "output": "verified"},
    ]
    assert manifest is None or not responses_input_suffix_matches_pending_tool_calls(
        partial, stored_count=1, pending_tool_calls=manifest
    )


@pytest.mark.parametrize(
    "kind", ["tool_search_call", "unknown_client_call", "computer_call", "mcp_approval_request", "tool_search_output"]
)
def test_unrepresentable_parallel_client_work_invalidates_the_entire_manifest(kind):
    state = _state()
    calls = [_mixed_calls()[0], {"type": kind, "execution": "unknown", "call_id": "other_obligation"}]
    for event in ("response.output_item.added", "response.output_item.done"):
        for call in calls:
            _record_http_bridge_tool_call_lifecycle(state, event_type=event, payload={"item": call})
    assert _durable_pending_tool_call_manifest(state, {"response": {"output": calls}}) is None


def test_loaded_function_schema_property_names_are_not_provider_resources():
    history = discovery_history()
    output = history[2]
    assert isinstance(output, dict)
    tools = output["tools"]
    assert isinstance(tools, list)
    tool = tools[0]
    assert isinstance(tool, dict)
    tool["parameters"] = {
        "type": "object",
        "properties": {"file_id": {"type": "string"}, "vector_store_id": {"type": "string"}},
    }
    assert responses_payload_is_account_neutral_fresh_replay({"input": history})


@pytest.mark.parametrize(
    "definition",
    [
        {"type": "mcp", "server_label": "synthetic"},
        {"type": "namespace", "name": "synthetic", "tools": []},
        {"type": "function", "name": "synthetic", "file_id": "provider-owned"},
    ],
)
def test_unknown_or_account_owned_loaded_definitions_cannot_be_transferred(definition):
    history = discovery_history()
    output = history[2]
    assert isinstance(output, dict)
    output["tools"] = [definition]
    original = deepcopy(history)
    projection = project_responses_input_for_account_neutral_fresh_replay(history, stored_count=3)
    assert projection is not None and projection.input_items == history
    assert not responses_payload_is_account_neutral_fresh_replay({"input": projection.input_items})
    assert history == original


def test_terminal_only_search_does_not_manufacture_an_observed_call_manifest():
    assert _durable_pending_tool_call_manifest(_state(), {"response": {"output": _mixed_calls()}}) is None


def test_duplicate_call_id_across_search_and_function_invalidates_manifest():
    state = _state()
    calls = _mixed_calls()
    calls[1]["call_id"] = calls[0]["call_id"]
    for event in ("response.output_item.added", "response.output_item.done"):
        for call in calls:
            _record_http_bridge_tool_call_lifecycle(state, event_type=event, payload={"item": call})
    assert _durable_pending_tool_call_manifest(state, {"response": {"output": calls}}) is None


def test_known_hosted_search_is_not_a_client_obligation():
    state = _state()
    calls = [_mixed_calls()[0], {"type": "tool_search_call", "execution": "server", "status": "completed"}]
    for event in ("response.output_item.added", "response.output_item.done"):
        for call in calls:
            _record_http_bridge_tool_call_lifecycle(state, event_type=event, payload={"item": call})
    assert _durable_pending_tool_call_manifest(state, {"response": {"output": calls}}) == {
        "function_one": "function_call"
    }
