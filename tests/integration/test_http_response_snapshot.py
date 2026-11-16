import json

import pytest

from app.core.types import JsonValue
from tests.integration.test_http_responses_bridge import _collect_sse_events
from tests.integration.test_http_responses_bridge import promotion_transport as promotion_transport

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
async def test_older_exact_response_cannot_trim_using_newer_independent_input(async_client, promotion_transport, path):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.http_downstream_transport_policy = "always_websocket"
    first_input = [{"role": "user", "content": [{"type": "input_text", "text": "turn A"}]}]
    body = {"model": "gpt-5.4", "stream": True, "prompt_cache_key": "synthetic-anchor-siblings", "input": first_input}
    first = await _collect_sse_events(async_client, path, json_body=body)
    assert first[-1]["type"] == "response.completed"
    older_id = first[-1]["response"]["id"]
    discovery = [
        {
            "type": "tool_search_call",
            "execution": "client",
            "call_id": "synthetic_search",
            "arguments": {"query": "synthetic_only_in_history"},
            "status": "completed",
        },
        {
            "type": "tool_search_output",
            "execution": "client",
            "call_id": "synthetic_search",
            "status": "completed",
            "tools": [{"type": "function", "name": "synthetic_only_in_history", "parameters": {"type": "object"}}],
        },
    ]
    second_input = [
        *first_input,
        *first[-1]["response"]["output"],
        *discovery,
        {"role": "user", "content": [{"type": "input_text", "text": "turn B"}]},
    ]
    second = await _collect_sse_events(async_client, path, json_body={**body, "input": second_input})
    assert second[-1]["type"] == "response.completed"
    assert len(sockets) == 1 and not raw_calls
    assert "previous_response_id" not in json.loads(sockets[0].sent_text[-1])
    supplied = [*second_input, {"role": "user", "content": [{"type": "input_text", "text": "turn C anchored to A"}]}]
    third = await _collect_sse_events(
        async_client, path, json_body={**body, "previous_response_id": older_id, "input": supplied}
    )
    assert third[-1]["type"] == "response.completed"
    sent = json.loads(sockets[0].sent_text[-1])
    assert sent["previous_response_id"] == older_id
    assert any(
        item.get("type") == "tool_search_output" and item.get("tools") == discovery[1]["tools"]
        for item in sent["input"]
    )
    assert any(
        part.get("text") == "turn B"
        for item in sent["input"]
        for part in item.get("content", [])
        if isinstance(part, dict)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
async def test_old_anchor_preserves_marked_newer_output_and_call_prefix(async_client, promotion_transport, path):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.http_downstream_transport_policy = "always_websocket"
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-marked-output",
        "input": [{"role": "user", "content": "anchor A"}],
    }
    first = await _collect_sse_events(async_client, path, json_body=body)
    second = await _collect_sse_events(
        async_client, path, json_body={**body, "input": [{"role": "user", "content": "independent branch B"}]}
    )
    assert first[-1]["type"] == second[-1]["type"] == "response.completed"
    prefix = [
        {
            "type": "message",
            "id": "msg_B",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "meaningful branch B"}],
        },
        {"type": "function_call", "id": "fc_B", "call_id": "call_B", "name": "synthetic_tool", "arguments": "{}"},
    ]
    supplied = [
        *prefix,
        {"type": "function_call_output", "call_id": "call_B", "output": "synthetic result"},
        {"role": "user", "content": [{"type": "input_text", "text": "continue B under old A"}]},
    ]
    third = await _collect_sse_events(
        async_client, path, json_body={**body, "previous_response_id": first[-1]["response"]["id"], "input": supplied}
    )
    assert third[-1]["type"] == "response.completed" and not raw_calls
    expected = [
        {k: v for k, v in item.items() if k != "id"} if item.get("type") in {"message", "function_call"} else item
        for item in supplied
    ]
    assert json.loads(sockets[0].sent_text[-1])["input"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
@pytest.mark.parametrize("change", ["none", "arguments", "metadata"])
async def test_marked_output_trim_requires_actual_anchor_output(
    async_client, promotion_transport, monkeypatch, path, change
):
    from tests.integration.test_http_responses_bridge import (
        _FakeBridgeUpstreamWebSocket,
        _FakeUpstreamMessage,
        proxy_module,
    )

    sockets, raw_calls, dashboard = promotion_transport
    dashboard.http_downstream_transport_policy = "always_websocket"
    owned: list[dict[str, JsonValue]] = [
        {
            "type": "message",
            "id": "msg_owned",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "synthetic owned output"}],
        },
        {
            "type": "function_call",
            "id": "fc_owned",
            "call_id": "call_owned",
            "name": "synthetic_tool",
            "arguments": '{"x":1}',
            "status": "completed",
        },
    ]

    class OwnedOutputSocket(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            if self.sent_text:
                await super().send_text(text)
                return
            self.sent_text.append(text)
            events = [{"type": "response.created", "response": {"id": "resp_owned", "status": "in_progress"}}]
            for index, item in enumerate(owned):
                for event_type in ["response.output_item.added", "response.output_item.done"]:
                    events.append(
                        {"type": event_type, "response_id": "resp_owned", "output_index": index, "item": item}
                    )
            events.append(
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_owned",
                        "object": "response",
                        "status": "completed",
                        "output": owned,
                    },
                }
            )
            for event in events:
                await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(event)))

    async def connect(*args, **kwargs):
        socket = OwnedOutputSocket("resp_followup")
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-covered-output",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "produce the synthetic call"}]}],
        "tools": [{"type": "function", "name": "synthetic_tool", "parameters": {"type": "object"}}],
    }
    first = await _collect_sse_events(async_client, path, json_body=body)
    assert first[-1]["type"] == "response.completed"
    echo: list[dict[str, JsonValue]] = [
        owned[0],
        {**owned[1], "arguments": '{"x":2}' if change == "arguments" else '{ "x": 1 }'},
    ]
    if change == "metadata":
        echo[1]["internal_chat_message_metadata_passthrough"] = {"capability": "must_preserve"}
    suffix = [
        {"type": "function_call_output", "call_id": "call_owned", "output": "done"},
        {"role": "user", "content": [{"type": "input_text", "text": "follow-up"}]},
    ]
    result = await _collect_sse_events(
        async_client, path, json_body={**body, "previous_response_id": "resp_owned", "input": [*echo, *suffix]}
    )
    assert result[-1]["type"] == "response.completed" and not raw_calls
    sent = json.loads(sockets[0].sent_text[-1])
    expected = [*echo, *suffix] if change != "none" else suffix
    # Existing wire normalization removes output item IDs; content/calls must survive.
    expected = [
        {k: v for k, v in item.items() if k != "id"} if item.get("type") in {"message", "function_call"} else item
        for item in expected
    ]
    assert sent["input"] == expected
