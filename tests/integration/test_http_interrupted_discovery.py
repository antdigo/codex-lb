from __future__ import annotations

import json

import pytest

from app.modules.proxy.input_contract import client_search_output_is_complete
from tests.integration.test_http_responses_bridge import (
    _collect_sse_events,
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
    proxy_module,
)
from tests.integration.test_http_responses_bridge import promotion_transport as promotion_transport

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
async def test_anchored_interruption_closes_search_with_native_empty_result(
    async_client, promotion_transport, monkeypatch, path
):
    sockets, raw_calls, _ = promotion_transport

    class DiscoverySocket(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            if self.sent_text:
                await super().send_text(text)
                return
            self.sent_text.append(text)
            call = {
                "type": "tool_search_call",
                "execution": "client",
                "call_id": "synthetic_search",
                "arguments": {"query": "synthetic"},
                "status": "completed",
            }
            response = {"id": "resp_discovery", "object": "response", "status": "completed", "output": [call]}
            for event in [
                {"type": "response.created", "response": {"id": "resp_discovery", "status": "in_progress"}},
                {
                    "type": "response.output_item.added",
                    "response_id": "resp_discovery",
                    "output_index": 0,
                    "item": call,
                },
                {"type": "response.output_item.done", "response_id": "resp_discovery", "output_index": 0, "item": call},
                {"type": "response.completed", "response": response},
            ]:
                await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(event)))

    async def connect(*args, **kwargs):
        socket = DiscoverySocket("resp_next")
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-discovery-interrupt",
        "tools": [{"type": "tool_search", "execution": "client"}],
        "input": [{"role": "user", "content": "discover a tool"}],
    }
    first = await _collect_sse_events(async_client, path, json_body=body)
    assert first[-1]["type"] == "response.completed"
    second = await _collect_sse_events(
        async_client,
        path,
        json_body={**body, "previous_response_id": "resp_discovery", "input": "continue without executing search"},
    )
    assert second[-1]["type"] == "response.completed"
    assert not raw_calls and len(sockets) == 1
    sent = json.loads(sockets[0].sent_text[-1])
    assert sent["previous_response_id"] == "resp_discovery"
    result = sent["input"][0]
    assert result["type"] == "tool_search_output"
    assert client_search_output_is_complete(result)
    assert result["call_id"] == "synthetic_search" and result["tools"] == []
    assert "output" not in result
