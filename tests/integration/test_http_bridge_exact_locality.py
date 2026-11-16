"""Two proved concurrent lanes: exact-repeat locality vs strict extension."""

import asyncio
import json

import pytest

from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonValue
from app.dependencies import get_proxy_service_for_app
from tests.integration.test_http_responses_bridge import (
    _collect_sse_events,
    _PromotionUpstreamWebSocket,
    proxy_module,
)
from tests.integration.test_http_responses_bridge import (
    promotion_transport as promotion_transport,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
@pytest.mark.parametrize("input_shape,extend", [("items", False), ("items", True), ("string", False)])
@pytest.mark.parametrize("proof", ["valid", "missing", "mismatch"])
async def test_exact_repeat_uses_its_proved_matching_warm_lane(
    async_client, app_instance, promotion_transport, monkeypatch, path, extend, proof, input_shape
):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.http_downstream_transport_policy = "always_websocket"
    monkeypatch.setattr(proxy_module, "_proxy_admission_wait_timeout_seconds", lambda: 10.0)
    first_sent, second_sent, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    sends = 0

    class HeldCompletion(_PromotionUpstreamWebSocket):
        async def send_text(self, text):
            nonlocal sends
            await super().send_text(text)
            sends += 1
            if sends == 1:
                first_sent.set()
            elif sends == 2:
                second_sent.set()

        async def receive(self):
            message = await super().receive()
            if message.text and json.loads(message.text).get("type") == "response.completed":
                await release.wait()
            return message

    async def connect(*args, **kwargs):
        socket = HeldCompletion(response_id_prefix=f"resp_locality_{len(sockets)}")
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "instructions": "synthetic",
        "prompt_cache_key": "synthetic-exact-two-lanes",
    }

    def history(label: str) -> list[JsonValue]:
        return [
            {"role": "user", "content": [{"type": "input_text", "text": label}]},
            {"role": "assistant", "content": [{"type": "output_text", "text": "first answer"}]},
            {"role": "user", "content": [{"type": "input_text", "text": "continue"}]},
        ]

    histories: list[JsonValue] = (
        [history("synthetic independent A"), history("synthetic independent B")]
        if input_shape == "items"
        else ["synthetic independent A", "synthetic independent B"]
    )
    tasks = [asyncio.create_task(_collect_sse_events(async_client, path, json_body={**body, "input": histories[0]}))]
    try:
        await asyncio.wait_for(first_sent.wait(), 5)
        tasks.append(
            asyncio.create_task(_collect_sse_events(async_client, path, json_body={**body, "input": histories[1]}))
        )
        await asyncio.wait_for(second_sent.wait(), 5)
        assert len(sockets) == 2 and sends == 2
        service = get_proxy_service_for_app(app_instance)
        coordinator = service._http_bridge_affinity_pool
        assert coordinator is not None
        sessions = [
            next(s for s in service._http_bridge_sessions.values() if s.upstream is socket) for socket in sockets
        ]
        refs = []
        for session in sessions:
            ref = coordinator.lane_reference(
                (session.key.affinity_kind, session.key.affinity_key, session.key.api_key_id)
            )
            assert ref is not None
            refs.append(ref)
        assert {ref.lane for ref in refs} == {0, 1}, "Must prove distinct logical lanes, not two sockets for lane0"
        assert refs[0].group == refs[1].group
        assert refs[0].account_id == refs[1].account_id
    finally:
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), 15)
    assert all(events[-1]["type"] == "response.completed" for events in results)
    if input_shape == "string":
        assert all(session.last_completed_input_count == 1 for session in sessions)
        assert all(session.last_completed_input_prefix_fingerprint is not None for session in sessions)
    if proof != "valid":
        sessions[1].last_completed_input_prefix_fingerprint = (
            None if proof == "missing" else "synthetic-mismatched-proof"
        )
    repeat = histories[1]
    if extend:
        assert isinstance(repeat, list)
        repeat = [
            *repeat,
            {"role": "assistant", "content": [{"type": "output_text", "text": "OK"}]},
            {"role": "user", "content": [{"type": "input_text", "text": "next"}]},
        ]
    result = await _collect_sse_events(async_client, path, json_body={**body, "input": repeat})
    assert result[-1]["type"] == "response.completed" and not raw_calls
    assert len(sockets) == 2
    selected = next(socket for socket in sockets if len(socket.sent_text) == 2)
    sent = json.loads(selected.sent_text[-1])
    expected_input = ResponsesRequest.model_validate({**body, "input": repeat}).to_payload()["input"]
    assert sent["input"] == expected_input and "previous_response_id" not in sent
    expected = sockets[1] if proof == "valid" else sockets[0]
    assert selected is expected, "Only a matching stored prefix may earn advisory locality preference"
