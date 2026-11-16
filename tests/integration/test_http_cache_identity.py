import json

import pytest

from app.dependencies import get_proxy_service_for_app
from tests.integration.test_http_responses_bridge import (
    _collect_sse_events,
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
    proxy_module,
)
from tests.integration.test_http_responses_bridge import promotion_transport as promotion_transport
from tests.unit.test_thread_cache_identity import loopback as loopback

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
@pytest.mark.parametrize(
    "fleet,override,isolated",
    [
        ("shared", "isolated", True),
        ("isolated", "shared", False),
        ("shared", None, False),
        ("isolated", None, True),
    ],
)
async def test_bridge_respects_effective_api_key_cache_identity(
    async_client, promotion_transport, fleet, override, isolated, path
):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = fleet
    dashboard.http_downstream_transport_policy = "always_websocket"
    assert (await async_client.put("/api/settings", json={"apiKeyAuthEnabled": True})).status_code == 200
    created = await async_client.post(
        "/api/api-keys/",
        json={
            "name": "synthetic-affinity-review",
            "threadCacheIdentityOverride": override,
        },
    )
    assert created.status_code == 200, created.text
    key = created.json()
    assert key["threadCacheIdentityOverride"] == override
    dashboard.api_key_auth_enabled = True
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-stable-key",
        "instructions": "synthetic stable instructions",
        "input": [{"role": "user", "content": "synthetic task"}],
    }
    events = await _collect_sse_events(
        async_client, path, json_body=body, headers={"Authorization": f"Bearer {key['key']}"}
    )
    assert events[-1]["type"] == "response.completed"
    assert not raw_calls and len(sockets) == 1
    sent = json.loads(sockets[0].sent_text[0])
    if isolated:
        assert sent["prompt_cache_key"] != "synthetic-stable-key"
        assert sent["instructions"].startswith("codex-lb-cache-scope: ")
    else:
        assert sent["prompt_cache_key"] == "synthetic-stable-key"
        assert sent["instructions"] == "synthetic stable instructions"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
async def test_isolated_bridge_scopes_present_handshake_locality_headers(
    async_client, promotion_transport, monkeypatch, path
):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = "isolated"
    dashboard.http_downstream_transport_policy = "always_websocket"
    handshakes = []

    async def connect(headers, *args, **kwargs):
        handshakes.append(dict(headers))
        socket = _FakeBridgeUpstreamWebSocket("resp_synthetic_header")
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    headers = {"session_id": "synthetic-session-locality", "thread-id": "synthetic-thread-locality"}
    events = await _collect_sse_events(
        async_client,
        path,
        headers=headers,
        json_body={
            "model": "gpt-5.4",
            "stream": True,
            "prompt_cache_key": "synthetic-stable-key",
            "input": [{"role": "user", "content": "synthetic task"}],
        },
    )
    assert events[-1]["type"] == "response.completed" and not raw_calls
    actual = {k.lower(): v for k, v in handshakes[0].items()}
    assert "session_id" in actual or "thread-id" in actual
    for name, raw in headers.items():
        if name in actual:
            assert actual[name] != raw


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["shared", "isolated"])
async def test_http_operation_cache_policy_is_stable_across_reconnect(
    async_client, promotion_transport, monkeypatch, mode
):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = mode
    dashboard.http_downstream_transport_policy = "always_websocket"
    handshakes = []

    class OpaqueRestart(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            self.sent_text.append(text)
            dashboard.thread_cache_identity_mode = "isolated" if mode == "shared" else "shared"
            for event in [
                {"type": "response.created", "response": {"id": "resp_policy_first", "status": "in_progress"}},
                {
                    "type": "response.output_item.added",
                    "response_id": "resp_policy_first",
                    "item": {
                        "type": "reasoning",
                        "id": "rs_synthetic_policy",
                        "summary": [],
                        "encrypted_content": "opaque",
                    },
                },
            ]:
                await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(event)))
            await self._messages.put(_FakeUpstreamMessage("close", close_code=1012))

    async def connect(headers, *args, **kwargs):
        assert len(sockets) < 2
        handshakes.append(dict(headers))
        socket = (
            OpaqueRestart("resp_policy_first") if not sockets else _FakeBridgeUpstreamWebSocket("resp_policy_winner")
        )
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    events = await _collect_sse_events(
        async_client,
        "/v1/responses",
        headers={"session_id": "synthetic-policy-session"},
        json_body={
            "model": "gpt-6.1-sol",
            "stream": True,
            "prompt_cache_key": "synthetic-policy-key",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "synthetic policy task"}]}],
        },
    )
    assert events[-1]["type"] == "response.completed"
    assert len(sockets) == 2 and not raw_calls
    first, retry = [json.loads(socket.sent_text[0]) for socket in sockets]
    assert first["prompt_cache_key"] == retry["prompt_cache_key"]
    assert first["input"] == retry["input"]
    assert handshakes[0].get("session_id") == handshakes[1].get("session_id")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["shared", "isolated"])
async def test_new_policy_uses_a_compatible_independent_connection(
    async_client, promotion_transport, monkeypatch, mode
):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = mode
    dashboard.http_downstream_transport_policy = "always_websocket"
    handshakes = []

    async def connect(headers, *args, **kwargs):
        handshakes.append(dict(headers))
        socket = _FakeBridgeUpstreamWebSocket(f"resp_policy_{len(sockets)}")
        sockets.append(socket)
        return socket

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-policy-key",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "synthetic task"}]}],
    }
    headers = {"session_id": "synthetic-policy-session"}
    first = await _collect_sse_events(async_client, "/v1/responses", headers=headers, json_body=body)
    assert first[-1]["type"] == "response.completed"
    dashboard.thread_cache_identity_mode = "isolated" if mode == "shared" else "shared"
    second = await _collect_sse_events(async_client, "/v1/responses", headers=headers, json_body=body)
    assert second[-1]["type"] == "response.completed" and not raw_calls
    assert len(sockets) == 2
    assert handshakes[0]["session_id"] != handshakes[1]["session_id"]
    assert "previous_response_id" not in json.loads(sockets[1].sent_text[0])


@pytest.mark.asyncio
async def test_exact_anchor_policy_change_refuses_before_an_incompatible_send(async_client, promotion_transport):
    sockets, raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = "shared"
    dashboard.http_downstream_transport_policy = "always_websocket"
    body = {
        "model": "gpt-5.4",
        "stream": True,
        "prompt_cache_key": "synthetic-policy-key",
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "synthetic task"}]}],
    }
    first = await _collect_sse_events(async_client, "/v1/responses", json_body=body)
    dashboard.thread_cache_identity_mode = "isolated"
    response = await async_client.post(
        "/v1/responses",
        json={
            **body,
            "previous_response_id": first[-1]["response"]["id"],
            "input": "explicit continuation after policy change",
        },
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "cache_identity_policy_changed"
    assert len(sockets) == 1 and len(sockets[0].sent_text) == 1 and not raw_calls


@pytest.mark.asyncio
async def test_raw_handoff_uses_frozen_policy_in_actual_http_wire(
    async_client, app_instance, promotion_transport, monkeypatch, loopback
):
    import time

    from app.core.clients import proxy as core_proxy
    from app.core.clients.thread_cache_identity import ThreadCachePolicy
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy.http_operation import DecisionAction, HandoffObservation, OperationController

    _sockets, _raw_calls, dashboard = promotion_transport
    dashboard.thread_cache_identity_mode = "shared"
    monkeypatch.setattr(proxy_module, "core_stream_responses", core_proxy.stream_responses)
    service = get_proxy_service_for_app(app_instance)
    operation = OperationController(
        time.monotonic() + 30,
        complete_input=True,
        client_tools_only=True,
        cache_policy=ThreadCachePolicy("isolated"),
    )
    assert operation.decide(HandoffObservation(time.monotonic(), proven_unsent=True)).action is DecisionAction.HANDOFF
    body = ResponsesRequest.model_validate(
        {
            "model": "gpt-5.4",
            "instructions": "",
            "prompt_cache_key": "synthetic-handoff-key",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "synthetic handoff"}]}],
        }
    )
    try:
        lines = [
            line
            async for line in service._stream_with_retry(
                body,
                {"session_id": "synthetic-handoff-session"},
                codex_session_affinity=False,
                propagate_http_errors=True,
                openai_cache_affinity=True,
                api_key=None,
                api_key_reservation=None,
                suppress_text_done_events=False,
                request_transport="http",
                upstream_stream_transport_override="http",
                http_operation=operation,
            )
        ]
        assert any("response.completed" in line for line in lines)
        assert len(loopback) == 1
        wire = json.loads(loopback[0].body)
        assert wire["prompt_cache_key"] != "synthetic-handoff-key"
        assert wire["input"][0]["content"][0]["text"].startswith("codex-lb-cache-scope: ")
        headers = {k.lower(): v for k, v in loopback[0].headers}
        assert headers["session_id"] != "synthetic-handoff-session"
        assert body.prompt_cache_key == "synthetic-handoff-key"
        assert len(operation.attempts) == 1
    finally:
        operation.close()
