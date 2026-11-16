from __future__ import annotations

import json

import pytest

from app.core.clients.thread_cache_identity import ThreadCacheIdentity
from app.dependencies import get_proxy_service_for_app
from tests.integration.test_http_responses_bridge import (
    _ClosingBridgeUpstreamWebSocket,
    _collect_sse_events,
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
    _get_account,
    _import_account,
    _install_bridge_settings,
    _install_proxy_settings,
    _make_app_settings,
    _make_dashboard_settings,
    proxy_module,
)
from tests.integration.test_http_responses_bridge import (
    promotion_transport as promotion_transport,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1/responses", "/backend-api/codex/responses"])
@pytest.mark.parametrize("header", ["session_id", "thread-id"])
@pytest.mark.parametrize("promoted", [False, True])
async def test_independent_warm_broad_header_request_keeps_its_own_branch(
    async_client, app_instance, promotion_transport, path, header, promoted
):
    sockets, raw_calls, _ = promotion_transport
    history = [
        {"role": "user", "content": [{"type": "input_text", "text": "task"}]},
        {"role": "assistant", "content": [{"type": "output_text", "text": "first answer"}]},
        {"role": "user", "content": [{"type": "input_text", "text": "continue"}]},
    ]
    body = {
        "model": "gpt-5.4",
        "instructions": "branch A instructions",
        "input": history,
        "tools": [{"type": "function", "name": "branch_a_tool", "parameters": {"type": "object"}}],
        "stream": True,
    }
    headers = {header: "synthetic-shared-locality"}
    first = await _collect_sse_events(async_client, path, json_body=body, headers=headers)
    assert first[-1]["type"] == "response.completed"
    if promoted:
        service = get_proxy_service_for_app(app_instance)
        session = next(iter(service._http_bridge_sessions.values()))
        assert session.downstream_turn_state
        history = [
            *history,
            *first[-1]["response"]["output"],
            {"role": "user", "content": [{"type": "input_text", "text": "explicit next turn"}]},
        ]
        continuation = await _collect_sse_events(
            async_client,
            path,
            headers={**headers, "x-codex-turn-state": session.downstream_turn_state},
            json_body={**body, "previous_response_id": first[-1]["response"]["id"], "input": history},
        )
        assert continuation[-1]["type"] == "response.completed"
        assert json.loads(sockets[0].sent_text[-1])["previous_response_id"] == first[-1]["response"]["id"]
        # Pool-key preservation can bypass automatic promotion on this route.
        # Exercise the normal promotion method as well: lane history must never
        # grant continuation authority to the next independent request.
        service._promote_http_bridge_session_to_codex_affinity(session, turn_state=session.downstream_turn_state)
        assert session.codex_session
    second_history = [
        *history,
        {"role": "assistant", "content": [{"type": "output_text", "text": "independent branch B answer"}]},
        {"role": "user", "content": [{"type": "input_text", "text": "continue branch B"}]},
    ]
    tools = [{"type": "function", "name": "branch_b_tool", "parameters": {"type": "object"}}]
    second = await _collect_sse_events(
        async_client,
        path,
        headers=headers,
        json_body={**body, "input": second_history, "tools": tools, "instructions": "branch B instructions"},
    )
    assert second[-1]["type"] == "response.completed"
    assert not raw_calls and len(sockets) == 1
    frame = json.loads(sockets[0].sent_text[-1])
    assert "previous_response_id" not in frame
    assert frame["input"] == second_history
    assert frame["tools"] == tools
    assert frame["instructions"] == "branch B instructions"


@pytest.mark.asyncio
@pytest.mark.parametrize("catalog", ["omitted", "ordinary", "client-search"])
async def test_owner_recovery_retains_the_only_historical_discovery_definition(async_client, monkeypatch, catalog):
    _install_bridge_settings(monkeypatch, enabled=True)
    owner_id = await _import_account(async_client, "capability_owner", "capability-owner@example.com")
    owner = await _get_account(owner_id)
    owner_socket = _ClosingBridgeUpstreamWebSocket("resp_capability_owner")
    replacement = _FakeBridgeUpstreamWebSocket("resp_capability_replacement")

    async def fresh(self, account, **kwargs):
        return account

    async def connect(headers, access_token, account_id_header, **kwargs):
        return owner_socket if account_id_header == owner.chatgpt_account_id else replacement

    monkeypatch.setattr(proxy_module.ProxyService, "_ensure_fresh_with_budget", fresh)
    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    definition = {
        "type": "function",
        "name": "historically_discovered_only",
        "parameters": {"type": "object", "properties": {}},
        "defer_loading": True,
    }
    prefix = [
        {"role": "user", "content": "initial task"},
        {
            "type": "tool_search_call",
            "execution": "client",
            "call_id": "historical_search",
            "arguments": {"query": "historically_discovered_only"},
            "status": "completed",
        },
        {
            "type": "tool_search_output",
            "execution": "client",
            "call_id": "historical_search",
            "status": "completed",
            "tools": [definition],
        },
    ]
    body = {
        "model": "gpt-5.1",
        "instructions": "retain native capabilities",
        "input": prefix,
        "prompt_cache_key": "synthetic-capability-recovery",
        "stream": True,
    }
    if catalog == "ordinary":
        body["tools"] = [{"type": "function", "name": "another_tool", "parameters": {"type": "object"}}]
    elif catalog == "client-search":
        body["tools"] = [{"type": "tool_search", "execution": "client"}]
    first = await _collect_sse_events(async_client, "/backend-api/codex/responses", json_body=body)
    prior = first[-1]["response"]
    await _import_account(async_client, "capability_alternate", "capability-alternate@example.com")
    pause = await async_client.post(f"/api/accounts/{owner_id}/pause")
    assert pause.status_code == 200
    full = [*prefix, prior["output"][0], {"role": "user", "content": "use the discovered function"}]
    second = await _collect_sse_events(
        async_client,
        "/backend-api/codex/responses",
        json_body={**body, "input": full, "previous_response_id": prior["id"]},
    )
    assert second[-1]["type"] == "response.completed"
    assert len(owner_socket.sent_text) == len(replacement.sent_text) == 1
    frame = json.loads(replacement.sent_text[0])
    assert "previous_response_id" not in frame
    searches = [item for item in frame["input"] if item.get("type", "").startswith("tool_search_")]
    assert searches == prefix[1:]
    assert searches[-1]["tools"] == [definition]
    assert frame["prompt_cache_key"] == "synthetic-capability-recovery"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["shared", "isolated"])
@pytest.mark.parametrize("key_shape", ["short", "long", "suffix-like"])
async def test_opaque_same_account_replay_preserves_one_time_cache_identity(
    async_client, app_instance, monkeypatch, mode, key_shape
):
    settings = _make_app_settings(enabled=True).model_copy(update={"thread_cache_identity_mode": mode})
    dashboard = _make_dashboard_settings()
    dashboard.thread_cache_identity_mode = mode
    _install_proxy_settings(monkeypatch, app_settings=settings, dashboard_settings=dashboard)
    owner = await _get_account(await _import_account(async_client, "cache_owner", "cache-owner@example.com"))

    class OpaqueRestart(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            self.sent_text.append(text)
            for event in (
                {"type": "response.created", "response": {"id": "resp_cache_original", "status": "in_progress"}},
                {
                    "type": "response.output_item.added",
                    "response_id": "resp_cache_original",
                    "item": {"type": "reasoning", "id": "rs_cache", "summary": [], "encrypted_content": "opaque"},
                },
            ):
                await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(event)))
            await self._messages.put(_FakeUpstreamMessage("close", close_code=1012))

    original = OpaqueRestart("resp_cache_original")
    replacement = _FakeBridgeUpstreamWebSocket("resp_cache_winner")
    connections = []

    async def select(self, deadline, **kwargs):
        return proxy_module.AccountSelection(account=owner, error_code=None, error_message=None)

    async def fresh(self, account, **kwargs):
        return account

    async def connect(*args, **kwargs):
        connections.append(True)
        assert len(connections) <= 2
        return original if len(connections) == 1 else replacement

    monkeypatch.setattr(proxy_module.ProxyService, "_select_account_with_budget", select)
    monkeypatch.setattr(proxy_module.ProxyService, "_ensure_fresh_with_budget", fresh)
    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    raw_key = (
        "synthetic-cache"
        if key_shape == "short"
        else "synthetic-cache-" + "x" * 256
        if key_shape == "long"
        else "caller-" + ThreadCacheIdentity("isolated", owner.id).token
    )
    body = {
        "model": "gpt-6.1-sol",
        "instructions": "synthetic cache instructions",
        "prompt_cache_key": raw_key,
        "stream": True,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "synthetic task"}]}],
    }
    try:
        events = await _collect_sse_events(async_client, "/v1/responses", json_body=body)
        assert events[-1]["type"] == "response.completed"
        assert len(original.sent_text) == len(replacement.sent_text) == 1
        first, replay = json.loads(original.sent_text[0]), json.loads(replacement.sent_text[0])
        assert replay["prompt_cache_key"] == first["prompt_cache_key"]
        assert replay["input"] == first["input"]
        assert replay["instructions"] == first["instructions"]
        assert body["prompt_cache_key"] == raw_key
        if mode == "isolated":
            assert first["prompt_cache_key"] != raw_key
        else:
            assert first["prompt_cache_key"] == raw_key
    finally:
        service = get_proxy_service_for_app(app_instance)
        await service.close_all_http_bridge_sessions()
        await service.drain_persistence_tasks(timeout_seconds=5)
