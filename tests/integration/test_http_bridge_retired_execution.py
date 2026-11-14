import asyncio
import json

import pytest

from app.core.clients.proxy import ProxyResponseError
from app.core.clock import RealClock
from app.core.errors import openai_error
from app.modules.proxy import service as proxy_module
from tests.integration.test_http_responses_bridge import (
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
    _get_account,
    _import_account,
    _install_bridge_settings_with_limits,
    get_proxy_service_for_app,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("retirement", ["closed", "draining"])
@pytest.mark.parametrize(
    "outcome",
    [
        "success",
        "owner_unavailable",
        "repeat_retirement",
        "expired",
        "late_expired",
        "authenticated",
        "prepared",
        "concurrent",
        "cancelled",
        "retired_during_preparation",
        "retired_after_admission",
        "reader_closed_during_preparation",
        "lifetime_after_send",
    ],
)
async def test_public_http_waiter_moves_to_one_same_account_replacement(
    async_client, app_instance, monkeypatch, retirement, outcome
):
    _install_bridge_settings_with_limits(monkeypatch, enabled=True, max_sessions=128)
    owner = await _get_account(await _import_account(async_client, "waiter_owner", "waiter-owner@example.com"))
    service = get_proxy_service_for_app(app_instance)

    class OffsetClock(RealClock):
        offset = 0.0

        def monotonic(self):
            return super().monotonic() + self.offset

    clock = OffsetClock()
    service._clock = clock
    auth_headers = {}
    if outcome == "authenticated":
        response = await async_client.put("/api/settings", json={"apiKeyAuthEnabled": True})
        assert response.status_code == 200
        response = await async_client.post(
            "/api/api-keys/", json={"name": "retired-waiter", "weeklyTokenLimit": 1000000}
        )
        assert response.status_code == 200
        auth_headers = {"Authorization": "Bearer " + response.json()["key"]}
    old_socket = _FakeBridgeUpstreamWebSocket("resp_warm")
    replacement_socket = _FakeBridgeUpstreamWebSocket("resp_replacement")
    if outcome == "lifetime_after_send":

        class LifetimeRejectedSocket(_FakeBridgeUpstreamWebSocket):
            async def send_text(self, text):
                self.sent_text.append(text)
                await self._messages.put(
                    _FakeUpstreamMessage(
                        "text",
                        text=json.dumps(
                            {
                                "type": "error",
                                "status": 400,
                                "error": {
                                    "type": "invalid_request_error",
                                    "code": "websocket_connection_limit_reached",
                                    "message": (
                                        "Responses websocket connection limit reached (60 minutes). "
                                        "Create a new websocket connection to continue."
                                    ),
                                },
                            }
                        ),
                    )
                )

        replacement_socket = LifetimeRejectedSocket("resp_rejected")
    replacement_sockets = [replacement_socket]
    selections, connects = [], []

    async def select(self, deadline, **kwargs):
        selections.append(kwargs)
        if len(selections) > 1 and outcome == "owner_unavailable":
            return proxy_module.AccountSelection(
                account=None, error_code="continuity_owner_unavailable", error_message="Owner unavailable"
            )
        return proxy_module.AccountSelection(account=owner, error_code=None, error_message=None)

    async def fresh(self, account, **kwargs):
        return account

    async def connect(self, account, headers, **kwargs):
        connects.append(account.id)
        assert len(connects) <= (
            3
            if outcome
            in (
                "concurrent",
                "retired_during_preparation",
                "retired_after_admission",
                "reader_closed_during_preparation",
                "lifetime_after_send",
            )
            else 2
        )
        if len(connects) == 1:
            return old_socket
        if len(connects) == 3:
            replacement_sockets.append(_FakeBridgeUpstreamWebSocket("resp_replacement_second"))
        return replacement_sockets[-1]

    monkeypatch.setattr(proxy_module.ProxyService, "_select_account_with_budget", select)
    monkeypatch.setattr(proxy_module.ProxyService, "_ensure_fresh_with_budget", fresh)
    monkeypatch.setattr(proxy_module.ProxyService, "_open_upstream_websocket_with_budget", connect)
    body = {
        "model": "gpt-6.1-sol",
        "prompt_cache_key": "synthetic-retiring-waiter",
        "stream": True,
        "input": [
            {"role": "user", "content": "synthetic context"},
            {"type": "reasoning", "id": "rs_synthetic", "encrypted_content": "synthetic-ciphertext"},
            {"type": "function_call", "call_id": "call_synthetic", "name": "test_tool", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call_synthetic", "output": "synthetic result"},
        ],
    }
    warm = await async_client.post("/v1/responses", json=body, headers=auth_headers)
    assert warm.status_code == 200 and '"type":"response.completed"' in warm.text
    old = next(s for s in service._http_bridge_sessions.values() if s.key.affinity_kind == "prompt_cache")
    await old.execution_gate.acquire()
    waiting = asyncio.Event()
    acquire = service._acquire_request_state_response_create_admission
    reservations = []
    waiters_seen = 0

    async def observe(state, **kwargs):
        nonlocal waiters_seen
        target = kwargs.get("bridge_session")
        assert target is not None
        reservations.append(state.api_key_reservation)
        if target is old:
            waiters_seen += 1
            if waiters_seen == (2 if outcome == "concurrent" else 1):
                waiting.set()
        elif outcome == "repeat_retirement":
            target.closed = True
            raise ProxyResponseError(
                503,
                openai_error("response_execution_session_retired", "Retired before dispatch"),
                local_pre_dispatch_refusal=True,
            )
        result = await acquire(state, **kwargs)
        if target is not old and outcome == "late_expired":
            clock.offset += 10000
        if target is not old and outcome == "retired_after_admission":
            target.closed = True
        return result

    if outcome == "prepared":
        prepared_texts = []

        async def prepare_snapshot(text, state):
            if not prepared_texts:
                text = text.replace("synthetic context", "synthetic prepared snapshot")
            prepared_texts.append(text)
            state.request_text = text
            return text

        monkeypatch.setattr(service, "_inline_http_bridge_image_urls", prepare_snapshot)

    if outcome in ("retired_during_preparation", "reader_closed_during_preparation"):
        allowed = service._http_bridge_precreated_retry_allowed
        retired_again = False

        async def close_replacement_during_preparation(session, **kwargs):
            nonlocal retired_again
            result = await allowed(session, **kwargs)
            if session is not old and not retired_again:
                retired_again = True
                if outcome == "reader_closed_during_preparation":
                    await session.upstream._messages.put(_FakeUpstreamMessage("close", close_code=1000))
                    assert session.upstream_reader is not None
                    await asyncio.wait_for(asyncio.shield(session.upstream_reader), timeout=5)
                    assert session.closed
                else:
                    session.closed = True
            return result

        monkeypatch.setattr(service, "_http_bridge_precreated_retry_allowed", close_replacement_during_preparation)

    monkeypatch.setattr(service, "_acquire_request_state_response_create_admission", observe)
    requests = [
        asyncio.create_task(async_client.post("/v1/responses", json=body, headers=auth_headers))
        for _ in range(2 if outcome == "concurrent" else 1)
    ]
    try:
        await asyncio.wait_for(waiting.wait(), timeout=5)
        await asyncio.sleep(0)
        if retirement == "closed":
            old.closed = True
        else:
            old.upstream_control.retire_after_drain = True
        if outcome == "expired":
            clock.offset += 10000
        if outcome == "cancelled":
            for request in requests:
                request.cancel()
            await asyncio.gather(*requests, return_exceptions=True)
            assert len(old_socket.sent_text) == 1 and not replacement_socket.sent_text
            assert connects == [owner.id]
            return
        old.execution_gate.release()
        responses = await asyncio.wait_for(asyncio.gather(*requests), timeout=10)
        response = responses[0]
    finally:
        if old.execution_gate.locked():
            old.execution_gate.release()
        for request in requests:
            if not request.done():
                request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
    assert len(old_socket.sent_text) == 1
    if outcome == "lifetime_after_send":
        assert response.status_code == 200 and '"type":"response.completed"' in response.text
        assert connects == [owner.id, owner.id, owner.id]
        assert len(replacement_sockets) == 2
        assert all(len(socket.sent_text) == 1 for socket in replacement_sockets)
        assert all(json.loads(socket.sent_text[0])["input"] == body["input"] for socket in replacement_sockets)
        assert all(
            s["preferred_account_id"] == owner.id and not s["fallback_on_preferred_account_unavailable"]
            for s in selections[1:]
        )
        return
    if outcome == "concurrent":
        assert all(r.status_code == 200 and '"type":"response.completed"' in r.text for r in responses)
        assert sum(len(s.sent_text) for s in replacement_sockets) == 2
        assert all(x == owner.id for x in connects)
        assert all(
            s["preferred_account_id"] == owner.id and not s["fallback_on_preferred_account_unavailable"]
            for s in selections[1:]
        )
        return
    if outcome == "expired":
        assert len(selections) == 1
        assert not replacement_socket.sent_text
        return
    assert len(selections) == 2
    assert selections[-1]["preferred_account_id"] == owner.id
    assert selections[-1]["fallback_on_preferred_account_unavailable"] is False
    if outcome == "owner_unavailable":
        assert connects == [owner.id]
        assert not replacement_socket.sent_text
    elif outcome in (
        "repeat_retirement",
        "late_expired",
        "retired_during_preparation",
        "retired_after_admission",
        "reader_closed_during_preparation",
    ):
        assert connects == [owner.id, owner.id]
        assert not replacement_socket.sent_text
    else:
        assert response.status_code == 200, response.text
        assert '"type":"response.completed"' in response.text
        assert "response_execution_session_retired" not in response.text
        assert connects == [owner.id, owner.id]
        assert len(replacement_socket.sent_text) == 1
        expected_input = (
            json.loads(json.dumps(body["input"]).replace("synthetic context", "synthetic prepared snapshot"))
            if outcome == "prepared"
            else body["input"]
        )
        assert json.loads(replacement_socket.sent_text[0])["input"] == expected_input
        if outcome == "prepared":
            assert len(prepared_texts) == 2
            assert "synthetic prepared snapshot" in prepared_texts[1]
    if outcome == "authenticated":
        assert len(reservations) == 2
        assert reservations[0] is not None and reservations[1] is reservations[0]
    if outcome not in ("success", "authenticated", "prepared"):
        assert '"type":"response.completed"' not in response.text
