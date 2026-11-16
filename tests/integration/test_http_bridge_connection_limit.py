import json

import pytest

from app.db.models import Account, AccountStatus
from app.modules.proxy import service as proxy_module
from app.modules.proxy.service import AccountSelection
from tests.integration.test_http_responses_bridge import (
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
    _get_account,
    _import_account,
    _install_bridge_settings_with_limits,
    get_proxy_service_for_app,
)

pytestmark = pytest.mark.integration

_LIMIT_CODE = "websocket_connection_limit_reached"
_LIMIT_MESSAGE = (
    "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue."
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    ["recover", "already_accepted", "named_response", "repeat_limit", "replacement_closes", "owner_unavailable"],
)
async def test_http_bridge_connection_limit_reconnects_only_an_unaccepted_turn_on_its_owner(
    async_client, app_instance, monkeypatch, outcome
):
    _install_bridge_settings_with_limits(monkeypatch, enabled=True, max_sessions=128)
    owner = await _get_account(await _import_account(async_client, "limit_owner", "limit-owner@example.com"))
    other = Account(
        id="other-limit-account",
        email="other-limit@example.com",
        access_token_encrypted="synthetic",
        refresh_token_encrypted="synthetic",
        id_token_encrypted="synthetic",
        status=AccountStatus.ACTIVE,
    )
    service = get_proxy_service_for_app(app_instance)
    selections = []
    connects = []

    class ExpiredSocket(_FakeBridgeUpstreamWebSocket):
        def __init__(self, *, warm_first=False):
            super().__init__("resp_warm")
            self.warm_first = warm_first

        async def send_text(self, text):
            if self.warm_first and not self.sent_text:
                return await super().send_text(text)
            self.sent_text.append(text)
            if outcome == "already_accepted":
                await self._messages.put(
                    _FakeUpstreamMessage(
                        "text",
                        text=json.dumps(
                            {"type": "response.created", "response": {"id": "resp_already", "status": "in_progress"}}
                        ),
                    )
                )
            error = {
                "type": "error",
                "status": 400,
                "error": {"type": "invalid_request_error", "code": _LIMIT_CODE, "message": _LIMIT_MESSAGE},
            }
            if outcome in {"already_accepted", "named_response"}:
                error["response_id"] = "resp_already"
            await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(error)))

    expired = ExpiredSocket(warm_first=True)

    class ClosedSocket(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            self.sent_text.append(text)
            await self._messages.put(_FakeUpstreamMessage("close", close_code=1000))

    if outcome == "repeat_limit":
        replacement = ExpiredSocket()
    elif outcome == "replacement_closes":
        replacement = ClosedSocket()
        monkeypatch.setattr(service, "_http_bridge_clean_close_retry_max_count", lambda: 2)
        monkeypatch.setattr(service, "_http_bridge_clean_close_retry_jitter_seconds", lambda: 0)
    else:
        replacement = _FakeBridgeUpstreamWebSocket("resp_recovered")
    sockets = [expired, replacement]

    async def select_account(self, deadline, **kwargs):
        selections.append(kwargs)
        if len(selections) == 1:
            return AccountSelection(account=owner, error_message=None, error_code=None)
        if outcome == "owner_unavailable":
            return AccountSelection(
                account=None, error_message="Owner unavailable", error_code="continuity_owner_unavailable"
            )
        selected = owner if kwargs.get("preferred_account_id") == owner.id else other
        return AccountSelection(account=selected, error_message=None, error_code=None)

    async def fresh(self, target, **kwargs):
        return target

    async def connect(self, target, headers, **kwargs):
        connects.append(target.id)
        assert len(connects) <= 2, "connection lifetime recovery must not loop"
        return sockets[len(connects) - 1]

    monkeypatch.setattr(proxy_module.ProxyService, "_select_account_with_budget", select_account)
    monkeypatch.setattr(proxy_module.ProxyService, "_ensure_fresh_with_budget", fresh)
    monkeypatch.setattr(proxy_module.ProxyService, "_open_upstream_websocket_with_budget", connect)
    warm = await async_client.post(
        "/v1/responses",
        json={
            "model": "gpt-6.1-sol",
            "prompt_cache_key": "synthetic-connection-lifetime",
            "stream": True,
            "input": [{"role": "user", "content": "synthetic warmup"}],
        },
    )
    assert warm.status_code == 200
    assert '"type":"response.completed"' in warm.text
    response = await async_client.post(
        "/v1/responses",
        json={
            "model": "gpt-6.1-sol",
            "prompt_cache_key": "synthetic-connection-lifetime",
            "stream": True,
            "input": [
                {"role": "user", "content": "synthetic complete context"},
                {"type": "reasoning", "id": "rs_synthetic", "encrypted_content": "synthetic-ciphertext"},
                {"type": "function_call", "call_id": "call_synthetic", "name": "test_tool", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_synthetic", "output": "synthetic result"},
            ],
        },
    )
    events = [
        json.loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ") and line[6:] != "[DONE]"
    ]
    if outcome in {"already_accepted", "named_response"}:
        assert connects == [owner.id]
        assert not replacement.sent_text
        assert _LIMIT_CODE in response.text
        assert not any(event["type"] == "response.completed" for event in events)
        return
    assert len(selections) == 2
    assert selections[-1]["preferred_account_id"] == owner.id
    assert selections[-1]["fallback_on_preferred_account_unavailable"] is False
    assert len(expired.sent_text) == 2
    if outcome == "owner_unavailable":
        assert connects == [owner.id]
        assert not replacement.sent_text
        assert not any(event["type"] == "response.completed" for event in events)
        return
    assert connects == [owner.id, owner.id]
    assert expired.closed
    assert len(replacement.sent_text) == 1
    assert json.loads(replacement.sent_text[0]) == json.loads(expired.sent_text[-1])
    if outcome == "repeat_limit":
        assert _LIMIT_CODE in response.text
        assert not any(event["type"] == "response.completed" for event in events)
        return
    if outcome == "replacement_closes":
        assert not any(event["type"] == "response.completed" for event in events)
        return
    assert response.status_code == 200, response.text
    assert any(event["type"] == "response.completed" for event in events)
    assert _LIMIT_CODE not in response.text
    assert service._http_bridge_sessions
