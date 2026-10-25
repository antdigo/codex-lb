from __future__ import annotations

import base64
import json

import httpx
import httpx2
import openai
import pytest
from httpx import ASGITransport
from openai.types.responses import ResponseFunctionToolCall

import app.modules.proxy.service as proxy_module

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/v1", "/v1/"])
async def test_openai_sdk_recovers_owned_terminal_after_long_unowned_prelude(app_instance, monkeypatch, path):
    async def fake_stream(self, payload, headers, **kwargs):
        for index in range(96):
            yield (
                "data: "
                + json.dumps(
                    {
                        "type": "response.output_text.delta",
                        "sequence_number": index,
                        "output_index": 0,
                        "item_id": "msg_orphan",
                        "delta": "discard me",
                    }
                )
                + "\n\n"
            )
        yield (
            "data: "
            + json.dumps(
                {
                    "type": "response.completed",
                    "sequence_number": 96,
                    "response": {
                        "id": "resp_recovered",
                        "object": "response",
                        "created_at": 1,
                        "model": "gpt-5.1",
                        "status": "completed",
                        "output": [
                            {
                                "type": "message",
                                "id": "msg_owned",
                                "role": "assistant",
                                "status": "completed",
                                "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
                            },
                            {
                                "type": "function_call",
                                "id": "fc_owned",
                                "call_id": "call_owned",
                                "name": "echo",
                                "arguments": '{"note":"owned"}',
                                "status": "completed",
                            },
                        ],
                        "usage": {"input_tokens": 100, "output_tokens": 10, "total_tokens": 110},
                    },
                }
            )
            + "\n\n"
        )

    monkeypatch.setattr(proxy_module.ProxyService, "stream_http_responses", fake_stream)
    async with httpx.AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://testserver") as admin:
        account = _make_auth_json("acc_long_prelude", "long-prelude@example.com")
        imported = await admin.post(
            "/api/accounts/import", files={"auth_json": ("auth.json", json.dumps(account), "application/json")}
        )
        assert imported.status_code == 200
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app_instance), base_url="http://testserver" + path
    ) as http:
        client = openai.AsyncOpenAI(
            api_key="test", base_url="http://testserver" + path, http_client=http, max_retries=0
        )
        async with client.responses.stream(model="gpt-5.1", input="hi") as stream:
            events = [event async for event in stream]
            result = await stream.get_final_response()
    assert result.id == "resp_recovered"
    assert result.status == "completed"
    assert result.output_text == "owned answer"
    call = result.output[1]
    assert isinstance(call, ResponseFunctionToolCall)
    assert call.id == "fc_owned"
    assert call.call_id == "call_owned"
    assert call.arguments == '{"note":"owned"}'
    assert [event.delta for event in events if event.type == "response.output_text.delta"] == ["owned answer"]
    assert not any(event.type == "response.failed" for event in events)


def _encode_jwt(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    return f"header.{body}.sig"


def _make_auth_json(account_id: str, email: str) -> dict:
    payload = {
        "email": email,
        "chatgpt_account_id": account_id,
        "https://api.openai.com/auth": {"chatgpt_plan_type": "plus"},
    }
    return {
        "tokens": {
            "idToken": _encode_jwt(payload),
            "accessToken": "access-token",
            "refreshToken": "refresh-token",
            "accountId": account_id,
        },
    }


@pytest.mark.asyncio
async def test_openai_client_responses_create(app_instance, monkeypatch):
    async def fake_stream(payload, headers, access_token, account_id, base_url=None, raise_for_status=False):
        yield (
            'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
            '"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n'
        )

    monkeypatch.setattr(proxy_module, "core_stream_responses", fake_stream)

    transport = ASGITransport(app=app_instance)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as admin_client:
        auth_json = _make_auth_json("acc_openai_resp", "openai-resp@example.com")
        files = {"auth_json": ("auth.json", json.dumps(auth_json), "application/json")}
        response = await admin_client.post("/api/accounts/import", files=files)
        assert response.status_code == 200

    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app_instance), base_url="http://testserver/v1"
    ) as http_client:
        client = openai.AsyncOpenAI(api_key="test", base_url="http://testserver/v1", http_client=http_client)
        result = await client.responses.create(model="gpt-5.1", input="hi")

    assert result.id == "resp_1"
    assert result.object == "response"


@pytest.mark.asyncio
async def test_openai_client_responses_stream_backend_codex_base_url(app_instance, monkeypatch):
    seen: dict[str, object] = {}

    async def fake_stream(payload, headers, access_token, account_id, base_url=None, raise_for_status=False, **kwargs):
        del headers, access_token, account_id, base_url, raise_for_status, kwargs
        seen["instructions"] = payload.instructions
        seen["input"] = payload.input
        yield (
            'data: {"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":1}},'
            '"response":{"id":"resp_backend_sdk"}}\n\n'
        )
        yield (
            'data: {"type":"response.created","response":{"id":"resp_backend_sdk","object":"response",'
            '"status":"in_progress","output":[]}}\n\n'
        )
        yield 'data: {"type":"response.output_text.delta","delta":"OK"}\n\n'
        yield (
            'data: {"type":"response.completed","response":{"id":"resp_backend_sdk","object":"response",'
            '"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}\n\n'
        )

    monkeypatch.setattr(proxy_module, "core_stream_responses", fake_stream)

    transport = ASGITransport(app=app_instance)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as admin_client:
        auth_json = _make_auth_json("acc_openai_backend_resp", "openai-backend-resp@example.com")
        files = {"auth_json": ("auth.json", json.dumps(auth_json), "application/json")}
        response = await admin_client.post("/api/accounts/import", files=files)
        assert response.status_code == 200

    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app_instance),
        base_url="http://testserver/backend-api/codex",
    ) as http_client:
        client = openai.AsyncOpenAI(
            api_key="test",
            base_url="http://testserver/backend-api/codex",
            http_client=http_client,
        )
        stream = await client.responses.create(model="gpt-5.5", input="hi", stream=True)
        events = [event async for event in stream]

    assert [event.type for event in events] == [
        "response.created",
        "response.output_text.delta",
        "response.completed",
    ]
    assert seen["instructions"] == ""
    assert seen["input"] == [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}]


@pytest.mark.asyncio
async def test_openai_client_chat_completions_create(app_instance, monkeypatch):
    async def fake_stream(payload, headers, access_token, account_id, base_url=None, raise_for_status=False):
        yield 'data: {"type":"response.output_text.delta","delta":"hi"}\n\n'
        yield 'data: {"type":"response.completed","response":{"id":"resp_2"}}\n\n'

    monkeypatch.setattr(proxy_module, "core_stream_responses", fake_stream)

    transport = ASGITransport(app=app_instance)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as admin_client:
        auth_json = _make_auth_json("acc_openai_chat", "openai-chat@example.com")
        files = {"auth_json": ("auth.json", json.dumps(auth_json), "application/json")}
        response = await admin_client.post("/api/accounts/import", files=files)
        assert response.status_code == 200

    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app_instance), base_url="http://testserver/v1"
    ) as http_client:
        client = openai.AsyncOpenAI(api_key="test", base_url="http://testserver/v1", http_client=http_client)
        result = await client.chat.completions.create(
            model="gpt-5.2",
            messages=[{"role": "user", "content": "hi"}],
        )

    assert result.id == "resp_2"
    assert result.object == "chat.completion"
