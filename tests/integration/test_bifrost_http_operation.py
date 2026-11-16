"""Optional acceptance against a pinned Bifrost binary and synthetic upstream.

Run with CODEX_LB_BIFROST_ACCEPTANCE_BINARY pointing at Bifrost 2.5.1. The test
starts private localhost processes and never uses production application data.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
from pathlib import Path

import httpx
import pytest
import uvicorn

from app.dependencies import get_proxy_service_for_app
from app.modules.proxy import service as proxy_module
from tests.integration.test_http_responses_bridge import (
    _FakeBridgeUpstreamWebSocket,
    _FakeUpstreamMessage,
)
from tests.integration.test_http_responses_bridge import (
    promotion_transport as promotion_transport,
)

pytestmark = pytest.mark.integration


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,identity_mode",
    [
        ("opaque", "shared"),
        ("opaque", "isolated"),
        ("readable", "shared"),
        ("tool", "shared"),
        ("opaque-telemetry", "shared"),
        ("terminal-first", "shared"),
        ("tool-search", "shared"),
        ("tool-search", "isolated"),
    ],
)
async def test_pinned_bifrost_maps_one_http_operation(
    async_client, app_instance, promotion_transport, monkeypatch, tmp_path, mode, identity_mode
):
    binary = os.environ.get("CODEX_LB_BIFROST_ACCEPTANCE_BINARY")
    if not binary:
        pytest.skip("set CODEX_LB_BIFROST_ACCEPTANCE_BINARY to run pinned Bifrost acceptance")
    assert Path(binary).is_file()
    sockets, raw_calls, _dashboard = promotion_transport
    _dashboard.thread_cache_identity_mode = identity_mode

    class SyntheticSocket(_FakeBridgeUpstreamWebSocket):
        async def send_text(self, text):
            self.sent_text.append(text)
            response_id = self.response_id_prefix
            first = response_id == "resp_abandoned"
            reason = {
                "id": "rs_source" if first else "rs_winner",
                "type": "reasoning",
                "summary": [],
                "encrypted_content": "abandoned-cipher" if first else "winner-cipher",
            }
            if first and mode == "readable":
                reason["summary"] = [{"type": "summary_text", "text": "Visible reasoning"}]
            events = [
                {
                    "type": "response.created",
                    "response": {"id": response_id, "object": "response", "status": "in_progress"},
                },
                {"type": "response.in_progress", "response": {"id": response_id, "status": "in_progress"}},
                {"type": "response.output_item.added", "response_id": response_id, "output_index": 0, "item": reason},
            ]
            if first and mode == "opaque-telemetry":
                events.extend(
                    [
                        {"type": "codex.rate_limits", "response_id": response_id, "rate_limits": {}},
                        {
                            "type": "response.output_item.added",
                            "response_id": response_id,
                            "output_index": 1,
                            "item": {**reason, "id": "rs_source_2", "encrypted_content": "abandoned-cipher-2"},
                        },
                    ]
                )
            if not first:
                events.append(
                    {"type": "response.output_item.done", "response_id": response_id, "output_index": 0, "item": reason}
                )
                if mode == "tool":
                    item = {
                        "type": "function_call",
                        "id": "fc_winner",
                        "call_id": "call_winner",
                        "name": "verify",
                        "arguments": '{"ok":true}',
                        "status": "completed",
                    }
                    events.extend(
                        [
                            {
                                "type": "response.output_item.added",
                                "response_id": response_id,
                                "output_index": 1,
                                "item": {**item, "arguments": "", "status": "in_progress"},
                            },
                            {
                                "type": "response.function_call_arguments.delta",
                                "response_id": response_id,
                                "item_id": "fc_winner",
                                "output_index": 1,
                                "delta": '{"ok":true}',
                            },
                            {
                                "type": "response.output_item.done",
                                "response_id": response_id,
                                "output_index": 1,
                                "item": item,
                            },
                        ]
                    )
                else:
                    item = {
                        "type": "message",
                        "id": "msg_winner",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "OK", "annotations": []}],
                    }
                    events.extend(
                        [
                            {
                                "type": "response.output_item.added",
                                "response_id": response_id,
                                "output_index": 1,
                                "item": {**item, "status": "in_progress", "content": []},
                            },
                            {
                                "type": "response.content_part.added",
                                "response_id": response_id,
                                "output_index": 1,
                                "content_index": 0,
                                "item_id": "msg_winner",
                                "part": {"type": "output_text", "text": "", "annotations": []},
                            },
                            {
                                "type": "response.output_text.delta",
                                "response_id": response_id,
                                "output_index": 1,
                                "content_index": 0,
                                "item_id": "msg_winner",
                                "delta": "OK",
                            },
                            {
                                "type": "response.output_item.done",
                                "response_id": response_id,
                                "output_index": 1,
                                "item": item,
                            },
                        ]
                    )
                events.append(
                    {
                        "type": "response.completed",
                        "response": {
                            "id": response_id,
                            "object": "response",
                            "status": "completed",
                            "output": [reason, item],
                            "usage": {"input_tokens": 24, "output_tokens": 2, "total_tokens": 26},
                        },
                    }
                )
                if mode == "terminal-first":
                    events = events[-1:]
            for event in events:
                await self._messages.put(_FakeUpstreamMessage("text", text=json.dumps(event)))
            if first:
                await self._messages.put(_FakeUpstreamMessage("close", close_code=1012))

    async def connect(*args, **kwargs):
        assert len(sockets) < 2, "one transport replay maximum"
        upstream = SyntheticSocket("resp_abandoned" if not sockets else "resp_winner")
        sockets.append(upstream)
        return upstream

    monkeypatch.setattr(proxy_module, "connect_responses_websocket", connect)
    backend_port, bifrost_port = _port(), _port()
    backend_paths = []
    backend_frames = []

    async def recording_app(scope, receive, send):
        if scope["type"] == "http":
            backend_paths.append((scope["method"], scope["path"]))
        body_parts = []

        async def recording_receive():
            message = await receive()
            if message["type"] == "http.request":
                body_parts.append(message.get("body", b""))
                if not message.get("more_body", False) and b"".join(body_parts):
                    backend_frames.append(json.loads(b"".join(body_parts)))
            return message

        await app_instance(scope, recording_receive, send)

    server = uvicorn.Server(
        uvicorn.Config(
            recording_app, host="127.0.0.1", port=backend_port, lifespan="off", access_log=False, log_level="error"
        )
    )
    server_task = asyncio.create_task(server.serve())
    pricing = tmp_path / "pricing.json"
    pricing.write_text(
        json.dumps(
            {
                "gpt-5.4": {
                    "provider": "openai",
                    "base_model": "gpt-5.4",
                    "mode": "responses",
                    "input_cost_per_token": 0,
                    "output_cost_per_token": 0,
                    "supports_function_calling": True,
                    "max_input_tokens": 32768,
                    "max_output_tokens": 1024,
                }
            }
        )
    )
    config = {
        "$schema": "https://www.getbifrost.ai/schema",
        "framework": {
            "pricing": {
                "pricing_url": pricing.as_uri(),
                "model_parameters_url": pricing.as_uri(),
                "pricing_sync_interval": 86400,
            }
        },
        "config_store": {"enabled": False},
        "logs_store": {"enabled": False},
        "client": {"enable_logging": False, "compat": {"convert_chat_to_responses": True}},
        "providers": {
            "codex-lb": {
                "keys": [{"name": "synthetic", "value": "synthetic", "weight": 1, "models": ["*"]}],
                "network_config": {"base_url": f"http://127.0.0.1:{backend_port}", "max_retries": 0},
                "custom_provider_config": {
                    "base_provider_type": "openai",
                    "allowed_requests": {
                        "responses": True,
                        "responses_stream": True,
                    },
                },
            }
        },
    }
    (tmp_path / "config.json").write_text(json.dumps(config))
    # No inherited credentials; only localhost bypasses a deliberately closed
    # proxy, so optional pricing/catalog startup fetches cannot reach the web.
    environment = {
        "PATH": os.defpath,
        "HTTP_PROXY": "http://127.0.0.1:9",
        "HTTPS_PROXY": "http://127.0.0.1:9",
        "NO_PROXY": "127.0.0.1,localhost",
    }
    process = None
    try:
        async with asyncio.timeout(15):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        with (tmp_path / "bifrost.log").open("wb") as log:
            process = await asyncio.create_subprocess_exec(
                binary,
                "-app-dir",
                str(tmp_path),
                "-host",
                "127.0.0.1",
                "-port",
                str(bifrost_port),
                "-log-level",
                "error",
                env=environment,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
            )
            async with httpx.AsyncClient(trust_env=False, timeout=30) as client:
                async with asyncio.timeout(30):
                    while True:
                        assert process.returncode is None, (tmp_path / "bifrost.log").read_text()[-4000:]
                        try:
                            if (await client.get(f"http://127.0.0.1:{bifrost_port}/health")).status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        await asyncio.sleep(0.05)
                body = {
                    "model": "codex-lb/gpt-5.4",
                    "stream": True,
                    "max_tokens": 32,
                    "messages": [
                        {"role": "user", "content": "Synthetic start"},
                        {"role": "assistant", "content": "Prior"},
                        {"role": "user", "content": "Synthetic finish"},
                    ],
                }
                if mode == "tool":
                    body["tools"] = [
                        {
                            "name": "verify",
                            "description": "Synthetic tool",
                            "input_schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
                        }
                    ]
                if mode == "tool-search":
                    body["tools"] = [
                        {
                            "name": "ToolSearch",
                            "description": "Find a synthetic tool",
                            "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}},
                        },
                        {
                            "name": "verify",
                            "description": "Synthetic deferred tool",
                            "defer_loading": True,
                            "input_schema": {"type": "object", "properties": {}},
                        },
                    ]
                    body["messages"] = [
                        {"role": "user", "content": "Synthetic tool discovery"},
                        {
                            "role": "assistant",
                            "content": [
                                {
                                    "type": "tool_use",
                                    "id": "call_synthetic_search",
                                    "name": "ToolSearch",
                                    "input": {"query": "select:verify"},
                                }
                            ],
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "tool_result",
                                    "tool_use_id": "call_synthetic_search",
                                    "content": [{"type": "tool_reference", "tool_name": "verify"}],
                                }
                            ],
                        },
                        {"role": "assistant", "content": "Synthetic discovery completed"},
                        {"role": "user", "content": "Synthetic finish"},
                    ]
                response = await client.post(f"http://127.0.0.1:{bifrost_port}/anthropic/v1/messages", json=body)
            assert response.status_code == 200, f"{response.text}; backend paths={backend_paths}"
            events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
            assert sum(event.get("type") == "message_start" for event in events) == 1, response.text
            if mode == "tool-search":
                frame = backend_frames[0]
                searches = [item for item in frame["input"] if item.get("type", "").startswith("tool_search_")]
                assert [item["type"] for item in searches] == ["tool_search_call", "tool_search_output"]
                assert all(item["execution"] == "client" for item in searches)
                assert all(item["call_id"] == "call_synthetic_search" for item in searches)
                assert any(tool["type"] == "tool_search" and tool["execution"] == "client" for tool in frame["tools"])
            assert not raw_calls, "Acceptance must traverse the WebSocket-backed HTTP bridge"
            if mode == "readable":
                assert len(sockets) == 1
                assert any(event.get("type") == "error" for event in events), response.text
            else:
                assert len(sockets) == 2
                if mode == "tool-search":
                    cache_key = json.loads(sockets[0].sent_text[0])["prompt_cache_key"]
                    assert isinstance(cache_key, str) and cache_key
                    if identity_mode == "shared" and frame.get("prompt_cache_key") is not None:
                        assert cache_key == frame["prompt_cache_key"]
                    first_attempt = json.loads(sockets[0].sent_text[0])
                    if identity_mode == "shared":
                        assert first_attempt["input"] == frame["input"]
                    else:
                        assert first_attempt["input"][1:] == frame["input"]
                        assert "codex-lb-cache-scope:" in json.dumps(first_attempt["input"][0])
                    for upstream in sockets:
                        assert len(upstream.sent_text) == 1
                        attempted = json.loads(upstream.sent_text[0])
                        assert attempted["input"] == first_attempt["input"]
                        assert attempted["tools"] == frame["tools"]
                        assert attempted["prompt_cache_key"] == cache_key
                assert "abandoned-cipher" not in response.text
                assert sum(event.get("type") == "message_stop" for event in events) == 1, response.text
                if mode == "tool":
                    starts = [
                        event.get("content_block", {}) for event in events if event.get("type") == "content_block_start"
                    ]
                    assert any(
                        block.get("type") == "tool_use" and block.get("id") == "call_winner" for block in starts
                    ), response.text
                else:
                    assert any(event.get("delta", {}).get("text") == "OK" for event in events), response.text
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                process.kill()
                await process.wait()
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, 10)
        finally:
            await get_proxy_service_for_app(app_instance).close_all_http_bridge_sessions()
