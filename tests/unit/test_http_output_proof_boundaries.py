"""Untrusted tool payloads cannot interrupt completed-response bookkeeping."""

import json
from collections import deque
from contextlib import nullcontext
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from app.modules.proxy import service as proxy_module
from tests.unit.test_proxy_http_bridge import _make_bridge_session

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_deep_tool_arguments_do_not_interrupt_atomic_terminal_snapshot(monkeypatch):
    service = proxy_module.ProxyService(cast(Any, nullcontext()))
    monkeypatch.setattr(service, "_register_http_bridge_previous_response_id", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_finalize_websocket_request_state", AsyncMock())
    state = proxy_module._WebSocketRequestState(
        request_id="synthetic-proof-depth",
        response_id="resp_new",
        model="gpt-5.4",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=1.0,
        transport="http",
        skip_request_log=True,
        input_item_count=0,
        input_full_fingerprint=None,
    )
    session = _make_bridge_session(pending_requests=deque([state]), queued_request_count=1)
    session.last_completed_response_id = "resp_old"
    session.last_completed_response_account_id = session.account.id
    session.last_completed_input_count = 3
    session.last_completed_input_prefix_fingerprint = "old-prefix"
    session.last_completed_output_identities = ("old-proof",)
    arguments = "[" * 10000 + "0" + "]" * 10000
    await service._process_http_bridge_upstream_text(
        session,
        json.dumps(
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_new",
                    "object": "response",
                    "status": "completed",
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "synthetic_call",
                            "name": "synthetic_tool",
                            "arguments": arguments,
                        }
                    ],
                },
            }
        ),
    )
    assert session.last_completed_response_id == "resp_new"
    assert session.last_completed_input_count == 0
    assert session.last_completed_input_prefix_fingerprint is None
    assert session.last_completed_output_identities != ("old-proof",)
    assert len(session.last_completed_output_identities) == 1


def test_non_finite_argument_parsing_does_not_merge_distinct_wire_arguments():
    from app.modules.proxy._service.http_bridge.continuation import response_output_identity

    call = {"type": "function_call", "call_id": "synthetic", "name": "synthetic_tool"}
    assert response_output_identity({**call, "arguments": "1e999"}) != response_output_identity(
        {**call, "arguments": "1e998"}
    )
