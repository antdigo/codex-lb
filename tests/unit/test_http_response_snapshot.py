import json
from dataclasses import replace
from typing import Any, cast

import pytest

from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonValue
from app.db.models import HttpBridgeSessionState
from app.modules.proxy import service as proxy_module
from app.modules.proxy._service.http_bridge import streaming
from app.modules.proxy._service.http_bridge.continuation import ContinuationBinding
from app.modules.proxy.durable_bridge_coordinator import DurableBridgeLookup
from tests.unit.test_proxy_http_bridge import _make_bridge_session

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_response_advance_clears_unproven_prefix_metadata(monkeypatch):
    from collections import deque
    from contextlib import nullcontext
    from unittest.mock import AsyncMock

    from tests.unit.test_proxy_http_bridge import _make_bridge_session

    service = proxy_module.ProxyService(cast(Any, nullcontext()))
    monkeypatch.setattr(service, "_register_http_bridge_previous_response_id", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_finalize_websocket_request_state", AsyncMock())
    state = proxy_module._WebSocketRequestState(
        request_id="synthetic-metadata-B",
        response_id="resp_synthetic_B",
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
    session = _make_bridge_session(
        key_value="synthetic-prefix-snapshot",
        pending_requests=deque([state]),
        queued_request_count=1,
    )
    session.last_completed_response_id = "resp_synthetic_A"
    session.last_completed_response_account_id = session.account.id
    session.last_completed_input_count = 2
    session.last_completed_input_prefix_fingerprint = "synthetic-prefix-A"
    await service._process_http_bridge_upstream_text(
        session,
        json.dumps(
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_synthetic_B",
                    "object": "response",
                    "status": "completed",
                    "output": [],
                    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                },
            }
        ),
    )
    assert session.last_completed_response_id == "resp_synthetic_B"
    assert session.last_completed_input_count == 0
    assert session.last_completed_input_prefix_fingerprint is None


def context():
    prefix: list[JsonValue] = [{"role": "user", "content": [{"type": "input_text", "text": "synthetic base"}]}]
    lookup = DurableBridgeLookup(
        session_id="synthetic-session",
        canonical_kind="session_header",
        canonical_key="synthetic",
        api_key_scope="__anonymous__",
        account_id="synthetic-owner",
        owner_instance_id="instance-a",
        owner_epoch=1,
        lease_expires_at=None,
        state=HttpBridgeSessionState.ACTIVE,
        latest_turn_state=None,
        latest_response_id="resp_latest",
        latest_input_item_count=1,
        latest_input_full_fingerprint=proxy_module._fingerprint_input_items(prefix),
    )
    body = ResponsesRequest.model_validate(
        {
            "model": "gpt-5.4",
            "instructions": "",
            "previous_response_id": "resp_latest",
            "input": [
                *prefix,
                {"role": "assistant", "content": [{"type": "output_text", "text": "synthetic answer"}]},
                {"role": "user", "content": [{"type": "input_text", "text": "synthetic next"}]},
            ],
        }
    )
    return body, lookup


@pytest.mark.parametrize("anchor", ["resp_older", "resp_latest", None])
@pytest.mark.parametrize("manifest", [None, {}])
def test_durable_latest_snapshot_cannot_prove_another_exact_anchor(anchor, manifest):
    body, lookup = context()
    body = body.model_copy(update={"previous_response_id": anchor})
    lookup = replace(lookup, latest_pending_tool_calls=manifest)
    proof = streaming._verify_durable_full_resend(body, lookup)
    reason = streaming._durable_full_resend_anchor_rejection(body, lookup, payload_looks_like_full_resend=True)
    if anchor == "resp_older":
        assert proof is None
        assert reason == "anchor_response_mismatch"
    else:
        assert proof is not None and proof.matches(body, lookup)
        assert reason is None


def test_verified_snapshot_is_not_reusable_for_changed_requested_anchor():
    body, lookup = context()
    proof = streaming._verify_durable_full_resend(body, lookup)
    assert proof is not None
    assert not proof.matches(body.model_copy(update={"previous_response_id": "resp_older"}), lookup)


@pytest.mark.parametrize("projected", [False, True])
@pytest.mark.parametrize("correct_owner", [False, True])
def test_interrupted_obligations_require_exact_snapshot_owner(projected, correct_owner):
    session = _make_bridge_session()
    session.last_completed_response_id = "resp_physical"
    session.last_completed_response_account_id = session.account.id if correct_owner else "another-owner"
    session.last_pending_tool_calls = {"synthetic_call": "function_call"}
    anchor = "resp_public" if projected else "resp_physical"
    if projected:
        session.continuation_bindings[anchor] = ContinuationBinding(
            anchor,
            "resp_physical",
            session.account.id,
            session.key,
            session.connection_generation,
            session.durable_owner_epoch,
        )
    body = ResponsesRequest.model_validate(
        {
            "model": "gpt-5.4",
            "instructions": "",
            "previous_response_id": anchor,
            "input": [{"role": "user", "content": "synthetic follow-up"}],
        }
    )
    result = streaming._http_bridge_interrupted_tool_outputs_input(session, payload=body, request_id="synthetic")
    if correct_owner:
        assert result is not None
        assert isinstance(result[0], dict)
        assert result[0]["type"] == "function_call_output" and result[0]["call_id"] == "synthetic_call"
    else:
        assert result is None
