import asyncio
from collections import deque
from typing import Any, cast

import pytest

from app.core.clients.proxy import ProxyResponseError
from app.core.errors import openai_error
from app.modules.proxy import service as facade
from app.modules.proxy._service.http_bridge.streaming import _http_bridge_can_replace_retired_gate_session
from tests.unit.test_http_bridge_cancel_drain import _make_http_bridge_session, _make_request_state

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "guard",
    [
        "none",
        "sent",
        "accepted",
        "events",
        "visible",
        "sequence",
        "anchor",
        "hard_anchor",
        "conversation",
        "operation",
        "codex",
        "replayed",
        "enqueued",
        "execution_owned",
        "create_owned",
        "account_lease",
        "admission_owned",
        "not_retired",
        "hard_key",
        "fake_provenance",
        "queue_detached",
        "draining",
    ],
)
def test_only_a_clean_unsubmitted_soft_waiter_can_replace_its_retired_execution(guard):
    key = facade._HTTPBridgeSessionKey("prompt_cache", "synthetic-retired", None)
    session = _make_http_bridge_session(deque(), queued_request_count=0, key=key)
    session.closed = True
    session.codex_session = False
    state = _make_request_state("waiter", response_id=None, awaiting_response_created=True, event_queue=asyncio.Queue())
    state.request_text = '{"type":"response.create","model":"gpt-6.1-sol","input":"synthetic"}'
    if guard == "sent":
        state.response_create_attempt_count = 1
    elif guard == "accepted":
        state.response_id = "resp_accepted"
    elif guard == "events":
        state.response_event_count = 1
    elif guard == "visible":
        state.downstream_visible = True
    elif guard == "sequence":
        state.last_downstream_sequence_number = 0
    elif guard == "anchor":
        state.previous_response_id = "resp_parent"
    elif guard == "hard_anchor":
        state.hard_continuity_anchor = True
    elif guard == "conversation":
        state.payload_conversation_bound = True
    elif guard == "operation":
        state.operation_id = "operation"
    elif guard == "codex":
        session.codex_session = True
    elif guard == "replayed":
        state.replay_count = 1
    elif guard == "execution_owned":
        state.http_bridge_execution_gate = asyncio.Semaphore(1)
    elif guard == "create_owned":
        state.response_create_gate = asyncio.Semaphore(1)
    elif guard == "account_lease":
        state.account_response_create_lease = cast(Any, object())
    elif guard == "admission_owned":
        state.response_create_admission = cast(Any, object())
    elif guard == "not_retired":
        session.closed = False
    elif guard == "hard_key":
        session.key = facade._HTTPBridgeSessionKey("session_header", "synthetic", None)
    elif guard == "queue_detached":
        state.event_queue = None
    elif guard == "draining":
        state.draining_until_terminal = True
    error = ProxyResponseError(
        503,
        openai_error("response_execution_session_retired", "Retired before dispatch"),
        local_pre_dispatch_refusal=guard != "fake_provenance",
    )
    assert _http_bridge_can_replace_retired_gate_session(
        error, session=session, request_state=state, request_was_enqueued=guard == "enqueued"
    ) is (guard == "none")
