import pytest

from app.core.types import JsonValue
from app.modules.proxy import service as proxy_service

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "guard",
    [
        "none",
        "raw_websocket",
        "sibling",
        "anchor",
        "hard_anchor",
        "response_id",
        "event",
        "visible",
        "sequence",
        "already_replayed",
        "named_response",
        "failed_response",
        "typeless_error",
    ],
)
def test_connection_limit_retry_requires_an_unaccepted_unanchored_http_turn(guard):
    state = proxy_service._WebSocketRequestState(
        request_id="req_connection_limit",
        model="gpt-6.1-sol",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=0.0,
        awaiting_response_created=True,
        request_text='{"type":"response.create","model":"gpt-6.1-sol","input":"synthetic"}',
    )
    payload: dict[str, JsonValue] = {
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "code": "websocket_connection_limit_reached",
            "message": "Responses websocket connection limit reached (60 minutes).",
        },
    }
    if guard == "anchor":
        state.previous_response_id = "resp_anchor"
    elif guard == "hard_anchor":
        state.hard_continuity_anchor = True
    elif guard == "response_id":
        state.response_id = "resp_accepted"
    elif guard == "event":
        state.response_event_count = 1
    elif guard == "visible":
        state.downstream_visible = True
    elif guard == "sequence":
        state.last_downstream_sequence_number = 0
    elif guard == "already_replayed":
        state.replay_count = 1
    elif guard == "named_response":
        payload["response_id"] = "resp_accepted"
    elif guard == "failed_response":
        payload = {"type": "response.failed", "response": {"status": "failed", "error": payload["error"]}}
    elif guard == "typeless_error":
        del payload["type"]
    kwargs = {} if guard == "raw_websocket" else {"allow_connection_lifetime_retry": True}
    code = proxy_service._websocket_precreated_retry_error_code(
        state,
        event_type=str(payload.get("type", "error")),
        payload=payload,
        has_other_pending_requests=guard == "sibling",
        **kwargs,
    )
    assert code == ("websocket_connection_limit_reached" if guard == "none" else None)
