"""One execution owner per HTTP bridge, independent of create admission."""

from app.core.clients.proxy import ProxyResponseError
from app.core.clock import Scheduler
from app.core.errors import openai_error
from app.modules.proxy._service.support import _HTTPBridgeSession, _WebSocketRequestState


async def acquire_http_bridge_execution(
    request_state: _WebSocketRequestState,
    session: _HTTPBridgeSession,
    *,
    scheduler: Scheduler,
    timeout_seconds: float,
) -> None:
    gate = session.execution_gate
    owned = request_state.http_bridge_execution_gate
    if owned is gate:
        return
    if owned is not None:
        raise ProxyResponseError(
            503,
            openai_error("response_execution_owner_changed", "HTTP bridge execution owner changed before dispatch"),
            local_pre_dispatch_refusal=True,
        )
    queued_behind_owner = gate.locked()
    try:
        await scheduler.wait_for(gate.acquire(), timeout=timeout_seconds)
    except TimeoutError as exc:
        raise ProxyResponseError(
            503,
            openai_error(
                "response_execution_wait_timeout", "Timed out waiting for the current HTTP bridge response to finish"
            ),
            local_pre_dispatch_refusal=True,
        ) from exc
    # No await between successful acquisition and publishing its sole owner.
    request_state.http_bridge_execution_gate = gate
    if queued_behind_owner and (session.closed or session.upstream_control.retire_after_drain):
        release_http_bridge_execution(request_state)
        raise ProxyResponseError(
            503,
            openai_error(
                "response_execution_session_retired",
                "HTTP bridge retired while waiting for response execution; retry the request",
            ),
            local_pre_dispatch_refusal=True,
        )


def release_http_bridge_execution(request_state: _WebSocketRequestState) -> None:
    gate = request_state.http_bridge_execution_gate
    if gate is not None:
        request_state.http_bridge_execution_gate = None
        gate.release()
