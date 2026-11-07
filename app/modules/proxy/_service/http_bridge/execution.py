"""One execution owner per HTTP bridge, independent of create admission."""

from app.core.clients.proxy import ProxyResponseError
from app.core.clock import Clock, Scheduler
from app.core.config.settings import Settings
from app.core.errors import openai_error
from app.modules.proxy._service.http_bridge.helpers import (
    _http_bridge_admission_timeout_seconds,
    _http_bridge_eventless_budget_seconds,
    _http_bridge_request_budget_seconds,
)
from app.modules.proxy._service.support import _HTTPBridgeSession, _WebSocketRequestState


def validate_http_bridge_dispatch_account(session: _HTTPBridgeSession, expected_account_id: str | None) -> None:
    if expected_account_id is not None and session.account.id != expected_account_id:
        raise ProxyResponseError(
            503,
            openai_error(
                "response_execution_owner_changed", "HTTP bridge account changed before dispatch; retry the request"
            ),
            local_pre_dispatch_refusal=True,
        )


async def acquire_http_bridge_execution_before_create(
    request_state: _WebSocketRequestState,
    session: _HTTPBridgeSession,
    *,
    scheduler: Scheduler,
    clock: Clock,
    settings: Settings,
    admission_timeout_seconds: float,
    prewarm: bool,
    expected_account_id: str | None = None,
) -> float:
    """Acquire execution and return the bounded absolute create-admission deadline."""
    now = clock.monotonic()
    admission_deadline = now + admission_timeout_seconds
    execution_timeout = (
        admission_timeout_seconds
        if prewarm
        else _http_bridge_eventless_budget_seconds(
            settings, fallback_seconds=_http_bridge_request_budget_seconds(settings)
        )
    )
    execution_timeout = _http_bridge_admission_timeout_seconds(request_state, execution_timeout, settings, now=now)
    await acquire_http_bridge_execution(request_state, session, scheduler=scheduler, timeout_seconds=execution_timeout)
    try:
        validate_http_bridge_dispatch_account(session, expected_account_id)
    except ProxyResponseError:
        release_http_bridge_execution(request_state)
        raise
    now = clock.monotonic()
    if not prewarm:
        admission_deadline = now + admission_timeout_seconds
    remaining = _http_bridge_admission_timeout_seconds(
        request_state, max(0.0, admission_deadline - now), settings, now=now
    )
    return now + remaining


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
