import asyncio
import contextlib
from collections import deque
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.core.clients.proxy import ProxyResponseError
from app.core.clock import REAL_SCHEDULER
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service.http_bridge.execution import (
    acquire_http_bridge_execution,
    release_http_bridge_execution,
)
from tests.simulation.virtual_time import VirtualClock, VirtualScheduler
from tests.unit.test_http_bridge_cancel_drain import _make_http_bridge_session, _make_request_state

pytestmark = pytest.mark.unit


def _state(name):
    return _make_request_state(name, response_id=None, awaiting_response_created=True)


@pytest.mark.asyncio
async def test_cancelled_execution_waiter_cannot_release_the_active_owner():
    session = _make_http_bridge_session(deque(), queued_request_count=0)
    owner, waiter, successor = (_state(name) for name in ["owner", "waiter", "successor"])
    await acquire_http_bridge_execution(owner, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    # A pre-response retry owns the same execution, rather than acquiring twice.
    await acquire_http_bridge_execution(owner, session, scheduler=REAL_SCHEDULER, timeout_seconds=0.01)
    task = asyncio.create_task(
        acquire_http_bridge_execution(waiter, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release_http_bridge_execution(waiter)
    with pytest.raises(ProxyResponseError) as error:
        await acquire_http_bridge_execution(successor, session, scheduler=REAL_SCHEDULER, timeout_seconds=0.001)
    assert error.value.payload["error"]["code"] == "response_execution_wait_timeout"
    release_http_bridge_execution(owner)
    release_http_bridge_execution(owner)
    await acquire_http_bridge_execution(waiter, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    with pytest.raises(ProxyResponseError):
        await acquire_http_bridge_execution(successor, session, scheduler=REAL_SCHEDULER, timeout_seconds=0.001)
    release_http_bridge_execution(waiter)
    await acquire_http_bridge_execution(successor, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    release_http_bridge_execution(successor)


@pytest.mark.asyncio
@pytest.mark.parametrize("retired", ["closed", "draining"])
async def test_execution_waiter_never_dispatches_on_a_retired_owner_generation(retired):
    session = _make_http_bridge_session(deque(), queued_request_count=0)
    owner, waiter = _state("owner"), _state("waiter")
    await acquire_http_bridge_execution(owner, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    task = asyncio.create_task(
        acquire_http_bridge_execution(waiter, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    )
    await asyncio.sleep(0)
    if retired == "closed":
        session.closed = True
    else:
        session.upstream_control.reconnect_requested = True
        session.upstream_control.retire_after_drain = True
    release_http_bridge_execution(owner)
    with pytest.raises(ProxyResponseError) as error:
        await task
    assert error.value.payload["error"]["code"] == "response_execution_session_retired"
    assert waiter.http_bridge_execution_gate is None
    assert not session.execution_gate.locked()


@pytest.mark.asyncio
async def test_execution_wait_and_create_wait_share_one_admission_budget(monkeypatch):
    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("budget-waiter")
    state.started_at = 0
    state.bridge_request_deadline = 100
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    await session.execution_gate.acquire()
    await session.response_create_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state,
            response_create_gate=session.response_create_gate,
            bridge_session=session,
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(6)
        session.execution_gate.release()
        await scheduler.drain()
        await scheduler.advance(4.1)
        assert task.done(), "The create gate must get the remaining four seconds, not a fresh ten-second budget"
        with pytest.raises(ProxyResponseError):
            await task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        release_http_bridge_execution(state)
        session.response_create_gate.release()
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "exception", "cancel"])
async def test_terminal_claim_detach_cannot_wake_successor_before_bookkeeping(monkeypatch, terminal_outcome):
    from unittest.mock import AsyncMock

    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()))
    owner = _state("claimed-owner")
    owner.response_id = "resp_claimed_owner"
    owner.awaiting_response_created = False
    session = _make_http_bridge_session(deque([owner]), queued_request_count=1)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def bookkeeping(*args, **kwargs):
        entered.set()
        await finish.wait()
        if terminal_outcome == "exception":
            raise RuntimeError("terminal bookkeeping failure")
        return True

    monkeypatch.setattr(service, "_register_http_bridge_previous_response_id", bookkeeping)
    monkeypatch.setattr(service, "_finalize_websocket_request_state", AsyncMock())
    await acquire_http_bridge_execution(owner, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
    terminal = asyncio.create_task(
        service._process_http_bridge_upstream_text(
            session, '{"type":"response.completed","response":{"id":"resp_claimed_owner","status":"completed"}}'
        )
    )
    waiter = _state("queued-behind-claimed-owner")
    waiting = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        waiting = asyncio.create_task(
            acquire_http_bridge_execution(waiter, session, scheduler=REAL_SCHEDULER, timeout_seconds=1)
        )
        await asyncio.sleep(0)
        assert not await service._detach_http_bridge_request(session, request_state=owner)
        for _ in range(4):
            await asyncio.sleep(0)
        assert not waiting.done(), "Downstream detach cannot release a terminal bookkeeping owner's execution permit"
        if terminal_outcome == "cancel":
            terminal.cancel()
            with pytest.raises(asyncio.CancelledError):
                await terminal
        else:
            finish.set()
            if terminal_outcome == "exception":
                with pytest.raises(RuntimeError, match="terminal bookkeeping failure"):
                    await terminal
            else:
                await terminal
        if terminal_outcome == "success":
            await asyncio.wait_for(waiting, timeout=1)
        else:
            with pytest.raises(ProxyResponseError) as error:
                await waiting
            assert error.value.payload["error"]["code"] == "response_execution_session_retired"
    finally:
        finish.set()
        await asyncio.gather(terminal, *([waiting] if waiting is not None else []), return_exceptions=True)
        release_http_bridge_execution(owner)
        release_http_bridge_execution(waiter)


@pytest.mark.asyncio
async def test_execution_and_global_admission_cannot_extend_request_deadline(monkeypatch):
    from app.modules.proxy.work_admission import WorkAdmissionController

    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    controller = WorkAdmissionController(
        token_refresh_limit=0,
        websocket_connect_limit=0,
        response_create_limit=1,
        compact_response_create_limit=0,
        admission_wait_timeout_seconds=10,
        scheduler=scheduler,
    )
    blocker = await controller.acquire_response_create()
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    service._work_admission = controller
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("global-budget-waiter")
    state.started_at = 0
    state.bridge_request_deadline = 10
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    await session.execution_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state, response_create_gate=session.response_create_gate, bridge_session=session
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(6)
        session.execution_gate.release()
        await scheduler.drain()
        await scheduler.advance(4.1)
        assert task.done(), "The global create controller must not receive a fresh timeout past the request deadline"
        with pytest.raises(ProxyResponseError):
            await task
        assert state.response_create_admission is None
        assert not session.response_create_gate.locked()
    finally:
        blocker.release()
        await scheduler.drain()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        if state.response_create_admission is not None:
            state.response_create_admission.release()
        release_http_bridge_execution(state)
        await scheduler.cancel_owned_tasks()
