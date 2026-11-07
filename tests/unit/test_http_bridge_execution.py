import asyncio
import contextlib
from collections import deque
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

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
@pytest.mark.parametrize("surface", ["http_bridge", "http_bridge_prewarm"])
async def test_execution_wait_does_not_consume_create_admission_budget(monkeypatch, surface):
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
            surface=surface,
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(6)
        session.execution_gate.release()
        await scheduler.drain()
        await scheduler.advance(4.1)
        if surface == "http_bridge_prewarm":
            assert task.done(), "Optional prewarm retains one short admission budget"
        else:
            assert not task.done(), "Waiting for execution must not consume the short create-admission budget"
            await scheduler.advance(6)
            assert task.done(), "Create admission remains bounded after execution becomes owned"
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
@pytest.mark.parametrize("surface", ["http_bridge", "http_bridge_prewarm"])
async def test_execution_wait_uses_startup_budget_but_prewarm_stays_short(monkeypatch, surface):
    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("wait-behind-healthy-owner")
    state.started_at = 0
    state.bridge_request_deadline = 100
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    await session.execution_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state, response_create_gate=session.response_create_gate, bridge_session=session, surface=surface
        )
    )
    try:
        await scheduler.drain()
        if surface == "http_bridge_prewarm":
            await scheduler.advance(10.1)
            assert task.done()
            with pytest.raises(ProxyResponseError) as error:
                await task
            assert error.value.payload["error"]["code"] == "response_execution_wait_timeout"
            assert session.execution_gate.locked(), "A timed-out warmup must not release the active response"
        else:
            await scheduler.advance(25)
            assert not task.done(), "An ordinary waiter may outlive short create admission behind a healthy response"
            session.execution_gate.release()
            await scheduler.drain()
            await task
            assert state.http_bridge_execution_gate is session.execution_gate
            assert state.response_create_gate_acquired
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        if state.response_create_admission is not None:
            state.response_create_admission.release()
        release_http_bridge_execution(state)
        if state.response_create_gate_acquired:
            session.response_create_gate.release()
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_execution_wait_respects_shorter_stream_silence_budget(monkeypatch):
    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("short-silence-budget")
    state.started_at = 0
    state.bridge_request_deadline = 100
    settings = proxy_service.get_settings().model_copy(update={"stream_idle_timeout_seconds": 4})
    monkeypatch.setattr(proxy_service, "get_settings", lambda: settings)
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    await session.execution_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state, response_create_gate=session.response_create_gate, bridge_session=session
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(4.1)
        assert task.done(), "Execution waiting must also respect a shorter configured silence budget"
        with pytest.raises(ProxyResponseError) as error:
            await task
        assert error.value.payload["error"]["code"] == "response_execution_wait_timeout"
        assert session.execution_gate.locked()
        assert state.http_bridge_execution_gate is None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        release_http_bridge_execution(state)
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "exception", "cancel"])
@pytest.mark.parametrize("cleanup_kind", ["detach", "submit_interruption"])
async def test_terminal_claim_detach_cannot_wake_successor_before_bookkeeping(
    monkeypatch, terminal_outcome, cleanup_kind
):
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
        if cleanup_kind == "detach":
            assert not await service._detach_http_bridge_request(session, request_state=owner)
        else:
            await service._cleanup_http_bridge_submit_interruption(
                session, request_state=owner, gate_acquired=True, request_enqueued=True, counted_in_queue=False
            )
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
@pytest.mark.parametrize("event_type", ["response.failed", "response.incomplete", "response.completed"])
@pytest.mark.parametrize("terminal_cancelled", [False, True])
async def test_prewarm_terminal_cleanup_cannot_dispatch_successor_before_finalization(
    monkeypatch, event_type, terminal_cancelled
):
    import json

    import anyio

    from app.modules.proxy._service.websocket.helpers import _release_websocket_response_create_gate
    from tests.unit.test_proxy_http_bridge import _make_app_settings, _make_bridge_session

    clock = VirtualClock(monotonic_value=100)
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    session = _make_bridge_session()
    session.codex_session = True
    session.prewarm_lock = anyio.Lock()
    service._http_bridge_sessions[session.key] = session
    monkeypatch.setattr(
        proxy_service,
        "get_settings",
        lambda: _make_app_settings(http_responses_session_bridge_codex_prewarm_enabled=True),
    )
    monkeypatch.setattr(
        proxy_service,
        "get_settings_cache",
        lambda: SimpleNamespace(get=AsyncMock(return_value=proxy_service.DashboardSettings())),
    )
    monkeypatch.setattr(service, "_http_bridge_precreated_retry_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_register_http_bridge_previous_response_id", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_handle_stream_error", AsyncMock())
    monkeypatch.setattr(service._load_balancer, "record_success", AsyncMock())
    sent_frames = []

    async def send_text(text):
        sent_frames.append(json.loads(text))

    session.upstream = cast(Any, SimpleNamespace(send_text=send_text, close=AsyncMock()))
    parent, waiter = _state("prewarm-parent"), _state("prewarm-successor")
    for state in (parent, waiter):
        state.model = "gpt-5.2"
        state.started_at = clock.monotonic()
        state.bridge_request_deadline = 200
        state.request_text = '{"type":"response.create","model":"gpt-5.2","input":"new"}'
        state.skip_request_log = True
    session.account_lease = await service._load_balancer.acquire_account_lease(session.account.id, kind="stream")
    entered, finish = asyncio.Event(), asyncio.Event()
    warmup = None
    prewarm = scheduler.create_task(
        service._maybe_prewarm_http_bridge_session(session, request_state=parent, text_data=parent.request_text)
    )
    tasks = [prewarm]
    try:
        await scheduler.drain()
        assert len(sent_frames) == 1 and sent_frames[0]["generate"] is False
        warmup = session.pending_requests[0]
        warmup.replay_count = 1
        release_lease = warmup.account_response_create_release
        assert release_lease is not None

        async def delayed_release(lease):
            entered.set()
            await finish.wait()
            await release_lease(lease)

        # Terminal delivery precedes this real finalization await. Only the
        # dependency's completion is held; all ownership and fencing stay real.
        warmup.account_response_create_release = delayed_release
        submit = scheduler.create_task(
            service._submit_http_bridge_request(
                session, request_state=waiter, text_data=waiter.request_text, queue_limit=8
            )
        )
        tasks.append(submit)
        await scheduler.drain()
        assert not submit.done()
        response = {"id": "resp_warmup_owned_terminal", "status": event_type.removeprefix("response."), "output": []}
        if event_type == "response.failed":
            response["error"] = {"code": "server_error", "message": "upstream failed"}
        terminal = scheduler.create_task(
            service._process_http_bridge_upstream_text(session, json.dumps({"type": event_type, "response": response}))
        )
        tasks.append(terminal)
        await asyncio.wait_for(entered.wait(), timeout=1)
        await scheduler.drain()
        assert warmup not in session.pending_requests
        assert not terminal.done()
        assert warmup.http_bridge_execution_gate is session.execution_gate
        assert not submit.done(), "Prewarm consumer cleanup must not release the terminal continuation's execution"
        assert len(sent_frames) == 1
        assert waiter.response_create_attempt_count == 0

        if terminal_cancelled:
            terminal.cancel()
        finish.set()
        terminal_result = (await asyncio.gather(terminal, return_exceptions=True))[0]
        if terminal_cancelled:
            assert isinstance(terminal_result, asyncio.CancelledError)
        else:
            assert terminal_result is None
        await scheduler.drain()
        if terminal_cancelled or event_type == "response.failed":
            with pytest.raises(ProxyResponseError) as error:
                await submit
            assert error.value.payload["error"]["code"] == "response_execution_session_retired"
            assert waiter.response_create_attempt_count == 0
            assert len(sent_frames) == 1
        else:
            await submit
            assert waiter.response_create_attempt_count == 1
            assert len(sent_frames) == 2
            assert sent_frames[1].get("generate") is not False
        prewarm_result = (await asyncio.gather(prewarm, return_exceptions=True))[0]
        if event_type == "response.completed":
            assert prewarm_result is None
        else:
            assert isinstance(prewarm_result, ProxyResponseError)
    finally:
        finish.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for state in (parent, waiter, warmup):
            if state is not None:
                await _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
                release_http_bridge_execution(state)
        await service._load_balancer.release_account_lease(session.account_lease)
        await scheduler.cancel_owned_tasks()


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


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["settings", "account"])
@pytest.mark.parametrize("request_deadline", [10, 100])
async def test_account_admission_is_bounded_after_execution_wait(monkeypatch, phase, request_deadline):
    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("bounded-account-admission")
    state.started_at = 0
    state.bridge_request_deadline = request_deadline
    entered = asyncio.Event()
    stopped = asyncio.Event()
    settings = proxy_service.DashboardSettings()

    async def delayed():
        entered.set()
        try:
            await scheduler.sleep(100)
        finally:
            stopped.set()
        return settings

    release = AsyncMock()
    service._load_balancer = cast(Any, SimpleNamespace(release_account_lease=release))
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    monkeypatch.setattr(
        proxy_service,
        "get_settings_cache",
        lambda: SimpleNamespace(get=delayed if phase == "settings" else AsyncMock(return_value=settings)),
    )
    acquire = delayed if phase == "account" else AsyncMock()

    async def acquire_lease(**kwargs):
        return await acquire()

    monkeypatch.setattr(service, "_acquire_account_response_create_lease_or_overload", acquire_lease)
    await session.execution_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state,
            response_create_gate=session.response_create_gate,
            bridge_session=session,
            account_id=session.account.id,
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(6)
        state.started_at = 6  # A retry's new start must not replace its retained outer deadline.
        session.execution_gate.release()
        await scheduler.drain()
        assert entered.is_set()
        await scheduler.advance(min(request_deadline, 16) - 6 + 0.1)
        assert task.done(), "Settings and account admission cannot outlive the absolute create/request deadline"
        with pytest.raises(ProxyResponseError):
            await task
        assert stopped.is_set(), "The timed-out preparatory operation must be cancelled"
        assert state.account_response_create_lease is None
        assert not session.response_create_gate.locked()
        assert state.http_bridge_execution_gate is session.execution_gate
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        release_http_bridge_execution(state)
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_gate", ["session", "global"])
@pytest.mark.parametrize("outcome", ["deadline", "cancel"])
async def test_account_preparation_does_not_restart_later_gate_budget(monkeypatch, blocked_gate, outcome):
    from app.modules.proxy.work_admission import WorkAdmissionController

    clock = VirtualClock()
    scheduler = VirtualScheduler(clock)
    service = proxy_service.ProxyService(cast(Any, SimpleNamespace()), clock=clock, scheduler=scheduler)
    controller = WorkAdmissionController(
        token_refresh_limit=0,
        websocket_connect_limit=0,
        response_create_limit=1,
        compact_response_create_limit=0,
        admission_wait_timeout_seconds=10,
        scheduler=scheduler,
    )
    service._work_admission = controller
    blocker = await controller.acquire_response_create() if blocked_gate == "global" else None
    session = _make_http_bridge_session(deque(), queued_request_count=1)
    state = _state("preparation-consumes-create-budget")
    state.started_at = 0
    state.bridge_request_deadline = 10
    lease = object()
    release = AsyncMock()
    service._load_balancer = cast(Any, SimpleNamespace(release_account_lease=release))

    async def settings():
        await scheduler.sleep(3)
        return proxy_service.DashboardSettings()

    monkeypatch.setattr(proxy_service, "get_settings_cache", lambda: SimpleNamespace(get=settings))
    monkeypatch.setattr(proxy_service, "_proxy_admission_wait_timeout_seconds", lambda: 10)
    monkeypatch.setattr(service, "_acquire_account_response_create_lease_or_overload", AsyncMock(return_value=lease))
    if blocked_gate == "session":
        await session.response_create_gate.acquire()
    await session.execution_gate.acquire()
    task = asyncio.create_task(
        service._acquire_request_state_response_create_admission(
            state,
            response_create_gate=session.response_create_gate,
            bridge_session=session,
            account_id=session.account.id,
        )
    )
    try:
        await scheduler.drain()
        await scheduler.advance(6)
        state.started_at = 6
        session.execution_gate.release()
        await scheduler.drain()
        await scheduler.advance(3)
        assert state.account_response_create_lease is lease
        assert not task.done()
        if outcome == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await scheduler.advance(1.1)
            assert task.done(), "The next gate must receive only the remaining absolute budget"
            with pytest.raises(ProxyResponseError):
                await task
        release.assert_awaited_once_with(lease)
        assert state.account_response_create_lease is None
        assert not state.response_create_gate_acquired
        assert session.response_create_gate.locked() == (blocked_gate == "session")
        assert state.http_bridge_execution_gate is session.execution_gate
    finally:
        if blocker is not None:
            blocker.release()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, ProxyResponseError):
            await task
        release_http_bridge_execution(state)
        await scheduler.cancel_owned_tasks()
