import asyncio
from collections import deque
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from app.modules.proxy import service as facade
from app.modules.proxy._service.http_bridge import mixin, request_submit, upstream_events
from app.modules.proxy._service.websocket.helpers import _release_websocket_response_create_gate
from tests.simulation.virtual_time import VirtualClock, VirtualScheduler
from tests.unit.test_http_bridge_cancel_drain import _make_http_bridge_session, _make_request_state

pytestmark = pytest.mark.unit


def _candidate():
    clock = VirtualClock(monotonic_value=102)
    scheduler = VirtualScheduler(clock)
    service = facade.ProxyService(cast(Any, nullcontext()), clock=clock, scheduler=scheduler)
    session = _make_http_bridge_session(deque(), queued_request_count=0)
    session.key = facade._HTTPBridgeSessionKey("prompt_cache", "deadline", None)
    state = _make_request_state(
        "retained-deadline", response_id=None, awaiting_response_created=True, event_queue=asyncio.Queue()
    )
    state.started_at = 100
    state.bridge_request_deadline = 105
    state.bridge_age_required_account_id = session.account.id
    state.preferred_account_id = session.account.id
    return service, session, state, scheduler


@pytest.mark.asyncio
async def test_lifetime_reconnect_keeps_the_retained_absolute_deadline(monkeypatch):
    service, session, state, scheduler = _candidate()
    settings = facade.get_settings().model_copy(update={"http_responses_session_bridge_request_budget_seconds": 100})
    monkeypatch.setattr(mixin, "_service_get_settings", lambda: settings)
    select = AsyncMock(
        return_value=facade.AccountSelection(
            account=None, error_message="Owner unavailable", error_code="continuity_owner_unavailable"
        )
    )
    monkeypatch.setattr(service, "_select_account_with_budget_for_stream", select)
    try:
        with pytest.raises(facade.ProxyResponseError):
            await service._reconnect_http_bridge_session(
                session,
                request_state=state,
                require_same_account=True,
                require_preferred_account=True,
                dashboard_settings=facade.DashboardSettings(),
            )
        assert select.await_args is not None
        assert select.await_args.args[0] == 105
    finally:
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["deadline", "removed", "draining"])
async def test_lifetime_retry_never_sends_after_deadline_or_loss_of_ownership(monkeypatch, failure):
    service, session, state, scheduler = _candidate()
    state.request_text = '{"type":"response.create","model":"gpt-6.1-sol","input":"synthetic"}'
    lease = SimpleNamespace()
    state.account_response_create_lease = lease
    release = AsyncMock()
    monkeypatch.setattr(service._load_balancer, "release_account_lease", release)
    state.account_response_create_release = release
    state.response_create_gate_acquired = True
    await session.response_create_gate.acquire()
    session.pending_requests.append(state)
    session.queued_request_count = 1
    monkeypatch.setattr(service, "_http_bridge_precreated_retry_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_http_bridge_clean_close_retry_max_count", lambda: 0)

    async def reconnect(*args, **kwargs):
        if failure == "deadline":
            await scheduler.advance(4)
        elif failure == "removed":
            session.pending_requests.remove(state)
        else:
            state.draining_until_terminal = True

    monkeypatch.setattr(service, "_reconnect_http_bridge_session", reconnect)
    monkeypatch.setattr(service, "_http_bridge_text_with_account_installation_id", lambda session, state, text: text)
    send = AsyncMock()
    monkeypatch.setattr(request_submit, "_send_http_bridge_request_text_with_archive_id", send)
    try:
        assert await service._retry_http_bridge_precreated_request(session) is False
        send.assert_not_awaited()
        release.assert_awaited_once_with(lease)
        assert state.account_response_create_lease is None
        assert state.account_response_create_release is None
    finally:
        if session.response_create_gate.locked():
            session.response_create_gate.release()
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_cancelled_lifetime_refusal_still_finishes_releasing_its_real_account_lease(monkeypatch):
    service, session, state, scheduler = _candidate()
    lease = await service._load_balancer.acquire_account_lease(session.account.id, kind="response_create")
    state.account_response_create_lease = lease
    state.request_text = '{"type":"response.create","model":"gpt-6.1-sol","input":"synthetic"}'
    state.response_create_gate_acquired = True
    await session.response_create_gate.acquire()
    session.pending_requests.append(state)
    session.queued_request_count = 1
    monkeypatch.setattr(service, "_http_bridge_precreated_retry_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_http_bridge_clean_close_retry_max_count", lambda: 0)

    async def reconnect(*args, **kwargs):
        await scheduler.advance(4)

    monkeypatch.setattr(service, "_reconnect_http_bridge_session", reconnect)
    monkeypatch.setattr(service, "_http_bridge_text_with_account_installation_id", lambda session, state, text: text)
    send = AsyncMock()
    monkeypatch.setattr(request_submit, "_send_http_bridge_request_text_with_archive_id", send)
    original_release = service._load_balancer.release_account_lease
    entered_release = asyncio.Event()

    async def release(target):
        entered_release.set()
        await original_release(target)

    monkeypatch.setattr(service._load_balancer, "release_account_lease", release)
    state.account_response_create_release = release
    lock = service._load_balancer._runtime_lock
    await lock.acquire()
    task = asyncio.create_task(service._retry_http_bridge_precreated_request(session))
    try:
        await asyncio.wait_for(entered_release.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        lock.release()
        with pytest.raises(asyncio.CancelledError):
            await task
        send.assert_not_awaited()
        assert (await service._load_balancer.account_pressure_snapshot(session.account.id))[0] == 0
        assert state.account_response_create_lease is None
    finally:
        if lock.locked():
            lock.release()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if session.response_create_gate.locked():
            session.response_create_gate.release()
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("follower", ["gate", "capacity", "terminal"])
async def test_account_create_release_and_gate_cleanup_join_one_recorded_callback(follower):
    service, session, state, scheduler = _candidate()
    lease = await service._load_balancer.acquire_account_lease(session.account.id, kind="response_create")
    entered, finish = asyncio.Event(), asyncio.Event()
    releases = []

    async def release(target):
        releases.append(target)
        entered.set()
        await finish.wait()
        await service._load_balancer.release_account_lease(target)

    state.account_response_create_lease = lease
    state.account_response_create_release = release
    state.response_create_gate_acquired = True
    state.response_create_gate = session.response_create_gate
    await session.response_create_gate.acquire()
    early = asyncio.create_task(service._release_request_state_account_response_create_lease(state))
    gate = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        if follower == "capacity":
            cleanup = upstream_events._release_http_bridge_model_capacity_retry_admission(state)
        elif follower == "terminal":
            cleanup = service._emit_websocket_terminal_error(
                cast(Any, SimpleNamespace()),
                client_send_lock=session.pending_lock,
                request_state=state,
                error_code="stream_incomplete",
                error_message="Synthetic terminal cleanup",
            )
        else:
            cleanup = _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
        gate = asyncio.create_task(cleanup)
        await asyncio.sleep(0)
        early.cancel()
        gate.cancel()
        await asyncio.sleep(0)
        assert state.account_response_create_lease is lease
        assert state.account_response_create_release is release
        assert releases == [lease]
        finish.set()
        outcomes = await asyncio.gather(early, gate, return_exceptions=True)
        assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
        assert releases == [lease]
        assert (await service._load_balancer.account_pressure_snapshot(session.account.id))[0] == 0
        assert state.account_response_create_lease is None
        assert state.account_response_create_release is None
        if follower == "capacity":
            assert state.response_create_gate_acquired and session.response_create_gate.locked()
            await _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
        assert not state.response_create_gate_acquired and not session.response_create_gate.locked()
    finally:
        finish.set()
        await asyncio.gather(early, *([gate] if gate is not None else []), return_exceptions=True)
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_failed_account_create_release_retains_the_receipt_for_retry():
    service, session, state, scheduler = _candidate()
    lease = await service._load_balancer.acquire_account_lease(session.account.id, kind="response_create")
    calls = 0

    async def release(target):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("release unavailable")
        await service._load_balancer.release_account_lease(target)

    state.account_response_create_lease = lease
    state.account_response_create_release = release
    state.response_create_gate_acquired = True
    await session.response_create_gate.acquire()
    try:
        with pytest.raises(RuntimeError, match="release unavailable"):
            await _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
        assert state.account_response_create_lease is lease
        assert state.account_response_create_release is release
        assert not state.response_create_gate_acquired and not session.response_create_gate.locked()
        await service._release_request_state_account_response_create_lease(state)
        assert calls == 2
        assert (await service._load_balancer.account_pressure_snapshot(session.account.id))[0] == 0
        assert state.account_response_create_lease is None
        assert state.account_response_create_release is None
    finally:
        await service._load_balancer.release_account_lease(lease)
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_old_account_create_release_cannot_clear_a_replacement_lease():
    service, session, state, scheduler = _candidate()
    first = await service._load_balancer.acquire_account_lease(session.account.id, kind="response_create")
    replacement = await service._load_balancer.acquire_account_lease("replacement-owner", kind="response_create")
    entered, finish = asyncio.Event(), asyncio.Event()
    releases = []

    async def release_first(target):
        releases.append(target)
        entered.set()
        await finish.wait()
        await service._load_balancer.release_account_lease(target)

    release_replacement = AsyncMock(side_effect=service._load_balancer.release_account_lease)
    state.account_response_create_lease = first
    state.account_response_create_release = release_first
    early = asyncio.create_task(service._release_request_state_account_response_create_lease(state))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        state.account_response_create_lease = replacement
        state.account_response_create_release = release_replacement
        gate = asyncio.create_task(
            _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
        )
        await asyncio.sleep(0)
        finish.set()
        await asyncio.gather(early, gate)
        assert releases == [first]
        release_replacement.assert_not_awaited()
        assert state.account_response_create_lease is replacement
        assert state.account_response_create_release is release_replacement
        assert (await service._load_balancer.account_pressure_snapshot("replacement-owner"))[0] == 1
        await service._release_request_state_account_response_create_lease(state)
        release_replacement.assert_awaited_once_with(replacement)
        assert (await service._load_balancer.account_pressure_snapshot("replacement-owner"))[0] == 0
    finally:
        finish.set()
        await asyncio.gather(early, return_exceptions=True)
        await service._load_balancer.release_account_lease(first)
        await service._load_balancer.release_account_lease(replacement)
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("pin", ["age", "replay", "file", "turn", "hard_anchor"])
async def test_soft_physical_lane_preserves_explicit_legacy_owner_before_reconnect(monkeypatch, pin):
    service, session, state, scheduler = _candidate()
    state.bridge_age_required_account_id = session.account.id if pin == "age" else None
    state.replay_required_account_id = session.account.id if pin == "replay" else None
    state.file_required_preferred_account = pin == "file"
    state.hard_continuity_anchor = pin == "hard_anchor"
    if pin == "turn":
        state.affinity_policy = facade._AffinityPolicy(codex_session_source="turn_state")
    state.request_text = '{"type":"response.create","model":"gpt-6.1-sol","input":"complete"}'
    lease = await service._load_balancer.acquire_account_lease(session.account.id, kind="response_create")
    release = AsyncMock(side_effect=service._load_balancer.release_account_lease)
    state.account_response_create_lease = lease
    state.account_response_create_release = release
    state.response_create_gate_acquired = True
    await session.response_create_gate.acquire()
    session.pending_requests.append(state)
    session.queued_request_count = 1
    reconnect, send = AsyncMock(), AsyncMock()
    monkeypatch.setattr(service, "_http_bridge_precreated_retry_allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(service, "_http_bridge_clean_close_retry_max_count", lambda: 0)
    monkeypatch.setattr(service, "_reconnect_http_bridge_session", reconnect)
    monkeypatch.setattr(service, "_http_bridge_text_with_account_installation_id", lambda session, state, text: text)
    monkeypatch.setattr(request_submit, "_send_http_bridge_request_text_with_archive_id", send)
    try:
        assert await service._retry_http_bridge_precreated_request(session) is True
        reconnect.assert_awaited_once_with(
            session,
            request_state=state,
            require_same_account=True,
            **({"require_preferred_account": True} if pin in {"age", "file"} else {}),
        )
        release.assert_not_awaited()
        assert state.account_response_create_lease is lease
        assert session.account.id not in state.excluded_account_ids
        send.assert_awaited_once()
    finally:
        try:
            await _release_websocket_response_create_gate(state, session.response_create_gate, scheduler=scheduler)
            release.assert_awaited_once_with(lease)
            assert state.account_response_create_lease is None
            assert state.account_response_create_release_task is None
            assert (await service._load_balancer.account_pressure_snapshot(session.account.id))[0] == 0
        finally:
            await service._load_balancer.release_account_lease(lease)
            await scheduler.cancel_owned_tasks()
