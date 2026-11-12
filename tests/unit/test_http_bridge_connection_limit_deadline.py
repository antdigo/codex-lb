import asyncio
from collections import deque
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from app.modules.proxy import service as facade
from app.modules.proxy._service.http_bridge import mixin, request_submit
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
