"""Logical lane permits survive independently of physical socket generations."""

import asyncio
from typing import Any, cast

import pytest

from app.modules.proxy._service.http_bridge.affinity_pool import AffinityPool, PoolScope

pytestmark = pytest.mark.unit


def scope(account="a", api_key="key"):
    return PoolScope("cache", account, api_key, "model", "tier")


@pytest.mark.asyncio
async def test_seven_lanes_run_and_eighth_waits_until_release():
    pool = AffinityPool()
    leases = [await pool.acquire(scope(), str(i)) for i in range(7)]
    assert len({lease.lane for lease in leases}) == 7
    eighth = asyncio.create_task(pool.acquire(scope(), "eighth"))
    await asyncio.sleep(0)
    assert not eighth.done()
    pool.release(leases[3])
    replacement = await asyncio.wait_for(eighth, 1)
    assert replacement.lane == leases[3].lane
    pool.release(replacement)
    for lease in leases:
        pool.release(lease)
    assert pool.active_operations == 0


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_release_owner_or_strand_successor():
    pool = AffinityPool()
    owner = await pool.acquire(scope(), "owner", anchor=0)
    cancelled = asyncio.create_task(pool.acquire(scope(), "cancel", anchor=0))
    successor = asyncio.create_task(pool.acquire(scope(), "successor", anchor=0))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    assert not successor.done()
    pool.release(owner)
    successor_lease = await asyncio.wait_for(successor, 1)
    pool.release(owner)
    assert pool.active_operations == 1
    pool.release(successor_lease)
    assert pool.active_operations == 0


@pytest.mark.asyncio
async def test_scope_and_anchor_isolation():
    pool = AffinityPool()
    owner = await pool.acquire(scope(), "owner", anchor=0)
    other_account = await pool.acquire(scope(account="b"), "other", anchor=0)
    other_tenant = await pool.acquire(scope(api_key="other"), "tenant", anchor=0)
    sibling = await pool.acquire(scope(), "sibling", anchor=1)
    assert owner.lane != sibling.lane
    for lease in (owner, other_account, other_tenant, sibling):
        pool.release(lease)
    assert pool.active_operations == 0


@pytest.mark.asyncio
async def test_returning_account_bootstrap_uses_ready_lane_without_moving_old_anchor():
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup

    coordinator = PoolCoordinator()
    group = PoolGroup("cache", "key", "model", None)
    old = coordinator.open(group, "old-a")
    await old.enter()
    await old.bind("a")
    old.remember_lane(("internal_pool_lane", "old-physical-a", "key"), "a", 1)
    coordinator.invalidate_account_hint(group, "a")
    other = coordinator.open(group, "b")
    await other.enter()
    await other.bind("b")
    coordinator.invalidate_account_hint(group, "b")
    returning = coordinator.open(group, "new-a")
    await returning.enter()
    bind = asyncio.create_task(returning.bind("a"))
    try:
        await asyncio.wait_for(asyncio.shield(bind), 0.2)
        returning.remember_lane(("internal_pool_lane", "new-physical-a", "key"), "a", 1)
        old_ref = coordinator.lane_reference(("internal_pool_lane", "old-physical-a", "key"))
        new_ref = coordinator.lane_reference(("internal_pool_lane", "new-physical-a", "key"))
        assert old_ref is not None and new_ref is not None
        assert old_ref.lane == 0 and new_ref.lane == 1
        assert coordinator.warm_key(group, "a", 1) == ("internal_pool_lane", "new-physical-a", "key")
        assert coordinator.active_operations == 3
    finally:
        bind.cancel()
        await asyncio.gather(bind, return_exceptions=True)
        for request in (returning, other, old):
            request.close()
    assert coordinator.active_operations == coordinator.queued_operations == 0


@pytest.mark.asyncio
async def test_bootstrap_waits_for_account_then_allows_independent_lanes():
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup

    coordinator = PoolCoordinator()
    group = PoolGroup("cache", "key", "model", "tier")
    first = coordinator.open(group, "first")
    others = [coordinator.open(group, str(i)) for i in range(6)]
    assert await first.enter() == 0
    tasks = [asyncio.create_task(request.enter()) for request in others]
    await asyncio.sleep(0)
    assert all(not task.done() for task in tasks)
    await first.bind("account")
    lanes = await asyncio.gather(*tasks)
    assert set(lanes) == set(range(1, 7))
    assert all(request.account_id == "account" for request in others)
    for request in (first, *others):
        request.close()
    assert coordinator.active_operations == 0


@pytest.mark.asyncio
async def test_cancelled_bootstrap_leader_does_not_cancel_independent_request():
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup

    coordinator = PoolCoordinator()
    group = PoolGroup("cache", None, "model", None)
    first = coordinator.open(group, "first")
    next_request = coordinator.open(group, "next")
    sibling = coordinator.open(group, "sibling")
    assert await first.enter() == 0
    waiter = asyncio.create_task(next_request.enter())
    sibling_waiter = asyncio.create_task(sibling.enter())
    await asyncio.sleep(0)
    first.close()
    done, _pending = await asyncio.wait((waiter, sibling_waiter), timeout=1, return_when=asyncio.FIRST_COMPLETED)
    assert len(done) == 1
    elected = next(iter(done))
    assert elected.result() == 0
    winner, follower = (next_request, sibling_waiter) if elected is waiter else (sibling, waiter)
    await winner.bind("account")
    assert await asyncio.wait_for(follower, 1) == 1
    next_request.close()
    sibling.close()
    assert coordinator.active_operations == 0


@pytest.mark.asyncio
async def test_ready_fifo_older_flexible_waiter_precedes_younger_anchor():
    pool = AffinityPool(lanes=2)
    first = await pool.acquire(scope(), "first", anchor=0)
    second = await pool.acquire(scope(), "second", anchor=1)
    older = asyncio.create_task(pool.acquire(scope(), "older"))
    younger = asyncio.create_task(pool.acquire(scope(), "younger", anchor=1))
    await asyncio.sleep(0)
    pool.release(second)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    try:
        assert older.done(), "An older movable waiter must use the freed lane"
        assert not younger.done()
    finally:
        pool.release(first)
        for task in (older, younger):
            task.cancel()
            try:
                lease = await task
            except asyncio.CancelledError:
                continue
            pool.release(lease)


@pytest.mark.asyncio
async def test_sibling_lane_identity_is_replica_local_and_owner_routed():
    from types import SimpleNamespace

    from app.core.clock import REAL_CLOCK, REAL_SCHEDULER
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy._service.http_bridge.pool_routing import PoolRequestContext
    from app.modules.proxy._service.support import _HTTPBridgeSessionKey

    contexts = []
    keys = []
    payload = ResponsesRequest.model_validate(
        {"model": "gpt-5.4", "instructions": "", "input": [{"role": "user", "content": "complete"}]}
    )
    try:
        for replica in ("replica-a", "replica-b"):
            host = SimpleNamespace(_http_bridge_affinity_pool=None, _http_bridge_sessions={})
            leader, sibling = PoolRequestContext(), PoolRequestContext()
            contexts.extend((leader, sibling))
            options: dict[str, Any] = dict(
                lookup=None,
                instance_id=replica,
                incoming_turn_state=None,
                forwarded_request=False,
                responses_lite=False,
                queue_limit=8,
                deadline=REAL_CLOCK.monotonic() + 30,
                clock=REAL_CLOCK,
                scheduler=REAL_SCHEDULER,
            )
            base = _HTTPBridgeSessionKey("prompt_cache", "same-key", "tenant")
            first_key = await leader.choose(host, base, payload, **options)
            await leader.bind(
                cast(
                    Any,
                    SimpleNamespace(
                        account=SimpleNamespace(id="same-account"),
                        key=first_key,
                        connection_generation=0,
                        pool_waiter_count=0,
                    ),
                )
            )
            keys.append(await sibling.choose(host, base, payload, **options))
        assert keys[0] != keys[1], "Independent replica pools must not compete for one durable lane row"
        assert all(key.strength == "hard" for key in keys)
    finally:
        for context in contexts:
            context.close()


@pytest.mark.asyncio
async def test_bootstrap_work_blocks_restart_before_a_socket_exists():
    from contextlib import nullcontext

    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup
    from app.modules.proxy.service import ProxyService

    service = ProxyService(cast(Any, nullcontext()))
    service._http_bridge_affinity_pool = PoolCoordinator()
    request = service._http_bridge_affinity_pool.open(PoolGroup("cache", None, "model", None), "queued")
    assert await request.enter() == 0
    try:
        snapshot = service.http_bridge_activity_snapshot_nowait()
        assert snapshot["http_bridge_restart_blocking"] is True
        assert snapshot["http_bridge_pool_queued_operations"] == 1
    finally:
        request.close()
    assert service.http_bridge_activity_snapshot_nowait()["http_bridge_restart_blocking"] is False


@pytest.mark.asyncio
async def test_idle_group_keeps_account_and_can_select_matching_warm_lane():
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup

    coordinator = PoolCoordinator()
    group = PoolGroup("cache", None, "model", None)
    first = coordinator.open(group, "first")
    assert await first.enter() == 0
    await first.bind("account")
    first.close()
    next_request = coordinator.open(group, "follow-up")
    try:
        assert await next_request.enter(preferred_lane=lambda _account: 3) == 3
        assert next_request.account_id == "account"
    finally:
        next_request.close()


@pytest.mark.asyncio
async def test_initial_lane_key_isolated_between_model_and_tier_groups():
    from types import SimpleNamespace

    from app.core.clock import REAL_CLOCK, REAL_SCHEDULER
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy._service.http_bridge.pool_routing import PoolRequestContext
    from app.modules.proxy._service.support import _HTTPBridgeSessionKey

    host = SimpleNamespace(_http_bridge_affinity_pool=None, _http_bridge_sessions={})
    contexts, keys = [], []
    try:
        for model, tier in (("gpt-5.4", None), ("gpt-5.1", None), ("gpt-5.4", "priority")):
            context = PoolRequestContext()
            contexts.append(context)
            payload = ResponsesRequest.model_validate(
                {
                    "model": model,
                    "instructions": "",
                    "service_tier": tier,
                    "input": [{"role": "user", "content": "complete"}],
                }
            )
            keys.append(
                await context.choose(
                    host,
                    _HTTPBridgeSessionKey("prompt_cache", "cache", None),
                    payload,
                    lookup=None,
                    instance_id="replica",
                    incoming_turn_state=None,
                    forwarded_request=False,
                    responses_lite=False,
                    queue_limit=8,
                    deadline=REAL_CLOCK.monotonic() + 30,
                    clock=REAL_CLOCK,
                    scheduler=REAL_SCHEDULER,
                )
            )
        assert len(set(keys)) == 3, "Each compatible group needs its own physical lane zero"
    finally:
        for context in contexts:
            context.close()


@pytest.mark.asyncio
async def test_auth_migration_updates_physical_owner_without_releasing_logical_fence():
    from dataclasses import replace

    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup

    coordinator = PoolCoordinator()
    group = PoolGroup("cache", "tenant", "model", None)
    request = coordinator.open(group, "auth-migrating")
    assert await request.enter() == 0
    await request.bind("a")
    key = ("internal_pool_lane", "physical", "tenant")
    request.remember_lane(key, "a", 0)
    assert coordinator.warm_key(group, "a", 0) == key
    request.remember_lane(key, "b", 1)
    reference = coordinator.lane_reference(key)
    assert reference is not None
    assert (reference.scope_account_id, reference.account_id, reference.generation) == ("a", "b", 1)
    assert coordinator.warm_key(group, "a", 0) is None
    assert coordinator.warm_key(group, "b", 0) is None
    continuation = coordinator.open(replace(group, account_id=reference.scope_account_id), "continuation", anchor=0)
    waiter = asyncio.create_task(continuation.enter())
    await asyncio.sleep(0)
    assert not waiter.done()
    request.close()
    assert await asyncio.wait_for(waiter, 1) == 0
    continuation.close()
    assert coordinator.active_operations == 0


@pytest.mark.asyncio
async def test_reference_cache_bounds_stale_keys_while_preserving_active_operation():
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolCoordinator, PoolGroup, PoolLaneReference

    coordinator = PoolCoordinator(max_idle_groups=1)
    group = PoolGroup("cache", None, "model", None)
    owner = coordinator.open(group, "owner")
    await owner.enter()
    await owner.bind("account")
    current = ("internal_pool_lane", "active", None)
    owner.remember_lane(current, "account", 0)
    reference = PoolLaneReference(group, "account", "account", 0, 0)
    for index in range(12):
        coordinator.remember_lane(("internal_pool_lane", f"stale-{index}", None), reference)
    assert coordinator.lane_reference(current) is not None
    assert coordinator.lane_reference(("internal_pool_lane", "stale-0", None)) is None
    owner.close()


@pytest.mark.asyncio
async def test_new_bootstrap_after_auth_move_cannot_alias_old_physical_generation():
    from types import SimpleNamespace

    from app.core.clock import REAL_CLOCK, REAL_SCHEDULER
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy._service.http_bridge.pool_routing import PoolRequestContext
    from app.modules.proxy._service.support import _HTTPBridgeSessionKey
    from tests.unit.test_http_bridge_idle_leases import _make_bridge_session

    host = SimpleNamespace(_http_bridge_affinity_pool=None, _http_bridge_sessions={})
    first, independent, anchor = PoolRequestContext(), PoolRequestContext(), PoolRequestContext()
    payload = ResponsesRequest.model_validate(
        {"model": "gpt-5.4", "instructions": "", "input": [{"role": "user", "content": "complete"}]}
    )
    base = _HTTPBridgeSessionKey("prompt_cache", "cache", None)
    options: dict[str, Any] = dict(
        instance_id="replica",
        incoming_turn_state=None,
        forwarded_request=False,
        responses_lite=False,
        queue_limit=8,
        deadline=REAL_CLOCK.monotonic() + 30,
        clock=REAL_CLOCK,
        scheduler=REAL_SCHEDULER,
    )
    try:
        old_key = await first.choose(host, base, payload, lookup=None, **options)
        session = _make_bridge_session()
        session.key, session.request_model, session.account.id = old_key, "gpt-5.4", "a"
        host._http_bridge_sessions[old_key] = session
        await first.bind(session)
        session.account.id = "b"
        session.connection_generation += 1
        first.observe_generation(session)
        first.close()
        new_key = await independent.choose(host, base, payload, lookup=None, **options)
        assert new_key != old_key, "A fresh bootstrap cannot assign the old B socket a second logical queue"
        lookup = SimpleNamespace(
            owner_instance_id="replica",
            canonical_kind=old_key.affinity_kind,
            canonical_key=old_key.affinity_key,
            account_id="b",
        )
        delta = payload.model_copy(update={"previous_response_id": "resp_prior", "input": "next"})
        assert await anchor.choose(host, base, delta, lookup=cast(Any, lookup), **options) == old_key
        await anchor.bind(session)
        assert anchor.account_id == "b"
    finally:
        for context in (first, independent, anchor):
            context.close()


@pytest.mark.asyncio
async def test_bootstrap_publishes_physical_lane_before_followers_see_account(monkeypatch):
    from contextlib import nullcontext

    from app.core.clock import REAL_CLOCK, REAL_SCHEDULER
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy._service.http_bridge.affinity_pool import PoolRequest
    from app.modules.proxy._service.http_bridge.pool_routing import PoolRequestContext
    from app.modules.proxy._service.support import _HTTPBridgeSessionKey
    from app.modules.proxy.service import ProxyService
    from tests.unit.test_http_bridge_idle_leases import _make_bridge_session

    service = ProxyService(cast(Any, nullcontext()))
    first, follower = PoolRequestContext(), PoolRequestContext()
    published, release = asyncio.Event(), asyncio.Event()
    original_bind = PoolRequest.bind

    async def held_bind(request, *args, **kwargs):
        await original_bind(request, *args, **kwargs)
        published.set()
        await release.wait()

    monkeypatch.setattr(PoolRequest, "bind", held_bind)
    payload = ResponsesRequest.model_validate(
        {"model": "gpt-5.4", "instructions": "", "input": [{"role": "user", "content": "complete"}]}
    )
    base = _HTTPBridgeSessionKey("thread_header", "shared-thread", None)
    options: Any = dict(
        lookup=None,
        instance_id="replica",
        incoming_turn_state=None,
        forwarded_request=False,
        responses_lite=False,
        queue_limit=8,
        deadline=REAL_CLOCK.monotonic() + 10,
        clock=REAL_CLOCK,
        scheduler=REAL_SCHEDULER,
    )
    key = await first.choose(service, base, payload, **options)
    session = _make_bridge_session()
    session.key, session.request_model, session.account.id = key, "gpt-5.4", "owner"
    service._http_bridge_sessions[key] = session
    binding = asyncio.create_task(first.bind(session))
    try:
        await asyncio.wait_for(published.wait(), 1)
        await follower.choose(service, base, payload, **options)
        assert follower.account_id == "owner", "A published account must retain the ready physical-lane reference"
        assert first.request is not None and follower.request is not None
        assert first.request.bootstrap_id == follower.request.bootstrap_id
    finally:
        release.set()
        await binding
        follower.close()
        first.close()


@pytest.mark.asyncio
async def test_selected_warm_generation_is_not_idle_evictable_before_bind():
    from contextlib import nullcontext
    from typing import Any, cast

    from app.core.clock import REAL_CLOCK, REAL_SCHEDULER
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy._service.http_bridge.pool_routing import PoolRequestContext
    from app.modules.proxy._service.support import _HTTPBridgeSessionKey
    from app.modules.proxy.service import ProxyService
    from tests.unit.test_http_bridge_idle_leases import _make_bridge_session

    service = ProxyService(cast(Any, nullcontext()))
    first, selected = PoolRequestContext(), PoolRequestContext()
    payload = ResponsesRequest.model_validate(
        {"model": "gpt-5.4", "instructions": "", "input": [{"role": "user", "content": "complete"}]}
    )
    base = _HTTPBridgeSessionKey("prompt_cache", "cache", None)
    options: Any = dict(
        lookup=None,
        instance_id="replica",
        incoming_turn_state=None,
        forwarded_request=False,
        responses_lite=False,
        queue_limit=8,
        deadline=REAL_CLOCK.monotonic() + 30,
        clock=REAL_CLOCK,
        scheduler=REAL_SCHEDULER,
    )
    try:
        key = await first.choose(service, base, payload, **options)
        session = _make_bridge_session()
        session.key, session.request_model, session.account.id = key, "gpt-5.4", "pin-owner"
        session.last_used_at, session.idle_ttl_seconds = 0, 1
        service._http_bridge_sessions[key] = session
        await first.bind(session)
        first.close()
        assert await selected.choose(service, base, payload, **options) == key
        assert selected.request is not None
        assert service._prune_http_bridge_sessions_locked() == [], "Selected warm generation is live admission work"
        selected.close()
        assert service._prune_http_bridge_sessions_locked() == [session]
    finally:
        first.close()
        selected.close()


def test_advisory_locality_does_not_relax_strict_continuation_prefix_authority():
    from app.core.types import JsonValue
    from app.modules.proxy import service

    items: list[JsonValue] = [{"role": "user", "content": "synthetic full context"}]
    fingerprint = service._fingerprint_input_items(items)
    assert not service._input_prefix_matches_stored_context(items, stored_count=1, stored_fingerprint=fingerprint)
    assert service._input_prefix_matches_stored_context(
        [*items, {"role": "user", "content": "synthetic next"}], stored_count=1, stored_fingerprint=fingerprint
    )
    assert not service._input_prefix_matches_stored_context(items, stored_count=0, stored_fingerprint=fingerprint)
