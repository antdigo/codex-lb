"""Bounded logical HTTP lanes; physical sockets are owned by the registry.

All mutations are synchronous on the service event loop. Waiting consumes no
socket or account lease; cancellation removes only the caller's ticket.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from uuid import uuid4

from app.core.utils.shared_future import wait_on_shared_future

DEFAULT_POOL_LANES = 7


@dataclass(frozen=True, slots=True)
class PoolScope:
    cache_key: str
    account_id: str
    api_key_id: str | None
    model: str | None
    service_tier: str | None
    cache_identity_mode: str = "shared"


@dataclass(frozen=True, slots=True)
class PoolLease:
    scope: PoolScope
    lane: int
    operation_id: str


@dataclass(slots=True)
class _Ticket:
    operation_id: str
    ready: asyncio.Future[PoolLease]
    anchor: int | None
    order: int


class PoolQueueFull(Exception):
    """Every compatible lane has reached the existing per-lane queue limit."""


class AffinityPool:
    def __init__(self, *, lanes: int = DEFAULT_POOL_LANES, queue_limit: int = 8) -> None:
        if lanes < 1 or queue_limit < 1:
            raise ValueError("pool and queue limits must be positive")
        self._lane_count = lanes
        self._queue_limit = queue_limit
        self._next_order = 0
        self._groups: dict[PoolScope, list[deque[_Ticket]]] = {}

    @property
    def lane_count(self) -> int:
        return self._lane_count

    @property
    def active_operations(self) -> int:
        return sum(bool(queue) for lanes in self._groups.values() for queue in lanes)

    async def acquire(
        self, scope: PoolScope, operation_id: str, *, anchor: int | None = None, preferred_lane: int | None = None
    ) -> PoolLease:
        """Wait for one lane; the caller applies its original absolute deadline."""
        if anchor is not None and not 0 <= anchor < self._lane_count:
            raise ValueError("invalid continuity lane")
        lanes = self._groups.setdefault(scope, [deque() for _ in range(self._lane_count)])
        if any(ticket.operation_id == operation_id for queue in lanes for ticket in queue):
            raise ValueError("operation already admitted to this pool")
        if anchor is not None:
            lane = anchor
        elif preferred_lane is not None and 0 <= preferred_lane < self._lane_count and not lanes[preferred_lane]:
            lane = preferred_lane
        else:
            lane = min(range(self._lane_count), key=lambda index: len(lanes[index]))
        queue = lanes[lane]
        if len(queue) >= self._queue_limit:
            raise PoolQueueFull
        ticket = _Ticket(operation_id, asyncio.get_running_loop().create_future(), anchor, self._next_order)
        self._next_order += 1
        queue.append(ticket)
        if len(queue) == 1:
            ticket.ready.set_result(PoolLease(scope, lane, operation_id))
        try:
            return await asyncio.shield(ticket.ready)
        except BaseException:
            self._remove(scope, lane, ticket)
            raise

    def release(self, lease: PoolLease) -> None:
        """Idempotent release by identity; an old lease cannot release a successor."""
        lanes = self._groups.get(lease.scope)
        if lanes is None:
            return
        queue = lanes[lease.lane]
        if queue and queue[0].ready.done() and not queue[0].ready.cancelled():
            if queue[0].ready.result() is lease:
                self._remove(lease.scope, lease.lane, queue[0])

    def _remove(self, scope: PoolScope, lane: int, ticket: _Ticket) -> None:
        lanes = self._groups[scope]
        lane = next(index for index, queue in enumerate(lanes) if ticket in queue)
        queue = lanes[lane]
        queue.remove(ticket)
        if not ticket.ready.done():
            ticket.ready.cancel()
        self._rebalance(scope)
        if not any(lanes):
            del self._groups[scope]

    def _rebalance(self, scope: PoolScope) -> None:
        lanes = self._groups[scope]
        for lane, queue in enumerate(lanes):
            if queue and queue[0].ready.done():
                continue
            candidates = [
                (ticket.order, source, ticket)
                for source, source_queue in enumerate(lanes)
                for ticket in source_queue
                if not ticket.ready.done() and (ticket.anchor is None or ticket.anchor == lane)
            ]
            if not candidates:
                continue
            _, source, ticket = min(candidates, key=lambda candidate: candidate[0])
            lanes[source].remove(ticket)
            queue.appendleft(ticket)
            ticket.ready.set_result(PoolLease(scope, lane, ticket.operation_id))


@dataclass(frozen=True, slots=True)
class PoolGroup:
    cache_key: str
    api_key_id: str | None
    model: str | None
    service_tier: str | None
    account_id: str | None = None
    cache_identity_mode: str = "shared"


LaneKey = tuple[str, str, str | None]


@dataclass(frozen=True, slots=True)
class PoolLaneReference:
    group: PoolGroup
    scope_account_id: str
    account_id: str
    lane: int
    generation: int


@dataclass(slots=True, eq=False)
class _Bootstrap:
    account: asyncio.Future[str]
    subscribers: int = 0
    nonce: str = field(default_factory=lambda: uuid4().hex)


class PoolRequest:
    def __init__(
        self, coordinator: PoolCoordinator, group: PoolGroup, operation_id: str, bootstrap: _Bootstrap, leader: bool
    ) -> None:
        self._coordinator = coordinator
        self.group = group
        self.operation_id = operation_id
        self._bootstrap = bootstrap
        self._leader = leader
        self._lease: PoolLease | None = None
        self._closed = False
        self.anchor: int | None = None

    @property
    def bootstrap_id(self) -> str:
        return self._bootstrap.nonce

    @property
    def account_id(self) -> str | None:
        return self._lease.scope.account_id if self._lease is not None else None

    async def enter(self, *, preferred_lane: Callable[[str], int | None] | None = None) -> int:
        while not self._leader:
            try:
                account_id = await wait_on_shared_future(self._bootstrap.account)
            except asyncio.CancelledError:
                task = asyncio.current_task()
                if not self._bootstrap.account.cancelled() or (task is not None and task.cancelling()):
                    raise
                self._coordinator._unsubscribe(self.group, self._bootstrap)
                self._bootstrap, self._leader = self._coordinator._subscribe(self.group)
                continue
            self._lease = await self._coordinator._pool.acquire(
                self._scope(account_id),
                self.operation_id,
                anchor=self.anchor,
                preferred_lane=preferred_lane(account_id) if preferred_lane is not None else None,
            )
            return self._lease.lane
        return 0

    async def bind(self, account_id: str, *, on_bound: Callable[[], None] | None = None) -> None:
        if self._lease is not None:
            if self._lease.scope.account_id != account_id:
                raise ValueError("pool lane changed its account owner")
            if on_bound is not None:
                on_bound()
            return
        if not self._leader or self._closed:
            raise ValueError("pool request cannot bind an account")
        # A new bootstrap can revisit an account whose old operations still
        # occupy lanes. Its provisional physical key has a fresh nonce; bind
        # that transport to a ready logical lane rather than waiting on zero.
        self._lease = await self._coordinator._pool.acquire(
            self._scope(account_id),
            self.operation_id,
            anchor=self.anchor,
        )
        if on_bound is not None:
            on_bound()
        self._bootstrap.account.set_result(account_id)

    def track_lane(self, key: LaneKey) -> None:
        self._coordinator._operation_keys[self.operation_id] = key

    def remember_lane(self, key: LaneKey, account_id: str, generation: int) -> None:
        if self._lease is None:
            return
        self._coordinator._operation_keys[self.operation_id] = key
        self._coordinator.remember_lane(
            key,
            PoolLaneReference(
                replace(self.group, account_id=None),
                self._lease.scope.account_id,
                account_id,
                self._lease.lane,
                generation,
            ),
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._coordinator._operation_keys.pop(self.operation_id, None)
        if self._leader and not self._bootstrap.account.done():
            self._bootstrap.account.cancel()
        if self._lease is not None:
            self._coordinator._pool.release(self._lease)
        self._coordinator._unsubscribe(self.group, self._bootstrap)

    def _scope(self, account_id: str) -> PoolScope:
        return PoolScope(
            self.group.cache_key,
            account_id,
            self.group.api_key_id,
            self.group.model,
            self.group.service_tier,
            self.group.cache_identity_mode,
        )


class PoolCoordinator:
    def __init__(self, *, queue_limit: int = 8, max_idle_groups: int = 256) -> None:
        self._max_idle_groups = max_idle_groups
        self._pool = AffinityPool(queue_limit=queue_limit)
        self._max_subscribers = self._pool.lane_count * queue_limit
        self._bootstraps: dict[PoolGroup, _Bootstrap] = {}
        self._retired_bootstraps: set[_Bootstrap] = set()
        self._lane_refs: dict[LaneKey, PoolLaneReference] = {}
        self._operation_keys: dict[str, LaneKey] = {}
        self._warm_keys: dict[tuple[PoolScope, int], LaneKey] = {}

    @property
    def lane_count(self) -> int:
        return self._pool.lane_count

    @property
    def active_operations(self) -> int:
        return self._pool.active_operations

    @property
    def queued_operations(self) -> int:
        subscribers = sum(group.subscribers for group in self._bootstraps.values())
        subscribers += sum(group.subscribers for group in self._retired_bootstraps)
        return max(0, subscribers - self.active_operations)

    def open(self, group: PoolGroup, operation_id: str, *, anchor: int | None = None) -> PoolRequest:
        bootstrap, leader = self._subscribe(group)
        request = PoolRequest(self, group, operation_id, bootstrap, leader)
        request.anchor = anchor
        return request

    def lane_reference(self, key: LaneKey) -> PoolLaneReference | None:
        return self._lane_refs.get(key)

    def warm_key(self, group: PoolGroup, account_id: str, lane: int) -> LaneKey | None:
        scope = PoolScope(
            group.cache_key, account_id, group.api_key_id, group.model, group.service_tier, group.cache_identity_mode
        )
        return self._warm_keys.get((scope, lane))

    def remember_lane(self, key: LaneKey, reference: PoolLaneReference) -> None:
        for warm, value in list(self._warm_keys.items()):
            if value == key:
                del self._warm_keys[warm]
        self._lane_refs.pop(key, None)
        self._lane_refs[key] = reference
        group = reference.group
        scope = PoolScope(
            group.cache_key,
            reference.account_id,
            group.api_key_id,
            group.model,
            group.service_tier,
            group.cache_identity_mode,
        )
        # Auth recovery may move the transport while its original logical lane
        # still fences the operation. Such a transport is anchor-routable but
        # must not be advertised as a warm ordinary lane of either account.
        if reference.scope_account_id == reference.account_id:
            self._warm_keys[(scope, reference.lane)] = key
        if len(self._lane_refs) > self._max_idle_groups * self._pool.lane_count:
            for candidate, value in list(self._lane_refs.items()):
                if candidate not in self._operation_keys.values():
                    del self._lane_refs[candidate]
                    for warm, physical in list(self._warm_keys.items()):
                        if physical == candidate:
                            del self._warm_keys[warm]
                    if len(self._lane_refs) <= self._max_idle_groups * self._pool.lane_count:
                        break

    def account_hint(self, group: PoolGroup) -> str | None:
        bootstrap = self._bootstraps.get(group)
        if bootstrap is None or not bootstrap.account.done() or bootstrap.account.cancelled():
            return None
        return bootstrap.account.result()

    def has_active_scope(self, group: PoolGroup, account_id: str) -> bool:
        scope = PoolScope(
            group.cache_key, account_id, group.api_key_id, group.model, group.service_tier, group.cache_identity_mode
        )
        return scope in self._pool._groups

    def _scope_active(self, group: PoolGroup, bootstrap: _Bootstrap) -> bool:
        if not bootstrap.account.done() or bootstrap.account.cancelled():
            return False
        scope = PoolScope(
            group.cache_key,
            bootstrap.account.result(),
            group.api_key_id,
            group.model,
            group.service_tier,
            group.cache_identity_mode,
        )
        return scope in self._pool._groups

    def discard_idle(self, group: PoolGroup) -> None:
        bootstrap = self._bootstraps.get(group)
        if bootstrap is not None and bootstrap.subscribers == 0 and not self._scope_active(group, bootstrap):
            del self._bootstraps[group]

    def invalidate_account_hint(self, group: PoolGroup, account_id: str) -> None:
        """New independent work reselects; issued tickets keep their old scope."""
        bootstrap = self._bootstraps.get(group)
        if bootstrap is None or self.account_hint(group) != account_id:
            return
        del self._bootstraps[group]
        if bootstrap.subscribers:
            self._retired_bootstraps.add(bootstrap)

    def _subscribe(self, group: PoolGroup) -> tuple[_Bootstrap, bool]:
        bootstrap = self._bootstraps.get(group)
        if bootstrap is None or bootstrap.account.cancelled():
            bootstrap = _Bootstrap(asyncio.get_running_loop().create_future())
            self._bootstraps[group] = bootstrap
            if group.account_id is not None:
                bootstrap.account.set_result(group.account_id)
            leader = group.account_id is None
        else:
            leader = False
        if bootstrap.subscribers >= self._max_subscribers:
            raise PoolQueueFull
        bootstrap.subscribers += 1
        return bootstrap, leader

    def _unsubscribe(self, group: PoolGroup, bootstrap: _Bootstrap) -> None:
        bootstrap.subscribers -= 1
        if bootstrap.subscribers == 0:
            self._retired_bootstraps.discard(bootstrap)
        if bootstrap.subscribers == 0 and self._bootstraps.get(group) is bootstrap:
            if not bootstrap.account.done() or bootstrap.account.cancelled():
                del self._bootstraps[group]
            else:
                # Retain warm account locality while bounding idle metadata.
                del self._bootstraps[group]
                self._bootstraps[group] = bootstrap
                idle = [
                    key
                    for key, value in self._bootstraps.items()
                    if value.subscribers == 0 and not self._scope_active(key, value)
                ]
                for key in idle[: max(0, len(idle) - self._max_idle_groups)]:
                    del self._bootstraps[key]
