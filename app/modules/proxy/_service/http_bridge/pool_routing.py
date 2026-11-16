"""Bind complete independent HTTP requests to bounded, account-owned lanes."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from hashlib import sha256
from typing import Protocol
from uuid import uuid4

from app.core.clients.proxy import ProxyResponseError
from app.core.clients.thread_cache_identity import ThreadCachePolicy
from app.core.clock import Clock, Scheduler
from app.core.errors import openai_error
from app.core.openai.requests import ResponsesRequest
from app.modules.api_keys.service import ApiKeyData
from app.modules.proxy._service.http_bridge.affinity_pool import (
    LaneKey,
    PoolCoordinator,
    PoolGroup,
    PoolQueueFull,
    PoolRequest,
)
from app.modules.proxy._service.http_bridge.helpers import (
    _http_bridge_compatible,
    _http_bridge_session_account_active,
    _http_bridge_session_allows_api_key,
)
from app.modules.proxy._service.http_bridge.service_stubs import _fingerprint_input_items
from app.modules.proxy._service.support import _HTTPBridgeSession, _HTTPBridgeSessionKey
from app.modules.proxy.account_cache import is_account_routing_unavailable
from app.modules.proxy.durable_bridge_coordinator import DurableBridgeLookup
from app.modules.proxy.input_contract import _complete_original_items
from app.modules.proxy.pool_identity import HTTP_BRIDGE_POOL_LANE_KIND, pool_instance_prefix


class PoolService(Protocol):
    _http_bridge_affinity_pool: PoolCoordinator | None
    _http_bridge_sessions: dict[_HTTPBridgeSessionKey, _HTTPBridgeSession]


@dataclass(slots=True)
class PoolRequestContext:
    request: PoolRequest | None = None
    independent: bool = True
    _actual_account_id: str | None = None
    source_affinity_key: _HTTPBridgeSessionKey | None = None
    _pinned_session: _HTTPBridgeSession | None = field(default=None, repr=False)
    _deadline: float | None = None
    _clock: Clock | None = field(default=None, repr=False)
    _scheduler: Scheduler | None = field(default=None, repr=False)

    @property
    def independent_admission(self) -> bool:
        """Only an admitted full-context operation owns independent authority."""
        return self.request is not None and self.independent

    @property
    def account_id(self) -> str | None:
        return self._actual_account_id or (self.request.account_id if self.request is not None else None)

    async def choose(
        self,
        service: PoolService,
        key: _HTTPBridgeSessionKey,
        payload: ResponsesRequest,
        *,
        lookup: DurableBridgeLookup | None,
        instance_id: str,
        incoming_turn_state: str | None,
        forwarded_request: bool,
        responses_lite: bool,
        queue_limit: int,
        deadline: float,
        clock: Clock,
        scheduler: Scheduler,
        api_key: ApiKeyData | None = None,
        restart: bool = False,
        cache_policy: ThreadCachePolicy = ThreadCachePolicy(),
    ) -> _HTTPBridgeSessionKey:
        self.source_affinity_key = key
        self._deadline, self._clock, self._scheduler = deadline, clock, scheduler
        coordinator = service._http_bridge_affinity_pool
        if (
            coordinator is not None
            and lookup is not None
            and lookup.owner_instance_id in {None, instance_id}
            and (payload.previous_response_id is not None or incoming_turn_state is not None)
        ):
            target = _HTTPBridgeSessionKey(lookup.canonical_kind, lookup.canonical_key, key.api_key_id)
            identity = (target.affinity_kind, target.affinity_key, target.api_key_id)
            reference = coordinator.lane_reference(identity)
            session = service._http_bridge_sessions.get(target)
            if reference is not None and session is not None and lookup.account_id == session.account.id:
                self.independent = False
                self._actual_account_id = session.account.id
                group = replace(reference.group, account_id=reference.scope_account_id)
                await self._acquire(
                    coordinator,
                    group,
                    deadline=deadline,
                    clock=clock,
                    scheduler=scheduler,
                    anchor=reference.lane,
                    tracked_key=identity,
                    pin_session=session,
                )
                return target
        # Unsafe/legacy work retains its established routing. Known exact
        # anchors above join the same queue as independent work on their lane.
        if (
            key.affinity_kind not in {"prompt_cache", "session_header", "thread_header"}
            or key.affinity_kind == "prompt_cache"
            and key.strength != "soft"
            or payload.previous_response_id is not None
            or payload.conversation is not None
            or incoming_turn_state is not None
            or forwarded_request
            or responses_lite
            or restart
            or (lookup is not None and lookup.owner_instance_id not in {None, instance_id})
        ):
            return key
        proof_payload = payload
        if isinstance(payload.input, str):
            proof_payload = payload.model_copy(update={"input": [{"role": "user", "content": payload.input}]})
        if not _complete_original_items(proof_payload):
            return key
        if service._http_bridge_affinity_pool is None:
            service._http_bridge_affinity_pool = PoolCoordinator(queue_limit=queue_limit)
        coordinator = service._http_bridge_affinity_pool
        group_key = (
            key.affinity_key
            if key.affinity_kind == "prompt_cache"
            else json.dumps([key.affinity_kind, key.affinity_key], separators=(",", ":"))
        )
        group = PoolGroup(
            group_key, key.api_key_id, payload.model, payload.service_tier, cache_identity_mode=cache_policy.mode
        )

        def compatible_lanes(account_id: str) -> list[tuple[int, _HTTPBridgeSession]]:
            result = []
            for lane in range(coordinator.lane_count):
                candidate_key = _lane_key(coordinator, key, group, account_id, instance_id, lane)
                session = service._http_bridge_sessions.get(candidate_key)
                if (
                    session is not None
                    and not session.closed
                    and session.cache_identity_mode == cache_policy.mode
                    and session.account.id == account_id
                    and not session.upstream_control.retire_after_drain
                    and _http_bridge_session_account_active(session)
                    and _http_bridge_session_allows_api_key(session, api_key)
                    and _http_bridge_compatible(session, payload.model, payload.service_tier)
                ):
                    result.append((lane, session))
            return result

        def preferred_lane(account_id: str) -> int | None:
            candidates = []
            for lane, session in compatible_lanes(account_id):
                if session.pending_requests or session.queued_request_count or session.admission_waiter_count:
                    continue
                count = session.last_completed_input_count
                # Advisory locality includes equality; it grants no authority
                # to trim input or inherit a continuation from this lane.
                matches = (
                    count > 0
                    and session.last_completed_input_prefix_fingerprint is not None
                    and isinstance(payload.input, list)
                    and len(payload.input) >= count
                    and _fingerprint_input_items(payload.input[:count])
                    == session.last_completed_input_prefix_fingerprint
                )
                candidates.append((count if matches else 0, -lane))
            return -max(candidates)[1] if candidates else None

        hint = service._http_bridge_affinity_pool.account_hint(group)
        if hint is not None and not compatible_lanes(hint):
            owned_sessions = [
                session for session in service._http_bridge_sessions.values() if session.account.id == hint
            ]
            ineligible = (
                api_key is not None
                and api_key.account_assignment_scope_enabled
                and hint not in api_key.assigned_account_ids
                or is_account_routing_unavailable(hint)
                or bool(owned_sessions)
                and not any(_http_bridge_session_account_active(s) for s in owned_sessions)
            )
            # Temporary lack of a warm generation is not account rejection.
            # Issued lanes keep scheduling ownership during bind/replacement;
            # only positive ineligibility or an entirely idle stale hint may
            # bootstrap a different account for new independent requests.
            if ineligible or not coordinator.has_active_scope(group, hint):
                coordinator.invalidate_account_hint(group, hint)
        lane = await self._acquire(
            coordinator, group, deadline=deadline, clock=clock, scheduler=scheduler, preferred_lane=preferred_lane
        )
        assert self.request is not None
        selected = _lane_key(
            coordinator,
            key,
            group,
            self.account_id,
            instance_id,
            lane,
            bootstrap_id=self.request.bootstrap_id if self.account_id is None else None,
        )
        self.request.track_lane((selected.affinity_kind, selected.affinity_key, selected.api_key_id))
        selected_session = service._http_bridge_sessions.get(selected)
        if selected_session is not None:
            self._pin(selected_session)
        return selected

    async def _acquire(
        self,
        coordinator: PoolCoordinator,
        group: PoolGroup,
        *,
        deadline: float,
        clock: Clock,
        scheduler: Scheduler,
        anchor: int | None = None,
        preferred_lane: Callable[[str], int | None] | None = None,
        tracked_key: LaneKey | None = None,
        pin_session: _HTTPBridgeSession | None = None,
    ) -> int:
        try:
            self.request = coordinator.open(group, uuid4().hex, anchor=anchor)
            if tracked_key is not None:
                self.request.track_lane(tracked_key)
            if pin_session is not None:
                self._pin(pin_session)
            return await scheduler.wait_for(
                self.request.enter(preferred_lane=preferred_lane), timeout=max(0.0, deadline - clock.monotonic())
            )
        except PoolQueueFull as exc:
            raise ProxyResponseError(
                429,
                openai_error(
                    "bridge_queue_full", "HTTP responses session bridge queue is full", error_type="rate_limit_error"
                ),
                local_pre_dispatch_refusal=True,
            ) from exc
        except TimeoutError as exc:
            raise ProxyResponseError(
                503,
                openai_error("response_execution_wait_timeout", "Timed out waiting for an HTTP bridge lane"),
                local_pre_dispatch_refusal=True,
            ) from exc

    def affinity_authority(self, physical_key: _HTTPBridgeSessionKey) -> _HTTPBridgeSessionKey:
        if self.request is not None and self.independent and self.source_affinity_key is not None:
            return replace(self.source_affinity_key, strength="soft")
        return physical_key

    async def bind(self, session: _HTTPBridgeSession) -> None:
        if self.request is not None:
            self._pin(session)
            assert self._scheduler is not None and self._clock is not None and self._deadline is not None
            try:
                await self._scheduler.wait_for(
                    self.request.bind(
                        self.request.account_id or session.account.id,
                        on_bound=lambda: self.observe_generation(session),
                    ),
                    timeout=max(0.0, self._deadline - self._clock.monotonic()),
                )
            except TimeoutError as exc:
                raise ProxyResponseError(
                    503,
                    openai_error("response_execution_wait_timeout", "Timed out binding an HTTP bridge lane"),
                    local_pre_dispatch_refusal=True,
                ) from exc

    def owns_pin(self, session: _HTTPBridgeSession) -> bool:
        return self._pinned_session is session

    def _pin(self, session: _HTTPBridgeSession) -> None:
        if self._pinned_session is session:
            return
        self._unpin()
        self._pinned_session = session
        session.pool_waiter_count += 1

    def _unpin(self) -> None:
        session = self._pinned_session
        if session is not None:
            self._pinned_session = None
            session.pool_waiter_count -= 1

    def observe_generation(self, session: _HTTPBridgeSession) -> None:
        if self.request is not None:
            self._pin(session)
            self._actual_account_id = session.account.id
            self.request.remember_lane(
                (session.key.affinity_kind, session.key.affinity_key, session.key.api_key_id),
                session.account.id,
                session.connection_generation,
            )

    def close(self) -> None:
        self._unpin()
        if self.request is not None:
            self.request.close()


def _lane_key(
    coordinator: PoolCoordinator,
    canonical: _HTTPBridgeSessionKey,
    group: PoolGroup,
    account_id: str | None,
    instance_id: str,
    lane: int,
    *,
    bootstrap_id: str | None = None,
) -> _HTTPBridgeSessionKey:
    if account_id is not None:
        remembered = coordinator.warm_key(group, account_id, lane)
        if remembered is not None:
            return _HTTPBridgeSessionKey(*remembered)
    components = [group.cache_key, group.api_key_id, account_id, group.model, group.service_tier, lane, bootstrap_id]
    if group.cache_identity_mode != "shared":
        components.append(group.cache_identity_mode)
    identity = json.dumps(
        components,
        separators=(",", ":"),
    )
    value = f"{pool_instance_prefix(instance_id)}{sha256(identity.encode()).hexdigest()}.{lane}"
    return _HTTPBridgeSessionKey(HTTP_BRIDGE_POOL_LANE_KIND, value, group.api_key_id)
