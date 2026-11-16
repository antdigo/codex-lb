"""Conservative physical-socket retirement for complete canonical HTTP resends."""

from dataclasses import dataclass

import anyio

from app.core.clients.proxy import ProxyResponseError
from app.core.openai.requests import ResponsesRequest
from app.modules.proxy._service.support import _HTTPBridgeSession, _HTTPBridgeSessionKey
from app.modules.proxy.input_contract import _complete_original_items as _complete_original_items
from app.modules.proxy.pool_identity import HTTP_BRIDGE_POOL_LANE_KIND

_HTTP_BRIDGE_MAX_IDLE_SOCKET_AGE_SECONDS = 3000.0


class _HTTPBridgeAgeReplacementError(ProxyResponseError):
    """A required socket owner must not be dropped by outer HTTP fallback."""

    def __init__(self, cause: ProxyResponseError) -> None:
        super().__init__(
            cause.status_code,
            cause.payload,
            failure_phase=cause.failure_phase,
            retryable_same_contract=cause.retryable_same_contract,
            failed_session=cause.failed_session,
            failure_detail=cause.failure_detail,
            failure_exception_type=cause.failure_exception_type,
            upstream_status_code=cause.upstream_status_code,
            upstream_error_code=cause.upstream_error_code,
            retry_after_seconds=cause.retry_after_seconds,
            retry_after_header=cause.retry_after_header,
            reservation_released=cause.reservation_released,
            local_pre_dispatch_refusal=cause.local_pre_dispatch_refusal,
        )


@dataclass(slots=True)
class _HTTPBridgeAgeRotation:
    key: _HTTPBridgeSessionKey
    payload: ResponsesRequest
    required_account_id: str | None = None
    own_pool_waiters: int = 0
    independent_pool_lane: bool = False

    def allows(self, session: _HTTPBridgeSession, *, now: float) -> bool:
        if (
            session.key != self.key
            or session.key.affinity_kind not in {"prompt_cache", HTTP_BRIDGE_POOL_LANE_KIND}
            or (session.key.strength != "soft" and session.key.affinity_kind != HTTP_BRIDGE_POOL_LANE_KIND)
            or session.codex_session
            and not (self.independent_pool_lane and session.key.affinity_kind == HTTP_BRIDGE_POOL_LANE_KIND)
            or session.physical_opened_at is None
            or now - session.physical_opened_at < _HTTP_BRIDGE_MAX_IDLE_SOCKET_AGE_SECONDS
        ):
            return False
        # No await between ownership inspection and the caller's detach. Both
        # locks are try-only: uncertainty retains the existing generation.
        try:
            session.lifecycle_lock.acquire_nowait()
        except (anyio.WouldBlock, RuntimeError):
            return False
        try:
            try:
                session.pending_lock.acquire_nowait()
            except (anyio.WouldBlock, RuntimeError):
                return False
            try:
                if (
                    session.pending_requests
                    or session.pool_waiter_count > self.own_pool_waiters
                    or session.queued_request_count
                    or session.admission_waiter_count
                    or session.unanchored_reservation_id is not None
                    or session.handoff_in_progress
                    or session.handoff_future is not None
                    or session.resource_close_task is not None
                    or session.closed
                    or session.reader_processing
                    or session.liveness_settlement_owner is not None
                    or session.upstream_control.reconnect_requested
                    or session.upstream_control.retire_after_drain
                ):
                    return False
                return _complete_original_items(self.payload)
            finally:
                session.pending_lock.release()
        finally:
            session.lifecycle_lock.release()
