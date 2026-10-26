"""Conservative physical-socket retirement for complete canonical HTTP resends."""

from dataclasses import dataclass

import anyio

from app.core.clients.proxy import ProxyResponseError
from app.core.openai.requests import ResponsesRequest
from app.modules.proxy._service.support import _HTTPBridgeSession, _HTTPBridgeSessionKey

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

    def allows(self, session: _HTTPBridgeSession, *, now: float) -> bool:
        if (
            session.key != self.key
            or session.key.affinity_kind != "prompt_cache"
            or session.key.strength != "soft"
            or session.codex_session
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


def _complete_original_items(payload: ResponsesRequest) -> bool:
    """Accept only understood full native items; ciphertext stays untouched."""
    if payload.previous_response_id is not None or payload.conversation is not None:
        return False
    if not isinstance(payload.input, list) or not payload.input:
        return False
    pending_calls: set[str] = set()
    seen_calls: set[str] = set()
    for item in payload.input:
        if not isinstance(item, dict):
            return False
        kind = item.get("type", "message")
        # Cancelled tools can carry a complete output with native "incomplete"
        # status. That status is data, not a missing continuation reference.
        allowed_statuses = (None, "completed", "incomplete") if kind == "function_call_output" else (None, "completed")
        if item.get("status") not in allowed_statuses:
            return False
        if kind == "message":
            if item.get("role") not in {"user", "assistant", "system", "developer"}:
                return False
            content = item.get("content")
            if isinstance(content, str):
                continue
            if not isinstance(content, list) or not content:
                return False
            if any(
                not isinstance(part, dict)
                or part.get("type") not in {"input_text", "output_text"}
                or not isinstance(part.get("text"), str)
                for part in content
            ):
                return False
        elif kind == "function_call":
            call_id = item.get("call_id")
            name = item.get("name")
            if (
                not isinstance(call_id, str)
                or not call_id.strip()
                or call_id in seen_calls
                or not isinstance(name, str)
                or not name.strip()
                or not isinstance(item.get("arguments"), str)
                or "encrypted_function_args" in item
                or "encrypted_arguments" in item
            ):
                return False
            seen_calls.add(call_id)
            pending_calls.add(call_id)
        elif kind == "function_call_output":
            call_id = item.get("call_id")
            if not isinstance(call_id, str) or call_id not in pending_calls:
                return False
            if not isinstance(item.get("output"), str):
                return False
            pending_calls.remove(call_id)
        elif kind == "reasoning":
            if any(not isinstance(item.get(field), str) or not item[field] for field in ("id", "encrypted_content")):
                return False
        else:
            return False
    return not pending_calls
