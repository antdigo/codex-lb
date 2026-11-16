from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from hashlib import sha256
from typing import TYPE_CHECKING

from app.core.clients.proxy import ProxyResponseError
from app.core.errors import openai_error
from app.core.openai.requests import ResponsesRequest
from app.modules.proxy.affinity import (
    _prompt_cache_key_from_request_model,
    _sticky_key_from_session_header,
    _sticky_key_from_turn_state_header,
)

if TYPE_CHECKING:
    from app.modules.proxy._service.support import _HTTPBridgeSession
    from app.modules.proxy.durable_bridge_coordinator import DurableBridgeSessionCoordinator


_TOOL_RESULTS = frozenset({"function_call_output", "custom_tool_call_output", "apply_patch_call_output"})


def http_continuation_signal(payload: ResponsesRequest, headers: Mapping[str, str]) -> str | None:
    """Classify supplied evidence, before affinity derives a cache key."""
    if payload.previous_response_id is not None:
        return "previous_response"
    if payload.conversation and payload.conversation.strip():
        return "conversation"
    if _sticky_key_from_turn_state_header(headers) is not None:
        return "turn_state"
    if _sticky_key_from_session_header(headers) is not None:
        return "session"
    if _prompt_cache_key_from_request_model(payload) is not None:
        return "prompt_cache"
    return http_history_signal(payload)


def http_history_signal(payload: ResponsesRequest) -> str | None:
    if not isinstance(payload.input, list):
        return None
    assistant_seen = False
    for item in payload.input:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if isinstance(item_type, str) and item_type in _TOOL_RESULTS:
            return "tool_result"
        if item.get("role") == "assistant":
            assistant_seen = True
        elif item.get("role") == "user" and assistant_seen:
            return "history"
    return None


def inferred_http_bridge_key(payload: ResponsesRequest) -> str | None:
    """Soft connection locality only; this key is never a continuity proof.

    API-key isolation is supplied by _HTTPBridgeSessionKey. Hash complete
    initial context, not the truncated prompt-cache prefix. Requests sharing
    that context may share a socket, but must send their own complete history.
    """
    if payload.conversation and payload.conversation.strip():
        identity = [payload.model, payload.conversation]
        kind = "conversation"
    elif http_history_signal(payload) is not None and isinstance(payload.input, list):
        initial_context = []
        for item in payload.input:
            if not isinstance(item, dict):
                continue
            if item.get("role") in ("system", "developer", "user"):
                initial_context.append(item)
            if item.get("role") == "user":
                break
        else:
            # A delta-only tool result cannot identify its conversation.
            return None
        identity = [payload.model, payload.instructions, initial_context]
        kind = "history"
    else:
        return None
    encoded = json.dumps(identity, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()
    return f"http-{kind}:{sha256(encoded).hexdigest()}"


async def reject_raw_projected_continuation(
    previous_response_id: str | None,
    *,
    coordinator: DurableBridgeSessionCoordinator,
    sessions: Iterable[_HTTPBridgeSession],
) -> None:
    """A raw transport has no authority to consume a bridge's projected ID.

    Any local projection is rejection evidence, including foreign or stale
    bindings. Durable existence covers process-local loss and API-scope changes;
    it supplies no owner data or permission to reuse another key's response.
    """
    if previous_response_id is None:
        return
    projected = any(previous_response_id in session.continuation_bindings for session in sessions)
    if not projected:
        try:
            projected = await coordinator.is_projected_response_id(response_id=previous_response_id)
        except Exception as exc:
            raise ProxyResponseError(
                502,
                openai_error("bridge_continuity_persistence_failed", "Response continuity could not be verified."),
                local_pre_dispatch_refusal=True,
            ) from exc
    if projected:
        raise ProxyResponseError(
            502,
            openai_error(
                "previous_response_owner_unavailable",
                "The projected response requires its owning bridge connection; resend the complete conversation.",
            ),
            local_pre_dispatch_refusal=True,
        )
