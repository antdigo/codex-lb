"""Exact public-to-physical response translation within the owning lane."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING

from app.core.clients.proxy import ProxyResponseError
from app.core.errors import openai_error
from app.core.types import JsonValue

if TYPE_CHECKING:
    from app.modules.proxy._service.support import _HTTPBridgeSession, _HTTPBridgeSessionKey
    from app.modules.proxy.durable_bridge_coordinator import DurableBridgeSessionCoordinator


@dataclass(frozen=True, slots=True)
class ContinuationBinding:
    public_response_id: str
    upstream_response_id: str
    account_id: str
    lane_key: _HTTPBridgeSessionKey
    generation: int
    owner_epoch: int | None

    def belongs_to(self, session: _HTTPBridgeSession) -> bool:
        return (
            self.account_id == session.account.id
            and self.lane_key == session.key
            and self.generation == session.connection_generation
            and self.owner_epoch == session.durable_owner_epoch
        )


def response_snapshot_matches(requested_response_id: str | None, snapshot_response_id: str | None) -> bool:
    """An explicit anchor cannot borrow a newer lane's context evidence."""
    return snapshot_response_id is not None and requested_response_id in (None, snapshot_response_id)


def owns_completed_response_context(session: _HTTPBridgeSession, response_id: str | None) -> bool:
    """Match current context by exact physical ID or a verified projection."""
    if (
        response_id is None
        or session.last_completed_response_id is None
        or session.last_completed_response_account_id != session.account.id
    ):
        return False
    if response_id == session.last_completed_response_id:
        return True
    binding = session.continuation_bindings.get(response_id)
    return bool(
        binding is not None
        and binding.belongs_to(session)
        and binding.upstream_response_id == session.last_completed_response_id
    )


_MAX_COMPLETED_OUTPUT_IDENTITIES = 256


def response_output_identity(item: JsonValue) -> str | None:
    """Compare meaningful output, preserving unknown fields and opaque data."""
    if isinstance(item, dict):
        item = dict(item)
        # Ignore only bookkeeping already normalized on HTTP input. Unknown
        # metadata and opaque reasoning content remain part of the proof.
        call_id = item.get("call_id")
        if item.get("type") == "message" or (
            item.get("type") in {"function_call", "custom_tool_call", "apply_patch_call"}
            and isinstance(call_id, str)
            and call_id.strip()
        ):
            item.pop("id", None)
        if item.get("status") in (None, "completed", "in_progress"):
            item.pop("status", None)
        metadata = item.get("internal_chat_message_metadata_passthrough")
        if isinstance(metadata, dict) and metadata.keys() <= {"turn_id"}:
            item.pop("internal_chat_message_metadata_passthrough")
        if item.get("type") == "function_call" and "arguments" in item:
            arguments = item["arguments"]
            if isinstance(arguments, str):
                try:
                    json.loads(arguments)
                except (ValueError, RecursionError):
                    item["arguments"] = ["wire", arguments]
                else:
                    # Validation is not equality authority: Python decoding
                    # loses numeric precision, duplicate keys and escapes.
                    normalized = re.sub(
                        r'"(?:\\.|[^"\\])*"|[ \t\r\n]+',
                        lambda match: match[0] if match[0].startswith('"') else "",
                        arguments,
                    )
                    item["arguments"] = ["json", normalized]
            else:
                item["arguments"] = ["invalid", arguments]
    try:
        encoded = json.dumps(item, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    except (ValueError, TypeError, RecursionError):
        # Optional deduplication evidence cannot fail a completed response.
        return None
    return sha256(encoded.encode()).hexdigest()


def completed_output_identities(payload: dict[str, JsonValue] | None) -> tuple[str, ...]:
    response = payload.get("response") if isinstance(payload, dict) else None
    output = response.get("output") if isinstance(response, dict) else None
    if not isinstance(output, list) or len(output) > _MAX_COMPLETED_OUTPUT_IDENTITIES:
        return ()
    identities: list[str] = []
    for item in output:
        identity = response_output_identity(item)
        if identity is None:
            return ()
        identities.append(identity)
    return tuple(identities)


async def projection_required(
    coordinator: DurableBridgeSessionCoordinator, session: _HTTPBridgeSession, public_response_id: str
) -> bool:
    """Read the durable marker after acquiring the predecessor's execution slot."""
    try:
        owner = await coordinator.lookup_projected_response_id(
            response_id=public_response_id, api_key_id=session.key.api_key_id
        )
    except Exception as exc:
        raise ProxyResponseError(
            502,
            openai_error("bridge_continuity_persistence_failed", "Response continuity could not be verified."),
            local_pre_dispatch_refusal=True,
        ) from exc
    if owner is None:
        return False
    if (
        owner.session_id != session.durable_session_id
        or owner.account_id != session.account.id
        or owner.owner_epoch != session.durable_owner_epoch
    ):
        raise ProxyResponseError(
            502,
            openai_error(
                "previous_response_owner_unavailable",
                "The projected response owner changed; resend the complete conversation.",
            ),
            local_pre_dispatch_refusal=True,
        )
    return True


def translate_previous_response_id(session: _HTTPBridgeSession, text: str, *, projection_required: bool = False) -> str:
    """Translate only the protocol anchor, never item IDs or opaque data."""
    if not session.continuation_bindings and not projection_required:
        return text
    payload = json.loads(text)
    previous_response_id = payload.get("previous_response_id")
    if not isinstance(previous_response_id, str):
        return text
    binding = session.continuation_bindings.get(previous_response_id)
    if binding is None and not projection_required:
        return text
    if binding is None or not binding.belongs_to(session):
        raise ProxyResponseError(
            502,
            openai_error(
                "previous_response_owner_unavailable",
                "The response's physical owner changed; resend the complete conversation.",
            ),
            local_pre_dispatch_refusal=True,
        )
    payload["previous_response_id"] = binding.upstream_response_id
    return json.dumps(payload, separators=(",", ":"))
