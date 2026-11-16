"""Pure native-input and client-call contracts shared by transport and recovery.

Completeness, portability and pending-output ownership remain separate policies;
the vocabulary and meaning of client discovery must agree across those policies.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from types import MappingProxyType
from typing import Final

from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonValue

CLIENT_CALL_OUTPUT_TYPES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "function_call": "function_call_output",
        "custom_tool_call": "custom_tool_call_output",
        "apply_patch_call": "apply_patch_call_output",
        "tool_search_call": "tool_search_output",
    }
)

CLIENT_SEARCH_CALL_FIELDS: Final = frozenset(
    {
        "type",
        "id",
        "call_id",
        "execution",
        "status",
        "arguments",
        "internal_chat_message_metadata_passthrough",
    }
)
CLIENT_SEARCH_OUTPUT_FIELDS: Final = frozenset(
    {
        "type",
        "id",
        "call_id",
        "execution",
        "status",
        "tools",
        "internal_chat_message_metadata_passthrough",
    }
)
_HOSTED_OUTPUT_TYPES: Final = frozenset(
    {
        "web_search_call",
        "file_search_call",
        "code_interpreter_call",
        "image_generation_call",
        "mcp_call",
        "mcp_list_tools",
    }
)


class CallExecution(Enum):
    CLIENT = "client"
    HOSTED = "hosted"
    NON_CALL = "non_call"
    CAPABILITY_UPDATE = "capability_update"
    UNPROVEN = "unproven"


def tool_call_execution(item: Mapping[str, JsonValue]) -> CallExecution:
    """Classify obligations without discarding unknown output capabilities."""
    kind = item.get("type")
    if not isinstance(kind, str):
        return CallExecution.UNPROVEN
    if kind == "tool_search_call":
        if item.get("execution") == "client":
            return CallExecution.CLIENT
        if item.get("execution") == "server" and item.get("call_id") is None:
            return CallExecution.HOSTED
        return CallExecution.UNPROVEN
    if kind in CLIENT_CALL_OUTPUT_TYPES:
        return CallExecution.CLIENT
    if kind in _HOSTED_OUTPUT_TYPES:
        return CallExecution.HOSTED
    if kind == "tool_search_output":
        # A call-only durable manifest cannot prove a resend retained these
        # loaded definitions; recording only parallel calls would be partial.
        return CallExecution.CAPABILITY_UPDATE
    if kind in {"message", "reasoning", "compaction"}:
        return CallExecution.NON_CALL
    return CallExecution.UNPROVEN


def client_search_call_is_complete(item: Mapping[str, JsonValue]) -> bool:
    call_id = item.get("call_id")
    return (
        item.get("execution") == "client"
        and isinstance(call_id, str)
        and bool(call_id.strip())
        and item.get("status") in (None, "completed")
        and isinstance(item.get("arguments"), dict)
        and "encrypted_function_args" not in item
        and "encrypted_arguments" not in item
    )


def client_search_output_is_complete(item: Mapping[str, JsonValue]) -> bool:
    call_id, tools = item.get("call_id"), item.get("tools")
    return (
        item.get("execution") == "client"
        and isinstance(call_id, str)
        and bool(call_id.strip())
        and item.get("status") in (None, "completed")
        and isinstance(tools, list)
        and all(_client_function_definition(tool) for tool in tools)
    )


def _client_function_definition(tool: JsonValue) -> bool:
    if not isinstance(tool, dict):
        return False
    name = tool.get("name")
    return (
        tool.get("type") == "function"
        and isinstance(name, str)
        and bool(name.strip())
        and (tool.get("parameters") is None or isinstance(tool["parameters"], dict))
    )


def _client_tools_only(tools: JsonValue) -> bool:
    """A replay may ask for client work, never repeat a hosted tool effect."""
    return tools is None or (
        isinstance(tools, list)
        and all(
            _client_function_definition(tool)
            or (
                isinstance(tool, dict)
                and tool.get("type") == "tool_search"
                and tool.get("execution") == "client"
                and (tool.get("parameters") is None or isinstance(tool["parameters"], dict))
            )
            for tool in tools
        )
    )


def _complete_original_items(payload: ResponsesRequest) -> bool:
    """Accept only understood full native items; ciphertext stays untouched."""
    if payload.previous_response_id is not None or payload.conversation is not None:
        return False
    if not isinstance(payload.input, list) or not payload.input:
        return False
    pending_calls: set[str] = set()
    pending_searches: set[str] = set()
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
            if not isinstance(item.get("output"), str) and not _complete_text_parts(
                item.get("output"), kind="input_text"
            ):
                return False
            pending_calls.remove(call_id)
        elif kind == "tool_search_call":
            call_id = item.get("call_id")
            if not client_search_call_is_complete(item) or call_id in seen_calls:
                return False
            assert isinstance(call_id, str)
            seen_calls.add(call_id)
            pending_searches.add(call_id)
        elif kind == "tool_search_output":
            call_id = item.get("call_id")
            if not client_search_output_is_complete(item) or call_id not in pending_searches:
                return False
            assert isinstance(call_id, str)
            pending_searches.remove(call_id)
        elif kind == "reasoning":
            if not isinstance(item.get("id"), str) or not item["id"]:
                return False
            encrypted = item.get("encrypted_content")
            if encrypted is None:
                if not _complete_text_parts(item.get("content"), kind="reasoning_text", require_nonempty_text=True):
                    return False
            elif not isinstance(encrypted, str) or not encrypted:
                return False
        else:
            return False
    return not pending_calls and not pending_searches


def _complete_text_parts(content: object, *, kind: str, require_nonempty_text: bool = False) -> bool:
    return (
        isinstance(content, list)
        and bool(content)
        and all(
            isinstance(part, dict)
            and part.get("type") == kind
            and isinstance(part.get("text"), str)
            and (not require_nonempty_text or bool(part["text"]))
            for part in content
        )
    )
