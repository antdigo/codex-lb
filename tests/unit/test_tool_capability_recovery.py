import json

import pytest

from app.core.types import JsonValue
from app.modules.proxy._service.response_create import (
    _inject_missing_interrupted_function_call_outputs,
    _response_output_item_done_tool_call,
)
from app.modules.proxy.durable_bridge_repository import _decode_pending_tool_calls, _encode_pending_tool_calls
from app.modules.proxy.input_contract import client_search_output_is_complete


def test_interrupted_search_result_uses_native_discovery_schema():
    result = _inject_missing_interrupted_function_call_outputs(
        [{"role": "user", "content": "continue after interrupt"}],
        missing_call_ids=["synthetic_search"],
        pending_call_types={"synthetic_search": "tool_search_call"},
    )
    item = result[0]
    assert isinstance(item, dict)
    assert item["type"] == "tool_search_output"
    assert client_search_output_is_complete(item)
    assert item["tools"] == []
    assert "output" not in item


def test_unknown_discovery_execution_is_not_a_client_pending_obligation():
    event: dict[str, JsonValue] = {
        "type": "response.output_item.done",
        "item": {"type": "tool_search_call", "execution": "server", "call_id": "unproven", "arguments": {}},
    }
    assert _response_output_item_done_tool_call(event) is None


def test_old_partial_manifest_cannot_grant_new_completeness_authority():
    legacy = json.dumps({"response_id": "synthetic_response", "calls": {"function_one": "function_call"}})
    assert _decode_pending_tool_calls("synthetic_response", legacy) is None


def test_new_mixed_manifest_has_explicit_completeness_version():
    calls = {"function_one": "function_call", "search_one": "tool_search_call"}
    encoded = _encode_pending_tool_calls("synthetic_response", calls)
    assert isinstance(encoded, str)
    assert json.loads(encoded)["version"] == 2
    assert _decode_pending_tool_calls("synthetic_response", encoded) == calls
    unknown = json.loads(encoded)
    unknown["version"] = 999
    assert _decode_pending_tool_calls("synthetic_response", json.dumps(unknown)) is None


pytestmark = pytest.mark.unit
