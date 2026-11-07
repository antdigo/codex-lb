from __future__ import annotations

import asyncio
import json
import weakref
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast

import anyio
import httpx
import pytest
from openai import AsyncOpenAI, omit
from openai._models import construct_type_unchecked
from openai.lib.streaming.responses._responses import ResponseStreamState
from openai.types.responses import ResponseStreamEvent
from pydantic import TypeAdapter
from starlette.requests import Request

import app.modules.proxy.api as proxy_api_module
from app.core.openai.models import CompactResponsePayload
from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonValue
from app.core.utils.sse import ParsedSseBlock, format_sse_event, sse_block_with_payload

pytestmark = pytest.mark.unit


async def _iter_blocks(*blocks: str) -> AsyncIterator[str]:
    for block in blocks:
        yield block


async def _public_api_response_blocks(monkeypatch, source: list[dict[str, JsonValue]]) -> list[str]:
    raw = _iter_blocks(*(format_sse_event(event) for event in source))
    context = SimpleNamespace(service=SimpleNamespace(stream_http_responses=lambda *_args, **_kwargs: raw))

    async def bridge_active(*_args, **_kwargs):
        return True

    async def admit(*_args, **_kwargs):
        return None

    monkeypatch.setattr(proxy_api_module, "_http_bridge_active_for_request", bridge_active)
    monkeypatch.setattr(proxy_api_module, "_opportunistic_admission_denial", admit)
    response = await proxy_api_module._stream_responses(
        Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []}),
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, context),
        None,
        prefer_http_bridge=True,
        skip_limit_enforcement=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
    )
    assert isinstance(response, proxy_api_module.DeliveryTracedStreamingResponse)
    try:
        return [cast(str, block) async for block in response.body_iterator]
    finally:
        await proxy_api_module._aclose_stream(response.body_iterator)


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["account", "source"])
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
@pytest.mark.parametrize("case", ["tool_done", "message_done", "reasoning_added", "multiple_gaps"])
async def test_public_sparse_output_indexes_are_sdk_safe(monkeypatch, surface: str, terminal_type: str, case: str):
    reasoning: dict[str, JsonValue] = {
        "type": "reasoning",
        "id": "rs_owned",
        "summary": [],
        "encrypted_content": "ciphertext-must-not-change",
    }
    message: dict[str, JsonValue] = {
        "type": "message",
        "id": "msg_owned",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
    }
    tool: dict[str, JsonValue] = {
        "type": "function_call",
        "id": "fc_owned",
        "call_id": "call_unchanged",
        "name": "echo",
        "arguments": '{"value":"owned"}',
        "status": "completed",
    }
    output: list[JsonValue] = [reasoning, tool if case == "tool_done" else message]
    early: dict[str, JsonValue] = {"type": "response.output_item.done", "output_index": 1, "item": output[1]}
    if case == "reasoning_added":
        output = [message, reasoning]
        early = {"type": "response.output_item.added", "output_index": 1, "item": reasoning}
    elif case == "multiple_gaps":
        output = [reasoning, message, {**reasoning, "id": "rs_second"}, tool]
        early = {"type": "response.output_item.done", "output_index": 3, "item": tool}
    source: list[dict[str, JsonValue]] = [
        {"type": "response.created", "response": {"id": "resp_owned", "status": "in_progress", "output": []}},
        early,
        {
            "type": terminal_type,
            "response": {"id": "resp_owned", "status": terminal_type.removeprefix("response."), "output": output},
        },
    ]
    for sequence, event in enumerate(source):
        event["sequence_number"] = sequence
    if surface == "account":
        blocks = await _public_api_response_blocks(monkeypatch, source)
    else:

        async def byte_source():
            for event in source:
                yield format_sse_event(event).encode()

        blocks = [block async for block in proxy_api_module._wrap_source_responses_public_stream(byte_source())]
    async with AsyncOpenAI(
        api_key="offline-test",
        http_client=cast(
            Any,
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _request: httpx.Response(
                        200, headers={"content-type": "text/event-stream"}, content="".join(blocks).encode()
                    )
                )
            ),
        ),
    ) as client:
        async with client.responses.stream(model="offline-test", input="offline-test") as stream:
            sdk_events = [event async for event in stream]
            assert sdk_events[-1].type == terminal_type
            if terminal_type == "response.completed":
                final = await stream.get_final_response()
                assert final.output_text == ("" if case == "tool_done" else "owned answer")
                assert [item.model_dump().get("id") for item in final.output] == [
                    cast(dict, item)["id"] for item in output
                ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["output_index"] for event in events if event["type"] == "response.output_item.added"] == list(
        range(len(output))
    )
    assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == (
        [] if case == "tool_done" else ["owned answer"]
    )
    assert [event["delta"] for event in events if event["type"] == "response.function_call_arguments.delta"] == (
        [tool["arguments"]] if case in {"tool_done", "multiple_gaps"} else []
    )
    assert events[-1]["response"] == source[-1]["response"]
    sequences = [cast(int, event["sequence_number"]) for event in events]
    assert all(a < b for a, b in zip(sequences, sequences[1:]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ending",
    ["event_overflow", "unicode_overflow", "completed_gap", "incomplete_gap", "eof", "foreign", "local_failure"],
)
async def test_sparse_output_bounds_and_terminal_causes(monkeypatch, ending: str):
    created = format_sse_event(
        {"type": "response.created", "sequence_number": 0, "response": {"id": "resp_owned", "output": []}}
    )
    opening = format_sse_event(
        {
            "type": "response.output_item.added",
            "sequence_number": 1,
            "output_index": 1,
            "item": {"type": "reasoning", "id": "rs_1", "summary": []},
        }
    )
    deferred = [opening]
    if ending == "event_overflow":
        deferred.extend(
            format_sse_event(
                {
                    "type": "response.reasoning_summary_text.delta",
                    "sequence_number": n + 2,
                    "output_index": 1,
                    "item_id": "rs_1",
                    "summary_index": 0,
                    "delta": "x",
                }
            )
            for n in range(64)
        )
    elif ending == "unicode_overflow":
        monkeypatch.setattr(proxy_api_module, "_PUBLIC_OUTPUT_ORDER_MAX_BYTES", 1024, raising=False)
        deferred.extend(
            "event: response.reasoning_summary_text.delta\ndata: "
            + json.dumps(
                {
                    "type": "response.reasoning_summary_text.delta",
                    "sequence_number": n + 2,
                    "output_index": 1,
                    "item_id": "rs_1",
                    "summary_index": 0,
                    "delta": "ж" * 200,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n\n"
            for n in range(2)
        )
        assert sum(map(len, deferred)) < 1024 < sum(len(block.encode("utf-8")) for block in deferred)
    tail: list[str] = []
    if ending in {"completed_gap", "incomplete_gap", "foreign"}:
        tail = [
            format_sse_event(
                {
                    "type": "response.incomplete" if ending == "incomplete_gap" else "response.completed",
                    "sequence_number": 100,
                    "response": {
                        "id": "resp_foreign" if ending == "foreign" else "resp_owned",
                        "output": [],
                        "status": "completed",
                    },
                }
            )
        ]
    elif ending == "local_failure":
        failure: dict[str, JsonValue] = {
            "type": "response.failed",
            "response": {
                "id": "req_local",
                "status": "failed",
                "error": {
                    "code": "rate_limit_exceeded",
                    "message": "original cause",
                    "type": "rate_limit_error",
                    "param": "model",
                },
            },
        }
        tail = [
            ParsedSseBlock(
                "retry: 7000\n" + format_sse_event(failure), failure, is_local=True, response_id_is_local=True
            )
        ]
    closed = 0

    async def source():
        nonlocal closed
        try:
            for block in [created, *deferred, *tail]:
                yield block
        finally:
            closed += 1

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(source())]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert closed == 1
    assert not any(event["type"] == "response.output_item.added" for event in events)
    assert events[-1]["type"] == "response.failed"
    response = cast(dict[str, JsonValue], events[-1]["response"])
    assert response["id"] == "resp_owned"
    error = cast(dict[str, JsonValue], response["error"])
    expected = {
        "eof": "upstream_stream_truncated",
        "foreign": "upstream_response_owner_mismatch",
        "local_failure": "rate_limit_exceeded",
    }.get(ending, "upstream_output_index_gap")
    assert error["code"] == expected
    if ending == "local_failure":
        assert error["message"] == "original cause"
        assert error["param"] == "model"
        assert blocks[-1].startswith("retry: 7000\n")
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize("predecessor_first", [False, True])
async def test_sparse_ordering_resumes_before_terminal_and_keeps_valid_interleaving(predecessor_first: bool):
    release = asyncio.Event()
    closed = False
    reasoning: dict[str, JsonValue] = {"type": "reasoning", "id": "rs_0", "summary": []}
    tool: dict[str, JsonValue] = {
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_owned",
        "name": "echo",
        "arguments": "",
        "status": "in_progress",
    }

    async def source():
        nonlocal closed
        try:
            yield format_sse_event(
                {"type": "response.created", "sequence_number": 0, "response": {"id": "resp_owned", "output": []}}
            )
            indexes = [0, 1] if predecessor_first else [1, 0]
            for sequence, index in enumerate(indexes, 1):
                yield format_sse_event(
                    {
                        "type": "response.output_item.added",
                        "sequence_number": sequence,
                        "output_index": index,
                        "item": reasoning if index == 0 else tool,
                    }
                )
            await release.wait()
            terminal: dict[str, JsonValue] = {
                "type": "response.completed",
                "sequence_number": 3,
                "response": {"id": "resp_owned", "output": [reasoning, tool]},
            }
            yield format_sse_event(terminal)
        finally:
            closed = True

    stream = proxy_api_module._normalize_public_responses_stream(source())
    blocks: list[str] = []
    try:
        for _ in range(3):
            blocks.append(await asyncio.wait_for(anext(stream), 5))
        events = [cast(dict[str, JsonValue], proxy_api_module._parse_sse_payload(block)) for block in blocks]
        assert [event.get("output_index") for event in events] == [None, 0, 1]
        assert not release.is_set()
        release.set()
        blocks.extend([block async for block in stream])
        _consume_boundary_with_sdk(
            [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
        )
    finally:
        release.set()
        await proxy_api_module._aclose_stream(stream)
    assert closed


@pytest.mark.asyncio
async def test_sparse_ordering_cancellation_closes_input_without_background_drain():
    seen = asyncio.Event()
    closed = 0

    async def source():
        nonlocal closed
        try:
            yield format_sse_event({"type": "response.created", "response": {"id": "resp_owned", "output": []}})
            seen.set()
            yield format_sse_event(
                {
                    "type": "response.output_item.added",
                    "output_index": 1,
                    "item": {"type": "reasoning", "id": "rs_1", "summary": []},
                }
            )
            await asyncio.Event().wait()
        finally:
            closed += 1

    stream = proxy_api_module._normalize_public_responses_stream(source())
    await anext(stream)

    async def read_next() -> str:
        return await anext(stream)

    pending = asyncio.create_task(read_next())
    try:
        await asyncio.wait_for(seen.wait(), 5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    finally:
        await proxy_api_module._aclose_stream(stream)
    assert closed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["failure", "flush"])
async def test_sparse_ordering_releases_deferred_objects_before_terminal_or_metadata(ending: str):
    class Payload(dict[str, JsonValue]):
        pass

    references: list[weakref.ReferenceType] = []

    def deferred(event: dict[str, JsonValue]) -> ParsedSseBlock:
        payload = Payload(event)
        block = ParsedSseBlock(format_sse_event(payload), payload)
        references.extend([weakref.ref(block), weakref.ref(payload)])
        return block

    async def source():
        yield format_sse_event({"type": "response.created", "response": {"id": "resp_owned", "output": []}})
        yield deferred(
            {
                "type": "response.output_item.added",
                "output_index": 1,
                "item": {"type": "message", "id": "msg_1", "content": []},
            }
        )
        yield deferred(
            {"type": "response.output_text.delta", "output_index": 1, "content_index": 0, "delta": "x" * 32768}
        )
        if ending == "failure":
            yield format_sse_event(
                {
                    "type": "response.failed",
                    "response": {"id": "resp_owned", "error": {"code": "upstream_error", "message": "failed"}},
                }
            )
        else:
            yield format_sse_event(
                {
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "reasoning", "id": "rs_0", "summary": []},
                }
            )
            yield format_sse_event({"type": "response.metadata", "response_id": "resp_owned"})
        await asyncio.Event().wait()

    stream = proxy_api_module._order_public_output_openings(source())
    try:
        assert cast(dict, proxy_api_module._parse_sse_payload(await anext(stream)))["type"] == "response.created"
        count = 1 if ending == "failure" else 4
        for _ in range(count):
            await anext(stream)
        assert all(reference() is None for reference in references), (
            "Deferred frames/payloads must be freed while teardown remains suspended"
        )
    finally:
        await proxy_api_module._aclose_stream(stream)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
async def test_native_source_sparse_indexes_keep_original_response_bytes(terminal_type: str):
    reasoning: dict[str, JsonValue] = {
        "type": "reasoning",
        "id": "rs_0",
        "summary": [],
        "encrypted_content": "opaque-ciphertext",
    }
    tool: dict[str, JsonValue] = {
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_unchanged",
        "name": "echo",
        "arguments": '{"x":1}',
    }
    events: list[dict[str, JsonValue]] = [
        {"type": "response.created", "sequence_number": 0, "response": {"id": "resp_owned", "output": []}},
        {"type": "response.output_item.done", "sequence_number": 1, "output_index": 1, "item": tool},
        {"type": "codex.rate_limits", "remaining": 7},
        {"type": terminal_type, "sequence_number": 2, "response": {"id": "resp_owned", "output": [reasoning, tool]}},
    ]
    raw = [format_sse_event(event) for event in events]

    async def source():
        for block in raw:
            yield block.encode()

    blocks = [
        block
        async for block in proxy_api_module._wrap_source_responses_public_stream(
            source(), enforce_openai_sdk_contract=False
        )
    ]
    response_blocks = [
        block
        for block in blocks
        if (payload := proxy_api_module._parse_sse_payload(block)) is not None
        and cast(str, payload.get("type", "")).startswith("response.")
    ]
    assert response_blocks == [
        block for block, event in zip(raw, events) if cast(str, event["type"]).startswith("response.")
    ]
    assert raw[2] in blocks
    assert any("codex.keepalive" in block for block in blocks)


@pytest.mark.asyncio
async def test_sparse_ordering_releases_popped_bucket_before_awaiting_child_cleanup():
    release = asyncio.Event()
    closing = asyncio.Event()
    references: list[weakref.ReferenceType] = []

    def deferred(event: dict[str, JsonValue]) -> ParsedSseBlock:
        block = ParsedSseBlock(format_sse_event(event), event)
        references.append(weakref.ref(block))
        return block

    async def source():
        try:
            yield format_sse_event({"type": "response.created", "response": {"id": "resp_owned", "output": []}})
            yield deferred(
                {
                    "type": "response.output_item.added",
                    "output_index": 1,
                    "item": {"type": "message", "id": "msg_1", "content": []},
                }
            )
            yield deferred(
                {"type": "response.output_text.delta", "output_index": 1, "content_index": 0, "delta": "x" * 32768}
            )
            yield format_sse_event(
                {
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "reasoning", "id": "rs_0", "summary": []},
                }
            )
            await asyncio.Event().wait()
        finally:
            closing.set()
            await release.wait()

    stream = proxy_api_module._order_public_output_openings(source())
    for _ in range(3):
        await anext(stream)

    async def close_stream():
        await proxy_api_module._aclose_stream(stream)

    cleanup = asyncio.create_task(close_stream())
    try:
        await asyncio.wait_for(closing.wait(), 5)
        assert all(reference() is None for reference in references), (
            "Popped buckets must be released before child cleanup finishes"
        )
    finally:
        release.set()
        await asyncio.wait_for(cleanup, 5)


@pytest.mark.asyncio
async def test_sparse_ordering_allows_exactly_sixty_four_deferred_events():
    reasoning: dict[str, JsonValue] = {"type": "reasoning", "id": "rs_1", "summary": []}
    predecessor: dict[str, JsonValue] = {**reasoning, "id": "rs_0"}
    events: list[dict[str, JsonValue]] = [
        {"type": "response.created", "response": {"id": "resp_owned", "output": []}},
        {"type": "response.output_item.added", "output_index": 1, "item": reasoning},
        *[
            cast(
                dict[str, JsonValue],
                {
                    "type": "response.reasoning_summary_text.delta",
                    "output_index": 1,
                    "summary_index": 0,
                    "item_id": "rs_1",
                    "delta": "x",
                },
            )
            for _ in range(63)
        ],
        {"type": "response.output_item.added", "output_index": 0, "item": predecessor},
        {"type": "response.completed", "response": {"id": "resp_owned", "output": [predecessor, reasoning]}},
    ]
    for sequence, event in enumerate(events):
        event["sequence_number"] = sequence
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(format_sse_event(event) for event in events))
        )
    ]
    emitted = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["output_index"] for event in emitted if event["type"] == "response.output_item.added"] == [0, 1]
    assert sum(event["type"] == "response.reasoning_summary_text.delta" for event in emitted) == 63
    assert emitted[-1]["type"] == "response.completed"
    _consume_boundary_with_sdk(emitted)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
@pytest.mark.parametrize("before_created", [False, True])
@pytest.mark.parametrize("include_item_id", [False, True])
@pytest.mark.parametrize("output_kind", ["message", "function_call", "refusal"])
@pytest.mark.parametrize("reasoning_index", [0, 1])
async def test_public_api_repairs_owned_output_after_unindexed_reasoning(
    monkeypatch,
    terminal_type: str,
    before_created: bool,
    include_item_id: bool,
    output_kind: str,
    reasoning_index: int,
) -> None:
    reasoning: dict[str, JsonValue] = {
        "type": "reasoning",
        "id": "rs_owned",
        "summary": [{"type": "summary_text", "text": "owned summary"}],
        "content": [{"type": "reasoning_text", "text": "private reasoning"}],
        "encrypted_content": "opaque ciphertext",
    }
    answer: dict[str, JsonValue] = {
        "type": "message",
        "id": "msg_owned",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
    }
    if output_kind == "function_call":
        answer = {
            "type": "function_call",
            "id": "fc_owned",
            "call_id": "call_owned",
            "name": "echo",
            "arguments": '{"value":"owned"}',
            "status": "completed",
        }
    elif output_kind == "refusal":
        answer["content"] = [{"type": "refusal", "refusal": "declined"}]
    output: list[JsonValue] = [reasoning, answer]
    if reasoning_index == 1:
        output.insert(
            0,
            {
                "type": "reasoning",
                "id": "rs_other",
                "summary": [{"type": "summary_text", "text": "other summary"}],
            },
        )
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "response": {"id": "resp_owned", "status": "in_progress", "output": []},
    }
    delta: dict[str, JsonValue] = {
        "type": "response.reasoning_summary_text.delta",
        "summary_index": 0,
        "delta": "owned summary",
    }
    if include_item_id:
        delta["item_id"] = "rs_owned"
    source = [delta, created] if before_created else [created, delta]
    source.append(
        {
            "type": terminal_type,
            "response": {
                "id": "resp_owned",
                "status": terminal_type.removeprefix("response."),
                "output": output,
            },
        }
    )
    for sequence, event in enumerate(source):
        event["sequence_number"] = sequence
    blocks = await _public_api_response_blocks(monkeypatch, source)
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    async with AsyncOpenAI(
        api_key="offline-test",
        http_client=cast(
            Any,
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _request: httpx.Response(
                        200, headers={"content-type": "text/event-stream"}, content="".join(blocks).encode()
                    )
                )
            ),
        ),
    ) as client:
        async with client.responses.stream(model="offline-test", input="offline-test") as stream:
            sdk_events = [event async for event in stream]
            assert sdk_events[-1].type == terminal_type
            if terminal_type == "response.completed":
                final = await stream.get_final_response()
                assert final.output_text == ("owned answer" if output_kind == "message" else "")
    assert [event["output_index"] for event in events if event["type"] == "response.output_item.added"] == list(
        range(len(output))
    )
    assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == (
        ["owned answer"] if output_kind == "message" else []
    )
    assert [event["delta"] for event in events if event["type"] == "response.function_call_arguments.delta"] == (
        ['{"value":"owned"}'] if output_kind == "function_call" else []
    )
    assert [event["delta"] for event in events if event["type"] == "response.refusal.delta"] == (
        ["declined"] if output_kind == "refusal" else []
    )
    summaries = [event["delta"] for event in events if event["type"] == "response.reasoning_summary_text.delta"]
    assert summaries.count("owned summary") == 1
    if reasoning_index == 1:
        assert summaries.count("other summary") == int(before_created or include_item_id)
    assert events[-1]["response"] == source[-1]["response"]
    sequences = [cast(int, event["sequence_number"]) for event in events]
    assert all(a < b for a, b in zip(sequences, sequences[1:]))


@pytest.mark.asyncio
@pytest.mark.parametrize("public", [False, True])
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
async def test_unindexed_reasoning_preserves_native_and_foreign_owner_boundaries(
    monkeypatch, public: bool, terminal_type: str
) -> None:
    source: list[dict[str, JsonValue]] = [
        {
            "type": "response.created",
            "sequence_number": 0,
            "response": {"id": "resp_owned", "status": "in_progress", "output": []},
        },
        {"type": "response.reasoning_summary_text.delta", "sequence_number": 1, "delta": "owned summary"},
        {
            "type": terminal_type,
            "sequence_number": 2,
            "response": {
                "id": "resp_foreign",
                "status": terminal_type.removeprefix("response."),
                "output": [
                    {
                        "type": "message",
                        "id": "msg_foreign",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "foreign answer", "annotations": []}],
                    }
                ],
            },
        },
    ]
    raw_blocks = [format_sse_event(event) for event in source]
    blocks = (
        await _public_api_response_blocks(monkeypatch, source)
        if public
        else [
            block
            async for block in proxy_api_module._normalize_public_responses_stream(
                _iter_blocks(*raw_blocks), enforce_openai_sdk_contract=False
            )
        ]
    )
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert not any(event["type"] == "response.output_item.added" for event in events)
    if public:
        assert [event["type"] for event in events] == [
            "response.created",
            "response.reasoning_summary_text.delta",
            "response.failed",
        ]
        failed = cast(dict[str, JsonValue], events[-1]["response"])
        assert failed["id"] == "resp_owned"
        assert cast(dict[str, JsonValue], failed["error"])["code"] == "upstream_response_owner_mismatch"
        assert "foreign answer" not in "".join(blocks)
        _consume_boundary_with_sdk(events)
    else:
        assert raw_blocks[1] in blocks
        assert events[-1]["response"] == source[-1]["response"]


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
@pytest.mark.parametrize(
    "scenario",
    [
        "reasoning_then_message",
        "reasoning_summary_without_opening",
        "reasoning_done_without_opening",
        "opened_message",
        "opened_message_then_done",
        "message_done_without_opening",
        "reasoning_then_tool",
        "reasoning_then_refusal",
        "opened_tool",
        "opened_custom_tool",
        "visible_custom_tool",
        "opened_empty_part",
        "text_done_without_part_done",
        "visible_partial_text",
        "prefilled_message",
        "visible_part_then_new_part",
        "visible_two_parts",
        "visible_message_and_tool",
        "legacy_prefix_then_message",
        "legacy_prefix_then_tool",
        "legacy_prefix_then_refusal",
        "legacy_terminal_then_message",
        "legacy_terminal_then_tool",
        "legacy_terminal_then_refusal",
    ],
)
async def test_public_api_repairs_partial_lifecycle_for_real_sdk(
    monkeypatch, terminal_type: str, scenario: str
) -> None:
    reasoning: dict[str, JsonValue] = {"type": "reasoning", "id": "rs_owned", "summary": []}
    message: dict[str, JsonValue] = {
        "type": "message",
        "id": "msg_owned",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
    }
    tool: dict[str, JsonValue] = {
        "type": "function_call",
        "id": "fc_owned",
        "call_id": "call_owned",
        "name": "echo",
        "arguments": '{"value":"owned"}',
        "status": "completed",
    }
    output: list[JsonValue] = [message]
    prefix: list[dict[str, JsonValue]] = []
    expected_text = ["owned answer"]
    expected_final_text = "owned answer"
    expected_arguments: list[str] = []
    expected_inputs: list[str] = []
    expected_refusals: list[str] = []
    if scenario.startswith("reasoning_"):
        output = [reasoning, message]
        prefix = [
            {"type": "response.output_item.added", "output_index": 0, "item": reasoning},
            {"type": "response.output_item.done", "output_index": 0, "item": reasoning},
        ]
        if scenario == "reasoning_summary_without_opening":
            prefix = [
                {
                    "type": "response.reasoning_summary_text.delta",
                    "output_index": 0,
                    "item_id": "rs_owned",
                    "summary_index": 0,
                    "delta": "visible summary",
                }
            ]
        elif scenario == "reasoning_done_without_opening":
            prefix = [{"type": "response.output_item.done", "output_index": 0, "item": reasoning}]
        if scenario == "reasoning_then_tool":
            output = [reasoning, tool]
            expected_text = []
            expected_final_text = ""
            expected_arguments = ['{"value":"owned"}']
        elif scenario == "reasoning_then_refusal":
            output = [reasoning, {**message, "content": [{"type": "refusal", "refusal": "declined"}]}]
            expected_text = []
            expected_final_text = ""
            expected_refusals = ["declined"]
    elif scenario in {"opened_tool", "opened_custom_tool", "visible_custom_tool"}:
        expected_text = []
        expected_final_text = ""
        tool_field = "arguments"
        tool_event = "function_call_arguments"
        if scenario != "opened_tool":
            tool = {
                "type": "custom_tool_call",
                "id": "ctc_owned",
                "call_id": "call_owned",
                "name": "echo",
                "input": "owned input",
                "status": "completed",
            }
            tool_field = "input"
            tool_event = "custom_tool_call_input"
            expected_inputs = ["owned input"]
        else:
            expected_arguments = ['{"value":"owned"}']
        output = [tool]
        prefix = [
            {
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {**tool, tool_field: "", "status": "in_progress"},
            }
        ]
        if scenario == "visible_custom_tool":
            prefix.extend(
                [
                    {
                        "type": f"response.{tool_event}.delta",
                        "output_index": 0,
                        "item_id": "ctc_owned",
                        "delta": "owned input",
                    },
                    {
                        "type": f"response.{tool_event}.done",
                        "output_index": 0,
                        "item_id": "ctc_owned",
                        tool_field: "owned input",
                    },
                    {"type": "response.output_item.done", "output_index": 0, "item": tool},
                ]
            )
    elif scenario.startswith("legacy_"):
        output = [
            {
                **message,
                "id": "msg_legacy",
                "content": [{"type": "output_text", "text": "legacy answer", "annotations": []}],
            },
            message,
        ]
        expected_text = ["legacy answer", "owned answer"]
        expected_final_text = "legacy answerowned answer"
        if scenario.endswith("_tool"):
            output[1] = tool
            expected_text = ["legacy answer"]
            expected_final_text = "legacy answer"
            expected_arguments = ['{"value":"owned"}']
        elif scenario.endswith("_refusal"):
            output[1] = {**message, "content": [{"type": "refusal", "refusal": "declined"}]}
            expected_text = ["legacy answer"]
            expected_final_text = "legacy answer"
            expected_refusals = ["declined"]
        prefix = [{"type": "response.output_text.delta", "delta": "legacy answer"}]
    else:
        prefix.append(
            {
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {**message, "status": "in_progress", "content": []},
            }
        )
        if scenario != "opened_message":
            prefix.append(
                {
                    "type": "response.content_part.added",
                    "output_index": 0,
                    "content_index": 0,
                    "item_id": "msg_owned",
                    "part": {"type": "output_text", "text": "", "annotations": []},
                }
            )
        if scenario in {"opened_message_then_done", "message_done_without_opening"}:
            prefix = prefix[:1] if scenario == "opened_message_then_done" else []
            prefix.append({"type": "response.output_item.done", "output_index": 0, "item": message})
        if scenario in {
            "visible_part_then_new_part",
            "visible_message_and_tool",
            "text_done_without_part_done",
            "visible_partial_text",
            "visible_two_parts",
        }:
            prefix.extend(
                [
                    {
                        "type": "response.output_text.delta",
                        "output_index": 0,
                        "content_index": 0,
                        "item_id": "msg_owned",
                        "delta": "owned answer",
                        "logprobs": [],
                    },
                    {
                        "type": "response.output_text.done",
                        "output_index": 0,
                        "content_index": 0,
                        "item_id": "msg_owned",
                        "text": "owned answer",
                        "logprobs": [],
                    },
                    {
                        "type": "response.content_part.done",
                        "output_index": 0,
                        "content_index": 0,
                        "item_id": "msg_owned",
                        "part": {"type": "output_text", "text": "owned answer", "annotations": []},
                    },
                ]
            )
            if scenario == "text_done_without_part_done":
                prefix.pop()
            elif scenario == "visible_partial_text":
                prefix = prefix[:3]
                prefix[-1]["delta"] = "partial"
                expected_text = ["partial"]
        elif scenario == "prefilled_message":
            prefix = [{"type": "response.output_item.added", "output_index": 0, "item": message}]
            expected_text = []
        if scenario in {"visible_part_then_new_part", "visible_two_parts"}:
            output = [
                {
                    **message,
                    "content": [
                        {"type": "output_text", "text": "owned answer", "annotations": []},
                        {"type": "output_text", "text": "new part", "annotations": []},
                    ],
                }
            ]
            expected_text = ["owned answer", "new part"]
            expected_final_text = "owned answernew part"
            if scenario == "visible_two_parts":
                prefix.extend(
                    [
                        {
                            "type": "response.content_part.added",
                            "output_index": 0,
                            "content_index": 1,
                            "item_id": "msg_owned",
                            "part": {"type": "output_text", "text": "", "annotations": []},
                        },
                        {
                            "type": "response.output_text.delta",
                            "output_index": 0,
                            "content_index": 1,
                            "item_id": "msg_owned",
                            "delta": "new part",
                            "logprobs": [],
                        },
                    ]
                )
        elif scenario == "visible_message_and_tool":
            output = [message, tool]
            prefix.extend(
                [
                    {"type": "response.output_item.done", "output_index": 0, "item": message},
                    {
                        "type": "response.output_item.added",
                        "output_index": 1,
                        "item": {**tool, "arguments": "", "status": "in_progress"},
                    },
                    {
                        "type": "response.function_call_arguments.delta",
                        "output_index": 1,
                        "item_id": "fc_owned",
                        "delta": '{"value":"owned"}',
                    },
                    {
                        "type": "response.function_call_arguments.done",
                        "output_index": 1,
                        "item_id": "fc_owned",
                        "arguments": '{"value":"owned"}',
                        "name": "echo",
                    },
                    {"type": "response.output_item.done", "output_index": 1, "item": tool},
                ]
            )
            expected_arguments = ['{"value":"owned"}']
    source: list[dict[str, JsonValue]] = [
        {
            "type": "response.created",
            "response": {"id": "resp_owned", "status": "in_progress", "output": []},
        },
        *prefix,
        {
            "type": terminal_type,
            "response": {
                "id": "resp_owned",
                "status": terminal_type.removeprefix("response."),
                "output": output,
            },
        },
    ]
    if scenario.startswith("legacy_prefix_"):
        source = [prefix[0], source[0], source[-1]]
    elif scenario.startswith("legacy_terminal_"):
        source = [prefix[0], source[-1]]
    for sequence, event in enumerate(source):
        event["sequence_number"] = sequence
    raw = _iter_blocks(*(format_sse_event(event) for event in source))
    context = SimpleNamespace(service=SimpleNamespace(stream_http_responses=lambda *_args, **_kwargs: raw))

    async def bridge_active(*_args, **_kwargs):
        return True

    async def admit(*_args, **_kwargs):
        return None

    monkeypatch.setattr(proxy_api_module, "_http_bridge_active_for_request", bridge_active)
    monkeypatch.setattr(proxy_api_module, "_opportunistic_admission_denial", admit)
    response = await proxy_api_module._stream_responses(
        Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []}),
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, context),
        None,
        prefer_http_bridge=True,
        skip_limit_enforcement=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
    )
    assert isinstance(response, proxy_api_module.DeliveryTracedStreamingResponse)
    try:
        blocks = [cast(str, block) async for block in response.body_iterator]
    finally:
        await proxy_api_module._aclose_stream(response.body_iterator)
    async with AsyncOpenAI(
        api_key="offline-test",
        http_client=cast(
            Any,
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _request: httpx.Response(
                        200, headers={"content-type": "text/event-stream"}, content="".join(blocks).encode()
                    )
                )
            ),
        ),
    ) as client:
        async with client.responses.stream(model="offline-test", input="offline-test") as stream:
            sdk_events = [event async for event in stream]
            assert [event.delta for event in sdk_events if event.type == "response.output_text.delta"] == expected_text
            assert [
                event.delta for event in sdk_events if event.type == "response.function_call_arguments.delta"
            ] == expected_arguments
            assert [event.delta for event in sdk_events if event.type == "response.refusal.delta"] == expected_refusals
            assert [
                event.delta for event in sdk_events if event.type == "response.custom_tool_call_input.delta"
            ] == expected_inputs
            assert sdk_events[-1].type == terminal_type
            if terminal_type == "response.completed":
                final = await stream.get_final_response()
                assert final.output_text == expected_final_text
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["output_index"] for event in events if event["type"] == "response.output_item.added"] == list(
        range(len(output))
    )
    assert [event["output_index"] for event in events if event["type"] == "response.output_item.done"] == list(
        range(len(output))
    )
    for event_type in ("response.content_part.added", "response.content_part.done", "response.output_text.done"):
        part_keys = [
            (cast(int, event["output_index"]), cast(int, event["content_index"]))
            for event in events
            if event["type"] == event_type
        ]
        assert len(part_keys) == len(set(part_keys)), f"Repeated {event_type}"
    sequences = [cast(int, event["sequence_number"]) for event in events]
    if scenario.startswith("legacy_"):
        assert sequences[-1] > max(sequences[:-1])
    else:
        assert all(a < b for a, b in zip(sequences, sequences[1:]))


@pytest.mark.asyncio
@pytest.mark.parametrize("startup_handoff", [False, True])
@pytest.mark.parametrize("ending", ["foreign_owner", "completed", "disconnect"])
async def test_public_account_api_deterministically_closes_service_stream(
    monkeypatch, startup_handoff: bool, ending: str
) -> None:
    first_ready = asyncio.Event()
    if not startup_handoff:
        first_ready.set()

    class ServiceStream:
        index = 0
        closed = False

        def __aiter__(self) -> AsyncIterator[str]:
            return self

        async def __anext__(self) -> str:
            if self.index == 0:
                await first_ready.wait()
                event: dict[str, JsonValue] = {
                    "type": "response.created",
                    "response": {"id": "resp_owned", "status": "in_progress", "output": []},
                }
            elif self.index == 1:
                event = {
                    "type": "response.completed" if ending == "completed" else "response.in_progress",
                    "response": {"id": "resp_owned", "status": "completed", "output": []},
                }
                if ending == "foreign_owner":
                    event["response_id"] = "resp_foreign"
            else:
                if ending != "completed":
                    raise AssertionError("Rejected/disconnected stream consumed another upstream frame")
                raise StopAsyncIteration
            self.index += 1
            return format_sse_event(event)

        async def aclose(self) -> None:
            self.closed = True

    raw = ServiceStream()
    context = SimpleNamespace(service=SimpleNamespace(stream_http_responses=lambda *_args, **_kwargs: raw))

    async def bridge_active(*_args, **_kwargs):
        return True

    async def admit(*_args, **_kwargs):
        return None

    monkeypatch.setattr(proxy_api_module, "_http_bridge_active_for_request", bridge_active)
    monkeypatch.setattr(proxy_api_module, "_opportunistic_admission_denial", admit)
    if startup_handoff:
        monkeypatch.setattr(proxy_api_module, "_STREAM_STARTUP_ERROR_PROBE_SECONDS", 0)
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    response = await proxy_api_module._stream_responses(
        request,
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, context),
        None,
        prefer_http_bridge=True,
        skip_limit_enforcement=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
    )
    assert isinstance(response, proxy_api_module.DeliveryTracedStreamingResponse)
    first_ready.set()
    events: list[dict[str, JsonValue]] = []
    try:
        async for block in response.body_iterator:
            event = proxy_api_module._parse_sse_payload(cast(str, block))
            if event is not None:
                events.append(event)
            if ending == "disconnect" and events:
                break
    finally:
        await proxy_api_module._aclose_stream(response.body_iterator)
    assert raw.closed, "Service cleanup must finish without garbage collection or a later timeout"
    if ending == "foreign_owner":
        assert [event["type"] for event in events] == ["response.created", "response.failed"]
        failed = cast(dict[str, JsonValue], events[-1]["response"])
        assert failed["id"] == "resp_owned"
        assert cast(dict[str, JsonValue], failed["error"])["code"] == "upstream_response_owner_mismatch"
    elif ending == "completed":
        assert events[-1]["type"] == "response.completed"


def _boundary_terminal(response_id: str, terminal_type: str) -> dict[str, JsonValue]:
    return {
        "type": terminal_type,
        "sequence_number": 100,
        "response": {
            "id": response_id,
            "object": "response",
            "status": terminal_type.removeprefix("response."),
            "output": [
                {
                    "type": "reasoning",
                    "id": "rs_boundary",
                    "summary": [],
                    "content": [{"type": "reasoning_text", "text": "private reasoning"}],
                    "encrypted_content": "opaque-boundary-state",
                },
                {
                    "type": "message",
                    "id": "msg_boundary",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "boundary answer", "annotations": []}],
                },
                {
                    "type": "function_call",
                    "id": "fc_boundary",
                    "call_id": "call_boundary",
                    "name": "echo",
                    "arguments": '{"value":"boundary"}',
                    "status": "completed",
                },
            ],
        },
    }


@pytest.mark.asyncio
async def test_public_account_api_joins_keepalive_read_before_closing_children(monkeypatch) -> None:
    closed = asyncio.Event()
    blocked = asyncio.Event()
    release = asyncio.Event()

    async def source():
        try:
            yield format_sse_event(
                {"type": "response.created", "response": {"id": "resp_owned", "status": "in_progress", "output": []}}
            )
            blocked.set()
            await release.wait()
        finally:
            closed.set()

    raw = source()
    context = SimpleNamespace(service=SimpleNamespace(stream_http_responses=lambda *_args, **_kwargs: raw))
    settings = proxy_api_module.get_settings().model_copy(update={"sse_keepalive_interval_seconds": 0.005})

    async def bridge_active(*_args, **_kwargs):
        return True

    async def admit(*_args, **_kwargs):
        return None

    monkeypatch.setattr(proxy_api_module, "_http_bridge_active_for_request", bridge_active)
    monkeypatch.setattr(proxy_api_module, "_opportunistic_admission_denial", admit)
    monkeypatch.setattr(proxy_api_module, "get_settings", lambda: settings)
    monkeypatch.setattr(proxy_api_module, "with_dashboard_overrides", lambda value: value)
    injectors: list[AsyncIterator[str]] = []
    real_injector = proxy_api_module.inject_sse_keepalives

    def track_injector(*args, **kwargs):
        injector = real_injector(*args, **kwargs)
        injectors.append(injector)
        return injector

    monkeypatch.setattr(proxy_api_module, "inject_sse_keepalives", track_injector)
    response = await proxy_api_module._stream_responses(
        Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []}),
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, context),
        None,
        prefer_http_bridge=True,
        skip_limit_enforcement=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
    )
    assert isinstance(response, proxy_api_module.DeliveryTracedStreamingResponse)
    body = aiter(response.body_iterator)
    assert "response.created" in cast(str, await anext(body))
    assert await asyncio.wait_for(anext(body), 5) == ": keepalive\n\n"
    assert blocked.is_set()
    pending = [
        task
        for task in asyncio.all_tasks()
        if (coro := task.get_coro()) is not None and "inject_sse_keepalives" in coro.__qualname__
    ]
    assert len(pending) == 1
    try:
        await proxy_api_module._aclose_stream(body)
        assert closed.is_set(), "Close the keepalive owner before its running child generators"
        assert all(task.done() for task in pending)
    finally:
        for injector in injectors:
            await proxy_api_module._aclose_stream(injector)
        release.set()


def _consume_boundary_with_sdk(events: list[dict[str, JsonValue]]) -> ResponseStreamState:
    state = ResponseStreamState(input_tools=omit, text_format=omit)
    for event in events:
        state.handle_event(construct_type_unchecked(type_=cast(Any, ResponseStreamEvent), value=event))
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code", ["stream_idle_timeout", "upstream_unavailable", "response_create_gate_timeout", "provider_custom_error"]
)
async def test_local_stream_failure_preserves_error_and_retry_after_accepted_response(code):
    async def source():
        yield format_sse_event(
            {
                "type": "response.created",
                "sequence_number": 0,
                "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
            }
        )
        raise proxy_api_module.ProxyResponseError(
            504,
            {"error": {"code": code, "type": "server_error", "message": "original failure", "param": "input"}},
            retry_after_seconds=2,
            local_pre_dispatch_refusal=code == "response_create_gate_timeout",
        )

    error_stream = proxy_api_module._stream_response_error_events(source(), owns_reservation=False, reservation=None)
    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(error_stream)]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["type"] for event in events] == ["response.created", "response.failed"]
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert failed["error"] == {"code": code, "type": "server_error", "message": "original failure", "param": "input"}
    assert blocks[-1].startswith("retry: 2000\n")
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_kind", ["wire_marker", "explicit_exception_owner", "explicit_exception_top", "explicit_exception_conflict"]
)
async def test_proxy_failure_provenance_cannot_reassign_explicit_foreign_owner(source_kind):
    async def source():
        yield format_sse_event(
            {
                "type": "response.created",
                "sequence_number": 0,
                "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
            }
        )
        error: dict[str, JsonValue] = {
            "code": "stream_idle_timeout",
            "type": "server_error",
            "message": "foreign failure",
        }
        if source_kind.startswith("explicit_exception"):
            body: dict[str, JsonValue] = {"error": error}
            if source_kind != "explicit_exception_top":
                body["response"] = {"id": "resp_accepted" if source_kind.endswith("conflict") else "resp_foreign"}
            if source_kind != "explicit_exception_owner":
                body["response_id"] = "resp_foreign"
            raise proxy_api_module.ProxyResponseError(504, cast(Any, body))
        yield format_sse_event(
            {
                "type": "response.failed",
                proxy_api_module.SYNTHETIC_TRANSPORT_FAILURE_MARKER: True,
                "response": {"id": "resp_foreign", "status": "failed", "output": [], "error": error},
            }
        )

    error_stream = proxy_api_module._stream_response_error_events(source(), owns_reservation=False, reservation=None)
    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(error_stream)]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert failed["error"]["code"] == "upstream_response_owner_mismatch"
    assert "resp_foreign" not in "".join(blocks)
    assert "foreign failure" not in "".join(blocks)


@pytest.mark.asyncio
@pytest.mark.parametrize("id_kind", ["nested", "top", "conflicting"])
async def test_public_raw_error_rejects_explicit_foreign_owner(id_kind):
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    error: dict[str, JsonValue] = {"type": "error", "error": {"code": "foreign_error", "message": "foreign diagnostic"}}
    if id_kind != "top":
        error["response"] = {"id": "resp_accepted" if id_kind == "conflicting" else "resp_foreign"}
    if id_kind != "nested":
        error["response_id"] = "resp_foreign"
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(format_sse_event(created), format_sse_event(error))
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert failed["error"]["code"] == "upstream_response_owner_mismatch"
    assert "foreign diagnostic" not in "".join(blocks)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "joined_foreign",
        "hidden_foreign",
        "typeless_top",
        "typeless_nested",
        "typed_error",
        "anonymous_control",
        "same_owner_control",
    ],
)
async def test_source_owner_fence_precedes_reasoning_rewrite_and_error_classification(scenario):
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    completed: dict[str, JsonValue] = {
        "type": "response.completed",
        "sequence_number": 10,
        "response": {"id": "resp_accepted", "status": "completed", "output": []},
    }

    def reasoning(text: str, owner: str | None) -> dict[str, JsonValue]:
        event: dict[str, JsonValue] = {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_same",
            "output_index": 0,
            "summary_index": 0,
            "delta": text,
        }
        if owner is not None:
            event["response_id"] = owner
        return event

    source = [created]
    positive = scenario.endswith("control")
    if scenario in {"typeless_top", "typeless_nested", "typed_error"}:
        error: dict[str, JsonValue] = {"error": {"code": "foreign_error", "message": "Foreign diagnostic"}}
        error["response" if scenario == "typeless_nested" else "response_id"] = (
            {"id": "resp_foreign"} if scenario == "typeless_nested" else "resp_foreign"
        )
        if scenario == "typed_error":
            error["type"] = "error"
        source.append(error)
    else:
        first_owner = (
            None
            if scenario == "anonymous_control"
            else ("resp_foreign" if scenario == "hidden_foreign" else "resp_accepted")
        )
        second_owner = None if scenario == "anonymous_control" else ("resp_accepted" if positive else "resp_foreign")
        source.extend(
            [
                reasoning("<!--", first_owner),
                reasoning(" -->\n" + ("Owned reasoning" if positive else "Foreign reasoning"), second_owner),
            ]
        )
        if scenario == "hidden_foreign":
            source[-1] = reasoning(" -->\n", "resp_foreign")
        source.append(completed)
    closed = False

    async def wire():
        nonlocal closed
        try:
            for event in source:
                yield format_sse_event(event).encode()
        finally:
            closed = True

    wrapped = proxy_api_module._wrap_source_responses_public_stream(wire())
    try:
        blocks = [block async for block in wrapped]
    finally:
        await proxy_api_module._aclose_stream(wrapped)
    assert closed
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    if positive:
        assert events[-1]["type"] == "response.completed"
        assert (
            "".join(
                cast(str, event["delta"])
                for event in events
                if event["type"] == "response.reasoning_summary_text.delta"
            )
            == "Owned reasoning"
        )
        return
    assert events[-1]["type"] == "response.failed"
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert failed["error"]["code"] == "upstream_response_owner_mismatch"
    assert "Foreign reasoning" not in "".join(blocks)
    assert "Foreign diagnostic" not in "".join(blocks)
    assert "resp_foreign" not in "".join(blocks)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix_owner", ["resp_foreign", "resp_accepted"])
async def test_precreated_explicit_owner_is_checked_before_legacy_text_replay(prefix_owner):
    prefix: dict[str, JsonValue] = {
        "type": "response.output_text.delta",
        "response_id": prefix_owner,
        "delta": "Foreign prefix" if prefix_owner == "resp_foreign" else "Owned answer",
    }
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    terminal: dict[str, JsonValue] = {
        "type": "response.completed",
        "sequence_number": 10,
        "response": {
            "id": "resp_accepted",
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "id": "msg_accepted",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "Owned answer", "annotations": []}],
                }
            ],
        },
    }
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(format_sse_event(event) for event in [prefix, created, terminal]))
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert events[-1]["type"] == "response.completed"
    assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == ["Owned answer"]
    assert "Foreign prefix" not in "".join(blocks)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["error", "truncated", "invalid_json", "invalid_output_item"])
async def test_normalizer_generated_failure_retains_accepted_response_owner(failure_kind):
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    source = [format_sse_event(created)]
    code = "upstream_stream_truncated" if failure_kind == "truncated" else failure_kind
    if failure_kind == "error":
        code = "provider_error"
        source.append(format_sse_event({"type": "error", "error": {"code": code, "message": "original error"}}))
    elif failure_kind == "invalid_json":
        source.append("data: not-json\n\n")
    elif failure_kind == "invalid_output_item":
        malformed: dict[str, JsonValue] = {
            "type": "response.completed",
            "sequence_number": 1,
            "response": {"id": "resp_accepted", "status": "completed", "output": [{"type": "opaque_unknown"}]},
        }
        source.append(format_sse_event(malformed))
    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*source))]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert events[-1]["type"] == "response.failed"
    assert failed["error"]["code"] == code
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
async def test_first_terminal_recovers_after_unreplayed_unindexed_reasoning(terminal_type):
    terminal = _boundary_terminal("resp_boundary", terminal_type)
    hidden: dict[str, JsonValue] = {
        "type": "response.reasoning_summary_text.delta",
        "summary_index": 0,
        "delta": "unowned summary",
    }
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(format_sse_event(hidden), format_sse_event(terminal))
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["delta"] for event in events if event["type"] == "response.function_call_arguments.delta"] == [
        '{"value":"boundary"}'
    ]
    assert "unowned summary" not in "".join(blocks)
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
@pytest.mark.parametrize("prelude_count", [0, 1, 64, 65])
async def test_first_terminal_recovers_full_lifecycle_without_prelude_size_dependency(terminal_type, prelude_count):
    terminal = _boundary_terminal("resp_boundary", terminal_type)
    orphan = {"type": "response.output_text.delta", "output_index": 0, "item_id": "msg_orphan", "delta": "orphan text"}
    source = [orphan] * prelude_count + [terminal]
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(format_sse_event(event) for event in source))
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["type"] for event in events].count("response.created") == 1
    assert [cast(dict, event["item"])["id"] for event in events if event["type"] == "response.output_item.added"] == [
        "rs_boundary",
        "msg_boundary",
        "fc_boundary",
    ]
    assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == ["boundary answer"]
    assert [event["delta"] for event in events if event["type"] == "response.function_call_arguments.delta"] == [
        '{"value":"boundary"}'
    ]
    assert "orphan text" not in "".join(blocks)
    assert events[-1]["response"] == terminal["response"]
    sequences = [cast(int, event["sequence_number"]) for event in events]
    assert all(a < b for a, b in zip(sequences, sequences[1:]))
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type",
    ["response.created", "response.in_progress", "response.completed", "response.incomplete", "response.failed"],
)
@pytest.mark.parametrize("public", [True, False])
async def test_public_stream_rejects_foreign_envelope_before_rendering(event_type, public):
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    foreign = _boundary_terminal("resp_foreign", event_type)
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(format_sse_event(created), format_sse_event(foreign)), enforce_openai_sdk_contract=public
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    if not public:
        assert any(event.get("response") == foreign["response"] for event in events)
        return
    assert events[0] == created
    assert len(events) == 2
    assert events[-1]["type"] == "response.failed"
    failed = cast(dict, events[-1]["response"])
    assert failed["id"] == "resp_accepted"
    assert failed["error"]["code"] == "upstream_response_owner_mismatch"
    assert "resp_foreign" not in "".join(blocks)
    assert "boundary answer" not in "".join(blocks)
    assert "private reasoning" not in "".join(blocks)
    _consume_boundary_with_sdk(events)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["summary", "item"])
async def test_partial_reasoning_never_becomes_assistant_text_in_legacy_fallback(source_kind):
    terminal = _boundary_terminal("resp_boundary", "response.completed")
    response = cast(dict, terminal["response"])
    reasoning = response["output"][0]
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_boundary", "status": "in_progress", "output": []},
    }
    partial: dict[str, JsonValue] = (
        {
            "type": "response.reasoning_summary_text.delta",
            "sequence_number": 1,
            "output_index": 0,
            "item_id": "rs_boundary",
            "summary_index": 0,
            "delta": "visible summary",
        }
        if source_kind == "summary"
        else {"type": "response.output_item.done", "sequence_number": 1, "output_index": 0, "item": reasoning}
    )
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(format_sse_event(event) for event in [created, partial, terminal]))
        )
    ]
    events = [event for block in blocks if (event := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == ["boundary answer"]
    assert [event["output_index"] for event in events if event["type"] == "response.output_item.added"] == [0, 1, 2]
    assert [event["delta"] for event in events if event["type"] == "response.reasoning_summary_text.delta"] == (
        ["visible summary"] if source_kind == "summary" else []
    )
    assert events[-1]["response"] == response


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError], ids=["error", "cancelled"])
@pytest.mark.parametrize(("owns_reservation", "expected_releases"), [(True, 1), (False, 0)])
async def test_rate_limit_header_failure_releases_only_owned_reservation(
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[BaseException],
    owns_reservation: bool,
    expected_releases: int,
) -> None:
    reservation = object()
    failure = failure_type("rate-limit header failure")
    releases: list[object] = []

    async def fail_headers(*_args: object) -> dict[str, str]:
        raise failure

    async def release_reservation(value: object) -> None:
        releases.append(value)
        await asyncio.sleep(0)

    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", fail_headers)
    monkeypatch.setattr(proxy_api_module, "_release_reservation", release_reservation)

    with pytest.raises(failure_type) as caught:
        await proxy_api_module._rate_limit_headers_with_reservation_cleanup(
            cast(Any, object()),
            None,
            cast(Any, reservation if owns_reservation else None),
        )

    assert caught.value is failure
    assert releases == ([reservation] if expected_releases else [])


@pytest.mark.asyncio
async def test_rate_limit_header_cancellation_shields_reservation_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation = object()
    failure = asyncio.CancelledError("rate-limit header cancellation")
    releases: list[object] = []
    release_started = asyncio.Event()
    release_finished = asyncio.Event()

    async def cancel_headers(*_args: object) -> dict[str, str]:
        raise failure

    async def release_reservation(value: object) -> None:
        releases.append(value)
        release_started.set()
        await asyncio.sleep(0)
        release_finished.set()

    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", cancel_headers)
    monkeypatch.setattr(proxy_api_module, "_release_reservation", release_reservation)

    with anyio.CancelScope() as cancel_scope:
        cancel_scope.cancel()
        with pytest.raises(asyncio.CancelledError) as caught:
            await proxy_api_module._rate_limit_headers_with_reservation_cleanup(
                cast(Any, object()),
                None,
                cast(Any, reservation),
            )

    assert caught.value is failure
    assert release_started.is_set()
    assert release_finished.is_set()
    assert releases == [reservation]


@pytest.mark.asyncio
async def test_rate_limit_header_failure_defers_repeated_cancellation_until_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation = object()
    failure = RuntimeError("rate-limit header failure")
    releases: list[object] = []
    release_started = asyncio.Event()
    release_continue = asyncio.Event()
    release_finished = asyncio.Event()

    async def fail_headers(*_args: object) -> dict[str, str]:
        raise failure

    async def release_reservation(value: object) -> None:
        releases.append(value)
        release_started.set()
        await release_continue.wait()
        release_finished.set()

    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", fail_headers)
    monkeypatch.setattr(proxy_api_module, "_release_reservation", release_reservation)

    caller = asyncio.create_task(
        proxy_api_module._rate_limit_headers_with_reservation_cleanup(
            cast(Any, object()),
            None,
            cast(Any, reservation),
        )
    )
    await release_started.wait()
    caller.cancel()
    await asyncio.sleep(0)
    caller.cancel()
    release_continue.set()

    with pytest.raises(RuntimeError) as caught:
        await caller

    assert caught.value is failure
    assert release_finished.is_set()
    assert releases == [reservation]


@pytest.mark.asyncio
async def test_rate_limit_header_failure_survives_release_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reservation = object()
    header_failure = RuntimeError("rate-limit header failure")
    releases: list[object] = []

    async def fail_headers(*_args: object) -> dict[str, str]:
        raise header_failure

    async def fail_release(value: object) -> None:
        releases.append(value)
        raise ValueError("release persistence failed")

    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", fail_headers)
    monkeypatch.setattr(proxy_api_module, "_release_reservation", fail_release)

    with pytest.raises(RuntimeError) as caught:
        await proxy_api_module._rate_limit_headers_with_reservation_cleanup(
            cast(Any, object()),
            None,
            cast(Any, reservation),
        )

    assert caught.value is header_failure
    assert releases == [reservation]
    assert "Failed to release API key reservation after rate-limit header failure" in caplog.text


@pytest.mark.asyncio
async def test_rate_limit_header_failure_uses_reservation_cleanup_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservation = object()
    header_failure = RuntimeError("rate-limit header failure")
    released: list[str] = []

    async def fail_headers(*_args: object) -> dict[str, str]:
        raise header_failure

    async def record_release(
        value: object,
        *,
        action: str,
        scheduler: object,
        request_id: str,
    ) -> None:
        del value, scheduler, request_id
        released.append(action)

    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", fail_headers)
    monkeypatch.setattr(proxy_api_module, "_release_reservation_best_effort", record_release)
    cleanup = proxy_api_module._ResponsesReservationCleanup(
        owns_reservation=True,
        reservation=cast(Any, reservation),
        scheduler=None,
        request_id="req_header_cleanup",
    )

    with pytest.raises(RuntimeError) as caught:
        await proxy_api_module._rate_limit_headers_with_reservation_cleanup(
            cast(Any, object()),
            None,
            cast(Any, reservation),
            reservation_cleanup=cleanup,
        )

    assert caught.value is header_failure
    assert released == ["rate limit headers"]


def test_strip_blank_reasoning_comment_preserves_unmatched_whitespace_and_inline_comments() -> None:
    assert proxy_api_module._strip_blank_html_comment_lines("Need more steps\n") == "Need more steps\n"
    assert proxy_api_module._strip_blank_html_comment_lines("Hard break  \n") == "Hard break  \n"
    assert proxy_api_module._strip_blank_html_comment_lines("<!-- -->some text") == "<!-- -->some text"
    assert proxy_api_module._strip_blank_html_comment_lines("Plan\n\n<!-- -->") == "Plan"


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_removes_split_placeholder_delta() -> None:
    source = _iter_blocks(
        proxy_api_module.format_sse_event(
            {"type": "response.created", "response": {"id": "resp_1", "status": "in_progress", "output": []}}
        ),
        proxy_api_module.format_sse_event(
            {
                "type": "response.reasoning_summary_text.delta",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "delta": "Plan\n\n<!",
            }
        ),
        proxy_api_module.format_sse_event(
            {
                "type": "response.reasoning_summary_text.delta",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "delta": "-- -->",
            }
        ),
        proxy_api_module.format_sse_event(
            {
                "type": "response.reasoning_summary_text.done",
                "item_id": "rs_1",
                "output_index": 0,
                "summary_index": 0,
                "text": "Plan\n\n<!-- -->",
            }
        ),
        proxy_api_module.format_sse_event(
            {"type": "response.completed", "response": {"id": "resp_1", "status": "completed", "output": []}}
        ),
    )

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(source)]
    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    deltas = [
        payload for payload in payloads if payload and payload.get("type") == "response.reasoning_summary_text.delta"
    ]

    assert [payload["delta"] for payload in deltas] == ["Plan"]
    assert "<!-- -->" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_does_not_delay_less_than_text() -> None:
    first = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "x < y",
        }
    )
    second = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_part.added",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 1,
            "part": {"type": "summary_text", "text": "next"},
        }
    )

    blocks = [
        block async for block in proxy_api_module._normalize_reasoning_summary_stream(_iter_blocks(first, second))
    ]

    assert blocks == [first, second]


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_keeps_candidate_across_telemetry() -> None:
    first = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "<",
        }
    )
    second = proxy_api_module.format_sse_event({"type": "codex.rate_limits", "limits": {}})

    blocks = [
        block async for block in proxy_api_module._normalize_reasoning_summary_stream(_iter_blocks(first, second))
    ]

    assert blocks == [second, first]


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_removes_split_placeholder_across_telemetry() -> None:
    first = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "Plan\n\n<!",
        }
    )
    telemetry = proxy_api_module.format_sse_event({"type": "codex.rate_limits", "limits": {}})
    progress = proxy_api_module.format_sse_event({"type": "response.in_progress", "response": {"id": "resp_1"}})
    final = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "-- -->",
        }
    )

    blocks = [
        block
        async for block in proxy_api_module._normalize_reasoning_summary_stream(
            _iter_blocks(first, telemetry, progress, final)
        )
    ]

    assert blocks[:2] == [telemetry, progress]
    payload = proxy_api_module._parse_sse_payload(blocks[-1])
    assert payload is not None
    assert payload["delta"] == "Plan"
    assert "<!-- -->" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_cleans_complete_marker_inside_one_delta() -> None:
    source = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "Plan\n<!-- -->\nNext",
        }
    )

    blocks = [block async for block in proxy_api_module._normalize_reasoning_summary_stream(_iter_blocks(source))]
    payload = proxy_api_module._parse_sse_payload(blocks[0])

    assert payload is not None
    assert payload["delta"] == "Plan\nNext"


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_flushes_before_typeless_error() -> None:
    pending = proxy_api_module.format_sse_event(
        {
            "type": "response.reasoning_summary_text.delta",
            "item_id": "rs_1",
            "output_index": 0,
            "summary_index": 0,
            "delta": "Plan\n\n<!-- -->",
        }
    )
    typeless_error = "data: " + json.dumps({"error": {"code": "upstream_error"}}) + "\n\n"

    blocks = [
        block
        async for block in proxy_api_module._normalize_reasoning_summary_stream(_iter_blocks(pending, typeless_error))
    ]

    assert blocks[0] != typeless_error
    first_payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert first_payload is not None
    assert first_payload["delta"] == "Plan"
    assert blocks[1] == typeless_error


def test_normalize_reasoning_summary_part_removes_only_standalone_placeholder() -> None:
    payload, violation = proxy_api_module._normalize_public_stream_payload(
        {
            "type": "response.reasoning_summary_part.done",
            "part": {"type": "summary_text", "text": "Plan\n\n<!-- -->"},
        }
    )

    assert violation is None
    assert payload is not None
    assert payload["part"] == {"type": "summary_text", "text": "Plan"}


def test_compact_response_output_item_accepts_modeled_output_field() -> None:
    class ModeledCompactPayload(CompactResponsePayload):
        output: list[dict[str, JsonValue]] | None = None

    payload = ModeledCompactPayload.model_validate(
        {
            "object": "response.compaction",
            "output": [
                {
                    "id": "cmp_modeled_context",
                    "type": "compaction",
                    "encrypted_content": "MODELED_CONTEXT",
                }
            ],
        }
    )

    assert proxy_api_module._compact_response_output_item(payload) == {
        "id": "cmp_modeled_context",
        "type": "compaction",
        "encrypted_content": "MODELED_CONTEXT",
    }


def test_compact_response_output_item_preserves_summary_item_id() -> None:
    payload = CompactResponsePayload.model_validate(
        {
            "object": "response.compaction",
            "compaction_summary": {
                "id": "cmp_summary_context",
                "status": "completed",
                "encrypted_content": "SUMMARY_CONTEXT",
            },
        }
    )

    assert proxy_api_module._compact_response_output_item(payload) == {
        "id": "cmp_summary_context",
        "type": "compaction",
        "status": "completed",
        "encrypted_content": "SUMMARY_CONTEXT",
    }


def test_compact_response_output_item_drops_invalid_id_prefix() -> None:
    payload = CompactResponsePayload.model_validate(
        {
            "object": "response.compaction",
            "output": [
                {
                    "id": "msg_compact_context",
                    "type": "compaction",
                    "encrypted_content": "COMPACT_CONTEXT",
                }
            ],
        }
    )

    assert proxy_api_module._compact_response_output_item(payload) == {
        "type": "compaction",
        "encrypted_content": "COMPACT_CONTEXT",
    }


def test_compact_response_id_generates_unique_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(proxy_api_module, "get_request_id", lambda: None)
    payload = CompactResponsePayload.model_validate({"object": "response.compaction"})

    first = proxy_api_module._compact_response_id(payload)
    second = proxy_api_module._compact_response_id(payload)

    assert first.startswith("resp_")
    assert second.startswith("resp_")
    assert first != second


@pytest.mark.asyncio
async def test_synthetic_compaction_stream_preserves_mapping_usage() -> None:
    blocks = [
        block
        async for block in proxy_api_module._synthetic_compaction_response_stream(
            {"type": "compaction", "encrypted_content": "SUMMARY"},
            response_id="resp_mapping_usage",
            usage={"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        )
    ]

    completed = proxy_api_module._parse_sse_payload(blocks[3])
    assert completed is not None
    response = completed["response"]
    assert isinstance(response, dict)
    assert response["usage"] == {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}


@pytest.mark.asyncio
async def test_synthetic_compaction_stream_emits_complete_lifecycle() -> None:
    blocks = [
        block
        async for block in proxy_api_module._synthetic_compaction_response_stream(
            {
                "id": "cmp_authoritative",
                "type": "compaction",
                "status": "completed",
                "encrypted_content": "SUMMARY",
            },
            response_id="resp_compaction",
            usage=None,
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks[:-1]]
    assert all(payload is not None for payload in payloads)
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.output_item.added",
        "response.output_item.done",
        "response.completed",
    ]
    assert [payload["sequence_number"] for payload in payloads if payload is not None] == [0, 1, 2, 3]

    created, added, done, completed = payloads
    assert created is not None
    assert added is not None
    assert done is not None
    assert completed is not None
    assert created["response"] == {
        "id": "resp_compaction",
        "object": "response",
        "status": "in_progress",
        "output": [],
    }
    assert added["item"] == {
        "id": "cmp_authoritative",
        "type": "compaction",
        "status": "in_progress",
        "encrypted_content": "SUMMARY",
    }
    terminal_item = {
        "id": "cmp_authoritative",
        "type": "compaction",
        "status": "completed",
        "encrypted_content": "SUMMARY",
    }
    assert done["item"] == terminal_item
    completed_response = completed["response"]
    assert isinstance(completed_response, dict)
    assert completed_response["output"] == [terminal_item]
    assert blocks[-1] == "data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_collect_responses_payload_returns_contract_error_on_truncated_stream() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks('data: {"type":"response.output_text.delta","delta":"hello"}\n\n')
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["error"]["code"] == "upstream_stream_truncated"


@pytest.mark.asyncio
async def test_collect_responses_payload_captures_turn_state_metadata_before_failed_response() -> None:
    captured_headers: dict[str, str] = {}

    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            'data: {"type":"response.metadata","headers":{"X-Codex-Turn-State":" turn-owner "}}\n\n',
            'data: {"type":"response.failed","response":{"error":{"code":"upstream_error",'
            '"message":"failed","type":"server_error"}}}\n\n',
        ),
        captured_turn_state_headers=captured_headers,
    )

    assert result.error is not None
    assert captured_headers == {"x-codex-turn-state": "turn-owner"}


@pytest.mark.asyncio
async def test_collect_responses_preserves_captured_turn_state_when_stream_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.core.clients.proxy import ProxyResponseError
    from app.core.openai.requests import ResponsesRequest

    async def failing_stream():
        yield 'data: {"type":"response.metadata","headers":{"X-Codex-Turn-State":"turn-owner"}}\n\n'
        raise ProxyResponseError(
            502,
            {"error": {"code": "upstream_error", "message": "failed", "type": "server_error"}},
        )

    service = SimpleNamespace(stream_responses=lambda *_args, **_kwargs: failing_stream())
    context = SimpleNamespace(service=service)
    request = SimpleNamespace(
        headers={},
        method="POST",
        url=SimpleNamespace(path="/backend-api/codex/responses"),
        client=None,
    )
    monkeypatch.setattr(proxy_api_module, "_opportunistic_admission_denial", AsyncMock(return_value=None))
    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", AsyncMock(return_value=None))
    monkeypatch.setattr(proxy_api_module, "_rate_limit_headers_for_request", AsyncMock(return_value={}))
    monkeypatch.setattr(proxy_api_module, "_release_reservation", AsyncMock())

    response = await proxy_api_module._collect_responses(
        cast(Any, request),
        ResponsesRequest.model_validate({"model": "gpt-5.5", "instructions": "test", "input": []}),
        cast(Any, context),
        None,
    )

    assert response.status_code == 502
    assert response.headers["x-codex-turn-state"] == "turn-owner"


@pytest.mark.asyncio
async def test_collect_responses_payload_normalizes_unknown_output_item_to_message() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"fa_1","type":"final_answer","text":"hello from final answer"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"] == [
        {
            "id": "fa_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hello from final answer"}],
        }
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_appends_response_failed_on_invalid_json() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks("data: {not-json}\n\n"))
    ]

    assert len(blocks) == 2
    created_payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert created_payload is not None
    assert created_payload["type"] == "response.created"
    assert created_payload["sequence_number"] == 0
    payload = proxy_api_module._parse_sse_payload(blocks[1])
    assert payload is not None
    assert payload["type"] == "response.failed"
    assert payload["sequence_number"] == 1
    response = payload["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "invalid_json"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_initial_error_details() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"error","error":{"type":"rate_limit_error",'
                    '"code":"rate_limit_exceeded","message":"slow down","param":"model"}}\n\n'
                )
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == ["response.created", "response.failed"]
    assert [payload["sequence_number"] for payload in payloads] == [0, 1]
    response = payloads[1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["type"] == "rate_limit_error"
    assert error["code"] == "rate_limit_exceeded"
    assert error["message"] == "slow down"
    assert error["param"] == "model"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_masks_initial_previous_response_not_found() -> None:
    raw_response_id = "resp_0ba42212936dca97016a0d52aec2588191bc2499d3088e4e3e"
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"error","status":400,"error":{"type":"invalid_request_error",'
                    '"code":"previous_response_not_found",'
                    f'"message":"Previous response with id \'{raw_response_id}\' not found.",'
                    '"param":"previous_response_id"}}\n\n'
                )
            )
        )
    ]

    joined = "".join(blocks)
    assert "previous_response_not_found" not in joined
    assert raw_response_id not in joined
    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == ["response.created", "response.failed"]
    response = payloads[1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["type"] == "server_error"
    assert error["code"] == "stream_incomplete"
    assert error["message"] == "Upstream websocket closed before response.completed"
    assert "param" not in error


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_masks_typeless_previous_response_not_found() -> None:
    raw_response_id = "resp_typeless_stale"
    source = (
        "data: "
        + json.dumps(
            {
                "error": {
                    "type": "invalid_request_error",
                    "code": "previous_response_not_found",
                    "message": f"Previous response with id '{raw_response_id}' not found.",
                    "param": "previous_response_id",
                }
            }
        )
        + "\n\n"
    )

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(source))]

    joined = "".join(blocks)
    assert "previous_response_not_found" not in joined
    assert raw_response_id not in joined
    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == ["response.created", "response.failed"]
    response = payloads[1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "stream_incomplete"
    assert "param" not in error


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_masks_nested_stale_error_but_keeps_response_id() -> None:
    raw_response_id = "resp_nested_stale"
    source = (
        "data: "
        + json.dumps(
            {
                "type": "response.failed",
                "response": {
                    "id": raw_response_id,
                    "status": "failed",
                    "error": {
                        "type": "invalid_request_error",
                        "code": "previous_response_not_found",
                        "message": f"Previous response with id '{raw_response_id}' not found.",
                        "param": "previous_response_id",
                    },
                },
            }
        )
        + "\n\n"
    )

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(source))]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    response = payloads[-1]["response"]
    assert isinstance(response, dict)
    assert response["id"] == raw_response_id
    error = response["error"]
    assert isinstance(error, dict)
    assert error == {
        "message": "Upstream websocket closed before response.completed",
        "type": "server_error",
        "code": "stream_incomplete",
    }


@pytest.mark.asyncio
async def test_collect_responses_payload_recognizes_typeless_error_event() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks('data: {"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}\n\n')
    )

    assert result.error is not None
    assert result.error.code == "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_sanitizes_native_malformed_error_param() -> None:
    source = 'event: error\ndata: {"type":"error","error":{"code":"upstream_error","message":"bad","param":{}}}\n\n'

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(source),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks[-1] == "data: [DONE]\n\n"
    payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert payload is not None
    error = payload["error"]
    assert isinstance(error, dict)
    assert "param" not in error
    assert payload["type"] == "error"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_sanitizes_top_level_and_nested_error_params() -> None:
    source = (
        'data: {"type":"error","param":[],"status":400,'
        '"error":{"code":"upstream_error","message":"bad","param":" model "}}\n\n'
    )

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(source),
            enforce_openai_sdk_contract=False,
        )
    ]

    payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert payload is not None
    assert payload["type"] == "error"
    assert payload["status"] == 400
    assert "param" not in payload
    error = payload["error"]
    assert isinstance(error, dict)
    assert error["param"] == "model"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_sanitizes_response_failed_top_level_param() -> None:
    source = (
        'data: {"type":"response.failed","param":{},"response":{"id":"resp_1",'
        '"error":{"code":"upstream_error","message":"bad","param":[]}}}\n\n'
    )

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(source),
            enforce_openai_sdk_contract=False,
        )
    ]

    payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert payload is not None
    assert payload["type"] == "response.failed"
    assert "param" not in payload
    response = payload["response"]
    assert isinstance(response, dict)
    assert response["id"] == "resp_1"
    error = response["error"]
    assert isinstance(error, dict)
    assert "param" not in error


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_keeps_native_valid_error_frame_byte_identical() -> None:
    source = 'data: {"type":"error","error":{"code":"upstream_error","message":"bad","param":"model"}}\n\n'

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(source),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks == [source, "data: [DONE]\n\n"]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_comment_keepalive() -> None:
    terminal = 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(": keepalive\n\n", terminal),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks[0] == ": keepalive\n\n"
    assert "response.completed" in blocks[-2]
    assert blocks[-1] == "data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_comment_keepalive_for_public_contract() -> None:
    terminal = 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(": keepalive\n\n", terminal),
        )
    ]

    assert blocks[0] == ": keepalive\n\n"
    assert "response.completed" in blocks[-1]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_normalizes_unknown_terminal_output_item() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_1",'
                    '"object":"response",'
                    '"status":"completed","output":[{"id":"fa_1","type":"final_answer","text":"normalized"}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [p["type"] for p in payloads] == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert payloads[3]["delta"] == "normalized"
    payload = payloads[-1]
    assert payload is not None
    assert payload["type"] == "response.completed"
    response = payload["response"]
    assert isinstance(response, dict)
    output = response["output"]
    assert isinstance(output, list)
    assert output == [
        {
            "id": "fa_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "normalized"}],
        }
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_synthesizes_delta_from_done_message() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant",'
                    '"content":[{"type":"output_text","text":"visible text"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":2,"response":{"id":"resp_1",'
                    '"object":"response",'
                    '"status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [p["type"] for p in payloads] == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert payloads[3]["delta"] == "visible text"
    assert payloads[3]["item_id"] == "msg_1"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_synthesizes_delta_from_completed_output() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_1",'
                    '"object":"response",'
                    '"status":"completed","output":[{"id":"msg_1","type":"message",'
                    '"content":[{"type":"output_text","text":"terminal text"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [p["type"] for p in payloads] == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert payloads[3]["delta"] == "terminal text"
    assert payloads[3]["item_id"] == "msg_1"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_does_not_duplicate_existing_delta() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                'data: {"type":"response.output_text.delta","sequence_number":1,"item_id":"msg_1",'
                '"delta":"already visible"}\n\n',
                (
                    'data: {"type":"response.output_item.done","sequence_number":2,"output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant",'
                    '"content":[{"type":"output_text","text":"already visible"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":3,"response":{"id":"resp_1",'
                    '"object":"response",'
                    '"status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    event_types = [payload["type"] for payload in payloads if payload is not None]
    assert event_types == [
        "response.created",
        "response.output_text.delta",
        "response.output_item.done",
        "response.completed",
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_apply_patch_call_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"apc_1","type":"apply_patch_call","status":"completed",'
                '"call_id":"call_1","patch":"*** Begin Patch\\n*** End Patch\\n"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"] == [
        {
            "id": "apc_1",
            "type": "apply_patch_call",
            "status": "completed",
            "call_id": "call_1",
            "patch": "*** Begin Patch\n*** End Patch\n",
        }
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_mcp_approval_request_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"mcp_1","type":"mcp_approval_request","status":"in_progress",'
                '"request_id":"req_1","server_label":"github","tool_name":"repos/list"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_2","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_2"
    assert body["output"] == [
        {
            "id": "mcp_1",
            "type": "mcp_approval_request",
            "status": "in_progress",
            "request_id": "req_1",
            "server_label": "github",
            "tool_name": "repos/list",
        }
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_output_image_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"img_1","type":"output_image","image_url":"https://example.com/a.png"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_3","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_3"
    assert body["output"] == [
        {
            "id": "img_1",
            "type": "output_image",
            "image_url": "https://example.com/a.png",
        }
    ]


# --- OpenAI SDK stream contract regressions ---
# These tests cover the public /v1/responses streaming SSE contract gaps found
# during the OpenAI Python SDK compatibility audit. See change
# `normalize-v1-responses-openai-sdk-stream` in openspec/changes/ for the
# full audit, design rationale, and spec delta.


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_codex_rate_limits_prefix() -> None:
    """G1: Codex-internal vendor events MUST NOT leak onto the public /v1 stream.

    The upstream Codex backend emits `codex.rate_limits` (throttled per window)
    before `response.created`. The OpenAI SDK's ResponseStreamState raises
    `RuntimeError: Expected to have received 'response.created' before
    'codex.rate_limits'` on the first event. The /v1 normalizer must drop any
    `codex.*` event so the first event the public stream emits is
    `response.created`.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":true}}\n\n',
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,'
                    '"response":{"id":"resp_1","object":"response","status":"completed",'
                    '"output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"hi"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    event_types = [p["type"] for p in payloads if p is not None]
    assert "codex.rate_limits" not in event_types
    # First event MUST be response.created (OpenAI SDK contract A)
    standard_events = [t for t in event_types if t not in ("[DONE]",)]
    assert standard_events[0] == "response.created"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "diagnostic_position", [0, 1, 2], ids=["before-created", "before-completed", "after-completed"]
)
@pytest.mark.parametrize("enforce_contract", [True, False], ids=["public", "native"])
async def test_normalize_responses_stream_filters_timing_only_for_public_contract(
    diagnostic_position: int, enforce_contract: bool
) -> None:
    upstream_blocks = [
        'data: {"type":"response.created","sequence_number":0,'
        '"response":{"id":"resp_timing","object":"response","status":"in_progress","output":[]}}\n\n',
        'data: {"type":"response.completed","sequence_number":1,'
        '"response":{"id":"resp_timing","object":"response","status":"completed","output":[]}}\n\n',
    ]
    upstream_blocks.insert(diagnostic_position, 'data: {"type":"responsesapi.websocket_timing","latency_ms":12}\n\n')
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*upstream_blocks), enforce_openai_sdk_contract=enforce_contract
        )
    ]
    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    event_types = [payload["type"] for payload in payloads if payload is not None]
    expected_types = ["response.created", "response.completed"]
    if not enforce_contract:
        expected_types.insert(diagnostic_position, "responsesapi.websocket_timing")
    assert event_types == expected_types


@pytest.mark.parametrize(
    ("event_type", "forwarded"),
    [("response.future_event", True), ("error", True), ("vendor.future_event", False)],
)
def test_public_responses_event_family_contract(event_type: str, forwarded: bool) -> None:
    payload: dict[str, JsonValue] = {"type": event_type, "code": "server_error", "message": "test error"}
    normalized, violation = proxy_api_module._normalize_public_stream_payload(payload)
    assert (normalized is not None) is forwarded
    if normalized is not None:
        assert normalized["type"] == event_type
    assert violation is None


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_backfills_terminal_output_from_items() -> None:
    """G3: terminal `response.completed.output` MUST be backfilled from
    `response.output_item.done` events when upstream sends empty output.

    The Codex backend emits items via `output_item.done` and then sends
    `response.completed` with `output: []`. The non-streaming path
    (`_collect_responses_payload`) already merges these; the streaming path
    must do the same so OpenAI SDK consumers calling
    `stream.get_final_response().output` see the items.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"backfilled"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":2,'
                    '"response":{"id":"resp_1","object":"response","status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    completed = next(p for p in payloads if p and p.get("type") == "response.completed")
    response_obj = completed["response"]
    assert isinstance(response_obj, dict)
    output = response_obj["output"]
    assert isinstance(output, list)
    assert len(output) == 1
    output_item = cast(dict[str, Any], output[0])
    assert output_item["id"] == "msg_1"
    assert output_item["type"] == "message"
    assert output_item["content"] == [{"type": "output_text", "text": "backfilled"}]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_existing_terminal_output() -> None:
    """G3 inverse: when upstream already includes terminal `output`,
    the normalizer MUST NOT overwrite it from collected items."""
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,'
                    '"item":{"id":"msg_stream","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"from-stream-events"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":2,'
                    '"response":{"id":"resp_1","object":"response","status":"completed",'
                    '"output":[{"id":"msg_terminal","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"from-terminal"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    completed = next(p for p in payloads if p and p.get("type") == "response.completed")
    response_obj = completed["response"]
    assert isinstance(response_obj, dict)
    output = response_obj["output"]
    assert isinstance(output, list)
    assert len(output) == 1
    output_item = cast(dict[str, Any], output[0])
    assert output_item["id"] == "msg_terminal"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_synthesizes_response_created_on_leading_failure() -> None:
    """G4: when the upstream stream's first standard event is not
    `response.created` (e.g. upstream rejects and emits only
    `response.failed`), the normalizer MUST synthesize a `response.created`
    event from the failed event's envelope so the OpenAI SDK parser can begin.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.failed","sequence_number":0,'
                    '"response":{"id":"resp_err","object":"response","status":"failed","output":[],'
                    '"error":{"code":"invalid_request_error","message":"bad schema"}}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    event_types = [p["type"] for p in payloads if p is not None]
    # response.created must be synthesized FIRST, then response.failed forwarded
    assert event_types[:2] == ["response.created", "response.failed"]
    created = payloads[0]
    assert created is not None
    created_response = created["response"]
    assert isinstance(created_response, dict)
    # Synthesized envelope must use in_progress + empty output (the contract
    # values for response.created), not copy "failed" from the source.
    assert created_response["status"] == "in_progress"
    assert created_response["output"] == []
    # But the upstream id is preserved so downstream consumers can correlate.
    assert created_response["id"] == "resp_err"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_sequences_created_before_unsequenced_leading_failure() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.failed","response":{"id":"resp_err","object":"response",'
                '"status":"failed","error":{"code":"stream_incomplete","message":"closed"}}}\n\n'
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    created = next(payload for payload in payloads if payload and payload.get("type") == "response.created")
    failed = next(payload for payload in payloads if payload and payload.get("type") == "response.failed")
    assert created["sequence_number"] == 0
    assert failed["sequence_number"] == 1


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_replaces_non_integer_failure_sequence() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":7,'
                    '"response":{"id":"resp_err","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.failed","sequence_number":"error",'
                    '"response":{"id":"resp_err","object":"response","status":"failed",'
                    '"error":{"code":"stream_incomplete","message":"closed"}}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    failed = next(payload for payload in payloads if payload and payload.get("type") == "response.failed")
    assert failed["sequence_number"] == 8


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_sequences_failure_after_reasoning() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":7,'
                    '"response":{"id":"resp_err","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.reasoning_summary_text.delta","sequence_number":8,'
                    '"item_id":"rs_1","output_index":0,"summary_index":0,"delta":"Checking."}\n\n'
                ),
                (
                    'data: {"type":"response.failed","response":{"id":"resp_err","object":"response",'
                    '"status":"failed","error":{"code":"stream_incomplete","message":"closed"}}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    failed = next(payload for payload in payloads if payload and payload.get("type") == "response.failed")
    assert failed["sequence_number"] == 9


@pytest.mark.asyncio
@pytest.mark.parametrize(("failure_sequence", "created_sequence"), [(12, 11), (0, -1)])
async def test_normalize_public_responses_stream_preserves_failure_sequence(
    failure_sequence: int,
    created_sequence: int,
) -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    f'data: {{"type":"response.failed","sequence_number":{failure_sequence},'
                    '"response":{"id":"resp_err","object":"response","status":"failed",'
                    '"error":{"code":"stream_incomplete","message":"closed"}}}\n\n'
                )
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    created = next(payload for payload in payloads if payload and payload.get("type") == "response.created")
    failed = next(payload for payload in payloads if payload and payload.get("type") == "response.failed")
    assert created["sequence_number"] == created_sequence
    assert failed["sequence_number"] == failure_sequence


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_preserves_unsequenced_failure() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.failed","response":{"id":"resp_err","object":"response",'
                '"status":"failed","error":{"code":"stream_incomplete","message":"closed"}}}\n\n'
            ),
            enforce_openai_sdk_contract=False,
        )
    ]

    failed = proxy_api_module._parse_sse_payload(blocks[0])
    assert failed is not None
    assert "sequence_number" not in failed


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_precreated_output_when_envelope_arrives() -> None:
    """A: public /v1 must never attach anonymous pre-created output to a later response.

    A downstream-cancelled HTTP bridge request can leave behind an anonymous
    output event that has no response envelope. If a later retry response
    envelope arrives, the orphan output still has no id proving ownership, so it
    must be dropped rather than replayed into the retry.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,'
                    '"item":{"id":"msg_orphan","type":"message","role":"assistant",'
                    '"status":"in_progress","content":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,'
                    '"response":{"id":"resp_retry","object":"response","status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    event_types = [payload["type"] for payload in payloads if payload is not None]
    assert event_types[:2] == ["response.created", "response.completed"]
    assert "response.output_item.added" not in event_types
    assert payloads[0] is not None
    created_response = payloads[0]["response"]
    assert isinstance(created_response, dict)
    assert created_response["id"] == "resp_retry"
    completed = payloads[1]
    assert completed is not None
    completed_response = completed["response"]
    assert isinstance(completed_response, dict)
    assert completed_response["output"] == []


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_replays_legacy_precreated_text_after_created() -> None:
    """Legacy unindexed text events can be preserved without violating SDK order.

    These events have no output lifecycle of their own, so the normalizer emits
    a synthetic message/content-part envelope after response.created before it
    replays the visible text events.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.output_text.delta","delta":"hello "}\n\n',
                'data: {"type":"response.output_text.done","text":"hello world"}\n\n',
                ('data: {"type":"response.content_part.done","part":{"type":"output_text","text":"hello world"}}\n\n'),
                (
                    'data: {"type":"response.completed","sequence_number":9,'
                    '"response":{"id":"resp_legacy","object":"response","status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    replayed_delta = payloads[3]
    assert replayed_delta["output_index"] == 0
    assert replayed_delta["content_index"] == 0
    assert replayed_delta["item_id"] == "msg_resp_legacy_precreated"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_legacy_precreated_text_suppresses_terminal_duplicate() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.output_text.delta","delta":"hello world"}\n\n',
                (
                    'data: {"type":"response.completed","sequence_number":9,'
                    '"response":{"id":"resp_legacy","object":"response","status":"completed",'
                    '"output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"hello world"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    event_types = [payload["type"] for payload in payloads]
    assert event_types == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_item.done",
        "response.completed",
    ]
    assert [payload.get("delta") for payload in payloads if payload["type"] == "response.output_text.delta"] == [
        "hello world"
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_dropped_precreated_delta_does_not_suppress_terminal_delta() -> None:
    """Buffered orphan deltas are not marked seen until actually emitted.

    Indexed pre-created deltas are dropped as unowned cancel/retry orphans. If a
    later terminal response carries the real output, the normalizer must still
    synthesize a replacement text delta for SDK streaming consumers.
    """
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_text.delta","sequence_number":0,'
                    '"output_index":0,"content_index":0,"item_id":"msg_1",'
                    '"delta":"terminal text"}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_1",'
                    '"object":"response","status":"completed",'
                    '"output":[{"id":"msg_1","type":"message",'
                    '"content":[{"type":"output_text","text":"terminal text"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == [
        "response.created",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert payloads[3]["delta"] == "terminal text"
    assert payloads[3]["item_id"] == "msg_1"
    _consume_boundary_with_sdk(payloads)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_fails_when_long_prelude_ends_without_envelope() -> None:
    event_count = proxy_api_module._PUBLIC_RESPONSES_PRE_CREATED_BUFFER_LIMIT + 1
    source_blocks = [
        f'data: {{"type":"response.output_text.delta","delta":"orphan {index}"}}\n\n' for index in range(event_count)
    ]

    blocks = [
        block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*source_blocks))
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    payloads = [payload for payload in payloads if payload is not None]
    assert [payload["type"] for payload in payloads] == ["response.created", "response.failed"]
    response = payloads[1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "upstream_stream_truncated"


@pytest.mark.asyncio
@pytest.mark.parametrize("prelude_count", [65, 1024])
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
async def test_normalize_public_responses_stream_consumes_owned_terminal_after_long_prelude(
    prelude_count: int, terminal_type: str
) -> None:
    owned_output: list[JsonValue] = [
        {
            "type": "message",
            "id": "msg_owned",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
        },
        {
            "type": "function_call",
            "id": "fc_owned",
            "call_id": "call_owned",
            "name": "echo",
            "arguments": '{"note":"owned"}',
            "status": "completed",
        },
        {
            "type": "reasoning",
            "id": "rs_owned",
            "summary": [{"type": "summary_text", "text": "owned reasoning"}],
            "encrypted_content": "opaque-owned-state",
        },
    ]
    consumed_terminal = False

    async def source() -> AsyncIterator[str]:
        nonlocal consumed_terminal
        for index in range(prelude_count):
            yield proxy_api_module.format_sse_event(
                {
                    "type": "response.output_text.delta",
                    "sequence_number": index,
                    "output_index": 0,
                    "item_id": "msg_orphan",
                    "delta": "orphan text",
                }
            )
        consumed_terminal = True
        yield proxy_api_module.format_sse_event(
            {
                "type": terminal_type,
                "sequence_number": prelude_count,
                "response": {
                    "id": "resp_owned",
                    "object": "response",
                    "status": terminal_type.removeprefix("response."),
                    "output": owned_output,
                },
            }
        )

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(source())]
    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert consumed_terminal
    assert payloads[0]["type"] == "response.created"
    created_response = payloads[0]["response"]
    assert isinstance(created_response, dict)
    assert created_response["id"] == "resp_owned"
    assert payloads[-1]["type"] == terminal_type
    terminal_response = payloads[-1]["response"]
    assert isinstance(terminal_response, dict)
    assert terminal_response["output"] == owned_output
    assert not any(p["type"] == "response.failed" for p in payloads)
    assert "orphan text" not in "".join(blocks)
    assert "msg_orphan" not in "".join(blocks)
    added_item_ids = []
    for payload in payloads:
        if payload["type"] == "response.output_item.added":
            item = payload["item"]
            assert isinstance(item, dict)
            added_item_ids.append(item["id"])
    assert added_item_ids == [
        "msg_owned",
        "fc_owned",
        "rs_owned",
    ]
    assert [p["delta"] for p in payloads if p["type"] == "response.output_text.delta"] == ["owned answer"]
    sequence_numbers: list[int] = []
    for payload in payloads:
        sequence_number = payload["sequence_number"]
        assert isinstance(sequence_number, int)
        sequence_numbers.append(sequence_number)
    assert all(a < b for a, b in zip(sequence_numbers, sequence_numbers[1:]))


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.incomplete"])
@pytest.mark.parametrize("reasoning_summary", [[], [{"type": "summary_text", "text": "owned reasoning"}]])
async def test_normalize_public_responses_stream_recovers_terminal_items_after_accepted_creation(
    terminal_type: str,
    reasoning_summary: list[JsonValue],
) -> None:
    """A full terminal must open every item before exposing its text or tool arguments."""
    output: list[JsonValue] = [
        {"type": "reasoning", "id": "rs_accepted", "summary": reasoning_summary},
        {
            "type": "message",
            "id": "msg_accepted",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
        },
        {
            "type": "function_call",
            "id": "fc_accepted",
            "call_id": "call_accepted",
            "name": "Read",
            "arguments": '{"path":"owned.txt"}',
            "status": "completed",
        },
    ]
    source: list[dict[str, JsonValue]] = [
        {
            "type": event_type,
            "sequence_number": sequence,
            "response": {"id": "resp_accepted", "object": "response", "status": "in_progress", "output": []},
        }
        for sequence, event_type in enumerate(["response.created", "response.in_progress"])
    ]
    source.extend(
        [
            {"type": "response.metadata", "sequence_number": 155, "response_id": "resp_accepted"},
            {
                "type": terminal_type,
                "sequence_number": 156,
                "response": {
                    "id": "resp_accepted",
                    "object": "response",
                    "status": terminal_type.removeprefix("response."),
                    "output": output,
                },
            },
        ]
    )

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(proxy_api_module.format_sse_event(payload) for payload in source))
        )
    ]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]

    opened: set[int] = set()
    closed: set[int] = set()
    for event in events:
        output_index = event.get("output_index")
        if event["type"] == "response.output_item.added":
            assert isinstance(output_index, int)
            assert output_index not in opened
            opened.add(output_index)
        elif isinstance(output_index, int):
            assert output_index in opened, f"content arrived before its item: {event['type']}"
            assert output_index not in closed
            if event["type"] == "response.output_item.done":
                closed.add(output_index)
    assert opened == closed == {0, 1, 2}
    assert [e["delta"] for e in events if e["type"] == "response.output_text.delta"] == ["owned answer"]
    assert [e["delta"] for e in events if e["type"] == "response.function_call_arguments.delta"] == [
        '{"path":"owned.txt"}'
    ]
    assert [e["type"] for e in events].count("response.created") == 1
    assert events[-1]["type"] == terminal_type
    response = events[-1]["response"]
    assert isinstance(response, dict)
    assert response["output"] == output
    sequences = [cast(int, event["sequence_number"]) for event in events]
    assert all(a < b for a, b in zip(sequences, sequences[1:]))

    async with AsyncOpenAI(
        api_key="offline-test",
        http_client=cast(
            Any,
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _request: httpx.Response(
                        200, headers={"content-type": "text/event-stream"}, content="".join(blocks).encode()
                    )
                )
            ),
        ),
    ) as client:
        async with client.responses.stream(model="offline-test", input="offline-test") as stream:
            sdk_events = [event async for event in stream]
            assert [event.delta for event in sdk_events if event.type == "response.output_text.delta"] == [
                "owned answer"
            ]
            assert [event.delta for event in sdk_events if event.type == "response.function_call_arguments.delta"] == [
                '{"path":"owned.txt"}'
            ]
            if terminal_type == "response.completed":
                final = await stream.get_final_response()
                assert final.output_text == "owned answer"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario", ["native", "text_delta", "tool_delta", "reasoning_delta", "empty_terminal", "different_owner"]
)
async def test_accepted_terminal_recovery_does_not_duplicate_or_reassign_existing_output(scenario: str) -> None:
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    terminal: dict[str, JsonValue] = {
        "type": "response.completed",
        "sequence_number": 10,
        "response": {
            "id": "resp_other" if scenario == "different_owner" else "resp_accepted",
            "status": "completed",
            "output": []
            if scenario == "empty_terminal"
            else [
                {
                    "id": "msg_accepted",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "terminal answer"}],
                }
            ],
        },
    }
    source = [created]
    prior_delta = {
        "text_delta": "response.output_text.delta",
        "tool_delta": "response.function_call_arguments.delta",
        "reasoning_delta": "response.reasoning_summary_text.delta",
    }.get(scenario)
    if prior_delta is not None:
        source.append({"type": prior_delta, "sequence_number": 1, "delta": "already sent"})
    source.append(terminal)
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*(proxy_api_module.format_sse_event(payload) for payload in source)),
            enforce_openai_sdk_contract=scenario != "native",
        )
    ]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]

    if scenario == "reasoning_delta":
        assert [event["output_index"] for event in events if event["type"] == "response.output_item.added"] == [0]
        assert [event["delta"] for event in events if event["type"] == "response.output_text.delta"] == [
            "terminal answer"
        ]
        _consume_boundary_with_sdk(events)
    else:
        assert not any(event["type"] == "response.output_item.added" for event in events)
    if prior_delta is not None:
        assert [event["delta"] for event in events if event["type"] == prior_delta] == ["already sent"]
    if scenario == "empty_terminal":
        assert events == source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra_type", ["response.completed", "response.incomplete", "response.function_call_arguments.delta"]
)
async def test_accepted_terminal_recovery_emits_no_output_after_the_first_terminal(
    extra_type: str, monkeypatch
) -> None:
    violations: list[str] = []
    monkeypatch.setattr(proxy_api_module, "_record_public_contract_violation", violations.append)
    created: dict[str, JsonValue] = {
        "type": "response.created",
        "sequence_number": 0,
        "response": {"id": "resp_accepted", "status": "in_progress", "output": []},
    }
    terminal: dict[str, JsonValue] = {
        "type": "response.completed",
        "sequence_number": 20,
        "response": {
            "id": "resp_accepted",
            "status": "completed",
            "output": [
                {
                    "id": "fc_accepted",
                    "type": "function_call",
                    "call_id": "call_accepted",
                    "name": "Read",
                    "arguments": '{"path":"owned.txt"}',
                    "status": "completed",
                }
            ],
        },
    }
    extra = {**terminal, "type": extra_type, "sequence_number": 21}
    extra["response"] = {**cast(dict, terminal["response"]), "id": "resp_foreign"}
    if extra_type.endswith(".delta"):
        extra = {
            "type": extra_type,
            "sequence_number": 21,
            "output_index": 0,
            "delta": "late arguments",
            "response_id": "resp_foreign",
        }
    source = _iter_blocks(*(proxy_api_module.format_sse_event(p) for p in [created, terminal, extra]))

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(source)]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]

    assert [event["type"] for event in events] == [
        "response.created",
        "response.output_item.added",
        "response.function_call_arguments.delta",
        "response.function_call_arguments.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert [event["delta"] for event in events if event["type"] == "response.function_call_arguments.delta"] == [
        '{"path":"owned.txt"}'
    ]
    assert violations == [], "Late ignored events must not turn a completed stream into a monitoring error"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_waits_for_created_after_long_prelude() -> None:
    prelude = [
        proxy_api_module.format_sse_event({"type": "response.output_text.delta", "delta": "orphan"}) for _ in range(96)
    ]
    owned = [
        {
            "type": "response.created",
            "response": {"id": "resp_owned", "object": "response", "status": "in_progress", "output": []},
        },
        {"type": "response.output_text.delta", "delta": "owned"},
        {"type": "response.completed", "response": {"id": "resp_owned", "status": "completed", "output": []}},
    ]
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*prelude, *(proxy_api_module.format_sse_event(p) for p in owned))
        )
    ]
    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert payloads == owned


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_failure_after_long_prelude() -> None:
    prelude = ['data: {"type":"response.output_text.delta","delta":"orphan"}\n\n'] * 96
    upstream_error = 'data: {"type":"error","error":{"code":"overloaded_error","message":"busy"}}\n\n'
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*prelude, upstream_error))
    ]
    payloads = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert payloads[-1]["type"] == "response.failed"
    response = payloads[-1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "overloaded_error"
    assert "orphan" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_does_not_swallow_cancellation_after_long_prelude() -> None:
    finalized = False

    async def source() -> AsyncIterator[str]:
        nonlocal finalized
        try:
            for _ in range(96):
                yield 'data: {"type":"response.output_text.delta","delta":"orphan"}\n\n'
            raise asyncio.CancelledError
        finally:
            finalized = True

    with pytest.raises(asyncio.CancelledError):
        _ = [block async for block in proxy_api_module._normalize_public_responses_stream(source())]
    assert finalized


@pytest.mark.asyncio
async def test_native_codex_stream_keeps_long_prelude_without_public_recovery() -> None:
    source = ['data: {"type":"response.output_text.delta","delta":"native"}\n\n'] * 96
    source.append('data: {"type":"response.completed","response":{"id":"resp_native","output":[]}}\n\n')
    output = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(*source), enforce_openai_sdk_contract=False
        )
    ]
    assert output[-1] == "data: [DONE]\n\n"
    assert [proxy_api_module._parse_sse_payload(block) for block in output[:-1]] == [
        proxy_api_module._parse_sse_payload(block) for block in source
    ]


@pytest.mark.asyncio
async def test_late_owned_terminal_preserves_multipart_text_refusal_and_empty_content() -> None:
    parts: list[JsonValue] = [
        {"type": "output_text", "text": "first", "annotations": []},
        {"type": "output_text", "text": "second", "annotations": []},
        {"type": "refusal", "refusal": "declined"},
        {"type": "output_text", "text": "", "annotations": []},
    ]
    output: list[JsonValue] = [{"type": "message", "id": "msg_owned", "role": "assistant", "content": parts}]
    terminal = proxy_api_module.format_sse_event(
        {
            "type": "response.completed",
            "response": {
                "id": "resp_owned",
                "status": "completed",
                "output": output,
            },
        }
    )
    prelude = ['data: {"type":"response.output_text.delta","delta":"orphan"}\n\n'] * 96
    blocks = [
        block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*prelude, terminal))
    ]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [(p["content_index"], p["delta"]) for p in events if p["type"] == "response.output_text.delta"] == [
        (0, "first"),
        (1, "second"),
        (3, ""),
    ]
    assert [(p["content_index"], p["delta"]) for p in events if p["type"] == "response.refusal.delta"] == [
        (2, "declined")
    ]
    assert [p["part"] for p in events if p["type"] == "response.content_part.done"] == parts
    response = events[-1]["response"]
    assert isinstance(response, dict)
    assert response["output"] == output


@pytest.mark.asyncio
async def test_recovered_function_lifecycle_validates_against_sdk_event_schema() -> None:
    output: list[JsonValue] = [
        {
            "type": "function_call",
            "id": "fc_owned",
            "call_id": "call_owned",
            "name": "echo",
            "arguments": '{"note":"owned"}',
            "status": "completed",
        }
    ]
    terminal = proxy_api_module.format_sse_event(
        {
            "type": "response.completed",
            "response": {
                "id": "resp_owned",
                "status": "completed",
                "output": output,
            },
        }
    )
    prelude = ['data: {"type":"response.output_text.delta","delta":"orphan"}\n\n'] * 96
    blocks = [
        block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*prelude, terminal))
    ]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    adapter = TypeAdapter(ResponseStreamEvent)
    for event in events[1:-1]:
        adapter.validate_python(event)


@pytest.mark.asyncio
async def test_recovered_reasoning_content_is_not_reemitted_as_assistant_text() -> None:
    output: list[JsonValue] = [
        {
            "type": "reasoning",
            "id": "rs_owned",
            "summary": [],
            "content": [{"type": "reasoning_text", "text": "private reasoning"}],
            "encrypted_content": "opaque-owned-state",
        },
        {
            "type": "message",
            "id": "msg_owned",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "owned answer", "annotations": []}],
        },
    ]
    terminal = proxy_api_module.format_sse_event(
        {
            "type": "response.completed",
            "sequence_number": 96,
            "response": {"id": "resp_owned", "status": "completed", "output": output},
        }
    )
    prelude = ['data: {"type":"response.output_text.delta","delta":"orphan"}\n\n'] * 96
    blocks = [
        block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(*prelude, terminal))
    ]
    events = [p for block in blocks if (p := proxy_api_module._parse_sse_payload(block)) is not None]
    assert [p["delta"] for p in events if p["type"] == "response.output_text.delta"] == ["owned answer"]
    response = events[-1]["response"]
    assert isinstance(response, dict)
    assert response["output"] == output
    assert all(isinstance(p.get("sequence_number"), int) for p in events)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_precreated_output_when_no_envelope_arrives() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,'
                    '"item":{"id":"msg_orphan","type":"message","role":"assistant",'
                    '"status":"in_progress","content":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    event_types = [payload["type"] for payload in payloads if payload is not None]
    assert event_types == ["response.created", "response.failed"]
    assert payloads[1] is not None
    response = payloads[1]["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "upstream_stream_truncated"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_does_not_double_emit_response_created() -> None:
    """G4 inverse: when the upstream stream already starts with
    `response.created`, the normalizer MUST NOT emit a second synthesized one."""
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,'
                    '"response":{"id":"resp_1","object":"response","status":"completed",'
                    '"output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"ok"}]}]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    event_types = [p["type"] for p in payloads if p is not None]
    assert event_types.count("response.created") == 1


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_preserves_codex_events() -> None:
    """`enforce_openai_sdk_contract=False` (used by /backend-api/codex/*) MUST
    forward `codex.*` vendor events verbatim, MUST NOT backfill terminal
    output, and MUST NOT synthesize a leading `response.created`. The Codex
    CLI consumes the upstream stream natively."""
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"codex.rate_limits","plan_type":"pro","rate_limits":{"allowed":true}}\n\n',
                (
                    'data: {"type":"response.failed","sequence_number":0,'
                    '"response":{"id":"resp_err","object":"response","status":"failed","output":[],'
                    '"error":{"code":"x","message":"y"}}}\n\n'
                ),
            ),
            enforce_openai_sdk_contract=False,
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    event_types = [p["type"] for p in payloads if p is not None]
    # codex.rate_limits MUST be preserved
    assert "codex.rate_limits" in event_types
    # response.created MUST NOT be synthesized
    assert "response.created" not in event_types
    # Original sequence order preserved
    assert event_types == ["codex.rate_limits", "response.failed"]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_preserves_raw_error_frame() -> None:
    raw_error = (
        'data: {"type":"error","sequence_number":"error","error_type":"server_error",'
        '"message":"OpenCode stream failed"}\n\n'
    )
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(raw_error),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks == [raw_error, "data: [DONE]\n\n"]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_truncated_stream_does_not_synthesize_created() -> None:
    """`enforce_openai_sdk_contract=False` appends a terminal failure for
    truncated upstream streams without injecting an SDK-only created envelope."""
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks('data: {"type":"response.output_text.delta","delta":"hello"}\n\n'),
            enforce_openai_sdk_contract=False,
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    event_types = [p["type"] for p in payloads if p is not None]
    assert event_types == ["response.output_text.delta", "response.failed"]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_does_not_backfill_output() -> None:
    """`enforce_openai_sdk_contract=False` MUST NOT backfill terminal
    `response.completed.output` from streamed item events. The Codex CLI
    expects upstream's native item shape."""
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed",'
                    '"content":[{"type":"output_text","text":"hi"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":2,'
                    '"response":{"id":"resp_1","object":"response","status":"completed","output":[]}}\n\n'
                ),
            ),
            enforce_openai_sdk_contract=False,
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(b) for b in blocks]
    completed = next(p for p in payloads if p and p.get("type") == "response.completed")
    # Output stays empty — Codex CLI handles its own assembly.
    response_obj = cast(dict[str, Any], completed["response"])
    assert response_obj["output"] == []


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_appends_done_after_terminal() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.created","sequence_number":0,'
                    '"response":{"id":"resp_1","object":"response","status":"in_progress","output":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","sequence_number":1,'
                    '"response":{"id":"resp_1","object":"response","status":"completed","output":[]}}\n\n'
                ),
            ),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks[-1] == "data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_codex_route_does_not_duplicate_done() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.completed","sequence_number":1,'
                    '"response":{"id":"resp_1","object":"response","status":"completed","output":[]}}\n\n'
                ),
                "data: [DONE]\n\n",
            ),
            enforce_openai_sdk_contract=False,
        )
    ]

    assert blocks.count("data: [DONE]\n\n") == 1


# ----------------------------------------------------------------------------
# internal_bridge_responses must opt out of OpenAI SDK contract enforcement.
# (Regression guard: a forwarded /backend-api/codex/responses request that
# travels through the internal bridge MUST NOT drop codex.* events or
# synthesize a response.created envelope on the owner instance — the origin
# instance is responsible for honouring the original route's policy.)
# ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_internal_bridge_responses_disables_openai_sdk_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock

    from app.modules.proxy import http_bridge_forwarding as bridge_module

    captured: dict[str, object] = {}

    async def fake_stream_responses(*args: object, **kwargs: object) -> object:
        captured["kwargs"] = kwargs
        return object()  # any non-None response; the handler returns it directly

    monkeypatch.setattr(proxy_api_module, "_stream_responses", fake_stream_responses)

    # Bypass HMAC verification + header parsing — we only care about the flag
    # that internal_bridge_responses passes to _stream_responses.
    fake_context = bridge_module.HTTPBridgeForwardContext(
        origin_instance="origin-a",
        target_instance="owner-b",
        codex_session_affinity=True,
        downstream_turn_state="http_turn_generated",
        original_request_unanchored=True,
        original_affinity_kind="session",
        original_affinity_key="sid-abc",
        reservation=None,
        signature_version="2",
    )
    fake_forwarded = bridge_module.HTTPBridgeForwardedRequest(context=fake_context)

    def fake_parse(headers, *, payload, current_instance):
        return fake_forwarded, None

    monkeypatch.setattr(proxy_api_module, "parse_forwarded_request", fake_parse)
    # The API-key validation hits the DB by default; short-circuit it.
    monkeypatch.setattr(
        proxy_api_module,
        "_validate_internal_bridge_api_key",
        AsyncMock(return_value=(None, None)),
    )
    monkeypatch.setattr(proxy_api_module, "_strip_internal_bridge_headers", lambda h: dict(h))
    monkeypatch.setattr(proxy_api_module, "_prohibit_fast_mode_enabled", AsyncMock(return_value=False))

    # Minimal payload + request stubs.
    from app.core.openai.requests import ResponsesRequest

    payload = ResponsesRequest(model="gpt-5.5", input="hi", instructions="")

    class _StubRequest:
        @property
        def headers(self) -> dict[str, str]:
            return {"x-codex-turn-state": "http_turn_generated"}

    response = await proxy_api_module.internal_bridge_responses(
        request=cast(Any, _StubRequest()),
        payload=payload,
        context=cast(Any, object()),
    )

    assert response is not None
    kwargs_obj = captured["kwargs"]
    assert isinstance(kwargs_obj, dict)
    # cast for the type-checker — isinstance narrows at runtime, ty doesn't track it here.
    kwargs = cast(dict[str, object], kwargs_obj)
    # The regression we are guarding against: enforce_openai_sdk_contract must
    # be passed AS False so the owner instance forwards the upstream stream
    # verbatim. The origin instance reapplies normalization based on the
    # original route's policy.
    assert kwargs.get("enforce_openai_sdk_contract") is False, (
        f"internal_bridge_responses must pass enforce_openai_sdk_contract=False; got kwargs={kwargs!r}"
    )
    assert kwargs.get("forwarded_downstream_turn_state") == "http_turn_generated"
    assert kwargs.get("forwarded_original_request_unanchored") is True
    assert kwargs.get("forwarded_legacy_signature") is False
    forwarded_headers = kwargs.get("forwarded_headers")
    assert isinstance(forwarded_headers, dict)
    assert "x-codex-turn-state" not in forwarded_headers


@pytest.mark.asyncio
async def test_internal_bridge_rejects_unknown_legacy_anchor_before_terminal_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.core.clients.proxy import ProxyResponseError
    from app.core.openai.requests import ResponsesRequest
    from app.modules.proxy import http_bridge_forwarding as bridge_module

    fake_context = bridge_module.HTTPBridgeForwardContext(
        origin_instance="origin-old",
        target_instance="owner-current",
        codex_session_affinity=True,
        downstream_turn_state="http_turn_unknown",
        original_affinity_kind="session_header",
        original_affinity_key="sid-shared",
        signature_version=None,
    )
    monkeypatch.setattr(
        proxy_api_module,
        "parse_forwarded_request",
        lambda headers, *, payload, current_instance: (
            bridge_module.HTTPBridgeForwardedRequest(context=fake_context),
            None,
        ),
    )
    monkeypatch.setattr(
        proxy_api_module,
        "_validate_internal_bridge_api_key",
        AsyncMock(return_value=(None, None)),
    )
    unexpected_stream = AsyncMock(side_effect=AssertionError("streaming must not start before legacy proof"))
    monkeypatch.setattr(proxy_api_module, "_stream_responses", unexpected_stream)
    service = SimpleNamespace(
        validate_http_bridge_legacy_forward_anchor=AsyncMock(
            side_effect=ProxyResponseError(
                409,
                {
                    "error": {
                        "message": "Legacy owner forwarding requires a registered turn-state continuity anchor",
                        "type": "server_error",
                        "code": "bridge_forward_upgrade_required",
                    }
                },
            )
        )
    )
    request = cast(
        Any,
        SimpleNamespace(
            headers={"x-codex-turn-state": "http_turn_unknown"},
            method="POST",
            url=SimpleNamespace(path="/internal/responses"),
            client=None,
        ),
    )
    payload = ResponsesRequest.model_validate(
        {
            "model": "gpt-5.5",
            "instructions": "compact",
            "input": [{"role": "user", "content": "hi"}, {"type": "compaction_trigger"}],
        }
    )

    response = await proxy_api_module.internal_bridge_responses(
        request=request,
        payload=payload,
        context=cast(Any, SimpleNamespace(service=service)),
    )

    assert response.status_code == 409
    assert b'"code":"bridge_forward_upgrade_required"' in response.body
    unexpected_stream.assert_not_awaited()
    service.validate_http_bridge_legacy_forward_anchor.assert_awaited_once_with(
        original_affinity_kind="session_header",
        original_affinity_key="sid-shared",
        downstream_turn_state="http_turn_unknown",
        previous_response_id=None,
        api_key=None,
    )


@pytest.mark.asyncio
async def test_normalize_public_stream_passes_canonical_unmutated_blocks_verbatim() -> None:
    """Unmutated events that already carry canonical `event:` framing pass
    through byte-identically instead of being re-serialized."""
    created = proxy_api_module.format_sse_event(
        {"type": "response.created", "response": {"id": "resp_pt", "output": []}}
    )
    delta = proxy_api_module.format_sse_event(
        {"type": "response.output_text.delta", "item_id": "msg_1", "output_index": 0, "delta": "hi"}
    )
    completed_payload: dict[str, Any] = {
        "type": "response.completed",
        "response": {
            "id": "resp_pt",
            "output": [{"type": "message", "id": "msg_1", "content": [{"type": "output_text", "text": "hi"}]}],
        },
    }
    completed = proxy_api_module.format_sse_event(completed_payload)

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(created, delta, completed))
    ]

    assert blocks[0] == created
    assert delta in blocks


@pytest.mark.asyncio
async def test_normalize_public_stream_passes_raw_utf8_verbatim_blocks_byte_identically() -> None:
    """Upstream-verbatim delta blocks (raw UTF-8, upstream key spacing — not
    the ensure_ascii canonical re-encode) still satisfy the identity
    pass-through gate: it compares parsed-payload object identity plus the
    `event:` framing prefix, never re-serialized bytes."""
    created = proxy_api_module.format_sse_event(
        {"type": "response.created", "response": {"id": "resp_utf8", "output": []}}
    )
    verbatim_delta = (
        "event: response.output_text.delta\n"
        'data: {"type": "response.output_text.delta", "item_id": "msg_1", "output_index": 0, "delta": "안녕"}\n\n'
    )
    completed_payload: dict[str, Any] = {
        "type": "response.completed",
        "response": {
            "id": "resp_utf8",
            "output": [{"type": "message", "id": "msg_1", "content": [{"type": "output_text", "text": "안녕"}]}],
        },
    }
    completed = proxy_api_module.format_sse_event(completed_payload)

    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(created, verbatim_delta, completed)
        )
    ]

    assert verbatim_delta in blocks


@pytest.mark.asyncio
async def test_normalize_public_stream_reframes_data_only_blocks_with_event_name() -> None:
    """A data-only block (e.g. bridge-rewritten terminal event) must regain
    the canonical `event: <type>` line so named-event clients see it."""
    payload = {
        "type": "response.output_text.delta",
        "item_id": "msg_d",
        "output_index": 0,
        "delta": "x",
    }
    import json as _json

    data_only = "data: " + _json.dumps(payload, separators=(",", ":")) + "\n\n"

    created = proxy_api_module.format_sse_event(
        {"type": "response.created", "response": {"id": "resp_df", "output": []}}
    )
    blocks = [
        block async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks(created, data_only))
    ]

    reframed = [block for block in blocks if '"delta":"x"' in block]
    assert reframed
    assert reframed[0].startswith("event: response.output_text.delta\n")


class _FrozenDict(dict[str, Any]):
    """Test-only dict that fails loudly when a stream stage mutates a shared payload."""

    def _frozen(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("shared SSE payload was mutated in place")

    __setitem__ = __delitem__ = _frozen
    clear = pop = popitem = setdefault = update = _frozen


class _FrozenList(list[Any]):
    def _frozen(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("shared SSE payload list was mutated in place")

    __setitem__ = __delitem__ = __iadd__ = _frozen
    append = extend = insert = pop = remove = clear = sort = reverse = _frozen


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return _FrozenList(_deep_freeze(item) for item in value)
    return value


_CARRIER_STREAM_PAYLOADS: list[dict[str, Any]] = [
    {"type": "codex.rate_limits", "plan_type": "pro", "rate_limits": {"allowed": True}},
    {"type": "response.created", "sequence_number": 0, "response": {"id": "resp_c", "status": "in_progress"}},
    {
        "type": "response.output_item.added",
        "sequence_number": 1,
        "output_index": 0,
        "item": {"id": "rs_1", "type": "reasoning", "summary": []},
    },
    {
        "type": "response.reasoning_summary_text.delta",
        "sequence_number": 2,
        "item_id": "rs_1",
        "output_index": 0,
        "summary_index": 0,
        "delta": "\ud55c\uae00 thinking",
    },
    {
        "type": "response.reasoning_summary_text.delta",
        "sequence_number": 3,
        "item_id": "rs_1",
        "output_index": 0,
        "summary_index": 0,
        "delta": " more",
    },
    {
        "type": "response.reasoning_summary_text.done",
        "sequence_number": 4,
        "item_id": "rs_1",
        "output_index": 0,
        "summary_index": 0,
        "text": "\ud55c\uae00 thinking more",
    },
    {
        "type": "response.output_item.done",
        "sequence_number": 5,
        "output_index": 0,
        "item": {
            "id": "rs_1",
            "type": "reasoning",
            "summary": [{"type": "summary_text", "text": "\ud55c\uae00 thinking more"}],
        },
    },
    {
        "type": "response.output_item.added",
        "sequence_number": 6,
        "output_index": 1,
        "item": {"id": "msg_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []},
    },
    {
        "type": "response.output_text.delta",
        "sequence_number": 7,
        "item_id": "msg_1",
        "output_index": 1,
        "content_index": 0,
        "delta": "caf\u00e9 \u2028",
    },
    {
        "type": "response.output_text.delta",
        "sequence_number": 8,
        "item_id": "msg_1",
        "output_index": 1,
        "content_index": 0,
        "delta": " done",
    },
    {
        "type": "response.output_item.done",
        "sequence_number": 9,
        "output_index": 1,
        "item": {
            "id": "msg_1",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "caf\u00e9 \u2028 done", "annotations": []}],
        },
    },
    {
        "type": "response.completed",
        "sequence_number": 10,
        "response": {
            "id": "resp_c",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        },
    },
]


def _carrier_blocks(*, frozen: bool) -> list[str]:
    blocks: list[str] = []
    for payload in _CARRIER_STREAM_PAYLOADS:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        event_type = payload["type"]
        block = f"event: {event_type}\ndata: {text}\n\n"
        blocks.append(sse_block_with_payload(block, _deep_freeze(payload) if frozen else json.loads(text)))
    return blocks


async def _iter_prebuilt_blocks(blocks: list[str]) -> AsyncIterator[str]:
    for block in blocks:
        yield block


@pytest.mark.asyncio
@pytest.mark.parametrize("enforce_openai_sdk_contract", [True, False])
async def test_normalize_public_responses_stream_reuses_carried_payload_without_mutating_it(
    enforce_openai_sdk_contract: bool,
) -> None:
    plain_blocks = [str(block) for block in _carrier_blocks(frozen=False)]
    assert all(type(block) is str for block in plain_blocks)
    carrier_blocks = _carrier_blocks(frozen=True)

    expected = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_prebuilt_blocks(plain_blocks),
            enforce_openai_sdk_contract=enforce_openai_sdk_contract,
        )
    ]
    actual = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_prebuilt_blocks(carrier_blocks),
            enforce_openai_sdk_contract=enforce_openai_sdk_contract,
        )
    ]

    # Same bytes whether or not the payload rode along with the block, and no
    # stage mutated the shared (frozen) payload in place.
    assert actual == expected
    # Unchanged text deltas pass through as the very same carrier object.
    delta_blocks = [block for block in carrier_blocks if block.startswith("event: response.output_text.delta\n")]
    assert delta_blocks
    for delta_block in delta_blocks:
        assert any(block is delta_block for block in actual)
    # Public re-serialization retains the payload for the output-ordering gate.
    reserialized = [block for block in actual if not isinstance(block, ParsedSseBlock)]
    if enforce_openai_sdk_contract:
        assert not reserialized, "re-framed public events must not require another JSON parse"
        assert not any(block.startswith("event: codex.") for block in actual)
    assert "\ud55c\uae00" in "".join(actual)


@pytest.mark.asyncio
async def test_normalize_reasoning_summary_stream_passes_carrier_through_and_reads_its_payload() -> None:
    payload = {"type": "response.output_text.delta", "item_id": "msg_1", "delta": "plain"}
    block = sse_block_with_payload(format_sse_event(payload), _deep_freeze(payload))
    typeless_error = sse_block_with_payload(
        'data: {"error":{"code":"server_error","message":"boom"}}\n\n',
        _deep_freeze({"error": {"code": "server_error", "message": "boom"}}),
    )

    blocks = [
        emitted
        async for emitted in proxy_api_module._normalize_reasoning_summary_stream(_iter_blocks(block, typeless_error))
    ]

    assert blocks[0] is block
    assert blocks[1] is typeless_error


def test_looks_like_sse_comment_block_fast_path_matches_scan() -> None:
    blocks = [
        ": keepalive\n\n",
        ":\n: two\n\n",
        "\n\n",
        "",
        "   ",
        "data: [DONE]\n\n",
        'data: {"type":"x"}\n\n',
        'event: x\ndata: {"type":"x"}\n\n',
        "event: x\r\ndata: {}\r\n\r\n",
        "retry: 100\n\n",
        ": comment\ndata: {}\n\n",
    ]

    def scan(event_block: str) -> bool:
        return bool(event_block.strip()) and all(
            not line.strip() or line.lstrip().startswith(":") for line in event_block.splitlines()
        )

    for event_block in blocks:
        assert proxy_api_module._looks_like_sse_comment_block(event_block) is scan(event_block), repr(event_block)
