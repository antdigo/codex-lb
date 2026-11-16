from __future__ import annotations

import errno
from collections.abc import AsyncGenerator, AsyncIterator
from copy import deepcopy
from types import MethodType, SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.clients import proxy as core_proxy
from app.core.errors import openai_error, synthetic_stream_failure_event
from app.core.openai.models import OpenAIError
from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonValue
from app.core.upstream_proxy import UpstreamProxyRouteError
from app.core.utils.request_id import reset_request_id, set_request_id
from app.core.utils.sse import ParsedSseBlock, format_local_sse_event, format_sse_event, parse_sse_data_json
from app.db.models import Account
from app.modules.api_keys.service import ApiKeyUsageReservationData
from app.modules.proxy import api as proxy_api
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service.streaming.helpers import _record_or_defer_owner_recovery_health


def _stream_owner() -> SimpleNamespace:
    # Only external admission, persistence, and credential boundaries are fake.
    owner = SimpleNamespace(
        _encryptor=SimpleNamespace(decrypt=lambda _value: "offline-fixture"),
        _resolve_upstream_route_for_account=AsyncMock(return_value=None),
        _acquire_account_response_create_lease_or_overload=AsyncMock(return_value=None),
        _get_work_admission=lambda: SimpleNamespace(acquire_response_create=AsyncMock(return_value=MagicMock())),
        _load_balancer=SimpleNamespace(release_account_lease=AsyncMock()),
        _maybe_touch_api_key_reservation=AsyncMock(return_value=0.0),
        _websocket_previous_response_account_index={("resp_foreign", None, None): "account_other"},
        _write_request_log=AsyncMock(),
        _handle_stream_error=AsyncMock(),
    )
    owner._remember_websocket_previous_response_owner = MethodType(
        proxy_service.ProxyService._remember_websocket_previous_response_owner, owner
    )
    return owner


async def _blocks(source: list[str]) -> AsyncIterator[str]:
    for block in source:
        yield block


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_kind", "expected_code"),
    [
        ("local_transport", "stream_idle_timeout"),
        ("upstream_unowned_error", "rate_limit_exceeded"),
        ("service_exception", "stream_incomplete"),
        ("route_failure", "upstream_proxy_unavailable"),
        ("rewritten_error", "stream_incomplete"),
        ("rewritten_exception", "stream_incomplete"),
        ("foreign_progress", "upstream_response_owner_mismatch"),
        ("foreign_terminal", "upstream_response_owner_mismatch"),
    ],
)
async def test_classic_failure_preserves_accepted_owner_and_cause(
    monkeypatch: pytest.MonkeyPatch, failure_kind: str, expected_code: str
) -> None:
    original_blocks: list[str] = []
    original_payloads: list[dict[str, JsonValue] | None] = []

    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        yield format_sse_event(
            {"type": "response.created", "response": {"id": "resp_accepted", "status": "in_progress", "output": []}}
        )
        if failure_kind.startswith("foreign_"):
            yield format_sse_event(
                {
                    "type": "response.in_progress" if failure_kind == "foreign_progress" else "response.failed",
                    "response": {"id": "resp_foreign", "status": "in_progress", "output": []},
                }
            )
            block = format_local_sse_event(
                synthetic_stream_failure_event("stream_idle_timeout", "Original idle timeout", response_id="req_local")
            )
        elif failure_kind == "local_transport":
            block = format_local_sse_event(
                synthetic_stream_failure_event(expected_code, "Original idle timeout", response_id="req_local")
            )
        elif failure_kind in {"upstream_unowned_error", "rewritten_error"}:
            raw: dict[str, JsonValue] = {
                "type": "error",
                "error": {
                    "type": "invalid_request_error" if failure_kind == "rewritten_error" else "rate_limit_error",
                    "code": "previous_response_not_found" if failure_kind == "rewritten_error" else expected_code,
                    "message": (
                        "Previous response not found" if failure_kind == "rewritten_error" else "Original rejection"
                    ),
                    "param": "previous_response_id" if failure_kind == "rewritten_error" else "input",
                },
            }
            block = core_proxy._format_normalized_stream_event(raw, core_proxy._normalize_stream_event_payload(raw))
        elif failure_kind == "route_failure":
            raise UpstreamProxyRouteError("pool_unavailable")
        elif failure_kind == "rewritten_exception":
            error = openai_error(
                "previous_response_not_found", "Previous response not found", error_type="invalid_request_error"
            )
            error["error"]["param"] = "previous_response_id"
            raise core_proxy.ProxyResponseError(
                400,
                error,
            )
        else:
            raise RuntimeError("Offline service exception")
        original_blocks.append(block)
        original_payloads.append(deepcopy(parse_sse_data_json(block)))
        yield block

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    owner = _stream_owner()
    account = Account(
        id="account_offline",
        access_token_encrypted=b"offline",
        chatgpt_account_id="account_offline",
        codex_installation_id="installation_offline",
    )
    request = ResponsesRequest.model_validate(
        {
            "model": "gpt-5",
            "instructions": "",
            "input": [],
            "stream": True,
            "previous_response_id": "resp_parent" if failure_kind.startswith("rewritten_") else None,
        }
    )
    settlement = proxy_service._StreamSettlement()
    token = set_request_id("req_local")
    try:
        relayed = [
            block
            async for block in proxy_service.ProxyService._stream_once(
                cast(Any, owner),
                account,
                request,
                {},
                "req_local",
                False,
                request_started_at=0.0,
                api_key=None,
                api_key_reservation=None,
                settlement=settlement,
                suppress_text_done_events=False,
                upstream_stream_transport="http",
                request_transport="http",
            )
        ]
        wrapped = proxy_api._stream_response_error_events(_blocks(relayed), owns_reservation=False, reservation=None)
        public = [parse_sse_data_json(block) async for block in proxy_api._normalize_public_responses_stream(wrapped)]
    finally:
        reset_request_id(token)

    terminal = public[-1]
    assert terminal is not None and terminal["type"] == "response.failed"
    response = terminal["response"]
    assert isinstance(response, dict) and response["id"] == "resp_accepted"
    error = response["error"]
    assert isinstance(error, dict) and error["code"] == expected_code
    if failure_kind.startswith("rewritten_"):
        assert error["message"] == "Upstream websocket closed before response.completed"
    assert settlement.response_id == "resp_accepted"
    assert settlement.error_code == expected_code
    assert owner._write_request_log.await_args.kwargs["request_id"] == "resp_accepted"
    assert owner._websocket_previous_response_account_index == {
        ("resp_foreign", None, None): "account_other",
        ("resp_accepted", None, None): "account_offline",
    }
    assert [parse_sse_data_json(block) for block in original_blocks] == original_payloads


@pytest.mark.asyncio
@pytest.mark.parametrize("is_local", [True, False])
async def test_classic_first_failure_keeps_local_id_provenance(monkeypatch: pytest.MonkeyPatch, is_local: bool) -> None:
    payload = cast(
        dict[str, JsonValue],
        synthetic_stream_failure_event("stream_idle_timeout", "Original timeout", response_id="req_local"),
    )
    original = ParsedSseBlock(format_sse_event(payload), payload, is_local=is_local, response_id_is_local=True)

    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        yield original

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    settlement = proxy_service._StreamSettlement()
    account = Account(
        id="account_offline",
        access_token_encrypted=b"offline",
        chatgpt_account_id="account_offline",
        codex_installation_id="installation_offline",
    )
    relayed: list[str] = []
    with pytest.raises(proxy_service._TerminalStreamError):
        async for block in proxy_service.ProxyService._stream_once(
            cast(Any, _stream_owner()),
            account,
            ResponsesRequest(model="gpt-5", instructions="", input=[], stream=True),
            {},
            "req_local",
            False,
            request_started_at=0.0,
            api_key=None,
            api_key_reservation=None,
            settlement=settlement,
            suppress_text_done_events=False,
            upstream_stream_transport="http",
            request_transport="http",
        ):
            relayed.append(block)

    assert isinstance(relayed[0], ParsedSseBlock)
    assert relayed[0].is_local is is_local
    assert relayed[0].response_id_is_local is True
    assert parse_sse_data_json(relayed[0]) is payload
    assert settlement.response_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["error", "response.failed"])
@pytest.mark.parametrize(
    ("nested_id", "top_id"),
    [
        (None, None),
        ("resp_foreign", None),
        (None, "resp_foreign"),
        ("resp_accepted", "resp_foreign"),
        ("resp_foreign", "resp_accepted"),
        ("resp_accepted", None),
        (None, "resp_accepted"),
    ],
)
async def test_core_error_normalization_preserves_explicit_upstream_owners(
    event_type: str, nested_id: str | None, top_id: str | None
) -> None:
    raw: dict[str, JsonValue] = {
        "type": event_type,
        "error": {"type": "server_error", "code": "original_error", "message": "Original diagnostic"},
    }
    if nested_id is not None:
        raw["response"] = {"id": nested_id, "status": "failed", "output": []}
    if top_id is not None:
        raw["response_id"] = top_id
    original = deepcopy(raw)
    token = set_request_id("req_local")
    try:
        block, _ = core_proxy._normalize_stream_payload_for_http_block(format_sse_event(raw))
        public = [
            parse_sse_data_json(item)
            async for item in proxy_api._normalize_public_responses_stream(
                _blocks(
                    [
                        format_sse_event({"type": "response.created", "response": {"id": "resp_accepted"}}),
                        block,
                    ]
                )
            )
        ]
    finally:
        reset_request_id(token)

    terminal = public[-1]
    assert terminal is not None
    response = terminal["response"]
    assert isinstance(response, dict) and response["id"] == "resp_accepted"
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == (
        "upstream_response_owner_mismatch" if "resp_foreign" in (nested_id, top_id) else "original_error"
    )
    normalized = parse_sse_data_json(block)
    assert normalized is not None
    normalized_response = normalized["response"]
    assert isinstance(normalized_response, dict)
    assert normalized_response["id"] == (nested_id or top_id or "req_local")
    if top_id is not None:
        assert normalized["response_id"] == top_id
    assert isinstance(block, ParsedSseBlock)
    assert block.response_id_is_local is (nested_id is None and top_id is None)
    assert raw == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "foreign_kind",
    [
        "completed",
        "previous_response_error",
        "raw_error",
        "exception_previous_response_error",
        "conflicting_ids",
        "delta_after_ttft",
        "escaped_delta_after_ttft",
    ],
)
async def test_classic_fences_foreign_frames_before_rewrites_and_settlement(
    monkeypatch: pytest.MonkeyPatch, foreign_kind: str
) -> None:
    upstream_closed: list[bool] = []

    async def upstream_events() -> AsyncIterator[str]:
        yield format_sse_event(
            {
                "type": "response.created",
                "response": {"id": "resp_accepted", "status": "in_progress", "service_tier": "default", "output": []},
            }
        )
        if foreign_kind in {"delta_after_ttft", "escaped_delta_after_ttft"}:
            yield format_sse_event({"type": "response.output_text.delta", "item_id": "msg_owned", "delta": "Owned"})
            foreign_delta = format_sse_event(
                {"type": "response.output_text.delta", "response_id": "resp_foreign", "delta": "Foreign"}
            )
            if foreign_kind == "escaped_delta_after_ttft":
                foreign_delta = foreign_delta.replace('"response_id"', '"response_\\u0069d"')
            yield foreign_delta
            return
        foreign: dict[str, JsonValue] = {
            "type": "response.completed" if foreign_kind == "completed" else "response.failed",
            "response": {
                "id": "resp_foreign",
                "status": "completed" if foreign_kind == "completed" else "failed",
                "service_tier": "priority",
                "output": [],
                "usage": {"input_tokens": 1000, "output_tokens": 2000, "total_tokens": 3000},
                "error": {
                    "type": "invalid_request_error",
                    "code": "previous_response_not_found",
                    "message": "Previous response not found",
                    "param": "previous_response_id",
                },
            },
        }
        if foreign_kind == "conflicting_ids":
            foreign["response"] = {"id": "resp_accepted", "service_tier": "priority"}
            foreign["response_id"] = "resp_foreign"
        elif foreign_kind == "raw_error":
            foreign = {
                "type": "error",
                "response_id": "resp_foreign",
                "error": {
                    "type": "invalid_request_error",
                    "code": "previous_response_not_found",
                    "message": "Previous response not found",
                    "param": "previous_response_id",
                },
            }
        elif foreign_kind == "exception_previous_response_error":
            raise core_proxy.ProxyResponseError(
                400,
                cast(
                    Any,
                    {
                        "response": {"id": "resp_foreign"},
                        "error": {
                            "type": "invalid_request_error",
                            "code": "previous_response_not_found",
                            "message": "Previous response not found",
                            "param": "previous_response_id",
                        },
                    },
                ),
            )
        yield format_sse_event(foreign)

    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        try:
            async for block in upstream_events():
                yield block
        finally:
            upstream_closed.append(True)

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    owner = _stream_owner()
    settlement = proxy_service._StreamSettlement()
    stream = proxy_service.ProxyService._stream_once(
        cast(Any, owner),
        Account(id="account_offline", access_token_encrypted=b"offline", chatgpt_account_id="account_offline"),
        ResponsesRequest(model="gpt-5", instructions="", input=[], stream=True, previous_response_id="resp_parent"),
        {},
        "req_local",
        False,
        request_started_at=0.0,
        api_key=None,
        api_key_reservation=None,
        settlement=settlement,
        suppress_text_done_events=False,
        upstream_stream_transport="http",
        request_transport="http",
    )
    normalized = proxy_api._normalize_public_responses_stream(stream)
    try:
        public = [parse_sse_data_json(block) async for block in normalized]
    finally:
        await cast(AsyncGenerator[str, None], normalized).aclose()
        await cast(AsyncGenerator[str, None], stream).aclose()

    terminal = public[-1]
    assert terminal is not None
    response = terminal["response"]
    assert isinstance(response, dict) and response["id"] == "resp_accepted"
    error = response["error"]
    assert isinstance(error, dict) and error["code"] == "upstream_response_owner_mismatch"
    assert settlement.response_id == "resp_accepted"
    assert settlement.status == "error"
    assert settlement.error_code == "upstream_response_owner_mismatch"
    assert settlement.record_success is False
    assert settlement.account_health_error is False
    assert settlement.input_tokens is None and settlement.output_tokens is None
    assert settlement.service_tier == "default"
    log = owner._write_request_log.await_args.kwargs
    assert log["request_id"] == "resp_accepted" and log["status"] == "error"
    assert log["input_tokens"] is None and log["output_tokens"] is None
    assert log["service_tier"] == "default"
    assert owner._websocket_previous_response_account_index[("resp_foreign", None, None)] == "account_other"
    owner._handle_stream_error.assert_not_awaited()
    assert upstream_closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("vendor_position", [None, "before_created", "after_created"])
async def test_classic_new_attempt_accepts_owner_when_settlement_was_reused(
    monkeypatch: pytest.MonkeyPatch, vendor_position: str | None
) -> None:
    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        vendor = format_sse_event(
            {
                "type": "codex.rate_limits",
                "response_id": "diagnostic_other",
                "response": {"id": "diagnostic_other"},
            }
        )
        if vendor_position == "before_created":
            yield vendor
        yield format_sse_event({"type": "response.created", "response": {"id": "resp_new_attempt"}})
        if vendor_position == "after_created":
            yield vendor
        yield format_sse_event(
            {
                "type": "response.completed",
                "response": {"id": "resp_new_attempt", "usage": {"input_tokens": 1, "output_tokens": 2}},
            }
        )

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    settlement = proxy_service._StreamSettlement(response_id="resp_old_attempt")
    owner = _stream_owner()
    stream = proxy_service.ProxyService._stream_once(
        cast(Any, owner),
        Account(id="account_offline", access_token_encrypted=b"offline", chatgpt_account_id="account_offline"),
        ResponsesRequest(model="gpt-5", instructions="", input=[], stream=True),
        {},
        "req_local",
        False,
        request_started_at=0.0,
        api_key=None,
        api_key_reservation=None,
        settlement=settlement,
        suppress_text_done_events=False,
        upstream_stream_transport="http",
        request_transport="http",
    )
    normalized = proxy_api._normalize_public_responses_stream(stream)
    try:
        public = [parse_sse_data_json(block) async for block in normalized]
    finally:
        await cast(AsyncGenerator[str, None], normalized).aclose()
        await cast(AsyncGenerator[str, None], stream).aclose()
    assert len(public) == 2
    assert public[-1] is not None and public[-1]["type"] == "response.completed"
    assert settlement.response_id == "resp_new_attempt"
    assert settlement.status == "success"
    assert settlement.input_tokens == 1 and settlement.output_tokens == 2
    assert ("diagnostic_other", None, None) not in owner._websocket_previous_response_account_index


@pytest.mark.asyncio
@pytest.mark.parametrize("late_type", ["response.failed", "response.completed"])
async def test_classic_preserves_accepted_completion_when_foreign_terminal_arrives_later(
    monkeypatch: pytest.MonkeyPatch, late_type: str
) -> None:
    late_vendor_processed: list[bool] = []

    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        yield format_sse_event({"type": "response.created", "response": {"id": "resp_accepted"}})
        yield format_sse_event(
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_accepted",
                    "status": "completed",
                    "output": [],
                    "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
                },
            }
        )
        yield format_sse_event(
            {
                "type": late_type,
                "response": {
                    "id": "resp_foreign",
                    "status": "failed" if late_type == "response.failed" else "completed",
                    "output": [],
                    "usage": {"input_tokens": 1000, "output_tokens": 2000, "total_tokens": 3000},
                    "error": {"type": "server_error", "code": "foreign_error", "message": "Foreign diagnostic"},
                },
            }
        )
        yield format_sse_event({"type": "codex.rate_limits", "response_id": "diagnostic_other"})
        late_vendor_processed.append(True)

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    owner = _stream_owner()
    settlement = proxy_service._StreamSettlement()
    stream = proxy_service.ProxyService._stream_once(
        cast(Any, owner),
        Account(id="account_offline", access_token_encrypted=b"offline", chatgpt_account_id="account_offline"),
        ResponsesRequest(model="gpt-5", instructions="", input=[], stream=True),
        {},
        "req_local",
        False,
        request_started_at=0.0,
        api_key=None,
        api_key_reservation=None,
        settlement=settlement,
        suppress_text_done_events=False,
        upstream_stream_transport="http",
        request_transport="http",
    )
    normalized = proxy_api._normalize_public_responses_stream(stream)
    try:
        public = [parse_sse_data_json(block) async for block in normalized]
    finally:
        await cast(AsyncGenerator[str, None], normalized).aclose()
        await cast(AsyncGenerator[str, None], stream).aclose()

    terminal = public[-1]
    assert terminal is not None and terminal["type"] == "response.completed"
    response = terminal["response"]
    assert isinstance(response, dict) and response["id"] == "resp_accepted"
    assert settlement.response_id == "resp_accepted"
    assert settlement.status == "success" and settlement.record_success is True
    assert settlement.error_code is None
    assert settlement.input_tokens == 1 and settlement.output_tokens == 2
    log = owner._write_request_log.await_args.kwargs
    assert log["request_id"] == "resp_accepted" and log["status"] == "success"
    assert log["input_tokens"] == 1 and log["output_tokens"] == 2
    assert owner._websocket_previous_response_account_index[("resp_foreign", None, None)] == "account_other"
    owner._handle_stream_error.assert_not_awaited()
    assert late_vendor_processed == [True]


def _late_exception_classic_stream(
    owner: SimpleNamespace,
    settlement: proxy_service._StreamSettlement,
    *,
    native: bool = False,
    allow_retry: bool = False,
) -> AsyncIterator[str]:
    return proxy_service.ProxyService._stream_once(
        cast(Any, owner),
        Account(id="account_offline", access_token_encrypted=b"offline", chatgpt_account_id="account_offline"),
        ResponsesRequest(model="gpt-5", instructions="", input=[], stream=True),
        {"originator": "codex_exec"} if native else {},
        "req_local",
        allow_retry,
        request_started_at=0.0,
        api_key=None,
        api_key_reservation=None,
        settlement=settlement,
        suppress_text_done_events=False,
        upstream_stream_transport="http",
        request_transport="http",
        enforce_openai_sdk_contract=not native,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_type", ["response.completed", "response.failed", "response.incomplete"])
@pytest.mark.parametrize("exception_kind", ["runtime", "proxy", "route", "core_cleanup"])
async def test_classic_late_exception_preserves_accepted_terminal(
    monkeypatch: pytest.MonkeyPatch, terminal_type: str, exception_kind: str
) -> None:
    response: dict[str, JsonValue] = {"id": "resp_accepted", "output": []}
    if terminal_type == "response.failed":
        response.update(
            status="failed", error={"type": "server_error", "code": "original_failure", "message": "Original failure"}
        )
    else:
        response.update(
            status="completed" if terminal_type == "response.completed" else "incomplete",
            usage={"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        )

    frames: list[dict[str, JsonValue]] = [
        {"type": "response.created", "response": {"id": "resp_accepted"}},
        {"type": terminal_type, "response": response},
    ]

    class OfflineContent:
        async def iter_chunked(self, _size: int) -> AsyncIterator[bytes]:
            for frame in frames:
                yield format_sse_event(frame).encode()

    class OfflineResponse:
        status = 200
        headers = {"content-type": "text/event-stream"}
        content = OfflineContent()

        async def __aenter__(self) -> OfflineResponse:
            return self

        async def __aexit__(self, *_args: Any) -> None:
            raise OSError(errno.ENETUNREACH, "Offline transport cleanup failure")

    class OfflineSession:
        def post(self, *_args: Any, **_kwargs: Any) -> OfflineResponse:
            return OfflineResponse()

    async def upstream(*args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        if exception_kind == "core_cleanup":
            async for block in core_proxy._stream_responses_with_session(
                args[0],
                args[1],
                args[2],
                args[3],
                cast(Any, OfflineSession()),
                base_url="https://offline.invalid",
                raise_for_status=True,
                upstream_stream_transport_override="http",
                suppress_live_usage=True,
            ):
                yield block
            return
        for frame in frames:
            yield format_sse_event(frame)
        if exception_kind == "proxy":
            raise core_proxy.ProxyResponseError(502, openai_error("proxy_network_unavailable", "Late proxy failure"))
        if exception_kind == "route":
            raise UpstreamProxyRouteError("late_route_failure")
        raise RuntimeError("Late cleanup failure")

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    monkeypatch.setattr(core_proxy, "discover_native_egress_client", lambda: None)
    owner = _stream_owner()
    settlement = proxy_service._StreamSettlement()
    stream = _late_exception_classic_stream(owner, settlement)
    wrapped = proxy_api._stream_response_error_events(stream, owns_reservation=False, reservation=None)
    normalized = proxy_api._normalize_public_responses_stream(wrapped)
    try:
        public = [parse_sse_data_json(block) async for block in normalized]
    finally:
        await cast(AsyncGenerator[str, None], normalized).aclose()
        await cast(AsyncGenerator[str, None], wrapped).aclose()
        await cast(AsyncGenerator[str, None], stream).aclose()
    terminal = public[-1]
    assert terminal is not None and terminal["type"] == terminal_type
    expected_status = "success" if terminal_type == "response.completed" else "error"
    expected_code = "original_failure" if terminal_type == "response.failed" else None
    assert settlement.response_id == "resp_accepted"
    assert settlement.status == expected_status
    assert settlement.record_success is (terminal_type != "response.failed")
    assert settlement.error_code == expected_code
    assert settlement.input_tokens == (None if terminal_type == "response.failed" else 1)
    assert settlement.output_tokens == (None if terminal_type == "response.failed" else 2)
    log = owner._write_request_log.await_args.kwargs
    assert log["request_id"] == "resp_accepted" and log["status"] == expected_status
    assert log["error_code"] == expected_code
    assert log["input_tokens"] == (None if terminal_type == "response.failed" else 1)
    assert log["output_tokens"] == (None if terminal_type == "response.failed" else 2)
    owner._handle_stream_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_classic_native_late_exception_keeps_transport_failure_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        yield format_sse_event({"type": "response.created", "response": {"id": "resp_accepted"}})
        yield format_sse_event({"type": "response.completed", "response": {"id": "resp_accepted"}})
        raise RuntimeError("Native late cleanup failure")

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    settlement = proxy_service._StreamSettlement()
    with pytest.raises(core_proxy.ProxyResponseError) as exc_info:
        _ = [block async for block in _late_exception_classic_stream(_stream_owner(), settlement, native=True)]
    assert exc_info.value.payload["error"]["code"] == "stream_incomplete"
    assert settlement.status == "error" and settlement.record_success is False


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_retry", [False, True])
async def test_classic_first_failed_terminal_keeps_terminal_and_retry_control_flow(
    monkeypatch: pytest.MonkeyPatch, allow_retry: bool
) -> None:
    async def upstream(*_args: Any, **_kwargs: Any) -> AsyncIterator[str]:
        yield format_local_sse_event(
            synthetic_stream_failure_event("stream_idle_timeout", "Original timeout", response_id="req_local")
        )

    monkeypatch.setattr(proxy_service, "core_stream_responses", upstream)
    settlement = proxy_service._StreamSettlement()
    expected_exception = proxy_service._RetryableStreamError if allow_retry else proxy_service._TerminalStreamError
    with pytest.raises(expected_exception):
        _ = [
            block
            async for block in _late_exception_classic_stream(_stream_owner(), settlement, allow_retry=allow_retry)
        ]
    assert settlement.status == "error"
    assert settlement.error_code == "stream_idle_timeout"


@pytest.mark.asyncio
@pytest.mark.parametrize("reserved", [False, True])
async def test_classic_owner_health_error_model_preserves_settlement_order(reserved: bool) -> None:
    owner = _stream_owner()
    account = Account(id="account_offline")
    settlement = proxy_service._StreamSettlement()
    reservation = ApiKeyUsageReservationData("reservation", "key", "gpt-5") if reserved else None
    await _record_or_defer_owner_recovery_health(
        cast(Any, owner),
        account,
        settlement,
        reservation,
        OpenAIError(message="Usage exhausted", resets_at=123),
        "usage_limit_reached",
    )
    if reserved:
        assert settlement.error_code == "usage_limit_reached"
        assert settlement.account_health_error is True and settlement.settlement_order_required is True
        owner._handle_stream_error.assert_not_awaited()
    else:
        owner._handle_stream_error.assert_awaited_once_with(
            account, {"message": "Usage exhausted", "resets_at": 123}, "usage_limit_reached"
        )
        assert settlement.settlement_order_required is False
