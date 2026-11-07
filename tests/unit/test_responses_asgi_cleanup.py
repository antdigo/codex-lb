"""The Responses ASGI writer owns service cleanup, even before body iteration."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from starlette.requests import ClientDisconnect, Request

from app.core.clients import proxy as core_proxy
from app.core.openai.requests import ResponsesRequest
from app.core.utils.sse import format_sse_event
from app.db.models import Account
from app.modules.api_keys.service import ApiKeyUsageReservationData
from app.modules.proxy import api
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service import support
from tests.unit.test_proxy_streaming_failure_provenance import _stream_owner

pytestmark = pytest.mark.unit


@dataclass
class _ServiceOwner:
    wait_before_created: bool = False
    terminal: str | None = None
    handoff: str | None = None
    first_ready: asyncio.Event = field(default_factory=asyncio.Event)
    reading: asyncio.Event = field(default_factory=asyncio.Event)
    close_started: asyncio.Event = field(default_factory=asyncio.Event)
    close_ready: asyncio.Event = field(default_factory=asyncio.Event)
    closed: bool = False
    settlements: int = 0
    reader: asyncio.Task[Any] | None = None
    blocks: list[str] = field(default_factory=list)

    async def stream(self) -> AsyncIterator[str]:
        try:
            self.reader = asyncio.current_task()
            if self.handoff == "service":
                support._signal_propagated_responses_service_cleanup_ready()
            elif self.handoff in {"forwarded", "rejected"}:
                support._signal_propagated_responses_owner_forward_dispatched()
                if self.handoff == "rejected":
                    support._signal_propagated_responses_owner_forward_rejected()
            if self.wait_before_created:
                self.reading.set()
                await self.first_ready.wait()
            created = format_sse_event(
                {"type": "response.created", "response": {"id": "resp_owned", "status": "in_progress", "output": []}}
            )
            self.blocks.append(created)
            yield created
            self.reader = asyncio.current_task()
            if self.terminal is None:
                self.reading.set()
                await asyncio.Event().wait()
            terminal = format_sse_event(
                {
                    "type": self.terminal,
                    "response": {
                        "id": "resp_owned",
                        "status": "completed" if self.terminal == "response.completed" else "failed",
                        "output": [],
                        "error": {"code": "owned_failure", "message": "owned failure"}
                        if self.terminal == "response.failed"
                        else None,
                    },
                }
            )
            self.blocks.append(terminal)
            yield terminal
        finally:
            self.close_started.set()
            await self.close_ready.wait()
            self.settlements += 1
            self.closed = True


async def _response(monkeypatch: pytest.MonkeyPatch, owner: _ServiceOwner, *, native: bool = False):
    raw = owner.stream()
    released: list[str] = []
    reservation = ApiKeyUsageReservationData(reservation_id="reservation", key_id="key", model="gpt-5")

    async def release(value, **_kwargs):
        assert value is reservation
        released.append(value.reservation_id)

    monkeypatch.setattr(api, "_http_bridge_active_for_request", AsyncMock(return_value=True))
    monkeypatch.setattr(api, "_opportunistic_admission_denial", AsyncMock(return_value=None))
    monkeypatch.setattr(api, "_enforce_request_limits", AsyncMock(return_value=reservation))
    monkeypatch.setattr(api, "_release_reservation_best_effort", release)
    settings = api.get_settings().model_copy(update={"sse_keepalive_interval_seconds": 0.005})
    monkeypatch.setattr(api, "get_settings", lambda: settings)
    monkeypatch.setattr(api, "with_dashboard_overrides", lambda value: value)
    monkeypatch.setattr(api, "_HTTP_BRIDGE_STARTUP_ERROR_PROBE_SECONDS", 0.001)
    monkeypatch.setattr(api, "_CAPACITY_STARTUP_SIGNAL_DISCOVERY_SECONDS", 0.001)
    headers = [(b"user-agent", b"codex_cli_rs/0.100.0")] if native else []
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/responses",
        "headers": headers,
        "asgi": {"version": "3.0", "spec_version": "2.4"},
    }
    context = SimpleNamespace(service=SimpleNamespace(stream_http_responses=lambda *_a, **_kw: raw))
    response = await api._stream_responses(
        Request(scope),
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, context),
        None,
        prefer_http_bridge=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
        enforce_openai_sdk_contract=not native,
    )
    assert isinstance(response, api.DeliveryTracedStreamingResponse)
    return response, raw, scope, released


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("target", ["start", "created", "keepalive"])
async def test_responses_asgi_disconnect_closes_service_and_keepalive_owner(monkeypatch, spec_version, target):
    owner = _ServiceOwner()
    owner.close_ready.set()
    response, raw, scope, released = await _response(monkeypatch, owner)
    scope["asgi"]["spec_version"] = spec_version
    send_started = asyncio.Event()

    async def send(message):
        body = message.get("body", b"")
        if (
            (target == "start" and message["type"] == "http.response.start")
            or (target == "created" and b"response.created" in body)
            or (target == "keepalive" and b"keepalive" in body)
        ):
            send_started.set()
            if spec_version == "2.4":
                raise OSError("peer disconnected during write")
            await asyncio.Event().wait()

    async def receive():
        await send_started.wait()
        return {"type": "http.disconnect"}

    try:
        if spec_version == "2.4":
            with pytest.raises(ClientDisconnect):
                await response(scope, receive, send)
        else:
            await response(scope, receive, send)
        assert send_started.is_set()
        assert owner.closed, "ASGI exit must close service ownership without GC or explicit body close"
        assert owner.settlements == 1
        assert released == ["reservation"]
        assert owner.reader is not None and owner.reader.done(), "Keepalive/probe reads must be joined"
        assert response.outcome == (
            "exception_before_terminal" if spec_version == "2.4" else "cancelled_before_terminal"
        )
    finally:
        await response.body_iterator.aclose()
        await raw.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("probe_completed", [False, True])
async def test_response_header_failure_joins_unconsumed_startup_probe(monkeypatch, spec_version, probe_completed):
    owner = _ServiceOwner(wait_before_created=True)
    owner.close_ready.set()
    response, raw, scope, released = await _response(monkeypatch, owner)
    scope["asgi"]["spec_version"] = spec_version
    assert owner.reading.is_set() and owner.reader is not None
    if probe_completed:
        owner.first_ready.set()
        await asyncio.wait_for(asyncio.shield(owner.reader), 1)
    send_started = asyncio.Event()

    async def send(_message):
        send_started.set()
        if spec_version == "2.4":
            raise OSError("headers could not be sent")
        await asyncio.Event().wait()

    async def receive():
        await send_started.wait()
        return {"type": "http.disconnect"}

    try:
        if spec_version == "2.4":
            with pytest.raises(ClientDisconnect):
                await response(scope, receive, send)
        else:
            await response(scope, receive, send)
        assert owner.reader.done(), "An unstarted delivery iterator must not retain its startup probe"
        assert owner.closed and owner.settlements == 1
        assert released == ["reservation"]
    finally:
        if not owner.reader.done():
            owner.reader.cancel()
            await asyncio.gather(owner.reader, return_exceptions=True)
        await response.body_iterator.aclose()
        await raw.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("terminal", ["response.completed", "response.failed"])
async def test_responses_normal_asgi_delivery_settles_once(monkeypatch, native, terminal):
    owner = _ServiceOwner(terminal=terminal)
    owner.close_ready.set()
    response, raw, scope, released = await _response(monkeypatch, owner, native=native)
    sent: list[bytes] = []

    async def send(message):
        if message["type"] == "http.response.body":
            sent.append(message["body"])

    async def receive():
        await asyncio.Event().wait()

    try:
        await response(scope, receive, send)
        assert owner.closed and owner.settlements == 1
        assert released == ["reservation"]
        assert response.outcome == "terminal_written"
        assert sent[-1] == b""
        assert terminal.encode() in b"".join(sent)
        if native:
            assert sent[:-1] == [*[block.encode() for block in owner.blocks], b"data: [DONE]\n\n"]
    finally:
        await response.body_iterator.aclose()
        await raw.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_server_cancellation_waits_for_service_cleanup_despite_repeated_cancel(monkeypatch, spec_version):
    owner = _ServiceOwner()
    response, raw, scope, released = await _response(monkeypatch, owner)
    scope["asgi"]["spec_version"] = spec_version
    send_started = asyncio.Event()

    async def send(message):
        if b"keepalive" in message.get("body", b""):
            send_started.set()
            await asyncio.Event().wait()

    async def receive():
        await asyncio.Event().wait()

    task = asyncio.create_task(response(scope, receive, send))
    try:
        await asyncio.wait_for(send_started.wait(), 1)
        task.cancel()
        await asyncio.wait_for(owner.close_started.wait(), 0.2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "The writer must join owned cleanup even after a repeated server cancel"
        owner.close_ready.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owner.closed and owner.settlements == 1
        assert released == ["reservation"]
        assert owner.reader is not None and owner.reader.done()
        assert response.outcome == "cancelled_before_terminal"
    finally:
        owner.close_ready.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await response.body_iterator.aclose()
        await raw.aclose()


@pytest.mark.asyncio
async def test_simultaneous_iterator_and_writer_close_join_one_settlement(monkeypatch):
    owner = _ServiceOwner()
    response, raw, scope, released = await _response(monkeypatch, owner)
    sending = asyncio.Event()

    async def send(message):
        if b"response.created" in message.get("body", b""):
            sending.set()
            await asyncio.Event().wait()

    async def receive():
        await asyncio.Event().wait()

    writer = asyncio.create_task(response(scope, receive, send))
    closing = None
    try:
        await asyncio.wait_for(sending.wait(), 1)
        closing = asyncio.create_task(response.body_iterator.aclose())
        await asyncio.wait_for(owner.close_started.wait(), 1)
        writer.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not writer.done(), "ASGI close must join cleanup already owned by the iterator"
        owner.close_ready.set()
        await closing
        with pytest.raises(asyncio.CancelledError):
            await writer
        assert owner.closed and owner.settlements == 1
        assert released == ["reservation"]
    finally:
        owner.close_ready.set()
        writer.cancel()
        await asyncio.gather(writer, *(tasks for tasks in [closing] if tasks is not None), return_exceptions=True)
        await response.body_iterator.aclose()
        await raw.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("handoff", ["service", "forwarded", "rejected"])
async def test_header_failure_respects_explicit_reservation_handoff(monkeypatch, handoff):
    owner = _ServiceOwner(handoff=handoff)
    owner.close_ready.set()
    response, raw, scope, released = await _response(monkeypatch, owner)

    async def send(_message):
        raise OSError("headers could not be sent")

    async def receive():
        await asyncio.Event().wait()

    try:
        with pytest.raises(ClientDisconnect):
            await response(scope, receive, send)
        assert owner.closed and owner.settlements == 1
        assert released == (["reservation"] if handoff == "rejected" else [])
    finally:
        await response.body_iterator.aclose()
        await raw.aclose()


@dataclass
class _ClassicHTTPResponse:
    terminal: bool = False
    close_error: Exception | None = None
    entered: bool = False
    closed: bool = False
    close_count: int = 0
    close_started: asyncio.Event = field(default_factory=asyncio.Event)
    close_ready: asyncio.Event = field(default_factory=asyncio.Event)
    status = 200
    headers = {"content-type": "text/event-stream"}

    @property
    def content(self):
        return self

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *_args):
        self.close_count += 1
        self.close_started.set()
        await self.close_ready.wait()
        self.closed = True
        if self.close_error is not None:
            raise self.close_error

    async def iter_chunked(self, _size):
        yield format_sse_event(
            {"type": "response.created", "response": {"id": "resp_owned", "status": "in_progress", "output": []}}
        ).encode()
        if not self.terminal:
            await asyncio.Event().wait()
        yield format_sse_event(
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_owned",
                    "status": "completed",
                    "output": [],
                    "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
                },
            }
        ).encode()


async def _classic_response(monkeypatch, upstream, *, native=False):
    # Real core/service/API ownership, with only HTTP, admission, credentials and
    # persistence substituted. Retained handles make assertions independent of GC.
    session = SimpleNamespace(post=lambda *_a, **_kw: upstream, close=AsyncMock())
    core_streams = []
    service_streams = []
    owner = _stream_owner()
    settlement = proxy_service._StreamSettlement()
    account = Account(id="account_offline", access_token_encrypted=b"offline", chatgpt_account_id="account_offline")

    def core_stream(*args, **kwargs):
        stream = core_proxy.stream_responses(*args, **kwargs, session=cast(Any, session))
        core_streams.append(stream)
        return stream

    def service_stream(payload, headers, **_kwargs):
        stream = proxy_service.ProxyService._stream_once(
            cast(Any, owner),
            account,
            payload,
            headers,
            "request_offline",
            False,
            request_started_at=0.0,
            api_key=None,
            api_key_reservation=None,
            settlement=settlement,
            suppress_text_done_events=False,
            upstream_stream_transport="http",
            request_transport="http",
            enforce_openai_sdk_contract=not native,
        )
        service_streams.append(stream)
        return stream

    monkeypatch.setattr(proxy_service, "core_stream_responses", core_stream)
    monkeypatch.setattr(core_proxy, "discover_native_egress_client", lambda: None)
    monkeypatch.setattr(api, "_http_bridge_active_for_request", AsyncMock(return_value=False))
    monkeypatch.setattr(api, "_opportunistic_admission_denial", AsyncMock(return_value=None))
    settings = api.get_settings().model_copy(update={"sse_keepalive_interval_seconds": 0.005})
    monkeypatch.setattr(api, "get_settings", lambda: settings)
    monkeypatch.setattr(api, "with_dashboard_overrides", lambda value: value)
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/responses",
        "headers": [(b"user-agent", b"codex_cli_rs/0.100.0")] if native else [],
        "asgi": {"version": "3.0", "spec_version": "2.4"},
    }
    response = await api._stream_responses(
        Request(scope),
        ResponsesRequest(model="gpt-5", instructions="", input=[]),
        cast(Any, SimpleNamespace(service=SimpleNamespace(stream_responses=service_stream))),
        None,
        prefer_http_bridge=False,
        skip_limit_enforcement=True,
        include_rate_limit_headers=False,
        api_key_policy_already_applied=True,
        enforce_openai_sdk_contract=not native,
    )
    return response, scope, core_streams, service_streams, owner, settlement, session


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
@pytest.mark.parametrize("target", ["start", "created", "keepalive", "complete"])
async def test_classic_asgi_close_reaches_owned_http_response(monkeypatch, spec_version, target):
    upstream = _ClassicHTTPResponse(terminal=target == "complete")
    upstream.close_ready.set()
    response, scope, core_streams, service_streams, owner, settlement, session = await _classic_response(
        monkeypatch, upstream
    )
    scope["asgi"]["spec_version"] = spec_version
    sending = asyncio.Event()

    async def send(message):
        body = message.get("body", b"")
        if (
            (target == "start" and message["type"] == "http.response.start")
            or (target == "created" and b"response.created" in body)
            or (target == "keepalive" and b"keepalive" in body)
        ):
            sending.set()
            if spec_version == "2.4":
                raise OSError("offline peer disconnected")
            await asyncio.Event().wait()

    async def receive():
        await sending.wait()
        return {"type": "http.disconnect"}

    try:
        if spec_version == "2.4" and target != "complete":
            with pytest.raises(ClientDisconnect):
                await response(scope, receive, send)
        else:
            await response(scope, receive, send)
        assert upstream.entered and upstream.closed, "ASGI cleanup must close the real core HTTP response without GC"
        assert upstream.close_count == 1
        assert all(stream.ag_frame is None for stream in [*core_streams, *service_streams])
        assert owner._write_request_log.await_count == 1
        session.close.assert_not_awaited()
        if target == "complete":
            assert settlement.status == "success"
            assert settlement.input_tokens == 1 and settlement.output_tokens == 2
            assert response.outcome == "terminal_written"
    finally:
        await response.body_iterator.aclose()
        for stream in core_streams:
            await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_classic_http_close_preserves_delivery_failure_and_shared_session(monkeypatch, native, close_fails):
    upstream = _ClassicHTTPResponse(
        close_error=RuntimeError("offline response cleanup failed") if close_fails else None
    )
    upstream.close_ready.set()
    response, scope, core_streams, _service_streams, owner, settlement, session = await _classic_response(
        monkeypatch, upstream, native=native
    )

    async def send(_message):
        raise OSError("original writer failure")

    async def receive():
        await asyncio.Event().wait()

    try:
        with pytest.raises(ClientDisconnect) as failure:
            await response(scope, receive, send)
        assert isinstance(failure.value.__context__, OSError)
        assert str(failure.value.__context__) == "original writer failure"
        assert upstream.closed and upstream.close_count == 1
        assert all(stream.ag_frame is None for stream in core_streams)
        assert settlement.response_id == "resp_owned"
        assert owner._write_request_log.await_count == 1
        session.close.assert_not_awaited()
    finally:
        await response.body_iterator.aclose()
        for stream in core_streams:
            await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
async def test_classic_http_close_is_joined_under_repeated_server_cancellation(monkeypatch, spec_version):
    upstream = _ClassicHTTPResponse()
    response, scope, core_streams, _service_streams, owner, _settlement, session = await _classic_response(
        monkeypatch, upstream
    )
    scope["asgi"]["spec_version"] = spec_version
    sending = asyncio.Event()

    async def send(message):
        if b"response.created" in message.get("body", b""):
            sending.set()
            await asyncio.Event().wait()

    async def receive():
        await asyncio.Event().wait()

    writer = asyncio.create_task(response(scope, receive, send))
    try:
        await asyncio.wait_for(sending.wait(), 1)
        writer.cancel()
        await asyncio.wait_for(upstream.close_started.wait(), 1)
        writer.cancel()
        await asyncio.sleep(0)
        assert not writer.done(), "The writer must join the real HTTP response close"
        upstream.close_ready.set()
        with pytest.raises(asyncio.CancelledError):
            await writer
        assert upstream.closed and upstream.close_count == 1
        assert all(stream.ag_frame is None for stream in core_streams)
        assert owner._write_request_log.await_count == 1
        session.close.assert_not_awaited()
        assert response.outcome == "cancelled_before_terminal"
    finally:
        upstream.close_ready.set()
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        await response.body_iterator.aclose()
        for stream in core_streams:
            await stream.aclose()
