"""Execute a proven-unsent bridge handoff under the original HTTP authority."""

import asyncio
import logging
from collections.abc import AsyncGenerator

from app.core.clients.proxy import ProxyResponseError, is_confirmed_pre_dispatch_transport_error
from app.core.clock import Clock, Scheduler
from app.core.errors import openai_error, response_failed_event
from app.core.utils.sse import format_sse_event, parse_sse_data_json
from app.modules.proxy._service.support import _RetryableStreamError, _TransientStreamError
from app.modules.proxy.helpers import (
    _is_account_model_unsupported_error,
    _normalize_error_code,
    _parse_openai_error,
    _upstream_error_from_openai,
    classify_upstream_failure,
    is_upstream_model_capacity_error,
)
from app.modules.proxy.http_operation import (
    AttemptEvent,
    CancellationObservation,
    CloseObservation,
    DecisionAction,
    DispatchDenied,
    OperationController,
    ReplayKind,
    ResponseCreateAttempt,
    TerminalObservation,
    TimeoutObservation,
)

logger = logging.getLogger("app.modules.proxy.service")


def begin_handoff_dispatch(
    operation: OperationController,
    *,
    account_id: str,
    clock: Clock,
    connection_identity: int,
    request_id: str,
) -> None:
    try:
        attempt = operation.begin_attempt(
            permit=operation.pending_dispatch,
            now=clock.monotonic(),
            account_id=account_id,
            generation=len(operation.attempts) + 1,
            connection_identity=connection_identity,
            transport="http",
        )
        if attempt.ordinal > 1:
            abandoned = operation.attempts[-2]
            logger.info(
                "HTTP response attempt replaced request_id=%s attempt=%s generation=%s "
                "account_id=%s recovery_kind=%s usage_unknown=%s transport=http",
                request_id,
                abandoned.ordinal,
                abandoned.generation,
                abandoned.account_id,
                abandoned.recovery_kind.value if abandoned.recovery_kind is not None else "unspecified",
                abandoned.usage_unknown,
            )
    except DispatchDenied as exc:
        expired = operation.deadline is not None and clock.monotonic() >= operation.deadline
        raise ProxyResponseError(
            504 if expired else 502,
            openai_error("upstream_request_timeout" if expired else "upstream_unavailable", str(exc)),
            local_pre_dispatch_refusal=True,
        ) from exc


async def stream_operation_attempt(
    stream: AsyncGenerator[str, None],
    operation: OperationController,
    *,
    account_id: str,
    clock: Clock,
    scheduler: Scheduler,
    required_account_id: str | None = None,
    model: str | None = None,
) -> AsyncGenerator[str, None]:
    previous_attempt_count = len(operation.attempts)
    attempt: ResponseCreateAttempt | None = None
    owner_key = f"raw-http:{previous_attempt_count + 1}"
    try:
        while True:
            try:
                remaining = None if operation.deadline is None else max(0, operation.deadline - clock.monotonic())
                block = await scheduler.wait_for(stream.__anext__(), timeout=remaining)
                attempt = operation.attempts[-1] if len(operation.attempts) > previous_attempt_count else None
            except StopAsyncIteration:
                break
            except TimeoutError:
                now = clock.monotonic()
                decision = operation.decide(TimeoutObservation(now, operation.deadline or now))
                if decision.action is DecisionAction.SETTLE:
                    terminal = format_sse_event(
                        response_failed_event(
                            "upstream_request_timeout",
                            "Original HTTP deadline exhausted",
                            response_id=operation.public_response_id,
                        )
                    )
                    for published in operation.publication_batch(terminal, final=True):
                        yield published
                return
            payload = parse_sse_data_json(block)
            if payload is None:
                yield block
                continue
            event_type = payload.get("type")
            if not isinstance(event_type, str):
                event_type = "unknown"
            response = payload.get("response")
            response_id = response.get("id") if isinstance(response, dict) else payload.get("response_id")
            response_id = response_id if isinstance(response_id, str) else None
            if attempt is None:
                raise DispatchDenied("HTTP adapter yielded without dispatch authority")
            assert attempt.generation is not None
            operation.observe_event(AttemptEvent(attempt, attempt.generation, account_id, event_type, response_id))
            if event_type in {"response.created", "response.completed", "response.failed", "response.incomplete"}:
                operation.choose_public_response_id(response_id)
            terminal = event_type in {"response.completed", "response.failed", "response.incomplete", "error"}
            if terminal:
                operation.decide(TerminalObservation(attempt, owner_key, successful=event_type == "response.completed"))
            if operation.buffer_opaque(event_type, payload, block):
                continue
            for published in operation.publication_batch(block, final=terminal):
                yield published
    except (asyncio.CancelledError, GeneratorExit):
        operation.decide(CancellationObservation(owner_key))
        raise
    except (ProxyResponseError, _TransientStreamError, _RetryableStreamError) as exc:
        attempt = operation.attempts[-1] if len(operation.attempts) > previous_attempt_count else None
        if attempt is None:
            raise
        # The raw adapter may propose another selection, but only a proved
        # rejection before any accepted lifecycle can issue its next permit.
        code = exc.code if isinstance(exc, (_TransientStreamError, _RetryableStreamError)) else None
        message = exc.error.get("message") if isinstance(exc, (_TransientStreamError, _RetryableStreamError)) else None
        proven_unsent = isinstance(exc, ProxyResponseError) and is_confirmed_pre_dispatch_transport_error(exc)
        error = None
        if isinstance(exc, ProxyResponseError):
            error = _parse_openai_error(exc.payload)
            code = error.code if error is not None else None
            message = error.message if error is not None else None
        kind = None
        if isinstance(exc, ProxyResponseError) and exc.status_code == 401:
            refreshed = any(g.kind is ReplayKind.AUTH_REFRESH and g.account_id == account_id for g in operation.grants)
            kind = ReplayKind.AUTH_FAILOVER if refreshed else ReplayKind.AUTH_REFRESH
        elif is_upstream_model_capacity_error(message):
            kind = ReplayKind.CAPACITY
        elif _is_account_model_unsupported_error(code=code, message=message, model=model):
            kind = ReplayKind.MODEL_FALLBACK
        elif code == "security_work_authorization_required":
            kind = ReplayKind.SECURITY
        elif proven_unsent:
            kind = ReplayKind.TRANSPORT
        elif (
            isinstance(exc, ProxyResponseError)
            and exc.failure_phase == "status"
            and exc.status_code >= 400
            and not exc.local_pre_dispatch_refusal
        ):
            # Share the raw selector's refusal classification. A status-shaped
            # transport exception is not proof that upstream rejected the POST.
            classified = classify_upstream_failure(
                error_code=_normalize_error_code(code, error.type if error is not None else None),
                error=_upstream_error_from_openai(error),
                http_status=exc.status_code,
                phase="first_event",
            )
            if classified["failure_class"] in {"rate_limit", "quota", "retryable_transient", "account_unavailable"}:
                kind = ReplayKind.HTTP_REFUSAL
        candidate = kind is not None and operation.public_response_id is None
        decision = operation.decide(
            CloseObservation(
                attempt,
                clock.monotonic(),
                attempt.generation or 0,
                account_id,
                recovery_candidate=candidate,
                ambiguous=not candidate,
            )
        )
        if kind is not None and decision.action is DecisionAction.RECOVER:
            operation.authorize_replay(
                kind,
                now=clock.monotonic(),
                account_id=account_id,
                complete_input_required=kind not in {ReplayKind.AUTH_REFRESH, ReplayKind.AUTH_FAILOVER},
                allow_account_switch=kind
                in {
                    ReplayKind.AUTH_FAILOVER,
                    ReplayKind.CAPACITY,
                    ReplayKind.HTTP_REFUSAL,
                    ReplayKind.MODEL_FALLBACK,
                    ReplayKind.SECURITY,
                },
                required_account_id=required_account_id,
            )
        raise
    finally:
        await stream.aclose()
