"""HTTP operation authority, independent of mutable socket/request adapters.

The controller makes synchronous decisions before adapters await I/O. Physical
attempts stay in the ledger after replacement so delayed circuit settlement can
still charge the correct failed send.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

import anyio

from app.core.clients.thread_cache_identity import ThreadCachePolicy
from app.core.types import JsonValue
from app.core.utils.sse import parse_sse_data_json, reformat_sse_event

PUBLICATION_BUFFER_LIMIT = 256 * 1024


class PublicationState(Enum):
    OPEN = "open"
    COMMITTED = "committed"
    CLOSED = "closed"


class ReplayKind(Enum):
    TRANSPORT = "transport"
    CAPACITY = "capacity"
    HTTP_REFUSAL = "http_refusal"
    AUTH_REFRESH = "auth_refresh"
    AUTH_FAILOVER = "auth_failover"
    CLEAN_CLOSE = "clean_close"
    MODEL_FALLBACK = "model_fallback"
    SECURITY = "security"
    CONTINUITY = "continuity"


_ORDINARY_REPLAY_KINDS = frozenset(
    {
        ReplayKind.TRANSPORT,
        ReplayKind.CAPACITY,
        ReplayKind.HTTP_REFUSAL,
        ReplayKind.MODEL_FALLBACK,
        ReplayKind.SECURITY,
        ReplayKind.CONTINUITY,
    }
)


@dataclass(frozen=True, slots=True)
class OperationInput:
    deadline: float | None
    request_text: str | None = field(default=None, repr=False)
    complete: bool = False
    client_tools_only: bool = False
    cache_policy: ThreadCachePolicy = ThreadCachePolicy()


@dataclass(frozen=True, slots=True)
class AttemptEvent:
    attempt: "ResponseCreateAttempt"
    generation: int
    account_id: str
    event_type: str
    response_id: str | None = None


@dataclass(frozen=True, slots=True, eq=False)
class ReplayGrant:
    kind: ReplayKind
    attempt_ordinal: int
    account_id: str | None


@dataclass(frozen=True, slots=True, eq=False)
class DispatchPermit:
    """Controller-issued capability; identity, not equal fields, is authority."""

    source: "ResponseCreateAttempt | None"
    required_account_id: str | None
    grant: ReplayGrant | None = None
    transport: Literal["websocket", "http"] = "websocket"


class DispatchDenied(ValueError):
    """The caller does not own a live single-use physical dispatch capability."""


class DecisionAction(Enum):
    IGNORE = "ignore"
    WAIT = "wait"
    RECOVER = "recover"
    SETTLE = "settle"
    HANDOFF = "handoff"


@dataclass(frozen=True, slots=True)
class CloseObservation:
    attempt: "ResponseCreateAttempt | None"
    now: float
    generation: int
    account_id: str
    recovery_candidate: bool
    ambiguous: bool = False


@dataclass(frozen=True, slots=True)
class TimeoutObservation:
    now: float
    due_at: float
    recovery_candidate: bool = False
    completion_owned: bool = False


@dataclass(frozen=True, slots=True)
class CancellationObservation:
    owner_key: str


@dataclass(frozen=True, slots=True)
class HandoffObservation:
    now: float
    proven_unsent: bool


@dataclass(frozen=True, slots=True)
class TerminalObservation:
    attempt: "ResponseCreateAttempt | None"
    owner_key: str
    successful: bool = False
    compensation: bool = False


@dataclass(frozen=True, slots=True)
class OperationDecision:
    action: DecisionAction
    owner_key: str | None = None
    total_deadline_expired: bool = False


class SettlementResource(Enum):
    RESERVATION = "reservation"
    CREATE_GATE = "create_gate"
    EXECUTION = "execution"
    HEARTBEAT = "heartbeat"
    LEASE = "lease"
    REQUEST_LOG = "request_log"
    HEALTH = "health"
    TERMINAL_DELIVERY = "terminal_delivery"


class TerminalOwnership(Enum):
    ACTIVE = "active"
    ABANDONED = "abandoned"
    FINISHED = "finished"


@dataclass(frozen=True, slots=True)
class ResourceKey:
    resource: SettlementResource
    identity: str


@dataclass(eq=False, slots=True)
class SettlementReceipt:
    key: ResourceKey
    owner_key: str
    completed: bool = False
    running: bool = True
    released: anyio.Event = field(default_factory=anyio.Event, repr=False)


def is_strictly_opaque_reasoning(event_type: str | None, payload: Mapping[str, JsonValue] | None) -> bool:
    """Unknown fields and readable summary/content are publication barriers."""
    if event_type not in {"response.output_item.added", "response.output_item.done"} or payload is None:
        return False
    if set(payload) - {"type", "response_id", "item", "output_index", "sequence_number"}:
        return False
    item = payload.get("item")
    if not isinstance(item, dict) or item.get("type") != "reasoning":
        return False
    if set(item) - {"id", "type", "status", "encrypted_content", "summary", "content"}:
        return False
    if item.get("summary") not in (None, []) or item.get("content") not in (None, []):
        return False
    encrypted = item.get("encrypted_content")
    return (
        isinstance(item.get("id"), str)
        and bool(item["id"])
        and item.get("status") in (None, "in_progress", "completed")
        and (encrypted is None or isinstance(encrypted, str))
    )


@dataclass(eq=False, slots=True)
class ResponseCreateAttempt:
    ordinal: int
    disarmed: bool = False
    response_observed: bool = False
    non_terminal_response_observed: bool = False
    retry_circuit_failure_recorded: bool = False
    retry_circuit_failure_settled: anyio.Event | None = None
    account_id: str | None = None
    generation: int | None = None
    upstream_response_id: str | None = None
    prepared_replay_text: str | None = field(default=None, repr=False)
    usage_unknown: bool = False
    recovery_kind: ReplayKind | None = None
    connection_identity: int | None = None


@dataclass(slots=True, init=False)
class OperationController:
    initial: OperationInput
    publication: PublicationState
    public_response_id: str | None
    _attempts: list[ResponseCreateAttempt]
    _grants: list[ReplayGrant]
    _buffer: list[str] = field(repr=False)
    _buffer_size: int
    _last_public_sequence: int | None
    _pending_dispatch: DispatchPermit | None
    _settlement_owners: set[str]
    _resource_receipts: dict[ResourceKey, SettlementReceipt]
    _terminal_owners: dict[str, TerminalOwnership]
    _transport: Literal["websocket", "http"]
    _reservation_keys: dict[str, ResourceKey]

    def __init__(
        self,
        deadline: float | None,
        *,
        request_text: str | None = None,
        complete_input: bool = False,
        client_tools_only: bool = False,
        cache_policy: ThreadCachePolicy = ThreadCachePolicy(),
    ) -> None:
        self.initial = OperationInput(deadline, request_text, complete_input, client_tools_only, cache_policy)
        self.publication = PublicationState.OPEN
        self.public_response_id = None
        self._attempts = []
        self._grants = []
        self._buffer = []
        self._buffer_size = 0
        self._last_public_sequence = None
        self._pending_dispatch = DispatchPermit(None, None)
        self._settlement_owners = set()
        self._resource_receipts = {}
        self._terminal_owners = {}
        self._transport = "websocket"
        self._reservation_keys = {}

    @property
    def deadline(self) -> float | None:
        return self.initial.deadline

    @property
    def buffered(self) -> bool:
        return bool(self._buffer)

    @property
    def grants(self) -> tuple[ReplayGrant, ...]:
        return tuple(self._grants)

    def used(self, kind: ReplayKind) -> bool:
        return any(grant.kind is kind for grant in self._grants)

    @property
    def attempts(self) -> tuple[ResponseCreateAttempt, ...]:
        return tuple(self._attempts)

    @property
    def pending_dispatch(self) -> DispatchPermit | None:
        return self._pending_dispatch

    def accepts_grant(self, grant: ReplayGrant) -> bool:
        permit = self._pending_dispatch
        return permit is not None and permit.grant is grant and any(issued is grant for issued in self._grants)

    def decide(
        self,
        observation: CloseObservation
        | TimeoutObservation
        | CancellationObservation
        | TerminalObservation
        | HandoffObservation,
    ) -> OperationDecision:
        """The adapters submit evidence; only this authority chooses recovery/termination."""
        if isinstance(observation, HandoffObservation):
            if (
                not observation.proven_unsent
                or self._attempts
                or self._transport != "websocket"
                or self.publication is not PublicationState.OPEN
                or self.deadline is not None
                and observation.now >= self.deadline
            ):
                return OperationDecision(DecisionAction.SETTLE)
            self._transport = "http"
            self._pending_dispatch = DispatchPermit(None, None, transport="http")
            return OperationDecision(DecisionAction.HANDOFF)
        if isinstance(observation, CancellationObservation):
            self.close()
            self._settlement_owners.add(observation.owner_key)
            return OperationDecision(DecisionAction.SETTLE, observation.owner_key)
        if isinstance(observation, TerminalObservation):
            if not observation.compensation and (
                observation.attempt is not None
                and (not self._attempts or self._attempts[-1] is not observation.attempt)
            ):
                return OperationDecision(DecisionAction.IGNORE)
            self._settlement_owners.add(observation.owner_key)
            if not observation.compensation:
                self._terminal_owners[observation.owner_key] = TerminalOwnership.ACTIVE
            return OperationDecision(DecisionAction.SETTLE, observation.owner_key)
        if self.publication is PublicationState.CLOSED:
            return OperationDecision(DecisionAction.IGNORE)
        expired = self.deadline is not None and observation.now >= self.deadline
        if isinstance(observation, CloseObservation):
            if observation.attempt is not None and (
                not self.accepts(observation.attempt, generation=observation.generation)
                or observation.attempt.account_id != observation.account_id
            ):
                return OperationDecision(DecisionAction.IGNORE)
            recover = observation.recovery_candidate and not observation.ambiguous
        else:
            if not expired and (observation.now < observation.due_at or observation.completion_owned):
                return OperationDecision(DecisionAction.WAIT)
            recover = observation.recovery_candidate
        if not expired and recover and self.publication is PublicationState.OPEN:
            return OperationDecision(DecisionAction.RECOVER)
        return OperationDecision(DecisionAction.SETTLE, total_deadline_expired=expired)

    def resource_receipt(self, key: ResourceKey) -> SettlementReceipt | None:
        return self._resource_receipts.get(key)

    def reservation_key(self, decision: OperationDecision, reservation_id: str | None) -> ResourceKey:
        if decision.owner_key is None:
            raise ValueError("reservation has no settlement owner")
        return self._reservation_keys.setdefault(
            decision.owner_key, ResourceKey(SettlementResource.RESERVATION, reservation_id or decision.owner_key)
        )

    def abandon_terminal(self, owner_key: str) -> None:
        self._terminal_owners[owner_key] = TerminalOwnership.ABANDONED

    def finish_terminal(self, owner_key: str) -> None:
        self._terminal_owners[owner_key] = TerminalOwnership.FINISHED

    def allows_compensation(self, owner_key: str) -> bool:
        return self._terminal_owners.get(owner_key) is not TerminalOwnership.ACTIVE

    def claim_resource(self, decision: OperationDecision, key: ResourceKey) -> SettlementReceipt | None:
        if decision.action is not DecisionAction.SETTLE or decision.owner_key not in self._settlement_owners:
            raise ValueError("resource effect has no operation settlement authority")
        existing = self._resource_receipts.get(key)
        if existing is not None and (existing.running or existing.completed):
            return None
        assert decision.owner_key is not None
        receipt = SettlementReceipt(key, decision.owner_key)
        self._resource_receipts[key] = receipt
        return receipt

    def finish_resource(self, receipt: SettlementReceipt, *, completed: bool) -> None:
        if self._resource_receipts.get(receipt.key) is not receipt or not receipt.running:
            raise ValueError("stale operation settlement receipt")
        receipt.completed = completed
        receipt.running = False
        receipt.released.set()

    def begin_attempt(
        self,
        *,
        permit: DispatchPermit | None,
        now: float,
        account_id: str,
        generation: int,
        prepared_replay_text: str | None = None,
        connection_identity: int | None = None,
        transport: Literal["websocket", "http"] = "websocket",
    ) -> ResponseCreateAttempt:
        source = self._attempts[-1] if self._attempts else None
        if (
            permit is None
            or permit is not self._pending_dispatch
            or permit.source is not source
            or permit.transport != transport
            or self.publication is not PublicationState.OPEN
            or self.deadline is not None
            and now >= self.deadline
            or permit.required_account_id is not None
            and permit.required_account_id != account_id
            or generation < 0
            or source is not None
            and source.connection_identity == connection_identity
            and source.generation is not None
            and generation <= source.generation
        ):
            raise DispatchDenied("stale, consumed, expired, or incompatible HTTP dispatch permit")
        # Consume before the adapter invokes or awaits send_text. An ambiguous
        # or cancelled send remains one physical attempt, never a reusable token.
        self._pending_dispatch = None
        attempt = ResponseCreateAttempt(
            ordinal=len(self._attempts) + 1,
            account_id=account_id,
            generation=generation,
            prepared_replay_text=prepared_replay_text,
            connection_identity=connection_identity,
        )
        self._attempts.append(attempt)
        return attempt

    def accepts(self, attempt: ResponseCreateAttempt, *, generation: int | None = None) -> bool:
        return bool(
            self._attempts
            and self._attempts[-1] is attempt
            and not attempt.disarmed
            and (generation is None or attempt.generation == generation)
        )

    def observe_event(self, event: AttemptEvent) -> bool:
        if event.account_id != event.attempt.account_id or not self.accepts(event.attempt, generation=event.generation):
            return False
        return self.observe(event.attempt, event.event_type, response_id=event.response_id)

    def observe(self, attempt: ResponseCreateAttempt, event_type: str, *, response_id: str | None = None) -> bool:
        if self.publication is PublicationState.CLOSED or not self.accepts(attempt):
            return False
        if not event_type.startswith("response."):
            return False
        attempt.response_observed = True
        if event_type not in {"response.failed", "response.incomplete"}:
            attempt.non_terminal_response_observed = True
        if response_id is not None and attempt.upstream_response_id is None:
            attempt.upstream_response_id = response_id
        return True

    def buffer_opaque(self, event_type: str | None, payload: Mapping[str, JsonValue] | None, block: str) -> bool:
        if self.publication is not PublicationState.OPEN:
            return False
        if event_type in {"response.created", "response.in_progress"}:
            response = payload.get("response") if payload is not None else None
            if (
                not isinstance(response, Mapping)
                or response.get("output") not in (None, [])
                or response.get("output_text") not in (None, "")
            ):
                self.publication = PublicationState.COMMITTED
            return False
        if event_type in {"codex.rate_limits", "rate_limits.updated"}:
            return False
        # Terminal policies decide whether to retry before publishing a terminal.
        if event_type in {"response.completed", "response.failed", "response.incomplete", "error"}:
            return False
        if is_strictly_opaque_reasoning(event_type, payload):
            size = len(block.encode("utf-8"))
            if self._buffer_size + size <= PUBLICATION_BUFFER_LIMIT:
                self._buffer.append(block)
                self._buffer_size += size
                return True
        # Commit synchronously, before any adapter enqueues the current frame or
        # the previously held prefix. Overflow uses the same publication fence.
        self.publication = PublicationState.COMMITTED
        return False

    def take_buffer(self) -> list[str]:
        result = self._buffer
        self._buffer = []
        self._buffer_size = 0
        return [self.project_frame(block) for block in result]

    def publication_batch(self, block: str, *, final: bool = False) -> tuple[str, ...]:
        """Select exactly the frames an adapter may publish, before its first await.

        Telemetry and lifecycle heartbeats never release the opaque prefix. A
        failed terminal may still be consumed by the outer recovery adapter;
        that adapter passes final=True only when it actually exposes the error.
        """
        if self.publication is PublicationState.CLOSED:
            return ()
        payload = parse_sse_data_json(block)
        terminal = payload is not None and payload.get("type") in {
            "response.completed",
            "response.failed",
            "response.incomplete",
            "error",
        }
        if payload is not None and (payload.get("type") == "response.completed" or final and terminal):
            self.commit()
        prefix = self.take_buffer() if self.publication is PublicationState.COMMITTED else []
        return (*prefix, self.project_frame(block))

    def project_frame(self, block: str) -> str:
        """Allocate HTTP sequence numbers only for frames selected for publication."""
        payload = parse_sse_data_json(block)
        if payload is None:
            return block
        projected = dict(payload)
        if self.public_response_id is not None:
            if isinstance(payload.get("response_id"), str):
                projected["response_id"] = self.public_response_id
            response = payload.get("response")
            if isinstance(response, dict) and isinstance(response.get("id"), str):
                projected["response"] = {**response, "id": self.public_response_id}
        upstream_sequence = payload.get("sequence_number") if payload is not None else None
        if not isinstance(upstream_sequence, int) or isinstance(upstream_sequence, bool):
            return reformat_sse_event(block, projected) if projected != payload else block
        sequence = (
            upstream_sequence
            if self._last_public_sequence is None
            else max(upstream_sequence, self._last_public_sequence + 1)
        )
        self._last_public_sequence = sequence
        projected["sequence_number"] = sequence
        return reformat_sse_event(block, projected) if projected != payload else block

    def discard_buffer(self) -> None:
        self._buffer.clear()
        self._buffer_size = 0

    def choose_public_response_id(self, response_id: str | None) -> str | None:
        if self.public_response_id is None:
            self.public_response_id = response_id
        return self.public_response_id

    def authorize_replay(
        self,
        kind: ReplayKind,
        *,
        now: float,
        account_id: str,
        authority_valid: bool = True,
        complete_input_required: bool = False,
        verified_complete_input: bool = False,
        allow_account_switch: bool = False,
        required_account_id: str | None = None,
    ) -> ReplayGrant | None:
        if (
            self.publication is not PublicationState.OPEN
            or not authority_valid
            or (self.deadline is not None and now >= self.deadline)
            or not self._attempts
            or self._pending_dispatch is not None
        ):
            return None
        if complete_input_required and not (
            (self.initial.complete or verified_complete_input) and self.initial.client_tools_only
        ):
            return None
        if any(
            (grant.kind is kind or (kind in _ORDINARY_REPLAY_KINDS and grant.kind in _ORDINARY_REPLAY_KINDS))
            and (kind not in {ReplayKind.AUTH_REFRESH, ReplayKind.AUTH_FAILOVER} or grant.account_id == account_id)
            for grant in self._grants
        ):
            return None
        grant = ReplayGrant(kind, self._attempts[-1].ordinal, account_id)
        self._grants.append(grant)
        self._pending_dispatch = DispatchPermit(
            self._attempts[-1],
            required_account_id or (None if allow_account_switch else account_id),
            grant,
            self._transport,
        )
        self._attempts[-1].usage_unknown = True
        self._attempts[-1].recovery_kind = kind
        return grant

    def commit(self) -> None:
        if self.publication is PublicationState.OPEN:
            self.publication = PublicationState.COMMITTED
            self._pending_dispatch = None

    def close(self) -> None:
        self.publication = PublicationState.CLOSED
        self._pending_dispatch = None
        self.discard_buffer()
