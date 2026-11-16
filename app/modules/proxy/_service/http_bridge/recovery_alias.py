"""Captured authority for one exact stale-anchor turn-alias transition."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.modules.proxy.durable_bridge_repository import DurableBridgeAliasSourceFence, durable_bridge_api_key_scope
from app.modules.proxy.http_operation import DispatchPermit, OperationController, ReplayKind

if TYPE_CHECKING:
    from app.modules.proxy._service.http_bridge.protocol import _HTTPBridgeServiceProtocol
    from app.modules.proxy._service.support import _HTTPBridgeSession, _WebSocketRequestState


@dataclass(frozen=True, slots=True)
class RecoveryAliasTransfer:
    source: _HTTPBridgeSession
    alias: str
    generation: int
    alias_generation: int
    operation: OperationController
    permit: DispatchPermit
    durable: DurableBridgeAliasSourceFence

    @classmethod
    def capture_locked(
        cls,
        service: _HTTPBridgeServiceProtocol,
        source: _HTTPBridgeSession,
        state: _WebSocketRequestState,
        alias: str,
        *,
        instance_id: str,
        process_epoch: str,
    ) -> RecoveryAliasTransfer | None:
        """Called only after full-resend proof and CONTINUITY authorization."""
        operation = state.http_operation
        permit = operation.pending_dispatch if operation is not None else None
        if (
            operation is None
            or permit is None
            or permit.grant is None
            or permit.grant.kind is not ReplayKind.CONTINUITY
            or not operation.accepts_grant(permit.grant)
            or permit.source is not state.response_create_attempt
            or permit.source is None
            or permit.source.account_id != source.account.id
            or permit.source.generation != source.connection_generation
            or state.file_required_preferred_account
            or not state.operation_registered
            or state.operation_id is None
            or source.durable_session_id is None
            or source.durable_owner_epoch is None
            or service._http_bridge_sessions.get(source.key) is not source
            or service._http_bridge_turn_state_index.get((alias, source.key.api_key_id)) != source.key
            or alias not in source.downstream_turn_state_aliases
            or alias not in source.turn_state_alias_registration_generations
        ):
            return None
        return cls(
            source=source,
            alias=alias,
            generation=source.connection_generation,
            alias_generation=source.alias_registration_generation,
            operation=operation,
            permit=permit,
            durable=DurableBridgeAliasSourceFence(
                session_id=source.durable_session_id,
                api_key_scope=durable_bridge_api_key_scope(source.key.api_key_id),
                instance_id=instance_id,
                owner_epoch=source.durable_owner_epoch,
                owner_process_epoch=process_epoch,
                account_id=source.account.id,
                canonical_kind=source.key.affinity_kind,
                canonical_key=source.key.affinity_key,
            ),
        )

    def allows_locked(self, service: _HTTPBridgeServiceProtocol, target: _HTTPBridgeSession, alias: str) -> bool:
        """Recheck local ownership before and after the durable alias CAS."""
        source = self.source
        current_key = service._http_bridge_turn_state_index.get((alias, target.key.api_key_id))
        registered_source = service._http_bridge_sessions.get(source.key)
        return (
            alias == self.alias
            and target.key.api_key_id == source.key.api_key_id
            and durable_bridge_api_key_scope(target.key.api_key_id) == self.durable.api_key_scope
            and source.closed
            and source.connection_generation == self.generation
            and source.alias_registration_generation == self.alias_generation
            and source.account.id == self.durable.account_id
            and source.durable_session_id == self.durable.session_id
            and source.durable_owner_epoch == self.durable.owner_epoch
            and source.key.affinity_kind == self.durable.canonical_kind
            and source.key.affinity_key == self.durable.canonical_key
            and self.operation.pending_dispatch is self.permit
            and self.permit.grant is not None
            and self.operation.accepts_grant(self.permit.grant)
            # Controlled source detachment removes the old local index. Any
            # intervening publication, including another generation at the
            # same key, invalidates the capture before target publication.
            and (current_key is None or current_key == source.key)
            and (registered_source is None or registered_source is source)
        )
