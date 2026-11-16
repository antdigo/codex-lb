"""Open physical HTTP bridge sockets under an immutable request policy."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from app.core.clients.proxy_websocket import UpstreamWebSocket
from app.core.clients.thread_cache_identity import ThreadCachePolicy
from app.db.models import Account
from app.modules.proxy._service.support import _call_with_supported_optional_kwargs

if TYPE_CHECKING:
    from app.modules.proxy._service.http_bridge.protocol import _HTTPBridgeServiceProtocol
    from app.modules.proxy._service.support import _HTTPBridgeSession, _WebSocketRequestState


async def open_bridge_upstream(
    service: _HTTPBridgeServiceProtocol,
    account: Account,
    headers: dict[str, str],
    policy: ThreadCachePolicy,
    state: _WebSocketRequestState,
    timeout: float,
) -> UpstreamWebSocket:
    return await _call_with_supported_optional_kwargs(
        service._open_upstream_websocket_with_budget,
        account,
        headers,
        timeout_seconds=timeout,
        optional_kwargs={"request_state": state, "thread_cache_identity": policy.for_account(account.id)},
    )


def bridge_replacement_opener(
    service: _HTTPBridgeServiceProtocol,
    session: _HTTPBridgeSession,
    state: _WebSocketRequestState,
    deadline: float,
) -> Callable[[Account, dict[str, str]], Awaitable[UpstreamWebSocket]]:
    policy = (
        state.http_operation.initial.cache_policy
        if state.http_operation
        else ThreadCachePolicy(session.cache_identity_mode)
    )

    async def open_replacement(account: Account, headers: dict[str, str]) -> UpstreamWebSocket:
        try:
            return await open_bridge_upstream(
                service, account, headers, policy, state, service._remaining_budget_seconds(deadline)
            )
        except Exception:
            session.closed = True
            raise

    return open_replacement
