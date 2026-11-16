"""One resumable owner for an account's response-create lease release."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Protocol

from app.core.clock import Scheduler
from app.core.utils.shared_future import _await_task_deferring_cancellation

if TYPE_CHECKING:
    from app.modules.proxy.load_balancer import AccountLease


class AccountCreateLeaseOwner(Protocol):
    account_response_create_lease: AccountLease | None
    account_response_create_release: Callable[[AccountLease | None], Coroutine[Any, Any, None]] | None
    account_response_create_release_task: asyncio.Task[None] | None


async def _release_request_state_account_create_lease(
    request_state: AccountCreateLeaseOwner,
    *,
    scheduler: Scheduler,
) -> asyncio.CancelledError | None:
    """Join the captured lease's release without losing its resumable receipt."""

    task = request_state.account_response_create_release_task
    if task is None:
        lease = request_state.account_response_create_lease
        release = request_state.account_response_create_release
        if lease is None or release is None:
            # Only the service adapter can supply a missing release callback.
            # This adapter must not claim that an unowned release succeeded.
            return None

        async def release_captured() -> None:
            await release(lease)
            if request_state.account_response_create_lease is lease:
                request_state.account_response_create_lease = None
                request_state.account_response_create_release = None

        task = scheduler.create_task(release_captured(), name="proxy-response-create-account-release")
        request_state.account_response_create_release_task = task
    try:
        _, cancellation = await _await_task_deferring_cancellation(task)
        return cancellation
    finally:
        if task.done() and request_state.account_response_create_release_task is task:
            # A failed callback retains the lease and callback for a later
            # cleanup attempt. Old compensation never clears a new lease.
            request_state.account_response_create_release_task = None
