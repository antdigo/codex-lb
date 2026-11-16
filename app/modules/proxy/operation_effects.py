"""Execute controller-owned resource effects, resuming only unfinished work."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import ParamSpec, TypeVar

from app.modules.proxy.http_operation import OperationController, OperationDecision, ResourceKey

P = ParamSpec("P")
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class EffectResult[T]:
    executed: bool
    result: T | None = None


def execute_resource_now(
    operation: OperationController | None,
    decision: OperationDecision | None,
    key: ResourceKey,
    effect: Callable[P, T],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> EffectResult[T]:
    if operation is None:
        return EffectResult(True, effect(*args, **kwargs))
    if decision is None:
        raise ValueError("HTTP effect requires an operation decision")
    receipt = operation.claim_resource(decision, key)
    if receipt is None:
        previous = operation.resource_receipt(key)
        if previous is None or not previous.completed:
            raise ValueError("synchronous effect is already owned")
        return EffectResult(False)
    try:
        result = effect(*args, **kwargs)
    except BaseException:
        operation.finish_resource(receipt, completed=False)
        raise
    operation.finish_resource(receipt, completed=True)
    return EffectResult(True, result)


async def execute_resource(
    operation: OperationController | None,
    decision: OperationDecision | None,
    key: ResourceKey,
    effect: Callable[P, Awaitable[T]],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> EffectResult[T]:
    if operation is None:
        return EffectResult(True, await effect(*args, **kwargs))
    if decision is None:
        raise ValueError("HTTP effect requires an operation decision")
    while True:
        receipt = operation.claim_resource(decision, key)
        if receipt is not None:
            break
        previous = operation.resource_receipt(key)
        assert previous is not None
        if previous.completed:
            return EffectResult(False)
        await previous.released.wait()
    try:
        result = await effect(*args, **kwargs)
    except BaseException:
        operation.finish_resource(receipt, completed=False)
        raise
    # A False settlement result means responsibility has not been confirmed;
    # release/compensation must remain able to claim this resource.
    operation.finish_resource(receipt, completed=result is not False)
    return EffectResult(True, result)
