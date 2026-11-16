"""Operation-owned physical attempts reject superseded observations."""

import pytest

from app.modules.proxy.http_operation import DispatchDenied, OperationController, ReplayKind

pytestmark = pytest.mark.unit


def test_attempt_ledger_retains_physical_identity_across_replacement():
    operation = OperationController(deadline=100.0)
    first = operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="a", generation=1)
    assert operation.observe(first, "response.created", response_id="physical-first")
    assert operation.authorize_replay(ReplayKind.TRANSPORT, now=1.0, account_id="a") is not None
    replacement = operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="a", generation=2)
    assert replacement.ordinal == 2
    assert not first.disarmed
    assert first.upstream_response_id == "physical-first"
    assert operation.observe(replacement, "response.created", response_id="physical-second")
    assert operation.attempts == (first, replacement)
    assert operation.deadline == 100.0


def test_superseded_event_cannot_mutate_current_attempt():
    operation = OperationController(deadline=100.0)
    old = operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="a", generation=1)
    assert operation.authorize_replay(ReplayKind.TRANSPORT, now=1.0, account_id="a") is not None
    current = operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="a", generation=2)
    assert not operation.observe(old, "response.completed", response_id="stale")
    assert not current.response_observed
    assert current.upstream_response_id is None


def test_terminal_failure_does_not_prove_nonterminal_output():
    operation = OperationController(deadline=100.0)
    attempt = operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="a", generation=1)
    assert operation.observe(attempt, "response.failed")
    assert attempt.response_observed
    assert not attempt.non_terminal_response_observed


@pytest.mark.parametrize(
    "first_kind",
    [
        ReplayKind.TRANSPORT,
        ReplayKind.CAPACITY,
        ReplayKind.HTTP_REFUSAL,
        ReplayKind.MODEL_FALLBACK,
        ReplayKind.CONTINUITY,
        ReplayKind.SECURITY,
    ],
)
@pytest.mark.parametrize(
    "second_kind",
    [
        ReplayKind.TRANSPORT,
        ReplayKind.CAPACITY,
        ReplayKind.HTTP_REFUSAL,
        ReplayKind.MODEL_FALLBACK,
        ReplayKind.CONTINUITY,
        ReplayKind.SECURITY,
    ],
)
def test_ordinary_replay_quota_spans_failure_kinds_and_physical_attempts(first_kind, second_kind):
    operation = OperationController(deadline=100.0, complete_input=True, client_tools_only=True)
    operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="owner", generation=1)
    assert operation.authorize_replay(first_kind, now=1.0, account_id="owner") is not None
    operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="owner", generation=2)
    assert operation.authorize_replay(second_kind, now=2.0, account_id="owner") is None
    assert operation.authorize_replay(ReplayKind.AUTH_REFRESH, now=2.0, account_id="owner") is not None
    operation.begin_attempt(permit=operation.pending_dispatch, now=2.0, account_id="owner", generation=3)
    assert operation.authorize_replay(ReplayKind.CLEAN_CLOSE, now=2.0, account_id="owner") is not None


@pytest.mark.parametrize("fence", ["committed", "closed", "deadline", "incomplete-input", "unsafe-tools"])
def test_http_refusal_cannot_bypass_operation_replay_fences(fence):
    operation = OperationController(
        deadline=100.0,
        complete_input=fence != "incomplete-input",
        client_tools_only=fence != "unsafe-tools",
    )
    operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="owner", generation=1)
    if fence == "committed":
        operation.commit()
    elif fence == "closed":
        operation.close()
    assert (
        operation.authorize_replay(
            ReplayKind.HTTP_REFUSAL,
            now=100.0 if fence == "deadline" else 2.0,
            account_id="owner",
            complete_input_required=True,
        )
        is None
    )
    assert operation.pending_dispatch is None
    assert not operation.grants
    assert len(operation.attempts) == 1


def test_http_refusal_account_switch_keeps_required_owner_dispatch_fence():
    operation = OperationController(deadline=100.0, complete_input=True, client_tools_only=True)
    operation.begin_attempt(permit=operation.pending_dispatch, now=1.0, account_id="file-owner", generation=1)
    assert (
        operation.authorize_replay(
            ReplayKind.HTTP_REFUSAL,
            now=2.0,
            account_id="file-owner",
            complete_input_required=True,
            allow_account_switch=True,
            required_account_id="file-owner",
        )
        is not None
    )
    permit = operation.pending_dispatch
    with pytest.raises(DispatchDenied):
        operation.begin_attempt(permit=permit, now=2.0, account_id="different-account", generation=2)
    assert len(operation.attempts) == 1
    operation.begin_attempt(permit=permit, now=2.0, account_id="file-owner", generation=2)
    assert len(operation.attempts) == 2
    assert operation.deadline == 100.0
