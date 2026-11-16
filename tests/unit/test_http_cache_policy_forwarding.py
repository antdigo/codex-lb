import pytest

from app.core.clients.thread_cache_identity import ThreadCachePolicy
from app.modules.proxy.http_bridge_forwarding import (
    HTTP_BRIDGE_SIGNATURE_V2_HEADER,
    HTTPBridgeForwardContext,
    build_owner_forward_headers,
    parse_forwarded_request,
)
from tests.unit.test_http_bridge_forwarding import _payload
from tests.unit.test_http_bridge_forwarding import _temp_bridge_key as _temp_bridge_key

pytestmark = pytest.mark.unit


def forwarded(mode="isolated", from_key=False):
    payload = _payload()
    context = HTTPBridgeForwardContext(
        origin_instance="instance-a",
        target_instance="instance-b",
        codex_session_affinity=True,
        downstream_turn_state="http_turn_synthetic",
        cache_policy=ThreadCachePolicy(mode, from_key),
    )
    return payload, context, build_owner_forward_headers(headers={}, payload=payload, context=context)


@pytest.mark.parametrize("mode", ["shared", "isolated"])
@pytest.mark.parametrize("from_key", [False, True])
def test_authenticated_forward_preserves_original_cache_policy(mode, from_key):
    payload, context, headers = forwarded(mode, from_key)
    decoded, error = parse_forwarded_request(headers, payload=payload, current_instance="instance-b")
    assert error is None and decoded is not None
    assert decoded.context.cache_policy == ThreadCachePolicy(mode, from_key)


@pytest.mark.parametrize("mutation", ["mode", "provenance", "signature", "strip_policy", "strip_all"])
def test_cache_policy_cannot_be_forged_or_stripped_into_legacy_forward(mutation):
    payload, context, headers = forwarded()
    if mutation == "mode":
        headers["x-codex-bridge-cache-identity-mode"] = "shared"
    elif mutation == "provenance":
        headers["x-codex-bridge-cache-identity-from-key"] = "1"
    elif mutation == "signature":
        headers.pop(HTTP_BRIDGE_SIGNATURE_V2_HEADER)
    else:
        headers.pop("x-codex-bridge-cache-identity-mode")
        headers.pop("x-codex-bridge-cache-identity-from-key")
        if mutation == "strip_all":
            headers.pop("x-codex-bridge-signature-version", None)
            headers.pop("x-codex-bridge-original-unanchored", None)
            headers.pop(HTTP_BRIDGE_SIGNATURE_V2_HEADER)
    decoded, error = parse_forwarded_request(headers, payload=payload, current_instance="instance-b")
    assert decoded is None and error is not None
