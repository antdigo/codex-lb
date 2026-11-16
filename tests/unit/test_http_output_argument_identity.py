"""Equality authority must not come from lossy JSON value conversion."""

import pytest

from app.modules.proxy._service.http_bridge.continuation import response_output_identity

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "left,right",
    [
        ('{"x":1.00000000000000001}', '{"x":1.0}'),
        ('{"x":1,"x":2}', '{"x":2}'),
        ('{"x":"a b"}', '{"x":"ab"}'),
        ('{"x":"\\u0061"}', '{"x":"a"}'),
    ],
)
def test_distinct_argument_content_cannot_authorize_deduplication(left, right):
    call = {"type": "function_call", "call_id": "synthetic", "name": "synthetic_tool"}
    assert response_output_identity({**call, "arguments": left}) != response_output_identity(
        {**call, "arguments": right}
    )


@pytest.mark.parametrize(
    "left,right",
    [
        ('{"x":1}', '{ "x" : 1 }'),
        ('{"x":"a b"}', '{ "x" : "a b" }'),
    ],
)
def test_outer_json_whitespace_does_not_prevent_owned_output_deduplication(left, right):
    call = {"type": "function_call", "call_id": "synthetic", "name": "synthetic_tool"}
    assert response_output_identity({**call, "arguments": left}) == response_output_identity(
        {**call, "arguments": right}
    )
