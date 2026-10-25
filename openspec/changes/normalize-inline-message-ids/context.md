## Reproduction and rationale

A completed assistant output_text message can carry a synthetic msg_ ID that
causes the subscription backend to attempt unsupported persisted-item lookup.
The qualifying message already contains its complete plaintext content, so
classic subscription egress can omit that top-level ID while preserving content.

The function-call pass also handles these completed messages in the same walk.
Incomplete, empty, unknown, reference-only and encrypted items keep their IDs.
Encrypted reasoning remains paired with its real ID. Native WebSocket, Lite,
forwarding and ownership classification retain their previous contracts.

## Validation and limits

Tests cover caller immutability, wire conversion, public HTTP paths and original
ownership after trim, reconstruction and replay. Fresh current-main results and
request-path measurements are recorded in the PR. This change makes no claim
that all replay IDs are unnecessary or that encrypted content can be rewritten.
Private histories and production runtime files are excluded.
