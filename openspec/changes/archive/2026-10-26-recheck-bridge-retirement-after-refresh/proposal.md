## Why

An HTTP bridge reuse check can pass before the awaited durable lease refresh. An older response can detach during that await and mark the bridge retiring. The lookup then returns a condemned socket, so the next unsubmitted public HTTP request receives a preventable `502 HTTP responses session bridge is retiring` before any upstream dispatch.

## What Changes

- Recheck retirement after refreshing a reusable bridge, before handing it to a new unreserved request.
- Use the existing lookup replacement path, including durable owner and preferred-account constraints.
- Preserve an already reserved unanchored handoff and retain the final submit retirement fence.
- Reproduce the cancellation/refresh race through the public Responses route with mock upstream transport.

## Impact

No replay after upstream dispatch, new retry loop, settings, database schema, or native protocol changes. The common reuse path adds one retirement flag check; no new retained per-request data or buffers.
