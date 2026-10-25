# Recover public Responses streams after an oversized unowned prelude

Claude Desktop receives `upstream_stream_truncated` when the public SSE
normalizer aborts after 65 events without a response envelope, even when the
source subsequently contains a valid completed response. The observed failure
has the same synthetic created/failed envelope and sequence numbers. Its native
upstream prelude was not recorded, so this change fixes the independently
reproduced failure mode without claiming a proven native disconnect cause.

Keep the existing 64-event retention limit. On overflow discard the entire
unowned prelude and stop retaining additional events, but continue consuming the
existing bounded upstream request until a real response envelope or EOF.
Preserve fail-closed behavior for EOF with no terminal response. When a terminal
response is the first owned envelope after overflow, expose its owned output as
SDK-safe item/content/argument lifecycle events before the terminal event.

No new configuration, retries, account switching, cache-key changes, DB changes,
or changes to the existing plaintext item-ID compatibility patches.
