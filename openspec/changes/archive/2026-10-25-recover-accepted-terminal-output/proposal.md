## Why

A public Responses stream can include `response.created` and `response.in_progress`, lose every intermediate output event, and end with a complete, owned terminal response. The normalizer currently emits a text delta without opening its item and omits function-call argument events. Strict streaming clients therefore fail or miss output even though the terminal envelope contains the answer.

## What Changes

- Recover the missing output lifecycle from a completed or incomplete terminal response when the accepted response has emitted no output events and the terminal response id matches its created envelope.
- Reuse the existing terminal lifecycle renderer, preserve one creation event, and assign increasing sequence numbers to recovered events.
- Keep already streamed output and native Codex streams unchanged; never recover a different response's terminal output into an accepted lifecycle.

## Impact

- Python public HTTP Responses normalization only; no configuration, database, or native helper changes.
- This recovers owned terminal output and does not infer ownership of dropped anonymous upstream events.
