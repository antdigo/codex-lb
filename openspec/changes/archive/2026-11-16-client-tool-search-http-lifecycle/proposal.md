## Why

The installed Bifrost 2.5.1 converts Claude Code's client-side ToolSearch and its completed history into native `tool_search_call` / `tool_search_output` items. The HTTP operation proof recognizes function calls only. Full histories containing client tool search therefore remain on the canonical prompt-cache execution queue and cannot use the pool or pre-publication transport recovery. Production reported `response_execution_session_retired`; the pinned-Bifrost synthetic regression independently reproduces missing recovery.

## What Changes

Recognize complete, paired client-executed tool search as client work in the existing operation contract. Retain its original JSON, loaded definitions, cache identity, owner, original deadline and replay quotas. Hosted tools, incomplete pairs, opaque state and visible output retain conservative behavior. This is a compatibility correction within the accepted operation/pool architecture; no database, dependencies, settings, live installation or Git integration changes are included.
