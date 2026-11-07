# Isolate HTTP bridge response execution

Two concurrent HTTP requests sharing a prompt-cache WebSocket failed at 2026-10-26
06:55 UTC. The second create was not acknowledged within sixty seconds, and the
connection-wide watchdog aborted the first response while it streamed tool arguments.

Serialize execution on each existing HTTP bridge socket before dispatch, independently
of the response-created gate and account admission. A locally waiting request may
time out or be cancelled without affecting the executing request. Keep the socket,
account, request body, cache key and existing admission budgets.

No schema, dependency, dashboard, configuration or Bifrost changes. Direct downstream
WebSocket behavior is unchanged. Scope is one response execution owner per HTTP
bridge; separate physical sockets retain concurrency.
