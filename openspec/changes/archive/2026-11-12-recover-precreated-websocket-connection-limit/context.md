The HTTP bridge already rotates eligible idle prompt-cache sockets before the
upstream lifetime limit. Busy or owned sockets can outlive that rotation window.
An explicit connection-limit rejection before response acceptance supplies a
bounded recovery opportunity without changing the request's account owner.

For example, Claude Code can submit its full reasoning and tool history through
Bifrost on an existing prompt-cache socket. If that socket rejects response.create
with the explicit lifetime error, the bridge sends the same context once on a new
socket for the same account. An unavailable owner, expired deadline, detached
consumer or a second transport failure terminates the request.

This policy does not replay an accepted response after a service-restart close
(1012). Such a turn may already have generated output or performed tool work.
Raw downstream WebSocket recovery keeps its existing policy.
