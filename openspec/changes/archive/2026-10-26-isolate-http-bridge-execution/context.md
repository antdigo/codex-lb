# Design and operational context

The current response-create gate is released on response.created, which permits
another response.create while the first generation still executes. A new execution
gate must remain independent: account/global create leases still end at acceptance.

Approaches considered:
- Serial admission before send (chosen): avoids upstream queue acknowledgement ambiguity, keeps
  the warm prompt-cache socket, and uses existing bounded local queue/deadline rules.
- A request-specific timeout after sending: requires a terminalized draining owner,
  late anonymous-error handling and fencing successor durable ownership. Wider and
  harder to prove than avoiding the second send.
- Fork every concurrent prompt-cache request onto another socket: retains physical
  parallelism but causes additional cold contexts and consumes more connection slots.

One semaphore per bridge and one nullable ownership pointer per request are bounded
by existing session and queue limits. Acquire before account/create admission; retain
ownership during safe recovery on the same session. Release after terminal processing,
or fenced cancellation/failure cleanup. New waiters must never dispatch on a retiring
generation after an owner cancellation or terminal bookkeeping failure.

The subscribed backend's multiplexing support is unproven. Public OpenAI docs describe
FIFO within a default lane and parallelism with explicit named stream_id lanes, which
this application does not implement. This fix does not add an unverified protocol.

Example: A generates for ninety seconds. B waits locally and exceeds its admission
budget. B receives an explicit local wait error, A completes normally, and C can reuse
the same warm socket. B is never sent, so no late B response needs guessing or replay.

Local admission/release microbenchmark on the test host (5000 iterations, same dependencies) measured a median 60.4us before and120.4us after; concurrent test load produced substantial variance. This measures local admission only, not model latency or TPS. Retained state is one semaphore per bounded bridge and one nullable owner reference per bounded request; no history cache and no per-chunk buffer is added.
