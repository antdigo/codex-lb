## 1. Shared bridge reconnect ownership

- [x] 1.1 Reproduce the pre-created retry replacing a socket with an accepted or draining sibling in a bridge regression test; record the failing command.
- [x] 1.2 Apply a pending-ownership guard before replay state mutation or reconnect.
- [x] 1.3 Verify shared-socket reader failure settles every request without sending on a replacement socket; verify single-request replay remains available.
- [ ] 1.4 Run focused bridge tests, lint/type checks, broad tests within host resources, and strict OpenSpec validation; record limitations and archive only after verification.
- [x] 1.5 Audit and fence reconnect entry itself for fresh-upstream recovery and prewarm timeout ownership, with explicit warmup disposal and regression coverage.
