## Implementation and verification
- [x] Trace the exact Bifrost error to a 300-second execution wait behind a successful 330-second response.
- [x] Confirm the existing isolated checkout and clean 29-test execution baseline.
- [x] Reproduce the ordinary wait failure with virtual time and both public Responses routes, with and without API-key authentication.
- [x] Use the ordinary request/stream-idle budgets without the pre-response cap; preserve short prewarm and retained-deadline limits.
- [x] Run focused and related regressions, lint/type/architecture checks and strict OpenSpec validation; sync and archive the verified spec.
- [x] Obtain independent gpt-6-astra/max review and prepare a sealed code-only deployment with the current runtime as rollback target.
