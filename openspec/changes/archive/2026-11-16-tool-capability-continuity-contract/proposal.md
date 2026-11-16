## Why

The deeper architecture review found three inconsistent contracts: account-neutral projection drops tool-discovery definitions, independent pool requests can inherit a reused lane's conversation anchor, and durable pending-call manifests can omit parallel client ToolSearch. A separate cache review identified a conditional isolated-account retry path that reapplies cache identity to an already transformed physical frame. These must be fixed as shared invariants rather than adapter-specific retries.

## What Changes

Unify the client-tool vocabulary and discovery validation used by native input, portable projection and durable completeness. Preserve meaningful capability-bearing history or refuse unsupported portability. Keep request-local independence authoritative through final preparation. Separate canonical prepared request text from the final account-transformed wire frame so retries apply identity once. Preserve existing quotas, deadlines, publication and owner fences; no new retry loops, pool-width changes, settings, schemas, dependencies or automatic activation.
