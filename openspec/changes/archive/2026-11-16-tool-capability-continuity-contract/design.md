## Required invariants

1. A recovery cannot silently erase tool definitions, completed client discovery or pending client obligations. Native, portable and durable checks use one client-call vocabulary; each policy may conservatively refuse an unsupported transfer.
2. An independently admitted full-context request does not inherit a physical lane's prior response, instructions or loaded tools. Explicit continuation authority belongs to the request and remains scoped to its verified owner.
3. Canonical prepared input and final account-scoped wire text are distinct snapshots. Reconnect/replay builds from canonical prepared data, applies account identity once and verifies the exact wire size. It never guesses provenance from a key suffix.

## Minimal architecture

Extract pure tool/input-contract logic from physical lifetime code into a service-independent module. Reuse its call-family classification and client discovery validation in account-neutral projection and durable manifests. Retain portable client-discovery items with complete loaded definitions; keep hosted/unknown/account-owned discovery outside portable recovery rather than dropping it. Preserve existing account-scoped reasoning/ID sanitization only where its dedicated proof allows it. Unsupported calls invalidate whole durable completeness instead of producing a partial manifest. The existing JSON envelope carries completeness version2; legacy or unknown envelopes grant no completeness authority. This needs no database schema migration or rewrite. Interrupted explicit client discovery uses a native empty tools result, never a string function result; supplied results remain intact.

Reuse the existing request-local pool independence property at the final conversation-state preparation boundary. Do not add a second persisted authority bit or strip capabilities to manufacture eligibility. A broad session header and a previously promoted lane cannot authorize implicit anchoring for independent work.

Keep one controller and replay ledger. Introduce explicit canonical/pre-egress attempt snapshot provenance by renaming and reusing the existing retained attempt field; final wire text remains local for exact physical sizing and send. Retry from the former; never feed the latter through identity construction again. Account changes use the existing owner/file/continuation rules, not cache optimization. Logical PCK remains stable, while account-isolated physical PCK changes only when the required account changes.

## Why this scope

Removing two names from a drop list would merely turn capability loss into unexplained refusal. Adding a special retry would leave the classifiers inconsistent. New prewarming, more lanes, stable recovery UUIDs or suffix detection do not address the identified causes and add cost or ambiguity. Existing prefix-aware idle-lane preference remains.

## Evidence and limits

Each confirmed path gets a RED route or wire regression, positive and negative client/hosted/unknown controls, and whole affected-file verification. Error-conditioned cache analysis remains separate from aggregate hit ratios: prior workload windows alone do not establish causality or current isolated mode. No live model probes or production data writes. The previous source-pinned candidate is held and never rewritten; delivery uses a separate audited runtime and code-only rollback after verification.
