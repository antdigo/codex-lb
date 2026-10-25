## Example

Input history system(rules), user(task), system(budget) used to become instructions="rules\nbudget", input=[user(task)]. Appending another budget update then changed instructions before all replayed history.

The normalized shape is now instructions="rules", input=[user(task), developer(budget)]. Appending another turn preserves those instructions and the serialized old-input prefix.

## Validation evidence

The implementation was developed and deployed against codex-lb v1.24.0. A controlled comparison on one WebSocket session observed 72,448 cached tokens on an identical repeat in both versions. On an appended turn, baseline normalization read zero cached tokens; prefix-preserving normalization retained 72,448 cached tokens.

The combined compatibility patch was subsequently checked through official Bifrost v2.3.2. The appended-turn request reported 72,448 cached tokens out of 72,683 total input tokens (99.68%). These are specific observations; routing, account selection, time, and backend behavior can still affect cache reuse.

## Scope

This change adds no session cache, retained request content, configuration, or dependencies. It changes the existing normalization pass rather than adding another traversal. The messages alias keeps its prior coercion behavior; the tested gateway supplies Responses input.
