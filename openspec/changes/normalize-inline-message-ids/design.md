## Decision

Extend the existing bounded inline-ID wire pass rather than adding another traversal. A message qualifies only when type is message, role is assistant, status is completed, content is a nonempty list of output_text objects with string text, and neither encrypted_content nor encrypted_function_args is nonempty. Omit only its top-level id. Keep every field of that normalized wire copy unchanged. The pre-existing assistant content coercion, which can discard annotations, remains separate and unchanged.

Incomplete/unknown/encrypted/reference shapes remain unchanged. Responses Lite, including metadata-only continuation, keeps its bypass. Rename the internal wire opt-out to strip_inline_item_ids and the existing evidence flag to has_original_inline_item_id to describe both messages and function calls. Direct client WebSocket dispatch/fresh replay and replay-safety serialization disable the entire inline-ID pass.

## Ownership

Capture original IDs on function_call/message input before wire stripping, consistent with the existing fail-closed replay classifier. Carry the single boolean through all existing preparation states. Only the already-proven explicit account-neutral projection may reset it. Forwarding and same-account replay keep their existing behavior. No request content is retained in the flag.

## Evidence and limitations

An authorized same-key A/B/A replay observed reject → completed → reject when changing only message IDs. Encrypted reasoning and call/output pairing were unchanged. Twelve synthetic checks and a fresh-namespace real-history check had passed before this comparison; those were insufficient to establish compatibility in the affected reused session. Details and serializer costs belong in context.md and the PR test plan; private histories and ciphertext are excluded from this change.
