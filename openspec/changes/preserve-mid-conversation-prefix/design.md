## Decision

Keep the existing instruction-hoisting pass, but bound it to the initial instruction prefix. An ordinary input item closes that prefix. Preserved typed system/developer directives retain their existing handling. A retained non-text attachment from leading instruction content becomes a user input item and also closes the prefix.

After the prefix closes, preserve instruction-message placement. Shallow-copy later system messages with role developer, which the subscription backend accepts; leave later developer messages in place. Do not mutate caller input or remove token-budget reminders.

Responses Lite retains its existing bypass. Responses and compact share the normalizer; compact truncation and replay-safety transformations remain independent. The messages alias is unchanged; the observed Bifrost path supplies input.

## Alternatives

Removing reminders loses context. Adding session state or synthetic cache markers hides the normalization problem. Replacing the gateway or adapter expands deployment scope without fixing this local transformation.

## Verification

Compare instructions and serialized input before and after appending a turn, including tool history and late reminders. Exercise both HTTP route families, trailing slashes, and streamed/collected responses with a mocked subscription boundary. Check mixed text/attachment prefixes and requests without initial instructions.

Controlled live cache measurements are supporting observations, not a guarantee of backend cache hits. See [context.md](context.md).
