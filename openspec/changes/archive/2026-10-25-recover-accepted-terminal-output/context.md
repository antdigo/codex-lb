# Accepted terminal output recovery

## Evidence and scope

On 2026-10-25, five downstream bridge streams contain only response creation, in-progress, metadata, and completed events. Their terminal responses contain output items, including reasoning, messages, and function calls. Their missing intermediate events correspond to anonymous upstream matching warnings. The existing public normalizer's full terminal lifecycle recovery is restricted to a discarded oversized pre-created prelude, so an accepted response cannot use it.

## Design

Track the accepted response id and whether any output event has been forwarded. For an entirely output-free accepted lifecycle, a matching terminal id proves ownership of its complete output. Reuse `_terminal_output_lifecycle_events`; do not replay any orphan event, synthesize another creation, or recover partial output by guessing which bytes were delivered. Keep existing pre-created overflow recovery intact.

Per stream, this adds one response-id reference and one boolean; no output buffer or long-lived cache. Recovery iterates the terminal output already in memory. The ordinary stream adds only constant-size bookkeeping and no per-event collection beyond existing normalizer state. No native helper rebuild or migration is required.
