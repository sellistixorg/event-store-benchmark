# Boomerang adapter

gRPC client for the Boomerang/Slingshot event store (`sellistix.slingshot.v1.EventStoreService`).

## `SLINGSHOT_CONTEXT`

Slingshot will require the gRPC metadata header `x-slingshot-context` on every call (Slingshot Phase 1 PR 2). Today's server ignores it. When that requirement lands, a missing header will be rejected with `INVALID_ARGUMENT` and reason `CONTEXT_REQUIRED`.

The adapter sets that header on each client from `connect_client`. Unary calls, streaming reads, subscribe, and calls made after the channel reconnects all include it.

| `SLINGSHOT_CONTEXT` | Header value |
| --- | --- |
| unset | `default` |
| set | the variable's value |

The value is read when the store starts and again when a client connects. It must be a legal gRPC ASCII header value: one or more bytes in the range `0x20`–`0x7E` (space through `~`) with no leading or trailing whitespace. An empty string, surrounding whitespace, tab, other control characters, `DEL`, non-ASCII bytes, or a non-Unicode environment value fails startup with an error that names `SLINGSHOT_CONTEXT`. The check runs before the adapter dials and before readiness retries, so a bad value is not reported as a connection timeout.
