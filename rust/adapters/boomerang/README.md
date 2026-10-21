# Boomerang adapter

Client for the Boomerang/Slingshot event store (`sellistix.slingshot.v1.EventStoreService`) over gRPC, the remote procedure call protocol.

## Slingshot contract

The adapter targets the typed Slingshot version 1 contract on branch `cursor/typed-contract-cleanup-1720` at commit `924f8440458bdbbcf10e538bada4a0752b77763a` (Slingshot Phase 1, pull request 5).

Events, append conditions, queries, and business times are protocol buffer messages. The adapter does not hand-build those values as text. Optional fields are left unset when the harness has no value. The old sentinel numbers (`-1` and `0`) are not sent, because on this contract those numbers are real versions, positions, and limits.

The vendored copy of the contract is `protos/sellistix/slingshot/v1/event_store.proto`. Set `SLINGSHOT_PROTO_DIR` to replace that copy. The variable is the proto root: the compiler reads `<root>/sellistix/slingshot/v1/event_store.proto` and uses `<root>` as the include path. Slingshot's smoke check sets `SLINGSHOT_PROTO_DIR` to its own `protos` directory. `google.protobuf.Timestamp` comes from the Rust `prost-types` crate. No other Slingshot protocol file is required.

## `SLINGSHOT_CONTEXT`

Slingshot will require the gRPC metadata header `x-slingshot-context` on every call (Slingshot Phase 1 pull request 2). Today's server ignores it. When that requirement lands, a missing header will be rejected with `INVALID_ARGUMENT` and reason `CONTEXT_REQUIRED`.

The adapter sets that header on each client from `connect_client`. Unary calls, streaming reads, subscribe, and calls made after the channel reconnects all include it.

| `SLINGSHOT_CONTEXT` | Header value |
| --- | --- |
| unset | `default` |
| set | the variable's value |

The value is read when the store starts and again when a client connects. It must be a legal gRPC ASCII header value: one or more bytes in the range `0x20`–`0x7E` (space through `~`) with no leading or trailing whitespace. An empty string, surrounding whitespace, tab, other control characters, `DEL`, non-ASCII bytes, or a non-Unicode environment value fails startup with an error that names `SLINGSHOT_CONTEXT`. The check runs before the adapter dials and before readiness retries, so a bad value is not reported as a connection timeout.
