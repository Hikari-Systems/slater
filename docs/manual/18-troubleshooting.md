# 18 · Troubleshooting

The error messages a user or operator is most likely to hit, what each one means,
and how to fix it. Slater's errors are deliberately legible — the message usually
names the fix.

## Write errors

These come from the writable layer. Background: [11 Writing data](11-writing-data.md).

| Message | Meaning | Fix |
|---|---|---|
| `this slater connection is read-only: the writable layer is not enabled (set delta.enabled)` | The graph is served read-only. | Start the server with `delta__enabled=true` (and grant the user `write`). |
| `unsupported write: the writable layer accepts business-key MERGE / SET / REMOVE / [DETACH] DELETE, CREATE / INSERT (GQL), and relationship writes only` | The statement's shape isn't a supported write — including a `RETURN` after a write. | Use a supported write shape; issue a separate `MATCH … RETURN` to read back. |
| `write access to graph 'X' is not granted` | The user has `read` but not `write`. | Add `write` to the user's grant for that graph in `acl.json`. |
| `cannot add label ':X' — it is not defined in the graph (only pre-existing labels can be set)` | Query-time writes can't introduce a new label. | Introduce the label at build time; only pre-existing labels can be `SET`. |
| `cannot write a :T relationship: the relationship type must already exist in the graph` | Query-time writes can't introduce a new relationship type. | Add the type at build time. |
| `cannot CREATE (:L): none of its properties is the label's range-indexed business key …` | The node has no range-indexed business key to identify it. | Add a range index on the key (build time), or use `MERGE` with an inline key. |
| `Cannot delete node, because it still has relationships. To delete it and its relationships, use DETACH DELETE.` | Plain `DELETE` won't remove a connected node. | Use `DETACH DELETE`. |
| `the vector index on (:L {p}) is N-dimensional …` | A written vector's dimension doesn't match the index. | Write a vector of the index's dimension. |

## Build and serve errors

| Message | Meaning | Fix |
|---|---|---|
| `consolidation failed: … spawn builder 'slater-build': No such file or directory` | `CALL slater.consolidate()` spawned `delta.builderBin`, which isn't on `PATH`. | Set `delta__builderBin` to an absolute path to the `slater-build` binary. |
| `load ACL … No such file or directory` | The server can't read `aclPath`. | Create the ACL file, or point `aclPath` at it (shipped default `/config/acl.json`). |
| generation refused for a missing `aclBlake3` stamp | `requireAclStamp` is on and the generation is unstamped. | Build with `slater-build --acl acl.json`, or set `requireAclStamp=false` for unstamped/dev graphs. |
| `… must be rebuilt` (format version) | The generation's `FORMAT_VERSION` is not the one this server understands. | Rebuild the graph with a matching `slater-build`. Slater has no backwards compatibility. |
| `parse error: … expected stmt` (during build) | A dump statement isn't a recognised shape — often a `//` comment or an unsupported form. | Remove comments; check the statement against [05 Building graphs](05-building-graphs.md). |
| `vector values are not supported in merge dumps` | A business-key `MERGE` dump carries a `vecf32(...)` value. | Load vectors via the CREATE/`--pk` build form or the writable layer. See [10 Vector search](10-vector-search.md). |

## Resource and value errors

| Message | Meaning | Fix |
|---|---|---|
| an `ArithmeticOverflow` error | Integer arithmetic (or `sum()`) exceeded `i64`. | Slater never silently wraps; restructure the computation or use floats. See [04 Types & values](04-types-and-values.md). |
| a `maxIntermediate` / memory-guard abort | The query retained more intermediate elements than `query.maxIntermediate`. | Narrow the query, or raise `query.maxIntermediate` if the envelope allows. See [16 Performance tuning](16-performance-tuning.md). |
| query timeout | The query exceeded `query.timeoutMs` (default 30 s). | Optimise the query or raise the timeout. |
| `toInteger(...)` errors on a value | An out-of-range or non-finite float was converted. | Use `toIntegerOrNull(...)` to get `null` instead of an error. |

## Diagnostics

When something is slow or a connection is being rejected, the live counters help.
Enable `loadTestDiagnostics=true` and read them over Bolt:

```
CALL slater.diagnostics()
```

or from the shell with the `slater diagnostics [HOST] [PORT] [USER] [PASSWORD]`
subcommand. The snapshot reports uptime, RSS, connection occupancy and rejections,
query counts and failure breakdowns, latency percentiles, and cache-pool
hit/miss/eviction stats. See
[09 Procedures & algorithms](09-procedures-and-algorithms.md).

## Next

- Back to the reference: [14 Configuration reference](14-configuration-reference.md).
- Understand the write rules: [11 Writing data](11-writing-data.md).
