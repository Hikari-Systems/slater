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
| `consolidation failed: … this build has no consolidation worker` | The server was built without the `consolidate` feature — the `slater:latest-lite` image. | Set `delta__builderBin` to a `slater-build` binary, or use the full `slater:latest` image. |
| `consolidation failed: … spawn builder '<name>' (also tried …)` | `delta.builderBin` names a builder that is on neither `PATH` nor beside the server binary. | Fix the path, or clear `delta__builderBin` to use the server's own worker (the default). |
| `consolidation failed: … exceeded the Ns delta.consolidateTimeoutSecs budget` | The rebuild outran its wall-clock bound and was killed. Nothing was lost — the old core kept serving and the delta is still live. | Raise `delta__consolidateTimeoutSecs`, or `0` to disable. An O(core) rebuild takes ~45 min on a 91M-node graph. |
| Consolidation is killed with no message, repeatedly | The builder OOMed: its default 4 GiB budget (and a peak RSS above it) does not fit the container. | Leave `delta__builderMaxMemory` at `0` so it is derived from the cgroup limit, or set it explicitly. |
| `load ACL … No such file or directory` | The server can't read `aclPath`. | Create the ACL file, or point `aclPath` at it (shipped default `/config/acl.json`). |
| generation refused for a missing `aclBlake3` stamp | `requireAclStamp` is on and the generation is unstamped. | Build with `slater-build --acl acl.json`, or set `requireAclStamp=false` for unstamped/dev graphs. |
| `… must be rebuilt` (format version) | The generation's `FORMAT_VERSION` is not the one this server understands. | Rebuild the graph with a matching `slater-build`. Slater has no backwards compatibility. |
| `parse error: … expected stmt` (during build) | A dump statement isn't a recognised shape — often a `//` comment or an unsupported form. | Remove comments; check the statement against [05 Building graphs](05-building-graphs.md). |
| `node MERGE business key: vector values are not supported` | A `vecf32(...)` is used as a node's identity, or in an edge `SET`. | A vector may only be a node `SET` value: `MERGE (n:L {k: 'v'}) SET n.embedding = vecf32([…])`. See [10 Vector search](10-vector-search.md). |
| `vecf32 in build-time SET takes a literal vector` | The argument to `vecf32(…)` is an expression, not a list of numbers. | Write the literal form, `vecf32([0.1, 0.2, …])`; the builder does not compute embeddings. |
| `vector index <p> declared dim N but a node has M` | An embedding's length disagrees with the declared index. | Fix the dump or the declaration — the dimension is fixed at build time. |
| `vector index (:L {p}) is declared after node data` | In a CREATE/`--pk` dump the `CALL db.idx.vector.createNodeIndex(...)` sits below the first node, where pass 1's parallel routing can't see it. | Move every vector-index declaration into the dump header, or pass `--vector-index-json`. See [10 Vector search](10-vector-search.md). |
| `warning: vector index L.p matched no node` (build still succeeds) | The declaration is fine but nothing matched it — usually a label/property typo, or embeddings missing from the dump. | Check the spelling against the dump; the index is real but every KNN over it returns nothing. |

## At-rest encryption errors

These only occur on a deployment with a master key configured. Note that `FORMAT_VERSION`
did **not** change for these, so the `… must be rebuilt` row above will not match — an
encrypted image built before per-file AEAD binding fails on its `aadScheme` instead.

| Message | Meaning | Fix |
|---|---|---|
| `encrypted image declares aadScheme …, but this build seals blocks under "file-block-v1"` | The image predates per-file/per-ordinal AEAD binding. Its blocks are sealed under a scheme this server cannot verify, so opening it safely is impossible. | Rebuild the graph with a matching `slater-build` and republish. Plaintext images are unaffected. |
| `… manifest carries no MAC` / `MAC did not verify` | With a key configured, the generation manifest, every sealed segment's `SEGMENT.json` and the `sets/<uuid>.json` pointer must each carry a valid keyed MAC. A missing one is a strip downgrade; a mismatched one is a forged or altered document. | Rebuild and republish under the key. There is deliberately no flag to accept a MAC-less document under a key. |
| `block file is not sealed, but a master key is configured` (or `isam index …`) | A plaintext block file or range index was found inside an image whose manifest declares encryption — a keyed build seals every file it writes, so this is a substitution, not a mixed image. | Republish the generation. If it appeared on a shared mount or bucket, treat it as tampering rather than corruption. |
| `… is plaintext but a master key is configured` (WAL or L0 segment) | The writable layer's on-disk artifacts are unsealed. On a running deployment that is the strip downgrade; on an **upgrade**, it is simply a delta written before at-rest sealing existed. | Consolidate (or otherwise drain the delta) on the previous version *before* upgrading. Failing that, delete the graph's WAL and L0 directories — which discards every write not yet folded into the core. |
| `WAL dir … is missing N segment(s)` | A segment was removed from the middle of the run; replaying across the hole would return a silently shorter history. | Restore the segment, or remove the whole WAL directory (losing every unconsolidated write). Removing part of it is never safe. |

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
