# Slater — design decisions log

Append-only. Each entry mirrors a `// DESIGN:` comment in the code so the
rationale survives even if a file is rewritten. Newest at the bottom.

---

### D1 — `hs-utils` via git+tag `v0.16.0`, `default-features = false`
House convention is `git + tag`, never a path/workspace dependency, even though
Slater is itself a workspace. `config`/`logging`/`healthcheck` are not
feature-gated in `hs-utils`, so `default-features = false` gives us exactly those
without dragging in the actix-web/sqlx stack the data services use. Pinned to the
latest tag (`v0.16.0`); the layered-config + logging API we use is unchanged from
the siblings' `v0.10.0`.

### D2 — Cargo fetches git deps via the system git CLI
`.cargo/config.toml` sets `net.git-fetch-with-cli = true`. In this environment the
git CLI's HTTPS path to GitHub works whereas raw libgit2 transport does not, so
this makes the `hs-utils` dependency resolve reliably.

### D3 — Bolt-native healthcheck, not `hs_utils::healthcheck`
`hs_utils::healthcheck::run` sends an HTTP `GET /healthcheck` and checks for
`HTTP/1.1 200`. Slater's port speaks Bolt, not HTTP, so that probe would always
fail against us. `slater::health::probe` instead performs the Bolt handshake
(magic preamble + four version proposals) and treats a non-zero negotiated
version as healthy. Same stdlib-only, pre-runtime, `healthcheck` subcommand shape
the house uses; same Docker `HEALTHCHECK CMD ["/app/slater","healthcheck"]`.

### D4 — `Value::Vector` is a first-class type, distinct from `List`
A `vecf32([...])` literal becomes `Value::Vector(Vec<f32>)`, not a generic
`List` of floats, so it can be routed to the vector store and round-tripped to the
similarity index with dimensionality preserved. Homogeneous scalar arrays use
`Value::List`.

### D6 — Properties stored row-per-entity, not strictly column-per-property
`node_props.blk` / `edge_props.blk` keep one record per entity (its whole property
map), addressed by dense id. The dominant read is "materialise a matched entity's
properties for a `RETURN {…}` map projection", which this serves in a single block
read. PLAN.md says "column-oriented"; row-per-entity better fits the actual query
shape. Per-property scans (rare, un-indexed aggregations) read entity records; the
ISAM indexes cover the selective cases.

### D7 — String property *values* stored inline; symbol tables live in the MANIFEST
Rather than a global value dictionary (`dictionary.blk`), string property values
are encoded inline in the entity record (zstd collapses the repetition within a
block) so materialising an entity needs no extra dictionary block reads on the hot
path. Only the small, bounded symbol sets — labels, relationship types, property
keys — are interned to ints, and they live directly in the MANIFEST (resident).
`dictionary.blk` is therefore not emitted in v1; revisit if a graph ever has a huge
high-cardinality-but-repeated string column where global dedup would pay off.

### D8 — Forward + reverse CSR in one `topology.csr.blk`
Records `0..N` are outgoing adjacency, `N..2N` incoming, in dense-node order.
`node_count = total_records / 2`. One file, one reader, both directions.

### D9 — Blockfile global record addressing (no per-entity side table)
The block directory carries each block's record count, so the reader builds a tiny
`O(num_blocks)` prefix-sum index at open and maps a global record index (= dense
entity/node id, by append order) to `(block, slot)` with a binary search. This is
what lets `columns` and `topology` look an entity up by id without a separate
`id → location` table resident.

### D5 — Container base is debian-slim; `aws-lc-rs` build deps required
Per the approved decision we follow house style (`debian:bookworm-slim`) rather
than musl/distroless, which lets us use the C-backed `zstd` crate freely. The
rustls default crypto backend is `aws-lc-rs`, which needs `cmake`, `clang`, and
`libclang-dev` in the builder stage. The
M9 Dockerfile must install these. (Switching rustls to the pure-Rust `ring`
backend is an alternative if we ever want to drop the C toolchain — revisit only
if the Docker build proves painful.)

### D10 — `vectors.f32.blk` is grouped by index; `firstRecord` lives in the descriptor
The single vector store holds every index's vectors back-to-back, one index group
at a time; `VectorIndexDesc.first_record` records where a group begins and `count`
how long it is, so the reader fetches exactly the contiguous range
`[firstRecord, firstRecord + count)` for a brute-force scan with no per-record
dispatch. Each record is `node_id ‖ dim ‖ dim×f32`: the node id rides alongside so
a KNN hit maps straight back to a dense node, and `dim` is stored per record so the
store is self-describing and a dimension mismatch is caught at read time rather
than trusted from the MANIFEST.

### D11 — `node_labels.blk` is the forward (node → labels) store; postings deferred
`columns` holds only properties, so a separate per-node store carries which labels
each node has (`uvarint(count) ‖ count×uvarint(label_id)`, addressed by dense node
id). This answers `labels(n)` and `n:Label` predicates with one block read during a
scan. The *inverted* postings (label → nodes) that seed a selective label scan are
a different access pattern; that `labels.post` file is built in M4 when the
executor needs it. M3 produces only the forward store.

### D12 — `vecf32` is routed to the vector store only when an index covers it
A node's `vecf32` value goes to `vectors.f32.blk` only if a vector index is
declared on a `(label, property)` the node actually carries; otherwise it stays
inline as a `Value::Vector` column value. This keeps the build lossless (an
unindexed vector is still returnable from props) while ensuring every indexed
embedding is in the store the KNN path reads. In the live dumps every embedding has
a declared index, so embeddings are always routed out of the column store.

### D13 — Streaming statement splitter cuts on top-level `;` at byte level
The dump reader pulls bytes from a `BufRead` and emits one statement at a time,
never slurping the (potentially huge, multi-paragraph-markdown) script. It splits
on `;` only outside a string literal, tracking single/double quotes and `\`
escapes. Scanning is byte-level: the delimiters (`;'"\`) are ASCII and UTF-8
continuation bytes are always `>= 0x80`, so a multibyte character can never be
mis-split or mistaken for a delimiter.

### D14 — Atomic publish: temp dir → fsync → hash → MANIFEST last → rename → swap `current`
A generation is written into `.tmp-<uuid>/`, every data file fsynced by its writer;
then the per-file BLAKE3 inventory is hashed, the MANIFEST (carrying the content
hash) is written **last**, the temp dir is fsynced and atomically renamed to
`<uuid>/`, and finally the `current` pointer — a small text file holding the
generation uuid — is swapped via write-temp-then-rename. Writing the MANIFEST last
means a half-written generation has no MANIFEST and is ignored; the content hash is
the copy-completeness guard the reader validates on open (matches the NFS-rsync
failure mode in the plan).

### D15 — The pest grammar is the dump dialect only, not the query language
`primitive_cypher.pest` parses just the handful of statement shapes a dump script
contains (node/edge create, node/edge range index, the two vector-index forms) plus
the ignorable marker/cleanup/drop lines, which parse-and-discard. It is deliberately
*not* the read-query grammar (that is a separate parser in `slater`, M4). The dump
marker index (`__DumpVertex__`/`__dump_id__`) parses as an ordinary range index and
is dropped in the builder. Range index files are named `node_<label>_<prop>.isam` /
`edge_<type>_<prop>.isam` (labels/types/keys are identifier-safe, so this is also a
safe filename).

### D16 — A generation opens all readers eagerly; block bytes stay lazy
`slater::generation::Generation::open` opens every reader (`columns`/`nodelabels`/
`topology`/`vectors` + each range ISAM) at open time, not on first use. Each reader's
`open()` reads only its footer / sparse top-level — kilobytes — so eager opening adds
no meaningful resident cost, keeps the type free of interior mutability (so it is
trivially `Sync` for sharing across Bolt tasks behind an `Arc`), and surfaces a
corrupt/short file immediately rather than mid-query. "Lazy" in the plan is honoured
at the *block* level: block bytes are still fetched by `pread` and decompressed on
demand (no mmap, no slurp), and from M4.2 routed through the bounded block LRU.

### D17 — Inverted label/reltype postings are built in-memory at generation open
The forward stores (`node_labels.blk`, the CSR) answer "what labels/edges does this
node have"; the executor's selective scans need the *inverse* ("which nodes carry
label L", "which edges are type T"). Per D11 these are built in memory at open by a
single forward pass: `label_id → ascending node ids` from `node_labels`, and
`reltype_id → ascending edge ids` from the forward CSR (each edge appears once in the
outgoing adjacency). No `labels.post`/`reltypes.post` file is emitted — the in-memory
index is `O(N + M)` to build and is the resident selectivity structure for the
generation's lifetime. If a graph ever outgrows an in-memory postings map this is the
place to spill to an on-disk postings file, but the live estate fits comfortably.

### D18 — Block LRU keys on the generation UUID alone; decompressed blocks; BTreeMap LRU
`slater::cache::BlockCache` keys a cached block by `(gen_uuid_u128, file_code, block)`.
The plan's key is `(graph, gen, file_id, block_id)`, but a generation UUID is globally
unique, so it already subsumes `(graph, gen)`: two graphs can never share a generation
id, and a generation swap changes the UUID — which orphans every stale entry for free
(the result-cache "gen UUID in key → swap orphans stale" trick, applied to blocks).
`file_code` is a small `u32`: 0–4 for the fixed files, `0x8000_0000 | i` for the i-th
range index, so range indexes never collide with fixed files. The cache holds
**decompressed** block bytes (`Arc<Vec<u8>>`) — `graph_format::blockfile` exposes
`parse_block`/`record_from_block` exactly so a record can be sliced out of a cached
block with no second decompress. Eviction is true LRU via a monotonic tick +
`BTreeMap<tick,key>` ordering (O(log n)/access) rather than a hand-rolled intrusive
list — simplicity and obvious correctness win here. The loader runs **outside** the
mutex so a slow `pread`+decompress never serialises other readers; a concurrent
double-miss dedups to one `Arc` on insert. A single block larger than the whole budget
is retained (never evicted to empty) so reads always make progress. Hit/miss/eviction
counters are atomics for lock-free metric reads. (Threading the *typed* readers
through the cache happens in M4.5 when the executor reads records; M4.2 ships the cache
and its `record()` routing over `BlockFileReader`, tested against a real multi-block file.)

### D19 — Bolt wire layer is split into four independently-tested modules
`slater::bolt` is `packstream` (PackStream v2 value codec), `handshake` (preamble +
version negotiation), `chunk` (length-prefixed framing + `00 00` terminator) and
`message` (request decode / response build). Each is pure and unit-tested in
isolation, so the protocol is verifiable without a live socket. Notable choices:
- **PackStream**: hand-rolled, big-endian, smallest-int encoding; maps are an
  *ordered* `Vec<(String, PsValue)>` so the wire output is deterministic (stable
  tests, stable metadata). Only the tiny-struct form (`0xB0..0xBF`) is emitted —
  every Bolt message has < 16 fields — though the decoder rejects unknown markers.
- **Handshake**: supports `(5,4)` then `(4,4)` in preference order; honours the
  Bolt ≥4.3 `range` byte (`[0, range, minor, major]`) so a single proposal can offer
  a span of minors. A valid hello with no common version returns the four-zero
  "no version" reply rather than erroring (only a bad preamble errors).
- **Messages**: `RUN` is the only 3-field message; `HELLO`/`LOGON`/`PULL`/`DISCARD`/
  `BEGIN` carry one metadata map (kept as `PsValue::Map` so the loop reads whatever
  keys it needs); the rest are zero-field control messages. Auth lives in `LOGON`
  (Bolt 5.x), not `HELLO`. Unknown tags and wrong arity are decode errors → the
  connection loop answers `FAILURE`.

The per-connection `tokio` state machine + TLS acceptor that drive these modules are
deferred to after the ACL (M4.4) and executor (M4.5) they depend on; the wire layer
itself is complete here.

### D20 — ACL: argon2id PHC hashes, hot-reload that keeps the last-good file
`slater::acl` parses the JSON ACL into `Acl { users: { name → { passwordArgon2id,
grants } } }` (unknown keys like the sample's `_comment` are ignored). Passwords are
stored as argon2id **PHC strings**; `hash_password` (exposed as the `slater
hash-password` subcommand, wired in `main` before the runtime starts, mirroring the
healthcheck pattern) mints them with a random salt and the argon2 crate's default
params. `verify` runs a dummy verify on the unknown-user path so a missing account is
not distinguishable by response timing, and a malformed stored hash logs and rejects
rather than erroring. Grants are per-graph and only `"read"` is meaningful today;
`can_read(user, graph)` gates both the `db` select and every query. `AclHandle` wraps
`RwLock<Arc<Acl>>`: handlers take a cheap `snapshot()` per request while the
background poller swaps the active ACL underneath them. `reload()` re-reads and, on a
parse/IO error, **keeps the last-good ACL** and logs loudly (a fat-fingered edit on
the shared mount must never lock everyone out); `poll()` gates `reload()` on the
file's mtime so it is cheap to call on the generation-poll interval. The initial
`AclHandle::load` *does* error — a server must not start with no usable ACL.

### D21 — Read-Cypher parser: a pest grammar with atomic, word-boundary keywords
`slater::parser` (`cypher.pest` + `parser.rs`) is the ONLINE query grammar, separate
from `slater-build`'s dump dialect. Key choices, several learned the hard way:
- **Every keyword is an atomic (`@{}`) rule with a trailing `!ident_cont`.** In a
  non-atomic rule pest inserts implicit whitespace between sequence elements — so a
  silent `kw = _{ ^"or" ~ !ident_cont }` consumes the space *before* the boundary
  check and then `or` matches the `OR` inside `ORDER` (and a real `1 OR 2` fails).
  Making the keyword rules atomic suppresses that whitespace so the boundary holds.
  Atomic rules surface as leaf tokens, so lowering routes child iteration through a
  `kids()` filter that drops them.
- **Write/procedure clauses parse, then are rejected in the translator.** A
  `forbidden_query` alternative matches `reading_clause* ~ forbidden_clause ~ ANY*`
  (consuming the rest so the *parse* succeeds), and lowering raises a clear "Slater
  is read-only; the 'CREATE' clause is not permitted" — far better than an opaque
  syntax error. Genuine syntax errors still fail at parse.
- **Precedence is encoded structurally** (or→and→xor→not→comparison→add→mul→unary→
  postfix→primary), so lowering is a straight tree-walk with no Pratt table.
- **Parameter names may be reserved words** (`$limit`), so `parameter` uses an
  unreserved `param_name`, unlike bare variables which are reserved-checked.
- Literals reuse `graph_format::ids::Value`; strings are unescaped at lowering.

The planner + volcano executor that consume this AST, and the `tokio` Bolt listener
that ties generation+cache+bolt+acl+parser+executor together, are the next M4.5
increment.

### D22 — Planner narrows candidates; the executor re-checks every predicate
`slater::plan::choose_node_scan` is a pure function picking how to generate the
*anchor* node of a pattern: range-index equality → range-index range → smallest
label posting → full node sweep. Crucially it only ever **narrows** the candidate
set — the executor (`exec`) re-applies every label and inline/`WHERE` predicate to
each candidate it is handed — so a worse plan costs time, never correctness. That
is what lets the planner plan on **literals only** and ignore parameters: a
`$param` predicate simply isn't used for index selection and falls through to a
scan that the executor then filters. The planner reads the open generation so it
only selects an index/label that actually exists, and for a multi-label anchor it
chooses the label with the smallest in-memory posting (D17). Only the *anchor* uses
the planner; every other node in a pattern is reached by CSR traversal from a
already-bound neighbour, so its candidate set is the neighbourhood, not a scan.

### D23 — Executor: backtracking matcher + materialising pipeline, reads via the cache
`slater::exec::Engine` runs an AST `Query` against one `Generation` + `BlockCache`.
Design points:
- **Records are read through the block cache (D18).** Each typed reader now exposes
  its underlying `BlockFileReader` (`inner()`) and a public record decoder
  (`columns::decode_props`, `nodelabels::decode_labels`, `topology::decode_adj`), so
  the executor routes node/edge/label/topology reads through `BlockCache::record`
  and slices the record out of an already-decompressed (often resident) block — no
  second decompress, hot blocks stay warm across queries and connections. Range
  (ISAM) lookups go straight to the `IsamReader` (a different on-disk structure, not
  block-cached). A test asserts a second identical run adds zero cache misses.
- **Runtime value `Val`** extends the stored `Value` with `Node(id)`/`Rel(id)` (lazy
  references resolved against the generation) and `Map` (for map projection / map
  literals). It carries a deterministic total order (`cmp_total`, numbers compared
  numerically, `NaN` via `total_cmp`) for `ORDER BY`/`DISTINCT`/grouping, and a
  three-valued `loose_eq` for Cypher `=`/`<>` (`null` propagates). An embedding
  routed out to the vector store (D12) reads as `Null` from a column access —
  vector *values* are the M5 KNN/`similarity()` path, not a scalar column read.
- **Matching is depth-first backtracking.** A `MATCH` expands each existing row by
  binding the anchor (via the planner) then walking the relationship chain over the
  CSR; direction, type alternation and relationship-property predicates filter each
  hop. Variable-length (`*min..max`) is a DFS with **relationship uniqueness** (no
  edge reused within a path) emitting every endpoint whose depth is in range; an
  open-ended `*` is capped at `MAX_VARLEN_HOPS = 15` so a dense graph can't blow up
  (explicit upper bounds are honoured exactly). `OPTIONAL MATCH` emits the row with
  the new variables `null` when nothing matches.
- **The projection pipeline materialises** (`project`): it is `WITH`/`RETURN`-shared
  and does star-expansion → simple-or-aggregated projection → `DISTINCT` →
  (`WITH`) `WHERE` → `ORDER BY` → `SKIP` → `LIMIT`. Aggregation groups by the
  non-aggregating items (a `BTreeMap<GroupKey,_>`, so group order is deterministic);
  aggregates inside a larger expression (`sum(x)/count(*)`) are handled by collecting
  the aggregate nodes pre-order and replaying them through an `AggCursor` during a
  single `eval`, so indices line up without rewriting the tree. `ORDER BY` keys see
  the projected aliases merged over the input row (alias wins). Result size is bounded
  by `max_rows`; traversal time by an optional wall-clock deadline. (Streaming
  early-`LIMIT` pushdown into the leading scan is a later optimisation — correctness
  is identical, and the headline memory budget is graph residency, not result rows.)
- **`UNION[ ALL]`** runs each part, checks equal column arity, concatenates, and
  dedups across the whole result unless `ALL`.

The `tokio` Bolt listener + per-connection state machine + TLS that drive this
executor (decode `RUN`/`PULL`, PackStream-encode rows, enforce the ACL grant per
query) are the remaining M4 increment.

### D24 — The Bolt listener: shared cache, blocking execution, buffered streaming
`slater::server` is the final M4 increment — the `tokio` listener and
per-connection state machine that ties `generation`+`cache`+`bolt`+`acl`+`parser`+
`exec` together. Design points:
- **One shared `BlockCache` across every graph and connection.** Per D18 the cache
  key is the (globally unique) generation UUID, so a single byte-budgeted pool
  already isolates graphs and orphans a swapped generation's blocks for free — no
  need for a per-graph cache. `Graphs::open_all` discovers every `<data_dir>/<name>/`
  that carries a published `current` pointer and opens+validates it at boot; a
  corrupt/incomplete generation fails the whole boot (the copy-completeness guard).
- **Execution *and* row-encoding run on `spawn_blocking`.** The planner/executor and
  its `pread`s are synchronous, and encoding a returned `Node`/`Relationship`
  resolves its labels/type/properties through the same block cache (more blocking
  IO). Doing both inside one blocking task keeps all storage IO off the async
  reactor; the async side only frames and writes. `Arc<Generation>` + the shared
  `Arc<BlockCache>` move into the task; `max_rows`/`timeout_ms` come from config
  (`query.maxRows`/`query.timeoutMs`) into the `Engine`.
- **Buffered streaming.** `RUN` executes, buffers the (already `max_rows`-bounded)
  result, and replies `SUCCESS {fields}`; the following `PULL` drains the buffer as
  `RECORD`s then a final `SUCCESS {has_more}` (honouring `PULL`'s `n`). The headline
  memory budget is graph residency (the caches), not result rows, so buffering the
  result set is acceptable; true storage→wire streaming is a later optimisation.
- **Bolt FAILED-state semantics.** Any `FAILURE` (auth, forbidden graph, syntax,
  read-only, execution) puts the connection into a failed state where every message
  but `RESET` is answered `IGNORED`, exactly as the neo4j drivers expect. `GOODBYE`
  closes; `RESET` clears failed/streaming state.
- **Auth at `LOGON` (5.x) and embedded in `HELLO` (4.4 fallback).** Only the `basic`
  scheme is accepted; both paths share one `authenticate()` that verifies against the
  ACL (`acl.poll()` first, to pick up an out-of-band edit) and records the user. A
  5.x `HELLO` has no `scheme` and merely opens the connection. Every `RUN` selects a
  graph (explicit `db` in the metadata, else the user's sole readable graph) and
  enforces the per-graph `read` grant before parsing — codes:
  `Security.Unauthorized`/`Security.Forbidden`/`Database.DatabaseNotFound`/
  `Statement.SyntaxError`/`Statement.AccessMode` (read-only)/`Statement.ExecutionFailed`.
- **`server` agent string `Neo4j/5.4.0 (Slater <ver>)`.** Kept with the `Neo4j/`
  prefix so the official drivers' agent-sniffing treats us as a modern Bolt server,
  while still naming Slater honestly. TLS is optional (`rustls` acceptor when a
  cert+key are configured; plaintext on loopback for dev). `main` builds the
  multi-thread `tokio` runtime *after* the stdlib-only `hash-password`/`healthcheck`
  subcommands and hands off to `server::serve`.

### D25 — `Val::Rel` carries endpoints + type so a Bolt `Relationship` is materialisable
A Bolt `Relationship` structure needs the edge's id, **start/end node ids**, type
and properties; the executor previously bound only the edge id (`Val::Rel(u64)`),
which cannot reconstruct endpoints without an O(M) reverse index. Rather than keep a
resident edge→endpoints table (which would undercut the bounded-memory goal),
`Val::Rel` now carries `{ id, start, end, reltype }`, captured for free at traversal
time where the neighbour is already known. Crucially `start`/`end` are the edge's
**stored** direction (src→dst), independent of which way the pattern walked it — so a
relationship reached by an incoming or undirected pattern still reports the true
graph direction (test: `relationship_value_carries_type_and_stored_endpoints`). This
also enables the planned-but-missing `type(r)` function. `expand_one_hop` now returns
a `Hop { edge, neighbour, reltype, start, end }` (computing start/end from the
traversal direction), and variable-length paths carry `Vec<Hop>` so every edge in a
`*`-expansion materialises as a full relationship. The Bolt encoder (`exec::Val` →
`bolt::packstream::PsValue`) maps `Node`/`Relationship` via new public
`Engine::node_record`/`rel_record` (label/type/property resolution through the cache),
emitting the Bolt-5 element-id struct fields only when the negotiated major version is
≥ 5; a stored vector value encodes as a PackStream list of floats (Bolt has no native
vector type).

### D26 — Vector KNN: one CALL allowed through, brute-force, distance-as-score
The estate is entirely below the 50k-vector ANN threshold, so the *real* read path
is brute force; Vamana/PQ (`AnnMode::Vamana`) is M7. `slater::vector` is the
`AnnMode::BruteForce` arm: a pure `brute_force_knn(entries, query, k, metric)` over a
slice of `VectorEntry`s, so the scoring + top-k selection is unit-testable against a
hand-computed reference independently of the store/cache plumbing.
- **The parser admits exactly one procedure.** The read grammar still rejects every
  `CALL`, *except* `db.idx.vector.queryNodes`: `forbidden_clause`'s `call` branch now
  carries a negative lookahead `!(ws+ ~ vector_proc)` so that one form falls through
  to a real `vector_call_clause` (a reading clause that binds its `YIELD` outputs,
  like a `MATCH` introduces variables) while `CALL db.labels()` etc. still reject as
  read-only. `YIELD ... WHERE` is supported (FalkorDB allows it). The label/property
  args must be string literals; `k` and the query vector are expressions (so `$param`
  works), and the query vector is a `vecf32([...])` literal — `vecf32` is now an
  executor function building a first-class `Val::Vector` (a `$param` arrives as a
  numeric list and is coerced).
- **`score` is the distance, ordered ascending** (nearest first), mirroring
  FalkorDB's `queryNodes` contract — a smaller score is a closer match. For a cosine
  index that distance is `1 - cosine_similarity`, in `[0, 2]`; ties on score break by
  ascending node id so a query is deterministic. The companion scalar `similarity(a,
  b)` returns the complementary cosine *similarity* in `[-1, 1]` (so `score == 1 -
  similarity(query, node)`); a zero-norm vector has similarity `0` (not `NaN`), making
  it maximally distant.
- **The candidate set is the index group, read through the block LRU.** The vector
  store groups an index's vectors contiguously (D10) and only indexed `(label,
  property)` embeddings are routed there (D12), so reading `[first_record, count)`
  *is* the label-filtered candidate set with no separate label scan. The executor
  reads each record via `BlockCache::record` over a new `VectorStoreReader::inner()` +
  public `decode_vector` (mirroring `columns`/`nodelabels`/`topology`), so the group's
  blocks stay warm across repeat queries (a test asserts a second identical KNN run
  adds zero cache misses). A query/index dimension mismatch is a hard error.

### D27 — The result cache: a third LRU pool, generic, gen-UUID-keyed
`slater::cache::ResultCache<V>` is the third cache pool (alongside the block LRU; the
vector-index pool is M7), with its own `result_cache_bytes` budget and the same
tick + `BTreeMap` LRU machinery and atomic hit/miss/eviction metrics as the block
cache. It is **generic over the stored value** so `cache` carries no dependency on the
executor's result type and the pool is unit-testable in isolation; `server`
instantiates it over `exec::QueryResult`.
- **Key = `(generation UUID, normalised query + params)`.** The gen UUID is in the
  key on purpose (as for blocks, D18): a generation swap mints a new UUID, so every
  stale entry is orphaned for free and a result from a swapped-out generation can
  never be served. The query is normalised by collapsing whitespace; parameters are
  appended in name-sorted order (`\u{1}`-delimited, which can't occur in a query).
- **All queries are cached, including vector ones; the key's bytes are charged to the
  budget.** PLAN.md flags the choice of normalising large inlined-`vecf32` literals vs
  skipping vector queries — we normalise and cache them, but `insert` adds the key's
  string length to the value's byte estimate, so a big inlined-vector key pays for the
  memory it occupies and the pool stays bounded. Like the block LRU, one oversized
  entry is retained rather than evicting to empty.
- **The cache stores the version-independent `QueryResult`, not encoded rows.** On a
  hit the rows are re-encoded for the connection's negotiated Bolt version (element-id
  fields gate on major ≥ 5, D25), so two connections at different versions share one
  entry. Both execution *and* encoding still run on `spawn_blocking` (encoding resolves
  node/rel records through the block cache — D24); only successful results are cached.

### D28 — Encryption at rest: per-block XChaCha20-Poly1305, decrypt-before-decompress
At-rest encryption is **optional and per block**, sealed *after* compression so the
on-disk bytes of every block are ciphertext and the block LRU keeps holding
plaintext-decompressed blocks — the executor, KNN and result-cache paths are entirely
unaware encryption happened. The new `graph_format::crypto` module is the seam:
`BlockCipher` wraps pure-Rust `chacha20poly1305::XChaCha20Poly1305` (NOT the C/aws-lc
stack, keeping the crypto musl-clean), with `encrypt`/`decrypt`, a `random_nonce`, and
`from_master(master_key, salt)` deriving the block key via `BLAKE3::derive_key` over
(master key ‖ salt). Hex helpers live here too (no `hex` crate in the tree).
- **The key never touches the data directory.** The MANIFEST `EncryptionHeader` records
  only `aead`/`kdf` identifiers and the per-generation random `salt_hex`; the runtime
  *master key* arrives out of band (`slater-build --encrypt --key-file|--key-env`, or
  the server's `config.encryption{keyFile|keyEnv}`, both hex). A **per-generation** key
  is derived from (master key ‖ salt), so two generations off the same master key use
  independent block keys and the salt alone weakens nothing.
- **Per-block random nonces, stored beside the block — never the key.** XChaCha20's
  24-byte nonce is wide enough that fresh random nonces never realistically collide.
  `blockfile` gains an encrypted magic `SLBLKE01` whose directory entries are 24 bytes
  wider (they carry the nonce); `read_block` does `pread → decrypt(nonce) → decompress`
  on a cache miss, and `BlockFileWriter::create_with_cipher` does the inverse. Each typed
  store (`columns`/`nodelabels`/`topology`/`vectors`) gained an `_with_cipher` ctor that
  threads `Option<Arc<BlockCipher>>`; the plaintext ctors delegate with `None`, so M2–M5
  fixtures and the golden test keep their byte layout unchanged.
- **ISAM range indexes are encrypted too, and their sparse top-level is *also* sealed.**
  An ISAM index leaks key material differently from a `.blk`: its resident top-level
  stores each block's first key in the clear. So the encrypted form (`SLISME01`) seals
  every data block under its own nonce **and** seals the whole top-level under one more
  nonce carried in a widened footer — otherwise the first key per block would sit in
  plaintext on disk. This means a wrong key fails at *open* (the top-level tag check),
  whereas a wrong key on a `.blk` opens but fails on the first block read. Encrypting
  ISAM as well (not only the `blockfile` choke point the milestone named) is deliberate:
  a half-encrypted generation with plaintext range-index values is not "encryption at
  rest". So an `--encrypt` image has **no** plaintext data file.
- **Refusal is clean, never garbage.** An encrypted file opened without a key is refused
  at open with a precise error; a wrong key fails the AEAD tag (a clear "wrong key or
  corrupt/tampered block", not a panic or silent misread). `Generation::open_with_key`
  derives the cipher from the header + runtime key (refusing an unknown AEAD/KDF, or an
  encrypted generation with no key) and hands it to every reader; `Generation::open`
  delegates with `None` (plaintext only). A plaintext generation opens with or without a
  key present, so encryption stays optional end to end.

### D29 — PQ on normalised vectors, squared-L2 ADC, exact cosine re-rank
The large-vector ANN path quantises each vector into `m` product-quantisation codes
(`k = 2^bits` centroids per subspace, k-means trained) so the navigation set is
~`m` bytes/vector and can stay resident. For a **cosine** index every vector is
L2-normalised before training/encoding, and the PQ estimate is the **asymmetric**
squared-L2 distance (ADC: the query stays full-precision; a small per-query lookup
table `table[s][c]` holds the query sub-vector's squared-L2 to each centroid, and a
candidate's estimate is `m` table look-ups keyed by its codes). On unit vectors
squared-L2 is `2 − 2·cos`, i.e. monotonic in cosine distance, so navigating by the
PQ estimate ranks candidates identically to cosine — while the **final re-rank uses
the exact cosine distance** on the full vectors, so the `score` returned matches the
brute-force arm's contract (D26) exactly. Training on normalised vectors keeps the
codebooks in the space the estimate works in. k-means uses a tiny deterministic LCG
(no `rand` dependency) so the same vectors always produce the same codebooks.
v1 builds Vamana only for **cosine** indexes and requires `pq_subspaces` to divide
the dimension; anything else above the threshold falls back to brute force with a
note on stderr. (`graph_format::pq`.)

### D30 — Vamana adjacency stores global indices; the reader derives `(block, slot)`
The `.vamana` block file packs one record per node — `node_id ‖ full vec ‖
adjacency` — laid out in **BFS-from-medoid order** for locality (a walk touches few
distinct blocks). The plan says "rewrite adjacency to block-relative `(block_id,
slot)`"; we instead store each neighbour as its **global vamana index** (= its
record position) and let the reader map an index to its `(block, slot)` via the
blockfile's existing resident prefix-sum directory (`BlockFileReader::locate`).
Reason: storing the pair on disk is circular to size — records are variable-width
(uvarint neighbour fields), so a neighbour's block boundary depends on the very
field widths that encode it. Storing the index sidesteps that entirely, costs no
extra resident memory (the directory is already resident and tiny), and still yields
block-relative addressing + per-block read coalescing at read time. The medoid in
the MANIFEST `AnnMode::Vamana` is recorded as its post-BFS-permutation index.
(`graph_format::vamana`.)

### D31 — Full vectors live in the `.vamana` blocks, not also in `vectors.f32.blk`
An above-threshold index's vectors are written into the `.vamana` blocks (for the
exact re-rank) and PQ-encoded into the `.pq` file (for navigation) — and are **not**
also appended to `vectors.f32.blk`. The `AnnMode::Vamana` arm never reads that store,
so duplicating the full vectors there would only waste space; `VectorIndexDesc`
`first_record` (a `vectors.f32.blk` offset) is therefore irrelevant for a Vamana
index and is recorded as `0`. Below-threshold indexes are unchanged — full-precision
in `vectors.f32.blk`, the M5 brute-force path. The build gathers each index's vector
set first, then routes by cardinality (`shared.rs` `PendingIndex` → `build_vamana_index`).

### D32 — The vector-index pool: a second cache pool, resident PQ pinned + Vamana block LRU
`slater::cache::VectorIndexCache` is the **second** cache pool (alongside the M4
block LRU and the M5 result LRU), with its own `vector_cache_bytes` budget. It holds
two things under one budget: the **resident PQ codes** for each `(label, property)`,
**pinned** (per the milestone `// DESIGN:` — the resident set is PQ codes only, never
a full in-memory graph; pinned entries are charged to the budget but never evicted),
and an **LRU of the 1–2 MiB Vamana blocks** the beam search pages in for the frontier
+ exact re-rank. Keeping it separate from the block LRU means the large-vector path
cannot evict hot graph blocks and vice versa. Like the block LRU it keys on the
generation UUID (so a swap orphans stale entries — D18) plus the index's ordinal in
`manifest.vector_indexes`. The server pins every generation's resident PQ at startup;
`exec::Engine::vamana_knn` then runs the generic `vamana::beam_search` — navigating by
the resident PQ ADC estimate (no IO) and reading a node's block per expansion through
the pool's `record()` (so popping several nodes in one block reuses the one
decompressed block — the coalescing D30 relies on) — and re-ranks the beam exactly.
The resident PQ + a bounded block LRU keep RSS flat regardless of index size, the
headline guarantee (exec test `vamana_knn_matches_brute_force_with_bounded_vector_cache`:
a 2000-vector index ≫ the pool budget gives recall@10 ≥ 0.8 while the pool never pages
in the whole store). The `Engine` carries the pool as an `Option` set via
`with_vector_cache`, so the brute-force arm and non-vector queries are untouched.

### D33 — Generation guard: poll, `RwLock<Arc<Generation>>` swap, exit-via-bail
The in-flight guard for a `current` pointer that changes under a running server
(`slater::server`, M8). **Poll, not inotify** — the data dir is an NFS mount (D14/
D16), so a single background tokio task sweeps every graph every
`generation_poll_ms` (default 5 s) and compares each graph's on-disk `current` UUID
(read cheaply via `Generation::current_uuid`, which parses only the small pointer
file — never opens the generation) against the live `Generation`'s UUID.
- **Interior mutability per graph.** `Graphs` now holds
  `HashMap<String, RwLock<Arc<Generation>>>` (plus the retained `data_dir` +
  `master_key` so the guard can re-open a graph). `get()` returns an
  `Arc<Generation>` *snapshot* a query holds for its whole life, so a concurrent
  swap — which only replaces the slot's `Arc` under the write lock — **never mixes
  two generations within one query**. A plain `RwLock` (not `arc-swap`) keeps the
  dependency set unchanged; the lock is held only for the pointer clone/replace, so
  contention is negligible.
- **`swap` strategy: open → validate → pin new → swap → unpin old.** On a changed
  UUID, `swap_if_changed` opens the new generation with the **same content-hash
  copy-completeness guard** as boot (`Generation::open_with_key`), so a
  truncated/half-rsynced copy errors *at open* and the old generation is kept
  serving — a corrupt swap can never take the server down or serve garbage. On a
  valid open it pins the new generation's resident PQ into the `VectorIndexCache`,
  atomically swaps the slot's `Arc`, then unpins the old generation's PQ. The order
  is safe because an in-flight query holds its own `Arc<Generation>` (and thus its
  own resident-PQ `Arc`) to completion — the unpin only drops the *pool's* clone and
  frees its budget; the gen-UUID-keyed block/result/PQ caches orphan the old
  generation's entries for free (D18/D27/D32).
- **`exit` strategy: signal, don't `process::exit`.** The default logs fatal and
  must exit non-zero so the orchestrator restarts cleanly. Rather than
  `std::process::exit` (which would bypass the runtime and is untestable), the guard
  sends the changed graph's name down a `tokio::sync::oneshot`; `serve`'s accept
  loop `select!`s on it and `bail!`s, so `main` returns `Err` and the process exits
  non-zero through the normal path. The decision core (`guard_sweep` → `SweepAction`)
  is a **pure synchronous function** (the swap does blocking IO, so the async task
  wraps it in `spawn_blocking`), unit-testable without timers or sockets; the async
  `spawn_generation_guard` only adds the poll timer + the shutdown wiring. Per-graph
  errors inside a sweep are logged, never propagated, so one bad graph can't stall
  the guard for the others.
- **`reload_strategy` parsed at boot.** `AppConfig::reload_strategy()` maps the
  config string to a `ReloadStrategy` enum and **errors on an unknown value**, so a
  fat-fingered strategy fails fast at startup rather than silently defaulting.

### D34 — Bounded-RSS *headline* test: lib+bin, in-process server, growth-bounded assertion
The project's raison d'être is **flat resident memory bounded by the cache
budgets, independent of graph size** (M9, PLAN "Memory (headline)"). M7 already
proved the *accounted* residency is capped deterministically (the pool's byte
counters); M9 adds the real-OS-RSS-under-load assertion the plan calls the
headline. Three coupled decisions made it land cleanly and non-flakily:
- **`slater` is now a library + thin binary.** A new `src/lib.rs` exposes the
  modules (`pub mod server/bolt/cache/exec/…`); `main.rs` shrinks to load
  config/logging, build the runtime, and call `server::serve`. This is the only
  way a `crates/slater/tests/` integration crate can drive the *real* server
  in-process and sample `/proc/self/statm` (a binary-only crate exposes nothing to
  link against). It is also idiomatic and unblocks all future integration tests.
- **`serve` split into a bind step + `serve_with_listener(cfg, listener)`.** The
  test binds an ephemeral `127.0.0.1:0` loopback port itself (so it learns the
  address — `serve`'s fixed config port can't give an ephemeral one back), then
  hands the listener to `serve_with_listener`, which runs the **production wiring**:
  graph open + integrity validation, ACL load, the three cache pools at the
  *configured* (tiny) budgets, resident-PQ pinning, and the generation guard. So
  the test exercises the real path, not a mock. A `// DESIGN:` comment marks the
  split in `server.rs`.
- **The assertion is growth-bounded + a generous absolute ceiling, not a tight
  absolute bound.** Real-OS RSS is dominated by a fluctuating process baseline
  (tokio + rustls + loaded `.text` + allocator arenas ≈ tens of MiB) that no
  portable formula predicts, which is exactly why M7 deemed unit RSS sampling
  flaky. So the test: builds a synthetic above-threshold Vamana/PQ generation whose
  `.vamana` store (~1.2 MiB) is ~5× the vector-cache budget (256 KiB) — so the pool
  **must** page and the caches saturate during a 30-query warm-up — then drives 150
  more distinct cosine-KNN (+ occasional `MATCH`) queries and asserts (1) ANN
  recall@10 ≥ 0.7 vs brute-force ground truth (so the bounded RSS is real, not an
  empty search), (2) **peak − warm-up RSS ≤ budgets + 48 MiB slack** (the rigorous
  one: once the caches are saturated, further growth can only be a leak / unbounded
  accumulation — observed growth is ~0), and (3) peak RSS < a 512 MiB headline
  ceiling. Distinct inline `vecf32([…])` literals make every query a result-cache
  miss (real work). `N` is kept modest because the *fixture's* Vamana build is the
  slow part (~30 s for 4 000 nodes), not the property under test — the bound holds
  identically at any scale; a 100× store lands in the same RSS envelope. The
  fixture mirrors `slater-build`'s output via the public `graph-format` API rather
  than reusing `slater::testgen::write_vamana` (which is `#[cfg(test)]`-private to
  the crate and so invisible across the integration boundary).

### D35 — Container & ops: workspace multi-stage image, read-only root, Bolt healthcheck
The `Dockerfile`/`docker-compose.yml`/`README.md` follow the house `*-data-service`
conventions adapted for a **three-crate workspace shipping two binaries**:
- **Builder stage installs `cmake` + `clang` + `libclang-dev`** for the rustls
  `aws-lc-rs` backend (D5); `git` (already in `rust:1-bookworm`) plus the copied
  `.cargo/config.toml` satisfy the `hs-utils` git+tag fetch via the git CLI.
- **Workspace dep-cache layer.** The single-crate stub trick is generalised: copy
  the workspace `Cargo.toml` + `Cargo.lock` + each crate's `Cargo.toml`, synthesise
  a stub `lib.rs`/`main.rs` per crate (graph-format lib; slater-build bin; slater
  lib **and** bin), `cargo build --release --locked` to cache the dependency graph,
  then drop the stub artefacts and build the real `--bin slater --bin slater-build`.
- **Slim runtime, non-root, Bolt healthcheck.** `debian:bookworm-slim` +
  `ca-certificates`, `appuser:1000`, both binaries + `config.json` + `acl.json`
  copied in. `slater` is the `ENTRYPOINT`; `slater-build` is the alternate command
  (`--entrypoint /app/slater-build`, and a `profiles: [build]` `builder` service in
  compose). `HEALTHCHECK CMD ["/app/slater","healthcheck"]` — the probe speaks Bolt
  (a handshake), **not** HTTP (D2). `EXPOSE 7687`.
- **Read-only root filesystem.** Compose sets `read_only: true` + `tmpfs: [/tmp,
  /run]` (Slater never writes); `/data` is the read-only NFS generation mount,
  `/sandbox` the read-only config overlay + secrets (acl.json, TLS PEM, at-rest key
  file). Env overrides use the hs-utils `KEY__sub` (double-underscore) convention
  matching the camelCase config keys. The README documents the mounts/env table and
  a worked example: build a graph with `slater-build`, connect with the neo4j JS
  **and** Python drivers, run a `MATCH … RETURN` and a cosine-KNN query.

### D36 — GQL path restrictors are scoped to the variable-length walk (GQL track PR 2)
GQL's `WALK`/`TRAIL`/`ACYCLIC`/`SIMPLE` prefix a MATCH pattern and control node/edge
reuse along a path. Slater's `varlen` (`exec.rs`) already owns the natural scope for
this — it is the one place that threads a per-path `used` edge set — so PR 2 maps the
restrictors onto that walk rather than inventing a whole-pattern uniqueness pass.
- **`Pattern.restrictor: Option<PathRestrictor>`; `None` ≡ today's behaviour.** The
  field is additive, so every existing pattern construction is unaffected. The
  executor folds `None` onto `Trail` (`walk_mode`), because slater's `*` has *always*
  been edge-unique — i.e. a bare `*` is already a TRAIL. So absence of a restrictor
  and an explicit `TRAIL` run the identical code path, and **only `WALK` relaxes**
  uniqueness; `ACYCLIC`/`SIMPLE` add node-uniqueness.
- **Mode → uniqueness.** `WALK`: no check (bounded only by `max`/`MAX_VARLEN_HOPS`,
  the budget and the deadline — a cycle would otherwise expand without limit).
  `TRAIL`: no repeated edge (`used`). `ACYCLIC`: no repeated node, endpoints included
  (`visited`, seeded with the walk's start). `SIMPLE`: no repeated node *except* the
  two endpoints may coincide — a hop back to the start is emitted but not extended, so
  the start can never become an interior repeat. Node-uniqueness implies
  edge-uniqueness, so `ACYCLIC`/`SIMPLE` track only `visited` and `TRAIL` only `used`;
  each mode's per-hop cost stays minimal and the `Trail`/default path is byte-for-byte
  as before.
- **Restrictor requires a variable-length relationship (PR 2 scope).** On a fixed hop
  or node-only pattern there is no `varlen` scope to attach to, so a restrictor there
  is **rejected** with a clear message rather than silently ignored. Honouring
  restrictors over fixed-length chains is later work. A pattern with *several* varlen
  relationships gives each its own independent scope (not one scope spanning the whole
  path) — also acceptable for PR 2 and revisitable later.
- **Restrictor over a quantified group is rejected.** `TRAIL ((x)-[:R]->(y)){1,3}`
  parses but is rejected at lowering: PR 1 desugars a quantified group into the union
  of separate fixed-length expansions, which cannot share one uniqueness scope, so the
  restrictor's intent can't be honoured across the repetitions. Reject (clear message)
  beats silently dropping it. The later fix is a dedicated repeater that threads one
  `used`/`visited` set instead of desugaring when a restrictor is present.

### D37 — GQL shortest-path selectors share `shortestPath()`'s BFS core (GQL track PR 3)
GQL's `ANY SHORTEST` / `ALL SHORTEST` / `SHORTEST k` prefix a MATCH pattern and pick
shortest connecting paths between the pattern's two endpoints. Rather than add a second
traversal, PR 3 generalises the BFS that already backed the `shortestPath()` function
into one shared core (`select_paths`, `exec.rs`) and routes both callers through it.
- **`Pattern.selector: Option<PathSelector>` (`AnyShortest`/`AllShortest`/`ShortestK`);
  `None` ≡ the ordinary matcher.** Additive, like the PR 1/PR 2 pattern fields, so every
  existing construction is unaffected. A selected pattern is routed out of `apply_match`
  *before* the streaming/quantified/restrictor paths to its own handler
  (`apply_match_selected`).
- **One BFS core for both callers.** `select_paths(src, dst, rel, bounds, selector)`
  returns the chosen paths as hop-lists in walk order: `AnyShortest` → ≤1 path,
  `AllShortest` → every path of the single minimum length, `ShortestK(k)` → up to `k`
  paths in non-decreasing length order. `shortestPath()` is now exactly `AnyShortest`
  between two *bound* nodes — it validates its wrapped pattern as before, then delegates,
  so the two can never diverge. Paths are **loopless** (no repeated node), matching
  `shortestPath()`'s long-standing simple-path search and bounding the walk on a cyclic
  graph. BFS explores layer-by-layer, so every entry in a layer has the same hop count
  and paths surface in non-decreasing length order — the property `AllShortest`/`ShortestK`
  rely on; a path is never extended past `dst`.
- **Endpoints need not be pre-bound (the real generalisation over `shortestPath()`).**
  Each endpoint is either a node already bound by the seed/an earlier clause, or a free
  endpoint **scanned** by the usual planner strategy and filtered by `node_ok` (its
  labels + inline props). The selector then runs per `(src, dst)` pair. A shared endpoint
  variable (`(a)-[*]->(a)`) is kept consistent by the same `loose_eq` guard the ordinary
  matcher uses.
- **WHERE is applied *after* selection**, per produced path (consistent with how the
  ordinary matcher applies a clause `WHERE` to a completed binding). So a selector finds
  the shortest paths first, then filters them by the endpoint/`WHERE` predicates — not a
  shortest-path-subject-to-`WHERE` search. Acceptable and predictable for the read subset.
- **Scope (PR 3): a single relationship, like `shortestPath()`.** A multi-relationship
  selected pattern, a selector combined with a path restrictor, a relationship property
  filter, and a selector sharing its clause with a comma-joined pattern are all
  **rejected** with clear messages (future work). A selector over a quantified group is
  rejected at lowering (same reasoning as D36). `SHORTEST 0` is rejected as meaningless.

### D38 — GQL label boolean expressions reuse one `LabelExpr` AST (GQL track PR 4)
GQL extends label/type predicates beyond Cypher's `:A:B` (AND) and `:T1|T2` (rel
alternation) to full booleans `!` > `&` > `|` with parentheses. PR 4 is the one PR
with AST churn, deliberately sequenced last so the pattern AST (PRs 1–3) had settled.
- **Sugar lowers into the same tree — no special cases.** The grammar makes both
  `labels` and `rel_types` a `":" ~ label_expr` precedence climb; `:A:B` parses with
  the `:` as an AND connector (→ `And`) and `:T1|T2` / `:T1|:T2` as `Or`. So every
  pre-GQL query produces an ordinary `LabelExpr` and there is no parallel code path for
  the classic forms. The WHERE postfix predicate `n:A:B` (`label_pred` →
  `Expr::HasLabels`) is a *different* rule and keeps its AND-only form — out of scope,
  smaller blast radius.
- **One `LabelExpr` enum (`Atom`/`And`/`Or`/`Not`) for both node labels and
  relationship types.** `NodePat.label_expr: Option<LabelExpr>` and
  `RelPat.type_expr: Option<LabelExpr>` (`None` ≡ no constraint, the additive
  default that leaves every other construction site untouched, as in D34/D36/D37).
  Reusing the same enum rather than a parallel `type_expr` type meant a single
  evaluator and a single grammar. A relationship carries exactly one type, so its
  expression is evaluated over the singleton present-set `{this edge's type}` — `:A&B`
  is then correctly always empty, `:!T` excludes one type.
- **No three-valued logic.** A label is present or absent on a node (a relationship has
  its one type or not), so `eval` is plain boolean recursion over a present-predicate.
  An atom naming a label/type the symbol table doesn't know is simply *absent* — so
  `!Unknown` holds and `Unknown` fails, the sound set-membership answer.
- **The single-positive-atom fast path is preserved end to end.** This is the common
  `(:Person)` / `-[:KNOWS]->` case and must not regress:
  - Planner: `choose_from_preds` reads `node.required_labels()` — the *conjunctive
    positive atoms* (`A&B`→{A,B}; `A|B`,`!A`→{}). For `:A`/`:A:B` this equals the old
    `node.labels`, so existing plans (LabelScan / index pick) are byte-for-byte
    unchanged; a disjunction/negation yields no required label → full scan + `node_ok`
    re-check (sound, because `node_ok` always re-checks the whole expression).
  - `node_ok`: a lone positive atom the anchor scan already guaranteed skips the label
    record decode entirely; only a boolean expression decodes once and evaluates,
    folding the guaranteed labels into the present-predicate.
  - `expand_one_hop`: untyped / single `:T` / `:T1|T2` alternation pre-resolve (via
    `positive_atoms`) to a flat reltype-id set so the per-edge loop stays the pre-GQL
    `ids.contains` integer test; only `&`/`!` falls to per-edge `eval`.
  - The single-node count/group fast paths gate on `as_single_atom`, taking the
    posting/index shortcut only for the lone-atom case and falling back otherwise.

### D39 — GQL `FOR x IN list` lowers onto the existing `UnwindClause` (GQL track PR 5)
GQL spells `UNWIND list AS x` as `FOR x IN list` — the operands reversed. The grammar
adds a `for_clause = { kw_for ~ alias ~ kw_in ~ expr }` (reusing the already-defined
`kw_in`) to `reading_clause`, and `for` joins the reserved set so it can't be a bare
identifier. The parser's `lower_for_clause` reads alias-then-expr and returns the
**identical** `UnwindClause` as `lower_unwind_clause` — so past the parser the two
spellings are the same AST and the executor (`apply_unwind`) is untouched. This is the
same additive, lower-onto-existing-capability discipline as the rest of the track: no
new clause type, no new executor path.

### D40 — Optional `GQL` / `CYPHER` dialect prefix is stripped in the server, no-op routing (GQL track PR 5)
Neo4j selects dialect with a query-string prefix (`CYPHER 5` / `CYPHER 25`), never a
protocol field; GQL arrives over the same Bolt `RUN`. Slater mirrors this with
`strip_dialect_prefix` (next to `normalize_query` in `server.rs`): a leading `GQL` /
`CYPHER` keyword (case-insensitive, at a token boundary), optionally followed by a
single bare numeric version token (`5`, `25`, `5.0`), is consumed before anything
inspects the statement, so the USE check, Memgraph detection, introspection and the
parser all see the bare query.
- **Stripped in the server layer, not `parser::parse`.** This keeps `parser.rs`
  language-agnostic — it never learns there is a dialect concept. The prefix is a
  transport/routing nicety, which is a server concern.
- **Routing is a deliberate no-op.** One parser serves both Cypher and the GQL subset
  today (the whole track is a superset grammar), so the dialect selector records nothing
  and changes no behaviour — it exists for client compatibility and forward room. A
  following query keyword (`CYPHER MATCH`) and an identifier merely sharing the prefix
  (`cypher_score`) are left untouched; a bare query is byte-for-byte unaffected.

### D41 — GQLSTATUS surfaced additively in Bolt metadata (GQL track PR 5)
ISO GQL defines GQLSTATUS status objects; Neo4j surfaces them in Bolt `SUCCESS` /
`FAILURE` metadata alongside the legacy `code`/`message`. Slater does the same **purely
additively** — no existing key is removed or renamed, because deployed neo4j drivers
read `code`/`message`/`has_more`.
- **FAILURE:** `message::failure_gqlstatus` adds `gql_status` + `status_description` to
  the existing `code`/`message` map. `Failure::gqlstatus` maps the Neo4j code to a GQL
  SQLSTATE-style class: `42000` (syntax error or access rule violation) for a malformed
  or read-only-rejected statement, `50000` (general processing exception) otherwise. The
  description follows GQL house style (`error: <condition>. <message>`).
- **SUCCESS:** the *final* PULL / DISCARD SUCCESS (the one completing the statement)
  carries `gqlstatus_completion`: `00000` (successful completion), or `02000` (no data)
  on an empty result. Intermediate PULL successes (`has_more = true`) are unchanged,
  since the statement isn't complete. The low-level decode-error `failure()` path keeps
  its legacy form (not a query status).

### D42 — GQL `CAST(expr AS TYPE)` lowers onto existing conversion functions (GQL track PR 5)
A survey of the value-conversion surface found slater's scalar conversions
(`toInteger`/`toFloat`/`toString`/`toBoolean`, each already NULL-on-failure) and the
temporal constructors (`date`/`localtime`/`localdatetime`/`duration`, single-argument)
already cover GQL's typed-value targets — there is no genuine coercion *gap*, only a
missing surface form. So GQL `CAST` is implemented as a parser lowering, not new
executor code: a `cast_expr` grammar rule (tried before `function_call`, backtracking
cleanly for a `cast(…)` without the `AS TYPE` tail) and `lower_cast`, which maps the
type name to the matching function and emits an ordinary `Expr::Function` — `INTEGER`/
`INT`→`toInteger`, `FLOAT`/`DOUBLE`/`REAL`→`toFloat`, `STRING`/`VARCHAR`→`toString`,
`BOOLEAN`/`BOOL`→`toBoolean`, plus `DATE`/`LOCALTIME`/`LOCALDATETIME`/`DURATION`. The
same additive discipline as D39. Exotic GQL types (zoned temporals, typed lists,
user-defined types) are deferred — they would need genuine new conversion logic.

### D43 — Writable layer reads through a generic `ReadView` seam, not a `dyn` overlay
The writable layer (the `writeable` track) overlays a delta on the immutable core
*below* the executor's read surface (option A — storage-reader overlay, not
executor-level merge). The executor and planner read the graph through the
`ReadView` trait (`crates/slater/src/read_view.rs`), which lifts the ~30 methods
they called inherently on `Generation` (the six readers + `.inner()`, the symbol
lookups both directions, the count/marginal accessors, `range_index`/
`property_histogram`/`vamana_index`, the two scans, `manifest`/`uuid`) plus two new
handles: `delta()` (the overlay, empty for a bare generation) and
`core_generation()`. `Generation` implements it as an identity pass-through;
`MergedView` overlays a `DeltaSnapshot`. `Engine` is made **generic**
(`Engine<'g, V: ReadView>`) rather than holding a `&dyn ReadView`: monomorphisation
means the read-only path compiles to `Engine<'_, Generation>` — byte-identical
codegen to before the seam existed, no vtable — and the empty-delta path
(`Engine<'_, MergedView>`) inlines its forwards to the core. The `delta_overlay`
bench (`--features testkit`) confirms the empty-delta arm sits within noise of the
core arm. `ReadView: Send + Sync` so a view is still usable by the rayon fan-out
readers. Keeping the whole surface in one trait is what lets a future delta overlay
be added purely inside `MergedView`'s method bodies without touching the executor.

### D44 — WAL durability has two seams: a local floor and object-store shipping
The write-ahead log is split across two seams with **contradictory contracts** that
must never be folded together (`crates/slater-delta/src/wal.rs`;
`docs/WRITABLE-PLAN.md`). `WalSink` is the **local durability floor**: ordered,
append-structured, fsync-durable at sub-millisecond latency, and **not
parameterised by the storage backend** — a record never travels through
`ObjectStore`, and a Bolt `SUCCESS` is returned strictly after the group-commit
`sync()` that covers it. `ObjectStore` is used **only** to ship *sealed* WAL
segments as numbered, immutable, content-addressed objects (S3/GCS have no append),
with a `wal/HEAD` pointer written last as the copy-completeness barrier — the same
pointer-last discipline as `current` in `write_manifest_and_publish` (D14), reusing
`ObjectStore::put` verbatim with no WAL-shaped trait methods added. So `fs`/`s3`/
`gcs` governs only the shipping tier; the floor is always local. This is a *core*
concern, not a clustering one: even a plain local-disk + S3 single-writer
deployment needs it to get its un-consolidated write tail off the writer node
(durability + read-replica visibility). Consequences: a local segment is not
retired until its PUT is acked; freeze ships the frozen tail before spawning the
consolidation builder; the writeback interval is simultaneously the object-store
RPO and the cross-replica read-visibility lag; and the writer node therefore needs
a durable local volume, not ephemeral instance storage.

### D45 — Writable-layer create is spelled `MERGE`; `MATCH … SET` is update-only
Phase 2c adds node *creation* to the writable layer (`crates/slater`). The write
grammar (`cypher.pest` `write_statement`) accepts two anchor keywords with distinct
create-semantics: `MERGE (n:L {k:v}) SET n.p = x` **creates** the node when the
business key `k=v` is absent from the core (else patches it in place — upsert by
business key), while `MATCH (n:L {k:v}) SET …` addresses an **existing** node only
and errors on an absent key (the error points at `MERGE`). `MERGE … DELETE` is
rejected. This keeps the layer honest to openCypher (a bare `MATCH` that matches
nothing is a no-op, never a silent create) while giving creation the spelling that
matches Slater's identity model — the builder already compiles business-key `MERGE`,
and consolidation serialises the merged state back to `MERGE` (D-less; see
`consolidate.rs`). Mechanically a `MERGE` create resolves its business key to
`KeyResolution::Absent` and writes with `resolved = None`; the memtable allocates a
**synthetic dense id** past the core's `node_count` (`Memtable::with_synthetic_base`
+ `born`), deterministic across WAL replay because allocation follows first-seen
(= replay) order. Rejected alternative: overloading `MATCH … SET` to create on a miss
(smaller change, but a create-on-miss `MATCH` is a real openCypher surprise). Also
considered and deferred: a distinct `CREATE` clause (most honest create/update split,
but two write grammars to carry and `CREATE` on an existing unique key would have to
error anyway — `MERGE` subsumes it).

### D46 — Relationship writes: `MERGE (a)-[:R]->(b)` create, `MATCH …-[r:R]->… DELETE r`
Phase 3c adds relationship writes to the writable layer (`crates/slater`). The grammar
(`cypher.pest` `write_statement`) gains an `edge_write` alternative, tried *before* the
node arm because both start with a `(node)` prefix — a node write only reaches its arm
when no relationship follows the anchor. Two shapes, as narrow as the node write: a
single directed `-[:R]->` (one type, no variable-length, no edge properties — validated
at lowering, reusing the read grammar's `rel_pattern`) between two single-label,
single-business-key endpoint node patterns. `MERGE (a:L {k:v})-[:R]->(b:M {j:w})`
**creates** the edge (create-if-absent by edge identity), **auto-creating an absent
endpoint node** as a delta-born node — the openCypher MERGE-on-a-path semantics, and it
falls out of the memtable's `endpoint_dense_or_create`. `MATCH (a:L {k:v})-[r:R]->
(b:M {j:w}) DELETE r` removes one (the rel variable is required — it names the edge;
`DETACH` is a node concept and rejected). Two deliberate constraints, both surfaced as
clear errors: (1) **the relationship type must already exist in the core** — the
traversal read overlay maps a born edge's reltype *name* to a core reltype id, so a
brand-new type would be invisible to `:R` traversal (mirrors the born-node rule that a
label must pre-exist); (2) **a `MERGE` of an edge whose endpoints are both existing core
nodes is deduped against the core** (`server::core_edge_exists` scans the source's
`outgoing_adj` over an empty-delta view) so it does not add a born duplicate of a core
edge — a born-vs-born duplicate is already impossible because the memtable is idempotent
by edge identity. Edge *properties* are deferred: `WalOp::UpsertEdge` carries a reserved
`patches` field and `EdgeDelta` a `patches` map, but the grammar creates topology only
for now. Rejected alternative: keying the delete on a `MATCH`-bound edge dense id
(a full traversal to bind `r`), rather than the edge business key — the business key
`(src, reltype, dst)` is the stable identity the whole delta layer binds to, so the
delete resolves it directly, no traversal needed.

### D47 — `CALL slater.consolidate()` is a write-layer statement, not a read `read_proc`
Phase 5 makes consolidation client-reachable: `CALL slater.consolidate()` folds the
writable delta into a fresh generation and swaps it in, returning the new generation's id
as a `generation` column. **It is parsed as a `parser::parse_statement` entry** (a new
`ast::Statement::Consolidate`, matched by its own SOI/EOI-anchored `consolidate_call`
grammar rule, tried before the node/edge write shapes) — **deliberately *not* added to the
read grammar's `read_proc` whitelist**, even though the plan's shorthand suggested "like
the other CALLs". Reasoning: `read_proc` (and its `dbms.procedures` self-report) is
documented as read-only, and consolidation mutates — mixing a write proc into the
read-only carve-out would misrepresent the model. Keeping it in the write parse entry also
means it is only reachable when the writable layer is enabled (the server calls
`parse_statement` only then); with the layer off the read parser rejects the `CALL` as a
forbidden write, which is the correct answer (nothing to consolidate). The RUN handler
dispatches `Statement::Consolidate` to `execute_consolidate`, which runs
`Graphs::consolidate_graph` (with the production `run_builder` seam) on a
`tokio::task::spawn_blocking` thread — the dump/subprocess/validate/swap work must never
park the Bolt reactor. A builder failure is surfaced as a query `Failure`,
non-destructively (old core keeps serving, delta stays live), exactly as the direct
orchestrator path. `ConnCtx` gains `data_dir` + `builder_bin` (from `config.delta`) to
supply the seam. Rejected alternative: a dedicated `Statement`-less path that routes
through `apply_call` like the metadata procs — that would force a read-shaped, result-cache
-eligible, generation-pinned execution around a mutation, and re-introduce the read-only
labelling problem.

### D48 — Consolidation carries post-freeze writes forward by replaying the WAL onto the new core
Phase 4a removes the Phase-1 restriction that no write may be admitted while a
consolidation runs (`crates/slater/src/delta_writer.rs`, `server.rs`). Previously
`DeltaWriter::retire` reset the live memtable to empty, so a write that arrived between
`freeze()` and `retire()` — durable in the fresh WAL segment freeze had opened, but
resolved against the *old* core's dense ids — was silently dropped from RAM until a process
reopen. That was safe only because Phase 1 forbade concurrent writes during a build; an
automatic soft-cap trigger (Phase 4d) fires while clients keep writing, so it must be
correct. The fix leans on an existing invariant: `freeze` seals the current segment and
rotates to a fresh one, and `Frozen.consumed` is exactly the *pre-freeze* set, so every
post-freeze write lands in a segment that is **not** consumed. `retire` therefore (1)
deletes the consumed segments (their writes now live in the new core), then (2) rebuilds the
memtable by `replay_dir` over the surviving segments, applying each op through a `resolve`
closure **bound to the new core** (`resolve_op(new_gen, op)`). Because WAL records are
self-describing (business-key names, no dense ids), re-resolution is automatic and, crucially,
a node that was delta-born pre-freeze (a synthetic id) and folded into the new core by the
rebuild re-resolves to its now-real dense id. No seal/rotate is needed inside `retire` — a
committed record is already fsync-durable (`WalSink::commit` flushes + `sync_data`), so the
still-open post-freeze segment replays fine and keeps taking appends afterwards. The rebuilt
snapshot is published *before* the core UUID is re-bound (rebuilt-publish-before-rebind), so a
lock-free reader that observes the new `core_uuid` also observes the re-resolved overlay; a
reader straddling the swap briefly falls back to the pure new core (which already holds the
pre-freeze writes) — the same benign visibility blip Phase 1 documented. This makes an
automatic consolidation that fires under sustained write volume non-lossy, the prerequisite
for the L0 flush + backpressure work (Phase 4b–4d).

### D49 — L0 flush publishes levels atomically; born identity resolves across levels
Phase 4c-B wires the L0 LSM into the writer (`crates/slater/src/delta_writer.rs`,
`server.rs`). Two decisions make the multi-level layer correct.

**(1) Atomic level publish.** `DeltaWriter::flush_to_l0` seals the active memtable to an
immutable `L0Segment` under `<wal_dir>/<graph>/l0/<n>.l0`, rebases a fresh active memtable
past every level (node **and** edge synthetic id spaces), rotates the WAL and deletes the
pre-flush segments (their writes are now fsync-durable in the L0 file). The subtlety is the
read view: a flush moves a node from the memtable into a new L0 level, and a lock-free reader
that read the memtable and the L0 list *separately* could see the datum in **neither** (read
new-empty memtable, then old L0 list) or, worse, see a delta-born node's synthetic id in
**both** levels (born-id sets union across levels → the same id listed twice in a label scan).
So the writer publishes the whole `DeltaSnapshot { mem, l0 }` as **one** `RwLock` swap
(`republish`), and `delta_for_read` clones that single value — a reader can never straddle a
flush. This replaces the Phase-1 `RwLock<Arc<Memtable>>` (active memtable only); `snapshot()`
still returns the active memtable for the writer's diagnostics/tests via
`DeltaSnapshot::active_memtable`.

**(2) Born identity resolves across levels (the flush crux).** With full-flush, a delta-born
node created before a flush lives in an L0 level while later writes land in the active
memtable — born entities span levels. A re-`MERGE` of such a node must resolve to its
**existing** synthetic id, not allocate a duplicate. The primitive is
`Memtable::born_synthetic_for_identity(label, key, value)` — non-mutating (it resolves names
through the memtable's interner via `Interner::get`, so a name absent there short-circuits to
`None`) — folded over the sealed L0 levels by `DeltaWriter::born_synthetic_for_identity`. The
live write path consults it in `execute_write`'s MERGE-`Absent` branch (nodes) and after the
core-only duplicate check in `execute_edge_write` (born endpoints — the check must run on
genuine core dense ids, never a synthetic id, so the L0 fallback happens *after* it). The
**same** substitution runs on the WAL-tail replay path (`resolve_with_l0` in
`DeltaWriter::open`), so a reopen re-resolves a re-`MERGE` against the reloaded L0 files
identically — no duplicate on replay. A born edge re-`MERGE` is not separately de-duplicated:
the read merge already dedups edges by `(reltype, neighbour)` newest-wins, so traversal and
consolidation stay correct (the only residue is a harmless `edge_count` over-estimate, gated
off the count fast paths whenever the delta is non-empty). Consolidation folds the L0 levels
for free — `freeze` captures them into `Frozen.l0`, the dump reads through the multi-level
`DeltaSnapshot::with_levels`, and `retire` deletes the consumed L0 files and clears the level
stack. A flush is **not** admitted during an in-flight consolidation (that guard is Phase 4d),
so at retire the level stack is exactly the frozen `consumed_l0`.

### D50 — Delta compaction is two-tier; core consolidation fires at a fraction of core, opt-in
Phase 4d admission (`crates/slater/src/{server.rs,config.rs,delta_writer.rs}`,
`crates/slater-delta/src/memtable.rs`). The only fold-into-core path is a full `slater-build`
rebuild — **O(core), not O(delta)**: it re-clusters, re-ISAMs and re-builds topology + vector
indexes over the whole permuted dense-id space, ~an hour and a ~180 GB dump on the 91M-node core
*regardless of how small the delta is*. So a single fixed-byte "soft cap → rebuild" trigger is a
lose-lose: fire it often and you rebuild the whole core per fill (catastrophic **write**
amplification — the very thing L0 exists to avoid); fire it rarely and hundreds of L0 segments
accumulate, and every read unions/dedups across all of them (**read** amplification). The core is a
read-optimised base whose merge is inherently expensive (read-optimised-base + write-optimised-delta,
à la C-Store/Mesa); the fix is not to make the rebuild cheap but to make it **rare**, with a cheap
intermediate tier absorbing the churn. So compaction is **two-tier**:

- **Tier 1 (cheap, frequent, O(delta), no core rebuild):** memtable→L0 flush at `memtableBytes`
  (4c-B) and **L0→L0 compaction** at `l0CompactionTrigger` segments (4d-i, `Memtable::merge_levels`
  + `DeltaWriter::compact_l0`) — merge small L0 segments into one, reclaiming overwrites/tombstones
  and bounding **both** resident RAM and read fan-out. On by default. This is what sustains write
  volume.
- **Tier 2 (expensive, rare, O(core)):** the full rebuild fires when the delta's changed-entity
  count reaches **`deltaCorePercent`% of the core's entity count** — a *fraction of core*, not an
  absolute byte count, so write amplification is bounded ~`100/percent`× independent of core size.
  **Off by default** (`deltaCorePercent = 0`): auto-firing an ~hour-long rebuild must be opt-in;
  otherwise the manual `CALL slater.consolidate()` (or a future schedule) is the path. It is spawned
  **detached** (`spawn_auto_consolidation`), never blocking the write ack; 4a keeps concurrent writes
  safe. A `deltaHardBytes` hard cap is the OOM backstop — a write past it throttles (ensure a drain,
  await headroom, bounded so a wedged rebuild can't hang a writer forever); also off by default.

**Update (segmented core, `docs/SEGMENTED-CORE-PLAN.md` Phases 4–5): the two tiers become a
four-rung ladder.** The segmented core inserts two O(delta)/O(segments) rungs *between* the L0 tier
and the O(core) rebuild, so a fill climbs progressively larger but still-sub-core folds before it
ever needs a rebuild — each rung defers work to the next, rarer, coarser one, and the rebuild stays
the terminal escape rather than the routine consolidation:

1. **memtable → L0 flush** at `memtableBytes` — bound resident memtable RAM.
2. **L0 → L0 compaction** at `l0CompactionTrigger` levels — bound L0 read fan-out (reclaim
   overwrites/tombstones).
3. **L0 → core segment (T2 flush)** — fold the sealed delta into one immutable upper *core* segment
   over the base (`Graphs::flush_graph_to_segment`, Phase 4), draining the delta to near-empty
   without a core rebuild. The segment reads newest-wins over the base; ids are preserved (no
   re-resolution).
4. **core segment → core segment (T3 compaction)** at **`maxUpperSegments`** — merge a contiguous
   run of upper segments into one (`Graphs::compact_graph_segments`, Phase 5), bounding the *segment*
   fan-out a point read crosses. Admission is by segment count; run selection is **size-tiered**
   (`select_compaction_run`, slice 5.3): the longest contiguous run within a `SIZE_TIER_RATIO`× size
   band, so each byte is rewritten at most once per tier climbed — the size-tiered-compaction
   invariant, adapted to the contiguity constraint (only adjacent segments fold, their id bands must
   tile). Cheap, on by default (`maxUpperSegments = 8`), like rung 2.

Rungs 3–4 are O(delta)/O(segments) and preserve the id space, so — unlike the rebuild — they need no
re-resolution or rebase, only a lightweight delta rebind. **Auto-firing rungs 3 and 4 from the write
path is Phase-6-gated** (both need a segment-aware write resolve): until then they run explicitly
(`flush_graph_to_segment` / `compact_graph_segments_auto`), exactly as the tier-2 rebuild runs from
`CALL slater.consolidate()`. The tier-2 rebuild (the rung-5 terminal) is unchanged — it now folds a
whole *stacked* set, not just base + delta.

The reframe that makes this sound: because the delta is already durable, read-correct and
RAM-bounded (WAL + on-disk L0 + tier-1 compaction), tier-2 consolidation is a **background
read-locality optimisation, not a correctness requirement** — so it can be rare, opt-in, and
deferred to quiet periods. `consolidation_due` (`u128`-safe) is the pure predicate;
`DeltaWriter::begin/end/is_consolidating` (a `consolidating: AtomicBool`, released by an RAII
`ConsolidationGuard` in `consolidate_graph`) is the single-flight guard that also excludes
flush/compaction across the freeze→retire window (which `retire` clears wholesale). Rejected:
*partial-flush* (born entities stay resident) — degrades to no-L0 for insert-heavy loads; and a
fixed-byte consolidation cap — unbounded write amplification on a large core. Prompted by the
"consolidation on a 91M core takes ~an hour" review.

### D51 — In-place core-edge property patching keys on the resolved core edge id
Follow-up from D46/3c (`crates/slater-delta/src/memtable.rs`, `crates/slater/src/{server.rs,exec.rs}`).
`MERGE (a)-[r:R]->(b) SET r.p = …` on an edge that **already exists in the core** now patches that
core edge's properties in place, rather than being rejected. The read path materialises a
relationship only from an **edge id** (bound by traversal), never from its identity, so the patch
must be reachable **by core edge id** — exactly the node-patch overlay shape (`by_dense`), transposed
to edges. So: a new `Memtable::by_edge_id` (core edge id → identity key) indexes a
`synthetic_edge = None` patch entry (distinct from a delta-**born** edge, which uses `synthetic_edge`,
and from a tombstone-only entry, which carries neither); `EdgeEntry.core_edge` records the id and is
persisted, with `by_edge_id` **rebuilt** from it on deserialise (not serialised — the entries are
authoritative). `edge_delta_by_id` resolves a core edge id through `by_edge_id`, and the
`DeltaSnapshot` edge-property accessors fold **newest-wins across levels** (a core edge may be
patched in several L0 levels — unlike a born edge, whose id lives in exactly one level).

**The write path re-resolves the core edge id against the *current* core on every replay**, never
storing it in the WAL: the same `find_core_edge_id` scan that provides `MERGE` idempotency yields the
id, carried on `OpResolution::Edge { edge_id }`; `apply` routes `Some` → `patch_core_edge`, `None` →
`upsert_edge` (born create). This is load-bearing for correctness after consolidation — a born edge
folded into a fresh core *becomes* a core edge, so a later patch of it must resolve as a core-edge
patch against the new core, which a WAL-stored flag could not express. A core-edge patch **does not**
touch the born vector or the adjacency indexes (traversal reads the edge from the core; only its
properties overlay), so topology is unchanged and consolidation carries the patch for free (the dump
reads `edge_props`, now overlay-aware, exactly like a patched core node). **Scope unchanged
otherwise:** a delete of a core edge still resolves by identity (no id needed), and a
patched-then-deleted-across-levels edge reads stale props *only via a path traversal never reaches*
(the tombstone suppresses it in the adjacency overlay), so it is unobservable.

### D52 — Range-index reads cache *decoded* leaf blocks (not raw bytes) and binary-search them
`crates/graph-format/src/isam.rs`, wired per-generation in `crates/slater/src/generation.rs`
(`cache.rangeIndexCacheBytes`, default 16 MiB). A business-key write resolve (`resolve_business_key`)
and an indexed range seek both probe an ISAM range index, and `IsamReader::lookup_eq` previously
**re-read + re-decompressed + fully decoded, then linearly scanned, a whole leaf block on every
probe**. Measured on the 91M-shaped 1M-node Wikidata core: the `wikidata_id` index has **27 blocks of
~37 000 entries each** (blocks are sized for range-scan compression, not point lookups), so a single
resolve cost **~2.6 ms** and a 300K-key bulk delete spent ~800s *just resolving*, CPU-bound — the
finding that surfaced once group-commit removed the fsync wait.

**A raw-byte block cache is the wrong altitude here** (measured **~15%** only): with 37K-entry blocks
the cost is the *decode + scan*, not the read + decompress, and a byte cache still re-decodes every
probe. So the cache stores **decoded** blocks (`Arc<Vec<(Value, u64)>>`) — `DecodedBlockCache`, a
byte-budgeted LRU keyed `(index-ordinal, block)`, one instance shared across a generation's range
readers and freed when the generation drops on swap — and `lookup_eq`/`lookup_range` **binary-search**
the cached sorted block (the block is ascending by `cmp_key`) instead of scanning it. A repeated probe
into a warm block is then O(log n), and each block is decompressed+decoded at most once. Result:
**~2.6 ms → ~1.5 µs per resolve (~1750×)**; the 30%-delete smoke **875s → 13.2s (~66×)**, now bound by
the 30 batch fsyncs, not the resolve. Off for every non-server opener (`None` budget: tools, tests,
consolidate) so their behaviour is unchanged; no change to `resolve_business_key`/`scan_candidates`
call sites (the reader caches transparently). Rejected: a raw-byte cache (decode still dominates); a
per-reader cache (memory multiplies across a generation's indexes — one shared budget is bounded).
Complementary build-side lever (smaller range-index blocks) is D53.

### D53 — Range (ISAM) indexes are built with smaller leaf blocks than the columnar files
`crates/slater-build/src/{shared.rs,main.rs,build_external.rs}`. The builder sized every file —
node/edge props, topology, **and range ISAMs** — with one `--block-size` (256 KiB), which is right for
*columnar* files (scanned sequentially; big blocks compress well and amortise seeks) but wrong for a
range index, which is probed by **point** lookups (business-key write resolve, indexed equality/range
seeks). A point lookup decodes a whole leaf, so a 256 KiB leaf = ~37 000 entries decoded per probe
(the D52 finding). So range ISAMs now take their own `--range-block-size`, default **16 KiB**, while
columnar files keep 256 KiB (a `vector_block_size` split already set the precedent).

Measured on 1M contiguous int keys: 256 KiB → 27 blocks (~37K entries), **~2836 µs/uncached lookup**;
16 KiB → 426 blocks (~2.3K entries), **~182 µs (~15×)**. This is **complementary to D52, not a
replacement**: D52's decoded-block cache makes a *warm* probe O(log n) (~1.5 µs) regardless of block
size; D53 makes the *cold* path — a cache miss's one-time decode, an uncached tool, or a random-access
workload whose working set exceeds the cache — ~15× cheaper, and shrinks each cache entry so the same
budget holds far more blocks. Costs are modest: more blocks ⇒ a slightly larger resident top-level
(~426 vs 27 entries here — still tiny) and marginally worse compression / more range-scan seeks. Only
affects **newly built** generations; existing images are unchanged until rebuilt. Determinism/golden
tests are invariant (they build-twice-and-compare, not against a pinned old-block-size hash).

### D54 — Off-heap L0 delta segments read through the shared block cache
`crates/slater-delta/src/{memtable.rs,l0_offheap.rs}`, `crates/slater/src/{delta_writer.rs,cache.rs,config.rs,server.rs}`.
A sealed L0 delta level was reloaded **whole** into RAM (`L0Segment` → `Arc<Memtable>`), so the
resident footprint of the L0 stack grew with the delta byte budget — the deferred RSS item. Off-heap
L0 (opt-in `delta.offHeapL0`, default off) instead spills a flushed level to a **directory** of
`graph_format::blockfile` sections (`node`/`adj_out`/`adj_in`/`edge`) whose per-entity payloads page
on demand through the server's **shared columnar `BlockCache`** (user decision: one budget + one
eviction domain, not a dedicated delta cache), plus a resident `meta.bin` (scalars, per-section sorted
`u64` key columns, secondary indexes). A point read binary-searches the resident key column: a **miss
costs no I/O** (the hot tombstone/patch path), a **hit pages+caches one block**.

Enabled by the `LevelRead` trait seam (Phase A): `DeltaSnapshot` folds over `Arc<dyn LevelRead>`, so a
level is resident (`Memtable`) or off-heap (`L0Reader`) transparently. `Memtable::to_segment_data`
gathers the delta through the memtable's own read methods and an `offheap_reader_matches_resident_memtable`
parity test proves read-for-read equality, so the two formats are behaviourally identical. The writer
holds `Vec<L0Level>` (resident|off-heap); reads/publish/freeze/consolidation go through `LevelRead`, and
a segment is reloaded in whichever format it is on disk (a directory ⇒ off-heap).

**Rejected / deferred, deliberately.** (1) A **dedicated** delta cache — the user chose the shared one;
per-segment cache scopes (a fixed-seed hash of the segment dir path) are disjoint from the generation-UUID
columnar scopes, and a retired segment's blocks age out of the LRU. (2) **Blocking the secondary indexes**
(`born_by_label`/`index`/`identity`, `core_patched`) — they stay resident (they re-hold born *values*, the
only unbounded term left, and only for insert-heavy deltas); blocking them is a mechanical follow-up. (3)
Naively **porting `merge_levels`** (fold the whole run into a resident memtable, then write it) for
off-heap compaction — it would spike RSS to the merged-run size, defeating the point. Instead off-heap
L0→L0 compaction is a **disk-native streaming merge** (`l0_offheap::merge_run`): the sorted on-disk runs
are folded through a `DeltaSnapshot` (reusing the tested read/fold semantics) and streamed out through an
incremental `OffheapSegmentWriter`, so peak RSS is a block window, not the merged result. A merge reuses
the run's oldest segment directory, so a **fresh unique scope** (v4 UUID) is persisted in each segment's
meta and read back at open — otherwise the reused path would serve the pre-merge segment's stale cached
blocks. The hot read path and every per-entity payload are fully off-heap. `#![forbid(unsafe_code)]` holds
throughout (`pread`, no mmap). No L0 back-compat obligation (segments are ephemeral between flush and
consolidation). See `docs/WRITABLE-PROGRESS.md` §"Off-heap L0 reads" and `~/.claude/plans/offheap-l0.md`.

### D56 — A filesystem-only generation records BLAKE3 only; SHA-256/CRC32C are for object stores
`crates/graph-format/src/integrity.rs`, `crates/slater-build/src/common.rs`, `crates/slater-build/src/main.rs`.
The MANIFEST recorded three digests per file — BLAKE3 (canonical content hash), base64 SHA-256 (S3's
`x-amz-checksum-sha256`) and base64 CRC32C (GCS's `crc32c`) — for **every** build. The two object
checksums exist for exactly one purpose: so a generation *served from an object store* can be verified
against the store's server-computed checksum from a metadata `HEAD`, with no body read. A generation
that lives on a filesystem is verified by re-hashing its bytes with BLAKE3 and has no use for either.

They are not free. SHA-256 has no tree structure, so it cannot be parallelised within a file and runs
at roughly one core's throughput; at 91.6M nodes it *was* the `publish` phase — 1.0 min at 99% CPU for
100% of its samples, reading 23.4 GB at 411 MB/s, which is about where a single SHA-256 core saturates.
So SHA-256 and CRC32C are now computed only when `--store` is set (the build publishes to an object
store) or `--object-checksums` is passed (the generation is bound for one by other means, e.g. `aws s3
cp`). `FileEntry::sha256` / `::crc32c` were already `Option` and `skip_serializing_if = "Option::is_none"`,
so a filesystem MANIFEST simply omits both keys.

**This changes `MANIFEST.json` for filesystem builds but *not* the content hash** — which
`docs/BUILD-PERF-PLAN.md` predicted it would, wrongly. `content_hash` is a digest over the inventory's
`(name, blake3)` pairs only (`integrity::content_hash`, `Manifest::verify_content_hash`), and the BLAKE3
values are unchanged; `MANIFEST.json` is not itself in the inventory. So the omitted `sha256`/`crc32c`
keys are invisible to it. Verified: a 1M Wikidata build before and after this change both hash to
`6cbc6508cfb33d9e70bb1e7ca7c7a88073c5f18b55a9b9b87037eaee007ea638`. **No re-baseline was needed**, and
the 91.6M fixed point `5e8e7307…` still stands. `emit_determinism.rs` (build-twice, compare bytes) is
invariant either way.

The cost of the trade is bounded and known: a generation built without `--object-checksums`, then
hand-copied to S3/GCS, falls back to that backend's size-only completeness check (`Content-Length`
vs the manifest's byte count) instead of a content-grade checksum comparison — an S3 PUT is atomic, so
a present, right-sized object is a complete one. `--object-checksums` restores the strong check for
anyone who wants it, at the old price. BLAKE3 itself is unaffected and still covers every file; the
`rayon` feature plus an 8 MiB read buffer (was 64 KiB) let `Hasher::update_rayon` fan a single large
file across the pool, which matters because `topology.csr.blk` alone is ~71% of a generation's bytes.

### D57 — Block sealing (zstd + AEAD) runs on a shared pool, bounded globally, drained in block order
`crates/graph-format/src/blockfile.rs`.
`BlockFileWriter::flush_block` compressed — and, for an encrypted generation, AEAD-sealed — each full
block **inline, on whichever thread appended the record that filled it**. Every `.blk` file in a
generation goes through this writer, so a build phase whose shape is "read a sorted stream on one
thread, write it back out" was really measuring *one core's zstd throughput*. Per-op diagnostics at 1M
nodes named four of them: `cluster`'s `route adjacency into stripes` (2.7s at 116% CPU), `dedup`'s
drain (84%), `emit.node_stores`' drain (99%), and `emit.topology`'s stitch (79%).

Sealing now goes to a shared, bounded worker pool (`SLATER_BLOCKFILE_SEAL_THREADS`, default
`--threads`; `1` restores the inline path as an escape hatch). The appending thread hands off a full raw
block and keeps filling the next; workers seal concurrently; the appending thread drains the
**contiguous completed prefix** in block order and writes it. Blocks that finish early wait their turn,
so block boundaries, block contents, directory order and file bytes are all independent of the order
workers finish in. zstd is deterministic for a given `(input, level)`, so an unencrypted file is
byte-identical to the serial path — verified: a 1M Wikidata build hashes to `6cbc6508…` before and
after. (An encrypted file was never byte-reproducible: each block takes a fresh random nonce.)

The in-flight cap is **global, not per-writer**, which is the load-bearing choice. A single thread can
hold many writers open at once — `cluster` routes into 1,398 stripe files, one `BlockFileWriter` each —
so a per-writer allowance would multiply by 1,398. One counted semaphore over `seal_threads × 2` blocks
bounds the pipeline's resident bytes to ~16 MB at the default 256 KiB block regardless of how many
writers are live. `BlockId`s are handed out from a `next_block` counter rather than `dir.len()`, since
the directory now lags the submission point by whatever is in flight.

Measured at 1M nodes / 12.2M edges (16 cores, `--threads 14`): total build **30.6s → 24.4s**, with
`emit.node_stores`' drain 0.9s@99% → 0.5s@243%, `cluster`'s route 2.7s@116% → 2.0s@164%, and
`emit.topology`'s forward band pass 5.6s → 3.8s. The residual serial time in `cluster`'s route is the
k-way merge *decompressing* run blocks on the consuming thread — the read side, not the write side.

### D58 — `--max-memory` is arbitrated by one accountant, and sorters budget *resident* bytes
`crates/graph-format/src/membudget.rs`, `crates/graph-format/src/extsort.rs`, `crates/slater-build/src/*`.
`--max-memory` was a number every consumer helped itself to a fraction of: two posting sorters took
`/16` each, every range index another `/16`, each band worker `/16/threads`. Nothing held the whole
number, so nothing noticed when the fractions summed past it — a 4 GiB cap peaked at 8.06 GB, with 20.1%
of all diagnostic samples above the cap.

`MemoryBudget` is the missing arbiter: a counted semaphore over the cap handing out RAII `Reservation`s,
granting `min(want, free)`. Two grant modes, and the difference is a liveness argument. `reserve_now`
never blocks and fails when less than `floor` is free — for long-lived reservations taken on one thread,
where a blocking wait could only deadlock against the caller's own earlier grants. `reserve` blocks, and
is sound only where another holder is guaranteed to release: a worker pool drawing from
`Reservation::into_sub_budget()`, each worker holding one slice per work item. A `floor` above the whole
cap can never be satisfied, so it fails loudly rather than parking. `Reservation::split_off` exists
because `resolve`'s stage 2 holds two sorters at once, and reserving twice inside a pool would let every
worker take its first slice and then wait forever for a second.

**The subtle half.** An accountant is only as honest as the estimates it is handed. `ExtSorter` sized its
buffer with `SortRecord::size_hint()`, whose contract is *"an estimate of the **encoded** size"* — the
run-file cost. `EndpointRef` reports 24 bytes and occupies 56 resident (its `Value` alone is 32); a `Vec`
holds up to twice the elements it has. The old `/16/threads` fractions were small enough (~18 MB per
sorter) that the 3-4× under-count never mattered. Granting the real budget multiplied it: the first
full-scale run of this change put `resolve` at **15.11 GB against 4.29 GB reserved**. So `SortRecord`
grew `resident_hint()` (`size_of::<Self>() + size_hint()`), and `push` budgets
`buf.capacity() * size_of::<R>() + heap_bytes`. `resolve`'s peak fell to 5.62 GB and `dedup`'s from
4.89 GB to 1.53 GB. The bug was *found* by the `budget_reserved_bytes` counter this decision added
alongside `rss_bytes` — divergence between the two is exactly the bug it exists to show.

A pool sorter's spill mode is a property of the data, not the code: `ExtSorter::new_for_pool(…,
saturated)`. At 91.6M nodes `emit.topology` runs 88 bands over 14 workers, so the shared spill pool can
hand a band no extra cores and splitting its reservation across in-flight buffers would only multiply its
run count (the merge holds one decompressed block per run) — inline is right. At 1M nodes there is
exactly **one** band, and inline left 13 cores idle (`emit.topology` 6.5s → 9.95s). The caller passes
`nbands >= threads`.

**Measured, full 91.6M / 1.49B rebuild, 4 GiB cap, `--threads 14`.** Peak **reserved** is 4.29 GB =
**1.00× the cap** — the accountant provably never overcommits. Samples above 2× the cap: **0.0%** (was
20.1% above the cap outright). Content hash unchanged at `5e8e7307…`.

**What the accountant could not bound, and why.** On glibc, peak *RSS* was 8.29 GB = 1.93× the cap even
though peak reserved was exactly 1.00×. The gap was never live memory: `emit.topology`'s `stitch` step
held **6.25 GB resident against 0.81 GB reserved** while doing nothing but a verbatim block-concat of
finished files, and `emit reverse CSR per band` sat 4.7 GB above its reservation despite `EdgeRev` owning
no heap at all. That was glibc arena retention — 14 worker threads churning ~1.5B small `props_blob`
allocations, freed into per-thread arenas that are never returned to the OS. Fixed by **D59** (jemalloc);
`emit.topology` now peaks at 4.60 GB = **1.07× the cap** and `stitch` at 2.53 GB against the same
0.81 GB reserved. B1's acceptance is met.

### D59 — `slater-build` runs on jemalloc, and the histogram scan abandons instead of counting
`crates/slater-build/src/main.rs`, `crates/slater-build/Cargo.toml`, `crates/graph-format/src/{isam,histogram}.rs`.
D58's accountant bounds what the build's sorters *reserve* — at 91.6M nodes, peak reserved is exactly the
`--max-memory` cap. Peak *RSS* stayed at 1.93× that, and the excess was not live: `emit.topology`'s
`stitch` step held 6.25 GB resident against 0.81 GB reserved while only concatenating finished files, and
the reverse-band sorter sat 4.7 GB above its reservation though `EdgeRev` owns no heap. Fourteen band
workers free ~1.5B small `props_blob` allocations into per-thread glibc arenas, which glibc never returns
to the OS.

So `slater-build` takes `tikv-jemallocator` as its `#[global_allocator]` on Linux, as `slater` already
does. Its `background_threads` purge threads return freed heap on a decay timer without the process
making `free()` calls. **Not `malloc_trim`**: this crate sets `unsafe_code = "forbid"`, so the libc FFI
is unavailable to it — and the server migrated *away* from an idle-gated `malloc_trim` for exactly that
reason, "moving the last `unsafe` out of this crate and into the audited allocator". The allocator is
`cfg`-exclusive with the `profiling` feature's dhat allocator (only one `#[global_allocator]` may exist),
and `slater-build` is its own binary, so nothing about the server changes.

**Measured, full 91.6M / 1.49B rebuild, 4 GiB cap, `--threads 14`, against the same build on glibc.**
Content hash unchanged (`5e8e7307…`). Wall **48.08 → 47.04 min**; peak RSS **8.13 → 5.66 GB**.

| phase | glibc wall / peak RSS | jemalloc wall / peak RSS |
|---|--:|--:|
| dedup | 1.61m / 1.53 G | **1.13m / 1.01 G** |
| resolve | 11.61m / 5.62 G | **11.31m / 4.31 G** |
| cluster | 8.53m / 4.25 G | 8.52m / **2.08 G** |
| emit.node_stores | 1.68m / 2.86 G | **0.98m / 1.24 G** |
| emit.topology | 11.93m / **8.29 G** | 12.03m / **4.60 G** |
| emit.topology → `stitch` | 6.25 G resident / 0.81 G reserved | **2.53 G** / 0.81 G reserved |

jemalloc is not only returning pages, it services the small-allocation churn faster: `emit.node_stores`
went 1.68 → 0.98 min (**2.9× faster than the 2.8 min pre-B1 baseline**, at 5.4× cpu/wall vs 1.4×).

**The last peak was somewhere else entirely.** With retention gone, the build's peak RSS became a *single
sample* — 5.78 GB, 1.34× the cap, 1 of 11,059 samples — inside `emit.prop_hist`, a five-second phase that
reserves nothing. `derive_histogram_from_isam` called `distinct_key_counts()`, which materialises **every**
distinct `(Value, count)` pair, and only then checked `pairs.len() > max_distinct` and discarded the lot.
`node_Entity_wikidata_id` is near-unique over 91.6M nodes. Replaced by `distinct_key_counts_bounded`,
which abandons the moment a `max_distinct + 1`-th key appears; boundary semantics are unchanged (a
histogram with exactly `max_distinct` keys is still stored) and pinned by test. Excluding that transient,
the build's peak was already 4.60 GB = **1.07× the cap**, with **zero** samples above 1.25×.

Remaining, and deliberately not done: jemalloc treats the symptom. The churn is ~1.5B small
`props_blob` `Vec<u8>` allocations, one per edge; a per-band bump arena (or inlining short blobs into the
record) would remove them and cut CPU as well as RSS.

### D60 — `cluster` sorts each stripe, never the whole adjacency
`crates/slater-build/src/cluster.rs`.
LDG clustering needs, for each **stripe** of 65,536 node ids, that stripe's undirected
adjacency ordered by node. It was getting that from a *global* sort: one `ExtSorter` over every
half-edge, after which a single thread drained the k-way merge — decompressing every run — and
scattered records into 1,398 stripe writers. At 91.6M nodes that drain was **54% of the whole
`cluster` phase at 120% CPU**, and D57's parallel block sealing did nothing for it because the
bottleneck is *de*compression on the consuming thread, not compression.

The global sort never needed to exist. Stripes partition on `node`, which is the primary sort
key, so a stripe's adjacency is a **contiguous slice** of the global `(node, nbr)` order —
sorting each stripe independently yields byte-for-byte the file the global merge would have
scattered into it. Now: pass A routes each half-edge to the stripe owning its `node`, parallel
over the edge bucket's shards; pass B sorts each stripe, parallel over stripes. Nothing is
globally ordered, so nothing is globally serial. `build_permutation` takes a shard-parallel scan
(`buckets::for_each_edge` had no callers left and is now test-only).

Full 91.6M rebuild, content hash `5e8e7307…` unchanged — which is the load-bearing check, since
`cluster` decides dense-id assignment and any change to iteration order would move every emitted
file:

| | before | after |
|---|--:|--:|
| `cluster` phase | 8.52 min @ 2.9× | **4.55 min @ 8.3×** |
| build adjacency + route | 115.5s + 272.8s | — |
| route into stripes | — | **51.2s @ 796%** |
| sort stripes | — | **100.5s @ 989%** |

Total build 47.04 → 43.48 min. The LDG passes themselves (111s at ~740%) were always parallel and
were never the problem; B4 stage 1's per-sub-step instrumentation is what made that visible.

**Two changes measured and reverted, recorded so they are not retried.**

1. *Exact `resident_hint` for inline blobs.* With `props_blob` inlined into the record
   (`SmallVec<[u8; 16]>`), the resident size of `resolve`'s records is knowable exactly, and
   overriding `SortRecord::resident_hint` to report it is correct *per record*. In aggregate it is
   wrong: buffers then pack ~1.7× more records, and a `Vec` grows by **doubling** — the `realloc`
   that crosses the spill threshold holds the old array and the new one at once. `resolve` went
   4.31 GB → **6.04 GB against a 4 GiB cap** (1.41×, past B1's acceptance) and `emit.topology`'s
   reverse band 123.8s → 243.8s, for zero wall-clock gain. The default's apparent double-count is a
   load-bearing margin that also silently covers the k-way merge's per-run block, the band
   batchers, and the block writers' partial blocks. Documented on the trait.

2. *Parallel `stitch`.* `emit.topology`'s block-concat sat at 85% of one core, which reads like a
   parallelism problem and is not: it is bounded by **write bandwidth**. Copying the 176 band
   regions concurrently with positional writes scattered the write stream and raised CPU to 110%
   for no gain. Measured, all ±10% of each other: serial `BufWriter` 247.8s @85%, parallel `pwrite`
   255.8s @110%, sequential `std::io::copy` 272.5s @**79%**. The last is what ships — on Linux it
   dispatches file-to-file to `copy_file_range(2)`, so the bytes never enter user space (no memcpy,
   no second set of page-cache pages) — chosen for costing the least CPU, not for being faster.
   `stitch` is now 36% of `emit.topology` and the build's largest serial step. Beating it means
   writing fewer bytes, not writing them differently.

### D61 — Endpoint postings are a set, so compute them as a bitmap; and `psi_io` on a one-thread op measures queue depth, not the disk
`crates/graph-format/src/postings.rs`, `crates/slater-build/src/build_external.rs`.

`emit.topology`'s tail ran four serial single-threaded operations under one diagnostics label,
`stitch CSR + edge_props + postings`. D60 benchmarked three implementations of the *concat* under
that label — serial `BufWriter` 247.8s, parallel `pwrite` 255.8s, sequential `std::io::copy` 272.5s
— and, landing within noise of each other, read them as proof that the step is write-bandwidth-bound
and unimprovable. The label was hiding the cost. **Each of those three numbers was ~220s of posting
drain plus a ~55s concat**, so all three measured mostly the same thing. `7fd8949` split the label,
and both halves of the old conclusion then failed. Split out on the 91.6M build:

| sub-op | wall | cores | dev read | dev write | psi_io | RSS − reserved |
|---|--:|--:|--:|--:|--:|--:|
| `concat topology.csr.blk` | 50.7s | 0.31 | 18.0 GB @ 356 MB/s | 18.0 GB @ 355 MB/s | 48.7 | −0.57 GB |
| `concat edge_props.blk` | 8.4s | 0.38 | 3.9 GB @ 470 MB/s | 3.9 GB @ 469 MB/s | 43.9 | −0.68 GB |
| `drain reltype_src.post` | 88.8s | 0.91 | 4.5 GB @ 50 MB/s | 0 | 9.1 | **+1.98 GB** |
| `drain reltype_tgt.post` | 142.8s | 0.92 | 7.4 GB @ 52 MB/s | 0 | 2.9 | **+2.43 GB** |

**1. The sort never needed to exist.** `reltype_src.post` record `t` is the ascending distinct set of
node ids that are a source of a reltype-`t` edge. The builder reached that by pushing one
`RelEndpoint {reltype, node}` per edge per side into an `ExtSorter` — 2.98 B records — sorting by
`(reltype, node)`, and run-length-collapsing the drain. **A bit plane per reltype computes the same
set with no sort.** Bits only go 0→1, so the dedup is free and the answer is independent of the order
edges arrive in. Forward bands own disjoint, word-aligned slices of the source plane; targets scatter
across every band, so the planes are `AtomicU64` and workers `fetch_or` into them (`Relaxed` — the
band join is the happens-before edge). A plain `load` before the `fetch_or` elides most of the atomics,
because edges arrive grouped by source node.

Cost is `n_reltypes × ceil(node_count/8)` bytes per side. Checked against every dataset we build, the
product `n_reltypes × node_count` is bounded by ~92M bits — the richly typed graphs are the small ones:

| dataset | nodes | reltypes | planes, both sides |
|---|--:|--:|--:|
| Wikidata-full | 91,600,504 | 1 | 22.9 MB |
| Monarch-KG | 1,462,594 | 63 | 23.0 MB |
| MeSH | 340,839 | 3 | 256 KB |
| EU-AI-Act | 21,817 | 60 | 327 KB |
| POLE | 61,521 | 17 | 261 KB |
| Camelid-vet | 5,377 | 85 | 114 KB |

A graph both large *and* richly typed would not fit, so `write_endpoint_postings_from_sorted` stays as
the fallback, gated on `2 × bytes_for(…) ≤ --max-memory / 8`. Nothing real takes that branch, so
`SLATER_POSTINGS_FORCE_SORTER=1` exists to make CI take it.

**2. `budget_reserved_bytes` found its fourth bug.** The drains held 2.0–2.4 GB nobody reserved:
`write_endpoint_postings_from_sorted` accumulates `bucket: Vec<u64>` — every distinct node id of the
current reltype — before flushing. Wikidata has one reltype and 91,306,368 distinct sources, so that
is **730 MB**, plus the two copies `BlockFileWriter` makes of an over-target record. D59 had already
halved this gap (6.25 → 2.53 GB) by moving to jemalloc and attributed *all* of it to glibc arena
retention. The residual was not retention; it was an allocation nobody counted. The bitmap path
reserves its planes and its record buffer, and the gap is gone.

**3. `psi_io` on a one-thread op is not a saturation signal.** PSI counts the fraction of time a
*runnable task is stalled on I/O*. With exactly one thread that degenerates to "the one thread is
waiting", which a queue-depth-1 `copy_file_range` loop always is. The concat's `psi_io` 48.7 at
711 MB/s therefore says nothing about the device. In the same build, `publish` reads 25.1 GB at
**2,099 MB/s at `psi_io` 3.5** — it BLAKE3s the inventory under `par_iter` and keeps the queue full —
and `partition edges by src band` sustains 636 MB/s at `psi_io` 3.9 across 14 workers. Measured
directly (concurrent `copy_file_range`, 6 GiB, page cache evicted between runs): ~1.1 GB/s aggregate
at one stream, ~1.6 GB/s at 4–6. So the concat has ~1.5× of headroom, worth ~20s — **~1% of the
build, inside its own ±10% run-to-run noise.** Not taken, but the reason is economic, not physical.
Documented on `concat_block_files`. (D60's "parallel `pwrite` raised CPU to 110% for no gain" measured
the cost of abandoning `copy_file_range` and memcpying every byte through user space, not the cost of
parallelism.)

**4. Segmented generations were evaluated and rejected.** Publishing the per-band files as segments
(an `fs::rename` each) instead of concatenating them would delete the concat outright. But
`BAND_NODES = 1<<20`, so `bands = ceil(nodes / 1,048,576)` is **1** for EU-AI-Act, camelid, MeSH, POLE,
bioalphaengine, Wikidata-1M and Wikidata-100k, and **2** for Monarch-KG. Only full Wikidata has more
(88). A segment-per-band *is* today's single file for 11 of the 13 graphs we build, and on the one
that differs it is worth 55.8s of 2,610s — 2.1% — against a format change, a second binary search on
every `read_record_global`, a content-hash re-baseline, 264 files (≈2,862 at 1B nodes), and per-object
overhead on the S3/GCS upload and disk-cache tiers. The stronger form — band workers writing straight
into the published generation, "also removing 20.4 GB of scratch writes" — does not remove them: those
bytes are written either way, only to a different directory. Both forms buy the same ~56s. Revisit
only if the format changes for an independent reason, or on a reflink filesystem (XFS/btrfs), where
`copy_file_range` makes the concat O(1) outright.

**Verification.** The bitmap and the sorter emit the same bytes, so **no content hash moves** — a
stronger check than any re-baseline would have been. Two full 91.6M rebuilds, `5e8e7307…` unchanged in
both, and every published file's BLAKE3 matches the baseline generation's inventory exactly.

**Quote the noise-free quantities, not a wall-clock draw.** This box's `rest of build` — every phase
this change does not touch — spans **1858–2075s across the five runs**, a ±6% machine-condition band,
so any single wall or peak-RSS figure is one sample from it. What does not move run to run:

| | before (3 runs) | after (2 runs) |
|---|--:|--:|
| content hash | `5e8e7307…` | **`5e8e7307…`** |
| `drain reltype_{src,tgt}.post` | 231.6s @ 0.91 cores | **gone** — bit-plane write 1.0s @ 0.9 cores |
| `emit forward CSR per band`, **bytes written** | **54.24 GB** | **42.34 / 42.39 GB** (−22%) |
| `concat` (both), wall | 55.5 / 55.8s | 55.8 / 56.4s (untouched) |
| RSS − reserved, worst op | +2.43 GB (`drain … tgt`) | **+0.87 GB** (band `resident_hint` margin) |
| Monarch-KG peak RSS | 2.03 GB | **1.39 GB** |

And what does move, quoted as a range rather than a best draw:

| | before: f / g / h | after: #1 / #2 |
|---|--:|--:|
| `emit.topology` | 892.6 / 750.6 / 902.1s | **471.7 / 619.4s** |
| …normalised to `g`'s control | 884.9 / 750.6 / 850.6s | **441.7 / 554.6s** |
| build wall (phase sum) | 46.12 / 43.48 / 47.88m | **40.94 / 44.91m** |
| peak RSS (`getrusage`) | 5.97 / 4.95 / 4.28 GB | 5.66 / 5.01 GB |

**The worst new run's `emit.topology` (619.4s) beats the best old run's (750.6s) by 131s.** That is the
claim to make; "12.5 → 7.9 min" is true of one pair of draws and would be cherry-picked.

**Peak RSS is unchanged in substance, and its old headline was a cherry-pick.** The two posting sinks
used to reserve `2 × --max-memory/16` for the whole forward pass; that 512 MB now goes to the
band-worker pool, which spends it on larger band sort buffers, so the net is ~zero. The documented
"4.95 GB = 1.15× cap" was the `g` run; `f` and `h`, on *identical* code, measured 5.97 and 4.28 GB.
Do not compare a single RSS number across two runs of this build.

Cross-checked at other scales, where the answer *is* deterministic: 1M Wikidata hashes `6cbc6508…` on
**both** paths; Monarch-KG (63 reltypes — the only real dataset exercising `reltype_count > 3`) hashes
`89d2e818…` on both paths, with all 63 per-reltype source and target counts matching the pre-change
manifest.
