# Slater security worklist

Items from the security review of 2026-06-12, plus everything raised since — most recently the
external at-rest review of 2026-07-22 and the adversarial review of its remediation
(2026-07-31). The headline ACL-stamp-on-reload fix and the
Tier-1 DoS caps were implemented in that pass (see `THREAT_MODEL.md`); the rest were triaged
as lower priority and recorded here. Several have since been completed — each item lists where
it lives, why it matters, the fix, and its current status. Each line also carries a GitHub
checkbox (`[x]`/`[ ]`) and an inline **✅ DONE** / **⬜ OPEN** tag.

Severity reflects impact assuming the documented trust model (read-only server; the data
dir and `acl.json` are protected by filesystem permissions; queries arrive from
authenticated principals over Bolt).

## Closed — write authorisation for the writable layer (2026-07-09)

- [x] **✅ FIXED — a `read` grant authorised writes.** The writable layer (`delta.enabled`)
  added `MERGE` / `SET` / `DELETE` and `CALL slater.consolidate()` to the Bolt surface, but the
  ACL was never extended: `Acl` exposed only `can_read`, and the write dispatch in
  `server.rs` was gated solely by the `can_read` check performed at graph selection. Any user
  who could **read** a graph could **mutate** it — including triggering a consolidation, which
  rewrites the served generation. The `"write"` string parsed (grants is an unvalidated
  `Vec<String>`) but was never consulted; a `["write"]`-only grant conferred nothing at all.

  **Fix:** `Acl::can_write` plus `authorize_statement`, which classifies a parsed statement via
  `statement_mutates` (an exhaustive `match` on `Statement`, so a new write statement cannot be
  added without a compile-time decision) and refuses it with `Neo.ClientError.Security.Forbidden`
  unless the caller holds `write` on that graph. `read` does **not** imply `write`, so an
  existing read-only `acl.json` keeps its meaning when the writable layer is switched on.

  **Tests:** `a_read_only_grant_forbids_every_write_operation` iterates every operation of the
  write grammar (node `MERGE`/`SET`/`DELETE`/`DETACH DELETE`, the write-`UNWIND` batch forms,
  relationship `MERGE`/`MERGE … SET`/`DELETE`, and `CALL slater.consolidate()`) and asserts each
  is refused under a read-only grant; `a_read_write_grant_authorises_every_write_operation` and
  `the_write_grant_is_per_graph_and_reads_stay_allowed` cover the positive and per-graph cases,
  and `acl::tests::a_read_grant_never_implies_write` pins the predicate. Verified end-to-end over
  Bolt: a read-only user is refused all nine statements and the graph is provably unmutated.

## Closed — S3 per-file verify downgraded a manifest SHA-256 to a length check (2026-07-13, HIK-97)

- [x] **✅ FIXED — `verify_file` silently degraded a requested SHA-256 to byte length.** On the
  S3 backend, the per-file integrity check at generation open compares the object's
  server-stored SHA-256 (from `HEAD`) to the manifest's. When the manifest recorded a SHA-256
  but the object carried **none** server-stored (e.g. it was uploaded out-of-band under S3's
  default CRC64-NVME checksum), the `(_, None)` arm fell through to a `Content-Length`
  completeness check — with no warning. An operator who asked for a content digest silently got
  "the file is the right length", which catches truncation but not tampering. **Impact:** an
  attacker with bucket-write (but without the master key or the MAC'd manifest) could overwrite
  a *plaintext* block with same-length malicious bytes lacking a `checksum-sha256`, and verify
  would pass on size alone. (Encrypted images still fail at the AEAD tag on read — detection is
  only deferred there.)

  **Fix (`crates/graph-format/src/store/s3.rs`):** split the fallback. `(Some, None)` — manifest
  wants a SHA-256 the object can't prove server-side — now re-reads the object body and verifies
  it against the manifest's canonical BLAKE3 (the trait's default `verify_file`), restoring the
  content-grade guarantee at the cost of one GET; it is **not** a downgrade. The byte-length
  completeness floor is used only for `(None, _)` — a pre-checksum generation with nothing to
  compare. The routing is a pure, unit-tested `plan_verify()` so the invariant "a requested
  SHA-256 never falls back to length" is pinned without a network. slater's own `put` always
  stores a SHA-256, so a slater-published generation stays on the metadata-only path and never
  pays the body read.

  **Tests:** `s3::tests::manifest_sha256_without_server_sha256_never_downgrades_to_length` (+
  siblings) pins the routing; `s3_minio::verify_rehashes_body_when_server_sha256_absent`
  exercises the end-to-end path against MinIO (PUT with no checksum; a same-length tamper is
  rejected, which the pre-fix length check would have passed).

  **Sibling (now closed, HIK-107):** the GCS backend (`store/gcs.rs`) had the analogous
  `(_, None)` shape for its CRC32C check — see the entry below.

## Closed — GCS per-file verify downgraded a manifest CRC32C to a length check (2026-07-14, HIK-107)

- [x] **✅ FIXED — the GCS sibling of HIK-97.** `GcsObjectStore::verify_file` compared the
  object's server-stored CRC32C (from a metadata `get_object`) to the manifest's. When the
  manifest recorded a CRC32C but the object carried **none** server-stored (a composite object,
  or an out-of-band copy), the `(_, None)` arm fell through to a `Content-Length` completeness
  check — with no warning. A requested content digest was silently satisfied by "the file is the
  right length", which catches truncation but not a same-length tamper. **Impact** is identical
  to HIK-97 on GCS-backed deployments: an attacker with bucket-write (but without the master key
  or the MAC'd manifest) could overwrite a *plaintext* block with same-length malicious bytes and
  verify would pass on size alone. (Encrypted images still fail at the AEAD tag on read —
  detection is only deferred there.)

  **Fix (`crates/graph-format/src/store/gcs.rs`):** mirror HIK-97 exactly. The routing is now a
  pure, unit-tested `plan_verify()`: `(Some, Some)` → compare CRC32C (unchanged, content-grade,
  no body read); `(Some, None)` → re-read the object body and verify it against the manifest's
  canonical BLAKE3 (the trait's default `verify_file`), restoring the content-grade guarantee at
  the cost of one GET — **not** a downgrade; `(None, _)` → the byte-length completeness floor
  (a pre-checksum generation with nothing to compare) — the only case that uses length. slater's
  own `put` always sends a CRC32C, so every slater-published object carries a server CRC32C and
  stays on the metadata-only path; the body read is paid only by objects that genuinely lack a
  server checksum.

  **Tests:** `gcs::tests::manifest_crc32c_without_server_crc32c_never_downgrades_to_length` (+
  siblings) pins the routing without a network — proving `(Some, None)` routes to a body re-hash,
  never the length floor (fails on the pre-fix logic). `gcs_emulator::verify_rejects_same_length_tamper`
  exercises the end-to-end tamper rejection against `fake-gcs-server`. (Note: unlike S3, GCS's
  `put` always stores a CRC32C and the emulator always returns one, so the server-CRC-absent arm
  itself is not reproducible against the emulator through slater's API — hence the unit tests pin
  that specific arm.)

## Closed — the timing equalisation used the wrong argon2 parameters (2026-08-03, HIK-222)

- [x] **✅ FIXED — the unknown-user dummy hash was minted at `Argon2::default()`, not at the
  stored parameters.** `Acl::verify` burns a full argon2id verify for an unknown principal so a
  username cannot be found by timing. But argon2 verification is **parameter-agnostic**:
  `PasswordHash::new` reads `m`/`t`/`p` out of the stored PHC string and the blanket
  `PasswordVerifier` impl re-derives with *those*. The dummy was minted at the crate defaults
  (m=19456, t=2, p=1), so the moment a deployment's hashes came from anywhere other than
  `slater hash-password` — and `acl.json` accepts any valid PHC string — the two paths derived
  at different costs and the equalisation was gone.

  **Live for any non-default deployment, and worse than "stronger hashes diverge".** The gap
  opens in *both* directions, and the cheaper direction is the more common one (operators lower
  the memory cost for speed). Measured against the unfixed code with a 64 KiB / t=1 stored hash:
  **unknown principal 841 ms, known principal 8.9 ms — a 95× gap**, trivially readable over a
  network. The old guard (`unknown * 2 >= known`) bounded only the "unknown is suspiciously
  fast" direction and could not see this at all.

  *Fixed (HIK-222):* the unknown-user path now verifies against **the costliest hash the ACL
  actually holds**, rather than a minted one. Exact by construction — it runs the very same
  derivation a real login runs — and free to build, where minting at observed parameters would
  cost an argon2 hash per ACL construction. Safe: the result is discarded and `verify` returns
  `false` unconditionally, so even a correct guess authenticates nobody. With heterogeneous
  stored parameters no single dummy can equalise everything (timing already leaks *which* user);
  taking the maximum means an unknown name looks like the most expensive known user, which is
  the best a single dummy can do and strictly better than equalising downward. An ACL with no
  users at all keeps the minted fallback, where the question is vacuous.

  Tests: `acl::tests::the_equalisation_hash_tracks_the_stored_parameters` and
  `…_takes_the_costliest_stored_parameters` (structural and deterministic — they pin the derived
  parameters rather than a wall clock), `an_unknown_principal_tracks_non_default_stored_parameters`
  (end-to-end backstop), and `unknown_principal_still_pays_for_a_full_verify` now bounded
  **two-sided** — conspicuously slower enumerates as well as conspicuously faster.

  *Not done here:* making the argon2 parameters configurable. The 19 MiB figure is load-bearing
  in the `maxConcurrentAuth` DoS budget (`config.rs`, `handle.rs`, `HARDENING.md`,
  `THREAT_MODEL.md`), so that sizing rule has to become a function of it first.

## Closed — graph names were enumerable through the failure code (2026-08-03, HIK-221)

- [x] **✅ FIXED — `select_graph` answered "not served" and "no read grant" differently.**
  Any authenticated principal may name an arbitrary graph in `BEGIN {db:…}` / `RUN {db:…}`
  (also `SHOW STORAGE INFO` and the scoped introspection statements); only "is authenticated"
  is checked first, so a principal holding **no grant at all** reached both branches. A
  non-existent name returned `CODE_NOT_FOUND`, an existing-but-ungranted one `CODE_FORBIDDEN`
  — so the response was an oracle for "does this deployment host a graph called X?".

  Four channels carried the distinction, not the three first identified: the legacy `code`,
  the `message`, the derived `gql_status` (`Failure::gqlstatus` maps FORBIDDEN into class
  `42000` and NOT_FOUND into `50000`), and `status_description`, which embeds the message
  again. A client reading *any* one of them could tell the cases apart.

  Exposure was names only, post-authentication — no data, no grant escalation — and the
  names were not otherwise reachable: `readable_databases`, `Acl::readable_graphs` and the
  `available:` list inside the error are all `can_read`-filtered. This dichotomy was the
  whole of it.

  *Fixed (HIK-221):* both cases now return the identical `CODE_NOT_FOUND` failure, which is
  what `USE <graph>` had always done — an internal inconsistency, not a missing principle.
  The operator-facing distinction moves server-side: `warn!` when a real graph is denied (a
  possible probe), `debug!` when the name is simply not served (usually a typo). The
  db-less arm keeps `CODE_FORBIDDEN` deliberately — it names no graph, so there is nothing
  to enumerate, and HIK-123's regression tests depend on that signal. Test:
  `an_unreadable_graph_is_indistinguishable_from_a_missing_one`, which compares the whole
  failure metadata map (so a fifth channel added later cannot reopen the hole silently).

## Closed — session state outlived the identity it belonged to (2026-07-16, HIK-123)

- [x] **✅ FIXED — `LOGOFF` left the prior user's rows and transaction graph on the connection.**
  A Bolt connection outlives the principal on it: `LOGOFF` → `LOGON` (and a bare re-`LOGON`,
  which `authenticate` permits for token rotation) hand the same socket to a new user. `LOGOFF`
  cleared only `sess.user` and left two pieces of the previous user's state behind:

  * `sess.pending` — their buffered result rows. `Request::Pull` drained it with no `sess.user`
    check, so the next user on the connection **received the previous user's query results**.
  * `sess.tx_graph` — the graph their `BEGIN {db:…}` resolved. The `Some(g)` arm of `RUN`'s
    graph resolution returned it without calling `select_graph`/`can_read`, so a db-less `RUN`
    by the next user **read that graph with no grant of their own** — a read-ACL bypass. (The
    write path was unaffected: `authorize_statement` re-checks `can_write` independently.)

  Reachable on any pooled/shared connection, and directly by a client chaining credentials.

  **Fix:** the cause was three identity transitions (`RESET`, `LOGOFF`, re-`LOGON`) agreeing on
  what to clear in only one of them — `RESET` was correct and the other two drifted from it.
  `Session::clear_user_state()` is now the single owner of user-scoped session state and every
  transition calls it, so a field added to `Session` cannot be cleared on one path and forgotten
  on another. Two independent checks close the same doors: `PULL` requires an authenticated
  session, and the `tx_graph` arm re-checks `can_read` **per RUN** rather than trusting the
  BEGIN-time decision — the ACL hot-reloads, so that arm also served reads on grants **revoked
  mid-transaction**, with no identity change involved.

  **Tests:** `logoff_does_not_leave_the_prior_users_rows_for_the_next_user` and
  `logoff_does_not_leave_the_prior_users_transaction_graph_for_the_next_user` (the two attacks,
  each over one socket with two users); `a_bare_relogon_does_not_inherit_the_prior_users_rows`
  (the no-LOGOFF path); `a_grant_revoked_mid_transaction_stops_serving_reads` (the revocation
  bug). All four were confirmed to fail against the unfixed handlers — the leak tests receive
  `RECORD`s of the prior user's rows, the ACL tests are served `SUCCESS` where a `FAILURE` is
  required.

## Closed — a forged `nav` discriminator could mis-navigate a cosine/L2 index (2026-07-17, HIK-137)

- [x] **✅ FIXED — `nav: inner_product` on a non-Dot index was navigated by the IP navigator, not
  refused.** HIK-137 added an IP-native (MIPS) vector navigator, selected by an additive-optional
  `nav` discriminator on the manifest / `SealedVamanaMeta` / `DumpVectorCarry`. The read path
  dispatched on `nav` alone: `InnerProduct` → `AdcTable::new_ip` (raw inner-product ADC), `Augmented`
  → the L2-reduced ADC. The `(metric, nav)` pair was **not** cross-checked. The generation-open
  validator (`validate_vamana_index`) does tie the `.pq` codebook back to the declared space, but
  that check cannot catch this case: an `InnerProduct` codebook is `PqParams::new(dim, …)`, and a
  **cosine or L2** codebook has the *identical* width (`ann_pq_params` only augments for `Dot`), so a
  forged/bit-rotted `nav: inner_product` on a cosine/L2 index passes the width check and is then
  navigated by inner product over a graph built for angular/Euclidean closeness — **wrong neighbours,
  plausible scores, no error**. On a **plaintext** image the manifest's own fields are unauthenticated
  (`THREAT_MODEL.md` limitation 2) and `content_hash` covers the *inventory files*, not the manifest
  JSON, so a same-length `nav` flip survives every integrity check. `nav == InnerProduct` is only ever
  *produced* for `Metric::Dot` (`build_vamana_ip` and the segment seal both gate on it), so the forged
  pairing is unreachable through any legitimate build.

  **Fix:** a typed invariant `AnnNav::check_metric(metric)` (returning `NavMetricMismatch`, so callers
  branch on the error *type*, not its text — house rule) refuses `InnerProduct` on any non-`Dot`
  index. It is enforced at two read sites: `validate_vamana_index` (base index, fail-fast at generation
  open, matching the file's "refuse to serve on any mismatch" doctrine) and the shared `beam_over_index`
  navigator (fail-closed at query time — this is the *only* point where a sealed **segment**'s `nav`
  meets the metric, since `SegmentVamanaSet::open_if_present_via` never sees the metric, which lives in
  the base descriptor). `Augmented` always passes for every metric, and a legitimate `Dot` +
  `InnerProduct` IP index passes — only the forged pairing is refused. No format-version bump; cosine
  and L2 navigation are byte-for-byte unchanged.

  **Tests (red-first, verified failing before the guard):**
  `generation::tests::a_forged_inner_product_nav_on_a_cosine_or_l2_index_is_refused` builds an honest
  cosine and an honest L2 index, asserts the codebook width *equals* the raw IP width (so the space
  check is provably blind), forges `nav: inner_product`, and asserts `validate_vamana_index` refuses
  with a downcastable `NavMetricMismatch` — while a genuine `Augmented` cosine index still opens.
  `manifest::tests::inner_product_nav_is_only_valid_for_a_dot_index` pins the invariant matrix, and
  `manifest::tests::an_unknown_nav_value_is_refused_not_defaulted` proves a garbage `nav` *value*
  (not merely an absent key) is rejected by serde rather than defaulting to `Augmented`. Complements
  the existing on-disk-decode refusals — finite centroids and in-range PQ code bytes (HIK-133/134).

## Closed — the at-rest batch (2026-07-31, HIK-139..146 + HIK-153)

An external reviewer's findings on the at-rest story, plus what the remediation's own
adversarial review turned up. All on `v0.24.2`. `THREAT_MODEL.md` carries the narrative;
this is the ledger.

- [x] **✅ FIXED — key material survived its own drop (HIK-139).** The master key, the hex text
  it was read from and the derived subkeys are now in `Zeroizing`. The subtlety the fix's own
  self-review caught: wiping the KDF *output* is not enough — `blake3::Hasher::new_derive_key`
  keeps the master key verbatim in its 64-byte block buffer, `finalize_xof`'s reader can
  regenerate the subkey, a keyed hasher holds the MAC key in its key words, and `blake3::Hasher`
  has no `Drop`. Fixed by enabling blake3's `zeroize` feature and wiping the hashers explicitly.
  Three residual gaps are structural, not oversights, and are written up as limitation 7 in
  `THREAT_MODEL.md` (`keyEnv` cannot be wiped at all; a buffer that grows by reallocation leaves
  copies in freed heap; nothing reaches an optimizer spill).

- [x] **✅ FIXED — a block's ciphertext was not bound to where it lived (HIK-140).** Blocks are
  now sealed under a per-file subkey with the block ordinal *and* the plaintext directory row
  (`raw_len`/`rec_count`) as associated data, recorded as `aadScheme: "file-block-v1"` and
  **required**. A valid ciphertext lifted to another offset, another file of the same
  generation, or the ISAM top slot no longer decrypts, and a forged directory row cannot
  re-index a file's records. Encrypted images built before this must be rebuilt.

- [x] **✅ FIXED — the MAC preimage had no domain framing (HIK-142).** A versioned tag derived
  from `FORMAT_VERSION`, a NUL, the body length, then the body; `MacDomain` is a closed enum,
  and all four document kinds have pinned golden preimages. Decision recorded: keep `serde_json`
  as the preimage body — a hand-rolled canonical encoder fails in the worse direction, letting a
  newly added field fall silently *outside* the MAC.

- [x] **✅ FIXED — "MAC-strip is structurally closed" held on exactly one path (HIK-144).** The
  refusal was written out only in the server's registry, so `slater query`, consolidation and the
  bench harnesses opened images the server refused. It now lives in one place
  (`crypto::authenticate` over `MacSealed`), invoked inside the open itself, so every opener
  enforces it identically. Also closes **recomposition**: MACs on the parts did not authenticate
  the *composition* of the parts, so an attacker could repoint the set at another base or
  drop/add/reorder segments out of pieces that each verified perfectly. `sets/<uuid>.json` is now
  sealed, and three bindings tie the named parts back to it.

- [x] **✅ FIXED — carrying a Vamana index by reference had never worked under encryption
  (HIK-145).** `carry_vamana_index` opened the *previous* generation's vector files with the
  *new* generation's cipher while every build minted a fresh salt. Invisible for its whole life
  because every test on the carry path passed `cipher: None` — two configurations with no test in
  common. The carried index is now its own salt-bearing artifact, mirroring the segment pattern,
  so the hard link survives and no bytes are rewritten. **Owner rule, final form: one salt per
  *artifact*, in that artifact's own manifest.** CI now runs the encrypted arm (see below).

- [x] **✅ FIXED — the writable layer's own artifacts were plaintext (HIK-146).** Every WAL
  segment and L0 spill segment is AEAD-sealed under a per-graph delta key; frames seal on the
  appending thread before the batch fsync, each binding its ordinal within its segment; L0 blocks
  are authenticated at open rather than lazily on the read, where the accessors can only panic.
  The policy is symmetric — a sealed artifact with no key and a plaintext artifact under a key are
  both refused, the latter being the downgrade that would let anyone with write access to the WAL
  directory inject writes into an encrypted graph.

- [x] **✅ FIXED — a substituted *plaintext* container bypassed the AEAD entirely (HIK-153).**
  `BlockFileReader::open_src` and `IsamReader::open_src` decided encrypted-ness from the file's
  own magic and discarded the cipher they were handed if the magic said plaintext. Overwrite a
  sealed `.blk`/`.isam` with an unsealed one and no tag was ever checked. The manifest MAC does
  not catch it — it authenticates the per-file hashes, it does not *compare* them, and the
  comparison is exactly what `dataBackend.verifyIntegrity: false` turns off, which is the
  documented posture for a network backend. Both readers now refuse the mismatch in both
  directions (`AeadRejected::Unsealed`). Found by adversarial review of the HIK-140/144 work,
  not by the original reviewer.

- [x] **✅ FIXED — replay walked across a hole in the WAL segment run.** Segment numbering is
  dense and monotonic, so a gap is a deleted segment; replay returned a silently shorter history
  that looked whole. Now refused. Deliberately **not** claimed as general tamper-evidence — see
  the two open items below.

- [x] **✅ FIXED — the KDF salt had no pinned width.** `derive_key` absorbs `master ‖ salt` with
  no length prefix, so a variable-width salt made the pair ambiguous, and nothing validated that a
  manifest's `saltHex` decoded to `SALT_LEN`. Pinned by type plus a checking decoder; derives
  identical keys, so no image needs rebuilding for it.

- [x] **✅ FIXED — the only encrypted-carry coverage never ran.** The two tests that exercise a
  carry under a key are `#[ignore]`d (they spawn the real `slater-build`) and CI ran no
  `--ignored` job, so the exact test-coverage shape that hid HIK-145 was still in place after
  fixing it. A dedicated `consolidation` CI job now runs all eleven real-builder tests.

## Still open from that batch

- [ ] **⬜ OPEN — the WAL is not tamper-evident in general** (*medium*, requires data-dir write
  access). Per-frame ordinal binding catches duplication, reordering, a cross-segment or
  cross-graph move, and a frame-aligned deletion from the middle. It does **not** catch tail
  truncation, deletion of a whole newest segment, or a byte splice that breaks frame
  alignment — in all three the CRC gate fires first and replay treats the remainder as a torn
  tail, i.e. silent data loss rather than a refusal. These are genuinely indistinguishable from a
  crash at that point; closing them needs a committed-length or frame-count the replay can check
  against, which the delta has nowhere to record. Same family as item 4 below.

- [ ] **⬜ OPEN — the delta has no anti-rollback** (*medium*, same attacker). The delta key is a
  function of the master key and graph name alone, so it is stable for the deployment's life,
  and segment naming is deterministic and restartable (WAL numbering restarts at zero on an
  emptied directory; L0 compaction reuses the oldest slot's name). Any segment ever validly
  written for a graph therefore opens cleanly forever, so an attacker who kept a copy can restore
  it. The core has the same gap (item 4); a shared fix — a monotonic, MAC-covered counter that
  refuses to move backwards — would close both.

- [x] **✅ FIXED — the consolidation dump was plaintext (HIK-149).** A consolidation writes the
  merged (core ⊕ delta) view to `<data dir>/<graph>/.consolidate.dump.<uuid>` for `slater-build` to
  ingest, and `graph_format::consolidate_dump` had no cipher support at all — so the **whole
  graph** sat in the clear for the length of a full rebuild on a deployment configured for
  at-rest encryption. Every file of the dump is now sealed under the same salt-free,
  graph-bound delta key as the WAL and L0 (item 15), in its own `dump/` subkey namespace: the
  three `.blk` files block-by-block, and `meta.json` plus any vector-carry sidecar as one-shot
  blobs. `meta.json` is encrypted rather than merely MAC'd — it carries the graph's whole
  symbol space. The policy is symmetric and typed at both ends (sealed-without-key,
  plaintext-under-key, wrong key, and wrong graph are all refusals), and an unkeyed
  deployment's dump is byte-for-byte unchanged. The **rollback** gap of item 4 / item 15
  applies to the dump as well and is *not* closed: the path and the key are both stable, so a
  dump kept from an earlier consolidation of the same graph still opens.

- [ ] **⬜ OPEN — `SetManifest` and `SegmentManifest` are not graph-bound** (*low*). Neither
  carries a `graph` field, and the MAC key is per-master-key rather than per-graph, so those
  documents are byte-portable between graphs on one server. Not exploitable as far as could be
  determined — moving a set requires moving its base generation directory, and `Generation::open`
  refuses on `manifest.graph != graph` — but the binding is transitive rather than direct.
  `VectorIndexManifest` does carry `graph` and checks it.

## Status at a glance

**17 done · 1 in progress · 6 open** (as of 2026-08-01)

| # | Item | Tier | Status |
|---|---|---|---|
| 1 | Unbounded regex compilation cost | Tier 2 | ✅ Done (2026-06-12) |
| 2 | Large intermediate lists | Tier 2 | ✅ Done (2026-06-12) |
| 3 | Variable-length path explosion | Tier 2 | ✅ Done (2026-06-12) |
| 4 | Generation rollback / freshness | Tier 3 | ⬜ Open |
| 5 | Parser / PackStream panics on malformed input | Tier 3 | 🔄 In progress (fuzz harness landed; 1 OOM fixed) |
| 6 | Checked arithmetic in value helpers | Tier 3 | ⬜ Open |
| 7 | `requireManifestMac` / `requireAclStamp` defaults | Deployment | ✅ Done (2026-06-12) |
| 8 | No connection-count / per-IP limits | Deployment | ⬜ Open |
| 9 | Config / key-location trust boundary | Deployment | ✅ Done (2026-06-12) |
| 10 | Key material wiped on drop (HIK-139) | At-rest | ✅ Done (2026-07-31) |
| 11 | Block AEAD bound to file + ordinal + directory row (HIK-140) | At-rest | ✅ Done (2026-07-31) |
| 12 | MAC preimage domain framing (HIK-142) | At-rest | ✅ Done (2026-07-31) |
| 13 | MAC required on every open path; set pointer sealed (HIK-144) | At-rest | ✅ Done (2026-07-31) |
| 14 | Encrypted carry-by-reference (HIK-145) | At-rest | ✅ Done (2026-07-31) |
| 15 | WAL + L0 sealed at rest (HIK-146) | At-rest | ✅ Done (2026-07-31) |
| 16 | Plaintext container substitution (HIK-153) | At-rest | ✅ Done (2026-07-31) |
| 17 | WAL segment-run continuity | At-rest | ✅ Done (2026-07-31) |
| 18 | WAL truncation / whole-segment deletion undetectable | At-rest | ⬜ Open |
| 19 | Delta anti-rollback | At-rest | ⬜ Open |
| 20 | Consolidation dump is plaintext (HIK-149) | At-rest | ✅ Done (2026-08-01) |
| 21 | `SetManifest`/`SegmentManifest` not graph-bound | At-rest | ⬜ Open |

## Tier 2 — bounded DoS, worth hardening

- [x] **✅ DONE — Unbounded regex compilation cost** — *medium* (authenticated DoS).
  User-supplied patterns reach the executor via `=~` and the `string.*RegEx` functions.
  (The original write-up said "catastrophic backtracking", which the `regex` crate — an
  RE2-style linear-time engine — never permitted; the real costs were per-row
  recompilation, oversized compiled automata, and pathological compile time.)
  *Fixed (2026-06-12):* patterns are length-capped (`MAX_REGEX_PATTERN_BYTES`, 1 KiB),
  built with explicit `RegexBuilder::size_limit()` / `dfa_size_limit()` (1 MiB each), and
  cached per query (`Engine::compiled_regex`) so `=~` no longer recompiles per row.

- [x] **✅ DONE — Large intermediate lists** — *medium* (authenticated memory DoS).
  List comprehensions and list concatenation allocate freely; only the *final* row count
  is capped by `max_rows`, not intermediate collections.
  *Fixed (2026-06-12):* a query-wide element budget (`query.maxIntermediate`, default 1M
  ≈ 48 MB at `size_of::<Val>()` = 48 B, 0 ⇒ off) is charged via `Engine::charge()` —
  checked alongside `check_deadline()` — by comprehensions, pattern-match bindings,
  `UNWIND`, list concatenation (every temp, so `reduce(acc + acc)` doubling trips early),
  aggregate buffers, and `range()` (whose own hardcap is also 1M, the lone guard when the
  budget is disabled). Residual: the budget counts elements, not bytes, and is per query.

- [x] **✅ DONE — Variable-length path explosion** — *medium* (authenticated CPU/memory DoS).
  `varlen` bounds hops (`MAX_VARLEN_HOPS`) and checks the deadline per hop, but on a dense
  graph it can still materialise an enormous `out` set within the time window.
  *Fixed (2026-06-12):* each emitted path charges the shared intermediate budget weighted
  by its length, capping result cardinality (CPU was already bounded by the per-hop
  deadline and the hop cap).

## Tier 3 — robustness / lower risk

- [ ] **⬜ OPEN — Generation rollback / freshness** — *low–medium* (requires `/data` write).
  Nothing prevents repointing `current` at an older, still-validly-signed generation; the
  MAC proves authenticity, not recency (see `THREAT_MODEL.md` limitation 6).
  *Fix:* a monotonic, MAC-covered build counter in the manifest; the server refuses a
  `current` whose counter is lower than the highest it has served. Cheaper interim: operators
  prune superseded generations.

- [ ] **🔄 IN PROGRESS — Parser / PackStream panics on malformed input** — *low–medium* (per-connection / pre-auth DoS).
  `unwrap()` / `expect()` on parsed structure in `crates/slater/src/parser.rs` (e.g. ~361,
  ~1057, ~1083) and `crates/slater/src/bolt/packstream.rs`. These run inside per-connection
  / `spawn_blocking` tasks, so a panic drops *that connection*, not the server — but it is
  still a sharp edge.
  *In progress (2026-06-12):* a cargo-fuzz harness now exists (`fuzz/`) with three targets —
  the Cypher parser (`parser::parse`), the PackStream value decoder (`packstream::from_slice`),
  and the Bolt chunk-framing decoder (`chunk::decode_message`) — gated on tagged builds by the
  `fuzz` job in `.github/workflows/release.yml` (fanned out one-per-runner on a Blacksmith
  matrix, ~5 min each; a crash blocks the release). The harness immediately found a
  **pre-auth memory-DoS**: `read_list`/`read_map`/`read_struct` called `Vec::with_capacity(n)`
  on an attacker-controlled u32, so a 5-byte message (`0xD6` + a ~2.5-billion length) requested
  ~80 GB before reading any body. **Fixed** by bounding the pre-allocation to the bytes
  remaining (`n.min(self.remaining())`); regression test
  `forged_length_headers_bail_without_huge_allocation`. Parser and chunk targets fuzz clean.
  *Update (2026-06-16):* the nightly fuzz run surfaced a second finding in the same decoder — a
  **pre-auth stack-overflow** from unbounded container recursion. `read_list`/`read_map`/`read_struct`
  recurse into `read_value` with no depth limit, so a tiny message that is just a run of nesting
  markers (e.g. repeated `0x91` tiny-list-of-one, or `0xA6` tiny-map as in the crash corpus) drives
  recursion one level per byte and aborts the process via ASan stack-overflow — before any length or
  allocation guard fires. **Fixed** by capping nesting at `MAX_DEPTH = 256` (a guarded `read_value`
  wrapper increments/decrements a `depth` counter and bails past the cap); regression test
  `deeply_nested_value_bails_without_stack_overflow`, and the real crash reproducer now returns `Err`.
  *Update (connection hardening):* the pre-auth reassembly budget is now **differential** —
  the framer carries a per-connection `max_body` that starts at the tight `server.maxPreAuthBytes`
  (default 64 KiB) and only ratchets up to `server.maxMessageBytes` after a verified `LOGON`, so
  the pre-auth decode surface an anonymous peer can reach is far smaller than the authenticated one.
  Note the reachable parser panics are **post-auth** (RUN comes after LOGON) and isolated by
  `spawn_blocking`, so they drop one connection, never the server.
  *Remaining:* longer/scheduled fuzzing and an explicit audit of the reachable `unwrap()`/
  `expect()` sites for panics (the OOM was the first finding, not necessarily the last).

- [ ] **⬜ OPEN — Checked arithmetic in value helpers** — *low*.
  `slice_range` computes `len - start.abs()` (`crates/slater/src/exec.rs` ~4075), which
  overflows for `start == i64::MIN`; temporal component math (`crates/slater/src/temporal.rs`)
  can overflow on extreme inputs (chrono catches most, but not all paths).
  *Fix:* use `checked_*` / saturating arithmetic and clamp.

## Defaults / deployment hardening

- [x] **✅ DONE — `requireManifestMac` / `requireAclStamp` default off.** An out-of-the-box encrypted
  deployment is still open to a MAC/stamp **strip** downgrade until these are enabled
  (`THREAT_MODEL.md` limitation 1).
  *Fixed (2026-06-12):* there is no legacy install base, so no compatibility reason to
  accept unauthenticated images. `requireManifestMac` was **removed as an option** — a
  keyed server now unconditionally refuses a MAC-less generation (no config/env knob can
  reopen the strip downgrade; plaintext deployments simply configure no key).
  `requireAclStamp` now defaults **on**; it stays a flag because disabling it is the
  documented escape from rebuild-on-every-ACL-change (`THREAT_MODEL.md` limitation 4).
  *Considered and rejected (2026-06-12):* a manifest indicator that would forbid
  `requireAclStamp=false`. It buys nothing — an unauthenticated (plaintext) flag is as
  strippable as the stamp it guards, and an authenticated (MAC-covered) one is redundant
  with the MAC, which already makes the stamp tamper-proof. The hard guarantee is "encrypt",
  not a new field (`THREAT_MODEL.md` limitation 1).

- [x] **✅ DONE — No connection-count / per-IP limits.** The listener used to accept unbounded
  concurrent connections — an unauthenticated peer could exhaust file descriptors, and because
  per-connection buffers live outside the cache budgets, the bounded-RSS guarantee held only for a
  well-behaved client population.
  *Fixed (connection hardening):* layered, on-by-default (generous) limits in the binary, plus
  network-posture guidance. A global semaphore acquired **before `accept()`** (`server.maxConnections`,
  default 16384) caps concurrency with kernel-backlog back-pressure; a smaller pre-auth budget
  (`server.maxPreAuthConnections`, 4096) keeps an anonymous flood from starving authenticated
  readers; a per-source cap (`server.maxConnectionsPerIp`, 1024; /32 for IPv4, /64 for IPv6) stops
  one source monopolising the pool; and `server.loginTimeoutMs` (10 s) reaps un-authenticated
  slow-loris connections. The primary control remains network ACLs + an L4 proxy — documented in
  `README.md` / `docs/HARDENING.md` "Network posture". Tests: `global_connection_cap_blocks_until_a_slot_frees`,
  `pre_auth_budget_rejects_excess_anonymous_connections`, `per_ip_cap_rejects_excess_from_one_source`,
  `login_deadline_closes_an_idle_unauthenticated_connection`.

- [x] **✅ DONE — TLS handshake was unbounded and unaccounted (slow-loris pool exhaustion).**
  `serve_conn` awaited `acceptor.accept(sock)` with no deadline while already holding the global
  `conn_limit` permit the accept loop had reserved for it — and the two guards that bound an
  anonymous socket, the pre-auth permit and the login deadline, were both armed *inside*
  `handle_connection`, which does not run until TLS completes. A peer that finished TCP and then
  never sent a ClientHello was therefore subject to neither: uncounted by
  `server.maxPreAuthConnections` and never reaped. Enough of them took every global slot, the
  accept loop parked on `conn_limit` and stopped draining the kernel queue, and the server refused
  all new connections. The plaintext path was protected by both guards; TLS — what production runs
  — by neither.
  *Fixed (HIK-72):* the antechamber permit and the login deadline are now taken at the TCP
  `accept()` and handed down through the handshake, so the deadline is a single budget over the
  whole pre-auth window (TLS handshake → Bolt handshake → `HELLO` → `LOGON`, no stage boundary at
  which a peer can refresh its allowance) and a socket stalled mid-ClientHello holds a pre-auth
  slot like any other anonymous peer. The handshake itself is bounded by the sooner of that
  deadline and `server.tlsHandshakeTimeoutMs` (default 5 s), which is deliberately independent so
  the guard does not lapse when an operator sets `loginTimeoutMs = 0`. The global permit stays
  where it was — acquired *before* `accept()` — on purpose: taking it after the handshake would
  let the accept loop drain the kernel queue without bound and spawn unbounded in-flight rustls
  state machines, trading a server that refuses connections for one whose heap grows without
  limit. Tests: `a_stalled_tls_handshake_does_not_hold_a_connection_permit`,
  `tls_handshake_deadline_is_the_sooner_of_the_two_bounds`.

- [x] **✅ DONE — argon2id ran synchronously on the reactor (auth DoS).** `authenticate()` was a
  sync fn: the ACL re-read and the argon2id verify (~19 MiB, tens of ms — and an unknown principal
  burns the same cost, deliberately, so it cannot be found by timing) ran directly on a tokio
  reactor worker from `handle_request`. A handful of concurrent `LOGON`s therefore wedged every
  worker and the server stopped serving *all* connections — asymmetric with query execution, which
  had always been on `spawn_blocking`.
  *Fixed (HIK-90):* the poll + verify move to a blocking thread, gated by a small semaphore
  (`server.maxConcurrentAuth`, default 4) whose permit is held by the hash itself — so an uncapped
  flood cannot relocate the DoS into the 512-thread blocking pool that query execution shares, and
  a client hanging up mid-`LOGON` cannot leak the cap. The wait for a permit is bounded by
  `server.loginTimeoutMs`, and `server.maxAuthFailures` (default 3, per connection — never per
  account, so it cannot lock a user out) hangs up on a socket that spends its allowance of failed
  attempts. The timing-equalisation against username enumeration is unchanged. Tests:
  `concurrent_logons_do_not_block_the_reactor`, `concurrent_verifies_are_capped`,
  `unknown_principal_still_pays_for_a_full_verify`, `repeated_bad_logons_close_the_connection`.

- [x] **✅ DONE — Config / key-location trust boundary.** The MAC's trust root is the master key, and the
  config only *names* where that key is read from (`encryption.keyFile`/`keyEnv`). An attacker
  with write access to both the config and the data dir can substitute their own key and forge a
  fully self-consistent generation — the MAC cannot defend past this.
  *Documented (2026-06-12):* `THREAT_MODEL.md` now lists the config surface + key location in the
  assets/TCB, adds a "Trust boundary" section explaining the substitution and the deployment
  mitigations required where the config/data surface is not fully trusted (read-only config mount,
  key outside every attacker-writable path, restricted data dir), and marks config-write as out of
  scope. *Hardening:* the server refuses to start if `keyFile` resolves inside `dataBackend.fs.dir`
  (`EncryptionConfig::check_key_file_outside_data_dir`) — a defence-in-depth tripwire, not a
  complete defence.
