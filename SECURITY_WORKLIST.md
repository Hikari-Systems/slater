# Slater security worklist

Items from the security review of 2026-06-12. The headline ACL-stamp-on-reload fix and the
Tier-1 DoS caps were implemented in that pass (see `THREAT_MODEL.md`); the rest were triaged
as lower priority and recorded here. Several have since been completed — each item lists where
it lives, why it matters, the fix, and its current status. Each line also carries a GitHub
checkbox (`[x]`/`[ ]`) and an inline **✅ DONE** / **⬜ OPEN** tag.

Severity reflects impact assuming the documented trust model (read-only server; the data
dir and `acl.json` are protected by filesystem permissions; queries arrive from
authenticated principals over Bolt).

## Status at a glance

**5 done · 1 in progress · 3 open** (as of 2026-06-12)

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

- [ ] **⬜ OPEN — No connection-count / per-IP limits.** The listener accepts unbounded concurrent
  connections. *Action:* document fronting with a proxy/limits, or add a `maxConnections`
  config.

- [x] **✅ DONE — Config / key-location trust boundary.** The MAC's trust root is the master key, and the
  config only *names* where that key is read from (`encryption.keyFile`/`keyEnv`). An attacker
  with write access to both the config and the data dir can substitute their own key and forge a
  fully self-consistent generation — the MAC cannot defend past this.
  *Documented (2026-06-12):* `THREAT_MODEL.md` now lists the config surface + key location in the
  assets/TCB, adds a "Trust boundary" section explaining the substitution and the deployment
  mitigations required where the config/data surface is not fully trusted (read-only config mount,
  key outside every attacker-writable path, restricted data dir), and marks config-write as out of
  scope. *Hardening:* the server refuses to start if `keyFile` resolves inside `dataDir`
  (`EncryptionConfig::check_key_file_outside_data_dir`) — a defence-in-depth tripwire, not a
  complete defence.
