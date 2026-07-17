# QA handoff — `v0.23.1` integration branch

**Branch:** `v0.23.1` (on `origin`) · **HEAD at write time:** `b2c1b9a`
**Scope:** the 2026-07-16 whole-codebase review remediation (HIK-122..136) + the dot-product/MIPS feature (HIK-137).
**Status convention:** agents never set `Done`. Everything below is `In Review` / `In Progress` awaiting human QA. A human closes after sign-off.

> **Calibration note for the reviewer.** Five of six of the orchestrator's *follow-up determinations* this session were corrected by the implementing agents (wrong causal chains, tests that would have passed against unfixed code, a proposed fix that didn't work, a cost that didn't exist). The **fixes are sound because the agents caught these.** But it means the **original ticket descriptions are the least-trustworthy artifact** — when reviewing, trust the agents' comments and the tests over the orchestrator's original framing. Instances are flagged inline below.

---

## 1. Review fixes — verify + close (14 tickets, all In Review)

Each has a regression test **observed red before the fix** and an adversarial self-review posted on the ticket.

### Security / data-loss — weight these hardest

| Ticket | Fix | QA focus |
|---|---|---|
| **HIK-123** | LOGOFF left prior user's `pending` rows + `tx_graph` for the next user (cross-user leak + read-ACL bypass) | Verify the two single-connection tests: A→RUN→LOGOFF→B→PULL must not leak A's rows; A→`BEGIN{db:g1}`→LOGOFF→B(no grant)→RUN must be denied. **Parked decision — see §3.2.** |
| **HIK-122** | Consolidation permanently destroyed a label-removed embedding | ⚠️ **The orchestrator's causal chain was wrong.** The agent refuted it with a red test and corrected it on the ticket — read that comment; the real loss is at a different step than the description states. Verify write→remove-label→consolidate→re-add-label. |
| **HIK-135** | Zero-filled WAL tail passed CRC (`crc32c("")==0`) and wedged startup | 4-line fix; verify a zero-padded committed segment replays (not `bail`). |

### Correctness — standard verify

HIK-124, HIK-125 (disk-cache: unbounded disk across restarts; unaccounted RSS — **note these were untested in CI, see HIK-138**), HIK-126 (Elias-Fano low-bits width), HIK-127 (duration overflow — **parked decision §3.1**), HIK-128 (pq offset table), HIK-129 (De Morgan index filter — **deliberately landed a known permanently-empty-index wart to avoid a worse silent-drop; read its comment**), HIK-132 (EF `saturating_sub` — **`EfMono` still open, §3.3**), HIK-133 (PQ code bytes), HIK-136 (PERF-REPORT overclaim, doc-only — superseded by HIK-137 phase 4's quantitative version).

### Confirmed but not fully closed

- **HIK-130 — WAL retro-commit.** Finding **confirmed real** (a commit marker can survive a failed fsync and retro-commit an unacked batch). The orchestrator's proposed fix was **implemented and proven not to work** — `sync_data` makes bytes durable *before* the fsync gating them, so no write ordering closes it; measured cost of the attempted fix was +54–111%/batch "to close nothing." What shipped: a truthful docstring, `warn!` on the previously-silent failed-rollback path, and a characterization test. **The residual retro-commit window is left open by design (irreducible, fsyncgate-class).** QA decision: accept the residual, or escalate.

### Refuted — close as won't-fix

- **HIK-131 — L0 `.expect()`.** Panic on a bit-rotted L0 block is real, but the severity claim ("aborts the whole server process") is **false** — queries run on `spawn_blocking` with an existing error arm; blast radius is one query. **No code change.** The orchestrator's suggested fix would have been a *regression* (`Err→None` returns stale data instead of failing). Reasoning on the ticket.

---

## 2. HIK-137 — dot-product (MIPS) support — In Progress, functionally complete

**Result:** recall@10 lifted from the augmented **0.868 / 0.407 / 0.395** to **~0.994 / 0.997 / 1.000** (uniform / lognormal / pareto), and preserved across the whole ladder. Cosine/L2 untouched; **no FORMAT_VERSION bump** (additive-optional `nav` discriminator).

Built in four gated phases, none on spec:
1. D1 dataset + independent brute-force IP ground truth.
2. Spike (proved the ip-NSW *graph* under exact IP: 0.998/0.998/1.000).
3. Base build — incl. a pre-format PQ-under-IP checkpoint (0.994/0.997/1.000) that gated the format work.
4. Ladder integration + hardening.

**QA focus:**
- Per-rung recall table (phase-3 comment): base / T0 / T2 / T4b / T4a. **Known caveat: T4b (merge) is ~0.95–0.97, marginally below base** — expected cost of incremental insert-weave, not a regression. Confirm it's acceptable.
- **Phase-4 security fix (worth a close look):** a forged `nav: inner_product` on a *cosine/L2* index previously mis-navigated (the codebook-width check couldn't catch it — IP and cosine/L2 codebooks are the same width). Now refused via a typed `check_metric`, at both the base-open and segment-query sites. Same class as HIK-128/133/134. THREAT_MODEL.md + SECURITY_WORKLIST.md updated.
- Verify cosine/L2 behaviour is genuinely unchanged (every change gates on `Dot`/`InnerProduct`; `Augmented` manifests serialize byte-identically).

This is the only ticket genuinely *in progress* rather than done-pending-QA; the feature is complete but a human should validate before it's called shipped.

---

## 3. Parked decisions — the orchestrator's to raise, yours to make (not code-verifiable)

1. **HIK-127 — deliberate behaviour change.** Durations now cap at **260,172 years** (chrono's calendar edge); `duration({days: 1e8})` was a (silently-wrong) answer and is now a clean error. Reversible in one line if you disagree.
2. **HIK-123 — deferred sub-case.** A *failed* LOGON leaves `sess.user` as the prior identity. Same bug class; grants an attacker nothing they don't already hold; failing closed would change LOGON-failure semantics for token-rotation clients. Agent deferred it as a judgement on principle.
3. **HIK-132 — `EfMono` uncovered.** The chosen fix (option 2a, `saturating_sub` on `EfChunk::degree_at`) does not cover the sibling `EfMono` non-monotone hazard (no subtraction to saturate — it's a binary-search-on-broken-monotonicity wrong-answer). Explicitly out of scope; needs its own ticket if wanted.

---

## 4. Open follow-ups (filed) and unticketed items

**Filed, Backlog:**
- **HIK-138 (High)** — **CI never compiles graph-format's s3/gcs test code.** Plausibly why HIK-124/125 survived — the disk-cache paths are behind a feature flag CI never sets, so `cargo test --all` runs zero of those tests and a non-compiling feature-gated test is invisible. **Recommended next.**
- **HIK-134** — the NaN finiteness gate is the correct dot-product baseline (merged). Its read-side centroid backstop remains as defense-in-depth.
- **HIK-136 / HIK-137** — dot-recall documentation now quantitatively corrected (phase 4).

**Unticketed (file if you want them tracked):**
- **`toInteger()`** in slater-scalar has HIK-127's identical saturating f64→i64 trap (`toInteger(1e19)`→i64::MAX, `toInteger(NaN)`→0).
- **`unwrap_or_else(epoch_date)`** appears ~5× as the common root of a silent-1970 date family.

**Minor test-verification notes (on tickets, non-blocking):**
- HIK-134: a `vecf32([1e400])` *literal* write rejects with a generic parse message rather than the precise finiteness one (invariant holds; message imprecise for that rare literal-±inf case; the common `log`/`0.0/0.0` sources get the typed error).
- HIK-134: `ann_point`'s pre-existing `.unwrap()` would panic if a non-finite reached it — unreachable given the ingest gates.

---

## 5. Merge / release readiness

- All 14 review fixes + HIK-137 are on `origin/v0.23.1` (`b2c1b9a`), fast-forward-only history from `main` (`d5974c5`).
- Full suites green on the integration branch; `clippy --all-targets -D warnings` clean except a **pre-existing, unrelated** `object_store_readamp` warning present on the `main` baseline.
- When QA passes: fast-forward `main` → `v0.23.1`, bump the version to match, cut the 0.23.1 release (per the release-version-matches-tag + pre-tag-docker-build-check conventions — verify the Dockerfile dep-cache stub list names every new bench/bin, incl. `ipnsw_spike` / `ipnsw_ladder_e2e`).
