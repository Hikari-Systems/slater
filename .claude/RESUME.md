# Resume prompt — slater at-rest / delta-count batch

Paste everything below into a fresh session.

---

Continue the slater ticket batch. Work tickets from the Linear project **slater** (team
Hikari Systems) one at a time, following the working agreement in the project description —
read it first, it is mandatory and not the same as ordinary practice. **It changed on
2026-08-01**: the comment thread must be re-read *before every numbered step*, and human
comments are instructions (`HOLD:` stops work until answered, `NOTE:` must be acknowledged
with how it changed the plan, anything else is a correction requiring a revised plan before
more code).

## Where things are

* Repo `/home/rickk/git/hs/slater`, on branch `v0.24.3` — the integration branch for this
  batch (the project description no longer names a specific one; confirm at claim time).
* `v0.24.3` is **13 commits ahead of `main`** at `7d86957`. `main` is **53 commits ahead of
  `origin/main`** at `be517b1`. **Nothing is pushed and nothing is tagged** — do not push or
  tag without asking.
* Version files still say `0.24.2` (Cargo.toml + README + DOCKERHUB). If this batch becomes
  `v0.24.3`, all three need bumping — `.githooks/pre-push` refuses a tag otherwise.
* Per-ticket worktrees at `/home/rickk/git/hs/wt/hik-NNN` on `fix/hik-NNN`. All reclaimed
  except `hik-126` (an old `v0.23.1` one, unrelated).
* Build with `CARGO_TARGET_DIR=/home/rickk/.cache/slater-target-hikNNN` per ticket, and
  `dangerouslyDisableSandbox: true` (the default `target/` is sandbox-denied).
* `cargo test --workspace` on `v0.24.3` is **1527 passed / 0 failed**; clippy
  (`--all-targets -D warnings`, plus the four feature-gated variants in `ci.yml`) and
  `cargo fmt --check` are clean. That is the baseline to keep.

## What is done

HIK-149 (seal the consolidation dump), HIK-150 (edge-tombstone degree mis-count), HIK-151
(Stage B gate), HIK-152 (node-tombstone edge losses vs the segment stack) — all merged to
`v0.24.3` and left **In Review**. See the `hik-149-152-batch-2026-08-01` memory.

**17 tickets sit in In Review awaiting the user's QA** (HIK-139..152, 153..157). Leave them
there — agents never move a ticket to Done.

## What is open

* **HIK-158** (Medium, Backlog) — filed during HIK-149's review. Two vector indexes can
  sanitise to the same carry-sidecar filename (`carry.Doc_b_a.ids`) and the second silently
  overwrites the first. Not caused or worsened by HIK-149.
* One stale doc noted but not fixed: `flush_graph_to_segment`'s "Scope (slice 4.1):
  births-only … a tombstone is refused" is no longer true — the flush resolves node and edge
  tombstones now, and HIK-150's self-healing test depends on it doing so.

## Things already learned the hard way — do not re-derive these

* **Never move a ticket to Done.** Agents hand off to In Review; a human closes after QA.
* **Post the plan comment BEFORE writing code.** I skipped it on HIK-151 and had to record
  the deviation on the ticket. It is the intended intervention point; write it to be argued
  with (state the alternative rejected, not just the steps).
* **Observe the red test first, and check it is red for the *right reason*.** Several tickets
  here specified regression tests that would have passed against the unfixed code. Where a
  fix is already written, `git stash` it and re-run rather than arguing the case.
* **Ask "does any test exercise the call production actually makes?" — and "is there a second
  *implementation*?"** Five bugs in this batch were that shape; HIK-149's was a duplicated
  decode in `slater-build/src/shared.rs` that only production reached.
* **Measurement traps that produce false greens:** `serde_json`'s `get("encryption")` returns
  `Some(Value::Null)` for an explicit null — assert `is_some_and(|v| !v.is_null())`.
  `GlobalIntermediateBudget` cannot see an anchor scan — use `Engine::anchor_ids_scanned()`.
  A marker grep through a zstd-compressed dump is nearly a coin flip — pair it with a
  structural assertion. `ADJ_VISIT_COUNT` is the non-flaky way to assert a fast path was
  entered.
* **Verify a reported severity before acting on it**, and check whether the ticket's repro
  actually reaches the bug (HIK-152's needed `DETACH DELETE`, not `DELETE`).
* The step-6 self-review must **try to break the diff**, not sign it off. In this batch it
  found a real defect in my own work every single time.
* When waiting on a background command, match on a **marker in its output file**, never
  `pgrep -f "cargo test"` — that pattern matches the waiting shell's own argv and deadlocks
  forever. 19 such loops from earlier sessions were found still spinning and killed.

Relevant memories: `hik-149-152-batch-2026-08-01`, `linear-ticket-lifecycle`,
`test-seams-hide-production-paths`, `adversarial-self-review`,
`external-review-remediation-2026-07-22`, `no-prs-direct-to-main`,
`build-target-dir-sandbox`, `release-version-matches-tag`.
