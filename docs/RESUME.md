# Resuming the Slater build (after a context clear)

This is a large, multi-session build. State lives **on disk**, not in the chat.
After a `/clear`, paste the prompt below into a fresh Claude Code session.

## Resume prompt

```
Resume the Slater build. Working dir: /home/rickk/git/hs/slater

First read docs/PROGRESS.md (start at the NEXT ACTION block) and docs/DECISIONS.md,
then docs/PLAN.md for the milestone named in NEXT ACTION. Follow the resume protocol
in PROGRESS.md:

1. Confirm the tree is green before doing new work:
   export PATH="$HOME/.cargo/bin:$PATH"
   cargo build && cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --all -- --check
2. Do the next milestone's work, keeping tests green at every step.
3. Before you finish, update docs/PROGRESS.md (status, test names + pass state, NEXT
   ACTION) and docs/DECISIONS.md, and re-run the green-state check.

Notes: cargo is NOT on PATH by default — always export it first. hs-utils is a
git+tag dependency (do not change it to a path dep). Keep British English in docs,
comments and log messages.
```

## Why this works

- **`docs/PROGRESS.md`** is the authoritative ledger: a top **NEXT ACTION** block
  (the single next step + the green-state command), a milestone checklist with
  status (`[ ] / [~] / [x] / [!]`), and a per-milestone log with the exact test
  names and any deviations from the plan. The prompt is milestone-agnostic — it
  always resumes at whatever NEXT ACTION points to, so the *same* prompt carries you
  from M3 → M4 → … as each milestone completes and updates the ledger.
- **`docs/DECISIONS.md`** is the append-only log of `// DESIGN:` choices + rationale,
  mirrored from the in-code comments, so decisions survive even if a file is rewritten.
- **`docs/PLAN.md`** is the frozen implementation plan (the contract for what we are
  building). Read only the section for the current milestone.

## Safe points to clear

Only clear context at a **milestone boundary** — i.e. when `cargo build` plus that
milestone's tests are green and `docs/PROGRESS.md` has been updated. Mid-milestone,
the ledger may not yet reflect reality; finish to a green, documented state first.

## Environment reminders

- Cargo is installed via rustup but **not on `PATH`** by default in this shell:
  `export PATH="$HOME/.cargo/bin:$PATH"` before any cargo command.
- `hs-utils` resolves over the **git CLI** (`.cargo/config.toml` sets
  `net.git-fetch-with-cli = true`); keep it a `git + tag` dependency, never a path dep.
- British English throughout docs, comments, and log messages.
