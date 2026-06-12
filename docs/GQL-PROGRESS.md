# Slater — ISO GQL track, progress ledger

**Authoritative, on-disk record of the GQL track.** A fresh session resumes from
this file + `docs/GQL-PLAN.md` alone (see GQL-PLAN.md §4 for the copy-paste resume
prompts). Update it at the end of every PR and keep the green-state gate passing —
**PR boundaries are the only safe context-clear points** (GQL-PLAN.md §3).

Green-state gate (cargo is NOT on PATH):
```
export PATH="$HOME/.cargo/bin:$PATH"
cargo build -p slater && cargo test -p slater && cargo clippy -p slater --all-targets -- -D warnings && cargo fmt -p slater -- --check
```

---

## NEXT ACTION

> **Resume at PR 2 — path restrictors WALK / TRAIL / ACYCLIC / SIMPLE.**
> Use the "Resume → PR 2" prompt in GQL-PLAN.md §4. Before any new work, run the
> green-state gate. New branch: `gql-path-restrictors`.
> Heed PR 2's central design decision (GQL-PLAN.md §2): scope restrictors to
> variable-length `-[*]-` patterns where `varlen` already owns the `used` edge set;
> reject restrictor-over-quantified-group for now.

---

## PR checklist

- [x] **PR 1 — Quantified path patterns `((…)){m,n}`** — branch `gql-quantified-paths`
- [ ] **PR 2 — Path restrictors WALK/TRAIL/ACYCLIC/SIMPLE**
- [ ] **PR 3 — Shortest-path selectors ANY/ALL SHORTEST, SHORTEST k**
- [ ] **PR 4 — Label boolean expressions `& | !`** (AST churn)
- [ ] **PR 5 — FOR, dialect prefix, GQLSTATUS, value gap-fill, docs**

Status keys: `[ ]` todo · `[~]` in progress · `[x]` done-green · `[!]` blocked.

---

## Log

### PR 1 — Quantified path patterns ✅ (branch `gql-quantified-paths`)

Delivered an additive implementation that leaves the quantifier-free hot path
unchanged. Full suite green: **362 passed** (346 pre-existing + 16 new); clippy
clean; fmt clean.

**Approach (so PR 2+ build on it correctly):**
- Grammar: a separate `match_pattern` rule (used only by `match_clause`) carries the
  quantifier; the shared `pattern` rule (shortestPath/EXISTS/comprehensions) stays
  quantifier-free, so those contexts can never receive a segment they can't run.
- AST: new `ast::Segment` enum (`Hop` | `Quantified{inner, bounds, exit}`) + additive
  `Pattern.segments: Option<Vec<Segment>>`. `None` = ordinary pattern (all ~15
  `.rels` consumers untouched); `Some` only when a group appears, with `rels` empty.
- Executor: `apply_match` dispatches segment-bearing patterns to
  `apply_match_quantified`, which **desugars a bounded group into the union of its
  fixed-length expansions** (`expand_quantified_pattern` → one ordinary pattern per
  repetition count, cartesian across multiple groups) and runs each through the
  existing matcher — reusing edge-uniqueness, `node_ok`, the intermediate budget and
  the deadline. `QUANT_MAX_UNROLL = 32` caps expansion.
- Guards: the three single-node fast paths (`try_count_fast_path`,
  `try_grouped_index_fast_path`) and `try_stream_match` now also check
  `segments.is_some()` so a quantified pattern (empty `rels`) is never mistaken for a
  node-only match.

**Design decisions / limitations (carried forward):**
- **Bounded only.** `+`, `*`, `{m,}` (open upper) and `{0,n}` (lower < 1) are
  rejected at execution with clear messages. Unbounded forms are PR 2/later work
  (they need a real repeater, not desugaring).
- **WALK semantics inside a group.** Desugared groups expand to plain hops, so they
  carry no cross-hop edge-uniqueness — unlike Cypher `*` (which `varlen` makes
  edge-unique). PR 2 must reconcile this; for now a restrictor over a quantified
  group should be rejected.
- **Group variables not exposed.** Inner node/relationship variables are anonymised
  (intermediate node labels/props are preserved; only the names are dropped); a path
  variable over a quantified pattern is rejected. GQL group-variable *lists* are a
  later feature.
- **Inner start-node constraints.** Labels/props on a group's inner *first* node are
  rejected (they would have to be enforced at every junction).

**Tests (all green):**
- Parser (`parser.rs`): `ordinary_pattern_has_no_segments`, `lowers_quantified_range`,
  `lowers_quantifier_bound_forms`, `lowers_multi_hop_quantified_inner`,
  `lowers_hop_then_quantified_mixed`, `quantified_rejects_path_variable`,
  `quantified_rejects_inner_start_labels`, `bare_pattern_rejects_quantifier`.
- Exec (`exec.rs`): `quantified_path_equals_varlength`,
  `quantified_exact_equals_fixed_varlength`,
  `quantified_multi_hop_inner_matches_unrolled`,
  `quantified_dialect_switch_across_union`, `quantified_mixed_with_plain_hop`,
  `quantified_count_bypasses_fast_path`, `quantified_unbounded_rejected`,
  `quantified_zero_lower_bound_rejected`.
- Dual-dialect coverage: GQL↔Cypher parity (`{1,2}`≡`*1..2`, `{2}`≡`*2..2`, multi-hop
  inner ≡ unrolled chain) and dialect *switching* (Cypher `UNION` GQL; Cypher hop +
  GQL group in one pattern).

**Not yet committed** as of this entry — the branch holds the working tree; commit
before clearing context.
