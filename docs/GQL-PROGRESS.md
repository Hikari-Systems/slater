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

> **Resume at PR 4 — label boolean expressions `& | !`.**
> Use the "Resume → PR 4" prompt in GQL-PLAN.md §4. Before any new work, run the
> green-state gate. New branch: `gql-label-expressions`.
> **This PR changes the AST** (`NodePat.labels` → a `label_expr`); grep every
> `.labels` reader first and expect broad edits. Commit a WIP-green checkpoint once
> the AST change compiles and all existing tests pass, *before* adding new behaviour.

---

## PR checklist

- [x] **PR 1 — Quantified path patterns `((…)){m,n}`** — branch `gql-quantified-paths`
- [x] **PR 2 — Path restrictors WALK/TRAIL/ACYCLIC/SIMPLE** — branch `gql-path-restrictors`
- [x] **PR 3 — Shortest-path selectors ANY/ALL SHORTEST, SHORTEST k** — branch `gql-shortest-selectors`
- [~] **PR 4 — Label boolean expressions `& | !`** (AST churn) — branch `gql-label-expressions`; WIP-green checkpoint committed (AST swapped to `LabelExpr`, all 386 existing tests green via sugar lowering) before the new `&|!` tests
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

Committed on branch `gql-quantified-paths` (commit `fa260f1`).

### PR 2 — Path restrictors WALK/TRAIL/ACYCLIC/SIMPLE ✅ (branch `gql-path-restrictors`)

Additive again: the quantifier-free, restrictor-free hot path is byte-for-byte
unchanged. Full suite green: **372 passed** (362 pre-existing + 10 new); clippy
clean; fmt clean.

**Approach (so PR 3+ build on it correctly):**
- Grammar: a `path_restrictor` rule (`kw_walk`/`kw_trail`/`kw_acyclic`/`kw_simple`)
  prefixed on `match_pattern` only (`MATCH TRAIL (a)-[:R*]->(b)`). It sits at the head
  of the pattern, never inside `(…)`, so a node variable spelled `walk` is unaffected.
- AST: `ast::PathRestrictor` (`Walk`|`Trail`|`Acyclic`|`Simple`) + additive
  `Pattern.restrictor: Option<PathRestrictor>`. `None` = no explicit restrictor; all
  other Pattern construction sites pass `None` (or carry it through, in
  `reverse_pattern`).
- Executor: `expand_chain` reads `pattern.restrictor` at a variable-length hop and
  threads a `WalkMode` into `varlen`. `walk_mode` folds `None` onto `Trail` because
  slater's `*` has always been edge-unique — so absence ≡ explicit `TRAIL` and only
  `WALK` relaxes. `varlen` gained a node `visited` set (seeded with the walk start)
  for `ACYCLIC`/`SIMPLE`; node-uniqueness implies edge-uniqueness, so those modes skip
  the `used` edge set entirely and `Trail` keeps only `used` — each mode's per-hop
  cost stays minimal.
- Guards: a restrictor is honoured only where `varlen` owns the scope, so `apply_match`
  rejects a restrictor on any pattern with no variable-length relationship (fixed hop
  or node-only) with a clear message rather than silently ignoring it.

**Design decisions / limitations (carried forward, see DECISIONS D36):**
- **Scoped to the variable-length walk.** Restrictors over fixed-length chains are
  rejected (later work). Multiple varlen relationships in one pattern each get an
  independent uniqueness scope, not one spanning the whole path.
- **Restrictor over a quantified group rejected** at lowering (PR 1's desugaring can't
  share a uniqueness scope across the separate fixed-length expansions).
- **`SIMPLE` vs `ACYCLIC`.** `ACYCLIC` forbids every repeated node (endpoints
  included); `SIMPLE` forbids interior repeats but lets the two endpoints coincide (a
  single closed cycle) — the closing hop is emitted but not extended.

**Tests (all green):**
- Parser (`parser.rs`): `lowers_path_restrictors`, `absent_restrictor_is_none`,
  `restrictor_lowercase_accepted`, `restrictor_does_not_shadow_node_var`,
  `restrictor_over_quantified_rejected`.
- Exec (`exec.rs`): `restrictors_distinguish_modes_on_cycle` (the headline — WALK 6,
  TRAIL 4, SIMPLE 3, ACYCLIC 2 paths on a triangle+chord cycle, all distinct),
  `bare_star_equals_trail` (parity: a bare `*` ≡ explicit `TRAIL`),
  `acyclic_excludes_start_that_simple_keeps`, `restrictor_requires_variable_length`,
  `restrictor_over_quantified_group_rejected`.
- New fixture `testgen::write_cycle`: three `:N` nodes, reltype `R`, a→b→c→a triangle
  plus a c→b chord — the minimal graph that tells all four modes apart.

Committed on branch `gql-path-restrictors` (commit `237093c`).

### PR 3 — Shortest-path selectors ANY/ALL SHORTEST, SHORTEST k ✅ (branch `gql-shortest-selectors`)

Additive again: the quantifier-free, restrictor-free, selector-free hot path is
untouched. Full suite green: **386 passed** (372 pre-existing + 14 new); clippy clean;
fmt clean.

**Approach (so PR 4+ build on it correctly):**
- Grammar: a `path_selector` rule (`any_shortest`/`all_shortest`/`shortest_k`) prefixed
  on `match_pattern` *before* the restrictor and the optional path variable
  (`MATCH ANY SHORTEST (a)-[:R*]->(b)`, `MATCH ALL SHORTEST p = …`, `MATCH SHORTEST 3 …`).
  `all_shortest` reuses the existing `kw_all`; `kw_any`/`kw_shortest` are new. Like the
  restrictors it sits only at the pattern head, so a node var spelled `any`/`shortest`
  is unaffected.
- AST: `ast::PathSelector` (`AnyShortest`|`AllShortest`|`ShortestK(u32)`) + additive
  `Pattern.selector: Option<PathSelector>`. All other Pattern construction sites pass
  `None`. `SHORTEST 0` is rejected at lowering.
- Executor: **one shared BFS core** `select_paths(src, dst, rel, bounds, selector)`
  drives both the selector and `shortestPath()`. It returns loopless paths in
  non-decreasing length order — `AnyShortest` ≤1, `AllShortest` every minimum-length
  path, `ShortestK(k)` the first `k`. `eval_shortest_path` now validates its wrapped
  pattern then delegates with `AnyShortest` between two bound nodes (so the function and
  the selector can never diverge — confirmed by the unchanged `phase7_shortest_path*`
  tests). `apply_match_selected` resolves each endpoint (bound node, or scanned +
  `node_ok`-filtered), runs the core per `(src, dst)` pair, binds endpoints / the
  list-valued rel var / any path var, and applies the clause `WHERE` per produced path.
- Guards: a selected pattern is routed out of `apply_match` first; the fast paths and
  the quantified/restrictor branches never see one.

**Design decisions / limitations (carried forward, see DECISIONS D37):**
- **Single relationship, like `shortestPath()`.** Multi-relationship selected patterns,
  selector+restrictor combinations, relationship property filters, and a selector
  sharing its clause with a comma-joined pattern are rejected with clear messages.
- **Endpoints need not be pre-bound** — the real generalisation over `shortestPath()`
  (which requires both endpoints bound). Free endpoints are scanned and label/prop
  filtered.
- **WHERE is applied after selection** (find shortest, then filter), not a
  shortest-subject-to-WHERE search.
- **Loopless paths** (no repeated node), matching `shortestPath()`'s simple-path search.

**Tests (all green):**
- Parser (`parser.rs`): `lowers_path_selectors`, `absent_selector_is_none`,
  `selector_lowercase_accepted`, `selector_with_path_var_follows_prefix`,
  `selector_does_not_shadow_node_var`, `selector_zero_k_rejected`,
  `selector_over_quantified_rejected`.
- Exec (`exec.rs`): `any_shortest_parity_with_shortest_path` (parity vs the function:
  same length + node sequence), `any_shortest_picks_one_of_the_ties`,
  `all_shortest_returns_all_ties` (both length-2 ties, distinct interior nodes),
  `shortest_k_returns_k_in_length_order` (`SHORTEST 2`→2, `SHORTEST 3`→2+1 longer,
  `SHORTEST 4` capped at the 3 existing paths, `SHORTEST 1`≡`ANY SHORTEST`),
  `selector_applies_where_after_selection`, `selector_optional_emits_null_when_no_path`,
  `selector_rejections`.
- New fixture `testgen::write_diamond`: five `:N` nodes, reltype `R`, two length-2
  `s→t` paths (via `a`, via `b`) plus a length-3 detour `s→a→c→t` — the minimal graph
  that tells the three selectors apart.

**Not yet committed** as of this entry — the branch holds the working tree; commit
before clearing context.
