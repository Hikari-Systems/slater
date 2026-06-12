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

> **Resume at PR 5 — FOR, dialect prefix, GQLSTATUS, value gap-fill, docs.**
> Use the "Resume → PR 5" prompt in GQL-PLAN.md §4. Before any new work, run the
> green-state gate. New branch: `gql-finish`. PR 5 is several small *independent*
> items — they can be separate commits. This is the final PR; mark the track
> COMPLETE when done.

---

## PR checklist

- [x] **PR 1 — Quantified path patterns `((…)){m,n}`** — branch `gql-quantified-paths`
- [x] **PR 2 — Path restrictors WALK/TRAIL/ACYCLIC/SIMPLE** — branch `gql-path-restrictors`
- [x] **PR 3 — Shortest-path selectors ANY/ALL SHORTEST, SHORTEST k** — branch `gql-shortest-selectors`
- [x] **PR 4 — Label boolean expressions `& | !`** — branch `gql-label-expressions`
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

Committed on branch `gql-shortest-selectors` (commit `2a499fe`).

### PR 4 — Label boolean expressions `& | !` ✅ (branch `gql-label-expressions`)

The one PR with AST churn, done last so the pattern AST had settled. The non-label
hot path stays byte-for-byte unchanged. Full suite green: **399 passed** (386
pre-existing + 13 new); clippy clean; fmt clean. A WIP-green checkpoint (commit
`6263fa8`) captured the AST swap with all 386 existing tests passing, before the new
`&|!` behaviour tests — the documented mid-PR sub-boundary (GQL-PLAN.md §3).

**Approach (so PR 5 builds on it correctly):**
- Grammar: `labels` and `rel_types` both become `":" ~ label_expr`, a precedence
  climb `le_or` (`|`) → `le_and` (`&` *or* `:`) → `le_not` (leading `!`s) → `le_atom`
  (a name or a parenthesised sub-expression). The classic sugar lowers into the SAME
  tree: `:A:B` → `And` (the `:` is an AND connector), `:T1|T2` / `:T1|:T2` → `Or` — so
  every pre-GQL query parses unchanged. The WHERE postfix predicate `n:A:B`
  (`label_pred`, `Expr::HasLabels`) is a separate rule and keeps its AND-only form.
- AST: one shared `ast::LabelExpr` (`Atom`/`And`/`Or`/`Not`) reused for *both* node
  labels and relationship types — `NodePat.label_expr: Option<LabelExpr>` and
  `RelPat.type_expr: Option<LabelExpr>` (`None` ≡ no constraint). Reusing one enum
  (rather than a parallel type) kept the blast radius to a single evaluator and one
  grammar. Helpers: `as_single_atom` (the fast-path probe), `positive_atoms` (single
  atom or `OR`-tree of atoms → flat name list), `required_atoms`/`NodePat::
  required_labels` (conjunctive positive atoms, for the planner), and `eval` (plain
  boolean over a present-predicate — no three-valued logic; a label is present or
  absent).
- Executor — nodes (`node_ok`): a single positive atom the anchor scan already proved
  skips the label-record decode entirely (the pre-GQL hot path, preserved exactly);
  any boolean expression decodes the resident labels once and evaluates, folding the
  anchor-guaranteed labels into the present-predicate. An atom naming an unknown label
  is simply absent (so `!Unknown` holds, `Unknown` fails) — sound set logic.
- Executor — relationships (`expand_one_hop`): the type constraint resolves once,
  before the per-edge loop, into a `TypeFilter`. Untyped / single `:T` / `:T1|T2`
  alternation collapse to a flat reltype-id set (`positive_atoms`) so the hot loop
  stays the pre-GQL `ids.contains` integer test; only a genuine `&`/`!` type
  expression falls to per-edge `eval` (evaluated over the edge's singleton type, so
  `:A&B` is correctly always empty).
- Planner (`choose_from_preds`): reads `node.required_labels()` (the conjunctive
  positive atoms) wherever it used `node.labels`. For `:A` / `:A:B` this is identical
  to before, so existing plans are unchanged; a disjunction/negation yields no
  required label and falls back to a full scan + `node_ok` re-check. The single-node
  count/group fast paths (`try_count_fast_path`, `try_grouped_index_fast_path`) gate
  on `as_single_atom`, so only the lone-positive-atom case takes the posting/index
  shortcut.

**Design decisions / limitations (carried forward, see DECISIONS D38):**
- **One `LabelExpr` for nodes and rel-types**, evaluated as plain boolean set
  membership (relationships over their singleton type).
- **Sugar lowers, never special-cases.** `:A:B` and `:T1|T2` produce ordinary
  `And`/`Or` trees; there is no separate code path for them.
- **Single-positive-atom fast path preserved** end to end (LabelScan + guaranteed
  label + decode-skip); only `&`/`|`/`!`/parens take the general evaluator.
- **WHERE postfix `n:A:B` predicate unchanged** (AND-only) — out of scope, smaller
  blast radius.

**Tests (all green):**
- Parser (`parser.rs`): `lowers_label_and_with_colon_sugar` (`:A&B` ≡ `:A:B`),
  `lowers_label_or_on_node_and_reltype`, `lowers_label_negation`,
  `label_expr_precedence_not_over_and_over_or` (`!A&B`≡`(!A)&B`, `A|B&C`≡`A|(B&C)`),
  `label_parens_override_precedence` (`(A|B)&C`), `absent_label_is_none`.
- Exec (`exec.rs`, basic fixture): `label_boolean_node_cardinalities` (OR=5, NOT=2,
  AND=0), `colon_chain_lowers_to_and_not_or` (parity: `:A:B` ≡ `:A&B`, not OR),
  `label_boolean_reltype_cardinalities` (OR/NOT/AND on KNOWS/WORKS_AT),
  `reltype_alternation_parity_with_single_types` (`:T1|T2` ≡ union of single types).
- Planner (`plan.rs`): `single_positive_label_atom_still_picks_label_scan`
  (perf-parity), `conjunction_label_scans_the_smaller_posting` (`:A&B` → smaller
  posting), `disjunctive_or_negated_label_expr_falls_back_to_all_nodes`.

Committed on branch `gql-label-expressions`.
