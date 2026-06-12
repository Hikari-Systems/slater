# Slater — ISO GQL read-subset support (frozen plan)

**This is the frozen contract for the GQL track.** It is independent of the main
build plan (`docs/PLAN.md`, milestones M1–M9, complete). State lives on disk: read
this with `docs/GQL-PROGRESS.md` (the living ledger). After a context clear, resume
from the prompt in the "Resume prompts" section that matches the ledger's NEXT
ACTION.

The work is sequenced as five PRs, each on its own branch, each independently
green. **PR 1 is delivered** (branch `gql-quantified-paths`). PRs 2–5 remain.

---

## 1. Durable architecture context (a fresh session needs this)

**No new sockets / listeners / ports.** Neo4j accepts GQL over the *same* Bolt
protocol: the Bolt `RUN` message carries `{query, parameters, extra}` and the query
string is opaque to the protocol — the server decides how to parse it. Neo4j
selects dialect with a query-string prefix (`CYPHER 5` / `CYPHER 25`), never a
protocol negotiation. So slater's entire Bolt stack (`crates/slater/src/bolt/*`,
the listener in `server.rs`) is **untouched** by this whole track. Every PR lives
in the parser and the executor only.

**Pipeline.** `parser::parse(&str)` (`parser.rs:358`) → AST (`parser.rs` `mod ast`)
→ `Engine::run(&Query)` (`exec.rs:778`). There is one parser, one AST, one
executor — GQL is an *extension* of each, not a second language.

**Anchor points (verified against the tree at PR 1):**

| Concern | Location |
|---|---|
| Grammar (online query language) | `crates/slater/src/cypher.pest` |
| MATCH-only grammar (quantifier-capable) | `match_pattern` rule (PR 1 added it; `pattern` stays plain) |
| Parse → AST lowering | `parser.rs` `lower_*` fns; `lower_match_pattern` (PR 1) |
| AST types | `parser.rs` `mod ast` — `Pattern`, `RelPat`, `NodePat`, `Direction`, `VarLength`, `Segment` (PR 1) |
| MATCH execution | `exec.rs` `apply_match` (~1175); quantified path `apply_match_quantified` (PR 1) |
| Pattern traversal | `expand_chain` (~2360), `expand_one_hop` (~2534), `varlen` (~2495), `varlen_bounds` (~4748) |
| Node label/prop check | `node_ok` (~2645) |
| Shortest path (single-rel BFS) | `eval_shortest_path` (~3127), reached from `Expr::ShortestPath` |
| Quantifier desugaring | `expand_quantified_pattern`, `repeat_inner`, `cartesian_patterns` (PR 1, near `reverse_pattern`) |

**Test harness.**
- `run(tag, q) -> (PathBuf, QueryResult)` and `run_result(tag, q) -> Result<QueryResult, String>` (PR 1) build the basic fixture, run `q`, clean up.
- Fixture `testgen::write_basic` topology: nodes `0 Alice:Person, 1 Bob:Person, 2 Carol:Person, 3 Acme:Company, 4 Globex:Company`; `KNOWS` = Alice→Bob, Bob→Carol, Alice→Carol; `WORKS_AT` = Alice→Acme, Carol→Globex.
- `col0(res)` / `gql_col0(tag,q)` give sorted first-column display strings for order-free assertions.

**Environment & quality bar (mirrors `docs/RESUME.md`):**
- `cargo` is **not on PATH**: `export PATH="$HOME/.cargo/bin:$PATH"` before any cargo command.
- Green-state gate for every PR:
  `cargo build -p slater && cargo test -p slater && cargo clippy -p slater --all-targets -- -D warnings && cargo fmt -p slater -- --check`
- British English in docs, comments, log/error messages. Match surrounding comment density and idiom — this codebase comments the *why* heavily.

---

## 2. The five PRs

### PR 1 — Quantified path patterns `((…)){m,n}` ✅ DELIVERED
Branch `gql-quantified-paths`. Grammar `match_pattern`/`quantified_path`/
`quantifier_bounds`; AST `Segment` + additive `Pattern.segments`; executor
`apply_match_quantified` desugars a **bounded** group into the union of its
fixed-length expansions (one ordinary pattern per repetition count) and runs each
through the existing matcher. Unbounded (`+`,`*`,`{m,}`) and `{0,n}` are rejected
with clear messages. 16 tests (8 parser, 8 exec) incl. GQL↔Cypher parity and
cross-dialect switching (UNION, mixed hop+group). See `docs/GQL-PROGRESS.md` for the
test list and the design decisions it recorded.

### PR 2 — Path restrictors `WALK` / `TRAIL` / `ACYCLIC` / `SIMPLE`
**Goal:** control repeated node/edge use over variable-length patterns.

**Central design decision (read before coding):** slater's `varlen` (`exec.rs:2495`)
*already* enforces relationship-uniqueness within a path (the `used` edge set) —
that is TRAIL semantics. PR 1's desugared quantified groups, by contrast, expand to
*plain* hops and so carry **no** cross-hop uniqueness (WALK semantics). PR 2 must
therefore:
1. **Scope restrictors to variable-length `-[*]-` patterns first** (where `varlen`
   already owns a `used` set). This is the cheap, high-value slice.
2. Map each restrictor onto the `varlen` walk:
   - `WALK` → relax: allow repeated edges (drop the `used`-edge check).
   - `TRAIL` → current behaviour (edge-unique) — make it the explicit default for `*`.
   - `ACYCLIC` / `SIMPLE` → add a **node-uniqueness** visited set (no repeated node,
     except possibly the endpoints — confirm GQL's SIMPLE-vs-ACYCLIC node rules and
     encode the difference).
3. **Defer restrictor-over-quantified-group** (e.g. `TRAIL ((x)-[:R]->(y)){1,3}`):
   PR 1's desugaring can't share a uniqueness scope across expansions. Either reject
   it for now with a clear message, or (later increment) stop desugaring when a
   restrictor is present and run a dedicated repeater that threads one `used` set.
   Pick rejection for PR 2 to keep scope tight; note it in DECISIONS.

**Grammar:** add `kw_walk`/`kw_trail`/`kw_acyclic`/`kw_simple` and a
`path_restrictor` rule placed before a path in `match_pattern` (GQL pattern-level
placement, e.g. `MATCH TRAIL (a)-[:R*]->(b)`).
**AST:** add `restrictor: PathRestrictor` (enum, default `Walk`) to `Pattern`; thread
it into `match_single_pattern`/`expand_chain`/`varlen`. (Additive — default keeps
existing behaviour; but note `*` default today is edge-unique = TRAIL, so make the
*absence* of a restrictor preserve today's semantics, and only `WALK` relaxes.)
**Executor:** parameterise `varlen`'s uniqueness by the restrictor; add the node
visited-set for ACYCLIC/SIMPLE.
**Tests:** WALK vs TRAIL vs ACYCLIC vs SIMPLE cardinality on a graph with a cycle
(extend the fixture or build a small inline cycle); parity that a bare `*` still
equals today's edge-unique result; rejection of restrictor-over-quantified-group.
**Done when:** the green-state gate passes; the four restrictors are distinguished
by a cycle test; existing var-length tests unchanged.

### PR 3 — Shortest-path selectors `ANY SHORTEST` / `ALL SHORTEST` / `SHORTEST k`
**Goal:** GQL's built-in shortest-path selection on a MATCH pattern.
**Grammar:** a `path_selector` prefix on a `match_pattern` (`MATCH ANY SHORTEST
(a)-[:R*]->(b)`, `SHORTEST 3 …`); integer for `SHORTEST k`.
**AST:** `selector: Option<PathSelector>` on `Pattern` (`AnyShortest` / `AllShortest`
/ `ShortestK(u32)`).
**Executor:** generalise `eval_shortest_path` (`exec.rs:3127`) — today single-rel and
reached only from the `shortestPath()` function. Drive it from a selected MATCH
pattern; yield first path (`ANY SHORTEST`), all min-length paths (`ALL SHORTEST`), or
first k by length (`SHORTEST k`). Lift the single-relationship restriction
(`exec.rs:~3131`) for the selector path; keep `shortestPath()` delegating to the same
core so there is one BFS.
**Tests:** parity vs existing `shortestPath()` (`phase7_shortest_path`); `ALL
SHORTEST` returns all ties; `SHORTEST 2` returns two; interaction with WHERE.
**Done when:** green gate; selectors match `shortestPath()` where they overlap; the
single BFS core is shared (no duplicate traversal logic).

### PR 4 — Label boolean expressions `&` `|` `!`
**Goal:** GQL label predicates beyond Cypher's `:A:B` (AND) and `:T1|T2` (rel alt).
**This is the one PR with AST churn** — do it after 1–3 so pattern AST has settled.
**Grammar:** replace `labels`/`rel_types` with a `label_expr` grammar (precedence
`!` > `&` > `|`, parens); keep bare `:A:B` and `:T1|T2` as sugar that lowers into it.
**AST:** `NodePat.labels: Vec<String>` → `label_expr: Option<LabelExpr>` (`Atom/And/
Or/Not`); same for relationship types (or keep `types` for the common alternation +
add a `type_expr` for the general case to limit blast radius). **Touches every
pattern construction + every `.labels` read** — grep first; expect to update
`node_ok`, `collect_pattern_vars`-adjacent code, planner label hints in `plan.rs`
(`choose_node_scan` reads a single label for LabelScan — keep a fast path when the
expr is a single positive atom), and PR 1's `repeat_inner` (copies `node.labels`).
**Executor:** a `LabelExpr` evaluator over a node's label set in `node_ok`; the
rel-type version in `expand_one_hop` (`exec.rs:2534`).
**Tests:** `:A&B`, `:A|B`, `:!A`, nested/parens, rel-type exprs; planner still picks
LabelScan for a single positive atom (a perf-parity assertion).
**Done when:** green gate; the boolean forms work on nodes and rel-types; no
planner regression for the common single-label case.

### PR 5 — `FOR`, dialect prefix, GQLSTATUS, value gap-fill, docs
Small, independent wins (can be split):
- **`FOR x IN list`** → add `kw_for` + `for_clause`, lower to the existing
  `UnwindClause` (no executor change).
- **Optional dialect prefix** `GQL` / `CYPHER` at parse entry / `handle_request`
  (`server.rs`): strip + record, no-op routing today (one parser serves both).
  Mirrors Neo4j's `CYPHER 5`/`CYPHER 25`.
- **GQLSTATUS** (optional, additive): GQL-compliant status objects in Bolt
  `SUCCESS`/`FAILURE` metadata (`server.rs` response path, `bolt/message.rs`).
- **Typed-value / `CAST` gap-fill** against `Val`/`temporal.rs`.
- **Docs:** supported GQL subset + Cypher↔GQL mapping in README/docs.

---

## 3. Safe clear / resume points

**Clear context only at a PR boundary** — i.e. when that PR's branch is committed,
the green-state gate passes, and `docs/GQL-PROGRESS.md`'s NEXT ACTION + per-PR log
are updated. Mid-PR the ledger may not reflect reality; finish to a green,
documented state first. (Same discipline as `docs/RESUME.md`.)

Recommended boundaries:
1. **After PR 1** (now) → clear → resume at PR 2.
2. **After PR 2** → clear → resume at PR 3.
3. **After PR 3** → clear → resume at PR 4. *(PR 4 has AST churn — start it fresh.)*
4. **After PR 4** → clear → resume at PR 5.

Within PR 4, if context fills mid-way, an additional safe sub-boundary is *after the
AST change compiles and all existing tests pass* (before adding new label-expr
behaviour) — commit that as a WIP-green checkpoint and note it in PROGRESS.

---

## 4. Resume prompts (paste one into a fresh session)

Each prompt is self-contained. Always start the body with the working dir and the
two docs; the green-state gate runs first so you never build on a red tree.

### Resume → PR 2 (path restrictors)
```
Resume the Slater GQL track. Working dir: /home/rickk/git/hs/slater

Read docs/GQL-PLAN.md (sections 1, "PR 2", 3) and docs/GQL-PROGRESS.md (NEXT ACTION)
first. PR 1 (quantified path patterns) is merged-green on branch gql-quantified-paths.

1. Green-state gate (cargo is NOT on PATH):
   export PATH="$HOME/.cargo/bin:$PATH"
   cargo build -p slater && cargo test -p slater && cargo clippy -p slater --all-targets -- -D warnings && cargo fmt -p slater -- --check
2. git checkout -b gql-path-restrictors
3. Implement PR 2 per the plan. Heed the central design decision: scope restrictors
   to variable-length -[*]- patterns (varlen already owns the edge-unique `used`
   set); WALK relaxes it, TRAIL is today's default, ACYCLIC/SIMPLE add node
   uniqueness; REJECT a restrictor over a quantified group for now (note it in
   docs/DECISIONS.md). Keep tests green at every step.
4. Add tests distinguishing WALK/TRAIL/ACYCLIC/SIMPLE on a cycle, plus parity that a
   bare `*` is unchanged.
5. Update docs/GQL-PROGRESS.md (PR 2 status, test names, NEXT ACTION → PR 3) and
   docs/DECISIONS.md, then re-run the green-state gate.
```

### Resume → PR 3 (shortest-path selectors)
```
Resume the Slater GQL track. Working dir: /home/rickk/git/hs/slater

Read docs/GQL-PLAN.md (sections 1, "PR 3", 3) and docs/GQL-PROGRESS.md (NEXT ACTION).
PRs 1–2 are merged-green.

1. export PATH="$HOME/.cargo/bin:$PATH"; run the green-state gate (build/test/clippy/fmt -p slater).
2. git checkout -b gql-shortest-selectors
3. Implement ANY SHORTEST / ALL SHORTEST / SHORTEST k by generalising
   eval_shortest_path (exec.rs ~3127) and driving it from a selected MATCH pattern;
   keep shortestPath() delegating to the same BFS core.
4. Tests: parity with shortestPath(); ALL SHORTEST ties; SHORTEST k.
5. Update docs/GQL-PROGRESS.md (NEXT ACTION → PR 4) + DECISIONS.md; re-run the gate.
```

### Resume → PR 4 (label boolean expressions — AST churn)
```
Resume the Slater GQL track. Working dir: /home/rickk/git/hs/slater

Read docs/GQL-PLAN.md (sections 1, "PR 4", 3) and docs/GQL-PROGRESS.md (NEXT ACTION).
PRs 1–3 are merged-green. THIS PR changes the AST (NodePat labels → label_expr);
grep every `.labels` reader first and expect broad edits.

1. export PATH="$HOME/.cargo/bin:$PATH"; run the green-state gate (-p slater).
2. git checkout -b gql-label-expressions
3. Add the label_expr grammar (! > & > |, parens); change NodePat to label_expr and
   update all construction/read sites incl. node_ok, plan.rs choose_node_scan (keep a
   single-positive-atom LabelScan fast path), and exec.rs repeat_inner. Commit a
   WIP-green checkpoint once the AST change compiles and all existing tests pass,
   BEFORE adding new behaviour.
4. Tests: :A&B, :A|B, :!A, nested, rel-type exprs; planner perf-parity for single label.
5. Update docs/GQL-PROGRESS.md (NEXT ACTION → PR 5) + DECISIONS.md; re-run the gate.
```

### Resume → PR 5 (FOR, dialect prefix, GQLSTATUS, gap-fill, docs)
```
Resume the Slater GQL track. Working dir: /home/rickk/git/hs/slater

Read docs/GQL-PLAN.md (sections 1, "PR 5", 3) and docs/GQL-PROGRESS.md (NEXT ACTION).
PRs 1–4 are merged-green. PR 5 is several small independent items — they can be
separate commits/branches.

1. export PATH="$HOME/.cargo/bin:$PATH"; run the green-state gate (-p slater).
2. git checkout -b gql-finish
3. Implement: FOR→UNWIND lowering; optional GQL/CYPHER query prefix (no-op router);
   GQLSTATUS metadata (additive); CAST/value gap-fill; docs (supported subset +
   Cypher↔GQL map). Keep tests green per item.
4. Update docs/GQL-PROGRESS.md (mark the track COMPLETE) + DECISIONS.md; re-run the gate.
```

---

## 5. Sources (for the "no new sockets" claim, if challenged)
- neo4j.com/blog/cypher-and-gql/cypher-path-gql/ — GQL↔Cypher convergence; INSERT/CREATE, FOR/UNWIND.
- neo4j.com/docs/bolt/current/bolt/message/ — `RUN` = {query, parameters, extra}; query opaque, no language field.
- neo4j.com/docs/cypher-manual/current/queries/select-version/ — dialect via query-string prefix.
