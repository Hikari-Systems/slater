// SPDX-License-Identifier: Apache-2.0
//! `fulltext` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── CALL db.idx.fulltext.queryNodes ──────────────────────────────────────────
//
// The fixture indexes `(:Person)` over `(name, city)`: Alice/London, Bob/London,
// Carol/Paris. `Acme`/`Globex` are `:Company` and are not documents, which is what makes
// a docid provably not a node id here.

/// Run `q` against the full-text fixture.
fn run_ft(root_tag: &str, q: &str) -> (std::path::PathBuf, QueryResult) {
    let (root, graph, _) = testgen::write_basic_with_fulltext(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast).unwrap();
    (root, res)
}

/// A relationship hit must not cost a scan of the whole corpus to fetch its document.
///
/// The core arm reads the document record to get the hit's entity id — an O(1) read that
/// already yields the endpoints too — and then discarded everything but the id, so
/// `fulltext_doc_entry` re-found the very record it had just held, by scanning `.docs.blk`
/// from the top. That is O(hits x corpus) on every relationship query.
///
/// Asserted as reads-per-hit rather than as a timing, so it cannot go green on a fast
/// machine. The fixture's corpus is deliberately larger than the result set: querying the
/// term only `beta` carries returns one edge out of three.
#[test]
fn a_relationship_hit_does_not_scan_the_corpus_for_its_document() {
    let (root, graph, _) = testgen::write_basic_with_fulltext("exec_ft_rel_docscan");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.fulltext.queryRelationships('KNOWS', ' (beta)') \
         YIELD relationship AS r, score RETURN r, score",
    )
    .unwrap();

    crate::exec::fulltext::DOC_READ_COUNT.with(|c| c.set(0));
    let res = engine.run(&ast).unwrap();
    let reads = crate::exec::fulltext::DOC_READ_COUNT.with(|c| c.get());

    assert_eq!(res.rows.len(), 1, "expected the one `beta` edge");
    // One hit. The core arm reads its record once; nothing else should need a read.
    assert!(
        reads <= 2,
        "a single relationship hit read {reads} document records; the record is already \
         held when the hit is scored, so this should not grow with the corpus"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fulltext_query_nodes_binds_the_matching_nodes() {
    let (root, res) = run_ft(
        "exec_ft_basic",
        "CALL db.idx.fulltext.queryNodes('Person', ' (London)') \
             YIELD node, score RETURN node.name AS name ORDER BY name",
    );
    assert_eq!(col0(&res), ["Alice", "Bob"], "both Londoners match");
    let _ = std::fs::remove_dir_all(&root);
}

/// The bound value is a real node, so ordinary property access and traversal work off it
/// — that is what makes the procedure composable rather than a special case.
#[test]
fn a_fulltext_hit_is_an_ordinary_node() {
    let (root, res) = run_ft(
        "exec_ft_compose",
        "CALL db.idx.fulltext.queryNodes('Person', ' (Alice)') YIELD node AS n \
             MATCH (n)-[:KNOWS]->(m) RETURN m.name AS name ORDER BY name",
    );
    // Alice KNOWS both Bob (e0) and Carol (e4) in the fixture.
    assert_eq!(
        col0(&res),
        ["Bob", "Carol"],
        "a hit must traverse like any other node"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// `score` is BM25 and **descends** — the opposite of the vector procedure's ascending
/// distance. Graphiti's `ORDER BY score DESC` depends on it.
#[test]
fn fulltext_score_is_positive_and_orders_descending() {
    let (root, res) = run_ft(
        "exec_ft_score",
        "CALL db.idx.fulltext.queryNodes('Person', ' (Alice | London)') \
             YIELD node, score RETURN node.name AS name, score ORDER BY score DESC",
    );
    // Alice matches both terms, Bob only `london`, so Alice must come first.
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    let scores: Vec<f64> = res
        .rows
        .iter()
        .map(|r| match r[1] {
            Val::Float(f) => f,
            ref other => panic!("score should be a float, got {other:?}"),
        })
        .collect();
    assert!(scores.iter().all(|s| *s > 0.0), "{scores:?}");
    assert!(scores[0] > scores[1], "descending: {scores:?}");
    let _ = std::fs::remove_dir_all(&root);
}

/// A field filter resolves against the index's *declared property order* — `city` is
/// field 1 — and restricts without contributing to the score.
#[test]
fn a_field_filter_restricts_the_hits() {
    let (root, res) = run_ft(
        "exec_ft_filter",
        "CALL db.idx.fulltext.queryNodes('Person', '(@city:\"Paris\") (Alice | Carol)') \
             YIELD node RETURN node.name AS name ORDER BY name",
    );
    assert_eq!(
        col0(&res),
        ["Carol"],
        "Alice matches the term but not the filter"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// `YIELD … WHERE` filters the yielded rows, as it does for the vector procedure.
#[test]
fn fulltext_yield_where_filters_rows() {
    let (root, res) = run_ft(
        "exec_ft_where",
        "CALL db.idx.fulltext.queryNodes('Person', ' (London)') \
             YIELD node, score WHERE node.name = 'Bob' RETURN node.name AS name",
    );
    assert_eq!(col0(&res), ["Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// An absent term is an ordinary empty answer; a query naming an index the graph does not
/// declare is an **error**, because answering "no results" would hide the misconfiguration.
#[test]
fn absent_terms_are_empty_but_an_undeclared_index_is_an_error() {
    let (root, res) = run_ft(
        "exec_ft_absent",
        "CALL db.idx.fulltext.queryNodes('Person', ' (nobodywrotethis)') \
             YIELD node RETURN node.name AS name",
    );
    assert!(res.rows.is_empty());

    let (root2, graph, _) = testgen::write_basic_with_fulltext("exec_ft_undeclared");
    let gen = Generation::open(&root2, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.fulltext.queryNodes('Company', ' (Acme)') YIELD node RETURN node",
    )
    .unwrap();
    let err = engine
        .run(&ast)
        .expect_err("Company declares no full-text index");
    assert!(format!("{err:#}").contains("no full-text index"), "{err:#}");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

/// A docid is a rank among *documents*, not a node id: only the three `Person` nodes are
/// indexed, so a hit must resolve through `.docs.blk` rather than be used directly. Carol
/// is document 2 and node 2, but Paris matching only her would pass either way — so this
/// asserts the identity that would break if docids were used as node ids.
#[test]
fn a_docid_is_resolved_to_its_node_id() {
    let (root, res) = run_ft(
        "exec_ft_docid",
        "CALL db.idx.fulltext.queryNodes('Person', ' (Paris)') \
             YIELD node RETURN id(node) AS id, node.name AS name",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1].to_display(), "Carol");
    assert_eq!(
        res.rows[0][0].to_display(),
        "2",
        "the hit must carry Carol's node id"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Unsupported query syntax is refused, naming what is accepted — never mistaken for a
/// query that matched nothing.
#[test]
fn unsupported_fulltext_syntax_is_refused_at_execution() {
    let (root, graph, _) = testgen::write_basic_with_fulltext("exec_ft_bad_syntax");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast =
        parser::parse("CALL db.idx.fulltext.queryNodes('Person', 'Alice*') YIELD node RETURN node")
            .unwrap();
    let err = engine
        .run(&ast)
        .expect_err("bare/wildcard syntax must be refused");
    let msg = format!("{err:#}");
    assert!(msg.contains("phrases, wildcards"), "{msg}");
    assert!(
        msg.contains("queryNodes"),
        "the error should name the call: {msg}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── full text over the write delta ───────────────────────────────────────────
//
// The built index covers the core generation only; everything written since is served
// by the overlay scan. These pin the three things that can go wrong: a fresh write
// being invisible, an edited document being returned from its *stale* core text, and a
// deleted one still matching.

use crate::read_view::MergedView as FtMergedView;
use slater_delta::{DeltaSnapshot as FtDeltaSnapshot, Memtable as FtMemtable};

/// Open the full-text fixture, let `build` populate a memtable against it, and run `q`.
///
/// The memtable is seeded with the generation's counts: a bare `Memtable` numbers born
/// ids from 0, which would collide with the core's dense ids.
fn run_ft_delta(
    root_tag: &str,
    build: impl FnOnce(&mut FtMemtable),
    q: &str,
) -> (std::path::PathBuf, QueryResult) {
    let (root, graph, _) = testgen::write_basic_with_fulltext(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut mem = FtMemtable::with_bases(gen.node_count(), gen.edge_count());
    build(&mut mem);
    let view = FtMergedView::new(&gen, FtDeltaSnapshot::from_memtable(Arc::new(mem)));
    let engine = Engine::new(&view, &cache);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast).unwrap();
    (root, res)
}

const FT_LONDON: &str = "CALL db.idx.fulltext.queryNodes('Person', ' (London)') \
     YIELD node RETURN node.name AS name ORDER BY name";

/// A node written through the delta since the build is searchable immediately — the
/// gap this arm exists to close. Without it a memory added and searched in the same
/// session would simply not be found.
#[test]
fn a_delta_born_node_is_searchable_before_consolidation() {
    let (root, res) = run_ft_delta(
        "exec_ft_born",
        |mem| {
            mem.upsert_node(
                "Person",
                "name",
                Value::Str("Dave".into()),
                None, // no core id: born in the delta
                [("city".to_string(), Value::Str("London".into()))],
            );
        },
        FT_LONDON,
    );
    assert_eq!(
        col0(&res),
        ["Alice", "Bob", "Dave"],
        "the delta-born Londoner must be found alongside the core ones"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Editing a document must re-score it from its **current** text, not return the core
/// index's stale copy. Carol moves Paris → London: she must now match, and the core
/// arm must not also return her under her old text.
#[test]
fn an_edited_node_is_scored_from_its_current_text() {
    let (root, res) = run_ft_delta(
        "exec_ft_edit",
        |mem| {
            mem.upsert_node(
                "Person",
                "name",
                Value::Str("Carol".into()),
                Some(2),
                [("city".to_string(), Value::Str("London".into()))],
            );
        },
        FT_LONDON,
    );
    assert_eq!(col0(&res), ["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// …and the other direction: a document edited *away* from a term must stop matching,
/// which is the case a core arm without suppression gets wrong. Alice moves London →
/// Paris, so only Bob is left.
#[test]
fn an_edit_that_removes_a_term_stops_matching() {
    let (root, res) = run_ft_delta(
        "exec_ft_unedit",
        |mem| {
            mem.upsert_node(
                "Person",
                "name",
                Value::Str("Alice".into()),
                Some(0),
                [("city".to_string(), Value::Str("Paris".into()))],
            );
        },
        FT_LONDON,
    );
    assert_eq!(
        col0(&res),
        ["Bob"],
        "Alice's stale core posting must be suppressed, not returned"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A deleted node must stop matching even though its core posting is still on disk —
/// the index is immutable, so the only place a delete can take effect is the read.
#[test]
fn a_deleted_node_stops_matching() {
    let (root, res) = run_ft_delta(
        "exec_ft_delete",
        |mem| {
            mem.delete_node("Person", "name", Value::Str("Bob".into()), Some(1));
        },
        FT_LONDON,
    );
    assert_eq!(col0(&res), ["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// A field filter is evaluated against the overlay document's own fields, so a
/// delta-born node is filtered on the same terms a core one is.
#[test]
fn a_field_filter_applies_to_overlay_documents() {
    let (root, res) = run_ft_delta(
        "exec_ft_overlay_filter",
        |mem| {
            mem.upsert_node(
                "Person",
                "name",
                Value::Str("Dave".into()),
                None,
                [("city".to_string(), Value::Str("Paris".into()))],
            );
        },
        "CALL db.idx.fulltext.queryNodes('Person', '(@city:\"Paris\") (Dave | Carol)') \
             YIELD node RETURN node.name AS name ORDER BY name",
    );
    assert_eq!(
        col0(&res),
        ["Carol", "Dave"],
        "both Parisians match; the filter resolves for core and overlay alike"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// An empty delta must leave the core arm's answer exactly as it was — the overlay
/// machinery costs nothing and changes nothing on a read-only estate.
#[test]
fn an_empty_delta_does_not_disturb_the_core_answer() {
    let (root, plain) = run_ft("exec_ft_nodelta_a", FT_LONDON);
    let (root2, empty) = run_ft_delta("exec_ft_nodelta_b", |_| {}, FT_LONDON);
    assert_eq!(col0(&plain), col0(&empty));
    assert_eq!(col0(&plain), ["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

// ── a deleted relationship must stop matching ────────────────────────────────
//
// A relationship index is served from the core generation alone. That costs recall on
// *new* facts, which is a stated limit — but it must never cost correctness on deleted
// ones. An index that keeps returning a fact the graph no longer holds is worse than one
// that is merely behind.

/// All three KNOWS edges share the term `shared`, so this addresses the whole corpus and
/// a suppression shows up as a missing id rather than as an empty result.
const FT_KNOWS_SHARED: &str = "CALL db.idx.fulltext.queryRelationships('KNOWS', ' (shared)') \
     YIELD relationship AS r RETURN id(r) AS eid ORDER BY eid";

fn ft_edge_ids(res: &QueryResult) -> Vec<i64> {
    res.rows
        .iter()
        .map(|r| match &r[0] {
            Val::Int(i) => *i,
            other => panic!("expected an edge id, got {other:?}"),
        })
        .collect()
}

/// Deleting a relationship through the writable layer must remove it from full text
/// immediately, without waiting for a consolidation to rebuild the index.
#[test]
fn a_deleted_relationship_stops_matching_before_consolidation() {
    // Control: with an empty delta the fixture's three KNOWS edges all match.
    let (root0, all) = run_ft_delta("exec_ft_reldel_ctl", |_| {}, FT_KNOWS_SHARED);
    assert_eq!(
        ft_edge_ids(&all),
        [0, 1, 4],
        "control: every KNOWS edge is a document"
    );
    let _ = std::fs::remove_dir_all(&root0);

    // e0 is Alice -KNOWS-> Bob. Delete it, and it must stop being returned — while its
    // two siblings, which the same query matches, must not.
    let (root, res) = run_ft_delta(
        "exec_ft_reldel",
        |mem| {
            mem.delete_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Bob".into()),
                Some(0),
                Some(1),
            );
        },
        FT_KNOWS_SHARED,
    );
    assert_eq!(
        ft_edge_ids(&res),
        [1, 4],
        "the deleted edge must be gone and its siblings must survive"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A relationship whose *endpoint* was deleted is gone too — the edge is unreachable, so
/// returning it from full text would hand the caller a relationship to a node that no
/// longer exists.
#[test]
fn a_relationship_to_a_deleted_node_stops_matching() {
    let (root, res) = run_ft_delta(
        "exec_ft_reldel_node",
        |mem| {
            // Bob is node 1: the destination of e0 and the source of e1.
            mem.delete_node("Person", "name", Value::Str("Bob".into()), Some(1));
        },
        FT_KNOWS_SHARED,
    );
    assert_eq!(
        ft_edge_ids(&res),
        [4],
        "both edges incident to Bob are gone; Alice -KNOWS-> Carol survives"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── the relationship overlay arm ─────────────────────────────────────────────
//
// The built index covers the core generation; edges written or edited since are served
// by the same overlay scan the node arm uses. These pin the three things that can go
// wrong, and they are the edge twins of the node tests above.

/// A relationship written through the delta is searchable immediately. Before the edge
/// overlay existed this returned only the three core edges, so a fact added and searched
/// in one session was simply not found.
#[test]
fn a_delta_born_relationship_is_searchable_before_consolidation() {
    let (root, res) = run_ft_delta(
        "exec_ft_rel_born",
        |mem| {
            // Carol -KNOWS-> Alice: a pair the core has no KNOWS edge for, so this is
            // born rather than a patch. It takes the first synthetic edge id, 5.
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Carol".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Alice".into()),
                Some(2),
                Some(0),
                [("fact".to_string(), Value::Str("delta shared".into()))],
            );
        },
        FT_KNOWS_SHARED,
    );
    assert_eq!(
        ft_edge_ids(&res),
        [0, 1, 4, 5],
        "the born edge must join the three core ones"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// An edited relationship is scored from its *current* text, never from the stale copy
/// the built index still holds. The core index says edge 0's fact is `alpha shared`; the
/// delta says it is `omega`. Only the delta is true.
#[test]
fn an_edited_relationship_is_scored_from_its_current_text() {
    let edit = |mem: &mut FtMemtable| {
        mem.patch_core_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
            0, // the core edge id
            [("fact".to_string(), Value::Str("omega".into()))],
        );
    };

    // The term it *used* to carry must no longer find it — the core arm has to have
    // suppressed it, not merely been outvoted by the overlay.
    let (root, stale) = run_ft_delta(
        "exec_ft_rel_edit_stale",
        edit,
        "CALL db.idx.fulltext.queryRelationships('KNOWS', ' (alpha)') \
         YIELD relationship AS r RETURN id(r) AS eid ORDER BY eid",
    );
    assert!(
        ft_edge_ids(&stale).is_empty(),
        "the edited edge must not answer to the text it no longer carries"
    );
    let _ = std::fs::remove_dir_all(&root);

    // Its new term must find it, which is what proves the overlay scored it at all.
    let (root2, fresh) = run_ft_delta(
        "exec_ft_rel_edit_fresh",
        edit,
        "CALL db.idx.fulltext.queryRelationships('KNOWS', ' (omega)') \
         YIELD relationship AS r RETURN id(r) AS eid ORDER BY eid",
    );
    assert_eq!(ft_edge_ids(&fresh), [0], "the new text must match");
    let _ = std::fs::remove_dir_all(&root2);
}

/// An edge query's corpus statistics must come from the **edge** index.
///
/// This is the one failure in this area that is silent rather than loud: asking the node
/// index for an edge query's document frequencies returns plausible weights from the
/// wrong corpus, and nothing looks broken — the ranking is just wrong.
///
/// So it is asserted through scores rather than through membership, and the terms are
/// chosen so that the *wrong* lookup is observable. Two born edges carry one term each,
/// of equal length: `shared`, which every core KNOWS document carries (edge df = 3), and
/// `zzq`, which nothing has ever carried (df = 0). A common term must be judged common,
/// so `shared` has to score **strictly lower** than `zzq` — 0.168 against 2.614.
///
/// The first version of this test used `london`, a term the *node* index knows, and
/// asserted the two scored equally. It had no teeth: `fulltext_index(Node, "KNOWS")`
/// finds no index at all, so the wrong lookup also returns `df = 0` and the assertion
/// held either way. Verified by mutation this time — forcing the kind back to `Node`
/// makes both scores 2.614 and fails.
///
/// The first version of this test used `london` — a term the *node* index knows — and
/// asserted the two scored equally. It had no teeth: `fulltext_index(Node, "KNOWS")`
/// finds no index at all, so the wrong lookup returns `df = 0` too and the assertion held
/// either way. Verified by mutation this time: forcing the kind back to `Node` fails
/// this.
#[test]
fn an_edge_query_scores_against_the_edge_index_not_the_node_index() {
    let (root, res) = run_ft_delta(
        "exec_ft_rel_idf",
        |mem| {
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Carol".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Alice".into()),
                Some(2),
                Some(0),
                [("fact".to_string(), Value::Str("shared".into()))],
            );
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Bob".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Alice".into()),
                Some(1),
                Some(0),
                [("fact".to_string(), Value::Str("zzq".into()))],
            );
        },
        "CALL db.idx.fulltext.queryRelationships('KNOWS', ' (shared | zzq)') \
         YIELD relationship AS r, score RETURN id(r) AS eid, score ORDER BY eid",
    );
    // `shared` matches the core documents too, so select the two born edges by id rather
    // than by position: 5 carries `shared`, 6 carries `zzq`.
    let score_of = |eid: i64| -> f64 {
        res.rows
            .iter()
            .find(|r| matches!(&r[0], Val::Int(i) if *i == eid))
            .map(|r| match &r[1] {
                Val::Float(f) => *f,
                other => panic!("expected a score, got {other:?}"),
            })
            .unwrap_or_else(|| panic!("edge {eid} did not match"))
    };
    let (common, unseen) = (score_of(5), score_of(6));
    assert!(
        common < unseen,
        "`shared` is carried by every core document and must be judged common against \
         `zzq`, which nothing carries; equal scores mean the document frequencies came \
         from the wrong corpus: shared={common}, zzq={unseen}"
    );
    let _ = std::fs::remove_dir_all(&root);
}
