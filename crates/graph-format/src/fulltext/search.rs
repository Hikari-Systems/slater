// SPDX-License-Identifier: Apache-2.0
//! Running a query against one full-text index: match, score, rank.
//!
//! # The query shape, and why it is this small
//!
//! Graphiti builds its query with `build_falkor_fulltext_query`, which emits exactly one
//! form — `(@group_id:"g1"|"g2") (term1 | term2)` — and runs
//! `sanitize_falkor_fulltext_query` over the user's text first, replacing every character
//! that could spell a phrase, wildcard, negation or range with whitespace. So those
//! constructs cannot reach Slater, and implementing them would be implementing something
//! nothing can ask for. [`FulltextQuery`] is the whole surface.
//!
//! The two halves mean different things, and conflating them would be a ranking bug:
//!
//! - **Filters** (`@field:"value"`) are *required* and score **nothing**. A group id is a
//!   tenancy boundary, not evidence of relevance; letting it contribute would rank every
//!   document in a small group above every document in a large one.
//! - **Terms** are a *disjunction* and are what scores. A document matching two of them
//!   outranks one matching either alone, which is what [`bm25::idf`]'s non-negativity
//!   guarantees.
//!
//! # Memory shape
//!
//! Candidates are accumulated into a map keyed by docid, so the working set is the number
//! of documents matching **any** query term — not the corpus, and not the result limit.
//! For a disjunction of ordinary words that is the honest cost, and it is what block-max
//! WAND would later cut by skipping chunks that cannot beat the running k-th score. The
//! `max_impact` bytes for that are already on disk (see [`super::index`]); nothing reads
//! them yet.
//!
//! Filters are applied *after* accumulation, by streaming each filter group's postings
//! once and keeping the candidates it names. Building the filter's document set up front
//! instead would be O(matching documents) memory for a predicate like `@group_id` that
//! commonly matches nearly everything — the one shape where materialising it is worst.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::bm25::{self, Bm25Params};
use super::index::FulltextReader;

/// A parsed full-text query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FulltextQuery {
    /// Required, unscored. Outer vector is **AND** across groups, inner is **OR** of
    /// `(field, term)` alternatives within a group — the shape `(@group_id:"a"|"b")` has.
    pub filters: Vec<Vec<(u32, String)>>,
    /// The scored disjunction. Empty means nothing scores, and the query matches nothing:
    /// graphiti already returns `''` (and skips the call) when every word was a stopword,
    /// so an empty term list arriving here is a caller that should have short-circuited.
    pub terms: Vec<String>,
}

/// One result. `score` is BM25 and orders **descending** — see the D-FT1 note in
/// [`super::bm25`], which is the opposite convention to the vector procedure's distance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hit {
    pub doc: u64,
    pub score: f64,
}

/// Match, score and rank `query` against `reader`.
///
/// `avg_doc_len` comes from the index descriptor rather than the reader so the delta and
/// segment arms can pass a *reconciled* average — every arm must score against the same
/// corpus statistics or a document's rank would depend on which arm found it.
///
/// Returns at most `limit` hits, ordered by descending score and then ascending docid.
/// The docid tie-break is not cosmetic: without it, two documents with identical scores
/// would come back in `HashMap` iteration order, and a repeated query could return
/// different results.
pub fn search(
    reader: &FulltextReader,
    query: &FulltextQuery,
    avg_doc_len: f32,
    params: Bm25Params,
    limit: usize,
) -> Result<Vec<Hit>> {
    if query.terms.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let n_docs = reader.doc_count();
    if n_docs == 0 {
        return Ok(Vec::new());
    }

    let mut acc: HashMap<u64, f64> = HashMap::new();
    // Document lengths are read once each and reused across terms — a document matching
    // five query terms would otherwise cost five identical `.docs.blk` reads.
    let mut lens: HashMap<u64, u32> = HashMap::new();

    for term in &query.terms {
        let metas = reader.term_metas(term)?;
        let Some(first) = metas.first() else {
            continue; // an absent term contributes nothing; not an error
        };
        // Whole-document df, identical across the term's field records by construction.
        let idf = bm25::idf(n_docs, first.doc_df);

        // Sum the term's frequency across every field of a document before scoring:
        // scoring is whole-document, so a name-and-summary hit is one term occurring
        // twice, not two separate contributions.
        let mut tfs: HashMap<u64, u32> = HashMap::new();
        for m in &metas {
            for c in 0..m.chunk_count {
                let chunk = reader.chunk(m, c)?;
                for (d, tf) in chunk.docs.iter().zip(&chunk.tfs) {
                    *tfs.entry(*d).or_insert(0) += *tf;
                }
            }
        }
        for (doc, tf) in tfs {
            let len = match lens.get(&doc) {
                Some(l) => *l,
                None => {
                    let l = reader.doc(doc)?.len;
                    lens.insert(doc, l);
                    l
                }
            };
            *acc.entry(doc).or_insert(0.0) += bm25::term_score(idf, tf, len, avg_doc_len, params);
        }
    }

    // Required filters, applied by streaming rather than materialising.
    for group in &query.filters {
        if acc.is_empty() {
            break;
        }
        let mut keep: HashSet<u64> = HashSet::new();
        for (field, term) in group {
            for m in reader.term_metas(term)? {
                if m.field != *field {
                    continue;
                }
                for c in 0..m.chunk_count {
                    for d in reader.chunk(&m, c)?.docs {
                        if acc.contains_key(&d) {
                            keep.insert(d);
                        }
                    }
                }
            }
        }
        acc.retain(|d, _| keep.contains(d));
    }

    let mut hits: Vec<Hit> = acc
        .into_iter()
        .map(|(doc, score)| Hit { doc, score })
        .collect();
    // Descending score, ascending docid. `total_cmp` rather than `partial_cmp` because a
    // sort comparator must be a total order; the scorer keeps scores finite, and this
    // means a NaN that slipped through would still sort rather than panic.
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
    hits.truncate(limit);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::index::{write_fulltext_index, DocEntry, FulltextReader, Posting};
    use crate::fulltext::Analyzer;
    use std::path::{Path, PathBuf};

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "slater-ftsearch-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn no_cipher(_: &str) -> Option<std::sync::Arc<crate::crypto::FileCipher>> {
        None
    }

    /// Field 0 is `name`, field 1 is `summary`, field 2 is `group_id` — graphiti's
    /// `Entity` index, in order.
    fn index(dir: &Path, docs: &[(&str, &str, &str)]) -> (FulltextReader, f32) {
        let a = Analyzer::new(&["a".to_string(), "the".to_string(), "is".to_string()]);
        let mut entries = Vec::new();
        let mut postings = Vec::new();
        for (docid, (name, summary, group)) in docs.iter().enumerate() {
            let mut len = 0u32;
            for (field, text) in [name, summary, group].iter().enumerate() {
                let terms = a.terms(text);
                len += terms.len() as u32;
                let mut counts: std::collections::BTreeMap<String, u32> = Default::default();
                for t in terms {
                    *counts.entry(t).or_default() += 1;
                }
                for (term, tf) in counts {
                    postings.push(Posting {
                        term,
                        field: field as u32,
                        doc: docid as u64,
                        tf,
                    });
                }
            }
            entries.push(DocEntry {
                entity: docid as u64 * 10,
                len,
                endpoints: None,
            });
        }
        postings.sort_by(|x, y| {
            (x.term.as_str(), x.field, x.doc).cmp(&(y.term.as_str(), y.field, y.doc))
        });
        let stats = write_fulltext_index(
            dir,
            "node_Entity",
            entries.into_iter().map(Ok),
            postings.into_iter().map(Ok),
            4096,
            3,
            &no_cipher,
        )
        .unwrap();
        (
            FulltextReader::open(dir, "node_Entity", false, &no_cipher).unwrap(),
            stats.avg_doc_len,
        )
    }

    fn q(terms: &[&str]) -> FulltextQuery {
        FulltextQuery {
            filters: Vec::new(),
            terms: terms.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn ranks_by_relevance_and_orders_descending() {
        let dir = tmp("rank");
        let (r, avg) = index(
            &dir,
            &[
                ("Alice", "Alice studies databases and Alice writes", "g1"),
                ("Bob", "Bob mentions Alice once", "g1"),
                ("Carol", "unrelated text entirely", "g1"),
            ],
        );
        let hits = search(&r, &q(&["alice"]), avg, Bm25Params::default(), 10).unwrap();
        assert_eq!(hits.len(), 2, "Carol does not mention Alice");
        assert_eq!(hits[0].doc, 0, "three occurrences beats one");
        assert_eq!(hits[1].doc, 1);
        assert!(
            hits[0].score > hits[1].score,
            "scores must order descending: {hits:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A document matching two of the disjunction's terms beats one matching either
    /// alone — the property `idf`'s non-negativity exists to guarantee.
    #[test]
    fn matching_more_terms_scores_higher() {
        let dir = tmp("more-terms");
        let (r, avg) = index(
            &dir,
            &[
                ("one", "alpha", "g1"),
                ("two", "alpha beta", "g1"),
                ("three", "beta", "g1"),
            ],
        );
        let hits = search(&r, &q(&["alpha", "beta"]), avg, Bm25Params::default(), 10).unwrap();
        assert_eq!(hits[0].doc, 1, "the only document with both: {hits:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The filter half is required and contributes nothing to the score. Both halves of
    /// that matter: `g2` documents are excluded, and the surviving order is the order the
    /// unfiltered query would have given.
    #[test]
    fn a_field_filter_restricts_without_scoring() {
        let dir = tmp("filter");
        let (r, avg) = index(
            &dir,
            &[
                ("Alice", "alpha alpha alpha", "g1"),
                ("Bob", "alpha", "g1"),
                ("Mallory", "alpha alpha alpha alpha", "g2"),
            ],
        );
        let unfiltered = search(&r, &q(&["alpha"]), avg, Bm25Params::default(), 10).unwrap();
        assert_eq!(unfiltered.len(), 3);
        assert_eq!(unfiltered[0].doc, 2, "the g2 document scores highest");

        let filtered = search(
            &r,
            &FulltextQuery {
                filters: vec![vec![(2, "g1".to_string())]],
                terms: vec!["alpha".to_string()],
            },
            avg,
            Bm25Params::default(),
            10,
        )
        .unwrap();
        assert_eq!(
            filtered.iter().map(|h| h.doc).collect::<Vec<_>>(),
            [0, 1],
            "g2 is excluded and the rest keep their relative order"
        );
        // The filter term must not have added weight: a surviving document scores
        // exactly what it scored without the filter.
        for h in &filtered {
            let same = unfiltered.iter().find(|u| u.doc == h.doc).unwrap();
            assert_eq!(
                h.score, same.score,
                "doc {} scored differently once a filter was added",
                h.doc
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Alternatives *within* a filter group are OR; separate groups are AND.
    #[test]
    fn filter_groups_are_and_alternatives_within_one_are_or() {
        let dir = tmp("and-or");
        let (r, avg) = index(
            &dir,
            &[
                ("Alice", "alpha", "g1"),
                ("Bob", "alpha", "g2"),
                ("Carol", "alpha", "g3"),
            ],
        );
        let or = search(
            &r,
            &FulltextQuery {
                filters: vec![vec![(2, "g1".into()), (2, "g3".into())]],
                terms: vec!["alpha".into()],
            },
            avg,
            Bm25Params::default(),
            10,
        )
        .unwrap();
        assert_eq!(or.iter().map(|h| h.doc).collect::<Vec<_>>(), [0, 2]);

        // Two groups that cannot both hold: an AND of disjoint filters matches nothing.
        let and = search(
            &r,
            &FulltextQuery {
                filters: vec![vec![(2, "g1".into())], vec![(2, "g2".into())]],
                terms: vec!["alpha".into()],
            },
            avg,
            Bm25Params::default(),
            10,
        )
        .unwrap();
        assert!(and.is_empty(), "{and:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Identical scores must come back in a stable, repeatable order, or the same query
    /// answers differently run to run.
    #[test]
    fn ties_break_on_docid_so_results_are_deterministic() {
        let dir = tmp("ties");
        let (r, avg) = index(
            &dir,
            &[
                ("same", "alpha", "g1"),
                ("same", "alpha", "g1"),
                ("same", "alpha", "g1"),
            ],
        );
        let first = search(&r, &q(&["alpha"]), avg, Bm25Params::default(), 10).unwrap();
        assert_eq!(first.iter().map(|h| h.doc).collect::<Vec<_>>(), [0, 1, 2]);
        for _ in 0..16 {
            let again = search(&r, &q(&["alpha"]), avg, Bm25Params::default(), 10).unwrap();
            assert_eq!(
                again.iter().map(|h| h.doc).collect::<Vec<_>>(),
                [0, 1, 2],
                "a repeated query must not reorder ties"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn limit_truncates_the_ranked_list_not_the_candidate_set() {
        let dir = tmp("limit");
        let (r, avg) = index(
            &dir,
            &[
                ("d0", "alpha", "g1"),
                ("d1", "alpha alpha", "g1"),
                ("d2", "alpha alpha alpha", "g1"),
            ],
        );
        let hits = search(&r, &q(&["alpha"]), avg, Bm25Params::default(), 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].doc, 2,
            "the best document survives the limit, not the first found"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Absent terms and empty queries are ordinary answers, not errors — a caller that
    /// searched for a word nobody wrote should get nothing back and no failure.
    #[test]
    fn absent_terms_and_empty_queries_return_nothing() {
        let dir = tmp("absent");
        let (r, avg) = index(&dir, &[("Alice", "alpha", "g1")]);
        let p = Bm25Params::default();
        assert!(search(&r, &q(&["nobodywrotethis"]), avg, p, 10)
            .unwrap()
            .is_empty());
        assert!(search(&r, &q(&[]), avg, p, 10).unwrap().is_empty());
        assert!(search(&r, &q(&["alpha"]), avg, p, 0).unwrap().is_empty());
        // A term that exists plus one that does not still returns the real match.
        assert_eq!(
            search(&r, &q(&["alpha", "nobodywrotethis"]), avg, p, 10)
                .unwrap()
                .len(),
            1
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
