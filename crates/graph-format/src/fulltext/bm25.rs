// SPDX-License-Identifier: Apache-2.0
//! BM25 — the scoring math, as pure functions over plain numbers.
//!
//! Deliberately separate from anything that reads an index. Full-text visibility will
//! eventually have three arms (core postings, a per-segment sidecar, a bounded delta
//! scan), and they have to agree to the last bit: a term whose weight jumped between arms
//! would reorder results depending on how recently a document was written, which is the
//! kind of wrong that looks like a ranking opinion rather than a bug. One shared exact
//! scorer is how `slater::vector::distance` keeps the three KNN arms honest, and it is why
//! this module knows nothing about files.
//
// DESIGN (D-FT1): `score` is **BM25 and orders DESCENDING** — larger is a better match.
// That is the exact opposite of `db.idx.vector.queryNodes`, whose `score` is a *distance*
// ordered ascending (see the D26 note in `slater/src/vector.rs`). The two procedures sit
// next to each other in the same namespace and disagree on the sign convention, so this is
// stated in both places. Graphiti's `ORDER BY score DESC` depends on this direction, and
// FalkorDB's own full-text procedure has the same contract.

/// The two BM25 tuning constants.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    /// Term-frequency saturation. Higher ⇒ repeated occurrences keep adding weight for
    /// longer before flattening out.
    pub k1: f32,
    /// Length normalisation, in `[0, 1]`. `0` disables it entirely; `1` normalises fully
    /// by `len / avgdl`.
    pub b: f32,
}

impl Default for Bm25Params {
    /// The textbook defaults, and what Lucene, RediSearch and FalkorDB all ship. Chosen
    /// for comparability rather than tuned: a search that ranks differently here than on
    /// the engine Slater is standing in for would be a compatibility bug, not an
    /// improvement.
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Inverse document frequency, in the "probabilistic with +1" form Lucene uses:
/// `ln(1 + (N - df + 0.5) / (df + 0.5))`.
///
/// The `1 +` is what keeps it non-negative. The textbook form goes negative once a term
/// appears in more than half the documents, which would let a common term *subtract* from
/// a document's score and rank a document that matched two query terms below one that
/// matched a single rare one. Every modern implementation takes this variant; it matters
/// here because graphiti's queries are disjunctions over ordinary English words, so
/// terms above the half-the-corpus line are routine rather than exotic.
///
/// `df > n_docs` cannot arise from a consistent index, but the arithmetic is saturating
/// anyway — this reads numbers off an untrusted on-disk document, and a forged `df`
/// should produce a poor ranking, not a NaN that propagates into a comparator.
pub fn idf(n_docs: u64, df: u64) -> f64 {
    let n = n_docs as f64;
    let df = (df as f64).min(n);
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}

/// One term's contribution to a document's score.
///
/// `tf` is the term's frequency in the document (summed across fields — scoring is
/// whole-document), `doc_len` its total term count, `avg_doc_len` the index's mean.
///
/// A zero `avg_doc_len` means the index has no documents to average, so length
/// normalisation is skipped rather than dividing by it. That is not a defensive
/// afterthought: it is the state a legally-empty index is in, and an index that has just
/// had its first document written through the delta is momentarily in it too.
pub fn term_score(idf: f64, tf: u32, doc_len: u32, avg_doc_len: f32, params: Bm25Params) -> f64 {
    if tf == 0 {
        return 0.0;
    }
    let tf = tf as f64;
    let k1 = params.k1 as f64;
    let norm = if avg_doc_len > 0.0 {
        let b = params.b as f64;
        1.0 - b + b * (doc_len as f64 / avg_doc_len as f64)
    } else {
        1.0
    };
    idf * (tf * (k1 + 1.0)) / (tf + k1 * norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the `1 +` exists for: a term in most of the corpus still contributes
    /// nothing *negative*, so matching more query terms can never rank a document lower.
    #[test]
    fn idf_is_never_negative_even_for_a_term_in_almost_every_document() {
        for (n, df) in [(100, 99), (100, 100), (1000, 999), (2, 2), (1, 1)] {
            let v = idf(n, df);
            assert!(v >= 0.0, "idf({n}, {df}) = {v} must not be negative");
            assert!(v.is_finite(), "idf({n}, {df}) = {v}");
        }
    }

    #[test]
    fn idf_falls_as_a_term_gets_commoner() {
        let rare = idf(1000, 1);
        let mid = idf(1000, 100);
        let common = idf(1000, 900);
        assert!(rare > mid && mid > common, "{rare} {mid} {common}");
    }

    /// An empty or forged index must not produce a NaN — it would poison the comparator
    /// the top-k heap is ordered by, and `NaN` comparisons are not a total order.
    #[test]
    fn degenerate_inputs_stay_finite() {
        assert!(idf(0, 0).is_finite());
        assert!(idf(10, 99).is_finite(), "df > n_docs is clamped, not NaN");
        let p = Bm25Params::default();
        assert!(term_score(idf(0, 0), 1, 0, 0.0, p).is_finite());
        assert_eq!(
            term_score(1.0, 0, 5, 3.0, p),
            0.0,
            "a term the document does not contain scores nothing"
        );
    }

    /// Term frequency saturates: the second occurrence is worth less than the first, and
    /// the hundredth is worth almost nothing. Without this a keyword-stuffed document
    /// outranks a relevant one.
    #[test]
    fn term_frequency_saturates() {
        let p = Bm25Params::default();
        let s = |tf| term_score(1.0, tf, 10, 10.0, p);
        let (s1, s2, s3, s100) = (s(1), s(2), s(3), s(100));
        assert!(s1 < s2 && s2 < s3, "more occurrences still score higher");
        assert!(
            s2 - s1 > s3 - s2,
            "but with diminishing returns: {s1} {s2} {s3}"
        );
        assert!(
            s100 < 1.0 * (p.k1 as f64 + 1.0),
            "bounded by idf * (k1 + 1)"
        );
    }

    /// A term found in a short document counts for more than the same term buried in a
    /// long one — that is what `b` buys, and it is why a document's length is stored.
    #[test]
    fn shorter_documents_score_higher_for_the_same_term_frequency() {
        let p = Bm25Params::default();
        let short = term_score(1.0, 1, 2, 10.0, p);
        let average = term_score(1.0, 1, 10, 10.0, p);
        let long = term_score(1.0, 1, 100, 10.0, p);
        assert!(
            short > average && average > long,
            "{short} {average} {long}"
        );
    }

    /// `b = 0` turns length normalisation off, so document length stops mattering. Pinned
    /// because it is the knob's whole meaning, and the `avg_doc_len == 0` guard has to
    /// agree with it.
    #[test]
    fn b_zero_ignores_length_and_matches_the_no_average_fallback() {
        let no_norm = Bm25Params { k1: 1.2, b: 0.0 };
        let a = term_score(1.0, 3, 2, 10.0, no_norm);
        let b = term_score(1.0, 3, 500, 10.0, no_norm);
        assert_eq!(a, b, "with b = 0, length is irrelevant");

        // An index with no average behaves as if unnormalised, whatever `b` says.
        assert_eq!(
            term_score(1.0, 3, 7, 0.0, Bm25Params::default()),
            term_score(1.0, 3, 7, 1.0, no_norm),
        );
    }
}
