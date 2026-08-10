// SPDX-License-Identifier: Apache-2.0
//! The full-text **analyzer** — the one tokenisation the builder and the reader must
//! agree on.
//!
//! This module is deliberately tiny and deliberately shared. If the side that *writes*
//! postings and the side that *reads* them disagree about where a token starts, nothing
//! errors: the term is simply never found, and a search quietly returns fewer results
//! than it should. There is no assertion that can catch that downstream, so the only
//! defence is that there is exactly one implementation and both sides call it.
//!
//! # The contract is FalkorDB's, and it is not negotiable
//!
//! Slater serves graphiti over the FalkorDB dialect, and graphiti sanitises a query
//! *before* it reaches us — `sanitize_falkor_fulltext_query`
//! (`graphiti_core/driver/falkordb/fulltext.py`) replaces each character in a fixed
//! 30-character map with whitespace, then splits on whitespace. A term only matches if
//! the indexer split the stored text the same way, so [`SEPARATORS`] is a transcription
//! of that map rather than a tokenizer we chose.
//!
//! Two consequences worth stating, because both look like bugs from the outside:
//!
//! - **`_` is not a separator.** `group_id` is one token, which is what makes graphiti's
//!   `@group_id` field filter work at all.
//! - **Non-ASCII punctuation is not a separator.** A curly apostrophe or an em dash is an
//!   ordinary term character, so `don't` (U+2019) is one token while `don't` (U+0027) is
//!   two. That is FalkorDB's behaviour, and diverging from it would mean a query
//!   graphiti sanitised one way and an index built another.
//!
//! Case is folded at both ends — the index lowercases as it writes, the query lowercases
//! as it reads — because RediSearch matching is case-insensitive and graphiti passes
//! query terms through with their original case (it lowercases only to test a stopword).
//!
//! # What is deliberately absent
//!
//! **No stemming.** It is the first item on this work's cut list;
//! [`FulltextIndexDesc::stemmer`](crate::manifest::FulltextIndexDesc::stemmer) reserves
//! the field so adding one later is a value change, not a format change. Until then a
//! search for `running` does not match `runs`, which is a recall limit, not a defect.

pub mod bm25;
pub mod index;
pub mod search;

/// The characters FalkorDB treats as token separators, verbatim from graphiti-core's
/// `_SEPARATOR_MAP`. ASCII whitespace separates too and is handled by
/// [`char::is_whitespace`], so it is not listed here.
///
/// Sorted so the membership test below is a binary search, and so a diff against the
/// Python map is readable.
pub const SEPARATORS: &[char] = &[
    '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=',
    '>', '?', '@', '[', '\\', ']', '^', '`', '{', '|', '}', '~',
];

/// Whether `c` ends a token. Any Unicode whitespace, or one of [`SEPARATORS`].
///
/// Whitespace is included beyond the transcribed map because graphiti's sanitiser
/// *substitutes* whitespace for each separator and then splits on it — so whitespace is
/// the separator the map is defined in terms of.
#[inline]
pub fn is_separator(c: char) -> bool {
    c.is_whitespace() || SEPARATORS.binary_search(&c).is_ok()
}

/// Splits text into the lowercased terms an index stores and a query looks up.
///
/// The declared stopword list is normalised once at construction — lowercased, sorted,
/// deduped — so the per-token test is a binary search rather than a linear scan of a
/// 33-element list for every token of every property of every entity.
pub struct Analyzer {
    stopwords: Vec<String>,
}

impl Analyzer {
    /// Build an analyzer over an index's declared stopword list.
    pub fn new(stopwords: &[String]) -> Self {
        let mut s: Vec<String> = stopwords.iter().map(|w| w.to_lowercase()).collect();
        s.sort();
        s.dedup();
        Self { stopwords: s }
    }

    /// Whether `lowercased` is a stopword. Takes an already-lowercased term because
    /// every caller has just produced one.
    #[inline]
    pub fn is_stopword(&self, lowercased: &str) -> bool {
        self.stopwords
            .binary_search_by(|w| w.as_str().cmp(lowercased))
            .is_ok()
    }

    /// The surviving terms of `text`, lowercased, in order.
    ///
    /// Allocates one `String` per term. That is the honest cost of case folding
    /// (lowercasing is not always length-preserving in Unicode), and the indexer sorts
    /// the terms externally straight afterwards, so nothing is saved by borrowing.
    pub fn terms(&self, text: &str) -> Vec<String> {
        let mut out = Vec::new();
        for raw in text.split(is_separator) {
            if raw.is_empty() {
                continue;
            }
            let term = raw.to_lowercase();
            if !self.is_stopword(&term) {
                out.push(term);
            }
        }
        out
    }

    /// The number of surviving terms, without materialising them — the document length
    /// BM25 normalises by. Kept separate so a second pass over a large `summary` does not
    /// have to rebuild the token vector.
    pub fn term_count(&self, text: &str) -> usize {
        text.split(is_separator)
            .filter(|raw| !raw.is_empty())
            .filter(|raw| !self.is_stopword(&raw.to_lowercase()))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Graphiti's list, so the fixtures below read as they would in production.
    fn graphiti_stopwords() -> Vec<String> {
        [
            "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
            "into", "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there",
            "these", "they", "this", "to", "was", "will", "with",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn separators_are_sorted_so_the_binary_search_is_valid() {
        let mut sorted = SEPARATORS.to_vec();
        sorted.sort();
        assert_eq!(SEPARATORS, sorted.as_slice());
    }

    /// The transcription against graphiti-core 0.29.3's `_SEPARATOR_MAP`, character for
    /// character. A divergence here does not fail a query — it makes some terms
    /// unfindable — so it is pinned rather than trusted.
    #[test]
    fn separators_match_graphitis_map_exactly() {
        // Transcribed independently from the Python dict, in its own order.
        let python_map = ",.<>{}[]\"':;!@#$%^&*()-+=~?|/\\`";
        let mut want: Vec<char> = python_map.chars().collect();
        want.sort();
        want.dedup();
        assert_eq!(
            SEPARATORS,
            want.as_slice(),
            "SEPARATORS has drifted from graphiti's _SEPARATOR_MAP"
        );
    }

    /// The two behaviours that look like bugs but are the contract.
    #[test]
    fn underscores_and_unicode_punctuation_are_term_characters() {
        let a = Analyzer::new(&[]);
        assert_eq!(
            a.terms("group_id"),
            ["group_id"],
            "`_` must not split, or graphiti's @group_id filter cannot resolve"
        );
        // U+2019 RIGHT SINGLE QUOTATION MARK is not in the map; U+0027 APOSTROPHE is.
        assert_eq!(a.terms("don\u{2019}t"), ["don\u{2019}t"]);
        assert_eq!(a.terms("don't"), ["don", "t"]);
    }

    #[test]
    fn folds_case_and_drops_stopwords() {
        let a = Analyzer::new(&graphiti_stopwords());
        assert_eq!(
            a.terms("The Quick Brown Fox IS in The Box"),
            ["quick", "brown", "fox", "box"]
        );
        assert_eq!(a.term_count("The Quick Brown Fox IS in The Box"), 4);
    }

    /// A run of separators yields no empty terms, and a string of nothing but separators
    /// yields none at all — an empty document is legal and must not become a term.
    #[test]
    fn runs_of_separators_produce_no_empty_terms() {
        let a = Analyzer::new(&[]);
        assert_eq!(a.terms("a -- b,,,  c"), ["a", "b", "c"]);
        assert!(a.terms("  --,.  ").is_empty());
        assert_eq!(a.term_count("  --,.  "), 0);
        assert!(a.terms("").is_empty());
    }

    /// `terms` and `term_count` must never disagree: the count is the BM25 length norm
    /// for the very postings `terms` produces, so a divergence would mis-score every
    /// document without failing anything.
    #[test]
    fn term_count_agrees_with_terms() {
        let a = Analyzer::new(&graphiti_stopwords());
        for text in [
            "",
            "the",
            "Alice knows Bob",
            "a,b.c-d/e",
            "The rain in Spain: mostly on the plain!",
            "group_id _leading trailing_",
            "\u{2019}\u{2014}unicode\u{2014}\u{2019}",
        ] {
            assert_eq!(
                a.terms(text).len(),
                a.term_count(text),
                "disagreement on {text:?}"
            );
        }
    }

    /// The stopword list arrives from a declaration, so it may be unsorted, mixed case or
    /// hold duplicates. Normalising once at construction is what makes the per-token test
    /// a binary search.
    #[test]
    fn stopwords_are_normalised_at_construction() {
        let a = Analyzer::new(&[
            "The".to_string(),
            "a".to_string(),
            "THE".to_string(),
            "Is".to_string(),
        ]);
        assert_eq!(a.terms("The A is here"), ["here"]);
    }
}
