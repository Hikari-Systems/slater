// SPDX-License-Identifier: Apache-2.0
//! Parsing the query string a client sends to `db.idx.fulltext.query*`.
//!
//! # The whole grammar
//!
//! ```text
//! query        := ws* group* ws*
//! group        := filter_group | term_group
//! filter_group := '(' '@' field ':' value ('|' value)* ')'
//! term_group   := '(' term (('|') term)* ')'
//! value        := '"' escaped* '"'
//! ```
//!
//! That is not a subset chosen for convenience — it is everything that can arrive.
//! Graphiti builds its query with `build_falkor_fulltext_query`, which emits exactly
//! `(@group_id:"g1"|"g2") (term1 | term2)`, and runs `sanitize_falkor_fulltext_query` over
//! the user's text first, replacing every character that could spell a phrase, wildcard,
//! negation, range or nested boolean with whitespace. Those constructs are unreachable, so
//! implementing them would be implementing something nothing can ask for.
//!
//! Anything outside the grammar is **refused, loudly**. A query form Slater does not
//! support must not be mistaken for a query that matched nothing — that is the failure
//! mode where a client silently gets worse results forever, and it is exactly what an
//! empty return would look like.
//!
//! # Escaping
//!
//! `_escape_fulltext_group_id` backslash-escapes every non-alphanumeric character in a
//! group id before wrapping it in quotes, so `my-group` arrives as `"my\-group"`. A
//! backslash therefore always means "the next character, literally".
//!
//! # Why a value becomes several terms
//!
//! A quoted value is analyzed like any other text, so `"550e8400-e29b-41d4-…"` — a
//! perfectly ordinary group id — becomes five terms, because `-` is a separator. The
//! filter requires all of them. See [`FilterAlternative`] for why the resulting
//! over-approximation is safe.

use anyhow::{bail, Context, Result};

use super::search::{FilterAlternative, FulltextQuery};
use super::Analyzer;

/// Parse `input` against an index's declared `properties` (position = field id) and
/// `analyzer`.
///
/// An empty or whitespace-only input parses to an empty query, which matches nothing.
/// That is a legal input rather than an error: graphiti returns `''` from
/// `build_falkor_fulltext_query` whenever every word of the user's text was a stopword.
pub fn parse_query(
    input: &str,
    properties: &[String],
    analyzer: &Analyzer,
) -> Result<FulltextQuery> {
    let mut p = Parser {
        s: input.as_bytes(),
        i: 0,
        properties,
        analyzer,
    };
    let mut out = FulltextQuery::default();
    // Tracked explicitly rather than inferred from `out.terms.is_empty()`: a term group
    // whose every word was a stopword analyzes to nothing, and inferring would then let a
    // second group through as if the first had never appeared.
    let mut saw_terms = false;
    p.ws();
    while p.i < p.s.len() {
        if p.s[p.i] != b'(' {
            bail!(
                "unsupported full-text query syntax at byte {}: expected '(' but found {:?}. \
                 Slater accepts `(@field:\"value\") (term | term)` — the form \
                 db.idx.fulltext.query* is given; phrases, wildcards, negation and ranges \
                 are not supported",
                p.i,
                input[p.i..].chars().next().unwrap_or('?')
            );
        }
        p.i += 1; // consume '('
        p.ws();
        if p.i < p.s.len() && p.s[p.i] == b'@' {
            out.filters.push(p.filter_group()?);
        } else {
            if saw_terms {
                bail!("full-text query has more than one term group; expected at most one");
            }
            saw_terms = true;
            out.terms = p.term_group()?;
        }
        p.ws();
    }
    Ok(out)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    properties: &'a [String],
    analyzer: &'a Analyzer,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    /// `@field:"v"("|""v")*)` — the leading `(` and `@` are already consumed.
    fn filter_group(&mut self) -> Result<Vec<FilterAlternative>> {
        self.i += 1; // '@'
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i] != b':' {
            self.i += 1;
        }
        if self.i >= self.s.len() {
            bail!("full-text field filter has no ':' after the field name");
        }
        let name = std::str::from_utf8(&self.s[start..self.i])
            .context("full-text field name is not UTF-8")?
            .trim()
            .to_string();
        self.i += 1; // ':'

        // A filter naming a property the index does not cover cannot be answered, and
        // answering it as "matches nothing" would look identical to a genuinely empty
        // result. Name the field and what is available.
        let field = self
            .properties
            .iter()
            .position(|p| *p == name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "full-text query filters on '@{name}', which this index does not cover; \
                     it indexes: {}",
                    self.properties.join(", ")
                )
            })? as u32;

        let mut alts = Vec::new();
        loop {
            self.ws();
            let value = self.value()?;
            alts.push(FilterAlternative {
                field,
                terms: self.analyzer.terms(&value),
            });
            self.ws();
            match self.s.get(self.i) {
                Some(b'|') => self.i += 1,
                Some(b')') => {
                    self.i += 1;
                    break;
                }
                Some(c) => bail!(
                    "unexpected {:?} in a full-text field filter; expected '|' or ')'",
                    *c as char
                ),
                None => bail!("unterminated full-text field filter: missing ')'"),
            }
        }
        Ok(alts)
    }

    /// A double-quoted, backslash-escaped value.
    fn value(&mut self) -> Result<String> {
        if self.s.get(self.i) != Some(&b'"') {
            bail!(
                "a full-text field filter value must be double-quoted (byte {})",
                self.i
            );
        }
        self.i += 1;
        let mut out = Vec::new();
        loop {
            match self.s.get(self.i) {
                None => bail!("unterminated quoted value in a full-text query"),
                Some(b'\\') => {
                    // A backslash always means "the next byte, literally" — that is how
                    // `_escape_fulltext_group_id` escapes, and it is why a trailing
                    // backslash is malformed rather than a literal one.
                    self.i += 1;
                    match self.s.get(self.i) {
                        Some(c) => out.push(*c),
                        None => bail!("full-text query ends in a trailing backslash"),
                    }
                    self.i += 1;
                }
                Some(b'"') => {
                    self.i += 1;
                    break;
                }
                Some(c) => {
                    out.push(*c);
                    self.i += 1;
                }
            }
        }
        String::from_utf8(out).context("full-text filter value is not UTF-8")
    }

    /// `term (| term)*)` — the leading `(` is already consumed.
    fn term_group(&mut self) -> Result<Vec<String>> {
        let mut terms = Vec::new();
        loop {
            self.ws();
            if self.s.get(self.i) == Some(&b')') {
                self.i += 1;
                break;
            }
            let start = self.i;
            while self
                .s
                .get(self.i)
                .is_some_and(|c| *c != b'|' && *c != b')' && !c.is_ascii_whitespace())
            {
                self.i += 1;
            }
            if self.i == start {
                bail!("unterminated full-text term group: missing ')'");
            }
            let raw = std::str::from_utf8(&self.s[start..self.i])
                .context("full-text query term is not UTF-8")?;
            // Analyzed, not taken verbatim: the index stores lowercased terms, and
            // graphiti passes query words through with their original case (it lowercases
            // only to test a stopword). A term that analyzes away — a stopword that
            // survived, or punctuation — simply drops out.
            terms.extend(self.analyzer.terms(raw));
            self.ws();
            if self.s.get(self.i) == Some(&b'|') {
                self.i += 1;
            }
        }
        Ok(terms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props() -> Vec<String> {
        ["name", "summary", "group_id"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn analyzer() -> Analyzer {
        Analyzer::new(&["a".to_string(), "the".to_string(), "is".to_string()])
    }

    fn parse(q: &str) -> Result<FulltextQuery> {
        parse_query(q, &props(), &analyzer())
    }

    /// Every string below was produced by running graphiti-core 0.29.3's
    /// `build_falkor_fulltext_query` and copying its output verbatim — including the
    /// escaping, the leading space of the no-group form, and the empty string it returns
    /// when the user's text was entirely stopwords. If a graphiti upgrade changes the
    /// shape it emits, this is the test that says so.
    #[test]
    fn parses_every_shape_graphiti_actually_emits() {
        // (input text, group ids)                    -> emitted query
        let cases: &[(&str, &str, &[&str], &[&str])] = &[
            // "Alice Smith" / ["verify"]
            (
                r#"(@group_id:"verify") (Alice | Smith)"#,
                "two words, one group",
                &["alice", "smith"],
                &["verify"],
            ),
            // "Who is Alice?" / ["verify"] — `is` is a stopword, `?` a separator
            (
                r#"(@group_id:"verify") (Who | Alice)"#,
                "stopword and punctuation already stripped by graphiti",
                &["who", "alice"],
                &["verify"],
            ),
            // "alice" / None — note the leading space where the filter would go
            (" (alice)", "no group ids", &["alice"], &[]),
            // "C++ / Rust: memory-safety!" / ["verify"]
            (
                r#"(@group_id:"verify") (C | Rust | memory | safety)"#,
                "separators split before we ever see them",
                &["c", "rust", "memory", "safety"],
                &["verify"],
            ),
            // "Alice" / ["550e8400-e29b-41d4-a716-446655440000"]
            (
                r#"(@group_id:"550e8400\-e29b\-41d4\-a716\-446655440000") (Alice)"#,
                "a UUID group id, escaped",
                &["alice"],
                &["550e8400", "e29b", "41d4", "a716", "446655440000"],
            ),
        ];
        for (q, what, want_terms, want_filter) in cases {
            let got = parse(q).unwrap_or_else(|e| panic!("{what}: {q:?} failed: {e:#}"));
            assert_eq!(got.terms, *want_terms, "{what}: {q:?}");
            let filter: Vec<&str> = got
                .filters
                .first()
                .map(|g| g[0].terms.iter().map(|s| s.as_str()).collect())
                .unwrap_or_default();
            assert_eq!(filter, *want_filter, "{what}: {q:?}");
        }

        // "Alice" / ["_slater_seed_", "g2"] — two group ids become two alternatives.
        let two = parse(r#"(@group_id:"\_slater\_seed\_"|"g2") (Alice)"#).unwrap();
        assert_eq!(two.filters.len(), 1);
        assert_eq!(two.filters[0][0].terms, ["_slater_seed_"]);
        assert_eq!(two.filters[0][1].terms, ["g2"]);

        // "the a is" / ["verify"] — graphiti returns `''` outright and never calls us,
        // but the empty string must still parse rather than error.
        assert_eq!(parse("").unwrap(), FulltextQuery::default());
    }

    /// The exact string `build_falkor_fulltext_query` produces for a one-group search.
    #[test]
    fn parses_graphitis_query_verbatim() {
        let q = parse(r#"(@group_id:"verify") (Alice | Bob)"#).unwrap();
        assert_eq!(
            q.terms,
            ["alice", "bob"],
            "terms are analyzed, so lowercased"
        );
        assert_eq!(
            q.filters,
            vec![vec![FilterAlternative {
                field: 2,
                terms: vec!["verify".into()]
            }]]
        );
    }

    /// With no group ids graphiti emits an empty filter and a leading space.
    #[test]
    fn parses_the_no_group_form_with_its_leading_space() {
        let q = parse(" (alpha | beta)").unwrap();
        assert!(q.filters.is_empty());
        assert_eq!(q.terms, ["alpha", "beta"]);
    }

    /// Several group ids become alternatives of one group: OR, not AND.
    #[test]
    fn several_group_ids_are_alternatives_of_one_group() {
        let q = parse(r#"(@group_id:"g1"|"g2") (alpha)"#).unwrap();
        assert_eq!(q.filters.len(), 1, "one group");
        assert_eq!(q.filters[0].len(), 2, "two alternatives within it");
        assert_eq!(q.filters[0][1].terms, ["g2"]);
    }

    /// `_escape_fulltext_group_id` backslash-escapes every non-alphanumeric character, so
    /// a hyphenated or underscored group id arrives escaped and must come back intact.
    #[test]
    fn unescapes_a_group_id_the_way_graphiti_escaped_it() {
        // graphiti turns `_slater_seed_` into `\_slater\_seed\_`.
        let q = parse(r#"(@group_id:"\_slater\_seed\_") (alpha)"#).unwrap();
        assert_eq!(
            q.filters[0][0].terms,
            ["_slater_seed_"],
            "`_` is not a separator, so this stays one term"
        );
    }

    /// A UUID group id is the realistic multi-token case: `-` *is* a separator, so the
    /// value analyzes to several terms and the filter must require all of them.
    #[test]
    fn a_uuid_group_id_becomes_a_conjunction_of_terms() {
        let q = parse(r#"(@group_id:"550e8400\-e29b\-41d4") (alpha)"#).unwrap();
        assert_eq!(q.filters[0][0].terms, ["550e8400", "e29b", "41d4"]);
    }

    /// A value that is entirely stopwords analyzes to nothing, which means "no
    /// constraint" rather than "matches nothing" — see `FilterAlternative::terms`.
    #[test]
    fn a_value_that_analyzes_away_is_no_constraint() {
        let q = parse(r#"(@group_id:"the") (alpha)"#).unwrap();
        assert!(q.filters[0][0].terms.is_empty());
    }

    /// An empty query is legal and matches nothing — graphiti returns `''` when every
    /// word of the user's text was a stopword.
    #[test]
    fn an_empty_query_is_legal_and_empty() {
        for empty in ["", "   ", "\n"] {
            let q = parse(empty).unwrap();
            assert!(q.terms.is_empty() && q.filters.is_empty(), "{empty:?}");
        }
    }

    /// A filter on a property the index does not cover must name the problem. Answering
    /// it as "no results" would be indistinguishable from a genuinely empty search.
    #[test]
    fn a_filter_on_an_unindexed_field_is_refused_by_name() {
        let err = parse(r#"(@created_at:"x") (alpha)"#).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("@created_at"), "{msg}");
        assert!(msg.contains("name, summary, group_id"), "{msg}");
    }

    /// Everything outside the grammar is refused rather than silently ignored — a query
    /// form Slater cannot answer must not look like a query that matched nothing.
    #[test]
    fn unsupported_syntax_is_refused_not_silently_dropped() {
        for bad in [
            "alpha",                        // bare terms, no group
            "(alpha",                       // unterminated term group
            r#"(@group_id:"g") (a) (b)"#,   // two term groups
            r#"(@group_id:"g"#,             // unterminated value
            r#"(@group_id:g) (a)"#,         // unquoted value
            r#"(@group_id) (a)"#,           // no colon
            r#"(@group_id:"g" & "h") (a)"#, // an operator that cannot arrive
            r#"(@group_id:"g\"#,            // trailing backslash
        ] {
            assert!(parse(bad).is_err(), "should have been refused: {bad:?}");
        }
    }

    /// The message has to be actionable — it is what a client author sees when they try a
    /// RediSearch feature Slater does not implement.
    #[test]
    fn the_refusal_names_what_is_accepted() {
        let msg = format!("{:#}", parse("alpha*").unwrap_err());
        assert!(
            msg.contains("phrases, wildcards, negation and ranges"),
            "{msg}"
        );
    }
}
