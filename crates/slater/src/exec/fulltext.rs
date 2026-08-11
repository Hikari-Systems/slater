// SPDX-License-Identifier: Apache-2.0
//! `Engine` methods for `CALL db.idx.fulltext.query{Nodes,Relationships}`.
//!
//! Mirrors [`Engine::apply_vector_call`](super::Engine::apply_vector_call) in shape — a
//! reading clause that expands each input row with its hits and binds the `YIELD` outputs
//! — and differs from it in three ways that are worth knowing before reading the code.
//!
//! **There is no `k`.** Graphiti passes only `(label, query)`, so nothing in the statement
//! bounds the result set. It is bounded by `fulltext.maxHits` instead, and a caller
//! wanting fewer writes a `LIMIT` (which is what graphiti does: `ORDER BY score DESC LIMIT
//! $limit`).
//!
//! **`score` is BM25 and descends.** The vector procedure's `score` is a *distance* and
//! ascends (D26). Same namespace, opposite convention — see the D-FT1 note in
//! `graph_format::fulltext::bm25`.
//!
//! **The core arm is the only arm.** A document written through the write delta since the
//! generation was built is *not* searchable yet; delta and segment visibility is separate
//! work. This is a recall gap, not a correctness one — every hit returned is real and
//! correctly scored — but it is a gap, and `CALL slater.consolidate()` closes it by
//! rebuilding the index.

use anyhow::Context as _;
use graph_format::fulltext::bm25::Bm25Params;
use graph_format::fulltext::query::parse_query;
use graph_format::fulltext::search::search;
use graph_format::fulltext::Analyzer;
use graph_format::manifest::EntityKind;

use super::*;

impl<'g, V: ReadView> Engine<'g, V> {
    /// Expand each input row with the index's hits for the query, binding the `YIELD`
    /// outputs (`node` or `relationship`, and `score`).
    pub(crate) fn apply_fulltext_call(
        &self,
        table: Table,
        fc: &FulltextCallClause,
    ) -> Result<Table> {
        let entity = if fc.relationships {
            EntityKind::Edge
        } else {
            EntityKind::Node
        };
        let proc = if fc.relationships {
            "db.idx.fulltext.queryRelationships"
        } else {
            "db.idx.fulltext.queryNodes"
        };

        // A declared index whose files were never written (a relationship index, today)
        // resolves to a descriptor with no reader. That is deliberately *not* an error:
        // it is an empty index, and an empty index answers nothing. An index that was
        // never declared at all is an error, because the query names something the graph
        // does not have and answering "no results" would hide the misconfiguration.
        let Some(desc) = self
            .gen
            .manifest()
            .fulltext_indexes
            .iter()
            .find(|f| f.entity == entity && f.label_or_type == fc.label)
        else {
            bail!(
                "no full-text index on {}{} — {proc} needs one declared at build time",
                if fc.relationships { "" } else { ":" },
                fc.label
            );
        };
        let analyzer = Analyzer::new(&desc.stopwords);

        // Parsed per statement, not per row: the query is a `$param` in every observed
        // call, so it is constant across rows, and parsing it once also means a malformed
        // query fails before any row is processed rather than partway through.
        let mut new_vars: Vec<String> = Vec::new();
        for (_, bound) in &fc.yields {
            if !table.cols.contains(bound) && !new_vars.contains(bound) {
                new_vars.push(bound.clone());
            }
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let reader = self.gen.fulltext_index(entity, &fc.label);
        let limit = self.fulltext_max_hits;
        let params = Bm25Params::default();

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let query_str = match self.eval(&fc.query, &scope, None)? {
                Val::Str(s) => s,
                Val::Null => String::new(),
                other => bail!("{proc} query must be a string, got {}", other.to_display()),
            };
            let hits = match reader {
                None => Vec::new(),
                Some(r) => {
                    let q = parse_query(&query_str, &desc.properties, &analyzer)
                        .with_context(|| format!("in {proc}('{}', …)", fc.label))?;
                    // Charge the hits against the row budget before building them, so a
                    // broad query is stopped by the same accounting every other clause is.
                    let hits = search(r, &q, desc.avg_doc_len, params, limit)?;
                    self.charge(hits.len() as u64)?;
                    hits
                }
            };

            for hit in hits {
                let doc = match reader {
                    Some(r) => r.doc(hit.doc)?,
                    None => unreachable!("hits are empty when there is no reader"),
                };
                let mut r = row.clone();
                for bound in &new_vars {
                    let output = fc
                        .yields
                        .iter()
                        .find(|(_, b)| b == bound)
                        .map(|(o, _)| o.as_str())
                        .unwrap_or("");
                    r.push(match output {
                        "node" => Val::Node(doc.entity),
                        // The endpoints ride the document record precisely so this does not
                        // need an adjacency read per hit.
                        "relationship" => match doc.endpoints {
                            Some((start, end, reltype)) => Val::Rel {
                                id: doc.entity,
                                start,
                                end,
                                reltype,
                            },
                            None => bail!(
                                "full-text index on {} has no relationship endpoints — it was \
                                 built as a node index",
                                fc.label
                            ),
                        },
                        "score" => Val::Float(hit.score),
                        _ => Val::Null,
                    });
                }
                if let Some(w) = &fc.where_ {
                    let row_scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &row_scope, None)?) != Some(true) {
                        continue;
                    }
                }
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }
}
