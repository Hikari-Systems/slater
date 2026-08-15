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
//! **Two arms, not one.** The built index covers the core generation; everything written
//! since — sealed segments and the write delta — is served by an *overlay scan* that
//! analyzes the affected documents' current text at query time. That is far cheaper here
//! than it is for vectors, and for one reason: **text is never routed out of the property
//! record**. `n.summary` is an ordinary property the merged view already reads correctly,
//! so there is no new delta format, no WAL change and no index data through consolidation
//! — only the declaration.
//!
//! The candidate set is `delta.node_dense_ids() ∪ every segment's node_ids()`, bounded by
//! the delta and segment sizes rather than the graph's. Those same ids are **suppressed**
//! in the core arm, so a document whose text changed is scored once, from its current
//! text, rather than twice or from a stale copy.
//!
//! # The approximation, stated rather than hidden
//!
//! Both arms score with **one reconciled idf**, taken from the core index's statistics, so
//! a term's weight cannot jump depending on which arm found a document — that would
//! reorder results by how recently something was written, which reads as a ranking opinion
//! rather than a bug.
//!
//! Those statistics are slightly stale by construction: a superseded core document still
//! contributes to the stored `df(t)` and `doc_count`, and cannot be subtracted without its
//! old text, which the index does not keep. So both over-count by at most the overlay's
//! size — an `O(overlay/N)` uniform downward bias on recently-edited terms, which vanishes
//! at `CALL slater.consolidate()`. Ranking is affected; correctness is not.

use graph_format::fulltext::bm25;
use graph_format::fulltext::bm25::Bm25Params;
use graph_format::fulltext::index::DocEntry;
use graph_format::fulltext::query::parse_query;
use graph_format::fulltext::search::{search, FulltextQuery, Hit};
use graph_format::fulltext::Analyzer;
use graph_format::manifest::EntityKind;
use std::collections::{HashMap, HashSet};

use super::*;

#[cfg(test)]
thread_local! {
    /// Test-only seam: how many `.docs.blk` records this statement read.
    ///
    /// The document record is what a relationship hit needs (its endpoints), and the
    /// only honest way to assert that fetching it costs O(hits) rather than O(corpus)
    /// is to count the reads. Thread-local, and the full-text call runs entirely on the
    /// calling thread, so the count is exact.
    pub(crate) static DOC_READ_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Read a document record, counting the read under `cfg(test)`.
fn read_doc(r: &graph_format::fulltext::index::FulltextReader, docid: u64) -> Result<DocEntry> {
    #[cfg(test)]
    DOC_READ_COUNT.with(|c| c.set(c.get() + 1));
    r.doc(docid)
}

impl<'g, V: ReadView> Engine<'g, V> {
    /// Dense ids of every entity a level above the core generation has touched: the write
    /// delta's, plus every sealed segment's rows.
    ///
    /// A segment already enumerates the rows it holds (`SegmentReader::node_ids`), so this
    /// needs no per-segment sidecar of its own — the touched set the design called for was
    /// already on disk.
    ///
    /// **Relationships have no overlay arm.** A relationship's text can only reach the
    /// delta through an edge write, and the writable layer's edge rows carry patches
    /// rather than a scannable identity set; more to the point, graphiti's edge search
    /// reads facts that are written once with the edge. So an edge index is served from
    /// the core alone, and its recall gap closes at consolidation. Returning an empty set
    /// here is what makes that explicit rather than accidental.
    fn fulltext_overlay_ids(&self, relationships: bool) -> Result<HashSet<u64>> {
        let mut ids: HashSet<u64> = HashSet::new();
        if relationships {
            return Ok(ids);
        }
        ids.extend(self.gen.delta().node_dense_ids());
        let stack = self.gen.core_stack();
        if !stack.is_singleton() {
            for seg in stack.segments() {
                ids.extend(seg.reader.node_ids());
            }
        }
        Ok(ids)
    }

    /// Whether this entity has been deleted out from under the built index.
    fn fulltext_dead(&self, id: u64, relationships: bool) -> Result<bool> {
        if relationships {
            // An edge index is core-only, so the only suppression that applies is a
            // deleted *endpoint*, which the executor already drops downstream.
            return Ok(false);
        }
        let stack = self.gen.core_stack();
        Ok(self.gen.delta().is_tombstoned(id)
            || (!stack.is_singleton() && stack.is_node_tombstoned(id)?))
    }

    /// A synthetic document record for an **overlay** hit, which is in no `.docs.blk`.
    ///
    /// A core hit never comes here: its record was read to score it and is carried
    /// forward, so this is not the place to go looking for one. It used to be — this
    /// scanned `.docs.blk` from the top to re-find a record the caller had just held,
    /// which made every relationship query O(hits x corpus).
    ///
    /// `len` is 0 because nothing downstream reads it, and `endpoints` is `None` because
    /// the overlay arm is nodes-only; when it grows an edge arm, this is where a born
    /// edge's endpoints come from.
    fn fulltext_doc_entry(&self, _fc: &FulltextCallClause, entity: u64) -> Result<DocEntry> {
        Ok(DocEntry {
            entity,
            len: 0,
            endpoints: None,
        })
    }

    /// Score the overlay's documents against `q`, using the **core index's** corpus
    /// statistics so both arms put a term on one scale (see the module note).
    fn fulltext_overlay_hits(
        &self,
        fc: &FulltextCallClause,
        desc: &graph_format::manifest::FulltextIndexDesc,
        analyzer: &Analyzer,
        q: &FulltextQuery,
        overlay: &HashSet<u64>,
    ) -> Result<Vec<(u64, f64)>> {
        let mut out = Vec::new();
        if q.terms.is_empty() || overlay.is_empty() {
            return Ok(out);
        }
        let Some(label_id) = self.gen.label_id(&fc.label) else {
            // The label is not in the symbol table at all, so nothing can carry it.
            return Ok(out);
        };
        // `doc_count` is the core's `N`. A generation built before anything was written
        // has none, and idf then degenerates — treat the overlay as the whole corpus.
        let n_docs = desc.doc_count.max(overlay.len() as u64);

        // Ascending, so equal scores break ties the same way the core arm's do.
        let mut ids: Vec<u64> = overlay.iter().copied().collect();
        ids.sort_unstable();
        for id in ids {
            self.check_deadline()?;
            if self.fulltext_dead(id, false)? {
                continue;
            }
            if !self.node_label_ids(id)?.contains(&label_id) {
                continue;
            }
            // Analyze the entity's *current* text, field by field, exactly as the builder
            // did — same analyzer, same declaration order, so a term found here is the
            // term the core index would have stored.
            let mut tfs: HashMap<String, u32> = HashMap::new();
            let mut per_field: Vec<HashMap<String, u32>> =
                vec![HashMap::new(); desc.properties.len()];
            let mut len = 0u32;
            for (field, prop) in desc.properties.iter().enumerate() {
                let Val::Str(text) = self.node_prop(id, prop)? else {
                    continue;
                };
                for term in analyzer.terms(&text) {
                    *per_field[field].entry(term.clone()).or_insert(0) += 1;
                    *tfs.entry(term).or_insert(0) += 1;
                    len += 1;
                }
            }
            if len == 0 {
                continue; // no indexable text: not a document, exactly as at build time
            }
            // Required filters, evaluated against this document's own fields.
            let passes = q.filters.iter().all(|group| {
                group.iter().any(|alt| {
                    alt.terms.is_empty()
                        || alt.terms.iter().all(|t| {
                            per_field
                                .get(alt.field as usize)
                                .is_some_and(|f| f.contains_key(t))
                        })
                })
            });
            if !passes {
                continue;
            }
            let mut score = 0.0f64;
            for term in &q.terms {
                let Some(tf) = tfs.get(term) else { continue };
                // The term's *core* document frequency — the reconciliation that keeps
                // both arms on one scale. A term the core has never seen has `df = 0`,
                // which scores it as maximally rare, and that is the honest answer.
                let df = self
                    .gen
                    .fulltext_index(graph_format::manifest::EntityKind::Node, &fc.label)
                    .map(|r| -> Result<u64> {
                        Ok(r.term_metas(term)?.first().map_or(0, |m| m.doc_df))
                    })
                    .transpose()?
                    .unwrap_or(0);
                score += bm25::term_score(
                    bm25::idf(n_docs, df),
                    *tf,
                    len,
                    desc.avg_doc_len,
                    Bm25Params::default(),
                );
            }
            if score > 0.0 {
                out.push((id, score));
            }
        }
        Ok(out)
    }

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
        // Every entity some level above the built index has touched. Computed once per
        // statement — it does not vary by row — and bounded by the delta and segment
        // sizes, never by the graph's.
        let overlay = self.fulltext_overlay_ids(fc.relationships)?;

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let query_str = match self.eval(&fc.query, &scope, None)? {
                Val::Str(s) => s,
                Val::Null => String::new(),
                other => bail!("{proc} query must be a string, got {}", other.to_display()),
            };
            // Not `.context(…)`: a Bolt failure renders the error with anyhow's `Display`,
            // which shows only the *outermost* message — so wrapping would replace "this
            // syntax is not supported" with "in db.idx.fulltext.queryNodes(…)" and tell
            // the client nothing. Flatten the chain into one message instead, reason
            // first, location after.
            let q = parse_query(&query_str, &desc.properties, &analyzer)
                .map_err(|e| anyhow::anyhow!("{e:#} (in {proc}('{}', …))", fc.label))?;

            // ── the core arm, minus everything a newer level has touched ──
            //
            // The document record is carried forward, not just the entity id. Scoring a
            // hit already reads the record — it is how the entity id is known — and for a
            // relationship that record is also the only place the endpoints live. Keeping
            // it is what stops the projection below having to find it again.
            let mut scored: Vec<(u64, f64, Option<DocEntry>)> = Vec::new();
            if let Some(r) = reader {
                // Over-fetch by the overlay's size: a suppressed hit consumes a slot the
                // limit would otherwise have given to a live document, so asking for
                // exactly `limit` here can return fewer than `limit` live results.
                let want = limit.saturating_add(overlay.len());
                for hit in search(r, &q, desc.avg_doc_len, params, want)? {
                    let doc = read_doc(r, hit.doc)?;
                    let entity = doc.entity;
                    if overlay.contains(&entity) || self.fulltext_dead(entity, fc.relationships)? {
                        continue;
                    }
                    scored.push((entity, hit.score, Some(doc)));
                }
            }

            // ── the overlay arm ──
            // An overlay document is in no `.docs.blk`, so it brings no record; the
            // projection synthesises one.
            scored.extend(
                self.fulltext_overlay_hits(fc, desc, &analyzer, &q, &overlay)?
                    .into_iter()
                    .map(|(e, s)| (e, s, None)),
            );

            // One document can reach here from at most one arm — the core arm skipped
            // every id the overlay owns — so a duplicate would be a suppression bug.
            debug_assert_eq!(
                scored
                    .iter()
                    .map(|(e, ..)| *e)
                    .collect::<HashSet<_>>()
                    .len(),
                scored.len(),
                "a full-text document was scored by both arms"
            );
            // Descending score, then ascending entity id so a repeated query is stable.
            scored.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            scored.truncate(limit);
            self.charge(scored.len() as u64)?;

            for (entity, score, carried) in scored {
                let hit = Hit { doc: 0, score };
                let doc = match carried {
                    Some(d) => d,
                    None => self.fulltext_doc_entry(fc, entity)?,
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
