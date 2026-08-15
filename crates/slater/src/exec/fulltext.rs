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
//! The candidate set is `delta.node_dense_ids() ∪ every segment's node_ids()` for nodes,
//! and the edge twin of that for relationships, bounded by the delta and segment sizes
//! rather than the graph's. Those same ids are **suppressed** in the core arm, so a
//! document whose text changed is scored once, from its current text, rather than twice
//! or from a stale copy.
//!
//! **Relationships differ in one way only.** An edge id cannot be resolved to its
//! endpoints — the core CSR is keyed by node — and a hit must be yielded as a bound
//! relationship, which carries them. So the edge candidate set is gathered as a map of
//! `id -> (src, dst, reltype)` rather than a bare id set, which is the same reason
//! `.docs.blk` stores endpoints for a core document. Everything downstream is shared.
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
    fn fulltext_overlay_ids(&self) -> Result<HashSet<u64>> {
        let mut ids: HashSet<u64> = HashSet::new();
        ids.extend(self.gen.delta().node_dense_ids());
        let stack = self.gen.core_stack();
        if !stack.is_singleton() {
            for seg in stack.segments() {
                ids.extend(seg.reader.node_ids());
            }
        }
        Ok(ids)
    }

    /// The edge twin: `edge id -> (src, dst, reltype)` for every edge a level above the
    /// core generation has touched.
    ///
    /// Endpoints are gathered here rather than looked up per hit, for the same reason
    /// `.docs.blk` stores them: an edge id alone cannot be resolved to its endpoints
    /// without an adjacency read, and the core CSR is keyed by node, not by edge. Every
    /// source below already knows them, so collecting once per statement costs nothing
    /// extra and makes the per-hit path a map lookup.
    ///
    /// Three sources, mirroring the three ways an edge can sit above the core:
    ///
    /// - a **sealed segment** row, which carries `src`/`dst`/`reltype` directly;
    /// - a **patched core edge**, whose endpoints the delta records when it resolves the
    ///   patch (they cannot be recovered from the core by id);
    /// - a **delta-born** edge, walked as `adj_out_nodes x out_edges` — the only
    ///   enumeration that yields an edge id beside its endpoints in one pass.
    ///
    /// Later sources win, which is the level order: a born edge cannot collide with a
    /// core id, but a core edge patched in the delta must beat the same edge's segment
    /// row.
    fn fulltext_edge_overlay(&self) -> Result<HashMap<u64, (u64, u64, u32)>> {
        let mut m: HashMap<u64, (u64, u64, u32)> = HashMap::new();
        let stack = self.gen.core_stack();
        if !stack.is_singleton() {
            for seg in stack.segments() {
                for id in seg.reader.edge_ids() {
                    let Some(row) = stack.resolve_edge_row(id)? else {
                        continue;
                    };
                    if let Some(rt) = self.gen.reltype_id(&row.reltype) {
                        m.insert(id, (row.src, row.dst, rt));
                    }
                }
            }
        }
        let delta = self.gen.delta();
        if delta.is_empty() {
            return Ok(m);
        }
        for (id, src, dst, rt_name) in delta.core_patched_edges() {
            if let Some(rt) = self.gen.reltype_id(&rt_name) {
                m.insert(id, (src, dst, rt));
            }
        }
        for src in delta.adj_out_nodes() {
            for e in delta.out_edges(src) {
                // A tombstone-only entry carries no edge id and suppresses rather than
                // contributes; `fulltext_dead` is what applies it.
                let Some(eid) = e.edge_id else { continue };
                if e.tombstoned {
                    continue;
                }
                if let Some(rt) = self.gen.reltype_id(&e.reltype) {
                    m.insert(eid, (src, e.other, rt));
                }
            }
        }
        Ok(m)
    }

    /// Whether a **node** has been deleted, in the delta or in a sealed segment. The
    /// overlay arm asks this directly (it holds ids, not document records); the edge arm
    /// asks it of each endpoint.
    fn fulltext_node_dead(&self, id: u64) -> Result<bool> {
        let stack = self.gen.core_stack();
        Ok(self.gen.delta().is_tombstoned(id)
            || (!stack.is_singleton() && stack.is_node_tombstoned(id)?))
    }

    /// Whether this document has been deleted out from under the built index.
    ///
    /// Takes the whole document record rather than an id because a **relationship**
    /// cannot be checked without its endpoints, and the record already carries them —
    /// the caller read it to score the hit.
    ///
    /// Deletion reaches an edge three ways, and all three are asked here:
    ///
    /// 1. **An endpoint was deleted**, which takes every incident edge with it. An
    ///    earlier comment here claimed the executor dropped these downstream; it does
    ///    not, and a regression test now pins that.
    /// 2. **A sealed segment removed the edge**, by id — segments have always suppressed
    ///    that way.
    /// 3. **The live delta tombstoned it**, by `(reltype, neighbour)`. That is the exact
    ///    predicate the traversal overlay applies (`exec.rs`, the `suppress` set), and
    ///    matching it verbatim is deliberate: suppressing every parallel edge of a type
    ///    to a neighbour is precisely what a delete does today, so full text agrees with
    ///    traversal by construction rather than by coincidence. When a keyed `DELETE`
    ///    later narrows it, this narrows with it and needs no edit.
    fn fulltext_dead(&self, doc: &DocEntry, relationships: bool) -> Result<bool> {
        let stack = self.gen.core_stack();
        let node_dead = |id: u64| self.fulltext_node_dead(id);
        if !relationships {
            return node_dead(doc.entity);
        }
        let Some((src, dst, reltype)) = doc.endpoints else {
            // A relationship index whose documents carry no endpoints is malformed; the
            // projection refuses it by name a few lines on, which is a better error than
            // anything invented here.
            return Ok(false);
        };
        if node_dead(src)? || node_dead(dst)? {
            return Ok(true);
        }
        if !stack.is_singleton() {
            if let Some(row) = stack.resolve_edge_row(doc.entity)? {
                if row.tombstoned {
                    return Ok(true);
                }
            }
        }
        let delta = self.gen.delta();
        if !delta.is_empty() {
            for e in delta.out_edges(src) {
                if e.tombstoned
                    && e.other == dst
                    && self.gen.reltype_id(&e.reltype) == Some(reltype)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// A synthetic document record for an **overlay** hit, which is in no `.docs.blk`.
    ///
    /// A core hit never comes here: its record was read to score it and is carried
    /// forward, so this is not the place to go looking for one. It used to be — this
    /// scanned `.docs.blk` from the top to re-find a record the caller had just held,
    /// which made every relationship query O(hits x corpus).
    ///
    /// `len` is 0 because nothing downstream reads it. `endpoints` come from the edge
    /// overlay map, which gathered them when it enumerated the candidates — a
    /// relationship must be yielded with its endpoints, and an edge id alone cannot be
    /// resolved to them.
    fn fulltext_doc_entry(
        &self,
        entity: u64,
        edges: &HashMap<u64, (u64, u64, u32)>,
    ) -> Result<DocEntry> {
        Ok(DocEntry {
            entity,
            len: 0,
            endpoints: edges.get(&entity).copied(),
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
        edges: &HashMap<u64, (u64, u64, u32)>,
    ) -> Result<Vec<(u64, f64)>> {
        let mut out = Vec::new();
        if q.terms.is_empty() || overlay.is_empty() {
            return Ok(out);
        }
        // A node index selects its documents by label, an edge index by relationship
        // type. Both are one symbol-table lookup; neither being present means nothing in
        // the graph can carry it, so there is nothing to score.
        let want_symbol = if fc.relationships {
            self.gen.reltype_id(&fc.label)
        } else {
            self.gen.label_id(&fc.label)
        };
        let Some(want_symbol) = want_symbol else {
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
            if fc.relationships {
                // Suppression asks the same three questions of an overlay edge that it
                // asks of a core one, through the same record shape.
                let doc = self.fulltext_doc_entry(id, edges)?;
                if doc.endpoints.is_none() || self.fulltext_dead(&doc, true)? {
                    continue;
                }
                if doc.endpoints.map(|(_, _, rt)| rt) != Some(want_symbol) {
                    continue;
                }
            } else {
                if self.fulltext_node_dead(id)? {
                    continue;
                }
                if !self.node_label_ids(id)?.contains(&want_symbol) {
                    continue;
                }
            }
            // Analyze the entity's *current* text, field by field, exactly as the builder
            // did — same analyzer, same declaration order, so a term found here is the
            // term the core index would have stored.
            let mut tfs: HashMap<String, u32> = HashMap::new();
            let mut per_field: Vec<HashMap<String, u32>> =
                vec![HashMap::new(); desc.properties.len()];
            let mut len = 0u32;
            for (field, prop) in desc.properties.iter().enumerate() {
                let value = if fc.relationships {
                    self.edge_prop(id, prop)?
                } else {
                    self.node_prop(id, prop)?
                };
                let Val::Str(text) = value else {
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
                //
                // The entity kind has to follow the call. Asking the *node* index for an
                // edge query's statistics would not fail — it would return plausible
                // weights from the wrong corpus, and the ranking would simply be wrong
                // with nothing to show for it.
                let kind = if fc.relationships {
                    EntityKind::Edge
                } else {
                    EntityKind::Node
                };
                let df = self
                    .gen
                    .fulltext_index(kind, &fc.label)
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
        // For a relationship call the candidate set comes with its endpoints attached;
        // for a node call there are none to attach.
        let edges = if fc.relationships {
            self.fulltext_edge_overlay()?
        } else {
            HashMap::new()
        };
        let overlay: HashSet<u64> = if fc.relationships {
            edges.keys().copied().collect()
        } else {
            self.fulltext_overlay_ids()?
        };

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
                    if overlay.contains(&entity) || self.fulltext_dead(&doc, fc.relationships)? {
                        continue;
                    }
                    scored.push((entity, hit.score, Some(doc)));
                }
            }

            // ── the overlay arm ──
            // An overlay document is in no `.docs.blk`, so it brings no record; the
            // projection synthesises one.
            scored.extend(
                self.fulltext_overlay_hits(fc, desc, &analyzer, &q, &overlay, &edges)?
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
                    None => self.fulltext_doc_entry(entity, &edges)?,
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
