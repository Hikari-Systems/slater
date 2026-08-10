// SPDX-License-Identifier: Apache-2.0
//! The on-disk full-text index: four files under `fulltext/`, and the reader over them.
//!
//! Everything goes through [`BlockFileWriter`] and [`write_isam_sorted`], so per-block
//! AEAD, BLAKE3, zstd and the S3/GCS backends are inherited rather than reimplemented —
//! the same reason the vector store and the range indexes are built on them.
//!
//! # The four files
//!
//! | file | record index is | holds |
//! |---|---|---|
//! | `<stem>.ftd` | — (an ISAM multi-map) | `term → term-meta record index`, one entry per *field* the term occurs in |
//! | `<stem>.ftm.blk` | a term-meta index | field, document frequencies, where the postings start, the skip list |
//! | `<stem>.post.blk` | a chunk index | one fixed-size run of `(docid, tf)` |
//! | `<stem>.docs.blk` | a **docid** | the entity the doc is, its length, and for an edge its endpoints |
//!
//! ## Why postings are chunked rather than one record per term
//!
//! [`BlockFileWriter::append_record`] closes a block once the accumulated data reaches
//! the target — so a record *larger* than the target becomes a block of its own. One
//! record per term would therefore give a common term a single enormous block, and
//! touching that term would fault the whole thing into the block cache. That is precisely
//! the bounded-memory property this engine exists to keep, so postings are split into
//! fixed [`CHUNK_DOCS`]-document chunks and a term's chunks are contiguous
//! (`first_chunk .. first_chunk + chunk_count`).
//!
//! ## Docids are dense, and assigned in ascending entity order
//!
//! A docid is the entity's *rank* among the entities this index covers, not its node or
//! edge id. Two things follow, and both are load-bearing: posting lists are ascending and
//! delta-encodable with small gaps, and `.docs.blk` needs one record per indexed entity
//! rather than one per entity in the graph. A label matching 1% of the graph pays 1%.
//!
//! ## Term meta carries two document frequencies
//!
//! `field_df` is the count for this `(term, field)`; `doc_df` is the count of documents
//! containing the term in **any** field, and is therefore identical across all of a
//! term's records.
//!
//! `doc_df` exists because scoring is whole-document while the index is per-field.
//! Without it, idf for a free term could only be had by unioning the term's field lists
//! *before* scoring — which would mean materialising a common term's whole posting list
//! to compute one number, when the scorer otherwise only ever needs to stream it. Summing
//! `field_df` is not a substitute: it double-counts a document that has the term in two
//! fields, which for `name`/`summary` is the common case rather than the corner.
//!
//! ## Reserved: `max_impact` and the skip list
//!
//! Each chunk gets a quantised `max_impact` byte and the skip list records each chunk's
//! last docid. Nothing reads them yet — block-max WAND is on the cut list. They are
//! *written* now because the format reserving the space is exactly what makes enabling
//! skipping later a change to the reader alone, with no format bump and no rebuild.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::FileCipher;
use crate::ids::Value;
use crate::isam::{write_isam_sorted, IsamReader};
use crate::wire::{read_uvarint, write_uvarint};

/// Documents per posting chunk. Fixed, so a chunk index is a chunk index — the reader
/// never needs a per-term chunk size, and a term's `n`th chunk is `first_chunk + n`.
///
/// 128 is the usual trade: large enough that the per-chunk uvarint header and the
/// skip-list entry amortise, small enough that a chunk stays far below any sane block
/// target so the "one giant record" failure above cannot come back through the side door.
pub const CHUNK_DOCS: usize = 128;

/// The four file suffixes, appended to an index's stem.
pub const SUFFIXES: [&str; 4] = [".ftd", ".ftm.blk", ".post.blk", ".docs.blk"];

/// One document as the writer takes it. `len` is the surviving term count across every
/// indexed field — BM25's document length, and what `avgDocLen` averages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocEntry {
    /// Dense node id, or dense edge id for a relationship index.
    pub entity: u64,
    pub len: u32,
    /// `(start, end, reltype)` for a relationship index; `None` for a node index.
    ///
    /// Stored rather than looked up because a hit has to be yielded as a bound
    /// relationship, which carries its endpoints and type — resolving them per hit would
    /// turn every edge search into an adjacency read per result.
    pub endpoints: Option<(u64, u64, u32)>,
}

/// One posting, as the sorted stream delivers it. The stream must be ascending by
/// `(term, field, doc)`; [`write_fulltext_index`] checks and refuses otherwise, because
/// an out-of-order stream would silently produce a corrupt-but-readable index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub term: String,
    pub field: u32,
    pub doc: u64,
    pub tf: u32,
}

/// What the writer learnt, for the manifest descriptor.
#[derive(Debug, Clone, PartialEq)]
pub struct FulltextBuildStats {
    pub doc_count: u64,
    pub avg_doc_len: f32,
    /// Store-relative file names written, in [`SUFFIXES`] order — the inventory entries
    /// and `block_sizes` keys the caller must record.
    pub files: Vec<String>,
}

/// A term's entry for one field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermMeta {
    pub field: u32,
    /// Documents containing the term **in this field**.
    pub field_df: u64,
    /// Documents containing the term in **any** field — the idf input (see module docs).
    pub doc_df: u64,
    pub first_chunk: u64,
    pub chunk_count: u64,
    /// Last docid of each chunk, ascending. Reserved for skipping; see module docs.
    pub skips: Vec<u64>,
    /// Quantised per-chunk impact ceiling. Reserved for block-max WAND.
    pub max_impacts: Vec<u8>,
}

/// One decoded posting chunk: parallel docids (ascending) and term frequencies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Chunk {
    pub docs: Vec<u64>,
    pub tfs: Vec<u32>,
}

// ── writing ──────────────────────────────────────────────────────────────────────

/// Write a full-text index. `docs` must be in **docid order** (record index *is* the
/// docid) and `postings` ascending by `(term, field, doc)`.
///
/// `cipher_for` is handed each file's store-relative name so the caller binds the
/// generation cipher exactly as it does for every other file (HIK-140 requires the name
/// to be byte-identical on the writing and reading sides — see [`open`]).
pub fn write_fulltext_index<D, P>(
    dir: &Path,
    rel_stem: &str,
    docs: D,
    postings: P,
    block_bytes: usize,
    zstd_level: i32,
    cipher_for: &dyn Fn(&str) -> Option<Arc<FileCipher>>,
) -> Result<FulltextBuildStats>
where
    D: IntoIterator<Item = Result<DocEntry>>,
    P: IntoIterator<Item = Result<Posting>>,
{
    let names: Vec<String> = SUFFIXES.iter().map(|s| format!("{rel_stem}{s}")).collect();
    if let Some(parent) = dir.join(&names[0]).parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }

    // ── .docs.blk — record index is the docid ──
    let mut doc_w = BlockFileWriter::create_with_cipher(
        dir.join(&names[3]),
        block_bytes,
        zstd_level,
        cipher_for(&names[3]),
    )?;
    let mut doc_count = 0u64;
    let mut total_len = 0u128;
    let mut rec = Vec::new();
    for d in docs {
        let d = d?;
        rec.clear();
        write_uvarint(&mut rec, d.entity);
        write_uvarint(&mut rec, d.len as u64);
        if let Some((s, e, t)) = d.endpoints {
            write_uvarint(&mut rec, s);
            write_uvarint(&mut rec, e);
            write_uvarint(&mut rec, t as u64);
        }
        doc_w.append_record(&rec)?;
        doc_count += 1;
        total_len += d.len as u128;
    }
    doc_w.finish()?;

    // ── .post.blk + .ftm.blk, driven by one pass over the sorted postings ──
    //
    // The two are written together because a term-meta record cannot be finalised until
    // its last chunk is known: `chunk_count` and the skip list are only complete at the
    // end of the term's run.
    let mut post_w = BlockFileWriter::create_with_cipher(
        dir.join(&names[2]),
        block_bytes,
        zstd_level,
        cipher_for(&names[2]),
    )?;
    let mut meta_w = BlockFileWriter::create_with_cipher(
        dir.join(&names[1]),
        block_bytes,
        zstd_level,
        cipher_for(&names[1]),
    )?;

    // `(term, meta_idx)` pairs for the dictionary. Ascending by construction: the
    // postings arrive sorted by term, and meta indices are handed out in that order.
    let mut dict: Vec<(String, u64)> = Vec::new();
    // Per-term run state, flushed when the (term, field) group changes.
    let mut run = RunState::default();
    let mut next_chunk = 0u64;
    let mut next_meta = 0u64;
    // Every meta record of a term needs `doc_df`, which is not known until the term's
    // last field is seen — so a term's records are buffered and patched at the term
    // boundary. Bounded by the number of fields, not by the term's document count.
    let mut term_records: Vec<PendingMeta> = Vec::new();
    let mut term_docs: Vec<u64> = Vec::new();
    let mut prev: Option<(String, u32, u64)> = None;

    for p in postings {
        let p = p?;
        if let Some((pt, pf, pd)) = &prev {
            let order = (pt.as_str(), *pf, *pd).cmp(&(p.term.as_str(), p.field, p.doc));
            if order != std::cmp::Ordering::Less {
                bail!(
                    "full-text postings are not strictly ascending by (term, field, doc): \
                     ({pt:?}, {pf}, {pd}) then ({:?}, {}, {})",
                    p.term,
                    p.field,
                    p.doc
                );
            }
        }
        let new_term = prev.as_ref().is_none_or(|(t, _, _)| *t != p.term);
        let new_group = new_term || prev.as_ref().is_some_and(|(_, f, _)| *f != p.field);

        if new_group && !run.is_empty() {
            term_records.push(run.flush(&mut post_w, &mut next_chunk)?);
        }
        if new_term {
            if !term_records.is_empty() {
                flush_term(
                    &mut meta_w,
                    &mut dict,
                    &mut next_meta,
                    prev.as_ref().map(|(t, _, _)| t.clone()).unwrap_or_default(),
                    &mut term_records,
                    &mut term_docs,
                )?;
            }
            term_docs.clear();
        }
        if new_group {
            run.begin(p.field);
        }
        run.push(p.doc, p.tf);
        term_docs.push(p.doc);
        prev = Some((p.term.clone(), p.field, p.doc));
    }
    if !run.is_empty() {
        term_records.push(run.flush(&mut post_w, &mut next_chunk)?);
    }
    if !term_records.is_empty() {
        flush_term(
            &mut meta_w,
            &mut dict,
            &mut next_meta,
            prev.map(|(t, _, _)| t).unwrap_or_default(),
            &mut term_records,
            &mut term_docs,
        )?;
    }
    post_w.finish()?;
    meta_w.finish()?;

    // ── .ftd — the term dictionary ──
    write_isam_sorted(
        dir.join(&names[0]),
        dict.into_iter().map(|(t, i)| Ok((Value::Str(t), i))),
        block_bytes,
        zstd_level,
        cipher_for(&names[0]),
    )?;

    Ok(FulltextBuildStats {
        doc_count,
        avg_doc_len: if doc_count == 0 {
            0.0
        } else {
            (total_len as f64 / doc_count as f64) as f32
        },
        files: names,
    })
}

/// A term-meta record whose `doc_df` is not yet known.
struct PendingMeta {
    field: u32,
    field_df: u64,
    first_chunk: u64,
    chunk_count: u64,
    skips: Vec<u64>,
    max_impacts: Vec<u8>,
}

/// Accumulates one `(term, field)` run into [`CHUNK_DOCS`]-sized chunks.
#[derive(Default)]
struct RunState {
    field: u32,
    first_chunk: u64,
    chunk_count: u64,
    df: u64,
    docs: Vec<u64>,
    tfs: Vec<u32>,
    skips: Vec<u64>,
    max_impacts: Vec<u8>,
    started: bool,
}

impl RunState {
    fn is_empty(&self) -> bool {
        !self.started
    }

    fn begin(&mut self, field: u32) {
        self.field = field;
        self.first_chunk = u64::MAX; // set on the first chunk emitted
        self.chunk_count = 0;
        self.df = 0;
        self.docs.clear();
        self.tfs.clear();
        self.skips.clear();
        self.max_impacts.clear();
        self.started = true;
    }

    fn push(&mut self, doc: u64, tf: u32) {
        self.docs.push(doc);
        self.tfs.push(tf);
        self.df += 1;
    }

    /// Emit the buffered docs as chunks and return the term-meta shape.
    fn flush(&mut self, w: &mut BlockFileWriter, next_chunk: &mut u64) -> Result<PendingMeta> {
        let mut rec = Vec::new();
        for (i, chunk_docs) in self.docs.chunks(CHUNK_DOCS).enumerate() {
            let chunk_tfs = &self.tfs[i * CHUNK_DOCS..i * CHUNK_DOCS + chunk_docs.len()];
            rec.clear();
            write_uvarint(&mut rec, chunk_docs.len() as u64);
            let mut prev = 0u64;
            for (n, d) in chunk_docs.iter().enumerate() {
                // First docid absolute, the rest gaps. Ascending is guaranteed by the
                // stream check in `write_fulltext_index`.
                write_uvarint(&mut rec, if n == 0 { *d } else { *d - prev });
                prev = *d;
            }
            for tf in chunk_tfs {
                write_uvarint(&mut rec, *tf as u64);
            }
            w.append_record(&rec)?;
            if self.first_chunk == u64::MAX {
                self.first_chunk = *next_chunk;
            }
            *next_chunk += 1;
            self.chunk_count += 1;
            self.skips
                .push(*chunk_docs.last().expect("chunks are non-empty"));
            self.max_impacts.push(quantise_impact(
                chunk_tfs.iter().copied().max().unwrap_or(0),
            ));
        }
        self.started = false;
        Ok(PendingMeta {
            field: self.field,
            field_df: self.df,
            first_chunk: if self.first_chunk == u64::MAX {
                0
            } else {
                self.first_chunk
            },
            chunk_count: self.chunk_count,
            skips: std::mem::take(&mut self.skips),
            max_impacts: std::mem::take(&mut self.max_impacts),
        })
    }
}

/// Write every buffered record of one term, now that `doc_df` is known, and add the
/// term's dictionary entries.
fn flush_term(
    meta_w: &mut BlockFileWriter,
    dict: &mut Vec<(String, u64)>,
    next_meta: &mut u64,
    term: String,
    records: &mut Vec<PendingMeta>,
    term_docs: &mut Vec<u64>,
) -> Result<()> {
    // Distinct documents across every field of this term. The per-field runs are each
    // ascending but interleave across fields, so sort before deduping.
    term_docs.sort_unstable();
    term_docs.dedup();
    let doc_df = term_docs.len() as u64;

    let mut rec = Vec::new();
    for m in records.drain(..) {
        rec.clear();
        write_uvarint(&mut rec, m.field as u64);
        write_uvarint(&mut rec, m.field_df);
        write_uvarint(&mut rec, doc_df);
        write_uvarint(&mut rec, m.first_chunk);
        write_uvarint(&mut rec, m.chunk_count);
        let mut prev = 0u64;
        for (n, s) in m.skips.iter().enumerate() {
            write_uvarint(&mut rec, if n == 0 { *s } else { *s - prev });
            prev = *s;
        }
        rec.extend_from_slice(&m.max_impacts);
        meta_w.append_record(&rec)?;
        dict.push((term.clone(), *next_meta));
        *next_meta += 1;
    }
    Ok(())
}

/// Quantise a chunk's peak term frequency into one byte. Saturating: the ceiling only
/// ever needs to *bound* the impact, and every tf at or above 255 bounds identically.
fn quantise_impact(max_tf: u32) -> u8 {
    max_tf.min(u8::MAX as u32) as u8
}

// ── reading ──────────────────────────────────────────────────────────────────────

/// A full-text index opened for reading.
pub struct FulltextReader {
    dict: IsamReader,
    meta: BlockFileReader,
    post: BlockFileReader,
    docs: BlockFileReader,
    /// True for a relationship index — decides whether a doc record carries endpoints.
    /// Taken from the manifest descriptor, which is the authority for the layout, exactly
    /// as the vector reader takes `dim` and `metric` from its descriptor rather than
    /// re-deriving them from the bytes.
    edges: bool,
}

impl FulltextReader {
    /// Open the four files of the index with stem `rel_stem` under `dir`.
    ///
    /// `cipher_for` must produce the identical store-relative names
    /// [`write_fulltext_index`] was given, or an encrypted index fails to open — the AAD
    /// binds each block to its file name (HIK-140).
    pub fn open(
        dir: &Path,
        rel_stem: &str,
        edges: bool,
        cipher_for: &dyn Fn(&str) -> Option<Arc<FileCipher>>,
    ) -> Result<Self> {
        let n: Vec<String> = SUFFIXES.iter().map(|s| format!("{rel_stem}{s}")).collect();
        Ok(Self {
            dict: IsamReader::open_with_cipher(dir.join(&n[0]), cipher_for(&n[0]))?,
            meta: BlockFileReader::open_with_cipher(dir.join(&n[1]), cipher_for(&n[1]))?,
            post: BlockFileReader::open_with_cipher(dir.join(&n[2]), cipher_for(&n[2]))?,
            docs: BlockFileReader::open_with_cipher(dir.join(&n[3]), cipher_for(&n[3]))?,
            edges,
        })
    }

    /// Indexed documents — BM25's `N`, and the bound on a valid docid.
    pub fn doc_count(&self) -> u64 {
        self.docs.total_records()
    }

    /// Every field entry for `term`, in field order. Empty when the term is absent, which
    /// is an ordinary answer and not an error.
    ///
    /// `term` must already be analyzed (lowercased, and not a stopword) — see
    /// [`Analyzer`](super::Analyzer). Passing raw user text here is the mistake that
    /// makes a term silently unfindable.
    pub fn term_metas(&self, term: &str) -> Result<Vec<TermMeta>> {
        let idxs = self.dict.lookup_eq(&Value::Str(term.to_string()))?;
        let mut out = Vec::with_capacity(idxs.len());
        for i in idxs {
            out.push(self.term_meta(i)?);
        }
        Ok(out)
    }

    fn term_meta(&self, idx: u64) -> Result<TermMeta> {
        let bytes = self.meta.read_record_global(idx)?;
        let r = &mut bytes.as_slice();
        let field = read_uvarint(r)? as u32;
        let field_df = read_uvarint(r)?;
        let doc_df = read_uvarint(r)?;
        let first_chunk = read_uvarint(r)?;
        let chunk_count = read_uvarint(r)?;
        let mut skips = Vec::with_capacity(chunk_count as usize);
        let mut prev = 0u64;
        for n in 0..chunk_count {
            let d = read_uvarint(r)?;
            prev = if n == 0 { d } else { prev + d };
            skips.push(prev);
        }
        if r.len() < chunk_count as usize {
            bail!(
                "full-text term meta {idx} is truncated: want {chunk_count} impact bytes, \
                 have {}",
                r.len()
            );
        }
        let max_impacts = r[..chunk_count as usize].to_vec();
        Ok(TermMeta {
            field,
            field_df,
            doc_df,
            first_chunk,
            chunk_count,
            skips,
            max_impacts,
        })
    }

    /// Decode the `n`th chunk of a term's postings (`0 <= n < meta.chunk_count`).
    pub fn chunk(&self, meta: &TermMeta, n: u64) -> Result<Chunk> {
        if n >= meta.chunk_count {
            bail!(
                "chunk {n} out of range for a term with {} chunk(s)",
                meta.chunk_count
            );
        }
        let bytes = self.post.read_record_global(meta.first_chunk + n)?;
        let r = &mut bytes.as_slice();
        let count = read_uvarint(r)? as usize;
        let mut docs = Vec::with_capacity(count);
        let mut prev = 0u64;
        for i in 0..count {
            let d = read_uvarint(r)?;
            prev = if i == 0 { d } else { prev + d };
            docs.push(prev);
        }
        let mut tfs = Vec::with_capacity(count);
        for _ in 0..count {
            tfs.push(read_uvarint(r)? as u32);
        }
        Ok(Chunk { docs, tfs })
    }

    /// The document at `docid`.
    pub fn doc(&self, docid: u64) -> Result<DocEntry> {
        let bytes = self.docs.read_record_global(docid)?;
        let r = &mut bytes.as_slice();
        let entity = read_uvarint(r)?;
        let len = read_uvarint(r)? as u32;
        let endpoints = if self.edges {
            let s = read_uvarint(r)?;
            let e = read_uvarint(r)?;
            let t = read_uvarint(r)? as u32;
            Some((s, e, t))
        } else {
            None
        };
        Ok(DocEntry {
            entity,
            len,
            endpoints,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::Analyzer;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "slater-fulltext-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn no_cipher(_: &str) -> Option<Arc<FileCipher>> {
        None
    }

    /// Index a handful of small documents the way the builder will: analyze each field,
    /// count term frequencies, and hand the writer a sorted posting stream.
    fn build(dir: &Path, stem: &str, docs: &[(u64, Vec<&str>)], edges: bool) -> FulltextBuildStats {
        let a = Analyzer::new(&["the".to_string(), "a".to_string()]);
        let mut entries = Vec::new();
        let mut postings: Vec<Posting> = Vec::new();
        for (docid, (entity, fields)) in docs.iter().enumerate() {
            let mut len = 0u32;
            for (field, text) in fields.iter().enumerate() {
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
                entity: *entity,
                len,
                endpoints: edges.then_some((*entity * 2, *entity * 2 + 1, 7)),
            });
        }
        postings.sort_by(|x, y| {
            (x.term.as_str(), x.field, x.doc).cmp(&(y.term.as_str(), y.field, y.doc))
        });
        write_fulltext_index(
            dir,
            stem,
            entries.into_iter().map(Ok),
            postings.into_iter().map(Ok),
            4096,
            3,
            &no_cipher,
        )
        .unwrap()
    }

    /// Collect every `(docid, tf)` of a term across all its fields.
    fn all_postings(r: &FulltextReader, term: &str) -> Vec<(u32, u64, u32)> {
        let mut out = Vec::new();
        for m in r.term_metas(term).unwrap() {
            for n in 0..m.chunk_count {
                let c = r.chunk(&m, n).unwrap();
                for (d, tf) in c.docs.iter().zip(&c.tfs) {
                    out.push((m.field, *d, *tf));
                }
            }
        }
        out
    }

    #[test]
    fn round_trips_terms_documents_and_frequencies() {
        let dir = tmp("roundtrip");
        let stats = build(
            &dir,
            "node_Doc",
            &[
                (10, vec!["Alice Smith", "Alice is an engineer"]),
                (20, vec!["Bob Jones", "a baker"]),
                (30, vec!["Carol", "Carol knows Alice"]),
            ],
            false,
        );
        assert_eq!(stats.doc_count, 3);
        assert_eq!(
            stats.files,
            [
                "node_Doc.ftd",
                "node_Doc.ftm.blk",
                "node_Doc.post.blk",
                "node_Doc.docs.blk"
            ]
        );

        let r = FulltextReader::open(&dir, "node_Doc", false, &no_cipher).unwrap();
        assert_eq!(r.doc_count(), 3);

        // "alice" is in field 0 of doc 0, and field 1 of docs 0 and 2.
        let mut alice = all_postings(&r, "alice");
        alice.sort();
        assert_eq!(alice, [(0, 0, 1), (1, 0, 1), (1, 2, 1)]);

        // Absent terms are an ordinary empty answer, not an error.
        assert!(r.term_metas("nobody").unwrap().is_empty());
        // Stopwords never made it in.
        assert!(r.term_metas("the").unwrap().is_empty());

        // Documents map back to their entities and lengths.
        assert_eq!(
            r.doc(0).unwrap(),
            DocEntry {
                entity: 10,
                // "alice smith" + "alice is an engineer": this fixture's stopword list is
                // only ["the", "a"], so "is" and "an" survive and the length is 6.
                len: 6,
                endpoints: None
            }
        );
        assert_eq!(r.doc(1).unwrap().entity, 20);
        assert_eq!(r.doc(2).unwrap().entity, 30);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `doc_df` counts documents, not `(document, field)` pairs — the distinction that
    /// makes it worth storing at all. "alice" is in two documents but three postings.
    #[test]
    fn doc_df_counts_documents_while_field_df_counts_field_occurrences() {
        let dir = tmp("dfs");
        build(
            &dir,
            "node_Doc",
            &[
                (10, vec!["Alice", "Alice again"]),
                (20, vec!["Bob", "Bob again"]),
                (30, vec!["Carol", "mentions Alice"]),
            ],
            false,
        );
        let r = FulltextReader::open(&dir, "node_Doc", false, &no_cipher).unwrap();
        let metas = r.term_metas("alice").unwrap();
        assert_eq!(metas.len(), 2, "one record per field the term occurs in");
        for m in &metas {
            assert_eq!(
                m.doc_df, 2,
                "documents 0 and 2 contain 'alice'; summing field_df would say 3"
            );
        }
        assert_eq!(metas.iter().map(|m| m.field_df).sum::<u64>(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A term spanning several chunks: the chunks are contiguous, the docids reassemble
    /// in order across the boundaries, and the skip list names each chunk's last docid.
    #[test]
    fn a_long_posting_list_spans_contiguous_chunks() {
        let dir = tmp("chunks");
        let n = CHUNK_DOCS * 2 + 5;
        let docs: Vec<(u64, Vec<&str>)> = (0..n).map(|i| (i as u64 * 3, vec!["common"])).collect();
        build(&dir, "node_Doc", &docs, false);

        let r = FulltextReader::open(&dir, "node_Doc", false, &no_cipher).unwrap();
        let m = &r.term_metas("common").unwrap()[0];
        assert_eq!(m.chunk_count, 3, "{n} docs at {CHUNK_DOCS} per chunk");
        assert_eq!(m.field_df, n as u64);
        assert_eq!(m.skips.len(), 3);
        assert_eq!(m.max_impacts.len(), 3);
        assert_eq!(
            m.skips,
            [
                CHUNK_DOCS as u64 - 1,
                CHUNK_DOCS as u64 * 2 - 1,
                n as u64 - 1
            ],
            "the skip list names each chunk's last docid"
        );

        let mut seen = Vec::new();
        for c in 0..m.chunk_count {
            seen.extend(r.chunk(m, c).unwrap().docs);
        }
        assert_eq!(seen, (0..n as u64).collect::<Vec<_>>());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A relationship index stores its endpoints, because a hit is yielded as a bound
    /// relationship and resolving them per hit would cost an adjacency read per result.
    #[test]
    fn a_relationship_index_round_trips_its_endpoints() {
        let dir = tmp("edges");
        build(
            &dir,
            "edge_RELATES_TO",
            &[(5, vec!["Alice knows Bob"])],
            true,
        );
        let r = FulltextReader::open(&dir, "edge_RELATES_TO", true, &no_cipher).unwrap();
        assert_eq!(
            r.doc(0).unwrap(),
            DocEntry {
                entity: 5,
                len: 3,
                endpoints: Some((10, 11, 7))
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An out-of-order posting stream would produce an index that reads back without
    /// error and returns the wrong documents. Refuse it at the door instead.
    #[test]
    fn refuses_an_unsorted_posting_stream() {
        let dir = tmp("unsorted");
        let docs = vec![Ok(DocEntry {
            entity: 0,
            len: 2,
            endpoints: None,
        })];
        let postings = vec![
            Ok(Posting {
                term: "b".into(),
                field: 0,
                doc: 0,
                tf: 1,
            }),
            Ok(Posting {
                term: "a".into(),
                field: 0,
                doc: 0,
                tf: 1,
            }),
        ];
        let err = write_fulltext_index(&dir, "x", docs, postings, 4096, 3, &no_cipher)
            .expect_err("an unsorted stream must be refused");
        assert!(
            format!("{err:#}").contains("ascending"),
            "the message should name the ordering requirement: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An index over no documents is legal — a declaration whose label matched nothing.
    /// It must open and answer, not fail.
    #[test]
    fn an_empty_index_opens_and_answers_nothing() {
        let dir = tmp("empty");
        let stats = write_fulltext_index(
            &dir,
            "node_Nothing",
            Vec::<Result<DocEntry>>::new(),
            Vec::<Result<Posting>>::new(),
            4096,
            3,
            &no_cipher,
        )
        .unwrap();
        assert_eq!(stats.doc_count, 0);
        assert_eq!(
            stats.avg_doc_len, 0.0,
            "no documents means no average to take"
        );

        let r = FulltextReader::open(&dir, "node_Nothing", false, &no_cipher).unwrap();
        assert_eq!(r.doc_count(), 0);
        assert!(r.term_metas("anything").unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
