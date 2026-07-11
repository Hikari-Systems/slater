// SPDX-License-Identifier: Apache-2.0
//! A core segment's **posting fragments** — the additive per-reltype endpoint driving
//! sets a flush carries (the segmented-core track; see `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! The base generation's [`crate::postings`] precompute, per reltype, the ascending
//! distinct source / target node ids that have an out / in edge of that type — the
//! ~8%-of-nodes driving set an unanchored typed scan `(a)-[:T]->(b)` starts from. A
//! flush that adds edges of reltype `T` carries a **posting fragment**: the ascending
//! distinct **born** source and target ids for each reltype it touched. At read time
//! (Phase 3) a typed scan unions the base driving set with every segment's fragment.
//!
//! # Removals are not tracked here — and needn't be
//! A posting is a *driving set*, not a correctness source: the scan still probes each
//! candidate's adjacency and keeps only real `T` edges. So the union may over-include (a
//! node whose only `T` edge a segment *removed* still appears in the base posting) without
//! changing any answer — it just makes the driving set slightly less selective. Fragments
//! therefore carry only born endpoints; edge removal is honoured by the adjacency fold, not
//! a posting sidecar.
//!
//! # `post.meta`
//! ```text
//! MAGIC(8) ‖ crc32c(body)(4) ‖ body
//! body = version:uvarint ‖ count:uvarint ‖ count × ( reltype:str ‖ src_ids ‖ tgt_ids )
//! ```
//! Each id list is the delta-varint endpoint posting encoding
//! ([`encode_endpoint_posting`]). Ids are held resident (a flush's born endpoint set is
//! delta-sized); if a flush ever grew large enough to matter, these move to block files
//! like the base `.post`. Absent `post.meta` ⇒ a segment that added no edges;
//! [`SegmentPostingsReader::open_if_present`] returns `None`.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::postings::{decode_endpoint_posting, encode_endpoint_posting};
use crate::wire::{read_uvarint, write_uvarint};

/// Magic at the head of `post.meta`.
const POST_MAGIC: &[u8; 8] = b"SLSEGPO1";
/// Posting-fragment format version.
const POST_VERSION: u64 = 1;

/// One reltype's posting fragment: the ascending, distinct born source and target ids.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostingSpec {
    pub reltype: String,
    /// Ascending, de-duplicated born **source** node ids with an out-edge of this reltype.
    pub src_ids: Vec<u64>,
    /// Ascending, de-duplicated born **target** node ids with an in-edge of this reltype.
    pub tgt_ids: Vec<u64>,
}

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> Result<String> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("segpostings: short string");
    }
    let s = std::str::from_utf8(&r[..n])
        .context("segpostings: invalid utf8")?
        .to_string();
    *r = &r[n..];
    Ok(s)
}

/// Write a delta-varint id list prefixed by its byte length, so the reader can slice it
/// out and hand it straight to [`decode_endpoint_posting`].
fn w_posting(buf: &mut Vec<u8>, ids: &[u64]) {
    let enc = encode_endpoint_posting(ids);
    write_uvarint(buf, enc.len() as u64);
    buf.extend_from_slice(&enc);
}

fn r_posting(r: &mut &[u8]) -> Result<Vec<u64>> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("segpostings: short posting");
    }
    let ids = decode_endpoint_posting(&r[..n])?;
    *r = &r[n..];
    Ok(ids)
}

fn ascending_distinct(ids: &[u64]) -> bool {
    ids.windows(2).all(|w| w[0] < w[1])
}

/// Write a segment's posting fragments into `dir` (its segment directory) as the resident
/// `post.meta`. Each spec's `src_ids`/`tgt_ids` must be ascending and de-duplicated.
pub fn write_posting_fragments(dir: impl AsRef<Path>, specs: &[PostingSpec]) -> Result<()> {
    let mut body = Vec::new();
    write_uvarint(&mut body, POST_VERSION);
    write_uvarint(&mut body, specs.len() as u64);
    for spec in specs {
        if !ascending_distinct(&spec.src_ids) || !ascending_distinct(&spec.tgt_ids) {
            bail!(
                "segpostings endpoint ids for {:?} must be ascending and de-duplicated",
                spec.reltype
            );
        }
        w_str(&mut body, &spec.reltype);
        w_posting(&mut body, &spec.src_ids);
        w_posting(&mut body, &spec.tgt_ids);
    }
    let crc = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(POST_MAGIC);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    std::fs::write(dir.as_ref().join("post.meta"), &out).context("write post.meta")?;
    Ok(())
}

/// The opened posting fragments of one core segment (all resident).
#[derive(Debug)]
pub struct SegmentPostingsReader {
    specs: Vec<PostingSpec>,
}

impl SegmentPostingsReader {
    /// Open the posting fragments in segment directory `dir`. Errors if `post.meta` is
    /// absent — use [`open_if_present`](SegmentPostingsReader::open_if_present) otherwise.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_if_present(dir)?.ok_or_else(|| anyhow::anyhow!("segment has no post.meta"))
    }

    /// Open the posting fragments if the segment carries any; `None` if `post.meta` absent.
    pub fn open_if_present(dir: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = dir.as_ref().join("post.meta");
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {path:?}")),
        };
        if bytes.len() < 12 || &bytes[..8] != POST_MAGIC {
            bail!("segment post.meta {path:?} has bad magic");
        }
        let crc = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let body = &bytes[12..];
        if crc32c::crc32c(body) != crc {
            bail!("segment post.meta {path:?} failed checksum");
        }
        let mut r = body;
        let version = read_uvarint(&mut r)?;
        if version != POST_VERSION {
            bail!(
                "unsupported segpostings version {version} (this build understands {POST_VERSION})"
            );
        }
        let count = read_uvarint(&mut r)? as usize;
        let mut specs = Vec::with_capacity(count);
        for _ in 0..count {
            let reltype = r_str(&mut r)?;
            let src_ids = r_posting(&mut r)?;
            let tgt_ids = r_posting(&mut r)?;
            specs.push(PostingSpec {
                reltype,
                src_ids,
                tgt_ids,
            });
        }
        if !r.is_empty() {
            bail!("segment post.meta {path:?} has {} trailing bytes", r.len());
        }
        Ok(Some(Self { specs }))
    }

    fn find(&self, reltype: &str) -> Option<&PostingSpec> {
        self.specs.iter().find(|s| s.reltype == reltype)
    }

    /// Reltypes this segment carries a posting fragment for.
    pub fn reltypes(&self) -> Vec<&str> {
        self.specs.iter().map(|s| s.reltype.as_str()).collect()
    }

    /// Ascending born source ids with an out-edge of `reltype` in this segment; empty if
    /// none.
    pub fn src_ids(&self, reltype: &str) -> &[u64] {
        self.find(reltype).map_or(&[], |s| &s.src_ids)
    }

    /// Ascending born target ids with an in-edge of `reltype` in this segment; empty if
    /// none.
    pub fn tgt_ids(&self, reltype: &str) -> &[u64] {
        self.find(reltype).map_or(&[], |s| &s.tgt_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_segpo_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn specs() -> Vec<PostingSpec> {
        vec![
            PostingSpec {
                reltype: "KNOWS".into(),
                src_ids: vec![5, 10, 100],
                tgt_ids: vec![12, 15],
            },
            PostingSpec {
                reltype: "IN".into(),
                src_ids: vec![10],
                tgt_ids: vec![], // added an out-edge but no distinct new target
            },
        ]
    }

    #[test]
    fn round_trip() {
        let dir = tmp("rt");
        write_posting_fragments(&dir, &specs()).unwrap();
        let r = SegmentPostingsReader::open(&dir).unwrap();
        assert_eq!(r.src_ids("KNOWS"), &[5, 10, 100]);
        assert_eq!(r.tgt_ids("KNOWS"), &[12, 15]);
        assert_eq!(r.src_ids("IN"), &[10]);
        assert_eq!(r.tgt_ids("IN"), &[] as &[u64]);
        // Absent reltype: empty, never an error.
        assert!(r.src_ids("ABSENT").is_empty());
        assert!(r.tgt_ids("ABSENT").is_empty());
        let mut rts = r.reltypes();
        rts.sort();
        assert_eq!(rts, vec!["IN", "KNOWS"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn absent_meta_is_none() {
        let dir = tmp("absent");
        assert!(SegmentPostingsReader::open_if_present(&dir)
            .unwrap()
            .is_none());
        assert!(SegmentPostingsReader::open(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_specs_round_trip() {
        let dir = tmp("empty");
        write_posting_fragments(&dir, &[]).unwrap();
        let r = SegmentPostingsReader::open(&dir).unwrap();
        assert!(r.reltypes().is_empty());
        assert!(r.src_ids("x").is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rejects_unsorted_ids() {
        let dir = tmp("bad");
        let bad = vec![PostingSpec {
            reltype: "T".into(),
            src_ids: vec![10, 5], // descending
            tgt_ids: vec![],
        }];
        let err = write_posting_fragments(&dir, &bad).unwrap_err();
        assert!(format!("{err:#}").contains("ascending"), "{err:#}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let dir = tmp("corrupt");
        write_posting_fragments(&dir, &specs()).unwrap();
        let mut meta = std::fs::read(dir.join("post.meta")).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0xff;
        std::fs::write(dir.join("post.meta"), &meta).unwrap();
        assert!(SegmentPostingsReader::open(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
