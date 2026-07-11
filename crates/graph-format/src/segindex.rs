// SPDX-License-Identifier: Apache-2.0
//! A core segment's **index fragments** — the additive range-index material a flush
//! carries so a range/equality probe can be answered over a stacked set (the
//! segmented-core track; see `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! A segment holds, per indexed `(label, prop)`:
//!   * an **ISAM fragment** (`idx_<k>.isam`) — a normal [`crate::isam`] index over just
//!     the `(value, node_id)` pairs of the born/patched nodes this segment carries. Built
//!     with the same [`write_isam_with_cipher`](crate::isam::write_isam_with_cipher), read
//!     with the same [`IsamReader`], so equality/range lookups work unchanged; the only
//!     difference is scope — it indexes a delta, not the whole graph.
//!   * a **removal sidecar** — the sorted node ids whose *base* index entry for this
//!     `(label, prop)` must be **suppressed** (the node was deleted, or its indexed value
//!     moved, so the old `(value, id)` in the base ISAM is stale). Held resident (the
//!     suppression set is proportional to the delta, and a probe must consult it on every
//!     base hit) as a delta-varint id list in `idx.meta`.
//!
//! At read time (Phase 3) an index probe unions *base hits minus removals* with the
//! fragment hits, newest-wins across segments. This slice is the **format only** — the
//! writer, the reader, and their round-trip — and does not wire that union.
//!
//! # `idx.meta`
//! ```text
//! MAGIC(8) ‖ crc32c(body)(4) ‖ body
//! body = version:uvarint ‖ count:uvarint ‖ count × ( label:str ‖ prop:str ‖ removals:u64-list )
//! ```
//! The `k`-th descriptor owns `idx_<k>.isam`. Absent `idx.meta` ⇒ a segment with no index
//! fragments (a flush that touched no indexed property); [`SegmentIndexReader::open_if_present`]
//! returns `None`.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::crypto::BlockCipher;
use crate::ids::Value;
use crate::isam::{write_isam_with_cipher, IsamReader};
use crate::store::{join_key, ObjectStore};
use crate::wire::{read_uvarint, write_uvarint};

/// Magic at the head of `idx.meta`.
const IDX_MAGIC: &[u8; 8] = b"SLSEGIX1";
/// Index-fragment format version.
const IDX_VERSION: u64 = 1;

/// One `(label, prop)` index fragment a segment contributes: the born/patched
/// `(value, node_id)` pairs (need not be pre-sorted — the ISAM writer sorts) and the
/// sorted node ids whose base entry this fragment **removes**.
#[derive(Debug, Clone, Default)]
pub struct IndexSpec {
    pub label: String,
    pub prop: String,
    pub entries: Vec<(Value, u64)>,
    /// Sorted, de-duplicated node ids to suppress from the base index for `(label, prop)`.
    pub removals: Vec<u64>,
}

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> Result<String> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("segindex: short string");
    }
    let s = std::str::from_utf8(&r[..n])
        .context("segindex: invalid utf8")?
        .to_string();
    *r = &r[n..];
    Ok(s)
}

/// Delta-varint encode an ascending id list (matches the postings encoding).
fn w_ids(buf: &mut Vec<u8>, ids: &[u64]) {
    write_uvarint(buf, ids.len() as u64);
    let mut prev = 0u64;
    for &id in ids {
        write_uvarint(buf, id - prev);
        prev = id;
    }
}

fn r_ids(r: &mut &[u8]) -> Result<Vec<u64>> {
    let n = read_uvarint(r)? as usize;
    let mut out = Vec::with_capacity(n);
    let mut prev = 0u64;
    for _ in 0..n {
        prev += read_uvarint(r)?;
        out.push(prev);
    }
    Ok(out)
}

fn isam_name(k: usize) -> String {
    format!("idx_{k}.isam")
}

/// Write a segment's index fragments into `dir` (which must already exist — it is the
/// segment directory): one ISAM file per spec plus the resident `idx.meta`. `cipher`
/// seals the ISAM blocks and top-levels (and is mirrored by the reader). Removal lists
/// are validated to be ascending and de-duplicated so a base probe can merge-suppress.
pub fn write_index_fragments(
    dir: impl AsRef<Path>,
    specs: &[IndexSpec],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<()> {
    let dir = dir.as_ref();
    for (k, spec) in specs.iter().enumerate() {
        if !spec.removals.windows(2).all(|w| w[0] < w[1]) {
            bail!(
                "segindex removals for ({}, {}) must be ascending and de-duplicated",
                spec.label,
                spec.prop
            );
        }
        write_isam_with_cipher(
            dir.join(isam_name(k)),
            spec.entries.clone(),
            target_block_bytes,
            zstd_level,
            cipher.clone(),
        )
        .with_context(|| format!("write index fragment {k} ({}, {})", spec.label, spec.prop))?;
    }

    let mut body = Vec::new();
    write_uvarint(&mut body, IDX_VERSION);
    write_uvarint(&mut body, specs.len() as u64);
    for spec in specs {
        w_str(&mut body, &spec.label);
        w_str(&mut body, &spec.prop);
        w_ids(&mut body, &spec.removals);
    }
    let crc = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(IDX_MAGIC);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    std::fs::write(dir.join("idx.meta"), &out).context("write idx.meta")?;
    Ok(())
}

/// One opened index fragment: its `(label, prop)`, the ISAM reader, and the resident
/// removal set.
struct Fragment {
    label: String,
    prop: String,
    isam: IsamReader,
    removals: Vec<u64>,
}

/// The opened index fragments of one core segment.
/// Parse and validate an `idx.meta` body (magic ‖ crc32c ‖ uvarint body) into the per-
/// fragment `(label, prop, removals)` descriptors. The ISAM files are opened separately by
/// the caller (differently for fs vs store), but the magic/crc/version checks are shared.
fn decode_idx_meta(meta: &[u8]) -> Result<Vec<(String, String, Vec<u64>)>> {
    if meta.len() < 12 || &meta[..8] != IDX_MAGIC {
        bail!("segment idx.meta has bad magic");
    }
    let crc = u32::from_le_bytes([meta[8], meta[9], meta[10], meta[11]]);
    let body = &meta[12..];
    if crc32c::crc32c(body) != crc {
        bail!("segment idx.meta failed checksum");
    }
    let mut r = body;
    let version = read_uvarint(&mut r)?;
    if version != IDX_VERSION {
        bail!("unsupported segindex version {version} (this build understands {IDX_VERSION})");
    }
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let label = r_str(&mut r)?;
        let prop = r_str(&mut r)?;
        let removals = r_ids(&mut r)?;
        out.push((label, prop, removals));
    }
    if !r.is_empty() {
        bail!("segment idx.meta has {} trailing bytes", r.len());
    }
    Ok(out)
}

pub struct SegmentIndexReader {
    fragments: Vec<Fragment>,
}

impl std::fmt::Debug for SegmentIndexReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentIndexReader")
            .field("fragments", &self.fragments.len())
            .finish()
    }
}

impl SegmentIndexReader {
    /// Open the index fragments in segment directory `dir`. Errors if `idx.meta` is
    /// absent — use [`open_if_present`](SegmentIndexReader::open_if_present) for the
    /// optional case.
    pub fn open(dir: impl AsRef<Path>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        Self::open_if_present(dir, cipher)?
            .ok_or_else(|| anyhow::anyhow!("segment has no idx.meta"))
    }

    /// Open the index fragments if the segment carries any; `None` if `idx.meta` is absent.
    pub fn open_if_present(
        dir: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Option<Self>> {
        let dir = dir.as_ref().to_path_buf();
        let meta_path = dir.join("idx.meta");
        let meta = match std::fs::read(&meta_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {meta_path:?}")),
        };
        let mut fragments = Vec::new();
        for (k, (label, prop, removals)) in decode_idx_meta(&meta)?.into_iter().enumerate() {
            let isam = IsamReader::open_with_cipher(dir.join(isam_name(k)), cipher.clone())
                .with_context(|| format!("open index fragment {k} ({label}, {prop})"))?;
            fragments.push(Fragment {
                label,
                prop,
                isam,
                removals,
            });
        }
        Ok(Some(Self { fragments }))
    }

    /// Store-native counterpart of [`open_if_present`](SegmentIndexReader::open_if_present) —
    /// reads `<prefix>/idx.meta` and each ISAM fragment through `store`, so a segment on any
    /// backend opens like the base generation's range indexes. `None` if `idx.meta` is absent.
    pub fn open_if_present_via(
        store: &dyn ObjectStore,
        prefix: &str,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Option<Self>> {
        let meta_key = join_key(prefix, "idx.meta");
        if !store.exists(&meta_key)? {
            return Ok(None);
        }
        let meta = store
            .read_all(&meta_key)
            .with_context(|| format!("read {meta_key}"))?;
        let mut fragments = Vec::new();
        for (k, (label, prop, removals)) in decode_idx_meta(&meta)?.into_iter().enumerate() {
            let isam = IsamReader::open_src(
                store.open(&join_key(prefix, &isam_name(k)))?,
                cipher.clone(),
            )
            .with_context(|| format!("open index fragment {k} ({label}, {prop})"))?;
            fragments.push(Fragment {
                label,
                prop,
                isam,
                removals,
            });
        }
        Ok(Some(Self { fragments }))
    }

    fn find(&self, label: &str, prop: &str) -> Option<&Fragment> {
        self.fragments
            .iter()
            .find(|f| f.label == label && f.prop == prop)
    }

    /// The `(label, prop)` pairs this segment carries a fragment for.
    pub fn indexed(&self) -> Vec<(&str, &str)> {
        self.fragments
            .iter()
            .map(|f| (f.label.as_str(), f.prop.as_str()))
            .collect()
    }

    /// Born/patched node ids in this segment whose `(label, prop)` value equals `key`.
    /// Empty if the segment carries no fragment for `(label, prop)`.
    pub fn lookup_eq(&self, label: &str, prop: &str, key: &Value) -> Result<Vec<u64>> {
        match self.find(label, prop) {
            Some(fr) => fr.isam.lookup_eq(key),
            None => Ok(Vec::new()),
        }
    }

    /// Born/patched node ids in this segment whose `(label, prop)` value is in the range.
    pub fn lookup_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Result<Vec<u64>> {
        match self.find(label, prop) {
            Some(fr) => fr.isam.lookup_range(lo, lo_inclusive, hi, hi_inclusive),
            None => Ok(Vec::new()),
        }
    }

    /// The sorted base-index node ids this segment suppresses for `(label, prop)` — the
    /// removal sidecar a base probe must merge-subtract. Empty if none.
    pub fn removals(&self, label: &str, prop: &str) -> &[u64] {
        self.find(label, prop).map_or(&[], |f| &f.removals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_segix_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Copy every file in a written segment directory into `store` under `prefix`, so the
    /// store-native open path reads the same bytes the fs writer produced.
    fn stage_dir(dir: &std::path::Path, prefix: &str, store: &dyn ObjectStore) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let name = entry.file_name().into_string().unwrap();
                let bytes = std::fs::read(entry.path()).unwrap();
                store
                    .put(&format!("{prefix}/{name}"), &bytes, None)
                    .unwrap();
            }
        }
    }

    fn specs() -> Vec<IndexSpec> {
        vec![
            IndexSpec {
                label: "Person".into(),
                prop: "age".into(),
                entries: vec![
                    (Value::Int(30), 10),
                    (Value::Int(9), 12),
                    (Value::Int(30), 14), // duplicate value, different id
                ],
                removals: vec![5, 7, 100],
            },
            IndexSpec {
                label: "Person".into(),
                prop: "name".into(),
                entries: vec![
                    (Value::Str("Al".into()), 10),
                    (Value::Str("Zoe".into()), 12),
                ],
                removals: vec![],
            },
        ]
    }

    fn assert_reads(r: &SegmentIndexReader) {
        // Equality over the born fragment (value 30 held by two ids, ascending).
        let mut got = r.lookup_eq("Person", "age", &Value::Int(30)).unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![10, 14]);
        assert_eq!(
            r.lookup_eq("Person", "age", &Value::Int(9)).unwrap(),
            vec![12]
        );
        assert!(r
            .lookup_eq("Person", "age", &Value::Int(999))
            .unwrap()
            .is_empty());

        // Range.
        let mut rng = r
            .lookup_range("Person", "age", Some(&Value::Int(10)), true, None, true)
            .unwrap();
        rng.sort_unstable();
        assert_eq!(rng, vec![10, 14]); // 9 excluded, 30s included

        // String fragment.
        assert_eq!(
            r.lookup_eq("Person", "name", &Value::Str("Zoe".into()))
                .unwrap(),
            vec![12]
        );

        // Removal sidecar.
        assert_eq!(r.removals("Person", "age"), &[5, 7, 100]);
        assert_eq!(r.removals("Person", "name"), &[] as &[u64]);
        // Absent (label, prop): empty everywhere, never an error.
        assert!(r.removals("Ghost", "x").is_empty());
        assert!(r
            .lookup_eq("Ghost", "x", &Value::Int(1))
            .unwrap()
            .is_empty());

        let mut idx = r.indexed();
        idx.sort();
        assert_eq!(idx, vec![("Person", "age"), ("Person", "name")]);
    }

    #[test]
    fn round_trip_plaintext() {
        let dir = tmp("rt");
        write_index_fragments(&dir, &specs(), 64, 3, None).unwrap();
        let r = SegmentIndexReader::open(&dir, None).unwrap();
        assert_reads(&r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trips_via_object_store() {
        use crate::store::mem::MemObjectStore;
        let dir = tmp("via");
        write_index_fragments(&dir, &specs(), 64, 3, None).unwrap();
        let store = MemObjectStore::new();
        stage_dir(&dir, "seg", &store);
        let r = SegmentIndexReader::open_if_present_via(&store, "seg", None)
            .unwrap()
            .unwrap();
        assert_reads(&r);
        // Absent prefix ⇒ None, matching the fs `open_if_present`.
        assert!(
            SegmentIndexReader::open_if_present_via(&store, "nope", None)
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trip_encrypted_and_refuses_without_key() {
        let dir = tmp("enc");
        let cipher = Arc::new(BlockCipher::from_key(&[3u8; 32]));
        write_index_fragments(&dir, &specs(), 64, 3, Some(cipher.clone())).unwrap();
        let r = SegmentIndexReader::open(&dir, Some(cipher)).unwrap();
        assert_reads(&r);
        // The encrypted ISAM fragments refuse to open without the key.
        assert!(SegmentIndexReader::open(&dir, None).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn absent_meta_is_none() {
        let dir = tmp("absent");
        assert!(SegmentIndexReader::open_if_present(&dir, None)
            .unwrap()
            .is_none());
        assert!(SegmentIndexReader::open(&dir, None).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_specs_round_trip() {
        let dir = tmp("emptyspecs");
        write_index_fragments(&dir, &[], 64, 3, None).unwrap();
        let r = SegmentIndexReader::open(&dir, None).unwrap();
        assert!(r.indexed().is_empty());
        assert!(r.lookup_eq("x", "y", &Value::Int(1)).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rejects_unsorted_removals() {
        let dir = tmp("badrem");
        let bad = vec![IndexSpec {
            label: "L".into(),
            prop: "p".into(),
            entries: vec![],
            removals: vec![5, 3], // descending
        }];
        let err = write_index_fragments(&dir, &bad, 64, 3, None).unwrap_err();
        assert!(format!("{err:#}").contains("ascending"), "{err:#}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let dir = tmp("corrupt");
        write_index_fragments(&dir, &specs(), 64, 3, None).unwrap();
        let mut meta = std::fs::read(dir.join("idx.meta")).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0xff;
        std::fs::write(dir.join("idx.meta"), &meta).unwrap();
        assert!(SegmentIndexReader::open(&dir, None).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
