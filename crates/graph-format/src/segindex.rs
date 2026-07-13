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
//! body = version:uvarint ‖ count:uvarint ‖ count × ( label:str ‖ prop:str ‖ removals:u64-list ‖ fence )
//! fence = 0:u8                        (no fragment values — a removal-only fragment)
//!       | 1:u8 ‖ min:value ‖ max:value   (the cmp_key min/max of this fragment's entries)
//! ```
//! The `k`-th descriptor owns `idx_<k>.isam`. Absent `idx.meta` ⇒ a segment with no index
//! fragments (a flush that touched no indexed property); [`SegmentIndexReader::open_if_present`]
//! returns `None`.
//!
//! The **fence** is a resident per-fragment value range (min/max under [`Value::cmp_key`], the
//! ISAM order). A probe whose key/range cannot fall inside the fence provably has no hit in this
//! fragment, so the fold skips the fragment's ISAM `lookup_*` — and with it the leaf-block
//! decompress that is the write-path resolve floor — without an I/O. It gates only the *fragment*
//! lookup, never the removal sidecar: removals suppress base ids by *id* regardless of the probed
//! value, so they are always applied.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::crypto::BlockCipher;
use crate::ids::Value;
use crate::isam::{write_isam_with_cipher, IsamReader};
use crate::store::{join_key, ObjectStore};
use crate::wire::{capacity_for, read_uvarint, read_value, write_uvarint, write_value};

/// Magic at the head of `idx.meta`.
const IDX_MAGIC: &[u8; 8] = b"SLSEGIX1";
/// Index-fragment format version. v2 adds the per-fragment value fence.
const IDX_VERSION: u64 = 2;

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
    // `n` is an untrusted on-disk uvarint and each delta costs ≥1 byte, so reserve no more
    // than the bytes left can justify (`wire::capacity_for`) — a forged count then runs the
    // buffer dry and errors instead of aborting the process on a 16-exabyte allocation.
    let mut out = Vec::with_capacity(capacity_for(n, r.len(), 1));
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

/// The `cmp_key` min/max of a fragment's entry values — its value fence. `None` for an
/// entry-less (removal-only) fragment, which holds no value at all.
fn fence_of(entries: &[(Value, u64)]) -> Option<(Value, Value)> {
    let mut it = entries.iter().map(|(v, _)| v);
    let first = it.next()?;
    let (mut min, mut max) = (first.clone(), first.clone());
    for v in it {
        if v.cmp_key(&min) == Ordering::Less {
            min = v.clone();
        }
        if v.cmp_key(&max) == Ordering::Greater {
            max = v.clone();
        }
    }
    Some((min, max))
}

fn w_fence(buf: &mut Vec<u8>, fence: &Option<(Value, Value)>) {
    match fence {
        Some((min, max)) => {
            buf.push(1);
            write_value(buf, min);
            write_value(buf, max);
        }
        None => buf.push(0),
    }
}

fn r_fence(r: &mut &[u8]) -> Result<Option<(Value, Value)>> {
    let Some((&tag, rest)) = r.split_first() else {
        bail!("segindex: short fence");
    };
    *r = rest;
    match tag {
        0 => Ok(None),
        1 => {
            let min = read_value(r)?;
            let max = read_value(r)?;
            Ok(Some((min, max)))
        }
        b => bail!("segindex: bad fence tag {b}"),
    }
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
        w_fence(&mut body, &fence_of(&spec.entries));
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
    /// `cmp_key` min/max of the fragment's entry values; `None` for a removal-only fragment.
    fence: Option<(Value, Value)>,
}

/// Parse and validate an `idx.meta` body (magic ‖ crc32c ‖ uvarint body) into the per-
/// fragment `(label, prop, removals)` descriptors. The ISAM files are opened separately by
/// the caller (differently for fs vs store), but the magic/crc/version checks are shared.
type IdxDescriptor = (String, String, Vec<u64>, Option<(Value, Value)>);

fn decode_idx_meta(meta: &[u8]) -> Result<Vec<IdxDescriptor>> {
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
        let fence = r_fence(&mut r)?;
        out.push((label, prop, removals, fence));
    }
    if !r.is_empty() {
        bail!("segment idx.meta has {} trailing bytes", r.len());
    }
    Ok(out)
}

/// The opened index fragments of one core segment.
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
        for (k, (label, prop, removals, fence)) in decode_idx_meta(&meta)?.into_iter().enumerate() {
            let isam = IsamReader::open_with_cipher(dir.join(isam_name(k)), cipher.clone())
                .with_context(|| format!("open index fragment {k} ({label}, {prop})"))?;
            fragments.push(Fragment {
                label,
                prop,
                isam,
                removals,
                fence,
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
        for (k, (label, prop, removals, fence)) in decode_idx_meta(&meta)?.into_iter().enumerate() {
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
                fence,
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

    /// Fence-gated **batch** equality lookup: `out[i]` is the born/patched node ids whose
    /// `(label, prop)` value equals `keys[i]`, aligned to the input. `keys` must be sorted
    /// ascending by `cmp_key` and distinct — the batch counterpart of
    /// [`lookup_eq`](Self::lookup_eq), driven by [`IsamReader::lookup_eq_sorted`] so the
    /// whole batch costs one decompress per touched fragment block (the bulk-write ISAM
    /// floor). The **fence** gates it exactly as [`may_hold_eq`](Self::may_hold_eq) gates a
    /// point lookup: a key outside the fragment's resident `cmp_key` range (or every key,
    /// for an absent / removal-only fragment) is a provable miss and is swept-over — its
    /// slot stays empty at no I/O. So `out[i]` equals `may_hold_eq(k) ? lookup_eq(k) : []`
    /// for each key, byte-identical to the point path.
    pub fn lookup_eq_sorted(
        &self,
        label: &str,
        prop: &str,
        keys: &[&Value],
    ) -> Result<Vec<Vec<u64>>> {
        let mut out = vec![Vec::new(); keys.len()];
        let Some(fr) = self.find(label, prop) else {
            return Ok(out); // no fragment ⇒ every key misses
        };
        // A removal-only fragment carries no fence (and no entries); a key outside the fence
        // cannot be in the ISAM. Sweep only the in-fence keys, then scatter back by slot.
        let Some((min, max)) = fr.fence.as_ref() else {
            return Ok(out);
        };
        let mut slots = Vec::new();
        let mut in_fence = Vec::new();
        for (i, &k) in keys.iter().enumerate() {
            if k.cmp_key(min) != Ordering::Less && k.cmp_key(max) != Ordering::Greater {
                slots.push(i);
                in_fence.push(k);
            }
        }
        for (slot, ids) in slots.into_iter().zip(fr.isam.lookup_eq_sorted(&in_fence)?) {
            out[slot] = ids;
        }
        Ok(out)
    }

    /// The sorted base-index node ids this segment suppresses for `(label, prop)` — the
    /// removal sidecar a base probe must merge-subtract. Empty if none.
    pub fn removals(&self, label: &str, prop: &str) -> &[u64] {
        self.find(label, prop).map_or(&[], |f| &f.removals)
    }

    /// Fence check for an **equality** probe: `true` when `key` could fall inside this
    /// fragment's value range, so [`lookup_eq`](Self::lookup_eq) must actually be run.
    /// `false` — a certain miss — when there is no fragment for `(label, prop)` (nothing to
    /// look up) or `key` lies outside the resident `cmp_key` fence (no leaf-block read needed).
    /// A `false` here yields exactly the empty result `lookup_eq` would have, at no I/O.
    pub fn may_hold_eq(&self, label: &str, prop: &str, key: &Value) -> bool {
        match self.find(label, prop).and_then(|f| f.fence.as_ref()) {
            None => false,
            Some((min, max)) => {
                key.cmp_key(min) != Ordering::Less && key.cmp_key(max) != Ordering::Greater
            }
        }
    }

    /// Fence check for a **range** probe: `true` when the fragment's value range overlaps the
    /// half-open/closed probe range `[lo, hi]` (an unbounded side never bounds the overlap),
    /// so [`lookup_range`](Self::lookup_range) must run; `false` — a certain miss — otherwise.
    pub fn may_hold_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> bool {
        let Some((min, max)) = self.find(label, prop).and_then(|f| f.fence.as_ref()) else {
            return false;
        };
        if let Some(lo) = lo {
            match max.cmp_key(lo) {
                Ordering::Less => return false,
                Ordering::Equal if !lo_inclusive => return false,
                _ => {}
            }
        }
        if let Some(hi) = hi {
            match min.cmp_key(hi) {
                Ordering::Greater => return false,
                Ordering::Equal if !hi_inclusive => return false,
                _ => {}
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    // HIK-80: `r_ids` sized its `Vec` from the untrusted on-disk count. Six bytes declaring
    // 2^64 ids was a 16-exabyte reservation — an allocator abort, i.e. the process dies — on a
    // path reached whenever a segment's index metadata is opened. It must refuse instead, and
    // reaching this assertion at all is the proof: pre-fix, the test binary aborts here.
    #[test]
    fn forged_removal_count_is_refused_not_preallocated() {
        let mut rec = Vec::new();
        write_uvarint(&mut rec, u64::MAX);
        assert!(r_ids(&mut &rec[..]).is_err());

        // An honest list still round-trips (the clamp only ever bounds the *reservation*).
        let mut ok = Vec::new();
        w_ids(&mut ok, &[3, 9, 900]);
        assert_eq!(r_ids(&mut &ok[..]).unwrap(), vec![3, 9, 900]);
    }

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

        // Value fence: age spans [9, 30], name spans ["Al", "Zoe"] under cmp_key. A probe
        // inside the fence must run; one outside is a certain miss the fold can skip.
        assert!(r.may_hold_eq("Person", "age", &Value::Int(30)));
        assert!(r.may_hold_eq("Person", "age", &Value::Int(9)));
        assert!(!r.may_hold_eq("Person", "age", &Value::Int(8))); // below min
        assert!(!r.may_hold_eq("Person", "age", &Value::Int(31))); // above max
        assert!(!r.may_hold_eq("Person", "age", &Value::Str("30".into()))); // wrong type, above
        assert!(!r.may_hold_eq("Ghost", "x", &Value::Int(1))); // no fragment
        assert!(r.may_hold_eq("Person", "name", &Value::Str("Bo".into()))); // "Al" < "Bo" < "Zoe"
        assert!(!r.may_hold_eq("Person", "name", &Value::Str("Zz".into()))); // above "Zoe"

        // Batch equality sweep == the per-key point path, with the fence gating each key:
        // 8 (below min) and 31 (above max) are provable misses ⇒ empty slots; 9 and 30 hit.
        let keys = [Value::Int(8), Value::Int(9), Value::Int(30), Value::Int(31)];
        let refs: Vec<&Value> = keys.iter().collect();
        let swept = r.lookup_eq_sorted("Person", "age", &refs).unwrap();
        assert_eq!(swept.len(), keys.len());
        for (k, got) in keys.iter().zip(&swept) {
            let want = if r.may_hold_eq("Person", "age", k) {
                let mut v = r.lookup_eq("Person", "age", k).unwrap();
                v.sort_unstable();
                v
            } else {
                Vec::new()
            };
            let mut got = got.clone();
            got.sort_unstable();
            assert_eq!(got, want, "batch sweep diverges at {k:?}");
        }
        // An absent fragment sweeps to all-empty; an empty key list is a no-op.
        assert_eq!(
            r.lookup_eq_sorted("Ghost", "x", &refs).unwrap(),
            vec![Vec::<u64>::new(); keys.len()]
        );
        assert!(r.lookup_eq_sorted("Person", "age", &[]).unwrap().is_empty());

        // Range fence overlap, respecting inclusivity and unbounded sides.
        assert!(!r.may_hold_range("Person", "age", Some(&Value::Int(31)), true, None, true));
        assert!(r.may_hold_range("Person", "age", Some(&Value::Int(30)), true, None, true)); // touches max
        assert!(!r.may_hold_range("Person", "age", Some(&Value::Int(30)), false, None, true)); // (30,∞) misses
        assert!(!r.may_hold_range("Person", "age", None, true, Some(&Value::Int(9)), false)); // (-∞,9) misses
        assert!(r.may_hold_range("Person", "age", None, true, Some(&Value::Int(9)), true)); // (-∞,9] touches min
        assert!(r.may_hold_range("Person", "age", None, true, None, true)); // fully unbounded
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
