// SPDX-License-Identifier: Apache-2.0
//! A core segment's **vector sidecar** — which nodes this segment gives an embedding to,
//! and which it takes one away from (the segmented-core track; see
//! `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! # Why there is no vector *fragment*
//! Unlike [`crate::segindex`], this carries **no copy of the data**. A segment's node rows
//! are generic over [`Value`](crate::ids::Value), and `Value::Vector` is a first-class wire
//! type, so a flushed node's embedding is *already* sitting in the segment's `node.blk` row
//! — a vector fragment would be a second copy of every embedding on disk. What the rows
//! cannot express is the two things a KNN read actually needs:
//!
//!   * **which** nodes carry one. Finding them by scanning every row in the segment costs
//!     O(segment), on every query, to recover a set that is usually tiny. The `ids` list
//!     makes it O(vectors).
//!   * **removals**. This is the one thing the rows genuinely *cannot* say. An indexed
//!     embedding is routed out of the column store (D12), so a base node's row never held
//!     it — which means a flushed row that lacks an embedding is ambiguous: the node might
//!     have had its embedding `REMOVE`d, or it might simply have been flushed for an
//!     unrelated reason (`SET n.age = 99`) while keeping the base's vector. Absence cannot
//!     distinguish them. Without an explicit removal record the node's stale base vector
//!     keeps scoring, and `REMOVE n.embedding` silently does nothing to KNN.
//!
//! So: `ids` is an optimisation, `removals` is a correctness requirement.
//!
//! # Removal *kind* — why there are two removal lists
//! A removal is recorded for one of two reasons that need **opposite** treatment on a later
//! `SET n:Doc`, and a flat "removed id" cannot tell them apart:
//!
//!   * **label-removal** — the node left the index's *scope* (`REMOVE n:Doc` dropped the
//!     index's label). The embedding *value* is untouched (D64), so re-adding the label must
//!     **un-suppress** the id and let its base/lower vector score again.
//!   * **value-removal** — the embedding *value* is gone (`REMOVE n.embedding`, a `SET n = {…}`
//!     replace that dropped it, or a non-vector overwrite). It must stay suppressed **regardless
//!     of labels** — there is no value to restore.
//!
//! So the sidecar keeps them apart: `label_removals` is un-suppressible on re-label,
//! `value_removals` is permanent (HIK-118). When an id appears in both (a below-run fold can
//! produce it), **value dominates** — the value is genuinely gone.
//!
//! # `vec.meta`
//! ```text
//! MAGIC(8) ‖ crc32c(body)(4) ‖ body
//! body = version:uvarint ‖ count:uvarint
//!        ‖ count × ( label:str ‖ prop:str ‖ ids:u64-list ‖ label_removals:u64-list
//!                    ‖ value_removals:u64-list )
//! ```
//! Every id list is ascending, de-duplicated and delta-varint encoded (the `segindex` /
//! postings encoding). Absent `vec.meta` ⇒ a segment that touched no embedding;
//! [`SegmentVectorReader::open_if_present`] returns `None` and the fold leaves the base's
//! vectors alone.
//!
//! At read time the fold runs oldest → newest: per segment, drop the ids it removes and
//! the ids it re-embeds from the surviving set, then add its own. The newest level wins,
//! which is the same shape as [`crate::segindex`]'s `fold_index_eq`.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::store::{join_key, ObjectStore};
use crate::wire::{capacity_for, read_uvarint, write_uvarint};

/// Magic at the head of `vec.meta`. The trailing digit is the format version, so an older
/// sidecar shares the [`VEC_MAGIC_PREFIX`] and can be refused with a legible "rebuild" error
/// rather than a generic bad-magic (there are zero legacy installs, but the file is on-disk).
const VEC_MAGIC: &[u8; 8] = b"SLSEGVE2";
/// The version-independent prefix of [`VEC_MAGIC`] — a `vec.meta` from any build starts with it.
const VEC_MAGIC_PREFIX: &[u8; 7] = b"SLSEGVE";
/// Vector-sidecar format version. Bumped to 2 for the removal-kind split (HIK-118).
const VEC_VERSION: u64 = 2;

/// One `(label, prop)` vector index a segment touches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorSpec {
    pub label: String,
    pub prop: String,
    /// Ascending, de-duplicated dense node ids this segment carries an embedding for — the
    /// vector itself lives in the node's row, not here.
    pub ids: Vec<u64>,
    /// Ascending, de-duplicated dense node ids the node left the index's **scope** for
    /// (`REMOVE n:Doc` dropped the index's label). Their base — or older-segment — vector must
    /// be suppressed *while the node stays out of scope*; a later `SET n:Doc` un-suppresses it
    /// (the value was never destroyed — D64/HIK-118). See the module docs.
    pub label_removals: Vec<u64>,
    /// Ascending, de-duplicated dense node ids whose embedding **value** this segment removes
    /// (`REMOVE n.embedding`, a `SET n = {…}` replace that dropped it, or a non-vector
    /// overwrite). Their base — or older-segment — vector must be suppressed **permanently**,
    /// with nothing put in its place: there is no value to bring back.
    pub value_removals: Vec<u64>,
}

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> Result<String> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("segvectors: short string");
    }
    let s = std::str::from_utf8(&r[..n])
        .context("segvectors: invalid utf8")?
        .to_string();
    *r = &r[n..];
    Ok(s)
}

/// Delta-varint encode an ascending id list (matches the `segindex` / postings encoding).
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
    // buffer dry and errors instead of aborting the process on a huge allocation.
    let mut out = Vec::with_capacity(capacity_for(n, r.len(), 1));
    let mut prev = 0u64;
    for _ in 0..n {
        prev += read_uvarint(r)?;
        out.push(prev);
    }
    Ok(out)
}

fn ascending_distinct(ids: &[u64]) -> bool {
    ids.windows(2).all(|w| w[0] < w[1])
}

fn encode(specs: &[VectorSpec]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    write_uvarint(&mut body, VEC_VERSION);
    write_uvarint(&mut body, specs.len() as u64);
    for spec in specs {
        // The reader merge-suppresses by binary search, so an unsorted or duplicated list
        // would silently fail to suppress rather than error at read time.
        if !ascending_distinct(&spec.ids)
            || !ascending_distinct(&spec.label_removals)
            || !ascending_distinct(&spec.value_removals)
        {
            bail!(
                "segvectors ids/removals for ({}, {}) must be ascending and de-duplicated",
                spec.label,
                spec.prop
            );
        }
        w_str(&mut body, &spec.label);
        w_str(&mut body, &spec.prop);
        w_ids(&mut body, &spec.ids);
        w_ids(&mut body, &spec.label_removals);
        w_ids(&mut body, &spec.value_removals);
    }
    let crc = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(VEC_MAGIC);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn decode(bytes: &[u8]) -> Result<Vec<VectorSpec>> {
    if bytes.len() < 12 {
        bail!("segvectors: vec.meta too short");
    }
    if &bytes[..8] != VEC_MAGIC {
        // A `vec.meta` from an older build shares the version-independent prefix; report it as an
        // out-of-date format that a rebuild fixes, rather than as an unrecognisable file. Zero
        // legacy installs exist, but the file is on-disk and must be refused legibly, not misread.
        if &bytes[..7] == VEC_MAGIC_PREFIX {
            bail!(
                "segvectors: vec.meta is an older on-disk format (magic {:?}); this build reads \
                 only {:?} — rebuild the store",
                String::from_utf8_lossy(&bytes[..8]),
                String::from_utf8_lossy(VEC_MAGIC),
            );
        }
        bail!("segvectors: bad magic in vec.meta");
    }
    let want = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let body = &bytes[12..];
    let got = crc32c::crc32c(body);
    if got != want {
        bail!("segvectors: vec.meta crc mismatch (want {want:08x}, got {got:08x})");
    }
    let mut r = body;
    let version = read_uvarint(&mut r)?;
    if version != VEC_VERSION {
        bail!("segvectors: unsupported vec.meta version {version}");
    }
    let n = read_uvarint(&mut r)? as usize;
    // Each spec costs ≥4 bytes (two empty strings, two empty lists), so clamp the count.
    let mut out = Vec::with_capacity(capacity_for(n, r.len(), 4));
    for _ in 0..n {
        let label = r_str(&mut r)?;
        let prop = r_str(&mut r)?;
        let ids = r_ids(&mut r)?;
        let label_removals = r_ids(&mut r)?;
        let value_removals = r_ids(&mut r)?;
        out.push(VectorSpec {
            label,
            prop,
            ids,
            label_removals,
            value_removals,
        });
    }
    Ok(out)
}

/// Write a segment's vector sidecar into `dir` (the segment directory, which must exist).
/// Writes nothing at all when there is nothing to say, so a graph with no vector index
/// never grows a `vec.meta`.
///
/// Unencrypted, deliberately: it holds only dense node ids — no property values, no
/// embeddings — exactly like the segment manifest that names it, and the manifest's MAC
/// covers its content hash via the directory inventory.
pub fn write_vector_fragments(dir: impl AsRef<Path>, specs: &[VectorSpec]) -> Result<()> {
    let specs: Vec<VectorSpec> = specs
        .iter()
        .filter(|s| {
            !s.ids.is_empty() || !s.label_removals.is_empty() || !s.value_removals.is_empty()
        })
        .cloned()
        .collect();
    if specs.is_empty() {
        return Ok(());
    }
    let out = encode(&specs)?;
    std::fs::write(dir.as_ref().join("vec.meta"), &out).context("write vec.meta")?;
    Ok(())
}

/// A segment's opened vector sidecar.
pub struct SegmentVectorReader {
    specs: Vec<VectorSpec>,
}

impl SegmentVectorReader {
    /// Open the vector sidecar if the segment carries one; `None` if `vec.meta` is absent
    /// (the segment touched no embedding).
    pub fn open_if_present(dir: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = dir.as_ref().join("vec.meta");
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {path:?}")),
        };
        Ok(Some(Self {
            specs: decode(&bytes)?,
        }))
    }

    /// Store-native counterpart of [`open_if_present`](SegmentVectorReader::open_if_present),
    /// so a segment on any backend opens the same way.
    pub fn open_if_present_via(store: &dyn ObjectStore, prefix: &str) -> Result<Option<Self>> {
        let key = join_key(prefix, "vec.meta");
        if !store.exists(&key)? {
            return Ok(None);
        }
        let bytes = store
            .read_all(&key)
            .with_context(|| format!("read {key}"))?;
        Ok(Some(Self {
            specs: decode(&bytes)?,
        }))
    }

    fn find(&self, label: &str, prop: &str) -> Option<&VectorSpec> {
        self.specs
            .iter()
            .find(|s| s.label == label && s.prop == prop)
    }

    /// The dense node ids this segment carries an embedding for, ascending.
    pub fn ids(&self, label: &str, prop: &str) -> &[u64] {
        self.find(label, prop).map_or(&[], |s| &s.ids)
    }

    /// The dense node ids this segment removes from the index's **scope** (`REMOVE n:Doc`),
    /// ascending. Un-suppressible on re-label — the value is retained (HIK-118).
    pub fn label_removals(&self, label: &str, prop: &str) -> &[u64] {
        self.find(label, prop).map_or(&[], |s| &s.label_removals)
    }

    /// The dense node ids whose embedding **value** this segment removes (`REMOVE n.embedding`
    /// and friends), ascending. Permanent — there is no value to bring back (HIK-118).
    pub fn value_removals(&self, label: &str, prop: &str) -> &[u64] {
        self.find(label, prop).map_or(&[], |s| &s.value_removals)
    }

    /// Every spec this segment carries — the T3 merge folds these.
    pub fn specs(&self) -> &[VectorSpec] {
        &self.specs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_segvec_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn spec(
        label: &str,
        ids: &[u64],
        label_removals: &[u64],
        value_removals: &[u64],
    ) -> VectorSpec {
        VectorSpec {
            label: label.into(),
            prop: "embedding".into(),
            ids: ids.to_vec(),
            label_removals: label_removals.to_vec(),
            value_removals: value_removals.to_vec(),
        }
    }

    #[test]
    fn round_trips_ids_and_both_removal_kinds() {
        let dir = tmp("roundtrip");
        let specs = vec![
            spec("Person", &[1, 5, 900_000], &[2, 7], &[3]),
            spec("Doc", &[], &[42], &[]),
        ];
        write_vector_fragments(&dir, &specs).unwrap();

        let r = SegmentVectorReader::open_if_present(&dir)
            .unwrap()
            .expect("sidecar written");
        assert_eq!(r.ids("Person", "embedding"), &[1, 5, 900_000]);
        assert_eq!(r.label_removals("Person", "embedding"), &[2, 7]);
        assert_eq!(r.value_removals("Person", "embedding"), &[3]);
        // A removal-only spec is legal — that is the whole point of the sidecar. Here a pure
        // label-removal (the HIK-118 shape: scope left, value retained).
        assert_eq!(r.ids("Doc", "embedding"), &[] as &[u64]);
        assert_eq!(r.label_removals("Doc", "embedding"), &[42]);
        assert_eq!(r.value_removals("Doc", "embedding"), &[] as &[u64]);
        // An index the segment never touched reads as empty, not as an error.
        assert_eq!(r.ids("Person", "other"), &[] as &[u64]);
    }

    /// A segment that touches no embedding writes no file, and opening one is `None`
    /// rather than an error — the read fold then leaves the base's vectors alone.
    #[test]
    fn writes_nothing_when_there_is_nothing_to_say() {
        let dir = tmp("empty");
        write_vector_fragments(&dir, &[]).unwrap();
        write_vector_fragments(&dir, &[spec("Person", &[], &[], &[])]).unwrap();
        assert!(!dir.join("vec.meta").exists());
        assert!(SegmentVectorReader::open_if_present(&dir)
            .unwrap()
            .is_none());
    }

    /// The reader merge-suppresses by binary search, so an unsorted list would silently
    /// fail to suppress. Refuse it at the writer instead.
    #[test]
    fn refuses_unsorted_or_duplicated_ids() {
        let dir = tmp("unsorted");
        // Each list is checked independently: ids, then label_removals, then value_removals.
        for bad in [
            spec("P", &[5, 1], &[], &[]),
            spec("P", &[], &[3, 3], &[]),
            spec("P", &[], &[], &[9, 2]),
        ] {
            let e = write_vector_fragments(&dir, &[bad]).unwrap_err();
            assert!(
                e.to_string().contains("ascending and de-duplicated"),
                "got: {e}"
            );
        }
    }

    #[test]
    fn rejects_a_corrupt_sidecar() {
        let specs = vec![spec("Person", &[1, 2], &[3], &[4])];
        let good = encode(&specs).unwrap();

        let mut bad_magic = good.clone();
        bad_magic[0] ^= 0xff;
        assert!(decode(&bad_magic)
            .unwrap_err()
            .to_string()
            .contains("magic"));

        // Flip a body byte: the crc must catch it rather than decode garbage ids.
        let mut bad_crc = good.clone();
        *bad_crc.last_mut().unwrap() ^= 0xff;
        assert!(decode(&bad_crc).unwrap_err().to_string().contains("crc"));

        assert_eq!(decode(&good).unwrap(), specs);
    }

    /// An older-format `vec.meta` (the v1 magic `SLSEGVE1`, pre-HIK-118) shares the version
    /// prefix, so decode must refuse it with a legible "older on-disk format … rebuild" error —
    /// not silently misread its two-list body as this build's three-list one, and not report a
    /// generic bad-magic. There are zero legacy installs, but the file is on-disk.
    #[test]
    fn refuses_an_older_format_sidecar_legibly() {
        let specs = vec![spec("Person", &[1, 2], &[3], &[4])];
        let mut v1 = encode(&specs).unwrap();
        // Rewrite the magic's version digit to the previous format's.
        v1[7] = b'1';
        // Re-stamp the crc so the magic check (not the crc) is what rejects it.
        let crc = crc32c::crc32c(&v1[12..]);
        v1[8..12].copy_from_slice(&crc.to_le_bytes());
        let e = decode(&v1).unwrap_err().to_string();
        assert!(
            e.contains("older on-disk format") && e.contains("rebuild"),
            "expected a legible rebuild error, got: {e}"
        );
        // A file that is not a vec.meta at all still reports the generic bad-magic.
        let mut alien = encode(&specs).unwrap();
        alien[..7].copy_from_slice(b"NOTAVEC");
        assert!(decode(&alien)
            .unwrap_err()
            .to_string()
            .contains("bad magic"));
    }

    /// A forged length must run the buffer dry and error, not attempt a huge allocation.
    #[test]
    fn a_forged_id_count_errors_rather_than_aborting() {
        let mut body = Vec::new();
        write_uvarint(&mut body, VEC_VERSION);
        write_uvarint(&mut body, 1);
        w_str(&mut body, "Person");
        w_str(&mut body, "embedding");
        write_uvarint(&mut body, u64::MAX); // ids: forged count, no data behind it
        let crc = crc32c::crc32c(&body);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(VEC_MAGIC);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&body);
        assert!(decode(&bytes).is_err(), "a forged count must error");
    }
}
