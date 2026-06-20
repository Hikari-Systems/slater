// SPDX-License-Identifier: Apache-2.0
//! ISAM-style sorted range index.
//!
//! `(value, id)` pairs are sorted, packed into compressed key-blocks, and a small
//! **sparse top-level** (the first key of each block → block location) is held
//! resident at open. Equality and range lookups binary-search the top-level to
//! find the candidate block(s), then read and scan only those — never the whole
//! index.
//!
//! On-disk layout:
//! ```text
//! MAGIC(8) ‖ block_0 ‖ … ‖ block_{n-1} ‖ top_level ‖ footer(24)
//! footer    = top_offset:u64 ‖ top_len:u64 ‖ block_count:u64
//! top_level = block_count × ( value(first_key) ‖ offset:u64 ‖ comp_len:u32 ‖ raw_len:u32 )
//! block raw = uvarint(count) ‖ count × ( value(key) ‖ uvarint(id) )   (then zstd)
//! ```

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec;
use crate::crypto::{BlockCipher, NONCE_LEN};
use crate::ids::Value;
use crate::store::fs::FileObject;
use crate::store::RandomReadAt;
use crate::wire::{read_uvarint, read_value, write_uvarint, write_value};

const ISAM_MAGIC: &[u8; 8] = b"SLISM001";
/// Magic for an AEAD-encrypted ISAM index: the block bytes are sealed compressed
/// blocks, and the whole resident top-level — which holds each block's first key
/// in the clear — is itself sealed so no key material leaks at rest (D28).
const ISAM_MAGIC_ENC: &[u8; 8] = b"SLISME01";
const FOOTER_LEN: u64 = 24;
/// Encrypted footer also carries the nonce that seals the top-level.
const FOOTER_LEN_ENC: u64 = FOOTER_LEN + NONCE_LEN as u64;

/// Build an ISAM index from `(value, id)` pairs. The input need not be sorted —
/// it is sorted here by key order then id.
pub fn write_isam(
    path: impl AsRef<Path>,
    entries: Vec<(Value, u64)>,
    target_block_bytes: usize,
    zstd_level: i32,
) -> Result<u64> {
    write_isam_with_cipher(path, entries, target_block_bytes, zstd_level, None)
}

/// Build an ISAM index, optionally AEAD-encrypted (`cipher = None` ⇒ plaintext,
/// identical to [`write_isam`]). Each compressed block is sealed under its own
/// random nonce, which is stored in the resident top-level beside the block's
/// location — the key never touches the file (D28).
///
/// Convenience wrapper that sorts in memory and delegates to
/// [`write_isam_sorted`]; the external builder, which cannot hold all entries,
/// feeds an already-sorted stream to `write_isam_sorted` directly.
pub fn write_isam_with_cipher(
    path: impl AsRef<Path>,
    mut entries: Vec<(Value, u64)>,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64> {
    entries.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
    write_isam_sorted(
        path,
        entries.into_iter().map(Ok),
        target_block_bytes,
        zstd_level,
        cipher,
    )
}

/// One block's resident top-level entry (first key → block location + nonce).
struct Top {
    first_key: Value,
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    nonce: Option<[u8; NONCE_LEN]>,
}

/// Compress, optionally seal, and write one key-block; record its top-level entry.
#[allow(clippy::too_many_arguments)] // a cohesive write step; bundling into a struct would not aid clarity
fn flush_isam_block(
    file: &mut BufWriter<File>,
    offset: &mut u64,
    tops: &mut Vec<Top>,
    first_key: Value,
    count: u64,
    body: &[u8],
    zstd_level: i32,
    cipher: &Option<Arc<BlockCipher>>,
) -> Result<()> {
    let mut raw = Vec::with_capacity(body.len() + 8);
    write_uvarint(&mut raw, count);
    raw.extend_from_slice(body);
    let comp = codec::compress(&raw, zstd_level)?;
    let (stored, nonce) = match cipher {
        Some(c) => {
            let nonce = BlockCipher::random_nonce();
            (c.encrypt(&nonce, &comp)?, Some(nonce))
        }
        None => (comp, None),
    };
    file.write_all(&stored)?;
    tops.push(Top {
        first_key,
        offset: *offset,
        comp_len: stored.len() as u32,
        raw_len: raw.len() as u32,
        nonce,
    });
    *offset += stored.len() as u64;
    Ok(())
}

/// Build an ISAM index from an **already-sorted** stream of `(value, id)` pairs
/// (ascending by [`Value::cmp_key`] then id — the same order [`write_isam`]
/// produces internally). Byte-for-byte identical to `write_isam` on the same
/// entries, but holds only the current key-block resident, so the external
/// builder can stream entries straight from an [`crate::extsort::ExtSorter`]
/// without materialising them all.
pub fn write_isam_sorted<I>(
    path: impl AsRef<Path>,
    entries: I,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64>
where
    I: IntoIterator<Item = Result<(Value, u64)>>,
{
    let f = File::create(path.as_ref())
        .with_context(|| format!("create isam {}", path.as_ref().display()))?;
    let mut file = BufWriter::new(f);
    let magic = if cipher.is_some() {
        ISAM_MAGIC_ENC
    } else {
        ISAM_MAGIC
    };
    file.write_all(magic)?;
    let mut offset = magic.len() as u64;

    let mut tops: Vec<Top> = Vec::new();
    let mut first_key: Option<Value> = None;
    let mut count = 0u64;
    let mut body: Vec<u8> = Vec::new();

    for e in entries {
        let (k, id) = e?;
        if first_key.is_none() {
            first_key = Some(k.clone());
        }
        write_value(&mut body, &k);
        write_uvarint(&mut body, id);
        count += 1;
        // Close the block once it reaches target — the entry that crosses the
        // threshold is the block's last, matching `write_isam`'s boundary exactly.
        if body.len() >= target_block_bytes {
            flush_isam_block(
                &mut file,
                &mut offset,
                &mut tops,
                first_key.take().unwrap(),
                count,
                &body,
                zstd_level,
                &cipher,
            )?;
            count = 0;
            body.clear();
        }
    }
    if count > 0 {
        flush_isam_block(
            &mut file,
            &mut offset,
            &mut tops,
            first_key.take().unwrap(),
            count,
            &body,
            zstd_level,
            &cipher,
        )?;
    }

    // Top level.
    let top_offset = offset;
    let mut top_bytes = Vec::new();
    for t in &tops {
        write_value(&mut top_bytes, &t.first_key);
        top_bytes.write_u64::<LittleEndian>(t.offset)?;
        top_bytes.write_u32::<LittleEndian>(t.comp_len)?;
        top_bytes.write_u32::<LittleEndian>(t.raw_len)?;
        if let Some(nonce) = &t.nonce {
            top_bytes.write_all(nonce)?;
        }
    }
    // Seal the whole top-level (first keys + per-block nonces) when encrypting so
    // no plaintext key material is left resident on disk.
    let (stored_top, top_nonce) = match &cipher {
        Some(c) => {
            let nonce = BlockCipher::random_nonce();
            (c.encrypt(&nonce, &top_bytes)?, Some(nonce))
        }
        None => (top_bytes, None),
    };
    file.write_all(&stored_top)?;

    let mut footer = Vec::with_capacity(FOOTER_LEN_ENC as usize);
    footer.write_u64::<LittleEndian>(top_offset)?;
    footer.write_u64::<LittleEndian>(stored_top.len() as u64)?;
    footer.write_u64::<LittleEndian>(tops.len() as u64)?;
    if let Some(nonce) = &top_nonce {
        footer.write_all(nonce)?;
    }
    file.write_all(&footer)?;
    file.flush()?;
    file.get_ref().sync_all().context("fsync isam")?;
    Ok(tops.len() as u64)
}

struct TopEntry {
    first_key: Value,
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    /// Per-block AEAD nonce, present iff the index is encrypted.
    nonce: Option<[u8; NONCE_LEN]>,
}

/// Reader holding the resident sparse top-level.
pub struct IsamReader {
    src: Arc<dyn RandomReadAt>,
    top: Vec<TopEntry>,
    /// Per-generation cipher, set iff the index is encrypted.
    cipher: Option<Arc<BlockCipher>>,
}

impl IsamReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open an ISAM index by path (local filesystem) — a convenience wrapper
    /// over [`open_src`](IsamReader::open_src) for path-holding callers.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(FileObject::open(path.as_ref())?);
        Self::open_src(src, cipher)
    }

    /// Open an ISAM index from any positional-read source (local file or remote
    /// object), supplying the per-generation cipher for an encrypted index. An
    /// encrypted index opened without a key is refused with a clear error rather
    /// than returning garbage.
    pub fn open_src(src: Arc<dyn RandomReadAt>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        let len = src.len();
        if len < ISAM_MAGIC.len() as u64 + FOOTER_LEN {
            bail!("isam too short");
        }
        let mut magic = [0u8; 8];
        src.read_exact_at(&mut magic, 0)?;
        let encrypted = match &magic {
            m if m == ISAM_MAGIC => false,
            m if m == ISAM_MAGIC_ENC => true,
            _ => bail!("bad isam magic"),
        };
        let cipher = if encrypted {
            match cipher {
                Some(c) => Some(c),
                None => bail!("isam index is encrypted but no key was supplied"),
            }
        } else {
            None
        };
        let footer_len = if encrypted {
            FOOTER_LEN_ENC
        } else {
            FOOTER_LEN
        };
        if len < ISAM_MAGIC.len() as u64 + footer_len {
            bail!("isam too short");
        }
        let mut footer = vec![0u8; footer_len as usize];
        src.read_exact_at(&mut footer, len - footer_len)?;
        let mut fr = &footer[..];
        let top_offset = fr.read_u64::<LittleEndian>()?;
        let top_len = fr.read_u64::<LittleEndian>()?;
        let block_count = fr.read_u64::<LittleEndian>()?;
        let top_nonce = if encrypted {
            let mut n = [0u8; NONCE_LEN];
            fr.read_exact(&mut n)?;
            Some(n)
        } else {
            None
        };

        let mut stored_top = vec![0u8; top_len as usize];
        src.read_exact_at(&mut stored_top, top_offset)?;
        // Unseal the top-level when encrypted (a wrong key is caught here, at open).
        let top_bytes = match (&cipher, &top_nonce) {
            (Some(c), Some(nonce)) => c.decrypt(nonce, &stored_top)?,
            _ => stored_top,
        };
        let mut tr = &top_bytes[..];
        let mut top = Vec::with_capacity(block_count as usize);
        for _ in 0..block_count {
            let first_key = read_value(&mut tr)?;
            let offset = tr.read_u64::<LittleEndian>()?;
            let comp_len = tr.read_u32::<LittleEndian>()?;
            let raw_len = tr.read_u32::<LittleEndian>()?;
            let nonce = if encrypted {
                let mut n = [0u8; NONCE_LEN];
                tr.read_exact(&mut n)?;
                Some(n)
            } else {
                None
            };
            top.push(TopEntry {
                first_key,
                offset,
                comp_len,
                raw_len,
                nonce,
            });
        }
        Ok(Self { src, top, cipher })
    }

    pub fn num_blocks(&self) -> usize {
        self.top.len()
    }

    fn read_block(&self, b: usize) -> Result<Vec<(Value, u64)>> {
        let t = &self.top[b];
        let mut stored = vec![0u8; t.comp_len as usize];
        self.src.read_exact_at(&mut stored, t.offset)?;
        let comp = match (&self.cipher, &t.nonce) {
            (Some(cipher), Some(nonce)) => cipher.decrypt(nonce, &stored)?,
            _ => stored,
        };
        let raw = codec::decompress(&comp, t.raw_len as usize)?;
        let mut r = &raw[..];
        let count = read_uvarint(&mut r)? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let key = read_value(&mut r)?;
            let id = read_uvarint(&mut r)?;
            out.push((key, id));
        }
        Ok(out)
    }

    /// Exact-match lookup: all ids whose key equals `key`, sorted ascending.
    pub fn lookup_eq(&self, key: &Value) -> Result<Vec<u64>> {
        if self.top.is_empty() {
            return Ok(Vec::new());
        }
        // Count blocks whose first key is <= key; the matches live in the last
        // such block, possibly continuing backwards into earlier blocks if the
        // key value spans a block boundary.
        let le = self
            .top
            .partition_point(|t| t.first_key.cmp_key(key) != Ordering::Greater);
        if le == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut b = le - 1;
        loop {
            let entries = self.read_block(b)?;
            let first_is_key = entries
                .first()
                .map(|(k, _)| k.cmp_key(key) == Ordering::Equal)
                .unwrap_or(false);
            for (k, id) in entries {
                if k.cmp_key(key) == Ordering::Equal {
                    out.push(id);
                }
            }
            if first_is_key && b > 0 {
                b -= 1;
            } else {
                break;
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Range lookup over `[lo, hi]` with per-bound inclusivity. A `None` bound is
    /// unbounded on that side. Returns matching ids, sorted ascending.
    pub fn lookup_range(
        &self,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Result<Vec<u64>> {
        if self.top.is_empty() {
            return Ok(Vec::new());
        }
        // First block that may contain `lo`: the block before the first whose
        // first_key >= lo (or block 0 when lo is unbounded).
        let start = match lo {
            None => 0,
            Some(lo) => {
                let ip = self
                    .top
                    .partition_point(|t| t.first_key.cmp_key(lo) == Ordering::Less);
                ip.saturating_sub(1)
            }
        };

        let in_lo = |k: &Value| match lo {
            None => true,
            Some(lo) => match k.cmp_key(lo) {
                Ordering::Greater => true,
                Ordering::Equal => lo_inclusive,
                Ordering::Less => false,
            },
        };
        let past_hi = |k: &Value| match hi {
            None => false,
            Some(hi) => match k.cmp_key(hi) {
                Ordering::Greater => true,
                Ordering::Equal => !hi_inclusive,
                Ordering::Less => false,
            },
        };

        let mut out = Vec::new();
        for b in start..self.top.len() {
            // If this block starts beyond hi, no later block can match.
            if past_hi(&self.top[b].first_key) {
                break;
            }
            for (k, id) in self.read_block(b)? {
                if past_hi(&k) {
                    // entries are sorted; nothing later in this scan matches hi
                    out.sort_unstable();
                    out.dedup();
                    return Ok(out);
                }
                if in_lo(&k) {
                    out.push(id);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// Distinct keys in ascending order, each paired with the number of entries
    /// (ids) sharing that key. One sequential pass over all blocks — no per-key
    /// lookups and no node-record reads. Keys are globally sorted with equal keys
    /// adjacent (see `write_isam`), so a run-length count over the concatenated
    /// blocks is exact; an open run is carried across the block boundary.
    pub fn distinct_key_counts(&self) -> Result<Vec<(Value, u64)>> {
        let mut out: Vec<(Value, u64)> = Vec::new();
        for b in 0..self.top.len() {
            for (k, _) in self.read_block(b)? {
                match out.last_mut() {
                    Some((prev, n)) if prev.cmp_key(&k) == Ordering::Equal => *n += 1,
                    _ => out.push((k, 1)),
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_isam_{}_{}", std::process::id(), name))
    }

    fn linear_eq(entries: &[(Value, u64)], key: &Value) -> Vec<u64> {
        let mut v: Vec<u64> = entries
            .iter()
            .filter(|(k, _)| k.cmp_key(key) == Ordering::Equal)
            .map(|(_, id)| *id)
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn string_index_eq_matches_linear_scan() {
        let path = tmp("streq");
        // Repeated source names, the camelid-scale shape; many ids per key.
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999"];
        for id in 0..600u64 {
            let s = sources[(id % 3) as usize];
            entries.push((Value::Str(s.to_string()), id));
        }
        // Tiny blocks so keys span many blocks (stresses the boundary walk).
        write_isam(&path, entries.clone(), 64, 3).unwrap();

        let r = IsamReader::open(&path).unwrap();
        assert!(r.num_blocks() > 1);
        for s in sources {
            let key = Value::Str(s.to_string());
            assert_eq!(r.lookup_eq(&key).unwrap(), linear_eq(&entries, &key));
        }
        // Absent key.
        assert!(r.lookup_eq(&Value::Str("Nope".into())).unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn distinct_key_counts_matches_lookup_eq() {
        let path = tmp("distinct");
        // Repeated keys whose runs span block boundaries (tiny blocks below).
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Smith-1999", "Whitehead-2024"];
        for id in 0..600u64 {
            entries.push((Value::Str(sources[(id % 3) as usize].to_string()), id));
        }
        write_isam(&path, entries.clone(), 64, 3).unwrap();
        let r = IsamReader::open(&path).unwrap();
        assert!(r.num_blocks() > 1);

        let got = r.distinct_key_counts().unwrap();
        // Keys ascending and distinct.
        assert_eq!(
            got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            sources
                .iter()
                .map(|s| Value::Str(s.to_string()))
                .collect::<Vec<_>>(),
        );
        // Each count equals the per-key lookup length (the run-length is exact).
        for (k, n) in &got {
            assert_eq!(*n, r.lookup_eq(k).unwrap().len() as u64);
            assert_eq!(*n, linear_eq(&entries, k).len() as u64);
        }
        // Counts sum to the total number of entries.
        assert_eq!(
            got.iter().map(|(_, n)| n).sum::<u64>(),
            entries.len() as u64
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_isam_sorted_is_byte_identical_to_write_isam() {
        // Feeding a pre-sorted stream to write_isam_sorted must reproduce exactly
        // the bytes write_isam emits after its in-RAM sort (deterministic zstd,
        // plaintext). This is the contract the external builder relies on.
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999", "Adams-2001"];
        for id in 0..500u64 {
            entries.push((Value::Str(sources[(id % 4) as usize].to_string()), id));
        }
        // Mix in some ints to exercise cross-type ordering at the boundary.
        for id in 500..600u64 {
            entries.push((Value::Int((id as i64) % 7), id));
        }

        let p1 = tmp("sorted_ref");
        write_isam(&p1, entries.clone(), 96, 3).unwrap();

        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
        let p2 = tmp("sorted_stream");
        write_isam_sorted(&p2, sorted.into_iter().map(Ok), 96, 3, None).unwrap();

        assert_eq!(
            std::fs::read(&p1).unwrap(),
            std::fs::read(&p2).unwrap(),
            "write_isam_sorted output differs from write_isam"
        );
        let _ = std::fs::remove_file(&p1);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn int_index_range_matches_linear_scan() {
        let path = tmp("intrange");
        let entries: Vec<(Value, u64)> = (0..400u64).map(|i| (Value::Int(i as i64), i)).collect();
        write_isam(&path, entries.clone(), 128, 3).unwrap();
        let r = IsamReader::open(&path).unwrap();

        // [100, 200) inclusive lo, exclusive hi.
        let got = r
            .lookup_range(Some(&Value::Int(100)), true, Some(&Value::Int(200)), false)
            .unwrap();
        let want: Vec<u64> = (100..200).collect();
        assert_eq!(got, want);

        // Unbounded below, <= 5.
        let got = r
            .lookup_range(None, true, Some(&Value::Int(5)), true)
            .unwrap();
        assert_eq!(got, (0..=5).collect::<Vec<_>>());

        // Unbounded above, > 395.
        let got = r
            .lookup_range(Some(&Value::Int(395)), false, None, true)
            .unwrap();
        assert_eq!(got, (396..400).collect::<Vec<_>>());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_index_roundtrips_and_refuses_wrong_or_absent_key() {
        let path = tmp("enc");
        let cipher = Arc::new(BlockCipher::from_master(b"isam-master", &[5u8; 32]));
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999"];
        for id in 0..600u64 {
            entries.push((Value::Str(sources[(id % 3) as usize].to_string()), id));
        }
        // Tiny blocks → many encrypted blocks, each with its own nonce.
        write_isam_with_cipher(&path, entries.clone(), 64, 3, Some(cipher.clone())).unwrap();

        // Right key: lookups match a linear scan.
        let r = IsamReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert!(r.num_blocks() > 1);
        for s in sources {
            let key = Value::Str(s.to_string());
            assert_eq!(r.lookup_eq(&key).unwrap(), linear_eq(&entries, &key));
        }

        // The raw key values must not appear in plaintext on disk.
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes
            .windows("Whitehead-2024".len())
            .any(|w| w == b"Whitehead-2024"));

        // Absent key: refused at open.
        assert!(IsamReader::open(&path).is_err());

        // Wrong key: refused at open — the sealed top-level fails its tag check
        // before any lookup, so a wrong key never even sees the block directory.
        let wrong = Arc::new(BlockCipher::from_master(b"nope", &[5u8; 32]));
        assert!(IsamReader::open_with_cipher(&path, Some(wrong)).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
