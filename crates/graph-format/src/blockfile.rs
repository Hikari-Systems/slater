// SPDX-License-Identifier: Apache-2.0
//! Generic block-container file used by every `.blk` file in a generation.
//!
//! A block file packs many small, length-delimited **records** into fixed-size
//! raw blocks (default 256 KiB), compresses each block independently, and writes
//! a small **block directory** (block id → file offset + lengths) as a trailer.
//! The directory is tiny and is held resident by the reader at open; block bytes
//! are fetched with positional reads (`pread`) and decompressed on demand — we do
//! not mmap (over a remote/network filesystem such as NFS, mmap gives
//! close-to-open surprises and unpredictable eviction; we want explicit, bounded
//! caching instead).
//!
//! On-disk layout:
//! ```text
//! MAGIC(8) ‖ block_0 ‖ … ‖ block_{n-1} ‖ directory ‖ footer(24)
//! footer    = dir_offset:u64 ‖ dir_len:u64 ‖ block_count:u64   (little-endian)
//! directory = block_count × ( offset:u64 ‖ comp_len:u32 ‖ raw_len:u32 ‖ rec_count:u32 [‖ nonce:24] )
//! block raw = count:u32 ‖ (count+1)×offset:u32 ‖ record_bytes…   (then zstd-compressed)
//! ```
//! A record at `slot` is `data[off[slot]..off[slot+1]]`; the trailing offset is
//! the data length, so record boundaries need no separate length prefixes.
//!
//! When a block file is **encrypted** the magic is `SLBLKE01`, each directory
//! entry carries the block's 24-byte XChaCha20-Poly1305 nonce, and the on-disk
//! block bytes are the compressed block *sealed* with the AEAD (so `comp_len`
//! counts ciphertext = compressed + 16-byte tag). The per-generation key never
//! touches the file — only the nonces do (D28). Reading is decrypt-then-
//! decompress on a cache miss; the cache therefore holds plaintext bytes.

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec;
use crate::crypto::{BlockCipher, NONCE_LEN};
use crate::ids::BlockId;

const BLOCKFILE_MAGIC: &[u8; 8] = b"SLBLK001";
/// Magic for an AEAD-encrypted block file. The directory entries are wider (they
/// carry a per-block nonce) so the two formats are never confused.
const BLOCKFILE_MAGIC_ENC: &[u8; 8] = b"SLBLKE01";
const FOOTER_LEN: u64 = 24; // dir_offset(8) + dir_len(8) + block_count(8)
const DIR_ENTRY_LEN: usize = 20; // offset(8) + comp_len(4) + raw_len(4) + rec_count(4)
const DIR_ENTRY_LEN_ENC: usize = DIR_ENTRY_LEN + NONCE_LEN; // + per-block nonce

/// Location of a record: which block, and which slot within that block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordLoc {
    pub block: BlockId,
    pub slot: u32,
}

#[derive(Clone, Copy)]
struct DirEntry {
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    rec_count: u32,
    /// Per-block AEAD nonce, present iff the file is encrypted.
    nonce: Option<[u8; NONCE_LEN]>,
}

/// Streaming writer that packs records into compressed blocks.
pub struct BlockFileWriter {
    file: BufWriter<File>,
    target: usize,
    level: i32,
    offset: u64,
    dir: Vec<DirEntry>,
    cur_offsets: Vec<u32>,
    cur_data: Vec<u8>,
    /// When set, each compressed block is sealed with this cipher under a fresh
    /// per-block nonce before it is written.
    cipher: Option<Arc<BlockCipher>>,
}

impl BlockFileWriter {
    /// Create a new plaintext block file with the given target block size
    /// (bytes, raw) and zstd level.
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_inner(path, target_block_bytes, zstd_level, None)
    }

    /// Create a block file, optionally AEAD-encrypted. `cipher = None` writes the
    /// plaintext format (identical to [`BlockFileWriter::create`]) so existing
    /// fixtures and the golden test keep working unchanged.
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Self::create_inner(path, target_block_bytes, zstd_level, cipher)
    }

    fn create_inner(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let f = File::create(path.as_ref())
            .with_context(|| format!("create block file {}", path.as_ref().display()))?;
        let mut file = BufWriter::new(f);
        let magic = if cipher.is_some() {
            BLOCKFILE_MAGIC_ENC
        } else {
            BLOCKFILE_MAGIC
        };
        file.write_all(magic)?;
        Ok(Self {
            file,
            target: target_block_bytes.max(1),
            level: zstd_level,
            offset: magic.len() as u64,
            dir: Vec::new(),
            cur_offsets: vec![0],
            cur_data: Vec::new(),
            cipher,
        })
    }

    /// Append a record and return its location. Records are packed into the
    /// current block until it reaches the target size, then the block is flushed.
    pub fn append_record(&mut self, record: &[u8]) -> Result<RecordLoc> {
        let block = BlockId(self.dir.len() as u32);
        let slot = (self.cur_offsets.len() - 1) as u32;
        self.cur_data.extend_from_slice(record);
        self.cur_offsets.push(self.cur_data.len() as u32);
        if self.cur_data.len() >= self.target {
            self.flush_block()?;
        }
        Ok(RecordLoc { block, slot })
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.cur_data.is_empty() {
            return Ok(());
        }
        let count = (self.cur_offsets.len() - 1) as u32;
        let mut raw = Vec::with_capacity(4 + self.cur_offsets.len() * 4 + self.cur_data.len());
        raw.write_u32::<LittleEndian>(count)?;
        for off in &self.cur_offsets {
            raw.write_u32::<LittleEndian>(*off)?;
        }
        raw.extend_from_slice(&self.cur_data);

        let comp = codec::compress(&raw, self.level)?;
        // On-disk bytes are the compressed block, sealed with the AEAD when a
        // cipher is configured. `comp_len` then counts ciphertext (+16 tag).
        let (stored, nonce) = match &self.cipher {
            Some(cipher) => {
                let nonce = BlockCipher::random_nonce();
                let sealed = cipher.encrypt(&nonce, &comp)?;
                (sealed, Some(nonce))
            }
            None => (comp, None),
        };
        self.file.write_all(&stored)?;
        self.dir.push(DirEntry {
            offset: self.offset,
            comp_len: stored.len() as u32,
            raw_len: raw.len() as u32,
            rec_count: count,
            nonce,
        });
        self.offset += stored.len() as u64;
        self.cur_offsets.clear();
        self.cur_offsets.push(0);
        self.cur_data.clear();
        Ok(())
    }

    /// Flush the final block, write the directory and footer, and return the
    /// number of blocks written.
    pub fn finish(mut self) -> Result<u64> {
        self.flush_block()?;
        let entry_len = if self.cipher.is_some() {
            DIR_ENTRY_LEN_ENC
        } else {
            DIR_ENTRY_LEN
        };
        let dir_offset = self.offset;
        let mut dir_bytes = Vec::with_capacity(self.dir.len() * entry_len);
        for e in &self.dir {
            dir_bytes.write_u64::<LittleEndian>(e.offset)?;
            dir_bytes.write_u32::<LittleEndian>(e.comp_len)?;
            dir_bytes.write_u32::<LittleEndian>(e.raw_len)?;
            dir_bytes.write_u32::<LittleEndian>(e.rec_count)?;
            if let Some(nonce) = &e.nonce {
                dir_bytes.write_all(nonce)?;
            }
        }
        self.file.write_all(&dir_bytes)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.write_u64::<LittleEndian>(dir_offset)?;
        footer.write_u64::<LittleEndian>(dir_bytes.len() as u64)?;
        footer.write_u64::<LittleEndian>(self.dir.len() as u64)?;
        self.file.write_all(&footer)?;

        self.file.flush()?;
        // fsync so a generation survives an unclean shutdown / network-FS flush.
        self.file.get_ref().sync_all().context("fsync block file")?;
        Ok(self.dir.len() as u64)
    }
}

/// Reader holding the resident block directory; fetches blocks with `pread`.
pub struct BlockFileReader {
    file: File,
    dir: Vec<DirEntry>,
    /// Prefix sums of records-per-block: `block_start[i]` is the global index of
    /// block `i`'s first record. Length `dir.len() + 1`; the last entry is the
    /// total record count. Resident, `O(num_blocks)` — tiny.
    block_start: Vec<u64>,
    /// Per-generation cipher, set iff the file is encrypted. Refused at open if
    /// the file is encrypted and no key was supplied (absent-key refusal).
    cipher: Option<Arc<BlockCipher>>,
}

impl BlockFileReader {
    /// Open a plaintext block file. Refuses an encrypted file (no key supplied).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open a block file, validating the magic and loading the block directory.
    /// An encrypted file requires `cipher = Some(..)`; an encrypted file opened
    /// without a key is refused with a clear error rather than returning garbage.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let file = File::open(path.as_ref())
            .with_context(|| format!("open block file {}", path.as_ref().display()))?;
        let len = file.metadata()?.len();
        if len < BLOCKFILE_MAGIC.len() as u64 + FOOTER_LEN {
            bail!(
                "block file too short to be valid: {}",
                path.as_ref().display()
            );
        }

        let mut magic = [0u8; 8];
        file.read_exact_at(&mut magic, 0)?;
        let encrypted = match &magic {
            m if m == BLOCKFILE_MAGIC => false,
            m if m == BLOCKFILE_MAGIC_ENC => true,
            _ => bail!("bad block file magic in {}", path.as_ref().display()),
        };
        // Bind the cipher to what the file actually is: an encrypted file with no
        // key is refused; a plaintext file ignores any key it was handed.
        let cipher = if encrypted {
            match cipher {
                Some(c) => Some(c),
                None => bail!(
                    "block file {} is encrypted but no key was supplied",
                    path.as_ref().display()
                ),
            }
        } else {
            None
        };

        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact_at(&mut footer, len - FOOTER_LEN)?;
        let mut fr = &footer[..];
        let dir_offset = fr.read_u64::<LittleEndian>()?;
        let dir_len = fr.read_u64::<LittleEndian>()?;
        let block_count = fr.read_u64::<LittleEndian>()?;

        let mut dir_bytes = vec![0u8; dir_len as usize];
        file.read_exact_at(&mut dir_bytes, dir_offset)?;
        let mut dr = &dir_bytes[..];
        let mut dir = Vec::with_capacity(block_count as usize);
        let mut block_start = Vec::with_capacity(block_count as usize + 1);
        let mut acc = 0u64;
        for _ in 0..block_count {
            let offset = dr.read_u64::<LittleEndian>()?;
            let comp_len = dr.read_u32::<LittleEndian>()?;
            let raw_len = dr.read_u32::<LittleEndian>()?;
            let rec_count = dr.read_u32::<LittleEndian>()?;
            let nonce = if encrypted {
                let mut n = [0u8; NONCE_LEN];
                dr.read_exact(&mut n)?;
                Some(n)
            } else {
                None
            };
            block_start.push(acc);
            acc += rec_count as u64;
            dir.push(DirEntry {
                offset,
                comp_len,
                raw_len,
                rec_count,
                nonce,
            });
        }
        block_start.push(acc);
        Ok(Self {
            file,
            dir,
            block_start,
            cipher,
        })
    }

    /// Number of blocks in the file.
    pub fn num_blocks(&self) -> usize {
        self.dir.len()
    }

    /// Total number of records across all blocks.
    pub fn total_records(&self) -> u64 {
        *self.block_start.last().unwrap_or(&0)
    }

    /// Map a global record index (0-based, in append order) to its location.
    pub fn locate(&self, global: u64) -> Result<RecordLoc> {
        if global >= self.total_records() {
            bail!(
                "record index {global} out of range (have {})",
                self.total_records()
            );
        }
        // block_start is sorted ascending; find the block whose range contains `global`.
        let block = match self.block_start.binary_search(&global) {
            Ok(b) => b,      // exact match: first record of block b
            Err(b) => b - 1, // between starts: belongs to the previous block
        };
        Ok(RecordLoc {
            block: BlockId(block as u32),
            slot: (global - self.block_start[block]) as u32,
        })
    }

    /// Read the `global`-th record (append order) directly.
    pub fn read_record_global(&self, global: u64) -> Result<Vec<u8>> {
        let loc = self.locate(global)?;
        self.read_record(loc)
    }

    /// Read and decompress a whole block by id.
    pub fn read_block(&self, block: BlockId) -> Result<Vec<u8>> {
        let e = self
            .dir
            .get(block.index())
            .with_context(|| format!("block {} out of range", block.0))?;
        let mut stored = vec![0u8; e.comp_len as usize];
        self.file.read_exact_at(&mut stored, e.offset)?;
        // Decrypt-before-decompress on a (cache) miss: an encrypted file's bytes
        // are sealed compressed blocks. The cache above us keeps the plaintext
        // decompressed result, so this AEAD pass runs once per cold block.
        let comp = match (&self.cipher, &e.nonce) {
            (Some(cipher), Some(nonce)) => cipher.decrypt(nonce, &stored)?,
            _ => stored,
        };
        codec::decompress(&comp, e.raw_len as usize)
    }

    /// Read a single record by location (decompresses its block, copies the slot).
    pub fn read_record(&self, loc: RecordLoc) -> Result<Vec<u8>> {
        let raw = self.read_block(loc.block)?;
        let (offsets, data) = parse_block(&raw)?;
        let slot = loc.slot as usize;
        if slot + 1 >= offsets.len() {
            bail!(
                "record slot {} out of range in block {}",
                loc.slot,
                loc.block.0
            );
        }
        let start = offsets[slot] as usize;
        let end = offsets[slot + 1] as usize;
        Ok(data[start..end].to_vec())
    }

    /// Visit every record once, in ascending global order, decompressing each
    /// block a single time. `f` borrows each record (no per-record allocation).
    ///
    /// Use this for O(n) whole-file scans (e.g. building label / reltype
    /// postings at generation open). The per-record [`read_record_global`] path
    /// re-decompresses a record's *entire* block on every call, so scanning all
    /// N records that way does O(records-per-block) redundant zstd work per
    /// block — quadratic-feeling on a large store. This pass is O(total bytes).
    pub fn for_each_record(&self, mut f: impl FnMut(u64, &[u8]) -> Result<()>) -> Result<()> {
        for bi in 0..self.dir.len() {
            let raw = self.read_block(BlockId(bi as u32))?;
            let (offsets, data) = parse_block(&raw)?;
            let start = self.block_start[bi];
            for slot in 0..self.dir[bi].rec_count {
                let rec = record_from_block(&offsets, data, slot)?;
                f(start + slot as u64, rec)?;
            }
        }
        Ok(())
    }
}

/// Parse a decompressed block into its `(count+1)` slot offsets and the record
/// data region. Exposed so callers that already hold a cached decompressed block
/// can slice records without a second decode.
pub fn parse_block(raw: &[u8]) -> Result<(Vec<u32>, &[u8])> {
    let mut r = raw;
    let count = r.read_u32::<LittleEndian>()? as usize;
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..=count {
        offsets.push(r.read_u32::<LittleEndian>()?);
    }
    let header_len = 4 + (count + 1) * 4;
    Ok((offsets, &raw[header_len..]))
}

/// Slice the `slot`-th record out of an already-parsed block.
pub fn record_from_block<'a>(offsets: &[u32], data: &'a [u8], slot: u32) -> Result<&'a [u8]> {
    let slot = slot as usize;
    if slot + 1 >= offsets.len() {
        bail!("record slot out of range");
    }
    Ok(&data[offsets[slot] as usize..offsets[slot + 1] as usize])
}

/// Byte range of the `slot`-th record within the **whole** decompressed block
/// (header included), reading only `count` and the two bracketing slot offsets —
/// no offset-table allocation. A cache holder that keeps the `Arc`-owned block can
/// slice the record straight out by this range, so a scan touching N records pays
/// neither a per-record `to_vec()` copy nor a per-record `parse_block` allocation.
/// Bounds-checked against the block length so a corrupt block errors instead of
/// panicking.
pub fn record_range_in_block(raw: &[u8], slot: u32) -> Result<std::ops::Range<usize>> {
    let mut hdr = raw;
    let count = hdr.read_u32::<LittleEndian>()? as usize;
    let slot = slot as usize;
    if slot + 1 > count {
        bail!("record slot out of range");
    }
    let header_len = 4 + (count + 1) * 4;
    if raw.len() < header_len {
        bail!("truncated block header");
    }
    let off = |i: usize| -> usize {
        let at = 4 + i * 4;
        u32::from_le_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]) as usize
    };
    let start = header_len + off(slot);
    let end = header_len + off(slot + 1);
    if start > end || end > raw.len() {
        bail!("record range out of bounds");
    }
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_bf_{}_{}", std::process::id(), name))
    }

    #[test]
    fn many_records_roundtrip_across_blocks() {
        let path = tmp("many");
        // Small target forces multiple blocks.
        let mut w = BlockFileWriter::create(&path, 1024, 3).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..500u32 {
            let rec = format!("record-{i}-{}", "x".repeat((i % 40) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(
            nblocks > 1,
            "expected the small target to span multiple blocks"
        );

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.num_blocks() as u64, nblocks);
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn global_record_addressing_matches_append_order() {
        let path = tmp("global");
        let mut w = BlockFileWriter::create(&path, 512, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..300u32 {
            let rec = format!("n{i}:{}", "y".repeat((i % 30) as usize)).into_bytes();
            w.append_record(&rec).unwrap();
            expected.push(rec);
        }
        w.finish().unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), expected.len() as u64);
        // Every node id resolves to the record appended for it, no side table.
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&r.read_record_global(i as u64).unwrap(), want);
        }
        assert!(r.locate(expected.len() as u64).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn record_range_in_block_matches_record_from_block() {
        // The allocation-free range helper must slice the exact same bytes as the
        // parse_block + record_from_block path, for every slot in a real block.
        let path = tmp("range");
        let mut w = BlockFileWriter::create(&path, 1024, 3).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..200u32 {
            let rec = format!("r{i}:{}", "q".repeat((i % 50) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        w.finish().unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        for (loc, want) in locs.iter().zip(&expected) {
            let raw = r.read_block(loc.block).unwrap();
            let range = record_range_in_block(&raw, loc.slot).unwrap();
            assert_eq!(&raw[range], &want[..]);
        }
        // Out-of-range slot errors instead of panicking.
        let raw0 = r.read_block(locs[0].block).unwrap();
        assert!(record_range_in_block(&raw0, u32::MAX).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_file_opens_with_zero_blocks() {
        let path = tmp("empty");
        let w = BlockFileWriter::create(&path, 256 * 1024, 3).unwrap();
        let n = w.finish().unwrap();
        assert_eq!(n, 0);
        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.num_blocks(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_bad_magic() {
        let path = tmp("badmagic");
        std::fs::write(&path, vec![0u8; 64]).unwrap();
        assert!(BlockFileReader::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_records_roundtrip_across_blocks() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_many");
        let cipher = Arc::new(BlockCipher::from_master(b"master-key", &[7u8; 32]));
        // Small target forces multiple blocks, each with its own nonce.
        let mut w =
            BlockFileWriter::create_with_cipher(&path, 1024, 3, Some(cipher.clone())).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..500u32 {
            let rec = format!("secret-{i}-{}", "z".repeat((i % 40) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(nblocks > 1, "small target should span multiple blocks");

        // The right key reads every record back.
        let r = BlockFileReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert_eq!(r.num_blocks() as u64, nblocks);
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_block_bytes_are_not_plaintext_on_disk() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_ondisk");
        let cipher = Arc::new(BlockCipher::from_master(b"k", &[1u8; 32]));
        let mut w = BlockFileWriter::create_with_cipher(&path, 4096, 3, Some(cipher)).unwrap();
        let marker = b"PLAINTEXT-MARKER-camelid-camelid";
        w.append_record(marker).unwrap();
        w.finish().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(marker.len()).any(|w| w == marker),
            "the plaintext marker must not appear in the encrypted file"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn wrong_key_and_absent_key_are_refused() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_refuse");
        let right = Arc::new(BlockCipher::from_master(b"right", &[3u8; 32]));
        let mut w =
            BlockFileWriter::create_with_cipher(&path, 4096, 3, Some(right.clone())).unwrap();
        w.append_record(b"sensitive payload here").unwrap();
        w.finish().unwrap();

        // Absent key: refused at open with a clear error (not a panic).
        let err = match BlockFileReader::open(&path) {
            Ok(_) => panic!("expected an absent-key refusal"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("encrypted but no key"));

        // Wrong key: opens (the directory is plaintext) but a block read fails
        // cleanly at the AEAD tag check.
        let wrong = Arc::new(BlockCipher::from_master(b"wrong", &[3u8; 32]));
        let r = BlockFileReader::open_with_cipher(&path, Some(wrong)).unwrap();
        let err = r.read_record_global(0).unwrap_err();
        assert!(err.to_string().contains("wrong key"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn plaintext_file_opens_with_a_key_present() {
        use crate::crypto::BlockCipher;
        // A key supplied for a plaintext file is simply ignored — encryption is
        // optional, so a plaintext generation keeps opening even under a key.
        let path = tmp("plain_with_key");
        let mut w = BlockFileWriter::create(&path, 4096, 3).unwrap();
        w.append_record(b"plain record").unwrap();
        w.finish().unwrap();
        let cipher = Arc::new(BlockCipher::from_master(b"unused", &[9u8; 32]));
        let r = BlockFileReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert_eq!(&r.read_record_global(0).unwrap(), b"plain record");
        let _ = std::fs::remove_file(&path);
    }
}
