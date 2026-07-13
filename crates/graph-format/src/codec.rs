// SPDX-License-Identifier: Apache-2.0
//! Block compression codec.
//!
//! All `.blk` files are split into fixed-size raw blocks, each compressed
//! independently with **zstd** (better decode speed than gzip at a comparable
//! ratio). Compression is per-block so the reader can decompress exactly the
//! blocks a query touches and no more.
//!
// DESIGN: the C-backed `zstd` crate is acceptable under the debian-slim image —
// it is self-contained and links statically, unlike rocksdb. If a fully musl /
// pure-Rust build is ever needed, `ruzstd` covers the decode path.

use anyhow::{Context, Result};
use std::io::{self, Write};

/// Compress a raw block with zstd at the given level.
pub fn compress(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::stream::encode_all(raw, level).context("zstd compress block")
}

/// Absolute ceiling on a single decompressed block, and on the stored bytes a reader
/// will read for one block.
///
/// Every length fed to [`decompress`] comes off disk — `blockfile`'s `DirEntry.raw_len`,
/// ISAM's top-level `raw_len`, the element count in a `plane` / `degree_ef` `zstd-dense`
/// record — and for a plaintext (unencrypted) generation an attacker with data-dir write
/// access can forge all of them. The declared length is therefore a claim to be checked,
/// not a number to allocate on.
///
/// What a *legitimate* block can be:
///
/// * `--block-size` (prop / label / topology / vector `.blk`) defaults to **256 KiB**, and
///   `--range-block-size` (ISAM leaves) to **16 KiB**;
/// * but [`crate::blockfile::BlockFileWriter::append_record`] flushes *after* appending, so
///   a block legitimately overshoots its target by **one oversized record** — and a record
///   is a whole node's CSR adjacency (a hub's entire edge list) or a whole property blob.
///   The biggest hub of the 91.6M-node / 1.53B-edge wikidata build is O(10M) incident edges
///   at ~6–12 varint bytes each: a ~100–150 MB single record. The worst *conceivable* record
///   is a node adjacent to every other node — for a 100M-node graph, ~0.7–1 GB;
/// * and the format's own ceiling is 4 GiB − 1, because `raw_len` is a `u32`.
///
/// 2 GiB is therefore deliberately generous: 8192× the default block size, and still ~2–3×
/// above a *universal hub* in a 100M-node graph, which is the largest record the format can
/// be driven to emit before the `u32` overflows. Nothing `slater-build` can produce, at any
/// supported `--block-size`, is rejected by it — a tighter cap would trade a real
/// availability risk for very little, because the ceiling is only the backstop here:
///
/// the operative bound on a decode is the block's **own declared `raw_len`** (256 KiB for a
/// normal block), which [`decompress`] enforces as a hard output cap. That is what kills the
/// amplification — a few-KB body can no longer inflate into gigabytes. To even reach this
/// ceiling an attacker must forge a directory entry that *declares* gigabytes, and an
/// attacker who can rewrite the data dir could simply store a gigabyte block outright.
pub const MAX_BLOCK_BYTES: usize = 2 << 30;

/// Ratio bounding the *pre-allocation* — never the decode. zstd on graph blocks runs ~3–5×,
/// but a block of mostly-empty CSR records can compress far harder, so a low clamp would
/// cost a `Vec` regrow on honest data. 64× keeps the hint useful for real blocks while
/// capping what a 40-byte body can talk us into reserving at 2.5 KiB rather than 4 GiB.
const MAX_CAPACITY_RATIO: usize = 64;

/// A block's on-disk length claim is not credible, or its body does not honour it.
///
/// Typed so callers classify it with `err.downcast_ref::<BlockSizeExceeded>()` rather than
/// matching the message text: this is a corrupt-or-hostile image, not an I/O hiccup, and a
/// caller (e.g. the degree-column fault handler, which falls back to the CSR on `Err`) may
/// want to tell the two apart.
#[derive(Debug, thiserror::Error)]
pub enum BlockSizeExceeded {
    /// The length read off disk is above [`MAX_BLOCK_BYTES`]. Refused before allocating.
    #[error("block declares {declared} bytes, above the {max}-byte block ceiling")]
    Declared { declared: usize, max: usize },
    /// The zstd body inflates past the raw length its own directory entry declares — a zip
    /// bomb. Refused mid-decode, so no more than `raw_len` bytes are ever materialised.
    #[error("zstd block expands past the {raw_len} raw bytes it declares")]
    Expanded { raw_len: usize },
}

/// Check an on-disk *stored* (compressed, possibly sealed) block length before allocating a
/// buffer for it. A compressed block cannot legitimately be larger than the raw ceiling.
pub fn check_stored_len(comp_len: usize) -> Result<()> {
    if comp_len > MAX_BLOCK_BYTES {
        return Err(BlockSizeExceeded::Declared {
            declared: comp_len,
            max: MAX_BLOCK_BYTES,
        }
        .into());
    }
    Ok(())
}

/// How many bytes it is worth reserving up front: no more than the declared raw length, what
/// the compressed body could plausibly justify, or the ceiling. Without the ratio term a
/// forged `raw_len` drives a multi-gigabyte `with_capacity` before a single byte is inflated.
/// Under-reserving only costs a regrow; over-reserving is the bug.
fn capacity_for(comp_len: usize, raw_len: usize) -> usize {
    raw_len
        .min(comp_len.saturating_mul(MAX_CAPACITY_RATIO))
        .min(MAX_BLOCK_BYTES)
}

/// Decompress a zstd block whose raw length the caller knows from the format (`raw_len`).
///
/// `raw_len` is a **hard bound**, not a hint: the decode is refused as soon as the output
/// would pass it, so a small body cannot inflate into gigabytes, and the pre-allocation is
/// clamped so a forged length cannot drive the allocation on its own. The output must come
/// out exactly `raw_len` bytes long — every caller in the format knows the exact raw length
/// of the block it is reading.
pub fn decompress(comp: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    decompress_capped(comp, raw_len, MAX_BLOCK_BYTES)
}

/// As [`decompress`], but with an explicit ceiling in place of [`MAX_BLOCK_BYTES`], so the
/// ceiling is testable without allocating a gigabyte.
pub fn decompress_capped(comp: &[u8], raw_len: usize, max_block_bytes: usize) -> Result<Vec<u8>> {
    if raw_len > max_block_bytes {
        return Err(BlockSizeExceeded::Declared {
            declared: raw_len,
            max: max_block_bytes,
        }
        .into());
    }
    let mut sink = BoundedSink {
        out: Vec::with_capacity(capacity_for(comp.len(), raw_len)),
        limit: raw_len,
        overflowed: false,
    };
    if let Err(e) = zstd::stream::copy_decode(comp, &mut sink) {
        // The sink's own refusal surfaces as an `io::Error` out of zstd; recover the typed
        // error rather than let a wrapped message stand in for it.
        if sink.overflowed {
            return Err(BlockSizeExceeded::Expanded { raw_len }.into());
        }
        return Err(anyhow::Error::new(e).context("zstd decompress block"));
    }
    if sink.out.len() != raw_len {
        anyhow::bail!(
            "zstd block decoded {} bytes, directory says {}",
            sink.out.len(),
            raw_len
        );
    }
    Ok(sink.out)
}

/// A `Write` sink that refuses the write which would take the output past `limit`, so a zip
/// bomb dies mid-decode with at most `limit` bytes materialised instead of inflating first
/// and being caught after.
struct BoundedSink {
    out: Vec<u8>,
    limit: usize,
    overflowed: bool,
}

impl Write for BoundedSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.out.len() + buf.len() > self.limit {
            self.overflowed = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                BlockSizeExceeded::Expanded {
                    raw_len: self.limit,
                },
            ));
        }
        self.out.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_compresses_and_restores() {
        // Repetitive, property-like payload compresses well.
        let raw = b"camelid camelid camelid \x00\x01\x02 source source source".repeat(64);
        let comp = compress(&raw, 3).unwrap();
        assert!(
            comp.len() < raw.len(),
            "expected compression to shrink payload"
        );
        let back = decompress(&comp, raw.len()).unwrap();
        assert_eq!(back, raw);
    }

    #[test]
    fn roundtrip_empty() {
        let comp = compress(b"", 3).unwrap();
        assert_eq!(decompress(&comp, 0).unwrap(), b"");
    }

    #[test]
    fn absurd_declared_length_is_refused_without_allocating() {
        // Regression (HIK-75): `raw_len` is an on-disk `u32` (blockfile/ISAM directory) or a
        // count read out of a plane/degree record, and a forged generation can say anything.
        // A 3 GiB claim on a 30-byte body used to be a 3 GiB `with_capacity` before zstd had
        // even looked at the frame.
        let comp = compress(b"a tiny block", 3).unwrap();
        let err = decompress(&comp, 3 << 30).expect_err("absurd raw_len must be refused");
        assert!(
            matches!(
                err.downcast_ref::<BlockSizeExceeded>(),
                Some(BlockSizeExceeded::Declared {
                    declared: 3_221_225_472,
                    max: MAX_BLOCK_BYTES
                })
            ),
            "expected a typed BlockSizeExceeded::Declared, got: {err}"
        );

        // And the pre-allocation itself is clamped by what the body could justify, so even a
        // claim *inside* the ceiling cannot drive the allocation on its own.
        assert_eq!(
            capacity_for(comp.len(), MAX_BLOCK_BYTES),
            comp.len() * MAX_CAPACITY_RATIO
        );

        // The same check guards the stored-length reads (`vec![0u8; comp_len]`).
        assert!(check_stored_len(MAX_BLOCK_BYTES).is_ok());
        assert!(check_stored_len(MAX_BLOCK_BYTES + 1)
            .unwrap_err()
            .downcast_ref::<BlockSizeExceeded>()
            .is_some());
    }

    #[test]
    fn zip_bomb_is_refused_mid_decode() {
        // Regression (HIK-75): zstd's RLE block type reaches ~32 000:1, so a body of a few KB
        // inflates into many GB. `copy_decode` into a `Vec` had no output cap at all, so the
        // expansion happened and *then* (maybe) someone noticed the length was wrong.
        let bomb = compress(&vec![0u8; 64 << 20], 3).unwrap();
        assert!(
            bomb.len() < 64 << 10,
            "expected the bomb body to be tiny, got {} bytes",
            bomb.len()
        );

        // Declared as a normal 256 KiB block: refused as soon as the output passes 256 KiB —
        // the 64 MiB is never materialised.
        let err = decompress(&bomb, 256 << 10).expect_err("zip bomb must be refused");
        assert!(
            matches!(
                err.downcast_ref::<BlockSizeExceeded>(),
                Some(BlockSizeExceeded::Expanded { raw_len: 262_144 })
            ),
            "expected a typed BlockSizeExceeded::Expanded, got: {err}"
        );

        // Truthfully declared, but the ceiling says no — and says so before allocating the
        // 64 MiB. (`decompress_capped` makes the ceiling testable without a 1 GiB buffer; the
        // production ceiling is `MAX_BLOCK_BYTES`.)
        let err = decompress_capped(&bomb, 64 << 20, 1 << 20)
            .expect_err("a block over the ceiling must be refused");
        assert!(
            matches!(
                err.downcast_ref::<BlockSizeExceeded>(),
                Some(BlockSizeExceeded::Declared {
                    declared: 67_108_864,
                    max: 1_048_576
                })
            ),
            "expected a typed BlockSizeExceeded::Declared, got: {err}"
        );

        // A truncated/mismatched body is still an error, but a plain one — not a bomb.
        let short = compress(b"short", 3).unwrap();
        let err = decompress(&short, 4096).expect_err("a short block must not pass silently");
        assert!(err.downcast_ref::<BlockSizeExceeded>().is_none());
    }

    #[test]
    fn legitimately_large_block_still_round_trips() {
        // The cap must not reject real data. A block can legitimately overshoot the 256 KiB
        // `--block-size` target by one oversized record — a hub node's whole CSR adjacency —
        // so shape one: 1M varint-ish adjacency entries, ~5 MB raw, 20× the block target.
        let mut raw = Vec::with_capacity(5 << 20);
        for i in 0..1_000_000u32 {
            raw.extend_from_slice(&i.to_le_bytes());
            raw.push((i % 251) as u8);
        }
        let comp = compress(&raw, 3).unwrap();
        let back = decompress(&comp, raw.len()).expect("a legitimately large block must decode");
        assert_eq!(back, raw);
    }
}
