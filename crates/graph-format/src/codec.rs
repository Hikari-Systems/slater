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

/// Compress a raw block with zstd at the given level.
pub fn compress(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::stream::encode_all(raw, level).context("zstd compress block")
}

/// Decompress a zstd block. `raw_len_hint` pre-sizes the output buffer; it is an
/// optimisation only and need not be exact.
pub fn decompress(comp: &[u8], raw_len_hint: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(raw_len_hint);
    zstd::stream::copy_decode(comp, &mut out).context("zstd decompress block")?;
    Ok(out)
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
}
