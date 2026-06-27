// SPDX-License-Identifier: Apache-2.0
//! Copy-completeness / integrity hashing.
//!
//! The MANIFEST records a BLAKE3 content hash over the generation's files. The
//! builder writes every file, fsyncs, computes the hash, then writes the MANIFEST
//! last. On open the reader recomputes the hash over the same inventory and
//! refuses to serve a generation whose contents do not match its MANIFEST — which
//! is exactly the failure mode of a generation half-copied onto the data dir
//! (which may be remote/network storage, e.g. an in-progress rsync onto NFS).

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use base64::Engine;
use sha2::{Digest, Sha256};

/// Base64 of a raw SHA-256 digest — the exact form S3 stores and returns as the
/// `x-amz-checksum-sha256` object checksum, so a value computed here can be sent
/// on upload (S3 verifies it against the bytes) and compared to S3's
/// server-computed checksum at open without reading the object body.
pub fn sha256_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(Sha256::digest(bytes))
}

/// Base64 of a CRC32C digest encoded as a big-endian `u32` — the exact form GCS
/// stores and returns as the object's `crc32c` checksum (also what `gcloud
/// storage objects describe` / `gsutil hash` print). A value computed here is
/// sent on upload (GCS validates the bytes against it) and compared to GCS's
/// server-computed checksum at open without reading the object body. The GCS
/// backend decodes it back to a `u32` for the comparison.
pub fn crc32c_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(crc32c::crc32c(bytes).to_be_bytes())
}

/// Stream a file through BLAKE3, SHA-256, AND CRC32C in a single read pass,
/// returning `(blake3_hex, sha256_base64, crc32c_base64)`. The builder uses this
/// for the inventory so each file is read once for the canonical content digest
/// (BLAKE3) and both server-comparable object checksums: SHA-256 (S3) and CRC32C
/// (GCS). The CRC32C is base64 of the digest as a big-endian `u32` — GCS's wire
/// form (see [`crc32c_base64`]).
pub fn hash_file_blake3_sha256_crc32c(path: impl AsRef<Path>) -> Result<(String, String, String)> {
    let f = File::open(path.as_ref())
        .with_context(|| format!("open for hashing {}", path.as_ref().display()))?;
    let mut reader = BufReader::new(f);
    let mut b3 = blake3::Hasher::new();
    let mut sha = Sha256::new();
    let mut crc: u32 = 0;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        b3.update(&buf[..n]);
        sha.update(&buf[..n]);
        crc = crc32c::crc32c_append(crc, &buf[..n]);
    }
    let sha_b64 = base64::engine::general_purpose::STANDARD.encode(sha.finalize());
    let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
    Ok((b3.finalize().to_hex().to_string(), sha_b64, crc_b64))
}

/// Stream a file through BLAKE3 and return the lowercase hex digest.
pub fn hash_file(path: impl AsRef<Path>) -> Result<String> {
    let f = File::open(path.as_ref())
        .with_context(|| format!("open for hashing {}", path.as_ref().display()))?;
    let mut reader = BufReader::new(f);
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Stream an object through BLAKE3 via positional reads and return the
/// lowercase hex digest. Identical result to [`hash_file`] over the same bytes,
/// so it works for any backend (local file or remote object) behind a
/// [`RandomReadAt`](crate::store::RandomReadAt) handle.
pub fn hash_object(src: &dyn crate::store::RandomReadAt) -> Result<String> {
    let len = src.len();
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut off = 0u64;
    while off < len {
        let n = ((len - off) as usize).min(buf.len());
        src.read_exact_at(&mut buf[..n], off)
            .context("read object chunk for hashing")?;
        hasher.update(&buf[..n]);
        off += n as u64;
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Hash an in-memory byte slice through BLAKE3 and return the lowercase hex
/// digest. Identical to [`hash_file`] for the same bytes, so a digest computed
/// over a file's contents in memory matches the one [`hash_file`] streams from
/// disk — used to bind an `acl.json` digest to the exact bytes just parsed.
pub fn hash_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Compute a single content hash over an ordered inventory of `(name, hex_hash)`
/// pairs. The name is folded in too, so a rename or reordering changes the
/// digest. The caller supplies the per-file hashes (typically from [`hash_file`]).
pub fn content_hash(inventory: &[(String, String)]) -> String {
    let mut hasher = blake3::Hasher::new();
    for (name, hash) in inventory {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(hash.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_hash_is_stable_and_content_sensitive() {
        let dir = std::env::temp_dir().join(format!("slater_int_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let a = dir.join("a.blk");
        std::fs::write(&a, b"hello camelid").unwrap();
        let h1 = hash_file(&a).unwrap();
        let h2 = hash_file(&a).unwrap();
        assert_eq!(h1, h2);
        std::fs::write(&a, b"hello camelidX").unwrap();
        assert_ne!(hash_file(&a).unwrap(), h1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crc32c_base64_is_big_endian_u32_and_round_trips() {
        // GCS returns the object's crc32c as a raw u32; the manifest stores the
        // big-endian-u32 base64 form. Decoding our base64 back to a u32 must equal
        // the raw crc — this is exactly the comparison the GCS backend performs, so
        // the test fences an endianness regression.
        let bytes = b"hello camelid";
        let raw = crc32c::crc32c(bytes);
        let b64 = crc32c_base64(bytes);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        assert_eq!(decoded.len(), 4);
        assert_eq!(u32::from_be_bytes(decoded.try_into().unwrap()), raw);
        // Single-pass file hasher agrees with the in-memory helper.
        let dir = std::env::temp_dir().join(format!("slater_crc_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("c.blk");
        std::fs::write(&p, bytes).unwrap();
        let (_b3, _sha, crc_b64) = hash_file_blake3_sha256_crc32c(&p).unwrap();
        assert_eq!(crc_b64, b64);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn content_hash_detects_reorder_and_change() {
        let inv1 = vec![
            ("a".to_string(), "11".to_string()),
            ("b".to_string(), "22".to_string()),
        ];
        let inv2 = vec![
            ("b".to_string(), "22".to_string()),
            ("a".to_string(), "11".to_string()),
        ];
        let inv3 = vec![
            ("a".to_string(), "11".to_string()),
            ("b".to_string(), "23".to_string()),
        ];
        assert_eq!(content_hash(&inv1), content_hash(&inv1));
        assert_ne!(content_hash(&inv1), content_hash(&inv2)); // order matters
        assert_ne!(content_hash(&inv1), content_hash(&inv3)); // a byte changed
    }
}
