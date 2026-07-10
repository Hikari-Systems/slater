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

/// Bytes read per hashing pass iteration.
///
/// The old value was 64 KiB, which is too small twice over: it costs a syscall
/// every 64 KiB across a 16 GB file, and `blake3::Hasher::update_rayon` has nothing
/// to fan out across below a few hundred KiB, so a big-buffer read is what makes
/// the tree hash parallel at all.
const HASH_CHUNK: usize = 8 << 20;

/// Stream a file through BLAKE3 and, when `object_checksums` is set, SHA-256 and
/// CRC32C too — all in a single read pass. Returns `(blake3_hex, sha256_base64,
/// crc32c_base64)`.
///
/// BLAKE3 is the canonical content digest and is always computed. The other two
/// exist so a generation served from an object store can be verified against the
/// store's *server-computed* object checksum from a metadata request, with no body
/// read: SHA-256 is S3's `x-amz-checksum-sha256`, CRC32C is GCS's `crc32c`. A build
/// that never reaches an object store has no use for either, and SHA-256 is by far
/// the slowest of the three — it has no tree structure, so it is a hard serial floor
/// at roughly one core's throughput. Skipping it is most of this function's cost.
///
/// When they *are* wanted, the three run concurrently over one chunk rather than one
/// after another, so the wall time per chunk is `max(blake3, sha256+crc32c)` instead
/// of their sum.
pub fn hash_file_checksums(
    path: impl AsRef<Path>,
    object_checksums: bool,
) -> Result<(String, Option<String>, Option<String>)> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open for hashing {}", path.display()))?;
    let mut reader = BufReader::with_capacity(HASH_CHUNK, f);
    let mut b3 = blake3::Hasher::new();
    let mut sha = Sha256::new();
    let mut crc: u32 = 0;
    let mut buf = vec![0u8; HASH_CHUNK];
    loop {
        // `BufReader::read` returns at most one buffer's worth, but a short read is
        // legal; fill the chunk so `update_rayon` always sees a splittable buffer.
        let mut n = 0;
        while n < buf.len() {
            match reader.read(&mut buf[n..])? {
                0 => break,
                k => n += k,
            }
        }
        if n == 0 {
            break;
        }
        let chunk = &buf[..n];
        if object_checksums {
            let (_, c) = rayon::join(
                || b3.update_rayon(chunk),
                || {
                    sha.update(chunk);
                    crc32c::crc32c_append(crc, chunk)
                },
            );
            crc = c;
        } else {
            b3.update_rayon(chunk);
        }
    }
    let b3_hex = b3.finalize().to_hex().to_string();
    if !object_checksums {
        return Ok((b3_hex, None, None));
    }
    let sha_b64 = base64::engine::general_purpose::STANDARD.encode(sha.finalize());
    let crc_b64 = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
    Ok((b3_hex, Some(sha_b64), Some(crc_b64)))
}

/// Stream a file through BLAKE3 and return the lowercase hex digest.
pub fn hash_file(path: impl AsRef<Path>) -> Result<String> {
    Ok(hash_file_checksums(path, false)?.0)
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
        // Single-pass file hasher agrees with the in-memory helpers.
        let dir = std::env::temp_dir().join(format!("slater_crc_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("c.blk");
        std::fs::write(&p, bytes).unwrap();
        let (b3, sha, crc) = hash_file_checksums(&p, true).unwrap();
        assert_eq!(crc.unwrap(), b64);
        assert_eq!(sha.unwrap(), sha256_base64(bytes));
        assert_eq!(b3, hash_bytes(bytes));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Skipping the object checksums must not perturb the canonical digest — that
    /// is the whole premise of gating them on an object-store publish.
    #[test]
    fn skipping_object_checksums_leaves_blake3_unchanged() {
        let dir = std::env::temp_dir().join(format!("slater_skip_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("big.blk");
        // Larger than one HASH_CHUNK, so the multi-chunk `update_rayon` path runs.
        let bytes: Vec<u8> = (0..(HASH_CHUNK + 12345)).map(|i| (i % 251) as u8).collect();
        std::fs::write(&p, &bytes).unwrap();
        let (with, sha, crc) = hash_file_checksums(&p, true).unwrap();
        let (without, none_sha, none_crc) = hash_file_checksums(&p, false).unwrap();
        assert_eq!(with, without);
        assert_eq!(with, hash_bytes(&bytes));
        assert!(sha.is_some() && crc.is_some());
        assert!(none_sha.is_none() && none_crc.is_none());
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
