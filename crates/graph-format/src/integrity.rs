// SPDX-License-Identifier: Apache-2.0
//! Copy-completeness / integrity hashing.
//!
//! The MANIFEST records a BLAKE3 content hash over the generation's files. The
//! builder writes every file, fsyncs, computes the hash, then writes the MANIFEST
//! last. On open the reader recomputes the hash over the same inventory and
//! refuses to serve a generation whose contents do not match its MANIFEST — which
//! is exactly the failure mode of a half-copied generation rsync'd onto an NFS
//! mount.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};

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
