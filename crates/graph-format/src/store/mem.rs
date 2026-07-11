// SPDX-License-Identifier: Apache-2.0
//! In-memory [`ObjectStore`] — a reference backend and test double.
//!
//! Holds every object as bytes in a map keyed by its full `/`-joined key. It is
//! the simplest possible non-filesystem backend, so a generation that opens
//! through it proves the readers and validation are genuinely backend-agnostic
//! (the same property an S3 backend relies on) without needing any network.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};

use super::{FileIntegrity, ObjectStore, RandomReadAt};

/// One in-memory object: shared bytes, read positionally.
pub struct MemObject {
    bytes: Arc<Vec<u8>>,
}

impl RandomReadAt for MemObject {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let start = offset as usize;
        let end = start
            .checked_add(buf.len())
            .context("read range overflow")?;
        if end > self.bytes.len() {
            bail!(
                "read past end of in-memory object ({}..{} of {})",
                start,
                end,
                self.bytes.len()
            );
        }
        buf.copy_from_slice(&self.bytes[start..end]);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

/// In-memory object store. Cheap to clone the bytes (they are `Arc`-shared on
/// open). Intended for tests and as the canonical reference implementation.
#[derive(Default)]
pub struct MemObjectStore {
    objects: RwLock<BTreeMap<String, Arc<Vec<u8>>>>,
}

impl MemObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ObjectStore for MemObjectStore {
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        let bytes = self
            .objects
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .with_context(|| format!("no such object {key}"))?;
        Ok(Arc::new(MemObject { bytes }))
    }

    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        let map = self.objects.read().unwrap();
        let bytes = map
            .get(key)
            .with_context(|| format!("no such object {key}"))?;
        Ok(bytes.as_ref().clone())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // Return the distinct immediate child names directly under `prefix`
        // (one level, no recursion), matching the filesystem `read_dir` shape.
        let norm = prefix.trim_end_matches('/');
        let head = if norm.is_empty() {
            String::new()
        } else {
            format!("{norm}/")
        };
        let mut seen = std::collections::BTreeSet::new();
        for key in self.objects.read().unwrap().keys() {
            let Some(rest) = key.strip_prefix(&head) else {
                continue;
            };
            if rest.is_empty() {
                continue;
            }
            let child = rest.split('/').next().unwrap_or(rest);
            seen.insert(child.to_string());
        }
        Ok(seen.into_iter().collect())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.read().unwrap().contains_key(key))
    }

    fn put(&self, key: &str, bytes: &[u8], _sha256_b64: Option<&str>) -> Result<()> {
        self.objects
            .write()
            .unwrap()
            .insert(key.to_string(), Arc::new(bytes.to_vec()));
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.objects.write().unwrap().remove(key); // idempotent — absent key is a no-op
        Ok(())
    }

    /// Mirror the S3 backend's verification so the SHA-256 path is exercisable
    /// without a network: when the manifest records a SHA-256, recompute it from
    /// the stored bytes (standing in for S3's server-computed object checksum) and
    /// compare; otherwise fall back to a size (copy-completeness) check.
    fn verify_file(&self, key: &str, expected: &FileIntegrity) -> Result<()> {
        let map = self.objects.read().unwrap();
        let bytes = map
            .get(key)
            .with_context(|| format!("no such object {key}"))?;
        match expected.sha256 {
            Some(want) => {
                let got = crate::integrity::sha256_base64(bytes);
                if got != want {
                    bail!("object {key} failed its SHA-256 integrity check (manifest {want}, store {got})");
                }
            }
            None => {
                if bytes.len() as u64 != expected.size {
                    bail!(
                        "object {key} failed its size check (manifest {} bytes, store {})",
                        expected.size,
                        bytes.len()
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FileIntegrity;

    #[test]
    fn verify_file_uses_sha256_when_present() {
        let s = MemObjectStore::new();
        let bytes = b"hello generation".to_vec();
        let sha = crate::integrity::sha256_base64(&bytes);
        let key = "g/u/node_props.blk";
        s.put(key, &bytes, Some(&sha)).unwrap();

        let size = bytes.len() as u64;
        // Matching checksum verifies.
        s.verify_file(
            key,
            &FileIntegrity {
                size,
                blake3: "ignored",
                sha256: Some(&sha),
                crc32c: None,
            },
        )
        .unwrap();
        // A wrong checksum is rejected.
        assert!(s
            .verify_file(
                key,
                &FileIntegrity {
                    size,
                    blake3: "ignored",
                    sha256: Some("AAAAAAAA"),
                    crc32c: None,
                },
            )
            .is_err());
        // Corrupted content (same claimed checksum) is rejected — the recomputed
        // digest no longer matches what the manifest recorded.
        s.put(key, b"hello generationX", Some(&sha)).unwrap();
        assert!(s
            .verify_file(
                key,
                &FileIntegrity {
                    size,
                    blake3: "ignored",
                    sha256: Some(&sha),
                    crc32c: None,
                },
            )
            .is_err());
    }
}
