// SPDX-License-Identifier: Apache-2.0
//! Storage backend abstraction.
//!
//! Every file in a generation is opened through an [`ObjectStore`] rather than
//! `std::fs` directly, so a generation can be served from the local filesystem
//! (the default, [`fs::FsObjectStore`]) or an alternate backend such as S3
//! without changing the on-disk byte format, the readers, or query semantics.
//!
//! The hot path is positional reads: a reader holds an [`RandomReadAt`] handle
//! to one object and fetches blocks with `read_exact_at(offset, len)` — which
//! maps directly onto a `pread` on a local file and an HTTP `Range` GET on an
//! object store. We deliberately do not mmap (see [`crate::blockfile`]); the
//! abstraction preserves that explicit, bounded-read model.
//!
//! Keys are backend-relative `/`-joined paths (e.g.
//! `"<graph>/<uuid>/topology.csr.blk"`); the store owns the root (an FS
//! directory, or a bucket + key prefix). Use [`join_key`] to build them so the
//! join rule is identical across backends.

use std::sync::Arc;

use anyhow::{Context, Result};

// The async-runtime bridge and the local-disk block cache are shared by every
// network backend (S3, GCS), so they compile when either is enabled.
#[cfg(any(feature = "s3", feature = "gcs"))]
pub mod asyncbridge;
#[cfg(any(feature = "s3", feature = "gcs"))]
pub mod diskcache;
pub mod fs;
#[cfg(feature = "gcs")]
pub mod gcs;
pub mod mem;
#[cfg(feature = "s3")]
pub mod s3;

/// A handle to one object that supports positional (random-access) reads.
///
/// Cheap to wrap in an `Arc` and share; a reader keeps one of these for the
/// lifetime of the open generation. Implementations must be safe to call from
/// many threads at once (each read is independent and stateless).
pub trait RandomReadAt: Send + Sync {
    /// Fill `buf` from the object starting at byte `offset`. Errors if fewer
    /// than `buf.len()` bytes are available at that offset (mirrors
    /// `std::os::unix::fs::FileExt::read_exact_at`).
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

    /// Total length of the object in bytes (known at open).
    fn len(&self) -> u64;

    /// True iff the object is empty. Provided to satisfy clippy and callers.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read a batch of `(offset, len)` byte ranges of *this* object, returning the
    /// bytes in the same order as `ranges`. The default reads them serially; a
    /// remote backend (S3) overrides this to issue the range GETs **concurrently**
    /// so their round-trips overlap — turning an N×RTT serial scan into roughly
    /// `N/concurrency × RTT`. Callers use it as a bounded read-ahead window (a
    /// fixed-size batch), so resident memory stays capped at the window, not the
    /// whole object — the latency win without an unbounded preload.
    fn read_ranges(&self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        ranges
            .iter()
            .map(|&(offset, len)| {
                let mut buf = vec![0u8; len as usize];
                self.read_exact_at(&mut buf, offset)?;
                Ok(buf)
            })
            .collect()
    }
}

/// A backend that resolves keys to objects and performs small whole-object and
/// directory operations used at generation open and discovery.
///
/// The trait is intentionally **synchronous**: the serve path runs inside
/// `tokio::task::spawn_blocking`, so a backend that talks to the network blocks
/// its worker thread on I/O (a parked thread, not CPU). Latency over a remote
/// backend is hidden by [`ObjectStore::prefetch`] at the coarse points where
/// many block offsets are known up front, plus the block cache above the
/// readers — not by making the executor async.
pub trait ObjectStore: Send + Sync {
    /// Open an object for positional reads (the hot path).
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>>;

    /// Read a whole (small) object into memory — the `current` pointer and
    /// `MANIFEST.json`. Not for large block files.
    fn read_all(&self, key: &str) -> Result<Vec<u8>>;

    /// List the immediate child names under `prefix` (one directory level, no
    /// recursion): graph directories under the root, index files under
    /// `range/` and `vector/`. Names are returned without the prefix. A missing
    /// prefix yields an empty list rather than an error.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Whether an object exists at `key`.
    fn exists(&self, key: &str) -> Result<bool>;

    /// Verify the object at `key` matches what the manifest asserts about it (the
    /// copy-completeness guard — catch a generation that landed half-published).
    ///
    /// The default reads the whole object and checks its BLAKE3 digest — exact,
    /// backend-agnostic, but O(bytes). A backend overrides this with the cheapest
    /// *native* check it can offer: the S3 backend compares the object's
    /// `Content-Length` from a `HEAD` (a metadata request, **no body read**),
    /// which is a sound completeness check there because an S3 `PUT` is atomic —
    /// a partial upload never produces a visible object, so "present and the right
    /// size" means "fully published". A backend that can have the store itself
    /// recompute a content digest (e.g. via an object checksum baked in at
    /// publish) can strengthen this without reading the body — see the S3 impl.
    fn verify_file(&self, key: &str, expected: &FileIntegrity) -> Result<()> {
        let src = self.open(key)?;
        let computed = crate::integrity::hash_object(src.as_ref())
            .with_context(|| format!("re-hash {key}"))?;
        if computed != expected.blake3 {
            anyhow::bail!(
                "object {key} failed its integrity check (manifest {}, on-disk {}) — \
                 refusing to serve an incomplete copy",
                expected.blake3,
                computed
            );
        }
        Ok(())
    }

    /// Write (or overwrite) an object. Used by the builder to publish a finished
    /// generation to the store. `sha256_b64` is the object's base64 SHA-256 when
    /// known (from the manifest inventory): the S3 backend sends it so S3
    /// validates the upload against it and stores it as the object checksum;
    /// other backends ignore it. The default refuses — a backend opts in by
    /// overriding (the filesystem, S3, and in-memory backends all do).
    fn put(&self, key: &str, _bytes: &[u8], _sha256_b64: Option<&str>) -> Result<()> {
        anyhow::bail!("storage backend is read-only; cannot write {key}")
    }

    /// Delete the object at `key`, tolerating an already-absent key (idempotent). Used by the
    /// segment/set GC sweep to reclaim the objects an orphaned segment or superseded set left in
    /// the store. The default refuses — a writable backend opts in by overriding (as it does for
    /// [`put`](Self::put)); a read-only backend never accumulates orphans to reclaim.
    fn delete(&self, key: &str) -> Result<()> {
        anyhow::bail!("storage backend is read-only; cannot delete {key}")
    }

    /// Whether this store is the plain local filesystem rooted at the data directory. A
    /// flush/build that writes objects directly under `data_dir` via `std::fs` has already
    /// published them through such a store, so no explicit `put` upload is required. Remote
    /// and in-memory backends return `false`, so a publisher must upload each object with
    /// [`put`](Self::put). Default `false` (assume an explicit upload is needed).
    fn is_local_fs(&self) -> bool {
        false
    }
}

/// What the manifest asserts about one generation file, passed to
/// [`ObjectStore::verify_file`] so each backend can check it the cheapest way it
/// can. `size` is the file's byte length; `blake3` is its content digest.
pub struct FileIntegrity<'a> {
    pub size: u64,
    pub blake3: &'a str,
    /// Base64 SHA-256 (the `x-amz-checksum-sha256` form), when the manifest
    /// records one. The S3 backend compares it to S3's server-computed object
    /// checksum; backends that ignore it fall back to `blake3` / `size`.
    pub sha256: Option<&'a str>,
    /// Base64 CRC32C (big-endian `u32`, the GCS `crc32c` form), when the manifest
    /// records one. The GCS backend compares it to GCS's server-computed object
    /// checksum; backends that ignore it fall back to `blake3` / `size`.
    pub crc32c: Option<&'a str>,
}

/// Join a base key and a child component with the canonical `/` separator,
/// avoiding a leading or doubled slash. Used everywhere a generation path is
/// built so every backend sees identical keys.
pub fn join_key(base: &str, child: &str) -> String {
    if base.is_empty() {
        child.to_string()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), child)
    }
}
