// SPDX-License-Identifier: Apache-2.0
//! Integration test for the S3 backend against a real S3-compatible server
//! (MinIO). Skipped unless `SLATER_S3_TEST_ENDPOINT` is set, so an ordinary
//! `cargo test` (no MinIO) is unaffected. Run with:
//!
//! ```text
//! SLATER_S3_TEST_ENDPOINT=http://localhost:9100 SLATER_S3_TEST_BUCKET=slater \
//!   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!   cargo test -p graph-format --features s3 --test s3_minio -- --nocapture
//! ```
#![cfg(feature = "s3")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use graph_format::integrity::{hash_bytes, sha256_base64};
use graph_format::store::diskcache::{write_behind_budget, CachingObjectStore, DiskCache};
use graph_format::store::s3::{S3Config, S3ObjectStore};
use graph_format::store::{FileIntegrity, ObjectStore, RandomReadAt};

fn config_or_skip() -> Option<S3Config> {
    let endpoint = std::env::var("SLATER_S3_TEST_ENDPOINT").ok()?;
    Some(S3Config {
        bucket: std::env::var("SLATER_S3_TEST_BUCKET").unwrap_or_else(|_| "slater".into()),
        region: "us-east-1".into(),
        endpoint: Some(endpoint),
        prefix: "itest".into(),
        path_style: true,
        access_key: None,
        secret_key: None,
        session_token: None,
    })
}

#[test]
fn s3_roundtrip_and_sha256_verify() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping s3_minio: set SLATER_S3_TEST_ENDPOINT to run");
        return;
    };
    let store = S3ObjectStore::connect(&cfg).expect("connect S3");

    let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    let sha = sha256_base64(&data);
    let key = "g/u/node_props.blk";

    // PUT with the correct SHA-256 — S3 validates it against the body and stores it.
    store
        .put(key, &data, Some(&sha))
        .expect("put with checksum");

    // PUT with a deliberately wrong checksum — S3 must reject it.
    let wrong = sha256_base64(b"some other content entirely");
    let bad = store.put("g/u/bad.blk", &data, Some(&wrong));
    assert!(
        bad.is_err(),
        "S3 should reject a PUT whose x-amz-checksum-sha256 does not match the body"
    );

    // open() → length from HEAD.
    let obj = store.open(key).expect("open");
    assert_eq!(obj.len(), data.len() as u64);

    // Positional read of a middle slice.
    let mut buf = vec![0u8; 100];
    obj.read_exact_at(&mut buf, 1000).expect("read_exact_at");
    assert_eq!(buf, data[1000..1100]);

    // Concurrent batched range reads, returned in request order.
    let got = obj
        .read_ranges(&[(0, 10), (2000, 50), (4990, 10)])
        .expect("read_ranges");
    assert_eq!(got[0], data[0..10]);
    assert_eq!(got[1], data[2000..2050]);
    assert_eq!(got[2], data[4990..5000]);

    // Whole-object read.
    assert_eq!(store.read_all(key).expect("read_all"), data);

    // Existence + one-level listing.
    assert!(store.exists(key).expect("exists"));
    assert!(!store.exists("g/u/nope.blk").expect("exists nope"));
    let names = store.list("g/u").expect("list");
    assert!(
        names.contains(&"node_props.blk".to_string()),
        "list returned {names:?}"
    );

    // verify_file reads S3's stored SHA-256 via HEAD and compares it to the
    // manifest value — correct passes, wrong is rejected (no body read).
    store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: "ignored",
                sha256: Some(&sha),
                crc32c: None,
            },
        )
        .expect("verify_file with correct sha256");
    let err = store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: "ignored",
                sha256: Some(&wrong),
                crc32c: None,
            },
        )
        .expect_err("verify_file with wrong sha256 must fail");
    assert!(
        format!("{err:#}").contains("SHA-256"),
        "expected a SHA-256 mismatch error, got: {err:#}"
    );

    eprintln!("S3/MinIO integration: all assertions passed");
}

/// HIK-97 regression, end-to-end: an object whose manifest recorded a SHA-256 but
/// which S3 stores with **no** server SHA-256 (uploaded without a checksum, as an
/// out-of-band `aws s3 cp` under the default CRC64-NVME would leave it) must be
/// verified by a body re-hash against the manifest's BLAKE3 — it must **not** pass
/// on byte length alone. A same-length tampered overwrite must be rejected, which
/// is exactly the case the pre-fix length-only fallback let through.
#[test]
fn verify_rehashes_body_when_server_sha256_absent() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping s3_minio: set SLATER_S3_TEST_ENDPOINT to run");
        return;
    };
    let store = S3ObjectStore::connect(&cfg).expect("connect S3");

    let data: Vec<u8> = (0..4096u32).map(|i| (i % 253) as u8).collect();
    let key = "g/u/no_checksum.blk";
    // PUT WITHOUT a checksum → S3 stores no server SHA-256 for this object.
    store.put(key, &data, None).expect("put without checksum");

    let sha = sha256_base64(&data); // the manifest asked for a SHA-256 ...
    let good_blake3 = hash_bytes(&data); // ... and carries the canonical BLAKE3.

    // Correct content: the object has no server SHA-256 to compare the manifest's
    // against, so verify re-reads the body and matches it to BLAKE3 → passes.
    store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: &good_blake3,
                sha256: Some(&sha),
                crc32c: None,
            },
        )
        .expect("verify_file re-hashes the body and passes on a matching BLAKE3");

    // Tamper: overwrite with a DIFFERENT body of the SAME LENGTH, still with no
    // server SHA-256. The pre-fix length-only check would have PASSED this; the
    // body re-hash must reject it.
    let mut tampered = data.clone();
    tampered[0] ^= 0xFF;
    assert_eq!(
        tampered.len(),
        data.len(),
        "tamper preserves the byte length"
    );
    store
        .put(key, &tampered, None)
        .expect("overwrite with a tampered, same-length body");

    let err = store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: &good_blake3, // manifest still claims the ORIGINAL digest
                sha256: Some(&sha),
                crc32c: None,
            },
        )
        .expect_err("a same-length tampered body must be rejected, not passed on length");
    assert!(
        format!("{err:#}").contains("re-hash") || format!("{err:#}").contains("BLAKE3"),
        "expected a content re-hash failure, got: {err:#}"
    );

    eprintln!("S3/MinIO HIK-97: body re-hash on an absent server SHA-256 verified");
}

/// An [`ObjectStore`] that counts positional reads reaching the inner store, so
/// the test can prove the disk cache absorbs the warm read instead of issuing a
/// second S3 GET.
struct CountingStore {
    inner: S3ObjectStore,
    reads: Arc<AtomicUsize>,
}
struct CountingObj {
    inner: Arc<dyn RandomReadAt>,
    reads: Arc<AtomicUsize>,
}
impl RandomReadAt for CountingObj {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_exact_at(buf, offset)
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
    fn read_ranges(&self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        self.reads.fetch_add(ranges.len(), Ordering::SeqCst);
        self.inner.read_ranges(ranges)
    }
}
impl ObjectStore for CountingStore {
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        Ok(Arc::new(CountingObj {
            inner: self.inner.open(key)?,
            reads: self.reads.clone(),
        }))
    }
    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.read_all(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }
}

/// End-to-end: the [`CachingObjectStore`] disk tier over a real S3 (MinIO) store
/// serves a warm block from local disk, so the second read of the same block does
/// not issue a second S3 GET.
#[test]
fn disk_cache_absorbs_warm_reads_over_s3() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping disk_cache_absorbs_warm_reads_over_s3: set SLATER_S3_TEST_ENDPOINT");
        return;
    };
    let s3 = S3ObjectStore::connect(&cfg).expect("connect S3");

    let key = "g/u/cached.blk";
    let data: Vec<u8> = (0..8000u32).map(|i| (i % 251) as u8).collect();
    s3.put(key, &data, Some(&sha256_base64(&data)))
        .expect("put");

    // Unique temp dir for the cache (removed at the end).
    let cache_dir = std::env::temp_dir().join(format!(
        "slater-diskcache-itest-{:?}",
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    // Queue budget derived the way the server derives it, rather than picked —
    // this test is about S3 round-tripping, not shedding, so all that matters is
    // that it sits far above the handful of blocks the test queues.
    let cache = DiskCache::open(
        &cache_dir,
        64 << 20,
        write_behind_budget(64 << 20, 64 << 20),
    )
    .expect("open disk cache");

    let reads = Arc::new(AtomicUsize::new(0));
    let store = CachingObjectStore::new(
        Arc::new(CountingStore {
            inner: s3,
            reads: reads.clone(),
        }),
        cache.clone(),
    );

    let obj = store.open(key).expect("open");
    let mut buf = vec![0u8; 1000];
    obj.read_exact_at(&mut buf, 500).expect("cold read");
    assert_eq!(buf, data[500..1500]);
    assert_eq!(reads.load(Ordering::SeqCst), 1, "cold read hits S3");
    cache.flush();

    // Warm read of the same block: served from local disk, no second S3 GET.
    let obj2 = store.open(key).expect("reopen");
    let mut buf2 = vec![0u8; 1000];
    obj2.read_exact_at(&mut buf2, 500).expect("warm read");
    assert_eq!(buf2, data[500..1500]);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "warm read served from disk — no second S3 GET"
    );

    let _ = std::fs::remove_dir_all(&cache_dir);
    eprintln!("S3/MinIO disk-cache integration: all assertions passed");
}

/// Credentials supplied **explicitly** in [`S3Config`] (the config-driven path)
/// are honoured: a round-trip read works with the keys passed in `access_key`/
/// `secret_key` rather than relying on the ambient AWS env chain. The test reads
/// the MinIO keys from `SLATER_S3_TEST_ACCESS_KEY`/`SLATER_S3_TEST_SECRET_KEY`,
/// falling back to the standard `AWS_*` vars the MinIO harness already sets.
#[test]
fn s3_explicit_config_credentials() {
    let Some(mut cfg) = config_or_skip() else {
        eprintln!("skipping s3_explicit_config_credentials: set SLATER_S3_TEST_ENDPOINT");
        return;
    };
    let access = std::env::var("SLATER_S3_TEST_ACCESS_KEY")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY_ID"))
        .ok();
    let secret = std::env::var("SLATER_S3_TEST_SECRET_KEY")
        .or_else(|_| std::env::var("AWS_SECRET_ACCESS_KEY"))
        .ok();
    let (Some(access), Some(secret)) = (access, secret) else {
        eprintln!("skipping s3_explicit_config_credentials: no access/secret key in the env");
        return;
    };
    cfg.access_key = Some(access);
    cfg.secret_key = Some(secret);

    let store = S3ObjectStore::connect(&cfg).expect("connect S3 with explicit config credentials");
    let key = "g/u/explicit_creds.blk";
    let data: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
    store
        .put(key, &data, Some(&sha256_base64(&data)))
        .expect("put with explicit config credentials");
    assert_eq!(
        store.read_all(key).expect("read_all"),
        data,
        "round-trip read using config-supplied credentials"
    );
    eprintln!("S3/MinIO explicit-config-credentials integration: all assertions passed");
}
