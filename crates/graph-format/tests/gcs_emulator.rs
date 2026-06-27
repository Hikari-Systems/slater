// SPDX-License-Identifier: Apache-2.0
//! Integration test for the GCS backend against a real GCS emulator
//! (`fake-gcs-server`). Skipped unless `SLATER_GCS_TEST_ENDPOINT` is set, so an
//! ordinary `cargo test` (no emulator) is unaffected. Run with:
//!
//! ```text
//! # docker run -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443
//! # (and create the bucket, e.g. via the emulator's create-bucket API)
//! SLATER_GCS_TEST_ENDPOINT=http://localhost:4443 SLATER_GCS_TEST_BUCKET=slater \
//!   cargo test -p graph-format --features gcs --test gcs_emulator -- --nocapture
//! ```
//!
//! The emulator needs no credentials, so the test connects with
//! [`GcsConfig::anonymous`] set.
#![cfg(feature = "gcs")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use graph_format::integrity::crc32c_base64;
use graph_format::store::diskcache::{CachingObjectStore, DiskCache};
use graph_format::store::gcs::{GcsConfig, GcsObjectStore};
use graph_format::store::{FileIntegrity, ObjectStore, RandomReadAt};

fn config_or_skip() -> Option<GcsConfig> {
    let endpoint = std::env::var("SLATER_GCS_TEST_ENDPOINT").ok()?;
    Some(GcsConfig {
        bucket: std::env::var("SLATER_GCS_TEST_BUCKET").unwrap_or_else(|_| "slater".into()),
        prefix: "itest".into(),
        endpoint: Some(endpoint),
        credentials_path: None,
        credentials_json: None,
        anonymous: true,
    })
}

#[test]
fn gcs_roundtrip_and_crc32c_verify() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping gcs_emulator: set SLATER_GCS_TEST_ENDPOINT to run");
        return;
    };
    let store = GcsObjectStore::connect(&cfg).expect("connect GCS");

    let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    let crc = crc32c_base64(&data);
    let key = "g/u/node_props.blk";

    // PUT — the backend computes the CRC32C and GCS validates the body against it.
    store.put(key, &data, None).expect("put");

    // open() → length from object metadata.
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

    // verify_file reads GCS's stored CRC32C via get_object metadata and compares it
    // to the manifest value — correct passes, wrong is rejected (no body read).
    store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: "ignored",
                sha256: None,
                crc32c: Some(&crc),
            },
        )
        .expect("verify_file with correct crc32c");
    let wrong = crc32c_base64(b"some other content entirely");
    let err = store
        .verify_file(
            key,
            &FileIntegrity {
                size: data.len() as u64,
                blake3: "ignored",
                sha256: None,
                crc32c: Some(&wrong),
            },
        )
        .expect_err("verify_file with wrong crc32c must fail");
    assert!(
        format!("{err:#}").contains("CRC32C"),
        "expected a CRC32C mismatch error, got: {err:#}"
    );

    eprintln!("GCS/fake-gcs-server integration: all assertions passed");
}

/// An [`ObjectStore`] that counts positional reads reaching the inner store, so
/// the test can prove the disk cache absorbs the warm read instead of issuing a
/// second GCS read.
struct CountingStore {
    inner: GcsObjectStore,
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

/// End-to-end: the [`CachingObjectStore`] disk tier over a real GCS (emulator)
/// store serves a warm block from local disk, so the second read of the same block
/// does not issue a second GCS read.
#[test]
fn disk_cache_absorbs_warm_reads_over_gcs() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping disk_cache_absorbs_warm_reads_over_gcs: set SLATER_GCS_TEST_ENDPOINT");
        return;
    };
    let gcs = GcsObjectStore::connect(&cfg).expect("connect GCS");

    let key = "g/u/cached.blk";
    let data: Vec<u8> = (0..8000u32).map(|i| (i % 251) as u8).collect();
    gcs.put(key, &data, None).expect("put");

    // Unique temp dir for the cache (removed at the end).
    let cache_dir = std::env::temp_dir().join(format!(
        "slater-gcs-diskcache-itest-{:?}",
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&cache_dir);
    let cache = DiskCache::open(&cache_dir, 64 << 20).expect("open disk cache");

    let reads = Arc::new(AtomicUsize::new(0));
    let store = CachingObjectStore::new(
        Arc::new(CountingStore {
            inner: gcs,
            reads: reads.clone(),
        }),
        cache.clone(),
    );

    let obj = store.open(key).expect("open");
    let mut buf = vec![0u8; 1000];
    obj.read_exact_at(&mut buf, 500).expect("cold read");
    assert_eq!(buf, data[500..1500]);
    assert_eq!(reads.load(Ordering::SeqCst), 1, "cold read hits GCS");
    cache.flush();

    // Warm read of the same block: served from local disk, no second GCS read.
    let obj2 = store.open(key).expect("reopen");
    let mut buf2 = vec![0u8; 1000];
    obj2.read_exact_at(&mut buf2, 500).expect("warm read");
    assert_eq!(buf2, data[500..1500]);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        1,
        "warm read served from disk — no second GCS read"
    );

    let _ = std::fs::remove_dir_all(&cache_dir);
    eprintln!("GCS/fake-gcs-server disk-cache integration: all assertions passed");
}
