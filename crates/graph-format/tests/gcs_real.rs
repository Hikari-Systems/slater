// SPDX-License-Identifier: Apache-2.0
//! Integration test for the GCS backend against **real** Google Cloud Storage,
//! authenticating with Application Default Credentials (no endpoint override, not
//! anonymous). Skipped unless `SLATER_GCS_TEST_BUCKET_REAL` names a real bucket
//! the active ADC identity can read/write, so an ordinary `cargo test` is
//! unaffected. Run with:
//!
//! ```text
//! # one-time: gcloud auth application-default login
//! SLATER_GCS_TEST_BUCKET_REAL=slater-gcs-test \
//!   cargo test -p graph-format --features gcs --test gcs_real -- --nocapture
//! ```
//!
//! Objects are written under the `itest/` key prefix; the test does not delete
//! them (the `ObjectStore` trait has no delete) — clean up with
//! `gcloud storage rm -r gs://<bucket>/itest/` if desired.
#![cfg(feature = "gcs")]

use graph_format::integrity::crc32c_base64;
use graph_format::store::gcs::{GcsConfig, GcsObjectStore};
use graph_format::store::{FileIntegrity, ObjectStore};

fn config_or_skip() -> Option<GcsConfig> {
    let bucket = std::env::var("SLATER_GCS_TEST_BUCKET_REAL").ok()?;
    Some(GcsConfig {
        bucket,
        prefix: "itest".into(),
        // Real GCS: standard endpoint, ADC credentials (Workload Identity / GCE
        // metadata / `gcloud auth application-default login`).
        endpoint: None,
        credentials_path: None,
        credentials_json: None,
        anonymous: false,
    })
}

#[test]
fn gcs_real_roundtrip_and_crc32c_verify() {
    let Some(cfg) = config_or_skip() else {
        eprintln!("skipping gcs_real: set SLATER_GCS_TEST_BUCKET_REAL to run");
        return;
    };
    let store = GcsObjectStore::connect(&cfg).expect("connect real GCS via ADC");

    let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    let crc = crc32c_base64(&data);
    let key = "g/u/node_props.blk";

    // PUT — the backend computes the CRC32C and real GCS validates the body.
    store.put(key, &data, None).expect("put");

    // open() → length from object metadata (get_object).
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

    eprintln!("real GCS integration: all assertions passed");
}
