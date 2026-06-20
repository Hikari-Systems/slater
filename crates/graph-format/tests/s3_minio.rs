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

use graph_format::integrity::sha256_base64;
use graph_format::store::s3::{S3Config, S3ObjectStore};
use graph_format::store::{FileIntegrity, ObjectStore};

fn config_or_skip() -> Option<S3Config> {
    let endpoint = std::env::var("SLATER_S3_TEST_ENDPOINT").ok()?;
    Some(S3Config {
        bucket: std::env::var("SLATER_S3_TEST_BUCKET").unwrap_or_else(|_| "slater".into()),
        region: "us-east-1".into(),
        endpoint: Some(endpoint),
        prefix: "itest".into(),
        path_style: true,
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
            },
        )
        .expect_err("verify_file with wrong sha256 must fail");
    assert!(
        format!("{err:#}").contains("SHA-256"),
        "expected a SHA-256 mismatch error, got: {err:#}"
    );

    eprintln!("S3/MinIO integration: all assertions passed");
}
