// SPDX-License-Identifier: Apache-2.0
//! Read-amp **correctness parity** for the segmented core over a real object-store backend.
//!
//! The read path is generic over [`graph_format::store::ObjectStore`], so the read-amp harness
//! (`slater::benchkit`) serves a stacked set through *any* store with no backend-specific code:
//! a stacked set is built on the local fs, mirrored byte-for-byte into the store, and read back
//! — the base + segment **block-miss counts must be identical** to the fs reader for every read
//! shape (read amplification is a format/read-path property, not a backend one). This is the
//! same parity the in-memory `read_amp_parity_fs_vs_object_store` unit test pins, now against a
//! real network backend to prove the S3/GCS read path actually serves the fold.
//!
//! Per the `s3-benchmark-methodology` note MinIO / the GCS emulator are **correctness-only**
//! (real *latency* is an EC2, in-region exercise) — this test asserts block counts, never time.
//!
//! Both arms are **skipped** unless their endpoint env var is set, so an ordinary `cargo test`
//! is unaffected. Run:
//!
//! ```text
//! # S3 (MinIO on :9100, bucket `slater`):
//! SLATER_S3_TEST_ENDPOINT=http://localhost:9100 SLATER_S3_TEST_BUCKET=slater \
//!   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!   cargo test -p slater --features testkit,s3 --test object_store_readamp -- --nocapture
//!
//! # GCS (fake-gcs-server on :4443, bucket `slater`):
//! SLATER_GCS_TEST_ENDPOINT=http://localhost:4443 SLATER_GCS_TEST_BUCKET=slater \
//!   cargo test -p slater --features testkit,gcs --test object_store_readamp -- --nocapture
//! ```
#![cfg(feature = "testkit")]

// Only the s3/gcs arms below use the shared harness, so gate it with them — under
// `testkit` alone (no backend) it is dead code, which `clippy -D warnings` rejects.
#[cfg(any(feature = "s3", feature = "gcs"))]
use graph_format::store::ObjectStore;
#[cfg(any(feature = "s3", feature = "gcs"))]
use slater::benchkit;

/// The four read shapes and a depth-4 stacked set, served once from the local fs and once
/// through `store` (mirrored in): the base + segment block-miss counts must match shape-for-shape.
#[cfg(any(feature = "s3", feature = "gcs"))]
fn assert_readamp_parity(store: &dyn ObjectStore, tag: &str) {
    let n: u64 = 4_000;
    let anchor = n / 3;
    let queries = [
        format!("MATCH (p:Person {{name:'p{anchor:07}'}}) RETURN p.age"),
        format!(
            "MATCH (p:Person {{name:'p{anchor:07}'}})-[:KNOWS]->()-[:KNOWS]->(q) RETURN q.name"
        ),
        "MATCH (p:Person) RETURN p.name LIMIT 500".to_string(),
        "MATCH (p:Person) RETURN count(p)".to_string(),
    ];

    // Build the stacked set on fs, then mirror it byte-for-byte into the store.
    let (root, graph) = benchkit::build_stacked(tag, n, 4);
    benchkit::mirror_fs_into_store(store, &root);

    for q in &queries {
        let fs = benchkit::read_amp_cold(&root, &graph, q);
        let st = benchkit::read_amp_cold_store(store, &graph, q);
        eprintln!(
            "  {q}\n    fs   base+seg = {}+{}\n    store base+seg = {}+{}",
            fs.base_blocks, fs.segment_blocks, st.base_blocks, st.segment_blocks
        );
        assert_eq!(
            (fs.base_blocks, fs.segment_blocks),
            (st.base_blocks, st.segment_blocks),
            "read-amp over the store must equal fs for query: {q}"
        );
    }

    std::fs::remove_dir_all(&root).ok();
}

/// S3 arm (MinIO). Skipped unless `SLATER_S3_TEST_ENDPOINT` is set.
#[cfg(feature = "s3")]
#[test]
fn s3_minio_readamp_parity() {
    use graph_format::store::s3::{S3Config, S3ObjectStore};

    let Ok(endpoint) = std::env::var("SLATER_S3_TEST_ENDPOINT") else {
        eprintln!("skipping s3_minio_readamp_parity: set SLATER_S3_TEST_ENDPOINT to run");
        return;
    };
    let cfg = S3Config {
        bucket: std::env::var("SLATER_S3_TEST_BUCKET").unwrap_or_else(|_| "slater".into()),
        region: "us-east-1".into(),
        endpoint: Some(endpoint),
        // A distinct prefix so a rerun never reads a prior run's objects.
        prefix: "readamp".into(),
        path_style: true,
        access_key: None,
        secret_key: None,
        session_token: None,
    };
    let store = S3ObjectStore::connect(&cfg).expect("connect S3/MinIO");
    eprintln!("=== S3 (MinIO) read-amp parity ===");
    assert_readamp_parity(&store, "s3_readamp");
}

/// GCS arm (fake-gcs-server). Skipped unless `SLATER_GCS_TEST_ENDPOINT` is set. Proves the same
/// store-agnostic harness serves GCS with no GCS-specific code — only the store constructor.
#[cfg(feature = "gcs")]
#[test]
fn gcs_emulator_readamp_parity() {
    use graph_format::store::gcs::{GcsConfig, GcsObjectStore};

    let Ok(endpoint) = std::env::var("SLATER_GCS_TEST_ENDPOINT") else {
        eprintln!("skipping gcs_emulator_readamp_parity: set SLATER_GCS_TEST_ENDPOINT to run");
        return;
    };
    let cfg = GcsConfig {
        bucket: std::env::var("SLATER_GCS_TEST_BUCKET").unwrap_or_else(|_| "slater".into()),
        prefix: "readamp".into(),
        endpoint: Some(endpoint),
        credentials_path: None,
        credentials_json: None,
        anonymous: true,
    };
    let store = GcsObjectStore::connect(&cfg).expect("connect GCS emulator");
    eprintln!("=== GCS (emulator) read-amp parity ===");
    assert_readamp_parity(&store, "gcs_readamp");
}
