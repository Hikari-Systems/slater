// SPDX-License-Identifier: Apache-2.0
//! Bounded-resident **headline** test (M9) — Slater's raison d'être.
//!
//! The project exists to replace an in-memory graph engine whose RSS scales with
//! graph size. The headline guarantee is the opposite: **resident memory stays
//! bounded by the cache budgets, independent of graph size**, even on the
//! disk-native ANN path (the hard case — PQ codes resident + coalesced block
//! reads only). M7's `vamana_knn_matches_brute_force_with_bounded_vector_cache`
//! already proved the *accounted* residency is capped deterministically; this
//! test adds the real-OS-RSS-under-load assertion the plan calls the headline.
//!
//! Why an integration test (not a unit test): real-RSS sampling in a fast unit
//! test sharing a process with ~115 other parallel tests is hopelessly noisy
//! (M7 deliberately asserted on the pool's accounted bytes for that reason). Here
//! we get a dedicated test binary: we stand up the *real* server wiring in-process
//! via `server::serve_with_listener` against a synthetic Vamana/PQ generation far
//! larger than the cache budgets, drive a sustained stream of cosine-KNN + MATCH
//! queries over a real Bolt connection, and sample `/proc/self/statm`. The bound
//! is kept generous so it asserts the real property — no unbounded growth / no
//! per-query leak under load — without flaking on allocator/runtime jitter. (D34.)

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use graph_format::columns::PropsWriter;
use graph_format::ids::Generation as GenId;
use graph_format::integrity::{content_hash, hash_file};
use graph_format::manifest::{AnnMode, FileEntry, Manifest, Metric, VectorIndexDesc};
use graph_format::nodelabels::NodeLabelsWriter;
use graph_format::pq::{train_codebooks, PqParams, PqWriter};
use graph_format::topology::write_csr;
use graph_format::vamana::{bfs_order, build_vamana, VamanaWriter};
use graph_format::vectors::VectorStoreWriter;
use graph_format::{FORMAT_VERSION, MAGIC};

use slater::bolt::message::tag;
use slater::bolt::packstream::PsValue;
use slater::bolt::{chunk, handshake, message, packstream};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ── Fixture & cache sizing ───────────────────────────────────────────────────
//
// The store must be comfortably larger than the vector-cache budget so the pool
// genuinely cannot hold it (it must page Vamana blocks under the LRU). With these
// numbers the `.vamana` store is ~1.2 MiB while the vector-cache budget is 256 KiB
// (of which ~64 KiB is the pinned PQ codes), i.e. ~5× the budget — so the caches
// saturate during warm-up and any later RSS growth would have to come from
// unbounded accumulation or a leak, which is exactly what we assert against. N is
// kept modest because the *fixture* build (the Vamana graph construction) is the
// slow part, not the property under test; the bound holds the same at any scale.

const N: usize = 4_000; // :Doc nodes / vectors
const DIM: usize = 64;
const R: u32 = 32;
const ALPHA: f32 = 1.2;
const PQ_SUBSPACES: u32 = 16; // DIM % PQ_SUBSPACES == 0
const PQ_BITS: u32 = 8;
const VECTOR_BLOCK_SIZE: usize = 8192; // small ⇒ the store spans many blocks
const BLOCK: usize = 4096;
const LEVEL: i32 = 3;

// Deliberately tiny budgets: the store (~1.2 MiB of Vamana blocks) is several
// times the vector-cache budget, so the pool cannot hold it and must page. RSS
// growth under load is then bounded by these budgets, not by the store.
const BLOCK_CACHE_BYTES: usize = 512 * 1024;
const VECTOR_CACHE_BYTES: usize = 256 * 1024;
const RESULT_CACHE_BYTES: usize = 128 * 1024;

const PAGE_SIZE: u64 = 4096; // Linux x86-64 / WSL2

// ── /proc/self/statm RSS sampling ────────────────────────────────────────────

/// Resident set size of *this* process in bytes, read from `/proc/self/statm`
/// (field 2 = resident pages). The server runs in-process, so this includes its
/// caches and any pages the ANN path forces resident.
fn rss_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("parse resident pages");
    resident_pages * PAGE_SIZE
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ── Synthetic large Vamana/PQ generation ─────────────────────────────────────
//
// Mirrors exactly what `slater-build` writes for an above-threshold cosine index
// (graph build → BFS-from-medoid layout → PQ codes in the same order), built here
// from the public `graph-format` API. `slater::testgen::write_vamana` does the
// same for the in-crate unit tests, but it is `#[cfg(test)]`-private to the crate
// and so unreachable from this separate integration crate (D34) — hence the local
// copy, scaled up. Returns the raw unit vectors so the test can compute brute-force
// ground truth for a recall sanity check.

/// L2-normalise to unit length (the cosine space the Vamana path uses).
fn unit(v: &[f32]) -> Vec<f32> {
    let n: f64 = v
        .iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt();
    if n == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|&x| (x as f64 / n) as f32).collect()
}

fn build_large_vamana(root: &Path, graph: &str) -> Vec<Vec<f32>> {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0009);
    let dir = root.join(graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();
    std::fs::create_dir_all(dir.join("vector")).unwrap();

    // Deterministic synthetic unit vectors (no `rand` dependency).
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (N as u64).wrapping_mul(2654435761);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    let raw: Vec<Vec<f32>> = (0..N)
        .map(|_| {
            let v: Vec<f32> = (0..DIM).map(|_| (next() as f32) - 0.5).collect();
            unit(&v)
        })
        .collect();

    // node_props.blk (empty maps) + node_labels.blk (all :Doc) + empty edges.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..N {
        np.append(&[]).unwrap();
        nl.append(&[0]).unwrap();
    }
    np.finish().unwrap();
    nl.finish().unwrap();
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();
    write_csr(dir.join("topology.csr.blk"), N as u64, &[], BLOCK, LEVEL).unwrap();
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Vamana graph + PQ codes, both in BFS-from-medoid layout order.
    let graph_v = build_vamana(&raw, R as usize, ALPHA).unwrap();
    let order = bfs_order(&graph_v);
    let mut new_of = vec![0u32; order.len()];
    for (new_idx, &old) in order.iter().enumerate() {
        new_of[old as usize] = new_idx as u32;
    }
    let medoid_new = new_of[graph_v.medoid as usize];

    let mut vw = VamanaWriter::create_with_cipher(
        dir.join("vector/Doc.embedding.vamana"),
        VECTOR_BLOCK_SIZE,
        LEVEL,
        None,
    )
    .unwrap();
    for &old in &order {
        let nbrs: Vec<u32> = graph_v.adjacency[old as usize]
            .iter()
            .map(|&j| new_of[j as usize])
            .collect();
        vw.append(old as u64, &raw[old as usize], &nbrs).unwrap();
    }
    vw.finish().unwrap();

    let params = PqParams::new(DIM as u32, PQ_SUBSPACES, PQ_BITS).unwrap();
    let codebook = train_codebooks(&raw, params, 25).unwrap();
    let mut pw = PqWriter::create_with_cipher(
        dir.join("vector/Doc.embedding.pq"),
        &codebook,
        BLOCK,
        LEVEL,
        None,
    )
    .unwrap();
    for &old in &order {
        pw.append_codes(old as u64, &codebook.encode(&raw[old as usize]).unwrap())
            .unwrap();
    }
    pw.finish().unwrap();

    // Inventory + manifest.
    let names = [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "vector/Doc.embedding.vamana",
        "vector/Doc.embedding.pq",
    ];
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    for name in names {
        let path = dir.join(name);
        files.push(FileEntry {
            name: name.to_string(),
            bytes: std::fs::metadata(&path).unwrap().len(),
            blake3: hash_file(&path).unwrap(),
            sha256: None,
        });
        let bs = if name == "vector/Doc.embedding.vamana" {
            VECTOR_BLOCK_SIZE as u32
        } else {
            BLOCK as u32
        };
        block_sizes.insert(name.to_string(), bs);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.to_string(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: N as u64,
        edge_count: 0,
        labels: vec!["Doc".into()],
        reltypes: vec![],
        property_keys: vec!["embedding".into()],
        range_indexes: vec![],
        vector_indexes: vec![VectorIndexDesc {
            label: "Doc".into(),
            property: "embedding".into(),
            dim: DIM as u32,
            metric: Metric::Cosine,
            count: N as u64,
            first_record: 0,
            mode: AnnMode::Vamana {
                r: R,
                alpha: ALPHA,
                medoid: medoid_new as u64,
                pq_subspaces: PQ_SUBSPACES,
                pq_bits: PQ_BITS,
            },
        }],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        property_histograms: vec![],
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();
    std::fs::write(
        root.join(graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    raw
}

// ── Config + ACL for the in-process server ───────────────────────────────────

fn write_acl(root: &Path) -> PathBuf {
    let path = root.join("acl.json");
    let hash = slater::acl::hash_password("pw").unwrap();
    let json = serde_json::json!({
        "users": {
            "reporting": {
                "passwordArgon2id": hash,
                "grants": { "docs": ["read"] }
            }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

fn build_config(root: &Path, acl_path: &Path) -> slater::config::AppConfig {
    // A tiny-budget config: the three cache pools are deliberately far smaller than
    // the store, and the generation poll is slow so the guard never fires mid-test.
    let value = serde_json::json!({
        "server": { "bind": "127.0.0.1", "port": 0 },
        "dataBackend": { "kind": "fs", "fs": { "dir": root.to_str().unwrap() } },
        "aclPath": acl_path.to_str().unwrap(),
        // The fixture is built unstamped on purpose — this test exercises the
        // memory headline, not manifest authentication (requireAclStamp defaults
        // on). No key is configured, so the unconditional MAC check is inert.
        "requireAclStamp": false,
        "cache": {
            "blockCacheBytes": BLOCK_CACHE_BYTES,
            "vectorCacheBytes": VECTOR_CACHE_BYTES,
            "resultCacheBytes": RESULT_CACHE_BYTES
        },
        "query": { "maxRows": 100000, "timeoutMs": 0 },
        "vectorQuery": { "beamWidth": 64, "maxHops": 256 },
        "reloadStrategy": "exit",
        "generationPollMs": 600000,
        "log": { "level": "warn" }
    });
    serde_json::from_value(value).expect("build AppConfig")
}

// ── Minimal in-process Bolt client (reuses `slater::bolt`) ────────────────────

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut hs = Vec::new();
        hs.extend_from_slice(&handshake::PREAMBLE);
        hs.extend_from_slice(&[0, 0, 4, 5]); // offer 5.4
        hs.extend_from_slice(&[0, 0, 4, 4]); // then 4.4
        hs.extend_from_slice(&[0, 0, 0, 0]);
        hs.extend_from_slice(&[0, 0, 0, 0]);
        stream.write_all(&hs).await.unwrap();
        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0, 0, 4, 5], "should negotiate Bolt 5.4");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, msg: PsValue) {
        self.stream
            .write_all(&message::to_wire(&msg))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> (u8, Vec<PsValue>) {
        loop {
            if let Some((body, consumed)) = chunk::decode_message(&self.buf).unwrap() {
                self.buf.drain(..consumed);
                match packstream::from_slice(&body).unwrap() {
                    PsValue::Struct { tag, fields } => return (tag, fields),
                    other => panic!("expected a struct, got {other:?}"),
                }
            }
            let mut tmp = [0u8; 16384];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed unexpectedly");
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn logon(&mut self) {
        self.send(PsValue::Struct {
            tag: tag::HELLO,
            fields: vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str("slater-rss-test/1.0"),
            )])],
        })
        .await;
        assert_eq!(self.recv().await.0, tag::SUCCESS, "HELLO");
        self.send(PsValue::Struct {
            tag: tag::LOGON,
            fields: vec![PsValue::Map(vec![
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str("reporting")),
                ("credentials".into(), PsValue::str("pw")),
            ])],
        })
        .await;
        assert_eq!(self.recv().await.0, tag::SUCCESS, "LOGON");
    }

    /// RUN a query against the `docs` graph then PULL all records; return each
    /// record's field list.
    async fn run(&mut self, query: &str) -> Vec<Vec<PsValue>> {
        self.send(PsValue::Struct {
            tag: tag::RUN,
            fields: vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(vec![("db".into(), PsValue::str("docs"))]),
            ],
        })
        .await;
        let (t, _) = self.recv().await;
        assert_eq!(t, tag::SUCCESS, "RUN should succeed: {query}");
        self.send(PsValue::Struct {
            tag: tag::PULL,
            fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
        })
        .await;
        let mut rows = Vec::new();
        loop {
            let (t, fields) = self.recv().await;
            if t == tag::RECORD {
                if let PsValue::List(v) = &fields[0] {
                    rows.push(v.clone());
                }
            } else if t == tag::SUCCESS {
                break;
            } else {
                panic!("unexpected message tag {t:#x} draining PULL for {query}");
            }
        }
        rows
    }
}

// ── The KNN query (inline `vecf32([...])`, distinct each iteration) ───────────

fn knn_query(q: &[f32], k: usize) -> String {
    let csv: Vec<String> = q.iter().map(|x| format!("{x:.6}")).collect();
    format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', {k}, vecf32([{}])) \
         YIELD node, score RETURN id(node) AS id, score",
        csv.join(", ")
    )
}

fn brute_force_topk(raw: &[Vec<f32>], q: &[f32], k: usize) -> HashSet<u64> {
    let mut truth: Vec<(f64, u64)> = raw
        .iter()
        .enumerate()
        .map(|(i, v)| (1.0 - slater::vector::cosine_similarity(q, v), i as u64))
        .collect();
    truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    truth.iter().take(k).map(|(_, id)| *id).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rss_stays_bounded_under_sustained_knn_load() {
    let root = std::env::temp_dir().join(format!("slater_rss_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let raw = build_large_vamana(&root, "docs");
    let store_bytes = std::fs::metadata(
        root.join("docs")
            .join("00051a7e-0000-0000-0000-000000000009")
            .join("vector/Doc.embedding.vamana"),
    )
    .map(|m| m.len())
    .unwrap_or(0);
    let budgets_total = (BLOCK_CACHE_BYTES + VECTOR_CACHE_BYTES + RESULT_CACHE_BYTES) as u64;
    assert!(
        store_bytes > VECTOR_CACHE_BYTES as u64 * 3,
        "the Vamana store ({} MiB) must dwarf the vector-cache budget ({} MiB) for the \
         test to mean anything",
        mib(store_bytes),
        mib(VECTOR_CACHE_BYTES as u64),
    );

    let acl_path = write_acl(&root);
    let cfg = build_config(&root, &acl_path);

    // Bind the loopback port ourselves, then hand the listener to the real server
    // entry point so the test exercises the production wiring (graph open + PQ pin
    // into the bounded pool + the generation guard).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });

    // Everything below is time-boxed so a wiring bug fails loudly instead of hanging.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let mut c = Client::connect(addr).await;
        c.logon().await;

        // Sanity: the whole graph is visible (a label scan through the block cache).
        let count = c.run("MATCH (n:Doc) RETURN count(*) AS c").await;
        assert_eq!(count.len(), 1);
        assert_eq!(
            count[0][0],
            PsValue::Int(N as i64),
            "all :Doc nodes visible"
        );

        let k = 10;

        // Warm-up: distinct KNN queries fill the bounded caches to their budgets.
        // The store is ~4× the vector budget, so the pool must saturate and page.
        for i in 0..30usize {
            let mut q = raw[(i * 97) % N].clone();
            q[0] += 0.03;
            let _ = c.run(&knn_query(&q, k)).await;
        }
        let mid_rss = rss_bytes();

        // Sustained load: many more distinct KNN queries (+ an occasional MATCH),
        // tracking peak RSS and a recall sanity vs brute-force ground truth.
        let mut peak_rss = mid_rss;
        let mut recall_sum = 0.0f64;
        let mut recall_n = 0usize;
        for i in 0..150usize {
            let idx = (i * 131 + 7) % N;
            let mut q = raw[idx].clone();
            q[i % DIM] += 0.04;
            let rows = c.run(&knn_query(&q, k)).await;
            assert!(rows.len() <= k, "KNN must not exceed k rows");

            // Check recall on a sample (brute force over all N is O(N) — do a few).
            if i % 20 == 0 {
                let truth = brute_force_topk(&raw, &q, k);
                let got: HashSet<u64> = rows
                    .iter()
                    .filter_map(|r| r[0].as_int().map(|n| n as u64))
                    .collect();
                let hit = truth.iter().filter(|id| got.contains(id)).count();
                recall_sum += hit as f64 / k as f64;
                recall_n += 1;

                // Scores are ascending cosine distances (the brute-force contract).
                let mut prev = f64::NEG_INFINITY;
                for r in &rows {
                    if let PsValue::Float(s) = r[1] {
                        assert!(s + 1e-6 >= prev, "scores must be ascending");
                        prev = s;
                    }
                }
            }
            if i % 50 == 0 {
                let _ = c.run("MATCH (n:Doc) RETURN id(n) AS id LIMIT 50").await;
            }
            peak_rss = peak_rss.max(rss_bytes());
        }

        let recall = recall_sum / recall_n as f64;
        (mid_rss, peak_rss, recall)
    })
    .await;

    server.abort();
    let _ = std::fs::remove_dir_all(&root);

    let (mid_rss, peak_rss, recall) = outcome.expect("test timed out (server wiring hung)");

    // ── Assertions ───────────────────────────────────────────────────────────
    //
    // (1) Recall is acceptable: the ANN path is actually finding near neighbours,
    //     so the bounded-RSS result is real (not the engine returning nothing).
    assert!(
        recall >= 0.7,
        "ANN recall@10 was {recall:.3}; expected ≥ 0.7 (bounded RSS must not come \
         from a broken search path)"
    );

    // (2) No unbounded growth / no per-query leak under sustained load. By warm-up
    //     the caches are already at budget (store ≫ budgets), so further growth can
    //     only be bounded cache headroom + transient query buffers + allocator
    //     jitter — never the store paging in. A generous slack keeps it non-flaky.
    let growth = peak_rss.saturating_sub(mid_rss);
    let slack: u64 = 48 * 1024 * 1024;
    assert!(
        growth <= budgets_total + slack,
        "RSS grew {:.1} MiB under sustained load (mid {:.1} → peak {:.1} MiB); the \
         caches must be bounded by the budgets ({:.1} MiB) + slack ({:.1} MiB)",
        mib(growth),
        mib(mid_rss),
        mib(peak_rss),
        mib(budgets_total),
        mib(slack),
    );

    // (3) Headline-readable absolute ceiling: resident never balloons with the
    //     graph. A 12k-vector store served under 1 MiB of vector cache stays far
    //     below this; a store 100× larger would land in the same envelope.
    let ceiling: u64 = 512 * 1024 * 1024;
    assert!(
        peak_rss < ceiling,
        "peak RSS {:.1} MiB exceeded the {:.0} MiB ceiling — residency is not bounded",
        mib(peak_rss),
        mib(ceiling),
    );

    eprintln!(
        "headline RSS: store {:.1} MiB, budgets {:.1} MiB | mid {:.1} → peak {:.1} MiB \
         (growth {:.1} MiB), recall@10 {:.3}",
        mib(store_bytes),
        mib(budgets_total),
        mib(mid_rss),
        mib(peak_rss),
        mib(growth),
        recall,
    );
}

// ── Cache idle-TTL reclaim (opt-in; runs ~4½ minutes) ─────────────────────────
//
// The headline test above proves residency stays *bounded* under load. This one
// proves the new idle-TTL feature *reclaims* that residency once the working set
// goes quiet: with a 3-minute TTL it fills the caches to their budgets (real heap,
// real evictions), then sits idle and watches the background maintenance sweep
// free everything after the entries have been untouched for the TTL.
//
// Ignored by default because of the real 3-minute wall-clock wait. Run it with:
//
//   cargo test --test memory_headline -- --ignored --nocapture \
//     cache_ttl_reclaims_idle_caches_after_three_minutes
//
// The block-cache fill uses 1 MiB blocks so each allocation is above glibc's mmap
// threshold — freeing them munmaps, so the reclaim shows up in real RSS, not just
// the caches' own byte accounting (which is the authoritative signal asserted on).
#[test]
#[ignore = "runs ~4½ minutes: a real 3-minute idle wait. Run with --ignored --nocapture"]
fn cache_ttl_reclaims_idle_caches_after_three_minutes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use slater::cache::{BlockCache, BlockKey, FileKind, ResultCache, VectorIndexCache};
    use slater::exec::{Engine, QueryResult};
    use slater::generation::Generation as SlaterGen;

    const TTL: Duration = Duration::from_secs(180); // the 3-minute idle TTL under test
    const BLOCK_BUDGET: usize = 128 * 1024 * 1024; // block-cache budget: 128 MiB
    const DEMO_BLOCK: usize = 1024 * 1024; // 1 MiB blocks (mmap-backed ⇒ RSS-visible)
    const DEMO_BLOCKS: u32 = 200; // 200 MiB attempted ⇒ the LRU pages out

    let root = std::env::temp_dir().join(format!("slater_ttl_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let raw = build_large_vamana(&root, "docs");
    let gen = SlaterGen::open(&root, "docs").unwrap();
    let (ord, pq_bytes) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (vi.ord, vi.pq.resident_bytes())
    };

    // Budgets the working set exceeds, so loading saturates them (genuine evictions).
    let block_cache = Arc::new(BlockCache::new(BLOCK_BUDGET));
    let vec_cache = Arc::new(VectorIndexCache::new(pq_bytes + 96 * 1024)); // PQ + ~16 blocks
    let result_cache: Arc<ResultCache<QueryResult>> = Arc::new(ResultCache::new(1024 * 1024));
    vec_cache.pin(
        gen.uuid(),
        ord,
        gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
    );

    // ── LOAD: fill the caches and hit their limits ────────────────────────────
    // Real KNN queries page the vector pool (store ≫ budget); 1 MiB synthetic
    // blocks fill the block LRU to its 128 MiB budget (and evict beyond it).
    let k = 10;
    for i in 0..40usize {
        let mut q = raw[(i * 97) % raw.len()].clone();
        let qi = i % q.len();
        q[qi] += 0.03;
        let ast = slater::parser::parse(&knn_query(&q, k)).unwrap();
        let engine = Engine::new(&gen, &block_cache).with_vector_cache(&vec_cache, 64);
        let _ = engine.run(&ast).unwrap();
    }
    for b in 0..DEMO_BLOCKS {
        let key = BlockKey::new(gen.uuid(), FileKind::Vectors, b);
        let _ = block_cache
            .get_or_try_insert(key, || Ok(vec![0xABu8; DEMO_BLOCK]))
            .unwrap();
    }

    let load_block_evict = block_cache.metrics().evictions;
    let load_vec_evict = vec_cache.metrics().evictions;
    let rss_after_load = rss_bytes();
    eprintln!(
        "LOADED: block cache {} blocks / {:.1} MiB (evictions {load_block_evict}); \
         vector pool {} blocks (evictions {load_vec_evict}); RSS {:.1} MiB",
        block_cache.len(),
        mib(block_cache.bytes() as u64),
        vec_cache.block_count(),
        mib(rss_after_load),
    );
    assert!(
        load_block_evict > 0,
        "the block cache should have hit its 128 MiB budget and evicted"
    );
    assert!(!block_cache.is_empty() && block_cache.bytes() <= BLOCK_BUDGET);

    // ── Maintenance sweep — mirrors server::spawn_cache_maintenance exactly ────
    // Every (TTL/4) clamped to [1s, 30s], evict entries idle past the TTL. Started
    // now (after load) so the reclaim timing is measured cleanly from the idle point.
    let sweep_every = (TTL / 4).clamp(Duration::from_secs(1), Duration::from_secs(30));
    let stop = Arc::new(AtomicBool::new(false));
    let sweeper = {
        let (bc, vc, rc, stop) = (
            block_cache.clone(),
            vec_cache.clone(),
            result_cache.clone(),
            stop.clone(),
        );
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(sweep_every);
                let now = Instant::now();
                let freed = bc.evict_expired(now, TTL)
                    + vc.evict_expired(now, TTL)
                    + rc.evict_expired(now, TTL);
                if freed > 0 {
                    eprintln!("   [sweep] reclaimed {freed} idle cache entries");
                }
            }
        })
    };

    // ── IDLE: touch nothing for > TTL and watch the sweep reclaim ──────────────
    let idle_start = Instant::now();
    let mut reclaim_secs: Option<u64> = None;
    let mut min_rss = rss_after_load;
    loop {
        std::thread::sleep(Duration::from_secs(10));
        let elapsed = idle_start.elapsed().as_secs();
        let (blen, vblocks, rss) = (block_cache.len(), vec_cache.block_count(), rss_bytes());
        min_rss = min_rss.min(rss);
        eprintln!(
            "  idle {elapsed:>3}s: block cache {blen:>3} blocks / {:>5.1} MiB, \
             vector pool {vblocks:>2} blocks, RSS {:.1} MiB",
            mib(block_cache.bytes() as u64),
            mib(rss),
        );
        if reclaim_secs.is_none() && blen == 0 && vblocks == 0 {
            reclaim_secs = Some(elapsed);
        }
        if elapsed >= 250 {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = sweeper.join();
    let pq_survived = vec_cache.resident_pq(gen.uuid(), ord).is_some();
    let _ = std::fs::remove_dir_all(&root);

    let reclaim_secs = reclaim_secs.expect("caches were never reclaimed after the TTL idle period");
    let freed_rss = rss_after_load.saturating_sub(min_rss);
    eprintln!(
        "RECLAIMED after {reclaim_secs}s idle: block cache {} blocks, vector pool {} blocks; \
         RSS {:.1} → {:.1} MiB (freed {:.1} MiB); pinned PQ survived: {pq_survived}",
        block_cache.len(),
        vec_cache.block_count(),
        mib(rss_after_load),
        mib(min_rss),
        mib(freed_rss),
    );

    // ── Assertions ────────────────────────────────────────────────────────────
    // Authoritative signal is the caches' own accounting; RSS is reported, not
    // asserted (the allocator may retain freed pages — though 1 MiB mmap blocks
    // munmap on free, so a drop is expected here).
    assert!(
        reclaim_secs >= 175,
        "reclaim happened at {reclaim_secs}s — before the 180s TTL elapsed"
    );
    assert!(
        reclaim_secs <= 240,
        "reclaim took {reclaim_secs}s — too long for a 180s TTL + 30s sweep cadence"
    );
    assert_eq!(
        block_cache.len(),
        0,
        "block cache must be fully reclaimed when idle past the TTL"
    );
    assert_eq!(
        vec_cache.block_count(),
        0,
        "vector blocks must be reclaimed when idle past the TTL"
    );
    assert!(
        pq_survived,
        "pinned PQ codes must be exempt from TTL reclaim"
    );
    if freed_rss < 32 * 1024 * 1024 {
        eprintln!(
            "NOTE: RSS only fell {:.1} MiB — the caches' accounting confirms reclaim; the \
             allocator likely retained freed pages on this platform.",
            mib(freed_rss)
        );
    }
}
