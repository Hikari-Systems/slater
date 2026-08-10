// SPDX-License-Identifier: Apache-2.0
//! Round-trip for the build.
//!
//! Builds a small dump under both `--cluster=none` and `--cluster=ldg`, then
//! re-opens the `graph-format` readers. Because the build permutes node/edge ids
//! for on-disk locality, the assertions recover each node by a stable property
//! (`name`) and verify labels, properties, adjacency and the range index *relative
//! to the recovered ids* — proving the graph survived build → permute → emit
//! semantically intact. Further tests round-trip a brute-force vector index and an
//! `--encrypt`ed build (every store sealed at rest).

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::{NodeId, Value};
use graph_format::isam::IsamReader;
use graph_format::manifest::Manifest;
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::topology::TopologyReader;

// dump ids start at 100 (offset, contiguous) — exercises the dense resolver. No
// vector index in this dump; `external_build_routes_and_emits_a_vector_index` and
// the encrypted round-trip below cover the vector store. One node range index.
const DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE INDEX FOR (n:Concept) ON (n.name);
CREATE (:Person:__DumpVertex__ {__dump_id__: 100, name: 'Alice', age: 30});
CREATE (:Person:__DumpVertex__ {__dump_id__: 101, name: 'Bob', age: 25});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 102, name: 'Graphs'});
MATCH (a:__DumpVertex__ {__dump_id__: 100}), (b:__DumpVertex__ {__dump_id__: 102}) CREATE (a)-[:LIKES {since: 2020}]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 101}), (b:__DumpVertex__ {__dump_id__: 100}) CREATE (a)-[:KNOWS]->(b);
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_extrt_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

fn run_external(cluster: &str) {
    let work = unique_dir(cluster);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "social",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            cluster,
        ])
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build ({cluster}) failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("social");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());

    // Scratch must have been cleaned up on success.
    let leftover: Vec<_> = std::fs::read_dir(&graph_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".slater-scratch")
        })
        .collect();
    assert!(leftover.is_empty(), "scratch dir was not cleaned up");

    // --- MANIFEST + integrity ---
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.node_count, 3);
    assert_eq!(m.edge_count, 2);
    assert!(m.vector_indexes.is_empty());
    for f in &m.files {
        let got = graph_format::integrity::hash_file(gen_dir.join(&f.name)).unwrap();
        assert_eq!(got, f.blake3, "hash mismatch for {}", f.name);
    }

    // Recover each node's final id by its (unique) name.
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    assert_eq!(np.len(), 3);
    let mut id_of: HashMap<String, u64> = HashMap::new();
    for id in 0..m.node_count {
        let props = np.props(id).unwrap();
        if let Some(Value::Str(s)) = prop(&props, &m.property_keys, "name") {
            id_of.insert(s.clone(), id);
        }
    }
    assert_eq!(id_of.len(), 3, "all three names recovered");
    let (alice, bob, graphs) = (id_of["Alice"], id_of["Bob"], id_of["Graphs"]);

    // age survived on the Person nodes.
    assert_eq!(
        prop(&np.props(alice).unwrap(), &m.property_keys, "age"),
        Some(&Value::Int(30))
    );
    assert_eq!(
        prop(&np.props(bob).unwrap(), &m.property_keys, "age"),
        Some(&Value::Int(25))
    );

    // --- labels ---
    let nl = NodeLabelsReader::open(gen_dir.join("node_labels.blk")).unwrap();
    let labels_of = |id: u64| -> Vec<String> {
        nl.labels(id)
            .unwrap()
            .iter()
            .map(|i| m.labels[*i as usize].clone())
            .collect()
    };
    assert_eq!(labels_of(alice), vec!["Person"]);
    assert_eq!(labels_of(bob), vec!["Person"]);
    assert_eq!(labels_of(graphs), vec!["Concept"]);

    // --- topology: Alice -LIKES-> Graphs, Bob -KNOWS-> Alice ---
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    assert_eq!(topo.node_count(), 3);
    let reltype = |i: u32| m.reltypes[i as usize].clone();

    let a_out = topo.outgoing(NodeId(alice)).unwrap();
    assert_eq!(a_out.len(), 1);
    assert_eq!(a_out[0].neighbour.0, graphs);
    assert_eq!(reltype(a_out[0].reltype), "LIKES");

    let b_out = topo.outgoing(NodeId(bob)).unwrap();
    assert_eq!(b_out.len(), 1);
    assert_eq!(b_out[0].neighbour.0, alice);
    assert_eq!(reltype(b_out[0].reltype), "KNOWS");

    // Graphs has no outgoing, one incoming (from Alice).
    assert!(topo.outgoing(NodeId(graphs)).unwrap().is_empty());
    let g_in = topo.incoming(NodeId(graphs)).unwrap();
    assert_eq!(g_in.len(), 1);
    assert_eq!(g_in[0].neighbour.0, alice);

    // --- edge properties: the LIKES edge carries since=2020 ---
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    assert_eq!(ep.len(), 2);
    let likes_edge = a_out[0].edge.0;
    assert_eq!(
        prop(&ep.props(likes_edge).unwrap(), &m.property_keys, "since"),
        Some(&Value::Int(2020))
    );
    // KNOWS has no properties.
    let knows_edge = b_out[0].edge.0;
    assert!(ep.props(knows_edge).unwrap().is_empty());

    // --- range index: only the Concept node 'Graphs' is indexed ---
    assert_eq!(m.range_indexes.len(), 1);
    let ri = &m.range_indexes[0];
    assert_eq!(ri.name, "node_Concept_name");
    let isam = IsamReader::open(gen_dir.join(format!("range/{}.isam", ri.name))).unwrap();
    assert_eq!(
        isam.lookup_eq(&Value::Str("Graphs".into())).unwrap(),
        vec![graphs]
    );
    // Alice has a name but is a Person, so she is NOT in the Concept index.
    assert!(isam
        .lookup_eq(&Value::Str("Alice".into()))
        .unwrap()
        .is_empty());

    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn external_build_cluster_none_roundtrips() {
    run_external("none");
}

#[test]
fn external_build_cluster_ldg_roundtrips() {
    run_external("ldg");
}

// A dump with a brute-force node vector index, to exercise the external path's
// vecf32 routing (out of the column store) and vector-store emit.
const VEC_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 3, 'cosine');
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 0, title: 'First chunk', embedding: vecf32([1.0, 0.0, 0.0])});
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 1, title: 'Second chunk', embedding: vecf32([0.0, 1.0, 0.0])});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 2, name: 'Alpha'});
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

#[test]
fn external_build_routes_and_emits_a_vector_index() {
    use graph_format::manifest::{AnnMode, Metric};
    use graph_format::vectors::VectorStoreReader;

    let work = unique_dir("vec");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, VEC_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "docs",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build (vectors)");
    assert!(
        out.status.success(),
        "vector build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("docs");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();

    // The embedding was routed OUT of the column store (not a property key).
    assert!(!m.property_keys.iter().any(|k| k == "embedding"));

    // One brute-force cosine index over 2 vectors.
    assert_eq!(m.vector_indexes.len(), 1);
    let vi = &m.vector_indexes[0];
    assert_eq!(
        (vi.label.as_str(), vi.property.as_str()),
        ("Chunk", "embedding")
    );
    assert_eq!(vi.dim, 3);
    assert_eq!(vi.metric, Metric::Cosine);
    assert_eq!(vi.count, 2);
    assert_eq!(vi.mode, AnnMode::BruteForce);

    // The vector store round-trips both embeddings, keyed by their (final) node id.
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let title_of = |id: u64| match prop(&np.props(id).unwrap(), &m.property_keys, "title") {
        Some(Value::Str(s)) => s.clone(),
        _ => String::new(),
    };
    let vs = VectorStoreReader::open(gen_dir.join("vectors.f32.blk")).unwrap();
    let group = vs.group(vi.first_record, vi.count).unwrap();
    assert_eq!(group.len(), 2);
    for rec in &group {
        let want = match title_of(rec.node_id).as_str() {
            "First chunk" => vec![1.0, 0.0, 0.0],
            "Second chunk" => vec![0.0, 1.0, 0.0],
            other => panic!("unexpected vector node title {other:?}"),
        };
        assert_eq!(rec.vector, want);
    }

    let _ = std::fs::remove_dir_all(&work);
}

// A dump exercising every encrypted store at once: multi-label nodes with scalar /
// list properties, an edge with a property, a brute-force vector index, and a node
// range index. Built `--cluster none` so dump node `i` keeps dense id `i`.
const ENC_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 3, 'cosine');
CREATE INDEX FOR (n:Concept) ON (n.name);
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 0, title: 'First chunk', n: 10, tags: ['eu', 'ai'], embedding: vecf32([1.0, 0.0, 0.0])});
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 1, title: 'Second; with semicolon and \'quote\'', n: 20, embedding: vecf32([0.0, 1.0, 0.0])});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 2, name: 'Alpha'});
MATCH (a:__DumpVertex__ {__dump_id__: 0}), (b:__DumpVertex__ {__dump_id__: 2}) CREATE (a)-[:MENTIONS {w: 5}]->(b);
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

/// The same build, but `--encrypt`ed: every data block is sealed at rest, the
/// MANIFEST carries the KDF salt (never the key), and the readers round-trip the
/// data only when handed the derived cipher. Absent the key they refuse.
#[test]
fn external_encrypted_build_then_reopen_with_key() {
    use std::sync::Arc;

    use graph_format::crypto::{self, BlockCipher};
    use graph_format::vectors::VectorStoreReader;

    let work = unique_dir("enc");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, ENC_DUMP).unwrap();

    // A 32-byte master key, hex-encoded, handed to the build via an env var.
    let key_hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    std::env::set_var("SLATER_GOLDEN_ENC_KEY", key_hex);

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "eu_ai_act",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
            "--encrypt",
            "--key-env",
            "SLATER_GOLDEN_ENC_KEY",
        ])
        .output()
        .expect("run slater-build --encrypt");
    assert!(
        out.status.success(),
        "encrypted build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("eu_ai_act");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());

    // The MANIFEST records the AEAD/KDF + salt, but never the key.
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    let header = m.encryption.as_ref().expect("encryption header present");
    assert_eq!(header.aead, crypto::AEAD_NAME);
    assert_eq!(header.kdf, crypto::KDF_NAME);
    assert!(!header.salt_hex.is_empty());

    // The plaintext title must not be readable in the raw block file.
    let raw = std::fs::read(gen_dir.join("node_props.blk")).unwrap();
    assert!(!raw
        .windows(b"First chunk".len())
        .any(|w| w == b"First chunk"));

    // Derive the per-generation cipher exactly as the reader does, then re-open
    // every store and assert the data survived encrypt→build→decrypt unchanged.
    // `--cluster none` ⇒ dense id == dump id, so positional lookups are valid.
    let key = crypto::hex_decode(key_hex).unwrap();
    let salt = crypto::hex_decode_salt(&header.salt_hex, "the fixture generation").unwrap();
    let cipher = Some(Arc::new(BlockCipher::from_master(&key, &salt)));
    // HIK-140: the generation must declare the AAD scheme this build seals under, and
    // every store below is opened under a subkey bound to its store-relative name — this
    // is the end-to-end check that the builder and the reader agree on those names.
    assert_eq!(header.aad_scheme, crypto::AAD_SCHEME);

    let np = PropsReader::open_with_cipher(
        gen_dir.join("node_props.blk"),
        crypto::file_cipher(&cipher, "node_props.blk"),
    )
    .unwrap();
    assert_eq!(np.len(), 3);
    assert_eq!(
        prop(&np.props(0).unwrap(), &m.property_keys, "title"),
        Some(&Value::Str("First chunk".into()))
    );
    assert_eq!(
        prop(&np.props(1).unwrap(), &m.property_keys, "title"),
        Some(&Value::Str("Second; with semicolon and 'quote'".into()))
    );

    let nl = NodeLabelsReader::open_with_cipher(
        gen_dir.join("node_labels.blk"),
        crypto::file_cipher(&cipher, "node_labels.blk"),
    )
    .unwrap();
    assert_eq!(nl.labels(2).unwrap().len(), 1);

    let topo = TopologyReader::open_with_cipher(
        gen_dir.join("topology.csr.blk"),
        crypto::file_cipher(&cipher, "topology.csr.blk"),
    )
    .unwrap();
    assert_eq!(topo.node_count(), 3);
    assert_eq!(
        topo.outgoing(NodeId(0)).unwrap()[0].neighbour.0,
        2,
        "Chunk 0 -MENTIONS-> Concept 2"
    );

    let vs = VectorStoreReader::open_with_cipher(
        gen_dir.join("vectors.f32.blk"),
        crypto::file_cipher(&cipher, "vectors.f32.blk"),
    )
    .unwrap();
    let vi = &m.vector_indexes[0];
    let group = vs.group(vi.first_record, vi.count).unwrap();
    assert_eq!(group[0].vector, vec![1.0, 0.0, 0.0]);

    let ri = &m.range_indexes[0];
    let isam_rel = format!("range/{}.isam", ri.name);
    let isam = IsamReader::open_with_cipher(
        gen_dir.join(&isam_rel),
        crypto::file_cipher(&cipher, &isam_rel),
    )
    .unwrap();
    assert_eq!(
        isam.lookup_eq(&Value::Str("Alpha".into())).unwrap(),
        vec![2]
    );

    // HIK-140, the whole-inventory check: **every** file the builder emitted must open —
    // and read its first block — under a subkey bound to the name the MANIFEST inventory
    // records for it. A writer that sealed a file under any other string fails here, so a
    // store added later cannot quietly drift from the reader's name for it.
    let mut checked = 0;
    for f in &m.files {
        let path = gen_dir.join(&f.name);
        let fc = crypto::file_cipher(&cipher, &f.name);
        if f.name.ends_with(".isam") {
            let r = IsamReader::open_with_cipher(&path, fc)
                .unwrap_or_else(|e| panic!("open {} bound to its inventory name: {e:#}", f.name));
            if r.num_blocks() > 0 {
                r.lookup_eq(&Value::Str("Alpha".into())).unwrap();
            }
        } else {
            let r = graph_format::blockfile::BlockFileReader::open_with_cipher(&path, fc)
                .unwrap_or_else(|e| panic!("open {} bound to its inventory name: {e:#}", f.name));
            for b in 0..r.num_blocks() {
                r.read_block(graph_format::ids::BlockId(b as u32))
                    .unwrap_or_else(|e| panic!("read block {b} of {}: {e:#}", f.name));
            }
        }
        checked += 1;
    }
    assert!(checked >= 8, "expected a real inventory, checked {checked}");

    // Absent the key, the encrypted store is refused — not silently misread.
    assert!(PropsReader::open(gen_dir.join("node_props.blk")).is_err());

    let _ = std::fs::remove_dir_all(&work);
}

// The same vectors as VEC_DUMP, but the index is declared *after* the node data —
// where pass 1's header pre-scan can no longer see it. Routing is decided per node,
// in parallel, so this cannot be honoured; before it was caught, the build succeeded
// and produced an index descriptor with `count: 0` while every embedding sat in
// `node_props.blk`.
const LATE_VEC_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 0, title: 'First chunk', embedding: vecf32([1.0, 0.0, 0.0])});
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 1, title: 'Second chunk', embedding: vecf32([0.0, 1.0, 0.0])});
CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 3, 'cosine');
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

#[test]
fn a_vector_index_declared_after_node_data_fails_the_build() {
    let work = unique_dir("latevec");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, LATE_VEC_DUMP).unwrap();

    let run = |extra: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_slater-build"))
            .args(["--pk", "__dump_id__"])
            .args([
                "--input",
                input.to_str().unwrap(),
                "--graph",
                "docs",
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--cluster",
                "none",
            ])
            .args(extra)
            .output()
            .expect("run slater-build")
    };

    let out = run(&[]);
    assert!(
        !out.status.success(),
        "a late vector-index declaration must not build silently"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("declared after node data"),
        "unexpected failure: {err}"
    );
    // No generation was published, so nothing can read the half-built graph.
    assert!(!data_dir.join("docs").join("current").exists());

    // The sidecar is the documented escape hatch: the routing set no longer depends on
    // where in the dump the declaration sits, so the same input now builds and routes.
    let sidecar = work.join("vectors.json");
    std::fs::write(
        &sidecar,
        r#"[{"label": "Chunk", "property": "embedding", "dim": 3, "metric": "cosine"}]"#,
    )
    .unwrap();
    let out = run(&["--vector-index-json", sidecar.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "sidecar build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let graph_dir = data_dir.join("docs");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let m = Manifest::read_from_dir(graph_dir.join(gen.trim())).unwrap();
    assert_eq!(m.vector_indexes.len(), 1);
    assert_eq!(m.vector_indexes[0].count, 2, "both embeddings routed");

    let _ = std::fs::remove_dir_all(&work);
}

// The same nodes again, with **no** `CALL db.idx.vector.createNodeIndex` anywhere —
// the sidecar is the sole declaration. This is what the late-declaration bail tells
// an operator to do ("or declare the indexes with --vector-index-json") and what the
// manual recommends when a generator cannot emit the DDL first.
const SIDECAR_ONLY_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 0, title: 'First chunk', embedding: vecf32([1.0, 0.0, 0.0])});
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 1, title: 'Second chunk', embedding: vecf32([0.0, 1.0, 0.0])});
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

/// A sidecar-only declaration must build a real, populated index.
///
/// The sidecar used to feed pass 1's *routing* set alone, so every `vecf32` was moved
/// out of the node's property record — and then nothing collected it, because the
/// index descriptors were built from the dump's parsed `CALL` statements only. The
/// build exited 0 with `vector_indexes: null` and the embeddings in neither store:
/// silent, total loss, on the very path the late-declaration bail recommends.
///
/// Assert the **count**, never "the build succeeded" — a zero-count index is exactly
/// the failure this whole check exists to catch.
#[test]
fn a_sidecar_only_vector_index_is_built_and_populated() {
    let work = unique_dir("sidecaronly");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, SIDECAR_ONLY_DUMP).unwrap();

    let sidecar = work.join("vectors.json");
    std::fs::write(
        &sidecar,
        r#"[{"label": "Chunk", "property": "embedding", "dim": 3, "metric": "cosine"}]"#,
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "docs",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
            "--vector-index-json",
            sidecar.to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "sidecar-only build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("docs");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let m = Manifest::read_from_dir(graph_dir.join(gen.trim())).unwrap();
    assert_eq!(
        m.vector_indexes.len(),
        1,
        "the sidecar declared one index; the manifest advertises {}",
        m.vector_indexes.len()
    );
    assert_eq!(
        m.vector_indexes[0].count, 2,
        "both embeddings must be routed into the sidecar-declared index"
    );

    let _ = std::fs::remove_dir_all(&work);
}

/// `--key-stdin`: the key source the server uses when it spawns this binary for a
/// consolidation (HIK-157). Covers the flag's guard rails, which are what stop a
/// misconfigured invocation from silently publishing a plaintext image.
///
/// The happy path is covered end-to-end by
/// `slater::server::tests::a_production_consolidation_of_an_encrypted_graph_publishes_an_encrypted_generation`;
/// this pins the refusals, which that test cannot reach.
#[test]
fn key_stdin_guard_rails() {
    use std::process::Stdio;

    let work = unique_dir("keystdin");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();
    let data = work.join("data");

    let base = |extra: &[&str]| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_slater-build"));
        c.args(["--pk", "__dump_id__"])
            .args([
                "--input",
                input.to_str().unwrap(),
                "--graph",
                "social",
                "--data-dir",
                data.to_str().unwrap(),
                "--cluster",
                "none",
            ])
            .args(extra);
        c
    };
    let stderr_of = |mut c: Command| {
        let out = c.output().expect("run slater-build");
        assert!(!out.status.success(), "expected a refusal");
        String::from_utf8_lossy(&out.stderr).to_string()
    };

    // Without --encrypt it is a misconfiguration, not a silent plaintext build.
    let e = stderr_of(base(&["--key-stdin"]));
    assert!(e.contains("without --encrypt"), "unexpected: {e}");

    // Two sources is ambiguous — refuse rather than pick one.
    let e = stderr_of(base(&["--encrypt", "--key-stdin", "--key-env", "SOME_VAR"]));
    assert!(e.contains("only one of"), "unexpected: {e}");

    // --encrypt with no source at all must still name every source.
    let e = stderr_of(base(&["--encrypt"]));
    assert!(
        e.contains("--key-stdin"),
        "the error must offer the new source: {e}"
    );

    // An *empty* pipe under --encrypt is the dangerous case: it must fail closed, never
    // fall back to writing the image in the clear.
    let mut child = base(&["--encrypt", "--key-stdin"])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn slater-build");
    drop(child.stdin.take().unwrap()); // immediate EOF, nothing written
    let out = child.wait_with_output().expect("wait");
    assert!(
        !out.status.success(),
        "an empty key pipe under --encrypt must not produce an image"
    );
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("empty"), "unexpected: {e}");
    assert!(
        !data.join("social").join("current").exists(),
        "nothing may be published when the key never arrived"
    );

    let _ = std::fs::remove_dir_all(&work);
}

/// `--key-stdin` and `--input -` both consume stdin, so together the key read swallows
/// the dump. Found in adversarial review of HIK-157: without this guard the failure is a
/// hex-decode error on the *dump contents*, which tells the operator nothing.
#[test]
fn key_stdin_refuses_a_dump_on_stdin() {
    let work = unique_dir("keystdin_conflict");
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            "-",
            "--graph",
            "social",
            "--data-dir",
            work.join("data").to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .args(["--encrypt", "--key-stdin"])
        .output()
        .expect("run slater-build");
    assert!(!out.status.success());
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(
        e.contains("both read stdin"),
        "the refusal must explain the conflict: {e}"
    );
    let _ = std::fs::remove_dir_all(&work);
}

// A dump carrying graphiti's two full-text declarations verbatim (graphiti-core 0.29.3
// `graph_queries.get_fulltext_indices(FALKORDB)`), over seed rows that put the label and
// the relationship type in the symbol table.
const FULLTEXT_DUMP: &str = r#"CREATE INDEX FOR (n:Entity) ON (n.uuid);
CALL db.idx.fulltext.createNodeIndex(
                                                {
                                                    label: 'Entity',
                                                    stopwords: ['a', 'is', 'the']
                                                },
                                                'name', 'summary', 'group_id'
                                                );
CREATE FULLTEXT INDEX FOR ()-[e:RELATES_TO]-() ON (e.name, e.fact, e.group_id);
MERGE (n:Entity {uuid: 'a'}) SET n.name = 'Alice', n.summary = 'an engineer', n.group_id = 'g';
MERGE (n:Entity {uuid: 'b'}) SET n.name = 'Bob', n.summary = 'a baker', n.group_id = 'g';
MERGE (a:Entity {uuid: 'a'})-[r:RELATES_TO]->(b:Entity {uuid: 'b'}) SET r.name = 'KNOWS', r.fact = 'Alice knows Bob', r.group_id = 'g';
"#;

/// The declaration reaches the manifest with its property order and stopwords intact,
/// for both entities — the surface `SHOW INDEXES` reports and therefore the surface
/// `graphiti_slater`'s startup schema assertion checks.
#[test]
fn external_build_declares_fulltext_indexes() {
    use graph_format::fulltext::bm25::Bm25Params;
    use graph_format::fulltext::index::FulltextReader;
    use graph_format::fulltext::search::{search, FulltextQuery};
    use graph_format::manifest::EntityKind;

    let work = unique_dir("fulltext");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, FULLTEXT_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "docs",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build (fulltext)");
    assert!(
        out.status.success(),
        "fulltext build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("docs");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let m = Manifest::read_from_dir(graph_dir.join(gen.trim())).unwrap();
    m.verify_content_hash().unwrap();

    assert_eq!(m.fulltext_indexes.len(), 2, "one per declaration");

    let node = m
        .fulltext_indexes
        .iter()
        .find(|f| f.entity == EntityKind::Node)
        .expect("the Entity declaration");
    assert_eq!(node.label_or_type, "Entity");
    assert_eq!(
        node.properties,
        ["name", "summary", "group_id"],
        "declaration order is the field id the postings are keyed by"
    );
    assert_eq!(node.stopwords, ["a", "is", "the"]);
    assert_eq!(node.name, "node_Entity");

    let edge = m
        .fulltext_indexes
        .iter()
        .find(|f| f.entity == EntityKind::Edge)
        .expect("the RELATES_TO declaration");
    assert_eq!(edge.label_or_type, "RELATES_TO");
    assert_eq!(edge.properties, ["name", "fact", "group_id"]);
    assert_eq!(edge.name, "edge_RELATES_TO");

    // A manifest that declares no full-text index must not gain the key, or every
    // existing sealed MANIFEST in the estate stops verifying (see the manifest test
    // `a_pre_fulltext_manifest_parses_and_re_serialises_byte_identically`). Check the
    // real on-disk bytes, not just the parsed value.
    let gen_dir = graph_dir.join(gen.trim());
    let raw = std::fs::read_to_string(gen_dir.join("MANIFEST.json")).unwrap();
    assert!(
        raw.contains("fulltextIndexes"),
        "a declaring generation must record the key"
    );

    // ── the node index is populated, and searchable ──
    assert_eq!(node.doc_count, 2, "two Entity nodes carry text");
    assert!(node.avg_doc_len > 0.0);
    // The four files are in the inventory, so the content hash covers them.
    for suffix in [".ftd", ".ftm.blk", ".post.blk", ".docs.blk"] {
        let name = format!("fulltext/node_Entity{suffix}");
        assert!(
            m.files.iter().any(|f| f.name == name),
            "{name} must be in the generation inventory"
        );
        assert!(gen_dir.join(&name).exists(), "{name} must exist on disk");
    }

    let no_cipher = |_: &str| None;
    let r = FulltextReader::open(&gen_dir, "fulltext/node_Entity", false, &no_cipher).unwrap();
    assert_eq!(r.doc_count(), 2);

    let q = |term: &str| FulltextQuery {
        filters: Vec::new(),
        terms: vec![term.to_string()],
    };
    // "alice" is in Alice's `name`, and "engineer" in her `summary` — different fields of
    // the same document, which is what the multi-property declaration is for.
    for term in ["alice", "engineer"] {
        let hits = search(&r, &q(term), node.avg_doc_len, Bm25Params::default(), 10).unwrap();
        assert_eq!(hits.len(), 1, "one match for {term:?}: {hits:?}");
        // A hit resolves back to the node's dense id, which is what the query layer binds.
        let doc = r.doc(hits[0].doc).unwrap();
        assert_eq!(
            entity_name(&gen_dir, &m, doc.entity),
            "Alice",
            "the hit for {term:?} must resolve to Alice"
        );
    }
    // The stopword list from the declaration was applied at index time.
    assert!(
        r.term_metas("the").unwrap().is_empty(),
        "'the' was declared a stopword"
    );
    // `group_id` is an indexed field, which is what makes graphiti's @group_id filter work.
    let filtered = search(
        &r,
        &FulltextQuery {
            filters: vec![vec![(2, "g".to_string())]],
            terms: vec!["alice".to_string()],
        },
        node.avg_doc_len,
        Bm25Params::default(),
        10,
    )
    .unwrap();
    assert_eq!(
        filtered.len(),
        1,
        "the group filter resolves against a field"
    );

    let _ = std::fs::remove_dir_all(&work);
}

/// The clustered path visits nodes in *provisional* order and only sees them in ascending
/// final-id order at its sorted drain — which is why the gather happens there. A docid is a
/// rank in that order, so getting it wrong would hand out docids the postings are not
/// sorted by. This builds the same dump both ways and asserts the searches agree.
#[test]
fn fulltext_is_identical_under_both_cluster_modes() {
    use graph_format::fulltext::bm25::Bm25Params;
    use graph_format::fulltext::index::FulltextReader;
    use graph_format::fulltext::search::{search, FulltextQuery};

    let names = |mode: &str| -> Vec<String> {
        let work = unique_dir(&format!("ftclust-{mode}"));
        let data_dir = work.join("data");
        let input = work.join("dump.cypher");
        std::fs::write(&input, FULLTEXT_DUMP).unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
            .args([
                "--input",
                input.to_str().unwrap(),
                "--graph",
                "docs",
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--cluster",
                mode,
            ])
            .output()
            .expect("run slater-build");
        assert!(
            out.status.success(),
            "{mode} build failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let graph_dir = data_dir.join("docs");
        let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
        let gen_dir = graph_dir.join(gen.trim());
        let m = Manifest::read_from_dir(&gen_dir).unwrap();
        let desc = m
            .fulltext_indexes
            .iter()
            .find(|f| f.label_or_type == "Entity")
            .unwrap()
            .clone();
        let no_cipher = |_: &str| None;
        let r = FulltextReader::open(&gen_dir, "fulltext/node_Entity", false, &no_cipher).unwrap();

        // Resolve each hit to the node's `name`, so the comparison is over graph
        // identities rather than over docids — which are *expected* to differ between
        // the two clusterings, since the node ids themselves do.
        let hits = search(
            &r,
            &FulltextQuery {
                filters: Vec::new(),
                terms: vec!["alice".into(), "baker".into()],
            },
            desc.avg_doc_len,
            Bm25Params::default(),
            10,
        )
        .unwrap();
        assert_eq!(desc.doc_count, 2);
        let out: Vec<String> = hits
            .iter()
            .map(|h| entity_name(&gen_dir, &m, r.doc(h.doc).unwrap().entity))
            .collect();
        let _ = std::fs::remove_dir_all(&work);
        out
    };

    let none = names("none");
    let ldg = names("ldg");
    assert!(
        !none.is_empty(),
        "the fixture must actually match something"
    );
    assert_eq!(
        none, ldg,
        "clustering must not change which nodes a full-text search returns, or in what order"
    );
}

/// The `name` property of a node, read back out of the generation's property store — so
/// a full-text hit is checked against the actual node it names rather than an id.
fn entity_name(gen_dir: &std::path::Path, m: &Manifest, id: u64) -> String {
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    match prop(&np.props(id).unwrap(), &m.property_keys, "name") {
        Some(Value::Str(s)) => s.clone(),
        other => panic!("node {id} has no string name: {other:?}"),
    }
}
