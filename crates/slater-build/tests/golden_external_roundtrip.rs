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
    let salt = crypto::hex_decode(&header.salt_hex).unwrap();
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

    // Absent the key, the encrypted store is refused — not silently misread.
    assert!(PropsReader::open(gen_dir.join("node_props.blk")).is_err());

    let _ = std::fs::remove_dir_all(&work);
}
