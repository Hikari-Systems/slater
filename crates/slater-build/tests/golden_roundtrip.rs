//! Golden round-trip for `slater-build`.
//!
//! Builds a tiny but representative dump (multi-label nodes, a string array, an
//! escaped string, a `vecf32` embedding, a node range index, a node vector index,
//! one relationship, plus the marker/cleanup lines) by running the real
//! `slater-build` binary, then re-opens *every* `graph-format` reader against the
//! published generation and asserts the data survived the round-trip exactly.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use graph_format::columns::PropsReader;
use graph_format::crypto::{self, BlockCipher};
use graph_format::ids::Value;
use graph_format::isam::IsamReader;
use graph_format::manifest::{AnnMode, Manifest, Metric};
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::topology::TopologyReader;
use graph_format::vectors::VectorStoreReader;

const DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 3, 'cosine');
CREATE INDEX FOR (n:Concept) ON (n.name);
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 0, title: 'First chunk', n: 10, tags: ['eu', 'ai'], embedding: vecf32([1.0, 0.0, 0.0])});
CREATE (:Chunk:__DumpVertex__ {__dump_id__: 1, title: 'Second; with semicolon and \'quote\'', n: 20, embedding: vecf32([0.0, 1.0, 0.0])});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 2, name: 'Alpha'});
MATCH (a:__DumpVertex__ {__dump_id__: 0}), (b:__DumpVertex__ {__dump_id__: 2}) CREATE (a)-[:MENTIONS {w: 5}]->(b);
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_golden_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Find the property value for `key_name` in an entity's decoded props.
fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

#[test]
fn build_then_reopen_every_reader() {
    let work = unique_dir("rt");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "eu_ai_act",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Resolve current -> generation dir.
    let graph_dir = data_dir.join("eu_ai_act");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen = gen.trim();
    let gen_dir = graph_dir.join(gen);
    assert!(gen_dir.is_dir(), "generation dir {gen_dir:?} missing");

    // --- MANIFEST + integrity ------------------------------------------------
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.magic, "SLATER01");
    assert_eq!(m.node_count, 3);
    assert_eq!(m.edge_count, 1);
    assert_eq!(m.labels, vec!["Chunk", "Concept"]);
    assert_eq!(m.reltypes, vec!["MENTIONS"]);
    // Property keys, first-seen order; the routed-out embedding is NOT a key.
    assert_eq!(m.property_keys, vec!["title", "n", "tags", "name", "w"]);
    assert!(!m.property_keys.iter().any(|k| k == "embedding"));

    // Recompute every file hash matches the inventory (copy-completeness guard).
    for f in &m.files {
        let got = graph_format::integrity::hash_file(gen_dir.join(&f.name)).unwrap();
        assert_eq!(got, f.blake3, "hash mismatch for {}", f.name);
    }

    // --- node properties -----------------------------------------------------
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    assert_eq!(np.len(), 3);
    let n0 = np.props(0).unwrap();
    assert_eq!(
        prop(&n0, &m.property_keys, "title"),
        Some(&Value::Str("First chunk".into()))
    );
    assert_eq!(prop(&n0, &m.property_keys, "n"), Some(&Value::Int(10)));
    assert_eq!(
        prop(&n0, &m.property_keys, "tags"),
        Some(&Value::List(vec![
            Value::Str("eu".into()),
            Value::Str("ai".into())
        ]))
    );
    // The embedding is not in the column store.
    assert_eq!(prop(&n0, &m.property_keys, "embedding"), None);
    let n1 = np.props(1).unwrap();
    assert_eq!(
        prop(&n1, &m.property_keys, "title"),
        Some(&Value::Str("Second; with semicolon and 'quote'".into()))
    );
    let n2 = np.props(2).unwrap();
    assert_eq!(
        prop(&n2, &m.property_keys, "name"),
        Some(&Value::Str("Alpha".into()))
    );

    // --- node labels ---------------------------------------------------------
    let nl = NodeLabelsReader::open(gen_dir.join("node_labels.blk")).unwrap();
    let label_of = |id: u64| -> Vec<String> {
        nl.labels(id)
            .unwrap()
            .iter()
            .map(|i| m.labels[*i as usize].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(label_of(0), vec!["Chunk"]);
    assert_eq!(label_of(1), vec!["Chunk"]);
    assert_eq!(label_of(2), vec!["Concept"]);

    // --- edge properties -----------------------------------------------------
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    assert_eq!(ep.len(), 1);
    assert_eq!(
        prop(&ep.props(0).unwrap(), &m.property_keys, "w"),
        Some(&Value::Int(5))
    );

    // --- topology (CSR) ------------------------------------------------------
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    assert_eq!(topo.node_count(), 3);
    let out0 = topo.outgoing(graph_format::ids::NodeId(0)).unwrap();
    assert_eq!(out0.len(), 1);
    assert_eq!(out0[0].neighbour.0, 2);
    assert_eq!(m.reltypes[out0[0].reltype as usize], "MENTIONS");
    let in2 = topo.incoming(graph_format::ids::NodeId(2)).unwrap();
    assert_eq!(in2.len(), 1);
    assert_eq!(in2[0].neighbour.0, 0);
    assert!(topo
        .outgoing(graph_format::ids::NodeId(1))
        .unwrap()
        .is_empty());

    // --- vector index --------------------------------------------------------
    assert_eq!(m.vector_indexes.len(), 1);
    let vi = &m.vector_indexes[0];
    assert_eq!(vi.label, "Chunk");
    assert_eq!(vi.property, "embedding");
    assert_eq!(vi.dim, 3);
    assert_eq!(vi.metric, Metric::Cosine);
    assert_eq!(vi.count, 2);
    assert_eq!(vi.mode, AnnMode::BruteForce);
    let vs = VectorStoreReader::open(gen_dir.join("vectors.f32.blk")).unwrap();
    let group = vs.group(vi.first_record, vi.count).unwrap();
    assert_eq!(group.len(), 2);
    assert_eq!(group[0].node_id, 0);
    assert_eq!(group[0].vector, vec![1.0, 0.0, 0.0]);
    assert_eq!(group[1].node_id, 1);
    assert_eq!(group[1].vector, vec![0.0, 1.0, 0.0]);

    // --- range index ---------------------------------------------------------
    assert_eq!(m.range_indexes.len(), 1, "marker index must be dropped");
    let ri = &m.range_indexes[0];
    assert_eq!(ri.name, "node_Concept_name");
    let isam = IsamReader::open(gen_dir.join(format!("range/{}.isam", ri.name))).unwrap();
    assert_eq!(
        isam.lookup_eq(&Value::Str("Alpha".into())).unwrap(),
        vec![2]
    );
    assert!(isam
        .lookup_eq(&Value::Str("Nope".into()))
        .unwrap()
        .is_empty());

    let _ = std::fs::remove_dir_all(&work);
}

/// The same build, but `--encrypt`ed: every data block is sealed at rest, the
/// MANIFEST carries the KDF salt (never the key), and the readers round-trip the
/// data only when handed the derived cipher. Absent the key they refuse.
#[test]
fn encrypted_build_then_reopen_with_key() {
    let work = unique_dir("enc_rt");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    // A 32-byte master key, hex-encoded, handed to the build via an env var.
    let key_hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    std::env::set_var("SLATER_GOLDEN_ENC_KEY", key_hex);

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "eu_ai_act",
            "--data-dir",
            data_dir.to_str().unwrap(),
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
    let key = crypto::hex_decode(key_hex).unwrap();
    let salt = crypto::hex_decode(&header.salt_hex).unwrap();
    let cipher = Arc::new(BlockCipher::from_master(&key, &salt));

    let np = PropsReader::open_with_cipher(gen_dir.join("node_props.blk"), Some(cipher.clone()))
        .unwrap();
    assert_eq!(np.len(), 3);
    assert_eq!(
        prop(&np.props(0).unwrap(), &m.property_keys, "title"),
        Some(&Value::Str("First chunk".into()))
    );

    let nl =
        NodeLabelsReader::open_with_cipher(gen_dir.join("node_labels.blk"), Some(cipher.clone()))
            .unwrap();
    assert_eq!(nl.labels(2).unwrap().len(), 1);

    let topo =
        TopologyReader::open_with_cipher(gen_dir.join("topology.csr.blk"), Some(cipher.clone()))
            .unwrap();
    assert_eq!(topo.node_count(), 3);
    assert_eq!(
        topo.outgoing(graph_format::ids::NodeId(0)).unwrap()[0]
            .neighbour
            .0,
        2
    );

    let vs =
        VectorStoreReader::open_with_cipher(gen_dir.join("vectors.f32.blk"), Some(cipher.clone()))
            .unwrap();
    let vi = &m.vector_indexes[0];
    let group = vs.group(vi.first_record, vi.count).unwrap();
    assert_eq!(group[0].vector, vec![1.0, 0.0, 0.0]);

    let ri = &m.range_indexes[0];
    let isam = IsamReader::open_with_cipher(
        gen_dir.join(format!("range/{}.isam", ri.name)),
        Some(cipher),
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
