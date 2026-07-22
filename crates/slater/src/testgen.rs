// SPDX-License-Identifier: Apache-2.0
//! Shared test fixture: a small, representative generation built directly with
//! the `graph-format` writers (no dependency on the `slater-build` binary), used
//! by the planner and executor tests.
//!
//! The graph (dense node ids in brackets):
//! ```text
//! [0] Alice  :Person {name:'Alice', age:30, city:'London', team:'Red'}  (+ embedding → vec store)
//! [1] Bob    :Person {name:'Bob',   age:25, city:'London', team:'Red'}
//! [2] Carol  :Person {name:'Carol', age:40, city:'Paris'}
//! [3] Acme   :Company {name:'Acme'}
//! [4] Globex :Company {name:'Globex'}
//!
//! e0 (Alice)-[:KNOWS {since:2020}]->(Bob)
//! e1 (Bob)  -[:KNOWS]->(Carol)
//! e2 (Alice)-[:WORKS_AT]->(Acme)
//! e3 (Carol)-[:WORKS_AT]->(Globex)
//! e4 (Alice)-[:KNOWS]->(Carol)
//! ```
//! Symbol tables: labels Person(0)/Company(1); reltypes KNOWS(0)/WORKS_AT(1);
//! property keys name(0)/age(1)/city(2)/since(3)/embedding(4)/team(5). Range
//! indexes on (Person,name), (Person,age) and (Person,team) — the last with a
//! duplicate value (Alice/Bob='Red') and a node lacking it (Carol), so the
//! grouped-index fast path's run-length and null-group logic are covered. One
//! brute-force vector index on
//! (Person,embedding) holding the three Person embeddings (Alice/Bob/Carol), in
//! node order, so the KNN path has a real candidate set to rank.

#![cfg(any(test, feature = "testkit"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use graph_format::columns::PropsWriter;
use graph_format::crypto::{self, file_cipher, BlockCipher};
use graph_format::histogram::{
    derive_histogram_from_isam, encode_histogram, write_property_histograms,
};
use graph_format::ids::{EdgeId, Generation as GenId, NodeId, Value};
use graph_format::integrity::hash_file;
use graph_format::isam::write_isam;
use graph_format::isam::write_isam_with_cipher;
use graph_format::manifest::{
    AnnMode, EncryptionHeader, EntityKind, FileEntry, Manifest, Metric, PropertyHistogramDesc,
    RangeIndexDesc, VectorIndexDesc,
};
use graph_format::nodelabels::{NodeLabelsReader, NodeLabelsWriter};
use graph_format::pq::{normalise, train_codebooks, PqParams, PqWriter, HOLE};
use graph_format::topology::{write_csr, write_csr_with_cipher, Edge, TopologyReader};
use graph_format::vamana::{bfs_order, build_vamana, VamanaWriter};
use graph_format::vectors::VectorStoreWriter;
use graph_format::{FORMAT_VERSION, MAGIC};

const BLOCK: usize = 4096;
const LEVEL: i32 = 3;

/// Build the fixture under a unique temp root and publish its `current` pointer.
/// Returns `(data_dir, graph, uuid)`. Each `tag` gets its own root so tests can
/// run (and tear down) in parallel.
pub fn write_basic(tag: &str) -> (PathBuf, String, uuid::Uuid) {
    write_basic_opt(tag, false)
}

/// Like [`write_basic`] but also emits the `prop_hist.blk` value→count histograms
/// for the node range indexes (the format-v3 precompute), so the grouped-index
/// fast path answers group-by / count(DISTINCT) from the histogram instead of
/// walking the ISAM. Used by the histogram-on vs histogram-off parity test.
pub fn write_basic_with_histograms(tag: &str) -> (PathBuf, String, uuid::Uuid) {
    write_basic_opt(tag, true)
}

fn write_basic_opt(tag: &str, with_histogram: bool) -> (PathBuf, String, uuid::Uuid) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0002);
    let graph = "people".to_string();
    let root = std::env::temp_dir().join(format!("slater_fixture_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();

    // node_props.blk — Alice's embedding is routed to the vector store (D12).
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    np.append(&[
        (0, Value::Str("Alice".into())),
        (1, Value::Int(30)),
        (2, Value::Str("London".into())),
        (5, Value::Str("Red".into())),
    ])
    .unwrap();
    np.append(&[
        (0, Value::Str("Bob".into())),
        (1, Value::Int(25)),
        (2, Value::Str("London".into())),
        (5, Value::Str("Red".into())),
    ])
    .unwrap();
    np.append(&[
        (0, Value::Str("Carol".into())),
        (1, Value::Int(40)),
        (2, Value::Str("Paris".into())),
    ])
    .unwrap();
    np.append(&[(0, Value::Str("Acme".into()))]).unwrap();
    np.append(&[(0, Value::Str("Globex".into()))]).unwrap();
    np.finish().unwrap();

    // node_labels.blk
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    nl.append(&[0]).unwrap(); // Alice :Person
    nl.append(&[0]).unwrap(); // Bob :Person
    nl.append(&[0]).unwrap(); // Carol :Person
    nl.append(&[1]).unwrap(); // Acme :Company
    nl.append(&[1]).unwrap(); // Globex :Company
    nl.finish().unwrap();

    // edge_props.blk — only e0 (Alice-KNOWS->Bob) carries `since`.
    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    ep.append(&[(3, Value::Int(2020))]).unwrap(); // e0
    ep.append(&[]).unwrap(); // e1
    ep.append(&[]).unwrap(); // e2
    ep.append(&[]).unwrap(); // e3
    ep.append(&[]).unwrap(); // e4
    ep.finish().unwrap();

    // topology.csr.blk
    let edges = vec![
        Edge {
            src: NodeId(0),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(0),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(1),
        },
        Edge {
            src: NodeId(0),
            dst: NodeId(3),
            reltype: 1,
            edge: EdgeId(2),
        },
        Edge {
            src: NodeId(2),
            dst: NodeId(4),
            reltype: 1,
            edge: EdgeId(3),
        },
        Edge {
            src: NodeId(0),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(4),
        },
    ];
    write_csr(dir.join("topology.csr.blk"), 5, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — the three Person embeddings under the (Person, embedding)
    // index, appended in node order (the group is `[first_record, count)`, D10).
    let mut vw = VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL).unwrap();
    vw.append(0, &[0.1, 0.2, 0.3]).unwrap(); // Alice
    vw.append(1, &[0.2, 0.1, 0.0]).unwrap(); // Bob
    vw.append(2, &[0.9, 0.8, 0.7]).unwrap(); // Carol
    vw.finish().unwrap();

    // range indexes on (Person, name) and (Person, age).
    write_isam(
        dir.join("range").join("node_Person_name.isam"),
        vec![
            (Value::Str("Alice".into()), 0),
            (Value::Str("Bob".into()), 1),
            (Value::Str("Carol".into()), 2),
        ],
        BLOCK,
        LEVEL,
    )
    .unwrap();
    write_isam(
        dir.join("range").join("node_Person_age.isam"),
        vec![
            (Value::Int(30), 0),
            (Value::Int(25), 1),
            (Value::Int(40), 2),
        ],
        BLOCK,
        LEVEL,
    )
    .unwrap();
    // (Person, team) — Alice and Bob are both "Red"; Carol has no team. A
    // duplicate key plus a node lacking the property, so the grouped-index fast
    // path's run-length count and null-group arithmetic are both exercised.
    write_isam(
        dir.join("range").join("node_Person_team.isam"),
        vec![(Value::Str("Red".into()), 0), (Value::Str("Red".into()), 1)],
        BLOCK,
        LEVEL,
    )
    .unwrap();

    // prop_hist.blk — value→count histograms for the three node range indexes,
    // derived from the just-written ISAMs (same path as the builder), so the
    // grouped-index fast path reads them instead of walking the index.
    let node_indexes = [
        ("node_Person_name", "name"),
        ("node_Person_age", "age"),
        ("node_Person_team", "team"),
    ];
    let mut property_histograms: Vec<PropertyHistogramDesc> = Vec::new();
    if with_histogram {
        let mut records = Vec::new();
        for (name, prop) in node_indexes {
            let pairs = derive_histogram_from_isam(
                dir.join("range").join(format!("{name}.isam")),
                None,
                4096,
            )
            .unwrap()
            .expect("low-cardinality fixture index is under the cap");
            property_histograms.push(PropertyHistogramDesc {
                index_name: name.into(),
                label: "Person".into(),
                property: prop.into(),
                distinct_count: pairs.len() as u64,
            });
            records.push(encode_histogram(&pairs));
        }
        write_property_histograms(dir.join("prop_hist.blk"), &records, BLOCK, LEVEL, None).unwrap();
    }

    // Inventory + manifest.
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    let mut inv_names = vec![
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "range/node_Person_name.isam",
        "range/node_Person_age.isam",
        "range/node_Person_team.isam",
    ];
    if with_histogram {
        inv_names.push("prop_hist.blk");
    }
    for name in inv_names {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: 5,
        edge_count: 5,
        labels: vec!["Person".into(), "Company".into()],
        reltypes: vec!["KNOWS".into(), "WORKS_AT".into()],
        property_keys: vec![
            "name".into(),
            "age".into(),
            "city".into(),
            "since".into(),
            "embedding".into(),
            "team".into(),
        ],
        range_indexes: vec![
            RangeIndexDesc {
                name: "node_Person_name".into(),
                entity: EntityKind::Node,
                label_or_type: "Person".into(),
                property: "name".into(),
            },
            RangeIndexDesc {
                name: "node_Person_age".into(),
                entity: EntityKind::Node,
                label_or_type: "Person".into(),
                property: "age".into(),
            },
            RangeIndexDesc {
                name: "node_Person_team".into(),
                entity: EntityKind::Node,
                label_or_type: "Person".into(),
                property: "team".into(),
            },
        ],
        vector_indexes: vec![VectorIndexDesc {
            carried_graph: None,
            label: "Person".into(),
            property: "embedding".into(),
            dim: 3,
            metric: Metric::Cosine,
            count: 3,
            first_record: 0,
            mode: AnnMode::BruteForce,
        }],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms,
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph, uuid)
}

/// A vector fixture with the base embeddings under the caller's control: one `:Doc` node per
/// entry of `vectors`, keyed by `name` (`"d00"`, `"d01"`, … — zero-padded so the business-key
/// ISAM's lexicographic order matches the dense id order), each carrying `vectors[i]` in a
/// brute-force **cosine** index on `(:Doc {embedding})`. No edges.
///
/// The `(Doc, name)` range index is what the write path resolves a business key through, so a
/// test can drive real `MATCH (n:Doc {name:'d07'}) SET n.embedding = …` writes against it,
/// flush them into core segments, and exercise the write ladder's levels for real.
pub fn write_vector_docs(tag: &str, vectors: &[Vec<f32>]) -> (PathBuf, String) {
    write_vector_docs_keyed(tag, vectors, "Doc")
}

/// [`write_vector_docs`] with the business key on a **second label**: every node is
/// `:Doc:<key_label>`, the `name` range index is declared on `(key_label, name)`, and the
/// vector index is still on `(:Doc {embedding})`.
///
/// So a write anchors on `key_label` while the vector index is scoped to `Doc` — the one shape
/// that tells apart "is this node in the index" asked of the node's *effective label set* (what
/// the read fold asks) from the same question asked of the write's *anchor* label (what the
/// segment flush asks). In a single-label graph the two coincide and every test passes either
/// way.
pub fn write_vector_docs_keyed(
    tag: &str,
    vectors: &[Vec<f32>],
    key_label: &str,
) -> (PathBuf, String) {
    assert!(!vectors.is_empty(), "a vector fixture needs vectors");
    let dim = vectors[0].len();
    assert!(
        vectors.iter().all(|v| v.len() == dim),
        "every fixture vector must have the index's dimension"
    );
    let two_label = key_label != "Doc";
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0004);
    let graph = "docs".to_string();
    let root = std::env::temp_dir().join(format!("slater_vecdocs_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();

    let name_of = |i: usize| format!("d{i:02}");
    let isam_name = format!("node_{key_label}_name");

    // node_props.blk — the embedding is routed out to the vector store (D12), so the row holds
    // only the business key, exactly as the builder writes it.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..vectors.len() {
        np.append(&[(0, Value::Str(name_of(i)))]).unwrap();
        if two_label {
            nl.append(&[0, 1]).unwrap();
        } else {
            nl.append(&[0]).unwrap();
        }
    }
    np.finish().unwrap();
    nl.finish().unwrap();

    // No edges, but the readers still expect the files.
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();
    write_csr(
        dir.join("topology.csr.blk"),
        vectors.len() as u64,
        &[],
        BLOCK,
        LEVEL,
    )
    .unwrap();

    // vectors.f32.blk — the group is `[first_record, count)`, in dense node order (D10).
    let mut vw = VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL).unwrap();
    for (i, v) in vectors.iter().enumerate() {
        vw.append(i as u64, v).unwrap();
    }
    vw.finish().unwrap();

    write_isam(
        dir.join("range").join(format!("{isam_name}.isam")),
        (0..vectors.len())
            .map(|i| (Value::Str(name_of(i)), i as u64))
            .collect(),
        BLOCK,
        LEVEL,
    )
    .unwrap();

    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    for name in [
        "node_props.blk".to_string(),
        "node_labels.blk".to_string(),
        "edge_props.blk".to_string(),
        "topology.csr.blk".to_string(),
        "vectors.f32.blk".to_string(),
        format!("range/{isam_name}.isam"),
    ] {
        let name = name.as_str();
        let path = dir.join(name);
        files.push(FileEntry {
            name: name.to_string(),
            bytes: std::fs::metadata(&path).unwrap().len(),
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        block_sizes.insert(name.to_string(), BLOCK as u32);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: vectors.len() as u64,
        edge_count: 0,
        labels: if two_label {
            vec!["Doc".into(), key_label.into()]
        } else {
            vec!["Doc".into()]
        },
        reltypes: vec![],
        property_keys: vec!["name".into(), "embedding".into()],
        range_indexes: vec![RangeIndexDesc {
            name: isam_name.clone(),
            entity: EntityKind::Node,
            label_or_type: key_label.into(),
            property: "name".into(),
        }],
        vector_indexes: vec![VectorIndexDesc {
            carried_graph: None,
            label: "Doc".into(),
            property: "embedding".into(),
            dim: dim as u32,
            metric: Metric::Cosine,
            count: vectors.len() as u64,
            first_record: 0,
            mode: AnnMode::BruteForce,
        }],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();
    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// A fully single-label, range-indexed fixture for the consolidation serialiser
/// (every node has a recoverable business key). Three `:Person` nodes keyed by
/// `name`, plus one `:KNOWS` edge carrying `since`:
/// ```text
/// [0] Alice :Person {name:'Alice', age:30}
/// [1] Bob   :Person {name:'Bob',   age:25}
/// [2] Carol :Person {name:'Carol', age:40}
/// e0 (Alice)-[:KNOWS {since:2020}]->(Bob)
/// ```
/// Symbol tables: label Person(0); reltype KNOWS(0); property keys
/// name(0)/age(1)/since(2). One range index on (Person, name).
pub fn write_indexed_people(tag: &str) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0007);
    let root = std::env::temp_dir().join(format!("slater_idxfix_{}_{tag}", std::process::id()));
    let graph = write_indexed_people_at(&root, uuid, [30, 25, 40]);
    (root, graph)
}

/// [`write_indexed_people`] built **encrypted at rest** under `master_key`: every
/// section is written through the block cipher, and the manifest carries the KDF
/// encryption header + a sealed MAC (so a keyed [`crate::server::Graphs`] accepts it).
/// The stand-in for a real encrypted core a T2 flush must extend with an encrypted
/// segment. `master_key: None` reduces to the plaintext fixture.
pub fn write_indexed_people_keyed(tag: &str, master_key: Option<&[u8]>) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0007);
    let root = std::env::temp_dir().join(format!("slater_idxfixk_{}_{tag}", std::process::id()));
    let graph = write_indexed_people_at_keyed(&root, uuid, [30, 25, 40], master_key);
    (root, graph)
}

/// Write the `people` generation of [`write_indexed_people`] into `root/people/<uuid>/`
/// with the given Alice/Bob/Carol ages, updating `root/people/current` to name it.
/// Parameterised so a consolidation test can publish a fresh, independently-known
/// generation (a new `uuid`, patched ages) into an existing data directory — the
/// stand-in for what the real builder produces. Returns the graph name (`people`).
pub fn write_indexed_people_at(root: &Path, uuid: uuid::Uuid, ages: [i64; 3]) -> String {
    write_indexed_people_at_keyed(root, uuid, ages, None)
}

/// The body of [`write_indexed_people_at`], additionally routing every section
/// through a per-generation block cipher derived from `master_key` (and sealing the
/// manifest MAC) when a key is supplied. `None` writes the plaintext fixture.
pub fn write_indexed_people_at_keyed(
    root: &Path,
    uuid: uuid::Uuid,
    ages: [i64; 3],
    master_key: Option<&[u8]>,
) -> String {
    let graph = "people".to_string();
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();

    // Derive the block cipher + MANIFEST encryption header (salt only, never the key).
    let (cipher, encryption): (Option<Arc<BlockCipher>>, Option<EncryptionHeader>) =
        match master_key {
            Some(key) => {
                let salt = crypto::random_salt();
                let header = EncryptionHeader {
                    aead: crypto::AEAD_NAME.to_string(),
                    kdf: crypto::KDF_NAME.to_string(),
                    salt_hex: crypto::hex_encode(&salt),
                    aad_scheme: crypto::AAD_SCHEME.to_string(),
                };
                (
                    Some(Arc::new(BlockCipher::from_master(key, &salt))),
                    Some(header),
                )
            }
            None => (None, None),
        };

    // node_props.blk — name(0) + age(1) on every node.
    let mut np = PropsWriter::create_with_cipher(
        dir.join("node_props.blk"),
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "node_props.blk"),
    )
    .unwrap();
    for (name, age) in [("Alice", ages[0]), ("Bob", ages[1]), ("Carol", ages[2])] {
        np.append(&[(0, Value::Str(name.into())), (1, Value::Int(age))])
            .unwrap();
    }
    np.finish().unwrap();

    // node_labels.blk — all :Person(0).
    let mut nl = NodeLabelsWriter::create_with_cipher(
        dir.join("node_labels.blk"),
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "node_labels.blk"),
    )
    .unwrap();
    for _ in 0..3 {
        nl.append(&[0]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk — e0 carries since(2).
    let mut ep = PropsWriter::create_with_cipher(
        dir.join("edge_props.blk"),
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "edge_props.blk"),
    )
    .unwrap();
    ep.append(&[(2, Value::Int(2020))]).unwrap();
    ep.finish().unwrap();

    // topology.csr.blk — one edge Alice-KNOWS->Bob.
    let edges = vec![Edge {
        src: NodeId(0),
        dst: NodeId(1),
        reltype: 0,
        edge: EdgeId(0),
    }];
    write_csr_with_cipher(
        dir.join("topology.csr.blk"),
        3,
        &edges,
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "topology.csr.blk"),
    )
    .unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create_with_cipher(
        dir.join("vectors.f32.blk"),
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "vectors.f32.blk"),
    )
    .unwrap()
    .finish()
    .unwrap();

    // range index on (Person, name).
    write_isam_with_cipher(
        dir.join("range").join("node_Person_name.isam"),
        vec![
            (Value::Str("Alice".into()), 0),
            (Value::Str("Bob".into()), 1),
            (Value::Str("Carol".into()), 2),
        ],
        BLOCK,
        LEVEL,
        file_cipher(&cipher, "range/node_Person_name.isam"),
    )
    .unwrap();

    // Inventory + manifest.
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "range/node_Person_name.isam",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let mut manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption,
        node_count: 3,
        edge_count: 1,
        labels: vec!["Person".into()],
        reltypes: vec!["KNOWS".into()],
        property_keys: vec!["name".into(), "age".into(), "since".into()],
        range_indexes: vec![RangeIndexDesc {
            name: "node_Person_name".into(),
            entity: EntityKind::Node,
            label_or_type: "Person".into(),
            property: "name".into(),
        }],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    if let Some(key) = master_key {
        manifest.seal_mac(key).unwrap();
    }
    manifest.write_to_dir(&dir).unwrap();

    // A keyed fixture also publishes its singleton set manifest, sealed — the builder
    // does, and under a key a reader refuses both an unsealed set and an absent one
    // (HIK-144: the implicit-singleton fallback is itself a composition downgrade).
    if let Some(key) = master_key {
        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let mut set = graph_format::setmanifest::SetManifest::singleton(GenId(uuid), 0);
        set.seal_mac(key).unwrap();
        std::fs::write(sets.join(format!("{uuid}.json")), set.to_bytes().unwrap()).unwrap();
    }

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    graph
}

/// A richer fixture for the whole-graph label/reltype metadata fast paths. It has
/// several labels and reltypes, a **multi-label** node (Bob `:Person:Admin`), a
/// node with **no label** (node 4, → the `labels(n)[0]` null bucket), and a
/// **self-loop** (Acme `OWNS` Acme):
/// ```text
/// [0] Alice  :Person
/// [1] Bob    :Person:Admin        (first label Person)
/// [2] Carol  :Admin
/// [3] Acme   :Company
/// [4] Ghost  (no labels)
/// e0 (0)-[:KNOWS]->(1)
/// e1 (1)-[:KNOWS]->(2)
/// e2 (0)-[:WORKS_AT]->(3)
/// e3 (3)-[:OWNS]->(3)             ← self-loop
/// e4 (1)-[:WORKS_AT]->(3)
/// ```
/// Symbol tables: labels Person(0)/Company(1)/Admin(2); reltypes
/// KNOWS(0)/WORKS_AT(1)/OWNS(2). Only the stores the metadata queries need are
/// written; all summary vectors — including the full `schema_triple_counts` cube —
/// are computed from the written stores (via [`fixture_summaries`]) so the manifest
/// matches the graph exactly.
pub fn write_meta(tag: &str) -> (PathBuf, String, uuid::Uuid) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_00a0);
    let graph = "meta".to_string();
    let root = std::env::temp_dir().join(format!("slater_fixture_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for name in ["Alice", "Bob", "Carol", "Acme", "Ghost"] {
        np.append(&[(0, Value::Str(name.into()))]).unwrap();
    }
    np.finish().unwrap();

    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    nl.append(&[0]).unwrap(); // Alice :Person
    nl.append(&[0, 2]).unwrap(); // Bob :Person:Admin (first label Person)
    nl.append(&[2]).unwrap(); // Carol :Admin
    nl.append(&[1]).unwrap(); // Acme :Company
    nl.append(&[]).unwrap(); // Ghost (no labels)
    nl.finish().unwrap();

    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..5 {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    let edges = vec![
        Edge {
            src: NodeId(0),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(0),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(1),
        },
        Edge {
            src: NodeId(0),
            dst: NodeId(3),
            reltype: 1,
            edge: EdgeId(2),
        },
        Edge {
            src: NodeId(3),
            dst: NodeId(3),
            reltype: 2,
            edge: EdgeId(3),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(3),
            reltype: 1,
            edge: EdgeId(4),
        },
    ];
    write_csr(dir.join("topology.csr.blk"), 5, &edges, BLOCK, LEVEL).unwrap();

    // Empty-but-present vector store (`open` opens `vectors.f32.blk` unconditionally).
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    let s = fixture_summaries(&dir, 3, 3);

    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: 5,
        edge_count: 5,
        labels: vec!["Person".into(), "Company".into(), "Admin".into()],
        reltypes: vec!["KNOWS".into(), "WORKS_AT".into(), "OWNS".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: s.reltype_edge_counts,
        reltype_self_loop_counts: s.reltype_self_loop_counts,
        label_node_counts: s.label_node_counts,
        first_label_counts: s.first_label_counts,
        src_label_reltype_counts: s.src_label_reltype_counts,
        reltype_tgt_label_counts: s.reltype_tgt_label_counts,
        schema_triple_counts: s.schema_triple_counts,
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph, uuid)
}

/// The metadata summary vectors, computed from a fixture's written stores the same
/// way the builder does (so the fixture manifest matches its graph exactly).
pub struct FixtureSummaries {
    pub reltype_edge_counts: Vec<u64>,
    pub reltype_self_loop_counts: Vec<u64>,
    pub label_node_counts: Vec<u64>,
    pub first_label_counts: Vec<u64>,
    pub src_label_reltype_counts: Vec<(u32, u32, u64)>,
    pub reltype_tgt_label_counts: Vec<(u32, u32, u64)>,
    pub schema_triple_counts: Vec<(u32, u32, u32, u64)>,
}

/// Compute [`FixtureSummaries`] by scanning the just-written `topology.csr.blk` and
/// `node_labels.blk` (forward pass for source-side tallies, reverse pass for the
/// target-side marginal) — an independent re-derivation of what the builder writes.
pub fn fixture_summaries(
    dir: &std::path::Path,
    n_labels: usize,
    n_reltypes: usize,
) -> FixtureSummaries {
    use std::collections::HashMap;
    let topo = TopologyReader::open(dir.join("topology.csr.blk")).unwrap();
    let labels = NodeLabelsReader::open(dir.join("node_labels.blk")).unwrap();
    let node_count = topo.node_count();

    let mut reltype_edge = vec![0u64; n_reltypes];
    let mut reltype_self = vec![0u64; n_reltypes];
    let mut label_node = vec![0u64; n_labels];
    let mut first_label = vec![0u64; n_labels];
    let mut src_marg: HashMap<(u32, u32), u64> = HashMap::new();
    let mut tgt_marg: HashMap<(u32, u32), u64> = HashMap::new();
    let mut cube: HashMap<(u32, u32, u32), u64> = HashMap::new();

    for id in 0..node_count {
        let labs = labels.labels(id).unwrap();
        if let Some(&f) = labs.first() {
            first_label[f as usize] += 1;
        }
        for &l in &labs {
            label_node[l as usize] += 1;
        }
        for adj in topo.outgoing(NodeId(id)).unwrap() {
            reltype_edge[adj.reltype as usize] += 1;
            if adj.neighbour.0 == id {
                reltype_self[adj.reltype as usize] += 1;
            }
            // Random dst-label lookup — fine for a tiny test fixture (the builder
            // does this join via an external sort-merge instead).
            let dst_labs = labels.labels(adj.neighbour.0).unwrap();
            for &a in &labs {
                *src_marg.entry((a, adj.reltype)).or_insert(0) += 1;
                for &b in &dst_labs {
                    *cube.entry((a, adj.reltype, b)).or_insert(0) += 1;
                }
            }
        }
        for adj in topo.incoming(NodeId(id)).unwrap() {
            for &b in &labs {
                *tgt_marg.entry((adj.reltype, b)).or_insert(0) += 1;
            }
        }
    }
    let mut src_label_reltype_counts: Vec<(u32, u32, u64)> =
        src_marg.into_iter().map(|((a, t), c)| (a, t, c)).collect();
    src_label_reltype_counts.sort_unstable();
    let mut reltype_tgt_label_counts: Vec<(u32, u32, u64)> =
        tgt_marg.into_iter().map(|((t, b), c)| (t, b, c)).collect();
    reltype_tgt_label_counts.sort_unstable();
    let mut schema_triple_counts: Vec<(u32, u32, u32, u64)> = cube
        .into_iter()
        .map(|((a, t, b), c)| (a, t, b, c))
        .collect();
    schema_triple_counts.sort_unstable();

    FixtureSummaries {
        reltype_edge_counts: reltype_edge,
        reltype_self_loop_counts: reltype_self,
        label_node_counts: label_node,
        first_label_counts: first_label,
        src_label_reltype_counts,
        reltype_tgt_label_counts,
        schema_triple_counts,
    }
}

/// A tiny **cyclic** fixture for the GQL path-restrictor tests (PR 2). Three
/// `:N` nodes joined by a single relationship type `R`:
/// ```text
/// [0] a  [1] b  [2] c          (name = 'a' / 'b' / 'c')
/// e0 (a)-[:R]->(b)
/// e1 (b)-[:R]->(c)
/// e2 (c)-[:R]->(a)             ← closes the a→b→c→a triangle
/// e3 (c)-[:R]->(b)             ← second route into b (distinct edge)
/// ```
/// Adjacency `a→{b}`, `b→{c}`, `c→{a,b}`. This is the minimal graph that tells the
/// four path modes apart over `MATCH … (s WHERE name='a')-[:R*1..4]->(x)`: the
/// triangle lets SIMPLE close back at the start `a` (which ACYCLIC forbids), while
/// the `c→b` chord lets TRAIL revisit `b` via a distinct edge (which SIMPLE/ACYCLIC
/// forbid as an interior node repeat). Path counts are WALK 6, TRAIL 4, SIMPLE 3,
/// ACYCLIC 2 — all distinct. No vector/range indexes (restrictors exercise only the
/// traversal), so the scans fall back to a full node scan.
pub fn write_cycle(tag: &str) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0004);
    let graph = "cycle".to_string();
    let root = std::env::temp_dir().join(format!("slater_cycfix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    // node_props.blk — just a name per node so a WHERE can anchor the start.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    np.append(&[(0, Value::Str("a".into()))]).unwrap();
    np.append(&[(0, Value::Str("b".into()))]).unwrap();
    np.append(&[(0, Value::Str("c".into()))]).unwrap();
    np.finish().unwrap();

    // node_labels.blk — every node is :N (label id 0).
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    nl.append(&[0]).unwrap();
    nl.append(&[0]).unwrap();
    nl.append(&[0]).unwrap();
    nl.finish().unwrap();

    // edge_props.blk — four edges, none carrying properties.
    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..4 {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    // topology.csr.blk — the triangle plus the c→b chord (reltype R = 0).
    let edges = vec![
        Edge {
            src: NodeId(0),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(0),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(1),
        },
        Edge {
            src: NodeId(2),
            dst: NodeId(0),
            reltype: 0,
            edge: EdgeId(2),
        },
        Edge {
            src: NodeId(2),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(3),
        },
    ];
    write_csr(dir.join("topology.csr.blk"), 3, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Inventory + manifest (no index files to list).
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: 3,
        edge_count: 4,
        labels: vec!["N".into()],
        reltypes: vec!["R".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// A **sparse-reltype** fixture for the relationship-type-scan tests. Six `:N`
/// nodes (label id 0), where only a few are edge endpoints — so a typed first hop
/// drives from far fewer nodes than the 6-node label posting:
/// ```text
/// reltype T (id 0): (0)-[:T]->(1), (1)-[:T]->(2)   sources {0,1}, targets {1,2}
/// reltype U (id 1): (0)-[:U]->(3)                  source {0},    target {3}
/// ```
/// Nodes 4 and 5 are isolated. Carries the endpoint postings + manifest counts, so
/// `has_reltype_postings()` is true and `maybe_rel_type_scan` can fire.
pub fn write_rel_sparse(tag: &str) -> (PathBuf, String) {
    write_rel_sparse_opt(tag, true)
}

/// As [`write_rel_sparse`] but without the endpoint postings — the *same* graph
/// (identical node ids and edges), so a rel-type-scan-on vs -off comparison runs
/// over byte-identical data. `has_reltype_postings()` is false, so every query
/// drives from the label scan.
pub fn write_rel_sparse_no_postings(tag: &str) -> (PathBuf, String) {
    write_rel_sparse_opt(tag, false)
}

fn write_rel_sparse_opt(tag: &str, with_postings: bool) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0009);
    let graph = "relsparse".to_string();
    let root = std::env::temp_dir().join(format!("slater_relspx_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for name in ["a", "b", "c", "d", "e", "f"] {
        np.append(&[(0, Value::Str(name.into()))]).unwrap();
    }
    np.finish().unwrap();

    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..6 {
        nl.append(&[0]).unwrap(); // all :N
    }
    nl.finish().unwrap();

    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..3 {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    let edges = vec![
        Edge {
            src: NodeId(0),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(0),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(1),
        },
        Edge {
            src: NodeId(0),
            dst: NodeId(3),
            reltype: 1,
            edge: EdgeId(2),
        },
    ];
    write_csr(dir.join("topology.csr.blk"), 6, &edges, BLOCK, LEVEL).unwrap();

    let (reltype_source_counts, reltype_target_counts) = if with_postings {
        graph_format::postings::write_reltype_endpoint_postings(
            dir.join("reltype_src.post"),
            dir.join("reltype_tgt.post"),
            2,
            &edges,
            BLOCK,
            LEVEL,
            None,
        )
        .unwrap()
    } else {
        (vec![], vec![])
    };

    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    let mut names = vec![
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ];
    if with_postings {
        names.push("reltype_src.post");
        names.push("reltype_tgt.post");
    }
    for name in names {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: 6,
        edge_count: 3,
        labels: vec!["N".into()],
        reltypes: vec!["T".into(), "U".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts,
        reltype_target_counts,
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();
    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();
    (root, graph)
}

/// A **dense-reltype chain** fixture (HIK-104) for the streaming `RelTypeScan` test: `n`
/// `:N` nodes (label id 0), each carrying `name = "node{i:04}"`, joined into a single chain
/// by one relationship type `T` (id 0) — edge `i -> i+1` for `i` in `0..n-1`. Every node but
/// the last is a **source** endpoint and every node but the first a **target**, so the
/// endpoint postings are *dense* (stored as their sparse complement — the 733 MB-class case)
/// and a `RelTypeScan` drives from ~all `n` nodes. Carries the endpoint postings + manifest
/// counts so `has_reltype_postings()` is true. No indexes.
pub fn write_rel_chain(tag: &str, n: u64) -> (PathBuf, String) {
    assert!(n >= 2, "chain needs at least two nodes");
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_000c);
    let graph = "relchain".to_string();
    let root = std::env::temp_dir().join(format!("slater_relchn_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        np.append(&[(0, Value::Str(format!("node{i:04}")))])
            .unwrap();
    }
    np.finish().unwrap();

    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..n {
        nl.append(&[0]).unwrap(); // all :N
    }
    nl.finish().unwrap();

    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..n - 1 {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    let edges: Vec<Edge> = (0..n - 1)
        .map(|i| Edge {
            src: NodeId(i),
            dst: NodeId(i + 1),
            reltype: 0,
            edge: EdgeId(i),
        })
        .collect();
    write_csr(dir.join("topology.csr.blk"), n, &edges, BLOCK, LEVEL).unwrap();

    let (reltype_source_counts, reltype_target_counts) =
        graph_format::postings::write_reltype_endpoint_postings(
            dir.join("reltype_src.post"),
            dir.join("reltype_tgt.post"),
            1,
            &edges,
            BLOCK,
            LEVEL,
            None,
        )
        .unwrap();

    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "reltype_src.post",
        "reltype_tgt.post",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n,
        edge_count: n - 1,
        labels: vec!["N".into()],
        reltypes: vec!["T".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts,
        reltype_target_counts,
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();
    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();
    (root, graph)
}

/// A tiny **diamond** fixture for the GQL shortest-path-selector tests (PR 3). Five
/// `:N` nodes joined by a single relationship type `R`:
/// ```text
/// [0] s  [1] a  [2] b  [3] c  [4] t     (name = 's'/'a'/'b'/'c'/'t')
/// e0 (s)-[:R]->(a)
/// e1 (s)-[:R]->(b)
/// e2 (a)-[:R]->(t)
/// e3 (b)-[:R]->(t)
/// e4 (a)-[:R]->(c)
/// e5 (c)-[:R]->(t)
/// ```
/// There are three loopless `s→t` paths: `s→a→t` and `s→b→t` (both length 2), plus
/// `s→a→c→t` (length 3). This is the minimal graph that tells the selectors apart:
/// `ALL SHORTEST` returns the two length-2 ties, `SHORTEST 3` returns all three (two
/// of length 2 then one of length 3), and `ANY SHORTEST` returns a single length-2
/// path. No vector/range indexes, so the endpoint scans fall back to a full node
/// scan filtered by `node_ok`.
pub fn write_diamond(tag: &str) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0005);
    let graph = "diamond".to_string();
    let root = std::env::temp_dir().join(format!("slater_diafix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    // node_props.blk — a name per node so a WHERE can anchor the endpoints.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for name in ["s", "a", "b", "c", "t"] {
        np.append(&[(0, Value::Str(name.into()))]).unwrap();
    }
    np.finish().unwrap();

    // node_labels.blk — every node is :N (label id 0).
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..5 {
        nl.append(&[0]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk — six edges, none carrying properties.
    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..6 {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    // topology.csr.blk — the diamond plus the length-3 detour (reltype R = 0).
    let edges = vec![
        Edge {
            src: NodeId(0),
            dst: NodeId(1),
            reltype: 0,
            edge: EdgeId(0),
        },
        Edge {
            src: NodeId(0),
            dst: NodeId(2),
            reltype: 0,
            edge: EdgeId(1),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(4),
            reltype: 0,
            edge: EdgeId(2),
        },
        Edge {
            src: NodeId(2),
            dst: NodeId(4),
            reltype: 0,
            edge: EdgeId(3),
        },
        Edge {
            src: NodeId(1),
            dst: NodeId(3),
            reltype: 0,
            edge: EdgeId(4),
        },
        Edge {
            src: NodeId(3),
            dst: NodeId(4),
            reltype: 0,
            edge: EdgeId(5),
        },
    ];
    write_csr(dir.join("topology.csr.blk"), 5, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Inventory + manifest (no index files to list).
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: 5,
        edge_count: 6,
        labels: vec!["N".into()],
        reltypes: vec!["R".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// A straight **chain** of `len + 1` nodes `n0 -[:R]-> n1 -[:R]-> … -> n{len}` (all label `N`,
/// one reltype `R`, name `n{i}` per node). A single shortest path of length `len` between the
/// ends, with the intermediate frontier growing one hop per layer — the fixture for the
/// depth-proportional shortest-path frontier charge (a deep branch clones an O(depth) path).
pub fn write_chain(tag: &str, len: u64) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0009);
    let graph = "chain".to_string();
    let root = std::env::temp_dir().join(format!("slater_chainfix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();
    let n = len + 1;

    // node_props.blk — a name `n{i}` per node so a WHERE can anchor the endpoints.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        np.append(&[(0, Value::Str(format!("n{i}")))]).unwrap();
    }
    np.finish().unwrap();

    // node_labels.blk — every node is :N (label id 0).
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..n {
        nl.append(&[0]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk — `len` edges, none carrying properties.
    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..len {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    // topology.csr.blk — n_i -> n_{i+1} (reltype R = 0).
    let edges: Vec<Edge> = (0..len)
        .map(|i| Edge {
            src: NodeId(i),
            dst: NodeId(i + 1),
            reltype: 0,
            edge: EdgeId(i),
        })
        .collect();
    write_csr(dir.join("topology.csr.blk"), n, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Inventory + manifest (no index files to list).
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n,
        edge_count: len,
        labels: vec!["N".into()],
        reltypes: vec!["R".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// `n` **isolated** nodes (`n0..n{n-1}`, label `N`) with the reltype `R` declared but **zero
/// edges**. A shortest-path selector between two free endpoints scans all `n` for each side and
/// launches `n²` searches, every one of which returns immediately (no edges) — so the frontier
/// work is ~0 but the *number* of searches is quadratic. The fixture for the two-free-endpoint
/// fan-out charge.
pub fn write_isolated(tag: &str, n: u64) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_000a);
    let graph = "isolated".to_string();
    let root = std::env::temp_dir().join(format!("slater_isofix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        np.append(&[(0, Value::Str(format!("n{i}")))]).unwrap();
    }
    np.finish().unwrap();

    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..n {
        nl.append(&[0]).unwrap();
    }
    nl.finish().unwrap();

    // No edges, but both edge-side files still exist and `R` is a declared reltype so a
    // `-[:R*]->` selector type-checks.
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();
    write_csr(dir.join("topology.csr.blk"), n, &[], BLOCK, LEVEL).unwrap();

    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n,
        edge_count: 0,
        labels: vec!["N".into()],
        reltypes: vec!["R".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// A **wide** fixture of `n` nodes for the parallel anchor-filter test (Task 10):
/// enough candidates to clear `SCAN_PAR_MIN` so the pooled `node_ok` prefilter
/// actually fans out. Node `i` is `:Person` when `i` is even, else `:Company`; every
/// node carries `name = "node{i:04}"` (zero-padded for a stable string ORDER BY), and
/// each `:Person` additionally carries `team = "Red"` when `i % 4 == 0` else `"Blue"`.
/// No edges and no indexes — scans fall back to a label scan / full node scan, which
/// is exactly the path the prefilter sits on. Property keys: name(0)/team(1); labels
/// Person(0)/Company(1).
pub fn write_wide(tag: &str, n: u64) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0006);
    let graph = "wide".to_string();
    let root = std::env::temp_dir().join(format!("slater_widefix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    // node_props.blk — name on every node, team only on :Person.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        let name = Value::Str(format!("node{i:04}"));
        if i % 2 == 0 {
            let team = if i % 4 == 0 { "Red" } else { "Blue" };
            np.append(&[(0, name), (1, Value::Str(team.into()))])
                .unwrap();
        } else {
            np.append(&[(0, name)]).unwrap();
        }
    }
    np.finish().unwrap();

    // node_labels.blk — Person(0) on evens, Company(1) on odds.
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        nl.append(&[if i % 2 == 0 { 0 } else { 1 }]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk + topology.csr.blk — no edges, but both files always exist.
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();
    write_csr(dir.join("topology.csr.blk"), n, &[], BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Inventory + manifest (no index files to list).
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n,
        edge_count: 0,
        labels: vec!["Person".into(), "Company".into()],
        reltypes: vec![],
        property_keys: vec!["name".into(), "team".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// A **hub** (star) fixture for the expansion-charge tests (root cause 2b): one
/// centre node `[0]` with `n` outgoing `:LINK` edges to leaves `[1..=n]`, and each
/// leaf carrying a single `:LINK` edge back to the centre. So expanding the centre
/// reads an `n`-edge adjacency in one step — exactly the hub read that must trip the
/// intermediate budget immediately, before the `Vec<Hop>` it materialises (and the
/// completed rows downstream) balloon RSS. The two-way wiring lets a fixed 2-hop
/// `(:Hub)-[:LINK]->(x)-[:LINK]->(y)` traverse (each leaf hops back to the centre),
/// so both a 1-hop and a 2-hop expansion can be exercised; with `n ≥ EXPAND_PAR_MIN`
/// the hop-1 frontier (`n` leaves) also clears the parallel fan-out threshold, so the
/// pooled `expand_chain_par` path is covered too.
/// ```text
/// [0] centre  :Hub  {name:'hub'}
/// [i] leaf i  :Leaf {name:'leaf{i:05}'}   for i in 1..=n
/// centre -[:LINK]-> leaf i   (n edges)
/// leaf i -[:LINK]-> centre   (n edges)
/// ```
/// Labels Hub(0)/Leaf(1); reltype LINK(0); property key name(0). No indexes — the
/// `:Hub` anchor is a label scan that yields the single centre node.
pub fn write_hub(tag: &str, n: u64) -> (PathBuf, String) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0007);
    let graph = "hub".to_string();
    let root = std::env::temp_dir().join(format!("slater_hubfix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(&dir).unwrap();

    // node_props.blk — a name on every node (centre then leaves, in id order).
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    np.append(&[(0, Value::Str("hub".into()))]).unwrap();
    for i in 1..=n {
        np.append(&[(0, Value::Str(format!("leaf{i:05}")))])
            .unwrap();
    }
    np.finish().unwrap();

    // node_labels.blk — centre is :Hub(0), every leaf is :Leaf(1).
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    nl.append(&[0]).unwrap();
    for _ in 1..=n {
        nl.append(&[1]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk — 2n edges, none carrying properties.
    let mut ep = PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..(2 * n) {
        ep.append(&[]).unwrap();
    }
    ep.finish().unwrap();

    // topology.csr.blk — centre→leaf then leaf→centre (reltype LINK = 0). Edge ids
    // are unique across both directions; write_csr buckets them by endpoint itself.
    let mut edges = Vec::with_capacity(2 * n as usize);
    for i in 1..=n {
        edges.push(Edge {
            src: NodeId(0),
            dst: NodeId(i),
            reltype: 0,
            edge: EdgeId(i - 1),
        });
    }
    for i in 1..=n {
        edges.push(Edge {
            src: NodeId(i),
            dst: NodeId(0),
            reltype: 0,
            edge: EdgeId(n - 1 + i),
        });
    }
    write_csr(dir.join("topology.csr.blk"), n + 1, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Inventory + manifest (no index files to list).
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n + 1,
        edge_count: 2 * n,
        labels: vec!["Hub".into(), "Leaf".into()],
        reltypes: vec!["LINK".into()],
        property_keys: vec!["name".into()],
        range_indexes: vec![],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

/// Parameters for a synthetic Vamana/PQ generation.
pub struct VamanaFixture {
    pub n: usize,
    pub dim: usize,
    pub r: u32,
    pub alpha: f32,
    pub pq_subspaces: u32,
    pub pq_bits: u32,
    /// Vamana block size — keep it small in a test so the store spans many blocks
    /// and the vector cache must page.
    pub vector_block_size: usize,
}

/// [`write_vamana_holed`] with no holes — every record live.
pub fn write_vamana(tag: &str, f: &VamanaFixture) -> (PathBuf, String, Vec<Vec<f32>>) {
    let (root, graph, raw, _) = write_vamana_holed(tag, f, |_, _| false);
    (root, graph, raw)
}

/// [`write_vamana_holed`], then **delete-consolidated** (`graph_format::vamana_delete`) — the
/// FreshVamana `Delete` pass patches the holes out of the adjacency before the generation is
/// sealed, so no reachable node names one.
///
/// The point of the fixture pair is that the *only* difference between it and
/// [`write_vamana_holed`] is the pass: same vectors, same graph, same holes, same queries. So
/// the IO a query pays on the two can be compared directly, which is the only way to show the
/// pass does what it exists to do — a recall assertion cannot, because recall looks fine while
/// IO quietly doubles.
pub fn write_vamana_holed_consolidated(
    tag: &str,
    f: &VamanaFixture,
    hole: impl Fn(u64, bool) -> bool,
) -> (PathBuf, String, Vec<Vec<f32>>, u64) {
    write_vamana_inner(tag, f, hole, true)
}

/// Build a synthetic generation whose single `(:Doc, embedding)` vector index is an
/// above-threshold **Vamana/PQ** index of `n` unit vectors, mirroring exactly what
/// `slater-build` writes (graph build → BFS layout → PQ codes in the same order).
///
/// `hole(node_id, is_medoid)` decides, per record, whether its `.pq` id is written as the
/// tombstone sentinel `pq::HOLE` instead of the node id — making it a **hole**: navigable,
/// never emitted. `is_medoid` is passed because the medoid's node id is not knowable to the
/// caller before the graph is built, and holing the *medoid* is the sharpest test of the
/// waypoint contract there is: it is the fixed entry point of every beam search, so if a
/// hole were dropped from *navigation* rather than only from *emission*, holing it would
/// take recall for the whole index to zero.
///
/// Returns `(data_dir, graph, raw_vectors, medoid_node_id)`; `raw_vectors[i]` is dense node
/// `i`'s vector, so a test can compute brute-force ground truth over whichever subset it
/// left live. The graph has `n` `:Doc` nodes and no edges.
pub fn write_vamana_holed(
    tag: &str,
    f: &VamanaFixture,
    hole: impl Fn(u64, bool) -> bool,
) -> (PathBuf, String, Vec<Vec<f32>>, u64) {
    write_vamana_inner(tag, f, hole, false)
}

fn write_vamana_inner(
    tag: &str,
    f: &VamanaFixture,
    hole: impl Fn(u64, bool) -> bool,
    consolidate: bool,
) -> (PathBuf, String, Vec<Vec<f32>>, u64) {
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0003);
    let graph = "docs".to_string();
    let root = std::env::temp_dir().join(format!("slater_vamfix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();
    std::fs::create_dir_all(dir.join("vector")).unwrap();

    // Deterministic synthetic unit vectors (no `rand` dependency).
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (f.n as u64).wrapping_mul(2654435761);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    let raw: Vec<Vec<f32>> = (0..f.n)
        .map(|_| {
            let v: Vec<f32> = (0..f.dim).map(|_| (next() as f32) - 0.5).collect();
            normalise(&v)
        })
        .collect();

    // node_props.blk (empty maps) + node_labels.blk (all :Doc).
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..f.n {
        np.append(&[]).unwrap();
        nl.append(&[0]).unwrap();
    }
    np.finish().unwrap();
    nl.finish().unwrap();

    // No edges, but the readers still expect the files.
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();
    write_csr(dir.join("topology.csr.blk"), f.n as u64, &[], BLOCK, LEVEL).unwrap();
    // The Vamana arm never reads vectors.f32.blk; write it empty for the reader.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // Build the Vamana graph + PQ codes, both in BFS-from-medoid layout order.
    let graph_v = build_vamana(&raw, f.r as usize, f.alpha).unwrap();
    let order = bfs_order(&graph_v);
    let mut new_of = vec![0u32; order.len()];
    for (new_idx, &old) in order.iter().enumerate() {
        new_of[old as usize] = new_idx as u32;
    }
    let medoid_new = new_of[graph_v.medoid as usize];

    let mut vw = VamanaWriter::create_with_cipher(
        dir.join("vector/Doc.embedding.vamana"),
        f.vector_block_size,
        LEVEL,
        None,
    )
    .unwrap();
    for &old in &order {
        let nbrs: Vec<u32> = graph_v.adjacency[old as usize]
            .iter()
            .map(|&j| new_of[j as usize])
            .collect();
        // Pure geometry — the record carries no node id (v8); the `.pq` below is the map.
        vw.append(&raw[old as usize], &nbrs).unwrap();
    }
    vw.finish().unwrap();

    let params = PqParams::new(f.dim as u32, f.pq_subspaces, f.pq_bits).unwrap();
    let codebook = train_codebooks(&raw, params, 25).unwrap();
    let mut pw = PqWriter::create_with_cipher(
        dir.join("vector/Doc.embedding.pq"),
        &codebook,
        BLOCK,
        LEVEL,
        None,
    )
    .unwrap();
    // The `.pq` node-id column IS the layout→id map, and a `HOLE` in it is a tombstoned
    // record: still walked through, never emitted.
    let medoid_node_id = graph_v.medoid as u64;
    let mut live_count = 0u64;
    for &old in &order {
        let node_id = old as u64;
        let id = if hole(node_id, node_id == medoid_node_id) {
            HOLE
        } else {
            live_count += 1;
            node_id
        };
        pw.append_codes(id, &codebook.encode(&raw[old as usize]).unwrap())
            .unwrap();
    }
    pw.finish().unwrap();

    // FreshVamana's `Delete` (S5): patch the holes out of the adjacency, so no reachable node
    // names one and the dead records cost zero query IO. Runs over the files just written and
    // replaces them, *before* the inventory is hashed — so the generation is self-consistent
    // either way and the only difference between the two fixtures is the pass itself.
    if consolidate {
        let vam = dir.join("vector/Doc.embedding.vamana");
        let pq = dir.join("vector/Doc.embedding.pq");
        let vam_tmp = dir.join("vector/Doc.embedding.vamana.tmp");
        let pq_tmp = dir.join("vector/Doc.embedding.pq.tmp");
        let stats = graph_format::vamana_delete::consolidate_index_files(
            &vam,
            &pq,
            &vam_tmp,
            &pq_tmp,
            &graph_format::vamana_delete::ConsolidateIndex {
                medoid: medoid_new,
                r: f.r as usize,
                alpha: f.alpha,
                metric: Metric::Cosine,
                max_norm: 1.0,
                nav: graph_format::manifest::AnnNav::Augmented,
                // The `.pq` written above already names every hole; nothing extra to tombstone.
                tombstoned: &[],
                vamana_block_bytes: f.vector_block_size,
                pq_block_bytes: BLOCK,
                zstd_level: LEVEL,
                cipher: None,
                stem: "vector/Doc.embedding".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            stats.live, live_count,
            "the pass must not change what is live"
        );
        std::fs::rename(&vam_tmp, &vam).unwrap();
        std::fs::rename(&pq_tmp, &pq).unwrap();
    }

    // Inventory + manifest.
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let names = [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "vector/Doc.embedding.vamana",
        "vector/Doc.embedding.pq",
    ];
    for name in names {
        let path = dir.join(name);
        files.push(FileEntry {
            name: name.to_string(),
            bytes: std::fs::metadata(&path).unwrap().len(),
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        let bs = if name == "vector/Doc.embedding.vamana" {
            f.vector_block_size as u32
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
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: f.n as u64,
        edge_count: 0,
        labels: vec!["Doc".into()],
        reltypes: vec![],
        property_keys: vec!["embedding".into()],
        range_indexes: vec![],
        vector_indexes: vec![VectorIndexDesc {
            carried_graph: None,
            label: "Doc".into(),
            property: "embedding".into(),
            dim: f.dim as u32,
            metric: Metric::Cosine,
            count: f.n as u64,
            first_record: 0,
            mode: AnnMode::Vamana {
                r: f.r,
                alpha: f.alpha,
                medoid: medoid_new as u64,
                pq_subspaces: f.pq_subspaces,
                pq_bits: f.pq_bits,
                live_count,
                // The fixture's vectors are already unit length, so M = 1; a cosine index
                // never reads it anyway.
                max_norm: 1.0,
                nav: graph_format::manifest::AnnNav::Augmented,
            },
        }],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: vec![],
        reltype_self_loop_counts: vec![],
        label_node_counts: vec![],
        first_label_counts: vec![],
        src_label_reltype_counts: vec![],
        reltype_tgt_label_counts: vec![],
        schema_triple_counts: vec![],
        property_histograms: vec![],
        hub_degrees: None,
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();
    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph, raw, medoid_node_id)
}
