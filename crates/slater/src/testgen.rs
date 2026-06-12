// SPDX-License-Identifier: Apache-2.0
//! Shared test fixture: a small, representative generation built directly with
//! the `graph-format` writers (no dependency on the `slater-build` binary), used
//! by the planner and executor tests.
//!
//! The graph (dense node ids in brackets):
//! ```text
//! [0] Alice  :Person {name:'Alice', age:30, city:'London'}   (+ embedding → vec store)
//! [1] Bob    :Person {name:'Bob',   age:25, city:'London'}
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
//! property keys name(0)/age(1)/city(2)/since(3)/embedding(4). Range indexes on
//! (Person,name) and (Person,age); one brute-force vector index on
//! (Person,embedding) holding the three Person embeddings (Alice/Bob/Carol), in
//! node order, so the KNN path has a real candidate set to rank.

#![cfg(test)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use graph_format::columns::PropsWriter;
use graph_format::ids::{EdgeId, Generation as GenId, NodeId, Value};
use graph_format::integrity::hash_file;
use graph_format::isam::write_isam;
use graph_format::manifest::{
    AnnMode, EntityKind, FileEntry, Manifest, Metric, RangeIndexDesc, VectorIndexDesc,
};
use graph_format::nodelabels::NodeLabelsWriter;
use graph_format::pq::{train_codebooks, PqParams, PqWriter};
use graph_format::topology::{write_csr, Edge};
use graph_format::vamana::{bfs_order, build_vamana, VamanaWriter};
use graph_format::vectors::VectorStoreWriter;
use graph_format::{FORMAT_VERSION, MAGIC};

const BLOCK: usize = 4096;
const LEVEL: i32 = 3;

/// Build the fixture under a unique temp root and publish its `current` pointer.
/// Returns `(data_dir, graph, uuid)`. Each `tag` gets its own root so tests can
/// run (and tear down) in parallel.
pub fn write_basic(tag: &str) -> (PathBuf, String, uuid::Uuid) {
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
    ])
    .unwrap();
    np.append(&[
        (0, Value::Str("Bob".into())),
        (1, Value::Int(25)),
        (2, Value::Str("London".into())),
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
        "range/node_Person_age.isam",
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
        ],
        vector_indexes: vec![VectorIndexDesc {
            label: "Person".into(),
            property: "embedding".into(),
            dim: 3,
            metric: Metric::Cosine,
            count: 3,
            first_record: 0,
            mode: AnnMode::BruteForce,
        }],
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

/// Build a synthetic generation whose single `(:Doc, embedding)` vector index is an
/// above-threshold **Vamana/PQ** index of `n` unit vectors, mirroring exactly what
/// `slater-build` writes (graph build → BFS layout → PQ codes in the same order).
/// Returns `(data_dir, graph, raw_vectors)` so a test can compute brute-force
/// ground truth. The graph has `n` `:Doc` nodes and no edges.
pub fn write_vamana(tag: &str, f: &VamanaFixture) -> (PathBuf, String, Vec<Vec<f32>>) {
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
            unit(&v)
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
        vw.append(old as u64, &raw[old as usize], &nbrs).unwrap();
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
    for &old in &order {
        pw.append_codes(old as u64, &codebook.encode(&raw[old as usize]).unwrap())
            .unwrap();
    }
    pw.finish().unwrap();

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
        encryption: None,
        node_count: f.n as u64,
        edge_count: 0,
        labels: vec!["Doc".into()],
        reltypes: vec![],
        property_keys: vec!["embedding".into()],
        range_indexes: vec![],
        vector_indexes: vec![VectorIndexDesc {
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
            },
        }],
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

    (root, graph, raw)
}

/// L2-normalise a vector to unit length (the cosine space the Vamana path uses).
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
