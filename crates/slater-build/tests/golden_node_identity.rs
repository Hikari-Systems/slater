// SPDX-License-Identifier: Apache-2.0
//! Node-identity and fold semantics of a business-key merge import: a node is
//! identified by its single `(label, keyField, value)` triple; statements sharing
//! that triple fold into ONE node (last-writer-wins, with functions/prop refs
//! reading the state folded so far). Statements without a single business key, or
//! using `MATCH` instead of `MERGE`, are rejected — the builder is not a Cypher
//! engine and never scans the partial graph.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::Value;
use graph_format::manifest::Manifest;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_nodeid_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

fn build_ok(tag: &str, dump: &str) -> (Manifest, PathBuf, PathBuf) {
    let work = unique_dir(tag);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, dump).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "g",
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
    let graph_dir = data_dir.join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    (m, gen_dir, work)
}

fn build_err(tag: &str, dump: &str, needle: &str) {
    let work = unique_dir(tag);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, dump).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "g",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    assert!(!out.status.success(), "expected non-zero exit for {tag}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(needle),
        "expected stderr to contain {needle:?}, got: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn same_business_key_folds_to_one_node() {
    // Two MERGE on the SAME (Company, ticker, 'X') → one node. The coalesce on the
    // second line reads the name folded by the first → keeps 'm3'. A fourth case
    // shows coalesce with no prior value falling to the literal.
    const DUMP: &str = r#"MERGE (n:Company {ticker: 'X'}) SET n.name = 'm3';
MERGE (n:Company {ticker: 'X'}) SET n.name = coalesce(n.name, 'default');
MERGE (n:Company {ticker: 'Y'}) SET n.name = coalesce(n.name, 'default');
"#;
    let (m, gen_dir, work) = build_ok("fold", DUMP);
    assert_eq!(
        m.node_count, 2,
        "ticker X folds to one node; Y is the second"
    );

    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut name_of = HashMap::new();
    for id in 0..m.node_count {
        let props = np.props(id).unwrap();
        if let Some(Value::Str(t)) = prop(&props, &m.property_keys, "ticker") {
            let name = prop(&props, &m.property_keys, "name").cloned();
            name_of.insert(t.clone(), name);
        }
    }
    assert_eq!(
        name_of["X"],
        Some(Value::Str("m3".into())),
        "prior name kept"
    );
    assert_eq!(
        name_of["Y"],
        Some(Value::Str("default".into())),
        "no prior → literal"
    );
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn different_key_field_is_a_different_node() {
    // Same label + same value but DIFFERENT key field ⇒ two distinct identities.
    const DUMP: &str = r#"MERGE (n:Indication {meshUi: 'D1'}) SET n.name = 'byMesh';
MERGE (n:Indication {canonicalName: 'D1'}) SET n.name = 'byCanon';
"#;
    let (m, gen_dir, work) = build_ok("keyfield", DUMP);
    assert_eq!(
        m.node_count, 2,
        "Indication{{meshUi:'D1'}} and Indication{{canonicalName:'D1'}} are distinct nodes"
    );
    // Sanity: both names are present across the two nodes.
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut names: Vec<String> = (0..m.node_count)
        .filter_map(
            |id| match prop(&np.props(id).unwrap(), &m.property_keys, "name") {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            },
        )
        .collect();
    names.sort();
    assert_eq!(names, vec!["byCanon".to_string(), "byMesh".to_string()]);
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn match_label_only_is_rejected() {
    // `MATCH … SET` is not a merge-dump statement (the builder never scans the graph).
    build_err(
        "matchlabel",
        "MERGE (n:Company {ticker: 'X'}) SET n.name = 'm3';\nMATCH (n:Company) SET n.name = 'default';\n",
        "MATCH",
    );
}

#[test]
fn merge_without_business_key_is_rejected() {
    // A node pattern must carry exactly one {key: value} identity.
    build_err(
        "nokey",
        "MERGE (n:Company) SET n.name = 'default';\n",
        "exactly one {key: value} entry, got 0",
    );
}
