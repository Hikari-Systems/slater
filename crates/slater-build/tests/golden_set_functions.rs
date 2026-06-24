// SPDX-License-Identifier: Apache-2.0
//! Build-time evaluation of functions in node `SET` clauses (the `slater-scalar`
//! pure-function subset): `coalesce`, string/numeric functions, same-node property
//! references, fold ordering, and the rejection of impure functions / functions on
//! edges.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::Value;
use graph_format::manifest::Manifest;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_setfn_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

/// Build `dump` and return, per `ticker`, that node's `(property_keys, props)`.
fn build_ok(tag: &str, dump: &str) -> (Manifest, HashMap<String, Vec<(u32, Value)>>) {
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

    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut by_ticker = HashMap::new();
    for id in 0..m.node_count {
        let props = np.props(id).unwrap();
        if let Some(Value::Str(t)) = prop(&props, &m.property_keys, "ticker") {
            by_ticker.insert(t.clone(), props.clone());
        }
    }
    let _ = std::fs::remove_dir_all(&work);
    (m, by_ticker)
}

/// Expect the build to fail, with `needle` somewhere in stderr.
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
    assert!(!out.status.success(), "expected a non-zero exit for {tag}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(needle),
        "expected stderr to contain {needle:?}, got: {stderr}"
    );
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn coalesce_and_scalar_functions() {
    // The `name` placeholder pattern, plus a string fn, a numeric fn, and a
    // same-node property reference. Fold is last-writer-wins in file order, and a
    // function/prop reads the props accumulated *before* its statement.
    const DUMP: &str = r#"MERGE (n:Co {ticker: 'A'}) SET n.name = 'Real';
MERGE (n:Co {ticker: 'A'}) SET n.label = coalesce(n.name, n.canonicalName, 'fallback');
MERGE (n:Co {ticker: 'B'}) SET n.label = coalesce(n.name, n.canonicalName, 'fallback');
MERGE (n:Co {ticker: 'C'}) SET n.canonicalName = 'Canon';
MERGE (n:Co {ticker: 'C'}) SET n.label = coalesce(n.name, n.canonicalName, 'fallback');
MERGE (n:Co {ticker: 'U'}) SET n.name = 'hello';
MERGE (n:Co {ticker: 'U'}) SET n.up = toUpper(n.name);
MERGE (n:Co {ticker: 'R'}) SET n.r = round(2.5);
MERGE (n:Co {ticker: 'S'}) SET n.self = n.ticker;
MERGE (n:Co {ticker: 'P'}) SET n.name = coalesce(n.name, n.canonicalName, 'P');
MERGE (n:Co {ticker: 'P'}) SET n.name = 'RealTitle';
MERGE (n:Co {ticker: 'Q'}) SET n.name = 'RealTitle';
MERGE (n:Co {ticker: 'Q'}) SET n.name = coalesce(n.name, n.canonicalName, 'Q');
"#;
    let (m, t) = build_ok("scalars", DUMP);
    let k = &m.property_keys;
    let str_eq = |props: &[(u32, Value)], key: &str, want: &str| {
        assert_eq!(prop(props, k, key), Some(&Value::Str(want.into())), "{key}");
    };

    // coalesce keeps an already-set real name (line 1 set it before the coalesce).
    str_eq(&t["A"], "label", "Real");
    // no name / canonicalName → the literal fallback.
    str_eq(&t["B"], "label", "fallback");
    // canonicalName set in an earlier statement → coalesce picks it.
    str_eq(&t["C"], "label", "Canon");
    // string function over a prop ref.
    str_eq(&t["U"], "up", "HELLO");
    // numeric function — exact Value type.
    assert_eq!(prop(&t["R"], k, "r"), Some(&Value::Float(3.0)));
    // a property reference resolves the identity-seeded business key.
    str_eq(&t["S"], "self", "S");
    // The real-data `slater.import` shape for NCT04595903: a placeholder coalesce
    // first, then the real title as a later literal → last-writer-wins keeps the
    // real title (P). And the upsert-safe reverse order (Q) keeps it too.
    str_eq(&t["P"], "name", "RealTitle");
    str_eq(&t["Q"], "name", "RealTitle");
}

#[test]
fn coalesce_ordering_is_file_order() {
    // A coalesce evaluated BEFORE canonicalName exists must take the literal; a
    // later `SET n.canonicalName` does not retroactively change the earlier result.
    const DUMP: &str = r#"MERGE (n:Co {ticker: 'O'}) SET n.label = coalesce(n.name, n.canonicalName, 'fallback');
MERGE (n:Co {ticker: 'O'}) SET n.canonicalName = 'Late';
"#;
    let (m, t) = build_ok("ordering", DUMP);
    assert_eq!(
        prop(&t["O"], &m.property_keys, "label"),
        Some(&Value::Str("fallback".into())),
        "coalesce ran before canonicalName existed → literal, not retroactively 'Late'"
    );
    assert_eq!(
        prop(&t["O"], &m.property_keys, "canonicalName"),
        Some(&Value::Str("Late".into()))
    );
}

#[test]
fn rejects_impure_function() {
    // Non-deterministic / graph-context functions are not in the build allowlist.
    build_err(
        "timestamp",
        "MERGE (n:Co {ticker: 'A'}) SET n.t = timestamp();\n",
        "not supported in build-time SET",
    );
    build_err(
        "randfn",
        "MERGE (n:Co {ticker: 'A'}) SET n.x = rand();\n",
        "not supported in build-time SET",
    );
}

#[test]
fn rejects_function_on_edge_set() {
    build_err(
        "edgefn",
        "MERGE (a:Co {ticker: 'A'})-[r:T]->(b:Co {ticker: 'B'}) SET r.x = toUpper('a');\n",
        "edge SET supports only literal values",
    );
}
