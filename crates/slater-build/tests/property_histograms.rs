// SPDX-License-Identifier: Apache-2.0
//! Per-(label, property) value→count histograms (`prop_hist.blk`).
//!
//! Three invariants:
//!   1. **Consistency** — within a build, each stored histogram equals the
//!      `(value, count)` pairs independently derived from the node properties in
//!      the dump (and equals what `distinct_key_counts` would walk off the ISAM).
//!   2. **Parity** — the in-memory and external (bounded-memory) builds produce
//!      identical histograms (value→count is permutation-invariant, so the
//!      external build's node-id permutation must not change them).
//!   3. **Cap** — a `(label, property)` whose distinct count exceeds
//!      `--histogram-max-distinct` gets no histogram (the descriptor is absent),
//!      while a low-cardinality one in the same build still does.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use graph_format::histogram::decode_histogram;
use graph_format::ids::Value;
use graph_format::manifest::Manifest;

// label T: `kind` is low-cardinality (x,x,y,z → 3 distinct), `name` is unique
// (4 distinct over 4 nodes). Both are range-indexed.
const DUMP: &str = r#"CREATE INDEX FOR (n:T) ON (n.kind);
CREATE INDEX FOR (n:T) ON (n.name);
CREATE (:T:__DumpVertex__ {__dump_id__: 1, kind: 'x', name: 'n1'});
CREATE (:T:__DumpVertex__ {__dump_id__: 2, kind: 'x', name: 'n2'});
CREATE (:T:__DumpVertex__ {__dump_id__: 3, kind: 'y', name: 'n3'});
CREATE (:T:__DumpVertex__ {__dump_id__: 4, kind: 'z', name: 'n4'});
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_histtest_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Build the DUMP and return the generation dir. `external` toggles the
/// bounded-memory path; `max_distinct` is passed to `--histogram-max-distinct`.
fn build(tag: &str, external: bool, max_distinct: u64) -> PathBuf {
    let work = unique_dir(tag);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let mut args = vec![
        "--input".to_string(),
        input.to_str().unwrap().to_string(),
        "--graph".to_string(),
        "g".to_string(),
        "--data-dir".to_string(),
        data_dir.to_str().unwrap().to_string(),
        "--histogram-max-distinct".to_string(),
        max_distinct.to_string(),
    ];
    if external {
        args.push("--external".to_string());
        args.push("on".to_string());
    }
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(&args)
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build (external={external}) failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    graph_dir.join(gen.trim())
}

/// Decode every stored histogram, keyed by the descriptor's `index_name`.
fn histograms_by_index(gen_dir: &PathBuf, m: &Manifest) -> BTreeMap<String, Vec<(Value, u64)>> {
    let r = graph_format::blockfile::BlockFileReader::open(gen_dir.join("prop_hist.blk")).unwrap();
    assert_eq!(
        r.total_records(),
        m.property_histograms.len() as u64,
        "prop_hist.blk record count must match the descriptor count"
    );
    m.property_histograms
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let rec = r.read_record_global(i as u64).unwrap();
            (d.index_name.clone(), decode_histogram(&rec).unwrap())
        })
        .collect()
}

#[test]
fn histograms_consistent_parity_and_capped() {
    // Default cap: both `kind` and `name` are under 4096, so both are stored.
    let inmem = build("inmem", false, 4096);
    let extern_ = build("extern", true, 4096);

    let mi = Manifest::read_from_dir(&inmem).unwrap();
    let me = Manifest::read_from_dir(&extern_).unwrap();
    mi.verify_content_hash().unwrap();
    me.verify_content_hash().unwrap();

    let hi = histograms_by_index(&inmem, &mi);
    let he = histograms_by_index(&extern_, &me);

    // Consistency: the kind histogram is exactly {x:2, y:1, z:1}, ascending key.
    let want_kind = vec![
        (Value::Str("x".into()), 2u64),
        (Value::Str("y".into()), 1),
        (Value::Str("z".into()), 1),
    ];
    assert_eq!(hi["node_T_kind"], want_kind);
    // name is unique: 4 distinct values, each count 1.
    assert_eq!(hi["node_T_name"].len(), 4);
    assert!(hi["node_T_name"].iter().all(|(_, n)| *n == 1));

    // Parity: in-memory and external builds agree on every histogram (value→count
    // is permutation-invariant), aligned by index name.
    assert_eq!(
        hi, he,
        "histograms differ between in-memory and external builds"
    );

    // The descriptor's distinct_count matches the record length.
    for d in &mi.property_histograms {
        assert_eq!(d.distinct_count, hi[&d.index_name].len() as u64);
    }

    // Cap: with --histogram-max-distinct 2, `kind` (3 distinct) is skipped but a
    // 1-distinct property would still store. Here both kind(3) and name(4) exceed
    // 2, so NO histograms are stored — yet prop_hist.blk still exists (empty).
    let capped = build("capped", false, 2);
    let mc = Manifest::read_from_dir(&capped).unwrap();
    mc.verify_content_hash().unwrap();
    assert!(
        mc.property_histograms.is_empty(),
        "every index exceeds the cap of 2 → no histogram descriptors"
    );
    let r = graph_format::blockfile::BlockFileReader::open(capped.join("prop_hist.blk")).unwrap();
    assert_eq!(r.total_records(), 0, "prop_hist.blk is written but empty");

    // Disabled: --histogram-max-distinct 0 stores nothing either.
    let off = build("off", false, 0);
    let mo = Manifest::read_from_dir(&off).unwrap();
    assert!(mo.property_histograms.is_empty());
}
