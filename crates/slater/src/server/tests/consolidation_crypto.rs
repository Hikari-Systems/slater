// SPDX-License-Identifier: Apache-2.0
//! `consolidation_crypto` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Consolidating an encrypted graph through the **production** builder invocation must
/// publish an **encrypted** generation.
///
/// Every other encrypted-consolidation test here passes its own `build` closure carrying
/// `--encrypt --key-env`, i.e. it exercises a `slater-build` invocation production never
/// makes. `run_builder` — the closure production actually supplies — passed no key and no
/// `--encrypt` at all, so a real consolidation published the whole graph as a **plaintext**
/// generation and then failed at the swap on HIK-144's require-a-MAC policy. The graph kept
/// serving its old core, so nothing was wrong-answered; the damage was a permanent
/// clear-text copy of everything, plus consolidation that could never succeed on a keyed
/// deployment (HIK-157).
///
/// This test therefore calls `run_builder` directly. That is the point of it — the
/// `build`-closure seam is precisely what let every existing test miss this.
///
/// Note on the assertions: `encryption` and `mac` serialise as explicit `null` when absent,
/// so `serde_json`'s `get()` returns `Some(Null)` for a plaintext manifest. Asserting the
/// field is merely *present* passes against the bug. Assert non-null.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_production_consolidation_of_an_encrypted_graph_publishes_an_encrypted_generation() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_prodenc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    let key_hex = "0123456789abcdef0123456789abcdef";
    let key = graph_format::crypto::hex_decode(key_hex).unwrap();

    // Two markers, because they leak through different files and one alone would be a
    // weaker test: the *value* is node content (`nodes.blk`), the *property key* is a
    // symbol-table entry (`meta.json`). HIK-149 leaked both.
    let script = format!(
        "CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);\n\
         CREATE (:Doc:__DumpVertex__ {{__dump_id__: 0, {MARKER_KEY}: '{MARKER_VALUE}'}});\n\
         CREATE (:Doc:__DumpVertex__ {{__dump_id__: 1, {MARKER_KEY}: 'beta'}});\n\
         MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n\
         DROP INDEX ON :__DumpVertex__(__dump_id__);\n"
    );
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();

    assert!(
        std::process::Command::new(&bin)
            .args(["--input", input.to_str().unwrap()])
            .args(["--pk", "__dump_id__"])
            .args(["--cluster", "none"])
            .args(["--graph", "docs"])
            .args(["--data-dir", data.to_str().unwrap()])
            .arg("--encrypt")
            .args(["--key-env", "SLATER_PRODENC_KEY"])
            .env("SLATER_PRODENC_KEY", key_hex)
            .status()
            .expect("spawn slater-build")
            .success(),
        "the encrypted fixture build must succeed"
    );

    let mut graphs = Graphs::open_all(&data, Some(&key)).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);
    let base = graphs.get("docs").unwrap().uuid();

    // THE PRODUCTION PATH — `run_builder`, not a bespoke closure. The closure is handed the
    // scratch dump directory, which is the one moment it exists on disk, so HIK-149's
    // assertion goes here: on an encrypted graph the dump must be sealed too.
    let new_uuid = graphs
        .consolidate_graph("docs", &cache, &vc, &data, |d, g, dd, k, _acl| {
            assert_dump_is_sealed(d);
            crate::server::run_builder(&bin, d, g, dd, k, BuilderLimits::default(), None, None)
        })
        .expect("a consolidation of an encrypted graph must succeed");
    assert_ne!(
        new_uuid, base,
        "a fresh generation must have been published"
    );

    let mani: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            data.join("docs")
                .join(new_uuid.0.to_string())
                .join("MANIFEST.json"),
        )
        .expect("read the consolidated manifest"),
    )
    .unwrap();
    assert!(
        mani.get("encryption").is_some_and(|v| !v.is_null()),
        "the consolidated generation must be encrypted, got encryption={:?}",
        mani.get("encryption")
    );
    assert!(
        mani.get("mac").is_some_and(|v| !v.is_null()),
        "the consolidated generation must be MAC-sealed, got mac={:?}",
        mani.get("mac")
    );

    // And it must actually be servable under the key — the swap succeeding already implies
    // it, but assert a read so this cannot pass on a manifest inspection alone.
    let served = graphs.get("docs").unwrap();
    assert_eq!(served.uuid(), new_uuid);
    assert_eq!(served.node_count(), 2);

    let _ = std::fs::remove_dir_all(&work);
}

/// The **`keyEnv` route** end to end: when the server's own key came from an environment
/// variable, the builder is handed `--key-env <VAR>` instead of a piped stdin, and must
/// publish a correctly encrypted, MAC-sealed, servable generation.
///
/// This is not a loosening. `std::process::Command` inherits the parent's environment and
/// `run_builder` never clears it, so under `keyEnv` the key is already in the child's
/// environment block for the whole rebuild whether or not we name the variable — measured,
/// not assumed. What the routing removes is a piped stdin (and its drain-ordering hazard)
/// that was buying nothing in this configuration. The `keyFile` route still uses stdin and
/// is covered by `a_production_consolidation_of_an_encrypted_graph_publishes_an_encrypted_generation`
/// above, so both key sources have a test — the "two configurations with no test in common"
/// gap that produced HIK-145 and HIK-157 stays closed.
///
/// Sets a process-wide env var, so it must run single-threaded; the CI job passes
/// `--test-threads 1`.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_key_env_consolidation_forwards_the_variable_and_publishes_an_encrypted_generation() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_keyenv_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    const VAR: &str = "SLATER_KEYENV_ROUTE_KEY";
    let key_hex = "0123456789abcdef0123456789abcdef";
    let key = graph_format::crypto::hex_decode(key_hex).unwrap();
    // The server's own source. The child inherits it — that is the whole point.
    std::env::set_var(VAR, key_hex);

    let script = format!(
        "CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);\n\
         CREATE (:Doc:__DumpVertex__ {{__dump_id__: 0, {MARKER_KEY}: '{MARKER_VALUE}'}});\n\
         CREATE (:Doc:__DumpVertex__ {{__dump_id__: 1, {MARKER_KEY}: 'beta'}});\n\
         MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n\
         DROP INDEX ON :__DumpVertex__(__dump_id__);\n"
    );
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();
    assert!(
        std::process::Command::new(&bin)
            .args(["--input", input.to_str().unwrap()])
            .args(["--pk", "__dump_id__"])
            .args(["--cluster", "none"])
            .args(["--graph", "docs"])
            .args(["--data-dir", data.to_str().unwrap()])
            .arg("--encrypt")
            .args(["--key-env", VAR])
            .status()
            .expect("spawn slater-build")
            .success(),
        "the encrypted fixture build must succeed"
    );

    let mut graphs = Graphs::open_all(&data, Some(&key)).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);
    let base = graphs.get("docs").unwrap().uuid();

    // `Some(VAR)` is the production wiring for a `keyEnv` deployment: no stdin is piped,
    // and the builder resolves the key from the environment it inherited.
    let new_uuid = graphs
        .consolidate_graph("docs", &cache, &vc, &data, |d, g, dd, k, _acl| {
            assert_dump_is_sealed(d);
            crate::server::run_builder(&bin, d, g, dd, k, BuilderLimits::default(), Some(VAR), None)
        })
        .expect("a keyEnv-routed consolidation must succeed");
    assert_ne!(
        new_uuid, base,
        "a fresh generation must have been published"
    );

    let mani: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            data.join("docs")
                .join(new_uuid.0.to_string())
                .join("MANIFEST.json"),
        )
        .expect("read the consolidated manifest"),
    )
    .unwrap();
    assert!(
        mani.get("encryption").is_some_and(|v| !v.is_null()),
        "the keyEnv-routed generation must be encrypted, got encryption={:?}",
        mani.get("encryption")
    );
    assert!(
        mani.get("mac").is_some_and(|v| !v.is_null()),
        "the keyEnv-routed generation must be MAC-sealed, got mac={:?}",
        mani.get("mac")
    );

    // Servable under the key, so this cannot pass on manifest inspection alone.
    let served = graphs.get("docs").unwrap();
    assert_eq!(served.uuid(), new_uuid);
    assert_eq!(served.node_count(), 2);

    std::env::remove_var(VAR);
    let _ = std::fs::remove_dir_all(&work);
}

/// HIK-145, found while adversarially reviewing the fix: making the encrypted carry *work*
/// must not make a **downgrade** work too.
///
/// A carry never rewrites the graph's bytes — that is its entire purpose — so it cannot
/// change how they are sealed. Turning on `--encrypt` over a base whose `.vamana` is
/// plaintext would therefore publish an "encrypted" generation serving an unencrypted ~370 GB
/// vector graph, with no error and nothing in the manifest to show it. Before this ticket
/// that combination failed by accident (the reader tried to decrypt plaintext blocks); the
/// fix must refuse it deliberately, not inherit the accident and then lose it.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_keyed_consolidation_refuses_to_carry_a_plaintext_vector_graph() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_vamana_mixed_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    let (script, _vectors) = vamana_fixture_script(16, 400);
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();

    // A **plaintext** base.
    assert!(
        std::process::Command::new(&bin)
            .args(["--input", input.to_str().unwrap()])
            .args(["--graph", "docs"])
            .args(["--data-dir", data.to_str().unwrap()])
            .args(["--pk", "__dump_id__"])
            .args(["--cluster", "none"])
            .args(["--ann-threshold", "50"])
            .args(["--pq-subspaces", "8"])
            .args(["--pq-bits", "8"])
            .status()
            .expect("spawn slater-build")
            .success(),
        "the plaintext fixture build must succeed"
    );

    let mut graphs = Graphs::open_all(&data, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);

    // …consolidated by a build that has suddenly been given `--encrypt`.
    let err =
        match graphs.consolidate_graph("docs", &cache, &vc, &data, |d, _g, _dd, _key, _acl| {
            let st = std::process::Command::new(&bin)
                .arg("--input")
                .arg(d)
                .args(["--input-format", "slater-dump"])
                .args(["--graph", "docs"])
                .args(["--data-dir", data.to_str().unwrap()])
                .args(["--ann-threshold", "50"])
                .args(["--pq-subspaces", "8"])
                .args(["--pq-bits", "8"])
                .arg("--encrypt")
                .args(["--key-env", "SLATER_HIK145_MIXED_KEY"])
                .env(
                    "SLATER_HIK145_MIXED_KEY",
                    "0123456789abcdef0123456789abcdef",
                )
                .status()
                .context("spawn builder")?;
            anyhow::ensure!(
                st.success(),
                "keyed build over a plaintext base failed: {st}"
            );
            Ok(())
        }) {
            Ok(_) => {
                panic!(
                    "a keyed build must not carry a plaintext vector graph into an encrypted image"
                )
            }
            Err(e) => e,
        };
    // The refusal happens inside the builder, so it reaches the server as a failed build.
    // Which of the two gates fires is not the property under test — in practice the earlier
    // one does, because a keyed build authenticating the plaintext base generation's
    // `MANIFEST.json` already refuses it as MAC-less (HIK-144). What matters, and what is
    // asserted, is that no downgraded generation is published.
    let _ = &err;

    // The served generation is unchanged: the failed build published nothing.
    assert!(
        graphs.get("docs").unwrap().manifest().encryption.is_none(),
        "a refused consolidation must leave the plaintext generation serving"
    );
    std::fs::remove_dir_all(&work).ok();
}

/// The **Vamana** arm of the same gate — and, since HIK-117, the server-level proof of
/// **carry-by-reference**. A Vamana index's full vectors live in its `.vamana` blocks; the
/// consolidation no longer streams them back out (the ~370 GB read at scale) and no longer
/// rebuilds the graph from zero. Instead the dump carries a reference to the base
/// `.vamana`/`.pq` plus a `layout → new-id` map, and the builder folds the (here empty) Δ in
/// with `streaming_merge`. With no deletes and no Δ that is the pure-permutation fast path,
/// so the new generation's `.vamana` is **byte-identical** to the base's — the whole thesis
/// of the FreshDiskANN write ladder, asserted end-to-end through the dump and the forked
/// builder, not just the in-crate primitive.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn consolidate_carries_a_vamana_index_out_of_its_vamana_blocks() {
    use graph_format::manifest::AnnMode;

    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_vamana_consol_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    // 400 × dim-16 cosine vectors: enough to clear an `--ann-threshold 50`, and
    // `pq_subspaces = 8` divides 16, so the index is Vamana-eligible (D29).
    let (dim, n) = (16usize, 400usize);
    let mut seed: u64 = 0xDEAD_BEEF_1234;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let mut script =
        format!("CALL db.idx.vector.createNodeIndex('Doc', 'embedding', {dim}, 'cosine');\n");
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    for i in 0..n {
        let v: Vec<f32> = (0..dim).map(|_| next()).collect();
        let body: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
        script.push_str(&format!(
            "CREATE (:Doc:__DumpVertex__ {{__dump_id__: {i}, embedding: vecf32([{}])}});\n",
            body.join(", ")
        ));
        vectors.push(v);
    }
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();

    let ok = std::process::Command::new(&bin)
        .args(["--input", input.to_str().unwrap()])
        .args(["--graph", "docs"])
        .args(["--data-dir", data.to_str().unwrap()])
        .args(["--pk", "__dump_id__"])
        .args(["--cluster", "none"])
        .args(["--ann-threshold", "50"])
        .args(["--pq-subspaces", "8"])
        .args(["--pq-bits", "8"])
        .status()
        .expect("spawn slater-build")
        .success();
    assert!(ok, "the fixture build must succeed");

    let mut graphs = Graphs::open_all(&data, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);

    let gen0 = graphs.get("docs").unwrap();
    let desc0 = &gen0.manifest().vector_indexes[0];
    assert!(
        matches!(desc0.mode, AnnMode::Vamana { .. }),
        "the fixture must actually be a Vamana index, else this proves nothing"
    );
    assert_eq!(desc0.count, n as u64);
    // Snapshot the base `.vamana` bytes *before* the consolidation, to prove afterward that
    // carry-by-reference left the graph file untouched (the pure-permutation fast path).
    let base_vamana = data
        .join("docs")
        .join(gen0.base_uuid().to_string())
        .join("vector/Doc.embedding.vamana");
    let base_bytes = std::fs::read(&base_vamana).expect("base .vamana must exist");

    graphs
        .consolidate_graph("docs", &cache, &vc, &data, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();

    let gen1 = graphs.get("docs").unwrap();
    let vidx = &gen1.manifest().vector_indexes;
    assert_eq!(
        vidx.len(),
        1,
        "the vector index must survive consolidation of a Vamana graph"
    );
    assert_eq!(
        vidx[0].count, n as u64,
        "every vector must be carried out of the .vamana blocks — a 0 here is the \
             'read the wrong store' bug, which is silent by construction"
    );
    // Carry-by-reference: the index stays Vamana (not rebuilt as brute-force), and its
    // `.vamana` is byte-identical to the base — the graph was carried, not reconstructed.
    assert!(
        matches!(vidx[0].mode, AnnMode::Vamana { .. }),
        "a carried Vamana base must stay Vamana, not be rebuilt as brute-force"
    );
    // HIK-145: the carried graph now lives in its own `vecidx/<uuid>/` artifact rather than
    // inside the new generation's directory — one carry path for plaintext and encrypted
    // alike, because two structurally different paths are exactly what let the encrypted
    // arm go untested for the whole life of the feature. What must not regress for an
    // unencrypted deployment is the *optimisation*: byte-identical, and still a hard link
    // (the same inode as the base), never a 370 GB copy.
    let carried = carried_vamana_bytes(&data, "docs", gen1.as_ref());
    assert_eq!(
        carried, base_bytes,
        "a pure-permutation consolidation must carry the .vamana byte-identically — this is \
             the BLAKE3-unchanged thesis at the server level"
    );
    let vecidx = data.join("docs").join("vecidx");
    let artifact_file = std::fs::read_dir(&vecidx)
        .expect("a carry must publish a vecidx/ artifact")
        .map(|e| e.unwrap().path().join("Doc.embedding.vamana"))
        .find(|p| p.exists())
        .expect("the artifact must hold the carried graph file");
    assert_eq!(
        same_inode(&artifact_file, &base_vamana),
        Some(true),
        "the plaintext carry must still be a hard link to the base inode, not a copy"
    );

    // The data has to be the real thing, not zeros: query with node 7's own embedding
    // and it must come back first, at distance ~0. (`--cluster none` ⇒ dense id == i.)
    let probe: Vec<String> = vectors[7].iter().map(|x| format!("{x:.6}")).collect();
    let view = MergedView::read_only(gen1.as_ref());
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', 1, vecf32([{}])) \
             YIELD node, score RETURN id(node) AS id, score",
        probe.join(", ")
    ))
    .unwrap();
    // The carried index is Vamana, so serving KNN needs the vector-index cache the
    // consolidation pinned it into (a brute-force rebuild would not have).
    let res = Engine::new(&view, &cache)
        .with_vector_cache(&vc, 96)
        .run(&ast)
        .unwrap();
    assert_eq!(res.rows.len(), 1, "the carried index must return a hit");
    assert!(
        matches!(res.rows[0][0], Val::Int(7)),
        "a node's own embedding must be its own nearest neighbour, got {:?}",
        res.rows[0][0]
    );
    let Val::Float(score) = res.rows[0][1] else {
        panic!("score should be a float");
    };
    assert!(
        score.abs() < 1e-5,
        "an exact match must score ~0 (cosine is scale-invariant, so the .vamana file's \
             normalised vectors round-trip exactly); got {score}"
    );
    std::fs::remove_dir_all(&work).ok();
}

/// Every operation of the write grammar, so a new one cannot be added without being
/// listed here. Each must parse to a mutating statement.
fn every_write_statement() -> Vec<&'static str> {
    vec![
        // ── node writes (the grammar requires a SET or DELETE after the pattern,
        //    so a bare `MERGE (n:L {k:v})` is not a valid statement) ───────────
        "MERGE (n:Person {name:'Dave'}) SET n.age = 1",
        "MATCH (n:Person {name:'Alice'}) SET n.age = 1",
        "MATCH (n:Person {name:'Alice'}) SET n.age = 1, n.city = 'Oslo'",
        "MATCH (n:Person {name:'Alice'}) DELETE n",
        "MATCH (n:Person {name:'Alice'}) DETACH DELETE n",
        // ── batched (write-`UNWIND`) node writes ─────────────────────────────
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
        "UNWIND $rows AS r MATCH (n:Person {name: r.name}) SET n.age = r.age",
        "UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n",
        // ── relationship writes ──────────────────────────────────────────────
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})",
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 2020",
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
        // ── batched (write-`UNWIND`) relationship writes ─────────────────────
        //    MERGE only: `edge_delete` has no UNWIND prefix (see `cypher.pest`).
        "UNWIND $rows AS e MERGE (a:Person {name: e.src})-[r:KNOWS]->(b:Person {name: e.dst})",
        "UNWIND $rows AS e MATCH (a:Person {name: e.src}) MATCH (b:Person {name: e.dst}) \
         MERGE (a)-[r:KNOWS {uuid: e.uuid}]->(b) SET r = e RETURN r.uuid AS uuid",
        // ── admin: rewrites the served generation ────────────────────────────
        "CALL slater.consolidate()",
    ]
}

/// **A read grant must not authorise any write.** Before the writable layer landed the
/// ACL had only `can_read`, so switching on `delta.enabled` would silently have promoted
/// every reader into a writer. Every operation of the write grammar is checked.
#[test]
fn a_read_only_grant_forbids_every_write_operation() {
    let read_only = acl_json(serde_json::json!({ "people": ["read"] }));
    for q in every_write_statement() {
        let stmt = parser::parse_statement(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
        assert!(
            statement_mutates(&stmt),
            "{q} must be classified as a mutating statement"
        );
        let err = authorize_statement(&read_only, "u", "people", &stmt).expect_err(q);
        assert_eq!(err.code, CODE_FORBIDDEN, "{q}");
        assert!(err.message.contains("write access"), "{q}: {}", err.message);
    }
}

/// The same statements are authorised once the user also holds `write`.
#[test]
fn a_read_write_grant_authorises_every_write_operation() {
    let rw = acl_json(serde_json::json!({ "people": ["read", "write"] }));
    for q in every_write_statement() {
        let stmt = parser::parse_statement(q).unwrap();
        authorize_statement(&rw, "u", "people", &stmt)
            .unwrap_or_else(|e| panic!("read+write must authorise {q}: {}", e.message));
    }
}
