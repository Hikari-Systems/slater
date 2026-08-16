// SPDX-License-Identifier: Apache-2.0
//! `generation_guard` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Generation guard (M8) ──────────────────────────────────────────────

#[test]
fn swap_refuses_a_truncated_new_generation() {
    let (root, _g, old) = testgen::write_basic("guard_swap_refuse");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // A half-copied (truncated) new generation is published under `current`.
    publish_copy_as_new_generation(&root, "people", Some("node_props.blk"));
    let err = guard_swap(&graphs, "people", &vc).err().unwrap();
    assert!(
        err.chain().any(|e| e.to_string().contains("integrity")),
        "unexpected error: {err:#}"
    );
    // The live generation is untouched — the corrupt copy never took over.
    assert_eq!(graphs.get("people").unwrap().uuid().0, old);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_applies_a_valid_new_generation_while_in_flight_reads_the_old() {
    let (root, _g, old) = testgen::write_basic("guard_swap_apply");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // An in-flight query's snapshot, taken before the swap.
    let in_flight = graphs.get("people").unwrap();

    let new = publish_copy_as_new_generation(&root, "people", None);
    let swapped = guard_swap(&graphs, "people", &vc).unwrap();
    assert_eq!(swapped.map(|g| g.0), Some(new));

    // New queries see the new generation; the in-flight handle still reads old.
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    assert_eq!(in_flight.uuid().0, old);

    // A second swap with no further change on disk is a clean no-op.
    assert!(guard_swap(&graphs, "people", &vc).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&root);
}

/// The publish primitive that consolidation, flush and compaction all share. Flush and
/// compaction have no injectable seam inside their publish window (they write `current`
/// themselves), so the property the guard race turns on is asserted here directly: once
/// an op has published a generation, `adopt_published_generation` gives it the same
/// answer — *this is the served generation* — whether the op swapped it in itself or
/// the guard got there first. It never reports "nothing was published" for a generation
/// that was.
#[test]
fn adopt_published_generation_is_idempotent_after_a_racing_swap() {
    let (root, _g, old) = testgen::write_basic("adopt_after_race");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // Nothing published: the answer is the generation already served. This is how a
    // caller detects a builder that exited 0 without publishing anything (it compares
    // against the core it started from) — the check the old `Ok(None)` used to make.
    assert_eq!(
        graphs
            .adopt_published_generation("people", &vc)
            .unwrap()
            .uuid()
            .0,
        old,
        "an unchanged pointer reports the served generation"
    );

    // Now an op publishes, and the guard wins the swap. The op must still get *its own*
    // generation back — the whole point: its post-swap cleanup is keyed off this answer.
    let new = publish_copy_as_new_generation(&root, "people", None);
    assert_eq!(
        guard_swap(&graphs, "people", &vc).unwrap().map(|g| g.0),
        Some(new),
        "the guard swapped first"
    );
    assert_eq!(
        graphs
            .adopt_published_generation("people", &vc)
            .unwrap()
            .uuid()
            .0,
        new,
        "the op adopts the generation it published, whoever performed the swap"
    );
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn exit_strategy_guard_sweep_signals_shutdown_on_change() {
    let (root, _g, _) = testgen::write_basic("guard_exit_sweep");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // No change yet → keep serving.
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None),
        SweepAction::Continue
    ));

    // A changed `current` → shutdown signal naming the graph. Exit does not even
    // open the new generation — the orchestrator restart re-opens it cleanly.
    publish_copy_as_new_generation(&root, "people", None);
    match guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None) {
        SweepAction::Shutdown(name) => assert_eq!(name, "people"),
        SweepAction::Continue => panic!("expected a shutdown signal on a changed current"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_strategy_guard_sweep_swaps_in_place() {
    let (root, _g, old) = testgen::write_basic("guard_swap_sweep");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    let new = publish_copy_as_new_generation(&root, "people", None);
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Swap, None),
        SweepAction::Continue
    ));
    assert_ne!(new, old);
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-93: the generation guard must not adopt an `acl.json` that fails the served
/// ACL stamp. The swap's own policy check hashes the live `acl.json` (read #1); the
/// post-swap ACL adopt is a *second* read, and if it re-reads the file unconditionally
/// (the old `reload()`), the bytes it loads need not be the bytes read #1 verified — a
/// check-then-load TOCTOU. The adopt now goes through `reload_checked`, which hashes
/// the exact bytes it loads and installs them only when that digest still matches every
/// served generation's stamp, so the bytes loaded are the bytes checked.
///
/// The race is made deterministic with two graphs and no threads: `people` is stamped
/// against `acl.json` bytes A (it pins the ACL); `docs` is unstamped and is the graph
/// the guard legitimately swaps (its swap policy check passes regardless of the live
/// ACL). Before the sweep, `acl.json` is tampered to bytes B (a `secret` self-grant),
/// standing in for a file that changed after read #1. The guard swaps `docs` and then
/// adopts the ACL: the old unconditional `reload()` adopted B unverified (self-grant
/// live); `reload_checked` re-checks B against `people`'s stamp `digest(A)`, mismatches,
/// and keeps the last-good ACL.
#[test]
fn guard_swap_refuses_a_stamp_violating_acl_after_the_swap() {
    // Two graphs in one root: `people` (from the fixture) and a copy `docs`. The
    // manifest embeds the graph name, so re-stamp the copy's `graph` field to "docs"
    // (a field content_hash does not cover, and the plaintext fixture carries no MAC).
    let (root, _g, _) = testgen::write_basic("guard_acl_toctou");
    copy_dir_all(&root.join("people"), &root.join("docs"));
    patch_manifest(&root, "docs", "graph", serde_json::json!("docs"));

    // acl.json bytes A grant `reporting`/`pw` read on `people`. Stamp only `people`
    // with digest(A); `docs` stays unstamped, so the guard may swap it freely.
    let acl_path = write_acl(&root);
    let digest_a = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(digest_a));

    let acl = AclHandle::load(&acl_path).unwrap();
    assert!(acl.snapshot().can_read("reporting", "people"));
    assert!(!acl.snapshot().can_read("reporting", "secret"));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), false);
    let vc = VectorIndexCache::new(1 << 20);

    // Publish a fresh generation for the *unstamped* `docs`, so the guard swaps it.
    publish_copy_as_new_generation(&root, "docs", None);

    // Tamper acl.json to bytes B: a `secret` self-grant. digest(B) != digest(A), so B
    // violates `people`'s served stamp (a fresh argon2 salt alone already diverges A).
    let tampered = serde_json::json!({
        "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"], "secret": ["read"] } } }
    });
    std::fs::write(&acl_path, tampered.to_string()).unwrap();

    // Sweep: swaps `docs`, then adopts the ACL through the stamp gate.
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Swap, Some(&acl)),
        SweepAction::Continue
    ));
    // `docs` really was swapped, so the adopt path ran.
    assert_ne!(
        graphs.get("docs").unwrap().uuid().0,
        graphs.get("people").unwrap().uuid().0,
        "the guard should have swapped docs to its new generation"
    );

    // The stamp-violating self-grant must NOT have been adopted (pre-fix: it was),
    // and the last-good stamp-matching ACL keeps serving.
    assert!(
        !acl.snapshot().can_read("reporting", "secret"),
        "guard must not adopt an acl.json that violates a served generation's stamp"
    );
    assert_eq!(
        acl.digest(),
        digest_a,
        "the stamp-matching last-good ACL must be kept"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_moves_pinned_pq_from_the_old_generation_to_the_new() {
    let f = testgen::VamanaFixture {
        n: 64,
        dim: 8,
        r: 16,
        alpha: 1.2,
        pq_subspaces: 4,
        pq_bits: 6,
        vector_block_size: 1024,
    };
    let (root, _g, _) = testgen::write_vamana("guard_swap_pq", &f);
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // Pin the live generation's resident PQ, as `serve` does at startup.
    let old = graphs.get("docs").unwrap();
    for vi in old.vamana_indexes() {
        vc.pin(old.uuid(), vi.ord, vi.pq.clone());
    }
    assert!(vc.resident_pq(old.uuid(), 0).is_some());

    let new = publish_copy_as_new_generation(&root, "docs", None);
    guard_swap(&graphs, "docs", &vc).unwrap();

    // The new generation's PQ is now pinned and the old generation's released —
    // so the pool's resident set tracks the live generation (D32).
    assert!(
        vc.resident_pq(GenId(new), 0).is_some(),
        "new generation PQ should be pinned"
    );
    assert!(
        vc.resident_pq(old.uuid(), 0).is_none(),
        "old generation PQ should be unpinned after swap"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn exit_strategy_guard_task_signals_shutdown_over_oneshot() {
    let (root, _g, _) = testgen::write_basic("guard_exit_task");
    let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
    let vc = Arc::new(VectorIndexCache::new(1 << 20));
    let (tx, rx) = tokio::sync::oneshot::channel();
    // A tight poll interval so the test does not wait the production default.
    spawn_generation_guard(
        graphs.clone(),
        vc,
        ReloadStrategy::Exit,
        Duration::from_millis(20),
        tx,
        None,
    );

    publish_copy_as_new_generation(&root, "people", None);
    let reason = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("guard should fire within the timeout")
        .expect("the shutdown sender should not be dropped");
    assert_eq!(reason, "people");
    let _ = std::fs::remove_dir_all(&root);
}
