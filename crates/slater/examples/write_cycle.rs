// SPDX-License-Identifier: Apache-2.0
//! Writable-layer exercise driver against an already-running slater server.
//!
//! Connects over Bolt to an external server (env `WC_HOST`/`WC_PORT`) serving the
//! 91M Wikidata core with the delta layer enabled, then runs the full
//! mostly-reads-some-writes cycle the task asks for and prints timed phase
//! summaries plus `CALL slater.diagnostics()`:
//!
//!   1. basic correctness / availability probes over the pure core (via overlay);
//!   2. ADD 0.5% of core volume as delta-born nodes, in *varying* batch sizes;
//!   3. EDIT the same volume, split across core nodes and the new born nodes;
//!   4. DELETE 100, split across core and new;
//!   5. re-verify counts + read-back, then dump the diagnostics snapshot.
//!
//! Run (after starting the server):
//!   WC_PORT=7699 WC_USER=wc WC_PASS=wcpw WC_GRAPH=wd91m_fixed \
//!     cargo run --release -p slater --example write_cycle

use slater::bolt::client::BoltClient;
use slater::bolt::packstream::PsValue;
use std::time::{Duration, Instant};

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}
fn envn(k: &str, d: i64) -> i64 {
    std::env::var(k)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(d)
}

/// Connection parameters, so a probe can open its own isolated session.
struct Conn {
    host: String,
    port: u16,
    user: String,
    pass: String,
}

impl Conn {
    fn open(&self) -> std::io::Result<BoltClient> {
        let mut c = BoltClient::connect(&self.host, self.port, Duration::from_secs(120))?;
        c.login("write-cycle/1", &self.user, &self.pass)?;
        Ok(c)
    }
}

fn ints(c: &mut BoltClient, g: &str, q: &str) -> Vec<i64> {
    let (_c, rows) = c
        .run_pull(q, Some(g))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    rows.iter()
        .filter_map(|r| r.first().and_then(|v| v.as_int()))
        .collect()
}

/// Whole-graph `count(*)` as a **non-fatal, timed** probe. Runs on its own short-lived
/// connection: a query that breaches a budget leaves the Bolt session FAILED until a
/// RESET, so a failure here must not poison the driver's main session. Both the labelled
/// and unlabelled shapes are probed — they take different fast-path arms.
fn count_all(conn: &Conn, g: &str, label: &str) {
    for q in [
        "MATCH (n:Entity) RETURN count(*)",
        "MATCH (n) RETURN count(*)",
    ] {
        let t = Instant::now();
        let mut c = match conn.open() {
            Ok(c) => c,
            Err(e) => {
                println!("  [{label}] connect failed: {e}");
                return;
            }
        };
        match c.run_pull(q, Some(g)) {
            Ok((_c, rows)) => {
                let n = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_int())
                    .unwrap_or(-1);
                println!("  [{label}] {q:34} = {n}  ({:?})", t.elapsed());
            }
            Err(e) => println!("  [{label}] {q:34} FAILED after {:?}: {e}", t.elapsed()),
        }
    }
}
/// Bounded, index-served count of just the delta-born range (cheap even with a delta).
fn born_count(c: &mut BoltClient, g: &str, base: i64) -> i64 {
    ints(
        c,
        g,
        &format!("MATCH (n:Entity) WHERE n.wikidata_id >= {base} RETURN count(*)"),
    )[0]
}
fn strs(c: &mut BoltClient, g: &str, q: &str) -> Vec<String> {
    let (_c, rows) = c
        .run_pull(q, Some(g))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    rows.iter()
        .filter_map(|r| r.first().and_then(|v| v.as_str().map(str::to_string)))
        .collect()
}

/// One group-committed write-UNWIND batch. Returns wall time.
fn batch(c: &mut BoltClient, g: &str, cypher: &str, rows: Vec<PsValue>) -> Duration {
    let t = Instant::now();
    c.run_pull_params(cypher, vec![("rows".into(), PsValue::List(rows))], Some(g))
        .unwrap_or_else(|e| panic!("batch failed\n  {e}"));
    t.elapsed()
}

fn main() {
    let host = env("WC_HOST", "127.0.0.1");
    let port: u16 = env("WC_PORT", "7699").parse().unwrap();
    let user = env("WC_USER", "wc");
    let pass = env("WC_PASS", "wcpw");
    let g = env("WC_GRAPH", "wd91m_fixed");

    let add_total = envn("WC_ADD", 458_000) as usize;
    let edit_total = envn("WC_EDIT", 458_000) as usize;
    let del_total = envn("WC_DELETE", 100) as usize;
    // Base id for delta-born nodes: far above any real Wikidata Q-number in the sample.
    let born_base: i64 = envn("WC_BORN_BASE", 10_000_000_000);
    // Varying batch sizes, cycled until the target is reached.
    let sizes: Vec<usize> = env("WC_BATCH_SIZES", "1000,2000,5000,10000,25000,50000")
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();

    let conn = Conn {
        host: host.clone(),
        port,
        user: user.clone(),
        pass: pass.clone(),
    };
    let mut c = conn.open().expect("connect+login");
    println!("connected to {host}:{port} graph={g}");

    // ── 1. Basic correctness / availability over the core (through the overlay). ──
    println!("\n== PHASE 1: baseline correctness / availability ==");
    let t = Instant::now();
    let c0 = ints(&mut c, &g, "MATCH (n:Entity) RETURN count(*)")[0];
    println!("  count(*) :Entity = {c0}  ({:?})", t.elapsed());
    let t = Instant::now();
    let seek = strs(
        &mut c,
        &g,
        "MATCH (n:Entity {wikidata_id: 412684}) RETURN n.name",
    );
    println!("  indexed seek id 412684 -> {seek:?}  ({:?})", t.elapsed());
    let t = Instant::now();
    let hop = ints(
        &mut c,
        &g,
        "MATCH (n:Entity {wikidata_id: 412684})-[]->(m) RETURN count(m)",
    );
    println!(
        "  1-hop out-degree of 412684 = {hop:?}  ({:?})",
        t.elapsed()
    );

    // ── Fetch real core ids to edit + core-delete (ascending via the range index). ──
    let core_edit = edit_total / 2;
    let core_del = del_total / 2;
    let need = core_edit + core_del;
    println!("\n  fetching {need} real core ids for edit/delete ...");
    let t = Instant::now();
    let core_ids = ints(
        &mut c,
        &g,
        &format!("MATCH (n:Entity) WHERE n.wikidata_id >= 0 RETURN n.wikidata_id LIMIT {need}"),
    );
    println!("  fetched {} core ids ({:?})", core_ids.len(), t.elapsed());
    assert!(
        core_ids.len() >= need,
        "need {need} core ids, got {}",
        core_ids.len()
    );
    // Witness: an untouched core node whose id is strictly above every edit/delete id
    // (a cheap index seek just past the fetched ascending window).
    let hi = *core_ids.iter().max().unwrap();
    let witness = ints(
        &mut c,
        &g,
        &format!("MATCH (n:Entity) WHERE n.wikidata_id > {hi} RETURN n.wikidata_id LIMIT 1"),
    )[0];
    let wname = strs(
        &mut c,
        &g,
        &format!("MATCH (n:Entity {{wikidata_id: {witness}}}) RETURN n.name"),
    );
    println!("  witness id {witness} name = {wname:?}");
    assert!(!core_ids.contains(&witness), "witness must be untouched");

    // ── 2. ADD 0.5% volume as delta-born nodes, in VARYING batch sizes. ──
    println!("\n== PHASE 2: ADD {add_total} born nodes (0.5% of core) in varying batches ==");
    let t_add = Instant::now();
    let mut added = 0usize;
    let mut si = 0usize;
    let mut nbatches = 0usize;
    let mut slow: Vec<(usize, usize, u128)> = Vec::new(); // (batch#, size, ms) outliers
    while added < add_total {
        let sz = sizes[si % sizes.len()].min(add_total - added);
        si += 1;
        let rows: Vec<PsValue> = (added..added + sz)
            .map(|i| {
                let id = born_base + i as i64;
                PsValue::Map(vec![("id".into(), PsValue::Int(id))])
            })
            .collect();
        let d = batch(
            &mut c,
            &g,
            "UNWIND $rows AS r MERGE (n:Entity {wikidata_id: r.id}) SET n.name = 'wc-born'",
            rows,
        );
        nbatches += 1;
        if d.as_millis() > 1500 {
            slow.push((nbatches, sz, d.as_millis()));
        }
        added += sz;
        if nbatches % 20 == 0 || added >= add_total {
            println!("  +{added}/{add_total} ({nbatches} batches, last size {sz} in {d:?})");
        }
    }
    println!(
        "  ADD done: {added} nodes in {nbatches} group-committed batches, {:?}",
        t_add.elapsed()
    );
    if !slow.is_empty() {
        println!("  slow batches (>1.5s) — likely flush/compaction stalls:");
        for (b, s, ms) in &slow {
            println!("    batch #{b} size {s}: {ms}ms");
        }
    }

    // Verify the add via the bounded born-range count (index-served, cheap with a delta),
    // then also probe the whole-graph count(*) to time the fast-path loss.
    let bc = born_count(&mut c, &g, born_base);
    println!("  born-range count = {bc} (expected {added})");
    assert_eq!(bc, added as i64, "born-range count reflects the adds");
    count_all(&conn, &g, "after ADD");

    // ── 3. EDIT the same volume: half core nodes, half born nodes. ──
    let new_edit = edit_total - core_edit;
    println!("\n== PHASE 3: EDIT {edit_total} ({core_edit} core + {new_edit} new) ==");
    let t_edit = Instant::now();
    // 3a: edit core nodes by their real business keys.
    let mut done = 0usize;
    si = 0;
    let mut eb = 0usize;
    while done < core_edit {
        let sz = sizes[si % sizes.len()].min(core_edit - done);
        si += 1;
        let rows: Vec<PsValue> = core_ids[done..done + sz]
            .iter()
            .map(|id| PsValue::Map(vec![("id".into(), PsValue::Int(*id))]))
            .collect();
        batch(
            &mut c,
            &g,
            "UNWIND $rows AS r MATCH (n:Entity {wikidata_id: r.id}) SET n.wc_edited = 1",
            rows,
        );
        eb += 1;
        done += sz;
    }
    println!("  edited {core_edit} CORE nodes in {eb} batches");
    // 3b: edit born nodes.
    done = 0;
    si = 0;
    let mut eb2 = 0usize;
    while done < new_edit {
        let sz = sizes[si % sizes.len()].min(new_edit - done);
        si += 1;
        let rows: Vec<PsValue> = (done..done + sz)
            .map(|i| PsValue::Map(vec![("id".into(), PsValue::Int(born_base + i as i64))]))
            .collect();
        batch(
            &mut c,
            &g,
            "UNWIND $rows AS r MATCH (n:Entity {wikidata_id: r.id}) SET n.wc_edited = 1",
            rows,
        );
        eb2 += 1;
        done += sz;
    }
    println!(
        "  edited {new_edit} NEW nodes in {eb2} batches, EDIT total {:?}",
        t_edit.elapsed()
    );

    // Verify edits: sampled core + new carry the marker; witness does NOT.
    let sample_core = core_ids[0];
    let vc = ints(
        &mut c,
        &g,
        &format!("MATCH (n:Entity {{wikidata_id: {sample_core}}}) RETURN n.wc_edited"),
    );
    let vn = ints(
        &mut c,
        &g,
        &format!("MATCH (n:Entity {{wikidata_id: {born_base}}}) RETURN n.wc_edited"),
    );
    let vw = ints(
        &mut c,
        &g,
        &format!("MATCH (n:Entity {{wikidata_id: {witness}}}) RETURN n.wc_edited"),
    );
    println!("  sampled core {sample_core} wc_edited={vc:?}, new {born_base} wc_edited={vn:?}, witness wc_edited={vw:?}");
    assert_eq!(vc, vec![1], "edited core node carries the marker");
    assert_eq!(vn, vec![1], "edited new node carries the marker");
    assert!(vw.is_empty(), "witness untouched (no marker)");

    // ── 4. DELETE 100: half core, half new. ──
    println!(
        "\n== PHASE 4: DELETE {del_total} ({core_del} core + {} new) ==",
        del_total - core_del
    );
    let core_del_ids: Vec<i64> = core_ids[core_edit..core_edit + core_del].to_vec();
    let new_del_ids: Vec<i64> = ((add_total - (del_total - core_del))..add_total)
        .map(|i| born_base + i as i64)
        .collect();
    let del_rows = |ids: &[i64]| -> Vec<PsValue> {
        ids.iter()
            .map(|id| PsValue::Map(vec![("id".into(), PsValue::Int(*id))]))
            .collect()
    };
    batch(
        &mut c,
        &g,
        "UNWIND $rows AS r MATCH (n:Entity {wikidata_id: r.id}) DELETE n",
        del_rows(&core_del_ids),
    );
    batch(
        &mut c,
        &g,
        "UNWIND $rows AS r MATCH (n:Entity {wikidata_id: r.id}) DELETE n",
        del_rows(&new_del_ids),
    );
    println!(
        "  deleted {} core + {} new",
        core_del_ids.len(),
        new_del_ids.len()
    );

    // ── 5. Final verification. ──
    println!("\n== PHASE 5: final verification ==");
    let new_del = del_total - core_del;
    let bc2 = born_count(&mut c, &g, born_base);
    println!("  born-range count = {bc2} (expected {})", added - new_del);
    assert_eq!(
        bc2,
        (added - new_del) as i64,
        "born-range count reflects new deletes"
    );
    count_all(&conn, &g, "final (adds - deletes)");
    let gone_core = strs(
        &mut c,
        &g,
        &format!(
            "MATCH (n:Entity {{wikidata_id: {}}}) RETURN n.name",
            core_del_ids[0]
        ),
    );
    let gone_new = strs(
        &mut c,
        &g,
        &format!(
            "MATCH (n:Entity {{wikidata_id: {}}}) RETURN n.name",
            new_del_ids[0]
        ),
    );
    assert!(gone_core.is_empty(), "deleted core id resolves empty");
    assert!(gone_new.is_empty(), "deleted new id resolves empty");
    let wafter = strs(
        &mut c,
        &g,
        &format!("MATCH (n:Entity {{wikidata_id: {witness}}}) RETURN n.name"),
    );
    assert_eq!(
        wafter, wname,
        "witness name unchanged through the whole cycle"
    );
    println!("  deleted ids resolve empty; witness intact. ALL CHECKS PASSED.");

    // ── 6. Diagnostics snapshot. ──
    println!("\n== PHASE 6: CALL slater.diagnostics() ==");
    let (_c, rows) = c
        .run_pull("CALL slater.diagnostics()", Some(&g))
        .expect("diagnostics");
    for r in &rows {
        if r.len() >= 2 {
            let k = r[0].as_str().unwrap_or("");
            let v = match &r[1] {
                PsValue::Int(i) => i.to_string(),
                PsValue::Float(f) => format!("{f:.3}"),
                PsValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            };
            println!("  {k:32} {v}");
        }
    }
}
