// SPDX-License-Identifier: Apache-2.0
//! The build: a bounded-memory, external-sort offline writer.
//!
//! This is the sole build path. It streams the dump into on-disk buckets (pass 1),
//! computes a locality-aware node-id permutation under a memory cap (pass 2 /
//! clustering), then emits the final stores by external sort — so peak memory is
//! independent of the edge count and a graph larger than RAM still builds. The
//! published generation is the format the server reads unchanged.
//!
//! All scratch lives under a per-generation directory **outside** the staged
//! generation (so the publish rename never drags 20+ GB of buckets into the
//! image), and is removed on success unless `--keep-temp`.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use graph_format::blockfile::{concat_block_files, BlockFileReader, BlockFileWriter};
use graph_format::columns::PropsWriter;
use graph_format::crypto::BlockCipher;
use graph_format::extsort::{ExtSorter, SortRecord};
use graph_format::ids::{EdgeId, Generation, NodeId, Value};
use graph_format::isam::write_isam_sorted;
use graph_format::manifest::{EntityKind, RangeIndexDesc};
use graph_format::membudget::{MemoryBudget, Reservation, MIN_SORT_BYTES};
use graph_format::nodelabels::{NodeLabelsReader, NodeLabelsWriter};
use graph_format::postings::{
    write_endpoint_postings_from_planes, write_endpoint_postings_from_sorted, EndpointPlanes,
};
use graph_format::topology::{Adj, CsrHalfWriter, TopologyReader};
use graph_format::wire::{read_uvarint, read_value, skip_value, write_uvarint, write_value};

use crate::buckets::{
    self, read_blob, write_blob, Blob, BucketWriter, EdgeRec, NodeRec, UnresolvedEdge,
};
use crate::cluster::{self, ClusterParams, Permutation};
use crate::common::{self, BuildOutcome, PublishInputs};
use crate::merge_build;
use crate::model::{Entity, RangeIndexStmt, Statement, VectorIndexStmt};
use crate::parser::{parse_statement, parse_statement_with_id_field, StatementReader};
use crate::resolve::{DumpResolver, NO_DUMP};
use crate::shared::{parse_metric, write_vector_indexes, BuildOptions, Interner, PendingIndex};

const DUMP_VERTEX: &str = "__DumpVertex__";
const DUMP_ID: &str = "__dump_id__";
/// Bigger blocks for the transient buckets — fewer, fatter blocks, all deleted at
/// the end of the build.
const BUCKET_BLOCK: usize = 1 << 20;
/// zstd level for transient scratch (buckets, spill runs, cluster adjacency). These
/// are deleted at the end of the build, so favour speed (level 1) over ratio — the
/// final published stores still use `--zstd-level`.
const SCRATCH_ZSTD: i32 = 1;
/// Default node-id band width for the range-partitioned parallel `emit.topology`.
/// Fixed (not derived from `--threads`) so the band boundaries — and therefore the
/// emitted block layout and content hash — are independent of the worker count,
/// exactly like the cluster phase's `STRIPE_NODES`.
const BAND_NODES_DEFAULT: u64 = 1 << 20;

/// Resolved band width. The `SLATER_EMIT_BAND_NODES` override exists only so tests
/// can force many small bands over a tiny fixture (production leaves it unset and
/// gets the fixed default). Cached so every call site agrees within a run.
fn band_nodes() -> u64 {
    use std::sync::OnceLock;
    static BAND: OnceLock<u64> = OnceLock::new();
    *BAND.get_or_init(|| {
        std::env::var("SLATER_EMIT_BAND_NODES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(BAND_NODES_DEFAULT)
    })
}

/// Force the external-sort endpoint-postings path even when the bit planes would
/// fit. No graph we build is large *and* richly typed enough to spill naturally
/// (Wikidata needs 22.9 MB of planes, Monarch-KG 23.0 MB), so without this the
/// fallback would never be exercised. Tests set it and assert the build still
/// publishes the same content hash. Mirrors `SLATER_EMIT_BAND_NODES`.
fn force_sorter_postings() -> bool {
    std::env::var_os("SLATER_POSTINGS_FORCE_SORTER").is_some_and(|v| !v.is_empty() && v != "0")
}

/// Checkpoint file (in scratch) recording how far a build got, for `--resume`.
const STATE_FILE: &str = "BUILD-STATE.json";

/// The furthest phase a build has durably completed. Ordered so `>=` works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum Phase {
    /// Nothing durable yet.
    Start,
    /// Node + unresolved-edge buckets written; interners/counts captured.
    Pass1,
    /// (`merge` dumps only) deduped node bucket + node-key stream written.
    Deduped,
    /// Provisional-id edge bucket written.
    Resolved,
    /// Node-id permutation computed (perm.bin written, or identity).
    Clustered,
}

/// Durable cross-phase state, persisted to `BUILD-STATE.json` after each phase so
/// an interrupted build can resume the expensive later phases instead of redoing
/// them. Determinism (stable gen UUID, total-order sorts) makes the regenerated
/// artifacts identical to what the crashed run would have produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BuildState {
    generation: String,
    phase: Phase,
    node_count: u64,
    /// Valid once `phase >= Resolved`.
    edge_count: u64,
    labels: Vec<String>,
    reltypes: Vec<String>,
    property_keys: Vec<String>,
    range_stmts: Vec<RangeIndexStmt>,
    vector_stmts: Vec<VectorIndexStmt>,
    /// Valid once `phase >= Clustered`: whether the permutation is the identity
    /// (so no `perm.bin` was written).
    cluster_identity: bool,
}
/// Atomically write the checkpoint (temp file + rename).
fn checkpoint(scratch_dir: &Path, state: &BuildState) -> Result<()> {
    let path = scratch_dir.join(STATE_FILE);
    let tmp = scratch_dir.join(".BUILD-STATE.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("commit {}", path.display()))?;
    Ok(())
}

/// Test-only fault injection: `SLATER_BUILD_FAIL_AFTER=<phase>` exits hard right
/// after that phase checkpoints (skipping all cleanup), leaving scratch intact so
/// the resume path can be exercised. Never triggers unless the env var is set.
fn fault_after(phase: &str) {
    if std::env::var("SLATER_BUILD_FAIL_AFTER").as_deref() == Ok(phase) {
        eprintln!("SLATER_BUILD_FAIL_AFTER={phase}: simulating a crash after {phase}");
        std::process::exit(70);
    }
}

/// Find a resumable build: the first `.slater-scratch-*` under `scratch_base`
/// holding a parseable `BUILD-STATE.json`.
fn find_resume_state(
    scratch_base: &Path,
) -> Result<Option<(Generation, std::path::PathBuf, BuildState)>> {
    let Ok(rd) = std::fs::read_dir(scratch_base) else {
        return Ok(None);
    };
    for entry in rd.flatten() {
        let p = entry.path();
        let is_scratch = p
            .file_name()
            .map(|n| n.to_string_lossy().starts_with(".slater-scratch-"))
            .unwrap_or(false);
        if p.is_dir() && is_scratch {
            let sp = p.join(STATE_FILE);
            if sp.exists() {
                let state: BuildState = serde_json::from_str(&std::fs::read_to_string(&sp)?)
                    .with_context(|| format!("parse {}", sp.display()))?;
                let gen = Generation(
                    uuid::Uuid::parse_str(&state.generation).context("parse generation uuid")?,
                );
                return Ok(Some((gen, p, state)));
            }
        }
    }
    Ok(None)
}

/// Persist a non-identity permutation table to `perm.bin` (identity writes nothing).
fn save_perm(perm: &Permutation, path: &Path) -> Result<()> {
    if let Some(table) = perm.table() {
        let mut buf = Vec::with_capacity(table.len() * 4);
        for &x in table {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        std::fs::write(path, &buf).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

/// Open the dump input seeked to `offset`. `-` is stdin (which cannot seek, so a
/// non-zero offset — i.e. a mid-pass-1 resume — is refused with a clear error).
fn open_input(input_path: &str, offset: u64) -> Result<Box<dyn BufRead>> {
    if input_path == "-" {
        if offset != 0 {
            bail!(
                "--resume cannot continue pass 1 from a stdin pipe (offset {offset}); \
                 re-run the build against the dump as a file"
            );
        }
        Ok(Box::new(BufReader::new(std::io::stdin())))
    } else {
        let mut f =
            std::fs::File::open(input_path).with_context(|| format!("open input {input_path}"))?;
        if offset != 0 {
            f.seek(SeekFrom::Start(offset))
                .with_context(|| format!("seek input to {offset}"))?;
        }
        Ok(Box::new(BufReader::new(f)))
    }
}
/// Quote-aware scan mirroring [`StatementReader`]'s tokenizer: byte offset just
/// past the **last top-level `;`** in `buf` (the end of the last *complete*
/// statement), or 0 if `buf` holds no complete statement. Because the cut lands
/// right after a `;`, the carry always begins **outside any string literal**, so a
/// fresh scan of `carry + next_block` is correct — this is what makes the
/// block-streaming reader stdin-safe without seeking.
fn last_statement_end(buf: &[u8]) -> usize {
    let mut in_string: Option<u8> = None;
    let mut escaped = false;
    let mut last_end = 0usize;
    for (i, &b) in buf.iter().enumerate() {
        if let Some(q) = in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == q {
                in_string = None;
            }
        } else {
            match b {
                b'\'' | b'"' => in_string = Some(b),
                b';' => last_end = i + 1,
                _ => {}
            }
        }
    }
    last_end
}
// ── shard-parallel pass 1 ────────────────────────────────────────────────────

/// One bordered input range handed to a pass-1 worker.
struct ShardChunk {
    shard: u64,
    input_start: u64,
    input_end: u64,
    bytes: Vec<u8>,
}

/// Target bytes per shard (resume + segment granularity). `SLATER_SHARD_BYTES`
/// overrides; default 64 MiB (≈ 2800 shards for the 180 GB wiki dump).
fn shard_bytes_target() -> usize {
    std::env::var("SLATER_SHARD_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .map(|n| n.max(1)) // tiny values allowed for tests (forces many shards)
        .unwrap_or(64 << 20)
}

/// Parse one shard chunk into shard-indexed node + unresolved-edge segments using a
/// **local** interner (workers share nothing), then finalize it (sidecar). The
/// sidecar's presence marks the shard durably complete — the resume signal.
#[allow(clippy::too_many_arguments)]
fn process_shard(
    chunk: ShardChunk,
    node_bkt: &Path,
    uedge_bkt: &Path,
    merge_mode: bool,
    node_merge_bkt: &Path,
    edge_merge_bkt: &Path,
    pk_field: &str,
    vec_index_set: &std::collections::HashSet<(String, String)>,
) -> Result<()> {
    if merge_mode {
        return process_shard_merge(chunk, node_bkt, node_merge_bkt, edge_merge_bkt);
    }
    let shard_index = chunk.shard;
    let mut labels = Interner::default();
    let mut reltypes = Interner::default();
    let mut keys = Interner::default();
    let mut rstmts: Vec<RangeIndexStmt> = Vec::new();
    let mut vstmts: Vec<VectorIndexStmt> = Vec::new();
    let mut node_ovr: Vec<crate::model::NodeOverwriteStmt> = Vec::new();
    let mut edge_ovr: Vec<crate::model::EdgeOverwriteStmt> = Vec::new();
    // Inline seal: pass 1 already runs one of these per shard across every core, so the
    // block-seal pool can lend it nothing and would only add a hop per block.
    let mut node_w = BucketWriter::create_inline(
        buckets::seg_path(node_bkt, chunk.shard),
        BUCKET_BLOCK,
        SCRATCH_ZSTD,
    )?;
    let mut uedge_w = BucketWriter::create_inline(
        buckets::seg_path(uedge_bkt, chunk.shard),
        BUCKET_BLOCK,
        SCRATCH_ZSTD,
    )?;
    let mut node_count = 0u64;
    let mut uedge_count = 0u64;
    let mut scalar_props: Vec<(u32, Value)> = Vec::new();
    let mut sr = StatementReader::new(std::io::Cursor::new(&chunk.bytes));
    while let Some(raw) = sr.next_statement()? {
        match parse_statement_with_id_field(&raw, pk_field)
            .with_context(|| format!("in statement: {}", truncate(&raw, 120)))?
        {
            Statement::Node(n) => {
                let mut label_names: Vec<&str> = Vec::new();
                let mut label_ids = Vec::new();
                for l in &n.labels {
                    if l != DUMP_VERTEX {
                        label_names.push(l);
                        label_ids.push(labels.intern(l));
                    }
                }
                scalar_props.clear();
                let mut vec_props: Vec<(String, Vec<f32>)> = Vec::new();
                let mut dump_id = NO_DUMP;
                for (k, v) in n.props {
                    if k == pk_field {
                        // The pk field is the node identity AND a stored, queryable
                        // property (unlike the legacy consumed `__dump_id__`).
                        let Value::Int(id) = v else {
                            bail!("pk field '{pk_field}' must be an integer");
                        };
                        dump_id = id;
                        let kid = keys.intern(&k);
                        scalar_props.push((kid, Value::Int(id)));
                        continue;
                    }
                    match v {
                        Value::Vector(xs)
                            if label_names
                                .iter()
                                .any(|l| vec_index_set.contains(&(l.to_string(), k.clone()))) =>
                        {
                            vec_props.push((k, xs));
                        }
                        other => {
                            let kid = keys.intern(&k);
                            scalar_props.push((kid, other));
                        }
                    }
                }
                let labels_blob = buckets::labels_blob(&label_ids);
                let props_blob = buckets::props_blob(&scalar_props);
                node_w.append_node(&NodeRec {
                    dump_id: if dump_id == NO_DUMP {
                        None
                    } else {
                        Some(dump_id)
                    },
                    labels_blob,
                    props_blob,
                    vec_props,
                })?;
                node_count += 1;
            }
            Statement::Edge(e) => {
                let reltype = reltypes.intern(&e.reltype);
                scalar_props.clear();
                for (k, v) in e.props {
                    let kid = keys.intern(&k);
                    scalar_props.push((kid, v));
                }
                let props_blob = buckets::props_blob(&scalar_props);
                uedge_w.append_unresolved_edge(&UnresolvedEdge {
                    src_dump: e.src_dump_id,
                    dst_dump: e.dst_dump_id,
                    reltype,
                    props_blob,
                })?;
                uedge_count += 1;
            }
            Statement::RangeIndex(r) => {
                if r.label_or_type != DUMP_VERTEX && r.property != pk_field {
                    rstmts.push(r);
                }
            }
            Statement::VectorIndex(v) => {
                if !vstmts
                    .iter()
                    .any(|e: &VectorIndexStmt| e.label == v.label && e.property == v.property)
                {
                    vstmts.push(v);
                }
            }
            // Overlay overwrites: stash verbatim (in statement order) for the global
            // pass-1.9 — matching is by label+property against ALL nodes, so it can't
            // be resolved shard-locally.
            Statement::NodeOverwrite(o) => node_ovr.push(o),
            Statement::EdgeOverwrite(o) => edge_ovr.push(o),
            Statement::Ignored => {}
        }
    }
    node_w.finish()?;
    uedge_w.finish()?;
    let meta = buckets::ShardMeta {
        shard: chunk.shard,
        input_start: chunk.input_start,
        input_end: chunk.input_end,
        node_count,
        uedge_count,
        labels: labels.into_names(),
        reltypes: reltypes.into_names(),
        keys: keys.into_names(),
        range_stmts: rstmts,
        vector_stmts: vstmts,
        node_overwrites: node_ovr,
        edge_overwrites: edge_ovr,
    };
    buckets::finalize_shard(
        node_bkt,
        &[
            buckets::seg_path(node_bkt, chunk.shard),
            buckets::seg_path(uedge_bkt, chunk.shard),
        ],
        &meta,
    )?;
    // Test hook: crash right after a given shard is durably finalized, to exercise
    // shard-granular pass-1 resume (use `--threads 1` for a deterministic order).
    if std::env::var("SLATER_BUILD_FAIL_AFTER_SHARD")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        == Some(shard_index)
    {
        eprintln!("SLATER_BUILD_FAIL_AFTER_SHARD={shard_index}: simulating a mid-pass-1 crash");
        std::process::exit(70);
    }
    Ok(())
}

/// Pass-1 for a merge-style shard (the default, no `--pk`): parse business-key
/// node/edge MERGEs into the per-shard node-merge / edge-merge buckets (LOCAL symbol
/// ids), then finalize the sidecar. Node dedup and endpoint resolution happen globally
/// in later phases. CREATE (`__dump_id__`) statements and MATCH overwrites are rejected
/// — merge dumps are MERGE-only (use `--pk <field>` for dump_id-style CREATE imports).
fn process_shard_merge(
    chunk: ShardChunk,
    node_bkt: &Path,
    node_merge_bkt: &Path,
    edge_merge_bkt: &Path,
) -> Result<()> {
    let shard_index = chunk.shard;
    let mut labels = Interner::default();
    let mut reltypes = Interner::default();
    let mut keys = Interner::default();
    let mut rstmts: Vec<RangeIndexStmt> = Vec::new();
    let mut vstmts: Vec<VectorIndexStmt> = Vec::new();
    let mut mw = merge_build::MergeShardWriters::create(
        &buckets::seg_path(node_merge_bkt, chunk.shard),
        &buckets::seg_path(edge_merge_bkt, chunk.shard),
        SCRATCH_ZSTD,
    )?;
    let mut node_count = 0u64;
    let mut edge_count = 0u64;
    let mut sr = StatementReader::new(std::io::Cursor::new(&chunk.bytes));
    while let Some(raw) = sr.next_statement()? {
        match parse_statement(&raw)
            .with_context(|| format!("in statement: {}", truncate(&raw, 120)))?
        {
            Statement::NodeOverwrite(o) => {
                if !o.is_merge {
                    bail!(
                        "merge dumps expect MERGE node statements, not MATCH … SET: {}",
                        truncate(&raw, 120)
                    );
                }
                let rec = merge_build::build_node_merge_rec(&o, &mut labels, &mut keys)?;
                mw.append_node(&rec)?;
                node_count += 1;
            }
            Statement::EdgeOverwrite(o) => {
                if !o.is_merge {
                    bail!(
                        "merge dumps expect MERGE edge statements, not MATCH … SET: {}",
                        truncate(&raw, 120)
                    );
                }
                let rec =
                    merge_build::build_edge_merge_rec(&o, &mut labels, &mut reltypes, &mut keys)?;
                mw.append_edge(&rec)?;
                edge_count += 1;
            }
            Statement::RangeIndex(r) => {
                if r.label_or_type != DUMP_VERTEX && r.property != DUMP_ID {
                    rstmts.push(r);
                }
            }
            Statement::VectorIndex(v) => {
                if !vstmts
                    .iter()
                    .any(|e: &VectorIndexStmt| e.label == v.label && e.property == v.property)
                {
                    vstmts.push(v);
                }
            }
            Statement::Node(_) | Statement::Edge(_) => bail!(
                "merge dump does not accept __dump_id__ CREATE statements; emit business-key \
                 MERGE statements, or pass --pk <field> for a dump_id-style import: {}",
                truncate(&raw, 120)
            ),
            Statement::Ignored => {}
        }
    }
    mw.finish()?;
    let meta = buckets::ShardMeta {
        shard: chunk.shard,
        input_start: chunk.input_start,
        input_end: chunk.input_end,
        node_count,
        uedge_count: edge_count,
        labels: labels.into_names(),
        reltypes: reltypes.into_names(),
        keys: keys.into_names(),
        range_stmts: rstmts,
        vector_stmts: vstmts,
        node_overwrites: Vec::new(),
        edge_overwrites: Vec::new(),
    };
    buckets::finalize_shard(
        node_bkt,
        &[
            buckets::seg_path(node_merge_bkt, chunk.shard),
            buckets::seg_path(edge_merge_bkt, chunk.shard),
        ],
        &meta,
    )?;
    if std::env::var("SLATER_BUILD_FAIL_AFTER_SHARD")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        == Some(shard_index)
    {
        eprintln!("SLATER_BUILD_FAIL_AFTER_SHARD={shard_index}: simulating a mid-pass-1 crash");
        std::process::exit(70);
    }
    Ok(())
}

/// Read the input sequentially, cutting it into ~`target`-byte shards at statement
/// boundaries (string-aware via [`last_statement_end`]), and dispatch each
/// not-yet-complete shard to a worker. On resume, shards whose sidecar already
/// exists are re-read but not re-dispatched (their work is durable). Works for both
/// a file and stdin (stdin: fresh only — no sidecars exist).
fn shard_reader(
    mut r: Box<dyn BufRead>,
    target: usize,
    node_bkt: &Path,
    tx: &std::sync::mpsc::SyncSender<ShardChunk>,
    err: &std::sync::Mutex<Option<anyhow::Error>>,
) -> Result<()> {
    let mut shard = 0u64;
    let mut consumed = 0u64; // input offset of bytes already cut into prior shards
    let mut carry: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; target];
    loop {
        if err.lock().unwrap().is_some() {
            return Ok(());
        }
        // Accumulate ≥ target bytes ending at a statement boundary (or hit EOF).
        let mut eof = false;
        let mut cut = last_statement_end(&carry);
        while cut == 0 || carry.len() < target {
            let n = std::io::Read::read(&mut r, &mut buf).context("read dump")?;
            if n == 0 {
                eof = true;
                break;
            }
            carry.extend_from_slice(&buf[..n]);
            cut = last_statement_end(&carry);
        }
        if carry.is_empty() {
            break;
        }
        let (bytes, end) = if eof {
            // Final shard: take everything (the last statement may lack a `;`).
            let all = std::mem::take(&mut carry);
            let end = consumed + all.len() as u64;
            (all, end)
        } else {
            let rest = carry.split_off(cut);
            let chunk = std::mem::replace(&mut carry, rest);
            let end = consumed + chunk.len() as u64;
            (chunk, end)
        };
        let start = consumed;
        consumed = end;
        // Skip shards already finalized by an earlier (interrupted) run.
        let complete = buckets::read_shard_meta(node_bkt, shard)?.is_some();
        if !complete
            && tx
                .send(ShardChunk {
                    shard,
                    input_start: start,
                    input_end: end,
                    bytes,
                })
                .is_err()
        {
            break; // workers gone (an error elsewhere)
        }
        shard += 1;
        if eof {
            break;
        }
    }
    Ok(())
}

/// Shard-parallel pass 1: fan bordered input shards across `threads` independent
/// workers, each writing a self-contained segment + sidecar. Returns every shard's
/// metadata (complete-from-a-prior-run + freshly written), read back from disk in
/// shard order, for the deterministic symbol merge.
#[allow(clippy::too_many_arguments)]
fn run_pass1_sharded(
    input_path: &str,
    node_bkt: &Path,
    uedge_bkt: &Path,
    merge_mode: bool,
    node_merge_bkt: &Path,
    edge_merge_bkt: &Path,
    pk_field: &str,
    vec_index_set: std::collections::HashSet<(String, String)>,
    threads: usize,
    diag: &crate::diag::BuildDiag,
) -> Result<Vec<buckets::ShardMeta>> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::sync_channel;
    use std::sync::{Arc, Mutex};

    let nworkers = threads.max(1);
    let vec_index_set = Arc::new(vec_index_set);
    let target = shard_bytes_target();
    let seekable = input_path != "-";
    let file_len = if seekable {
        std::fs::metadata(input_path).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };
    diag.set_op("shard-parallel parse → buckets", "bytes", file_len);
    diag.set_active_workers(nworkers as u64);

    let err: Arc<Mutex<Option<anyhow::Error>>> = Arc::new(Mutex::new(None));
    let done = Arc::new(AtomicU64::new(0));
    let (tx, rx) = sync_channel::<ShardChunk>(nworkers);
    let rx = Arc::new(Mutex::new(rx));

    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..nworkers {
            let rx = Arc::clone(&rx);
            let err = Arc::clone(&err);
            let done = Arc::clone(&done);
            let vis = Arc::clone(&vec_index_set);
            handles.push(scope.spawn(move || loop {
                if err.lock().unwrap().is_some() {
                    break;
                }
                let msg = {
                    let g = rx.lock().unwrap();
                    g.recv()
                };
                let chunk = match msg {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let len = chunk.bytes.len() as u64;
                match process_shard(
                    chunk,
                    node_bkt,
                    uedge_bkt,
                    merge_mode,
                    node_merge_bkt,
                    edge_merge_bkt,
                    pk_field,
                    &vis,
                ) {
                    Ok(()) => {
                        diag.progress_add(len);
                        let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                        if diag.enabled() {
                            diag.set_op_detail(&format!("shards done {d}"));
                        }
                    }
                    Err(e) => {
                        let mut s = err.lock().unwrap();
                        if s.is_none() {
                            *s = Some(e);
                        }
                        break;
                    }
                }
            }));
        }
        let reader_input = open_input(input_path, 0)?;
        let reader_res = shard_reader(reader_input, target, node_bkt, &tx, &err);
        drop(tx);
        for h in handles {
            let _ = h.join();
        }
        reader_res
    })?;

    if let Some(e) = Arc::try_unwrap(err)
        .ok()
        .and_then(|m| m.into_inner().ok())
        .flatten()
    {
        return Err(e);
    }

    // Authoritative shard list: every shard 0..K now has a sidecar on disk.
    let mut metas = Vec::new();
    let mut n = 0u64;
    while let Some(m) = buckets::read_shard_meta(node_bkt, n)? {
        metas.push(m);
        n += 1;
    }
    Ok(metas)
}

/// Load a permutation table written by [`save_perm`].
fn load_perm(path: &Path, node_count: u64) -> Result<Permutation> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() != node_count as usize * 4 {
        bail!(
            "perm.bin has {} bytes, expected {} (node_count {node_count}) — resume state corrupt",
            bytes.len(),
            node_count as usize * 4
        );
    }
    let table = bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(Permutation::Table(table))
}

/// Build a generation with bounded memory. `input_path` is the dump script path,
/// or `-` for stdin (stdin cannot be sought, so mid-pass-1 resume needs a file).
/// See module docs.
pub fn build_external(
    input_path: &str,
    graph: &str,
    data_dir: &Path,
    opts: &BuildOptions,
    diag: &crate::diag::BuildDiag,
) -> Result<BuildOutcome> {
    let graph_dir = data_dir.join(graph);
    std::fs::create_dir_all(&graph_dir)
        .with_context(|| format!("create {}", graph_dir.display()))?;
    // Scratch (buckets + spill) lives OUTSIDE tmp_dir so publish never captures it.
    let scratch_base = opts.temp_dir.clone().unwrap_or_else(|| graph_dir.clone());

    let (generation, tmp_dir, scratch_dir, resume_state) = if opts.resume {
        match find_resume_state(&scratch_base)? {
            Some((gen, sdir, state)) => {
                eprintln!("resuming build {} from phase {:?}", gen.0, state.phase);
                (
                    gen,
                    graph_dir.join(format!(".tmp-{}", gen.0)),
                    sdir,
                    Some(state),
                )
            }
            None => bail!(
                "--resume: no resumable build found under {}",
                scratch_base.display()
            ),
        }
    } else {
        let gen = Generation(uuid::Uuid::new_v4());
        let sdir = scratch_base.join(format!(".slater-scratch-{}", gen.0));
        if sdir.exists() {
            std::fs::remove_dir_all(&sdir).ok();
        }
        std::fs::create_dir_all(&sdir)
            .with_context(|| format!("create scratch {}", sdir.display()))?;
        (gen, graph_dir.join(format!(".tmp-{}", gen.0)), sdir, None)
    };
    let final_dir = graph_dir.join(generation.0.to_string());

    // Emit is redone wholesale on resume, so the staging dir always starts clean.
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir).ok();
    }
    std::fs::create_dir_all(tmp_dir.join("range"))
        .with_context(|| format!("create {}", tmp_dir.display()))?;
    std::fs::create_dir_all(tmp_dir.join("vector"))?;

    // stdin can only be read once, but the build reads the input twice (a header
    // pre-scan for the vector-index routing, then pass 1) and may re-read it on
    // resume. A pipe can't be rewound, so spool it to a scratch file once and build
    // from that — making stdin behave exactly like a file input. On resume the spool
    // already exists (scratch persists), so we reuse it rather than draining the
    // now-empty pipe.
    let spooled_input;
    let input_path: &str = if input_path == "-" {
        let spool = scratch_dir.join("stdin-input.cypher");
        if !spool.exists() {
            let mut w = std::io::BufWriter::new(
                std::fs::File::create(&spool)
                    .with_context(|| format!("create stdin spool {}", spool.display()))?,
            );
            std::io::copy(&mut std::io::stdin().lock(), &mut w)
                .context("spool stdin to scratch")?;
            w.into_inner()
                .context("flush stdin spool")?
                .sync_all()
                .context("fsync stdin spool")?;
        }
        spooled_input = spool.to_string_lossy().into_owned();
        &spooled_input
    } else {
        input_path
    };

    let result = build_inner(
        input_path,
        graph,
        opts,
        generation,
        &graph_dir,
        &tmp_dir,
        &final_dir,
        &scratch_dir,
        resume_state,
        diag,
    );
    match &result {
        // Success: drop the scratch (unless asked to keep it).
        Ok(_) if !opts.keep_temp => {
            std::fs::remove_dir_all(&scratch_dir).ok();
        }
        // Failure: leave scratch + checkpoint intact so `--resume` can continue;
        // only the half-written staging dir is cleared.
        Err(_) => {
            std::fs::remove_dir_all(&tmp_dir).ok();
        }
        _ => {}
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn build_inner(
    input_path: &str,
    graph: &str,
    opts: &BuildOptions,
    generation: Generation,
    graph_dir: &Path,
    tmp_dir: &Path,
    final_dir: &Path,
    scratch_dir: &Path,
    resume: Option<BuildState>,
    diag: &crate::diag::BuildDiag,
) -> Result<BuildOutcome> {
    let (cipher, encryption_header) = common::derive_cipher(&opts.encryption_key);
    // The single arbiter of `--max-memory`. Every sorter below reserves from it and
    // hands the bytes back when it drops, so the live reservations can never sum
    // past the cap — which is what the old per-consumer `/16` fractions did. The
    // diagnostics sampler reports the live total as `budget_reserved_bytes`, so a
    // run shows reserved-against-resident and any divergence is a visible bug.
    let mem = MemoryBudget::new(opts.max_memory_bytes);
    // No `--pk` ⇒ merge style (business-key MERGE engine). `--pk FIELD` ⇒ the legacy
    // dense-id pipeline keyed on FIELD (the configurable, stored `__dump_id__`).
    let merge_mode = opts.pk.is_none();
    let pk_field: &str = opts.pk.as_deref().unwrap_or(DUMP_ID);
    let node_bkt = scratch_dir.join("nodes.bkt");
    let uedge_bkt = scratch_dir.join("edges_unresolved.bkt");
    let edge_bkt = scratch_dir.join("edges.bkt");
    // `merge` dumps spill business-key node/edge MERGEs into these per-shard buckets in
    // pass 1; the deduped node bucket (`node_bkt`) and resolved `edge_bkt` are produced
    // by the dedup / resolve phases. The node-key stream feeds the edge merge-join.
    let node_merge_bkt = scratch_dir.join("nodes_merge.bkt");
    let edge_merge_bkt = scratch_dir.join("edges_merge.bkt");
    let node_keys_bkt = scratch_dir.join("node_keys.bkt");
    let perm_path = scratch_dir.join("perm.bin");
    let resume_phase = resume.as_ref().map(|s| s.phase).unwrap_or(Phase::Start);

    let range_stmts: Vec<RangeIndexStmt>;
    let vector_stmts: Vec<VectorIndexStmt>;

    // The post-resolve inputs the clustering + emit phases consume. Produced either by
    // the Cypher front-half (pass 1 → dedup → resolve, below) or, for a
    // `--input-format=slater-dump` build, directly from the binary dump — both leave
    // `node_bkt`/`edge_bkt` in the same shape and these values set.
    let mut node_count: u64;
    let edge_count: u64;
    let mut labels: Interner;
    let reltypes: Interner;
    let mut keys: Interner;
    // `remaps`/`overlay`/`base_node_count` are Cypher-front-half machinery; a dump has
    // no per-shard symbol remap (its ids are already global), no overlay patch pass,
    // and no MERGE-created overlay nodes, so they are empty/None/`node_count` there.
    let remaps: Vec<buckets::ShardRemap>;
    let overlay: Option<crate::overlay::Overlay>;
    let base_node_count: u64;

    // ── direct binary-dump ingest (skips parse / dedup / resolve) ──────────────
    if matches!(opts.input_format, crate::shared::InputFormat::SlaterDump) {
        let _dump_g = diag.phase("ingest");
        if resume_phase < Phase::Resolved {
            let ing = crate::direct_ingest::ingest_dump(
                Path::new(input_path),
                &node_bkt,
                &edge_bkt,
                BUCKET_BLOCK,
                SCRATCH_ZSTD,
                diag,
            )?;
            node_count = ing.node_count;
            edge_count = ing.edge_count;
            labels = Interner::from_names(ing.labels);
            reltypes = Interner::from_names(ing.reltypes);
            keys = Interner::from_names(ing.keys);
            range_stmts = ing.range_stmts;
            vector_stmts = Vec::new();
            checkpoint(
                scratch_dir,
                &BuildState {
                    generation: generation.0.to_string(),
                    phase: Phase::Resolved,
                    node_count,
                    edge_count,
                    labels: labels.names().to_vec(),
                    reltypes: reltypes.names().to_vec(),
                    property_keys: keys.names().to_vec(),
                    range_stmts: range_stmts.clone(),
                    vector_stmts: vector_stmts.clone(),
                    cluster_identity: false,
                },
            )?;
            fault_after("resolve");
        } else {
            // Resume past a completed ingest: the buckets survive in scratch; recover
            // counts/symbols/DDL from the checkpoint.
            let s = resume.as_ref().unwrap();
            node_count = s.node_count;
            edge_count = s.edge_count;
            labels = Interner::from_names(s.labels.clone());
            reltypes = Interner::from_names(s.reltypes.clone());
            keys = Interner::from_names(s.property_keys.clone());
            range_stmts = s.range_stmts.clone();
            vector_stmts = s.vector_stmts.clone();
        }
        remaps = Vec::new();
        overlay = None;
        base_node_count = node_count;
    } else {
        // Per-shard local→global symbol remaps, used by resolve (reltype/key ids) and
        // emit (label/key ids) to translate the buckets' local ids to global.

        // ---- pass 1: shard-parallel parse into node + unresolved-edge buckets -------
        let pass1_g = diag.phase("pass1");
        if resume_phase < Phase::Pass1 {
            // Vector-index routing set: which `(label, property)` vecf32 values go to the
            // vector store. The parallel workers see shards out of order, so they need
            // this up front. The dump format puts ALL index DDL before any node data, so
            // a cheap pre-scan of the header (stops at the first node) plus the optional
            // sidecar gives the complete set.
            let mut vec_index_set: std::collections::HashSet<(String, String)> =
                std::collections::HashSet::new();
            if let Some(path) = &opts.vector_index_json {
                for v in crate::shared::load_vector_sidecar(path)? {
                    vec_index_set.insert((v.label.clone(), v.property.clone()));
                }
            }
            {
                let mut sr = StatementReader::new(open_input(input_path, 0)?);
                while let Some(raw) = sr.next_statement()? {
                    match parse_statement(&raw)
                        .with_context(|| format!("in statement: {}", truncate(&raw, 120)))?
                    {
                        Statement::VectorIndex(v) => {
                            vec_index_set.insert((v.label, v.property));
                        }
                        // DDL header ends at the first data statement — a CREATE node in a
                        // `dump-id` dump, or a business-key MERGE in a `merge` dump.
                        Statement::Node(_)
                        | Statement::NodeOverwrite(_)
                        | Statement::EdgeOverwrite(_) => break,
                        _ => {}
                    }
                }
            }
            // Mark the scratch resumable *before* the long parallel pass: a mid-pass-1
            // crash then leaves this `Phase::Start` state + the finalized shard sidecars,
            // and `--resume` re-enters pass 1, skipping shards whose sidecar exists.
            checkpoint(
                scratch_dir,
                &BuildState {
                    generation: generation.0.to_string(),
                    phase: Phase::Start,
                    node_count: 0,
                    edge_count: 0,
                    labels: Vec::new(),
                    reltypes: Vec::new(),
                    property_keys: Vec::new(),
                    range_stmts: Vec::new(),
                    vector_stmts: Vec::new(),
                    cluster_identity: false,
                },
            )?;
            run_pass1_sharded(
                input_path,
                &node_bkt,
                &uedge_bkt,
                merge_mode,
                &node_merge_bkt,
                &edge_merge_bkt,
                pk_field,
                vec_index_set,
                opts.threads,
                diag,
            )?;
        }

        // Merge the shards' local symbol tables (in shard = input order) into the global
        // tables + per-shard remaps. Done fresh or on resume (the node sidecars persist
        // until publish), so the global ids reproduce the historical first-seen order.
        diag.set_op("merge shard symbol tables", "shards", 0);
        let metas = {
            let mut v = Vec::new();
            let mut n = 0u64;
            while let Some(m) = buckets::read_shard_meta(&node_bkt, n)? {
                v.push(m);
                n += 1;
            }
            if v.is_empty() {
                bail!("pass 1 produced no shards");
            }
            v
        };
        let (g_labels, g_reltypes, g_keys, rmaps) = buckets::merge_shard_symbols(&metas);
        remaps = rmaps;
        labels = Interner::from_names(g_labels);
        reltypes = Interner::from_names(g_reltypes);
        keys = Interner::from_names(g_keys);
        node_count = metas.iter().map(|m| m.node_count).sum();
        // Provisional ids `[0, base_node_count)` are the parsed (CREATEd) nodes; any
        // MERGE-created overlay nodes follow at `base_node_count + i`.
        base_node_count = node_count;
        // Union the index DDL across shards (it lives in shard 0; dedup defensively).
        {
            let mut rs: Vec<RangeIndexStmt> = Vec::new();
            let mut vs: Vec<VectorIndexStmt> = Vec::new();
            for m in &metas {
                for r in &m.range_stmts {
                    if !rs.contains(r) {
                        rs.push(r.clone());
                    }
                }
                for v in &m.vector_stmts {
                    if !vs
                        .iter()
                        .any(|e| e.label == v.label && e.property == v.property)
                    {
                        vs.push(v.clone());
                    }
                }
            }
            range_stmts = rs;
            vector_stmts = vs;
        }

        // ---- pass 1.9: resolve overlay overwrites (MERGE|MATCH … SET …) -----------
        // Re-derived every run (incl. resume) from the persisted shard sidecars + node
        // buckets — deterministic, so no separate checkpoint is needed. Extends the
        // global label/key tables with SET targets and MERGE-created labels, and grows
        // `node_count` by the MERGE-created nodes so clustering covers them. `None` ⇒ a
        // plain CREATE-only dump, which pays nothing here.
        overlay = if merge_mode {
            None
        } else {
            crate::overlay::Overlay::build(
                &node_bkt,
                &remaps,
                &metas,
                &mut labels,
                &mut keys,
                &reltypes,
            )?
        };
        if let Some(ov) = &overlay {
            node_count += ov.created.len() as u64;
        }

        if resume_phase < Phase::Pass1 {
            checkpoint(
                scratch_dir,
                &BuildState {
                    generation: generation.0.to_string(),
                    phase: Phase::Pass1,
                    node_count,
                    edge_count: 0,
                    labels: labels.names().to_vec(),
                    reltypes: reltypes.names().to_vec(),
                    property_keys: keys.names().to_vec(),
                    range_stmts: range_stmts.clone(),
                    vector_stmts: vector_stmts.clone(),
                    cluster_identity: false,
                },
            )?;
            fault_after("pass1");
        }
        drop(pass1_g);

        // ---- pass 1.5 (merge dumps): dedup business-key node MERGEs ----------------
        // Collapse same-identity node MERGEs into one node each (SET props last-wins) via an
        // external sort, writing the deduped node bucket + the `(identity → prov)` key stream
        // the edge resolve below joins against. `node_count` becomes the distinct-node count.
        if merge_mode {
            let dedup_g = diag.phase("dedup");
            if resume_phase < Phase::Deduped {
                // `dedup_nodes` labels its own sub-steps (scan+sort, then the drain).
                node_count = merge_build::dedup_nodes(
                    &node_merge_bkt,
                    &remaps,
                    &node_bkt,
                    &node_keys_bkt,
                    scratch_dir,
                    &mem,
                    SCRATCH_ZSTD,
                    diag,
                )?;
                checkpoint(
                    scratch_dir,
                    &BuildState {
                        generation: generation.0.to_string(),
                        phase: Phase::Deduped,
                        node_count,
                        edge_count: 0,
                        labels: labels.names().to_vec(),
                        reltypes: reltypes.names().to_vec(),
                        property_keys: keys.names().to_vec(),
                        range_stmts: range_stmts.clone(),
                        vector_stmts: vector_stmts.clone(),
                        cluster_identity: false,
                    },
                )?;
                fault_after("deduped");
            } else {
                node_count = resume.as_ref().unwrap().node_count;
            }
            drop(dedup_g);
        }

        // ---- resolve dump ids → provisional node ids, write the edge bucket -------
        let resolve_g = diag.phase("resolve");
        if resume_phase >= Phase::Resolved {
            edge_count = resume.as_ref().unwrap().edge_count;
        } else if merge_mode {
            // Resolve each edge MERGE's endpoints by business key against the node-key
            // stream (sort-merge-join), collapse identical (src, reltype, dst) edges, and
            // write the final edge bucket — the same `EdgeRec` shape cluster/emit consume.
            diag.set_op("resolve business keys (merge-join)", "edges", 0);
            edge_count = merge_build::resolve_edges(
                &edge_merge_bkt,
                &remaps,
                &node_keys_bkt,
                &edge_bkt,
                scratch_dir,
                &mem,
                opts.threads,
                SCRATCH_ZSTD,
            )?;
            checkpoint(
                scratch_dir,
                &BuildState {
                    generation: generation.0.to_string(),
                    phase: Phase::Resolved,
                    node_count,
                    edge_count,
                    labels: labels.names().to_vec(),
                    reltypes: reltypes.names().to_vec(),
                    property_keys: keys.names().to_vec(),
                    range_stmts: range_stmts.clone(),
                    vector_stmts: vector_stmts.clone(),
                    cluster_identity: false,
                },
            )?;
            fault_after("resolve");
        } else {
            diag.set_op("resolve dump_id → node_id (parallel)", "edges", 0);
            diag.set_active_workers(opts.threads.max(1) as u64);
            // Rebuild the resolver by scanning the node bucket's dump ids (read-only once
            // built, so it is shared across resolve workers behind an `Arc`).
            let mut dump_ids: Vec<i64> = Vec::with_capacity(node_count as usize);
            buckets::for_each_node_dump_id(&node_bkt, |_, d| {
                dump_ids.push(d.unwrap_or(NO_DUMP));
                Ok(())
            })?;
            let resolver =
                std::sync::Arc::new(DumpResolver::build_dense(&dump_ids, opts.max_memory_bytes)?);
            drop(dump_ids);

            // Per-shard base prov_edge_id (prefix sum of uedge counts) → contiguous,
            // input-ordered edge ids identical to the old single-threaded resolve.
            let counts: Vec<u64> = metas.iter().map(|m| m.uedge_count).collect();
            let mut bases: Vec<u64> = Vec::with_capacity(counts.len());
            let mut acc = 0u64;
            for &c in &counts {
                bases.push(acc);
                acc += c;
            }
            let total_edges = acc;
            diag.set_op("resolve dump_id → node_id (parallel)", "edges", total_edges);

            // Each worker resolves one unresolved-edge shard into edge_bkt.<shard> with
            // global symbol ids (via the shard remap) and its deterministic id range.
            use std::sync::atomic::{AtomicU64, Ordering};
            let next = std::sync::Arc::new(AtomicU64::new(0));
            let err: std::sync::Arc<std::sync::Mutex<Option<anyhow::Error>>> =
                std::sync::Arc::new(std::sync::Mutex::new(None));
            let nshards = metas.len() as u64;
            let bases_r = &bases;
            let counts_r = &counts;
            let remaps_r = &remaps;
            let uedge_r = &uedge_bkt;
            let edge_r = &edge_bkt;
            // Overlay edge patches are folded here (this pass already holds the resolved
            // src/dst provs + global reltype). `Option<&Overlay>` is `Copy` + `Sync`, so
            // each worker shares it read-only; matched patches mark themselves hit.
            let overlay_r = overlay.as_ref();
            std::thread::scope(|scope| {
                for _ in 0..opts.threads.max(1) {
                    let next = std::sync::Arc::clone(&next);
                    let err = std::sync::Arc::clone(&err);
                    let resolver = std::sync::Arc::clone(&resolver);
                    scope.spawn(move || loop {
                        if err.lock().unwrap().is_some() {
                            break;
                        }
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= nshards {
                            break;
                        }
                        let rm = &remaps_r[i as usize];
                        let res = (|| -> Result<()> {
                            let mut id = bases_r[i as usize];
                            // One writer per resolve worker; the pool is already saturated.
                            let mut edge_w = BucketWriter::create_inline(
                                buckets::seg_path(edge_r, i),
                                BUCKET_BLOCK,
                                SCRATCH_ZSTD,
                            )?;
                            let rdr = graph_format::blockfile::BlockFileReader::open(
                                buckets::seg_path(uedge_r, i),
                            )?;
                            rdr.for_each_record(|_, rec| {
                                let ue = buckets::UnresolvedEdge::decode(rec)?;
                                let src = resolver.get(ue.src_dump).with_context(|| {
                                    format!(
                                        "edge references unknown source __dump_id__ {}",
                                        ue.src_dump
                                    )
                                })?;
                                let dst = resolver.get(ue.dst_dump).with_context(|| {
                                    format!(
                                        "edge references unknown target __dump_id__ {}",
                                        ue.dst_dump
                                    )
                                })?;
                                let props_blob = if rm.identity {
                                    ue.props_blob
                                } else {
                                    buckets::remap_props_blob(&ue.props_blob, rm)?
                                };
                                let reltype = rm.map_reltype(ue.reltype);
                                let props_blob = match overlay_r {
                                    Some(ov) if ov.has_edge_patches() => ov
                                        .fold_edge(src, dst, reltype, &props_blob)?
                                        .map(|v| Blob::from_slice(&v))
                                        .unwrap_or(props_blob),
                                    _ => props_blob,
                                };
                                edge_w.append_edge(&EdgeRec {
                                    prov_edge_id: id,
                                    src_prov: src,
                                    dst_prov: dst,
                                    reltype,
                                    props_blob,
                                })?;
                                id += 1;
                                Ok(())
                            })?;
                            edge_w.finish()?;
                            Ok(())
                        })();
                        match res {
                            Ok(()) => diag.progress_add(counts_r[i as usize]),
                            Err(e) => {
                                let mut s = err.lock().unwrap();
                                if s.is_none() {
                                    *s = Some(e);
                                }
                                break;
                            }
                        }
                    });
                }
            });
            if let Some(e) = std::sync::Arc::try_unwrap(err)
                .ok()
                .and_then(|m| m.into_inner().ok())
                .flatten()
            {
                return Err(e);
            }
            // An overlay edge patch that matched no resolved edge means the targeted
            // relationship does not exist — and edge create-on-absent is not a v1 feature.
            if let Some(ov) = overlay.as_ref() {
                let unmatched = ov.unmatched_edges();
                if let Some(&(s, d, rt)) = unmatched.first() {
                    bail!(
                        "{} overlay edge overwrite(s) matched no existing relationship \
                     (e.g. src node {s} → dst node {d}, reltype id {rt}); edge \
                     create-on-absent is not supported",
                        unmatched.len()
                    );
                }
            }
            // Unresolved-edge shards consumed; reclaim their scratch.
            for i in 0..nshards {
                let _ = std::fs::remove_file(buckets::seg_path(&uedge_bkt, i));
            }
            edge_count = total_edges;
            checkpoint(
                scratch_dir,
                &BuildState {
                    generation: generation.0.to_string(),
                    phase: Phase::Resolved,
                    node_count,
                    edge_count,
                    labels: labels.names().to_vec(),
                    reltypes: reltypes.names().to_vec(),
                    property_keys: keys.names().to_vec(),
                    range_stmts: range_stmts.clone(),
                    vector_stmts: vector_stmts.clone(),
                    cluster_identity: false,
                },
            )?;
            fault_after("resolve");
        }
        drop(resolve_g);
    } // end Cypher front-half

    // ---- pass 2: clustering → node-id permutation -----------------------------
    let cluster_g = diag.phase("cluster");
    let perm = if resume_phase >= Phase::Clustered {
        let s = resume.as_ref().unwrap();
        if s.cluster_identity {
            Permutation::Identity
        } else {
            load_perm(&perm_path, node_count)?
        }
    } else {
        // `build_permutation` labels its own sub-steps (stripe routing, stripe sort,
        // each LDG pass, final permutation) so a --diagnostics run can attribute this
        // phase's serial stretches to a named step. It routes and sorts the undirected
        // adjacency shard-parallel, so it wants the edge bucket's segments, not one
        // sequential scan of it.
        let edge_segs = buckets::segments(&edge_bkt);
        let block_capacity = (opts.block_size / 48).max(1) as u32;
        let perm = cluster::build_permutation(
            node_count,
            &ClusterParams {
                mode: opts.cluster,
                passes: opts.cluster_passes,
                block_capacity,
                budget: Arc::clone(&mem),
                temp_dir: scratch_dir.to_path_buf(),
                zstd_level: SCRATCH_ZSTD,
                threads: opts.threads,
            },
            edge_segs.len(),
            |shard, visit| {
                BlockFileReader::open(&edge_segs[shard])?.for_each_record(|_, rec| {
                    let e = EdgeRec::decode(rec)?;
                    visit(e.src_prov, e.dst_prov)
                })
            },
            diag,
        )?;
        save_perm(&perm, &perm_path)?;
        checkpoint(
            scratch_dir,
            &BuildState {
                generation: generation.0.to_string(),
                phase: Phase::Clustered,
                node_count,
                edge_count,
                labels: labels.names().to_vec(),
                reltypes: reltypes.names().to_vec(),
                property_keys: keys.names().to_vec(),
                range_stmts: range_stmts.clone(),
                vector_stmts: vector_stmts.clone(),
                cluster_identity: perm.is_identity(),
            },
        )?;
        fault_after("cluster");
        perm
    };
    drop(cluster_g);

    // ---- emit (always redone on resume) --------------------------------------
    let mut block_sizes: BTreeMap<String, u32> = BTreeMap::new();
    // In `merge` mode the deduped node bucket already holds GLOBAL symbol ids (the
    // pass-1 remaps applied during dedup), so the node scan must NOT remap again —
    // an empty slice makes `for_each_node_remapped` byte-copy each blob unchanged.
    let emit_remaps: &[buckets::ShardRemap] = if merge_mode { &[] } else { &remaps };

    // Range-index plumbing: one external sorter per declared index, plus the
    // resolved (label/reltype id, key id) needed to extract entries during emit.
    struct RangeMeta {
        name: String,
        entity: EntityKind,
        label_or_type: String,
        property: String,
    }
    struct NodeRangeSpec {
        idx: usize,
        label_id: Option<u32>,
        key_id: Option<u32>,
    }
    // Emit-phase memory split. During `emit.topology` three sets of sorters are live
    // at once — the range indexes, the two endpoint-posting sinks, and the band-worker
    // pool — so each takes a *named share of one budget* rather than helping itself to
    // `max_memory / 16` of a number nobody was tracking. The pool gets whatever is
    // left, which is the bulk of it: the pool is the set that scales with `--threads`,
    // and it is where the resident bytes actually went (peak RSS was reached inside
    // "emit forward CSR + edge_props per band").
    //
    // The range sorters are the longest-lived of the three — created here, drained in
    // `emit.range_isam` well after topology — so they are reserved first and hold
    // their bytes across everything below.
    let range_want = if range_stmts.is_empty() {
        0
    } else {
        (mem.total() / 8 / range_stmts.len()).max(RANGE_SORT_FLOOR)
    };
    let mut range_metas: Vec<RangeMeta> = Vec::new();
    let mut range_sorters: Vec<ExtSorter<RangeEntry>> = Vec::new();
    let mut node_range: Vec<NodeRangeSpec> = Vec::new();
    let mut edge_range: Vec<EdgeRangeSpec> = Vec::new();
    for ri in &range_stmts {
        let idx = range_metas.len();
        let key_id = keys.get(&ri.property);
        match ri.entity {
            Entity::Node => {
                node_range.push(NodeRangeSpec {
                    idx,
                    label_id: labels.get(&ri.label_or_type),
                    key_id,
                });
                range_metas.push(RangeMeta {
                    name: format!("node_{}_{}", ri.label_or_type, ri.property),
                    entity: EntityKind::Node,
                    label_or_type: ri.label_or_type.clone(),
                    property: ri.property.clone(),
                });
            }
            Entity::Edge => {
                edge_range.push(EdgeRangeSpec {
                    idx,
                    reltype_id: reltypes.get(&ri.label_or_type),
                    key_id,
                });
                range_metas.push(RangeMeta {
                    name: format!("edge_{}_{}", ri.label_or_type, ri.property),
                    entity: EntityKind::Edge,
                    label_or_type: ri.label_or_type.clone(),
                    property: ri.property.clone(),
                });
            }
        }
        range_sorters.push(ExtSorter::<RangeEntry>::new(
            scratch_dir,
            mem.reserve_now("range-index sorter", range_want, RANGE_SORT_FLOOR)?,
            opts.zstd_level,
        )?);
    }

    // Vector-index plumbing: gather each declared index's `(final_id, vector)` set
    // during the node scan (the vectors were routed to `vec_props` in pass 1). The
    // index files themselves are written by the shared `write_vector_indexes`. Each
    // spec is `(pending_idx, label_id, property, dim)`.
    let mut vec_specs: Vec<(usize, Option<u32>, String, u32)> = Vec::new();
    let mut pending: Vec<PendingIndex> = Vec::new();
    for vi in &vector_stmts {
        vec_specs.push((
            pending.len(),
            labels.get(&vi.label),
            vi.property.clone(),
            vi.dim,
        ));
        pending.push(PendingIndex {
            label: vi.label.clone(),
            property: vi.property.clone(),
            dim: vi.dim,
            metric: parse_metric(&vi.metric)?,
            entries: Vec::new(),
        });
    }

    // --- node stores: node_props.blk + node_labels.blk ---
    let mut props_w = PropsWriter::create_with_cipher(
        tmp_dir.join("node_props.blk"),
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    // Choose the label encoding by alphabet: a u64 bitmask (Raw container) when it fits, else
    // varint (zstd). The alphabet is finalised by emit, so this is decided correctly here.
    let mut labels_w = NodeLabelsWriter::create_for_alphabet(
        tmp_dir.join("node_labels.blk"),
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
        labels.names().len(),
    )?;

    let emit_node_ranges =
        |node: &NodeRec, final_id: u64, sorters: &mut [ExtSorter<RangeEntry>]| -> Result<()> {
            for spec in &node_range {
                if let (Some(lid), Some(kid)) = (spec.label_id, spec.key_id) {
                    if has_label(&node.labels_blob, lid)? {
                        if let Some(v) = extract_value(&node.props_blob, kid)? {
                            sorters[spec.idx].push(RangeEntry {
                                key: v,
                                id: final_id,
                            })?;
                        }
                    }
                }
            }
            Ok(())
        };

    let emit_nodes_g = diag.phase("emit.node_stores");
    diag.set_op(
        "scan nodes → node_props.blk + node_labels.blk",
        "nodes",
        node_count,
    );
    // Fold overlay set-prop patches onto a node (keyed by provisional id) before any
    // node-derived output, so the rewritten value flows into props, labels-independent
    // range indexes and histograms alike. A no-op when the node has no patch.
    let fold_node = |node: NodeRec, prov: u64| -> Result<NodeRec> {
        if let Some(ov) = overlay.as_ref() {
            if ov.has_node_patches() {
                if let Some(folded) = ov.fold_node(prov, &node.props_blob)? {
                    return Ok(NodeRec {
                        props_blob: Blob::from_slice(&folded),
                        ..node
                    });
                }
            }
        }
        Ok(node)
    };
    if perm.is_identity() {
        // Fast path: final id = prov id, so byte-copy straight through in order.
        // `for_each_node_remapped` translates each shard's local symbol ids to
        // global (identity shards byte-copy unchanged).
        buckets::for_each_node_remapped(&node_bkt, emit_remaps, |prov, node| {
            diag.tick(prov);
            let node = fold_node(node, prov)?;
            labels_w.append_blob(&node.labels_blob)?;
            props_w.append_raw(&node.props_blob)?;
            emit_node_ranges(&node, prov, &mut range_sorters)?;
            gather_node_vectors(&node, prov, &vec_specs, &mut pending)?;
            Ok(())
        })?;
        // MERGE-created overlay nodes follow at prov = base_node_count + i (= final id
        // under the identity permutation), in creation order.
        if let Some(ov) = overlay.as_ref() {
            for (i, cnode) in ov.created.iter().enumerate() {
                let final_id = base_node_count + i as u64;
                diag.tick(final_id);
                labels_w.append_blob(&cnode.labels_blob)?;
                props_w.append_raw(&cnode.props_blob)?;
                emit_node_ranges(cnode, final_id, &mut range_sorters)?;
                gather_node_vectors(cnode, final_id, &vec_specs, &mut pending)?;
            }
        }
    } else {
        // The only sorter live in this phase besides the (small) range sinks, and it
        // is consumed before `emit.topology` starts — so it may take everything the
        // range sorters left. A big buffer means few runs means a cheap merge.
        let mut node_sorter = ExtSorter::<NodeEmit>::new(
            scratch_dir,
            mem.reserve_now("node-store sorter", mem.available(), MIN_SORT_BYTES)?,
            SCRATCH_ZSTD,
        )?;
        buckets::for_each_node_remapped(&node_bkt, emit_remaps, |prov, node| {
            diag.tick(prov);
            let node = fold_node(node, prov)?;
            let final_id = perm.final_of(prov);
            emit_node_ranges(&node, final_id, &mut range_sorters)?;
            gather_node_vectors(&node, final_id, &vec_specs, &mut pending)?;
            node_sorter.push(NodeEmit {
                final_id,
                labels_blob: node.labels_blob,
                props_blob: node.props_blob,
            })?;
            Ok(())
        })?;
        // MERGE-created overlay nodes carry provisional ids base_node_count + i; route
        // them through the same final-id sort.
        if let Some(ov) = overlay.as_ref() {
            for (i, cnode) in ov.created.iter().enumerate() {
                let final_id = perm.final_of(base_node_count + i as u64);
                emit_node_ranges(cnode, final_id, &mut range_sorters)?;
                gather_node_vectors(cnode, final_id, &vec_specs, &mut pending)?;
                node_sorter.push(NodeEmit {
                    final_id,
                    labels_blob: cnode.labels_blob.clone(),
                    props_blob: cnode.props_blob.clone(),
                })?;
            }
        }
        diag.set_op("sort + write nodes (clustered order)", "nodes", node_count);
        let mut written = 0u64;
        for r in node_sorter.sorted()? {
            let ne = r?;
            labels_w.append_blob(&ne.labels_blob)?;
            props_w.append_raw(&ne.props_blob)?;
            written += 1;
            diag.tick(written);
        }
    }
    props_w.finish()?;
    labels_w.finish()?;
    block_sizes.insert("node_props.blk".into(), opts.block_size as u32);
    block_sizes.insert("node_labels.blk".into(), opts.block_size as u32);
    drop(emit_nodes_g);

    // --- topology.csr.blk + edge_props.blk (range-partitioned parallel emit) ---
    //
    // The five fused outputs (forward CSR, reverse CSR, edge_props in final-edge-id
    // order, (reltype,src)/(reltype,dst) postings, edge range entries) are produced
    // by partitioning the resolved edges into fixed `BAND_NODES`-wide node bands and
    // working each band in parallel, then stitching the per-band block files with a
    // verbatim block-concat. `final_edge_id` is `base_b + i` (band prefix-sum base +
    // sorted position within the band) — bit-identical to the serial forward-merge
    // position because bands partition by the primary sort key `final_src`. Only the
    // *block layout* (boundaries fall at band edges) differs from the serial stream,
    // so `topology.csr.blk` / `edge_props.blk` content hashes change once; the logical
    // content (and the postings / range ISAMs) is identical.
    let emit_topo_g = diag.phase("emit.topology");
    let threads = opts.threads.max(1);
    let band = band_nodes();
    let nbands = node_count.div_ceil(band).max(1) as usize;
    let pid = std::process::id();
    // Local batching buffer per (worker, band); cap total batching memory at a small
    // fraction of the budget so the route buffers never rival the sorters.
    let batch_threshold =
        (opts.max_memory_bytes / 32 / (nbands * threads).max(1)).clamp(16 * 1024, 1 << 20);

    // 1) Partition resolved edges into per-src-band files (parallel over the edge
    //    bucket's segments) and count edges per band → prefix-sum `base_b`.
    diag.set_op("partition edges by src band", "edges", edge_count);
    diag.set_active_workers(threads as u64);
    let fwd_spill = BandSpill::new(nbands, |b| band_path(scratch_dir, pid, "fwd_band", b))?;
    {
        let segs = buckets::segments(&edge_bkt);
        let next = AtomicU64::new(0);
        let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let (perm_r, fwd_spill_r, segs_r, next_r, err_r) = (&perm, &fwd_spill, &segs, &next, &err);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(move || {
                    let mut batcher = BandBatcher::new(fwd_spill_r, batch_threshold);
                    loop {
                        if err_r.lock().unwrap().is_some() {
                            break;
                        }
                        let si = next_r.fetch_add(1, Ordering::Relaxed) as usize;
                        if si >= segs_r.len() {
                            break;
                        }
                        let res = (|| -> Result<()> {
                            let r = BlockFileReader::open(&segs_r[si])?;
                            let mut local = 0u64;
                            r.for_each_record(|_, rec| {
                                let e = EdgeRec::decode(rec)?;
                                let final_src = perm_r.final_of(e.src_prov);
                                let final_dst = perm_r.final_of(e.dst_prov);
                                batcher.push(
                                    (final_src / band) as usize,
                                    &EdgeFwd {
                                        final_src,
                                        final_dst,
                                        prov_edge_id: e.prov_edge_id,
                                        reltype: e.reltype,
                                        props_blob: e.props_blob,
                                    },
                                )?;
                                local += 1;
                                Ok(())
                            })?;
                            batcher.flush_all()?;
                            diag.progress_add(local);
                            Ok(())
                        })();
                        if let Err(e) = res {
                            let mut g = err_r.lock().unwrap();
                            if g.is_none() {
                                *g = Some(e);
                            }
                            break;
                        }
                    }
                });
            }
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
    }
    let (fwd_band_paths, band_counts) = fwd_spill.finish()?;
    let mut base = vec![0u64; nbands + 1];
    for b in 0..nbands {
        base[b + 1] = base[b] + band_counts[b];
    }

    // Shared sinks for the forward phase. Edge-range entries are fed into global
    // externally-sorted sinks (push order is irrelevant — they sort) behind a mutex;
    // the reverse records are range-routed by `final_dst` into per-dst-band files
    // for the parallel reverse phase.
    //
    // Endpoint postings want only a per-reltype *set* of node ids, so bit planes
    // answer them outright: no sort, no spill, and nothing held away from the band
    // workers but the planes themselves. `reltype_count` and `node_count` are both
    // final long before this phase (pass 1 interns the reltypes; the band layout
    // above already needs `node_count`), so the planes can be sized up front.
    let reltype_count = reltypes.names().len() as u32;
    let planes_bytes = 2 * EndpointPlanes::bytes_for(reltype_count, node_count);
    let plane_cap = (mem.total() / 8) as u64;
    let posting_sinks = if planes_bytes <= plane_cap && !force_sorter_postings() {
        let res = mem.reserve_now(
            "endpoint posting planes",
            planes_bytes as usize,
            planes_bytes as usize,
        )?;
        PostingSinks::Planes {
            src: EndpointPlanes::new(reltype_count, node_count),
            tgt: EndpointPlanes::new(reltype_count, node_count),
            _res: res,
        }
    } else {
        // A graph both large and richly typed enough to blow the plane budget, or a
        // test forcing this path. Costs 2 records per edge through an external sort.
        let post_want = (mem.total() / 16).max(MIN_SORT_BYTES);
        PostingSinks::Sorters {
            src: Mutex::new(ExtSorter::<RelEndpoint>::new(
                scratch_dir,
                mem.reserve_now("src endpoint postings", post_want, MIN_SORT_BYTES)?,
                SCRATCH_ZSTD,
            )?),
            tgt: Mutex::new(ExtSorter::<RelEndpoint>::new(
                scratch_dir,
                mem.reserve_now("tgt endpoint postings", post_want, MIN_SORT_BYTES)?,
                SCRATCH_ZSTD,
            )?),
        }
    };
    let range_mx = Mutex::new(range_sorters);
    let rev_spill = BandSpill::new(nbands, |b| band_path(scratch_dir, pid, "rev_route", b))?;

    // Everything the long-lived sinks left goes to the band workers, re-lent as a
    // budget of their own. A worker reserves one slice per band and returns it when
    // the band is done, so `reserve` inside a worker is a wait on a *peer*, never on
    // the caller — the one shape in which blocking for memory cannot deadlock.
    //
    // The consequence the plan asked for: when the cap is tight, `threads` workers
    // cannot all hold a slice, so the surplus ones park until a band finishes.
    // Parallelism is then throttled by memory rather than by core count, instead of
    // every worker over-committing and the phase peaking at 2× the cap. If not even
    // one worker can be funded, `reserve` fails loudly rather than parking forever.
    let worker_pool = mem
        .reserve_now("band-worker pool", mem.available(), MIN_SORT_BYTES)?
        .into_sub_budget();
    let worker_want = (worker_pool.total() / threads).max(MIN_SORT_BYTES);
    // Are there enough bands to keep every worker busy? If so each band sorter spills
    // inline (see `emit_forward_band`); if not, they lean on the shared spill pool.
    let bands_saturate_pool = nbands >= threads;

    // 2) Forward: each band sorts its edges by (final_src, final_dst, prov_edge_id),
    //    assigns final_edge_id = base_b + i, writes its forward CSR half + edge_props
    //    slice, feeds the postings/edge-range sinks, and routes reverse records.
    diag.set_op(
        "emit forward CSR + edge_props per band",
        "edges",
        edge_count,
    );
    diag.set_active_workers(threads as u64);
    {
        let next = AtomicU64::new(0);
        let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let (
            base_r,
            fwd_band_paths_r,
            edge_range_r,
            posts_r,
            range_r,
            rev_spill_r,
            worker_pool_r,
            cipher_r,
            next_r,
            err_r,
        ) = (
            &base,
            &fwd_band_paths,
            &edge_range,
            &posting_sinks,
            &range_mx,
            &rev_spill,
            &worker_pool,
            &cipher,
            &next,
            &err,
        );
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(move || loop {
                    if err_r.lock().unwrap().is_some() {
                        break;
                    }
                    let b = next_r.fetch_add(1, Ordering::Relaxed) as usize;
                    if b >= nbands {
                        break;
                    }
                    let lo = (b as u64) * band;
                    let hi = (((b as u64) + 1) * band).min(node_count);
                    let res = emit_forward_band(
                        lo,
                        hi,
                        band,
                        base_r[b],
                        &fwd_band_paths_r[b],
                        &band_path(scratch_dir, pid, "csr_fwd", b),
                        &band_path(scratch_dir, pid, "eprops", b),
                        edge_range_r,
                        posts_r,
                        range_r,
                        rev_spill_r,
                        batch_threshold,
                        scratch_dir,
                        worker_pool_r,
                        worker_want,
                        bands_saturate_pool,
                        opts.block_size,
                        opts.zstd_level,
                        cipher_r.clone(),
                        diag,
                    );
                    if let Err(e) = res {
                        let mut g = err_r.lock().unwrap();
                        if g.is_none() {
                            *g = Some(e);
                        }
                        break;
                    }
                });
            }
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
    }
    for p in &fwd_band_paths {
        let _ = std::fs::remove_file(p);
    }
    let (rev_route_paths, _rev_counts) = rev_spill.finish()?;

    // 3) Reverse: each dst-band sorts its routed records by (final_dst, final_edge_id)
    //    and writes its reverse CSR half. Independent per band — no global merge.
    diag.set_op("emit reverse CSR per band", "edges", edge_count);
    diag.set_active_workers(threads as u64);
    {
        let next = AtomicU64::new(0);
        let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let (rev_route_paths_r, worker_pool_r, cipher_r, next_r, err_r) =
            (&rev_route_paths, &worker_pool, &cipher, &next, &err);
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(move || loop {
                    if err_r.lock().unwrap().is_some() {
                        break;
                    }
                    let b = next_r.fetch_add(1, Ordering::Relaxed) as usize;
                    if b >= nbands {
                        break;
                    }
                    let lo = (b as u64) * band;
                    let hi = (((b as u64) + 1) * band).min(node_count);
                    let res = emit_reverse_band(
                        lo,
                        hi,
                        &rev_route_paths_r[b],
                        &band_path(scratch_dir, pid, "csr_rev", b),
                        scratch_dir,
                        worker_pool_r,
                        worker_want,
                        bands_saturate_pool,
                        opts.block_size,
                        opts.zstd_level,
                        cipher_r.clone(),
                        diag,
                    );
                    if let Err(e) = res {
                        let mut g = err_r.lock().unwrap();
                        if g.is_none() {
                            *g = Some(e);
                        }
                        break;
                    }
                });
            }
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
    }
    for p in &rev_route_paths {
        let _ = std::fs::remove_file(p);
    }
    // The band workers are done: hand their share back so the phases below (the
    // postings drain, `emit.graph_summaries`) can reserve against a full budget.
    drop(worker_pool);

    // 4) Stitch: concat the per-band block files (forward halves then reverse halves
    //    for the CSR; band order for edge_props), then drain the postings sinks.
    //
    // Four serial operations, labelled separately: one `stitch` label over all of them
    // hid which was costing what, and led to the whole 272.5s being attributed to the
    // concat — see the note on the postings drain below.
    diag.set_active_workers(1);
    diag.set_op("concat topology.csr.blk", "", 0);
    let mut csr_parts: Vec<PathBuf> = Vec::with_capacity(nbands * 2);
    csr_parts.extend((0..nbands).map(|b| band_path(scratch_dir, pid, "csr_fwd", b)));
    csr_parts.extend((0..nbands).map(|b| band_path(scratch_dir, pid, "csr_rev", b)));
    concat_block_files(tmp_dir.join("topology.csr.blk"), &csr_parts)?;
    diag.set_op("concat edge_props.blk", "", 0);
    let eprops_parts: Vec<PathBuf> = (0..nbands)
        .map(|b| band_path(scratch_dir, pid, "eprops", b))
        .collect();
    concat_block_files(tmp_dir.join("edge_props.blk"), &eprops_parts)?;
    diag.set_op("remove band scratch files", "", 0);
    for p in csr_parts.iter().chain(eprops_parts.iter()) {
        let _ = std::fs::remove_file(p);
    }
    block_sizes.insert("edge_props.blk".into(), opts.block_size as u32);
    block_sizes.insert("topology.csr.blk".into(), opts.block_size as u32);

    // Write the endpoint postings. From bit planes this is a linear scan of
    // `reltype_count × node_count` bits per side. From the sorter fallback it is a
    // k-way merge over one `RelEndpoint` per edge — 1.49B of them at Wikidata scale
    // — decompressing every run block on this one thread. Not a file copy; do not
    // read the merge's cost as the concat's.
    let (reltype_source_counts, reltype_target_counts) = match posting_sinks {
        PostingSinks::Planes { src, tgt, _res } => {
            // `BlockFileWriter` copies an over-target record into `cur_data` and
            // again into the raw block, so the peak is ~3× the largest record. The
            // sorter path never reserved its equivalent (`bucket: Vec<u64>` plus
            // both copies) — that was the gap `budget_reserved_bytes` reported.
            let rec = src.max_record_bytes().max(tgt.max_record_bytes());
            let _rec = mem.reserve_now("endpoint posting record", rec * 3, rec * 3)?;
            diag.set_op(
                "write reltype_src.post (bit planes)",
                "reltypes",
                reltype_count as u64,
            );
            let sc = write_endpoint_postings_from_planes(
                tmp_dir.join("reltype_src.post"),
                &src,
                opts.block_size,
                opts.zstd_level,
                cipher.clone(),
            )?;
            diag.set_op(
                "write reltype_tgt.post (bit planes)",
                "reltypes",
                reltype_count as u64,
            );
            let tc = write_endpoint_postings_from_planes(
                tmp_dir.join("reltype_tgt.post"),
                &tgt,
                opts.block_size,
                opts.zstd_level,
                cipher.clone(),
            )?;
            (sc, tc)
        }
        PostingSinks::Sorters { src, tgt } => {
            diag.set_op("drain reltype_src.post (k-way merge)", "edges", edge_count);
            let sc = write_endpoint_postings_from_sorted(
                tmp_dir.join("reltype_src.post"),
                reltype_count,
                src.into_inner()
                    .unwrap()
                    .sorted()?
                    .map(|r| r.map(|e| (e.reltype, e.node))),
                opts.block_size,
                opts.zstd_level,
                cipher.clone(),
            )?;
            diag.set_op("drain reltype_tgt.post (k-way merge)", "edges", edge_count);
            let tc = write_endpoint_postings_from_sorted(
                tmp_dir.join("reltype_tgt.post"),
                reltype_count,
                tgt.into_inner()
                    .unwrap()
                    .sorted()?
                    .map(|r| r.map(|e| (e.reltype, e.node))),
                opts.block_size,
                opts.zstd_level,
                cipher.clone(),
            )?;
            (sc, tc)
        }
    };
    block_sizes.insert("reltype_src.post".into(), opts.block_size as u32);
    block_sizes.insert("reltype_tgt.post".into(), opts.block_size as u32);
    // Recover the range sorters (now carrying the edge-range entries) for the range
    // ISAM phase below.
    let range_sorters = range_mx.into_inner().unwrap();
    drop(emit_topo_g);

    // Whole-graph metadata summaries — one post-emit pass over the finished
    // topology + node labels, persisted so `open` need not rescan and the
    // label/reltype fast paths answer from resident metadata.
    let emit_summary_g = diag.phase("emit.graph_summaries");
    let mut summaries = compute_graph_summaries(
        &tmp_dir.join("topology.csr.blk"),
        &tmp_dir.join("node_labels.blk"),
        node_count,
        reltypes.names().len(),
        labels.names().len(),
        opts.hub_degree_floor,
        cipher.clone(),
        scratch_dir,
        &mem,
        opts.threads,
        diag,
    )?;
    drop(emit_summary_g);

    // --- vectors.f32.blk + any Vamana/PQ files (via the shared writer) ---
    let emit_vec_g = diag.phase("emit.vectors");
    diag.set_op(
        "write vector indexes (vamana/pq/brute)",
        "indexes",
        pending.len() as u64,
    );
    let (vector_indexes, vector_files) =
        write_vector_indexes(tmp_dir, &pending, opts, cipher.clone(), &mut block_sizes)?;
    drop(emit_vec_g);

    // --- range/*.isam (each fed its external-sorted stream) ---
    let emit_range_g = diag.phase("emit.range_isam");
    diag.set_op("write range ISAMs", "indexes", range_metas.len() as u64);
    let mut range_indexes: Vec<RangeIndexDesc> = Vec::new();
    for (done, (meta, sorter)) in range_metas.into_iter().zip(range_sorters).enumerate() {
        diag.set_op_detail(&meta.name);
        diag.set_progress(done as u64);
        let rel_path = format!("range/{}.isam", meta.name);
        write_isam_sorted(
            tmp_dir.join(&rel_path),
            sorter.sorted()?.map(|r| r.map(|re| (re.key, re.id))),
            opts.range_block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        block_sizes.insert(rel_path, opts.range_block_size as u32);
        range_indexes.push(RangeIndexDesc {
            name: meta.name,
            entity: meta.entity,
            label_or_type: meta.label_or_type,
            property: meta.property,
        });
    }
    drop(emit_range_g);

    // prop_hist.blk — value→count histograms from the node range ISAMs just
    // written (run-length-count the finished ISAM). High-cardinality / disabled
    // indexes are skipped.
    let emit_hist_g = diag.phase("emit.prop_hist");
    diag.set_op(
        "derive prop_hist.blk from range ISAMs",
        "indexes",
        range_indexes.len() as u64,
    );
    let property_histograms = common::build_property_histograms(
        tmp_dir,
        &range_indexes,
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
        opts.histogram_max_distinct,
    )?;
    block_sizes.insert("prop_hist.blk".into(), opts.block_size as u32);
    drop(emit_hist_g);

    // hub_degrees.blk — per-node out/in degree for nodes at/above `hubDegreeFloor`,
    // collected during the summary pass, so a traversal identifies a hub with no
    // adjacency read. Always written (empty lists ⇒ two empty records) so the inventory
    // and content hash stay stable.
    let emit_hub_g = diag.phase("emit.hub_degrees");
    let hub_out = std::mem::take(&mut summaries.out_hub_degrees);
    let hub_in = std::mem::take(&mut summaries.in_hub_degrees);
    graph_format::hubdegree::write_hub_degrees(
        tmp_dir.join("hub_degrees.blk"),
        &hub_out,
        &hub_in,
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    block_sizes.insert("hub_degrees.blk".into(), opts.block_size as u32);
    let hub_degrees = Some(graph_format::manifest::HubDegreeDesc {
        floor: opts.hub_degree_floor,
        out_hubs: hub_out.len() as u64,
        in_hubs: hub_in.len() as u64,
    });
    drop(emit_hub_g);

    // node_degrees.blk — the dense per-node out/in degree column (also collected during the
    // summary pass), so the degree-sum count fast path answers a penultimate-frontier
    // lookup in O(1) with no adjacency read. Always written (empty for a 0-node graph).
    let emit_deg_g = diag.phase("emit.node_degrees");
    let out_degs = std::mem::take(&mut summaries.out_degrees);
    let in_degs = std::mem::take(&mut summaries.in_degrees);
    graph_format::nodedegree::write_node_degrees(
        tmp_dir.join("node_degrees.blk"),
        &out_degs,
        &in_degs,
        opts.block_size,
        graph_format::degree_ef::DegreeCodecOpts {
            zstd_level: opts.zstd_level,
            zstd_margin: opts.degree_zstd_margin,
        },
        cipher.clone(),
    )?;
    block_sizes.insert("node_degrees.blk".into(), opts.block_size as u32);
    drop(emit_deg_g);

    // ---- publish (via the shared scaffolding) ----
    let _publish_g = diag.phase("publish");
    diag.set_op("manifest + fsync + atomic publish", "", 0);
    common::write_manifest_and_publish(PublishInputs {
        tmp_dir,
        graph_dir,
        final_dir,
        generation,
        graph,
        zstd_level: opts.zstd_level,
        compression_profile: opts.compression_profile.clone(),
        block_sizes,
        node_count,
        edge_count,
        labels: labels.into_names(),
        reltypes: reltypes.into_names(),
        property_keys: keys.into_names(),
        range_indexes,
        vector_indexes,
        reltype_source_counts,
        reltype_target_counts,
        reltype_edge_counts: summaries.reltype_edge_counts,
        reltype_self_loop_counts: summaries.reltype_self_loop_counts,
        label_node_counts: summaries.label_node_counts,
        first_label_counts: summaries.first_label_counts,
        src_label_reltype_counts: summaries.src_label_reltype_counts,
        reltype_tgt_label_counts: summaries.reltype_tgt_label_counts,
        schema_triple_counts: summaries.schema_triple_counts,
        property_histograms,
        hub_degrees,
        encryption_header,
        encryption_key: &opts.encryption_key,
        acl_blake3: opts.acl_blake3.clone(),
        extra_files: vector_files,
        store: opts.publish_store.clone(),
        force_object_checksums: opts.object_checksums,
    })
}

/// Gather a node's routed vectors into the pending per-index entry sets.
fn gather_node_vectors(
    node: &NodeRec,
    final_id: u64,
    specs: &[(usize, Option<u32>, String, u32)],
    pending: &mut [PendingIndex],
) -> Result<()> {
    for (idx, label_id, property, dim) in specs {
        let Some(lid) = label_id else { continue };
        if has_label(&node.labels_blob, *lid)? {
            if let Some((_, xs)) = node.vec_props.iter().find(|(k, _)| k == property) {
                if xs.len() as u32 != *dim {
                    bail!(
                        "vector index {} declared dim {dim} but a node has {}",
                        property,
                        xs.len()
                    );
                }
                pending[*idx].entries.push((final_id, xs.clone()));
            }
        }
    }
    Ok(())
}

/// True if a pre-encoded label record contains `label_id`.
fn has_label(labels_blob: &[u8], label_id: u32) -> Result<bool> {
    let mut r = labels_blob;
    let count = read_uvarint(&mut r)?;
    for _ in 0..count {
        if read_uvarint(&mut r)? as u32 == label_id {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Extract the value of `key_id` from a pre-encoded property record, if present.
fn extract_value(props_blob: &[u8], key_id: u32) -> Result<Option<Value>> {
    let mut r = props_blob;
    let count = read_uvarint(&mut r)?;
    for _ in 0..count {
        let k = read_uvarint(&mut r)? as u32;
        if k == key_id {
            return Ok(Some(read_value(&mut r)?));
        }
        skip_value(&mut r)?;
    }
    Ok(None)
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let head: String = s.chars().take(n).collect();
        format!("{head}…")
    }
}

// ---- external-sort record types ----

/// Node payload sorted by final id for the emit reorder.
struct NodeEmit {
    final_id: u64,
    labels_blob: Blob,
    props_blob: Blob,
}
impl SortRecord for NodeEmit {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.final_id);
        write_blob(buf, &self.labels_blob);
        write_blob(buf, &self.props_blob);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let final_id = read_uvarint(r)?;
        let labels_blob = Blob::from_slice(read_blob(r)?);
        let props_blob = Blob::from_slice(read_blob(r)?);
        Ok(NodeEmit {
            final_id,
            labels_blob,
            props_blob,
        })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.final_id.cmp(&other.final_id)
    }
    fn size_hint(&self) -> usize {
        16 + self.labels_blob.len() + self.props_blob.len()
    }
}

/// Edge sorted by source for the forward CSR half (and to assign final edge ids).
struct EdgeFwd {
    final_src: u64,
    final_dst: u64,
    prov_edge_id: u64,
    reltype: u32,
    props_blob: Blob,
}
impl SortRecord for EdgeFwd {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.final_src);
        write_uvarint(buf, self.final_dst);
        write_uvarint(buf, self.prov_edge_id);
        write_uvarint(buf, self.reltype as u64);
        write_blob(buf, &self.props_blob);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let final_src = read_uvarint(r)?;
        let final_dst = read_uvarint(r)?;
        let prov_edge_id = read_uvarint(r)?;
        let reltype = read_uvarint(r)? as u32;
        let props_blob = Blob::from_slice(read_blob(r)?);
        Ok(EdgeFwd {
            final_src,
            final_dst,
            prov_edge_id,
            reltype,
            props_blob,
        })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.final_src
            .cmp(&other.final_src)
            .then(self.final_dst.cmp(&other.final_dst))
            .then(self.prov_edge_id.cmp(&other.prov_edge_id))
    }
    fn size_hint(&self) -> usize {
        40 + self.props_blob.len()
    }
}

/// Edge sorted by destination for the reverse CSR half.
struct EdgeRev {
    final_dst: u64,
    final_edge_id: u64,
    final_src: u64,
    reltype: u32,
}
impl SortRecord for EdgeRev {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.final_dst);
        write_uvarint(buf, self.final_edge_id);
        write_uvarint(buf, self.final_src);
        write_uvarint(buf, self.reltype as u64);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let final_dst = read_uvarint(r)?;
        let final_edge_id = read_uvarint(r)?;
        let final_src = read_uvarint(r)?;
        let reltype = read_uvarint(r)? as u32;
        Ok(EdgeRev {
            final_dst,
            final_edge_id,
            final_src,
            reltype,
        })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.final_dst
            .cmp(&other.final_dst)
            .then(self.final_edge_id.cmp(&other.final_edge_id))
    }
    fn size_hint(&self) -> usize {
        32
    }
}

/// One `(dst, src_label, reltype)` spill record for the edge-schema-cube join —
/// one per (edge × source label). Sorted by `dst` so the drain merges linearly with
/// a node-id-ordered walk of `node_labels`, which supplies the target labels to
/// complete each `(src_label, reltype, tgt_label)` triple without a resident map.
struct TripleSpill {
    dst: u64,
    src_label: u32,
    reltype: u32,
}
impl SortRecord for TripleSpill {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.dst);
        write_uvarint(buf, self.src_label as u64);
        write_uvarint(buf, self.reltype as u64);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let dst = read_uvarint(r)?;
        let src_label = read_uvarint(r)? as u32;
        let reltype = read_uvarint(r)? as u32;
        Ok(TripleSpill {
            dst,
            src_label,
            reltype,
        })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.dst.cmp(&other.dst)
    }
    fn size_hint(&self) -> usize {
        16
    }
}

/// Where the forward pass records "node `n` is an endpoint of a reltype-`t` edge".
///
/// Both variants produce byte-identical `reltype_{src,tgt}.post` files; the
/// builder picks `Planes` unless they would not fit the memory budget. `Planes`
/// needs no sort, no spill and no mutex — a band owns a disjoint slice of the
/// source plane, and target bits are set with an atomic `fetch_or`. `Sorters` is
/// the bounded-memory fallback: one `RelEndpoint` per edge per side into an
/// external sort, drained as a k-way merge.
enum PostingSinks {
    Planes {
        src: EndpointPlanes,
        tgt: EndpointPlanes,
        /// Holds the planes' bytes against the budget for as long as they exist.
        _res: Reservation,
    },
    Sorters {
        src: Mutex<ExtSorter<RelEndpoint>>,
        tgt: Mutex<ExtSorter<RelEndpoint>>,
    },
}

/// A `(reltype, node)` endpoint posting entry, sorted by reltype then node so the
/// drain can write one ascending-distinct record per reltype. Used for both the
/// source posting (node = edge source) and the target posting (node = edge dest).
struct RelEndpoint {
    reltype: u32,
    node: u64,
}
impl SortRecord for RelEndpoint {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.reltype as u64);
        write_uvarint(buf, self.node);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let reltype = read_uvarint(r)? as u32;
        let node = read_uvarint(r)?;
        Ok(RelEndpoint { reltype, node })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.reltype
            .cmp(&other.reltype)
            .then(self.node.cmp(&other.node))
    }
    fn size_hint(&self) -> usize {
        16
    }
}

/// A `(value, id)` range-index entry, sorted by key then id.
struct RangeEntry {
    key: Value,
    id: u64,
}
impl SortRecord for RangeEntry {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_value(buf, &self.key);
        write_uvarint(buf, self.id);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let key = read_value(r)?;
        let id = read_uvarint(r)?;
        Ok(RangeEntry { key, id })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp_key(&other.key).then(self.id.cmp(&other.id))
    }
    fn size_hint(&self) -> usize {
        match &self.key {
            Value::Str(s) => s.len() + 16,
            _ => 16,
        }
    }
}

// ---- range-partitioned parallel emit.topology plumbing ----

/// A set of per-band scratch block files written concurrently by many workers. Each
/// band has its own lock; workers batch encoded records locally (see [`BandBatcher`])
/// and take a band's lock only on bulk flushes, so cross-worker contention is rare.
/// Used to range-partition the edge stream by node id — forward edges by `final_src`,
/// reverse records by `final_dst` — into the per-band files the parallel emit drains.
/// The files are transient scratch (plaintext, [`SCRATCH_ZSTD`]); never published.
pub(crate) struct BandSpill {
    writers: Vec<Mutex<BandWriter>>,
    paths: Vec<PathBuf>,
}

struct BandWriter {
    w: BlockFileWriter,
    count: u64,
}

impl BandSpill {
    fn new(nbands: usize, path_for: impl FnMut(usize) -> PathBuf) -> Result<Self> {
        Self::with_block(nbands, BUCKET_BLOCK, path_for)
    }

    /// [`BandSpill::new`] with an explicit block size. Every band holds one partially
    /// filled block resident, so `nbands × block_bytes` is a floor on the spill's
    /// footprint: `cluster` routes into 1,398 stripes and must not pay 1 MiB apiece.
    pub(crate) fn with_block(
        nbands: usize,
        block_bytes: usize,
        mut path_for: impl FnMut(usize) -> PathBuf,
    ) -> Result<Self> {
        let mut writers = Vec::with_capacity(nbands);
        let mut paths = Vec::with_capacity(nbands);
        for b in 0..nbands {
            let p = path_for(b);
            let w = BlockFileWriter::create(&p, block_bytes, SCRATCH_ZSTD)?;
            writers.push(Mutex::new(BandWriter { w, count: 0 }));
            paths.push(p);
        }
        Ok(Self { writers, paths })
    }

    /// Finalize every band writer; returns `(paths, per-band record counts)`.
    pub(crate) fn finish(self) -> Result<(Vec<PathBuf>, Vec<u64>)> {
        let mut counts = Vec::with_capacity(self.writers.len());
        for m in self.writers {
            let bw = m.into_inner().unwrap();
            counts.push(bw.count);
            bw.w.finish()?;
        }
        Ok((self.paths, counts))
    }
}

/// Per-worker local batcher over a shared [`BandSpill`]. Accumulates each band's
/// records (length-prefixed in one contiguous buffer to avoid per-record allocation)
/// and flushes a band under its lock once its buffer crosses `threshold` bytes.
pub(crate) struct BandBatcher<'a> {
    set: &'a BandSpill,
    bufs: Vec<Vec<u8>>,
    threshold: usize,
    scratch: Vec<u8>,
}

impl<'a> BandBatcher<'a> {
    pub(crate) fn new(set: &'a BandSpill, threshold: usize) -> Self {
        let n = set.writers.len();
        Self {
            set,
            bufs: (0..n).map(|_| Vec::new()).collect(),
            threshold: threshold.max(1),
            scratch: Vec::new(),
        }
    }

    pub(crate) fn push<R: SortRecord>(&mut self, band: usize, rec: &R) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        let buf = &mut self.bufs[band];
        write_uvarint(buf, self.scratch.len() as u64);
        buf.extend_from_slice(&self.scratch);
        if buf.len() >= self.threshold {
            self.flush_band(band)?;
        }
        Ok(())
    }

    fn flush_band(&mut self, band: usize) -> Result<()> {
        if self.bufs[band].is_empty() {
            return Ok(());
        }
        let set = self.set;
        let buf = &mut self.bufs[band];
        let mut g = set.writers[band].lock().unwrap();
        let mut r: &[u8] = buf;
        while !r.is_empty() {
            let len = read_uvarint(&mut r)? as usize;
            let (rec, rest) = r.split_at(len);
            g.w.append_record(rec)?;
            g.count += 1;
            r = rest;
        }
        drop(g);
        buf.clear();
        Ok(())
    }

    pub(crate) fn flush_all(&mut self) -> Result<()> {
        for b in 0..self.bufs.len() {
            self.flush_band(b)?;
        }
        Ok(())
    }
}

/// A resolved edge range-index spec: which (reltype, property-key) ids to extract,
/// and the index slot to push the `(value, final_edge_id)` entry into. Module-scoped
/// so [`emit_forward_band`] can take a slice of them.
struct EdgeRangeSpec {
    idx: usize,
    reltype_id: Option<u32>,
    key_id: Option<u32>,
}

/// Path of a per-band emit scratch file (`kind` ∈ fwd_band / rev_route / csr_fwd /
/// csr_rev / eprops). Shared by the parallel workers and the serial stitch so the
/// names never drift.
fn band_path(dir: &Path, pid: u32, kind: &str, b: usize) -> PathBuf {
    dir.join(format!("topo_{kind}_{pid}_{b}"))
}

/// How many posting/range entries a forward worker batches before taking a global
/// sink lock — small enough that the lock is held only briefly.
const FWD_SINK_BATCH: usize = 8192;

/// Smallest reservation a range-index sorter is given. Range sorters are the
/// longest-lived of the emit-phase sorters but among the smallest consumers, so
/// they take a modest floor rather than [`MIN_SORT_BYTES`]: a graph declaring many
/// indexes must not starve the band workers, which is where the bytes actually go.
const RANGE_SORT_FLOOR: usize = 1 << 20;

/// Emit one forward node band `[lo, hi)`: sort the band's edges, assign
/// `final_edge_id = base + i`, write the band's forward CSR half and `edge_props`
/// slice, feed the global postings/edge-range sinks, and route each edge's reverse
/// record (by `final_dst` band) into `rev_spill` for the reverse phase.
#[allow(clippy::too_many_arguments)]
fn emit_forward_band(
    lo: u64,
    hi: u64,
    band: u64,
    base: u64,
    band_file: &Path,
    csr_out: &Path,
    eprops_out: &Path,
    edge_range: &[EdgeRangeSpec],
    posts: &PostingSinks,
    range_sorters: &Mutex<Vec<ExtSorter<RangeEntry>>>,
    rev_spill: &BandSpill,
    batch_threshold: usize,
    scratch_dir: &Path,
    pool: &Arc<MemoryBudget>,
    want: usize,
    saturated: bool,
    block_size: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
    diag: &crate::diag::BuildDiag,
) -> Result<()> {
    // Load + sort this band's edges by (final_src, final_dst, prov_edge_id). Blocks
    // here if every slice of the pool is already out on another band.
    //
    // When the band pool is saturated (more bands than workers, the graph-scale case)
    // this sorter spills inline: the shared spill pool can hand it no extra cores, and
    // splitting its reservation across `spill_threads()+1` in-flight buffers would
    // multiply its run count by that factor — the k-way merge holds one decompressed
    // block per run, and with 14 bands live that `#runs × RUN_BLOCK_BYTES` is what
    // dominates peak RSS. Run *compression* stays parallel either way: `BlockFileWriter`
    // seals on its own pool. On a small graph with fewer bands than workers, inline
    // spilling would instead leave most cores idle — hence `saturated`.
    let mut sorter = ExtSorter::<EdgeFwd>::new_for_pool(
        scratch_dir,
        pool.reserve("forward band sorter", want, MIN_SORT_BYTES)?,
        SCRATCH_ZSTD,
        saturated,
    )?;
    {
        let r = BlockFileReader::open(band_file)?;
        r.for_each_record(|_, rec| {
            let mut s = rec;
            sorter.push(EdgeFwd::decode(&mut s)?)
        })?;
    }

    let mut csr = CsrHalfWriter::create_with_cipher(
        csr_out,
        lo,
        hi,
        true,
        block_size,
        zstd_level,
        cipher.clone(),
    )?;
    let mut eprops =
        BlockFileWriter::create_with_cipher(eprops_out, block_size, zstd_level, cipher)?;
    let mut rev_batch = BandBatcher::new(rev_spill, batch_threshold);
    // Only the sorter sink batches; the plane sink writes a bit per edge in place.
    let batch_cap = match posts {
        PostingSinks::Sorters { .. } => FWD_SINK_BATCH,
        PostingSinks::Planes { .. } => 0,
    };
    let mut src_batch: Vec<RelEndpoint> = Vec::with_capacity(batch_cap);
    let mut tgt_batch: Vec<RelEndpoint> = Vec::with_capacity(batch_cap);
    let mut range_batch: Vec<(usize, RangeEntry)> = Vec::new();

    let flush_posts =
        |src_batch: &mut Vec<RelEndpoint>, tgt_batch: &mut Vec<RelEndpoint>| -> Result<()> {
            let PostingSinks::Sorters { src, tgt } = posts else {
                return Ok(());
            };
            {
                let mut g = src.lock().unwrap();
                for r in src_batch.drain(..) {
                    g.push(r)?;
                }
            }
            let mut g = tgt.lock().unwrap();
            for r in tgt_batch.drain(..) {
                g.push(r)?;
            }
            Ok(())
        };

    let mut i = 0u64;
    for r in sorter.sorted()? {
        let ef = r?;
        let final_edge_id = base + i;
        i += 1;
        csr.push(
            ef.final_src,
            Adj {
                reltype: ef.reltype,
                neighbour: NodeId(ef.final_dst),
                edge: EdgeId(final_edge_id),
            },
        )?;
        eprops.append_record(&ef.props_blob)?;
        match posts {
            // `final_src` is inside this band, so the source plane's words are ours
            // alone; `final_dst` can land in any band, hence the atomic in `set`.
            PostingSinks::Planes { src, tgt, .. } => {
                src.set(ef.reltype, ef.final_src);
                tgt.set(ef.reltype, ef.final_dst);
            }
            PostingSinks::Sorters { .. } => {
                src_batch.push(RelEndpoint {
                    reltype: ef.reltype,
                    node: ef.final_src,
                });
                tgt_batch.push(RelEndpoint {
                    reltype: ef.reltype,
                    node: ef.final_dst,
                });
            }
        }
        for spec in edge_range {
            if let (Some(rid), Some(kid)) = (spec.reltype_id, spec.key_id) {
                if ef.reltype == rid {
                    if let Some(v) = extract_value(&ef.props_blob, kid)? {
                        range_batch.push((
                            spec.idx,
                            RangeEntry {
                                key: v,
                                id: final_edge_id,
                            },
                        ));
                    }
                }
            }
        }
        rev_batch.push(
            (ef.final_dst / band) as usize,
            &EdgeRev {
                final_dst: ef.final_dst,
                final_edge_id,
                final_src: ef.final_src,
                reltype: ef.reltype,
            },
        )?;
        if src_batch.len() >= FWD_SINK_BATCH {
            flush_posts(&mut src_batch, &mut tgt_batch)?;
        }
    }
    flush_posts(&mut src_batch, &mut tgt_batch)?;
    rev_batch.flush_all()?;
    if !range_batch.is_empty() {
        let mut g = range_sorters.lock().unwrap();
        for (idx, re) in range_batch {
            g[idx].push(re)?;
        }
    }
    csr.finish()?;
    eprops.finish()?;
    diag.progress_add(i);
    Ok(())
}

/// Emit one reverse node band `[lo, hi)`: sort the band's routed reverse records by
/// (final_dst, final_edge_id) and write the reverse CSR half for those nodes.
#[allow(clippy::too_many_arguments)]
fn emit_reverse_band(
    lo: u64,
    hi: u64,
    route_file: &Path,
    csr_out: &Path,
    scratch_dir: &Path,
    pool: &Arc<MemoryBudget>,
    want: usize,
    saturated: bool,
    block_size: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
    diag: &crate::diag::BuildDiag,
) -> Result<()> {
    // Inline vs pooled spill for the same reason as the forward band above.
    let mut sorter = ExtSorter::<EdgeRev>::new_for_pool(
        scratch_dir,
        pool.reserve("reverse band sorter", want, MIN_SORT_BYTES)?,
        SCRATCH_ZSTD,
        saturated,
    )?;
    {
        let r = BlockFileReader::open(route_file)?;
        r.for_each_record(|_, rec| {
            let mut s = rec;
            sorter.push(EdgeRev::decode(&mut s)?)
        })?;
    }
    let mut csr =
        CsrHalfWriter::create_with_cipher(csr_out, lo, hi, false, block_size, zstd_level, cipher)?;
    let mut i = 0u64;
    for r in sorter.sorted()? {
        let er = r?;
        csr.push(
            er.final_dst,
            Adj {
                reltype: er.reltype,
                neighbour: NodeId(er.final_src),
                edge: EdgeId(er.final_edge_id),
            },
        )?;
        i += 1;
    }
    csr.finish()?;
    diag.progress_add(i);
    Ok(())
}

/// Whole-graph metadata summaries tallied in a post-emit pass over the finished
/// topology + node labels. Persisted in the manifest so `open` need not rescan and
/// the whole-graph label/reltype fast paths answer from resident metadata. Vectors
/// are index-aligned with `reltypes` / `labels`; marginals are sparse `(key…, count)`
/// tuples sorted by key for deterministic emit.
struct GraphSummaries {
    reltype_edge_counts: Vec<u64>,
    reltype_self_loop_counts: Vec<u64>,
    label_node_counts: Vec<u64>,
    first_label_counts: Vec<u64>,
    src_label_reltype_counts: Vec<(u32, u32, u64)>,
    reltype_tgt_label_counts: Vec<(u32, u32, u64)>,
    schema_triple_counts: Vec<(u32, u32, u32, u64)>,
    /// Ascending-by-id `(node_id, degree)` for every node whose out/in degree is
    /// `>= hub_degree_floor` — the `hub_degrees.blk` sidecar. Deterministic: each
    /// worker owns a disjoint ascending id range and the concatenation is re-sorted.
    out_hub_degrees: Vec<(u64, u32)>,
    in_hub_degrees: Vec<(u64, u32)>,
    /// Dense per-node out/in degree (degree of dense id `i` at index `i`) — the
    /// `node_degrees.blk` column. Assembled in id order (each worker owns a contiguous
    /// ascending range; the concatenation is dense and ordered).
    out_degrees: Vec<u32>,
    in_degrees: Vec<u32>,
}

/// Compute [`GraphSummaries`] over the finished stores with **no resident node→label
/// map** — labels are read block-sequentially, never a whole-graph table.
///
/// Every accumulator here is a sum over nodes, so the whole computation is a
/// map-reduce over disjoint, contiguous node ranges. Contiguity is what makes it
/// cheap: each worker's ranges of `topology.csr.blk` and `node_labels.blk` are
/// ascending, so a two-block cache always hits and each block is decompressed
/// exactly once (see [`BlockFileReader::for_each_record_in`]).
///
/// **Map (over source-node ranges).** The forward half treats each node as the
/// *source* of its outgoing edges (per-reltype edge + self-loop counts, the
/// `(src_label, reltype)` marginal, per-label first/occurrence tallies) and routes a
/// `(dst, src_label, reltype)` record per edge×source-label into the dst-range it
/// belongs to. The reverse half treats each node as the *target* of its incoming
/// edges (the `(reltype, tgt_label)` marginal).
///
/// **Join (over target-node ranges).** The full `(src_label, reltype, tgt_label)`
/// cube needs both endpoints' labels on each edge, and the map half only ever held
/// the source's. Routing by `dst` means every record a range needs is in that
/// range's one spill file: worker `j` sorts its file by `dst` and merge-joins it
/// against an ascending walk of `node_labels[lo_j..hi_j]`, which supplies the target
/// labels. A bounded external sort-merge join, and one per range rather than one
/// globally — so the join parallelises with the tally instead of serialising after it.
///
/// **Reduce.** Addition over `u64` is commutative and associative, and the per-range
/// cubes are disjoint in `dst` but not in the triple key, so they are summed. The
/// emitted vectors are index-aligned and the marginals are sorted, so the output does
/// not depend on the worker count. `emit_determinism.rs` is the gate on that.
#[allow(clippy::too_many_arguments)]
fn compute_graph_summaries(
    topo_path: &Path,
    labels_path: &Path,
    node_count: u64,
    n_reltypes: usize,
    n_labels: usize,
    hub_degree_floor: u32,
    cipher: Option<Arc<BlockCipher>>,
    scratch_dir: &Path,
    mem: &Arc<MemoryBudget>,
    threads: usize,
    diag: &crate::diag::BuildDiag,
) -> Result<GraphSummaries> {
    use std::collections::HashMap;

    /// The `(src_label, reltype, tgt_label)` cube one dst-range worker tallied.
    type Cube = HashMap<(u32, u32, u32), u64>;

    let empty = GraphSummaries {
        reltype_edge_counts: vec![0; n_reltypes],
        reltype_self_loop_counts: vec![0; n_reltypes],
        label_node_counts: vec![0; n_labels],
        first_label_counts: vec![0; n_labels],
        src_label_reltype_counts: Vec::new(),
        reltype_tgt_label_counts: Vec::new(),
        schema_triple_counts: Vec::new(),
        out_hub_degrees: Vec::new(),
        in_hub_degrees: Vec::new(),
        out_degrees: Vec::new(),
        in_degrees: Vec::new(),
    };
    if node_count == 0 {
        return Ok(empty);
    }
    // `>=` floor lists a node; a `floor` of 0 would list every node (allowed, discouraged).
    let hub_floor = hub_degree_floor as u64;

    let topo = TopologyReader::open_with_cipher(topo_path, cipher.clone())?;
    let labels = NodeLabelsReader::open_with_cipher(labels_path, cipher)?;

    // Contiguous node ranges, one per worker. `chunk` also defines the dst-routing
    // bands, so a triple's band is `dst / chunk` and range `j` owns exactly the
    // triples whose target labels it will read.
    let nthreads = threads.max(1);
    let chunk = node_count.div_ceil(nthreads as u64).max(1);
    let nranges = node_count.div_ceil(chunk) as usize;
    let range_of = |j: usize| -> (u64, u64) {
        let lo = (j as u64) * chunk;
        (lo, (lo + chunk).min(node_count))
    };

    // A tiny windowed block cache per worker, rather than a per-node
    // `labels.labels(id)` call (which re-decompresses a node's whole block on *every*
    // call — at 91.6M nodes, empirically ~30% of the phase's instructions, confirmed
    // via callgrind on a 1M-node sample, for barely 1% of the work done) or a full
    // flat table materialising every node's labels up front (O(node_count) resident —
    // unbounded as the schema grows wider). Each worker visits node ids strictly
    // ascending within its own range, matching `node_labels.blk`'s on-disk order, so a
    // cache sized for a couple of blocks always hits until the scan crosses into the
    // next one.
    const LABEL_CACHE_BYTES: usize = 4 << 20;

    let pid = std::process::id();
    let triple_spill = BandSpill::new(nranges, |j| band_path(scratch_dir, pid, "summary_dst", j))?;
    let batch_threshold = (mem.total() / 32 / (nranges * nranges).max(1)).clamp(16 << 10, 1 << 20);

    // ---- map: tally over source-node ranges, route triples by dst ----
    diag.set_op("tally label/reltype summaries", "nodes", node_count);
    diag.set_active_workers(nranges as u64);
    struct Tally {
        reltype_edge: Vec<u64>,
        reltype_self: Vec<u64>,
        label_node: Vec<u64>,
        first_label: Vec<u64>,
        src_marg: HashMap<(u32, u32), u64>,
        tgt_marg: HashMap<(u32, u32), u64>,
        // Hub-degree sidecar: this worker's range's nodes with out/in degree >= floor,
        // ascending by id (the range is scanned in id order in both halves).
        out_hubs: Vec<(u64, u32)>,
        in_hubs: Vec<(u64, u32)>,
        // Dense per-node out/in degree for this worker's contiguous id range, in id order.
        out_degrees: Vec<u32>,
        in_degrees: Vec<u32>,
    }

    let tallies: Vec<Result<Tally>> = {
        let (topo_r, labels_r, spill_r) = (&topo, &labels, &triple_spill);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..nranges)
                .map(|j| {
                    scope.spawn(move || -> Result<Tally> {
                        let (lo, hi) = range_of(j);
                        let mut t = Tally {
                            reltype_edge: vec![0u64; n_reltypes],
                            reltype_self: vec![0u64; n_reltypes],
                            label_node: vec![0u64; n_labels],
                            first_label: vec![0u64; n_labels],
                            src_marg: HashMap::new(),
                            tgt_marg: HashMap::new(),
                            out_hubs: Vec::new(),
                            in_hubs: Vec::new(),
                            out_degrees: Vec::new(),
                            in_degrees: Vec::new(),
                        };
                        let cache = graph_format::blockcache::BlockCache::new(LABEL_CACHE_BYTES);
                        let labels_bitmask = labels_r.bitmask();
                        let labels_of = |id: u64| -> Result<Vec<u32>> {
                            let rec = cache.record(labels_r.inner(), 0, 0, id)?;
                            graph_format::nodelabels::decode_labels(&rec, labels_bitmask)
                        };
                        let mut batcher = BandBatcher::new(spill_r, batch_threshold);

                        // Forward: `global` is the source of each outgoing edge. Every
                        // node id in the graph falls in exactly one range's forward
                        // half, so the label tallies below count each node once.
                        topo_r.inner().for_each_record_in(lo, hi, |id, rec| {
                            let adjs = graph_format::topology::decode_adj(rec, true)?;
                            // Out-degree hub: `adjs.len()` is the out-degree; the scan is
                            // ascending in id, so `out_hubs` stays sorted.
                            if adjs.len() as u64 >= hub_floor {
                                t.out_hubs.push((id, adjs.len() as u32));
                            }
                            // Dense per-node out-degree (ascending id ⇒ in order).
                            t.out_degrees.push(adjs.len() as u32);
                            let labs = labels_of(id)?;
                            if let Some(&f) = labs.first() {
                                t.first_label[f as usize] += 1;
                            }
                            for &l in &labs {
                                t.label_node[l as usize] += 1;
                            }
                            for adj in &adjs {
                                let r = adj.reltype;
                                t.reltype_edge[r as usize] += 1;
                                if adj.neighbour.0 == id {
                                    t.reltype_self[r as usize] += 1;
                                }
                                for &a in &labs {
                                    *t.src_marg.entry((a, r)).or_insert(0) += 1;
                                    batcher.push(
                                        (adj.neighbour.0 / chunk) as usize,
                                        &TripleSpill {
                                            dst: adj.neighbour.0,
                                            src_label: a,
                                            reltype: r,
                                        },
                                    )?;
                                }
                            }
                            Ok(())
                        })?;

                        // Reverse: the reverse adjacency shares the topology file at
                        // `node_count + id` (see `topology.rs`), so the same node range
                        // is a second contiguous record range. `id` is the target of
                        // each incoming edge.
                        topo_r.inner().for_each_record_in(
                            node_count + lo,
                            node_count + hi,
                            |g, rec| {
                                let adjs = graph_format::topology::decode_adj(rec, false)?;
                                let id = g - node_count;
                                // Dense per-node in-degree (ascending id ⇒ in order).
                                t.in_degrees.push(adjs.len() as u32);
                                // In-degree hub: reverse adjacency length; ascending in id.
                                if adjs.len() as u64 >= hub_floor {
                                    t.in_hubs.push((id, adjs.len() as u32));
                                }
                                let labs = labels_of(id)?;
                                for adj in &adjs {
                                    for &b in &labs {
                                        *t.tgt_marg.entry((adj.reltype, b)).or_insert(0) += 1;
                                    }
                                }
                                Ok(())
                            },
                        )?;

                        batcher.flush_all()?;
                        diag.progress_add(hi - lo);
                        Ok(t)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };

    let mut reltype_edge = vec![0u64; n_reltypes];
    let mut reltype_self = vec![0u64; n_reltypes];
    let mut label_node = vec![0u64; n_labels];
    let mut first_label = vec![0u64; n_labels];
    let mut src_marg: HashMap<(u32, u32), u64> = HashMap::new();
    let mut tgt_marg: HashMap<(u32, u32), u64> = HashMap::new();
    let mut out_hub_degrees: Vec<(u64, u32)> = Vec::new();
    let mut in_hub_degrees: Vec<(u64, u32)> = Vec::new();
    let mut out_degrees: Vec<u32> = Vec::with_capacity(node_count as usize);
    let mut in_degrees: Vec<u32> = Vec::with_capacity(node_count as usize);
    for t in tallies {
        let t = t?;
        out_hub_degrees.extend(t.out_hubs);
        in_hub_degrees.extend(t.in_hubs);
        // Workers own ascending, contiguous, disjoint ranges and `tallies` is in range
        // order, so concatenating yields the dense degree columns in dense-id order.
        out_degrees.extend(t.out_degrees);
        in_degrees.extend(t.in_degrees);
        for (acc, v) in reltype_edge.iter_mut().zip(&t.reltype_edge) {
            *acc += v;
        }
        for (acc, v) in reltype_self.iter_mut().zip(&t.reltype_self) {
            *acc += v;
        }
        for (acc, v) in label_node.iter_mut().zip(&t.label_node) {
            *acc += v;
        }
        for (acc, v) in first_label.iter_mut().zip(&t.first_label) {
            *acc += v;
        }
        for (k, v) in t.src_marg {
            *src_marg.entry(k).or_insert(0) += v;
        }
        for (k, v) in t.tgt_marg {
            *tgt_marg.entry(k).or_insert(0) += v;
        }
    }
    let (spill_paths, _counts) = triple_spill.finish()?;

    // ---- join: resolve each dst range's target labels, tally the cube ----
    diag.set_op("join schema cube by target label", "nodes", node_count);
    diag.set_active_workers(nranges as u64);
    let pool = mem
        .reserve_now("summary cube pool", mem.available(), MIN_SORT_BYTES)?
        .into_sub_budget();
    let want = (pool.total() / nranges).max(MIN_SORT_BYTES);

    let cubes: Vec<Result<Cube>> = {
        let (labels_r, pool_r, paths_r) = (&labels, &pool, &spill_paths);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..nranges)
                .map(|j| {
                    scope.spawn(move || -> Result<Cube> {
                        let (lo, hi) = range_of(j);
                        let mut sorter = ExtSorter::<TripleSpill>::new_inline(
                            scratch_dir,
                            pool_r.reserve("summary triple sorter", want, MIN_SORT_BYTES)?,
                            SCRATCH_ZSTD,
                        )?;
                        BlockFileReader::open(&paths_r[j])?.for_each_record(|_, rec| {
                            let mut s = rec;
                            sorter.push(TripleSpill::decode(&mut s)?)
                        })?;

                        // Both sides ascend in `dst` / node id, so one linear pass
                        // resolves every record's target labels; each label block is
                        // decompressed once. Every routed `dst` lies in `[lo, hi)`, so
                        // the walk reaches it.
                        let mut cube: Cube = HashMap::new();
                        let mut sorted = sorter.sorted()?;
                        let mut pending: Option<TripleSpill> = sorted.next().transpose()?;
                        labels_r
                            .inner()
                            .for_each_record_in(lo, hi, |node_id, rec| {
                                if pending.as_ref().is_some_and(|p| p.dst == node_id) {
                                    let tgt_labs = graph_format::nodelabels::decode_labels(
                                        rec,
                                        labels_r.bitmask(),
                                    )?;
                                    while let Some(p) = pending.as_ref() {
                                        if p.dst != node_id {
                                            break;
                                        }
                                        for &b in &tgt_labs {
                                            *cube
                                                .entry((p.src_label, p.reltype, b))
                                                .or_insert(0) += 1;
                                        }
                                        pending = sorted.next().transpose()?;
                                    }
                                }
                                Ok(())
                            })?;
                        if let Some(p) = pending {
                            bail!(
                                "internal: summary triple for dst {} was routed to the \
                                 node range [{lo}, {hi}) that does not contain it",
                                p.dst
                            );
                        }
                        diag.progress_add(hi - lo);
                        Ok(cube)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };
    for p in &spill_paths {
        let _ = std::fs::remove_file(p);
    }

    let mut cube: Cube = HashMap::new();
    for c in cubes {
        for (k, v) in c? {
            *cube.entry(k).or_insert(0) += v;
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

    // Workers own disjoint ascending id ranges, so the concatenation is already sorted;
    // sort anyway to make the sidecar independent of worker count/scheduling (the
    // `emit_determinism` guarantee). Node ids are unique per direction ⇒ no dedup needed.
    out_hub_degrees.sort_unstable();
    in_hub_degrees.sort_unstable();

    Ok(GraphSummaries {
        reltype_edge_counts: reltype_edge,
        reltype_self_loop_counts: reltype_self,
        label_node_counts: label_node,
        first_label_counts: first_label,
        src_label_reltype_counts,
        reltype_tgt_label_counts,
        schema_triple_counts,
        out_hub_degrees,
        in_hub_degrees,
        out_degrees,
        in_degrees,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::manifest::{AnnMode, Manifest};
    use graph_format::pq::{AdcTable, PqReader};
    use graph_format::vamana::{beam_search, VamanaReader};

    /// A deterministic LCG so the synthetic dump is reproducible without a `rand`
    /// dependency (mirrors graph-format's training RNG).
    struct Lcg(u64);
    impl Lcg {
        fn next_f64(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    fn unit(v: &[f32]) -> Vec<f32> {
        let n: f64 = v
            .iter()
            .map(|&x| (x as f64) * (x as f64))
            .sum::<f64>()
            .sqrt();
        v.iter().map(|&x| (x as f64 / n) as f32).collect()
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f64;
        let (mut na, mut nb) = (0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            dot += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        (1.0 - dot / (na.sqrt() * nb.sqrt())) as f32
    }

    /// Build a dump script of `n` nodes each carrying a `dim`-dim `vecf32`
    /// embedding, plus a cosine vector index over `(:Doc, embedding)`. Returns the
    /// script and the raw (un-normalised) vectors for ground-truth checks.
    fn synthetic_dump(n: usize, dim: usize) -> (String, Vec<Vec<f32>>) {
        let mut rng = Lcg(0xDEAD_BEEF_1234);
        let mut script = String::new();
        script.push_str("CALL db.idx.vector.createNodeIndex('Doc', 'embedding', ");
        script.push_str(&format!("{dim}, 'cosine');\n"));
        let mut vectors = Vec::with_capacity(n);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
            let body: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
            script.push_str(&format!(
                "CREATE (:Doc:__DumpVertex__ {{__dump_id__: {i}, embedding: vecf32([{}])}});\n",
                body.join(", ")
            ));
            vectors.push(v);
        }
        (script, vectors)
    }

    /// Run the external build over `script` in a fresh temp dir. `--cluster none`
    /// keeps dump order, so the dense node id of dump node `i` is exactly `i` — the
    /// recall check below maps Vamana hits back to dump indices on that basis. The
    /// caller tweaks `opts` (threshold / Vamana / PQ knobs / publish store).
    fn run_build(tag: &str, script: &str, tweak: impl FnOnce(&mut BuildOptions)) -> BuildOutcome {
        let work = std::env::temp_dir().join(format!("slater_be_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&work);
        std::fs::create_dir_all(&work).unwrap();
        let input = work.join("dump.cypher");
        std::fs::write(&input, script).unwrap();
        let data_dir = work.join("data");

        let mut opts = BuildOptions {
            // The synthetic dumps use the `__dump_id__`/CREATE shape.
            pk: Some("__dump_id__".into()),
            cluster: crate::cluster::ClusterMode::None,
            ..Default::default()
        };
        tweak(&mut opts);
        build_external(
            input.to_str().unwrap(),
            "docs",
            &data_dir,
            &opts,
            &crate::diag::BuildDiag::disabled(),
        )
        .unwrap()
    }

    #[test]
    fn above_threshold_builds_vamana_and_pq_files_with_acceptable_recall() {
        let dim = 16;
        let n = 400;
        let (script, vectors) = synthetic_dump(n, dim);
        let outcome = run_build("vam", &script, |o| {
            // A low threshold forces the Vamana path on this small synthetic set.
            o.ann_threshold = 50;
            o.vamana_r = 24;
            o.vamana_alpha = 1.2;
            o.pq_subspaces = 8;
            o.pq_bits = 8;
        });

        // The descriptor records Vamana mode with the build parameters.
        let manifest = Manifest::read_from_dir(&outcome.dir).unwrap();
        assert_eq!(manifest.vector_indexes.len(), 1);
        let desc = &manifest.vector_indexes[0];
        assert_eq!(desc.count, n as u64);
        let (medoid, pqm) = match desc.mode {
            AnnMode::Vamana {
                r,
                medoid,
                pq_subspaces,
                ..
            } => {
                assert_eq!(r, 24);
                (medoid, pq_subspaces)
            }
            AnnMode::BruteForce => panic!("expected Vamana mode above the threshold"),
        };
        assert_eq!(pqm, 8);

        // The two ANN files were written and are in the manifest inventory.
        let vam_path = outcome.dir.join("vector/Doc.embedding.vamana");
        let pq_path = outcome.dir.join("vector/Doc.embedding.pq");
        assert!(vam_path.exists() && pq_path.exists());
        assert!(manifest
            .files
            .iter()
            .any(|f| f.name == "vector/Doc.embedding.vamana"));
        assert!(manifest
            .files
            .iter()
            .any(|f| f.name == "vector/Doc.embedding.pq"));

        // Read the ANN files back and run the same beam search the server will,
        // checking recall@k against brute-force ground truth.
        let vam = VamanaReader::open_with_cipher(&vam_path, None).unwrap();
        let pq = PqReader::open_with_cipher(&pq_path, None).unwrap();
        let resident = pq.load_resident().unwrap();
        assert_eq!(vam.len(), n as u64);
        assert_eq!(resident.len(), n);

        let k = 10;
        let queries = 15;
        let mut recall_sum = 0.0f64;
        for q in 0..queries {
            let query = unit(&vectors[(q * 23) % n]);
            let adc = AdcTable::new(&resident.codebook, &query).unwrap();
            let hits = beam_search(
                medoid as u32,
                64,
                k,
                n,
                |i| adc.estimate(resident.codes_of(i as usize)),
                |i| {
                    let node = vam.node(i).unwrap();
                    Ok((node.vector, node.neighbours))
                },
                |v| cosine_distance(&query, v),
            )
            .unwrap();
            // Map hits back to dense node ids and compare with brute force over the
            // original (raw) vectors. `--cluster none` ⇒ dense id == dump index.
            let got: std::collections::HashSet<u64> = hits
                .iter()
                .map(|h| vam.node(h.index).unwrap().node_id)
                .collect();
            let mut truth: Vec<(f32, u64)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (cosine_distance(&query, &unit(v)), i as u64))
                .collect();
            truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let found = truth
                .iter()
                .take(k)
                .filter(|(_, id)| got.contains(id))
                .count();
            recall_sum += found as f64 / k as f64;
        }
        let recall = recall_sum / queries as f64;
        assert!(recall >= 0.8, "build→read recall@{k} was {recall:.3}");
    }

    #[test]
    fn below_threshold_stays_brute_force() {
        let dim = 8;
        let n = 30;
        let (script, _) = synthetic_dump(n, dim);
        // Default threshold (50k) ⇒ this 30-vector index stays brute-force.
        let outcome = run_build("bf", &script, |_| {});
        let manifest = Manifest::read_from_dir(&outcome.dir).unwrap();
        assert!(matches!(
            manifest.vector_indexes[0].mode,
            AnnMode::BruteForce
        ));
        // No ANN files written for a brute-force index.
        assert!(!outcome.dir.join("vector/Doc.embedding.vamana").exists());
    }

    /// A dump with one hub node (`0`) linking out to `leaves` leaf nodes. `--cluster
    /// none` ⇒ dense id == dump id, so the hub is node 0 and leaf `k` is node `k`.
    fn hub_dump(leaves: usize) -> String {
        let mut s = String::new();
        s.push_str("CREATE (:Hub:__DumpVertex__ {__dump_id__: 0});\n");
        for k in 1..=leaves {
            s.push_str(&format!(
                "CREATE (:Leaf:__DumpVertex__ {{__dump_id__: {k}}});\n"
            ));
        }
        for k in 1..=leaves {
            s.push_str(&format!(
                "MATCH (a:__DumpVertex__ {{__dump_id__: 0}}), \
                 (b:__DumpVertex__ {{__dump_id__: {k}}}) CREATE (a)-[:LINK]->(b);\n"
            ));
        }
        s
    }

    /// Slice 3: the build emits a correct, deterministic `hub_degrees.blk` sidecar — the
    /// hub node's exact out-degree in the out-list, nothing in the in-list (leaf in-degree
    /// 1 < floor), the manifest descriptor records the floor + counts, and two independent
    /// builds produce byte-identical sidecars and the same content hash.
    #[test]
    fn hub_degree_sidecar_is_emitted_and_deterministic() {
        use graph_format::blockfile::BlockFileReader;
        use graph_format::hubdegree::decode_hub_list;

        let leaves = 10usize;
        let build = |tag: &str| {
            run_build(tag, &hub_dump(leaves), |o| {
                o.hub_degree_floor = 4; // hub out-degree 10 >= 4; leaf in-degree 1 < 4
            })
        };
        let a = build("hubdeg_a");
        let b = build("hubdeg_b");

        // Manifest descriptor: floor + list lengths (one out-hub, no in-hubs).
        let ma = Manifest::read_from_dir(&a.dir).unwrap();
        let desc = ma.hub_degrees.as_ref().expect("sidecar descriptor present");
        assert_eq!(desc.floor, 4);
        assert_eq!(desc.out_hubs, 1);
        assert_eq!(desc.in_hubs, 0);
        assert!(ma.files.iter().any(|f| f.name == "hub_degrees.blk"));

        // The sidecar records the hub's exact out-degree and nothing on the in side.
        let r = BlockFileReader::open(a.dir.join("hub_degrees.blk")).unwrap();
        assert_eq!(r.total_records(), 2);
        assert_eq!(
            decode_hub_list(&r.read_record_global(0).unwrap()).unwrap(),
            vec![(0u64, leaves as u32)]
        );
        assert!(decode_hub_list(&r.read_record_global(1).unwrap())
            .unwrap()
            .is_empty());

        // The dense per-node degree column is emitted and exact: the hub (node 0) has
        // `leaves` out-edges and 0 in; each leaf has 0 out and 1 in.
        let ndr = BlockFileReader::open(a.dir.join("node_degrees.blk")).unwrap();
        let (out_degs, in_degs) =
            graph_format::nodedegree::read_node_degrees(&ndr, leaves + 1).unwrap();
        assert_eq!(out_degs[0], leaves as u32);
        assert_eq!(in_degs[0], 0);
        for k in 1..=leaves {
            assert_eq!(out_degs[k], 0, "leaf {k} out-degree");
            assert_eq!(in_degs[k], 1, "leaf {k} in-degree");
        }

        // Determinism: byte-identical sidecar + column and equal content hash across builds.
        let bytes_a = std::fs::read(a.dir.join("hub_degrees.blk")).unwrap();
        let bytes_b = std::fs::read(b.dir.join("hub_degrees.blk")).unwrap();
        assert_eq!(bytes_a, bytes_b, "hub_degrees.blk must be deterministic");
        let deg_a = std::fs::read(a.dir.join("node_degrees.blk")).unwrap();
        let deg_b = std::fs::read(b.dir.join("node_degrees.blk")).unwrap();
        assert_eq!(deg_a, deg_b, "node_degrees.blk must be deterministic");
        assert_eq!(a.content_hash, b.content_hash, "content hash must match");

        let _ = std::fs::remove_dir_all(a.dir.parent().unwrap().parent().unwrap());
        let _ = std::fs::remove_dir_all(b.dir.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn publishes_finished_generation_to_object_store() {
        use graph_format::store::mem::MemObjectStore;
        use graph_format::store::{join_key, ObjectStore};

        let (script, _) = synthetic_dump(20, 8);
        let mem = Arc::new(MemObjectStore::new());
        let outcome = run_build("pub", &script, |o| {
            o.publish_store = Some(mem.clone() as Arc<dyn ObjectStore>);
        });

        let base = join_key("docs", &outcome.generation.0.to_string());
        // The current pointer was written to the store and names the built generation.
        let current =
            String::from_utf8(mem.read_all(&join_key("docs", "current")).unwrap()).unwrap();
        assert_eq!(current.trim(), outcome.generation.0.to_string());
        // The MANIFEST and a data file landed in the store with the right bytes.
        assert!(mem.exists(&join_key(&base, "MANIFEST.json")).unwrap());
        let np_key = join_key(&base, "node_props.blk");
        let from_store = mem.read_all(&np_key).unwrap();
        let from_disk = std::fs::read(outcome.dir.join("node_props.blk")).unwrap();
        assert_eq!(from_store, from_disk);
    }
}
