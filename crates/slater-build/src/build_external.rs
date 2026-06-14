// SPDX-License-Identifier: Apache-2.0
//! The external, bounded-memory build.
//!
//! Where [`crate::build`] holds the whole parsed graph in RAM, this path streams
//! the dump into on-disk buckets (pass 1), computes a locality-aware node-id
//! permutation under a memory cap (pass 2 / clustering), then emits the final
//! stores by external sort — so peak memory is independent of the edge count.
//! The published generation is byte-format-identical to the in-memory build's
//! (the server reads it unchanged); only record *order* (and thus the dense ids)
//! differs, which the MANIFEST does not constrain.
//!
//! All scratch lives under a per-generation directory **outside** the staged
//! generation (so the publish rename never drags 20+ GB of buckets into the
//! image), and is removed on success unless `--keep-temp`.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use graph_format::columns::{encode_props_record, PropsWriter};
use graph_format::extsort::{ExtSorter, SortRecord};
use graph_format::ids::{EdgeId, Generation, NodeId, Value};
use graph_format::isam::write_isam_sorted;
use graph_format::manifest::{EntityKind, RangeIndexDesc};
use graph_format::nodelabels::{encode_labels_record, NodeLabelsWriter};
use graph_format::topology::{Adj, CsrStreamWriter};
use graph_format::wire::{read_uvarint, read_value, skip_value, write_uvarint, write_value};

use crate::buckets::{self, read_blob, write_blob, BucketWriter, EdgeRec, NodeRec, UnresolvedEdge};
use crate::build::{parse_metric, write_vector_indexes, BuildOptions, Interner, PendingIndex};
use crate::cluster::{self, ClusterParams, Permutation};
use crate::common::{self, BuildOutcome, PublishInputs};
use crate::model::{Entity, RangeIndexStmt, Statement, VectorIndexStmt};
use crate::parser::{parse_statement, StatementReader};
use crate::resolve::{DumpResolver, NO_DUMP};

const DUMP_VERTEX: &str = "__DumpVertex__";
const DUMP_ID: &str = "__dump_id__";
/// Bigger blocks for the transient buckets — fewer, fatter blocks, all deleted at
/// the end of the build.
const BUCKET_BLOCK: usize = 1 << 20;
/// zstd level for transient scratch (buckets, spill runs, cluster adjacency). These
/// are deleted at the end of the build, so favour speed (level 1) over ratio — the
/// final published stores still use `--zstd-level`.
const SCRATCH_ZSTD: i32 = 1;
/// Pass 1 parses this many statements per rayon batch before applying them in order.
const PARSE_BATCH: usize = 8192;
/// Checkpoint file (in scratch) recording how far a build got, for `--resume`.
const STATE_FILE: &str = "BUILD-STATE.json";

/// The furthest phase a build has durably completed. Ordered so `>=` works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum Phase {
    /// Nothing durable yet.
    Start,
    /// Node + unresolved-edge buckets written; interners/counts captured.
    Pass1,
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
    /// Mid-pass-1 progress, present (with `phase == Start`) only while pass 1 is
    /// in flight and the input is a seekable file. Lets an interrupted pass 1 pick
    /// up from the last completed bucket segment rather than restarting.
    #[serde(default)]
    pass1: Option<Pass1Progress>,
}

/// A pass-1 checkpoint taken at a bucket-segment boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pass1Progress {
    /// Absolute input byte offset of the next statement to read.
    input_offset: u64,
    /// Nodes / unresolved edges written so far.
    node_count: u64,
    uedge_count: u64,
    /// Completed segment counts (the next segment to write is this index).
    node_segments: u64,
    uedge_segments: u64,
    /// Whether any node has been seen (for the vector-DDL-before-nodes check).
    seen_node: bool,
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

/// Remove bucket segments `>= from` (an interrupted run's incomplete trailing
/// segment, plus any stragglers) before resuming pass 1 into segment `from`.
fn cleanup_partial_segments(base: &Path, from: u64) {
    let mut n = from;
    while crate::buckets::seg_path(base, n).exists() {
        let _ = std::fs::remove_file(crate::buckets::seg_path(base, n));
        n += 1;
    }
}

/// Records per pass-1 bucket segment (a checkpoint boundary). Overridable via
/// `SLATER_PASS1_SEGMENT` for tests; defaults to 5M.
fn pass1_segment_records() -> u64 {
    std::env::var("SLATER_PASS1_SEGMENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000)
}

/// Statements parsed per rayon batch. Overridable via `SLATER_PARSE_BATCH` for
/// tests; defaults to [`PARSE_BATCH`].
fn pass1_parse_batch() -> usize {
    std::env::var("SLATER_PARSE_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(PARSE_BATCH)
}

/// Experimental: run pass 1 as a parallel reader→worker pool where each worker
/// parses *and applies* (interns, encodes, compresses, writes) into its **own**
/// bucket segment, out of original order. Gated by `SLATER_PARALLEL_PASS1=1`.
/// Sound because pass 2 re-keys everything by `__dump_id__` (provisional ids are
/// just the segment-concatenation order) and sorts — so write order is irrelevant
/// to the result. It is *not* deterministic/resumable (interner ids depend on
/// thread race order), so it falls back to serial when resuming.
fn pass1_parallel() -> bool {
    matches!(
        std::env::var("SLATER_PARALLEL_PASS1").as_deref(),
        Ok("1") | Ok("on") | Ok("true")
    )
}

fn pass1_workers() -> usize {
    std::env::var("SLATER_PASS1_WORKERS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
}

/// Block size the parallel reader hands to workers (default 8 MiB).
fn pass1_block_bytes() -> usize {
    std::env::var("SLATER_PASS1_BLOCK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 4096)
        .unwrap_or(8 << 20)
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

/// Outputs of pass 1, however it was run (serial or parallel).
struct Pass1Out {
    nodes: u64,
    uedges: u64,
    labels: Interner,
    reltypes: Interner,
    keys: Interner,
    rstmts: Vec<RangeIndexStmt>,
    vstmts: Vec<VectorIndexStmt>,
    seen_node: bool,
}

/// Parallel pass 1: a single reader thread splits the input into statement batches
/// and fans them to `nworkers`; each worker parses + applies into its own segment
/// (`<bucket>.<worker>`). Interners are shared behind a mutex with a per-worker
/// read-through cache (so the lock is hit only on first-sight of each distinct
/// label/key/reltype — negligible for low-cardinality schemas).
#[allow(clippy::too_many_arguments)]
fn run_pass1_parallel(
    input_path: &str,
    start_offset: u64,
    node_bkt: &Path,
    uedge_bkt: &Path,
    vec_index_set: std::collections::HashSet<(String, String)>,
    rstmts0: Vec<RangeIndexStmt>,
    vstmts0: Vec<VectorIndexStmt>,
    parse_batch: usize,
) -> Result<Pass1Out> {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    let nworkers = pass1_workers();
    let labels = Arc::new(Mutex::new(Interner::default()));
    let keys = Arc::new(Mutex::new(Interner::default()));
    let reltypes = Arc::new(Mutex::new(Interner::default()));
    let vec_index_set = Arc::new(vec_index_set);
    let rstmts = Arc::new(Mutex::new(rstmts0));
    let vstmts = Arc::new(Mutex::new(vstmts0));
    let seen_node = Arc::new(AtomicBool::new(false));
    let total_nodes = Arc::new(AtomicU64::new(0));
    let total_uedges = Arc::new(AtomicU64::new(0));

    let _ = parse_batch; // block-streaming sizes work by bytes, not statement count
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(nworkers * 2);
    let rx = Arc::new(Mutex::new(rx));

    // Reader thread: stream large raw byte blocks, cut each at the last complete
    // statement boundary, carry the partial tail to the next block, and hand whole
    // blocks (groups of complete statements) to workers. It does *no* per-statement
    // allocation and *no* parsing — that all moves onto the workers — so a single
    // sequential reader (stdin-safe, no seek) can feed every core. Dropping `tx` on
    // exit disconnects the channel so workers drain and stop.
    let reader_path = input_path.to_string();
    let block_bytes = pass1_block_bytes();
    let reader = std::thread::spawn(move || -> Result<()> {
        let mut r = open_input(&reader_path, start_offset)?;
        let mut carry: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; block_bytes];
        loop {
            // Fill up to a full block (BufRead can return short reads).
            let mut filled = 0usize;
            while filled < block_bytes {
                let n =
                    std::io::Read::read(&mut r, &mut chunk[filled..]).context("read dump block")?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                // EOF: flush the trailing partial statement(s); StatementReader's
                // own EOF handling emits a final statement with no terminating `;`.
                if !carry.is_empty() && tx.send(std::mem::take(&mut carry)).is_err() {
                    break;
                }
                break;
            }
            let mut work = std::mem::take(&mut carry);
            work.extend_from_slice(&chunk[..filled]);
            let cut = last_statement_end(&work);
            if cut == 0 {
                // No complete statement yet (a statement longer than one block);
                // keep accumulating.
                carry = work;
                continue;
            }
            carry = work[cut..].to_vec();
            work.truncate(cut);
            if tx.send(work).is_err() {
                break; // workers gone (an error elsewhere)
            }
        }
        Ok(())
    });

    let mut workers = Vec::with_capacity(nworkers);
    for w in 0..nworkers {
        let rx = rx.clone();
        let labels = labels.clone();
        let keys = keys.clone();
        let reltypes = reltypes.clone();
        let vec_index_set = vec_index_set.clone();
        let rstmts = rstmts.clone();
        let vstmts = vstmts.clone();
        let seen_node = seen_node.clone();
        let total_nodes = total_nodes.clone();
        let total_uedges = total_uedges.clone();
        let node_seg = buckets::seg_path(node_bkt, w as u64);
        let uedge_seg = buckets::seg_path(uedge_bkt, w as u64);
        workers.push(std::thread::spawn(move || -> Result<()> {
            let mut node_w = BucketWriter::create(node_seg, BUCKET_BLOCK, SCRATCH_ZSTD)?;
            let mut uedge_w = BucketWriter::create(uedge_seg, BUCKET_BLOCK, SCRATCH_ZSTD)?;
            let mut lcache: HashMap<String, u32> = HashMap::new();
            let mut kcache: HashMap<String, u32> = HashMap::new();
            let mut rcache: HashMap<String, u32> = HashMap::new();
            let mut scalar_props: Vec<(u32, Value)> = Vec::new();
            let mut lnodes = 0u64;
            let mut luedges = 0u64;
            loop {
                let msg = {
                    let g = rx.lock().expect("rx poisoned");
                    g.recv()
                };
                let block = match msg {
                    Ok(b) => b,
                    Err(_) => break,
                };
                // Split the block into statements here (in parallel) — the reader
                // never did this, which is what frees it to feed every worker.
                let mut sr = StatementReader::new(std::io::Cursor::new(block));
                while let Some(raw) = sr.next_statement()? {
                    let raw = raw.as_str();
                    let stmt = parse_statement(raw)
                        .with_context(|| format!("in statement: {}", truncate(raw, 120)))?;
                    match stmt {
                        Statement::Node(n) => {
                            seen_node.store(true, Ordering::Relaxed);
                            let mut label_names: Vec<&str> = Vec::new();
                            let mut label_ids = Vec::new();
                            for l in &n.labels {
                                if l != DUMP_VERTEX {
                                    label_names.push(l);
                                    let id = match lcache.get(l) {
                                        Some(&id) => id,
                                        None => {
                                            let id = labels.lock().unwrap().intern(l);
                                            lcache.insert(l.clone(), id);
                                            id
                                        }
                                    };
                                    label_ids.push(id);
                                }
                            }
                            scalar_props.clear();
                            let mut vec_props: Vec<(String, Vec<f32>)> = Vec::new();
                            let mut dump_id = NO_DUMP;
                            for (k, v) in n.props {
                                if k == DUMP_ID {
                                    match v {
                                        Value::Int(id) => dump_id = id,
                                        _ => bail!("__dump_id__ must be an integer"),
                                    }
                                    continue;
                                }
                                match v {
                                    Value::Vector(xs)
                                        if label_names.iter().any(|l| {
                                            vec_index_set.contains(&(l.to_string(), k.clone()))
                                        }) =>
                                    {
                                        vec_props.push((k, xs));
                                    }
                                    other => {
                                        let kid = match kcache.get(&k) {
                                            Some(&id) => id,
                                            None => {
                                                let id = keys.lock().unwrap().intern(&k);
                                                kcache.insert(k.clone(), id);
                                                id
                                            }
                                        };
                                        scalar_props.push((kid, other));
                                    }
                                }
                            }
                            let labels_blob = encode_labels_record(&label_ids);
                            let props_blob = encode_props_record(&scalar_props);
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
                            lnodes += 1;
                        }
                        Statement::Edge(e) => {
                            let reltype = match rcache.get(&e.reltype) {
                                Some(&id) => id,
                                None => {
                                    let id = reltypes.lock().unwrap().intern(&e.reltype);
                                    rcache.insert(e.reltype.clone(), id);
                                    id
                                }
                            };
                            scalar_props.clear();
                            for (k, v) in e.props {
                                let kid = match kcache.get(&k) {
                                    Some(&id) => id,
                                    None => {
                                        let id = keys.lock().unwrap().intern(&k);
                                        kcache.insert(k.clone(), id);
                                        id
                                    }
                                };
                                scalar_props.push((kid, v));
                            }
                            let props_blob = encode_props_record(&scalar_props);
                            uedge_w.append_unresolved_edge(&UnresolvedEdge {
                                src_dump: e.src_dump_id,
                                dst_dump: e.dst_dump_id,
                                reltype,
                                props_blob,
                            })?;
                            luedges += 1;
                        }
                        Statement::RangeIndex(r) => {
                            if r.label_or_type != DUMP_VERTEX && r.property != DUMP_ID {
                                rstmts.lock().unwrap().push(r);
                            }
                        }
                        Statement::VectorIndex(v) => {
                            // Ordering check is relaxed in parallel mode (it's a
                            // best-effort guard; sidecar declarations are the
                            // supported route for the parallel path).
                            let mut vs = vstmts.lock().unwrap();
                            if !vs
                                .iter()
                                .any(|e| e.label == v.label && e.property == v.property)
                            {
                                vs.push(v);
                            }
                        }
                        Statement::Ignored => {}
                    }
                }
            }
            node_w.finish()?;
            uedge_w.finish()?;
            total_nodes.fetch_add(lnodes, Ordering::Relaxed);
            total_uedges.fetch_add(luedges, Ordering::Relaxed);
            Ok(())
        }));
    }

    // Only the workers hold the receiver now; drop ours so that when the workers
    // finish (or die), the channel disconnects and the reader's `send` unblocks.
    drop(rx);
    let mut first_err: Option<anyhow::Error> = None;
    for h in workers {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(_) => {
                first_err.get_or_insert_with(|| anyhow::anyhow!("pass-1 worker panicked"));
            }
        }
    }
    let reader_res = reader.join();
    if let Some(e) = first_err {
        return Err(e);
    }
    reader_res.map_err(|_| anyhow::anyhow!("pass-1 reader panicked"))??;

    let unwrap_interner = |a: Arc<Mutex<Interner>>| {
        Arc::try_unwrap(a)
            .map_err(|_| anyhow::anyhow!("dangling interner ref"))
            .map(|m| m.into_inner().expect("interner poisoned"))
    };
    Ok(Pass1Out {
        nodes: total_nodes.load(Ordering::Relaxed),
        uedges: total_uedges.load(Ordering::Relaxed),
        labels: unwrap_interner(labels)?,
        reltypes: unwrap_interner(reltypes)?,
        keys: unwrap_interner(keys)?,
        rstmts: Arc::try_unwrap(rstmts)
            .map_err(|_| anyhow::anyhow!("dangling rstmts ref"))?
            .into_inner()
            .expect("rstmts poisoned"),
        vstmts: Arc::try_unwrap(vstmts)
            .map_err(|_| anyhow::anyhow!("dangling vstmts ref"))?
            .into_inner()
            .expect("vstmts poisoned"),
        seen_node: seen_node.load(Ordering::Relaxed),
    })
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

/// Which build path the binary selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ExternalMode {
    /// In-memory build ([`crate::build::build`]).
    Off,
    /// External bounded-memory build.
    On,
    /// External build (same as `On` today; reserved for a future size heuristic).
    Auto,
}

/// Build a generation with bounded memory. `input_path` is the dump script path,
/// or `-` for stdin (stdin cannot be sought, so mid-pass-1 resume needs a file).
/// See module docs.
pub fn build_external(
    input_path: &str,
    graph: &str,
    data_dir: &Path,
    opts: &BuildOptions,
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
) -> Result<BuildOutcome> {
    let (cipher, encryption_header) = common::derive_cipher(&opts.encryption_key);
    let node_bkt = scratch_dir.join("nodes.bkt");
    let uedge_bkt = scratch_dir.join("edges_unresolved.bkt");
    let edge_bkt = scratch_dir.join("edges.bkt");
    let perm_path = scratch_dir.join("perm.bin");
    let resume_phase = resume.as_ref().map(|s| s.phase).unwrap_or(Phase::Start);

    let mut labels;
    let mut reltypes;
    let mut keys;
    let range_stmts: Vec<RangeIndexStmt>;
    let vector_stmts: Vec<VectorIndexStmt>;
    let node_count: u64;

    // ---- pass 1: stream the dump into node + unresolved-edge buckets ----------
    if resume_phase >= Phase::Pass1 {
        let s = resume.as_ref().unwrap();
        labels = Interner::from_names(s.labels.clone());
        reltypes = Interner::from_names(s.reltypes.clone());
        keys = Interner::from_names(s.property_keys.clone());
        range_stmts = s.range_stmts.clone();
        vector_stmts = s.vector_stmts.clone();
        node_count = s.node_count;
    } else {
        // Fresh pass 1, or resume mid-pass-1 from the last completed bucket segment.
        let p1 = resume.as_ref().and_then(|s| s.pass1.clone());
        let mut rstmts;
        let mut vstmts: Vec<VectorIndexStmt>;
        let mut vec_index_set: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        if let (Some(s), Some(_)) = (resume.as_ref(), &p1) {
            // Restore interners + declarations captured by the pass-1 checkpoint.
            labels = Interner::from_names(s.labels.clone());
            reltypes = Interner::from_names(s.reltypes.clone());
            keys = Interner::from_names(s.property_keys.clone());
            rstmts = s.range_stmts.clone();
            vstmts = s.vector_stmts.clone();
            for v in &vstmts {
                vec_index_set.insert((v.label.clone(), v.property.clone()));
            }
        } else {
            labels = Interner::default();
            reltypes = Interner::default();
            keys = Interner::default();
            rstmts = Vec::new();
            vstmts = Vec::new();
            // Vector index declarations must be known before the nodes they cover so
            // an indexed `vecf32` is routed to the vector store. A sidecar is known
            // up front; inline declarations must precede node data (the dump tool's
            // output does this).
            if let Some(path) = &opts.vector_index_json {
                for v in crate::build::load_vector_sidecar(path)? {
                    if vec_index_set.insert((v.label.clone(), v.property.clone())) {
                        vstmts.push(v);
                    }
                }
            }
        }

        // Segment / offset state — resumed from the checkpoint, or fresh at zero.
        let (
            start_offset,
            mut node_seg,
            mut uedge_seg,
            mut total_nodes,
            mut total_uedges,
            mut seen_node,
        ) = match &p1 {
            Some(p) => (
                p.input_offset,
                p.node_segments,
                p.uedge_segments,
                p.node_count,
                p.uedge_count,
                p.seen_node,
            ),
            None => (0, 0, 0, 0, 0, false),
        };
        // Drop any incomplete trailing segment a crash left behind before appending.
        cleanup_partial_segments(&node_bkt, node_seg);
        cleanup_partial_segments(&uedge_bkt, uedge_seg);

        let seekable = input_path != "-";
        let seg_records = pass1_segment_records();
        let parse_batch = pass1_parse_batch();
        if pass1_parallel() && start_offset == 0 && node_seg == 0 && uedge_seg == 0 {
            // Experimental fully-parallel pass 1 (workers write their own segments,
            // out of order). Only on a fresh build — resume needs the serial,
            // deterministic path.
            let out = run_pass1_parallel(
                input_path,
                start_offset,
                &node_bkt,
                &uedge_bkt,
                vec_index_set,
                rstmts,
                vstmts,
                parse_batch,
            )?;
            total_nodes = out.nodes;
            total_uedges = out.uedges;
            labels = out.labels;
            reltypes = out.reltypes;
            keys = out.keys;
            rstmts = out.rstmts;
            vstmts = out.vstmts;
            // `seen_node` only gated the vector-DDL-before-nodes check during pass 1;
            // it is unused afterwards, so the parallel path doesn't propagate it.
            let _ = (out.seen_node, seekable, seg_records, node_seg, uedge_seg);
        } else {
            let reader = open_input(input_path, start_offset)?;
            let mut sreader = StatementReader::new(reader);
            let mut scalar_props: Vec<(u32, Value)> = Vec::new();
            let mut node_w = BucketWriter::create(
                buckets::seg_path(&node_bkt, node_seg),
                BUCKET_BLOCK,
                SCRATCH_ZSTD,
            )?;
            let mut uedge_w = BucketWriter::create(
                buckets::seg_path(&uedge_bkt, uedge_seg),
                BUCKET_BLOCK,
                SCRATCH_ZSTD,
            )?;
            let mut records_in_seg = 0u64;

            // Pass 1 is parse-bound: parse a batch in parallel (rayon), then apply the
            // results in order — interning + bucket writes stay sequential so
            // provisional-id assignment is deterministic.
            let mut batch: Vec<String> = Vec::with_capacity(parse_batch);
            loop {
                batch.clear();
                while batch.len() < parse_batch {
                    match sreader.next_statement()? {
                        Some(s) => batch.push(s),
                        None => break,
                    }
                }
                if batch.is_empty() {
                    break;
                }
                let parsed: Vec<Result<Statement>> =
                    batch.par_iter().map(|raw| parse_statement(raw)).collect();
                for (raw, parsed) in batch.iter().zip(parsed) {
                    let stmt =
                        parsed.with_context(|| format!("in statement: {}", truncate(raw, 120)))?;
                    match stmt {
                        Statement::Node(n) => {
                            seen_node = true;
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
                                if k == DUMP_ID {
                                    match v {
                                        Value::Int(id) => dump_id = id,
                                        _ => bail!("__dump_id__ must be an integer"),
                                    }
                                    continue;
                                }
                                match v {
                                    Value::Vector(xs)
                                        if label_names.iter().any(|l| {
                                            vec_index_set.contains(&(l.to_string(), k.clone()))
                                        }) =>
                                    {
                                        // Routed to the vector store (a declared index covers it).
                                        vec_props.push((k, xs));
                                    }
                                    other => {
                                        let kid = keys.intern(&k);
                                        scalar_props.push((kid, other));
                                    }
                                }
                            }
                            let labels_blob = encode_labels_record(&label_ids);
                            let props_blob = encode_props_record(&scalar_props);
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
                            total_nodes += 1;
                            records_in_seg += 1;
                        }
                        Statement::Edge(e) => {
                            let reltype = reltypes.intern(&e.reltype);
                            scalar_props.clear();
                            for (k, v) in e.props {
                                let kid = keys.intern(&k);
                                scalar_props.push((kid, v));
                            }
                            let props_blob = encode_props_record(&scalar_props);
                            uedge_w.append_unresolved_edge(&UnresolvedEdge {
                                src_dump: e.src_dump_id,
                                dst_dump: e.dst_dump_id,
                                reltype,
                                props_blob,
                            })?;
                            total_uedges += 1;
                            records_in_seg += 1;
                        }
                        Statement::RangeIndex(r) => {
                            if r.label_or_type != DUMP_VERTEX && r.property != DUMP_ID {
                                rstmts.push(r);
                            }
                        }
                        Statement::VectorIndex(v) => {
                            if seen_node {
                                bail!(
                                    "vector index declaration for {}.{} appears after node data; \
                                 the external build needs vector-index DDL before the nodes it \
                                 covers (the dump tool emits it first)",
                                    v.label,
                                    v.property
                                );
                            }
                            if vec_index_set.insert((v.label.clone(), v.property.clone())) {
                                vstmts.push(v);
                            }
                        }
                        Statement::Ignored => {}
                    }
                }
                // Roll a segment + checkpoint at the boundary (seekable inputs only — a
                // stdin pipe can't be resumed mid-stream regardless).
                if seekable && records_in_seg >= seg_records {
                    node_w.finish()?;
                    uedge_w.finish()?;
                    node_seg += 1;
                    uedge_seg += 1;
                    records_in_seg = 0;
                    checkpoint(
                        scratch_dir,
                        &BuildState {
                            generation: generation.0.to_string(),
                            phase: Phase::Start,
                            node_count: total_nodes,
                            edge_count: 0,
                            labels: labels.names().to_vec(),
                            reltypes: reltypes.names().to_vec(),
                            property_keys: keys.names().to_vec(),
                            range_stmts: rstmts.clone(),
                            vector_stmts: vstmts.clone(),
                            cluster_identity: false,
                            pass1: Some(Pass1Progress {
                                input_offset: start_offset + sreader.byte_offset(),
                                node_count: total_nodes,
                                uedge_count: total_uedges,
                                node_segments: node_seg,
                                uedge_segments: uedge_seg,
                                seen_node,
                            }),
                        },
                    )?;
                    fault_after("pass1_partial");
                    node_w = BucketWriter::create(
                        buckets::seg_path(&node_bkt, node_seg),
                        BUCKET_BLOCK,
                        SCRATCH_ZSTD,
                    )?;
                    uedge_w = BucketWriter::create(
                        buckets::seg_path(&uedge_bkt, uedge_seg),
                        BUCKET_BLOCK,
                        SCRATCH_ZSTD,
                    )?;
                }
            }
            node_w.finish()?;
            uedge_w.finish()?;
        }
        let _ = total_uedges;

        range_stmts = rstmts;
        vector_stmts = vstmts;
        node_count = total_nodes;
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
                pass1: None,
            },
        )?;
        fault_after("pass1");
    }

    // ---- resolve dump ids → provisional node ids, write the edge bucket -------
    let edge_count: u64;
    if resume_phase >= Phase::Resolved {
        edge_count = resume.as_ref().unwrap().edge_count;
    } else {
        // Rebuild the resolver by scanning the node bucket's dump ids (also the
        // resume-replay path), so nothing dump-id-scale stays resident from pass 1.
        let mut dump_ids: Vec<i64> = Vec::with_capacity(node_count as usize);
        buckets::for_each_node_dump_id(&node_bkt, |_, d| {
            dump_ids.push(d.unwrap_or(NO_DUMP));
            Ok(())
        })?;
        let resolver = DumpResolver::build_dense(&dump_ids, opts.max_memory_bytes)?;
        drop(dump_ids);

        let mut count = 0u64;
        {
            let mut edge_w = BucketWriter::create(&edge_bkt, BUCKET_BLOCK, SCRATCH_ZSTD)?;
            buckets::for_each_unresolved_edge(&uedge_bkt, |prov_edge, ue| {
                let src = resolver.get(ue.src_dump).with_context(|| {
                    format!("edge references unknown source __dump_id__ {}", ue.src_dump)
                })?;
                let dst = resolver.get(ue.dst_dump).with_context(|| {
                    format!("edge references unknown target __dump_id__ {}", ue.dst_dump)
                })?;
                edge_w.append_edge(&EdgeRec {
                    prov_edge_id: prov_edge,
                    src_prov: src,
                    dst_prov: dst,
                    reltype: ue.reltype,
                    props_blob: ue.props_blob,
                })?;
                count += 1;
                Ok(())
            })?;
            edge_w.finish()?;
        }
        std::fs::remove_file(&uedge_bkt).ok();
        edge_count = count;
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
                pass1: None,
            },
        )?;
        fault_after("resolve");
    }

    // ---- pass 2: clustering → node-id permutation -----------------------------
    let perm = if resume_phase >= Phase::Clustered {
        let s = resume.as_ref().unwrap();
        if s.cluster_identity {
            Permutation::Identity
        } else {
            load_perm(&perm_path, node_count)?
        }
    } else {
        let block_capacity = (opts.block_size / 48).max(1) as u32;
        let perm = cluster::build_permutation(
            node_count,
            &ClusterParams {
                mode: opts.cluster,
                passes: opts.cluster_passes,
                block_capacity,
                mem_budget: opts.max_memory_bytes,
                temp_dir: scratch_dir.to_path_buf(),
                zstd_level: SCRATCH_ZSTD,
            },
            |visit| buckets::for_each_edge(&edge_bkt, |e| visit(e.src_prov, e.dst_prov)),
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
                pass1: None,
            },
        )?;
        fault_after("cluster");
        perm
    };

    // ---- emit (always redone on resume) --------------------------------------
    let mut block_sizes: BTreeMap<String, u32> = BTreeMap::new();
    let sort_budget = (opts.max_memory_bytes / 16).max(16 * 1024 * 1024);

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
    struct EdgeRangeSpec {
        idx: usize,
        reltype_id: Option<u32>,
        key_id: Option<u32>,
    }
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
            sort_budget,
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
    let mut labels_w = NodeLabelsWriter::create_with_cipher(
        tmp_dir.join("node_labels.blk"),
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
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

    if perm.is_identity() {
        // Fast path: final id = prov id, so byte-copy straight through in order.
        buckets::for_each_node(&node_bkt, |prov, node| {
            labels_w.append_raw(&node.labels_blob)?;
            props_w.append_raw(&node.props_blob)?;
            emit_node_ranges(&node, prov, &mut range_sorters)?;
            gather_node_vectors(&node, prov, &vec_specs, &mut pending)?;
            Ok(())
        })?;
    } else {
        let mut node_sorter = ExtSorter::<NodeEmit>::new(scratch_dir, sort_budget, SCRATCH_ZSTD)?;
        buckets::for_each_node(&node_bkt, |prov, node| {
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
        for r in node_sorter.sorted()? {
            let ne = r?;
            labels_w.append_raw(&ne.labels_blob)?;
            props_w.append_raw(&ne.props_blob)?;
        }
    }
    props_w.finish()?;
    labels_w.finish()?;
    block_sizes.insert("node_props.blk".into(), opts.block_size as u32);
    block_sizes.insert("node_labels.blk".into(), opts.block_size as u32);

    // --- topology.csr.blk + edge_props.blk ---
    let mut edge_props_w = PropsWriter::create_with_cipher(
        tmp_dir.join("edge_props.blk"),
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    let mut csr = CsrStreamWriter::create_with_cipher(
        tmp_dir.join("topology.csr.blk"),
        node_count,
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;

    let mut fwd_sorter = ExtSorter::<EdgeFwd>::new(scratch_dir, sort_budget, SCRATCH_ZSTD)?;
    buckets::for_each_edge(&edge_bkt, |e| {
        fwd_sorter.push(EdgeFwd {
            final_src: perm.final_of(e.src_prov),
            final_dst: perm.final_of(e.dst_prov),
            prov_edge_id: e.prov_edge_id,
            reltype: e.reltype,
            props_blob: e.props_blob,
        })
    })?;

    let mut rev_sorter = ExtSorter::<EdgeRev>::new(scratch_dir, sort_budget, SCRATCH_ZSTD)?;
    for (final_edge_id, r) in fwd_sorter.sorted()?.enumerate() {
        let final_edge_id = final_edge_id as u64;
        let ef = r?;
        csr.push(
            ef.final_src,
            Adj {
                reltype: ef.reltype,
                neighbour: NodeId(ef.final_dst),
                edge: EdgeId(final_edge_id),
            },
        )?;
        edge_props_w.append_raw(&ef.props_blob)?;
        for spec in &edge_range {
            if let (Some(rid), Some(kid)) = (spec.reltype_id, spec.key_id) {
                if ef.reltype == rid {
                    if let Some(v) = extract_value(&ef.props_blob, kid)? {
                        range_sorters[spec.idx].push(RangeEntry {
                            key: v,
                            id: final_edge_id,
                        })?;
                    }
                }
            }
        }
        rev_sorter.push(EdgeRev {
            final_dst: ef.final_dst,
            final_edge_id,
            final_src: ef.final_src,
            reltype: ef.reltype,
        })?;
    }
    csr.finish_half()?; // forward records 0..N
    for r in rev_sorter.sorted()? {
        let er = r?;
        csr.push(
            er.final_dst,
            Adj {
                reltype: er.reltype,
                neighbour: NodeId(er.final_src),
                edge: EdgeId(er.final_edge_id),
            },
        )?;
    }
    csr.finish_half()?; // reverse records N..2N
    csr.finish()?;
    edge_props_w.finish()?;
    block_sizes.insert("edge_props.blk".into(), opts.block_size as u32);
    block_sizes.insert("topology.csr.blk".into(), opts.block_size as u32);

    // --- vectors.f32.blk + any Vamana/PQ files (shared with the in-memory build) ---
    let (vector_indexes, vector_files) =
        write_vector_indexes(tmp_dir, &pending, opts, cipher.clone(), &mut block_sizes)?;

    // --- range/*.isam (each fed its external-sorted stream) ---
    let mut range_indexes: Vec<RangeIndexDesc> = Vec::new();
    for (meta, sorter) in range_metas.into_iter().zip(range_sorters) {
        let rel_path = format!("range/{}.isam", meta.name);
        write_isam_sorted(
            tmp_dir.join(&rel_path),
            sorter.sorted()?.map(|r| r.map(|re| (re.key, re.id))),
            opts.block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        block_sizes.insert(rel_path, opts.block_size as u32);
        range_indexes.push(RangeIndexDesc {
            name: meta.name,
            entity: meta.entity,
            label_or_type: meta.label_or_type,
            property: meta.property,
        });
    }

    // ---- publish (shared with the in-memory build) ----
    common::write_manifest_and_publish(PublishInputs {
        tmp_dir,
        graph_dir,
        final_dir,
        generation,
        graph,
        zstd_level: opts.zstd_level,
        block_sizes,
        node_count,
        edge_count,
        labels: labels.into_names(),
        reltypes: reltypes.into_names(),
        property_keys: keys.into_names(),
        range_indexes,
        vector_indexes,
        encryption_header,
        encryption_key: &opts.encryption_key,
        acl_blake3: opts.acl_blake3.clone(),
        extra_files: vector_files,
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
    labels_blob: Vec<u8>,
    props_blob: Vec<u8>,
}
impl SortRecord for NodeEmit {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.final_id);
        write_blob(buf, &self.labels_blob);
        write_blob(buf, &self.props_blob);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let final_id = read_uvarint(r)?;
        let labels_blob = read_blob(r)?.to_vec();
        let props_blob = read_blob(r)?.to_vec();
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
    props_blob: Vec<u8>,
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
        let props_blob = read_blob(r)?.to_vec();
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
