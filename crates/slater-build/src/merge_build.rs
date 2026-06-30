// SPDX-License-Identifier: Apache-2.0
//! Business-key MERGE dumps (the default import; the `--pk <field>` path is the
//! single-global-key alternative): build a graph from scratch out of `MERGE` statements
//! whose node identity is a per-pattern business key, not a `__dump_id__`.
//!
//! The dump is entirely:
//!   * node MERGE   `MERGE (n:L {k:'v'}) [SET n.a = …, …]` — identity `(label, k, v)`;
//!   * edge MERGE   `MERGE (a:L {k:'v'})-[r:T]->(b:M {j:'w'}) [SET r.a = …]` — endpoints
//!     resolved by their business keys against the nodes built this run.
//!
//! Both forms already parse into [`crate::model::NodeOverwriteStmt`] /
//! [`crate::model::EdgeOverwriteStmt`]; this module is the bounded-memory *build path*
//! that treats them as the primary graph (vs. the in-memory overlay patch path).
//!
//! Two streaming phases, each built on [`ExtSorter`] so peak memory is independent of
//! the node/edge count:
//!   1. **node dedup** — collapse same-identity node MERGEs into one node, folding SET
//!      props last-writer-wins (in input order). Emits the deduped node bucket plus a
//!      `(identity → prov)` key stream, both in identity sort order.
//!   2. **edge resolve** — resolve each edge's two endpoints by business key via an
//!      external sort-merge-join against the node key stream, then collapse identical
//!      `(src, reltype, dst)` edges (last-wins) into the final edge bucket.
//!
//! Determinism: every id is assigned in a total-order sort (node prov in identity
//! order; edge id in `(src, reltype, dst, input-seq)` order), independent of worker
//! scheduling — matching the build's reproducible-output guarantee.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};

use graph_format::blockfile::{parse_block, BlockFileReader, BlockFileWriter};
use graph_format::columns::{decode_props, encode_props_record};
use graph_format::extsort::{ExtSorter, SortRecord};
use graph_format::ids::{BlockId, Value};
use graph_format::nodelabels::encode_labels_record;
use graph_format::wire::{read_uvarint, read_value, write_uvarint, write_value};

use crate::buckets::{
    read_blob, seg_path, segments, write_blob, BucketWriter, EdgeRec, NodeRec, ShardRemap,
};
use crate::model::SetExpr;
use crate::set_eval::validate_set_expr;

/// Bigger blocks for transient buckets, mirroring `build_external::BUCKET_BLOCK`.
const BUCKET_BLOCK: usize = 1 << 20;

/// Hash-partition count for the parallel resolve. **Fixed** (independent of
/// `--threads`) so the build output is byte-identical regardless of worker count — the
/// same determinism discipline emit.topology uses for its fixed node bands. Partitions
/// are processed on a `threads`-wide pool, so parallelism scales with cores up to this.
const RESOLVE_PARTS: usize = 64;

/// FNV-1a over a value's canonical wire encoding → a stable partition for the
/// business-key join. Type-exact (uses the same `write_value` bytes as equality).
fn part_of_value(v: &Value) -> usize {
    let mut buf = Vec::new();
    write_value(&mut buf, v);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &buf {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % RESOLVE_PARTS as u64) as usize
}

/// splitmix64 finalizer → a stable partition for an integer key (edge_seq).
fn part_of_u64(mut x: u64) -> usize {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    (x % RESOLVE_PARTS as u64) as usize
}

/// A set of `RESOLVE_PARTS` scratch bucket files written concurrently by many workers
/// — one mutex per partition, with per-worker batching ([`PartBatcher`]) so a worker
/// takes a partition's lock only on bulk flushes. Mirrors `build_external::BandSpill`,
/// keyed by hash partition instead of node band. Files are `<base>.<p>`, so
/// [`segments`] / [`seg_path`] address them like any segmented bucket.
struct PartSpill {
    writers: Vec<Mutex<(BlockFileWriter, u64)>>,
}

impl PartSpill {
    fn new(base: &Path, zstd: i32) -> Result<Self> {
        let mut writers = Vec::with_capacity(RESOLVE_PARTS);
        for p in 0..RESOLVE_PARTS {
            let w = BlockFileWriter::create(seg_path(base, p as u64), BUCKET_BLOCK, zstd)?;
            writers.push(Mutex::new((w, 0u64)));
        }
        Ok(Self { writers })
    }
    fn finish(self) -> Result<()> {
        for m in self.writers {
            m.into_inner().unwrap().0.finish()?;
        }
        Ok(())
    }
}

/// Per-worker local batcher over a shared [`PartSpill`] (length-prefixed records in one
/// buffer per partition; flush under the partition lock past `threshold` bytes).
struct PartBatcher<'a> {
    spill: &'a PartSpill,
    bufs: Vec<Vec<u8>>,
    threshold: usize,
    scratch: Vec<u8>,
}

impl<'a> PartBatcher<'a> {
    fn new(spill: &'a PartSpill, threshold: usize) -> Self {
        Self {
            spill,
            bufs: (0..RESOLVE_PARTS).map(|_| Vec::new()).collect(),
            threshold: threshold.max(1),
            scratch: Vec::new(),
        }
    }
    fn push<R: SortRecord>(&mut self, part: usize, rec: &R) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        let b = &mut self.bufs[part];
        write_uvarint(b, self.scratch.len() as u64);
        b.extend_from_slice(&self.scratch);
        if b.len() >= self.threshold {
            self.flush(part)?;
        }
        Ok(())
    }
    fn flush(&mut self, part: usize) -> Result<()> {
        if self.bufs[part].is_empty() {
            return Ok(());
        }
        let mut g = self.spill.writers[part].lock().unwrap();
        let mut r: &[u8] = &self.bufs[part];
        while !r.is_empty() {
            let len = read_uvarint(&mut r)? as usize;
            let (rec, rest) = r.split_at(len);
            g.0.append_record(rec)?;
            g.1 += 1;
            r = rest;
        }
        drop(g);
        self.bufs[part].clear();
        Ok(())
    }
    fn flush_all(&mut self) -> Result<()> {
        for p in 0..RESOLVE_PARTS {
            self.flush(p)?;
        }
        Ok(())
    }
}

/// Run `f(partition)` for each of the `RESOLVE_PARTS` partitions on a `threads`-wide
/// scoped pool, surfacing the first error. Deterministic output regardless of order
/// because every partition's work is independent and internally sorted.
fn par_partitions(threads: usize, f: impl Fn(usize) -> Result<()> + Sync) -> Result<()> {
    let next = AtomicU64::new(0);
    let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let (next_r, err_r, f_r) = (&next, &err, &f);
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(move || loop {
                if err_r.lock().unwrap().is_some() {
                    break;
                }
                let p = next_r.fetch_add(1, AtomicOrdering::Relaxed) as usize;
                if p >= RESOLVE_PARTS {
                    break;
                }
                if let Err(e) = f_r(p) {
                    let mut g = err_r.lock().unwrap();
                    if g.is_none() {
                        *g = Some(e);
                    }
                    break;
                }
            });
        }
    });
    match err.into_inner().unwrap() {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ── value ordering ───────────────────────────────────────────────────────────

/// Total, **type-exact** order over [`Value`], used as the business-key comparator.
/// Unlike [`Value::cmp_key`] (which coerces `Int`/`Float` to compare numerically for
/// range indexes), this keeps `Int` and `Float` in distinct rank bands: business keys
/// are identifiers, so `{id: 1}` must NOT resolve against `{id: 1.0}`. The within-type
/// order is deterministic (so prov-id assignment reproduces across runs).
pub(crate) fn value_cmp_exact(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    fn rank(v: &Value) -> u8 {
        match v {
            Null => 0,
            Bool(_) => 1,
            Int(_) => 2,
            Float(_) => 3,
            Str(_) => 4,
            List(_) => 5,
            Vector(_) => 6,
        }
    }
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.total_cmp(y),
        (Str(x), Str(y)) => x.cmp(y),
        (List(x), List(y)) => x
            .iter()
            .zip(y)
            .map(|(p, q)| value_cmp_exact(p, q))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or_else(|| x.len().cmp(&y.len())),
        (Vector(x), Vector(y)) => x
            .iter()
            .zip(y)
            .map(|(p, q)| p.total_cmp(q))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or_else(|| x.len().cmp(&y.len())),
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Fold one statement's `(key, value)` assignments onto an accumulating prop list,
/// per-key last-writer-wins (overrides an existing key or appends a new one).
fn fold_props(into: &mut Vec<(u32, Value)>, add: &[(u32, Value)]) {
    for (k, v) in add {
        if let Some(slot) = into.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            into.push((*k, v.clone()));
        }
    }
}

/// A node `SET` right-hand side with its property-key references interned to local
/// (then global) symbol ids — the spill-resident mirror of [`SetExpr`]. Functions
/// are evaluated at fold time against the node's accumulated props, so the whole
/// expression tree must survive the external sort.
pub(crate) enum SetExprI {
    Lit(Value),
    Prop(u32),
    Func {
        name: String,
        args: Vec<SetExprI>,
    },
    BinOp {
        op: slater_scalar::BinOp,
        l: Box<SetExprI>,
        r: Box<SetExprI>,
    },
    Cmp {
        op: slater_scalar::CmpOp,
        l: Box<SetExprI>,
        r: Box<SetExprI>,
    },
    And(Box<SetExprI>, Box<SetExprI>),
    Or(Box<SetExprI>, Box<SetExprI>),
    Not(Box<SetExprI>),
    Case {
        subject: Option<Box<SetExprI>>,
        whens: Vec<(SetExprI, SetExprI)>,
        els: Option<Box<SetExprI>>,
    },
}

/// Intern a parsed [`SetExpr`]'s property references against the local key interner.
/// Validates that any function is build-evaluable and rejects vector literals.
fn intern_set_expr(e: &SetExpr, keys: &mut crate::shared::Interner) -> Result<SetExprI> {
    Ok(match e {
        SetExpr::Lit(v) => {
            reject_vector(v, "node MERGE SET")?;
            SetExprI::Lit(v.clone())
        }
        SetExpr::Prop(name) => SetExprI::Prop(keys.intern(name)),
        SetExpr::Func { name, args } => SetExprI::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| intern_set_expr(a, keys))
                .collect::<Result<_>>()?,
        },
        SetExpr::BinOp { op, l, r } => SetExprI::BinOp {
            op: *op,
            l: Box::new(intern_set_expr(l, keys)?),
            r: Box::new(intern_set_expr(r, keys)?),
        },
        SetExpr::Cmp { op, l, r } => SetExprI::Cmp {
            op: *op,
            l: Box::new(intern_set_expr(l, keys)?),
            r: Box::new(intern_set_expr(r, keys)?),
        },
        SetExpr::And(l, r) => SetExprI::And(
            Box::new(intern_set_expr(l, keys)?),
            Box::new(intern_set_expr(r, keys)?),
        ),
        SetExpr::Or(l, r) => SetExprI::Or(
            Box::new(intern_set_expr(l, keys)?),
            Box::new(intern_set_expr(r, keys)?),
        ),
        SetExpr::Not(e) => SetExprI::Not(Box::new(intern_set_expr(e, keys)?)),
        SetExpr::Case {
            subject,
            whens,
            els,
        } => {
            let subject = match subject {
                Some(s) => Some(Box::new(intern_set_expr(s, keys)?)),
                None => None,
            };
            let mut iw = Vec::with_capacity(whens.len());
            for (c, t) in whens {
                iw.push((intern_set_expr(c, keys)?, intern_set_expr(t, keys)?));
            }
            let els = match els {
                Some(e) => Some(Box::new(intern_set_expr(e, keys)?)),
                None => None,
            };
            SetExprI::Case {
                subject,
                whens: iw,
                els,
            }
        }
    })
}

/// Rewrite the interned property-key ids inside an expression tree when a shard's
/// local symbol ids are remapped to global ids in [`dedup_nodes`].
fn remap_set_expr(e: &mut SetExprI, rm: &ShardRemap) {
    match e {
        SetExprI::Lit(_) => {}
        SetExprI::Prop(k) => *k = rm.map_key(*k),
        SetExprI::Func { args, .. } => {
            for a in args {
                remap_set_expr(a, rm);
            }
        }
        SetExprI::BinOp { l, r, .. }
        | SetExprI::Cmp { l, r, .. }
        | SetExprI::And(l, r)
        | SetExprI::Or(l, r) => {
            remap_set_expr(l, rm);
            remap_set_expr(r, rm);
        }
        SetExprI::Not(e) => remap_set_expr(e, rm),
        SetExprI::Case {
            subject,
            whens,
            els,
        } => {
            if let Some(s) = subject {
                remap_set_expr(s, rm);
            }
            for (c, t) in whens {
                remap_set_expr(c, rm);
                remap_set_expr(t, rm);
            }
            if let Some(e) = els {
                remap_set_expr(e, rm);
            }
        }
    }
}

fn encode_set_expr(buf: &mut Vec<u8>, e: &SetExprI) {
    match e {
        SetExprI::Lit(v) => {
            buf.push(0);
            write_value(buf, v);
        }
        SetExprI::Prop(k) => {
            buf.push(1);
            write_uvarint(buf, *k as u64);
        }
        SetExprI::Func { name, args } => {
            buf.push(2);
            write_uvarint(buf, name.len() as u64);
            buf.extend_from_slice(name.as_bytes());
            write_uvarint(buf, args.len() as u64);
            for a in args {
                encode_set_expr(buf, a);
            }
        }
        SetExprI::BinOp { op, l, r } => {
            buf.push(3);
            buf.push(bin_op_byte(*op));
            encode_set_expr(buf, l);
            encode_set_expr(buf, r);
        }
        SetExprI::Cmp { op, l, r } => {
            buf.push(4);
            buf.push(cmp_op_byte(*op));
            encode_set_expr(buf, l);
            encode_set_expr(buf, r);
        }
        SetExprI::And(l, r) => {
            buf.push(5);
            encode_set_expr(buf, l);
            encode_set_expr(buf, r);
        }
        SetExprI::Or(l, r) => {
            buf.push(6);
            encode_set_expr(buf, l);
            encode_set_expr(buf, r);
        }
        SetExprI::Not(e) => {
            buf.push(7);
            encode_set_expr(buf, e);
        }
        SetExprI::Case {
            subject,
            whens,
            els,
        } => {
            buf.push(8);
            match subject {
                Some(s) => {
                    buf.push(1);
                    encode_set_expr(buf, s);
                }
                None => buf.push(0),
            }
            write_uvarint(buf, whens.len() as u64);
            for (c, t) in whens {
                encode_set_expr(buf, c);
                encode_set_expr(buf, t);
            }
            match els {
                Some(e) => {
                    buf.push(1);
                    encode_set_expr(buf, e);
                }
                None => buf.push(0),
            }
        }
    }
}

fn bin_op_byte(op: slater_scalar::BinOp) -> u8 {
    use slater_scalar::BinOp::*;
    match op {
        Add => 0,
        Sub => 1,
        Mul => 2,
        Div => 3,
        Mod => 4,
    }
}

fn bin_op_from_byte(b: u8) -> Result<slater_scalar::BinOp> {
    use slater_scalar::BinOp::*;
    Ok(match b {
        0 => Add,
        1 => Sub,
        2 => Mul,
        3 => Div,
        4 => Mod,
        other => bail!("unknown BinOp byte {other} in node SET spill"),
    })
}

fn cmp_op_byte(op: slater_scalar::CmpOp) -> u8 {
    use slater_scalar::CmpOp::*;
    match op {
        Eq => 0,
        Ne => 1,
        Lt => 2,
        Le => 3,
        Gt => 4,
        Ge => 5,
    }
}

fn cmp_op_from_byte(b: u8) -> Result<slater_scalar::CmpOp> {
    use slater_scalar::CmpOp::*;
    Ok(match b {
        0 => Eq,
        1 => Ne,
        2 => Lt,
        3 => Le,
        4 => Gt,
        5 => Ge,
        other => bail!("unknown CmpOp byte {other} in node SET spill"),
    })
}

fn decode_set_expr(r: &mut &[u8]) -> Result<SetExprI> {
    let tag = read_u8(r)?;
    Ok(match tag {
        0 => SetExprI::Lit(read_value(r)?),
        1 => SetExprI::Prop(read_uvarint(r)? as u32),
        2 => {
            let nlen = read_uvarint(r)? as usize;
            if r.len() < nlen {
                bail!("truncated function name in node SET spill");
            }
            let name = std::str::from_utf8(&r[..nlen])
                .context("function name is not valid UTF-8")?
                .to_string();
            *r = &r[nlen..];
            let n = read_uvarint(r)? as usize;
            let mut args = Vec::with_capacity(n);
            for _ in 0..n {
                args.push(decode_set_expr(r)?);
            }
            SetExprI::Func { name, args }
        }
        3 => {
            let op = bin_op_from_byte(read_u8(r)?)?;
            let l = Box::new(decode_set_expr(r)?);
            let rr = Box::new(decode_set_expr(r)?);
            SetExprI::BinOp { op, l, r: rr }
        }
        4 => {
            let op = cmp_op_from_byte(read_u8(r)?)?;
            let l = Box::new(decode_set_expr(r)?);
            let rr = Box::new(decode_set_expr(r)?);
            SetExprI::Cmp { op, l, r: rr }
        }
        5 => {
            let l = Box::new(decode_set_expr(r)?);
            let rr = Box::new(decode_set_expr(r)?);
            SetExprI::And(l, rr)
        }
        6 => {
            let l = Box::new(decode_set_expr(r)?);
            let rr = Box::new(decode_set_expr(r)?);
            SetExprI::Or(l, rr)
        }
        7 => SetExprI::Not(Box::new(decode_set_expr(r)?)),
        8 => {
            let subject = if read_u8(r)? != 0 {
                Some(Box::new(decode_set_expr(r)?))
            } else {
                None
            };
            let n = read_uvarint(r)? as usize;
            let mut whens = Vec::with_capacity(n);
            for _ in 0..n {
                let c = decode_set_expr(r)?;
                let t = decode_set_expr(r)?;
                whens.push((c, t));
            }
            let els = if read_u8(r)? != 0 {
                Some(Box::new(decode_set_expr(r)?))
            } else {
                None
            };
            SetExprI::Case {
                subject,
                whens,
                els,
            }
        }
        other => bail!("unknown SET-expr tag {other}"),
    })
}

fn encode_node_set_props(buf: &mut Vec<u8>, props: &[(u32, SetExprI)]) {
    write_uvarint(buf, props.len() as u64);
    for (k, e) in props {
        write_uvarint(buf, *k as u64);
        encode_set_expr(buf, e);
    }
}

fn decode_node_set_props(r: &mut &[u8]) -> Result<Vec<(u32, SetExprI)>> {
    let n = read_uvarint(r)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let k = read_uvarint(r)? as u32;
        out.push((k, decode_set_expr(r)?));
    }
    Ok(out)
}

fn read_u8(r: &mut &[u8]) -> Result<u8> {
    let (first, rest) = r
        .split_first()
        .ok_or_else(|| anyhow!("unexpected end of spill"))?;
    *r = rest;
    Ok(*first)
}

/// Evaluate a node `SET` expression against the node's accumulated props (`props`),
/// in file order — so a property reference resolves to the value folded *before*
/// this statement, matching Cypher MERGE execution order.
fn eval_set_expr(e: &SetExprI, props: &[(u32, Value)]) -> Result<Value> {
    match e {
        SetExprI::Lit(v) => Ok(v.clone()),
        SetExprI::Prop(k) => Ok(props
            .iter()
            .find(|(ek, _)| ek == k)
            .map(|(_, v)| v.clone())
            .unwrap_or(Value::Null)),
        SetExprI::Func { name, args } => {
            let vs: Vec<Value> = args
                .iter()
                .map(|a| eval_set_expr(a, props))
                .collect::<Result<_>>()?;
            slater_scalar::eval_pure(name, &vs)?
                .ok_or_else(|| anyhow!("function `{name}` is not supported in build-time SET"))
        }
        SetExprI::BinOp { op, l, r } => {
            let a = eval_set_expr(l, props)?;
            let b = eval_set_expr(r, props)?;
            slater_scalar::eval_binop(*op, a, b)
        }
        SetExprI::Cmp { op, l, r } => {
            let a = eval_set_expr(l, props)?;
            let b = eval_set_expr(r, props)?;
            Ok(slater_scalar::eval_compare(*op, &a, &b))
        }
        // Three-valued (Kleene) boolean logic, short-circuiting on a definite result.
        SetExprI::And(l, r) => {
            let a = eval_set_expr(l, props)?;
            if matches!(a, Value::Bool(false)) {
                return Ok(Value::Bool(false));
            }
            let b = eval_set_expr(r, props)?;
            Ok(and3(&a, &b))
        }
        SetExprI::Or(l, r) => {
            let a = eval_set_expr(l, props)?;
            if matches!(a, Value::Bool(true)) {
                return Ok(Value::Bool(true));
            }
            let b = eval_set_expr(r, props)?;
            Ok(or3(&a, &b))
        }
        SetExprI::Not(e) => Ok(match eval_set_expr(e, props)? {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Null,
        }),
        // CASE: only the chosen branch is evaluated (lazy). A WHEN matches when its
        // condition is truthy (searched form) or equals the subject (simple form).
        SetExprI::Case {
            subject,
            whens,
            els,
        } => {
            let subj = match subject {
                Some(s) => Some(eval_set_expr(s, props)?),
                None => None,
            };
            for (cond, then) in whens {
                let matched = match &subj {
                    Some(s) => {
                        let c = eval_set_expr(cond, props)?;
                        matches!(
                            slater_scalar::eval_compare(slater_scalar::CmpOp::Eq, s, &c),
                            Value::Bool(true)
                        )
                    }
                    None => slater_scalar::truthy(&eval_set_expr(cond, props)?),
                };
                if matched {
                    return eval_set_expr(then, props);
                }
            }
            match els {
                Some(e) => eval_set_expr(e, props),
                None => Ok(Value::Null),
            }
        }
    }
}

/// Boolean view of a value for Kleene logic: `Bool` → `Some`, everything else
/// (including NULL) → `None` (unknown).
fn three_valued(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

/// Kleene AND: false dominates; both-true is true; otherwise unknown (NULL).
fn and3(a: &Value, b: &Value) -> Value {
    match (three_valued(a), three_valued(b)) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

/// Kleene OR: true dominates; both-false is false; otherwise unknown (NULL).
fn or3(a: &Value, b: &Value) -> Value {
    match (three_valued(a), three_valued(b)) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

/// Fold a statement's `SET` assignments into the accumulating node props, evaluating
/// each right-hand side against the state so far (last-writer-wins per key).
fn fold_node_props(into: &mut Vec<(u32, Value)>, add: &[(u32, SetExprI)]) -> Result<()> {
    for (k, e) in add {
        let v = eval_set_expr(e, into)?;
        if let Some(slot) = into.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v;
        } else {
            into.push((*k, v));
        }
    }
    Ok(())
}

// ── pass-1 spill records (per-shard, LOCAL symbol ids) ─────────────────────────

/// A node MERGE as spilled in pass 1. `label`/`key`/set-prop keys are the shard's
/// **local** symbol ids (remapped to global in [`dedup_nodes`]). `seq` is unused on
/// disk (written 0) and assigned during dedup; it carries the input-order tiebreaker
/// so last-writer-wins is well defined and the sort total.
pub(crate) struct NodeMergeRec {
    pub label: u32,
    pub key: u32,
    pub value: Value,
    pub set_props: Vec<(u32, SetExprI)>,
    pub seq: u64,
}

fn encode_props(buf: &mut Vec<u8>, props: &[(u32, Value)]) {
    write_uvarint(buf, props.len() as u64);
    for (k, v) in props {
        write_uvarint(buf, *k as u64);
        write_value(buf, v);
    }
}

fn decode_props_pairs(r: &mut &[u8]) -> Result<Vec<(u32, Value)>> {
    let n = read_uvarint(r)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let k = read_uvarint(r)? as u32;
        let v = read_value(r)?;
        out.push((k, v));
    }
    Ok(out)
}

impl SortRecord for NodeMergeRec {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.label as u64);
        write_uvarint(buf, self.key as u64);
        write_value(buf, &self.value);
        encode_node_set_props(buf, &self.set_props);
        write_uvarint(buf, self.seq);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let label = read_uvarint(r)? as u32;
        let key = read_uvarint(r)? as u32;
        let value = read_value(r)?;
        let set_props = decode_node_set_props(r)?;
        let seq = read_uvarint(r)?;
        Ok(NodeMergeRec {
            label,
            key,
            value,
            set_props,
            seq,
        })
    }
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.label
            .cmp(&other.label)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| value_cmp_exact(&self.value, &other.value))
            .then_with(|| self.seq.cmp(&other.seq))
    }
    fn size_hint(&self) -> usize {
        24 + 16 * self.set_props.len()
    }
}

/// An edge MERGE as spilled in pass 1 (LOCAL symbol ids). Endpoints are business-key
/// triples; the edge is created by resolving both against the node key stream.
pub(crate) struct EdgeMergeRec {
    pub src_label: u32,
    pub src_key: u32,
    pub src_value: Value,
    pub dst_label: u32,
    pub dst_key: u32,
    pub dst_value: Value,
    pub reltype: u32,
    pub set_props: Vec<(u32, Value)>,
}

impl EdgeMergeRec {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.src_label as u64);
        write_uvarint(buf, self.src_key as u64);
        write_value(buf, &self.src_value);
        write_uvarint(buf, self.dst_label as u64);
        write_uvarint(buf, self.dst_key as u64);
        write_value(buf, &self.dst_value);
        write_uvarint(buf, self.reltype as u64);
        encode_props(buf, &self.set_props);
    }
    fn decode(mut r: &[u8]) -> Result<Self> {
        let src_label = read_uvarint(&mut r)? as u32;
        let src_key = read_uvarint(&mut r)? as u32;
        let src_value = read_value(&mut r)?;
        let dst_label = read_uvarint(&mut r)? as u32;
        let dst_key = read_uvarint(&mut r)? as u32;
        let dst_value = read_value(&mut r)?;
        let reltype = read_uvarint(&mut r)? as u32;
        let set_props = decode_props_pairs(&mut r)?;
        Ok(EdgeMergeRec {
            src_label,
            src_key,
            src_value,
            dst_label,
            dst_key,
            dst_value,
            reltype,
            set_props,
        })
    }
}

/// Pass-1 writers for one shard's merge records. Created per worker; the segment
/// files are fsynced by `finalize_shard` (the sidecar is the resume signal).
pub(crate) struct MergeShardWriters {
    node: BlockFileWriter,
    edge: BlockFileWriter,
    scratch: Vec<u8>,
}

impl MergeShardWriters {
    pub(crate) fn create(node_seg: &Path, edge_seg: &Path, zstd_level: i32) -> Result<Self> {
        Ok(Self {
            node: BlockFileWriter::create(node_seg, BUCKET_BLOCK, zstd_level)?,
            edge: BlockFileWriter::create(edge_seg, BUCKET_BLOCK, zstd_level)?,
            scratch: Vec::new(),
        })
    }
    pub(crate) fn append_node(&mut self, rec: &NodeMergeRec) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        self.node.append_record(&self.scratch)?;
        Ok(())
    }
    pub(crate) fn append_edge(&mut self, rec: &EdgeMergeRec) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        self.edge.append_record(&self.scratch)?;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<()> {
        self.node.finish()?;
        self.edge.finish()?;
        Ok(())
    }
}

/// Reject a value that cannot be a business key or a stored scalar prop (vectors are
/// routed to the vector store, which merge dumps don't drive).
fn reject_vector(v: &Value, ctx: &str) -> Result<()> {
    if matches!(v, Value::Vector(_)) {
        bail!("{ctx}: vector values are not supported in merge dumps");
    }
    Ok(())
}

/// Intern a statement's SET / business-key value list against a (mutable) local
/// interner, returning `(key_local, value)` pairs. Rejects vector values.
pub(crate) fn build_node_merge_rec(
    o: &crate::model::NodeOverwriteStmt,
    labels: &mut crate::shared::Interner,
    keys: &mut crate::shared::Interner,
) -> Result<NodeMergeRec> {
    reject_vector(&o.match_.value, "node MERGE business key")?;
    let label = labels.intern(&o.match_.label);
    let key = keys.intern(&o.match_.key);
    let mut set_props = Vec::with_capacity(o.set_props.len());
    for (k, e) in &o.set_props {
        validate_set_expr(e)?;
        set_props.push((keys.intern(k), intern_set_expr(e, keys)?));
    }
    Ok(NodeMergeRec {
        label,
        key,
        value: o.match_.value.clone(),
        set_props,
        seq: 0,
    })
}

/// Build an [`EdgeMergeRec`] from a parsed edge MERGE, interning its symbols locally.
pub(crate) fn build_edge_merge_rec(
    o: &crate::model::EdgeOverwriteStmt,
    labels: &mut crate::shared::Interner,
    reltypes: &mut crate::shared::Interner,
    keys: &mut crate::shared::Interner,
) -> Result<EdgeMergeRec> {
    reject_vector(&o.src.value, "edge MERGE source key")?;
    reject_vector(&o.dst.value, "edge MERGE target key")?;
    let src_label = labels.intern(&o.src.label);
    let src_key = keys.intern(&o.src.key);
    let dst_label = labels.intern(&o.dst.label);
    let dst_key = keys.intern(&o.dst.key);
    let reltype = reltypes.intern(&o.reltype);
    let mut set_props = Vec::with_capacity(o.set_props.len());
    for (k, v) in &o.set_props {
        reject_vector(v, "edge MERGE SET")?;
        set_props.push((keys.intern(k), v.clone()));
    }
    Ok(EdgeMergeRec {
        src_label,
        src_key,
        src_value: o.src.value.clone(),
        dst_label,
        dst_key,
        dst_value: o.dst.value.clone(),
        reltype,
        set_props,
    })
}

// ── phase 1: node dedup ────────────────────────────────────────────────────────

/// `(label, key, value) → prov` record, written in identity sort order by
/// [`dedup_nodes`] and replayed as the "one" side of the edge merge-join.
struct KeyProv {
    label: u32,
    key: u32,
    value: Value,
    prov: u64,
}

impl KeyProv {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.label as u64);
        write_uvarint(buf, self.key as u64);
        write_value(buf, &self.value);
        write_uvarint(buf, self.prov);
    }
    fn decode(mut r: &[u8]) -> Result<Self> {
        let label = read_uvarint(&mut r)? as u32;
        let key = read_uvarint(&mut r)? as u32;
        let value = read_value(&mut r)?;
        let prov = read_uvarint(&mut r)?;
        Ok(KeyProv {
            label,
            key,
            value,
            prov,
        })
    }
}

/// Collapse same-identity node MERGEs into one node each, folding SET props
/// last-writer-wins in input order. Writes the deduped node bucket (`nodes_out`,
/// global symbol ids, dump_id `None`) and the `(identity → prov)` key stream
/// **hash-partitioned by value** into `node_keys_out.<p>` (one file per
/// [`RESOLVE_PARTS`]). Because the drain is in global `(label, key, value)` order, each
/// partition file is itself sorted by that key — exactly the "one" side the parallel
/// per-partition merge-join in [`resolve_edges`] consumes. Returns the distinct-node
/// count. (prov ids are still assigned in global identity order, so deterministic.)
pub(crate) fn dedup_nodes(
    node_merge_bkt: &Path,
    remaps: &[ShardRemap],
    nodes_out: &Path,
    node_keys_out: &Path,
    scratch_dir: &Path,
    sort_budget: usize,
    zstd_level: i32,
) -> Result<u64> {
    let mut sorter = ExtSorter::<NodeMergeRec>::new(scratch_dir, sort_budget, zstd_level)?;
    let mut seq = 0u64;
    for (si, seg) in segments(node_merge_bkt).into_iter().enumerate() {
        let rm = remaps.get(si);
        let rdr = BlockFileReader::open(&seg)?;
        rdr.for_each_record(|_, rec| {
            let mut s = rec;
            let mut nm = NodeMergeRec::decode(&mut s)?;
            if let Some(rm) = rm {
                if !rm.identity {
                    nm.label = rm.map_label(nm.label);
                    nm.key = rm.map_key(nm.key);
                    for (k, e) in nm.set_props.iter_mut() {
                        *k = rm.map_key(*k);
                        remap_set_expr(e, rm);
                    }
                }
            }
            nm.seq = seq;
            seq += 1;
            sorter.push(nm)
        })?;
    }

    let mut nodes_w = BucketWriter::create(nodes_out, BUCKET_BLOCK, zstd_level)?;
    // One key-stream file per hash partition; each ends up sorted by (label,key,value)
    // because the drain visits identities in that global order.
    let mut keys_w: Vec<BlockFileWriter> = (0..RESOLVE_PARTS)
        .map(|p| {
            BlockFileWriter::create(seg_path(node_keys_out, p as u64), BUCKET_BLOCK, zstd_level)
        })
        .collect::<Result<_>>()?;
    let mut scratch = Vec::new();
    let mut prov = 0u64;
    let mut cur: Option<(u32, u32, Value)> = None;
    let mut props: Vec<(u32, Value)> = Vec::new();

    let mut flush = |cur: &mut Option<(u32, u32, Value)>,
                     props: &mut Vec<(u32, Value)>,
                     nodes_w: &mut BucketWriter,
                     keys_w: &mut [BlockFileWriter],
                     prov: &mut u64|
     -> Result<()> {
        if let Some((label, key, value)) = cur.take() {
            nodes_w.append_node(&NodeRec {
                dump_id: None,
                labels_blob: encode_labels_record(&[label]),
                props_blob: encode_props_record(props),
                vec_props: Vec::new(),
            })?;
            scratch.clear();
            let part = part_of_value(&value);
            KeyProv {
                label,
                key,
                value,
                prov: *prov,
            }
            .encode(&mut scratch);
            keys_w[part].append_record(&scratch)?;
            *prov += 1;
        }
        Ok(())
    };

    for r in sorter.sorted()? {
        let nm = r?;
        let same = matches!(&cur, Some((l, k, v))
            if *l == nm.label && *k == nm.key && value_cmp_exact(v, &nm.value) == Ordering::Equal);
        if !same {
            flush(&mut cur, &mut props, &mut nodes_w, &mut keys_w, &mut prov)?;
            cur = Some((nm.label, nm.key, nm.value.clone()));
            // Identity prop first; SET props then fold over it last-wins.
            props = vec![(nm.key, nm.value.clone())];
        }
        fold_node_props(&mut props, &nm.set_props)?;
    }
    flush(&mut cur, &mut props, &mut nodes_w, &mut keys_w, &mut prov)?;
    nodes_w.finish()?;
    for w in keys_w {
        w.finish()?;
    }
    Ok(prov)
}

// ── phase 2: edge resolve (business-key merge-join) ────────────────────────────

/// A business-key endpoint reference of one edge, sorted by its triple so it can be
/// merge-joined against the (also triple-sorted) node key stream.
struct EndpointRef {
    label: u32,
    key: u32,
    value: Value,
    edge_seq: u64,
    which: u8, // 0 = src, 1 = dst
}

impl SortRecord for EndpointRef {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.label as u64);
        write_uvarint(buf, self.key as u64);
        write_value(buf, &self.value);
        write_uvarint(buf, self.edge_seq);
        buf.push(self.which);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let label = read_uvarint(r)? as u32;
        let key = read_uvarint(r)? as u32;
        let value = read_value(r)?;
        let edge_seq = read_uvarint(r)?;
        let (which, rest) = r.split_first().context("endpoint ref truncated")?;
        let which = *which;
        *r = rest;
        Ok(EndpointRef {
            label,
            key,
            value,
            edge_seq,
            which,
        })
    }
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.label
            .cmp(&other.label)
            .then_with(|| self.key.cmp(&other.key))
            .then_with(|| value_cmp_exact(&self.value, &other.value))
            .then_with(|| self.edge_seq.cmp(&other.edge_seq))
            .then_with(|| self.which.cmp(&other.which))
    }
    fn size_hint(&self) -> usize {
        24
    }
}

/// A resolved endpoint: which `prov` an edge's `which` end maps to. Sorted by
/// `(edge_seq, which)` so the two ends of each edge come out adjacent and in
/// (src, dst) order.
struct ResolvedEndpoint {
    edge_seq: u64,
    which: u8,
    prov: u64,
}

impl SortRecord for ResolvedEndpoint {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.edge_seq);
        buf.push(self.which);
        write_uvarint(buf, self.prov);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let edge_seq = read_uvarint(r)?;
        let (which, rest) = r.split_first().context("resolved endpoint truncated")?;
        let which = *which;
        *r = rest;
        let prov = read_uvarint(r)?;
        Ok(ResolvedEndpoint {
            edge_seq,
            which,
            prov,
        })
    }
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.edge_seq
            .cmp(&other.edge_seq)
            .then_with(|| self.which.cmp(&other.which))
    }
    fn size_hint(&self) -> usize {
        24
    }
}

/// A fully resolved edge, sorted by `(src, reltype, dst, edge_seq)` so identical
/// relationships group adjacently with their SET props in input order (last-wins).
struct EdgeFinal {
    src: u64,
    dst: u64,
    reltype: u32,
    props_blob: Vec<u8>,
    edge_seq: u64,
}

impl SortRecord for EdgeFinal {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.src);
        write_uvarint(buf, self.dst);
        write_uvarint(buf, self.reltype as u64);
        write_uvarint(buf, self.edge_seq);
        write_blob(buf, &self.props_blob);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let src = read_uvarint(r)?;
        let dst = read_uvarint(r)?;
        let reltype = read_uvarint(r)? as u32;
        let edge_seq = read_uvarint(r)?;
        let props_blob = read_blob(r)?.to_vec();
        Ok(EdgeFinal {
            src,
            dst,
            reltype,
            props_blob,
            edge_seq,
        })
    }
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.src
            .cmp(&other.src)
            .then_with(|| self.reltype.cmp(&other.reltype))
            .then_with(|| self.dst.cmp(&other.dst))
            .then_with(|| self.edge_seq.cmp(&other.edge_seq))
    }
    fn size_hint(&self) -> usize {
        40 + self.props_blob.len()
    }
}

/// An edge's reltype + props, keyed by `edge_seq`. Sorted by `edge_seq` so it
/// merge-joins in lockstep with the resolved endpoints within an edge-partition.
struct Payload {
    edge_seq: u64,
    reltype: u32,
    props_blob: Vec<u8>,
}

impl SortRecord for Payload {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.edge_seq);
        write_uvarint(buf, self.reltype as u64);
        write_blob(buf, &self.props_blob);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let edge_seq = read_uvarint(r)?;
        let reltype = read_uvarint(r)? as u32;
        let props_blob = read_blob(r)?.to_vec();
        Ok(Payload {
            edge_seq,
            reltype,
            props_blob,
        })
    }
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.edge_seq.cmp(&other.edge_seq)
    }
    fn size_hint(&self) -> usize {
        16 + self.props_blob.len()
    }
}

/// Stable partition for an edge's `(src, reltype, dst)` identity, so identical
/// relationships land in the same dedup partition.
fn part_of_triple(src: u64, reltype: u32, dst: u64) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for x in [src, reltype as u64, dst] {
        h ^= x;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % RESOLVE_PARTS as u64) as usize
}

/// A sequential pull reader over a single block file — one decompressed block resident
/// at a time. Used as the "one" side of the merge-join and for the in-order payload
/// replay, where a callback-style scan can't interleave with a second stream.
struct RecStream {
    rdr: BlockFileReader,
    nblocks: usize,
    block: usize,
    offsets: Vec<u32>,
    data: Vec<u8>,
    slot: usize,
}

impl RecStream {
    fn open(path: &Path) -> Result<Self> {
        let rdr = BlockFileReader::open(path)?;
        let nblocks = rdr.num_blocks();
        let mut s = Self {
            rdr,
            nblocks,
            block: 0,
            offsets: Vec::new(),
            data: Vec::new(),
            slot: 0,
        };
        if nblocks > 0 {
            s.load_block(0)?;
        }
        Ok(s)
    }
    fn load_block(&mut self, b: usize) -> Result<()> {
        let raw = self.rdr.read_block(BlockId(b as u32))?;
        let (offsets, data) = parse_block(&raw)?;
        self.offsets = offsets;
        self.data = data.to_vec();
        self.slot = 0;
        Ok(())
    }
    /// Next raw record, or `None` at end of file.
    fn next_raw(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            let nslots = self.offsets.len().saturating_sub(1);
            if self.slot < nslots {
                let start = self.offsets[self.slot] as usize;
                let end = self.offsets[self.slot + 1] as usize;
                self.slot += 1;
                return Ok(Some(self.data[start..end].to_vec()));
            }
            self.block += 1;
            if self.block >= self.nblocks {
                return Ok(None);
            }
            self.load_block(self.block)?;
        }
    }
}

/// Resolve every edge MERGE's endpoints by business key against the `node_keys.<p>`
/// partitioned `(identity → prov)` streams from [`dedup_nodes`], then collapse
/// identical `(src, reltype, dst)` edges (SET props last-wins) into the final segmented
/// edge bucket (`edge_out.<t>`, global reltype ids). An endpoint with no matching node
/// is a hard error (self-contained-dump invariant). Returns the edge count.
///
/// Parallelized by hash-partitioning the keyspace into [`RESOLVE_PARTS`] independent
/// shards worked on a `threads`-wide pool (mirroring `emit.topology`'s node bands), so
/// the join no longer runs single-core. Three barrier'd stages, each repartitioning by
/// the key the next stage needs:
///   0. **scan** edges → endpoint refs partitioned by `value` (co-located with the node
///      keys they match), payloads partitioned by `edge_seq`;
///   1. **resolve** per value-partition: merge-join refs against `node_keys.<p>`, emit
///      `(edge_seq, which, prov)` repartitioned by `edge_seq`;
///   2. **reassemble** per edge-partition: pair src/dst by `edge_seq` with the payload,
///      emit `EdgeFinal` repartitioned by `(src, reltype, dst)`;
///   3. **dedup** per triple-partition: sort, collapse identical edges, write
///      `edge_out.<t>`.
///
/// Determinism: `RESOLVE_PARTS` and the partition hashes are fixed (independent of
/// `threads`); `edge_seq` is assigned by per-segment prefix-sum bases; every
/// per-partition sort is total-ordered; `prov_edge_id` is `(t << 40) | i` (a stable
/// per-partition value — emit uses it only as a sort tiebreaker, not as a dense index).
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_edges(
    edge_merge_bkt: &Path,
    remaps: &[ShardRemap],
    node_keys: &Path,
    edge_out: &Path,
    scratch_dir: &Path,
    sort_budget: usize,
    threads: usize,
    zstd_level: i32,
) -> Result<u64> {
    let nthreads = threads.max(1);
    // Per-partition sort budget: at most `nthreads` partitions hold an ExtSorter live at
    // once, so divide to keep total resident ≈ sort_budget (as emit.topology does).
    let part_budget = (sort_budget / nthreads).max(8 << 20);
    let batch_threshold =
        (sort_budget / 64 / (RESOLVE_PARTS * nthreads).max(1)).clamp(16 << 10, 1 << 20);
    let ep_base = scratch_dir.join("ep_part.bkt");
    let pay_base = scratch_dir.join("pay_part.bkt");
    let res_base = scratch_dir.join("res_part.bkt");
    let ef_base = scratch_dir.join("ef_part.bkt");

    // ── stage 0: scan edges → endpoint refs (by value) + payloads (by edge_seq) ──
    // Deterministic `edge_seq` via per-segment prefix-sum bases, so the parallel scan
    // assigns the same ids as a serial pass.
    let segs = segments(edge_merge_bkt);
    let mut seg_bases: Vec<u64> = Vec::with_capacity(segs.len());
    {
        let mut acc = 0u64;
        for seg in &segs {
            seg_bases.push(acc);
            acc += BlockFileReader::open(seg)?.total_records();
        }
    }
    let ep_spill = PartSpill::new(&ep_base, zstd_level)?;
    let pay_spill = PartSpill::new(&pay_base, zstd_level)?;
    {
        let next = AtomicU64::new(0);
        let err: Mutex<Option<anyhow::Error>> = Mutex::new(None);
        let (next_r, err_r, segs_r, bases_r, ep_r, pay_r) =
            (&next, &err, &segs, &seg_bases, &ep_spill, &pay_spill);
        std::thread::scope(|scope| {
            for _ in 0..nthreads {
                scope.spawn(move || {
                    let mut epb = PartBatcher::new(ep_r, batch_threshold);
                    let mut payb = PartBatcher::new(pay_r, batch_threshold);
                    loop {
                        if err_r.lock().unwrap().is_some() {
                            break;
                        }
                        let si = next_r.fetch_add(1, AtomicOrdering::Relaxed) as usize;
                        if si >= segs_r.len() {
                            break;
                        }
                        let rm = remaps.get(si);
                        let mut edge_seq = bases_r[si];
                        let res = (|| -> Result<()> {
                            let rdr = BlockFileReader::open(&segs_r[si])?;
                            rdr.for_each_record(|_, rec| {
                                let mut em = EdgeMergeRec::decode(rec)?;
                                if let Some(rm) = rm {
                                    if !rm.identity {
                                        em.src_label = rm.map_label(em.src_label);
                                        em.src_key = rm.map_key(em.src_key);
                                        em.dst_label = rm.map_label(em.dst_label);
                                        em.dst_key = rm.map_key(em.dst_key);
                                        em.reltype = rm.map_reltype(em.reltype);
                                        for (k, _) in em.set_props.iter_mut() {
                                            *k = rm.map_key(*k);
                                        }
                                    }
                                }
                                epb.push(
                                    part_of_value(&em.src_value),
                                    &EndpointRef {
                                        label: em.src_label,
                                        key: em.src_key,
                                        value: em.src_value,
                                        edge_seq,
                                        which: 0,
                                    },
                                )?;
                                epb.push(
                                    part_of_value(&em.dst_value),
                                    &EndpointRef {
                                        label: em.dst_label,
                                        key: em.dst_key,
                                        value: em.dst_value,
                                        edge_seq,
                                        which: 1,
                                    },
                                )?;
                                payb.push(
                                    part_of_u64(edge_seq),
                                    &Payload {
                                        edge_seq,
                                        reltype: em.reltype,
                                        props_blob: encode_props_record(&em.set_props),
                                    },
                                )?;
                                edge_seq += 1;
                                Ok(())
                            })?;
                            epb.flush_all()?;
                            payb.flush_all()?;
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
    ep_spill.finish()?;
    pay_spill.finish()?;

    // ── stage 1: resolve each value-partition by merge-join, repartition by edge_seq ─
    let res_spill = PartSpill::new(&res_base, zstd_level)?;
    par_partitions(nthreads, |p| {
        let mut sorter = ExtSorter::<EndpointRef>::new(scratch_dir, part_budget, zstd_level)?;
        let rdr = BlockFileReader::open(seg_path(&ep_base, p as u64))?;
        rdr.for_each_record(|_, rec| {
            let mut s = rec;
            sorter.push(EndpointRef::decode(&mut s)?)
        })?;
        let mut keys = RecStream::open(&seg_path(node_keys, p as u64))?;
        let mut cur_key: Option<KeyProv> = match keys.next_raw()? {
            Some(b) => Some(KeyProv::decode(&b)?),
            None => None,
        };
        let mut batcher = PartBatcher::new(&res_spill, batch_threshold);
        for ep in sorter.sorted()? {
            let ep = ep?;
            loop {
                match &cur_key {
                    Some(k) => match cmp_key_triple(k, &ep) {
                        Ordering::Less => {
                            cur_key = match keys.next_raw()? {
                                Some(b) => Some(KeyProv::decode(&b)?),
                                None => None,
                            };
                        }
                        Ordering::Equal => break,
                        Ordering::Greater => bail!(unmatched_endpoint(&ep)),
                    },
                    None => bail!(unmatched_endpoint(&ep)),
                }
            }
            let prov = cur_key.as_ref().unwrap().prov;
            batcher.push(
                part_of_u64(ep.edge_seq),
                &ResolvedEndpoint {
                    edge_seq: ep.edge_seq,
                    which: ep.which,
                    prov,
                },
            )?;
        }
        batcher.flush_all()?;
        Ok(())
    })?;
    res_spill.finish()?;
    rm_parts(&ep_base);

    // ── stage 2: reassemble each edge-partition (pair src/dst + payload), repart by triple ─
    let ef_spill = PartSpill::new(&ef_base, zstd_level)?;
    par_partitions(nthreads, |q| {
        let mut res_sorter =
            ExtSorter::<ResolvedEndpoint>::new(scratch_dir, part_budget, zstd_level)?;
        BlockFileReader::open(seg_path(&res_base, q as u64))?.for_each_record(|_, rec| {
            let mut s = rec;
            res_sorter.push(ResolvedEndpoint::decode(&mut s)?)
        })?;
        let mut pay_sorter = ExtSorter::<Payload>::new(scratch_dir, part_budget, zstd_level)?;
        BlockFileReader::open(seg_path(&pay_base, q as u64))?.for_each_record(|_, rec| {
            let mut s = rec;
            pay_sorter.push(Payload::decode(&mut s)?)
        })?;
        let mut batcher = PartBatcher::new(&ef_spill, batch_threshold);
        let mut res_iter = res_sorter.sorted()?;
        let mut next_res = || -> Result<Option<ResolvedEndpoint>> { res_iter.next().transpose() };
        for p in pay_sorter.sorted()? {
            let p = p?;
            let src = next_res()?.context("resolved endpoints exhausted (src)")?;
            let dst = next_res()?.context("resolved endpoints exhausted (dst)")?;
            if src.edge_seq != p.edge_seq
                || dst.edge_seq != p.edge_seq
                || src.which != 0
                || dst.which != 1
            {
                bail!(
                    "internal: endpoint/payload misalignment at edge_seq {}",
                    p.edge_seq
                );
            }
            batcher.push(
                part_of_triple(src.prov, p.reltype, dst.prov),
                &EdgeFinal {
                    src: src.prov,
                    dst: dst.prov,
                    reltype: p.reltype,
                    props_blob: p.props_blob,
                    edge_seq: p.edge_seq,
                },
            )?;
        }
        if next_res()?.is_some() {
            bail!("internal: leftover resolved endpoints after reassembly");
        }
        batcher.flush_all()?;
        Ok(())
    })?;
    ef_spill.finish()?;
    rm_parts(&res_base);
    rm_parts(&pay_base);

    // ── stage 3: dedup each triple-partition, write the final edge bucket segments ──
    let counts: Vec<AtomicU64> = (0..RESOLVE_PARTS).map(|_| AtomicU64::new(0)).collect();
    let counts_r = &counts;
    par_partitions(nthreads, |t| {
        let mut sorter = ExtSorter::<EdgeFinal>::new(scratch_dir, part_budget, zstd_level)?;
        BlockFileReader::open(seg_path(&ef_base, t as u64))?.for_each_record(|_, rec| {
            let mut s = rec;
            sorter.push(EdgeFinal::decode(&mut s)?)
        })?;
        let mut edge_w =
            BucketWriter::create(seg_path(edge_out, t as u64), BUCKET_BLOCK, zstd_level)?;
        let mut local = 0u64;
        let mut cur: Option<(u64, u32, u64)> = None;
        let mut props: Vec<(u32, Value)> = Vec::new();
        let flush = |cur: &mut Option<(u64, u32, u64)>,
                     props: &mut Vec<(u32, Value)>,
                     w: &mut BucketWriter,
                     local: &mut u64|
         -> Result<()> {
            if let Some((src, reltype, dst)) = cur.take() {
                w.append_edge(&EdgeRec {
                    // Per-partition id: emit uses prov_edge_id only as a sort
                    // tiebreaker, so a stable (partition, local) value suffices.
                    prov_edge_id: ((t as u64) << 40) | *local,
                    src_prov: src,
                    dst_prov: dst,
                    reltype,
                    props_blob: encode_props_record(props),
                })?;
                *local += 1;
            }
            Ok(())
        };
        for r in sorter.sorted()? {
            let ef = r?;
            let id = (ef.src, ef.reltype, ef.dst);
            if cur != Some(id) {
                flush(&mut cur, &mut props, &mut edge_w, &mut local)?;
                cur = Some(id);
                props = decode_props(&ef.props_blob)?;
            } else {
                let add = decode_props(&ef.props_blob)?;
                fold_props(&mut props, &add);
            }
        }
        flush(&mut cur, &mut props, &mut edge_w, &mut local)?;
        edge_w.finish()?;
        counts_r[t].store(local, AtomicOrdering::Relaxed);
        Ok(())
    })?;
    rm_parts(&ef_base);
    Ok(counts.iter().map(|c| c.load(AtomicOrdering::Relaxed)).sum())
}

/// The self-contained-dump violation error for an unresolved endpoint.
fn unmatched_endpoint(ep: &EndpointRef) -> String {
    format!(
        "edge MERGE references an endpoint with no matching node MERGE this run \
         (label id {}, key id {}, value {:?}); merge dumps must be self-contained",
        ep.label, ep.key, ep.value
    )
}

/// Remove all `<base>.<p>` partition segment files.
fn rm_parts(base: &Path) {
    for p in 0..RESOLVE_PARTS {
        let _ = std::fs::remove_file(seg_path(base, p as u64));
    }
}

/// Compare a node identity against an endpoint reference by `(label, key, value)`.
fn cmp_key_triple(k: &KeyProv, ep: &EndpointRef) -> Ordering {
    k.label
        .cmp(&ep.label)
        .then_with(|| k.key.cmp(&ep.key))
        .then_with(|| value_cmp_exact(&k.value, &ep.value))
}

#[cfg(test)]
mod set_expr_tests {
    use super::*;

    fn str_lit(s: &str) -> SetExprI {
        SetExprI::Lit(Value::Str(s.to_string()))
    }

    /// The dump's affiliation-append idiom for property `k`, appending `lit`:
    /// `CASE WHEN coalesce(p.k,'')='' THEN lit ELSE p.k + '; ' + lit END`.
    fn append_idiom(k: u32, lit: &str) -> SetExprI {
        SetExprI::Case {
            subject: None,
            whens: vec![(
                SetExprI::Cmp {
                    op: slater_scalar::CmpOp::Eq,
                    l: Box::new(SetExprI::Func {
                        name: "coalesce".to_string(),
                        args: vec![SetExprI::Prop(k), str_lit("")],
                    }),
                    r: Box::new(str_lit("")),
                },
                str_lit(lit),
            )],
            els: Some(Box::new(SetExprI::BinOp {
                op: slater_scalar::BinOp::Add,
                l: Box::new(SetExprI::BinOp {
                    op: slater_scalar::BinOp::Add,
                    l: Box::new(SetExprI::Prop(k)),
                    r: Box::new(str_lit("; ")),
                }),
                r: Box::new(str_lit(lit)),
            })),
        }
    }

    fn get(props: &[(u32, Value)], k: u32) -> Option<Value> {
        props
            .iter()
            .find(|(ek, _)| *ek == k)
            .map(|(_, v)| v.clone())
    }

    #[test]
    fn case_append_idiom_accumulates_in_input_order() {
        let k = 0u32;
        let mut props: Vec<(u32, Value)> = Vec::new();
        // First append hits the THEN branch (prop absent → coalesce empty); later
        // appends hit the ELSE concat against the accumulated value.
        for (lit, expected) in [("A", "A"), ("B", "A; B"), ("C", "A; B; C")] {
            fold_node_props(&mut props, &[(k, append_idiom(k, lit))]).unwrap();
            assert_eq!(get(&props, k), Some(Value::Str(expected.to_string())));
        }
    }

    #[test]
    fn binop_and_cmp_against_accumulated_props() {
        let props: Vec<(u32, Value)> = vec![(0, Value::Int(2))];
        let add = SetExprI::BinOp {
            op: slater_scalar::BinOp::Add,
            l: Box::new(SetExprI::Prop(0)),
            r: Box::new(SetExprI::Lit(Value::Int(3))),
        };
        assert_eq!(eval_set_expr(&add, &props).unwrap(), Value::Int(5));
        let cmp = SetExprI::Cmp {
            op: slater_scalar::CmpOp::Eq,
            l: Box::new(SetExprI::Prop(0)),
            r: Box::new(SetExprI::Lit(Value::Int(2))),
        };
        assert_eq!(eval_set_expr(&cmp, &props).unwrap(), Value::Bool(true));
    }

    #[test]
    fn case_with_no_matching_when_and_no_else_is_null() {
        let e = SetExprI::Case {
            subject: None,
            whens: vec![(
                SetExprI::Lit(Value::Bool(false)),
                SetExprI::Lit(Value::Int(1)),
            )],
            els: None,
        };
        assert_eq!(eval_set_expr(&e, &[]).unwrap(), Value::Null);
    }

    #[test]
    fn spill_roundtrip_preserves_all_variants() {
        let k = 3u32;
        let exprs = vec![
            append_idiom(k, "Acme"),
            SetExprI::Not(Box::new(SetExprI::Lit(Value::Bool(true)))),
            SetExprI::And(
                Box::new(SetExprI::Lit(Value::Bool(true))),
                Box::new(SetExprI::Lit(Value::Bool(false))),
            ),
            SetExprI::Or(
                Box::new(SetExprI::Lit(Value::Bool(false))),
                Box::new(SetExprI::Prop(k)),
            ),
        ];
        for e in &exprs {
            let mut buf = Vec::new();
            encode_set_expr(&mut buf, e);
            let mut r = buf.as_slice();
            let decoded = decode_set_expr(&mut r).unwrap();
            assert!(r.is_empty(), "decoder must consume every byte it wrote");
            // SetExprI has no PartialEq; assert structural equality via re-encode.
            let mut buf2 = Vec::new();
            encode_set_expr(&mut buf2, &decoded);
            assert_eq!(buf, buf2, "re-encode of decoded tree must match");
        }
    }
}
