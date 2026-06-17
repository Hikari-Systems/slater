// SPDX-License-Identifier: Apache-2.0
//! Build-time diagnostics: an opt-in, gated sampler that records *what the build
//! was doing* and *what resource it was spending* at regular intervals, so a long
//! build (e.g. the 91M-node wiki set, an hour-plus) can be analysed afterwards to
//! see which component throttles wall time — CPU, IO, the memory budget, or a lack
//! of parallelism.
//!
//! Shape: when enabled (`--diagnostics`), a background thread samples `/proc`
//! counters every `interval` and appends one JSON object per line to a log file.
//! Each sample is *self-describing*: alongside RSS/CPU/IO/threads/PSI it carries
//! the current [`BuildMemo`] — the coarse `phase`, the fine `op`, an `op_detail`,
//! and `progress_done`/`progress_total` — which the build mutates cheaply as it
//! runs. So a line decodes to e.g. "phase=emit.topology, op=scan edges →
//! topology.csr, 62% (56.4M/91M edges), workers=8, write 110 MB/s".
//!
//! When **disabled** (the default) the whole thing is an `Option::None`: no thread,
//! no file, no timers — every method is an inert early-return, mirroring the
//! server's `Diagnostics::new(false)` model.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::json;

const PAGE_SIZE: u64 = 4096;
/// `sysconf(_SC_CLK_TCK)` is 100 on every Linux we target; hard-coded to avoid a
/// libc dependency (matches the server crate's `diag.rs`).
const CLK_TCK: f64 = 100.0;
/// Throttle: hot loops bump `progress_done` only every Nth item to keep the store
/// off the critical path even at 91M iterations.
const PROGRESS_STRIDE: u64 = 1 << 16;

// ── shared "what's happening now" memo ───────────────────────────────────────

/// Lock-light context the build updates as it progresses and the sampler reads on
/// every tick. Coarse string fields change only at stage boundaries (a `Mutex` is
/// fine — never touched per item); the per-item counter is a relaxed atomic.
#[derive(Default)]
struct BuildMemo {
    phase: Mutex<String>,
    op: Mutex<String>,
    op_detail: Mutex<String>,
    progress_unit: Mutex<String>,
    progress_done: AtomicU64,
    progress_total: AtomicU64,
    active_workers: AtomicU64,
    /// Peak RSS seen since the current phase started (sampler `fetch_max`es it).
    rss_peak: AtomicU64,
}

impl BuildMemo {
    fn set_str(slot: &Mutex<String>, v: &str) {
        if let Ok(mut g) = slot.lock() {
            g.clear();
            g.push_str(v);
        }
    }
    fn get_str(slot: &Mutex<String>) -> String {
        slot.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

// ── the handle threaded through the build ────────────────────────────────────

struct Inner {
    start: Instant,
    interval: Duration,
    memo: BuildMemo,
    writer: Mutex<BufWriter<File>>,
    stop: AtomicBool,
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Append one JSON record + newline; best-effort (a write error must never
    /// derail a build).
    fn write_value(&self, v: &serde_json::Value) {
        if let Ok(mut w) = self.writer.lock() {
            let _ = serde_json::to_writer(&mut *w, v);
            let _ = w.write_all(b"\n");
            let _ = w.flush();
        }
    }

    fn write_sample(&self, s: &Sample) {
        if let Ok(mut w) = self.writer.lock() {
            let _ = serde_json::to_writer(&mut *w, s);
            let _ = w.write_all(b"\n");
            let _ = w.flush();
        }
    }
}

/// Opt-in build diagnostics. `disabled()` is the inert default; `new()` opens the
/// log and spawns the sampler. Cheap, no-op methods when disabled.
pub struct BuildDiag {
    inner: Option<Arc<Inner>>,
    /// Only the owner holds the sampler handle; joining it in `finish()` drops the
    /// sampler's `Arc<Inner>` and so breaks the would-be reference cycle.
    sampler: Option<JoinHandle<()>>,
}

impl BuildDiag {
    /// Inert diagnostics — every method early-returns. Use for the off path and
    /// for in-crate callers (tests) that don't want a log.
    pub fn disabled() -> Self {
        Self {
            inner: None,
            sampler: None,
        }
    }

    /// Open `log_path`, write the `header` record, and spawn the sampler thread.
    /// `header_fields` is merged into the header record (host + build options).
    pub fn new(
        log_path: &Path,
        interval: Duration,
        header_fields: serde_json::Value,
    ) -> anyhow::Result<Self> {
        let file = File::create(log_path)?;
        let inner = Arc::new(Inner {
            start: Instant::now(),
            interval,
            memo: BuildMemo::default(),
            writer: Mutex::new(BufWriter::new(file)),
            stop: AtomicBool::new(false),
        });

        // Header: host facts + whatever the caller passed (build options, etc.).
        let mut header = json!({
            "kind": "header",
            "t_ms": 0u64,
            "interval_ms": interval.as_millis() as u64,
            "page_size": PAGE_SIZE,
            "online_cores": std::thread::available_parallelism().map(|n| n.get() as u64).ok(),
            "cgroup_mem_limit_bytes": cgroup_mem_limit(),
            "cgroup_cpu_quota_cores": cgroup_cpu_quota_cores(),
        });
        if let (Some(obj), Some(extra)) = (header.as_object_mut(), header_fields.as_object()) {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
        inner.write_value(&header);

        let sampler_inner = Arc::clone(&inner);
        let sampler = std::thread::Builder::new()
            .name("slater-build-diag".into())
            .spawn(move || sampler_loop(sampler_inner))?;

        Ok(Self {
            inner: Some(inner),
            sampler: Some(sampler),
        })
    }

    /// Whether diagnostics are live (used to skip building expensive detail strings
    /// on the off path).
    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Open a phase: records `phase_start`, sets the memo phase, and returns a guard
    /// that records `phase_end` (with raw elapsed/CPU/IO/RSS-peak deltas) on drop.
    pub fn phase(&self, name: &str) -> PhaseGuard<'_> {
        if let Some(inner) = &self.inner {
            BuildMemo::set_str(&inner.memo.phase, name);
            // Reset per-phase progress + peak so a new phase starts clean.
            inner.memo.progress_done.store(0, Ordering::Relaxed);
            inner.memo.progress_total.store(0, Ordering::Relaxed);
            inner.memo.active_workers.store(1, Ordering::Relaxed);
            inner
                .memo
                .rss_peak
                .store(rss_bytes().unwrap_or(0), Ordering::Relaxed);
            let base = Baseline {
                t: Instant::now(),
                cpu: cpu_seconds_total(),
                io: io_counters(),
            };
            inner.write_value(&json!({
                "kind": "phase_start",
                "t_ms": inner.now_ms(),
                "phase": name,
            }));
            PhaseGuard {
                inner: Some(inner),
                name: name.to_string(),
                base,
            }
        } else {
            PhaseGuard {
                inner: None,
                name: String::new(),
                base: Baseline::empty(),
            }
        }
    }

    /// Set the current fine operation, its progress unit, and total workload (0 if
    /// unknown). Resets `progress_done` to 0.
    pub fn set_op(&self, op: &str, unit: &str, total: u64) {
        if let Some(inner) = &self.inner {
            BuildMemo::set_str(&inner.memo.op, op);
            BuildMemo::set_str(&inner.memo.progress_unit, unit);
            BuildMemo::set_str(&inner.memo.op_detail, "");
            inner.memo.progress_total.store(total, Ordering::Relaxed);
            inner.memo.progress_done.store(0, Ordering::Relaxed);
        }
    }

    /// Optional free-form detail for the object currently in play (file name, index
    /// name, segment number).
    pub fn set_op_detail(&self, detail: &str) {
        if let Some(inner) = &self.inner {
            BuildMemo::set_str(&inner.memo.op_detail, detail);
        }
    }

    /// Absolute progress within the current op.
    pub fn set_progress(&self, done: u64) {
        if let Some(inner) = &self.inner {
            inner.memo.progress_done.store(done, Ordering::Relaxed);
        }
    }

    /// Add `n` to the current op's progress with an atomic `fetch_add`. Unlike
    /// [`set_progress`]/[`tick`] (plain stores), this is safe to call concurrently
    /// from many worker threads, each contributing the work it finished.
    pub fn progress_add(&self, n: u64) {
        if let Some(inner) = &self.inner {
            inner.memo.progress_done.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Bump progress by `n`, but only touch the atomic on a stride boundary so a
    /// 91M-iteration loop calling this per item stays cheap. `counter` is the
    /// loop's own running total.
    #[inline]
    pub fn tick(&self, counter: u64) {
        if let Some(inner) = &self.inner {
            if counter & (PROGRESS_STRIDE - 1) == 0 {
                inner.memo.progress_done.store(counter, Ordering::Relaxed);
            }
        }
    }

    /// How many parallel workers/tasks the build believes are live right now.
    pub fn set_active_workers(&self, n: u64) {
        if let Some(inner) = &self.inner {
            inner.memo.active_workers.store(n, Ordering::Relaxed);
        }
    }

    /// Ad-hoc structured event (e.g. "segment flushed"). `fields` is merged in.
    pub fn event(&self, msg: &str, fields: serde_json::Value) {
        if let Some(inner) = &self.inner {
            let mut rec = json!({
                "kind": "event",
                "t_ms": inner.now_ms(),
                "phase": BuildMemo::get_str(&inner.memo.phase),
                "msg": msg,
            });
            if let (Some(obj), Some(extra)) = (rec.as_object_mut(), fields.as_object()) {
                for (k, v) in extra {
                    obj.insert(k.clone(), v.clone());
                }
            }
            inner.write_value(&rec);
        }
    }

    /// Stop the sampler, write the `footer`, and flush. Idempotent.
    pub fn finish(&mut self) {
        if let Some(inner) = &self.inner {
            inner.stop.store(true, Ordering::Relaxed);
        }
        if let Some(h) = self.sampler.take() {
            let _ = h.join();
        }
        if let Some(inner) = &self.inner {
            inner.write_value(&json!({
                "kind": "footer",
                "t_ms": inner.now_ms(),
                "total_ms": inner.now_ms(),
            }));
            if let Ok(mut w) = inner.writer.lock() {
                let _ = w.flush();
            }
        }
    }
}

impl Drop for BuildDiag {
    fn drop(&mut self) {
        // Safety net if the caller forgot finish(): stop + join so we don't leak
        // the sampler thread.
        self.finish();
    }
}

struct Baseline {
    t: Instant,
    cpu: Option<f64>,
    io: Option<IoCounters>,
}

impl Baseline {
    fn empty() -> Self {
        Self {
            t: Instant::now(),
            cpu: None,
            io: None,
        }
    }
}

/// RAII phase timer. Records `phase_end` with raw deltas on drop.
pub struct PhaseGuard<'a> {
    inner: Option<&'a Arc<Inner>>,
    name: String,
    base: Baseline,
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        let Some(inner) = self.inner else { return };
        let elapsed_ms = self.base.t.elapsed().as_millis() as u64;
        let cpu_delta = match (cpu_seconds_total(), self.base.cpu) {
            (Some(now), Some(then)) => Some(now - then),
            _ => None,
        };
        let now_io = io_counters();
        let (io_r, io_w) = match (&now_io, &self.base.io) {
            (Some(now), Some(then)) => (
                Some(now.read_bytes.saturating_sub(then.read_bytes)),
                Some(now.write_bytes.saturating_sub(then.write_bytes)),
            ),
            _ => (None, None),
        };
        let rss_peak = inner.memo.rss_peak.load(Ordering::Relaxed);
        inner.write_value(&json!({
            "kind": "phase_end",
            "t_ms": inner.now_ms(),
            "phase": self.name,
            "elapsed_ms": elapsed_ms,
            "cpu_seconds_delta": cpu_delta,
            "io_read_bytes_delta": io_r,
            "io_write_bytes_delta": io_w,
            "rss_peak_bytes": if rss_peak == 0 { None } else { Some(rss_peak) },
        }));
    }
}

// ── the sampler thread ───────────────────────────────────────────────────────

fn sampler_loop(inner: Arc<Inner>) {
    // Wake on a short cadence so stop is prompt, but only sample every `interval`.
    let tick = Duration::from_millis(50).min(inner.interval);
    let mut last_sample = Instant::now()
        .checked_sub(inner.interval)
        .unwrap_or_else(Instant::now);
    let mut prev: Option<(Instant, f64, IoCounters)> = None;

    loop {
        if inner.stop.load(Ordering::Relaxed) {
            break;
        }
        if last_sample.elapsed() >= inner.interval {
            last_sample = Instant::now();
            let s = collect_sample(&inner, &mut prev);
            inner.write_sample(&s);
        }
        std::thread::sleep(tick);
    }
}

fn collect_sample(inner: &Inner, prev: &mut Option<(Instant, f64, IoCounters)>) -> Sample {
    let now = Instant::now();
    let rss = rss_bytes();
    if let Some(r) = rss {
        inner.memo.rss_peak.fetch_max(r, Ordering::Relaxed);
    }
    let cpu = cpu_seconds_total();
    let io = io_counters();

    // Derived (raw arithmetic, not a verdict): %CPU and IO rates since the last
    // sample.
    let (mut cpu_pct, mut read_bps, mut write_bps) = (None, None, None);
    if let (Some((pt, pcpu, pio)), Some(cpu), Some(io)) = (prev.as_ref(), cpu, io.as_ref()) {
        let dt = now.duration_since(*pt).as_secs_f64();
        if dt > 0.0 {
            cpu_pct = Some((cpu - pcpu) / dt * 100.0);
            read_bps = Some(((io.read_bytes.saturating_sub(pio.read_bytes)) as f64 / dt) as u64);
            write_bps = Some(((io.write_bytes.saturating_sub(pio.write_bytes)) as f64 / dt) as u64);
        }
    }
    if let (Some(cpu), Some(io)) = (cpu, io.as_ref()) {
        *prev = Some((now, cpu, io.clone()));
    }

    let done = inner.memo.progress_done.load(Ordering::Relaxed);
    let total = inner.memo.progress_total.load(Ordering::Relaxed);
    let ctxt = ctxt_switches();

    Sample {
        kind: "sample",
        t_ms: inner.now_ms(),
        phase: BuildMemo::get_str(&inner.memo.phase),
        op: BuildMemo::get_str(&inner.memo.op),
        op_detail: BuildMemo::get_str(&inner.memo.op_detail),
        progress_unit: BuildMemo::get_str(&inner.memo.progress_unit),
        progress_done: done,
        progress_total: total,
        progress_pct: (total > 0).then(|| done as f64 / total as f64 * 100.0),
        active_workers: inner.memo.active_workers.load(Ordering::Relaxed),
        rss_bytes: rss,
        cgroup_mem_current_bytes: cgroup_mem_current(),
        cgroup_mem_limit_bytes: cgroup_mem_limit(),
        cpu_seconds_total: cpu,
        cpu_pct,
        num_threads: num_threads(),
        io: io.clone(),
        read_bytes_per_sec: read_bps,
        write_bytes_per_sec: write_bps,
        voluntary_ctxt_switches: ctxt.map(|c| c.0),
        nonvoluntary_ctxt_switches: ctxt.map(|c| c.1),
        psi_cpu: psi_some_avg10("cpu"),
        psi_io: psi_some_avg10("io"),
        psi_mem: psi_some_avg10("memory"),
    }
}

// ── the sample record ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Sample {
    kind: &'static str,
    t_ms: u64,
    // memo (what was running)
    phase: String,
    op: String,
    op_detail: String,
    progress_unit: String,
    progress_done: u64,
    progress_total: u64,
    progress_pct: Option<f64>,
    active_workers: u64,
    // memory
    rss_bytes: Option<u64>,
    cgroup_mem_current_bytes: Option<u64>,
    cgroup_mem_limit_bytes: Option<u64>,
    // cpu
    cpu_seconds_total: Option<f64>,
    cpu_pct: Option<f64>,
    num_threads: Option<u64>,
    // io
    #[serde(flatten)]
    io: Option<IoCounters>,
    read_bytes_per_sec: Option<u64>,
    write_bytes_per_sec: Option<u64>,
    // contention / stall
    voluntary_ctxt_switches: Option<u64>,
    nonvoluntary_ctxt_switches: Option<u64>,
    psi_cpu: Option<f64>,
    psi_io: Option<f64>,
    psi_mem: Option<f64>,
}

// ── /proc samplers (best-effort, read-only; None on non-Linux / sandboxed) ────

#[derive(Clone, Serialize)]
struct IoCounters {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
    syscr: u64,
    syscw: u64,
}

/// Resident set size in bytes, from `/proc/self/statm` (field 2, resident pages).
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * PAGE_SIZE)
}

/// Cumulative CPU seconds (user+system) from `/proc/self/stat` fields 14/15. The
/// `comm` field can contain spaces/parens, so split after the trailing `)`.
fn cpu_seconds_total() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    parse_cpu_seconds(&stat)
}

fn parse_cpu_seconds(stat: &str) -> Option<f64> {
    let after = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / CLK_TCK)
}

/// Thread count: `/proc/self/stat` field 20 (`num_threads`), index 17 after `)`.
fn num_threads() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    parse_num_threads(&stat)
}

fn parse_num_threads(stat: &str) -> Option<u64> {
    let after = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    fields.get(17)?.parse().ok()
}

/// Parse `/proc/self/io` (rchar/wchar/read_bytes/write_bytes/syscr/syscw).
fn io_counters() -> Option<IoCounters> {
    let s = std::fs::read_to_string("/proc/self/io").ok()?;
    parse_io(&s)
}

fn parse_io(s: &str) -> Option<IoCounters> {
    let mut c = IoCounters {
        rchar: 0,
        wchar: 0,
        read_bytes: 0,
        write_bytes: 0,
        syscr: 0,
        syscw: 0,
    };
    let mut any = false;
    for line in s.lines() {
        let (k, v) = match line.split_once(':') {
            Some(kv) => kv,
            None => continue,
        };
        let v: u64 = match v.trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        any = true;
        match k.trim() {
            "rchar" => c.rchar = v,
            "wchar" => c.wchar = v,
            "read_bytes" => c.read_bytes = v,
            "write_bytes" => c.write_bytes = v,
            "syscr" => c.syscr = v,
            "syscw" => c.syscw = v,
            _ => {}
        }
    }
    any.then_some(c)
}

/// Voluntary / nonvoluntary context switches from `/proc/self/status`.
fn ctxt_switches() -> Option<(u64, u64)> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_ctxt_switches(&s)
}

fn parse_ctxt_switches(s: &str) -> Option<(u64, u64)> {
    let mut vol = None;
    let mut nonvol = None;
    for line in s.lines() {
        if let Some(v) = line.strip_prefix("voluntary_ctxt_switches:") {
            vol = v.trim().parse().ok();
        } else if let Some(v) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
            nonvol = v.trim().parse().ok();
        }
    }
    Some((vol?, nonvol?))
}

/// PSI `some avg10` for a resource from `/proc/pressure/<resource>` — the share of
/// the last 10s at least one task was stalled on it. The most direct "what is the
/// process waiting on" signal. `None` when the kernel doesn't expose PSI.
fn psi_some_avg10(resource: &str) -> Option<f64> {
    let s = std::fs::read_to_string(format!("/proc/pressure/{resource}")).ok()?;
    parse_psi_some_avg10(&s)
}

fn parse_psi_some_avg10(s: &str) -> Option<f64> {
    let line = s.lines().find(|l| l.starts_with("some"))?;
    let tok = line
        .split_whitespace()
        .find_map(|t| t.strip_prefix("avg10="))?;
    tok.parse().ok()
}

fn read_cgroup_u64(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    let s = s.trim();
    if s == "max" {
        return None;
    }
    s.parse().ok()
}

/// cgroup memory limit: v2 `memory.max`, falling back to v1 `memory.limit_in_bytes`.
fn cgroup_mem_limit() -> Option<u64> {
    read_cgroup_u64("/sys/fs/cgroup/memory.max")
        .or_else(|| read_cgroup_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .filter(|&v| v < u64::MAX - PAGE_SIZE)
}

/// cgroup current memory usage: v2 `memory.current`, then v1 `memory.usage_in_bytes`.
fn cgroup_mem_current() -> Option<u64> {
    read_cgroup_u64("/sys/fs/cgroup/memory.current")
        .or_else(|| read_cgroup_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
}

/// cgroup CPU quota in cores: v2 `cpu.max`, falling back to v1 cfs quota/period.
fn cgroup_cpu_quota_cores() -> Option<f64> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut it = s.split_whitespace();
        let quota = it.next()?;
        if quota == "max" {
            return None;
        }
        let quota: f64 = quota.parse().ok()?;
        let period: f64 = it.next().and_then(|p| p.parse().ok()).unwrap_or(100_000.0);
        return (period > 0.0).then_some(quota / period);
    }
    let quota: i64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    if quota <= 0 {
        return None;
    }
    let period: f64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    (period > 0.0).then_some(quota as f64 / period)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_seconds_handles_comm_with_spaces() {
        // utime=field14, stime=field15; comm contains spaces and a paren.
        let stat = "1234 (weird (name) ) R 1 1234 1234 0 -1 0 0 0 0 0 \
                    150 50 0 0 20 0 8 0 0";
        let secs = parse_cpu_seconds(stat).unwrap();
        assert!((secs - 2.0).abs() < 1e-9, "got {secs}"); // (150+50)/100
    }

    #[test]
    fn num_threads_is_field_20() {
        let stat = "1234 (proc) R 1 1234 1234 0 -1 0 0 0 0 0 \
                    150 50 0 0 20 0 8 0 0";
        assert_eq!(parse_num_threads(stat), Some(8));
    }

    #[test]
    fn io_parses_named_counters() {
        let s = "rchar: 100\nwchar: 200\nread_bytes: 4096\nwrite_bytes: 8192\nsyscr: 3\nsyscw: 4\n";
        let io = parse_io(s).unwrap();
        assert_eq!(io.read_bytes, 4096);
        assert_eq!(io.write_bytes, 8192);
        assert_eq!(io.rchar, 100);
        assert_eq!(io.syscw, 4);
    }

    #[test]
    fn io_none_when_empty() {
        assert!(parse_io("").is_none());
    }

    #[test]
    fn ctxt_switches_parsed() {
        let s = "Name:\tx\nvoluntary_ctxt_switches:\t12\nnonvoluntary_ctxt_switches:\t34\n";
        assert_eq!(parse_ctxt_switches(s), Some((12, 34)));
    }

    #[test]
    fn psi_some_avg10_parsed() {
        let s = "some avg10=1.23 avg60=4.56 avg300=7.89 total=42\n\
                 full avg10=0.10 avg60=0.20 avg300=0.30 total=10\n";
        assert_eq!(parse_psi_some_avg10(s), Some(1.23));
    }
}
