// SPDX-License-Identifier: Apache-2.0
//! Load-test diagnostics — a gated, in-process health registry.
//!
//! Slater speaks Bolt, not HTTP, so there is no `/metrics` endpoint to scrape.
//! When `config.loadTestDiagnostics` is on, this module maintains a small set of
//! atomic counters and a latency histogram that the connection accept loop, the
//! per-connection state machine, and `run_query` feed, and answers the
//! `CALL slater.diagnostics()` introspection statement with a live snapshot:
//! process RSS / CPU, the cgroup memory & CPU limits (so a report can name the
//! limiter), connection-cap headroom, and per-reason query-failure tallies.
//!
//! **Gating.** The whole point is that the *normal* hot path is unchanged when
//! the flag is off. Every `record_*` method early-returns on `enabled`, so a
//! disabled [`Diagnostics`] is one predictable branch per call site and touches
//! no atomics — the same shape as the `instrument = tracing::enabled!(DEBUG)`
//! gate already in [`crate::server::run_query`]. All process/cgroup sampling
//! happens **on read** (inside [`Diagnostics::snapshot`]), never on the hot path.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use crate::bolt::packstream::PsValue;

/// Linux page size (statm reports resident memory in pages). Hard-coded to match
/// the existing `/proc/self/statm` sampler in `tests/memory_headline.rs`; every
/// target we run on uses 4 KiB pages.
const PAGE_SIZE: u64 = 4096;
/// Clock ticks per second (`utime`/`stime` in `/proc/self/stat` are in ticks).
/// `sysconf(_SC_CLK_TCK)` is 100 on every Linux we target; hard-coded to avoid a
/// `libc` dependency for one constant.
const CLK_TCK: f64 = 100.0;

/// Upper bounds (milliseconds) of the latency histogram buckets. A sample lands
/// in the first bucket whose bound it does not exceed; anything above the last
/// bound lands in a final overflow bucket (so there are `BUCKETS.len() + 1`
/// counters). Log-spaced 0.1 ms → 60 s to span a healthy point lookup through a
/// timed-out pathological query.
const BUCKETS_MS: [f64; 18] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10_000.0, 30_000.0, 60_000.0,
];

/// A coarse fixed-bucket latency histogram over `total_ms`, updated only when
/// diagnostics are enabled. Percentiles are computed on read by walking the
/// cumulative counts — exact to the bucket boundary, which is all the brown-out
/// detector needs.
#[derive(Default)]
struct LatencyHistogram {
    /// One counter per bucket in [`BUCKETS_MS`] plus a trailing overflow bucket.
    counts: [AtomicU64; BUCKETS_MS.len() + 1],
    /// Total observations and summed milliseconds, for the mean.
    count: AtomicU64,
    sum_ms: AtomicU64,
}

impl LatencyHistogram {
    fn record(&self, ms: f64) {
        let idx = BUCKETS_MS
            .iter()
            .position(|&b| ms <= b)
            .unwrap_or(BUCKETS_MS.len());
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // Round to whole ms for the sum; sub-ms latencies are noise at this scale.
        self.sum_ms
            .fetch_add(ms.round().max(0.0) as u64, Ordering::Relaxed);
    }

    /// The bucket upper bound at the `p`-th percentile (0.0–1.0). Returns the
    /// representative bound (the overflow bucket reports the last finite bound as
    /// a `>=` floor). `None` when no samples have been recorded.
    fn percentile(&self, p: f64) -> Option<f64> {
        let total: u64 = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * p).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            cumulative += c.load(Ordering::Relaxed);
            if cumulative >= target {
                return Some(
                    *BUCKETS_MS
                        .get(i)
                        .unwrap_or(&BUCKETS_MS[BUCKETS_MS.len() - 1]),
                );
            }
        }
        Some(BUCKETS_MS[BUCKETS_MS.len() - 1])
    }

    fn mean_ms(&self) -> Option<f64> {
        let n = self.count.load(Ordering::Relaxed);
        (n > 0).then(|| self.sum_ms.load(Ordering::Relaxed) as f64 / n as f64)
    }
}

/// The in-process diagnostics registry. Held as `Arc<Diagnostics>` in the
/// connection context and shared across all connections.
#[derive(Default)]
pub struct Diagnostics {
    /// When `false`, every `record_*` method is a no-op and `snapshot` is never
    /// reached (the introspection arm errors first). Set once at construction.
    pub enabled: bool,

    // ── connection-level counters ────────────────────────────────────────────
    conn_accepted: AtomicU64,
    conn_rejected_per_ip: AtomicU64,
    conn_rejected_pre_auth: AtomicU64,
    login_timeouts: AtomicU64,
    idle_timeouts: AtomicU64,
    msg_too_large_pre_auth: AtomicU64,
    msg_too_large_auth: AtomicU64,
    auth_failures: AtomicU64,

    // ── query-level counters ─────────────────────────────────────────────────
    queries_started: AtomicU64,
    queries_ok: AtomicU64,
    /// Currently-executing queries (on the blocking pool). Balanced inc/dec, so a
    /// signed counter guards against a transient negative read under races.
    queries_in_flight: AtomicI64,
    fail_budget: AtomicU64,
    fail_global_budget: AtomicU64,
    fail_deadline: AtomicU64,
    fail_shortest_path: AtomicU64,
    fail_parse: AtomicU64,
    fail_other: AtomicU64,

    latency: LatencyHistogram,

    /// Process-start instant, for an uptime row. `Instant` not `SystemTime` — we
    /// only ever report a duration.
    started_at: Option<Instant>,
}

impl Diagnostics {
    /// A registry in the given state. When `enabled` is false the result is inert
    /// (all counters dormant); construction is cheap either way.
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            started_at: enabled.then(Instant::now),
            ..Default::default()
        }
    }

    // ── connection-level record paths (all gated, all O(1) atomics) ──────────

    pub fn record_accepted(&self) {
        if self.enabled {
            self.conn_accepted.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_rejected_per_ip(&self) {
        if self.enabled {
            self.conn_rejected_per_ip.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_rejected_pre_auth(&self) {
        if self.enabled {
            self.conn_rejected_pre_auth.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_login_timeout(&self) {
        if self.enabled {
            self.login_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_idle_timeout(&self) {
        if self.enabled {
            self.idle_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// A reassembled body exceeded the cap. `pre_auth` selects which counter (the
    /// tight pre-`LOGON` cap vs the authenticated cap).
    pub fn record_msg_too_large(&self, pre_auth: bool) {
        if self.enabled {
            let c = if pre_auth {
                &self.msg_too_large_pre_auth
            } else {
                &self.msg_too_large_auth
            };
            c.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn record_auth_failure(&self) {
        if self.enabled {
            self.auth_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ── query-level record paths ─────────────────────────────────────────────

    /// A query is about to execute on the blocking pool.
    pub fn on_query_start(&self) {
        if self.enabled {
            self.queries_started.fetch_add(1, Ordering::Relaxed);
            self.queries_in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// A query finished successfully; `total_ms` is its wall-clock (exec+encode).
    pub fn on_query_ok(&self, total_ms: f64) {
        if self.enabled {
            self.queries_in_flight.fetch_sub(1, Ordering::Relaxed);
            self.queries_ok.fetch_add(1, Ordering::Relaxed);
            self.latency.record(total_ms);
        }
    }
    /// A query failed; classify the `anyhow` error into the right reason counter
    /// by its message (the same string signatures `Failure::from_query_error`
    /// keys on, plus the executor's budget / deadline / shortest-path messages).
    pub fn on_query_err(&self, err: &anyhow::Error) {
        if !self.enabled {
            return;
        }
        self.queries_in_flight.fetch_sub(1, Ordering::Relaxed);
        let m = err.to_string();
        let counter = if m.contains("server-wide intermediate budget") {
            &self.fail_global_budget
        } else if m.contains("intermediate result budget") {
            &self.fail_budget
        } else if m.contains("exceeded its time limit") {
            &self.fail_deadline
        } else if m.contains("shortestPath exceeded the node cap") {
            &self.fail_shortest_path
        } else if m.contains("syntax error") {
            &self.fail_parse
        } else {
            &self.fail_other
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    /// The query task itself failed to join (panic / cancellation). Counts as an
    /// `other` failure and still balances the in-flight gauge.
    pub fn on_query_task_failed(&self) {
        if self.enabled {
            self.queries_in_flight.fetch_sub(1, Ordering::Relaxed);
            self.fail_other.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Render the live snapshot as ordered `(metric, value)` rows. Samples
    /// process RSS/CPU and the cgroup limits **here**, on read — never on the hot
    /// path. `live` carries the connection-semaphore and cache-pool state the
    /// registry does not itself own.
    pub fn snapshot(&self, live: &LiveGauges) -> Vec<(String, PsValue)> {
        let mut rows: Vec<(String, PsValue)> = Vec::new();
        // Local push helpers — macros rather than closures so they don't hold a
        // borrow on `rows` across the whole function.
        macro_rules! int {
            ($n:expr, $v:expr) => {
                rows.push(($n.to_string(), PsValue::Int($v)))
            };
        }
        macro_rules! flt {
            // An `Option<f64>` metric; `None` (unknown / unsupported) renders as -1.
            ($n:expr, $v:expr) => {
                rows.push(($n.to_string(), PsValue::Float($v.unwrap_or(-1.0))))
            };
        }
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed) as i64;

        // ── process / system — the memory & CPU limiter evidence ─────────────
        int!(
            "uptime_ms",
            self.started_at
                .map_or(0, |t| t.elapsed().as_millis() as i64)
        );
        int!("rss_bytes", rss_bytes().map(|v| v as i64).unwrap_or(-1));
        int!(
            "cgroup_mem_current_bytes",
            cgroup_mem_current().map(|v| v as i64).unwrap_or(-1)
        );
        int!(
            "cgroup_mem_limit_bytes",
            cgroup_mem_limit().map(|v| v as i64).unwrap_or(-1)
        );
        // CPU is cumulative seconds; the coordinator diffs successive reads for %.
        flt!("cpu_seconds_total", cpu_seconds_total());
        flt!("cgroup_cpu_quota_cores", cgroup_cpu_quota_cores());

        // ── connection caps: headroom + cumulative rejections ────────────────
        int!("conn_in_use", live.conn_in_use as i64);
        int!("conn_limit", live.conn_limit as i64);
        int!("conn_pre_auth_in_use", live.pre_auth_in_use as i64);
        int!("conn_pre_auth_limit", live.pre_auth_limit as i64);
        int!("conn_max_per_ip", live.max_per_ip as i64);
        int!("conn_accepted_total", load(&self.conn_accepted));
        int!(
            "conn_rejected_per_ip_total",
            load(&self.conn_rejected_per_ip)
        );
        int!(
            "conn_rejected_pre_auth_total",
            load(&self.conn_rejected_pre_auth)
        );
        int!("login_timeouts_total", load(&self.login_timeouts));
        int!("idle_timeouts_total", load(&self.idle_timeouts));
        int!(
            "msg_too_large_pre_auth_total",
            load(&self.msg_too_large_pre_auth)
        );
        int!("msg_too_large_auth_total", load(&self.msg_too_large_auth));
        int!("auth_failures_total", load(&self.auth_failures));

        // ── queries: throughput, in-flight, per-reason failures ──────────────
        int!("queries_started_total", load(&self.queries_started));
        int!("queries_ok_total", load(&self.queries_ok));
        int!(
            "queries_in_flight",
            self.queries_in_flight.load(Ordering::Relaxed).max(0)
        );
        int!("fail_budget_total", load(&self.fail_budget));
        int!("fail_global_budget_total", load(&self.fail_global_budget));
        int!("fail_deadline_total", load(&self.fail_deadline));
        int!("fail_shortest_path_total", load(&self.fail_shortest_path));
        int!("fail_parse_total", load(&self.fail_parse));
        int!("fail_other_total", load(&self.fail_other));

        // ── latency percentiles (ms) over total wall-clock ───────────────────
        flt!("latency_p50_ms", self.latency.percentile(0.50));
        flt!("latency_p95_ms", self.latency.percentile(0.95));
        flt!("latency_p99_ms", self.latency.percentile(0.99));
        flt!("latency_mean_ms", self.latency.mean_ms());

        // ── configured query budgets (echoed for headroom interpretation) ────
        int!("cfg_max_rows", live.max_rows as i64);
        int!("cfg_timeout_ms", live.timeout_ms as i64);
        int!("cfg_max_intermediate", live.max_intermediate as i64);
        int!(
            "cfg_max_intermediate_global",
            live.max_intermediate_global as i64
        );
        int!(
            "intermediate_global_in_use",
            live.intermediate_global_in_use as i64
        );
        int!(
            "intermediate_global_peak",
            live.intermediate_global_peak as i64
        );
        int!(
            "cfg_max_shortest_path_explore",
            live.max_shortest_path_explore as i64
        );
        int!("cfg_max_fanout", live.max_fanout as i64);
        int!("cfg_max_message_bytes", live.max_message_bytes as i64);

        // ── cache pools (residency + hit/miss/eviction pressure) ─────────────
        for (prefix, pool) in [
            ("block", &live.block_cache),
            ("vector", &live.vector_cache),
            ("result", &live.result_cache),
        ] {
            int!(format!("cache_{prefix}_bytes"), pool.bytes as i64);
            int!(format!("cache_{prefix}_entries"), pool.entries as i64);
            int!(format!("cache_{prefix}_hits"), pool.hits as i64);
            int!(format!("cache_{prefix}_misses"), pool.misses as i64);
            int!(format!("cache_{prefix}_evictions"), pool.evictions as i64);
        }

        rows
    }
}

/// Snapshot of one cache pool's live counters, supplied by the caller (which owns
/// the cache handles) so [`diag`](self) stays free of cache-type dependencies.
#[derive(Default, Clone, Copy)]
pub struct CachePoolSnapshot {
    pub bytes: u64,
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Live state the registry does not itself own — connection-semaphore occupancy,
/// configured caps, and the three cache pools — passed into [`Diagnostics::snapshot`].
pub struct LiveGauges {
    pub conn_in_use: u64,
    pub conn_limit: u64,
    pub pre_auth_in_use: u64,
    pub pre_auth_limit: u64,
    pub max_per_ip: u64,
    pub max_rows: u64,
    pub timeout_ms: u64,
    pub max_intermediate: u64,
    /// Server-wide intermediate budget: the ceiling and its live/peak occupancy
    /// (`query.maxIntermediateGlobal`). 0 limit ⇒ the guard is disabled.
    pub max_intermediate_global: u64,
    pub intermediate_global_in_use: u64,
    pub intermediate_global_peak: u64,
    pub max_shortest_path_explore: u64,
    pub max_fanout: u64,
    pub max_message_bytes: u64,
    pub block_cache: CachePoolSnapshot,
    pub vector_cache: CachePoolSnapshot,
    pub result_cache: CachePoolSnapshot,
}

// ── process / cgroup sampling (best-effort, read-only, on snapshot) ──────────

/// Resident set size of this process in bytes, from `/proc/self/statm` (field 2,
/// resident pages). `None` if the file is unreadable (non-Linux / sandboxed).
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * PAGE_SIZE)
}

/// Cumulative CPU seconds (user+system) for this process, from `/proc/self/stat`
/// fields 14 (`utime`) and 15 (`stime`), in clock ticks. The leading `comm`
/// field can contain spaces/parens, so we split after the trailing `)`.
fn cpu_seconds_total() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after = stat.rsplit_once(')')?.1; // everything past "(comm)"
    let fields: Vec<&str> = after.split_whitespace().collect();
    // After the ')' split, field index 0 is `state`; `utime` is field 14 and
    // `stime` field 15 in the 1-based proc(5) layout, i.e. indices 11 and 12 here.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / CLK_TCK)
}

/// Read a single unsigned value from a cgroup file, treating `"max"` (cgroup v2
/// unlimited) as `None`.
fn read_cgroup_u64(path: &str) -> Option<u64> {
    let s = std::fs::read_to_string(path).ok()?;
    let s = s.trim();
    if s == "max" {
        return None;
    }
    s.parse().ok()
}

/// cgroup memory limit in bytes: cgroup v2 `memory.max`, falling back to v1
/// `memory.limit_in_bytes`. `None` when unlimited or unreadable.
fn cgroup_mem_limit() -> Option<u64> {
    read_cgroup_u64("/sys/fs/cgroup/memory.max")
        .or_else(|| read_cgroup_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        // v1 "unlimited" is a huge sentinel (~u64::MAX rounded to a page); treat
        // anything within a page of u64::MAX as no limit.
        .filter(|&v| v < u64::MAX - PAGE_SIZE)
}

/// cgroup current memory usage in bytes: v2 `memory.current`, then v1
/// `memory.usage_in_bytes`.
fn cgroup_mem_current() -> Option<u64> {
    read_cgroup_u64("/sys/fs/cgroup/memory.current")
        .or_else(|| read_cgroup_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
}

/// cgroup CPU quota expressed in cores: v2 `cpu.max` (`"<quota> <period>"`, or
/// `"max"` for unlimited), falling back to v1 `cpu.cfs_quota_us` /
/// `cpu.cfs_period_us`. `None` when unlimited or unreadable.
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
    // cgroup v1.
    let quota: i64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    if quota <= 0 {
        return None; // -1 == unlimited
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
    fn disabled_registry_records_nothing() {
        let d = Diagnostics::new(false);
        d.on_query_start();
        d.on_query_ok(12.0);
        d.record_accepted();
        d.record_rejected_per_ip();
        let live = LiveGauges {
            conn_in_use: 0,
            conn_limit: 16,
            pre_auth_in_use: 0,
            pre_auth_limit: 4,
            max_per_ip: 8,
            max_rows: 100,
            timeout_ms: 30_000,
            max_intermediate: 1_000_000,
            max_intermediate_global: 8_000_000,
            intermediate_global_in_use: 0,
            intermediate_global_peak: 0,
            max_shortest_path_explore: 0,
            max_fanout: 1,
            max_message_bytes: 64,
            block_cache: CachePoolSnapshot::default(),
            vector_cache: CachePoolSnapshot::default(),
            result_cache: CachePoolSnapshot::default(),
        };
        let rows = d.snapshot(&live);
        let get = |k: &str| rows.iter().find(|(n, _)| n == k).map(|(_, v)| v).cloned();
        // Counters stayed at zero despite the record calls above.
        assert_eq!(get("queries_started_total"), Some(PsValue::Int(0)));
        assert_eq!(get("conn_accepted_total"), Some(PsValue::Int(0)));
        // Live caps still echo through (they come from `live`, not the counters).
        assert_eq!(get("conn_limit"), Some(PsValue::Int(16)));
    }

    #[test]
    fn enabled_registry_counts_and_classifies() {
        let d = Diagnostics::new(true);
        d.record_accepted();
        d.record_accepted();
        d.on_query_start();
        d.on_query_ok(5.0);
        d.on_query_start();
        d.on_query_err(&anyhow::anyhow!(
            "query exceeded the intermediate result budget of 1000000 elements (query.maxIntermediate)"
        ));
        d.on_query_start();
        d.on_query_err(&anyhow::anyhow!("query exceeded its time limit"));

        assert_eq!(d.conn_accepted.load(Ordering::Relaxed), 2);
        assert_eq!(d.queries_started.load(Ordering::Relaxed), 3);
        assert_eq!(d.queries_ok.load(Ordering::Relaxed), 1);
        assert_eq!(d.fail_budget.load(Ordering::Relaxed), 1);
        assert_eq!(d.fail_deadline.load(Ordering::Relaxed), 1);
        // Three started, three finished ⇒ none in flight.
        assert_eq!(d.queries_in_flight.load(Ordering::Relaxed), 0);
        // A latency sample was recorded for the one OK query.
        assert!(d.latency.percentile(0.5).is_some());
    }

    #[test]
    fn histogram_percentiles_track_distribution() {
        let h = LatencyHistogram::default();
        for _ in 0..99 {
            h.record(1.0); // lands in the 1.0 ms bucket
        }
        h.record(5000.0); // one slow sample
                          // p50 sits in the fast bucket, p99/p100 reach the slow one.
        assert_eq!(h.percentile(0.50), Some(1.0));
        assert_eq!(h.percentile(0.99), Some(1.0));
        assert_eq!(h.percentile(1.0), Some(5000.0));
    }
}
