// SPDX-License-Identifier: Apache-2.0
//! Delta maintenance and the consolidation trigger.
//!
//! Split out of `server.rs` as a child module (a pure relocation). Shared types,
//! consts and helpers stay in the parent, reached via `use super::*`; the parent
//! re-exports this module's items so sibling modules can call them by name.

use super::*;

/// Post-write delta maintenance — the write path's self-tuning (Phase 4d-ii). Three
/// tiers, cheapest first:
///
/// 1. **Flush** the active memtable to an L0 segment when it exceeds `memtableBytes`.
/// 2. **Compact** the L0 stack when it exceeds `l0CompactionTrigger` levels (4d-i).
/// 3. **Consolidate** — fire a *background* full rebuild when the delta reaches
///    `deltaCorePercent`% of the core's entity count (4d-ii-b): rare because it is
///    O(core), triggered as a fraction of core so write amplification stays bounded.
///
/// Flush + compaction are cheap (O(delta), fsync only) and run on a blocking thread;
/// neither can fail the write (it already acked durably), so an error is logged and
/// swallowed. Both are skipped while a consolidation owns the L0 stack. The
/// consolidation is spawned detached — it must not block the ack. Finally, if the delta
/// has blown past the `deltaHardBytes` **hard cap**, the write **throttles**: it ensures
/// a drain is running and waits for headroom before returning (the OOM backstop).
pub(crate) async fn maybe_maintain_delta(
    ctx: &Arc<ConnCtx>,
    graph: &str,
    writer: &Arc<DeltaWriter>,
) {
    if !writer.is_consolidating() {
        if ctx.memtable_bytes > 0 && writer.bytes() >= ctx.memtable_bytes {
            let w = writer.clone();
            match tokio::task::spawn_blocking(move || w.flush_to_l0()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta flush_to_l0 failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta flush task panicked"),
            }
        }
        if ctx.l0_compaction_trigger > 0 && writer.l0_len() >= ctx.l0_compaction_trigger {
            let w = writer.clone();
            match tokio::task::spawn_blocking(move || w.compact_l0()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta compact_l0 failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta compaction task panicked"),
            }
        }

        // ── Segment-tier maintenance (Phase 6 closing slice): the two upper rungs of
        // the D50 ladder, beside the L0-internal rungs above. Safe to auto-fire now the
        // 6.1 segment-aware write resolve is in — a concurrent re-MERGE of a just-flushed
        // key resolves through the new segment instead of duplicating. Both take the
        // `begin_consolidation` claim inside `Graphs` (so they never overlap each other or
        // a consolidation) and run on a blocking pool; a lost single-flight race bails as
        // "already in progress" (logged at debug, not warn). The L0 rungs above still run
        // regardless, so the memtable always drains even if a flush bails — the T2 flush's
        // extra L0 write before it folds the whole delta is the cheap price of that.

        // T2: once the WHOLE delta (memtable + every L0 level) reaches `segmentFlushBytes`,
        // fold it into one durable core segment — the O(delta) drain that keeps the delta
        // small without an O(core) consolidation. Off by default (0). Fires for a resident or
        // an off-heap L0 stack alike (the off-heap fold is `flush_segment_data`, Phase 7.5).
        if ctx.segment_flush_bytes > 0 && writer.total_bytes() >= ctx.segment_flush_bytes {
            let (g, c) = (graph.to_string(), ctx.clone());
            match tokio::task::spawn_blocking(move || {
                c.graphs
                    .flush_graph_to_segment(&g, &c.vector_cache, &c.data_dir)
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment flush skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta flush_to_segment failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta flush_to_segment task panicked"),
            }
        }

        // T3: once the served set carries more than `maxUpperSegments` upper segments,
        // fold a contiguous run (the size-tiered selector picks it — self-gating, so the
        // auto entry point is a true no-op when within budget). Pre-checked on the resident
        // segment count (the selector's own admission predicate) so no blocking task is
        // spawned per write. Runs after the T2 flush so a freshly appended segment that
        // tips the stack over budget folds in the same pass.
        let over_segment_budget = ctx.max_upper_segments > 0
            && ctx
                .graphs
                .get(graph)
                .map(|gen| gen.stack().segments().len() > ctx.max_upper_segments)
                .unwrap_or(false);
        let mut compacted = false;
        if over_segment_budget {
            let (g, c) = (graph.to_string(), ctx.clone());
            let max_upper = ctx.max_upper_segments;
            match tokio::task::spawn_blocking(move || {
                c.graphs
                    .compact_graph_segments_auto(&g, &c.vector_cache, &c.data_dir, max_upper)
            })
            .await
            {
                // `Some` means a run actually folded — its old segment dirs + the superseded
                // set are now orphaned, so a GC sweep below has something to reclaim.
                Ok(Ok(folded)) => compacted = folded.is_some(),
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment compaction skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "segment compaction failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "segment compaction task panicked"),
            }
        }

        // T4 GC (Phase 7 slice 7.2): after a compaction folds a run, reclaim its now-orphaned
        // segment dirs + the superseded set — only when a fold happened (so it is not paid per
        // write) and GC is enabled (`segmentGcGraceSecs > 0`). The sweep takes its own
        // `begin_consolidation` claim (the compaction already released it); a lost race is
        // benign (another op holds it — it will re-observe the orphans on a later write).
        if compacted && ctx.segment_gc_grace_secs > 0 {
            let (g, c) = (graph.to_string(), ctx.clone());
            let grace = ctx.segment_gc_grace_secs;
            match tokio::task::spawn_blocking(move || {
                c.graphs.gc_orphan_segments(&g, &c.data_dir, grace)
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment GC skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "segment GC after compaction failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "segment GC task panicked"),
            }
        }
    }

    // Background consolidation at a fraction of the core's size (4d-ii-b). Spawned
    // detached so the ack never waits on the O(core) rebuild; 4a keeps writes that
    // arrive during it safe. `begin_consolidation` inside `consolidate_graph` is the
    // real single-flight guard — the pre-check only avoids a spurious spawn.
    if ctx.delta_core_percent > 0 && !writer.is_consolidating() {
        if let Some(gen) = ctx.graphs.get(graph) {
            let core_entities = gen.node_count() + gen.edge_count();
            if consolidation_due(
                core_entities,
                writer.delta_entity_count() as u64,
                ctx.delta_core_percent,
            ) {
                // Defer to the off-peak window if one is configured (the hard-cap
                // throttle below still fires anytime as the OOM backstop).
                if window_permits(&ctx.consolidate_window, crate::cron_window::local_now_hms()) {
                    spawn_auto_consolidation(ctx.clone(), graph.to_string());
                } else {
                    debug!(
                        graph = %graph,
                        "auto-consolidation is due but deferred — outside the configured off-peak window"
                    );
                }
            }
        }
    }

    // Hard-cap throttle (runs even during a consolidation — waiting for it is the
    // point). The OOM backstop: block this write until the delta drains below the cap.
    if ctx.delta_hard_bytes > 0 && writer.total_bytes() >= ctx.delta_hard_bytes {
        throttle_until_drained(ctx, graph, writer).await;
    }
}

/// Whether the delta has grown to `percent`% of the core's entity count — the
/// fraction-of-core auto-consolidation predicate (Phase 4d-ii-b). `false` when
/// disabled (`percent == 0`), the core is empty, or the rounded threshold is 0 (a
/// core too small for this percent to mean one whole entity). `u128` maths avoids
/// overflow on a large core.
pub(crate) fn consolidation_due(core_entities: u64, delta_entities: u64, percent: usize) -> bool {
    if percent == 0 || core_entities == 0 {
        return false;
    }
    let threshold = (core_entities as u128 * percent as u128 / 100) as u64;
    threshold > 0 && delta_entities >= threshold
}

/// Whether the off-peak window (if any) permits a fraction-triggered consolidation at
/// the given server-local time `(hour, day-of-month, month, day-of-week)`. `None` window
/// ⇒ always permitted. Pure over the supplied time so it is testable without a clock —
/// the caller reads the real clock via [`crate::cron_window::local_now_hms`]. The
/// hard-cap throttle never consults this (it is the OOM backstop, fires anytime).
pub(crate) fn window_permits(
    window: &Option<crate::cron_window::CronWindow>,
    (hour, dom, month, dow): (u32, u32, u32, u32),
) -> bool {
    match window {
        None => true,
        Some(w) => w.contains(hour, dom, month, dow),
    }
}

/// Fire a background consolidation for `graph`, detached from the write that triggered
/// it. Reuses the `execute_consolidate` path (dump → builder → swap → retire on a
/// blocking thread). A lost single-flight race (another consolidation already claimed
/// the writer) surfaces as a benign "already in progress" and is logged at debug, not
/// warn.
pub(crate) fn spawn_auto_consolidation(ctx: Arc<ConnCtx>, graph: String) {
    tokio::spawn(async move {
        match execute_consolidate(&ctx, &graph).await {
            Ok(_) => info!(graph = %graph, "auto-consolidation folded the delta into a fresh core"),
            Err(e) if e.code == CODE_CONSOLIDATION_IN_PROGRESS => {
                debug!(graph = %graph, "auto-consolidation skipped: one is already running")
            }
            Err(e) => warn!(graph = %graph, error = %e.message, "auto-consolidation failed"),
        }
    });
}

/// Block the calling write until the delta drains below the `deltaHardBytes` hard cap
/// (Phase 4d-ii-b). Ensures a consolidation is draining (kicking one if none is), then
/// awaits headroom. The await yields the reactor thread, so other connections proceed;
/// a client whose write blocks too long times out — the correct "server saturated"
/// signal. Re-kicks if a drain finishes/fails without clearing the cap, and bails after
/// a generous bound so a wedged consolidation cannot hang a writer forever (logged
/// loudly — for a very large core whose rebuild exceeds the window, the hard cap is
/// advisory; the fraction-of-core trigger is what keeps the delta from getting there).
pub(crate) async fn throttle_until_drained(
    ctx: &Arc<ConnCtx>,
    graph: &str,
    writer: &Arc<DeltaWriter>,
) {
    use std::time::Duration;
    const STEP_MS: u64 = 50;
    const MAX_WAIT_MS: u64 = 10 * 60 * 1000;
    warn!(
        graph = %graph,
        delta_bytes = writer.total_bytes(),
        hard_cap = ctx.delta_hard_bytes,
        "delta hard cap reached — throttling the writer until a consolidation drains it"
    );
    let mut waited_ms = 0u64;
    while writer.total_bytes() >= ctx.delta_hard_bytes {
        if !writer.is_consolidating() {
            spawn_auto_consolidation(ctx.clone(), graph.to_string());
        }
        if waited_ms >= MAX_WAIT_MS {
            warn!(graph = %graph, "delta hard-cap throttle timed out; proceeding over cap");
            return;
        }
        tokio::time::sleep(Duration::from_millis(STEP_MS)).await;
        waited_ms += STEP_MS;
    }
}

/// Execute `CALL slater.consolidate()` (Phase 5): fold the graph's writable delta
/// into a fresh generation and swap it in, returning the new generation's id as a
/// single `generation` column. The heavy work — dumping the merged view, spawning the
/// builder subprocess, validating and swapping the new generation — runs on a blocking
/// thread so the Bolt reactor is never parked on it. Only reached when the writable
/// layer is enabled for this graph (the caller already resolved a `writer`).
pub(crate) async fn execute_consolidate(
    ctx: &Arc<ConnCtx>,
    graph: &str,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let graphs = ctx.graphs.clone();
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let data_dir = ctx.data_dir.clone();
    let builder_bin = ctx.builder_bin.clone();
    let graph = graph.to_string();
    let gc_graph = graph.clone(); // retained for the post-consolidation GC sweep below
    let new_uuid = tokio::task::spawn_blocking(move || {
        graphs.consolidate_graph(&graph, &cache, &vector_cache, &data_dir, |dump, g, dd| {
            run_builder(&builder_bin, dump, g, dd)
        })
    })
    .await
    .map_err(|e| Failure::new(CODE_EXECUTION, format!("consolidation task failed: {e}")))?
    .map_err(|e| {
        // Classify a lost single-flight race here, where the typed `ConsolidationInProgress`
        // cause is still intact — `{e:#}` below flattens it to a string. Callers then branch
        // on the resulting `code`, never on the message text.
        let code = if is_already_in_progress(&e) {
            CODE_CONSOLIDATION_IN_PROGRESS
        } else {
            CODE_EXECUTION
        };
        Failure::new(code, format!("consolidation failed: {e:#}"))
    })?;

    // T4 GC (Phase 7 slice 7.2): a retarget collapses the served set to a singleton, orphaning
    // the whole prior set + every one of its segments. Reclaim them when GC is enabled — a
    // best-effort sweep whose failure never fails the (already-published) consolidation.
    if ctx.segment_gc_grace_secs > 0 {
        let (g, graphs, data_dir) = (gc_graph.clone(), ctx.graphs.clone(), ctx.data_dir.clone());
        let grace = ctx.segment_gc_grace_secs;
        match tokio::task::spawn_blocking(move || graphs.gc_orphan_segments(&g, &data_dir, grace))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if is_already_in_progress(&e) => {
                debug!(graph = %gc_graph, "segment GC skipped: a flush/consolidation is already running")
            }
            Ok(Err(e)) => {
                warn!(graph = %gc_graph, error = %format!("{e:#}"), "segment GC after consolidation failed")
            }
            Err(e) => warn!(graph = %gc_graph, error = %e, "segment GC task panicked"),
        }
    }
    Ok((
        vec!["generation".to_string()],
        vec![vec![PsValue::String(new_uuid.to_string())]],
    ))
}
