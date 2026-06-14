#!/usr/bin/env python3
"""Brown-out coordinator for the slater load-test suite.

Ramps a scenario's load in steps (driving locust headless once per step), polls
`CALL slater.diagnostics()` while the load is applied, and auto-detects the
capacity knee — the point where the service starts degrading — then names the
limiter (memory / cpu / connection-cap / query-budget / message).

The server must be running with `loadTestDiagnostics: true`. To get a meaningful
limiter verdict, run it under a cgroup memory/CPU limit (e.g. systemd-run or a
container) so RSS and CPU have a ceiling to hit.

Example:

    python3 perf/loadtest/coordinator.py --scenario mem_cache_churn \
        --pass polereader --users 100,250,500,1000,2000 --step 45

Needs:  pip install -r perf/loadtest/requirements.txt
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time

from neo4j import GraphDatabase

import scenarios

HERE = os.path.dirname(os.path.abspath(__file__))
LOCUSTFILE = os.path.join(HERE, "locustfile.py")


# ── diagnostics snapshot ──────────────────────────────────────────────────────

def read_diagnostics(driver):
    """Run CALL slater.diagnostics() and return {metric: number}."""
    snap = {}
    with driver.session() as s:
        for rec in s.run("CALL slater.diagnostics()"):
            snap[rec["metric"]] = rec["value"]
    return snap


def g(snap, key, default=-1):
    v = snap.get(key, default)
    return v if v is not None else default


# ── locust step ───────────────────────────────────────────────────────────────

def run_locust_step(args, users, rate, csv_prefix):
    """Launch locust headless for one ramp step; return the Popen handle."""
    env = dict(os.environ)
    env.update({
        "SLATER_URI": args.uri,
        "SLATER_USER": args.user,
        "SLATER_PASS": args.password,
        "SLATER_DB": args.db,
        "SLATER_SCENARIO": args.scenario,
    })
    cmd = [
        sys.executable, "-m", "locust", "-f", LOCUSTFILE, "--headless",
        "-u", str(users), "-r", str(rate), "-t", f"{args.step}s",
        "--host", args.uri, "--csv", csv_prefix, "--only-summary",
    ]
    return subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def parse_locust_stats(csv_prefix):
    """Read the Aggregated row from locust's <prefix>_stats.csv."""
    path = f"{csv_prefix}_stats.csv"
    try:
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    agg = next((r for r in rows if r.get("Name") == "Aggregated"), None)
    if not agg:
        return None

    def num(key, default=0.0):
        try:
            return float(agg.get(key, default))
        except (TypeError, ValueError):
            return default

    reqs = num("Request Count")
    fails = num("Failure Count")
    return {
        "reqs": reqs,
        "fails": fails,
        "fail_frac": (fails / reqs) if reqs else 0.0,
        "rps": num("Requests/s"),
        "p50": num("50%"),
        "p95": num("95%"),
        "p99": num("99%"),
    }


# ── knee detection ────────────────────────────────────────────────────────────

REJECTION_KEYS = ["conn_rejected_per_ip_total", "conn_rejected_pre_auth_total",
                  "login_timeouts_total"]
BUDGET_KEYS = ["fail_budget_total", "fail_deadline_total", "fail_shortest_path_total"]
MESSAGE_KEYS = ["msg_too_large_auth_total", "msg_too_large_pre_auth_total"]


def cores(snap):
    q = g(snap, "cgroup_cpu_quota_cores", -1)
    return q if q and q > 0 else (os.cpu_count() or 1)


def delta(cur, prev, keys):
    return sum(max(0, g(cur, k, 0) - g(prev, k, 0)) for k in keys)


def detect_knee(step, baseline, cur, prev, args):
    """Return (is_knee, reasons[]). `cur`/`prev` are diagnostics snapshots; `step`
    holds this step's locust stats."""
    reasons = []
    p99 = step["p99"]
    base_p99 = baseline["p99"] if baseline else p99

    if base_p99 > 0 and p99 > args.latency_mult * base_p99:
        reasons.append(f"p99 {p99:.0f}ms > {args.latency_mult}x baseline {base_p99:.0f}ms")
    if step["fail_frac"] > args.fail_frac:
        reasons.append(f"failure rate {step['fail_frac']*100:.1f}% > {args.fail_frac*100:.0f}%")

    if prev is not None:
        if delta(cur, prev, REJECTION_KEYS) > 0:
            reasons.append("connection rejections / login timeouts climbing")
        if delta(cur, prev, BUDGET_KEYS) > 0:
            reasons.append("query budget/deadline failures climbing")
        if delta(cur, prev, MESSAGE_KEYS) > 0:
            reasons.append("oversized-message rejections climbing")

    rss = g(cur, "rss_bytes", -1)
    lim = g(cur, "cgroup_mem_limit_bytes", -1)
    if rss > 0 and lim > 0 and rss > args.rss_frac * lim:
        reasons.append(f"RSS {rss/1e6:.0f}MB > {args.rss_frac*100:.0f}% of cgroup "
                       f"limit {lim/1e6:.0f}MB")

    return (len(reasons) > 0, reasons)


def attribute_limiter(scn, cur, prev, wall):
    """Best-effort: which resource is the binding constraint at the knee."""
    rss = g(cur, "rss_bytes", -1)
    lim = g(cur, "cgroup_mem_limit_bytes", -1)
    if rss > 0 and lim > 0 and rss > 0.9 * lim:
        return "memory", f"RSS {rss/1e6:.0f}MB ≈ cgroup limit {lim/1e6:.0f}MB"

    if prev is not None and wall > 0:
        cpu_d = g(cur, "cpu_seconds_total", -1) - g(prev, "cpu_seconds_total", -1)
        util = cpu_d / (wall * cores(cur)) if cpu_d >= 0 else 0
        if util > 0.85:
            return "cpu", f"CPU utilisation ≈ {util*100:.0f}% of {cores(cur):.1f} cores"
        if delta(cur, prev, REJECTION_KEYS) > 0:
            return "connection-cap", "new connection/login rejections this step"
        if delta(cur, prev, BUDGET_KEYS) > 0:
            return "query-budget", "new intermediate/deadline/shortest-path failures"
        if delta(cur, prev, MESSAGE_KEYS) > 0:
            return "message", "new oversized-message rejections"

    # Fall back to the scenario's declared target.
    return scn.targets, "inferred from scenario target (no single resource saturated)"


# ── main ──────────────────────────────────────────────────────────────────────

def fmt_mb(v):
    return f"{v/1e6:.0f}M" if isinstance(v, (int, float)) and v >= 0 else "  n/a"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", required=True, help="see scenarios.py")
    ap.add_argument("--uri", default="bolt://127.0.0.1:7687")
    ap.add_argument("--user", default="reporting")
    ap.add_argument("--password", required=True)
    ap.add_argument("--db", default="pole")
    ap.add_argument("--users", default="50,100,250,500,1000,2000",
                    help="comma-separated ramp steps (concurrent users ≈ connections)")
    ap.add_argument("--rate", type=int, default=100, help="spawn rate (users/s)")
    ap.add_argument("--step", type=int, default=45, help="seconds per ramp step")
    ap.add_argument("--latency-mult", type=float, default=4.0,
                    help="knee if p99 exceeds this multiple of the baseline p99")
    ap.add_argument("--fail-frac", type=float, default=0.02,
                    help="knee if the failure rate exceeds this fraction")
    ap.add_argument("--rss-frac", type=float, default=0.9,
                    help="knee if RSS exceeds this fraction of the cgroup mem limit")
    ap.add_argument("--outdir", default=os.path.join(HERE, "out"))
    ap.add_argument("--keep-going", action="store_true",
                    help="run all ramp steps even after the knee is found")
    args = ap.parse_args()

    scn = scenarios.get(args.scenario)
    os.makedirs(args.outdir, exist_ok=True)
    steps = [int(x) for x in args.users.split(",") if x.strip()]

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()
    # Fail fast with a clear message if diagnostics are not enabled server-side.
    try:
        pre = read_diagnostics(driver)
    except Exception as exc:
        raise SystemExit(
            f"cannot read CALL slater.diagnostics(): {exc}\n"
            "Start slater with loadTestDiagnostics: true.")

    print(f"scenario: {scn.name} — {scn.description}")
    print(f"target area: {scn.targets}; watching: {', '.join(scn.watch)}")
    lim = g(pre, "cgroup_mem_limit_bytes", -1)
    print(f"cgroup mem limit: {fmt_mb(lim)}  |  cpu quota: {cores(pre):.1f} cores\n")

    header = (f"{'users':>6} {'rps':>7} {'p50':>7} {'p99':>8} {'fail%':>6} "
              f"{'rss':>6} {'inflight':>8} {'rejects':>7}  status")
    print(header)
    print("-" * len(header))

    baseline = None
    prev_snap = pre
    knee_at = None
    for users in steps:
        rate = min(args.rate, users)
        prefix = os.path.join(args.outdir, f"{args.scenario}_{users}")
        proc = run_locust_step(args, users, rate, prefix)

        # Snapshot diagnostics while the load is applied (near the end of the step).
        # Consecutive snapshots are ~one step apart, so `args.step` is the interval
        # the coordinator uses to turn the CPU-seconds delta into a utilisation.
        time.sleep(max(1, int(args.step * 0.8)))
        try:
            cur = read_diagnostics(driver)
        except Exception:
            cur = prev_snap
        proc.wait()
        wall = float(args.step)

        step = parse_locust_stats(prefix) or {
            "reqs": 0, "fails": 0, "fail_frac": 0, "rps": 0, "p50": 0, "p95": 0, "p99": 0}
        if baseline is None:
            baseline = step

        is_knee, reasons = detect_knee(step, baseline, cur, prev_snap, args)
        rejects = (delta(cur, prev_snap, REJECTION_KEYS)
                   if prev_snap is not None else 0)
        status = "⚠ KNEE" if is_knee else "OK"
        print(f"{users:>6} {step['rps']:>7.0f} {step['p50']:>6.0f}m {step['p99']:>7.0f}m "
              f"{step['fail_frac']*100:>5.1f}% {fmt_mb(g(cur,'rss_bytes',-1)):>6} "
              f"{int(g(cur,'queries_in_flight',0)):>8} {rejects:>7}  {status}")

        if is_knee and knee_at is None:
            knee_at = (users, cur, prev_snap, wall, reasons)
            if not args.keep_going:
                break
        prev_snap = cur

    print("-" * len(header))
    if knee_at is None:
        print(f"No brown-out detected up to {steps[-1]} users. The service held; "
              "raise --users to push further.")
    else:
        users, cur, prev, wall, reasons = knee_at
        limiter, why = attribute_limiter(scn, cur, prev, wall)
        print(f"BROWN-OUT at ~{users} concurrent users")
        print(f"Limiter: {limiter} ({why})")
        for r in reasons:
            print(f"  - {r}")
    driver.close()


if __name__ == "__main__":
    main()
