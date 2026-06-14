"""Scenario registry for the slater load-test suite.

A scenario names a query mix (which shapes from queries.py, with weights) plus a
hint about what fragile area it targets and what the operator should watch in the
diagnostics snapshot. The coordinator and the locustfile both read this registry,
so a scenario is selected by a single name (env var SLATER_SCENARIO) end-to-end.

Connection-shaped scenarios (conn_flood, pre_auth_loris) are driven mainly by the
ramp (user/connection count) rather than the query mix; their `pool_size` keeps
one Bolt connection per Locust user so user count ≈ connection count.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Scenario:
    name: str
    description: str
    # shape_name -> weight. 'all' is allowed as a shorthand in weights keys.
    weights: dict
    # The fragile area this targets, and the diagnostics metric(s) that should
    # move first when it brown-outs (used by the coordinator's limiter hint).
    targets: str
    watch: list = field(default_factory=list)
    # Bolt connections per Locust user (1 ⇒ user count == connection count).
    pool_size: int = 1
    # For pre_auth_loris: open a socket and stall before LOGON instead of querying.
    loris: bool = False


SCENARIOS = {
    "mixed_soak": Scenario(
        "mixed_soak",
        "Balanced steady-state mix — the SLA soak. Establishes a healthy baseline "
        "and surfaces gradual degradation (leaks, cache thrash) over time.",
        weights={"point_lookup": 5, "two_hop": 3, "three_hop": 2, "agg_by_type": 2,
                 "count_all": 1, "scan_contains": 1},
        targets="overall",
        watch=["latency_p99_ms", "rss_bytes", "queries_ok_total"],
    ),
    "cpu_fanout": Scenario(
        "cpu_fanout",
        "Heavy multi-hop + aggregation to saturate CPU. Run the server with "
        "query.maxFanout > 1 so a single query recruits the rayon pool and "
        "concurrent queries contend for cores.",
        weights={"three_hop": 4, "two_hop": 3, "agg_by_type": 3, "deep_varlen": 2},
        targets="cpu",
        watch=["latency_p99_ms", "cpu_seconds_total", "queries_in_flight"],
    ),
    "mem_cache_churn": Scenario(
        "mem_cache_churn",
        "Wide cold scans that spread reads across the store so the block cache "
        "cannot hold the working set — eviction storms and RSS climb toward the "
        "cgroup limit.",
        weights={"scan_contains": 5, "count_all": 2, "two_hop": 2},
        targets="memory",
        watch=["rss_bytes", "cgroup_mem_limit_bytes", "cache_block_evictions"],
    ),
    "intermediate_breach": Scenario(
        "intermediate_breach",
        "Large UNWIND/collect materialisations that charge the per-query "
        "intermediate budget — fail_budget should climb as concurrency rises.",
        weights={"fat_unwind": 5, "deep_varlen": 3},
        targets="query-budget",
        watch=["fail_budget_total", "latency_p99_ms", "rss_bytes"],
    ),
    "deadline_storm": Scenario(
        "deadline_storm",
        "Deep variable-length expansions that approach query.timeoutMs — "
        "fail_deadline climbs and the blocking pool saturates (queries_in_flight).",
        weights={"deep_varlen": 6, "three_hop": 2},
        targets="query-budget",
        watch=["fail_deadline_total", "queries_in_flight", "latency_p99_ms"],
    ),
    "fat_message": Scenario(
        "fat_message",
        "Large IN-list parameter maps near server.maxMessageBytes — exercises "
        "reassembly. With the cap tightened, msg_too_large_auth climbs.",
        weights={"fat_param": 6, "point_lookup": 1},
        targets="message",
        watch=["msg_too_large_auth_total", "rss_bytes", "latency_p99_ms"],
    ),
    "shortest_path_bfs": Scenario(
        "shortest_path_bfs",
        "shortestPath over the graph — unbounded BFS discovery when "
        "query.maxShortestPathExplore is 0. Watch RSS and fail_shortest_path "
        "(set the cap to convert OOM risk into a clean failure).",
        weights={"shortest_path": 6, "point_lookup": 1},
        targets="memory",
        watch=["rss_bytes", "fail_shortest_path_total", "latency_p99_ms"],
    ),
    "conn_flood": Scenario(
        "conn_flood",
        "Drive the user (=connection) count toward server.maxConnections / "
        "maxConnectionsPerIp. Light queries — the stress is the socket count: "
        "global backpressure (conn_in_use vs conn_limit) and per-IP rejections.",
        weights={"count_all": 1},
        targets="connection-cap",
        watch=["conn_in_use", "conn_limit", "conn_rejected_per_ip_total"],
        pool_size=1,
    ),
    "pre_auth_loris": Scenario(
        "pre_auth_loris",
        "Open many sockets and stall before LOGON to exercise the pre-auth budget "
        "and login deadline (slow-loris). Watch conn_rejected_pre_auth and "
        "login_timeouts.",
        weights={},
        targets="connection-cap",
        watch=["conn_rejected_pre_auth_total", "login_timeouts_total", "conn_pre_auth_in_use"],
        pool_size=1,
        loris=True,
    ),
}


def get(name):
    if name not in SCENARIOS:
        raise SystemExit(
            f"unknown scenario '{name}'. Available: {', '.join(sorted(SCENARIOS))}")
    return SCENARIOS[name]
