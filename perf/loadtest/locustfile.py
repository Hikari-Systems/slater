"""Locust load driver for the slater read service (Bolt protocol).

Locust has no native Bolt support, so this defines a custom `User` that drives
the official neo4j Python driver — the same driver perf/bench.py uses. Locust
runs on gevent and monkey-patches the socket layer at startup, so the driver's
blocking I/O becomes cooperative and thousands of virtual users scale on one box.

Selection is by environment variable so the coordinator and a bare `locust`
invocation behave identically:

    SLATER_URI       bolt://127.0.0.1:7687
    SLATER_USER      reporting
    SLATER_PASS      <password>          (required)
    SLATER_DB        pole
    SLATER_SCENARIO  mixed_soak          (see scenarios.py)

Run directly, e.g.:

    SLATER_PASS=polereader SLATER_SCENARIO=cpu_fanout \
      locust -f perf/loadtest/locustfile.py --headless \
             -u 500 -r 50 -t 60s --host bolt://127.0.0.1:7687 \
             --csv perf/loadtest/out/cpu_fanout

…or let coordinator.py ramp it and detect the brown-out knee.
"""

from __future__ import annotations

import os
import random
import socket
import struct
import time

import gevent
from locust import User, task, constant
from neo4j import GraphDatabase

import queries
import scenarios

URI = os.environ.get("SLATER_URI", "bolt://127.0.0.1:7687")
DBUSER = os.environ.get("SLATER_USER", "reporting")
PASSWORD = os.environ.get("SLATER_PASS", "")
DB = os.environ.get("SLATER_DB", "pole")
SCN = scenarios.get(os.environ.get("SLATER_SCENARIO", "mixed_soak"))

# Pools are derived once per worker process (cheap, schema-agnostic with fallback).
_POOLS = {"types": [], "nhs": []}


def _ensure_pools():
    global _POOLS
    if _POOLS["types"] or _POOLS["nhs"]:
        return
    try:
        d = GraphDatabase.driver(URI, auth=(DBUSER, PASSWORD))
        _POOLS = queries.derive_pools(d, DB)
        d.close()
    except Exception:
        pass


def _weighted_shapes():
    """Expand the scenario weights into a flat pick-list of (name, text, fn)."""
    resolved = {n: (n, t, f) for (n, t, f) in queries.shapes_for(["all"], _POOLS)}
    flat = []
    for name, weight in SCN.weights.items():
        for cand in ([name] if name != "all" else list(resolved)):
            if cand in resolved:
                flat.extend([resolved[cand]] * max(1, int(weight)))
    # Fall back to a schema-agnostic shape so a mis-derived pool never empties the
    # task list (which would make Locust idle silently).
    if not flat and resolved:
        flat = [resolved.get("count_all", next(iter(resolved.values())))]
    return flat


class SlaterQueryUser(User):
    """Runs the scenario's weighted query mix over its own Bolt connection."""

    wait_time = constant(0)  # drive as hard as the gevent loop allows
    # Disabled for the loris scenario (a different connection behaviour applies).
    weight = 0 if SCN.loris else 1

    def on_start(self):
        _ensure_pools()
        # One driver per user with a tiny pool ⇒ user count ≈ open connection count,
        # which is what the connection-cap scenarios need to mean anything.
        self._driver = GraphDatabase.driver(
            URI, auth=(DBUSER, PASSWORD),
            max_connection_pool_size=max(1, SCN.pool_size),
            connection_acquisition_timeout=30.0,
        )
        self._session = self._driver.session(database=DB)
        self._shapes = _weighted_shapes()
        self._seed = queries.rand_seed_iter()

    def on_stop(self):
        try:
            self._session.close()
            self._driver.close()
        except Exception:
            pass

    @task
    def run_query(self):
        if not self._shapes:
            gevent.sleep(0.5)
            return
        name, text, pf = random.choice(self._shapes)
        params = pf(self._seed, _POOLS)
        self._seed += 1
        t0 = time.perf_counter()
        try:
            rows = list(self._session.run(text, params))
            self._fire(name, t0, len(rows), None)
        except Exception as exc:  # noqa: BLE001 — report any driver/server error
            self._fire(name, t0, 0, exc)
            # A failed session can be poisoned; reopen it for the next task.
            self._reconnect()

    def _fire(self, name, t0, nrows, exc):
        self.environment.events.request.fire(
            request_type="bolt",
            name=name,
            response_time=(time.perf_counter() - t0) * 1000.0,
            response_length=nrows,
            exception=exc,
            context={},
        )

    def _reconnect(self):
        try:
            self._session.close()
        except Exception:
            pass
        try:
            self._session = self._driver.session(database=DB)
        except Exception:
            gevent.sleep(0.1)


# ── Slow-loris variant: open a socket, do the Bolt handshake, never LOGON ──────

_BOLT_MAGIC = b"\x60\x60\xb0\x17"
_PROPOSALS = struct.pack(">IIII", 0x00060605, 0x00030404, 0, 0)  # 5.6..5.0, 4.4..4.1


class SlaterLorisUser(User):
    """Holds an unauthenticated connection open (handshake done, no LOGON) to
    exercise the pre-auth budget and the login deadline. The socket is the stress,
    so the 'task' just keeps it open and reopens it after the server times it out."""

    wait_time = constant(0)
    weight = 1 if SCN.loris else 0

    def on_start(self):
        host = URI.split("://", 1)[-1]
        self._addr = (host.rsplit(":", 1)[0], int(host.rsplit(":", 1)[1]))
        self._sock = None

    def on_stop(self):
        self._close()

    @task
    def hold(self):
        if self._sock is None:
            self._open()
        # Probe liveness: a server that hit the login deadline has closed us.
        t0 = time.perf_counter()
        try:
            self._sock.settimeout(0.5)
            data = self._sock.recv(16)
            if data == b"":  # server closed (login deadline / pre-auth eviction)
                self._fire("loris_closed", t0, None)
                self._close()
                return
        except socket.timeout:
            pass  # still being held — the intended state
        except Exception as exc:  # noqa: BLE001
            self._fire("loris_error", t0, exc)
            self._close()
            return
        gevent.sleep(1.0)  # keep holding

    def _open(self):
        t0 = time.perf_counter()
        try:
            s = socket.create_connection(self._addr, timeout=5)
            s.sendall(_BOLT_MAGIC + _PROPOSALS)
            s.recv(4)  # negotiated version; then we deliberately never LOGON
            self._sock = s
            self._fire("loris_open", t0, None)
        except Exception as exc:  # noqa: BLE001 — connection refused = cap reached
            self._fire("loris_refused", t0, exc)
            self._sock = None
            gevent.sleep(0.2)

    def _close(self):
        try:
            if self._sock:
                self._sock.close()
        finally:
            self._sock = None

    def _fire(self, name, t0, exc):
        self.environment.events.request.fire(
            request_type="loris",
            name=name,
            response_time=(time.perf_counter() - t0) * 1000.0,
            response_length=0,
            exception=exc,
            context={},
        )
