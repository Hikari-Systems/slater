# Hardening

This is the human-readable index to how Slater defends itself. It summarises the
posture and points at the canonical detail in [`THREAT_MODEL.md`](../THREAT_MODEL.md)
(what is in/out of scope, and the trust boundary) and
[`SECURITY_WORKLIST.md`](../SECURITY_WORKLIST.md) (per-item status and history). Where
the two disagree with this file, they win — this page is a map, not the territory.

Slater's shape makes hardening tractable: it is a **read-only** serving engine over an
immutable, content-hashed on-disk generation, behind standard neo4j (Bolt) drivers. No
writes, no query-side mutation, a narrow message surface.

## Memory safety

- **Zero `unsafe`** in the entire workspace — unusual for code doing columnar block
  files, CSR topology, and PQ-quantised vectors. The class of memory-corruption bugs
  that dominates C/C++ database CVEs is absent by construction.

## Pre-authentication wire surface

Everything an unauthenticated peer can reach is the Bolt handshake, the chunk framing,
and the PackStream decode of `HELLO`/`LOGON`. `RUN` — and therefore the Cypher parser —
only runs *after* `LOGON`, so parser panics are post-auth and isolated (see below).

- **Bounded chunk reassembly.** The framer caps both a single reassembled message body
  and the unparsed accumulation buffer, so a peer that streams chunks forever — or
  withholds the terminating `00 00` — is disconnected rather than driving the process to
  OOM.
- **Differential body cap.** The reassembly budget is per-connection and auth-aware: it
  starts tight before `LOGON` (`server.maxPreAuthBytes`, default 64 KiB — `HELLO`/`LOGON`
  are a few hundred bytes) and ratchets up to the generous authenticated cap
  (`server.maxMessageBytes`, default 64 MiB) only on a verified `LOGON`, ratcheting back
  down on `LOGOFF`/re-auth.
- **No forged-length allocation.** PackStream list/map/struct decoders bound their
  pre-allocation by the bytes actually remaining (`n.min(remaining())`), so a 5-byte
  message claiming a ~2.5-billion element count fails fast instead of requesting
  gigabytes. (Regression test: `forged_length_headers_bail_without_huge_allocation`.)
- **Login deadline.** An unauthenticated peer must finish handshake → `LOGON` within
  `server.loginTimeoutMs` (default 10 s) or be closed, defeating the slow-loris a byte
  cap alone leaves open. `server.idleTimeoutMs` adds an optional post-auth idle timeout
  (off by default — pooled drivers legitimately hold idle connections).

## Connection-resource controls

Per-connection buffers live *outside* the cache budgets, so without these the headline
bounded-RSS guarantee held only for well-behaved clients. With them it is unconditional.

- **Global cap, applied before `accept()`** (`server.maxConnections`, default 16384):
  the listener reserves a permit before pulling a connection off the queue, so at
  capacity back-pressure lands in the kernel listen backlog (`somaxconn`) and then the OS
  refuses new SYNs — the process never accepts a descriptor it cannot service.
- **Pre-auth budget** (`server.maxPreAuthConnections`, default 4096): a smaller, separate
  cap on connections that have not yet authenticated, so a flood of anonymous sockets
  cannot starve the authenticated readers that have released their pre-auth slot.
- **Per-source cap** (`server.maxConnectionsPerIp`, default 1024): keyed on the /32 for
  IPv4 and the /64 for IPv6 (an attacker controls a whole /64), so one source cannot
  monopolise the global pool.

All default **on but generous** — invisible to a legitimate client population, a backstop
under adversarial load. `0` disables any individual limit.

## Panic isolation

Query execution runs on `spawn_blocking`; a panic there surfaces as a `JoinError` and the
blocking-pool thread is reused, while the `accept()` loop is a separate task. A reachable
panic therefore drops a **single connection**, never the server. Combined with the fuzz
harness below, the residual risk from the remaining `unwrap()`/`expect()` sites is bounded.

## Fuzzing & CI

A cargo-fuzz harness (`fuzz/`) carries three targets — the Cypher parser, the PackStream
value decoder, and the Bolt chunk-framing decoder — gated on tagged builds in CI (a crash
blocks the release). It found and closed the pre-auth allocation OOM noted above.

## At-rest & integrity

- **At-rest encryption:** XChaCha20-Poly1305 AEAD over block data.
- **Integrity on open:** BLAKE3 per-block checksums re-verified when a generation opens,
  so a half-copied / corrupt image (e.g. an interrupted NFS copy) is refused, not served.
- **Manifest authentication:** a keyed server unconditionally refuses a MAC-less
  generation (the strip downgrade has no off switch).
- **ACL stamp:** the served generation's `aclBlake3` stamp is enforced on every
  hot-reload; an out-of-band edit to `acl.json` whose digest no longer matches is refused
  and the last-good ACL kept. The legitimate path to change access is to rebuild and
  publish a stamped generation. (`requireAclStamp`, default on.)
- **Key-location tripwire:** the server refuses to start if the at-rest key file resolves
  inside the (attacker-writable) `dataDir`.

See `THREAT_MODEL.md` "Trust boundary" for what these protections assume.

## Query-level DoS budgets

Even an authenticated reader is bounded per query: `query.maxRows` (row cap),
`query.timeoutMs` (wall-clock deadline), `query.maxIntermediate` (cumulative
intermediate-element budget), a dedicated `range()` element guard, and bounded
(length- and automaton-size-capped) regex compilation for user-supplied patterns.

## Deployment posture (network is the primary control)

Slater is a read replica handle and should not be internet-facing. The first line of
defence is the network, not the binary:

1. **Bind privately** and restrict source ranges at the network layer (security groups /
   NetworkPolicy).
2. **Front it with a connection-limiting L4 proxy** — HAProxy `maxconn` + a per-source
   `stick-table`, or nftables `connlimit` + `hashlimit`. This sits before the file
   descriptor is handed to the process, so it is harder to evade than anything app-level.
3. **Use TLS** (`tls.cert` / `tls.key`) for anything beyond loopback.

The in-binary limits above are defence-in-depth so the guarantees hold even when the proxy
is forgotten — not a substitute for the network controls.
