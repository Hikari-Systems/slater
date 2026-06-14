# Slater threat model

This document states what Slater's at-rest protections do and do not defend against.
It is deliberately narrow: Slater serves **immutable, read-only** graph generations built
offline by `slater-build` and published to a data directory the `slater` server opens.

## Assets

- **Generation images** — the `.blk` data files (properties, labels, topology, vectors,
  range/vector indexes) under `<data-dir>/<graph>/<generation>/`.
- **`MANIFEST.json`** — the per-generation inventory: file list, per-file BLAKE3 hashes,
  the `content_hash` over that inventory, the encryption header (KDF params + salt), and the
  new authentication fields (`aclBlake3`, `mac`).
- **`acl.json`** — server-wide users → per-graph read grants + argon2id password hashes.
- **The at-rest master key** — supplied to both `slater-build` and `slater` out of band (an
  env var or a mounted secret file). It is **never** written into the data directory.
- **The server configuration** — `config.json`, the `/sandbox` overlay deep-merged over it,
  and the process environment, which together set `dataDir`, `aclPath`, the
  `encryption.keyFile`/`keyEnv` that *name where the master key is read from*, and the
  security flags. This surface is **trusted** (part of the TCB) — see "Trust boundary" below.

## Existing protections

- **At-rest encryption (optional, per block).** With `--encrypt`, each compressed block is
  sealed with XChaCha20-Poly1305 under a per-generation block key = `BLAKE3::derive_key`
  over (master key ‖ per-generation salt). The salt lives in the manifest; the key never
  does. A wrong/absent key fails closed (the Poly1305 tag does not verify). This protects
  **block contents** at rest.
- **Copy-completeness integrity.** Per-file BLAKE3 + a `content_hash` over the file
  inventory let the reader refuse a half-copied generation (e.g. an in-progress rsync onto
  network storage). This proves the files are **complete and self-consistent**, *not* that
  they are **authentic** — `content_hash` excludes `MANIFEST.json` itself, so an attacker who
  rewrites the data files *and* the manifest defeats it (when no key is in play).

## New protections

### ACL consistency stamp (`aclBlake3`)
`slater-build --acl <path>` records the BLAKE3 digest of the `acl.json` the image was built
against. At open/swap time the server re-hashes the configured live `acl.json` and **refuses
to serve** any stamped graph whose digest differs. This binds a generation to a known
access-control surface and catches deploy-time skew (an image shipped against a stale or
swapped ACL). The stamp is not secret and, on its own (plaintext image), is not
cryptographically authenticated — see the MAC.

**Threat policed — post-generation tampering with `acl.json` at runtime.** `acl.json` is
**hot-reloaded** while the server runs, so the stamp must hold for the *whole life* of a
served generation, not only at open/swap. The hot-reload path therefore enforces the stamp
too: on every reload the freshly read `acl.json` is hashed and **adopted only if its digest
matches the `aclBlake3` of every stamped served generation**. A digest that does not match
is treated as tampering — the edit is refused, the last-good ACL (which still matches the
served stamp) keeps serving, and the divergence is logged loudly. The result: an attacker
(or a mistaken operator) with write access to `acl.json` **cannot** self-grant a read between
generation swaps; the only way to change access control is the legitimate one — rebuild and
publish a generation stamped against the new `acl.json`. That swap is the synchronisation
point: its policy check confirms the live `acl.json` hashes to the new stamp, after which the
server adopts the matching ACL. An **unstamped** generation imposes no constraint, so it
would hot-reload unchecked — which is why `requireAclStamp` (on by default) refuses to serve
unstamped generations at all. (Enforced in `AclHandle::poll_checked` /
`Graphs::acl_digest_acceptable`.)

### Manifest MAC (`mac`)
When encryption is enabled, `slater-build` seals the whole manifest with a keyed-BLAKE3 MAC
under a subkey derived from the master key (`BLAKE3::derive_key`, context
`"slater manifest mac v1"`, domain-separated from the block-key context). At open time, when a master key is configured, the server requires
the manifest to carry a `mac` — absence is refused outright (see limitation 1) — and
recomputes it, **refusing to serve on mismatch**. The MAC covers every other manifest field — `content_hash`,
the file inventory, the encryption header, and `aclBlake3`. This upgrades the integrity guard
from *copy-complete* to *authentic* for encrypted deployments: an attacker with write access
to `/data` but **without the master key** cannot forge a manifest (or a swapped ACL stamp)
that opens. This is the core defense for the at-rest-write-access threat that at-rest
encryption implies.

## Known limitations

1. **Downgrade / strip — closed.** Because the authentication fields are optional in the
   manifest format, an attacker who can rewrite the manifest could *delete* `mac`/`aclBlake3`
   to silence the checks. Both strips are refused:
   - **MAC strip: structurally closed, no off switch.** A server configured with a master key
     unconditionally refuses any generation whose manifest lacks a MAC. This is deliberately
     not a config flag: `slater-build` seals a MAC whenever it has the key (and refuses a key
     without `--encrypt`), so a MAC-less generation on a keyed server is either a strip attack
     or a plaintext image that does not need the key in the first place. There is no
     legitimate keyed-but-unauthenticated deployment — and therefore no knob an attacker with
     config access could flip to reopen the downgrade. Plaintext deployments configure no key.
   - **Stamp strip: closed under encryption; defence-in-depth in plaintext.**
     `requireAclStamp` (default **on**) refuses any generation with no `aclBlake3` stamp. It
     remains a flag (unlike the MAC) because disabling it is the documented escape from the
     rebuild-every-graph-on-ACL-change contract (limitation 4) — a genuine operational tradeoff,
     made explicitly. The strength of the stamp depends on whether the image is authenticated:
     in an **encrypted + MAC'd** image the MAC covers `aclBlake3`, so a strip invalidates the
     MAC and is refused unconditionally — the stamp is genuinely *tamper-proof* there, and
     `requireAclStamp` is in fact redundant for closing the strip (it still earns its keep by
     refusing legitimately-unstamped images and catching deploy-time skew). In a **plaintext**
     image the whole manifest is unauthenticated: a data-dir attacker can strip the stamp, or
     re-stamp it against an `acl.json` they also control, or simply rewrite the data — so
     `requireAclStamp` there is a tripwire resting on filesystem permissions, **not** a
     cryptographic guarantee. For a hard guarantee, encrypt. This is also why no in-manifest
     "must-enforce-the-stamp" flag would help: an unauthenticated flag is equally strippable,
     and an authenticated one (MAC-covered) is redundant with the MAC that already makes the
     stamp tamper-proof.
2. **Plaintext images have no manifest authenticity.** With no master key there is no MAC;
   such images are guarded only by the copy-completeness hash. Use `--encrypt` for authenticity.
3. **Runtime trust boundary for `acl.json`.** The stamp is now re-verified on every
   hot-reload (see "Threat policed" above), so a post-build edit that diverges from the
   served stamp is refused rather than adopted. Two residuals remain by design: (a) for an
   **unstamped** generation there is no stamp to check, so its ACL hot-reloads on filesystem
   permissions alone — `requireAclStamp` (on by default) forbids serving such generations,
   so this residual only exists where an operator has explicitly disabled it; (b) the
   digest binds *content*, not freshness, so an attacker who can replay a *previously valid*
   `acl.json` whose digest still matches the served stamp would be re-adopting an ACL the
   generation already accepts (no privilege gain). Restrict `acl.json` to the server user
   (`chmod 600`) as defence-in-depth — the server now warns at load if it is group/world
   writable.
4. **Multi-graph operational note.** There is one server-wide `acl.json` but a manifest per
   graph. Each stamped graph independently checks the same live file, so when `acl.json`
   legitimately changes, **every stamped graph must be rebuilt** (`--acl`) or it refuses to
   serve. This is intentional: a change to the access-control surface forces an explicit rebuild.
5. **MAC comparison is not constant-time.** Irrelevant here — the attacker controls an
   offline manifest, not an online verification oracle, and forging the MAC requires the key.
6. **Generation rollback is not prevented (freshness ≠ authenticity).** The MAC proves a
   served generation was *built with the key*, not that it is the *newest* such build. An
   attacker with write access to `/data` can repoint a graph's `current` at an **older,
   still-validly-signed** generation; the swap re-verifies its MAC, content hash, and ACL
   stamp, all of which an authentic old image passes. This is only exploitable if old
   generations are retained on disk **and** the old image's `aclBlake3` still matches the
   live `acl.json` (else the stamp check refuses the rollback). Mitigation, if rollback is a
   concern for your deployment: prune superseded generations, or adopt a monotonic, MAC-covered
   build counter and refuse a `current` that moves backwards (tracked in `SECURITY_WORKLIST.md`).

## Availability (denial of service)

Some DoS surface is now defended at the cheap, high-value end; the rest is enumerated as
accepted risk so future changes stay aware of it.

**Defended:**
- **Pre-auth Bolt framing flood.** The message framer caps both a single reassembled message
  and the unparsed accumulation buffer, so a peer that streams chunks endlessly — or withholds
  the terminating `00 00` — is disconnected instead of driving the process to OOM. The cap is
  **differential**: before `LOGON` only `HELLO`/`LOGON` can arrive (a few hundred bytes), so the
  pre-auth budget is tight (`server.maxPreAuthBytes`, default 64 KiB); it ratchets up to the
  generous authenticated cap (`server.maxMessageBytes`, default 64 MiB) on a verified `LOGON`
  and back down on `LOGOFF`. This runs before authentication, so it is the most important cap.
- **Pre-auth slow-loris.** An unauthenticated peer must complete handshake → `LOGON` within
  `server.loginTimeoutMs` (default 10 s) or the connection is closed, so an anonymous peer cannot
  hold a socket open indefinitely. (`server.idleTimeoutMs` adds an optional post-auth idle
  timeout; off by default, since pooled drivers legitimately hold idle connections.)
- **Connection-count exhaustion.** The listener acquires a global permit *before* `accept()`
  (`server.maxConnections`), so at capacity back-pressure flows into the kernel listen backlog
  rather than the heap — the process never accepts a descriptor it cannot service. A separate,
  smaller budget caps connections that have not yet authenticated (`server.maxPreAuthConnections`),
  so an anonymous flood cannot starve authenticated readers, and a per-source cap
  (`server.maxConnectionsPerIp`, keyed on the /32 for IPv4 and /64 for IPv6) stops one source
  monopolising the pool. Because per-connection buffers live outside the cache budgets, this is
  also what makes the **bounded-RSS guarantee hold under adversarial connection load**, not just
  well-behaved clients. These are defence-in-depth behind the primary control (network ACLs + an
  L4 proxy); see `docs/HARDENING.md` "Network posture".
- **`range()` blow-up.** `range()` refuses a span whose element count exceeds a guardrail
  (1M ≈ 48 MB) and uses checked arithmetic, closing a single-query OOM / infinite-loop (the
  result-row cap does not catch it, since a giant list is one row). This is the lone guard
  when the intermediate budget below is disabled.
- **User-supplied regexes** (`=~`, `string.matchRegEx`, `string.replaceRegEx`). Patterns are
  length-capped (1 KiB), compiled with explicit NFA / lazy-DFA size limits (1 MiB each, vs the
  crate's 10 MiB default), and cached per query so a constant pattern compiles once rather than
  once per row. (The regex crate is an RE2-style linear-time engine — catastrophic backtracking
  was never possible; the bounded costs are compile time, automaton size, and recompilation.)
- **Intermediate-collection growth.** A query-wide element budget (`query.maxIntermediate`,
  default 1M, 0 ⇒ off) is charged by every operation that materialises a collection — list and
  pattern comprehensions, pattern-match bindings, `UNWIND`, list concatenation (every temp, which
  defeats `reduce(acc + acc)` doubling), aggregate buffers, `range()`, and variable-length path
  expansion (charged per emitted path, weighted by length, so a dense graph cannot blow up the
  result set within the hop cap). Charging is cumulative across the query, so repeated or
  geometric allocation trips the budget early instead of allocating until `timeout_ms`. The
  default is sized against the typical 100–200 MB deployment envelope: at ~48 bytes per
  element (`size_of::<Val>()`), 1M elements bounds one query's intermediate state at roughly
  48 MB worst case, leaving headroom for a few concurrent queries.

**Accepted / not yet defended:** the budget counts *elements*, not bytes (1M long strings is
far more memory than 1M ints — operators can lower the knob); the budget is per query, so N
concurrent queries can each claim it in full — there is no cross-query global memory accounting.
The connection caps bound *count*, not aggregate work: `maxConnections` authenticated readers can
still each run a query up to the per-query budgets concurrently. Operators relying on availability
under hostile *authenticated* load should size the per-query budgets and `maxConnections` together,
and front the listener with network-level resource limits (see `docs/HARDENING.md`).

## Trust boundary (what the at-rest protections assume)

Every at-rest guarantee above — the MAC, the encryption, the ACL stamp — assumes the
**server configuration and the master key's storage location are trusted**. They are part of
the trusted computing base, not part of the attacker-writable surface. The single surface this
model treats as attacker-writable is the **data directory**; the configuration that tells the
server *how to interpret* that directory is not.

This matters because the config does not contain the key — it contains a **pointer** to the
key (`encryption.keyFile` / `keyEnv`). The MAC's security reduces to the secrecy of the key it
is computed under, and the config names where that key comes from. So an attacker who can write
**both** the config **and** the data directory defeats the MAC completely, without ever learning
the real key:

1. generate their own key `K'`;
2. write `K'` to a file they control, repoint `encryption.keyFile` at it;
3. rebuild each generation under `K'` exactly as `slater-build` would — re-encrypt blocks,
   recompute `content_hash`, `seal_mac(K')` last;
4. on the next start the server loads `K'` as "the master key", and every block decrypts and
   every MAC verifies. The ACL stamp can be re-pointed the same way.

No MAC hardening can close this: the MAC only ever proves "built by someone who knew the
configured key", and here the attacker *chose* the configured key. The same config-write access
is independently fatal by other routes — repoint `aclPath` at a permissive ACL, repoint
`dataDir` at staged data, or drop the key reference to disable verification — so **config-write
is equivalent to full compromise** and is out of scope for the at-rest model. (The substitution
requires a server **restart**: the key is read once at boot and retained across generation swaps,
so a running server does not pick up an edited `keyFile` until it restarts — an attacker who can
write config can usually wait for or induce one, so this only raises the bar, it does not close
the gap.)

### Mitigations — required where the config/data surface is not fully trusted

In a deployment where the principal that publishes generations is *not* the same as, or as
trusted as, the operator who owns the server's config and secret — a shared data volume, an
ingestion pipeline with write access, a multi-tenant host — the boundary above must be enforced
operationally. None of these are on by default in the sense of being inferable from the code;
they are deployment obligations:

- **Mount the config read-only to the server**, and do not let any lesser-privileged principal
  write `config.json`, the `/sandbox` overlay, or the process environment. Treat write access to
  any of them as equivalent to handing over the master key.
- **Place the master key outside every attacker-writable path.** `keyFile` must point at a
  location the data-publishing principal cannot write (ideally a mounted secret with `0400`,
  owned by the server user). As a tripwire, the server **refuses to start** if `keyFile`
  resolves *inside* `dataDir` (`EncryptionConfig::check_key_file_outside_data_dir`). This is
  defence-in-depth, not a complete defence — it does not catch a `keyFile` pointing at some
  *other* writable path (e.g. `/tmp`); only the isolation above does.
- **Restrict the data directory** to the publishing principal and the server user, so the
  attacker-writable surface is as small as the model assumes.
- **Prefer `keyEnv` over `keyFile`** when the environment is harder to influence than the
  filesystem, and **prune superseded generations** (also closes the rollback residual,
  limitation 6).

Where config, key location, and data directory are all owned by a single trusted operator (the
typical single-tenant deployment), these are satisfied by construction and need no extra action.

## Out of scope

Confidentiality/integrity of the master key itself (operator's secret store), **write access to
the server configuration or the master key's storage location** (the trusted computing base — see
"Trust boundary"), the transport (TLS terminates at the Bolt listener; see `tls` config), and
host compromise with read access to the live master key in process memory.
