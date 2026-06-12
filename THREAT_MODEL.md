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

### Manifest MAC (`mac`)
When encryption is enabled, `slater-build` seals the whole manifest with a keyed-BLAKE3 MAC
under a subkey derived from the master key (`BLAKE3::derive_key`, context
`"slater manifest mac v1"`, domain-separated from the block-key context). At open time, when
a master key is configured and the manifest carries a `mac`, the server recomputes and
**refuses to serve on mismatch**. The MAC covers every other manifest field — `content_hash`,
the file inventory, the encryption header, and `aclBlake3`. This upgrades the integrity guard
from *copy-complete* to *authentic* for encrypted deployments: an attacker with write access
to `/data` but **without the master key** cannot forge a manifest (or a swapped ACL stamp)
that opens. This is the core defense for the at-rest-write-access threat that at-rest
encryption implies.

## Known limitations

1. **Downgrade / strip.** Because the new fields are optional (older images lack them), an
   attacker who can rewrite the manifest could *delete* `mac`/`aclBlake3` to silence the
   checks. Mitigated by two opt-in config flags, both off by default for compatibility:
   - `requireManifestMac` — refuse any generation with no MAC when a master key is configured.
   - `requireAclStamp` — refuse any generation with no ACL stamp.
   Operators who rely on these guarantees should enable the relevant flag.
2. **Plaintext images have no manifest authenticity.** With no master key there is no MAC;
   such images are guarded only by the copy-completeness hash. Use `--encrypt` for authenticity.
3. **ACL stamp is checked at open/swap, not on every hot-reload.** `acl.json` is hot-reloaded
   while the server runs; the stamp guarantees the generation matched the live ACL *at open
   time*. A post-boot edit to `acl.json` is not re-checked against the stamp until the next
   generation swap or restart. (The ACL subsystem itself remains fail-safe: a malformed edit
   keeps the last-good ACL.)
4. **Multi-graph operational note.** There is one server-wide `acl.json` but a manifest per
   graph. Each stamped graph independently checks the same live file, so when `acl.json`
   legitimately changes, **every stamped graph must be rebuilt** (`--acl`) or it refuses to
   serve. This is intentional: a change to the access-control surface forces an explicit rebuild.
5. **MAC comparison is not constant-time.** Irrelevant here — the attacker controls an
   offline manifest, not an online verification oracle, and forging the MAC requires the key.

## Out of scope

Confidentiality/integrity of the master key itself (operator's secret store), the transport
(TLS terminates at the Bolt listener; see `tls` config), host compromise with read access to
the live master key in process memory, and denial of service.
