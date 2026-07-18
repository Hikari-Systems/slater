# 15 · Security

Slater is built to serve untrusted network traffic safely: authentication and
access control are mandatory, protections default **on**, and the on-disk image
can be authenticated and encrypted. This page covers what you configure. The
canonical, exhaustive security documents are `THREAT_MODEL.md` and
`SECURITY_WORKLIST.md` at the repository root; this is the operator's how-to.

## Authentication

Every connection authenticates with a username and password over Bolt. Passwords
are verified with **argon2id**; cleartext passwords are never stored. You mint a
hash with the `slater hash-password` subcommand and place it in the ACL file:

```sh
slater hash-password        # type a password; it prints an $argon2id$… hash
```

An unknown username still runs a verification pass, so authentication does not
leak which usernames exist through timing.

## Authorization: per-graph grants

Access control lives in a plain-JSON `acl.json` (path set by `aclPath`, shipped
default `/config/acl.json`). Each user has argon2id credentials and a map of
**per-graph grants**. Two permissions are meaningful, and they are **independent**
— a `read` grant confers no write access:

- `read` — serve queries on the graph.
- `write` — additionally permit `MERGE`/`SET`/`REMOVE`/`DELETE`, `CREATE` /
  `INSERT`, and `CALL slater.consolidate()`, when the writable layer is on.

```json
{
  "users": {
    "admin": {
      "passwordArgon2id": "$argon2id$v=19$m=19456,t=2,p=1$…",
      "grants": { "social": ["read", "write"], "products": ["read", "write"] }
    },
    "reporting": {
      "passwordArgon2id": "$argon2id$v=19$m=19456,t=2,p=1$…",
      "grants": { "social": ["read"] }
    }
  }
}
```

The ACL is **hot-reloaded**: editing the file re-reads it, keeping the last-good
version if the new file is malformed.

## The ACL stamp

`requireAclStamp` is **on by default**. It refuses to serve any generation whose
manifest lacks an `aclBlake3` stamp — a BLAKE3 digest of the ACL, written into the
generation at build time with `slater-build --acl acl.json`. This binds a
generation to the access-control policy it was published with. On hot-reload the
server re-checks that the running ACL's digest still matches the served
generation's stamp, refusing a mismatched swap.

The sample graphs in this manual are built *without* `--acl`, so serving them
needs `requireAclStamp=false`. Production builds should stamp the ACL and leave
`requireAclStamp` at its default.

## Encryption at rest

Data blocks can be encrypted at rest with XChaCha20-Poly1305. Build with
`slater-build --encrypt` and supply the master key (hex) to both builder and
server:

```sh
# build
slater-build … --encrypt --key-file /secrets/master.key
# serve
export encryption__keyFile=/secrets/master.key
```

Supply the key by file (`encryption.keyFile`) or env var (`encryption.keyEnv`);
the file takes precedence and must live **outside** the data directory (a tripwire
refuses a key inside it). Independently of encryption, the manifest carries a
keyed-BLAKE3 **MAC** that authenticates every manifest field when a key is
configured; a keyed server refuses a MAC-less generation.

## Resource limits (denial-of-service protection)

The `server.*` limits are **on by default but generous**, sized to keep a server
stable inside a roughly 100–200 MB memory envelope under hostile load. The main
ones:

| Concern | Knob(s) | Default |
|---|---|---|
| Too many connections | `server.maxConnections` / `maxPreAuthConnections` / `maxConnectionsPerIp` | 16384 / 4096 / 1024 |
| Oversized messages | `server.maxMessageBytes` / `maxPreAuthBytes` | 64 MiB / 64 KiB |
| Slow-loris / idle | `server.loginTimeoutMs` / `tlsHandshakeTimeoutMs` / `idleTimeoutMs` | 10000 / 5000 / 0 |
| Auth abuse | `server.maxConcurrentAuth` / `maxAuthFailures` | 4 / 3 |
| Write pressure | `server.maxConcurrentWrites` | 4 |
| Query memory | `query.maxIntermediate` / `maxIntermediateGlobal` | 1M / 8M |

Query-memory bounding is covered in [16 Performance tuning](16-performance-tuning.md);
every knob is in [14 Configuration reference](14-configuration-reference.md).

## Read-only enforcement

Independently of the ACL, the query parser is read-only unless the writable layer
is enabled (`delta.enabled`), and even then a write statement is authorized
against the user's `write` grant before it executes. A write attempt by a
read-only connection or a read-only user is rejected with a clear message; see
[11 Writing data](11-writing-data.md) and
[18 Troubleshooting](18-troubleshooting.md).

## Next

- The write surface the `write` grant unlocks: [11 Writing data](11-writing-data.md).
- Deployment specifics (TLS, containers): [13 Deployment](13-deployment.md).
