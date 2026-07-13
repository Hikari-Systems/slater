// SPDX-License-Identifier: Apache-2.0
//! Slater server configuration.
//!
// Many fields below are consumed only from milestone 4 (server/cache/query
// wiring). Allow dead_code for the scaffold so the build stays warning-clean;
// the allow is removed once the server loop reads them.
#![allow(dead_code)]
//!
//! Loaded through `hs_utils::config::load_layered_value()` — the house-standard
//! layered loader: base `config.json` (or `/app/config.json`) + `/sandbox`
//! overlay + `[SECRET]:` resolution + `KEY__sub` env overrides. All scalar leaves
//! arrive as strings after that pass, so numeric fields use the `deser_*_or_str`
//! helpers from `hs-utils`.

use anyhow::{Context, Result};
use hs_utils::config::{deser_u16_or_str, deser_u32_or_str};
use serde::Deserialize;

/// Local `or_str` deserialisers for the widths `hs-utils` does not ship (`u64`,
/// `usize`). After `prepare_config` every scalar is a string, so we must accept
/// both the numeric-string form and a raw number (the latter for tests that
/// bypass the layered loader).
mod de {
    use serde::de::{Error, Unexpected, Visitor};
    use serde::Deserializer;
    use std::fmt;

    struct U64;
    impl Visitor<'_> for U64 {
        type Value = u64;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("u64 or numeric string")
        }
        fn visit_u64<E: Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::invalid_value(Unexpected::Signed(v), &self))
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<u64, E> {
            v.parse::<u64>()
                .map_err(|_| E::invalid_value(Unexpected::Str(v), &self))
        }
    }

    pub fn u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        d.deserialize_any(U64)
    }

    pub fn usize<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
        u64(d).map(|v| v as usize)
    }

    struct I64;
    impl Visitor<'_> for I64 {
        type Value = i64;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("i64 or numeric string")
        }
        fn visit_i64<E: Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_u64<E: Error>(self, v: u64) -> Result<i64, E> {
            i64::try_from(v).map_err(|_| E::invalid_value(Unexpected::Unsigned(v), &self))
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<i64, E> {
            v.parse::<i64>()
                .map_err(|_| E::invalid_value(Unexpected::Str(v), &self))
        }
    }

    pub fn i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        d.deserialize_any(I64)
    }

    struct Bool;
    impl Visitor<'_> for Bool {
        type Value = bool;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("bool or \"true\"/\"false\" string")
        }
        fn visit_bool<E: Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<bool, E> {
            match v {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(E::invalid_value(Unexpected::Str(v), &self)),
            }
        }
    }

    pub fn bool<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        d.deserialize_any(Bool)
    }

    /// A byte budget where a non-positive value means "disabled" (floored to 0).
    /// Accepts a number or numeric string (including a negative one) like the
    /// other helpers, so `resultCacheBytes: 0` (or any `<= 0`) turns the pool off.
    pub fn usize_floor0<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
        i64(d).map(|v| usize::try_from(v).unwrap_or(0))
    }

    /// The dense-degree-column residency policy: `"lazy"` (chunk-lazy, the default) or
    /// `"pinned"` (eager, never evicted). Case-insensitive.
    pub fn degree_residency<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<crate::degree_column::DegreeResidency, D::Error> {
        use crate::degree_column::DegreeResidency;
        struct V;
        impl Visitor<'_> for V {
            type Value = DegreeResidency;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("\"lazy\" or \"pinned\"")
            }
            fn visit_str<E: Error>(self, v: &str) -> Result<DegreeResidency, E> {
                match v.to_ascii_lowercase().as_str() {
                    "lazy" => Ok(DegreeResidency::Lazy),
                    "pinned" => Ok(DegreeResidency::Pinned),
                    _ => Err(E::invalid_value(Unexpected::Str(v), &self)),
                }
            }
        }
        d.deserialize_str(V)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    pub server: ServerConfig,
    #[serde(default)]
    pub log: LogConfig,
    /// Where generations are read from: the local filesystem (`fs`, default,
    /// rooted at `dataBackend.fs.dir`) or an object store (`s3`). The on-disk byte
    /// format is identical across backends.
    #[serde(default)]
    pub data_backend: DataBackendConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    /// Path to the JSON ACL file (users → per-graph grants).
    #[serde(default = "default_acl_path")]
    pub acl_path: String,
    /// Refuse to serve any generation whose manifest lacks an `aclBlake3` stamp.
    /// Closes the stamp-strip downgrade. On by default — build images with
    /// `--acl`; set to `false` only to escape the rebuild-every-graph-on-ACL-change
    /// contract (`THREAT_MODEL.md` limitation 4), accepting unstamped images.
    /// (There is no equivalent flag for the manifest MAC: a server configured with
    /// a master key unconditionally refuses a MAC-less generation.)
    #[serde(default = "default_true", deserialize_with = "de::bool")]
    pub require_acl_stamp: bool,
    #[serde(default)]
    pub cache: CacheConfig,
    /// Writable-layer (delta) configuration. Off by default; when disabled the
    /// server is exactly the read-only server it was before this field existed.
    #[serde(default)]
    pub delta: DeltaConfig,
    /// `(label, property)` vector indexes to pin resident for the generation's lifetime.
    #[serde(default)]
    pub vector_index_pins: Vec<VectorIndexPin>,
    #[serde(default)]
    pub encryption: EncryptionConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub vector_query: VectorQueryConfig,
    /// Cypher query run once at boot against every served graph, with its results
    /// discarded. Its only effect is to fault the blocks needed to answer it into
    /// the block (and vector) cache, so the first real client query of that shape
    /// is served warm rather than paying the cold-read penalty. Empty (default)
    /// disables warming. A parse error is logged and warming is skipped (a bad
    /// warming query must never take the server down); a per-graph execution error
    /// is likewise logged and that graph skipped, since the query need not be valid
    /// against every graph's schema. The configured `query.*` limits and
    /// `query.timeoutMs` apply, so a warming run is bounded exactly like a real query.
    #[serde(default)]
    pub cache_warming_query: String,
    /// Enable load-test diagnostics: maintain extra per-connection / per-query
    /// counters and a latency histogram, and answer `CALL slater.diagnostics()`
    /// with live RSS/CPU/cgroup, connection-cap headroom, and failure tallies.
    /// OFF by default — when off, every record path is a single inert branch and
    /// the introspection statement errors, so the normal hot path is unchanged.
    /// Never enable on a production replica (it widens the observable surface).
    #[serde(default = "default_false", deserialize_with = "de::bool")]
    pub load_test_diagnostics: bool,
    /// How often to poll each graph's `current` pointer for a generation change.
    #[serde(default = "default_generation_poll_ms", deserialize_with = "de::u64")]
    pub generation_poll_ms: u64,
    /// What to do when `current` changes under us: `exit` (default) or `swap`.
    #[serde(default = "default_reload_strategy")]
    pub reload_strategy: String,
    /// Graph flagged as the home database in `SHOW DATABASES`. Display metadata only:
    /// it is never used to auto-select a graph for a query. A session that names no
    /// graph (and can read more than one) errors and must name an exact graph in its
    /// `db` field — slater never silently serves a graph the client did not name.
    #[serde(default)]
    pub default_graph: String,
}

impl AppConfig {
    /// Root directory of the local `fs` storage backend (`dataBackend.fs.dir`).
    /// Also the local area the at-rest key file must stay outside of, and the
    /// staging root regardless of which backend serves — kept behind an accessor
    /// so the many call sites read one canonical place.
    pub fn data_dir(&self) -> &str {
        &self.data_backend.fs.dir
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port", deserialize_with = "deser_u16_or_str")]
    pub port: u16,

    // ── Connection-security limits ───────────────────────────────────────────
    // All default ON but generous: invisible to a legitimate client population,
    // a backstop against adversarial load (see `THREAT_MODEL.md` Availability and
    // `docs/HARDENING.md`). The primary control for a read replica is still the
    // network posture (private bind + an L4 proxy); these are defence-in-depth so
    // the bounded-RSS guarantee holds even if the proxy is forgotten.
    /// Largest reassembled Bolt message body accepted from an **authenticated**
    /// reader (a fat parameter map, an `IN`-list, a KNN query vector, a batch).
    #[serde(default = "default_max_message_bytes", deserialize_with = "de::usize")]
    pub max_message_bytes: usize,
    /// Largest reassembled Bolt message body accepted **before `LOGON`**. Only
    /// `HELLO`/`LOGON` arrive pre-auth (a user-agent string + credentials — a few
    /// hundred bytes), so this is tight; it ratchets up to `maxMessageBytes` the
    /// instant authentication succeeds, and back down on `LOGOFF`.
    #[serde(default = "default_max_pre_auth_bytes", deserialize_with = "de::usize")]
    pub max_pre_auth_bytes: usize,
    /// Deadline (ms) for an unauthenticated peer to complete TLS handshake → Bolt
    /// handshake → `LOGON`. Armed at `accept()`, so it bounds the *whole* pre-auth
    /// window as one budget — a peer cannot refresh its allowance by advancing a stage.
    /// Closes the slow-loris a byte cap alone leaves open. 0 = no deadline.
    #[serde(default = "default_login_timeout_ms", deserialize_with = "de::u64")]
    pub login_timeout_ms: u64,
    /// Deadline (ms) for the **TLS handshake** alone, on top of `loginTimeoutMs` —
    /// whichever expires first wins. A handshake is a 2-RTT machine-to-machine exchange,
    /// so it warrants a far tighter bound than a login window that must also cover a
    /// driver's `HELLO`/`LOGON` round trips; and unlike `loginTimeoutMs` (which an
    /// operator may legitimately set to 0 for a slow interactive auth flow) this one
    /// must never lapse, because a peer stalled mid-ClientHello holds a connection slot
    /// while being invisible to every guard that lives behind the handshake. 0 = no
    /// deadline (do not: with `loginTimeoutMs` also 0, a stalled ClientHello would hold
    /// its slot forever and enough of them would exhaust `maxConnections`).
    #[serde(
        default = "default_tls_handshake_timeout_ms",
        deserialize_with = "de::u64"
    )]
    pub tls_handshake_timeout_ms: u64,
    /// Idle read timeout (ms) for an **authenticated** connection between messages.
    /// 0 (default) = no timeout — pooled drivers legitimately hold idle connections.
    #[serde(default = "default_idle_timeout_ms", deserialize_with = "de::u64")]
    pub idle_timeout_ms: u64,
    /// Global cap on concurrent connections. A permit is acquired *before* `accept()`,
    /// so at capacity back-pressure flows into the kernel listen backlog rather than
    /// the heap. Makes the bounded-RSS guarantee hold under adversarial connection load.
    #[serde(default = "default_max_connections", deserialize_with = "de::usize")]
    pub max_connections: usize,
    /// Cap on connections that have **not** yet completed `LOGON`. Must be smaller than
    /// `maxConnections` so authenticated readers always have reachable global headroom;
    /// a flood of anonymous sockets then cannot starve them. 0 = unlimited.
    #[serde(
        default = "default_max_pre_auth_connections",
        deserialize_with = "de::usize"
    )]
    pub max_pre_auth_connections: usize,
    /// Cap on concurrent connections from one source: the full address for IPv4 (/32),
    /// the /64 prefix for IPv6 (an attacker owns a whole /64). Contains one
    /// compromised/misbehaving client on a private network. 0 = unlimited.
    #[serde(
        default = "default_max_connections_per_ip",
        deserialize_with = "de::usize"
    )]
    pub max_connections_per_ip: usize,
    /// Size of the tokio blocking-thread pool that runs query execution and
    /// storage reads. 0 keeps the tokio default (512). Raise it for a remote
    /// backend (S3) under cold-cache bursts, where each in-flight cold read parks
    /// one blocking thread on the network round-trip.
    #[serde(
        default = "default_max_blocking_threads",
        deserialize_with = "de::usize"
    )]
    pub max_blocking_threads: usize,
    /// Cap on argon2id password verifies running **at once**. Password hashing is
    /// deliberately expensive (~19 MiB of scratch and tens of ms of CPU per verify, and
    /// an unknown principal burns the same cost so it cannot be identified by timing),
    /// so an unauthenticated `LOGON` flood is a CPU/memory flood by construction. Each
    /// verify runs on a blocking thread, never on a reactor worker; this caps how many
    /// of those threads (and how much argon2 scratch) auth may hold, leaving the
    /// blocking pool free to keep running queries. Small on purpose — even 4 sustains
    /// ~100 logins/s. 0 = unlimited (do not: a flood would then park the whole blocking
    /// pool and gigabytes of scratch).
    #[serde(
        default = "default_max_concurrent_auth",
        deserialize_with = "de::usize"
    )]
    pub max_concurrent_auth: usize,
    /// Failed `LOGON`s one connection may make before the server closes it. Stops a
    /// single socket from queueing password verifies for its whole login window; a
    /// determined attacker must pay a fresh TCP + Bolt handshake per few attempts, and
    /// is then bounded by `maxConnectionsPerIp` / `maxPreAuthConnections`. Per
    /// *connection*, never per account — so it cannot be abused to lock a user out.
    /// 0 = unlimited.
    #[serde(default = "default_max_auth_failures", deserialize_with = "de::usize")]
    pub max_auth_failures: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// PEM certificate chain path; empty disables TLS (plaintext, for loopback dev).
    #[serde(default)]
    pub cert: String,
    /// PEM private key path; empty disables TLS.
    #[serde(default)]
    pub key: String,
}

impl TlsConfig {
    pub fn enabled(&self) -> bool {
        !self.cert.is_empty() && !self.key.is_empty()
    }
}

/// Which storage backend serves generation files. The byte format is identical
/// across backends; only where the bytes come from differs.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataBackendConfig {
    /// `fs` (local filesystem rooted at `fs.dir`, the default) or `s3`.
    #[serde(default = "default_backend_kind")]
    pub kind: String,
    /// Verify each generation file against the manifest at open (the
    /// copy-completeness guard). `None` ⇒ on. The check is cheap on every
    /// backend: a local file re-hashes its bytes (BLAKE3); S3 reads its
    /// server-computed SHA-256 object checksum from object metadata and compares
    /// it to the manifest — one `HEAD` per file, no body read. Set `false` to
    /// skip it entirely (the manifest MAC + per-block AEAD still apply).
    #[serde(default)]
    pub verify_integrity: Option<bool>,
    /// Local-filesystem backend settings (used when `kind = "fs"`).
    #[serde(default)]
    pub fs: FsBackendConfig,
    /// S3 connection settings (used when `kind = "s3"`).
    #[serde(default)]
    pub s3: S3BackendConfig,
    /// GCS connection settings (used when `kind = "gcs"`).
    #[serde(default)]
    pub gcs: GcsBackendConfig,
}

impl Default for DataBackendConfig {
    fn default() -> Self {
        Self {
            kind: default_backend_kind(),
            verify_integrity: None,
            fs: FsBackendConfig::default(),
            s3: S3BackendConfig::default(),
            gcs: GcsBackendConfig::default(),
        }
    }
}

/// Local-filesystem backend settings: the symmetric `fs` counterpart to
/// [`S3BackendConfig`], so the root directory lives under `dataBackend` like the
/// bucket/prefix do — not at the top level.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FsBackendConfig {
    /// Root directory holding `<graph>/<generation>/` images and `current`
    /// pointers. The `fs` backend serves from here; it is also the local area the
    /// at-rest key file must stay outside of.
    #[serde(default = "default_data_dir")]
    pub dir: String,
}

impl Default for FsBackendConfig {
    fn default() -> Self {
        Self {
            dir: default_data_dir(),
        }
    }
}

impl DataBackendConfig {
    /// Whether to verify each generation file against the manifest at open.
    /// Defaults on — the check is a cheap metadata request on every backend (see
    /// [`verify_integrity`](Self::verify_integrity)).
    pub fn verify_integrity_resolved(&self) -> bool {
        self.verify_integrity.unwrap_or(true)
    }
}

/// S3 (or S3-compatible) connection settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3BackendConfig {
    /// Bucket name.
    #[serde(default)]
    pub bucket: String,
    /// AWS region (e.g. `eu-west-2`).
    #[serde(default)]
    pub region: String,
    /// Custom endpoint URL for an S3-compatible store (MinIO, localstack); empty
    /// uses the standard AWS endpoint for `region`.
    #[serde(default)]
    pub endpoint: String,
    /// Key prefix every generation key is joined under; empty uses the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Use path-style addressing (`endpoint/bucket/key`); required by most
    /// S3-compatible servers.
    #[serde(default, deserialize_with = "de::bool")]
    pub path_style: bool,
    /// AWS access key id. This is the **primary** way to supply S3 credentials
    /// (set it via `dataBackend.s3.awsAccessKey` in config or the
    /// `dataBackend__s3__awsAccessKey` env var). Empty ⇒ fall back to the
    /// standard AWS credential chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`,
    /// shared profile, or instance role).
    #[serde(default)]
    pub aws_access_key: String,
    /// AWS secret access key, paired with `awsAccessKey`. Empty ⇒ AWS chain.
    #[serde(default)]
    pub aws_secret_key: String,
    /// Optional AWS session token, for temporary (STS) credentials. Only used
    /// when `awsAccessKey`/`awsSecretKey` are set.
    #[serde(default)]
    pub aws_session_token: String,
    /// Byte budget for an optional **local-disk second cache tier** in front of
    /// S3 (`store::diskcache`). `0` (the default) disables it. When `> 0` a
    /// cold-from-RAM block is served from local SSD (~0.1 ms) instead of a fresh
    /// S3 GET, surviving in-memory eviction and cutting S3 request count/cost.
    /// `diskCacheDir` is required when this is non-zero. The in-memory LRU index
    /// costs RAM proportional to entry count (~tens of bytes/entry), so it counts
    /// against the configured RSS ceiling.
    #[serde(default, deserialize_with = "de::usize")]
    pub disk_cache_bytes: usize,
    /// Directory for the S3 disk cache (used iff `diskCacheBytes > 0`). Must be a
    /// **real writable volume — never tmpfs** (tmpfs is RAM and would defeat the
    /// bounded-RSS guarantee).
    #[serde(default)]
    pub disk_cache_dir: String,
}

/// GCS connection settings. All fields live under `dataBackend.gcs.*` (env
/// `dataBackend__gcs__*`), mirroring [`S3BackendConfig`] one-for-one; the
/// disk-cache fields are backend-neutral and behave identically.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsBackendConfig {
    /// Bucket name (no `gs://` scheme).
    #[serde(default)]
    pub bucket: String,
    /// Key prefix every generation key is joined under; empty uses the bucket root.
    #[serde(default)]
    pub prefix: String,
    /// Custom endpoint URL for a GCS emulator (e.g. `fake-gcs-server`); empty uses
    /// the standard Google Cloud Storage endpoint.
    #[serde(default)]
    pub endpoint: String,
    /// Path to a **service-account JSON key file**. Empty ⇒ fall back to
    /// Application Default Credentials (GKE Workload Identity, the GCE metadata
    /// server, or a `gcloud`/`GOOGLE_APPLICATION_CREDENTIALS` key). Set it via
    /// `dataBackend.gcs.credentialsPath` / `dataBackend__gcs__credentialsPath`.
    #[serde(default)]
    pub credentials_path: String,
    /// Inline service-account JSON key. Takes precedence over `credentialsPath`
    /// when both are set. Empty ⇒ use `credentialsPath`, else ADC.
    #[serde(default)]
    pub credentials_json: String,
    /// Use **anonymous** (unauthenticated) credentials — for a local GCS emulator
    /// (`fake-gcs-server`) only, never against real GCS. Overrides every other
    /// credential source.
    #[serde(default, deserialize_with = "de::bool")]
    pub anonymous: bool,
    /// Byte budget for the optional **local-disk second cache tier** in front of
    /// GCS (`store::diskcache`); `0` (the default) disables it. Identical in
    /// behaviour to [`S3BackendConfig::disk_cache_bytes`]. `diskCacheDir` is
    /// required when this is non-zero.
    #[serde(default, deserialize_with = "de::usize")]
    pub disk_cache_bytes: usize,
    /// Directory for the GCS disk cache (used iff `diskCacheBytes > 0`). Must be a
    /// **real writable volume — never tmpfs**.
    #[serde(default)]
    pub disk_cache_dir: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheConfig {
    /// Global byte budget for the decompressed/decrypted block LRU.
    #[serde(default = "default_block_cache", deserialize_with = "de::usize")]
    pub block_cache_bytes: usize,
    /// Separate byte budget for the large vector-index pool (Vamana blocks + PQ codes).
    #[serde(default = "default_vector_cache", deserialize_with = "de::usize")]
    pub vector_cache_bytes: usize,
    /// Byte budget for the result LRU. A value of **0 or less disables the pool**
    /// entirely: every query then executes for real (no result reuse) — useful for
    /// honest cold-execution benchmarking, or deployments that never want cached
    /// results. The block and vector pools have no such switch.
    #[serde(
        default = "default_result_cache",
        deserialize_with = "de::usize_floor0"
    )]
    pub result_cache_bytes: usize,
    /// Idle TTL in milliseconds: a cached entry not accessed for this long is
    /// reclaimed by the background maintenance sweep, freeing memory below the
    /// byte budgets. Defaults to 30 minutes. A **negative** value (or zero)
    /// disables the sweep — caches then evict purely on budget pressure, as
    /// before. Pinned PQ codes are never swept.
    #[serde(default = "default_cache_ttl_ms", deserialize_with = "de::i64")]
    pub cache_ttl_ms: i64,
    /// Per-generation byte budget for the range-index (ISAM) decompressed-leaf-block
    /// cache. A business-key write resolve and an indexed range seek both probe the
    /// range index; without this cache each probe re-reads + re-decompresses its leaf
    /// block, so a bulk write over a contiguous key range re-decompresses the same block
    /// once per key. One budget is shared across all of a generation's range readers and
    /// freed when the generation is dropped on swap. **0 disables it** (every probe reads
    /// fresh, the pre-cache behaviour). Defaults to 16 MiB.
    #[serde(
        default = "default_range_index_cache",
        deserialize_with = "de::usize_floor0"
    )]
    pub range_index_cache_bytes: usize,
    /// Residency policy for the dense per-node degree column (`node_degrees.blk`), which
    /// backs the degree-sum `count(endpoint)` fast path. `"lazy"` (default) faults a
    /// ~1 MiB chunk on first touch and frees cold chunks on the idle-TTL sweep — elastic,
    /// so a query that never sums degrees (or touches only part of the id space) doesn't
    /// hold the whole ~733 MB column. `"pinned"` prefaults the whole column at open and
    /// never evicts — preferred for latency-critical or object-store deployments, where a
    /// mid-query range-GET fault (~10–100 ms) is worse than the steady resident cost.
    #[serde(
        default = "default_degree_column",
        deserialize_with = "de::degree_residency"
    )]
    pub degree_column: crate::degree_column::DegreeResidency,
}

impl CacheConfig {
    /// The idle TTL as a `Duration`, or `None` when disabled (a non-positive
    /// `cache_ttl_ms`).
    pub fn cache_ttl(&self) -> Option<std::time::Duration> {
        (self.cache_ttl_ms > 0).then(|| std::time::Duration::from_millis(self.cache_ttl_ms as u64))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexPin {
    pub label: String,
    pub property: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionConfig {
    /// Env var holding the at-rest master key (hex). Empty ⇒ unencrypted reads.
    #[serde(default)]
    pub key_env: String,
    /// File path holding the at-rest master key. Empty ⇒ use `keyEnv` (or none).
    #[serde(default)]
    pub key_file: String,
}

impl EncryptionConfig {
    /// Resolve the at-rest master key (raw bytes) from the configured source.
    /// `keyFile` takes precedence over `keyEnv`; both empty ⇒ `None` (plaintext
    /// generations only). The source holds the key as hex.
    pub fn load_key(&self) -> Result<Option<Vec<u8>>> {
        let hex = if !self.key_file.is_empty() {
            std::fs::read_to_string(&self.key_file)
                .with_context(|| format!("read encryption key file {}", self.key_file))?
        } else if !self.key_env.is_empty() {
            std::env::var(&self.key_env)
                .with_context(|| format!("read encryption key env var {}", self.key_env))?
        } else {
            return Ok(None);
        };
        let key =
            graph_format::crypto::hex_decode(&hex).context("decode at-rest master key hex")?;
        if key.is_empty() {
            anyhow::bail!("the configured at-rest master key is empty");
        }
        Ok(Some(key))
    }

    /// Refuse a `keyFile` that resolves *inside* `data_dir`. The data directory is
    /// the one surface this threat model treats as attacker-writable, so a master
    /// key staged there could be substituted by the same attacker who rewrites the
    /// generations it authenticates — collapsing the MAC's trust root (see
    /// `THREAT_MODEL.md`, "Trust boundary"). This is a defence-in-depth tripwire,
    /// not a complete defence: it does not stop a `keyFile` pointing at some *other*
    /// attacker-writable path, which only deployment-level isolation can prevent.
    /// Best-effort: if either path cannot be canonicalised (e.g. the key file does
    /// not exist) the check is skipped and `load_key` surfaces the real error.
    pub fn check_key_file_outside_data_dir(&self, data_dir: &str) -> Result<()> {
        if self.key_file.is_empty() {
            return Ok(());
        }
        let (Ok(key), Ok(data)) = (
            std::fs::canonicalize(&self.key_file),
            std::fs::canonicalize(data_dir),
        ) else {
            return Ok(());
        };
        if key.starts_with(&data) {
            anyhow::bail!(
                "encryption keyFile {} resolves inside the data directory {} — the master key \
                 must live outside the attacker-writable data surface; move it to a path the \
                 data-publishing principal cannot write",
                self.key_file,
                data_dir
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConfig {
    #[serde(default = "default_max_rows", deserialize_with = "de::u64")]
    pub max_rows: u64,
    #[serde(default = "default_timeout_ms", deserialize_with = "de::u64")]
    pub timeout_ms: u64,
    /// Per-query budget on intermediate elements *retained* in memory: rows
    /// materialised by comprehensions, UNWIND, list concatenation, aggregate/DISTINCT
    /// buffers and variable-length paths. This is the memory (OOM) guard — kept tight,
    /// and the only budget the server-wide aggregate (`maxIntermediateGlobal`) tracks.
    /// 0 disables it. Distinct from `maxScan` on purpose (see there): a `count(*)`
    /// over a multi-hop expansion retains nothing (count-pushdown), so it is bounded by
    /// `maxScan`, not this.
    #[serde(default = "default_max_intermediate", deserialize_with = "de::u64")]
    pub max_intermediate: u64,
    /// Per-query budget on *transient* walk elements that are produced and immediately
    /// discarded — the adjacency reads and per-row tallies of a count-pushdown
    /// (`RETURN count(*)`) multi-hop walk, which holds O(1) rows and a structurally
    /// bounded frontier, so its RSS is flat regardless of this value. This bounds total
    /// walked *work* (a runaway/geometric-explosion backstop), not memory; the primary
    /// governor for such queries is `timeoutMs`. It does **not** draw down the
    /// server-wide `maxIntermediateGlobal` aggregate (transient work holds no memory a
    /// concurrent query competes for). 0 disables it. Generous by default because
    /// raising it is memory-safe — see the knee sweep in perf/PERF_CURRENT_STATUS.md.
    #[serde(default = "default_max_scan", deserialize_with = "de::u64")]
    pub max_scan: u64,
    /// Server-wide ceiling on the *sum* of all in-flight queries' intermediate
    /// elements. `max_intermediate` bounds one query; this bounds the aggregate so
    /// `N` concurrent heavy queries cannot multiply into an OOM. A charge that would
    /// cross it fails that query with a clean, retryable error. 0 disables the guard.
    #[serde(
        default = "default_max_intermediate_global",
        deserialize_with = "de::u64"
    )]
    pub max_intermediate_global: u64,
    /// Optional cap on how many nodes a single `shortestPath()` global-visited BFS
    /// may discover; 0 = unlimited (default). Dedicated to that one O(V) operation so
    /// tiny-memory deployments can bound it without shrinking the general
    /// `maxIntermediate` budget every other query shares.
    #[serde(
        default = "default_max_shortest_path_explore",
        deserialize_with = "de::u64"
    )]
    pub max_shortest_path_explore: u64,
    /// Max worker threads for per-query parallelism (shortestPath BFS frontier
    /// expansion, multi-hop expansion, brute-force kNN, anchor scans, …).
    /// 1 (default) keeps queries sequential — the safe choice for a throughput-oriented
    /// read server (per-query parallelism steals cores from concurrent queries).
    /// Raise it to overlap the I/O-bound CSR block reads of a large batch across
    /// cores; the effective fanout is `min(this, available cores)`.
    #[serde(default = "default_max_fanout", deserialize_with = "de::usize")]
    pub max_fanout: usize,
    /// Effective (upper-bound) degree at or above which a node's adjacency is **streamed**
    /// in bounded chunks instead of materialised whole, so a high-degree hub cannot inflate
    /// a wide parallel gather (the fan-out OOM guard). Must be `>=` the build's
    /// `hubDegreeFloor` so the sidecar holds an exact degree for every streamable node.
    #[serde(default = "default_adj_stream_threshold", deserialize_with = "de::u64")]
    pub adj_stream_threshold: u64,
    /// Edges per chunk handed to the streaming adjacency reader — bounds a streamed hub's
    /// live neighbour buffer to O(this) regardless of degree.
    #[serde(default = "default_adj_stream_chunk", deserialize_with = "de::usize")]
    pub adj_stream_chunk: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorQueryConfig {
    #[serde(default = "default_beam_width", deserialize_with = "deser_u32_or_str")]
    pub beam_width: u32,
    #[serde(default = "default_max_hops", deserialize_with = "deser_u32_or_str")]
    pub max_hops: u32,
}

// ── Defaults ─────────────────────────────────────────────────────────────────

fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}
fn default_data_dir() -> String {
    "/data".into()
}
fn default_acl_path() -> String {
    "acl.json".into()
}
fn default_backend_kind() -> String {
    "fs".into()
}
fn default_max_blocking_threads() -> usize {
    0 // 0 ⇒ keep the tokio default (512)
}
fn default_bind() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    7687
}
// Connection-security defaults: on, but generous (see `ServerConfig`).
fn default_max_message_bytes() -> usize {
    64 * 1024 * 1024 // matches the historical chunk::MAX_MESSAGE_BYTES
}
fn default_max_pre_auth_bytes() -> usize {
    64 * 1024 // HELLO/LOGON are a few hundred bytes; 64 KiB is ample headroom
}
fn default_login_timeout_ms() -> u64 {
    10_000
}
fn default_tls_handshake_timeout_ms() -> u64 {
    5_000 // a 2-RTT exchange; generous even for a bad mobile link, brutal to a stall
}
fn default_idle_timeout_ms() -> u64 {
    0 // off — pooled drivers legitimately hold idle authenticated connections
}
fn default_max_connections() -> usize {
    16_384
}
fn default_max_pre_auth_connections() -> usize {
    4_096
}
fn default_max_connections_per_ip() -> usize {
    1_024
}
fn default_max_concurrent_auth() -> usize {
    4 // ~100 logins/s of headroom; bounds argon2 scratch at ~4 × 19 MiB
}
fn default_max_auth_failures() -> usize {
    3 // a fat-fingered password is retried, a credential-stuffer is hung up on
}
fn default_log_level() -> String {
    // `info` already surfaces the per-query `query executed` timing summary (the
    // instrumentation gate sits at INFO), without the chatty `debug` AWS-SDK /
    // wire tracing. Matches the documented default in `help.rs`.
    "info".into()
}
// Cache defaults are sized for the typical deployment envelope of 100–200 MB
// total resident memory (resident ≈ the three cache budgets + fixed overhead).
fn default_block_cache() -> usize {
    64 * 1024 * 1024
}
fn default_vector_cache() -> usize {
    // Sized to hold a typical brute-force estate's resident, pre-decoded vector
    // matrices (the no-gather kNN path) as well as Vamana PQ codes. It is a *cap*,
    // not a reservation — empty for deployments without vector indexes — and the
    // matrix path falls back to the per-query gather when a group does not fit, so
    // the bound is never exceeded. With block (64 MiB) + result (16 MiB) this keeps
    // the default envelope at ~144 MiB, inside the 100–200 MiB target.
    64 * 1024 * 1024
}
fn default_result_cache() -> usize {
    16 * 1024 * 1024
}
fn default_range_index_cache() -> usize {
    16 * 1024 * 1024
}
fn default_degree_column() -> crate::degree_column::DegreeResidency {
    crate::degree_column::DegreeResidency::Lazy
}
fn default_cache_ttl_ms() -> i64 {
    30 * 60 * 1000
}
fn default_generation_poll_ms() -> u64 {
    5_000
}
fn default_reload_strategy() -> String {
    "exit".into()
}
fn default_max_rows() -> u64 {
    100_000
}
fn default_timeout_ms() -> u64 {
    30_000
}
// ~48 bytes per element (size_of::<Val>()), so 1M elements bounds a single
// query's intermediate materialisation at roughly 48 MB worst case — sized for
// deployments with 100–200 MB memory limits and a few concurrent queries.
fn default_max_intermediate() -> u64 {
    1_000_000
}
// Transient walk-work budget for count-pushdown traversals (`query.maxScan`). These
// retain ~O(1) memory, so this charges no retained bytes and is memory-safe to set high —
// it only backstops runaway *work*; the 30 s `timeoutMs` is the real governor. Peak anon
// RSS was flat ~2–2.6 GB across the whole 1M→200M knee sweep on the 91.6M-node Wikidata
// graph (perf/PERF_CURRENT_STATUS.md) — the budget value is decoupled from RSS, so raising
// it costs no memory and only lets more genuinely-huge mega-hub `count(*)`s complete instead
// of trip. 500M is chosen generously on that basis (a finite backstop still catches a
// geometric blow-up sooner than the timeout would).
fn default_max_scan() -> u64 {
    500_000_000
}
// Server-wide companion to `max_intermediate`. At ~48 bytes per element the 8M
// default bounds the aggregate live intermediate memory of *all* concurrent
// queries at roughly 384 MB — generous enough for normal concurrency (a point
// lookup charges ~0; only memory-heavy expand/aggregate queries draw it down) yet
// enough to stop `N × maxIntermediate` from OOMing under a flood of heavy queries.
// 0 disables the guard.
fn default_max_intermediate_global() -> u64 {
    8_000_000
}
fn default_max_shortest_path_explore() -> u64 {
    0 // unlimited — preserves the AnyShortest "always succeeds in O(V+E)" guarantee
}
fn default_max_fanout() -> usize {
    1 // sequential by default — opt in to per-query parallelism explicitly
}
fn default_adj_stream_threshold() -> u64 {
    8192 // >= the build hubDegreeFloor (1024); above it a node streams
}
fn default_adj_stream_chunk() -> usize {
    8192 // edges per streamed chunk
}
fn default_beam_width() -> u32 {
    64
}
fn default_max_hops() -> u32 {
    256
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            block_cache_bytes: default_block_cache(),
            vector_cache_bytes: default_vector_cache(),
            result_cache_bytes: default_result_cache(),
            cache_ttl_ms: default_cache_ttl_ms(),
            range_index_cache_bytes: default_range_index_cache(),
            degree_column: default_degree_column(),
        }
    }
}
/// Writable-layer (delta) configuration. **Off by default** — with `enabled`
/// false every query serves the pure immutable core exactly as before, no WAL is
/// opened, and write statements are rejected as read-only. See
/// `docs/WRITABLE-PLAN.md` and D44.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaConfig {
    /// Master switch for the writable layer. Off ⇒ reads never build a
    /// `MergedView` and writes are refused.
    #[serde(default = "default_false", deserialize_with = "de::bool")]
    pub enabled: bool,
    /// Directory holding per-graph WAL segments (the local durability floor). A
    /// relative path is resolved under the data directory; one graph's segments
    /// live under `<walDir>/<graph>/`. Must be a durable local volume, never
    /// ephemeral instance storage (see D44 — the floor is always local disk).
    #[serde(default = "default_wal_dir")]
    pub wal_dir: String,
    /// Byte budget for a graph's in-RAM active memtable before it is flushed to an
    /// immutable L0 segment (Phase 4c/4d). A write that pushes the memtable past this
    /// spills it to disk and resets it empty, bounding resident memtable RAM. Defaults
    /// to 64 MiB.
    #[serde(default = "default_memtable_bytes", deserialize_with = "de::usize")]
    pub memtable_bytes: usize,
    /// Number of sealed L0 segments that triggers an L0→L0 compaction (Phase 4d-i):
    /// once the stack reaches this many levels, a write merges them into one, bounding
    /// read fan-out and reclaiming overwritten/tombstoned space. Cheap (O(delta), no
    /// core rebuild). Defaults to 4; set to 0 to disable auto-compaction.
    #[serde(
        default = "default_l0_compaction_trigger",
        deserialize_with = "de::usize"
    )]
    pub l0_compaction_trigger: usize,
    /// Byte budget for the **whole** delta (active memtable + every L0 level) before it is
    /// flushed into a durable core **segment** — the T2 rung of the D50 ladder (Phase 4
    /// writer, Phase 6 auto-trigger). Distinct from `memtableBytes`, which drains only the
    /// *active memtable* into an L0 level: this folds the *entire* delta into an upper core
    /// segment, the cheap O(delta) intermediate that keeps the delta small without an O(core)
    /// consolidation rebuild. A flushed segment is then compacted with its peers once the
    /// stack exceeds `maxUpperSegments` (T3), and consolidation (`deltaCorePercent`) stays
    /// rare. **Off by default (0)** — like `deltaCorePercent`, folding the delta into the core
    /// is a durable heavyweight operation operators opt into; the explicit
    /// `flush_graph_to_segment` path is unaffected. Fires for a resident **or** an off-heap
    /// (`offHeapL0`) L0 stack — the off-heap fold lands at the `SegmentData` level (Phase 7.5).
    #[serde(default, deserialize_with = "de::usize")]
    pub segment_flush_bytes: usize,
    /// Maximum number of upper **core segments** a served set may carry before a T3
    /// segment→segment compaction is admissible (Phase 5 slice 5.3 — the fourth rung of the
    /// D50 ladder). A point read may consult every upper segment, so once the stack exceeds
    /// this the size-tiered run selector ([`crate::merge_segment::select_compaction_run`])
    /// picks a contiguous run to fold, bounding read fan-out. Cheap (O(segments), no core
    /// rebuild), like L0→L0 compaction. Defaults to 8; 0 disables admission (the explicit
    /// `compact_graph_segments(start, end)` path is unaffected). The write path auto-fires this
    /// from `maybe_maintain_delta` once the served stack exceeds it (Phase 6 closing slice —
    /// the segment-aware write resolve gate is met), beside the explicit
    /// `compact_graph_segments_auto` entry point.
    #[serde(default = "default_max_upper_segments", deserialize_with = "de::usize")]
    pub max_upper_segments: usize,
    /// Auto-consolidation threshold as a **percent of the core's size** (Phase 4d-ii-b):
    /// once the delta's changed-entity count reaches `deltaCorePercent`% of the served
    /// generation's entity count, a background consolidation folds it into a fresh core.
    /// Expressing it as a fraction of core (not an absolute byte count) bounds write
    /// amplification independent of core size — the rebuild is O(core), so it must stay
    /// rare. A full rebuild of a large core is expensive (~an hour on a 91M-node core),
    /// so this is **off by default** (0): operators opt in, or rely on the manual `CALL
    /// slater.consolidate()` / a schedule. Typical opt-in values are 5–25.
    #[serde(default, deserialize_with = "de::usize")]
    pub delta_core_percent: usize,
    /// Hard cap on total resident delta bytes (Phase 4d-ii-b): a write that pushes the
    /// delta past this **throttles** — it ensures a consolidation is draining and waits
    /// for it before acking, the backstop that stops runaway delta growth from
    /// exhausting RAM. Off by default (0 = never throttle). Set well above
    /// `deltaCorePercent`'s working set; hitting it is an operational signal, not routine.
    #[serde(default, deserialize_with = "de::usize")]
    pub delta_hard_bytes: usize,
    /// Off-peak window (cron-style, **server-local** time) that gates the
    /// fraction-of-core auto-consolidation (`deltaCorePercent`): a due consolidation
    /// fires only when the current local time is inside this window (or when it is
    /// unset). The `deltaHardBytes` throttle is unaffected — it fires anytime as the
    /// OOM backstop. Five fields `minute hour day-of-month month day-of-week`; the
    /// window has hour granularity (the minute field is accepted but not used). Empty
    /// (the default) = no gating (fire whenever due). Example: `"0 1-5 * * *"` =
    /// 01:00–05:59 daily. Parsed by [`crate::cron_window::CronWindow`].
    #[serde(default)]
    pub consolidate_window: String,
    /// Path to the `slater-build` binary invoked to rebuild a fresh generation
    /// during consolidation (Phase 1d). A bare name is resolved on `PATH`; an
    /// absolute path pins a specific binary. Defaults to `slater-build`.
    #[serde(default = "default_builder_bin")]
    pub builder_bin: String,
    /// Read sealed L0 delta segments **off-heap** (Phase C): a flushed level spills to a
    /// directory of block files whose per-entity payloads page through the server's shared
    /// `BlockCache` on demand, rather than being reloaded whole into RAM — so the resident
    /// footprint of the L0 stack is a compact index, not the full delta. Off by default;
    /// when off, a flush writes the resident single-file L0 segment exactly as before.
    /// While on, L0→L0 compaction is skipped (consolidation bounds the level count); see
    /// `docs/WRITABLE-PROGRESS.md` and D54.
    #[serde(default)]
    pub off_heap_l0: bool,
    /// Grace period, in **seconds**, before the segment/set GC sweep (Phase 7 slice 7.2 —
    /// [`crate::server::Graphs::gc_orphan_segments`]) reclaims an orphaned `segments/<uuid>/`
    /// directory or stale `sets/<uuid>.json` that the served set no longer references (a
    /// compaction supersedes the run's dirs, a retarget the whole prior set). The grace runs
    /// from the sweep's *first observation* of the orphan (not its file mtime), so an in-flight
    /// reader that opened its `Generation` before the swap finishes reading before the delete —
    /// the reader-safety margin. **0 disables the auto sweep** (off by default, like
    /// `segmentFlushBytes`); a positive value both enables it and sets the grace. When enabled,
    /// the sweep fires after the orphan-creating events (a T3 compaction, a consolidation).
    #[serde(default, deserialize_with = "de::u64")]
    pub segment_gc_grace_secs: u64,
}

impl Default for DeltaConfig {
    fn default() -> Self {
        Self {
            enabled: default_false(),
            wal_dir: default_wal_dir(),
            memtable_bytes: default_memtable_bytes(),
            l0_compaction_trigger: default_l0_compaction_trigger(),
            segment_flush_bytes: 0,
            max_upper_segments: default_max_upper_segments(),
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: String::new(),
            builder_bin: default_builder_bin(),
            off_heap_l0: false,
            segment_gc_grace_secs: 0,
        }
    }
}

fn default_l0_compaction_trigger() -> usize {
    4
}

fn default_max_upper_segments() -> usize {
    8
}

fn default_wal_dir() -> String {
    "wal".to_string()
}

fn default_builder_bin() -> String {
    "slater-build".to_string()
}

fn default_memtable_bytes() -> usize {
    64 << 20
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_rows: default_max_rows(),
            timeout_ms: default_timeout_ms(),
            max_intermediate: default_max_intermediate(),
            max_scan: default_max_scan(),
            max_intermediate_global: default_max_intermediate_global(),
            max_shortest_path_explore: default_max_shortest_path_explore(),
            max_fanout: default_max_fanout(),
            adj_stream_threshold: default_adj_stream_threshold(),
            adj_stream_chunk: default_adj_stream_chunk(),
        }
    }
}
impl Default for VectorQueryConfig {
    fn default() -> Self {
        Self {
            beam_width: default_beam_width(),
            max_hops: default_max_hops(),
        }
    }
}

/// What the generation guard does when a graph's `current` pointer changes under
/// a running server (config `reloadStrategy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadStrategy {
    /// Log fatal and exit non-zero so the orchestrator restarts the process
    /// cleanly against the new generation (default — the caches and
    /// `Arc<Generation>`s are rebuilt from scratch on restart).
    Exit,
    /// Open + validate the new generation and atomically swap it in, keeping the
    /// old one serving in-flight queries to completion. A corrupt/incomplete new
    /// generation is refused and the old one keeps serving.
    Swap,
}

impl AppConfig {
    /// Parse `reload_strategy` into a [`ReloadStrategy`]. An unknown value is an
    /// error so a fat-fingered config is caught at boot rather than silently
    /// defaulting to a behaviour the operator did not ask for.
    pub fn reload_strategy(&self) -> Result<ReloadStrategy> {
        match self.reload_strategy.as_str() {
            "exit" => Ok(ReloadStrategy::Exit),
            "swap" => Ok(ReloadStrategy::Swap),
            other => {
                anyhow::bail!("unknown reloadStrategy {other:?}; expected \"exit\" or \"swap\"")
            }
        }
    }

    /// The generation-poll interval as a `Duration`.
    pub fn generation_poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.generation_poll_ms)
    }
}

/// Load and deserialise the Slater config via the house-standard layered loader.
pub fn load() -> Result<AppConfig> {
    let root = hs_utils::config::load_layered_value()?;
    serde_json::from_value(root).context("deserialise Slater config")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_consolidate_window_deserialises_and_defaults_empty() {
        // Absent ⇒ empty (no gating), and the camelCase key is captured verbatim for the
        // cron parser (`crate::cron_window::CronWindow::parse`) to validate at startup.
        let default: DeltaConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(default.consolidate_window, "");

        let cfg: DeltaConfig =
            serde_json::from_value(serde_json::json!({ "consolidateWindow": "0 1-5 * * *" }))
                .unwrap();
        assert_eq!(cfg.consolidate_window, "0 1-5 * * *");
        assert!(
            crate::cron_window::CronWindow::parse(&cfg.consolidate_window)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn degree_column_defaults_lazy_and_parses_pinned() {
        use crate::degree_column::DegreeResidency;
        let default: CacheConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(default.degree_column, DegreeResidency::Lazy);

        for (s, want) in [
            ("lazy", DegreeResidency::Lazy),
            ("pinned", DegreeResidency::Pinned),
            ("PINNED", DegreeResidency::Pinned), // case-insensitive
        ] {
            let cfg: CacheConfig =
                serde_json::from_value(serde_json::json!({ "degreeColumn": s })).unwrap();
            assert_eq!(cfg.degree_column, want, "degreeColumn={s}");
        }

        // An unknown value is a hard config error, not a silent fallback.
        assert!(serde_json::from_value::<CacheConfig>(
            serde_json::json!({ "degreeColumn": "sometimes" })
        )
        .is_err());
    }

    #[test]
    fn cache_ttl_defaults_to_30_minutes() {
        let cfg: CacheConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.cache_ttl_ms, 30 * 60 * 1000);
        assert_eq!(
            cfg.cache_ttl(),
            Some(std::time::Duration::from_secs(30 * 60))
        );
    }

    #[test]
    fn cache_ttl_parses_number_and_numeric_string() {
        let from_num: CacheConfig =
            serde_json::from_value(serde_json::json!({ "cacheTtlMs": 1000 })).unwrap();
        assert_eq!(
            from_num.cache_ttl(),
            Some(std::time::Duration::from_secs(1))
        );

        // The `de::i64` deserializer also accepts a numeric string (config layer parity).
        let from_str: CacheConfig =
            serde_json::from_value(serde_json::json!({ "cacheTtlMs": "1000" })).unwrap();
        assert_eq!(
            from_str.cache_ttl(),
            Some(std::time::Duration::from_secs(1))
        );
    }

    #[test]
    fn cache_ttl_negative_or_zero_disables() {
        for v in [0, -1, -1000] {
            let cfg: CacheConfig =
                serde_json::from_value(serde_json::json!({ "cacheTtlMs": v })).unwrap();
            assert_eq!(cfg.cache_ttl(), None, "cacheTtlMs={v} should disable");
        }
        // Negative numeric string too.
        let cfg: CacheConfig =
            serde_json::from_value(serde_json::json!({ "cacheTtlMs": "-5" })).unwrap();
        assert_eq!(cfg.cache_ttl(), None);
    }

    #[test]
    fn require_acl_stamp_parses_bool_and_string() {
        // The layered loader stringifies every scalar leaf, so the config must
        // accept both a raw boolean and the stringified "true"/"false" form.
        let from_bool: AppConfig =
            serde_json::from_value(serde_json::json!({ "server": {}, "requireAclStamp": false }))
                .expect("raw bool requireAclStamp");
        assert!(!from_bool.require_acl_stamp);

        let from_str: AppConfig =
            serde_json::from_value(serde_json::json!({ "server": {}, "requireAclStamp": "false" }))
                .expect("stringified requireAclStamp (layered-loader form)");
        assert!(!from_str.require_acl_stamp);

        let true_str: AppConfig =
            serde_json::from_value(serde_json::json!({ "server": {}, "requireAclStamp": "true" }))
                .expect("stringified true");
        assert!(true_str.require_acl_stamp);

        // Absent ⇒ defaults to true (protections on by default).
        let absent: AppConfig =
            serde_json::from_value(serde_json::json!({ "server": {} })).expect("absent default");
        assert!(absent.require_acl_stamp);
    }

    #[test]
    fn load_test_diagnostics_defaults_off_and_parses_bool_and_string() {
        // Absent ⇒ off, so the diagnostics surface and counters stay dormant
        // unless explicitly enabled.
        let absent: AppConfig =
            serde_json::from_value(serde_json::json!({ "server": {} })).expect("absent default");
        assert!(!absent.load_test_diagnostics);

        // Raw bool and the stringified layered-loader form both enable it.
        let from_bool: AppConfig = serde_json::from_value(
            serde_json::json!({ "server": {}, "loadTestDiagnostics": true }),
        )
        .expect("raw bool loadTestDiagnostics");
        assert!(from_bool.load_test_diagnostics);

        let from_str: AppConfig = serde_json::from_value(
            serde_json::json!({ "server": {}, "loadTestDiagnostics": "true" }),
        )
        .expect("stringified loadTestDiagnostics (layered-loader form)");
        assert!(from_str.load_test_diagnostics);
    }

    #[test]
    fn key_file_inside_data_dir_is_refused() {
        let dir = std::env::temp_dir().join("slater_keyfile_guard_test");
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        // A key staged inside the (attacker-writable) data dir is refused.
        let inside = data.join("master.hex");
        std::fs::write(&inside, "00112233").unwrap();
        let cfg = EncryptionConfig {
            key_env: String::new(),
            key_file: inside.to_str().unwrap().to_string(),
        };
        let err = cfg
            .check_key_file_outside_data_dir(data.to_str().unwrap())
            .expect_err("keyFile inside the data dir must be refused");
        assert!(
            format!("{err:#}").contains("inside the data directory"),
            "expected the data-dir containment error, got: {err:#}"
        );

        // The same key one level up (outside the data dir) is accepted.
        let outside = dir.join("master.hex");
        std::fs::write(&outside, "00112233").unwrap();
        let ok = EncryptionConfig {
            key_env: String::new(),
            key_file: outside.to_str().unwrap().to_string(),
        };
        assert!(ok
            .check_key_file_outside_data_dir(data.to_str().unwrap())
            .is_ok());

        // No keyFile (keyEnv or plaintext) ⇒ nothing to check.
        let none = EncryptionConfig {
            key_env: "SOME_VAR".into(),
            key_file: String::new(),
        };
        assert!(none
            .check_key_file_outside_data_dir(data.to_str().unwrap())
            .is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn server_security_limits_default_on_and_generous() {
        let cfg: ServerConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg.max_message_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.max_pre_auth_bytes, 64 * 1024);
        assert_eq!(cfg.login_timeout_ms, 10_000);
        assert_eq!(cfg.idle_timeout_ms, 0);
        assert_eq!(cfg.max_connections, 16_384);
        assert_eq!(cfg.max_pre_auth_connections, 4_096);
        assert_eq!(cfg.max_connections_per_ip, 1_024);
        // The pre-auth budget must leave global headroom for authenticated readers.
        assert!(cfg.max_pre_auth_connections < cfg.max_connections);
        // Password hashing is bounded by default: concurrent verifies are capped well
        // below the blocking pool that query execution shares, and one connection gets
        // a small allowance of failed LOGONs.
        assert_eq!(cfg.max_concurrent_auth, 4);
        assert_eq!(cfg.max_auth_failures, 3);
        assert!(
            cfg.max_concurrent_auth > 0,
            "unbounded by default would be a DoS"
        );
        assert!(cfg.max_concurrent_auth < cfg.max_blocking_threads.max(512));
        // The TLS handshake is bounded by default and *more* tightly than the login
        // window it sits inside: a peer stalled mid-ClientHello holds a connection slot
        // while being invisible to every guard behind the handshake, so it must not be
        // given a window sized for a driver's HELLO/LOGON round trips.
        assert_eq!(cfg.tls_handshake_timeout_ms, 5_000);
        assert!(
            cfg.tls_handshake_timeout_ms > 0,
            "unbounded by default is the slow-loris hole"
        );
        assert!(cfg.tls_handshake_timeout_ms < cfg.login_timeout_ms);
    }

    #[test]
    fn server_security_limits_parse_number_and_numeric_string() {
        // The layered loader stringifies every scalar, so each limit must accept
        // both a raw number and its numeric-string form.
        let from_num: ServerConfig = serde_json::from_value(serde_json::json!({
            "maxMessageBytes": 1024,
            "maxPreAuthBytes": 256,
            "loginTimeoutMs": 2000,
            "tlsHandshakeTimeoutMs": 750,
            "idleTimeoutMs": 60000,
            "maxConnections": 8,
            "maxPreAuthConnections": 4,
            "maxConnectionsPerIp": 2,
            "maxConcurrentAuth": 2,
            "maxAuthFailures": 5,
        }))
        .unwrap();
        let from_str: ServerConfig = serde_json::from_value(serde_json::json!({
            "maxMessageBytes": "1024",
            "maxPreAuthBytes": "256",
            "loginTimeoutMs": "2000",
            "tlsHandshakeTimeoutMs": "750",
            "idleTimeoutMs": "60000",
            "maxConnections": "8",
            "maxPreAuthConnections": "4",
            "maxConnectionsPerIp": "2",
            "maxConcurrentAuth": "2",
            "maxAuthFailures": "5",
        }))
        .unwrap();
        for cfg in [&from_num, &from_str] {
            assert_eq!(cfg.max_message_bytes, 1024);
            assert_eq!(cfg.max_pre_auth_bytes, 256);
            assert_eq!(cfg.login_timeout_ms, 2000);
            assert_eq!(cfg.tls_handshake_timeout_ms, 750);
            assert_eq!(cfg.idle_timeout_ms, 60000);
            assert_eq!(cfg.max_connections, 8);
            assert_eq!(cfg.max_pre_auth_connections, 4);
            assert_eq!(cfg.max_connections_per_ip, 2);
            assert_eq!(cfg.max_concurrent_auth, 2);
            assert_eq!(cfg.max_auth_failures, 5);
        }
    }
}
