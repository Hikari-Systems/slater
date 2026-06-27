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
    /// Deadline (ms) for an unauthenticated peer to complete handshake → `LOGON`.
    /// Closes the slow-loris a byte cap alone leaves open. 0 = no deadline.
    #[serde(default = "default_login_timeout_ms", deserialize_with = "de::u64")]
    pub login_timeout_ms: u64,
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
}

impl Default for DataBackendConfig {
    fn default() -> Self {
        Self {
            kind: default_backend_kind(),
            verify_integrity: None,
            fs: FsBackendConfig::default(),
            s3: S3BackendConfig::default(),
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
fn default_log_level() -> String {
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
        }
    }
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
    }

    #[test]
    fn server_security_limits_parse_number_and_numeric_string() {
        // The layered loader stringifies every scalar, so each limit must accept
        // both a raw number and its numeric-string form.
        let from_num: ServerConfig = serde_json::from_value(serde_json::json!({
            "maxMessageBytes": 1024,
            "maxPreAuthBytes": 256,
            "loginTimeoutMs": 2000,
            "idleTimeoutMs": 60000,
            "maxConnections": 8,
            "maxPreAuthConnections": 4,
            "maxConnectionsPerIp": 2,
        }))
        .unwrap();
        let from_str: ServerConfig = serde_json::from_value(serde_json::json!({
            "maxMessageBytes": "1024",
            "maxPreAuthBytes": "256",
            "loginTimeoutMs": "2000",
            "idleTimeoutMs": "60000",
            "maxConnections": "8",
            "maxPreAuthConnections": "4",
            "maxConnectionsPerIp": "2",
        }))
        .unwrap();
        for cfg in [&from_num, &from_str] {
            assert_eq!(cfg.max_message_bytes, 1024);
            assert_eq!(cfg.max_pre_auth_bytes, 256);
            assert_eq!(cfg.login_timeout_ms, 2000);
            assert_eq!(cfg.idle_timeout_ms, 60000);
            assert_eq!(cfg.max_connections, 8);
            assert_eq!(cfg.max_pre_auth_connections, 4);
            assert_eq!(cfg.max_connections_per_ip, 2);
        }
    }
}
