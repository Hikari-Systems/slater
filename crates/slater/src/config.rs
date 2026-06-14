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
    /// Root directory holding `<graph>/<generation>/` images and `current` pointers.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port", deserialize_with = "deser_u16_or_str")]
    pub port: u16,
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
    /// Per-query budget on intermediate elements materialised by comprehensions,
    /// UNWIND, list concatenation, aggregate buffers and variable-length paths;
    /// 0 disables the budget.
    #[serde(default = "default_max_intermediate", deserialize_with = "de::u64")]
    pub max_intermediate: u64,
    /// Optional cap on how many nodes a single `shortestPath()` global-visited BFS
    /// may discover; 0 = unlimited (default). Dedicated to that one O(V) operation so
    /// tiny-memory deployments can bound it without shrinking the general
    /// `maxIntermediate` budget every other query shares.
    #[serde(
        default = "default_max_shortest_path_explore",
        deserialize_with = "de::u64"
    )]
    pub max_shortest_path_explore: u64,
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
fn default_data_dir() -> String {
    "/data".into()
}
fn default_acl_path() -> String {
    "acl.json".into()
}
fn default_bind() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    7687
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
    32 * 1024 * 1024
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
fn default_max_shortest_path_explore() -> u64 {
    0 // unlimited — preserves the AnyShortest "always succeeds in O(V+E)" guarantee
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
            max_shortest_path_explore: default_max_shortest_path_explore(),
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
}
