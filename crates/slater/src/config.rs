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
    /// Byte budget for the result LRU.
    #[serde(default = "default_result_cache", deserialize_with = "de::usize")]
    pub result_cache_bytes: usize,
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
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryConfig {
    #[serde(default = "default_max_rows", deserialize_with = "de::u64")]
    pub max_rows: u64,
    #[serde(default = "default_timeout_ms", deserialize_with = "de::u64")]
    pub timeout_ms: u64,
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
fn default_block_cache() -> usize {
    256 * 1024 * 1024
}
fn default_vector_cache() -> usize {
    128 * 1024 * 1024
}
fn default_result_cache() -> usize {
    32 * 1024 * 1024
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
        }
    }
}
impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_rows: default_max_rows(),
            timeout_ms: default_timeout_ms(),
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
