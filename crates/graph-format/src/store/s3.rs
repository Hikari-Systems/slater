// SPDX-License-Identifier: Apache-2.0
//! S3 / object-store backend (enabled by the `s3` cargo feature).
//!
//! Generation files are S3 objects under a key prefix; the readers' positional
//! reads become HTTP `Range` GETs — the same explicit, bounded-read model as the
//! local `pread` backend (no whole-object download, no mmap).
//!
//! Integrity is verified from S3's **server-computed SHA-256 object checksum**
//! via a metadata request (no body read): the builder sends each object's
//! SHA-256 on `PUT` (S3 validates it against the bytes and stores it), and at
//! open the backend reads it back with `HEAD` + checksum mode and compares it to
//! the value recorded in the manifest. SHA-256 is collision-resistant and the
//! stored checksum is server-authoritative, so this is a content-grade check at
//! one metadata request per file. When an object carries no server-stored
//! SHA-256 (e.g. it was uploaded with S3's default full-object checksum,
//! CRC64-NVME, which the manifest's SHA-256 cannot be compared against), the
//! check falls back to copy-completeness (byte length) — S3 still validated the
//! object against its own checksum at upload, so this confirms an intact, complete
//! copy, just not a manifest content-match. See [`S3ObjectStore::verify_file`].
//!
//! `aws-sdk-s3` is async; the [`ObjectStore`] trait is synchronous (the serve
//! path runs under `spawn_blocking`). The bridge is contained entirely here: the
//! store owns a small `tokio` runtime and `block_on`s each operation. Read-ahead
//! batches issue their range GETs concurrently inside one `block_on` so the
//! round-trips overlap.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::Client;
use tokio::runtime::Runtime;

use super::{join_key, FileIntegrity, ObjectStore, RandomReadAt};

/// Connection parameters for an S3 (or S3-compatible, e.g. MinIO) bucket.
#[derive(Clone, Debug)]
pub struct S3Config {
    /// Bucket name.
    pub bucket: String,
    /// AWS region name (e.g. `eu-west-2`). Empty ⇒ resolved from the environment.
    pub region: String,
    /// Custom endpoint URL for S3-compatible stores (MinIO, localstack). `None`
    /// uses the standard AWS endpoint for `region`.
    pub endpoint: Option<String>,
    /// Key prefix every generation key is joined under. May be empty.
    pub prefix: String,
    /// Path-style addressing (`endpoint/bucket/key`); required by most
    /// S3-compatible servers.
    pub path_style: bool,
    /// AWS access key id. When `Some` (paired with `secret_key`), it is used as
    /// an explicit static credential — the primary, config-driven mechanism.
    /// `None` ⇒ resolve from the standard AWS chain (environment, shared
    /// profile, or instance role).
    pub access_key: Option<String>,
    /// AWS secret access key, paired with `access_key`. `None` ⇒ AWS chain.
    pub secret_key: Option<String>,
    /// Optional AWS session token for temporary (STS) credentials. Only applied
    /// when `access_key`/`secret_key` are both `Some`.
    pub session_token: Option<String>,
}

/// Wraps the backend runtime so its `Drop` is **non-blocking**. A tokio
/// [`Runtime`]'s default `Drop` performs a *blocking* shutdown, which panics
/// ("Cannot drop a runtime in a context where blocking is not allowed") if it
/// runs while the thread is already inside another runtime's async context.
/// That is exactly what happens when the server drops a partially-constructed
/// S3 store on an error path — graph open, disk-cache open, checksum verify all
/// run on the main runtime, so a failure there unwinds and drops this store on
/// an async thread. `shutdown_background()` releases the runtime without
/// blocking, so the *real* open error surfaces instead of a masking panic.
struct BackgroundRuntime(Option<Runtime>);

impl std::ops::Deref for BackgroundRuntime {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        // `Some` for the whole lifetime; only `Drop` takes it.
        self.0
            .as_ref()
            .expect("S3 backend runtime present until drop")
    }
}

impl Drop for BackgroundRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// S3-backed object store. Credentials are taken **first** from the explicit
/// `access_key`/`secret_key` in [`S3Config`] (the config-driven mechanism);
/// when those are unset they fall back to the standard AWS chain (environment,
/// profile, or instance role). Resolved at construction.
pub struct S3ObjectStore {
    client: Client,
    rt: Arc<BackgroundRuntime>,
    bucket: String,
    prefix: String,
}

/// Build a clear error from an AWS SDK error, including the full source chain.
fn sdk_err(context: &str, e: impl std::error::Error) -> anyhow::Error {
    anyhow!("{context}: {}", DisplayErrorContext(&e))
}

/// Drive an async future to completion from a synchronous caller, **without**
/// `block_on`. The future is spawned onto the backend's own runtime (whose
/// worker threads drive it) and the caller blocks on a plain std channel for the
/// result. Unlike `Runtime::block_on`, this is legal from *any* thread —
/// including a thread already inside another tokio runtime (the server opens
/// graphs on its main runtime) and a `spawn_blocking` worker (query execution).
fn run_blocking<F, T>(rt: &Runtime, fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    rt.spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv()
        .expect("S3 backend runtime dropped the task before returning a result")
}

/// Range GET exactly `len` bytes of `key` at `offset` and return them.
async fn get_range(
    client: &Client,
    bucket: &str,
    key: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let end = offset + len - 1;
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={offset}-{end}"))
        .send()
        .await
        .map_err(|e| sdk_err(&format!("S3 GET {key} [{offset}..={end}]"), e))?;
    let data = out
        .body
        .collect()
        .await
        .map_err(|e| sdk_err(&format!("S3 GET {key} body"), e))?
        .to_vec();
    if data.len() as u64 != len {
        bail!("S3 GET {key} returned {} bytes, expected {len}", data.len());
    }
    Ok(data)
}

impl S3ObjectStore {
    /// Connect to the bucket described by `cfg`.
    pub fn connect(cfg: &S3Config) -> Result<Self> {
        // A small dedicated runtime drives the async SDK. Its worker threads run
        // every S3 future; synchronous callers dispatch onto it via
        // [`run_blocking`] (never `block_on`), so the bridge works from the
        // server's main runtime (graph open) and from spawn_blocking workers
        // (query execution) alike.
        let rt = Arc::new(BackgroundRuntime(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("build S3 backend runtime")?,
        )));
        let region = cfg.region.clone();
        let endpoint = cfg.endpoint.clone();
        let path_style = cfg.path_style;
        // Explicit static credentials (config-driven) take precedence; absent
        // them the loader resolves the standard AWS chain below.
        let static_creds = match (cfg.access_key.clone(), cfg.secret_key.clone()) {
            (Some(ak), Some(sk)) => Some(aws_sdk_s3::config::Credentials::new(
                ak,
                sk,
                cfg.session_token.clone(),
                None,
                "slater-config",
            )),
            _ => None,
        };
        let client = run_blocking(&rt, async move {
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if !region.is_empty() {
                loader = loader.region(Region::new(region));
            }
            if let Some(ep) = endpoint {
                loader = loader.endpoint_url(ep);
            }
            if let Some(creds) = static_creds {
                loader = loader.credentials_provider(creds);
            }
            let shared = loader.load().await;
            let mut b = aws_sdk_s3::config::Builder::from(&shared);
            if path_style {
                b = b.force_path_style(true);
            }
            Client::from_conf(b.build())
        });
        Ok(Self {
            client,
            rt,
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
        })
    }

    /// Map a backend-relative key to the full object key (prefix-joined).
    fn full_key(&self, key: &str) -> String {
        join_key(&self.prefix, key)
    }
}

/// One S3 object, read by `Range` GET. Holds a clone of the client + runtime so
/// each positional read drives the async SDK from a synchronous caller.
struct S3Object {
    client: Client,
    rt: Arc<BackgroundRuntime>,
    bucket: String,
    key: String,
    len: u64,
}

impl RandomReadAt for S3Object {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let len = buf.len() as u64;
        let data = run_blocking(&self.rt, async move {
            get_range(&client, &bucket, &key, offset, len).await
        })?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn read_ranges(&self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        // Issue the batch's range GETs concurrently on the backend runtime so
        // their round-trips overlap; return the bytes in request order.
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let ranges = ranges.to_vec();
        let n = ranges.len();
        run_blocking(&self.rt, async move {
            let mut tasks = tokio::task::JoinSet::new();
            for (i, (offset, len)) in ranges.into_iter().enumerate() {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = key.clone();
                tasks.spawn(
                    async move { (i, get_range(&client, &bucket, &key, offset, len).await) },
                );
            }
            let mut out: Vec<Vec<u8>> = vec![Vec::new(); n];
            while let Some(joined) = tasks.join_next().await {
                let (i, res) = joined.map_err(|e| anyhow!("S3 read_ranges task panicked: {e}"))?;
                out[i] = res?;
            }
            Ok(out)
        })
    }
}

impl ObjectStore for S3ObjectStore {
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let fk = full.clone();
        let len = run_blocking(&self.rt, async move {
            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&fk)
                .send()
                .await
                .map_err(|e| sdk_err(&format!("S3 HEAD {fk}"), e))?;
            head.content_length()
                .ok_or_else(|| anyhow!("S3 HEAD {fk} has no content length"))
        })?;
        Ok(Arc::new(S3Object {
            client: self.client.clone(),
            rt: self.rt.clone(),
            bucket: self.bucket.clone(),
            key: full,
            len: len as u64,
        }))
    }

    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            let out = client
                .get_object()
                .bucket(&bucket)
                .key(&full)
                .send()
                .await
                .map_err(|e| sdk_err(&format!("S3 GET {full}"), e))?;
            let data = out
                .body
                .collect()
                .await
                .map_err(|e| sdk_err(&format!("S3 GET {full} body"), e))?
                .to_vec();
            Ok(data)
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // One directory level: a `/` delimiter folds deeper keys into common
        // prefixes; return the immediate child names (objects + subdirs).
        let full = self.full_key(prefix);
        let dir = if full.is_empty() {
            String::new()
        } else {
            format!("{}/", full.trim_end_matches('/'))
        };
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            let strip = |k: &str| -> Option<String> {
                k.strip_prefix(&dir)
                    .map(|r| r.trim_end_matches('/').to_string())
                    .filter(|r| !r.is_empty())
            };
            let mut names = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let mut req = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&dir)
                    .delimiter("/");
                if let Some(t) = &token {
                    req = req.continuation_token(t);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| sdk_err(&format!("S3 ListObjectsV2 prefix {dir:?}"), e))?;
                for obj in resp.contents() {
                    if let Some(name) = obj.key().and_then(strip) {
                        names.push(name);
                    }
                }
                for cp in resp.common_prefixes() {
                    if let Some(name) = cp.prefix().and_then(strip) {
                        names.push(name);
                    }
                }
                match resp.next_continuation_token() {
                    Some(t) if resp.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
                    _ => break,
                }
            }
            names.sort();
            names.dedup();
            Ok(names)
        })
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            match client.head_object().bucket(&bucket).key(&full).send().await {
                Ok(_) => Ok(true),
                Err(e) => {
                    let se = e.into_service_error();
                    if se.is_not_found() {
                        Ok(false)
                    } else {
                        Err(sdk_err(&format!("S3 HEAD {full}"), se))
                    }
                }
            }
        })
    }

    fn verify_file(&self, key: &str, expected: &FileIntegrity) -> Result<()> {
        // Content-grade check from object metadata, no body read: ask S3 for the
        // object's stored SHA-256 (server-computed at upload) and compare it to
        // the manifest's. Falls back to a Content-Length completeness check when
        // the manifest has no SHA-256 (a generation built before checksums) — an
        // S3 PUT is atomic, so a present, right-sized object is a complete one.
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let want = expected.sha256.map(str::to_string);
        let size = expected.size;
        run_blocking(&self.rt, async move {
            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&full)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .map_err(|e| sdk_err(&format!("S3 HEAD {full} for integrity check"), e))?;
            // Right-sized object ⇒ a complete copy. S3 validates every object
            // against its own server-stored full-object checksum at upload, so an
            // object present at the manifest's byte length is intact-at-rest. This
            // is the integrity floor when a content-grade SHA-256 comparison is not
            // available.
            let check_complete = || -> Result<()> {
                let len = head
                    .content_length()
                    .ok_or_else(|| anyhow!("S3 HEAD {full} has no content length"))?;
                if len as u64 != size {
                    bail!(
                        "object {full} failed its copy-completeness check \
                         (manifest {size} bytes, S3 reports {len}) — refusing to serve an incomplete copy"
                    );
                }
                Ok(())
            };
            match (want, head.checksum_sha256()) {
                // Strong path: the object carries a server-stored SHA-256; compare
                // it to the manifest's. SHA-256 is collision-resistant and the
                // stored value is server-authoritative, so this is content-grade.
                (Some(want), Some(got)) => {
                    if got != want {
                        bail!(
                            "object {full} failed its SHA-256 integrity check \
                             (manifest {want}, S3 {got}) — refusing to serve a mismatched object"
                        );
                    }
                }
                // The object has no server-stored SHA-256 — it was uploaded with
                // S3's default full-object checksum (CRC64-NVME) instead of SHA-256,
                // so the manifest's SHA-256 has nothing to compare against. S3 still
                // validated the object against its CRC64-NVME at upload, so fall back
                // to the completeness check rather than refusing to serve. Weaker
                // than the SHA-256 comparison: it proves the copy is complete and
                // intact-at-rest, not that its bytes match the manifest.
                (_, None) => check_complete()?,
                // No manifest SHA-256 (a generation built before checksums): the
                // object's own checksum is irrelevant to a content comparison, so
                // fall back to completeness as before.
                (None, Some(_)) => check_complete()?,
            }
            Ok(())
        })
    }

    fn put(&self, key: &str, bytes: &[u8], sha256_b64: Option<&str>) -> Result<()> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let body = bytes.to_vec();
        let sum = sha256_b64.map(str::to_string);
        run_blocking(&self.rt, async move {
            let mut req = client
                .put_object()
                .bucket(&bucket)
                .key(&full)
                .body(ByteStream::from(body));
            // Hand S3 our SHA-256: it validates the upload against it (rejecting a
            // mismatch) and stores it as the object checksum for later HEAD checks.
            if let Some(sum) = sum {
                req = req.checksum_sha256(sum);
            }
            req.send()
                .await
                .map_err(|e| sdk_err(&format!("S3 PUT {full}"), e))?;
            Ok(())
        })
    }
}
