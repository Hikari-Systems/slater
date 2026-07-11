// SPDX-License-Identifier: Apache-2.0
//! GCS object-store backend (enabled by the `gcs` cargo feature).
//!
//! Generation files are GCS objects under a key prefix; the readers' positional
//! reads become object range reads — the same explicit, bounded-read model as the
//! local `pread` and the S3 `Range` GET backends (no whole-object download, no
//! mmap).
//!
//! Integrity is verified from GCS's **server-computed CRC32C object checksum** via
//! a metadata request (no body read): the builder sends each object's CRC32C on
//! upload (GCS validates the bytes against it and stores it), and at open the
//! backend reads it back with a metadata `get_object` and compares it to the value
//! recorded in the manifest. CRC32C is GCS's canonical, always-present object
//! checksum, and GCS reports it in the exact base64 form the manifest stores, so
//! the comparison is a direct string match — content-grade, at one metadata
//! request per file. When the manifest has no CRC32C (a generation built before
//! checksums existed), the check falls back to copy-completeness (byte length) — a
//! GCS upload is atomic and CRC32C-validated at write, so a present, right-sized
//! object is an intact, complete copy. See [`GcsObjectStore::verify_file`].
//!
//! Authorization is GCP-native. By default the client resolves Application Default
//! Credentials (GKE Workload Identity, the GCE metadata server, or a `gcloud` /
//! `GOOGLE_APPLICATION_CREDENTIALS` key); a service-account JSON key (file path or
//! inline) in [`GcsConfig`] overrides that, and `anonymous` selects
//! unauthenticated access for a local GCS emulator.
//!
//! The `gcloud-storage` client talks to GCS over its **JSON API** (the same API
//! real GCS and a `fake-gcs-server` emulator both serve), so one code path works
//! against production and the emulator alike. It is async; the [`ObjectStore`]
//! trait is synchronous (the serve path runs under `spawn_blocking`), so the store
//! drives it through the same owned runtime as the S3 backend ([`super::asyncbridge`]).
//! Read-ahead batches issue their range reads concurrently inside one
//! `run_blocking` so the round-trips overlap.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use gcloud_storage::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_storage::client::{Client, ClientConfig};
use gcloud_storage::http::objects::delete::DeleteObjectRequest;
use gcloud_storage::http::objects::download::Range;
use gcloud_storage::http::objects::get::GetObjectRequest;
use gcloud_storage::http::objects::list::ListObjectsRequest;
use gcloud_storage::http::objects::upload::{UploadObjectRequest, UploadType};
use gcloud_storage::http::objects::Object;
use gcloud_storage::http::Error as GcsError;

use super::asyncbridge::{run_blocking, BackgroundRuntime};
use super::{join_key, FileIntegrity, ObjectStore, RandomReadAt};

/// Connection parameters for a GCS bucket.
#[derive(Clone, Debug, Default)]
pub struct GcsConfig {
    /// Bucket name (no `gs://` scheme).
    pub bucket: String,
    /// Key prefix every generation key is joined under. May be empty.
    pub prefix: String,
    /// Custom base endpoint URL (e.g. a `fake-gcs-server` emulator such as
    /// `http://localhost:4443`). `None` uses the standard Google Cloud Storage
    /// endpoint. The client appends `/storage/v1` and `/upload/storage/v1`.
    pub endpoint: Option<String>,
    /// Path to a service-account JSON key file. When set (and `credentials_json`
    /// is not), the key is read and used as the explicit credential. `None` ⇒
    /// resolve Application Default Credentials (Workload Identity / metadata /
    /// gcloud).
    pub credentials_path: Option<String>,
    /// Inline service-account JSON key. Takes precedence over `credentials_path`
    /// when both are set. `None` ⇒ ADC (unless `credentials_path` is set).
    pub credentials_json: Option<String>,
    /// Use **anonymous** (unauthenticated) credentials. For a no-auth endpoint
    /// only — i.e. a local GCS emulator such as `fake-gcs-server`. Overrides every
    /// other credential source. Never enable this against real GCS.
    pub anonymous: bool,
}

/// GCS-backed object store over the JSON API. Credentials are resolved at
/// construction: anonymous when [`GcsConfig::anonymous`], else an explicit
/// service-account JSON when present, else Application Default Credentials.
pub struct GcsObjectStore {
    client: Arc<Client>,
    rt: Arc<BackgroundRuntime>,
    bucket: String,
    prefix: String,
}

/// Build a clear error from a `gcloud-storage` error.
fn gcs_err(context: &str, e: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("{context}: {e}")
}

/// Whether a `gcloud-storage` error is a "not found" (HTTP 404) — a missing
/// object, distinguished from a transport/auth failure.
fn is_not_found(e: &GcsError) -> bool {
    matches!(e, GcsError::Response(r) if r.code == 404)
}

impl GcsObjectStore {
    /// Connect to the bucket described by `cfg`.
    pub fn connect(cfg: &GcsConfig) -> Result<Self> {
        // A small dedicated runtime drives the async client; synchronous callers
        // dispatch onto it via `run_blocking` (never `block_on`), so the bridge
        // works from the server's main runtime (graph open) and from
        // spawn_blocking workers (query execution) alike.
        let rt = Arc::new(BackgroundRuntime::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("build GCS backend runtime")?,
        ));
        let endpoint = cfg.endpoint.clone();
        let anonymous = cfg.anonymous;
        // Resolve the explicit service-account JSON, if configured. Inline JSON
        // wins over a file path; neither ⇒ ADC (or anonymous).
        let sa_json = match (&cfg.credentials_json, &cfg.credentials_path) {
            _ if anonymous => None,
            (Some(inline), _) => Some(inline.clone()),
            (None, Some(path)) => Some(
                std::fs::read_to_string(path)
                    .with_context(|| format!("read GCS service-account key file {path}"))?,
            ),
            (None, None) => None,
        };
        let client = run_blocking(&rt, async move {
            let mut config = if anonymous {
                ClientConfig::default().anonymous()
            } else if let Some(json) = sa_json {
                let cred = CredentialsFile::new_from_str(&json)
                    .await
                    .map_err(|e| gcs_err("parse GCS service-account JSON key", e))?;
                ClientConfig::default()
                    .with_credentials(cred)
                    .await
                    .map_err(|e| gcs_err("apply GCS service-account credentials", e))?
            } else {
                ClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| gcs_err("resolve GCS Application Default Credentials", e))?
            };
            // The endpoint override (emulator / private endpoint) is independent of
            // the credential source. The client appends `/storage/v1` itself.
            if let Some(ep) = endpoint {
                config.storage_endpoint = ep;
            }
            Ok::<_, anyhow::Error>(Client::new(config))
        })?;
        Ok(Self {
            client: Arc::new(client),
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

/// Read exactly `len` bytes of `key` at `offset` and return them, by a JSON-API
/// object range download.
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
    let req = GetObjectRequest {
        bucket: bucket.to_string(),
        object: key.to_string(),
        ..Default::default()
    };
    // Range end is inclusive: bytes=offset-(offset+len-1).
    let range = Range(Some(offset), Some(offset + len - 1));
    let data = client
        .download_object(&req, &range)
        .await
        .map_err(|e| gcs_err(&format!("GCS download {key} [{offset}..+{len}]"), e))?;
    if data.len() as u64 != len {
        bail!(
            "GCS download {key} returned {} bytes, expected {len}",
            data.len()
        );
    }
    Ok(data)
}

/// One GCS object, read by object range download. Holds clones of the client +
/// runtime so each positional read drives the async client from a sync caller.
struct GcsObject {
    client: Arc<Client>,
    rt: Arc<BackgroundRuntime>,
    bucket: String,
    key: String,
    len: u64,
}

impl RandomReadAt for GcsObject {
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
        // Issue the batch's range reads concurrently on the backend runtime so
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
                let (i, res) = joined.map_err(|e| anyhow!("GCS read_ranges task panicked: {e}"))?;
                out[i] = res?;
            }
            Ok(out)
        })
    }
}

impl GcsObjectStore {
    /// Fetch an object's metadata (`get_object`, no body read).
    async fn head(client: &Client, bucket: &str, key: &str) -> Result<Object, GcsError> {
        let req = GetObjectRequest {
            bucket: bucket.to_string(),
            object: key.to_string(),
            ..Default::default()
        };
        client.get_object(&req).await
    }
}

impl ObjectStore for GcsObjectStore {
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let fk = full.clone();
        let size = run_blocking(&self.rt, async move {
            let obj = Self::head(&client, &bucket, &fk)
                .await
                .map_err(|e| gcs_err(&format!("GCS get_object {fk}"), e))?;
            Ok::<_, anyhow::Error>(obj.size)
        })?;
        if size < 0 {
            bail!("GCS get_object {full} reported a negative size {size}");
        }
        Ok(Arc::new(GcsObject {
            client: self.client.clone(),
            rt: self.rt.clone(),
            bucket: self.bucket.clone(),
            key: full,
            len: size as u64,
        }))
    }

    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            let req = GetObjectRequest {
                bucket: bucket.clone(),
                object: full.clone(),
                ..Default::default()
            };
            // Whole object: an unbounded range (no `Range` header).
            client
                .download_object(&req, &Range(None, None))
                .await
                .map_err(|e| gcs_err(&format!("GCS download {full}"), e))
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
            let mut page_token: Option<String> = None;
            loop {
                let req = ListObjectsRequest {
                    bucket: bucket.clone(),
                    prefix: Some(dir.clone()),
                    delimiter: Some("/".to_string()),
                    page_token: page_token.clone(),
                    ..Default::default()
                };
                let resp = client
                    .list_objects(&req)
                    .await
                    .map_err(|e| gcs_err(&format!("GCS list_objects prefix {dir:?}"), e))?;
                for obj in resp.items.into_iter().flatten() {
                    if let Some(name) = strip(&obj.name) {
                        names.push(name);
                    }
                }
                for cp in resp.prefixes.into_iter().flatten() {
                    if let Some(name) = strip(&cp) {
                        names.push(name);
                    }
                }
                match resp.next_page_token {
                    Some(t) if !t.is_empty() => page_token = Some(t),
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
            match Self::head(&client, &bucket, &full).await {
                Ok(_) => Ok(true),
                Err(e) if is_not_found(&e) => Ok(false),
                Err(e) => Err(gcs_err(&format!("GCS get_object {full}"), e)),
            }
        })
    }

    fn verify_file(&self, key: &str, expected: &FileIntegrity) -> Result<()> {
        // Content-grade check from object metadata, no body read: ask GCS for the
        // object's server-computed CRC32C and compare it to the manifest's. Both
        // are the same base64 (big-endian u32) form, so the comparison is a direct
        // string match. Falls back to a byte-length completeness check when the
        // manifest has no CRC32C (a generation built before checksums) — a GCS
        // upload is atomic and CRC32C-validated at write, so a present, right-sized
        // object is complete.
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let want = expected.crc32c.map(str::to_string);
        let size = expected.size;
        run_blocking(&self.rt, async move {
            let obj = Self::head(&client, &bucket, &full)
                .await
                .map_err(|e| gcs_err(&format!("GCS get_object {full} for integrity check"), e))?;
            // Right-sized object ⇒ a complete copy (the integrity floor when a
            // content-grade CRC32C comparison is not available).
            let check_complete = || -> Result<()> {
                if obj.size < 0 || obj.size as u64 != size {
                    bail!(
                        "object {full} failed its copy-completeness check \
                         (manifest {size} bytes, GCS reports {}) — refusing to serve an incomplete copy",
                        obj.size
                    );
                }
                Ok(())
            };
            match (want, obj.crc32c.as_deref()) {
                // Strong path: compare the manifest's CRC32C to GCS's server-stored
                // one. CRC32C is GCS's authoritative object checksum, so this is a
                // content-grade match.
                (Some(want), Some(got)) => {
                    if got != want {
                        bail!(
                            "object {full} failed its CRC32C integrity check \
                             (manifest {want}, GCS {got}) — refusing to serve a mismatched object"
                        );
                    }
                }
                // The object carries no CRC32C in its metadata (unexpected for a
                // simple upload, but possible for composite objects) — fall back to
                // the completeness check rather than refusing to serve.
                (_, None) => check_complete()?,
                // No manifest CRC32C (a generation built before checksums): nothing
                // to compare against, so fall back to completeness as before.
                (None, Some(_)) => check_complete()?,
            }
            Ok(())
        })
    }

    fn put(&self, key: &str, bytes: &[u8], _sha256_b64: Option<&str>) -> Result<()> {
        // GCS validates the upload against CRC32C (not SHA-256), so we compute the
        // CRC32C of the bytes and send it as the uploaded object's metadata: GCS
        // rejects a mismatch and stores the checksum for later metadata integrity
        // checks. The base64 (big-endian u32) form is exactly what GCS expects.
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let crc_b64 = crate::integrity::crc32c_base64(bytes);
        let body = bytes.to_vec();
        run_blocking(&self.rt, async move {
            let req = UploadObjectRequest {
                bucket: bucket.clone(),
                ..Default::default()
            };
            // Multipart upload carries object metadata (name + crc32c) alongside the
            // bytes so GCS validates the write against the checksum.
            let metadata = Object {
                name: full.clone(),
                crc32c: Some(crc_b64),
                ..Default::default()
            };
            let upload_type = UploadType::Multipart(Box::new(metadata));
            client
                .upload_object(&req, body, &upload_type)
                .await
                .map_err(|e| gcs_err(&format!("GCS upload {full}"), e))?;
            Ok(())
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            let req = DeleteObjectRequest {
                bucket: bucket.clone(),
                object: full.clone(),
                ..Default::default()
            };
            match client.delete_object(&req).await {
                Ok(()) => Ok(()),
                // Tolerate an already-absent object (idempotent), like the other backends.
                Err(e) if is_not_found(&e) => Ok(()),
                Err(e) => Err(gcs_err(&format!("GCS delete {full}"), e)),
            }
        })
    }
}
