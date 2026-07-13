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
//! one metadata request per file. When the manifest recorded a SHA-256 but the
//! object carries no server-stored one (e.g. it was uploaded with S3's default
//! full-object checksum, CRC64-NVME, which the manifest's SHA-256 cannot be
//! compared against), the backend does **not** downgrade to a byte-length check —
//! that would satisfy a requested content digest with "the file is the right
//! length", which catches truncation but not tampering. Instead it reads the
//! object body once and re-verifies it against the manifest's canonical BLAKE3,
//! restoring the same content-grade guarantee at the cost of a GET (exactly the
//! trait's default `verify_file`). The length-only completeness check is used
//! solely when the manifest itself recorded no SHA-256 (a pre-checksum
//! generation), where there is nothing to compare. See
//! [`S3ObjectStore::verify_file`].
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

use super::asyncbridge::{run_blocking, BackgroundRuntime};
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

/// Validate an S3 `content_length` header before trusting it as an object size.
/// `content_length` is a signed `i64`; a missing or negative value would otherwise
/// become an absurd `u64` via `as` (a silent wrap), so reject both — matching the
/// guard the GCS backend already applies to its object size.
fn checked_content_length(content_length: Option<i64>, ctx: &str) -> Result<u64> {
    match content_length {
        None => bail!("{ctx} has no content length"),
        Some(len) if len < 0 => bail!("{ctx} reported a negative content length {len}"),
        Some(len) => Ok(len as u64),
    }
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
        let rt = Arc::new(BackgroundRuntime::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .context("build S3 backend runtime")?,
        ));
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

    /// Verify an object by reading its bytes and re-hashing them against the
    /// manifest's canonical BLAKE3 — the fallback [`verify_file`](ObjectStore::verify_file)
    /// takes when the manifest recorded a SHA-256 but the object carries none
    /// server-stored. This is the *same* content-grade guarantee as the server-side
    /// SHA-256 comparison (BLAKE3 is collision-resistant), paid for with one GET;
    /// it is deliberately not a downgrade to a length-only check, which would catch
    /// truncation but not tampering. slater's own `put` always stores a SHA-256, so
    /// a slater-published generation stays on the metadata-only path and never
    /// reaches here — the body read is incurred only by objects that genuinely lack
    /// a server checksum (out-of-band copies, or a tampered overwrite), which is the
    /// correct condition for paying it. `full` is the prefix-joined key, used only
    /// for diagnostics.
    fn verify_by_rehash(&self, key: &str, full: &str, expected: &FileIntegrity) -> Result<()> {
        let src = self.open(key)?;
        let computed = crate::integrity::hash_object(src.as_ref())
            .with_context(|| format!("re-hash {full} (no server-stored SHA-256 to compare)"))?;
        if computed != expected.blake3 {
            bail!(
                "object {full} failed its content re-hash \
                 (manifest BLAKE3 {}, recomputed {}) — refusing to serve a mismatched object; \
                 it carries no server-stored SHA-256, so its bytes were re-read and hashed",
                expected.blake3,
                computed
            );
        }
        Ok(())
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

/// How [`S3ObjectStore::verify_file`] should check an object, decided purely from
/// the manifest's recorded SHA-256 and the object's server-stored SHA-256 (read
/// from `HEAD`). Split out as a total, side-effect-free function so the
/// security-critical routing — in particular that a requested SHA-256 is *never*
/// silently satisfied by a byte-length check — is unit-testable without a network.
#[derive(Debug, PartialEq, Eq)]
enum VerifyPlan {
    /// Manifest and object both carry a SHA-256: compare the two (content-grade,
    /// no body read).
    CompareSha256,
    /// The manifest recorded a SHA-256 but the object has none server-stored. Read
    /// the body and re-verify it against the manifest's canonical BLAKE3. This is
    /// deliberately **not** a downgrade to a length check — the requested content
    /// guarantee is preserved, just paid for with a GET.
    ReHashBody,
    /// The manifest recorded no SHA-256 (a generation built before checksums): there
    /// is nothing to compare against, so a byte-length copy-completeness check is
    /// the intended floor.
    Complete,
}

/// Route an integrity check from `(manifest SHA-256, object SHA-256)`.
///
/// The one rule that matters for the security invariant: a manifest that recorded
/// a SHA-256 must never be satisfied by anything weaker than a content-grade
/// check, so `(Some, None)` maps to [`VerifyPlan::ReHashBody`], never to
/// [`VerifyPlan::Complete`].
fn plan_verify(manifest_sha256: Option<&str>, object_sha256: Option<&str>) -> VerifyPlan {
    match (manifest_sha256, object_sha256) {
        (Some(_), Some(_)) => VerifyPlan::CompareSha256,
        (Some(_), None) => VerifyPlan::ReHashBody,
        (None, _) => VerifyPlan::Complete,
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
            checked_content_length(head.content_length(), &format!("S3 HEAD {fk}"))
        })?;
        Ok(Arc::new(S3Object {
            client: self.client.clone(),
            rt: self.rt.clone(),
            bucket: self.bucket.clone(),
            key: full,
            len,
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
        // object's stored SHA-256 (server-computed at upload) and compare it to the
        // manifest's. When the manifest recorded a SHA-256 but the object carries
        // none (e.g. an out-of-band upload under S3's default CRC64-NVME checksum),
        // fall back to a body re-hash against BLAKE3 — never to a length-only check,
        // which would silently satisfy a requested content digest with mere size.
        // Only a manifest with no SHA-256 at all (a pre-checksum generation) uses
        // the copy-completeness (byte-length) floor.
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let want = expected.sha256.map(str::to_string);
        let size = expected.size;
        // One metadata request (HEAD, no body read): the object's server-stored
        // SHA-256 and its byte length.
        let (object_sha256, content_length) = run_blocking(&self.rt, {
            let full = full.clone();
            async move {
                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&full)
                    .checksum_mode(ChecksumMode::Enabled)
                    .send()
                    .await
                    .map_err(|e| sdk_err(&format!("S3 HEAD {full} for integrity check"), e))?;
                let len = head
                    .content_length()
                    .ok_or_else(|| anyhow!("S3 HEAD {full} has no content length"))?;
                Ok::<(Option<String>, i64), anyhow::Error>((
                    head.checksum_sha256().map(str::to_string),
                    len,
                ))
            }
        })?;

        match plan_verify(want.as_deref(), object_sha256.as_deref()) {
            // Strong path: both sides carry a SHA-256. SHA-256 is collision-resistant
            // and S3's stored value is server-authoritative, so this is content-grade.
            VerifyPlan::CompareSha256 => {
                // Both are `Some` by construction of the plan.
                let (want, got) = (want.unwrap_or_default(), object_sha256.unwrap_or_default());
                if got != want {
                    bail!(
                        "object {full} failed its SHA-256 integrity check \
                         (manifest {want}, S3 {got}) — refusing to serve a mismatched object"
                    );
                }
                Ok(())
            }
            // The manifest asked for a SHA-256 the object cannot prove server-side.
            // Restore the guarantee with a body re-hash rather than trusting length.
            VerifyPlan::ReHashBody => self.verify_by_rehash(key, &full, expected),
            // No manifest SHA-256 (a pre-checksum generation): a present, right-sized
            // object is a complete one (an S3 PUT is atomic), which is the intended
            // floor when there is nothing to compare against.
            VerifyPlan::Complete => {
                if content_length as u64 != size {
                    bail!(
                        "object {full} failed its copy-completeness check \
                         (manifest {size} bytes, S3 reports {content_length}) — refusing to serve an incomplete copy"
                    );
                }
                Ok(())
            }
        }
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

    fn delete(&self, key: &str) -> Result<()> {
        let full = self.full_key(key);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        run_blocking(&self.rt, async move {
            // S3 `DeleteObject` is idempotent — deleting an absent key returns success.
            client
                .delete_object()
                .bucket(&bucket)
                .key(&full)
                .send()
                .await
                .map_err(|e| sdk_err(&format!("S3 DELETE {full}"), e))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_verify, VerifyPlan};

    // Regression for HIK-97: when the manifest recorded a SHA-256 but the object
    // carries none server-stored, `verify_file` must NOT downgrade to a byte-length
    // completeness check (which catches truncation but not tampering). Before the
    // fix the routing collapsed this case into the completeness arm; it must now
    // route to a body re-hash. This pins the security-critical decision without a
    // network or a live S3.
    #[test]
    fn manifest_sha256_without_server_sha256_never_downgrades_to_length() {
        // The bug case: a requested SHA-256, no server-stored one → body re-hash,
        // never `Complete` (the length-only floor).
        assert_eq!(
            plan_verify(Some("bWFuaWZlc3Q="), None),
            VerifyPlan::ReHashBody,
            "a requested SHA-256 with no server checksum must re-hash the body, \
             never fall back to a length-only completeness check"
        );
        assert_ne!(
            plan_verify(Some("bWFuaWZlc3Q="), None),
            VerifyPlan::Complete
        );
    }

    #[test]
    fn both_present_compares_sha256() {
        assert_eq!(
            plan_verify(Some("bWFuaWZlc3Q="), Some("c2VydmVy")),
            VerifyPlan::CompareSha256
        );
    }

    #[test]
    fn no_manifest_sha256_uses_completeness_floor() {
        // A pre-checksum generation: nothing to compare, so the length floor is the
        // intended behaviour regardless of what the object happens to carry.
        assert_eq!(plan_verify(None, None), VerifyPlan::Complete);
        assert_eq!(plan_verify(None, Some("c2VydmVy")), VerifyPlan::Complete);
    }

    #[test]
    fn checked_content_length_rejects_missing_and_negative() {
        assert!(checked_content_length(None, "HEAD x").is_err());
        assert!(checked_content_length(Some(-1), "HEAD x").is_err());
        assert!(checked_content_length(Some(i64::MIN), "HEAD x").is_err());
        assert_eq!(checked_content_length(Some(0), "HEAD x").unwrap(), 0);
        assert_eq!(checked_content_length(Some(4096), "HEAD x").unwrap(), 4096);
    }
}
