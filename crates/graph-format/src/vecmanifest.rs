// SPDX-License-Identifier: Apache-2.0
//! The **carried vector-graph artifact** manifest — `<graph>/vecidx/<uuid>/VECIDX.json`
//! (HIK-145).
//!
//! # Why a carried `.vamana` is an artifact and not a generation file
//!
//! A consolidation folds a delta into a Vamana index with
//! [`streaming_merge`](crate::vamana_merge::streaming_merge). When there is nothing to fold
//! (the pure-permutation fast path) the graph file is **hard-linked**: the new generation's
//! `.vamana` *is* the old generation's `.vamana`, same inode, bytes never rewritten. That is
//! the whole point — the file is ~370 GB at 91.6M nodes, and re-reading it is exactly the
//! rebuild the carry exists to avoid.
//!
//! Every build mints a fresh per-generation salt. So a hard-linked file inside the new
//! generation's directory is sealed under the **previous** generation's key while the
//! directory's `MANIFEST.json` declares a **new** salt. Every block read fails its Poly1305
//! tag. On an encrypted image, carrying a vector graph by reference never worked.
//!
//! The rule that fixes it is the one the rest of the system already follows: **one salt per
//! artifact, recorded in that artifact's own manifest, beside the data it seals.**
//! [`SegmentManifest`](crate::segmanifest::SegmentManifest) has carried its own
//! [`EncryptionHeader`] since segments existed, and a server running a core plus three
//! segments already derives four independently-salted ciphers from one master key. The
//! carried vector graph was the one artifact that did not, because its salt was implied by
//! the directory it happened to sit in rather than travelling with the file.
//!
//! So the graph file moves out of the generation directory into its own
//! `vecidx/<uuid>/`, with this manifest beside it. The hard link survives, the bytes are
//! never rewritten, and [`EncryptionHeader`] is unchanged — no manifest anywhere gains a
//! second salt.
//!
//! # One file, deliberately
//!
//! A Vamana index is a pair: the `.vamana` graph and the `.pq` layout→id column. Only the
//! `.vamana` is carried. Every merge **rewrites** the `.pq` (the id column is permuted), so
//! it is fresh bytes sealed under the new generation's key and stays in that generation's
//! `files[]` inventory. Hard-linking both into one artifact would put two salts under one
//! manifest, which is precisely the thing that is not allowed.
//!
//! # [`aad_name`](VectorIndexManifest::aad_name), and why it is not the path
//!
//! HIK-140 binds each block's AEAD to a **per-file subkey** derived from the file's
//! store-relative name. The carried file's name changes when it moves
//! (`vector/L.P.vamana` inside a generation → `L.P.vamana` inside an artifact), so a subkey
//! label inferred from the path would break the AAD even with the right salt. The label the
//! blocks were actually sealed under is therefore recorded explicitly and travels with the
//! file. Relocating or renaming the file cannot desynchronise it.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::ids::Generation;
use crate::manifest::EncryptionHeader;
use crate::store::{join_key, ObjectStore};

/// Magic at the head of a carried-vector-graph manifest.
pub const VECIDX_MAGIC: &str = "SLVEC01";

/// Artifact-manifest schema version. A reader refuses a version it does not understand.
pub const VECIDX_VERSION: u32 = 1;

/// A reference to a carried vector-graph artifact: which artifact, and what it must
/// contain. Held by [`VectorIndexDesc`](crate::manifest::VectorIndexDesc) (so the
/// generation knows which artifact backs which index) **and** by
/// [`SetManifest`](crate::setmanifest::SetManifest) (so the *composition* is authenticated,
/// and so GC — which only ever reads the current set — knows what is live).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorArtifactRef {
    /// The artifact's uuid — its directory under `<graph>/vecidx/<uuid>/`.
    pub uuid: Generation,
    /// The artifact's [`content_hash`](VectorIndexManifest::content_hash), so substituting
    /// a *different* validly sealed artifact under the same uuid does not go unnoticed.
    pub content_hash: String,
}

/// The `<graph>/vecidx/<uuid>/VECIDX.json` manifest: one carried vector graph, and the salt
/// it was sealed under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexManifest {
    /// `"SLVEC01"` — quick sniff before trusting the JSON.
    pub magic: String,
    pub version: u32,
    /// This artifact's uuid (its directory under `<graph>/vecidx/<uuid>/`).
    pub artifact_uuid: Generation,
    /// The graph this artifact belongs to, so it cannot be moved between graphs.
    pub graph: String,
    pub created_unix: i64,
    /// The graph file's name **inside this artifact's directory**.
    pub file: String,
    /// The store-relative name the file's blocks were sealed under — the HIK-140 per-file
    /// subkey label. Recorded rather than inferred: the file's on-disk name changed when it
    /// was promoted out of the generation directory, and the subkey must not change with it.
    /// Inert for a plaintext artifact.
    pub aad_name: String,
    /// The file's length in bytes.
    pub bytes: u64,
    /// Target block size the file was written with (informational, mirroring
    /// [`Manifest::block_sizes`](crate::manifest::Manifest::block_sizes)).
    pub block_size: u32,
    /// Records the graph file holds (holes included) — cross-checked against the
    /// generation's descriptor at open, so a stale artifact cannot be paired with a
    /// newer id column.
    pub records: u64,
    /// This artifact's own encryption header — one aead, one kdf, **one** salt, one
    /// `aadScheme`. `None` for a plaintext artifact. The salt is not secret; it is
    /// published in every encrypted manifest by design.
    #[serde(default)]
    pub encryption: Option<EncryptionHeader>,
    /// BLAKE3 (hex) of the graph file's bytes — the artifact's content hash, and what a
    /// [`VectorArtifactRef`] pins.
    pub content_hash: String,
    /// Keyed-BLAKE3 MAC (hex) over the canonicalised manifest, under a subkey derived from
    /// the at-rest master key. `None` ⇒ plaintext artifact. Under a configured key its
    /// absence is a refusal, like every other sealed document (HIK-144) — the policy lives
    /// once in [`crypto::authenticate`](crate::crypto::authenticate).
    #[serde(default)]
    pub mac: Option<String>,
}

/// The fourth MAC-sealed document. Sealing, verification and the require-a-MAC-when-keyed
/// policy live once in [`crypto`](crate::crypto) (HIK-144); only the namespace, the
/// operator-facing label and the canonical body are this type's own.
///
/// The body is the whole struct with `mac` cleared, so the MAC covers the salt, the AAD
/// subkey label, the content hash and the artifact's identity — every input to opening the
/// file it describes.
///
/// **Rule (as for every MAC-covered struct): no `HashMap`/`HashSet` field may be added
/// here.** Iteration order is unspecified and randomised per process, so the same manifest
/// would MAC differently on each run. The body shape is pinned by
/// `mac_preimage_body_is_pinned_to_a_golden_shape` below.
impl crate::crypto::MacSealed for VectorIndexManifest {
    const DOMAIN: crate::crypto::MacDomain = crate::crypto::MacDomain::VectorIndexManifest;
    const SUBJECT: &'static str = "VECIDX.json";

    fn stored_mac(&self) -> Option<&str> {
        self.mac.as_deref()
    }
    fn set_mac(&mut self, mac: Option<String>) {
        self.mac = mac;
    }
    fn mac_body(&self) -> Result<Vec<u8>> {
        let mut canon = self.clone();
        canon.mac = None;
        serde_json::to_vec(&canon).context("serialise vector-index artifact manifest for MAC")
    }
}

impl VectorIndexManifest {
    /// The backend-relative key prefix of artifact `uuid` under `graph` — the directory the
    /// graph file and `VECIDX.json` live in.
    pub fn dir(graph: &str, uuid: Generation) -> String {
        join_key(graph, &format!("vecidx/{}", uuid.0))
    }

    /// The backend-relative key of `VECIDX.json` for `uuid` under `graph`.
    pub fn key(graph: &str, uuid: Generation) -> String {
        join_key(&Self::dir(graph, uuid), "VECIDX.json")
    }

    /// The backend-relative key of the graph file this manifest describes.
    pub fn file_key(&self, graph: &str) -> String {
        join_key(&Self::dir(graph, self.artifact_uuid), &self.file)
    }

    /// Compute the keyed-BLAKE3 MAC and store it in `mac`. Call **last**, after every other
    /// field (notably `content_hash`) is final.
    pub fn seal_mac(&mut self, master_key: &[u8]) -> Result<()> {
        crate::crypto::seal(self, master_key)
    }

    /// Recompute the MAC and compare it to the stored one; typed
    /// [`MacRejected`](crate::crypto::MacRejected) on absence or mismatch. Openers want
    /// [`crypto::authenticate`](crate::crypto::authenticate), which also requires the MAC to
    /// be present when a key is configured.
    pub fn verify_mac(&self, master_key: &[u8]) -> Result<()> {
        crate::crypto::verify(self, master_key)
    }

    /// Validate magic + version. Called after every read before the fields are trusted.
    pub fn validate(&self) -> Result<()> {
        if self.magic != VECIDX_MAGIC {
            bail!(
                "not a vector-index artifact manifest: magic {:?} != {VECIDX_MAGIC:?}",
                self.magic
            );
        }
        if self.version != VECIDX_VERSION {
            bail!(
                "unsupported vector-index artifact version {} (this build understands \
                 {VECIDX_VERSION})",
                self.version
            );
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_string_pretty(self)
            .context("serialise vector-index artifact manifest")?
            .into_bytes())
    }

    /// Read and validate `<graph>/vecidx/<uuid>/VECIDX.json` from any backend, binding the
    /// document to the location it was fetched from and to the graph it was fetched for.
    ///
    /// Without those two checks a *validly sealed* artifact manifest could simply be copied
    /// to another artifact's key (or another graph's): its MAC would still verify — the MAC
    /// covers `artifact_uuid`, not where the bytes are stored — and a generation reference
    /// naming that uuid would then resolve to a graph the operator never published there.
    /// The same trap `SetManifest::read_via` closes (HIK-144).
    pub fn read_via(store: &dyn ObjectStore, graph: &str, uuid: Generation) -> Result<Self> {
        let key = Self::key(graph, uuid);
        let bytes = store
            .read_all(&key)
            .with_context(|| format!("read {key}"))?;
        let m: VectorIndexManifest =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {key}"))?;
        m.validate()?;
        if m.artifact_uuid != uuid {
            bail!(
                "vector-index artifact at {key} declares uuid {} — refusing an artifact \
                 manifest stored under a different uuid",
                m.artifact_uuid
            );
        }
        if m.graph != graph {
            bail!(
                "vector-index artifact {uuid} declares graph {:?} but was read under {graph:?} \
                 — refusing an artifact manifest moved between graphs",
                m.graph
            );
        }
        Ok(m)
    }

    /// Whether artifact `uuid` exists under `graph`.
    pub fn exists_via(store: &dyn ObjectStore, graph: &str, uuid: Generation) -> bool {
        store.exists(&Self::key(graph, uuid)).unwrap_or(false)
    }

    /// The set-level / generation-level reference to this artifact.
    pub fn reference(&self) -> VectorArtifactRef {
        VectorArtifactRef {
            uuid: self.artifact_uuid,
            content_hash: self.content_hash.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::MacSealed as _;
    use crate::store::mem::MemObjectStore;

    fn uuid(n: u128) -> Generation {
        Generation(uuid::Uuid::from_u128(n))
    }

    /// A fully populated artifact manifest — every `Option` `Some` — so the golden preimage
    /// below pins every field. `version` is a **literal**, not [`VECIDX_VERSION`], so the
    /// golden survives a deliberate version bump and only trips on a *shape* change.
    fn golden_artifact() -> VectorIndexManifest {
        VectorIndexManifest {
            magic: VECIDX_MAGIC.to_string(),
            version: 1,
            artifact_uuid: uuid(0x0bad_c0de),
            graph: "docs".into(),
            created_unix: 1_700_000_000,
            file: "Doc.embedding.vamana".into(),
            aad_name: "vector/Doc.embedding.vamana".into(),
            bytes: 4096,
            block_size: 65536,
            records: 400,
            encryption: Some(EncryptionHeader {
                aead: "xchacha20poly1305".into(),
                kdf: "blake3-derive-key".into(),
                salt_hex: "00112233445566778899aabbccddeeff".into(),
                aad_scheme: "file-block-v1".into(),
            }),
            content_hash: "feedface".into(),
            mac: None,
        }
    }

    /// The exact MAC body for [`golden_artifact`]. Changing it invalidates every carried
    /// artifact in existence, so it is pinned rather than recomputed.
    const GOLDEN_BODY: &str = r#"{"magic":"SLVEC01","version":1,"artifactUuid":"00000000-0000-0000-0000-00000badc0de","graph":"docs","createdUnix":1700000000,"file":"Doc.embedding.vamana","aadName":"vector/Doc.embedding.vamana","bytes":4096,"blockSize":65536,"records":400,"encryption":{"aead":"xchacha20poly1305","kdf":"blake3-derive-key","saltHex":"00112233445566778899aabbccddeeff","aadScheme":"file-block-v1"},"contentHash":"feedface","mac":null}"#;

    /// HIK-145, on HIK-142's framing: pin the artifact manifest's MAC preimage.
    #[test]
    fn mac_preimage_body_is_pinned_to_a_golden_shape() {
        let pre = crate::crypto::mac_message(&golden_artifact()).unwrap();

        let tag = format!(
            "slater.vector-index-manifest.mac.v{}\0",
            crate::FORMAT_VERSION
        );
        assert!(
            pre.starts_with(tag.as_bytes()),
            "preimage must open with the versioned domain tag"
        );
        let hdr = tag.len();
        let len = u64::from_le_bytes(pre[hdr..hdr + 8].try_into().unwrap());
        let body = std::str::from_utf8(&pre[hdr + 8..]).unwrap();
        assert_eq!(
            len as usize,
            body.len(),
            "length prefix must state the body"
        );
        assert_eq!(
            body, GOLDEN_BODY,
            "the artifact MAC preimage body changed — a field was added, reordered or \
             reformatted. Confirm the change is intended, then re-pin (it invalidates every \
             existing carried artifact)."
        );
        // The body must actually carry what opening the file depends on, or the MAC
        // authenticates nothing that matters.
        for must in [
            "00112233445566778899aabbccddeeff", // the salt
            "vector/Doc.embedding.vamana",      // the AAD subkey label
            "feedface",                         // the content hash
            "file-block-v1",                    // the AAD scheme
        ] {
            assert!(body.contains(must), "the MAC must cover {must}");
        }
    }

    /// `mac_message` must be byte-identical across repeated calls — cheap smoke for a
    /// map-typed field sneaking into a MAC-covered struct (HIK-142's rule).
    #[test]
    fn mac_message_is_deterministic() {
        let m = golden_artifact();
        let first = crate::crypto::mac_message(&m).unwrap();
        for _ in 0..1000 {
            assert_eq!(crate::crypto::mac_message(&m).unwrap(), first);
        }
    }

    /// The artifact is its own MAC namespace: the same body under any other domain must not
    /// reproduce its MAC.
    #[test]
    fn artifact_mac_does_not_verify_under_another_domain() {
        let master = b"operator master key";
        let mut m = golden_artifact();
        m.seal_mac(master).unwrap();
        m.verify_mac(master).unwrap();

        let body = {
            let mut canon = m.clone();
            canon.mac = None;
            canon.mac_body().unwrap()
        };
        let key = crate::crypto::derive_manifest_mac_key(master);
        for foreign in [
            crate::crypto::MacDomain::Manifest,
            crate::crypto::MacDomain::SegmentManifest,
            crate::crypto::MacDomain::SetManifest,
        ] {
            let forged =
                crate::crypto::manifest_mac(&key, &crate::crypto::mac_preimage(foreign, &body));
            assert_ne!(
                Some(forged),
                m.mac,
                "an artifact MAC must not be reproducible under {foreign:?}"
            );
        }
    }

    /// Everything opening the carried file depends on is MAC-covered, so every substitution
    /// an attacker could attempt must fail verification — not just an incidental field.
    #[test]
    fn the_mac_covers_every_input_to_opening_the_file() {
        let master = b"operator master key";
        let mut sealed = golden_artifact();
        sealed.seal_mac(master).unwrap();
        sealed.verify_mac(master).unwrap();

        let mut wrong_salt = sealed.clone();
        wrong_salt.encryption.as_mut().unwrap().salt_hex =
            "ffffffffffffffffffffffffffffffff".into();

        let mut wrong_aad_name = sealed.clone();
        wrong_aad_name.aad_name = "vector/Other.embedding.vamana".into();

        let mut downgraded_aad_scheme = sealed.clone();
        downgraded_aad_scheme
            .encryption
            .as_mut()
            .unwrap()
            .aad_scheme = "none".into();

        let mut stripped_encryption = sealed.clone();
        stripped_encryption.encryption = None;

        let mut swapped_contents = sealed.clone();
        swapped_contents.content_hash = "0000".into();

        let mut renamed = sealed.clone();
        renamed.artifact_uuid = uuid(0xbeef);

        let mut restated_records = sealed.clone();
        restated_records.records = 399;

        for (what, tampered) in [
            ("salt swapped", wrong_salt),
            ("AAD subkey label swapped", wrong_aad_name),
            ("AAD scheme downgraded", downgraded_aad_scheme),
            ("encryption header stripped", stripped_encryption),
            ("contents swapped", swapped_contents),
            ("artifact renamed", renamed),
            ("record count restated", restated_records),
        ] {
            let err = tampered
                .verify_mac(master)
                .expect_err("a tampered artifact must not verify");
            assert!(
                matches!(
                    err.downcast_ref::<crate::crypto::MacRejected>(),
                    Some(crate::crypto::MacRejected::Mismatch { .. })
                ),
                "{what}: must be refused by type: {err:#}"
            );
        }
    }

    /// HIK-144 parity: under a configured key an unsealed artifact is refused; without a key
    /// there is nothing to authenticate.
    #[test]
    fn an_unsealed_artifact_is_rejected_under_a_key_and_fine_without_one() {
        let mut plain = golden_artifact();
        plain.encryption = None;
        plain.mac = None;
        assert!(crate::crypto::authenticate(&plain, None).is_ok());
        let err = crate::crypto::authenticate(&plain, Some(b"key"))
            .expect_err("a key configured ⇒ the MAC is required");
        assert!(matches!(
            err.downcast_ref::<crate::crypto::MacRejected>(),
            Some(crate::crypto::MacRejected::Missing { .. })
        ));
    }

    /// A validly sealed artifact manifest moved to another artifact's key — or another
    /// graph's — must be refused: the MAC covers the identity, not the location, so only the
    /// read path can bind the two.
    #[test]
    fn a_sealed_artifact_cannot_be_moved() {
        let master = b"operator master key";
        let mut m = golden_artifact();
        m.seal_mac(master).unwrap();

        let store = MemObjectStore::new();
        let elsewhere = uuid(0x7777);
        store
            .put(
                &VectorIndexManifest::key("docs", elsewhere),
                &m.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        let err = VectorIndexManifest::read_via(&store, "docs", elsewhere)
            .expect_err("an artifact stored under a foreign uuid must be refused");
        assert!(format!("{err:#}").contains("different uuid"), "{err:#}");

        store
            .put(
                &VectorIndexManifest::key("other", m.artifact_uuid),
                &m.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        let err = VectorIndexManifest::read_via(&store, "other", m.artifact_uuid)
            .expect_err("an artifact moved between graphs must be refused");
        assert!(format!("{err:#}").contains("between graphs"), "{err:#}");
    }

    #[test]
    fn roundtrips_through_a_store() {
        let store = MemObjectStore::new();
        let m = golden_artifact();
        assert!(!VectorIndexManifest::exists_via(
            &store,
            "docs",
            m.artifact_uuid
        ));
        store
            .put(
                &VectorIndexManifest::key("docs", m.artifact_uuid),
                &m.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        assert!(VectorIndexManifest::exists_via(
            &store,
            "docs",
            m.artifact_uuid
        ));
        let back = VectorIndexManifest::read_via(&store, "docs", m.artifact_uuid).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.reference().uuid, m.artifact_uuid);
        assert_eq!(back.reference().content_hash, m.content_hash);
    }

    #[test]
    fn rejects_foreign_magic_and_version() {
        let store = MemObjectStore::new();
        let mut m = golden_artifact();
        m.magic = "NOPE".into();
        store
            .put(
                &VectorIndexManifest::key("docs", m.artifact_uuid),
                &m.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        let err = VectorIndexManifest::read_via(&store, "docs", m.artifact_uuid).unwrap_err();
        assert!(format!("{err:#}").contains("not a vector-index artifact manifest"));

        let mut m2 = golden_artifact();
        m2.version = VECIDX_VERSION + 1;
        store
            .put(
                &VectorIndexManifest::key("g2", m2.artifact_uuid),
                &m2.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        let err = VectorIndexManifest::read_via(&store, "g2", m2.artifact_uuid).unwrap_err();
        assert!(format!("{err:#}").contains("unsupported vector-index artifact version"));
    }
}
