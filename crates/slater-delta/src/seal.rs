// SPDX-License-Identifier: Apache-2.0
//! At-rest AEAD for the **writable layer's own artifacts** — the WAL and the L0 spill
//! segments (HIK-146).
//!
//! The core is sealed block-by-block by `graph-format`; until this module existed the
//! delta was not sealed at all, so on a graph configured for at-rest encryption every
//! write sat in plaintext on disk from the moment it was acked to the Bolt client until
//! the next flush or consolidation folded it into a sealed segment.
//!
//! # The key
//!
//! One key per graph, [`graph_format::crypto::derive_delta_key`], derived from the runtime
//! master key under its own BLAKE3 `derive_key` context. The delta owns no manifest, so —
//! per the one-salt-per-artifact rule — it invents **no salt**; domain separation comes
//! from the KDF context and identity from the graph name. It is handed around as a
//! [`DeltaCipher`], which is a [`BlockCipher`], so every artifact below reuses HIK-140's
//! per-file subkey ([`BlockCipher::for_file`]) and [`BlockAad`] position binding verbatim.
//!
//! # What each artifact is sealed as
//!
//! | artifact | file-subkey name | AAD |
//! |---|---|---|
//! | WAL segment frame | `wal/<0000000001.wal>` | frame ordinal within the segment |
//! | resident L0 segment | `l0/<0000000001.l0>` | ordinal 0 (one blob) |
//! | off-heap L0 `meta.bin` | `l0/<0000000001.l0>/meta.bin` | ordinal 0 |
//! | off-heap L0 block file | `l0/<0000000001.l0>/node.blk`, … | the block file's own scheme |
//!
//! The subkey name pins the artifact's identity, so a segment cannot be renamed into
//! another slot, and the AAD ordinal pins a WAL frame's position, so frames cannot be
//! reordered, duplicated or dropped from the middle of a segment without failing to open.
//!
//! # Fail closed, both ways
//!
//! Sealed and plaintext artifacts carry **different magics**, and the policy mirrors
//! [`graph_format::crypto::authenticate`]: a sealed artifact with no key configured is
//! refused, and — the direction that matters — a **plaintext artifact under a configured
//! key is also refused**. The second is the strip downgrade: without it, deleting a WAL
//! segment's seal (or dropping in a plaintext one) would replay unauthenticated writes
//! into the memtable of an encrypted graph. Neither is a policy knob.

use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use graph_format::crypto::{BlockAad, BlockCipher, FileCipher, NONCE_LEN};

/// The writable layer's per-graph cipher. Built once at
/// [`DeltaWriter`](https://docs.rs/slater) open from the runtime master key
/// ([`graph_format::crypto::delta_cipher`]) and shared by every WAL segment and L0
/// segment of that graph; `None` is a plaintext deployment.
pub type DeltaCipher = Arc<BlockCipher>;

/// A delta artifact whose at-rest state does not match the configured key. A **type**, not
/// a message: callers branch on `downcast_ref::<DeltaSealRejected>()`, never on the text
/// (CONTRIBUTING.md).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeltaSealRejected {
    /// The artifact is sealed but no master key is configured.
    #[error(
        "{subject} is sealed at rest but no master key is configured — supply the key this \
         graph's delta was written under (or discard the delta)"
    )]
    KeyRequired { subject: &'static str },
    /// The artifact is plaintext under a configured master key — the strip downgrade.
    #[error(
        "{subject} is plaintext but a master key is configured — refusing to replay \
         unauthenticated delta data into an encrypted graph"
    )]
    Unsealed { subject: &'static str },
}

/// The store-relative name a WAL segment's frames are sealed under.
pub(crate) fn wal_name(file_name: &str) -> String {
    format!("wal/{file_name}")
}

/// The store-relative name an L0 segment — a resident file or an off-heap directory — is
/// sealed under. `sub` names a file *inside* an off-heap segment directory.
pub(crate) fn l0_name(path: &Path, sub: Option<&str>) -> Result<String> {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        bail!("L0 segment path {path:?} has no usable file name");
    };
    Ok(match sub {
        None => format!("l0/{name}"),
        Some(sub) => format!("l0/{name}/{sub}"),
    })
}

/// Bind an optional delta cipher to one named artifact.
pub(crate) fn bind(cipher: Option<&DeltaCipher>, name: &str) -> Option<Arc<FileCipher>> {
    cipher.map(|c| Arc::new(c.for_file(name)))
}

/// Seal one WAL frame payload at `ordinal` within its segment: `nonce(24) ‖ ciphertext`.
/// The nonce is stored (XChaCha20's 24 bytes are wide enough that fresh random nonces
/// never realistically collide) and the ordinal is authenticated, not stored.
pub(crate) fn seal_frame(cipher: &FileCipher, ordinal: u64, payload: &[u8]) -> Result<Vec<u8>> {
    let nonce = BlockCipher::random_nonce();
    let ct = cipher.seal(BlockAad::position_only(ordinal), &nonce, payload)?;
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// Open a stored WAL frame claimed to sit at `ordinal`. Fails — typed
/// ([`graph_format::crypto::AeadRejected`]) — for a wrong key, a tampered frame, or a
/// frame lifted from another ordinal or another segment.
pub(crate) fn open_frame(cipher: &FileCipher, ordinal: u64, stored: &[u8]) -> Result<Vec<u8>> {
    if stored.len() < NONCE_LEN {
        bail!("sealed WAL frame is shorter than its nonce");
    }
    let (nonce, ct) = stored.split_at(NONCE_LEN);
    let nonce: [u8; NONCE_LEN] = nonce.try_into().expect("split at NONCE_LEN");
    cipher.open(BlockAad::position_only(ordinal), &nonce, ct)
}

/// Frame one whole immutable blob (a resident L0 segment, an off-heap `meta.bin`):
///
/// ```text
/// MAGIC(8) ‖ crc32c:u32(LE) ‖ stored          (crc over `stored`)
/// stored = body                               (plaintext: MAGIC = `plain`)
///        | nonce(24) ‖ ciphertext(body+16)    (sealed:    MAGIC = `sealed`)
/// ```
///
/// The plaintext arm is byte-for-byte what the delta wrote before HIK-146.
pub(crate) fn frame_blob(
    plain: &[u8; 8],
    sealed: &[u8; 8],
    cipher: Option<&FileCipher>,
    body: &[u8],
) -> Result<Vec<u8>> {
    let (magic, stored): (&[u8; 8], Cow<'_, [u8]>) = match cipher {
        None => (plain, Cow::Borrowed(body)),
        Some(c) => (sealed, Cow::Owned(seal_frame(c, 0, body)?)),
    };
    let crc = crc32c::crc32c(&stored);
    let mut out = Vec::with_capacity(12 + stored.len());
    out.extend_from_slice(magic);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&stored);
    Ok(out)
}

/// The inverse of [`frame_blob`]: check the magic against the configured key, check the
/// crc, then (sealed) authenticate and decrypt. `subject` is how a refusal names the
/// artifact to an operator.
pub(crate) fn unframe_blob<'a>(
    plain: &[u8; 8],
    sealed: &[u8; 8],
    cipher: Option<&FileCipher>,
    bytes: &'a [u8],
    subject: &'static str,
) -> Result<Cow<'a, [u8]>> {
    if bytes.len() < 12 {
        bail!("{subject} is too short ({} bytes)", bytes.len());
    }
    let magic = &bytes[..8];
    let crc = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let stored = &bytes[12..];
    // The magic identifies what the artifact *is*; the key says what it must be. Check the
    // pair before the crc so a downgrade is never reported as corruption.
    let is_sealed = if magic == plain.as_slice() {
        false
    } else if magic == sealed.as_slice() {
        true
    } else {
        bail!("{subject} has bad magic");
    };
    match (is_sealed, cipher) {
        (true, None) => return Err(DeltaSealRejected::KeyRequired { subject }.into()),
        (false, Some(_)) => return Err(DeltaSealRejected::Unsealed { subject }.into()),
        _ => {}
    }
    if crc32c::crc32c(stored) != crc {
        bail!("{subject} failed checksum");
    }
    match cipher {
        None => Ok(Cow::Borrowed(stored)),
        Some(c) => Ok(Cow::Owned(open_frame(c, 0, stored)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::crypto::{delta_cipher, AeadRejected};

    const PLAIN: &[u8; 8] = b"SLTEST01";
    const SEALED: &[u8; 8] = b"SLTESTE1";

    fn cipher(graph: &str) -> DeltaCipher {
        Arc::new(delta_cipher(b"a master key", graph))
    }

    #[test]
    fn plaintext_framing_is_byte_identical_to_the_pre_hik146_shape() {
        let body = b"the delta body".as_slice();
        let framed = frame_blob(PLAIN, SEALED, None, body).unwrap();
        let mut expect = Vec::new();
        expect.extend_from_slice(PLAIN);
        expect.extend_from_slice(&crc32c::crc32c(body).to_le_bytes());
        expect.extend_from_slice(body);
        assert_eq!(framed, expect);
        assert_eq!(
            &*unframe_blob(PLAIN, SEALED, None, &framed, "test blob").unwrap(),
            body
        );
    }

    #[test]
    fn sealed_blob_round_trips_and_hides_the_body() {
        let c = bind(Some(&cipher("g")), "l0/000001.l0").unwrap();
        let body = b"MARKER-PLAINTEXT-BODY".as_slice();
        let framed = frame_blob(PLAIN, SEALED, Some(&c), body).unwrap();
        assert!(
            !framed.windows(body.len()).any(|w| w == body),
            "the body must not appear on disk"
        );
        assert_eq!(
            &*unframe_blob(PLAIN, SEALED, Some(&c), &framed, "test blob").unwrap(),
            body
        );
    }

    #[test]
    fn the_key_policy_is_symmetric_and_typed() {
        let c = bind(Some(&cipher("g")), "l0/000001.l0").unwrap();
        let body = b"body".as_slice();
        let sealed = frame_blob(PLAIN, SEALED, Some(&c), body).unwrap();
        let plain = frame_blob(PLAIN, SEALED, None, body).unwrap();

        let e = unframe_blob(PLAIN, SEALED, None, &sealed, "test blob").unwrap_err();
        assert_eq!(
            e.downcast_ref::<DeltaSealRejected>(),
            Some(&DeltaSealRejected::KeyRequired {
                subject: "test blob"
            })
        );
        let e = unframe_blob(PLAIN, SEALED, Some(&c), &plain, "test blob").unwrap_err();
        assert_eq!(
            e.downcast_ref::<DeltaSealRejected>(),
            Some(&DeltaSealRejected::Unsealed {
                subject: "test blob"
            })
        );
    }

    #[test]
    fn a_blob_is_bound_to_its_name_its_graph_and_its_key() {
        let body = b"body".as_slice();
        let c = bind(Some(&cipher("g")), "l0/000001.l0").unwrap();
        let framed = frame_blob(PLAIN, SEALED, Some(&c), body).unwrap();
        for other in [
            bind(Some(&cipher("g")), "l0/000002.l0").unwrap(), // another segment
            bind(Some(&cipher("other-graph")), "l0/000001.l0").unwrap(), // another graph
            bind(
                Some(&Arc::new(delta_cipher(b"another master key", "g"))),
                "l0/000001.l0",
            )
            .unwrap(), // another key
        ] {
            let e = unframe_blob(PLAIN, SEALED, Some(&other), &framed, "test blob").unwrap_err();
            assert_eq!(
                e.downcast_ref::<AeadRejected>(),
                Some(&AeadRejected::TagMismatch)
            );
        }
    }

    #[test]
    fn a_frame_is_bound_to_its_ordinal() {
        let c = bind(Some(&cipher("g")), "wal/0000000001.wal").unwrap();
        let sealed = seal_frame(&c, 7, b"payload").unwrap();
        assert_eq!(open_frame(&c, 7, &sealed).unwrap(), b"payload");
        let e = open_frame(&c, 8, &sealed).unwrap_err();
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch)
        );
    }

    #[test]
    fn l0_names_are_stable_and_distinct() {
        let p = Path::new("/data/wal/g/l0/0000000003.l0");
        assert_eq!(l0_name(p, None).unwrap(), "l0/0000000003.l0");
        assert_eq!(
            l0_name(p, Some("node.blk")).unwrap(),
            "l0/0000000003.l0/node.blk"
        );
        assert_eq!(wal_name("0000000003.wal"), "wal/0000000003.wal");
        // A WAL segment and an L0 segment that share a number never share a subkey.
        assert_ne!(wal_name("0000000003.l0"), l0_name(p, None).unwrap());
    }
}
