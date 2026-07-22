// SPDX-License-Identifier: Apache-2.0
//! Optional at-rest AEAD over individual blocks.
//!
//! Encryption is per **block**, not per file: a block is compressed first, then
//! the compressed bytes are sealed with **XChaCha20-Poly1305**. The reader
//! decrypts a block on a cache miss and then decompresses it, so the block LRU
//! continues to hold *plaintext-decompressed* blocks and the executor / KNN paths
//! are entirely unaware that the bytes on disk were encrypted (D28).
//!
// DESIGN: pure-Rust `chacha20poly1305` (not the C/aws-lc stack) keeps the whole
// crypto path musl-clean and self-contained. XChaCha20's 24-byte nonce is wide
// enough that fresh random nonces never realistically collide, so each block
// gets an independent random nonce stored beside it in the block directory — the
// MANIFEST carries only the KDF salt and parameters, never the key.
//
// DESIGN: the key-management seam. The MANIFEST holds a per-generation random
// salt; the *runtime master key* is supplied out of band (an env var or a mounted
// secret file — `config.encryption{key_env|key_file}`) and is NEVER written into
// the data directory. The per-generation block key is `BLAKE3::derive_key` over
// (master key ‖ salt), so two generations built from the same master key still
// use independent block keys, and losing the salt does not weaken a generation
// whose master key is held elsewhere.

// DESIGN: key material is wrapped in `Zeroizing<_>`, which wipes the buffer with
// volatile writes when it drops (HIK-139). This covers the derived per-generation
// block key, the manifest-MAC subkey, the master key bytes and the hex text they
// were decoded from. `XChaCha20Poly1305` needs no wrapper: it zeroizes its own key
// in `Drop` unconditionally (see the LIMIT note below for what it *cannot* cover).
//
// LIMIT: this is best-effort defence-in-depth against core dumps / swap / cold-boot
// disclosure — it is not a guarantee, and two gaps are structural:
//
//   * A master key supplied through `encryption.keyEnv` (or slater-build's
//     `--key-env`) **cannot be wiped**. The value lives in the process environment
//     block, which the C runtime owns and which is readable from `/proc/self/environ`
//     for the process's whole life; `std::env::var` only hands us a copy, and
//     `remove_var` is `unsafe` and process-global-racy. Wiping the copy we were
//     handed still leaves the original. Use `keyFile` if that matters.
//   * Growth by reallocation leaves untracked copies in freed heap. `hex_decode`
//     already pre-sizes its `Vec` with `with_capacity(s.len() / 2)` so *it* never
//     reallocates, but the `String` read from the key file is built by
//     `fs::read_to_string`, which grows as it reads. `Zeroizing` wipes only the
//     final buffer, never the earlier ones the allocator has already reclaimed.
//   * Wiping reaches only the values we hold. The KDF hashers below are wiped
//     explicitly (blake3's `zeroize` feature) because they retain the master key
//     and the MAC key, but nothing can reach a copy the optimizer spilled to an
//     unnamed stack slot or left in a register.

use std::sync::Arc;

use anyhow::{bail, Result};
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use zeroize::{Zeroize, Zeroizing};

/// AEAD identifier written into the MANIFEST `EncryptionHeader`.
pub const AEAD_NAME: &str = "xchacha20poly1305";
/// KDF identifier written into the MANIFEST `EncryptionHeader`.
pub const KDF_NAME: &str = "blake3-derive-key";
/// XChaCha20-Poly1305 nonce width (bytes).
pub const NONCE_LEN: usize = 24;
/// Derived block-key width (bytes).
pub const KEY_LEN: usize = 32;
/// Per-generation KDF salt width (bytes).
pub const SALT_LEN: usize = 32;

/// BLAKE3 derive-key context. Versioned so a future KDF change is unambiguous.
const KDF_CONTEXT: &str = "slater generation block key v1";

/// BLAKE3 derive-key context for the manifest-authentication (MAC) subkey.
/// Distinct from [`KDF_CONTEXT`] so the MAC key and per-block keys are
/// domain-separated — neither can ever collide with the other.
const MAC_KDF_CONTEXT: &str = "slater manifest mac v1";

/// BLAKE3 derive-key context for the **per-file** block subkey (HIK-140). A third
/// domain alongside [`KDF_CONTEXT`] / [`MAC_KDF_CONTEXT`].
const FILE_KDF_CONTEXT: &str = "slater generation file key v1";

/// The associated-data scheme this build seals blocks under, recorded in the
/// MANIFEST `EncryptionHeader.aadScheme`. A generation that does not name exactly
/// this scheme is refused (see [`AadSchemeRejected`]) — there is no legacy mode, so
/// an image sealed without AAD binding fails closed rather than opening unbound.
///
/// `file-block-v1` is: a per-file subkey ([`BlockCipher::for_file`]) plus [`BlockAad`]
/// — the block's ordinal within its file, and (for a block file, whose directory is
/// plaintext) the `first_record`/`raw_len`/`rec_count` the reader is about to trust for it.
pub const AAD_SCHEME: &str = "file-block-v1";

/// Generate a fresh per-generation random salt.
pub fn random_salt() -> [u8; SALT_LEN] {
    // `from_fn` rather than a `[0u8; N]` literal: both produce a zeroed buffer the
    // OS RNG immediately overwrites, but the literal form is a constant array that
    // static analysis (CodeQL rust/hard-coded-cryptographic-value) mistakes for a
    // hard-coded salt. There is no such thing as a "hard-coded" value here.
    let mut salt: [u8; SALT_LEN] = core::array::from_fn(|_| 0);
    OsRng.fill_bytes(&mut salt);
    salt
}

/// Derive the 32-byte per-generation block key from the runtime master key and
/// the generation's salt via `BLAKE3::derive_key`.
///
/// The subkey is returned in a [`Zeroizing`] so it is wiped when the caller drops
/// it; deref gives `&[u8; KEY_LEN]` so call sites are otherwise unchanged.
pub fn derive_key(master_key: &[u8], salt: &[u8]) -> Zeroizing<[u8; KEY_LEN]> {
    let mut h = blake3::Hasher::new_derive_key(KDF_CONTEXT);
    h.update(master_key);
    h.update(salt);
    let mut out = Zeroizing::new([0u8; KEY_LEN]);
    let mut xof = h.finalize_xof();
    xof.fill(&mut *out);
    // The hasher's block buffer still holds the master key verbatim (it is shorter
    // than one 64-byte block), and the XOF reader can regenerate the subkey at will.
    // Wiping only `out` would leave both a frame away, so wipe them too.
    xof.zeroize();
    h.zeroize();
    out
}

/// Derive the 32-byte manifest-MAC key from the runtime master key. Unlike the
/// per-generation block key this is **not** salted: it binds the manifest to the
/// operator's master key only, and the per-generation salt already lives inside
/// the (MAC-covered) encryption header. Salt-free keeps the key reproducible at
/// verify time from the master key alone. Domain-separated from [`derive_key`] by
/// its KDF context.
///
/// Wiped on drop via [`Zeroizing`], exactly like [`derive_key`].
pub fn derive_manifest_mac_key(master_key: &[u8]) -> Zeroizing<[u8; KEY_LEN]> {
    let mut h = blake3::Hasher::new_derive_key(MAC_KDF_CONTEXT);
    h.update(master_key);
    let mut out = Zeroizing::new([0u8; KEY_LEN]);
    let mut xof = h.finalize_xof();
    xof.fill(&mut *out);
    xof.zeroize();
    h.zeroize();
    out
}

/// Domain-separation label for the **generation** manifest's MAC preimage
/// ([`mac_preimage`]).
pub const MAC_DOMAIN_MANIFEST: &str = "slater.manifest";

/// Domain-separation label for the **segment** manifest's MAC preimage. Distinct from
/// [`MAC_DOMAIN_MANIFEST`], so a document of one kind can never verify as the other even
/// if their serialised bodies were somehow made to coincide.
pub const MAC_DOMAIN_SEGMENT_MANIFEST: &str = "slater.segment-manifest";

/// Frame `body` into the MAC preimage for `domain`:
///
/// ```text
/// "<domain>.mac.v<FORMAT_VERSION>" || 0x00 || (body.len() as u64, LE) || body
/// ```
///
/// Three properties, in the order they matter:
///
/// * **Domain separation.** The tag is a *fixed* prefix chosen by the caller, not by the
///   document, so nothing in `body` can shift the boundary or forge a different domain's
///   framing. `slater.manifest` and `slater.segment-manifest` are separate namespaces.
/// * **Version binding.** The version comes from [`crate::FORMAT_VERSION`], never a
///   hand-maintained `"v1"` string, so the MAC scheme cannot silently drift from the
///   on-disk format version. A format bump already forces a rebuild, so re-MACing with it
///   is free.
/// * **Unambiguous concatenation.** The `u64` length prefix means the body's extent is
///   stated, not inferred from "everything after the tag" — so no future field appended
///   *after* the body could be traded against body bytes to produce a second document
///   with the same preimage.
///
/// Neither domain constant may contain a NUL; the `0x00` is the tag/length delimiter.
pub fn mac_preimage(domain: &str, body: &[u8]) -> Vec<u8> {
    debug_assert!(!domain.contains('\0'), "a MAC domain must not contain NUL");
    let tag = format!("{domain}.mac.v{}", crate::FORMAT_VERSION);
    let mut out = Vec::with_capacity(tag.len() + 1 + 8 + body.len());
    out.extend_from_slice(tag.as_bytes());
    out.push(0);
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Compute a keyed-BLAKE3 MAC over `msg` and hex-encode it. The key is the
/// 32-byte subkey from [`derive_manifest_mac_key`].
///
/// Spelled out as a `Hasher` rather than the one-shot `blake3::keyed_hash` so the
/// hasher — which holds the MAC key verbatim in its key words — can be wiped after
/// use; the one-shot's hasher is a local we cannot reach.
pub fn manifest_mac(key: &[u8; KEY_LEN], msg: &[u8]) -> String {
    let mut h = blake3::Hasher::new_keyed(key);
    h.update(msg);
    let mac = hex_encode(h.finalize().as_bytes());
    h.zeroize();
    mac
}

/// Lower-case hex-encode bytes (for the MANIFEST salt field).
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Decode a lower/upper-case hex string into bytes. A clear error (not a panic)
/// on an odd length or a non-hex digit — keys and salts arrive from operators.
pub fn hex_decode(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        bail!("hex string has an odd number of digits");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow::anyhow!("invalid hex digit {:?}", pair[0] as char))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow::anyhow!("invalid hex digit {:?}", pair[1] as char))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// Derive a **per-file** block subkey from the per-generation key and the file's
/// store-relative name (HIK-140): `BLAKE3::derive_key(FILE_KDF_CONTEXT, gen_key ‖ name)`.
///
/// The name is fed as its raw bytes with no separator, which is unambiguous here
/// because the generation key is fixed-width (32 bytes) and leads.
///
/// Wiped on drop via [`Zeroizing`], and the hasher/XOF — which retain the generation
/// key verbatim — are wiped explicitly, exactly like [`derive_key`].
fn derive_file_key(gen_key: &[u8; KEY_LEN], name: &str) -> Zeroizing<[u8; KEY_LEN]> {
    let mut h = blake3::Hasher::new_derive_key(FILE_KDF_CONTEXT);
    h.update(gen_key);
    h.update(name.as_bytes());
    let mut out = Zeroizing::new([0u8; KEY_LEN]);
    let mut xof = h.finalize_xof();
    xof.fill(&mut *out);
    xof.zeroize();
    h.zeroize();
    out
}

/// A per-generation block key. It **cannot seal or open anything**: the only thing it
/// does is derive a [`FileCipher`] for one named file (HIK-140). Splitting the types
/// this way is what makes "the writer and the reader bound this block to different
/// files" a compile error at every call site rather than a silent runtime mismatch.
///
/// Cheap to clone-derive from (one BLAKE3 compression), so one `FileCipher` per file is
/// the intended usage.
pub struct BlockCipher {
    /// The per-generation subkey. Held (rather than folded straight into an AEAD) so
    /// [`for_file`](BlockCipher::for_file) can derive from it; wiped on drop.
    key: Zeroizing<[u8; KEY_LEN]>,
}

impl BlockCipher {
    /// Build a cipher from an already-derived 32-byte block key.
    pub fn from_key(key: &[u8; KEY_LEN]) -> Self {
        Self {
            key: Zeroizing::new(*key),
        }
    }

    /// Derive the per-generation key from a runtime master key + salt and build
    /// the cipher.
    pub fn from_master(master_key: &[u8], salt: &[u8]) -> Self {
        Self::from_key(&derive_key(master_key, salt))
    }

    /// The cipher for one file of this generation, named by its **store-relative**
    /// name inside the generation/segment directory (`node_props.blk`,
    /// `range/title.isam`, `vector/Doc.embedding.pq`, …). Writer and reader must pass
    /// byte-identical names; a mismatch fails closed at the first block read.
    pub fn for_file(&self, name: &str) -> FileCipher {
        FileCipher {
            aead: XChaCha20Poly1305::new(Key::from_slice(&*derive_file_key(&self.key, name))),
        }
    }

    /// A fresh random nonce for the next block to be sealed.
    pub fn random_nonce() -> [u8; NONCE_LEN] {
        // `generate_nonce` draws straight from the OS RNG and returns the bytes, so
        // there is no zeroed `[0u8; N]` buffer — and thus no constant array literal
        // for static analysis to mistake for a hard-coded nonce.
        XChaCha20Poly1305::generate_nonce(&mut OsRng).into()
    }
}

/// The cipher for one file of one generation — the only thing that can seal or open a
/// block. Every seal additionally binds the block's **ordinal** within that file as
/// associated data, so a sealed block cannot be moved to another offset, another
/// ordinal, or another file and still verify (HIK-140).
///
/// Holds no per-block state, so a single instance is shared (behind an `Arc`) across
/// every reader and writer of that one file.
pub struct FileCipher {
    aead: XChaCha20Poly1305,
}

/// Reserved block ordinal for a container's single out-of-band header block — the ISAM
/// top level. Distinct from every leaf ordinal (a file would need 2^64 blocks to reach
/// it), so a leaf can never be presented as the top level.
pub const HEADER_ORDINAL: u64 = u64::MAX;

/// What a block is sealed *as*: where it sits, and what the container's index claims about
/// it. The file identity is already in the key, so this carries only the per-block part.
///
/// `first_record`/`rec_count`/`raw_len` are here because a block file's directory is
/// **plaintext and outside the AEAD**, and the reader derives its global-record-index →
/// `(block, slot)` mapping from it: `block_start` is the prefix sum of `rec_count`, and
/// `locate` binary-searches that. Binding the ordinal alone does **not** close it — a
/// forged `rec_count` on block *i* does not make the reader open block *i*, it silently
/// redirects the lookup to block *i+1*, which then opens perfectly well at its own row.
/// Binding each block's own *global first record* is what refuses that: the redirected
/// block is asked for at a start it was not sealed at.
///
/// An ISAM index needs none of it: its per-block `raw_len` and location live *inside* the
/// sealed top level, so they are already authenticated, and its blocks carry no global
/// record index. Those use [`BlockAad::position_only`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockAad {
    pub ordinal: u64,
    /// Global index of this block's first record — `block_start[ordinal]`.
    pub first_record: u64,
    pub raw_len: u32,
    pub rec_count: u32,
}

impl BlockAad {
    /// A block-file block: ordinal plus the directory row the reader will trust.
    pub fn block(ordinal: usize, first_record: u64, raw_len: u32, rec_count: u32) -> Self {
        Self {
            ordinal: ordinal as u64,
            first_record,
            raw_len,
            rec_count,
        }
    }

    /// A block whose container authenticates its own index (the ISAM leaf/top levels):
    /// position alone.
    pub fn position_only(ordinal: u64) -> Self {
        Self {
            ordinal,
            first_record: 0,
            raw_len: 0,
            rec_count: 0,
        }
    }

    fn bytes(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&self.ordinal.to_le_bytes());
        out[8..16].copy_from_slice(&self.first_record.to_le_bytes());
        out[16..20].copy_from_slice(&self.raw_len.to_le_bytes());
        out[20..24].copy_from_slice(&self.rec_count.to_le_bytes());
        out
    }
}

impl FileCipher {
    /// Seal a (compressed) block that lives at `aad.ordinal` in this file. The returned
    /// ciphertext is `plaintext.len()` + the 16-byte Poly1305 tag — the AAD is
    /// authenticated, not stored, so nothing on disk grows.
    pub fn seal(
        &self,
        aad: BlockAad,
        nonce: &[u8; NONCE_LEN],
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        let aad = aad.bytes();
        self.aead
            .encrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("AEAD encryption failed"))
    }

    /// Open a sealed block claimed to live at `aad.ordinal` in this file, under the index
    /// row `aad` the container is about to trust. Fails with a
    /// typed error — never a panic or garbage — when the key is wrong, the block was
    /// tampered with, or the block was sealed for a different file or ordinal.
    pub fn open(
        &self,
        aad: BlockAad,
        nonce: &[u8; NONCE_LEN],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let aad = aad.bytes();
        self.aead
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| AeadRejected::TagMismatch.into())
    }
}

/// An AEAD open that did not verify. A **type**, not a message: the read paths that
/// distinguish "this block is not the block that was sealed here" from "this block
/// decoded but its contents are malformed" branch on `downcast_ref::<AeadRejected>()`,
/// never on the text (CONTRIBUTING.md).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AeadRejected {
    /// The Poly1305 tag did not verify: a wrong key, a corrupt/tampered block, or a
    /// block that was sealed under a different file or at a different block ordinal.
    #[error(
        "AEAD decryption failed: wrong key, corrupt/tampered block, or a block sealed \
         for a different file or block ordinal"
    )]
    TagMismatch,
}

/// An encrypted image whose `aadScheme` this build does not implement. A **type**, so
/// the server can tell "rebuild this image" apart from every other open failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "encrypted image declares aadScheme {got:?}, but this build seals blocks under \
     {AAD_SCHEME:?}; the image predates per-file/per-ordinal AEAD binding and must be \
     rebuilt (it cannot be opened safely)"
)]
pub struct AadSchemeRejected {
    pub got: String,
}

/// Bind an optional generation cipher to one named file (HIK-140). The idiom at every
/// reader/writer construction site: `file_cipher(&cipher, "node_props.blk")`.
///
/// `name` must be the file's **store-relative** name inside the generation or segment
/// directory (`node_props.blk`, `range/title.isam`, `vector/Doc.embedding.pq`, …), and
/// must be byte-identical on the writing and the reading side.
pub fn file_cipher(cipher: &Option<Arc<BlockCipher>>, name: &str) -> Option<Arc<FileCipher>> {
    cipher.as_ref().map(|c| Arc::new(c.for_file(name)))
}

/// Refuse an encryption header whose AAD scheme is not the one this build seals under.
pub fn check_aad_scheme(got: &str) -> Result<()> {
    if got == AAD_SCHEME {
        Ok(())
    } else {
        Err(AadSchemeRejected {
            got: got.to_string(),
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrips_and_rejects_garbage() {
        let bytes = [0x00u8, 0x0f, 0xa5, 0xff, 0x10];
        let hex = hex_encode(&bytes);
        assert_eq!(hex, "000fa5ff10");
        assert_eq!(hex_decode(&hex).unwrap(), bytes);
        assert_eq!(hex_decode("  000FA5FF10 ").unwrap(), bytes); // trims + upper-case
        assert!(hex_decode("abc").is_err()); // odd length
        assert!(hex_decode("zz").is_err()); // non-hex
    }

    #[test]
    fn derive_key_is_deterministic_and_salt_sensitive() {
        let master = b"super-secret-master-key";
        let salt_a = [1u8; SALT_LEN];
        let salt_b = [2u8; SALT_LEN];
        let k1 = derive_key(master, &salt_a);
        let k2 = derive_key(master, &salt_a);
        let k3 = derive_key(master, &salt_b);
        assert_eq!(k1, k2, "same master + salt derives the same key");
        assert_ne!(k1, k3, "a different salt derives a different key");
        // A different master key also diverges.
        assert_ne!(k1, derive_key(b"other-master-key", &salt_a));
    }

    #[test]
    fn manifest_mac_key_is_deterministic_and_domain_separated() {
        let master = b"super-secret-master-key";
        let k1 = derive_manifest_mac_key(master);
        let k2 = derive_manifest_mac_key(master);
        assert_eq!(k1, k2, "same master derives the same MAC key");
        assert_ne!(
            k1,
            derive_manifest_mac_key(b"other-master-key"),
            "a different master diverges"
        );
        // Domain separation: the MAC key must not equal a block key derived from
        // the same master (the block KDF also folds in a salt, but even an
        // empty-salt block key must differ thanks to the distinct context).
        assert_ne!(
            k1,
            derive_key(master, b""),
            "MAC key is domain-separated from the block key"
        );
    }

    #[test]
    fn manifest_mac_is_stable_and_message_sensitive() {
        let key = derive_manifest_mac_key(b"master");
        let mac = manifest_mac(&key, b"the blueprint bytes");
        assert_eq!(mac, manifest_mac(&key, b"the blueprint bytes"));
        assert_ne!(
            mac,
            manifest_mac(&key, b"the blueprint bytez"),
            "one flipped byte changes the MAC"
        );
        // A different key over the same message also diverges.
        assert_ne!(
            mac,
            manifest_mac(&derive_manifest_mac_key(b"other"), b"the blueprint bytes")
        );
    }

    #[test]
    fn encrypt_then_decrypt_roundtrips() {
        let cipher = BlockCipher::from_master(b"master", &random_salt()).for_file("node_props.blk");
        let nonce = BlockCipher::random_nonce();
        let plaintext = b"the quick brown fox compresses well well well".repeat(8);
        let aad = BlockAad::block(7, 21, plaintext.len() as u32, 3);
        let sealed = cipher.seal(aad, &nonce, &plaintext).unwrap();
        assert_eq!(
            sealed.len(),
            plaintext.len() + 16,
            "ciphertext carries a tag, and the AAD adds nothing on disk"
        );
        assert_ne!(sealed, plaintext);
        assert_eq!(cipher.open(aad, &nonce, &sealed).unwrap(), plaintext);
    }

    #[test]
    fn wrong_key_refuses_cleanly() {
        let salt = random_salt();
        let nonce = BlockCipher::random_nonce();
        let sealed = BlockCipher::from_master(b"right", &salt)
            .for_file("node_props.blk")
            .seal(BlockAad::position_only(0), &nonce, b"payload")
            .unwrap();
        let err = BlockCipher::from_master(b"wrong", &salt)
            .for_file("node_props.blk")
            .open(BlockAad::position_only(0), &nonce, &sealed)
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch)
        );
    }

    #[test]
    fn tampered_ciphertext_refuses_cleanly() {
        let cipher = BlockCipher::from_master(b"master", &random_salt()).for_file("edge_props.blk");
        let nonce = BlockCipher::random_nonce();
        let aad = BlockAad::position_only(0);
        let mut sealed = cipher.seal(aad, &nonce, b"payload bytes here").unwrap();
        sealed[0] ^= 0xff; // flip a bit
        assert!(cipher
            .open(BlockAad::position_only(0), &nonce, &sealed)
            .is_err());
    }

    /// HIK-140: a block is bound to (file, ordinal). Neither may drift.
    #[test]
    fn a_sealed_block_opens_only_at_its_own_file_and_ordinal() {
        let gen = BlockCipher::from_master(b"master", &random_salt());
        let nonce = BlockCipher::random_nonce();
        let msg = b"the block body";
        let aad = BlockAad::block(3, 3, msg.len() as u32, 1);
        let sealed = gen
            .for_file("node_props.blk")
            .seal(aad, &nonce, msg)
            .unwrap();

        // Its own (file, ordinal): opens.
        assert_eq!(
            gen.for_file("node_props.blk")
                .open(aad, &nonce, &sealed)
                .unwrap(),
            msg
        );
        // Same file, another ordinal: refused.
        assert_eq!(
            gen.for_file("node_props.blk")
                .open(BlockAad { ordinal: 4, ..aad }, &nonce, &sealed)
                .unwrap_err()
                .downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch)
        );
        // Same ordinal, another file of the same generation: refused.
        assert_eq!(
            gen.for_file("edge_props.blk")
                .open(aad, &nonce, &sealed)
                .unwrap_err()
                .downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch)
        );
        // The reserved header ordinal is not reachable by a leaf either.
        assert!(gen
            .for_file("node_props.blk")
            .open(
                BlockAad {
                    ordinal: HEADER_ORDINAL,
                    ..aad
                },
                &nonce,
                &sealed
            )
            .is_err());
        // …and neither may the directory row the reader is about to trust drift.
        assert!(gen
            .for_file("node_props.blk")
            .open(
                BlockAad {
                    rec_count: 2,
                    ..aad
                },
                &nonce,
                &sealed
            )
            .is_err());
        assert!(gen
            .for_file("node_props.blk")
            .open(BlockAad { raw_len: 99, ..aad }, &nonce, &sealed)
            .is_err());
        assert!(gen
            .for_file("node_props.blk")
            .open(
                BlockAad {
                    first_record: 2,
                    ..aad
                },
                &nonce,
                &sealed
            )
            .is_err());
    }

    #[test]
    fn file_key_is_deterministic_and_name_sensitive_and_domain_separated() {
        let master = b"super-secret-master-key";
        let salt = [3u8; SALT_LEN];
        let gen_key = derive_key(master, &salt);
        let k1 = derive_file_key(&gen_key, "node_props.blk");
        assert_eq!(k1, derive_file_key(&gen_key, "node_props.blk"));
        assert_ne!(k1, derive_file_key(&gen_key, "edge_props.blk"));
        // One character of the name is enough, including a prefix-only difference:
        // the fixed-width generation key leads, so no name is another's prefix-shift.
        assert_ne!(
            derive_file_key(&gen_key, "range/ab.isam"),
            derive_file_key(&gen_key, "range/a.isam")
        );
        // Domain-separated from the generation key itself and from the MAC key.
        assert_ne!(*k1, *gen_key);
        assert_ne!(*k1, *derive_manifest_mac_key(master));
    }

    #[test]
    fn aad_scheme_is_checked_by_type() {
        assert!(check_aad_scheme(AAD_SCHEME).is_ok());
        let err = check_aad_scheme("none").unwrap_err();
        assert_eq!(
            err.downcast_ref::<AadSchemeRejected>(),
            Some(&AadSchemeRejected {
                got: "none".to_string()
            })
        );
    }
}
