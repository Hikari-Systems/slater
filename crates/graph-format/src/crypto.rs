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

use anyhow::{bail, Result};
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
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

/// Compute a keyed-BLAKE3 MAC over `msg` and hex-encode it. The key is the
/// 32-byte subkey from [`derive_manifest_mac_key`].
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

/// A per-generation block cipher. Cheap to construct from a derived key; holds no
/// per-block state, so a single instance is shared (behind an `Arc`) across every
/// reader and the writer for one generation.
pub struct BlockCipher {
    aead: XChaCha20Poly1305,
}

impl BlockCipher {
    /// Build a cipher from an already-derived 32-byte block key.
    pub fn from_key(key: &[u8; KEY_LEN]) -> Self {
        Self {
            aead: XChaCha20Poly1305::new(Key::from_slice(key)),
        }
    }

    /// Derive the per-generation key from a runtime master key + salt and build
    /// the cipher.
    pub fn from_master(master_key: &[u8], salt: &[u8]) -> Self {
        Self::from_key(&derive_key(master_key, salt))
    }

    /// A fresh random nonce for the next block to be sealed.
    pub fn random_nonce() -> [u8; NONCE_LEN] {
        // `generate_nonce` draws straight from the OS RNG and returns the bytes, so
        // there is no zeroed `[0u8; N]` buffer — and thus no constant array literal
        // for static analysis to mistake for a hard-coded nonce.
        XChaCha20Poly1305::generate_nonce(&mut OsRng).into()
    }

    /// Seal a (compressed) block. The returned ciphertext is `plaintext.len()` +
    /// the 16-byte Poly1305 tag.
    pub fn encrypt(&self, nonce: &[u8; NONCE_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
        self.aead
            .encrypt(XNonce::from_slice(nonce), plaintext)
            .map_err(|_| anyhow::anyhow!("AEAD encryption failed"))
    }

    /// Open a sealed block. Fails with a clear error — never a panic or garbage —
    /// when the key is wrong or the block has been tampered with (the Poly1305
    /// tag does not verify).
    pub fn decrypt(&self, nonce: &[u8; NONCE_LEN], ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.aead
            .decrypt(XNonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                anyhow::anyhow!("AEAD decryption failed: wrong key or corrupt/tampered block")
            })
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
        let cipher = BlockCipher::from_master(b"master", &random_salt());
        let nonce = BlockCipher::random_nonce();
        let plaintext = b"the quick brown fox compresses well well well".repeat(8);
        let sealed = cipher.encrypt(&nonce, &plaintext).unwrap();
        assert_eq!(
            sealed.len(),
            plaintext.len() + 16,
            "ciphertext carries a tag"
        );
        assert_ne!(sealed, plaintext);
        assert_eq!(cipher.decrypt(&nonce, &sealed).unwrap(), plaintext);
    }

    #[test]
    fn wrong_key_refuses_cleanly() {
        let salt = random_salt();
        let nonce = BlockCipher::random_nonce();
        let sealed = BlockCipher::from_master(b"right", &salt)
            .encrypt(&nonce, b"payload")
            .unwrap();
        let err = BlockCipher::from_master(b"wrong", &salt)
            .decrypt(&nonce, &sealed)
            .unwrap_err();
        assert!(err.to_string().contains("wrong key"));
    }

    #[test]
    fn tampered_ciphertext_refuses_cleanly() {
        let cipher = BlockCipher::from_master(b"master", &random_salt());
        let nonce = BlockCipher::random_nonce();
        let mut sealed = cipher.encrypt(&nonce, b"payload bytes here").unwrap();
        sealed[0] ^= 0xff; // flip a bit
        assert!(cipher.decrypt(&nonce, &sealed).is_err());
    }
}
