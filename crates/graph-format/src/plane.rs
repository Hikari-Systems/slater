// SPDX-License-Identifier: Apache-2.0
//! Generic per-chunk "plane" codec: the reusable core behind the degree column
//! ([`crate::degree_ef`]), generalised so any *plane* of `u64` values can win the same
//! property — **O(1) random access in the encoded form, no materialised intermediate**, and
//! a compact resident form that is never a dense array.
//!
//! A plane is stored on disk in whichever of a few codecs is **smallest**
//! (`ef`/`rle`/`bitpacked`/`raw`/`zstd-dense`), and materialised in RAM as one of
//! three **compact** forms — [`PlaneChunk::Ef`], [`PlaneChunk::Rle`]
//! or [`PlaneChunk::BitPacked`]. A constant (or empty) run is just a single-run `rle`, so it
//! needs no dedicated codec. `ef`, `rle` and `bitpacked` are **decompress-free**
//! and are each their own resident form; `raw`/`zstd-dense` are disk-only and decode to a
//! compact resident form on fault, so nothing dense stays resident. `zstd-dense` alone pays a
//! decompress on fault and is penalised in selection (see [`PlaneCodecOpts::zstd_margin`]).
//!
//! ## Monotone planes only (for now)
//!
//! This module encodes an **already-monotone** (non-decreasing) `u64` sequence directly — the
//! shape of sorted key columns ([`crate::segment`]), endpoint postings ([`crate::postings`]),
//! and CSR neighbour runs ([`crate::topology`]). That is the complement of the degree column,
//! whose values are *unsorted counts* and are made monotone by an intra-chunk cumulative sum
//! before EF (the `IntegrateThenEf` strategy, kept in [`crate::degree_ef`]). The two share the
//! low-level bit machinery below; they differ only in whether the caller integrates first.
//!
//! The primitive that a monotone plane adds over the degree column is
//! [`PlaneChunk::successor`] — *first index whose value ≥ x* — which is the predecessor/
//! successor search a sorted key column already does, and the skip primitive behind leapfrog
//! list intersection. It is implemented as an O(log m) binary search over the O(1)
//! [`PlaneChunk::value_at`]; an EF bucket-skip successor can replace it later without changing
//! the on-disk format.

use anyhow::{bail, Result};

use crate::codec;
use crate::wire::{capacity_for, read_uvarint, write_uvarint, DecodeRejected};

/// One `select₁` sample per this many set bits (see [`select1`]).
pub(crate) const SELECT_SAMPLE: usize = 64;

/// Default zstd-selection penalty (see [`PlaneCodecOpts::zstd_margin`]). Latency-biased:
/// `zstd-dense` must be ≥ 2× smaller than the best decompress-free candidate to win.
pub const DEFAULT_ZSTD_SELECT_MARGIN: f64 = 0.5;

/// The default plane-codec zstd margin for a resolved compression-profile name
/// (`"local"`/`"remote"`/`"max"`/`"manual"`): wire-biased profiles (`remote`/`max`) let zstd
/// win on any size gain; everything else is latency-biased. Shared by the build-CLI resolver
/// and any retrofit tool so a retrofitted plane matches a fresh build's codec mix.
/// Ceiling on the number of values in a single plane record.
///
/// Every plane codec but RLE has its element count bounded by the record's own byte length —
/// `RawU64` spends 8 bytes a value, `BitPacked` at least one bit, EF's high bitmap one bit per
/// value. RLE does not: run lengths are uvarints, so six bytes are a well-formed single run
/// declaring 4·10⁹ values, and materialising that plane is a 34 GB allocation from a six-byte
/// record. The bound has to come from somewhere else, so it comes from the format's own
/// ceiling: this plane's `RawU64` form would be `n * 8` bytes, and `codec::MAX_BLOCK_BYTES`
/// already refuses a block that large. A plane above this is one no other codec could have
/// stored, so [`encode_plane`] refuses to *write* it and the RLE decoder refuses to read it —
/// the two agree, and a legitimate record can never be rejected.
///
/// 268M values: ~3× the 91.6M-node wikidata key column, the largest plane the builder emits
/// (and that one is distinct-ascending, so it encodes as EF, never RLE).
pub const MAX_PLANE_VALUES: usize = codec::MAX_BLOCK_BYTES / 8;

pub fn margin_for_profile(profile: &str) -> f64 {
    match profile {
        "remote" | "max" => 1.0,
        _ => DEFAULT_ZSTD_SELECT_MARGIN,
    }
}

/// Build-time codec knobs for a plane. Deployment-dependent: a filesystem/NVMe target wants
/// latency (prefer a decompress-free codec, so a low `zstd_margin`); an object-store target
/// wants small GETs (let zstd win more and compress harder, so a higher `zstd_margin` /
/// `zstd_level`). Set at the CLI/build config, not baked in.
#[derive(Clone, Copy, Debug)]
pub struct PlaneCodecOpts {
    /// zstd level for the `zstd-dense` candidate (the block container itself is uncompressed).
    pub zstd_level: i32,
    /// `zstd-dense` wins only when its encoded size is ≤ `zstd_margin` × the smallest
    /// *decompress-free* candidate. A zstd chunk pays a decompress **plus** a re-encode on
    /// every (re)fault — recurring — whereas the decompress-free codecs faults are decode-light.
    /// A low margin (< 1) makes zstd earn a large one-time disk/wire saving (latency-biased);
    /// a margin ≥ 1 lets it win on any size tie or loss (size/wire-biased).
    pub zstd_margin: f64,
}

impl Default for PlaneCodecOpts {
    fn default() -> Self {
        Self {
            zstd_level: 3,
            zstd_margin: DEFAULT_ZSTD_SELECT_MARGIN,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Shared bit-plane primitives — used by both this module and `degree_ef`. These are the exact
// algorithms the degree column shipped in v4, lifted here verbatim so the two codecs share one
// implementation; moving them does not change any serialised byte.
// ---------------------------------------------------------------------------------------------

/// Position of the `k`-th set bit (0-indexed) within a single word. `k` must be `< w.count_ones()`.
#[inline]
pub(crate) fn select_in_word(mut w: u64, mut k: u32) -> usize {
    loop {
        let t = w.trailing_zeros();
        if k == 0 {
            return t as usize;
        }
        k -= 1;
        w &= w - 1; // clear lowest set bit
    }
}

/// `ℓ = ⌊log₂(u / m)⌋`, clamped to 0 (the degenerate all-small / all-zero case). `u` is the
/// universe (max value or a bound on it), `m` the element count.
pub(crate) fn low_bits(universe: u64, m: u64) -> u8 {
    if universe < m || m == 0 {
        0
    } else {
        (universe / m).ilog2() as u8
    }
}

/// Write `l` bits of `v` (LSB-first) at bit offset `bitoff` into `lows`. Build-time only, so a
/// simple bit-by-bit loop is fine.
pub(crate) fn write_low(lows: &mut [u8], bitoff: usize, v: u64, l: u8) {
    for b in 0..l as usize {
        if (v >> b) & 1 != 0 {
            let p = bitoff + b;
            lows[p / 8] |= 1 << (p % 8);
        }
    }
}

/// Low `l` bits of packed element `i` from a byte-packed low-bits plane.
#[inline]
pub(crate) fn read_low(lows: &[u8], l: u8, i: usize) -> u64 {
    if l == 0 {
        return 0;
    }
    let bit = i * l as usize;
    let byte = bit / 8;
    let shift = bit % 8;
    let mut buf = [0u8; 8];
    let avail = lows.len().saturating_sub(byte).min(8);
    buf[..avail].copy_from_slice(&lows[byte..byte + avail]);
    let word = u64::from_le_bytes(buf);
    (word >> shift) & ((1u64 << l) - 1)
}

/// `sample[s]` = bit position of the `(s·SELECT_SAMPLE)`-th set bit, for the first `m` ones.
pub(crate) fn build_sample(highs: &[u64], m: usize) -> Vec<u32> {
    let mut sample = Vec::with_capacity(m / SELECT_SAMPLE + 1);
    let mut ones = 0usize;
    for (wi, &w) in highs.iter().enumerate() {
        let mut ww = w;
        while ww != 0 {
            if ones % SELECT_SAMPLE == 0 {
                sample.push((wi * 64 + ww.trailing_zeros() as usize) as u32);
            }
            ones += 1;
            ww &= ww - 1;
            if ones == m {
                return sample;
            }
        }
    }
    sample
}

/// Position of the `i`-th one-bit (0-indexed) in a high bitmap — O(1) via the sampled index.
#[inline]
pub(crate) fn select1(highs: &[u64], sample: &[u32], i: usize) -> usize {
    let s = i / SELECT_SAMPLE;
    let start = sample[s] as usize;
    let ones_at_start = s * SELECT_SAMPLE;
    if i == ones_at_start {
        return start;
    }
    let mut remaining = i - ones_at_start; // additional ones to advance past `start`
    let mut wi = (start + 1) / 64;
    let boff = (start + 1) % 64;
    let mut w = highs[wi] & (!0u64 << boff);
    loop {
        let c = w.count_ones() as usize;
        if remaining <= c {
            return wi * 64 + select_in_word(w, (remaining - 1) as u32);
        }
        remaining -= c;
        wi += 1;
        w = highs[wi];
    }
}

// ---------------------------------------------------------------------------------------------
// Fixed-width bit-packing (frame-of-reference). The other decompress-free O(1) codec: element
// i is `base + ((word >> shift) & mask)`, no scan, no state. Wins over EF when the value range
// is small enough that `⌈log₂(range)⌉` bits/element beats EF's `~2 + log₂(range/m)`.
// ---------------------------------------------------------------------------------------------

#[inline]
fn bp_words(n: usize, width: u8) -> usize {
    (n * width as usize).div_ceil(64)
}

/// Read the `width`-bit field at element `i` (LSB-first across `u64` words). `width` ∈ `1..=64`.
#[inline]
fn bp_read(words: &[u64], width: u8, i: usize) -> u64 {
    let bit = i * width as usize;
    let w = bit >> 6;
    let s = bit & 63;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    let lo = words[w] >> s;
    if s + width as usize <= 64 {
        lo & mask
    } else {
        let hi = words[w + 1] << (64 - s);
        (lo | hi) & mask
    }
}

/// Write the `width`-bit `v` at element `i`. `v` must fit in `width` bits.
#[inline]
fn bp_write(words: &mut [u64], width: u8, i: usize, v: u64) {
    let bit = i * width as usize;
    let w = bit >> 6;
    let s = bit & 63;
    words[w] |= v << s;
    if s + width as usize > 64 {
        words[w + 1] |= v >> (64 - s);
    }
}

/// Fixed-width, frame-of-reference bit-packed plane: `value_at(i) = base + unpack(i)`.
#[derive(Clone)]
pub struct BitPacked {
    n: u32,
    base: u64,
    /// Bits per element, `1..=64` (a uniform run is a single-run `Rle`, never a `0`-width plane).
    width: u8,
    words: Box<[u64]>,
}

impl BitPacked {
    /// Build from a value slice. `values` must be non-empty; a uniform slice is representable
    /// (`width == 1`, all-zero payload) but the selector prefers its cheaper single-run `Rle`.
    /// `base` is the min; `width` covers `max - base`.
    fn from_values(values: &[u64]) -> Self {
        let base = *values.iter().min().unwrap();
        let max = *values.iter().max().unwrap();
        let span = max - base;
        let width = if span == 0 {
            1
        } else {
            64 - span.leading_zeros() as u8
        };
        let mut words = vec![0u64; bp_words(values.len(), width).max(1)];
        for (i, &v) in values.iter().enumerate() {
            bp_write(&mut words, width, i, v - base);
        }
        Self {
            n: values.len() as u32,
            base,
            width,
            words: words.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    #[inline]
    pub fn value_at(&self, i: usize) -> u64 {
        self.base + bp_read(&self.words, self.width, i)
    }

    pub fn resident_bytes(&self) -> usize {
        self.words.len() * 8 + std::mem::size_of::<Self>()
    }

    fn body_len(&self) -> usize {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf.len()
    }

    /// Body: `uvarint(n) ‖ uvarint(base) ‖ u8(width) ‖ words(u64 LE)`. `words` length is derived
    /// from `n` and `width`, so it is not stored.
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.n as u64);
        write_uvarint(buf, self.base);
        buf.push(self.width);
        for &w in self.words.iter() {
            buf.extend_from_slice(&w.to_le_bytes());
        }
    }

    fn deserialize(body: &[u8]) -> Result<Self> {
        let mut r = body;
        let n = read_uvarint(&mut r)? as u32;
        let base = read_uvarint(&mut r)?;
        let (&width, mut r) = r
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("bitpacked plane body truncated at width"))?;
        if width == 0 || width > 64 {
            bail!("bitpacked plane width {width} out of range 1..=64");
        }
        let nwords = bp_words(n as usize, width);
        let need = nwords * 8;
        if r.len() != need {
            bail!(
                "bitpacked plane body has {} word bytes, expected {need} (n={n}, width={width})",
                r.len()
            );
        }
        let mut words = Vec::with_capacity(nwords);
        while !r.is_empty() {
            let (w, rest) = r.split_at(8);
            words.push(u64::from_le_bytes(w.try_into().unwrap()));
            r = rest;
        }
        Ok(Self {
            n,
            base,
            width,
            words: words.into_boxed_slice(),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Elias–Fano over an already-monotone u64 sequence.
// ---------------------------------------------------------------------------------------------

/// Elias–Fano encoding of a **non-decreasing** `u64` sequence of `m` values. Random access
/// `value_at(i)` is O(1) (one sampled `select₁` + a packed low read); `successor` is the skip
/// primitive. Sizes to ~`(2 + ℓ)` bits per element.
#[derive(Clone)]
pub struct EfMono {
    /// Element count.
    m: u32,
    /// Low-bits width `ℓ`.
    l: u8,
    /// Packed low bits: `m` values of `ℓ` bits each, LSB-first. Empty when `ℓ == 0`.
    lows: Box<[u8]>,
    /// High-bits unary bitmap as `u64` words; element `i`'s one-bit is at `(v[i] >> ℓ) + i`.
    highs: Box<[u64]>,
    /// Sampled `select₁` index. Rebuilt on decode, not serialised.
    sample: Box<[u32]>,
}

impl EfMono {
    /// Encode a non-decreasing `values` slice.
    pub fn encode(values: &[u64]) -> Self {
        debug_assert!(
            values.windows(2).all(|w| w[0] <= w[1]),
            "EfMono needs monotone input"
        );
        let universe = values.last().copied().unwrap_or(0);
        Self::from_ascending(values.len(), universe, values.iter().copied())
    }

    /// Encode from a **streaming** ascending source whose length (`m`) and `universe` (the last
    /// value, or 0 when empty) are known up front. One pass, no materialised `Vec` — so a caller
    /// holding the values as a bitmap (endpoint postings) or an external-sort drain can encode
    /// without a dense array. Produces byte-identical output to [`Self::encode`] on the same
    /// sequence, which is what lets the builder's two posting write-paths agree.
    pub fn from_ascending(m: usize, universe: u64, values: impl Iterator<Item = u64>) -> Self {
        let l = low_bits(universe, m as u64);
        let mask = if l == 0 { 0 } else { (1u64 << l) - 1 };

        let hi_max = (universe >> l) as usize;
        let nbits = hi_max + m;
        let nwords = nbits.div_ceil(64).max(1);
        let mut highs = vec![0u64; nwords];

        let low_bits_total = m * l as usize;
        let mut lows = vec![0u8; low_bits_total.div_ceil(8)];

        let mut count = 0usize;
        let mut prev = 0u64;
        for (i, v) in values.enumerate() {
            debug_assert!(i == 0 || v >= prev, "from_ascending needs monotone input");
            debug_assert!(
                v <= universe,
                "from_ascending value exceeds declared universe"
            );
            prev = v;
            let hi = (v >> l) as usize;
            let pos = hi + i;
            highs[pos / 64] |= 1u64 << (pos % 64);
            if l != 0 {
                write_low(&mut lows, i * l as usize, v & mask, l);
            }
            count += 1;
        }
        debug_assert_eq!(count, m, "from_ascending element count disagreed with m");

        let sample = build_sample(&highs, m);
        Self {
            m: m as u32,
            l,
            lows: lows.into_boxed_slice(),
            highs: highs.into_boxed_slice(),
            sample: sample.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.m as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.m == 0
    }

    /// The `i`-th value (`i ∈ 0..m`). O(1).
    #[inline]
    pub fn value_at(&self, i: usize) -> u64 {
        let p = select1(&self.highs, &self.sample, i);
        (((p - i) as u64) << self.l) | read_low(&self.lows, self.l, i)
    }

    /// First index whose value is `>= x` (in `0..=m`) — the skip primitive. Binary search over
    /// the O(1) [`Self::value_at`].
    #[inline]
    pub fn successor(&self, x: u64) -> usize {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.value_at(mid) < x {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Whether `x` is present. O(log m).
    #[inline]
    pub fn contains(&self, x: u64) -> bool {
        let i = self.successor(x);
        i < self.len() && self.value_at(i) == x
    }

    /// Iterate values in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len()).map(move |i| self.value_at(i))
    }

    pub fn serialized_len(&self) -> usize {
        9 + self.lows.len() + self.highs.len() * 8
    }

    /// The exact serialised length an EF record over `m` elements with maximum value `universe`
    /// will occupy, without building it — for buffer/budget reservation.
    pub fn serialized_len_for(m: usize, universe: u64) -> usize {
        let l = low_bits(universe, m as u64);
        let low_bytes = (m * l as usize).div_ceil(8);
        let hi_max = (universe >> l) as usize;
        let nwords = (hi_max + m).div_ceil(64).max(1);
        9 + low_bytes + nwords * 8
    }

    pub fn resident_bytes(&self) -> usize {
        self.lows.len() + self.highs.len() * 8 + self.sample.len() * 4
    }

    /// Serialise: `u32 m ‖ u8 l ‖ u32 n_words ‖ lows ‖ highs(u64 LE)`. The sample is rebuilt.
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        out.extend_from_slice(&self.m.to_le_bytes());
        out.push(self.l);
        out.extend_from_slice(&(self.highs.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.lows);
        for &w in self.highs.iter() {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    pub fn deserialize(body: &[u8]) -> Result<Self> {
        if body.len() < 9 {
            bail!("ef-mono body too short: {} bytes", body.len());
        }
        let m = u32::from_le_bytes(body[0..4].try_into().unwrap());
        let l = body[4];
        let nwords = u32::from_le_bytes(body[5..9].try_into().unwrap()) as usize;
        let low_bytes = (m as usize * l as usize).div_ceil(8);
        let high_bytes = nwords * 8;
        let need = 9 + low_bytes + high_bytes;
        if body.len() != need {
            bail!(
                "ef-mono body is {} bytes, expected {need} (m={m}, l={l}, words={nwords})",
                body.len()
            );
        }
        let lows = body[9..9 + low_bytes].to_vec().into_boxed_slice();
        let mut highs = Vec::with_capacity(nwords);
        let hstart = 9 + low_bytes;
        for w in body[hstart..hstart + high_bytes].chunks_exact(8) {
            highs.push(u64::from_le_bytes(w.try_into().unwrap()));
        }
        // The byte-length check above validates the *shape* of the body but says nothing about
        // its content: an EF high bitmap must hold exactly `m` one-bits (one per value), and
        // nothing so far requires that. With fewer, `build_sample` returns a short sample and
        // `select1` — which walks `highs` counting ones until it has passed `i` of them —
        // indexes `sample[s]` / `highs[wi]` off the end of the slice. That is an out-of-bounds
        // panic on an ordinary `value_at`, from bytes an attacker with data-dir write access
        // controls. Verify the invariant here, once at decode, rather than on every select.
        let ones: usize = highs.iter().map(|w| w.count_ones() as usize).sum();
        if ones != m as usize {
            return Err(DecodeRejected::EfBitCount {
                what: "ef-mono plane",
                declared: m as usize,
                found: ones,
            }
            .into());
        }
        let sample = build_sample(&highs, m as usize);
        Ok(Self {
            m,
            l,
            lows,
            highs: highs.into_boxed_slice(),
            sample: sample.into_boxed_slice(),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Run-length over u64 values.
// ---------------------------------------------------------------------------------------------

/// Run-length encoding of *consecutive* equal `u64` values, kept run-structured on disk and
/// resident. `value_at` is an O(log R) binary search over run starts (R = run count).
#[derive(Clone)]
pub struct RleU64 {
    n: u32,
    values: Box<[u64]>,
    /// `starts[k]` = first index of run `k` (strictly ascending, `starts[0] == 0`).
    starts: Box<[u32]>,
}

impl RleU64 {
    fn from_values(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                n: 0,
                values: Box::from([]),
                starts: Box::from([]),
            };
        }
        let mut vals = Vec::new();
        let mut starts = Vec::new();
        let mut prev = values[0];
        vals.push(prev);
        starts.push(0u32);
        for (i, &v) in values.iter().enumerate().skip(1) {
            if v != prev {
                vals.push(v);
                starts.push(i as u32);
                prev = v;
            }
        }
        Self {
            n: values.len() as u32,
            values: vals.into_boxed_slice(),
            starts: starts.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    #[inline]
    pub fn value_at(&self, off: usize) -> u64 {
        let k = self.starts.partition_point(|&s| s as usize <= off) - 1;
        self.values[k]
    }

    pub fn resident_bytes(&self) -> usize {
        self.values.len() * 8 + self.starts.len() * 4
    }

    fn body_len(&self) -> usize {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf.len()
    }

    /// Body: `uvarint(n) ‖ uvarint(run_count) ‖ run_count × (uvarint(value) ‖ uvarint(length))`.
    fn serialize_into(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.n as u64);
        write_uvarint(buf, self.values.len() as u64);
        for k in 0..self.values.len() {
            let end = if k + 1 < self.starts.len() {
                self.starts[k + 1]
            } else {
                self.n
            };
            let len = end - self.starts[k];
            write_uvarint(buf, self.values[k]);
            write_uvarint(buf, len as u64);
        }
    }

    fn deserialize(body: &[u8]) -> Result<Self> {
        let mut r = body;
        let n = read_uvarint(&mut r)? as u32;
        // Unlike the other plane codecs, RLE's element count `n` is *not* bounded by the
        // record's byte length: run lengths are uvarints, so `01 ff ff ff ff 0f` — six bytes —
        // is a well-formed single run declaring 4·10⁹ values, and `to_values` would then
        // materialise a 34 GB `Vec`. Bound it by [`MAX_PLANE_VALUES`], which `encode_plane`
        // refuses to exceed — so nothing this rejects is anything a writer could have emitted.
        if n as usize > MAX_PLANE_VALUES {
            return Err(DecodeRejected::TooManyElements {
                what: "rle plane",
                n: n as u64,
                max: MAX_PLANE_VALUES,
            }
            .into());
        }
        let run_count = read_uvarint(&mut r)? as usize;
        // Each run costs ≥2 bytes (value ‖ length), so clamp the reservation to what the body
        // can justify — a forged run count errors in the loop, not in the allocator.
        let cap = capacity_for(run_count, r.len(), 2);
        let mut values = Vec::with_capacity(cap);
        let mut starts = Vec::with_capacity(cap);
        let mut acc = 0u64;
        for _ in 0..run_count {
            let value = read_uvarint(&mut r)?;
            let len = read_uvarint(&mut r)?;
            if len == 0 {
                bail!("rle plane has a zero-length run");
            }
            values.push(value);
            starts.push(acc as u32);
            acc += len;
        }
        if acc != n as u64 {
            bail!("rle plane run lengths sum to {acc}, expected {n}");
        }
        Ok(Self {
            n,
            values: values.into_boxed_slice(),
            starts: starts.into_boxed_slice(),
        })
    }

    fn to_values(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.n as usize);
        for k in 0..self.values.len() {
            let end = if k + 1 < self.starts.len() {
                self.starts[k + 1]
            } else {
                self.n
            };
            out.resize(end as usize, self.values[k]);
        }
        out
    }
}

// ---------------------------------------------------------------------------------------------
// Disk codec tag + resident plane chunk + selector.
// ---------------------------------------------------------------------------------------------

/// Disk codec tag, one byte at the head of each stored plane record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum PlaneKind {
    /// Elias–Fano over the monotone values. Body: [`EfMono`] serialisation.
    Ef = 1,
    /// Dense `u64`-LE, uncompressed. Body: `n × u64`. Escape hatch.
    RawU64 = 2,
    /// zstd over the dense `u64`-LE form. Body: `uvarint(n) ‖ zstd(n × u64)`. Penalised candidate.
    ZstdDense = 3,
    /// Run-length of *consecutive* equal values. Body: [`RleU64`] serialisation.
    Rle = 4,
    /// Frame-of-reference fixed-width bit-packing. Body: [`BitPacked`] serialisation.
    BitPacked = 5,
}

impl PlaneKind {
    fn from_u8(b: u8) -> Result<Self> {
        Ok(match b {
            1 => Self::Ef,
            2 => Self::RawU64,
            3 => Self::ZstdDense,
            4 => Self::Rle,
            5 => Self::BitPacked,
            _ => bail!("unknown plane codec tag {b}"),
        })
    }
}

/// A monotone plane resident in RAM: always one of the three **compact** forms. A uniform
/// (or empty) run is a single-run [`RleU64`] — there is no dedicated constant form.
pub enum PlaneChunk {
    /// Elias–Fano over the monotone values.
    Ef(EfMono),
    /// Run-length of consecutive equal values.
    Rle(RleU64),
    /// Frame-of-reference fixed-width bit-packing.
    BitPacked(BitPacked),
}

impl PlaneChunk {
    pub fn len(&self) -> usize {
        match self {
            PlaneChunk::Ef(e) => e.len(),
            PlaneChunk::Rle(r) => r.len(),
            PlaneChunk::BitPacked(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn resident_bytes(&self) -> usize {
        match self {
            PlaneChunk::Ef(e) => e.resident_bytes(),
            PlaneChunk::Rle(r) => r.resident_bytes(),
            PlaneChunk::BitPacked(b) => b.resident_bytes(),
        }
    }

    /// The `i`-th value, or `None` if `i` is out of range (mirrors dense `slice.get(i)`). O(1).
    #[inline]
    pub fn value_at(&self, i: usize) -> Option<u64> {
        if i >= self.len() {
            return None;
        }
        Some(match self {
            PlaneChunk::Ef(e) => e.value_at(i),
            PlaneChunk::Rle(r) => r.value_at(i),
            PlaneChunk::BitPacked(b) => b.value_at(i),
        })
    }

    /// First index `i` whose value is `>= x` (in `0..=len`; `len` means "no such element").
    /// The plane is monotone, so this is a binary search over the O(1) [`Self::value_at`] —
    /// the same predecessor/successor search a sorted key column performs, in the encoded form.
    #[inline]
    pub fn successor(&self, x: u64) -> usize {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            // `value_at` is in-range because mid < hi <= len.
            if self.value_at(mid).unwrap() < x {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Materialise all values in index order — for the eager read and tests.
    pub fn to_values(&self) -> Vec<u64> {
        match self {
            PlaneChunk::Ef(e) => (0..e.len()).map(|i| e.value_at(i)).collect(),
            PlaneChunk::Rle(r) => r.to_values(),
            PlaneChunk::BitPacked(b) => (0..b.len()).map(|i| b.value_at(i)).collect(),
        }
    }
}

/// Build the compact resident form directly from a value slice: a single-run `Rle` when uniform
/// (or empty), else `Ef`. Used to decode a `raw`/`zstd-dense` disk chunk so nothing dense stays
/// resident.
fn resident_from_values(values: &[u64]) -> PlaneChunk {
    match values.first() {
        Some(&first) if !values.iter().all(|&v| v == first) => {
            PlaneChunk::Ef(EfMono::encode(values))
        }
        // Uniform or empty → one run.
        _ => PlaneChunk::Rle(RleU64::from_values(values)),
    }
}

/// Encode a **non-decreasing** `values` slice to a compact on-disk record: `[kind:u8] ‖ body`.
/// Prices `ef`, `rle`, `bitpacked`, `raw`, and (penalised) `zstd-dense`, and keeps the smallest
/// per `opts` — with `zstd-dense` taxed for the decompress it costs on every fault. A uniform run
/// falls out as a single-run `rle` (a constant is just a run); the empty slice is an empty `rle`.
pub fn encode_plane(values: &[u64], opts: &PlaneCodecOpts) -> Result<Vec<u8>> {
    // Never emit a record the decoder would refuse: `MAX_PLANE_VALUES` is a *shared* bound, so
    // the RLE arm cannot write a plane whose `n` `RleU64::deserialize` will reject.
    if values.len() > MAX_PLANE_VALUES {
        bail!(
            "plane of {} values exceeds the {MAX_PLANE_VALUES}-value ceiling",
            values.len()
        );
    }
    let n = values.len();

    // Empty short-circuits to an empty single-`Rle` record — `BitPacked::from_values` and the
    // other candidates assume a non-empty slice. A *uniform* slice falls through: its one-run
    // `Rle` candidate is the smallest and wins the selection below (a constant is just a run).
    if values.is_empty() {
        let mut out = vec![PlaneKind::Rle as u8];
        RleU64::from_values(values).serialize_into(&mut out);
        return Ok(out);
    }

    let ef = EfMono::encode(values);
    let ef_len = 1 + ef.serialized_len();
    let rle = RleU64::from_values(values);
    let rle_len = 1 + rle.body_len();
    let bp = BitPacked::from_values(values);
    let bp_len = 1 + bp.body_len();
    let raw_len = 1 + n * 8;

    // Decompress-free candidates compete on raw size; zstd is penalised by `opts.zstd_margin`.
    let free_len = ef_len.min(rle_len).min(bp_len).min(raw_len);

    let price_zstd =
        opts.zstd_margin > 0.0 && (opts.zstd_margin >= 1.0 || free_len.saturating_mul(4) > raw_len);
    let zstd = if price_zstd {
        let mut dense = Vec::with_capacity(n * 8);
        for &v in values {
            dense.extend_from_slice(&v.to_le_bytes());
        }
        let zbytes = codec::compress(&dense, opts.zstd_level)?;
        let mut hdr = Vec::with_capacity(1 + 10 + zbytes.len());
        hdr.push(PlaneKind::ZstdDense as u8);
        write_uvarint(&mut hdr, n as u64);
        let zstd_len = hdr.len() + zbytes.len();
        ((zstd_len as f64) <= free_len as f64 * opts.zstd_margin).then_some((hdr, zbytes))
    } else {
        None
    };

    if let Some((mut hdr, zbytes)) = zstd {
        hdr.extend_from_slice(&zbytes);
        Ok(hdr)
    } else if rle_len <= ef_len && rle_len <= bp_len && rle_len <= raw_len {
        let mut out = Vec::with_capacity(rle_len);
        out.push(PlaneKind::Rle as u8);
        rle.serialize_into(&mut out);
        Ok(out)
    } else if bp_len <= ef_len && bp_len <= raw_len {
        let mut out = Vec::with_capacity(bp_len);
        out.push(PlaneKind::BitPacked as u8);
        bp.serialize_into(&mut out);
        Ok(out)
    } else if ef_len <= raw_len {
        let mut out = Vec::with_capacity(ef_len);
        out.push(PlaneKind::Ef as u8);
        out.extend_from_slice(&ef.serialize());
        Ok(out)
    } else {
        let mut out = Vec::with_capacity(raw_len);
        out.push(PlaneKind::RawU64 as u8);
        for &v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        Ok(out)
    }
}

/// Decode a stored plane record into its compact resident form. `raw`/`zstd-dense` decode to the
/// value array and re-encode to `Ef`/`Rle` so nothing dense stays resident.
pub fn decode_plane(bytes: &[u8]) -> Result<PlaneChunk> {
    let (&tag, mut body) = bytes
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("empty plane record"))?;
    match PlaneKind::from_u8(tag)? {
        PlaneKind::Ef => Ok(PlaneChunk::Ef(EfMono::deserialize(body)?)),
        PlaneKind::Rle => Ok(PlaneChunk::Rle(RleU64::deserialize(body)?)),
        PlaneKind::BitPacked => Ok(PlaneChunk::BitPacked(BitPacked::deserialize(body)?)),
        PlaneKind::RawU64 => {
            if body.len() % 8 != 0 {
                bail!("raw-u64 plane body {} not a multiple of 8", body.len());
            }
            let vals: Vec<u64> = body
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            Ok(resident_from_values(&vals))
        }
        PlaneKind::ZstdDense => {
            let n = read_uvarint(&mut body)? as usize;
            // `n` is an on-disk varint: `n * 8` must not wrap (a wrapped product would be a
            // *small* bound that a body could then satisfy). `decompress` treats the product
            // as a hard cap and refuses anything above the block ceiling, so a forged `n`
            // costs an error, not an allocation.
            let raw_len = n.saturating_mul(8);
            let dense = codec::decompress(body, raw_len)?;
            if dense.len() != raw_len {
                bail!(
                    "zstd plane decoded {} bytes, expected {}",
                    dense.len(),
                    raw_len
                );
            }
            let vals: Vec<u64> = dense
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            Ok(resident_from_values(&vals))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// KeyColumn — a resident sorted-set presence column, the `Vec<u64>` replacement.
// ---------------------------------------------------------------------------------------------

/// A resident **ascending** `u64` key column stored as a compact [`PlaneChunk`] — the drop-in
/// replacement for a plain `Vec<u64>` presence set (segment / L0 key columns). Same
/// predecessor/successor search, ~6× less RAM. Keys must be non-decreasing; the sorted-set
/// membership map ([`Self::find`]) assumes they are distinct (as every slater key column is).
pub struct KeyColumn(PlaneChunk);

impl KeyColumn {
    /// Encode an ascending distinct key slice to its framed on-disk bytes (`uvarint(len) ‖ body`
    /// is applied by the caller; this returns just the plane record `[kind] ‖ body`).
    pub fn encode(keys: &[u64], opts: &PlaneCodecOpts) -> Result<Vec<u8>> {
        encode_plane(keys, opts)
    }

    /// Decode a plane record into a resident key column.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self(decode_plane(bytes)?))
    }

    /// Build directly from keys (default codec opts) — for tests and in-memory construction.
    pub fn from_keys(keys: &[u64]) -> Self {
        Self::decode(
            &encode_plane(keys, &PlaneCodecOpts::default()).expect("in-memory plane encode"),
        )
        .expect("in-memory plane decode")
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The `i`-th key (`i ∈ 0..len`), or `None`. O(1).
    #[inline]
    pub fn get(&self, i: usize) -> Option<u64> {
        self.0.value_at(i)
    }

    /// First / last key, or `None` when empty.
    #[inline]
    pub fn first(&self) -> Option<u64> {
        self.0.value_at(0)
    }

    #[inline]
    pub fn last(&self) -> Option<u64> {
        let n = self.len();
        if n == 0 {
            None
        } else {
            self.0.value_at(n - 1)
        }
    }

    /// `[min, max]` fence, or `None` when empty — the O(1) segment-skip pre-filter.
    #[inline]
    pub fn fence(&self) -> Option<(u64, u64)> {
        match (self.first(), self.last()) {
            (Some(lo), Some(hi)) => Some((lo, hi)),
            _ => None,
        }
    }

    /// Index of `x` if present, else `None` — the sorted-set membership → record-index map
    /// (replaces `slice.binary_search(&x).ok()`). O(log n) over the O(1) `get`.
    #[inline]
    pub fn find(&self, x: u64) -> Option<usize> {
        let i = self.0.successor(x);
        if i < self.len() && self.0.value_at(i) == Some(x) {
            Some(i)
        } else {
            None
        }
    }

    /// Lazily iterate the keys in ascending order (decodes from the plane; no materialisation).
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len()).map(move |i| self.0.value_at(i).unwrap())
    }

    /// Approximate resident footprint (bytes).
    pub fn resident_bytes(&self) -> usize {
        self.0.resident_bytes()
    }
}

impl std::fmt::Debug for KeyColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyColumn(len={}, resident={}B)",
            self.len(),
            self.resident_bytes()
        )
    }
}

/// Structural equality by decoded contents (not by codec choice) — for tests / round-trip
/// assertions where two columns are "equal" iff they hold the same keys.
impl PartialEq for KeyColumn {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl Eq for KeyColumn {}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> PlaneCodecOpts {
        PlaneCodecOpts::default()
    }

    /// Round-trip: every value survives encode → decode, `value_at`/`successor`/`to_values` agree
    /// with the reference, for a monotone slice.
    fn check_roundtrip(values: &[u64]) {
        let bytes = encode_plane(values, &opts()).unwrap();
        let chunk = decode_plane(&bytes).unwrap();
        assert_eq!(chunk.len(), values.len(), "len for {} vals", values.len());
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(
                chunk.value_at(i),
                Some(v),
                "index {i} of {} vals",
                values.len()
            );
        }
        assert_eq!(chunk.value_at(values.len()), None, "out-of-range index");
        assert_eq!(&chunk.to_values(), values);
        // successor agrees with a linear reference on a spread of probes.
        let probes = [
            0u64,
            values[0],
            values[0].saturating_sub(1),
            values[values.len() / 2],
            *values.last().unwrap(),
            values.last().unwrap().saturating_add(1),
            u64::MAX,
        ];
        for &x in &probes {
            let want = values.partition_point(|&v| v < x);
            assert_eq!(
                chunk.successor(x),
                want,
                "successor({x}) on {} vals",
                values.len()
            );
        }
    }

    #[test]
    fn roundtrip_strictly_ascending_sparse() {
        // A sorted distinct id set over a large universe — the key-column / postings shape.
        let vals: Vec<u64> = (0..5000u64)
            .map(|i| i.wrapping_mul(2_654_435_761) % 90_000_000)
            .collect();
        let mut sorted = vals;
        sorted.sort_unstable();
        sorted.dedup();
        check_roundtrip(&sorted);
    }

    #[test]
    fn roundtrip_dense_range_prefers_bitpacked_or_ef() {
        // A dense contiguous-ish range: small span, where BitPacked competes with EF.
        let vals: Vec<u64> = (1000..1000 + 4096u64).collect();
        check_roundtrip(&vals);
    }

    #[test]
    fn roundtrip_boundaries() {
        check_roundtrip(&[7]); // single → single-run Rle
        check_roundtrip(&[0, 5]);
        check_roundtrip(&[0, 0, 3, 3, 3, 9]); // equal runs → RLE candidate, still monotone
        let near_sample: Vec<u64> = (0..(SELECT_SAMPLE as u64 * 3 + 1)).map(|i| i * 3).collect();
        check_roundtrip(&near_sample);
    }

    #[test]
    fn constant_uses_single_run_rle() {
        // A uniform run has no dedicated codec — it is the smallest single-run `Rle`.
        for vals in [vec![0u64; 5000], vec![42u64; 5000]] {
            let bytes = encode_plane(&vals, &opts()).unwrap();
            assert_eq!(bytes[0], PlaneKind::Rle as u8);
            let chunk = decode_plane(&bytes).unwrap();
            assert!(matches!(chunk, PlaneChunk::Rle(_)));
            assert_eq!(chunk.value_at(0), Some(vals[0]));
            assert_eq!(chunk.value_at(4999), Some(vals[0]));
            // successor across the constant run.
            assert_eq!(chunk.successor(vals[0]), 0);
            assert_eq!(chunk.successor(vals[0] + 1), 5000);
        }
    }

    #[test]
    fn empty_plane_roundtrips_as_rle() {
        let bytes = encode_plane(&[], &opts()).unwrap();
        assert_eq!(bytes[0], PlaneKind::Rle as u8);
        let chunk = decode_plane(&bytes).unwrap();
        assert_eq!(chunk.len(), 0);
        assert!(chunk.is_empty());
        assert_eq!(chunk.value_at(0), None);
        assert_eq!(chunk.to_values(), Vec::<u64>::new());
    }

    #[test]
    fn consecutive_runs_pick_rle() {
        let mut vals = vec![0u64; 3000];
        vals.extend(std::iter::repeat_n(5, 2000));
        vals.extend(std::iter::repeat_n(9, 1500));
        let bytes = encode_plane(&vals, &opts()).unwrap();
        assert_eq!(
            bytes[0],
            PlaneKind::Rle as u8,
            "consecutive runs should pick RLE"
        );
        check_roundtrip(&vals);
    }

    #[test]
    fn bitpacked_roundtrips_directly() {
        // Exercise the FOR bit-packing across a word boundary at several widths.
        for width_span in [1u64, 3, 31, 63, 255, 1 << 20] {
            let vals: Vec<u64> = (0..300u64)
                .map(|i| 1_000_000 + (i * 7) % (width_span + 1))
                .collect();
            let mut sorted = vals;
            sorted.sort_unstable();
            let bp = BitPacked::from_values(&sorted);
            for (i, &v) in sorted.iter().enumerate() {
                assert_eq!(bp.value_at(i), v, "width_span {width_span} idx {i}");
            }
            let mut body = Vec::new();
            body.insert(0, PlaneKind::BitPacked as u8);
            bp.serialize_into(&mut body);
            let chunk = decode_plane(&body).unwrap();
            for (i, &v) in sorted.iter().enumerate() {
                assert_eq!(chunk.value_at(i), Some(v));
            }
        }
    }

    #[test]
    fn raw_and_zstd_decode_to_compact_resident() {
        // Force RawU64 by a tiny high-entropy set where nothing compresses; then a wire-biased
        // build for zstd. Both must decode to a compact (non-Raw) resident form.
        let vals: Vec<u64> = (0..64u64)
            .map(|i| i.wrapping_mul(0x9E37_79B1) % 1_000_000)
            .collect();
        let mut sorted = vals;
        sorted.sort_unstable();
        sorted.dedup();
        let wire = PlaneCodecOpts {
            zstd_level: 19,
            zstd_margin: 1.0,
        };
        for o in [opts(), wire] {
            let bytes = encode_plane(&sorted, &o).unwrap();
            let chunk = decode_plane(&bytes).unwrap();
            assert!(matches!(
                chunk,
                PlaneChunk::Ef(_) | PlaneChunk::Rle(_) | PlaneChunk::BitPacked(_)
            ));
            for (i, &v) in sorted.iter().enumerate() {
                assert_eq!(chunk.value_at(i), Some(v));
            }
        }
    }

    #[test]
    fn key_column_membership_fence_and_iter() {
        let keys: Vec<u64> = [3u64, 10, 11, 12, 50, 90_000_000, 90_000_001]
            .into_iter()
            .collect();
        let kc = KeyColumn::from_keys(&keys);
        assert_eq!(kc.len(), keys.len());
        assert_eq!(kc.fence(), Some((3, 90_000_001)));
        assert_eq!(kc.iter().collect::<Vec<_>>(), keys);
        // Membership → index, matching slice binary_search.
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(kc.find(k), Some(i), "find({k})");
        }
        for absent in [0u64, 2, 4, 13, 49, 51, 89_999_999, 90_000_002, u64::MAX] {
            assert_eq!(kc.find(absent), None, "find({absent}) absent");
            assert_eq!(kc.find(absent), keys.binary_search(&absent).ok());
        }
        // Empty column.
        let empty = KeyColumn::from_keys(&[]);
        assert!(empty.is_empty());
        assert_eq!(empty.fence(), None);
        assert_eq!(empty.find(0), None);
        assert_eq!(empty.iter().count(), 0);
    }

    #[test]
    fn rejects_corrupt_bodies() {
        let good = encode_plane(&[1u64, 2, 3, 4, 5], &opts()).unwrap();
        assert!(decode_plane(&good).is_ok());
        let mut bad = good.clone();
        bad.pop();
        assert!(
            decode_plane(&bad).is_err(),
            "truncated body must error, not mis-decode"
        );
        assert!(decode_plane(&[]).is_err(), "empty record must error");
        assert!(decode_plane(&[99]).is_err(), "unknown tag must error");
    }
}
