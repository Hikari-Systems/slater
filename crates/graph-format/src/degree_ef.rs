// SPDX-License-Identifier: Apache-2.0
//! Per-chunk Elias–Fano encoding for the dense degree column.
//!
//! A degree chunk (up to [`DEGREES_PER_RECORD`](crate::nodedegree::DEGREES_PER_RECORD)
//! per-node degrees) is stored on disk in whichever of a few codecs is **smallest**
//! (`constant`/`ef`/`rle`/`raw`/`zstd-dense`), and materialised in RAM as one of three
//! **compact** forms — [`DegreeChunk::Ef`] or [`DegreeChunk::Rle`] (a uniform run is a single-run Rle)
//! — never a dense 1 MiB `u32` array. The disk codec minimises bytes; the resident codec
//! minimises RAM.
//!
//! `constant`, `ef` and `rle` are **decompress-free** and are each their own resident form
//! (no re-encode on fault); `rle` also stays run-structured resident, ideal for segmented /
//! bulk-imported degree regions and the all-zero isolated tail. `zstd-dense` and `raw` are
//! disk-only: they decode to the compact resident form on fault (re-encoding to `ef`/`constant`),
//! so nothing dense stays resident. `zstd-dense` alone pays a decompress on fault, so it is
//! penalised in selection (see [`DegreeCodecOpts::zstd_margin`]) — it wins only for interleaved
//! repetition that `ef`/`rle` cannot capture. The ~6× residency win is uniform across *all*
//! chunks (including the numerous low-degree "leaf" chunks), not only the ones that stored as EF.
//!
//! ## Why Elias–Fano, and on what
//!
//! Degrees themselves are not sorted, so we EF-encode the **intra-chunk cumulative sum**
//! `c = 0, d₀, d₀+d₁, …, total` — a monotone sequence of `n+1` values (the trailing
//! `total` is the sentinel that lets the last node's degree be recovered without reaching
//! into the next chunk). Then `degree(slot) = c[slot+1] − c[slot]`, and each `c[i]` is
//! recovered in O(1) from a `select₁` over the high-bits bitmap plus a packed low-bits
//! read. See [`EfChunk`].
//!
//! The classic Elias–Fano split: pick `ℓ = ⌊log₂(u/m)⌋` (`u` = universe = `total`,
//! `m = n+1`); store each value's low `ℓ` bits verbatim (they carry no positional
//! information) and its high bits as a unary bucket bitmap where element `i`'s one-bit
//! sits at position `high[i] + i`. Cost ≈ `m·(2 + ℓ)` bits, within ~½ bit/element of the
//! information-theoretic floor, with O(1) random access.

use anyhow::{bail, Result};

use crate::codec;
use crate::plane::{self, build_sample, low_bits, read_low, write_low};
use crate::wire::{capacity_for, read_uvarint, write_uvarint, DecodeRejected};

/// The degree column reuses the generic plane-codec knobs ([`crate::plane`]); these aliases keep
/// the historical names at the call sites (`--degree-zstd-margin`, the retrofit tool) while the
/// type and its selection semantics live in one place.
pub use crate::plane::{
    margin_for_profile, PlaneCodecOpts as DegreeCodecOpts, DEFAULT_ZSTD_SELECT_MARGIN,
};

/// Disk codec tag, one byte at the head of each stored record. The build picks the tag
/// whose encoding is smallest for that chunk; the reader dispatches on it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum ChunkKind {
    /// Elias–Fano over the intra-chunk cumulative sum. Body: [`EfChunk`] serialisation.
    /// The common winner for heterogeneous / hub-heavy chunks.
    Ef = 1,
    /// Dense `u32`-LE, uncompressed. Body: `n × u32`. Escape hatch for a chunk EF loses on.
    RawU32 = 2,
    /// zstd over the dense `u32`-LE form. Body: `u32 n ‖ zstd(n × u32)`. A *candidate* only
    /// — it wins on low-entropy-but-not-constant chunks where zstd finds structure EF can't.
    ZstdDense = 3,
    /// Run-length encoding of *consecutive* equal degrees. Body:
    /// `uvarint(n) ‖ uvarint(run_count) ‖ run_count × ( uvarint(value) ‖ uvarint(length) )`.
    /// Decompress-free and stays run-structured resident (tiny for bulk-imported/segmented
    /// degree regions and the all-zero isolated tail). Only captures *consecutive* runs; zstd
    /// covers interleaved repetition RLE cannot.
    Rle = 4,
}

impl ChunkKind {
    fn from_u8(b: u8) -> Result<Self> {
        Ok(match b {
            1 => Self::Ef,
            2 => Self::RawU32,
            3 => Self::ZstdDense,
            4 => Self::Rle,
            _ => bail!("unknown degree-chunk codec tag {b}"),
        })
    }
}

/// Elias–Fano encoding of a chunk's cumulative degree sequence `c[0..=n]` (`m = n+1`
/// monotone values). Resident form: the packed low bits, the high-bits bitmap, and a
/// sampled `select₁` index. Sizes to ~`(2 + ℓ)` bits per node.
#[derive(Clone)]
pub struct EfChunk {
    /// Node count (degrees) in the chunk; the cumulative sequence has `n + 1` elements.
    n: u32,
    /// Low-bits width `ℓ`.
    l: u8,
    /// Packed low bits: `n+1` values of `ℓ` bits each, LSB-first. Empty when `ℓ == 0`.
    lows: Box<[u8]>,
    /// High-bits unary bitmap as `u64` words; element `i`'s one-bit is at `high[i] + i`.
    highs: Box<[u64]>,
    /// `sample[s]` = bit position of the `(s·SELECT_SAMPLE)`-th one-bit. Rebuilt on decode,
    /// not serialised.
    sample: Box<[u32]>,
}

impl EfChunk {
    /// Encode a chunk's degrees. Assumes `degrees` is non-empty; the empty chunk is routed to
    /// an empty [`ChunkKind::Rle`] before here (an all-equal chunk still encodes fine, but the
    /// selector prefers its cheaper single-run `rle`).
    fn encode(degrees: &[u32]) -> Self {
        let n = degrees.len();
        let m = n + 1; // cumulative has n+1 elements: c[0..=n]
        let total: u64 = degrees.iter().map(|&d| d as u64).sum();
        let l = low_bits(total, m as u64);
        let mask = if l == 0 { 0 } else { (1u64 << l) - 1 };

        // High bitmap spans positions 0..(hi_max + m): one bit per element at high[i]+i.
        let hi_max = (total >> l) as usize;
        let nbits = hi_max + m;
        let nwords = nbits.div_ceil(64).max(1);
        let mut highs = vec![0u64; nwords];

        let low_bits_total = m * l as usize;
        let mut lows = vec![0u8; low_bits_total.div_ceil(8)];

        let mut c = 0u64;
        // `i` runs to `m = n+1` (one past `degrees`, for the sentinel) and drives bit-offset
        // math, so a range loop is the right shape here — not a slice iteration.
        #[allow(clippy::needless_range_loop)]
        for i in 0..m {
            let hi = (c >> l) as usize;
            let pos = hi + i;
            highs[pos / 64] |= 1u64 << (pos % 64);
            if l != 0 {
                write_low(&mut lows, i * l as usize, c & mask, l);
            }
            if i < n {
                c += degrees[i] as u64;
            }
        }
        debug_assert_eq!(c, total);

        let sample = build_sample(&highs, m);
        Self {
            n: n as u32,
            l,
            lows: lows.into_boxed_slice(),
            highs: highs.into_boxed_slice(),
            sample: sample.into_boxed_slice(),
        }
    }

    /// Number of nodes (degrees) in the chunk.
    #[inline]
    pub fn len(&self) -> usize {
        self.n as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Low `ℓ` bits of cumulative element `i` (`i ∈ 0..=n`).
    #[inline]
    fn low(&self, i: usize) -> u64 {
        read_low(&self.lows, self.l, i)
    }

    /// Position of the `i`-th one-bit (0-indexed) in the high bitmap — O(1) via the sample.
    #[inline]
    fn select1(&self, i: usize) -> usize {
        plane::select1(&self.highs, &self.sample, i)
    }

    /// First set-bit position strictly after `p`. `p` is a valid one-bit position and a
    /// further one is known to exist (the caller only asks for consecutive cumulative
    /// elements), so this stays in bounds.
    #[inline]
    fn next_one_after(&self, p: usize) -> usize {
        let mut wi = (p + 1) / 64;
        let boff = (p + 1) % 64;
        let mut w = self.highs[wi] & (!0u64 << boff);
        while w == 0 {
            wi += 1;
            w = self.highs[wi];
        }
        wi * 64 + w.trailing_zeros() as usize
    }

    /// Exact degree of the `slot`-th node in the chunk (`slot ∈ 0..n`).
    ///
    /// `degree = c[slot+1] − c[slot]`. Both cumulative values need a `select₁`, but the
    /// `(slot+1)`-th one-bit is simply the next set bit after the `slot`-th — so we pay one
    /// sampled `select₁` and a cheap next-set-bit step, not two full scans. This is the degree-
    /// sum count fast path's inner loop (millions of scattered lookups), so halving the select
    /// work matters.
    #[inline]
    pub fn degree_at(&self, slot: usize) -> u32 {
        let p0 = self.select1(slot);
        let p1 = self.next_one_after(p0);
        let v0 = (((p0 - slot) as u64) << self.l) | self.low(slot);
        let v1 = (((p1 - (slot + 1)) as u64) << self.l) | self.low(slot + 1);
        (v1 - v0) as u32
    }

    /// Serialised size in bytes (header + lows + highs), for codec selection without
    /// allocating the buffer.
    fn serialized_len(&self) -> usize {
        9 + self.lows.len() + self.highs.len() * 8
    }

    /// Approximate resident footprint (packed lows + high bitmap + select sample).
    pub fn resident_bytes(&self) -> usize {
        self.lows.len() + self.highs.len() * 8 + self.sample.len() * 4
    }

    /// Serialise: `u32 n ‖ u8 l ‖ u32 n_words ‖ lows ‖ highs(u64 LE)`. `n_words` lets the
    /// reader split lows from highs; the sample is rebuilt, not stored.
    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.serialized_len());
        out.extend_from_slice(&self.n.to_le_bytes());
        out.push(self.l);
        out.extend_from_slice(&(self.highs.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.lows);
        for &w in self.highs.iter() {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    /// Inverse of [`Self::serialize`]; rebuilds the `select₁` sample.
    fn deserialize(body: &[u8]) -> Result<Self> {
        if body.len() < 9 {
            bail!("ef chunk body too short: {} bytes", body.len());
        }
        let n = u32::from_le_bytes(body[0..4].try_into().unwrap());
        let l = body[4];
        // Same invariant, same reasoning as `plane::EfMono::deserialize`: `l` is attacker-
        // controlled and every later shift by it is silently masked in release.
        plane::check_low_bits("ef degree chunk", l)?;
        let nwords = u32::from_le_bytes(body[5..9].try_into().unwrap()) as usize;
        let m = n as usize + 1;
        let low_bytes = (m * l as usize).div_ceil(8);
        let high_bytes = nwords * 8;
        let need = 9 + low_bytes + high_bytes;
        if body.len() != need {
            bail!(
                "ef chunk body is {} bytes, expected {need} (n={n}, l={l}, words={nwords})",
                body.len()
            );
        }
        let lows = body[9..9 + low_bytes].to_vec().into_boxed_slice();
        let mut highs = Vec::with_capacity(nwords);
        let hstart = 9 + low_bytes;
        for w in body[hstart..hstart + high_bytes].chunks_exact(8) {
            highs.push(u64::from_le_bytes(w.try_into().unwrap()));
        }
        // The byte-length check validates the body's shape, not its content: the high bitmap
        // must hold exactly `m` one-bits, and with fewer, `select1` walks off the end of
        // `highs`/`sample` — an out-of-bounds panic on an ordinary degree lookup. Same
        // invariant, same reasoning as `plane::EfMono::deserialize`.
        let ones: usize = highs.iter().map(|w| w.count_ones() as usize).sum();
        if ones != m {
            return Err(DecodeRejected::EfBitCount {
                what: "ef degree chunk",
                declared: m,
                found: ones,
            }
            .into());
        }
        let sample = build_sample(&highs, m);
        Ok(Self {
            n,
            l,
            lows,
            highs: highs.into_boxed_slice(),
            sample: sample.into_boxed_slice(),
        })
    }
}

/// Run-length encoding of a chunk's *consecutive* equal degrees, kept run-structured both on
/// disk and resident. Tiny for segmented/bulk-imported degree regions; `degree_at` is an
/// O(log R) binary search over run starts (R = run count).
#[derive(Clone)]
pub struct RleChunk {
    /// Total node count in the chunk.
    n: u32,
    /// One degree value per run, in node order.
    values: Box<[u32]>,
    /// `starts[k]` = first node index of run `k` (strictly ascending, `starts[0] == 0`).
    /// Parallel to `values`; no trailing sentinel.
    starts: Box<[u32]>,
}

impl RleChunk {
    /// Build the (value, start) runs from a degree slice. An empty slice is an empty run set
    /// (a uniform slice is a single run).
    fn from_degrees(degrees: &[u32]) -> Self {
        if degrees.is_empty() {
            return Self {
                n: 0,
                values: Box::from([]),
                starts: Box::from([]),
            };
        }
        let mut values = Vec::new();
        let mut starts = Vec::new();
        let mut prev = degrees[0];
        values.push(prev);
        starts.push(0u32);
        for (i, &d) in degrees.iter().enumerate().skip(1) {
            if d != prev {
                values.push(d);
                starts.push(i as u32);
                prev = d;
            }
        }
        Self {
            n: degrees.len() as u32,
            values: values.into_boxed_slice(),
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

    /// Exact degree of the `off`-th node (`off < n`): the value of the run containing `off`.
    #[inline]
    pub fn degree_at(&self, off: usize) -> u32 {
        // Number of run starts `<= off`; `>= 1` since `starts[0] == 0`. The containing run is
        // the last such start.
        let k = self.starts.partition_point(|&s| s as usize <= off) - 1;
        self.values[k]
    }

    /// Approximate resident footprint (bytes).
    pub fn resident_bytes(&self) -> usize {
        (self.values.len() + self.starts.len()) * 4
    }

    /// Encoded body length (no kind byte), for codec selection.
    fn body_len(&self) -> usize {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf.len()
    }

    /// Serialise the body: `uvarint(n) ‖ uvarint(run_count) ‖ run_count × (uvarint(value) ‖
    /// uvarint(length))`. Lengths (not starts) are stored so small runs stay 1 byte.
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
            write_uvarint(buf, self.values[k] as u64);
            write_uvarint(buf, len as u64);
        }
    }

    /// Inverse of [`Self::serialize_into`]; rebuilds `starts` from the run lengths and
    /// validates they sum to `n`.
    fn deserialize(body: &[u8]) -> Result<Self> {
        let mut r = body;
        let n = read_uvarint(&mut r)? as u32;
        // `n` is not bounded by the record's byte length — run lengths are uvarints, so a
        // six-byte single run can declare 4·10⁹ degrees and `to_degrees` would materialise a
        // 17 GB `Vec`. Bound it by [`MAX_CHUNK_DEGREES`], which `encode_chunk` refuses to
        // exceed — so nothing this rejects is anything a writer could have emitted.
        if n as usize > MAX_CHUNK_DEGREES {
            return Err(DecodeRejected::TooManyElements {
                what: "rle degree chunk",
                n: n as u64,
                max: MAX_CHUNK_DEGREES,
            }
            .into());
        }
        let run_count = read_uvarint(&mut r)? as usize;
        // Each run costs ≥2 bytes (value ‖ length); clamp the reservation to what the body can
        // justify so a forged run count errors in the loop, not in the allocator.
        let cap = capacity_for(run_count, r.len(), 2);
        let mut values = Vec::with_capacity(cap);
        let mut starts = Vec::with_capacity(cap);
        let mut acc = 0u64;
        for _ in 0..run_count {
            let value = read_uvarint(&mut r)? as u32;
            let len = read_uvarint(&mut r)?;
            if len == 0 {
                bail!("rle degree-chunk has a zero-length run");
            }
            values.push(value);
            starts.push(acc as u32);
            acc += len;
        }
        if acc != n as u64 {
            bail!("rle degree-chunk run lengths sum to {acc}, expected {n}");
        }
        Ok(Self {
            n,
            values: values.into_boxed_slice(),
            starts: starts.into_boxed_slice(),
        })
    }

    fn to_degrees(&self) -> Vec<u32> {
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

/// A degree chunk resident in RAM: `Ef`, or `Rle` (a uniform/all-zero chunk is a single-run
/// `Rle` — there is no dedicated constant form). Constructed by [`decode_chunk`]; queried by
/// the degree-column holder.
pub enum DegreeChunk {
    /// Elias–Fano over the cumulative degrees.
    Ef(EfChunk),
    /// Run-length encoding of consecutive equal degrees.
    Rle(RleChunk),
}

impl DegreeChunk {
    /// Number of nodes in the chunk.
    pub fn len(&self) -> usize {
        match self {
            DegreeChunk::Ef(ef) => ef.len(),
            DegreeChunk::Rle(rle) => rle.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate resident footprint of the chunk's payload (bytes).
    pub fn resident_bytes(&self) -> usize {
        match self {
            DegreeChunk::Ef(ef) => ef.resident_bytes(),
            DegreeChunk::Rle(rle) => rle.resident_bytes(),
        }
    }

    /// Exact degree of the `off`-th node in the chunk, or `None` if `off` is out of range
    /// (mirrors the dense `slice.get(off)` this replaces).
    #[inline]
    pub fn degree_at(&self, off: usize) -> Option<u32> {
        if off >= self.len() {
            return None;
        }
        Some(match self {
            DegreeChunk::Ef(ef) => ef.degree_at(off),
            DegreeChunk::Rle(rle) => rle.degree_at(off),
        })
    }

    /// Materialise the chunk's degrees in id order — for the eager full read and tests.
    pub fn to_degrees(&self) -> Vec<u32> {
        match self {
            DegreeChunk::Ef(ef) => (0..ef.len()).map(|i| ef.degree_at(i)).collect(),
            DegreeChunk::Rle(rle) => rle.to_degrees(),
        }
    }
}

/// Build the compact resident form directly from a degree slice: a single-run `Rle` when uniform
/// (or empty), else `Ef`. Used both to decode a `raw`/`zstd-dense` disk chunk and to construct EF.
fn resident_from_degrees(degrees: &[u32]) -> DegreeChunk {
    match degrees.first() {
        Some(&first) if !degrees.iter().all(|&d| d == first) => {
            DegreeChunk::Ef(EfChunk::encode(degrees))
        }
        // Uniform or empty → one run.
        _ => DegreeChunk::Rle(RleChunk::from_degrees(degrees)),
    }
}

/// Encode a chunk's degrees to a compact on-disk record: `[kind:u8] ‖ body`. Tries
/// `ef`, `rle`, `raw`, and (penalised) `zstd-dense`, and keeps the winner per `opts`. A uniform
/// (or all-zero) chunk falls out as a single-run `rle` — the smallest candidate and exactly the
/// degenerate case EF is worst at; the empty chunk is an empty `rle`.
/// Ceiling on the number of degrees in a single chunk record — the [`crate::plane::MAX_PLANE_VALUES`]
/// argument, for `u32` degrees: an RLE chunk's `n` is not bounded by its byte length, so bound it
/// by the format's own ceiling (this chunk's `RawU32` form would be `n * 4` bytes, which
/// `codec::MAX_BLOCK_BYTES` refuses). Shared by the encoder and the decoder so the two agree.
///
/// A real chunk is [`crate::nodedegree::DEGREES_PER_RECORD`] = 262 144 degrees — this clears it
/// by ~2000×, so it is a backstop against a forged record, never a constraint on a real one.
pub const MAX_CHUNK_DEGREES: usize = codec::MAX_BLOCK_BYTES / 4;

pub fn encode_chunk(degrees: &[u32], opts: &DegreeCodecOpts) -> Result<Vec<u8>> {
    // Never emit a chunk the decoder would refuse (see `MAX_CHUNK_DEGREES`).
    if degrees.len() > MAX_CHUNK_DEGREES {
        bail!(
            "degree chunk of {} degrees exceeds the {MAX_CHUNK_DEGREES}-degree ceiling",
            degrees.len()
        );
    }
    let n = degrees.len();

    // Empty short-circuits to an empty `rle` record (the other candidates assume a non-empty
    // slice). A *uniform* slice falls through: its one-run `rle` is the smallest and wins below.
    if degrees.is_empty() {
        let mut out = vec![ChunkKind::Rle as u8];
        RleChunk::from_degrees(degrees).serialize_into(&mut out);
        return Ok(out);
    }

    let ef = EfChunk::encode(degrees);
    let ef_len = 1 + ef.serialized_len();
    let rle = RleChunk::from_degrees(degrees);
    let rle_len = 1 + rle.body_len();
    let raw_len = 1 + n * 4;

    // Decompress-free candidates (ef/rle/raw) compete on raw size; zstd is penalised by
    // `opts.zstd_margin` — it wins only when small enough relative to the best of them, because
    // it alone pays a decompress + EF re-encode on every (re)fault.
    let free_len = ef_len.min(rle_len).min(raw_len);

    // Pricing zstd means a full compress (up to level 19) — the one expensive candidate. Skip
    // it when it cannot help: disabled (`margin <= 0`), or a latency-biased build (`margin < 1`)
    // where a decompress-free codec already compressed well (≥ 4× vs dense) — there we'd keep
    // the decompress-free form on fault anyway, so the level-19 pass is wasted build time. A
    // wire-biased build (`margin >= 1`) always prices it, to chase the smallest object.
    let price_zstd =
        opts.zstd_margin > 0.0 && (opts.zstd_margin >= 1.0 || free_len.saturating_mul(4) > raw_len);
    let zstd = if price_zstd {
        let mut dense = Vec::with_capacity(n * 4);
        for &d in degrees {
            dense.extend_from_slice(&d.to_le_bytes());
        }
        let zbytes = codec::compress(&dense, opts.zstd_level)?;
        let zstd_len = 1 + 4 + zbytes.len(); // kind + u32 n + zstd blob
        ((zstd_len as f64) <= free_len as f64 * opts.zstd_margin).then_some(zbytes)
    } else {
        None
    };

    if let Some(zbytes) = zstd {
        let mut out = Vec::with_capacity(1 + 4 + zbytes.len());
        out.push(ChunkKind::ZstdDense as u8);
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.extend_from_slice(&zbytes);
        Ok(out)
    } else if rle_len <= ef_len && rle_len <= raw_len {
        let mut out = Vec::with_capacity(rle_len);
        out.push(ChunkKind::Rle as u8);
        rle.serialize_into(&mut out);
        Ok(out)
    } else if ef_len <= raw_len {
        let mut out = Vec::with_capacity(ef_len);
        out.push(ChunkKind::Ef as u8);
        out.extend_from_slice(&ef.serialize());
        Ok(out)
    } else {
        let mut out = Vec::with_capacity(raw_len);
        out.push(ChunkKind::RawU32 as u8);
        for &d in degrees {
            out.extend_from_slice(&d.to_le_bytes());
        }
        Ok(out)
    }
}

/// Decode a stored record into its compact resident form. `raw`/`zstd-dense` are decoded to
/// the degree array and **re-encoded to EF (or a single-run Rle)** so nothing dense stays resident.
pub fn decode_chunk(bytes: &[u8]) -> Result<DegreeChunk> {
    let (&tag, body) = bytes
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("empty degree-chunk record"))?;
    match ChunkKind::from_u8(tag)? {
        ChunkKind::Ef => Ok(DegreeChunk::Ef(EfChunk::deserialize(body)?)),
        ChunkKind::Rle => Ok(DegreeChunk::Rle(RleChunk::deserialize(body)?)),
        ChunkKind::RawU32 => {
            if body.len() % 4 != 0 {
                bail!("raw degree-chunk body {} not a multiple of 4", body.len());
            }
            let degs: Vec<u32> = body
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();
            Ok(resident_from_degrees(&degs))
        }
        ChunkKind::ZstdDense => {
            if body.len() < 4 {
                bail!("zstd degree-chunk body too short: {} bytes", body.len());
            }
            let n = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
            // `n` is an on-disk `u32`, so `n * 4` is at most 16 GiB and cannot wrap; it is a
            // *claim*, which `decompress` enforces as a hard output cap (and refuses outright
            // above the block ceiling) rather than pre-allocating on it.
            let raw_len = n.saturating_mul(4);
            let dense = codec::decompress(&body[4..], raw_len)?;
            if dense.len() != raw_len {
                bail!(
                    "zstd degree-chunk decoded {} bytes, expected {}",
                    dense.len(),
                    raw_len
                );
            }
            let degs: Vec<u32> = dense
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();
            Ok(resident_from_degrees(&degs))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default build codec opts for tests (level 3, latency-biased margin).
    fn opts() -> DegreeCodecOpts {
        DegreeCodecOpts::default()
    }

    /// Round-trip: every slot's degree survives encode → decode for a range of shapes.
    fn check_roundtrip(degrees: &[u32]) {
        let bytes = encode_chunk(degrees, &opts()).unwrap();
        let chunk = decode_chunk(&bytes).unwrap();
        assert_eq!(chunk.len(), degrees.len(), "len for {} degs", degrees.len());
        for (i, &d) in degrees.iter().enumerate() {
            assert_eq!(
                chunk.degree_at(i),
                Some(d),
                "slot {i} of {} degs",
                degrees.len()
            );
        }
        assert_eq!(chunk.degree_at(degrees.len()), None, "out-of-range slot");
        assert_eq!(&chunk.to_degrees(), degrees);
    }

    #[test]
    fn roundtrip_hub_heavy_skewed() {
        // A few hubs among many small nodes — the EF sweet spot.
        let mut d: Vec<u32> = (0..5000).map(|i| (i % 4) as u32).collect();
        d[10] = 90_000;
        d[2000] = 1_000_000;
        d[4999] = 42;
        check_roundtrip(&d);
    }

    #[test]
    fn roundtrip_monotone_and_random_like() {
        let ramp: Vec<u32> = (0..3000u32).map(|i| i / 3).collect();
        check_roundtrip(&ramp);
        // Deterministic scatter (no rng in tests).
        let scatter: Vec<u32> = (0..4096u32)
            .map(|i| i.wrapping_mul(2654435761) % 137)
            .collect();
        check_roundtrip(&scatter);
    }

    #[test]
    fn roundtrip_boundaries() {
        check_roundtrip(&[7]); // single node → single-run Rle
        check_roundtrip(&[0, 5]); // leading zero
        check_roundtrip(&[5, 0, 0, 9, 0]); // interior zeros
                                           // Exactly on / around a select sample boundary.
        let near_sample: Vec<u32> = (0..(crate::plane::SELECT_SAMPLE as u32 * 3 + 1))
            .map(|i| i % 9 + 1)
            .collect();
        check_roundtrip(&near_sample);
    }

    #[test]
    fn constant_and_zero_chunks_use_single_run_rle() {
        // All-equal and all-zero have no dedicated codec — they are the smallest single-run Rle.
        for degs in [vec![0u32; 5000], vec![3u32; 5000]] {
            let bytes = encode_chunk(&degs, &opts()).unwrap();
            assert_eq!(bytes[0], ChunkKind::Rle as u8);
            let chunk = decode_chunk(&bytes).unwrap();
            assert!(matches!(chunk, DegreeChunk::Rle(_)));
            for (i, &d) in degs.iter().enumerate() {
                assert_eq!(chunk.degree_at(i), Some(d));
            }
        }
    }

    #[test]
    fn empty_chunk_roundtrips_as_rle() {
        let bytes = encode_chunk(&[], &opts()).unwrap();
        assert_eq!(bytes[0], ChunkKind::Rle as u8);
        let chunk = decode_chunk(&bytes).unwrap();
        assert_eq!(chunk.len(), 0);
        assert!(chunk.is_empty());
        assert_eq!(chunk.degree_at(0), None);
        assert_eq!(chunk.to_degrees(), Vec::<u32>::new());
    }

    #[test]
    fn skewed_chunk_beats_dense_and_avoids_raw() {
        // A heterogeneous chunk must encode smaller than the dense u32 form and never fall
        // back to the RawU32 escape hatch (it lands on EF or, if very compressible, zstd).
        let periodic: Vec<u32> = (0..8192u32).map(|i| (i % 13) + (i % 2) * 50).collect();
        // High-entropy skew where EF specifically wins over zstd.
        let hashed: Vec<u32> = (0..8192u32)
            .map(|i| (i.wrapping_mul(0x9E37_79B1) ^ (i << 3)) % 500)
            .collect();
        for d in [&periodic, &hashed] {
            let bytes = encode_chunk(d, &opts()).unwrap();
            assert!(
                bytes.len() < 1 + d.len() * 4,
                "encoded {} should beat dense {}",
                bytes.len(),
                1 + d.len() * 4
            );
            assert_ne!(
                bytes[0],
                ChunkKind::RawU32 as u8,
                "should not pick raw dense"
            );
        }
        // The genuinely high-entropy chunk lands on EF.
        assert_eq!(
            encode_chunk(&hashed, &opts()).unwrap()[0],
            ChunkKind::Ef as u8
        );
    }

    /// Opt-in measurement (not an assertion of a hard bound): reports the on-disk size of
    /// a realistic skewed degree column under EF-raw vs a dense-`u32`+zstd baseline, plus
    /// the per-chunk codec mix. Run:
    /// `cargo test -p graph-format --lib degree_ef::tests::measure_ -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement, prints sizes; run with --ignored --nocapture"]
    fn measure_ef_vs_dense_zstd_on_skewed_column() {
        use crate::nodedegree::DEGREES_PER_RECORD;
        // LDG clusters by graph proximity, which correlates degree — so a chunk is roughly
        // homogeneous in degree regime. Model that: chunks 0-2 are the low-degree leaf tail,
        // chunk 3 is a mid-degree body, chunk 4 is a hub region. Degrees vary continuously
        // within a regime (few exact repeats in the body/hub), which is where EF wins and a
        // dense+zstd blob does not.
        let n = 5 * DEGREES_PER_RECORD + 1234;
        let degs: Vec<u32> = (0..n as u64)
            .map(|i| {
                let chunk = i as usize / DEGREES_PER_RECORD;
                let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 11;
                match chunk {
                    0..=2 => (h % 4) as u32,          // leaf tail: 0..3 (repeats)
                    3 => (h % 600) as u32,            // body: spread 0..599
                    _ => 2000 + (h % 500_000) as u32, // hubs: wide 2000..502000
                }
            })
            .collect();

        let mut disk = [0usize; 4]; // bytes per disk codec
        let mut kinds = [0usize; 4];
        let mut ef_disk_total = 0usize;
        let mut resident_total = 0usize;
        for c in degs.chunks(DEGREES_PER_RECORD) {
            let enc = encode_chunk(c, &opts()).unwrap();
            ef_disk_total += enc.len();
            let k = enc[0] as usize;
            kinds[k] += 1;
            disk[k] += enc.len();
            resident_total += decode_chunk(&enc).unwrap().resident_bytes();
        }

        // Dense-u32 + zstd baseline (the pre-change encoding): one zstd blob per chunk on
        // disk, but a full 1 MiB dense u32 array *resident* per chunk.
        let dense_zstd: usize = degs
            .chunks(DEGREES_PER_RECORD)
            .map(|c| {
                let mut raw = Vec::with_capacity(c.len() * 4);
                for &d in c {
                    raw.extend_from_slice(&d.to_le_bytes());
                }
                codec::compress(&raw, 3).unwrap().len()
            })
            .sum();
        let dense_resident = degs.len() * 4;

        eprintln!(
            "nodes={n} chunks={}",
            degs.len().div_ceil(DEGREES_PER_RECORD)
        );
        // Pure-EF-on-disk (never pick zstd): every fault is then decompress-free.
        let ef_only_disk: usize = degs
            .chunks(DEGREES_PER_RECORD)
            .map(|c| {
                if c.iter().all(|&d| d == c[0]) {
                    9
                } else {
                    1 + EfChunk::encode(c).serialized_len()
                }
            })
            .sum();

        eprintln!("  -- disk --");
        eprintln!("  dense u32 + zstd (today)      : {:>10} B", dense_zstd);
        eprintln!(
            "  chosen (zstd penalised)        : {:>10} B  ({:.2}x vs dense+zstd)",
            ef_disk_total,
            dense_zstd as f64 / ef_disk_total as f64,
        );
        eprintln!(
            "  EF-only lower bound            : {:>10} B  ({:.2}x vs dense+zstd)",
            ef_only_disk,
            dense_zstd as f64 / ef_only_disk as f64,
        );
        eprintln!("  -- resident --");
        eprintln!("  dense u32 (today)          : {:>10} B", dense_resident);
        eprintln!(
            "  EF/Rle (this change)       : {:>10} B  ({:.2}x smaller)",
            resident_total,
            dense_resident as f64 / resident_total as f64,
        );
        let decompress_free = kinds[0] + kinds[1] + kinds[2];
        eprintln!(
            "  codec mix: constant={} ef={} raw={} zstd={}",
            kinds[0], kinds[1], kinds[2], kinds[3]
        );
        eprintln!(
            "  faults: {}/{} chunks decompress-free (EF/constant/raw); {} zstd chunks decompress+re-encode (≤ today's cost)",
            decompress_free,
            kinds.iter().sum::<usize>(),
            kinds[3],
        );
    }

    #[test]
    fn rle_roundtrips_and_is_chosen_for_segmented_runs() {
        // Consecutive equal-degree runs (bulk-imported / sorted-degree region): RLE should be
        // chosen (decompress-free) and round-trip exactly.
        let mut segmented: Vec<u32> = vec![0u32; 3000]; // isolated tail
        segmented.extend(std::iter::repeat_n(3, 2000)); // bulk import, degree 3
        segmented.extend(std::iter::repeat_n(1, 1500));
        segmented.extend(std::iter::repeat_n(7, 500));
        let bytes = encode_chunk(&segmented, &opts()).unwrap();
        assert_eq!(
            bytes[0],
            ChunkKind::Rle as u8,
            "segmented runs should pick RLE"
        );
        let chunk = decode_chunk(&bytes).unwrap();
        assert!(
            matches!(chunk, DegreeChunk::Rle(_)),
            "RLE stays run-structured resident"
        );
        assert_eq!(chunk.len(), segmented.len());
        for (i, &d) in segmented.iter().enumerate() {
            assert_eq!(chunk.degree_at(i), Some(d), "slot {i}");
        }
        assert_eq!(chunk.degree_at(segmented.len()), None);
        assert_eq!(&chunk.to_degrees(), &segmented);
        // Run boundaries specifically (last id of each run and first of the next).
        for &b in &[2999usize, 3000, 4999, 5000, 6499, 6500, 6999] {
            assert_eq!(chunk.degree_at(b), Some(segmented[b]), "boundary {b}");
        }
    }

    #[test]
    fn rle_not_chosen_for_high_entropy() {
        // No consecutive runs → RLE would be one run per element (huge), so EF wins.
        let hashed: Vec<u32> = (0..8192u32)
            .map(|i| (i.wrapping_mul(0x9E37_79B1) ^ (i << 5)) % 400)
            .collect();
        assert_eq!(
            encode_chunk(&hashed, &opts()).unwrap()[0],
            ChunkKind::Ef as u8
        );
    }

    #[test]
    fn rle_rejects_corrupt_run_sums() {
        // A body whose run lengths don't sum to n must be rejected, not silently mis-decode.
        let good = encode_chunk(&[5u32, 5, 5, 2, 2], &opts()).unwrap();
        assert_eq!(good[0], ChunkKind::Rle as u8);
        assert!(decode_chunk(&good).is_ok());
        // Truncating the last run's length byte region corrupts the sum → error.
        let mut bad = good.clone();
        bad.pop();
        assert!(decode_chunk(&bad).is_err());
    }

    /// An [`EfChunk`] body for `n` degrees at low-bits width `l`, made deliberately
    /// **self-consistent**: the low plane is sized to `(m·l)/8` bytes and the high bitmap holds
    /// exactly `m = n+1` one-bits, so the decoder's `body.len() != need` check and its
    /// `ones == m` invariant both pass and `l` is the only thing left to catch.
    fn forged_ef_chunk_body(n: u32, l: u8) -> Vec<u8> {
        let m = n as usize + 1;
        let nwords = m.div_ceil(64).max(1);
        let mut highs = vec![0u64; nwords];
        for i in 0..m {
            highs[i / 64] |= 1u64 << (i % 64);
        }
        let low_bytes = (m * l as usize).div_ceil(8);
        let mut body = Vec::new();
        body.extend_from_slice(&n.to_le_bytes());
        body.push(l);
        body.extend_from_slice(&(nwords as u32).to_le_bytes());
        body.resize(body.len() + low_bytes, 0);
        for w in &highs {
            body.extend_from_slice(&w.to_le_bytes());
        }
        body
    }

    /// A forged degree chunk whose low-bits width `ℓ` is outside `0..=63` must be rejected at
    /// decode, cleanly.
    ///
    /// Without the bound the record decodes *successfully* and `degree_at` then evaluates
    /// `hi << 100`. Debug panics; **release masks it to `100 & 63 = 36`, colliding the high and
    /// low bits, so `v1 - v0` yields a wrong degree and the degree-sum count fast path returns
    /// a wrong k-hop count with no error.** That silent case is the one that matters, so this
    /// must hold under `cargo test --release` too.
    #[test]
    fn rejects_forged_ef_low_bits_width() {
        let forged = forged_ef_chunk_body(8, 100);
        let err = EfChunk::deserialize(&forged)
            .map(|_| ())
            .expect_err("l=100 must error, not mis-decode");
        assert!(
            matches!(
                err.downcast_ref::<DecodeRejected>(),
                Some(DecodeRejected::EfLowBitsWidth { l: 100, .. })
            ),
            "expected a typed EfLowBitsWidth rejection, got: {err}"
        );

        // Same through the public record path, tag included.
        let mut rec = vec![ChunkKind::Ef as u8];
        rec.extend_from_slice(&forged);
        assert!(
            decode_chunk(&rec).is_err(),
            "l=100 must error via decode_chunk"
        );

        // 64 is the first rejected width — `1u64 << 64` overflows exactly as 100 does, so a
        // `l <= 64` bound would let it through.
        assert!(EfChunk::deserialize(&forged_ef_chunk_body(8, 64)).is_err());

        // The bound must not reject a legal width. A chunk of `u32` degrees can't reach ℓ=63,
        // but ℓ=0 (the all-zero / all-small cumulative sum) is routine and must decode.
        let narrow = EfChunk::encode(&[0, 1, 0, 2]);
        assert_eq!(narrow.l, 0);
        let rt = EfChunk::deserialize(&narrow.serialize()).expect("ℓ=0 is legal and must decode");
        assert_eq!(
            (0..4).map(|s| rt.degree_at(s)).collect::<Vec<_>>(),
            vec![0, 1, 0, 2]
        );
    }

    /// Wire-biased opts (`margin = 1.0`): always prices zstd, lets it win on any size gain.
    fn wire_opts() -> DegreeCodecOpts {
        DegreeCodecOpts {
            zstd_level: 9,
            zstd_margin: 1.0,
        }
    }

    #[test]
    fn zstd_wins_only_when_wire_biased() {
        // Interleaved (non-consecutive) repetition: RLE can't help (one run per element) and
        // EF pays its per-node floor, but zstd crushes the periodicity.
        let crushable: Vec<u32> = (0..20_000u32).map(|i| 2 + (i % 2)).collect();
        // Wire-biased: zstd is priced and wins.
        let wb = encode_chunk(&crushable, &wire_opts()).unwrap();
        assert_eq!(
            wb[0],
            ChunkKind::ZstdDense as u8,
            "wire-biased build should pick zstd"
        );
        // Latency-biased default: EF already compresses this ≥4× vs dense, so zstd is not even
        // priced — the fault stays decompress-free, EF is chosen.
        let lb = encode_chunk(&crushable, &opts()).unwrap();
        assert_eq!(
            lb[0],
            ChunkKind::Ef as u8,
            "latency-biased build keeps EF (decompress-free)"
        );
        // Both round-trip.
        for (i, &c) in crushable.iter().enumerate() {
            assert_eq!(decode_chunk(&wb).unwrap().degree_at(i), Some(c));
            assert_eq!(decode_chunk(&lb).unwrap().degree_at(i), Some(c));
        }
    }

    #[test]
    fn high_entropy_stays_ef_even_wire_biased() {
        // No structure any codec beats EF at: even a wire-biased build keeps EF.
        let hi_entropy: Vec<u32> = (0..8192u32)
            .map(|i| (i.wrapping_mul(0x9E37_79B1) ^ (i << 5)) % 400)
            .collect();
        assert_eq!(
            encode_chunk(&hi_entropy, &opts()).unwrap()[0],
            ChunkKind::Ef as u8
        );
        assert_eq!(
            encode_chunk(&hi_entropy, &wire_opts()).unwrap()[0],
            ChunkKind::Ef as u8
        );
    }

    #[test]
    fn zstd_dense_decodes_to_compact_resident() {
        // Interleaved repetition that zstd crushes (wire-biased so it's chosen): whatever the
        // disk tag, the resident form must be compact (Ef/Rle), never a raw dense array.
        let d: Vec<u32> = (0..6000u32).map(|i| 2 + (i % 2)).collect();
        let bytes = encode_chunk(
            &d,
            &DegreeCodecOpts {
                zstd_level: 19,
                zstd_margin: 1.0,
            },
        )
        .unwrap();
        assert_eq!(bytes[0], ChunkKind::ZstdDense as u8);
        let chunk = decode_chunk(&bytes).unwrap();
        assert!(
            !matches!(chunk, DegreeChunk::Rle(_)) || chunk.resident_bytes() < d.len() * 4,
            "resident form stays compact, not dense"
        );
        for (i, &v) in d.iter().enumerate() {
            assert_eq!(chunk.degree_at(i), Some(v), "slot {i}");
        }
    }
}
