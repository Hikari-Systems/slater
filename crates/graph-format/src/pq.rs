// SPDX-License-Identifier: Apache-2.0
//! Product quantisation (PQ) codebooks + codes for the large-vector ANN path.
//!
//! A PQ index splits each `dim`-dimensional vector into `m` contiguous
//! sub-vectors of length `dsub = dim / m`, and quantises each sub-vector against a
//! per-subspace codebook of `k = 2^bits` centroids trained by k-means. A vector is
//! then stored as `m` small codes (one centroid id per subspace, `bits ≤ 8` ⇒ one
//! byte each), so a 1024-dim f32 vector (4 KiB) compresses to `m` bytes
//! (~16–128 B). Those codes are what the beam search holds **resident** (the
//! `// DESIGN:` of the whole milestone — never a full in-memory graph), navigating
//! by a PQ-*estimated* distance computed from a small per-query lookup table.
//!
//! The estimate is **asymmetric distance computation** (ADC): the query stays
//! full-precision; for each subspace we precompute the squared-L2 distance from the
//! query sub-vector to every centroid (`AdcTable`), then a candidate's estimated
//! distance is the sum of `m` table look-ups keyed by its codes. ADC is the
//! standard, more accurate PQ estimator (the query is never quantised).
//!
// DESIGN (D29): for a **cosine** index every vector is L2-normalised before
// training/encoding, and the PQ estimate is squared-L2 in that normalised space.
// On unit vectors squared-L2 is `2 − 2·cos`, i.e. monotonic in cosine distance, so
// navigating by PQ-estimated squared-L2 ranks candidates identically to cosine —
// while the final re-rank still uses the *exact* cosine distance on the full
// vectors. Training on normalised vectors keeps the codebooks in the same space the
// estimate works in. Callers pass already-normalised vectors; this module does the
// quantisation maths only.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::blockfile::{parse_block, BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::wire::{capacity_for, capacity_hint, checked_span, read_uvarint, write_uvarint};

/// PQ structural parameters, recorded so the store is self-describing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PqParams {
    pub dim: u32,
    /// Number of subspaces (`m`). Must divide `dim`.
    pub subspaces: u32,
    /// Sub-vector length (`dim / m`).
    pub dsub: u32,
    /// Centroids per subspace (`k = 2^bits`).
    pub k: u32,
}

impl PqParams {
    pub fn new(dim: u32, subspaces: u32, bits: u32) -> Result<Self> {
        if subspaces == 0 || dim == 0 {
            bail!("PQ requires non-zero dim and subspaces");
        }
        if !dim.is_multiple_of(subspaces) {
            bail!("PQ subspaces ({subspaces}) must divide dim ({dim})");
        }
        if !(1..=8).contains(&bits) {
            bail!("PQ bits ({bits}) must be in 1..=8 (codes are stored one byte each)");
        }
        Ok(Self {
            dim,
            subspaces,
            dsub: dim / subspaces,
            k: 1u32 << bits,
        })
    }
}

/// A trained codebook: `subspaces × k × dsub` centroids, stored flat. The
/// centroid `c` of subspace `s` is `centroids[(s*k + c)*dsub .. +dsub]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Codebook {
    pub params: PqParams,
    pub centroids: Vec<f32>,
}

impl Codebook {
    fn centroid(&self, s: usize, c: usize) -> &[f32] {
        let dsub = self.params.dsub as usize;
        let k = self.params.k as usize;
        let base = (s * k + c) * dsub;
        &self.centroids[base..base + dsub]
    }

    /// Encode one full vector into its `m` subspace codes (one byte per subspace).
    /// The vector must be `dim`-long and is expected already normalised for a
    /// cosine index (D29).
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if vector.len() != self.params.dim as usize {
            bail!(
                "cannot encode dim {} vector with a dim {} codebook",
                vector.len(),
                self.params.dim
            );
        }
        let m = self.params.subspaces as usize;
        let dsub = self.params.dsub as usize;
        let k = self.params.k as usize;
        let mut codes = Vec::with_capacity(m);
        for s in 0..m {
            let sub = &vector[s * dsub..(s + 1) * dsub];
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for c in 0..k {
                let d = sq_l2(sub, self.centroid(s, c));
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            codes.push(best as u8);
        }
        Ok(codes)
    }
}

/// Squared-L2 distance between two equal-length slices (f64 accumulation).
/// `pub(crate)` so the Vamana builder shares one definition.
pub(crate) fn sq_l2(a: &[f32], b: &[f32]) -> f64 {
    let mut acc = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let d = *x as f64 - *y as f64;
        acc += d * d;
    }
    acc
}

/// Per-query ADC lookup table: `table[s*k + c]` is the squared-L2 distance from the
/// query's `s`-th sub-vector to subspace `s`'s centroid `c`. Estimating a
/// candidate's distance is then `m` adds — no access to the candidate's full vector.
pub struct AdcTable {
    table: Vec<f32>,
    m: usize,
    k: usize,
}

impl AdcTable {
    /// Build the table for `query` (which must already be normalised for a cosine
    /// index, matching how the codebook was trained — D29).
    pub fn new(codebook: &Codebook, query: &[f32]) -> Result<Self> {
        if query.len() != codebook.params.dim as usize {
            bail!(
                "query dim {} does not match codebook dim {}",
                query.len(),
                codebook.params.dim
            );
        }
        let m = codebook.params.subspaces as usize;
        let dsub = codebook.params.dsub as usize;
        let k = codebook.params.k as usize;
        let mut table = vec![0.0f32; m * k];
        for s in 0..m {
            let sub = &query[s * dsub..(s + 1) * dsub];
            for c in 0..k {
                table[s * k + c] = sq_l2(sub, codebook.centroid(s, c)) as f32;
            }
        }
        Ok(Self { table, m, k })
    }

    /// Estimated squared-L2 distance of the vector with these `m` codes.
    pub fn estimate(&self, codes: &[u8]) -> f32 {
        debug_assert_eq!(codes.len(), self.m);
        let mut acc = 0.0f32;
        for (s, &c) in codes.iter().enumerate() {
            acc += self.table[s * self.k + c as usize];
        }
        acc
    }
}

// ── k-means training ──────────────────────────────────────────────────────────

/// A tiny deterministic LCG (Numerical Recipes constants). The build must be
/// reproducible — same vectors in ⇒ same codebooks out — so k-means init uses this
/// rather than a system RNG, and there is no `rand` dependency in the tree.
/// `pub(crate)` so the Vamana builder shares one definition.
pub(crate) struct Lcg(pub(crate) u64);
impl Lcg {
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    /// A float in `[0, 1)`.
    pub(crate) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub(crate) fn next_below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Train PQ codebooks over `vectors` (each `dim`-long, expected already normalised
/// for a cosine index). `iters` Lloyd iterations per subspace. Deterministic.
pub fn train_codebooks(vectors: &[Vec<f32>], params: PqParams, iters: usize) -> Result<Codebook> {
    let m = params.subspaces as usize;
    let dsub = params.dsub as usize;
    let k = params.k as usize;
    for (i, v) in vectors.iter().enumerate() {
        if v.len() != params.dim as usize {
            bail!(
                "training vector {i} has dim {} but codebook dim is {}",
                v.len(),
                params.dim
            );
        }
    }
    let mut centroids = vec![0.0f32; m * k * dsub];
    // One LCG for the whole training run; seeded by a constant so the codebooks are
    // reproducible across builds of the same data.
    let mut rng = Lcg(0x5111_a7e1_5eed_1234);

    for s in 0..m {
        // Gather this subspace's sub-vectors as references (no copy).
        let subs: Vec<&[f32]> = vectors
            .iter()
            .map(|v| &v[s * dsub..(s + 1) * dsub])
            .collect();
        let cents = kmeans(&subs, k, dsub, iters, &mut rng);
        for (c, cent) in cents.iter().enumerate() {
            let base = (s * k + c) * dsub;
            centroids[base..base + dsub].copy_from_slice(cent);
        }
    }
    Ok(Codebook { params, centroids })
}

/// k-means++ initialisation + Lloyd iterations over `points` (each `dsub`-long).
/// Returns exactly `k` centroids; empty clusters are reseeded to a random point so
/// the codebook is always fully populated.
fn kmeans(points: &[&[f32]], k: usize, dsub: usize, iters: usize, rng: &mut Lcg) -> Vec<Vec<f32>> {
    let n = points.len();
    if n == 0 {
        return vec![vec![0.0f32; dsub]; k];
    }
    // k-means++ seeding: first centroid random, each subsequent chosen with
    // probability proportional to squared distance from the nearest chosen one.
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(points[rng.next_below(n)].to_vec());
    let mut nearest = vec![f64::INFINITY; n];
    while centroids.len() < k {
        let last = centroids.last().unwrap();
        let mut total = 0.0f64;
        for (i, p) in points.iter().enumerate() {
            let d = sq_l2(p, last);
            if d < nearest[i] {
                nearest[i] = d;
            }
            total += nearest[i];
        }
        if total <= 0.0 {
            // All remaining points coincide with a centroid — pad with copies.
            centroids.push(points[rng.next_below(n)].to_vec());
            continue;
        }
        let mut target = rng.next_f64() * total;
        let mut chosen = n - 1;
        for (i, &d) in nearest.iter().enumerate() {
            target -= d;
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        centroids.push(points[chosen].to_vec());
    }

    // Lloyd iterations.
    let mut assign = vec![0usize; n];
    for _ in 0..iters.max(1) {
        let mut changed = false;
        for (i, p) in points.iter().enumerate() {
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for (c, cent) in centroids.iter().enumerate() {
                let d = sq_l2(p, cent);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if assign[i] != best {
                assign[i] = best;
                changed = true;
            }
        }
        // Recompute centroids as cluster means.
        let mut sums = vec![vec![0.0f64; dsub]; k];
        let mut counts = vec![0usize; k];
        for (i, p) in points.iter().enumerate() {
            let c = assign[i];
            counts[c] += 1;
            for (acc, &x) in sums[c].iter_mut().zip(p.iter()) {
                *acc += x as f64;
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                // Reseed an empty cluster to a random point so it stays useful.
                centroids[c] = points[rng.next_below(n)].to_vec();
            } else {
                for (d, acc) in centroids[c].iter_mut().zip(&sums[c]) {
                    *d = (*acc / counts[c] as f64) as f32;
                }
            }
        }
        if !changed {
            break;
        }
    }
    centroids
}

// ── `.pq` store (codebook + per-vector codes), block-file backed ───────────────
//
// Record 0 is the header+codebook; records 1..=count are one code record per
// vector (`uvarint(node_id) ‖ m × u8`). The file goes through the same blockfile
// seam as every other store, so it inherits zstd + the M6 AEAD for free (D28).

/// Writer for `vector/<l>.<p>.pq`.
pub struct PqWriter {
    inner: BlockFileWriter,
    m: usize,
}

impl PqWriter {
    /// Create the store and write the codebook header as record 0.
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        codebook: &Codebook,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let mut inner =
            BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
        let mut hdr = Vec::new();
        let p = codebook.params;
        write_uvarint(&mut hdr, p.dim as u64);
        write_uvarint(&mut hdr, p.subspaces as u64);
        write_uvarint(&mut hdr, p.dsub as u64);
        write_uvarint(&mut hdr, p.k as u64);
        for x in &codebook.centroids {
            hdr.write_f32::<LittleEndian>(*x)?;
        }
        inner.append_record(&hdr)?;
        Ok(Self {
            inner,
            m: p.subspaces as usize,
        })
    }

    /// Append one vector's codes (in vamana-index / layout order).
    pub fn append_codes(&mut self, node_id: u64, codes: &[u8]) -> Result<()> {
        if codes.len() != self.m {
            bail!("expected {} codes, got {}", self.m, codes.len());
        }
        let mut rec = Vec::with_capacity(10 + self.m);
        write_uvarint(&mut rec, node_id);
        rec.extend_from_slice(codes);
        self.inner.append_record(&rec)?;
        Ok(())
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// All of a PQ index's codes held **resident** — the navigation set for the beam
/// search. `node_ids[i]` is the dense graph node for vamana index `i`, and its
/// codes are `codes[i*m .. i*m+m]`.
#[derive(Debug, Clone)]
pub struct ResidentPq {
    pub codebook: Codebook,
    pub node_ids: Vec<u64>,
    pub codes: Vec<u8>,
    pub m: usize,
}

impl ResidentPq {
    /// The `m` codes for vamana index `i`.
    pub fn codes_of(&self, i: usize) -> &[u8] {
        &self.codes[i * self.m..i * self.m + self.m]
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.node_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }

    /// Approximate resident footprint in bytes (codes + node-id table + codebook).
    pub fn resident_bytes(&self) -> usize {
        self.codes.len() + self.node_ids.len() * 8 + self.codebook.centroids.len() * 4
    }
}

/// Reader for `vector/<l>.<p>.pq`.
pub struct PqReader {
    inner: BlockFileReader,
    codebook: Codebook,
}

impl PqReader {
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(crate::store::fs::FileObject::open(path)?);
        Self::open_src(src, cipher)
    }

    /// Open from any positional-read source (local file or remote object).
    pub fn open_src(
        src: Arc<dyn crate::store::RandomReadAt>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let inner = BlockFileReader::open_src(src, cipher)?;
        let hdr = inner
            .read_record_global(0)
            .context("read PQ codebook header (record 0)")?;
        let mut r = &hdr[..];
        let dim = read_uvarint(&mut r)? as u32;
        let subspaces = read_uvarint(&mut r)? as u32;
        let dsub = read_uvarint(&mut r)? as u32;
        let k = read_uvarint(&mut r)? as u32;
        // The four header fields are untrusted on-disk uvarints, and the codebook size is
        // their *product*: `subspaces * k * dsub` was computed in `u32`, so it wrapped — a
        // forged header could name a small `n` and a `dim`/`k` the rest of the reader then
        // used at full width, or (in a debug build) simply panic on the overflow.
        //
        // Re-derive the params through the constructor that the writer used, so the invariants
        // that make the product meaningful (`dsub == dim / subspaces`, `k = 2^bits` with
        // `bits` in `1..=8`) are re-checked against the image rather than assumed.
        if !k.is_power_of_two() {
            bail!("PQ codebook header: k ({k}) is not a power of two");
        }
        let params = PqParams::new(dim, subspaces, k.trailing_zeros())
            .context("PQ codebook header failed validation")?;
        if params.dsub != dsub || params.k != k {
            bail!(
                "PQ codebook header is inconsistent: dsub={dsub}, k={k}, but dim={dim} over \
                 {subspaces} subspaces implies dsub={}, k={}",
                params.dsub,
                params.k
            );
        }
        // Even validated, `dim` is an unbounded `u32`, so the product still needs a checked
        // multiply and the reservation still needs clamping by the bytes actually present
        // (4 per `f32`) — the loop below errors on the first short read.
        let n = checked_span("PQ codebook", subspaces as u64 * k as u64, dsub as usize)?;
        let mut centroids = Vec::with_capacity(capacity_for(n, r.len(), 4));
        for _ in 0..n {
            centroids.push(r.read_f32::<LittleEndian>()?);
        }
        Ok(Self {
            inner,
            codebook: Codebook { params, centroids },
        })
    }

    pub fn params(&self) -> PqParams {
        self.codebook.params
    }

    /// Load every code record (records `1..total`) into one resident structure.
    /// Reads block-by-block so each block is decompressed exactly once.
    pub fn load_resident(&self) -> Result<ResidentPq> {
        let m = self.codebook.params.subspaces as usize;
        let total = self.inner.total_records();
        // `total` is the block directory's record count — an on-disk number, not a count
        // backed by a buffer we hold, and `n * m` would wrap. Reserve a bounded prefix and
        // let the `Vec`s grow as the blocks are actually read (`wire::capacity_hint`).
        let records = total.saturating_sub(1) as usize;
        let mut node_ids = Vec::with_capacity(capacity_hint(records));
        let mut codes = Vec::with_capacity(capacity_hint(records.saturating_mul(m)));
        let mut global: u64 = 0;
        // Whole-file load via a bounded concurrent read-ahead, so a remote backend
        // overlaps its fetch round-trips at generation open without holding more
        // than the read-ahead window resident (no-op fan-out on a local file).
        self.inner.for_each_block(|_bi, raw| {
            let (offsets, data) = parse_block(raw)?;
            for slot in 0..offsets.len().saturating_sub(1) {
                // Skip record 0 (the codebook header).
                if global == 0 {
                    global += 1;
                    continue;
                }
                let rec = &data[offsets[slot] as usize..offsets[slot + 1] as usize];
                let mut rr = rec;
                node_ids.push(read_uvarint(&mut rr)?);
                if rr.len() != m {
                    bail!("PQ code record has {} bytes, expected {m}", rr.len());
                }
                let mut buf = vec![0u8; m];
                rr.read_exact(&mut buf)?;
                codes.extend_from_slice(&buf);
                global += 1;
            }
            Ok(())
        })?;
        Ok(ResidentPq {
            codebook: self.codebook.clone(),
            node_ids,
            codes,
            m,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic synthetic clusters: `clusters` blobs of `per` points in
    /// `dim` dimensions, each blob centred at a distinct corner, lightly jittered.
    fn clustered(dim: usize, clusters: usize, per: usize) -> Vec<Vec<f32>> {
        let mut out = Vec::new();
        let mut seed = Lcg(0xABCD_1234);
        for _ in 0..clusters {
            let mut centre = vec![0.0f32; dim];
            for x in centre.iter_mut() {
                *x = seed.next_f64() as f32;
            }
            for _ in 0..per {
                let mut v = centre.clone();
                for x in v.iter_mut() {
                    *x += (seed.next_f64() as f32 - 0.5) * 0.01;
                }
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn params_validate_divisibility_and_bits() {
        assert!(PqParams::new(1024, 16, 8).is_ok());
        assert!(PqParams::new(10, 3, 8).is_err()); // 3 ∤ 10
        assert!(PqParams::new(8, 4, 9).is_err()); // bits > 8
        let p = PqParams::new(64, 8, 4).unwrap();
        assert_eq!((p.dsub, p.k), (8, 16));
    }

    #[test]
    fn encode_assigns_clustered_points_to_distinct_codes() {
        // With one subspace and exactly as many centroids as clusters (k=4,
        // bits=2), points from different clusters must land on different codes and
        // same-cluster points share a code.
        let dim = 8;
        let data = clustered(dim, 4, 25);
        let params = PqParams::new(dim as u32, 1, 2).unwrap();
        let cb = train_codebooks(&data, params, 25).unwrap();
        let c0 = cb.encode(&data[0]).unwrap()[0];
        let c1 = cb.encode(&data[25]).unwrap()[0]; // second cluster
        let c2 = cb.encode(&data[50]).unwrap()[0]; // third cluster
        assert_ne!(c0, c1);
        assert_ne!(c1, c2);
        assert_ne!(c0, c2);
        // A point in the same cluster shares its code.
        assert_eq!(cb.encode(&data[1]).unwrap()[0], c0);
    }

    #[test]
    fn adc_estimate_tracks_true_distance_ordering() {
        // The ADC estimate should rank candidates the same way the true squared-L2
        // distance does, on well-separated clusters.
        let dim = 16;
        let data = clustered(dim, 6, 20);
        let params = PqParams::new(dim as u32, 4, 4).unwrap();
        let cb = train_codebooks(&data, params, 30).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();

        let query = &data[0];
        let adc = AdcTable::new(&cb, query).unwrap();

        // The nearest candidate by ADC must be in the query's own cluster (the
        // first 20 points), and the estimate must be small there and large for a
        // far cluster.
        let near = adc.estimate(&codes[1]); // same cluster
        let far = adc.estimate(&codes[100]); // a distant cluster
        assert!(near < far, "near {near} should beat far {far}");

        // Argmin of ADC over all candidates is within the query's cluster.
        let best = (0..data.len())
            .min_by(|&a, &b| adc.estimate(&codes[a]).total_cmp(&adc.estimate(&codes[b])))
            .unwrap();
        assert!(
            best < 20,
            "ADC argmin {best} should be in the query cluster"
        );
    }

    #[test]
    fn pq_store_roundtrips_codebook_and_codes() {
        let dim = 16;
        let data = clustered(dim, 4, 10);
        let params = PqParams::new(dim as u32, 4, 4).unwrap();
        let cb = train_codebooks(&data, params, 20).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();

        let path = std::env::temp_dir().join(format!("slater_pq_{}_{}", std::process::id(), "rt"));
        let mut w = PqWriter::create_with_cipher(&path, &cb, 4096, 3, None).unwrap();
        for (i, c) in codes.iter().enumerate() {
            w.append_codes(i as u64, c).unwrap();
        }
        w.finish().unwrap();

        let r = PqReader::open_with_cipher(&path, None).unwrap();
        assert_eq!(r.params(), params);
        let resident = r.load_resident().unwrap();
        assert_eq!(resident.len(), data.len());
        for (i, code) in codes.iter().enumerate() {
            assert_eq!(resident.node_ids[i], i as u64);
            assert_eq!(resident.codes_of(i), code.as_slice());
        }
        // The codebook read back equals the one written.
        assert_eq!(resident.codebook, cb);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pq_store_roundtrips_under_encryption() {
        let dim = 8;
        let data = clustered(dim, 3, 8);
        let params = PqParams::new(dim as u32, 2, 4).unwrap();
        let cb = train_codebooks(&data, params, 15).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();
        let cipher = Arc::new(BlockCipher::from_master(b"pq-key", &[5u8; 32]));

        let path = std::env::temp_dir().join(format!("slater_pq_{}_{}", std::process::id(), "enc"));
        let mut w =
            PqWriter::create_with_cipher(&path, &cb, 4096, 3, Some(cipher.clone())).unwrap();
        for (i, c) in codes.iter().enumerate() {
            w.append_codes(i as u64, c).unwrap();
        }
        w.finish().unwrap();

        // Right key reads the codes; absent key is refused at open.
        let r = PqReader::open_with_cipher(&path, Some(cipher)).unwrap();
        let resident = r.load_resident().unwrap();
        assert_eq!(resident.codes_of(2), codes[2].as_slice());
        assert!(PqReader::open_with_cipher(&path, None).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
