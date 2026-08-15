// SPDX-License-Identifier: Apache-2.0
//! `builtins` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Phase 1b — non-deterministic builtins (rand / randomUUID / timestamp) ──
#[test]
fn phase1b_nondeterministic_functions() {
    let (root, res) = run(
        "exec_p1b_fns",
        "RETURN rand() AS r, randomUUID() AS u, timestamp() AS t",
    );
    let r = &res.rows[0];
    match r[0] {
        Val::Float(x) => assert!((0.0..1.0).contains(&x), "rand() in [0,1): {x}"),
        ref o => panic!("rand() → {o:?}"),
    }
    match &r[1] {
        // RFC-4122 v4: 36 chars, 4 hyphens, version nibble '4'.
        Val::Str(s) => {
            assert_eq!(s.len(), 36, "uuid {s}");
            assert_eq!(s.matches('-').count(), 4, "uuid {s}");
            assert_eq!(s.as_bytes()[14], b'4', "v4 version nibble: {s}");
        }
        o => panic!("randomUUID() → {o:?}"),
    }
    match r[2] {
        // Milliseconds since the epoch — well past 2020 (1.6e12 ms).
        Val::Int(t) => assert!(t > 1_600_000_000_000, "timestamp() ms: {t}"),
        ref o => panic!("timestamp() → {o:?}"),
    }

    // Two randomUUID() calls in one row are distinct.
    let (root2, res2) = run(
        "exec_p1b_uuid2",
        "RETURN randomUUID() AS a, randomUUID() AS b",
    );
    let r = &res2.rows[0];
    assert_ne!(render(&r[0]), render(&r[1]), "two UUIDs differ");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Regression (HIK-74): `rand()` must cover the whole of `[0, 1)`, not a
/// sliver of it. The old implementation sliced the *low* 64 bits of a v4
/// UUID, whose two most-significant bits are the fixed RFC-4122 variant
/// (`10`), so every draw landed in `[0.5, 0.75)` — `WHERE rand() < 0.1` could
/// never match, and `ORDER BY rand()` shuffled over a quarter of the range.
///
/// The bounds below are deliberately loose: with a correct uniform generator
/// and `N = 20_000` draws, every assertion here fails with probability far
/// below 1e-9 (an empty octile alone is `(7/8)^20000 ≈ 1e-1160`), so this is
/// a distribution test that cannot realistically flake in CI.
#[test]
fn rand_is_uniform_over_unit_interval() {
    const N: usize = 20_000;
    const BUCKETS: usize = 8;

    let mut hist = [0usize; BUCKETS];
    let mut sum = 0.0f64;
    let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);

    for _ in 0..N {
        let x = random_f64();
        // Hard invariant: the contract is [0, 1), so 1.0 and NaN are bugs.
        assert!(
            (0.0..1.0).contains(&x),
            "rand() escaped [0, 1): {x} (NaN? {})",
            x.is_nan()
        );
        hist[(x * BUCKETS as f64) as usize] += 1;
        sum += x;
        min = min.min(x);
        max = max.max(x);
    }

    // Every octile is hit. This is the assertion the pre-fix code failed:
    // it only ever populated bucket 4 ([0.5, 0.625)).
    for (i, &count) in hist.iter().enumerate() {
        assert!(
                count > 0,
                "octile {i} ([{:.3}, {:.3})) never drawn in {N} samples — rand() is not uniform: {hist:?}",
                i as f64 / BUCKETS as f64,
                (i + 1) as f64 / BUCKETS as f64,
            );
    }

    // The tails are reached, and the mean sits where a uniform mean should.
    // (σ of the mean of N uniforms is 1/√(12N) ≈ 0.002, so ±0.02 is ~10σ.)
    assert!(
        min < 0.05,
        "min draw {min} — low tail unreachable: {hist:?}"
    );
    assert!(
        max > 0.95,
        "max draw {max} — high tail unreachable: {hist:?}"
    );
    let mean = sum / N as f64;
    assert!(
        (0.48..0.52).contains(&mean),
        "mean of {N} draws is {mean}, expected ≈ 0.5: {hist:?}"
    );
}
