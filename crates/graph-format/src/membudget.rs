// SPDX-License-Identifier: Apache-2.0
//! A memory accountant for the external builder.
//!
//! `--max-memory` used to be a number every consumer helped itself to a fraction
//! of: two posting sorters took `/16` each, every range index took another `/16`,
//! each band worker took `/16/threads`. Nothing held the whole number, so nothing
//! noticed when the fractions summed past it — a 4 GiB cap peaked at 8 GB resident.
//!
//! [`MemoryBudget`] is that missing arbiter: a counted semaphore over the cap,
//! handing out RAII [`Reservation`]s. A consumer states what it *wants* and the
//! smallest slice it can still make progress with (its `floor`); it is granted
//! `min(want, free)`, and the bytes go back to the budget when the reservation
//! drops. The sum of live reservations can therefore never exceed the cap.
//!
//! Two grant modes, and the difference is a liveness argument:
//!
//! * [`MemoryBudget::reserve_now`] never blocks. It fails when less than `floor`
//!   is free. Use it for **long-lived** reservations taken on one thread — a
//!   blocking wait there can only deadlock, because the only holders that could
//!   release are the caller's own earlier reservations.
//! * [`MemoryBudget::reserve`] blocks until `floor` bytes are free. It is only
//!   sound where some *other* live holder is guaranteed to release: a pool of
//!   workers drawing from a [sub-budget](Reservation::into_sub_budget), each
//!   holding one reservation for the length of one work item. A `floor` larger
//!   than the whole budget can never be satisfied, so it fails immediately rather
//!   than parking forever.
//!
//! **What it does not account.** A budget is only as honest as the estimates its
//! consumers hand it. `ExtSorter` reserves its run-formation buffer, and sizes that
//! buffer with [`SortRecord::resident_hint`](crate::extsort::SortRecord::resident_hint)
//! — *not* `size_hint`, which measures a record's encoded size and understated the
//! resident buffer by 3-4× until this was fixed. The k-way merge's one decompressed
//! block per run (`#runs × 256 KiB`) is still outside the budget; it is small when
//! buffers are large, which is the shape the reservations produce. Watch
//! `budget_reserved_bytes` against `rss_bytes` in a `--diagnostics` run: a phase whose
//! RSS climbs while its reservations stay flat is spending memory nobody reserved.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use anyhow::{bail, Result};

/// The smallest reservation an [`ExtSorter`](crate::extsort::ExtSorter) can form
/// runs with and still make progress. Below this the run count explodes and the
/// k-way merge's per-run resident block dominates whatever the buffer saved.
pub const MIN_SORT_BYTES: usize = 8 << 20;

/// Total bytes currently reserved across every live budget, for the build's
/// diagnostics sampler to emit alongside `rss_bytes`. Divergence between the two
/// is a consumer that is spending memory it never reserved.
static GLOBAL_RESERVED: AtomicU64 = AtomicU64::new(0);

/// Bytes reserved right now across all budgets in this process.
pub fn global_reserved_bytes() -> u64 {
    GLOBAL_RESERVED.load(Ordering::Relaxed)
}

/// A counted semaphore over a byte cap. Clone the `Arc`, not the budget.
pub struct MemoryBudget {
    total: usize,
    used: Mutex<usize>,
    cv: Condvar,
    /// A sub-budget holds its parent's reservation for its whole life, so the
    /// parent cannot re-lend those bytes. Dropped (released) with the sub-budget.
    _parent: Option<Reservation>,
}

impl MemoryBudget {
    /// A root budget of `total_bytes`. A zero cap is clamped to one byte so a
    /// misconfigured `--max-memory 0` fails at the first `floor` check with a
    /// budget error rather than dividing by zero somewhere downstream.
    pub fn new(total_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            total: total_bytes.max(1),
            used: Mutex::new(0),
            cv: Condvar::new(),
            _parent: None,
        })
    }

    pub fn total(&self) -> usize {
        self.total
    }

    pub fn reserved(&self) -> usize {
        *self.used.lock().unwrap()
    }

    pub fn available(&self) -> usize {
        self.total - *self.used.lock().unwrap()
    }

    /// Grant `min(want, free)` without blocking, or fail if less than `floor` is
    /// free. `what` names the consumer in the error.
    pub fn reserve_now(
        self: &Arc<Self>,
        what: &str,
        want: usize,
        floor: usize,
    ) -> Result<Reservation> {
        let mut used = self.used.lock().unwrap();
        let free = self.total - *used;
        if free < floor {
            bail!(
                "memory budget exhausted reserving for {what}: {} MiB free of a {} MiB cap, \
                 need at least {} MiB — raise --max-memory",
                free >> 20,
                self.total >> 20,
                floor >> 20,
            );
        }
        let grant = want.clamp(floor, free);
        *used += grant;
        drop(used);
        Ok(Reservation::new(Arc::clone(self), grant))
    }

    /// Grant `min(want, free)`, blocking until at least `floor` bytes are free.
    ///
    /// Only call this where another holder is guaranteed to release — see the
    /// module docs. A `floor` above the cap is unsatisfiable by any amount of
    /// waiting, so it fails loudly instead of parking.
    pub fn reserve(self: &Arc<Self>, what: &str, want: usize, floor: usize) -> Result<Reservation> {
        if floor > self.total {
            bail!(
                "memory budget too small for {what}: needs at least {} MiB but the whole \
                 cap is {} MiB — raise --max-memory",
                floor >> 20,
                self.total >> 20,
            );
        }
        let mut used = self.used.lock().unwrap();
        while self.total - *used < floor {
            used = self.cv.wait(used).unwrap();
        }
        let free = self.total - *used;
        let grant = want.clamp(floor, free);
        *used += grant;
        drop(used);
        Ok(Reservation::new(Arc::clone(self), grant))
    }

    fn release(&self, bytes: usize) {
        let mut used = self.used.lock().unwrap();
        *used -= bytes;
        drop(used);
        self.cv.notify_all();
    }
}

/// Bytes lent by a [`MemoryBudget`], returned to it on drop.
pub struct Reservation {
    budget: Arc<MemoryBudget>,
    bytes: usize,
    /// False once [`Reservation::into_sub_budget`] has re-lent these bytes to a
    /// nested budget, whose own grants are counted instead. Keeps
    /// [`global_reserved_bytes`] a sum over leaves rather than double-counting
    /// every byte at each level of nesting.
    counted: bool,
}

impl Reservation {
    fn new(budget: Arc<MemoryBudget>, bytes: usize) -> Self {
        GLOBAL_RESERVED.fetch_add(bytes as u64, Ordering::Relaxed);
        Self {
            budget,
            bytes,
            counted: true,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Carve `bytes` out of this reservation into a second one. The budget is not
    /// touched — the pair still sums to what was granted once.
    ///
    /// This is how a consumer that must hold **two** sorters at the same time stays
    /// deadlock-free inside a worker pool. Reserving twice would let every worker
    /// take its first slice, exhaust the pool, and then wait forever for a second
    /// slice that only a peer in the same predicament could release.
    pub fn split_off(&mut self, bytes: usize) -> Reservation {
        let taken = bytes.min(self.bytes.saturating_sub(1));
        self.bytes -= taken;
        Reservation {
            budget: Arc::clone(&self.budget),
            bytes: taken,
            counted: self.counted,
        }
    }

    /// Re-lend these bytes as a budget of their own — the pattern a worker pool
    /// uses: reserve the pool's whole share once from the parent (so no other
    /// phase can take it), then let workers contend for slices of it among
    /// themselves, where a blocking [`MemoryBudget::reserve`] is safe because
    /// every worker releases at the end of its work item.
    pub fn into_sub_budget(mut self) -> Arc<MemoryBudget> {
        GLOBAL_RESERVED.fetch_sub(self.bytes as u64, Ordering::Relaxed);
        self.counted = false;
        let bytes = self.bytes;
        Arc::new(MemoryBudget {
            total: bytes.max(1),
            used: Mutex::new(0),
            cv: Condvar::new(),
            _parent: Some(self),
        })
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Reservation({} bytes)", self.bytes)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.counted {
            GLOBAL_RESERVED.fetch_sub(self.bytes as u64, Ordering::Relaxed);
        }
        self.budget.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn grants_sum_to_at_most_the_cap_and_return_on_drop() {
        let b = MemoryBudget::new(1000);
        let a = b.reserve_now("a", 600, 1).unwrap();
        assert_eq!(a.bytes(), 600);
        // The second consumer wants 600 too but only 400 is left: it is capped,
        // not overcommitted. This is exactly the `/16`-fractions bug.
        let c = b.reserve_now("c", 600, 1).unwrap();
        assert_eq!(c.bytes(), 400);
        assert_eq!(b.reserved(), 1000);
        drop(a);
        assert_eq!(b.reserved(), 400);
        drop(c);
        assert_eq!(b.reserved(), 0);
    }

    #[test]
    fn reserve_now_fails_loudly_below_the_floor() {
        let b = MemoryBudget::new(1000);
        let _a = b.reserve_now("a", 900, 1).unwrap();
        let err = b.reserve_now("c", 500, 200).unwrap_err().to_string();
        assert!(err.contains("memory budget exhausted"), "{err}");
    }

    #[test]
    fn reserve_fails_rather_than_parking_when_the_floor_exceeds_the_cap() {
        // The starvation case: `--max-memory` so small no worker can ever run.
        // It must be an error, not a deadlock, so the build reports and exits.
        let b = MemoryBudget::new(1000);
        let err = b.reserve("worker", 2000, 2000).unwrap_err().to_string();
        assert!(err.contains("memory budget too small"), "{err}");
    }

    #[test]
    fn reserve_blocks_until_a_holder_releases() {
        let b = MemoryBudget::new(1000);
        let held = b.reserve_now("held", 800, 1).unwrap();
        let b2 = Arc::clone(&b);
        let t = std::thread::spawn(move || {
            // Needs 500 free; only 200 is. Parks until `held` drops.
            let r = b2.reserve("waiter", 500, 500).unwrap();
            assert!(r.bytes() >= 500);
        });
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(b.reserved(), 800, "waiter must not have been granted yet");
        drop(held);
        t.join().unwrap();
        assert_eq!(b.reserved(), 0);
    }

    #[test]
    fn split_off_divides_a_grant_without_touching_the_budget() {
        let b = MemoryBudget::new(1000);
        let mut a = b.reserve_now("a", 800, 1).unwrap();
        let c = a.split_off(300);
        assert_eq!((a.bytes(), c.bytes()), (500, 300));
        assert_eq!(b.reserved(), 800, "the split is invisible to the budget");
        drop(a);
        assert_eq!(b.reserved(), 300);
        drop(c);
        assert_eq!(
            b.reserved(),
            0,
            "both halves return their share exactly once"
        );
    }

    #[test]
    fn sub_budget_bounds_its_workers() {
        let b = MemoryBudget::new(1000);
        let pool = b.reserve_now("pool", 600, 1).unwrap().into_sub_budget();
        assert_eq!(b.reserved(), 600, "parent still lends the pool its bytes");

        let w1 = pool.reserve("w1", 400, 100).unwrap();
        let w2 = pool.reserve("w2", 400, 100).unwrap();
        assert_eq!(
            w1.bytes() + w2.bytes(),
            600,
            "workers share the pool, no more"
        );
        drop(w1);
        drop(w2);
        drop(pool);
        assert_eq!(b.reserved(), 0, "the pool returns its share to the parent");
    }
}
