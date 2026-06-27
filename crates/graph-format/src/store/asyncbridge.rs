// SPDX-License-Identifier: Apache-2.0
//! Shared sync→async bridge for the network object-store backends (S3, GCS).
//!
//! Both backends wrap an **async** SDK, but the [`ObjectStore`](super::ObjectStore)
//! trait is synchronous (the serve path runs under `spawn_blocking`). Each backend
//! owns a small dedicated `tokio` runtime and dispatches every operation onto it
//! via [`run_blocking`] — never `Runtime::block_on`, so the bridge is legal from
//! *any* thread, including a thread already inside another runtime (the server
//! opens graphs on its main runtime) and a `spawn_blocking` worker (query
//! execution). Read-ahead batches issue their range GETs concurrently inside one
//! `run_blocking` so the round-trips overlap.

use tokio::runtime::Runtime;

/// Wraps the backend runtime so its `Drop` is **non-blocking**. A tokio
/// [`Runtime`]'s default `Drop` performs a *blocking* shutdown, which panics
/// ("Cannot drop a runtime in a context where blocking is not allowed") if it
/// runs while the thread is already inside another runtime's async context.
/// That is exactly what happens when the server drops a partially-constructed
/// store on an error path — graph open, disk-cache open, checksum verify all
/// run on the main runtime, so a failure there unwinds and drops this store on
/// an async thread. `shutdown_background()` releases the runtime without
/// blocking, so the *real* open error surfaces instead of a masking panic.
pub struct BackgroundRuntime(Option<Runtime>);

impl BackgroundRuntime {
    /// Wrap an owned runtime so it shuts down in the background on drop.
    pub fn new(rt: Runtime) -> Self {
        Self(Some(rt))
    }
}

impl std::ops::Deref for BackgroundRuntime {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        // `Some` for the whole lifetime; only `Drop` takes it.
        self.0.as_ref().expect("backend runtime present until drop")
    }
}

impl Drop for BackgroundRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// Drive an async future to completion from a synchronous caller, **without**
/// `block_on`. The future is spawned onto the backend's own runtime (whose
/// worker threads drive it) and the caller blocks on a plain std channel for the
/// result. Unlike `Runtime::block_on`, this is legal from *any* thread —
/// including a thread already inside another tokio runtime (the server opens
/// graphs on its main runtime) and a `spawn_blocking` worker (query execution).
pub fn run_blocking<F, T>(rt: &Runtime, fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    rt.spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv()
        .expect("backend runtime dropped the task before returning a result")
}
