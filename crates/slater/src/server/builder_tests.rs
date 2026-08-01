// SPDX-License-Identifier: Apache-2.0
//! Supervision of the consolidation child — discovery, resource limits, output capture
//! and the timeout.
//!
//! These cover the *mechanics* `run_builder` composes, each of which was previously
//! untested because the only route to them was a real `slater-build` behind an
//! `#[ignore]`d, env-gated test. The end-to-end invocation is covered separately by the
//! real-builder consolidation suite.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{
    drain_pipe, finish, spawn_builder, wait_with_timeout, BuilderLimits, MIN_BUILDER_MEMORY,
};
use crate::config::DeltaConfig;

/// A unique scratch name so concurrently-running tests never collide on the directory
/// beside `current_exe()`, which is shared by every test in the binary.
fn unique(tag: &str) -> String {
    format!("slater-test-{tag}-{}", uuid::Uuid::new_v4())
}

/// The directory the test binary itself lives in — where [`spawn_builder`]'s fallback
/// looks, standing in for `/app` in the shipped image.
fn exe_dir() -> std::path::PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Put an executable stub at `<exe_dir>/<name>` by copying a real binary, so the spawn
/// is a genuine `execve` and not a shell-dependent shim (the runtime image has no shell).
struct Stub(std::path::PathBuf);

impl Stub {
    fn place(name: &str) -> Self {
        let dst = exe_dir().join(name);
        std::fs::copy("/bin/true", &dst).expect("copy /bin/true beside the test binary");
        Self(dst)
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// ── discovery ────────────────────────────────────────────────────────────────────

/// The shipped-image case: `delta.builderBin` is the bare default `slater-build`, the
/// binary is **not** on `PATH`, and it sits beside the running server. Before the
/// fallback this was `No such file or directory` with the correct binary one directory
/// entry away — the failure every `slater:latest` operator hits on their first
/// `CALL slater.consolidate()`.
#[test]
fn a_bare_builder_name_falls_back_to_the_binary_beside_the_server() {
    let name = unique("fallback");
    let _stub = Stub::place(&name);
    // Nothing of this name is on PATH — the name is uuid-suffixed.
    let mut child = spawn_builder(&name, |c| {
        c.stdout(Stdio::null()).stderr(Stdio::null());
    })
    .expect("bare name should resolve beside current_exe()");
    assert!(child.wait().unwrap().success());
}

/// The fallback must not paper over a *wrong path*. An operator who wrote an explicit
/// path meant that path, and silently running some other binary that happens to share
/// the file name would be worse than the error.
#[test]
fn an_explicit_path_is_never_retried_beside_the_server() {
    let name = unique("explicit");
    let _stub = Stub::place(&name);
    // Same file name, but given as a path — so the fallback must not rescue it.
    let explicit = format!("/nonexistent-dir/{name}");
    let err = spawn_builder(&explicit, |c| {
        c.stdout(Stdio::null()).stderr(Stdio::null());
    })
    .expect_err("an explicit path must not fall back");
    let msg = format!("{err:#}");
    assert!(msg.contains(&explicit), "{msg}");
    assert!(
        !msg.contains("also tried"),
        "an explicit path must not report a fallback attempt: {msg}"
    );
}

/// When neither `PATH` nor the sibling directory has it, the error names both attempts
/// and points at the config key — the actionable message.
#[test]
fn a_missing_builder_reports_both_attempts() {
    let name = unique("missing");
    let err = spawn_builder(&name, |_| {}).expect_err("nothing of this name exists");
    let msg = format!("{err:#}");
    assert!(msg.contains(&name), "{msg}");
    assert!(msg.contains("also tried"), "{msg}");
    assert!(msg.contains("delta.builderBin"), "{msg}");
}

// ── how the master key reaches the child ─────────────────────────────────────────

/// `keyEnv` is the effective source only when `keyFile` is empty — `load_key` gives the
/// file precedence, so forwarding the variable while the server was actually reading a
/// file would hand the builder a *different* key (or none), and the failure would surface
/// as an opaque AEAD mismatch a full rebuild later.
#[test]
fn the_key_env_var_is_forwarded_only_when_it_is_the_effective_source() {
    use crate::config::EncryptionConfig;
    let env_only = EncryptionConfig {
        key_env: "SLATER_KEY".into(),
        key_file: String::new(),
    };
    assert_eq!(env_only.key_env_var(), Some("SLATER_KEY"));

    // keyFile wins in `load_key`, so it must win here too.
    let both = EncryptionConfig {
        key_env: "SLATER_KEY".into(),
        key_file: "/etc/slater.key".into(),
    };
    assert_eq!(
        both.key_env_var(),
        None,
        "keyFile takes precedence in load_key, so the env var is NOT the source"
    );

    assert_eq!(EncryptionConfig::default().key_env_var(), None);
    let file_only = EncryptionConfig {
        key_env: String::new(),
        key_file: "/etc/slater.key".into(),
    };
    assert_eq!(file_only.key_env_var(), None);
}

// ── resource limits ──────────────────────────────────────────────────────────────

/// Explicit config always wins over the derivation, so an operator can pin the budget
/// on a host whose cgroup files say something they disagree with.
#[test]
fn explicit_builder_limits_win_over_the_derivation() {
    let cfg = DeltaConfig {
        builder_max_memory: 3 << 30,
        builder_threads: 5,
        consolidate_timeout_secs: 900,
        ..DeltaConfig::default()
    };
    assert_eq!(
        BuilderLimits::resolve(&cfg),
        BuilderLimits {
            max_memory_bytes: 3 << 30,
            threads: 5,
            timeout_secs: 900,
        }
    );
}

/// The derivation arithmetic, pinned independently of whatever cgroup the test host
/// happens to be in.
///
/// The floor is the interesting case, and it caught a real mistake: a floor set *above*
/// the fraction raises the budget on exactly the small containers the derivation exists to
/// protect, and without the final clamp it can hand the builder more memory than the
/// container has — turning a clear budget abort into an OOM kill.
#[test]
fn the_memory_derivation_floors_but_never_exceeds_the_container() {
    let derive = |limit: u64| {
        (((limit as f64) * super::BUILDER_MEMORY_FRACTION) as u64)
            .max(MIN_BUILDER_MEMORY)
            .min(limit)
    };
    // Roomy container: a plain fraction, well clear of both bounds.
    assert_eq!(derive(8 << 30), (8u64 << 30) * 35 / 100);
    // Small container: the floor lifts it, but not past the limit.
    let small = derive(100 << 20);
    assert_eq!(small, MIN_BUILDER_MEMORY, "the floor applies");
    assert!(small < 100 << 20, "and still fits the container");
    // Tiny container: the clamp wins over the floor — never ask for more than exists.
    assert_eq!(derive(32 << 20), 32 << 20);
    // The floor must stay above the sorter's own minimum or it buys nothing.
    assert!(MIN_BUILDER_MEMORY > graph_format::membudget::MIN_SORT_BYTES as u64);
}

/// The defaults are "derive, or say nothing". A zero is never forwarded as a flag: the
/// builder's own default is right for an uncapped host, and inventing a number there
/// would be worse than leaving it alone.
#[test]
fn unset_builder_limits_either_derive_or_omit_the_flag() {
    let limits = BuilderLimits::resolve(&DeltaConfig::default());
    assert_eq!(limits.timeout_secs, 0, "no timeout unless asked for");
    // Whether a cgroup limit exists — and how big it is — depends on the host, so assert
    // only what holds on any of them: an absent limit omits the flag entirely (exactly 0),
    // and a present one never yields a budget larger than the limit itself. The floor is
    // *not* asserted here: the clamp deliberately wins over it on a sub-64 MiB container,
    // so `>= MIN_BUILDER_MEMORY` would be wrong. The exact arithmetic is pinned over
    // synthetic limits in `the_memory_derivation_floors_but_never_exceeds_the_container`.
    match crate::diag::cgroup_mem_limit() {
        None => assert_eq!(limits.max_memory_bytes, 0, "uncapped ⇒ omit the flag"),
        Some(lim) => assert!(
            limits.max_memory_bytes > 0 && limits.max_memory_bytes <= lim,
            "derived budget {} must fit the {lim}-byte limit",
            limits.max_memory_bytes
        ),
    }
}

// ── output capture ───────────────────────────────────────────────────────────────

/// Lines per stream for the chatty-child tests.
///
/// **Calibrated, not guessed.** A pipe holds 64 KiB by default, and the whole hazard is a
/// child blocked on a *full* pipe. Measured against an undrained parent: 4 000 lines
/// (~35 KiB/stream) completes fine — a test at that size asserts nothing — while 20 000
/// (~175 KiB/stream) hangs the parent's `wait()` indefinitely. So this must stay
/// comfortably above the buffer or these tests go quietly vacuous.
const CHATTY_LINES: usize = 20_000;

/// A child that floods both streams and then exits with `code`. `seq`/`sed` rather than a
/// shell loop so 20 000 lines cost milliseconds, not seconds.
fn chatty(code: i32) -> String {
    format!(
        "seq {CHATTY_LINES} | sed 's/^/out-/'; seq {CHATTY_LINES} | sed 's/^/err-/' >&2; \
         exit {code}"
    )
}

/// The deadlock the previous inherited-stdio comment warned about, now that the pipes
/// are real: a child that writes more than a pipe buffer before exiting blocks forever if
/// nobody drains it, and the parent's `wait()` blocks with it. With the drain threads
/// running the pair completes.
///
/// Fails by hanging rather than by asserting, so it carries its own deadline.
#[test]
fn a_chatty_child_does_not_deadlock_the_wait() {
    let started = Instant::now();
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(chatty(0))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = drain_pipe(child.stdout.take().unwrap(), "g".into(), false);
    let err = drain_pipe(child.stderr.take().unwrap(), "g".into(), true);
    let handle = Arc::new(Mutex::new(child));
    let status = wait_with_timeout(&handle, 60, "g").expect("must not time out");
    assert!(status.success());

    let mut drains = vec![out, err];
    let tail = finish(&handle, &mut drains);
    // Both streams' tails are present, and the whole thing is bounded — not 40 000 lines.
    assert!(tail.contains(&format!("out-{CHATTY_LINES}")), "{tail}");
    assert!(tail.contains(&format!("err-{CHATTY_LINES}")), "{tail}");
    assert!(
        !tail.contains("out-1\n"),
        "the tail must be bounded: {tail}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(60),
        "drained wait should be prompt"
    );
}

/// A second `finish` is a no-op rather than a panic on an already-joined thread — the
/// error paths in `run_builder` are exclusive, but only at runtime.
#[test]
fn finish_is_idempotent() {
    let child = Command::new("/bin/true").spawn().unwrap();
    let handle = Arc::new(Mutex::new(child));
    let mut drains = Vec::new();
    assert_eq!(finish(&handle, &mut drains), "");
    assert_eq!(finish(&handle, &mut drains), "");
}

// ── timeout ──────────────────────────────────────────────────────────────────────

/// A wedged builder is killed at the budget and reported as a timeout, rather than
/// holding the single-flight consolidation claim — and so every cheaper flush/compaction
/// rung — indefinitely. Before this the wait was a bare blocking `child.wait()`.
#[test]
fn a_builder_past_its_budget_is_killed_and_reported() {
    let child = Command::new("/bin/sleep").arg("120").spawn().unwrap();
    let handle = Arc::new(Mutex::new(child));
    let started = Instant::now();
    let err = wait_with_timeout(&handle, 1, "g").expect_err("must time out");
    let msg = format!("{err:#}");
    assert!(msg.contains("consolidateTimeoutSecs"), "{msg}");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "killed promptly"
    );
    // Reaped, not left as a zombie: a second wait sees an already-exited child.
    assert!(handle.lock().unwrap().try_wait().unwrap().is_some());
}

/// `timeout_secs == 0` keeps the historical unbounded wait — the right default for a
/// large core whose rebuild legitimately runs for hours.
#[test]
fn a_zero_timeout_waits_indefinitely() {
    let child = Command::new("/bin/sleep").arg("0.2").spawn().unwrap();
    let handle = Arc::new(Mutex::new(child));
    assert!(wait_with_timeout(&handle, 0, "g").unwrap().success());
}

// ── scratch hygiene ──────────────────────────────────────────────────────────────

/// A server killed mid-rebuild cannot run its own cleanup, so the whole-graph scratch
/// dump (sealed, but a complete copy) used to sit in the data directory forever. Boot
/// reclaims it.
#[test]
fn boot_reclaims_a_leftover_consolidation_scratch_dump() {
    let root = std::env::temp_dir().join(unique("sweep"));
    let graph = root.join("people");
    std::fs::create_dir_all(&graph).unwrap();

    let orphan = graph.join(format!(
        "{}{}",
        crate::server::registry::CONSOLIDATE_SCRATCH_PREFIX,
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("nodes.blk"), b"whole-graph bytes").unwrap();
    // A published generation directory sits beside it and must survive.
    let keep = graph.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&keep).unwrap();
    std::fs::write(graph.join("current"), b"pointer").unwrap();

    crate::server::registry::sweep_consolidation_scratch(&root, "people");

    assert!(!orphan.exists(), "the scratch dump should be reclaimed");
    assert!(keep.exists(), "a real generation must not be touched");
    assert!(graph.join("current").exists(), "the pointer must survive");
    let _ = std::fs::remove_dir_all(&root);
}

/// A graph directory that does not exist yet (fresh deployment) is not an error — the
/// sweep runs on the boot path and must never stop a graph coming online.
#[test]
fn sweeping_a_missing_graph_dir_is_a_no_op() {
    let root = std::env::temp_dir().join(unique("sweep-missing"));
    crate::server::registry::sweep_consolidation_scratch(&root, "nope");
}

// ── shutdown registry ────────────────────────────────────────────────────────────

/// `LIVE_BUILDERS` is process-global — correct in production (one server per process,
/// and shutdown means *every* builder) but shared by every test in this binary, which
/// cargo runs in parallel threads. Without this, one test's `kill_live_builders()` reaps
/// another's child mid-assertion. Serialising is the honest fix: the global really is
/// global, and pretending otherwise would only make the tests lie.
static REGISTRY: Mutex<()> = Mutex::new(());

/// Shutdown must stop an in-flight rebuild rather than orphan it: left running it keeps
/// burning CPU and IO for an operator who has already stopped the server, and can still
/// flip `current` afterwards for a *restarted* server to adopt.
#[test]
fn shutdown_kills_a_registered_builder() {
    let _serial = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let child = Command::new("/bin/sleep").arg("120").spawn().unwrap();
    let handle = super::register_builder(child);
    super::kill_live_builders();
    assert!(
        handle.lock().unwrap().try_wait().unwrap().is_some(),
        "the child should have been killed and reaped"
    );
    // The registry is drained, so a second shutdown is a no-op.
    super::kill_live_builders();
}

/// Unregistering is by pointer identity, so one graph's consolidation finishing cannot
/// deregister another's still-running child out from under shutdown.
#[test]
fn unregister_only_drops_its_own_child() {
    let _serial = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let mine = super::register_builder(Command::new("/bin/sleep").arg("120").spawn().unwrap());
    let theirs = super::register_builder(Command::new("/bin/sleep").arg("120").spawn().unwrap());
    super::unregister_builder(&mine);
    super::kill_live_builders();
    assert!(
        theirs.lock().unwrap().try_wait().unwrap().is_some(),
        "the still-registered child must be killed"
    );
    // Ours was removed from the registry, so shutdown left it alone — clean it up.
    let mut m = mine.lock().unwrap();
    assert!(
        m.try_wait().unwrap().is_none(),
        "unregistered, so untouched"
    );
    let _ = m.kill();
    let _ = m.wait();
}

/// A key written to a child that never reads it must not wedge the parent — the drains
/// keep the child's own output flowing while the parent writes.
#[test]
fn the_key_write_survives_a_child_that_ignores_stdin() {
    let mut child = Command::new("/bin/sh")
        .arg("-c")
        .arg(chatty(7))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = drain_pipe(child.stdout.take().unwrap(), "g".into(), false);
    let err = drain_pipe(child.stderr.take().unwrap(), "g".into(), true);
    let mut stdin = child.stdin.take().unwrap();
    let _ = stdin.write_all(b"deadbeef");
    drop(stdin);

    let handle = Arc::new(Mutex::new(child));
    let status = wait_with_timeout(&handle, 60, "g").expect("must not deadlock");
    assert_eq!(status.code(), Some(7));
    let mut drains = vec![out, err];
    // The child's own diagnostics reach the failure message instead of a bare exit code.
    assert!(finish(&handle, &mut drains).contains(&format!("err-{CHATTY_LINES}")));
}
