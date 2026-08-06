// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
//! The WAL's headline durability claim, proved against a **real killed process**:
//! once `append_batch` returns, the batch survives `SIGKILL`.
//!
//! `wal.rs`'s own unit tests cover the *shapes* a crash leaves behind — a torn frame
//! (`torn_frame_truncation_is_ignored`), a zero-filled tail
//! (`zero_filled_tail_replays_as_torn_tail`), a failed rollback truncate — by writing
//! those bytes deliberately and replaying them in-process. They are good tests, but
//! every one of them asks "given these bytes, does replay do the right thing?". None
//! asks the question an operator actually cares about: **is the data out of the process
//! when the ack returns?** That cannot be observed from inside the process that would
//! have done the writing — a test that mocks the crash also mocks away the thing under
//! test.
//!
//! So this spawns a real child, has it ack a batch the way the writer's ack path does,
//! kills it with `SIGKILL` (uncatchable — no destructor, no flush, no unwinding), and
//! replays the directory from the parent. `append_batch` is the exact call the Bolt ack
//! barrier sits behind.
//!
//! # What this proves, and what it does not
//!
//! Be precise about the boundary, because it is easy to read more into a `SIGKILL` test
//! than it earns. Killing a process destroys its **userspace** buffers and nothing else:
//! the kernel page cache belongs to the kernel and survives untouched, so the parent can
//! read back bytes that were merely `write(2)`-n and never `fsync`-ed.
//!
//! - **Proved:** nothing is acked while still sitting in the `BufWriter`. The commit
//!   frame and every record it covers have left the process and are visible to an
//!   independent reader at the moment the ack is observed. That is a real and frequently
//!   re-introduced bug class, and it is what the mutation check pins: replacing
//!   `append_batch` with bare `append`s (no commit frame) drops `last_seq` to `Seq(0)`
//!   and fails this test.
//! - **Proved:** an *uncommitted* tail never replays **even though its bytes are on
//!   disk**. That second clause is load-bearing and was wrong in the first version of
//!   this file. `WalSink` writes through a `BufWriter` (wal.rs:606, default 8 KiB) and
//!   only `commit` flushes it, so a small uncommitted record never leaves userspace and
//!   `SIGKILL` discards it — the assertion could not fail, and a regression that made
//!   `replay_dir` honour trailing uncommitted frames would have sailed through. The
//!   uncommitted record below therefore carries a payload deliberately larger than the
//!   buffer, so it is written straight to the file, and the test asserts the segment
//!   really did grow before asserting that replay ignores it.
//! - **NOT proved:** that the bytes reached durable media. Only a power cut, a device
//!   with write-cache fault injection, or a VM snapshot/kill can show that, and none of
//!   those belongs in `cargo test`. `fsync` correctness on the ack path stays an
//!   argument from code review (`WalSink::commit`) plus the fault-injection hook in
//!   `wal.rs`, not something this test can witness.
//!
//! Naming it after what it is — a killed *process*, not a killed *machine* — is the
//! point; a test called "power failure" that a page cache can satisfy would be worse
//! than no test at all.
//!
//! # The child
//!
//! **The child is this same test binary re-executed**, which keeps the whole thing
//! inside one auto-discovered integration test with no extra `[[bin]]`/`[[test]]` target
//! to declare (and so no new Docker dep-cache stub to keep in step).
//!
//! Selecting it needs care, because env vars are inherited by *every* descendant. A bare
//! `if env::var(CHILD).is_ok()` would turn any process that happened to inherit the
//! variable — a developer who exported it while debugging, a suite re-run from inside a
//! shell the child spawned — into the parked child body, hanging `cargo test` instead of
//! failing it. So the parent mints a per-run token, writes it to a sentinel file, and
//! passes it by value: a process is the child only if the env token matches the file's
//! contents. A stale ambient variable points at a directory that no longer exists (the
//! parent removes it) or holds a different token, and the top-level test runs normally.
//!
//! Deliberately **not** feature-gated and **not** `#[ignore]`d: CI runs
//! `cargo test --workspace --locked`, and a durability proof that only runs when someone
//! remembers to pass a flag is not a durability proof. That makes hang-safety a release
//! concern rather than a convenience — every wait here is bounded, and the child parks
//! for a finite time so an orphan reaps itself.

use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use graph_format::ids::Value;
use slater_delta::wal::{replay_dir, segment_path, Seq, WalOp, WalRecord, WalSink};

/// Env var carrying the per-run token. Its *value* is the selector, not its presence.
const TOKEN_ENV: &str = "SLATER_WAL_KILL9_TOKEN";
/// Env var carrying the run root the child should work under.
const ROOT_ENV: &str = "SLATER_WAL_KILL9_ROOT";
/// The child prints this to stdout *after* its batch is durable, and only then.
const ACK_MARKER: &str = "SLATER_WAL_ACKED";
/// How many records the child acks before it is killed.
const ACKED: u64 = 8;
/// Payload size for the uncommitted record. Must exceed `WalSink`'s `BufWriter`
/// capacity (`BufWriter::new` ⇒ 8 KiB) so the frame bypasses the buffer and lands on
/// disk; otherwise the "uncommitted frames never replay" assertion is vacuous.
const OVERFLOW_PAYLOAD: usize = 64 * 1024;
/// How long the child parks before giving up. Bounded so an orphan — a parent that
/// panicked before `kill`, or was itself killed — reaps itself instead of lingering.
const CHILD_PARK: Duration = Duration::from_secs(120);
/// Bound on the parent's wait for the ack.
const ACK_TIMEOUT: Duration = Duration::from_secs(60);

fn wal_dir(root: &Path) -> PathBuf {
    root.join("wal")
}
fn token_path(root: &Path) -> PathBuf {
    root.join("token")
}

fn upsert(seq: u64, key: &str, value: Value) -> WalRecord {
    WalRecord {
        seq: Seq(seq),
        op: WalOp::UpsertNode {
            label: "L".into(),
            key: key.into(),
            value,
            patches: Vec::new(),
        },
    }
}

fn acked_record(i: u64) -> WalRecord {
    upsert(i, &format!("k{i}"), Value::Int(i as i64))
}

/// Reaps its child on drop, so a panic anywhere in the test cannot leak a process.
/// `std::process::Child` deliberately does *not* do this for you.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The child process body: ack a batch, write an uncommitted tail, announce, park.
///
/// The ordering is the whole point, and it is not the obvious one. `append_batch` returns
/// only once the commit frame is written and fsynced, so the marker printed afterwards
/// means the parent's `SIGKILL` can only land on a process that has already acked.
///
/// The uncommitted record has to go **before** the marker, not after. Announcing first
/// and appending second is a race the parent wins essentially always: it reads the line
/// and kills within microseconds, and the child is still between the two statements — the
/// segment then holds nothing but the acked batch (measured: 170 bytes), and the "an
/// uncommitted tail never replays" assertion has nothing to bite on. Writing it first
/// costs nothing and makes the disk state deterministic: acked batch, then an
/// uncommitted frame large enough to overflow the `BufWriter` and reach the file, then
/// the announcement.
fn child_writer_body(root: &Path) -> ! {
    let dir = wal_dir(root);
    let mut sink = WalSink::create(&dir, 0, None).expect("create WAL segment");
    let recs: Vec<WalRecord> = (1..=ACKED).map(acked_record).collect();
    sink.append_batch(&recs, Seq(ACKED))
        .expect("ack batch (fsynced)");

    // A frame nothing will ever commit, big enough to be written straight through the
    // BufWriter to the file. Replay must ignore it despite the bytes being there.
    let big = Value::Str("x".repeat(OVERFLOW_PAYLOAD));
    sink.append(&upsert(ACKED + 1, "uncommitted", big))
        .expect("append the uncommitted tail");

    // Acked and the tail is on disk. Tell the parent it may kill us.
    println!("{ACK_MARKER}");
    std::io::stdout().flush().expect("flush ack marker");

    std::thread::sleep(CHILD_PARK);
    std::process::exit(70); // Only reached if the parent never killed us.
}

/// Wait for `ACK_MARKER` on the child's stdout under a real timeout.
///
/// The read runs on its own thread and reports through a channel, because
/// `BufReader::read_line` blocks with no deadline of its own: checking `Instant::now()`
/// between reads — the obvious shape, and the one this started as — leaves the timeout
/// unreachable in exactly the case it exists for, and this test gates the release.
///
/// Matching is `contains`, not equality. With `--nocapture` libtest's pretty formatter
/// prints `test <name> ... ` *without* a trailing newline whenever it runs
/// single-threaded (a one-vCPU runner, or `RUST_TEST_THREADS=1`), so the child's marker
/// arrives glued to that prefix. An equality check passes on a 16-core dev box and hangs
/// on a small CI container.
fn wait_for_ack(stdout: std::process::ChildStdout) -> Result<(), String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(Err("child closed stdout before acking".to_string()));
                    return;
                }
                Ok(_) if line.contains(ACK_MARKER) => {
                    let _ = tx.send(Ok(()));
                    return;
                }
                Ok(_) => continue,
                Err(e) => {
                    let _ = tx.send(Err(format!("reading child stdout: {e}")));
                    return;
                }
            }
        }
    });
    match rx.recv_timeout(ACK_TIMEOUT) {
        Ok(r) => r,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "timed out after {ACK_TIMEOUT:?} waiting for the ack"
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err("stdout reader died".to_string()),
    }
}

/// Are we the child? Only if the env token matches the sentinel the parent wrote.
fn child_root() -> Option<PathBuf> {
    let token = std::env::var(TOKEN_ENV).ok()?;
    let root = PathBuf::from(std::env::var(ROOT_ENV).ok()?);
    let on_disk = std::fs::read_to_string(token_path(&root)).ok()?;
    (on_disk == token).then_some(root)
}

#[test]
fn an_acked_batch_survives_sigkill() {
    if let Some(root) = child_root() {
        child_writer_body(&root);
    }

    // Per-run token: pid alone is not enough, since a re-run reuses pids.
    let nonce = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before the epoch")
            .as_nanos()
    );
    let root = std::env::temp_dir().join(format!("slater_wal_kill9_{nonce}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create run root");
    std::fs::write(token_path(&root), &nonce).expect("write the child token");

    let exe = std::env::current_exe().expect("path to this test binary");
    let mut child = KillOnDrop(
        Command::new(exe)
            // Run only this test in the child, and let its `println!` reach the pipe.
            .args(["an_acked_batch_survives_sigkill", "--exact", "--nocapture"])
            .env(TOKEN_ENV, &nonce)
            .env(ROOT_ENV, &root)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the child writer"),
    );

    let stdout = child.0.stdout.take().expect("piped stdout");
    if let Err(e) = wait_for_ack(stdout) {
        // `KillOnDrop` reaps the child on the way out of this panic.
        panic!("child never acked: {e}");
    }

    // SIGKILL: no unwinding, no destructors, no buffered-writer flush. Whatever is in
    // the file now is what the child had already written.
    child.0.kill().expect("SIGKILL the child");
    let status = child.0.wait().expect("reap the child");

    // Assert death *by signal 9*, not merely a non-zero exit. A child that panicked
    // after acking (or was OOM-killed, or hit `CHILD_PARK` and exited 70) also exits
    // non-zero, and every assertion below would still pass — the test would report
    // green having never exercised an uncatchable kill.
    assert_eq!(
        status.signal(),
        Some(9),
        "the child must have died by SIGKILL, not exited on its own — otherwise this \
         test proves nothing about an uncatchable kill (status: {status:?}, code: {:?})",
        status.code()
    );

    // The uncommitted frame must actually be on disk, or the negative assertion below
    // is vacuous: a frame still sitting in the BufWriter dies with the process, and a
    // replay that wrongly honoured trailing frames would never be caught.
    let seg = segment_path(&wal_dir(&root), 0);
    let on_disk = std::fs::metadata(&seg).expect("segment file").len();
    assert!(
        on_disk > OVERFLOW_PAYLOAD as u64,
        "the uncommitted frame never reached the file ({on_disk} bytes) — the \
         'uncommitted frames never replay' assertion would be vacuous"
    );

    // The claim: every acked record replays, in order, with the acked high-water seq —
    // and nothing else does, despite those extra bytes sitting right there.
    let replay = replay_dir(&wal_dir(&root), None).expect("replay the killed process's WAL");
    assert_eq!(
        replay.last_seq,
        Seq(ACKED),
        "replay must recover exactly the acked high-water mark"
    );
    assert_eq!(
        replay.records.len(),
        ACKED as usize,
        "expected {ACKED} acked records, got {}: an uncommitted frame must never \
         replay, and an acked one must never be lost",
        replay.records.len()
    );
    for (i, rec) in replay.records.iter().enumerate() {
        assert_eq!(
            *rec,
            acked_record(i as u64 + 1),
            "record {i} did not survive intact"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
