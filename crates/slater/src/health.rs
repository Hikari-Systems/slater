//! Bolt-native health probe for the container `HEALTHCHECK`.
//!
//! The house helper `hs_utils::healthcheck` sends an HTTP `GET /healthcheck` and
//! checks for `HTTP/1.1 200`. Slater's port speaks **Bolt**, not HTTP, so that
//! probe would always fail against us. Instead we perform the Bolt handshake:
//! connect, send the magic preamble plus four version proposals, and treat a
//! non-zero negotiated version in the four-byte reply as "healthy".
//!
//! Stdlib only — no tokio, no extra dependencies — so it is safe to call at the
//! very top of `main` before the async runtime starts.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// The Bolt handshake magic preamble (`0x6060B017`).
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Connect to `host:port`, perform a Bolt handshake, and return `true` if the
/// server negotiates a non-zero protocol version.
pub fn probe(host: &str, port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect((host, port)) else {
        return false;
    };
    stream.set_read_timeout(Some(Duration::from_secs(4))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(4))).ok();

    // Magic preamble followed by four big-endian u32 version proposals, highest
    // preference first. We offer 5.x then 4.4; any conformant Bolt server replies
    // with the chosen version, or four zero bytes if it supports none.
    let mut hs = Vec::with_capacity(20);
    hs.extend_from_slice(&BOLT_MAGIC);
    for v in [
        0x0000_0005u32,
        0x0000_0004u32,
        0x0000_0000u32,
        0x0000_0000u32,
    ] {
        hs.extend_from_slice(&v.to_be_bytes());
    }
    if stream.write_all(&hs).is_err() {
        return false;
    }

    let mut reply = [0u8; 4];
    stream.read_exact(&mut reply).is_ok() && reply != [0, 0, 0, 0]
}

/// Handle the `healthcheck` CLI subcommand and exit if present.
///
/// No-op unless `argv[1] == "healthcheck"`, so it can be called unconditionally
/// at the top of `main`. Parses `argv[2..4]` as `[host] [port]`, defaulting to
/// `localhost` and `default_port`. Exits `0` on success, `1` on failure.
pub fn check_subcommand(default_port: u16) {
    if std::env::args().nth(1).as_deref() != Some("healthcheck") {
        return;
    }
    let host = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "localhost".to_string());
    let port = std::env::args()
        .nth(3)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(default_port);
    std::process::exit(if probe(&host, port) { 0 } else { 1 });
}
