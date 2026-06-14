// SPDX-License-Identifier: Apache-2.0
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
    // preference first. Each proposal is encoded `00 <range> <minor> <major>`,
    // where `range` is how many consecutive lower minors are ALSO acceptable — so
    // `00 06 06 05` offers Bolt 5.6 down to 5.0. We MUST offer a range, not a bare
    // version: this server negotiates Bolt 5.1–5.4, so a plain `5.0` proposal
    // (`00 00 00 05`) matches nothing and the server replies `00 00 00 00`, making
    // the probe (and the container HEALTHCHECK) spuriously fail against a perfectly
    // healthy server. A conformant Bolt server replies with the chosen version, or
    // four zero bytes if it supports none.
    let mut hs = Vec::with_capacity(20);
    hs.extend_from_slice(&BOLT_MAGIC);
    for v in [
        0x0006_0605u32, // Bolt 5.6 … 5.0
        0x0003_0404u32, // Bolt 4.4 … 4.1
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

/// Handle the `diagnostics` CLI subcommand and exit if present.
///
/// A thin operator/CI convenience over the `CALL slater.diagnostics()` Bolt
/// introspection statement (which the load-test coordinator reads directly): it
/// opens a full Bolt session, runs the statement, and prints the metric snapshot
/// as JSON to stdout. Exits `0` on a successful read, `1` on any failure —
/// notably when diagnostics are disabled server-side (`loadTestDiagnostics`
/// off), which surfaces as a Bolt `FAILURE`.
///
/// No-op unless `argv[1] == "diagnostics"`, so it is safe to call unconditionally
/// at the top of `main` (before the async runtime — this is a blocking stdlib
/// client reusing the synchronous `bolt` encoders). Usage:
/// `slater diagnostics [host] [port] [user] [password]`, with `user`/`password`
/// also taken from `SLATER_DIAG_USER` / `SLATER_DIAG_PASSWORD` when omitted.
pub fn diagnostics_subcommand(default_port: u16) {
    if std::env::args().nth(1).as_deref() != Some("diagnostics") {
        return;
    }
    let host = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "localhost".to_string());
    let port = std::env::args()
        .nth(3)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(default_port);
    let user = std::env::args()
        .nth(4)
        .or_else(|| std::env::var("SLATER_DIAG_USER").ok())
        .unwrap_or_default();
    let password = std::env::args()
        .nth(5)
        .or_else(|| std::env::var("SLATER_DIAG_PASSWORD").ok())
        .unwrap_or_default();

    match fetch_diagnostics(&host, port, &user, &password) {
        Ok(json) => {
            println!("{json}");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("slater diagnostics failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Open a Bolt session, run `CALL slater.diagnostics()`, and render the resulting
/// `metric`/`value` rows as a single JSON object. Blocking, stdlib + the crate's
/// synchronous `bolt` codec only.
fn fetch_diagnostics(host: &str, port: u16, user: &str, password: &str) -> std::io::Result<String> {
    use crate::bolt::message::tag;
    use crate::bolt::packstream::PsValue;

    let mut conn = BoltConn::connect(host, port)?;

    // HELLO (5.x: no auth here) → LOGON (basic auth).
    conn.request(
        tag::HELLO,
        vec![PsValue::Map(vec![(
            "user_agent".into(),
            PsValue::str("slater-diagnostics/1.0"),
        )])],
    )?;
    conn.request(
        tag::LOGON,
        vec![PsValue::Map(vec![
            ("scheme".into(), PsValue::str("basic")),
            ("principal".into(), PsValue::str(user)),
            ("credentials".into(), PsValue::str(password)),
        ])],
    )?;

    // RUN the diagnostics statement (server-level: no graph needed), then PULL.
    conn.request(
        tag::RUN,
        vec![
            PsValue::str("CALL slater.diagnostics()"),
            PsValue::Map(vec![]),
            PsValue::Map(vec![]),
        ],
    )?;
    conn.send(
        tag::PULL,
        vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
    )?;

    // Drain RECORD rows (each a [metric, value] list) until the closing SUCCESS.
    let mut obj = serde_json::Map::new();
    loop {
        let (t, fields) = conn.recv()?;
        if t == tag::RECORD {
            if let Some(PsValue::List(vals)) = fields.first() {
                if let (Some(PsValue::String(k)), Some(v)) = (vals.first(), vals.get(1)) {
                    obj.insert(k.clone(), ps_to_json(v));
                }
            }
        } else if t == tag::SUCCESS {
            break;
        } else {
            return Err(std::io::Error::other(format!(
                "unexpected Bolt message tag {t:#x} while reading diagnostics \
                 (is loadTestDiagnostics enabled?)"
            )));
        }
    }
    serde_json::to_string_pretty(&serde_json::Value::Object(obj)).map_err(std::io::Error::other)
}

/// Map a PackStream scalar to a JSON value (the diagnostics column is Int/Float).
fn ps_to_json(v: &crate::bolt::packstream::PsValue) -> serde_json::Value {
    use crate::bolt::packstream::PsValue;
    match v {
        PsValue::Int(i) => serde_json::Value::from(*i),
        PsValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        PsValue::String(s) => serde_json::Value::String(s.clone()),
        PsValue::Bool(b) => serde_json::Value::Bool(*b),
        _ => serde_json::Value::Null,
    }
}

/// A minimal blocking Bolt client: handshake, then request/response over the
/// crate's synchronous chunk/packstream codec. Used only by the `diagnostics`
/// subcommand, which runs before the async runtime is built.
struct BoltConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl BoltConn {
    fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let mut stream = TcpStream::connect((host, port))?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        // Handshake: magic + four version proposals (5.6…5.0, 4.4…4.1).
        let mut hs = Vec::with_capacity(20);
        hs.extend_from_slice(&BOLT_MAGIC);
        for v in [0x0006_0605u32, 0x0003_0404, 0, 0] {
            hs.extend_from_slice(&v.to_be_bytes());
        }
        stream.write_all(&hs)?;
        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply)?;
        if reply == [0, 0, 0, 0] {
            return Err(std::io::Error::other(
                "server negotiated no Bolt version (handshake rejected)",
            ));
        }
        Ok(Self {
            stream,
            buf: Vec::new(),
        })
    }

    /// Encode and send one Bolt message (chunked + terminated by `bolt::to_wire`).
    fn send(
        &mut self,
        tag: u8,
        fields: Vec<crate::bolt::packstream::PsValue>,
    ) -> std::io::Result<()> {
        let msg = crate::bolt::packstream::PsValue::Struct { tag, fields };
        self.stream.write_all(&crate::bolt::message::to_wire(&msg))
    }

    /// Read the next decoded Bolt message as `(tag, fields)`.
    fn recv(&mut self) -> std::io::Result<(u8, Vec<crate::bolt::packstream::PsValue>)> {
        use crate::bolt::chunk;
        use crate::bolt::packstream::{self, PsValue};
        loop {
            if let Some((body, consumed)) =
                chunk::decode_message(&self.buf).map_err(std::io::Error::other)?
            {
                self.buf.drain(..consumed);
                return match packstream::from_slice(&body).map_err(std::io::Error::other)? {
                    PsValue::Struct { tag, fields } => Ok((tag, fields)),
                    other => Err(std::io::Error::other(format!(
                        "expected a Bolt struct, got {other:?}"
                    ))),
                };
            }
            let mut tmp = [0u8; 16384];
            let n = self.stream.read(&mut tmp)?;
            if n == 0 {
                return Err(std::io::Error::other("server closed the connection"));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Send a message and assert the reply is `SUCCESS`, returning its fields.
    fn request(
        &mut self,
        tag: u8,
        fields: Vec<crate::bolt::packstream::PsValue>,
    ) -> std::io::Result<Vec<crate::bolt::packstream::PsValue>> {
        use crate::bolt::message::tag as t;
        self.send(tag, fields)?;
        let (rt, rfields) = self.recv()?;
        if rt == t::SUCCESS {
            Ok(rfields)
        } else {
            // Surface the server's failure message (e.g. bad credentials, or
            // diagnostics disabled) verbatim.
            let detail = rfields
                .first()
                .and_then(|m| m.get("message"))
                .and_then(crate::bolt::packstream::PsValue::as_str)
                .unwrap_or("unknown error")
                .to_string();
            Err(std::io::Error::other(format!(
                "Bolt request (tag {tag:#x}) failed: {detail}"
            )))
        }
    }
}
