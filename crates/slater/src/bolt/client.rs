// SPDX-License-Identifier: Apache-2.0
//! A minimal **blocking** Bolt client.
//!
//! Handshake, `HELLO`/`LOGON` (basic auth), then request/response over the
//! crate's synchronous chunk/packstream codec. Stdlib + this crate's `bolt` wire
//! layer only — no tokio, no extra dependencies — so it is safe to use from the
//! CLI subcommands that run at the very top of `main`, before the async runtime
//! is built (`healthcheck`/`diagnostics`, and the `dump` exporter).
//!
//! This is deliberately *not* the server's connection state machine (that lives
//! in `server.rs` on `tokio`); it is the client half, used to talk **to** a
//! running slater server over the same protocol.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::bolt::message::tag;
use crate::bolt::packstream::PsValue;

/// The Bolt handshake magic preamble (`0x6060B017`).
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// A synchronous Bolt client connection: handshake done, ready for `HELLO`.
pub struct BoltClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl BoltClient {
    /// Connect to `host:port` and negotiate a Bolt version. `timeout` bounds each
    /// read and write (pick generously for a streaming `PULL` over a large graph).
    pub fn connect(host: &str, port: u16, timeout: Duration) -> std::io::Result<Self> {
        let mut stream = TcpStream::connect((host, port))?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
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

    /// `HELLO` (5.x: no auth here) then `LOGON` with `basic` scheme credentials.
    /// `user_agent` identifies the client in the server's connection log.
    pub fn login(&mut self, user_agent: &str, user: &str, password: &str) -> std::io::Result<()> {
        self.request(
            tag::HELLO,
            vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str(user_agent),
            )])],
        )?;
        self.request(
            tag::LOGON,
            vec![PsValue::Map(vec![
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str(user)),
                ("credentials".into(), PsValue::str(password)),
            ])],
        )?;
        Ok(())
    }

    /// `RUN` a statement (optionally against graph `db`), `PULL` every record, and
    /// return `(column names, rows)`. Each row is the record's field list. The
    /// column names come from the `RUN` `SUCCESS` `fields` metadata.
    pub fn run_pull(
        &mut self,
        query: &str,
        db: Option<&str>,
    ) -> std::io::Result<(Vec<String>, Vec<Vec<PsValue>>)> {
        let mut extra: Vec<(String, PsValue)> = Vec::new();
        if let Some(db) = db.filter(|s| !s.is_empty()) {
            extra.push(("db".into(), PsValue::str(db)));
        }
        let run_ok = self.request(
            tag::RUN,
            vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(extra),
            ],
        )?;
        let columns = run_ok
            .first()
            .and_then(|m| m.get("fields"))
            .and_then(|f| match f {
                PsValue::List(items) => Some(
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            })
            .unwrap_or_default();

        self.send(
            tag::PULL,
            vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
        )?;
        let mut rows: Vec<Vec<PsValue>> = Vec::new();
        loop {
            let (t, fields) = self.recv()?;
            if t == tag::RECORD {
                // A RECORD carries a single List field: the row's values.
                match fields.into_iter().next() {
                    Some(PsValue::List(vals)) => rows.push(vals),
                    Some(other) => rows.push(vec![other]),
                    None => rows.push(Vec::new()),
                }
            } else if t == tag::SUCCESS {
                break;
            } else {
                let detail = fields
                    .first()
                    .and_then(|m| m.get("message"))
                    .and_then(PsValue::as_str)
                    .unwrap_or("unknown error")
                    .to_string();
                return Err(std::io::Error::other(format!(
                    "Bolt PULL for `{query}` failed: {detail}"
                )));
            }
        }
        Ok((columns, rows))
    }

    /// Encode and send one Bolt message (chunked + terminated by `to_wire`).
    pub fn send(&mut self, tag: u8, fields: Vec<PsValue>) -> std::io::Result<()> {
        let msg = PsValue::Struct { tag, fields };
        self.stream.write_all(&crate::bolt::message::to_wire(&msg))
    }

    /// Read the next decoded Bolt message as `(tag, fields)`.
    pub fn recv(&mut self) -> std::io::Result<(u8, Vec<PsValue>)> {
        use crate::bolt::chunk;
        use crate::bolt::packstream;
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
    /// Surfaces the server's failure message verbatim (bad credentials, etc.).
    pub fn request(&mut self, tag: u8, fields: Vec<PsValue>) -> std::io::Result<Vec<PsValue>> {
        self.send(tag, fields)?;
        let (rt, rfields) = self.recv()?;
        if rt == crate::bolt::message::tag::SUCCESS {
            Ok(rfields)
        } else {
            let detail = rfields
                .first()
                .and_then(|m| m.get("message"))
                .and_then(PsValue::as_str)
                .unwrap_or("unknown error")
                .to_string();
            Err(std::io::Error::other(format!(
                "Bolt request (tag {tag:#x}) failed: {detail}"
            )))
        }
    }
}
