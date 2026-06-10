//! Bolt message set — request decoding and response construction.
//!
//! Every Bolt message is a PackStream structure: a one-byte signature (tag) and
//! its fields. This module maps the request tags Slater accepts to a typed
//! [`Request`] and builds the response messages ([`success`], [`record`],
//! [`failure`], [`ignored`]) as PackStream structs. Framing (chunking) is the
//! [`super::chunk`] layer; [`to_wire`] composes the two for a ready-to-send
//! message.
//!
//! Slater is **read-only**, so `BEGIN` is accepted but only a read transaction is
//! ever run; write-implying messages have no tags here and any unknown tag is
//! rejected with a `FAILURE` by the connection loop.

use anyhow::{bail, Result};

use super::chunk;
use super::packstream::{self, PsValue};

/// Bolt message signature bytes.
pub mod tag {
    pub const HELLO: u8 = 0x01;
    pub const GOODBYE: u8 = 0x02;
    pub const RESET: u8 = 0x0F;
    pub const RUN: u8 = 0x10;
    pub const BEGIN: u8 = 0x11;
    pub const COMMIT: u8 = 0x12;
    pub const ROLLBACK: u8 = 0x13;
    pub const DISCARD: u8 = 0x2F;
    pub const PULL: u8 = 0x3F;
    pub const LOGON: u8 = 0x6A;
    pub const LOGOFF: u8 = 0x6B;

    pub const SUCCESS: u8 = 0x70;
    pub const RECORD: u8 = 0x71;
    pub const IGNORED: u8 = 0x7E;
    pub const FAILURE: u8 = 0x7F;
}

/// A decoded client request. Map-bearing messages keep their metadata as a
/// `PsValue::Map` so the connection loop can read whichever keys it needs.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    /// `HELLO {user_agent, ...}` — connection metadata (auth moved to `LOGON` in 5.x).
    Hello(PsValue),
    /// `LOGON {scheme, principal, credentials}`.
    Logon(PsValue),
    /// `LOGOFF`.
    Logoff,
    /// `RUN query params extra`.
    Run {
        query: String,
        params: PsValue,
        extra: PsValue,
    },
    /// `PULL {n, qid}`.
    Pull(PsValue),
    /// `DISCARD {n, qid}`.
    Discard(PsValue),
    /// `BEGIN {db, mode, ...}` — only read mode is honoured.
    Begin(PsValue),
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `RESET`.
    Reset,
    /// `GOODBYE`.
    Goodbye,
}

/// Decode a single request from a **de-chunked** message body.
pub fn decode_request(body: &[u8]) -> Result<Request> {
    let (tag, mut fields) = match packstream::from_slice(body)? {
        PsValue::Struct { tag, fields } => (tag, fields),
        other => bail!("bolt: expected a message struct, got {other:?}"),
    };

    let expect = |n: usize, name: &str| -> Result<()> {
        if fields.len() != n {
            bail!("bolt: {name} expects {n} field(s), got {}", fields.len());
        }
        Ok(())
    };

    match tag {
        tag::HELLO => {
            expect(1, "HELLO")?;
            Ok(Request::Hello(take_map(fields.pop().unwrap(), "HELLO")?))
        }
        tag::LOGON => {
            expect(1, "LOGON")?;
            Ok(Request::Logon(take_map(fields.pop().unwrap(), "LOGON")?))
        }
        tag::LOGOFF => {
            expect(0, "LOGOFF")?;
            Ok(Request::Logoff)
        }
        tag::RUN => {
            expect(3, "RUN")?;
            let extra = take_map(fields.pop().unwrap(), "RUN.extra")?;
            let params = take_map(fields.pop().unwrap(), "RUN.params")?;
            let query = match fields.pop().unwrap() {
                PsValue::String(s) => s,
                other => bail!("bolt: RUN query must be a string, got {other:?}"),
            };
            Ok(Request::Run {
                query,
                params,
                extra,
            })
        }
        tag::PULL => {
            expect(1, "PULL")?;
            Ok(Request::Pull(take_map(fields.pop().unwrap(), "PULL")?))
        }
        tag::DISCARD => {
            expect(1, "DISCARD")?;
            Ok(Request::Discard(take_map(
                fields.pop().unwrap(),
                "DISCARD",
            )?))
        }
        tag::BEGIN => {
            expect(1, "BEGIN")?;
            Ok(Request::Begin(take_map(fields.pop().unwrap(), "BEGIN")?))
        }
        tag::COMMIT => {
            expect(0, "COMMIT")?;
            Ok(Request::Commit)
        }
        tag::ROLLBACK => {
            expect(0, "ROLLBACK")?;
            Ok(Request::Rollback)
        }
        tag::RESET => {
            expect(0, "RESET")?;
            Ok(Request::Reset)
        }
        tag::GOODBYE => {
            expect(0, "GOODBYE")?;
            Ok(Request::Goodbye)
        }
        other => bail!("bolt: unsupported message tag 0x{other:02X}"),
    }
}

/// Ensure a field is a `Map`, returning it unchanged.
fn take_map(v: PsValue, ctx: &str) -> Result<PsValue> {
    match v {
        m @ PsValue::Map(_) => Ok(m),
        other => bail!("bolt: {ctx} expected a map, got {other:?}"),
    }
}

// ── Response construction ───────────────────────────────────────────────────

/// `SUCCESS {metadata}`.
pub fn success(metadata: Vec<(String, PsValue)>) -> PsValue {
    PsValue::Struct {
        tag: tag::SUCCESS,
        fields: vec![PsValue::Map(metadata)],
    }
}

/// `RECORD [values]`.
pub fn record(values: Vec<PsValue>) -> PsValue {
    PsValue::Struct {
        tag: tag::RECORD,
        fields: vec![PsValue::List(values)],
    }
}

/// `FAILURE {code, message}`.
pub fn failure(code: &str, message: &str) -> PsValue {
    PsValue::Struct {
        tag: tag::FAILURE,
        fields: vec![PsValue::Map(vec![
            ("code".into(), PsValue::str(code)),
            ("message".into(), PsValue::str(message)),
        ])],
    }
}

/// `IGNORED` — sent for messages received while the connection is in FAILED state.
pub fn ignored() -> PsValue {
    PsValue::Struct {
        tag: tag::IGNORED,
        fields: vec![],
    }
}

/// PackStream-encode a message and frame it into chunks, ready to write.
pub fn to_wire(msg: &PsValue) -> Vec<u8> {
    chunk::frame(&packstream::to_vec(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round a request through encode → frame → de-frame → decode.
    fn through(msg: PsValue) -> Request {
        let wire = to_wire(&msg);
        let (body, consumed) = chunk::decode_complete(&wire).unwrap();
        assert_eq!(consumed, wire.len());
        decode_request(&body).unwrap()
    }

    fn map(pairs: &[(&str, PsValue)]) -> PsValue {
        PsValue::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn decodes_hello_and_logon() {
        let hello = PsValue::Struct {
            tag: tag::HELLO,
            fields: vec![map(&[("user_agent", PsValue::str("neo4j-python/5.0"))])],
        };
        match through(hello) {
            Request::Hello(m) => {
                assert_eq!(
                    m.get("user_agent").and_then(PsValue::as_str),
                    Some("neo4j-python/5.0")
                );
            }
            other => panic!("expected Hello, got {other:?}"),
        }

        let logon = PsValue::Struct {
            tag: tag::LOGON,
            fields: vec![map(&[
                ("scheme", PsValue::str("basic")),
                ("principal", PsValue::str("alice")),
                ("credentials", PsValue::str("s3cret")),
            ])],
        };
        match through(logon) {
            Request::Logon(m) => {
                assert_eq!(m.get("principal").and_then(PsValue::as_str), Some("alice"));
            }
            other => panic!("expected Logon, got {other:?}"),
        }
    }

    #[test]
    fn decodes_run_with_params() {
        let run = PsValue::Struct {
            tag: tag::RUN,
            fields: vec![
                PsValue::str("MATCH (n:Person) RETURN n.name"),
                map(&[("limit", PsValue::Int(10))]),
                map(&[("db", PsValue::str("people"))]),
            ],
        };
        match through(run) {
            Request::Run {
                query,
                params,
                extra,
            } => {
                assert_eq!(query, "MATCH (n:Person) RETURN n.name");
                assert_eq!(params.get("limit").and_then(PsValue::as_int), Some(10));
                assert_eq!(extra.get("db").and_then(PsValue::as_str), Some("people"));
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn decodes_zero_field_control_messages() {
        for (tag, want) in [
            (tag::COMMIT, Request::Commit),
            (tag::ROLLBACK, Request::Rollback),
            (tag::RESET, Request::Reset),
            (tag::GOODBYE, Request::Goodbye),
            (tag::LOGOFF, Request::Logoff),
        ] {
            let msg = PsValue::Struct {
                tag,
                fields: vec![],
            };
            assert_eq!(through(msg), want);
        }
    }

    #[test]
    fn decodes_pull_and_discard() {
        let pull = PsValue::Struct {
            tag: tag::PULL,
            fields: vec![map(&[("n", PsValue::Int(-1)), ("qid", PsValue::Int(-1))])],
        };
        match through(pull) {
            Request::Pull(m) => assert_eq!(m.get("n").and_then(PsValue::as_int), Some(-1)),
            other => panic!("expected Pull, got {other:?}"),
        }
    }

    #[test]
    fn responses_encode_to_decodable_structs() {
        // SUCCESS with field metadata for a RUN.
        let succ = success(vec![(
            "fields".into(),
            PsValue::List(vec![PsValue::str("n.name")]),
        )]);
        let wire = to_wire(&succ);
        let (body, _) = chunk::decode_complete(&wire).unwrap();
        let decoded = packstream::from_slice(&body).unwrap();
        match decoded {
            PsValue::Struct { tag, fields } => {
                assert_eq!(tag, tag::SUCCESS);
                assert_eq!(fields.len(), 1);
            }
            other => panic!("expected struct, got {other:?}"),
        }

        // RECORD round-trips its value list.
        let rec = record(vec![PsValue::str("Alice"), PsValue::Int(30)]);
        let wire = to_wire(&rec);
        let (body, _) = chunk::decode_complete(&wire).unwrap();
        match packstream::from_slice(&body).unwrap() {
            PsValue::Struct { tag, fields } => {
                assert_eq!(tag, tag::RECORD);
                assert_eq!(
                    fields,
                    vec![PsValue::List(vec![PsValue::str("Alice"), PsValue::Int(30)])]
                );
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn failure_carries_code_and_message() {
        let f = failure("Neo.ClientError.Statement.SyntaxError", "boom");
        match f {
            PsValue::Struct { tag, ref fields } => {
                assert_eq!(tag, tag::FAILURE);
                let m = &fields[0];
                assert_eq!(
                    m.get("code").and_then(PsValue::as_str),
                    Some("Neo.ClientError.Statement.SyntaxError")
                );
                assert_eq!(m.get("message").and_then(PsValue::as_str), Some("boom"));
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_tag_and_bad_arity() {
        // Unknown tag.
        let bad = to_wire(&PsValue::Struct {
            tag: 0x55,
            fields: vec![],
        });
        let (body, _) = chunk::decode_complete(&bad).unwrap();
        assert!(decode_request(&body).is_err());

        // RUN with the wrong field count.
        let bad_run = to_wire(&PsValue::Struct {
            tag: tag::RUN,
            fields: vec![PsValue::str("RETURN 1")],
        });
        let (body, _) = chunk::decode_complete(&bad_run).unwrap();
        assert!(decode_request(&body).is_err());
    }
}
