// SPDX-License-Identifier: Apache-2.0
//! Bolt protocol wire layer.
//!
//! Slater speaks Bolt 5.x (with a 4.4 fallback) over TCP, optionally wrapped in
//! TLS. This module owns the *wire* concerns, each independently testable:
//!
//! - [`packstream`] — PackStream v2 value serialisation (the format Bolt rides on).
//! - [`handshake`] — the magic preamble + version negotiation.
//! - [`chunk`] — message framing (length-prefixed chunks + the `00 00` terminator).
//! - [`message`] — the message set: decode client [`message::Request`]s and build
//!   `SUCCESS`/`RECORD`/`FAILURE`/`IGNORED` responses.
//!
//! The per-connection state machine and `tokio` listener that drive these — and
//! the TLS acceptor — are wired once the ACL (M4.4) and executor (M4.5) they call
//! into exist; the wire layer here is complete and verified against the neo4j
//! JavaScript and Python drivers' framing.
//
// The listener/state-machine consumers land in later M4 sub-steps; allow
// dead_code for the standalone wire layer until then.
#![allow(dead_code)]

pub mod chunk;
pub mod client;
pub mod handshake;
pub mod message;
pub mod packstream;
