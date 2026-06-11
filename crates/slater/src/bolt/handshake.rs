// SPDX-License-Identifier: Apache-2.0
//! Bolt connection handshake and version negotiation.
//!
//! A Bolt client opens with a 4-byte magic preamble followed by **four** 4-byte
//! version proposals, best first. The server replies with the single 4-byte
//! version it agrees to, or four zero bytes if it supports none of them.
//!
//! Each proposal is `[0x00, range, minor, major]` (Bolt ≥ 4.3): `range` lets a
//! client offer a span of minors `[minor - range, minor]` in one slot, so a
//! single proposal `00 02 04 05` offers 5.4, 5.3 and 5.2. We support **5.4 with a
//! 4.4 fallback** (the plan's target); negotiation picks the highest supported
//! version that any proposal covers.

/// The Bolt magic preamble: `0x60 0x60 0xB0 0x17`.
pub const PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Versions this server speaks, in descending preference. `(major, minor)`.
///
/// `(4, 1)` is accepted for the older `neo4rs` Rust driver (frozen at Bolt
/// 4.0/4.1), which proposes only 4.1/4.0 and would otherwise get `NO_VERSION`.
/// A 4.1 session reuses the 4.4 fallback paths verbatim: auth is read from HELLO
/// (not version-gated, see `server::handle_request`), the message loop is
/// version-agnostic, and the only version-branched encoding is Node/Relationship
/// `element_id` (Bolt ≥ 5 only) — which read projections never emit.
pub const SUPPORTED: [(u8, u8); 3] = [(5, 4), (4, 4), (4, 1)];

/// The four zero bytes a server sends when it supports none of the proposals.
pub const NO_VERSION: [u8; 4] = [0, 0, 0, 0];

/// One client version proposal, decoded from its 4 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposal {
    pub major: u8,
    pub minor: u8,
    /// How many minor versions below `minor` the client also accepts.
    pub range: u8,
}

impl Proposal {
    /// Decode a 4-byte proposal `[0, range, minor, major]`.
    pub fn from_bytes(b: [u8; 4]) -> Self {
        Proposal {
            major: b[3],
            minor: b[2],
            range: b[1],
        }
    }

    /// Does this proposal cover exactly `(major, minor)`?
    pub fn covers(&self, major: u8, minor: u8) -> bool {
        self.major == major && minor <= self.minor && minor + self.range >= self.minor
    }
}

/// Pick the agreed version from four client proposals, returning the 4 reply
/// bytes (`[0, 0, minor, major]`) or [`NO_VERSION`] if nothing matches.
pub fn negotiate(proposals: &[Proposal; 4]) -> [u8; 4] {
    for &(major, minor) in &SUPPORTED {
        if proposals.iter().any(|p| p.covers(major, minor)) {
            return [0, 0, minor, major];
        }
    }
    NO_VERSION
}

/// Parse the 20-byte client hello (`PREAMBLE` + four proposals) and negotiate.
/// Returns the reply bytes; an `Err` only if the preamble is wrong (not a Bolt
/// client) — a valid hello with no common version still returns [`NO_VERSION`].
pub fn handle_client_hello(buf: &[u8; 20]) -> anyhow::Result<[u8; 4]> {
    if buf[..4] != PREAMBLE {
        anyhow::bail!("not a Bolt connection: bad preamble {:02X?}", &buf[..4]);
    }
    let mut proposals = [Proposal {
        major: 0,
        minor: 0,
        range: 0,
    }; 4];
    for (i, p) in proposals.iter_mut().enumerate() {
        let off = 4 + i * 4;
        *p = Proposal::from_bytes(buf[off..off + 4].try_into().unwrap());
    }
    Ok(negotiate(&proposals))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 4-byte proposal in wire order `[0, range, minor, major]`.
    fn prop(major: u8, minor: u8, range: u8) -> [u8; 4] {
        [0, range, minor, major]
    }

    fn hello(props: [[u8; 4]; 4]) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[..4].copy_from_slice(&PREAMBLE);
        for (i, p) in props.iter().enumerate() {
            buf[4 + i * 4..8 + i * 4].copy_from_slice(p);
        }
        buf
    }

    #[test]
    fn picks_5_4_when_offered() {
        let buf = hello([prop(5, 4, 0), prop(4, 4, 0), prop(4, 3, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 4, 5]);
    }

    #[test]
    fn falls_back_to_4_4() {
        let buf = hello([prop(4, 4, 0), prop(4, 3, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 4, 4]);
    }

    #[test]
    fn honours_range_span() {
        // One proposal offering 5.4..5.2 should still match our 5.4.
        let buf = hello([prop(5, 4, 2), prop(0, 0, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 4, 5]);
        // A range that stops above 4.4 (offers 5.2..5.1) matches nothing we speak.
        let buf = hello([prop(5, 2, 1), prop(0, 0, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), NO_VERSION);
    }

    #[test]
    fn prefers_5_4_over_4_4() {
        // Both offered; the server's preference order wins.
        let buf = hello([prop(4, 4, 0), prop(5, 4, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 4, 5]);
    }

    #[test]
    fn accepts_neo4rs_4_1_proposal() {
        // The `neo4rs` Rust driver proposes exactly [4.1, 4.0, 0, 0]; we agree on 4.1.
        let buf = hello([prop(4, 1, 0), prop(4, 0, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 1, 4]);
    }

    #[test]
    fn prefers_4_4_over_4_1() {
        // A client offering both still gets the higher-preference 4.4.
        let buf = hello([prop(4, 1, 0), prop(4, 4, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), [0, 0, 4, 4]);
    }

    #[test]
    fn no_common_version() {
        let buf = hello([prop(3, 0, 0), prop(1, 0, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        assert_eq!(handle_client_hello(&buf).unwrap(), NO_VERSION);
    }

    #[test]
    fn rejects_bad_preamble() {
        let mut buf = hello([prop(5, 4, 0), prop(0, 0, 0), prop(0, 0, 0), prop(0, 0, 0)]);
        buf[0] = 0xFF;
        assert!(handle_client_hello(&buf).is_err());
    }
}
