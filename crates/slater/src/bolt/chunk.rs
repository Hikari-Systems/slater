// SPDX-License-Identifier: Apache-2.0
//! Bolt message chunking.
//!
//! After the handshake, every Bolt message travels as one or more **chunks**: a
//! 2-byte big-endian length header followed by that many bytes. A message ends
//! with a zero-length chunk (`0x00 0x00`). A single logical message may therefore
//! span several chunks, and the chunk boundaries are independent of the
//! PackStream structure inside.

use anyhow::{bail, Result};

/// Largest payload carried by one chunk (the 16-bit length ceiling).
pub const MAX_CHUNK: usize = u16::MAX as usize;

/// Frame a complete message body into chunks plus the terminating `00 00`.
pub fn encode_message(body: &[u8], out: &mut Vec<u8>) {
    for piece in body.chunks(MAX_CHUNK.max(1)) {
        out.extend_from_slice(&(piece.len() as u16).to_be_bytes());
        out.extend_from_slice(piece);
    }
    // End-of-message marker.
    out.extend_from_slice(&[0x00, 0x00]);
}

/// Frame a message body into a fresh `Vec`.
pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 4);
    encode_message(body, &mut out);
    out
}

/// Pull the next complete message body out of `buf`, returning the reassembled
/// body and the number of bytes consumed (including all chunk headers and the
/// terminator). Returns `Ok(None)` if `buf` does not yet hold a full message —
/// the caller should read more bytes and retry.
pub fn decode_message(buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>> {
    let mut pos = 0;
    let mut body = Vec::new();
    loop {
        if pos + 2 > buf.len() {
            return Ok(None); // header not fully arrived yet
        }
        let len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if len == 0 {
            // End-of-message marker.
            return Ok(Some((body, pos)));
        }
        if pos + len > buf.len() {
            return Ok(None); // chunk payload not fully arrived yet
        }
        body.extend_from_slice(&buf[pos..pos + len]);
        pos += len;
    }
}

/// Decode a message that must be wholly present, erroring if it is incomplete.
pub fn decode_complete(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    match decode_message(buf)? {
        Some(r) => Ok(r),
        None => bail!("incomplete Bolt message ({} bytes)", buf.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_chunk_roundtrip() {
        let body = b"hello bolt".to_vec();
        let framed = frame(&body);
        // header(2) + body + terminator(2)
        assert_eq!(framed.len(), 2 + body.len() + 2);
        assert_eq!(&framed[..2], &(body.len() as u16).to_be_bytes());
        assert_eq!(&framed[framed.len() - 2..], &[0, 0]);

        let (got, consumed) = decode_complete(&framed).unwrap();
        assert_eq!(got, body);
        assert_eq!(consumed, framed.len());
    }

    #[test]
    fn empty_body_is_just_terminator() {
        let framed = frame(&[]);
        assert_eq!(framed, vec![0, 0]);
        let (got, consumed) = decode_complete(&framed).unwrap();
        assert!(got.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn large_body_spans_multiple_chunks() {
        let body: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
        let framed = frame(&body);
        // Three full chunks (65535 * 3 = 196605) + a remainder chunk + terminator.
        let (got, consumed) = decode_complete(&framed).unwrap();
        assert_eq!(got, body);
        assert_eq!(consumed, framed.len());
        // At least 4 chunk headers were emitted.
        assert!(framed.len() > body.len() + 4);
    }

    #[test]
    fn partial_buffer_returns_none() {
        let body = b"waiting for more".to_vec();
        let framed = frame(&body);
        // Truncate mid-message: decoder must ask for more, not error.
        assert!(decode_message(&framed[..framed.len() - 3])
            .unwrap()
            .is_none());
        // A lone, incomplete header.
        assert!(decode_message(&[0x00]).unwrap().is_none());
    }

    #[test]
    fn two_messages_back_to_back() {
        let mut stream = frame(b"first");
        stream.extend_from_slice(&frame(b"second"));
        let (m1, c1) = decode_complete(&stream).unwrap();
        assert_eq!(m1, b"first");
        let (m2, c2) = decode_complete(&stream[c1..]).unwrap();
        assert_eq!(m2, b"second");
        assert_eq!(c1 + c2, stream.len());
    }
}
