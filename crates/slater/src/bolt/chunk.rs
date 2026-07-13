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

/// Largest reassembled message body we will accept. A Bolt message is a query
/// string plus a parameter map; legitimate ones are far smaller than this. The
/// cap exists so a peer cannot stream chunks endlessly (or withhold the
/// terminating `00 00`) and drive the reassembly buffer to OOM — a pre-auth
/// denial-of-service, since this runs before `LOGON`.
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

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
    decode_message_capped(buf, MAX_MESSAGE_BYTES)
}

/// As [`decode_message`], but with an explicit body cap. The server passes a
/// per-connection cap here (small before `LOGON`, generous after) so the
/// pre-auth reassembly budget can be far tighter than the authenticated one; the
/// explicit cap also keeps the limit unit-testable without a multi-megabyte buffer.
pub fn decode_message_capped(buf: &[u8], max_body: usize) -> Result<Option<(Vec<u8>, usize)>> {
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
        if body.len() + len > max_body {
            bail!("Bolt message exceeds the {max_body}-byte limit; closing connection");
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

/// A **resumable** Bolt message reassembler.
///
/// [`decode_message_capped`] restarts from the front of the buffer on every call, so a
/// framer that retries it as bytes trickle in re-scans (and re-copies) everything already
/// received — O(n²) work for a large message that arrives in many small reads. This keeps a
/// cursor into the caller's buffer plus the partial body across calls, so each chunk-payload
/// byte is copied into the body exactly once (O(n)).
///
/// Usage mirrors the framer's read loop: the caller appends bytes to its own buffer and calls
/// [`feed`](Self::feed) with the whole buffer (front **not** yet drained). On `Some`, the
/// reassembler has reset for the next message and the caller drains `consumed` bytes.
#[derive(Default)]
pub struct ChunkReassembler {
    /// The in-progress message body assembled so far (across chunks).
    body: Vec<u8>,
    /// Offset in the caller's buffer of the next chunk header not yet folded into `body`.
    /// Persists across `feed` calls (the resumable cursor); reset to 0 when a message
    /// completes and the caller drains.
    pos: usize,
    /// Test-only: total payload bytes copied into `body`. Proves the single-copy (O(n))
    /// property — for one message it equals the final body length.
    #[cfg(test)]
    copied: usize,
}

impl ChunkReassembler {
    /// A fresh reassembler with an empty body and a zero cursor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resume reassembly against `buf` (the caller's whole receive buffer). Returns the
    /// reassembled body plus the number of bytes consumed from the front of `buf` (chunk
    /// headers + payloads + the terminating `00 00`) once a message completes, or `Ok(None)`
    /// when more bytes are needed. On `Some` the reassembler is reset for the next message.
    pub fn feed(&mut self, buf: &[u8], max_body: usize) -> Result<Option<(Vec<u8>, usize)>> {
        loop {
            if self.pos + 2 > buf.len() {
                return Ok(None); // header not fully arrived; resume from `pos` next call
            }
            let len = u16::from_be_bytes([buf[self.pos], buf[self.pos + 1]]) as usize;
            if len == 0 {
                // End-of-message marker: hand back the body and reset for the next message.
                let consumed = self.pos + 2;
                let body = std::mem::take(&mut self.body);
                self.pos = 0;
                return Ok(Some((body, consumed)));
            }
            if self.pos + 2 + len > buf.len() {
                return Ok(None); // payload not fully arrived; resume from this header
            }
            if self.body.len() + len > max_body {
                bail!("Bolt message exceeds the {max_body}-byte limit; closing connection");
            }
            self.body
                .extend_from_slice(&buf[self.pos + 2..self.pos + 2 + len]);
            #[cfg(test)]
            {
                self.copied += len;
            }
            self.pos += 2 + len;
        }
    }

    /// Test-only: total payload bytes copied into the body since construction.
    #[cfg(test)]
    pub fn bytes_copied(&self) -> usize {
        self.copied
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
    fn oversized_message_is_refused() {
        // Two 3-byte chunks (body 6) under a 4-byte cap must error rather than
        // accumulate — the guard that stops a chunk-flood / withheld-terminator OOM.
        let mut stream = Vec::new();
        for _ in 0..2 {
            stream.extend_from_slice(&3u16.to_be_bytes());
            stream.extend_from_slice(b"abc");
        }
        stream.extend_from_slice(&[0, 0]);
        assert!(decode_message_capped(&stream, 4).is_err());
        // The same stream is fine under a cap that admits it.
        assert!(decode_message_capped(&stream, 64).unwrap().is_some());
    }

    #[test]
    fn reassembler_copies_each_byte_once_across_trickled_reads() {
        // A large multi-chunk message arriving one byte at a time must reassemble
        // correctly and copy each payload byte exactly once. Pre-fix the framer re-ran
        // `decode_message_capped` from the buffer front on every read, re-copying the whole
        // accumulated body each time — O(n²) copies; here the cursor advances so total
        // copied == body length.
        let body: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
        let framed = frame(&body);

        let mut re = ChunkReassembler::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut result = None;
        for (i, b) in framed.iter().enumerate() {
            buf.push(*b);
            match re.feed(&buf, MAX_MESSAGE_BYTES).unwrap() {
                Some((got, consumed)) => {
                    assert_eq!(i + 1, framed.len(), "completes only on the final byte");
                    assert_eq!(consumed, framed.len());
                    result = Some(got);
                    break;
                }
                None => {}
            }
        }
        assert_eq!(result.unwrap(), body, "reassembled body matches");
        assert_eq!(
            re.bytes_copied(),
            body.len(),
            "each payload byte copied exactly once (no quadratic re-copy)"
        );
    }

    #[test]
    fn reassembler_handles_back_to_back_and_caps() {
        // Two messages in one buffer: decode the first, drain, decode the second.
        let mut re = ChunkReassembler::new();
        let mut stream = frame(b"first");
        stream.extend_from_slice(&frame(b"second"));
        let (m1, c1) = re.feed(&stream, 64).unwrap().unwrap();
        assert_eq!(m1, b"first");
        let rest = &stream[c1..];
        let (m2, _) = re.feed(rest, 64).unwrap().unwrap();
        assert_eq!(m2, b"second");

        // The body cap still trips mid-stream, exactly as the one-shot decoder's does.
        let mut over = ChunkReassembler::new();
        let mut flood = Vec::new();
        for _ in 0..2 {
            flood.extend_from_slice(&3u16.to_be_bytes());
            flood.extend_from_slice(b"abc");
        }
        flood.extend_from_slice(&[0, 0]);
        assert!(over.feed(&flood, 4).is_err());
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
