// SPDX-License-Identifier: Apache-2.0
//! PackStream v2 — the value serialisation Bolt rides on.
//!
//! PackStream is a binary format of self-describing markers. Every value begins
//! with a marker byte; integers, strings, lists, maps and structs then carry an
//! inline or sized length. All multi-byte integers are **big-endian**. This is a
//! from-scratch implementation of the subset Bolt needs (no v1 struct_8/struct_16,
//! no temporal/spatial structs — those are encoded as ordinary structs by the
//! caller when needed).
//!
//! Reference: the Neo4j Bolt PackStream v2 specification; verified against the
//! neo4j JavaScript and Python drivers' framing in M4 integration tests.

use anyhow::{bail, Result};

/// A PackStream value. Maps preserve insertion order (a `Vec` of pairs) so the
/// wire encoding is deterministic — handy for tests and for stable metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum PsValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PsValue>),
    Map(Vec<(String, PsValue)>),
    /// A Bolt structure: a one-byte tag (signature) and up to 15 fields.
    Struct {
        tag: u8,
        fields: Vec<PsValue>,
    },
}

impl PsValue {
    /// Convenience: a string value from anything `Into<String>`.
    pub fn str(s: impl Into<String>) -> Self {
        PsValue::String(s.into())
    }

    /// Look a key up in a `Map` value, returning `None` for a non-map or absent key.
    pub fn get(&self, key: &str) -> Option<&PsValue> {
        match self {
            PsValue::Map(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Borrow the inner string, if this is a `String`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PsValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// The inner integer, if this is an `Int`.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PsValue::Int(i) => Some(*i),
            _ => None,
        }
    }
}

// ── Encoding ────────────────────────────────────────────────────────────────

/// Encode a value, appending to `out`.
pub fn encode(v: &PsValue, out: &mut Vec<u8>) {
    match v {
        PsValue::Null => out.push(0xC0),
        PsValue::Bool(false) => out.push(0xC2),
        PsValue::Bool(true) => out.push(0xC3),
        PsValue::Int(i) => encode_int(*i, out),
        PsValue::Float(f) => {
            out.push(0xC1);
            out.extend_from_slice(&f.to_be_bytes());
        }
        PsValue::Bytes(b) => {
            let n = b.len();
            if n <= u8::MAX as usize {
                out.push(0xCC);
                out.push(n as u8);
            } else if n <= u16::MAX as usize {
                out.push(0xCD);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(0xCE);
                out.extend_from_slice(&(n as u32).to_be_bytes());
            }
            out.extend_from_slice(b);
        }
        PsValue::String(s) => {
            let bytes = s.as_bytes();
            let n = bytes.len();
            if n <= 15 {
                out.push(0x80 | n as u8);
            } else if n <= u8::MAX as usize {
                out.push(0xD0);
                out.push(n as u8);
            } else if n <= u16::MAX as usize {
                out.push(0xD1);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(0xD2);
                out.extend_from_slice(&(n as u32).to_be_bytes());
            }
            out.extend_from_slice(bytes);
        }
        PsValue::List(items) => {
            encode_sized_header(items.len(), 0x90, 0xD4, out);
            for it in items {
                encode(it, out);
            }
        }
        PsValue::Map(entries) => {
            encode_sized_header(entries.len(), 0xA0, 0xD8, out);
            for (k, val) in entries {
                encode(&PsValue::String(k.clone()), out);
                encode(val, out);
            }
        }
        PsValue::Struct { tag, fields } => {
            // Bolt messages always have < 16 fields, so only the tiny-struct form
            // (0xB0..0xBF) is emitted.
            debug_assert!(fields.len() <= 15, "struct has too many fields for Bolt");
            out.push(0xB0 | fields.len() as u8);
            out.push(*tag);
            for f in fields {
                encode(f, out);
            }
        }
    }
}

/// Encode a value to a fresh `Vec`.
pub fn to_vec(v: &PsValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode(v, &mut out);
    out
}

/// Header for a list (`tiny_base` 0x90, `wide_base` 0xD4) or map (0xA0/0xD8):
/// `tiny | n` for n ≤ 15, else the 8/16/32-bit sized markers `base, base+1, base+2`.
fn encode_sized_header(n: usize, tiny_base: u8, wide_base: u8, out: &mut Vec<u8>) {
    if n <= 15 {
        out.push(tiny_base | n as u8);
    } else if n <= u8::MAX as usize {
        out.push(wide_base);
        out.push(n as u8);
    } else if n <= u16::MAX as usize {
        out.push(wide_base + 1);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(wide_base + 2);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

/// Encode an integer in the smallest representation PackStream allows.
fn encode_int(v: i64, out: &mut Vec<u8>) {
    if (-16..=127).contains(&v) {
        out.push(v as i8 as u8);
    } else if (i8::MIN as i64..=i8::MAX as i64).contains(&v) {
        out.push(0xC8);
        out.push(v as i8 as u8);
    } else if (i16::MIN as i64..=i16::MAX as i64).contains(&v) {
        out.push(0xC9);
        out.extend_from_slice(&(v as i16).to_be_bytes());
    } else if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
        out.push(0xCA);
        out.extend_from_slice(&(v as i32).to_be_bytes());
    } else {
        out.push(0xCB);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

// ── Decoding ────────────────────────────────────────────────────────────────

/// A cursor over a PackStream byte buffer.
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            bail!(
                "packstream: unexpected end of input (need {n} at {}, have {})",
                self.pos,
                self.buf.len()
            );
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Decode the next value.
    pub fn read_value(&mut self) -> Result<PsValue> {
        let m = self.u8()?;
        match m {
            // Tiny ints.
            0x00..=0x7F => Ok(PsValue::Int(m as i64)),
            0xF0..=0xFF => Ok(PsValue::Int(m as i8 as i64)),
            // Tiny string / list / map.
            0x80..=0x8F => self.read_string((m & 0x0F) as usize),
            0x90..=0x9F => self.read_list((m & 0x0F) as usize),
            0xA0..=0xAF => self.read_map((m & 0x0F) as usize),
            0xB0..=0xBF => self.read_struct((m & 0x0F) as usize),
            0xC0 => Ok(PsValue::Null),
            0xC1 => {
                let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
                Ok(PsValue::Float(f64::from_be_bytes(bytes)))
            }
            0xC2 => Ok(PsValue::Bool(false)),
            0xC3 => Ok(PsValue::Bool(true)),
            0xC8 => Ok(PsValue::Int(self.u8()? as i8 as i64)),
            0xC9 => Ok(PsValue::Int(self.u16()? as i16 as i64)),
            0xCA => Ok(PsValue::Int(self.u32()? as i32 as i64)),
            0xCB => {
                let bytes: [u8; 8] = self.take(8)?.try_into().unwrap();
                Ok(PsValue::Int(i64::from_be_bytes(bytes)))
            }
            0xCC => {
                let n = self.u8()? as usize;
                Ok(PsValue::Bytes(self.take(n)?.to_vec()))
            }
            0xCD => {
                let n = self.u16()? as usize;
                Ok(PsValue::Bytes(self.take(n)?.to_vec()))
            }
            0xCE => {
                let n = self.u32()? as usize;
                Ok(PsValue::Bytes(self.take(n)?.to_vec()))
            }
            0xD0 => {
                let n = self.u8()? as usize;
                self.read_string(n)
            }
            0xD1 => {
                let n = self.u16()? as usize;
                self.read_string(n)
            }
            0xD2 => {
                let n = self.u32()? as usize;
                self.read_string(n)
            }
            0xD4 => {
                let n = self.u8()? as usize;
                self.read_list(n)
            }
            0xD5 => {
                let n = self.u16()? as usize;
                self.read_list(n)
            }
            0xD6 => {
                let n = self.u32()? as usize;
                self.read_list(n)
            }
            0xD8 => {
                let n = self.u8()? as usize;
                self.read_map(n)
            }
            0xD9 => {
                let n = self.u16()? as usize;
                self.read_map(n)
            }
            0xDA => {
                let n = self.u32()? as usize;
                self.read_map(n)
            }
            other => bail!("packstream: unknown marker byte 0x{other:02X}"),
        }
    }

    fn read_string(&mut self, n: usize) -> Result<PsValue> {
        let bytes = self.take(n)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("packstream: invalid UTF-8 string: {e}"))?;
        Ok(PsValue::String(s.to_string()))
    }

    fn read_list(&mut self, n: usize) -> Result<PsValue> {
        // `n` is an attacker-controlled u32; each element is ≥1 byte, so a valid
        // list of `n` items needs ≥`n` bytes of body. Cap the pre-allocation at
        // the bytes actually remaining so a bogus huge length (e.g. `0xD6` with a
        // 2.5-billion count in a 5-byte message) can't drive an out-of-memory
        // allocation before the loop's first short read bails.
        let mut items = Vec::with_capacity(n.min(self.remaining()));
        for _ in 0..n {
            items.push(self.read_value()?);
        }
        Ok(PsValue::List(items))
    }

    fn read_map(&mut self, n: usize) -> Result<PsValue> {
        // See `read_list`: bound the pre-allocation by the remaining bytes (each
        // entry is ≥2 bytes), so a forged length cannot OOM the decoder.
        let mut entries = Vec::with_capacity(n.min(self.remaining()));
        for _ in 0..n {
            let key = match self.read_value()? {
                PsValue::String(s) => s,
                other => bail!("packstream: map key is not a string: {other:?}"),
            };
            let value = self.read_value()?;
            entries.push((key, value));
        }
        Ok(PsValue::Map(entries))
    }

    fn read_struct(&mut self, field_count: usize) -> Result<PsValue> {
        let tag = self.u8()?;
        // See `read_list`: bound the pre-allocation by the remaining bytes so a
        // forged field count cannot OOM the decoder.
        let mut fields = Vec::with_capacity(field_count.min(self.remaining()));
        for _ in 0..field_count {
            fields.push(self.read_value()?);
        }
        Ok(PsValue::Struct { tag, fields })
    }
}

/// Decode exactly one value from `buf`, erroring if there are trailing bytes.
pub fn from_slice(buf: &[u8]) -> Result<PsValue> {
    let mut d = Decoder::new(buf);
    let v = d.read_value()?;
    if d.remaining() != 0 {
        bail!("packstream: {} trailing bytes after value", d.remaining());
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: PsValue) {
        let bytes = to_vec(&v);
        let back = from_slice(&bytes).unwrap();
        assert_eq!(v, back, "roundtrip mismatch; bytes={bytes:02X?}");
    }

    #[test]
    fn forged_length_headers_bail_without_huge_allocation() {
        // Regression (found by the `packstream_decode` fuzz target): a list/map/
        // struct header declaring a ~2.5-billion element count in a tiny message
        // must error on the short body, not pre-allocate gigabytes. With the
        // capacity bounded by the remaining bytes this returns quickly.
        // `0xD6` = list, u32 length 0x959595AF (≈2.5e9), then no body.
        assert!(from_slice(&[0xD6, 0x95, 0x95, 0x95, 0xAF]).is_err());
        // `0xDA` = map, u32 length; `0xDE`?—use 0xDA marker with huge count.
        assert!(from_slice(&[0xDA, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        // `0xB?` tiny-struct is ≤15 fields, but the u32 list path above is the
        // unbounded one; also check a u16 list length with no body.
        assert!(from_slice(&[0xD5, 0xFF, 0xFF]).is_err());
    }

    #[test]
    fn known_encodings_match_spec() {
        assert_eq!(to_vec(&PsValue::Null), vec![0xC0]);
        assert_eq!(to_vec(&PsValue::Bool(true)), vec![0xC3]);
        assert_eq!(to_vec(&PsValue::Bool(false)), vec![0xC2]);
        // Tiny ints.
        assert_eq!(to_vec(&PsValue::Int(0)), vec![0x00]);
        assert_eq!(to_vec(&PsValue::Int(1)), vec![0x01]);
        assert_eq!(to_vec(&PsValue::Int(127)), vec![0x7F]);
        assert_eq!(to_vec(&PsValue::Int(-16)), vec![0xF0]);
        assert_eq!(to_vec(&PsValue::Int(-1)), vec![0xFF]);
        // Wider ints.
        assert_eq!(to_vec(&PsValue::Int(-17)), vec![0xC8, 0xEF]);
        assert_eq!(to_vec(&PsValue::Int(128)), vec![0xC9, 0x00, 0x80]);
        assert_eq!(to_vec(&PsValue::Int(1000)), vec![0xC9, 0x03, 0xE8]);
        assert_eq!(
            to_vec(&PsValue::Int(100_000)),
            vec![0xCA, 0x00, 0x01, 0x86, 0xA0]
        );
        // Strings.
        assert_eq!(to_vec(&PsValue::str("")), vec![0x80]);
        assert_eq!(to_vec(&PsValue::str("A")), vec![0x81, 0x41]);
        // Empty list / map.
        assert_eq!(to_vec(&PsValue::List(vec![])), vec![0x90]);
        assert_eq!(to_vec(&PsValue::Map(vec![])), vec![0xA0]);
    }

    #[test]
    fn int_boundaries_roundtrip() {
        for v in [
            0i64,
            1,
            -1,
            -16,
            127,
            -17,
            128,
            -128,
            255,
            256,
            -129,
            32_767,
            -32_768,
            32_768,
            i32::MAX as i64,
            i32::MIN as i64,
            i32::MAX as i64 + 1,
            i64::MAX,
            i64::MIN,
        ] {
            roundtrip(PsValue::Int(v));
        }
    }

    #[test]
    fn float_roundtrips() {
        for f in [0.0, 1.5, -2.25, std::f64::consts::PI, 1e-9, 1e300] {
            roundtrip(PsValue::Float(f));
        }
    }

    #[test]
    fn sized_strings_lists_maps_roundtrip() {
        // String lengths spanning tiny / 8-bit / 16-bit markers.
        for len in [0usize, 1, 15, 16, 255, 256, 70_000] {
            roundtrip(PsValue::String("x".repeat(len)));
        }
        // List with > 15 items forces the wide header.
        let big_list = PsValue::List((0..300).map(PsValue::Int).collect());
        roundtrip(big_list);
        // Map with > 15 entries.
        let big_map = PsValue::Map(
            (0..20)
                .map(|i| (format!("k{i}"), PsValue::Int(i)))
                .collect(),
        );
        roundtrip(big_map);
    }

    #[test]
    fn nested_and_struct_roundtrip() {
        let v = PsValue::Struct {
            tag: 0x71,
            fields: vec![PsValue::List(vec![
                PsValue::Map(vec![
                    ("name".into(), PsValue::str("Alice")),
                    ("age".into(), PsValue::Int(30)),
                    (
                        "tags".into(),
                        PsValue::List(vec![PsValue::str("a"), PsValue::Null]),
                    ),
                ]),
                PsValue::Bytes(vec![1, 2, 3, 254]),
                PsValue::Bool(true),
            ])],
        };
        roundtrip(v);
    }

    #[test]
    fn map_lookup_helpers() {
        let m = PsValue::Map(vec![
            ("scheme".into(), PsValue::str("basic")),
            ("n".into(), PsValue::Int(-1)),
        ]);
        assert_eq!(m.get("scheme").and_then(PsValue::as_str), Some("basic"));
        assert_eq!(m.get("n").and_then(PsValue::as_int), Some(-1));
        assert!(m.get("missing").is_none());
    }

    #[test]
    fn rejects_trailing_and_unknown() {
        assert!(from_slice(&[0x01, 0x02]).is_err()); // trailing byte
        assert!(from_slice(&[0xC4]).is_err()); // unused marker
        assert!(from_slice(&[0x81]).is_err()); // truncated string
    }
}
