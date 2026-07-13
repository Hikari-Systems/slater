// SPDX-License-Identifier: Apache-2.0
//! Low-level wire encoding shared by the property store and the indexes.
//!
//! Integers use LEB128 unsigned varints (and zig-zag for signed), which keeps
//! the dense node/edge ids and small counts compact and zstd-friendly.
//!
// DESIGN: string property *values* are encoded inline here (tag 4), not via a
// global value dictionary. A matched entity's whole property record then lives in
// a single block — materialising it for a RETURN map projection costs one block
// read, with no extra dictionary lookups on the hot path. zstd still collapses the
// repetition within a block. Labels, relationship types and property *keys* (the
// small, bounded symbol sets) are interned to ints in the MANIFEST instead.

use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::ids::Value;

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_STR: u8 = 4;
const TAG_LIST: u8 = 5;
const TAG_VECTOR: u8 = 6;

/// Append an unsigned LEB128 varint.
pub fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Bytes [`write_uvarint`] would append for `v`, without encoding it. Lets a
/// caller size a record buffer exactly before filling it.
pub fn uvarint_len(v: u64) -> usize {
    let bits = (u64::BITS - v.leading_zeros()).max(1) as usize;
    bits.div_ceil(7)
}

/// Read an unsigned LEB128 varint, advancing the slice.
pub fn read_uvarint(r: &mut &[u8]) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let Some((&byte, rest)) = r.split_first() else {
            bail!("varint truncated");
        };
        *r = rest;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
}

#[inline]
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Encode a property value inline.
pub fn write_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(*b as u8);
        }
        Value::Int(i) => {
            buf.push(TAG_INT);
            write_uvarint(buf, zigzag(*i));
        }
        Value::Float(f) => {
            buf.push(TAG_FLOAT);
            buf.write_f64::<LittleEndian>(*f).unwrap();
        }
        Value::Str(s) => {
            buf.push(TAG_STR);
            write_uvarint(buf, s.len() as u64);
            buf.extend_from_slice(s.as_bytes());
        }
        Value::List(items) => {
            buf.push(TAG_LIST);
            write_uvarint(buf, items.len() as u64);
            for it in items {
                write_value(buf, it);
            }
        }
        Value::Vector(xs) => {
            buf.push(TAG_VECTOR);
            write_uvarint(buf, xs.len() as u64);
            for x in xs {
                buf.write_f32::<LittleEndian>(*x).unwrap();
            }
        }
    }
}

/// Advance the slice past one encoded value without materialising it. Used by
/// the single-key property reader (`columns::decode_one`) to skip the values of
/// keys the caller didn't ask for — a string/list/vector is stepped over rather
/// than allocated, so reading one property of a many-property record costs no
/// per-value heap allocation.
pub fn skip_value(r: &mut &[u8]) -> Result<()> {
    let Some((&tag, rest)) = r.split_first() else {
        bail!("value truncated (no tag)");
    };
    *r = rest;
    match tag {
        TAG_NULL => {}
        TAG_BOOL => {
            let Some((_, rest)) = r.split_first() else {
                bail!("bool truncated");
            };
            *r = rest;
        }
        TAG_INT => {
            read_uvarint(r)?;
        }
        TAG_FLOAT => skip_bytes(r, 8)?,
        TAG_STR => {
            let len = read_uvarint(r)? as usize;
            skip_bytes(r, len)?;
        }
        TAG_LIST => {
            let n = read_uvarint(r)? as usize;
            for _ in 0..n {
                skip_value(r)?;
            }
        }
        TAG_VECTOR => {
            let n = read_uvarint(r)? as usize;
            skip_bytes(
                r,
                n.checked_mul(4)
                    .ok_or_else(|| anyhow::anyhow!("vector too long"))?,
            )?;
        }
        other => bail!("unknown value tag {other}"),
    }
    Ok(())
}

#[inline]
fn skip_bytes(r: &mut &[u8], n: usize) -> Result<()> {
    if r.len() < n {
        bail!("value truncated");
    }
    *r = &r[n..];
    Ok(())
}

/// Decode a property value, advancing the slice.
pub fn read_value(r: &mut &[u8]) -> Result<Value> {
    let Some((&tag, rest)) = r.split_first() else {
        bail!("value truncated (no tag)");
    };
    *r = rest;
    Ok(match tag {
        TAG_NULL => Value::Null,
        TAG_BOOL => {
            let Some((&b, rest)) = r.split_first() else {
                bail!("bool truncated");
            };
            *r = rest;
            Value::Bool(b != 0)
        }
        TAG_INT => Value::Int(unzigzag(read_uvarint(r)?)),
        TAG_FLOAT => Value::Float(r.read_f64::<LittleEndian>()?),
        TAG_STR => {
            let len = read_uvarint(r)? as usize;
            if r.len() < len {
                bail!("string truncated");
            }
            let (s, rest) = r.split_at(len);
            *r = rest;
            Value::Str(String::from_utf8(s.to_vec())?)
        }
        TAG_LIST => {
            let n = read_uvarint(r)? as usize;
            // `n` is an untrusted uvarint; each element is ≥1 byte (a value tag), so
            // a valid list of `n` items needs ≥`n` bytes remaining. Cap the
            // pre-allocation at the bytes actually left so a forged huge length can't
            // drive an out-of-memory allocation before the loop's first short read
            // bails — same discipline as `packstream::read_list`.
            let mut items = Vec::with_capacity(n.min(r.len()));
            for _ in 0..n {
                items.push(read_value(r)?);
            }
            Value::List(items)
        }
        TAG_VECTOR => {
            let n = read_uvarint(r)? as usize;
            // Each element is a 4-byte `f32`, so bound the pre-allocation by
            // `remaining / 4` (see `TAG_LIST`) against a forged length.
            let mut xs = Vec::with_capacity(n.min(r.len() / 4));
            for _ in 0..n {
                xs.push(r.read_f32::<LittleEndian>()?);
            }
            Value::Vector(xs)
        }
        other => bail!("unknown value tag {other}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_uvarint(&mut buf, v);
            let mut r = &buf[..];
            assert_eq!(read_uvarint(&mut r).unwrap(), v);
            assert!(r.is_empty());
        }
    }

    /// `uvarint_len` sizes a buffer that `write_uvarint` then fills exactly, so it
    /// has to agree with the encoder on every boundary, not approximately.
    #[test]
    fn uvarint_len_agrees_with_encoder() {
        let mut cases: Vec<u64> = vec![0, 1, u64::MAX];
        for shift in 0..64u32 {
            let b = 1u64 << shift;
            cases.extend([b.wrapping_sub(1), b, b.wrapping_add(1)]);
        }
        for v in cases {
            let mut buf = Vec::new();
            write_uvarint(&mut buf, v);
            assert_eq!(uvarint_len(v), buf.len(), "uvarint_len({v})");
        }
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, -1, 1, -1000, 1000, i64::MIN, i64::MAX] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    #[test]
    fn value_roundtrip_all_kinds() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(-42),
            Value::Int(1_000_000),
            Value::Float(-0.0195908118),
            Value::Str("Camelus dromedarius \" \n |pipe|".into()),
            Value::List(vec![
                Value::Str("a".into()),
                Value::Str("b".into()),
                Value::Str("Whitehead-2024".into()),
            ]),
            Value::Vector(vec![-0.5, 0.25, 1.0, 0.0]),
        ];
        for v in &values {
            let mut buf = Vec::new();
            write_value(&mut buf, v);
            let mut r = &buf[..];
            assert_eq!(&read_value(&mut r).unwrap(), v);
            assert!(r.is_empty(), "leftover bytes for {v:?}");
        }
    }
}
