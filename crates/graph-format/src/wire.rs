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

/// A decoder refused a length claim it read off disk.
///
/// Typed so a caller classifies it with `err.downcast_ref::<DecodeRejected>()` rather than
/// matching the message text: like [`crate::codec::BlockSizeExceeded`], this says *the image
/// is corrupt or hostile*, which is a different thing from an I/O error and a caller may want
/// to tell the two apart.
#[derive(Debug, thiserror::Error)]
pub enum DecodeRejected {
    /// A length/count product overflowed, so the bound derived from it is meaningless. A
    /// forged varint that wraps `n * elem_size` to a *small* number would otherwise sail
    /// through a length check and then be `with_capacity`'d at full width.
    #[error("{what}: length {n} × {elem} overflows")]
    LengthOverflow {
        what: &'static str,
        n: u64,
        elem: usize,
    },
    /// An on-disk offset/length pair does not lie inside the file it claims to index.
    #[error("{what}: {offset}+{len} lies outside the {file_len}-byte file")]
    OutOfFile {
        what: &'static str,
        offset: u64,
        len: u64,
        file_len: u64,
    },
    /// An Elias–Fano high bitmap does not hold the number of one-bits its header declares.
    /// `select₁` walks the bitmap counting ones and would index past its end — a panic.
    #[error("{what}: high bitmap holds {found} one-bits, header declares {declared}")]
    EfBitCount {
        what: &'static str,
        declared: usize,
        found: usize,
    },
    /// An Elias–Fano header's low-bits width `ℓ` is outside the `0..=63` a legitimate encoder
    /// can produce. Unchecked, `value_at`/`degree_at` shift a `u64` left by `ℓ`: in debug that
    /// panics, but in release Rust masks the shift to `ℓ & 63` and the decoder silently returns
    /// *wrong values with no error* — the case that matters.
    #[error("{what}: low-bits width {l} is outside the valid range 0..=63")]
    EfLowBitsWidth { what: &'static str, l: u8 },
    /// A block's slot-offset table is not a partition of the block's data region.
    /// [`crate::blockfile::parse_block`] hands the table straight to callers that slice
    /// `data[offsets[i]..offsets[i+1]]` by hand, so a non-monotone or overrunning entry is a
    /// slice-out-of-bounds *panic* at generation open where a recoverable error is available.
    #[error(
        "{what}: slot-offset table is not a partition of the {data_len}-byte data region \
         ({reason}: slot {slot} spans {start}..{end})"
    )]
    BlockOffsetTable {
        what: &'static str,
        reason: &'static str,
        slot: usize,
        start: u32,
        end: u32,
        data_len: usize,
    },
    /// A run-length record's element count is above what the format could ever store.
    #[error("{what}: declares {n} elements, above the {max}-element ceiling")]
    TooManyElements {
        what: &'static str,
        n: u64,
        max: usize,
    },
}

/// How many elements to *reserve* for a `count`-element sequence being decoded out of
/// `remaining` bytes, where the encoding spends at least `min_elem_bytes` per element.
///
/// `count` comes off disk — a uvarint, or an on-disk `u32`/`u64` — and for a plaintext
/// generation an attacker with data-dir write access forges it. `Vec::with_capacity(count)`
/// on that number is an allocator abort (a `u64` count is a 16-exabyte request), which kills
/// the process on a path as ordinary as a scan. But a *valid* sequence of `count` elements
/// cannot fit in fewer than `count * min_elem_bytes` bytes, so anything above
/// `remaining / min_elem_bytes` is a claim the input cannot possibly honour.
///
/// This bounds the **reservation only** — never acceptance. The decode loop still runs to the
/// declared `count` and fails on its first short read, so the error a forged length produces
/// is unchanged; all that changes is that we no longer allocate for it first. Nothing a
/// legitimate writer can emit is rejected by this, and an under-reservation costs one regrow.
#[inline]
pub fn capacity_for(count: usize, remaining: usize, min_elem_bytes: usize) -> usize {
    count.min(remaining / min_elem_bytes.max(1))
}

/// Ceiling on a reservation whose count is *not* backed by a buffer we hold — a blockfile
/// directory's record total, a manifest's node count. There is no `remaining` to clamp
/// against, and the count is still untrusted, so reserve a bounded prefix and let the `Vec`
/// grow: reaching 91.6M (wikidata) from here is ~7 doublings, which is nothing beside the
/// block reads that fill it.
pub const MAX_UNBACKED_PREALLOC: usize = 1 << 20;

/// See [`MAX_UNBACKED_PREALLOC`].
#[inline]
pub fn capacity_hint(count: usize) -> usize {
    count.min(MAX_UNBACKED_PREALLOC)
}

/// `n * elem`, as a `usize`, or [`DecodeRejected::LengthOverflow`]. Use wherever a length
/// read off disk is multiplied by an element width before being compared against a buffer:
/// in release builds `n * elem` *wraps*, and a forged `n` chosen to wrap to exactly the
/// buffer's length passes the check and reaches `with_capacity(n)` at full width.
#[inline]
pub fn checked_span(what: &'static str, n: u64, elem: usize) -> Result<usize> {
    usize::try_from(n)
        .ok()
        .and_then(|n| n.checked_mul(elem))
        .ok_or_else(|| DecodeRejected::LengthOverflow { what, n, elem }.into())
}

/// Maximum value nesting a decode will follow, counting the value itself: a bare scalar
/// is depth 1, an item of a top-level list is depth 2.
///
/// A list decodes by recursing, so nesting costs *stack*, and the length guards below do
/// not bound it: `05 01` (TAG_LIST, one item) is a wholly credible header — one item needs
/// one byte and there is one byte left — so a ~1 MiB run of `05 01` pairs is 500 000
/// well-formed frames and overflows the stack. In Rust that is an abort, not a catchable
/// panic: the whole server dies. Every caller of [`read_value`] / [`skip_value`] is holding
/// untrusted bytes — a `.blk` block off disk (`columns`, `segment`, `segindex`, `isam`,
/// `histogram`) or a WAL record being replayed (`slater-delta`) — and for a plaintext
/// generation an attacker with data-dir write access forges those directly.
///
/// **Why 256 and not the ~64 the finding suggested.** Not for legitimacy headroom: a real
/// property value nests 2–3 levels (a list of strings; a list of lists of ids), and nothing
/// an upstream KG import can produce comes near even 16. The binding constraint is the other
/// direction — *whatever the decoder refuses, the writer must never have accepted.* This is a
/// live write path: a Bolt client can `SET n.p = $param`, and that parameter is admitted by
/// `bolt::packstream`, whose own guard (`MAX_DEPTH`, HIK-79) allows nesting to 256 by the same
/// counting rule. It then becomes a `Val`, then a [`Value`], and is persisted by [`write_value`],
/// which is infallible and encodes whatever it is handed. A cap of 64 here would let a client
/// write a 100-deep list, be told OK, and leave that property — and the WAL segment holding it —
/// permanently unreadable: a stack-overflow DoS traded for a data-loss bug. Setting the decode
/// gate exactly at the accept gate closes that. (A property value sits *inside* the params map,
/// so its own subtree is bounded by 255 in practice — one level of margin.)
///
/// It concedes nothing to the attacker: 256 frames of [`read_value`] is tens of KiB, safe on the
/// smallest (2 MiB) worker stack, while the attack needs ~500 000 frames.
pub const MAX_VALUE_DEPTH: usize = 256;

/// An encoded property value cannot be decoded.
///
/// Typed so callers classify it with `err.downcast_ref::<ValueDecodeError>()` rather than
/// matching the message text (cf. [`crate::codec::BlockSizeExceeded`]): this is a corrupt or
/// hostile image, not an I/O hiccup.
#[derive(Debug, thiserror::Error)]
pub enum ValueDecodeError {
    /// The value nests past [`MAX_VALUE_DEPTH`]. Refused before recursing any further, so the
    /// stack cost of a hostile value is bounded whatever the payload claims.
    #[error("property value nests deeper than the {max}-level limit")]
    DepthExceeded { max: usize },
}

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
///
/// Nesting is bounded by [`MAX_VALUE_DEPTH`] — skipping recurses just as decoding does, so
/// it overflows the stack on the same payload if left unguarded.
pub fn skip_value(r: &mut &[u8]) -> Result<()> {
    skip_value_at(r, 1)
}

/// `depth` is the nesting level of the value about to be skipped (root = 1). Checked on entry,
/// so recursion stops *before* the frame that would take it past the cap.
fn skip_value_at(r: &mut &[u8], depth: usize) -> Result<()> {
    if depth > MAX_VALUE_DEPTH {
        return Err(ValueDecodeError::DepthExceeded {
            max: MAX_VALUE_DEPTH,
        }
        .into());
    }
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
                skip_value_at(r, depth + 1)?;
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
///
/// Nesting is bounded by [`MAX_VALUE_DEPTH`]; a value nested past it is refused with
/// [`ValueDecodeError::DepthExceeded`] rather than recursing until the stack overflows.
pub fn read_value(r: &mut &[u8]) -> Result<Value> {
    read_value_at(r, 1)
}

/// `depth` is the nesting level of the value about to be decoded (root = 1). Checked on entry,
/// so recursion stops *before* the frame that would take it past the cap.
fn read_value_at(r: &mut &[u8], depth: usize) -> Result<Value> {
    if depth > MAX_VALUE_DEPTH {
        return Err(ValueDecodeError::DepthExceeded {
            max: MAX_VALUE_DEPTH,
        }
        .into());
    }
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
            let mut items = Vec::with_capacity(capacity_for(n, r.len(), 1));
            for _ in 0..n {
                items.push(read_value_at(r, depth + 1)?);
            }
            Value::List(items)
        }
        TAG_VECTOR => {
            let n = read_uvarint(r)? as usize;
            // Each element is a 4-byte `f32`, so bound the pre-allocation by
            // `remaining / 4` (see `TAG_LIST`) against a forged length.
            let mut xs = Vec::with_capacity(capacity_for(n, r.len(), 4));
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

    /// `levels` nested one-item lists wrapped around a `Null`, i.e. the `05 01 … 00` shape.
    /// The innermost `Null` sits at depth `levels + 1`.
    fn nested_list_bytes(levels: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(levels * 2 + 1);
        for _ in 0..levels {
            buf.push(TAG_LIST);
            buf.push(1);
        }
        buf.push(TAG_NULL);
        buf
    }

    /// A hostile value nests, it does not lie about lengths — so the pre-allocation guards
    /// never fire. Pre-fix, this input killed the process outright: `fatal runtime error:
    /// stack overflow, aborting` (SIGABRT), which no caller can catch. It must come back as a
    /// typed `Err`, from both recursive entry points.
    #[test]
    fn over_nested_value_is_refused_not_a_stack_overflow() {
        // ~400 KiB. Every header here is credible — one item declared, one byte left to hold
        // it — so only the depth guard can stop it.
        let buf = nested_list_bytes(200_000);

        let mut r = &buf[..];
        let err = read_value(&mut r).expect_err("over-nested value must be refused");
        assert!(
            matches!(
                err.downcast_ref::<ValueDecodeError>(),
                Some(ValueDecodeError::DepthExceeded { .. })
            ),
            "expected a typed DepthExceeded, got: {err}"
        );

        // `skip_value` recurses on exactly the same shape (`columns::decode_one` steps over
        // the keys a query didn't ask for), so it needs the same guard.
        let mut r = &buf[..];
        let err = skip_value(&mut r).expect_err("over-nested value must be refused by skip too");
        assert!(
            matches!(
                err.downcast_ref::<ValueDecodeError>(),
                Some(ValueDecodeError::DepthExceeded { .. })
            ),
            "expected a typed DepthExceeded, got: {err}"
        );
    }

    /// The bound must not reject valid graphs: everything up to the cap still round-trips, and
    /// exactly one level past it is refused (the cap is off-by-one correct, not approximate).
    #[test]
    fn nesting_up_to_the_cap_still_decodes() {
        // The deepest value the cap admits: `MAX_VALUE_DEPTH - 1` lists around a scalar leaves
        // the scalar itself at depth `MAX_VALUE_DEPTH`.
        let mut v = Value::Null;
        for _ in 0..MAX_VALUE_DEPTH - 1 {
            v = Value::List(vec![v]);
        }
        let mut buf = Vec::new();
        write_value(&mut buf, &v);
        assert_eq!(buf, nested_list_bytes(MAX_VALUE_DEPTH - 1));

        let mut r = &buf[..];
        assert_eq!(
            read_value(&mut r).unwrap(),
            v,
            "value at the cap must decode"
        );
        assert!(r.is_empty());

        let mut r = &buf[..];
        skip_value(&mut r).expect("value at the cap must skip");
        assert!(r.is_empty());

        // One level deeper — the scalar now at `MAX_VALUE_DEPTH + 1` — is refused.
        let over = nested_list_bytes(MAX_VALUE_DEPTH);
        let mut r = &over[..];
        let err = read_value(&mut r).expect_err("one past the cap must be refused");
        assert!(matches!(
            err.downcast_ref::<ValueDecodeError>(),
            Some(ValueDecodeError::DepthExceeded { .. })
        ));
        let mut r = &over[..];
        assert!(skip_value(&mut r).is_err());
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
