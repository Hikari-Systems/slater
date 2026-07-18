//! Pure, deterministic scalar-function evaluation over the on-disk
//! [`graph_format::ids::Value`].
//!
//! This is the single source of truth for the scalar functions that are both
//! (a) **pure** — their result depends only on their argument values, never on
//! graph structure, a runtime row, a cache, the clock, or an RNG — and
//! (b) **`Value`-typed** — every argument and result round-trips through the
//! 7-variant on-disk type (Null/Bool/Int/Float/Str/List/Vector).
//!
//! The query engine (`slater`) delegates the matching arms of its `call_function`
//! dispatcher here (converting its richer runtime `Val` to/from `Value` at the
//! boundary), and the offline builder (`slater-build`) evaluates dump `SET`
//! expressions here at build time. Impure / non-deterministic / non-`Value`
//! functions (`labels`, `id`, `timestamp`, `rand`, `point`, temporal, `vecf32`,
//! …) deliberately live only in the query engine and are rejected by the builder.

use anyhow::{bail, Result};
use graph_format::ids::Value;
use std::cmp::Ordering;

/// Evaluate a pure scalar function on already-evaluated `Value` arguments.
///
/// * `Ok(Some(v))` — handled; `v` is the result.
/// * `Ok(None)`    — `name` is not a function this crate evaluates (the caller
///   falls back to its own dispatch, or — for the builder — reports the function
///   as unsupported in a build-time `SET`).
/// * `Err(e)`      — a genuine type error for a function this crate *does* handle.
///
/// `name` is matched case-insensitively.
pub fn eval_pure(name: &str, args: &[Value]) -> Result<Option<Value>> {
    let n = name.to_ascii_lowercase();
    let a0 = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    let out = match n.as_str() {
        // ── null handling ───────────────────────────────────────────────────
        "coalesce" => args
            .iter()
            .find(|v| !matches!(v, Value::Null))
            .cloned()
            .unwrap_or(Value::Null),

        // ── string transforms ───────────────────────────────────────────────
        "tolower" | "lower" => str_fn(&a0(0), |s| s.to_lowercase()),
        "toupper" | "upper" => str_fn(&a0(0), |s| s.to_uppercase()),
        "trim" => str_fn(&a0(0), |s| s.trim().to_string()),
        "ltrim" => str_fn(&a0(0), |s| s.trim_start().to_string()),
        "rtrim" => str_fn(&a0(0), |s| s.trim_end().to_string()),
        "reverse" => match a0(0) {
            Value::Str(s) => Value::Str(s.chars().rev().collect()),
            Value::List(mut xs) => {
                xs.reverse();
                Value::List(xs)
            }
            Value::Null => Value::Null,
            other => bail!("reverse() needs a string or list, got {}", display(&other)),
        },
        "left" => left_right(args, true)?,
        "right" => left_right(args, false)?,
        "substring" => substring(args)?,
        "split" => match (a0(0), a0(1)) {
            (Value::Str(s), Value::Str(sep)) => {
                Value::List(s.split(&sep).map(|p| Value::Str(p.to_string())).collect())
            }
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => bail!("split() needs two strings"),
        },
        "replace" => match (a0(0), a0(1), a0(2)) {
            (Value::Str(s), Value::Str(a), Value::Str(b)) => Value::Str(s.replace(&a, &b)),
            (Value::Null, _, _) => Value::Null,
            _ => bail!("replace() needs three strings"),
        },
        "string.join" => string_join(args)?,

        // ── size / containers ───────────────────────────────────────────────
        // `length`/`size` over a collection or string is the element/char count.
        // (A `Val::Map`/`Val::Path` is runtime-only and never reaches this crate.)
        "size" | "length" => match a0(0) {
            Value::List(xs) => Value::Int(xs.len() as i64),
            Value::Vector(xs) => Value::Int(xs.len() as i64),
            Value::Str(s) => Value::Int(s.chars().count() as i64),
            Value::Null => Value::Null,
            other => bail!(
                "{n}() needs a collection or string, got {}",
                display(&other)
            ),
        },
        "head" => match a0(0) {
            Value::List(xs) => xs.into_iter().next().unwrap_or(Value::Null),
            Value::Null => Value::Null,
            other => bail!("head() needs a list, got {}", display(&other)),
        },
        "last" => match a0(0) {
            Value::List(xs) => xs.into_iter().last().unwrap_or(Value::Null),
            Value::Null => Value::Null,
            other => bail!("last() needs a list, got {}", display(&other)),
        },
        "tail" => match a0(0) {
            Value::Null => Value::Null,
            Value::List(xs) => Value::List(xs.into_iter().skip(1).collect()),
            other => bail!("tail() needs a list, got {}", display(&other)),
        },
        "isempty" => match a0(0) {
            Value::Str(s) => Value::Bool(s.is_empty()),
            Value::List(xs) => Value::Bool(xs.is_empty()),
            Value::Null => Value::Null,
            other => bail!(
                "isEmpty() needs a string, list or map, got {}",
                display(&other)
            ),
        },

        // ── list functions (FalkorDB list_funcs.c) ──────────────────────────
        "list.dedup" => match a0(0) {
            Value::Null => Value::Null,
            Value::List(mut xs) => {
                dedup_vals(&mut xs);
                Value::List(xs)
            }
            other => bail!("list.dedup() needs a list, got {}", display(&other)),
        },
        "list.sort" => list_sort(args)?,
        "list.remove" => list_remove(args)?,
        "list.insert" => list_insert(args)?,
        "list.insertlistelements" => list_insert_elements(args)?,
        "tobooleanlist" => to_type_list(&a0(0), "toboolean")?,
        "tofloatlist" => to_type_list(&a0(0), "tofloat")?,
        "tointegerlist" => to_type_list(&a0(0), "tointeger")?,
        "tostringlist" => to_type_list(&a0(0), "tostring")?,

        // ── conversions ─────────────────────────────────────────────────────
        // `toString` and its `*OrNull` variant coincide here (the renderer never
        // errors). `toFloat`/`toBoolean` NULL on a failed coercion, so their
        // `*OrNull` forms are aliases. `toInteger` is the exception: an
        // out-of-range/non-finite *float* is a real number it cannot represent —
        // a hard error for `toInteger`, a NULL for `toIntegerOrNull` (see below).
        "tostring" | "tostringornull" => match a0(0) {
            Value::Null => Value::Null,
            v => Value::Str(display(&v)),
        },
        "tointeger" | "tointegerornull" => {
            let or_null = n == "tointegerornull";
            match a0(0) {
                Value::Int(i) => Value::Int(i),
                // A finite in-range float truncates toward zero; a non-finite or
                // out-of-i64-range one is unrepresentable — error for `toInteger`,
                // NULL for `…OrNull`. (Deliberately *not* symmetric with the string
                // arm: a malformed string was never an integer, so it is NULL for
                // both spellings — Cypher semantics we must not change.)
                Value::Float(f) => match f64_to_i64_checked(f) {
                    Ok(i) => Value::Int(i),
                    Err(_) if or_null => Value::Null,
                    Err(e) => return Err(e),
                },
                Value::Str(s) => s
                    .trim()
                    .parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or(Value::Null),
                Value::Bool(b) => Value::Int(b as i64),
                _ => Value::Null,
            }
        }
        "tofloat" | "tofloatornull" => match a0(0) {
            Value::Int(i) => Value::Float(i as f64),
            Value::Float(f) => Value::Float(f),
            Value::Str(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        "toboolean" | "tobooleanornull" => match a0(0) {
            Value::Bool(b) => Value::Bool(b),
            Value::Str(s) => match s.trim().to_lowercase().as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => Value::Null,
            },
            _ => Value::Null,
        },

        // ── numeric (libm-direct; non-numeric / NULL → NULL) ─────────────────
        "abs" => num_fn(&a0(0), |x| x.abs()),
        "ceil" => num_fn(&a0(0), |x| x.ceil()),
        "floor" => num_fn(&a0(0), |x| x.floor()),
        "round" => num_fn(&a0(0), |x| x.round()),
        "sqrt" => num_fn(&a0(0), |x| x.sqrt()),
        "log" => num_fn(&a0(0), |x| x.ln()),
        "log10" => num_fn(&a0(0), |x| x.log10()),
        "exp" => num_fn(&a0(0), |x| x.exp()),
        "e" => Value::Float(std::f64::consts::E),
        "pi" => Value::Float(std::f64::consts::PI),
        "pow" => match (value_as_num(&a0(0)), value_as_num(&a0(1))) {
            (Some(b), Some(e)) => Value::Float(b.powf(e)),
            _ => Value::Null,
        },
        "sign" => num_fn(&a0(0), |x| x.signum().trunc()),
        "sin" => num_fn(&a0(0), |x| x.sin()),
        "cos" => num_fn(&a0(0), |x| x.cos()),
        "tan" => num_fn(&a0(0), |x| x.tan()),
        "cot" => num_fn(&a0(0), |x| x.cos() / x.sin()),
        "asin" => num_fn(&a0(0), |x| x.asin()),
        "acos" => num_fn(&a0(0), |x| x.acos()),
        "atan" => num_fn(&a0(0), |x| x.atan()),
        "atan2" => match (value_as_num(&a0(0)), value_as_num(&a0(1))) {
            (Some(y), Some(x)) => Value::Float(y.atan2(x)),
            _ => Value::Null,
        },
        "degrees" => num_fn(&a0(0), |x| x.to_degrees()),
        "radians" => num_fn(&a0(0), |x| x.to_radians()),
        "haversin" => num_fn(&a0(0), |x| (1.0 - x.cos()) / 2.0),

        // ── type / presence ─────────────────────────────────────────────────
        "typeof" => Value::Str(type_name_title(&a0(0)).to_string()),
        "exists" => Value::Bool(!matches!(a0(0), Value::Null)),

        _ => return Ok(None),
    };
    Ok(Some(out))
}

/// Whether [`eval_pure`] handles `name` (case-insensitive). This is the build-time
/// allowlist: a function not handled here is rejected in a build `SET` clause.
pub fn handles(name: &str) -> bool {
    PURE_FUNCTIONS.contains(&name.to_ascii_lowercase().as_str())
}

/// Every name [`eval_pure`] handles (lowercased, including aliases). Kept in sync
/// with the match above; the query engine asserts this is a subset of its
/// `IMPLEMENTED_FUNCTIONS` registry.
pub const PURE_FUNCTIONS: &[&str] = &[
    "coalesce",
    "tolower",
    "lower",
    "toupper",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "reverse",
    "left",
    "right",
    "substring",
    "split",
    "replace",
    "string.join",
    "size",
    "length",
    "head",
    "last",
    "tail",
    "isempty",
    "list.dedup",
    "list.sort",
    "list.remove",
    "list.insert",
    "list.insertlistelements",
    "tobooleanlist",
    "tofloatlist",
    "tointegerlist",
    "tostringlist",
    "tostring",
    "tostringornull",
    "tointeger",
    "tointegerornull",
    "tofloat",
    "tofloatornull",
    "toboolean",
    "tobooleanornull",
    "abs",
    "ceil",
    "floor",
    "round",
    "sqrt",
    "log",
    "log10",
    "exp",
    "e",
    "pi",
    "pow",
    "sign",
    "sin",
    "cos",
    "tan",
    "cot",
    "asin",
    "acos",
    "atan",
    "atan2",
    "degrees",
    "radians",
    "haversin",
    "typeof",
    "exists",
];

// ── operators / CASE (build-time SET expressions) ─────────────────────────────
//
// The offline builder evaluates dump `SET` right-hand sides that use infix
// operators and `CASE` (e.g. `n.s = CASE WHEN coalesce(n.s,'')='' THEN x ELSE
// n.s + '; ' + x END`). These mirror the query engine's `arith`/`compare`/
// `loose_eq`/`truthy` (`slater/src/exec.rs`) restricted to the 7 on-disk `Value`
// variants — the runtime-only types (Node/Rel/Path/Map/Point/temporal) cannot
// occur in a build-time SET, so their cases are omitted. CASE short-circuiting
// lives in the caller (only the chosen branch is evaluated); this crate supplies
// the leaf operator / comparison / truthiness semantics.

/// Binary arithmetic / concatenation operator. Mirrors the query engine's `BinOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl BinOp {
    /// The Cypher source spelling, for error messages.
    fn symbol(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
        }
    }
}

/// Integer arithmetic whose result does not fit in a signed 64-bit integer.
///
/// Cypher integers are `i64`, and Slater — like Neo4j — **refuses** to produce a
/// value it cannot represent: it does not wrap, saturate, or silently promote to
/// `f64`. Every one of those alternatives answers the query with a number that is
/// simply wrong; an error is the only honest reply. (Neo4j raises `ArithmeticError`
/// / "long overflow" here, so this also keeps us Cypher-compatible.)
///
/// This exists as a **typed** error, not a message, because the release profile
/// carries no `overflow-checks`: before this type, `+`/`-`/`*`/unary `-` wrapped
/// silently in production and panicked in debug — the same query giving a quiet
/// wrong answer in prod and killing the process under test. Every integer path in
/// this crate and in the query engine's `arith`/`Expr::Neg`/`sum` now goes through
/// `checked_*` and reports overflow *here*, so **debug and release agree** and the
/// profile setting no longer changes behaviour. Callers classify it with
/// `err.downcast_ref::<ArithmeticOverflow>()`, never by matching the message text.
#[derive(Debug, thiserror::Error)]
#[error("integer overflow evaluating `{expr}`: the result does not fit in a signed 64-bit integer")]
pub struct ArithmeticOverflow {
    /// The operation that overflowed, rendered with its operands — e.g.
    /// `9223372036854775807 + 1`, `-(-9223372036854775808)`, `sum(): 1 + 2`.
    pub expr: String,
}

impl ArithmeticOverflow {
    /// `lhs <op> rhs` overflowed.
    pub fn binary(op: &str, lhs: i64, rhs: i64) -> Self {
        Self {
            expr: format!("{lhs} {op} {rhs}"),
        }
    }

    /// A unary operator overflowed. Only `-i64::MIN` can: the two's-complement
    /// range is asymmetric, so `i64::MIN` has no positive counterpart.
    pub fn unary(op: &str, operand: i64) -> Self {
        Self {
            expr: format!("{op}({operand})"),
        }
    }

    /// An aggregate's running total overflowed while folding in the next value.
    pub fn aggregate(name: &str, acc: i64, next: i64) -> Self {
        Self {
            expr: format!("{name}: {acc} + {next}"),
        }
    }
}

/// Comparison operator. Mirrors the query engine's `CmpOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Evaluate `a <op> b`. Mirrors `arith` in `slater/src/exec.rs`: a NULL operand
/// yields NULL; `+` with any string operand concatenates (and with a list operand
/// builds/extends a list); Int+Int stays Int; otherwise both sides coerce to
/// Float. Division / modulo by zero, and arithmetic on non-numeric non-string
/// operands, are errors — as is an integer result that does not fit in an `i64`
/// ([`ArithmeticOverflow`]); integer arithmetic never wraps.
pub fn eval_binop(op: BinOp, a: Value, b: Value) -> Result<Value> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    if let BinOp::Add = op {
        match (&a, &b) {
            (Value::Str(_), _) | (_, Value::Str(_)) => {
                return Ok(Value::Str(format!("{}{}", display(&a), display(&b))));
            }
            (Value::List(xs), Value::List(ys)) => {
                let mut v = xs.clone();
                v.extend(ys.clone());
                return Ok(Value::List(v));
            }
            (Value::List(xs), _) => {
                let mut v = xs.clone();
                v.push(b);
                return Ok(Value::List(v));
            }
            (_, Value::List(ys)) => {
                let mut v = vec![a];
                v.extend(ys.clone());
                return Ok(Value::List(v));
            }
            _ => {}
        }
    }
    if let (Value::Int(x), Value::Int(y)) = (&a, &b) {
        let (x, y) = (*x, *y);
        // Every integer op is `checked_*`: an `i64` result that does not exist is an
        // `ArithmeticOverflow`, never a wrapped value and never a panic. Keep this in
        // lockstep with `arith` in `slater/src/exec.rs`.
        let checked = match op {
            BinOp::Add => x.checked_add(y),
            BinOp::Sub => x.checked_sub(y),
            BinOp::Mul => x.checked_mul(y),
            BinOp::Div => {
                if y == 0 {
                    bail!("integer division by zero");
                }
                // `i64::MIN / -1` is the one overflowing division. Unlike `+`/`-`/`*`
                // it panics even in release (Rust always checks division overflow),
                // so `checked_div` is a liveness fix, not just a correctness one.
                x.checked_div(y)
            }
            BinOp::Mod => {
                if y == 0 {
                    bail!("integer modulo by zero");
                }
                x.checked_rem(y)
            }
        };
        return match checked {
            Some(v) => Ok(Value::Int(v)),
            None => bail!(ArithmeticOverflow::binary(op.symbol(), x, y)),
        };
    }
    match (value_as_num(&a), value_as_num(&b)) {
        (Some(x), Some(y)) => Ok(Value::Float(match op {
            BinOp::Add => x + y,
            BinOp::Sub => x - y,
            BinOp::Mul => x * y,
            BinOp::Div => x / y,
            BinOp::Mod => x % y,
        })),
        _ => bail!(
            "cannot apply arithmetic to {} and {}",
            display(&a),
            display(&b)
        ),
    }
}

/// Evaluate `a <op> b` as a boolean, three-valued: a NULL operand yields NULL.
/// Mirrors `compare` in `slater/src/exec.rs`.
pub fn eval_compare(op: CmpOp, a: &Value, b: &Value) -> Value {
    match op {
        CmpOp::Eq => loose_eq(a, b).map(Value::Bool).unwrap_or(Value::Null),
        CmpOp::Ne => loose_eq(a, b)
            .map(|e| Value::Bool(!e))
            .unwrap_or(Value::Null),
        _ => {
            if matches!(a, Value::Null) || matches!(b, Value::Null) {
                return Value::Null;
            }
            let Some(ord) = comparable(a, b) else {
                return Value::Null;
            };
            Value::Bool(match op {
                CmpOp::Lt => ord == Ordering::Less,
                CmpOp::Le => ord != Ordering::Greater,
                CmpOp::Gt => ord == Ordering::Greater,
                CmpOp::Ge => ord != Ordering::Less,
                _ => unreachable!(),
            })
        }
    }
}

/// Equality core (FalkorDB loose equality) over `Value`. NULL on either side ⇒
/// `None` (the comparison is itself NULL); Int/Float compare cross-type as f64;
/// other types compare structurally within the same type, mixed types are unequal.
/// Mirrors `Val::loose_eq` restricted to the on-disk variants.
fn loose_eq(a: &Value, b: &Value) -> Option<bool> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return None;
    }
    if let (Some(x), Some(y)) = (value_as_num(a), value_as_num(b)) {
        return Some(x == y);
    }
    Some(match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Vector(x), Value::Vector(y)) => x == y,
        (Value::List(x), Value::List(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| loose_eq(p, q) == Some(true))
        }
        _ => false,
    })
}

/// Ordering for `<`/`>` etc — only like-typed ordered operands (numbers cross
/// Int/Float via `f64::total_cmp`, str/str, bool/bool); anything else ⇒ `None`
/// (the comparison is NULL). Mirrors `comparable` for the on-disk variants.
fn comparable(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
            Some(value_as_num(a)?.total_cmp(&value_as_num(b)?))
        }
        (Value::Str(x), Value::Str(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn value_as_num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

fn str_fn(v: &Value, f: impl Fn(&str) -> String) -> Value {
    match v {
        Value::Str(s) => Value::Str(f(s)),
        _ => Value::Null,
    }
}

fn num_fn(v: &Value, f: impl Fn(f64) -> f64) -> Value {
    match value_as_num(v) {
        Some(x) => Value::Float(f(x)),
        None => Value::Null,
    }
}

/// Render for `toString` / error messages. Mirrors the query engine's
/// `Val::to_display` for the scalar variants (notably `f64::to_string`, not the
/// `Debug` form). List/Vector are never produced by a real build `SET` and fall to
/// `Debug` only in error text.
fn display(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// TitleCase type tag matching the query engine's `type_name` (FalkorDB
/// `SIType_ToString`) for the on-disk variants — used by `typeof`.
fn type_name_title(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Int(_) => "Integer",
        Value::Float(_) => "Float",
        Value::Str(_) => "String",
        Value::List(_) => "List",
        Value::Vector(_) => "Vectorf32",
    }
}

/// Cypher truthiness: only `true` is truthy. NULL and every non-boolean value are
/// not truthy (so a `CASE WHEN <non-bool>` skips that branch). Mirrors `truthy` in
/// `slater/src/exec.rs`.
pub fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

/// Resolve a (possibly negative) list index to a forward offset (FalkorDB
/// `list_funcs.c normalize_index`). With `inclusive`, the valid range gains one
/// slot at the end (so `list.insert` can append). `None` when out of bounds.
fn normalize_index(idx: i64, len: usize, inclusive: bool) -> Option<usize> {
    let alen = len as i64 + if inclusive { 1 } else { 0 };
    if (idx < 0 && idx + alen < 0) || (idx > 0 && idx >= alen) {
        return None;
    }
    Some((if idx < 0 { alen + idx } else { idx }) as usize)
}

fn list_contains(xs: &[Value], v: &Value) -> bool {
    xs.iter().any(|x| x.cmp_key(v) == Ordering::Equal)
}

/// Drop later duplicates, preserving first-seen order, under the total order.
fn dedup_vals(xs: &mut Vec<Value>) {
    let mut seen: Vec<Value> = Vec::new();
    xs.retain(|v| {
        if seen.iter().any(|s| s.cmp_key(v) == Ordering::Equal) {
            false
        } else {
            seen.push(v.clone());
            true
        }
    });
}

/// Truncate a float to `i64`, erroring instead of silently saturating. Plain `f as i64`
/// clamps out-of-range values (`1e19 → i64::MAX`, `-1e19 → i64::MIN`) and maps `NaN → 0`
/// — every one a wrong answer with no signal. The bounds are the exact f64 window that
/// truncates back into range: `i64::MIN` (-2^63) is representable, but `i64::MAX` is not
/// (it rounds up to 2^63), so the upper bound is the *exclusive* 2^63.
fn f64_to_i64_checked(f: f64) -> Result<i64> {
    let min = i64::MIN as f64; // -2^63, exact
    let max = -min; //  2^63, exclusive upper bound
    if f.is_finite() && f >= min && f < max {
        Ok(f as i64)
    } else {
        bail!("integer conversion out of range: {f}");
    }
}

/// A mandatory integer argument (FalkorDB `SI_GET_NUMERIC`, so a float truncates).
fn num_i64(v: Option<&Value>) -> Result<i64> {
    match v {
        Some(Value::Int(i)) => Ok(*i),
        Some(Value::Float(f)) => f64_to_i64_checked(*f),
        _ => bail!("expected an integer index argument"),
    }
}

fn substring(args: &[Value]) -> Result<Value> {
    let s = match args.first() {
        Some(Value::Str(s)) => s,
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!("substring() needs a string, got {}", display(other)),
    };
    let chars: Vec<char> = s.chars().collect();
    let start = match args.get(1) {
        Some(Value::Int(i)) if *i >= 0 => (*i as usize).min(chars.len()),
        _ => bail!("substring() start must be a non-negative integer"),
    };
    let end = match args.get(2) {
        Some(Value::Int(len)) if *len >= 0 => (start + *len as usize).min(chars.len()),
        None => chars.len(),
        _ => bail!("substring() length must be a non-negative integer"),
    };
    Ok(Value::Str(chars[start..end].iter().collect()))
}

fn left_right(args: &[Value], from_left: bool) -> Result<Value> {
    let s = match args.first() {
        Some(Value::Str(s)) => s,
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!(
            "{}() needs a string, got {}",
            if from_left { "left" } else { "right" },
            display(other)
        ),
    };
    let n = match args.get(1) {
        Some(Value::Int(i)) if *i >= 0 => *i as usize,
        _ => bail!("length must be a non-negative integer"),
    };
    let chars: Vec<char> = s.chars().collect();
    if n >= chars.len() {
        return Ok(Value::Str(s.clone()));
    }
    let slice = if from_left {
        &chars[..n]
    } else {
        &chars[chars.len() - n..]
    };
    Ok(Value::Str(slice.iter().collect()))
}

fn string_join(args: &[Value]) -> Result<Value> {
    let list = match args.first() {
        Some(Value::List(xs)) => xs,
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!("string.join() needs a list, got {}", display(other)),
    };
    let delim = match args.get(1) {
        None => "",
        Some(Value::Str(d)) => d.as_str(),
        Some(other) => bail!(
            "Type mismatch: expected String but was {}",
            type_name_title(other)
        ),
    };
    let mut parts = Vec::with_capacity(list.len());
    for v in list {
        match v {
            Value::Str(s) => parts.push(s.as_str()),
            other => bail!(
                "Type mismatch: expected String but was {}",
                type_name_title(other)
            ),
        }
    }
    Ok(Value::Str(parts.join(delim)))
}

fn list_sort(args: &[Value]) -> Result<Value> {
    let mut xs = match args.first() {
        Some(Value::List(xs)) => xs.clone(),
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!("list.sort() needs a list, got {}", display(other)),
    };
    let ascending = args.get(1).map(truthy).unwrap_or(true);
    xs.sort_by(|a, b| {
        let o = a.cmp_key(b);
        if ascending {
            o
        } else {
            o.reverse()
        }
    });
    Ok(Value::List(xs))
}

fn list_remove(args: &[Value]) -> Result<Value> {
    let xs = match args.first() {
        Some(Value::List(xs)) => xs.clone(),
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!("list.remove() needs a list, got {}", display(other)),
    };
    let index = num_i64(args.get(1))?;
    let count = match args.get(2) {
        Some(v) => num_i64(Some(v))?,
        None => 1,
    };
    if count <= 0 {
        return Ok(Value::List(xs));
    }
    let Some(idx) = normalize_index(index, xs.len(), false) else {
        return Ok(Value::List(xs));
    };
    let count = (count as usize).min(xs.len() - idx);
    let mut out = Vec::with_capacity(xs.len() - count);
    out.extend_from_slice(&xs[..idx]);
    out.extend_from_slice(&xs[idx + count..]);
    Ok(Value::List(out))
}

fn list_insert(args: &[Value]) -> Result<Value> {
    let xs = match args.first() {
        Some(Value::List(xs)) => xs.clone(),
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!("list.insert() needs a list, got {}", display(other)),
    };
    let val = args.get(2).cloned().unwrap_or(Value::Null);
    if matches!(val, Value::Null) {
        return Ok(Value::List(xs));
    }
    let index = num_i64(args.get(1))?;
    let Some(idx) = normalize_index(index, xs.len(), true) else {
        return Ok(Value::List(xs));
    };
    let allow_dups = args.get(3).map(truthy).unwrap_or(true);
    if !allow_dups && list_contains(&xs, &val) {
        return Ok(Value::List(xs));
    }
    let mut out = Vec::with_capacity(xs.len() + 1);
    out.extend_from_slice(&xs[..idx]);
    out.push(val);
    out.extend_from_slice(&xs[idx..]);
    Ok(Value::List(out))
}

fn list_insert_elements(args: &[Value]) -> Result<Value> {
    let a = match args.first() {
        Some(Value::List(xs)) => xs.clone(),
        Some(Value::Null) | None => return Ok(Value::Null),
        Some(other) => bail!(
            "list.insertListElements() needs a list, got {}",
            display(other)
        ),
    };
    let mut b = match args.get(1) {
        Some(Value::List(xs)) => xs.clone(),
        Some(Value::Null) | None => return Ok(Value::List(a)),
        Some(other) => bail!(
            "list.insertListElements() needs a list as its second argument, got {}",
            display(other)
        ),
    };
    let index = num_i64(args.get(2))?;
    let Some(idx) = normalize_index(index, a.len(), true) else {
        return Ok(Value::List(a));
    };
    let allow_dups = args.get(3).map(truthy).unwrap_or(true);
    if !allow_dups {
        dedup_vals(&mut b);
        b.retain(|v| !list_contains(&a, v));
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    out.extend_from_slice(&a[..idx]);
    out.extend(b);
    out.extend_from_slice(&a[idx..]);
    Ok(Value::List(out))
}

/// Element-wise list conversion shared by `to{Boolean,Float,Integer,String}List`:
/// run each element through the named scalar. A NULL list yields NULL.
fn to_type_list(v: &Value, conv: &str) -> Result<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::List(xs) => {
            let mut out = Vec::with_capacity(xs.len());
            for x in xs {
                out.push(
                    eval_pure(conv, std::slice::from_ref(x))?.expect("conv is a handled scalar"),
                );
            }
            Ok(Value::List(out))
        }
        other => bail!("{conv}List() needs a list, got {}", display(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(x: &str) -> Value {
        Value::Str(x.into())
    }
    fn ev(name: &str, args: &[Value]) -> Value {
        eval_pure(name, args).unwrap().expect("handled")
    }

    #[test]
    fn coalesce_first_non_null() {
        assert_eq!(ev("coalesce", &[Value::Null, Value::Null, s("d")]), s("d"));
        assert_eq!(ev("coalesce", &[s("a"), s("b")]), s("a"));
        assert_eq!(ev("coalesce", &[Value::Null]), Value::Null);
    }

    #[test]
    fn string_family() {
        assert_eq!(ev("toupper", &[s("aB")]), s("AB"));
        assert_eq!(ev("tolower", &[s("aB")]), s("ab"));
        assert_eq!(ev("trim", &[s("  x ")]), s("x"));
        assert_eq!(ev("reverse", &[s("abc")]), s("cba"));
        assert_eq!(ev("left", &[s("abcdef"), Value::Int(3)]), s("abc"));
        assert_eq!(ev("right", &[s("abcdef"), Value::Int(2)]), s("ef"));
        assert_eq!(
            ev("substring", &[s("abcdef"), Value::Int(1), Value::Int(3)]),
            s("bcd")
        );
        assert_eq!(
            ev("split", &[s("a,b,c"), s(",")]),
            Value::List(vec![s("a"), s("b"), s("c")])
        );
        assert_eq!(ev("replace", &[s("axa"), s("x"), s("y")]), s("aya"));
        // non-string → NULL (str_fn semantics)
        assert_eq!(ev("toupper", &[Value::Int(7)]), Value::Null);
        assert_eq!(ev("toupper", &[Value::Null]), Value::Null);
    }

    #[test]
    fn numeric_family() {
        assert_eq!(ev("abs", &[Value::Int(-3)]), Value::Float(3.0));
        assert_eq!(ev("round", &[Value::Float(2.5)]), Value::Float(3.0));
        assert_eq!(ev("sign", &[Value::Float(-9.0)]), Value::Float(-1.0));
        assert_eq!(
            ev("pow", &[Value::Int(2), Value::Int(10)]),
            Value::Float(1024.0)
        );
        assert_eq!(ev("abs", &[s("x")]), Value::Null);
    }

    #[test]
    fn conversions() {
        assert_eq!(ev("tostring", &[Value::Int(5)]), s("5"));
        assert_eq!(ev("tostring", &[Value::Float(1.5)]), s("1.5"));
        assert_eq!(ev("tostring", &[Value::Null]), Value::Null);
        assert_eq!(ev("tointeger", &[s(" 42 ")]), Value::Int(42));
        assert_eq!(ev("tointeger", &[s("x")]), Value::Null);
        assert_eq!(ev("tofloat", &[Value::Int(3)]), Value::Float(3.0));
        assert_eq!(ev("toboolean", &[s("TRUE")]), Value::Bool(true));
    }

    #[test]
    fn tointeger_out_of_range_errors_rather_than_saturating() {
        // In-range floats truncate toward zero (unchanged).
        assert_eq!(ev("tointeger", &[Value::Float(2.9)]), Value::Int(2));
        assert_eq!(ev("tointeger", &[Value::Float(-2.9)]), Value::Int(-2));

        // Out-of-range / non-finite floats used to silently saturate
        // (1e19 → i64::MAX, NaN → 0). `toInteger` now errors instead.
        for bad in [
            1e19_f64,
            -1e19_f64,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert!(
                eval_pure("tointeger", &[Value::Float(bad)]).is_err(),
                "toInteger({bad}) must error, not saturate"
            );
            // …while `toIntegerOrNull` absorbs the same failure as NULL.
            assert_eq!(
                ev("tointegerornull", &[Value::Float(bad)]),
                Value::Null,
                "toIntegerOrNull({bad}) must be NULL"
            );
        }

        // The string arm is unchanged: a malformed string is NULL for both spellings
        // (Cypher semantics), deliberately asymmetric with the float overflow above.
        assert_eq!(ev("tointeger", &[s("abc")]), Value::Null);
        assert_eq!(ev("tointegerornull", &[s("abc")]), Value::Null);

        // The `num_i64` index-argument path (list.remove/insert/…) is guarded too: a
        // saturating index would otherwise clamp to i64::MAX silently.
        let xs = Value::List(vec![Value::Int(1), Value::Int(2)]);
        assert!(
            eval_pure("list.remove", &[xs, Value::Float(1e19)]).is_err(),
            "an out-of-range list index must error"
        );
    }

    #[test]
    fn containers() {
        let xs = Value::List(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
        let one = std::slice::from_ref(&xs);
        assert_eq!(ev("size", one), Value::Int(3));
        assert_eq!(ev("head", one), Value::Int(3));
        assert_eq!(ev("last", one), Value::Int(2));
        assert_eq!(
            ev("list.sort", one),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
        assert_eq!(ev("isempty", &[Value::List(vec![])]), Value::Bool(true));
        assert_eq!(
            ev(
                "list.dedup",
                &[Value::List(vec![
                    Value::Int(1),
                    Value::Int(1),
                    Value::Int(2)
                ])]
            ),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
    }

    #[test]
    fn typeof_and_exists() {
        assert_eq!(ev("typeof", &[Value::Int(1)]), s("Integer"));
        assert_eq!(ev("typeof", &[s("x")]), s("String"));
        assert_eq!(ev("exists", &[Value::Null]), Value::Bool(false));
        assert_eq!(ev("exists", &[Value::Int(1)]), Value::Bool(true));
    }

    #[test]
    fn contract() {
        // unknown → Ok(None)
        assert!(eval_pure("definitely_not_a_fn", &[]).unwrap().is_none());
        // impure names are not handled here
        for name in [
            "rand",
            "timestamp",
            "labels",
            "id",
            "point",
            "vecf32",
            "range",
        ] {
            assert!(
                eval_pure(name, &[]).unwrap().is_none(),
                "{name} must not be pure"
            );
            assert!(!handles(name), "{name} must not be in the allowlist");
        }
        // genuine type error → Err
        assert!(eval_pure("substring", &[Value::Int(1)]).is_err());
        // case-insensitive
        assert_eq!(ev("ToUpper", &[s("a")]), s("A"));
        assert!(handles("COALESCE"));
    }

    #[test]
    fn pure_functions_all_handled() {
        // every advertised name routes to a real arm (not the `_ => None` default)
        for name in PURE_FUNCTIONS {
            assert!(handles(name), "{name} advertised but handles() is false");
        }
    }

    #[test]
    fn binop_add_concat_and_numeric() {
        // `+` concatenates when either side is a string (mirrors the query engine).
        assert_eq!(eval_binop(BinOp::Add, s("a"), s("b")).unwrap(), s("ab"));
        assert_eq!(
            eval_binop(BinOp::Add, s("n="), Value::Int(3)).unwrap(),
            s("n=3")
        );
        assert_eq!(
            eval_binop(BinOp::Add, Value::Float(1.5), s("x")).unwrap(),
            s("1.5x")
        );
        // Int+Int stays Int; a float operand promotes to Float.
        assert_eq!(
            eval_binop(BinOp::Add, Value::Int(2), Value::Int(3)).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            eval_binop(BinOp::Add, Value::Int(2), Value::Float(0.5)).unwrap(),
            Value::Float(2.5)
        );
        // NULL propagates.
        assert_eq!(
            eval_binop(BinOp::Add, Value::Null, Value::Int(1)).unwrap(),
            Value::Null
        );
        // list concat / append.
        assert_eq!(
            eval_binop(BinOp::Add, Value::List(vec![Value::Int(1)]), Value::Int(2)).unwrap(),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
        // division by zero is an error for integers.
        assert!(eval_binop(BinOp::Div, Value::Int(1), Value::Int(0)).is_err());
        // (overflow: `int_arithmetic_overflows_to_a_typed_error`)
        // arithmetic on two non-numeric non-strings errors.
        assert!(eval_binop(BinOp::Sub, Value::Bool(true), Value::Bool(false)).is_err());
    }

    #[test]
    fn compare_three_valued() {
        assert_eq!(eval_compare(CmpOp::Eq, &s("x"), &s("x")), Value::Bool(true));
        assert_eq!(eval_compare(CmpOp::Eq, &s(""), &s("")), Value::Bool(true));
        assert_eq!(eval_compare(CmpOp::Ne, &s("a"), &s("b")), Value::Bool(true));
        // cross Int/Float numeric equality.
        assert_eq!(
            eval_compare(CmpOp::Eq, &Value::Int(2), &Value::Float(2.0)),
            Value::Bool(true)
        );
        // NULL on either side ⇒ NULL (not false).
        assert_eq!(eval_compare(CmpOp::Eq, &Value::Null, &s("x")), Value::Null);
        // mixed types: equality is false, ordered comparison is NULL.
        assert_eq!(
            eval_compare(CmpOp::Eq, &Value::Int(1), &s("1")),
            Value::Bool(false)
        );
        assert_eq!(
            eval_compare(CmpOp::Lt, &Value::Int(1), &s("1")),
            Value::Null
        );
        assert_eq!(
            eval_compare(CmpOp::Lt, &Value::Int(1), &Value::Int(2)),
            Value::Bool(true)
        );
    }

    #[test]
    fn truthy_only_true() {
        assert!(truthy(&Value::Bool(true)));
        assert!(!truthy(&Value::Bool(false)));
        assert!(!truthy(&Value::Null));
        assert!(!truthy(&Value::Int(1)));
        assert!(!truthy(&s("true")));
    }

    /// Integer arithmetic that leaves `i64` is a typed [`ArithmeticOverflow`] — it
    /// never wraps, never saturates, never promotes to `f64`.
    ///
    /// Regression for HIK-73. The release profile carries no `overflow-checks`, so
    /// `+`/`-`/`*` here wrapped silently in a release build while panicking in a
    /// debug one. This test pins the **release** behaviour rather than the debug
    /// panic: it demands an `Err`, so the pre-fix code fails it in release by
    /// returning `Ok(<wrapped>)` — not merely by failing to panic.
    ///
    /// This is `slater-build`'s arithmetic too: the builder's `SET` evaluator
    /// (`merge_build::eval_set_expr`) forwards this `Result` with `?`, so an
    /// overflowing build-time `SET` now fails the build instead of persisting a
    /// wrapped integer into the graph.
    #[test]
    fn int_arithmetic_overflows_to_a_typed_error() {
        // Classified by *type*, never by message text (house rule).
        let overflowed = |op, a, b| {
            eval_binop(op, a, b)
                .err()
                .is_some_and(|e| e.downcast_ref::<ArithmeticOverflow>().is_some())
        };

        assert!(overflowed(BinOp::Add, Value::Int(i64::MAX), Value::Int(1)));
        assert!(overflowed(BinOp::Sub, Value::Int(i64::MIN), Value::Int(1)));
        assert!(overflowed(BinOp::Mul, Value::Int(i64::MAX), Value::Int(2)));
        assert!(overflowed(BinOp::Mul, Value::Int(i64::MIN), Value::Int(-1)));
        // `i64::MIN / -1` (and `%`) are worse than a wrap: Rust checks division
        // overflow in *every* profile, so these **panicked in release too** — a
        // build-time `SET` could take the builder down. Now a clean error.
        assert!(overflowed(BinOp::Div, Value::Int(i64::MIN), Value::Int(-1)));
        assert!(overflowed(BinOp::Mod, Value::Int(i64::MIN), Value::Int(-1)));

        // Representable results are untouched, including at the boundary.
        assert_eq!(
            eval_binop(BinOp::Add, Value::Int(2), Value::Int(3)).unwrap(),
            Value::Int(5)
        );
        assert_eq!(
            eval_binop(BinOp::Sub, Value::Int(i64::MAX), Value::Int(1)).unwrap(),
            Value::Int(i64::MAX - 1)
        );
        assert_eq!(
            eval_binop(BinOp::Div, Value::Int(i64::MIN), Value::Int(1)).unwrap(),
            Value::Int(i64::MIN)
        );
        assert_eq!(
            eval_binop(BinOp::Mod, Value::Int(i64::MIN), Value::Int(2)).unwrap(),
            Value::Int(0)
        );

        // Division by zero keeps its own distinct error — not an overflow.
        assert!(!overflowed(BinOp::Div, Value::Int(1), Value::Int(0)));
        assert!(eval_binop(BinOp::Div, Value::Int(1), Value::Int(0)).is_err());
        assert!(!overflowed(BinOp::Mod, Value::Int(1), Value::Int(0)));
        // A float operand still promotes and saturates to inf, as before.
        assert!(matches!(
            eval_binop(BinOp::Mul, Value::Float(f64::MAX), Value::Float(2.0)).unwrap(),
            Value::Float(f) if f.is_infinite()
        ));
    }
}
