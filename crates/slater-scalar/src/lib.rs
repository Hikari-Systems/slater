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
        // `toString` and the `*OrNull` variant coincide here (the renderer never
        // errors). `toInteger`/`toFloat`/`toBoolean` already NULL on a failed
        // coercion, so their `*OrNull` forms are aliases.
        "tostring" | "tostringornull" => match a0(0) {
            Value::Null => Value::Null,
            v => Value::Str(display(&v)),
        },
        "tointeger" | "tointegerornull" => match a0(0) {
            Value::Int(i) => Value::Int(i),
            Value::Float(f) => Value::Int(f as i64),
            Value::Str(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .unwrap_or(Value::Null),
            Value::Bool(b) => Value::Int(b as i64),
            _ => Value::Null,
        },
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

fn truthy(v: &Value) -> bool {
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

/// A mandatory integer argument (FalkorDB `SI_GET_NUMERIC`, so a float truncates).
fn num_i64(v: Option<&Value>) -> Result<i64> {
    match v {
        Some(Value::Int(i)) => Ok(*i),
        Some(Value::Float(f)) => Ok(*f as i64),
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
    fn containers() {
        let xs = Value::List(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
        assert_eq!(ev("size", &[xs.clone()]), Value::Int(3));
        assert_eq!(ev("head", &[xs.clone()]), Value::Int(3));
        assert_eq!(ev("last", &[xs.clone()]), Value::Int(2));
        assert_eq!(
            ev("list.sort", &[xs.clone()]),
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
}
