// SPDX-License-Identifier: Apache-2.0
//! Build-time validation of node `SET` right-hand-side expressions.
//!
//! A dump `SET n.k = <expr>` may use a literal, a same-node property reference, or
//! a pure scalar function call. Function calls are evaluated at build time by
//! [`slater_scalar`], so only the functions that crate handles — pure,
//! deterministic, `Value`-typed — are admissible. Impure / non-deterministic /
//! non-`Value` functions (`id`, `timestamp`, `rand`, `point`, …) live only in the
//! query engine and are rejected here. Evaluation itself happens in
//! [`crate::merge_build`] against each node's accumulated properties at fold time.
//!
//! `vecf32([…])` is not a function here at all: the grammar lifts it to a *literal*
//! (a [`graph_format::ids::Value::Vector`]), exactly as in a CREATE property map, so
//! a business-key MERGE dump can attach an embedding through SET. Only that literal
//! form parses; `vecf32(<expression>)` still reaches this validator as a call and is
//! rejected below with a pointer to the literal spelling.

use anyhow::{bail, Result};

use crate::model::SetExpr;

/// Reject any function a build `SET` cannot evaluate. Recurses into call arguments.
pub(crate) fn validate_set_expr(e: &SetExpr) -> Result<()> {
    match e {
        SetExpr::Lit(_) | SetExpr::Prop(_) => Ok(()),
        SetExpr::Func { name, args } => {
            if name.eq_ignore_ascii_case("vecf32") {
                // Reached only when the argument is not a literal number list — the
                // literal form never becomes a `Func` (see the module docs).
                bail!(
                    "vecf32 in build-time SET takes a literal vector: \
                     `SET n.embedding = vecf32([0.1, 0.2, …])`"
                );
            }
            if !slater_scalar::handles(name) {
                bail!(
                    "function `{name}` is not supported in build-time SET \
                     (only pure, deterministic scalar functions are allowed)"
                );
            }
            for a in args {
                validate_set_expr(a)?;
            }
            Ok(())
        }
        SetExpr::BinOp { l, r, .. }
        | SetExpr::Cmp { l, r, .. }
        | SetExpr::And(l, r)
        | SetExpr::Or(l, r) => {
            validate_set_expr(l)?;
            validate_set_expr(r)
        }
        SetExpr::Not(e) => validate_set_expr(e),
        SetExpr::Case {
            subject,
            whens,
            els,
        } => {
            if let Some(s) = subject {
                validate_set_expr(s)?;
            }
            for (c, t) in whens {
                validate_set_expr(c)?;
                validate_set_expr(t)?;
            }
            if let Some(e) = els {
                validate_set_expr(e)?;
            }
            Ok(())
        }
    }
}
