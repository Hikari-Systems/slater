// SPDX-License-Identifier: Apache-2.0
//! openCypher conformance suite for `cypher.pest`.
//!
//! The oracle is the openCypher reference grammar as transcribed to pest at
//! <https://github.com/a-poor/open-cypher/blob/main/src/cypher.pest>. Each test
//! below names the reference production it pins and exercises every alternative
//! of that production.
//!
//! ## What "conformant" means here
//!
//! **Slater accepts every string the oracle accepts, and gives it the same
//! meaning.** A strict superset is fine — Slater's grammar is free to accept more
//! — but it may never accept an oracle-legal string and read it as something
//! else. That distinction is the whole point of [`octal_literal_is_base_eight`]:
//! before this suite, `RETURN 017` parsed and yielded `17`, where the oracle says
//! `OctalInteger` and means `15`. Silent disagreement, not a syntax error.
//!
//! Conformance is asserted at two levels, because they can fail independently:
//!
//! * [`g`] / [`g_no`] — the *grammar* accepts/rejects the string (`Rule::query`).
//! * [`ok`] / [`err`] — the string also *lowers* to an AST the executor can run.
//!
//! A handful of productions parse but do not lower (see `grammar_without_backing`
//! at the foot of this file); they are pinned at the `g` level with the lowering
//! error asserted verbatim, so the gap is a named test rather than a surprise.
//!
//! ## Where the oracle is wrong, and we deliberately do not follow it
//!
//! The oracle is a faithful map of the openCypher *language* but a buggy PEG.
//! Three of its rules misbehave as ordered-choice parsers, and Slater is
//! deliberately correct instead. These are pinned by [`oracle_peg_bugs`] so a
//! future "let's match the oracle exactly" refactor trips over them:
//!
//! 1. `PartialComparisonExpression = EQ | NE | LT | GT | LE | GE` — ordered choice
//!    means `<=` matches `LT` first and strands the `=`. Slater tries `<=` before
//!    `<`.
//! 2. Keyword terminals (`RETURN = @{ ^"RETURN" }`) carry no trailing word
//!    boundary, so `RETURNS` begins with a `RETURN` token. Every Slater `kw_*`
//!    asserts `!ident_cont`.
//! 3. `Comment`'s block arm cannot match `/***/`. Slater's `(!"*/" ~ ANY)*` can.
//!
//! The oracle also demands `SP` after `RETURN`/`WITH` (its `ProjectionBody` opens
//! with `SP ~ ProjectionItems`), so it rejects `RETURN(1)`. Slater uses pest's
//! implicit `WHITESPACE` and accepts it. That is a superset, hence conformant.
//!
//! ## A note on `SchemaName` vs `SymbolicName`
//!
//! The oracle's `UnescapedSymbolicName = IdentifierStart ~ IdentifierPart*` matches
//! every reserved word, and `ReservedWord` is referenced *only* from
//! `SchemaName = SymbolicName | ReservedWord`. So the two productions accept the
//! same strings and no bare name is ever excluded: `MATCH (n:Order)`,
//! `RETURN n.end` and `{limit: 1}` are all legal openCypher. Slater used to carry
//! a `!reserved` guard that rejected them.

use super::*;

// ── Assertion helpers ────────────────────────────────────────────────────────

/// The grammar accepts the whole input (`Rule::query` is `SOI`/`EOI`-anchored).
#[track_caller]
fn g(q: &str) {
    if let Err(e) = CypherParser::parse(Rule::query, q) {
        panic!("grammar should accept {q:?}\n{e}");
    }
}

/// The grammar rejects the input.
#[track_caller]
fn g_no(q: &str) {
    if CypherParser::parse(Rule::query, q).is_ok() {
        panic!("grammar should reject {q:?}");
    }
}

/// The input parses *and* lowers to a runnable AST.
#[track_caller]
fn ok(q: &str) -> Query {
    parse(q).unwrap_or_else(|e| panic!("should lower {q:?}\n{e}"))
}

/// The input fails to parse or lower; returns the message.
#[track_caller]
fn err(q: &str) -> String {
    match parse(q) {
        Ok(_) => panic!("should reject {q:?}"),
        Err(e) => e.to_string(),
    }
}

/// Lower `RETURN <lit>` and pull the single projected literal back out, so a test
/// can assert the literal's *value*, not merely that it parsed.
///
/// A leading `-` is folded. The reference has no negative literal: `IntegerLiteral`
/// is unsigned and negation is `UnaryAddOrSubtractExpression`, so `-1` lowers to
/// `Neg(Literal(1))`. Slater agrees, and this helper unwraps that one layer so the
/// tests can talk about values.
#[track_caller]
fn lit(source: &str) -> Value {
    match expr_of(source) {
        Expr::Literal(v) => v,
        Expr::Neg(inner) => match *inner {
            Expr::Literal(Value::Int(i)) => Value::Int(-i),
            Expr::Literal(Value::Float(f)) => Value::Float(-f),
            other => panic!("{source:?} negated a non-literal: {other:?}"),
        },
        other => panic!("{source:?} did not lower to a literal: {other:?}"),
    }
}

/// Lower `RETURN <expr>` and return the projected expression.
#[track_caller]
fn expr_of(source: &str) -> Expr {
    ok(&format!("RETURN {source}")).head.ret.body.items[0]
        .expr
        .clone()
}

// ── Cypher / Statement / Query / RegularQuery / Union ────────────────────────

#[test]
fn cypher_allows_surrounding_space_and_one_trailing_semicolon() {
    // Cypher = SP? ~ Statement ~ (SP? ~ ";")? ~ SP? ~ EOI
    g("RETURN 1");
    g("  RETURN 1  ");
    g("RETURN 1;");
    g("RETURN 1 ;");
    g("RETURN 1;  ");
    g("\n\tRETURN 1\n");
    // Slater serves one statement per RUN, so a `;`-separated batch is not a Query.
    g_no("RETURN 1; RETURN 2");
    // ...and only one terminator.
    g_no("RETURN 1;;");
}

#[test]
fn regular_query_unions_single_queries() {
    // RegularQuery = SingleQuery ~ (SP? ~ Union)*
    // Union = UNION ~ (SP ~ ALL)? ~ SP? ~ SingleQuery
    ok("RETURN 1 UNION RETURN 2");
    ok("RETURN 1 UNION ALL RETURN 2");
    ok("RETURN 1 UNION RETURN 2 UNION RETURN 3");
    ok("RETURN 1 UNION ALL RETURN 2 UNION RETURN 3");
    ok("MATCH (a) RETURN a UNION MATCH (b) RETURN b");
    // Case-insensitive, like every keyword.
    ok("RETURN 1 union all RETURN 2");
    ok("RETURN 1 UnIoN AlL RETURN 2");

    let q = ok("RETURN 1 UNION ALL RETURN 2 UNION RETURN 3");
    assert_eq!(q.tail.len(), 2);
    assert!(q.tail[0].0, "UNION ALL keeps duplicates");
    assert!(!q.tail[1].0, "bare UNION is distinct");

    g_no("RETURN 1 UNION");
    g_no("UNION RETURN 1");
}

#[test]
fn multi_part_query_chains_with_clauses() {
    // MultiPartQuery = ((ReadingClause SP?)* (UpdatingClause SP?)* With SP?)+ SinglePartQuery
    ok("MATCH (n) WITH n RETURN n");
    ok("MATCH (n) WITH n AS m RETURN m");
    ok("MATCH (n) WITH n WHERE n.x > 1 RETURN n");
    ok("MATCH (a) WITH a MATCH (b) WITH a, b RETURN a, b");
    ok("MATCH (n) WITH DISTINCT n RETURN n");
    ok("UNWIND [1, 2] AS x WITH x WHERE x > 1 RETURN x");
    // WITH * projects everything in scope.
    ok("MATCH (n) WITH * RETURN n");
    ok("MATCH (n) WITH *, n.x AS x RETURN x");
    // A WITH may carry the full ProjectionBody tail.
    ok("MATCH (n) WITH n ORDER BY n.x DESC SKIP 1 LIMIT 2 RETURN n");
}

// ── ReadingClause: Match / Unwind / InQueryCall ──────────────────────────────

#[test]
fn match_clause_optional_multi_pattern_and_where() {
    // Match = (OPTIONAL SP)? MATCH SP? Pattern (SP? Where)?
    ok("MATCH (n) RETURN n");
    ok("OPTIONAL MATCH (n) RETURN n");
    ok("optional match (n) RETURN n");
    ok("MATCH (a), (b) RETURN a, b");
    ok("MATCH (a), (b), (c) RETURN a");
    ok("MATCH (n) WHERE n.x = 1 RETURN n");
    ok("OPTIONAL MATCH (a)-[:R]->(b) WHERE b.x IS NULL RETURN a");
    ok("MATCH (a) MATCH (b) RETURN a, b");
    // MATCH with no pattern is not a Match.
    g_no("MATCH RETURN 1");
    g_no("OPTIONAL RETURN 1");
}

#[test]
fn unwind_clause() {
    // Unwind = UNWIND SP? Expression SP AS SP Variable
    ok("UNWIND [1, 2, 3] AS x RETURN x");
    ok("UNWIND $rows AS r RETURN r");
    ok("UNWIND [[1], [2]] AS xs UNWIND xs AS x RETURN x");
    ok("MATCH (n) UNWIND n.list AS x RETURN x");
    ok("unwind [1] as x RETURN x");
    g_no("UNWIND [1] RETURN 1");
    g_no("UNWIND AS x RETURN x");
    // The `AS` alias is a Variable, so it may not be a bare literal.
    g_no("UNWIND [1] AS 2 RETURN 1");
}

// ── With / Return / ProjectionBody / ProjectionItems ─────────────────────────

#[test]
fn projection_body_distinct_items_order_skip_limit() {
    // ProjectionBody = (SP? DISTINCT)? SP ProjectionItems (SP Order)? (SP Skip)? (SP Limit)?
    ok("MATCH (n) RETURN DISTINCT n");
    ok("MATCH (n) RETURN n ORDER BY n.x");
    ok("MATCH (n) RETURN n SKIP 1");
    ok("MATCH (n) RETURN n LIMIT 1");
    ok("MATCH (n) RETURN n ORDER BY n.x SKIP 1 LIMIT 2");
    ok("MATCH (n) RETURN DISTINCT n ORDER BY n.x SKIP 1 LIMIT 2");
    // The tail clauses are strictly ordered.
    g_no("MATCH (n) RETURN n LIMIT 1 SKIP 2");
    g_no("MATCH (n) RETURN n SKIP 1 ORDER BY n.x");
}

#[test]
fn projection_items_star_and_aliases() {
    // ProjectionItems = (STAR ("," ProjectionItem)*) | (ProjectionItem ("," ProjectionItem)*)
    // ProjectionItem  = (Expression SP AS SP Variable) | Expression
    ok("MATCH (n) RETURN *");
    ok("MATCH (n) RETURN *, n.x");
    ok("MATCH (n) RETURN *, n.x AS x, n.y AS y");
    ok("RETURN 1 AS one");
    ok("RETURN 1 AS one, 2 AS two");
    ok("RETURN 1, 2, 3");
    // `*` may only lead.
    g_no("MATCH (n) RETURN n.x, *");

    let q = ok("RETURN 1 AS one");
    assert_eq!(q.head.ret.body.items[0].alias.as_deref(), Some("one"));
}

#[test]
fn order_sort_item_skip_limit() {
    // Order = ORDER SP BY SP SortItem ("," SP? SortItem)*
    // SortItem = Expression (SP? (ASCENDING | ASC | DESCENDING | DESC))?
    ok("MATCH (n) RETURN n ORDER BY n.x");
    ok("MATCH (n) RETURN n ORDER BY n.x ASC");
    ok("MATCH (n) RETURN n ORDER BY n.x ASCENDING");
    ok("MATCH (n) RETURN n ORDER BY n.x DESC");
    ok("MATCH (n) RETURN n ORDER BY n.x DESCENDING");
    ok("MATCH (n) RETURN n ORDER BY n.x ASC, n.y DESC");
    ok("MATCH (n) RETURN n ORDER BY n.x + n.y DESC");
    // Skip = SKIP SP Expression; Limit = LIMIT SP Expression — any expression.
    ok("MATCH (n) RETURN n SKIP 1 + 1");
    ok("MATCH (n) RETURN n SKIP $s LIMIT $l");
    g_no("MATCH (n) RETURN n ORDER n.x");
    g_no("MATCH (n) RETURN n ORDER BY");
}

#[test]
fn where_clause() {
    // Where = WHERE SP Expression
    ok("MATCH (n) WHERE n.x RETURN n");
    ok("MATCH (n) WHERE true RETURN n");
    ok("MATCH (n) WHERE n.a = 1 AND n.b = 2 RETURN n");
    g_no("MATCH (n) WHERE RETURN n");
}

// ── Pattern / PatternPart / PatternElement ───────────────────────────────────

#[test]
fn pattern_part_binds_a_path_variable() {
    // PatternPart = (Variable SP? "=" SP?)? AnonymousPatternPart
    ok("MATCH p = (a)-[:R]->(b) RETURN p");
    ok("MATCH p = (a) RETURN p");
    ok("MATCH p = (a)-[:R]->(b), q = (c)-[:S]->(d) RETURN p, q");
    g_no("MATCH = (a) RETURN 1");
}

#[test]
fn pattern_element_may_be_parenthesised() {
    // PatternElement = (NodePattern PatternElementChain*) | ("(" PatternElement ")")
    // Redundant parentheses group but carry no meaning; nesting is unbounded.
    ok("MATCH ((a)-[:R]->(b)) RETURN a");
    ok("MATCH (((a)-[:R]->(b))) RETURN a");
    ok("MATCH ((n)) RETURN n");
    ok("MATCH p = ((a)-[:R]->(b)) RETURN p");
    ok("MATCH ((a)-[:R]->(b)-[:S]->(c)) RETURN a");
    // The parentheses really do vanish: this lowers exactly like the bare form.
    assert_eq!(
        ok("MATCH ((a)-[:R]->(b)) RETURN a").head.reading,
        ok("MATCH (a)-[:R]->(b) RETURN a").head.reading,
    );
    // An unbalanced wrap is still a syntax error.
    g_no("MATCH ((a)-[:R]->(b) RETURN a");
}

#[test]
fn node_pattern_variable_labels_properties() {
    // NodePattern = "(" SP? (Variable SP?)? (NodeLabels SP?)? (Properties SP?)? ")"
    ok("MATCH () RETURN 1");
    ok("MATCH (n) RETURN n");
    ok("MATCH (:Label) RETURN 1");
    ok("MATCH (n:Label) RETURN n");
    ok("MATCH (n:A:B) RETURN n");
    ok("MATCH ({k: 1}) RETURN 1");
    ok("MATCH (n {k: 1}) RETURN n");
    ok("MATCH (:Label {k: 1}) RETURN 1");
    ok("MATCH (n:Label {k: 1, j: 2}) RETURN n");
    // Whitespace is permitted at every seam.
    ok("MATCH ( n : Label { k : 1 } ) RETURN n");
    ok("MATCH (n:A :B) RETURN n");
}

#[test]
fn relationship_pattern_all_four_directions() {
    // RelationshipPattern = the four arrow arms
    ok("MATCH (a)-->(b) RETURN a"); // undirected detail, right arrow
    ok("MATCH (a)<--(b) RETURN a");
    ok("MATCH (a)--(b) RETURN a");
    ok("MATCH (a)-[:R]->(b) RETURN a");
    ok("MATCH (a)<-[:R]-(b) RETURN a");
    ok("MATCH (a)-[:R]-(b) RETURN a");
    // Both heads at once is a parse-level pattern but a lowering error.
    assert!(err("MATCH (a)<-[:R]->(b) RETURN a").contains("both directions"));
}

#[test]
fn relationship_detail_variable_types_range_properties() {
    // RelationshipDetail = "[" (Variable)? (RelationshipTypes)? RangeLiteral? (Properties)? "]"
    ok("MATCH (a)-[]->(b) RETURN a");
    ok("MATCH (a)-[r]->(b) RETURN r");
    ok("MATCH (a)-[:R]->(b) RETURN a");
    ok("MATCH (a)-[r:R]->(b) RETURN r");
    ok("MATCH (a)-[r:R*]->(b) RETURN r");
    ok("MATCH (a)-[r:R*1..2]->(b) RETURN r");
    ok("MATCH (a)-[r:R {k: 1}]->(b) RETURN r");
    ok("MATCH (a)-[r:R*1..2 {k: 1}]->(b) RETURN r");
    ok("MATCH (a)-[ r : R * 1 .. 2 { k : 1 } ]->(b) RETURN r");
    // The element order is fixed: properties may not precede the range.
    g_no("MATCH (a)-[r:R {k: 1} *1..2]->(b) RETURN r");
}

#[test]
fn relationship_types_alternation() {
    // RelationshipTypes = ":" RelTypeName (SP? "|" ":"? SP? RelTypeName)*
    ok("MATCH (a)-[:R]->(b) RETURN a");
    ok("MATCH (a)-[:R|S]->(b) RETURN a");
    ok("MATCH (a)-[:R|:S]->(b) RETURN a");
    ok("MATCH (a)-[:R|S|T]->(b) RETURN a");
    ok("MATCH (a)-[:R | :S]->(b) RETURN a");
}

#[test]
fn node_labels() {
    // NodeLabels = NodeLabel (SP? NodeLabel)*; NodeLabel = ":" SP? LabelName
    ok("MATCH (n:A) RETURN n");
    ok("MATCH (n:A:B:C) RETURN n");
    ok("MATCH (n: A) RETURN n");
    // NodeLabels also appear as a postfix predicate on an expression.
    ok("MATCH (n) RETURN n:A");
    ok("MATCH (n) RETURN n:A:B");
    ok("MATCH (n) WHERE n:A RETURN n");
}

#[test]
fn range_literal_every_shape() {
    // RangeLiteral = "*" SP? (IntegerLiteral SP?)? (".." SP? (IntegerLiteral SP?)?)?
    ok("MATCH (a)-[:R*]->(b) RETURN a"); // no bounds
    ok("MATCH (a)-[:R*2]->(b) RETURN a"); // exact
    ok("MATCH (a)-[:R*1..3]->(b) RETURN a"); // both
    ok("MATCH (a)-[:R*..3]->(b) RETURN a"); // upper only
    ok("MATCH (a)-[:R*2..]->(b) RETURN a"); // lower only
    ok("MATCH (a)-[:R* 1 .. 3]->(b) RETURN a");

    // RangeLiteral bounds are IntegerLiteral, so they share its radices.
    let vl = |q: &str| match &ok(q).head.reading[0] {
        Clause::Match(m) => m.patterns[0].rels[0].0.var_length.unwrap(),
        other => panic!("expected a MATCH, got {other:?}"),
    };
    let v = vl("MATCH (a)-[:R*017]->(b) RETURN a");
    assert_eq!(
        (v.min, v.max),
        (Some(15), Some(15)),
        "*017 is 15 hops, octal"
    );
    let v = vl("MATCH (a)-[:R*0x10..0x20]->(b) RETURN a");
    assert_eq!((v.min, v.max), (Some(16), Some(32)), "hex bounds");
}

// ── SchemaName: labels, rel types and property keys may be reserved words ─────

#[test]
fn schema_name_admits_reserved_words() {
    // SchemaName = SymbolicName | ReservedWord, used by LabelName, RelTypeName and
    // PropertyKeyName. Every one of these is legal openCypher and every one of
    // them was rejected before this suite existed.
    for kw in [
        "MATCH", "Order", "By", "Skip", "Limit", "Where", "Return", "With", "End", "In", "Is",
        "Not", "Null", "True", "False", "Case", "When", "Then", "Else", "Contains", "Starts",
        "Ends", "Union", "Distinct", "As", "And", "Or", "Xor", "Asc", "Desc", "Call", "Yield",
        "Unwind", "For", "Create", "Delete", "Set", "Remove", "Detach", "Merge", "Optional",
    ] {
        // LabelName
        ok(&format!("MATCH (n:{kw}) RETURN n"));
        // RelTypeName
        ok(&format!("MATCH ()-[:{kw}]->() RETURN 1"));
        // PropertyKeyName, in a map literal, a pattern and a property lookup
        ok(&format!("RETURN {{{kw}: 1}}"));
        ok(&format!("MATCH (n {{{kw}: 1}}) RETURN n"));
        ok(&format!("MATCH (n) RETURN n.{kw}"));
    }
}

#[test]
fn reserved_property_keys_do_not_swallow_the_following_clause() {
    // The interesting failure mode of the fix above: a reserved property key must
    // not eat the clause keyword that follows it.
    let q = ok("MATCH (n) RETURN n.order ORDER BY n.x");
    assert_eq!(q.head.ret.body.order_by.len(), 1, "ORDER BY still parsed");

    let q = ok("MATCH (n) RETURN n.skip SKIP 1");
    assert!(q.head.ret.body.skip.is_some(), "SKIP still parsed");

    let q = ok("MATCH (n) RETURN n.limit LIMIT 1");
    assert!(q.head.ret.body.limit.is_some(), "LIMIT still parsed");

    // `DESC` is only a SortItem suffix, so it is a stray token here — in Slater
    // and in the reference alike.
    g_no("MATCH (n) RETURN n.desc DESC");

    ok("MATCH (n) RETURN n.x ORDER BY n.desc DESC");
    ok("MATCH (n) WHERE n.contains CONTAINS 'a' RETURN n");
    ok("MATCH (n) WHERE n.is IS NULL RETURN n");
    ok("MATCH (n) WHERE n.in IN [1] RETURN n");
    ok("MATCH (n) WHERE n.not AND n.and RETURN n");
    ok("MATCH (n) RETURN n.as AS as");
    ok("MATCH (n) RETURN CASE WHEN n.when THEN n.then ELSE n.else END");
}

#[test]
fn keyword_word_boundary_is_still_enforced() {
    // The `!ident_cont` guard on every kw_* terminal. Dropping the reserved-word
    // exclusion must not let a keyword match a prefix of a longer name.
    ok("MATCH (n) RETURN n.orders"); // not ORDER
    ok("MATCH (n) RETURN n.ends"); // property `ends`, not ENDS WITH
    ok("MATCH (returns) RETURN returns");
    ok("MATCH (n) WHERE n.index > 1 RETURN n"); // `in` is not a prefix match
    ok("MATCH (notable) RETURN notable"); // not NOT
    ok("MATCH (n) RETURN n.counted");
    // `RETURNS 1` is not `RETURN S`.
    g_no("RETURNS 1");
    g_no("UNWINDx AS y RETURN y");
}

// ── Expression precedence chain ──────────────────────────────────────────────

#[test]
fn boolean_operator_precedence_or_xor_and_not() {
    // OrExpression > XorExpression > AndExpression > NotExpression
    ok("RETURN true OR false");
    ok("RETURN true XOR false");
    ok("RETURN true AND false");
    ok("RETURN NOT true");
    ok("RETURN NOT NOT true");
    ok("RETURN true OR false XOR true AND NOT false");
    ok("RETURN true or false xor true and not false");

    // OR binds loosest: `a OR (b AND c)`.
    assert_eq!(
        expr_of("true OR false AND false"),
        expr_of("true OR (false AND false)")
    );
    assert_ne!(
        expr_of("true OR false AND false"),
        expr_of("(true OR false) AND false")
    );
    // XOR sits between OR and AND: `Or > Xor > And`, so AND binds tightest.
    // Slater used to nest `Or > And > Xor`, which grouped `a XOR b AND c` as
    // `(a XOR b) AND c` — the same tokens, a different truth table.
    assert_eq!(
        expr_of("true OR false XOR true"),
        expr_of("true OR (false XOR true)")
    );
    assert_eq!(
        expr_of("true XOR false AND true"),
        expr_of("true XOR (false AND true)")
    );
    assert_ne!(
        expr_of("true XOR false AND true"),
        expr_of("(true XOR false) AND true")
    );
    assert_eq!(
        expr_of("true AND false XOR true AND false"),
        expr_of("(true AND false) XOR (true AND false)")
    );
    // NOT binds tighter than all three.
    assert_eq!(
        expr_of("NOT true AND false"),
        expr_of("(NOT true) AND false")
    );
}

#[test]
fn arithmetic_operator_precedence() {
    // AddOrSubtract > MultiplyDivideModulo > PowerOf > UnaryAddOrSubtract
    ok("RETURN 1 + 2");
    ok("RETURN 1 - 2");
    ok("RETURN 1 * 2");
    ok("RETURN 1 / 2");
    ok("RETURN 1 % 2");
    ok("RETURN 2 ^ 3");
    ok("RETURN -1");
    ok("RETURN +1");
    ok("RETURN --1");
    ok("RETURN 1 + 2 * 3 ^ 4");

    assert_eq!(expr_of("1 + 2 * 3"), expr_of("1 + (2 * 3)"));
    assert_eq!(expr_of("2 * 3 ^ 4"), expr_of("2 * (3 ^ 4)"));
    // `^` is left-associative in the reference, and binds looser than a unary sign,
    // so `-2 ^ 2` is `(-2) ^ 2`.
    assert_eq!(expr_of("-2 ^ 2"), expr_of("(-2) ^ 2"));
    assert_eq!(expr_of("2 ^ 3 ^ 2"), expr_of("(2 ^ 3) ^ 2"));
    // Subtraction is left-associative.
    assert_eq!(expr_of("1 - 2 - 3"), expr_of("(1 - 2) - 3"));
}

#[test]
fn partial_comparison_expression_all_six() {
    // PartialComparisonExpression = (EQ | NE | LT | GT | LE | GE) SP? AddOrSubtract
    ok("RETURN 1 = 2");
    ok("RETURN 1 <> 2");
    ok("RETURN 1 < 2");
    ok("RETURN 1 > 2");
    ok("RETURN 1 <= 2");
    ok("RETURN 1 >= 2");
    // Chained comparisons are legal (the reference allows `PartialComparison*`).
    ok("RETURN 1 < 2 < 3");

    // The two-character operators must win over their one-character prefixes.
    assert_eq!(expr_of("1 <= 2"), expr_of("1 <= 2"));
    assert_ne!(expr_of("1 <= 2"), expr_of("1 < 2"));
    assert_ne!(expr_of("1 >= 2"), expr_of("1 > 2"));
    assert_ne!(expr_of("1 <> 2"), expr_of("1 < 2"));
}

#[test]
fn string_list_and_null_operator_expressions() {
    // StringOperatorExpression / ListOperatorExpression / NullOperatorExpression
    ok("MATCH (n) WHERE n.x STARTS WITH 'a' RETURN n");
    ok("MATCH (n) WHERE n.x ENDS WITH 'a' RETURN n");
    ok("MATCH (n) WHERE n.x CONTAINS 'a' RETURN n");
    ok("MATCH (n) WHERE n.x starts with 'a' RETURN n");
    ok("RETURN 1 IN [1, 2]");
    ok("RETURN 'a' IN ['a']");
    ok("MATCH (n) WHERE n.x IS NULL RETURN n");
    ok("MATCH (n) WHERE n.x IS NOT NULL RETURN n");
    // Index and slice are ListOperatorExpression arms.
    ok("RETURN [1, 2, 3][0]");
    ok("RETURN [1, 2, 3][0..2]");
    ok("RETURN [1, 2, 3][..2]");
    ok("RETURN [1, 2, 3][1..]");
    ok("RETURN [1, 2, 3][..]");
    ok("RETURN $list[$i]");
    // These postfix operators chain.
    ok("MATCH (n) WHERE n.x STARTS WITH 'a' AND n.y IS NOT NULL RETURN n");
}

#[test]
fn property_or_labels_expression() {
    // PropertyOrLabelsExpression = Atom (SP? PropertyLookup)* (SP? NodeLabels)?
    ok("MATCH (n) RETURN n.a");
    ok("MATCH (n) RETURN n.a.b");
    ok("MATCH (n) RETURN n.a.b.c");
    ok("MATCH (n) RETURN n . a");
    ok("MATCH (n) RETURN n:Label");
    ok("MATCH (n) RETURN n.a:Label");
    ok("RETURN $p.field");
    ok("RETURN {a: 1}.a");
}

// ── Atom ─────────────────────────────────────────────────────────────────────

#[test]
fn atom_count_star_and_filter_functions() {
    // Atom = ... | COUNT "(" STAR ")" | ALL/ANY/NONE/SINGLE "(" FilterExpression ")"
    ok("MATCH (n) RETURN count(*)");
    ok("MATCH (n) RETURN COUNT(*)");
    ok("MATCH (n) RETURN count( * )");
    // FilterExpression = IdInColl (SP? Where)?; IdInColl = Variable SP IN SP Expression
    ok("RETURN all(x IN [1, 2] WHERE x > 0)");
    ok("RETURN any(x IN [1, 2] WHERE x > 0)");
    ok("RETURN none(x IN [1, 2] WHERE x > 0)");
    ok("RETURN single(x IN [1, 2] WHERE x > 0)");
    ok("RETURN ALL(x IN $l WHERE x IS NOT NULL)");
    // ...and the same names remain usable as ordinary variables.
    ok("MATCH (any) RETURN any");
    ok("MATCH (none) RETURN none");
    ok("MATCH (single) RETURN single");
    ok("MATCH (count) RETURN count");
    ok("MATCH (filter) RETURN filter");
    ok("MATCH (extract) RETURN extract");
}

#[test]
fn atom_parenthesized_expression_and_relationships_pattern() {
    // ParenthesizedExpression = "(" SP? Expression SP? ")"
    ok("RETURN (1)");
    ok("RETURN (1 + 2) * 3");
    ok("RETURN ( 1 )");
    assert_eq!(expr_of("(1 + 2) * 3"), expr_of("(1 + 2) * 3"));
    assert_ne!(expr_of("(1 + 2) * 3"), expr_of("1 + 2 * 3"));

    // RelationshipsPattern = NodePattern PatternElementChain+ — a pattern used as
    // a boolean atom. One relationship is required, which is what keeps it apart
    // from a ParenthesizedExpression.
    ok("MATCH (a) WHERE (a)-[:R]->() RETURN a");
    ok("MATCH (a) WHERE (a)-[:R]->()-[:S]->() RETURN a");
    ok("MATCH (a) WHERE NOT (a)-[:R]->() RETURN a");
    // A bare `(a)` is a parenthesised variable, not a pattern.
    ok("MATCH (a) WHERE (a) IS NOT NULL RETURN a");
}

#[test]
fn case_expression_simple_and_generic() {
    // CaseExpression = (CASE CaseAlternative+) | (CASE Expression CaseAlternative+)
    //                  (ELSE Expression)? END
    ok("RETURN CASE WHEN true THEN 1 END");
    ok("RETURN CASE WHEN true THEN 1 ELSE 2 END");
    ok("RETURN CASE WHEN true THEN 1 WHEN false THEN 2 ELSE 3 END");
    ok("RETURN CASE 1 WHEN 1 THEN 'a' END");
    ok("RETURN CASE 1 WHEN 1 THEN 'a' ELSE 'b' END");
    ok("RETURN CASE 1 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END");
    ok("RETURN case when true then 1 else 2 end");
    ok("MATCH (n) RETURN CASE n.x WHEN 1 THEN n.y ELSE n.z END");
    // Nested.
    ok("RETURN CASE WHEN true THEN CASE WHEN false THEN 1 ELSE 2 END END");
    // At least one WHEN is required, and END is mandatory.
    g_no("RETURN CASE ELSE 1 END");
    g_no("RETURN CASE WHEN true THEN 1");
}

#[test]
fn list_and_pattern_comprehension() {
    // ListComprehension = "[" FilterExpression ("|" Expression)? "]"
    ok("RETURN [x IN [1, 2] WHERE x > 1 | x * 2]");
    ok("RETURN [x IN [1, 2] WHERE x > 1]");
    ok("RETURN [x IN [1, 2] | x * 2]");
    ok("RETURN [x IN $l WHERE x IS NOT NULL | x]");
    // PatternComprehension = "[" (Variable "=")? RelationshipPattern (Where)? "|" Expression "]"
    ok("MATCH (a) RETURN [(a)-[:R]->(b) | b]");
    ok("MATCH (a) RETURN [(a)-[:R]->(b) WHERE b.x > 1 | b.x]");
    ok("MATCH (a) RETURN [(a)-[:R]->(b)-[:S]->(c) | c]");
    // A one-element list is still a list literal, not a comprehension.
    ok("RETURN [x]");
    ok("MATCH (a) RETURN [a]");
}

#[test]
fn existential_subquery_pattern_form() {
    // ExistentialSubquery = EXISTS "{" (RegularQuery | Pattern Where?) "}"
    ok("MATCH (a) WHERE EXISTS { (a)-[:R]->() } RETURN a");
    ok("MATCH (a) WHERE EXISTS { MATCH (a)-[:R]->() } RETURN a");
    ok("MATCH (a) WHERE EXISTS { (a)-[:R]->(b) WHERE b.x > 1 } RETURN a");
    ok("MATCH (a) WHERE EXISTS { (a)-[:R]->(), (a)-[:S]->() } RETURN a");
    ok("MATCH (a) WHERE NOT EXISTS { (a)-[:R]->() } RETURN a");
    ok("MATCH (a) WHERE exists { (a)-[:R]->() } RETURN a");
}

#[test]
fn function_invocation_namespace_and_distinct() {
    // FunctionInvocation = FunctionName "(" (DISTINCT)? (Expression ("," Expression)*)? ")"
    // FunctionName = Namespace SymbolicName; Namespace = (SymbolicName ".")*
    ok("RETURN f()");
    ok("RETURN f(1)");
    ok("RETURN f(1, 2)");
    ok("RETURN f( 1 , 2 )");
    ok("RETURN a.f(1)");
    ok("RETURN a.b.c(1)");
    ok("MATCH (n) RETURN count(DISTINCT n)");
    ok("MATCH (n) RETURN collect(DISTINCT n.x)");
    ok("RETURN toInteger('1')");
    ok("RETURN f(g(h(1)))");
    // A namespaced name must not be confused with a property lookup.
    ok("MATCH (n) RETURN n.a");
}

#[test]
fn parameter_symbolic_and_positional() {
    // Parameter = "$" (SymbolicName | DecimalInteger)
    ok("RETURN $p");
    ok("RETURN $param_1");
    ok("RETURN $0");
    ok("RETURN $1");
    ok("RETURN $42");
    ok("RETURN $`odd name`");
    // A parameter name may be a reserved word.
    ok("RETURN $limit");
    ok("RETURN $order");
    ok("MATCH (n) WHERE n.x = $0 RETURN n");
    assert_eq!(expr_of("$0"), Expr::Param("0".into()));
    assert_eq!(expr_of("$limit"), Expr::Param("limit".into()));
    g_no("RETURN $");
}

// ── Literal ──────────────────────────────────────────────────────────────────

#[test]
fn decimal_integer_literal() {
    // DecimalInteger = ZeroDigit | (NonZeroDigit Digit*)
    assert_eq!(lit("0"), Value::Int(0));
    assert_eq!(lit("1"), Value::Int(1));
    assert_eq!(lit("123"), Value::Int(123));
    assert_eq!(lit("-1"), Value::Int(-1));
    assert_eq!(lit("9223372036854775807"), Value::Int(i64::MAX));
    // `-9223372036854775808` is unary minus applied to a magnitude one past
    // `i64::MAX`, so it overflows while lowering — as it does in the reference,
    // where the sign is likewise not part of the literal.
    assert!(err("RETURN -9223372036854775808").contains("bad integer"));
    // A leading zero followed by a non-octal digit is neither octal nor decimal.
    g_no("RETURN 08");
    g_no("RETURN 09");
}

#[test]
fn octal_literal_is_base_eight() {
    // OctalInteger = ZeroDigit OctDigit+
    //
    // The regression this whole suite exists for: before the fix, `RETURN 017`
    // parsed happily and evaluated to 17. The reference says octal — 15.
    assert_eq!(lit("017"), Value::Int(15));
    assert_eq!(lit("07"), Value::Int(7));
    assert_eq!(lit("0777"), Value::Int(511));
    assert_eq!(lit("-017"), Value::Int(-15));
    assert_eq!(lit("00"), Value::Int(0));
    // `0` alone is DecimalInteger, not a zero-length octal.
    assert_eq!(lit("0"), Value::Int(0));
}

#[test]
fn hex_integer_literal() {
    // HexInteger = "0x" HexDigit+ ; HexDigit = Digit | HexLetter (A-F, case-insensitive)
    assert_eq!(lit("0x1F"), Value::Int(31));
    assert_eq!(lit("0x1f"), Value::Int(31));
    assert_eq!(lit("0xff"), Value::Int(255));
    assert_eq!(lit("0xFF"), Value::Int(255));
    assert_eq!(lit("0x0"), Value::Int(0));
    assert_eq!(lit("-0x10"), Value::Int(-16));
    assert_eq!(lit("0xabcdef"), Value::Int(11259375));
    // Slater additionally accepts `0X` (a superset of the reference's `0x`).
    assert_eq!(lit("0X1F"), Value::Int(31));
    // `0x` needs at least one digit; `g` is not a hex digit.
    g_no("RETURN 0x");
    g_no("RETURN 0xg");
}

#[test]
fn double_literal_regular_and_exponent() {
    // DoubleLiteral = ExponentDecimalReal | RegularDecimalReal
    // RegularDecimalReal = Digit* "." Digit+
    assert_eq!(lit("1.5"), Value::Float(1.5));
    assert_eq!(lit(".5"), Value::Float(0.5));
    assert_eq!(lit("0.5"), Value::Float(0.5));
    assert_eq!(lit("-1.5"), Value::Float(-1.5));
    assert_eq!(lit("123.456"), Value::Float(123.456));
    // ExponentDecimalReal = (Digit+ | Digit+ "." Digit+ | "." Digit+) "E" "-"? Digit+
    assert_eq!(lit("1e3"), Value::Float(1000.0));
    assert_eq!(lit("1E3"), Value::Float(1000.0));
    assert_eq!(lit("1.5e2"), Value::Float(150.0));
    assert_eq!(lit("1.5E-2"), Value::Float(0.015));
    assert_eq!(lit(".5e1"), Value::Float(5.0));
    assert_eq!(lit("-1.5e-2"), Value::Float(-0.015));
    // Slater additionally accepts an explicit `+` exponent sign (a superset).
    assert_eq!(lit("1e+3"), Value::Float(1000.0));
    // A double beats an integer: `017.5` is a real, not an octal followed by junk.
    assert_eq!(lit("017.5"), Value::Float(17.5));
    // A trailing dot with no fraction is not a RegularDecimalReal.
    g_no("RETURN 1.");
}

#[test]
fn boolean_and_null_literals() {
    // BooleanLiteral = TRUE | FALSE ; NULL
    assert_eq!(lit("true"), Value::Bool(true));
    assert_eq!(lit("TRUE"), Value::Bool(true));
    assert_eq!(lit("TrUe"), Value::Bool(true));
    assert_eq!(lit("false"), Value::Bool(false));
    assert_eq!(lit("FALSE"), Value::Bool(false));
    assert_eq!(lit("null"), Value::Null);
    assert_eq!(lit("NULL"), Value::Null);
    // The word boundary keeps `nullable` a name, not NULL followed by `able`.
    ok("MATCH (nullable) RETURN nullable");
    ok("MATCH (trueish) RETURN trueish");
}

#[test]
fn string_literal_quoting_and_escapes() {
    // StringLiteral = '"' StringDoubleText '"' | "'" StringSingleText "'"
    assert_eq!(lit("'a'"), Value::Str("a".into()));
    assert_eq!(lit("\"a\""), Value::Str("a".into()));
    assert_eq!(lit("''"), Value::Str("".into()));
    assert_eq!(lit("\"\""), Value::Str("".into()));
    // The other quote passes through unescaped.
    assert_eq!(lit("'he said \"hi\"'"), Value::Str("he said \"hi\"".into()));
    assert_eq!(lit("\"it's\""), Value::Str("it's".into()));
    // EscapedChar = "\\" ("\\" | "'" | '"' | B | F | N | R | T | U hex{4} | U hex{8})
    assert_eq!(lit(r"'\\'"), Value::Str("\\".into()));
    assert_eq!(lit(r"'\''"), Value::Str("'".into()));
    assert_eq!(lit(r#""\"""#), Value::Str("\"".into()));
    assert_eq!(lit(r"'\n'"), Value::Str("\n".into()));
    assert_eq!(lit(r"'\r'"), Value::Str("\r".into()));
    assert_eq!(lit(r"'\t'"), Value::Str("\t".into()));
    assert_eq!(lit(r"'\b'"), Value::Str("\u{08}".into()));
    assert_eq!(lit(r"'\f'"), Value::Str("\u{0C}".into()));
    // Unicode content needs no escaping at all.
    assert_eq!(lit("'héllo — 世界'"), Value::Str("héllo — 世界".into()));
    g_no("RETURN 'unterminated");
    g_no("RETURN \"mismatched'");
}

#[test]
fn list_and_map_literals() {
    // ListLiteral = "[" (Expression ("," Expression)*)? "]"
    ok("RETURN []");
    ok("RETURN [1]");
    ok("RETURN [1, 2, 3]");
    ok("RETURN [ 1 , 2 ]");
    ok("RETURN [1, 'a', true, null]");
    ok("RETURN [[1], [2]]");
    ok("RETURN [1 + 1, $p]");
    // MapLiteral = "{" (PropertyKeyName ":" Expression ("," ...)*)? "}"
    ok("RETURN {}");
    ok("RETURN {a: 1}");
    ok("RETURN {a: 1, b: 2}");
    ok("RETURN { a : 1 }");
    ok("RETURN {a: {b: 1}}");
    ok("RETURN {a: [1, 2]}");
    ok("RETURN {a: $p}");
    g_no("RETURN {1: 2}"); // key must be a PropertyKeyName
    g_no("RETURN [1,]");
}

// ── SymbolicName / Variable ──────────────────────────────────────────────────

#[test]
fn unescaped_symbolic_name_is_unicode() {
    // IdentifierStart = ID_Start | Pc ; IdentifierPart = ID_Continue | Sc
    ok("MATCH (n) RETURN n");
    ok("MATCH (_n) RETURN _n"); // Pc (connector punctuation) starts a name
    ok("MATCH (n_1) RETURN n_1");
    ok("MATCH (café) RETURN café"); // ID_Start beyond ASCII
    ok("MATCH (π) RETURN π");
    ok("MATCH (世界) RETURN 世界");
    ok("MATCH (naïve) RETURN naïve");
    ok("MATCH (a$b) RETURN a$b"); // Sc (currency symbol) continues a name
    ok("MATCH (n) RETURN n.café");
    ok("MATCH (n:Étiquette) RETURN n");
    // A name may not start with a digit.
    g_no("MATCH (1n) RETURN 1n");
}

#[test]
fn escaped_symbolic_name() {
    // EscapedSymbolicName = ("`" (!"`" ANY)* "`")+
    ok("MATCH (`odd name`) RETURN `odd name`");
    ok("MATCH (`n`) RETURN `n`");
    ok("MATCH (n:`Label With Space`) RETURN n");
    ok("MATCH ()-[:`REL TYPE`]->() RETURN 1");
    ok("MATCH (n) RETURN n.`property key`");
    ok("RETURN {`odd key`: 1}");
    // The `+` repetition is how a literal backtick is escaped: `` `a``b` `` is one
    // name, ``a`b``.
    ok("MATCH (`a``b`) RETURN 1");
    let q = ok("MATCH (`a``b`) RETURN 1");
    match &q.head.reading[0] {
        Clause::Match(m) => assert_eq!(m.patterns[0].start.var.as_deref(), Some("a`b")),
        other => panic!("expected a MATCH, got {other:?}"),
    }
    // The `*` permits the empty name.
    ok("MATCH (``) RETURN 1");
    // An odd number of backticks cannot close.
    g_no("MATCH (`a) RETURN 1");
}

#[test]
fn symbolic_name_admits_hex_letters_and_function_words() {
    // SymbolicName = ... | HexLetter | COUNT | FILTER | EXTRACT | ANY | NONE | SINGLE
    for name in ["a", "b", "c", "d", "e", "f", "A", "F"] {
        ok(&format!("MATCH ({name}) RETURN {name}"));
    }
    for name in ["count", "filter", "extract", "any", "none", "single"] {
        ok(&format!("MATCH ({name}) RETURN {name}"));
        ok(&format!("MATCH (n) RETURN n.{name}"));
    }
}

// ── Comments and whitespace ──────────────────────────────────────────────────

#[test]
fn comments_line_and_block() {
    // Comment = "/*" ... "*/" | "//" ... (NEWLINE | EOI)
    ok("MATCH (n) // trailing\n RETURN n");
    ok("MATCH (n) RETURN n // at EOI, no newline");
    ok("// leading\nMATCH (n) RETURN n");
    ok("MATCH (n) /* inline */ RETURN n");
    ok("MATCH /* between */ (n) RETURN n");
    ok("/* leading */ MATCH (n) RETURN n");
    ok("MATCH (n) /* multi\nline */ RETURN n");
    ok("RETURN /**/ 1");
    // Block comments do not nest: the first `*/` closes, so an inner `/*` is just
    // comment text...
    ok("MATCH (n) /* /* */ RETURN n");
    // ...and a second `*/` is then stray input, not the close of an outer comment.
    g_no("MATCH (n) /* /* */ RETURN n */");
    g_no("RETURN /* unterminated 1");
    // A `//` inside a string is not a comment.
    assert_eq!(
        lit("'// not a comment'"),
        Value::Str("// not a comment".into())
    );
}

// ── Unicode arrow heads and dashes ───────────────────────────────────────────

#[test]
fn relationship_arrows_accept_unicode_look_alikes() {
    // Dash = "-" | soft hyphen | ‐ | ‑ | ‒ | – | — | ― | − | ﹘ | ﹣ | －
    // LeftArrowHead = "<" | ⟨ | 〈 | ﹤ | ＜   (RightArrowHead likewise)
    for dash in [
        "-", "\u{00AD}", "\u{2010}", "\u{2011}", "\u{2012}", "\u{2013}", "\u{2014}", "\u{2015}",
        "\u{2212}", "\u{FE58}", "\u{FE63}", "\u{FF0D}",
    ] {
        ok(&format!("MATCH (a){dash}[:R]{dash}(b) RETURN a"));
        ok(&format!("MATCH (a){dash}{dash}(b) RETURN a"));
        ok(&format!("MATCH (a){dash}[:R]{dash}>(b) RETURN a"));
        ok(&format!("MATCH (a)<{dash}[:R]{dash}(b) RETURN a"));
    }
    for right in [">", "\u{27E9}", "\u{3009}", "\u{FE65}", "\u{FF1E}"] {
        ok(&format!("MATCH (a)-[:R]-{right}(b) RETURN a"));
    }
    for left in ["<", "\u{27E8}", "\u{3008}", "\u{FE64}", "\u{FF1C}"] {
        ok(&format!("MATCH (a){left}-[:R]-(b) RETURN a"));
    }
    // An en dash is a Dash only inside a relationship; arithmetic stays ASCII.
    g_no("RETURN 1 \u{2013} 2");
}

// ── Deliberate deviations from the oracle's PEG bugs ─────────────────────────

#[test]
fn oracle_peg_bugs_are_not_reproduced() {
    // (1) `PartialComparisonExpression = EQ | NE | LT | GT | LE | GE` — as an
    // ordered choice, `<=` matches LT and leaves a stray `=`. We order `<=` first.
    ok("RETURN 1 <= 2");
    ok("RETURN 1 >= 2");

    // (2) The oracle's keyword terminals have no trailing word boundary, so
    // `RETURNS` would tokenise as `RETURN` + `S`. Every kw_* asserts !ident_cont.
    g_no("RETURNS 1");
    ok("MATCH (n) RETURN n.orders");

    // (3) The oracle's block-comment arm cannot match `/***/`.
    ok("RETURN /***/ 1");
    ok("RETURN /****/ 1");

    // The oracle requires SP after RETURN (`ProjectionBody = ... SP ProjectionItems`),
    // so it rejects `RETURN(1)`. Accepting it is a superset, hence conformant.
    ok("RETURN(1)");
    ok("MATCH (n) WHERE(n.x)RETURN n");
}

// ── Deliberate scope: writes and procedures ──────────────────────────────────

#[test]
fn updating_clauses_are_rejected_as_read_only() {
    // Create / Merge / Delete / Set / Remove are the reference's UpdatingClause.
    // Slater's read grammar parses them structurally as `forbidden_clause` so the
    // client gets a clear message rather than an opaque syntax error.
    for (clause, q) in [
        ("CREATE", "CREATE (n)"),
        ("CREATE", "CREATE (a)-[:R]->(b)"),
        ("MERGE", "MERGE (n:A {x: 1})"),
        ("MERGE", "MERGE (n) ON CREATE SET n.x = 1"),
        ("DELETE", "MATCH (n) DELETE n"),
        ("DETACH", "MATCH (n) DETACH DELETE n"),
        ("SET", "MATCH (n) SET n.x = 1"),
        ("SET", "MATCH (n) SET n += {x: 1}"),
        ("SET", "MATCH (n) SET n:Label"),
        ("REMOVE", "MATCH (n) REMOVE n:Label"),
        ("REMOVE", "MATCH (n) REMOVE n.x"),
    ] {
        let e = err(q);
        assert!(
            e.contains("read-only") && e.contains(clause),
            "{q:?} should name {clause} as read-only, got: {e}"
        );
    }
}

#[test]
fn procedure_calls_outside_the_whitelist_are_read_only() {
    // StandaloneCall / InQueryCall. Slater whitelists only read procedures.
    assert!(err("CALL db.labels()").contains("read-only"));
    assert!(err("CALL db.labels").contains("read-only"));
    assert!(err("CALL db.labels() YIELD *").contains("read-only"));
    // The whitelisted metadata and algo procedures do parse.
    ok("CALL db.meta.stats()");
    ok("CALL db.meta.stats() YIELD x RETURN x");
}

// ── Grammar with no implementation behind it ─────────────────────────────────

#[test]
fn grammar_without_backing_parses_then_names_its_limitation() {
    // These productions are in the reference grammar and Slater's `.pest` accepts
    // them, but the lowering rejects them by name rather than by syntax error.
    // Each has a tracked task; when one is implemented, its assertion here flips
    // from `err` to `ok`.

    // Properties = MapLiteral | Parameter — the parameter arm.
    g("MATCH (n $p) RETURN n");
    g("MATCH (a)-[r:R $p]->(b) RETURN r");
    assert!(err("MATCH (n $p) RETURN n").contains("parameter property map"));
    assert!(err("MATCH (a)-[r:R $p]->(b) RETURN r").contains("parameter property map"));

    // ExistentialSubquery = EXISTS "{" RegularQuery "}" — the RegularQuery arm.
    g("MATCH (a) WHERE EXISTS { MATCH (a) RETURN a } RETURN a");
    assert!(
        err("MATCH (a) WHERE EXISTS { MATCH (a) RETURN a } RETURN a")
            .contains("only the pattern form of EXISTS")
    );
}
