#![no_main]
//! Fuzz the write-enabled statement parser. Property: `parser::parse_statement`
//! must never panic on arbitrary input — a malformed write (`CREATE`/`MERGE`/`SET`/
//! `REMOVE`/`DELETE`/GQL `INSERT`), a bogus consolidate call, or plain garbage must
//! come back as `Err`, never as an `unwrap()`/`expect()`/index panic.
//!
//! This is the writable-layer sibling of `parse_query` (which only reaches the
//! read-only `parser::parse`). `parse_statement` is the entry the server calls when
//! the writable layer is enabled: it tries the `consolidate_call` and
//! `write_statement` grammar rules and drives the whole write-lowering tree
//! (`lower_write_statement`/`lower_edge_write`/`lower_insert_stmt`/`lower_create_stmt`/
//! `lower_set_item`/…) that the read parser never touches. (SECURITY_WORKLIST item 5.)
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(query) = std::str::from_utf8(data) {
        let _ = slater::parser::parse_statement(query);
    }
});
