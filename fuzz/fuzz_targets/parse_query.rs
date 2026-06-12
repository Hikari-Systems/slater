#![no_main]
//! Fuzz the read-only Cypher parser. Property: `parser::parse` must never panic
//! on arbitrary input — malformed queries must come back as `Err`, never as an
//! `unwrap()`/`expect()`/index panic (SECURITY_WORKLIST item 5).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(query) = std::str::from_utf8(data) {
        let _ = slater::parser::parse(query);
    }
});
