# Contributing (for Claude purposes only)

## General

This file is intended to tell claude what rules it needs to follow when authoring. 

As much as any human interest is appreciated, I'd really prefer not to accept contributions on this 
repository. My purpose in sharing it at all is simply transparency and openness about how i work, and 
since doing pull request reviews is my least favourite (but necessary) part of the job I won't be 
doing any on a project that is primarily aimed at my enjoyment and acting as an entry on my CV. 

If you like it and want to change it, please feel free to fork and modify (please rename it if you plan to 
share it yourself), but mostly please don't submit pull requests. Thanks in advance for your understanding.

## Error handling — typed errors, never string matching

When code **branches on why an operation failed** — classifying an error to pick a
status code, reword a message, retry, or fall back — match on a **typed error**, never
on the text of an error message.

```rust
// NO — brittle: a message reword silently breaks the classification, and the same
// wording can arise from unrelated causes.
if e.to_string().contains("read-only") { /* … */ }

// YES — structural: define a typed error and downcast (anyhow) or match the enum.
#[derive(Debug, thiserror::Error)]
#[error("Slater is read-only; the '{clause}' clause is not permitted")]
pub struct WriteClauseRejected { pub clause: String }

if e.downcast_ref::<WriteClauseRejected>().is_some() { /* … */ }
```

Message text is for humans and may change freely; behaviour must key off the type. A
typed error also carries structured fields (here, the rejected `clause`) that callers
would otherwise have to re-parse out of a string. `thiserror` is already a dependency;
its `Display` can keep whatever human wording you like without becoming load-bearing.

## Formatting (local pre-commit gate)

Formatting is enforced prettier/eslint-style: a pre-commit hook runs `rustfmt
--check` on the Rust files **you are committing** (not the whole tree), so it never
trips on formatting elsewhere. Enable it once per clone:

```sh
git config core.hooksPath .githooks
```

After that, a commit that touches a `.rs` file which isn't rustfmt-clean is rejected
with the fix command. To format and retry:

```sh
rustfmt --edition 2021 path/to/file.rs   # or: cargo fmt --all
git add -u && git commit
```

Bypass once (not recommended) with `git commit --no-verify`. If `rustfmt` isn't
installed the hook warns and lets the commit through — add it with
`rustup component add rustfmt`.

## Tests gate releases

The `release` workflow (`.github/workflows/release.yml`) runs the full workspace
suite — `cargo test --workspace --locked` — as a `test` job that **every** publishing
job (`docker-build`, `docker-manifest`, `release-binaries`) depends on. A red test
run on a `vX.Y.Z` tag blocks the Docker push and the GitHub binary release for that
tag. Run it locally before tagging:

```sh
cargo test --workspace --locked
```
