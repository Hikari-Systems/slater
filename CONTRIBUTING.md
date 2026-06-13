# Contributing (for Claude purposes only)

## General

This file is intended to tell claude what rules it needs to follow when authoring. 

As much as any human interest is appreciated, I'd really prefer not to accept contributions on this 
repository. My purpose in sharing it at all is simply transparency and openness about how i work, and 
since doing pull request reviews is my least favourite (but necessary) part of the job I won't be 
doing any on a project that is primarily aimed at my enjoyment and acting as an entry on my CV. 

If you like it and want to change it, please feel free to fork and modify (please rename it if you plan to 
share it yourself), but mostly please don't submit pull requests. Thanks in advance for your understanding.

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
