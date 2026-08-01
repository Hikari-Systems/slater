// SPDX-License-Identifier: Apache-2.0
//! **Capability confinement:** the server must not link the build pipeline.
//!
//! `slater` is the network-facing, always-running process — it terminates untrusted Bolt
//! connections. `slater-build` is the only thing that can compile and publish a
//! generation. Keeping them in separate binaries means:
//!
//! * Publishing a generation requires an `execve` of a *different file*, which is a
//!   visible, separately-controllable event — file permissions and ownership on the
//!   builder, `execve` auditing, image contents, AppArmor/SELinux profiles. None of those
//!   controls exist for a function call inside an already-running process.
//! * The audit question "what in this system can write a generation?" has a small,
//!   explicit answer with a CLI-shaped invocation surface, rather than several thousand
//!   symbols sitting inside the process that parses network input.
//! * A read replica can be made *incapable* of building by not shipping the file
//!   (`slater:latest-lite`), rather than by trusting a compile-time flag.
//!
//! This was briefly given up: a change linked `slater-build`'s pipeline into the server so
//! it could re-exec itself as a consolidation worker, which put ~4 100 build-pipeline
//! symbols (`build_external`, `merge_build`, `direct_ingest`) into a binary that had
//! exactly zero, reachable behind a single `argv[1]` comparison. It was reverted. This test
//! is what stops it returning by accident — the operational itch it scratched (the shipped
//! image failing to find `/app/slater-build`) is served instead by `spawn_builder`'s
//! `current_exe()`-sibling fallback, which links nothing.
//!
//! Checked at the manifest level rather than by inspecting symbols, so it fails in review
//! at the moment the dependency is added rather than after a release build.

use std::path::Path;

/// Workspace-internal crates `slater` is allowed to depend on. `slater-build` is
/// deliberately absent, and so is anything that itself pulls it in — hence the transitive
/// walk below over this closed set.
const ALLOWED_INTERNAL: &[&str] = &["graph-format", "slater-scalar", "slater-delta"];

/// The crate that must never appear anywhere in the server's dependency closure.
const FORBIDDEN: &str = "slater-build";

fn manifest_of(crate_name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join(crate_name)
        .join("Cargo.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Does `manifest` declare a dependency on `dep`? Matches the `dep = …` / `dep.workspace`
/// forms this workspace uses, anchored at the start of a line so a mention inside a comment
/// or a longer crate name does not count.
fn declares_dependency(manifest: &str, dep: &str) -> bool {
    manifest.lines().map(str::trim_start).any(|line| {
        // Skip comments — every dependency here carries an explanatory one above it, and
        // several of those legitimately name `slater-build`.
        !line.starts_with('#')
            && (line.starts_with(&format!("{dep} ="))
                || line.starts_with(&format!("{dep}.workspace"))
                || line.starts_with(&format!("{dep}.path")))
    })
}

#[test]
fn the_server_does_not_link_the_build_pipeline() {
    // Direct dependency.
    assert!(
        !declares_dependency(&manifest_of("slater"), FORBIDDEN),
        "`slater` must not depend on `{FORBIDDEN}` — the server is the network-facing \
         process and must not contain the code that publishes a generation. See this \
         file's module docs; if you need the builder reachable from the server, spawn it, \
         do not link it."
    );

    // Transitive, through the closed set of workspace-internal crates it may use.
    for internal in ALLOWED_INTERNAL {
        assert!(
            !declares_dependency(&manifest_of(internal), FORBIDDEN),
            "`{internal}` depends on `{FORBIDDEN}`, which pulls the build pipeline into \
             `slater` transitively — the confinement is on the whole dependency closure, \
             not just the direct edge."
        );
    }
}

/// The allow-list above is only meaningful if it is complete: a new workspace-internal
/// dependency added to `slater` must be considered here, not silently skipped by a walk
/// that only knows about three crates.
#[test]
fn the_allow_list_covers_every_internal_dependency_the_server_has() {
    let manifest = manifest_of("slater");
    // Every workspace member except the server itself and the builder.
    for member in ["graph-format", "slater-scalar", "slater-delta"] {
        if declares_dependency(&manifest, member) {
            assert!(
                ALLOWED_INTERNAL.contains(&member),
                "`slater` depends on workspace crate `{member}`, which is missing from \
                 ALLOWED_INTERNAL — add it so its own manifest is checked too"
            );
        }
    }
}
