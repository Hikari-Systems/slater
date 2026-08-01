// SPDX-License-Identifier: Apache-2.0
//! Compression-profile policy: which zstd level, and which degree-column selection
//! margin, a build uses when the operator has not pinned them.
//!
//! **Why this is in the library rather than the CLI.** It used to live in
//! `slater-build`'s `main.rs`, which was fine while the binary was the only way to run a
//! build. It is not fine now that the server can run one in-process-as-a-worker: two
//! copies of this policy would drift, and the drift is invisible — a consolidation would
//! quietly publish generations at zstd 3 instead of 9, with a different degree-codec
//! margin, and every test asserting "the rebuild worked" would still pass.
//!
//! That is not hypothetical. `BuildOptions::default()` carries `zstd_level: 3` /
//! `compression_profile: "manual"`, because a struct default cannot know whether the
//! target is local or remote. The CLI never uses those values — it always runs the
//! resolution below first — so a caller that reached for `..Default::default()` would get
//! a materially worse generation than the identical `slater-build` invocation.

/// Balanced for local/NVMe reads: decompression CPU is a larger share of a local read, so
/// do not pay level 19 for bytes that never cross a network.
pub const LOCAL_ZSTD_LEVEL: i32 = 9;
/// Object-store target: every saved byte is network and RTT, so squeeze harder.
pub const REMOTE_ZSTD_LEVEL: i32 = 19;
/// Squeeze hardest, build cost no object.
pub const MAX_ZSTD_LEVEL: i32 = 22;

/// How hard to compress, when `--zstd-level` is not pinned.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum CompressionProfile {
    /// `remote` when a remote publish target (`--publish-s3-bucket` /
    /// `--publish-gcs-bucket`) is configured, else `local`.
    Auto,
    /// Balanced for local/NVMe reads (decompress CPU is a larger share there).
    Local,
    /// Max ratio for remote/object-store reads (bytes-on-the-wire dominate).
    Remote,
    /// Highest ratio regardless of build cost.
    Max,
}

/// Resolve `(zstd_level, profile_name)`. An explicit level always wins and is recorded as
/// `manual`; otherwise `Auto` picks local/remote from the publish target.
pub fn resolve_compression(
    explicit_level: Option<i32>,
    profile: CompressionProfile,
    publishing_remote: bool,
) -> (i32, String) {
    if let Some(level) = explicit_level {
        return (level, "manual".into());
    }
    match resolve_profile(profile, publishing_remote) {
        CompressionProfile::Local => (LOCAL_ZSTD_LEVEL, "local".into()),
        CompressionProfile::Remote => (REMOTE_ZSTD_LEVEL, "remote".into()),
        CompressionProfile::Max => (MAX_ZSTD_LEVEL, "max".into()),
        CompressionProfile::Auto => unreachable!("auto resolved to local/remote"),
    }
}

/// Resolve the degree-column `zstd-dense` selection margin. An explicit margin always
/// wins; otherwise it tracks the compression profile — a local (fs/NVMe) target is
/// latency-biased (0.5: prefer decompress-free EF), a remote/max (object-store) target is
/// wire-biased (1.0: let zstd win when it is any smaller).
pub fn resolve_degree_zstd_margin(
    explicit: Option<f64>,
    profile: CompressionProfile,
    publishing_remote: bool,
) -> f64 {
    if let Some(m) = explicit {
        return m;
    }
    let name = match resolve_profile(profile, publishing_remote) {
        CompressionProfile::Local => "local",
        CompressionProfile::Remote => "remote",
        CompressionProfile::Max => "max",
        CompressionProfile::Auto => unreachable!("auto resolved to local/remote"),
    };
    graph_format::degree_ef::margin_for_profile(name)
}

/// Collapse `Auto` against the publish target. Shared by both resolvers so they can never
/// disagree about what `Auto` meant.
fn resolve_profile(profile: CompressionProfile, publishing_remote: bool) -> CompressionProfile {
    match profile {
        CompressionProfile::Auto if publishing_remote => CompressionProfile::Remote,
        CompressionProfile::Auto => CompressionProfile::Local,
        p => p,
    }
}

/// The settings a **local** publish uses — the consolidation case, where the server hands
/// the worker a plain `--data-dir` and no object-store target.
///
/// Exists so the in-server worker cannot get this subtly wrong: it is the same call the
/// CLI makes for the same invocation, not a second transcription of it.
pub fn local_publish_defaults() -> (i32, String, f64) {
    let (level, profile) = resolve_compression(None, CompressionProfile::Auto, false);
    let margin = resolve_degree_zstd_margin(None, CompressionProfile::Auto, false);
    (level, profile, margin)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The invocation the server's consolidation actually makes: no explicit level, no
    /// remote target. It must land on `local`/9, **not** on `BuildOptions::default()`'s
    /// `manual`/3 — the whole reason this policy is shared rather than duplicated.
    #[test]
    fn a_local_consolidation_publish_resolves_to_the_local_profile() {
        let (level, profile, margin) = local_publish_defaults();
        assert_eq!(level, LOCAL_ZSTD_LEVEL);
        assert_eq!(profile, "local");
        assert_eq!(margin, graph_format::degree_ef::margin_for_profile("local"));
        // Guard the exact trap: the struct default is not the resolved answer.
        let d = crate::BuildOptions::default();
        assert_ne!(
            (d.zstd_level, d.compression_profile.as_str()),
            (level, profile.as_str()),
            "if these ever coincide, delete this test rather than weakening it"
        );
    }

    #[test]
    fn an_explicit_level_wins_and_is_recorded_as_manual() {
        assert_eq!(
            resolve_compression(Some(1), CompressionProfile::Max, true),
            (1, "manual".into())
        );
    }

    #[test]
    fn auto_follows_the_publish_target() {
        assert_eq!(
            resolve_compression(None, CompressionProfile::Auto, true),
            (REMOTE_ZSTD_LEVEL, "remote".into())
        );
        assert_eq!(
            resolve_compression(None, CompressionProfile::Auto, false),
            (LOCAL_ZSTD_LEVEL, "local".into())
        );
    }
}
