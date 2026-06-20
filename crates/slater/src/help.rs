// SPDX-License-Identifier: Apache-2.0
//! The `--help` / `help` CLI subcommand for the `slater` server binary.
//!
//! Unlike the offline `slater-build` writer (which takes its configuration as
//! `clap` flags and so gets a help page for free), the server is configured
//! entirely through a layered `config.json` plus `KEY__sub` environment
//! overrides — there are no server flags to enumerate. This module gives an
//! operator a single discoverable place that lists the subcommands and the
//! configuration knobs (with their env-var spellings), so `slater --help`
//! answers "what can I run and what can I set?" without reading the source.
//!
//! Stdlib-only and side-effect-free until it decides to act, mirroring the
//! other `*_subcommand` helpers so it can be called unconditionally at the very
//! top of `main` — before any config load — and thus works even with no config
//! file present.

/// Handle the `--help` / `-h` / `help` CLI subcommand and exit if present.
///
/// No-op unless `argv[1]` is one of `--help`, `-h`, or `help`. Prints the usage
/// summary to stdout and exits `0`. Safe to call as the first line of `main`.
pub fn help_subcommand() {
    if let Some("--help" | "-h" | "help") = std::env::args().nth(1).as_deref() {
        print!("{HELP}");
        std::process::exit(0);
    }
}

/// The full help text. Kept as one literal so the layout is obvious at a glance;
/// the config rows mirror the tables in `README.md` / `DOCKERHUB.md`.
const HELP: &str = "\
slater — read-only Bolt graph engine

USAGE:
    slater                          Start the Bolt server (the default; no args).
    slater <SUBCOMMAND> [args]      Run an offline/operator subcommand and exit.
    slater --help | -h | help       Show this help and exit.

SUBCOMMANDS:
    hash-password [PASSWORD]
        Mint an argon2id PHC hash for an ACL entry. Reads the password from the
        argument, or one line of stdin if omitted. Prints the hash and exits.

    healthcheck [HOST] [PORT]
        Bolt handshake probe (HOST defaults to localhost, PORT to the configured
        server port). Exits 0 if the server answers, 1 otherwise. This is the
        container HEALTHCHECK.

    diagnostics [HOST] [PORT] [USER] [PASSWORD]
        Open a Bolt session, run CALL slater.diagnostics(), print the metric
        snapshot as JSON, and exit. USER/PASSWORD also read from SLATER_DIAG_USER
        / SLATER_DIAG_PASSWORD. Requires loadTestDiagnostics=true server-side.

CONFIGURATION:
    The server takes no flags. It reads a layered JSON config and lets you
    override any field from the environment. Sources, lowest precedence first:
        1. The baked-in / mounted config.json (or /app/config.json).
        2. A /sandbox/config.json overlay, if present.
        3. Environment overrides: KEY__sub (double underscore nests). Examples:
               cache__blockCacheBytes=536870912
               dataBackend__fs__dir=/data
               server__port=7687
        4. [SECRET]:name values resolved from the secret provider.

    Key knobs (config path | env override | default):
        dataBackend.kind            | dataBackend__kind            | fs
        dataBackend.fs.dir          | dataBackend__fs__dir         | /data
        dataBackend.s3.bucket       | dataBackend__s3__bucket      | (empty)
        dataBackend.s3.region       | dataBackend__s3__region      | (empty)
        dataBackend.s3.endpoint     | dataBackend__s3__endpoint    | (empty)
        dataBackend.s3.diskCacheBytes | dataBackend__s3__diskCacheBytes | 0
        aclPath                     | aclPath                      | /config/acl.json
        requireAclStamp             | requireAclStamp              | true
        server.bind                 | server__bind                 | 0.0.0.0
        server.port                 | server__port                 | 7687
        cache.blockCacheBytes       | cache__blockCacheBytes       | 64 MiB
        cache.vectorCacheBytes      | cache__vectorCacheBytes      | 64 MiB
        cache.resultCacheBytes      | cache__resultCacheBytes      | 16 MiB
        cache.cacheTtlMs            | cache__cacheTtlMs            | 1800000
        query.maxRows               | query__maxRows               | 100000
        query.timeoutMs             | query__timeoutMs             | 30000
        query.maxIntermediate       | query__maxIntermediate       | 1000000
        vectorQuery.beamWidth       | vectorQuery__beamWidth       | 64
        generationPollMs            | generationPollMs             | 5000
        reloadStrategy              | reloadStrategy               | exit
        cacheWarmingQuery           | cacheWarmingQuery            | (empty)
        loadTestDiagnostics         | loadTestDiagnostics          | false
        log.level                   | log__level                   | info

    See README.md / DOCKERHUB.md for the full list and the meaning of each field.
";

#[cfg(test)]
mod tests {
    use super::HELP;

    #[test]
    fn help_text_lists_the_subcommands_and_override_mechanism() {
        // The three operator subcommands are discoverable here — this is the only
        // place they are listed, so a rename must update the help in lockstep.
        for sub in ["hash-password", "healthcheck", "diagnostics"] {
            assert!(HELP.contains(sub), "help should mention {sub}");
        }
        // The env-override convention and a representative nested example.
        assert!(HELP.contains("KEY__sub"));
        assert!(HELP.contains("cache__blockCacheBytes"));
        // The feature that prompted documenting all this.
        assert!(HELP.contains("cacheWarmingQuery"));
    }
}
