// SPDX-License-Identifier: Apache-2.0
//! The `dump` CLI subcommand: export a graph from a **running** server to
//! `slater-build`-compatible business-key `MERGE` Cypher.
//!
//! ```text
//! slater dump [GRAPH] -u USER [--host H] [--port P] [-o FILE] \
//!             [--key Label=prop]… [--pk FIELD]
//! slater dump --list -u USER                      # graphs the user may read
//! ```
//!
//! Unlike `slater query` (which mounts a generation in-process), `dump` connects
//! over **Bolt**, authenticates, and honours per-graph ACLs — so it works against
//! a live deployment with no disk access. The password is read from stdin (or the
//! `SLATER_DUMP_PASSWORD` env var), never a flag, to keep it out of `ps`/shell
//! history. The emitted dialect is byte-for-byte the business-key `MERGE` import
//! that `slater-build` ingests, so a graph round-trips: dump → `slater-build` →
//! new generation.
//!
//! It runs synchronously before the server's tokio runtime is built (the Bolt
//! client is blocking stdlib), mirroring the `diagnostics`/`query` subcommands.

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::io::Write;
use std::time::Duration;

use crate::bolt::client::BoltClient;

/// Parsed `dump` invocation.
#[derive(Debug, Parser)]
#[command(
    name = "slater dump",
    about = "Export a graph to business-key MERGE Cypher"
)]
struct DumpArgs {
    /// Graph to dump (required unless `--list`).
    graph: Option<String>,

    /// List the graphs the authenticated user may read, then exit.
    #[arg(short = 'l', long)]
    list: bool,

    /// Server host.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Server Bolt port (defaults to the configured `server.port`).
    #[arg(long)]
    port: Option<u16>,

    /// User to authenticate as.
    #[arg(short = 'u', long)]
    user: String,

    /// Read the password from stdin (the default); given for parity with Docker's
    /// convention. The password is always taken from `SLATER_DUMP_PASSWORD` if set,
    /// else read as one line from stdin.
    #[arg(long)]
    password_stdin: bool,
    // NB: `-o/--out`, `--key Label=prop`, and `--pk` land with the schema +
    // node/edge dump in the following milestones (kept off the struct until used
    // so every commit stays warning-clean under `-D warnings`).
}

/// Handle the `dump` CLI subcommand and exit if present.
///
/// No-op unless `argv[1] == "dump"`, so it can be called unconditionally near the
/// top of `main`. Always exits the process on the `dump` path: `0` on success,
/// `1` on any error (message on stderr). `default_port` is the configured
/// `server.port`, used when `--port` is omitted.
pub fn dump_subcommand(default_port: u16) {
    if std::env::args().nth(1).as_deref() != Some("dump") {
        return;
    }
    // clap treats the first item as the program name; feed it "dump" (argv[1]) as
    // the pseudo-bin and the flags after it, so `graph` is the first positional.
    let args = DumpArgs::parse_from(std::env::args().skip(1));
    match run(args, default_port) {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("slater dump failed: {e:#}");
            std::process::exit(1);
        }
    }
}

/// Read the password from `SLATER_DUMP_PASSWORD` if set, else one line from stdin
/// (trailing newline stripped). `force_stdin` (the `--password-stdin` toggle)
/// always reads stdin, ignoring the env var. Empty is allowed (an open ACL).
fn read_password(force_stdin: bool) -> Result<String> {
    if !force_stdin {
        if let Ok(p) = std::env::var("SLATER_DUMP_PASSWORD") {
            return Ok(p);
        }
    }
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading password from stdin")?;
    Ok(line.trim_end_matches(['\n', '\r']).to_string())
}

/// Connect, authenticate, and dispatch to `--list` or the graph dump.
fn run(args: DumpArgs, default_port: u16) -> Result<()> {
    if !args.list && args.graph.is_none() {
        bail!("no graph named — pass a GRAPH argument, or --list to see readable graphs");
    }
    let port = args.port.unwrap_or(default_port);
    let password = read_password(args.password_stdin)?;

    // A generous read timeout: a large graph's `PULL` streams many records, but
    // the server sends them continuously, so the timeout only fires on a genuine
    // stall or a dropped connection.
    let mut conn = BoltClient::connect(&args.host, port, Duration::from_secs(120))
        .with_context(|| format!("connecting to {}:{}", args.host, port))?;
    conn.login("slater-dump/1.0", &args.user, &password)
        .context("authenticating (check --user and the password)")?;

    if args.list {
        return list_graphs(&mut conn);
    }
    // The graph data dump (schema + nodes + edges) lands in the next milestone.
    bail!("graph data dump is not yet implemented (this milestone ships --list)");
}

/// `--list`: print the graph names the authenticated user may read, one per line,
/// to stdout. Backed by the server's `SHOW DATABASES` (which already filters to
/// the caller's read grants via the ACL).
fn list_graphs(conn: &mut BoltClient) -> Result<()> {
    let (_cols, rows) = conn
        .run_pull("SHOW DATABASES", None)
        .context("listing graphs (SHOW DATABASES)")?;
    let stdout = std::io::stdout();
    let mut w = std::io::BufWriter::new(stdout.lock());
    for row in rows {
        if let Some(name) = row
            .first()
            .and_then(crate::bolt::packstream::PsValue::as_str)
        {
            writeln!(w, "{name}").context("writing graph name")?;
        }
    }
    w.flush().context("flushing stdout")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// clap's own consistency check on the derived CLI (catches duplicate flags,
    /// bad `short`/`long` combos, etc.).
    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        DumpArgs::command().debug_assert();
    }

    #[test]
    fn parses_a_graph_dump_invocation() {
        let a = DumpArgs::parse_from([
            "dump",
            "people",
            "-u",
            "alice",
            "--host",
            "db.internal",
            "--port",
            "7690",
        ]);
        assert_eq!(a.graph.as_deref(), Some("people"));
        assert_eq!(a.user, "alice");
        assert_eq!(a.host, "db.internal");
        assert_eq!(a.port, Some(7690));
        assert!(!a.list);
    }

    #[test]
    fn parses_list_without_a_graph_and_defaults_host() {
        let a = DumpArgs::parse_from(["dump", "--list", "-u", "bob"]);
        assert!(a.list);
        assert_eq!(a.graph, None);
        assert_eq!(a.host, "localhost");
        assert_eq!(a.port, None);
    }

    #[test]
    fn neither_graph_nor_list_is_rejected() {
        // clap parses (user only); `run` rejects the missing target before any I/O.
        let a = DumpArgs::parse_from(["dump", "-u", "bob"]);
        let err = run(a, 7687).unwrap_err();
        assert!(
            err.to_string().contains("no graph named"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn password_prefers_the_env_var_unless_forced_to_stdin() {
        // Only assert the env-var branch (the stdin branch would block on a tty in
        // the test harness); `force_stdin` bypassing the env var is covered by
        // construction — the env path is the one with observable behaviour here.
        std::env::set_var("SLATER_DUMP_PASSWORD", "s3cr3t");
        assert_eq!(read_password(false).unwrap(), "s3cr3t");
        std::env::remove_var("SLATER_DUMP_PASSWORD");
    }
}
