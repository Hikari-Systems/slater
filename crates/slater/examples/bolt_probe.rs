// SPDX-License-Identifier: Apache-2.0
//! Time arbitrary Cypher against a running server, one isolated Bolt session per query.
//!
//! A query that breaches a budget leaves its session FAILED, so each statement gets a
//! fresh connection — a failing probe never poisons the next one. Prints wall time and
//! either the first scalar of the first row or the failure.
//!
//!   WC_PORT=7699 WC_USER=wc WC_PASS=wcpw WC_GRAPH=wd91m_fixed \
//!     cargo run --release -p slater --example bolt_probe -- \
//!       'MATCH (n) RETURN count(*)' 'MATCH ()-[r]->() RETURN count(*)'

use slater::bolt::client::BoltClient;
use slater::bolt::packstream::PsValue;
use std::time::{Duration, Instant};

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

fn render(v: &PsValue) -> String {
    match v {
        PsValue::Int(i) => i.to_string(),
        PsValue::Float(f) => format!("{f:.3}"),
        PsValue::String(s) => s.clone(),
        PsValue::Null => "null".into(),
        PsValue::Bool(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}

fn main() {
    let host = env("WC_HOST", "127.0.0.1");
    let port: u16 = env("WC_PORT", "7699").parse().unwrap();
    let user = env("WC_USER", "wc");
    let pass = env("WC_PASS", "wcpw");
    let g = env("WC_GRAPH", "wd91m_fixed");
    let queries: Vec<String> = std::env::args().skip(1).collect();

    for q in &queries {
        let t = Instant::now();
        let mut c = match BoltClient::connect(&host, port, Duration::from_secs(300))
            .and_then(|mut c| c.login("bolt-probe/1", &user, &pass).map(|_| c))
        {
            Ok(c) => c,
            Err(e) => {
                println!("CONNECT-FAIL  {q}\n    {e}");
                continue;
            }
        };
        match c.run_pull(q, Some(&g)) {
            Ok((_cols, rows)) => {
                // `WC_PRINT_ALL=1` dumps every row (comma-joined) — used to pull a small
                // id set out of the server and feed it back into a follow-up query.
                if std::env::var("WC_PRINT_ALL").is_ok() {
                    let all: Vec<String> = rows
                        .iter()
                        .map(|r| r.iter().map(render).collect::<Vec<_>>().join(" | "))
                        .collect();
                    println!("{:>10.1?}  rows={:<7} {q}", t.elapsed(), rows.len());
                    println!("    → {}", all.join(","));
                    continue;
                }
                let first = rows
                    .first()
                    .map(|r| r.iter().map(render).collect::<Vec<_>>().join(" | "))
                    .unwrap_or_else(|| "<no rows>".into());
                println!(
                    "{:>10.1?}  rows={:<7} {q}\n    → {first}",
                    t.elapsed(),
                    rows.len()
                );
            }
            Err(e) => println!("{:>10.1?}  FAILED  {q}\n    → {e}", t.elapsed()),
        }
    }
}
