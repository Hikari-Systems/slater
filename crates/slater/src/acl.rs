// SPDX-License-Identifier: Apache-2.0
//! Access control — argon2id authentication and per-graph read grants.
//!
//! The ACL is a plain-JSON file (it lives on shared storage alongside the
//! data) mapping each user to an **argon2id password hash** and a set of
//! per-graph grants. Cleartext passwords are never stored; hashes are minted with
//! the `slater hash-password` subcommand ([`hash_password`]). At `LOGON` the
//! server [`Acl::verify`]s the supplied credentials; before serving any query it
//! checks [`Acl::can_read`] for the selected graph.
//!
//! The file is **hot-reloaded**: [`AclHandle::poll`] re-reads it when it changes,
//! and a malformed file is rejected loudly while the last-good ACL keeps serving
//! (a fat-fingered edit must never lock every user out).
//
// The server loop that calls poll()/verify()/can_read() lands with the Bolt
// connection state machine; allow dead_code for the standalone ACL until then.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

use anyhow::{Context, Result};
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use serde::Deserialize;
use tracing::{info, warn};

/// One user's stored credential and grants. Unknown JSON fields (e.g. the
/// sample file's `_comment`) are ignored by serde.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEntry {
    /// PHC-string argon2id hash (`$argon2id$v=19$m=...$salt$hash`).
    pub password_argon2id: String,
    /// Graph name → granted permissions (only `"read"` is meaningful today).
    #[serde(default)]
    pub grants: HashMap<String, Vec<String>>,
}

/// The parsed ACL: a set of users keyed by name.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Acl {
    #[serde(default)]
    pub users: HashMap<String, UserEntry>,
}

impl Acl {
    /// Parse an ACL from a JSON string.
    pub fn from_json_str(s: &str) -> Result<Self> {
        serde_json::from_str(s).context("parse ACL JSON")
    }

    /// Read and parse the ACL file at `path`.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read ACL {}", path.display()))?;
        Self::from_json_str(&text).with_context(|| format!("parse ACL {}", path.display()))
    }

    /// Verify `password` for `user`. Returns `true` only for a known user whose
    /// stored argon2id hash verifies. An unknown user still runs a verify against
    /// a dummy hash so a missing account is not distinguishable by timing.
    pub fn verify(&self, user: &str, password: &str) -> bool {
        match self.users.get(user) {
            Some(u) => verify_hash(&u.password_argon2id, password),
            None => {
                // Equalise timing against the absent-user path.
                let _ = verify_hash(dummy_hash(), password);
                false
            }
        }
    }

    /// Does `user` hold a `read` grant on `graph`?
    pub fn can_read(&self, user: &str, graph: &str) -> bool {
        self.users.get(user).is_some_and(|u| {
            u.grants
                .get(graph)
                .is_some_and(|perms| perms.iter().any(|p| p == "read"))
        })
    }

    /// The set of graphs `user` may read (for `SHOW DATABASES`-style listing).
    pub fn readable_graphs(&self, user: &str) -> Vec<String> {
        self.users.get(user).map_or_else(Vec::new, |u| {
            let mut gs: Vec<String> = u
                .grants
                .iter()
                .filter(|(_, perms)| perms.iter().any(|p| p == "read"))
                .map(|(g, _)| g.clone())
                .collect();
            gs.sort();
            gs
        })
    }
}

/// Verify a password against a stored PHC argon2 hash, returning `false` (and
/// logging) on a malformed stored hash rather than erroring.
fn verify_hash(stored: &str, password: &str) -> bool {
    match PasswordHash::new(stored) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(e) => {
            warn!(error = %e, "ACL contains a malformed password hash; rejecting");
            false
        }
    }
}

/// A throwaway hash used to keep the unknown-user verify path constant-time. Built
/// once on first use.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| {
        hash_password("\0slater-absent-user\0")
            .unwrap_or_else(|_| "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string())
    })
}

/// Mint an argon2id PHC-string hash for `password` (used by `slater hash-password`).
pub fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("argon2 hash failed: {e}"))?;
    Ok(hash.to_string())
}

/// A hot-reloadable handle around an [`Acl`]. Cheap to clone-snapshot
/// (`Arc<Acl>`), so request handlers take a snapshot per LOGON/query and the
/// background poller swaps the active ACL underneath them.
pub struct AclHandle {
    path: PathBuf,
    state: RwLock<State>,
}

struct State {
    acl: Arc<Acl>,
    mtime: Option<SystemTime>,
}

impl AclHandle {
    /// Load the ACL once; errors if the initial file is missing or malformed
    /// (a server should refuse to start with no usable ACL).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let acl = Acl::load(&path)?;
        let mtime = file_mtime(&path);
        Ok(Self {
            path,
            state: RwLock::new(State {
                acl: Arc::new(acl),
                mtime,
            }),
        })
    }

    /// A cheap snapshot of the currently-active ACL.
    pub fn snapshot(&self) -> Arc<Acl> {
        self.state.read().unwrap().acl.clone()
    }

    /// Re-read the file and swap in the new ACL. On a parse/IO error the last-good
    /// ACL is kept and the error logged loudly. Returns `true` if a new ACL was
    /// installed.
    pub fn reload(&self) -> bool {
        let mtime = file_mtime(&self.path);
        match Acl::load(&self.path) {
            Ok(acl) => {
                let mut s = self.state.write().unwrap();
                s.acl = Arc::new(acl);
                s.mtime = mtime;
                info!(path = %self.path.display(), users = s.acl.users.len(), "reloaded ACL");
                true
            }
            Err(e) => {
                // Keep last-good; advance the recorded mtime so we do not re-log
                // the same broken file every poll until it changes again.
                warn!(path = %self.path.display(), error = %e, "ACL reload failed; keeping last-good ACL");
                self.state.write().unwrap().mtime = mtime;
                false
            }
        }
    }

    /// Reload only if the file's modification time has changed since the last
    /// load. Intended to be called on the generation-poll interval. Returns
    /// `true` if a reload was attempted (whether or not it succeeded).
    pub fn poll(&self) -> bool {
        let current = file_mtime(&self.path);
        let changed = {
            let s = self.state.read().unwrap();
            current != s.mtime
        };
        if changed {
            self.reload();
        }
        changed
    }
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Handle the `hash-password` CLI subcommand and exit if present.
///
/// No-op unless `argv[1] == "hash-password"`, so it can be called near the top of
/// `main`. The password is taken from `argv[2]` if given, else read as one line
/// from stdin. Prints the PHC hash to stdout and exits `0`; exits `1` on error.
pub fn hash_password_subcommand() {
    if std::env::args().nth(1).as_deref() != Some("hash-password") {
        return;
    }
    let password = match std::env::args().nth(2) {
        Some(p) => p,
        None => {
            use std::io::BufRead;
            let mut line = String::new();
            if std::io::stdin().lock().read_line(&mut line).is_err() {
                eprintln!("hash-password: failed to read password from stdin");
                std::process::exit(1);
            }
            line.trim_end_matches(['\r', '\n']).to_string()
        }
    };
    match hash_password(&password) {
        Ok(hash) => {
            println!("{hash}");
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("hash-password: {e:#}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl_with(user: &str, password: &str, grants: &[(&str, &[&str])]) -> Acl {
        let hash = hash_password(password).unwrap();
        let grants_json: serde_json::Value = grants
            .iter()
            .map(|(g, perms)| {
                (
                    g.to_string(),
                    serde_json::Value::from(
                        perms.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                    ),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into();
        let json = serde_json::json!({
            "users": { user: { "passwordArgon2id": hash, "grants": grants_json } }
        });
        Acl::from_json_str(&json.to_string()).unwrap()
    }

    #[test]
    fn hash_is_argon2id_and_verifies() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(hash.starts_with("$argon2id$"), "got {hash}");
        assert!(verify_hash(&hash, "correct horse battery staple"));
        assert!(!verify_hash(&hash, "wrong password"));
    }

    #[test]
    fn verify_checks_user_and_password() {
        let acl = acl_with("reporting", "s3cret", &[("eu_ai_act", &["read"])]);
        assert!(acl.verify("reporting", "s3cret"));
        assert!(!acl.verify("reporting", "nope"));
        assert!(!acl.verify("ghost", "s3cret")); // unknown user
    }

    #[test]
    fn grants_are_per_graph_and_read_only() {
        let acl = acl_with(
            "reporting",
            "pw",
            &[("eu_ai_act", &["read"]), ("secret_graph", &["write"])],
        );
        assert!(acl.can_read("reporting", "eu_ai_act"));
        assert!(!acl.can_read("reporting", "secret_graph")); // granted, but not "read"
        assert!(!acl.can_read("reporting", "unlisted")); // no grant at all
        assert!(!acl.can_read("ghost", "eu_ai_act")); // unknown user
        assert_eq!(
            acl.readable_graphs("reporting"),
            vec!["eu_ai_act".to_string()]
        );
    }

    #[test]
    fn parses_sample_file_shape_with_comment() {
        let hash = hash_password("pw").unwrap();
        let json = format!(
            r#"{{
              "_comment": "sample with an ignored comment field",
              "users": {{
                "reporting": {{
                  "passwordArgon2id": "{hash}",
                  "grants": {{ "eu_ai_act": ["read"], "companies": ["read"] }}
                }}
              }}
            }}"#
        );
        let acl = Acl::from_json_str(&json).unwrap();
        assert!(acl.verify("reporting", "pw"));
        assert!(acl.can_read("reporting", "companies"));
    }

    #[test]
    fn hot_reload_keeps_last_good_on_malformed_file() {
        let dir = std::env::temp_dir().join(format!("slater_acl_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("acl.json");

        // Initial good ACL.
        let first = serde_json::json!({
            "users": { "alice": { "passwordArgon2id": hash_password("a").unwrap(), "grants": { "g": ["read"] } } }
        });
        std::fs::write(&path, first.to_string()).unwrap();
        let handle = AclHandle::load(&path).unwrap();
        assert!(handle.snapshot().verify("alice", "a"));

        // Malformed edit: reload must fail-safe and keep alice.
        std::fs::write(&path, "{ this is not json").unwrap();
        assert!(!handle.reload());
        assert!(
            handle.snapshot().verify("alice", "a"),
            "last-good ACL must survive a bad file"
        );

        // A new good ACL installs cleanly.
        let second = serde_json::json!({
            "users": { "bob": { "passwordArgon2id": hash_password("b").unwrap(), "grants": { "g": ["read"] } } }
        });
        std::fs::write(&path, second.to_string()).unwrap();
        assert!(handle.reload());
        let snap = handle.snapshot();
        assert!(snap.verify("bob", "b"));
        assert!(
            !snap.verify("alice", "a"),
            "old user gone after a successful reload"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_initial_acl_is_an_error() {
        let path =
            std::env::temp_dir().join(format!("slater_acl_absent_{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert!(AclHandle::load(&path).is_err());
    }
}
