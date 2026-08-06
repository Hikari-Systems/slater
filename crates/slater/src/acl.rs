// SPDX-License-Identifier: Apache-2.0
//! Access control — argon2id authentication and per-graph `read` / `write` grants.
//!
//! The ACL is a plain-JSON file (it lives on shared storage alongside the
//! data) mapping each user to an **argon2id password hash** and a set of
//! per-graph grants. Cleartext passwords are never stored; hashes are minted with
//! the `slater hash-password` subcommand ([`hash_password`]). At `LOGON` the
//! server [`Acl::verify`]s the supplied credentials; before serving any query it
//! checks [`Acl::can_read`] for the selected graph, and before executing any
//! mutation it checks [`Acl::can_write`]. The two grants are **independent** — a
//! reader is not implicitly a writer.
//!
//! The file is **hot-reloaded**: [`AclHandle::poll`] re-reads it when it changes,
//! and a malformed file is rejected loudly while the last-good ACL keeps serving
//! (a fat-fingered edit must never lock every user out).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

use anyhow::{Context, Result};
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Argon2, Params};
use serde::Deserialize;
use tracing::{info, warn};

/// One user's stored credential and grants. Unknown JSON fields (e.g. the
/// sample file's `_comment`) are ignored by serde.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEntry {
    /// PHC-string argon2id hash (`$argon2id$v=19$m=...$salt$hash`).
    pub password_argon2id: String,
    /// Graph name → granted permissions. Two are meaningful: `"read"` (serve queries on
    /// the graph) and `"write"` (mutate it through the writable layer). They are
    /// independent — a `"read"` grant confers no write access — so a writer is granted
    /// `["read", "write"]`. Unrecognised permission strings are ignored.
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
    /// an equalisation hash so a missing account is not distinguishable by timing.
    /// All three arms — known-and-well-formed, known-but-malformed, unknown — must cost
    /// the same, so every equalisation decision lives here rather than in [`verify_hash`],
    /// which stays a pure "does this password match this PHC string" predicate.
    pub fn verify(&self, user: &str, password: &str) -> bool {
        match self.users.get(user) {
            // A stored hash that will not parse answers `None`, having done no derive at
            // all. Left there it costs microseconds against the milliseconds every other
            // arm costs, which enumerates the accounts whose hash is broken — the oracle
            // HIK-222 closed for unknown users, one arm over. Burn the same equalisation
            // verify before rejecting. (HIK-238)
            Some(u) => verify_hash(&u.password_argon2id, password).unwrap_or_else(|| {
                self.burn_equalisation_verify(password);
                false
            }),
            None => {
                self.burn_equalisation_verify(password);
                false
            }
        }
    }

    /// Run a full argon2id derive against [`Self::equalisation_hash`] and throw the answer
    /// away, so a path that has no real verify to perform still costs what one costs.
    ///
    /// The inner call cannot recurse into the malformed arm: `equalisation_hash` filters
    /// unparseable entries out, and its no-users fallback is a well-formed PHC string by
    /// construction. An ACL whose stored hashes are *all* malformed therefore burns at
    /// that fallback's cost — right answer, since in that state there is no well-formed
    /// arm for it to be distinguished from.
    fn burn_equalisation_verify(&self, password: &str) {
        let _ = verify_hash(self.equalisation_hash(), password);
    }

    /// The stored hash an *unknown* principal is verified against, so the two paths
    /// cost the same and a username cannot be found by timing.
    ///
    /// This borrows the costliest hash the ACL actually holds rather than minting one
    /// at [`Argon2::default`]'s parameters. Verification is **parameter-agnostic** —
    /// `PasswordHash::new` reads `m`/`t`/`p` out of the stored PHC string and the
    /// blanket `PasswordVerifier` impl re-derives with those — so a hash minted by any
    /// third-party argon2 tool verifies at *its* cost, not the default's. A fixed
    /// default-parameter dummy therefore diverged from the known-user path the moment
    /// an operator minted with anything else, and username enumeration by timing came
    /// back. (HIK-222)
    ///
    /// Borrowing is exact by construction: the unknown-user path runs the very same
    /// derivation a real login runs. Minting at "observed" parameters could only
    /// approximate that, and only for as long as the two code paths agreed.
    ///
    /// Verifying an attacker-supplied password against a real user's hash is safe: the
    /// result is discarded and the caller returns `false` unconditionally, so even a
    /// correct guess authenticates nobody — there is no account being logged into. The
    /// comparison is constant-time, so a hit and a miss cost the same.
    ///
    /// **Cost metric** is `m_cost × t_cost`: the number of block computations. `p`
    /// partitions the same memory into lanes rather than multiplying the work, so it
    /// does not belong in the product; it breaks ties only to keep the choice stable.
    ///
    /// **Limit worth knowing.** With *heterogeneous* stored parameters no single dummy
    /// can equalise everything — different users already cost different amounts, so
    /// timing leaks *which* user independently of whether the name exists. Taking the
    /// maximum means an unknown name looks like the most expensive known user: an
    /// attacker who sees "fast" learns only "some cheap known user", and "slow" stays
    /// ambiguous between unknown and the expensive one. Equalising *downward* would be
    /// worse — it would make the known path the slow one and reopen the oracle with the
    /// sign flipped. With uniform parameters (one minting tool, the normal case) the
    /// equalisation is exact.
    fn equalisation_hash(&self) -> &str {
        self.users
            .values()
            .filter_map(|u| {
                let parsed = PasswordHash::new(&u.password_argon2id).ok()?;
                let params = Params::try_from(&parsed).ok()?;
                let work = u64::from(params.m_cost()) * u64::from(params.t_cost());
                Some((work, params.p_cost(), u.password_argon2id.as_str()))
            })
            // `max_by_key` over the whole tuple, so the hash string itself is the final
            // tie-break: the choice must not depend on `HashMap` iteration order, or it
            // would drift between runs and between ACL reloads.
            .max_by_key(|&(work, p, hash)| (work, p, hash))
            .map(|(_, _, hash)| hash)
            // No users at all — nothing to borrow, so fall back to the minted default.
            // Vacuous in this case: with zero accounts *every* login is an unknown-user
            // login, so there is nothing for it to be distinguished from.
            .unwrap_or_else(|| dummy_hash().as_str())
    }

    /// Does `user` hold a `read` grant on `graph`?
    pub fn can_read(&self, user: &str, graph: &str) -> bool {
        self.has_grant(user, graph, "read")
    }

    /// Does `user` hold a `write` grant on `graph`?
    ///
    /// **A `read` grant never implies `write`.** The writable layer (`delta.enabled`) is
    /// the only way a Bolt statement can mutate a graph, and every such statement is
    /// checked against this predicate — so an existing read-only ACL keeps its meaning
    /// when the writable layer is switched on, rather than silently gaining write access.
    /// A writer needs both: `"grants": { "g": ["read", "write"] }` (the write path resolves
    /// business keys, which is a read).
    pub fn can_write(&self, user: &str, graph: &str) -> bool {
        self.has_grant(user, graph, "write")
    }

    fn has_grant(&self, user: &str, graph: &str, perm: &str) -> bool {
        self.users.get(user).is_some_and(|u| {
            u.grants
                .get(graph)
                .is_some_and(|perms| perms.iter().any(|p| p == perm))
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

/// Verify a password against a stored PHC argon2 hash.
///
/// `None` means the *stored* hash would not parse, which is not the same answer as
/// "wrong password": no derive happened, so the call cost nothing. Callers that care
/// about timing must not collapse it into `false` — see [`Acl::verify`], which burns an
/// equalisation verify on that path. Logging stays here, at the site that saw the parse
/// fail, so a deployment in this state keeps shouting on every attempt.
fn verify_hash(stored: &str, password: &str) -> Option<bool> {
    match PasswordHash::new(stored) {
        Ok(parsed) => Some(
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
        ),
        Err(e) => {
            warn!(error = %e, "ACL contains a malformed password hash; rejecting");
            None
        }
    }
}

/// Last-resort equalisation hash for an ACL that holds **no users at all**, so
/// [`Acl::equalisation_hash`] has nothing to borrow. Built once on first use.
///
/// Not the params-of-record for anything: an ACL with users always equalises against
/// one of its own hashes. The literal fallback below is reached only if minting
/// itself fails (`OsRng` unavailable), and being a well-formed PHC string it still
/// burns a full verify rather than returning early.
fn dummy_hash() -> &'static String {
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
    /// BLAKE3 hex digest of the exact `acl.json` bytes this ACL was parsed from —
    /// the same hash a generation's manifest stamps as `aclBlake3`. Used to refuse
    /// a hot-reloaded ACL that no longer matches the served generation's stamp.
    digest: String,
}

/// Read and parse the ACL at `path`, returning the parsed ACL together with the
/// BLAKE3 digest of the exact bytes parsed (so the digest binds to the content
/// actually adopted, with no re-read race).
fn load_with_digest(path: &Path) -> Result<(Acl, String)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("read ACL {}", path.display()))?;
    let digest = graph_format::integrity::hash_bytes(text.as_bytes());
    let acl = Acl::from_json_str(&text).with_context(|| format!("parse ACL {}", path.display()))?;
    Ok((acl, digest))
}

impl AclHandle {
    /// Load the ACL once; errors if the initial file is missing or malformed
    /// (a server should refuse to start with no usable ACL).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let (acl, digest) = load_with_digest(&path)?;
        let mtime = file_mtime(&path);
        warn_if_world_writable(&path);
        Ok(Self {
            path,
            state: RwLock::new(State {
                acl: Arc::new(acl),
                mtime,
                digest,
            }),
        })
    }

    /// A cheap snapshot of the currently-active ACL.
    pub fn snapshot(&self) -> Arc<Acl> {
        self.state.read().unwrap().acl.clone()
    }

    /// The BLAKE3 digest of the `acl.json` bytes the active ACL was parsed from.
    pub fn digest(&self) -> String {
        self.state.read().unwrap().digest.clone()
    }

    /// Re-read the file and swap in the new ACL unconditionally. On a parse/IO
    /// error the last-good ACL is kept and the error logged loudly. Returns `true`
    /// if a new ACL was installed.
    ///
    /// Use this only where the caller has *already* established that the on-disk
    /// `acl.json` matches the served generation's stamp — notably right after a
    /// generation swap, whose policy check hashes the live ACL against the new
    /// stamp. For the hot-reload poll path use [`AclHandle::poll_checked`], which
    /// refuses an ACL that does not match the served stamp.
    pub fn reload(&self) -> bool {
        let mtime = file_mtime(&self.path);
        match load_with_digest(&self.path) {
            Ok((acl, digest)) => {
                let mut s = self.state.write().unwrap();
                s.acl = Arc::new(acl);
                s.mtime = mtime;
                s.digest = digest;
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

    /// Hot-reload variant that **enforces the manifest ACL stamp**: a freshly read
    /// `acl.json` is adopted only when `accept(digest)` returns `true` — i.e. its
    /// BLAKE3 digest matches the `aclBlake3` of every stamped served generation. A
    /// digest that does not match is treated as post-generation tampering: the
    /// last-good ACL is kept and the divergence logged loudly (a malformed file is
    /// handled the same way). The legitimate path for changing the ACL is to
    /// rebuild and publish a generation stamped against the new `acl.json`; the
    /// swap then adopts it via [`AclHandle::reload`].
    ///
    /// Returns `true` only if a new ACL was actually installed.
    pub fn reload_checked(&self, accept: impl Fn(&str) -> bool) -> bool {
        let mtime = file_mtime(&self.path);
        match load_with_digest(&self.path) {
            Ok((acl, digest)) if accept(&digest) => {
                let mut s = self.state.write().unwrap();
                s.acl = Arc::new(acl);
                s.mtime = mtime;
                s.digest = digest;
                info!(path = %self.path.display(), users = s.acl.users.len(), "reloaded ACL");
                true
            }
            Ok((_, digest)) => {
                // Stamp mismatch: refuse the edit and keep the last-good ACL that
                // matches the served generation. Advance mtime so a steady-state
                // tamper is logged once, not every poll.
                warn!(
                    path = %self.path.display(),
                    digest = %digest,
                    "live acl.json does not match the served generation's ACL stamp — refusing the \
                     hot-reload and keeping the last-good ACL (rebuild the generation against the \
                     new acl.json to change access control)"
                );
                self.state.write().unwrap().mtime = mtime;
                false
            }
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "ACL reload failed; keeping last-good ACL");
                self.state.write().unwrap().mtime = mtime;
                false
            }
        }
    }

    /// Stamp-enforcing counterpart to [`AclHandle::poll`]: reload (via
    /// [`AclHandle::reload_checked`]) only when the file's mtime changed. Returns
    /// `true` if a reload was attempted (whether or not it was accepted).
    pub fn poll_checked(&self, accept: impl Fn(&str) -> bool) -> bool {
        let current = file_mtime(&self.path);
        let changed = {
            let s = self.state.read().unwrap();
            current != s.mtime
        };
        if changed {
            self.reload_checked(accept);
        }
        changed
    }
}

/// Warn (once, at load) if `acl.json` is group- or world-writable. Its runtime
/// integrity rests on the manifest ACL stamp plus filesystem permissions; a
/// writable-by-others ACL is a standing tamper risk worth surfacing. Unix-only;
/// a no-op elsewhere.
fn warn_if_world_writable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mode = meta.permissions().mode();
            if mode & 0o022 != 0 {
                warn!(
                    path = %path.display(),
                    mode = format!("{:o}", mode & 0o777),
                    "acl.json is group- or world-writable; restrict it to the server user \
                     (chmod 600) — its integrity depends on filesystem permissions between \
                     generation swaps"
                );
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
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

    /// Mint a hash at explicit, non-default argon2id parameters — the shape an
    /// operator produces with any third-party tool, which `acl.json` accepts happily.
    fn hash_password_with(password: &str, m_cost: u32, t_cost: u32, p_cost: u32) -> String {
        let params = Params::new(m_cost, t_cost, p_cost, None).unwrap();
        let a = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let salt = SaltString::generate(&mut OsRng);
        a.hash_password(password.as_bytes(), &salt)
            .unwrap()
            .to_string()
    }

    fn params_of(phc: &str) -> (u32, u32, u32) {
        let parsed = PasswordHash::new(phc).unwrap();
        let p = Params::try_from(&parsed).unwrap();
        (p.m_cost(), p.t_cost(), p.p_cost())
    }

    /// **The unknown-principal path must cost what a real login costs.** argon2
    /// verification reads `m`/`t`/`p` from the *stored* PHC string, so a dummy minted
    /// at `Argon2::default()` diverges the moment an operator mints with anything else
    /// — and username enumeration by timing returns. The equalisation hash must
    /// therefore be one the ACL actually holds. (HIK-222)
    ///
    /// Structural rather than timed, so it cannot go flaky: it asserts the *parameters*
    /// the unknown path will derive at, which is the property the timing depends on.
    #[test]
    fn the_equalisation_hash_tracks_the_stored_parameters() {
        // Deliberately *cheaper* than the default (m=19456, t=2): same divergence to
        // detect, but the test does less work than one at default cost. argon2 is
        // unoptimised in a debug build, so an expensive fixture would tax every run.
        let weak = hash_password_with("pw", 8, 1, 1);
        let json = serde_json::json!({
            "users": { "only": { "passwordArgon2id": weak, "grants": {} } }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();

        assert_eq!(
            params_of(acl.equalisation_hash()),
            (8, 1, 1),
            "the unknown-user path must derive at the stored parameters, not at \
             Argon2::default()'s — otherwise its cost diverges from a real login's"
        );
    }

    /// With mixed parameters, equalise *upward*: an unknown name must look like the
    /// most expensive known user. Equalising downward would make the known path the
    /// slow one and reopen the same oracle with the sign flipped.
    #[test]
    fn the_equalisation_hash_takes_the_costliest_stored_parameters() {
        let cheap = hash_password_with("pw", 8, 1, 1);
        let dear = hash_password_with("pw", 16, 3, 1); // 6x the block computations
        let json = serde_json::json!({
            "users": {
                "cheap": { "passwordArgon2id": cheap, "grants": {} },
                "dear":  { "passwordArgon2id": dear,  "grants": {} }
            }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();

        assert_eq!(
            params_of(acl.equalisation_hash()),
            (16, 3, 1),
            "the costliest stored parameters must win"
        );

        // Stable across reloads: the choice must not ride on HashMap iteration order.
        for _ in 0..8 {
            let again = Acl::from_json_str(&json.to_string()).unwrap();
            assert_eq!(again.equalisation_hash(), acl.equalisation_hash());
        }
    }

    /// An ACL with no users has nothing to borrow and falls back to the minted dummy.
    /// Vacuous for enumeration — with zero accounts every login is an unknown one —
    /// but it must still be a well-formed hash that burns a real verify.
    #[test]
    fn an_empty_acl_still_burns_a_verify_for_an_unknown_principal() {
        let acl = Acl::from_json_str(r#"{"users":{}}"#).unwrap();
        let phc = acl.equalisation_hash();
        assert!(phc.starts_with("$argon2id$"), "got {phc}");
        assert!(PasswordHash::new(phc).is_ok(), "must be parseable: {phc}");
        assert!(!acl.verify("nobody", "wrong"));
    }

    /// ADVERSARIAL: a malformed stored hash must not become the equalisation hash,
    /// and must not make the ACL fall back to the default-params dummy either.
    #[test]
    fn adversarial_malformed_hash_is_skipped_not_selected() {
        let good = hash_password_with("pw", 16, 2, 1);
        let json = serde_json::json!({
            "users": {
                "broken": { "passwordArgon2id": "not-a-phc-string", "grants": {} },
                "good":   { "passwordArgon2id": good, "grants": {} }
            }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();
        assert_eq!(params_of(acl.equalisation_hash()), (16, 2, 1));
        assert!(!acl.verify("nobody", "x"));
    }

    /// ADVERSARIAL: an ACL where *every* stored hash is malformed has nothing valid to
    /// borrow. It must still burn a real verify rather than returning early.
    #[test]
    fn adversarial_all_malformed_still_burns_a_verify() {
        let json = serde_json::json!({
            "users": { "a": { "passwordArgon2id": "garbage", "grants": {} } }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();
        let phc = acl.equalisation_hash();
        assert!(
            PasswordHash::new(phc).is_ok(),
            "must fall back to a usable hash: {phc}"
        );
        assert!(!acl.verify("nobody", "x"));
    }

    /// ADVERSARIAL: the unknown-user arm must return false even when the supplied
    /// password is the *correct* one for the borrowed hash's owner.
    #[test]
    fn adversarial_correct_password_for_the_borrowed_hash_authenticates_nobody() {
        let json = serde_json::json!({
            "users": { "real": { "passwordArgon2id": hash_password("s3cret").unwrap(), "grants": {} } }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();
        assert!(
            acl.verify("real", "s3cret"),
            "precondition: the real login works"
        );
        assert!(
            !acl.verify("ghost", "s3cret"),
            "a correct password against the borrowed hash must still authenticate nobody"
        );
    }

    /// **A known user with a malformed stored hash must not answer faster than a real
    /// one.** HIK-222 equalised the *unknown*-principal arm against a borrowed stored
    /// hash, but the third arm was left short: a stored hash that fails to parse returned
    /// `false` on the parse error alone, with no derive at all. That is microseconds
    /// against milliseconds, and it is an oracle for "this username exists but its hash
    /// is broken" — the same bug class, one arm over. (HIK-238)
    ///
    /// Timed rather than structural, because what is being asserted *is* the cost. Two
    /// things keep it off the flaky list: each arm is the **minimum** of several samples
    /// (scheduler noise only ever adds time, so the minimum is the closest estimate of
    /// what the path really costs), and the band is an order of magnitude while the
    /// defect is three.
    #[test]
    fn a_malformed_stored_hash_still_burns_an_equalisation_verify() {
        // Cheap, non-default parameters: enough work to be measurable against a parse
        // failure, little enough not to tax every `cargo test`. argon2 is unoptimised in
        // a debug build, so a default-cost fixture would cost ~100x this.
        let good = hash_password_with("pw", 32, 1, 1);
        let json = serde_json::json!({
            "users": {
                "good":   { "passwordArgon2id": good, "grants": {} },
                "broken": { "passwordArgon2id": "not-a-phc-string", "grants": {} }
            }
        });
        let acl = Acl::from_json_str(&json.to_string()).unwrap();

        // The arm under test is "known user, malformed hash". If serde ever dropped the
        // entry, `broken` would silently become an *unknown* user — the arm HIK-222
        // already equalised — and this test would pass while proving nothing. Pinned
        // structurally rather than left resting on the red run having caught it once.
        assert!(
            acl.users.contains_key("broken"),
            "fixture: `broken` must be a KNOWN user, or this measures the unknown arm"
        );

        // Warm first: the first derive pays a cold allocation, and an unknown principal
        // may mint the lazy fallback dummy. Neither belongs in the measurement.
        assert!(!acl.verify("good", "wrong"));
        assert!(!acl.verify("nobody", "wrong"));

        let best = |user: &str| {
            (0..5)
                .map(|_| {
                    let t0 = std::time::Instant::now();
                    assert!(!acl.verify(user, "wrong"), "{user} must not authenticate");
                    t0.elapsed()
                })
                .min()
                .unwrap()
        };
        let known = best("good");
        let unknown = best("nobody");
        let malformed = best("broken");

        // Asserted FIRST and separately: this is HIK-222's property, which this fixture
        // relies on but does not test. If it fails, the fixture is wrong (a hash that did
        // not mint, an entry serde dropped) rather than this ticket's arm being open —
        // and this test deliberately constructs a malformed hash, so the two failure
        // modes are easy to confuse.
        assert!(
            unknown * 10 >= known && known * 10 >= unknown,
            "precondition (HIK-222): unknown {unknown:?} vs known {known:?} — the \
             fixture is not exercising an equalised unknown-user path, so nothing below \
             this line means what it says"
        );

        assert!(
            malformed * 10 >= known,
            "a known user with a malformed stored hash answered in {malformed:?} against \
             {known:?} for a well-formed one — the malformed arm skips the derive \
             entirely, so an attacker can enumerate accounts whose stored hash is broken"
        );
    }

    #[test]
    fn hash_is_argon2id_and_verifies() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(hash.starts_with("$argon2id$"), "got {hash}");
        assert_eq!(
            verify_hash(&hash, "correct horse battery staple"),
            Some(true)
        );
        assert_eq!(verify_hash(&hash, "wrong password"), Some(false));
        // A malformed *stored* hash is a third answer, not a `false`: no derive ran, so
        // the caller must decide what that costs. (HIK-238)
        assert_eq!(verify_hash("not-a-phc-string", "anything"), None);
    }

    #[test]
    fn verify_checks_user_and_password() {
        let acl = acl_with("reporting", "s3cret", &[("eu_ai_act", &["read"])]);
        assert!(acl.verify("reporting", "s3cret"));
        assert!(!acl.verify("reporting", "nope"));
        assert!(!acl.verify("ghost", "s3cret")); // unknown user
    }

    #[test]
    fn grants_are_per_graph_and_permission_specific() {
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

    /// `read` and `write` are independent grants: reading never confers the right to
    /// mutate, so enabling the writable layer cannot promote existing readers to writers.
    #[test]
    fn a_read_grant_never_implies_write() {
        let acl = acl_with(
            "reader",
            "pw",
            &[
                ("g", &["read"]),
                ("w_only", &["write"]),
                ("both", &["read", "write"]),
            ],
        );
        assert!(acl.can_read("reader", "g"));
        assert!(!acl.can_write("reader", "g"), "read must not imply write");

        // A write-only grant confers no read access (and so cannot even select the graph).
        assert!(acl.can_write("reader", "w_only"));
        assert!(!acl.can_read("reader", "w_only"));

        // The writer's grant is both.
        assert!(acl.can_read("reader", "both"));
        assert!(acl.can_write("reader", "both"));

        // Unknown user / ungranted graph deny both.
        assert!(!acl.can_write("reader", "unlisted"));
        assert!(!acl.can_write("ghost", "both"));
    }

    /// An unrecognised permission string is inert — it grants nothing, rather than being
    /// silently treated as a wildcard.
    #[test]
    fn unknown_permission_strings_grant_nothing() {
        let acl = acl_with("u", "pw", &[("g", &["admin", "ALL", "Read", "WRITE"])]);
        assert!(!acl.can_read("u", "g"), "permissions are case-sensitive");
        assert!(!acl.can_write("u", "g"));
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
    fn reload_checked_enforces_the_stamp() {
        let dir = std::env::temp_dir().join(format!("slater_acl_checked_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("acl.json");

        let first = serde_json::json!({
            "users": { "alice": { "passwordArgon2id": hash_password("a").unwrap(), "grants": { "g": ["read"] } } }
        });
        std::fs::write(&path, first.to_string()).unwrap();
        let handle = AclHandle::load(&path).unwrap();
        let original = handle.digest();
        assert!(handle.snapshot().can_read("alice", "g"));

        // A runtime edit that diverges from the stamp (here: every digest rejected)
        // is refused — the new grant never takes effect and the last-good ACL stays.
        let tampered = serde_json::json!({
            "users": { "alice": { "passwordArgon2id": hash_password("a").unwrap(),
                "grants": { "g": ["read"], "secret": ["read"] } } }
        });
        std::fs::write(&path, tampered.to_string()).unwrap();
        assert!(
            !handle.reload_checked(|_| false),
            "tampered ACL must be refused"
        );
        assert!(
            !handle.snapshot().can_read("alice", "secret"),
            "a refused edit must not grant new access"
        );
        assert_eq!(handle.digest(), original, "digest unchanged after refusal");

        // When the digest is accepted (the legitimate rebuild-and-publish path), the
        // new ACL installs.
        assert!(handle.reload_checked(|_| true));
        assert!(handle.snapshot().can_read("alice", "secret"));

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
