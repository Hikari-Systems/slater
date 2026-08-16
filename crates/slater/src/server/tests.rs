// SPDX-License-Identifier: Apache-2.0
//! Unit tests for the parent module (extracted verbatim from the inline
//! `mod tests`; a pure relocation, no test logic changed).

/// The Bolt version write tests encode results for — 5.4 is what the server
/// prefers, and the only one where element-id fields are emitted.
const TEST_BOLT_VERSION: (u8, u8) = (5, 4);

use super::*;
use crate::acl::hash_password;
use crate::testgen;
use tokio::net::TcpStream;

mod acl_and_grants;
mod compaction;
mod connection_security;
mod consolidation;
mod consolidation_crypto;
mod copy_and_ctx;
mod dump_and_gc;
mod flush_segment;
mod generation_guard;
mod gql_dialect;
mod limits_and_estimates;
mod reload_and_integrity;
mod resolve_and_keys;
mod session_identity;
mod vector_labels;
mod vector_ladder;
mod writes;

/// A minimal async Bolt client for the tests.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

/// Stand up a ConnCtx over the shared fixture graph + a temp ACL.
/// Per-connection security limits for the test ConnCtx builders. Defaults are
/// generous/on so existing tests are unaffected; the connection-security tests
/// pass tight values to exercise a specific gate.
#[derive(Clone)]
struct TestLimits {
    max_message_bytes: usize,
    max_pre_auth_bytes: usize,
    login_timeout_ms: u64,
    tls_handshake_timeout_ms: u64,
    idle_timeout_ms: u64,
    max_pre_auth_connections: usize,
    max_per_ip: usize,
    max_concurrent_auth: usize,
    max_auth_failures: usize,
    max_concurrent_writes: usize,
    max_concurrent_parses: usize,
    /// Turn the writable layer on for the fixture graph (WAL under `<root>/_wal`), so
    /// the ctx has a `DeltaWriter` and the RUN write arms are reachable.
    writable: bool,
    load_test_diagnostics: bool,
    /// Replace the single-user fixture ACL with one the test writes itself — for the
    /// multi-user grant checks, where "user B holds no read grant" is the point.
    acl_json: Option<serde_json::Value>,
}

/// The unit query vector every level test scores against.
const VQ: [f32; 2] = [1.0, 0.0];

/// The unit 2-vector at cosine **distance** `d` from [`VQ`]: `cos θ = 1 − d`, so the
/// distance a KNN scan reports for it is `d` itself (to f32 rounding). Lets a fixture and
/// a write be specified directly in the quantity the assertions are about.
fn at_distance(d: f64) -> Vec<f32> {
    let cos = 1.0 - d;
    let sin = (1.0 - cos * cos).max(0.0).sqrt();
    vec![cos as f32, sin as f32]
}

/// HIK-149's markers. The value is node content; the key is a symbol-table entry. Chosen
/// to be high-entropy and unique so a hit in the dump can only have come from the graph.
const MARKER_KEY: &str = "hik149canarykey";
const MARKER_VALUE: &str = "HIK149-CANARY-VALUE-7f3a91c2e5b8";

/// HIK-149: every file of a consolidation dump written for an **encrypted** graph must be
/// sealed. Asserted two ways, because either alone can pass against the bug:
///
/// * **No marker bytes anywhere in the directory.** Direct, but a false green is possible —
///   the `.blk` bodies are zstd-compressed, so a plaintext value might not appear verbatim.
/// * **Structurally sealed.** Every `.blk` reports the encrypted magic (a `BlockFileReader`
///   opened with no key is *refused*), and `meta.json` does not parse as JSON. This half
///   cannot be fooled by compression: it asks what the file *is*, not what it happens to
///   contain.
fn assert_dump_is_sealed(dump: &Path) {
    let mut leaked: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for entry in std::fs::read_dir(dump).expect("read the scratch dump dir") {
        let path = entry.unwrap().path();
        if !path.is_file() {
            continue;
        }
        checked += 1;
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let bytes = std::fs::read(&path).expect("read a dump file");
        for needle in [MARKER_KEY, MARKER_VALUE] {
            if bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                leaked.push(format!("{name} contains {needle:?} in the clear"));
            }
        }
        // What the file *is*, independent of what compression did to its contents.
        if name.ends_with(".blk") {
            if graph_format::blockfile::BlockFileReader::open(&path).is_ok() {
                leaked.push(format!("{name} opens with no key — it is not sealed"));
            }
        } else if name == "meta.json" {
            if serde_json::from_slice::<serde_json::Value>(&bytes).is_ok() {
                leaked.push("meta.json is readable JSON — it is not sealed".to_string());
            }
        } else if name.starts_with("carry.") {
            // A vector-carry sidecar. Reading it with no key must be *refused*, not read
            // as a plain id table: `expected` is deliberately the length the raw file
            // would have, so an unsealed sidecar sails through the length check and only a
            // real seal stops it.
            let raw_ids = (bytes.len() / 8) as u64;
            if graph_format::consolidate_dump::read_vector_carry_at(&path, raw_ids, None).is_ok() {
                leaked.push(format!("{name} reads with no key — it is not sealed"));
            }
        }
    }
    assert!(
        checked >= 4,
        "expected meta.json + three .blk files, saw {checked}"
    );
    assert!(
        leaked.is_empty(),
        "the consolidation dump of an encrypted graph is not sealed:\n  {}",
        leaked.join("\n  ")
    );
}

/// A `DeltaConfig` with the writable layer on and a throwaway WAL directory.
fn delta_cfg(wal_dir: &Path) -> DeltaConfig {
    DeltaConfig {
        enabled: true,
        wal_dir: wal_dir.to_string_lossy().into_owned(),
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: String::new(),
        builder_bin: "slater-build".to_string(),
        builder_max_memory: 0,
        builder_threads: 0,
        consolidate_timeout_secs: 0,
        off_heap_l0: false,
        segment_gc_grace_secs: 0,
    }
}

fn build_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(tag, TestLimits::default())
}

fn build_ctx_limited(tag: &str, limits: TestLimits) -> (std::path::PathBuf, Arc<ConnCtx>) {
    let (root, _graph, _) = testgen::write_basic(tag);
    let acl_path = match &limits.acl_json {
        Some(json) => {
            let path = root.join("acl.json");
            std::fs::write(&path, json.to_string()).unwrap();
            path
        }
        None => write_acl(&root),
    };
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    if limits.writable {
        graphs
            .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
            .unwrap();
    }
    let graphs = Arc::new(graphs);
    let cache = Arc::new(BlockCache::new(1 << 20));
    let vector_cache = Arc::new(VectorIndexCache::new(1 << 20));
    for gen in graphs.current_generations() {
        for vi in gen.vamana_indexes() {
            vector_cache.pin(gen.uuid(), vi.ord, vi.pq.clone());
        }
    }
    let result_cache = Arc::new(ResultCache::new(1 << 20));
    let ctx = Arc::new(ConnCtx {
        fulltext_max_hits: crate::config::DEFAULT_FULLTEXT_MAX_HITS,
        acl,
        graphs,
        cache,
        vector_cache,
        rw_indexes: Arc::new(RwIndexCache::new()),
        rw_index_cfg: crate::rwindex::RwIndexConfig::default(),
        result_cache,
        max_rows: 100_000,
        timeout_ms: 0,
        max_intermediate: 1_000_000,
        max_scan: 500_000_000,
        intermediate_budget: Arc::new(GlobalIntermediateBudget::new(8_000_000)),
        max_shortest_path_explore: 0,
        adj_stream_threshold: 8192,
        adj_stream_chunk: 8192,
        fanout_pool: None,
        beam_width: 64,
        temp_beam_width: 128,
        bind_addr: "127.0.0.1:7687".to_string(),
        default_graph: None,
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: limits.max_message_bytes,
        max_pre_auth_bytes: limits.max_pre_auth_bytes,
        login_timeout_ms: limits.login_timeout_ms,
        tls_handshake_timeout_ms: limits.tls_handshake_timeout_ms,
        idle_timeout_ms: limits.idle_timeout_ms,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_pre_auth_connections,
        ))),
        auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_concurrent_auth,
        ))),
        max_auth_failures: limits.max_auth_failures,
        write_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_concurrent_writes,
        ))),
        parse_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_concurrent_parses,
        ))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: limits.max_per_ip,
        diag: Arc::new(crate::diag::Diagnostics::new(limits.load_test_diagnostics)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: limits.max_pre_auth_connections,
        data_dir: root.clone(),
        builder_bin: "slater-build".to_string(),
        builder_limits: BuilderLimits::default(),
        builder_key_env: None,
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        segment_gc_grace_secs: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: None,
    });
    (root, ctx)
}

/// Spawn the connection handler over a fresh loopback listener, returning the
/// bound address so a client can connect. Goes through `serve_conn` (plaintext),
/// not `handle_connection` directly, so the tests exercise the same admission path
/// as production: the antechamber permit and the login deadline are taken at accept.
async fn spawn_server(ctx: Arc<ConnCtx>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _ = serve_conn(sock, None, ctx).await;
            });
        }
    });
    addr
}

/// Run a write statement against the graph's delta.
fn vwrite(graphs: &Graphs, graph: &str, q: &str) {
    vwrite_params(graphs, graph, q, &HashMap::new());
}

/// [`vwrite`] with bound parameters. A vector with a negative component has no *literal*
/// spelling the Phase 1c write grammar admits (a unary minus is an expression, not a
/// literal), so a re-embed onto an arbitrary vector has to go through `vecf32($v)`.
fn vwrite_params(graphs: &Graphs, graph: &str, q: &str, params: &HashMap<String, Val>) {
    let gen = graphs.get(graph).unwrap();
    let writer = graphs.writer(graph).unwrap();
    match parser::parse_statement(q).unwrap() {
        parser::ast::Statement::Write(w) => {
            execute_write(&writer, gen.as_ref(), &w, params, TEST_BOLT_VERSION)
                .unwrap_or_else(|e| panic!("write failed ({q}): {e:?}"));
        }
        _ => panic!("expected a write: {q}"),
    }
}

/// KNN over the merged view (base + segments + delta), as `(id, score)` in rank order.
fn vknn(graphs: &Graphs, graph: &str, cache: &BlockCache, q: &[f32], k: usize) -> Vec<(u64, f64)> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let parts: Vec<String> = q.iter().map(|x| format!("{x:?}")).collect();
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', {k}, vecf32([{}])) \
             YIELD node, score RETURN id(node) AS id, score",
        parts.join(", ")
    ))
    .unwrap();
    let res = Engine::new(&view, cache).run(&ast).unwrap();
    res.rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(i), Val::Float(s)) => (*i as u64, *s),
            other => panic!("unexpected KNN row {other:?}"),
        })
        .collect()
}

/// Assert no consolidation scratch dump survives under `<root>/<graph>/`.
///
/// Matches on [`CONSOLIDATE_SCRATCH_PREFIX`] rather than a fixed name: the dump directory
/// is uniquified per attempt, so the old `join(".consolidate.dump").exists()` assertion
/// would now pass without the cleanup running at all.
fn assert_no_consolidate_scratch(root: &Path, graph: &str) {
    let leftovers: Vec<_> = std::fs::read_dir(root.join(graph))
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(crate::server::registry::CONSOLIDATE_SCRATCH_PREFIX))
        .collect();
    assert!(
        leftovers.is_empty(),
        "consolidation scratch not cleaned up: {leftovers:?}"
    );
}

/// A freshly handshaken, unauthenticated session (no login deadline).
fn pre_auth_session() -> Session {
    Session {
        user: None,
        failed: false,
        pending: None,
        tx_graph: None,
        version: (5, 4),
        auth_failures: 0,
        login_deadline: None,
        in_tx: false,
        tx_writes: 0,
    }
}

/// A `LOGON` metadata map, as `authenticate` sees it once the message is decoded.
fn logon_meta(user: &str, pw: &str) -> PsValue {
    PsValue::Map(vec![
        ("scheme".into(), PsValue::str("basic")),
        ("principal".into(), PsValue::str(user)),
        ("credentials".into(), PsValue::str(pw)),
    ])
}

/// Number of `*.l0` segment files under `<wal>/<graph>/l0/`.
fn l0_count(wal_dir: &Path) -> usize {
    let l0 = wal_dir.join("l0");
    std::fs::read_dir(&l0)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("l0"))
                .count()
        })
        .unwrap_or(0)
}

/// Read a binary consolidation dump into a `{ node name → node props }` map, for
/// tests that assert the serialiser saw the merged state. Nodes are keyed by their
/// `name` property (the fixtures' business key).
fn dump_nodes(
    dump: &Path,
) -> std::collections::HashMap<String, Vec<(String, graph_format::ids::Value)>> {
    use graph_format::consolidate_dump::DumpReader;
    let r = DumpReader::open(dump, None).unwrap();
    let keys = r.meta().property_keys.clone();
    let mut out = std::collections::HashMap::new();
    r.for_each_node(|_, _lb, pb| {
        let props: Vec<(String, graph_format::ids::Value)> =
            graph_format::columns::decode_props(pb)
                .unwrap()
                .into_iter()
                .map(|(k, v)| (keys[k as usize].clone(), v))
                .collect();
        if let Some((_, graph_format::ids::Value::Str(name))) =
            props.iter().find(|(k, _)| k == "name")
        {
            out.insert(name.clone(), props);
        }
        Ok(())
    })
    .unwrap();
    out
}

/// Write a temp ACL granting `reporting`/`pw` read on `people`, return its path.
fn write_acl(root: &Path) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let json = serde_json::json!({
        "users": {
            "reporting": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"] }
            }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

/// Count the `*.wal` segment files under a WAL directory.
fn wal_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("wal"))
        .count()
}

/// Patch one top-level field in every generation manifest of `graph` under
/// `root`. Safe for fields outside the data-file inventory (e.g. `aclBlake3`),
/// which `content_hash` excludes, so `open_all` still validates afterwards.
fn patch_manifest(root: &Path, graph: &str, key: &str, value: serde_json::Value) {
    for entry in std::fs::read_dir(root.join(graph)).unwrap() {
        let man = entry.unwrap().path().join("MANIFEST.json");
        if man.exists() {
            let mut v: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
            v[key] = value.clone();
            std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
    }
}

/// A deterministic `n × dim` vector fixture plus the `slater-dump` script that creates it as
/// `(:Doc:__DumpVertex__ {__dump_id__, embedding})`. Shared by the plaintext and encrypted
/// carry-by-reference tests so both index exactly the same data.
#[cfg(test)]
fn vamana_fixture_script(dim: usize, n: usize) -> (String, Vec<Vec<f32>>) {
    let mut seed: u64 = 0xDEAD_BEEF_1234;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let mut script =
        format!("CALL db.idx.vector.createNodeIndex('Doc', 'embedding', {dim}, 'cosine');\n");
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    for i in 0..n {
        let v: Vec<f32> = (0..dim).map(|_| next()).collect();
        let body: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
        script.push_str(&format!(
            "CREATE (:Doc:__DumpVertex__ {{__dump_id__: {i}, embedding: vecf32([{}])}});\n",
            body.join(", ")
        ));
        vectors.push(v);
    }
    (script, vectors)
}

/// The segment directory names (uuid dirs, skipping dot-files) under `<root>/people/`.
fn seg_dirs(root: &Path) -> Vec<String> {
    std::fs::read_dir(root.join("people").join("segments"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| !n.starts_with('.'))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether two paths are the same inode — i.e. whether the carry hard-linked rather than
/// copied. `None` when either is missing.
#[cfg(test)]
fn same_inode(a: &Path, b: &Path) -> Option<bool> {
    use std::os::unix::fs::MetadataExt as _;
    let (a, b) = (std::fs::metadata(a).ok()?, std::fs::metadata(b).ok()?);
    Some(a.dev() == b.dev() && a.ino() == b.ino())
}

/// Copy `graph`'s live generation directory to a fresh UUID, optionally
/// truncating `corrupt` (a path relative to the generation dir) in the copy to
/// simulate a half-rsynced generation, then republish `current` to name the new
/// UUID. Returns the new UUID.
///
/// The copy's MANIFEST is restamped with the new `build_uuid`, because a generation is
/// no longer identified by its `current` pointer alone: HIK-144 requires the MANIFEST to
/// agree that it *is* the generation the set names, so that a directory cannot be
/// swapped underneath an authenticated set. A real publisher (the builder) writes that
/// field itself; only this hand-rolled copy has to restamp it.
fn publish_copy_as_new_generation(root: &Path, graph: &str, corrupt: Option<&str>) -> uuid::Uuid {
    let graph_dir = root.join(graph);
    let old = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_00ff);
    let src = graph_dir.join(old.trim());
    let dst = graph_dir.join(new_uuid.to_string());
    copy_dir_all(&src, &dst);
    {
        let man = dst.join("MANIFEST.json");
        let mut m: graph_format::manifest::Manifest =
            serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
        m.build_uuid = GenId(new_uuid);
        std::fs::write(&man, m.to_json().unwrap()).unwrap();
    }
    if let Some(rel) = corrupt {
        let victim = dst.join(rel);
        let mut bytes = std::fs::read(&victim).unwrap();
        bytes.truncate(bytes.len().saturating_sub(16));
        std::fs::write(&victim, bytes).unwrap();
    }
    std::fs::write(
        graph_dir.join("current"),
        format!("{}\n", new_uuid.hyphenated()),
    )
    .unwrap();
    new_uuid
}

/// Recursively load every file under `root` into a `MemObjectStore`, keyed by its
/// `/`-joined path relative to `root` — the same keys the store abstraction builds.
fn load_dir_into_mem(store: &graph_format::store::mem::MemObjectStore, root: &Path, dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            load_dir_into_mem(store, root, &path);
        } else {
            let key = path
                .strip_prefix(root)
                .unwrap()
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            store
                .put(&key, &std::fs::read(&path).unwrap(), None)
                .unwrap();
        }
    }
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Handshake: preamble + offer 5.4 then 4.4.
        let mut hs = Vec::new();
        hs.extend_from_slice(&handshake::PREAMBLE);
        hs.extend_from_slice(&[0, 0, 4, 5]);
        hs.extend_from_slice(&[0, 0, 4, 4]);
        hs.extend_from_slice(&[0, 0, 0, 0]);
        hs.extend_from_slice(&[0, 0, 0, 0]);
        stream.write_all(&hs).await.unwrap();
        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0, 0, 4, 5], "should negotiate Bolt 5.4");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, msg: PsValue) {
        self.stream
            .write_all(&message::to_wire(&msg))
            .await
            .unwrap();
    }

    /// Read the next response message as a decoded struct `(tag, fields)`.
    async fn recv(&mut self) -> (u8, Vec<PsValue>) {
        loop {
            if let Some((body, consumed)) = chunk::decode_message(&self.buf).unwrap() {
                self.buf.drain(..consumed);
                match crate::bolt::packstream::from_slice(&body).unwrap() {
                    PsValue::Struct { tag, fields } => return (tag, fields),
                    other => panic!("expected a struct, got {other:?}"),
                }
            }
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed unexpectedly");
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    fn hello() -> PsValue {
        PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str("slater-test/1.0"),
            )])],
        }
    }

    fn logon(user: &str, pw: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::LOGON,
            fields: vec![PsValue::Map(vec![
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str(user)),
                ("credentials".into(), PsValue::str(pw)),
            ])],
        }
    }

    /// A 4.4-style HELLO carrying auth inline (no separate LOGON).
    fn hello_with_auth(user: &str, pw: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![
                ("user_agent".into(), PsValue::str("slater-test/1.0")),
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str(user)),
                ("credentials".into(), PsValue::str(pw)),
            ])],
        }
    }

    fn run(query: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::RUN,
            fields: vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(vec![("db".into(), PsValue::str("people"))]),
            ],
        }
    }

    fn pull_all() -> PsValue {
        PsValue::Struct {
            tag: message::tag::PULL,
            fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
        }
    }

    fn discard(n: i64) -> PsValue {
        PsValue::Struct {
            tag: message::tag::DISCARD,
            fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(n))])],
        }
    }

    fn logoff() -> PsValue {
        PsValue::Struct {
            tag: message::tag::LOGOFF,
            fields: vec![],
        }
    }

    /// A RUN that names no `db` — the shape that resolves through `tx_graph`.
    fn run_no_db(query: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::RUN,
            fields: vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(vec![]),
            ],
        }
    }

    /// A BEGIN naming its target graph, which `Request::Begin` resolves into `tx_graph`.
    fn begin_db(graph: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::BEGIN,
            fields: vec![PsValue::Map(vec![("db".into(), PsValue::str(graph))])],
        }
    }

    /// Clears the Bolt FAILED state a failed LOGON leaves behind.
    fn reset() -> PsValue {
        PsValue::Struct {
            tag: message::tag::RESET,
            fields: vec![],
        }
    }
}

impl Default for TestLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            max_pre_auth_bytes: 64 * 1024,
            login_timeout_ms: 0, // off by default so unrelated tests never time out
            tls_handshake_timeout_ms: 0,
            idle_timeout_ms: 0,
            max_pre_auth_connections: 4_096,
            max_per_ip: 0,                // unlimited by default
            max_concurrent_auth: 4,       // as in prod
            max_auth_failures: 3,         // as in prod
            max_concurrent_writes: 4,     // as in prod
            max_concurrent_parses: 32,    // as in prod
            writable: false,              // read-only unless a test asks for writes
            load_test_diagnostics: false, // diagnostics off by default, as in prod
            acl_json: None,               // the single-user fixture ACL
        }
    }
}

/// Exactly the swap the generation guard performs on a graph it is allowed to swap:
/// take the graph's swap mutex, then adopt whatever `current` names. `guard_sweep`
/// inlines this (it must hold the same lock across its *decision*, not just the
/// swap), so this is how a test makes the guard's swap happen at a chosen instant —
/// including inside another operation's publish window.
fn guard_swap(graphs: &Graphs, name: &str, vc: &VectorIndexCache) -> Result<Option<GenId>> {
    let _swap = graphs.swap_lock(name)?;
    graphs.swap_locked_guard(name, vc)
}

/// Re-embed a `:Doc` fixture node onto `vector`, through a bound `vecf32($v)`.
fn embed_param(graphs: &Graphs, graph: &str, name: &str, vector: &[f32]) {
    let mut params = HashMap::new();
    params.insert(
        "v".to_string(),
        Val::List(vector.iter().map(|x| Val::Float(*x as f64)).collect()),
    );
    vwrite_params(
        graphs,
        graph,
        &format!("MATCH (n:Doc {{name:'{name}'}}) SET n.embedding = vecf32($v)"),
        &params,
    );
}

/// The dumped `(node_id, vector)` set of the consolidation view — what a rebuild indexes.
fn dump_vectors(
    graphs: &Graphs,
    graph: &str,
    cache: &BlockCache,
    dump: &std::path::Path,
) -> Vec<(u64, Vec<f32>)> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    std::fs::create_dir_all(dump).unwrap();
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, cache), &view, dump, None)
        .unwrap();
    let reader = graph_format::consolidate_dump::DumpReader::open(dump, None).unwrap();
    let mut out: Vec<(u64, Vec<f32>)> = Vec::new();
    reader
        .for_each_vector(|node_id, _key_id, v| {
            out.push((node_id, v.to_vec()));
            Ok(())
        })
        .unwrap();
    out.sort_by_key(|(id, _)| *id);
    out
}

/// The integer `age` of node `name` in a binary dump, if present.
fn dump_age(dump: &Path, name: &str) -> Option<i64> {
    dump_nodes(dump).get(name).and_then(|p| {
        p.iter()
            .find(|(k, _)| k == "age")
            .and_then(|(_, v)| match v {
                graph_format::ids::Value::Int(i) => Some(*i),
                _ => None,
            })
    })
}

/// [`delta_cfg`] reading sealed L0 levels **off-heap** (a block image paged through the
/// shared cache, not a resident memtable) — the config a T2 flush over off-heap L0 exercises.
fn delta_cfg_offheap(wal_dir: &Path) -> DeltaConfig {
    DeltaConfig {
        off_heap_l0: true,
        ..delta_cfg(wal_dir)
    }
}

/// Recursively copy `src` to `dst` (files + subdirectories).
fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

/// The bytes of the served generation's `Doc.embedding` Vamana graph, wherever it lives —
/// inside the generation directory (a freshly built or rewritten index) or in its own
/// `vecidx/<uuid>/` artifact directory (a carried one, HIK-145).
#[cfg(test)]
fn carried_vamana_bytes(data: &Path, graph: &str, gen: &Generation) -> Vec<u8> {
    let rel = "vector/Doc.embedding.vamana";
    let in_gen = data.join(graph).join(gen.base_uuid().to_string()).join(rel);
    if in_gen.exists() {
        return std::fs::read(&in_gen).unwrap();
    }
    let vecidx = data.join(graph).join("vecidx");
    for e in std::fs::read_dir(&vecidx).expect("no in-generation .vamana and no vecidx/ dir") {
        let d = e.unwrap().path();
        let f = d.join("Doc.embedding.vamana");
        if f.exists() {
            return std::fs::read(&f).unwrap();
        }
    }
    panic!("no carried .vamana found under {}", vecidx.display());
}

fn acl_json(grants: serde_json::Value) -> Acl {
    let json = serde_json::json!({
        "users": { "u": { "passwordArgon2id": hash_password("pw").unwrap(), "grants": grants } }
    });
    Acl::from_json_str(&json.to_string()).unwrap()
}

/// A session already past LOGON, as every RUN in these tests needs.
fn authenticated_session(user: &str) -> Session {
    Session {
        user: Some(user.into()),
        failed: false,
        pending: None,
        tx_graph: None,
        version: (5, 4),
        auth_failures: 0,
        login_deadline: None,
        in_tx: false,
        tx_writes: 0,
    }
}

/// A ConnCtx serving two graphs (`people` + a copy `places`), with `reporting`
/// granted read on both — exercises the ambiguous (multi-graph) selection path.
fn build_multi_ctx(tag: &str) -> Arc<ConnCtx> {
    let (root, _graph, _) = testgen::write_basic(tag);
    let places = root.join("places");
    copy_dir(&root.join("people"), &places);
    // The manifest records its own graph name (and open_all rejects a mismatch);
    // the data-file content hash excludes MANIFEST.json, so renaming the copied
    // graph to "places" only requires patching that one field.
    for entry in std::fs::read_dir(&places).unwrap() {
        let gen_dir = entry.unwrap().path();
        let man = gen_dir.join("MANIFEST.json");
        if man.exists() {
            let mut v: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
            v["graph"] = serde_json::json!("places");
            std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
    }
    let acl_path = root.join("acl.json");
    let json = serde_json::json!({
        "users": { "reporting": {
            "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"], "places": ["read"] }
        }}
    });
    std::fs::write(&acl_path, json.to_string()).unwrap();
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
    Arc::new(ConnCtx {
        fulltext_max_hits: crate::config::DEFAULT_FULLTEXT_MAX_HITS,
        acl,
        graphs,
        cache: Arc::new(BlockCache::new(1 << 20)),
        vector_cache: Arc::new(VectorIndexCache::new(1 << 20)),
        rw_indexes: Arc::new(RwIndexCache::new()),
        rw_index_cfg: crate::rwindex::RwIndexConfig::default(),
        result_cache: Arc::new(ResultCache::new(1 << 20)),
        max_rows: 100_000,
        timeout_ms: 0,
        max_intermediate: 1_000_000,
        max_scan: 500_000_000,
        intermediate_budget: Arc::new(GlobalIntermediateBudget::new(8_000_000)),
        max_shortest_path_explore: 0,
        adj_stream_threshold: 8192,
        adj_stream_chunk: 8192,
        fanout_pool: None,
        beam_width: 64,
        temp_beam_width: 128,
        bind_addr: "127.0.0.1:7687".to_string(),
        // A default is configured but must NOT be silently served for queries.
        default_graph: Some("people".to_string()),
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: 64 * 1024 * 1024,
        max_pre_auth_bytes: 64 * 1024,
        login_timeout_ms: 0,
        tls_handshake_timeout_ms: 0,
        idle_timeout_ms: 0,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(4_096))),
        auth_limit: Arc::new(Semaphore::new(semaphore_permits(4))),
        max_auth_failures: 3,
        write_limit: Arc::new(Semaphore::new(semaphore_permits(4))),
        parse_limit: Arc::new(Semaphore::new(semaphore_permits(32))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: 0,
        diag: Arc::new(crate::diag::Diagnostics::new(false)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: 4_096,
        data_dir: root.clone(),
        builder_bin: "slater-build".to_string(),
        builder_limits: BuilderLimits::default(),
        builder_key_env: None,
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        segment_gc_grace_secs: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: None,
    })
}

/// Recursively copy a (small fixture) directory tree.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}
