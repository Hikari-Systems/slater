// SPDX-License-Identifier: Apache-2.0
//! `impl ConnCtx` — the shared per-server connection context.
//!
//! Split out of `server.rs` as a child module (a pure relocation). The struct,
//! its fields and the shared helpers stay in the parent, reached via `use super::*`.

use super::*;

impl ConnCtx {
    /// The graph `user` last selected via `USE`, if any.
    pub(crate) fn current_selection(&self, user: &str) -> Option<String> {
        self.use_selection.read().unwrap().get(user).cloned()
    }

    /// Record `user`'s `USE <graph>` selection for subsequent db-less queries.
    pub(crate) fn set_selection(&self, user: &str, graph: &str) {
        self.use_selection
            .write()
            .unwrap()
            .insert(user.to_string(), graph.to_string());
    }

    /// Has `user` issued a Memgraph-dialect statement on any connection?
    pub(crate) fn is_memgraph(&self, user: &str) -> bool {
        self.memgraph_users.read().unwrap().contains(user)
    }

    /// Flag `user` as a Memgraph-dialect client (idempotent).
    pub(crate) fn mark_memgraph(&self, user: &str) {
        self.memgraph_users
            .write()
            .unwrap()
            .insert(user.to_string());
    }
}

impl ConnCtx {
    /// Resolve the graph a `RUN` targets and check the user may read it. An explicit
    /// `db` in the message metadata wins; otherwise the user's single readable graph
    /// is used, and ambiguity (or none) is an error.
    pub(crate) fn select_graph(
        &self,
        extra: &PsValue,
        user: &str,
        sticky: Option<&str>,
    ) -> std::result::Result<String, Failure> {
        let acl = self.acl.snapshot();
        if let Some(db) = extra
            .get("db")
            .and_then(PsValue::as_str)
            .filter(|s| !s.is_empty())
        {
            if self.graphs.get(db).is_none() {
                let mut served: Vec<String> = self
                    .graphs
                    .names()
                    .into_iter()
                    .filter(|g| acl.can_read(user, g))
                    .collect();
                served.sort();
                return Err(Failure::new(
                    CODE_NOT_FOUND,
                    format!(
                        "graph '{db}' is not served (available: {})",
                        served.join(", ")
                    ),
                ));
            }
            if !acl.can_read(user, db) {
                return Err(Failure::new(
                    CODE_FORBIDDEN,
                    format!("user '{user}' has no read grant on graph '{db}'"),
                ));
            }
            return Ok(db.to_string());
        }
        // No explicit `db`: honour a sticky `USE <graph>` selection if it is still
        // served and readable, before falling back to the single-graph / ambiguous
        // resolution.
        if let Some(g) = sticky {
            if self.graphs.get(g).is_some() && acl.can_read(user, g) {
                return Ok(g.to_string());
            }
        }
        let mut readable: Vec<String> = self
            .graphs
            .names()
            .into_iter()
            .filter(|g| acl.can_read(user, g))
            .collect();
        match readable.len() {
            1 => Ok(readable.pop().unwrap()),
            0 => Err(Failure::new(
                CODE_FORBIDDEN,
                format!("user '{user}' has no readable graph"),
            )),
            // Ambiguous: the session named no graph but can read several. We do NOT
            // silently fall back to a default — that masks a mistyped or unset graph
            // name by serving an unrelated graph. Require an exact name and tell the
            // client which graphs are on offer.
            _ => {
                readable.sort();
                Err(Failure::new(
                    CODE_NOT_FOUND,
                    format!(
                        "no graph selected: name an exact graph in the connection's \
                         database field (one of: {})",
                        readable.join(", ")
                    ),
                ))
            }
        }
    }

    /// The graphs `user` may read, each flagged whether it is the default/home
    /// graph (the configured `defaultGraph`, or the sole graph when there is one).
    /// Used to answer `SHOW DATABASES`.
    pub(crate) fn readable_databases(&self, user: &str) -> Vec<(String, bool)> {
        let acl = self.acl.snapshot();
        let mut names: Vec<String> = self
            .graphs
            .names()
            .into_iter()
            .filter(|g| acl.can_read(user, g))
            .collect();
        names.sort();
        let default = self
            .default_graph
            .clone()
            .filter(|dg| names.iter().any(|g| g == dg))
            .or_else(|| (names.len() == 1).then(|| names[0].clone()));
        names
            .into_iter()
            .map(|n| {
                let is_default = default.as_deref() == Some(n.as_str());
                (n, is_default)
            })
            .collect()
    }

    /// Intercept the read-only introspection / metadata statements a browser GUI
    /// fires on connect (which the strict read-only Cypher grammar would reject),
    /// answering them from the in-memory manifest. Returns `Ok(None)` for anything
    /// that is not such a statement, so the caller falls through to the query path.
    /// Assemble the live (non-counter) state the diagnostics snapshot needs:
    /// connection-semaphore occupancy, configured caps, and the three cache pools.
    /// Read on demand from `CALL slater.diagnostics()` only, never on the hot path.
    pub(crate) fn live_gauges(&self) -> crate::diag::LiveGauges {
        use crate::diag::{CachePoolSnapshot, LiveGauges};
        // In-use = configured permits − currently available. `semaphore_permits`
        // maps the "0 = unlimited" config to the sentinel the semaphore was built
        // with, so the subtraction matches the live permit count either way.
        let in_use = |configured: usize, sem: &Semaphore| -> u64 {
            (semaphore_permits(configured) as u64).saturating_sub(sem.available_permits() as u64)
        };
        let (bm, vm, rm) = (
            self.cache.metrics(),
            self.vector_cache.metrics(),
            self.result_cache.metrics(),
        );
        LiveGauges {
            conn_in_use: in_use(self.max_connections, &self.conn_limit),
            conn_limit: self.max_connections as u64,
            pre_auth_in_use: in_use(self.max_pre_auth_connections, &self.pre_auth_limit),
            pre_auth_limit: self.max_pre_auth_connections as u64,
            max_per_ip: self.max_per_ip as u64,
            max_rows: self.max_rows as u64,
            timeout_ms: self.timeout_ms,
            max_intermediate: self.max_intermediate,
            max_scan: self.max_scan,
            max_intermediate_global: self.intermediate_budget.limit(),
            intermediate_global_in_use: self.intermediate_budget.in_use(),
            intermediate_global_peak: self.intermediate_budget.peak(),
            max_shortest_path_explore: self.max_shortest_path_explore,
            // The effective fanout = the pool's thread count (1 when sequential).
            max_fanout: self
                .fanout_pool
                .as_ref()
                .map_or(1, |p| p.current_num_threads()) as u64,
            max_message_bytes: self.max_message_bytes as u64,
            block_cache: CachePoolSnapshot {
                bytes: self.cache.bytes() as u64,
                entries: self.cache.len() as u64,
                hits: bm.hits,
                misses: bm.misses,
                evictions: bm.evictions,
            },
            vector_cache: CachePoolSnapshot {
                bytes: self.vector_cache.bytes() as u64,
                entries: self.vector_cache.block_count() as u64,
                hits: vm.hits,
                misses: vm.misses,
                evictions: vm.evictions,
            },
            result_cache: CachePoolSnapshot {
                bytes: self.result_cache.bytes() as u64,
                entries: self.result_cache.len() as u64,
                hits: rm.hits,
                misses: rm.misses,
                evictions: rm.evictions,
            },
        }
    }

    pub(crate) fn introspect(
        &self,
        user: &str,
        extra: &PsValue,
        query: &str,
        sticky: Option<&str>,
        memgraph: bool,
    ) -> std::result::Result<Option<introspect::Rows>, Failure> {
        let q = normalize_query(query);

        // Graph-agnostic (server-level) statements — answerable without a graph.
        let agnostic = match q.as_str() {
            _ if q.starts_with("call dbms.components") => Some(introspect::dbms_components()),
            // Memgraph Lab and Neo4j Browser want different `SHOW DATABASES` shapes.
            _ if q.starts_with("show databases") => Some(if memgraph {
                introspect::show_databases_memgraph(&self.readable_databases(user))
            } else {
                introspect::show_databases(&self.readable_databases(user), &self.bind_addr, |g| {
                    self.graphs.writer(g).is_some()
                })
            }),
            _ if q.starts_with("show default database") => Some(introspect::show_databases(
                &self.readable_databases(user),
                &self.bind_addr,
                |g| self.graphs.writer(g).is_some(),
            )),
            _ if q.starts_with("show version") => Some(introspect::show_version()),
            _ if q.starts_with("show license info") => Some(introspect::show_license_info()),
            _ if q.starts_with("show replication role") => {
                Some(introspect::show_replication_role())
            }
            _ if q == "show database" => Some(introspect::show_database(sticky)),
            _ if q.starts_with("show procedures") => Some(introspect::show_procedures()),
            _ if q.starts_with("show functions") => {
                Some(introspect::empty(&["name", "category", "description"]))
            }
            _ if q.starts_with("show constraints") => Some(introspect::empty(&[
                "id",
                "name",
                "type",
                "entityType",
                "labelsOrTypes",
                "properties",
                "ownedIndex",
            ])),
            _ if q.starts_with("show constraint info") => Some(introspect::empty(&[
                "constraint type",
                "label",
                "properties",
            ])),
            _ if q.starts_with("show triggers") => Some(introspect::empty(&[
                "trigger name",
                "statement",
                "event type",
                "phase",
                "owner",
            ])),
            _ if q.starts_with("show transactions") => Some(introspect::empty(&[
                "transactionId",
                "username",
                "currentQuery",
            ])),
            _ => None,
        };
        if let Some(rows) = agnostic {
            return Ok(Some(rows));
        }

        // `CALL slater.diagnostics()` / `SHOW SERVER DIAGNOSTICS` — the gated
        // load-test health snapshot: process RSS/CPU, the cgroup memory & CPU
        // limits, connection-cap headroom, per-reason query-failure tallies, and
        // latency percentiles. Server-level (no graph needed). Errors unless
        // `loadTestDiagnostics` is on, so the surface stays dark by default and the
        // hot path keeps no extra state.
        if q.starts_with("call slater.diagnostics") || q.starts_with("show server diagnostics") {
            if !self.diag.enabled {
                return Err(Failure::new(
                    CODE_REQUEST,
                    "load-test diagnostics are disabled; set loadTestDiagnostics=true to \
                     enable CALL slater.diagnostics()"
                        .to_string(),
                ));
            }
            let live = self.live_gauges();
            let rows = self.diag.snapshot(&live);
            return Ok(Some(introspect::server_diagnostics(&rows)));
        }

        // `SHOW STORAGE INFO` is graph-scoped *and* carries the live per-pool cache
        // metrics (block / vector / result) so an operator can watch residency, hit
        // rate, and eviction pressure — the evidence for tuning the budget split.
        if q.starts_with("show storage info") {
            let graph = self.select_graph(extra, user, sticky)?;
            let gen = self.graphs.get(&graph).ok_or_else(|| {
                Failure::new(CODE_NOT_FOUND, format!("graph '{graph}' is not served"))
            })?;
            let (bm, vm, rm) = (
                self.cache.metrics(),
                self.vector_cache.metrics(),
                self.result_cache.metrics(),
            );
            let pools = [
                introspect::CachePoolStat {
                    name: "block",
                    bytes: self.cache.bytes(),
                    entries: self.cache.len(),
                    hits: bm.hits,
                    misses: bm.misses,
                    evictions: bm.evictions,
                },
                introspect::CachePoolStat {
                    name: "vector",
                    bytes: self.vector_cache.bytes(),
                    entries: self.vector_cache.block_count(),
                    hits: vm.hits,
                    misses: vm.misses,
                    evictions: vm.evictions,
                },
                introspect::CachePoolStat {
                    name: "result",
                    bytes: self.result_cache.bytes(),
                    entries: self.result_cache.len(),
                    hits: rm.hits,
                    misses: rm.misses,
                    evictions: rm.evictions,
                },
            ];
            return Ok(Some(introspect::show_storage_info_with_caches(
                gen.manifest(),
                &pools,
            )));
        }

        // Graph-scoped statements — resolve the graph (honouring an explicit `db`
        // or the default) and read its manifest.
        let scoped: Option<fn(&graph_format::manifest::Manifest) -> introspect::Rows> =
            if q.starts_with("call db.labels") {
                Some(introspect::db_labels)
            } else if q.starts_with("call db.relationshiptypes") {
                Some(introspect::db_relationship_types)
            } else if q.starts_with("call db.propertykeys") {
                Some(introspect::db_property_keys)
            } else if q.starts_with("show indexes") {
                Some(introspect::show_indexes)
            } else if q.starts_with("call db.indexes") {
                Some(introspect::db_indexes)
            } else if q.starts_with("show index info") {
                Some(introspect::show_index_info)
            } else if q.starts_with("call db.schema.visualization") {
                Some(|_| introspect::schema_visualization())
            } else {
                None
            };
        if let Some(build) = scoped {
            let graph = self.select_graph(extra, user, sticky)?;
            let gen = self.graphs.get(&graph).ok_or_else(|| {
                Failure::new(CODE_NOT_FOUND, format!("graph '{graph}' is not served"))
            })?;
            return Ok(Some(build(gen.manifest())));
        }

        Ok(None)
    }
}
