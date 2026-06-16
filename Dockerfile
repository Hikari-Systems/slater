# syntax=docker/dockerfile:1
#
# Slater — multi-stage build of the workspace's two binaries:
#   * slater       — the online, read-only Bolt server (default ENTRYPOINT).
#   * slater-build — the offline writer (alternate command; see README).
#
# House style: a `rust:1-bookworm` builder that first caches dependencies against
# stub sources, then compiles the real crates, and a slim `debian:bookworm-slim`
# runtime running as the non-root `appuser:1000`.

FROM rust:1-bookworm AS builder

WORKDIR /app

# Per-arch codegen tuning for the hot vector-kNN dot kernel, supplied by the
# release workflow's build matrix (amd64 → `-Ctarget-cpu=x86-64-v3` for AVX2+FMA;
# arm64 → empty, since aarch64 already mandates NEON at baseline). Promoted to an
# env so it applies to BOTH the dependency-cache build and the real build below.
# Default empty → a portable baseline build for local `docker build` without the
# arg. Note: `x86-64-v3` raises the amd64 CPU floor to Haswell (2013+).
ARG RUSTFLAGS=""
ENV RUSTFLAGS=$RUSTFLAGS

# Build deps for aws-lc-rs (pulled in transitively by rustls — D5): cmake + a
# C/C++ toolchain (clang), and libclang for its bindgen step. Without these the
# rustls/aws-lc-rs build fails. `git` is already present in rust:1-bookworm and is
# required because `.cargo/config.toml` forces the git CLI for the `hs-utils`
# git+tag fetch (libgit2's transport is unreliable in this environment).
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Dependency cache layer ────────────────────────────────────────────────────
# Copy only the manifests (+ the lockfile and the cargo config that forces the
# git CLI), synthesise stub sources for every workspace crate, and build once so
# the heavy dependency graph (tokio, rustls/aws-lc-rs, zstd, argon2, …) is cached
# in its own layer. Editing `crates/*/src` afterwards only recompiles our code.
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml .cargo/config.toml
COPY crates/graph-format/Cargo.toml crates/graph-format/Cargo.toml
COPY crates/slater-build/Cargo.toml crates/slater-build/Cargo.toml
COPY crates/slater/Cargo.toml crates/slater/Cargo.toml
RUN mkdir -p crates/graph-format/src crates/slater-build/src crates/slater/src \
    && echo '' > crates/graph-format/src/lib.rs \
    && echo 'fn main() {}' > crates/slater-build/src/main.rs \
    && echo '' > crates/slater/src/lib.rs \
    && echo 'fn main() {}' > crates/slater/src/main.rs \
    && cargo build --release --locked \
    && rm -rf crates/*/src \
       target/release/slater target/release/slater-build \
       target/release/deps/slater-* target/release/deps/slater_build-* \
       target/release/deps/graph_format-* target/release/deps/libgraph_format-*

# ── Real build ────────────────────────────────────────────────────────────────
COPY crates ./crates
RUN cargo build --release --locked --bin slater --bin slater-build

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/slater ./slater
COPY --from=builder /app/target/release/slater-build ./slater-build
# Baked-in defaults; per-environment overrides arrive via the /sandbox overlay
# and `KEY__sub` env vars (hs-utils layered config), see docker-compose.yml.
COPY config.json ./config.json
COPY acl.json ./acl.json

# Bound the glibc allocator's retained high-water under concurrency (load-test
# finding 1): without this, the default per-CPU arenas keep freed expansion/encode
# scratch resident, so a burst of concurrent hub 2-hops drives process RSS to many
# times the cache budget and can OOM a tightly-capped container even though the
# logical working set is small. `MALLOC_ARENA_MAX=2` caps arena count and the trim
# threshold returns freed chunks to the OS promptly. With the intermediate-budget
# guards charging expansion (so heavy queries fail cleanly), this keeps RSS bounded
# under the `wiki_budget` brown-out suite at 1000 concurrent clients. Override at
# run time if a workload genuinely benefits from more arenas.
ENV MALLOC_ARENA_MAX=2 \
    MALLOC_TRIM_THRESHOLD_=131072

RUN useradd -r -u 1000 appuser
USER appuser

# Bolt (optionally Bolt+TLS) — not HTTP.
EXPOSE 7687

# The probe speaks Bolt (a handshake against the configured port), not HTTP.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/app/slater", "healthcheck"]

# `slater` is the default. Run the offline writer with:
#   docker run --rm --entrypoint /app/slater-build <image> --input dump.cypher …
ENTRYPOINT ["/app/slater"]
