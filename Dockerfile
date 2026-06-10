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
