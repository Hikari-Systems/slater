#!/usr/bin/env bash
# Slater compiles exactly ONE rustls crypto provider: `ring`.
#
# A second provider is not just bloat. With two compiled in, rustls cannot pick
# a process default on its own, and every `ServerConfig::builder()` /
# reqwest-without-a-provider call site panics at RUNTIME, not at build time.
# hs-utils' OTLP exporter hard-wires `ring`, so the provider that must not come
# back is aws-lc-rs: rustls' default, and the default of aws-config, aws-sdk-s3,
# gcloud-storage and reqwest's `rustls` feature. A dependency bump or a new crate
# with default features left on is all it takes.
#
# One aws-lc-rs edge is allowed: jsonwebtoken (GCS service-account JWT signing,
# `gcs` feature only). It is a signing library there, not a TLS provider; the
# alternative backend reintroduces two unfixed RustCrypto advisories (see
# crates/graph-format/Cargo.toml). Everything else must not depend on it.
set -euo pipefail

ALLOWED_DEPENDENTS='^jsonwebtoken$'
status=0

# (1) No feature combination may enable an aws-lc provider in any rustls-family crate.
feats=$(cargo tree --workspace --all-features --locked -e features,normal,build,dev -i rustls 2>&1 || true)
if grep -Eq 'rustls(-webpki)? feature "(aws[-_]lc[-_]rs|fips|prefer-post-quantum)"' <<<"$feats"; then
  echo "error: a rustls aws-lc provider feature is enabled; slater must compile only ring:" >&2
  grep -E 'feature "(aws[-_]lc[-_]rs|fips|prefer-post-quantum)"' <<<"$feats" | sort -u >&2
  status=1
fi
if ! grep -q 'rustls feature "ring"' <<<"$feats"; then
  echo "error: rustls is not built with ring (cargo tree output follows):" >&2
  echo "$feats" | head -20 >&2
  status=1
fi

# (2) aws-lc-rs may be depended on by the allowed signing edge only.
if out=$(cargo tree --workspace --all-features --locked -e normal,build,dev -i aws-lc-rs --depth 1 --prefix none 2>&1); then
  bad=$(tail -n +2 <<<"$out" | awk '{print $1}' | sort -u | grep -Ev "$ALLOWED_DEPENDENTS" | paste -sd, - || true)
  if [ -n "$bad" ]; then
    echo "error: aws-lc-rs is depended on by: $bad (only jsonwebtoken is allowed)" >&2
    status=1
  fi
elif ! grep -q "did not match any packages" <<<"$out"; then
  echo "error: cargo tree failed:" >&2; echo "$out" >&2; status=1
fi

# (3) The default build (the lite image, and plain `cargo build`) has no aws-lc at all.
for crate in aws-lc-rs aws-lc-sys; do
  if cargo tree --workspace --locked -e normal,build -i "$crate" >/dev/null 2>&1; then
    echo "error: $crate is in the default-features build" >&2
    status=1
  fi
done

[ "$status" -eq 0 ] && echo "ok: ring is the only rustls crypto provider"
exit "$status"
