#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Assert both Dockerfiles' dependency-cache stub lists cover every declared target.

Why this exists
---------------
The builder stages copy only the ``Cargo.toml`` files, then synthesise a stub source
for every workspace target before the dependency build, because cargo parses the
manifests at that layer and aborts with ``can't find <target>`` if a declared
``[[bench]]`` / ``[[test]]`` / ``[[bin]]`` / ``[[example]]`` has no source file.

That list is hand-maintained, duplicated byte-for-byte across ``Dockerfile`` and
``Dockerfile.lite``, and — critically — **only exercised on a ``vX.Y.Z`` tag**, because
``docker-build`` lives in ``release.yml`` and no branch CI job builds an image. So a
contributor who adds a target sees six green jobs, merges, and the gap surfaces only
after the tag's ``test`` and ``fuzz`` gates have burned an hour, with no image published
for that release. That has now happened at least twice (the `query_runtime` bench being
the most recent).

The repo already retired this shape once, in ``ci.yml``'s builder-test filter: *"a
deliberate act is one someone can forget"*. This is the same argument applied to the
stub list — a cheap textual invariant that turns a release-day failure into a red build
on the commit that caused it, in the same spirit as ``release.yml``'s
"version tag matches README + Cargo.toml" grep.

What it checks
--------------
Both directions, because both are drift:

* **Forward** (release-breaking): every declared target has a stub line in *both*
  Dockerfiles. A missing stub fails the tag build.
* **Reverse** (silently misleading): every stub under ``benches/`` / ``tests/`` /
  ``examples/`` corresponds to a declared target. The builder layer copies no sources,
  so cargo can never auto-discover one of these — an orphan stub is therefore a target
  that was removed from a manifest without its stub being cleaned up, and it makes the
  pre-tag ``docker build --target builder`` check certify a manifest the tree no longer
  matches. ``src/`` stubs are exempt: ``lib.rs`` / ``main.rs`` are auto-discovered and
  legitimately unlisted in any manifest.

Run it locally before tagging, alongside the ``docker build --target builder`` check:

    python3 .github/scripts/check_dockerfile_stubs.py
"""

from __future__ import annotations

import pathlib
import re
import sys
import tomllib

# Where cargo looks for a target of each kind when the manifest gives no explicit `path`.
DEFAULT_DIR = {
    "bench": "benches",
    "test": "tests",
    "example": "examples",
    "bin": "src/bin",
}
# Stub paths under these directories must correspond to a declared target (see module
# docstring). Anything under `src/` is auto-discovered and exempt.
REVERSE_CHECKED = ("benches/", "tests/", "examples/")

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCKERFILES = ("Dockerfile", "Dockerfile.lite")
# `... > crates/<crate>/<path>` — the stub-writing redirect, however it is quoted.
STUB_RE = re.compile(r">\s*(crates/[A-Za-z0-9_./-]+\.rs)")


def declared_targets() -> dict[str, str]:
    """Map every declared target's source path to a human label naming its manifest."""
    found: dict[str, str] = {}
    for manifest in sorted(ROOT.glob("crates/*/Cargo.toml")):
        crate_dir = manifest.parent.relative_to(ROOT)
        data = tomllib.loads(manifest.read_text())
        for kind, default_dir in DEFAULT_DIR.items():
            for target in data.get(kind, []):
                name = target.get("name")
                if not name:
                    continue
                rel = target.get("path") or f"{default_dir}/{name}.rs"
                found[f"{crate_dir}/{rel}"] = f"[[{kind}]] {name} in {crate_dir}/Cargo.toml"
    return found


def stubs_in(dockerfile: str) -> set[str]:
    return set(STUB_RE.findall((ROOT / dockerfile).read_text()))


def main() -> int:
    declared = declared_targets()
    problems: list[str] = []

    for dockerfile in DOCKERFILES:
        stubs = stubs_in(dockerfile)
        for path, label in sorted(declared.items()):
            if path not in stubs:
                problems.append(
                    f"{dockerfile}: no stub for {path}\n"
                    f"    declared as {label}\n"
                    f"    add:  && echo 'fn main() {{}}' > {path}   (or `echo ''` for a lib/test)"
                )
        for path in sorted(stubs):
            rel = path.split("/", 2)[2] if path.count("/") >= 2 else ""
            if rel.startswith(REVERSE_CHECKED) and path not in declared:
                problems.append(
                    f"{dockerfile}: stub for {path} has no declared target\n"
                    f"    the builder layer copies no sources, so cargo cannot "
                    f"auto-discover it — remove the stub, or restore its manifest entry"
                )

    if problems:
        print("Dockerfile dependency-cache stub lists are out of step:\n", file=sys.stderr)
        for p in problems:
            print(f"  - {p}\n", file=sys.stderr)
        print(
            "These lists are only exercised by `docker build` on a vX.Y.Z tag, which is\n"
            "why this check exists: without it the failure lands on release day, after\n"
            "the test and fuzz gates have already run, with no image published.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: {len(declared)} declared targets stubbed in {', '.join(DOCKERFILES)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
