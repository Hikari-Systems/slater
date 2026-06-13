#!/usr/bin/env python3
"""Run a benchmark script and append this process' RSS to memory.txt."""
import contextlib
import io
import os
import resource
import runpy
import subprocess
import sys


def current_rss_bytes(fallback):
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(os.getpid())], text=True)
        return int(out.strip()) * 1024
    except Exception:
        return fallback


def peak_rss_bytes():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux reports KiB.
    return rss if rss > 10 * 1024 * 1024 else rss * 1024


def main():
    if len(sys.argv) < 4:
        raise SystemExit("usage: bench_with_rss.py <bench_script> <engine> <memory_txt>")
    bench_script, engine, memory_txt = sys.argv[1], sys.argv[2], sys.argv[3]
    sys.argv = [bench_script, engine]
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        runpy.run_path(bench_script, run_name="__main__")
    output = buf.getvalue()
    sys.stdout.write(output)
    os.makedirs(os.path.dirname(memory_txt), exist_ok=True)
    with open(memory_txt, "a", encoding="utf-8") as f:
        peak = peak_rss_bytes()
        f.write(f"{engine} peak={peak} current={current_rss_bytes(peak)}\n")


if __name__ == "__main__":
    main()
