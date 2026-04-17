# derive

An experimental project, a RDF 1.1 quad store in Zig, with an optional write-ahead log for durability. A SPOG (subject-predicate-object-graph) store with interned strings, duplicate suppression, and indexed matching over basic graph patterns and a WAL for durability and replay on crash recovery.

**derive** ships demo binaries and **`libderive/`**, a Zig module that stores [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/)-style quads (subject, predicate, object, named graph) with interned strings, duplicate suppression, and indexed matching over basic graph patterns. The index uses an **indexed permutation model**: the same SPOG quad keys are stored in multiple lexicographically sorted orders so pattern scans can pick the permutation with the longest bound prefix and perform efficient range scans. Choose a contiguous or treap-backed permutation store at init time depending on write-heavy or scan-heavy workloads.

Durability is optional: keep everything in memory, or enable an append-only **journal** (write-ahead style log with a versioned header, replay on open), wal enables crash recovery.

## Requirements

- [Zig](https://ziglang.org/) **0.16.0-dev** or newer (the project tracks Zig master; pin your toolchain if you need reproducible builds).

## Quick start

```sh
zig build                    # compile and install demo binaries (use -Doptimize=ReleaseFast for realistic demo timings)
zig build test               # run all tests
zig build demo-contiguous    # LUBM benchmark on the contiguous index; untimed WAL roundtrip at derive-demo.wal
zig build demo-tree          # LUBM benchmark on the treap index; untimed WAL roundtrip at derive-tree-demo.wal
```

Each demo loads a LUBM-shaped dataset (universities, departments, faculty, students, courses, publications) at 100 000 and 1 000 000 quads, runs the canonical basic-graph-pattern query suite, and prints per-query timings as `k=v` log lines. After the timed section the demo performs a separate, untimed WAL write/replay roundtrip to exercise durability.

## Documentation

### Requirements

- [Zig](https://ziglang.org/) **0.16.0-dev** or newer (the project tracks Zig master; pin your toolchain if you need reproducible builds).
- [astral-sh/uv](https://github.com/astral-sh/uv) **0.11.0** or newer.

The documentation is generated via the Zig build system and is available as a html site with a WASM search index. To generate the documentation, run:

```sh
zig build docs

cd docs/libderive
uv run python -m http.server 8000
```
