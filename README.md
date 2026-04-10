# derive

An experimental project, a RDF 1.1 quad store in Zig, with an optional write-ahead log for durability. A SPOG (subject-predicate-object-graph) store with interned strings, duplicate suppression, and indexed matching over basic graph patterns and a WAL for durability and replay on crash recovery.

**derive** ships a **`demo`** binary and **`libderive/`**, a Zig module that stores [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/)-style quads (subject, predicate, object, named graph) with interned strings, duplicate suppression, and indexed matching over basic graph patterns.

Durability is optional: keep everything in memory, or enable an append-only **journal** (write-ahead style log with a versioned header, replay on open), wal enables crash recovery.

## Requirements

- [Zig](https://ziglang.org/) **0.16.0-dev** or newer (the project tracks Zig master; pin your toolchain if you need reproducible builds).

## Quick start

```sh
zig build          # compile and install the demo binary
zig build test     # run all tests
zig build demo     # build and run the demo (journal: derive-demo.wal in cwd)
```

## Documentation

### Requirements

- [Zig](https://ziglang.org/) **0.16.0-dev** or newer (the project tracks Zig master; pin your toolchain if you need reproducible builds).
- [astral-sh/uv](https://github.com/astral-sh/uv) **0.11.0** or newer.

The documentation is generated via the Zig build system and is available as a html site with a WASM search index. To generate the documentation, run:

```sh
zig build doc

cd docs/libderive
uv run python -m http.server 8000
```
