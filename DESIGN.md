# Design document

**Purpose**: This document outlines the iterative design of the `libderive` library and the accompanying `demo`. It captures the rationale behind design decisions, trade-offs considered, and future directions for development.

---

### Design principles

The codebase follows [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md):

- **Safety first.** `std.debug.assert` guards programmer invariants (deduplication preconditions, live-count consistency, pool capacity). User input is validated with explicit error returns at the API boundary, never assertions.
- **No speculative abstraction.** There is one concrete engine implementation (`Core`). The `Engine` tagged union exists as a single seam for future backends but carries no generic machinery or duplicated code.
- **Zero dependencies.** Only the Zig standard library.
- **Concrete naming.** No abbreviations (`subject` not `s`, `predicate` not `p`, `position` not `pos`). Qualifiers are appended, sorted by significance.

---

### Design update: indexed permutation model

The store uses an **indexed permutation model** for RDF quads. Each quad is encoded as four `u32` handles in SPOG order and stored in multiple lexicographically sorted permutations (`spog`, `posg`, `ospg`, `gspo`, `gpos`, `gosp`). Pattern matching chooses the permutation with the longest bound prefix, then performs a contiguous range scan over that prefix. This yields efficient ordered scans for basic graph patterns without needing a join planner.

**Why this choice**
- **Read locality:** prefix scans over a sorted run are cache-friendly and support range queries directly.
- **Match planning:** the longest-prefix heuristic is simple and deterministic, yet captures most practical patterns.
- **Flexibility:** alternative backings can provide the same ordered scan interface with different write costs.

**Trade-offs**
- **Write amplification:** each insert updates six permutations; contiguous stores pay shift costs.
- **Memory overhead:** storing multiple orderings increases index footprint.
- **Backings matter:** write-heavy workloads benefit from tree-backed permutations; scan-heavy workloads benefit from contiguous storage.

---

## Proof of concept

The proof of concept stores RDF 1.1 style quads (subject, predicate, object, named graph) in memory with matching over basic graph patterns. The implementation combines a deduplicated quad store, a string pool for interned text, six lexicographically sorted key runs (one per index permutation), optional single-pattern matching over those indexes, and an append-only journal for durability experiments. It is not a full SPARQL engine, optimizer, or distributed store.

### API design

The exported surface is deliberately small: one `RDFDataset` type, a memory versus journal open mode, pattern and input types for add and match, WAL helpers, and RDF value types. Internal modules stay private so refactors do not break callers. Opening a dataset takes process context (allocator, I/O, environment) so the same entry points work in unit tests, small demos, and embedding scenarios. `Input` and `Pattern` carry API strings and optional components; resolving them to interned handles happens inside `addQuad` and `match`, and invalid shapes (for example literal as subject, bad IRI rules, or ill-formed literals) fail with explicit errors at that boundary.

### Data modelling

RDF terms are represented with a string pool: IRIs, blank node labels, and literal payloads are stored once and referenced by small handles. A quad is a subject term, predicate handle, object term, and graph handle. The quad store holds a growable array of optional quads (tombstones mark deleted slots), a free list to reuse slots, a hash map from full SPOG keys (four `u32` components) to slot indices for deduplication, and a live count. That layout makes "same quad twice" a no-op at the store and keeps stable slot indices for iterators, at the cost of updating every auxiliary structure when strings or membership change. Deletes tombstone slots rather than compacting the array, so iterators that walk slot order can stay simple. The public `contains` API tests triple equality (subject, predicate, object) and ignores graph: the backing index exposes `containsTriple` by scanning the SPOG permutation for a three-component prefix, so any graph with that triple satisfies the check.

### Storage engine

The storage layer (`storage/mod.zig`) owns `Core`, which bundles a `StringPool`, `QuadStore`, and `Index`. The `Engine` tagged union wraps `Core` with a `.memory` variant today; a future file-backed variant can be added by extending the union without changing the `RDFDataset` API. `Engine.core()` and `Engine.coreConst()` provide uniform mutable and immutable access to the underlying state.

### Indexing structure

The index is six independent permutation stores, one per ordering of `(g, s, p, o)` encoded as four `u32` components in a fixed order for that permutation: `spog`, `posg`, `ospg`, `gspo`, `gpos`, and `gosp`. The backing is selected at init time (`IndexBacking`) and determines how each permutation stores its ordered keys.

- **ContiguousStore:** keys are kept in a single lexicographically sorted list. Inserts binary-search for position, skip duplicates, and shift tail elements. This is cache-friendly for scans but write cost is linear in the list length.
- **TreeStore:** keys are stored in a treap with lexicographic ordering. Inserts and deletes are logarithmic in size, and scans iterate in-order from a lower bound. This reduces write amplification at the cost of pointer-chasing during scans.

Both backings expose the same ordered-scan interface (`KeyScan`) so match planning and iterators do not depend on the physical layout. Adding or removing one quad updates all six permutations so total write cost still scales with six index updates.

### Matching and iterators

`match` first binds pattern components to pool handles. If any bound term is not present in the pool (for example a concrete IRI in the pattern that was never inserted), binding fails and the implementation returns an iterator positioned at the end of the slot array, which yields nothing. If every component is unbound, the iterator walks live quads in physical slot order and returns each one (full scan of live statements).

If at least one component is bound, `chooseScanPlan` scores all six permutations by how many leading components are fixed when the pattern's bound values are read in that permutation's order. It keeps the permutation with the largest such prefix length. Ties break in a fixed order: `gspo`, then `gpos`, `gosp`, `spog`, `posg`, `ospg`. The chosen permutation's store is prefix-scanned with that many `u32` components, producing a slice of full four-component keys in that permutation's encoding.

The match iterator then walks those keys. For each key it decodes from the active permutation back to canonical SPOG order, looks up the quad by SPOG key in the quad store, and applies a final filter so every bound component in the original pattern matches the quad (redundant if the prefix already fixed every component, but necessary when the prefix is shorter than four). This two-stage design lets partial patterns use index ranges where possible while still enforcing the full pattern.

This is single basic graph pattern matching only: no joins, no optional, no property paths, and no reordering planner beyond the prefix-length heuristic.

### WAL layout and replay

The journal file begins with a nine-byte header: four ASCII bytes `DERW` (magic), one byte format version (currently `0`), and four bytes for a little-endian `u32` checksum seed. If the file is created empty, that header is written first. Every subsequent append is a record. Records are read sequentially after the header until an end-of-stream read returns zero bytes.

Each record has a fixed structure, protecting against partial or corrupted writes using a cumulative CRC32 checksum:

1. **One byte** `kind`: `0` commit, `0x21` add quad, `0x23` remove quad.
2. **Four bytes** unsigned little-endian `plen`, the payload length in bytes.
3. **`plen` bytes** of payload (may be zero).
4. **Four bytes** unsigned little-endian `checksum`, the running CRC32 hash of the file up to and including the payload.

A commit record has `plen = 0` and no payload; it exists so `commitWal` can force a durability point after a sequence of data records.

Add and remove payloads share the same encoding: a self-contained quad described in UTF-8, not raw handles. Order is: encoded subject term, length-prefixed predicate string, encoded object term, length-prefixed graph string. Term encoding is: one tag byte (`0` IRI, `1` blank node, `2` literal), then for IRI and blank a `u32` length and UTF-8 bytes; for literal, value string, then a flag byte and optional datatype string, then a flag byte and optional language tag string. Lengths are always `u32` little-endian before the corresponding UTF-8 run.

The append path for quad records builds the payload in a scratch buffer. It then writes the `kind` byte at the current end-of-file, then the four length bytes, and then the payload bytes using positional writes. As these bytes are written, a running `Crc32` context is updated. Finally, the four-byte running checksum is appended.

Opening an existing journal opens or creates the file and reads the header to initialize the checksum seed. It then loops: read one byte `kind`, read `plen`, if add or remove allocate and read exactly `plen` bytes. The replay loop continuously hashes these bytes. It then reads the 4-byte expected checksum from the file and compares it to the running hash. If they match, it decodes to `Input` values and calls `addQuad` or `removeQuad` so replay goes through the same validation and indexing as live updates. 

`syncWal` calls the platform file sync; `commitWal` appends a commit record and then syncs. In the event of a crash in the middle of writing a record, the file will either end prematurely or the checksum validation will fail during replay. If this happens, replay stops cleanly and the file is truncated back to the last valid record boundary, skipping the corrupted tail without losing any valid data.
