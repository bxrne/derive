//! Demo: LUBM workload on the treap permutation index.
//!
//! Treap-backed stores absorb writes in O(log n) per permutation, which
//! matters once the dataset is large enough that the contiguous memmove
//! cost dominates. Prefix scans pay for pointer-chasing but still hand
//! back keys in lex order.

const std = @import("std");
const libderive = @import("libderive");
const lubm = @import("lubm");

const scales = [_]u64{ 100_000, 1_000_000 };

pub fn main(init: std.process.Init) !void {
    try lubm.bench(init, .tree, &scales);
    try lubm.walRoundtrip(init, .tree, "derive-tree-demo.wal");
}
