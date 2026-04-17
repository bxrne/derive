//! Demo: LUBM workload on the contiguous permutation index.
//!
//! Contiguous stores favour scan-heavy reads: each permutation is a single
//! sorted array, so prefix scans hit dense cache lines. Writes cost O(n)
//! per index because the tail has to shift, which shows up most clearly as
//! the load scales up.

const std = @import("std");
const libderive = @import("libderive");
const lubm = @import("lubm");

const scales = [_]u64{ 100_000, 1_000_000 };

pub fn main(init: std.process.Init) !void {
    try lubm.bench(init, .contiguous, &scales);
    try lubm.walRoundtrip(init, .contiguous, "derive-demo.wal");
}
