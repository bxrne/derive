//! Demo: bulk insert and scan with treap-backed index.

const std = @import("std");
const libderive = @import("libderive");

const ns = "urn:derive:bulk:";
const graph = ns ++ "graph";
const predicate = ns ++ "p";

const QuadTarget = 1_000_000;

pub fn main(init: std.process.Init) !void {
    const wal_path = "derive-tree-demo.wal";

    var ds = try libderive.RDFDataset.init(init, .{ .journal = wal_path }, .tree);
    defer ds.deinit();

    if (ds.statementCount() == 0) {
        std.log.info("empty store — inserting {d} quads", .{QuadTarget});
        var subject_buffer: [64]u8 = undefined;
        var object_buffer: [64]u8 = undefined;
        var i: u32 = 0;
        while (i < QuadTarget) : (i += 1) {
            const subject = try std.fmt.bufPrint(&subject_buffer, "{s}s:{d}", .{ ns, i });
            const object = try std.fmt.bufPrint(&object_buffer, "{s}o:{d}", .{ ns, i % 10_000 });
            try ds.addQuad(.{ .iri = subject }, predicate, .{ .iri = object }, graph);
        }
        try ds.commitWal();
    }

    var it = ds.match(.{ .p = predicate, .g = graph });
    var count: usize = 0;
    while (it.next()) |_| {
        count += 1;
    }
    std.log.info("Statements: {d}  |  journal: {s}", .{ count, wal_path });
}
