//! Demo: parts-compatibility graph with WAL persistence (contiguous index).

const std = @import("std");
const libderive = @import("libderive");

const ns = "urn:derive:demo:";
const graph = ns ++ "graph:inventory";
const pred_compat = ns ++ "compatibleWith";
const pred_label = ns ++ "label";
const sedan = ns ++ "vehicle:sedan";
const truck = ns ++ "vehicle:truck";
const part_brake = ns ++ "part:brake-pad";
const part_tow = ns ++ "part:tow-hitch";
const part_filter = ns ++ "part:air-filter";

fn logPartsForVehicle(ds: *libderive.RDFDataset, arena: std.mem.Allocator, title: []const u8, vehicle_iri: []const u8) !void {
    var it = ds.match(.{ .p = pred_compat, .o = .{ .iri = vehicle_iri }, .g = graph });
    var names = std.ArrayList([]const u8).empty;
    while (it.next()) |q| {
        try names.append(arena, ds.subjectLabel(q));
    }
    const line = if (names.items.len == 0)
        "(none)"
    else
        try std.mem.join(arena, ", ", names.items);
    std.log.info("{s}: {s}", .{ title, line });
}

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const wal_path = "derive-demo.wal";

    var ds = try libderive.RDFDataset.initWithBacking(init, .{ .journal = wal_path }, .contiguous);
    defer ds.deinit();

    if (ds.statementCount() == 0) {
        std.log.info("empty store — inserting demo quads (parts ↔ vehicles)", .{});
        try ds.addQuad(.{ .iri = part_brake }, pred_compat, .{ .iri = sedan }, graph);
        try ds.addQuad(.{ .iri = part_brake }, pred_compat, .{ .iri = truck }, graph);
        try ds.addQuad(.{ .iri = part_tow }, pred_compat, .{ .iri = truck }, graph);
        try ds.addQuad(.{ .iri = part_filter }, pred_compat, .{ .iri = sedan }, graph);
        try ds.addQuad(.{ .blank_node = "b1" }, pred_label, .{ .literal = .{ .value = "brake pad" } }, graph);
        try ds.commitWal();
    }

    try logPartsForVehicle(&ds, arena, "Parts compatible with sedan", sedan);
    try logPartsForVehicle(&ds, arena, "Parts compatible with truck", truck);

    std.log.info("Total statements: {d}  |  journal: {s}", .{ ds.statementCount(), wal_path });
}
