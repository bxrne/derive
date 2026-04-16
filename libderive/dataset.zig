//! RDFDataset: the public dataset type with optional WAL durability.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const rdf = @import("rdf.zig");
const Quad = rdf.Quad;
const Handle = rdf.Handle;

const storage = @import("storage/mod.zig");
const Engine = storage.Engine;
const Core = storage.Core;
const spogKeyFromQuad = storage.spogKeyFromQuad;
const LiveQuadIterator = storage.LiveQuadIterator;

const query = @import("query.zig");
const wal = storage;

pub const AddStatementError = Allocator.Error || rdf.StatementBoundaryError || wal.WalError;

pub const OpenError = Allocator.Error || wal.WalError || AddStatementError || Io.File.OpenError;

pub const WalMode = wal.WalMode;
pub const WalBundle = wal.WalBundle;
pub const IndexBacking = @import("index.zig").IndexBacking;

pub const RDFDataset = struct {
    engine: Engine,
    wal_bundle: ?WalBundle = null,

    pub const Input = rdf.Input;
    pub const LiteralInput = rdf.LiteralInput;
    pub const StatementBoundaryError = rdf.StatementBoundaryError;
    pub const Pattern = rdf.Pattern;
    pub const MatchIterator = query.Iterator;

    /// Open a dataset. `.memory` — in RAM only. `.journal` — open/create
    /// path, replay existing records, then append new operations.
    pub fn init(init_process: std.process.Init, mode: WalMode) OpenError!RDFDataset {
        return initWithBacking(init_process, mode, .contiguous);
    }

    /// Open a dataset with an explicit index backing.
    pub fn initWithBacking(init_process: std.process.Init, mode: WalMode, index_backing: IndexBacking) OpenError!RDFDataset {
        const allocator = init_process.arena.allocator();
        const io = init_process.io;

        var dataset = RDFDataset{
            .engine = .{ .memory = try Core.init(allocator, index_backing) },
            .wal_bundle = null,
        };
        errdefer dataset.deinit();

        switch (mode) {
            .memory => {},
            .journal => |wal_path| {
                var file = Io.Dir.cwd().openFile(io, wal_path, .{ .mode = .read_write }) catch |err| switch (err) {
                    error.FileNotFound => try Io.Dir.cwd().createFile(io, wal_path, .{ .read = true }),
                    else => return err,
                };

                var seed: u32 = 0x12345678; // Hardcoded or random seed
                const stat = try file.stat(io);
                if (stat.size == 0) {
                    try wal.writeHeader(io, &file, seed);
                } else {
                    var read_buffer: [4096]u8 = undefined;
                    var file_reader = file.reader(io, &read_buffer);
                    seed = try wal.readAndVerifyHeader(&file_reader.interface);
                    try dataset.replayWalRecords(allocator, &file_reader.interface, seed, &file, io);
                }

                var crc = std.hash.Crc32.init();
                // We re-seed the checksum to match the one on disk.
                // However, Crc32 doesn't let us seed cleanly, so we just hash the seed initially.
                var seed_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &seed_buf, seed, .little);
                crc.update(&seed_buf);

                dataset.wal_bundle = .{
                    .io = io,
                    .file = file,
                    .scratch = .empty,
                    .crc = crc,
                };
            },
        }
        return dataset;
    }

    pub fn deinit(self: *RDFDataset) void {
        if (self.wal_bundle) |*bundle| {
            bundle.scratch.deinit(self.engine.core().strings.allocator);
            bundle.file.close(bundle.io);
        }
        self.engine.deinit();
    }

    /// Flush WAL metadata to disk.
    pub fn syncWal(self: *RDFDataset) wal.WalError!void {
        if (self.wal_bundle) |*bundle| try wal.syncFile(bundle.io, &bundle.file);
    }

    /// Append a commit marker and sync.
    pub fn commitWal(self: *RDFDataset) wal.WalError!void {
        if (self.wal_bundle) |*bundle| {
            try wal.appendCommit(bundle.io, &bundle.file, &bundle.crc);
            try wal.syncFile(bundle.io, &bundle.file);
        }
    }

    fn replayWalRecords(self: *RDFDataset, allocator: Allocator, reader: *Io.Reader, seed: u32, file: *Io.File, io: Io) OpenError!void {
        try wal.replay(
            AddStatementError,
            AddStatementError,
            allocator,
            reader,
            seed,
            file,
            io,
            self,
            replayAddQuad,
            replayRemoveQuad,
        );
    }

    fn replayAddQuad(self: *RDFDataset, subject: Input, predicate: []const u8, object: Input, graph: []const u8) AddStatementError!void {
        try self.addQuad(subject, predicate, object, graph);
    }

    fn replayRemoveQuad(self: *RDFDataset, subject: Input, predicate: []const u8, object: Input, graph: []const u8) AddStatementError!void {
        try self.removeQuad(subject, predicate, object, graph);
    }

    pub fn statementCount(self: *const RDFDataset) usize {
        return self.engine.coreConst().store.len();
    }

    /// Iterate live quads in slot order, skipping tombstones.
    pub fn iterStatements(self: *const RDFDataset) LiveQuadIterator {
        return self.engine.coreConst().store.liveIterator();
    }

    pub fn addTriple(self: *RDFDataset, subject: Input, predicate: []const u8, object: Input) AddStatementError!void {
        try self.addQuad(subject, predicate, object, rdf.default_graph_iri);
    }

    pub fn addQuad(self: *RDFDataset, subject_input: Input, predicate_iri: []const u8, object_input: Input, graph_name: []const u8) AddStatementError!void {
        const c = self.engine.core();

        try rdf.validateSubjectInput(subject_input);
        if (!rdf.isValidPredicateIri(predicate_iri)) return error.InvalidPredicateIri;
        try rdf.validateObjectInput(object_input);
        if (!rdf.isValidNamedGraphName(graph_name)) return error.InvalidGraphName;

        const subject = try rdf.internTerm(&c.strings, subject_input);
        const quad: Quad = .{
            .subject = subject,
            .predicate = try c.strings.intern(predicate_iri),
            .object = try rdf.internTerm(&c.strings, object_input),
            .graph = try c.strings.intern(graph_name),
        };

        const key = spogKeyFromQuad(quad);
        if (c.store.containsSpogKey(key)) return;

        if (self.wal_bundle) |*bundle| {
            try wal.appendQuadRecord(bundle.io, &bundle.file, c.strings.allocator, &bundle.scratch, &c.strings, quad, .add_quad, &bundle.crc);
        }

        try c.index.add(key[0], key[1], key[2], key[3]);
        errdefer c.index.remove(key[0], key[1], key[2], key[3]);
        try c.store.appendUnique(c.strings.allocator, quad, key);
    }

    pub fn contains(self: *const RDFDataset, subject_input: Input, predicate_iri: []const u8, object_input: Input) bool {
        const c = self.engine.coreConst();
        const subject_key = rdf.findTermKey(&c.strings, subject_input) orelse return false;
        const predicate_handle = c.strings.find(predicate_iri) orelse return false;
        const object_key = rdf.findTermKey(&c.strings, object_input) orelse return false;
        return c.index.containsTriple(subject_key, @intFromEnum(predicate_handle), object_key);
    }

    pub fn resolve(self: *const RDFDataset, handle: Handle) []const u8 {
        return self.engine.coreConst().strings.get(handle);
    }

    /// Resolved subject string for logging or display.
    pub fn subjectLabel(self: *const RDFDataset, quad: Quad) []const u8 {
        return switch (quad.subject) {
            .iri => |handle| self.resolve(handle),
            .blank_node => |handle| self.resolve(handle),
            .literal => "(literal)",
        };
    }

    pub fn match(self: *const RDFDataset, pattern: Pattern) MatchIterator {
        const c = self.engine.coreConst();
        const bound = query.bindHandles(&c.strings, pattern) orelse return query.unmatchable(&c.store);
        return query.build(&c.store, &c.index, bound);
    }

    pub fn removeTriple(self: *RDFDataset, subject: Input, predicate: []const u8, object: Input) AddStatementError!void {
        try self.removeQuad(subject, predicate, object, rdf.default_graph_iri);
    }

    pub fn removeQuad(self: *RDFDataset, subject_input: Input, predicate_iri: []const u8, object_input: Input, graph_name: []const u8) AddStatementError!void {
        const c = self.engine.core();
        const subject_key = rdf.findTermKey(&c.strings, subject_input) orelse return;
        const predicate_key = if (c.strings.find(predicate_iri)) |handle| @intFromEnum(handle) else return;
        const object_key = rdf.findTermKey(&c.strings, object_input) orelse return;
        const graph_key = if (c.strings.find(graph_name)) |handle| @intFromEnum(handle) else return;
        const key: [4]u32 = .{ subject_key, predicate_key, object_key, graph_key };
        if (!c.store.containsSpogKey(key)) return;
        const quad = c.store.quadCopyForSpogKey(key) orelse return;

        if (self.wal_bundle) |*bundle| {
            try wal.appendQuadRecord(bundle.io, &bundle.file, c.strings.allocator, &bundle.scratch, &c.strings, quad, .remove_quad, &bundle.crc);
        }

        try c.store.removeBySpogKey(c.strings.allocator, key);
        c.index.remove(subject_key, predicate_key, object_key, graph_key);
    }
};

const testing = std.testing;

/// Minimal `std.process.Init` for unit tests.
const TestHarness = struct {
    arena: std.heap.ArenaAllocator,
    env_map: std.process.Environ.Map,

    fn init() TestHarness {
        return .{
            .arena = std.heap.ArenaAllocator.init(testing.allocator),
            .env_map = std.process.Environ.Map.init(testing.allocator),
        };
    }

    fn deinit(self: *TestHarness) void {
        self.env_map.deinit();
        self.arena.deinit();
    }

    fn processInit(self: *TestHarness) std.process.Init {
        return .{
            .minimal = .{
                .environ = .empty,
                .args = .{ .vector = &.{} },
            },
            .arena = &self.arena,
            .gpa = testing.allocator,
            .io = testing.io,
            .environ_map = &self.env_map,
            .preopens = .empty,
        };
    }
};

test "add triples and quads" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    const alice = "http://example.org/alice";
    const knows = "http://example.org/knows";
    const bob = "http://example.org/bob";
    try dataset.addTriple(.{ .iri = alice }, knows, .{ .iri = bob });
    try dataset.addQuad(.{ .iri = alice }, knows, .{ .iri = bob }, "http://example.org/graph");
    var iterator = dataset.iterStatements();
    const first = iterator.next().?;
    try testing.expectEqualStrings(alice, dataset.resolve(first.subject.iri));
    try testing.expectEqualStrings(knows, dataset.resolve(first.predicate));
    try testing.expectEqualStrings(bob, dataset.resolve(first.object.iri));
    try testing.expectEqualStrings(rdf.default_graph_iri, dataset.resolve(first.graph));
}

test "contains without allocating" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" });
    try testing.expect(dataset.contains(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" }));
    try testing.expect(!dataset.contains(.{ .iri = "http://example.org/x" }, "http://example.org/p", .{ .iri = "http://example.org/b" }));
}

test "deduplicates across quads" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = "http://example.org/alice" }, "http://example.org/knows", .{ .iri = "http://example.org/bob" });
    try dataset.addTriple(.{ .iri = "http://example.org/alice" }, "http://example.org/knows", .{ .iri = "http://example.org/bob" });
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
    try testing.expect(dataset.contains(.{ .iri = "http://example.org/alice" }, "http://example.org/knows", .{ .iri = "http://example.org/bob" }));
}

test "remove quad" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" });
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
    try dataset.removeTriple(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" });
    try testing.expectEqual(@as(usize, 0), dataset.statementCount());
    try testing.expect(!dataset.contains(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" }));
}

test "rejects literal subject" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidSubject, dataset.addTriple(.{ .literal = .{ .value = "x" } }, "http://example.org/p", .{ .iri = "http://example.org/o" }));
}

test "rejects non-iri predicate" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidPredicateIri, dataset.addTriple(.{ .iri = "http://example.org/a" }, "_:b", .{ .iri = "http://example.org/b" }));
    try testing.expectError(error.InvalidPredicateIri, dataset.addTriple(.{ .iri = "http://example.org/a" }, "noscheme", .{ .iri = "http://example.org/b" }));
}

test "accepts blank graph name" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try dataset.addQuad(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" }, "_:g1");
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
}

test "rejects invalid graph name" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidGraphName, dataset.addQuad(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" }, "no-scheme"));
}

test "rejects invalid subject or object IRI" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidSubjectIri, dataset.addTriple(.{ .iri = "bad" }, "http://example.org/p", .{ .iri = "http://example.org/o" }));
    try testing.expectError(error.InvalidObjectIri, dataset.addTriple(.{ .iri = "http://example.org/s" }, "http://example.org/p", .{ .iri = "bad" }));
}

test "rejects empty blank node label" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidBlankNodeLabel, dataset.addTriple(.{ .blank_node = "" }, "http://example.org/p", .{ .iri = "http://example.org/o" }));
}

test "rejects invalid literal shape" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try RDFDataset.init(harness.processInit(), .memory);
    defer dataset.deinit();
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(.{ .iri = "http://example.org/s" }, "http://example.org/p", .{
        .value = "x",
        .lang = "en",
        .datatype = "http://www.w3.org/2001/XMLSchema#string",
    }));
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(.{ .iri = "http://example.org/s" }, "http://example.org/p", .{
        .value = "x",
        .datatype = "not-an-iri",
    }));
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(.{ .iri = "http://example.org/s" }, "http://example.org/p", .{
        .value = "x",
        .lang = "",
    }));
}

test "wal roundtrip replay" {
    const path = "derive_test_wal.bin";
    defer Io.Dir.cwd().deleteFile(testing.io, path) catch {};
    {
        var harness = TestHarness.init();
        defer harness.deinit();
        var dataset = try RDFDataset.init(harness.processInit(), .{ .journal = path });
        defer dataset.deinit();
        try dataset.addTriple(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" });
        try dataset.commitWal();
    }
    {
        var harness = TestHarness.init();
        defer harness.deinit();
        var dataset = try RDFDataset.init(harness.processInit(), .{ .journal = path });
        defer dataset.deinit();
        try testing.expectEqual(@as(usize, 1), dataset.statementCount());
        try testing.expect(dataset.contains(.{ .iri = "http://example.org/a" }, "http://example.org/p", .{ .iri = "http://example.org/b" }));
    }
}
