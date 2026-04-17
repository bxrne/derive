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
const StringPool = storage.StringPool;

const query = @import("query.zig");
const wal = storage;

pub const AddStatementError = Allocator.Error || rdf.StatementBoundaryError || wal.WalError;
pub const OpenError = Allocator.Error || wal.WalError || AddStatementError || Io.File.OpenError;

pub const WalMode = wal.WalMode;
pub const WalBundle = wal.WalBundle;
pub const IndexBacking = @import("index.zig").IndexBacking;

/// Checksum seed written into the WAL header of new journals.
const wal_header_seed: u32 = 0x12345678;

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
    pub fn init(init_process: std.process.Init, mode: WalMode, index_backing: IndexBacking) OpenError!RDFDataset {
        const allocator = init_process.arena.allocator();
        const io = init_process.io;

        var dataset = RDFDataset{
            .engine = .{ .memory = try Core.init(allocator, index_backing) },
            .wal_bundle = null,
        };
        errdefer dataset.deinit();

        const journal_path = switch (mode) {
            .memory => return dataset,
            .journal => |path| path,
        };

        var file = Io.Dir.cwd().openFile(io, journal_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try Io.Dir.cwd().createFile(io, journal_path, .{ .read = true }),
            else => return err,
        };

        const stat = try file.stat(io);
        const seed: u32 = if (stat.size == 0) blk: {
            try wal.writeHeader(io, &file, wal_header_seed);
            break :blk wal_header_seed;
        } else blk: {
            var read_buffer: [4096]u8 = undefined;
            var file_reader = file.reader(io, &read_buffer);
            const header_seed = try wal.readAndVerifyHeader(&file_reader.interface);
            try dataset.replayWalRecords(allocator, &file_reader.interface, header_seed, &file, io);
            break :blk header_seed;
        };

        var crc = std.hash.Crc32.init();
        var seed_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &seed_buf, seed, .little);
        crc.update(&seed_buf);

        dataset.wal_bundle = .{ .io = io, .file = file, .scratch = .empty, .crc = crc };
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
        try wal.replay(AddStatementError, AddStatementError, allocator, reader, seed, file, io, self, addQuad, removeQuad);
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

        const quad: Quad = .{
            .subject = try rdf.internTerm(&c.strings, subject_input),
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
            .iri, .blank_node => |handle| self.resolve(handle),
            .literal => "(literal)",
        };
    }

    pub fn match(self: *const RDFDataset, pattern: Pattern) MatchIterator {
        const c = self.engine.coreConst();
        const bindings = query.bindHandles(&c.strings, pattern) orelse return query.unmatchable(&c.store);
        return query.build(&c.store, &c.index, bindings);
    }

    pub fn removeTriple(self: *RDFDataset, subject: Input, predicate: []const u8, object: Input) AddStatementError!void {
        try self.removeQuad(subject, predicate, object, rdf.default_graph_iri);
    }

    pub fn removeQuad(self: *RDFDataset, subject_input: Input, predicate_iri: []const u8, object_input: Input, graph_name: []const u8) AddStatementError!void {
        const c = self.engine.core();
        const key = resolveSpogKey(&c.strings, subject_input, predicate_iri, object_input, graph_name) orelse return;
        if (!c.store.containsSpogKey(key)) return;
        const quad = c.store.quadCopyForSpogKey(key) orelse return;

        if (self.wal_bundle) |*bundle| {
            try wal.appendQuadRecord(bundle.io, &bundle.file, c.strings.allocator, &bundle.scratch, &c.strings, quad, .remove_quad, &bundle.crc);
        }

        try c.store.removeBySpogKey(c.strings.allocator, key);
        c.index.remove(key[0], key[1], key[2], key[3]);
    }
};

/// Resolve an API-level quad description to its raw SPOG key, or null when
/// any component is absent from the string pool (so the quad cannot exist).
fn resolveSpogKey(
    pool: *const StringPool,
    subject_input: rdf.Input,
    predicate_iri: []const u8,
    object_input: rdf.Input,
    graph_name: []const u8,
) ?[4]u32 {
    const subject = rdf.findTermKey(pool, subject_input) orelse return null;
    const predicate = if (pool.find(predicate_iri)) |h| @intFromEnum(h) else return null;
    const object = rdf.findTermKey(pool, object_input) orelse return null;
    const graph = if (pool.find(graph_name)) |h| @intFromEnum(h) else return null;
    return .{ subject, predicate, object, graph };
}

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
            .minimal = .{ .environ = .empty, .args = .{ .vector = &.{} } },
            .arena = &self.arena,
            .gpa = testing.allocator,
            .io = testing.io,
            .environ_map = &self.env_map,
            .preopens = .empty,
        };
    }

    fn openMemory(self: *TestHarness) !RDFDataset {
        return RDFDataset.init(self.processInit(), .memory, .contiguous);
    }
};

const ex_a = "http://example.org/a";
const ex_b = "http://example.org/b";
const ex_p = "http://example.org/p";
const ex_alice = "http://example.org/alice";
const ex_bob = "http://example.org/bob";
const ex_knows = "http://example.org/knows";

test "add triples and quads" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = ex_alice }, ex_knows, .{ .iri = ex_bob });
    try dataset.addQuad(.{ .iri = ex_alice }, ex_knows, .{ .iri = ex_bob }, "http://example.org/graph");
    var iterator = dataset.iterStatements();
    const first = iterator.next().?;
    try testing.expectEqualStrings(ex_alice, dataset.resolve(first.subject.iri));
    try testing.expectEqualStrings(ex_knows, dataset.resolve(first.predicate));
    try testing.expectEqualStrings(ex_bob, dataset.resolve(first.object.iri));
    try testing.expectEqualStrings(rdf.default_graph_iri, dataset.resolve(first.graph));
}

test "contains without allocating" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b });
    try testing.expect(dataset.contains(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b }));
    try testing.expect(!dataset.contains(.{ .iri = "http://example.org/x" }, ex_p, .{ .iri = ex_b }));
}

test "deduplicates across quads" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = ex_alice }, ex_knows, .{ .iri = ex_bob });
    try dataset.addTriple(.{ .iri = ex_alice }, ex_knows, .{ .iri = ex_bob });
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
    try testing.expect(dataset.contains(.{ .iri = ex_alice }, ex_knows, .{ .iri = ex_bob }));
}

test "remove quad" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try dataset.addTriple(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b });
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
    try dataset.removeTriple(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b });
    try testing.expectEqual(@as(usize, 0), dataset.statementCount());
    try testing.expect(!dataset.contains(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b }));
}

test "rejects literal subject" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try testing.expectError(error.InvalidSubject, dataset.addTriple(.{ .literal = .{ .value = "x" } }, ex_p, .{ .iri = "http://example.org/o" }));
}

test "rejects non-iri predicate" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try testing.expectError(error.InvalidPredicateIri, dataset.addTriple(.{ .iri = ex_a }, "_:b", .{ .iri = ex_b }));
    try testing.expectError(error.InvalidPredicateIri, dataset.addTriple(.{ .iri = ex_a }, "noscheme", .{ .iri = ex_b }));
}

test "accepts blank graph name" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try dataset.addQuad(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b }, "_:g1");
    try testing.expectEqual(@as(usize, 1), dataset.statementCount());
}

test "rejects invalid graph name" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try testing.expectError(error.InvalidGraphName, dataset.addQuad(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b }, "no-scheme"));
}

test "rejects invalid subject or object IRI" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try testing.expectError(error.InvalidSubjectIri, dataset.addTriple(.{ .iri = "bad" }, ex_p, .{ .iri = "http://example.org/o" }));
    try testing.expectError(error.InvalidObjectIri, dataset.addTriple(.{ .iri = "http://example.org/s" }, ex_p, .{ .iri = "bad" }));
}

test "rejects empty blank node label" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    try testing.expectError(error.InvalidBlankNodeLabel, dataset.addTriple(.{ .blank_node = "" }, ex_p, .{ .iri = "http://example.org/o" }));
}

test "rejects invalid literal shape" {
    var harness = TestHarness.init();
    defer harness.deinit();
    var dataset = try harness.openMemory();
    defer dataset.deinit();
    const subject: rdf.Input = .{ .iri = "http://example.org/s" };
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(subject, ex_p, .{ .literal = .{
        .value = "x",
        .lang = "en",
        .datatype = "http://www.w3.org/2001/XMLSchema#string",
    } }));
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(subject, ex_p, .{ .literal = .{
        .value = "x",
        .datatype = "not-an-iri",
    } }));
    try testing.expectError(error.InvalidLiteral, dataset.addTriple(subject, ex_p, .{ .literal = .{
        .value = "x",
        .lang = "",
    } }));
}

test "wal roundtrip replay" {
    const path = "derive_test_wal.bin";
    defer Io.Dir.cwd().deleteFile(testing.io, path) catch {};
    {
        var harness = TestHarness.init();
        defer harness.deinit();
        var dataset = try RDFDataset.init(harness.processInit(), .{ .journal = path }, .contiguous);
        defer dataset.deinit();
        try dataset.addTriple(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b });
        try dataset.commitWal();
    }
    {
        var harness = TestHarness.init();
        defer harness.deinit();
        var dataset = try RDFDataset.init(harness.processInit(), .{ .journal = path }, .contiguous);
        defer dataset.deinit();
        try testing.expectEqual(@as(usize, 1), dataset.statementCount());
        try testing.expect(dataset.contains(.{ .iri = ex_a }, ex_p, .{ .iri = ex_b }));
    }
}
