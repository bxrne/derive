//! Write-ahead log: append-only binary journal. Records are self-contained UTF-8 strings for replay.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const File = Io.File;
const rdf = @import("../rdf.zig");
const Quad = rdf.Quad;
const Term = rdf.Term;
const StringPool = @import("string_pool.zig").StringPool;

pub const magic: *const [4]u8 = "DERW";
pub const format_version: u8 = 0;

pub const RecordKind = enum(u8) {
    commit = 0,
    add_quad = 0x21,
    remove_quad = 0x23,
};

/// How to open the dataset: pure in-memory, or durable append-only journal.
pub const WalMode = union(enum) {
    memory,
    journal: []const u8,
};

/// Open WAL file handle and encode scratch buffer when journaling is active.
pub const WalBundle = struct {
    io: Io,
    file: Io.File,
    scratch: std.ArrayList(u8),
    crc: std.hash.Crc32,
};

pub const WalError = error{
    InvalidMagic,
    UnsupportedVersion,
    TruncatedRecord,
    BadPayload,
} || File.WritePositionalError || File.SyncError || File.LengthError || File.StatError || Allocator.Error;

/// Read exactly `buf.len` bytes from the reader.
/// Returns `error.TruncatedRecord` on EOF or failure.
fn readSliceAllWalBuf(r: *Io.Reader, buf: []u8) WalError!void {
    Io.Reader.readSliceAll(r, buf) catch |err| switch (err) {
        error.EndOfStream => return error.TruncatedRecord,
        error.ReadFailed => return error.TruncatedRecord,
    };
}

/// Write a little-endian 32-bit integer to the buffer.
fn writeU32Le(gpa: Allocator, buf: *std.ArrayList(u8), v: u32) Allocator.Error!void {
    var b: [4]u8 = undefined;
    std.mem.writeInt(u32, &b, v, .little);
    try buf.appendSlice(gpa, b[0..]);
}

/// Read a little-endian 32-bit integer from the reader.
pub fn readU32Le(r: *Io.Reader) WalError!u32 {
    var b: [4]u8 = undefined;
    try readSliceAllWalBuf(r, &b);
    return std.mem.readInt(u32, &b, .little);
}

/// Write a string prefixed by its length (as a 32-bit little-endian integer).
fn writeStr(gpa: Allocator, buf: *std.ArrayList(u8), s: []const u8) Allocator.Error!void {
    if (s.len > std.math.maxInt(u32)) return error.OutOfMemory;
    try writeU32Le(gpa, buf, @intCast(s.len));
    try buf.appendSlice(gpa, s);
}

/// Read a string prefixed by its length (as a 32-bit little-endian integer).
/// The caller owns the returned slice and must free it with `allocator`.
pub fn readStr(allocator: Allocator, r: *Io.Reader) (Allocator.Error || WalError)![]const u8 {
    const len = try readU32Le(r);
    if (len == 0) return "";
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try readSliceAllWalBuf(r, out);
    return out;
}

/// Encode a term into the buffer.
/// `iri` is 0, `blank_node` is 1, `literal` is 2.
fn encodeTerm(gpa: Allocator, buf: *std.ArrayList(u8), pool: *const StringPool, term: Term) Allocator.Error!void {
    switch (term) {
        .iri => |h| {
            try buf.append(gpa, 0);
            try writeStr(gpa, buf, pool.get(h));
        },
        .blank_node => |h| {
            try buf.append(gpa, 1);
            try writeStr(gpa, buf, pool.get(h));
        },
        .literal => |lit| {
            try buf.append(gpa, 2);
            try writeStr(gpa, buf, pool.get(lit.value));
            if (lit.datatype) |dt| {
                try buf.append(gpa, 1);
                try writeStr(gpa, buf, pool.get(dt));
            } else {
                try buf.append(gpa, 0);
            }
            if (lit.lang) |l| {
                try buf.append(gpa, 1);
                try writeStr(gpa, buf, pool.get(l));
            } else {
                try buf.append(gpa, 0);
            }
        },
    }
}

/// Encode a quad into the buffer (subject, predicate, object, graph).
fn encodeQuadPayload(gpa: Allocator, buf: *std.ArrayList(u8), pool: *const StringPool, quad: Quad) Allocator.Error!void {
    try encodeTerm(gpa, buf, pool, quad.subject);
    try writeStr(gpa, buf, pool.get(quad.predicate));
    try encodeTerm(gpa, buf, pool, quad.object);
    try writeStr(gpa, buf, pool.get(quad.graph));
}

/// Append a framed record: `[kind][u32 LE length][payload][u32 LE crc]`.
/// The running CRC32 covers kind, length, and payload but excludes the trailing
/// checksum bytes themselves.
fn appendRecord(io: Io, file: *File, kind: RecordKind, payload: []const u8, crc: *std.hash.Crc32) WalError!void {
    if (payload.len > std.math.maxInt(u32)) return error.OutOfMemory;

    var header: [5]u8 = undefined;
    header[0] = @intFromEnum(kind);
    std.mem.writeInt(u32, header[1..5], @intCast(payload.len), .little);

    var offset = try file.length(io);
    try file.writePositionalAll(io, &header, offset);
    crc.update(&header);
    offset += header.len;

    if (payload.len > 0) {
        try file.writePositionalAll(io, payload, offset);
        crc.update(payload);
        offset += payload.len;
    }

    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, crc.final(), .little);
    try file.writePositionalAll(io, &crc_buf, offset);
}

/// Append a quad record (add or remove) to the WAL.
pub fn appendQuadRecord(
    io: Io,
    file: *File,
    gpa: Allocator,
    scratch: *std.ArrayList(u8),
    pool: *const StringPool,
    quad: Quad,
    kind: RecordKind,
    crc: *std.hash.Crc32,
) WalError!void {
    scratch.clearRetainingCapacity();
    try encodeQuadPayload(gpa, scratch, pool, quad);
    try appendRecord(io, file, kind, scratch.items, crc);
}

/// Append a commit record (empty payload) to the WAL.
pub fn appendCommit(io: Io, file: *File, crc: *std.hash.Crc32) WalError!void {
    try appendRecord(io, file, .commit, &.{}, crc);
}

/// Sync the file to disk.
pub fn syncFile(io: Io, file: *File) WalError!void {
    try file.sync(io);
}

/// Write the WAL header (magic and format version).
pub fn writeHeader(io: Io, file: *File, seed: u32) WalError!void {
    try file.writePositionalAll(io, magic, 0);
    try file.writePositionalAll(io, &.{format_version}, 4);
    var seed_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &seed_buf, seed, .little);
    try file.writePositionalAll(io, &seed_buf, 5);
}

/// Read and verify the WAL header. Returns the checksum seed.
pub fn readAndVerifyHeader(r: *Io.Reader) WalError!u32 {
    var m: [4]u8 = undefined;
    readSliceAllWalBuf(r, &m) catch return error.InvalidMagic;
    if (!std.mem.eql(u8, &m, magic)) return error.InvalidMagic;
    var ver: [1]u8 = undefined;
    try readSliceAllWalBuf(r, &ver);
    if (ver[0] != format_version) return error.UnsupportedVersion;
    return try readU32Le(r);
}

/// Read a term input from the reader.
pub fn readTermInput(allocator: Allocator, r: *Io.Reader) (Allocator.Error || WalError)!rdf.Input {
    var tag_buf: [1]u8 = undefined;
    try readSliceAllWalBuf(r, &tag_buf);
    return switch (tag_buf[0]) {
        0 => .{ .iri = try readStr(allocator, r) },
        1 => .{ .blank_node = try readStr(allocator, r) },
        2 => blk: {
            const val = try readStr(allocator, r);
            errdefer allocator.free(val);
            var dt_flag: [1]u8 = undefined;
            try readSliceAllWalBuf(r, &dt_flag);
            var dt: ?[]const u8 = null;
            if (dt_flag[0] != 0) {
                dt = try readStr(allocator, r);
            }
            var lang_flag: [1]u8 = undefined;
            try readSliceAllWalBuf(r, &lang_flag);
            var lang: ?[]const u8 = null;
            if (lang_flag[0] != 0) {
                lang = try readStr(allocator, r);
            }
            break :blk .{ .literal = .{
                .value = val,
                .datatype = dt,
                .lang = lang,
            } };
        },
        else => error.BadPayload,
    };
}

/// Free memory allocated for a term input.
pub fn freeInput(allocator: Allocator, input: rdf.Input) void {
    switch (input) {
        .iri => |s| allocator.free(s),
        .blank_node => |s| allocator.free(s),
        .literal => |lit| {
            allocator.free(lit.value);
            if (lit.datatype) |d| allocator.free(d);
            if (lit.lang) |l| allocator.free(l);
        },
    }
}

/// Decoded payload for a quad addition or removal.
pub const DecodedQuadPayload = struct {
    subject: rdf.Input,
    predicate: []const u8,
    object: rdf.Input,
    graph: []const u8,
};

/// Decode a quad payload from a buffer.
pub fn decodeAddOrRemovePayload(allocator: Allocator, payload: []const u8) (Allocator.Error || WalError)!DecodedQuadPayload {
    var r = Io.Reader.fixed(payload);
    const subject = try readTermInput(allocator, &r);
    errdefer freeInput(allocator, subject);
    const predicate = try readStr(allocator, &r);
    errdefer allocator.free(predicate);
    const object = try readTermInput(allocator, &r);
    errdefer freeInput(allocator, object);
    const graph = try readStr(allocator, &r);
    return .{ .subject = subject, .predicate = predicate, .object = object, .graph = graph };
}

/// Free memory allocated for a decoded quad payload.
pub fn freeDecodedQuadPayload(allocator: Allocator, decoded: DecodedQuadPayload) void {
    freeInput(allocator, decoded.subject);
    allocator.free(decoded.predicate);
    freeInput(allocator, decoded.object);
    allocator.free(decoded.graph);
}

/// Replay WAL records into a dataset-like target.
pub fn replay(
    comptime AddError: type,
    comptime RemoveError: type,
    allocator: Allocator,
    reader: *Io.Reader,
    seed: u32,
    file: *Io.File,
    io: Io,
    target: anytype,
    add_fn: fn (@TypeOf(target), rdf.Input, []const u8, rdf.Input, []const u8) AddError!void,
    remove_fn: fn (@TypeOf(target), rdf.Input, []const u8, rdf.Input, []const u8) RemoveError!void,
) (Allocator.Error || WalError || File.LengthError || AddError || RemoveError)!void {
    var replay_crc = std.hash.Crc32.init();
    var seed_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &seed_buf, seed, .little);
    replay_crc.update(&seed_buf);

    // Keep track of the last valid record boundary to truncate if corruption occurs.
    // 9 is magic(4) + version(1) + seed(4)
    var last_valid_offset: u64 = 9;

    while (true) {
        var kind_buffer: [1]u8 = undefined;
        const bytes_read = Io.Reader.readSliceShort(reader, &kind_buffer) catch break;
        if (bytes_read == 0) break;
        if (bytes_read != 1) break;

        const payload_length = readU32Le(reader) catch break;

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, payload_length, .little);

        const payload = allocator.alloc(u8, payload_length) catch return error.OutOfMemory;
        defer allocator.free(payload);
        Io.Reader.readSliceAll(reader, payload) catch break;

        // Update cumulative checksum for kind, length, and payload
        replay_crc.update(&kind_buffer);
        replay_crc.update(&len_buf);
        replay_crc.update(payload);

        // Read the record's checksum
        const expected_checksum = readU32Le(reader) catch break;

        if (expected_checksum != replay_crc.final()) {
            // Checksum mismatch, corrupted record. Stop replay.
            break;
        }

        const kind: RecordKind = @enumFromInt(kind_buffer[0]);
        switch (kind) {
            .commit => {},
            .add_quad => {
                const decoded = decodeAddOrRemovePayload(allocator, payload) catch break;
                defer freeDecodedQuadPayload(allocator, decoded);
                try add_fn(target, decoded.subject, decoded.predicate, decoded.object, decoded.graph);
            },
            .remove_quad => {
                const decoded = decodeAddOrRemovePayload(allocator, payload) catch break;
                defer freeDecodedQuadPayload(allocator, decoded);
                try remove_fn(target, decoded.subject, decoded.predicate, decoded.object, decoded.graph);
            },
        }

        // Valid record parsed successfully. 1 byte kind + 4 byte len + payload + 4 byte checksum
        last_valid_offset += 1 + 4 + payload_length + 4;
    }

    // Truncate the file to last_valid_offset to drop any corrupted/incomplete tail
    file.setLength(io, last_valid_offset) catch {};
}
