//! String interning with compact handles and arena-backed storage.
//!
//! Each unique string is stored once in an arena. Callers receive a `Handle`
//! (a `u32` enum) that can be resolved back to the original bytes in O(1).

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

pub const Handle = enum(u32) { _ };

pub const StringPool = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayListUnmanaged([]const u8),
    lookup: std.StringHashMapUnmanaged(Handle),

    pub fn init(allocator: Allocator) StringPool {
        return .{
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .entries = .empty,
            .lookup = .empty,
        };
    }

    pub fn deinit(self: *StringPool) void {
        self.lookup.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        self.arena.deinit();
    }

    /// Intern a string, returning an existing handle if already present.
    pub fn intern(self: *StringPool, str: []const u8) Allocator.Error!Handle {
        assert(str.len > 0);
        if (self.lookup.get(str)) |handle| return handle;

        const owned = try self.arena.allocator().dupe(u8, str);
        const handle: Handle = @enumFromInt(self.entries.items.len);
        try self.entries.append(self.allocator, owned);
        try self.lookup.put(self.allocator, owned, handle);
        return handle;
    }

    /// Resolve a handle back to its interned string.
    pub fn get(self: *const StringPool, handle: Handle) []const u8 {
        const idx = @intFromEnum(handle);
        assert(idx < self.entries.items.len);
        return self.entries.items[idx];
    }

    /// Look up a string without interning. Returns null if not present.
    pub fn find(self: *const StringPool, str: []const u8) ?Handle {
        return self.lookup.get(str);
    }
};

const testing = std.testing;

test "deduplicates strings" {
    var pool = StringPool.init(testing.allocator);
    defer pool.deinit();

    const a = try pool.intern("hello");
    const b = try pool.intern("world");
    const c = try pool.intern("hello");

    try testing.expectEqual(a, c);
    try testing.expect(a != b);
    try testing.expectEqualStrings("hello", pool.get(a));
    try testing.expectEqualStrings("world", pool.get(b));
}

test "find without interning" {
    var pool = StringPool.init(testing.allocator);
    defer pool.deinit();

    try testing.expectEqual(pool.find("missing"), null);
    const h = try pool.intern("present");
    try testing.expectEqual(pool.find("present"), h);
}
