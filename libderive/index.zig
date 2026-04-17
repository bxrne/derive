//! Six-permutation index over `(subject, predicate, object, graph)` backed
//! by lexicographically ordered key stores.
//!
//! The default backing is a contiguous sorted list; alternate stores can
//! provide the same ordered scan semantics with different write costs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const index_key = @import("storage/index_key.zig");
const ContiguousStore = @import("storage/contiguous_store.zig").ContiguousStore;
const TreeStore = @import("storage/tree_store.zig").TreeStore;
const Key = index_key.Key;

/// Iterator over keys returned by an index scan.
pub const KeyScan = union(enum) {
    slice: struct { items: []const Key, position: usize },
    tree: TreeStore.Iterator,

    pub fn next(self: *KeyScan) ?Key {
        return switch (self.*) {
            .slice => |*slice| blk: {
                if (slice.position >= slice.items.len) break :blk null;
                const key = slice.items[slice.position];
                slice.position += 1;
                break :blk key;
            },
            .tree => |*iterator| iterator.next(),
        };
    }
};

/// Supported index store backings.
pub const IndexBacking = enum { contiguous, tree };

/// The six quad-component orderings used for index permutations.
pub const Permutation = enum {
    spog,
    posg,
    ospg,
    gspo,
    gpos,
    gosp,

    /// Re-order a quad according to this permutation.
    fn encode(self: Permutation, subject: u32, predicate: u32, object: u32, graph: u32) Key {
        return switch (self) {
            .spog => .{ subject, predicate, object, graph },
            .posg => .{ predicate, object, subject, graph },
            .ospg => .{ object, subject, predicate, graph },
            .gspo => .{ graph, subject, predicate, object },
            .gpos => .{ graph, predicate, object, subject },
            .gosp => .{ graph, object, subject, predicate },
        };
    }
};

/// Six-permutation index backed by sorted key stores.
pub const Index = struct {
    stores: Stores,

    const Stores = union(IndexBacking) {
        contiguous: [6]ContiguousStore,
        tree: [6]TreeStore,
    };

    pub fn init(allocator: Allocator, index_backing: IndexBacking) Index {
        return switch (index_backing) {
            .contiguous => .{ .stores = .{ .contiguous = initStores(ContiguousStore, allocator) } },
            .tree => .{ .stores = .{ .tree = initStores(TreeStore, allocator) } },
        };
    }

    fn initStores(comptime Store: type, allocator: Allocator) [6]Store {
        var stores: [6]Store = undefined;
        for (&stores) |*store| store.* = Store.init(allocator);
        return stores;
    }

    pub fn backing(self: *const Index) IndexBacking {
        return std.meta.activeTag(self.stores);
    }

    pub fn deinit(self: *Index) void {
        switch (self.stores) {
            inline else => |*stores| for (stores) |*store| store.deinit(),
        }
    }

    /// Insert a quad into every permutation index.
    pub fn add(self: *Index, subject: u32, predicate: u32, object: u32, graph: u32) Allocator.Error!void {
        switch (self.stores) {
            inline else => |*stores| inline for (std.meta.tags(Permutation), stores) |permutation, *store| {
                try store.insert(permutation.encode(subject, predicate, object, graph));
            },
        }
    }

    /// Return true when the exact quad is present.
    pub fn containsQuad(self: *const Index, subject: u32, predicate: u32, object: u32, graph: u32) bool {
        const key = Permutation.spog.encode(subject, predicate, object, graph);
        return switch (self.stores) {
            inline else => |stores| stores[@intFromEnum(Permutation.spog)].contains(key),
        };
    }

    /// Return true when any quad with the given subject, predicate and object exists.
    pub fn containsTriple(self: *const Index, subject: u32, predicate: u32, object: u32) bool {
        const prefix: [3]u32 = .{ subject, predicate, object };
        var scan_result = self.scan(.spog, &prefix);
        return scan_result.next() != null;
    }

    /// Remove a quad from every permutation index.
    pub fn remove(self: *Index, subject: u32, predicate: u32, object: u32, graph: u32) void {
        switch (self.stores) {
            inline else => |*stores| inline for (std.meta.tags(Permutation), stores) |permutation, *store| {
                _ = store.remove(permutation.encode(subject, predicate, object, graph));
            },
        }
    }

    /// Prefix scan on a specific permutation.
    pub fn scan(self: *const Index, permutation: Permutation, prefix: []const u32) KeyScan {
        return switch (self.stores) {
            .contiguous => |stores| .{ .slice = .{ .items = stores[@intFromEnum(permutation)].scan(prefix), .position = 0 } },
            .tree => |stores| .{ .tree = stores[@intFromEnum(permutation)].scan(prefix) },
        };
    }

    /// Clear every permutation index.
    pub fn clear(self: *Index) void {
        switch (self.stores) {
            inline else => |*stores| for (stores) |*store| store.clear(),
        }
    }
};

const testing = std.testing;
const compareKeys = index_key.compareKeys;
const prefixOrder = index_key.prefixOrder;

test "sorted insert and contains" {
    var store = ContiguousStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 3, 1, 4, 1 });
    try store.insert(.{ 1, 2, 3, 4 });
    try store.insert(.{ 2, 0, 0, 0 });

    try testing.expect(store.contains(.{ 1, 2, 3, 4 }));
    try testing.expect(store.contains(.{ 2, 0, 0, 0 }));
    try testing.expect(store.contains(.{ 3, 1, 4, 1 }));
    try testing.expect(!store.contains(.{ 9, 9, 9, 9 }));
}

test "duplicate insert is idempotent" {
    var store = ContiguousStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 2, 3, 4 });
    try store.insert(.{ 1, 2, 3, 4 });
    try testing.expectEqual(@as(usize, 1), store.count());
}

test "prefix scan" {
    var store = ContiguousStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 10, 100, 0 });
    try store.insert(.{ 1, 20, 200, 0 });
    try store.insert(.{ 2, 10, 100, 0 });

    try testing.expectEqual(@as(usize, 2), store.scan(&.{1}).len);
    try testing.expectEqual(@as(usize, 1), store.scan(&.{ 1, 10 }).len);
}

test "remove" {
    var store = ContiguousStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 2, 3, 4 });
    try testing.expect(store.remove(.{ 1, 2, 3, 4 }));
    try testing.expect(!store.contains(.{ 1, 2, 3, 4 }));
    try testing.expect(!store.remove(.{ 1, 2, 3, 4 }));
}

test "add and exact lookup" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try testing.expect(index.containsQuad(1, 2, 3, 4));
    try testing.expect(index.containsTriple(1, 2, 3));
    try testing.expect(!index.containsQuad(9, 2, 3, 4));
    try testing.expect(!index.containsTriple(9, 2, 3));
}

test "remove from all indices" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    index.remove(1, 2, 3, 4);
    try testing.expect(!index.containsQuad(1, 2, 3, 4));
}

test "prefix scan by subject" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 10, 100, 0);
    try index.add(1, 20, 200, 0);
    try index.add(2, 10, 100, 0);

    var scan_result = index.scan(.spog, &.{1});
    try testing.expectEqual(@as(usize, 2), scanCount(&scan_result));
}

test "prefix scan by graph" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 2, 3, 10);
    try index.add(4, 5, 6, 10);
    try index.add(7, 8, 9, 20);

    var scan_ten = index.scan(.gspo, &.{10});
    try testing.expectEqual(@as(usize, 2), scanCount(&scan_ten));
    var scan_twenty = index.scan(.gspo, &.{20});
    try testing.expectEqual(@as(usize, 1), scanCount(&scan_twenty));
}

test "prefix scan by predicate and object" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 99, 50, 0);
    try index.add(2, 99, 50, 0);
    try index.add(3, 99, 60, 0);

    var scan_result = index.scan(.posg, &.{ 99, 50 });
    try testing.expectEqual(@as(usize, 2), scanCount(&scan_result));
}

test "index duplicate insert is idempotent" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try index.add(1, 2, 3, 4);
    var scan_result = index.scan(.spog, &.{1});
    try testing.expectEqual(@as(usize, 1), scanCount(&scan_result));
}

test "clear all indices" {
    var index = Index.init(testing.allocator, .contiguous);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try index.add(5, 6, 7, 8);
    index.clear();
    try testing.expect(!index.containsQuad(1, 2, 3, 4));
    try testing.expect(!index.containsQuad(5, 6, 7, 8));
}

test "tree backing preserves prefix order" {
    var index = Index.init(testing.allocator, .tree);
    defer index.deinit();

    try index.add(1, 10, 100, 0);
    try index.add(1, 20, 200, 0);
    try index.add(1, 30, 50, 0);

    var scan_result = index.scan(.spog, &.{1});
    var previous: ?Key = null;
    while (scan_result.next()) |key| {
        if (previous) |last| {
            try testing.expect(prefixOrder(key, &.{1}) == .eq);
            try testing.expect(compareKeys(last, key) != .gt);
        }
        previous = key;
    }
}

fn scanCount(scan: *KeyScan) usize {
    var count: usize = 0;
    while (scan.next()) |_| count += 1;
    return count;
}
