//! Six-permutation index over `(subject, predicate, object, graph)` backed
//! by in-memory sorted key stores.
//!
//! Each permutation keeps its keys in lexicographic order in a contiguous
//! list. Insertions binary-search for position and shift tail elements;
//! prefix scans return a contiguous slice via lower and upper bound searches.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// A four-component key: one `u32` per quad component in permutation order.
pub const Key = [4]u32;

/// Lexicographic comparison of two keys.
fn compareKeys(a: Key, b: Key) std.math.Order {
    for (a, b) |x, y| {
        const order = std.math.order(x, y);
        if (order != .eq) return order;
    }
    return .eq;
}

/// Compare the leading components of a key against a prefix slice.
fn prefixOrder(key: Key, prefix: []const u32) std.math.Order {
    for (prefix, 0..) |value, i| {
        const order = std.math.order(key[i], value);
        if (order != .eq) return order;
    }
    return .eq;
}

/// Return the index of the first key not less than the prefix.
fn lowerBound(items: []const Key, prefix: []const u32) usize {
    var low: usize = 0;
    var high: usize = items.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (prefixOrder(items[mid], prefix) == .lt) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

/// Return the index one past the last key not greater than the prefix.
fn upperBound(items: []const Key, prefix: []const u32) usize {
    var low: usize = 0;
    var high: usize = items.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (prefixOrder(items[mid], prefix) != .gt) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

/// Sorted in-memory key store for one permutation slice of the index.
pub const MemoryStore = struct {
    allocator: Allocator,
    items: std.ArrayListUnmanaged(Key),

    pub fn init(allocator: Allocator) MemoryStore {
        return .{ .allocator = allocator, .items = .empty };
    }

    pub fn deinit(self: *MemoryStore) void {
        self.items.deinit(self.allocator);
    }

    /// Insert a key in sorted order. Duplicates are silently ignored.
    pub fn insert(self: *MemoryStore, key: Key) Allocator.Error!void {
        const position = lowerBound(self.items.items, &key);
        if (position < self.items.items.len and compareKeys(self.items.items[position], key) == .eq) return;

        try self.items.append(self.allocator, key);
        var i: usize = self.items.items.len - 1;
        while (i > position) : (i -= 1) {
            self.items.items[i] = self.items.items[i - 1];
        }
        self.items.items[position] = key;
    }

    /// Return true when the store contains the exact key.
    pub fn contains(self: *const MemoryStore, key: Key) bool {
        const position = lowerBound(self.items.items, &key);
        return position < self.items.items.len and compareKeys(self.items.items[position], key) == .eq;
    }

    /// Remove a key. Returns true when a key was actually removed.
    pub fn remove(self: *MemoryStore, key: Key) bool {
        const position = lowerBound(self.items.items, &key);
        if (position >= self.items.items.len) return false;
        if (compareKeys(self.items.items[position], key) != .eq) return false;
        _ = self.items.orderedRemove(position);
        return true;
    }

    /// Return a slice of all keys whose leading components match the prefix.
    pub fn scan(self: *const MemoryStore, prefix: []const u32) []const Key {
        std.debug.assert(prefix.len > 0);
        std.debug.assert(prefix.len <= 4);
        const low = lowerBound(self.items.items, prefix);
        const high = upperBound(self.items.items, prefix);
        return self.items.items[low..high];
    }

    /// Remove all keys but keep allocated memory.
    pub fn clear(self: *MemoryStore) void {
        self.items.clearRetainingCapacity();
    }

    /// Return the number of stored keys.
    pub fn count(self: *const MemoryStore) usize {
        return self.items.items.len;
    }
};

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

/// Six-permutation index backed by in-memory sorted vectors.
pub const Index = struct {
    stores: [6]MemoryStore,

    pub fn init(allocator: Allocator) Index {
        var stores: [6]MemoryStore = undefined;
        for (&stores) |*store| store.* = MemoryStore.init(allocator);
        return .{ .stores = stores };
    }

    pub fn deinit(self: *Index) void {
        for (&self.stores) |*store| store.deinit();
    }

    /// Insert a quad into every permutation index.
    pub fn add(self: *Index, subject: u32, predicate: u32, object: u32, graph: u32) Allocator.Error!void {
        inline for (std.meta.tags(Permutation), &self.stores) |permutation, *store| {
            try store.insert(permutation.encode(subject, predicate, object, graph));
        }
    }

    /// Return true when the exact quad is present.
    pub fn containsQuad(self: *const Index, subject: u32, predicate: u32, object: u32, graph: u32) bool {
        return self.stores[@intFromEnum(Permutation.spog)].contains(
            Permutation.spog.encode(subject, predicate, object, graph),
        );
    }

    /// Return true when any quad with the given subject, predicate and object exists.
    pub fn containsTriple(self: *const Index, subject: u32, predicate: u32, object: u32) bool {
        return self.stores[@intFromEnum(Permutation.spog)].scan(&.{ subject, predicate, object }).len > 0;
    }

    /// Remove a quad from every permutation index.
    pub fn remove(self: *Index, subject: u32, predicate: u32, object: u32, graph: u32) void {
        inline for (std.meta.tags(Permutation), &self.stores) |permutation, *store| {
            _ = store.remove(permutation.encode(subject, predicate, object, graph));
        }
    }

    /// Prefix scan on a specific permutation.
    pub fn scan(self: *const Index, permutation: Permutation, prefix: []const u32) []const Key {
        return self.stores[@intFromEnum(permutation)].scan(prefix);
    }

    /// Clear every permutation index.
    pub fn clear(self: *Index) void {
        for (&self.stores) |*store| store.clear();
    }
};

const testing = std.testing;

test "sorted insert and contains" {
    var store = MemoryStore.init(testing.allocator);
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
    var store = MemoryStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 2, 3, 4 });
    try store.insert(.{ 1, 2, 3, 4 });
    try testing.expectEqual(@as(usize, 1), store.count());
}

test "prefix scan" {
    var store = MemoryStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 10, 100, 0 });
    try store.insert(.{ 1, 20, 200, 0 });
    try store.insert(.{ 2, 10, 100, 0 });

    const one_prefix = store.scan(&.{1});
    try testing.expectEqual(@as(usize, 2), one_prefix.len);

    const exact = store.scan(&.{ 1, 10 });
    try testing.expectEqual(@as(usize, 1), exact.len);
}

test "remove" {
    var store = MemoryStore.init(testing.allocator);
    defer store.deinit();

    try store.insert(.{ 1, 2, 3, 4 });
    try testing.expect(store.remove(.{ 1, 2, 3, 4 }));
    try testing.expect(!store.contains(.{ 1, 2, 3, 4 }));
    try testing.expect(!store.remove(.{ 1, 2, 3, 4 }));
}

test "add and exact lookup" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try testing.expect(index.containsQuad(1, 2, 3, 4));
    try testing.expect(index.containsTriple(1, 2, 3));
    try testing.expect(!index.containsQuad(9, 2, 3, 4));
    try testing.expect(!index.containsTriple(9, 2, 3));
}

test "remove from all indices" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    index.remove(1, 2, 3, 4);
    try testing.expect(!index.containsQuad(1, 2, 3, 4));
}

test "prefix scan by subject" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 10, 100, 0);
    try index.add(1, 20, 200, 0);
    try index.add(2, 10, 100, 0);

    try testing.expectEqual(@as(usize, 2), index.scan(.spog, &.{1}).len);
}

test "prefix scan by graph" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 2, 3, 10);
    try index.add(4, 5, 6, 10);
    try index.add(7, 8, 9, 20);

    try testing.expectEqual(@as(usize, 2), index.scan(.gspo, &.{10}).len);
    try testing.expectEqual(@as(usize, 1), index.scan(.gspo, &.{20}).len);
}

test "prefix scan by predicate and object" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 99, 50, 0);
    try index.add(2, 99, 50, 0);
    try index.add(3, 99, 60, 0);

    try testing.expectEqual(@as(usize, 2), index.scan(.posg, &.{ 99, 50 }).len);
}

test "index duplicate insert is idempotent" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try index.add(1, 2, 3, 4);
    try testing.expectEqual(@as(usize, 1), index.scan(.spog, &.{1}).len);
}

test "clear all indices" {
    var index = Index.init(testing.allocator);
    defer index.deinit();

    try index.add(1, 2, 3, 4);
    try index.add(5, 6, 7, 8);
    index.clear();
    try testing.expect(!index.containsQuad(1, 2, 3, 4));
    try testing.expect(!index.containsQuad(5, 6, 7, 8));
}
