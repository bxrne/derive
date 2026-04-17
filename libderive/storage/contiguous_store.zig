//! Contiguous sorted key store for one permutation slice of the index.

const std = @import("std");
const Allocator = std.mem.Allocator;
const index_key = @import("index_key.zig");
const Key = index_key.Key;
const compareKeys = index_key.compareKeys;
const prefixOrder = index_key.prefixOrder;

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

pub const ContiguousStore = struct {
    allocator: Allocator,
    items: std.ArrayListUnmanaged(Key),

    pub fn init(allocator: Allocator) ContiguousStore {
        return .{ .allocator = allocator, .items = .empty };
    }

    pub fn deinit(self: *ContiguousStore) void {
        self.items.deinit(self.allocator);
    }

    /// Insert a key in sorted order. Duplicates are silently ignored.
    pub fn insert(self: *ContiguousStore, key: Key) Allocator.Error!void {
        const position = lowerBound(self.items.items, &key);
        if (position < self.items.items.len and compareKeys(self.items.items[position], key) == .eq) return;
        try self.items.insert(self.allocator, position, key);
    }

    /// Return true when the store contains the exact key.
    pub fn contains(self: *const ContiguousStore, key: Key) bool {
        const position = lowerBound(self.items.items, &key);
        return position < self.items.items.len and compareKeys(self.items.items[position], key) == .eq;
    }

    /// Remove a key. Returns true when a key was actually removed.
    pub fn remove(self: *ContiguousStore, key: Key) bool {
        const position = lowerBound(self.items.items, &key);
        if (position >= self.items.items.len) return false;
        if (compareKeys(self.items.items[position], key) != .eq) return false;
        _ = self.items.orderedRemove(position);
        return true;
    }

    /// Return a slice of all keys whose leading components match the prefix.
    pub fn scan(self: *const ContiguousStore, prefix: []const u32) []const Key {
        std.debug.assert(prefix.len > 0);
        std.debug.assert(prefix.len <= 4);
        const low = lowerBound(self.items.items, prefix);
        const high = upperBound(self.items.items, prefix);
        return self.items.items[low..high];
    }

    /// Remove all keys but keep allocated memory.
    pub fn clear(self: *ContiguousStore) void {
        self.items.clearRetainingCapacity();
    }

    /// Return the number of stored keys.
    pub fn count(self: *const ContiguousStore) usize {
        return self.items.items.len;
    }
};
