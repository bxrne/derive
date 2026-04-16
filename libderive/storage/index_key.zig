//! Shared index key definition for permutation stores.

const std = @import("std");

/// A four-component key: one `u32` per quad component in permutation order.
pub const Key = [4]u32;

/// Lexicographic comparison of two keys.
pub fn compareKeys(a: Key, b: Key) std.math.Order {
    for (a, b) |x, y| {
        const order = std.math.order(x, y);
        if (order != .eq) return order;
    }
    return .eq;
}

/// Compare the leading components of a key against a prefix slice.
pub fn prefixOrder(key: Key, prefix: []const u32) std.math.Order {
    for (prefix, 0..) |value, i| {
        const order = std.math.order(key[i], value);
        if (order != .eq) return order;
    }
    return .eq;
}
