//! Treap-backed key store for one permutation slice of the index.

const std = @import("std");
const Allocator = std.mem.Allocator;
const index_key = @import("index_key.zig");
const Key = index_key.Key;
const compareKeys = index_key.compareKeys;
const prefixOrder = index_key.prefixOrder;

pub const TreeStore = struct {
    allocator: Allocator,
    root: ?*Node,
    key_count: usize,

    const Node = struct {
        key: Key,
        priority: u64,
        parent: ?*Node,
        left: ?*Node,
        right: ?*Node,
    };

    pub const Iterator = struct {
        current: ?*Node,
        prefix: [4]u32,
        prefix_length: u8,

        pub fn next(self: *Iterator) ?Key {
            while (self.current) |node| {
                if (!matchesPrefix(node.key, self.prefix, self.prefix_length)) {
                    self.current = null;
                    return null;
                }
                const out = node.key;
                self.current = successor(node);
                return out;
            }
            return null;
        }
    };

    pub fn init(allocator: Allocator) TreeStore {
        return .{ .allocator = allocator, .root = null, .key_count = 0 };
    }

    pub fn deinit(self: *TreeStore) void {
        self.clear();
    }

    pub fn insert(self: *TreeStore, key: Key) Allocator.Error!void {
        if (self.root == null) {
            self.root = try self.createNode(key, null);
            self.key_count = 1;
            return;
        }

        var current = self.root.?;
        while (true) {
            const order = compareKeys(key, current.key);
            if (order == .eq) return;
            if (order == .lt) {
                if (current.left) |left| {
                    current = left;
                } else {
                    const node = try self.createNode(key, current);
                    current.left = node;
                    self.key_count += 1;
                    self.bubbleUp(node);
                    return;
                }
            } else {
                if (current.right) |right| {
                    current = right;
                } else {
                    const node = try self.createNode(key, current);
                    current.right = node;
                    self.key_count += 1;
                    self.bubbleUp(node);
                    return;
                }
            }
        }
    }

    pub fn contains(self: *const TreeStore, key: Key) bool {
        return self.findNode(key) != null;
    }

    pub fn remove(self: *TreeStore, key: Key) bool {
        const node = self.findNode(key) orelse return false;
        while (node.left != null or node.right != null) {
            if (node.left == null) {
                self.rotateLeft(node);
            } else if (node.right == null) {
                self.rotateRight(node);
            } else if (node.left.?.priority < node.right.?.priority) {
                self.rotateRight(node);
            } else {
                self.rotateLeft(node);
            }
        }

        if (node.parent) |parent| {
            if (parent.left == node) {
                parent.left = null;
            } else {
                parent.right = null;
            }
        } else {
            self.root = null;
        }
        self.allocator.destroy(node);
        self.key_count -= 1;
        return true;
    }

    pub fn scan(self: *const TreeStore, prefix: []const u32) Iterator {
        std.debug.assert(prefix.len > 0);
        std.debug.assert(prefix.len <= 4);
        const low_key = prefixLowKey(prefix);
        const lower = self.lowerBoundNode(low_key);
        return .{ .current = lower, .prefix = prefixArray(prefix), .prefix_length = @intCast(prefix.len) };
    }

    pub fn clear(self: *TreeStore) void {
        var stack = std.ArrayListUnmanaged(*Node).empty;
        defer stack.deinit(self.allocator);
        if (self.root) |root| stack.append(self.allocator, root) catch return;
        while (stack.items.len > 0) {
            const node = stack.pop().?;
            if (node.left) |left| stack.append(self.allocator, left) catch {};
            if (node.right) |right| stack.append(self.allocator, right) catch {};
            self.allocator.destroy(node);
        }
        self.root = null;
        self.key_count = 0;
    }

    pub fn count(self: *const TreeStore) usize {
        return self.key_count;
    }

    fn createNode(self: *TreeStore, key: Key, parent: ?*Node) Allocator.Error!*Node {
        const node = try self.allocator.create(Node);
        node.* = .{
            .key = key,
            .priority = keyPriority(key),
            .parent = parent,
            .left = null,
            .right = null,
        };
        return node;
    }

    fn findNode(self: *const TreeStore, key: Key) ?*Node {
        var current = self.root;
        while (current) |node| {
            const order = compareKeys(key, node.key);
            if (order == .eq) return node;
            if (order == .lt) {
                current = node.left;
            } else {
                current = node.right;
            }
        }
        return null;
    }

    fn lowerBoundNode(self: *const TreeStore, key: Key) ?*Node {
        var current = self.root;
        var candidate: ?*Node = null;
        while (current) |node| {
            const order = compareKeys(node.key, key);
            if (order == .lt) {
                current = node.right;
            } else {
                candidate = node;
                current = node.left;
            }
        }
        return candidate;
    }

    fn bubbleUp(self: *TreeStore, node: *Node) void {
        while (node.parent) |parent| {
            if (node.priority >= parent.priority) break;
            if (parent.left == node) {
                self.rotateRight(parent);
            } else {
                self.rotateLeft(parent);
            }
        }
    }

    fn rotateLeft(self: *TreeStore, pivot: *Node) void {
        const right = pivot.right orelse return;
        pivot.right = right.left;
        if (right.left) |child| child.parent = pivot;
        right.parent = pivot.parent;
        if (pivot.parent) |parent| {
            if (parent.left == pivot) {
                parent.left = right;
            } else {
                parent.right = right;
            }
        } else {
            self.root = right;
        }
        right.left = pivot;
        pivot.parent = right;
    }

    fn rotateRight(self: *TreeStore, pivot: *Node) void {
        const left = pivot.left orelse return;
        pivot.left = left.right;
        if (left.right) |child| child.parent = pivot;
        left.parent = pivot.parent;
        if (pivot.parent) |parent| {
            if (parent.left == pivot) {
                parent.left = left;
            } else {
                parent.right = left;
            }
        } else {
            self.root = left;
        }
        left.right = pivot;
        pivot.parent = left;
    }

    fn keyPriority(key: Key) u64 {
        return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
    }
};

fn prefixArray(prefix: []const u32) [4]u32 {
    var out: [4]u32 = .{ 0, 0, 0, 0 };
    for (prefix, 0..) |value, i| {
        out[i] = value;
    }
    return out;
}

fn prefixLowKey(prefix: []const u32) Key {
    var out: [4]u32 = .{ 0, 0, 0, 0 };
    for (prefix, 0..) |value, i| {
        out[i] = value;
    }
    return out;
}

fn matchesPrefix(key: Key, prefix: [4]u32, prefix_length: u8) bool {
    return prefixOrder(key, prefix[0..prefix_length]) == .eq;
}

fn successor(node: *TreeStore.Node) ?*TreeStore.Node {
    if (node.right) |right| {
        var current = right;
        while (current.left) |left| current = left;
        return current;
    }
    var current = node;
    var parent = node.parent;
    while (parent) |p| {
        if (p.left == current) return p;
        current = p;
        parent = p.parent;
    }
    return null;
}
