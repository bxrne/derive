//! Pattern binding, scan-plan selection, and match iteration.
//!
//! Given a `Pattern` with optional bound components, this module resolves
//! each component to a handle key, picks the index permutation whose leading
//! prefix covers the most bound components, and returns an iterator over
//! the matching quads.

const std = @import("std");
const rdf = @import("rdf.zig");
const Index = @import("index.zig").Index;
const KeyScan = @import("index.zig").KeyScan;
const Permutation = @import("index.zig").Permutation;
const Quad = rdf.Quad;
const storage = @import("storage/mod.zig");
const QuadStore = storage.QuadStore;
const StringPool = storage.StringPool;
const Pattern = rdf.Pattern;

/// Walks quads that match a bound pattern, using either a full slot scan
/// or a prefix-narrowed key range.
pub const Iterator = struct {
    store: *const QuadStore,
    mode: union(enum) {
        statements: void,
        keys: struct { permutation: Permutation, scan: KeyScan },
    },
    subject: ?u32,
    predicate: ?u32,
    object: ?u32,
    graph: ?u32,
    position: usize,

    /// Advance to the next quad matching the bound pattern.
    pub fn next(self: *Iterator) ?Quad {
        switch (self.mode) {
            .statements => {
                const items = self.store.slotSlice();
                while (self.position < items.len) {
                    const index = self.position;
                    self.position += 1;
                    if (items[index]) |quad| {
                        if (matches(self, quad)) return quad;
                    }
                }
                return null;
            },
            .keys => |*mode| {
                while (mode.scan.next()) |key| {
                    const spog_key = decodeToSpog(mode.permutation, key);
                    const quad = self.store.quadCopyForSpogKey(spog_key) orelse continue;
                    if (matches(self, quad)) return quad;
                }
                return null;
            },
        }
    }
};

/// Check whether a quad satisfies all bound filter components.
fn matches(filters: *const Iterator, quad: Quad) bool {
    if (filters.subject) |subject| if (quad.subject.key() != subject) return false;
    if (filters.predicate) |predicate| if (@intFromEnum(quad.predicate) != predicate) return false;
    if (filters.object) |object| if (quad.object.key() != object) return false;
    if (filters.graph) |graph| if (@intFromEnum(quad.graph) != graph) return false;
    return true;
}

/// Translate a permutation-ordered key back to subject-predicate-object-graph order.
fn decodeToSpog(permutation: Permutation, key: [4]u32) [4]u32 {
    return switch (permutation) {
        .spog => key,
        .posg => .{ key[2], key[0], key[1], key[3] },
        .ospg => .{ key[1], key[2], key[0], key[3] },
        .gspo => .{ key[1], key[2], key[3], key[0] },
        .gpos => .{ key[3], key[1], key[2], key[0] },
        .gosp => .{ key[2], key[3], key[1], key[0] },
    };
}

/// Handle keys resolved from a `Pattern` via the string pool.
pub const BoundHandles = struct {
    subject: ?u32,
    predicate: ?u32,
    object: ?u32,
    graph: ?u32,
};

/// Resolve a pattern to bound handle keys via the string pool. Returns null
/// if any specified term does not exist in the pool.
pub fn bindHandles(pool: *const StringPool, pattern: Pattern) ?BoundHandles {
    const subject: ?u32 = if (pattern.s) |value| rdf.findTermKey(pool, value) orelse return null else null;
    const predicate: ?u32 = if (pattern.p) |value| if (pool.find(value)) |handle| @intFromEnum(handle) else return null else null;
    const object: ?u32 = if (pattern.o) |value| rdf.findTermKey(pool, value) orelse return null else null;
    const graph: ?u32 = if (pattern.g) |value| if (pool.find(value)) |handle| @intFromEnum(handle) else return null else null;
    return .{ .subject = subject, .predicate = predicate, .object = object, .graph = graph };
}

const ScanPlan = struct {
    permutation: Permutation,
    prefix: [4]u32,
    prefix_length: u8,
};

/// Select the permutation whose longest prefix covers the most bound components.
fn chooseScanPlan(subject: ?u32, predicate: ?u32, object: ?u32, graph: ?u32) ScanPlan {
    const Candidate = struct { permutation: Permutation, prefix_length: u8 };
    const candidates = [_]Candidate{
        .{ .permutation = .gspo, .prefix_length = prefixLength(.gspo, subject, predicate, object, graph) },
        .{ .permutation = .gpos, .prefix_length = prefixLength(.gpos, subject, predicate, object, graph) },
        .{ .permutation = .gosp, .prefix_length = prefixLength(.gosp, subject, predicate, object, graph) },
        .{ .permutation = .spog, .prefix_length = prefixLength(.spog, subject, predicate, object, graph) },
        .{ .permutation = .posg, .prefix_length = prefixLength(.posg, subject, predicate, object, graph) },
        .{ .permutation = .ospg, .prefix_length = prefixLength(.ospg, subject, predicate, object, graph) },
    };
    var best = candidates[0];
    for (candidates[1..]) |candidate| {
        if (candidate.prefix_length > best.prefix_length) best = candidate;
    }
    return .{
        .permutation = best.permutation,
        .prefix = buildPrefix(best.permutation, best.prefix_length, subject, predicate, object, graph),
        .prefix_length = best.prefix_length,
    };
}

/// Count how many leading components of the permutation are bound.
fn prefixLength(permutation: Permutation, subject: ?u32, predicate: ?u32, object: ?u32, graph: ?u32) u8 {
    const components = permutationComponents(permutation, subject, predicate, object, graph);
    var length: u8 = 0;
    inline for (components) |component| {
        if (component == null) break;
        length += 1;
    }
    return length;
}

/// Map the four SPOG components into the order defined by the given permutation.
fn permutationComponents(permutation: Permutation, subject: ?u32, predicate: ?u32, object: ?u32, graph: ?u32) [4]?u32 {
    return switch (permutation) {
        .spog => .{ subject, predicate, object, graph },
        .posg => .{ predicate, object, subject, graph },
        .ospg => .{ object, subject, predicate, graph },
        .gspo => .{ graph, subject, predicate, object },
        .gpos => .{ graph, predicate, object, subject },
        .gosp => .{ graph, object, subject, predicate },
    };
}

/// Build a fixed-size prefix array from the bound components in permutation order.
fn buildPrefix(permutation: Permutation, prefix_length: u8, subject: ?u32, predicate: ?u32, object: ?u32, graph: ?u32) [4]u32 {
    std.debug.assert(prefix_length <= 4);
    const components = permutationComponents(permutation, subject, predicate, object, graph);
    var out: [4]u32 = .{ 0, 0, 0, 0 };
    if (prefix_length >= 1) out[0] = components[0].?;
    if (prefix_length >= 2) out[1] = components[1].?;
    if (prefix_length >= 3) out[2] = components[2].?;
    if (prefix_length >= 4) out[3] = components[3].?;
    return out;
}

/// Create a statement-scan iterator starting at the given position.
fn statementScan(
    store: *const QuadStore,
    position: usize,
    subject: ?u32,
    predicate: ?u32,
    object: ?u32,
    graph: ?u32,
) Iterator {
    return .{
        .store = store,
        .mode = .statements,
        .subject = subject,
        .predicate = predicate,
        .object = object,
        .graph = graph,
        .position = position,
    };
}

/// Return an iterator that yields no results.
pub fn unmatchable(store: *const QuadStore) Iterator {
    return statementScan(store, store.physicalSlotCount(), null, null, null, null);
}

/// Build an iterator for the given bound handles, choosing the best scan plan.
pub fn build(store: *const QuadStore, index: *const Index, bound: BoundHandles) Iterator {
    const subject = bound.subject;
    const predicate = bound.predicate;
    const object = bound.object;
    const graph = bound.graph;

    if (subject == null and predicate == null and object == null and graph == null) {
        return statementScan(store, 0, null, null, null, null);
    }

    const plan = chooseScanPlan(subject, predicate, object, graph);
    const keys = index.scan(plan.permutation, plan.prefix[0..plan.prefix_length]);
    return .{
        .store = store,
        .mode = .{ .keys = .{ .permutation = plan.permutation, .scan = keys } },
        .subject = subject,
        .predicate = predicate,
        .object = object,
        .graph = graph,
        .position = 0,
    };
}
