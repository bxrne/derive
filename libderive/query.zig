//! Pattern binding, scan-plan selection, and match iteration.
//!
//! Given a `Pattern` with optional bound components, this module resolves
//! each component to a handle key, picks the index permutation whose leading
//! prefix covers the most bound components, and returns an iterator over
//! the matching quads.

const std = @import("std");
const rdf = @import("rdf.zig");
const index_mod = @import("index.zig");
const Index = index_mod.Index;
const KeyScan = index_mod.KeyScan;
const Permutation = index_mod.Permutation;
const Quad = rdf.Quad;
const storage = @import("storage/mod.zig");
const QuadStore = storage.QuadStore;
const StringPool = storage.StringPool;
const Pattern = rdf.Pattern;

/// Bindings of a match pattern to string-pool handle keys.
/// A null component is an unbound wildcard.
pub const Bindings = struct {
    subject: ?u32 = null,
    predicate: ?u32 = null,
    object: ?u32 = null,
    graph: ?u32 = null,

    fn isFullyUnbound(self: Bindings) bool {
        return self.subject == null and self.predicate == null and
            self.object == null and self.graph == null;
    }

    /// Map the four SPOG components into the order defined by the permutation.
    fn componentsFor(self: Bindings, permutation: Permutation) [4]?u32 {
        return switch (permutation) {
            .spog => .{ self.subject, self.predicate, self.object, self.graph },
            .posg => .{ self.predicate, self.object, self.subject, self.graph },
            .ospg => .{ self.object, self.subject, self.predicate, self.graph },
            .gspo => .{ self.graph, self.subject, self.predicate, self.object },
            .gpos => .{ self.graph, self.predicate, self.object, self.subject },
            .gosp => .{ self.graph, self.object, self.subject, self.predicate },
        };
    }
};

/// Walks quads that match a bound pattern, using either a full slot scan
/// or a prefix-narrowed key range.
pub const Iterator = struct {
    store: *const QuadStore,
    mode: Mode,
    bindings: Bindings,
    position: usize,

    const Mode = union(enum) {
        statements,
        keys: struct { permutation: Permutation, scan: KeyScan },
    };

    /// Advance to the next quad matching the bound pattern.
    pub fn next(self: *Iterator) ?Quad {
        switch (self.mode) {
            .statements => {
                const items = self.store.slotSlice();
                while (self.position < items.len) : (self.position += 1) {
                    if (items[self.position]) |quad| if (self.matches(quad)) {
                        self.position += 1;
                        return quad;
                    };
                }
                return null;
            },
            .keys => |*key_mode| while (key_mode.scan.next()) |key| {
                const spog = decodeToSpog(key_mode.permutation, key);
                const quad = self.store.quadCopyForSpogKey(spog) orelse continue;
                if (self.matches(quad)) return quad;
            },
        }
        return null;
    }

    fn matches(self: *const Iterator, quad: Quad) bool {
        if (self.bindings.subject) |s| if (quad.subject.key() != s) return false;
        if (self.bindings.predicate) |p| if (@intFromEnum(quad.predicate) != p) return false;
        if (self.bindings.object) |o| if (quad.object.key() != o) return false;
        if (self.bindings.graph) |g| if (@intFromEnum(quad.graph) != g) return false;
        return true;
    }
};

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

/// Resolve a string through the pool to its raw `u32` handle key.
fn findKey(pool: *const StringPool, name: []const u8) ?u32 {
    return if (pool.find(name)) |h| @intFromEnum(h) else null;
}

/// Resolve a pattern to handle keys via the string pool. Returns null if
/// any specified term does not exist in the pool.
pub fn bindHandles(pool: *const StringPool, pattern: Pattern) ?Bindings {
    var bindings: Bindings = .{};
    if (pattern.s) |value| bindings.subject = rdf.findTermKey(pool, value) orelse return null;
    if (pattern.p) |value| bindings.predicate = findKey(pool, value) orelse return null;
    if (pattern.o) |value| bindings.object = rdf.findTermKey(pool, value) orelse return null;
    if (pattern.g) |value| bindings.graph = findKey(pool, value) orelse return null;
    return bindings;
}

const ScanPlan = struct {
    permutation: Permutation,
    prefix: [4]u32,
    prefix_length: u8,
};

/// Select the permutation whose longest prefix covers the most bound
/// components. Ties break in the `Permutation` declaration order.
fn chooseScanPlan(bindings: Bindings) ScanPlan {
    const tie_break_order = [_]Permutation{ .gspo, .gpos, .gosp, .spog, .posg, .ospg };

    var best_permutation: Permutation = tie_break_order[0];
    var best_length: u8 = 0;
    var best_components: [4]?u32 = bindings.componentsFor(best_permutation);

    for (tie_break_order) |permutation| {
        const components = bindings.componentsFor(permutation);
        var length: u8 = 0;
        for (components) |component| {
            if (component == null) break;
            length += 1;
        }
        if (length > best_length) {
            best_length = length;
            best_permutation = permutation;
            best_components = components;
        }
    }

    var prefix: [4]u32 = .{ 0, 0, 0, 0 };
    for (best_components[0..best_length], 0..) |component, i| prefix[i] = component.?;
    return .{ .permutation = best_permutation, .prefix = prefix, .prefix_length = best_length };
}

/// Return an iterator that yields no results.
pub fn unmatchable(store: *const QuadStore) Iterator {
    return .{
        .store = store,
        .mode = .statements,
        .bindings = .{},
        .position = store.physicalSlotCount(),
    };
}

/// Build an iterator for the given bound handles, choosing the best scan plan.
pub fn build(store: *const QuadStore, index: *const Index, bindings: Bindings) Iterator {
    if (bindings.isFullyUnbound()) {
        return .{ .store = store, .mode = .statements, .bindings = bindings, .position = 0 };
    }
    const plan = chooseScanPlan(bindings);
    const keys = index.scan(plan.permutation, plan.prefix[0..plan.prefix_length]);
    return .{
        .store = store,
        .mode = .{ .keys = .{ .permutation = plan.permutation, .scan = keys } },
        .bindings = bindings,
        .position = 0,
    };
}
