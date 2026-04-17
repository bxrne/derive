//! Quad store, engine core, and the Engine tagged union.
//!
//! `Core` bundles a string pool, quad store, and six-permutation index into
//! the shared state that every engine variant needs. `Engine` wraps `Core`
//! in a tagged union so future backends can be added without changing the
//! public `RDFDataset` shape.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const rdf = @import("../rdf.zig");
const Quad = rdf.Quad;
const Handle = rdf.Handle;
const Index = @import("../index.zig").Index;
const IndexBacking = @import("../index.zig").IndexBacking;

pub const StringPool = @import("string_pool.zig").StringPool;

/// Extract the SPOG key from a quad for deduplication and index lookup.
pub fn spogKeyFromQuad(quad: Quad) [4]u32 {
    return .{
        quad.subject.key(),
        @intFromEnum(quad.predicate),
        quad.object.key(),
        @intFromEnum(quad.graph),
    };
}

/// Iterates live quads in physical slot order, skipping tombstones.
pub const LiveQuadIterator = struct {
    store: *const QuadStore,
    position: usize,

    /// Advance to the next live quad, or return null at end.
    pub fn next(self: *LiveQuadIterator) ?Quad {
        const items = self.store.slotSlice();
        while (self.position < items.len) {
            const quad = items[self.position];
            self.position += 1;
            if (quad) |value| return value;
        }
        return null;
    }
};

/// Deduped quad storage with stable slot indices.
///
/// Slots hold `?Quad` where null is a tombstone. A hash map from SPOG keys
/// to slot indices provides O(1) deduplication. A free list recycles
/// tombstoned slots on the next insert.
pub const QuadStore = struct {
    slots: std.ArrayList(?Quad),
    free_slots: std.ArrayListUnmanaged(usize),
    spog_to_slot: std.AutoHashMapUnmanaged([4]u32, usize),
    live_count: usize,

    pub fn init() QuadStore {
        return .{
            .slots = .empty,
            .free_slots = .empty,
            .spog_to_slot = .empty,
            .live_count = 0,
        };
    }

    pub fn deinit(self: *QuadStore, allocator: Allocator) void {
        self.spog_to_slot.deinit(allocator);
        self.free_slots.deinit(allocator);
        self.slots.deinit(allocator);
    }

    pub fn len(self: *const QuadStore) usize {
        return self.live_count;
    }

    pub fn physicalSlotCount(self: *const QuadStore) usize {
        return self.slots.items.len;
    }

    pub fn slotSlice(self: *const QuadStore) []const ?Quad {
        return self.slots.items;
    }

    pub fn liveIterator(self: *const QuadStore) LiveQuadIterator {
        return .{ .store = self, .position = 0 };
    }

    pub fn containsSpogKey(self: *const QuadStore, spog_key: [4]u32) bool {
        return self.spog_to_slot.contains(spog_key);
    }

    pub fn quadCopyForSpogKey(self: *const QuadStore, spog_key: [4]u32) ?Quad {
        const slot = self.spog_to_slot.get(spog_key) orelse return null;
        return self.slots.items[slot] orelse null;
    }

    /// Append a quad that is known not to exist yet. Caller must guarantee uniqueness.
    pub fn appendUnique(self: *QuadStore, allocator: Allocator, quad: Quad, spog_key: [4]u32) Allocator.Error!void {
        assert(!self.spog_to_slot.contains(spog_key));

        const slot: usize = if (self.free_slots.pop()) |reuse| blk: {
            self.slots.items[reuse] = quad;
            break :blk reuse;
        } else blk: {
            try self.slots.append(allocator, quad);
            break :blk self.slots.items.len - 1;
        };

        try self.spog_to_slot.ensureTotalCapacity(allocator, self.spog_to_slot.count() + 1);
        self.spog_to_slot.putAssumeCapacityNoClobber(spog_key, slot);
        self.live_count += 1;
    }

    /// Remove a quad by its SPOG key, leaving a tombstone for slot reuse.
    pub fn removeBySpogKey(self: *QuadStore, allocator: Allocator, spog_key: [4]u32) Allocator.Error!void {
        const slot = self.spog_to_slot.get(spog_key) orelse return;
        assert(self.live_count > 0);
        _ = self.spog_to_slot.remove(spog_key);
        self.slots.items[slot] = null;
        try self.free_slots.append(allocator, slot);
        self.live_count -= 1;
    }
};

/// Shared state for any engine variant: string pool, quad store, and index.
pub const Core = struct {
    strings: StringPool,
    store: QuadStore,
    index: Index,

    pub fn init(allocator: Allocator, index_backing: IndexBacking) Allocator.Error!Core {
        return .{
            .strings = StringPool.init(allocator),
            .store = QuadStore.init(),
            .index = Index.init(allocator, index_backing),
        };
    }

    pub fn deinit(self: *Core) void {
        self.index.deinit();
        self.store.deinit(self.strings.allocator);
        self.strings.deinit();
    }
};

/// Tagged union so file-backed or WAL engines can be added without changing
/// the public `RDFDataset` shape.
pub const Engine = union(enum) {
    memory: Core,

    /// Access the underlying core state, regardless of engine variant.
    pub fn core(self: *Engine) *Core {
        return switch (self.*) {
            .memory => |*c| c,
        };
    }

    /// Access the underlying core state immutably.
    pub fn coreConst(self: *const Engine) *const Core {
        return switch (self.*) {
            .memory => |*c| c,
        };
    }

    pub fn deinit(self: *Engine) void {
        self.core().deinit();
    }
};

const testing = std.testing;

test "tombstone reuses slot index on reinsert" {
    var store = QuadStore.init();
    defer store.deinit(testing.allocator);

    const quad_a = Quad{
        .subject = .{ .iri = @enumFromInt(@as(u32, 1)) },
        .predicate = @enumFromInt(@as(u32, 2)),
        .object = .{ .iri = @enumFromInt(@as(u32, 3)) },
        .graph = @enumFromInt(@as(u32, 4)),
    };
    const key_a = spogKeyFromQuad(quad_a);
    try store.appendUnique(testing.allocator, quad_a, key_a);
    try testing.expectEqual(@as(usize, 1), store.physicalSlotCount());

    try store.removeBySpogKey(testing.allocator, key_a);
    try testing.expectEqual(@as(usize, 0), store.len());
    try testing.expectEqual(@as(usize, 1), store.physicalSlotCount());

    const quad_b = Quad{
        .subject = .{ .iri = @enumFromInt(@as(u32, 10)) },
        .predicate = @enumFromInt(@as(u32, 20)),
        .object = .{ .iri = @enumFromInt(@as(u32, 30)) },
        .graph = @enumFromInt(@as(u32, 40)),
    };
    const key_b = spogKeyFromQuad(quad_b);
    try store.appendUnique(testing.allocator, quad_b, key_b);
    try testing.expectEqual(@as(usize, 1), store.len());
    try testing.expectEqual(@as(usize, 1), store.physicalSlotCount());
    try testing.expect(store.quadCopyForSpogKey(key_b).?.subject.iri == quad_b.subject.iri);
}

test "core init and deinit" {
    var engine_core = try Core.init(testing.allocator, .contiguous);
    defer engine_core.deinit();
    try testing.expectEqual(@as(usize, 0), engine_core.store.len());
}

test "engine core accessor" {
    var engine = Engine{ .memory = try Core.init(testing.allocator, .contiguous) };
    defer engine.deinit();
    try testing.expectEqual(@as(usize, 0), engine.core().store.len());
}
