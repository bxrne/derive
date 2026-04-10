//! Storage subsystem: engine, string interning, and write-ahead log.

pub const engine = @import("engine.zig");
pub const string_pool = @import("string_pool.zig");
pub const wal = @import("wal.zig");

pub const Engine = engine.Engine;
pub const Core = engine.Core;
pub const QuadStore = engine.QuadStore;
pub const LiveQuadIterator = engine.LiveQuadIterator;
pub const StringPool = string_pool.StringPool;
pub const Handle = string_pool.Handle;
pub const spogKeyFromQuad = engine.spogKeyFromQuad;
