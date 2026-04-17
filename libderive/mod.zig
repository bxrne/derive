//! libderive — embeddable RDF quad store.
//!
//! Public API is intentionally small: use `RDFDataset` and the types below.
//! Internal modules (`rdf.zig`, `storage/`, …) are implementation details.

const dataset = @import("dataset.zig");
const rdf = @import("rdf.zig");
const storage = @import("storage/mod.zig");

pub const RDFDataset = dataset.RDFDataset;
pub const WalMode = dataset.WalMode;
pub const WalBundle = dataset.WalBundle;
pub const OpenError = dataset.OpenError;
pub const AddStatementError = dataset.AddStatementError;

pub const Pattern = rdf.Pattern;
pub const Input = rdf.Input;
pub const LiteralInput = rdf.LiteralInput;
pub const StatementBoundaryError = rdf.StatementBoundaryError;
pub const MatchIterator = dataset.RDFDataset.MatchIterator;

pub const IndexBacking = dataset.IndexBacking;

pub const Quad = rdf.Quad;
pub const Term = rdf.Term;
pub const Handle = rdf.Handle;

test {
    _ = @import("rdf.zig");
    _ = @import("index.zig");
    _ = @import("query.zig");
    _ = @import("dataset.zig");
    _ = @import("storage/mod.zig");
    _ = @import("storage/engine.zig");
    _ = @import("storage/string_pool.zig");
    _ = @import("storage/contiguous_store.zig");
    _ = @import("storage/tree_store.zig");
    _ = @import("storage/wal.zig");
}
