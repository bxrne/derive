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

pub const Quad = rdf.Quad;
pub const Term = rdf.Term;
pub const Handle = rdf.Handle;
