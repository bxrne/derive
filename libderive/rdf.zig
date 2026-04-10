//! RDF types, validation, and term interning.
//!
//! All public RDF value types (`Term`, `Quad`, `Input`, `Pattern`) live here
//! alongside IRI and literal validation and the helpers that translate API
//! strings into interned handles. Validation happens at the API boundary;
//! internal code relies on already-validated handles.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const StringPool = @import("storage/string_pool.zig").StringPool;

pub const Handle = @import("storage/string_pool.zig").Handle;

/// An interned RDF term: IRI, blank node, or literal.
pub const Term = union(enum) {
    iri: Handle,
    blank_node: Handle,
    literal: struct {
        value: Handle,
        datatype: ?Handle = null,
        lang: ?Handle = null,
    },

    /// Return true when the term may appear in subject position.
    pub fn isSubject(self: Term) bool {
        return self != .literal;
    }

    /// Return the primary handle as a raw `u32` for use as an index key.
    pub fn key(self: Term) u32 {
        return switch (self) {
            .iri => |value| @intFromEnum(value),
            .blank_node => |value| @intFromEnum(value),
            .literal => |literal_value| @intFromEnum(literal_value.value),
        };
    }
};

/// An RDF quad: subject, predicate, object, and named graph.
pub const Quad = struct {
    subject: Term,
    predicate: Handle,
    object: Term,
    graph: Handle,
};

/// API-boundary literal with optional datatype IRI or language tag.
pub const LiteralInput = struct {
    value: []const u8,
    datatype: ?[]const u8 = null,
    lang: ?[]const u8 = null,
};

/// Non-allocator failures for `addTriple` and `addQuad`.
pub const StatementBoundaryError = error{
    InvalidPredicateIri,
    InvalidSubject,
    InvalidSubjectIri,
    InvalidObjectIri,
    InvalidGraphName,
    InvalidLiteral,
    InvalidBlankNodeLabel,
};

/// API-boundary term: an IRI string, blank node label, or literal.
pub const Input = union(enum) {
    iri: []const u8,
    blank_node: []const u8,
    literal: LiteralInput,
};

/// Match pattern with optional components. Null means unbound (wildcard).
pub const Pattern = struct {
    s: ?Input = null,
    p: ?[]const u8 = null,
    o: ?Input = null,
    g: ?[]const u8 = null,
};

/// Canonical IRI for the RDF default graph when APIs omit an explicit graph name.
pub const default_graph_iri = "urn:derive:default";

/// Structural absolute-IRI check (RFC 3986 scheme plus non-empty remainder).
/// This is not normalization or resolution.
fn isValidAbsoluteIriStructure(source: []const u8) bool {
    if (source.len == 0) return false;
    if (std.mem.startsWith(u8, source, "_:")) return false;
    if (source[0] == '"' or source[0] == '\'') return false;

    // RFC 3986 scheme: ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"
    if (!std.ascii.isAlphabetic(source[0])) return false;
    var index: usize = 1;
    while (index < source.len and source[index] != ':') {
        const character = source[index];
        if (!std.ascii.isAlphanumeric(character) and character != '+' and character != '-' and character != '.') return false;
        index += 1;
    }
    if (index >= source.len or source[index] != ':') return false;
    if (index + 1 >= source.len) return false;
    return true;
}

/// Predicate position: absolute IRI only (not a blank node).
pub fn isValidPredicateIri(source: []const u8) bool {
    return isValidAbsoluteIriStructure(source);
}

/// IRI term position (subject or object `.iri`): same structural rules as predicates.
pub fn isValidTermIri(source: []const u8) bool {
    return isValidAbsoluteIriStructure(source);
}

/// Named graph term: absolute IRI or blank-node label `_:…` per RDF 1.1.
pub fn isValidNamedGraphName(source: []const u8) bool {
    if (std.mem.startsWith(u8, source, "_:")) return source.len > 2;
    return isValidAbsoluteIriStructure(source);
}

/// RDF 1.1: a literal has a lexical form and either a language tag or a
/// datatype IRI, not both.
pub fn validateLiteralInput(literal_input: LiteralInput) StatementBoundaryError!void {
    if (literal_input.lang != null and literal_input.datatype != null) return error.InvalidLiteral;
    if (literal_input.datatype) |datatype| {
        if (!isValidPredicateIri(datatype)) return error.InvalidLiteral;
    }
    if (literal_input.lang) |language| {
        if (language.len == 0) return error.InvalidLiteral;
        if (!isValidLanguageTag(language)) return error.InvalidLiteral;
    }
}

/// Conservative BCP 47-style check: ASCII alphanumerics and hyphens, length
/// bounds, no leading, trailing, or doubled hyphens.
pub fn isValidLanguageTag(source: []const u8) bool {
    if (source.len == 0 or source.len > 64) return false;
    if (source[0] == '-' or source[source.len - 1] == '-') return false;
    for (source) |character| {
        if (std.ascii.isAlphanumeric(character) or character == '-') continue;
        return false;
    }
    if (std.mem.indexOf(u8, source, "--")) |_| return false;
    return true;
}

/// Validate that an input is acceptable in subject position (IRI or blank
/// node, not a literal).
pub fn validateSubjectInput(subject: Input) StatementBoundaryError!void {
    switch (subject) {
        .iri => |value| {
            if (!isValidTermIri(value)) return error.InvalidSubjectIri;
        },
        .blank_node => |value| {
            if (value.len == 0) return error.InvalidBlankNodeLabel;
        },
        .literal => return error.InvalidSubject,
    }
}

/// Validate that an input is acceptable in object position (IRI, blank node,
/// or literal).
pub fn validateObjectInput(object: Input) StatementBoundaryError!void {
    switch (object) {
        .iri => |value| {
            if (!isValidTermIri(value)) return error.InvalidObjectIri;
        },
        .blank_node => |value| {
            if (value.len == 0) return error.InvalidBlankNodeLabel;
        },
        .literal => |literal_input| try validateLiteralInput(literal_input),
    }
}

/// Intern an Input into the string pool, returning the corresponding Term.
pub fn internTerm(pool: *StringPool, input: Input) Allocator.Error!Term {
    assert(pool.entries.items.len < std.math.maxInt(u32));

    return switch (input) {
        .iri => |value| .{ .iri = try pool.intern(value) },
        .blank_node => |value| .{ .blank_node = try pool.intern(value) },
        .literal => |literal_input| .{ .literal = .{
            .value = try pool.intern(literal_input.value),
            .datatype = if (literal_input.datatype) |datatype| try pool.intern(datatype) else null,
            .lang = if (literal_input.lang) |language| try pool.intern(language) else null,
        } },
    };
}

/// Look up an Input in the string pool without interning. Returns null if
/// the key string is not present.
pub fn findTermKey(pool: *const StringPool, input: Input) ?u32 {
    return switch (input) {
        .iri => |value| if (pool.find(value)) |handle| @intFromEnum(handle) else null,
        .blank_node => |value| if (pool.find(value)) |handle| @intFromEnum(handle) else null,
        .literal => |literal_input| if (pool.find(literal_input.value)) |handle| @intFromEnum(handle) else null,
    };
}

const testing = std.testing;

test "predicate IRI accepts common schemes" {
    try testing.expect(isValidPredicateIri("http://example.org/p"));
    try testing.expect(isValidPredicateIri("https://example.org/p"));
    try testing.expect(isValidPredicateIri("urn:example:thing"));
    try testing.expect(isValidPredicateIri("file:///tmp/a"));
}

test "predicate IRI rejects invalid" {
    try testing.expect(!isValidPredicateIri(""));
    try testing.expect(!isValidPredicateIri("_:b1"));
    try testing.expect(!isValidPredicateIri("nocolon"));
    try testing.expect(!isValidPredicateIri("1http://x"));
    try testing.expect(!isValidPredicateIri("http:"));
    try testing.expect(!isValidPredicateIri("\"literal\""));
}

test "term IRI matches predicate rules" {
    try testing.expect(isValidTermIri("http://example.org/a"));
    try testing.expect(!isValidTermIri("_:a"));
}

test "named graph: IRI or blank" {
    try testing.expect(isValidNamedGraphName("http://example.org/g"));
    try testing.expect(isValidNamedGraphName("urn:derive:default"));
    try testing.expect(isValidNamedGraphName("_:g"));
    try testing.expect(!isValidNamedGraphName("_:"));
    try testing.expect(!isValidNamedGraphName("relative"));
}

test "language tag validation" {
    try testing.expect(isValidLanguageTag("en"));
    try testing.expect(isValidLanguageTag("en-US"));
    try testing.expect(isValidLanguageTag("zh-Hans"));
    try testing.expect(!isValidLanguageTag(""));
    try testing.expect(!isValidLanguageTag("-en"));
    try testing.expect(!isValidLanguageTag("en--US"));
    try testing.expect(!isValidLanguageTag("en "));
}

test "literal mutual exclusion" {
    try testing.expectError(error.InvalidLiteral, validateLiteralInput(.{
        .value = "hi",
        .lang = "en",
        .datatype = "http://www.w3.org/2001/XMLSchema#string",
    }));
}
