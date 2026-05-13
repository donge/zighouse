//! Schema-driven column binding (PR-A1).
//!
//! `BoundColumn` is the dispatch-level runtime representation of a column,
//! produced by `bindColumn(native, name)`. It carries:
//!   - the column's CapabilityTag (derived from schema)
//!   - the column's name
//!   - typed slice / lowcard StringColumn ref / source handle
//!   - relevant capability flags from the schema
//!
//! BoundColumn does NOT own memory; all slices and pointers reference data
//! whose lifetime is tied to the underlying store / cache (mmap pages or
//! Native's internal cache).
//!
//! The `bindColumn` factory itself is implemented as a Native method (see
//! `src/native.zig`) because it must access Native's private getters (e.g.
//! getSearchPhraseColumn). This file declares the BoundColumn type and pure
//! schema lookup helpers; runtime binding lives next to its data source.

const std = @import("std");
const schema = @import("../schema.zig");
const lowcard = @import("../lowcard.zig");

/// Source handle for hash_text columns (URL/Title/...). The actual byte
/// slices are obtained on demand via existing Native APIs; this type only
/// records identity so dispatch can decide which operator to invoke.
pub const HashTextSource = struct {
    column_name: []const u8,
    /// Hash sidecar slice (always available for hash_text capability).
    hash: []const u64,
};

/// Source handle for lazy_text columns (Referer / generic text). First
/// version (PR-A1) carries only the name; operators that need bytes must
/// resolve them via Native's existing lazy APIs.
pub const LazyTextSource = struct {
    column_name: []const u8,
};

/// Runtime column binding tagged by CapabilityTag.
///
/// Variants intentionally mirror `schema.CapabilityTag`. Operators dispatch
/// on the active tag and consume the variant payload directly.
pub const BoundColumn = union(schema.CapabilityTag) {
    fixed_i16: struct {
        name: []const u8,
        values: []const i16,
    },
    fixed_i32: struct {
        name: []const u8,
        values: []const i32,
    },
    fixed_i64: struct {
        name: []const u8,
        values: []const i64,
    },
    fixed_date: struct {
        name: []const u8,
        values: []const i32,
    },
    fixed_timestamp: struct {
        name: []const u8,
        values: []const i64,
    },
    fixed_char: struct {
        name: []const u8,
        values: []const u8,
    },
    lowcard_text: struct {
        name: []const u8,
        column: *const lowcard.StringColumn,
        empty_id: ?u32,
        hash: ?[]const u64,
        capabilities: schema.StringCapabilities,
    },
    hash_text: struct {
        name: []const u8,
        source: HashTextSource,
        capabilities: schema.StringCapabilities,
    },
    lazy_text: struct {
        name: []const u8,
        source: LazyTextSource,
        capabilities: schema.StringCapabilities,
    },
    derived: struct {
        name: []const u8,
        expr: schema.DerivedExpr,
    },

    /// Returns the column name of any variant.
    pub fn name(self: BoundColumn) []const u8 {
        return switch (self) {
            inline else => |payload| payload.name,
        };
    }

    /// Returns the CapabilityTag of this binding (active union tag).
    pub fn tag(self: BoundColumn) schema.CapabilityTag {
        return std.meta.activeTag(self);
    }
};

/// Pure schema lookup: returns the CapabilityTag for a column name in a
/// table without touching any runtime data source. Useful for `inferShape`
/// (PR-A2) and other planning-time decisions.
pub fn lookupCapability(table: *const schema.Table, col_name: []const u8) ?schema.CapabilityTag {
    const idx = table.findColumn(col_name) orelse return null;
    return schema.capabilityTag(table.columns[idx]);
}

// Tests for this module live in `src/main.zig` (the unit_tests entry) so
// that they can resolve sibling imports (../schema.zig, ../lowcard.zig,
// ../clickbench/schema.zig). Compiling bind.zig as a standalone test root
// would put it outside the project module, breaking those imports.
