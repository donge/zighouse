
pub const ColumnType = enum {
    int8,
    int16,
    int32,
    int64,
    date,
    timestamp,
    text,
    char,
    float32,
    float64,
    /// LowCardinality(T) — inner type stored in Column.low_card_inner.
    low_card,

    pub fn fixedWidth(self: ColumnType) ?usize {
        return switch (self) {
            .int8, .char => 1,
            .int16 => 2,
            .int32, .date, .float32 => 4,
            .int64, .timestamp, .float64 => 8,
            .text, .low_card => null,
        };
    }

    pub fn isString(self: ColumnType) bool {
        return switch (self) {
            .text, .low_card => true,
            else => false,
        };
    }
};

pub const EmptySemantics = enum {
    none,
    stored_empty_string,
    id_zero,
};

pub const DerivedExpr = enum {
    length,
    hash,
    event_minute,
    domain_from_url,
    date_trunc_minute,
};

pub const PhysicalColumn = union(enum) {
    none,
    fixed: struct {
        path_name: []const u8,
        ty: ColumnType,
    },
    lowcard_text: struct {
        id_path_name: []const u8,
        offsets_path_name: []const u8,
        bytes_path_name: []const u8,
        id_type: ColumnType = .int32,
        empty: EmptySemantics = .stored_empty_string,
        /// Optional parallel hash sidecar column (e.g. URLHash/TitleHash).
        /// When set together with `capabilities.hash_sidecar`, dispatch may
        /// choose the hashed_late_materialize_top operator.
        hash_column: ?[]const u8 = null,
    },
    hash_text: struct {
        hash_column: []const u8,
        dict_path_name: ?[]const u8 = null,
        id_path_name: ?[]const u8 = null,
        offsets_path_name: ?[]const u8 = null,
        bytes_path_name: ?[]const u8 = null,
        empty: EmptySemantics = .stored_empty_string,
    },
    lazy_text: struct {
        source_column: []const u8,
        hash_column: ?[]const u8 = null,
        sidecar_path_name: ?[]const u8 = null,
        empty: EmptySemantics = .stored_empty_string,
    },
    derived: struct {
        from: []const u8,
        expr: DerivedExpr,
        path_name: ?[]const u8 = null,
    },
};

pub const MaterializationHint = enum {
    fixed_hot_column,
    lowcard_dictionary,
    hash_column,
    hash_to_string_dict,
    contains_index,
    length_column,
    lazy_source_sidecar,
    domain_dictionary,
    result_sidecar,
};

pub const StringCapabilities = struct {
    count_distinct: bool = false,
    group_count_top: bool = false,
    group_distinct_user_top: bool = false,
    group_with_fixed_key: bool = false,
    order_by_value: bool = false,
    order_by_time: bool = false,
    contains_index: bool = false,
    min_value: bool = false,
    length: bool = false,
    late_materialize: bool = false,
    domain_extract: bool = false,
    conditional_materialize: bool = false,
    /// Column has a parallel hash sidecar (e.g. URLHash/TitleHash) suitable
    /// for late-materialization GROUP BY. Set on lowcard_text columns whose
    /// physical layout is dictionary-encoded but whose preferred dispatch
    /// path is hashed_late_materialize_top (PR-A4).
    hash_sidecar: bool = false,
};

pub const Column = struct {
    name: []const u8,
    ty: ColumnType,
    /// When ty == .low_card, the inner (wrapped) type.  Defaults to .text.
    low_card_inner: ColumnType = .text,
    /// Original ClickHouse type string as received on the wire (e.g. "Array(String)",
    /// "Map(String, Float64)", "LowCardinality(String)", "IPv6").
    /// null means unknown / use schemaTypeToChType(ty) fallback.
    ch_type: ?[]const u8 = null,
    physical: PhysicalColumn = .none,
    materialize: []const MaterializationHint = &.{},
    capabilities: StringCapabilities = .{},

    /// Return the inner type for LowCardinality columns, or self.ty for others.
    pub fn lowCardInner(self: Column) ColumnType {
        return if (self.ty == .low_card) self.low_card_inner else self.ty;
    }
};

pub const Table = struct {
    name: []const u8,
    columns: []const Column,
    /// Columns whose data is stored in globally-sorted (non-decreasing) order
    /// within a single part.  Used for binary-search row-range pushdown at
    /// query time: an equality filter `col = val` on a sort key column is
    /// resolved by binary-searching the raw column data to find [lo, hi) and
    /// restricting all scans to that window.
    sort_keys: []const []const u8 = &.{},

    pub fn findColumn(self: Table, name: []const u8) ?usize {
        for (self.columns, 0..) |column, i| {
            if (asciiEqlIgnoreCase(column.name, name)) return i;
        }
        return null;
    }
};

pub fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (asciiLower(ca) != asciiLower(cb)) return false;
    }
    return true;
}

fn asciiLower(c: u8) u8 {
    if (c >= 'A' and c <= 'Z') return c + 32;
    return c;
}

test "finds columns case insensitively" {
    const std = @import("std");
    const columns = [_]Column{
        .{ .name = "Id", .ty = .int64 },
        .{ .name = "TextValue", .ty = .text },
    };
    const table = Table{ .name = "test", .columns = &columns };
    try std.testing.expectEqual(@as(?usize, 0), table.findColumn("id"));
    try std.testing.expectEqual(@as(?usize, 1), table.findColumn("TextValue"));
    try std.testing.expectEqual(@as(?usize, null), table.findColumn("missing"));
}


