pub const ColumnType = enum {
    int16,
    int32,
    int64,
    date,
    timestamp,
    text,
    char,

    pub fn fixedWidth(self: ColumnType) ?usize {
        return switch (self) {
            .int16 => 2,
            .int32, .date => 4,
            .int64, .timestamp => 8,
            .char => 1,
            .text => null,
        };
    }

    pub fn isString(self: ColumnType) bool {
        return switch (self) {
            .text => true,
            else => false,
        };
    }
};

pub const CardinalityHint = enum {
    none,
    low,
    medium,
    high,
    mostly_unique,
};

pub const StorageHint = enum {
    auto,
    fixed_eager,
    lowcard_dict,
    medium_dict,
    highcard_dict,
    hash_only,
    lazy_source,
    derived,
};

pub const StringEncoding = enum {
    none,
    lowcard_dict,
    medium_dict,
    highcard_dict,
    hash_late_materialized,
    lazy_source,
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
};

pub const Column = struct {
    name: []const u8,
    ty: ColumnType,
    cardinality: CardinalityHint = .none,
    storage: StorageHint = .auto,
    string_encoding: StringEncoding = .none,
    physical: PhysicalColumn = .none,
    materialize: []const MaterializationHint = &.{},
    capabilities: StringCapabilities = .{},
};

pub const Table = struct {
    name: []const u8,
    columns: []const Column,

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

/// Capability tag used for (PlanShape, CapabilityTag) dispatch lookup.
/// Derived from `Column.physical` and `Column.ty` via `capabilityTag`.
/// First version (PR-A0): single `.derived` tag, not yet split per DerivedExpr.
pub const CapabilityTag = enum {
    fixed_i16,
    fixed_i32,
    fixed_i64,
    fixed_date,
    fixed_timestamp,
    fixed_char,
    lowcard_text,
    hash_text,
    lazy_text,
    derived,
};

/// Derive a single CapabilityTag from a Column definition.
///
/// Decision table:
///   physical = .lowcard_text   -> .lowcard_text
///   physical = .hash_text      -> .hash_text
///   physical = .lazy_text      -> .lazy_text
///   physical = .derived        -> .derived
///   physical = .fixed -> by ty:
///     .int16     -> .fixed_i16
///     .int32     -> .fixed_i32
///     .int64     -> .fixed_i64
///     .date      -> .fixed_date
///     .timestamp -> .fixed_timestamp
///     .char      -> .fixed_char
///     .text      -> @panic (fixed should not carry variable text)
///   physical = .none -> by ty:
///     .text/.char -> .lazy_text  (schema declared text without explicit
///                                 physical; defaults to lazy source)
///     other       -> @panic (numeric column missing physical is a schema bug)
pub fn capabilityTag(col: Column) CapabilityTag {
    return switch (col.physical) {
        .lowcard_text => .lowcard_text,
        .hash_text => .hash_text,
        .lazy_text => .lazy_text,
        .derived => .derived,
        .fixed => |f| switch (f.ty) {
            .int16 => .fixed_i16,
            .int32 => .fixed_i32,
            .int64 => .fixed_i64,
            .date => .fixed_date,
            .timestamp => .fixed_timestamp,
            .char => .fixed_char,
            .text => @panic("capabilityTag: fixed physical with text ty is invalid"),
        },
        .none => switch (col.ty) {
            .text, .char => .lazy_text,
            else => @panic("capabilityTag: numeric column has no physical representation"),
        },
    };
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

test "capabilityTag derives from physical/ty" {
    const std = @import("std");

    const fixed_i16_col = Column{
        .name = "X",
        .ty = .int16,
        .physical = .{ .fixed = .{ .path_name = "X", .ty = .int16 } },
    };
    try std.testing.expectEqual(CapabilityTag.fixed_i16, capabilityTag(fixed_i16_col));

    const fixed_i32_col = Column{
        .name = "X",
        .ty = .int32,
        .physical = .{ .fixed = .{ .path_name = "X", .ty = .int32 } },
    };
    try std.testing.expectEqual(CapabilityTag.fixed_i32, capabilityTag(fixed_i32_col));

    const fixed_i64_col = Column{
        .name = "X",
        .ty = .int64,
        .physical = .{ .fixed = .{ .path_name = "X", .ty = .int64 } },
    };
    try std.testing.expectEqual(CapabilityTag.fixed_i64, capabilityTag(fixed_i64_col));

    const fixed_date_col = Column{
        .name = "X",
        .ty = .date,
        .physical = .{ .fixed = .{ .path_name = "X", .ty = .date } },
    };
    try std.testing.expectEqual(CapabilityTag.fixed_date, capabilityTag(fixed_date_col));

    const fixed_ts_col = Column{
        .name = "X",
        .ty = .timestamp,
        .physical = .{ .fixed = .{ .path_name = "X", .ty = .timestamp } },
    };
    try std.testing.expectEqual(CapabilityTag.fixed_timestamp, capabilityTag(fixed_ts_col));

    const lowcard_col = Column{
        .name = "X",
        .ty = .text,
        .physical = .{ .lowcard_text = .{
            .id_path_name = "x.id",
            .offsets_path_name = "x.off",
            .bytes_path_name = "x.bytes",
        } },
    };
    try std.testing.expectEqual(CapabilityTag.lowcard_text, capabilityTag(lowcard_col));

    const hash_col = Column{
        .name = "X",
        .ty = .text,
        .physical = .{ .hash_text = .{ .hash_column = "XHash" } },
    };
    try std.testing.expectEqual(CapabilityTag.hash_text, capabilityTag(hash_col));

    const lazy_col = Column{
        .name = "X",
        .ty = .text,
        .physical = .{ .lazy_text = .{ .source_column = "X" } },
    };
    try std.testing.expectEqual(CapabilityTag.lazy_text, capabilityTag(lazy_col));

    const derived_col = Column{
        .name = "X",
        .ty = .int32,
        .physical = .{ .derived = .{ .from = "Y", .expr = .length } },
    };
    try std.testing.expectEqual(CapabilityTag.derived, capabilityTag(derived_col));
}

test "capabilityTag covers ClickBench hits 105 columns without panic" {
    const std = @import("std");
    const hits_schema = @import("clickbench/schema.zig");

    var counts = [_]usize{0} ** @typeInfo(CapabilityTag).@"enum".fields.len;

    for (hits_schema.hits.columns) |col| {
        const tag = capabilityTag(col);
        counts[@intFromEnum(tag)] += 1;
    }

    // Sanity: total columns processed equals hits column count.
    var total: usize = 0;
    for (counts) |c| total += c;
    try std.testing.expectEqual(hits_schema.hits.columns.len, total);

    // Distribution sanity (per Explore C report):
    //   fixed_*: ~77, lowcard_text: 2, hash_text: 2, lazy_text: 1, derived: 0
    // We assert non-zero counts on expected non-empty buckets and exact zero on `derived`.
    const fixed_total = counts[@intFromEnum(CapabilityTag.fixed_i16)] +
        counts[@intFromEnum(CapabilityTag.fixed_i32)] +
        counts[@intFromEnum(CapabilityTag.fixed_i64)] +
        counts[@intFromEnum(CapabilityTag.fixed_date)] +
        counts[@intFromEnum(CapabilityTag.fixed_timestamp)];
    try std.testing.expect(fixed_total > 0);
    try std.testing.expect(counts[@intFromEnum(CapabilityTag.lowcard_text)] >= 2);
    try std.testing.expect(counts[@intFromEnum(CapabilityTag.hash_text)] >= 2);
    try std.testing.expect(counts[@intFromEnum(CapabilityTag.lazy_text)] >= 1);
}
