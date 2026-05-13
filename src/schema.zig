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
