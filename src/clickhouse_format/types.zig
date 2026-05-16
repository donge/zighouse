/// ClickHouse type name mapping for MergeTree part files.
///
/// Phase 1 supported types:
///   Int16, Int32, Int64, Date (Int16 days), DateTime (Int32 unix seconds), String
///
/// Reference: src/DataTypes/DataTypeFactory.cpp, IDataType.h

const std = @import("std");
const schema = @import("schema");

/// Returns the ClickHouse type name string for a given schema ColumnType.
/// The returned slice is a string literal (static lifetime).
pub fn chTypeName(ty: schema.ColumnType) []const u8 {
    return switch (ty) {
        .int8 => "Int8",
        .int16 => "Int16",
        .int32 => "Int32",
        .int64 => "Int64",
        .date => "Date",
        .timestamp => "DateTime",
        .text => "String",
        .char => "String", // CH has no single-byte Char type; map to String
        .float32 => "Float32",
        .float64 => "Float64",
    };
}

/// Returns the fixed byte width for a ClickHouse type as stored in a MergeTree
/// .bin file.  Returns null for variable-width types (String).
pub fn chFixedWidth(ty: schema.ColumnType) ?usize {
    return switch (ty) {
        .int8, .char => 1,
        .int16, .date => 2,
        .int32 => 4,
        .timestamp => 8,  // DateTime64 stored as 8-byte Int64
        .int64 => 8,
        .float32 => 4,
        .float64 => 8,
        .text => null,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "chTypeName covers all types" {
    const cases = .{
        .{ schema.ColumnType.int16, "Int16" },
        .{ schema.ColumnType.int32, "Int32" },
        .{ schema.ColumnType.int64, "Int64" },
        .{ schema.ColumnType.date, "Date" },
        .{ schema.ColumnType.timestamp, "DateTime" },
        .{ schema.ColumnType.text, "String" },
        .{ schema.ColumnType.char, "String" },
    };
    inline for (cases) |c| {
        try std.testing.expectEqualStrings(c[1], chTypeName(c[0]));
    }
}

test "chFixedWidth" {
    try std.testing.expectEqual(@as(?usize, 1), chFixedWidth(.int8));
    try std.testing.expectEqual(@as(?usize, 2), chFixedWidth(.int16));
    try std.testing.expectEqual(@as(?usize, 2), chFixedWidth(.date));
    try std.testing.expectEqual(@as(?usize, 4), chFixedWidth(.int32));
    try std.testing.expectEqual(@as(?usize, 8), chFixedWidth(.timestamp)); // DateTime64 = 8 bytes
    try std.testing.expectEqual(@as(?usize, 8), chFixedWidth(.int64));
    try std.testing.expectEqual(@as(?usize, null), chFixedWidth(.text));
    try std.testing.expectEqual(@as(?usize, 1), chFixedWidth(.char));
}
