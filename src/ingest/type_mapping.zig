const std = @import("std");
const schema = @import("schema");

pub const Mode = enum {
    ddl,
    schema_config,
    wire,
};

pub fn parseType(s: []const u8, mode: Mode) ?schema.ColumnType {
    if (starts(s, "SimpleAggregateFunction(")) {
        const inner = simpleAggregateInnerType(s) orelse return .text;
        return parseType(inner, mode);
    }
    if (starts(s, "AggregateFunction(")) return .text;

    if (eq(s, "Int8")) return .int8;
    if (eq(s, "Int16")) return .int16;
    if (eq(s, "Int32")) return .int32;
    if (eq(s, "Int64")) return .int64;
    if (eq(s, "UInt8")) return .int8;
    if (eq(s, "UInt16")) return .int16;
    if (eq(s, "UInt32")) return .int32;
    if (eq(s, "UInt64")) return .int64;
    if (eq(s, "Date")) return .date;
    if (eq(s, "Date32")) return .date;
    if (eq(s, "DateTime")) return if (mode == .wire) .int32 else .timestamp;
    if (starts(s, "DateTime(")) return if (mode == .wire) .int32 else .timestamp;
    if (eq(s, "DateTime64") or starts(s, "DateTime64(")) return .timestamp;
    if (eq(s, "String") or eq(s, "FixedString") or starts(s, "FixedString(")) return .text;
    if (eq(s, "IPv4") or eq(s, "IPv6") or eq(s, "UUID")) return .text;
    if (eq(s, "Float32")) return .float32;
    if (eq(s, "Float64")) return .float64;
    if (eq(s, "Bool") or eq(s, "Boolean")) return .int8;

    if (eq(s, "INT") or eq(s, "INTEGER")) return .int32;
    if (eq(s, "SMALLINT")) return .int16;
    if (eq(s, "BIGINT")) return .int64;
    if (eq(s, "TINYINT")) return .int8;
    if (eq(s, "FLOAT") or eq(s, "REAL")) return .float32;
    if (eq(s, "DOUBLE") or eq(s, "DECIMAL") or eq(s, "NUMERIC") or eq(s, "DEC")) return .float64;
    if (eq(s, "VARCHAR") or eq(s, "CHAR") or eq(s, "CHARACTER") or
        eq(s, "TEXT") or eq(s, "BLOB") or eq(s, "CLOB") or eq(s, "NAME") or
        eq(s, "NCHAR")) return .text;
    if (eq(s, "TIME") or eq(s, "TIMESTAMP")) return .timestamp;

    if (starts(s, "Nullable(")) return parseType(innerType(s) orelse return null, mode);
    if (starts(s, "LowCardinality(")) {
        if (mode == .schema_config) return .low_card;
        return parseType(innerType(s) orelse return .text, mode);
    }
    if (starts(s, "Array(") or starts(s, "Map(")) return .text;
    if (starts(s, "Tuple(")) return if (mode == .wire) null else .text;

    if (starts(s, "Decimal")) {
        if (mode == .wire) {
            if (starts(s, "Decimal32")) return .float32;
            if (starts(s, "Decimal64")) return .float64;
            if (starts(s, "Decimal128") or starts(s, "Decimal256")) return .text;
            const body = innerType(s) orelse return .float64;
            const comma = std.mem.indexOfScalar(u8, body, ',') orelse body.len;
            const precision = std.fmt.parseInt(u32, std.mem.trim(u8, body[0..comma], " \t"), 10) catch 19;
            if (precision <= 9) return .float32;
            if (precision <= 18) return .float64;
            return .text;
        }
        return .float64;
    }

    if (starts(s, "Enum8(")) return if (mode == .wire) .int8 else .text;
    if (starts(s, "Enum16(")) return if (mode == .wire) .int16 else .text;
    return null;
}

pub fn innerType(s: []const u8) ?[]const u8 {
    const open = std.mem.indexOfScalar(u8, s, '(') orelse return null;
    if (s.len <= open + 1 or s[s.len - 1] != ')') return null;
    return std.mem.trim(u8, s[open + 1 .. s.len - 1], " \t\r\n");
}

pub fn wireFixedWidth(s: []const u8) ?usize {
    if (starts(s, "SimpleAggregateFunction(")) {
        return wireFixedWidth(simpleAggregateInnerType(s) orelse return null);
    }
    if (eq(s, "IPv4")) return 4;
    if (eq(s, "IPv6") or eq(s, "UUID")) return 16;
    if (eq(s, "UInt8") or eq(s, "Int8") or eq(s, "Bool") or eq(s, "Boolean")) return 1;
    if (eq(s, "UInt16") or eq(s, "Int16") or eq(s, "Date")) return 2;
    if (eq(s, "UInt32") or eq(s, "Int32") or eq(s, "Float32") or eq(s, "Date32") or
        eq(s, "DateTime")) return 4;
    if (eq(s, "UInt64") or eq(s, "Int64") or eq(s, "Float64") or starts(s, "DateTime64")) return 8;
    if (starts(s, "Decimal32")) return 4;
    if (starts(s, "Decimal64")) return 8;
    if (starts(s, "Decimal128")) return 16;
    if (starts(s, "Decimal256")) return 32;
    if (starts(s, "FixedString(")) {
        return std.fmt.parseInt(usize, innerType(s) orelse return null, 10) catch null;
    }
    return null;
}

fn simpleAggregateInnerType(s: []const u8) ?[]const u8 {
    const body = innerType(s) orelse return null;
    var depth: usize = 0;
    for (body, 0..) |c, i| {
        if (c == '(') {
            depth += 1;
        } else if (c == ')') {
            if (depth > 0) depth -= 1;
        } else if (c == ',' and depth == 0) {
            return std.mem.trim(u8, body[i + 1 ..], " \t\r\n");
        }
    }
    return null;
}

inline fn eq(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

inline fn starts(s: []const u8, prefix: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(s, prefix);
}

test "type mapping keeps storage and wire DateTime semantics explicit" {
    try std.testing.expectEqual(schema.ColumnType.timestamp, parseType("DateTime", .ddl).?);
    try std.testing.expectEqual(schema.ColumnType.timestamp, parseType("DateTime", .schema_config).?);
    try std.testing.expectEqual(schema.ColumnType.int32, parseType("DateTime", .wire).?);
    try std.testing.expectEqual(schema.ColumnType.timestamp, parseType("DateTime64(3)", .wire).?);
}

test "type mapping handles wrappers and common aliases" {
    try std.testing.expectEqual(schema.ColumnType.int32, parseType("Nullable(INTEGER)", .schema_config).?);
    try std.testing.expectEqual(schema.ColumnType.low_card, parseType("LowCardinality(VARCHAR)", .schema_config).?);
    try std.testing.expectEqual(schema.ColumnType.text, parseType("LowCardinality(VARCHAR)", .ddl).?);
    try std.testing.expectEqual(schema.ColumnType.int64, parseType("SimpleAggregateFunction(sum, UInt64)", .wire).?);
    try std.testing.expectEqual(schema.ColumnType.float32, parseType("Decimal32(9)", .wire).?);
    try std.testing.expectEqual(schema.ColumnType.float64, parseType("Decimal32(9)", .schema_config).?);
}

test "type mapping reports ClickHouse wire fixed widths" {
    try std.testing.expectEqual(@as(?usize, 16), wireFixedWidth("UUID"));
    try std.testing.expectEqual(@as(?usize, 4), wireFixedWidth("IPv4"));
    try std.testing.expectEqual(@as(?usize, 16), wireFixedWidth("IPv6"));
    try std.testing.expectEqual(@as(?usize, 1), wireFixedWidth("Bool"));
    try std.testing.expectEqual(@as(?usize, 4), wireFixedWidth("DateTime"));
    try std.testing.expectEqual(@as(?usize, 8), wireFixedWidth("DateTime64(3)"));
    try std.testing.expectEqual(@as(?usize, 12), wireFixedWidth("FixedString(12)"));
    try std.testing.expectEqual(@as(?usize, 8), wireFixedWidth("SimpleAggregateFunction(sum, UInt64)"));
    try std.testing.expectEqual(@as(?usize, null), wireFixedWidth("String"));
}
