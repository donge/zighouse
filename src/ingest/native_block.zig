/// Minimal ClickHouse Native Block encoder for HTTP responses.
///
/// Used to answer clickhouse-go/v2 HTTP queries that require Native format.
/// Only supports a small subset: String and UInt32 columns, 1 row.
///
/// Wire format (revision >= 54454 = DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION):
///
///   BlockInfo:
///     uvarint(1)   -- field_num=1
///     uint8(0)     -- is_overflows = false
///     uvarint(2)   -- field_num=2
///     int32(-1)    -- bucket_num (LE)
///     uvarint(0)   -- end marker
///
///   uint64(num_columns) as uvarint
///   uint64(num_rows)    as uvarint
///
///   For each column:
///     string(name)         -- uvarint(len) + bytes
///     string(type_name)    -- uvarint(len) + bytes
///     uint8(0)             -- custom_serialization = false (revision >= 54454)
///     <column data>
///       String: for each row: uvarint(len) + bytes
///       UInt32: for each row: uint32 LE
///
///   Empty block at end (signals query complete):
///     BlockInfo + uvarint(0) + uvarint(0)

const std = @import("std");

/// Append a uvarint to buf.
fn putUVarInt(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try buf.append(allocator, @as(u8, @intCast((x & 0x7F) | 0x80)));
        x >>= 7;
    }
    try buf.append(allocator, @as(u8, @intCast(x)));
}

/// Append a ClickHouse string (uvarint len + bytes).
fn putString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try putUVarInt(buf, allocator, s.len);
    try buf.appendSlice(allocator, s);
}

/// Append block info (fixed 9 bytes).
fn putBlockInfo(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator) !void {
    try putUVarInt(buf, allocator, 1);      // field_num=1
    try buf.append(allocator, 0);           // is_overflows=false
    try putUVarInt(buf, allocator, 2);      // field_num=2
    // bucket_num = -1 as int32 LE
    try buf.appendSlice(allocator, &[4]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    try putUVarInt(buf, allocator, 0);      // end marker
}

pub const ColKind = enum { string, uint32, int64, uint64 };

pub const Col = struct {
    name: []const u8,
    kind: ColKind,
    str_val: []const u8 = "",  // for string columns
    u32_val: u32 = 0,           // for uint32 columns
    i64_val: i64 = 0,           // for int64 columns
    u64_val: u64 = 0,           // for uint64 columns
};

/// A descriptor row for DESCRIBE TABLE responses.
/// Encodes as 7 String columns: name, type, default_type, default_expression,
/// comment, codec_expression, ttl_expression.
pub const DescribeRow = struct {
    name: []const u8,
    type_name: []const u8,
};

/// Encode a DESCRIBE TABLE response: 7 String columns, one row per DescribeRow.
/// Returns heap-allocated bytes; caller must free.
pub fn encodeDescribeTable(allocator: std.mem.Allocator, rows: []const DescribeRow) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    try putBlockInfo(&buf, allocator);

    const num_cols: usize = 7;
    try putUVarInt(&buf, allocator, num_cols);
    try putUVarInt(&buf, allocator, rows.len);

    const col_names = [num_cols][]const u8{
        "name", "type", "default_type", "default_expression",
        "comment", "codec_expression", "ttl_expression",
    };
    for (col_names, 0..) |cname, ci| {
        try putString(&buf, allocator, cname);
        try putString(&buf, allocator, "String");
        try buf.append(allocator, 0); // custom_serialization=false
        // Write one value per row for this column
        for (rows) |row| {
            const val: []const u8 = switch (ci) {
                0 => row.name,
                1 => row.type_name,
                else => "",
            };
            try putString(&buf, allocator, val);
        }
    }

    // Empty terminator block
    try putBlockInfo(&buf, allocator);
    try putUVarInt(&buf, allocator, 0);
    try putUVarInt(&buf, allocator, 0);

    return buf.toOwnedSlice(allocator);
}

/// Encode a single-row Native block with the given columns.
/// Returns heap-allocated bytes; caller must free.
pub fn encodeOneRow(allocator: std.mem.Allocator, cols: []const Col) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    // Block info
    try putBlockInfo(&buf, allocator);

    // num_columns, num_rows
    try putUVarInt(&buf, allocator, cols.len);
    try putUVarInt(&buf, allocator, 1); // 1 row

    for (cols) |col| {
        try putString(&buf, allocator, col.name);
        switch (col.kind) {
            .string => {
                try putString(&buf, allocator, "String");
                try buf.append(allocator, 0); // custom_serialization=false
                try putString(&buf, allocator, col.str_val); // 1 row data
            },
            .uint32 => {
                try putString(&buf, allocator, "UInt32");
                try buf.append(allocator, 0); // custom_serialization=false
                // 1 row: uint32 LE
                const v = col.u32_val;
                try buf.appendSlice(allocator, &[4]u8{
                    @intCast(v & 0xFF),
                    @intCast((v >> 8) & 0xFF),
                    @intCast((v >> 16) & 0xFF),
                    @intCast((v >> 24) & 0xFF),
                });
            },
            .int64 => {
                try putString(&buf, allocator, "Int64");
                try buf.append(allocator, 0); // custom_serialization=false
                var tmp: [8]u8 = undefined;
                std.mem.writeInt(i64, &tmp, col.i64_val, .little);
                try buf.appendSlice(allocator, &tmp);
            },
            .uint64 => {
                try putString(&buf, allocator, "UInt64");
                try buf.append(allocator, 0); // custom_serialization=false
                var tmp: [8]u8 = undefined;
                std.mem.writeInt(u64, &tmp, col.u64_val, .little);
                try buf.appendSlice(allocator, &tmp);
            },
        }
    }

    // Empty block (end of results)
    try putBlockInfo(&buf, allocator);
    try putUVarInt(&buf, allocator, 0); // num_columns=0
    try putUVarInt(&buf, allocator, 0); // num_rows=0

    return buf.toOwnedSlice(allocator);
}

/// Encode an empty block (used for DDL/INSERT responses — signals success with 0 rows).
pub fn encodeEmpty(allocator: std.mem.Allocator) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    // Single empty block
    try putBlockInfo(&buf, allocator);
    try putUVarInt(&buf, allocator, 0); // num_columns=0
    try putUVarInt(&buf, allocator, 0); // num_rows=0

    return buf.toOwnedSlice(allocator);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "encodeEmpty produces valid block" {
    const allocator = std.testing.allocator;
    const bytes = try encodeEmpty(allocator);
    defer allocator.free(bytes);
    // Block info: uvarint(1)=1 + u8(0)=1 + uvarint(2)=1 + i32(-1 LE)=4 + uvarint(0)=1 = 8 bytes
    // Then: uvarint(0) + uvarint(0) = 2 bytes
    // Total = 10 bytes
    try std.testing.expectEqual(@as(usize, 10), bytes.len);
}

test "encodeOneRow: string + uint32" {
    const allocator = std.testing.allocator;
    const cols = [_]Col{
        .{ .name = "name", .kind = .string, .str_val = "ZigHouse" },
        .{ .name = "rev",  .kind = .uint32, .u32_val = 54460 },
    };
    const bytes = try encodeOneRow(allocator, &cols);
    defer allocator.free(bytes);
    try std.testing.expect(bytes.len > 0);
}
