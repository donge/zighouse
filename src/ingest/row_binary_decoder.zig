/// ClickHouse RowBinary / RowBinaryWithNamesAndTypes decoder.
///
/// RowBinary format (per row, in schema column order):
///   Int16      — 2 bytes LE signed
///   Int32      — 4 bytes LE signed
///   Int64      — 8 bytes LE signed
///   Date       — 2 bytes LE UInt16 (days since 1970-01-01)
///   DateTime   — 4 bytes LE UInt32 (unix seconds)
///   String     — varUInt(len) + raw bytes
///
/// RowBinaryWithNamesAndTypes prefix (before the rows):
///   varUInt(num_columns)
///   for each column: varUInt(name_len) + name_bytes
///   for each column: varUInt(type_len) + type_bytes
///
/// Usage (RowBinary — schema must be known ahead of time):
///   var dec = try RowBinaryDecoder.init(allocator, schema_table);
///   defer dec.deinit();
///   const n = try dec.decode(raw_bytes);
///
/// Usage (RowBinaryWithNamesAndTypes — schema extracted from payload):
///   const result = try decodeWithHeader(allocator, raw_bytes);
///   defer result.deinit(allocator);
///   // result.table  — inferred schema.Table (name = "")
///   // result.decoder.columns — decoded column buffers

const std = @import("std");
const schema = @import("schema");

pub const MAX_STRING_LEN: usize = 128 * 1024 * 1024;

/// Per-column accumulation buffer.
pub const ColumnBuffer = struct {
    col: schema.Column,
    /// Fixed-width columns: i64 values (all fixed types widened to i64 internally).
    fixed_vals: std.ArrayListUnmanaged(i64),
    /// String columns: slices into str_bytes (no separate allocation per value).
    str_vals: std.ArrayListUnmanaged([]const u8),
    /// Backing store for string bytes.
    str_bytes: std.ArrayListUnmanaged(u8),

    fn init(col: schema.Column) ColumnBuffer {
        return .{
            .col = col,
            .fixed_vals = .empty,
            .str_vals = .empty,
            .str_bytes = .empty,
        };
    }

    pub fn deinit(self: *ColumnBuffer, allocator: std.mem.Allocator) void {
        self.fixed_vals.deinit(allocator);
        self.str_vals.deinit(allocator);
        self.str_bytes.deinit(allocator);
    }

    pub fn rowCount(self: *const ColumnBuffer) usize {
        return switch (self.col.ty) {
            .text, .char => self.str_vals.items.len,
            else => self.fixed_vals.items.len,
        };
    }
};

/// Decodes a complete RowBinary buffer into per-column buffers.
/// All bytes must be available at once (no streaming mid-row).
pub const RowBinaryDecoder = struct {
    allocator: std.mem.Allocator,
    table: schema.Table,
    columns: []ColumnBuffer,

    pub fn init(allocator: std.mem.Allocator, table: schema.Table) !RowBinaryDecoder {
        const columns = try allocator.alloc(ColumnBuffer, table.columns.len);
        for (table.columns, columns) |col, *buf| {
            buf.* = ColumnBuffer.init(col);
        }
        return .{
            .allocator = allocator,
            .table = table,
            .columns = columns,
        };
    }

    pub fn deinit(self: *RowBinaryDecoder) void {
        for (self.columns) |*col| col.deinit(self.allocator);
        self.allocator.free(self.columns);
    }

    /// Reset buffers for reuse without re-allocating.
    pub fn reset(self: *RowBinaryDecoder) void {
        for (self.columns) |*col| {
            col.fixed_vals.items.len = 0;
            col.str_vals.items.len = 0;
            col.str_bytes.items.len = 0;
        }
    }

    /// Decode a complete RowBinary payload.
    /// Appends decoded values into self.columns.
    /// Returns the number of rows decoded.
    pub fn decode(self: *RowBinaryDecoder, data: []const u8) !usize {
        var pos: usize = 0;
        var rows: usize = 0;

        while (pos < data.len) {
            for (self.table.columns, self.columns) |col, *buf| {
                switch (col.ty) {
                    .int16 => {
                        if (pos + 2 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i16, data[pos..][0..2], .little);
                        try buf.fixed_vals.append(self.allocator, @as(i64, v));
                        pos += 2;
                    },
                    .int32, .date => {
                        if (pos + 4 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i32, data[pos..][0..4], .little);
                        try buf.fixed_vals.append(self.allocator, @as(i64, v));
                        pos += 4;
                    },
                    .int64, .timestamp => {
                        if (pos + 8 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i64, data[pos..][0..8], .little);
                        try buf.fixed_vals.append(self.allocator, v);
                        pos += 8;
                    },
                    .text, .char => {
                        const len, const var_bytes = readVarUInt(data[pos..]) orelse
                            return error.UnexpectedEndOfData;
                        pos += var_bytes;
                        if (len > MAX_STRING_LEN) return error.StringTooLong;
                        if (pos + len > data.len) return error.UnexpectedEndOfData;
                        const start = buf.str_bytes.items.len;
                        try buf.str_bytes.appendSlice(self.allocator, data[pos..][0..len]);
                        try buf.str_vals.append(self.allocator, buf.str_bytes.items[start..]);
                        pos += len;
                    },
                }
            }
            rows += 1;
        }

        return rows;
    }
};

/// Read a ClickHouse varUInt from `buf`.
/// Returns .{value, bytes_consumed} or null if buffer is too short.
pub fn readVarUInt(buf: []const u8) ?struct { usize, usize } {
    var result: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < buf.len and i < 9) : (i += 1) {
        const b = buf[i];
        result |= @as(usize, b & 0x7F) << shift;
        shift += 7;
        if (b & 0x80 == 0) return .{ result, i + 1 };
    }
    return null;
}

// ── RowBinaryWithNamesAndTypes ─────────────────────────────────────────────────

pub const WithHeaderResult = struct {
    table: schema.Table,
    decoder: RowBinaryDecoder,

    pub fn deinit(self: *WithHeaderResult, allocator: std.mem.Allocator) void {
        for (self.table.columns) |col| allocator.free(col.name);
        allocator.free(self.table.columns);
        self.decoder.deinit();
    }
};

/// Parse a RowBinaryWithNamesAndTypes payload.
/// Reads the header (column names + types) then decodes the row data.
/// The returned table name is empty string ""; caller should set it.
/// Caller must call result.deinit(allocator).
pub fn decodeWithHeader(allocator: std.mem.Allocator, data: []const u8) !WithHeaderResult {
    var pos: usize = 0;

    // num_columns
    const num_cols, const nc_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
    pos += nc_bytes;
    if (num_cols == 0) return error.NoColumnsInHeader;

    // Read column names
    const col_names = try allocator.alloc([]u8, num_cols);
    var names_read: usize = 0;
    errdefer {
        for (col_names[0..names_read]) |n| allocator.free(n);
        allocator.free(col_names);
    }
    for (col_names) |*name| {
        const len, const lb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += lb;
        if (pos + len > data.len) return error.UnexpectedEndOfData;
        name.* = try allocator.dupe(u8, data[pos .. pos + len]);
        names_read += 1;
        pos += len;
    }

    // Read column types → schema.ColumnType
    const col_types = try allocator.alloc(schema.ColumnType, num_cols);
    defer allocator.free(col_types);

    for (col_types) |*ty| {
        const len, const lb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += lb;
        if (pos + len > data.len) return error.UnexpectedEndOfData;
        const type_str = data[pos .. pos + len];
        pos += len;
        ty.* = parseChType(type_str) orelse return error.UnsupportedColumnType;
    }

    // Build schema.Table columns
    const columns = try allocator.alloc(schema.Column, num_cols);
    errdefer allocator.free(columns);
    for (columns, col_names, col_types) |*col, name, ty| {
        col.* = .{ .name = name, .ty = ty };
    }
    allocator.free(col_names); // slice itself (names owned by columns now)

    const table = schema.Table{ .name = "", .columns = columns };

    // Decode row data (remainder of buffer)
    var dec = try RowBinaryDecoder.init(allocator, table);
    errdefer dec.deinit();
    _ = try dec.decode(data[pos..]);

    return .{ .table = table, .decoder = dec };
}

/// Map a ClickHouse type string to our schema.ColumnType.
/// Handles Nullable(T) and LowCardinality(T) wrappers.
fn parseChType(s: []const u8) ?schema.ColumnType {
    if (chTypeEql(s, "Int16")) return .int16;
    if (chTypeEql(s, "Int32")) return .int32;
    if (chTypeEql(s, "Int64")) return .int64;
    if (chTypeEql(s, "UInt16")) return .date;
    if (chTypeEql(s, "UInt32")) return .timestamp;
    if (chTypeEql(s, "Date")) return .date;
    if (chTypeEql(s, "Date32")) return .date;
    if (chTypeEql(s, "DateTime")) return .timestamp;
    if (chTypeEql(s, "String")) return .text;
    if (chTypeEql(s, "FixedString")) return .text;
    // Nullable(T) / LowCardinality(T)
    if (chTypeStartsWith(s, "Nullable(") or chTypeStartsWith(s, "LowCardinality(")) {
        const inner = extractInner(s);
        return parseChType(inner);
    }
    // DateTime64(precision) / DateTime64(precision, tz)
    if (chTypeStartsWith(s, "DateTime64")) return .timestamp;
    // FixedString(N)
    if (chTypeStartsWith(s, "FixedString(")) return .text;
    return null;
}

fn chTypeEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    return std.mem.eql(u8, a, b);
}

fn chTypeStartsWith(s: []const u8, prefix: []const u8) bool {
    return std.mem.startsWith(u8, s, prefix);
}

fn extractInner(s: []const u8) []const u8 {
    const lp = std.mem.indexOfScalar(u8, s, '(') orelse return s;
    const rp = std.mem.lastIndexOfScalar(u8, s, ')') orelse return s;
    if (rp > lp) return s[lp + 1 .. rp];
    return s;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "decode: two rows Int32 + String" {
    const allocator = std.testing.allocator;

    const table = schema.Table{
        .name = "t",
        .columns = &.{
            .{ .name = "id", .ty = .int32 },
            .{ .name = "name", .ty = .text },
        },
    };

    // Build RowBinary manually: row1=(42,"hello"), row2=(100,"world")
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    // row1: id=42
    std.mem.writeInt(i32, buf[pos..][0..4], 42, .little);
    pos += 4;
    // row1: name="hello" (varUInt 5 + bytes)
    buf[pos] = 5; pos += 1;
    @memcpy(buf[pos..][0..5], "hello"); pos += 5;
    // row2: id=100
    std.mem.writeInt(i32, buf[pos..][0..4], 100, .little);
    pos += 4;
    // row2: name="world"
    buf[pos] = 5; pos += 1;
    @memcpy(buf[pos..][0..5], "world"); pos += 5;
    const data = buf[0..pos];

    var dec = try RowBinaryDecoder.init(allocator, table);
    defer dec.deinit();

    const rows = try dec.decode(data);
    try std.testing.expectEqual(@as(usize, 2), rows);
    try std.testing.expectEqual(@as(i64, 42), dec.columns[0].fixed_vals.items[0]);
    try std.testing.expectEqual(@as(i64, 100), dec.columns[0].fixed_vals.items[1]);
    try std.testing.expectEqualSlices(u8, "hello", dec.columns[1].str_vals.items[0]);
    try std.testing.expectEqualSlices(u8, "world", dec.columns[1].str_vals.items[1]);
}

test "readVarUInt: single byte" {
    const r = readVarUInt(&.{0x05}).?;
    try std.testing.expectEqual(@as(usize, 5), r.@"0");
    try std.testing.expectEqual(@as(usize, 1), r.@"1");
}

test "readVarUInt: two bytes (300)" {
    const r = readVarUInt(&.{ 0xAC, 0x02 }).?;
    try std.testing.expectEqual(@as(usize, 300), r.@"0");
    try std.testing.expectEqual(@as(usize, 2), r.@"1");
}

test "decodeWithHeader: Int32 + String two rows" {
    const allocator = std.testing.allocator;

    // Build RowBinaryWithNamesAndTypes payload manually.
    var buf: [256]u8 = undefined;
    var pos: usize = 0;

    // num_columns = 2
    buf[pos] = 2; pos += 1;
    // names: "id", "name" (all names first)
    buf[pos] = 2; pos += 1; @memcpy(buf[pos..][0..2], "id"); pos += 2;
    buf[pos] = 4; pos += 1; @memcpy(buf[pos..][0..4], "name"); pos += 4;
    // types: "Int32", "String" (all types after all names)
    buf[pos] = 5; pos += 1; @memcpy(buf[pos..][0..5], "Int32"); pos += 5;
    buf[pos] = 6; pos += 1; @memcpy(buf[pos..][0..6], "String"); pos += 6;
    // row1: id=7, name="alice"
    std.mem.writeInt(i32, buf[pos..][0..4], 7, .little); pos += 4;
    buf[pos] = 5; pos += 1; @memcpy(buf[pos..][0..5], "alice"); pos += 5;
    // row2: id=8, name="bob"
    std.mem.writeInt(i32, buf[pos..][0..4], 8, .little); pos += 4;
    buf[pos] = 3; pos += 1; @memcpy(buf[pos..][0..3], "bob"); pos += 3;

    var result = try decodeWithHeader(allocator, buf[0..pos]);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.table.columns.len);
    try std.testing.expectEqualStrings("id",   result.table.columns[0].name);
    try std.testing.expectEqual(schema.ColumnType.int32, result.table.columns[0].ty);
    try std.testing.expectEqualStrings("name", result.table.columns[1].name);
    try std.testing.expectEqual(schema.ColumnType.text,  result.table.columns[1].ty);
    try std.testing.expectEqual(@as(usize, 2), result.decoder.columns[0].fixed_vals.items.len);
    try std.testing.expectEqual(@as(i64, 7), result.decoder.columns[0].fixed_vals.items[0]);
    try std.testing.expectEqual(@as(i64, 8), result.decoder.columns[0].fixed_vals.items[1]);
    try std.testing.expectEqualSlices(u8, "alice", result.decoder.columns[1].str_vals.items[0]);
    try std.testing.expectEqualSlices(u8, "bob",   result.decoder.columns[1].str_vals.items[1]);
}

test "decodeWithHeader: truncated header triggers errdefer (no leak)" {
    // Provide only the num_cols byte — no names follow.
    // The errdefer path must clean up without double-free or leak.
    const allocator = std.testing.allocator;
    const data = [_]u8{0x02}; // num_cols=2, then EOF
    try std.testing.expectError(error.UnexpectedEndOfData, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: partial name triggers errdefer (no leak)" {
    // num_cols=1, name_len=5 but only 3 bytes of name provided.
    const allocator = std.testing.allocator;
    const data = [_]u8{ 0x01, 0x05, 'a', 'b', 'c' };
    try std.testing.expectError(error.UnexpectedEndOfData, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: unsupported type triggers errdefer (no leak)" {
    // num_cols=1, name="x", type="Float64" (unsupported)
    const allocator = std.testing.allocator;
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 1; pos += 1;           // num_cols=1
    buf[pos] = 1; pos += 1; buf[pos] = 'x'; pos += 1; // name="x"
    buf[pos] = 7; pos += 1;
    @memcpy(buf[pos..][0..7], "Float64"); pos += 7;   // type="Float64"
    try std.testing.expectError(error.UnsupportedColumnType, decodeWithHeader(allocator, buf[0..pos]));
}

test "decodeWithHeader: zero columns returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{0x00}; // num_cols=0
    try std.testing.expectError(error.NoColumnsInHeader, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: all Phase-1 types" {
    // Build payload: num_cols=5, names+types for all fixed types, then one row.
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;
    var pos: usize = 0;

    buf[pos] = 5; pos += 1; // num_cols
    // names (all first)
    const names = [_][]const u8{ "i16", "i32", "i64", "d", "ts" };
    for (names) |n| {
        buf[pos] = @intCast(n.len); pos += 1;
        @memcpy(buf[pos..][0..n.len], n); pos += n.len;
    }
    // types
    const types = [_][]const u8{ "Int16", "Int32", "Int64", "Date", "DateTime" };
    for (types) |t| {
        buf[pos] = @intCast(t.len); pos += 1;
        @memcpy(buf[pos..][0..t.len], t); pos += t.len;
    }
    // one row: i16=1, i32=2, i64=3, d=4 (UInt16 days), ts=5 (UInt32 secs)
    std.mem.writeInt(i16, buf[pos..][0..2], 1, .little); pos += 2;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .little); pos += 4;
    std.mem.writeInt(i64, buf[pos..][0..8], 3, .little); pos += 8;
    std.mem.writeInt(i32, buf[pos..][0..4], 4, .little); pos += 4; // Date stored as i32 in RowBinary
    std.mem.writeInt(i64, buf[pos..][0..8], 5, .little); pos += 8; // DateTime stored as i64

    var result = try decodeWithHeader(allocator, buf[0..pos]);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), result.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int16,     result.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.int32,     result.table.columns[1].ty);
    try std.testing.expectEqual(schema.ColumnType.int64,     result.table.columns[2].ty);
    try std.testing.expectEqual(schema.ColumnType.date,      result.table.columns[3].ty);
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.table.columns[4].ty);
}
