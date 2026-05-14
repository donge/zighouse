/// Write `primary.idx` for a ClickHouse MergeTree part.
///
/// The primary index stores the first row of each granule for each primary key
/// column, serialized in ClickHouse's native binary format (no compression —
/// the file is small enough to keep in memory).
///
/// For ZigHouse Phase-1, the primary key is always `(EventDate, CounterID)` or
/// a single first fixed-width column, depending on what the caller provides.
/// We store one entry per granule.
///
/// Binary layout (per entry, per column):
///   Fixed-width types: raw LE bytes (2, 4, or 8 bytes depending on type)
///   String type: varint-encoded length + UTF-8 bytes (CH LEB128)
///
/// All granule entries for column 0 are written first (interleaved = false for
/// primary.idx); actually CH writes them interleaved per-row across columns.
/// Correct format: for each granule row, write each PK column value in order.
///
/// Reference: MergeTreeIndexGranularity.cpp, IMergeTreeDataPart.cpp

const std = @import("std");
const schema = @import("schema");
const types = @import("types");

/// Write a varint (LEB128 unsigned) as used by ClickHouse for String lengths.
pub fn writeVarint(writer: *std.Io.Writer, value: u64) !void {
    var v = value;
    while (v >= 0x80) {
        const byte: u8 = @truncate((v & 0x7F) | 0x80);
        try writer.writeAll((&byte)[0..1]);
        v >>= 7;
    }
    const byte: u8 = @truncate(v);
    try writer.writeAll((&byte)[0..1]);
}

/// Read a varint (LEB128 unsigned).
pub fn readVarint(reader: *std.Io.Reader) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        var byte_buf: [1]u8 = undefined;
        try reader.readSliceAll(&byte_buf);
        const b = byte_buf[0];
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) break;
        shift += 7;
        if (shift >= 63) return error.VarintOverflow;
    }
    return result;
}

/// A primary key value for a single granule entry (union of supported types).
pub const PkValue = union(enum) {
    i16: i16,
    i32: i32,
    i64: i64,
    str: []const u8,
};

/// Write a single primary key value to `writer`.
pub fn writePkValue(writer: *std.Io.Writer, value: PkValue) !void {
    switch (value) {
        .i16 => |v| {
            var buf: [2]u8 = undefined;
            std.mem.writeInt(i16, &buf, v, .little);
            try writer.writeAll(&buf);
        },
        .i32 => |v| {
            var buf: [4]u8 = undefined;
            std.mem.writeInt(i32, &buf, v, .little);
            try writer.writeAll(&buf);
        },
        .i64 => |v| {
            var buf: [8]u8 = undefined;
            std.mem.writeInt(i64, &buf, v, .little);
            try writer.writeAll(&buf);
        },
        .str => |s| {
            try writeVarint(writer, s.len);
            try writer.writeAll(s);
        },
    }
}

/// A row of primary key values (one per PK column).
pub const PkRow = []const PkValue;

/// Write the primary.idx file: one PkRow per granule, each row written in
/// column order (interleaved per granule row).
pub fn write(writer: *std.Io.Writer, granule_rows: []const PkRow) !void {
    for (granule_rows) |row| {
        for (row) |val| {
            try writePkValue(writer, val);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "varint round-trip" {
    const cases = [_]u64{ 0, 1, 127, 128, 255, 300, 16383, 16384, 0xFFFFFFFF };
    for (cases) |v| {
        var buf: [16]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try writeVarint(&w, v);
        var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
        const got = try readVarint(&r);
        try std.testing.expectEqual(v, got);
    }
}

test "primary_idx write fixed types" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    // Two granule entries: (EventDate=i16(19000), CounterID=i32(57))
    const row0 = [_]PkValue{ .{ .i16 = 19000 }, .{ .i32 = 57 } };
    const row1 = [_]PkValue{ .{ .i16 = 19001 }, .{ .i32 = 58 } };
    const rows = [_]PkRow{ &row0, &row1 };
    try write(&w, &rows);

    const written = std.Io.Writer.buffered(&w);
    // Each entry: 2 bytes (i16) + 4 bytes (i32) = 6 bytes; 2 entries = 12 bytes
    try std.testing.expectEqual(@as(usize, 12), written.len);

    // Verify first row
    try std.testing.expectEqual(@as(i16, 19000), std.mem.readInt(i16, written[0..2], .little));
    try std.testing.expectEqual(@as(i32, 57), std.mem.readInt(i32, written[2..6], .little));
    try std.testing.expectEqual(@as(i16, 19001), std.mem.readInt(i16, written[6..8], .little));
    try std.testing.expectEqual(@as(i32, 58), std.mem.readInt(i32, written[8..12], .little));
}

test "primary_idx write string" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    const row = [_]PkValue{.{ .str = "hello" }};
    const rows = [_]PkRow{&row};
    try write(&w, &rows);

    const written = std.Io.Writer.buffered(&w);
    // varint(5) = 1 byte, "hello" = 5 bytes = 6 total
    try std.testing.expectEqual(@as(usize, 6), written.len);
    try std.testing.expectEqual(@as(u8, 5), written[0]); // varint(5)
    try std.testing.expectEqualStrings("hello", written[1..6]);
}
