/// ClickHouse RowBinary encoder.
///
/// RowBinary format: rows are written sequentially with no header.
/// Each row is a concatenation of column values in schema order:
///   Int16/Int32/Int64  — little-endian LE
///   Date               — UInt16 LE (days since 1970-01-01, same epoch as i32 zighouse Date)
///   DateTime           — UInt32 LE (unix seconds, same as i64 zighouse Timestamp)
///   String             — varUInt byte length + raw UTF-8 bytes
///
/// Usage:
///   var enc = RowBinaryEncoder.init(writer);
///   try enc.writeInt16(-3);
///   try enc.writeInt32(42);
///   try enc.writeString("hello");
///   // repeat for each row

const std = @import("std");

/// Maximum allowed string length (128 MiB) — guards against OOM on bad data.
pub const MAX_STRING_LEN: usize = 128 * 1024 * 1024;

/// Write a RowBinary value stream to an `std.Io.Writer`.
pub const RowBinaryEncoder = struct {
    writer: *std.Io.Writer,

    pub fn init(writer: *std.Io.Writer) RowBinaryEncoder {
        return .{ .writer = writer };
    }

    /// Int16 — 2 bytes LE signed.
    pub fn writeInt16(self: *RowBinaryEncoder, v: i16) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(i16, &buf, v, .little);
        try self.writer.writeAll(&buf);
    }

    /// Int32 — 4 bytes LE signed.
    pub fn writeInt32(self: *RowBinaryEncoder, v: i32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &buf, v, .little);
        try self.writer.writeAll(&buf);
    }

    /// Int64 — 8 bytes LE signed.
    pub fn writeInt64(self: *RowBinaryEncoder, v: i64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, v, .little);
        try self.writer.writeAll(&buf);
    }

    /// Date — UInt16 LE (days since 1970-01-01).
    /// ZigHouse stores Date as i32 (days), CH wants UInt16.  Values >65535 wrap
    /// but that's unreachable for valid dates (would be year ~2149).
    pub fn writeDate(self: *RowBinaryEncoder, days: i32) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf, @intCast(@as(u32, @bitCast(days)) & 0xFFFF), .little);
        try self.writer.writeAll(&buf);
    }

    /// DateTime — UInt32 LE (unix seconds).
    /// ZigHouse stores DateTime as i64, CH wants UInt32.
    pub fn writeDateTime(self: *RowBinaryEncoder, unix_sec: i64) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, @intCast(@as(u64, @bitCast(unix_sec)) & 0xFFFFFFFF), .little);
        try self.writer.writeAll(&buf);
    }

    /// String — varUInt(len) + raw bytes.
    pub fn writeString(self: *RowBinaryEncoder, s: []const u8) !void {
        try writeVarUInt(self.writer, s.len);
        try self.writer.writeAll(s);
    }
};

/// Write `n` as ClickHouse varUInt (little-endian 7-bit groups, MSB = more).
pub fn writeVarUInt(writer: *std.Io.Writer, n: usize) !void {
    var v = n;
    while (true) {
        const byte: u8 = @intCast(v & 0x7F);
        v >>= 7;
        if (v == 0) {
            try writer.writeByte(byte);
            return;
        }
        try writer.writeByte(byte | 0x80);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "RowBinaryEncoder: Int16/Int32/Int64" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    var enc = RowBinaryEncoder.init(&w);

    try enc.writeInt16(-1);
    try enc.writeInt32(0x01020304);
    try enc.writeInt64(0x0102030405060708);

    const written = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xFF, 0xFF }, written[0..2]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x04, 0x03, 0x02, 0x01 }, written[2..6]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 }, written[6..14]);
}

test "RowBinaryEncoder: Date" {
    var buf: [4]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    var enc = RowBinaryEncoder.init(&w);
    // 2013-07-01 = 15887 days since epoch
    try enc.writeDate(15887);
    const written = std.Io.Writer.buffered(&w);
    const val = std.mem.readInt(u16, written[0..2], .little);
    try std.testing.expectEqual(@as(u16, 15887), val);
}

test "RowBinaryEncoder: DateTime" {
    var buf: [4]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    var enc = RowBinaryEncoder.init(&w);
    try enc.writeDateTime(1372636800); // 2013-07-01 00:00:00 UTC
    const written = std.Io.Writer.buffered(&w);
    const val = std.mem.readInt(u32, written[0..4], .little);
    try std.testing.expectEqual(@as(u32, 1372636800), val);
}

test "RowBinaryEncoder: String" {
    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    var enc = RowBinaryEncoder.init(&w);
    try enc.writeString("hello");
    const written = std.Io.Writer.buffered(&w);
    // varUInt(5) = 0x05, then "hello"
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x05, 'h', 'e', 'l', 'l', 'o' }, written[0..6]);
}

test "writeVarUInt: single byte" {
    var buf: [4]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeVarUInt(&w, 127);
    const written = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x7F}, written[0..1]);
}

test "writeVarUInt: two bytes" {
    var buf: [4]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeVarUInt(&w, 128);
    const written = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x80, 0x01 }, written[0..2]);
}

test "writeVarUInt: large value 300" {
    var buf: [4]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeVarUInt(&w, 300);
    // 300 = 0b100101100 -> groups: 0101100 (0x2C | 0x80 = 0xAC), 0000010 (0x02)
    const written = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xAC, 0x02 }, written[0..2]);
}
