/// ClickHouse MergeTree String column codec.
///
/// CH stores String columns in .bin files as a sequence of:
///   varuint(len) followed by len bytes of UTF-8 data
///
/// This is the same LEB128 varint used in primary.idx.
///
/// The .bin file for a String column is itself compressed in LZ4 blocks
/// (same block.zig format), just like fixed-width columns.
///
/// Reference: DataTypeString.cpp serializeBinaryBulkWithMultipleStreams

const std = @import("std");

pub const MAX_STRING_LEN: u64 = 1 << 30; // 1 GiB sanity limit

/// Write a single CH-encoded string to `writer`.
pub fn writeString(writer: *std.Io.Writer, s: []const u8) !void {
    try writeVarint(writer, s.len);
    try writer.writeAll(s);
}

/// Read a single CH-encoded string from `reader`.
/// Returns allocator-owned slice; caller must free.
pub fn readString(allocator: std.mem.Allocator, reader: *std.Io.Reader) ![]u8 {
    const len = try readVarint(reader);
    if (len > MAX_STRING_LEN) return error.StringTooLong;
    const buf = try allocator.alloc(u8, len);
    errdefer allocator.free(buf);
    try reader.readSliceAll(buf);
    return buf;
}

/// Write varuint (LEB128 unsigned) — same as primary_idx.writeVarint.
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

/// Read varuint (LEB128 unsigned).
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

/// Serialize a slice of strings into a CH .bin payload buffer.
/// The result is the raw (uncompressed) bytes to be passed to BlockWriter.
/// Caller must free the returned slice.
pub fn serializeStrings(
    allocator: std.mem.Allocator,
    strings: []const []const u8,
) ![]u8 {
    var aw = std.Io.Writer.Allocating.init(allocator);
    errdefer aw.deinit();
    for (strings) |s| {
        try writeVarint(&aw.writer, s.len);
        try aw.writer.writeAll(s);
    }
    const list = aw.toArrayList();
    return list.toOwnedSlice(allocator);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "string round-trip" {
    const cases = [_][]const u8{ "", "hello", "ClickHouse rocks!", "日本語" };
    for (cases) |s| {
        var buf: [256]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try writeString(&w, s);
        var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
        const got = try readString(std.testing.allocator, &r);
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualStrings(s, got);
    }
}

test "string codec multiple" {
    const strings = [_][]const u8{ "foo", "bar", "baz" };
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    for (strings) |s| try writeString(&w, s);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    for (strings) |expected| {
        const got = try readString(std.testing.allocator, &r);
        defer std.testing.allocator.free(got);
        try std.testing.expectEqualStrings(expected, got);
    }
}
