/// ClickHouse MergeTree `.mrk2` mark file writer.
///
/// Each mark entry = 24 bytes (3 × u64 LE):
///   offset_in_compressed_file  u64 LE  — byte offset of the LZ4 block in .bin
///   offset_in_decompressed_block u64 LE — byte offset within the decompressed block
///   granularity                u64 LE  — number of rows in this granule (= 8192 default)
///
/// The `.mrk2` format is used when `index_granularity_bytes > 0` (adaptive
/// granularity is OFF for our writer — we always write fixed 8192-row granules).
///
/// Reference: MergeTreeIndexGranularityInfo.cpp, MarkRange.h

const std = @import("std");

pub const MARK_SIZE: usize = 24; // 3 × u64
pub const DEFAULT_GRANULE: u64 = 8192;

/// A single mark entry.
pub const Mark = struct {
    /// Byte offset of the start of the compressed LZ4 block in the .bin file.
    offset_in_compressed_file: u64,
    /// Byte offset within the decompressed block where this granule begins.
    offset_in_decompressed_block: u64,
    /// Number of rows in this granule.
    granularity: u64,
};

/// Write a single mark to `writer`.
pub fn writeMark(writer: *std.Io.Writer, mark: Mark) !void {
    var buf: [MARK_SIZE]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], mark.offset_in_compressed_file, .little);
    std.mem.writeInt(u64, buf[8..16], mark.offset_in_decompressed_block, .little);
    std.mem.writeInt(u64, buf[16..24], mark.granularity, .little);
    try writer.writeAll(&buf);
}

/// Read a single mark from `reader`.
pub fn readMark(reader: *std.Io.Reader) !Mark {
    var buf: [MARK_SIZE]u8 = undefined;
    try reader.readSliceAll(&buf);
    return .{
        .offset_in_compressed_file = std.mem.readInt(u64, buf[0..8], .little),
        .offset_in_decompressed_block = std.mem.readInt(u64, buf[8..16], .little),
        .granularity = std.mem.readInt(u64, buf[16..24], .little),
    };
}

/// Write all marks to `writer`.
pub fn writeMarks(writer: *std.Io.Writer, marks: []const Mark) !void {
    for (marks) |m| try writeMark(writer, m);
}

/// Read all marks from `reader` (reads until EndOfStream).
/// Returns allocator-owned slice; caller must free.
pub fn readAllMarks(allocator: std.mem.Allocator, reader: *std.Io.Reader) ![]Mark {
    var list: std.ArrayList(Mark) = .empty;
    errdefer list.deinit(allocator);
    while (true) {
        var buf: [MARK_SIZE]u8 = undefined;
        const n = reader.readSliceShort(&buf) catch |e| switch (e) {
            error.ReadFailed => return e,
            else => unreachable,
        };
        if (n == 0) break;
        if (n != MARK_SIZE) return error.TruncatedMark;
        try list.append(allocator, .{
            .offset_in_compressed_file = std.mem.readInt(u64, buf[0..8], .little),
            .offset_in_decompressed_block = std.mem.readInt(u64, buf[8..16], .little),
            .granularity = std.mem.readInt(u64, buf[16..24], .little),
        });
    }
    return list.toOwnedSlice(allocator);
}

/// Compute marks for a fixed-width column given the per-granule compressed
/// block offsets.
///
/// For a fixed-width column written with `block.BlockWriter` (one block per
/// granule boundary), each granule starts at offset 0 in its decompressed
/// block because the BlockWriter flushes exactly at granule boundaries.
///
/// `compressed_offsets[i]` = byte offset in the .bin file where granule i's
/// compressed block begins.
/// `row_count` = total rows; last granule may have < DEFAULT_GRANULE rows.
pub fn marksForFixedColumn(
    allocator: std.mem.Allocator,
    compressed_offsets: []const u64,
    row_count: u64,
    granule_size: u64,
) ![]Mark {
    const n_granules = compressed_offsets.len;
    const marks = try allocator.alloc(Mark, n_granules);    for (marks, 0..) |*m, i| {
        const rows_so_far: u64 = @intCast(i);
        const remaining = row_count - rows_so_far * granule_size;
        const this_granule = if (remaining >= granule_size) granule_size else remaining;
        m.* = .{
            .offset_in_compressed_file = compressed_offsets[i],
            .offset_in_decompressed_block = 0,
            .granularity = this_granule,
        };
    }
    return marks;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "mark round-trip" {
    const mark = Mark{
        .offset_in_compressed_file = 0x1234567890ABCDEF,
        .offset_in_decompressed_block = 0,
        .granularity = DEFAULT_GRANULE,
    };
    var buf: [MARK_SIZE]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeMark(&w, mark);
    try std.testing.expectEqual(MARK_SIZE, std.Io.Writer.buffered(&w).len);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    const got = try readMark(&r);
    try std.testing.expectEqual(mark.offset_in_compressed_file, got.offset_in_compressed_file);
    try std.testing.expectEqual(mark.offset_in_decompressed_block, got.offset_in_decompressed_block);
    try std.testing.expectEqual(mark.granularity, got.granularity);
}

test "marks multiple round-trip" {
    const marks = [_]Mark{
        .{ .offset_in_compressed_file = 0, .offset_in_decompressed_block = 0, .granularity = 8192 },
        .{ .offset_in_compressed_file = 65600, .offset_in_decompressed_block = 0, .granularity = 8192 },
        .{ .offset_in_compressed_file = 131200, .offset_in_decompressed_block = 0, .granularity = 4096 },
    };
    var buf: [MARK_SIZE * 3]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeMarks(&w, &marks);
    try std.testing.expectEqual(MARK_SIZE * 3, std.Io.Writer.buffered(&w).len);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    const got = try readAllMarks(std.testing.allocator, &r);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqual(@as(usize, 3), got.len);
    for (marks, got) |expected, actual| {
        try std.testing.expectEqual(expected.offset_in_compressed_file, actual.offset_in_compressed_file);
        try std.testing.expectEqual(expected.granularity, actual.granularity);
    }
}

test "marksForFixedColumn exact multiple" {
    // 3 granules × 8192 rows = 24576 rows total
    const offsets = [_]u64{ 0, 65600, 131200 };
    const marks = try marksForFixedColumn(std.testing.allocator, &offsets, 24576, DEFAULT_GRANULE);
    defer std.testing.allocator.free(marks);
    try std.testing.expectEqual(@as(usize, 3), marks.len);
    try std.testing.expectEqual(@as(u64, 8192), marks[0].granularity);
    try std.testing.expectEqual(@as(u64, 8192), marks[1].granularity);
    try std.testing.expectEqual(@as(u64, 8192), marks[2].granularity);
}

test "marksForFixedColumn partial last granule" {
    // 2 full granules + 1 partial (100 rows)
    const offsets = [_]u64{ 0, 65600, 131200 };
    const marks = try marksForFixedColumn(std.testing.allocator, &offsets, 8192 * 2 + 100, DEFAULT_GRANULE);
    defer std.testing.allocator.free(marks);
    try std.testing.expectEqual(@as(u64, 8192), marks[0].granularity);
    try std.testing.expectEqual(@as(u64, 8192), marks[1].granularity);
    try std.testing.expectEqual(@as(u64, 100), marks[2].granularity);
}
