/// ClickHouse MergeTree `.mrk2` / `.cmrk2` / `.cmrk4` mark file support.
///
/// Wide-part marks (`.mrk2` / `.cmrk2`):
///   Each mark entry = 24 bytes (3 × u64 LE):
///     offset_in_compressed_file  u64 LE  — byte offset of the LZ4 block in .bin
///     offset_in_decompressed_block u64 LE — byte offset within the decompressed block
///     granularity                u64 LE  — number of rows in this granule (= 8192 default)
///   `.cmrk2` wraps mark bytes in a single CH compressed block (LZ4).
///
/// Compact-part marks (`.cmrk4`):
///   The entire mark file is a single CH compressed block (ZSTD or LZ4).
///   After decompression, the layout is:
///     For each substream s in [0 .. n_substreams):
///       offset_in_data_bin        u64 LE  — byte offset in data.bin where this substream's block starts
///       offset_in_decompressed    u64 LE  — offset within that decompressed block (0 for start of granule)
///   There are n_substreams entries per granule × n_granules entries total.
///   The column/substream order matches `columns_substreams.txt`.
///
/// Reference: MergeTreeIndexGranularityInfo.cpp, MarkRange.h, MergeTreeDataPartCompact.cpp

const std = @import("std");
const block = @import("block.zig");

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

// ── Compact part mark (.cmrk4) support ────────────────────────────────────────

/// A single substream mark entry from a `.cmrk4` file.
/// Identifies where in `data.bin` this substream's compressed block lives.
pub const CompactMark = struct {
    /// Byte offset in `data.bin` where this substream's compressed block starts.
    offset_in_file: u64,
    /// Byte offset within the decompressed block (0 for granule start).
    offset_in_block: u64,
};

pub const COMPACT_MARK_SIZE: usize = 16; // 2 × u64

/// Read all compact marks from a decompressed `.cmrk4` payload.
/// Returns allocator-owned slice of `n_substreams` entries per granule.
/// Total entries = n_granules × n_substreams, stored in granule-major order.
pub fn readCompactMarks(allocator: std.mem.Allocator, data: []const u8) ![]CompactMark {
    if (data.len % COMPACT_MARK_SIZE != 0) return error.TruncatedMark;
    const n = data.len / COMPACT_MARK_SIZE;
    const result = try allocator.alloc(CompactMark, n);
    for (result, 0..) |*m, i| {
        const off = i * COMPACT_MARK_SIZE;
        m.* = .{
            .offset_in_file = std.mem.readInt(u64, data[off..][0..8], .little),
            .offset_in_block = std.mem.readInt(u64, data[off + 8 ..][0..8], .little),
        };
    }
    return result;
}

/// Read a `.cmrk4` file: decompress the outer CH block, then parse marks.
/// The caller must provide the raw file bytes.
/// Returns allocator-owned slice of CompactMark (length = n_granules × n_substreams).
pub fn readCmrk4(allocator: std.mem.Allocator, file_bytes: []const u8) ![]CompactMark {
    var reader = std.Io.Reader.fixed(file_bytes);
    const decompressed = try block.readBlock(allocator, &reader);
    defer allocator.free(decompressed);
    return readCompactMarks(allocator, decompressed);
}

/// Encode compact marks and write as a single LZ4-compressed CH block.
///
/// CH adaptive-granularity format (version 4):
///   For each granule g in [0..n_granules]:  (n_granules+1 rows total, last is EOF)
///     n_substreams × 16 bytes: {offset_in_file: u64 LE, offset_in_block: u64 LE}
///     8 bytes: granularity (rows in this granule) as u64 LE
///
/// `compact_marks` must have n_granules × n_substreams entries (granule-major order).
/// `granularities` must have n_granules entries (row count per granule).
/// `eof_offset` is the byte size of data.bin (for the EOF sentinel granule).
pub fn writeCmrk4(
    writer: *std.Io.Writer,
    compact_marks: []const CompactMark,
    n_substreams: usize,
    granularities: []const u64,
    eof_offset: u64,
) !void {
    const n_granules = granularities.len;
    if (n_substreams == 0 or (n_granules > 0 and compact_marks.len != n_granules * n_substreams))
        return error.InvalidMarkCount;

    // (n_granules + 1) rows: one per granule + one EOF row
    const row_bytes = n_substreams * COMPACT_MARK_SIZE + 8; // marks + granularity
    const total_bytes = (n_granules + 1) * row_bytes;
    const raw = try std.heap.page_allocator.alloc(u8, total_bytes);
    defer std.heap.page_allocator.free(raw);

    var pos: usize = 0;
    for (0..n_granules) |g| {
        for (0..n_substreams) |s| {
            const m = compact_marks[g * n_substreams + s];
            std.mem.writeInt(u64, raw[pos..][0..8], m.offset_in_file, .little);
            std.mem.writeInt(u64, raw[pos + 8 ..][0..8], m.offset_in_block, .little);
            pos += COMPACT_MARK_SIZE;
        }
        std.mem.writeInt(u64, raw[pos..][0..8], granularities[g], .little);
        pos += 8;
    }
    // EOF sentinel granule: all marks point to eof_offset, granularity=0
    for (0..n_substreams) |_| {
        std.mem.writeInt(u64, raw[pos..][0..8], eof_offset, .little);
        std.mem.writeInt(u64, raw[pos + 8 ..][0..8], 0, .little);
        pos += COMPACT_MARK_SIZE;
    }
    std.mem.writeInt(u64, raw[pos..][0..8], 0, .little); // granularity=0 for EOF

    try block.writeBlock(writer, raw);
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
