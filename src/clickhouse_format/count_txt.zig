/// Read/write `count.txt` for a ClickHouse MergeTree part.
///
/// Format: just the ASCII decimal row count followed by a newline.
///   <N>\n
///
/// Reference: MergeTreeData.cpp loadDataPart

const std = @import("std");

/// Write count.txt containing `row_count` to `writer`.
pub fn write(writer: *std.Io.Writer, row_count: u64) !void {
    try writer.print("{d}\n", .{row_count});
}

/// Read row count from a count.txt file at `path`.
pub fn readPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !u64 {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(32));
    defer allocator.free(bytes);
    const line = std.mem.trim(u8, bytes, "\n\r ");
    return std.fmt.parseInt(u64, line, 10) catch return error.InvalidCountTxt;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "count_txt write" {
    var buf: [32]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try write(&w, 10_000_000);
    const got = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualStrings("10000000\n", got);
}

test "count_txt write zero" {
    var buf: [32]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try write(&w, 0);
    const got = std.Io.Writer.buffered(&w);
    try std.testing.expectEqualStrings("0\n", got);
}

test "count_txt readPath round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const path = "/tmp/zig_test_count_txt.txt";

    // Write
    {
        var f = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        defer f.close(io);
        var buf: [32]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try write(&w, 12345678);
        try f.writeStreamingAll(io, std.Io.Writer.buffered(&w));
    }

    // Read back
    const got = try readPath(allocator, io, path);
    try std.testing.expectEqual(@as(u64, 12345678), got);
}
