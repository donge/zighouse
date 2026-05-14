/// Write `count.txt` for a ClickHouse MergeTree part.
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
