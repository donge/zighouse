/// Write `columns.txt` for a ClickHouse MergeTree part.
///
/// Format (text):
///   columns format version: 1\n
///   <N> columns:\n
///   `<name>` <CHType>\n
///   ...
///
/// Reference: MergeTreeData.cpp, NamesAndTypes.cpp

const std = @import("std");
const schema = @import("schema");
const types = @import("types");

/// Write columns.txt for the given columns list to `writer`.
/// Only columns that have a direct ClickHouse representation are emitted
/// (i.e. columns with a fixedWidth or String type; derived/hash-only columns
/// are skipped since they are not stored as CH columns directly).
///
/// `columns` is a slice of (name, type) pairs representing the CH columns to
/// emit — the caller decides which logical columns map to which CH files.
pub fn write(writer: *std.Io.Writer, columns: []const ChColumn) !void {
    try writer.print("columns format version: 1\n", .{});
    try writer.print("{d} columns:\n", .{columns.len});
    for (columns) |col| {
        try writer.print("`{s}` {s}\n", .{ col.name, col.ch_type });
    }
}

/// A (name, ch_type_string) pair for columns.txt emission.
pub const ChColumn = struct {
    name: []const u8,
    ch_type: []const u8,
};

/// Build a ChColumn slice from a schema.Table, using only Phase-1 supported
/// types.  Allocates; caller must free with `freeChColumns(allocator, result)`.
pub fn fromTable(allocator: std.mem.Allocator, table: schema.Table) ![]ChColumn {
    var list: std.ArrayList(ChColumn) = .empty;
    errdefer list.deinit(allocator);
    for (table.columns) |col| {
        switch (col.ty) {
            .int8, .int16, .int32, .int64, .date, .timestamp, .text, .float32, .float64, .low_card => {
                try list.append(allocator, .{
                    .name = col.name,
                    .ch_type = col.ch_type orelse types.chTypeName(col.ty),
                });
            },
            .char => {
                // char maps to String in CH
                try list.append(allocator, .{
                    .name = col.name,
                    .ch_type = col.ch_type orelse "String",
                });
            },
        }
    }
    return list.toOwnedSlice(allocator);
}

pub fn freeChColumns(allocator: std.mem.Allocator, cols: []ChColumn) void {
    allocator.free(cols);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "columns_txt write" {
    var buf: [512]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    const cols = [_]ChColumn{
        .{ .name = "EventDate", .ch_type = "Date" },
        .{ .name = "CounterID", .ch_type = "Int32" },
        .{ .name = "Title", .ch_type = "String" },
    };
    try write(&w, &cols);
    const got = std.Io.Writer.buffered(&w);
    const expected =
        "columns format version: 1\n" ++
        "3 columns:\n" ++
        "`EventDate` Date\n" ++
        "`CounterID` Int32\n" ++
        "`Title` String\n";
    try std.testing.expectEqualStrings(expected, got);
}

test "columns_txt fromTable" {
    const cols = [_]schema.Column{
        .{ .name = "A", .ty = .int32 },
        .{ .name = "B", .ty = .text },
        .{ .name = "C", .ty = .date },
    };
    const table = schema.Table{ .name = "t", .columns = &cols };
    const ch_cols = try fromTable(std.testing.allocator, table);
    defer freeChColumns(std.testing.allocator, ch_cols);
    try std.testing.expectEqual(@as(usize, 3), ch_cols.len);
    try std.testing.expectEqualStrings("A", ch_cols[0].name);
    try std.testing.expectEqualStrings("Int32", ch_cols[0].ch_type);
    try std.testing.expectEqualStrings("String", ch_cols[1].ch_type);
    try std.testing.expectEqualStrings("Date", ch_cols[2].ch_type);
}
