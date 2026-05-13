const std = @import("std");
const lowcard = @import("../lowcard.zig");

const TopRow = struct { id: u32, count: u32 };

pub fn formatLowCardTextCountTop(allocator: std.mem.Allocator, column_name: []const u8, count_label: []const u8, column: *const lowcard.StringColumn, limit: usize) ![]u8 {
    const dict_size = column.dictSize();
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (column.ids.values) |id| counts[id] += 1;

    const top_capacity = limit + 1;
    const top = try allocator.alloc(TopRow, top_capacity);
    defer allocator.free(top);
    var top_len: usize = 0;
    for (counts, 0..) |count, idx| {
        if (count == 0) continue;
        insertTop(top, &top_len, .{ .id = @intCast(idx), .count = count });
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s}\n", .{ column_name, count_label });
    var emitted: usize = 0;
    for (top[0..top_len]) |row| {
        if (emitted == limit) break;
        const value = column.value(row.id);
        if (lowcard.isStoredEmptyString(value)) continue;
        try writeCsvField(allocator, &out, value);
        try out.print(allocator, ",{d}\n", .{row.count});
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

fn insertTop(top: []TopRow, top_len: *usize, row: TopRow) void {
    var pos: usize = 0;
    while (pos < top_len.* and rowBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= top.len) return;
    if (top_len.* < top.len) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn rowBefore(a: TopRow, b: TopRow) bool {
    if (a.count != b.count) return a.count > b.count;
    return a.id < b.id;
}

fn writeCsvField(allocator: std.mem.Allocator, out: *std.ArrayList(u8), value: []const u8) !void {
    var needs_quote = false;
    for (value) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r' or c < 0x20 or c >= 0x80) {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        try out.appendSlice(allocator, value);
        return;
    }
    try out.append(allocator, '"');
    for (value) |c| {
        if (c == '"') try out.append(allocator, '"');
        try out.append(allocator, c);
    }
    try out.append(allocator, '"');
}
