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

/// Two-pass distinct-aggregate top-N over a low-cardinality string group key.
///
///   Pass 1: dense counts[group_id]++ over `column.ids` to pick top-N
///           candidate group_ids by raw row count (excluding the empty id).
///   Pass 2: per-candidate bitset of `uid_ids`; popcount each bitset for
///           distinct count; sort top-K by distinct desc, group_id asc.
///
/// Correctness assumption: top-K-by-distinct group_ids are within
/// top-N-by-row-count, since distinct(uid) <= count. Choose N >> K.
///
/// Schema-driven generalization of formatSearchPhraseDistinctUserIdTop;
/// expects `column.ids.values.len == uid_ids.len`. Returns CSV
/// `<column_name>,<count_label>\n<value>,<distinct>\n...`.
pub fn formatLowCardTextDistinctTop(
    allocator: std.mem.Allocator,
    column_name: []const u8,
    count_label: []const u8,
    column: *const lowcard.StringColumn,
    empty_id: ?u32,
    uid_ids: []const u32,
    uid_dict_size: usize,
    limit: usize,
    n_candidates: usize,
) ![]u8 {
    if (column.ids.values.len != uid_ids.len) return error.CorruptHotColumns;
    const dict_size = column.dictSize();

    // Pass 1: dense row counts.
    const counts = try allocator.alloc(u32, dict_size);
    defer allocator.free(counts);
    @memset(counts, 0);
    for (column.ids.values) |id| counts[id] += 1;

    const Candidate = struct { id: u32, count: u32 };
    const cand = try allocator.alloc(Candidate, n_candidates);
    defer allocator.free(cand);
    var cand_len: usize = 0;
    for (counts, 0..) |c, idx| {
        if (c == 0) continue;
        if (empty_id) |eid| if (idx == eid) continue;
        const row: Candidate = .{ .id = @intCast(idx), .count = c };
        // Sort by count desc, id asc.
        var pos: usize = 0;
        while (pos < cand_len and (cand[pos].count > row.count or (cand[pos].count == row.count and cand[pos].id < row.id))) : (pos += 1) {}
        if (pos >= n_candidates) continue;
        if (cand_len < n_candidates) cand_len += 1;
        var j = cand_len - 1;
        while (j > pos) : (j -= 1) cand[j] = cand[j - 1];
        cand[pos] = row;
    }

    // candidate_idx[group_id] -> i32 (-1 not candidate, else slot).
    const cand_idx = try allocator.alloc(i32, dict_size);
    defer allocator.free(cand_idx);
    @memset(cand_idx, -1);
    for (cand[0..cand_len], 0..) |c, slot| cand_idx[c.id] = @intCast(slot);

    // Pass 2: per-candidate UserID bitset.
    const words_per_set = (uid_dict_size + 63) / 64;
    const bitsets = try allocator.alloc(u64, cand_len * words_per_set);
    defer allocator.free(bitsets);
    @memset(bitsets, 0);

    var i: usize = 0;
    const n = column.ids.values.len;
    while (i < n) : (i += 1) {
        const gid = column.ids.values[i];
        const slot = cand_idx[gid];
        if (slot < 0) continue;
        const uid = uid_ids[i];
        const base = @as(usize, @intCast(slot)) * words_per_set;
        bitsets[base + (uid >> 6)] |= @as(u64, 1) << @intCast(uid & 63);
    }

    const Result = struct { id: u32, distinct: u64 };
    const results = try allocator.alloc(Result, cand_len);
    defer allocator.free(results);
    for (cand[0..cand_len], 0..) |c, slot| {
        const base = slot * words_per_set;
        var sum: u64 = 0;
        for (bitsets[base .. base + words_per_set]) |w| sum += @popCount(w);
        results[slot] = .{ .id = c.id, .distinct = sum };
    }
    std.sort.pdq(Result, results, {}, struct {
        fn lt(_: void, a: Result, b: Result) bool {
            if (a.distinct != b.distinct) return a.distinct > b.distinct;
            return a.id < b.id;
        }
    }.lt);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s}\n", .{ column_name, count_label });
    const top_emit = @min(limit, cand_len);
    for (results[0..top_emit]) |r| {
        try writeCsvField(allocator, &out, column.value(r.id));
        try out.print(allocator, ",{d}\n", .{r.distinct});
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
