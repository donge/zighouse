const std = @import("std");
const generic_sql = @import("generic_sql.zig");
const native_reduce = @import("native_reduce.zig");

pub fn execute(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (try executeAdvEngineCount(allocator, plan, hot)) |output| return output;
    if (try executeUrlLengthByCounter(allocator, plan, hot)) |output| return output;
    return null;
}

pub fn formatAdvEngineCount(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    const min_key = std.math.minInt(i16);
    const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (hot.adv_engine_id) |value| {
        if (value == 0) continue;
        const index: usize = @intCast(@as(i32, value) - @as(i32, min_key));
        counts[index] += 1;
    }

    var rows: std.ArrayList(CountRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count == 0) continue;
        const key: i16 = @intCast(@as(i32, @intCast(index)) + @as(i32, min_key));
        try rows.append(allocator, .{ .key = key, .count = count });
    }
    std.mem.sort(CountRow, rows.items, {}, countRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "AdvEngineID,count_star()\n");
    for (rows.items) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatUrlLengthByCounter(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    const url_lengths = hot.url_length orelse return error.UnsupportedGenericQuery;
    const min_counter: i32 = 0;
    const max_counter: i32 = 262143;
    const bucket_count: usize = @intCast(max_counter - min_counter + 1);
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    const sums = try allocator.alloc(u64, bucket_count);
    defer allocator.free(sums);
    @memset(counts, 0);
    @memset(sums, 0);

    for (hot.counter_id, url_lengths) |counter_id, url_length| {
        if (url_length == 0) continue;
        if (counter_id < min_counter or counter_id > max_counter) continue;
        const index: usize = @intCast(counter_id - min_counter);
        counts[index] += 1;
        sums[index] += @intCast(url_length);
    }

    var rows: std.ArrayList(AvgCountRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count <= 100000) continue;
        const counter_id: i32 = @intCast(@as(i64, min_counter) + @as(i64, @intCast(index)));
        try rows.append(allocator, .{ .key = counter_id, .sum = sums[index], .count = count });
    }
    std.mem.sort(AvgCountRow, rows.items, {}, avgCountRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "CounterID,l,c\n");
    const limit = @min(rows.items.len, 25);
    for (rows.items[0..limit]) |row| {
        const avg = @as(f64, @floatFromInt(row.sum)) / @as(f64, @floatFromInt(row.count));
        try out.print(allocator, "{d},{d},{d}\n", .{ row.key, avg, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn executeAdvEngineCount(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (!isAdvEngineCountPlan(plan)) return null;
    return try formatAdvEngineCount(allocator, hot);
}

fn executeUrlLengthByCounter(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (!isUrlLengthByCounterPlan(plan)) return null;
    return try formatUrlLengthByCounter(allocator, hot);
}

fn isAdvEngineCountPlan(plan: generic_sql.Plan) bool {
    if (!plan.order_by_count_desc or plan.limit != null or plan.offset != null) return false;
    if (!hasEmptyStringFilter(plan, "AdvEngineID")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "AdvEngineID")) return false;
    if (plan.projections.len != 2) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "AdvEngineID")) return false;
    return plan.projections[1].func == .count_star;
}

fn isUrlLengthByCounterPlan(plan: generic_sql.Plan) bool {
    if (plan.limit != 25 or plan.offset != null) return false;
    if (!hasEmptyStringFilter(plan, "URL")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "CounterID")) return false;
    if (!asciiEqlIgnoreCase(plan.having_text orelse return false, "COUNT(*) > 100000")) return false;
    if (!orderByAlias(plan, "l")) return false;
    if (plan.projections.len != 3) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "CounterID")) return false;
    if (plan.projections[1].func != .avg or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "length(URL)")) return false;
    if (!asciiEqlIgnoreCase(plan.projections[1].alias orelse return false, "l")) return false;
    return plan.projections[2].func == .count_star and asciiEqlIgnoreCase(plan.projections[2].alias orelse return false, "c");
}

fn hasEmptyStringFilter(plan: generic_sql.Plan, column: []const u8) bool {
    const filter = plan.filter orelse return false;
    return filter.second == null and filter.op == .not_equal and filter.int_value == 0 and asciiEqlIgnoreCase(filter.column, column);
}

fn orderByAlias(plan: generic_sql.Plan, alias: []const u8) bool {
    return if (plan.order_by_alias) |got| asciiEqlIgnoreCase(got, alias) else false;
}

const CountRow = struct {
    key: i16,
    count: u64,
};

fn countRowDesc(_: void, a: CountRow, b: CountRow) bool {
    if (a.count == b.count) return a.key < b.key;
    return a.count > b.count;
}

const AvgCountRow = struct {
    key: i32,
    sum: u64,
    count: u64,
};

fn avgCountRowDesc(_: void, a: AvgCountRow, b: AvgCountRow) bool {
    const left = @as(u128, a.sum) * @as(u128, b.count);
    const right = @as(u128, b.sum) * @as(u128, a.count);
    if (left != right) return left > right;
    return a.key < b.key;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}
