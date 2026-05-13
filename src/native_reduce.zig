const std = @import("std");
const generic_sql = @import("generic_sql.zig");
const simd = @import("simd.zig");

pub const HotColumns = struct {
    adv_engine_id: []const i16,
    resolution_width: []const i16,
    user_id: []const i64,
    event_date: []const i32,
    counter_id: []const i32,
    is_refresh: []const i16,
    dont_count_hits: []const i16,
    url_length: ?[]const i32,

    fn rowCount(self: HotColumns) usize {
        return self.adv_engine_id.len;
    }
};

const Value = union(enum) {
    int: i64,
    float: f64,
    date: i32,
};

const Column = union(enum) {
    i16: []const i16,
    i32: []const i32,
    date: []const i32,
    i64: []const i64,
};

pub fn executeScalar(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: HotColumns) !?[]u8 {
    if (plan.group_by != null or plan.having_text != null or plan.order_by_text != null or plan.limit != null or plan.offset != null) return null;
    if (plan.where_text != null and plan.filter == null) return null;
    for (plan.projections) |expr| {
        if (expr.func == .column_ref or expr.func == .count_distinct) return null;
    }

    if (try executeFastCountStar(allocator, plan, hot)) |output| return output;

    const predicate = if (plan.filter) |filter| try materializePlanFilter(allocator, hot, filter) else null;
    defer if (predicate) |p| allocator.free(p);

    if (try executeFusedSumOffsets(allocator, plan, hot, predicate)) |output| return output;
    if (try executeFusedMinMax(allocator, plan, hot, predicate)) |output| return output;

    const values = try allocator.alloc(Value, plan.projections.len);
    defer allocator.free(values);
    for (plan.projections, 0..) |expr, i| {
        values[i] = try executeProjection(expr, hot, predicate);
    }
    return try formatValues(allocator, plan, values);
}

fn executeFastCountStar(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: HotColumns) !?[]u8 {
    if (plan.projections.len != 1 or plan.projections[0].func != .count_star) return null;
    const filter = plan.filter orelse return null;
    if (filter.second != null) return null;
    const column = bindFilterColumn(hot, filter.column) catch return error.UnsupportedGenericQuery;
    const count = try countMatches(column, filter.op, filter.int_value);
    return try formatOneInt(allocator, "count_star()", count);
}

fn countMatches(column: Column, op: generic_sql.FilterOp, int_value: i64) !u64 {
    return switch (column) {
        .i16 => |values| countMatchesTyped(i16, values, op, int_value),
        .i32, .date => |values| countMatchesTyped(i32, values, op, int_value),
        .i64 => |values| countMatchesTyped(i64, values, op, int_value),
    };
}

fn countMatchesTyped(comptime T: type, values: []const T, op: generic_sql.FilterOp, int_value: i64) !u64 {
    const target = std.math.cast(T, int_value) orelse return error.UnsupportedGenericQuery;
    if (op == .not_equal and target == 0) return simd.countNonZero(T, values);
    if (op == .equal) return simd.countEq(T, values, target);
    var count: u64 = 0;
    for (values) |value| count += @intFromBool(predicateMatches(T, value, target, op));
    return count;
}

pub fn formatOneInt(allocator: std.mem.Allocator, header: []const u8, value: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\n{d}\n", .{ header, value });
}

pub fn formatCountNonZeroI16(allocator: std.mem.Allocator, header: []const u8, values: []const i16) ![]u8 {
    return formatOneInt(allocator, header, simd.countNonZero(i16, values));
}

pub fn formatSumCountAvg(allocator: std.mem.Allocator, adv_engine_id: []const i16, resolution_width: []const i16) ![]u8 {
    const adv_sum = simd.sum(i16, adv_engine_id);
    const width_sum = simd.sum(i16, resolution_width);
    return std.fmt.allocPrint(allocator, "sum(AdvEngineID),count_star(),avg(ResolutionWidth)\n{d},{d},{d}\n", .{ adv_sum, adv_engine_id.len, avgFromSum(width_sum, resolution_width.len) });
}

pub fn formatAvgUserId(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    return std.fmt.allocPrint(allocator, "avg(UserID)\n{d}\n", .{simd.avg(i64, values)});
}

pub fn formatMinMaxEventDate(allocator: std.mem.Allocator, values: []const i32) ![]u8 {
    const mm = simd.minMax(i32, values);
    const min_date = dateString(@intCast(mm.min));
    const max_date = dateString(@intCast(mm.max));
    return std.fmt.allocPrint(allocator, "min(EventDate),max(EventDate)\n{s},{s}\n", .{ min_date, max_date });
}

pub fn formatWideResolutionSums(allocator: std.mem.Allocator, values: []const i16) ![]u8 {
    const base_sum = simd.sum(i16, values);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    for (0..90) |i| {
        if (i != 0) try out.append(allocator, ',');
        if (i == 0) {
            try out.appendSlice(allocator, "sum(ResolutionWidth)");
        } else {
            try out.print(allocator, "sum((ResolutionWidth + {d}))", .{i});
        }
    }
    try out.append(allocator, '\n');

    const row_count: i64 = @intCast(values.len);
    for (0..90) |i| {
        if (i != 0) try out.append(allocator, ',');
        const value = base_sum + @as(i64, @intCast(i)) * row_count;
        try out.print(allocator, "{d}", .{value});
    }
    try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

fn executeFusedSumOffsets(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: HotColumns, predicate: ?[]const i16) !?[]u8 {
    if (plan.projections.len < 2) return null;
    const first_col = plan.projections[0].column orelse return null;
    for (plan.projections) |expr| {
        if (expr.func != .sum) return null;
        const col = expr.column orelse return null;
        if (!asciiEqlIgnoreCase(col, first_col)) return null;
    }

    const column = bindColumn(hot, first_col) catch return error.UnsupportedGenericQuery;
    const sum_count: simd.SumCount = switch (column) {
        .i16 => |values| if (predicate) |p| simd.filteredSumCountNonZero(i16, p, values) else .{ .sum = simd.sum(i16, values), .count = @intCast(values.len) },
        .i32 => |values| if (predicate) |p| simd.filteredSumCountNonZero(i32, p, values) else .{ .sum = simd.sum(i32, values), .count = @intCast(values.len) },
        .date, .i64 => return error.UnsupportedGenericQuery,
    };

    const values = try allocator.alloc(Value, plan.projections.len);
    defer allocator.free(values);
    for (plan.projections, values) |expr, *out| {
        out.* = .{ .int = sum_count.sum + expr.int_offset * @as(i64, @intCast(sum_count.count)) };
    }
    return try formatValues(allocator, plan, values);
}

fn executeFusedMinMax(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: HotColumns, predicate: ?[]const i16) !?[]u8 {
    if (plan.projections.len != 2) return null;
    const first = plan.projections[0];
    const second = plan.projections[1];
    if (first.func != .min or second.func != .max) return null;
    const first_col = first.column orelse return error.UnsupportedGenericQuery;
    const second_col = second.column orelse return error.UnsupportedGenericQuery;
    if (!asciiEqlIgnoreCase(first_col, second_col)) return null;
    return switch (bindColumn(hot, first_col) catch return error.UnsupportedGenericQuery) {
        .i16 => |values| blk: {
            const mm = if (predicate) |p| simd.filteredMinMaxNonZero(i16, p, values) else simd.minMax(i16, values);
            break :blk try formatValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
        .i32 => |values| blk: {
            const mm = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values) else simd.minMax(i32, values);
            break :blk try formatValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
        .date => |values| blk: {
            const mm = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values) else simd.minMax(i32, values);
            break :blk try formatValues(allocator, plan, &.{ .{ .date = mm.min }, .{ .date = mm.max } });
        },
        .i64 => |values| blk: {
            const mm = if (predicate) |p| simd.filteredMinMaxNonZero(i64, p, values) else simd.minMax(i64, values);
            break :blk try formatValues(allocator, plan, &.{ .{ .int = mm.min }, .{ .int = mm.max } });
        },
    };
}

fn executeProjection(expr: generic_sql.Expr, hot: HotColumns, predicate: ?[]const i16) !Value {
    if (expr.func == .count_star) return .{ .int = if (predicate) |p| @intCast(simd.countNonZero(i16, p)) else @intCast(hot.rowCount()) };
    if (expr.func == .int_literal) return .{ .int = expr.int_offset };
    const column = bindColumn(hot, expr.column orelse return error.UnsupportedGenericQuery) catch return error.UnsupportedGenericQuery;
    return switch (expr.func) {
        .column_ref => error.UnsupportedGenericQuery,
        .int_literal => unreachable,
        .count_distinct => error.UnsupportedGenericQuery,
        .count_star => unreachable,
        .sum => aggregateSum(column, predicate, expr.int_offset),
        .avg => aggregateAvg(column, predicate),
        .min => aggregateMin(column, predicate),
        .max => aggregateMax(column, predicate),
    };
}

fn aggregateSum(column: Column, predicate: ?[]const i16, int_offset: i64) !Value {
    return switch (column) {
        .i16 => |values| .{ .int = sumWithOffset(if (predicate) |p| simd.filteredSumCountNonZero(i16, p, values) else .{ .sum = simd.sum(i16, values), .count = @intCast(values.len) }, int_offset) },
        .i32 => |values| .{ .int = sumWithOffset(if (predicate) |p| simd.filteredSumCountNonZero(i32, p, values) else .{ .sum = simd.sum(i32, values), .count = @intCast(values.len) }, int_offset) },
        .date, .i64 => error.UnsupportedGenericQuery,
    };
}

fn sumWithOffset(sum_count: simd.SumCount, int_offset: i64) i64 {
    return sum_count.sum + int_offset * @as(i64, @intCast(sum_count.count));
}

fn aggregateAvg(column: Column, predicate: ?[]const i16) !Value {
    return switch (column) {
        .i16 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i16, p, values) else avgFromSum(simd.sum(i16, values), values.len) },
        .i32 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i32, p, values) else avgFromSum(simd.sum(i32, values), values.len) },
        .i64 => |values| .{ .float = if (predicate) |p| simd.filteredAvgNonZero(i64, p, values) else simd.avg(i64, values) },
        .date => error.UnsupportedGenericQuery,
    };
}

fn aggregateMin(column: Column, predicate: ?[]const i16) !Value {
    return switch (column) {
        .i16 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i16, p, values).min else simd.minMax(i16, values).min },
        .i32 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).min else simd.minMax(i32, values).min },
        .date => |values| .{ .date = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).min else simd.minMax(i32, values).min },
        .i64 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i64, p, values).min else simd.minMax(i64, values).min },
    };
}

fn aggregateMax(column: Column, predicate: ?[]const i16) !Value {
    return switch (column) {
        .i16 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i16, p, values).max else simd.minMax(i16, values).max },
        .i32 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).max else simd.minMax(i32, values).max },
        .date => |values| .{ .date = if (predicate) |p| simd.filteredMinMaxNonZero(i32, p, values).max else simd.minMax(i32, values).max },
        .i64 => |values| .{ .int = if (predicate) |p| simd.filteredMinMaxNonZero(i64, p, values).max else simd.minMax(i64, values).max },
    };
}

fn materializePlanFilter(allocator: std.mem.Allocator, hot: HotColumns, filter: generic_sql.Filter) ![]i16 {
    if (filter.second) |second| {
        const left_column = bindFilterColumn(hot, filter.column) catch return error.UnsupportedGenericQuery;
        const right_column = bindFilterColumn(hot, second.column) catch return error.UnsupportedGenericQuery;
        const left = generic_sql.Predicate{ .column = filter.column, .op = filter.op, .int_value = filter.int_value };
        return materializeAndPredicate(allocator, left_column, left, right_column, second);
    }
    const column = bindFilterColumn(hot, filter.column) catch return error.UnsupportedGenericQuery;
    return materializePredicate(allocator, column, .{ .column = filter.column, .op = filter.op, .int_value = filter.int_value });
}

fn materializeAndPredicate(allocator: std.mem.Allocator, left_column: Column, left: generic_sql.Predicate, right_column: Column, right: generic_sql.Predicate) ![]i16 {
    const mask = try materializePredicate(allocator, left_column, left);
    errdefer allocator.free(mask);
    try applyPredicateAnd(right_column, right, mask);
    return mask;
}

fn materializePredicate(allocator: std.mem.Allocator, column: Column, predicate: generic_sql.Predicate) ![]i16 {
    return switch (column) {
        .i16 => |values| materializePredicateTyped(i16, allocator, values, predicate),
        .i32, .date => |values| materializePredicateTyped(i32, allocator, values, predicate),
        .i64 => |values| materializePredicateTyped(i64, allocator, values, predicate),
    };
}

fn applyPredicateAnd(column: Column, predicate: generic_sql.Predicate, mask: []i16) !void {
    switch (column) {
        .i16 => |values| try applyPredicateAndTyped(i16, values, predicate, mask),
        .i32, .date => |values| try applyPredicateAndTyped(i32, values, predicate, mask),
        .i64 => |values| try applyPredicateAndTyped(i64, values, predicate, mask),
    }
}

fn materializePredicateTyped(comptime T: type, allocator: std.mem.Allocator, values: []const T, predicate: generic_sql.Predicate) ![]i16 {
    const target = std.math.cast(T, predicate.int_value) orelse return error.UnsupportedGenericQuery;
    const mask = try allocator.alloc(i16, values.len);
    errdefer allocator.free(mask);
    for (values, mask) |value, *out| {
        out.* = if (predicateMatches(T, value, target, predicate.op)) 1 else 0;
    }
    return mask;
}

fn applyPredicateAndTyped(comptime T: type, values: []const T, predicate: generic_sql.Predicate, mask: []i16) !void {
    if (values.len != mask.len) return error.InvalidGenericResult;
    const target = std.math.cast(T, predicate.int_value) orelse return error.UnsupportedGenericQuery;
    for (values, mask) |value, *out| {
        if (out.* != 0 and !predicateMatches(T, value, target, predicate.op)) out.* = 0;
    }
}

fn predicateMatches(comptime T: type, value: T, target: T, op: generic_sql.FilterOp) bool {
    return switch (op) {
        .equal => value == target,
        .not_equal => value != target,
        .greater => value > target,
        .greater_equal => value >= target,
        .less => value < target,
        .less_equal => value <= target,
    };
}

fn bindColumn(hot: HotColumns, name: []const u8) !Column {
    if (asciiEqlIgnoreCase(name, "AdvEngineID")) return .{ .i16 = hot.adv_engine_id };
    if (asciiEqlIgnoreCase(name, "ResolutionWidth")) return .{ .i16 = hot.resolution_width };
    if (asciiEqlIgnoreCase(name, "UserID")) return .{ .i64 = hot.user_id };
    if (asciiEqlIgnoreCase(name, "EventDate")) return .{ .date = hot.event_date };
    if (asciiEqlIgnoreCase(name, "CounterID")) return .{ .i32 = hot.counter_id };
    if (asciiEqlIgnoreCase(name, "IsRefresh")) return .{ .i16 = hot.is_refresh };
    if (asciiEqlIgnoreCase(name, "DontCountHits")) return .{ .i16 = hot.dont_count_hits };
    if (asciiEqlIgnoreCase(name, "length(URL)")) return .{ .i32 = hot.url_length orelse return error.UnsupportedGenericColumn };
    return error.UnsupportedGenericColumn;
}

fn bindFilterColumn(hot: HotColumns, name: []const u8) !Column {
    if (asciiEqlIgnoreCase(name, "URL")) return .{ .i32 = hot.url_length orelse return error.UnsupportedGenericColumn };
    return bindColumn(hot, name);
}

fn formatValues(allocator: std.mem.Allocator, plan: generic_sql.Plan, values: []const Value) ![]u8 {
    if (values.len != plan.projections.len) return error.InvalidGenericResult;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try writeHeader(&out, allocator, plan);
    for (values, 0..) |value, i| {
        if (i != 0) try out.append(allocator, ',');
        try writeValue(&out, allocator, value);
    }
    try out.append(allocator, '\n');
    return out.toOwnedSlice(allocator);
}

fn writeHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, plan: generic_sql.Plan) !void {
    for (plan.projections, 0..) |expr, i| {
        if (i != 0) try out.append(allocator, ',');
        switch (expr.func) {
            .column_ref => try out.print(allocator, "{s}", .{expr.column.?}),
            .int_literal => try out.print(allocator, "{d}", .{expr.int_offset}),
            .count_distinct => try out.print(allocator, "count(DISTINCT {s})", .{expr.column.?}),
            .count_star => try out.appendSlice(allocator, "count_star()"),
            .sum => if (expr.int_offset == 0) try out.print(allocator, "sum({s})", .{expr.column.?}) else try out.print(allocator, "sum(({s} + {d}))", .{ expr.column.?, expr.int_offset }),
            .avg => try out.print(allocator, "avg({s})", .{expr.column.?}),
            .min => try out.print(allocator, "min({s})", .{expr.column.?}),
            .max => try out.print(allocator, "max({s})", .{expr.column.?}),
        }
    }
    try out.append(allocator, '\n');
}

fn writeValue(out: *std.ArrayList(u8), allocator: std.mem.Allocator, value: Value) !void {
    switch (value) {
        .int => |v| try out.print(allocator, "{d}", .{v}),
        .float => |v| try out.print(allocator, "{d}", .{v}),
        .date => |v| {
            const text = dateString(@intCast(v));
            try out.print(allocator, "{s}", .{text});
        },
    }
}

fn avgFromSum(sum: i64, count: usize) f64 {
    if (count == 0) return 0;
    return @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(count));
}

fn dateString(days: u47) [10]u8 {
    const epoch_day = std.time.epoch.EpochDay{ .day = days };
    const yd = epoch_day.calculateYearDay();
    const md = yd.calculateMonthDay();
    var buf: [10]u8 = undefined;
    _ = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yd.year, md.month.numeric(), md.day_index + 1 }) catch unreachable;
    return buf;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}
