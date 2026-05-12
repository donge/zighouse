const std = @import("std");

pub const AggregateFn = enum { column_ref, count_star, count_distinct, sum, avg, min, max };

pub const Expr = struct {
    func: AggregateFn,
    column: ?[]const u8 = null,
    int_offset: i64 = 0,
};

pub const FilterOp = enum { equal, not_equal, greater, greater_equal, less, less_equal };

pub const Predicate = struct {
    column: []const u8,
    op: FilterOp,
    int_value: i64,
};

pub const Filter = struct {
    column: []const u8,
    op: FilterOp,
    int_value: i64,
    second: ?Predicate = null,
};

pub const Plan = struct {
    table: []const u8,
    projections: []const Expr,
    filter: ?Filter = null,
    group_by: ?[]const u8 = null,
    order_by_count_desc: bool = false,
    limit: ?usize = null,
};

pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !?Plan {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    const from_pos = indexOfKeyword(trimmed, "from") orelse return null;
    const select_kw = "select";
    if (!startsWithKeyword(trimmed, select_kw)) return null;

    const select_body = std.mem.trim(u8, trimmed[select_kw.len..from_pos], " \t\r\n");
    const after_from = std.mem.trim(u8, trimmed[from_pos + "from".len ..], " \t\r\n");
    if (select_body.len == 0 or after_from.len == 0) return null;

    const where_pos = indexOfKeyword(after_from, "where");
    const group_by_pos = indexOfKeywordPair(after_from, "group", "by");
    const order_by_pos = indexOfKeywordPair(after_from, "order", "by");
    const limit_pos = indexOfKeyword(after_from, "limit");
    const table_end = minOptionalPos(where_pos, minOptionalPos(group_by_pos, minOptionalPos(order_by_pos, limit_pos))) orelse after_from.len;
    const table_text = std.mem.trim(u8, after_from[0..table_end], " \t\r\n");
    if (!asciiEqlIgnoreCase(table_text, "hits")) return null;

    var projections: std.ArrayList(Expr) = .empty;
    errdefer projections.deinit(allocator);
    var parts = std.mem.splitScalar(u8, select_body, ',');
    while (parts.next()) |raw_part| {
        const part = std.mem.trim(u8, raw_part, " \t\r\n");
        if (part.len == 0) return null;
        try projections.append(allocator, parseExpr(part) orelse return null);
    }
    if (projections.items.len == 0) return null;

    const filter = if (where_pos) |pos| blk: {
        const where_end = minOptionalPos(group_by_pos, minOptionalPos(order_by_pos, limit_pos)) orelse after_from.len;
        if (where_end <= pos) return null;
        const where_body = std.mem.trim(u8, after_from[pos + "where".len .. where_end], " \t\r\n");
        break :blk parseFilter(where_body) orelse {
            projections.deinit(allocator);
            return null;
        };
    } else null;

    const group_by = if (group_by_pos) |pos| blk: {
        const group_by_end = minOptionalPos(order_by_pos, limit_pos) orelse after_from.len;
        if (group_by_end <= pos) return null;
        const group_body = std.mem.trim(u8, after_from[pos + "group".len + "by".len + 1 .. group_by_end], " \t\r\n");
        if (group_body.len == 0 or std.mem.indexOfScalar(u8, group_body, ',') != null) return null;
        break :blk group_body;
    } else null;

    const order_by_count_desc = if (order_by_pos) |pos| blk: {
        const order_end = limit_pos orelse after_from.len;
        const order_body = std.mem.trim(u8, after_from[pos + "order".len + "by".len + 1 .. order_end], " \t\r\n");
        break :blk asciiEqlIgnoreCase(order_body, "COUNT(*) DESC");
    } else false;
    if (order_by_pos != null and !order_by_count_desc) {
        projections.deinit(allocator);
        return null;
    }
    const limit = if (limit_pos) |pos| blk: {
        const limit_body = std.mem.trim(u8, after_from[pos + "limit".len ..], " \t\r\n");
        if (limit_body.len == 0 or std.mem.indexOfAny(u8, limit_body, " \t\r\n") != null) {
            projections.deinit(allocator);
            return null;
        }
        break :blk std.fmt.parseInt(usize, limit_body, 10) catch {
            projections.deinit(allocator);
            return null;
        };
    } else null;

    if (!validPlanShape(projections.items, filter, group_by)) {
        projections.deinit(allocator);
        return null;
    }

    return .{
        .table = table_text,
        .projections = try projections.toOwnedSlice(allocator),
        .filter = filter,
        .group_by = group_by,
        .order_by_count_desc = order_by_count_desc,
        .limit = limit,
    };
}

pub fn deinit(allocator: std.mem.Allocator, plan: Plan) void {
    allocator.free(plan.projections);
}

fn validPlanShape(projections: []const Expr, filter: ?Filter, group_by: ?[]const u8) bool {
    if (group_by == null) {
        if (projections.len == 1 and projections[0].func == .column_ref) return filter != null;
        for (projections) |expr| if (expr.func == .column_ref) return false;
        return true;
    }
    if (projections.len != 2) return false;
    if (projections[0].func != .column_ref or projections[1].func != .count_star) return false;
    return asciiEqlIgnoreCase(projections[0].column orelse return false, group_by.?);
}

fn parseExpr(expr: []const u8) ?Expr {
    if (parseCall(expr, "count")) |arg| {
        const trimmed_arg = std.mem.trim(u8, arg, " \t\r\n");
        if (std.mem.eql(u8, trimmed_arg, "*")) return .{ .func = .count_star };
        const distinct_kw = "distinct";
        if (startsWithKeyword(trimmed_arg, distinct_kw)) {
            const column = std.mem.trim(u8, trimmed_arg[distinct_kw.len..], " \t\r\n");
            if (isIdentifierText(column)) return .{ .func = .count_distinct, .column = column };
        }
        return null;
    }
    if (parseCall(expr, "sum")) |arg| {
        const sum_arg = parseSumArg(std.mem.trim(u8, arg, " \t\r\n")) orelse return null;
        return .{ .func = .sum, .column = sum_arg.column, .int_offset = sum_arg.int_offset };
    }
    if (parseCall(expr, "avg")) |arg| return .{ .func = .avg, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (parseCall(expr, "min")) |arg| return .{ .func = .min, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (parseCall(expr, "max")) |arg| return .{ .func = .max, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (isIdentifierText(expr)) return .{ .func = .column_ref, .column = expr };
    return null;
}

fn parseSumArg(arg: []const u8) ?struct { column: []const u8, int_offset: i64 } {
    if (isIdentifierText(arg)) return .{ .column = arg, .int_offset = 0 };
    const plus_pos = std.mem.indexOfScalar(u8, arg, '+') orelse return null;
    if (std.mem.indexOfScalar(u8, arg[plus_pos + 1 ..], '+') != null) return null;
    const column = std.mem.trim(u8, arg[0..plus_pos], " \t\r\n");
    const value_text = std.mem.trim(u8, arg[plus_pos + 1 ..], " \t\r\n");
    if (!isIdentifierText(column) or value_text.len == 0) return null;
    const int_offset = std.fmt.parseInt(i64, value_text, 10) catch return null;
    return .{ .column = column, .int_offset = int_offset };
}

fn parseCall(expr: []const u8, name: []const u8) ?[]const u8 {
    const open = std.mem.indexOfScalar(u8, expr, '(') orelse return null;
    const close = std.mem.lastIndexOfScalar(u8, expr, ')') orelse return null;
    if (close <= open) return null;
    if (std.mem.trim(u8, expr[close + 1 ..], " \t\r\n").len != 0) return null;
    const got_name = std.mem.trim(u8, expr[0..open], " \t\r\n");
    if (!asciiEqlIgnoreCase(got_name, name)) return null;
    return expr[open + 1 .. close];
}

fn parseFilter(where_body: []const u8) ?Filter {
    if (indexOfKeyword(where_body, "and")) |and_pos| {
        const right = std.mem.trim(u8, where_body[and_pos + "and".len ..], " \t\r\n");
        if (indexOfKeyword(right, "and") != null) return null;
        const first = parsePredicate(std.mem.trim(u8, where_body[0..and_pos], " \t\r\n")) orelse return null;
        const second = parsePredicate(right) orelse return null;
        return .{ .column = first.column, .op = first.op, .int_value = first.int_value, .second = second };
    }
    const predicate = parsePredicate(where_body) orelse return null;
    return .{ .column = predicate.column, .op = predicate.op, .int_value = predicate.int_value };
}

fn parsePredicate(where_body: []const u8) ?Predicate {
    const ParsedOp = struct { pos: usize, text: []const u8, op: FilterOp };
    const parsed_op = blk: {
        const ops = [_]struct { text: []const u8, op: FilterOp }{
            .{ .text = "<>", .op = .not_equal },
            .{ .text = ">=", .op = .greater_equal },
            .{ .text = "<=", .op = .less_equal },
            .{ .text = "=", .op = .equal },
            .{ .text = ">", .op = .greater },
            .{ .text = "<", .op = .less },
        };
        for (ops) |candidate| {
            if (std.mem.indexOf(u8, where_body, candidate.text)) |pos| {
                break :blk ParsedOp{ .pos = pos, .text = candidate.text, .op = candidate.op };
            }
        }
        return null;
    };
    const column = std.mem.trim(u8, where_body[0..parsed_op.pos], " \t\r\n");
    const value_text = std.mem.trim(u8, where_body[parsed_op.pos + parsed_op.text.len ..], " \t\r\n");
    if (column.len == 0 or value_text.len == 0) return null;
    const value = std.fmt.parseInt(i64, value_text, 10) catch return null;
    return .{ .column = column, .op = parsed_op.op, .int_value = value };
}

fn indexOfKeyword(sql: []const u8, keyword: []const u8) ?usize {
    var i: usize = 0;
    while (i + keyword.len <= sql.len) : (i += 1) {
        if (!asciiEqlIgnoreCase(sql[i .. i + keyword.len], keyword)) continue;
        const before_ok = i == 0 or !isIdent(sql[i - 1]);
        const after = i + keyword.len;
        const after_ok = after == sql.len or !isIdent(sql[after]);
        if (before_ok and after_ok) return i;
    }
    return null;
}

fn indexOfKeywordPair(sql: []const u8, first: []const u8, second: []const u8) ?usize {
    var search_from: usize = 0;
    while (search_from < sql.len) {
        const rel = indexOfKeyword(sql[search_from..], first) orelse return null;
        const pos = search_from + rel;
        const after_first = std.mem.trim(u8, sql[pos + first.len ..], " \t\r\n");
        if (startsWithKeyword(after_first, second)) return pos;
        search_from = pos + first.len;
    }
    return null;
}

fn minOptionalPos(a: ?usize, b: ?usize) ?usize {
    if (a) |av| if (b) |bv| return @min(av, bv) else return av;
    return b;
}

fn startsWithKeyword(sql: []const u8, keyword: []const u8) bool {
    if (sql.len < keyword.len) return false;
    if (!asciiEqlIgnoreCase(sql[0..keyword.len], keyword)) return false;
    return sql.len == keyword.len or !isIdent(sql[keyword.len]);
}

fn isIdent(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn isIdentifierText(text: []const u8) bool {
    if (text.len == 0) return false;
    for (text) |c| if (!isIdent(c)) return false;
    return true;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}

test "parses count star" {
    const plan = (try parse(std.testing.allocator, " select count(*) from hits; ")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqualStrings("hits", plan.table);
    try std.testing.expectEqual(@as(usize, 1), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[0].func);
    try std.testing.expect(plan.filter == null);
}

test "parses aggregate list" {
    const plan = (try parse(std.testing.allocator, "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 3), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.sum, plan.projections[0].func);
    try std.testing.expectEqualStrings("AdvEngineID", plan.projections[0].column.?);
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[1].func);
    try std.testing.expectEqual(AggregateFn.avg, plan.projections[2].func);
    try std.testing.expectEqualStrings("ResolutionWidth", plan.projections[2].column.?);
}

test "parses count distinct" {
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(DISTINCT UserID) FROM hits")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 1), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.count_distinct, plan.projections[0].func);
    try std.testing.expectEqualStrings("UserID", plan.projections[0].column.?);
}

test "parses sum with integer offset" {
    const plan = (try parse(std.testing.allocator, "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 42) FROM hits")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 2), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.sum, plan.projections[0].func);
    try std.testing.expectEqualStrings("ResolutionWidth", plan.projections[0].column.?);
    try std.testing.expectEqual(@as(i64, 0), plan.projections[0].int_offset);
    try std.testing.expectEqual(AggregateFn.sum, plan.projections[1].func);
    try std.testing.expectEqualStrings("ResolutionWidth", plan.projections[1].column.?);
    try std.testing.expectEqual(@as(i64, 42), plan.projections[1].int_offset);
}

test "parses group by count query" {
    const plan = (try parse(std.testing.allocator, "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 2), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.column_ref, plan.projections[0].func);
    try std.testing.expectEqualStrings("AdvEngineID", plan.projections[0].column.?);
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[1].func);
    try std.testing.expectEqualStrings("AdvEngineID", plan.group_by.?);
    try std.testing.expect(plan.order_by_count_desc);
}

test "parses group by count limit" {
    const plan = (try parse(std.testing.allocator, "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 2), plan.projections.len);
    try std.testing.expectEqualStrings("UserID", plan.group_by.?);
    try std.testing.expect(plan.order_by_count_desc);
    try std.testing.expectEqual(@as(?usize, 10), plan.limit);
}

test "rejects unsupported order by" {
    try std.testing.expect((try parse(std.testing.allocator, "SELECT AdvEngineID, COUNT(*) FROM hits GROUP BY AdvEngineID ORDER BY AdvEngineID")) == null);
}

test "parses not equal filter" {
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expect(plan.filter != null);
    try std.testing.expectEqualStrings("AdvEngineID", plan.filter.?.column);
    try std.testing.expectEqual(FilterOp.not_equal, plan.filter.?.op);
    try std.testing.expectEqual(@as(i64, 0), plan.filter.?.int_value);
}

test "parses comparison filters" {
    const cases = [_]struct { sql: []const u8, op: FilterOp, value: i64 }{
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID = 1", .op = .equal, .value = 1 },
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE ResolutionWidth > 1024", .op = .greater, .value = 1024 },
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE ResolutionWidth >= 1024", .op = .greater_equal, .value = 1024 },
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE ResolutionWidth < 1024", .op = .less, .value = 1024 },
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE ResolutionWidth <= 1024", .op = .less_equal, .value = 1024 },
        .{ .sql = "SELECT COUNT(*) FROM hits WHERE AdvEngineID=-1", .op = .equal, .value = -1 },
    };
    for (cases) |case| {
        const plan = (try parse(std.testing.allocator, case.sql)).?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.filter != null);
        try std.testing.expectEqual(case.op, plan.filter.?.op);
        try std.testing.expectEqual(case.value, plan.filter.?.int_value);
    }
}

test "parses two predicate and filter" {
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM hits WHERE CounterID = 62 AND IsRefresh = 0")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expect(plan.filter != null);
    try std.testing.expectEqualStrings("CounterID", plan.filter.?.column);
    try std.testing.expectEqual(FilterOp.equal, plan.filter.?.op);
    try std.testing.expectEqual(@as(i64, 62), plan.filter.?.int_value);
    try std.testing.expect(plan.filter.?.second != null);
    try std.testing.expectEqualStrings("IsRefresh", plan.filter.?.second.?.column);
    try std.testing.expectEqual(FilterOp.equal, plan.filter.?.second.?.op);
    try std.testing.expectEqual(@as(i64, 0), plan.filter.?.second.?.int_value);
}

test "rejects unsupported sql" {
    try std.testing.expect((try parse(std.testing.allocator, "SELECT URL FROM hits")) == null);
    try std.testing.expect((try parse(std.testing.allocator, "SELECT COUNT(*) FROM other")) == null);
    try std.testing.expect((try parse(std.testing.allocator, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 AND ResolutionWidth > 0 AND IsRefresh = 0")) == null);
}

test "parses filtered column projection" {
    const plan = (try parse(std.testing.allocator, "SELECT UserID FROM hits WHERE UserID = 435090932899640449")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 1), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.column_ref, plan.projections[0].func);
    try std.testing.expectEqualStrings("UserID", plan.projections[0].column.?);
    try std.testing.expect(plan.filter != null);
}
