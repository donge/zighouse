const std = @import("std");

pub const AggregateFn = enum { count_star, sum, avg, min, max };

pub const Expr = struct {
    func: AggregateFn,
    column: ?[]const u8 = null,
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
    const table_text = if (where_pos) |pos| std.mem.trim(u8, after_from[0..pos], " \t\r\n") else after_from;
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
        const where_body = std.mem.trim(u8, after_from[pos + "where".len ..], " \t\r\n");
        break :blk parseFilter(where_body) orelse {
            projections.deinit(allocator);
            return null;
        };
    } else null;

    return .{
        .table = table_text,
        .projections = try projections.toOwnedSlice(allocator),
        .filter = filter,
    };
}

pub fn deinit(allocator: std.mem.Allocator, plan: Plan) void {
    allocator.free(plan.projections);
}

fn parseExpr(expr: []const u8) ?Expr {
    if (parseCall(expr, "count")) |arg| {
        if (std.mem.eql(u8, std.mem.trim(u8, arg, " \t\r\n"), "*")) return .{ .func = .count_star };
        return null;
    }
    if (parseCall(expr, "sum")) |arg| return .{ .func = .sum, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (parseCall(expr, "avg")) |arg| return .{ .func = .avg, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (parseCall(expr, "min")) |arg| return .{ .func = .min, .column = std.mem.trim(u8, arg, " \t\r\n") };
    if (parseCall(expr, "max")) |arg| return .{ .func = .max, .column = std.mem.trim(u8, arg, " \t\r\n") };
    return null;
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

fn startsWithKeyword(sql: []const u8, keyword: []const u8) bool {
    if (sql.len < keyword.len) return false;
    if (!asciiEqlIgnoreCase(sql[0..keyword.len], keyword)) return false;
    return sql.len == keyword.len or !isIdent(sql[keyword.len]);
}

fn isIdent(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
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
