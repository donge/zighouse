const std = @import("std");
const duckdb_parse = @import("duckdb_parse.zig");

pub const AggregateFn = enum {
    column_ref, int_literal, float_literal,
    count_star, count_distinct,
    count_if,         // countIf(expr) — condition stored in cond_col/cond_op/cond_val
    sum, avg, min, max,
    uniq_exact,       // uniqExact(col) — exact distinct count using string set
    uniq_exact_if,    // uniqExactIf(col, cond) — conditional exact distinct
    group_uniq_array, // groupUniqArray(col) — array of distinct values (joined as string)
    any_val,          // any(col) — first non-null value
    case_when,        // CASE WHEN … THEN … ELSE … END — data in case_when_data field
};

/// Optional inline condition for countIf / uniqExactIf:
///   cond_col op cond_num   (e.g. confidence >= 0.9)
///   cond_col op cond_str   (e.g. data['is_foreign'] = 'true')
pub const CondExpr = struct {
    cond_col: []const u8 = "",   // heap-allocated condition column name (empty when cond_text is set)
    cond_op:  CmpOp = .eq,
    cond_num: f64 = 0,       // used when cond_str and cond_text are null
    cond_str: ?[]const u8 = null, // heap-allocated; non-null for string comparisons
    cond_text: ?[]const u8 = null, // heap-allocated; non-null for complex conditions (use evalTextBoolExpr)
};

pub const Expr = struct {
    func: AggregateFn,
    column: ?[]const u8 = null,
    int_offset: i64 = 0,
    float_val: f64 = 0.0,
    alias: ?[]const u8 = null,
    /// Inline condition for countIf / uniqExactIf (owned, free in deinit).
    cond: ?*CondExpr = null,
    /// Separator for group_uniq_array result (default ", "). Owned when plan.owned=true.
    sep: ?[]const u8 = null,
    /// For group_uniq_array wrapped by an outer function (e.g. arraySlice, arrayDistinct).
    /// Template where "$" is replaced by the aggregate result at eval time.
    /// Example: "arraySlice($, 1, 5)"
    /// Heap-allocated; freed in deinit when plan.owned=true.
    post_fn: ?[]const u8 = null,
    /// For case_when: parallel when/then text slices + optional else text. Heap-allocated.
    case_when_data: ?*CaseWhenData = null,
};

/// Parallel when/then text pairs for CASE WHEN expressions.
pub const CaseWhenData = struct {
    /// Each element is a heap-allocated SQL text for the WHEN condition.
    when_texts: [][]const u8,
    /// Each element is a heap-allocated SQL text for the THEN value.
    then_texts: [][]const u8,
    /// Optional ELSE text (heap-allocated).
    else_text: ?[]const u8,
};

// ── WhereNode: generic predicate tree ─────────────────────────────────────────
//
// Represents an arbitrary WHERE / HAVING clause as a typed tree.
// Used by generic_executor.zig to evaluate predicates over streaming rows.
// native.zig continues to use the legacy Filter struct for its specialised paths.

pub const CmpOp = enum { eq, ne, lt, le, gt, ge };
pub const LikeOp = enum { like, not_like, ilike };

pub const WhereNode = union(enum) {
    /// col op int  (e.g. Age > 0, EventDate >= 15887)
    cmp_int: struct { col: []const u8, op: CmpOp, val: i64 },
    /// col op 'str'  (e.g. SearchPhrase <> '', URL = 'foo')
    cmp_str: struct { col: []const u8, op: CmpOp, val: []const u8 },
    /// col LIKE / NOT LIKE / ILIKE 'pattern'
    like: struct { col: []const u8, op: LikeOp, pattern: []const u8 },
    /// col IS NULL
    is_null: []const u8,
    /// col IS NOT NULL
    is_not_null: []const u8,
    /// AND / OR over a list of children (children slice is owned by allocator)
    and_: []const *WhereNode,
    or_: []const *WhereNode,
};

/// Recursively free a WhereNode tree.
pub fn freeWhereNode(allocator: std.mem.Allocator, node: *WhereNode) void {
    switch (node.*) {
        .cmp_int => |c| allocator.free(c.col),
        .cmp_str => |c| { allocator.free(c.col); allocator.free(c.val); },
        .like    => |l| { allocator.free(l.col); allocator.free(l.pattern); },
        .is_null, .is_not_null => |col| allocator.free(col),
        .and_, .or_ => |children| {
            for (children) |ch| freeWhereNode(allocator, ch);
            allocator.free(children);
        },
    }
    allocator.destroy(node);
}

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
    /// Typed WHERE predicate tree (superset of `filter`).
    /// Populated by the DuckDB-backed parser; null when using the legacy parser.
    /// Free with freeWhereNode(allocator, where_expr) before calling deinit.
     where_expr: ?*WhereNode = null,
     where_text: ?[]const u8 = null,
     group_by: ?[]const u8 = null,
     having_expr: ?*WhereNode = null,
     having_text: ?[]const u8 = null,
    order_by_count_desc: bool = false,
    order_by_alias: ?[]const u8 = null,
    /// When true, order_by_alias is ascending; when false (default), it is descending.
    order_by_alias_asc: bool = false,
    order_by_text: ?[]const u8 = null,
    limit: ?usize = null,
    offset: ?usize = null,
    /// Subquery in FROM clause: `SELECT … FROM (SELECT …) AS t`.
    /// When set, `table` is the subquery alias (or "__subquery__").
    /// Free with deinit(allocator, subquery_source.*) then destroy.
    subquery_source: ?*Plan = null,
    /// UNION ALL right-hand side plan. When set, executor runs both plans and concatenates rows.
    /// Free with deinit(allocator, union_other.*) then destroy.
    union_other: ?*Plan = null,
    /// When true, all string fields (table, where_text, group_by, having_text,
    /// order_by_alias, order_by_text) were heap-allocated by the DuckDB parser
    /// and must be freed by deinit().  Legacy parser uses SQL slices (no free).
    owned: bool = false,
};

/// Parse `sql` into a Plan.  Tries the DuckDB-backed parser first (when DuckDB
/// is linked); falls back to the legacy hand-written parser on failure.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !?Plan {
    return duckdb_parse.parse(allocator, sql) catch null;
}

pub fn deinit(allocator: std.mem.Allocator, plan: Plan) void {
    if (plan.subquery_source) |sq| { deinit(allocator, sq.*); allocator.destroy(sq); }
    if (plan.union_other) |uo| { deinit(allocator, uo.*); allocator.destroy(uo); }
    if (plan.where_expr) |we| freeWhereNode(allocator, we);
    if (plan.having_expr) |he| freeWhereNode(allocator, he);
    // Always free cond expressions in projections (they are always heap-allocated).
    for (plan.projections) |expr| {
        if (expr.cond) |c| {
            if (c.cond_col.len > 0) allocator.free(c.cond_col);
            if (c.cond_str) |s| allocator.free(s);
            if (c.cond_text) |s| allocator.free(s);
            allocator.destroy(c);
        }
    }
    if (plan.owned) {
        allocator.free(plan.table);
        if (plan.where_text) |s| allocator.free(s);
        if (plan.group_by) |s| allocator.free(s);
        if (plan.having_text) |s| allocator.free(s);
        if (plan.order_by_alias) |s| allocator.free(s);
        if (plan.order_by_text) |s| allocator.free(s);
        // Also free alias strings inside projections
        for (plan.projections) |expr| {
            if (expr.alias) |a| allocator.free(a);
            if (expr.column) |col| allocator.free(col);
            if (expr.sep) |s| allocator.free(s);
            if (expr.post_fn) |s| allocator.free(s);
            if (expr.case_when_data) |cwd| {
                for (cwd.when_texts) |t| allocator.free(t);
                for (cwd.then_texts) |t| allocator.free(t);
                allocator.free(cwd.when_texts);
                allocator.free(cwd.then_texts);
                if (cwd.else_text) |t| allocator.free(t);
                allocator.destroy(cwd);
            }
        }
    }
    allocator.free(plan.projections);
}


pub fn parseFilter(where_body: []const u8) ?Filter {
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
    const value = if (std.mem.eql(u8, value_text, "''")) 0 else std.fmt.parseInt(i64, value_text, 10) catch return null;
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



fn projectionAliasExists(projections: []const Expr, alias: []const u8) bool {
    for (projections) |expr| {
        if (expr.alias) |candidate| if (asciiEqlIgnoreCase(candidate, alias)) return true;
    }
    return false;
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
}

test "table name is passed through without validation" {
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM events")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqualStrings("events", plan.table);
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[0].func);
}

test "table name is case preserved" {
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM Hits")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqualStrings("Hits", plan.table);
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

test "parses region distinct alias top" {
    const plan = (try parse(std.testing.allocator, "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 2), plan.projections.len);
    try std.testing.expectEqualStrings("RegionID", plan.group_by.?);
    try std.testing.expectEqual(AggregateFn.count_distinct, plan.projections[1].func);
    try std.testing.expectEqualStrings("u", plan.projections[1].alias.?);
    try std.testing.expectEqualStrings("u", plan.order_by_alias.?);
    try std.testing.expectEqual(@as(?usize, 10), plan.limit);
}

test "parses region stats distinct alias top" {
    const plan = (try parse(std.testing.allocator, "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 5), plan.projections.len);
    try std.testing.expectEqualStrings("RegionID", plan.group_by.?);
    try std.testing.expectEqual(AggregateFn.count_star, plan.projections[2].func);
    try std.testing.expectEqualStrings("c", plan.projections[2].alias.?);
    try std.testing.expectEqual(AggregateFn.count_distinct, plan.projections[4].func);
    try std.testing.expectEqualStrings("c", plan.order_by_alias.?);
    try std.testing.expectEqual(@as(?usize, 10), plan.limit);
}

test "parses clickbench string top shapes" {
    const cases = [_][]const u8{
        "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10",
        "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10",
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
        "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10",
        "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10",
    };
    for (cases) |sql| {
        const plan = (try parse(std.testing.allocator, sql)).?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.filter != null);
        try std.testing.expectEqual(FilterOp.not_equal, plan.filter.?.op);
        try std.testing.expectEqual(@as(i64, 0), plan.filter.?.int_value);
        try std.testing.expect(plan.group_by != null);
        try std.testing.expect(plan.order_by_alias != null);
        try std.testing.expectEqual(@as(?usize, 10), plan.limit);
    }
}

test "parses user search phrase top shapes" {
    const q17 = (try parse(std.testing.allocator, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q17);
    try std.testing.expectEqual(@as(usize, 3), q17.projections.len);
    try std.testing.expectEqualStrings("UserID, SearchPhrase", q17.group_by.?);
    try std.testing.expect(q17.order_by_count_desc);

    const q18 = (try parse(std.testing.allocator, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10")).?;
    defer deinit(std.testing.allocator, q18);
    try std.testing.expectEqual(@as(usize, 3), q18.projections.len);
    try std.testing.expectEqualStrings("UserID, SearchPhrase", q18.group_by.?);
    try std.testing.expect(!q18.order_by_count_desc);

    const q19 = (try parse(std.testing.allocator, "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q19);
    try std.testing.expectEqual(@as(usize, 4), q19.projections.len);
    try std.testing.expectEqualStrings("EventMinuteOfHour", q19.projections[1].column.?);
    try std.testing.expectEqualStrings("m", q19.projections[1].alias.?);
    try std.testing.expectEqualStrings("UserID, m, SearchPhrase", q19.group_by.?);
    try std.testing.expect(q19.order_by_count_desc);
}

test "parses client ip and url top shapes" {
    const cases = [_][]const u8{
        "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10",
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
        "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10",
        "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
        "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
        "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
    };
    for (cases) |sql| {
        const plan = (try parse(std.testing.allocator, sql)).?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.group_by != null);
        try std.testing.expectEqualStrings("c", plan.order_by_alias.?);
        try std.testing.expectEqual(@as(?usize, 10), plan.limit);
    }
}

test "parses dashboard string top shapes" {
    const cases = [_]struct { sql: []const u8, group_by: []const u8 }{
        .{ .sql = "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10", .group_by = "URL" },
        .{ .sql = "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10", .group_by = "Title" },
    };
    for (cases) |case| {
        const maybe = try parse(std.testing.allocator, case.sql);
        try std.testing.expect(maybe != null);
        const plan = maybe.?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.where_text != null);
        try std.testing.expectEqualStrings(case.group_by, plan.group_by orelse return error.NullGroupBy);
        try std.testing.expectEqualStrings("PageViews", plan.order_by_alias orelse return error.NullOrderByAlias);
        try std.testing.expectEqual(@as(?usize, 10), plan.limit);
        try std.testing.expectEqual(@as(?usize, null), plan.offset);
    }

    const q39 = (try parse(std.testing.allocator, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000")) orelse return error.ParseNull;
    defer deinit(std.testing.allocator, q39);
    try std.testing.expect(q39.where_text != null);
    try std.testing.expectEqualStrings("URL", q39.group_by orelse return error.NullGroupBy);
    try std.testing.expectEqualStrings("PageViews", q39.order_by_alias orelse return error.NullOrderByAlias);
    try std.testing.expectEqual(@as(?usize, 10), q39.limit);
    try std.testing.expectEqual(@as(?usize, 1000), q39.offset);

    const q42 = (try parse(std.testing.allocator, "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000")) orelse return error.ParseNull;
    defer deinit(std.testing.allocator, q42);
    try std.testing.expect(q42.where_text != null);
    try std.testing.expectEqualStrings("WindowClientWidth, WindowClientHeight", q42.group_by orelse return error.NullGroupBy);
    try std.testing.expectEqualStrings("PageViews", q42.order_by_alias orelse return error.NullOrderByAlias);
    try std.testing.expectEqual(@as(?usize, 10), q42.limit);
    try std.testing.expectEqual(@as(?usize, 10000), q42.offset);

    const q41 = (try parse(std.testing.allocator, "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100")) orelse return error.ParseNull;
    defer deinit(std.testing.allocator, q41);
    try std.testing.expect(q41.where_text != null);
    try std.testing.expectEqualStrings("URLHash, EventDate", q41.group_by orelse return error.NullGroupBy);
    try std.testing.expectEqualStrings("PageViews", q41.order_by_alias orelse return error.NullOrderByAlias);
    try std.testing.expectEqual(@as(?usize, 10), q41.limit);
    try std.testing.expectEqual(@as(?usize, 100), q41.offset);

    const q43 = (try parse(std.testing.allocator, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000")) orelse return error.ParseNull;
    defer deinit(std.testing.allocator, q43);
    try std.testing.expect(q43.where_text != null);
    try std.testing.expectEqualStrings("EventMinute", q43.projections[0].column orelse return error.NullColumn);
    try std.testing.expectEqualStrings("M", q43.projections[0].alias orelse return error.NullAlias);
    try std.testing.expectEqualStrings("date_trunc('minute', EventTime)", q43.group_by orelse return error.NullGroupBy);
    try std.testing.expectEqualStrings("date_trunc('minute', EventTime) ASC", q43.order_by_text orelse return error.NullOrderByText);
    try std.testing.expect(q43.order_by_alias == null);
    try std.testing.expectEqual(@as(?usize, 10), q43.limit);
    try std.testing.expectEqual(@as(?usize, 1000), q43.offset);
}

test "parses search phrase order limit shapes" {
    const cases = [_][]const u8{
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10",
    };
    for (cases) |sql| {
        const plan = (try parse(std.testing.allocator, sql)).?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.filter != null);
        try std.testing.expectEqualStrings("SearchPhrase", plan.projections[0].column.?);
        try std.testing.expect(plan.order_by_text != null);
        try std.testing.expectEqual(@as(?usize, 10), plan.limit);
        try std.testing.expectEqual(@as(?usize, null), plan.offset);
    }
}

test "parses google like top shapes" {
    const q21 = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'")).?;
    defer deinit(std.testing.allocator, q21);
    try std.testing.expectEqualStrings("URL LIKE '%google%'", q21.where_text.?);
    try std.testing.expectEqual(AggregateFn.count_star, q21.projections[0].func);

    const q22 = (try parse(std.testing.allocator, "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q22);
    try std.testing.expectEqualStrings("SearchPhrase", q22.group_by.?);
    // DuckDB parser extracts alias-based ORDER BY into order_by_alias (not order_by_text)
    try std.testing.expectEqualStrings("c", q22.order_by_alias.?);
    try std.testing.expectEqual(false, q22.order_by_alias_asc);
    try std.testing.expectEqual(AggregateFn.min, q22.projections[1].func);
    try std.testing.expectEqual(AggregateFn.count_star, q22.projections[2].func);

    const q23 = (try parse(std.testing.allocator, "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q23);
    try std.testing.expectEqualStrings("SearchPhrase", q23.group_by.?);
    try std.testing.expectEqualStrings("c", q23.order_by_alias.?);
    try std.testing.expectEqual(false, q23.order_by_alias_asc);
    try std.testing.expectEqual(AggregateFn.min, q23.projections[1].func);
    try std.testing.expectEqual(AggregateFn.min, q23.projections[2].func);
    try std.testing.expectEqual(AggregateFn.count_distinct, q23.projections[4].func);
}

test "parses URL length by counter shape" {
    const plan = (try parse(std.testing.allocator, "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expect(plan.filter != null);
    try std.testing.expectEqualStrings("URL", plan.filter.?.column);
    try std.testing.expectEqualStrings("CounterID", plan.group_by.?);
    // DuckDB normalizes COUNT(*) to count_star() in having_text
    try std.testing.expect(std.mem.indexOf(u8, plan.having_text.?, "100000") != null);
    // DuckDB parser extracts alias-based ORDER BY into order_by_alias
    try std.testing.expectEqualStrings("l", plan.order_by_alias.?);
    try std.testing.expectEqual(false, plan.order_by_alias_asc);
    try std.testing.expectEqualStrings("length(URL)", plan.projections[1].column.?);
    try std.testing.expectEqualStrings("l", plan.projections[1].alias.?);
    try std.testing.expectEqual(@as(?usize, 25), plan.limit);
}

test "accepts group by with arbitrary order by via generic path" {
    // Previously rejected, now accepted by the generic executor path
    const plan = (try parse(std.testing.allocator, "SELECT AdvEngineID, COUNT(*) FROM hits GROUP BY AdvEngineID ORDER BY AdvEngineID")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqualStrings("AdvEngineID", plan.group_by.?);
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
    // Since generic route was broadened, plain column projections are now valid.
    // "SELECT URL FROM hits" should now parse successfully.
    const plan_url = (try parse(std.testing.allocator, "SELECT URL FROM hits")).?;
    defer deinit(std.testing.allocator, plan_url);
    try std.testing.expectEqual(@as(usize, 1), plan_url.projections.len);

    // Non-"hits" table names are now parsed successfully; callers (e.g.
    // Native.executeGenericSql) validate the table name and return
    // error.UnknownTable for unrecognised tables.
    // Note: complex multi-condition WHERE queries with aggregates are now
    // accepted by the generic executor path (where_expr + validGenericShape).
    const plan = (try parse(std.testing.allocator, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 AND ResolutionWidth > 0 AND IsRefresh = 0")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expect(plan.where_expr != null);
}

test "parses filtered column projection" {
    const plan = (try parse(std.testing.allocator, "SELECT UserID FROM hits WHERE UserID = 435090932899640449")).?;
    defer deinit(std.testing.allocator, plan);
    try std.testing.expectEqual(@as(usize, 1), plan.projections.len);
    try std.testing.expectEqual(AggregateFn.column_ref, plan.projections[0].func);
    try std.testing.expectEqualStrings("UserID", plan.projections[0].column.?);
    try std.testing.expect(plan.filter != null);
}
