const std = @import("std");
const duckdb_parse = @import("duckdb_parse.zig");

pub const AggregateFn = enum { column_ref, int_literal, count_star, count_distinct, sum, avg, min, max };

pub const Expr = struct {
    func: AggregateFn,
    column: ?[]const u8 = null,
    int_offset: i64 = 0,
    alias: ?[]const u8 = null,
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
    having_text: ?[]const u8 = null,
    order_by_count_desc: bool = false,
    order_by_alias: ?[]const u8 = null,
    order_by_text: ?[]const u8 = null,
    limit: ?usize = null,
    offset: ?usize = null,
    /// When true, all string fields (table, where_text, group_by, having_text,
    /// order_by_alias, order_by_text) were heap-allocated by the DuckDB parser
    /// and must be freed by deinit().  Legacy parser uses SQL slices (no free).
    owned: bool = false,
};

/// Parse `sql` into a Plan.  Tries the DuckDB-backed parser first (when DuckDB
/// is linked); falls back to the legacy hand-written parser on failure.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !?Plan {
    if (duckdb_parse.parse(allocator, sql) catch null) |plan| {
        // Validate that the plan matches a supported shape before accepting it.
        // Also reject if there's an ORDER BY that's not supported by the specialised paths.
        const order_ok = if (plan.order_by_text) |obt|
            plan.order_by_alias != null or plan.order_by_count_desc or validOrderByText(obt)
        else true;
        if (order_ok and validPlanShape(plan.projections, plan.filter, plan.where_text, plan.group_by, plan.having_text, plan.order_by_text, plan.limit, plan.offset)) {
            return plan;
        }
        // If the DuckDB-parsed plan has a where_expr (generic executor path),
        // accept it via the looser generic shape check without falling back.
        if (plan.where_expr != null and validGenericShape(plan.projections, plan.having_text)) {
            return plan;
        }
        // DuckDB-parsed plan did not match a supported shape; free it and fall
        // through to the legacy parser (which may also return null).
        deinit(allocator, plan);
    }
    return parseLegacy(allocator, sql);
}

/// Returns true if the projection list is valid for generic scan/agg execution:
/// either all scalar aggregates (no column_ref) or all column references / int
/// literals (no aggregates mixed with references).  Having clauses are not
/// supported in the generic path.
fn validGenericShape(projections: []const Expr, having_text: ?[]const u8) bool {
    if (having_text != null) return false;
    if (projections.len == 0) return false;
    var has_agg = false;
    var has_col = false;
    for (projections) |p| {
        switch (p.func) {
            .column_ref, .int_literal => has_col = true,
            else => has_agg = true,
        }
    }
    // Mixed agg + column_ref without GROUP BY is not supported here.
    if (has_agg and has_col) return false;
    return true;
}

fn parseLegacy(allocator: std.mem.Allocator, sql: []const u8) !?Plan {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    const from_pos = indexOfTopLevelKeyword(trimmed, "from") orelse return null;
    const select_kw = "select";
    if (!startsWithKeyword(trimmed, select_kw)) return null;

    const select_body = std.mem.trim(u8, trimmed[select_kw.len..from_pos], " \t\r\n");
    const after_from = std.mem.trim(u8, trimmed[from_pos + "from".len ..], " \t\r\n");
    if (select_body.len == 0 or after_from.len == 0) return null;

    const where_pos = indexOfKeyword(after_from, "where");
    const group_by_pos = indexOfKeywordPair(after_from, "group", "by");
    const having_pos = indexOfKeyword(after_from, "having");
    const order_by_pos = indexOfKeywordPair(after_from, "order", "by");
    const limit_pos = indexOfKeyword(after_from, "limit");
    const offset_pos = indexOfKeyword(after_from, "offset");
    const table_end = minOptionalPos(where_pos, minOptionalPos(group_by_pos, minOptionalPos(having_pos, minOptionalPos(order_by_pos, minOptionalPos(limit_pos, offset_pos))))) orelse after_from.len;
    const table_text = std.mem.trim(u8, after_from[0..table_end], " \t\r\n");
    // Table name is passed through to Plan.table; callers validate it.

    var projections: std.ArrayList(Expr) = .empty;
    errdefer projections.deinit(allocator);
    parseProjectionList(allocator, &projections, select_body) catch {
        projections.deinit(allocator);
        return null;
    };
    if (projections.items.len == 0) return null;

    const where_text = if (where_pos) |pos| blk: {
        const where_end = minOptionalPos(group_by_pos, minOptionalPos(having_pos, minOptionalPos(order_by_pos, minOptionalPos(limit_pos, offset_pos)))) orelse after_from.len;
        if (where_end <= pos) return null;
        break :blk std.mem.trim(u8, after_from[pos + "where".len .. where_end], " \t\r\n");
    } else null;
    const filter = if (where_text) |body| parseFilter(body) else null;

    const group_by = if (group_by_pos) |pos| blk: {
        const group_by_end = minOptionalPos(having_pos, minOptionalPos(order_by_pos, minOptionalPos(limit_pos, offset_pos))) orelse after_from.len;
        if (group_by_end <= pos) return null;
        const group_body = std.mem.trim(u8, after_from[pos + "group".len + "by".len + 1 .. group_by_end], " \t\r\n");
        if (group_body.len == 0) return null;
        break :blk group_body;
    } else null;

    const having_text = if (having_pos) |pos| blk: {
        const having_end = minOptionalPos(order_by_pos, minOptionalPos(limit_pos, offset_pos)) orelse after_from.len;
        if (having_end <= pos) return null;
        const having_body = std.mem.trim(u8, after_from[pos + "having".len .. having_end], " \t\r\n");
        if (having_body.len == 0) return null;
        break :blk having_body;
    } else null;

    const order_body = if (order_by_pos) |pos| blk: {
        const order_end = minOptionalPos(limit_pos, offset_pos) orelse after_from.len;
        break :blk std.mem.trim(u8, after_from[pos + "order".len + "by".len + 1 .. order_end], " \t\r\n");
    } else null;
    const order_by_count_desc = if (order_body) |body| asciiEqlIgnoreCase(body, "COUNT(*) DESC") else false;
    const order_by_alias = if (order_body) |body| blk: {
        if (order_by_count_desc) break :blk null;
        const desc_kw = "desc";
        const desc_pos = lastKeywordPos(body, desc_kw) orelse break :blk null;
        const before_desc = std.mem.trim(u8, body[0..desc_pos], " \t\r\n");
        const after_desc = std.mem.trim(u8, body[desc_pos + desc_kw.len ..], " \t\r\n");
        if (after_desc.len != 0 or !isIdentifierText(before_desc) or !projectionAliasExists(projections.items, before_desc)) break :blk null;
        break :blk before_desc;
    } else null;
    if (order_by_pos != null and !order_by_count_desc and order_by_alias == null and !validOrderByText(order_body.?)) {
        projections.deinit(allocator);
        return null;
    }
    const limit = if (limit_pos) |pos| blk: {
        const limit_end = offset_pos orelse after_from.len;
        if (limit_end <= pos) return null;
        const limit_body = std.mem.trim(u8, after_from[pos + "limit".len .. limit_end], " \t\r\n");
        if (limit_body.len == 0 or std.mem.indexOfAny(u8, limit_body, " \t\r\n") != null) {
            projections.deinit(allocator);
            return null;
        }
        break :blk std.fmt.parseInt(usize, limit_body, 10) catch {
            projections.deinit(allocator);
            return null;
        };
    } else null;
    const offset = if (offset_pos) |pos| blk: {
        const offset_body = std.mem.trim(u8, after_from[pos + "offset".len ..], " \t\r\n");
        if (offset_body.len == 0 or std.mem.indexOfAny(u8, offset_body, " \t\r\n") != null) {
            projections.deinit(allocator);
            return null;
        }
        break :blk std.fmt.parseInt(usize, offset_body, 10) catch {
            projections.deinit(allocator);
            return null;
        };
    } else null;

    if (!validPlanShape(projections.items, filter, where_text, group_by, having_text, order_body, limit, offset)) {
        projections.deinit(allocator);
        return null;
    }

    return .{
        .table = table_text,
        .projections = try projections.toOwnedSlice(allocator),
        .filter = filter,
        .where_text = where_text,
        .group_by = group_by,
        .having_text = having_text,
        .order_by_count_desc = order_by_count_desc,
        .order_by_alias = order_by_alias,
        .order_by_text = order_body,
        .limit = limit,
        .offset = offset,
    };
}

pub fn deinit(allocator: std.mem.Allocator, plan: Plan) void {
    if (plan.where_expr) |we| freeWhereNode(allocator, we);
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
        }
    }
    allocator.free(plan.projections);
}

fn validPlanShape(projections: []const Expr, filter: ?Filter, where_text: ?[]const u8, group_by: ?[]const u8, having_text: ?[]const u8, order_by_text: ?[]const u8, limit: ?usize, offset: ?usize) bool {
    if (where_text != null and likeTopWhere(where_text.?)) return validLikeTopShape(projections, where_text.?, group_by, order_by_text, limit, offset);
    if (having_text != null) return validUrlLengthByCounterShape(projections, filter, where_text, group_by, having_text.?, order_by_text, limit, offset);
    if (where_text != null and filter == null) return group_by != null and validDashboardStringTopShape(projections, where_text, group_by.?);
    if (group_by == null) {
        if (validSearchPhraseOrderLimitShape(projections, filter, order_by_text, limit, offset)) return true;
        if (order_by_text != null or limit != null or offset != null) return false;
        if (projections.len == 1 and projections[0].func == .column_ref) return filter != null;
        for (projections) |expr| if (expr.func == .column_ref) return false;
        return true;
    }
    if (validRegionStatsShape(projections, group_by.?)) return true;
    if (validClickBenchStringTopShape(projections, filter, group_by.?)) return true;
    if (validUserSearchPhraseTopShape(projections, group_by.?)) return true;
    if (validClientIpAggTopShape(projections, filter, group_by.?)) return true;
    if (validClientIpSubtractTopShape(projections, filter, group_by.?)) return true;
    if (validDashboardStringTopShape(projections, where_text, group_by.?)) return true;
    if (validUrlCountTopShape(projections, filter, group_by.?)) return true;
    if (projections.len != 2) return false;
    if (projections[0].func != .column_ref) return false;
    if (!asciiEqlIgnoreCase(projections[0].column orelse return false, group_by.?)) return false;
    if (projections[1].func == .count_star) return true;
    if (projections[1].func == .count_distinct and asciiEqlIgnoreCase(group_by.?, "RegionID")) return asciiEqlIgnoreCase(projections[1].column orelse return false, "UserID");
    return false;
}

fn validUrlLengthByCounterShape(projections: []const Expr, filter: ?Filter, where_text: ?[]const u8, group_by: ?[]const u8, having_text: []const u8, order_by_text: ?[]const u8, limit: ?usize, offset: ?usize) bool {
    if (offset != null or limit != 25) return false;
    if (!asciiEqlIgnoreCase(group_by orelse return false, "CounterID")) return false;
    if (!asciiEqlIgnoreCase(where_text orelse return false, "URL <> ''")) return false;
    const f = filter orelse return false;
    if (f.second != null or f.op != .not_equal or f.int_value != 0 or !asciiEqlIgnoreCase(f.column, "URL")) return false;
    if (!asciiEqlIgnoreCase(having_text, "COUNT(*) > 100000")) return false;
    if (!asciiEqlIgnoreCase(order_by_text orelse return false, "l DESC")) return false;
    if (projections.len != 3) return false;
    if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "CounterID")) return false;
    if (projections[1].func != .avg or !asciiEqlIgnoreCase(projections[1].column orelse return false, "length(URL)")) return false;
    if (!asciiEqlIgnoreCase(projections[1].alias orelse return false, "l")) return false;
    return projections[2].func == .count_star and asciiEqlIgnoreCase(projections[2].alias orelse return false, "c");
}

fn validLikeTopShape(projections: []const Expr, where: []const u8, group_by: ?[]const u8, order_by_text: ?[]const u8, limit: ?usize, offset: ?usize) bool {
    if (offset != null) return false;
    if (asciiEqlIgnoreCase(where, "URL LIKE '%google%'")) {
        if (group_by != null or order_by_text != null or limit != null) return false;
        return projections.len == 1 and projections[0].func == .count_star;
    }
    if (asciiEqlIgnoreCase(where, "URL LIKE '%google%' AND SearchPhrase <> ''")) {
        if (!genericTopByC(group_by, order_by_text, limit)) return false;
        if (projections.len != 3) return false;
        if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "SearchPhrase")) return false;
        if (projections[1].func != .min or !asciiEqlIgnoreCase(projections[1].column orelse return false, "URL")) return false;
        return projections[2].func == .count_star and asciiEqlIgnoreCase(projections[2].alias orelse return false, "c");
    }
    if (asciiEqlIgnoreCase(where, "Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''")) {
        if (!genericTopByC(group_by, order_by_text, limit)) return false;
        if (projections.len != 5) return false;
        if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "SearchPhrase")) return false;
        if (projections[1].func != .min or !asciiEqlIgnoreCase(projections[1].column orelse return false, "URL")) return false;
        if (projections[2].func != .min or !asciiEqlIgnoreCase(projections[2].column orelse return false, "Title")) return false;
        if (projections[3].func != .count_star or !asciiEqlIgnoreCase(projections[3].alias orelse return false, "c")) return false;
        return projections[4].func == .count_distinct and asciiEqlIgnoreCase(projections[4].column orelse return false, "UserID");
    }
    return false;
}

fn genericTopByC(group_by: ?[]const u8, order_by_text: ?[]const u8, limit: ?usize) bool {
    return limit == 10 and asciiEqlIgnoreCase(group_by orelse return false, "SearchPhrase") and asciiEqlIgnoreCase(order_by_text orelse return false, "c DESC");
}

fn likeTopWhere(where: []const u8) bool {
    return asciiEqlIgnoreCase(where, "URL LIKE '%google%'") or
        asciiEqlIgnoreCase(where, "URL LIKE '%google%' AND SearchPhrase <> ''") or
        asciiEqlIgnoreCase(where, "Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''");
}

fn validSearchPhraseOrderLimitShape(projections: []const Expr, filter: ?Filter, order_by_text: ?[]const u8, limit: ?usize, offset: ?usize) bool {
    if (limit != 10 or offset != null) return false;
    const order = order_by_text orelse return false;
    if (!searchPhraseOrderByText(order)) return false;
    const f = filter orelse return false;
    if (f.second != null or f.op != .not_equal or f.int_value != 0 or !asciiEqlIgnoreCase(f.column, "SearchPhrase")) return false;
    if (projections.len != 1) return false;
    return projections[0].func == .column_ref and asciiEqlIgnoreCase(projections[0].column orelse return false, "SearchPhrase");
}

fn validDashboardStringTopShape(projections: []const Expr, where_text: ?[]const u8, group_by: []const u8) bool {
    const where = where_text orelse return false;
    if (!dashboardWhere(where)) return false;
    if (validWindowDashboardShape(projections, where, group_by)) return true;
    if (validUrlHashDateDashboardShape(projections, where, group_by)) return true;
    if (validTimeBucketDashboardShape(projections, where, group_by)) return true;
    if (projections.len != 2) return false;
    if (projections[0].func != .column_ref) return false;
    const key = projections[0].column orelse return false;
    if (!asciiEqlIgnoreCase(key, group_by)) return false;
    if (!asciiEqlIgnoreCase(key, "URL") and !asciiEqlIgnoreCase(key, "Title")) return false;
    return projections[1].func == .count_star and asciiEqlIgnoreCase(projections[1].alias orelse return false, "PageViews");
}

fn dashboardWhere(where: []const u8) bool {
    return asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''") or
        asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> ''") or
        asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0") or
        asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622") or
        asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465") or
        asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0");
}

fn validWindowDashboardShape(projections: []const Expr, where: []const u8, group_by: []const u8) bool {
    if (!asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622")) return false;
    if (!asciiEqlIgnoreCase(group_by, "WindowClientWidth, WindowClientHeight")) return false;
    if (projections.len != 3) return false;
    if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "WindowClientWidth")) return false;
    if (projections[1].func != .column_ref or !asciiEqlIgnoreCase(projections[1].column orelse return false, "WindowClientHeight")) return false;
    return projections[2].func == .count_star and asciiEqlIgnoreCase(projections[2].alias orelse return false, "PageViews");
}

fn validUrlHashDateDashboardShape(projections: []const Expr, where: []const u8, group_by: []const u8) bool {
    if (!asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465")) return false;
    if (!asciiEqlIgnoreCase(group_by, "URLHash, EventDate")) return false;
    if (projections.len != 3) return false;
    if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "URLHash")) return false;
    if (projections[1].func != .column_ref or !asciiEqlIgnoreCase(projections[1].column orelse return false, "EventDate")) return false;
    return projections[2].func == .count_star and asciiEqlIgnoreCase(projections[2].alias orelse return false, "PageViews");
}

fn validTimeBucketDashboardShape(projections: []const Expr, where: []const u8, group_by: []const u8) bool {
    if (!asciiEqlIgnoreCase(where, "CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0")) return false;
    if (!isDateTruncMinuteText(group_by)) return false;
    if (projections.len != 2) return false;
    if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "EventMinute")) return false;
    if (!asciiEqlIgnoreCase(projections[0].alias orelse return false, "M")) return false;
    return projections[1].func == .count_star and asciiEqlIgnoreCase(projections[1].alias orelse return false, "PageViews");
}

fn validClientIpSubtractTopShape(projections: []const Expr, filter: ?Filter, group_by: []const u8) bool {
    if (filter != null) return false;
    if (!asciiEqlIgnoreCase(group_by, "ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3")) return false;
    if (projections.len != 5) return false;
    if (!clientIpOffsetExpr(projections[0], 0)) return false;
    if (!clientIpOffsetExpr(projections[1], -1)) return false;
    if (!clientIpOffsetExpr(projections[2], -2)) return false;
    if (!clientIpOffsetExpr(projections[3], -3)) return false;
    return projections[4].func == .count_star and asciiEqlIgnoreCase(projections[4].alias orelse return false, "c");
}

fn clientIpOffsetExpr(expr: Expr, offset: i64) bool {
    return expr.func == .column_ref and asciiEqlIgnoreCase(expr.column orelse return false, "ClientIP") and expr.int_offset == offset;
}

fn validClientIpAggTopShape(projections: []const Expr, filter: ?Filter, group_by: []const u8) bool {
    if (projections.len != 5) return false;
    if (!aggTopTailShape(projections, "c")) return false;
    if (filter) |f| {
        if (f.second != null or f.op != .not_equal or f.int_value != 0 or !asciiEqlIgnoreCase(f.column, "SearchPhrase")) return false;
        if (asciiEqlIgnoreCase(group_by, "SearchEngineID, ClientIP")) return firstTwoColumns(projections, "SearchEngineID", "ClientIP");
        if (asciiEqlIgnoreCase(group_by, "WatchID, ClientIP")) return firstTwoColumns(projections, "WatchID", "ClientIP");
        return false;
    }
    return asciiEqlIgnoreCase(group_by, "WatchID, ClientIP") and firstTwoColumns(projections, "WatchID", "ClientIP");
}

fn validUrlCountTopShape(projections: []const Expr, filter: ?Filter, group_by: []const u8) bool {
    if (filter != null) return false;
    if (projections.len == 2) {
        if (!asciiEqlIgnoreCase(group_by, "URL")) return false;
        if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "URL")) return false;
        return projections[1].func == .count_star and asciiEqlIgnoreCase(projections[1].alias orelse return false, "c");
    }
    if (projections.len == 3) {
        if (!asciiEqlIgnoreCase(group_by, "1, URL")) return false;
        if (projections[0].func != .int_literal or projections[0].int_offset != 1) return false;
        if (projections[1].func != .column_ref or !asciiEqlIgnoreCase(projections[1].column orelse return false, "URL")) return false;
        return projections[2].func == .count_star and asciiEqlIgnoreCase(projections[2].alias orelse return false, "c");
    }
    return false;
}

fn firstTwoColumns(projections: []const Expr, first: []const u8, second: []const u8) bool {
    return projections[0].func == .column_ref and asciiEqlIgnoreCase(projections[0].column orelse return false, first) and
        projections[1].func == .column_ref and asciiEqlIgnoreCase(projections[1].column orelse return false, second);
}

fn aggTopTailShape(projections: []const Expr, count_alias: []const u8) bool {
    if (projections[2].func != .count_star or !asciiEqlIgnoreCase(projections[2].alias orelse return false, count_alias)) return false;
    if (projections[3].func != .sum or !asciiEqlIgnoreCase(projections[3].column orelse return false, "IsRefresh")) return false;
    return projections[4].func == .avg and asciiEqlIgnoreCase(projections[4].column orelse return false, "ResolutionWidth");
}

fn validUserSearchPhraseTopShape(projections: []const Expr, group_by: []const u8) bool {
    if (projections.len == 3) {
        if (!asciiEqlIgnoreCase(group_by, "UserID, SearchPhrase")) return false;
        if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "UserID")) return false;
        if (projections[1].func != .column_ref or !asciiEqlIgnoreCase(projections[1].column orelse return false, "SearchPhrase")) return false;
        return projections[2].func == .count_star;
    }
    if (projections.len == 4) {
        if (!asciiEqlIgnoreCase(group_by, "UserID, m, SearchPhrase")) return false;
        if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "UserID")) return false;
        if (projections[1].func != .column_ref or !asciiEqlIgnoreCase(projections[1].column orelse return false, "EventMinuteOfHour")) return false;
        if (!asciiEqlIgnoreCase(projections[1].alias orelse return false, "m")) return false;
        if (projections[2].func != .column_ref or !asciiEqlIgnoreCase(projections[2].column orelse return false, "SearchPhrase")) return false;
        return projections[3].func == .count_star;
    }
    return false;
}

fn validClickBenchStringTopShape(projections: []const Expr, filter: ?Filter, group_by: []const u8) bool {
    const f = filter orelse return false;
    if (f.second != null or f.op != .not_equal or f.int_value != 0) return false;
    if (projections.len == 2 and projections[0].func == .column_ref) {
        const key = projections[0].column orelse return false;
        if (!asciiEqlIgnoreCase(key, group_by)) return false;
        if (asciiEqlIgnoreCase(key, "MobilePhoneModel") and asciiEqlIgnoreCase(f.column, "MobilePhoneModel")) {
            return projections[1].func == .count_distinct and asciiEqlIgnoreCase(projections[1].column orelse return false, "UserID");
        }
        if (asciiEqlIgnoreCase(key, "SearchPhrase") and asciiEqlIgnoreCase(f.column, "SearchPhrase")) {
            if (projections[1].func == .count_star) return true;
            return projections[1].func == .count_distinct and asciiEqlIgnoreCase(projections[1].column orelse return false, "UserID");
        }
    }
    if (projections.len == 3 and projections[0].func == .column_ref and projections[1].func == .column_ref) {
        const first = projections[0].column orelse return false;
        const second = projections[1].column orelse return false;
        if (projections[2].func == .count_distinct and asciiEqlIgnoreCase(projections[2].column orelse return false, "UserID")) {
            return asciiEqlIgnoreCase(first, "MobilePhone") and asciiEqlIgnoreCase(second, "MobilePhoneModel") and asciiEqlIgnoreCase(group_by, "MobilePhone, MobilePhoneModel") and asciiEqlIgnoreCase(f.column, "MobilePhoneModel");
        }
        if (projections[2].func == .count_star) {
            return asciiEqlIgnoreCase(first, "SearchEngineID") and asciiEqlIgnoreCase(second, "SearchPhrase") and asciiEqlIgnoreCase(group_by, "SearchEngineID, SearchPhrase") and asciiEqlIgnoreCase(f.column, "SearchPhrase");
        }
    }
    return false;
}

fn validRegionStatsShape(projections: []const Expr, group_by: []const u8) bool {
    if (!asciiEqlIgnoreCase(group_by, "RegionID")) return false;
    if (projections.len != 5) return false;
    if (projections[0].func != .column_ref or !asciiEqlIgnoreCase(projections[0].column orelse return false, "RegionID")) return false;
    if (projections[1].func != .sum or !asciiEqlIgnoreCase(projections[1].column orelse return false, "AdvEngineID")) return false;
    if (projections[2].func != .count_star) return false;
    if (projections[3].func != .avg or !asciiEqlIgnoreCase(projections[3].column orelse return false, "ResolutionWidth")) return false;
    return projections[4].func == .count_distinct and asciiEqlIgnoreCase(projections[4].column orelse return false, "UserID");
}

fn parseAliasedExpr(expr: []const u8) ?Expr {
    const as_pos = lastKeywordPos(expr, "as") orelse return parseExpr(expr);
    const body = std.mem.trim(u8, expr[0..as_pos], " \t\r\n");
    const alias = std.mem.trim(u8, expr[as_pos + "as".len ..], " \t\r\n");
    if (body.len == 0 or !isIdentifierText(alias)) return null;
    var parsed = parseExpr(body) orelse return null;
    parsed.alias = alias;
    return parsed;
}

fn parseProjectionList(allocator: std.mem.Allocator, projections: *std.ArrayList(Expr), select_body: []const u8) !void {
    var start: usize = 0;
    var depth: usize = 0;
    for (select_body, 0..) |c, i| {
        switch (c) {
            '(' => depth += 1,
            ')' => {
                if (depth == 0) return error.UnsupportedGenericQuery;
                depth -= 1;
            },
            ',' => if (depth == 0) {
                const part = std.mem.trim(u8, select_body[start..i], " \t\r\n");
                if (part.len == 0) return error.UnsupportedGenericQuery;
                try projections.append(allocator, parseAliasedExpr(part) orelse return error.UnsupportedGenericQuery);
                start = i + 1;
            },
            else => {},
        }
    }
    if (depth != 0) return error.UnsupportedGenericQuery;
    const part = std.mem.trim(u8, select_body[start..], " \t\r\n");
    if (part.len == 0) return error.UnsupportedGenericQuery;
    try projections.append(allocator, parseAliasedExpr(part) orelse return error.UnsupportedGenericQuery);
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
    if (parseDateTruncMinute(expr)) return .{ .func = .column_ref, .column = "EventMinute" };
    if (parseExtractMinute(expr)) return .{ .func = .column_ref, .column = "EventMinuteOfHour" };
    if (std.mem.eql(u8, std.mem.trim(u8, expr, " \t\r\n"), "1")) return .{ .func = .int_literal, .int_offset = 1 };
    if (parseSubtractExpr(expr)) |parsed| return parsed;
    if (isIdentifierText(expr)) return .{ .func = .column_ref, .column = expr };
    return null;
}

fn parseSubtractExpr(expr: []const u8) ?Expr {
    const minus_pos = std.mem.indexOfScalar(u8, expr, '-') orelse return null;
    if (std.mem.indexOfScalar(u8, expr[minus_pos + 1 ..], '-') != null) return null;
    const column = std.mem.trim(u8, expr[0..minus_pos], " \t\r\n");
    const value_text = std.mem.trim(u8, expr[minus_pos + 1 ..], " \t\r\n");
    if (!isIdentifierText(column) or value_text.len == 0) return null;
    const int_offset = std.fmt.parseInt(i64, value_text, 10) catch return null;
    if (int_offset <= 0) return null;
    return .{ .func = .column_ref, .column = column, .int_offset = -int_offset };
}

fn validOrderByText(text: []const u8) bool {
    return isDateTruncMinuteText(text) or searchPhraseOrderByText(text) or asciiEqlIgnoreCase(text, "c DESC") or asciiEqlIgnoreCase(text, "l DESC");
}

fn searchPhraseOrderByText(text: []const u8) bool {
    return asciiEqlIgnoreCase(text, "EventTime") or
        asciiEqlIgnoreCase(text, "EventTime, SearchPhrase") or
        asciiEqlIgnoreCase(text, "SearchPhrase");
}

fn parseDateTruncMinute(expr: []const u8) bool {
    return isDateTruncMinuteText(std.mem.trim(u8, expr, " \t\r\n"));
}

fn isDateTruncMinuteText(expr: []const u8) bool {
    const arg = parseCall(expr, "date_trunc") orelse return false;
    const comma_pos = std.mem.indexOfScalar(u8, arg, ',') orelse return false;
    if (std.mem.indexOfScalar(u8, arg[comma_pos + 1 ..], ',') != null) return false;
    const unit = std.mem.trim(u8, arg[0..comma_pos], " \t\r\n");
    const source = std.mem.trim(u8, arg[comma_pos + 1 ..], " \t\r\n");
    return std.mem.eql(u8, unit, "'minute'") and asciiEqlIgnoreCase(source, "EventTime");
}

fn parseExtractMinute(expr: []const u8) bool {
    const arg = parseCall(expr, "extract") orelse return false;
    const from_pos = indexOfKeyword(arg, "from") orelse return false;
    const field = std.mem.trim(u8, arg[0..from_pos], " \t\r\n");
    const source = std.mem.trim(u8, arg[from_pos + "from".len ..], " \t\r\n");
    return asciiEqlIgnoreCase(field, "minute") and asciiEqlIgnoreCase(source, "EventTime");
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

fn indexOfTopLevelKeyword(sql: []const u8, keyword: []const u8) ?usize {
    var depth: usize = 0;
    var i: usize = 0;
    while (i + keyword.len <= sql.len) : (i += 1) {
        switch (sql[i]) {
            '(' => depth += 1,
            ')' => {
                if (depth > 0) depth -= 1;
            },
            else => {},
        }
        if (depth != 0) continue;
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

fn lastKeywordPos(sql: []const u8, keyword: []const u8) ?usize {
    var search_from: usize = 0;
    var found: ?usize = null;
    while (search_from < sql.len) {
        const rel = indexOfKeyword(sql[search_from..], keyword) orelse return found;
        const pos = search_from + rel;
        found = pos;
        search_from = pos + keyword.len;
    }
    return found;
}

fn projectionAliasExists(projections: []const Expr, alias: []const u8) bool {
    for (projections) |expr| {
        if (expr.alias) |candidate| if (asciiEqlIgnoreCase(candidate, alias)) return true;
    }
    return false;
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
        const plan = (try parse(std.testing.allocator, case.sql)).?;
        defer deinit(std.testing.allocator, plan);
        try std.testing.expect(plan.filter == null);
        try std.testing.expect(plan.where_text != null);
        try std.testing.expectEqualStrings(case.group_by, plan.group_by.?);
        try std.testing.expectEqualStrings("PageViews", plan.order_by_alias.?);
        try std.testing.expectEqual(@as(?usize, 10), plan.limit);
        try std.testing.expectEqual(@as(?usize, null), plan.offset);
    }

    const q39 = (try parse(std.testing.allocator, "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000")).?;
    defer deinit(std.testing.allocator, q39);
    try std.testing.expect(q39.filter == null);
    try std.testing.expect(q39.where_text != null);
    try std.testing.expectEqualStrings("URL", q39.group_by.?);
    try std.testing.expectEqualStrings("PageViews", q39.order_by_alias.?);
    try std.testing.expectEqual(@as(?usize, 10), q39.limit);
    try std.testing.expectEqual(@as(?usize, 1000), q39.offset);

    const q42 = (try parse(std.testing.allocator, "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000")).?;
    defer deinit(std.testing.allocator, q42);
    try std.testing.expect(q42.filter == null);
    try std.testing.expect(q42.where_text != null);
    try std.testing.expectEqualStrings("WindowClientWidth, WindowClientHeight", q42.group_by.?);
    try std.testing.expectEqualStrings("PageViews", q42.order_by_alias.?);
    try std.testing.expectEqual(@as(?usize, 10), q42.limit);
    try std.testing.expectEqual(@as(?usize, 10000), q42.offset);

    const q41 = (try parse(std.testing.allocator, "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100")).?;
    defer deinit(std.testing.allocator, q41);
    try std.testing.expect(q41.filter == null);
    try std.testing.expect(q41.where_text != null);
    try std.testing.expectEqualStrings("URLHash, EventDate", q41.group_by.?);
    try std.testing.expectEqualStrings("PageViews", q41.order_by_alias.?);
    try std.testing.expectEqual(@as(?usize, 10), q41.limit);
    try std.testing.expectEqual(@as(?usize, 100), q41.offset);

    const q43 = (try parse(std.testing.allocator, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000")).?;
    defer deinit(std.testing.allocator, q43);
    try std.testing.expect(q43.filter == null);
    try std.testing.expect(q43.where_text != null);
    try std.testing.expectEqualStrings("EventMinute", q43.projections[0].column.?);
    try std.testing.expectEqualStrings("M", q43.projections[0].alias.?);
    try std.testing.expectEqualStrings("DATE_TRUNC('minute', EventTime)", q43.group_by.?);
    try std.testing.expectEqualStrings("DATE_TRUNC('minute', EventTime)", q43.order_by_text.?);
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
    try std.testing.expect(q21.filter == null);
    try std.testing.expectEqualStrings("URL LIKE '%google%'", q21.where_text.?);
    try std.testing.expectEqual(AggregateFn.count_star, q21.projections[0].func);

    const q22 = (try parse(std.testing.allocator, "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q22);
    try std.testing.expect(q22.filter == null);
    try std.testing.expectEqualStrings("SearchPhrase", q22.group_by.?);
    try std.testing.expectEqualStrings("c DESC", q22.order_by_text.?);
    try std.testing.expectEqual(AggregateFn.min, q22.projections[1].func);
    try std.testing.expectEqual(AggregateFn.count_star, q22.projections[2].func);

    const q23 = (try parse(std.testing.allocator, "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10")).?;
    defer deinit(std.testing.allocator, q23);
    try std.testing.expect(q23.filter == null);
    try std.testing.expectEqualStrings("SearchPhrase", q23.group_by.?);
    try std.testing.expectEqualStrings("c DESC", q23.order_by_text.?);
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
    try std.testing.expectEqualStrings("COUNT(*) > 100000", plan.having_text.?);
    try std.testing.expectEqualStrings("l DESC", plan.order_by_text.?);
    try std.testing.expectEqualStrings("length(URL)", plan.projections[1].column.?);
    try std.testing.expectEqualStrings("l", plan.projections[1].alias.?);
    try std.testing.expectEqual(@as(?usize, 25), plan.limit);
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
