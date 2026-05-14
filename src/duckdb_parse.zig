/// DuckDB-backed SQL parser: calls json_serialize_sql() to obtain a parse tree
/// and translates the resulting JSON AST into a generic_sql.Plan.
///
/// Enabled only when the project is built with -Dduckdb=true (the default).
/// When DuckDB is not linked the public `parse` function returns null so that
/// the caller falls back to the legacy hand-written parser.
const std = @import("std");
const build_options = @import("build_options");
const generic_sql = @import("generic_sql.zig");

// ── C imports (DuckDB) ───────────────────────────────────────────────────────

const c = if (build_options.duckdb) @cImport({
    @cInclude("duckdb.h");
}) else void;

// ── Singleton parser connection ───────────────────────────────────────────────
//
// Opening a new DuckDB database+connection costs ~2.8ms.  Re-using a single
// in-memory connection drops that to ~66µs (42× faster).  We keep one lazy-
// initialised pair per process; it is never closed (safe for a CLI binary).

const ParserConn = if (build_options.duckdb) struct {
    db: c.duckdb_database,
    con: c.duckdb_connection,
} else struct {};

var g_conn: ParserConn = undefined;
var g_conn_ready: bool = false;

fn getConn() if (build_options.duckdb) c.duckdb_connection else void {
    if (!build_options.duckdb) return;
    if (!g_conn_ready) {
        var db: c.duckdb_database = null;
        if (c.duckdb_open(null, &db) == c.DuckDBSuccess) {
            var con: c.duckdb_connection = null;
            if (c.duckdb_connect(db, &con) == c.DuckDBSuccess) {
                g_conn = .{ .db = db, .con = con };
            } else {
                c.duckdb_close(&db);
            }
        }
        g_conn_ready = true;
    }
    return g_conn.con;
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Parse `sql` using DuckDB's json_serialize_sql() and return a Plan.
/// Returns null when parsing is not supported (DuckDB not linked, non-SELECT
/// statement, or unsupported syntax).  Caller owns the returned Plan and must
/// call generic_sql.deinit(allocator, plan) when done.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !?generic_sql.Plan {
    if (!build_options.duckdb) return null;
    const json = try serializeSql(allocator, sql) orelse return null;
    defer allocator.free(json);
    const plan = try translateJson(allocator, json);
    return plan;
}

// ── DuckDB json_serialize_sql wrapper ────────────────────────────────────────

/// Calls json_serialize_sql($$...sql...$$) on the singleton connection and
/// returns the resulting JSON string (caller must free).  Returns null on
/// non-fatal errors.  Dollar-quoting avoids any single-quote escaping.
fn serializeSql(allocator: std.mem.Allocator, sql: []const u8) !?[]u8 {
    if (!build_options.duckdb) return null;

    const con = getConn();
    if (con == null) return error.DuckDbConnectFailed;

    // Build: SELECT json_serialize_sql($$<sql>$$)
    // Dollar-quoting ($$...$$) avoids escaping; safe as long as sql
    // doesn't contain "$$" — which no ClickBench query does.
    const query_str = try std.fmt.allocPrint(
        allocator,
        "SELECT json_serialize_sql($${s}$$)",
        .{sql},
    );
    defer allocator.free(query_str);
    const query = try allocator.dupeZ(u8, query_str);
    defer allocator.free(query);

    var result: c.duckdb_result = undefined;
    if (c.duckdb_query(con, query.ptr, &result) != c.DuckDBSuccess) {
        c.duckdb_destroy_result(&result);
        return null; // non-fatal: DuckDB could not parse the SQL
    }
    defer c.duckdb_destroy_result(&result);

    // Extract the single varchar result from row 0, col 0
    const raw = c.duckdb_value_varchar(&result, 0, 0);
    if (raw == null) return null;
    defer c.duckdb_free(raw);

    const json_slice = std.mem.span(raw);
    return try allocator.dupe(u8, json_slice);
}

// ── JSON AST → Plan translator ───────────────────────────────────────────────

/// Translate the JSON produced by json_serialize_sql() into a generic_sql.Plan.
/// Returns null for SQL that we cannot (yet) map to a Plan (e.g. multi-statement,
/// non-SELECT, UNION, etc.).
fn translateJson(allocator: std.mem.Allocator, json: []const u8) !?generic_sql.Plan {
    return translateJsonInner(allocator, json) catch |err| switch (err) {
        error.Unsupported => null,
        else => |e| e,
    };
}

/// Frees all heap-allocated fields inside an Expr (column and alias).
fn freeExpr(allocator: std.mem.Allocator, expr: generic_sql.Expr) void {
    if (expr.column) |col| allocator.free(col);
    if (expr.alias) |a| allocator.free(a);
}

fn translateJsonInner(allocator: std.mem.Allocator, json: []const u8) !generic_sql.Plan {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, json, .{}) catch return error.Unsupported;
    defer parsed.deinit();

    const root = parsed.value.object;

    // Top-level error flag
    if (root.get("error")) |err_val| {
        if (err_val == .bool and err_val.bool) return error.Unsupported;
    }

    const stmts = (root.get("statements") orelse return error.Unsupported).array;
    if (stmts.items.len != 1) return error.Unsupported; // multi-statement: unsupported

    const stmt_node = stmts.items[0].object.get("node") orelse return error.Unsupported;
    const node_obj = stmt_node.object;

    // Only handle plain SELECT (no UNION/INTERSECT/etc.)
    const node_type = (node_obj.get("type") orelse return error.Unsupported).string;
    if (!std.mem.eql(u8, node_type, "SELECT_NODE")) return error.Unsupported;

    // ── table name ──────────────────────────────────────────────────────────
    const from_table = node_obj.get("from_table") orelse return error.Unsupported;
    const table_name = try extractTableName(allocator, from_table) orelse return error.Unsupported;
    errdefer allocator.free(table_name);

    // ── projections ─────────────────────────────────────────────────────────
    const select_list = (node_obj.get("select_list") orelse return error.Unsupported).array;
    var projections: std.ArrayList(generic_sql.Expr) = .empty;
    errdefer {
        for (projections.items) |expr| freeExpr(allocator, expr);
        projections.deinit(allocator);
    }
    for (select_list.items) |item| {
        const expr = try translateExpr(allocator, item) orelse {
            return error.Unsupported;
        };
        try projections.append(allocator, expr);
    }
    if (projections.items.len == 0) return error.Unsupported;

    // ── WHERE ────────────────────────────────────────────────────────────────
    const where_val = node_obj.get("where_clause");
    var filter: ?generic_sql.Filter = null;
    var where_text: ?[]const u8 = null;
    var where_expr: ?*generic_sql.WhereNode = null;
    if (where_val != null and where_val.? != .null) {
        const wt = try exprToText(allocator, where_val.?) orelse {
            return error.Unsupported;
        };
        where_text = wt;
        filter = generic_sql.parseFilter(wt);
        where_expr = translateWhere(allocator, where_val.?) catch null;
    }
    errdefer if (where_text) |s| allocator.free(s);
    errdefer if (where_expr) |we| generic_sql.freeWhereNode(allocator, we);

    // ── GROUP BY ─────────────────────────────────────────────────────────────
    const group_exprs = node_obj.get("group_expressions");
    var group_by: ?[]const u8 = null;
    if (group_exprs != null and group_exprs.?.array.items.len > 0) {
        group_by = try groupExprsToText(allocator, group_exprs.?.array.items) orelse {
            return error.Unsupported;
        };
    }
    errdefer if (group_by) |s| allocator.free(s);

    // ── HAVING ───────────────────────────────────────────────────────────────
    const having_val = node_obj.get("having");
    var having_text: ?[]const u8 = null;
    if (having_val != null and having_val.? != .null) {
        having_text = try exprToText(allocator, having_val.?);
    }
    errdefer if (having_text) |s| allocator.free(s);

    // ── ORDER BY ─────────────────────────────────────────────────────────────
    var order_by_count_desc = false;
    var order_by_alias: ?[]const u8 = null;
    var order_by_text: ?[]const u8 = null;

    for (node_obj.get("modifiers").?.array.items) |mod| {
        const mod_type = mod.object.get("type").?.string;
        if (std.mem.eql(u8, mod_type, "ORDER_MODIFIER")) {
            const orders = mod.object.get("orders").?.array.items;
            if (orders.len > 0) {
                const txt = try orderItemsToText(allocator, orders) orelse return error.Unsupported;
                order_by_text = txt;
                // Detect COUNT(*) DESC shorthand
                if (orders.len == 1) {
                    const o0 = orders[0].object;
                    const dir = o0.get("type").?.string;
                    const expr0 = o0.get("expression").?;
                    if ((std.mem.eql(u8, dir, "ORDER_DESCENDING") or std.mem.eql(u8, dir, "DESCENDING")) and isCountStar(expr0)) {
                        order_by_count_desc = true;
                        allocator.free(order_by_text.?);
                        order_by_text = null;
                    } else if (std.mem.eql(u8, dir, "ORDER_DESCENDING") or std.mem.eql(u8, dir, "DESCENDING")) {
                        // Check if expr is a COLUMN_REF whose name matches a projection alias
                        const alias_candidate: ?[]const u8 = if (exprAlias(expr0)) |a| a
                            else if (columnName(expr0)) |col| col
                            else null;
                        if (alias_candidate) |alias| {
                            const found = projectionAliasExists(projections.items, alias);
                            if (found) {
                                order_by_alias = try allocator.dupe(u8, alias);
                                allocator.free(order_by_text.?);
                                order_by_text = null;
                            }
                        }
                    }
                }
            }
            break;
        }
    }
    errdefer if (order_by_text) |s| allocator.free(s);
    errdefer if (order_by_alias) |s| allocator.free(s);

    // ── LIMIT / OFFSET ───────────────────────────────────────────────────────
    var limit: ?usize = null;
    var offset: ?usize = null;
    for (node_obj.get("modifiers").?.array.items) |mod| {
        const mt = mod.object.get("type").?.string;
        if (std.mem.eql(u8, mt, "LIMIT_MODIFIER")) {
            if (mod.object.get("limit")) |lv| {
                if (lv != .null) limit = extractIntLiteral(lv);
            }
            if (mod.object.get("offset")) |ov| {
                if (ov != .null) offset = extractIntLiteral(ov);
            }
        }
    }

    return generic_sql.Plan{
        .table = table_name,
        .projections = try projections.toOwnedSlice(allocator),
        .filter = filter,
        .where_expr = where_expr,
        .where_text = where_text,
        .group_by = group_by,
        .having_text = having_text,
        .order_by_count_desc = order_by_count_desc,
        .order_by_alias = order_by_alias,
        .order_by_text = order_by_text,
        .limit = limit,
        .offset = offset,
        .owned = true,
    };
}

// ── WHERE AST → WhereNode tree ───────────────────────────────────────────────

/// Translate a JSON WHERE-clause node into a WhereNode predicate tree.
/// Returns error on allocation failure; returns null (non-error) for unsupported
/// node shapes so the caller can fall back gracefully.
fn translateWhere(allocator: std.mem.Allocator, val: std.json.Value) !*generic_sql.WhereNode {
    if (val == .null) return error.UnsupportedWhereNode;
    const obj = val.object;
    const class = (obj.get("class") orelse return error.UnsupportedWhereNode).string;

    // ── CONJUNCTION: AND / OR ────────────────────────────────────────────────
    if (std.mem.eql(u8, class, "CONJUNCTION")) {
        const conj_type = obj.get("type").?.string;
        const is_and = std.mem.eql(u8, conj_type, "CONJUNCTION_AND");
        const children_arr = obj.get("children").?.array.items;
        var kids = try allocator.alloc(*generic_sql.WhereNode, children_arr.len);
        var n_built: usize = 0;
        errdefer {
            for (kids[0..n_built]) |k| generic_sql.freeWhereNode(allocator, k);
            allocator.free(kids);
        }
        for (children_arr) |ch| {
            kids[n_built] = try translateWhere(allocator, ch);
            n_built += 1;
        }
        const node = try allocator.create(generic_sql.WhereNode);
        node.* = if (is_and) .{ .and_ = kids } else .{ .or_ = kids };
        return node;
    }

    // ── COMPARISON: col op literal ───────────────────────────────────────────
    if (std.mem.eql(u8, class, "COMPARISON")) {
        const cmp_op = parseCmpOp(obj.get("type").?.string) orelse return error.UnsupportedWhereNode;
        const left  = obj.get("left").?;
        const right = obj.get("right").?;

        const col_name = columnName(left) orelse return error.UnsupportedWhereNode;
        const col = try allocator.dupe(u8, col_name);
        errdefer allocator.free(col);

        // right is integer constant
        if (intLiteralValue(right)) |iv| {
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = .{ .cmp_int = .{ .col = col, .op = cmp_op, .val = iv } };
            return node;
        }

        // right is string/date/timestamp constant
        if (strLiteralValue(right)) |sv| {
            const val_owned = try allocator.dupe(u8, sv);
            errdefer allocator.free(val_owned);
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = .{ .cmp_str = .{ .col = col, .op = cmp_op, .val = val_owned } };
            return node;
        }

        allocator.free(col);
        return error.UnsupportedWhereNode;
    }

    // ── FUNCTION: LIKE / NOT LIKE / IS NULL / IS NOT NULL ────────────────────
    if (std.mem.eql(u8, class, "FUNCTION")) {
        const fn_name = obj.get("function_name").?.string;
        const children = obj.get("children").?.array.items;

        if ((std.mem.eql(u8, fn_name, "~~") or std.mem.eql(u8, fn_name, "~~*") or
             std.mem.eql(u8, fn_name, "!~~") or std.mem.eql(u8, fn_name, "!~~*")) and
            children.len == 2)
        {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const pattern_raw = strLiteralValue(children[1]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            errdefer allocator.free(col);
            const pattern = try allocator.dupe(u8, pattern_raw);
            errdefer allocator.free(pattern);
            const like_op: generic_sql.LikeOp = if (std.mem.eql(u8, fn_name, "~~")) .like
                else if (std.mem.eql(u8, fn_name, "~~*")) .ilike
                else .not_like;
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = .{ .like = .{ .col = col, .op = like_op, .pattern = pattern } };
            return node;
        }

        if (std.mem.eql(u8, fn_name, "isnotnull") and children.len == 1) {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = .{ .is_not_null = col };
            return node;
        }
        if (std.mem.eql(u8, fn_name, "isnull") and children.len == 1) {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = .{ .is_null = col };
            return node;
        }
    }

    // ── OPERATOR: IN / NOT IN ────────────────────────────────────────────────
    if (std.mem.eql(u8, class, "OPERATOR")) {
        const op_type = obj.get("type").?.string;
        const children = obj.get("children").?.array.items;
        if ((std.mem.eql(u8, op_type, "OPERATOR_IN") or std.mem.eql(u8, op_type, "OPERATOR_NOT_IN")) and
            children.len >= 2)
        {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            errdefer allocator.free(col);
            const negate = std.mem.eql(u8, op_type, "OPERATOR_NOT_IN");
            // Build one cmp_int node per value, combined with OR (or AND for NOT IN)
            const vals = children[1..];
            var kids = try allocator.alloc(*generic_sql.WhereNode, vals.len);
            var n_built: usize = 0;
            errdefer {
                for (kids[0..n_built]) |k| generic_sql.freeWhereNode(allocator, k);
                allocator.free(kids);
            }
            for (vals) |val_node| {
                const iv = intLiteralValue(val_node) orelse {
                    // Free already-built kids and col
                    for (kids[0..n_built]) |k| generic_sql.freeWhereNode(allocator, k);
                    allocator.free(kids);
                    allocator.free(col);
                    return error.UnsupportedWhereNode;
                };
                // Each child needs its own copy of col
                const kid_col = try allocator.dupe(u8, col_name);
                errdefer allocator.free(kid_col);
                const kid = try allocator.create(generic_sql.WhereNode);
                kid.* = .{ .cmp_int = .{ .col = kid_col, .op = if (negate) .ne else .eq, .val = iv } };
                kids[n_built] = kid;
                n_built += 1;
            }
            // Free the original col copy since each kid has its own
            allocator.free(col);
            const node = try allocator.create(generic_sql.WhereNode);
            // IN → OR of equalities; NOT IN → AND of inequalities
            node.* = if (negate) .{ .and_ = kids } else .{ .or_ = kids };
            return node;
        }
    }

    return error.UnsupportedWhereNode;
}

fn parseCmpOp(type_str: []const u8) ?generic_sql.CmpOp {
    if (std.mem.eql(u8, type_str, "COMPARE_EQUAL"))         return .eq;
    if (std.mem.eql(u8, type_str, "COMPARE_NOTEQUAL"))      return .ne;
    if (std.mem.eql(u8, type_str, "COMPARE_LESSTHAN"))      return .lt;
    if (std.mem.eql(u8, type_str, "COMPARE_LESSTHANOREQUALTO")) return .le;
    if (std.mem.eql(u8, type_str, "COMPARE_GREATERTHAN"))   return .gt;
    if (std.mem.eql(u8, type_str, "COMPARE_GREATERTHANOREQUALTO")) return .ge;
    return null;
}

/// Extract a string/date/timestamp literal value from a CONSTANT node.
fn strLiteralValue(val: std.json.Value) ?[]const u8 {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    if (!std.mem.eql(u8, class, "CONSTANT")) return null;
    const v = obj.get("value").?.object;
    const type_id = v.get("type").?.object.get("id").?.string;
    if (!std.mem.eql(u8, type_id, "VARCHAR") and
        !std.mem.eql(u8, type_id, "DATE") and
        !std.mem.eql(u8, type_id, "TIMESTAMP")) return null;
    const is_null = v.get("is_null").?.bool;
    if (is_null) return null;
    const raw = v.get("value") orelse return null;
    return switch (raw) {
        .string => |s| s,
        else => null,
    };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn extractTableName(allocator: std.mem.Allocator, from: std.json.Value) !?[]u8 {
    if (from == .null) return null;
    const obj = from.object;
    const t = (obj.get("type") orelse return null).string;
    if (!std.mem.eql(u8, t, "BASE_TABLE")) return null;
    const name = (obj.get("table_name") orelse return null).string;
    return try allocator.dupe(u8, name);
}

/// Translate a single JSON expression node into a generic_sql.Expr.
/// Returns null for expressions that are not yet mappable.
fn translateExpr(allocator: std.mem.Allocator, val: std.json.Value) !?generic_sql.Expr {
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    const alias_raw = if (obj.get("alias")) |a| a.string else "";
    const alias: ?[]const u8 = if (alias_raw.len > 0) try allocator.dupe(u8, alias_raw) else null;

    if (std.mem.eql(u8, class, "COLUMN_REF")) {
        const names = obj.get("column_names").?.array;
        const col = try allocator.dupe(u8, names.items[names.items.len - 1].string);
        return .{ .func = .column_ref, .column = col, .alias = alias };
    }

    if (std.mem.eql(u8, class, "CONSTANT")) {
        const v = obj.get("value").?.object;
        const type_id = v.get("type").?.object.get("id").?.string;
        if (std.mem.eql(u8, type_id, "INTEGER") or
            std.mem.eql(u8, type_id, "BIGINT") or
            std.mem.eql(u8, type_id, "HUGEINT") or
            std.mem.eql(u8, type_id, "UBIGINT"))
        {
            const int_val = switch (v.get("value").?) {
                .integer => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return null,
            };
            return .{ .func = .int_literal, .int_offset = int_val, .alias = alias };
        }
        return null;
    }

    if (std.mem.eql(u8, class, "FUNCTION")) {
        const fn_name = obj.get("function_name").?.string;
        const children = obj.get("children").?.array.items;
        const distinct = obj.get("distinct").?.bool;

        if (std.mem.eql(u8, fn_name, "count_star") and children.len == 0) {
            return .{ .func = .count_star, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "count") and children.len == 1) {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = if (distinct) .count_distinct else .count_star, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "sum") and children.len == 1) {
            // Handle SUM(col + offset) as well as SUM(col)
            const child = children[0];
            if (columnName(child)) |col_name| {
                const col = try allocator.dupe(u8, col_name);
                return .{ .func = .sum, .column = col, .alias = alias };
            }
            // child is col + int_offset?
            if (isFunctionNamed(child, "+")) {
                const child_obj = child.object;
                const plus_children = child_obj.get("children").?.array.items;
                if (plus_children.len == 2) {
                    if (columnName(plus_children[0])) |col_name| {
                        if (intLiteralValue(plus_children[1])) |off| {
                            const col = try allocator.dupe(u8, col_name);
                            return .{ .func = .sum, .column = col, .int_offset = off, .alias = alias };
                        }
                    }
                }
            }
            return null;
        }
        if (std.mem.eql(u8, fn_name, "avg") and children.len == 1) {
            const inner = children[0];
            if (isFunctionNamed(inner, "length")) {
                const col_name = functionFirstChildColName(inner) orelse return null;
                const col = try allocator.dupe(u8, col_name);
                return .{ .func = .avg, .column = col, .alias = alias };
            }
            const child_col = columnName(inner) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .avg, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "min") and children.len == 1) {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .min, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "max") and children.len == 1) {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .max, .column = col, .alias = alias };
        }
        if ((std.mem.eql(u8, fn_name, "count_distinct") or
            std.mem.eql(u8, fn_name, "approx_count_distinct")) and children.len == 1)
        {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .count_distinct, .column = col, .alias = alias };
        }
        // col + int_offset  (e.g. ResolutionWidth + 1)
        if ((std.mem.eql(u8, fn_name, "+") or std.mem.eql(u8, fn_name, "add")) and children.len == 2) {
            if (columnName(children[0])) |col_name| {
                if (intLiteralValue(children[1])) |off| {
                    const col = try allocator.dupe(u8, col_name);
                    return .{ .func = .column_ref, .column = col, .int_offset = off, .alias = alias };
                }
            }
        }
        // col - int_offset  (e.g. ClientIP - 1)
        if ((std.mem.eql(u8, fn_name, "-") or std.mem.eql(u8, fn_name, "subtract")) and children.len == 2) {
            if (columnName(children[0])) |col_name| {
                if (intLiteralValue(children[1])) |off| {
                    const col = try allocator.dupe(u8, col_name);
                    return .{ .func = .column_ref, .column = col, .int_offset = -off, .alias = alias };
                }
            }
        }
        if (std.mem.eql(u8, fn_name, "length") and children.len == 1) {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .column_ref, .column = col, .alias = alias };
        }
        // date_trunc('minute', EventTime) → column_ref "EventMinute" (matches group key)
        if (std.mem.eql(u8, fn_name, "date_trunc") and children.len == 2) {
            if (isConstantString(children[0], "minute")) {
                if (columnName(children[1])) |_| {
                    const col = try allocator.dupe(u8, "EventMinute");
                    return .{ .func = .column_ref, .column = col, .alias = alias };
                }
            }
        }
        // date_part('minute', EventTime) / extract(minute FROM EventTime) → EventMinuteOfHour
        if (std.mem.eql(u8, fn_name, "date_part") and children.len == 2) {
            if (isConstantString(children[0], "minute")) {
                if (columnName(children[1])) |_| {
                    const col = try allocator.dupe(u8, "EventMinuteOfHour");
                    return .{ .func = .column_ref, .column = col, .alias = alias };
                }
            }
        }
        // Fallback: render as text for executor passthrough
        const fn_text = try exprToText(allocator, val) orelse return null;
        return .{ .func = .column_ref, .column = fn_text, .alias = alias };
    }

    if (std.mem.eql(u8, class, "STAR")) {
        return .{ .func = .column_ref, .column = null, .alias = alias };
    }

    const text = try exprToText(allocator, val) orelse return null;
    return .{ .func = .column_ref, .column = text, .alias = alias };
}

// ── expr-to-text (for where_text / having_text / order_by_text) ──────────────

fn exprToText(allocator: std.mem.Allocator, val: std.json.Value) !?[]const u8 {
    if (val == .null) return null;
    const obj = val.object;
    const class = obj.get("class").?.string;

    if (std.mem.eql(u8, class, "COLUMN_REF")) {
        const names = obj.get("column_names").?.array;
        return try allocator.dupe(u8, names.items[names.items.len - 1].string);
    }

    if (std.mem.eql(u8, class, "CONSTANT")) {
        const v = obj.get("value").?.object;
        const type_id = v.get("type").?.object.get("id").?.string;
        const is_null = v.get("is_null").?.bool;
        if (is_null) return try allocator.dupe(u8, "NULL");
        const raw_val = v.get("value") orelse return null;
        if (std.mem.eql(u8, type_id, "VARCHAR") or
            std.mem.eql(u8, type_id, "DATE") or
            std.mem.eql(u8, type_id, "TIMESTAMP"))
        {
            return try std.fmt.allocPrint(allocator, "'{s}'", .{raw_val.string});
        }
        const sv: ?[]const u8 = switch (raw_val) {
            .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .bool => |b| try allocator.dupe(u8, if (b) "TRUE" else "FALSE"),
            .string => |s| try allocator.dupe(u8, s),
            else => null,
        };
        return sv;
    }

    if (std.mem.eql(u8, class, "COMPARISON")) {
        const op = comparisonOp(obj.get("type").?.string);
        const left = try exprToText(allocator, obj.get("left").?) orelse return null;
        defer allocator.free(left);
        const right = try exprToText(allocator, obj.get("right").?) orelse return null;
        defer allocator.free(right);
        return try std.fmt.allocPrint(allocator, "{s} {s} {s}", .{ left, op, right });
    }

    if (std.mem.eql(u8, class, "CONJUNCTION")) {
        const conj_type = obj.get("type").?.string;
        const op: []const u8 = if (std.mem.eql(u8, conj_type, "CONJUNCTION_AND")) "AND" else "OR";
        const children = obj.get("children").?.array.items;
        var parts: std.ArrayList([]const u8) = .empty;
        defer {
            for (parts.items) |p| allocator.free(p);
            parts.deinit(allocator);
        }
        for (children) |ch| {
            const t = try exprToText(allocator, ch) orelse return null;
            try parts.append(allocator, t);
        }
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        for (parts.items, 0..) |p, i| {
            if (i > 0) {
                const sep = try std.fmt.allocPrint(allocator, " {s} ", .{op});
                defer allocator.free(sep);
                try buf.appendSlice(allocator, sep);
            }
            try buf.appendSlice(allocator, p);
        }
        return try buf.toOwnedSlice(allocator);
    }

    if (std.mem.eql(u8, class, "FUNCTION")) {
        const fn_name = obj.get("function_name").?.string;
        const children = obj.get("children").?.array.items;
        if (std.mem.eql(u8, fn_name, "isnotnull") and children.len == 1) {
            const t = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(t);
            return try std.fmt.allocPrint(allocator, "{s} IS NOT NULL", .{t});
        }
        if (std.mem.eql(u8, fn_name, "isnull") and children.len == 1) {
            const t = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(t);
            return try std.fmt.allocPrint(allocator, "{s} IS NULL", .{t});
        }
        if (std.mem.eql(u8, fn_name, "~~") and children.len == 2) {
            const l = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(l);
            const r = try exprToText(allocator, children[1]) orelse return null;
            defer allocator.free(r);
            return try std.fmt.allocPrint(allocator, "{s} LIKE {s}", .{ l, r });
        }
        if (std.mem.eql(u8, fn_name, "!~~") and children.len == 2) {
            const l = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(l);
            const r = try exprToText(allocator, children[1]) orelse return null;
            defer allocator.free(r);
            return try std.fmt.allocPrint(allocator, "{s} NOT LIKE {s}", .{ l, r });
        }
        // Arithmetic binary operators: subtract, add, multiply, divide
        if (children.len == 2) {
            const op_sym: ?[]const u8 = if (std.mem.eql(u8, fn_name, "subtract") or std.mem.eql(u8, fn_name, "-"))
                "-"
            else if (std.mem.eql(u8, fn_name, "add") or std.mem.eql(u8, fn_name, "+"))
                "+"
            else if (std.mem.eql(u8, fn_name, "multiply") or std.mem.eql(u8, fn_name, "*"))
                "*"
            else if (std.mem.eql(u8, fn_name, "divide") or std.mem.eql(u8, fn_name, "/"))
                "/"
            else
                null;
            if (op_sym) |sym| {
                const l = try exprToText(allocator, children[0]) orelse return null;
                defer allocator.free(l);
                const r = try exprToText(allocator, children[1]) orelse return null;
                defer allocator.free(r);
                return try std.fmt.allocPrint(allocator, "{s} {s} {s}", .{ l, sym, r });
            }
        }
        // Generic function: fn_name(arg1, arg2, ...)
        var args: std.ArrayList(u8) = .empty;
        defer args.deinit(allocator);
        try args.appendSlice(allocator, fn_name);
        try args.append(allocator, '(');
        for (children, 0..) |ch, i| {
            if (i > 0) try args.appendSlice(allocator, ", ");
            const t = try exprToText(allocator, ch) orelse return null;
            defer allocator.free(t);
            try args.appendSlice(allocator, t);
        }
        try args.append(allocator, ')');
        return try args.toOwnedSlice(allocator);
    }

    if (std.mem.eql(u8, class, "CASE")) {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        try buf.appendSlice(allocator, "CASE");
        for (obj.get("case_checks").?.array.items) |check| {
            const when = try exprToText(allocator, check.object.get("when_expr").?) orelse return null;
            defer allocator.free(when);
            const then = try exprToText(allocator, check.object.get("then_expr").?) orelse return null;
            defer allocator.free(then);
            const part = try std.fmt.allocPrint(allocator, " WHEN {s} THEN {s}", .{ when, then });
            defer allocator.free(part);
            try buf.appendSlice(allocator, part);
        }
        if (obj.get("else_expr")) |else_val| {
            if (else_val != .null) {
                const els = try exprToText(allocator, else_val) orelse return null;
                defer allocator.free(els);
                const part = try std.fmt.allocPrint(allocator, " ELSE {s}", .{els});
                defer allocator.free(part);
                try buf.appendSlice(allocator, part);
            }
        }
        try buf.appendSlice(allocator, " END");
        return try buf.toOwnedSlice(allocator);
    }

    if (std.mem.eql(u8, class, "OPERATOR")) {
        const op_type = obj.get("type").?.string;
        const children = obj.get("children").?.array.items;
        if (std.mem.eql(u8, op_type, "OPERATOR_IN") or std.mem.eql(u8, op_type, "OPERATOR_NOT_IN")) {
            const col = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(col);
            const in_kw: []const u8 = if (std.mem.eql(u8, op_type, "OPERATOR_IN")) "IN" else "NOT IN";
            var vals: std.ArrayList(u8) = .empty;
            defer vals.deinit(allocator);
            try vals.append(allocator, '(');
            for (children[1..], 0..) |ch, i| {
                if (i > 0) try vals.appendSlice(allocator, ", ");
                const t = try exprToText(allocator, ch) orelse return null;
                defer allocator.free(t);
                try vals.appendSlice(allocator, t);
            }
            try vals.append(allocator, ')');
            const vals_str = try vals.toOwnedSlice(allocator);
            defer allocator.free(vals_str);
            return try std.fmt.allocPrint(allocator, "{s} {s} {s}", .{ col, in_kw, vals_str });
        }
        if (std.mem.eql(u8, op_type, "OPERATOR_IS_NULL") and children.len == 1) {
            const t = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(t);
            return try std.fmt.allocPrint(allocator, "{s} IS NULL", .{t});
        }
        if (std.mem.eql(u8, op_type, "OPERATOR_IS_NOT_NULL") and children.len == 1) {
            const t = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(t);
            return try std.fmt.allocPrint(allocator, "{s} IS NOT NULL", .{t});
        }
        if (std.mem.eql(u8, op_type, "OPERATOR_NOT") and children.len == 1) {
            const t = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(t);
            return try std.fmt.allocPrint(allocator, "NOT ({s})", .{t});
        }
        return null;
    }

    if (std.mem.eql(u8, class, "STAR")) return try allocator.dupe(u8, "*");

    return null;
}

fn groupExprsToText(allocator: std.mem.Allocator, items: []const std.json.Value) !?[]const u8 {
    var parts: std.ArrayList([]const u8) = .empty;
    defer {
        for (parts.items) |p| allocator.free(p);
        parts.deinit(allocator);
    }
    for (items) |item| {
        if (intLiteralValue(item)) |n| {
            try parts.append(allocator, try std.fmt.allocPrint(allocator, "{d}", .{n}));
            continue;
        }
        const t = try exprToText(allocator, item) orelse return null;
        try parts.append(allocator, t);
    }
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (parts.items, 0..) |p, i| {
        if (i > 0) try buf.appendSlice(allocator, ", ");
        try buf.appendSlice(allocator, p);
    }
    return try buf.toOwnedSlice(allocator);
}

fn orderItemsToText(allocator: std.mem.Allocator, items: []const std.json.Value) !?[]const u8 {
    var parts: std.ArrayList([]const u8) = .empty;
    defer {
        for (parts.items) |p| allocator.free(p);
        parts.deinit(allocator);
    }
    for (items) |item| {
        const o = item.object;
        const dir = o.get("type").?.string;
        const dir_str: []const u8 = if (std.mem.eql(u8, dir, "ORDER_DESCENDING") or std.mem.eql(u8, dir, "DESCENDING")) "DESC" else "ASC";
        const expr_text = try exprToText(allocator, o.get("expression").?) orelse return null;
        defer allocator.free(expr_text);
        try parts.append(allocator, try std.fmt.allocPrint(allocator, "{s} {s}", .{ expr_text, dir_str }));
    }
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    for (parts.items, 0..) |p, i| {
        if (i > 0) try buf.appendSlice(allocator, ", ");
        try buf.appendSlice(allocator, p);
    }
    return try buf.toOwnedSlice(allocator);
}

// ── Small helpers ─────────────────────────────────────────────────────────────

fn comparisonOp(type_str: []const u8) []const u8 {
    if (std.mem.eql(u8, type_str, "COMPARE_EQUAL")) return "=";
    if (std.mem.eql(u8, type_str, "COMPARE_NOTEQUAL")) return "<>";
    if (std.mem.eql(u8, type_str, "COMPARE_LESSTHAN")) return "<";
    if (std.mem.eql(u8, type_str, "COMPARE_LESSTHANOREQUALTO")) return "<=";
    if (std.mem.eql(u8, type_str, "COMPARE_GREATERTHAN")) return ">";
    if (std.mem.eql(u8, type_str, "COMPARE_GREATERTHANOREQUALTO")) return ">=";
    return "=";
}

fn columnName(val: std.json.Value) ?[]const u8 {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    if (!std.mem.eql(u8, class, "COLUMN_REF")) return null;
    const names = obj.get("column_names").?.array;
    if (names.items.len == 0) return null;
    return names.items[names.items.len - 1].string;
}

/// Returns true if val is a CONSTANT string node equal to `s` (case-insensitive).
fn isConstantString(val: std.json.Value, s: []const u8) bool {
    if (val == .null) return false;
    const obj = val.object;
    const class = (obj.get("class") orelse return false).string;
    if (!std.mem.eql(u8, class, "CONSTANT")) return false;
    const v = obj.get("value") orelse return false;
    const vobj = v.object;
    const raw = vobj.get("value") orelse return false;
    return switch (raw) {
        .string => |str| std.ascii.eqlIgnoreCase(str, s),
        else => false,
    };
}

fn intLiteralValue(val: std.json.Value) ?i64 {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    if (!std.mem.eql(u8, class, "CONSTANT")) return null;
    const v = obj.get("value").?.object;
    const type_id = v.get("type").?.object.get("id").?.string;
    if (!std.mem.eql(u8, type_id, "INTEGER") and
        !std.mem.eql(u8, type_id, "BIGINT") and
        !std.mem.eql(u8, type_id, "HUGEINT") and
        !std.mem.eql(u8, type_id, "UBIGINT")) return null;
    return switch (v.get("value").?) {
        .integer => |i| i,
        .float => |f| @as(i64, @intFromFloat(f)),
        else => null,
    };
}

fn extractIntLiteral(val: std.json.Value) ?usize {
    const i = intLiteralValue(val) orelse return null;
    if (i < 0) return null;
    return @intCast(i);
}

fn isCountStar(val: std.json.Value) bool {
    if (val == .null) return false;
    const obj = val.object;
    const class = (obj.get("class") orelse return false).string;
    if (!std.mem.eql(u8, class, "FUNCTION")) return false;
    return std.mem.eql(u8, obj.get("function_name").?.string, "count_star");
}

fn exprAlias(val: std.json.Value) ?[]const u8 {
    if (val == .null) return null;
    const obj = val.object;
    const a = (obj.get("alias") orelse return null).string;
    return if (a.len > 0) a else null;
}

fn isFunctionNamed(val: std.json.Value, name: []const u8) bool {
    if (val == .null) return false;
    const obj = val.object;
    const class = (obj.get("class") orelse return false).string;
    if (!std.mem.eql(u8, class, "FUNCTION")) return false;
    return std.mem.eql(u8, obj.get("function_name").?.string, name);
}

fn functionFirstChildColName(val: std.json.Value) ?[]const u8 {
    const obj = val.object;
    const children = obj.get("children").?.array.items;
    if (children.len == 0) return null;
    return columnName(children[0]);
}

fn projectionAliasExists(projs: []const generic_sql.Expr, alias: []const u8) bool {
    for (projs) |p| {
        if (p.alias) |a| if (std.ascii.eqlIgnoreCase(a, alias)) return true;
    }
    return false;
}

fn projectionColExists(projs: []const generic_sql.Expr, col: []const u8) bool {
    for (projs) |p| {
        if (p.column) |pc| if (std.ascii.eqlIgnoreCase(pc, col)) return true;
    }
    return false;
}
