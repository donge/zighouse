/// DuckDB-backed SQL parser: calls json_serialize_sql() to obtain a parse tree
/// and translates the resulting JSON AST into a generic_sql.Plan.
///
/// Enabled only when the project is built with -Dduckdb=true (the default).
/// When DuckDB is not linked the public `parse` function returns null so that
/// the caller falls back to the legacy hand-written parser.
const std = @import("std");
const build_options = @import("build_options");
const generic_sql = @import("generic_sql.zig");
const ch_compat = @import("ch_compat.zig");

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
                // Register ClickHouse-compatible scalar macros so DuckDB can parse
                // queries that use CH-specific function names.  These macros are
                // semantically approximate; the real evaluation happens in our own
                // evalTextExpr runtime.  Registrations are best-effort (errors ignored).
                registerMacros(con);
            } else {
                c.duckdb_close(&db);
            }
        }
        g_conn_ready = true;
    }
    return g_conn.con;
}

fn registerMacros(con: c.duckdb_connection) void {
    const macros = [_][*:0]const u8{
        "CREATE MACRO IF NOT EXISTS isIPv4String(x) AS (regexp_matches(CAST(x AS VARCHAR), '^[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+$'))",
        "CREATE MACRO IF NOT EXISTS isIPv6String(x) AS (regexp_matches(CAST(x AS VARCHAR), '.*:.*'))",
        "CREATE MACRO IF NOT EXISTS IPv4StringToNumOrDefault(x) AS (0)",
        "CREATE MACRO IF NOT EXISTS IPv6StringToNumOrDefault(x) AS (0)",
        "CREATE MACRO IF NOT EXISTS dictGetOrDefault(d, a, k, def) AS (def)",
        "CREATE MACRO IF NOT EXISTS dictHas(d, k) AS (false)",
        "CREATE MACRO IF NOT EXISTS toIPv4(x) AS (CAST(x AS VARCHAR))",
        "CREATE MACRO IF NOT EXISTS toIPv6(x) AS (CAST(x AS VARCHAR))",
        "CREATE MACRO IF NOT EXISTS risk_score(proto, feats) AS (0.0)",
    };
    for (macros) |m| {
        var r: c.duckdb_result = undefined;
        _ = c.duckdb_query(con, m, &r);
        c.duckdb_destroy_result(&r);
    }
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

    // Pre-process: rewrite CH-specific grammar that DuckDB cannot parse.
    // Returns null for constructs we cannot handle (e.g. ARRAY JOIN).
    const processed = try ch_compat.rewrite(allocator, sql) orelse return null;
    defer allocator.free(processed);

    // Build: SELECT json_serialize_sql($$<sql>$$)
    // Dollar-quoting ($$...$$) avoids escaping; safe as long as sql
    // doesn't contain "$$" — which no ClickBench query does.
    const query_str = try std.fmt.allocPrint(
        allocator,
        "SELECT json_serialize_sql($${s}$$)",
        .{processed},
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

    // Only handle plain SELECT and UNION ALL
    const node_type = (node_obj.get("type") orelse return error.Unsupported).string;
    if (std.mem.eql(u8, node_type, "SET_OPERATION_NODE")) {
        // UNION ALL: translate left and right, link via union_other
        const set_op = (node_obj.get("setop_type") orelse return error.Unsupported).string;
        if (!std.mem.eql(u8, set_op, "UNION_ALL") and !std.mem.eql(u8, set_op, "UNION")) return error.Unsupported;
        const left_val = node_obj.get("left") orelse return error.Unsupported;
        const right_val = node_obj.get("right") orelse return error.Unsupported;
        const left_type = (left_val.object.get("type") orelse return error.Unsupported).string;
        const right_type = (right_val.object.get("type") orelse return error.Unsupported).string;
        if (!std.mem.eql(u8, left_type, "SELECT_NODE")) return error.Unsupported;
        if (!std.mem.eql(u8, right_type, "SELECT_NODE")) return error.Unsupported;
        var left_plan = try translateSelectNode(allocator, left_val.object);
        errdefer generic_sql.deinit(allocator, left_plan);
        const right_plan_ptr = try allocator.create(generic_sql.Plan);
        errdefer { allocator.destroy(right_plan_ptr); }
        right_plan_ptr.* = try translateSelectNode(allocator, right_val.object);
        left_plan.union_other = right_plan_ptr;
        return left_plan;
    }
    if (!std.mem.eql(u8, node_type, "SELECT_NODE")) return error.Unsupported;

    return translateSelectNode(allocator, node_obj);
}

/// Translate a SELECT_NODE JSON object into a Plan (recursive for subqueries).
fn translateSelectNode(allocator: std.mem.Allocator, node_obj: std.json.ObjectMap) anyerror!generic_sql.Plan {
    // ── table name ──────────────────────────────────────────────────────────
    // If there is no FROM clause (e.g. SELECT 1, SELECT now()), treat as
    // FROM system.one — matches ClickHouse behaviour.
    var subquery_source: ?*generic_sql.Plan = null;
    errdefer if (subquery_source) |sq| { generic_sql.deinit(allocator, sq.*); allocator.destroy(sq); };
    const table_name = blk: {
        const from_table = node_obj.get("from_table") orelse {
            break :blk try allocator.dupe(u8, "system.one");
        };
        const name = try extractTableNameOrSubquery(allocator, from_table, &subquery_source) orelse
            break :blk try allocator.dupe(u8, "system.one");
        break :blk name;
    };
    errdefer allocator.free(table_name);

    // ── WITH / CTE support: if from_table is a CTE name, inline it as subquery ─
    if (subquery_source == null) {
        if (node_obj.get("cte_map")) |cte_map_val| {
            if (cte_map_val == .object) {
                if (cte_map_val.object.get("map")) |map_items| {
                    for (map_items.array.items) |entry| {
                        const key = (entry.object.get("key") orelse continue).string;
                        if (std.ascii.eqlIgnoreCase(key, table_name)) {
                            const cte_val = entry.object.get("value") orelse continue;
                            const cte_query = (cte_val.object.get("query") orelse continue).object;
                            const inner_node = cte_query.get("node") orelse continue;
                            const inner_type = (inner_node.object.get("type") orelse continue).string;
                            if (!std.mem.eql(u8, inner_type, "SELECT_NODE")) continue;
                            const inner_plan = try translateSelectNode(allocator, inner_node.object);
                            const sq = try allocator.create(generic_sql.Plan);
                            sq.* = inner_plan;
                            subquery_source = sq;
                            break;
                        }
                    }
                }
            }
        }
    }

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

    // ── Lift nested aggregates out of column_ref projections ─────────────────
    // e.g. `if(total > 0, avg(confidence), 0) AS avg_conf` becomes a column_ref whose
    // text is `CASE WHEN total > 0 THEN avg(confidence) ELSE 0 END`. The nested
    // `avg(confidence)` won't have a corresponding AggState unless we lift it here.
    {
        const agg_fns = [_][]const u8{ "avg(", "max(", "min(", "sum(", "any(" };
        var extra: std.ArrayList(generic_sql.Expr) = .empty;
        defer extra.deinit(allocator);
        for (projections.items) |*proj| {
            if (proj.func != .column_ref) continue;
            if (proj.column == null) continue;
            // Scan for agg function calls inside the text
            for (agg_fns) |pfx| {
                var search_pos: usize = 0;
                while (std.mem.indexOfPos(u8, proj.column.?, search_pos, pfx)) |pos| {
                    // Use current proj.column for all lookups (may have been updated by prior replacement)
                    const cur_text = proj.column.?;
                    // Verify this is a standalone function call (not inside an identifier like "hasAny")
                    if (pos > 0 and (std.ascii.isAlphanumeric(cur_text[pos - 1]) or cur_text[pos - 1] == '_')) {
                        search_pos = pos + 1;
                        continue;
                    }
                    // Find matching closing paren
                    var depth: usize = 1;
                    var end: usize = pos + pfx.len;
                    while (end < cur_text.len and depth > 0) : (end += 1) {
                        if (cur_text[end] == '(') depth += 1
                        else if (cur_text[end] == ')') depth -= 1;
                    }
                    const inner_col = std.mem.trim(u8, cur_text[pos + pfx.len .. end - 1], " \t");
                    // Build a hidden alias like "__ha__avg_confidence"
                    const fn_name = pfx[0 .. pfx.len - 1]; // strip "("
                    const safe_inner = blk: {
                        var sb: std.ArrayList(u8) = .empty;
                        for (inner_col) |ch| {
                            if (std.ascii.isAlphanumeric(ch) or ch == '_') sb.append(allocator, ch) catch {};
                        }
                        break :blk sb.toOwnedSlice(allocator) catch inner_col;
                    };
                    defer allocator.free(safe_inner);
                    const hidden_alias = try std.fmt.allocPrint(allocator, "__ha__{s}_{s}", .{ fn_name, safe_inner });
                    // Dupe inner_col BEFORE freeing the old text (inner_col is a slice of cur_text)
                    const inner_col_dup = try allocator.dupe(u8, inner_col);
                    // Replace the agg call in the text with the hidden alias
                    const old_call = cur_text[pos .. end];
                    const new_text = try std.mem.replaceOwned(u8, allocator, proj.column.?, old_call, hidden_alias);
                    allocator.free(proj.column.?);
                    proj.column = new_text;
                    // Check that this hidden projection doesn't already exist
                    var already = false;
                    for (projections.items) |ep| {
                        if (ep.alias) |a| {
                            if (std.mem.eql(u8, a, hidden_alias)) { already = true; break; }
                        }
                    }
                    for (extra.items) |ep| {
                        if (ep.alias) |a| {
                            if (std.mem.eql(u8, a, hidden_alias)) { already = true; break; }
                        }
                    }
                    if (!already) {
                        const fn_func: generic_sql.AggregateFn = if (std.mem.eql(u8, fn_name, "avg")) .avg
                            else if (std.mem.eql(u8, fn_name, "max")) .max
                            else if (std.mem.eql(u8, fn_name, "min")) .min
                            else if (std.mem.eql(u8, fn_name, "sum")) .sum
                            else .any_val;
                        try extra.append(allocator, .{ .func = fn_func, .column = inner_col_dup, .alias = hidden_alias });
                    } else {
                        allocator.free(inner_col_dup);
                        allocator.free(hidden_alias);
                    }
                    search_pos = pos + hidden_alias.len;
                    const new_col_len = if (proj.column) |nc| nc.len else 0;
                    if (search_pos >= new_col_len) break;
                }
            }
        }
        for (extra.items) |ep| try projections.append(allocator, ep);
    }

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
    var having_expr: ?*generic_sql.WhereNode = null;
    if (having_val != null and having_val.? != .null) {
        having_text = try exprToText(allocator, having_val.?);
        // Also try to parse as a structured WhereNode for IR planner.
        having_expr = translateWhere(allocator, having_val.?) catch null;
    }
    errdefer if (having_text) |s| allocator.free(s);
    errdefer if (having_expr) |he| generic_sql.freeWhereNode(allocator, he);

    // ── ORDER BY ─────────────────────────────────────────────────────────────
    var order_by_count_desc = false;
    var order_by_alias: ?[]const u8 = null;
    var order_by_alias_asc: bool = false;
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
                    } else {
                        // DESC or ASC: check if expr is a COLUMN_REF matching a projection alias
                        const is_asc = std.mem.eql(u8, dir, "ORDER_ASCENDING") or std.mem.eql(u8, dir, "ASCENDING");
                        const alias_candidate: ?[]const u8 = if (exprAlias(expr0)) |a| a
                            else if (columnName(expr0)) |col| col
                            else null;
                        if (alias_candidate) |alias| {
                            const found = projectionAliasExists(projections.items, alias);
                            if (found) {
                                order_by_alias = try allocator.dupe(u8, alias);
                                order_by_alias_asc = is_asc;
                                allocator.free(order_by_text.?);
                                order_by_text = null;
                            }
                        } else {
                            // No simple alias: try matching ORDER BY expr text to a projection expr text.
                            // This handles e.g. ORDER BY DATE_TRUNC('minute', col) matching AS M.
                            const ob_expr_text = exprToText(allocator, expr0) catch null;
                            defer if (ob_expr_text) |t| allocator.free(t);
                            if (ob_expr_text) |obt| {
                                if (findProjectionAliasByExprText(allocator, select_list.items, obt)) |matched_alias| {
                                    order_by_alias = try allocator.dupe(u8, matched_alias);
                                    order_by_alias_asc = is_asc;
                                    allocator.free(order_by_text.?);
                                    order_by_text = null;
                                }
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
        .having_expr = having_expr,
        .having_text = having_text,
        .order_by_count_desc = order_by_count_desc,
        .order_by_alias = order_by_alias,
        .order_by_alias_asc = order_by_alias_asc,
        .order_by_text = order_by_text,
        .limit = limit,
        .offset = offset,
        .subquery_source = subquery_source,
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

        const col_name: []const u8 = if (columnName(left)) |cn|
            try allocator.dupe(u8, cn)
        else
            try exprToText(allocator, left) orelse return error.UnsupportedWhereNode;
        const col = col_name;
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

        // Neither int nor string literal — unsupported.
        // errdefer at line 307 will free `col`.
        return error.UnsupportedWhereNode;
    }

    // ── BETWEEN: col BETWEEN lower AND upper → col >= lower AND col <= upper ─
    if (std.mem.eql(u8, class, "BETWEEN")) {
        const input_node = obj.get("input").?;
        const lower_node = obj.get("lower").?;
        const upper_node = obj.get("upper").?;
        const col_name = columnName(input_node) orelse return error.UnsupportedWhereNode;

        var kids = try allocator.alloc(*generic_sql.WhereNode, 2);
        var n_built: usize = 0;
        errdefer {
            for (kids[0..n_built]) |k| generic_sql.freeWhereNode(allocator, k);
            allocator.free(kids);
        }

        // lower bound: col >= lower
        {
            const col = try allocator.dupe(u8, col_name);
            if (epochMsFromNode(lower_node)) |iv| {
                const node = try allocator.create(generic_sql.WhereNode);
                node.* = .{ .cmp_int = .{ .col = col, .op = .ge, .val = iv } };
                kids[n_built] = node;
                n_built += 1;
            } else if (strLiteralValue(lower_node)) |sv| {
                const val_owned = allocator.dupe(u8, sv) catch { allocator.free(col); return error.UnsupportedWhereNode; };
                const node = allocator.create(generic_sql.WhereNode) catch { allocator.free(val_owned); allocator.free(col); return error.UnsupportedWhereNode; };
                node.* = .{ .cmp_str = .{ .col = col, .op = .ge, .val = val_owned } };
                kids[n_built] = node;
                n_built += 1;
            } else {
                allocator.free(col);
                return error.UnsupportedWhereNode;
            }
        }

        // upper bound: col <= upper
        {
            const col = try allocator.dupe(u8, col_name);
            if (epochMsFromNode(upper_node)) |iv| {
                const node = try allocator.create(generic_sql.WhereNode);
                node.* = .{ .cmp_int = .{ .col = col, .op = .le, .val = iv } };
                kids[n_built] = node;
                n_built += 1;
            } else if (strLiteralValue(upper_node)) |sv| {
                const val_owned = allocator.dupe(u8, sv) catch { allocator.free(col); return error.UnsupportedWhereNode; };
                const node = allocator.create(generic_sql.WhereNode) catch { allocator.free(val_owned); allocator.free(col); return error.UnsupportedWhereNode; };
                node.* = .{ .cmp_str = .{ .col = col, .op = .le, .val = val_owned } };
                kids[n_built] = node;
                n_built += 1;
            } else {
                allocator.free(col);
                return error.UnsupportedWhereNode;
            }
        }

        const node = try allocator.create(generic_sql.WhereNode);
        node.* = .{ .and_ = kids };
        return node;
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
        if ((std.mem.eql(u8, op_type, "OPERATOR_IS_NULL") or
             std.mem.eql(u8, op_type, "OPERATOR_IS_NOT_NULL")) and children.len == 1)
        {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            const node = try allocator.create(generic_sql.WhereNode);
            if (std.mem.eql(u8, op_type, "OPERATOR_IS_NULL")) {
                node.* = .{ .is_null = col };
            } else {
                node.* = .{ .is_not_null = col };
            }
            return node;
        }
        if ((std.mem.eql(u8, op_type, "OPERATOR_IN") or std.mem.eql(u8, op_type, "OPERATOR_NOT_IN") or
             std.mem.eql(u8, op_type, "COMPARE_IN") or std.mem.eql(u8, op_type, "COMPARE_NOT_IN")) and
            children.len >= 2)
        {
            const col_name = columnName(children[0]) orelse return error.UnsupportedWhereNode;
            const col = try allocator.dupe(u8, col_name);
            errdefer allocator.free(col);
            const negate = std.mem.eql(u8, op_type, "OPERATOR_NOT_IN") or std.mem.eql(u8, op_type, "COMPARE_NOT_IN");
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
                    // errdefers at lines 384 and 378 will handle cleanup.
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
            // Create the node before freeing col (create can fail).
            const node = try allocator.create(generic_sql.WhereNode);
            node.* = if (negate) .{ .and_ = kids } else .{ .or_ = kids };
            // Free the original col copy since each kid has its own.
            allocator.free(col);
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

/// Parse a simple comparison node like `confidence >= 0.9` or `col = 'str'`
/// into a CondExpr (heap-allocated; caller owns it via deinit).
fn parseCondExpr(allocator: std.mem.Allocator, val: std.json.Value) !?*generic_sql.CondExpr {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;

    // For complex conditions (CONJUNCTION, BETWEEN, etc.) store as text.
    if (!std.mem.eql(u8, class, "COMPARISON")) {
        const text = try exprToText(allocator, val) orelse return null;
        errdefer allocator.free(text);
        const cond = try allocator.create(generic_sql.CondExpr);
        cond.* = .{ .cond_text = text };
        return cond;
    }

    const type_str = (obj.get("type") orelse return null).string;
    const op = parseCmpOp(type_str) orelse return null;

    // DuckDB COMPARISON nodes use "left"/"right" keys (not "children").
    const left_node = obj.get("left") orelse return null;
    const right_node = obj.get("right") orelse return null;

    // Left side: column reference or expression (encode as text)
    const col_name = blk: {
        if (columnName(left_node)) |cn| {
            break :blk try allocator.dupe(u8, cn);
        }
        // fallback: expression as text
        break :blk try exprToText(allocator, left_node) orelse return null;
    };
    errdefer allocator.free(col_name);

    // Right side: numeric or string constant
    if (floatLiteralValue(right_node)) |fv| {
        const cond = try allocator.create(generic_sql.CondExpr);
        cond.* = .{ .cond_col = col_name, .cond_op = op, .cond_num = fv };
        return cond;
    }
    if (strLiteralValue(right_node)) |sv| {
        const sv_dup = try allocator.dupe(u8, sv);
        const cond = try allocator.create(generic_sql.CondExpr);
        cond.* = .{ .cond_col = col_name, .cond_op = op, .cond_str = sv_dup };
        return cond;
    }
    if (intLiteralValue(right_node)) |iv| {
        const cond = try allocator.create(generic_sql.CondExpr);
        cond.* = .{ .cond_col = col_name, .cond_op = op, .cond_num = @floatFromInt(iv) };
        return cond;
    }

    allocator.free(col_name);
    return null;
}

/// Extract a float literal value from a CONSTANT node (also handles int and DECIMAL).
fn floatLiteralValue(val: std.json.Value) ?f64 {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    if (!std.mem.eql(u8, class, "CONSTANT")) return null;
    const v = obj.get("value").?.object;
    const raw = v.get("value") orelse return null;
    const base: f64 = switch (raw) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => return null,
    };
    // DECIMAL type: value is stored as an integer scaled by 10^scale.
    if (v.get("type")) |type_node| {
        if (type_node == .object) {
            const type_id = (type_node.object.get("id") orelse return base).string;
            if (std.mem.eql(u8, type_id, "DECIMAL")) {
                if (type_node.object.get("type_info")) |ti| {
                    if (ti == .object) {
                        if (ti.object.get("scale")) |scale_node| {
                            const scale: i64 = switch (scale_node) {
                                .integer => |i| i,
                                else => 0,
                            };
                            if (scale > 0) {
                                var divisor: f64 = 1.0;
                                for (0..@intCast(scale)) |_| divisor *= 10.0;
                                return base / divisor;
                            }
                        }
                    }
                }
            }
        }
    }
    return base;
}

/// Extract a string/date/timestamp literal value from a CONSTANT node.
fn strLiteralValue(val: std.json.Value) ?[]const u8 {
    if (val == .null) return null;
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    // CAST(str AS TIMESTAMP/DATE/…) → unwrap to the child constant string
    if (std.mem.eql(u8, class, "CAST")) {
        const child = obj.get("child") orelse return null;
        return strLiteralValue(child);
    }
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
    return extractTableNameOrSubquery(allocator, from, null);
}

/// Like extractTableName, but also handles SUBQUERY type by recursing.
/// If `subquery_out` is non-null, it will be set to the parsed inner plan (caller owns it).
fn extractTableNameOrSubquery(allocator: std.mem.Allocator, from: std.json.Value, subquery_out: ?*?*generic_sql.Plan) !?[]u8 {
    if (from == .null) return null;
    const obj = from.object;
    const t = (obj.get("type") orelse return null).string;
    if (std.mem.eql(u8, t, "SUBQUERY")) {
        // FROM (SELECT ...) [AS alias]
        if (subquery_out == null) return null; // caller doesn't want subquery
        const subquery_val = obj.get("subquery") orelse return null;
        // subquery_val is {"node": {"type":"SELECT_NODE", ...}, "named_param_map":[]}
        const inner_node_val = subquery_val.object.get("node") orelse return null;
        const inner_type = (inner_node_val.object.get("type") orelse return null).string;
        if (!std.mem.eql(u8, inner_type, "SELECT_NODE")) return null;
        const inner_plan = try translateSelectNode(allocator, inner_node_val.object);
        const sq = try allocator.create(generic_sql.Plan);
        sq.* = inner_plan;
        subquery_out.?.* = sq;
        // Use alias if present, else "__subquery__".
        const alias_raw = obj.get("alias") orelse return try allocator.dupe(u8, "__subquery__");
        if (alias_raw == .string and alias_raw.string.len > 0)
            return try allocator.dupe(u8, alias_raw.string);
        return try allocator.dupe(u8, "__subquery__");
    }
    if (!std.mem.eql(u8, t, "BASE_TABLE")) return null;
    const name = (obj.get("table_name") orelse return null).string;
    // If schema_name is set and non-empty and not "main", reconstruct "schema.table".
    if (obj.get("schema_name")) |schema_val| {
        if (schema_val == .string) {
            const schema_str = schema_val.string;
            if (schema_str.len > 0 and !std.mem.eql(u8, schema_str, "main")) {
                return try std.fmt.allocPrint(allocator, "{s}.{s}", .{ schema_str, name });
            }
        }
    }
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
        // Float/Decimal literals (e.g. 0.0, 0.95)
        if (std.mem.eql(u8, type_id, "FLOAT") or
            std.mem.eql(u8, type_id, "DOUBLE"))
        {
            const fval: f64 = switch (v.get("value").?) {
                .integer => |i| @as(f64, @floatFromInt(i)),
                .float => |f| f,
                else => return null,
            };
            return .{ .func = .float_literal, .float_val = fval, .alias = alias };
        }
        if (std.mem.eql(u8, type_id, "DECIMAL")) {
            const type_info = if (v.get("type").?.object.get("type_info")) |ti| ti else return null;
            const scale: i64 = if (type_info == .object) blk: {
                if (type_info.object.get("scale")) |sn|
                    break :blk switch (sn) { .integer => |i| i, else => 0 };
                break :blk 0;
            } else 0;
            const raw_int: i64 = switch (v.get("value").?) {
                .integer => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return null,
            };
            if (scale <= 0) return .{ .func = .int_literal, .int_offset = raw_int, .alias = alias };
            var divisor: f64 = 1.0;
            for (0..@intCast(scale)) |_| divisor *= 10.0;
            const fval: f64 = @as(f64, @floatFromInt(raw_int)) / divisor;
            return .{ .func = .float_literal, .float_val = fval, .alias = alias };
        }
        // VARCHAR string literal
        if (std.mem.eql(u8, type_id, "VARCHAR") or
            std.mem.eql(u8, type_id, "DATE") or
            std.mem.eql(u8, type_id, "TIMESTAMP"))
        {
            const s = switch (v.get("value").?) {
                .string => |sv| sv,
                else => return null,
            };
            const col = try std.fmt.allocPrint(allocator, "'{s}'", .{s});
            errdefer allocator.free(col);
            return .{ .func = .column_ref, .column = col, .alias = alias };
        }
        return null;
    }

    // Map subscript: data['key'] → OPERATOR ARRAY_EXTRACT → column_ref "data['key']"
    if (std.mem.eql(u8, class, "OPERATOR")) {
        const op_type = obj.get("type").?.string;
        const children_node = obj.get("children") orelse return null;
        const children = children_node.array.items;
        if (std.mem.eql(u8, op_type, "ARRAY_EXTRACT") and children.len == 2) {
            // Reconstruct as text: col['key']
            const map_col = columnName(children[0]) orelse return null;
            const key = strLiteralValue(children[1]) orelse return null;
            const col_text = try std.fmt.allocPrint(allocator, "{s}['{s}']", .{ map_col, key });
            errdefer allocator.free(col_text);
            const alias_owned = alias;
            return .{ .func = .column_ref, .column = col_text, .alias = alias_owned };
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
            // General expression: render to text
            const col = try exprToText(allocator, child) orelse return null;
            return .{ .func = .sum, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "avg") and children.len == 1) {
            const inner = children[0];
            if (isFunctionNamed(inner, "length")) {
                const col_name = functionFirstChildColName(inner) orelse return null;
                const col = try std.fmt.allocPrint(allocator, "length({s})", .{col_name});
                return .{ .func = .avg, .column = col, .alias = alias };
            }
            const col = if (columnName(inner)) |cn|
                try allocator.dupe(u8, cn)
            else
                try exprToText(allocator, inner) orelse return null;
            return .{ .func = .avg, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "min") and children.len == 1) {
            const col = if (columnName(children[0])) |cn|
                try allocator.dupe(u8, cn)
            else
                try exprToText(allocator, children[0]) orelse return null;
            return .{ .func = .min, .column = col, .alias = alias };
        }
        if (std.mem.eql(u8, fn_name, "max") and children.len == 1) {
            const col = if (columnName(children[0])) |cn|
                try allocator.dupe(u8, cn)
            else
                try exprToText(allocator, children[0]) orelse return null;
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
            const child_col = columnName(children[0]) orelse {
                // Complex inner expression — fall through to text rendering
                const fn_text = try exprToText(allocator, val) orelse return null;
                return .{ .func = .column_ref, .column = fn_text, .alias = alias };
            };
            const col = try std.fmt.allocPrint(allocator, "length({s})", .{child_col});
            return .{ .func = .column_ref, .column = col, .alias = alias };
        }
        // countIf(expr) — condition is the argument expression
        if (std.mem.eql(u8, fn_name, "countif") and children.len == 1) {
            const cond = try parseCondExpr(allocator, children[0]) orelse return null;
            return .{ .func = .count_if, .column = null, .alias = alias, .cond = cond };
        }
        // uniqExact(col)
        if ((std.mem.eql(u8, fn_name, "uniqexact") or std.mem.eql(u8, fn_name, "approx_count_distinct")) and children.len == 1) {
            const child_col = columnName(children[0]) orelse {
                // Could be an expression like if(...)
                const col = try exprToText(allocator, children[0]) orelse return null;
                return .{ .func = .uniq_exact, .column = col, .alias = alias };
            };
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .uniq_exact, .column = col, .alias = alias };
        }
        // uniqExactIf(col, cond)
        if (std.mem.eql(u8, fn_name, "uniqexactif") and children.len == 2) {
            const child_col = columnName(children[0]) orelse {
                const col = try exprToText(allocator, children[0]) orelse return null;
                const cond = try parseCondExpr(allocator, children[1]) orelse {
                    allocator.free(col);
                    return null;
                };
                return .{ .func = .uniq_exact_if, .column = col, .alias = alias, .cond = cond };
            };
            const col = try allocator.dupe(u8, child_col);
            const cond = try parseCondExpr(allocator, children[1]) orelse {
                allocator.free(col);
                return null;
            };
            return .{ .func = .uniq_exact_if, .column = col, .alias = alias, .cond = cond };
        }
        // groupUniqArray(col) — array agg of distinct values
        if (std.mem.eql(u8, fn_name, "groupuniqarray") and children.len == 1) {
            const child_col = columnName(children[0]) orelse return null;
            const col = try allocator.dupe(u8, child_col);
            return .{ .func = .group_uniq_array, .column = col, .alias = alias };
        }
        // arrayStringConcat(groupUniqArray(col), sep) → group_uniq_array with sep
        // Also handles array_to_string(...) which is the DuckDB rename of arrayStringConcat.
        if ((std.mem.eql(u8, fn_name, "arraystringconcat") or std.mem.eql(u8, fn_name, "array_to_string")) and children.len >= 1) {
            const inner = children[0];
            const inner_obj = inner.object;
            const inner_class = (inner_obj.get("class") orelse return null).string;
            if (std.mem.eql(u8, inner_class, "FUNCTION")) {
                const inner_fn = inner_obj.get("function_name").?.string;
                if (std.mem.eql(u8, inner_fn, "groupuniqarray")) {
                    const inner_children = inner_obj.get("children").?.array.items;
                    if (inner_children.len == 1) {
                        const child_col = columnName(inner_children[0]) orelse return null;
                        const col = try allocator.dupe(u8, child_col);
                        const sep: ?[]const u8 = if (children.len >= 2)
                            if (strLiteralValue(children[1])) |sv| try allocator.dupe(u8, sv) else null
                        else null;
                        return .{ .func = .group_uniq_array, .column = col, .alias = alias, .sep = sep };
                    }
                }
            }
            // arrayStringConcat(some_array_expr, sep) — generic: render full expression as column_ref
            // (handles arrayFilter, arrayDistinct, literal arrays, etc.)
            const fn_text2 = try exprToText(allocator, val) orelse return null;
            return .{ .func = .column_ref, .column = fn_text2, .alias = alias };
        }
        // any(col) / any_value(col)
        if ((std.mem.eql(u8, fn_name, "any") or std.mem.eql(u8, fn_name, "any_value") or std.mem.eql(u8, fn_name, "first")) and children.len == 1) {
            // Try simple column name first.
            if (columnName(children[0])) |child_col| {
                const col = try allocator.dupe(u8, child_col);
                return .{ .func = .any_val, .column = col, .alias = alias };
            }
            // Complex expression (e.g. any(if(...))): render as text.
            if (exprToText(allocator, children[0])) |col| {
                return .{ .func = .any_val, .column = col, .alias = alias };
            } else |_| {}
            return null;
        }
        // date_trunc('minute'/'hour'/'day', col) → render as text for planner's tryParseFnCallItem.
        // Previously mapped EventTime variants to synthetic "EventMinute"/"EventHour"/"EventDay"
        // names, but those don't exist in schema so the planner couldn't resolve them as keys.
        if (std.mem.eql(u8, fn_name, "date_trunc") and children.len == 2) {
            const fn_text_dt = try exprToText(allocator, val) orelse return null;
            return .{ .func = .column_ref, .column = fn_text_dt, .alias = alias };
        }
        // date_part('minute', EventTime) / extract(minute FROM EventTime)
        // → render as text "date_part('minute', EventTime)" for parseArithExpr
        // (falls through to exprToText fallback below)
        // Wrap-aggregate pattern: outerFn(groupUniqArray(col), ...) → group_uniq_array with post_fn
        // Handles: arraySlice, arrayDistinct, arrayFilter, arrayMap, arrayConcat,
        //          arrayFlatten, arrayExists, arrayReverse, arraySort, arrayUniq
        const wrap_agg_fns = [_][]const u8{
            "arrayslice", "arraydistinct", "arrayfilter", "arraymap",
            "arrayconcat", "arrayflatten", "arrayexists", "arrayreverse",
            "arraysort", "arrayuniq", "arrayreversesort", "arraymax", "arraymin",
        };
        const is_wrap = blk: {
            for (wrap_agg_fns) |wf| {
                if (std.mem.eql(u8, fn_name, wf)) break :blk true;
            }
            break :blk false;
        };
        if (is_wrap and children.len >= 1) {
            // Special case: arrayConcat(groupUniqArray(A), groupUniqArray(B)) →
            // collect all groupUniqArray children and combine into one via post_fn.
            if (std.mem.eql(u8, fn_name, "arrayconcat") and children.len >= 2) {
                // Check if ALL children are groupUniqArray calls
                var all_gua = true;
                for (children) |child| {
                    const child_obj = if (child == .object) child.object else null;
                    if (child_obj) |co| {
                        const cls = if (co.get("class")) |cv| cv.string else "";
                        const fname = if (co.get("function_name")) |fv| fv.string else "";
                        if (!std.mem.eql(u8, cls, "FUNCTION") or
                            (!std.mem.eql(u8, fname, "groupuniqarray") and !std.mem.eql(u8, fname, "grouparray")))
                        {
                            all_gua = false;
                            break;
                        }
                    } else { all_gua = false; break; }
                }
                if (all_gua) {
                    // Use the first groupUniqArray's column as the main agg,
                    // collect all columns for a combined array via concat.
                    // Build a "fake" concat column expression: each child's column name.
                    var concat_cols: std.ArrayList(u8) = .empty;
                    defer concat_cols.deinit(allocator);
                    var first_col: ?[]const u8 = null;
                    for (children, 0..) |child, ci| {
                        const fo = child.object;
                        const ic = if (fo.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                        if (ic.len == 1) {
                            const col_name = columnName(ic[0]) orelse continue;
                            if (ci == 0) {
                                first_col = col_name;
                            } else {
                                if (concat_cols.items.len > 0) try concat_cols.append(allocator, ',');
                                try concat_cols.appendSlice(allocator, col_name);
                            }
                        }
                    }
                    if (first_col) |fc| {
                        const col = try allocator.dupe(u8, fc);
                        // post_fn: arrayConcat($, groupUniqArray(col2), groupUniqArray(col3)...)
                        var post_buf: std.ArrayList(u8) = .empty;
                        try post_buf.appendSlice(allocator, "arrayconcat($");
                        // Add each extra col as groupUniqArray in post_fn text
                        // The executor will see these as column refs (passthru)
                        // Instead: just concat the columns arrays raw via \x0c separator
                        // Simplest: post_fn = arrayconcat($,<col2_expr>) and handle in evalTextExpr
                        for (children[1..]) |child| {
                            const fo = child.object;
                            const ic = if (fo.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                            if (ic.len == 1) {
                                if (columnName(ic[0])) |cn| {
                                    try post_buf.appendSlice(allocator, ",groupUniqArray(");
                                    try post_buf.appendSlice(allocator, cn);
                                    try post_buf.append(allocator, ')');
                                }
                            }
                        }
                        try post_buf.append(allocator, ')');
                        const post_fn = try post_buf.toOwnedSlice(allocator);
                        const out_alias: ?[]const u8 = if (alias != null) alias else
                            try exprToText(allocator, val) orelse try allocator.dupe(u8, col);
                        return .{ .func = .group_uniq_array, .column = col, .alias = out_alias, .post_fn = post_fn };
                    }
                }
            }

            const first_child = children[0];
            const first_obj = if (first_child == .object) first_child.object else null;
            if (first_obj) |fo| {
                const first_class = if (fo.get("class")) |cls| cls.string else "";
                // Pattern: outerFn(lambda, groupUniqArray(col)) — lambda-first form
                if (std.mem.eql(u8, first_class, "LAMBDA") and children.len >= 2) {
                    const second_child = children[1];
                    const second_obj = if (second_child == .object) second_child.object else null;
                    if (second_obj) |so| {
                        const second_class = if (so.get("class")) |cv| cv.string else "";
                        const second_fn = if (so.get("function_name")) |fv| fv.string else "";
                        if (std.mem.eql(u8, second_class, "FUNCTION") and
                            (std.mem.eql(u8, second_fn, "groupuniqarray") or
                             std.mem.eql(u8, second_fn, "grouparray")))
                        {
                            const inner_children = if (so.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                            if (inner_children.len == 1) {
                                const child_col = columnName(inner_children[0]) orelse {
                                    const fn_text2 = try exprToText(allocator, val) orelse return null;
                                    return .{ .func = .column_ref, .column = fn_text2, .alias = alias };
                                };
                                const col = try allocator.dupe(u8, child_col);
                                // Lambda text: "x -> body_expr"
                                const lambda_text = try exprToText(allocator, first_child) orelse "";
                                // post_fn: outerFn(lambda, $) where $ = aggregate result sentinel
                                var post_buf: std.ArrayList(u8) = .empty;
                                try post_buf.appendSlice(allocator, fn_name);
                                try post_buf.append(allocator, '(');
                                try post_buf.appendSlice(allocator, lambda_text);
                                try post_buf.appendSlice(allocator, ", $)");
                                allocator.free(lambda_text);
                                const post_fn = try post_buf.toOwnedSlice(allocator);
                                const out_alias: ?[]const u8 = if (alias != null) alias else
                                    try exprToText(allocator, val) orelse try allocator.dupe(u8, col);
                                return .{ .func = .group_uniq_array, .column = col, .alias = out_alias, .post_fn = post_fn };
                            }
                        }
                    }
                }
                if (std.mem.eql(u8, first_class, "FUNCTION")) {
                    const first_fn = if (fo.get("function_name")) |f| f.string else "";
                    if (std.mem.eql(u8, first_fn, "groupuniqarray") or
                        std.mem.eql(u8, first_fn, "grouparray"))
                    {
                        const inner_children = if (fo.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                        if (inner_children.len == 1) {
                            const child_col = columnName(inner_children[0]) orelse {
                                // Fall through to generic text fallback below
                                const fn_text2 = try exprToText(allocator, val) orelse return null;
                                return .{ .func = .column_ref, .column = fn_text2, .alias = alias };
                            };
                            const col = try allocator.dupe(u8, child_col);
                            // Build the post_fn template: outerFn($, arg2, arg3, ...)
                            // where $ is placeholder for the aggregate result.
                            var post_buf: std.ArrayList(u8) = .empty;
                            try post_buf.appendSlice(allocator, fn_name);
                            try post_buf.append(allocator, '(');
                            try post_buf.append(allocator, '$');
                            for (children[1..]) |extra_child| {
                                try post_buf.append(allocator, ',');
                                if (try exprToText(allocator, extra_child)) |et| {
                                    defer allocator.free(et);
                                    try post_buf.appendSlice(allocator, et);
                                }
                            }
                            try post_buf.append(allocator, ')');
                            const post_fn = try post_buf.toOwnedSlice(allocator);
                            // Use original expression text as alias for header (if no explicit alias)
                            const out_alias: ?[]const u8 = if (alias != null) alias else
                                try exprToText(allocator, val) orelse try allocator.dupe(u8, col);
                            return .{ .func = .group_uniq_array, .column = col, .alias = out_alias, .post_fn = post_fn };
                        }
                    }
                }
            }
        }
        // Fallback: render as text for executor passthrough.
        // But first: check if first_child is a wrap_agg function that itself wraps groupUniqArray.
        // This handles: arrayMax(arrayMap(x->x, groupUniqArray(col))) etc.
        if (is_wrap and children.len >= 1) {
            const fc = children[0];
            if (fc == .object) {
                const fc_obj = fc.object;
                const fc_class = if (fc_obj.get("class")) |cv| cv.string else "";
                const fc_fn = if (fc_obj.get("function_name")) |fv| std.ascii.lowerString(
                    @as([]u8, @constCast(fv.string)), fv.string) else "";
                _ = fc_class;
                // Check if fc_fn is a known wrap function
                const fc_is_wrap = blk2: {
                    for (wrap_agg_fns) |wf| {
                        if (std.mem.eql(u8, fc_fn, wf)) break :blk2 true;
                    }
                    break :blk2 false;
                };
                if (fc_is_wrap) {
                    const fc_children = if (fc_obj.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                    // Find groupUniqArray in fc_children (lambda-first or direct)
                    var gua_child_val: ?std.json.Value = null;
                    var gua_col_name: ?[]const u8 = null;
                    var lambda_text_opt: ?[]const u8 = null;
                    for (fc_children, 0..) |fcc, fci| {
                        if (fcc != .object) continue;
                        const fcc_obj = fcc.object;
                        const fcc_class = if (fcc_obj.get("class")) |cv| cv.string else "";
                        const fcc_fn = if (fcc_obj.get("function_name")) |fv| fv.string else "";
                        if (std.mem.eql(u8, fcc_class, "FUNCTION") and
                            (std.mem.eql(u8, fcc_fn, "groupuniqarray") or std.mem.eql(u8, fcc_fn, "grouparray")))
                        {
                            gua_child_val = fcc;
                            const gua_ic = if (fcc_obj.get("children")) |ch| ch.array.items else &[_]std.json.Value{};
                            if (gua_ic.len == 1) gua_col_name = columnName(gua_ic[0]);
                            // If prev child is a LAMBDA, capture it
                            if (fci > 0 and fc_children[fci - 1] == .object) {
                                const prev = fc_children[fci - 1].object;
                                if (std.mem.eql(u8, if (prev.get("class")) |cv| cv.string else "", "LAMBDA")) {
                                    lambda_text_opt = try exprToText(allocator, fc_children[fci - 1]);
                                }
                            }
                            break;
                        }
                    }
                    if (gua_col_name) |col_name| {
                        const col = try allocator.dupe(u8, col_name);
                        // Build inner post_fn: fc_fn(lambda?, $) or fc_fn($)
                        var inner_pf: std.ArrayList(u8) = .empty;
                        try inner_pf.appendSlice(allocator, fc_fn);
                        try inner_pf.append(allocator, '(');
                        if (lambda_text_opt) |lt| {
                            try inner_pf.appendSlice(allocator, lt);
                            allocator.free(lt);
                            try inner_pf.appendSlice(allocator, ", $)");
                        } else {
                            try inner_pf.appendSlice(allocator, "$)");
                        }
                        const inner_pf_text = try inner_pf.toOwnedSlice(allocator);
                        defer allocator.free(inner_pf_text);
                        // Build outer post_fn: fn_name(__AGG__)
                        // where __AGG__ is the result of inner_pf applied to the aggregate
                        // We compose: post_fn = "fn_name(inner_pf($))"
                        // Replace $ in inner_pf with __AGG__ to get the composed template
                        const composed = try std.fmt.allocPrint(allocator, "{s}({s})", .{fn_name, inner_pf_text});
                        const out_alias: ?[]const u8 = if (alias != null) alias else
                            try exprToText(allocator, val) orelse try allocator.dupe(u8, col);
                        return .{ .func = .group_uniq_array, .column = col, .alias = out_alias, .post_fn = composed };
                    }
                }
            }
        }
        const fn_text = try exprToText(allocator, val) orelse return null;
        return .{ .func = .column_ref, .column = fn_text, .alias = alias };
    }

    if (std.mem.eql(u8, class, "STAR")) {
        return .{ .func = .column_ref, .column = null, .alias = alias };
    }

    // ── CASE WHEN … THEN … ELSE … END ────────────────────────────────────────
    if (std.mem.eql(u8, class, "CASE")) {
        const checks = obj.get("case_checks").?.array.items;
        var when_texts = try allocator.alloc([]const u8, checks.len);
        var n_when: usize = 0;
        errdefer {
            for (when_texts[0..n_when]) |t| allocator.free(t);
            allocator.free(when_texts);
        }
        var then_texts = try allocator.alloc([]const u8, checks.len);
        var n_then: usize = 0;
        errdefer {
            for (then_texts[0..n_then]) |t| allocator.free(t);
            allocator.free(then_texts);
        }
        for (checks) |check| {
            const when_t = try exprToText(allocator, check.object.get("when_expr").?) orelse return null;
            when_texts[n_when] = when_t;
            n_when += 1;
            const then_t = try exprToText(allocator, check.object.get("then_expr").?) orelse return null;
            then_texts[n_then] = then_t;
            n_then += 1;
        }
        var else_text: ?[]const u8 = null;
        if (obj.get("else_expr")) |else_val| {
            if (else_val != .null) {
                else_text = try exprToText(allocator, else_val);
            }
        }
        const cwd = try allocator.create(generic_sql.CaseWhenData);
        cwd.* = .{ .when_texts = when_texts, .then_texts = then_texts, .else_text = else_text };
        return generic_sql.Expr{ .func = .case_when, .alias = alias, .case_when_data = cwd };
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
        const type_node = v.get("type").?.object;
        const type_id = type_node.get("id").?.string;
        const is_null = v.get("is_null").?.bool;
        if (is_null) return try allocator.dupe(u8, "NULL");
        const raw_val = v.get("value") orelse return null;
        if (std.mem.eql(u8, type_id, "VARCHAR") or
            std.mem.eql(u8, type_id, "DATE") or
            std.mem.eql(u8, type_id, "TIMESTAMP"))
        {
            return try std.fmt.allocPrint(allocator, "'{s}'", .{raw_val.string});
        }
        // DECIMAL: stored as integer scaled by 10^scale — reconstruct the decimal string
        if (std.mem.eql(u8, type_id, "DECIMAL")) {
            const raw_int: i64 = switch (raw_val) {
                .integer => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return null,
            };
            const scale: i64 = blk: {
                if (type_node.get("type_info")) |ti| {
                    if (ti == .object) {
                        if (ti.object.get("scale")) |sn| {
                            break :blk switch (sn) { .integer => |i| i, else => 0 };
                        }
                    }
                }
                break :blk 0;
            };
            if (scale <= 0) return try std.fmt.allocPrint(allocator, "{d}", .{raw_int});
            // Build e.g. "0.9" from raw_int=9, scale=1
            var divisor: f64 = 1.0;
            for (0..@intCast(scale)) |_| divisor *= 10.0;
            const fval: f64 = @as(f64, @floatFromInt(raw_int)) / divisor;
            // Always include decimal point so downstream type inference sees a float.
            if (fval == @trunc(fval) and @abs(fval) < 1e15) {
                return try std.fmt.allocPrint(allocator, "{d}.0", .{@as(i64, @intFromFloat(fval))});
            }
            return try std.fmt.allocPrint(allocator, "{d}", .{fval});
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

    // CAST(expr AS type)
    if (std.mem.eql(u8, class, "CAST")) {
        const child = try exprToText(allocator, obj.get("child") orelse return null) orelse return null;
        defer allocator.free(child);
        const cast_type_id = obj.get("cast_type").?.object.get("id").?.string;
        return try std.fmt.allocPrint(allocator, "CAST({s} AS {s})", .{ child, cast_type_id });
    }

    // LAMBDA: x -> expr  (used in arrayFilter, arrayMap, etc.)
    if (std.mem.eql(u8, class, "LAMBDA")) {
        const lhs_val = obj.get("lhs") orelse return null;
        const expr_val = obj.get("expr") orelse return null;
        const lhs_text = try exprToText(allocator, lhs_val) orelse return null;
        defer allocator.free(lhs_text);
        const expr_text = try exprToText(allocator, expr_val) orelse return null;
        defer allocator.free(expr_text);
        return try std.fmt.allocPrint(allocator, "{s} -> {s}", .{ lhs_text, expr_text });
    }

    if (std.mem.eql(u8, class, "COMPARISON")) {
        const op = comparisonOp(obj.get("type").?.string);
        const left = try exprToText(allocator, obj.get("left").?) orelse return null;
        defer allocator.free(left);
        const right = try exprToText(allocator, obj.get("right").?) orelse return null;
        defer allocator.free(right);
        return try std.fmt.allocPrint(allocator, "{s} {s} {s}", .{ left, op, right });
    }

    if (std.mem.eql(u8, class, "BETWEEN")) {
        const input = try exprToText(allocator, obj.get("input").?) orelse return null;
        defer allocator.free(input);
        const lower = try exprToText(allocator, obj.get("lower").?) orelse return null;
        defer allocator.free(lower);
        const upper = try exprToText(allocator, obj.get("upper").?) orelse return null;
        defer allocator.free(upper);
        return try std.fmt.allocPrint(allocator, "{s} BETWEEN {s} AND {s}", .{ input, lower, upper });
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
        // list_value() / list_value(a, b, ...) — DuckDB's internal name for array constructors [...]
        if (std.mem.eql(u8, fn_name, "list_value")) {
            if (children.len == 0) return try allocator.dupe(u8, "[]");
            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try buf.appendSlice(allocator, "[");
            for (children, 0..) |ch, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                const t = try exprToText(allocator, ch) orelse return null;
                defer allocator.free(t);
                try buf.appendSlice(allocator, t);
            }
            try buf.append(allocator, ']');
            return try buf.toOwnedSlice(allocator);
        }
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
                // Wrap sub-expressions that contain arithmetic operators to preserve evaluation
                // order. DuckDB strips parens in the AST, so we must re-add them for our
                // text-based evaluator which parses right-to-left.
                const l_needs = std.mem.indexOfAny(u8, l, "+-*/") != null;
                const r_needs = std.mem.indexOfAny(u8, r, "+-*/") != null;
                const lp: []const u8 = if (l_needs) "(" else "";
                const lq: []const u8 = if (l_needs) ")" else "";
                const rp: []const u8 = if (r_needs) "(" else "";
                const rq: []const u8 = if (r_needs) ")" else "";
                return try std.fmt.allocPrint(allocator, "{s}{s}{s} {s} {s}{s}{s}", .{ lp, l, lq, sym, rp, r, rq });
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
        if (std.mem.eql(u8, op_type, "ARRAY_EXTRACT") and children.len == 2) {
            const map_col = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(map_col);
            const key = try exprToText(allocator, children[1]) orelse return null;
            defer allocator.free(key);
            // Reconstruct as col['key'] (strip surrounding quotes from key if present)
            const key_inner = if (key.len >= 2 and key[0] == '\'' and key[key.len-1] == '\'')
                key[1..key.len-1] else key;
            return try std.fmt.allocPrint(allocator, "{s}['{s}']", .{ map_col, key_inner });
        }
        if (std.mem.eql(u8, op_type, "OPERATOR_IN") or std.mem.eql(u8, op_type, "OPERATOR_NOT_IN") or
            std.mem.eql(u8, op_type, "COMPARE_IN") or std.mem.eql(u8, op_type, "COMPARE_NOT_IN")) {
            const col = try exprToText(allocator, children[0]) orelse return null;
            defer allocator.free(col);
            const in_kw: []const u8 = if (std.mem.eql(u8, op_type, "OPERATOR_NOT_IN") or std.mem.eql(u8, op_type, "COMPARE_NOT_IN")) "NOT IN" else "IN";
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

/// Try to extract an epoch-millisecond integer from a node that may be:
///   - an integer literal (already epoch ms)
///   - a string literal "YYYY-MM-DD[ HH:MM:SS]" (date or datetime)
///   - toDateTime('YYYY-MM-DD HH:MM:SS') or toDate('YYYY-MM-DD') function call
/// Returns null if none of the above match.
fn epochMsFromNode(val: std.json.Value) ?i64 {
    if (intLiteralValue(val)) |iv| return iv;
    if (strLiteralValue(val)) |sv| return parseDateTimeStrMs(sv);
    const obj = val.object;
    const class = (obj.get("class") orelse return null).string;
    if (!std.mem.eql(u8, class, "FUNCTION")) return null;
    const fn_name = obj.get("function_name").?.string;
    if (!std.ascii.eqlIgnoreCase(fn_name, "todatetime") and
        !std.ascii.eqlIgnoreCase(fn_name, "todate")) return null;
    const children = obj.get("children").?.array.items;
    if (children.len == 0) return null;
    const sv = strLiteralValue(children[0]) orelse return null;
    return parseDateTimeStrMs(sv);
}

/// Parse "YYYY-MM-DD[ HH:MM:SS]" → epoch milliseconds, or null.
fn parseDateTimeStrMs(s: []const u8) ?i64 {
    if (s.len < 10 or s[4] != '-' or s[7] != '-') return null;
    const y  = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const mo = std.fmt.parseInt(u8,  s[5..7],  10) catch return null;
    const d  = std.fmt.parseInt(u8,  s[8..10], 10) catch return null;
    const days = dateToDaysLocal(y, mo, d);
    var h:   i64 = 0;
    var mi:  i64 = 0;
    var sec: i64 = 0;
    if (s.len >= 19 and s[10] == ' ' and s[13] == ':' and s[16] == ':') {
        h   = std.fmt.parseInt(i64, s[11..13], 10) catch 0;
        mi  = std.fmt.parseInt(i64, s[14..16], 10) catch 0;
        sec = std.fmt.parseInt(i64, s[17..19], 10) catch 0;
    }
    return (days * 86400 + h * 3600 + mi * 60 + sec) * 1000;
}

fn dateToDaysLocal(year: i32, month: u8, day: u8) i64 {
    var y = year;
    var m: i32 = month;
    if (m <= 2) { y -= 1; m += 12; }
    const A = @divFloor(y, 100);
    const B = 2 - A + @divFloor(A, 4);
    const jd: i64 = @as(i64, @intFromFloat(@floor(365.25 * @as(f64, @floatFromInt(y + 4716))))) +
                    @as(i64, @intFromFloat(@floor(30.6001 * @as(f64, @floatFromInt(m + 1))))) +
                    @as(i64, day) + B - 1524;
    return jd - 2440588;
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

/// Find the alias of the first projection whose exprToText matches `expr_text`.
/// Returns null if no match found.
fn findProjectionAliasByExprText(allocator: std.mem.Allocator, projs: []const std.json.Value, expr_text: []const u8) ?[]const u8 {
    for (projs) |pv| {
        const pobj = pv.object;
        // Get the expression part of the projection (without alias).
        const expr_val = pobj.get("expr") orelse pv;
        const ptxt = exprToText(allocator, expr_val) catch continue;
        defer if (ptxt) |t| allocator.free(t);
        if (ptxt) |t| {
            if (std.ascii.eqlIgnoreCase(t, expr_text)) {
                // Return the alias if present.
                if (pobj.get("alias")) |av| {
                    if (av == .string) return av.string;
                }
            }
        }
    }
    return null;
}

