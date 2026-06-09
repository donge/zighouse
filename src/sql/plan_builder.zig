/// AST → generic_sql.Plan translator.
///
/// Converts the output of src/sql/parser.zig into a generic_sql.Plan that
/// generic_executor.zig can execute.
///
/// Key mapping decisions (mirrors duckdb_parse.zig logic):
/// - Aggregate functions (count, sum, avg, min, max, countIf, minIf, maxIf,
///   uniqExact, groupUniqArray, arrayStringConcat) → typed AggregateFn
/// - length(col) → column_ref "length(col)"
/// - Everything else → column_ref with exprToText(expr) as column name
///   (the runtime text evaluator handles it)
/// - WHERE/HAVING typed predicates → WhereNode tree (fast path for int/str/like)
/// - WHERE/HAVING text → where_text / having_text (fallback text evaluator)

const std = @import("std");
const generic_sql = @import("../generic_sql.zig");
const sql_parser = @import("sql_parser");
const ast = sql_parser.ast;

const Allocator = std.mem.Allocator;
const Plan = generic_sql.Plan;
const Expr = generic_sql.Expr;
const AggregateFn = generic_sql.AggregateFn;
const CondExpr = generic_sql.CondExpr;
const WhereNode = generic_sql.WhereNode;
const CmpOp = generic_sql.CmpOp;
const CaseWhenData = generic_sql.CaseWhenData;

pub const BuildError = error{
    OutOfMemory,
    UnsupportedFeature,
};

/// Translate an ast.Stmt into a generic_sql.Plan.
/// The caller is responsible for calling generic_sql.deinit() on the result.
pub fn buildPlan(allocator: Allocator, stmt: *ast.Stmt, ctes: ?[]ast.Cte) BuildError!Plan {
    switch (stmt.*) {
        .select => |sel| return buildSelectPlan(allocator, sel, ctes),
        .union_all => |u| {
            const left_plan = try buildPlan(allocator, u.left, ctes);
            const right_plan = try allocator.create(Plan);
            right_plan.* = try buildPlan(allocator, u.right, ctes);
            var plan = left_plan;
            plan.union_other = right_plan;
            return plan;
        },
    }
}

fn buildSelectPlan(allocator: Allocator, sel: ast.SelectStmt, ctes_from_parent: ?[]ast.Cte) BuildError!Plan {
    // Merge CTEs from the statement itself and from parent (for subqueries)
    const all_ctes = sel.ctes;
    _ = ctes_from_parent; // CTEs are inlined at lookup time

    // Build projections
    var projs = std.ArrayListUnmanaged(Expr).empty;
    for (sel.projections) |proj| {
        const e = try buildExpr(allocator, proj.expr, proj.alias, all_ctes);
        try projs.append(allocator, e);
    }
    const projs_owned = try projs.toOwnedSlice(allocator);

    // FROM clause → table name + optional subquery_source
    var table: []const u8 = "__compute__";
    var subquery_source: ?*Plan = null;
    var numbers_count: ?u64 = null;
    var join_spec_out: ?*generic_sql.JoinSpec = null;

    if (sel.from) |from| {
        switch (from) {
            .table => |tr| {
                // Check if it's a CTE reference
                if (findCte(all_ctes, tr.name)) |cte_stmt| {
                    const sub = try allocator.create(Plan);
                    sub.* = try buildPlan(allocator, cte_stmt, all_ctes);
                    subquery_source = sub;
                    table = try allocator.dupe(u8, tr.name);
                } else {
                    table = try allocator.dupe(u8, tr.name);
                }
            },
            .subquery => |sq| {
                const sub = try allocator.create(Plan);
                sub.* = try buildPlan(allocator, sq.stmt, all_ctes);
                subquery_source = sub;
                table = try allocator.dupe(u8, sq.alias orelse "__subquery__");
            },
            .cte_ref => |name| {
                if (findCte(all_ctes, name)) |cte_stmt| {
                    const sub = try allocator.create(Plan);
                    sub.* = try buildPlan(allocator, cte_stmt, all_ctes);
                    subquery_source = sub;
                }
                table = try allocator.dupe(u8, name);
            },
            .numbers => |n| {
                numbers_count = if (n >= 0) @intCast(n) else null;
                table = try allocator.dupe(u8, "numbers");
            },
            .table_func => |tf| {
                if (std.ascii.eqlIgnoreCase(tf.name, "numbers") and tf.args.len == 1) {
                    if (tf.args[0] == .int) numbers_count = @intCast(tf.args[0].int);
                }
                table = try allocator.dupe(u8, tf.name);
            },
            .join => |jc| {
                var on_lefts: std.ArrayListUnmanaged([]const u8) = .empty;
                var on_rights: std.ArrayListUnmanaged([]const u8) = .empty;
                try extractEquiKeys(allocator, jc.on.*, &on_lefts, &on_rights);
                const left_sub = try buildFromClause(allocator, jc.left.*, all_ctes);
                const right_sub = try buildFromClause(allocator, jc.right.*, all_ctes);
                const left_ptr = try allocator.create(Plan);
                left_ptr.* = left_sub;
                const right_ptr = try allocator.create(Plan);
                right_ptr.* = right_sub;
                const jspec = try allocator.create(generic_sql.JoinSpec);
                jspec.* = .{
                    .kind     = @enumFromInt(@intFromEnum(jc.kind)),
                    .left     = left_ptr,
                    .right    = right_ptr,
                    .on_left  = try on_lefts.toOwnedSlice(allocator),
                    .on_right = try on_rights.toOwnedSlice(allocator),
                };
                join_spec_out = jspec;
                table = try allocator.dupe(u8, left_sub.table);
            },
        }
    } else {
        table = try allocator.dupe(u8, "__compute__");
        numbers_count = 1; // no FROM → synthesise a single row (like numbers(1))
    }

    // WHERE clause
    var where_expr: ?*WhereNode = null;
    var where_text: ?[]const u8 = null;
    if (sel.where) |we| {
        where_expr = buildWhereNode(allocator, we.*, all_ctes) catch null;
        where_text = try exprToText(allocator, we.*, all_ctes);
    }

    // GROUP BY
    var group_by: ?[]const u8 = null;
    if (sel.group_by.len > 0) {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        for (sel.group_by, 0..) |ge, i| {
            if (i > 0) try buf.appendSlice(allocator, ", ");
            const part = try exprToText(allocator, ge, all_ctes);
            try buf.appendSlice(allocator, part);
            allocator.free(part);
        }
        group_by = try buf.toOwnedSlice(allocator);
    }

    // HAVING
    var having_expr: ?*WhereNode = null;
    var having_text: ?[]const u8 = null;
    if (sel.having) |he| {
        having_expr = buildWhereNode(allocator, he.*, all_ctes) catch null;
        having_text = try exprToText(allocator, he.*, all_ctes);
    }

    // ORDER BY
    var order_by_count_desc = false;
    var order_by_alias: ?[]const u8 = null;
    var order_by_alias_asc = false;
    var order_by_text: ?[]const u8 = null;
    if (sel.order_by.len > 0) {
        // Build full order_by_text
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        for (sel.order_by, 0..) |oi, i| {
            if (i > 0) try buf.appendSlice(allocator, ", ");
            const part = try exprToText(allocator, oi.expr, all_ctes);
            try buf.appendSlice(allocator, part);
            allocator.free(part);
            if (oi.desc) {
                try buf.appendSlice(allocator, " DESC");
            } else {
                try buf.appendSlice(allocator, " ASC");
            }
        }
        order_by_text = try buf.toOwnedSlice(allocator);

        // Set fast-path aliases
        if (sel.order_by.len == 1) {
            const oi = sel.order_by[0];
            // Check if it's ORDER BY COUNT(*) / count() DESC
            const is_count_order = oi.expr == .func and std.mem.eql(u8, oi.expr.func.name, "count") and
                (oi.expr.func.args.len == 0 or
                 (oi.expr.func.args.len == 1 and oi.expr.func.args[0] == .star) or
                 (oi.expr.func.args.len == 1 and oi.expr.func.args[0] == .int));
            if (is_count_order) {
                order_by_count_desc = oi.desc;
            } else {
                // Use alias or column text; if the text matches a projection's
                // .column field, use that projection's alias so that planner can
                // resolve ORDER BY to the correct output column.
                const alias_text = try exprToText(allocator, oi.expr, all_ctes);
                var matched_alias: ?[]const u8 = null;
                for (projs_owned) |proj| {
                    if (proj.column != null and std.mem.eql(u8, proj.column.?, alias_text)) {
                        matched_alias = proj.alias orelse proj.column;
                        break;
                    }
                }
                if (matched_alias) |ma| {
                    // Dupe the alias so order_by_alias owns its string independently.
                    order_by_alias = try allocator.dupe(u8, ma);
                    // Only clear order_by_text when the alias differs from the expression
                    // text (e.g. ORDER BY date_trunc(...) → alias "M"): the executor uses
                    // the alias, and keeping order_by_text would be misleading.
                    // When they're the same (e.g. ORDER BY SearchPhrase → alias
                    // "SearchPhrase"), keep order_by_text so either path is usable.
                    if (!std.mem.eql(u8, ma, alias_text)) {
                        if (order_by_text) |obt| allocator.free(obt);
                        order_by_text = null;
                    }
                    allocator.free(alias_text);
                } else {
                    order_by_alias = alias_text;
                }
                order_by_alias_asc = !oi.desc;
            }
        }
    }

    // LIMIT / OFFSET
    const limit: ?usize = if (sel.limit) |l| @intCast(@max(0, l)) else null;
    const offset: ?usize = if (sel.offset) |o| @intCast(@max(0, o)) else null;

    // Derive legacy Filter from where_text (mirrors duckdb_parse.zig behaviour).
    const filter: ?generic_sql.Filter = if (where_text) |wt| generic_sql.parseFilter(wt) else null;

    return Plan{
        .table = table,
        .projections = projs_owned,
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
        .numbers_count = numbers_count,
        .distinct = sel.distinct,
        .join = join_spec_out,
        .owned = true,
    };
}

fn findCte(ctes: []ast.Cte, name: []const u8) ?*ast.Stmt {
    for (ctes) |cte| {
        if (std.ascii.eqlIgnoreCase(cte.name, name)) return cte.stmt;
    }
    return null;
}

/// Build a minimal Plan from a single FromClause (used for JOIN sub-trees).
/// For simple tables/CTEs only sets table; for join, recurses.
fn buildFromClause(allocator: Allocator, from: ast.FromClause, ctes: []ast.Cte) BuildError!Plan {
    switch (from) {
        .table => |tr| {
            var p = Plan{ .table = try allocator.dupe(u8, tr.name), .projections = &.{}, .owned = true };
            if (findCte(ctes, tr.name)) |cte_stmt| {
                const sub = try allocator.create(Plan);
                sub.* = try buildPlan(allocator, cte_stmt, ctes);
                p.subquery_source = sub;
            }
            return p;
        },
        .subquery => |sq| {
            const sub = try allocator.create(Plan);
            sub.* = try buildPlan(allocator, sq.stmt, ctes);
            return Plan{ .table = try allocator.dupe(u8, sq.alias orelse "__subquery__"), .projections = &.{}, .subquery_source = sub, .owned = true };
        },
        .cte_ref => |name| {
            var p = Plan{ .table = try allocator.dupe(u8, name), .projections = &.{}, .owned = true };
            if (findCte(ctes, name)) |cte_stmt| {
                const sub = try allocator.create(Plan);
                sub.* = try buildPlan(allocator, cte_stmt, ctes);
                p.subquery_source = sub;
            }
            return p;
        },
        .numbers => |n| {
            return Plan{ .table = try allocator.dupe(u8, "numbers"), .projections = &.{}, .numbers_count = if (n >= 0) @intCast(n) else null, .owned = true };
        },
        .table_func => |tf| {
            var nc: ?u64 = null;
            if (std.ascii.eqlIgnoreCase(tf.name, "numbers") and tf.args.len == 1) {
                if (tf.args[0] == .int) nc = @intCast(tf.args[0].int);
            }
            return Plan{ .table = try allocator.dupe(u8, tf.name), .projections = &.{}, .numbers_count = nc, .owned = true };
        },
        .join => |jc| {
            var on_lefts: std.ArrayListUnmanaged([]const u8) = .empty;
            var on_rights: std.ArrayListUnmanaged([]const u8) = .empty;
            try extractEquiKeys(allocator, jc.on.*, &on_lefts, &on_rights);
            const left_sub = try buildFromClause(allocator, jc.left.*, ctes);
            const right_sub = try buildFromClause(allocator, jc.right.*, ctes);
            const left_ptr = try allocator.create(Plan);
            left_ptr.* = left_sub;
            const right_ptr = try allocator.create(Plan);
            right_ptr.* = right_sub;
            const jspec = try allocator.create(generic_sql.JoinSpec);
            jspec.* = .{
                .kind     = @enumFromInt(@intFromEnum(jc.kind)),
                .left     = left_ptr,
                .right    = right_ptr,
                .on_left  = try on_lefts.toOwnedSlice(allocator),
                .on_right = try on_rights.toOwnedSlice(allocator),
            };
            return Plan{ .table = try allocator.dupe(u8, left_sub.table), .projections = &.{}, .join = jspec, .owned = true };
        },
    }
}

/// Walk ON expression and extract equi-join column name pairs.
fn extractEquiKeys(
    allocator: Allocator,
    expr: ast.Expr,
    lefts:  *std.ArrayListUnmanaged([]const u8),
    rights: *std.ArrayListUnmanaged([]const u8),
) BuildError!void {
    switch (expr) {
        .binop => |bo| {
            if (bo.op == .and_) {
                try extractEquiKeys(allocator, bo.left, lefts, rights);
                try extractEquiKeys(allocator, bo.right, lefts, rights);
                return;
            }
            if (bo.op == .eq and bo.left == .col and bo.right == .col) {
                try lefts.append(allocator, try allocator.dupe(u8, bo.left.col));
                try rights.append(allocator, try allocator.dupe(u8, bo.right.col));
                return;
            }
            return error.UnsupportedFeature;
        },
        else => return error.UnsupportedFeature,
    }
}

// ── Projection expression builder ─────────────────────────────────────────────

fn buildExpr(allocator: Allocator, e: ast.Expr, alias: ?[]const u8, ctes: []ast.Cte) BuildError!Expr {
    const alias_owned: ?[]const u8 = if (alias) |a| try allocator.dupe(u8, a) else null;

    switch (e) {
        .func => |f| return buildFuncExpr(allocator, f, alias_owned, ctes),
        .star => return Expr{ .func = .column_ref, .column = try allocator.dupe(u8, "*"), .alias = alias_owned },
        .int => |v| return Expr{ .func = .int_literal, .int_offset = v, .alias = alias_owned },
        .uint => |v| return Expr{ .func = .int_literal, .int_offset = @intCast(v), .alias = alias_owned },
        .float => |v| return Expr{ .func = .float_literal, .float_val = v, .alias = alias_owned },
        .case_when => |cw| return buildCaseWhenExpr(allocator, cw.*, alias_owned, ctes),
        else => {
            // Everything else → column_ref with text representation
            const text = try exprToText(allocator, e, ctes);
            return Expr{ .func = .column_ref, .column = text, .alias = alias_owned };
        },
    }
}

fn buildFuncExpr(allocator: Allocator, f: ast.FuncExpr, alias: ?[]const u8, ctes: []ast.Cte) BuildError!Expr {
    const name = f.name; // already lowercase

    // COUNT(*) / count() / count(1)
    if (std.mem.eql(u8, name, "count") and (f.args.len == 0 or
        (f.args.len == 1 and f.args[0] == .star) or
        (f.args.len == 1 and f.args[0] == .int))) {
        return Expr{ .func = .count_star, .alias = alias };
    }
    // COUNT(DISTINCT col)
    if (std.mem.eql(u8, name, "count") and f.distinct) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .count_distinct, .column = col, .alias = alias };
    }
    // SUM(col) or SUM(col + int_offset)
    if (std.mem.eql(u8, name, "sum") and f.args.len == 1) {
        const arg = f.args[0];
        // Detect col + constant pattern: SUM(col + 42) → .column = col, .int_offset = 42
        if (arg == .binop and arg.binop.op == .add and arg.binop.left == .col) {
            switch (arg.binop.right) {
                .int  => |v| return Expr{ .func = .sum, .column = try allocator.dupe(u8, arg.binop.left.col), .int_offset = v, .alias = alias },
                .uint => |v| return Expr{ .func = .sum, .column = try allocator.dupe(u8, arg.binop.left.col), .int_offset = @intCast(v), .alias = alias },
                else => {},
            }
        }
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .sum, .column = col, .alias = alias };
    }
    // AVG(col)
    if (std.mem.eql(u8, name, "avg") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .avg, .column = col, .alias = alias };
    }
    // MIN(col)
    if (std.mem.eql(u8, name, "min") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .min, .column = col, .alias = alias };
    }
    // MAX(col)
    if (std.mem.eql(u8, name, "max") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .max, .column = col, .alias = alias };
    }
    // countIf(cond_expr) → sum(if(cond, 1, 0))  — avoids pipeline changes
    if ((std.mem.eql(u8, name, "countif") or std.mem.eql(u8, name, "count_if")) and f.args.len == 1) {
        const cond_text = try exprToText(allocator, f.args[0], ctes);
        defer allocator.free(cond_text);
        const col = try std.fmt.allocPrint(allocator, "if({s}, 1, 0)", .{cond_text});
        return Expr{ .func = .sum, .column = col, .alias = alias };
    }
    // minIf(col, cond)
    if ((std.mem.eql(u8, name, "minif") or std.mem.eql(u8, name, "min_if")) and f.args.len == 2) {
        const col = try firstArgText(allocator, f.args, ctes);
        const cond = try buildCondExpr(allocator, f.args[1], ctes);
        const cond_ptr = try allocator.create(CondExpr);
        cond_ptr.* = cond;
        return Expr{ .func = .min_if, .column = col, .alias = alias, .cond = cond_ptr };
    }
    // maxIf(col, cond)
    if ((std.mem.eql(u8, name, "maxif") or std.mem.eql(u8, name, "max_if")) and f.args.len == 2) {
        const col = try firstArgText(allocator, f.args, ctes);
        const cond = try buildCondExpr(allocator, f.args[1], ctes);
        const cond_ptr = try allocator.create(CondExpr);
        cond_ptr.* = cond;
        return Expr{ .func = .max_if, .column = col, .alias = alias, .cond = cond_ptr };
    }
    // uniqExact(col) / uniqExactIf(col, cond)
    if (std.mem.eql(u8, name, "uniqexact") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .uniq_exact, .column = col, .alias = alias };
    }
    if ((std.mem.eql(u8, name, "uniqexactif") or std.mem.eql(u8, name, "uniq_exact_if")) and f.args.len == 2) {
        const col = try firstArgText(allocator, f.args, ctes);
        const cond = try buildCondExpr(allocator, f.args[1], ctes);
        const cond_ptr = try allocator.create(CondExpr);
        cond_ptr.* = cond;
        return Expr{ .func = .uniq_exact_if, .column = col, .alias = alias, .cond = cond_ptr };
    }
    // groupUniqArray(col)
    if ((std.mem.eql(u8, name, "groupuniqarray") or std.mem.eql(u8, name, "group_uniq_array")) and f.args.len >= 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .group_uniq_array, .column = col, .alias = alias };
    }
    // arrayStringConcat(arr, sep) — DuckDB maps to array_to_string
    if ((std.mem.eql(u8, name, "arraystringconcat") or std.mem.eql(u8, name, "array_to_string")) and f.args.len >= 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        const sep: ?[]const u8 = if (f.args.len >= 2) blk: {
            if (f.args[1] == .str) break :blk try allocator.dupe(u8, f.args[1].str);
            break :blk null;
        } else null;
        return Expr{ .func = .group_uniq_array, .column = col, .sep = sep, .alias = alias };
    }
    // sumArray(arr)
    if ((std.mem.eql(u8, name, "sumarray") or std.mem.eql(u8, name, "sum_array")) and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .sum_array, .column = col, .alias = alias };
    }
    // any(col)
    if (std.mem.eql(u8, name, "any") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .any_val, .column = col, .alias = alias };
    }
    // any_value(col) — DuckDB alias for any()
    if (std.mem.eql(u8, name, "any_value") and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .any_val, .column = col, .alias = alias };
    }

    // uniq(col) / uniqHLL12(col) → uniqExact (exact distinct; memory tradeoff accepted)
    if ((std.mem.eql(u8, name, "uniq") or
         std.mem.eql(u8, name, "uniqhll12") or
         std.mem.eql(u8, name, "uniq_hll12")) and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .uniq_exact, .column = col, .alias = alias };
    }
    // uniqHLL12(a,b,c) multi-arg → uniq_exact on concat(a,'|',b,'|',c)
    if ((std.mem.eql(u8, name, "uniqhll12") or
         std.mem.eql(u8, name, "uniq_hll12") or
         std.mem.eql(u8, name, "uniq")) and f.args.len >= 2) {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        try buf.appendSlice(allocator, "concat(");
        for (f.args, 0..) |arg, i| {
            if (i > 0) try buf.appendSlice(allocator, ",'|',");
            const t = try exprToText(allocator, arg, ctes);
            defer allocator.free(t);
            try buf.appendSlice(allocator, t);
        }
        try buf.append(allocator, ')');
        const col = try buf.toOwnedSlice(allocator);
        return Expr{ .func = .uniq_exact, .column = col, .alias = alias };
    }
    // uniqHLL12If(col,cond) → uniq_exact_if(col,cond)
    if ((std.mem.eql(u8, name, "uniqhll12if") or
         std.mem.eql(u8, name, "uniq_hll12_if") or
         std.mem.eql(u8, name, "uniqif")) and f.args.len == 2) {
        const col = try exprToText(allocator, f.args[0], ctes);
        const cond = try buildCondExpr(allocator, f.args[1], ctes);
        const cond_ptr = try allocator.create(CondExpr);
        cond_ptr.* = cond;
        return Expr{ .func = .uniq_exact_if, .column = col, .alias = alias, .cond = cond_ptr };
    }
    // anyLast(col) → any_val (first-value approximation)
    if ((std.mem.eql(u8, name, "anylast") or std.mem.eql(u8, name, "any_last")) and f.args.len == 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .any_val, .column = col, .alias = alias };
    }
    // groupArray(col) → group_uniq_array (deduplicating approximation)
    if ((std.mem.eql(u8, name, "grouparray") or std.mem.eql(u8, name, "group_array")) and f.args.len >= 1) {
        const col = try firstArgText(allocator, f.args, ctes);
        return Expr{ .func = .group_uniq_array, .column = col, .alias = alias };
    }
    // sumIf(col, cond) → sum(if(cond, col, 0))  — avoids pipeline changes
    if ((std.mem.eql(u8, name, "sumif") or std.mem.eql(u8, name, "sum_if")) and f.args.len == 2) {
        const col_text  = try exprToText(allocator, f.args[0], ctes);
        defer allocator.free(col_text);
        const cond_text = try exprToText(allocator, f.args[1], ctes);
        defer allocator.free(cond_text);
        const col = try std.fmt.allocPrint(allocator, "if({s}, {s}, 0)", .{ cond_text, col_text });
        return Expr{ .func = .sum, .column = col, .alias = alias };
    }

    // length(col) → column_ref "length(col)"
    if (std.mem.eql(u8, name, "length") and f.args.len == 1) {
        const inner = try exprToText(allocator, f.args[0], ctes);
        defer allocator.free(inner);
        const text = try std.fmt.allocPrint(allocator, "length({s})", .{inner});
        return Expr{ .func = .column_ref, .column = text, .alias = alias };
    }

    // if(cond, then, else) → case_when
    if (std.mem.eql(u8, name, "if") and f.args.len == 3) {
        const when_texts = try allocator.alloc([]const u8, 1);
        const then_texts = try allocator.alloc([]const u8, 1);
        when_texts[0] = try exprToText(allocator, f.args[0], ctes);
        then_texts[0] = try exprToText(allocator, f.args[1], ctes);
        const else_text: ?[]const u8 = try exprToText(allocator, f.args[2], ctes);
        const cwd = try allocator.create(CaseWhenData);
        cwd.* = .{ .when_texts = when_texts, .then_texts = then_texts, .else_text = else_text };
        return Expr{ .func = .case_when, .case_when_data = cwd, .alias = alias };
    }

    // date_part('unit', col) → ClickHouse time-unit column names
    if (std.mem.eql(u8, name, "date_part") and f.args.len == 2 and f.args[0] == .str and f.args[1] == .col) {
        const unit = f.args[0].str;
        const col_name = f.args[1].col;
        const mapped: ?[]const u8 = blk: {
            if (std.ascii.eqlIgnoreCase(col_name, "EventTime")) {
                if (std.ascii.eqlIgnoreCase(unit, "minute")) break :blk "EventMinuteOfHour";
                if (std.ascii.eqlIgnoreCase(unit, "hour"))   break :blk "EventHour";
                if (std.ascii.eqlIgnoreCase(unit, "day"))    break :blk "EventDate";
            }
            break :blk null;
        };
        if (mapped) |m| {
            return Expr{ .func = .column_ref, .column = try allocator.dupe(u8, m), .alias = alias };
        }
    }

    // Everything else: render as text → column_ref
    const text = try exprToText(allocator, .{ .func = f }, ctes);
    return Expr{ .func = .column_ref, .column = text, .alias = alias };
}

fn buildCaseWhenExpr(allocator: Allocator, cw: ast.CaseExpr, alias: ?[]const u8, ctes: []ast.Cte) BuildError!Expr {
    var when_texts = try allocator.alloc([]const u8, cw.whens.len);
    var then_texts = try allocator.alloc([]const u8, cw.whens.len);
    for (cw.whens, 0..) |wc, i| {
        when_texts[i] = try exprToText(allocator, wc.cond, ctes);
        then_texts[i] = try exprToText(allocator, wc.then, ctes);
    }
    const else_text: ?[]const u8 = if (cw.else_) |el| try exprToText(allocator, el.*, ctes) else null;
    const cwd = try allocator.create(CaseWhenData);
    cwd.* = .{ .when_texts = when_texts, .then_texts = then_texts, .else_text = else_text };
    return Expr{ .func = .case_when, .alias = alias, .case_when_data = cwd };
}

fn buildCondExpr(allocator: Allocator, e: ast.Expr, ctes: []ast.Cte) BuildError!CondExpr {
    // Try to build a typed CondExpr (col op val)
    if (e == .binop) {
        const bo = e.binop;
        const op = switch (bo.op) {
            .eq  => generic_sql.CmpOp.eq,
            .neq => generic_sql.CmpOp.ne,
            .lt  => generic_sql.CmpOp.lt,
            .lte => generic_sql.CmpOp.le,
            .gt  => generic_sql.CmpOp.gt,
            .gte => generic_sql.CmpOp.ge,
            else => {
                // Complex — store as text
                const text = try exprToText(allocator, e, ctes);
                return CondExpr{ .cond_text = text };
            },
        };
        if (bo.left == .col) {
            const col = try allocator.dupe(u8, bo.left.col);
            switch (bo.right) {
                .int  => |v| return CondExpr{ .cond_col = col, .cond_op = op, .cond_num = @floatFromInt(v) },
                .uint => |v| return CondExpr{ .cond_col = col, .cond_op = op, .cond_num = @floatFromInt(v) },
                .float => |v| return CondExpr{ .cond_col = col, .cond_op = op, .cond_num = v },
                .str  => |v| {
                    const s = try allocator.dupe(u8, v);
                    return CondExpr{ .cond_col = col, .cond_op = op, .cond_str = s };
                },
                else => {
                    allocator.free(col);
                    const text = try exprToText(allocator, e, ctes);
                    return CondExpr{ .cond_text = text };
                },
            }
        }
    }
    // Complex — store as text
    const text = try exprToText(allocator, e, ctes);
    return CondExpr{ .cond_text = text };
}

fn firstArgText(allocator: Allocator, args: []ast.Expr, ctes: []ast.Cte) BuildError![]const u8 {
    if (args.len == 0) return allocator.dupe(u8, "");
    return exprToText(allocator, args[0], ctes);
}

// ── WhereNode builder ─────────────────────────────────────────────────────────

fn buildWhereNode(allocator: Allocator, e: ast.Expr, ctes: []ast.Cte) BuildError!*WhereNode {
    const node = try allocator.create(WhereNode);
    errdefer allocator.destroy(node);

    switch (e) {
        .binop => |bo| {
            switch (bo.op) {
                .and_ => {
                    // Flatten nested AND into children list
                    var children: std.ArrayListUnmanaged(*WhereNode) = .empty;
                    try flattenAndOr(allocator, e, .and_, &children, ctes);
                    const owned = try children.toOwnedSlice(allocator);
                    node.* = .{ .and_ = owned };
                    return node;
                },
                .or_ => {
                    var children: std.ArrayListUnmanaged(*WhereNode) = .empty;
                    try flattenAndOr(allocator, e, .or_, &children, ctes);
                    const owned = try children.toOwnedSlice(allocator);
                    node.* = .{ .or_ = owned };
                    return node;
                },
                .eq, .neq, .lt, .lte, .gt, .gte => {
                    const cmp_op = switch (bo.op) {
                        .eq  => CmpOp.eq,
                        .neq => CmpOp.ne,
                        .lt  => CmpOp.lt,
                        .lte => CmpOp.le,
                        .gt  => CmpOp.gt,
                        .gte => CmpOp.ge,
                        else => unreachable,
                    };
                    // Resolve LHS: plain column or any expression rendered as text.
                    const col: []const u8 = if (bo.left == .col)
                        try allocator.dupe(u8, bo.left.col)
                    else
                        exprToText(allocator, bo.left, ctes) catch return error.UnsupportedFeature;
                    switch (bo.right) {
                        .int  => |v| { node.* = .{ .cmp_int = .{ .col = col, .op = cmp_op, .val = v } }; return node; },
                        .uint => |v| { node.* = .{ .cmp_int = .{ .col = col, .op = cmp_op, .val = @intCast(v) } }; return node; },
                        .str  => |v| {
                            const val = try allocator.dupe(u8, v);
                            node.* = .{ .cmp_str = .{ .col = col, .op = cmp_op, .val = val } };
                            return node;
                        },
                        else => { allocator.free(col); },
                    }
                    return error.UnsupportedFeature;
                },
                .like => {
                    if (bo.left == .col and bo.right == .str) {
                        const col = try allocator.dupe(u8, bo.left.col);
                        const pat = try allocator.dupe(u8, bo.right.str);
                        node.* = .{ .like = .{ .col = col, .op = .like, .pattern = pat } };
                        return node;
                    }
                    return error.UnsupportedFeature;
                },
                .not_like => {
                    if (bo.left == .col and bo.right == .str) {
                        const col = try allocator.dupe(u8, bo.left.col);
                        const pat = try allocator.dupe(u8, bo.right.str);
                        node.* = .{ .like = .{ .col = col, .op = .not_like, .pattern = pat } };
                        return node;
                    }
                    return error.UnsupportedFeature;
                },
                else => return error.UnsupportedFeature,
            }
        },
        .is_null => |isn| {
            const col = try allocator.dupe(u8, (switch (isn.val) { .col => |c| c, else => return error.UnsupportedFeature }));
            node.* = if (isn.is_not) .{ .is_not_null = col } else .{ .is_null = col };
            return node;
        },
        .in_list => |il| {
            // col IN (v1, v2, ...) → OR(col=v1, col=v2, ...)
            // Only handle the case where LHS is a simple column and all items are integer literals.
            if (il.lhs != .col) return error.UnsupportedFeature;
            if (il.items.len == 0) return error.UnsupportedFeature;

            // Build one cmp_int WhereNode per item (each owns its own copy of col_name).
            const cmp_op: CmpOp = if (il.negated) .ne else .eq;
            var children: std.ArrayListUnmanaged(*WhereNode) = .empty;
            errdefer {
                for (children.items) |ch| generic_sql.freeWhereNode(allocator, ch);
                children.deinit(allocator);
            }
            for (il.items) |item| {
                const val: i64 = switch (item) {
                    .int  => |v| v,
                    .uint => |v| @intCast(v),
                    else  => return error.UnsupportedFeature,
                };
                const col_copy = try allocator.dupe(u8, il.lhs.col);
                errdefer allocator.free(col_copy);
                const ch = try allocator.create(WhereNode);
                ch.* = .{ .cmp_int = .{ .col = col_copy, .op = cmp_op, .val = val } };
                try children.append(allocator, ch);
            }

            if (children.items.len == 1) {
                // Single value: just return the cmp_int directly.
                node.* = children.items[0].*;
                allocator.destroy(children.items[0]);
                children.deinit(allocator);
            } else if (il.negated) {
                // NOT IN → AND(col!=v1, col!=v2, ...)
                const owned = try children.toOwnedSlice(allocator);
                node.* = .{ .and_ = owned };
            } else {
                // IN → OR(col=v1, col=v2, ...)
                const owned = try children.toOwnedSlice(allocator);
                node.* = .{ .or_ = owned };
            }
            return node;
        },
        else => return error.UnsupportedFeature,
    }
}

fn flattenAndOr(allocator: Allocator, e: ast.Expr, op: enum { and_, or_ }, out: *std.ArrayListUnmanaged(*WhereNode), ctes: []ast.Cte) BuildError!void {
    if (e == .binop) {
        const bo = e.binop;
        const is_same = switch (op) {
            .and_ => bo.op == .and_,
            .or_  => bo.op == .or_,
        };
        if (is_same) {
            try flattenAndOr(allocator, bo.left, op, out, ctes);
            try flattenAndOr(allocator, bo.right, op, out, ctes);
            return;
        }
    }
    const child = buildWhereNode(allocator, e, ctes) catch return;
    try out.append(allocator, child);
}

// ── Expression → SQL text renderer ───────────────────────────────────────────

/// Render an ast.Expr back to SQL text for the runtime text evaluator.
/// Caller owns the returned string.
pub fn exprToText(allocator: Allocator, e: ast.Expr, ctes: []ast.Cte) BuildError![]const u8 {
    switch (e) {
        .col => |s| return allocator.dupe(u8, s),
        .star => return allocator.dupe(u8, "*"),
        .int => |v| return std.fmt.allocPrint(allocator, "{d}", .{v}),
        .uint => |v| return std.fmt.allocPrint(allocator, "{d}", .{v}),
        .float => |v| return std.fmt.allocPrint(allocator, "{d}", .{v}),
        .null_lit => return allocator.dupe(u8, "NULL"),
        .bool_lit => |b| return allocator.dupe(u8, if (b) "1" else "0"),
        .str => |s| {
            // Re-add surrounding single quotes and escape any internal single quotes
            var out: std.ArrayListUnmanaged(u8) = .empty;
            try out.append(allocator, '\'');
            for (s) |c| {
                if (c == '\'') try out.append(allocator, '\'');
                try out.append(allocator, c);
            }
            try out.append(allocator, '\'');
            return out.toOwnedSlice(allocator);
        },
        .array => |items| {
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            try buf.append(allocator, '[');
            for (items, 0..) |item, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                const s = try exprToText(allocator, item, ctes);
                try buf.appendSlice(allocator, s);
                allocator.free(s);
            }
            try buf.append(allocator, ']');
            return buf.toOwnedSlice(allocator);
        },
        .func => |f| {
            // count(*) / count() / count(1) → always render as count(*)
            if (std.mem.eql(u8, f.name, "count") and (f.args.len == 0 or
                (f.args.len == 1 and f.args[0] == .star) or
                (f.args.len == 1 and f.args[0] == .int))) {
                return allocator.dupe(u8, "count(*)");
            }
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            try buf.appendSlice(allocator, f.name);
            try buf.append(allocator, '(');
            if (f.distinct) try buf.appendSlice(allocator, "DISTINCT ");
            for (f.args, 0..) |arg, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                const s = try exprToText(allocator, arg, ctes);
                try buf.appendSlice(allocator, s);
                allocator.free(s);
            }
            try buf.append(allocator, ')');
            return buf.toOwnedSlice(allocator);
        },
        .binop => |bo| {
            const ltext = try exprToText(allocator, bo.left, ctes);
            defer allocator.free(ltext);
            const rtext = try exprToText(allocator, bo.right, ctes);
            defer allocator.free(rtext);
            const op_str: []const u8 = switch (bo.op) {
                .eq     => "=",
                .neq    => "<>",
                .lt     => "<",
                .lte    => "<=",
                .gt     => ">",
                .gte    => ">=",
                .and_   => "AND",
                .or_    => "OR",
                .add    => "+",
                .sub    => "-",
                .mul    => "*",
                .div    => "/",
                .mod    => "%",
                .concat => "||",
                .like   => "LIKE",
                .not_like => "NOT LIKE",
            };
            // Only arithmetic operators need parentheses for precedence.
            // Comparison and logical operators are rendered without parens so
            // that where_text can be passed to parseFilter correctly.
            return switch (bo.op) {
                .add, .sub, .mul, .div, .mod, .concat =>
                    std.fmt.allocPrint(allocator, "({s} {s} {s})", .{ ltext, op_str, rtext }),
                else =>
                    std.fmt.allocPrint(allocator, "{s} {s} {s}", .{ ltext, op_str, rtext }),
            };
        },
        .not => |inner| {
            const s = try exprToText(allocator, inner.*, ctes);
            defer allocator.free(s);
            return std.fmt.allocPrint(allocator, "NOT ({s})", .{s});
        },
        .neg => |inner| {
            const s = try exprToText(allocator, inner.*, ctes);
            defer allocator.free(s);
            return std.fmt.allocPrint(allocator, "-({s})", .{s});
        },
        .in_list => |il| {
            const lhs = try exprToText(allocator, il.lhs, ctes);
            defer allocator.free(lhs);
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            try buf.appendSlice(allocator, lhs);
            if (il.negated) try buf.appendSlice(allocator, " NOT IN (") else try buf.appendSlice(allocator, " IN (");
            for (il.items, 0..) |item, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                const s = try exprToText(allocator, item, ctes);
                try buf.appendSlice(allocator, s);
                allocator.free(s);
            }
            try buf.append(allocator, ')');
            return buf.toOwnedSlice(allocator);
        },
        .in_subq => |isq| {
            // For NOT IN (subquery), we need to render the subquery as text
            const lhs = try exprToText(allocator, isq.lhs, ctes);
            defer allocator.free(lhs);
            // Render inner SELECT
            const sub_text = try stmtToText(allocator, isq.query, ctes);
            defer allocator.free(sub_text);
            const not_str: []const u8 = if (isq.negated) " NOT IN (" else " IN (";
            return std.fmt.allocPrint(allocator, "{s}{s}{s})", .{ lhs, not_str, sub_text });
        },
        .between => |b| {
            const val = try exprToText(allocator, b.val, ctes);
            defer allocator.free(val);
            const lo = try exprToText(allocator, b.lo, ctes);
            defer allocator.free(lo);
            const hi = try exprToText(allocator, b.hi, ctes);
            defer allocator.free(hi);
            if (b.negated) {
                return std.fmt.allocPrint(allocator, "{s} NOT BETWEEN {s} AND {s}", .{ val, lo, hi });
            } else {
                return std.fmt.allocPrint(allocator, "{s} BETWEEN {s} AND {s}", .{ val, lo, hi });
            }
        },
        .is_null => |isn| {
            const val = try exprToText(allocator, isn.val, ctes);
            defer allocator.free(val);
            if (isn.is_not) {
                return std.fmt.allocPrint(allocator, "{s} IS NOT NULL", .{val});
            } else {
                return std.fmt.allocPrint(allocator, "{s} IS NULL", .{val});
            }
        },
        .case_when => |cw| {
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            try buf.appendSlice(allocator, "CASE");
            if (cw.input) |inp| {
                const s = try exprToText(allocator, inp.*, ctes);
                defer allocator.free(s);
                try buf.append(allocator, ' ');
                try buf.appendSlice(allocator, s);
            }
            for (cw.whens) |wc| {
                const cond_s = try exprToText(allocator, wc.cond, ctes);
                defer allocator.free(cond_s);
                const then_s = try exprToText(allocator, wc.then, ctes);
                defer allocator.free(then_s);
                try buf.appendSlice(allocator, " WHEN ");
                try buf.appendSlice(allocator, cond_s);
                try buf.appendSlice(allocator, " THEN ");
                try buf.appendSlice(allocator, then_s);
            }
            if (cw.else_) |el| {
                const s = try exprToText(allocator, el.*, ctes);
                defer allocator.free(s);
                try buf.appendSlice(allocator, " ELSE ");
                try buf.appendSlice(allocator, s);
            }
            try buf.appendSlice(allocator, " END");
            return buf.toOwnedSlice(allocator);
        },
        .subscript => |sub| {
            const base = try exprToText(allocator, sub.base, ctes);
            defer allocator.free(base);
            const idx = try exprToText(allocator, sub.index, ctes);
            defer allocator.free(idx);
            return std.fmt.allocPrint(allocator, "{s}[{s}]", .{ base, idx });
        },
        .subquery => |sq| {
            return stmtToText(allocator, sq, ctes);
        },
        .cast => |c| {
            const val = try exprToText(allocator, c.val, ctes);
            defer allocator.free(val);
            return std.fmt.allocPrint(allocator, "CAST({s} AS {s})", .{ val, c.type_name });
        },
        .lambda => |lam| {
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            if (lam.params.len == 1) {
                try buf.appendSlice(allocator, lam.params[0]);
            } else {
                try buf.append(allocator, '(');
                for (lam.params, 0..) |p, i| {
                    if (i > 0) try buf.appendSlice(allocator, ", ");
                    try buf.appendSlice(allocator, p);
                }
                try buf.append(allocator, ')');
            }
            try buf.appendSlice(allocator, " -> ");
            const body = try exprToText(allocator, lam.body, ctes);
            defer allocator.free(body);
            try buf.appendSlice(allocator, body);
            return buf.toOwnedSlice(allocator);
        },
        .raw_text => |s| return allocator.dupe(u8, s),
    }
}

/// Render a statement as SQL text (for subquery embedding in WHERE text).
fn stmtToText(allocator: Allocator, stmt: *ast.Stmt, ctes: []ast.Cte) BuildError![]const u8 {
    switch (stmt.*) {
        .select => |sel| {
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            try buf.appendSlice(allocator, "SELECT ");
            for (sel.projections, 0..) |proj, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                const s = try exprToText(allocator, proj.expr, ctes);
                defer allocator.free(s);
                try buf.appendSlice(allocator, s);
                if (proj.alias) |a| {
                    try buf.appendSlice(allocator, " AS ");
                    try buf.appendSlice(allocator, a);
                }
            }
            if (sel.from) |from| {
                try buf.appendSlice(allocator, " FROM ");
                switch (from) {
                    .table => |tr| try buf.appendSlice(allocator, tr.name),
                    .numbers => try buf.appendSlice(allocator, "numbers"),
                    else => try buf.appendSlice(allocator, "__from__"),
                }
            }
            if (sel.where) |we| {
                const wtext = try exprToText(allocator, we.*, ctes);
                defer allocator.free(wtext);
                try buf.appendSlice(allocator, " WHERE ");
                try buf.appendSlice(allocator, wtext);
            }
            return buf.toOwnedSlice(allocator);
        },
        .union_all => return allocator.dupe(u8, "/* UNION */"),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test {
    _ = @import("plan_builder_test.zig");
}
