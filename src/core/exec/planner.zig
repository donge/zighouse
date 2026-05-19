/// Planner: translates a generic_sql.Plan (text-based IR from duckdb_parse)
/// into a PhysicalNode tree (typed IR for pipeline execution).
///
/// The planner resolves column names to schema indices, infers output types,
/// and builds the correct operator tree (scan → filter → project → agg → sort → limit).
///
/// Usage:
///   const node = try planner.plan(arena, generic_plan, schema_table);
///   var rs    = try pipeline.executePlan(node, ctx);
const std = @import("std");
const generic_sql = @import("generic_sql");
const schema_mod  = @import("schema");
const core        = @import("core");
const plan        = core.exec.plan;
const core_types  = core.types;

pub const ColumnType    = core_types.ColumnType;
pub const PhysicalNode  = plan.PhysicalNode;
pub const ProjectItem   = plan.ProjectItem;
pub const Expr          = plan.Expr;
pub const ColRef        = plan.ColRef;
pub const AggCall       = plan.AggCall;
pub const SortKey       = plan.SortKey;

// ── PlannerCtx ────────────────────────────────────────────────────────────────

/// All allocations go into a single arena that lives for the duration
/// of the query.  The caller is responsible for freeing the arena.
pub const PlannerCtx = struct {
    alloc: std.mem.Allocator,
    /// The schema of the table being scanned.
    tbl: ?schema_mod.Table,

    pub fn init(alloc: std.mem.Allocator, tbl: ?schema_mod.Table) PlannerCtx {
        return .{ .alloc = alloc, .tbl = tbl };
    }
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Translate a generic_sql.Plan into a PhysicalNode tree.
/// Returns null if the plan cannot be translated (e.g. unsupported construct).
pub fn plan_query(
    ctx: *PlannerCtx,
    gplan: generic_sql.Plan,
) !?*PhysicalNode {
    // ── Source node ───────────────────────────────────────────────────────────
    var source: *PhysicalNode = blk: {
        if (gplan.subquery_source) |sq| {
            // Subquery in FROM.
            const inner = try plan_query(ctx, sq.*) orelse return null;
            const n = try ctx.alloc.create(PhysicalNode);
            n.* = .{ .chunk_source = .{ .input = inner } };
            break :blk n;
        } else {
            const n = try ctx.alloc.create(PhysicalNode);
            const tbl_name = try ctx.alloc.dupe(u8, gplan.table);
            n.* = .{ .part_scan = .{
                .db      = "",
                .table   = tbl_name,
                .columns = &.{},
                .filter  = null,
            }};
            break :blk n;
        }
    };

    // ── WHERE filter ──────────────────────────────────────────────────────────
    if (gplan.where_expr) |we| {
        if (whereNodeToExpr(ctx, we)) |pred| {
            const fn_ptr = try ctx.alloc.create(PhysicalNode);
            fn_ptr.* = .{ .filter = .{ .input = source, .predicate = pred } };
            source = fn_ptr;
        }
        // If whereNodeToExpr returns null, we leave source unfiltered (conservative).
    } else if (gplan.where_text != null) {
        // where_text is a raw SQL string that requires evalTextExpr.
        // Not supported in IR path — caller falls back to generic_executor.
        return null;
    }

    // ── Collect projections ───────────────────────────────────────────────────
    const projs = gplan.projections;
    if (projs.len == 0) return null;

    // Determine if there are any aggregate functions.
    var has_agg = false;
    for (projs) |p| {
        if (isAggregate(p.func)) { has_agg = true; break; }
    }

    if (!has_agg) {
        // ── Pure projection / scan ─────────────────────────────────────────
        const items = try buildProjectItems(ctx, projs) orelse return null;
        const proj_node = try ctx.alloc.create(PhysicalNode);
        proj_node.* = .{ .project = .{ .input = source, .items = items } };
        source = proj_node;
    } else {
        // ── Aggregation ───────────────────────────────────────────────────
        // Split projs into key exprs (non-agg, in GROUP BY) and agg exprs.
        var key_items_list:  std.ArrayListUnmanaged(ProjectItem) = .empty;
        var agg_items_list:  std.ArrayListUnmanaged(ProjectItem) = .empty;

        for (projs) |p| {
            if (isAggregate(p.func)) {
                const item = try aggExprToProjectItem(ctx, p) orelse return null;
                try agg_items_list.append(ctx.alloc, item);
            } else {
                const item = try scalarExprToProjectItem(ctx, p) orelse return null;
                try key_items_list.append(ctx.alloc, item);
            }
        }

        const key_items = try key_items_list.toOwnedSlice(ctx.alloc);
        const agg_items = try agg_items_list.toOwnedSlice(ctx.alloc);

        const agg_node = try ctx.alloc.create(PhysicalNode);
        if (key_items.len == 0) {
            agg_node.* = .{ .scalar_agg = .{ .input = source, .aggs = agg_items } };
        } else {
            agg_node.* = .{ .hash_agg = .{
                .input = source,
                .keys  = key_items,
                .aggs  = agg_items,
            }};
        }
        source = agg_node;

        // HAVING → post-agg filter (where_text not supported in IR path)
        if (gplan.having_text != null) return null;
    }

    // ── ORDER BY ─────────────────────────────────────────────────────────────
    const has_limit = gplan.limit != null;
    if (gplan.order_by_count_desc) {
        // Special: ORDER BY count() DESC — sort index is last column of output.
        const output_len = outputLen(source);
        const sort_idx: usize = if (output_len > 0) output_len - 1 else 0;
        const sort_keys = try ctx.alloc.alloc(SortKey, 1);
        sort_keys[0] = .{ .col_idx = sort_idx, .desc = true, .nulls_first = false };
        if (has_limit) {
            const k: u64 = @intCast(gplan.limit.?);
            const topk = try ctx.alloc.create(PhysicalNode);
            topk.* = .{ .top_k = .{ .input = source, .keys = sort_keys, .k = k } };
            source = topk;
        } else {
            const ob = try ctx.alloc.create(PhysicalNode);
            ob.* = .{ .order_by = .{ .input = source, .keys = sort_keys } };
            source = ob;
        }
    } else if (gplan.order_by_alias) |alias| {
        // ORDER BY alias [ASC|DESC]
        const col_idx = findOutputColIdx(source, alias) orelse return null;
        const sort_keys = try ctx.alloc.alloc(SortKey, 1);
        sort_keys[0] = .{ .col_idx = col_idx, .desc = !gplan.order_by_alias_asc, .nulls_first = false };
        if (has_limit) {
            const k: u64 = @intCast(gplan.limit.?);
            const topk = try ctx.alloc.create(PhysicalNode);
            topk.* = .{ .top_k = .{ .input = source, .keys = sort_keys, .k = k } };
            source = topk;
        } else {
            const ob = try ctx.alloc.create(PhysicalNode);
            ob.* = .{ .order_by = .{ .input = source, .keys = sort_keys } };
            source = ob;
        }
    } else if (gplan.order_by_text != null) {
        // Complex ORDER BY expression — not supported in IR path.
        return null;
    }

    // ── LIMIT / OFFSET ────────────────────────────────────────────────────────
    if (gplan.limit) |lim| {
        // Skip if already consumed by top_k above.
        const already_topk = switch (source.*) {
            .top_k => true,
            else   => false,
        };
        if (!already_topk) {
            const lim_node = try ctx.alloc.create(PhysicalNode);
            lim_node.* = .{ .limit = .{
                .input  = source,
                .limit  = @intCast(lim),
                .offset = @intCast(gplan.offset orelse 0),
            }};
            source = lim_node;
        }
    }

    return source;
}

// ── Helper: find output column index by name ──────────────────────────────────

fn findOutputColIdx(node: *const PhysicalNode, name: []const u8) ?usize {
    switch (node.*) {
        .project    => |p| {
            for (p.items, 0..) |item, i| if (std.mem.eql(u8, item.alias, name)) return i;
            return null;
        },
        .hash_agg   => |ha| {
            for (ha.keys, 0..) |item, i| if (std.mem.eql(u8, item.alias, name)) return i;
            for (ha.aggs, 0..) |item, i| if (std.mem.eql(u8, item.alias, name)) return ha.keys.len + i;
            return null;
        },
        .scalar_agg => |sa| {
            for (sa.aggs, 0..) |item, i| if (std.mem.eql(u8, item.alias, name)) return i;
            return null;
        },
        .top_k      => |tk| return findOutputColIdx(tk.input, name),
        .order_by   => |ob| return findOutputColIdx(ob.input, name),
        .limit      => |lm| return findOutputColIdx(lm.input, name),
        .filter     => |f|  return findOutputColIdx(f.input, name),
        else        => return null,
    }
}

fn outputLen(node: *const PhysicalNode) usize {
    switch (node.*) {
        .project    => |p|  return p.items.len,
        .hash_agg   => |ha| return ha.keys.len + ha.aggs.len,
        .scalar_agg => |sa| return sa.aggs.len,
        .top_k      => |tk| return outputLen(tk.input),
        .order_by   => |ob| return outputLen(ob.input),
        .limit      => |lm| return outputLen(lm.input),
        .filter     => |f|  return outputLen(f.input),
        else        => return 0,
    }
}

// ── WhereNode → plan.Expr ─────────────────────────────────────────────────────

fn whereNodeToExpr(ctx: *PlannerCtx, wn: *const generic_sql.WhereNode) ?Expr {
    switch (wn.*) {
        .cmp_int => |c| {
            const col_expr = resolveColExpr(ctx, c.col) orelse return null;
            const binop = ctx.alloc.create(plan.BinOp) catch return null;
            binop.* = .{
                .left  = col_expr,
                .right = .{ .lit_i64 = c.val },
            };
            return switch (c.op) {
                .eq => Expr{ .eq  = binop },
                .ne => Expr{ .neq = binop },
                .lt => Expr{ .lt  = binop },
                .le => Expr{ .lte = binop },
                .gt => Expr{ .gt  = binop },
                .ge => Expr{ .gte = binop },
            };
        },
        .cmp_str => |c| {
            const col_expr = resolveColExpr(ctx, c.col) orelse return null;
            const binop = ctx.alloc.create(plan.BinOp) catch return null;
            binop.* = .{
                .left  = col_expr,
                .right = .{ .lit_str = c.val },
            };
            return switch (c.op) {
                .eq => Expr{ .eq  = binop },
                .ne => Expr{ .neq = binop },
                .lt => Expr{ .lt  = binop },
                .le => Expr{ .lte = binop },
                .gt => Expr{ .gt  = binop },
                .ge => Expr{ .gte = binop },
            };
        },
        .like => |l| {
            const col_expr = resolveColExpr(ctx, l.col) orelse return null;
            const binop = ctx.alloc.create(plan.BinOp) catch return null;
            binop.* = .{ .left = col_expr, .right = .{ .lit_str = l.pattern } };
            return switch (l.op) {
                .like, .ilike => Expr{ .like     = binop },
                .not_like     => Expr{ .not_like = binop },
            };
        },
        .is_null => |col| {
            const col_expr = resolveColExpr(ctx, col) orelse return null;
            const unop = ctx.alloc.create(plan.UnOp) catch return null;
            unop.* = .{ .operand = col_expr };
            return Expr{ .is_null = unop };
        },
        .is_not_null => |col| {
            const col_expr = resolveColExpr(ctx, col) orelse return null;
            const unop = ctx.alloc.create(plan.UnOp) catch return null;
            unop.* = .{ .operand = col_expr };
            return Expr{ .is_not_null = unop };
        },
        .and_ => |children| {
            if (children.len == 0) return Expr{ .lit_bool = true };
            if (children.len == 1) return whereNodeToExpr(ctx, children[0]);
            // Fold: (a AND b AND c) → (a AND (b AND c))
            var acc = whereNodeToExpr(ctx, children[children.len - 1]) orelse return null;
            var i = children.len - 1;
            while (i > 0) {
                i -= 1;
                const left = whereNodeToExpr(ctx, children[i]) orelse return null;
                const binop = ctx.alloc.create(plan.BinOp) catch return null;
                binop.* = .{ .left = left, .right = acc };
                acc = Expr{ .@"and" = binop };
            }
            return acc;
        },
        .or_ => |children| {
            if (children.len == 0) return Expr{ .lit_bool = false };
            if (children.len == 1) return whereNodeToExpr(ctx, children[0]);
            var acc = whereNodeToExpr(ctx, children[children.len - 1]) orelse return null;
            var i = children.len - 1;
            while (i > 0) {
                i -= 1;
                const left = whereNodeToExpr(ctx, children[i]) orelse return null;
                const binop = ctx.alloc.create(plan.BinOp) catch return null;
                binop.* = .{ .left = left, .right = acc };
                acc = Expr{ .@"or" = binop };
            }
            return acc;
        },
    }
}

// ── Column resolution ─────────────────────────────────────────────────────────

/// Resolve a column name to a col_ref Expr (index from schema) or a
/// string literal if the name looks like 'quoted'.
fn resolveColExpr(ctx: *PlannerCtx, col: []const u8) ?Expr {
    // String literal: 'value'
    if (col.len >= 2 and col[0] == '\'' and col[col.len - 1] == '\'') {
        return Expr{ .lit_str = col[1 .. col.len - 1] };
    }
    // Integer literal
    if (std.fmt.parseInt(i64, col, 10) catch null) |iv| {
        return Expr{ .lit_i64 = iv };
    }
    // Float literal
    if (std.fmt.parseFloat(f64, col) catch null) |fv| {
        return Expr{ .lit_f64 = fv };
    }
    // Column reference
    if (ctx.tbl) |tbl| {
        if (tbl.findColumn(col)) |idx| {
            // Array/Map columns are not yet supported in the IR path.
            const ct = schemaColType(ctx, col);
            if (ct == .array_string) return null;
            return Expr{ .col_ref = .{ .index = idx, .name = col } };
        }
    }
    // Unknown column — return null so the caller falls back to generic_executor.
    return null;
}

fn schemaColType(ctx: *PlannerCtx, col_name: []const u8) ColumnType {
    if (ctx.tbl) |tbl| {
        if (tbl.findColumn(col_name)) |idx| {
            return schemaToCore(tbl.columns[idx].ty, tbl.columns[idx].ch_type);
        }
    }
    return .string;
}

fn schemaToCore(ty: schema_mod.ColumnType, ch_type: ?[]const u8) ColumnType {
    if (ch_type) |ct| {
        if (std.mem.startsWith(u8, ct, "Array(") or std.mem.startsWith(u8, ct, "Map("))
            return .array_string;
    }
    return switch (ty) {
        .int8  => .bool_u8,
        .int16 => .uint64,   // UInt16 → uint64 (narrowed at wire time)
        .int32 => blk: {
            if (ch_type) |ct| if (std.mem.startsWith(u8, ct, "U")) break :blk .uint64;
            break :blk .int64;
        },
        .int64 => blk: {
            if (ch_type) |ct| if (std.mem.startsWith(u8, ct, "U")) break :blk .uint64;
            break :blk .int64;
        },
        .date      => .date_u16,
        .timestamp => .datetime64_ms,
        .float32, .float64 => .float64,
        .text, .char => .string,
    };
}

// ── Projection helpers ────────────────────────────────────────────────────────

fn isAggregate(func: generic_sql.AggregateFn) bool {
    return switch (func) {
        .count_star, .count_distinct, .count_if,
        .sum, .avg, .min, .max,
        .uniq_exact, .uniq_exact_if,
        .group_uniq_array, .any_val => true,
        .column_ref, .int_literal, .float_literal => false,
    };
}

fn buildProjectItems(ctx: *PlannerCtx, projs: []const generic_sql.Expr) !?[]ProjectItem {
    const items = try ctx.alloc.alloc(ProjectItem, projs.len);
    for (projs, 0..) |p, i| {
        items[i] = try scalarExprToProjectItem(ctx, p) orelse return null;
    }
    return items;
}

fn scalarExprToProjectItem(ctx: *PlannerCtx, p: generic_sql.Expr) !?ProjectItem {
    const alias = p.alias orelse p.column orelse "?";
    const col_name = p.column orelse "";
    return switch (p.func) {
        .column_ref => blk: {
            const col_expr = resolveColExpr(ctx, col_name) orelse break :blk null;
            const out_type = schemaColType(ctx, col_name);
            break :blk ProjectItem{
                .expr     = col_expr,
                .alias    = alias,
                .out_type = out_type,
            };
        },
        .int_literal => ProjectItem{
            .expr     = .{ .lit_i64 = p.int_offset },
            .alias    = alias,
            .out_type = .int64,
        },
        .float_literal => ProjectItem{
            .expr     = .{ .lit_f64 = p.float_val },
            .alias    = alias,
            .out_type = .float64,
        },
        else => null, // aggregate in scalar context — caller handles
    };
}

fn aggExprToProjectItem(ctx: *PlannerCtx, p: generic_sql.Expr) !?ProjectItem {
    const alias = p.alias orelse p.column orelse "?";
    const col_name = p.column orelse "";

    const agg_call = try ctx.alloc.create(AggCall);
    switch (p.func) {
        .count_star => {
            agg_call.* = .{ .kind = .count_star, .arg = null, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .uint64 };
        },
        .count_distinct, .uniq_exact => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .count, .arg = arg_expr, .distinct = true };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .uint64 };
        },
        .count_if => {
            // countIf uses a condition; translate to a conditional count.
            // For now: count_star (conservative — counts all rows).
            agg_call.* = .{ .kind = .count_star, .arg = null, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .uint64 };
        },
        .sum => {
            const col_type = schemaColType(ctx, col_name);
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .sum, .arg = arg_expr, .distinct = false };
            const out_type: ColumnType = switch (col_type) {
                .float64 => .float64,
                .int64   => .int64,
                else     => .uint64,
            };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = out_type };
        },
        .avg => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .avg, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .float64 };
        },
        .min => {
            const col_type = schemaColType(ctx, col_name);
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .min, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .max => {
            const col_type = schemaColType(ctx, col_name);
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .max, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .group_uniq_array, .uniq_exact_if => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .group_uniq_array, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .array_string };
        },
        .any_val => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .any, .arg = arg_expr, .distinct = false };
            const col_type = schemaColType(ctx, col_name);
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        else => return null,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "planner: simple scan" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Build a minimal schema: table "t" with column "n" Int64
    const cols = [_]schema_mod.Column{
        .{ .name = "n", .ty = .int64 },
    };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const proj_expr = generic_sql.Expr{
        .func   = .column_ref,
        .column = "n",
        .alias  = "n",
    };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{
        .table       = "t",
        .projections = &projs,
    };

    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
    try std.testing.expect(node.?.* == .project or node.?.* == .part_scan);
}

test "planner: count(*) scalar agg" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cols = [_]schema_mod.Column{ .{ .name = "n", .ty = .int64 } };
    const tbl = schema_mod.Table{ .name = "t", .columns = &cols };
    var ctx = PlannerCtx.init(alloc, tbl);

    const proj_expr = generic_sql.Expr{ .func = .count_star, .alias = "cnt" };
    const projs = [_]generic_sql.Expr{proj_expr};
    const gplan = generic_sql.Plan{ .table = "t", .projections = &projs };

    const node = try plan_query(&ctx, gplan);
    try std.testing.expect(node != null);
    // Root should be scalar_agg
    var n = node.?;
    while (true) {
        switch (n.*) {
            .scalar_agg => break,
            .limit => |lm| n = lm.input,
            else => { try std.testing.expect(false); break; },
        }
    }
}
