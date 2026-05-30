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
pub const VirtualCol = struct { name: []const u8, idx: usize, col_type: ColumnType };

pub const PlannerCtx = struct {
    alloc: std.mem.Allocator,
    /// The schema of the table being scanned.
    tbl: ?schema_mod.Table,
    /// Virtual columns from the agg output (alias → output index).
    /// Used to resolve __ha__* references in post-agg scalar projections.
    virtual_cols: []const VirtualCol = &.{},

    pub fn init(alloc: std.mem.Allocator, tbl: ?schema_mod.Table) PlannerCtx {
        return .{ .alloc = alloc, .tbl = tbl };
    }
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Collect all col_ref indices used in an Expr tree into a bitset.
fn collectExprCols(expr: Expr, used: *std.bit_set.IntegerBitSet(256)) void {
    switch (expr) {
        .col_ref => |r| { if (r.index < 256) used.set(r.index); },
        .add, .sub, .mul, .div, .mod,
        .eq, .neq, .lt, .lte, .gt, .gte,
        .@"and", .@"or",
        .like, .not_like, .concat => |b| {
            collectExprCols(b.left, used);
            collectExprCols(b.right, used);
        },
        .not, .is_null, .is_not_null => |u| collectExprCols(u.operand, used),
        .case_when => |cw| {
            for (cw.when) |c| collectExprCols(c, used);
            for (cw.then) |r| collectExprCols(r, used);
            if (cw.else_expr) |e| collectExprCols(e, used);
        },
        .agg_call => |a| {
            if (a.arg) |arg| collectExprCols(arg, used);
        },
        .fn_call => |f| for (f.args) |a| collectExprCols(a, used),
        .cast => |c| collectExprCols(c.expr, used),
        .dict_call => |d| for (d.keys) |k| collectExprCols(k, used),
        .lambda => |l| collectExprCols(l.body.*, used),
        else => {},
    }
}

/// Walk the plan tree and collect all col_ref indices into `used`.
fn collectPlanCols(node: *const PhysicalNode, used: *std.bit_set.IntegerBitSet(256)) void {
    switch (node.*) {
        .part_scan  => {},  // leaf — no exprs
        .mem_scan   => {},
        .chunk_source => |cs| collectPlanCols(cs.input, used),
        .filter => |f| {
            collectPlanCols(f.input, used);
            collectExprCols(f.predicate, used);
        },
        .project => |p| {
            collectPlanCols(p.input, used);
            for (p.items) |item| collectExprCols(item.expr, used);
        },
        .hash_agg => |ha| {
            collectPlanCols(ha.input, used);
            for (ha.keys) |k| collectExprCols(k.expr, used);
            for (ha.aggs) |a| collectExprCols(a.expr, used);
        },
        .scalar_agg => |sa| {
            collectPlanCols(sa.input, used);
            for (sa.aggs) |a| collectExprCols(a.expr, used);
        },
        .hash_join => |hj| {
            collectPlanCols(hj.left, used);
            collectPlanCols(hj.right, used);
        },
        .order_by => |ob| collectPlanCols(ob.input, used),
        .top_k => |tk| collectPlanCols(tk.input, used),
        .limit => |lm| collectPlanCols(lm.input, used),
    }
}

/// Find the PartScanNode and push down a column list.
/// `used` is the bitset of referenced column indices; `tbl` provides names.
fn pushdownColumns(
    node: *PhysicalNode,
    used: std.bit_set.IntegerBitSet(256),
    tbl: schema_mod.Table,
    alloc: std.mem.Allocator,
) !void {
    switch (node.*) {
        .part_scan => |*ps| {
            // Count referenced columns.
            var n: usize = 0;
            var it = used.iterator(.{});
            while (it.next()) |idx| {
                if (idx < tbl.columns.len) n += 1;
            }
            // If all columns are referenced (SELECT *), don't pushdown.
            if (n >= tbl.columns.len) return;
            if (n == 0) {
                // COUNT(*)-style: no columns referenced.  Pick the first column
                // so the source can still emit chunks with the correct num_rows.
                if (tbl.columns.len > 0) {
                    const cols = try alloc.alloc([]const u8, 1);
                    cols[0] = tbl.columns[0].name;
                    ps.columns = cols;
                }
                return;
            }
            const cols = try alloc.alloc([]const u8, n);
            var i: usize = 0;
            var it2 = used.iterator(.{});
            while (it2.next()) |idx| {
                if (idx < tbl.columns.len) {
                    cols[i] = tbl.columns[idx].name;
                    i += 1;
                }
            }
            ps.columns = cols;
        },
        .chunk_source => |cs| try pushdownColumns(cs.input, used, tbl, alloc),
        .filter => |f| try pushdownColumns(f.input, used, tbl, alloc),
        .project => |p| try pushdownColumns(p.input, used, tbl, alloc),
        .hash_agg => |ha| try pushdownColumns(ha.input, used, tbl, alloc),
        .scalar_agg => |sa| try pushdownColumns(sa.input, used, tbl, alloc),
        .hash_join => |hj| {
            try pushdownColumns(hj.left, used, tbl, alloc);
            try pushdownColumns(hj.right, used, tbl, alloc);
        },
        .order_by => |ob| try pushdownColumns(ob.input, used, tbl, alloc),
        .top_k => |tk| try pushdownColumns(tk.input, used, tbl, alloc),
        .limit => |lm| try pushdownColumns(lm.input, used, tbl, alloc),
        .mem_scan => {},
    }
}

/// Translate a generic_sql.Plan into a PhysicalNode tree.
/// Returns null if the plan cannot be translated (e.g. unsupported construct).
/// Walk the physical plan tree to find the columns list on the innermost
/// PartScanNode.  Returns an empty slice when the node is not a scan-based
/// plan (e.g. a VALUES literal) or when column pruning was not applied.
pub fn findPrunedCols(node: *const PhysicalNode) []const []const u8 {
    return switch (node.*) {
        .part_scan  => |ps| ps.columns,
        .filter     => |f|  findPrunedCols(f.input),
        .project    => |p|  findPrunedCols(p.input),
        .hash_agg   => |a|  findPrunedCols(a.input),
        .scalar_agg => |a|  findPrunedCols(a.input),
        .top_k      => |t|  findPrunedCols(t.input),
        .limit      => |l|  findPrunedCols(l.input),
        .order_by   => |o|  findPrunedCols(o.input),
        else        =>      &.{},
    };
}

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
        const pred = whereNodeToExpr(ctx, we) orelse {
            // whereNodeToExpr failed (e.g. unsupported column type) — must fall back.
            return null;
        };
        const fn_ptr = try ctx.alloc.create(PhysicalNode);
        fn_ptr.* = .{ .filter = .{ .input = source, .predicate = pred } };
        source = fn_ptr;
    } else if (gplan.where_text != null) {
        // No structured WhereNode (translateWhere failed) — fall back.
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

    var order_by_text_handled = false; // track if ORDER BY was consumed by sort-before-project

    if (!has_agg) {
        // ── Pure projection / scan ─────────────────────────────────────────
        // If ORDER BY references a schema column not in SELECT output, we must
        // sort the raw scan BEFORE projection (sort-before-project).
        // Detect this case: order_by_text is a simple ident present in schema.
        if (gplan.order_by_text) |ob_text| {
            // Parse the order_by_text for non-agg: comma-separated list of "col [DESC]".
            // All columns must be simple schema identifiers (no function calls, no parens).
            const tbl = ctx.tbl orelse return null;
            var sort_keys_list: std.ArrayListUnmanaged(SortKey) = .empty;
            var all_resolved = true;
            var col_it = std.mem.splitScalar(u8, ob_text, ',');
            while (col_it.next()) |raw_item| {
                const item = std.mem.trim(u8, raw_item, " \t\r\n");
                if (item.len == 0) continue;
                var desc = false;
                const col_part = if (std.ascii.endsWithIgnoreCase(item, " desc")) blk: {
                    desc = true;
                    break :blk std.mem.trimEnd(u8, item[0..item.len - 5], " \t");
                } else if (std.ascii.endsWithIgnoreCase(item, " asc")) blk: {
                    break :blk std.mem.trimEnd(u8, item[0..item.len - 4], " \t");
                } else item;
                // Only simple schema column idents (no parens, spaces, commas within a single item).
                const is_simple = std.mem.indexOfAny(u8, col_part, " \t(") == null;
                if (!is_simple) { all_resolved = false; break; }
                if (tbl.findColumn(col_part)) |schema_idx| {
                    try sort_keys_list.append(ctx.alloc, .{ .col_idx = schema_idx, .desc = desc, .nulls_first = false });
                } else {
                    all_resolved = false;
                    break;
                }
            }
            if (all_resolved and sort_keys_list.items.len > 0) {
                const sort_keys = try sort_keys_list.toOwnedSlice(ctx.alloc);
                if (gplan.limit != null) {
                    const k: u64 = @intCast(gplan.limit.?);
                    const topk = try ctx.alloc.create(PhysicalNode);
                    topk.* = .{ .top_k = .{ .input = source, .keys = sort_keys, .k = k } };
                    source = topk;
                } else {
                    const ob = try ctx.alloc.create(PhysicalNode);
                    ob.* = .{ .order_by = .{ .input = source, .keys = sort_keys } };
                    source = ob;
                }
                order_by_text_handled = true;
            } else {
                return null; // Unresolvable or complex ORDER BY — fall back.
            }
        }
        const items = try buildProjectItems(ctx, projs) orelse return null;
        const proj_node = try ctx.alloc.create(PhysicalNode);
        proj_node.* = .{ .project = .{ .input = source, .items = items } };
        source = proj_node;
    } else {
        // ── Aggregation ───────────────────────────────────────────────────
        // Split projs into key exprs (non-agg, in GROUP BY) and agg exprs.
        // Also detect "__aj__*" helper columns produced by rewriteArrayJoin:
        // these are arrayJoin expansion columns that serve as agg arguments
        // but should NOT be GROUP BY keys themselves.
        var key_items_list:  std.ArrayListUnmanaged(ProjectItem) = .empty;
        var agg_items_list:  std.ArrayListUnmanaged(ProjectItem) = .empty;

        // Check whether any non-agg projection has an "__aj__" alias (ARRAY JOIN helper).
        var has_aj_cols = false;
        for (projs) |p| {
            if (!isAggregate(p.func)) {
                const alias = p.alias orelse p.column orelse "";
                if (std.mem.startsWith(u8, alias, "__aj__")) {
                    has_aj_cols = true;
                    break;
                }
            }
        }

        if (has_aj_cols) {
            // ── ARRAY JOIN + GROUP BY path ────────────────────────────────
            // Build a pre-project node that expands the arrayJoin columns.
            // The pre-project emits: [key0, key1, ..., __aj__col0, __aj__col1, ...]
            // Then hash_agg uses indices into the pre-project output.

            // Collect pre-project items: real keys first, then __aj__ helpers.
            var pre_items_list: std.ArrayListUnmanaged(ProjectItem) = .empty;
            var real_key_count: usize = 0;
            // First pass: real (non-__aj__) key projections
            for (projs) |p| {
                if (isAggregate(p.func)) continue;
                const alias = p.alias orelse p.column orelse "";
                if (std.mem.startsWith(u8, alias, "__aj__")) continue;
                const item = try scalarExprToProjectItem(ctx, p) orelse return null;
                try pre_items_list.append(ctx.alloc, item);
                real_key_count += 1;
            }
            // Second pass: __aj__ helper projections
            for (projs) |p| {
                if (isAggregate(p.func)) continue;
                const alias = p.alias orelse p.column orelse "";
                if (!std.mem.startsWith(u8, alias, "__aj__")) continue;
                const item = try scalarExprToProjectItem(ctx, p) orelse return null;
                try pre_items_list.append(ctx.alloc, item);
            }
            const pre_items = try pre_items_list.toOwnedSlice(ctx.alloc);

            // Build pre-project node (performs arrayJoin row expansion).
            const pre_proj_node = try ctx.alloc.create(PhysicalNode);
            pre_proj_node.* = .{ .project = .{ .input = source, .items = pre_items } };

            // Build hash_agg keys: real keys only, referencing pre-project output indices.
            const hash_keys = try ctx.alloc.alloc(ProjectItem, real_key_count);
            for (0..real_key_count) |ki| {
                const src = pre_items[ki];
                const ref = try ctx.alloc.create(plan.ColRef);
                ref.* = .{ .index = ki, .name = src.alias };
                hash_keys[ki] = .{
                    .expr     = Expr{ .col_ref = ref.* },
                    .alias    = src.alias,
                    .out_type = src.out_type,
                };
            }

            // Build agg items: each agg arg is looked up in the pre-project output.
            for (projs) |p| {
                if (!isAggregate(p.func)) continue;
                // Build agg item with resolved arg from pre-project output.
                const agg_item = try aggExprToProjectItemWithPreProject(
                    ctx, p, pre_items, real_key_count,
                ) orelse return null;
                try agg_items_list.append(ctx.alloc, agg_item);
            }
            const agg_items = try agg_items_list.toOwnedSlice(ctx.alloc);

            const agg_node = try ctx.alloc.create(PhysicalNode);
            agg_node.* = .{ .hash_agg = .{
                .input = pre_proj_node,
                .keys  = hash_keys,
                .aggs  = agg_items,
            }};
            source = agg_node;
        } else {
            // ── Normal aggregation (no ARRAY JOIN helpers) ────────────────
            // Non-agg projections whose column text references "__ha__*" aliases
            // are "post-agg scalars" — they must be evaluated after the agg node.
            var post_agg_projs_list: std.ArrayListUnmanaged(generic_sql.Expr) = .empty;
            for (projs) |p| {
                if (isAggregate(p.func)) {
                    const item = try aggExprToProjectItem(ctx, p) orelse return null;
                    try agg_items_list.append(ctx.alloc, item);
                } else {
                    // Check if column text contains "__ha__" (post-agg scalar reference).
                    const col_text = p.column orelse "";
                    const has_ha = std.mem.indexOf(u8, col_text, "__ha__") != null;
                    if (has_ha) {
                        try post_agg_projs_list.append(ctx.alloc, p);
                    } else {
                        const item = try scalarExprToProjectItem(ctx, p) orelse return null;
                        try key_items_list.append(ctx.alloc, item);
                    }
                }
            }

            const key_items = try key_items_list.toOwnedSlice(ctx.alloc);
            const agg_items = try agg_items_list.toOwnedSlice(ctx.alloc);
            const post_agg_projs = try post_agg_projs_list.toOwnedSlice(ctx.alloc);

            const agg_node = try ctx.alloc.create(PhysicalNode);
            if (key_items.len == 0 and gplan.group_by != null) {
                // GROUP BY is present but no key projections found in SELECT:
                // fall back to generic executor which can evaluate complex GROUP BY expressions.
                return null;
            }
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

            // If there are post-agg scalar projections (e.g. display_risk = 1 - __ha__max_conf),
            // build a post-project node after the agg_node.
            // The agg output columns are: [key_items..., agg_items...].
            // We populate virtual_cols so resolveColExpr can find them by name.
            if (post_agg_projs.len > 0) {
                // Build virtual column list from agg output.
                var vcols_list: std.ArrayListUnmanaged(VirtualCol) = .empty;
                for (key_items, 0..) |ki, i| {
                    try vcols_list.append(ctx.alloc, .{
                        .name = ki.alias,
                        .idx = i,
                        .col_type = ki.out_type,
                    });
                }
                for (agg_items, 0..) |ai, i| {
                    try vcols_list.append(ctx.alloc, .{
                        .name = ai.alias,
                        .idx = key_items.len + i,
                        .col_type = ai.out_type,
                    });
                }
                ctx.virtual_cols = try vcols_list.toOwnedSlice(ctx.alloc);
                defer { ctx.virtual_cols = &.{}; } // reset after this block

                // Build post-project items: all key_items (as pass-through col refs)
                // plus the post-agg scalar expressions (which reference __ha__* cols).
                var post_items_list: std.ArrayListUnmanaged(ProjectItem) = .empty;
                // Pass through key_items (GROUP BY cols) first.
                for (key_items, 0..) |ki, i| {
                    const ref = try ctx.alloc.create(plan.ColRef);
                    ref.* = .{ .index = i, .name = ki.alias };
                    try post_items_list.append(ctx.alloc, .{
                        .expr = Expr{ .col_ref = ref.* },
                        .alias = ki.alias,
                        .out_type = ki.out_type,
                    });
                }
                // Pass through agg_items as col refs (skip __ha__* hidden ones from final output).
                for (agg_items, 0..) |ai, i| {
                    if (std.mem.startsWith(u8, ai.alias, "__ha__")) continue;
                    const ref = try ctx.alloc.create(plan.ColRef);
                    ref.* = .{ .index = key_items.len + i, .name = ai.alias };
                    try post_items_list.append(ctx.alloc, .{
                        .expr = Expr{ .col_ref = ref.* },
                        .alias = ai.alias,
                        .out_type = ai.out_type,
                    });
                }
                // Add post-agg scalar projections (using virtual_cols for __ha__* resolution).
                for (post_agg_projs) |pp| {
                    const item = try scalarExprToProjectItem(ctx, pp) orelse return null;
                    try post_items_list.append(ctx.alloc, item);
                }
                const post_items = try post_items_list.toOwnedSlice(ctx.alloc);
                const post_proj_node = try ctx.alloc.create(PhysicalNode);
                post_proj_node.* = .{ .project = .{ .input = source, .items = post_items } };
                source = post_proj_node;
            }
        }

        // HAVING → post-agg filter
        if (gplan.having_expr) |he| {
            // Build virtual_cols from the current agg output so resolveColExpr
            // can look up agg-output alias names (e.g. "c", "l") used in HAVING.
            // Also register canonical function-name forms (e.g. "count_star()")
            // since DuckDB's exprToText may produce those instead of aliases.
            var vcols_list: std.ArrayListUnmanaged(VirtualCol) = .empty;
            switch (source.*) {
                .hash_agg => |ha| {
                    for (ha.keys, 0..) |ki, i| try vcols_list.append(ctx.alloc, .{ .name = ki.alias, .idx = i, .col_type = ki.out_type });
                    for (ha.aggs, 0..) |ai, i| {
                        const out_idx = ha.keys.len + i;
                        try vcols_list.append(ctx.alloc, .{ .name = ai.alias, .idx = out_idx, .col_type = ai.out_type });
                        // Also register by canonical agg function name so HAVING COUNT(*) > N works
                        // even when the alias differs from the function text.
                        var canon_buf: [2][]const u8 = undefined;
                        for (aggCanonNames(ai.expr, &canon_buf)) |canon| {
                            try vcols_list.append(ctx.alloc, .{ .name = canon, .idx = out_idx, .col_type = ai.out_type });
                        }
                    }
                },
                .scalar_agg => |sa| {
                    for (sa.aggs, 0..) |ai, i| {
                        try vcols_list.append(ctx.alloc, .{ .name = ai.alias, .idx = i, .col_type = ai.out_type });
                        var canon_buf: [2][]const u8 = undefined;
                        for (aggCanonNames(ai.expr, &canon_buf)) |canon| {
                            try vcols_list.append(ctx.alloc, .{ .name = canon, .idx = i, .col_type = ai.out_type });
                        }
                    }
                },
                else => {},
            }
            const saved_vcols = ctx.virtual_cols;
            ctx.virtual_cols = try vcols_list.toOwnedSlice(ctx.alloc);
            defer {
                ctx.alloc.free(ctx.virtual_cols);
                ctx.virtual_cols = saved_vcols;
            }
            const pred = whereNodeToExpr(ctx, he) orelse return null;
            const filter_node = try ctx.alloc.create(PhysicalNode);
            filter_node.* = .{ .filter = .{ .input = source, .predicate = pred } };
            source = filter_node;
        } else if (gplan.having_text != null) {
            // having_text present but no structured expr — fall back.
            return null;
        }
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
        // If the alias is not in the output (e.g. FINAL-appended ORDER BY version),
        // silently skip rather than aborting the IR plan.
        if (findOutputColIdx(source, alias)) |col_idx| {
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
        }
        // else: ORDER BY references a column not in SELECT — skip silently.
    } else if (gplan.order_by_text) |ob_text| {
        // For aggregate queries, support multi-column ORDER BY when all referenced
        // columns are present in the output (e.g. "protocol, feature" after ARRAY JOIN).
        // If a simple single ident is not in output (FINAL case), skip silently.
        if (has_agg) {
            // Parse comma-separated order items: "col1 [ASC|DESC], col2 [ASC|DESC], ..."
            // Try to build SortKeys for all items. If any column is not in output → skip or fallback.
            var sort_keys_list: std.ArrayListUnmanaged(SortKey) = .empty;
            var all_found = true;
            var skip_silently = false; // single unknown ident → silent skip (FINAL)

            // Split by comma
            var it = std.mem.splitScalar(u8, ob_text, ',');
            while (it.next()) |raw_item| {
                const item = std.mem.trim(u8, raw_item, " \t\r\n");
                if (item.len == 0) continue;
                // Strip trailing ASC/DESC
                var desc = false;
                const col_part = if (std.ascii.endsWithIgnoreCase(item, " desc")) blk: {
                    desc = true;
                    break :blk std.mem.trimEnd(u8, item[0..item.len - 5], " \t");
                } else if (std.ascii.endsWithIgnoreCase(item, " asc")) blk: {
                    break :blk std.mem.trimEnd(u8, item[0..item.len - 4], " \t");
                } else item;

                if (findOutputColIdx(source, col_part)) |col_idx| {
                    try sort_keys_list.append(ctx.alloc, .{
                        .col_idx = col_idx,
                        .desc = desc,
                        .nulls_first = false,
                    });
                } else {
                    // Column not in output.
                    const is_simple_ident = std.mem.indexOfAny(u8, col_part, " \t(,") == null;
                    if (is_simple_ident and sort_keys_list.items.len == 0) {
                        // Single unknown ident at start → FINAL-style skip.
                        skip_silently = true;
                    }
                    all_found = false;
                    break;
                }
            }

            if (all_found and sort_keys_list.items.len > 0) {
                const sort_keys = try sort_keys_list.toOwnedSlice(ctx.alloc);
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
            } else if (!skip_silently and !all_found) {
                return null; // Complex or unresolvable ORDER BY — fall back.
            }
            // else: skip_silently or empty → do nothing.
        } else {
            // Non-aggregate with complex ORDER BY — not supported in IR path.
            // (Simple schema-column ORDER BY is handled above in sort-before-project.)
            if (!order_by_text_handled) return null;
        }
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

    // ── Column pruning: push down only referenced columns to part_scan ────────
    if (ctx.tbl) |tbl| {
        var used = std.bit_set.IntegerBitSet(256).initEmpty();
        collectPlanCols(source, &used);
        try pushdownColumns(source, used, tbl, ctx.alloc);
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

/// Return the canonical DuckDB exprToText form for a simple agg_call ProjectItem,
/// e.g. count(*) → "count_star()", sum(col) → null (too variable).
/// Used to register extra virtual_cols so HAVING COUNT(*) > N works regardless of alias.
/// Returns a slice of up to 2 canonical name variants for the given aggregate expression.
fn aggCanonNames(expr: plan.Expr, buf: *[2][]const u8) []const []const u8 {
    if (expr != .agg_call) return buf[0..0];
    const ac = expr.agg_call;
    return switch (ac.kind) {
        .count_star => blk: {
            buf[0] = "count_star()";
            buf[1] = "count(*)";
            break :blk buf[0..2];
        },
        .count => blk: {
            buf[0] = "count()";
            break :blk buf[0..1];
        },
        else => buf[0..0],
    };
}

fn aggCanonName(expr: plan.Expr) ?[]const u8 {
    if (expr != .agg_call) return null;
    const ac = expr.agg_call;
    return switch (ac.kind) {
        .count_star => "count_star()",
        .count      => "count()",
        else        => null,
    };
}

/// Parse "YYYY-MM-DD" string to days-since-epoch (i64), or null if not a date string.
fn parseDateStrToI64(s: []const u8) ?i64 {
    if (s.len < 10 or s[4] != '-' or s[7] != '-') return null;
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const m = std.fmt.parseInt(u32, s[5..7], 10) catch return null;
    const d = std.fmt.parseInt(u32, s[8..10], 10) catch return null;
    var yr: i32 = y;
    var mo: i32 = @intCast(m);
    if (mo <= 2) { yr -= 1; mo += 9; } else { mo -= 3; }
    const era: i32 = @divFloor(yr, 400);
    const yoe: i32 = yr - era * 400;
    const doy: i32 = @divFloor(153 * mo + 2, 5) + @as(i32, @intCast(d)) - 1;
    const doe: i32 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days: i32 = era * 146097 + doe - 719468;
    return @as(i64, days);
}

fn whereNodeToExpr(ctx: *PlannerCtx, wn: *const generic_sql.WhereNode) ?Expr {
    switch (wn.*) {
        .cmp_int => |c| {
            const col_expr = resolveColExpr(ctx, c.col) orelse
                (parseArithExpr(ctx, c.col) catch null) orelse return null;
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
            const col_expr = resolveColExpr(ctx, c.col) orelse
                (parseArithExpr(ctx, c.col) catch null) orelse return null;
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
             const ct = schemaColType(ctx, col);
             if (ct == .array_string) {
                 // Only Array(String) columns are supported in IR.
                 // Map(String,*) is .string type now (custom blob format).
                 // Non-string Arrays (Array(Float64) etc.) are not supported.
                 if (tbl.columns[idx].ch_type) |ch| {
                     if (!std.mem.startsWith(u8, ch, "Array(String)") and
                         !std.mem.startsWith(u8, ch, "Array(LowCardinality(String))"))
                         return null;
                 } else {
                     return null;
                 }
             }
             return Expr{ .col_ref = .{ .index = idx, .name = col } };
         }
    }
    // Unknown column — check virtual columns (agg output) before giving up.
    for (ctx.virtual_cols) |vc| {
        if (std.mem.eql(u8, vc.name, col)) {
            return Expr{ .col_ref = .{ .index = vc.idx, .name = col } };
        }
    }
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
        if (std.mem.startsWith(u8, ct, "Array("))
            return .array_string;
        // Map(String,*) stays as .string (custom blob, not array_string)
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
        .text, .char, .low_card => .string,
    };
}

// ── Projection helpers ────────────────────────────────────────────────────────

fn isAggregate(func: generic_sql.AggregateFn) bool {
    return switch (func) {
        .count_star, .count_distinct, .count_if,
        .sum, .avg, .min, .max,
        .min_if, .max_if, .sum_array, .sum_array_if,
        .uniq_exact, .uniq_exact_if,
        .group_uniq_array, .any_val => true,
        .column_ref, .int_literal, .float_literal, .case_when, .cmp_expr => false,
    };
}

fn buildProjectItems(ctx: *PlannerCtx, projs: []const generic_sql.Expr) !?[]ProjectItem {
    // Check for SELECT * (single column_ref with null column or column=="*").
    if (projs.len == 1 and projs[0].func == .column_ref and
        (projs[0].column == null or std.mem.eql(u8, projs[0].column orelse "", "*"))) {
        const tbl = ctx.tbl orelse return null;
        const ncols = tbl.columns.len;
        if (ncols == 0) return null;
        const items = try ctx.alloc.alloc(ProjectItem, ncols);
        for (tbl.columns, 0..) |col, i| {
            const ct = schemaToCore(col.ty, col.ch_type);
            items[i] = ProjectItem{
                .expr     = Expr{ .col_ref = .{ .index = i, .name = col.name } },
                .alias    = col.name,
                .out_type = ct,
            };
        }
        return items;
    }
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
            const col_expr = resolveColExpr(ctx, col_name) orelse {
                // col_name might be a function call like "lower(protocol)" — try to
                // parse it as a known scalar fn and build an fn_call Expr.
                const item = try tryParseFnCallItem(ctx, col_name, alias) orelse {
                    // Last resort: try parseArithExpr for multi-arg fns like date_part.
                    const expr = try parseArithExpr(ctx, col_name) orelse {
                        break :blk null;
                    };
                    break :blk ProjectItem{
                        .expr     = expr,
                        .alias    = alias,
                        .out_type = .string,  // scalar fn may return string
                    };
                };
                break :blk item;
            };
            const out_type = schemaColType(ctx, col_name);
            // Look up narrow wire type override (e.g. UInt16, UInt32) from schema.
            const ch_type_override: ?[]const u8 = ch_blk: {
                if (ctx.tbl) |tbl| {
                    if (tbl.findColumn(col_name)) |idx| {
                        if (tbl.columns[idx].ch_type) |ch| {
                            if (std.mem.eql(u8, ch, "UInt16") or std.mem.eql(u8, ch, "UInt32")) {
                                break :ch_blk ch;
                            }
                        }
                    }
                }
                break :ch_blk null;
            };
            // Handle col + N / col - N (int_offset)
            const final_expr: Expr = if (p.int_offset != 0) expr: {
                const binop = ctx.alloc.create(plan.BinOp) catch break :blk null;
                binop.* = .{ .left = col_expr, .right = .{ .lit_i64 = p.int_offset } };
                break :expr if (p.int_offset > 0) Expr{ .add = binop } else Expr{ .sub = binop };
            } else col_expr;
            break :blk ProjectItem{
                .expr     = final_expr,
                .alias    = alias,
                .out_type = out_type,
                .ch_type  = ch_type_override,
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
        .case_when => blk: {
            const cwd = p.case_when_data orelse break :blk null;
            const n = cwd.when_texts.len;
            const when_exprs = ctx.alloc.alloc(plan.Expr, n) catch break :blk null;
            const then_exprs = ctx.alloc.alloc(plan.Expr, n) catch break :blk null;
            for (cwd.when_texts, 0..) |wt, i| {
                when_exprs[i] = (resolveColExpr(ctx, wt) orelse
                    (parseArithExpr(ctx, wt) catch null) orelse break :blk null);
            }
            for (cwd.then_texts, 0..) |tt, i| {
                then_exprs[i] = (resolveColExpr(ctx, tt) orelse
                    (parseArithExpr(ctx, tt) catch null) orelse break :blk null);
            }
            const else_expr: ?plan.Expr = if (cwd.else_text) |et|
                (resolveColExpr(ctx, et) orelse (parseArithExpr(ctx, et) catch null))
            else null;
            const cw = ctx.alloc.create(plan.CaseWhen) catch break :blk null;
            cw.* = .{ .when = when_exprs, .then = then_exprs, .else_expr = else_expr };
            break :blk ProjectItem{
                .expr     = Expr{ .case_when = cw },
                .alias    = alias,
                .out_type = .string, // CASE WHEN output is typically string
            };
        },
        .cmp_expr => null, // handled by generic_executor path; planner doesn't process this
        else => null, // aggregate in scalar context — caller handles
    };
}

// ── Scalar function call text parser ─────────────────────────────────────────
//
// duckdb_parse encodes unrecognised function calls like lower(protocol) as:
//   .func = .column_ref, .column = "lower(protocol)"
//
// We detect a small set of single-argument scalar functions by parsing the
// text, resolve the argument column, and build a plan.FnCall Expr that
// kernels.evalFnCall already handles.
//
// Supported (single-arg, no inner parens in arg):
//   lower(col)  upper(col)  length(col)  char_length(col)
//   toDate(col)  toDateOrZero(col)  toYYYYMMDD(col)  toyyyymmdd(col)
//   toStartOfHour(col)  toStartOfDay(col)  toStartOfMinute(col)
//   toString(col)  abs(col)  floor(col)  ceil(col)  round(col)
//   trim(col)  trimLeft(col)  trimRight(col)  ltrim(col)  rtrim(col)
//   toInt64(col)  toInt32(col)  toFloat64(col)  toFloat32(col)
//
// Supported (two-arg where first arg is a string literal):
//   date_trunc('hour', col)  date_trunc('day', col)  date_trunc('minute', col)

/// Known single-argument scalar functions → output ColumnType.
const ScalarFn = struct { name: []const u8, out: ColumnType };
const scalar_fns = [_]ScalarFn{
    .{ .name = "lower",          .out = .string        },
    .{ .name = "upper",          .out = .string        },
    .{ .name = "lowerUTF8",      .out = .string        },
    .{ .name = "upperUTF8",      .out = .string        },
    .{ .name = "length",         .out = .int64         },
    .{ .name = "char_length",    .out = .int64         },
    .{ .name = "toString",       .out = .string        },
    .{ .name = "trim",           .out = .string        },
    .{ .name = "trimLeft",       .out = .string        },
    .{ .name = "trimRight",      .out = .string        },
    .{ .name = "ltrim",          .out = .string        },
    .{ .name = "rtrim",          .out = .string        },
    .{ .name = "abs",            .out = .float64       },
    .{ .name = "floor",          .out = .int64         },
    .{ .name = "ceil",           .out = .int64         },
    .{ .name = "ceiling",        .out = .int64         },
    .{ .name = "round",          .out = .float64       },
    .{ .name = "toInt64",        .out = .int64         },
    .{ .name = "toInt32",        .out = .int64         },
    .{ .name = "toFloat64",      .out = .float64       },
    .{ .name = "toFloat32",      .out = .float64       },
    .{ .name = "toInt64OrZero",  .out = .int64         },
    .{ .name = "toDate",         .out = .date_u16      },
    .{ .name = "toDateOrZero",   .out = .date_u16      },
    .{ .name = "toYYYYMMDD",     .out = .int64         },
    .{ .name = "toyyyymmdd",     .out = .int64         }, // duckdb_parse lowercases it
    .{ .name = "toStartOfHour",  .out = .datetime64_ms },
    .{ .name = "toStartOfDay",   .out = .datetime64_ms },
     .{ .name = "toStartOfMinute",.out = .datetime64_ms },
     .{ .name = "isIPv4String",    .out = .bool_u8       },
     .{ .name = "isIPv6String",    .out = .bool_u8       },
     .{ .name = "IPv4StringToNumOrDefault", .out = .uint64 },
     .{ .name = "IPv4NumToString", .out = .string        },
     .{ .name = "IPv6StringToNumOrDefault", .out = .uint64 },
     .{ .name = "IPv6NumToString", .out = .string        },
};

/// Map date_trunc unit string → kernels function name and output ColumnType.
const DateTruncUnit = struct { unit: []const u8, fn_name: []const u8, out: ColumnType };
const date_trunc_units = [_]DateTruncUnit{
    .{ .unit = "hour",   .fn_name = "toStartOfHour",   .out = .datetime64_ms },
    .{ .unit = "day",    .fn_name = "toStartOfDay",    .out = .datetime64_ms },
    .{ .unit = "minute", .fn_name = "toStartOfMinute", .out = .datetime64_ms },
};

// ── Pratt expression parser ───────────────────────────────────────────────────
//
// Converts a text expression (as emitted by duckdb_parse.exprToText) into a
// typed plan.Expr tree.  Handles:
//
//   - Integer / float / string literals
//   - Column references (looked up in schema)
//   - Arithmetic: + - * / %  (standard precedence)
//   - Unary minus
//   - Parenthesised sub-expressions: (expr)
//   - Scalar function calls: fn(expr, expr, …)
//     ∙ Single-arg: floor(x), abs(x), lower(x), …
//     ∙ Two-arg numeric: greatest(a,b), least(a,b), intDiv(a,b), …
//     ∙ date_trunc('unit', col)
//
// Returns null (not an error) if the text cannot be fully parsed.

/// Token kinds produced by the lexer.
const TokKind = enum {
    num_int,     // integer literal
    num_float,   // float literal
    str_lit,     // 'quoted string'
    ident,       // identifier / column name
    lparen,      // (
    rparen,      // )
    comma,       // ,
    plus,        // +
    minus,       // -
    star,        // *
    slash,       // /
    percent,     // %
    eq,          // =
    neq,         // <> or !=
    lt,          // <
    lte,         // <=
    gt,          // >
    gte,         // >=
    kw_and,      // AND
    kw_or,       // OR
    kw_not,      // NOT
    kw_cast,     // CAST
    kw_as,       // AS
    kw_case,     // CASE
    kw_when,     // WHEN
    kw_then,     // THEN
    kw_else,     // ELSE
    kw_end,      // END
    kw_is,       // IS
    kw_null,     // NULL
    kw_between,  // BETWEEN
    kw_in,       // IN
    lbracket,    // [
    rbracket,    // ]
    arrow,       // ->
    eof,
};

const Token = struct {
    kind: TokKind,
    text: []const u8,  // slice into original input
};

/// Minimal lexer: produces one token at a time from a []const u8 cursor.
const Lexer = struct {
    src:  []const u8,
    pos:  usize,

    fn init(src: []const u8) Lexer { return .{ .src = src, .pos = 0 }; }

    fn skipWs(self: *Lexer) void {
        while (self.pos < self.src.len and
               (self.src[self.pos] == ' ' or self.src[self.pos] == '\t' or
                self.src[self.pos] == '\r' or self.src[self.pos] == '\n'))
            self.pos += 1;
    }

    fn peek(self: *Lexer) Token {
        var lex = self.*;
        return lex.next();
    }

    fn next(self: *Lexer) Token {
        self.skipWs();
        if (self.pos >= self.src.len) return .{ .kind = .eof, .text = "" };
        const start = self.pos;
        const ch = self.src[self.pos];

        // Single-char tokens
        switch (ch) {
            '(' => { self.pos += 1; return .{ .kind = .lparen,  .text = self.src[start..self.pos] }; },
            ')' => { self.pos += 1; return .{ .kind = .rparen,  .text = self.src[start..self.pos] }; },
            ',' => { self.pos += 1; return .{ .kind = .comma,   .text = self.src[start..self.pos] }; },
            '+' => { self.pos += 1; return .{ .kind = .plus,    .text = self.src[start..self.pos] }; },
            '-' => {
                self.pos += 1;
                if (self.pos < self.src.len and self.src[self.pos] == '>') {
                    self.pos += 1;
                    return .{ .kind = .arrow, .text = self.src[start..self.pos] };
                }
                return .{ .kind = .minus, .text = self.src[start..self.pos] };
            },
            '*' => { self.pos += 1; return .{ .kind = .star,    .text = self.src[start..self.pos] }; },
            '/' => { self.pos += 1; return .{ .kind = .slash,    .text = self.src[start..self.pos] }; },
            '%' => { self.pos += 1; return .{ .kind = .percent,  .text = self.src[start..self.pos] }; },
            '=' => { self.pos += 1; return .{ .kind = .eq,       .text = self.src[start..self.pos] }; },
            '[' => { self.pos += 1; return .{ .kind = .lbracket, .text = self.src[start..self.pos] }; },
            ']' => { self.pos += 1; return .{ .kind = .rbracket, .text = self.src[start..self.pos] }; },
            '!' => {
                self.pos += 1;
                if (self.pos < self.src.len and self.src[self.pos] == '=') self.pos += 1;
                return .{ .kind = .neq, .text = self.src[start..self.pos] };
            },
            '<' => {
                self.pos += 1;
                if (self.pos < self.src.len and self.src[self.pos] == '=') {
                    self.pos += 1; return .{ .kind = .lte, .text = self.src[start..self.pos] };
                }
                if (self.pos < self.src.len and self.src[self.pos] == '>') {
                    self.pos += 1; return .{ .kind = .neq, .text = self.src[start..self.pos] };
                }
                return .{ .kind = .lt, .text = self.src[start..self.pos] };
            },
            '>' => {
                self.pos += 1;
                if (self.pos < self.src.len and self.src[self.pos] == '=') {
                    self.pos += 1; return .{ .kind = .gte, .text = self.src[start..self.pos] };
                }
                return .{ .kind = .gt, .text = self.src[start..self.pos] };
            },
            '\'' => {
                // String literal: consume until closing '
                self.pos += 1;
                while (self.pos < self.src.len and self.src[self.pos] != '\'') self.pos += 1;
                if (self.pos < self.src.len) self.pos += 1; // consume closing '
                return .{ .kind = .str_lit, .text = self.src[start..self.pos] };
            },
            else => {},
        }

        // Number literal (integer or float)
        if (std.ascii.isDigit(ch) or (ch == '-' and self.pos + 1 < self.src.len and std.ascii.isDigit(self.src[self.pos + 1]))) {
            if (ch == '-') self.pos += 1;
            while (self.pos < self.src.len and std.ascii.isDigit(self.src[self.pos])) self.pos += 1;
            var is_float = false;
            if (self.pos < self.src.len and self.src[self.pos] == '.') {
                is_float = true;
                self.pos += 1;
                while (self.pos < self.src.len and std.ascii.isDigit(self.src[self.pos])) self.pos += 1;
            }
            // Optional exponent: e+N / e-N
            if (self.pos < self.src.len and (self.src[self.pos] == 'e' or self.src[self.pos] == 'E')) {
                is_float = true;
                self.pos += 1;
                if (self.pos < self.src.len and (self.src[self.pos] == '+' or self.src[self.pos] == '-')) self.pos += 1;
                while (self.pos < self.src.len and std.ascii.isDigit(self.src[self.pos])) self.pos += 1;
            }
            return .{ .kind = if (is_float) .num_float else .num_int, .text = self.src[start..self.pos] };
        }

        // Identifier: starts with letter or underscore, may contain digits and dots
        if (std.ascii.isAlphabetic(ch) or ch == '_') {
            while (self.pos < self.src.len and
                   (std.ascii.isAlphanumeric(self.src[self.pos]) or
                    self.src[self.pos] == '_' or self.src[self.pos] == '.'))
                self.pos += 1;
            const word = self.src[start..self.pos];
            if (std.ascii.eqlIgnoreCase(word, "AND"))     return .{ .kind = .kw_and,     .text = word };
            if (std.ascii.eqlIgnoreCase(word, "OR"))      return .{ .kind = .kw_or,      .text = word };
            if (std.ascii.eqlIgnoreCase(word, "NOT"))     return .{ .kind = .kw_not,     .text = word };
            if (std.ascii.eqlIgnoreCase(word, "CAST"))    return .{ .kind = .kw_cast,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "AS"))      return .{ .kind = .kw_as,      .text = word };
            if (std.ascii.eqlIgnoreCase(word, "CASE"))    return .{ .kind = .kw_case,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "WHEN"))    return .{ .kind = .kw_when,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "THEN"))    return .{ .kind = .kw_then,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "ELSE"))    return .{ .kind = .kw_else,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "END"))     return .{ .kind = .kw_end,     .text = word };
            if (std.ascii.eqlIgnoreCase(word, "IS"))      return .{ .kind = .kw_is,      .text = word };
            if (std.ascii.eqlIgnoreCase(word, "NULL"))    return .{ .kind = .kw_null,    .text = word };
            if (std.ascii.eqlIgnoreCase(word, "BETWEEN")) return .{ .kind = .kw_between, .text = word };
            if (std.ascii.eqlIgnoreCase(word, "IN"))      return .{ .kind = .kw_in,      .text = word };
            return .{ .kind = .ident, .text = word };
        }

        // Unknown — advance one byte and return eof-equivalent
        self.pos += 1;
        return .{ .kind = .eof, .text = self.src[start..self.pos] };
    }
};

/// Normalize a function name (which may be lowercase from duckdb AST) to the
/// canonical casing used by kernels.zig. Falls back to the original if unknown.
fn isDictFn(name: []const u8) bool {
    return std.ascii.eqlIgnoreCase(name, "dictHas") or
           std.ascii.eqlIgnoreCase(name, "dictGet") or
           std.ascii.eqlIgnoreCase(name, "dictGetOrDefault") or
           std.ascii.eqlIgnoreCase(name, "dictGetOrNull");
}

fn canonFnName(name: []const u8) []const u8 {
    const canon_names = [_][]const u8{
        "lower", "upper", "length", "char_length", "lowerUTF8", "upperUTF8",
        "toDate", "toDateOrZero", "toYYYYMMDD", "toUnixTimestamp", "toFloat64",
        "toUInt64", "toInt64", "toString", "toStartOfMinute", "toStartOfHour",
        "toStartOfDay", "toStartOfWeek", "toStartOfMonth", "toStartOfYear",
        "toYear", "toMonth", "toDayOfMonth", "toDayOfWeek", "toHour", "toMinute", "toSecond",
        "abs", "round", "floor", "ceil", "log", "log2", "log10", "sqrt", "exp",
        "not", "isNull", "isNotNull", "isIPv4String", "isIPv6String",
        "IPv4StringToNumOrDefault", "IPv4NumToString",
        "IPv6StringToNumOrDefault", "IPv6NumToString",
        "greatest", "least", "intDiv", "modulo",
        "positionCaseInsensitive", "splitByChar", "concat",
        "if", "multiIf",
        "substring", "substr", "startsWith", "endsWith",
        "mapGet",
        "has", "hasAny", "hasAll",
        "arrayConcat", "arrayDistinct", "arrayFlatten", "arrayReverse",
        "arraySlice", "arrayMax", "arrayMin",
        "arrayMap", "arrayFilter", "arrayExists",
        "arrayJoin",
        "mapKeys", "mapValues",
        "tuple",
        "regexp_replace", "replaceRegexpOne",
    };
    for (canon_names) |cn| {
        if (std.ascii.eqlIgnoreCase(cn, name)) return cn;
    }
    return name;
}

/// Operator precedence for binary infix operators (Pratt binding power).
/// Higher number = binds tighter.
fn infixBP(kind: TokKind) ?u8 {
    return switch (kind) {
        .kw_or                           =>  5,
        .kw_and                          =>  7,
        .eq, .neq, .lt, .lte, .gt, .gte =>  9,
        .plus, .minus                    => 10,
        .star, .slash, .percent          => 20,
        else => null,
    };
}

/// Parse state threaded through Pratt calls.
const ParseCtx = struct {
    lex: Lexer,
    arena: std.mem.Allocator,
    plan_ctx: *PlannerCtx,
    /// Current lambda parameter name (e.g. "x"), set while parsing lambda body.
    /// References to this name compile to Expr.lambda_param.
    lambda_param: ?[]const u8 = null,
};

/// Entry: try to parse `text` as a complete arithmetic/function expression.
/// Returns null if parsing fails or doesn't consume the whole string.
fn parseArithExpr(ctx: *PlannerCtx, text: []const u8) !?Expr {
    var pctx = ParseCtx{ .lex = Lexer.init(text), .arena = ctx.alloc, .plan_ctx = ctx };
    const expr = try prattExpr(&pctx, 0) orelse return null;
    // Must have consumed the entire input (ignoring whitespace).
    pctx.lex.skipWs();
    if (pctx.lex.pos != pctx.lex.src.len) return null; // trailing garbage
    return expr;
}

/// Pratt parser: parse an expression with minimum binding power `min_bp`.
fn prattExpr(pctx: *ParseCtx, min_bp: u8) anyerror!?Expr {
    // ── NUD (prefix) ─────────────────────────────────────────────────────────
    var tok = pctx.lex.next();

    var lhs: Expr = switch (tok.kind) {
        // Unary minus: -(expr)
        .minus => blk_unary: {
            const rhs = try prattExpr(pctx, 25) orelse return null; // tight BP
            const bp = try pctx.arena.create(plan.BinOp);
            bp.* = .{ .left = Expr{ .lit_i64 = 0 }, .right = rhs };
            break :blk_unary Expr{ .sub = bp };
        },

        // Unary NOT
        .kw_not => blk_not: {
            const rhs = try prattExpr(pctx, 8) orelse return null;
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 1);
            fc_args[0] = rhs;
            fc.* = .{ .name = "not", .args = fc_args };
            break :blk_not Expr{ .fn_call = fc };
        },

        // CAST(expr AS type) → map to a known conversion function
        .kw_cast => blk_cast: {
            const lp = pctx.lex.next();
            if (lp.kind != .lparen) return null;
            const inner = try prattExpr(pctx, 0) orelse return null;
            // CAST supports two forms:
            //   CAST(expr AS type)       — SQL standard
            //   CAST(expr, 'type')       — ClickHouse shorthand
            const sep_tok = pctx.lex.next();
            const type_name: []const u8 = switch (sep_tok.kind) {
                .kw_as => blk_as: {
                    const type_tok = pctx.lex.next();
                    const rp = pctx.lex.next();
                    if (rp.kind != .rparen) return null;
                    break :blk_as switch (type_tok.kind) {
                        .ident   => type_tok.text,
                        .str_lit => if (type_tok.text.len >= 2) type_tok.text[1..type_tok.text.len - 1] else type_tok.text,
                        else     => return null,
                    };
                },
                .comma => blk_comma: {
                    const type_tok = pctx.lex.next();
                    const rp = pctx.lex.next();
                    if (rp.kind != .rparen) return null;
                    break :blk_comma switch (type_tok.kind) {
                        .str_lit => if (type_tok.text.len >= 2) type_tok.text[1..type_tok.text.len - 1] else type_tok.text,
                        .ident   => type_tok.text,
                        else     => return null,
                    };
                },
                else => return null,
            };
            // CAST(x, 'Array(String)') or CAST(x AS LIST) → pass inner expression through as-is
            if (std.mem.startsWith(u8, type_name, "Array(") or
                std.ascii.eqlIgnoreCase(type_name, "LIST") or
                std.ascii.eqlIgnoreCase(type_name, "LIST[]") or
                std.mem.endsWith(u8, type_name, "[]")) {
                break :blk_cast inner;
            }
            // Map target type to a kernels function
            const fn_name: []const u8 = if (std.ascii.eqlIgnoreCase(type_name, "DATE"))
                "toDate"
            else if (std.ascii.eqlIgnoreCase(type_name, "VARCHAR") or
                     std.ascii.eqlIgnoreCase(type_name, "String"))
                "toString"
            else
                return null; // unsupported cast type
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 1);
            fc_args[0] = inner;
            fc.* = .{ .name = fn_name, .args = fc_args };
            break :blk_cast Expr{ .fn_call = fc };
        },

        // Parenthesised sub-expression
        .lparen => blk_paren: {
            const inner = try prattExpr(pctx, 0) orelse return null;
            const close = pctx.lex.next();
            if (close.kind != .rparen) return null;
            break :blk_paren inner;
        },

        // NULL literal → lit_i64 0 (treated as null/zero in kernels)
        .kw_null => Expr{ .lit_null = {} },

        // Array literal: ['a', 'b', ...] — only string elements supported
        .lbracket => blk_arr: {
            // empty array: [] → lit_array &.{}
            if (pctx.lex.peek().kind == .rbracket) {
                _ = pctx.lex.next();
                break :blk_arr Expr{ .lit_array = &.{} };
            }
            // Parse elements as full Pratt expressions.
            // Pure string-literal arrays → lit_array.
            // Arrays with column refs / fn_calls → arrayConcat(e1,e2,…).
            var str_elems: std.ArrayListUnmanaged([]const u8) = .empty;
            var expr_elems: std.ArrayListUnmanaged(Expr) = .empty;
            var all_str = true;
            while (true) {
                const elem = try prattExpr(pctx, 1) orelse {
                    all_str = false;
                    break;
                };
                switch (elem) {
                    .lit_str => |s| try str_elems.append(pctx.arena, s),
                    else     => all_str = false,
                }
                try expr_elems.append(pctx.arena, elem);
                const sep = pctx.lex.peek();
                if (sep.kind == .comma) {
                    _ = pctx.lex.next();
                } else {
                    break;
                }
            }
            const rb = pctx.lex.next();
            if (rb.kind != .rbracket) return null;
            if (expr_elems.items.len == 0) break :blk_arr Expr{ .lit_array = &.{} };
            if (all_str) {
                const arr = try str_elems.toOwnedSlice(pctx.arena);
                break :blk_arr Expr{ .lit_array = arr };
            }
            // Column-ref or mixed → arrayConcat(e1, e2, …)
            const fc = try pctx.arena.create(plan.FnCall);
            const args = try expr_elems.toOwnedSlice(pctx.arena);
            fc.* = .{ .name = "arrayConcat", .args = args };
            break :blk_arr Expr{ .fn_call = fc };
        },

        // CASE [WHEN cond THEN val]… [ELSE val] END → multiIf(cond1,val1,…,else)
        .kw_case => blk_case: {
            var arms: std.ArrayListUnmanaged(Expr) = .empty;
            while (true) {
                const w = pctx.lex.peek();
                if (w.kind == .kw_when) {
                    _ = pctx.lex.next(); // consume WHEN
                    const cond = try prattExpr(pctx, 0) orelse return null;
                    const th = pctx.lex.next();
                    if (th.kind != .kw_then) return null;
                    const val = try prattExpr(pctx, 0) orelse return null;
                    try arms.append(pctx.arena, cond);
                    try arms.append(pctx.arena, val);
                } else if (w.kind == .kw_else) {
                    _ = pctx.lex.next(); // consume ELSE
                    const val = try prattExpr(pctx, 0) orelse return null;
                    try arms.append(pctx.arena, val);
                    break;
                } else if (w.kind == .kw_end) {
                    break;
                } else {
                    return null;
                }
            }
            const end = pctx.lex.next();
            if (end.kind != .kw_end) return null;
            if (arms.items.len == 0) return null;
            const args_slice = try arms.toOwnedSlice(pctx.arena);
            const fn_name: []const u8 = if (args_slice.len == 3) "if" else "multiIf";
            const fc = try pctx.arena.create(plan.FnCall);
            fc.* = .{ .name = fn_name, .args = args_slice };
            break :blk_case Expr{ .fn_call = fc };
        },

        // Integer literal
        .num_int => blk: {
            const v = std.fmt.parseInt(i64, tok.text, 10) catch return null;
            break :blk Expr{ .lit_i64 = v };
        },

        // Float literal
        .num_float => blk: {
            const v = std.fmt.parseFloat(f64, tok.text) catch return null;
            break :blk Expr{ .lit_f64 = v };
        },

        // String literal: 'value'  → lit_str (strip quotes)
        .str_lit => blk: {
            const s = if (tok.text.len >= 2) tok.text[1..tok.text.len - 1] else tok.text;
            break :blk Expr{ .lit_str = s };
        },

        // Identifier: either a lambda (x -> body), function call fn(…), or column/literal
        .ident => blk: {
            const name = tok.text;
            // Look ahead: is next token '->'?  → lambda expression
            if (pctx.lex.peek().kind == .arrow) {
                _ = pctx.lex.next(); // consume '->'
                // Parse body with lambda_param bound to `name`
                const saved_param = pctx.lambda_param;
                pctx.lambda_param = name;
                const body_expr = try prattExpr(pctx, 0) orelse return null;
                pctx.lambda_param = saved_param;
                const body_ptr = try pctx.arena.create(Expr);
                body_ptr.* = body_expr;
                break :blk Expr{ .lambda = .{ .param = name, .body = body_ptr } };
            }
            // Look ahead: is next token '('?  → function call
            const next = pctx.lex.peek();
            if (next.kind == .lparen) {
                _ = pctx.lex.next(); // consume '('
                // Parse argument list
                var args: std.ArrayListUnmanaged(Expr) = .empty;
                // Check for zero-arg call or count(*) star arg
                const first_peek = pctx.lex.peek();
                if (first_peek.kind == .star and std.ascii.eqlIgnoreCase(name, "count")) {
                    // count(*) → treat as zero-arg call (will be recognized as count_star)
                    _ = pctx.lex.next(); // consume '*'
                    const close_star = pctx.lex.next();
                    if (close_star.kind != .rparen) return null;
                    const args_slice = try args.toOwnedSlice(pctx.arena);
                    const fc = try pctx.arena.create(plan.FnCall);
                    fc.* = .{ .name = "count_star", .args = args_slice };
                    break :blk Expr{ .fn_call = fc };
                }
                if (first_peek.kind != .rparen) {
                    while (true) {
                        const arg = try prattExpr(pctx, 0) orelse return null;
                        try args.append(pctx.arena, arg);
                        const sep = pctx.lex.peek();
                        if (sep.kind == .comma) {
                            _ = pctx.lex.next();
                        } else {
                            break;
                        }
                    }
                }
                const close = pctx.lex.next();
                if (close.kind != .rparen) return null;

                const args_slice = try args.toOwnedSlice(pctx.arena);

                // date_trunc special-case: first arg is string literal unit
                if (std.mem.eql(u8, name, "date_trunc") and args_slice.len == 2) {
                    const unit_expr = args_slice[0];
                    const unit_str: []const u8 = switch (unit_expr) {
                        .lit_str => |s| s,
                        else => return null,
                    };
                    for (date_trunc_units) |dtu| {
                        if (std.mem.eql(u8, dtu.unit, unit_str)) {
                            const fc = try pctx.arena.create(plan.FnCall);
                            const fc_args = try pctx.arena.alloc(Expr, 1);
                            fc_args[0] = args_slice[1];
                            fc.* = .{ .name = dtu.fn_name, .args = fc_args };
                            break :blk Expr{ .fn_call = fc };
                        }
                    }
                    return null;
                }

                // date_part special-case: date_part('unit', col) → fn_call "date_part"
                if (std.mem.eql(u8, name, "date_part") and args_slice.len == 2) {
                    const fc = try pctx.arena.create(plan.FnCall);
                    const fc_args = try pctx.arena.alloc(Expr, 2);
                    fc_args[0] = args_slice[0];
                    fc_args[1] = args_slice[1];
                    fc.* = .{ .name = "date_part", .args = fc_args };
                    break :blk Expr{ .fn_call = fc };
                }

                // Dict function calls: dictHas(dict, key), dictGet(dict, attr, key),
                // dictGetOrDefault(dict, attr, key, default), dictGetOrNull(dict, attr, key)
                // Keys may be wrapped in tuple(...) — unwrap if so.
                if (isDictFn(name) and args_slice.len >= 2) {
                    // args[0] must be string literal → dict name
                    const dict_name: []const u8 = switch (args_slice[0]) {
                        .lit_str => |s| s,
                        else => return null,
                    };

                    // Helper: unwrap tuple(a,b,...) → []Expr, else single-element slice
                    const unwrapKeys = struct {
                        fn do(arena: std.mem.Allocator, key_expr: Expr) ![]Expr {
                            switch (key_expr) {
                                .fn_call => |fc| {
                                    if (std.ascii.eqlIgnoreCase(fc.name, "tuple")) {
                                        return fc.args;
                                    }
                                },
                                else => {},
                            }
                            const s = try arena.alloc(Expr, 1);
                            s[0] = key_expr;
                            return s;
                        }
                    }.do;

                     // Helper: resolve CAST([], 'Array(String)') → lit_array &.{}
                     // The Pratt parser now handles CAST([], ...) → lit_array directly.
                     // This is a pass-through; kept for any fallback cases.
                     const resolveDefault = struct {
                         fn do(e: Expr) Expr {
                             return e;
                         }
                     }.do;

                    const dc = try pctx.arena.create(plan.DictCall);
                    if (std.ascii.eqlIgnoreCase(name, "dictHas")) {
                        // dictHas(dict, key_or_tuple)
                        const keys = try unwrapKeys(pctx.arena, args_slice[1]);
                        dc.* = .{
                            .fn_name      = "dictHas",
                            .dict_name    = dict_name,
                            .attr_name    = null,
                            .keys         = keys,
                            .default_expr = null,
                        };
                    } else if (std.ascii.eqlIgnoreCase(name, "dictGetOrDefault") and args_slice.len >= 4) {
                        // dictGetOrDefault(dict, attr, key_or_tuple, default)
                        const attr: []const u8 = switch (args_slice[1]) {
                            .lit_str => |s| s,
                            else => return null,
                        };
                        const keys = try unwrapKeys(pctx.arena, args_slice[2]);
                        const default_e = resolveDefault(args_slice[args_slice.len - 1]);
                        dc.* = .{
                            .fn_name      = "dictGetOrDefault",
                            .dict_name    = dict_name,
                            .attr_name    = attr,
                            .keys         = keys,
                            .default_expr = default_e,
                        };
                    } else {
                        // dictGet / dictGetOrNull(dict, attr, key_or_tuple)
                        const attr: []const u8 = if (args_slice.len >= 3) switch (args_slice[1]) {
                            .lit_str => |s| s,
                            else => return null,
                        } else "";
                        const key_idx: usize = if (args_slice.len >= 3) 2 else 1;
                        const keys = try unwrapKeys(pctx.arena, args_slice[key_idx]);
                        dc.* = .{
                            .fn_name      = name,
                            .dict_name    = dict_name,
                            .attr_name    = attr,
                            .keys         = keys,
                            .default_expr = null,
                        };
                    }
                    break :blk Expr{ .dict_call = dc };
                }

                // list_value(...) — DuckDB internal name for array constructors []
                // list_value() with no args → empty array literal
                // list_value('a', 'b', ...) → lit_array with string values
                if (std.ascii.eqlIgnoreCase(name, "list_value")) {
                    if (args_slice.len == 0) {
                        const items = try pctx.arena.alloc([]const u8, 0);
                        break :blk Expr{ .lit_array = items };
                    }
                    // Build lit_array from string literal args
                    const str_items = try pctx.arena.alloc([]const u8, args_slice.len);
                    for (args_slice, 0..) |arg, ai| {
                        switch (arg) {
                            .lit_str => |s| str_items[ai] = s,
                            .lit_i64 => |n| str_items[ai] = try std.fmt.allocPrint(pctx.arena, "{d}", .{n}),
                            .lit_u64 => |n| str_items[ai] = try std.fmt.allocPrint(pctx.arena, "{d}", .{n}),
                            else => return null,
                        }
                    }
                    break :blk Expr{ .lit_array = str_items };
                }

                // Verify function is in known scalar_fns or 2-arg numerics
                const is_known = blk2: {
                    for (scalar_fns) |sf| {
                        if (std.ascii.eqlIgnoreCase(sf.name, name) and args_slice.len == 1) break :blk2 true;
                    }
                    if (args_slice.len == 2) {
                        if (std.ascii.eqlIgnoreCase(name, "greatest") or
                            std.ascii.eqlIgnoreCase(name, "least") or
                            std.ascii.eqlIgnoreCase(name, "intDiv") or
                            std.ascii.eqlIgnoreCase(name, "modulo") or
                            std.ascii.eqlIgnoreCase(name, "positionCaseInsensitive") or
                            std.ascii.eqlIgnoreCase(name, "splitByChar") or
                            std.ascii.eqlIgnoreCase(name, "startsWith") or
                            std.ascii.eqlIgnoreCase(name, "endsWith") or
                            std.ascii.eqlIgnoreCase(name, "mapGet") or
                            std.ascii.eqlIgnoreCase(name, "risk_score")) break :blk2 true;
                    }
                    if (args_slice.len == 3) {
                        if (std.ascii.eqlIgnoreCase(name, "if") or
                            std.ascii.eqlIgnoreCase(name, "substring") or
                            std.ascii.eqlIgnoreCase(name, "substr") or
                            std.ascii.eqlIgnoreCase(name, "regexp_replace") or
                            std.ascii.eqlIgnoreCase(name, "replaceRegexpOne")) break :blk2 true;
                    }
                    // substring/substr with 2 args (no length) — from position to end
                    if (args_slice.len == 2 and (
                        std.ascii.eqlIgnoreCase(name, "substring") or
                        std.ascii.eqlIgnoreCase(name, "substr"))) break :blk2 true;
                    if (args_slice.len >= 2 and std.ascii.eqlIgnoreCase(name, "concat")) break :blk2 true;
                    if (args_slice.len >= 1 and std.ascii.eqlIgnoreCase(name, "tuple")) break :blk2 true;
                    if (args_slice.len >= 3 and std.ascii.eqlIgnoreCase(name, "multiIf")) break :blk2 true;
                    // Array functions (2-arg)
                    if (args_slice.len == 2 and (
                        std.ascii.eqlIgnoreCase(name, "has") or
                        std.ascii.eqlIgnoreCase(name, "hasAny") or
                        std.ascii.eqlIgnoreCase(name, "hasAll") or
                        std.ascii.eqlIgnoreCase(name, "arrayMax") or
                        std.ascii.eqlIgnoreCase(name, "arrayMin"))) break :blk2 true;
                    // arrayConcat: 2 or more args
                    if (args_slice.len >= 2 and std.ascii.eqlIgnoreCase(name, "arrayConcat")) break :blk2 true;
                    // arrayDistinct: 1 or 2 args
                    if (args_slice.len >= 1 and args_slice.len <= 2 and
                        std.ascii.eqlIgnoreCase(name, "arrayDistinct")) break :blk2 true;
                    if (args_slice.len == 1 and (
                        std.ascii.eqlIgnoreCase(name, "mapKeys") or
                        std.ascii.eqlIgnoreCase(name, "mapValues") or
                        std.ascii.eqlIgnoreCase(name, "arrayFlatten") or
                        std.ascii.eqlIgnoreCase(name, "arrayJoin") or
                        std.ascii.eqlIgnoreCase(name, "arrayMax") or
                        std.ascii.eqlIgnoreCase(name, "arrayMin"))) break :blk2 true;
                    // Lambda-based array functions: arrayMap(x -> expr, arr), arrayFilter, arrayExists
                    if (args_slice.len == 2 and (
                        std.ascii.eqlIgnoreCase(name, "arrayMap") or
                        std.ascii.eqlIgnoreCase(name, "arrayFilter") or
                        std.ascii.eqlIgnoreCase(name, "arrayExists")) and
                        args_slice[0] == .lambda) break :blk2 true;
                    break :blk2 false;
                };
                if (!is_known) return null;

                const fc = try pctx.arena.create(plan.FnCall);
                // Normalize the function name to canonical casing that kernels.zig expects.
                var canon = canonFnName(name);
                // mapKeys/mapValues on Map(String,Float64) → use Float64 variant
                if (std.mem.eql(u8, canon, "mapKeys") and args_slice.len == 1) {
                    if (args_slice[0] == .col_ref) {
                        if (pctx.plan_ctx.tbl) |tbl| {
                            if (tbl.findColumn(args_slice[0].col_ref.name)) |idx| {
                                if (tbl.columns[idx].ch_type) |ch| {
                                    if (std.mem.startsWith(u8, ch, "Map(") and
                                        (std.mem.indexOf(u8, ch, "Float64") != null or
                                         std.mem.indexOf(u8, ch, "Float32") != null))
                                        canon = "mapKeysFloat64";
                                }
                            }
                        }
                    }
                }
                fc.* = .{ .name = canon, .args = args_slice };
                break :blk Expr{ .fn_call = fc };
            }

            // Not a function call — check if it's the lambda param, then resolve as column ref or literal
            if (pctx.lambda_param) |lp| {
                if (std.mem.eql(u8, name, lp)) break :blk Expr{ .lambda_param = {} };
            }
            break :blk resolveColExpr(pctx.plan_ctx, name) orelse return null;
        },

        else => return null,
    };

    // ── LED (infix) ───────────────────────────────────────────────────────────
    while (true) {
        const op = pctx.lex.peek();

        // IS [NOT] NULL  →  isNull(lhs) or isNotNull(lhs)
        if (op.kind == .kw_is) {
            _ = pctx.lex.next(); // consume IS
            const nx = pctx.lex.peek();
            const is_not = nx.kind == .kw_not;
            if (is_not) _ = pctx.lex.next();
            const nl = pctx.lex.next();
            if (nl.kind != .kw_null) return null;
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 1);
            fc_args[0] = lhs;
            fc.* = .{ .name = if (is_not) "isNotNull" else "isNull", .args = fc_args };
            lhs = Expr{ .fn_call = fc };
            continue;
        }

        // BETWEEN lo AND hi  →  (lhs >= lo AND lhs <= hi)
        if (op.kind == .kw_between) {
            _ = pctx.lex.next(); // consume BETWEEN
            const lo = try prattExpr(pctx, 8) orelse return null; // above AND BP
            const kw = pctx.lex.next();
            if (kw.kind != .kw_and) return null;
            const hi = try prattExpr(pctx, 8) orelse return null;
            // lhs >= lo
            const bp_gte = try pctx.arena.create(plan.BinOp);
            bp_gte.* = .{ .left = lhs, .right = lo };
            // lhs <= hi  (duplicate lhs — share the value)
            const bp_lte = try pctx.arena.create(plan.BinOp);
            bp_lte.* = .{ .left = lhs, .right = hi };
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 2);
            fc_args[0] = Expr{ .gte = bp_gte };
            fc_args[1] = Expr{ .lte = bp_lte };
            fc.* = .{ .name = "and", .args = fc_args };
            lhs = Expr{ .fn_call = fc };
            continue;
        }

        // MAP SUBSCRIPT: lhs['key']  →  mapGet(lhs, key) or mapGetFloat64(lhs, key)
        if (op.kind == .lbracket) {
            _ = pctx.lex.next(); // consume '['
            const key_tok = pctx.lex.next();
            if (key_tok.kind != .str_lit) return null;
            const rb = pctx.lex.next();
            if (rb.kind != .rbracket) return null;
            const key_str = if (key_tok.text.len >= 2) key_tok.text[1..key_tok.text.len - 1] else key_tok.text;
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 2);
            fc_args[0] = lhs;
            fc_args[1] = Expr{ .lit_str = key_str };
            // Choose function based on map value type
            const fn_name = blk: {
                if (lhs == .col_ref) {
                    if (pctx.plan_ctx.tbl) |tbl| {
                        if (tbl.findColumn(lhs.col_ref.name)) |idx| {
                            if (tbl.columns[idx].ch_type) |ch| {
                                if (std.mem.startsWith(u8, ch, "Map(") and
                                    (std.mem.indexOf(u8, ch, "Float64") != null or
                                     std.mem.indexOf(u8, ch, "Float32") != null))
                                    break :blk "mapGetFloat64";
                            }
                        }
                    }
                }
                break :blk "mapGet";
            };
            fc.* = .{ .name = fn_name, .args = fc_args };
            lhs = Expr{ .fn_call = fc };
            continue;
        }

        const bp = infixBP(op.kind) orelse break;
        if (bp <= min_bp) break;

        _ = pctx.lex.next(); // consume operator
        const rhs = try prattExpr(pctx, bp) orelse return null;

        // AND/OR: use fn_call representation (no BinOp needed)
        if (op.kind == .kw_and or op.kind == .kw_or) {
            const fc = try pctx.arena.create(plan.FnCall);
            const fc_args = try pctx.arena.alloc(Expr, 2);
            fc_args[0] = lhs;
            fc_args[1] = rhs;
            fc.* = .{ .name = if (op.kind == .kw_and) "and" else "or", .args = fc_args };
            lhs = Expr{ .fn_call = fc };
            continue;
        }

        const binop = try pctx.arena.create(plan.BinOp);
        binop.* = .{ .left = lhs, .right = rhs };

        lhs = switch (op.kind) {
            .plus    => Expr{ .add = binop },
            .minus   => Expr{ .sub = binop },
            .star    => Expr{ .mul = binop },
            .slash   => Expr{ .div = binop },
            .percent => Expr{ .mod = binop },
            .eq      => Expr{ .eq  = binop },
            .neq     => Expr{ .neq = binop },
            .lt      => Expr{ .lt  = binop },
            .lte     => Expr{ .lte = binop },
            .gt      => Expr{ .gt  = binop },
            .gte     => Expr{ .gte = binop },
            else     => unreachable,
        };
    }

    return lhs;
}

/// Infer the output ColumnType of an Expr tree (best-effort, conservative).
fn inferExprType(ctx: *PlannerCtx, expr: Expr) ColumnType {
    return switch (expr) {
        .lit_i64, .lit_u64 => .int64,
        .lit_f64            => .float64,
        .lit_str            => .string,
        .lit_bool           => .bool_u8,
        .lit_null           => .int64,
        .lit_array          => .array_string,
        .col_ref => |ref| schemaColType(ctx, ref.name),
        .add, .sub, .mul => |op| {
            const lt = inferExprType(ctx, op.left);
            const rt = inferExprType(ctx, op.right);
            if (lt == .float64 or rt == .float64) return .float64;
            return .int64;
        },
        .div => .float64,  // integer division may produce fraction
        .mod => .int64,
        // Comparison operators yield bool_u8
        .eq, .neq, .lt, .lte, .gt, .gte => .bool_u8,
        .fn_call => |fc| {
            for (scalar_fns) |sf| {
                if (std.mem.eql(u8, sf.name, fc.name)) return sf.out;
            }
            if (std.mem.eql(u8, fc.name, "greatest") or std.mem.eql(u8, fc.name, "least")) return .float64;
            if (std.mem.eql(u8, fc.name, "intDiv") or std.mem.eql(u8, fc.name, "modulo")) return .int64;
            if (std.mem.eql(u8, fc.name, "positionCaseInsensitive")) return .uint64;
            if (std.mem.eql(u8, fc.name, "concat")) return .string;
            if (std.mem.eql(u8, fc.name, "risk_score")) return .float64;
            if (std.mem.eql(u8, fc.name, "splitByChar") or
                std.mem.eql(u8, fc.name, "splitByString") or
                std.mem.eql(u8, fc.name, "mapKeys") or
                std.mem.eql(u8, fc.name, "mapKeysFloat64") or
                std.mem.eql(u8, fc.name, "mapValues") or
                std.mem.eql(u8, fc.name, "arrayConcat") or
                std.mem.eql(u8, fc.name, "arrayDistinct") or
                std.mem.eql(u8, fc.name, "arrayFlatten")) return .array_string;
            if (std.mem.eql(u8, fc.name, "has") or
                std.mem.eql(u8, fc.name, "hasAny") or
                std.mem.eql(u8, fc.name, "hasAll")) return .bool_u8;
            if (std.mem.eql(u8, fc.name, "substring") or std.mem.eql(u8, fc.name, "substr")) return .string;
            if (std.mem.eql(u8, fc.name, "startsWith") or std.mem.eql(u8, fc.name, "endsWith")) return .bool_u8;
            if (std.mem.eql(u8, fc.name, "mapGet")) return .string;
            if (std.mem.eql(u8, fc.name, "mapGetFloat64")) return .float64;
            if (std.mem.eql(u8, fc.name, "arrayExists")) return .bool_u8;
            if (std.mem.eql(u8, fc.name, "arrayJoin")) return .string;
            if (std.mem.eql(u8, fc.name, "arrayMap") or std.mem.eql(u8, fc.name, "arrayFilter")) return .array_string;
            if (std.mem.eql(u8, fc.name, "and") or std.mem.eql(u8, fc.name, "or")) return .uint64;
            if (std.mem.eql(u8, fc.name, "if") or std.mem.eql(u8, fc.name, "multiIf")) {
                if (fc.args.len >= 2) return inferExprType(ctx, fc.args[1]);
            }
            return .float64;
        },
        .dict_call => |dc| {
            if (std.ascii.eqlIgnoreCase(dc.fn_name, "dictHas")) return .bool_u8;
            // If the default expression is an array, the result is array_string
            if (dc.default_expr) |de| {
                const dt = inferExprType(ctx, de);
                if (dt == .array_string) return .array_string;
            }
            return .string;
        },
        else => .float64,
    };
}

/// Try to parse `text` as `fn_name(arg)` where fn_name is in scalar_fns and
/// arg resolves to a column or literal via resolveColExpr.
/// Also handles date_trunc('unit', col).
/// Returns a ready ProjectItem on success, null if not parseable.
fn tryParseFnCallItem(ctx: *PlannerCtx, text: []const u8, alias: []const u8) !?ProjectItem {
    // Delegate to the full Pratt expression parser first.
    if (try parseArithExpr(ctx, text)) |expr| {
        // Infer output type from the expression.
        const out_type = inferExprType(ctx, expr);
        return ProjectItem{ .expr = expr, .alias = alias, .out_type = out_type };
    }

    // Find opening paren — must not be the first char.
    const paren_open = std.mem.indexOfScalar(u8, text, '(') orelse return null;
    if (paren_open == 0) return null;
    // Must end with ')'
    if (text[text.len - 1] != ')') return null;

    const fn_name = text[0..paren_open];
    const args_text = text[paren_open + 1 .. text.len - 1];

    // ── date_trunc('unit', col) ───────────────────────────────────────────────
    if (std.mem.eql(u8, fn_name, "date_trunc")) {
        // args_text looks like: 'hour', timestamp
        const comma = std.mem.indexOfScalar(u8, args_text, ',') orelse return null;
        const unit_raw = std.mem.trim(u8, args_text[0..comma], " \t'\"");
        const col_raw  = std.mem.trim(u8, args_text[comma + 1..], " \t");
        // col_raw must be a simple column (no parens)
        if (std.mem.indexOfScalar(u8, col_raw, '(') != null) return null;
        const arg_expr = resolveColExpr(ctx, col_raw) orelse return null;
        for (date_trunc_units) |dtu| {
            if (std.mem.eql(u8, dtu.unit, unit_raw)) {
                const fc = try ctx.alloc.create(plan.FnCall);
                const fc_args = try ctx.alloc.alloc(Expr, 1);
                fc_args[0] = arg_expr;
                fc.* = .{ .name = dtu.fn_name, .args = fc_args };
                return ProjectItem{ .expr = .{ .fn_call = fc }, .alias = alias, .out_type = dtu.out };
            }
        }
        return null;
    }

    // ── Two-argument numeric functions: greatest(col, N), least(col, N) ─────────
    // Pattern: fn_name(col_or_lit, number_literal) — no nested parens in either arg.
    if (std.mem.eql(u8, fn_name, "greatest") or
        std.mem.eql(u8, fn_name, "least") or
        std.mem.eql(u8, fn_name, "intDiv") or
        std.mem.eql(u8, fn_name, "modulo"))
    {
        if (std.mem.indexOfScalar(u8, args_text, '(') == null) {
            const comma = std.mem.indexOfScalar(u8, args_text, ',') orelse return null;
            const a0_raw = std.mem.trim(u8, args_text[0..comma], " \t");
            const a1_raw = std.mem.trim(u8, args_text[comma+1..], " \t");
            if (std.mem.indexOfScalar(u8, a1_raw, ',') == null) {
                const a0_expr = resolveColExpr(ctx, a0_raw) orelse return null;
                const a1_expr = resolveColExpr(ctx, a1_raw) orelse return null;
                const fc = try ctx.alloc.create(plan.FnCall);
                const fc_args = try ctx.alloc.alloc(Expr, 2);
                fc_args[0] = a0_expr;
                fc_args[1] = a1_expr;
                fc.* = .{ .name = fn_name, .args = fc_args };
                const out: ColumnType = if (std.mem.eql(u8, fn_name, "intDiv") or
                    std.mem.eql(u8, fn_name, "modulo")) .int64 else .float64;
                return ProjectItem{ .expr = .{ .fn_call = fc }, .alias = alias, .out_type = out };
            }
        }
        return null;
    }

    // ── Single-argument functions ─────────────────────────────────────────────
    // Reject if arg contains nested parens or commas (multi-arg).
    if (std.mem.indexOfScalar(u8, args_text, '(') != null) return null;
    if (std.mem.indexOfScalar(u8, args_text, ',') != null) return null;

    const arg_text = std.mem.trim(u8, args_text, " \t");

    // Lookup in known scalar functions table (case-sensitive, matching kernels.zig).
    var out_type: ColumnType = .string;
    var found = false;
    for (scalar_fns) |sf| {
        if (std.mem.eql(u8, sf.name, fn_name)) {
            out_type = sf.out;
            found = true;
            break;
        }
    }
    if (!found) return null;

    // Resolve the argument.
    const arg_expr = resolveColExpr(ctx, arg_text) orelse return null;

    // Build FnCall node.
    const fc = try ctx.alloc.create(plan.FnCall);
    const fc_args = try ctx.alloc.alloc(Expr, 1);
    fc_args[0] = arg_expr;
    fc.* = .{ .name = fn_name, .args = fc_args };

    return ProjectItem{
        .expr     = .{ .fn_call = fc },
        .alias    = alias,
        .out_type = out_type,
    };
}


/// Build an agg ProjectItem where the aggregate argument is looked up in the
/// pre-project output (used for ARRAY JOIN + GROUP BY queries).
/// `pre_items` is the ordered list of pre-project output items;
/// `real_key_count` is the count of non-__aj__ key items at the front.
/// For avg/min/max/sum, the col_name is expected to be "__aj__<alias>" and will
/// be resolved to the corresponding pre-project output index.
fn aggExprToProjectItemWithPreProject(
    ctx: *PlannerCtx,
    p:   generic_sql.Expr,
    pre_items: []const ProjectItem,
    real_key_count: usize,
) !?ProjectItem {
    _ = real_key_count;
    const alias = p.alias orelse p.column orelse "?";
    const col_name = p.column orelse "";

    // Resolve a column name against pre-project output items by alias.
    const resolvePreProjectCol = struct {
        fn resolve(items: []const ProjectItem, name: []const u8) ?Expr {
            for (items, 0..) |item, idx| {
                if (std.ascii.eqlIgnoreCase(item.alias, name)) {
                    return Expr{ .col_ref = .{ .index = idx, .name = item.alias } };
                }
            }
            return null;
        }
    }.resolve;

    const agg_call = try ctx.alloc.create(AggCall);
    switch (p.func) {
        .count_star => {
            agg_call.* = .{ .kind = .count_star, .arg = null, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .uint64 };
        },
        .count_if => {
            agg_call.* = .{ .kind = .count_star, .arg = null, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .uint64 };
        },
        .avg => {
            const arg_expr = resolvePreProjectCol(pre_items, col_name) orelse
                resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            agg_call.* = .{ .kind = .avg, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .float64 };
        },
        .min => {
            const arg_expr = resolvePreProjectCol(pre_items, col_name) orelse
                resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            const col_type = inferExprType(ctx, arg_expr);
            agg_call.* = .{ .kind = .min, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .max => {
            const arg_expr = resolvePreProjectCol(pre_items, col_name) orelse
                resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            const col_type = inferExprType(ctx, arg_expr);
            agg_call.* = .{ .kind = .max, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .sum => {
            const arg_expr = resolvePreProjectCol(pre_items, col_name) orelse
                resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .sum, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .float64 };
        },
        else => return aggExprToProjectItem(ctx, p),
    }
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
            var arg_expr = resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            // Handle SUM(col + N) / SUM(col - N) via int_offset (legacy path)
            if (p.int_offset != 0 and resolveColExpr(ctx, col_name) != null) {
                const binop = try ctx.alloc.create(plan.BinOp);
                binop.* = .{ .left = arg_expr, .right = .{ .lit_i64 = p.int_offset } };
                arg_expr = if (p.int_offset > 0) Expr{ .add = binop } else Expr{ .sub = binop };
            }
            agg_call.* = .{ .kind = .sum, .arg = arg_expr, .distinct = false };
            const out_type: ColumnType = switch (col_type) {
                .float64 => .float64,
                .int64   => .int64,
                else     => .uint64,
            };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = out_type };
        },
        .avg => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            agg_call.* = .{ .kind = .avg, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = .float64 };
        },
        .min => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            const col_type = inferExprType(ctx, arg_expr);
            agg_call.* = .{ .kind = .min, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .max => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            const col_type = inferExprType(ctx, arg_expr);
            agg_call.* = .{ .kind = .max, .arg = arg_expr, .distinct = false };
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = col_type };
        },
        .group_uniq_array, .uniq_exact_if => {
            // If a post-processing function (e.g. arrayFlatten) is needed, fall back to
            // the generic executor which handles post_fn application.
            if (p.post_fn != null) return null;
            const arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            agg_call.* = .{ .kind = .group_uniq_array, .arg = arg_expr, .distinct = false, .sep = p.sep };
            // When sep is present (arrayStringConcat pattern), the result is a joined string.
            const out: ColumnType = if (p.sep != null) .string else .array_string;
            return ProjectItem{ .expr = .{ .agg_call = agg_call }, .alias = alias, .out_type = out };
        },
        .any_val => {
            const arg_expr = resolveColExpr(ctx, col_name) orelse
                (try parseArithExpr(ctx, col_name)) orelse return null;
            agg_call.* = .{ .kind = .any, .arg = arg_expr, .distinct = false };
            const col_type = inferExprType(ctx, arg_expr);
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
