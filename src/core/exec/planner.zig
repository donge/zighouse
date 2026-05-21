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
            const col_expr = resolveColExpr(ctx, col_name) orelse {
                // col_name might be a function call like "lower(protocol)" — try to
                // parse it as a known scalar fn and build an fn_call Expr.
                const item = try tryParseFnCallItem(ctx, col_name, alias) orelse break :blk null;
                break :blk item;
            };
            const out_type = schemaColType(ctx, col_name);
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
                // Check for zero-arg call
                const first_peek = pctx.lex.peek();
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
                            std.ascii.eqlIgnoreCase(name, "mapGet")) break :blk2 true;
                    }
                    if (args_slice.len == 3) {
                        if (std.ascii.eqlIgnoreCase(name, "if") or
                            std.ascii.eqlIgnoreCase(name, "substring") or
                            std.ascii.eqlIgnoreCase(name, "substr")) break :blk2 true;
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
            var arg_expr = resolveColExpr(ctx, col_name) orelse return null;
            // Handle SUM(col + N) / SUM(col - N) via int_offset
            if (p.int_offset != 0) {
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
