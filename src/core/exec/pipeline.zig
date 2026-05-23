/// Push-based Pipeline execution engine.
///
/// A Pipeline connects a Source, zero or more Transform operators, and a Sink.
/// Data flows in DataChunks from source → transforms → sink.
///
/// Pipeline breakers (HashAgg, OrderBy, HashJoin) are handled by splitting
/// execution into multiple pipeline segments. The breaker's build phase
/// consumes its input pipeline fully, then the probe/read phase feeds the
/// next pipeline segment.
///
/// Thread model: single-threaded. Each query runs one pipeline at a time.
const std    = @import("std");
const types  = @import("../types.zig");
const chunk  = @import("../chunk.zig");
const result = @import("../result.zig");
const plan   = @import("plan.zig");
const kernels = @import("kernels.zig");
const ht     = @import("hash_table.zig");

pub const Value      = types.Value;
pub const AggAccum   = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const DataChunk  = chunk.DataChunk;
pub const ResultSet  = result.ResultSet;
pub const ResultSink = result.ResultSink;

// ── QueryContext ──────────────────────────────────────────────────────────────

/// Per-query execution context. Holds the arena for all intermediate
/// allocations during one query's lifetime.
pub const QueryContext = struct {
    /// All transient allocations (intermediate chunks, hash tables, etc.)
    /// are made from this arena. Freed when the query finishes.
    arena: std.heap.ArenaAllocator,
    /// Injected source implementations (set before executing a plan).
    source: SourceIface,

    pub fn init(parent_alloc: std.mem.Allocator, source: SourceIface) QueryContext {
        return .{
            .arena  = std.heap.ArenaAllocator.init(parent_alloc),
            .source = source,
        };
    }

    pub fn deinit(self: *QueryContext) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *QueryContext) std.mem.Allocator {
        return self.arena.allocator();
    }
};

// ── Source interface ──────────────────────────────────────────────────────────

/// A type-erased source. Concrete implementations live in src/core/source/.
/// Using a vtable (function pointers) here rather than comptime generics
/// because the source type is selected at runtime (config / query routing).
pub const SourceIface = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Fill `out` with the next chunk of rows. Returns false when exhausted.
        nextChunk: *const fn (ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) anyerror!bool,
        /// Reset the source to re-scan from the beginning.
        reset: *const fn (ptr: *anyopaque) void,
        /// Return column metadata for this source's schema.
        schema: *const fn (ptr: *anyopaque) []const result.ColMeta,
    };

    pub fn nextChunk(self: SourceIface, out: *DataChunk, ctx: *QueryContext) !bool {
        return self.vtable.nextChunk(self.ptr, out, ctx);
    }

    pub fn reset(self: SourceIface) void {
        self.vtable.reset(self.ptr);
    }

    pub fn schema(self: SourceIface) []const result.ColMeta {
        return self.vtable.schema(self.ptr);
    }
};

// ── Operator state ────────────────────────────────────────────────────────────

/// Filter: evaluates predicate on each row, zeroes out non-matching rows.
/// Non-matching rows are compacted out — chunk.num_rows shrinks.
pub const FilterState = struct {
    predicate: plan.Expr,
    /// Column indices referenced by the predicate; populated lazily on first apply().
    ref_indices: ?[]usize = null,

    pub fn apply(self: *FilterState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        // Build ref_indices on first call (once per query).
        if (self.ref_indices == null) {
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            collectColRefs(self.predicate, mask);
            var count: usize = 0;
            for (mask) |m| { if (m) count += 1; }
            const indices = try alloc.alloc(usize, count);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { indices[wi] = j; wi += 1; } }
            self.ref_indices = indices;
        }
        const ref = self.ref_indices.?;

        // Row buffer: only fill referenced columns; others stay undefined/null.
        const row = try alloc.alloc(?Value, c.columns.len);
        @memset(row, null);

        var write_pos: usize = 0;
        for (0..c.num_rows) |r| {
            for (ref) |j| {
                const col = c.columns[j];
                row[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            const v = try kernels.evalExpr(self.predicate, row, null, alloc);
            const keep = if (v) |val| val.bool_u8 != 0 else false;
            if (keep and write_pos == r) {
                write_pos += 1;
            } else if (keep) {
                copyRow(c, r, write_pos);
                write_pos += 1;
            }
        }
        c.num_rows = write_pos;
        for (c.columns) |*col| col.len = write_pos;
    }
};

/// Recursively collect column reference indices from an expression into a mask.
fn collectColRefs(expr: plan.Expr, mask: []bool) void {
    switch (expr) {
        .col_ref => |cr| if (cr.index < mask.len) { mask[cr.index] = true; },
        .add, .sub, .mul, .div, .mod => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .eq, .neq, .lt, .lte, .gt, .gte => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .@"and", .@"or" => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .not => |inner| collectColRefs(inner.operand, mask),
        .like, .not_like, .concat => |op| { collectColRefs(op.left, mask); collectColRefs(op.right, mask); },
        .is_null, .is_not_null => |inner| collectColRefs(inner.operand, mask),
        .cast => |c| collectColRefs(c.expr, mask),
        .fn_call => |fc| for (fc.args) |arg| collectColRefs(arg, mask),
        .agg_call => |ac| if (ac.arg) |arg| collectColRefs(arg, mask),
        .case_when => |cw| {
            for (cw.when, cw.then) |wh, th| { collectColRefs(wh, mask); collectColRefs(th, mask); }
            if (cw.else_expr) |e| collectColRefs(e, mask);
        },
        else => {},
    }
}

fn copyRow(c: *DataChunk, from: usize, to: usize) void {
    for (c.columns) |*col| {
        const v = col.data.get(from);
        col.data.set(to, v);
        if (chunk.isNull(col.null_mask, from)) {
            chunk.setNull(col.null_mask, to);
        } else {
            chunk.clearNull(col.null_mask, to);
        }
    }
}

/// Project: evaluate SELECT list expressions, producing a new DataChunk.
pub const ProjectState = struct {
    items: []plan.ProjectItem,

    pub fn apply(self: *ProjectState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        const n     = c.num_rows;

        // Build output column buffers.
        var out_cols = try alloc.alloc(chunk.Column, self.items.len);
        for (self.items, 0..) |item, ci| {
            const nw        = chunk.nullMaskWords(n);
            const null_mask = try alloc.alloc(u64, nw);
            @memset(null_mask, 0);
            const data = allocColumnData(item.out_type, n, alloc) catch continue;
            out_cols[ci] = .{
                .name      = item.alias,
                .data      = data,
                .null_mask = null_mask,
                .len       = n,
            };
        }

        // Evaluate each row.
        for (0..n) |r| {
            const row = try c.readRow(r, alloc);
            for (self.items, 0..) |item, ci| {
                const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
                if (v_opt) |v| {
                    setColumnValue(&out_cols[ci].data, r, v);
                } else {
                    chunk.setNull(out_cols[ci].null_mask, r);
                    setColumnZero(&out_cols[ci].data, r);
                }
            }
        }

        // Replace chunk columns (arena owns both old and new allocations).
        c.columns = out_cols;
    }
};

fn allocColumnData(col_type: ColumnType, n: usize, alloc: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8       => .{ .bool_u8       = try alloc.alloc(u8,           n) },
        .int64         => .{ .int64         = try alloc.alloc(i64,          n) },
        .uint64        => .{ .uint64        = try alloc.alloc(u64,          n) },
        .float64       => .{ .float64       = try alloc.alloc(f64,          n) },
        .date_u16      => .{ .date_u16      = try alloc.alloc(u16,          n) },
        .datetime64_ms => .{ .datetime64_ms = try alloc.alloc(i64,          n) },
        .string        => .{ .string        = try alloc.alloc([]const u8,   n) },
        .array_string  => .{ .array_string  = try alloc.alloc([][]const u8, n) },
    };
}

fn setColumnValue(data: *chunk.ColumnData, r: usize, v: Value) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = switch (v) { .bool_u8 => |x| x, else => @intCast(v.toI64() orelse 0) },
        .int64         => |s| s[r] = v.toI64() orelse 0,
        .uint64        => |s| s[r] = v.toU64() orelse 0,
        .float64       => |s| s[r] = v.toF64() orelse 0.0,
        .date_u16      => |s| s[r] = switch (v) { .date_u16 => |x| x, else => @truncate(@as(u16, @intCast(v.toI64() orelse 0))) },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string        => |s| s[r] = v.toStr() orelse "",
        .array_string  => |s| s[r] = switch (v) { .array_string => |a| a, else => &.{} },
    }
}

fn setColumnZero(data: *chunk.ColumnData, r: usize) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = 0,
        .int64         => |s| s[r] = 0,
        .uint64        => |s| s[r] = 0,
        .float64       => |s| s[r] = 0.0,
        .date_u16      => |s| s[r] = 0,
        .datetime64_ms => |s| s[r] = 0,
        .string        => |s| s[r] = "",
        .array_string  => |s| s[r] = &.{},
    }
}

/// LimitState: tracks how many rows have been emitted, truncates chunks.
pub const LimitState = struct {
    limit:   u64,
    offset:  u64,
    emitted: u64 = 0,
    skipped: u64 = 0,

    pub fn done(self: LimitState) bool {
        return self.emitted >= self.limit;
    }

    pub fn apply(self: *LimitState, c: *DataChunk) void {
        // Handle offset.
        if (self.skipped < self.offset) {
            const skip = @min(c.num_rows, self.offset - self.skipped);
            self.skipped += skip;
            // Compact out the skipped rows.
            const remaining = c.num_rows - skip;
            if (remaining == 0) { c.num_rows = 0; return; }
            for (0..remaining) |i| copyRow(c, i + skip, i);
            c.num_rows = remaining;
            for (c.columns) |*col| col.len = remaining;
        }
        // Truncate to limit.
        if (self.emitted >= self.limit) {
            c.num_rows = 0;
        } else {
            const take = @min(c.num_rows, self.limit - self.emitted);
            self.emitted += take;
            c.num_rows = take;
            for (c.columns) |*col| col.len = take;
        }
    }
};

// ── PhysicalOperator ──────────────────────────────────────────────────────────

/// A single operator node. Operators are applied in sequence to each chunk.
/// Pipeline breakers (hash_agg, order_by, hash_join) are not handled here
/// directly — see executePlan() for how breakers split the pipeline.
pub const PhysicalOperator = union(enum) {
    filter:  FilterState,
    project: ProjectState,
    limit:   LimitState,
};

// ── executePlan ───────────────────────────────────────────────────────────────

/// Internal row list used during plan execution.
/// Memory owned by the QueryContext arena.
pub const RowList = struct {
    metas: []result.ColMeta,
    rows:  std.ArrayListUnmanaged([]?Value),

    pub fn init(metas: []result.ColMeta) RowList {
        return .{ .metas = metas, .rows = .empty };
    }

    pub fn append(self: *RowList, alloc: std.mem.Allocator, row: []?Value) !void {
        try self.rows.append(alloc, row);
    }

    /// Materialise into a ResultSet.  All values are duped into a fresh arena.
    pub fn toResultSet(self: RowList, parent_alloc: std.mem.Allocator) !ResultSet {
        var arena = std.heap.ArenaAllocator.init(parent_alloc);
        const ra  = arena.allocator();

        const num_rows = self.rows.items.len;
        const num_cols = self.metas.len;

        const out_metas = try ra.dupe(result.ColMeta, self.metas);
        if (num_rows == 0 or num_cols == 0) {
            return ResultSet{
                .metas    = out_metas,
                .columns  = &.{},
                .num_rows = 0,
                .arena    = arena,
            };
        }

        const out_cols = try ra.alloc(chunk.Column, num_cols);
        for (out_cols, out_metas) |*col, meta| {
            const nw        = chunk.nullMaskWords(num_rows);
            const null_mask = try ra.alloc(u64, nw);
            @memset(null_mask, 0);
            col.* = .{
                .name      = meta.name,
                .data      = try allocColumnDataRA(meta.col_type, num_rows, ra),
                .null_mask = null_mask,
                .len       = num_rows,
            };
        }

        for (self.rows.items, 0..) |row, r| {
            for (row, 0..) |v_opt, ci| {
                if (v_opt) |v| {
                    setColValue(&out_cols[ci].data, r, v, ra);
                } else {
                    chunk.setNull(out_cols[ci].null_mask, r);
                }
            }
        }

        return ResultSet{
            .metas    = out_metas,
            .columns  = out_cols,
            .num_rows = num_rows,
            .arena    = arena,
        };
    }
};

fn allocColumnDataRA(col_type: ColumnType, n: usize, ra: std.mem.Allocator) !chunk.ColumnData {
    return switch (col_type) {
        .bool_u8       => .{ .bool_u8       = try ra.alloc(u8,           n) },
        .int64         => .{ .int64         = try ra.alloc(i64,          n) },
        .uint64        => .{ .uint64        = try ra.alloc(u64,          n) },
        .float64       => .{ .float64       = try ra.alloc(f64,          n) },
        .date_u16      => .{ .date_u16      = try ra.alloc(u16,          n) },
        .datetime64_ms => .{ .datetime64_ms = try ra.alloc(i64,          n) },
        .string        => .{ .string        = try ra.alloc([]const u8,   n) },
        .array_string  => .{ .array_string  = try ra.alloc([][]const u8, n) },
    };
}

fn setColValue(data: *chunk.ColumnData, r: usize, v: Value, ra: std.mem.Allocator) void {
    switch (data.*) {
        .bool_u8       => |s| s[r] = switch (v) { .bool_u8 => |x| x, else => @intCast(v.toI64() orelse 0) },
        .int64         => |s| s[r] = v.toI64() orelse 0,
        .uint64        => |s| s[r] = v.toU64() orelse 0,
        .float64       => |s| s[r] = v.toF64() orelse 0.0,
        .date_u16      => |s| s[r] = switch (v) { .date_u16 => |x| x, .uint64 => |u| @truncate(u), else => @as(u16, @intCast(@max(0, v.toI64() orelse 0))) },
        .datetime64_ms => |s| s[r] = v.toI64() orelse 0,
        .string        => |s| s[r] = ra.dupe(u8, v.toStr() orelse "") catch (v.toStr() orelse ""),
        .array_string  => |s| s[r] = switch (v) { .array_string => |a| a, else => &.{} },
    }
}

/// Execute a PhysicalNode tree recursively, returning a ResultSet.
/// Handles all node types including pipeline breakers (HashAgg, ScalarAgg,
/// OrderBy, TopK, HashJoin).
pub fn executePlan(
    node: *const plan.PhysicalNode,
    ctx: *QueryContext,
) !ResultSet {
    const alloc = ctx.allocator();
    var rl = try executeNode(node, ctx);
    return rl.toResultSet(alloc);
}

fn executeNode(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowList {
    const alloc = ctx.allocator();
    switch (node.*) {
        // ── Sources ───────────────────────────────────────────────────────────
        .part_scan, .mem_scan => {
            const schema_metas = ctx.source.schema();
            const metas = try alloc.dupe(result.ColMeta, schema_metas);
            var rl = RowList.init(metas);
            var c: DataChunk = undefined;
            while (try ctx.source.nextChunk(&c, ctx)) {
                // Note: do NOT defer c.deinit() here — row values hold slices
                // into the chunk's arena (e.g. array_string elems). Those
                // slices remain valid until qctx.deinit() frees the parent
                // arena that owns the chunk sub-arenas.
                for (0..c.num_rows) |r| {
                    const row = try c.readRow(r, alloc);
                    try rl.append(alloc, row);
                }
            }
            return rl;
        },

        .chunk_source => |cs| return executeNode(cs.input, ctx),

        // ── Filter ────────────────────────────────────────────────────────────
        .filter => |f| {
            if (isScannable(f.input)) {
                return executeLimitChunked(node, ctx);
            }
            const inner = try executeNode(f.input, ctx);
            var rl = RowList.init(inner.metas);
            for (inner.rows.items) |row| {
                const v_opt = try kernels.evalExpr(f.predicate, row, null, alloc);
                const keep = valueToBool(v_opt);
                if (keep) try rl.append(alloc, row);
            }
            return rl;
        },

        // ── Project ───────────────────────────────────────────────────────────
        .project => |p| {
            if (isScannable(p.input)) {
                return executeLimitChunked(node, ctx);
            }
            const inner = try executeNode(p.input, ctx);
            return projectRowList(inner, p.items, alloc);
        },

        // ── Limit ─────────────────────────────────────────────────────────────
        .limit => |lim| {
            if (isScannable(node)) {
                return executeLimitChunked(node, ctx);
            }
            const inner = try executeNode(lim.input, ctx);
            var rl = RowList.init(inner.metas);
            var skipped: u64 = 0;
            var emitted: u64 = 0;
            for (inner.rows.items) |row| {
                if (skipped < lim.offset) { skipped += 1; continue; }
                if (emitted >= lim.limit) break;
                try rl.append(alloc, row);
                emitted += 1;
            }
            return rl;
        },

        // ── ScalarAgg ─────────────────────────────────────────────────────────
        .scalar_agg => |sa| {
            if (isScannable(sa.input)) {
                return executeScalarAggChunked(sa.input, sa.aggs, ctx);
            }
            const inner = try executeNode(sa.input, ctx);
            return executeScalarAgg(inner, sa.aggs, alloc);
        },

        // ── HashAgg ───────────────────────────────────────────────────────────
        .hash_agg => |ha| {
            if (isScannable(ha.input)) {
                return executeHashAggChunked(ha.input, ha.keys, ha.aggs, ctx);
            }
            const inner = try executeNode(ha.input, ctx);
            return executeHashAgg(inner, ha.keys, ha.aggs, alloc);
        },

        // ── OrderBy ───────────────────────────────────────────────────────────
        .order_by => |ob| {
            const inner = try executeNode(ob.input, ctx);
            return executeOrderBy(inner, ob.keys, alloc);
        },

        // ── TopK ──────────────────────────────────────────────────────────────
        .top_k => |tk| {
            const inner = try executeNode(tk.input, ctx);
            const sorted = try executeOrderBy(inner, tk.keys, alloc);
            const take = @min(sorted.rows.items.len, @as(usize, @intCast(tk.k)));
            var rl = RowList.init(sorted.metas);
            for (sorted.rows.items[0..take]) |row| try rl.append(alloc, row);
            return rl;
        },

        // ── HashJoin ──────────────────────────────────────────────────────────
        .hash_join => |hj| {
            const left_rl  = try executeNode(hj.left,  ctx);
            const right_rl = try executeNode(hj.right, ctx);
            return executeHashJoin(left_rl, right_rl, hj, alloc);
        },
    }
}

fn valueToBool(v: ?Value) bool {
    return if (v) |val| switch (val) {
        .bool_u8 => |b| b != 0,
        .int64   => |i| i != 0,
        .uint64  => |u| u != 0,
        .float64 => |f| f != 0.0,
        else     => false,
    } else false;
}

// ── Project helper ────────────────────────────────────────────────────────────

fn projectRowList(inner: RowList, items: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowList {
    const new_metas = try alloc.alloc(result.ColMeta, items.len);
    for (items, 0..) |item, ci| {
        new_metas[ci] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    var rl = RowList.init(new_metas);

    // Detect arrayJoin(expr) calls among the projection items.
    // Collect all indices; they will be expanded in lockstep (element i for all).
    // Other columns repeat their value for each element.
    var aj_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer aj_indices.deinit(alloc);
    for (items, 0..) |item, ci| {
        switch (item.expr) {
            .fn_call => |fc| if (std.mem.eql(u8, fc.name, "arrayJoin")) {
                try aj_indices.append(alloc, ci);
            },
            else => {},
        }
    }

    for (inner.rows.items) |row| {
        if (aj_indices.items.len > 0) {
            // Evaluate the first arrayJoin argument to determine the expansion count.
            const first_ai = aj_indices.items[0];
            const first_aj_expr = switch (items[first_ai].expr) {
                .fn_call => |fc| fc.args[0],
                else => unreachable,
            };
            const first_arr_val = try kernels.evalExpr(first_aj_expr, row, null, alloc);
            const first_elements: []const []const u8 = switch (first_arr_val orelse Value{ .array_string = &.{} }) {
                .array_string => |a| a,
                else => &.{},
            };
            // Evaluate all arrayJoin arrays upfront (for lockstep expansion).
            const aj_arrays = try alloc.alloc([]const []const u8, aj_indices.items.len);
            aj_arrays[0] = first_elements;
            for (aj_indices.items[1..], 1..) |ai, k| {
                const aj_expr = switch (items[ai].expr) {
                    .fn_call => |fc| fc.args[0],
                    else => unreachable,
                };
                const arr_val = try kernels.evalExpr(aj_expr, row, null, alloc);
                aj_arrays[k] = switch (arr_val orelse Value{ .array_string = &.{} }) {
                    .array_string => |a| a,
                    else => &.{},
                };
            }
            // Emit one output row per element (or one null row if empty)
            const n = if (first_elements.len > 0) first_elements.len else @as(usize, 1);
            for (0..n) |ei| {
                const new_row = try alloc.alloc(?Value, items.len);
                // Find which aj_index slot this column corresponds to (if any).
                for (items, 0..) |item, ci| {
                    var is_aj = false;
                    for (aj_indices.items, 0..) |ai, k| {
                        if (ci == ai) {
                            const elems = aj_arrays[k];
                            new_row[ci] = if (elems.len > ei)
                                Value{ .string = elems[ei] }
                            else
                                null;
                            is_aj = true;
                            break;
                        }
                    }
                    if (!is_aj) {
                        new_row[ci] = try kernels.evalExpr(item.expr, row, null, alloc);
                    }
                }
                try rl.append(alloc, new_row);
            }
        } else {
            const new_row = try alloc.alloc(?Value, items.len);
            for (items, 0..) |item, ci| {
                const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
                new_row[ci] = v_opt;
            }
            try rl.append(alloc, new_row);
        }
    }
    return rl;
}

// ── Chunked agg helpers ───────────────────────────────────────────────────────

/// Returns true if node is a direct source (part_scan/mem_scan) or a
/// filter/project/limit over a direct source — i.e. no pipeline breakers.
fn isScannable(node: *const plan.PhysicalNode) bool {
    return switch (node.*) {
        .part_scan, .mem_scan, .chunk_source => true,
        .filter  => |f| isScannable(f.input),
        .project => |p| isScannable(p.input),
        .limit   => |l| isScannable(l.input),
        else => false,
    };
}

/// Drive the source (and optional filter/project/limit pipeline) chunk by
/// chunk and accumulate scalar aggregates without materialising any rows.
fn executeScalarAggChunked(
    input: *const plan.PhysicalNode,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !RowList {
    const alloc = ctx.allocator();
    const accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| accums[ci] = initAccumForAgg(item.expr);

    var filter_state: ?FilterState = extractFilter(input);
    var lim_state:    ?LimitState  = extractLimit(input);

    var c: DataChunk = undefined;
    ctx.source.reset();
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (lim_state)    |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        try updateAccumsFromChunk(accums, aggs, &c, alloc);
        if (lim_state) |ls| if (ls.done()) break;
    }

    const metas   = try alloc.alloc(result.ColMeta, aggs.len);
    const out_row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, ci| {
        metas[ci]   = .{ .name = item.alias, .col_type = item.out_type };
        out_row[ci] = try finalizeAccum(accums[ci], item, alloc);
    }
    var rl = RowList.init(metas);
    try rl.append(alloc, out_row);
    return rl;
}

/// Drive the source chunk by chunk and build a hash aggregate without rows.
fn executeHashAggChunked(
    input: *const plan.PhysicalNode,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !RowList {
    const alloc = ctx.allocator();
    var ht_agg = try ht.AggHashTable.init(alloc, keys.len, aggs.len);
    const init_accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| init_accums[ci] = initAccumForAgg(item.expr);
    const key_buf = try alloc.alloc(Value, keys.len);

    var filter_state: ?FilterState = extractFilter(input);

    // Compute which column indices are referenced by keys and aggs.
    ctx.source.reset();
    var ref_indices: ?[]usize = null;

    var c: DataChunk = undefined;
    var row_buf: []?Value = &.{};
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;
        // Build ref_indices once (on first non-empty chunk).
        if (ref_indices == null) {
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            for (keys) |k| collectColRefs(k.expr, mask);
            for (aggs) |a| collectColRefs(a.expr, mask);
            var cnt: usize = 0;
            for (mask) |m| { if (m) cnt += 1; }
            const idxs = try alloc.alloc(usize, cnt);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { idxs[wi] = j; wi += 1; } }
            ref_indices = idxs;
        }
        const refs = ref_indices.?;
        for (0..c.num_rows) |r| {
            // Fill only referenced columns.
            for (refs) |j| {
                const col = c.columns[j];
                row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            for (keys, 0..) |k, ki| {
                const v = try kernels.evalExpr(k.expr, row_buf, null, alloc);
                key_buf[ki] = v orelse Value{ .int64 = 0 };
            }
            const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
            for (aggs, 0..) |item, ci| {
                const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                try kernels.updateAccum(&bucket[ci], v_opt, alloc);
            }
        }
    }

    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, ki| out_metas[ki] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, ai| out_metas[keys.len + ai] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);
    const CtxT = struct {
        rl: *RowList, alloc: std.mem.Allocator, keys_len: usize, aggs: []const plan.ProjectItem,
    };
    var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
    ht_agg.iterate(&emit_ctx, struct {
        fn cb(ec: *CtxT, k: []const Value, bucket: []const AggAccum) void {
            const row = ec.alloc.alloc(?Value, ec.keys_len + bucket.len) catch return;
            for (k, 0..) |kv, i| row[i] = kv;
            for (bucket, ec.aggs, 0..) |acc, item, i| {
                row[ec.keys_len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
            }
            ec.rl.append(ec.alloc, row) catch {};
        }
    }.cb);
    return rl;
}

/// Accumulate aggregate state from one DataChunk without building a row slice.
/// Fast-path: count_star and sum(col_ref) work vectorially on column slices.
/// Fallback: all other aggs are handled in a single per-row pass at the end.
fn updateAccumsFromChunk(
    accums: []AggAccum,
    aggs:   []const plan.ProjectItem,
    c:      *const DataChunk,
    alloc:  std.mem.Allocator,
) !void {
    // Track which aggs need a per-row fallback pass (one pass covers all of them).
    var needs_fallback = false;
    // Temp boolean array to mark which indices need fallback.
    const fb_mask = try alloc.alloc(bool, aggs.len);
    @memset(fb_mask, false);

    for (aggs, 0..) |item, ci| {
        const acc_ptr = &accums[ci];
        var handled = false;
        switch (item.expr) {
            .agg_call => |ac| {
                switch (ac.kind) {
                    .count_star => {
                        acc_ptr.count += c.num_rows;
                        handled = true;
                    },
                    .count => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    for (0..c.num_rows) |r| {
                                        if (!chunk.isNull(col.null_mask, r)) acc_ptr.count += 1;
                                    }
                                    handled = true;
                                },
                                else => {},
                            }
                        } else {
                            acc_ptr.count += c.num_rows;
                            handled = true;
                        }
                    },
                    .sum => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    switch (col.data) {
                                        .int64 => |vals| {
                                            if (acc_ptr.* == .i64_sum) {
                                                for (vals[0..c.num_rows]) |v| acc_ptr.i64_sum +%= v;
                                                handled = true;
                                            }
                                        },
                                        .uint64 => |vals| {
                                            if (acc_ptr.* == .u64_sum) {
                                                for (vals[0..c.num_rows]) |v| acc_ptr.u64_sum +%= v;
                                                handled = true;
                                            } else if (acc_ptr.* == .i64_sum) {
                                                for (vals[0..c.num_rows]) |v| acc_ptr.i64_sum +%= @bitCast(v);
                                                handled = true;
                                            }
                                        },
                                        .float64 => |vals| {
                                            if (acc_ptr.* == .f64_sum) {
                                                for (vals[0..c.num_rows]) |v| acc_ptr.f64_sum += v;
                                                handled = true;
                                            }
                                        },
                                        else => {},
                                    }
                                },
                                // SUM(col + int_literal): vectorized sum of (val + k)
                                .add => |bo| {
                                    const cr_opt: ?plan.ColRef = switch (bo.left) { .col_ref => |c2| c2, else => null };
                                    const k_opt: ?i64 = switch (bo.right) { .lit_i64 => |v| v, else => null };
                                    if (cr_opt != null and k_opt != null) {
                                        const cr = cr_opt.?;
                                        const k = k_opt.?;
                                        const col = c.columns[cr.index];
                                        switch (col.data) {
                                            .int64 => |vals| {
                                                if (acc_ptr.* == .i64_sum) {
                                                    for (0..c.num_rows) |r| {
                                                        if (!chunk.isNull(col.null_mask, r)) acc_ptr.i64_sum +%= vals[r] + k;
                                                    }
                                                    handled = true;
                                                }
                                            },
                                            else => {},
                                        }
                                    }
                                },
                                else => {},
                            }
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
        if (!handled) {
            fb_mask[ci] = true;
            needs_fallback = true;
        }
    }

    if (!needs_fallback) return;

    // Collect referenced columns for all fallback aggs.
    const ref_mask2 = try alloc.alloc(bool, c.columns.len);
    @memset(ref_mask2, false);
    for (aggs, 0..) |item, ci| { if (fb_mask[ci]) collectColRefs(item.expr, ref_mask2); }

    // Single per-row pass for all fallback aggs.
    const row = try alloc.alloc(?Value, c.columns.len);
    @memset(row, null);
    for (0..c.num_rows) |r| {
        for (ref_mask2, 0..) |m, j| if (m) {
            const col = c.columns[j];
            row[j] = if (col.isRowNull(r)) null else col.data.get(r);
        };
        for (aggs, 0..) |item, ci| {
            if (!fb_mask[ci]) continue;
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }
}

/// Extract the filter predicate from the outermost filter/limit/project wrapping a scan.
fn extractFilter(node: *const plan.PhysicalNode) ?FilterState {
    return switch (node.*) {
        .filter  => |f| .{ .predicate = f.predicate },
        .limit   => |l| extractFilter(l.input),
        .project => |p| extractFilter(p.input),
        else => null,
    };
}

/// Extract the limit state from the outermost limit wrapping a scan.
fn extractLimit(node: *const plan.PhysicalNode) ?LimitState {
    return switch (node.*) {
        .limit   => |l| .{ .limit = l.limit, .offset = l.offset, .emitted = 0 },
        .filter  => |f| extractLimit(f.input),
        .project => |p| extractLimit(p.input),
        else => null,
    };
}

// ── Chunked limit helper ──────────────────────────────────────────────────────

/// Chunked streaming execution for limit/project/filter/scan patterns.
fn executeLimitChunked(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowList {
    const alloc = ctx.allocator();

    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: LimitState = .{ .limit = std.math.maxInt(u64), .offset = 0 };

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit => |lim| { lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter => |f| { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p| { if (project_items == null) project_items = p.items; cur = p.input; },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);
    var rl = RowList.init(out_metas);

    ctx.source.reset();
    var c: DataChunk = undefined;
    var skipped: u64 = 0;
    var emitted: u64 = 0;
    var row_ref_indices: ?[]usize = null;

    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;

        if (row_ref_indices == null and c.columns.len > 0) {
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            if (project_items) |items| { for (items) |item| collectColRefs(item.expr, mask); }
            else @memset(mask, true);
            var cnt: usize = 0;
            for (mask) |m| { if (m) cnt += 1; }
            const idxs = try alloc.alloc(usize, cnt);
            var wi: usize = 0;
            for (mask, 0..) |m, j| { if (m) { idxs[wi] = j; wi += 1; } }
            row_ref_indices = idxs;
        }
        const refs = row_ref_indices orelse &[_]usize{};
        const row_buf = try alloc.alloc(?Value, c.columns.len);
        @memset(row_buf, null);

        for (0..c.num_rows) |r| {
            if (skipped < lim_state.offset) { skipped += 1; continue; }
            if (emitted >= lim_state.limit) break;
            for (refs) |j| {
                const col = c.columns[j];
                row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            const out_row: []?Value = if (project_items) |items| blk: {
                const out = try alloc.alloc(?Value, items.len);
                for (items, 0..) |item, i| out[i] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
                break :blk out;
            } else blk: {
                const out = try alloc.alloc(?Value, c.columns.len);
                for (c.columns, 0..) |col, j| out[j] = if (col.isRowNull(r)) null else col.data.get(r);
                break :blk out;
            };
            try rl.append(alloc, out_row);
            emitted += 1;
        }
        if (emitted >= lim_state.limit) break;
    }
    return rl;
}

// ── ScalarAgg helper ──────────────────────────────────────────────────────────

fn executeScalarAgg(inner: RowList, aggs: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowList {
    const accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| accums[ci] = initAccumForAgg(item.expr);

    for (inner.rows.items) |row| {
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }

    const metas = try alloc.alloc(result.ColMeta, aggs.len);
    const out_row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, ci| {
        metas[ci] = .{ .name = item.alias, .col_type = item.out_type };
        out_row[ci] = try finalizeAccum(accums[ci], item, alloc);
    }
    var rl = RowList.init(metas);
    try rl.append(alloc, out_row);
    return rl;
}

// ── HashAgg helper ────────────────────────────────────────────────────────────

fn executeHashAgg(
    inner: RowList,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    alloc: std.mem.Allocator,
) !RowList {
    var ht_agg = try ht.AggHashTable.init(alloc, keys.len, aggs.len);

    const init_accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| init_accums[ci] = initAccumForAgg(item.expr);

    const key_buf = try alloc.alloc(Value, keys.len);
    for (inner.rows.items) |row| {
        for (keys, 0..) |k, ki| {
            const v = try kernels.evalExpr(k.expr, row, null, alloc);
            key_buf[ki] = v orelse Value{ .int64 = 0 }; // NULL → zero sentinel for hashing
        }
        const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&bucket[ci], v_opt, alloc);
        }
    }

    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, ki| out_metas[ki] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, ai| out_metas[keys.len + ai] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);

    const CtxT = struct {
        rl:       *RowList,
        alloc:    std.mem.Allocator,
        keys_len: usize,
        aggs:     []const plan.ProjectItem,
    };
    var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
    ht_agg.iterate(&emit_ctx, struct {
        fn cb(c: *CtxT, k: []const Value, bucket: []const AggAccum) void {
            const row = c.alloc.alloc(?Value, c.keys_len + bucket.len) catch return;
            for (k, 0..) |kv, i| row[i] = kv;
            for (bucket, c.aggs, 0..) |acc, item, i| {
                row[c.keys_len + i] = finalizeAccum(acc, item, c.alloc) catch null;
            }
            c.rl.append(c.alloc, row) catch {};
        }
    }.cb);

    return rl;
}

// ── OrderBy helper ────────────────────────────────────────────────────────────

fn executeOrderBy(inner: RowList, keys: []const plan.SortKey, alloc: std.mem.Allocator) !RowList {
    const rows_copy = try alloc.dupe([]?Value, inner.rows.items);

    const SortCtx = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
            for (self.keys) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null)
                    .eq
                else if (av == null)
                    .lt  // NULL sorts first
                else
                    .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
    };
    std.sort.pdq([]?Value, rows_copy, SortCtx{ .keys = keys }, SortCtx.lessThan);

    var rl = RowList.init(inner.metas);
    for (rows_copy) |row| try rl.append(alloc, row);
    return rl;
}

// ── HashJoin helper ───────────────────────────────────────────────────────────

fn executeHashJoin(
    left_rl:  RowList,
    right_rl: RowList,
    hj: plan.HashJoinNode,
    alloc: std.mem.Allocator,
) !RowList {
    var jht = try ht.JoinHashTable.init(alloc);
    const key_buf = try alloc.alloc(Value, hj.equi_keys.len);

    for (right_rl.rows.items, 0..) |row, ri| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = (if (ek.right_col_idx < row.len) row[ek.right_col_idx] else null) orelse Value{ .int64 = 0 };
        }
        try jht.insert(key_buf, @intCast(ri));
    }

    const combined_metas = try alloc.alloc(result.ColMeta, left_rl.metas.len + right_rl.metas.len);
    @memcpy(combined_metas[0..left_rl.metas.len], left_rl.metas);
    @memcpy(combined_metas[left_rl.metas.len..], right_rl.metas);

    var rl = RowList.init(combined_metas);

    for (left_rl.rows.items) |lrow| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = ((if (ek.left_col_idx < lrow.len) lrow[ek.left_col_idx] else null)) orelse Value{ .int64 = 0 };
        }
        const matches = jht.probe(key_buf);

        if (matches.len == 0 and hj.join_type == .left) {
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            for (combined[lrow.len..]) |*vv| vv.* = null;
            try rl.append(alloc, combined);
            continue;
        }

        for (matches) |ri| {
            const rrow = right_rl.rows.items[ri];
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            @memcpy(combined[lrow.len..lrow.len + rrow.len], rrow);
            if (hj.filter) |filt| {
                const keep_v = try kernels.evalExpr(filt, combined, null, alloc);
                if (!valueToBool(keep_v)) continue;
            }
            try rl.append(alloc, combined);
        }
    }

    return rl;
}

// ── Aggregate helpers ─────────────────────────────────────────────────────────

/// Finalize one accumulator for a ProjectItem.
/// When the agg is group_uniq_array with a sep, joins the array into a string.
fn finalizeAccum(acc: AggAccum, item: plan.ProjectItem, alloc: std.mem.Allocator) !?Value {
    const sep: ?[]const u8 = switch (item.expr) {
        .agg_call => |ac| ac.sep,
        else => null,
    };
    if (sep) |s| {
        const arr_val = try acc.toArrayValue(alloc);
        const elems = arr_val.array_string;
        if (elems.len == 0) return Value{ .string = "" };
        // Calculate total length.
        var total: usize = 0;
        for (elems) |e| total += e.len;
        total += s.len * (elems.len - 1);
        const buf = try alloc.alloc(u8, total);
        var pos: usize = 0;
        for (elems, 0..) |e, idx| {
            if (idx > 0) {
                @memcpy(buf[pos..pos + s.len], s);
                pos += s.len;
            }
            @memcpy(buf[pos..pos + e.len], e);
            pos += e.len;
        }
        return Value{ .string = buf };
    }
    return acc.toValue() catch (try acc.toArrayValue(alloc));
}

fn initAccumForAgg(expr: plan.Expr) AggAccum {
    return switch (expr) {
        .agg_call => |ac| switch (ac.kind) {
            .count_star, .count => .{ .count = 0 },
            .sum  => .{ .i64_sum = 0 },
            .avg  => .{ .f64_sum = 0.0 },
            .min  => .{ .i64_min = std.math.maxInt(i64) },
            .max  => .{ .i64_max = std.math.minInt(i64) },
            .group_uniq_array => .{ .uniq_strs = .{} },
            .any  => .{ .any_val = null },
        },
        else => .{ .count = 0 },
    };
}

fn evalAggArg(expr: plan.Expr, row: []const ?Value, alloc: std.mem.Allocator) !?Value {
    return switch (expr) {
        .agg_call => |ac| if (ac.arg) |arg| kernels.evalExpr(arg, row, null, alloc) else null,
        else => kernels.evalExpr(expr, row, null, alloc),
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "LimitState basic" {
    var b = chunk.ChunkBuilder.init(std.testing.allocator, 5);
    defer b.chunk.deinit();
    const col = try b.addColumn("n", .int64);
    for (0..5) |i| col.data.int64[i] = @intCast(i);
    var c = b.finish();

    var lim = LimitState{ .limit = 3, .offset = 0 };
    lim.apply(&c);
    try std.testing.expectEqual(@as(usize, 3), c.num_rows);
}

// ── Mock source for executePlan tests ─────────────────────────────────────────

const MockSource = struct {
    chunk: DataChunk,
    done: bool = false,

    const metas_storage = [_]result.ColMeta{
        .{ .name = "n", .col_type = .int64 },
    };

    fn nextChunk(ptr: *anyopaque, out: *DataChunk, _ctx: *QueryContext) !bool {
        _ = _ctx;
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        if (self.done) return false;
        self.done = true;
        out.* = self.chunk;
        return true;
    }
    fn reset(ptr: *anyopaque) void {
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        self.done = false;
    }
    fn schema(_ptr: *anyopaque) []const result.ColMeta {
        _ = _ptr;
        return &metas_storage;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunk,
        .reset     = reset,
        .schema    = schema,
    };

    fn iface(self: *MockSource) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

test "executePlan: scalar_agg count(*)" {
    const alloc = std.testing.allocator;

    // Build a chunk with 4 rows, column "n" Int64: [1, 2, 3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 4);
    const ci = try b.addColumn("n", .int64);
    for (0..4) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Plan: scalar_agg [ count(*) ]
    var agg_call = plan.AggCall{ .kind = .count_star, .arg = null, .distinct = false };
    const agg_items = [_]plan.ProjectItem{.{
        .expr     = .{ .agg_call = &agg_call },
        .alias    = "count()",
        .out_type = .uint64,
    }};
    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const agg_node  = plan.PhysicalNode{ .scalar_agg = .{ .input = @constCast(&scan_node), .aggs = &agg_items } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&agg_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 1), rs.num_rows);
    try std.testing.expectEqual(@as(usize, 1), rs.metas.len);
    const v = rs.get(0, 0).?;
    try std.testing.expectEqual(Value{ .uint64 = 4 }, v);
}

test "executePlan: filter + limit" {
    const alloc = std.testing.allocator;

    // 5 rows: [1..5], keep n > 2, limit 2 → [3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 5);
    const ci = try b.addColumn("n", .int64);
    for (0..5) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Filter: n > 2   (col_ref index=0 > lit_i64 2)
    var gt_binop = plan.BinOp{
        .left  = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 2 },
    };
    const scan_node   = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const filter_node = plan.PhysicalNode{ .filter = .{ .input = @constCast(&scan_node), .predicate = .{ .gt = &gt_binop } } };
    const limit_node  = plan.PhysicalNode{ .limit  = .{ .input = @constCast(&filter_node), .limit = 2, .offset = 0 } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&limit_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(Value{ .int64 = 3 }, rs.get(0, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 4 }, rs.get(0, 1).?);
}
