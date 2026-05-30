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
const simd   = @import("../simd_ops.zig");

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
        /// Return an upper-bound row count estimate (0 = unknown).
        rowCount: *const fn (ptr: *anyopaque) u64,
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

    pub fn rowCount(self: SourceIface) u64 {
        return self.vtable.rowCount(self.ptr);
    }
};

// ── Operator state ────────────────────────────────────────────────────────────

/// Filter: evaluates predicate on each row, zeroes out non-matching rows.
/// Non-matching rows are compacted out — chunk.num_rows shrinks.
pub const LikeGuard = struct {
    col_idx: usize,
    pattern: []const u8,
    negate: bool,
    matcher: kernels.LikeMatcher,
};

pub const FilterState = struct {
    predicate: plan.Expr,
    /// Column indices referenced by the predicate; populated lazily on first apply().
    ref_indices: ?[]usize = null,
    /// Row buffer reused across chunk calls (allocated on first apply).
    row_buf: ?[]?Value = null,
    /// LIKE guards: col_ref LIKE/NOT_LIKE lit_str checks extracted from the predicate.
    /// Checked cheaply before full evalExpr to short-circuit expensive rows early.
    /// null = not yet initialized; empty slice = no LIKE guards in predicate.
    like_guards: ?[]LikeGuard = null,
    /// Set to true after first chunk if all guard columns are .string type.
    guards_verified: bool = false,

    /// When true, the pure-LIKE fast path skips copyRow and just counts matching rows.
    /// Safe only when downstream only reads c.num_rows (e.g. COUNT(*) aggregation).
    count_only_mode: bool = false,

    pub fn apply(self: *FilterState, c: *DataChunk, ctx: *QueryContext) !void {
        const alloc = ctx.allocator();
        // Build ref_indices, row_buf, and like_guards on first call (once per query).
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
            const row = try alloc.alloc(?Value, c.columns.len);
            @memset(row, null);
            self.row_buf = row;
            // Collect LIKE/NOT_LIKE guards; only keep if all guard columns are string type.
            var guards_list = std.ArrayListUnmanaged(LikeGuard){ .items = &.{}, .capacity = 0 };
            collectLikeGuards(self.predicate, &guards_list, alloc);
            const raw_guards = try guards_list.toOwnedSlice(alloc);
            var guards_ok = true;
            for (raw_guards) |lg| {
                if (lg.col_idx >= c.columns.len or c.columns[lg.col_idx].data != .string) {
                    guards_ok = false;
                    break;
                }
            }
            self.like_guards = if (guards_ok) raw_guards else &.{};
            self.guards_verified = true;
        }
        const ref = self.ref_indices.?;
        const row = self.row_buf.?;
        const guards = self.like_guards.?;

        // Pure-LIKE fast path: predicate is exactly col_ref LIKE/NOT_LIKE lit_str.
        if (guards.len == 1) {
            switch (self.predicate) {
                .like, .not_like => {
                    const lg = guards[0];
                    const col = c.columns[lg.col_idx];
                    if (self.count_only_mode) {
                        // Count-only: skip copyRow entirely — caller only needs c.num_rows.
                        var count: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (lg.matcher.match(s) != lg.negate) count += 1;
                        }
                        c.num_rows = count;
                        for (c.columns) |*col2| col2.len = count;
                        return;
                    }
                    var write_pos: usize = 0;
                    for (0..c.num_rows) |r| {
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        const keep = lg.matcher.match(s) != lg.negate;
                        if (keep and write_pos == r) {
                            write_pos += 1;
                        } else if (keep) {
                            copyRow(c, r, write_pos);
                            write_pos += 1;
                        }
                    }
                    c.num_rows = write_pos;
                    for (c.columns) |*col2| col2.len = write_pos;
                    return;
                },
                else => {},
            }
        }

        // Fast path: col_ref != lit_str (e.g. "Referer <> ''").
        // Avoids row_buf boxing and full evalExpr dispatch.
        switch (self.predicate) {
            .neq => |op| {
                if (op.left == .col_ref and op.right == .lit_str) {
                    const col_idx = op.left.col_ref.index;
                    const lit = op.right.lit_str;
                    if (col_idx < c.columns.len and c.columns[col_idx].data == .string) {
                        const col = c.columns[col_idx];
                        var write_pos: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (!std.mem.eql(u8, s, lit)) {
                                if (write_pos != r) copyRow(c, r, write_pos);
                                write_pos += 1;
                            }
                        }
                        c.num_rows = write_pos;
                        for (c.columns) |*col2| col2.len = write_pos;
                        return;
                    }
                }
            },
            .eq => |op| {
                if (op.left == .col_ref and op.right == .lit_str) {
                    const col_idx = op.left.col_ref.index;
                    const lit = op.right.lit_str;
                    if (col_idx < c.columns.len and c.columns[col_idx].data == .string) {
                        const col = c.columns[col_idx];
                        var write_pos: usize = 0;
                        for (0..c.num_rows) |r| {
                            const s = if (col.isRowNull(r)) "" else col.data.string[r];
                            if (std.mem.eql(u8, s, lit)) {
                                if (write_pos != r) copyRow(c, r, write_pos);
                                write_pos += 1;
                            }
                        }
                        c.num_rows = write_pos;
                        for (c.columns) |*col2| col2.len = write_pos;
                        return;
                    }
                }
            },
            else => {},
        }

        // Multi-LIKE guard short-circuit: check all LIKE guards before boxing row_buf.
        if (guards.len > 0) {
            var write_pos: usize = 0;
            row_loop: for (0..c.num_rows) |r| {
                for (guards) |lg| {
                    const col = c.columns[lg.col_idx];
                    const s = if (col.isRowNull(r)) "" else col.data.string[r];
                    if (lg.matcher.match(s) == lg.negate) continue :row_loop;
                }
                // All LIKE guards passed — fill row_buf and evaluate full predicate.
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
            return;
        }

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

/// Recursively collect all col_ref LIKE/NOT_LIKE lit_str guards from an AND-chained predicate.
/// These guards can be evaluated cheaply before full expression eval to short-circuit rows.
fn collectLikeGuards(expr: plan.Expr, guards: *std.ArrayListUnmanaged(LikeGuard), alloc: std.mem.Allocator) void {
    switch (expr) {
        .like, .not_like => |op| {
            if (op.left == .col_ref and op.right == .lit_str) {
                guards.append(alloc, .{
                    .col_idx = op.left.col_ref.index,
                    .pattern = op.right.lit_str,
                    .negate  = expr == .not_like,
                    .matcher = kernels.LikeMatcher.compile(op.right.lit_str),
                }) catch {};
            }
        },
        .@"and" => |op| {
            collectLikeGuards(op.left, guards, alloc);
            collectLikeGuards(op.right, guards, alloc);
        },
        else => {},
    }
}

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

    // ── Scannable path: stream chunks directly into ResultSink ─────────────
    if (isScannable(node)) {
        var sink = ResultSink.init(alloc);
        try executeScannableToSink(node, ctx, &sink);
        return sink.finish();
    }

    // ── Breaker path: existing RowList → ResultSet (single copy) ───────────
    var rl = try executeNode(node, ctx);
    return rl.toResultSet(alloc);
}

/// Stream a scannable node (scan/filter/project/limit) directly to a ResultSink.
/// Avoids building a RowList by operating on DataChunks throughout.
fn executeScannableToSink(
    node: *const plan.PhysicalNode,
    ctx:  *QueryContext,
    sink: *ResultSink,
) !void {
    const alloc = ctx.allocator();
    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: ?LimitState = null;

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit   => |lim| { if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter  => |f|   { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p|   {
                if (project_items == null) {
                    project_items = p.items;
                }
                cur = p.input;
            },
            else => break,
        }
    }

    ctx.source.reset();
    var c: DataChunk = undefined;
    // Row buffer for projection; allocated once on first non-empty chunk.
    var row_buf: ?[]?Value = null;
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state)  |*fs| try fs.apply(&c, ctx);
        if (lim_state)     |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        if (project_items) |items| {
            // Lazy-init row_buf once on first non-empty chunk.
            if (row_buf == null) {
                const rb = try alloc.alloc(?Value, c.columns.len);
                @memset(rb, null);
                row_buf = rb;
            }
            const rb = row_buf.?;
            const n = c.num_rows;
            const out_cols = try alloc.alloc(chunk.Column, items.len);
            for (items, 0..) |item, ci| {
                const nw        = chunk.nullMaskWords(n);
                const null_mask = try alloc.alloc(u64, nw);
                @memset(null_mask, 0);
                const data = try allocColumnData(item.out_type, n, alloc);
                out_cols[ci] = .{ .name = item.alias, .data = data, .null_mask = null_mask, .len = n };
            }
            for (0..n) |r| {
                for (c.columns, 0..) |col, j| {
                    rb[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (items, 0..) |item, ci| {
                    const v_opt = try kernels.evalExpr(item.expr, rb, null, alloc);
                    if (v_opt) |v| {
                        setColumnValue(&out_cols[ci].data, r, v);
                    } else {
                        chunk.setNull(out_cols[ci].null_mask, r);
                        setColumnZero(&out_cols[ci].data, r);
                    }
                }
            }
            c.columns = out_cols;
        }
        try sink.consume(c);
        if (lim_state) |ls| if (ls.done()) break;
    }
}fn executeNode(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowList {
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
            // Detect: project → top_k → scannable  (e.g. SELECT * … ORDER BY col LIMIT k)
            // Stream scannable input directly into heap to avoid materialising all rows.
            if (p.input.* == .top_k) {
                const tk = p.input.top_k;
                if (isScannable(tk.input)) {
                    var proj_over_scan = plan.PhysicalNode{ .project = .{ .input = tk.input, .items = p.items } };
                    return executeTopKFromScannable(&proj_over_scan, tk.keys, @intCast(tk.k), ctx);
                }
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
             const k = @as(usize, @intCast(tk.k));
             // Fast path: stream scannable input directly into heap — avoids
             // materialising all rows into a RowList before sorting.
             if (isScannable(tk.input)) {
                 return executeTopKFromScannable(tk.input, tk.keys, k, ctx);
             }
             const inner = try executeNode(tk.input, ctx);
             // For small K, use a partial selection (heap-based) instead of full sort.
             if (k <= 1024 and inner.rows.items.len > k * 4) {
                 return executeTopK(inner, tk.keys, k, alloc);
             }
             const sorted = try executeOrderBy(inner, tk.keys, alloc);
             const take = @min(sorted.rows.items.len, k);
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

    // Count-only mode: if all aggs are COUNT(*), skip copyRow in pure-LIKE filter.
    // Downstream only reads c.num_rows for count accumulation.
    if (filter_state != null) {
        const all_count_star = for (aggs) |item| {
            const ok = item.expr == .agg_call and item.expr.agg_call.kind == .count_star;
            if (!ok) break false;
        } else true;
        if (all_count_star) filter_state.?.count_only_mode = true;
    }

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

/// Returns true if all GROUP BY keys are `col_ref` or `col_ref ± lit_i64` —
/// a necessary (but not sufficient) condition for the int-key fast path.
fn keysAreIntExpr(keys: []const plan.ProjectItem) bool {
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => {},
            .add => |op| { if (op.left != .col_ref or op.right != .lit_i64) return false; },
            .sub => |op| { if (op.left != .col_ref or op.right != .lit_i64) return false; },
            else => return false,
        }
    }
    return true;
}

/// Returns true if all keys are plain col_ref expressions.
fn keysAreColRef(keys: []const plan.ProjectItem) bool {
    for (keys) |k| {
        if (k.expr != .col_ref) return false;
    }
    return true;
}


/// Drive the source chunk by chunk and build a hash aggregate without rows.
/// Convert compact u64 accumulator values to output Values for emit.
/// Shared between CompactIntKeyHashTable and StrAggHashTable emit paths.
fn emitCompactVals(
    vals:  []const u64,
    kinds: []const ht.CompactAggKind,
    aggs:  []const plan.ProjectItem,
    out:   []?Value,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) { cnt = cv; break; }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            // str_min/str_max: emitted via sidecar; return empty string as sentinel.
            .str_min, .str_max => Value{ .string = "" },
        };
    }
}

/// Like emitCompactVals but reads str_min/str_max from StrAggHashTable sidecar.
fn emitCompactValsWithSidecar(
    vals:         []const u64,
    kinds:        []const ht.CompactAggKind,
    aggs:         []const plan.ProjectItem,
    out:          []?Value,
    str_ht:       *const ht.StrAggHashTable,
    slot:         usize,
    sidecar_idx:  []const usize,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) { cnt = cv; break; }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            .str_min, .str_max => blk: {
                const s = str_ht.getStrSidecar(slot, sidecar_idx[i]) orelse "";
                break :blk Value{ .string = s };
            },
        };
    }
}

/// Update compact u64 accumulator slots for a single row.
/// Shared between int-key and str-agg paths to avoid code duplication.
inline fn updateCompactVals(
    slot_vals:       []u64,
    ck:              []const ht.CompactAggKind,
    aggs:            []const plan.ProjectItem,
    c:               *const DataChunk,
    r:               usize,
    str_ht:          ?*ht.StrAggHashTable,
    slot:            usize,
    sidecar_indices: []const usize,
) !void {
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) continue;
        const ac = item.expr.agg_call;
        switch (ck[ci]) {
            .count => {
                if (ac.kind == .count_star) {
                    slot_vals[ci] += 1;
                } else if (ac.kind == .count) {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            if (!c.columns[arg.col_ref.index].isRowNull(r))
                                slot_vals[ci] += 1;
                        } else slot_vals[ci] += 1;
                    }
                }
            },
            .i64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64  => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        .uint64 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += @as(i64, @bitCast(v[r])); slot_vals[ci] = @bitCast(s); },
                        .bool_u8 => |v| { var s: i64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        else => {},
                    };
                }}
            },
            .f64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += v[r]; slot_vals[ci] = @bitCast(s); },
                        .int64   => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                        .uint64  => |v| { var s: f64 = @bitCast(slot_vals[ci]); s += @floatFromInt(v[r]); slot_vals[ci] = @bitCast(s); },
                        else => {},
                    };
                }}
            },
            .i64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64 => |v| { const cur: i64 = @bitCast(slot_vals[ci]); if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .i64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .int64 => |v| { const cur: i64 = @bitCast(slot_vals[ci]); if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .u64_sum => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| slot_vals[ci] += v[r],
                        else => {},
                    };
                }}
            },
            .u64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| { if (v[r] < slot_vals[ci]) slot_vals[ci] = v[r]; },
                        else => {},
                    };
                }}
            },
            .u64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .uint64 => |v| { if (v[r] > slot_vals[ci]) slot_vals[ci] = v[r]; },
                        else => {},
                    };
                }}
            },
            .f64_min => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { const cur: f64 = @bitCast(slot_vals[ci]); if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .f64_max => {
                if (ac.arg) |arg| { if (arg == .col_ref) {
                    const col = c.columns[arg.col_ref.index];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .float64 => |v| { const cur: f64 = @bitCast(slot_vals[ci]); if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]); },
                        else => {},
                    };
                }}
            },
            .str_min => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| { if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], true),
                            else => {},
                        };
                    }}
                }
            },
            .str_max => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| { if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], false),
                            else => {},
                        };
                    }}
                }
            },
        }
    }
}

fn executeHashAggChunked(
    input: *const plan.PhysicalNode,
    keys:  []const plan.ProjectItem,
    aggs:  []const plan.ProjectItem,
    ctx:   *QueryContext,
) !RowList {
    const alloc = ctx.allocator();
    // Pre-size hash table when input is a bare scan (no filter reduces cardinality).
    // Use min(row_count, 2M) to avoid excessive memory that degrades subsequent queries.
    const MAX_PRESIZED: u64 = 8_000_000;
    const est_rows: u64 = switch (input.*) {
        .part_scan, .mem_scan, .chunk_source => @min(ctx.source.rowCount(), MAX_PRESIZED),
        else => 0,
    };

    // Fast path: if all keys are integer col_ref / col_ref±lit expressions, try
    // IntKeyHashTable (no []Value boxing, inline key storage).
    const maybe_int_keys = keysAreIntExpr(keys);

    // Compact accum fast path: if all aggs are pure-numeric, use
    // CompactIntKeyHashTable (8B/agg vs 32B/agg), cutting the accum slab 4×.
    // Only active when maybe_int_keys is true.
    const compact_kinds: ?[]ht.CompactAggKind = blk: {
        const kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
        for (aggs, 0..) |item, ci| {
            if (item.expr != .agg_call) break :blk null;
            kinds[ci] = switch (item.expr.agg_call.kind) {
                .count_star, .count => .count,
                .sum  => .i64_sum,   // type refined at runtime (int64/uint64/f64)
                .avg  => .f64_sum,
                // min/max: string args use str_min/str_max (StrAggHashTable sidecar);
                // numeric args use the appropriate numeric kind (refined at runtime).
                .min  => if (item.out_type == .string) .str_min else .i64_min,
                .max  => if (item.out_type == .string) .str_max else .i64_max,
                .group_uniq_array, .any => break :blk null,
            };
        }
        break :blk kinds;
    };
    // init_vals: u64 encoding of the initial value per compact agg kind.
    const compact_init_vals: []u64 = if (compact_kinds) |ck| blk: {
        const iv = try alloc.alloc(u64, ck.len);
        for (ck, 0..) |kind, ci| {
            iv[ci] = switch (kind) {
                .count, .i64_sum, .u64_sum, .u64_max => 0,
                .f64_sum => @bitCast(@as(f64, 0.0)),
                .i64_min => @bitCast(@as(i64, std.math.maxInt(i64))),
                .i64_max => @bitCast(@as(i64, std.math.minInt(i64))),
                .u64_min => std.math.maxInt(u64),
                .f64_min => @bitCast(std.math.inf(f64)),
                .f64_max => @bitCast(-std.math.inf(f64)),
                // str_min/str_max: vals_flat slot unused; sidecar handles the string.
                .str_min, .str_max => 0,
            };
        }
        break :blk iv;
    } else &.{};

    // Count str_min/str_max aggs for StrAggHashTable sidecar sizing.
    const num_str_aggs: usize = if (compact_kinds) |ck| blk: {
        var n: usize = 0;
        for (ck) |k| { if (k == .str_min or k == .str_max) n += 1; }
        break :blk n;
    } else 0;
    // Map compact_kind index → sidecar index (only valid for str_min/str_max entries).
    const str_agg_sidecar_idx: []usize = if (compact_kinds) |ck| blk: {
        const m = try alloc.alloc(usize, ck.len);
        var si: usize = 0;
        for (ck, 0..) |k, ci| {
            if (k == .str_min or k == .str_max) { m[ci] = si; si += 1; }
            else m[ci] = 0;
        }
        break :blk m;
    } else &.{};

    // Detect Q29-style regexp_replace(col_ref, lit_str_pattern, lit_str_repl) key.
    // Cache col_idx + whether it's the URL-domain pattern to avoid per-row checks.
    const RegexpReplaceKeyDesc = struct {
        col_idx: usize,
        is_url_domain: bool,  // true = Q29 fast path
    };
    var regexp_replace_key_descs: ?[]RegexpReplaceKeyDesc = null;
    check_rr: {
        if (keys.len == 0) break :check_rr;
        // Quick pre-check: first key must be a fn_call named regexp_replace.
        if (keys[0].expr != .fn_call) break :check_rr;
        const fc0 = keys[0].expr.fn_call;
        if (!(std.mem.eql(u8, fc0.name, "regexp_replace") or
              std.mem.eql(u8, fc0.name, "replaceRegexpOne"))) break :check_rr;
        // All keys must be regexp_replace(col_ref, lit_str, lit_str).
        const descs_buf = try alloc.alloc(RegexpReplaceKeyDesc, keys.len);
        for (keys, 0..) |k, ki| {
            if (k.expr != .fn_call) break :check_rr;
            const fc = k.expr.fn_call;
            if (!(std.mem.eql(u8, fc.name, "regexp_replace") or
                  std.mem.eql(u8, fc.name, "replaceRegexpOne")) or
                fc.args.len < 3 or
                fc.args[0] != .col_ref or
                fc.args[1] != .lit_str or
                fc.args[2] != .lit_str)
            {
                break :check_rr;
            }
            const pattern = fc.args[1].lit_str;
            const is_url = std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                           std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*");
            descs_buf[ki] = .{ .col_idx = fc.args[0].col_ref.index, .is_url_domain = is_url };
        }
        regexp_replace_key_descs = descs_buf;
    }

    // StrAggHashTable for regexp_replace single-key path (e.g. Q29).
    // Initialized here if rr_descs is single-key and aggs are compact-numeric.
    const rr_can_use_str_agg = compact_kinds != null and
        regexp_replace_key_descs != null and
        regexp_replace_key_descs.?.len == 1;

    var ht_agg = try ht.AggHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows);
    // Use CompactIntKeyHashTable when keys are all-int AND aggs are all pure-numeric.
    // Falls back to IntKeyHashTable when compact_kinds is null (e.g. any_val agg).
    var ht_compact: ?ht.CompactIntKeyHashTable = if (compact_kinds != null and maybe_int_keys and num_str_aggs == 0)
        try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else null;
    var ht_int: ?ht.IntKeyHashTable = if (maybe_int_keys and (compact_kinds == null or num_str_aggs > 0))
        try ht.IntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else null;

    // StrCountHashTable fast path: exactly one col_ref key (others may be constants) + count(*) agg.
    // Handles Q34 (GROUP BY URL) and Q35 (GROUP BY 1, URL).
    const maybe_str_count = blk: {
        if (aggs.len != 1) break :blk false;
        if (aggs[0].expr != .agg_call) break :blk false;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk false;
        var col_ref_count: usize = 0;
        for (keys) |k| {
            switch (k.expr) {
                .col_ref => col_ref_count += 1,
                .lit_i64, .lit_str => {},
                else => break :blk false,
            }
        }
        break :blk col_ref_count == 1;
    };
    var ht_str_count: ?ht.StrCountHashTable = null;
    var str_count_col_idx: usize = 0;
    var use_str_count_path: bool = false;

    // StrAggHashTable fast path: single string col_ref key + all-compact aggs
    // (including str_min/str_max via sidecar).
    // Handles Q22/Q23 (GROUP BY SearchPhrase + MIN/COUNT) and the Q29 regexp_replace path.
    // Also triggered when maybe_str_count would apply but there are additional aggs beyond COUNT(*).
    const str_agg_col_idx: ?usize = blk: {
        if (maybe_int_keys) break :blk null;      // int key path takes priority
        if (compact_kinds == null) break :blk null; // aggs not all compact
        if (keys.len != 1) break :blk null;        // single key only
        if (keys[0].expr != .col_ref) break :blk null;
        break :blk keys[0].expr.col_ref.index;
    };
    var ht_str_agg: ?ht.StrAggHashTable = if (str_agg_col_idx != null or rr_can_use_str_agg)
        try ht.StrAggHashTable.initWithCapacity(alloc, aggs.len, num_str_aggs, est_rows)
    else null;
    var use_str_agg_path: bool = false;
    // Set to true when regexp_replace key path routes to ht_str_agg (e.g. Q29).
    var rr_used_str_agg: bool = false;

    // PairCountHashTable fast path: exactly two col_ref keys (one i64, one string) + count(*).
    // Handles Q17/Q18 (GROUP BY UserID, SearchPhrase) and Q19 (3 keys — not handled here).
    const maybe_pair_count = blk: {
        if (aggs.len != 1) break :blk false;
        if (aggs[0].expr != .agg_call) break :blk false;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk false;
        var col_ref_count: usize = 0;
        for (keys) |k| {
            if (k.expr == .col_ref) col_ref_count += 1
            else break :blk false;
        }
        break :blk col_ref_count == 2;
    };
    var ht_pair_count: ?ht.PairCountHashTable = null;
    var pair_i64_col_idx: usize = 0;
    var pair_str_col_idx: usize = 0;
    var use_pair_count_path: bool = false;

    // TripleCountHashTable fast path: (i64_col, date_part(unit, datetime_col), string_col) + count(*).
    // Handles Q19: GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase.
    const DatePartUnit = enum { minute, hour, day };
    const TripleDesc = struct {
        n0_col:   usize,   // first i64 col_ref index
        dp_col:   usize,   // col_ref index inside date_part(...)
        dp_unit:  DatePartUnit,
        str_col:  usize,   // string col_ref index
        // Order of keys in output row: 0=n0, 1=dp, 2=str  or some permutation.
        key_order: [3]u8,  // key_order[i] = which variable fills keys[i]
    };
    const maybe_triple_count: ?TripleDesc = blk: {
        if (aggs.len != 1) break :blk null;
        if (aggs[0].expr != .agg_call) break :blk null;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk null;
        if (keys.len != 3) break :blk null;
        // Find which key is the date_part fn_call and record the others.
        var dp_idx: ?usize = null;
        var dp_col: usize = 0;
        var dp_unit: DatePartUnit = .minute;
        var col_ref_indices: [2]usize = .{0, 0};
        var cri: usize = 0;
        for (keys, 0..) |k, ki| {
            switch (k.expr) {
                .col_ref => {
                    if (cri >= 2) break :blk null;
                    col_ref_indices[cri] = ki;
                    cri += 1;
                },
                .fn_call => |fc| {
                    if (dp_idx != null) break :blk null; // two fn_calls
                    if (!(std.mem.eql(u8, fc.name, "date_part") or
                          std.mem.eql(u8, fc.name, "extract"))) break :blk null;
                    if (fc.args.len < 2) break :blk null;
                    if (fc.args[0] != .lit_str) break :blk null;
                    if (fc.args[1] != .col_ref) break :blk null;
                    const unit_str = fc.args[0].lit_str;
                    dp_unit = if (std.mem.eql(u8, unit_str, "minute") or std.mem.eql(u8, unit_str, "min"))
                        .minute
                    else if (std.mem.eql(u8, unit_str, "hour"))
                        .hour
                    else if (std.mem.eql(u8, unit_str, "day") or std.mem.eql(u8, unit_str, "dayofmonth"))
                        .day
                    else
                        break :blk null;
                    dp_col = fc.args[1].col_ref.index;
                    dp_idx = ki;
                },
                else => break :blk null,
            }
        }
        if (dp_idx == null or cri != 2) break :blk null;
        const key_order: [3]u8 = blk2: {
            var order: [3]u8 = .{0, 0, 0};
            for (keys, 0..) |k, ki| {
                if (ki == dp_idx.?) order[ki] = 1  // date_part → n1
                else if (k.expr == .col_ref) order[ki] = if (ki == col_ref_indices[0]) 0 else 2;
            }
            break :blk2 order;
        };
        _ = key_order;
        break :blk TripleDesc{
            .n0_col    = keys[col_ref_indices[0]].expr.col_ref.index,
            .dp_col    = dp_col,
            .dp_unit   = dp_unit,
            .str_col   = keys[col_ref_indices[1]].expr.col_ref.index,
            .key_order = blk2: {
                var order: [3]u8 = .{0, 0, 0};
                for (keys, 0..) |_, ki| {
                    if (ki == dp_idx.?) order[ki] = 1
                    else if (ki == col_ref_indices[0]) order[ki] = 0
                    else order[ki] = 2;
                }
                break :blk2 order;
            },
        };
    };
    var ht_triple_count: ?ht.TripleCountHashTable = null;
    var use_triple_count_path: bool = false;
    var triple_desc: TripleDesc = if (maybe_triple_count) |d| d else .{
        .n0_col = 0, .dp_col = 0, .dp_unit = .minute, .str_col = 0, .key_order = .{0,1,2},
    };

    const init_accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, ci| init_accums[ci] = initAccumForAgg(item.expr);
    const key_buf     = try alloc.alloc(Value, keys.len);
    const int_key_buf = try alloc.alloc(i64,   keys.len);

    var filter_state: ?FilterState = extractFilter(input);
    // Once we've verified on the first chunk that all key columns are int64/uint64,
    // this flag is set to true and we use ht_int for all subsequent rows.
    var use_int_path: bool = false;
    var int_path_checked: bool = false;

    // Column descriptors for int key path: per key, col index and addend.
    const IntKeyDesc = struct { col_idx: usize, addend: i64 };
    const int_key_descs = try alloc.alloc(IntKeyDesc, keys.len);

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
        // Verify int-key eligibility on first chunk.
        if (!int_path_checked) {
            int_path_checked = true;
            if (maybe_int_keys) {
                var all_int = true;
                for (keys, 0..) |k, ki| {
                    const col_idx: usize = switch (k.expr) {
                        .col_ref => |cr| cr.index,
                        .add     => |op| op.left.col_ref.index,
                        .sub     => |op| op.left.col_ref.index,
                        else => { all_int = false; break; },
                    };
                    const addend: i64 = switch (k.expr) {
                        .col_ref => 0,
                        .add     => |op| op.right.lit_i64,
                        .sub     => |op| -op.right.lit_i64,
                        else     => 0,
                    };
                    const cd = c.columns[col_idx];
                    switch (cd.data) {
                        .int64, .uint64 => {},
                        else => { all_int = false; break; },
                    }
                    int_key_descs[ki] = .{ .col_idx = col_idx, .addend = addend };
                }
                use_int_path = all_int;
            }
            // Verify str-count eligibility (single string key col).
            if (maybe_str_count and !use_int_path) {
                // Find the single col_ref key (others are literals).
                var found_col_ref: ?usize = null;
                for (keys) |k| {
                    if (k.expr == .col_ref) { found_col_ref = k.expr.col_ref.index; break; }
                }
                if (found_col_ref) |col_idx| {
                    if (col_idx < c.columns.len) {
                        switch (c.columns[col_idx].data) {
                            .string => {
                                str_count_col_idx = col_idx;
                                ht_str_count = try ht.StrCountHashTable.initWithCapacity(alloc, est_rows);
                                use_str_count_path = true;
                            },
                            else => {},
                        }
                    }
                }
            }
            // Verify str-agg eligibility: single string col_ref key + compact numeric aggs.
            if (str_agg_col_idx) |col_idx| {
                if (!use_str_count_path and col_idx < c.columns.len and
                    c.columns[col_idx].data == .string)
                {
                    use_str_agg_path = true;
                }
            }
            // Verify pair-count eligibility: exactly two col_refs, one i64 and one string.
            if (maybe_pair_count and !use_int_path and !use_str_count_path) {
                const c0 = keys[0].expr.col_ref.index;
                const c1 = keys[1].expr.col_ref.index;
                if (c0 < c.columns.len and c1 < c.columns.len) {
                    const d0 = c.columns[c0].data;
                    const d1 = c.columns[c1].data;
                    const ok0_i = (d0 == .int64 or d0 == .uint64) and d1 == .string;
                    const ok1_i = (d1 == .int64 or d1 == .uint64) and d0 == .string;
                    if (ok0_i) {
                        pair_i64_col_idx = c0;
                        pair_str_col_idx = c1;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    } else if (ok1_i) {
                        pair_i64_col_idx = c1;
                        pair_str_col_idx = c0;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    }
                }
            }
            // Verify triple-count eligibility: (i64, date_part_datetime, string) + count(*).
            if (maybe_triple_count != null and !use_int_path and !use_str_count_path and !use_pair_count_path) {
                const td = maybe_triple_count.?;
                if (td.n0_col < c.columns.len and td.dp_col < c.columns.len and td.str_col < c.columns.len) {
                    const n0_ok = c.columns[td.n0_col].data == .int64 or c.columns[td.n0_col].data == .uint64;
                    const dp_ok = c.columns[td.dp_col].data == .datetime64_ms or c.columns[td.dp_col].data == .int64;
                    const str_ok = c.columns[td.str_col].data == .string;
                    if (n0_ok and dp_ok and str_ok) {
                        triple_desc = td;
                        ht_triple_count = try ht.TripleCountHashTable.initWithCapacity(alloc, est_rows);
                        use_triple_count_path = true;
                    }
                }
            }
        }
        const refs = ref_indices.?;

        if (use_str_count_path) {
            // ── String-key count(*) fast path ─────────────────────────────────
            const col = c.columns[str_count_col_idx];
            const strs = col.data.string;
            for (0..c.num_rows) |r| {
                try ht_str_count.?.increment(strs[r]);
            }
            continue;
        }

        if (use_pair_count_path) {
            // ── (i64, string) pair count(*) fast path ─────────────────────────
            const strs = c.columns[pair_str_col_idx].data.string;
            // Handle both int64 and uint64 key columns.
            switch (c.columns[pair_i64_col_idx].data) {
                .int64  => |ints| { for (0..c.num_rows) |r| try ht_pair_count.?.increment(ints[r], strs[r]); },
                .uint64 => |ints| { for (0..c.num_rows) |r| try ht_pair_count.?.increment(@bitCast(ints[r]), strs[r]); },
                else    => unreachable,
            }
            continue;
        }

        if (use_triple_count_path) {
            // ── (i64, date_part, string) triple count(*) fast path ────────────
            const td = triple_desc;
            const n0_col = c.columns[td.n0_col];
            const dp_col = c.columns[td.dp_col];
            const strs   = c.columns[td.str_col].data.string;
            for (0..c.num_rows) |r| {
                const n0: i64 = switch (n0_col.data) {
                    .int64  => |v| v[r],
                    .uint64 => |v| @bitCast(v[r]),
                    else    => unreachable,
                };
                const ms: i64 = switch (dp_col.data) {
                    .datetime64_ms => |v| v[r],
                    .int64         => |v| v[r] * 1000,
                    else           => unreachable,
                };
                const secs = @divTrunc(ms, 1000);
                const n1: i64 = switch (td.dp_unit) {
                    .minute => @mod(@divTrunc(secs, 60), 60),
                    .hour   => @mod(@divTrunc(secs, 3600), 24),
                    .day    => blk: {
                        const days = @divTrunc(ms, 86400 * 1000);
                        // Simple day-of-month: reuse date math from kernels.
                        const d = if (days >= 0) @as(u64, @intCast(days)) else 0;
                        // Gregorian calendar: days since epoch.
                        const n: u64 = d + 719468;
                        const era: u64 = @divTrunc(n, 146097);
                        const doe: u64 = n - era * 146097;
                        const yoe: u64 = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
                        const doy: u64 = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
                        const mp:  u64 = @divTrunc(5 * doy + 2, 153);
                        break :blk @intCast(doy - @divTrunc(153 * mp + 2, 5) + 1);
                    },
                };
                try ht_triple_count.?.increment(n0, n1, strs[r]);
            }
            continue;
        }

        if (use_int_path) {
            // ── Integer-key fast path ──────────────────────────────────────────
            if (ht_compact) |*htc| {
                // ── Compact accum sub-path: 8B/agg instead of 32B/agg ─────────
                const ck = compact_kinds.?;
                for (0..c.num_rows) |r| {
                    var key_valid = true;
                    for (int_key_descs, 0..) |desc, ki| {
                        const col = c.columns[desc.col_idx];
                        if (chunk.isNull(col.null_mask, r)) { key_valid = false; break; }
                        int_key_buf[ki] = switch (col.data) {
                            .int64  => |v| v[r] +% desc.addend,
                            .uint64 => |v| @as(i64, @bitCast(v[r])) +% desc.addend,
                            else    => { key_valid = false; break; },
                        };
                    }
                    if (!key_valid) continue;
                    const slot_vals = try htc.getOrInsert(int_key_buf, compact_init_vals);
                    try updateCompactVals(slot_vals, ck, aggs, &c, r, null, 0, str_agg_sidecar_idx);
                }
            } else {
            // ── Regular AggAccum sub-path ──────────────────────────────────────
            for (0..c.num_rows) |r| {
                // Build int key without Value boxing.
                var key_valid = true;
                for (int_key_descs, 0..) |desc, ki| {
                    const col = c.columns[desc.col_idx];
                    if (chunk.isNull(col.null_mask, r)) { key_valid = false; break; }
                    const raw: i64 = switch (col.data) {
                        .int64  => |v| v[r],
                        .uint64 => |v| @bitCast(v[r]),
                        else    => { key_valid = false; break; },
                    };
                    int_key_buf[ki] = raw +% desc.addend;
                }
                if (!key_valid) continue;
                const bucket = try ht_int.?.getOrInsert(int_key_buf, init_accums);
                // Update accumulators (still uses row_buf for agg args).
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (aggs, 0..) |item, ci| {
                    const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                    try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                }
            }
            } // end else (regular path)
    } else if (use_str_agg_path) {
            // ── String-key compact agg path (e.g. Q22/Q23 GROUP BY SearchPhrase) ──
            const col_idx = str_agg_col_idx.?;
            const ck      = compact_kinds.?;
            const strs    = c.columns[col_idx].data.string;
            for (0..c.num_rows) |r| {
                if (c.columns[col_idx].isRowNull(r)) continue;
                const s = strs[r];
                const res = try ht_str_agg.?.getOrInsert(s, compact_init_vals);
                try updateCompactVals(res.vals, ck, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
            }
        } else if (regexp_replace_key_descs) |rr_descs| {
            // ── regexp_replace key fast path (e.g. Q29) ───────────────────────
            // Avoids per-row pattern string comparison in evalFnCall.
            const use_rr_str_agg = ht_str_agg != null and rr_descs.len == 1;
            if (use_rr_str_agg) rr_used_str_agg = true;
            const ck = compact_kinds;
            for (0..c.num_rows) |r| {
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                var key_valid = true;
                var domain_str: []const u8 = "";
                for (rr_descs, 0..) |desc, ki| {
                    const s_opt = row_buf[desc.col_idx];
                    const s = if (s_opt) |v| (v.toStr() orelse null) else null;
                    const domain: ?Value = if (s) |str| d: {
                        if (desc.is_url_domain) {
                            const after_proto = if (std.mem.startsWith(u8, str, "https://"))
                                str[8..]
                            else if (std.mem.startsWith(u8, str, "http://"))
                                str[7..]
                            else
                                break :d Value{ .string = str };
                            const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                break :d Value{ .string = str };
                            var host = after_proto[0..slash];
                            if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                            break :d Value{ .string = host };
                        }
                        break :d Value{ .string = str };
                    } else null;
                    if (domain == null) { key_valid = false; break; }
                    key_buf[ki] = domain.?;
                    if (ki == 0) domain_str = domain.?.string;
                }
                if (!key_valid) continue;
                if (use_rr_str_agg) {
                    const res = try ht_str_agg.?.getOrInsert(domain_str, compact_init_vals);
                    try updateCompactVals(res.vals, ck.?, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
                } else {
                    const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                    for (aggs, 0..) |item, ci| {
                        const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                        try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                    }
                }
            }
        } else {
            // ── General path ──────────────────────────────────────────────────
            for (0..c.num_rows) |r| {
                // Fill only referenced columns.
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (keys, 0..) |k, ki| {
                    // Inline fast path for common key expressions (avoids evalExpr dispatch).
                    const v: ?Value = switch (k.expr) {
                        .col_ref => |cr| row_buf[cr.index],
                        .add => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv +% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .sub => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv -% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .fn_call => |fc| blk: {
                            // Fast path: date_part('minute'/'hour', col_ref) — avoids arg eval + string dispatch.
                            if (fc.args.len == 2 and
                                fc.args[0] == .lit_str and
                                fc.args[1] == .col_ref)
                            {
                                const unit = fc.args[0].lit_str;
                                const col_idx = fc.args[1].col_ref.index;
                                if (row_buf[col_idx]) |ts_val| {
                                    const ms: i64 = switch (ts_val) {
                                        .datetime64_ms => |m| m,
                                        .int64         => |i| i * 1000,
                                        else           => {
                                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                                        },
                                    };
                                    const secs = @divTrunc(ms, 1000);
                                    if (std.mem.eql(u8, unit, "minute") or std.mem.eql(u8, unit, "min")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 60), 60) };
                                    }
                                    if (std.mem.eql(u8, unit, "hour")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 3600), 24) };
                                    }
                                }
                            }
                            // Fast path: regexp_replace(col_ref, lit_str_pattern, lit_str_repl)
                            // for the Q29 URL-domain extraction pattern.
                            if (fc.name.len > 0 and
                                (std.mem.eql(u8, fc.name, "regexp_replace") or
                                 std.mem.eql(u8, fc.name, "replaceRegexpOne")) and
                                fc.args.len >= 3 and
                                fc.args[0] == .col_ref and
                                fc.args[1] == .lit_str and
                                fc.args[2] == .lit_str)
                            {
                                const col_idx = fc.args[0].col_ref.index;
                                const pattern = fc.args[1].lit_str;
                                const s = (row_buf[col_idx] orelse break :blk null).toStr() orelse break :blk null;
                                if (std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                                    std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*"))
                                {
                                    const after_proto = if (std.mem.startsWith(u8, s, "https://"))
                                        s[8..]
                                    else if (std.mem.startsWith(u8, s, "http://"))
                                        s[7..]
                                    else
                                        break :blk Value{ .string = s };
                                    const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                        break :blk Value{ .string = s };
                                    var host = after_proto[0..slash];
                                    if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                                    break :blk Value{ .string = host };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        else => try kernels.evalExpr(k.expr, row_buf, null, alloc),
                    };
                    key_buf[ki] = v orelse Value{ .int64 = 0 };
                }
                const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                for (aggs, 0..) |item, ci| {
                    const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                    try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                }
            }
        }
    }

    const out_metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |k, ki| out_metas[ki] = .{ .name = k.alias, .col_type = k.out_type };
    for (aggs, 0..) |a, ai| out_metas[keys.len + ai] = .{ .name = a.alias, .col_type = a.out_type };

    var rl = RowList.init(out_metas);

    if (use_str_count_path) {
        // Emit from StrCountHashTable. Keys may include literals (e.g. Q35: GROUP BY 1, URL).
        const EmitCtxS = struct {
            rl: *RowList, alloc: std.mem.Allocator,
            keys: []const plan.ProjectItem,
        };
        var emit_ctx_s = EmitCtxS{ .rl = &rl, .alloc = alloc, .keys = keys };
        ht_str_count.?.iterate(&emit_ctx_s, struct {
            fn cb(ec: *EmitCtxS, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, ec.keys.len + 1) catch return;
                for (ec.keys, 0..) |k, i| {
                    row[i] = switch (k.expr) {
                        .col_ref => Value{ .string = s },
                        .lit_i64 => |v| Value{ .int64 = v },
                        .lit_str => |v| Value{ .string = v },
                        else => Value{ .int64 = 0 },
                    };
                }
                row[ec.keys.len] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_pair_count_path) {
        // Emit from PairCountHashTable: restore key order (i64, str or str, i64).
        const k0_is_i64 = keys[0].expr.col_ref.index == pair_i64_col_idx;
        const EmitCtxP = struct {
            rl: *RowList, alloc: std.mem.Allocator, k0_is_i64: bool,
        };
        var emit_ctx_p = EmitCtxP{ .rl = &rl, .alloc = alloc, .k0_is_i64 = k0_is_i64 };
        ht_pair_count.?.iterate(&emit_ctx_p, struct {
            fn cb(ec: *EmitCtxP, n: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 3) catch return;
                if (ec.k0_is_i64) {
                    row[0] = Value{ .int64 = n };
                    row[1] = Value{ .string = s };
                } else {
                    row[0] = Value{ .string = s };
                    row[1] = Value{ .int64 = n };
                }
                row[2] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_triple_count_path) {
        // Emit from TripleCountHashTable: restore key order per triple_desc.key_order.
        const td = triple_desc;
        const EmitCtxT = struct {
            rl: *RowList, alloc: std.mem.Allocator, key_order: [3]u8,
        };
        var emit_ctx_t = EmitCtxT{ .rl = &rl, .alloc = alloc, .key_order = td.key_order };
        ht_triple_count.?.iterate(&emit_ctx_t, struct {
            fn cb(ec: *EmitCtxT, n0: i64, n1: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 4) catch return;
                for (ec.key_order, 0..) |kind, i| {
                    row[i] = switch (kind) {
                        0 => Value{ .int64 = n0 },
                        1 => Value{ .int64 = n1 },
                        else => Value{ .string = s },
                    };
                }
                row[3] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_str_agg_path or rr_used_str_agg) {
        // Emit from StrAggHashTable: string key + compact aggs → Values.
        // Uses sidecar for str_min/str_max aggs.
        // Also used when regexp_replace key path routed to ht_str_agg (Q29).
        const EmitCtxSA = struct {
            rl:           *RowList,
            alloc:        std.mem.Allocator,
            aggs:         []const plan.ProjectItem,
            kinds:        []const ht.CompactAggKind,
            str_ht:       *ht.StrAggHashTable,
            sidecar_idx:  []const usize,
        };
        var emit_ctx_sa = EmitCtxSA{
            .rl          = &rl,
            .alloc       = alloc,
            .aggs        = aggs,
            .kinds       = compact_kinds.?,
            .str_ht      = &ht_str_agg.?,
            .sidecar_idx = str_agg_sidecar_idx,
        };
        ht_str_agg.?.iterateWithSlot(&emit_ctx_sa, struct {
            fn cb(ec: *EmitCtxSA, s: []const u8, vals: []const u64, slot: usize) void {
                const row = ec.alloc.alloc(?Value, 1 + vals.len) catch return;
                row[0] = Value{ .string = s };
                emitCompactValsWithSidecar(vals, ec.kinds, ec.aggs, row[1..], ec.str_ht, slot, ec.sidecar_idx);
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_int_path) {
        if (ht_compact) |*htc| {
             // Emit from CompactIntKeyHashTable: u64 vals → Values.
             const EmitCtxC = struct {
                 rl:    *RowList,
                 alloc: std.mem.Allocator,
                 keys:  []const plan.ProjectItem,
                 aggs:  []const plan.ProjectItem,
                 kinds: []const ht.CompactAggKind,
                 descs: []const IntKeyDesc,
             };
             var emit_ctx_c = EmitCtxC{
                 .rl    = &rl,
                 .alloc = alloc,
                 .keys  = keys,
                 .aggs  = aggs,
                 .kinds = compact_kinds.?,
                 .descs = int_key_descs,
             };
             htc.iterate(&emit_ctx_c, struct {
                 fn cb(ec: *EmitCtxC, k: []const i64, vals: []const u64) void {
                     const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
                     for (k, 0..) |raw_val, i| {
                         _ = ec.descs[i];
                         row[i] = Value{ .int64 = raw_val };
                     }
                     emitCompactVals(vals, ec.kinds, ec.aggs, row[ec.keys.len..]);
                     ec.rl.append(ec.alloc, row) catch {};
                 }
             }.cb);
        } else {
        // Emit from IntKeyHashTable: convert i64 keys back to Values.
        const EmitCtxI = struct {
            rl: *RowList, alloc: std.mem.Allocator,
            keys: []const plan.ProjectItem,
            aggs: []const plan.ProjectItem,
            descs: []const IntKeyDesc,
        };
        var emit_ctx_i = EmitCtxI{
            .rl = &rl, .alloc = alloc, .keys = keys, .aggs = aggs, .descs = int_key_descs,
        };
        ht_int.?.iterate(&emit_ctx_i, struct {
            fn cb(ec: *EmitCtxI, k: []const i64, bucket: []const AggAccum) void {
                const row = ec.alloc.alloc(?Value, ec.keys.len + bucket.len) catch return;
                for (k, ec.descs, 0..) |raw_val, desc, i| {
                    // Convert back: if column was uint64, re-interpret; otherwise int64.
                    _ = desc;
                    row[i] = Value{ .int64 = raw_val };
                }
                for (bucket, ec.aggs, 0..) |acc, item, i| {
                    row[ec.keys.len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
                }
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
        } // end else (regular IntKeyHashTable emit)
    } else {
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
    }
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

    // Fast path: all aggs are SUM(same_col) or SUM(same_col + k) for int64 column —
    // compute SUM(col) + count*k in a single pass instead of one pass per agg.
    // Saves O(90) passes for Q30 (90× SUM(ResolutionWidth + k)).
    if (aggs.len > 1) blk: {
        var base_col_idx: ?usize = null;
        for (aggs) |item| {
            const ac = switch (item.expr) { .agg_call => |a| a, else => break :blk };
            if (ac.kind != .sum) break :blk;
            const arg = ac.arg orelse break :blk;
            switch (arg) {
                .col_ref => |cr| {
                    if (base_col_idx == null) base_col_idx = cr.index
                    else if (base_col_idx.? != cr.index) break :blk;
                },
                .add => |bo| {
                    const cr = switch (bo.left) { .col_ref => |c2| c2, else => break :blk };
                    _ = switch (bo.right) { .lit_i64 => {}, else => break :blk };
                    if (base_col_idx == null) base_col_idx = cr.index
                    else if (base_col_idx.? != cr.index) break :blk;
                },
                else => break :blk,
            }
        }
        const col_idx = base_col_idx orelse break :blk;
        const col = c.columns[col_idx];
        const vals = switch (col.data) { .int64 => |v| v, else => break :blk };
        // Verify all accumulators are i64_sum.
        for (0..aggs.len) |ci| {
            if (accums[ci] != .i64_sum) break :blk;
        }
        // Single pass: accumulate col_sum and non_null_count.
        var col_sum: i64 = 0;
        var non_null_count: i64 = 0;
        if (chunk.allNonNull(col.null_mask)) {
            // Fast path: no nulls — use SIMD sum.
            col_sum = simd.sumI64(vals[0..c.num_rows]);
            non_null_count = @intCast(c.num_rows);
        } else {
            for (0..c.num_rows) |r| {
                if (!chunk.isNull(col.null_mask, r)) {
                    col_sum +%= vals[r];
                    non_null_count += 1;
                }
            }
        }
        // Update each accumulator analytically: SUM(col+k) = SUM(col) + count*k.
        for (aggs, 0..) |item, ci| {
            const k: i64 = switch (item.expr.agg_call.arg.?) {
                .col_ref => 0,
                .add => |bo| bo.right.lit_i64,
                else => 0,
            };
            accums[ci].i64_sum +%= col_sum + non_null_count * k;
        }
        return;
    }

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
                                                acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .uint64 => |vals| {
                                            if (acc_ptr.* == .u64_sum) {
                                                acc_ptr.u64_sum +%= @bitCast(simd.sumU64(vals[0..c.num_rows]));
                                                handled = true;
                                            } else if (acc_ptr.* == .i64_sum) {
                                                acc_ptr.i64_sum +%= simd.sumU64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .float64 => |vals| {
                                            if (acc_ptr.* == .f64_sum) {
                                                acc_ptr.f64_sum += simd.sumF64(vals[0..c.num_rows]);
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
                                                    if (chunk.allNonNull(col.null_mask)) {
                                                        acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]) +% (@as(i64, @intCast(c.num_rows)) *% k);
                                                    } else {
                                                        for (0..c.num_rows) |r| {
                                                            if (!chunk.isNull(col.null_mask, r)) acc_ptr.i64_sum +%= vals[r] + k;
                                                        }
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
                    .avg => {
                        // AVG accumulates into f64_sum (finalization divides by count elsewhere).
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                // Fast path: no nulls — sum as i64 then cast once.
                                                acc_ptr.f64_sum += @floatFromInt(simd.sumI64(vals[0..c.num_rows]));
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        acc_ptr.f64_sum += @floatFromInt(vals[r]);
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                acc_ptr.f64_sum += @floatFromInt(@as(u64, @bitCast(simd.sumU64(vals[0..c.num_rows]))));
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        acc_ptr.f64_sum += @floatFromInt(vals[r]);
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_sum) {
                                            acc_ptr.f64_sum += simd.sumF64(vals[0..c.num_rows]);
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .min => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minI64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.i64_min)
                                                        acc_ptr.i64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                     },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.u64_min)
                                                    acc_ptr.u64_min = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minF64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.f64_min) acc_ptr.f64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.f64_min)
                                                        acc_ptr.f64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .max => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxI64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.i64_max)
                                                        acc_ptr.i64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.u64_max)
                                                    acc_ptr.u64_max = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxF64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.f64_max) acc_ptr.f64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.f64_max)
                                                        acc_ptr.f64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
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
    var row_buf: []?Value = &.{}; // allocated once on first chunk

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
            // Allocate row_buf once (reused across all chunks).
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
        }
        const refs = row_ref_indices orelse &[_]usize{};

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

/// Heap-based top-K selection: O(n log k) time, O(k) extra memory.
/// Returns exactly min(k, n) rows in sorted order.
fn executeTopK(inner: RowList, keys: []const plan.SortKey, k: usize, alloc: std.mem.Allocator) !RowList {
    const rows = inner.rows.items;
    if (rows.len == 0 or k == 0) return RowList.init(inner.metas);

    // SortCtx: lessThan(a, b) = true means a should appear before b in the output.
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
                    .lt
                else
                    .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
        // heapLess(a,b): for a min-heap of the BEST k rows, the "min" is the
        // worst element (the one we'd evict). So heapLess = !lessThan.
        fn heapLess(self: @This(), a: []?Value, b: []?Value) std.math.Order {
            if (self.lessThan(a, b)) return .lt;
            if (self.lessThan(b, a)) return .gt;
            return .eq;
        }
    };
    const ctx = SortCtx{ .keys = keys };

    // Build a min-heap (worst-of-best k) to track the top-k rows.
    const heap_buf = try alloc.alloc([]?Value, k);
    var heap_len: usize = 0;

    for (rows) |row| {
        if (heap_len < k) {
            heap_buf[heap_len] = row;
            heap_len += 1;
            // Sift up.
            var i = heap_len - 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (ctx.lessThan(heap_buf[i], heap_buf[parent])) {
                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[parent]; heap_buf[parent] = tmp;
                    i = parent;
                } else break;
            }
        } else {
            // If this row is better than the heap root (worst of current best), replace root.
            if (ctx.lessThan(row, heap_buf[0])) {
                heap_buf[0] = row;
                // Sift down.
                var i: usize = 0;
                while (true) {
                    const l = 2 * i + 1;
                    const r = 2 * i + 2;
                    var smallest = i;
                    if (l < heap_len and ctx.lessThan(heap_buf[l], heap_buf[smallest])) smallest = l;
                    if (r < heap_len and ctx.lessThan(heap_buf[r], heap_buf[smallest])) smallest = r;
                    if (smallest == i) break;
                    const tmp = heap_buf[i]; heap_buf[i] = heap_buf[smallest]; heap_buf[smallest] = tmp;
                    i = smallest;
                }
            }
        }
    }

    // Sort the heap to get the final ordered result.
    std.sort.pdq([]?Value, heap_buf[0..heap_len], ctx, SortCtx.lessThan);

    var rl = RowList.init(inner.metas);
    for (heap_buf[0..heap_len]) |row| try rl.append(alloc, row);
    return rl;
}

/// Stream a scannable node (scan/filter/project/limit) directly into a min-heap
/// of at most K rows, avoiding materialisation of all rows into a RowList.
/// Only rows that actually enter the heap (≤ K rows) are fully materialised via readRow.
/// Read a single row from a slice of columns (without a DataChunk wrapper).
fn readRowFromCols(cols: []const chunk.Column, row: usize, a: std.mem.Allocator) ![]?Value {
    const vals = try a.alloc(?Value, cols.len);
    for (cols, 0..) |col, ci| vals[ci] = if (col.isRowNull(row)) null else col.data.get(row);
    return vals;
}

fn executeTopKFromScannable(
    node: *const plan.PhysicalNode,
    keys: []const plan.SortKey,
    k:    usize,
    ctx:  *QueryContext,
) !RowList {
    const alloc = ctx.allocator();

    // Traverse to extract filter / project / limit wrappers.
    var filter_state:  ?FilterState               = null;
    var project_items: ?[]const plan.ProjectItem  = null;
    var lim_state:     ?LimitState                = null;
    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit   => |lim| { if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset }; cur = lim.input; },
            .filter  => |f|   { if (filter_state == null) filter_state = .{ .predicate = f.predicate }; cur = f.input; },
            .project => |p|   { if (project_items == null) project_items = p.items; cur = p.input; },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);

    if (k == 0) return RowList.init(out_metas);

    const SortCtx = struct {
        keys: []const plan.SortKey,
        fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
            for (self.keys) |key| {
                const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (av != null and bv != null)
                    Value.order(av.?, bv.?)
                else if (av == null and bv == null) .eq
                else if (av == null) .lt
                else .gt;
                if (ord == .eq) continue;
                return if (key.desc) ord == .gt else ord == .lt;
            }
            return false;
        }
    };
    const sctx = SortCtx{ .keys = keys };

    // Strategy: accumulate up to K raw (pre-projection) schema rows in the heap.
    // Project only the final K winners — avoids projecting all 300K+ matching rows.
    // sort key col_idx = schema column index (same in pre- and post-projection).
    const heap_buf = try alloc.alloc([]?Value, k);
    var heap_len: usize = 0;

    // Scratch: only key columns need to be read for heap-root comparison.
    const num_schema_cols = schema_metas.len;
    const key_scratch = try alloc.alloc(?Value, num_schema_cols);
    @memset(key_scratch, null);

    ctx.source.reset();
    var c: DataChunk = undefined;
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (lim_state)    |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }

        for (0..c.num_rows) |r| {
            if (heap_len < k) {
                // Heap not full: read full raw row and insert.
                const row = try c.readRow(r, alloc);
                heap_buf[heap_len] = row;
                heap_len += 1;
                // Sift up.
                var i = heap_len - 1;
                while (i > 0) {
                    const parent = (i - 1) / 2;
                    if (sctx.lessThan(heap_buf[i], heap_buf[parent])) {
                        const tmp = heap_buf[i]; heap_buf[i] = heap_buf[parent]; heap_buf[parent] = tmp;
                        i = parent;
                    } else break;
                }
            } else {
                // Heap full: read only sort-key columns to compare against heap root.
                for (keys) |key| {
                    if (key.col_idx < c.columns.len) {
                        const col = &c.columns[key.col_idx];
                        key_scratch[key.col_idx] = if (col.isRowNull(r)) null else col.data.get(r);
                    }
                }
                if (sctx.lessThan(key_scratch, heap_buf[0])) {
                    // Winner: materialise full raw row and replace heap root.
                    const row = try c.readRow(r, alloc);
                    heap_buf[0] = row;
                    // Sift down.
                    var i: usize = 0;
                    while (true) {
                        const l = 2 * i + 1;
                        const r2 = 2 * i + 2;
                        var smallest = i;
                        if (l < heap_len and sctx.lessThan(heap_buf[l], heap_buf[smallest])) smallest = l;
                        if (r2 < heap_len and sctx.lessThan(heap_buf[r2], heap_buf[smallest])) smallest = r2;
                        if (smallest == i) break;
                        const tmp = heap_buf[i]; heap_buf[i] = heap_buf[smallest]; heap_buf[smallest] = tmp;
                        i = smallest;
                    }
                }
            }
        }
        if (lim_state) |ls| if (ls.done()) break;
    }

    std.sort.pdq([]?Value, heap_buf[0..heap_len], sctx, SortCtx.lessThan);

    // Project the K winners (only K rows — negligible cost).
    var rl = RowList.init(out_metas);
    if (project_items) |items| {
        const row_buf = try alloc.alloc(?Value, schema_metas.len);
        for (heap_buf[0..heap_len]) |raw_row| {
            const proj_row = try alloc.alloc(?Value, items.len);
            @memcpy(row_buf[0..raw_row.len], raw_row);
            for (items, 0..) |item, ci| {
                proj_row[ci] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
            }
            try rl.append(alloc, proj_row);
        }
    } else {
        for (heap_buf[0..heap_len]) |row| try rl.append(alloc, row);
    }
    return rl;
}


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
        .agg_call => |ac| if (ac.arg) |arg| blk: {
            // Inline fast paths for common single-arg function calls to avoid dispatch overhead.
            if (arg == .fn_call) {
                const fc = arg.fn_call;
                if (fc.args.len == 1 and fc.args[0] == .col_ref) {
                    const col_val = row[fc.args[0].col_ref.index] orelse break :blk null;
                    if (std.mem.eql(u8, fc.name, "length") or
                        std.mem.eql(u8, fc.name, "char_length") or
                        std.mem.eql(u8, fc.name, "len"))
                    {
                        const s = col_val.toStr() orelse break :blk null;
                        break :blk Value{ .int64 = @intCast(s.len) };
                    }
                }
            }
            break :blk kernels.evalExpr(arg, row, null, alloc);
        } else null,
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
        .rowCount  = struct { fn f(_: *anyopaque) u64 { return 0; } }.f,
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
