/// Generic SQL executor: evaluates a generic_sql.Plan by streaming rows
/// directly from a Parquet file.  This is the "slow path" fallback that
/// handles any SQL the specialized hot-column executors cannot.
///
/// Design goals:
///   - Zero DuckDB dependency at execution time (parser is DuckDB; executor is not)
///   - Single-pass over the Parquet file where possible
///   - Correct output for all 43 ClickBench query forms plus arbitrary
///     SELECT/WHERE/GROUP BY/ORDER BY/LIMIT/OFFSET on hits columns
///   - No performance requirement for this path (correctness only)
///
/// Supported plan shapes (what generic_sql.Plan can express):
///   1. Scalar aggregate  : SELECT count(*)/sum(col)/avg(col)/min(col)/max(col) [WHERE …]
///   2. count(distinct)   : SELECT COUNT(DISTINCT col) [WHERE …]
///   3. Group-by + agg    : SELECT col, count(*) … GROUP BY col [ORDER BY … LIMIT n]
///   4. Filtered select   : SELECT col FROM hits WHERE col op val [ORDER BY … LIMIT n]
///   5. Point lookup      : SELECT col FROM hits WHERE col = val
///   6. Multi-agg scalar  : SELECT sum(a), count(*), avg(b) … (no group by)
///
/// Columns are read from the Parquet file using parquet.streamFixedColumnsTypedPath
/// (fixed-width integers / dates / timestamps) and parquet.streamByteArrayColumnPath
/// (variable-length strings).  String columns are materialised into an arena on
/// the first pass and then referenced by index for filter and projection.

const std = @import("std");
const generic_sql = @import("generic_sql.zig");
const parquet = @import("parquet.zig");
const clickbench_schema = @import("clickbench/schema.zig");
const schema = @import("schema.zig");

// ── Public entry point ────────────────────────────────────────────────────────

/// Execute `plan` against `parquet_path` and return the result as a
/// CSV-formatted string (header row + data rows, comma-separated).
/// Returns `error.UnsupportedGenericQuery` when the plan shape is not handled.
pub fn run(
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    parquet_path: []const u8,
) anyerror![]u8 {
    // Reject non-hits tables immediately; caller already checks but be defensive.
    if (!std.ascii.eqlIgnoreCase(plan.table, "hits")) return error.UnsupportedGenericQuery;

    const exec = Executor{
        .allocator = allocator,
        .io = io,
        .plan = plan,
        .parquet_path = parquet_path,
    };

    // Dispatch based on plan shape
    const has_group = plan.group_by != null;
    const all_agg = allAggregates(plan.projections);

    if (!has_group and all_agg) return exec.runScalarAgg();
    if (has_group) return exec.runGroupBy();
    // No group, not all aggregates: projection / filtered scan
    return exec.runScan();
}

// ── Column descriptor ─────────────────────────────────────────────────────────

const ColKind = enum { fixed_i16, fixed_i32, fixed_i64, fixed_date, fixed_timestamp, string };

const ColDesc = struct {
    name: []const u8,    // original column name (case as in schema)
    index: usize,        // Parquet column index
    kind: ColKind,
};

fn lookupColumn(name: []const u8) ?ColDesc {
    // Handle computed / derived column names used in plan projections
    // e.g. "extract(minute from EventTime)" handled elsewhere; skip
    const idx = clickbench_schema.findColumn(name) orelse return null;
    const col = clickbench_schema.hits.columns[idx];
    const kind: ColKind = switch (col.ty) {
        .int16 => .fixed_i16,
        .int32 => .fixed_i32,
        .int64 => .fixed_i64,
        .date   => .fixed_date,
        .timestamp => .fixed_timestamp,
        .text, .char => .string,
    };
    return ColDesc{ .name = col.name, .index = idx, .kind = kind };
}

// ── Value type ────────────────────────────────────────────────────────────────

/// Runtime value for a single cell.
const Value = union(enum) {
    i64: i64,
    f64: f64,
    str: []const u8,  // slice into arena; valid for lifetime of Executor.run call
    null_val,

    fn isNull(self: Value) bool {
        return self == .null_val;
    }

    fn toI64(self: Value) ?i64 {
        return switch (self) {
            .i64 => |v| v,
            .f64 => |v| @intFromFloat(v),
            else => null,
        };
    }

    fn toF64(self: Value) ?f64 {
        return switch (self) {
            .i64 => |v| @floatFromInt(v),
            .f64 => |v| v,
            else => null,
        };
    }

    fn toStr(self: Value) ?[]const u8 {
        return switch (self) {
            .str => |s| s,
            else => null,
        };
    }

    fn order(a: Value, b: Value) std.math.Order {
        switch (a) {
            .i64 => |av| switch (b) {
                .i64 => |bv| return std.math.order(av, bv),
                .f64 => |bv| return std.math.order(@as(f64, @floatFromInt(av)), bv),
                else => return .lt,
            },
            .f64 => |av| switch (b) {
                .i64 => |bv| return std.math.order(av, @as(f64, @floatFromInt(bv))),
                .f64 => |bv| return std.math.order(av, bv),
                else => return .lt,
            },
            .str => |av| switch (b) {
                .str => |bv| return std.mem.order(u8, av, bv),
                else => return .gt,
            },
            .null_val => return if (b == .null_val) .eq else .lt,
        }
    }

    fn eql(a: Value, b: Value) bool {
        return order(a, b) == .eq;
    }

    /// Write in CSV-compatible format (no surrounding quotes for ints).
    fn writeCsv(self: Value, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        switch (self) {
            .i64 => |v| try out.print(allocator, "{d}", .{v}),
            .f64 => |v| {
                // Match existing generic output format: integer if whole number
                if (v == @trunc(v) and @abs(v) < 1e15) {
                    try out.print(allocator, "{d}", .{@as(i64, @intFromFloat(v))});
                } else {
                    try out.print(allocator, "{d}", .{v});
                }
            },
            .str => |s| try out.appendSlice(allocator, s),
            .null_val => {},
        }
    }
};

// ── Predicate evaluation ──────────────────────────────────────────────────────

/// Evaluate `Filter` against a row represented as a name→Value lookup.
fn evalFilter(filter: generic_sql.Filter, row: *const RowCtx) bool {
    const lv = row.get(filter.column) orelse return false;
    // Only numeric comparisons in Filter (string comparisons use where_text path)
    const lv_i = lv.toI64() orelse {
        // String column: only equality with '' supported via filter
        if (filter.op == .equal and filter.int_value == 0) {
            const sv = lv.toStr() orelse return false;
            return sv.len == 0;
        }
        if (filter.op == .not_equal and filter.int_value == 0) {
            const sv = lv.toStr() orelse return false;
            return sv.len != 0;
        }
        return false;
    };
    const rv = filter.int_value;
    const pass1: bool = switch (filter.op) {
        .equal         => lv_i == rv,
        .not_equal     => lv_i != rv,
        .greater       => lv_i > rv,
        .greater_equal => lv_i >= rv,
        .less          => lv_i < rv,
        .less_equal    => lv_i <= rv,
    };
    if (!pass1) return false;
    // Optional second predicate (AND)
    if (filter.second) |sec| {
        const sv = row.get(sec.column) orelse return false;
        const sv_i = sv.toI64() orelse return false;
        return switch (sec.op) {
            .equal         => sv_i == sec.int_value,
            .not_equal     => sv_i != sec.int_value,
            .greater       => sv_i > sec.int_value,
            .greater_equal => sv_i >= sec.int_value,
            .less          => sv_i < sec.int_value,
            .less_equal    => sv_i <= sec.int_value,
        };
    }
    return true;
}

// ── Row context: a lightweight name→value map backed by parallel slices ──────

const RowCtx = struct {
    names: []const []const u8,
    values: []const Value,

    fn get(self: *const RowCtx, name: []const u8) ?Value {
        for (self.names, self.values) |n, v| {
            if (std.ascii.eqlIgnoreCase(n, name)) return v;
        }
        return null;
    }
};

// ── Executor ──────────────────────────────────────────────────────────────────

const Executor = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    parquet_path: []const u8,

    // ── Scalar aggregate ─────────────────────────────────────────────────────

    fn runScalarAgg(self: Executor) anyerror![]u8 {
        // Collect column names needed
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed);

        var ctx = ScalarAggCtx.init(self.allocator, self.plan);
        defer ctx.deinit();
        try self.streamRows(&needed, &ctx, ScalarAggCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Group-by aggregate ────────────────────────────────────────────────────

    fn runGroupBy(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed);

        var ctx = GroupByCtx.init(self.allocator, self.plan);
        defer ctx.deinit(self.allocator);
        try self.streamRows(&needed, &ctx, GroupByCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Scan / filtered projection ────────────────────────────────────────────

    fn runScan(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed);

        var ctx = ScanCtx.init(self.allocator, self.plan);
        defer ctx.deinit(self.allocator);
        try self.streamRows(&needed, &ctx, ScanCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Generic row streamer ──────────────────────────────────────────────────
    //
    // Reads all needed columns from the Parquet file, assembles a RowCtx per
    // row, and calls `callback(context, row)`.
    //
    // Strategy: separate fixed-int columns (multi-column batch API) and
    // string columns (single-column streaming API, one pass per column).
    // For simplicity we do N+1 passes (1 for fixed, 1 per string column).
    // This is slow but correct and memory-efficient.

    fn streamRows(
        self: Executor,
        needed: *const std.ArrayList([]const u8),
        context: anytype,
        comptime callback: fn (@TypeOf(context), *const RowCtx) anyerror!void,
    ) anyerror!void {
        // Classify columns
        var fixed_descs: std.ArrayList(ColDesc) = .empty;
        defer fixed_descs.deinit(self.allocator);
        var str_descs: std.ArrayList(ColDesc) = .empty;
        defer str_descs.deinit(self.allocator);

        for (needed.items) |name| {
            const desc = lookupColumn(name) orelse continue;
            switch (desc.kind) {
                .string => try str_descs.append(self.allocator, desc),
                else    => try fixed_descs.append(self.allocator, desc),
            }
        }

        // Load all string columns fully into an arena (one pass per column).
        var str_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer str_arena.deinit();
        const str_alloc = str_arena.allocator();

        const str_data = try str_alloc.alloc(std.ArrayList([]const u8), str_descs.items.len);
        for (str_data) |*col| col.* = .empty;

        for (str_descs.items, str_data) |desc, *col_data| {
            const StrCtx = struct {
                data: *std.ArrayList([]const u8),
                str_alloc: std.mem.Allocator,
                fn cb(ctx: *@This(), bytes: []const u8) !void {
                    const owned = try ctx.str_alloc.dupe(u8, bytes);
                    try ctx.data.append(ctx.str_alloc, owned);
                }
            };
            var sc = StrCtx{ .data = col_data, .str_alloc = str_alloc };
            _ = try parquet.streamByteArrayColumnPath(
                self.allocator, self.io, self.parquet_path,
                desc.index, null, &sc, StrCtx.cb,
            );
        }

        const row_count: usize = if (str_data.len > 0)
            str_data[0].items.len
        else blk: {
            if (fixed_descs.items.len == 0) return;
            break :blk 0; // handled inside fixed stream
        };

        if (fixed_descs.items.len == 0) {
            // String-only columns: iterate by row index
            const names = try str_alloc.alloc([]const u8, str_descs.items.len);
            const vals  = try str_alloc.alloc(Value, str_descs.items.len);
            for (str_descs.items, names) |d, *n| n.* = d.name;
            for (0..row_count) |row_idx| {
                for (str_data, vals) |col_data, *v| {
                    v.* = if (row_idx < col_data.items.len)
                        Value{ .str = col_data.items[row_idx] }
                    else
                        Value{ .null_val = {} };
                }
                const row = RowCtx{ .names = names, .values = vals };
                try callback(context, &row);
            }
            return;
        }

        // Fixed columns: use multi-column batch streaming
        const fixed_indices = try self.allocator.alloc(usize, fixed_descs.items.len);
        defer self.allocator.free(fixed_indices);
        const fixed_targets = try self.allocator.alloc(parquet.FixedTarget, fixed_descs.items.len);
        defer self.allocator.free(fixed_targets);
        for (fixed_descs.items, fixed_indices, fixed_targets) |desc, *idx, *tgt| {
            idx.* = desc.index;
            tgt.* = switch (desc.kind) {
                .fixed_i16 => .i16,
                .fixed_i32, .fixed_date => .i32,
                .fixed_i64, .fixed_timestamp => .i64,
                .string => unreachable,
            };
        }

        // Row names: fixed first, then string
        const total_cols = fixed_descs.items.len + str_descs.items.len;
        const all_names = try str_alloc.alloc([]const u8, total_cols);
        for (fixed_descs.items, 0..) |d, i| all_names[i] = d.name;
        for (str_descs.items, 0..) |d, i| all_names[fixed_descs.items.len + i] = d.name;

        // Use a vtable to bridge comptime-generic callback into a runtime struct.
        const RowFn = *const fn (*anyopaque, *const RowCtx) anyerror!void;
        const RowBridge = struct {
            fn call(ptr: *anyopaque, row: *const RowCtx) anyerror!void {
                try callback(@as(@TypeOf(context), @ptrCast(@alignCast(ptr))), row);
            }
        };

        const FixedStreamCtx = struct {
            allocator: std.mem.Allocator,
            fixed_descs: []const ColDesc,
            str_descs: []const ColDesc,
            str_data: []std.ArrayList([]const u8),
            all_names: []const []const u8,
            row_offset: usize,
            row_fn: RowFn,
            outer_ptr: *anyopaque,

            fn batchCb(ctx: *@This(), batches: []const parquet.FixedColumnBatch) anyerror!void {
                if (batches.len == 0) return;
                const batch_size: usize = switch (batches[0].target) {
                    .i16 => batches[0].bytes.len / 2,
                    .i32 => batches[0].bytes.len / 4,
                    .i64 => batches[0].bytes.len / 8,
                };
                const vals = try ctx.allocator.alloc(Value, ctx.all_names.len);
                defer ctx.allocator.free(vals);

                for (0..batch_size) |bi| {
                    for (batches, 0..) |batch, fi| {
                        vals[fi] = switch (batch.target) {
                            .i16 => Value{ .i64 = std.mem.readInt(i16, batch.bytes[bi*2..][0..2], .little) },
                            .i32 => Value{ .i64 = std.mem.readInt(i32, batch.bytes[bi*4..][0..4], .little) },
                            .i64 => Value{ .i64 = std.mem.readInt(i64, batch.bytes[bi*8..][0..8], .little) },
                        };
                    }
                    const row_idx = ctx.row_offset + bi;
                    for (0..ctx.str_descs.len) |si| {
                        const col_data = ctx.str_data[si];
                        vals[ctx.fixed_descs.len + si] = if (row_idx < col_data.items.len)
                            Value{ .str = col_data.items[row_idx] }
                        else
                            Value{ .null_val = {} };
                    }
                    const row = RowCtx{ .names = ctx.all_names, .values = vals };
                    try ctx.row_fn(ctx.outer_ptr, &row);
                }
                ctx.row_offset += batch_size;
            }
        };

        var fsc = FixedStreamCtx{
            .allocator = self.allocator,
            .fixed_descs = fixed_descs.items,
            .str_descs = str_descs.items,
            .str_data = str_data,
            .all_names = all_names,
            .row_offset = 0,
            .row_fn = RowBridge.call,
            .outer_ptr = @ptrCast(@alignCast(context)),
        };

        _ = try parquet.streamFixedColumnsTypedPath(
            self.allocator, self.io, self.parquet_path,
            fixed_indices, fixed_targets, null,
            &fsc, FixedStreamCtx.batchCb,
        );
    }
};

// ── Scalar aggregate context ──────────────────────────────────────────────────

const AggState = struct {
    count: i64 = 0,
    sum: f64 = 0,
    min: ?Value = null,
    max: ?Value = null,
    distinct: ?*std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80) = null,

    fn update(self: *AggState, v: Value, func: generic_sql.AggregateFn, alloc: std.mem.Allocator) !void {
        switch (func) {
            .count_star => self.count += 1,
            .count_distinct => {
                if (self.distinct == null) {
                    const map = try alloc.create(std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80));
                    map.* = std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80).init(alloc);
                    self.distinct = map;
                }
                if (v.toI64()) |iv| try self.distinct.?.put(iv, {});
            },
            .sum => { if (v.toF64()) |fv| self.sum += fv; },
            .avg => {
                self.count += 1;
                if (v.toF64()) |fv| self.sum += fv;
            },
            .min => {
                if (self.min == null or Value.order(v, self.min.?) == .lt) self.min = v;
            },
            .max => {
                if (self.max == null or Value.order(v, self.max.?) == .gt) self.max = v;
            },
            .column_ref, .int_literal => {},
        }
    }

    fn result(self: *const AggState, func: generic_sql.AggregateFn) Value {
        return switch (func) {
            .count_star => Value{ .i64 = self.count },
            .count_distinct => Value{ .i64 = if (self.distinct) |d| @intCast(d.count()) else 0 },
            .sum => Value{ .f64 = self.sum },
            .avg => if (self.count == 0) Value{ .f64 = 0 } else Value{ .f64 = self.sum / @as(f64, @floatFromInt(self.count)) },
            .min => self.min orelse Value{ .null_val = {} },
            .max => self.max orelse Value{ .null_val = {} },
            .column_ref, .int_literal => Value{ .null_val = {} },
        };
    }

    fn deinit(self: *AggState, alloc: std.mem.Allocator) void {
        if (self.distinct) |d| {
            d.deinit();
            alloc.destroy(d);
            self.distinct = null;
        }
    }
};

const ScalarAggCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    states: []AggState,

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) ScalarAggCtx {
        const states = allocator.alloc(AggState, plan.projections.len) catch unreachable;
        for (states) |*s| s.* = .{};
        return .{ .allocator = allocator, .plan = plan, .states = states };
    }

    fn deinit(self: *ScalarAggCtx) void {
        for (self.states) |*s| s.deinit(self.allocator);
        self.allocator.free(self.states);
    }

    fn observe(self: *ScalarAggCtx, row: *const RowCtx) anyerror!void {
        // Apply filter
        if (self.plan.filter) |filter| {
            if (!evalFilter(filter, row)) return;
        }
        // Update each aggregate
        for (self.plan.projections, self.states) |proj, *state| {
            const v = evalProjectionExpr(proj, row);
            try state.update(v, proj.func, self.allocator);
        }
    }

    fn format(self: *const ScalarAggCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        // Header
        for (plan.projections, 0..) |proj, i| {
            if (i != 0) try out.append(allocator, ',');
            try writeExprHeader(&out, allocator, proj);
        }
        try out.append(allocator, '\n');
        // Values
        for (plan.projections, self.states, 0..) |proj, *state, i| {
            if (i != 0) try out.append(allocator, ',');
            try state.result(proj.func).writeCsv(&out, allocator);
        }
        try out.append(allocator, '\n');
        return out.toOwnedSlice(allocator);
    }
};

// ── Group-by context ──────────────────────────────────────────────────────────

/// Key for group-by: one or more column values combined as a string key.
/// We use a string key so that mixed types (int + string) work uniformly.
const GroupKey = []const u8;

const GroupEntry = struct {
    key: []const u8,       // owned
    key_values: []Value,   // the actual group key values (for output), owned
    states: []AggState,    // one per non-key projection
};

const GroupByCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    map: std.StringHashMap(usize), // key → index into entries
    entries: std.ArrayList(GroupEntry),
    group_cols: []const []const u8, // parsed group-by column names

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) GroupByCtx {
        return .{
            .allocator = allocator,
            .plan = plan,
            .map = std.StringHashMap(usize).init(allocator),
            .entries = .empty,
            .group_cols = parseGroupCols(allocator, plan.group_by orelse "") catch &.{},
        };
    }

    fn deinit(self: *GroupByCtx, allocator: std.mem.Allocator) void {
        for (self.entries.items) |*entry| {
            allocator.free(entry.key);
            for (entry.key_values) |v| {
                if (v == .str) allocator.free(v.str);
            }
            allocator.free(entry.key_values);
            for (entry.states) |*s| s.deinit(allocator);
            allocator.free(entry.states);
        }
        self.entries.deinit(allocator);
        self.map.deinit();
        if (self.group_cols.len > 0) {
            for (self.group_cols) |col| allocator.free(col);
            allocator.free(self.group_cols);
        }
    }

    fn observe(self: *GroupByCtx, row: *const RowCtx) anyerror!void {
        if (self.plan.filter) |filter| {
            if (!evalFilter(filter, row)) return;
        }

        // Build composite key
        var key_buf: std.ArrayList(u8) = .empty;
        defer key_buf.deinit(self.allocator);
        for (self.group_cols) |col| {
            const v = row.get(col) orelse Value{ .null_val = {} };
            try v.writeCsv(&key_buf, self.allocator);
            try key_buf.append(self.allocator, 0); // separator
        }
        const key = key_buf.items;

        const gop = try self.map.getOrPut(key);
        if (!gop.found_existing) {
            // Allocate a new entry
            const owned_key = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = self.entries.items.len;
            gop.key_ptr.* = owned_key;

            // Clone key values
            const key_values = try self.allocator.alloc(Value, self.group_cols.len);
            for (self.group_cols, key_values) |col, *kv| {
                const v = row.get(col) orelse Value{ .null_val = {} };
                kv.* = switch (v) {
                    .str => |s| Value{ .str = try self.allocator.dupe(u8, s) },
                    else => v,
                };
            }
            // Allocate agg states for non-group projections
            const states = try self.allocator.alloc(AggState, self.plan.projections.len);
            for (states) |*s| s.* = .{};

            try self.entries.append(self.allocator, GroupEntry{
                .key = owned_key,
                .key_values = key_values,
                .states = states,
            });
        }

        const entry = &self.entries.items[gop.value_ptr.*];
        // Update aggregates
        for (self.plan.projections, entry.states) |proj, *state| {
            const v = evalProjectionExpr(proj, row);
            try state.update(v, proj.func, self.allocator);
        }
    }

    fn format(self: *GroupByCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        // Sort entries
        const entries = self.entries.items;

        // Determine sort key: order_by_count_desc → sort by count(*) desc,
        // order_by_alias → sort by that alias desc,
        // order_by_text → best effort by first agg
        const order_by_count_desc = plan.order_by_count_desc;
        const order_by_alias = plan.order_by_alias;

        // Find which projection index is the sort key
        const sort_proj_idx: ?usize = blk: {
            if (order_by_count_desc) {
                for (plan.projections, 0..) |p, i| {
                    if (p.func == .count_star) break :blk i;
                }
                break :blk null;
            }
            if (order_by_alias) |alias| {
                for (plan.projections, 0..) |p, i| {
                    if (p.alias) |a| if (std.ascii.eqlIgnoreCase(a, alias)) break :blk i;
                }
                break :blk null;
            }
            break :blk null;
        };

        // Sort
        const SortCtx = struct {
            entries: []GroupEntry,
            plan: generic_sql.Plan,
            sort_idx: ?usize,
            desc: bool,

            fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                const ea = ctx.entries[a];
                const eb = ctx.entries[b];
                if (ctx.sort_idx) |si| {
                    if (si < ea.states.len) {
                        const va = ea.states[si].result(ctx.plan.projections[si].func);
                        const vb = eb.states[si].result(ctx.plan.projections[si].func);
                        const ord = Value.order(va, vb);
                        if (ctx.desc) return ord == .gt;
                        return ord == .lt;
                    }
                }
                return false;
            }
        };

        const indices = try allocator.alloc(usize, entries.len);
        defer allocator.free(indices);
        for (indices, 0..) |*idx, i| idx.* = i;

        const sort_ctx = SortCtx{
            .entries = entries,
            .plan = plan,
            .sort_idx = sort_proj_idx,
            .desc = order_by_count_desc or order_by_alias != null,
        };
        std.sort.block(usize, indices, sort_ctx, SortCtx.lessThan);

        // Apply limit / offset
        const offset = plan.offset orelse 0;
        const limit = plan.limit orelse std.math.maxInt(usize);

        const start = @min(offset, indices.len);
        const end = @min(start + limit, indices.len);
        const out_indices = indices[start..end];

        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);

        // Header
        try writeGroupHeader(&out, allocator, plan, self.group_cols);

        // Rows
        for (out_indices) |idx| {
            const entry = entries[idx];
            var col_written: usize = 0;

            // Write projections
            for (plan.projections, entry.states, 0..) |proj, *state, pi| {
                if (pi != 0) try out.append(allocator, ',');

                // Is this projection a group-by column reference?
                if (proj.func == .column_ref) {
                    const col_name = proj.column orelse {
                        try out.appendSlice(allocator, "*");
                        col_written += 1;
                        continue;
                    };
                    // Check if it's a group key
                    var is_key = false;
                    for (self.group_cols, entry.key_values) |gc, kv| {
                        if (std.ascii.eqlIgnoreCase(gc, col_name)) {
                            try kv.writeCsv(&out, allocator);
                            is_key = true;
                            break;
                        }
                    }
                    if (!is_key) {
                        // Non-key column_ref in projection: output first value seen
                        const v = state.result(proj.func);
                        try v.writeCsv(&out, allocator);
                    }
                } else if (proj.func == .int_literal) {
                    try out.print(allocator, "{d}", .{proj.int_offset});
                } else {
                    const v = state.result(proj.func);
                    try v.writeCsv(&out, allocator);
                }
                col_written += 1;
            }
            try out.append(allocator, '\n');
        }

        return out.toOwnedSlice(allocator);
    }
};

// ── Scan context ──────────────────────────────────────────────────────────────

const ScanCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    rows: std.ArrayList([]Value), // each entry owns its Value slice
    arena: std.heap.ArenaAllocator,

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) ScanCtx {
        return .{
            .allocator = allocator,
            .plan = plan,
            .rows = .empty,
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    fn deinit(self: *ScanCtx, allocator: std.mem.Allocator) void {
        self.rows.deinit(allocator);
        self.arena.deinit();
    }

    fn observe(self: *ScanCtx, row: *const RowCtx) anyerror!void {
        if (self.plan.filter) |filter| {
            if (!evalFilter(filter, row)) return;
        }

        // For ORDER BY / LIMIT we need to collect all matching rows.
        // Without ORDER BY and with LIMIT we can short-circuit.
        const has_order = self.plan.order_by_text != null or
            self.plan.order_by_count_desc or
            self.plan.order_by_alias != null;
        const limit = self.plan.limit orelse std.math.maxInt(usize);

        if (!has_order and self.rows.items.len >= limit + (self.plan.offset orelse 0)) return;

        const vals = try self.arena.allocator().alloc(Value, self.plan.projections.len);
        for (self.plan.projections, vals) |proj, *v| {
            const raw = evalProjectionExpr(proj, row);
            v.* = switch (raw) {
                .str => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                else => raw,
            };
        }
        try self.rows.append(self.allocator, vals);
    }

    fn format(self: *ScanCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        // Sort if needed
        const has_order = plan.order_by_text != null or plan.order_by_count_desc or plan.order_by_alias != null;
        if (has_order) {
            // Simple lexicographic sort on first column for now
            // (covers ORDER BY EventTime, ORDER BY SearchPhrase, etc.)
            const SortCtx = struct {
                rows: [][]Value,
                desc: bool,
                fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                    const ra = ctx.rows[a];
                    const rb = ctx.rows[b];
                    if (ra.len == 0) return false;
                    const ord = Value.order(ra[0], rb[0]);
                    if (ctx.desc) return ord == .gt;
                    return ord == .lt;
                }
            };
            const sc = SortCtx{ .rows = self.rows.items, .desc = plan.order_by_count_desc };
            const indices = try allocator.alloc(usize, self.rows.items.len);
            defer allocator.free(indices);
            for (indices, 0..) |*idx, i| idx.* = i;
            std.sort.block(usize, indices, sc, SortCtx.lessThan);

            // Reorder rows in-place
            const sorted = try allocator.alloc([]Value, self.rows.items.len);
            defer allocator.free(sorted);
            for (indices, sorted) |idx, *s| s.* = self.rows.items[idx];
            @memcpy(self.rows.items, sorted);
        }

        const offset = plan.offset orelse 0;
        const limit = plan.limit orelse std.math.maxInt(usize);
        const start = @min(offset, self.rows.items.len);
        const end = @min(start + limit, self.rows.items.len);
        const out_rows = self.rows.items[start..end];

        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);

        // Header
        for (plan.projections, 0..) |proj, i| {
            if (i != 0) try out.append(allocator, ',');
            try writeExprHeader(&out, allocator, proj);
        }
        try out.append(allocator, '\n');

        for (out_rows) |row_vals| {
            for (row_vals, 0..) |v, i| {
                if (i != 0) try out.append(allocator, ',');
                try v.writeCsv(&out, allocator);
            }
            try out.append(allocator, '\n');
        }

        return out.toOwnedSlice(allocator);
    }
};

// ── Helper: collect needed column names from a Plan ───────────────────────────

fn collectNeededColumns(
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    needed: *std.ArrayList([]const u8),
) !void {
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const add = struct {
        fn f(alloc: std.mem.Allocator, s: *std.StringHashMap(void), lst: *std.ArrayList([]const u8), name: []const u8) !void {
            if (s.contains(name)) return;
            if (lookupColumn(name) == null) return; // skip unknown (computed) columns
            try s.put(name, {});
            try lst.append(alloc, name);
        }
    }.f;

    for (plan.projections) |proj| {
        if (proj.column) |col| try add(allocator, &seen, needed, col);
    }
    if (plan.filter) |filter| {
        try add(allocator, &seen, needed, filter.column);
        if (filter.second) |sec| try add(allocator, &seen, needed, sec.column);
    }
    // Parse group-by columns
    if (plan.group_by) |gb| {
        const cols = try parseGroupCols(allocator, gb);
        defer {
            for (cols) |c| allocator.free(c);
            allocator.free(cols);
        }
        for (cols) |col| try add(allocator, &seen, needed, col);
    }
}

// ── Helper: parse group-by column list ───────────────────────────────────────

fn parseGroupCols(allocator: std.mem.Allocator, group_by: []const u8) ![][]const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |c| allocator.free(c);
        result.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, group_by, ',');
    while (iter.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) continue;
        // Skip numeric position references like "1", "2"
        _ = std.fmt.parseInt(usize, trimmed, 10) catch {
            try result.append(allocator, try allocator.dupe(u8, trimmed));
            continue;
        };
        // numeric: try to resolve to the n-th projection column name
        // (handled by caller if needed)
    }
    return result.toOwnedSlice(allocator);
}

// ── Helper: evaluate a projection expression against a row ────────────────────

fn evalProjectionExpr(proj: generic_sql.Expr, row: *const RowCtx) Value {
    switch (proj.func) {
        .column_ref => {
            const col = proj.column orelse return Value{ .null_val = {} };
            return row.get(col) orelse Value{ .null_val = {} };
        },
        .int_literal => return Value{ .i64 = proj.int_offset },
        .count_star  => return Value{ .i64 = 1 }, // counted by AggState
        .count_distinct, .sum, .avg, .min, .max => {
            const col = proj.column orelse return Value{ .null_val = {} };
            return row.get(col) orelse Value{ .null_val = {} };
        },
    }
}

// ── Helper: write expression header label ────────────────────────────────────

fn writeExprHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, proj: generic_sql.Expr) !void {
    if (proj.alias) |a| {
        try out.appendSlice(allocator, a);
        return;
    }
    switch (proj.func) {
        .column_ref => try out.appendSlice(allocator, proj.column orelse "*"),
        .int_literal => try out.print(allocator, "{d}", .{proj.int_offset}),
        .count_star => try out.appendSlice(allocator, "count_star()"),
        .count_distinct => try out.print(allocator, "count(DISTINCT {s})", .{proj.column orelse ""}),
        .sum => if (proj.int_offset == 0)
            try out.print(allocator, "sum({s})", .{proj.column orelse ""})
        else
            try out.print(allocator, "sum(({s} + {d}))", .{ proj.column orelse "", proj.int_offset }),
        .avg => try out.print(allocator, "avg({s})", .{proj.column orelse ""}),
        .min => try out.print(allocator, "min({s})", .{proj.column orelse ""}),
        .max => try out.print(allocator, "max({s})", .{proj.column orelse ""}),
    }
}

// ── Helper: write group-by header ────────────────────────────────────────────

fn writeGroupHeader(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    group_cols: []const []const u8,
) !void {
    _ = group_cols;
    for (plan.projections, 0..) |proj, i| {
        if (i != 0) try out.append(allocator, ',');
        try writeExprHeader(out, allocator, proj);
    }
    try out.append(allocator, '\n');
}

// ── Helper: detect all-aggregate plan ────────────────────────────────────────

fn allAggregates(projections: []const generic_sql.Expr) bool {
    for (projections) |p| {
        switch (p.func) {
            .count_star, .count_distinct, .sum, .avg, .min, .max => {},
            .column_ref, .int_literal => return false,
        }
    }
    return projections.len > 0;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "allAggregates" {
    const std2 = std;
    _ = std2;
    const plan_agg = [_]generic_sql.Expr{
        .{ .func = .count_star },
        .{ .func = .sum, .column = "x" },
    };
    try std.testing.expect(allAggregates(&plan_agg));

    const plan_mixed = [_]generic_sql.Expr{
        .{ .func = .column_ref, .column = "x" },
        .{ .func = .count_star },
    };
    try std.testing.expect(!allAggregates(&plan_mixed));
}

test "parseGroupCols" {
    const allocator = std.testing.allocator;
    const cols = try parseGroupCols(allocator, "RegionID, CounterID");
    defer {
        for (cols) |c| allocator.free(c);
        allocator.free(cols);
    }
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("RegionID", cols[0]);
    try std.testing.expectEqualStrings("CounterID", cols[1]);
}

test "value order" {
    try std.testing.expect(Value.order(.{ .i64 = 1 }, .{ .i64 = 2 }) == .lt);
    try std.testing.expect(Value.order(.{ .i64 = 2 }, .{ .i64 = 1 }) == .gt);
    try std.testing.expect(Value.order(.{ .i64 = 1 }, .{ .i64 = 1 }) == .eq);
    try std.testing.expect(Value.order(.{ .str = "a" }, .{ .str = "b" }) == .lt);
}
