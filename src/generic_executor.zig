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
const build_options = @import("build_options");

// ── Public entry point ────────────────────────────────────────────────────────

/// Execute `plan` against `parquet_path` and return the result as a
/// CSV-formatted string (header row + data rows, comma-separated).
/// `table` is used to resolve column names to Parquet column indices and types.
/// Returns `error.UnsupportedGenericQuery` when the plan shape is not handled.
pub fn run(
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    parquet_path: []const u8,
    table: *const schema.Table,
) anyerror![]u8 {
    const exec = Executor{
        .allocator = allocator,
        .io = io,
        .plan = plan,
        .parquet_path = parquet_path,
        .table = table,
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

fn lookupColumn(tbl: *const schema.Table, name: []const u8) ?ColDesc {
    // Handle computed / derived column names used in plan projections
    // e.g. "extract(minute from EventTime)" handled elsewhere; skip
    const idx = tbl.findColumn(name) orelse return null;
    const col = tbl.columns[idx];
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

/// Evaluate a `WhereNode` predicate tree against a row.
/// Returns true if the row passes the predicate.
fn evalWhereNode(node: *const generic_sql.WhereNode, row: *const RowCtx) bool {
    switch (node.*) {
        .cmp_int => |c| {
            const v = row.get(c.col) orelse return false;
            const iv = v.toI64() orelse return false;
            return switch (c.op) {
                .eq => iv == c.val,
                .ne => iv != c.val,
                .lt => iv <  c.val,
                .le => iv <= c.val,
                .gt => iv >  c.val,
                .ge => iv >= c.val,
            };
        },
        .cmp_str => |c| {
            const v = row.get(c.col) orelse return false;
            const sv = v.toStr() orelse {
                // numeric column compared to string literal (e.g. date as epoch vs '2013-07-01')
                // Try parsing the string as a date-epoch integer
                if (c.op == .eq or c.op == .ne or c.op == .lt or c.op == .le or c.op == .gt or c.op == .ge) {
                    if (parseDateStr(c.val)) |epoch| {
                        const iv = v.toI64() orelse return false;
                        return switch (c.op) {
                            .eq => iv == epoch,
                            .ne => iv != epoch,
                            .lt => iv <  epoch,
                            .le => iv <= epoch,
                            .gt => iv >  epoch,
                            .ge => iv >= epoch,
                        };
                    }
                }
                return false;
            };
            return switch (c.op) {
                .eq => std.mem.eql(u8, sv, c.val),
                .ne => !std.mem.eql(u8, sv, c.val),
                .lt => std.mem.order(u8, sv, c.val) == .lt,
                .le => std.mem.order(u8, sv, c.val) != .gt,
                .gt => std.mem.order(u8, sv, c.val) == .gt,
                .ge => std.mem.order(u8, sv, c.val) != .lt,
            };
        },
        .like => |l| {
            const v = row.get(l.col) orelse return false;
            const sv = v.toStr() orelse return false;
            const matched = likeMatch(sv, l.pattern, l.op == .ilike);
            return switch (l.op) {
                .like, .ilike => matched,
                .not_like => !matched,
            };
        },
        .is_null => |col| {
            const v = row.get(col) orelse return true; // missing → null
            return v == .null_val;
        },
        .is_not_null => |col| {
            const v = row.get(col) orelse return false;
            return v != .null_val;
        },
        .and_ => |children| {
            for (children) |ch| if (!evalWhereNode(ch, row)) return false;
            return true;
        },
        .or_ => |children| {
            for (children) |ch| if (evalWhereNode(ch, row)) return true;
            return false;
        },
    }
}

/// SQL LIKE pattern matching: '%' matches any sequence, '_' matches one char.
/// case_insensitive=true for ILIKE.
fn likeMatch(str: []const u8, pattern: []const u8, case_insensitive: bool) bool {
    if (pattern.len == 0) return str.len == 0;
    if (pattern[0] == '%') {
        // Try matching the rest of the pattern at each position in str
        if (likeMatch(str, pattern[1..], case_insensitive)) return true;
        if (str.len == 0) return false;
        return likeMatch(str[1..], pattern, case_insensitive);
    }
    if (str.len == 0) return false;
    if (pattern[0] == '_') {
        return likeMatch(str[1..], pattern[1..], case_insensitive);
    }
    const match = if (case_insensitive)
        std.ascii.toLower(str[0]) == std.ascii.toLower(pattern[0])
    else
        str[0] == pattern[0];
    if (!match) return false;
    return likeMatch(str[1..], pattern[1..], case_insensitive);
}

/// Parse a 'YYYY-MM-DD' date string into days-since-epoch (DuckDB DATE epoch).
/// Returns null if the string is not a recognisable date.
fn parseDateStr(s: []const u8) ?i64 {
    // Accept 'YYYY-MM-DD' (10 chars)
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const m = std.fmt.parseInt(u8,  s[5..7], 10) catch return null;
    const d = std.fmt.parseInt(u8,  s[8..10], 10) catch return null;
    // Days since 1970-01-01 using a simple Gregorian calendar formula
    return dateToDays(y, m, d);
}

fn dateToDays(year: i32, month: u8, day: u8) i64 {
    // Zeller-style: count days from 1970-01-01
    var y: i64 = year;
    var m: i64 = month;
    if (m <= 2) { y -= 1; m += 12; }
    const a = @divFloor(y, 100);
    const b = 2 - a + @divFloor(a, 4);
    const jdn = @as(i64, @intFromFloat(@floor(365.25 * @as(f64, @floatFromInt(y + 4716))))) +
                @as(i64, @intFromFloat(@floor(30.6001 * @as(f64, @floatFromInt(m + 1))))) +
                @as(i64, day) + b - 1524;
    // Julian Day Number for 1970-01-01 is 2440588
    return jdn - 2440588;
}

/// Evaluate `Filter` against a row represented as a name→Value lookup.
/// Used as fallback when where_expr is not available.
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

/// Evaluate the plan's WHERE predicate against a row.
/// Prefers where_expr (full Expr tree) over the legacy filter struct.
fn evalPlanFilter(plan: generic_sql.Plan, row: *const RowCtx) bool {
    if (plan.where_expr) |we| return evalWhereNode(we, row);
    if (plan.filter) |f| return evalFilter(f, row);
    return true; // no filter: row passes
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
    table: *const schema.Table,

    // ── Scalar aggregate ─────────────────────────────────────────────────────

    fn runScalarAgg(self: Executor) anyerror![]u8 {
        // Collect column names needed
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

        var ctx = ScalarAggCtx.init(self.allocator, self.plan);
        defer ctx.deinit();
        try self.streamRows(&needed, &ctx, ScalarAggCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Group-by aggregate ────────────────────────────────────────────────────

    fn runGroupBy(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

        var ctx = GroupByCtx.init(self.allocator, self.plan);
        defer ctx.deinit(self.allocator);
        try self.streamRows(&needed, &ctx, GroupByCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Scan / filtered projection ────────────────────────────────────────────

    fn runScan(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

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
            const desc = lookupColumn(self.table, name) orelse continue;
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
        if (!evalPlanFilter(self.plan, row)) return;
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

/// Group key expression: a base column plus an integer offset.
/// For "ClientIP - 1", base = "ClientIP", offset = -1.
/// For plain "ClientIP", offset = 0.
const GroupKeyExpr = struct {
    base_col: []const u8,  // base column name (heap-allocated)
    offset: i64 = 0,
};

const GroupByCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    map: std.StringHashMap(usize), // key → index into entries
    entries: std.ArrayList(GroupEntry),
    group_cols: []const []const u8, // parsed group-by base column names (for backwards compat)
    group_exprs: []const GroupKeyExpr, // full group key expressions with offsets

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) GroupByCtx {
        const exprs = parseGroupExprs(allocator, plan.group_by orelse "") catch &.{};
        // Derive simple col names from exprs for deinit / header
        const cols: [][]const u8 = allocator.alloc([]const u8, exprs.len) catch
            (allocator.dupe([]const u8, &.{}) catch &.{});
        for (exprs, 0..) |e, i| {
            cols[i] = allocator.dupe(u8, e.base_col) catch e.base_col;
        }        return .{
            .allocator = allocator,
            .plan = plan,
            .map = std.StringHashMap(usize).init(allocator),
            .entries = .empty,
            .group_cols = cols,
            .group_exprs = exprs,
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
        if (self.group_exprs.len > 0) {
            for (self.group_exprs) |e| allocator.free(e.base_col);
            allocator.free(self.group_exprs);
        }
    }

    /// Evaluate a group key expression against a row.
    fn evalGroupKeyExpr(expr: GroupKeyExpr, row: *const RowCtx) Value {
        // EventMinute is a derived column: truncate EventTime timestamp to minutes
        if (std.ascii.eqlIgnoreCase(expr.base_col, "EventMinute")) {
            // EventTime is stored as microseconds since epoch (DuckDB TIMESTAMP)
            // Truncate to minutes: floor(ts / 60_000_000) * 60_000_000
            const ts_v = row.get("EventTime") orelse return Value{ .null_val = {} };
            const ts = ts_v.toI64() orelse return Value{ .null_val = {} };
            const minute_us: i64 = 60 * 1_000_000;
            const truncated = @divFloor(ts, minute_us) * minute_us;
            return Value{ .i64 = truncated + expr.offset };
        }
        const v = row.get(expr.base_col) orelse return Value{ .null_val = {} };
        if (expr.offset == 0) return v;
        const iv = v.toI64() orelse return v;
        return Value{ .i64 = iv + expr.offset };
    }

    fn observe(self: *GroupByCtx, row: *const RowCtx) anyerror!void {
        if (!evalPlanFilter(self.plan, row)) return;

        // Build composite key using evaluated expressions
        var key_buf: std.ArrayList(u8) = .empty;
        defer key_buf.deinit(self.allocator);
        for (self.group_exprs) |expr| {
            const v = evalGroupKeyExpr(expr, row);
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

            // Clone key values (evaluated expressions)
            const key_values = try self.allocator.alloc(Value, self.group_exprs.len);
            for (self.group_exprs, key_values) |expr, *kv| {
                const v = evalGroupKeyExpr(expr, row);
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
        // order_by_alias → sort by that alias (desc by default, asc if order_by_alias_asc),
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
                    // Also match by column name for GROUP BY key projections
                    if (p.column) |c| if (std.ascii.eqlIgnoreCase(c, alias)) break :blk i;
                }
                break :blk null;
            }
            break :blk null;
        };

        // Parse HAVING predicate: "COUNT(*) > N" or "count_star() > N"
        const having: ?HavingPred = blk: {
            const ht = plan.having_text orelse break :blk null;
            break :blk parseHavingPred(plan.projections, ht);
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
            .desc = order_by_count_desc or (order_by_alias != null and !plan.order_by_alias_asc),
        };
        std.sort.block(usize, indices, sort_ctx, SortCtx.lessThan);

        // Apply HAVING filter, then limit / offset
        const offset = plan.offset orelse 0;
        const limit = plan.limit orelse std.math.maxInt(usize);

        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);

        // Header
        try writeGroupHeader(&out, allocator, plan, self.group_cols);

        // Rows: apply HAVING then offset/limit
        var emitted: usize = 0;
        var skipped: usize = 0;
        for (indices) |idx| {
            const entry = entries[idx];

            // HAVING filter
            if (having) |hv| {
                if (hv.proj_idx < entry.states.len) {
                    const v = entry.states[hv.proj_idx].result(plan.projections[hv.proj_idx].func);
                    const iv = v.toI64() orelse 0;
                    const passes = switch (hv.op) {
                        .eq => iv == hv.threshold,
                        .ne => iv != hv.threshold,
                        .lt => iv <  hv.threshold,
                        .le => iv <= hv.threshold,
                        .gt => iv >  hv.threshold,
                        .ge => iv >= hv.threshold,
                    };
                    if (!passes) continue;
                }
            }

            if (skipped < offset) {
                skipped += 1;
                continue;
            }
            if (emitted >= limit) break;
            emitted += 1;

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
                    // Check if it matches a group key expression (by base col + offset)
                    var is_key = false;
                    for (self.group_exprs, entry.key_values) |ge, kv| {
                        if (std.ascii.eqlIgnoreCase(ge.base_col, col_name) and ge.offset == proj.int_offset) {
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
        if (!evalPlanFilter(self.plan, row)) return;

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

fn collectWhereNodeColumns(
    allocator: std.mem.Allocator,
    seen: *std.StringHashMap(void),
    needed: *std.ArrayList([]const u8),
    node: *const generic_sql.WhereNode,
    table: *const schema.Table,
) !void {
    const add = struct {
        fn f(alloc: std.mem.Allocator, s: *std.StringHashMap(void), lst: *std.ArrayList([]const u8), name: []const u8, tbl: *const schema.Table) !void {
            if (s.contains(name)) return;
            if (tbl.findColumn(name) == null) return;
            try s.put(name, {});
            try lst.append(alloc, name);
        }
    }.f;
    switch (node.*) {
        .cmp_int  => |c| try add(allocator, seen, needed, c.col, table),
        .cmp_str  => |c| try add(allocator, seen, needed, c.col, table),
        .like     => |l| try add(allocator, seen, needed, l.col, table),
        .is_null, .is_not_null => |col| try add(allocator, seen, needed, col, table),
        .and_ => |children| for (children) |ch| try collectWhereNodeColumns(allocator, seen, needed, ch, table),
        .or_  => |children| for (children) |ch| try collectWhereNodeColumns(allocator, seen, needed, ch, table),
    }
}

fn collectNeededColumns(
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    needed: *std.ArrayList([]const u8),
    table: *const schema.Table,
) !void {
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const add = struct {
        fn f(alloc: std.mem.Allocator, s: *std.StringHashMap(void), lst: *std.ArrayList([]const u8), name: []const u8, tbl: *const schema.Table) !void {
            if (s.contains(name)) return;
            if (tbl.findColumn(name) == null) return; // skip unknown (computed) columns
            try s.put(name, {});
            try lst.append(alloc, name);
        }
    }.f;

    for (plan.projections) |proj| {
        if (proj.column) |col| {
            // Handle "length(<actual_col>)" — collect the inner column
            if (parseLengthCall(col)) |inner| {
                try add(allocator, &seen, needed, inner, table);
            } else {
                try add(allocator, &seen, needed, col, table);
            }
        }
    }
    if (plan.filter) |filter| {
        try add(allocator, &seen, needed, filter.column, table);
        if (filter.second) |sec| try add(allocator, &seen, needed, sec.column, table);
    }
    // Parse group-by columns using parseGroupExprs (handles arithmetic + date_trunc)
    if (plan.group_by) |gb| {
        const exprs = try parseGroupExprs(allocator, gb);
        defer {
            for (exprs) |e| allocator.free(e.base_col);
            allocator.free(exprs);
        }
        for (exprs) |e| {
            // "EventMinute" maps to the EventTime column in Parquet
            const real_col = if (std.ascii.eqlIgnoreCase(e.base_col, "EventMinute")) "EventTime" else e.base_col;
            try add(allocator, &seen, needed, real_col, table);
        }
    }
    // Also collect columns referenced by where_expr so filter evaluation works.
    if (plan.where_expr) |we| try collectWhereNodeColumns(allocator, &seen, needed, we, table);
    // Ensure at least one column is present so streamRows can count rows.
    if (needed.items.len == 0) {
        // Fall back to first column in schema if "CounterID" is not available.
        const fallback = if (table.findColumn("CounterID") != null) "CounterID" else table.columns[0].name;
        try add(allocator, &seen, needed, fallback, table);
    }
}

// ── Helper: parse HAVING predicate ───────────────────────────────────────────
//
// Supports: "COUNT(*) > N", "count_star() > N", "c > N" (alias), etc.
// Returns null if the HAVING text cannot be parsed.

const HavingPred = struct { proj_idx: usize, op: generic_sql.CmpOp, threshold: i64 };

fn parseHavingPred(projections: []const generic_sql.Expr, having_text: []const u8) ?HavingPred {
    const ops = [_]struct { text: []const u8, op: generic_sql.CmpOp }{
        .{ .text = ">=", .op = .ge },
        .{ .text = "<=", .op = .le },
        .{ .text = "<>", .op = .ne },
        .{ .text = ">",  .op = .gt },
        .{ .text = "<",  .op = .lt },
        .{ .text = "=",  .op = .eq },
    };
    for (ops) |candidate| {
        const pos = std.mem.indexOf(u8, having_text, candidate.text) orelse continue;
        const lhs = std.mem.trim(u8, having_text[0..pos], " \t\r\n");
        const rhs = std.mem.trim(u8, having_text[pos + candidate.text.len ..], " \t\r\n");
        const threshold = std.fmt.parseInt(i64, rhs, 10) catch continue;
        // Match lhs to a projection: count_star name variants, alias, or column
        for (projections, 0..) |p, i| {
            if (std.ascii.eqlIgnoreCase(lhs, "COUNT(*)") or
                std.ascii.eqlIgnoreCase(lhs, "count_star()"))
            {
                if (p.func == .count_star) return .{ .proj_idx = i, .op = candidate.op, .threshold = threshold };
            }
            if (p.alias) |a| if (std.ascii.eqlIgnoreCase(lhs, a)) return .{ .proj_idx = i, .op = candidate.op, .threshold = threshold };
        }
    }
    return null;
}

// ── Helper: parse group-by column list ───────────────────────────────────────

fn parseGroupCols(allocator: std.mem.Allocator, group_by: []const u8) ![][]const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |c| allocator.free(c);
        result.deinit(allocator);
    }
    // Split on commas at depth 0 (ignore commas inside parentheses)
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= group_by.len) : (i += 1) {
        const c = if (i < group_by.len) group_by[i] else ',';
        switch (c) {
            '(' => depth += 1,
            ')' => { if (depth > 0) depth -= 1; },
            ',' => if (depth == 0) {
                const part = std.mem.trim(u8, group_by[start..i], " \t\r\n");
                start = i + 1;
                if (part.len == 0) continue;
                // Skip numeric position references like "1", "2"
                if (std.fmt.parseInt(usize, part, 10) catch null) |_| continue;
                // Map date_trunc('minute', EventTime) → EventMinute
                if (isDateTruncMinutePart(part)) {
                    try result.append(allocator, try allocator.dupe(u8, "EventMinute"));
                    continue;
                }
                // Handle arithmetic expressions like "ClientIP - 1": use the base column name
                const base = extractBaseColumnName(part);
                try result.append(allocator, try allocator.dupe(u8, base));
            },
            else => {},
        }
    }
    return result.toOwnedSlice(allocator);
}

/// Returns true if `part` is a date_trunc('minute', EventTime) expression.
fn isDateTruncMinutePart(part: []const u8) bool {
    const lower_len = part.len;
    if (lower_len < 7) return false;
    if (!std.ascii.startsWithIgnoreCase(part, "date_trunc")) return false;
    const open = std.mem.indexOfScalar(u8, part, '(') orelse return false;
    const close = std.mem.lastIndexOfScalar(u8, part, ')') orelse return false;
    if (close <= open) return false;
    const inner = std.mem.trim(u8, part[open + 1 .. close], " \t\r\n");
    const comma = std.mem.indexOfScalar(u8, inner, ',') orelse return false;
    const unit = std.mem.trim(u8, inner[0..comma], " \t\r\n");
    const source = std.mem.trim(u8, inner[comma + 1 ..], " \t\r\n");
    return std.mem.eql(u8, unit, "'minute'") and std.ascii.eqlIgnoreCase(source, "EventTime");
}

/// Extract the base column name from an expression like "ClientIP - 1" → "ClientIP".
/// If no operator is found, returns the full expression (assumed to be a plain identifier).
fn extractBaseColumnName(expr: []const u8) []const u8 {
    // Look for arithmetic operators: only minus for now (covers q36)
    for ([_]u8{ '-', '+' }) |op| {
        if (std.mem.indexOfScalar(u8, expr, op)) |pos| {
            const base = std.mem.trim(u8, expr[0..pos], " \t\r\n");
            if (base.len > 0) return base;
        }
    }
    return expr;
}

/// Parse a group-by expression string like "ClientIP - 1" into a GroupKeyExpr.
fn parseGroupKeyExpr(allocator: std.mem.Allocator, part: []const u8) !GroupKeyExpr {
    // date_trunc → EventMinute with offset 0
    if (isDateTruncMinutePart(part)) {
        return .{ .base_col = try allocator.dupe(u8, "EventMinute"), .offset = 0 };
    }
    // Look for subtraction
    if (std.mem.indexOfScalar(u8, part, '-')) |pos| {
        const base = std.mem.trim(u8, part[0..pos], " \t\r\n");
        const rest = std.mem.trim(u8, part[pos + 1 ..], " \t\r\n");
        if (base.len > 0) {
            if (std.fmt.parseInt(i64, rest, 10) catch null) |off| {
                return .{ .base_col = try allocator.dupe(u8, base), .offset = -off };
            }
        }
    }
    // Look for addition
    if (std.mem.indexOfScalar(u8, part, '+')) |pos| {
        const base = std.mem.trim(u8, part[0..pos], " \t\r\n");
        const rest = std.mem.trim(u8, part[pos + 1 ..], " \t\r\n");
        if (base.len > 0) {
            if (std.fmt.parseInt(i64, rest, 10) catch null) |off| {
                return .{ .base_col = try allocator.dupe(u8, base), .offset = off };
            }
        }
    }
    return .{ .base_col = try allocator.dupe(u8, part), .offset = 0 };
}

/// Parse group-by string into GroupKeyExpr slice (handles depth-0 comma splitting).
fn parseGroupExprs(allocator: std.mem.Allocator, group_by: []const u8) ![]GroupKeyExpr {
    var result: std.ArrayList(GroupKeyExpr) = .empty;
    errdefer {
        for (result.items) |e| allocator.free(e.base_col);
        result.deinit(allocator);
    }
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= group_by.len) : (i += 1) {
        const c = if (i < group_by.len) group_by[i] else ',';
        switch (c) {
            '(' => depth += 1,
            ')' => { if (depth > 0) depth -= 1; },
            ',' => if (depth == 0) {
                const part = std.mem.trim(u8, group_by[start..i], " \t\r\n");
                start = i + 1;
                if (part.len == 0) continue;
                // Skip numeric position references
                if (std.fmt.parseInt(usize, part, 10) catch null) |_| continue;
                try result.append(allocator, try parseGroupKeyExpr(allocator, part));
            },
            else => {},
        }
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
            // Handle "length(<actual_col>)" — compute string length instead of column value
            if (parseLengthCall(col)) |inner_col| {
                const v = row.get(inner_col) orelse return Value{ .null_val = {} };
                return switch (v) {
                    .str => |s| Value{ .i64 = @intCast(s.len) },
                    else => Value{ .null_val = {} },
                };
            }
            return row.get(col) orelse Value{ .null_val = {} };
        },
    }
}

/// If expr is of the form "length(<col>)" (case-insensitive), returns the inner col name.
fn parseLengthCall(expr: []const u8) ?[]const u8 {
    const prefix = "length(";
    if (!std.ascii.startsWithIgnoreCase(expr, prefix)) return null;
    if (expr[expr.len - 1] != ')') return null;
    return expr[prefix.len .. expr.len - 1];
}

// ── Helper: write expression header label ────────────────────────────────────

fn writeExprHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, proj: generic_sql.Expr) !void {
    if (proj.alias) |a| {
        try out.appendSlice(allocator, a);
        return;
    }
    switch (proj.func) {
        .column_ref => {
            const col = proj.column orelse "*";
            if (proj.int_offset == 0) {
                try out.appendSlice(allocator, col);
            } else if (proj.int_offset > 0) {
                try out.print(allocator, "{s} + {d}", .{ col, proj.int_offset });
            } else {
                try out.print(allocator, "{s} - {d}", .{ col, -proj.int_offset });
            }
        },
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

// Smoke tests against data/fixture_hits.parquet (1 row):
//   WatchID=..., CounterID=62, Age=30, EventDate=15887, ...
// Run with: zig build test

const fixture_parquet = build_options.fixture_parquet_path;

fn runQuery(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    const plan = (try generic_sql.parse(allocator, sql)) orelse
        return error.ParseFailed;
    defer generic_sql.deinit(allocator, plan);
    return run(allocator, std.testing.io, plan, fixture_parquet, &clickbench_schema.hits);
}

test "smoke: count(*)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT count(*) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("count_star()\n1\n", out);
}

test "smoke: sum(Age)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT sum(Age) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("sum(Age)\n30\n", out);
}

test "smoke: avg(Age)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT avg(Age) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("avg(Age)\n30\n", out);
}

test "smoke: min and max EventDate" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT min(EventDate), max(EventDate) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("min(EventDate),max(EventDate)\n15887,15887\n", out);
}

test "smoke: count(DISTINCT CounterID)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT count(DISTINCT CounterID) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("count(DISTINCT CounterID)\n1\n", out);
}

test "smoke: GROUP BY CounterID" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID, count(*) AS c FROM hits GROUP BY CounterID ORDER BY c DESC LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID,c\n62,1\n", out);
}

test "smoke: WHERE Age > 0 scan" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE no match returns header only" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 999 LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}

test "smoke: sum with WHERE filter" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT sum(Age) FROM hits WHERE Age > 0");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("sum(Age)\n30\n", out);
}

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

test "likeMatch: basic percent wildcard" {
    try std.testing.expect(likeMatch("hello world", "hello%", false));
    try std.testing.expect(likeMatch("hello world", "%world", false));
    try std.testing.expect(likeMatch("hello world", "%lo wo%", false));
    try std.testing.expect(!likeMatch("hello world", "hello", false));
}

test "likeMatch: underscore wildcard" {
    try std.testing.expect(likeMatch("abc", "a_c", false));
    try std.testing.expect(!likeMatch("ac", "a_c", false));
    try std.testing.expect(likeMatch("aXc", "a_c", false));
}

test "likeMatch: case insensitive ILIKE" {
    try std.testing.expect(likeMatch("Hello World", "hello%", true));
    try std.testing.expect(likeMatch("GOOGLE", "%oog%", true));
    try std.testing.expect(!likeMatch("GOOGLE", "%oog%", false));
}

test "likeMatch: empty pattern and string" {
    try std.testing.expect(likeMatch("", "%", false));
    try std.testing.expect(likeMatch("", "", false));
    try std.testing.expect(!likeMatch("a", "", false));
}

test "smoke: WHERE AND multi-condition match" {
    // Age=30 AND CounterID=62 — fixture row should match
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 AND CounterID = 62");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE AND multi-condition no match" {
    // Age=30 AND CounterID=999 — no row matches
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 AND CounterID = 999");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}

test "smoke: WHERE date string comparison" {
    // EventDate=15887 corresponds to 2013-07-03; row should pass >= '2013-07-01'
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE EventDate >= '2013-07-01' AND EventDate < '2013-07-10'");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE date string no match" {
    // EventDate=15887 should not pass > '2013-12-31'
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE EventDate > '2013-12-31'");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}
