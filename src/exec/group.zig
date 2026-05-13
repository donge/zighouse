const std = @import("std");
const agg = @import("../agg.zig");
const generic_sql = @import("../generic_sql.zig");
const hashmap = @import("../hashmap.zig");
const native_reduce = @import("reduce.zig");
const parallel = @import("../parallel.zig");

pub const Column = union(enum) {
    i16: []const i16,
    i32: []const i32,
    i64: []const i64,
    empty_text_id: struct {
        ids: []const u32,
        empty_id: u32,
    },
};

pub const BoundColumn = struct {
    name: []const u8,
    column: Column,
};

pub const Context = struct {
    ptr: *const anyopaque,
    bind_column: *const fn (*const anyopaque, []const u8) anyerror!BoundColumn,
    bind_filter_column: *const fn (*const anyopaque, []const u8) anyerror!BoundColumn,

    fn column(self: Context, name: []const u8) !BoundColumn {
        return self.bind_column(self.ptr, name);
    }

    fn filterColumn(self: Context, name: []const u8) !BoundColumn {
        return self.bind_filter_column(self.ptr, name);
    }
};

pub fn execute(allocator: std.mem.Allocator, plan: generic_sql.Plan, ctx: Context) !?[]u8 {
    if (matchDenseCountGroup(plan, ctx)) |shape| return try formatDenseCountGroup(allocator, shape);
    if (matchDenseAvgCountTop(plan, ctx)) |shape| return try formatDenseAvgCountTop(allocator, shape);
    if (matchOffsetCountTop(plan, ctx)) |shape| return try formatOffsetCountTop(allocator, shape);
    if (matchTupleAggTop(plan, ctx)) |shape| return try formatTupleAggTop(allocator, shape);
    return null;
}

pub fn formatDenseCountI16(allocator: std.mem.Allocator, key_label: []const u8, values: []const i16, count_label: []const u8, skip_zero: bool) ![]u8 {
    return formatDenseCountGroup(allocator, .{ .key_label = key_label, .values = values, .filter = if (skip_zero) .same_key_non_zero else .none, .count_label = count_label });
}

fn formatDenseCountGroup(allocator: std.mem.Allocator, shape: DenseCountGroupShape) ![]u8 {
    const min_key = std.math.minInt(i16);
    const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (shape.values) |value| {
        if (shape.filter == .same_key_non_zero and value == 0) continue;
        const index: usize = @intCast(@as(i32, value) - @as(i32, min_key));
        counts[index] += 1;
    }

    var rows: std.ArrayList(CountRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count == 0) continue;
        const key: i16 = @intCast(@as(i32, @intCast(index)) + @as(i32, min_key));
        try rows.append(allocator, .{ .key = key, .count = count });
    }
    std.mem.sort(CountRow, rows.items, {}, countRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s}\n", .{ shape.key_label, shape.count_label });
    for (rows.items) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatDenseAvgCountTopI32(allocator: std.mem.Allocator, key_label: []const u8, keys: []const i32, measures: []const i32, filter_values: ?[]const i32, having_count_gt: u64, limit: usize, avg_label: []const u8, count_label: []const u8) ![]u8 {
    return formatDenseAvgCountTop(allocator, .{ .key_label = key_label, .keys = keys, .measures = measures, .filter_values = filter_values, .having_count_gt = having_count_gt, .limit = limit, .avg_label = avg_label, .count_label = count_label });
}

fn formatDenseAvgCountTop(allocator: std.mem.Allocator, shape: DenseAvgCountTopShape) ![]u8 {
    const min_counter: i32 = 0;
    const max_counter: i32 = 262143;
    const bucket_count: usize = @intCast(max_counter - min_counter + 1);
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    const sums = try allocator.alloc(u64, bucket_count);
    defer allocator.free(sums);
    @memset(counts, 0);
    @memset(sums, 0);

    if (shape.keys.len != shape.measures.len) return error.CorruptHotColumns;
    if (shape.filter_values) |filter_values| if (filter_values.len != shape.keys.len) return error.CorruptHotColumns;
    for (shape.keys, shape.measures, 0..) |counter_id, measure, i| {
        if (shape.filter_values) |filter_values| if (filter_values[i] == 0) continue;
        if (counter_id < min_counter or counter_id > max_counter) continue;
        const index: usize = @intCast(counter_id - min_counter);
        counts[index] += 1;
        sums[index] += @intCast(measure);
    }

    var rows: std.ArrayList(AvgCountRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count <= shape.having_count_gt) continue;
        const counter_id: i32 = @intCast(@as(i64, min_counter) + @as(i64, @intCast(index)));
        try rows.append(allocator, .{ .key = counter_id, .sum = sums[index], .count = count });
    }
    std.mem.sort(AvgCountRow, rows.items, {}, avgCountRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s},{s}\n", .{ shape.key_label, shape.avg_label, shape.count_label });
    const limit = @min(rows.items.len, shape.limit);
    for (rows.items[0..limit]) |row| {
        const avg = @as(f64, @floatFromInt(row.sum)) / @as(f64, @floatFromInt(row.count));
        try out.print(allocator, "{d},{d},{d}\n", .{ row.key, avg, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatOffsetCountTopI32(allocator: std.mem.Allocator, base_label: []const u8, values: []const i32, offsets: [4]i64, limit: usize, count_label: []const u8) ![]u8 {
    return formatOffsetCountTop(allocator, .{ .base_label = base_label, .values = values, .offsets = offsets, .limit = limit, .count_label = count_label });
}

fn formatOffsetCountTop(allocator: std.mem.Allocator, shape: OffsetCountTopShape) ![]u8 {
    // Parallel partitioned hash agg. Each worker owns a private
    // PartitionedHashU64Count; pass1 accumulates via morsels. Pass2
    // merges each of the 64 partitions independently in parallel,
    // keeping a per-merge-worker top-10. Final top-10 is the merge
    // of those per-worker heaps.
    const n_threads = parallel.defaultThreads();
    // Size per-thread tables for high-cardinality 32-bit keys.
    // get expected/n_threads keys.
    const expected_total: usize = 9_500_000;
    const expected_per_thread = expected_total / n_threads + 1;

    const tables = try allocator.alloc(hashmap.PartitionedHashU64Count, n_threads);
    var inited: usize = 0;
    defer {
        for (tables[0..inited]) |*t| t.deinit();
        allocator.free(tables);
    }
    for (tables) |*t| {
        t.* = try hashmap.PartitionedHashU64Count.init(allocator, expected_per_thread);
        inited += 1;
    }

    const Pass1Ctx = struct {
        values: []const i32,
        table: *hashmap.PartitionedHashU64Count,
    };
    const pass1_workers = struct {
        fn fill(ctx: *Pass1Ctx, source: *parallel.MorselSource) void {
            while (source.next()) |m| {
                var r = m.start;
                while (r < m.end) : (r += 1) {
                    ctx.table.bump(@as(u64, @bitCast(@as(i64, ctx.values[r]))));
                }
            }
        }
    };
    const pass1_ctxs = try allocator.alloc(Pass1Ctx, n_threads);
    defer allocator.free(pass1_ctxs);
    for (pass1_ctxs, 0..) |*c, t| c.* = .{ .values = shape.values, .table = &tables[t] };
    var src: parallel.MorselSource = .init(shape.values.len, parallel.default_morsel_size);
    try parallel.parallelFor(allocator, Pass1Ctx, pass1_workers.fill, pass1_ctxs, &src);

    // Build pointer array for mergePartition.
    const local_ptrs = try allocator.alloc(*hashmap.PartitionedHashU64Count, n_threads);
    defer allocator.free(local_ptrs);
    for (tables, 0..) |*t, i| local_ptrs[i] = t;

    // Per-merge-worker top-10. We use one worker per partition rebalanced
    // across n_threads physical workers. Since partition_count = 64,
    // each worker handles ~64/n_threads partitions.
    const n_workers = @min(n_threads, hashmap.partition_count);
    const worker_tops = try allocator.alloc([10]ClientIpCount, n_workers);
    defer allocator.free(worker_tops);
    const worker_top_lens = try allocator.alloc(usize, n_workers);
    defer allocator.free(worker_top_lens);
    @memset(worker_top_lens, 0);

    const MergeCtx = struct {
        allocator: std.mem.Allocator,
        local_ptrs: []*hashmap.PartitionedHashU64Count,
        worker_tops: [][10]ClientIpCount,
        worker_top_lens: []usize,
        n_workers: usize,
    };
    const merge_workers = struct {
        fn run(ctx: *MergeCtx, worker_id: usize) void {
            var p = worker_id;
            while (p < hashmap.partition_count) : (p += ctx.n_workers) {
                // Estimate output size = sum of per-thread part lens.
                var expected_p: usize = 0;
                for (ctx.local_ptrs) |lp| expected_p += lp.parts[p].len;
                if (expected_p == 0) continue;
                var merged = hashmap.mergePartition(ctx.allocator, ctx.local_ptrs, p, expected_p) catch return;
                defer merged.deinit();
                var it = merged.iterator();
                while (it.next()) |e| {
                    insertClientIpTop10(
                        &ctx.worker_tops[worker_id],
                        &ctx.worker_top_lens[worker_id],
                        .{ .client_ip = @as(i32, @truncate(@as(i64, @bitCast(e.key)))), .count = e.value },
                    );
                }
            }
        }
    };
    var merge_ctx: MergeCtx = .{
        .allocator = allocator,
        .local_ptrs = local_ptrs,
        .worker_tops = worker_tops,
        .worker_top_lens = worker_top_lens,
        .n_workers = n_workers,
    };
    try parallel.parallelIndices(allocator, MergeCtx, merge_workers.run, &merge_ctx, n_workers);

    // Final reduce: merge per-worker top-10 lists into a global top-10.
    var top: [10]ClientIpCount = undefined;
    var top_len: usize = 0;
    for (worker_tops, 0..) |*wt, w| {
        for (wt[0..worker_top_lens[w]]) |row| insertClientIpTop10(&top, &top_len, row);
    }

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s}", .{shape.base_label});
    for (shape.offsets[1..]) |offset| try out.print(allocator, ",subtract({s}, {d})", .{ shape.base_label, -offset });
    try out.print(allocator, ",{s}\n", .{shape.count_label});
    for (top[0..@min(top_len, shape.limit)]) |row| {
        try out.print(allocator, "{d}", .{row.client_ip});
        for (shape.offsets[1..]) |offset| try out.print(allocator, ",{d}", .{row.client_ip + @as(i32, @intCast(offset))});
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatTupleAggTopBound(allocator: std.mem.Allocator, key1_label: []const u8, key1: []const i64, key2_label: []const u8, key2: []const i32, filter_ids: []const u32, empty_id: u32, sum_label: []const u8, sum: []const i16, avg_label: []const u8, avg: []const i16, limit: usize, count_label: []const u8) ![]u8 {
    return formatTupleAggTop(allocator, .{ .key1_label = key1_label, .key1 = key1, .key2_label = key2_label, .key2 = key2, .filter_ids = filter_ids, .empty_id = empty_id, .sum_label = sum_label, .sum = sum, .avg_label = avg_label, .avg = avg, .limit = limit, .count_label = count_label });
}

pub const UrlHashCount = struct {
    url_hash: i64,
    count: u32,
};

pub const UrlTopCache = struct {
    rows: [10]UrlHashCount = undefined,
    len: usize = 0,
};

pub fn collectUrlHashTop(allocator: std.mem.Allocator, url_hash: []const i64) !UrlTopCache {
    var counts = try agg.I64CountTable.init(allocator, 1024 * 1024);
    defer counts.deinit(allocator);
    for (url_hash) |hash| try counts.add(allocator, hash);

    var top = UrlTopCache{};
    for (counts.occupied[0..counts.len]) |index| {
        insertUrlHashTop10(&top.rows, &top.len, .{ .url_hash = counts.keys[index], .count = counts.counts[index] });
    }
    return top;
}

fn formatTupleAggTop(allocator: std.mem.Allocator, shape: TupleAggTopShape) ![]u8 {
    if (shape.key1.len != shape.key2.len or shape.key1.len != shape.sum.len or shape.key1.len != shape.avg.len or shape.key1.len != shape.filter_ids.len) return error.CorruptHotColumns;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s},{s},sum({s}),avg({s})\n", .{ shape.key1_label, shape.key2_label, shape.count_label, shape.sum_label, shape.avg_label });

    var emitted: usize = 0;
    var i: usize = 0;
    while (i < shape.key1.len and emitted < shape.limit) : (i += 1) {
        if (shape.filter_ids[i] == shape.empty_id) continue;
        const avg = @as(f64, @floatFromInt(@as(i32, shape.avg[i])));
        try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ shape.key1[i], shape.key2[i], @as(i32, shape.sum[i]), avg });
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

const DenseCountFilter = enum { none, same_key_non_zero };

const DenseCountGroupShape = struct {
    key_label: []const u8,
    values: []const i16,
    filter: DenseCountFilter,
    count_label: []const u8,
};

const DenseAvgCountTopShape = struct {
    key_label: []const u8,
    keys: []const i32,
    measures: []const i32,
    filter_values: ?[]const i32,
    having_count_gt: u64,
    limit: usize,
    avg_label: []const u8,
    count_label: []const u8,
};

const OffsetCountTopShape = struct {
    base_label: []const u8,
    values: []const i32,
    offsets: [4]i64,
    limit: usize,
    count_label: []const u8,
};

const TupleAggTopShape = struct {
    key1_label: []const u8,
    key1: []const i64,
    key2_label: []const u8,
    key2: []const i32,
    filter_ids: []const u32,
    empty_id: u32,
    sum_label: []const u8,
    sum: []const i16,
    avg_label: []const u8,
    avg: []const i16,
    limit: usize,
    count_label: []const u8,
};

fn matchDenseCountGroup(plan: generic_sql.Plan, ctx: Context) ?DenseCountGroupShape {
    if (plan.having_text != null or plan.offset != null or plan.limit != null or !plan.order_by_count_desc) return null;
    const group = plan.group_by orelse return null;
    if (plan.projections.len != 2) return null;
    const key_expr = plan.projections[0];
    if (key_expr.func != .column_ref or key_expr.int_offset != 0) return null;
    if (!asciiEqlIgnoreCase(key_expr.column orelse return null, group)) return null;
    const count_expr = plan.projections[1];
    if (count_expr.func != .count_star) return null;
    const key = bindI16(ctx, group) orelse return null;
    const filter: DenseCountFilter = if (plan.filter) |filter| blk: {
        if (!simpleIntFilter(filter, group, .not_equal, 0)) return null;
        break :blk .same_key_non_zero;
    } else .none;
    return .{ .key_label = key.name, .values = key.values, .filter = filter, .count_label = count_expr.alias orelse "count_star()" };
}

fn matchDenseAvgCountTop(plan: generic_sql.Plan, ctx: Context) ?DenseAvgCountTopShape {
    if (plan.offset != null) return null;
    const group = plan.group_by orelse return null;
    if (plan.projections.len != 3) return null;
    const key_expr = plan.projections[0];
    const avg_expr = plan.projections[1];
    const count_expr = plan.projections[2];
    if (key_expr.func != .column_ref or key_expr.int_offset != 0 or !asciiEqlIgnoreCase(key_expr.column orelse return null, group)) return null;
    if (avg_expr.func != .avg or avg_expr.alias == null) return null;
    if (count_expr.func != .count_star) return null;
    if (!orderByAlias(plan, avg_expr.alias.?)) return null;
    const having_count_gt = parseHavingCountGreaterThan(plan.having_text orelse return null) orelse return null;
    const key = bindI32(ctx, group) orelse return null;
    const measure = bindI32(ctx, avg_expr.column orelse return null) orelse return null;
    const filter_values: ?[]const i32 = if (plan.filter) |filter| blk: {
        if (filter.second != null or filter.op != .not_equal or filter.int_value != 0) return null;
        break :blk (bindFilterI32(ctx, filter.column) orelse return null).values;
    } else null;
    return .{ .key_label = key.name, .keys = key.values, .measures = measure.values, .filter_values = filter_values, .having_count_gt = having_count_gt, .limit = plan.limit orelse return null, .avg_label = avg_expr.alias.?, .count_label = count_expr.alias orelse "count_star()" };
}

fn matchOffsetCountTop(plan: generic_sql.Plan, ctx: Context) ?OffsetCountTopShape {
    if (plan.filter != null or plan.offset != null or plan.projections.len != 5) return null;
    const count_expr = plan.projections[4];
    if (count_expr.func != .count_star or count_expr.alias == null or !orderByAlias(plan, count_expr.alias.?)) return null;
    var offsets: [4]i64 = undefined;
    const base_name = plan.projections[0].column orelse return null;
    for (plan.projections[0..4], 0..) |expr, idx| {
        if (expr.func != .column_ref or !asciiEqlIgnoreCase(expr.column orelse return null, base_name)) return null;
        offsets[idx] = expr.int_offset;
    }
    if (!offsetGroupMatches(plan.group_by orelse return null, base_name, offsets)) return null;
    const base = bindI32(ctx, base_name) orelse return null;
    return .{ .base_label = base.name, .values = base.values, .offsets = offsets, .limit = plan.limit orelse return null, .count_label = count_expr.alias.? };
}

fn matchTupleAggTop(plan: generic_sql.Plan, ctx: Context) ?TupleAggTopShape {
    if (plan.offset != null or plan.projections.len != 5) return null;
    const key1_expr = plan.projections[0];
    const key2_expr = plan.projections[1];
    const count_expr = plan.projections[2];
    const sum_expr = plan.projections[3];
    const avg_expr = plan.projections[4];
    if (key1_expr.func != .column_ref or key1_expr.int_offset != 0) return null;
    if (key2_expr.func != .column_ref or key2_expr.int_offset != 0) return null;
    const key1_name = key1_expr.column orelse return null;
    const key2_name = key2_expr.column orelse return null;
    if (!twoColumnGroupMatches(plan.group_by orelse return null, key1_name, key2_name)) return null;
    if (count_expr.func != .count_star or count_expr.alias == null or !orderByAlias(plan, count_expr.alias.?)) return null;
    if (sum_expr.func != .sum or avg_expr.func != .avg) return null;
    const key1 = bindI64(ctx, key1_name) orelse return null;
    const key2 = bindI32(ctx, key2_name) orelse return null;
    const sum = bindI16(ctx, sum_expr.column orelse return null) orelse return null;
    const avg = bindI16(ctx, avg_expr.column orelse return null) orelse return null;
    const filter = plan.filter orelse return null;
    if (filter.second != null or filter.op != .not_equal or filter.int_value != 0) return null;
    const text_filter = bindEmptyTextId(ctx, filter.column) orelse return null;
    return .{ .key1_label = key1.name, .key1 = key1.values, .key2_label = key2.name, .key2 = key2.values, .filter_ids = text_filter.ids, .empty_id = text_filter.empty_id, .sum_label = sum.name, .sum = sum.values, .avg_label = avg.name, .avg = avg.values, .limit = plan.limit orelse return null, .count_label = count_expr.alias.? };
}

const BoundI16 = struct { name: []const u8, values: []const i16 };
const BoundI32 = struct { name: []const u8, values: []const i32 };
const BoundI64 = struct { name: []const u8, values: []const i64 };
const BoundEmptyTextId = struct { ids: []const u32, empty_id: u32 };

fn bindI16(ctx: Context, name: []const u8) ?BoundI16 {
    const bound = ctx.column(name) catch return null;
    return switch (bound.column) {
        .i16 => |values| .{ .name = bound.name, .values = values },
        else => null,
    };
}

fn bindI32(ctx: Context, name: []const u8) ?BoundI32 {
    const bound = ctx.column(name) catch return null;
    return switch (bound.column) {
        .i32 => |values| .{ .name = bound.name, .values = values },
        else => null,
    };
}

fn bindI64(ctx: Context, name: []const u8) ?BoundI64 {
    const bound = ctx.column(name) catch return null;
    return switch (bound.column) {
        .i64 => |values| .{ .name = bound.name, .values = values },
        else => null,
    };
}

fn bindFilterI32(ctx: Context, name: []const u8) ?BoundI32 {
    const bound = ctx.filterColumn(name) catch return null;
    return switch (bound.column) {
        .i32 => |values| .{ .name = bound.name, .values = values },
        else => null,
    };
}

fn bindEmptyTextId(ctx: Context, name: []const u8) ?BoundEmptyTextId {
    const bound = ctx.filterColumn(name) catch return null;
    return switch (bound.column) {
        .empty_text_id => |text| .{ .ids = text.ids, .empty_id = text.empty_id },
        else => null,
    };
}

fn simpleIntFilter(filter: generic_sql.Filter, column: []const u8, op: generic_sql.FilterOp, int_value: i64) bool {
    return filter.second == null and filter.op == op and filter.int_value == int_value and asciiEqlIgnoreCase(filter.column, column);
}

fn parseHavingCountGreaterThan(text: []const u8) ?u64 {
    const prefix = "COUNT(*) >";
    if (!startsWithIgnoreCase(text, prefix)) return null;
    const raw = std.mem.trim(u8, text[prefix.len..], " \t\r\n");
    if (raw.len == 0) return null;
    return std.fmt.parseInt(u64, raw, 10) catch null;
}

fn offsetGroupMatches(group_by: []const u8, base_name: []const u8, offsets: [4]i64) bool {
    var buf: [128]u8 = undefined;
    const expected = std.fmt.bufPrint(&buf, "{s}, {s} - {d}, {s} - {d}, {s} - {d}", .{ base_name, base_name, -offsets[1], base_name, -offsets[2], base_name, -offsets[3] }) catch return false;
    return offsets[0] == 0 and offsets[1] < 0 and offsets[2] < 0 and offsets[3] < 0 and asciiEqlIgnoreCase(group_by, expected);
}

fn twoColumnGroupMatches(group_by: []const u8, first: []const u8, second: []const u8) bool {
    var buf: [96]u8 = undefined;
    const expected = std.fmt.bufPrint(&buf, "{s}, {s}", .{ first, second }) catch return false;
    return asciiEqlIgnoreCase(group_by, expected);
}

fn startsWithIgnoreCase(value: []const u8, prefix: []const u8) bool {
    if (value.len < prefix.len) return false;
    return asciiEqlIgnoreCase(value[0..prefix.len], prefix);
}

fn hasEmptyStringFilter(plan: generic_sql.Plan, column: []const u8) bool {
    const filter = plan.filter orelse return false;
    return filter.second == null and filter.op == .not_equal and filter.int_value == 0 and asciiEqlIgnoreCase(filter.column, column);
}

fn orderByAlias(plan: generic_sql.Plan, alias: []const u8) bool {
    return if (plan.order_by_alias) |got| asciiEqlIgnoreCase(got, alias) else false;
}

const CountRow = struct {
    key: i16,
    count: u64,
};

fn countRowDesc(_: void, a: CountRow, b: CountRow) bool {
    if (a.count == b.count) return a.key < b.key;
    return a.count > b.count;
}

const AvgCountRow = struct {
    key: i32,
    sum: u64,
    count: u64,
};

const ClientIpCount = struct {
    client_ip: i32,
    count: u32,
};

fn insertClientIpTop10(top: *[10]ClientIpCount, top_len: *usize, row: ClientIpCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and clientIpBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn clientIpBefore(a: ClientIpCount, b: ClientIpCount) bool {
    if (a.count == b.count) return a.client_ip < b.client_ip;
    return a.count > b.count;
}

fn insertUrlHashTop10(top: *[10]UrlHashCount, top_len: *usize, row: UrlHashCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and urlHashBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn urlHashBefore(a: UrlHashCount, b: UrlHashCount) bool {
    if (a.count == b.count) return a.url_hash < b.url_hash;
    return a.count > b.count;
}

fn avgCountRowDesc(_: void, a: AvgCountRow, b: AvgCountRow) bool {
    const left = @as(u128, a.sum) * @as(u128, b.count);
    const right = @as(u128, b.sum) * @as(u128, a.count);
    if (left != right) return left > right;
    return a.key < b.key;
}

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ac, bc| if (std.ascii.toLower(ac) != std.ascii.toLower(bc)) return false;
    return true;
}
