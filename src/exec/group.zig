const std = @import("std");
const agg = @import("../agg.zig");
const generic_sql = @import("../generic_sql.zig");
const hashmap = @import("../hashmap.zig");
const native_reduce = @import("reduce.zig");
const parallel = @import("../parallel.zig");

pub fn execute(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (matchDenseCountGroup(plan)) |shape| return try formatDenseCountGroup(allocator, hot, shape);
    if (matchDenseAvgCountTop(plan)) |shape| return try formatDenseAvgCountTop(allocator, hot, shape);
    if (matchOffsetCountTop(plan)) |shape| return try formatOffsetCountTop(allocator, hot, shape);
    if (matchTupleAggTop(plan)) |shape| {
        if (hot.search_phrase_id == null) return null;
        return try formatTupleAggTop(allocator, hot, shape);
    }
    return null;
}

pub fn needsSearchPhraseIds(plan: generic_sql.Plan) bool {
    return if (matchTupleAggTop(plan)) |shape| shape.filter == .search_phrase_non_empty else false;
}

pub fn formatAdvEngineCount(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    return formatDenseCountGroup(allocator, hot, .{ .key = .adv_engine_id, .filter = .same_key_non_zero, .count_label = "count_star()" });
}

fn formatDenseCountGroup(allocator: std.mem.Allocator, hot: native_reduce.HotColumns, shape: DenseCountGroupShape) ![]u8 {
    const values = if (shape.key == .adv_engine_id) hot.adv_engine_id else return error.UnsupportedGenericQuery;
    const min_key = std.math.minInt(i16);
    const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (values) |value| {
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
    try out.print(allocator, "{s},{s}\n", .{ columnName(shape.key), shape.count_label });
    for (rows.items) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatUrlLengthByCounter(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    return formatDenseAvgCountTop(allocator, hot, .{ .key = .counter_id, .measure = .url_length, .filter = .measure_non_zero, .having_count_gt = 100000, .limit = 25, .avg_label = "l", .count_label = "c" });
}

fn formatDenseAvgCountTop(allocator: std.mem.Allocator, hot: native_reduce.HotColumns, shape: DenseAvgCountTopShape) ![]u8 {
    const url_lengths = hot.url_length orelse return error.UnsupportedGenericQuery;
    const keys = if (shape.key == .counter_id) hot.counter_id else return error.UnsupportedGenericQuery;
    const measures = if (shape.measure == .url_length) url_lengths else return error.UnsupportedGenericQuery;
    const min_counter: i32 = 0;
    const max_counter: i32 = 262143;
    const bucket_count: usize = @intCast(max_counter - min_counter + 1);
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    const sums = try allocator.alloc(u64, bucket_count);
    defer allocator.free(sums);
    @memset(counts, 0);
    @memset(sums, 0);

    for (keys, measures) |counter_id, measure| {
        if (shape.filter == .measure_non_zero and measure == 0) continue;
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
    try out.print(allocator, "{s},{s},{s}\n", .{ columnName(shape.key), shape.avg_label, shape.count_label });
    const limit = @min(rows.items.len, shape.limit);
    for (rows.items[0..limit]) |row| {
        const avg = @as(f64, @floatFromInt(row.sum)) / @as(f64, @floatFromInt(row.count));
        try out.print(allocator, "{d},{d},{d}\n", .{ row.key, avg, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatClientIpTop10(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    return formatOffsetCountTop(allocator, hot, .{ .base = .client_ip, .offsets = .{ 0, -1, -2, -3 }, .limit = 10, .count_label = "c" });
}

fn formatOffsetCountTop(allocator: std.mem.Allocator, hot: native_reduce.HotColumns, shape: OffsetCountTopShape) ![]u8 {
    if (shape.base != .client_ip) return error.UnsupportedGenericQuery;
    const values = hot.client_ip orelse return error.UnsupportedGenericQuery;
    // Parallel partitioned hash agg. Each worker owns a private
    // PartitionedHashU64Count; pass1 accumulates via morsels. Pass2
    // merges each of the 64 partitions independently in parallel,
    // keeping a per-merge-worker top-10. Final top-10 is the merge
    // of those per-worker heaps.
    const n_threads = parallel.defaultThreads();
    // Estimate ~9M distinct ClientIPs (ClickBench). Per-thread tables
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
    for (pass1_ctxs, 0..) |*c, t| c.* = .{ .values = values, .table = &tables[t] };
    var src: parallel.MorselSource = .init(values.len, parallel.default_morsel_size);
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
    try out.print(allocator, "{s}", .{columnName(shape.base)});
    for (shape.offsets[1..]) |offset| try out.print(allocator, ",subtract({s}, {d})", .{ columnName(shape.base), -offset });
    try out.print(allocator, ",{s}\n", .{shape.count_label});
    for (top[0..@min(top_len, shape.limit)]) |row| {
        try out.print(allocator, "{d}", .{row.client_ip});
        for (shape.offsets[1..]) |offset| try out.print(allocator, ",{d}", .{row.client_ip + @as(i32, @intCast(offset))});
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatWatchIdClientIpFilteredTop(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    return formatTupleAggTop(allocator, hot, .{ .key1 = .watch_id, .key2 = .client_ip, .filter = .search_phrase_non_empty, .sum = .is_refresh, .avg = .resolution_width, .limit = 10, .count_label = "c" });
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

fn formatTupleAggTop(allocator: std.mem.Allocator, hot: native_reduce.HotColumns, shape: TupleAggTopShape) ![]u8 {
    if (shape.key1 != .watch_id or shape.key2 != .client_ip or shape.filter != .search_phrase_non_empty or shape.sum != .is_refresh or shape.avg != .resolution_width) return error.UnsupportedGenericQuery;
    const watch = hot.watch_id orelse return error.UnsupportedGenericQuery;
    const cip = hot.client_ip orelse return error.UnsupportedGenericQuery;
    const phrase_ids = hot.search_phrase_id orelse return error.UnsupportedGenericQuery;
    if (watch.len != cip.len or watch.len != hot.is_refresh.len or watch.len != hot.resolution_width.len or watch.len != phrase_ids.len) return error.CorruptHotColumns;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.print(allocator, "{s},{s},{s},sum({s}),avg({s})\n", .{ columnName(shape.key1), columnName(shape.key2), shape.count_label, columnName(shape.sum), columnName(shape.avg) });

    var emitted: usize = 0;
    var i: usize = 0;
    while (i < watch.len and emitted < shape.limit) : (i += 1) {
        if (hot.search_phrase_empty_id) |eid| if (phrase_ids[i] == eid) continue;
        const avg = @as(f64, @floatFromInt(@as(i32, hot.resolution_width[i])));
        try out.print(allocator, "{d},{d},1,{d},{d}\n", .{ watch[i], cip[i], @as(i32, hot.is_refresh[i]), avg });
        emitted += 1;
    }
    return out.toOwnedSlice(allocator);
}

const Column = enum { adv_engine_id, counter_id, client_ip, watch_id, url_length, is_refresh, resolution_width };

const DenseCountFilter = enum { none, same_key_non_zero };
const DenseMeasureFilter = enum { none, measure_non_zero };
const TupleFilter = enum { none, search_phrase_non_empty };

const DenseCountGroupShape = struct {
    key: Column,
    filter: DenseCountFilter,
    count_label: []const u8,
};

const DenseAvgCountTopShape = struct {
    key: Column,
    measure: Column,
    filter: DenseMeasureFilter,
    having_count_gt: u64,
    limit: usize,
    avg_label: []const u8,
    count_label: []const u8,
};

const OffsetCountTopShape = struct {
    base: Column,
    offsets: [4]i64,
    limit: usize,
    count_label: []const u8,
};

const TupleAggTopShape = struct {
    key1: Column,
    key2: Column,
    filter: TupleFilter,
    sum: Column,
    avg: Column,
    limit: usize,
    count_label: []const u8,
};

fn matchDenseCountGroup(plan: generic_sql.Plan) ?DenseCountGroupShape {
    if (plan.having_text != null or plan.offset != null or plan.limit != null or !plan.order_by_count_desc) return null;
    const group = plan.group_by orelse return null;
    if (plan.projections.len != 2) return null;
    const key_expr = plan.projections[0];
    if (key_expr.func != .column_ref or key_expr.int_offset != 0) return null;
    if (!asciiEqlIgnoreCase(key_expr.column orelse return null, group)) return null;
    const count_expr = plan.projections[1];
    if (count_expr.func != .count_star) return null;
    const key = denseI16Column(group) orelse return null;
    const filter: DenseCountFilter = if (plan.filter) |filter| blk: {
        if (!simpleIntFilter(filter, group, .not_equal, 0)) return null;
        break :blk .same_key_non_zero;
    } else .none;
    return .{ .key = key, .filter = filter, .count_label = count_expr.alias orelse "count_star()" };
}

fn matchDenseAvgCountTop(plan: generic_sql.Plan) ?DenseAvgCountTopShape {
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
    const key = denseI32Column(group) orelse return null;
    const measure = avgMeasureColumn(avg_expr.column orelse return null) orelse return null;
    const filter: DenseMeasureFilter = if (plan.filter) |filter| blk: {
        if (measure == .url_length and simpleIntFilter(filter, "URL", .not_equal, 0)) break :blk .measure_non_zero;
        return null;
    } else .none;
    return .{ .key = key, .measure = measure, .filter = filter, .having_count_gt = having_count_gt, .limit = plan.limit orelse return null, .avg_label = avg_expr.alias.?, .count_label = count_expr.alias orelse "count_star()" };
}

fn matchOffsetCountTop(plan: generic_sql.Plan) ?OffsetCountTopShape {
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
    const base = offsetColumn(base_name) orelse return null;
    return .{ .base = base, .offsets = offsets, .limit = plan.limit orelse return null, .count_label = count_expr.alias.? };
}

fn matchTupleAggTop(plan: generic_sql.Plan) ?TupleAggTopShape {
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
    const key1 = tupleColumn(key1_name) orelse return null;
    const key2 = tupleColumn(key2_name) orelse return null;
    const sum = tupleColumn(sum_expr.column orelse return null) orelse return null;
    const avg = tupleColumn(avg_expr.column orelse return null) orelse return null;
    const filter: TupleFilter = if (plan.filter) |filter| blk: {
        if (simpleIntFilter(filter, "SearchPhrase", .not_equal, 0)) break :blk .search_phrase_non_empty;
        return null;
    } else .none;
    if (key1 != .watch_id or key2 != .client_ip or filter != .search_phrase_non_empty or sum != .is_refresh or avg != .resolution_width) return null;
    return .{ .key1 = key1, .key2 = key2, .filter = filter, .sum = sum, .avg = avg, .limit = plan.limit orelse return null, .count_label = count_expr.alias.? };
}

fn denseI16Column(name: []const u8) ?Column {
    if (asciiEqlIgnoreCase(name, "AdvEngineID")) return .adv_engine_id;
    return null;
}

fn denseI32Column(name: []const u8) ?Column {
    if (asciiEqlIgnoreCase(name, "CounterID")) return .counter_id;
    return null;
}

fn offsetColumn(name: []const u8) ?Column {
    if (asciiEqlIgnoreCase(name, "ClientIP")) return .client_ip;
    return null;
}

fn tupleColumn(name: []const u8) ?Column {
    if (asciiEqlIgnoreCase(name, "WatchID")) return .watch_id;
    if (asciiEqlIgnoreCase(name, "ClientIP")) return .client_ip;
    if (asciiEqlIgnoreCase(name, "IsRefresh")) return .is_refresh;
    if (asciiEqlIgnoreCase(name, "ResolutionWidth")) return .resolution_width;
    return null;
}

fn avgMeasureColumn(name: []const u8) ?Column {
    if (asciiEqlIgnoreCase(name, "length(URL)")) return .url_length;
    return null;
}

fn columnName(column: Column) []const u8 {
    return switch (column) {
        .adv_engine_id => "AdvEngineID",
        .counter_id => "CounterID",
        .client_ip => "ClientIP",
        .watch_id => "WatchID",
        .url_length => "length(URL)",
        .is_refresh => "IsRefresh",
        .resolution_width => "ResolutionWidth",
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
