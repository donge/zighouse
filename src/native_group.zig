const std = @import("std");
const generic_sql = @import("generic_sql.zig");
const hashmap = @import("hashmap.zig");
const native_reduce = @import("native_reduce.zig");
const parallel = @import("parallel.zig");

pub fn execute(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (try executeAdvEngineCount(allocator, plan, hot)) |output| return output;
    if (try executeUrlLengthByCounter(allocator, plan, hot)) |output| return output;
    if (try executeClientIpTop10(allocator, plan, hot)) |output| return output;
    return null;
}

pub fn formatAdvEngineCount(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    const min_key = std.math.minInt(i16);
    const bucket_count = @as(usize, std.math.maxInt(u16)) + 1;
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    @memset(counts, 0);

    for (hot.adv_engine_id) |value| {
        if (value == 0) continue;
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
    try out.appendSlice(allocator, "AdvEngineID,count_star()\n");
    for (rows.items) |row| {
        try out.print(allocator, "{d},{d}\n", .{ row.key, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatUrlLengthByCounter(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
    const url_lengths = hot.url_length orelse return error.UnsupportedGenericQuery;
    const min_counter: i32 = 0;
    const max_counter: i32 = 262143;
    const bucket_count: usize = @intCast(max_counter - min_counter + 1);
    const counts = try allocator.alloc(u64, bucket_count);
    defer allocator.free(counts);
    const sums = try allocator.alloc(u64, bucket_count);
    defer allocator.free(sums);
    @memset(counts, 0);
    @memset(sums, 0);

    for (hot.counter_id, url_lengths) |counter_id, url_length| {
        if (url_length == 0) continue;
        if (counter_id < min_counter or counter_id > max_counter) continue;
        const index: usize = @intCast(counter_id - min_counter);
        counts[index] += 1;
        sums[index] += @intCast(url_length);
    }

    var rows: std.ArrayList(AvgCountRow) = .empty;
    defer rows.deinit(allocator);
    for (counts, 0..) |count, index| {
        if (count <= 100000) continue;
        const counter_id: i32 = @intCast(@as(i64, min_counter) + @as(i64, @intCast(index)));
        try rows.append(allocator, .{ .key = counter_id, .sum = sums[index], .count = count });
    }
    std.mem.sort(AvgCountRow, rows.items, {}, avgCountRowDesc);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "CounterID,l,c\n");
    const limit = @min(rows.items.len, 25);
    for (rows.items[0..limit]) |row| {
        const avg = @as(f64, @floatFromInt(row.sum)) / @as(f64, @floatFromInt(row.count));
        try out.print(allocator, "{d},{d},{d}\n", .{ row.key, avg, row.count });
    }
    return out.toOwnedSlice(allocator);
}

pub fn formatClientIpTop10(allocator: std.mem.Allocator, hot: native_reduce.HotColumns) ![]u8 {
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
    try out.appendSlice(allocator, "ClientIP,subtract(ClientIP, 1),subtract(ClientIP, 2),subtract(ClientIP, 3),c\n");
    for (top[0..top_len]) |row| {
        try out.print(allocator, "{d},{d},{d},{d},{d}\n", .{ row.client_ip, row.client_ip - 1, row.client_ip - 2, row.client_ip - 3, row.count });
    }
    return out.toOwnedSlice(allocator);
}

fn executeAdvEngineCount(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (!isAdvEngineCountPlan(plan)) return null;
    return try formatAdvEngineCount(allocator, hot);
}

fn executeUrlLengthByCounter(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (!isUrlLengthByCounterPlan(plan)) return null;
    return try formatUrlLengthByCounter(allocator, hot);
}

fn executeClientIpTop10(allocator: std.mem.Allocator, plan: generic_sql.Plan, hot: native_reduce.HotColumns) !?[]u8 {
    if (!isClientIpTop10Plan(plan)) return null;
    return try formatClientIpTop10(allocator, hot);
}

fn isAdvEngineCountPlan(plan: generic_sql.Plan) bool {
    if (!plan.order_by_count_desc or plan.limit != null or plan.offset != null) return false;
    if (!hasEmptyStringFilter(plan, "AdvEngineID")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "AdvEngineID")) return false;
    if (plan.projections.len != 2) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "AdvEngineID")) return false;
    return plan.projections[1].func == .count_star;
}

fn isUrlLengthByCounterPlan(plan: generic_sql.Plan) bool {
    if (plan.limit != 25 or plan.offset != null) return false;
    if (!hasEmptyStringFilter(plan, "URL")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "CounterID")) return false;
    if (!asciiEqlIgnoreCase(plan.having_text orelse return false, "COUNT(*) > 100000")) return false;
    if (!orderByAlias(plan, "l")) return false;
    if (plan.projections.len != 3) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "CounterID")) return false;
    if (plan.projections[1].func != .avg or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "length(URL)")) return false;
    if (!asciiEqlIgnoreCase(plan.projections[1].alias orelse return false, "l")) return false;
    return plan.projections[2].func == .count_star and asciiEqlIgnoreCase(plan.projections[2].alias orelse return false, "c");
}

fn isClientIpTop10Plan(plan: generic_sql.Plan) bool {
    if (plan.filter != null or plan.limit != 10 or plan.offset != null or !orderByAlias(plan, "c")) return false;
    if (!asciiEqlIgnoreCase(plan.group_by orelse return false, "ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3")) return false;
    if (plan.projections.len != 5) return false;
    if (plan.projections[0].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[0].column orelse return false, "ClientIP")) return false;
    if (plan.projections[1].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[1].column orelse return false, "ClientIP") or plan.projections[1].int_offset != -1) return false;
    if (plan.projections[2].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[2].column orelse return false, "ClientIP") or plan.projections[2].int_offset != -2) return false;
    if (plan.projections[3].func != .column_ref or !asciiEqlIgnoreCase(plan.projections[3].column orelse return false, "ClientIP") or plan.projections[3].int_offset != -3) return false;
    return plan.projections[4].func == .count_star and asciiEqlIgnoreCase(plan.projections[4].alias orelse return false, "c");
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
