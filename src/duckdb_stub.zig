const std = @import("std");
const storage = @import("storage.zig");

pub const QueryRange = struct {
    first: usize = 1,
    limit: ?usize = null,

    pub fn contains(self: QueryRange, query_num: usize) bool {
        if (query_num < self.first) return false;
        if (self.limit) |n| return query_num < self.first + n;
        return true;
    }
};

pub const DuckDb = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    io: std.Io,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) DuckDb {
        return .{ .allocator = allocator, .data_dir = data_dir, .io = io };
    }

    pub fn deinit(_: *DuckDb) void {}

    pub fn importParquet(self: *DuckDb, parquet_path: []const u8) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
    }

    pub fn query(_: *DuckDb, _: []const u8) ![]u8 {
        return error.DuckDbDisabled;
    }

    pub fn queryLimited(_: *DuckDb, _: []const u8, _: ?u64) ![]u8 {
        return error.DuckDbDisabled;
    }

    pub fn bench(self: *DuckDb, queries_path: []const u8, range: QueryRange) !void {
        try benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn runQuery(self: *DuckDb, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    pub fn runRawSql(_: *DuckDb, _: []const u8) ![]u8 {
        return error.DuckDbDisabled;
    }
};

pub fn benchWithRunner(allocator: std.mem.Allocator, io: std.Io, queries_path: []const u8, range: QueryRange, runner: anytype) !void {
    const queries = try std.Io.Dir.cwd().readFileAlloc(io, queries_path, allocator, .limited(512 * 1024));
    defer allocator.free(queries);

    var query_count: usize = 0;
    var null_count: usize = 0;
    var first_sum_ns: u64 = 0;
    var warm_best_sum_ns: u64 = 0;
    var all_sum_ns: u64 = 0;
    var query_num: usize = 1;
    var line_it = std.mem.splitScalar(u8, queries, '\n');
    while (line_it.next()) |raw_line| : (query_num += 1) {
        const query_text = std.mem.trim(u8, raw_line, " \t\r");
        if (query_text.len == 0) continue;
        if (!range.contains(query_num)) continue;

        try writeOut(io, "[");
        var row_ns: [3]?u64 = .{ null, null, null };
        for (0..3) |i| {
            const started = std.Io.Clock.Timestamp.now(io, .awake);
            const result = runner.runQuery(query_text) catch null;
            const elapsed = if (result) |output| blk: {
                allocator.free(output);
                const ended = std.Io.Clock.Timestamp.now(io, .awake);
                break :blk @as(u64, @intCast(started.durationTo(ended).raw.nanoseconds));
            } else null;
            row_ns[i] = elapsed;

            if (elapsed) |ns| {
                const seconds = @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
                var buf: [64]u8 = undefined;
                const text = try std.fmt.bufPrint(&buf, "{d:.6}", .{seconds});
                try writeOut(io, text);
            } else {
                try writeOut(io, "null");
            }
            if (i != 2) try writeOut(io, ", ");
        }
        try writeOut(io, "],\n");
        query_count += 1;
        if (row_ns[0]) |ns| {
            first_sum_ns += ns;
            all_sum_ns += ns;
        }
        var warm_best: ?u64 = null;
        for (row_ns[1..]) |maybe_ns| {
            if (maybe_ns) |ns| {
                all_sum_ns += ns;
                warm_best = if (warm_best) |best| @min(best, ns) else ns;
            }
        }
        if (warm_best) |ns| warm_best_sum_ns += ns else null_count += 1;
    }
    try writeBenchSummary(io, query_count, null_count, first_sum_ns, warm_best_sum_ns, all_sum_ns);
}

fn writeBenchSummary(io: std.Io, query_count: usize, null_count: usize, first_sum_ns: u64, warm_best_sum_ns: u64, all_sum_ns: u64) !void {
    var buf: [256]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf,
        "summary: queries={d} nulls={d} first_sum={d:.6} warm_best_sum={d:.6} all_runs_sum={d:.6}\n",
        .{ query_count, null_count, @as(f64, @floatFromInt(first_sum_ns)) / std.time.ns_per_s, @as(f64, @floatFromInt(warm_best_sum_ns)) / std.time.ns_per_s, @as(f64, @floatFromInt(all_sum_ns)) / std.time.ns_per_s },
    );
    try writeOut(io, text);
}

pub fn sqlStringLiteral(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '\'');
    for (value) |ch| {
        if (ch == '\'') try out.append(allocator, '\'');
        try out.append(allocator, ch);
    }
    try out.append(allocator, '\'');
    return out.toOwnedSlice(allocator);
}

pub fn duckDbExe() []const u8 { return "duckdb-disabled"; }
pub fn vectorSmoke(_: std.mem.Allocator, _: std.Io, _: []const u8, _: u64) !void { return error.DuckDbDisabled; }

pub const ClickBenchHotRow = struct {
    watch: i64,
    title: []const u8,
    event_time: i64,
    event_date: i32,
    counter: i32,
    client_ip: i32,
    region: i32,
    user: i64,
    url: []const u8,
    referer: []const u8,
    refresh: i16,
    width: i16,
    mobile_phone: i16,
    mobile_model: []const u8,
    trafic_source: i16,
    search_engine: i16,
    search_phrase: []const u8,
    adv: i16,
    window_width: i16,
    window_height: i16,
    is_link: i16,
    is_download: i16,
    dont_count: i16,
    referer_hash: i64,
    url_hash: i64,
    title_hash: i64,
};

pub const DuckString = []const u8;

pub const ClickBenchHotChunk = struct {
    watch: []const i64,
    title: []const DuckString,
    event_time: []const i64,
    event_date: []const i32,
    counter: []const i32,
    client_ip: []const i32,
    region: []const i32,
    user: []const i64,
    url: []const DuckString,
    referer: ?[]const DuckString = null,
    refresh: []const i16,
    width: []const i16,
    mobile_phone: []const i16,
    mobile_model: []const DuckString,
    trafic_source: []const i16,
    search_engine: []const i16,
    search_phrase: []const DuckString,
    adv: []const i16,
    window_width: []const i16,
    window_height: []const i16,
    is_link: []const i16,
    is_download: []const i16,
    dont_count: []const i16,
    referer_hash: []const i64,
    url_hash: []const i64,
    title_hash: []const i64,

    pub fn len(self: ClickBenchHotChunk) usize { return self.watch.len; }
};

pub const ClickBenchFixedChunk = struct {
    watch: []const i64,
    event_time: []const i64,
    event_date: []const i32,
    counter: []const i32,
    client_ip: []const i32,
    region: []const i32,
    user: []const i64,
    refresh: []const i16,
    width: []const i16,
    mobile_phone: []const i16,
    trafic_source: []const i16,
    search_engine: []const i16,
    adv: []const i16,
    window_width: []const i16,
    window_height: []const i16,
    is_link: []const i16,
    is_download: []const i16,
    dont_count: []const i16,
    referer_hash: []const i64,
    url_hash: []const i64,

    pub fn len(self: ClickBenchFixedChunk) usize { return self.watch.len; }
};

pub fn duckStringAt(values: []const DuckString, i: usize) []const u8 { return values[i]; }
pub fn streamClickBenchFixedChunks(_: std.mem.Allocator, _: std.Io, _: []const u8, _: ?u64, _: anytype, _: anytype) !void { return error.DuckDbDisabled; }
pub fn streamClickBenchHotRows(_: std.mem.Allocator, _: std.Io, _: []const u8, _: ?u64, _: anytype, _: anytype) !void { return error.DuckDbDisabled; }
pub fn streamClickBenchHotChunks(_: std.mem.Allocator, _: std.Io, _: []const u8, _: ?u64, _: anytype, _: anytype) !void { return error.DuckDbDisabled; }

fn writeOut(io: std.Io, s: []const u8) !void {
    try std.Io.File.stdout().writeStreamingAll(io, s);
}
