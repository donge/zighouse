const std = @import("std");
const storage = @import("storage.zig");

const c = @cImport({
    @cInclude("duckdb.h");
});

const default_duckdb_exe = "/opt/homebrew/bin/duckdb";

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
    io_threaded: std.Io.Threaded,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) DuckDb {
        return .{
            .allocator = allocator,
            .data_dir = data_dir,
            .io = io,
            .io_threaded = .init(allocator, .{}),
        };
    }

    pub fn deinit(self: *DuckDb) void {
        self.io_threaded.deinit();
    }

    pub fn importParquet(self: *DuckDb, parquet_path: []const u8) !void {
        try storage.initStore(self.io, self.data_dir);
        try storage.writeImportManifest(self.io, self.allocator, self.data_dir, parquet_path);
    }

    pub fn query(self: *DuckDb, sql: []const u8) ![]u8 {
        const parquet_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(parquet_path);
        const wrapped = try wrapSql(self.allocator, parquet_path, try importRowLimit(self.allocator, self.io, self.data_dir), sql);
        defer self.allocator.free(wrapped);
        return self.runDuckDb(&.{ duckDbExe(), "-csv", "-c", wrapped });
    }

    pub fn queryLimited(self: *DuckDb, sql: []const u8, limit_rows: ?u64) ![]u8 {
        const parquet_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(parquet_path);
        const wrapped = try wrapSql(self.allocator, parquet_path, limit_rows, sql);
        defer self.allocator.free(wrapped);
        return self.runDuckDb(&.{ duckDbExe(), "-csv", "-c", wrapped });
    }

    pub fn bench(self: *DuckDb, queries_path: []const u8, range: QueryRange) !void {
        try benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn runQuery(self: *DuckDb, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    pub fn runRawSql(self: *DuckDb, sql: []const u8) ![]u8 {
        return self.runDuckDb(&.{ duckDbExe(), "-csv", "-c", sql });
    }

    fn runDuckDb(self: *DuckDb, argv: []const []const u8) ![]u8 {
        const result = std.process.run(self.allocator, self.io_threaded.io(), .{
            .argv = argv,
            .stdout_limit = .unlimited,
            .stderr_limit = .unlimited,
        }) catch |err| switch (err) {
            error.FileNotFound => return error.DuckDbCliNotFound,
            else => return err,
        };
        errdefer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        switch (result.term) {
            .exited => |code| if (code != 0) {
                try std.Io.File.stderr().writeStreamingAll(self.io, result.stderr);
                return error.DuckDbCommandFailed;
            },
            else => return error.DuckDbCommandFailed,
        }

        return result.stdout;
    }

    fn writeOut(self: *DuckDb, bytes: []const u8) !void {
        try std.Io.File.stdout().writeStreamingAll(self.io, bytes);
    }
};

fn importRowLimit(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !?u64 {
    const manifest = storage.readImportManifest(io, allocator, data_dir) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(manifest);
    var lines = std.mem.splitScalar(u8, manifest, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "row_count=")) continue;
        const raw = std.mem.trim(u8, line["row_count=".len..], " \t\r");
        const rows = try std.fmt.parseInt(u64, raw, 10);
        return if (rows == 0) null else rows;
    }
    return null;
}

fn wrapSql(allocator: std.mem.Allocator, parquet_path: []const u8, limit_rows: ?u64, sql: []const u8) ![]u8 {
    const escaped_path = try sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(escaped_path);
    const source = if (limit_rows) |n|
        try std.fmt.allocPrint(allocator, "(SELECT * FROM read_parquet({s}, binary_as_string=True) LIMIT {d})", .{ escaped_path, n })
    else
        try std.fmt.allocPrint(allocator, "read_parquet({s}, binary_as_string=True)", .{escaped_path});
    defer allocator.free(source);
    return std.fmt.allocPrint(allocator,
        \\CREATE OR REPLACE VIEW hits AS
        \\SELECT * REPLACE (
        \\    make_date(EventDate) AS EventDate,
        \\    epoch_ms(EventTime * 1000) AS EventTime,
        \\    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        \\    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
        \\FROM {s};
        \\{s}
    , .{ source, sql });
}

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
        if (warm_best) |ns| {
            warm_best_sum_ns += ns;
        } else {
            null_count += 1;
        }
    }
    try writeBenchSummary(io, query_count, null_count, first_sum_ns, warm_best_sum_ns, all_sum_ns);
}

fn writeBenchSummary(io: std.Io, query_count: usize, null_count: usize, first_sum_ns: u64, warm_best_sum_ns: u64, all_sum_ns: u64) !void {
    var buf: [256]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf,
        "summary: queries={d} nulls={d} first_sum={d:.6} warm_best_sum={d:.6} all_runs_sum={d:.6}\n",
        .{
            query_count,
            null_count,
            @as(f64, @floatFromInt(first_sum_ns)) / std.time.ns_per_s,
            @as(f64, @floatFromInt(warm_best_sum_ns)) / std.time.ns_per_s,
            @as(f64, @floatFromInt(all_sum_ns)) / std.time.ns_per_s,
        },
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

pub fn vectorSmoke(allocator: std.mem.Allocator, io: std.Io, parquet_path: []const u8, limit_rows: u64) !void {
    const parquet_literal = try sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const sql_text = try std.fmt.allocPrint(allocator,
        \\SELECT CAST(WatchID AS BIGINT) AS WatchID,
        \\       CAST(EventDate AS BIGINT) AS EventDate,
        \\       CAST(UserID AS BIGINT) AS UserID,
        \\       CAST(URL AS VARCHAR) AS URL,
        \\       CAST(SearchPhrase AS VARCHAR) AS SearchPhrase
        \\FROM read_parquet({s}, binary_as_string=True)
        \\LIMIT {d}
    , .{ parquet_literal, limit_rows });
    defer allocator.free(sql_text);
    const sql = try allocator.dupeZ(u8, sql_text);
    defer allocator.free(sql);

    var db: c.duckdb_database = null;
    if (c.duckdb_open(null, &db) != c.DuckDBSuccess) return error.DuckDbOpenFailed;
    defer c.duckdb_close(&db);

    var con: c.duckdb_connection = null;
    if (c.duckdb_connect(db, &con) != c.DuckDBSuccess) return error.DuckDbConnectFailed;
    defer c.duckdb_disconnect(&con);

    const started = std.Io.Clock.Timestamp.now(io, .awake);

    var prepared: c.duckdb_prepared_statement = null;
    if (c.duckdb_prepare(con, sql.ptr, &prepared) != c.DuckDBSuccess) {
        const err = c.duckdb_prepare_error(prepared);
        if (err != null) try printErr(io, "duckdb prepare error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_prepare(&prepared);
        return error.DuckDbPrepareFailed;
    }
    defer c.duckdb_destroy_prepare(&prepared);

    var pending: c.duckdb_pending_result = null;
    if (c.duckdb_pending_prepared_streaming(prepared, &pending) != c.DuckDBSuccess) {
        const err = c.duckdb_pending_error(pending);
        if (err != null) try printErr(io, "duckdb pending error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_pending(&pending);
        return error.DuckDbPendingFailed;
    }
    defer c.duckdb_destroy_pending(&pending);

    while (true) {
        const state = c.duckdb_pending_execute_task(pending);
        if (state == c.DUCKDB_PENDING_RESULT_READY) break;
        if (state == c.DUCKDB_PENDING_ERROR) {
            const err = c.duckdb_pending_error(pending);
            if (err != null) try printErr(io, "duckdb execution error: {s}\n", .{std.mem.span(err)});
            return error.DuckDbExecutionFailed;
        }
    }

    var result: c.duckdb_result = undefined;
    if (c.duckdb_execute_pending(pending, &result) != c.DuckDBSuccess) {
        const err = c.duckdb_result_error(&result);
        if (err != null) try printErr(io, "duckdb result error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_result(&result);
        return error.DuckDbQueryFailed;
    }
    defer c.duckdb_destroy_result(&result);

    var rows: u64 = 0;
    var chunks: u64 = 0;
    var watch_sum: u64 = 0;
    var event_date_sum: i64 = 0;
    var user_sum: u64 = 0;
    var url_len_sum: u64 = 0;
    var phrase_len_sum: u64 = 0;
    while (true) {
        var chunk = c.duckdb_stream_fetch_chunk(result);
        if (chunk == null) break;
        chunks += 1;
        const n: usize = @intCast(c.duckdb_data_chunk_get_size(chunk));
        rows += n;

        const watch_data: [*]const i64 = @ptrCast(@alignCast(c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, 0))));
        const event_date_data: [*]const i64 = @ptrCast(@alignCast(c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, 1))));
        const user_data: [*]const i64 = @ptrCast(@alignCast(c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, 2))));
        const url_data: [*]c.duckdb_string_t = @ptrCast(@alignCast(c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, 3))));
        const phrase_data: [*]c.duckdb_string_t = @ptrCast(@alignCast(c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, 4))));

        for (0..n) |i| {
            watch_sum +%= @bitCast(watch_data[i]);
            event_date_sum +%= event_date_data[i];
            user_sum +%= @bitCast(user_data[i]);
            url_len_sum += c.duckdb_string_t_length(url_data[i]);
            phrase_len_sum += c.duckdb_string_t_length(phrase_data[i]);
        }
        c.duckdb_destroy_data_chunk(&chunk);
    }
    const ended = std.Io.Clock.Timestamp.now(io, .awake);
    const seconds = @as(f64, @floatFromInt(@as(u64, @intCast(started.durationTo(ended).raw.nanoseconds)))) / std.time.ns_per_s;
    try printOut(io, "duckdb_vector_smoke rows={d} chunks={d} seconds={d:.6} watch_sum={d} event_date_sum={d} user_sum={d} url_len_sum={d} phrase_len_sum={d}\n", .{
        rows,
        chunks,
        seconds,
        watch_sum,
        event_date_sum,
        user_sum,
        url_len_sum,
        phrase_len_sum,
    });
}

pub fn duckDbExe() []const u8 {
    if (std.c.getenv("ZIGHOUSE_DUCKDB_EXE")) |value| return std.mem.span(value);
    return default_duckdb_exe;
}

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

pub const DuckString = c.duckdb_string_t;

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

    pub fn len(self: ClickBenchHotChunk) usize {
        return self.watch.len;
    }
};

pub fn streamClickBenchHotRows(allocator: std.mem.Allocator, io: std.Io, parquet_path: []const u8, limit_rows: ?u64, context: anytype, callback: anytype) !void {
    const RowContext = struct {
        outer: @TypeOf(context),

        fn onChunk(self: *@This(), chunk: ClickBenchHotChunk) !void {
            for (0..chunk.len()) |i| {
                try callback(self.outer, ClickBenchHotRow{
                    .watch = chunk.watch[i],
                    .title = duckStringAt(chunk.title, i),
                    .event_time = chunk.event_time[i],
                    .event_date = chunk.event_date[i],
                    .counter = chunk.counter[i],
                    .client_ip = chunk.client_ip[i],
                    .region = chunk.region[i],
                    .user = chunk.user[i],
                    .url = duckStringAt(chunk.url, i),
                    .referer = if (chunk.referer) |referers| duckStringAt(referers, i) else "",
                    .refresh = chunk.refresh[i],
                    .width = chunk.width[i],
                    .mobile_phone = chunk.mobile_phone[i],
                    .mobile_model = duckStringAt(chunk.mobile_model, i),
                    .trafic_source = chunk.trafic_source[i],
                    .search_engine = chunk.search_engine[i],
                    .search_phrase = duckStringAt(chunk.search_phrase, i),
                    .adv = chunk.adv[i],
                    .window_width = chunk.window_width[i],
                    .window_height = chunk.window_height[i],
                    .is_link = chunk.is_link[i],
                    .is_download = chunk.is_download[i],
                    .dont_count = chunk.dont_count[i],
                    .referer_hash = chunk.referer_hash[i],
                    .url_hash = chunk.url_hash[i],
                    .title_hash = chunk.title_hash[i],
                });
            }
        }
    };
    var row_context = RowContext{ .outer = context };
    try streamClickBenchHotChunks(allocator, io, parquet_path, limit_rows, &row_context, RowContext.onChunk);
}

pub fn streamClickBenchHotChunks(allocator: std.mem.Allocator, io: std.Io, parquet_path: []const u8, limit_rows: ?u64, context: anytype, callback: anytype) !void {
    const parquet_literal = try sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);
    const limit_clause = if (limit_rows) |n| try std.fmt.allocPrint(allocator, "\nLIMIT {d}", .{n}) else try allocator.dupe(u8, "");
    defer allocator.free(limit_clause);
    const include_referer = std.c.getenv("ZIGHOUSE_IMPORT_REFERER") != null;
    const referer_select = if (include_referer) ",\n       COALESCE(CAST(Referer AS VARCHAR), '') AS Referer" else "";
    const sql_text = try std.fmt.allocPrint(allocator,
        \\SELECT CAST(WatchID AS BIGINT) AS WatchID,
        \\       CAST(Title AS VARCHAR) AS Title,
        \\       CAST(EventTime AS BIGINT) AS EventTime,
        \\       CAST(EventDate AS INTEGER) AS EventDate,
        \\       CAST(CounterID AS INTEGER) AS CounterID,
        \\       CAST(ClientIP AS INTEGER) AS ClientIP,
        \\       CAST(RegionID AS INTEGER) AS RegionID,
        \\       CAST(UserID AS BIGINT) AS UserID,
        \\       CAST(URL AS VARCHAR) AS URL{s},
        \\       CAST(IsRefresh AS SMALLINT) AS IsRefresh,
        \\       CAST(ResolutionWidth AS SMALLINT) AS ResolutionWidth,
        \\       CAST(MobilePhone AS SMALLINT) AS MobilePhone,
        \\       CAST(MobilePhoneModel AS VARCHAR) AS MobilePhoneModel,
        \\       CAST(TraficSourceID AS SMALLINT) AS TraficSourceID,
        \\       CAST(SearchEngineID AS SMALLINT) AS SearchEngineID,
        \\       CAST(SearchPhrase AS VARCHAR) AS SearchPhrase,
        \\       CAST(AdvEngineID AS SMALLINT) AS AdvEngineID,
        \\       CAST(WindowClientWidth AS SMALLINT) AS WindowClientWidth,
        \\       CAST(WindowClientHeight AS SMALLINT) AS WindowClientHeight,
        \\       CAST(IsLink AS SMALLINT) AS IsLink,
        \\       CAST(IsDownload AS SMALLINT) AS IsDownload,
        \\       CAST(DontCountHits AS SMALLINT) AS DontCountHits,
        \\       CAST(RefererHash AS BIGINT) AS RefererHash,
        \\       CAST(URLHash AS BIGINT) AS URLHash,
        \\       CAST(hash(CAST(Title AS VARCHAR)) & 9223372036854775807 AS BIGINT) AS TitleHash
        \\FROM read_parquet({s}, binary_as_string=True){s}
    , .{ referer_select, parquet_literal, limit_clause });
    defer allocator.free(sql_text);
    const sql = try allocator.dupeZ(u8, sql_text);
    defer allocator.free(sql);

    var db: c.duckdb_database = null;
    if (c.duckdb_open(null, &db) != c.DuckDBSuccess) return error.DuckDbOpenFailed;
    defer c.duckdb_close(&db);

    var con: c.duckdb_connection = null;
    if (c.duckdb_connect(db, &con) != c.DuckDBSuccess) return error.DuckDbConnectFailed;
    defer c.duckdb_disconnect(&con);

    var prepared: c.duckdb_prepared_statement = null;
    if (c.duckdb_prepare(con, sql.ptr, &prepared) != c.DuckDBSuccess) {
        const err = c.duckdb_prepare_error(prepared);
        if (err != null) try printErr(io, "duckdb prepare error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_prepare(&prepared);
        return error.DuckDbPrepareFailed;
    }
    defer c.duckdb_destroy_prepare(&prepared);

    var pending: c.duckdb_pending_result = null;
    if (c.duckdb_pending_prepared_streaming(prepared, &pending) != c.DuckDBSuccess) {
        const err = c.duckdb_pending_error(pending);
        if (err != null) try printErr(io, "duckdb pending error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_pending(&pending);
        return error.DuckDbPendingFailed;
    }
    defer c.duckdb_destroy_pending(&pending);

    while (true) {
        const state = c.duckdb_pending_execute_task(pending);
        if (state == c.DUCKDB_PENDING_RESULT_READY) break;
        if (state == c.DUCKDB_PENDING_ERROR) {
            const err = c.duckdb_pending_error(pending);
            if (err != null) try printErr(io, "duckdb execution error: {s}\n", .{std.mem.span(err)});
            return error.DuckDbExecutionFailed;
        }
    }

    var result: c.duckdb_result = undefined;
    if (c.duckdb_execute_pending(pending, &result) != c.DuckDBSuccess) {
        const err = c.duckdb_result_error(&result);
        if (err != null) try printErr(io, "duckdb result error: {s}\n", .{std.mem.span(err)});
        c.duckdb_destroy_result(&result);
        return error.DuckDbQueryFailed;
    }
    defer c.duckdb_destroy_result(&result);

    while (true) {
        var chunk = c.duckdb_stream_fetch_chunk(result);
        if (chunk == null) break;
        const n: usize = @intCast(c.duckdb_data_chunk_get_size(chunk));
        const watch: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 0)));
        const title: [*]DuckString = @ptrCast(@alignCast(vectorData(chunk, 1)));
        const event_time: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 2)));
        const event_date: [*]const i32 = @ptrCast(@alignCast(vectorData(chunk, 3)));
        const counter: [*]const i32 = @ptrCast(@alignCast(vectorData(chunk, 4)));
        const client_ip: [*]const i32 = @ptrCast(@alignCast(vectorData(chunk, 5)));
        const region: [*]const i32 = @ptrCast(@alignCast(vectorData(chunk, 6)));
        const user: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 7)));
        const url: [*]DuckString = @ptrCast(@alignCast(vectorData(chunk, 8)));
        const referer_col: ?[*]DuckString = if (include_referer) @ptrCast(@alignCast(vectorData(chunk, 9))) else null;
        const base: usize = if (include_referer) 1 else 0;
        const refresh: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 9 + base)));
        const width: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 10 + base)));
        const mobile_phone: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 11 + base)));
        const mobile_model: [*]DuckString = @ptrCast(@alignCast(vectorData(chunk, 12 + base)));
        const trafic_source: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 13 + base)));
        const search_engine: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 14 + base)));
        const search_phrase: [*]DuckString = @ptrCast(@alignCast(vectorData(chunk, 15 + base)));
        const adv: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 16 + base)));
        const window_width: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 17 + base)));
        const window_height: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 18 + base)));
        const is_link: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 19 + base)));
        const is_download: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 20 + base)));
        const dont_count: [*]const i16 = @ptrCast(@alignCast(vectorData(chunk, 21 + base)));
        const referer_hash: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 22 + base)));
        const url_hash: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 23 + base)));
        const title_hash: [*]const i64 = @ptrCast(@alignCast(vectorData(chunk, 24 + base)));
        try callback(context, ClickBenchHotChunk{
            .watch = watch[0..n],
            .title = title[0..n],
            .event_time = event_time[0..n],
            .event_date = event_date[0..n],
            .counter = counter[0..n],
            .client_ip = client_ip[0..n],
            .region = region[0..n],
            .user = user[0..n],
            .url = url[0..n],
            .referer = if (referer_col) |col| col[0..n] else null,
            .refresh = refresh[0..n],
            .width = width[0..n],
            .mobile_phone = mobile_phone[0..n],
            .mobile_model = mobile_model[0..n],
            .trafic_source = trafic_source[0..n],
            .search_engine = search_engine[0..n],
            .search_phrase = search_phrase[0..n],
            .adv = adv[0..n],
            .window_width = window_width[0..n],
            .window_height = window_height[0..n],
            .is_link = is_link[0..n],
            .is_download = is_download[0..n],
            .dont_count = dont_count[0..n],
            .referer_hash = referer_hash[0..n],
            .url_hash = url_hash[0..n],
            .title_hash = title_hash[0..n],
        });
        c.duckdb_destroy_data_chunk(&chunk);
    }
}

fn vectorData(chunk: c.duckdb_data_chunk, idx: usize) ?*anyopaque {
    return c.duckdb_vector_get_data(c.duckdb_data_chunk_get_vector(chunk, idx));
}

pub fn duckStringAt(values: []const DuckString, idx: usize) []const u8 {
    const len: usize = @intCast(c.duckdb_string_t_length(values[idx]));
    const ptr = c.duckdb_string_t_data(@constCast(&values[idx]));
    return ptr[0..len];
}

fn writeOut(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [512]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try writeOut(io, bytes);
}

fn printErr(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [4096]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try std.Io.File.stderr().writeStreamingAll(io, bytes);
}

test "escapes SQL string literals" {
    const allocator = std.testing.allocator;
    const escaped = try sqlStringLiteral(allocator, "a'b");
    defer allocator.free(escaped);
    try std.testing.expectEqualStrings("'a''b'", escaped);
}
