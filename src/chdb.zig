const std = @import("std");
const build_options = @import("build_options");
const duckdb = if (build_options.duckdb) @import("duckdb.zig") else @import("duckdb_stub.zig");
const storage = @import("storage.zig");

pub const default_python = "python3";

pub const ChDb = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    python: []const u8,
    io: std.Io,
    io_threaded: std.Io.Threaded,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, python: []const u8) ChDb {
        return .{
            .allocator = allocator,
            .data_dir = data_dir,
            .python = python,
            .io = io,
            .io_threaded = .init(allocator, .{}),
        };
    }

    pub fn deinit(self: *ChDb) void {
        self.io_threaded.deinit();
    }

    pub fn query(self: *ChDb, sql: []const u8) ![]u8 {
        const parquet_path = try storage.readImportSource(self.io, self.allocator, self.data_dir);
        defer self.allocator.free(parquet_path);
        const wrapped = try wrapSql(self.allocator, parquet_path, sql);
        defer self.allocator.free(wrapped);
        return self.runChDb(wrapped);
    }

    pub fn bench(self: *ChDb, queries_path: []const u8, range: duckdb.QueryRange) !void {
        try duckdb.benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn runQuery(self: *ChDb, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    fn runChDb(self: *ChDb, sql: []const u8) ![]u8 {
        const script =
            \\import chdb, sys
            \\sql = sys.argv[1]
            \\result = chdb.query(sql, 'CSVWithNames')
            \\sys.stdout.write(str(result))
        ;
        const result = std.process.run(self.allocator, self.io_threaded.io(), .{
            .argv = &.{ self.python, "-c", script, sql },
            .stdout_limit = .unlimited,
            .stderr_limit = .unlimited,
        }) catch |err| switch (err) {
            error.FileNotFound => return error.ChDbPythonNotFound,
            else => return err,
        };
        errdefer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        switch (result.term) {
            .exited => |code| if (code != 0) {
                try std.Io.File.stderr().writeStreamingAll(self.io, result.stderr);
                return error.ChDbCommandFailed;
            },
            else => return error.ChDbCommandFailed,
        }

        return result.stdout;
    }
};

fn wrapSql(allocator: std.mem.Allocator, parquet_path: []const u8, sql: []const u8) ![]u8 {
    const escaped_path = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(escaped_path);
    const adapted = try adaptSql(allocator, sql);
    defer allocator.free(adapted);
    const table_expr = try std.fmt.allocPrint(allocator, "file({s}, 'Parquet')", .{escaped_path});
    defer allocator.free(table_expr);
    const from_replacement = try std.fmt.allocPrint(allocator, "FROM {s}", .{table_expr});
    defer allocator.free(from_replacement);
    return replaceAll(allocator, adapted, "FROM hits", from_replacement);
}

pub fn adaptSql(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
    if (std.mem.eql(u8, trimmed, "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10")) {
        return allocator.dupe(u8, "SELECT UserID, toMinute(toDateTime(EventTime)) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10");
    }
    if (std.mem.eql(u8, trimmed, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000")) {
        return allocator.dupe(u8, "SELECT toStartOfMinute(toDateTime(EventTime)) AS M, COUNT(*) AS PageViews FROM hits WHERE toDate(EventDate) >= '2013-07-14' AND toDate(EventDate) <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY toStartOfMinute(toDateTime(EventTime)) ORDER BY toStartOfMinute(toDateTime(EventTime)) LIMIT 10 OFFSET 1000");
    }
    if (std.mem.eql(u8, trimmed, "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000")) {
        return allocator.dupe(u8, "SELECT toStartOfMinute(toDateTime(EventTime)) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND toDate(EventDate) >= '2013-07-14' AND toDate(EventDate) <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY toStartOfMinute(toDateTime(EventTime)) ORDER BY toStartOfMinute(toDateTime(EventTime)) LIMIT 10 OFFSET 1000");
    }

    var out = try replaceAll(allocator, trimmed, "REGEXP_REPLACE", "replaceRegexpOne");
    errdefer allocator.free(out);
    if (std.mem.indexOf(u8, out, "EventDate >= '")) |_| {
        const step1 = try replaceAll(allocator, out, "EventDate >= '", "toDate(EventDate) >= '");
        allocator.free(out);
        out = step1;
    }
    if (std.mem.indexOf(u8, out, "EventDate <= '")) |_| {
        const step2 = try replaceAll(allocator, out, "EventDate <= '", "toDate(EventDate) <= '");
        allocator.free(out);
        out = step2;
    }
    return out;
}

fn replaceAll(allocator: std.mem.Allocator, input: []const u8, needle: []const u8, replacement: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, input, pos, needle)) |idx| {
        try out.appendSlice(allocator, input[pos..idx]);
        try out.appendSlice(allocator, replacement);
        pos = idx + needle.len;
    }
    try out.appendSlice(allocator, input[pos..]);
    return out.toOwnedSlice(allocator);
}
