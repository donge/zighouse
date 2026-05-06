const std = @import("std");
const storage = @import("storage.zig");

const duckdb_exe = "/opt/homebrew/bin/duckdb";

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
        const wrapped = try wrapSql(self.allocator, parquet_path, sql);
        defer self.allocator.free(wrapped);
        return self.runDuckDb(&.{ duckdb_exe, "-csv", "-c", wrapped });
    }

    pub fn bench(self: *DuckDb, queries_path: []const u8, range: QueryRange) !void {
        try benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn runQuery(self: *DuckDb, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    pub fn runRawSql(self: *DuckDb, sql: []const u8) ![]u8 {
        return self.runDuckDb(&.{ duckdb_exe, "-csv", "-c", sql });
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

fn wrapSql(allocator: std.mem.Allocator, parquet_path: []const u8, sql: []const u8) ![]u8 {
    const escaped_path = try sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(escaped_path);
    return std.fmt.allocPrint(allocator,
        \\CREATE OR REPLACE VIEW hits AS
        \\SELECT * REPLACE (
        \\    make_date(EventDate) AS EventDate,
        \\    epoch_ms(EventTime * 1000) AS EventTime,
        \\    epoch_ms(ClientEventTime * 1000) AS ClientEventTime,
        \\    epoch_ms(LocalEventTime * 1000) AS LocalEventTime)
        \\FROM read_parquet({s}, binary_as_string=True);
        \\{s}
    , .{ escaped_path, sql });
}

pub fn benchWithRunner(allocator: std.mem.Allocator, io: std.Io, queries_path: []const u8, range: QueryRange, runner: anytype) !void {
    const queries = try std.Io.Dir.cwd().readFileAlloc(io, queries_path, allocator, .limited(512 * 1024));
    defer allocator.free(queries);

    var query_num: usize = 1;
    var line_it = std.mem.splitScalar(u8, queries, '\n');
    while (line_it.next()) |raw_line| : (query_num += 1) {
        const query_text = std.mem.trim(u8, raw_line, " \t\r");
        if (query_text.len == 0) continue;
        if (!range.contains(query_num)) continue;

        try writeOut(io, "[");
        for (0..3) |i| {
            const started = std.Io.Clock.Timestamp.now(io, .awake);
            const result = runner.runQuery(query_text) catch null;
            const elapsed = if (result) |output| blk: {
                allocator.free(output);
                const ended = std.Io.Clock.Timestamp.now(io, .awake);
                break :blk started.durationTo(ended).raw.nanoseconds;
            } else null;

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
    }
}

pub fn sqlStringLiteral(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '\'');
    for (value) |c| {
        if (c == '\'') try out.append(allocator, '\'');
        try out.append(allocator, c);
    }
    try out.append(allocator, '\'');
    return out.toOwnedSlice(allocator);
}

fn writeOut(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

test "escapes SQL string literals" {
    const allocator = std.testing.allocator;
    const escaped = try sqlStringLiteral(allocator, "a'b");
    defer allocator.free(escaped);
    try std.testing.expectEqualStrings("'a''b'", escaped);
}
