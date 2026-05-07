const std = @import("std");
const chdb = @import("chdb.zig");
const duckdb = @import("duckdb.zig");

pub const default_container = "sw_asdb";
pub const default_database = "default";
pub const default_password = "Sw@123456";

pub const Options = struct {
    container: []const u8 = default_container,
    database: []const u8 = default_database,
    password: []const u8 = default_password,
};

pub const ClickHouse = struct {
    allocator: std.mem.Allocator,
    options: Options,
    io: std.Io,
    io_threaded: std.Io.Threaded,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, _: []const u8, options: Options) ClickHouse {
        return .{
            .allocator = allocator,
            .options = options,
            .io = io,
            .io_threaded = .init(allocator, .{}),
        };
    }

    pub fn deinit(self: *ClickHouse) void {
        self.io_threaded.deinit();
    }

    pub fn query(self: *ClickHouse, sql: []const u8) ![]u8 {
        const adapted = try chdb.adaptSql(self.allocator, sql);
        defer self.allocator.free(adapted);
        return self.runClickHouse(adapted);
    }

    pub fn bench(self: *ClickHouse, queries_path: []const u8, range: duckdb.QueryRange) !void {
        try duckdb.benchWithRunner(self.allocator, self.io, queries_path, range, self);
    }

    pub fn runQuery(self: *ClickHouse, sql: []const u8) ![]u8 {
        return self.query(sql);
    }

    fn runClickHouse(self: *ClickHouse, sql: []const u8) ![]u8 {
        const result = std.process.run(self.allocator, self.io_threaded.io(), .{
            .argv = &.{
                "docker",
                "exec",
                self.options.container,
                "clickhouse-client",
                "--database",
                self.options.database,
                "--password",
                self.options.password,
                "--format",
                "CSVWithNames",
                "--query",
                sql,
            },
            .stdout_limit = .unlimited,
            .stderr_limit = .unlimited,
        }) catch |err| switch (err) {
            error.FileNotFound => return error.DockerNotFound,
            else => return err,
        };
        errdefer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        switch (result.term) {
            .exited => |code| if (code != 0) {
                try std.Io.File.stderr().writeStreamingAll(self.io, result.stderr);
                return error.ClickHouseCommandFailed;
            },
            else => return error.ClickHouseCommandFailed,
        }

        return result.stdout;
    }
};
