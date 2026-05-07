const std = @import("std");
const chdb = @import("chdb.zig");
const clickhouse = @import("clickhouse.zig");
const duckdb = @import("duckdb.zig");
const native = @import("native.zig");

pub const Kind = enum {
    duckdb,
    native,
    chdb,
    clickhouse,

    pub fn parse(value: []const u8) !Kind {
        if (std.mem.eql(u8, value, "duckdb")) return .duckdb;
        if (std.mem.eql(u8, value, "native")) return .native;
        if (std.mem.eql(u8, value, "chdb")) return .chdb;
        if (std.mem.eql(u8, value, "clickhouse")) return .clickhouse;
        return error.UnknownBackend;
    }
};

pub const Options = struct {
    kind: Kind = .duckdb,

    chdb_python: []const u8 = chdb.default_python,
    clickhouse_container: []const u8 = clickhouse.default_container,
    clickhouse_database: []const u8 = clickhouse.default_database,
    clickhouse_password: []const u8 = clickhouse.default_password,

    pub fn fromEnv(environ_map: *const std.process.Environ.Map) Options {
        var options: Options = .{};
        if (environ_map.get("ZIGHOUSE_CHDB_PYTHON")) |value| options.chdb_python = value;
        if (environ_map.get("ZIGHOUSE_CLICKHOUSE_CONTAINER")) |value| options.clickhouse_container = value;
        if (environ_map.get("ZIGHOUSE_CLICKHOUSE_DATABASE")) |value| options.clickhouse_database = value;
        if (environ_map.get("ZIGHOUSE_CLICKHOUSE_PASSWORD")) |value| options.clickhouse_password = value;
        return options;
    }
};

pub const Backend = union(Kind) {
    duckdb: duckdb.DuckDb,
    native: native.Native,
    chdb: chdb.ChDb,
    clickhouse: clickhouse.ClickHouse,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, options: Options) Backend {
        return switch (options.kind) {
            .duckdb => .{ .duckdb = duckdb.DuckDb.init(allocator, io, data_dir) },
            .native => .{ .native = native.Native.init(allocator, io, data_dir) },
            .chdb => .{ .chdb = chdb.ChDb.init(allocator, io, data_dir, options.chdb_python) },
            .clickhouse => .{ .clickhouse = clickhouse.ClickHouse.init(allocator, io, data_dir, .{
                .container = options.clickhouse_container,
                .database = options.clickhouse_database,
                .password = options.clickhouse_password,
            }) },
        };
    }

    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            .duckdb => |*b| b.deinit(),
            .native => |*b| b.deinit(),
            .chdb => |*b| b.deinit(),
            .clickhouse => |*b| b.deinit(),
        }
    }

    pub fn importParquet(self: *Backend, parquet_path: []const u8) !void {
        switch (self.*) {
            .duckdb => |*b| try b.importParquet(parquet_path),
            .native => |*b| try b.importParquet(parquet_path),
            .chdb => return error.UnsupportedBackendImport,
            .clickhouse => return error.UnsupportedBackendImport,
        }
    }

    pub fn query(self: *Backend, sql: []const u8) ![]u8 {
        return switch (self.*) {
            .duckdb => |*b| b.query(sql),
            .native => |*b| b.query(sql),
            .chdb => |*b| b.query(sql),
            .clickhouse => |*b| b.query(sql),
        };
    }

    pub fn bench(self: *Backend, queries_path: []const u8, range: duckdb.QueryRange) !void {
        switch (self.*) {
            .duckdb => |*b| try b.bench(queries_path, range),
            .native => |*b| try b.bench(queries_path, range),
            .chdb => |*b| try b.bench(queries_path, range),
            .clickhouse => |*b| try b.bench(queries_path, range),
        }
    }
};
