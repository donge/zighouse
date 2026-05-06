const std = @import("std");
const duckdb = @import("duckdb.zig");
const native = @import("native.zig");

pub const Kind = enum {
    duckdb,
    native,

    pub fn parse(value: []const u8) !Kind {
        if (std.mem.eql(u8, value, "duckdb")) return .duckdb;
        if (std.mem.eql(u8, value, "native")) return .native;
        return error.UnknownBackend;
    }
};

pub const Options = struct {
    kind: Kind = .duckdb,
};

pub const Backend = union(Kind) {
    duckdb: duckdb.DuckDb,
    native: native.Native,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, options: Options) Backend {
        return switch (options.kind) {
            .duckdb => .{ .duckdb = duckdb.DuckDb.init(allocator, io, data_dir) },
            .native => .{ .native = native.Native.init(allocator, io, data_dir) },
        };
    }

    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            .duckdb => |*b| b.deinit(),
            .native => |*b| b.deinit(),
        }
    }

    pub fn importParquet(self: *Backend, parquet_path: []const u8) !void {
        switch (self.*) {
            .duckdb => |*b| try b.importParquet(parquet_path),
            .native => |*b| try b.importParquet(parquet_path),
        }
    }

    pub fn query(self: *Backend, sql: []const u8) ![]u8 {
        return switch (self.*) {
            .duckdb => |*b| b.query(sql),
            .native => |*b| b.query(sql),
        };
    }

    pub fn bench(self: *Backend, queries_path: []const u8, range: duckdb.QueryRange) !void {
        switch (self.*) {
            .duckdb => |*b| try b.bench(queries_path, range),
            .native => |*b| try b.bench(queries_path, range),
        }
    }
};
