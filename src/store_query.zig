//! Store-format detection for the CLI.
//!
//! `zighouse query` and `zighouse bench` must work on both stores:
//!   - generic Store V2 parts written by `import-parquet --format=generic`
//!   - ClickHouse-compatible parts written by `--format=ch`/`ch-compact`
//!     and by streaming INSERTs through `zighouse serve`
//!
//! Both bridges expose the same `SourceIface`, so the planner and pipeline
//! are identical; this module only selects the physical reader.

const std = @import("std");
const schema = @import("schema");
const core = @import("core");
const generic_store_bridge = @import("generic_store_bridge");
const part_scan_bridge = @import("part_scan_bridge");
const part_scanner = @import("part_scanner");
const schema_persist = @import("schema_persist");
const schema_config = @import("schema_config");

pub const Kind = enum {
    generic,
    ch_mergetree,
};

pub const LoadedTable = struct {
    config: schema_config.SchemaConfig,
    table: schema.Table,

    pub fn deinit(self: *LoadedTable) void {
        self.config.deinit();
    }
};

/// Load a table schema from `<store>/<table>/schema.json`.  Both import paths
/// and the streaming server persist this file, so file and stream data share
/// one schema description.
pub fn loadTable(
    alloc: std.mem.Allocator,
    io: std.Io,
    store_dir: []const u8,
    table_name: []const u8,
) !LoadedTable {
    const table_dir = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ store_dir, table_name });
    defer alloc.free(table_dir);
    var config = (try schema_persist.loadFromDir(alloc, io, table_dir)) orelse
        return error.SchemaNotFound;
    errdefer config.deinit();
    if (config.tables.len == 0) return error.SchemaNotFound;
    return .{ .config = config, .table = config.tables[0].table };
}

pub const StoreSource = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    kind: Kind,
    table: schema.Table,
    generic: ?generic_store_bridge.GenericStoreBridge = null,
    part: ?part_scan_bridge.PartScanBridge = null,
    part_list: ?part_scanner.PartList = null,
    generic_part_dir: ?[]u8 = null,

    pub fn open(
        alloc: std.mem.Allocator,
        io: std.Io,
        store_dir: []const u8,
        table_name: []const u8,
        table: schema.Table,
        pruned_cols: []const []const u8,
    ) !StoreSource {
        const kind = try detectKind(alloc, io, store_dir, table_name);
        var result = StoreSource{
            .alloc = alloc,
            .io = io,
            .kind = kind,
            .table = table,
        };
        errdefer result.deinit();

        switch (kind) {
            .generic => {
                const part_dir = try std.fmt.allocPrint(
                    alloc,
                    "{s}/{s}/parts/all_1_1_0",
                    .{ store_dir, table_name },
                );
                result.generic_part_dir = part_dir;
                result.generic = try generic_store_bridge.GenericStoreBridge.init(
                    alloc,
                    io,
                    part_dir,
                    table,
                    pruned_cols,
                );
            },
            .ch_mergetree => {
                const parts_path = try std.fmt.allocPrint(
                    alloc,
                    "{s}/{s}/parts",
                    .{ store_dir, table_name },
                );
                defer alloc.free(parts_path);
                result.part_list = try part_scanner.scanPath(alloc, io, parts_path);
                result.part = try part_scan_bridge.PartScanBridge.init(
                    alloc,
                    io,
                    table,
                    result.part_list.?.dirs(),
                    pruned_cols,
                );
            },
        }
        return result;
    }

    pub fn source(self: *StoreSource) core.exec.pipeline.SourceIface {
        return switch (self.kind) {
            .generic => self.generic.?.source(),
            .ch_mergetree => self.part.?.source(),
        };
    }

    pub fn resetForNewQuery(self: *StoreSource, pruned_cols: []const []const u8) void {
        switch (self.kind) {
            .generic => self.generic.?.resetForNewQuery(pruned_cols),
            .ch_mergetree => self.part.?.resetForNewQuery(pruned_cols),
        }
    }

    pub fn deinit(self: *StoreSource) void {
        if (self.part) |*p| p.deinit();
        if (self.part_list) |*pl| pl.deinit();
        if (self.generic) |*g| g.deinit();
        if (self.generic_part_dir) |p| self.alloc.free(p);
        self.part = null;
        self.part_list = null;
        self.generic = null;
        self.generic_part_dir = null;
    }
};

/// Read `<store>/<table>/parts/all_1_1_0/catalog.zig-house`, then fall back
/// to inspecting the first part directory.  A streamed store has no manifest.
pub fn detectKind(
    alloc: std.mem.Allocator,
    io: std.Io,
    store_dir: []const u8,
    table_name: []const u8,
) !Kind {
    const parts_path = try std.fmt.allocPrint(
        alloc,
        "{s}/{s}/parts",
        .{ store_dir, table_name },
    );
    defer alloc.free(parts_path);

    const manifest_path = try std.fmt.allocPrint(
        alloc,
        "{s}/all_1_1_0/catalog.zig-house",
        .{parts_path},
    );
    defer alloc.free(manifest_path);
    if (readSmallFile(alloc, io, manifest_path, 4096)) |content| {
        defer alloc.free(content);
        if (std.mem.indexOf(u8, content, "part_format=ch_mergetree") != null) {
            return .ch_mergetree;
        }
        if (std.mem.indexOf(u8, content, "part_format=generic") != null) {
            return .generic;
        }
    } else |_| {}

    var dir = openDir(io, parts_path, .{ .iterate = true }) catch return .generic;
    defer dir.close(io);
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .directory and entry.kind != .sym_link) continue;
        if (std.mem.startsWith(u8, entry.name, "tmp_")) continue;
        const part_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ parts_path, entry.name });
        defer alloc.free(part_path);

        if (try exists(alloc, io, part_path, "checksums.txt") or
            try exists(alloc, io, part_path, "data.cmrk4"))
        {
            return .ch_mergetree;
        }
    }
    return .generic;
}

fn exists(
    alloc: std.mem.Allocator,
    io: std.Io,
    part_path: []const u8,
    name: []const u8,
) !bool {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ part_path, name });
    defer alloc.free(path);
    var file = openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    file.close(io);
    return true;
}

fn openFile(io: std.Io, path: []const u8, options: std.Io.Dir.OpenFileOptions) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, options)
    else
        std.Io.Dir.cwd().openFile(io, path, options);
}

fn openDir(io: std.Io, path: []const u8, options: std.Io.Dir.OpenOptions) !std.Io.Dir {
    return if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openDirAbsolute(io, path, options)
    else
        std.Io.Dir.cwd().openDir(io, path, options);
}

fn readSmallFile(alloc: std.mem.Allocator, io: std.Io, path: []const u8, max: usize) ![]u8 {
    var file = try openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size > max) return error.FileTooBig;
    const buf = try alloc.alloc(u8, @intCast(stat.size));
    errdefer alloc.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);
    return buf;
}

test "detect generic store format" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/zighouse_store_query_generic";
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};

    const part_dir = root ++ "/hits/parts/all_1_1_0";
    try std.Io.Dir.cwd().createDirPath(io, part_dir);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = part_dir ++ "/active.bin",
        .data = "ZHSTV2",
    });
    try std.testing.expectEqual(
        Kind.generic,
        try detectKind(allocator, io, root, "hits"),
    );
}

test "detect streamed compact part format" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/zighouse_store_query_ch";
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};

    const part_dir = root ++ "/hits/parts/all_7_7_0";
    try std.Io.Dir.cwd().createDirPath(io, part_dir);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = part_dir ++ "/data.cmrk4",
        .data = "mark",
    });
    try std.testing.expectEqual(
        Kind.ch_mergetree,
        try detectKind(allocator, io, root, "hits"),
    );
}
