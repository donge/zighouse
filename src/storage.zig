const std = @import("std");
const schema = @import("schema");

pub const segment_rows = 64 * 1024;
pub const manifest_name = "manifest.zig-house";
pub const import_name = "import.zig-house";
pub const columns_dir_name = "columns";

pub fn initStore(io: std.Io, data_dir: []const u8) !void {
    // Legacy ClickBench schema removed; create an empty store directory.
    const empty_table = schema.Table{ .name = "hits", .columns = &.{} };
    try initStoreWithSchema(io, data_dir, empty_table);
}

pub fn initStoreWithSchema(io: std.Io, data_dir: []const u8, table: schema.Table) !void {
    const cwd = std.Io.Dir.cwd();
    try cwd.createDirPath(io, data_dir);

    var dir = try cwd.openDir(io, data_dir, .{});
    defer dir.close(io);

    try dir.createDirPath(io, columns_dir_name);

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.heap.smp_allocator);
    try text.print(std.heap.smp_allocator, "format=zighouse-native-v0\ntable={s}\nsegment_rows={d}\ncolumns={d}\n", .{ table.name, segment_rows, table.columns.len });
    for (table.columns, 0..) |column, i| {
        try text.print(std.heap.smp_allocator, "column={d}:{s}:{s}:cardinality={s}:storage={s}\n", .{ i, column.name, @tagName(column.ty), @tagName(column.cardinality), @tagName(column.storage) });
        try createColumnPlaceholders(io, dir, i, column);
    }
    try dir.writeFile(io, .{ .sub_path = manifest_name, .data = text.items });
}

pub fn ensureStore(io: std.Io, data_dir: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    var manifest = try dir.openFile(io, manifest_name, .{});
    manifest.close(io);
}

pub fn writeImportManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8, parquet_path: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);

    const text = try std.fmt.allocPrint(allocator, "source={s}\nstatus=duckdb-parquet-view\n", .{parquet_path});
    defer allocator.free(text);
    try dir.writeFile(io, .{ .sub_path = import_name, .data = text });
}

pub fn readImportManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    return try dir.readFileAlloc(io, import_name, allocator, .limited(64 * 1024));
}

pub fn readStoreManifest(io: std.Io, allocator: std.mem.Allocator, data_dir: []const u8) ![]u8 {
    var dir = try std.Io.Dir.cwd().openDir(io, data_dir, .{});
    defer dir.close(io);
    return try dir.readFileAlloc(io, manifest_name, allocator, .limited(256 * 1024));
}

fn createColumnPlaceholders(io: std.Io, dir: std.Io.Dir, index: usize, column: schema.Column) !void {
    var columns_dir = try dir.openDir(io, columns_dir_name, .{});
    defer columns_dir.close(io);

    var name_buf: [256]u8 = undefined;
    if (column.ty.isString()) {
        const offsets = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.offsets", .{ index, column.name });
        try touch(io, columns_dir, offsets);
        const bytes = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.bytes", .{ index, column.name });
        try touch(io, columns_dir, bytes);
        const hashes = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.hash64", .{ index, column.name });
        try touch(io, columns_dir, hashes);
    } else {
        const values = try std.fmt.bufPrint(&name_buf, "{d:0>3}_{s}.values", .{ index, column.name });
        try touch(io, columns_dir, values);
    }
}

fn touch(io: std.Io, dir: std.Io.Dir, name: []const u8) !void {
    var file = try dir.createFile(io, name, .{ .truncate = false });
    file.close(io);
}

test "segment rows are power of two" {
    try std.testing.expect((segment_rows & (segment_rows - 1)) == 0);
}

test "schema exposes fixed widths" {
    try std.testing.expectEqual(@as(?usize, 2), schema.ColumnType.int16.fixedWidth());
    try std.testing.expectEqual(@as(?usize, null), schema.ColumnType.text.fixedWidth());
}
