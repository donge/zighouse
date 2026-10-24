/// Generic ClickHouse-style part store layout.
///
/// Directory structure:
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt     — tab-separated "name\ttype_tag\n" lines
///     count.txt       — row count as a decimal string
///     <col>.bin       — Store V2 fixed-width block file
///     <col>.str.bin   — variable-length string column (future; not yet written)
///
const std = @import("std");
const schema = @import("schema");
pub const v2 = @import("store_v2");

pub const part_dir_name = "all_1_1_0";
pub const columns_txt_name = "columns.txt";
pub const count_txt_name = "count.txt";
pub const manifest_bin_name = "manifest.bin";
const manifest_magic = "ZHMANV2\x00";

/// Return the part directory path for a given store root and table name.
/// Caller owns the result.
pub fn partDir(allocator: std.mem.Allocator, store_dir: []const u8, table_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}/parts/{s}", .{ store_dir, table_name, part_dir_name });
}

/// Return the path for a fixed-width column binary file.
pub fn columnBinPath(allocator: std.mem.Allocator, part: []const u8, col_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}.bin", .{ part, col_name });
}

/// Return the path for a string column binary file.
pub fn columnStrBinPath(allocator: std.mem.Allocator, part: []const u8, col_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}.str.bin", .{ part, col_name });
}

/// Write the columns.txt manifest for a part.
pub fn writeColumnsTxt(
    io: std.Io,
    allocator: std.mem.Allocator,
    part: []const u8,
    table: schema.Table,
) !void {
    var text: std.ArrayListUnmanaged(u8) = .empty;
    defer text.deinit(allocator);
    for (table.columns) |col| {
        try text.print(allocator, "{s}\t{s}\n", .{ col.name, @tagName(col.ty) });
    }
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ part, columns_txt_name });
    defer allocator.free(path);
    const cwd = std.Io.Dir.cwd();
    try cwd.writeFile(io, .{ .sub_path = path, .data = text.items });
}

/// Write count.txt for a part.
pub fn writeCountTxt(
    io: std.Io,
    allocator: std.mem.Allocator,
    part: []const u8,
    row_count: u64,
) !void {
    const text = try std.fmt.allocPrint(allocator, "{d}\n", .{row_count});
    defer allocator.free(text);
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ part, count_txt_name });
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = text });

    var manifest: [32]u8 = @as([32]u8, @splat(0));
    @memcpy(manifest[0..manifest_magic.len], manifest_magic);
    std.mem.writeInt(u16, manifest[8..10], 2, .little);
    std.mem.writeInt(u64, manifest[16..24], row_count, .little);
    const manifest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ part, manifest_bin_name });
    defer allocator.free(manifest_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = manifest_path, .data = &manifest });
}

/// Create the part directory tree (store_dir/table_name/parts/all_1_1_0/).
pub fn initPart(io: std.Io, store_dir: []const u8, table_name: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const cwd = std.Io.Dir.cwd();
    // Ensure base paths exist
    try cwd.createDirPath(io, store_dir);
    const table_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ store_dir, table_name });
    defer allocator.free(table_path);
    try cwd.createDirPath(io, table_path);
    const parts_path = try std.fmt.allocPrint(allocator, "{s}/parts", .{table_path});
    defer allocator.free(parts_path);
    try cwd.createDirPath(io, parts_path);
    const part = try partDir(allocator, store_dir, table_name);
    try cwd.createDirPath(io, part);
    return part; // caller owns
}

/// Read the Store V2 binary manifest from a part directory.
pub fn readCountTxt(io: std.Io, allocator: std.mem.Allocator, part: []const u8) !u64 {
    const manifest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ part, manifest_bin_name });
    defer allocator.free(manifest_path);
    if (std.Io.Dir.cwd().openFile(io, manifest_path, .{})) |manifest_file_value| {
        var manifest_file = manifest_file_value;
        defer manifest_file.close(io);
        var manifest: [32]u8 = undefined;
        const n = try manifest_file.readPositionalAll(io, &manifest, 0);
        if (n != manifest.len or
            !std.mem.eql(u8, manifest[0..manifest_magic.len], manifest_magic) or
            std.mem.readInt(u16, manifest[8..10], .little) != 2)
            return error.InvalidManifest;
        return std.mem.readInt(u64, manifest[16..24], .little);
    } else |err| return err;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "partDir returns expected path" {
    const allocator = std.testing.allocator;
    const p = try partDir(allocator, "/tmp/store", "orders");
    defer allocator.free(p);
    try std.testing.expectEqualStrings("/tmp/store/orders/parts/all_1_1_0", p);
}

test "columnBinPath returns expected path" {
    const allocator = std.testing.allocator;
    const p = try columnBinPath(allocator, "/tmp/store/orders/parts/all_1_1_0", "price");
    defer allocator.free(p);
    try std.testing.expectEqualStrings("/tmp/store/orders/parts/all_1_1_0/price.bin", p);
}

test "Store V2 is exposed through generic_store" {
    const io = std.testing.io;

    // Write to a temp file
    const path = "/tmp/zig_test_col_writer.bin";
    {
        var w = try v2.Writer.open(std.testing.allocator, io, path, .int32, 3, 2);
        defer w.deinit();
        try w.writeI32(42);
        try w.writeI32(-1);
        try w.writeI32(100);
        try w.finish();
    }
    const mapped = try v2.MappedColumn.open(std.testing.allocator, io, path, .int32, 3);
    defer mapped.deinit(std.testing.allocator);
    const bytes = try mapped.decodeRange(std.testing.allocator, 0, 3);
    defer std.testing.allocator.free(bytes);
    try std.testing.expectEqual(@as(i32, 42), std.mem.readInt(i32, bytes[0..4], .little));
    try std.testing.expectEqual(@as(i32, -1), std.mem.readInt(i32, bytes[4..8], .little));
    try std.testing.expectEqual(@as(i32, 100), std.mem.readInt(i32, bytes[8..12], .little));
}
