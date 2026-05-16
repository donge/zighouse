/// Generic ClickHouse-style part store layout.
///
/// Directory structure:
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt     — tab-separated "name\ttype_tag\n" lines
///     count.txt       — row count as a decimal string
///     <col>.bin       — raw fixed-width column data (little-endian)
///                       size = row_count * sizeof(type)
///     <col>.str.bin   — variable-length string column (future; not yet written)
///
/// The store layout is deliberately simple: no compression, no marks, no
/// primary index.  The goal is correct end-to-end import + query without the
/// complexity of a full MergeTree layout.
///
/// Supported column types for writing:
///   int16, int32, int64, date (i32), timestamp (i64)
/// String / text columns are recorded in columns.txt but no .bin is written
/// in this revision (marked "text" in columns.txt; generic_executor will
/// skip them until PR-H4 adds string writing).

const std = @import("std");
const schema = @import("schema");

pub const part_dir_name = "all_1_1_0";
pub const columns_txt_name = "columns.txt";
pub const count_txt_name = "count.txt";

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

/// Writer: streams fixed-width column values directly to a .bin file.
pub const ColumnBinWriter = struct {
    io: std.Io,
    file: std.Io.File,
    count: u64,

    pub fn open(io: std.Io, path: []const u8) !ColumnBinWriter {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        return .{ .io = io, .file = file, .count = 0 };
    }

    pub fn writeI8(self: *ColumnBinWriter, value: i8) !void {
        const buf = [1]u8{@bitCast(value)};
        try self.file.writeStreamingAll(self.io, &buf);
        self.count += 1;
    }

    pub fn writeI16(self: *ColumnBinWriter, value: i16) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(i16, &buf, value, .little);
        try self.file.writeStreamingAll(self.io, &buf);
        self.count += 1;
    }

    pub fn writeI32(self: *ColumnBinWriter, value: i32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &buf, value, .little);
        try self.file.writeStreamingAll(self.io, &buf);
        self.count += 1;
    }

    pub fn writeI64(self: *ColumnBinWriter, value: i64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &buf, value, .little);
        try self.file.writeStreamingAll(self.io, &buf);
        self.count += 1;
    }

    pub fn close(self: *ColumnBinWriter) void {
        self.file.close(self.io);
    }
};

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

/// Read count.txt from a part directory.
pub fn readCountTxt(io: std.Io, allocator: std.mem.Allocator, part: []const u8) !u64 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ part, count_txt_name });
    defer allocator.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var buf: [32]u8 = undefined;
    const n = try file.readPositionalAll(io, &buf, 0);
    const trimmed = std.mem.trimRight(u8, buf[0..n], " \t\r\n");
    return std.fmt.parseInt(u64, trimmed, 10);
}

/// Memory-map a fixed-width column .bin file as a typed slice.
/// The file must be mmap-able (regular file, non-empty).
pub fn MapResult(comptime T: type) type {
    return struct {
        values: []const T,
        ptr: []align(std.mem.page_size) u8,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            _ = allocator;
            std.posix.munmap(self.ptr);
        }
    };
}

pub fn mmapColumn(comptime T: type, io: std.Io, allocator: std.mem.Allocator, part: []const u8, col_name: []const u8) !MapResult(T) {
    const path = try columnBinPath(allocator, part, col_name);
    defer allocator.free(path);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size == 0) return error.EmptyColumn;
    const ptr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
    const values = std.mem.bytesAsSlice(T, ptr[0..stat.size]);
    return .{ .values = values, .ptr = ptr };
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

test "ColumnBinWriter writes and can be reopened" {
    const io = std.testing.io;

    // Write to a temp file
    const path = "/tmp/zig_test_col_writer.bin";
    {
        var w = try ColumnBinWriter.open(io, path);
        try w.writeI32(42);
        try w.writeI32(-1);
        try w.writeI32(100);
        w.close();
        try std.testing.expectEqual(@as(u64, 3), w.count);
    }
    // Read back
    {
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        var buf: [12]u8 = undefined;
        const n = try file.readPositionalAll(io, &buf, 0);
        try std.testing.expectEqual(@as(usize, 12), n);
        try std.testing.expectEqual(@as(i32, 42), std.mem.readInt(i32, buf[0..4], .little));
        try std.testing.expectEqual(@as(i32, -1), std.mem.readInt(i32, buf[4..8], .little));
        try std.testing.expectEqual(@as(i32, 100), std.mem.readInt(i32, buf[8..12], .little));
    }
}
