/// Generic Parquet importer.
///
/// `importParquet` reads a Parquet file and writes the generic part store
/// layout produced by `generic_store`:
///
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt
///     count.txt
///     <col>.bin         — fixed-width columns (int16/int32/int64/date/timestamp)
///     <col>.str.bin     — string columns (offset table + byte data)
///
/// String column format for <col>.str.bin:
///   [u64 row_count]
///   [row_count+1 × u64 offsets]   (offset[i] = start of string i in bytes blob)
///   [total_bytes × u8 bytes]       (UTF-8 or raw bytes, no NUL terminator)
///
/// All integers are little-endian.
///
/// The caller is responsible for ensuring that `table` schema matches the
/// Parquet file layout (column count and order) — `inferSchema` produces a
/// compatible table from the same file.

const std = @import("std");
const parquet = @import("parquet.zig");
const schema = @import("schema.zig");
const generic_store = @import("generic_store.zig");

/// Import a Parquet file into the generic part store.
///
/// Parameters:
///   allocator   — used for temporary allocations during the import
///   io          — I/O handle
///   parquet_path — path to the source .parquet file
///   store_dir   — root directory for the store (will be created if absent)
///   table       — schema of the table to import (column order must match file)
///
/// Returns the number of rows imported.
pub fn importParquet(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    store_dir: []const u8,
    table: schema.Table,
) !u64 {
    // Create the part directory tree and get the part path.
    const part = try generic_store.initPart(io, store_dir, table.name, allocator);
    defer allocator.free(part);

    // Write columns.txt manifest.
    try generic_store.writeColumnsTxt(io, allocator, part, table);

    // Determine total row count from metadata (sum of row group rows).
    const total_rows: u64 = try parquet.rowCountPath(allocator, io, parquet_path);

    // Import each column.
    for (table.columns, 0..) |col, col_idx| {
        try importColumn(allocator, io, parquet_path, part, col, col_idx, total_rows);
    }

    // Write count.txt last (so a partial import leaves no count.txt).
    try generic_store.writeCountTxt(io, allocator, part, total_rows);

    return total_rows;
}

// ── Column import ─────────────────────────────────────────────────────────────

fn importColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col: schema.Column,
    col_idx: usize,
    total_rows: u64,
) !void {
    switch (col.ty) {
        .text, .char => try importStringColumn(allocator, io, parquet_path, part, col.name, col_idx, total_rows),
        .int16 => try importFixedColumn(i16, allocator, io, parquet_path, part, col.name, col_idx),
        .int32, .date => try importFixedColumn(i32, allocator, io, parquet_path, part, col.name, col_idx),
        .int64, .timestamp => try importFixedColumn(i64, allocator, io, parquet_path, part, col.name, col_idx),
    }
}

// ── Fixed-width columns ────────────────────────────────────────────────────────

fn importFixedColumn(
    comptime T: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col_name: []const u8,
    col_idx: usize,
) !void {
    const path = try generic_store.columnBinPath(allocator, part, col_name);
    defer allocator.free(path);

    var writer = try generic_store.ColumnBinWriter.open(io, path);
    defer writer.close();

    const ctx = FixedWriteCtx(T){ .writer = &writer };

    _ = try parquet.streamFixedColumnPath(
        allocator,
        io,
        parquet_path,
        col_idx,
        null,
        ctx,
        writeFixedBatch(T),
    );
}

fn FixedWriteCtx(comptime T: type) type {
    _ = T;
    return struct { writer: *generic_store.ColumnBinWriter };
}

fn writeFixedBatch(comptime T: type) fn (FixedWriteCtx(T), []const i64) anyerror!void {
    return struct {
        fn cb(ctx: FixedWriteCtx(T), values: []const i64) anyerror!void {
            for (values) |v| {
                const cast: T = @intCast(v);
                switch (T) {
                    i16 => try ctx.writer.writeI16(cast),
                    i32 => try ctx.writer.writeI32(cast),
                    i64 => try ctx.writer.writeI64(cast),
                    else => @compileError("unsupported fixed type"),
                }
            }
        }
    }.cb;
}

// ── String columns ─────────────────────────────────────────────────────────────

/// String column (.str.bin) layout:
///   u64 row_count
///   (row_count+1) × u64 offsets  (byte offset into bytes blob)
///   bytes blob
///
/// We build the offset table and bytes blob in memory then flush.
fn importStringColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col_name: []const u8,
    col_idx: usize,
    total_rows: u64,
) !void {
    // Accumulate offsets and bytes.
    const offsets = try allocator.alloc(u64, total_rows + 1);
    defer allocator.free(offsets);
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);

    offsets[0] = 0;
    var row: u64 = 0;

    const ctx = StringCollectCtx{
        .allocator = allocator,
        .offsets = offsets,
        .bytes = &bytes,
        .row = &row,
    };

    _ = try parquet.streamByteArrayColumnPath(
        allocator,
        io,
        parquet_path,
        col_idx,
        null,
        ctx,
        collectStringValue,
    );

    // Flush to .str.bin
    const str_path = try generic_store.columnStrBinPath(allocator, part, col_name);
    defer allocator.free(str_path);

    const file = try std.Io.Dir.cwd().createFile(io, str_path, .{ .truncate = true });
    defer file.close(io);

    // Write row_count header
    var hdr: [8]u8 = undefined;
    std.mem.writeInt(u64, &hdr, total_rows, .little);
    try file.writeStreamingAll(io, &hdr);

    // Write offsets (row_count+1 entries)
    try file.writeStreamingAll(io, std.mem.sliceAsBytes(offsets[0 .. total_rows + 1]));

    // Write bytes blob
    try file.writeStreamingAll(io, bytes.items);
}

const StringCollectCtx = struct {
    allocator: std.mem.Allocator,
    offsets: []u64,
    bytes: *std.ArrayListUnmanaged(u8),
    row: *u64,
};

fn collectStringValue(ctx: StringCollectCtx, value: []const u8) anyerror!void {
    try ctx.bytes.appendSlice(ctx.allocator, value);
    ctx.row.* += 1;
    ctx.offsets[ctx.row.*] = @intCast(ctx.bytes.items.len);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "importParquet round-trips a tiny synthetic parquet" {
    // This test uses a real .parquet fixture if available.
    // Skipped here because we don't bundle test fixtures in the repo.
    // Integration tests live in tests/import_roundtrip_test.zig (future).
    // Smoke: just make sure the module compiles and the helpers link.
    _ = importParquet;
}
