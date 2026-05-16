/// Write a complete ClickHouse MergeTree part directory.
///
/// Part layout written (`all_1_1_0/`):
///   columns.txt
///   count.txt
///   primary.idx
///   checksums.txt
///   <col>.bin          — LZ4-compressed column data
///   <col>.cmrk2        — compressed mark file (LZ4 block, offset table)
///
/// Phase-1 supported column types: Int16, Int32, Int64, Date, DateTime, String.
///
/// Usage:
///   var part = try Part.open(io, allocator, "/store/default/hits/parts/all_1_1_0", schema, "WatchID");
///   for rows:
///     try part.appendRow(row_values);
///   try part.finish();
///   part.deinit();

const std = @import("std");
// schema and types: use named module when available (standalone tests),
// falling back handled via build.zig addImport("schema", ...).
const schema = @import("schema");
const types = @import("types");
const columns_txt = @import("columns_txt.zig");
const count_txt = @import("count_txt.zig");
const marks = @import("marks.zig");
const primary_idx = @import("primary_idx.zig");
const checksums = @import("checksums.zig");
const string_codec = @import("string_codec.zig");
const cityhash = @import("cityhash.zig");
const block = @import("block.zig");

pub const GRANULE_SIZE: u64 = 8192;
/// Maximum uncompressed bytes per LZ4 block (~1 MiB, matching CH default).
pub const MAX_BLOCK_BYTES: usize = 1 * 1024 * 1024;

/// A single column value for one row.
pub const Value = union(enum) {
    i16: i16,
    i32: i32,
    i64: i64,
    str: []const u8,
};

/// State for writing one column's .bin and .mrk2 files.
///
/// Streaming design: compressed data is written directly to `bin_file`.
/// Uncompressed bytes are buffered in `unc_buf` (RAM) so that CityHash128
/// can be computed at `Part.finish()` without a second disk pass.
/// `unc_buf` is freed immediately after hashing to reclaim memory column-by-column.
///
/// For String columns, CH wide-part format uses two sub-streams:
///   - data stream  ({col}.bin / {col}.mrk2):  raw UTF-8 bytes, no length prefix
///   - size stream  ({col}.size.bin / {col}.size.mrk2): u64 LE lengths, one per row
/// `size_writer` points to the companion size-stream writer (non-null only for String cols).
const ColumnWriter = struct {
    /// Accumulated uncompressed bytes for the current LZ4 block (at most MAX_BLOCK_BYTES).
    buf: std.ArrayList(u8),
    /// All uncompressed bytes written so far (used to compute uncompressed_hash at finish).
    /// Freed immediately after hashing in Part.finish() to reclaim memory column-by-column.
    unc_buf: std.ArrayList(u8),
    /// Mark entries accumulated so far.
    mark_list: std.ArrayList(marks.Mark),
    /// Byte offset in .bin where the current compressed block starts.
    bin_offset: u64,
    /// Number of rows in the current granule.
    granule_rows: u64,
    /// Total rows written to this column.
    total_rows: u64,
    /// Total uncompressed bytes written (for checksums).
    uncompressed_size: u64,
    /// Column schema info.
    col: schema.Column,
    allocator: std.mem.Allocator,
    io: std.Io,
    /// Open file handle for the .bin output (write-only, created by Part.open).
    bin_file: std.Io.File,
    /// Absolute path of the .bin file (owned, freed in deinit).
    bin_path: []u8,
    /// For String columns: pointer to companion size-stream writer.
    /// Owned by Part.column_size_writers; do NOT call deinit() from here.
    size_writer: ?*ColumnWriter = null,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        col: schema.Column,
        bin_file: std.Io.File,
        bin_path: []u8,
    ) ColumnWriter {
        return .{
            .buf = .empty,
            .unc_buf = .empty,
            .mark_list = .empty,
            .bin_offset = 0,
            .granule_rows = 0,
            .total_rows = 0,
            .uncompressed_size = 0,
            .col = col,
            .allocator = allocator,
            .io = io,
            .bin_file = bin_file,
            .bin_path = bin_path,
            .size_writer = null,
        };
    }

    fn deinit(self: *ColumnWriter) void {
        self.buf.deinit(self.allocator);
        self.unc_buf.deinit(self.allocator);
        self.mark_list.deinit(self.allocator);
        self.bin_file.close(self.io);
        self.allocator.free(self.bin_path);
    }

    /// Append raw bytes to the data stream (used by fixed-width and String data sub-stream).
    /// Does NOT increment granule_rows — callers do so explicitly.
    fn appendBytes(self: *ColumnWriter, bytes: []const u8) !void {
        try self.buf.appendSlice(self.allocator, bytes);
        try self.unc_buf.appendSlice(self.allocator, bytes);
        self.uncompressed_size += bytes.len;
    }

    fn appendFixed(self: *ColumnWriter, bytes: []const u8) !void {
        try self.appendBytes(bytes);
        self.granule_rows += 1;
        self.total_rows += 1;
        if (self.granule_rows >= GRANULE_SIZE or self.buf.items.len >= MAX_BLOCK_BYTES) {
            try self.flushBlock();
        }
    }

    fn appendStr(self: *ColumnWriter, s: []const u8) !void {
        // Data sub-stream: raw bytes only (no length prefix)
        try self.appendBytes(s);
        self.granule_rows += 1;
        self.total_rows += 1;
        // Only flush on row-count boundary, NOT on byte size.
        // This keeps mark count = ceil(rows / GRANULE_SIZE), matching CH expectations.
        const should_flush = self.granule_rows >= GRANULE_SIZE;

        // Size sub-stream: u64 LE length
        if (self.size_writer) |sw| {
            var lbuf: [8]u8 = undefined;
            std.mem.writeInt(u64, &lbuf, @intCast(s.len), .little);
            try sw.appendBytes(&lbuf);
            sw.granule_rows += 1;
            sw.total_rows += 1;
            if (should_flush) {
                try sw.flushBlock();
            }
        }

        if (should_flush) {
            try self.flushBlock();
        }
    }

    fn flushBlock(self: *ColumnWriter) !void {
        if (self.buf.items.len == 0) {
            // No data to flush, but still reset granule counter so the next
            // granule boundary is computed correctly.
            self.granule_rows = 0;
            return;
        }

        const uncompressed = self.buf.items;
        const bound = block.BLOCK_HEADER_TOTAL + @import("lz4.zig").compressBound(uncompressed.len);
        const compressed_buf = try self.allocator.alloc(u8, bound);
        defer self.allocator.free(compressed_buf);
        var w = std.Io.Writer.fixed(compressed_buf);
        try block.writeBlock(&w, uncompressed);
        const compressed = std.Io.Writer.buffered(&w);

        // Record mark
        try self.mark_list.append(self.allocator, .{
            .offset_in_compressed_file = self.bin_offset,
            .offset_in_decompressed_block = 0,
            .granularity = self.granule_rows,
        });

        // Write compressed block directly to .bin file
        try self.bin_file.writeStreamingAll(self.io, compressed);
        self.bin_offset += @intCast(compressed.len);
        self.buf.items.len = 0;
        self.granule_rows = 0;
    }

    fn finish(self: *ColumnWriter) !void {
        try self.flushBlock(); // flush partial last granule
    }

    /// Compute CityHash128 of a file by mmapping it.
    /// Returns hash of empty slice for empty files.
    fn hashFile(io: std.Io, path: []const u8) !u128 {
        var file = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.openFileAbsolute(io, path, .{})
        else
            try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        const stat = try file.stat(io);
        if (stat.size == 0) return cityhash.cityHash128(&.{});
        const size: usize = @intCast(stat.size);
        const ptr = try std.posix.mmap(null, size, @bitCast(@as(u32, 1)), .{ .TYPE = .PRIVATE }, file.handle, 0);
        defer std.posix.munmap(ptr);
        return cityhash.cityHash128(ptr[0..size]);
    }
};

/// A part writer. Open with `openPart`, append rows, call `finish`.
pub const Part = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    part_dir: []const u8,
    table: schema.Table,
    column_writers: []ColumnWriter,
    /// Companion size-stream writers for String columns (parallel slice, null for non-String cols).
    column_size_writers: []?ColumnWriter,
    row_count: u64,
    /// Index of the primary key column (first column by default).
    pk_col_idx: usize,
    /// Primary key granule entries (one per granule, for the first PK column).
    pk_entries: std.ArrayList(primary_idx.PkValue),

    /// Open (create) a new part directory for writing.
    ///
    /// `pk_col_name`: name of the column that is the first ORDER BY key.
    /// If null, defaults to the first column (index 0).
    /// The primary.idx will store one entry per granule for this column.
    pub fn open(
        io: std.Io,
        allocator: std.mem.Allocator,
        part_dir: []const u8,
        table: schema.Table,
        pk_col_name: ?[]const u8,
    ) !Part {
        // Resolve pk_col_idx from name
        const resolved_pk_col_idx: usize = blk: {
            if (pk_col_name) |name| {
                for (table.columns, 0..) |col, i| {
                    if (std.mem.eql(u8, col.name, name)) break :blk i;
                }
                return error.PkColumnNotFound;
            }
            break :blk 0;
        };
        // Create the part directory
        try std.Io.Dir.cwd().createDirPath(io, part_dir);

        const column_writers = try allocator.alloc(ColumnWriter, table.columns.len);
        var init_count: usize = 0;
        errdefer {
            for (column_writers[0..init_count]) |*cw| cw.deinit();
            allocator.free(column_writers);
        }

        const column_size_writers = try allocator.alloc(?ColumnWriter, table.columns.len);
        var size_init_count: usize = 0;
        errdefer {
            for (column_size_writers[0..size_init_count]) |*sw| if (sw.*) |*w| w.deinit();
            allocator.free(column_size_writers);
        }

        for (table.columns, 0..) |col, i| {
            const bin_path = try std.fmt.allocPrint(allocator, "{s}/{s}.bin", .{ part_dir, col.name });
            errdefer allocator.free(bin_path);

            var bin_file = try std.Io.Dir.cwd().createFile(io, bin_path, .{ .truncate = true });
            errdefer bin_file.close(io);

            column_writers[i] = ColumnWriter.init(allocator, io, col, bin_file, bin_path);
            init_count += 1;

            // For String columns, create a companion size-stream writer
            switch (col.ty) {
                .text, .char => {
                    const sz_bin_path = try std.fmt.allocPrint(allocator, "{s}/{s}.size.bin", .{ part_dir, col.name });
                    errdefer allocator.free(sz_bin_path);
                    var sz_bin_file = try std.Io.Dir.cwd().createFile(io, sz_bin_path, .{ .truncate = true });
                    errdefer sz_bin_file.close(io);
                    column_size_writers[i] = ColumnWriter.init(allocator, io, col, sz_bin_file, sz_bin_path);
                    column_writers[i].size_writer = &column_size_writers[i].?;
                },
                else => {
                    column_size_writers[i] = null;
                },
            }
            size_init_count += 1;
        }

        return .{
            .allocator = allocator,
            .io = io,
            .part_dir = part_dir,
            .table = table,
            .column_writers = column_writers,
            .column_size_writers = column_size_writers,
            .row_count = 0,
            .pk_col_idx = resolved_pk_col_idx,
            .pk_entries = .empty,
        };
    }

    pub fn deinit(self: *Part) void {
        for (self.column_writers) |*cw| cw.deinit();
        self.allocator.free(self.column_writers);
        for (self.column_size_writers) |*sw| if (sw.*) |*w| w.deinit();
        self.allocator.free(self.column_size_writers);
        self.pk_entries.deinit(self.allocator);
    }

    /// Append one row.  `values` must have one entry per table column, in order.
    pub fn appendRow(self: *Part, values: []const Value) !void {
        std.debug.assert(values.len == self.table.columns.len);
        const row_in_granule = self.row_count % GRANULE_SIZE;

        for (self.column_writers, values, self.table.columns) |*cw, val, col| {
            switch (col.ty) {
                .int8 => {
                    const buf = [1]u8{@bitCast(@as(i8, @intCast(val.i16)))};
                    try cw.appendFixed(&buf);
                },
                .int16 => {
                    var buf: [2]u8 = undefined;
                    std.mem.writeInt(i16, &buf, val.i16, .little);
                    try cw.appendFixed(&buf);
                },
                .int32, .date => {
                    var buf: [4]u8 = undefined;
                    std.mem.writeInt(i32, &buf, val.i32, .little);
                    try cw.appendFixed(&buf);
                },
                .int64, .timestamp => {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, val.i64, .little);
                    try cw.appendFixed(&buf);
                },
                .float32 => {
                    var buf: [4]u8 = undefined;
                    std.mem.writeInt(i32, &buf, val.i32, .little);
                    try cw.appendFixed(&buf);
                },
                .float64 => {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, val.i64, .little);
                    try cw.appendFixed(&buf);
                },
                .text, .char => {
                    try cw.appendStr(val.str);
                },
            }
        }

        // Capture PK value at granule boundary (row 0 of each granule)
        if (row_in_granule == 0) {
            const pk_val = values[self.pk_col_idx];
            const pk_entry: primary_idx.PkValue = switch (pk_val) {
                .i16 => |v| .{ .i16 = v },
                .i32 => |v| .{ .i32 = v },
                .i64 => |v| .{ .i64 = v },
                .str => |s| .{ .str = s },
            };
            try self.pk_entries.append(self.allocator, pk_entry);
        }

        self.row_count += 1;
    }

    /// Append a batch of i64 values to a fixed-width column by index.
    /// Used for columnar import (e.g. from Parquet streaming).
    /// `col_idx` must identify a fixed-width column (int16/int32/int64/date/timestamp).
    /// Values are cast from i64 to the column's native width.
    pub fn appendFixedBatch(self: *Part, col_idx: usize, values: []const i64) !void {
        const col = self.table.columns[col_idx];
        const cw = &self.column_writers[col_idx];
        for (values) |v| {
            switch (col.ty) {
                .int8 => {
                    const buf = [1]u8{@bitCast(@as(i8, @intCast(v)))};
                    try cw.appendFixed(&buf);
                },
                .int16 => {
                    var buf: [2]u8 = undefined;
                    std.mem.writeInt(i16, &buf, @intCast(v), .little);
                    try cw.appendFixed(&buf);
                },
                .int32, .date => {
                    var buf: [4]u8 = undefined;
                    std.mem.writeInt(i32, &buf, @intCast(v), .little);
                    try cw.appendFixed(&buf);
                },
                .int64, .timestamp => {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(i64, &buf, v, .little);
                    try cw.appendFixed(&buf);
                },
                // float32: raw bits stored as lower 32 bits of i64
                .float32 => {
                    var buf: [4]u8 = undefined;
                    std.mem.writeInt(u32, &buf, @intCast(v & 0xFFFF_FFFF), .little);
                    try cw.appendFixed(&buf);
                },
                // float64: raw bits stored as i64
                .float64 => {
                    var buf: [8]u8 = undefined;
                    std.mem.writeInt(u64, &buf, @bitCast(v), .little);
                    try cw.appendFixed(&buf);
                },
                else => return error.NotAFixedColumn,
            }
        }
        // Track row_count from the first column (pk_col_idx); update pk_entries
        if (col_idx == self.pk_col_idx) {
            for (values, 0..) |v, i| {
                const abs_row = self.row_count + i;
                if (abs_row % GRANULE_SIZE == 0) {
                    const pk_entry: primary_idx.PkValue = switch (col.ty) {
                        .int8 => .{ .i16 = @intCast(v) },
                        .int16 => .{ .i16 = @intCast(v) },
                        .int32, .date => .{ .i32 = @intCast(v) },
                        .int64, .timestamp => .{ .i64 = v },
                        // Floats as PK: store raw bits as i64 (unusual but valid)
                        .float32 => .{ .i32 = @intCast(v & 0xFFFF_FFFF) },
                        .float64 => .{ .i64 = v },
                        else => return error.NotAFixedColumn,
                    };
                    try self.pk_entries.append(self.allocator, pk_entry);
                }
            }
            self.row_count += values.len;
        }
    }

    /// Append a single string value to a string column by index.
    /// Used for streaming columnar import (avoids buffering all rows in memory).
    /// row_count is NOT updated here; call setRowCount() after all columns are done.
    pub fn appendStrOne(self: *Part, col_idx: usize, s: []const u8) !void {
        const cw = &self.column_writers[col_idx];
        try cw.appendStr(s);
        if (col_idx == self.pk_col_idx) {
            const abs_row = self.row_count;
            if (abs_row % GRANULE_SIZE == 0) {
                try self.pk_entries.append(self.allocator, .{ .str = s });
            }
            self.row_count += 1;
        }
    }

    /// Append a batch of string values to a string column by index.
    /// Used for columnar import.
    pub fn appendStrBatch(self: *Part, col_idx: usize, strings: []const []const u8) !void {
        const cw = &self.column_writers[col_idx];
        for (strings) |s| try cw.appendStr(s);
        if (col_idx == self.pk_col_idx) {
            for (strings, 0..) |s, i| {
                const abs_row = self.row_count + i;
                if (abs_row % GRANULE_SIZE == 0) {
                    try self.pk_entries.append(self.allocator, .{ .str = s });
                }
            }
            self.row_count += strings.len;
        }
    }

    /// Set total row count explicitly — call after all columnar batches are written,
    /// before finish().  Required when using appendFixedBatch/appendStrBatch since
    /// non-PK columns don't update row_count.
    pub fn setRowCount(self: *Part, n: u64) void {
        self.row_count = n;
    }

    /// Flush all column data and write the part metadata files.
    pub fn finish(self: *Part) !void {
        // Flush all column writers (and their size companions)
        for (self.column_writers) |*cw| try cw.finish();
        for (self.column_size_writers) |*sw| if (sw.*) |*w| try w.finish();

        // Build checksums entries list
        var checksum_entries: std.ArrayList(checksums.FileChecksum) = .empty;
        defer checksum_entries.deinit(self.allocator);
        // Names owned by checksum_entries — freed after checksums.txt is written
        var owned_names: std.ArrayList([]u8) = .empty;
        defer {
            for (owned_names.items) |n| self.allocator.free(n);
            owned_names.deinit(self.allocator);
        }

        // Write .bin and .mrk2 files for each column
        for (self.column_writers, self.column_size_writers, self.table.columns) |*cw, *sw_opt, col| {
            // Helper: record one compressed-stream pair (.bin + .mrk2) into checksum_entries
            try self.recordCompressedStream(&checksum_entries, &owned_names, cw, col.name);

            // For String columns: also record the size sub-stream
            if (sw_opt.*) |*sw| {
                const sz_stem = try std.fmt.allocPrint(self.allocator, "{s}.size", .{col.name});
                defer self.allocator.free(sz_stem);
                try self.recordCompressedStream(&checksum_entries, &owned_names, sw, sz_stem);
            }
        }

        // Write columns.txt
        {
            const ch_cols = try columns_txt.fromTable(self.allocator, self.table);
            defer columns_txt.freeChColumns(self.allocator, ch_cols);
            var aw = std.Io.Writer.Allocating.init(self.allocator);
            defer aw.deinit();
            try columns_txt.write(&aw.writer, ch_cols);
            var al = aw.toArrayList();
            defer al.deinit(self.allocator);
            const data = al.items;
            const path = try std.fmt.allocPrint(self.allocator, "{s}/columns.txt", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, data);
            const h = cityhash.cityHash128(data);
            try checksum_entries.append(self.allocator, .{
                .name = "columns.txt",
                .file_size = data.len,
                .file_hash = h,
                .is_compressed = false,
            });
        }

        // Write count.txt
        {
            var buf: [32]u8 = undefined;
            var w = std.Io.Writer.fixed(&buf);
            try count_txt.write(&w, self.row_count);
            const data = std.Io.Writer.buffered(&w);
            const path = try std.fmt.allocPrint(self.allocator, "{s}/count.txt", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, data);
            const h = cityhash.cityHash128(data);
            try checksum_entries.append(self.allocator, .{
                .name = "count.txt",
                .file_size = data.len,
                .file_hash = h,
                .is_compressed = false,
            });
        }

        // Write primary.idx
        {
            var aw = std.Io.Writer.Allocating.init(self.allocator);
            defer aw.deinit();
            for (self.pk_entries.items) |pk_val| {
                const row = [_]primary_idx.PkValue{pk_val};
                try primary_idx.write(&aw.writer, &[_]primary_idx.PkRow{&row});
            }
            var al = aw.toArrayList();
            defer al.deinit(self.allocator);
            const data = al.items;
            const path = try std.fmt.allocPrint(self.allocator, "{s}/primary.idx", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, data);
            const h = cityhash.cityHash128(data);
            try checksum_entries.append(self.allocator, .{
                .name = "primary.idx",
                .file_size = data.len,
                .file_hash = h,
                .is_compressed = false,
            });
        }

        // Sort checksum entries by name (CH requires sorted order)
        std.mem.sort(checksums.FileChecksum, checksum_entries.items, {}, checksumLessThan);

        // Write metadata_version.txt (required by CH 26.x)
        {
            const path = try std.fmt.allocPrint(self.allocator, "{s}/metadata_version.txt", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, "0");
            const h = cityhash.cityHash128("0");
            try checksum_entries.append(self.allocator, .{
                .name = "metadata_version.txt",
                .file_size = 1,
                .file_hash = h,
                .is_compressed = false,
            });
        }

        // Write serialization.json — tells CH to use String serialization v1
        // (separate data sub-stream + size sub-stream).
        // All columns use "Default" kind (no sparse encoding).
        {
            var aw = std.Io.Writer.Allocating.init(self.allocator);
            defer aw.deinit();
            try aw.writer.writeAll("{\"columns\":[");
            for (self.table.columns, 0..) |col, i| {
                if (i > 0) try aw.writer.writeAll(",");
                try aw.writer.print("{{\"kind\":\"Default\",\"name\":\"{s}\",\"num_defaults\":0,\"num_rows\":{d}}}", .{ col.name, self.row_count });
            }
            try aw.writer.writeAll("],\"propagate_types_serialization_versions_to_nested_types\":true,\"types_serialization_versions\":{\"string\":1},\"version\":1}");
            var al = aw.toArrayList();
            defer al.deinit(self.allocator);
            const data = al.items;
            const path = try std.fmt.allocPrint(self.allocator, "{s}/serialization.json", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, data);
            const h = cityhash.cityHash128(data);
            try checksum_entries.append(self.allocator, .{
                .name = "serialization.json",
                .file_size = data.len,
                .file_hash = h,
                .is_compressed = false,
            });
        }

        // Write checksums.txt
        {
            var aw = std.Io.Writer.Allocating.init(self.allocator);
            defer aw.deinit();
            try checksums.write(self.allocator, &aw.writer, checksum_entries.items);
            var al = aw.toArrayList();
            defer al.deinit(self.allocator);
            const path = try std.fmt.allocPrint(self.allocator, "{s}/checksums.txt", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, al.items);
        }
    }

    fn writeFile(io: std.Io, path: []const u8, data: []const u8) !void {
        var f = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        defer f.close(io);
        try f.writeStreamingAll(io, data);
    }

    /// Record a compressed-stream pair (`{stem}.bin` + `{stem}.cmrk2`) into checksum_entries.
    /// `stem` is the bare name without extension (e.g. "Title" or "Title.size").
    /// Uses `cw.unc_buf` (in-memory uncompressed bytes) instead of a temp file.
    fn recordCompressedStream(
        self: *Part,
        checksum_entries: *std.ArrayList(checksums.FileChecksum),
        owned_names: *std.ArrayList([]u8),
        cw: *ColumnWriter,
        stem: []const u8,
    ) !void {
        // --- {stem}.bin ---
        const bin_name = try std.fmt.allocPrint(self.allocator, "{s}.bin", .{stem});
        try owned_names.append(self.allocator, bin_name);

        // Hash the compressed .bin file (mmap)
        const file_hash = try ColumnWriter.hashFile(self.io, cw.bin_path);
        const bin_size = blk: {
            var f = if (std.fs.path.isAbsolute(cw.bin_path))
                try std.Io.Dir.openFileAbsolute(self.io, cw.bin_path, .{})
            else
                try std.Io.Dir.cwd().openFile(self.io, cw.bin_path, .{});
            defer f.close(self.io);
            const st = try f.stat(self.io);
            break :blk @as(u64, @intCast(st.size));
        };

        // Hash the uncompressed content directly from RAM — no temp file needed
        const uncompressed_hash = cityhash.cityHash128(cw.unc_buf.items);
        const uncompressed_size = cw.uncompressed_size;
        // Free the unc_buf now that we have the hash — reclaim memory early
        cw.unc_buf.deinit(self.allocator);
        cw.unc_buf = .empty;

        try checksum_entries.append(self.allocator, .{
            .name = bin_name,
            .file_size = bin_size,
            .file_hash = file_hash,
            .is_compressed = true,
            .uncompressed_size = uncompressed_size,
            .uncompressed_hash = uncompressed_hash,
        });

        // --- {stem}.cmrk2 (compressed marks, LZ4 block format) ---
        const mrk_name = try std.fmt.allocPrint(self.allocator, "{s}.cmrk2", .{stem});
        try owned_names.append(self.allocator, mrk_name);
        const mrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.part_dir, mrk_name });
        defer self.allocator.free(mrk_path);

        // Serialize raw marks
        const mrk_data = try self.allocator.alloc(u8, cw.mark_list.items.len * marks.MARK_SIZE);
        defer self.allocator.free(mrk_data);
        var mrk_w = std.Io.Writer.fixed(mrk_data);
        try marks.writeMarks(&mrk_w, cw.mark_list.items);
        const mrk_bytes = std.Io.Writer.buffered(&mrk_w);

        // Write as LZ4-compressed block (same format as .bin files)
        const bound = block.BLOCK_HEADER_TOTAL + @import("lz4.zig").compressBound(mrk_bytes.len);
        const cmrk_buf = try self.allocator.alloc(u8, bound);
        defer self.allocator.free(cmrk_buf);
        var cmrk_w = std.Io.Writer.fixed(cmrk_buf);
        try block.writeBlock(&cmrk_w, mrk_bytes);
        const cmrk_bytes = std.Io.Writer.buffered(&cmrk_w);

        try writeFile(self.io, mrk_path, cmrk_bytes);
        const mrk_hash = cityhash.cityHash128(cmrk_bytes);
        try checksum_entries.append(self.allocator, .{
            .name = mrk_name,
            .file_size = cmrk_bytes.len,
            .file_hash = mrk_hash,
            .is_compressed = true,
            .uncompressed_size = mrk_bytes.len,
            .uncompressed_hash = cityhash.cityHash128(mrk_bytes),
        });
    }
};

fn checksumLessThan(_: void, a: checksums.FileChecksum, b: checksums.FileChecksum) bool {
    return std.mem.lessThan(u8, a.name, b.name);
}

// ── Read path ─────────────────────────────────────────────────────────────────

/// Open an existing MergeTree part directory for reading.
///
/// `part_dir` and `table` must outlive the `OpenedPart`.
pub const OpenedPart = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    part_dir: []const u8,
    table: schema.Table,
    row_count: u64,

    pub fn open(
        io: std.Io,
        allocator: std.mem.Allocator,
        part_dir: []const u8,
        table: schema.Table,
    ) !OpenedPart {
        const count_path = try std.fmt.allocPrint(allocator, "{s}/count.txt", .{part_dir});
        defer allocator.free(count_path);
        const row_count = try count_txt.readPath(allocator, io, count_path);
        return .{
            .allocator = allocator,
            .io = io,
            .part_dir = part_dir,
            .table = table,
            .row_count = row_count,
        };
    }

    pub fn deinit(_: *OpenedPart) void {}

    /// Return a ColumnReader for column at `col_idx`.
    /// The caller owns the returned ColumnReader and must call deinit() on it.
    pub fn columnReader(self: *OpenedPart, col_idx: usize) !ColumnReader {
        const col = self.table.columns[col_idx];
        const bin_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.bin", .{ self.part_dir, col.name });
        defer self.allocator.free(bin_path);
        const mrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.cmrk2", .{ self.part_dir, col.name });
        defer self.allocator.free(mrk_path);

        // Load .cmrk2 (compressed marks — single LZ4 block)
        const cmrk_bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, mrk_path, self.allocator, .limited(std.math.maxInt(usize)));
        defer self.allocator.free(cmrk_bytes);
        var cmrk_r = std.Io.Reader.fixed(cmrk_bytes);
        const mrk_raw = try block.readBlock(self.allocator, &cmrk_r);
        defer self.allocator.free(mrk_raw);
        var mrk_r = std.Io.Reader.fixed(mrk_raw);
        const mark_list = try marks.readAllMarks(self.allocator, &mrk_r);
        defer self.allocator.free(mark_list);

        // Load .bin and decompress all blocks into a single contiguous buffer
        const bin_bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, bin_path, self.allocator, .limited(std.math.maxInt(usize)));
        defer self.allocator.free(bin_bytes);

        var data: std.ArrayListUnmanaged(u8) = .empty;
        errdefer data.deinit(self.allocator);

        if (bin_bytes.len > 0) {
            var bin_r = std.Io.Reader.fixed(bin_bytes);
            while (true) {
                const chunk = block.readBlock(self.allocator, &bin_r) catch |e| switch (e) {
                    error.TruncatedBlock => break,
                    else => return e,
                };
                defer self.allocator.free(chunk);
                try data.appendSlice(self.allocator, chunk);
            }
        }

        // For String columns: also load the size sub-stream
        var size_data: ?[]u8 = null;
        switch (col.ty) {
            .text, .char => {
                const sz_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.size.bin", .{ self.part_dir, col.name });
                defer self.allocator.free(sz_path);
                const sz_bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, sz_path, self.allocator, .limited(std.math.maxInt(usize)));
                defer self.allocator.free(sz_bytes);

                var sz_data: std.ArrayListUnmanaged(u8) = .empty;
                errdefer sz_data.deinit(self.allocator);
                if (sz_bytes.len > 0) {
                    var sz_r = std.Io.Reader.fixed(sz_bytes);
                    while (true) {
                        const chunk = block.readBlock(self.allocator, &sz_r) catch |e| switch (e) {
                            error.TruncatedBlock => break,
                            else => return e,
                        };
                        defer self.allocator.free(chunk);
                        try sz_data.appendSlice(self.allocator, chunk);
                    }
                }
                size_data = try sz_data.toOwnedSlice(self.allocator);
            },
            else => {},
        }

        return ColumnReader{
            .allocator = self.allocator,
            .col = col,
            .row_count = self.row_count,
            .data = try data.toOwnedSlice(self.allocator),
            .size_data = size_data,
            .cursor = 0,
            .size_cursor = 0,
            .rows_read = 0,
        };
    }
};

/// A cursor for reading decoded column values from a MergeTree part.
///
/// Data is fully decompressed on construction (全量加载).
/// String slices returned via `readStrings` callbacks point into the internal
/// buffer and are valid only for the duration of that callback invocation.
pub const ColumnReader = struct {
    allocator: std.mem.Allocator,
    col: schema.Column,
    row_count: u64,
    /// Fully decompressed column payload (owned).
    /// For String columns: raw UTF-8 bytes (no length prefix).
    data: []u8,
    /// For String columns: decompressed size sub-stream (u64 LE per row), owned.
    /// null for fixed-width columns.
    size_data: ?[]u8,
    /// Byte cursor into `data`.
    cursor: usize,
    /// Byte cursor into `size_data`.
    size_cursor: usize,
    rows_read: u64,

    pub fn deinit(self: *ColumnReader) void {
        self.allocator.free(self.data);
        if (self.size_data) |sd| self.allocator.free(sd);
    }

    /// Read up to `out.len` values from a fixed-width column, cast to i64.
    /// Returns the number of rows actually read.
    pub fn readFixed(self: *ColumnReader, out: []i64) !usize {
        const width: usize = types.chFixedWidth(self.col.ty) orelse return error.NotAFixedColumn;
        const remaining = self.row_count - self.rows_read;
        const n = @min(out.len, remaining);
        if (n == 0) return 0;
        const needed = n * width;
        if (self.cursor + needed > self.data.len) return error.UnexpectedEndOfData;
        for (0..n) |i| {
            const slice = self.data[self.cursor + i * width ..][0..width];
            out[i] = switch (self.col.ty) {
                .int8 => @as(i64, @as(i8, @bitCast(slice[0]))),
                .int16 => @as(i64, std.mem.readInt(i16, slice[0..2], .little)),
                .int32, .date => @as(i64, std.mem.readInt(i32, slice[0..4], .little)),
                .int64, .timestamp => std.mem.readInt(i64, slice[0..8], .little),
                // float32: raw 4-byte bits stored as i64 (upper 32 bits zero)
                .float32 => @as(i64, std.mem.readInt(u32, slice[0..4], .little)),
                // float64: raw 8-byte bits reinterpreted as i64
                .float64 => @bitCast(std.mem.readInt(u64, slice[0..8], .little)),
                else => {
                    return error.NotAFixedColumn;
                },
            };
        }
        self.cursor += needed;
        self.rows_read += n;
        return n;
    }

    /// Read up to `n` strings from a string column, invoking `callback(ctx, []const u8)`
    /// for each.  The slice passed to callback is valid only for that call.
    /// Returns the number of rows actually read.
    pub fn readStrings(
        self: *ColumnReader,
        n: usize,
        ctx: anytype,
        callback: anytype,
    ) !usize {
        switch (self.col.ty) {
            .text, .char => {},
            else => return error.NotAStringColumn,
        }
        const sd = self.size_data orelse return error.MissingSizeStream;
        const remaining_rows = self.row_count - self.rows_read;
        const count = @min(n, remaining_rows);
        var read: usize = 0;
        while (read < count) : (read += 1) {
            // Read u64 LE length from size sub-stream
            if (self.size_cursor + 8 > sd.len) return error.UnexpectedEndOfData;
            const len: usize = @intCast(std.mem.readInt(u64, sd[self.size_cursor..][0..8], .little));
            self.size_cursor += 8;
            if (len > string_codec.MAX_STRING_LEN) return error.StringTooLarge;
            if (self.cursor + len > self.data.len) return error.UnexpectedEndOfData;
            const s = self.data[self.cursor .. self.cursor + len];
            self.cursor += len;
            try callback(ctx, s);
        }
        self.rows_read += count;
        return count;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "part write and verify files exist" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_part_all_1_1_0";

    const cols = [_]schema.Column{
        .{ .name = "EventDate", .ty = .date },
        .{ .name = "CounterID", .ty = .int32 },
    };
    const table = schema.Table{ .name = "test", .columns = &cols };

    var part = try Part.open(io, allocator, part_dir, table, null);
    defer part.deinit();

    // Write 100 rows
    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const values = [_]Value{
            .{ .i32 = 19000 + @divTrunc(i, 10) },
            .{ .i32 = i + 1 },
        };
        try part.appendRow(&values);
    }
    try part.finish();

    // Verify files exist
    const cwd = std.Io.Dir.cwd();
    _ = try cwd.openFile(io, part_dir ++ "/columns.txt", .{});
    _ = try cwd.openFile(io, part_dir ++ "/count.txt", .{});
    _ = try cwd.openFile(io, part_dir ++ "/CounterID.bin", .{});
    _ = try cwd.openFile(io, part_dir ++ "/CounterID.cmrk2", .{});
    _ = try cwd.openFile(io, part_dir ++ "/checksums.txt", .{});
    _ = try cwd.openFile(io, part_dir ++ "/primary.idx", .{});
    _ = try cwd.openFile(io, part_dir ++ "/serialization.json", .{});
    _ = try cwd.openFile(io, part_dir ++ "/metadata_version.txt", .{});
}

test "Part write + OpenedPart read round-trips fixed and string columns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_part_roundtrip";

    const cols = [_]schema.Column{
        .{ .name = "ID", .ty = .int32 },
        .{ .name = "Name", .ty = .text },
    };
    const table = schema.Table{ .name = "test_rt", .columns = &cols };

    const N = 20;

    // ── Write ──────────────────────────────────────────────────────────────────
    {
        var part = try Part.open(io, allocator, part_dir, table, null);
        defer part.deinit();

        // Write fixed column (col 0 = PK, drives row_count)
        var fixed_vals: [N]i64 = undefined;
        for (0..N) |i| fixed_vals[i] = @intCast(i + 1);
        try part.appendFixedBatch(0, &fixed_vals);

        // Write string column (col 1)
        const strings = [N][]const u8{
            "alice", "bob", "carol", "dave", "eve",
            "frank", "grace", "hank", "iris", "jack",
            "karen", "liam", "mia", "ned", "olivia",
            "paul", "quinn", "rose", "sam", "tina",
        };
        try part.appendStrBatch(1, &strings);
        part.setRowCount(N);
        try part.finish();
    }

    // ── Read fixed ─────────────────────────────────────────────────────────────
    {
        var op = try OpenedPart.open(io, allocator, part_dir, table);
        defer op.deinit();

        try std.testing.expectEqual(@as(u64, N), op.row_count);

        var cr = try op.columnReader(0);
        defer cr.deinit();

        var out: [N]i64 = undefined;
        const n = try cr.readFixed(&out);
        try std.testing.expectEqual(N, n);
        for (0..N) |i| {
            try std.testing.expectEqual(@as(i64, @intCast(i + 1)), out[i]);
        }
    }

    // ── Read strings ───────────────────────────────────────────────────────────
    {
        var op = try OpenedPart.open(io, allocator, part_dir, table);
        defer op.deinit();

        var cr = try op.columnReader(1);
        defer cr.deinit();

        const expected = [N][]const u8{
            "alice", "bob", "carol", "dave", "eve",
            "frank", "grace", "hank", "iris", "jack",
            "karen", "liam", "mia", "ned", "olivia",
            "paul", "quinn", "rose", "sam", "tina",
        };

        const Ctx = struct {
            idx: usize = 0,
            expected: *const [N][]const u8,
            failed: bool = false,

            fn cb(self: *@This(), s: []const u8) anyerror!void {
                if (self.idx >= N) { self.failed = true; return; }
                if (!std.mem.eql(u8, s, self.expected[self.idx])) self.failed = true;
                self.idx += 1;
            }
        };
        var ctx = Ctx{ .expected = &expected };
        const n = try cr.readStrings(N, &ctx, Ctx.cb);
        try std.testing.expectEqual(N, n);
        try std.testing.expect(!ctx.failed);
        try std.testing.expectEqual(N, ctx.idx);
    }
}

test "pk_col_name: primary.idx stores correct PK column values" {
    // Regression test for the bug where pk_col_idx was hardcoded to 0.
    // When pk_col_name is "CounterID" (col index 1), primary.idx must store
    // CounterID i32 values at granule boundaries, NOT WatchID i64 values.
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_pk_regression";

    // Two-column table: WatchID (i64, col 0) and CounterID (i32, col 1).
    // ORDER BY CounterID → pk_col_name = "CounterID".
    const cols = [_]schema.Column{
        .{ .name = "WatchID", .ty = .int64 },
        .{ .name = "CounterID", .ty = .int32 },
    };
    const table = schema.Table{ .name = "pk_test", .columns = &cols };

    // Write exactly GRANULE_SIZE rows so we get exactly 1 granule mark entry.
    // First row values: WatchID=999, CounterID=42.
    {
        var part = try Part.open(io, allocator, part_dir, table, "CounterID");
        defer part.deinit();

        // Write GRANULE_SIZE rows for WatchID (col 0)
        var watch_ids: [GRANULE_SIZE]i64 = undefined;
        for (0..GRANULE_SIZE) |i| watch_ids[i] = @intCast(1000 + i);
        try part.appendFixedBatch(0, &watch_ids);

        // Write GRANULE_SIZE rows for CounterID (col 1 = PK)
        var counter_ids: [GRANULE_SIZE]i64 = undefined;
        for (0..GRANULE_SIZE) |i| counter_ids[i] = @intCast(42 + i);
        try part.appendFixedBatch(1, &counter_ids);

        try part.finish();
    }

    // Verify primary.idx:
    // - pk_col_idx = 1 (CounterID, i32 = 4 bytes)
    // - GRANULE_SIZE rows → 1 mark → 1 entry in primary.idx
    // - primary.idx size must be 4 bytes (one i32 = CounterID[0] = 42)
    // - NOT 8 bytes (which would mean WatchID i64 was mistakenly stored)
    {
        const cwd = std.Io.Dir.cwd();
        const idx_data = try cwd.readFileAlloc(io, part_dir ++ "/primary.idx", allocator, .limited(256));
        defer allocator.free(idx_data);

        // Must be exactly 4 bytes: one i32 value for CounterID granule 0
        try std.testing.expectEqual(@as(usize, 4), idx_data.len);

        // The value must be CounterID[0] = 42 (little-endian i32)
        const val = std.mem.readInt(i32, idx_data[0..4], .little);
        try std.testing.expectEqual(@as(i32, 42), val);
    }
}

test "pk_col_name null defaults to col 0" {
    // When pk_col_name is null, pk_col_idx defaults to 0.
    // primary.idx stores col-0 (WatchID i64) values.
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_pk_default";

    const cols = [_]schema.Column{
        .{ .name = "WatchID", .ty = .int64 },
        .{ .name = "CounterID", .ty = .int32 },
    };
    const table = schema.Table{ .name = "pk_default_test", .columns = &cols };

    {
        var part = try Part.open(io, allocator, part_dir, table, null);
        defer part.deinit();

        var watch_ids: [GRANULE_SIZE]i64 = undefined;
        for (0..GRANULE_SIZE) |i| watch_ids[i] = @intCast(7777 + i);
        try part.appendFixedBatch(0, &watch_ids);

        var counter_ids: [GRANULE_SIZE]i64 = undefined;
        for (0..GRANULE_SIZE) |i| counter_ids[i] = @intCast(1 + i);
        try part.appendFixedBatch(1, &counter_ids);

        try part.finish();
    }

    // primary.idx must be 8 bytes: one i64 = WatchID[0] = 7777
    {
        const cwd = std.Io.Dir.cwd();
        const idx_data = try cwd.readFileAlloc(io, part_dir ++ "/primary.idx", allocator, .limited(256));
        defer allocator.free(idx_data);

        try std.testing.expectEqual(@as(usize, 8), idx_data.len);
        const val = std.mem.readInt(i64, idx_data[0..8], .little);
        try std.testing.expectEqual(@as(i64, 7777), val);
    }
}

test "pk_col_name unknown returns PkColumnNotFound error" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const cols = [_]schema.Column{
        .{ .name = "WatchID", .ty = .int64 },
    };
    const table = schema.Table{ .name = "pk_err_test", .columns = &cols };

    const result = Part.open(io, allocator, "/tmp/zig_test_pk_err", table, "NonExistentColumn");
    try std.testing.expectError(error.PkColumnNotFound, result);
}
