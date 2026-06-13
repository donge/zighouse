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
const low_card = @import("low_card.zig");

pub const GRANULE_SIZE: u64 = 8192;

pub fn readCompactPrimaryFixedValues(
    io: std.Io,
    allocator: std.mem.Allocator,
    part_dir: []const u8,
    ty: schema.ColumnType,
) ![]i64 {
    const width = types.chFixedWidth(ty) orelse return error.UnsupportedPrimaryKeyType;
    if (width != 2 and width != 4 and width != 8) return error.UnsupportedPrimaryKeyType;

    const path = try std.fmt.allocPrint(allocator, "{s}/primary.cidx", .{part_dir});
    defer allocator.free(path);
    const compressed = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(compressed);

    var reader = std.Io.Reader.fixed(compressed);
    const raw = try block.readBlock(allocator, &reader);
    errdefer allocator.free(raw);
    if (raw.len % width != 0) return error.InvalidPrimaryIndex;

    const values = try allocator.alloc(i64, raw.len / width);
    errdefer allocator.free(values);
    for (values, 0..) |*out, i| {
        const bytes = raw[i * width ..][0..width];
        out.* = switch (ty) {
            .int16, .date => std.mem.readInt(i16, bytes[0..2], .little),
            .int32 => std.mem.readInt(i32, bytes[0..4], .little),
            .int64, .timestamp => std.mem.readInt(i64, bytes[0..8], .little),
            else => return error.UnsupportedPrimaryKeyType,
        };
    }
    allocator.free(raw);
    return values;
}
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
    /// Compression codec for this column's .bin file.
    codec: u8 = block.METHOD_LZ4,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        col: schema.Column,
        bin_file: std.Io.File,
        bin_path: []u8,
        codec: u8,
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
            .codec = codec,
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
        const bound = block.BLOCK_HEADER_TOTAL + switch (self.codec) {
            block.METHOD_ZSTD => block.zstd.ZSTD_compressBound(uncompressed.len),
            else => @import("lz4.zig").compressBound(uncompressed.len),
        };
        const compressed_buf = try self.allocator.alloc(u8, bound);
        defer self.allocator.free(compressed_buf);
        var w = std.Io.Writer.fixed(compressed_buf);
        try block.writeBlock(&w, uncompressed, self.codec);
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
    /// Compression codec for all column .bin files.
    codec: u8 = block.METHOD_LZ4,

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
        codec: u8,
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

            column_writers[i] = ColumnWriter.init(allocator, io, col, bin_file, bin_path, codec);
            init_count += 1;

            // For String columns, create a companion size-stream writer
            switch (col.ty) {
                .text, .char => {
                    const sz_bin_path = try std.fmt.allocPrint(allocator, "{s}/{s}.size.bin", .{ part_dir, col.name });
                    errdefer allocator.free(sz_bin_path);
                    var sz_bin_file = try std.Io.Dir.cwd().createFile(io, sz_bin_path, .{ .truncate = true });
                    errdefer sz_bin_file.close(io);
                    column_size_writers[i] = ColumnWriter.init(allocator, io, col, sz_bin_file, sz_bin_path, codec);
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
            .codec = codec,
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
                .text, .char, .low_card => {
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

        // Write default_compression_codec.txt
        {
            const codec_str: []const u8 = if (self.codec == block.METHOD_ZSTD) "CODEC(ZSTD(1))\n" else "CODEC(LZ4)\n";
            const path = try std.fmt.allocPrint(self.allocator, "{s}/default_compression_codec.txt", .{self.part_dir});
            defer self.allocator.free(path);
            try writeFile(self.io, path, codec_str);
            const h = cityhash.cityHash128(codec_str);
            try checksum_entries.append(self.allocator, .{
                .name = "default_compression_codec.txt",
                .file_size = codec_str.len,
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
        try block.writeBlock(&cmrk_w, mrk_bytes, block.METHOD_LZ4);
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

    fn fileExists(self: *OpenedPart, path: []const u8) bool {
        const f = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch return false;
        f.close(self.io);
        return true;
    }

    fn readMarksForStem(self: *OpenedPart, stem: []const u8) ![]marks.Mark {
        const cmrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.cmrk2", .{ self.part_dir, stem });
        defer self.allocator.free(cmrk_path);
        var compressed_marks = true;
        const cmrk_bytes = std.Io.Dir.cwd().readFileAlloc(self.io, cmrk_path, self.allocator, .limited(std.math.maxInt(usize))) catch |err| switch (err) {
            error.FileNotFound => blk: {
                const mrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.mrk2", .{ self.part_dir, stem });
                defer self.allocator.free(mrk_path);
                compressed_marks = false;
                break :blk try std.Io.Dir.cwd().readFileAlloc(self.io, mrk_path, self.allocator, .limited(std.math.maxInt(usize)));
            },
            else => return err,
        };
        defer self.allocator.free(cmrk_bytes);

        const raw = if (compressed_marks) raw_blk: {
            var r = std.Io.Reader.fixed(cmrk_bytes);
            break :raw_blk try block.readBlock(self.allocator, &r);
        } else try self.allocator.dupe(u8, cmrk_bytes);
        defer self.allocator.free(raw);

        var r = std.Io.Reader.fixed(raw);
        return marks.readAllMarks(self.allocator, &r);
    }

    fn compressedRange(mark_list: []const marks.Mark, idx: usize, file_size: u64) !struct { u64, u64 } {
        if (idx >= mark_list.len) return error.InvalidMarkCount;
        const start = mark_list[idx].offset_in_compressed_file;
        var end = file_size;
        var i = idx + 1;
        while (i < mark_list.len) : (i += 1) {
            const off = mark_list[i].offset_in_compressed_file;
            if (off > start) {
                end = off;
                break;
            }
        }
        if (end < start or end > file_size) return error.InvalidMarkCount;
        return .{ start, end - start };
    }

    fn readCompressedBlockAt(self: *OpenedPart, file: std.Io.File, mark_list: []const marks.Mark, idx: usize, file_size: u64) ![]u8 {
        const range = try compressedRange(mark_list, idx, file_size);
        const len: usize = @intCast(range[1]);
        const compressed = try self.allocator.alloc(u8, len);
        defer self.allocator.free(compressed);
        const read_n = try file.readPositionalAll(self.io, compressed, range[0]);
        if (read_n != compressed.len) return error.UnexpectedEndOfData;
        var reader = std.Io.Reader.fixed(compressed);
        return block.readBlock(self.allocator, &reader);
    }

    fn blockSliceForMark(mark_list: []const marks.Mark, idx: usize, decompressed: []const u8, max_len: ?usize) ![]const u8 {
        const decompressed_len = decompressed.len;
        const start: usize = @intCast(mark_list[idx].offset_in_decompressed_block);
        if (start > decompressed_len) return error.UnexpectedEndOfData;
        var end = decompressed_len;
        if (idx + 1 < mark_list.len and mark_list[idx + 1].offset_in_compressed_file == mark_list[idx].offset_in_compressed_file) {
            end = @intCast(mark_list[idx + 1].offset_in_decompressed_block);
        }
        if (max_len) |len| {
            if (start + len > decompressed_len) return error.UnexpectedEndOfData;
            end = @min(end, start + len);
        }
        if (end < start) return error.UnexpectedEndOfData;
        return decompressed[start..end];
    }

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

    pub fn columnReaderRange(self: *OpenedPart, col_idx: usize, start_row: u64, row_count: usize) !ColumnReader {
        if (row_count == 0 or start_row >= self.row_count) {
            return ColumnReader{
                .allocator = self.allocator,
                .col = self.table.columns[col_idx],
                .row_count = 0,
                .data = try self.allocator.alloc(u8, 0),
                .size_data = null,
                .cursor = 0,
                .size_cursor = 0,
                .rows_read = 0,
            };
        }

        const col = self.table.columns[col_idx];
        const end_row = @min(start_row + row_count, self.row_count);
        const marks_for_col = try self.readMarksForStem(col.name);
        defer self.allocator.free(marks_for_col);
        if (marks_for_col.len == 0) return error.InvalidMarkCount;

        const first_gran: usize = @intCast(start_row / GRANULE_SIZE);
        const last_gran: usize = @intCast((end_row - 1) / GRANULE_SIZE);
        if (first_gran >= marks_for_col.len) return error.InvalidMarkCount;

        const bin_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.bin", .{ self.part_dir, col.name });
        defer self.allocator.free(bin_path);
        const bin_file = try std.Io.Dir.cwd().openFile(self.io, bin_path, .{});
        defer bin_file.close(self.io);
        const bin_size = (try bin_file.stat(self.io)).size;

        var data_buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer data_buf.deinit(self.allocator);
        var rows_loaded: u64 = 0;

        const width = types.chFixedWidth(col.ty);
        for (first_gran..@min(last_gran + 1, marks_for_col.len)) |g| {
            const block_data = try self.readCompressedBlockAt(bin_file, marks_for_col, g, bin_size);
            defer self.allocator.free(block_data);
            const rows_in_gran = marks_for_col[g].granularity;
            rows_loaded += rows_in_gran;
            const byte_len = if (width) |w| @as(?usize, @intCast(rows_in_gran * w)) else null;
            const slice = try blockSliceForMark(marks_for_col, g, block_data, byte_len);
            try data_buf.appendSlice(self.allocator, slice);
        }

        const size_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.size.bin", .{ self.part_dir, col.name });
        defer self.allocator.free(size_path);
        var size_data: ?[]u8 = null;
        var varuint_strings = false;
        if (col.ty == .text or col.ty == .char or col.ty == .low_card) {
            const has_size_stream = self.fileExists(size_path);
            if (has_size_stream) {
                const size_stem = try std.fmt.allocPrint(self.allocator, "{s}.size", .{col.name});
                defer self.allocator.free(size_stem);
                const size_marks = try self.readMarksForStem(size_stem);
                defer {
                    self.allocator.free(size_marks);
                }
                const size_file = try std.Io.Dir.cwd().openFile(self.io, size_path, .{});
                defer size_file.close(self.io);
                const size_file_size = (try size_file.stat(self.io)).size;
                var size_buf: std.ArrayListUnmanaged(u8) = .empty;
                errdefer size_buf.deinit(self.allocator);
                for (first_gran..@min(last_gran + 1, size_marks.len)) |g| {
                    const block_data = try self.readCompressedBlockAt(size_file, size_marks, g, size_file_size);
                    defer self.allocator.free(block_data);
                    const slice = try blockSliceForMark(size_marks, g, block_data, @intCast(size_marks[g].granularity * 8));
                    try size_buf.appendSlice(self.allocator, slice);
                }
                size_data = try size_buf.toOwnedSlice(self.allocator);
            } else {
                varuint_strings = true;
            }
        }

        var cr = ColumnReader{
            .allocator = self.allocator,
            .col = col,
            .row_count = rows_loaded,
            .data = try data_buf.toOwnedSlice(self.allocator),
            .size_data = size_data,
            .cursor = 0,
            .size_cursor = 0,
            .rows_read = 0,
            .varuint_strings = varuint_strings,
        };
        const skip_rows: usize = @intCast(start_row - @as(u64, @intCast(first_gran)) * GRANULE_SIZE);
        try cr.seekRows(skip_rows);
        cr.row_count = cr.rows_read + (end_row - start_row);
        return cr;
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
    varuint_strings: bool = false,

    pub fn deinit(self: *ColumnReader) void {
        self.allocator.free(self.data);
        if (self.size_data) |sd| self.allocator.free(sd);
    }

    pub fn seekRows(self: *ColumnReader, n: usize) !void {
        if (n == 0) return;
        switch (self.col.ty) {
            .text, .char, .low_card => _ = try self.skipStrings(n),
            else => {
                const width: usize = types.chFixedWidth(self.col.ty) orelse return error.NotAFixedColumn;
                const remaining = self.row_count - self.rows_read;
                const count = @min(n, remaining);
                const needed = count * width;
                if (self.cursor + needed > self.data.len) return error.UnexpectedEndOfData;
                self.cursor += needed;
                self.rows_read += count;
            },
        }
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
                .date => @as(i64, std.mem.readInt(u16, slice[0..2], .little)),
                .int32 => @as(i64, std.mem.readInt(i32, slice[0..4], .little)),
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
            .text, .char, .low_card => {},
            else => return error.NotAStringColumn,
        }
        const remaining_rows = self.row_count - self.rows_read;
        const count = @min(n, remaining_rows);
        var read: usize = 0;

        while (read < count) : (read += 1) {
            const len: usize = if (self.varuint_strings) blk: {
                const r = readVarUIntLocal(self.data[self.cursor..]) orelse return error.UnexpectedEndOfData;
                self.cursor += r[1];
                break :blk r[0];
            } else blk: {
                const sd = self.size_data orelse return error.MissingSizeStream;
                if (self.size_cursor + 8 > sd.len) return error.UnexpectedEndOfData;
                const l: usize = @intCast(std.mem.readInt(u64, sd[self.size_cursor..][0..8], .little));
                self.size_cursor += 8;
                break :blk l;
            };
            if (len > string_codec.MAX_STRING_LEN) return error.StringTooLarge;
            if (self.cursor + len > self.data.len) return error.UnexpectedEndOfData;
            const s = self.data[self.cursor .. self.cursor + len];
            self.cursor += len;
            try callback(ctx, s);
        }
        self.rows_read += count;
        return count;
    }

    /// Skip `n` rows of a string column without invoking any callback.
    /// Advances internal cursors identically to readStrings but discards the data.
    pub fn skipStrings(self: *ColumnReader, n: usize) !usize {
        switch (self.col.ty) {
            .text, .char, .low_card => {},
            else => return error.NotAStringColumn,
        }
        const remaining_rows = self.row_count - self.rows_read;
        const count = @min(n, remaining_rows);
        var skipped: usize = 0;
        while (skipped < count) : (skipped += 1) {
            const len: usize = if (self.varuint_strings) blk: {
                const r = readVarUIntLocal(self.data[self.cursor..]) orelse return error.UnexpectedEndOfData;
                self.cursor += r[1];
                break :blk r[0];
            } else blk: {
                const sd = self.size_data orelse return error.MissingSizeStream;
                if (self.size_cursor + 8 > sd.len) return error.UnexpectedEndOfData;
                const l: usize = @intCast(std.mem.readInt(u64, sd[self.size_cursor..][0..8], .little));
                self.size_cursor += 8;
                break :blk l;
            };
            if (self.cursor + len > self.data.len) return error.UnexpectedEndOfData;
            self.cursor += len;
        }
        self.rows_read += count;
        return count;
    }

    /// Read `n` rows of an Array(String) column.
    ///
    /// On-disk format (written by ZigHouse's part writer via consumeNativeTextRows):
    ///   size.bin[i]: u64 LE byte-length of the concatenated element data for row i
    ///   bin: concatenated blobs for all rows; each row's blob = repeated (varint(len) + bytes)
    ///        until the byte-length is exhausted.
    ///
    /// The callback receives (ctx, [][]const u8) — a slice of strings for each row.
    /// String slices point into `self.data`; array slices are allocated from `arena`.
     pub fn readArrayStrings(
        self: *ColumnReader,
        n: usize,
        arena: std.mem.Allocator,
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
            // Read byte-length of this row's element blob from size sub-stream
            if (self.size_cursor + 8 > sd.len) return error.UnexpectedEndOfData;
            const blob_len: usize = @intCast(std.mem.readInt(u64, sd[self.size_cursor..][0..8], .little));
            self.size_cursor += 8;
            
            if (self.cursor + blob_len > self.data.len) return error.UnexpectedEndOfData;
            const blob = self.data[self.cursor .. self.cursor + blob_len];
            self.cursor += blob_len;

            // Detect storage format:
            // Format A (\x01-sentinel): \x01 + elements joined by \x0c
            // Format B (varuint):       varuint(len) + bytes per element
            if (blob.len > 0 and blob[0] == 0x01) {
                // Format A: decode \x01...\x0c... blob
                const content = blob[1..];
                if (content.len == 0) {
                    const elems: [][]const u8 = &.{};
                    try callback(ctx, elems);
                } else {
                    var it = std.mem.splitScalar(u8, content, '\x0c');
                    var elem_list: std.ArrayListUnmanaged([]const u8) = .empty;
                    while (it.next()) |elem| {
                        try elem_list.append(arena, try arena.dupe(u8, elem));
                    }
                    try callback(ctx, try elem_list.toOwnedSlice(arena));
                }
            } else {
                // Format B: varuint(len) + bytes per element
                var elem_count: usize = 0;
                {
                    var off: usize = 0;
                    while (off < blob.len) {
                        const r = readVarUIntLocal(blob[off..]) orelse return error.UnexpectedEndOfData;
                        const slen = r[0];
                        const lb = r[1];
                        off += lb + slen;
                        elem_count += 1;
                    }
                }
                const elems = try arena.alloc([]const u8, elem_count);
                {
                    var off: usize = 0;
                    var ei: usize = 0;
                    while (off < blob.len) {
                        const r = readVarUIntLocal(blob[off..]) orelse return error.UnexpectedEndOfData;
                        const slen = r[0];
                        const lb = r[1];
                        off += lb;
                        elems[ei] = try arena.dupe(u8, blob[off .. off + slen]);
                        off += slen;
                        ei += 1;
                    }
                }
                try callback(ctx, elems);
            }
        }
        self.rows_read += count;
        return count;
    }

    /// Skip `n` rows of an Array(String) column without decoding element data.
    pub fn skipArrayStrings(self: *ColumnReader, n: usize) !usize {
        switch (self.col.ty) {
            .text, .char => {},
            else => return error.NotAStringColumn,
        }
        const sd = self.size_data orelse return error.MissingSizeStream;
        const remaining_rows = self.row_count - self.rows_read;
        const count = @min(n, remaining_rows);
        var skipped: usize = 0;
        while (skipped < count) : (skipped += 1) {
            if (self.size_cursor + 8 > sd.len) return error.UnexpectedEndOfData;
            const blob_len: usize = @intCast(std.mem.readInt(u64, sd[self.size_cursor..][0..8], .little));
            self.size_cursor += 8;
            if (self.cursor + blob_len > self.data.len) return error.UnexpectedEndOfData;
            self.cursor += blob_len;
        }
        self.rows_read += count;
        return count;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Read a variable-length unsigned integer (LEB128) from buf.
/// Returns {value, bytes_consumed} or null if buf is empty or malformed.
fn readVarUIntLocal(buf: []const u8) ?struct { usize, usize } {
    if (buf.len == 0) return null;
    var val: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < buf.len and i < 10) : (i += 1) {
        const b = buf[i];
        val |= @as(usize, b & 0x7F) << shift;
        if (b & 0x80 == 0) return .{ val, i + 1 };
        shift += 7;
    }
    return null;
}

// ── Compact part read/write ────────────────────────────────────────────────────
//
// Compact part layout (CH 22+ default for small parts):
//   data.bin        — all columns concatenated; each column/granule = one CH block
//   data.cmrk4      — single CH block (ZSTD or LZ4); decompressed = n_substreams × 16B marks
//   primary.cidx    — single CH block (ZSTD or LZ4); same payload as primary.idx
//   columns.txt     — same format as wide part
//   columns_substreams.txt — substream listing per column
//   count.txt       — row count
//   checksums.txt   — file checksums (binary, CH format v4)
//   serialization.json — per-column serialization info
//   metadata_version.txt / default_compression_codec.txt / format_version.txt
//
// Substream ordering in data.bin and cmrk4:
//   For each column (in columns.txt declaration order):
//     - Fixed-width columns: 1 substream = the column data
//     - String columns: 2 substreams: <col>.size then <col> (raw bytes)

/// Open a compact-format MergeTree part for reading.
///
/// Handles parts written by ClickHouse server (ZSTD-compressed cmrk4/cidx)
/// or by ZigHouse (LZ4-compressed).
pub const CompactOpenedPart = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    part_dir: []const u8,
    table: schema.Table,
    row_count: u64,
    /// Compact-part logical marks from data.cmrk4. String columns have two
    /// logical substreams (sizes + bytes) sharing one physical compressed block.
    mark_table: marks.CompactMarkTable,
    data_file_size: u64,
    /// Number of logical substreams total.
    n_substreams: usize,
    /// n_granules (derived from row_count and GRANULE_SIZE).
    n_granules: usize,
    /// Substream index for each column's first substream.
    col_substream_start: []usize,
    /// Actual substream count per column (from columns_substreams.txt or schema).
    col_substream_count: []usize,

    pub fn open(
        io: std.Io,
        allocator: std.mem.Allocator,
        part_dir: []const u8,
        table: schema.Table,
    ) !CompactOpenedPart {
        // Read count.txt
        const count_path = try std.fmt.allocPrint(allocator, "{s}/count.txt", .{part_dir});
        defer allocator.free(count_path);
        const row_count = try count_txt.readPath(allocator, io, count_path);

        const n_gran: usize = if (row_count == 0) 0 else (row_count + GRANULE_SIZE - 1) / GRANULE_SIZE;

        const n_cols = table.columns.len;

        // Compute logical substream count and per-column mark start.
        // String/char: two logical marks, but both point into one combined block.
        // LowCardinality: two physical/logical blocks (dict + index) in ZH format,
        // but ClickHouse 26.x uses three (dict_prefix + dict + index).
        // Override with actual counts from columns_substreams.txt if present.
        const col_ss = try allocator.alloc(usize, n_cols);
        errdefer allocator.free(col_ss);
        var col_substream_count = try allocator.alloc(usize, n_cols);
        errdefer allocator.free(col_substream_count);
        var total_substreams: usize = 0;
        for (0..n_cols) |ci| {
            col_ss[ci] = total_substreams;
            const n: usize = switch (table.columns[ci].ty) {
                .text, .char, .low_card => 2,
                else => 1,
            };
            col_substream_count[ci] = n;
            total_substreams += n;
        }

        // Override with actual counts from columns_substreams.txt if present.
        {
            const sub_path = std.fmt.allocPrint(allocator, "{s}/columns_substreams.txt", .{part_dir}) catch null;
            if (sub_path) |sp| {
                defer allocator.free(sp);
                const subs_text = std.Io.Dir.cwd().readFileAlloc(io, sp, allocator, .limited(65536)) catch null;
                if (subs_text) |st| {
                    defer allocator.free(st);
                    total_substreams = 0;
                    var line_iter = std.mem.splitScalar(u8, st, '\n');
                    _ = line_iter.next(); // version line
                    _ = line_iter.next(); // N columns line
                    for (0..n_cols) |ci| {
                        const line = line_iter.next() orelse break;
                        const N = std.fmt.parseInt(usize, std.mem.trim(u8, line[0..std.mem.indexOfScalar(u8, line, ' ') orelse line.len], " \t"), 10) catch 2;
                        col_substream_count[ci] = N;
                        col_ss[ci] = total_substreams;
                        total_substreams += N;
                        for (0..N) |_| _ = line_iter.next() orelse break;
                    }
                }
            }
        }

        // Adjust column starts to skip extra substreams (sparse.idx, etc.).
        // Do NOT skip dict_prefix for LC — the reader handles 3-stream format.
        for (0..n_cols) |ci| {
            const expected: usize = switch (table.columns[ci].ty) {
                .text, .char, .low_card => 2,
                else => 1,
            };
            if (col_substream_count[ci] > expected and table.columns[ci].ty != .low_card) {
                col_ss[ci] += col_substream_count[ci] - expected;
            }
        }

        const cmrk_path = try std.fmt.allocPrint(allocator, "{s}/data.cmrk4", .{part_dir});
        defer allocator.free(cmrk_path);
        const cmrk_bytes = try std.Io.Dir.cwd().readFileAlloc(io, cmrk_path, allocator, .limited(std.math.maxInt(usize)));
        defer allocator.free(cmrk_bytes);
        const mark_table = try marks.readCmrk4Adaptive(allocator, cmrk_bytes, total_substreams);
        errdefer mark_table.deinit(allocator);

        const bin_path = try std.fmt.allocPrint(allocator, "{s}/data.bin", .{part_dir});
        defer allocator.free(bin_path);
        const bin_file = try std.Io.Dir.cwd().openFile(io, bin_path, .{});
        defer bin_file.close(io);
        const bin_stat = try bin_file.stat(io);

        if (n_gran != mark_table.n_granules) return error.InvalidMarkCount;

        return .{
            .allocator = allocator,
            .io = io,
            .part_dir = part_dir,
            .table = table,
            .row_count = row_count,
            .mark_table = mark_table,
            .data_file_size = bin_stat.size,
            .n_substreams = total_substreams,
            .n_granules = n_gran,
            .col_substream_start = col_ss,
            .col_substream_count = col_substream_count,
        };
    }

    pub fn deinit(self: *CompactOpenedPart) void {
        self.mark_table.deinit(self.allocator);
        self.allocator.free(self.col_substream_start);
        self.allocator.free(self.col_substream_count);
    }

    fn blockRange(self: *CompactOpenedPart, mark_idx: usize) !struct { u64, u64 } {
        if (mark_idx >= self.mark_table.marks.len) return error.InvalidMarkCount;
        const start = self.mark_table.marks[mark_idx].offset_in_file;
        var end = self.mark_table.eof_offset;
        var i = mark_idx + 1;
        while (i < self.mark_table.marks.len) : (i += 1) {
            const off = self.mark_table.marks[i].offset_in_file;
            if (off > start) {
                end = off;
                break;
            }
        }
        if (end < start or end > self.data_file_size) return error.InvalidMarkCount;
        return .{ start, end - start };
    }

    fn readBlockAt(self: *CompactOpenedPart, file: std.Io.File, mark_idx: usize) ![]u8 {
        const range = try self.blockRange(mark_idx);
        const len: usize = @intCast(range[1]);
        const compressed = try self.allocator.alloc(u8, len);
        defer self.allocator.free(compressed);
        const read_n = try file.readPositionalAll(self.io, compressed, range[0]);
        if (read_n != compressed.len) return error.UnexpectedEndOfData;
        var reader = std.Io.Reader.fixed(compressed);
        return block.readBlock(self.allocator, &reader);
    }

    /// Return a ColumnReader for column at `col_idx`.
    /// Collects and concatenates all granule blocks for this column's substreams.
    pub fn columnReader(self: *CompactOpenedPart, col_idx: usize) !ColumnReader {
        return self.columnReaderGranules(col_idx, 0, self.n_granules);
    }

    /// Return a ColumnReader that only reads granules intersecting
    /// `[start_row, start_row + row_count)`. The returned reader is positioned
    /// at `start_row`, so callers can read exactly `row_count` rows.
    pub fn columnReaderRange(self: *CompactOpenedPart, col_idx: usize, start_row: u64, row_count: usize) !ColumnReader {
        if (row_count == 0 or start_row >= self.row_count) {
            return self.emptyColumnReader(col_idx);
        }
        const end_row = @min(start_row + row_count, self.row_count);
        const first_gran: usize = @intCast(start_row / GRANULE_SIZE);
        const last_gran: usize = @intCast((end_row - 1) / GRANULE_SIZE);
        var cr = try self.columnReaderGranules(col_idx, first_gran, last_gran - first_gran + 1);
        const skip_rows: usize = @intCast(start_row - @as(u64, @intCast(first_gran)) * GRANULE_SIZE);
        try cr.seekRows(skip_rows);
        cr.row_count = cr.rows_read + (end_row - start_row);
        return cr;
    }

    fn emptyColumnReader(self: *CompactOpenedPart, col_idx: usize) !ColumnReader {
        return ColumnReader{
            .allocator = self.allocator,
            .col = self.table.columns[col_idx],
            .row_count = 0,
            .data = try self.allocator.alloc(u8, 0),
            .size_data = null,
            .cursor = 0,
            .size_cursor = 0,
            .rows_read = 0,
        };
    }

    fn columnReaderGranules(self: *CompactOpenedPart, col_idx: usize, first_granule: usize, granule_count: usize) !ColumnReader {
        const col = self.table.columns[col_idx];
        const ss_start = self.col_substream_start[col_idx];

        if (self.row_count == 0 or granule_count == 0 or first_granule >= self.n_granules)
            return self.emptyColumnReader(col_idx);

        // Concatenate all granule blocks for this column's data (and size if String)
        var data_buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer data_buf.deinit(self.allocator);
        var size_buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer size_buf.deinit(self.allocator);

        const bin_path = try std.fmt.allocPrint(self.allocator, "{s}/data.bin", .{self.part_dir});
        defer self.allocator.free(bin_path);
        const bin_file = try std.Io.Dir.cwd().openFile(self.io, bin_path, .{});
        defer bin_file.close(self.io);

        const granule_end = @min(first_granule + granule_count, self.n_granules);
        var rows_loaded: u64 = 0;
        if (col.ty == .low_card and first_granule > 0) {
            const dict_idx = ss_start;
            const dict = try self.readBlockAt(bin_file, dict_idx);
            defer self.allocator.free(dict);
            try data_buf.appendSlice(self.allocator, dict);
        }
        for (first_granule..granule_end) |g| {
            const n_rows_in_gran = self.mark_table.granularities[g];
            rows_loaded += n_rows_in_gran;
            // In CH Compact format: one physical block per column per granule.
            // For String columns: block = sizes_bytes(n_rows*8) + raw_bytes.
            const block_idx = g * self.n_substreams + ss_start;
            switch (col.ty) {
                .text, .char => {
                    const combined = try self.readBlockAt(bin_file, block_idx);
                    defer self.allocator.free(combined);
                    const sizes_bytes = n_rows_in_gran * 8;
                    if (sizes_bytes > combined.len) return error.UnexpectedEndOfData;
                    try size_buf.appendSlice(self.allocator, combined[0..sizes_bytes]);
                    try data_buf.appendSlice(self.allocator, combined[sizes_bytes..]);
                },
                .low_card => {
                    // LC: 3 substreams per granule in CH 26.x (dict_prefix + dict + index),
                    // or 2 substreams in old format (dict + index).
                    const ss_n = self.col_substream_count[col_idx];
                    if (ss_n == 3) {
                        // CH 26.x 3-substream LC: dict_prefix (granule 0), dict+index per granule.
                        // dict_prefix is the base dictionary (version + num_keys + keys).
                        if (g == 0) {
                            // Read dict_prefix as the initial dictionary blob
                            const dict_prefix = try self.readBlockAt(bin_file, block_idx);
                            defer self.allocator.free(dict_prefix);
                            try data_buf.appendSlice(self.allocator, dict_prefix);
                        }
                        // dict block: skip (dict updates are included in index_addl_keys)
                        // index block: IndexesSerializationType + optional addl keys + num_rows + index data
                        const index_raw = try self.readBlockAt(bin_file, block_idx + 2);
                        defer self.allocator.free(index_raw);
                        var ip: usize = 0;
                        if (ip + 8 > index_raw.len) return error.InvalidIndexStream;
                        const flags = std.mem.readInt(u64, index_raw[ip..][0..8], .little); ip += 8;
                        if ((flags & 0x200) != 0) {
                            // HasAdditionalKeys → read into dict
                            if (ip + 8 > index_raw.len) return error.InvalidIndexStream;
                            const num_keys = std.mem.readInt(u64, index_raw[ip..][0..8], .little); ip += 8;
                            if (ip + num_keys * 8 > index_raw.len) return error.InvalidIndexStream;
                            // Build old-format dict block: version(1) + num_keys + key data
                            try data_buf.ensureUnusedCapacity(self.allocator, 16 + num_keys * 8);
                            data_buf.appendSliceAssumeCapacity(&[_]u8{1,0,0,0,0,0,0,0}); // version=1
                            var num_bytes: [8]u8 = undefined;
                            std.mem.writeInt(u64, &num_bytes, num_keys, .little);
                            data_buf.appendSliceAssumeCapacity(&num_bytes);
                            // Copy key data
                            try data_buf.appendSlice(self.allocator, index_raw[ip..ip + num_keys * 8]);
                            ip += num_keys * 8;
                        }
                        // Read num_rows and skip it (index data follows)
                        if (ip + 8 > index_raw.len) return error.InvalidIndexStream;
                        _ = std.mem.readInt(u64, index_raw[ip..][0..8], .little); ip += 8;
                        // The rest is index data (appended to size_buf)
                        try size_buf.appendSlice(self.allocator, index_raw[ip..]);
                    } else {
                        // Old 2-substream format
                        if (g == 0) {
                            const dict = try self.readBlockAt(bin_file, block_idx);
                            defer self.allocator.free(dict);
                            try data_buf.appendSlice(self.allocator, dict);
                        }
                        const index = try self.readBlockAt(bin_file, block_idx + 1);
                        defer self.allocator.free(index);
                        try size_buf.appendSlice(self.allocator, index);
                    }
                },
                else => {
                    const data = try self.readBlockAt(bin_file, block_idx);
                    defer self.allocator.free(data);
                    try data_buf.appendSlice(self.allocator, data);
                },
            }
        }

        if (col.ty == .low_card) {
            // data_buf = dict stream, size_buf = all index payloads concatenated
            const dict_raw = try data_buf.toOwnedSlice(self.allocator);
            defer self.allocator.free(dict_raw);
            const index_raw = try size_buf.toOwnedSlice(self.allocator);
            defer self.allocator.free(index_raw);
            const deserialized = try low_card.deserializeToStringBuf(
                self.allocator, dict_raw, index_raw, rows_loaded,
            );
            return ColumnReader{
                .allocator = self.allocator,
                .col = col,
                .row_count = rows_loaded,
                .data = deserialized.data,
                .size_data = deserialized.size_data,
                .cursor = 0,
                .size_cursor = 0,
                .rows_read = 0,
            };
        }

        const size_data: ?[]u8 = switch (col.ty) {
            .text, .char => try size_buf.toOwnedSlice(self.allocator),
            else => blk: {
                size_buf.deinit(self.allocator);
                break :blk null;
            },
        };

        return ColumnReader{
            .allocator = self.allocator,
            .col = col,
            .row_count = rows_loaded,
            .data = try data_buf.toOwnedSlice(self.allocator),
            .size_data = size_data,
            .cursor = 0,
            .size_cursor = 0,
            .rows_read = 0,
        };
    }
};

// ── Compact part writer ────────────────────────────────────────────────────────

/// Write a ClickHouse-compatible compact MergeTree part directory.
///
/// Layout written:
///   data.bin                     — all substreams as sequential CH blocks (LZ4)
///   data.cmrk4                   — compressed marks (LZ4 block of 16-byte entries)
///   primary.cidx                 — compressed primary index (LZ4 block)
///   columns.txt                  — column declarations
///   columns_substreams.txt       — substream listing
///   count.txt                    — row count
///   checksums.txt                — checksums (CH format v4)
///   serialization.json           — minimal default serialization
///   metadata_version.txt         — "1"
///   default_compression_codec.txt — "CODEC(LZ4)"
///   format_version.txt           — "1"
pub const CompactPart = struct {
    const ManagedList = std.array_list.Managed(u8);
    const ManagedMarkList = std.array_list.Managed(marks.CompactMark);
    const ManagedCsList = std.array_list.Managed(checksums.FileChecksum);

    allocator: std.mem.Allocator,
    io: std.Io,
    part_dir: []u8,
    table: schema.Table,
    /// Per-column buffered row data (uncompressed).
    col_bufs: []ManagedList,
    /// For String columns: per-column size stream buffer (one u64 LE per row = string byte length).
    size_bufs: []?ManagedList,
    /// For LowCardinality columns: per-column DictBuilder pointer (null for non-LC cols).
    lc_builders: []?*low_card.DictBuilder,
    row_count: u64,
    pk_col_idx: usize,
    /// Compression codec for data.bin.
    codec: u8 = block.METHOD_LZ4,

    pub fn open(
        io: std.Io,
        allocator: std.mem.Allocator,
        part_dir: []const u8,
        table: schema.Table,
        codec: u8,
    ) !CompactPart {
        const dir = try allocator.dupe(u8, part_dir);
        errdefer allocator.free(dir);

        try std.Io.Dir.cwd().createDirPath(io, part_dir);

        const resolved_pk_col_idx: usize = blk: {
            if (table.sort_keys.len > 0) {
                for (table.columns, 0..) |col, i| {
                    if (std.mem.eql(u8, col.name, table.sort_keys[0])) break :blk i;
                }
            }
            break :blk 0;
        };

        const col_bufs = try allocator.alloc(ManagedList, table.columns.len);
        for (col_bufs) |*b| b.* = ManagedList.init(allocator);

        const size_bufs = try allocator.alloc(?ManagedList, table.columns.len);
        for (size_bufs, table.columns) |*sb, col| {
            sb.* = switch (col.ty) {
                .text, .char => ManagedList.init(allocator),
                else => null,
            };
        }

        const lc_builders = try allocator.alloc(?*low_card.DictBuilder, table.columns.len);
        for (lc_builders, table.columns) |*lb, col| {
            if (col.ty == .low_card) {
                const b = try allocator.create(low_card.DictBuilder);
                b.* = low_card.DictBuilder.init(allocator);
                lb.* = b;
            } else {
                lb.* = null;
            }
        }

        return .{
            .allocator = allocator,
            .io = io,
            .part_dir = dir,
            .table = table,
            .col_bufs = col_bufs,
            .size_bufs = size_bufs,
            .lc_builders = lc_builders,
            .row_count = 0,
            .pk_col_idx = resolved_pk_col_idx,
            .codec = codec,
        };
    }

    pub fn deinit(self: *CompactPart) void {
        for (self.col_bufs) |*b| b.deinit();
        for (self.size_bufs) |*sb| if (sb.*) |*b| b.deinit();
        for (self.lc_builders) |lb| {
            if (lb) |b| {
                b.deinit();
                self.allocator.destroy(b);
            }
        }
        self.allocator.free(self.col_bufs);
        self.allocator.free(self.size_bufs);
        self.allocator.free(self.lc_builders);
        self.allocator.free(self.part_dir);
    }

    /// Append a batch of fixed-width values for column `col_idx`.
    /// Also increments row_count if col_idx == 0.
    pub fn appendFixedBatch(self: *CompactPart, col_idx: usize, values: []const i64) !void {
        const col = self.table.columns[col_idx];
        const width: usize = types.chFixedWidth(col.ty) orelse return error.NotAFixedColumn;
        const buf = &self.col_bufs[col_idx];
        for (values) |v| {
            const old_len = buf.items.len;
            try buf.resize(old_len + width);
            const dest = buf.items[old_len..][0..width];
            switch (col.ty) {
                .int8 => dest[0] = @bitCast(@as(i8, @intCast(v))),
                .int16 => std.mem.writeInt(i16, dest[0..2], @intCast(v), .little),
                .date => std.mem.writeInt(u16, dest[0..2], @intCast(v), .little),
                .int32 => std.mem.writeInt(i32, dest[0..4], @intCast(v), .little),
                .int64, .timestamp => std.mem.writeInt(i64, dest[0..8], v, .little),
                .float32 => std.mem.writeInt(u32, dest[0..4], @truncate(@as(u64, @bitCast(v))), .little),
                .float64 => std.mem.writeInt(u64, dest[0..8], @bitCast(v), .little),
                else => return error.NotAFixedColumn,
            }
        }
        if (col_idx == 0) self.row_count += values.len;
    }

    /// Append a single string value for column `col_idx`.
    /// For .text/.char columns, stores in size_bufs+col_bufs.
    /// For .low_card columns, stores in the lc_builder dict.
    pub fn appendString(self: *CompactPart, col_idx: usize, s: []const u8) !void {
        const col = self.table.columns[col_idx];
        switch (col.ty) {
            .text, .char => {
                const sb = &(self.size_bufs[col_idx] orelse return error.NotAStringColumn);
                var len_buf: [8]u8 = undefined;
                std.mem.writeInt(u64, &len_buf, s.len, .little);
                try sb.appendSlice(&len_buf);
                try self.col_bufs[col_idx].appendSlice(s);
            },
            .low_card => {
                const b = self.lc_builders[col_idx] orelse return error.NotALCColumn;
                try b.append(s);
            },
            else => return error.NotAStringColumn,
        }
    }

    /// Append a batch of strings to a string column.
    pub fn appendStrBatch(self: *CompactPart, col_idx: usize, strings: []const []const u8) !void {
        for (strings) |s| try self.appendString(col_idx, s);
    }

    /// Set total row count explicitly — required when using appendFixedBatch/appendStrBatch
    /// (since those don't update row_count), before calling finish().
    pub fn setRowCount(self: *CompactPart, n: u64) void {
        self.row_count = n;
    }

    /// Finalize: write all compact part files to disk.
    pub fn finish(self: *CompactPart) !void {
        const n_gran: usize = if (self.row_count == 0) 0 else (self.row_count + GRANULE_SIZE - 1) / GRANULE_SIZE;

        // Populate granule_starts for LC builders (based on GRANULE_SIZE boundaries)
        for (self.lc_builders) |lb| {
            if (lb) |b| {
                try b.granule_starts.resize(b.allocator, n_gran);
                for (0..n_gran) |g| {
                    b.granule_starts.items[g] = @intCast(g * GRANULE_SIZE);
                }
            }
        }

        // n_substreams (logical: String/LC = 2 substreams)
        var n_sub: usize = 0;
        for (self.table.columns) |col| {
            n_sub += switch (col.ty) { .text, .char, .low_card => 2, else => 1 };
        }

        // n_gran already computed above

        // ── data.bin + cmrk4 marks ────────────────────────────────────────────
        var bin_aw = std.Io.Writer.Allocating.init(self.allocator);
        defer bin_aw.deinit();

        var cm_list = ManagedMarkList.init(self.allocator);
        defer cm_list.deinit();

        // Track row count per granule for adaptive marks
        const gran_rows = try self.allocator.alloc(u64, n_gran);
        defer self.allocator.free(gran_rows);

        // Pre-serialize LC column index payloads (per-column, per-granule slices)
        const lc_index_gran = try self.allocator.alloc(?[][]u8, self.table.columns.len);
        defer {
            for (lc_index_gran) |maybe_g| {
                if (maybe_g) |g| {
                    for (g) |b| self.allocator.free(b);
                    self.allocator.free(g);
                }
            }
            self.allocator.free(lc_index_gran);
        }
        for (self.table.columns, 0..) |col, ci| {
            if (col.ty == .low_card) {
                const b = self.lc_builders[ci].?;
                lc_index_gran[ci] = try b.serializeIndexAllGranules(self.allocator);
            } else {
                lc_index_gran[ci] = null;
            }
        }

        for (0..n_gran) |g| {
            const gs = g * GRANULE_SIZE;
            const ge = @min(gs + GRANULE_SIZE, self.row_count);
            gran_rows[g] = ge - gs;

            for (self.table.columns, 0..) |col, ci| {
                switch (col.ty) {
                    .text, .char => {
                        // CH Compact format: String = 1 physical block per granule.
                        // Block contents: sizes (n_rows*8 bytes of u64 LE lengths) + raw bytes.
                        // 2 marks: first at (block_offset, 0) for .size substream,
                        //          second at (block_offset, n_rows*8) for data substream.
                        const sb = self.size_bufs[ci].?.items;
                        const sz_slice = sb[gs * 8 .. ge * 8]; // size stream bytes

                        // Compute data byte range
                        var byte_start: u64 = 0;
                        for (0..gs) |ri| {
                            byte_start += std.mem.readInt(u64, sb[ri * 8 ..][0..8], .little);
                        }
                        var byte_end = byte_start;
                        for (gs..ge) |ri| {
                            byte_end += std.mem.readInt(u64, sb[ri * 8 ..][0..8], .little);
                        }
                        const data_slice = self.col_bufs[ci].items[byte_start..byte_end];

                        // Combined block = sizes + data
                        const combined = try self.allocator.alloc(u8, sz_slice.len + data_slice.len);
                        defer self.allocator.free(combined);
                        @memcpy(combined[0..sz_slice.len], sz_slice);
                        @memcpy(combined[sz_slice.len..], data_slice);

                        const block_offset: u64 = @intCast(bin_aw.writer.end);
                        const n_rows_in_gran = ge - gs;
                        try cm_list.append(.{ .offset_in_file = block_offset, .offset_in_block = 0 });
                        try cm_list.append(.{ .offset_in_file = block_offset, .offset_in_block = @intCast(n_rows_in_gran * 8) });
                        try block.writeBlock(&bin_aw.writer, combined, self.codec);
                    },
                    .low_card => {
                        // LC: 2 physical blocks per granule.
                        // Block 0 (dict substream): full dict bytes only for granule 0, else empty.
                        // Block 1 (index substream): per-granule index payload.
                        const b = self.lc_builders[ci].?;
                        const gran_bufs = lc_index_gran[ci].?;

                        // Dict substream block
                        const dict_offset: u64 = @intCast(bin_aw.writer.end);
                        if (g == 0) {
                            var dict_aw = std.Io.Writer.Allocating.init(self.allocator);
                            defer dict_aw.deinit();
                            try b.serializeDict(&dict_aw.writer);
                            var dict_al = dict_aw.toArrayList();
                            defer dict_al.deinit(self.allocator);
                            try cm_list.append(.{ .offset_in_file = dict_offset, .offset_in_block = 0 });
                            try block.writeBlock(&bin_aw.writer, dict_al.items, self.codec);
                        } else {
                            // Empty dict block for subsequent granules
                            try cm_list.append(.{ .offset_in_file = dict_offset, .offset_in_block = 0 });
                            try block.writeBlock(&bin_aw.writer, &[_]u8{}, self.codec);
                        }

                        // Index substream block
                        const idx_offset: u64 = @intCast(bin_aw.writer.end);
                        try cm_list.append(.{ .offset_in_file = idx_offset, .offset_in_block = 0 });
                        try block.writeBlock(&bin_aw.writer, gran_bufs[g], self.codec);
                    },
                    else => {
                        const width = types.chFixedWidth(col.ty) orelse continue;
                        const bs = gs * width;
                        const be = ge * width;
                        try cm_list.append(.{ .offset_in_file = @intCast(bin_aw.writer.end), .offset_in_block = 0 });
                        try block.writeBlock(&bin_aw.writer, self.col_bufs[ci].items[bs..be], self.codec);
                    },
                }
            }
        }

        var bin_al = bin_aw.toArrayList();
        defer bin_al.deinit(self.allocator);
        try compactWriteFile(self.io, self.part_dir, "data.bin", bin_al.items);

        // cmrk4 — adaptive granularity format: each row = n_sub × 16B marks + 8B granularity,
        // plus one EOF sentinel row.
        {
            var cmrk_aw = std.Io.Writer.Allocating.init(self.allocator);
            defer cmrk_aw.deinit();
            try marks.writeCmrk4(&cmrk_aw.writer, cm_list.items, n_sub, gran_rows, @intCast(bin_al.items.len));
            var cmrk_al = cmrk_aw.toArrayList();
            defer cmrk_al.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "data.cmrk4", cmrk_al.items);
        }

        // ── primary.cidx ──────────────────────────────────────────────────────
        {
            var pk_aw = std.Io.Writer.Allocating.init(self.allocator);
            defer pk_aw.deinit();
            const pk_col = self.table.columns[self.pk_col_idx];
            const width = types.chFixedWidth(pk_col.ty) orelse 0;
            var pk_raw = std.ArrayList(u8).empty;
            defer pk_raw.deinit(self.allocator);
            if (width > 0) {
                const cd = self.col_bufs[self.pk_col_idx].items;
                for (0..n_gran) |g| {
                    const off = g * GRANULE_SIZE * width;
                    if (off + width <= cd.len)
                        try pk_raw.appendSlice(self.allocator, cd[off .. off + width]);
                }
            }
            var cidx_aw = std.Io.Writer.Allocating.init(self.allocator);
            defer cidx_aw.deinit();
            try block.writeBlock(&cidx_aw.writer, pk_raw.items, block.METHOD_LZ4);
            var cidx_al = cidx_aw.toArrayList();
            defer cidx_al.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "primary.cidx", cidx_al.items);
        }

        // ── count.txt ─────────────────────────────────────────────────────────
        {
            var buf: [32]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{self.row_count}) catch unreachable;
            try compactWriteFile(self.io, self.part_dir, "count.txt", s);
        }

        // ── columns.txt ───────────────────────────────────────────────────────
        {
            const ch_cols = try columns_txt.fromTable(self.allocator, self.table);
            defer columns_txt.freeChColumns(self.allocator, ch_cols);
            var caw = std.Io.Writer.Allocating.init(self.allocator);
            defer caw.deinit();
            try columns_txt.write(&caw.writer, ch_cols);
            var cal = caw.toArrayList();
            defer cal.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "columns.txt", cal.items);
        }

        // ── columns_substreams.txt ─────────────────────────────────────────────
        {
            var ssw = std.Io.Writer.Allocating.init(self.allocator);
            defer ssw.deinit();
            try ssw.writer.print("columns substreams version: 1\n{d} columns:\n", .{self.table.columns.len});
            for (self.table.columns) |col| {
                switch (col.ty) {
                    .text, .char => try ssw.writer.print("2 substreams for column `{s}`:\n\t{s}.size\n\t{s}\n", .{ col.name, col.name, col.name }),
                    .low_card => try ssw.writer.print("2 substreams for column `{s}`:\n\t{s}.dict\n\t{s}\n", .{ col.name, col.name, col.name }),
                    else => try ssw.writer.print("1 substreams for column `{s}`:\n\t{s}\n", .{ col.name, col.name }),
                }
            }
            var ssal = ssw.toArrayList();
            defer ssal.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "columns_substreams.txt", ssal.items);
        }

        // ── serialization.json ────────────────────────────────────────────────
        // CH 26.5 requires: version=1, types_serialization_versions, columns sorted alphabetically.
        {
            // Build sorted column name list
            const sorted_cols = try self.allocator.dupe(schema.Column, self.table.columns);
            defer self.allocator.free(sorted_cols);
            std.mem.sort(schema.Column, sorted_cols, {}, struct {
                fn lessThan(_: void, a: schema.Column, b: schema.Column) bool {
                    return std.mem.order(u8, a.name, b.name) == .lt;
                }
            }.lessThan);

            // Check if any String or LC columns exist (need types_serialization_versions)
            var has_string = false;
            var has_lc = false;
            for (self.table.columns) |col| {
                if (col.ty == .text or col.ty == .char) has_string = true;
                if (col.ty == .low_card) has_lc = true;
            }

            var sjw = std.Io.Writer.Allocating.init(self.allocator);
            defer sjw.deinit();
            try sjw.writer.print("{{\"columns\":[", .{});
            for (sorted_cols, 0..) |col, i| {
                if (i > 0) try sjw.writer.print(",", .{});
                try sjw.writer.print("{{\"kind\":\"Default\",\"name\":\"{s}\",\"num_defaults\":0,\"num_rows\":{d}}}", .{ col.name, self.row_count });
            }
            if (has_string and has_lc) {
                try sjw.writer.print("],\"propagate_types_serialization_versions_to_nested_types\":true,\"types_serialization_versions\":{{\"low_cardinality\":1,\"string\":1}},\"version\":1}}", .{});
            } else if (has_string) {
                try sjw.writer.print("],\"propagate_types_serialization_versions_to_nested_types\":true,\"types_serialization_versions\":{{\"string\":1}},\"version\":1}}", .{});
            } else if (has_lc) {
                try sjw.writer.print("],\"propagate_types_serialization_versions_to_nested_types\":true,\"types_serialization_versions\":{{\"low_cardinality\":1}},\"version\":1}}", .{});
            } else {
                try sjw.writer.print("],\"version\":1}}", .{});
            }
            var sjal = sjw.toArrayList();
            defer sjal.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "serialization.json", sjal.items);
        }

        // ── static metadata files ─────────────────────────────────────────────
        try compactWriteFile(self.io, self.part_dir, "metadata_version.txt", "1\n");
        const codec_str: []const u8 = if (self.codec == block.METHOD_ZSTD) "CODEC(ZSTD(1))\n" else "CODEC(LZ4)\n";
        try compactWriteFile(self.io, self.part_dir, "default_compression_codec.txt", codec_str);
        // Note: format_version.txt is NOT written — CH 26.5 compact parts don't use it

        // ── checksums.txt ─────────────────────────────────────────────────────
        {
            const file_names = [_][]const u8{
                "count.txt",
                "columns.txt",
                "columns_substreams.txt",
                "data.bin",
                "data.cmrk4",
                "default_compression_codec.txt",
                "metadata_version.txt",
                "primary.cidx",
                "serialization.json",
            };
            var cs_entries = ManagedCsList.init(self.allocator);
            defer cs_entries.deinit();
            const cwd = std.Io.Dir.cwd();
            for (file_names) |fname| {
                const fpath = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.part_dir, fname });
                defer self.allocator.free(fpath);
                const fdata = cwd.readFileAlloc(self.io, fpath, self.allocator, .limited(std.math.maxInt(usize))) catch continue;
                defer self.allocator.free(fdata);
                const fhash = checksums.hashFile(fdata);
                try cs_entries.append(.{
                    .name = fname,
                    .file_size = @intCast(fdata.len),
                    .file_hash = fhash,
                    .is_compressed = false,
                });
            }
            // Sort by name (already sorted above, but be safe)
            std.mem.sort(checksums.FileChecksum, cs_entries.items, {}, checksumLessThan);
            var csaw = std.Io.Writer.Allocating.init(self.allocator);
            defer csaw.deinit();
            try checksums.write(self.allocator, &csaw.writer, cs_entries.items);
            var csal = csaw.toArrayList();
            defer csal.deinit(self.allocator);
            try compactWriteFile(self.io, self.part_dir, "checksums.txt", csal.items);
        }
    }
};

fn compactWriteFile(io: std.Io, dir: []const u8, name: []const u8, data: []const u8) !void {
    // Build path on stack where possible
    var path_buf: [4096]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, name }) catch return error.PathTooLong;
    var f = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
    defer f.close(io);
    try f.writeStreamingAll(io, data);
}

test "CompactPart write + read round-trip" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_compact_part";

    const cols = [_]schema.Column{
        .{ .name = "event_date", .ty = .date },
        .{ .name = "user_id", .ty = .int32 },
        .{ .name = "duration", .ty = .int64 },
        .{ .name = "url", .ty = .text },
    };
    const table = schema.Table{ .name = "test_compact", .columns = &cols };

    // Write
    {
        var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();

        const dates   = [_]i64{ 19723, 19723, 19724 };
        const users   = [_]i64{ 1, 2, 3 };
        const durs    = [_]i64{ 3500, 1200, 4200 };

        try cp.appendFixedBatch(0, &dates);
        try cp.appendFixedBatch(1, &users);
        try cp.appendFixedBatch(2, &durs);
        try cp.appendString(3, "https://example.com/home");
        try cp.appendString(3, "https://example.com/about");
        try cp.appendString(3, "https://example.com/contact");

        try cp.finish();
    }

    // Read back
    {
        var cop = try CompactOpenedPart.open(io, allocator, part_dir, table);
        defer cop.deinit();

        try std.testing.expectEqual(@as(u64, 3), cop.row_count);

        // Read fixed column: event_date
        var cr0 = try cop.columnReader(0);
        defer cr0.deinit();
        var date_vals: [3]i64 = undefined;
        const n0 = try cr0.readFixed(&date_vals);
        try std.testing.expectEqual(@as(usize, 3), n0);
        try std.testing.expectEqual(@as(i64, 19723), date_vals[0]);
        try std.testing.expectEqual(@as(i64, 19724), date_vals[2]);

        // Read string column: url
        var cr3 = try cop.columnReader(3);
        defer cr3.deinit();
        var url_count3: usize = 0;
        const n3 = try cr3.readStrings(3, &url_count3, struct {
            fn cb(ctx: *usize, _: []const u8) !void {
                ctx.* += 1;
            }
        }.cb);
        try std.testing.expectEqual(@as(usize, 3), n3);
        try std.testing.expectEqual(@as(usize, 3), url_count3);
    }
}

test "CompactPart primary.cidx uses leading sort key column" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_compact_primary_sort_key";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
    }

    const cols = [_]schema.Column{
        .{ .name = "WatchID", .ty = .int64 },
        .{ .name = "CounterID", .ty = .int32 },
    };
    const sort_keys = [_][]const u8{"CounterID"};
    const table = schema.Table{ .name = "test_compact_primary_sort_key", .columns = &cols, .sort_keys = &sort_keys };

    {
        var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();

        const rows = GRANULE_SIZE + 1;
        const watch_ids = try allocator.alloc(i64, rows);
        defer allocator.free(watch_ids);
        const counter_ids = try allocator.alloc(i64, rows);
        defer allocator.free(counter_ids);
        for (watch_ids, counter_ids, 0..) |*watch, *counter, i| {
            watch.* = 10_000 + @as(i64, @intCast(i));
            counter.* = if (i < GRANULE_SIZE) 42 else 99;
        }
        try cp.appendFixedBatch(0, watch_ids);
        try cp.appendFixedBatch(1, counter_ids);
        try cp.finish();
    }

    const pk = try readCompactPrimaryFixedValues(io, allocator, part_dir, .int32);
    defer allocator.free(pk);
    try std.testing.expectEqual(@as(usize, 2), pk.len);
    try std.testing.expectEqual(@as(i64, 42), pk[0]);
    try std.testing.expectEqual(@as(i64, 99), pk[1]);
}

test "CompactOpenedPart range reader keeps LowCardinality dictionary" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_compact_lc_range";

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "tag", .ty = .low_card, .ch_type = "LowCardinality(String)" },
    };
    const table = schema.Table{ .name = "test_compact_lc_range", .columns = &cols };
    const rows: usize = GRANULE_SIZE + 4;

    {
        var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();

        const ids = try allocator.alloc(i64, rows);
        defer allocator.free(ids);
        for (ids, 0..) |*v, i| v.* = @intCast(i);
        try cp.appendFixedBatch(0, ids);
        for (0..rows) |i| try cp.appendString(1, if (i % 2 == 0) "even" else "odd");
        try cp.finish();
    }

    {
        var cop = try CompactOpenedPart.open(io, allocator, part_dir, table);
        defer cop.deinit();
        var cr = try cop.columnReaderRange(1, GRANULE_SIZE + 1, 2);
        defer cr.deinit();

        const Ctx = struct {
            allocator: std.mem.Allocator,
            items: std.ArrayList([]const u8),
        };
        var got = Ctx{ .allocator = allocator, .items = .empty };
        defer got.items.deinit(allocator);
        _ = try cr.readStrings(2, &got, struct {
            fn cb(out: *Ctx, s: []const u8) !void {
                try out.items.append(out.allocator, s);
            }
        }.cb);
        try std.testing.expectEqual(@as(usize, 2), got.items.items.len);
        try std.testing.expectEqualStrings("odd", got.items.items[0]);
        try std.testing.expectEqualStrings("even", got.items.items[1]);
    }
}

test "CompactOpenedPart range reader reads fixed and string across granules" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_compact_range_fixed_string";

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "name", .ty = .text },
    };
    const table = schema.Table{ .name = "test_compact_range_fixed_string", .columns = &cols };
    const rows: usize = GRANULE_SIZE + 5;

    {
        var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();

        const ids = try allocator.alloc(i64, rows);
        defer allocator.free(ids);
        for (ids, 0..) |*v, i| v.* = @intCast(i);
        try cp.appendFixedBatch(0, ids);
        for (0..rows) |i| {
            const name = try std.fmt.allocPrint(allocator, "name-{d}", .{i});
            defer allocator.free(name);
            try cp.appendString(1, name);
        }
        try cp.finish();
    }

    {
        var cop = try CompactOpenedPart.open(io, allocator, part_dir, table);
        defer cop.deinit();

        var id_cr = try cop.columnReaderRange(0, GRANULE_SIZE - 2, 5);
        defer id_cr.deinit();
        var ids: [5]i64 = undefined;
        const n_ids = try id_cr.readFixed(&ids);
        try std.testing.expectEqual(@as(usize, 5), n_ids);
        try std.testing.expectEqual(@as(i64, GRANULE_SIZE - 2), ids[0]);
        try std.testing.expectEqual(@as(i64, GRANULE_SIZE + 2), ids[4]);

        var name_cr = try cop.columnReaderRange(1, GRANULE_SIZE - 2, 5);
        defer name_cr.deinit();
        const Ctx = struct {
            items: [5][]const u8 = undefined,
            idx: usize = 0,
        };
        var ctx = Ctx{};
        const n_names = try name_cr.readStrings(5, &ctx, struct {
            fn cb(c: *Ctx, s: []const u8) !void {
                c.items[c.idx] = s;
                c.idx += 1;
            }
        }.cb);
        try std.testing.expectEqual(@as(usize, 5), n_names);
        try std.testing.expectEqualStrings("name-8190", ctx.items[0]);
        try std.testing.expectEqualStrings("name-8194", ctx.items[4]);
    }
}

test "CompactOpenedPart reads CH-written compact part" {
    // This test reads the part created by the CH server in the integration setup.
    // It is skipped if the part directory doesn't exist.
    const ch_part_dir = "/tmp/ch-srv/data/store/4d0/4d02ac62-2539-4cf7-97dc-28415c3acc30/all_1_1_0";
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Check if part exists by trying to open count.txt
    {
        const check_path = ch_part_dir ++ "/count.txt";
        const f = std.Io.Dir.cwd().openFile(io, check_path, .{}) catch return; // skip if not present
        f.close(io);
    }

    const cols = [_]schema.Column{
        .{ .name = "event_date",  .ty = .date },
        .{ .name = "event_time",  .ty = .int32 },
        .{ .name = "user_id",     .ty = .int32 },
        .{ .name = "page_id",     .ty = .int32 },
        .{ .name = "duration",    .ty = .int64 },
        .{ .name = "url",         .ty = .text },
    };
    const table = schema.Table{ .name = "events", .columns = &cols };

    var cop = try CompactOpenedPart.open(io, allocator, ch_part_dir, table);
    defer cop.deinit();

    try std.testing.expectEqual(@as(u64, 5), cop.row_count);

    // Read event_date (u16 Date → stored as i16 → should be 19723 or 19724)
    var cr0 = try cop.columnReader(0);
    defer cr0.deinit();
    var dates: [5]i64 = undefined;
    const n = try cr0.readFixed(&dates);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqual(@as(i64, 19723), dates[0]);

    // Read user_id
    var cr2 = try cop.columnReader(2);
    defer cr2.deinit();
    var users: [5]i64 = undefined;
    const nu = try cr2.readFixed(&users);
    try std.testing.expectEqual(@as(usize, 5), nu);
    try std.testing.expectEqual(@as(i64, 1), users[0]);

    // Read url (String) — just count rows and check first
    var cr5 = try cop.columnReader(5);
    defer cr5.deinit();
    var url_count: usize = 0;
    const ns = try cr5.readStrings(5, &url_count, struct {
        fn cb(ctx: *usize, _s: []const u8) !void {
            _ = _s;
            ctx.* += 1;
        }
    }.cb);
    try std.testing.expectEqual(@as(usize, 5), ns);
    try std.testing.expectEqual(@as(usize, 5), url_count);
}

test "CompactPart write events schema for CH ATTACH" {
    // Writes a compact part matching 'default.events' to /tmp/ch-srv detached dir.
    // After the test, run:
    //   clickhouse client --port 19000 \
    //     --query "ALTER TABLE default.events ATTACH PART 'all_2_2_0'"
    // to verify CH can read the zighouse-written part.
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const ch_detached = "/tmp/ch-srv/data/store/4d0/4d02ac62-2539-4cf7-97dc-28415c3acc30/detached";
    const part_name = "all_2_2_0";
    const part_dir = ch_detached ++ "/" ++ part_name;

    // Remove existing if present
    {
        var detached_dir = std.Io.Dir.openDirAbsolute(io, ch_detached, .{}) catch null;
        if (detached_dir) |*d| {
            defer d.close(io);
            d.deleteTree(io, part_name) catch {};
        }
    }

    const cols = [_]schema.Column{
        .{ .name = "event_date",  .ty = .date },
        .{ .name = "event_time",  .ty = .int32 },
        .{ .name = "user_id",     .ty = .int32 },
        .{ .name = "page_id",     .ty = .int32 },
        .{ .name = "duration",    .ty = .int64 },
        .{ .name = "url",         .ty = .text },
    };
    const table = schema.Table{ .name = "events", .columns = &cols };

    var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
    defer cp.deinit();

    // 5 rows ordered by (event_date, user_id)
    // 2024-01-01 = day 19723, 2024-01-02 = day 19724
    const dates = [_]i64{ 19723, 19723, 19723, 19724, 19724 };
    const times = [_]i64{ 1704099600, 1704099720, 1704099660, 1704186000, 1704186300 };
    const users = [_]i64{ 1, 1, 2, 3, 4 };
    const pages = [_]i64{ 100, 102, 101, 100, 103 };
    const durs  = [_]i64{ 3500, 8900, 1200, 4200, 600 };
    const urls  = [_][]const u8{
        "https://example.com/home",
        "https://example.com/products",
        "https://example.com/about",
        "https://example.com/home",
        "https://example.com/contact",
    };

    try cp.appendFixedBatch(0, &dates);
    try cp.appendFixedBatch(1, &times);
    try cp.appendFixedBatch(2, &users);
    try cp.appendFixedBatch(3, &pages);
    try cp.appendFixedBatch(4, &durs);
    for (urls) |u| try cp.appendString(5, u);
    // row_count already set by appendFixedBatch(0); url has no row_count tracking
    try std.testing.expectEqual(@as(u64, 5), cp.row_count);

    try cp.finish();

    // Verify files exist
    const cwd = std.Io.Dir.cwd();
    inline for ([_][]const u8{
        "data.bin", "data.cmrk4", "primary.cidx", "columns.txt",
        "count.txt", "checksums.txt", "columns_substreams.txt",
    }) |fname| {
        const fpath = part_dir ++ "/" ++ fname;
        const f = try cwd.openFile(io, fpath, .{});
        f.close(io);
    }

    std.debug.print(
        \\
        \\Part written to {s}
        \\To attach: clickhouse client --port 19000 \
        \\  --query "ALTER TABLE default.events ATTACH PART '{s}'"
        \\
    , .{ part_dir, part_name });
}

test "CompactPart write all_3_3_0 for CH ATTACH E2E" {
    // Writes a NEW compact part all_3_3_0 to the CH detached dir.
    // After the test, verify with:
    //   curl 'http://localhost:19001/?query=SELECT+count(*)+FROM+default.events'
    //   -> should be 13 (10 existing + 3 new)
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const ch_detached = "/tmp/ch-srv/data/store/4d0/4d02ac62-2539-4cf7-97dc-28415c3acc30/detached";
    const part_name = "all_3_3_0";
    const part_dir = ch_detached ++ "/" ++ part_name;

    // Remove existing if present
    {
        var detached_dir = std.Io.Dir.openDirAbsolute(io, ch_detached, .{}) catch null;
        if (detached_dir) |*d| {
            defer d.close(io);
            d.deleteTree(io, part_name) catch {};
        }
    }

    const cols = [_]schema.Column{
        .{ .name = "event_date", .ty = .date },
        .{ .name = "event_time", .ty = .int32 },
        .{ .name = "user_id",    .ty = .int32 },
        .{ .name = "page_id",    .ty = .int32 },
        .{ .name = "duration",   .ty = .int64 },
        .{ .name = "url",        .ty = .text },
    };
    const table = schema.Table{ .name = "events", .columns = &cols };

    var cp = try CompactPart.open(io, allocator, part_dir, table, 0x82);
    defer cp.deinit();

    // 3 new rows (user_id 5,6,7) — not overlapping with existing 1..4
    const dates = [_]i64{ 19723, 19724, 19724 };
    const times = [_]i64{ 1704099900, 1704186600, 1704186900 };
    const users = [_]i64{ 5, 6, 7 };
    const pages = [_]i64{ 200, 201, 202 };
    const durs  = [_]i64{ 500, 750, 1100 };
    const urls  = [_][]const u8{
        "https://example.com/new1",
        "https://example.com/new2",
        "https://example.com/new3",
    };

    try cp.appendFixedBatch(0, &dates);
    try cp.appendFixedBatch(1, &times);
    try cp.appendFixedBatch(2, &users);
    try cp.appendFixedBatch(3, &pages);
    try cp.appendFixedBatch(4, &durs);
    for (urls) |u| try cp.appendString(5, u);
    try std.testing.expectEqual(@as(u64, 3), cp.row_count);
    try cp.finish();

    std.debug.print(
        \\
        \\[E2E] Part written: {s}
        \\[E2E] Run: curl 'http://localhost:19001/?query=ALTER+TABLE+default.events+ATTACH+PART+%27{s}%27'
        \\
    , .{ part_dir, part_name });
}

test "part write and verify files exist" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const part_dir = "/tmp/zig_test_part_all_1_1_0";

    const cols = [_]schema.Column{
        .{ .name = "EventDate", .ty = .date },
        .{ .name = "CounterID", .ty = .int32 },
    };
    const table = schema.Table{ .name = "test", .columns = &cols };

    var part = try Part.open(io, allocator, part_dir, table, null, 0x82);
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
        var part = try Part.open(io, allocator, part_dir, table, null, 0x82);
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

test "OpenedPart range reader reads ClickHouse wide String varUInt stream" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_ch_wide_string";
    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
        try cwd.createDirPath(io, part_dir);
    }
    defer {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
    }

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = part_dir ++ "/count.txt", .data = "3\n" });

    var raw = std.ArrayList(u8).empty;
    defer raw.deinit(allocator);
    const strings = [_][]const u8{ "alpha", "beta", "gamma" };
    for (strings) |s| {
        try raw.append(allocator, @intCast(s.len));
        try raw.appendSlice(allocator, s);
    }

    {
        var aw = std.Io.Writer.Allocating.init(allocator);
        defer aw.deinit();
        try block.writeBlock(&aw.writer, raw.items, block.METHOD_LZ4);
        var al = aw.toArrayList();
        defer al.deinit(allocator);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = part_dir ++ "/Name.bin", .data = al.items });
    }

    {
        var mark_raw: [marks.MARK_SIZE]u8 = undefined;
        var mw = std.Io.Writer.fixed(&mark_raw);
        try marks.writeMark(&mw, .{
            .offset_in_compressed_file = 0,
            .offset_in_decompressed_block = 0,
            .granularity = 3,
        });
        const mark_bytes = std.Io.Writer.buffered(&mw);

        var aw = std.Io.Writer.Allocating.init(allocator);
        defer aw.deinit();
        try block.writeBlock(&aw.writer, mark_bytes, block.METHOD_LZ4);
        var al = aw.toArrayList();
        defer al.deinit(allocator);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = part_dir ++ "/Name.cmrk2", .data = al.items });
    }

    const cols = [_]schema.Column{.{ .name = "Name", .ty = .text }};
    const table = schema.Table{ .name = "wide_strings", .columns = &cols };
    var op = try OpenedPart.open(io, allocator, part_dir, table);
    defer op.deinit();

    var cr = try op.columnReaderRange(0, 1, 2);
    defer cr.deinit();
    const expected = [_][]const u8{ "beta", "gamma" };
    const Ctx = struct {
        idx: usize = 0,
        expected: *const [2][]const u8,

        fn cb(self: *@This(), s: []const u8) !void {
            try std.testing.expectEqualStrings(self.expected[self.idx], s);
            self.idx += 1;
        }
    };
    var ctx = Ctx{ .expected = &expected };
    const n = try cr.readStrings(2, &ctx, Ctx.cb);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(@as(usize, 2), ctx.idx);
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
        var part = try Part.open(io, allocator, part_dir, table, "CounterID", 0x82);
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
        var part = try Part.open(io, allocator, part_dir, table, null, 0x82);
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

    const result = Part.open(io, allocator, "/tmp/zig_test_pk_err", table, "NonExistentColumn", 0x82);
    try std.testing.expectError(error.PkColumnNotFound, result);
}

// ── OpenedPartAny — format-auto-detecting reader ──────────────────────────────

/// Opens either a Wide or a Compact MergeTree part by probing for `data.cmrk4`.
/// Both variants expose the same `row_count`, `columnReader()`, and `deinit()` API.
pub const OpenedPartAny = union(enum) {
    wide:    OpenedPart,
    compact: CompactOpenedPart,

    pub fn open(io: std.Io, alloc: std.mem.Allocator, dir: []const u8, table: schema.Table) !OpenedPartAny {
        const cmrk_path = try std.fmt.allocPrint(alloc, "{s}/data.cmrk4", .{dir});
        defer alloc.free(cmrk_path);
        const is_compact = blk: {
            const f = std.Io.Dir.cwd().openFile(io, cmrk_path, .{}) catch break :blk false;
            f.close(io);
            break :blk true;
        };
        if (is_compact) {
            return .{ .compact = try CompactOpenedPart.open(io, alloc, dir, table) };
        }
        const cmrk3_path = try std.fmt.allocPrint(alloc, "{s}/data.cmrk3", .{dir});
        defer alloc.free(cmrk3_path);
        const has_cmrk3 = blk: {
            const f = std.Io.Dir.cwd().openFile(io, cmrk3_path, .{}) catch break :blk false;
            f.close(io);
            break :blk true;
        };
        if (has_cmrk3) return error.UnsupportedCompactMarkVersion;
        return .{ .wide = try OpenedPart.open(io, alloc, dir, table) };
    }

    pub fn deinit(self: *OpenedPartAny) void {
        switch (self.*) {
            .wide    => |*p| p.deinit(),
            .compact => |*p| p.deinit(),
        }
    }

    pub fn rowCount(self: *const OpenedPartAny) u64 {
        return switch (self.*) {
            .wide    => |*p| p.row_count,
            .compact => |*p| p.row_count,
        };
    }

    pub fn columnReader(self: *OpenedPartAny, col_idx: usize) !ColumnReader {
        return switch (self.*) {
            .wide    => |*p| p.columnReader(col_idx),
            .compact => |*p| p.columnReader(col_idx),
        };
    }

    pub fn columnReaderRange(self: *OpenedPartAny, col_idx: usize, start_row: u64, row_count: usize) !ColumnReader {
        return switch (self.*) {
            .compact => |*p| p.columnReaderRange(col_idx, start_row, row_count),
            .wide => |*p| p.columnReaderRange(col_idx, start_row, row_count),
        };
    }
};
