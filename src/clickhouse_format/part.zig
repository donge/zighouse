/// Write a complete ClickHouse MergeTree part directory.
///
/// Part layout written (`all_1_1_0/`):
///   columns.txt
///   count.txt
///   primary.idx
///   checksums.txt
///   <col>.bin          — LZ4-compressed column data
///   <col>.mrk2         — mark file (offset table)
///
/// Phase-1 supported column types: Int16, Int32, Int64, Date, DateTime, String.
///
/// Usage:
///   var part = try Part.open(io, allocator, "/store/default/hits/parts/all_1_1_0", schema);
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
const ColumnWriter = struct {
    /// Accumulated uncompressed bytes for the current LZ4 block.
    buf: std.ArrayList(u8),
    /// Compressed bytes accumulated for this column's .bin file.
    bin_data: std.ArrayList(u8),
    /// Mark entries accumulated so far.
    mark_list: std.ArrayList(marks.Mark),
    /// Byte offset in .bin where the current compressed block starts.
    bin_offset: u64,
    /// Number of rows in the current granule.
    granule_rows: u64,
    /// Total rows written to this column.
    total_rows: u64,
    /// Column schema info.
    col: schema.Column,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, col: schema.Column) ColumnWriter {
        return .{
            .buf = .empty,
            .bin_data = .empty,
            .mark_list = .empty,
            .bin_offset = 0,
            .granule_rows = 0,
            .total_rows = 0,
            .col = col,
            .allocator = allocator,
        };
    }

    fn deinit(self: *ColumnWriter) void {
        self.buf.deinit(self.allocator);
        self.bin_data.deinit(self.allocator);
        self.mark_list.deinit(self.allocator);
    }

    fn appendFixed(self: *ColumnWriter, bytes: []const u8) !void {
        try self.buf.appendSlice(self.allocator, bytes);
        self.granule_rows += 1;
        self.total_rows += 1;
        if (self.granule_rows >= GRANULE_SIZE or self.buf.items.len >= MAX_BLOCK_BYTES) {
            try self.flushBlock();
        }
    }

    fn appendStr(self: *ColumnWriter, s: []const u8) !void {
        // CH String: varuint(len) + bytes
        var vbuf: [10]u8 = undefined;
        var vw = std.Io.Writer.fixed(&vbuf);
        try string_codec.writeVarint(&vw, s.len);
        const vlen = std.Io.Writer.buffered(&vw);
        try self.buf.appendSlice(self.allocator, vlen);
        try self.buf.appendSlice(self.allocator, s);
        self.granule_rows += 1;
        self.total_rows += 1;
        if (self.granule_rows >= GRANULE_SIZE or self.buf.items.len >= MAX_BLOCK_BYTES) {
            try self.flushBlock();
        }
    }

    fn flushBlock(self: *ColumnWriter) !void {
        if (self.buf.items.len == 0) return;

        // Compress the buffered bytes into bin_data
        const uncompressed = self.buf.items;
        const bound = block.BLOCK_HEADER_TOTAL + @import("lz4.zig").compressBound(uncompressed.len);
        const compressed_buf = try self.allocator.alloc(u8, bound);
        defer self.allocator.free(compressed_buf);
        var w = std.Io.Writer.fixed(compressed_buf);
        try block.writeBlock(&w, uncompressed);
        const compressed = std.Io.Writer.buffered(&w);

        // Record mark: offset into .bin where this block starts, decompressed offset 0
        try self.mark_list.append(self.allocator, .{
            .offset_in_compressed_file = self.bin_offset,
            .offset_in_decompressed_block = 0,
            .granularity = self.granule_rows,
        });

        // Append compressed bytes to bin_data
        try self.bin_data.appendSlice(self.allocator, compressed);
        self.bin_offset += @intCast(compressed.len);
        self.buf.items.len = 0;
        self.granule_rows = 0;
    }

    fn finish(self: *ColumnWriter) !void {
        try self.flushBlock(); // flush partial last granule
    }
};

/// A part writer. Open with `openPart`, append rows, call `finish`.
pub const Part = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    part_dir: []const u8,
    table: schema.Table,
    column_writers: []ColumnWriter,
    row_count: u64,
    /// Index of the primary key column (first column by default).
    pk_col_idx: usize,
    /// Primary key granule entries (one per granule, for the first PK column).
    pk_entries: std.ArrayList(primary_idx.PkValue),

    pub fn open(
        io: std.Io,
        allocator: std.mem.Allocator,
        part_dir: []const u8,
        table: schema.Table,
    ) !Part {
        // Create the part directory
        try std.Io.Dir.cwd().createDirPath(io, part_dir);

        const column_writers = try allocator.alloc(ColumnWriter, table.columns.len);
        for (column_writers, table.columns) |*cw, col| {
            cw.* = ColumnWriter.init(allocator, col);
        }

        return .{
            .allocator = allocator,
            .io = io,
            .part_dir = part_dir,
            .table = table,
            .column_writers = column_writers,
            .row_count = 0,
            .pk_col_idx = 0,
            .pk_entries = .empty,
        };
    }

    pub fn deinit(self: *Part) void {
        for (self.column_writers) |*cw| cw.deinit();
        self.allocator.free(self.column_writers);
        self.pk_entries.deinit(self.allocator);
    }

    /// Append one row.  `values` must have one entry per table column, in order.
    pub fn appendRow(self: *Part, values: []const Value) !void {
        std.debug.assert(values.len == self.table.columns.len);
        const row_in_granule = self.row_count % GRANULE_SIZE;

        for (self.column_writers, values, self.table.columns) |*cw, val, col| {
            switch (col.ty) {
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
                else => return error.NotAFixedColumn,
            }
        }
        // Track row_count from the first column (pk_col_idx); update pk_entries
        if (col_idx == self.pk_col_idx) {
            for (values, 0..) |v, i| {
                const abs_row = self.row_count + i;
                if (abs_row % GRANULE_SIZE == 0) {
                    const pk_entry: primary_idx.PkValue = switch (col.ty) {
                        .int16 => .{ .i16 = @intCast(v) },
                        .int32, .date => .{ .i32 = @intCast(v) },
                        .int64, .timestamp => .{ .i64 = v },
                        else => return error.NotAFixedColumn,
                    };
                    try self.pk_entries.append(self.allocator, pk_entry);
                }
            }
            self.row_count += values.len;
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
        // Flush all column writers
        for (self.column_writers) |*cw| try cw.finish();

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
        for (self.column_writers, self.table.columns) |*cw, col| {
            // .bin file
            const bin_name = try std.fmt.allocPrint(self.allocator, "{s}.bin", .{col.name});
            try owned_names.append(self.allocator, bin_name);
            const bin_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.part_dir, bin_name });
            defer self.allocator.free(bin_path);

            try writeFile(self.io, bin_path, cw.bin_data.items);

            const file_hash = cityhash.cityHash128(cw.bin_data.items);
            // TODO PR-CH4+: track uncompressed hash during flushBlock
            const uncompressed_hash: u128 = 0;

            try checksum_entries.append(self.allocator, .{
                .name = bin_name,
                .file_size = cw.bin_data.items.len,
                .file_hash = file_hash,
                .is_compressed = true,
                .uncompressed_size = cw.total_rows * (types.chFixedWidth(col.ty) orelse 0),
                .uncompressed_hash = uncompressed_hash,
            });

            // .mrk2 file
            const mrk_name = try std.fmt.allocPrint(self.allocator, "{s}.mrk2", .{col.name});
            try owned_names.append(self.allocator, mrk_name);
            const mrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.part_dir, mrk_name });
            defer self.allocator.free(mrk_path);

            // Serialize marks
            const mrk_data = try self.allocator.alloc(u8, cw.mark_list.items.len * marks.MARK_SIZE);
            defer self.allocator.free(mrk_data);
            var mrk_w = std.Io.Writer.fixed(mrk_data);
            try marks.writeMarks(&mrk_w, cw.mark_list.items);
            const mrk_bytes = std.Io.Writer.buffered(&mrk_w);

            try writeFile(self.io, mrk_path, mrk_bytes);
            const mrk_hash = cityhash.cityHash128(mrk_bytes);
            try checksum_entries.append(self.allocator, .{
                .name = mrk_name,
                .file_size = mrk_bytes.len,
                .file_hash = mrk_hash,
                .is_compressed = false,
            });
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
        const mrk_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}.mrk2", .{ self.part_dir, col.name });
        defer self.allocator.free(mrk_path);

        // Load .mrk2
        const mrk_bytes = try std.Io.Dir.cwd().readFileAlloc(self.io, mrk_path, self.allocator, .limited(std.math.maxInt(usize)));
        defer self.allocator.free(mrk_bytes);
        var mrk_r = std.Io.Reader.fixed(mrk_bytes);
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

        return ColumnReader{
            .allocator = self.allocator,
            .col = col,
            .row_count = self.row_count,
            .data = try data.toOwnedSlice(self.allocator),
            .cursor = 0,
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
    data: []u8,
    /// Byte cursor into `data`.
    cursor: usize,
    rows_read: u64,

    pub fn deinit(self: *ColumnReader) void {
        self.allocator.free(self.data);
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
                .int16 => @as(i64, std.mem.readInt(i16, slice[0..2], .little)),
                .int32, .date => @as(i64, std.mem.readInt(i32, slice[0..4], .little)),
                .int64, .timestamp => std.mem.readInt(i64, slice[0..8], .little),
                else => return error.NotAFixedColumn,
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
        const remaining_rows = self.row_count - self.rows_read;
        const count = @min(n, remaining_rows);
        var read: usize = 0;
        while (read < count) : (read += 1) {
            // Decode LEB128 varint length inline
            var len: u64 = 0;
            var shift: u6 = 0;
            while (true) {
                if (self.cursor >= self.data.len) return error.UnexpectedEndOfData;
                const b = self.data[self.cursor];
                self.cursor += 1;
                len |= @as(u64, b & 0x7F) << shift;
                if (b & 0x80 == 0) break;
                shift += 7;
                if (shift > 63) return error.VarintOverflow;
            }
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

    var part = try Part.open(io, allocator, part_dir, table);
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
    _ = try cwd.openFile(io, part_dir ++ "/CounterID.mrk2", .{});
    _ = try cwd.openFile(io, part_dir ++ "/checksums.txt", .{});
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
        var part = try Part.open(io, allocator, part_dir, table);
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
