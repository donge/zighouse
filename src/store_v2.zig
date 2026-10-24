const std = @import("std");
const schema = @import("schema");

pub const magic = "ZHSTV2\x00\x00";
pub const header_size: usize = 64;
pub const directory_entry_size: usize = 64;
pub const default_block_rows: u32 = 65_536;

const Codec = enum(u8) {
    raw = 0,
    lz4 = 1,
};

const StatsKind = enum(u8) {
    signed = 0,
    float = 1,
};

extern fn LZ4_compressBound(input_size: c_int) c_int;
extern fn LZ4_compress_default(
    source: [*]const u8,
    dest: [*]u8,
    source_size: c_int,
    max_dest_size: c_int,
) c_int;
extern fn LZ4_decompress_safe(
    source: [*]const u8,
    dest: [*]u8,
    compressed_size: c_int,
    max_decompressed_size: c_int,
) c_int;

pub const Error = error{
    BadMagic,
    UnsupportedVersion,
    InvalidHeader,
    InvalidDirectory,
    InvalidRange,
    CorruptBlock,
    CompressionFailed,
    RowCountMismatch,
};

pub const BlockEntry = struct {
    row_start: u64,
    data_offset: u64,
    row_count: u32,
    compressed_size: u32,
    uncompressed_size: u32,
    codec: Codec,
    stats_kind: StatsKind,
    checksum: u64,
    min_bits: u64,
    max_bits: u64,
};

fn typeWidth(ty: schema.ColumnType) !u8 {
    return @intCast(ty.fixedWidth() orelse return error.InvalidHeader);
}

fn statsKind(ty: schema.ColumnType) StatsKind {
    return switch (ty) {
        .float32, .float64 => .float,
        else => .signed,
    };
}

fn encodeHeader(
    out: *[header_size]u8,
    ty: schema.ColumnType,
    row_count: u64,
    block_rows: u32,
    block_count: u32,
) !void {
    @memset(out, 0);
    @memcpy(out[0..magic.len], magic);
    std.mem.writeInt(u16, out[8..10], 2, .little);
    out[10] = @backingInt(ty);
    out[11] = try typeWidth(ty);
    std.mem.writeInt(u32, out[12..16], block_rows, .little);
    std.mem.writeInt(u64, out[16..24], row_count, .little);
    std.mem.writeInt(u32, out[24..28], block_count, .little);
    std.mem.writeInt(u16, out[28..30], directory_entry_size, .little);
    std.mem.writeInt(u16, out[30..32], header_size, .little);
}

fn encodeEntry(out: *[directory_entry_size]u8, entry: BlockEntry) void {
    @memset(out, 0);
    std.mem.writeInt(u64, out[0..8], entry.row_start, .little);
    std.mem.writeInt(u64, out[8..16], entry.data_offset, .little);
    std.mem.writeInt(u32, out[16..20], entry.row_count, .little);
    std.mem.writeInt(u32, out[20..24], entry.compressed_size, .little);
    std.mem.writeInt(u32, out[24..28], entry.uncompressed_size, .little);
    out[28] = @backingInt(entry.codec);
    out[29] = @backingInt(entry.stats_kind);
    std.mem.writeInt(u64, out[32..40], entry.checksum, .little);
    std.mem.writeInt(u64, out[40..48], entry.min_bits, .little);
    std.mem.writeInt(u64, out[48..56], entry.max_bits, .little);
}

fn decodeEntry(bytes: []const u8) !BlockEntry {
    const codec: Codec = switch (bytes[28]) {
        0 => .raw,
        1 => .lz4,
        else => return error.InvalidDirectory,
    };
    const kind: StatsKind = switch (bytes[29]) {
        0 => .signed,
        1 => .float,
        else => return error.InvalidDirectory,
    };
    return .{
        .row_start = std.mem.readInt(u64, bytes[0..8], .little),
        .data_offset = std.mem.readInt(u64, bytes[8..16], .little),
        .row_count = std.mem.readInt(u32, bytes[16..20], .little),
        .compressed_size = std.mem.readInt(u32, bytes[20..24], .little),
        .uncompressed_size = std.mem.readInt(u32, bytes[24..28], .little),
        .codec = codec,
        .stats_kind = kind,
        .checksum = std.mem.readInt(u64, bytes[32..40], .little),
        .min_bits = std.mem.readInt(u64, bytes[40..48], .little),
        .max_bits = std.mem.readInt(u64, bytes[48..56], .little),
    };
}

pub const Writer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    ty: schema.ColumnType,
    width: usize,
    expected_rows: u64,
    block_rows: u32,
    entries: []BlockEntry,
    block: []u8,
    compressed: []u8,
    block_len: usize = 0,
    rows_written: u64 = 0,
    entries_written: usize = 0,
    payload_offset: u64,
    min_i64: i64 = std.math.maxInt(i64),
    max_i64: i64 = std.math.minInt(i64),
    min_f64: f64 = std.math.inf(f64),
    max_f64: f64 = -std.math.inf(f64),
    finished: bool = false,

    pub fn open(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        ty: schema.ColumnType,
        expected_rows: u64,
        block_rows: u32,
    ) !Writer {
        const width = try typeWidth(ty);
        if (block_rows == 0) return error.InvalidHeader;
        const block_count_u64 = (expected_rows + block_rows - 1) / block_rows;
        if (block_count_u64 > std.math.maxInt(u32)) return error.InvalidHeader;
        const entries = try allocator.alloc(BlockEntry, @intCast(block_count_u64));
        errdefer allocator.free(entries);
        const block = try allocator.alloc(u8, @as(usize, block_rows) * width);
        errdefer allocator.free(block);
        const bound = LZ4_compressBound(@intCast(block.len));
        if (bound <= 0) return error.CompressionFailed;
        const compressed = try allocator.alloc(u8, @intCast(bound));
        errdefer allocator.free(compressed);

        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
        errdefer file.close(io);
        const prefix_size = header_size + entries.len * directory_entry_size;
        const zeros = try allocator.alloc(u8, prefix_size);
        defer allocator.free(zeros);
        @memset(zeros, 0);
        try file.writeStreamingAll(io, zeros);

        return .{
            .allocator = allocator,
            .io = io,
            .file = file,
            .ty = ty,
            .width = width,
            .expected_rows = expected_rows,
            .block_rows = block_rows,
            .entries = entries,
            .block = block,
            .compressed = compressed,
            .payload_offset = prefix_size,
        };
    }

    fn updateStats(self: *Writer, value: anytype) void {
        switch (@TypeOf(value)) {
            f32, f64 => {
                const f: f64 = @floatCast(value);
                self.min_f64 = @min(self.min_f64, f);
                self.max_f64 = @max(self.max_f64, f);
            },
            i8, i16, i32, i64 => {
                const v: i64 = @intCast(value);
                self.min_i64 = @min(self.min_i64, v);
                self.max_i64 = @max(self.max_i64, v);
            },
            else => @compileError("unsupported Store V2 stats type"),
        }
    }

    fn appendValue(self: *Writer, value: anytype) !void {
        if (self.rows_written >= self.expected_rows) return error.RowCountMismatch;
        const bytes = self.block[self.block_len..][0..self.width];
        switch (@TypeOf(value)) {
            i8 => bytes[0] = @bitCast(value),
            i16 => std.mem.writeInt(i16, bytes[0..2], value, .little),
            i32 => std.mem.writeInt(i32, bytes[0..4], value, .little),
            i64 => std.mem.writeInt(i64, bytes[0..8], value, .little),
            f32 => std.mem.writeInt(u32, bytes[0..4], @bitCast(value), .little),
            f64 => std.mem.writeInt(u64, bytes[0..8], @bitCast(value), .little),
            else => @compileError("unsupported Store V2 fixed-width type"),
        }
        self.updateStats(value);
        self.block_len += self.width;
        self.rows_written += 1;
        if (self.block_len == self.block.len) try self.flushBlock();
    }

    pub fn writeI8(self: *Writer, value: i8) !void {
        try self.appendValue(value);
    }
    pub fn writeI16(self: *Writer, value: i16) !void {
        try self.appendValue(value);
    }
    pub fn writeI32(self: *Writer, value: i32) !void {
        try self.appendValue(value);
    }
    pub fn writeI64(self: *Writer, value: i64) !void {
        try self.appendValue(value);
    }
    pub fn writeF32(self: *Writer, value: f32) !void {
        try self.appendValue(value);
    }
    pub fn writeF64(self: *Writer, value: f64) !void {
        try self.appendValue(value);
    }

    fn flushBlock(self: *Writer) !void {
        if (self.block_len == 0) return;
        const source = self.block[0..self.block_len];
        const compressed_len_raw = LZ4_compress_default(
            source.ptr,
            self.compressed.ptr,
            @intCast(source.len),
            @intCast(self.compressed.len),
        );
        if (compressed_len_raw <= 0) return error.CompressionFailed;
        const compressed_len: usize = @intCast(compressed_len_raw);
        const use_lz4 = compressed_len < source.len;
        const payload = if (use_lz4) self.compressed[0..compressed_len] else source;
        try self.file.writeStreamingAll(self.io, payload);

        const rows_in_block: u32 = @intCast(source.len / self.width);
        const row_start = self.rows_written - rows_in_block;
        self.entries[self.entries_written] = .{
            .row_start = row_start,
            .data_offset = self.payload_offset,
            .row_count = rows_in_block,
            .compressed_size = @intCast(payload.len),
            .uncompressed_size = @intCast(source.len),
            .codec = if (use_lz4) .lz4 else .raw,
            .stats_kind = statsKind(self.ty),
            .checksum = std.hash.Wyhash.hash(0, source),
            .min_bits = if (statsKind(self.ty) == .float) @bitCast(self.min_f64) else @bitCast(self.min_i64),
            .max_bits = if (statsKind(self.ty) == .float) @bitCast(self.max_f64) else @bitCast(self.max_i64),
        };
        self.entries_written += 1;
        self.payload_offset += payload.len;
        self.block_len = 0;
        self.min_i64 = std.math.maxInt(i64);
        self.max_i64 = std.math.minInt(i64);
        self.min_f64 = std.math.inf(f64);
        self.max_f64 = -std.math.inf(f64);
    }

    pub fn finish(self: *Writer) !void {
        if (self.finished) return;
        try self.flushBlock();
        if (self.rows_written != self.expected_rows or self.entries_written != self.entries.len)
            return error.RowCountMismatch;

        var header: [header_size]u8 = undefined;
        try encodeHeader(&header, self.ty, self.expected_rows, self.block_rows, @intCast(self.entries.len));
        try self.file.writePositionalAll(self.io, &header, 0);
        var encoded: [directory_entry_size]u8 = undefined;
        for (self.entries, 0..) |entry, i| {
            encodeEntry(&encoded, entry);
            try self.file.writePositionalAll(
                self.io,
                &encoded,
                header_size + i * directory_entry_size,
            );
        }
        self.finished = true;
    }

    pub fn deinit(self: *Writer) void {
        self.file.close(self.io);
        self.allocator.free(self.entries);
        self.allocator.free(self.block);
        self.allocator.free(self.compressed);
    }
};

pub const MappedColumn = struct {
    ptr: []align(std.heap.page_size_min) u8,
    entries: []BlockEntry,
    width: usize,
    row_count: u64,
    block_rows: u32,

    pub fn open(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        expected_ty: schema.ColumnType,
        expected_rows: u64,
    ) !MappedColumn {
        const file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        const stat = try file.stat(io);
        if (stat.size < header_size) return error.InvalidHeader;
        const ptr = try std.posix.mmap(null, stat.size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(ptr);
        if (!std.mem.eql(u8, ptr[0..magic.len], magic)) return error.BadMagic;
        if (std.mem.readInt(u16, ptr[8..10], .little) != 2) return error.UnsupportedVersion;
        if (ptr[10] != @backingInt(expected_ty)) return error.InvalidHeader;
        const width = ptr[11];
        if (width != try typeWidth(expected_ty)) return error.InvalidHeader;
        const block_rows = std.mem.readInt(u32, ptr[12..16], .little);
        const row_count = std.mem.readInt(u64, ptr[16..24], .little);
        const block_count = std.mem.readInt(u32, ptr[24..28], .little);
        if (row_count != expected_rows or block_rows == 0) return error.InvalidHeader;
        if (std.mem.readInt(u16, ptr[28..30], .little) != directory_entry_size or
            std.mem.readInt(u16, ptr[30..32], .little) != header_size)
            return error.InvalidHeader;
        const directory_end = header_size + @as(usize, block_count) * directory_entry_size;
        if (directory_end > stat.size) return error.InvalidDirectory;

        const entries = try allocator.alloc(BlockEntry, block_count);
        errdefer allocator.free(entries);
        var expected_start: u64 = 0;
        for (entries, 0..) |*entry, i| {
            const off = header_size + i * directory_entry_size;
            entry.* = try decodeEntry(ptr[off..][0..directory_entry_size]);
            const payload_end = entry.data_offset + entry.compressed_size;
            if (entry.row_start != expected_start or
                entry.uncompressed_size != entry.row_count * width or
                entry.data_offset < directory_end or payload_end > stat.size)
                return error.InvalidDirectory;
            expected_start += entry.row_count;
        }
        if (expected_start != row_count) return error.InvalidDirectory;
        return .{
            .ptr = ptr,
            .entries = entries,
            .width = width,
            .row_count = row_count,
            .block_rows = block_rows,
        };
    }

    pub fn deinit(self: MappedColumn, allocator: std.mem.Allocator) void {
        allocator.free(self.entries);
        std.posix.munmap(self.ptr);
    }

    pub const RangeView = struct {
        bytes: []const u8,
        owned: bool,
    };

    /// Zero-copy view when the requested rows are backed by raw (uncompressed)
    /// blocks. Borrowed slices stay valid for the lifetime of the mapping.
    pub fn decodeRangeView(
        self: MappedColumn,
        allocator: std.mem.Allocator,
        start: u64,
        count: usize,
    ) !RangeView {
        if (start > self.row_count or count > self.row_count - start) return error.InvalidRange;
        if (count == 0) return .{ .bytes = &.{}, .owned = false };
        if (self.rawPayload()) |payload| {
            const byte_start: usize = @intCast(start * self.width);
            return .{ .bytes = payload[byte_start..][0 .. count * self.width], .owned = false };
        }
        const block_index: usize = @intCast(start / self.block_rows);
        if (block_index < self.entries.len) {
            const entry = self.entries[block_index];
            if (entry.codec == .raw and
                start >= entry.row_start and
                start + count <= entry.row_start + entry.row_count)
            {
                const local: usize = @intCast(start - entry.row_start);
                return .{
                    .bytes = self.ptr[entry.data_offset + local * self.width ..][0 .. count * self.width],
                    .owned = false,
                };
            }
        }
        return .{ .bytes = try self.decodeRange(allocator, start, count), .owned = true };
    }

    /// All payloads are raw and stored back-to-back: the whole column is one
    /// contiguous slice in the mapping.
    fn rawPayload(self: MappedColumn) ?[]const u8 {
        if (self.entries.len == 0) return null;
        var expected_offset = self.entries[0].data_offset;
        for (self.entries) |entry| {
            if (entry.codec != .raw or entry.data_offset != expected_offset) return null;
            expected_offset += entry.compressed_size;
        }
        return self.ptr[self.entries[0].data_offset..][0 .. self.row_count * self.width];
    }

    pub fn decodeRange(
        self: MappedColumn,
        allocator: std.mem.Allocator,
        start: u64,
        count: usize,
    ) ![]u8 {
        if (start > self.row_count or count > self.row_count - start) return error.InvalidRange;
        const out = try allocator.alloc(u8, count * self.width);
        errdefer allocator.free(out);
        if (count == 0) return out;

        const end = start + count;
        var block_index: usize = @intCast(start / self.block_rows);
        var out_rows: usize = 0;
        while (block_index < self.entries.len) : (block_index += 1) {
            const entry = self.entries[block_index];
            const block_end = entry.row_start + entry.row_count;
            if (entry.row_start >= end) break;
            if (block_end <= start) continue;

            const payload = self.ptr[entry.data_offset..][0..entry.compressed_size];
            const copy_start = @max(start, entry.row_start);
            const copy_end = @min(end, block_end);
            const local_start: usize = @intCast(copy_start - entry.row_start);
            const rows: usize = @intCast(copy_end - copy_start);
            const dest = out[out_rows * self.width ..][0 .. rows * self.width];

            // Whole blocks can decode straight into the result, avoiding a
            // per-block temporary allocation and copy.
            if (local_start == 0 and rows == entry.row_count) {
                switch (entry.codec) {
                    .raw => @memcpy(dest, payload),
                    .lz4 => {
                        const n = LZ4_decompress_safe(
                            payload.ptr,
                            dest.ptr,
                            @intCast(payload.len),
                            @intCast(dest.len),
                        );
                        if (n < 0 or n != dest.len) return error.CorruptBlock;
                    },
                }
                if (std.hash.Wyhash.hash(0, dest) != entry.checksum) return error.CorruptBlock;
                out_rows += rows;
                continue;
            }

            const decoded = try allocator.alloc(u8, entry.uncompressed_size);
            defer allocator.free(decoded);
            switch (entry.codec) {
                .raw => @memcpy(decoded, payload),
                .lz4 => {
                    const n = LZ4_decompress_safe(
                        payload.ptr,
                        decoded.ptr,
                        @intCast(payload.len),
                        @intCast(decoded.len),
                    );
                    if (n < 0 or n != decoded.len) return error.CorruptBlock;
                },
            }
            if (std.hash.Wyhash.hash(0, decoded) != entry.checksum) return error.CorruptBlock;
            @memcpy(
                dest,
                decoded[local_start * self.width ..][0 .. rows * self.width],
            );
            out_rows += rows;
        }
        if (out_rows != count) return error.InvalidDirectory;
        return out;
    }

    pub fn compressedBytesForRange(self: MappedColumn, start: u64, count: usize) u64 {
        if (count == 0 or start >= self.row_count) return 0;
        const end = @min(self.row_count, start + count);
        var block_index: usize = @intCast(start / self.block_rows);
        var total: u64 = 0;
        while (block_index < self.entries.len) : (block_index += 1) {
            const entry = self.entries[block_index];
            if (entry.row_start >= end) break;
            if (entry.row_start + entry.row_count > start) total +|= entry.compressed_size;
        }
        return total;
    }
};

test "Store V2 fixed column round trip and range decode" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const path = "/tmp/zighouse_store_v2_roundtrip.bin";
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    var writer = try Writer.open(allocator, io, path, .int32, 10, 4);
    defer writer.deinit();
    for (0..10) |i| try writer.writeI32(@intCast(i * 3));
    try writer.finish();

    const mapped = try MappedColumn.open(allocator, io, path, .int32, 10);
    defer mapped.deinit(allocator);
    const bytes = try mapped.decodeRange(allocator, 3, 5);
    defer allocator.free(bytes);
    const values = std.mem.bytesAsSlice(i32, bytes);
    try std.testing.expectEqualSlices(i32, &.{ 9, 12, 15, 18, 21 }, values);
    try std.testing.expectEqual(@as(i64, 0), @as(i64, @bitCast(mapped.entries[0].min_bits)));
    try std.testing.expectEqual(@as(i64, 9), @as(i64, @bitCast(mapped.entries[0].max_bits)));
}

test "Store V2 raw ranges are borrowed from the mapping" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const path = "/tmp/zighouse_store_v2_raw_view.bin";
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    var writer = try Writer.open(allocator, io, path, .int64, 16, 8);
    defer writer.deinit();
    for (0..16) |i| try writer.writeI64(@intCast(i * 10));
    try writer.finish();

    const mapped = try MappedColumn.open(allocator, io, path, .int64, 16);
    defer mapped.deinit(allocator);
    const view = try mapped.decodeRangeView(allocator, 3, 5);
    defer if (view.owned) allocator.free(view.bytes);
    try std.testing.expect(!view.owned);
    const values = std.mem.bytesAsSlice(i64, view.bytes);
    try std.testing.expectEqualSlices(i64, &.{ 30, 40, 50, 60, 70 }, values);
}

test "Store V2 compressed ranges own their decoded bytes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const path = "/tmp/zighouse_store_v2_lz4_view.bin";
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    var writer = try Writer.open(allocator, io, path, .int64, 1000, 1000);
    defer writer.deinit();
    for (0..1000) |_| try writer.writeI64(7);
    try writer.finish();

    const mapped = try MappedColumn.open(allocator, io, path, .int64, 1000);
    defer mapped.deinit(allocator);
    const view = try mapped.decodeRangeView(allocator, 10, 20);
    defer if (view.owned) allocator.free(view.bytes);
    try std.testing.expect(view.owned);
    const values = std.mem.bytesAsSlice(i64, view.bytes);
    try std.testing.expectEqual(@as(usize, 20), values.len);
    for (values) |v| try std.testing.expectEqual(@as(i64, 7), v);
}

test "Store V2 detects payload corruption" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const path = "/tmp/zighouse_store_v2_corrupt.bin";
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};

    var writer = try Writer.open(allocator, io, path, .int64, 8, 8);
    defer writer.deinit();
    for (0..8) |_| try writer.writeI64(42);
    try writer.finish();
    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    const payload_offset = header_size + directory_entry_size;
    var byte: [1]u8 = undefined;
    _ = try file.readPositionalAll(io, &byte, payload_offset);
    byte[0] ^= 0x55;
    try file.writePositionalAll(io, &byte, payload_offset);

    const mapped = try MappedColumn.open(allocator, io, path, .int64, 8);
    defer mapped.deinit(allocator);
    try std.testing.expectError(error.CorruptBlock, mapped.decodeRange(allocator, 0, 8));
}
