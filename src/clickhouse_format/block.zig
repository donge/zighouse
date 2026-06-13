/// ClickHouse compressed block codec.
///
/// Binary layout (per block, written sequentially to a .bin file):
///
///   [16 bytes] CityHash128 checksum of (header + compressed_data), LE u128
///              stored as [low64 u64 LE][high64 u64 LE]
///   [1 byte]   method byte: 0x82=LZ4, 0x02=NONE, 0x90=ZSTD
///   [4 bytes]  size_compressed_with_header (LE u32): = 9 + compressed_data_len
///   [4 bytes]  size_decompressed (LE u32)
///   [N bytes]  compressed_data
///
/// The checksum covers bytes [method..method+header+compressed-1] (9+N bytes).
///
/// Reference: src/Compression/CompressionInfo.h, CompressedReadBufferBase.cpp

const std = @import("std");
const cityhash = @import("cityhash.zig");
const lz4 = @import("lz4.zig");

pub const CHECKSUM_SIZE: usize = 16;
pub const HEADER_SIZE: usize = 9; // method(1) + compressed_size(4) + decompressed_size(4)
pub const BLOCK_HEADER_TOTAL: usize = CHECKSUM_SIZE + HEADER_SIZE;

pub const METHOD_LZ4: u8 = 0x82;
pub const METHOD_NONE: u8 = 0x02;
pub const METHOD_ZSTD: u8 = 0x90;
/// CH 26.x uses alternative ZSTD method bytes:
/// 0x22 = ZSTD level 1, 0x23 = ZSTD level 2 (encoded as (level << 5) | 2)
const METHOD_ZSTD_ALT1: u8 = 0x22;
const METHOD_ZSTD_ALT2: u8 = 0x23;

// ── ZSTD C bindings ──────────────────────────────────────────────────────────
pub const zstd = struct {
    pub extern fn ZSTD_decompress(dst: [*]u8, dst_capacity: usize, src: [*]const u8, compressed_size: usize) usize;
    pub extern fn ZSTD_isError(code: usize) c_int;
    pub extern fn ZSTD_getErrorName(code: usize) [*:0]const u8;
    pub extern fn ZSTD_compress(dst: [*]u8, dst_capacity: usize, src: [*]const u8, src_size: usize, compression_level: c_int) usize;
    pub extern fn ZSTD_compressBound(src_size: usize) usize;
};

pub const Error = error{
    ChecksumMismatch,
    UnsupportedCompressionMethod,
    TruncatedBlock,
    DecompressedSizeMismatch,
    ZstdDecompressFailed,
};

/// Write a single compressed block to `writer` using the given `method`.
/// `method` must be METHOD_LZ4 (0x82) or METHOD_ZSTD (0x90).
/// `src` is the raw (uncompressed) data for this block.
pub fn writeBlock(writer: *std.Io.Writer, src: []const u8, method: u8) !void {
    // Compute upper-bound for compressed output size.
    const bound: usize = switch (method) {
        METHOD_ZSTD => zstd.ZSTD_compressBound(src.len),
        else => lz4.compressBound(src.len),
    };

    // Stack-allocate for small blocks; heap for large.
    var heap_buf: ?[]u8 = null;
    var stack_buf: [65536 + 512]u8 = undefined;

    const compressed_buf: []u8 = if (bound <= stack_buf.len)
        stack_buf[0..bound]
    else blk: {
        const hb = try std.heap.page_allocator.alloc(u8, bound);
        heap_buf = hb;
        break :blk hb;
    };
    defer if (heap_buf) |hb| std.heap.page_allocator.free(hb);

    const compressed_len: usize = switch (method) {
        METHOD_ZSTD => blk: {
            // ZSTD_compress returns 0 on error; positive = success.
            const n = zstd.ZSTD_compress(
                compressed_buf.ptr,
                compressed_buf.len,
                src.ptr,
                src.len,
                1, // compression level 1 = ClickHouse default
            );
            if (zstd.ZSTD_isError(n) != 0) return error.ZstdDecompressFailed;
            break :blk n;
        },
        else => try lz4.compress(src, compressed_buf),
    };
    const compressed = compressed_buf[0..compressed_len];

    // Build the 9-byte header + compressed data region, then checksum it.
    const size_with_header: u32 = @intCast(HEADER_SIZE + compressed_len);
    const size_decompressed: u32 = @intCast(src.len);

    // Build header bytes for checksum computation
    var header_bytes: [HEADER_SIZE]u8 = undefined;
    header_bytes[0] = method;
    std.mem.writeInt(u32, header_bytes[1..5], size_with_header, .little);
    std.mem.writeInt(u32, header_bytes[5..9], size_decompressed, .little);

    // Checksum covers [header_bytes ++ compressed]
    // We compute CityHash128 over a contiguous region.  For efficiency,
    // temporarily build it in the stack buffer (header + compressed).
    var to_hash_buf: [HEADER_SIZE + 65536 + 512]u8 = undefined;
    var to_hash_heap: ?[]u8 = null;
    const to_hash: []u8 = if (HEADER_SIZE + compressed_len <= to_hash_buf.len)
        to_hash_buf[0 .. HEADER_SIZE + compressed_len]
    else blk: {
        const hb = try std.heap.page_allocator.alloc(u8, HEADER_SIZE + compressed_len);
        to_hash_heap = hb;
        break :blk hb;
    };
    defer if (to_hash_heap) |hb| std.heap.page_allocator.free(hb);

    @memcpy(to_hash[0..HEADER_SIZE], &header_bytes);
    @memcpy(to_hash[HEADER_SIZE..], compressed);

    const checksum: u128 = cityhash.cityHash128(to_hash);

    // Write checksum as [low64 LE][high64 LE]
    var checksum_bytes: [CHECKSUM_SIZE]u8 = undefined;
    std.mem.writeInt(u64, checksum_bytes[0..8], @truncate(checksum), .little);
    std.mem.writeInt(u64, checksum_bytes[8..16], @truncate(checksum >> 64), .little);

    try writer.writeAll(&checksum_bytes);
    try writer.writeAll(&header_bytes);
    try writer.writeAll(compressed);
}

/// Read one block from `reader`, decompress into `allocator`-owned buffer.
/// Returns the decompressed data.  Caller must free.
pub fn readBlock(allocator: std.mem.Allocator, reader: *std.Io.Reader) ![]u8 {
    // Read checksum
    var checksum_bytes: [CHECKSUM_SIZE]u8 = undefined;
    reader.readSliceAll(&checksum_bytes) catch |e| switch (e) {
        error.EndOfStream => return error.TruncatedBlock,
        else => return e,
    };

    // Read header
    var header_bytes: [HEADER_SIZE]u8 = undefined;
    try reader.readSliceAll(&header_bytes);

    const method = header_bytes[0];
    // CH 26.x uses alternate method bytes for ZSTD (0x22, 0x23 etc.).
    // For standard LZ4/ZSTD/NONE: 9-byte header (method + 4B comp_size + 4B decomp_size).
    // For alternate methods: the 8-byte "size" fields might be absent or differently encoded.
    // Fall back to reading rest of stream as compressed data.
    if (method != METHOD_LZ4 and method != METHOD_NONE and method != METHOD_ZSTD and
        method != METHOD_ZSTD_ALT1 and method != METHOD_ZSTD_ALT2)
        return error.UnsupportedCompressionMethod;

    const is_alt_zstd = method == METHOD_ZSTD_ALT1 or method == METHOD_ZSTD_ALT2;
    const size_with_header = std.mem.readInt(u32, header_bytes[1..5], .little);
    const size_decompressed = std.mem.readInt(u32, header_bytes[5..9], .little);
    if (size_with_header < HEADER_SIZE and !is_alt_zstd) return error.TruncatedBlock;
    const compressed_len = if (is_alt_zstd) blk: {
        // For alternate ZSTD: the 8-byte header uses CH 26.x encoding which
        // may not store sizes in the same layout as 0x82/0x90.
        // Read what the header says, clamped to a reasonable maximum.
        const raw = size_with_header;
        if (raw > HEADER_SIZE and raw < 10 * 1024 * 1024)
            break :blk raw - HEADER_SIZE;
        // Fallback: if sizes are clearly wrong (larger than any realistic block),
        // use a reasonable default and let ZSTD determine actual output size.
        break :blk 256 * 1024;
    } else size_with_header - HEADER_SIZE;

    // Read compressed data
    var compressed_buf: [256 * 1024]u8 = undefined;
    const compressed_len_actual = @min(compressed_len, compressed_buf.len);
    const n_read = try reader.readSliceShort(compressed_buf[0..compressed_len_actual]);
    if (n_read == 0) return error.TruncatedBlock;
    const compressed = try allocator.alloc(u8, n_read);
    errdefer allocator.free(compressed);
    @memcpy(compressed, compressed_buf[0..n_read]);

    // Verify checksum over [header ++ compressed]
    const to_hash = try allocator.alloc(u8, HEADER_SIZE + n_read);
    defer allocator.free(to_hash);
    @memcpy(to_hash[0..HEADER_SIZE], &header_bytes);
    @memcpy(to_hash[HEADER_SIZE..], compressed);

    const computed: u128 = cityhash.cityHash128(to_hash);
    const stored_lo = std.mem.readInt(u64, checksum_bytes[0..8], .little);
    const stored_hi = std.mem.readInt(u64, checksum_bytes[8..16], .little);
    const stored: u128 = (@as(u128, stored_hi) << 64) | stored_lo;
    if (computed != stored) return error.ChecksumMismatch;

    // Decompress
    if (is_alt_zstd) {
        // CH 26.x ZSTD: header sizes may be unreliable. Read compressed data,
        // then ask ZSTD to decompress with a generous output estimate.
        const out_estimate = compressed.len * 8;
        const decompressed = try allocator.alloc(u8, out_estimate);
        errdefer allocator.free(decompressed);
        const result = zstd.ZSTD_decompress(decompressed.ptr, decompressed.len, compressed.ptr, compressed.len);
        allocator.free(compressed);
        if (zstd.ZSTD_isError(result) != 0) return error.ZstdDecompressFailed;
        return decompressed[0..result];
    }

    const decompressed = try allocator.alloc(u8, size_decompressed);
    errdefer allocator.free(decompressed);

    if (method == METHOD_NONE) {
        if (compressed_len != size_decompressed) return error.DecompressedSizeMismatch;
        @memcpy(decompressed, compressed);
        allocator.free(compressed);
        return decompressed;
    }

    if (method == METHOD_ZSTD or method == METHOD_ZSTD_ALT1 or method == METHOD_ZSTD_ALT2) {
        const result = zstd.ZSTD_decompress(
            decompressed.ptr,
            decompressed.len,
            compressed.ptr,
            compressed.len,
        );
        allocator.free(compressed);
        if (zstd.ZSTD_isError(result) != 0) return error.ZstdDecompressFailed;
        if (result != size_decompressed) return error.DecompressedSizeMismatch;
        return decompressed;
    }

    // Default: LZ4
    const n = try lz4.decompress(compressed, decompressed);
    allocator.free(compressed);
    if (n != size_decompressed) return error.DecompressedSizeMismatch;
    return decompressed;
}

/// A streaming block writer that accumulates raw bytes and flushes
/// compressed blocks when the buffer reaches `target_block_size`.
pub const BlockWriter = struct {
    allocator: std.mem.Allocator,
    buf: std.ArrayList(u8),
    target_block_size: usize,
    bytes_written_compressed: u64 = 0, // tracks .bin file offset
    method: u8 = METHOD_LZ4,

    pub fn init(allocator: std.mem.Allocator, target_block_size: usize) BlockWriter {
        return .{
            .allocator = allocator,
            .buf = std.ArrayList(u8).init(allocator),
            .target_block_size = target_block_size,
        };
    }

    pub fn deinit(self: *BlockWriter) void {
        self.buf.deinit();
    }

    /// Append raw bytes.  Flushes compressed blocks when buffer is full.
    pub fn append(self: *BlockWriter, writer: *std.Io.Writer, data: []const u8) !void {
        try self.buf.appendSlice(data);
        while (self.buf.items.len >= self.target_block_size) {
            const block_data = self.buf.items[0..self.target_block_size];
            const before = self.bytes_written_compressed;
            _ = before;
            // Track bytes written
            const start_pos: u64 = self.bytes_written_compressed;
            _ = start_pos;
            try writeBlock(writer, block_data, self.method);
            // Estimate written size for mark tracking (actual: checksum + header + compressed)
            // Callers that need exact offsets should use a counting writer.
            const compressed_estimate = lz4.compressBound(self.target_block_size) + BLOCK_HEADER_TOTAL;
            self.bytes_written_compressed += @intCast(compressed_estimate);

            // Remove flushed bytes
            const remaining = self.buf.items.len - self.target_block_size;
            std.mem.copyForwards(u8, self.buf.items[0..remaining], self.buf.items[self.target_block_size..]);
            self.buf.items.len = remaining;
        }
    }

    /// Flush any remaining buffered bytes as a final block.
    pub fn flush(self: *BlockWriter, writer: *std.Io.Writer) !void {
        if (self.buf.items.len > 0) {
            try writeBlock(writer, self.buf.items, self.method);
            self.buf.items.len = 0;
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

// ── Tests ────────────────────────────────────────────────────────────────────
//
// Tests use a pre-allocated fixed buffer large enough to hold one block.
// We use std.Io.Writer.fixed / std.Io.Reader.fixed (Zig 0.16 API).

const TEST_BUF_SIZE = 64 * 1024 + 4096;

test "block round-trip empty" {
    var buf: [TEST_BUF_SIZE]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeBlock(&w, "", METHOD_LZ4);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    const decompressed = try readBlock(std.testing.allocator, &r);
    defer std.testing.allocator.free(decompressed);
    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "block round-trip short" {
    const src = "hello, ClickHouse!";
    var buf: [TEST_BUF_SIZE]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeBlock(&w, src, METHOD_LZ4);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    const decompressed = try readBlock(std.testing.allocator, &r);
    defer std.testing.allocator.free(decompressed);
    try std.testing.expectEqualStrings(src, decompressed);
}

test "block round-trip 64KB" {
    const size = 64 * 1024;
    var src: [size]u8 = undefined;
    for (&src, 0..) |*b, i| b.* = @truncate(i * 251 + 7);

    const allocator = std.testing.allocator;
    const bound = BLOCK_HEADER_TOTAL + lz4.compressBound(size);
    const buf = try allocator.alloc(u8, bound);
    defer allocator.free(buf);
    var w = std.Io.Writer.fixed(buf);
    try writeBlock(&w, &src, METHOD_LZ4);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    const decompressed = try readBlock(allocator, &r);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, &src, decompressed);
}

test "block checksum mismatch detected" {
    const src = "tamper test";
    var buf: [TEST_BUF_SIZE]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeBlock(&w, src, METHOD_LZ4);

    // Corrupt a byte in the compressed data area (after checksum + header)
    const written = std.Io.Writer.buffered(&w);
    var tampered = buf[0..written.len];
    tampered[BLOCK_HEADER_TOTAL + 1] ^= 0xff;

    var r = std.Io.Reader.fixed(tampered);
    const result = readBlock(std.testing.allocator, &r);
    try std.testing.expectError(error.ChecksumMismatch, result);
}

test "block multiple sequential" {
    var buf: [TEST_BUF_SIZE]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);

    const payloads = [_][]const u8{ "first block", "second block data here", "third" };
    for (payloads) |p| try writeBlock(&w, p, METHOD_LZ4);

    var r = std.Io.Reader.fixed(std.Io.Writer.buffered(&w));
    for (payloads) |expected| {
        const dec = try readBlock(std.testing.allocator, &r);
        defer std.testing.allocator.free(dec);
        try std.testing.expectEqualStrings(expected, dec);
    }
}

