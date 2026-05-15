/// LZ4 block compression/decompression via liblz4.
///
/// ClickHouse uses **LZ4 block format** (not LZ4 frame format):
///   - LZ4_compress_default / LZ4_compress_HC  → compress
///   - LZ4_decompress_safe                     → decompress
///
/// This module wraps the C API.  Build.zig must:
///   exe.root_module.linkSystemLibrary("lz4", .{});
///   exe.root_module.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/lz4/include" });

const std = @import("std");
const c = @cImport({
    @cInclude("lz4.h");
});

pub const Error = error{
    LZ4CompressFailed,
    LZ4DecompressFailed,
    OutputTooSmall,
};

/// Maximum compressed size for a given source size (upper bound).
pub fn compressBound(source_size: usize) usize {
    return @intCast(c.LZ4_compressBound(@intCast(source_size)));
}

/// Compress `src` into `dst`.  `dst` must be at least `compressBound(src.len)` bytes.
/// Returns the number of bytes written into `dst`.
pub fn compress(src: []const u8, dst: []u8) !usize {
    if (dst.len < compressBound(src.len)) return error.OutputTooSmall;
    const n = c.LZ4_compress_default(
        src.ptr,
        dst.ptr,
        @intCast(src.len),
        @intCast(dst.len),
    );
    if (n <= 0) return error.LZ4CompressFailed;
    return @intCast(n);
}

/// Decompress `src` into `dst`.  `dst` must be large enough to hold the original data
/// (size is known from the ClickHouse block header).
/// Returns the number of bytes written.
pub fn decompress(src: []const u8, dst: []u8) !usize {
    const n = c.LZ4_decompress_safe(
        src.ptr,
        dst.ptr,
        @intCast(src.len),
        @intCast(dst.len),
    );
    if (n < 0) return error.LZ4DecompressFailed;
    return @intCast(n);
}

// ── Tests ────────────────────────────────────────────────────────────────────

test "lz4 round-trip empty" {
    var buf: [32]u8 = undefined;
    const n = try compress("", &buf);
    var out: [32]u8 = undefined;
    const m = try decompress(buf[0..n], &out);
    try std.testing.expectEqual(@as(usize, 0), m);
}

test "lz4 round-trip short" {
    const src = "hello, world!";
    var compressed: [64]u8 = undefined;
    const cn = try compress(src, &compressed);
    var decompressed: [64]u8 = undefined;
    const dn = try decompress(compressed[0..cn], &decompressed);
    try std.testing.expectEqualStrings(src, decompressed[0..dn]);
}

test "lz4 round-trip 1MB" {
    const allocator = std.testing.allocator;
    const src = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(src);
    for (src, 0..) |*b, i| b.* = @truncate(i * 7 + 3);

    const bound = compressBound(src.len);
    const compressed = try allocator.alloc(u8, bound);
    defer allocator.free(compressed);
    const cn = try compress(src, compressed);

    const decompressed = try allocator.alloc(u8, src.len);
    defer allocator.free(decompressed);
    const dn = try decompress(compressed[0..cn], decompressed);

    try std.testing.expectEqual(src.len, dn);
    try std.testing.expectEqualSlices(u8, src, decompressed[0..dn]);
}
