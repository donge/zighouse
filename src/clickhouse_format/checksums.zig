/// ClickHouse MergeTree `checksums.txt` writer (format version 4).
///
/// Binary layout:
///   "checksums format version: 4\n"   (written uncompressed to outer file)
///   <one LZ4 compressed block>        (using block.zig writeBlock)
///     inside the block:
///       varuint: N (number of files)
///       for each file (sorted by name):
///         writeBinary(name): varuint(len) + bytes
///         varuint: file_size (bytes on disk)
///         [16 bytes]: file_hash (CityHash128 of file bytes, lo64 LE + hi64 LE)
///         u8: is_compressed (0=raw file, 1=compressed .bin file)
///         if is_compressed == 1:
///           varuint: uncompressed_size
///           [16 bytes]: uncompressed_hash (CityHash128 of decompressed bytes)
///
/// For .bin files (compressed): is_compressed=1.
/// For .mrk2, columns.txt, count.txt, primary.idx: is_compressed=0.
///
/// Reference: src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp

const std = @import("std");
const block = @import("block.zig");
const cityhash = @import("cityhash.zig");
const primary_idx = @import("primary_idx.zig"); // for writeVarint

/// A file checksum entry.
pub const FileChecksum = struct {
    /// Filename (relative within the part directory, e.g. "CounterID.bin").
    name: []const u8,
    /// Size of the file on disk (compressed size for .bin, raw size for others).
    file_size: u64,
    /// CityHash128 of the file bytes on disk (the compressed bytes for .bin).
    file_hash: u128,
    /// Whether the file is a compressed .bin file.
    is_compressed: bool,
    /// Uncompressed size (only when is_compressed=true).
    uncompressed_size: u64 = 0,
    /// CityHash128 of the decompressed content (only when is_compressed=true).
    uncompressed_hash: u128 = 0,
};

/// Write checksums.txt to `writer` for the given file entries.
/// `entries` must be sorted by name (ascending) — CH requires sorted order.
pub fn write(
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
    entries: []const FileChecksum,
) !void {
    // 1. Uncompressed text header
    try writer.writeAll("checksums format version: 4\n");

    // 2. Build the inner binary payload in memory
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(allocator);

    // Write payload into a fixed/growing buffer via an ArrayList writer shim
    var pw = PayloadWriter{ .list = &payload, .allocator = allocator };

    // varuint: number of entries
    try writeVarint(&pw, entries.len);

    for (entries) |e| {
        // writeBinary(name): varuint(len) + bytes
        try writeVarint(&pw, e.name.len);
        try pw.writeAll(e.name);
        // varuint: file_size
        try writeVarint(&pw, e.file_size);
        // [16 bytes]: file_hash
        try writeHash128(&pw, e.file_hash);
        // u8: is_compressed
        const comp: u8 = if (e.is_compressed) 1 else 0;
        try pw.writeAll((&comp)[0..1]);
        if (e.is_compressed) {
            try writeVarint(&pw, e.uncompressed_size);
            try writeHash128(&pw, e.uncompressed_hash);
        }
    }

    // 3. Write the compressed block containing the payload
    try block.writeBlock(writer, payload.items);
}

/// Compute CityHash128 of a file's bytes, returning as u128.
pub fn hashFile(data: []const u8) u128 {
    return cityhash.cityHash128(data);
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// A minimal writer wrapper backed by an ArrayList(u8).
const PayloadWriter = struct {
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    fn writeAll(self: *PayloadWriter, bytes: []const u8) !void {
        try self.list.appendSlice(self.allocator, bytes);
    }
};

fn writeVarint(pw: *PayloadWriter, value: anytype) !void {
    const v: u64 = @intCast(value);
    var buf: [10]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try primary_idx.writeVarint(&w, v);
    try pw.writeAll(std.Io.Writer.buffered(&w));
}

fn writeHash128(pw: *PayloadWriter, hash: u128) !void {
    var buf: [16]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], @truncate(hash), .little);
    std.mem.writeInt(u64, buf[8..16], @truncate(hash >> 64), .little);
    try pw.writeAll(&buf);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "checksums write produces decompressible block" {
    const allocator = std.testing.allocator;

    const entries = [_]FileChecksum{
        .{
            .name = "CounterID.bin",
            .file_size = 1024,
            .file_hash = 0xDEADBEEFCAFEBABE1234567890ABCDEF,
            .is_compressed = true,
            .uncompressed_size = 8192 * 4,
            .uncompressed_hash = 0x0102030405060708090A0B0C0D0E0F10,
        },
        .{
            .name = "count.txt",
            .file_size = 9,
            .file_hash = 0xAABBCCDDEEFF00112233445566778899,
            .is_compressed = false,
        },
    };

    // Write to a buffer
    const out_buf = try allocator.alloc(u8, 4096);
    defer allocator.free(out_buf);
    var w = std.Io.Writer.fixed(out_buf);
    try write(allocator, &w, &entries);

    const written = std.Io.Writer.buffered(&w);

    // Check header
    const header = "checksums format version: 4\n";
    try std.testing.expect(written.len > header.len);
    try std.testing.expectEqualStrings(header, written[0..header.len]);

    // The rest should be a valid compressed block — decompress it
    var r = std.Io.Reader.fixed(written[header.len..]);
    const decompressed = try block.readBlock(allocator, &r);
    defer allocator.free(decompressed);

    // Payload must start with varuint(2) for 2 entries
    try std.testing.expect(decompressed.len > 0);
    try std.testing.expectEqual(@as(u8, 2), decompressed[0]); // varuint(2) = 0x02
}

test "checksums hashFile" {
    // Verify CityHash128 of empty string produces a consistent result
    const h = hashFile("");
    _ = h; // Just ensure it compiles and doesn't crash
}
