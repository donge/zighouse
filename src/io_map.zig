// Memory-mapped column loader. Validated by A.4 micro-bench:
// mmap+reduce is 1.25-1.75x faster than readAlloc+reduce on warm cache,
// and avoids the 1.5 GB memcpy on cold start since the kernel page-faults
// pages directly into the mapped region.
//
// Mapped slices are owned by `Mapping` and must be released with `unmap`.

const std = @import("std");
const posix = std.posix;

pub const Mapping = struct {
    raw: []align(std.heap.page_size_min) const u8,

    pub fn unmap(self: Mapping) void {
        posix.munmap(self.raw);
    }
};

pub fn MappedColumn(comptime T: type) type {
    return struct {
        values: []const T,
        mapping: Mapping,
    };
}

pub fn mapFile(io: std.Io, path: []const u8) !Mapping {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const len = try file.length(io);
    const size: usize = @intCast(len);
    if (size == 0) {
        // mmap of zero-length file is invalid on Darwin; return empty mapping.
        const empty: []align(std.heap.page_size_min) const u8 = &.{};
        return .{ .raw = empty };
    }
    const fd = file.handle;
    const mapped = try posix.mmap(
        null,
        size,
        .{ .READ = true },
        .{ .TYPE = .PRIVATE },
        fd,
        0,
    );
    return .{ .raw = mapped };
}

pub fn mapColumn(comptime T: type, io: std.Io, path: []const u8) !MappedColumn(T) {
    const mapping = try mapFile(io, path);
    if (mapping.raw.len % @sizeOf(T) != 0) {
        mapping.unmap();
        return error.CorruptHotColumns;
    }
    const elem_count = mapping.raw.len / @sizeOf(T);
    const ptr: [*]const T = @ptrCast(@alignCast(mapping.raw.ptr));
    return .{ .values = ptr[0..elem_count], .mapping = mapping };
}
