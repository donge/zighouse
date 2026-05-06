// A.4 micro-benchmark: mmap vs readFileAlloc on cold and warm caches.
//
// Compares wall-clock for "load + sumI16" with two strategies:
//   - readAlloc: read whole file into heap, run sumI16
//   - mmap:    mmap PROT_READ MAP_PRIVATE, run sumI16 directly
//
// Run with `purge` (macOS) before each invocation to drop the page cache and
// observe cold-start behaviour. Without purge it measures warm-cache.
//
// Gate: mmap (load + reduce) must be <= readAlloc (load + reduce) for both
// cold and warm to graduate into src/io_map.zig.

const std = @import("std");
const posix = std.posix;
const simd = @import("simd.zig");

const iterations: usize = 3;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();
    const data_dir = args.next() orelse {
        try writeErr(io, "usage: bench-mmap <data_dir>\n");
        return error.MissingDataDir;
    };

    try benchFile(allocator, io, data_dir, "hot_AdvEngineID.i16");
    try benchFile(allocator, io, data_dir, "hot_ResolutionWidth.i16");
    try benchFile(allocator, io, data_dir, "hot_UserID.i64");
}

fn benchFile(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, name: []const u8) !void {
    const path = try std.fs.path.joinZ(allocator, &.{ data_dir, name });
    defer allocator.free(path);

    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    const file_size_u64 = try file.length(io);
    const file_size: usize = @intCast(file_size_u64);
    file.close(io);
    try printOut(io, "\n=== {s} ({d} MB) ===\n", .{ name, file_size / (1024 * 1024) });

    // readAlloc strategy
    {
        var samples: [iterations]u64 = undefined;
        var hash: i64 = 0;
        for (0..iterations) |idx| {
            const start = std.Io.Clock.awake.now(io);
            const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
            defer allocator.free(bytes);
            const partial = sumOver(bytes, name);
            const stop = std.Io.Clock.awake.now(io);
            samples[idx] = @intCast(stop.nanoseconds - start.nanoseconds);
            hash ^= partial;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  readAlloc + reduce     median {d:>8} us   min {d:>8} us   sum {x}\n", .{
            samples[iterations / 2] / 1000, samples[0] / 1000, @as(u64, @bitCast(hash)),
        });
    }

    // mmap strategy
    {
        var samples: [iterations]u64 = undefined;
        var hash: i64 = 0;
        for (0..iterations) |idx| {
            const start = std.Io.Clock.awake.now(io);
            const fd = std.c.open(path.ptr, .{ .ACCMODE = .RDONLY });
            if (fd < 0) return error.OpenFailed;
            defer _ = std.c.close(fd);
            const mapped = try posix.mmap(null, file_size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0);
            defer posix.munmap(mapped);
            const partial = sumOver(mapped, name);
            const stop = std.Io.Clock.awake.now(io);
            samples[idx] = @intCast(stop.nanoseconds - start.nanoseconds);
            hash ^= partial;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  mmap + reduce          median {d:>8} us   min {d:>8} us   sum {x}\n", .{
            samples[iterations / 2] / 1000, samples[0] / 1000, @as(u64, @bitCast(hash)),
        });
    }
}

fn sumOver(bytes: []const u8, name: []const u8) i64 {
    if (std.mem.endsWith(u8, name, ".i16")) {
        const elem_count = bytes.len / @sizeOf(i16);
        const ptr: [*]const i16 = @ptrCast(@alignCast(bytes.ptr));
        return simd.sumI16(ptr[0..elem_count]);
    }
    if (std.mem.endsWith(u8, name, ".i64")) {
        const elem_count = bytes.len / @sizeOf(i64);
        const ptr: [*]const i64 = @ptrCast(@alignCast(bytes.ptr));
        var acc: i64 = 0;
        for (ptr[0..elem_count]) |v| acc +%= v;
        return acc;
    }
    return @intCast(bytes.len);
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [512]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

fn writeErr(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stderr().writeStreamingAll(io, bytes);
}
