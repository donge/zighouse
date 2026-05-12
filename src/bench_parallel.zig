// A.3 micro-benchmark: parallel sumI16 fan-out vs single-thread SIMD.
//
// Splits the column into N equal chunks and spawns N threads, each running
// simd.sum(i16, ...) on each chunk; the main thread sums the partials.
//
// Gate: parallel must be >= 2.5x single-thread to graduate into
// src/parallel.zig.

const std = @import("std");
const simd = @import("simd.zig");

const iterations: usize = 5;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();
    const data_dir = args.next() orelse {
        try writeErr(io, "usage: bench-parallel <data_dir>\n");
        return error.MissingDataDir;
    };

    try benchI16(allocator, io, data_dir, "hot_AdvEngineID.i16");
    try benchI16(allocator, io, data_dir, "hot_ResolutionWidth.i16");
}

fn benchI16(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, name: []const u8) !void {
    const path = try std.fs.path.join(allocator, &.{ data_dir, name });
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(bytes);
    const elem_count = bytes.len / @sizeOf(i16);
    const values = try allocator.alignedAlloc(i16, .@"64", elem_count);
    defer allocator.free(values);
    @memcpy(std.mem.sliceAsBytes(values), bytes[0 .. elem_count * @sizeOf(i16)]);

    try printOut(io, "\n=== {s} ({d} elements, {d} MB) ===\n", .{ name, values.len, bytes.len / (1024 * 1024) });

    try benchSingle(io, values);
    try benchThreaded(allocator, io, values, 2);
    try benchThreaded(allocator, io, values, 4);
    try benchThreaded(allocator, io, values, 6);
    try benchThreaded(allocator, io, values, 8);
}

fn benchSingle(io: std.Io, values: []const i16) !void {
    var samples: [iterations]u64 = undefined;
    var sum: i64 = 0;
    for (0..iterations) |idx| {
        const start = std.Io.Clock.awake.now(io);
        const partial = simd.sum(i16, values);
        const stop = std.Io.Clock.awake.now(io);
        samples[idx] = @intCast(stop.nanoseconds - start.nanoseconds);
        sum ^= partial;
    }
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    try printOut(io, "  sumI16 1-thread        median {d:>8} us   min {d:>8} us   sum {x}\n", .{
        samples[iterations / 2] / 1000, samples[0] / 1000, @as(u64, @bitCast(sum)),
    });
}

const ThreadCtx = struct {
    chunk: []const i16,
    out: i64 = 0,
};

fn workerSum(ctx: *ThreadCtx) void {
    ctx.out = simd.sum(i16, ctx.chunk);
}

fn benchThreaded(allocator: std.mem.Allocator, io: std.Io, values: []const i16, n_threads: usize) !void {
    var samples: [iterations]u64 = undefined;
    var hash: i64 = 0;
    for (0..iterations) |idx| {
        const start = std.Io.Clock.awake.now(io);

        const chunk = (values.len + n_threads - 1) / n_threads;
        var ctxs = try allocator.alloc(ThreadCtx, n_threads);
        defer allocator.free(ctxs);
        var threads = try allocator.alloc(std.Thread, n_threads - 1);
        defer allocator.free(threads);

        for (0..n_threads) |t| {
            const lo = t * chunk;
            const hi = @min(lo + chunk, values.len);
            ctxs[t] = .{ .chunk = values[lo..hi] };
        }

        // spawn workers 1..n_threads-1, run worker 0 inline on main thread
        for (0..n_threads - 1) |t| {
            threads[t] = try std.Thread.spawn(.{}, workerSum, .{&ctxs[t + 1]});
        }
        workerSum(&ctxs[0]);
        for (threads) |th| th.join();

        var total: i64 = 0;
        for (ctxs) |c| total += c.out;
        const stop = std.Io.Clock.awake.now(io);
        samples[idx] = @intCast(stop.nanoseconds - start.nanoseconds);
        hash ^= total;
    }
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    try printOut(io, "  sumI16 {d}-thread        median {d:>8} us   min {d:>8} us   sum {x}\n", .{
        n_threads, samples[iterations / 2] / 1000, samples[0] / 1000, @as(u64, @bitCast(hash)),
    });
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [512]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

fn writeErr(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stderr().writeStreamingAll(io, bytes);
}
