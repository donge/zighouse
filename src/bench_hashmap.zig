//! Compare custom open-addressing HashU64Count vs std.AutoHashMap on the
//! Q17 workload: build groups for (UserID id, SearchPhrase id) over 100M rows.
//!
//! Inputs (relative to <data_dir>):
//!   hot_UserID.id           u32 column
//!   hot_SearchPhrase.id     u32 column
//!
//! Reports wall-clock for end-to-end build + final group count, three runs.

const std = @import("std");
const posix = std.posix;
const hashmap = @import("hashmap.zig");

const iterations: usize = 3;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();
    const data_dir = args.next() orelse {
        try writeErr(io, "usage: bench-hashmap <data_dir>\n");
        return error.MissingDataDir;
    };

    const uid = try mapU32(allocator, data_dir, "hot_UserID.id");
    defer posix.munmap(uid.bytes);
    const phrase = try mapU32(allocator, data_dir, "hot_SearchPhrase.id");
    defer posix.munmap(phrase.bytes);

    if (uid.values.len != phrase.values.len) return error.LengthMismatch;
    const n = uid.values.len;
    try printOut(io, "rows: {d}\n", .{n});

    // Materialize packed u64 keys once (so the hashmap loops measure the
    // hashmap, not the key construction).
    const keys = try allocator.alloc(u64, n);
    defer allocator.free(keys);
    for (0..n) |i| {
        keys[i] = (@as(u64, uid.values[i]) << 32) | @as(u64, phrase.values[i]);
    }

    // Custom open-addressing, scalar.
    {
        var samples: [iterations]u64 = undefined;
        var len_hash: usize = 0;
        for (0..iterations) |it| {
            var m = try hashmap.HashU64Count.init(allocator, 32 * 1024 * 1024);
            defer m.deinit();
            const start = std.Io.Clock.awake.now(io);
            for (keys) |k| m.bump(k);
            const stop = std.Io.Clock.awake.now(io);
            samples[it] = @intCast(stop.nanoseconds - start.nanoseconds);
            len_hash ^= m.len;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  HashU64Count scalar    median {d:>7} ms   min {d:>7} ms   len_xor {d}\n", .{
            samples[iterations / 2] / 1_000_000, samples[0] / 1_000_000, len_hash,
        });
    }

    // Custom open-addressing, batched-prefetch (batch=8).
    {
        var samples: [iterations]u64 = undefined;
        var len_hash: usize = 0;
        for (0..iterations) |it| {
            var m = try hashmap.HashU64Count.init(allocator, 32 * 1024 * 1024);
            defer m.deinit();
            const start = std.Io.Clock.awake.now(io);
            m.bumpBatched(keys, 8);
            const stop = std.Io.Clock.awake.now(io);
            samples[it] = @intCast(stop.nanoseconds - start.nanoseconds);
            len_hash ^= m.len;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  HashU64Count batch=8   median {d:>7} ms   min {d:>7} ms   len_xor {d}\n", .{
            samples[iterations / 2] / 1_000_000, samples[0] / 1_000_000, len_hash,
        });
    }

    // Custom open-addressing, batched-prefetch (batch=16).
    {
        var samples: [iterations]u64 = undefined;
        var len_hash: usize = 0;
        for (0..iterations) |it| {
            var m = try hashmap.HashU64Count.init(allocator, 32 * 1024 * 1024);
            defer m.deinit();
            const start = std.Io.Clock.awake.now(io);
            m.bumpBatched(keys, 16);
            const stop = std.Io.Clock.awake.now(io);
            samples[it] = @intCast(stop.nanoseconds - start.nanoseconds);
            len_hash ^= m.len;
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  HashU64Count batch=16  median {d:>7} ms   min {d:>7} ms   len_xor {d}\n", .{
            samples[iterations / 2] / 1_000_000, samples[0] / 1_000_000, len_hash,
        });
    }

    // std.AutoHashMap baseline.
    {
        var samples: [iterations]u64 = undefined;
        var len_hash: usize = 0;
        for (0..iterations) |it| {
            var m = std.AutoHashMap(u64, u32).init(allocator);
            defer m.deinit();
            try m.ensureTotalCapacity(32 * 1024 * 1024);
            const start = std.Io.Clock.awake.now(io);
            for (keys) |k| {
                const gop = try m.getOrPut(k);
                if (!gop.found_existing) gop.value_ptr.* = 0;
                gop.value_ptr.* += 1;
            }
            const stop = std.Io.Clock.awake.now(io);
            samples[it] = @intCast(stop.nanoseconds - start.nanoseconds);
            len_hash ^= m.count();
        }
        std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
        try printOut(io, "  AutoHashMap (baseline) median {d:>7} ms   min {d:>7} ms   len_xor {d}\n", .{
            samples[iterations / 2] / 1_000_000, samples[0] / 1_000_000, len_hash,
        });
    }
}

const MappedU32 = struct { bytes: []align(std.heap.page_size_min) u8, values: []const u32 };

fn mapU32(allocator: std.mem.Allocator, data_dir: []const u8, name: []const u8) !MappedU32 {
    const path = try std.fs.path.joinZ(allocator, &.{ data_dir, name });
    defer allocator.free(path);
    const fd = std.c.open(path.ptr, .{ .ACCMODE = .RDONLY });
    if (fd < 0) return error.OpenFailed;
    defer _ = std.c.close(fd);
    var stat: std.c.Stat = undefined;
    if (std.c.fstat(fd, &stat) != 0) return error.StatFailed;
    const size: usize = @intCast(stat.size);
    const mapped = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0);
    const elem = size / @sizeOf(u32);
    const ptr: [*]const u32 = @ptrCast(@alignCast(mapped.ptr));
    return .{ .bytes = mapped, .values = ptr[0..elem] };
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [512]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

fn writeErr(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stderr().writeStreamingAll(io, bytes);
}
