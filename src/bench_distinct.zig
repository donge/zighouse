// COUNT DISTINCT micro-benchmark for q9-like aggregation paths.
//
// It compares the existing epoch set against the flat hash set and a
// pair-hash partitioned epoch pass, so executor changes can be based on a
// measured mechanism instead of another plan-shape special case.

const std = @import("std");
const hashmap = @import("hashmap");

const ROWS: usize = 5_000_000;
const KEY_SPAN: usize = 4096;
const DVAL_SPAN: u64 = 1_000_000;
const ITERATIONS: usize = 5;
const PARTS: usize = 128;

const PairRec = extern struct {
    key: u32,
    dval: u64,
};

const Sample = struct { median_ns: u64, min_ns: u64, result: u64 };

var g_io: std.Io = undefined;

fn mix64(x0: u64) u64 {
    var x = x0 ^ (x0 >> 30);
    x *%= 0xbf58_476d_1ce4_e5b9;
    x ^= x >> 27;
    x *%= 0x94d0_49bb_1331_11eb;
    return x ^ (x >> 31);
}

fn pairKey(key: u32, dval: u64) u64 {
    return mix64((@as(u64, key) << 40) ^ dval);
}

fn makeData(keys: []u32, dvals: []u64) void {
    var rng = std.Random.DefaultPrng.init(0x9e37_79b9_7f4a_7c15);
    const r = rng.random();
    for (keys, dvals, 0..) |*k, *d, i| {
        const hot = r.uintLessThan(u32, @intCast(KEY_SPAN / 8));
        const cold = r.uintLessThan(u32, @intCast(KEY_SPAN));
        k.* = if ((i & 7) == 0) cold else hot;
        d.* = r.uintLessThan(u64, DVAL_SPAN);
    }
}

fn clearCounts(counts: []u32) void {
    @memset(counts, 0);
}

fn checksum(counts: []const u32) u64 {
    var h: u64 = 0xcbf2_9ce4_8422_2325;
    for (counts, 0..) |c, i| {
        h ^= (@as(u64, c) << 32) ^ @as(u64, @intCast(i));
        h *%= 0x1000_0000_01b3;
    }
    return h;
}

fn measure(comptime func: anytype, args: anytype) !Sample {
    var samples: [ITERATIONS]u64 = undefined;
    var result: u64 = 0;
    for (&samples) |*s| {
        const t0 = std.Io.Clock.Timestamp.now(g_io, .awake);
        result ^= try @call(.never_inline, func, args);
        const t1 = std.Io.Clock.Timestamp.now(g_io, .awake);
        s.* = @intCast(t0.durationTo(t1).raw.nanoseconds);
    }
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    return .{ .median_ns = samples[ITERATIONS / 2], .min_ns = samples[0], .result = result };
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buf: [256]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, fmt, args);
    try std.Io.File.stdout().writeStreamingAll(io, s);
}

fn report(io: std.Io, label: []const u8, s: Sample) !void {
    try printOut(io, "  {s:<30} median {d:>7} us   min {d:>7} us   hash {x:016}\n", .{
        label, s.median_ns / 1000, s.min_ns / 1000, s.result,
    });
}

fn benchGlobalEpoch(keys: []const u32, dvals: []const u64, counts: []u32, alloc: std.mem.Allocator) !u64 {
    clearCounts(counts);
    var seen = try hashmap.DistinctEpochSet.init(alloc, keys.len);
    defer seen.deinit();
    for (keys, dvals) |k, d| {
        if (seen.needsGrow()) try seen.growDouble();
        if (seen.insertNew(pairKey(k, d))) counts[k] += 1;
    }
    return checksum(counts);
}

fn benchGlobalFlat(keys: []const u32, dvals: []const u64, counts: []u32, alloc: std.mem.Allocator) !u64 {
    clearCounts(counts);
    var seen = try hashmap.HashU64Count.init(alloc, keys.len);
    defer seen.deinit();
    for (keys, dvals) |k, d| {
        if (seen.bumpNew(pairKey(k, d))) counts[k] += 1;
    }
    return checksum(counts);
}

fn benchPairPartitionEpoch(keys: []const u32, dvals: []const u64, counts: []u32, alloc: std.mem.Allocator) !u64 {
    var parts = try alloc.alloc(std.ArrayListUnmanaged(PairRec), PARTS);
    defer alloc.free(parts);
    for (parts) |*p| p.* = .empty;
    defer for (parts) |*p| p.deinit(alloc);

    const reserve = keys.len / PARTS + 1024;
    for (parts) |*p| try p.ensureTotalCapacity(alloc, reserve);

    for (keys, dvals) |k, d| {
        const part = mix64(pairKey(k, d)) & (PARTS - 1);
        try parts[@intCast(part)].append(alloc, .{ .key = k, .dval = d });
    }

    clearCounts(counts);
    var seen = try hashmap.DistinctEpochSet.init(alloc, reserve);
    defer seen.deinit();
    for (parts) |*p| {
        seen.clearForNextPartition();
        for (p.items) |rec| {
            if (seen.needsGrow()) try seen.growDouble();
            if (seen.insertNew(pairKey(rec.key, rec.dval))) counts[rec.key] += 1;
        }
    }
    return checksum(counts);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    g_io = io;

    const keys = try allocator.alloc(u32, ROWS);
    defer allocator.free(keys);
    const dvals = try allocator.alloc(u64, ROWS);
    defer allocator.free(dvals);
    makeData(keys, dvals);

    const counts = try allocator.alloc(u32, KEY_SPAN);
    defer allocator.free(counts);

    try printOut(io, "\n=== bench_distinct: {d} rows, {d} keys, {d} dvals, {d} iterations ===\n\n", .{
        ROWS, KEY_SPAN, DVAL_SPAN, ITERATIONS,
    });

    const s1 = try measure(benchGlobalEpoch, .{ keys, dvals, counts, allocator });
    try report(io, "global DistinctEpochSet", s1);

    const s2 = try measure(benchGlobalFlat, .{ keys, dvals, counts, allocator });
    try report(io, "global HashU64Count set", s2);

    const s3 = try measure(benchPairPartitionEpoch, .{ keys, dvals, counts, allocator });
    try report(io, "pair partition epoch", s3);

    if (s1.result != s2.result or s1.result != s3.result) {
        try printOut(io, "\n  *** CORRECTNESS FAILURE: {x} {x} {x}\n", .{ s1.result, s2.result, s3.result });
        std.process.exit(1);
    }
    try printOut(io, "\n  All results match. OK\n\n", .{});
}
