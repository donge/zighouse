// A.1 micro-benchmarks: scalar vs @Vector(N) reduce on real hot columns.
// Run with: zig build bench-simd -Doptimize=ReleaseFast -- <data_dir>
//
// Measures wall-clock for each primitive on the actual hot column files
// living in <data_dir> (e.g. data/store_hot). Prints median of N iterations.
// Gate: SIMD must be >= 1.5x faster than scalar on the full column for the
// primitive to graduate into src/simd.zig.

const std = @import("std");

const iterations: usize = 5;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();
    const data_dir = args.next() orelse {
        try writeErr(io, "usage: bench-simd <data_dir>\n");
        return error.MissingDataDir;
    };

    try benchI16(allocator, io, data_dir, "hot_AdvEngineID.i16");
    try benchI16(allocator, io, data_dir, "hot_ResolutionWidth.i16");
    try benchI32(allocator, io, data_dir, "hot_EventDate.i32");
    try benchI64(allocator, io, data_dir, "hot_UserID.i64");
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
    const slice: []const i16 = values;
    try printOut(io, "\n=== {s} ({d} elements, {d} MB) ===\n", .{ name, slice.len, bytes.len / (1024 * 1024) });

    try report(io, "sumI16 scalar", measure(io, sumI16Scalar, .{slice}));
    try report(io, "sumI16 simd32", measure(io, sumI16Simd32, .{slice}));
    try report(io, "sumI16 simd64", measure(io, sumI16Simd64, .{slice}));

    try report(io, "countNonZero scalar", measure(io, countNonZeroI16Scalar, .{slice}));
    try report(io, "countNonZero simd32", measure(io, countNonZeroI16Simd32, .{slice}));
    try report(io, "countNonZero simd64", measure(io, countNonZeroI16Simd64, .{slice}));
}

fn benchI32(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, name: []const u8) !void {
    const path = try std.fs.path.join(allocator, &.{ data_dir, name });
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(bytes);
    const elem_count = bytes.len / @sizeOf(i32);
    const values = try allocator.alignedAlloc(i32, .@"64", elem_count);
    defer allocator.free(values);
    @memcpy(std.mem.sliceAsBytes(values), bytes[0 .. elem_count * @sizeOf(i32)]);
    const slice: []const i32 = values;
    try printOut(io, "\n=== {s} ({d} elements, {d} MB) ===\n", .{ name, slice.len, bytes.len / (1024 * 1024) });

    try report(io, "minI32 scalar", measure(io, minI32Scalar, .{slice}));
    try report(io, "minI32 simd16", measure(io, minI32Simd16, .{slice}));
    try report(io, "minMaxI32 scalar", measure(io, minMaxI32Scalar, .{slice}));
    try report(io, "minMaxI32 simd16", measure(io, minMaxI32Simd16, .{slice}));
}

fn benchI64(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, name: []const u8) !void {
    const path = try std.fs.path.join(allocator, &.{ data_dir, name });
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(bytes);
    const elem_count = bytes.len / @sizeOf(i64);
    const values = try allocator.alignedAlloc(i64, .@"64", elem_count);
    defer allocator.free(values);
    @memcpy(std.mem.sliceAsBytes(values), bytes[0 .. elem_count * @sizeOf(i64)]);
    const slice: []const i64 = values;
    try printOut(io, "\n=== {s} ({d} elements, {d} MB) ===\n", .{ name, slice.len, bytes.len / (1024 * 1024) });

    const target: i64 = 435090932899640449;
    try report(io, "pointEq scalar", measure(io, countEqI64Scalar, .{ slice, target }));
    try report(io, "pointEq simd8", measure(io, countEqI64Simd8, .{ slice, target }));

    try report(io, "avgI64 scalar", measure(io, avgI64Scalar, .{slice}));
    try report(io, "avgI64 simd8", measure(io, avgI64Simd8, .{slice}));
}

// ---------- scalar implementations ----------

fn sumI16Scalar(values: []const i16) i64 {
    var total: i64 = 0;
    for (values) |v| total += v;
    return total;
}

fn countNonZeroI16Scalar(values: []const i16) u64 {
    var count: u64 = 0;
    for (values) |v| if (v != 0) {
        count += 1;
    };
    return count;
}

fn minI32Scalar(values: []const i32) i32 {
    var m: i32 = std.math.maxInt(i32);
    for (values) |v| if (v < m) {
        m = v;
    };
    return m;
}

fn minMaxI32Scalar(values: []const i32) [2]i32 {
    var lo: i32 = std.math.maxInt(i32);
    var hi: i32 = std.math.minInt(i32);
    for (values) |v| {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    return .{ lo, hi };
}

fn countEqI64Scalar(values: []const i64, target: i64) u64 {
    var c: u64 = 0;
    for (values) |v| if (v == target) {
        c += 1;
    };
    return c;
}

fn avgI64Scalar(values: []const i64) f64 {
    if (values.len == 0) return 0;
    var total: f64 = 0;
    for (values) |v| total += @floatFromInt(v);
    return total / @as(f64, @floatFromInt(values.len));
}

// ---------- SIMD implementations ----------

fn sumI16Simd32(values: []const i16) i64 {
    const lanes = 32;
    const V = @Vector(lanes, i16);
    var total: i64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        total += @reduce(.Add, @as(@Vector(lanes, i32), v));
    }
    while (i < values.len) : (i += 1) total += values[i];
    return total;
}

fn sumI16Simd64(values: []const i16) i64 {
    const lanes = 64;
    const V = @Vector(lanes, i16);
    var total: i64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        total += @reduce(.Add, @as(@Vector(lanes, i32), v));
    }
    while (i < values.len) : (i += 1) total += values[i];
    return total;
}

fn countNonZeroI16Simd32(values: []const i16) u64 {
    const lanes = 32;
    const V = @Vector(lanes, i16);
    var count: u64 = 0;
    var i: usize = 0;
    const zero: V = @splat(0);
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v != zero;
        count += @intCast(@popCount(@as(u32, @bitCast(mask))));
    }
    while (i < values.len) : (i += 1) if (values[i] != 0) {
        count += 1;
    };
    return count;
}

fn countNonZeroI16Simd64(values: []const i16) u64 {
    const lanes = 64;
    const V = @Vector(lanes, i16);
    var count: u64 = 0;
    var i: usize = 0;
    const zero: V = @splat(0);
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v != zero;
        count += @intCast(@popCount(@as(u64, @bitCast(mask))));
    }
    while (i < values.len) : (i += 1) if (values[i] != 0) {
        count += 1;
    };
    return count;
}

fn minI32Simd16(values: []const i32) i32 {
    const lanes = 16;
    const V = @Vector(lanes, i32);
    var acc: V = @splat(std.math.maxInt(i32));
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        acc = @min(acc, v);
    }
    var result = @reduce(.Min, acc);
    while (i < values.len) : (i += 1) result = @min(result, values[i]);
    return result;
}

fn minMaxI32Simd16(values: []const i32) [2]i32 {
    const lanes = 16;
    const V = @Vector(lanes, i32);
    var lo_acc: V = @splat(std.math.maxInt(i32));
    var hi_acc: V = @splat(std.math.minInt(i32));
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        lo_acc = @min(lo_acc, v);
        hi_acc = @max(hi_acc, v);
    }
    var lo = @reduce(.Min, lo_acc);
    var hi = @reduce(.Max, hi_acc);
    while (i < values.len) : (i += 1) {
        lo = @min(lo, values[i]);
        hi = @max(hi, values[i]);
    }
    return .{ lo, hi };
}

fn countEqI64Simd8(values: []const i64, target: i64) u64 {
    const lanes = 8;
    const V = @Vector(lanes, i64);
    const target_vec: V = @splat(target);
    var count: u64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v == target_vec;
        count += @intCast(@popCount(@as(u8, @bitCast(mask))));
    }
    while (i < values.len) : (i += 1) if (values[i] == target) {
        count += 1;
    };
    return count;
}

fn avgI64Simd8(values: []const i64) f64 {
    if (values.len == 0) return 0;
    const lanes = 8;
    const VI = @Vector(lanes, i64);
    const VF = @Vector(lanes, f64);
    var acc: VF = @splat(0);
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: VI = values[i..][0..lanes].*;
        acc += @as(VF, @floatFromInt(v));
    }
    var total = @reduce(.Add, acc);
    while (i < values.len) : (i += 1) total += @floatFromInt(values[i]);
    return total / @as(f64, @floatFromInt(values.len));
}

// ---------- harness ----------

const Sample = struct {
    median_ns: u64,
    min_ns: u64,
    result_hash: u64,
};

fn measure(io: std.Io, comptime func: anytype, args: anytype) Sample {
    var samples: [iterations]u64 = undefined;
    var hash: u64 = 0;
    var idx: usize = 0;
    while (idx < iterations) : (idx += 1) {
        const start = std.Io.Clock.awake.now(io);
        const out = @call(.never_inline, func, args);
        const stop = std.Io.Clock.awake.now(io);
        samples[idx] = @intCast(stop.nanoseconds - start.nanoseconds);
        hash ^= mixHash(out);
    }
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    return .{ .median_ns = samples[iterations / 2], .min_ns = samples[0], .result_hash = hash };
}

fn mixHash(value: anytype) u64 {
    const T = @TypeOf(value);
    return switch (@typeInfo(T)) {
        .int => @bitCast(@as(i64, @intCast(value))),
        .float => @bitCast(@as(f64, value)),
        .array => blk: {
            var h: u64 = 0;
            for (value) |item| h ^= mixHash(item);
            break :blk h;
        },
        else => @compileError("unsupported result type for benchmark hash"),
    };
}

fn report(io: std.Io, label: []const u8, sample: Sample) !void {
    try printOut(io, "  {s:<22} median {d:>8} us   min {d:>8} us   hash {x}\n", .{
        label,
        sample.median_ns / 1000,
        sample.min_ns / 1000,
        sample.result_hash,
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
