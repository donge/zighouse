// SIMD reduce primitives.
//
// Each function in this module passed the A.1 micro-benchmark gate (>=1.5x
// vs scalar) on the live data/store_hot/ columns. See src/bench_simd.zig
// and the run output captured in docs/query_families.md.
//
// Lane widths chosen by best-of measurement on Apple M-series:
//   - i16 reductions: 32 lanes (sum) and 64 lanes (countNonZero)
//   - i32 reductions: 16 lanes
//   - i64 reductions: 8 lanes
//
// Scalar tail handling is kept inline; callers do not need to align inputs.

const std = @import("std");

pub fn sumI16(values: []const i16) i64 {
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

pub fn countNonZeroI16(values: []const i16) u64 {
    const lanes = 64;
    const V = @Vector(lanes, i16);
    const zero: V = @splat(0);
    var count: u64 = 0;
    var i: usize = 0;
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

pub fn minI32(values: []const i32) i32 {
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

pub fn maxI32(values: []const i32) i32 {
    const lanes = 16;
    const V = @Vector(lanes, i32);
    var acc: V = @splat(std.math.minInt(i32));
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        acc = @max(acc, v);
    }
    var result = @reduce(.Max, acc);
    while (i < values.len) : (i += 1) result = @max(result, values[i]);
    return result;
}

/// Fused single-pass min and max over the same column. ~8x scalar; ~2x
/// vs running minI32 then maxI32 thanks to a single memory traversal.
pub fn minMaxI32(values: []const i32) struct { min: i32, max: i32 } {
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
    return .{ .min = lo, .max = hi };
}

pub fn avgI64(values: []const i64) f64 {
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

/// Counts elements equal to `target`. Used by point lookups where we only
/// need the cardinality (Q20 prints each match, but the SIMD prefilter
/// here lets the caller scan only matching lanes scalarly).
pub fn countEqI64(values: []const i64, target: i64) u64 {
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

test "sumI16 matches scalar" {
    var data: [200]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(7);
    for (&data) |*v| v.* = rng.random().intRangeLessThan(i16, -1000, 1000);
    var expected: i64 = 0;
    for (data) |v| expected += v;
    try std.testing.expectEqual(expected, sumI16(&data));
}

test "countNonZeroI16 matches scalar" {
    var data: [300]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(11);
    for (&data) |*v| v.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 100);
    var expected: u64 = 0;
    for (data) |v| if (v != 0) {
        expected += 1;
    };
    try std.testing.expectEqual(expected, countNonZeroI16(&data));
}

test "minMaxI32 matches scalar" {
    var data: [123]i32 = undefined;
    var rng = std.Random.DefaultPrng.init(31);
    for (&data) |*v| v.* = rng.random().intRangeLessThan(i32, -100000, 100000);
    var lo: i32 = std.math.maxInt(i32);
    var hi: i32 = std.math.minInt(i32);
    for (data) |v| {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    const got = minMaxI32(&data);
    try std.testing.expectEqual(lo, got.min);
    try std.testing.expectEqual(hi, got.max);
}

test "countEqI64 matches scalar" {
    var data: [97]i64 = undefined;
    for (&data, 0..) |*v, i| v.* = if (i % 5 == 0) 42 else -@as(i64, @intCast(i + 1));
    try std.testing.expectEqual(@as(u64, 20), countEqI64(&data, 42));
}
