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

pub fn sum(comptime T: type, values: []const T) i64 {
    const lanes = lanesForSum(T);
    const V = @Vector(lanes, T);
    const AccT = sumAccumulator(T);
    var total: i64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        total += @reduce(.Add, @as(@Vector(lanes, AccT), v));
    }
    while (i < values.len) : (i += 1) total += values[i];
    return total;
}

pub fn countNonZero(comptime T: type, values: []const T) u64 {
    const lanes = lanesForCount(T);
    const V = @Vector(lanes, T);
    const zero: V = @splat(0);
    var count: u64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v != zero;
        count += popCountMask(mask);
    }
    while (i < values.len) : (i += 1) if (values[i] != 0) {
        count += 1;
    };
    return count;
}

pub const SumCount = struct { sum: i64, count: u64 };

pub fn filteredSumNonZero(comptime T: type, predicate: []const i16, values: []const T) i64 {
    const lanes = lanesForSum(T);
    const VP = @Vector(lanes, i16);
    const VV = @Vector(lanes, T);
    const AccT = sumAccumulator(T);
    var total: i64 = 0;
    var i: usize = 0;
    const p_zero: VP = @splat(0);
    const v_zero: VV = @splat(0);
    while (i + lanes <= predicate.len) : (i += lanes) {
        const p: VP = predicate[i..][0..lanes].*;
        const v: VV = values[i..][0..lanes].*;
        const selected = @select(T, p != p_zero, v, v_zero);
        total += @reduce(.Add, @as(@Vector(lanes, AccT), selected));
    }
    while (i < predicate.len) : (i += 1) if (predicate[i] != 0) {
        total += values[i];
    };
    return total;
}

pub fn filteredSumCountNonZero(comptime T: type, predicate: []const i16, values: []const T) SumCount {
    const lanes = lanesForSum(T);
    const VP = @Vector(lanes, i16);
    const VV = @Vector(lanes, T);
    const AccT = sumAccumulator(T);
    var total: i64 = 0;
    var count: u64 = 0;
    var i: usize = 0;
    const p_zero: VP = @splat(0);
    const v_zero: VV = @splat(0);
    while (i + lanes <= predicate.len) : (i += lanes) {
        const p: VP = predicate[i..][0..lanes].*;
        const v: VV = values[i..][0..lanes].*;
        const mask = p != p_zero;
        const selected = @select(T, mask, v, v_zero);
        total += @reduce(.Add, @as(@Vector(lanes, AccT), selected));
        count += popCountMask(mask);
    }
    while (i < predicate.len) : (i += 1) if (predicate[i] != 0) {
        total += values[i];
        count += 1;
    };
    return .{ .sum = total, .count = count };
}

pub fn filteredAvgNonZero(comptime T: type, predicate: []const i16, values: []const T) f64 {
    return switch (T) {
        i16, i32 => blk: {
            const result = filteredSumCountNonZero(T, predicate, values);
            if (result.count == 0) return 0;
            break :blk @as(f64, @floatFromInt(result.sum)) / @as(f64, @floatFromInt(result.count));
        },
        i64 => blk: {
            const lanes = 8;
            const VP = @Vector(lanes, i16);
            const VI = @Vector(lanes, i64);
            const VF = @Vector(lanes, f64);
            var total: f64 = 0;
            var count: u64 = 0;
            var i: usize = 0;
            const p_zero: VP = @splat(0);
            const f_zero: VF = @splat(0);
            while (i + lanes <= predicate.len) : (i += lanes) {
                const p: VP = predicate[i..][0..lanes].*;
                const v: VI = values[i..][0..lanes].*;
                const mask = p != p_zero;
                total += @reduce(.Add, @select(f64, mask, @as(VF, @floatFromInt(v)), f_zero));
                count += popCountMask(mask);
            }
            while (i < predicate.len) : (i += 1) if (predicate[i] != 0) {
                total += @floatFromInt(values[i]);
                count += 1;
            };
            if (count == 0) return 0;
            break :blk total / @as(f64, @floatFromInt(count));
        },
        else => @compileError("unsupported filtered average type"),
    };
}

pub fn filteredMinMaxNonZero(comptime T: type, predicate: []const i16, values: []const T) MinMax(T) {
    const lanes = lanesFor(T);
    const VP = @Vector(lanes, i16);
    const VV = @Vector(lanes, T);
    var lo_acc: VV = @splat(std.math.maxInt(T));
    var hi_acc: VV = @splat(std.math.minInt(T));
    var i: usize = 0;
    const p_zero: VP = @splat(0);
    const lo_sentinel: VV = @splat(std.math.maxInt(T));
    const hi_sentinel: VV = @splat(std.math.minInt(T));
    while (i + lanes <= predicate.len) : (i += lanes) {
        const p: VP = predicate[i..][0..lanes].*;
        const v: VV = values[i..][0..lanes].*;
        const mask = p != p_zero;
        lo_acc = @min(lo_acc, @select(T, mask, v, lo_sentinel));
        hi_acc = @max(hi_acc, @select(T, mask, v, hi_sentinel));
    }
    var lo = @reduce(.Min, lo_acc);
    var hi = @reduce(.Max, hi_acc);
    while (i < predicate.len) : (i += 1) if (predicate[i] != 0) {
        lo = @min(lo, values[i]);
        hi = @max(hi, values[i]);
    };
    return .{ .min = lo, .max = hi };
}

fn lanesFor(comptime T: type) comptime_int {
    return switch (T) {
        i16 => 32,
        i32 => 16,
        i64 => 8,
        else => @compileError("unsupported SIMD type"),
    };
}

fn lanesForSum(comptime T: type) comptime_int {
    return switch (T) {
        i16 => 32,
        i32 => 16,
        else => @compileError("unsupported SIMD sum type"),
    };
}

fn lanesForCount(comptime T: type) comptime_int {
    return switch (T) {
        i16 => 64,
        i32 => 16,
        i64 => 8,
        else => @compileError("unsupported SIMD count type"),
    };
}

fn popCountMask(mask: anytype) u64 {
    return switch (@bitSizeOf(@TypeOf(mask))) {
        8 => @intCast(@popCount(@as(u8, @bitCast(mask)))),
        16 => @intCast(@popCount(@as(u16, @bitCast(mask)))),
        32 => @intCast(@popCount(@as(u32, @bitCast(mask)))),
        64 => @intCast(@popCount(@as(u64, @bitCast(mask)))),
        else => @compileError("unsupported mask width"),
    };
}

fn MinMax(comptime T: type) type {
    return struct { min: T, max: T };
}

fn sumAccumulator(comptime T: type) type {
    return switch (T) {
        i16 => i32,
        i32 => i64,
        else => @compileError("unsupported SIMD sum type"),
    };
}

/// Fused single-pass min and max over the same column. ~8x scalar; ~2x
/// vs running minI32 then maxI32 thanks to a single memory traversal.
pub fn minMax(comptime T: type, values: []const T) MinMax(T) {
    const lanes = lanesFor(T);
    const V = @Vector(lanes, T);
    var lo_acc: V = @splat(std.math.maxInt(T));
    var hi_acc: V = @splat(std.math.minInt(T));
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

pub fn avg(comptime T: type, values: []const T) f64 {
    if (values.len == 0) return 0;
    if (T != i64) @compileError("unsupported SIMD average type");
    const lanes = lanesFor(T);
    const VI = @Vector(lanes, T);
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
pub fn countEq(comptime T: type, values: []const T, target: T) u64 {
    const lanes = lanesFor(T);
    const V = @Vector(lanes, T);
    const target_vec: V = @splat(target);
    var count: u64 = 0;
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: V = values[i..][0..lanes].*;
        const mask = v == target_vec;
        count += popCountMask(mask);
    }
    while (i < values.len) : (i += 1) if (values[i] == target) {
        count += 1;
    };
    return count;
}

test "sum i16 matches scalar" {
    var data: [200]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(7);
    for (&data) |*v| v.* = rng.random().intRangeLessThan(i16, -1000, 1000);
    var expected: i64 = 0;
    for (data) |v| expected += v;
    try std.testing.expectEqual(expected, sum(i16, &data));
}

test "countNonZero i16 matches scalar" {
    var data: [300]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(11);
    for (&data) |*v| v.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 100);
    var expected: u64 = 0;
    for (data) |v| if (v != 0) {
        expected += 1;
    };
    try std.testing.expectEqual(expected, countNonZero(i16, &data));
}

test "filtered sum i16 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(41);
    var expected: i64 = 0;
    for (&pred, &data) |*p, *v| {
        p.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 8);
        v.* = rng.random().intRangeLessThan(i16, -1000, 1000);
        if (p.* != 0) expected += v.*;
    }
    try std.testing.expectEqual(expected, filteredSumNonZero(i16, &pred, &data));
    const got = filteredSumCountNonZero(i16, &pred, &data);
    try std.testing.expectEqual(expected, got.sum);
}

test "filtered sum i32 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i32 = undefined;
    var rng = std.Random.DefaultPrng.init(43);
    var expected: i64 = 0;
    for (&pred, &data) |*p, *v| {
        p.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 8);
        v.* = rng.random().intRangeLessThan(i32, -100000, 100000);
        if (p.* != 0) expected += v.*;
    }
    try std.testing.expectEqual(expected, filteredSumNonZero(i32, &pred, &data));
    const got = filteredSumCountNonZero(i32, &pred, &data);
    try std.testing.expectEqual(expected, got.sum);
}

test "filtered sum count returns matching counts" {
    var pred: [257]i16 = undefined;
    var i16_data: [257]i16 = undefined;
    var i32_data: [257]i32 = undefined;
    var rng = std.Random.DefaultPrng.init(45);
    var expected_count: u64 = 0;
    for (&pred, &i16_data, &i32_data) |*p, *v16, *v32| {
        p.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 8);
        v16.* = rng.random().intRangeLessThan(i16, -1000, 1000);
        v32.* = rng.random().intRangeLessThan(i32, -100000, 100000);
        if (p.* != 0) expected_count += 1;
    }
    try std.testing.expectEqual(expected_count, filteredSumCountNonZero(i16, &pred, &i16_data).count);
    try std.testing.expectEqual(expected_count, filteredSumCountNonZero(i32, &pred, &i32_data).count);
}

test "filtered avg i64 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i64 = undefined;
    var rng = std.Random.DefaultPrng.init(46);
    var expected_sum: f64 = 0;
    var count: u64 = 0;
    for (&pred, &data) |*p, *v| {
        p.* = if (rng.random().boolean()) 0 else rng.random().intRangeLessThan(i16, 1, 8);
        v.* = rng.random().intRangeLessThan(i64, -1000000000000, 1000000000000);
        if (p.* != 0) {
            expected_sum += @floatFromInt(v.*);
            count += 1;
        }
    }
    const expected = expected_sum / @as(f64, @floatFromInt(count));
    try std.testing.expectApproxEqRel(expected, filteredAvgNonZero(i64, &pred, &data), 1e-12);
}

test "filtered minmax i16 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(47);
    var lo: i16 = std.math.maxInt(i16);
    var hi: i16 = std.math.minInt(i16);
    for (&pred, &data, 0..) |*p, *v, i| {
        p.* = if (i == 0 or rng.random().boolean()) 1 else 0;
        v.* = rng.random().intRangeLessThan(i16, -1000, 1000);
        if (p.* != 0) {
            lo = @min(lo, v.*);
            hi = @max(hi, v.*);
        }
    }
    const got = filteredMinMaxNonZero(i16, &pred, &data);
    try std.testing.expectEqual(lo, got.min);
    try std.testing.expectEqual(hi, got.max);
}

test "filtered minmax i32 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i32 = undefined;
    var rng = std.Random.DefaultPrng.init(47);
    var lo: i32 = std.math.maxInt(i32);
    var hi: i32 = std.math.minInt(i32);
    for (&pred, &data, 0..) |*p, *v, i| {
        p.* = if (i == 0 or rng.random().boolean()) 1 else 0;
        v.* = rng.random().intRangeLessThan(i32, -100000, 100000);
        if (p.* != 0) {
            lo = @min(lo, v.*);
            hi = @max(hi, v.*);
        }
    }
    const got = filteredMinMaxNonZero(i32, &pred, &data);
    try std.testing.expectEqual(lo, got.min);
    try std.testing.expectEqual(hi, got.max);
}

test "filtered minmax i64 matches scalar" {
    var pred: [257]i16 = undefined;
    var data: [257]i64 = undefined;
    var rng = std.Random.DefaultPrng.init(53);
    var lo: i64 = std.math.maxInt(i64);
    var hi: i64 = std.math.minInt(i64);
    for (&pred, &data, 0..) |*p, *v, i| {
        p.* = if (i == 0 or rng.random().boolean()) 1 else 0;
        v.* = rng.random().intRangeLessThan(i64, -1000000000000, 1000000000000);
        if (p.* != 0) {
            lo = @min(lo, v.*);
            hi = @max(hi, v.*);
        }
    }
    const got = filteredMinMaxNonZero(i64, &pred, &data);
    try std.testing.expectEqual(lo, got.min);
    try std.testing.expectEqual(hi, got.max);
}

fn expectMinMax(comptime T: type, data: []const T, got: MinMax(T)) !void {
    var lo: T = std.math.maxInt(T);
    var hi: T = std.math.minInt(T);
    for (data) |v| {
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    try std.testing.expectEqual(lo, got.min);
    try std.testing.expectEqual(hi, got.max);
}

test "minMax wrappers match scalar" {
    var i16_data: [123]i16 = undefined;
    var i32_data: [123]i32 = undefined;
    var i64_data: [123]i64 = undefined;
    var rng = std.Random.DefaultPrng.init(31);
    for (&i16_data) |*v| v.* = rng.random().intRangeLessThan(i16, -1000, 1000);
    for (&i32_data) |*v| v.* = rng.random().intRangeLessThan(i32, -100000, 100000);
    for (&i64_data) |*v| v.* = rng.random().intRangeLessThan(i64, -1000000000000, 1000000000000);
    try expectMinMax(i16, &i16_data, minMax(i16, &i16_data));
    try expectMinMax(i32, &i32_data, minMax(i32, &i32_data));
    try expectMinMax(i64, &i64_data, minMax(i64, &i64_data));
}

test "countEq i64 matches scalar" {
    var data: [97]i64 = undefined;
    for (&data, 0..) |*v, i| v.* = if (i % 5 == 0) 42 else -@as(i64, @intCast(i + 1));
    try std.testing.expectEqual(@as(u64, 20), countEq(i64, &data, 42));
}
