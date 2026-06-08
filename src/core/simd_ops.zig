/// SIMD reduction primitives for the core execution engine.
/// These are inlined into pipeline.zig via @import("simd_ops.zig").
/// Separate from src/simd.zig to avoid cross-module file ownership conflicts.

const std = @import("std");

/// SIMD sum of i16 slice, sign-extended and returned as i64 (wrapping).
/// Uses 16-wide vectors (256-bit with AVX2).
pub fn sumI16(values: []const i16) i64 {
    const LANES = 32;
    const V = @Vector(LANES, i16);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc +%= v;
    }
    var total: i32 = @reduce(.Add, acc);
    while (i < values.len) : (i += 1) total +%= values[i];
    return @as(i64, total);
}

/// SIMD sum of i64 slice using 8-wide vectors.
/// Returns i64 with wrapping arithmetic.
pub fn sumI64(values: []const i64) i64 {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc +%= v;
    }
    var total: i64 = @reduce(.Add, acc);
    while (i < values.len) : (i += 1) total +%= values[i];
    return total;
}

/// SIMD sum of u64 slice, returned as i64 (reinterpret, wrapping).
pub fn sumU64(values: []const u64) i64 {
    const LANES = 8;
    const V = @Vector(LANES, u64);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc +%= v;
    }
    var total: u64 = @reduce(.Add, acc);
    while (i < values.len) : (i += 1) total +%= values[i];
    return @bitCast(total);
}

/// SIMD sum of f64 slice using 4-wide vectors.
pub fn sumF64(values: []const f64) f64 {
    const LANES = 4;
    const V = @Vector(LANES, f64);
    var acc: V = @splat(0.0);
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc += v;
    }
    var total: f64 = @reduce(.Add, acc);
    while (i < values.len) : (i += 1) total += values[i];
    return total;
}

/// SIMD min of i64 slice using 8-wide vectors.
pub fn minI64(values: []const i64) i64 {
    if (values.len == 0) return std.math.maxInt(i64);
    const LANES = 8;
    const V = @Vector(LANES, i64);
    var acc: V = @splat(std.math.maxInt(i64));
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc = @min(acc, v);
    }
    var total: i64 = @reduce(.Min, acc);
    while (i < values.len) : (i += 1) total = @min(total, values[i]);
    return total;
}

/// SIMD max of i64 slice using 8-wide vectors.
pub fn maxI64(values: []const i64) i64 {
    if (values.len == 0) return std.math.minInt(i64);
    const LANES = 8;
    const V = @Vector(LANES, i64);
    var acc: V = @splat(std.math.minInt(i64));
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc = @max(acc, v);
    }
    var total: i64 = @reduce(.Max, acc);
    while (i < values.len) : (i += 1) total = @max(total, values[i]);
    return total;
}

/// SIMD min/max of f64 slice.
pub fn minF64(values: []const f64) f64 {
    if (values.len == 0) return std.math.inf(f64);
    const LANES = 4;
    const V = @Vector(LANES, f64);
    var acc: V = @splat(std.math.inf(f64));
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc = @min(acc, v);
    }
    var total: f64 = @reduce(.Min, acc);
    while (i < values.len) : (i += 1) total = @min(total, values[i]);
    return total;
}

pub fn maxF64(values: []const f64) f64 {
    if (values.len == 0) return -std.math.inf(f64);
    const LANES = 4;
    const V = @Vector(LANES, f64);
    var acc: V = @splat(-std.math.inf(f64));
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        acc = @max(acc, v);
    }
    var total: f64 = @reduce(.Max, acc);
    while (i < values.len) : (i += 1) total = @max(total, values[i]);
    return total;
}

/// SIMD count of i64 elements where value != cmp_val (8-wide).
pub fn countNeqI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v != splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] != cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count of i64 elements where value == cmp_val.
pub fn countEqI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v == splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] == cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count of i64 elements where value > cmp_val.
pub fn countGtI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v > splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] > cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count of i64 elements where value >= cmp_val.
pub fn countGteI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v >= splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] >= cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count of i64 elements where value < cmp_val.
pub fn countLtI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v < splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] < cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count of i64 elements where value <= cmp_val.
pub fn countLteI64(values: []const i64, cmp_val: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const splat_val: V = @splat(cmp_val);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = v <= splat_val;
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] <= cmp_val) acc += 1;
    }
    return acc;
}

/// SIMD count where (v == val1 OR v == val2).
pub fn countIn2I64(values: []const i64, val1: i64, val2: i64) usize {
    const LANES = 8;
    const V = @Vector(LANES, i64);
    const sv1: V = @splat(val1);
    const sv2: V = @splat(val2);
    var acc: usize = 0;
    var i: usize = 0;
    while (i + LANES <= values.len) : (i += LANES) {
        const v: V = values[i..][0..LANES].*;
        const mask = (v == sv1) | (v == sv2);
        acc += @popCount(@as(u8, @bitCast(mask)));
    }
    while (i < values.len) : (i += 1) {
        if (values[i] == val1 or values[i] == val2) acc += 1;
    }
    return acc;
}

/// Apply a single int comparison to a i64 slice, writing passing indices into
/// `out_indices`. Returns the number of passing rows.
/// Used for late-materialisation: build a pass-set before decoding other columns.
pub fn filterI64(
    values:      []const i64,
    op:          enum(u8) { eq, neq, lt, lte, gt, gte, in2 },
    val:         i64,
    val2:        i64,
    out_indices: []u32,   // must have capacity >= values.len
) usize {
    var n: usize = 0;
    switch (op) {
        .neq => for (values, 0..) |v, i| { if (v != val) { out_indices[n] = @intCast(i); n += 1; } },
        .eq  => for (values, 0..) |v, i| { if (v == val) { out_indices[n] = @intCast(i); n += 1; } },
        .gt  => for (values, 0..) |v, i| { if (v >  val) { out_indices[n] = @intCast(i); n += 1; } },
        .gte => for (values, 0..) |v, i| { if (v >= val) { out_indices[n] = @intCast(i); n += 1; } },
        .lt  => for (values, 0..) |v, i| { if (v <  val) { out_indices[n] = @intCast(i); n += 1; } },
        .lte => for (values, 0..) |v, i| { if (v <= val) { out_indices[n] = @intCast(i); n += 1; } },
        .in2 => for (values, 0..) |v, i| { if (v == val or v == val2) { out_indices[n] = @intCast(i); n += 1; } },
    }
    return n;
}

