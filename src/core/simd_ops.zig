/// SIMD reduction primitives for the core execution engine.
/// These are inlined into pipeline.zig via @import("simd_ops.zig").
/// Separate from src/simd.zig to avoid cross-module file ownership conflicts.

const std = @import("std");

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

