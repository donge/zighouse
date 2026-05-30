/// Vectorized predicate evaluation helpers for the core execution engine.
///
/// These routines mirror the SIMD primitives in src/simd.zig but live inside
/// the `core` module so that kernels.zig can import them without crossing
/// module boundaries (simd.zig is in the root module; core is its own module).
///
/// Supported element types for cmpBatch: i16, u16, i32, u32, i64, u64.
/// Mask format: []i16, value 1 = match, 0 = no match (compatible with the
/// predicate slices used by filteredSumNonZero / filteredMinMaxNonZero in
/// simd.zig).
const std = @import("std");

// ── Lane widths ───────────────────────────────────────────────────────────────

fn lanesFor(comptime T: type) comptime_int {
    return switch (T) {
        i16, u16 => 32,
        i32, u32 => 16,
        i64, u64 => 8,
        else => @compileError("unsupported SIMD batch type"),
    };
}

// ── Comparison kind ───────────────────────────────────────────────────────────

pub const CmpKind = enum { eq, neq, lt, lte, gt, gte };

pub fn flipCmp(op: CmpKind) CmpKind {
    return switch (op) {
        .eq  => .eq,
        .neq => .neq,
        .lt  => .gt,
        .lte => .gte,
        .gt  => .lt,
        .gte => .lte,
    };
}

// ── Vectorized comparison ─────────────────────────────────────────────────────

/// Write 1/0 into `out[i]` for each element in `values` using SIMD.
/// `out.len` must equal `values.len`.
pub fn cmpBatch(comptime T: type, values: []const T, comptime op: CmpKind, rhs: T, out: []i16) void {
    std.debug.assert(out.len == values.len);
    const lanes = lanesFor(T);
    const VT = @Vector(lanes, T);
    const VI = @Vector(lanes, i16);
    const rhs_vec: VT = @splat(rhs);
    const one_vec: VI = @splat(1);
    const zero_vec: VI = @splat(0);
    var i: usize = 0;
    while (i + lanes <= values.len) : (i += lanes) {
        const v: VT = values[i..][0..lanes].*;
        const mask = switch (op) {
            .eq  => v == rhs_vec,
            .neq => v != rhs_vec,
            .lt  => v < rhs_vec,
            .lte => v <= rhs_vec,
            .gt  => v > rhs_vec,
            .gte => v >= rhs_vec,
        };
        out[i..][0..lanes].* = @select(i16, mask, one_vec, zero_vec);
    }
    while (i < values.len) : (i += 1) {
        out[i] = switch (op) {
            .eq  => if (values[i] == rhs) @as(i16, 1) else 0,
            .neq => if (values[i] != rhs) @as(i16, 1) else 0,
            .lt  => if (values[i] < rhs)  @as(i16, 1) else 0,
            .lte => if (values[i] <= rhs) @as(i16, 1) else 0,
            .gt  => if (values[i] > rhs)  @as(i16, 1) else 0,
            .gte => if (values[i] >= rhs) @as(i16, 1) else 0,
        };
    }
}

// ── Mask combinators ──────────────────────────────────────────────────────────

/// Element-wise AND of two i16 predicate masks into `out`.
pub fn andMasks(a: []const i16, b: []const i16, out: []i16) void {
    std.debug.assert(a.len == b.len and out.len == a.len);
    const LANES = 32;
    const V = @Vector(LANES, i16);
    const zero: V = @splat(0);
    var i: usize = 0;
    while (i + LANES <= a.len) : (i += LANES) {
        const va: V = a[i..][0..LANES].*;
        const vb: V = b[i..][0..LANES].*;
        const ma = va != zero;
        const mb = vb != zero;
        // element-wise AND: both must be non-zero
        const both = @select(i16, ma, @select(i16, mb, @as(V, @splat(1)), zero), zero);
        out[i..][0..LANES].* = both;
    }
    while (i < a.len) : (i += 1) {
        out[i] = if (a[i] != 0 and b[i] != 0) 1 else 0;
    }
}

/// Element-wise OR of two i16 predicate masks into `out`.
pub fn orMasks(a: []const i16, b: []const i16, out: []i16) void {
    std.debug.assert(a.len == b.len and out.len == a.len);
    const LANES = 32;
    const V = @Vector(LANES, i16);
    const zero: V = @splat(0);
    const one: V = @splat(1);
    var i: usize = 0;
    while (i + LANES <= a.len) : (i += LANES) {
        const va: V = a[i..][0..LANES].*;
        const vb: V = b[i..][0..LANES].*;
        const ma = va != zero;
        const mb = vb != zero;
        const either = @select(i16, ma, one, @select(i16, mb, one, zero));
        out[i..][0..LANES].* = either;
    }
    while (i < a.len) : (i += 1) {
        out[i] = if (a[i] != 0 or b[i] != 0) 1 else 0;
    }
}

/// Negate a predicate mask in-place: 0 → 1, non-zero → 0.
pub fn notMask(mask: []i16) void {
    const LANES = 32;
    const V = @Vector(LANES, i16);
    const zero: V = @splat(0);
    const one: V = @splat(1);
    var i: usize = 0;
    while (i + LANES <= mask.len) : (i += LANES) {
        const v: V = mask[i..][0..LANES].*;
        mask[i..][0..LANES].* = @select(i16, v == zero, one, zero);
    }
    while (i < mask.len) : (i += 1) {
        mask[i] = if (mask[i] == 0) 1 else 0;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "cmpBatch i64 eq matches scalar" {
    var data: [257]i64 = undefined;
    var out: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(71);
    for (&data) |*v| v.* = rng.random().intRangeLessThan(i64, -10, 10);
    cmpBatch(i64, &data, .eq, 3, &out);
    for (data, out) |v, o| {
        try std.testing.expectEqual(if (v == 3) @as(i16, 1) else 0, o);
    }
}

test "cmpBatch i64 lt matches scalar" {
    var data: [257]i64 = undefined;
    var out: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(72);
    for (&data) |*v| v.* = rng.random().intRangeLessThan(i64, -100, 100);
    cmpBatch(i64, &data, .lt, 0, &out);
    for (data, out) |v, o| {
        try std.testing.expectEqual(if (v < 0) @as(i16, 1) else 0, o);
    }
}

test "cmpBatch u64 gte matches scalar" {
    var data: [257]u64 = undefined;
    var out: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(73);
    for (&data) |*v| v.* = rng.random().uintLessThan(u64, 200);
    cmpBatch(u64, &data, .gte, 100, &out);
    for (data, out) |v, o| {
        try std.testing.expectEqual(if (v >= 100) @as(i16, 1) else 0, o);
    }
}

test "andMasks / orMasks / notMask match scalar" {
    var a: [257]i16 = undefined;
    var b: [257]i16 = undefined;
    var out: [257]i16 = undefined;
    var rng = std.Random.DefaultPrng.init(74);
    for (&a) |*v| v.* = if (rng.random().boolean()) 1 else 0;
    for (&b) |*v| v.* = if (rng.random().boolean()) 1 else 0;

    andMasks(&a, &b, &out);
    for (a, b, out) |ai, bi, oi| try std.testing.expectEqual(
        if (ai != 0 and bi != 0) @as(i16, 1) else 0, oi,
    );

    orMasks(&a, &b, &out);
    for (a, b, out) |ai, bi, oi| try std.testing.expectEqual(
        if (ai != 0 or bi != 0) @as(i16, 1) else 0, oi,
    );

    var not_in = a;
    notMask(&not_in);
    for (a, not_in) |ai, ni| try std.testing.expectEqual(
        if (ai == 0) @as(i16, 1) else 0, ni,
    );
}
