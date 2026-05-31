// A.5 micro-benchmark: scalar evalExpr vs IntCmpCond scalar vs evalExprBatch SIMD.
//
// Measures wall-clock time for filtering a synthetic 1M-row i64 column
// using three execution strategies:
//
//   1. scalar_evalexpr    — row-by-row evalExpr via chunk.readRow (baseline)
//   2. intcond_scalar     — IntCmpCond tight scalar loop (no row boxing)
//   3. evalexpr_batch     — evalExprBatch SIMD mask + count
//   4. raw_cmpbatch       — raw cmpBatch (no evalExprBatch overhead)
//
// Run with: zig build bench-filter
//
// Expected: simd paths >= 4x faster than scalar_evalexpr.

const std = @import("std");
const chunk   = @import("core/chunk.zig");
const kernels = @import("core/exec/kernels.zig");
const plan    = @import("core/exec/plan.zig");
const simd    = @import("core/simd_batch.zig");

const ROWS:       usize = 1_024_000;
const CHUNK_ROWS: usize = 2048;
const ITERATIONS: usize = 7;

// ---------- harness ---------------------------------------------------------

const Sample = struct { median_ns: u64, min_ns: u64, result: u64 };

var g_io: std.Io = undefined;

fn measure(comptime func: anytype, args: anytype) Sample {
    var samples: [ITERATIONS]u64 = undefined;
    var result: u64 = 0;
    for (&samples) |*s| {
        const t0 = std.Io.Clock.Timestamp.now(g_io, .awake);
        result ^= @call(.never_inline, func, args);
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
    try printOut(io, "  {s:<28}  median {d:>7} us   min {d:>7} us   hash {x:016}\n", .{
        label, s.median_ns / 1000, s.min_ns / 1000, s.result,
    });
}

// ---------- data + chunks ---------------------------------------------------

fn makeChunks(
    allocator: std.mem.Allocator,
    data: []const i64,
) ![]chunk.DataChunk {
    const n = data.len / CHUNK_ROWS;
    const chunks = try allocator.alloc(chunk.DataChunk, n);
    for (chunks, 0..) |*dc, ci| {
        var b = chunk.ChunkBuilder.init(allocator, CHUNK_ROWS);
        const idx = try b.addColumn("v", .int64);
        const base = ci * CHUNK_ROWS;
        @memcpy(b.chunk.columns[idx].data.int64, data[base..][0..CHUNK_ROWS]);
        dc.* = b.finish();
    }
    return chunks;
}

// ---------- path 1: scalar evalExpr (row boxing baseline) -------------------

fn benchScalarEvalExpr(dc_slice: []const chunk.DataChunk, pred: plan.Expr, alloc: std.mem.Allocator) u64 {
    var kept: u64 = 0;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    for (dc_slice) |dc| {
        for (0..dc.num_rows) |r| {
            _ = arena.reset(.retain_capacity);
            const row = dc.readRow(r, arena.allocator()) catch continue;
            const v = kernels.evalExpr(pred, row, null, arena.allocator()) catch continue;
            if (v) |val| switch (val) {
                .bool_u8 => |b| { if (b != 0) kept += 1; },
                .int64   => |x| { if (x != 0) kept += 1; },
                else     => kept += 1,
            };
        }
    }
    return kept;
}

// ---------- path 2: IntCmpCond tight scalar loop ----------------------------

fn benchIntCondScalar(dc_slice: []const chunk.DataChunk, threshold: i64) u64 {
    var kept: u64 = 0;
    for (dc_slice) |dc| {
        const vals = dc.columns[0].data.int64;
        for (0..dc.num_rows) |r| {
            if (vals[r] > threshold) kept += 1;
        }
    }
    return kept;
}

// ---------- path 3: evalExprBatch SIMD mask + count -------------------------

fn benchEvalExprBatch(
    dc_slice: []const chunk.DataChunk,
    pred:     plan.Expr,
    mask_buf: []i16,
    alloc:    std.mem.Allocator,
) u64 {
    var kept: u64 = 0;
    for (dc_slice) |dc| {
        const out = mask_buf[0..dc.num_rows];
        kernels.evalExprBatch(pred, dc, out, alloc) catch continue;
        for (out) |m| if (m != 0) { kept += 1; };
    }
    return kept;
}

// ---------- path 4: raw cmpBatch (no evalExpr overhead) ---------------------

fn benchRawCmpBatch(dc_slice: []const chunk.DataChunk, threshold: i64, mask_buf: []i16) u64 {
    var kept: u64 = 0;
    for (dc_slice) |dc| {
        const out = mask_buf[0..dc.num_rows];
        simd.cmpBatch(i64, dc.columns[0].data.int64, .gt, threshold, out);
        for (out) |m| if (m != 0) { kept += 1; };
    }
    return kept;
}

// ---------- main ------------------------------------------------------------

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    g_io = io;

    const data = try allocator.alloc(i64, ROWS);
    defer allocator.free(data);
    var rng = std.Random.DefaultPrng.init(0xdeadbeef);
    for (data) |*v| v.* = rng.random().intRangeLessThan(i64, 0, 1000);
    const threshold: i64 = 500;

    const chunks = try makeChunks(allocator, data);
    defer {
        for (chunks) |*dc| dc.deinit();
        allocator.free(chunks);
    }

    const mask_buf = try allocator.alloc(i16, CHUNK_ROWS);
    defer allocator.free(mask_buf);

    // pred: col[0] > 500
    var binop = plan.BinOp{
        .left  = .{ .col_ref = .{ .index = 0, .name = "v" } },
        .right = .{ .lit_i64 = threshold },
    };
    const pred = plan.Expr{ .gt = &binop };

    try printOut(io, "\n=== bench_filter: {d}M rows, i64 > {d}, {d} iterations ===\n",
        .{ ROWS / 1_000_000, threshold, ITERATIONS });
    try printOut(io, "  (lower time is better; hash must match across all paths)\n\n", .{});

    const s1 = measure(benchScalarEvalExpr,  .{ chunks, pred, allocator });
    try report(io, "scalar evalExpr", s1);

    const s2 = measure(benchIntCondScalar,   .{ chunks, threshold });
    try report(io, "IntCmpCond scalar loop", s2);

    const s3 = measure(benchEvalExprBatch,   .{ chunks, pred, mask_buf, allocator });
    try report(io, "evalExprBatch (SIMD)", s3);

    const s4 = measure(benchRawCmpBatch,     .{ chunks, threshold, mask_buf });
    try report(io, "raw cmpBatch (SIMD)", s4);

    const f = @as(f64, @floatFromInt(s1.median_ns));
    try printOut(io, "\n  speedup IntCmpCond vs scalar:      {d:.1}x\n",
        .{ f / @as(f64, @floatFromInt(s2.median_ns)) });
    try printOut(io, "  speedup evalExprBatch vs scalar:   {d:.1}x\n",
        .{ f / @as(f64, @floatFromInt(s3.median_ns)) });
    try printOut(io, "  speedup raw cmpBatch vs scalar:    {d:.1}x\n\n",
        .{ f / @as(f64, @floatFromInt(s4.median_ns)) });

    if (s1.result != s2.result or s1.result != s3.result or s1.result != s4.result) {
        try printOut(io, "  *** CORRECTNESS FAILURE: {x} {x} {x} {x}\n",
            .{ s1.result, s2.result, s3.result, s4.result });
        std.process.exit(1);
    }
    try printOut(io, "  All results match. OK\n\n", .{});
}
