//! Scalar aggregation and raw aggregate fast paths.
const std = @import("std");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const plan = @import("plan.zig");
const kernels = @import("kernels.zig");
const ht = @import("hash_table.zig");
const parallel = @import("parallel");
const simd_ops = @import("../simd_ops.zig");
const simd_batch = @import("../simd_batch.zig");
const context = @import("context.zig");
const operators = @import("operators.zig");
const row_buffer = @import("row_buffer.zig");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const DataChunk = chunk.DataChunk;
pub const QueryContext = context.QueryContext;
pub const SourceIface = context.SourceIface;
pub const FilterState = operators.FilterState;
pub const ScanRequest = operators.ScanRequest;
pub const RawColSlice = operators.RawColSlice;
const IntCmpCond = operators.IntCmpCond;
const LimitState = operators.LimitState;
const collectColRefs = operators.collectColRefs;
const extractAndIntConds = operators.extractAndIntConds;
const cmpBatchDispatch = operators.cmpBatchDispatch;
pub const RowBuffer = row_buffer.RowBuffer;
const simd = simd_ops;

pub fn initAggAccums(aggs: []const plan.ProjectItem, alloc: std.mem.Allocator) ![]AggAccum {
    const accums = try alloc.alloc(AggAccum, aggs.len);
    for (aggs, 0..) |item, i| accums[i] = initAccumForAgg(item);
    return accums;
}

pub fn executeScalarAggChunked(
    input: *const plan.PhysicalNode,
    aggs: []const plan.ProjectItem,
    ctx: *QueryContext,
) !RowBuffer {
    const alloc = ctx.allocator();
    const accums = try initAggAccums(aggs, alloc);

    var filter_state: ?FilterState = extractFilter(input);
    var lim_state: ?LimitState = extractLimit(input);

    // Count-only mode: if all aggs are COUNT(*), skip copyRow in pure-LIKE filter.
    // Downstream only reads c.num_rows for count accumulation.
    if (filter_state != null) {
        const all_count_star = for (aggs) |item| {
            const ok = item.expr == .agg_call and item.expr.agg_call.kind == .count_star;
            if (!ok) break false;
        } else true;
        if (all_count_star) filter_state.?.count_only_mode = true;
    }

    var c: DataChunk = undefined;
    ctx.source.reset();
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (lim_state) |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            c.deinit();
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        try updateAccumsFromChunk(accums, aggs, &c, alloc, ctx);
        // Rescue str_min/str_max slices that point into the chunk's arena
        // before freeing the chunk.  All other accumulators hold numeric values.
        for (accums) |*acc| switch (acc.*) {
            .str_min => |v| if (v) |s| {
                acc.str_min = try alloc.dupe(u8, s);
            },
            .str_max => |v| if (v) |s| {
                acc.str_max = try alloc.dupe(u8, s);
            },
            else => {},
        };
        c.deinit();
        if (lim_state) |ls| if (ls.done()) break;
    }

    return finalizeScalarAgg(accums, aggs, alloc);
}

pub fn collectRawEqIndices(raw: RawColSlice, target: i64, alloc: std.mem.Allocator) ![]u32 {
    var out: std.ArrayListUnmanaged(u32) = .empty;
    errdefer out.deinit(alloc);
    switch (raw) {
        .i16s => |a| {
            const t: i16 = std.math.cast(i16, target) orelse return try out.toOwnedSlice(alloc);
            const L = 16;
            const V = @Vector(L, i16);
            var i: usize = 0;
            while (i + L <= a.len) : (i += L) {
                const v: V = a[i..][0..L].*;
                const eq = v == @as(V, @splat(t));
                if (@reduce(.Or, eq)) {
                    inline for (0..L) |k| if (eq[k]) try out.append(alloc, @intCast(i + k));
                }
            }
            while (i < a.len) : (i += 1) if (a[i] == t) try out.append(alloc, @intCast(i));
        },
        .i32s => |a| {
            const t: i32 = std.math.cast(i32, target) orelse return try out.toOwnedSlice(alloc);
            const L = 8;
            const V = @Vector(L, i32);
            var i: usize = 0;
            while (i + L <= a.len) : (i += L) {
                const v: V = a[i..][0..L].*;
                const eq = v == @as(V, @splat(t));
                if (@reduce(.Or, eq)) {
                    inline for (0..L) |k| if (eq[k]) try out.append(alloc, @intCast(i + k));
                }
            }
            while (i < a.len) : (i += 1) if (a[i] == t) try out.append(alloc, @intCast(i));
        },
        .i64s => |a| {
            const L = 4;
            const V = @Vector(L, i64);
            var i: usize = 0;
            while (i + L <= a.len) : (i += L) {
                const v: V = a[i..][0..L].*;
                const eq = v == @as(V, @splat(target));
                if (@reduce(.Or, eq)) {
                    inline for (0..L) |k| if (eq[k]) try out.append(alloc, @intCast(i + k));
                }
            }
            while (i < a.len) : (i += 1) if (a[i] == target) try out.append(alloc, @intCast(i));
        },
    }
    return try out.toOwnedSlice(alloc);
}

pub fn executeRawIntPointLookup(input: *const plan.PhysicalNode, items: []const plan.ProjectItem, ctx: *QueryContext) !?RowBuffer {
    if (input.* != .filter) return null;
    const pred = input.filter.predicate;
    if (pred != .eq) return null;
    const op = pred.eq;
    if (op.left != .col_ref or op.right != .lit_i64) return null;
    const filter_ci = op.left.col_ref.index;
    const target = op.right.lit_i64;
    for (items) |item| {
        if (item.expr != .col_ref) return null;
        switch (item.out_type) {
            .int64, .uint64, .date_u16, .datetime64_ms => {},
            else => return null,
        }
    }
    const inner = input.filter.input;
    if (inner.* != .part_scan and inner.* != .mem_scan) return null;

    const alloc = ctx.allocator();
    const sm = ctx.source.schema();
    var scan_request: ScanRequest = .{};
    scan_request.addIndex(filter_ci);
    scan_request.addItems(items);
    var scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }
    const metas = try alloc.alloc(result.ColMeta, items.len);
    for (items, 0..) |item, i| {
        metas[i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }

    // Block stats can prove the value is outside the column range.
    if (ctx.source.getIntMinMax(sm[filter_ci].name)) |stats| {
        if (target < stats[0] or target > stats[1]) return RowBuffer.init(metas);
    }

    const raw_filter = RawColSlice.resolve(ctx.source, sm, filter_ci) orelse return null;
    const matches = try collectRawEqIndices(raw_filter, target, alloc);
    defer alloc.free(matches);

    var rl = RowBuffer.init(metas);
    for (matches) |row_idx| {
        const row = try alloc.alloc(?Value, items.len);
        for (items, 0..) |item, i| {
            const ci = item.expr.col_ref.index;
            const raw = RawColSlice.resolve(ctx.source, sm, ci) orelse {
                row[i] = null;
                continue;
            };
            const v = raw.getI64(row_idx);
            row[i] = switch (item.out_type) {
                .int64 => .{ .int64 = v },
                .uint64 => .{ .uint64 = @intCast(v) },
                .date_u16 => .{ .date_u16 = @intCast(v) },
                .datetime64_ms => .{ .datetime64_ms = v },
                else => unreachable,
            };
        }
        try rl.append(alloc, row);
    }
    return rl;
}

pub fn executeRawMinMaxAgg(input: *const plan.PhysicalNode, aggs: []const plan.ProjectItem, ctx: *QueryContext) !?RowBuffer {
    if (input.* != .part_scan and input.* != .mem_scan) return null;
    if (aggs.len == 0) return null;
    for (aggs) |item| {
        if (item.expr != .agg_call) return null;
        const ac = item.expr.agg_call;
        if (ac.kind != .min and ac.kind != .max) return null;
        if (ac.distinct or ac.arg == null or ac.arg.? != .col_ref) return null;
        switch (item.out_type) {
            .int64, .uint64, .date_u16, .datetime64_ms => {},
            else => return null,
        }
    }
    const alloc = ctx.allocator();
    const sm = ctx.source.schema();
    const metas = try buildAggResultMetas(&.{}, aggs, alloc);
    var rl = RowBuffer.init(metas);
    const row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, i| {
        const ac = item.expr.agg_call;
        const ci = ac.arg.?.col_ref.index;
        if (ci >= sm.len) return null;
        const v = if (ctx.source.getIntMinMax(sm[ci].name)) |stats|
            (if (ac.kind == .min) stats[0] else stats[1])
        else blk: {
            const raw = RawColSlice.resolve(ctx.source, sm, ci) orelse return null;
            const mm = raw.minMax();
            break :blk if (ac.kind == .min) mm.min else mm.max;
        };
        row[i] = switch (item.out_type) {
            .int64 => .{ .int64 = v },
            .uint64 => .{ .uint64 = @intCast(v) },
            .date_u16 => .{ .date_u16 = @intCast(v) },
            .datetime64_ms => .{ .datetime64_ms = v },
            else => unreachable,
        };
    }
    try rl.append(alloc, row);
    return rl;
}

pub fn executeScalarAggParallel(
    input: *const plan.PhysicalNode,
    aggs: []const plan.ProjectItem,
    ctx: *QueryContext,
) !?RowBuffer {
    if (try executeRawMinMaxAgg(input, aggs, ctx)) |rl| return rl;
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows == 0) return null;
    // Only run parallel for large datasets (10M+ rows benefit most).
    const MIN_ROWS_FOR_PARALLEL: u64 = 500_000;
    if (total_rows < MIN_ROWS_FOR_PARALLEL) return null;

    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter => |f| f.predicate,
        .project => |p| switch (p.input.*) {
            .filter => |f| f.predicate,
            else => null,
        },
        else => null,
    };
    // For now, only parallelize queries without LIMIT (LIMIT complicates merge).
    const has_limit: bool = switch (input.*) {
        .limit => true,
        .filter => |f| switch (f.input.*) {
            .limit => true,
            else => false,
        },
        else => false,
    };
    if (has_limit) return null;

    const alloc = ctx.allocator();
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;

    // Allocate per-thread accumulators (shared by both fast and normal paths).
    const thread_accums = try alloc.alloc([]AggAccum, n_threads);
    const rows_per_thread: u32 = @intCast(@min(total_rows / n_threads + 1, 2_000_000));
    for (thread_accums) |*ta| {
        ta.* = try initAggAccums(aggs, alloc);
        for (ta.*) |*accum| {
            // Pre-allocate distinct_u64 hashmap to avoid repeated resizes.
            if (accum.* == .distinct_u64)
                try accum.distinct_u64.ensureTotalCapacity(std.heap.c_allocator, rows_per_thread);
        }
    }

    // ── Raw-byte LIKE count fast path ─────────────────────────────────────────
    // For COUNT(*) with a pure single LIKE/NOT_LIKE predicate, skip fetchRange
    // entirely and scan raw string offsets+bytes directly.
    // This avoids ~78ms of fat-pointer allocation overhead per query (e.g. Q21).
    const all_count_star_par: bool = for (aggs) |item| {
        if (item.expr != .agg_call or item.expr.agg_call.kind != .count_star) break false;
    } else true;

    if (all_count_star_par) {
        if (filter_pred) |pred| {
            // Detect pure single LIKE / NOT_LIKE on a col_ref vs lit_str.
            const is_raw_like = switch (pred) {
                .like, .not_like => |op| op.left == .col_ref and op.right == .lit_str,
                else => false,
            };
            if (is_raw_like) {
                const op = switch (pred) {
                    .like => |o| o,
                    .not_like => |o| o,
                    else => unreachable,
                };
                const col_idx = op.left.col_ref.index;
                const src_schema = ctx.source.schema();
                if (col_idx < src_schema.len) {
                    const col_name = src_schema[col_idx].name;
                    if (ctx.source.getRawStrOffsets(col_name)) |raw_offsets| {
                        if (ctx.source.getRawStrBytes(col_name)) |raw_bytes| {
                            // We have raw data — run specialized count loop without fetchRange.
                            const matcher = kernels.LikeMatcher.compile(op.right.lit_str, if (op.escape) |e| if (e.len > 0) e[0] else null else null);
                            const negate = pred == .not_like;

                            const RawParCtx = struct {
                                raw_offsets: []const u64,
                                raw_bytes: []const u8,
                                matcher: kernels.LikeMatcher,
                                negate: bool,
                                accums: []AggAccum,
                                morsel_src: *parallel.MorselSource,

                                fn work(self: *@This(), _: *parallel.MorselSource) void {
                                    while (self.morsel_src.next()) |m| {
                                        var count: u64 = 0;
                                        for (m.start..m.end) |r| {
                                            const lo: usize = @intCast(self.raw_offsets[r]);
                                            const hi: usize = @intCast(self.raw_offsets[r + 1]);
                                            const s = self.raw_bytes[lo..hi];
                                            if (self.matcher.match(s) != self.negate) count += 1;
                                        }
                                        self.accums[0].count += count;
                                    }
                                }
                            };

                            var raw_morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
                            const raw_pctxs = try alloc.alloc(RawParCtx, n_threads);
                            for (raw_pctxs, 0..) |*pc, ti| {
                                pc.* = .{
                                    .raw_offsets = raw_offsets,
                                    .raw_bytes = raw_bytes,
                                    .matcher = matcher,
                                    .negate = negate,
                                    .accums = thread_accums[ti],
                                    .morsel_src = &raw_morsel_src,
                                };
                            }

                            try parallel.parallelFor(alloc, RawParCtx, RawParCtx.work, raw_pctxs, &raw_morsel_src);

                            // Merge accumulators.
                            const raw_merged = thread_accums[0];
                            for (thread_accums[1..]) |ta| {
                                for (raw_merged, ta, 0..) |*m, t, ci| {
                                    try mergeAccum(m, t, aggs[ci], alloc);
                                }
                            }

                            const metas = try alloc.alloc(result.ColMeta, aggs.len);
                            const out_row = try alloc.alloc(?Value, aggs.len);
                            for (aggs, 0..) |item, ci| {
                                metas[ci] = .{ .name = item.alias, .col_type = item.out_type };
                                out_row[ci] = try finalizeAccum(raw_merged[ci], item, alloc);
                            }
                            var raw_rl = RowBuffer.init(metas);
                            try raw_rl.append(alloc, out_row);
                            ctx.profile.raw_path_hits += 1;
                            ctx.profile.recordReason("raw_like_count");
                            return raw_rl;
                        }
                    }
                }
            }
            // ── Raw int16 SIMD COUNT fast path (e.g. Q2: COUNT(*) WHERE AdvEngineID <> 0) ──
            // Bypasses fetchRange entirely: reads mmap'd i16 → cmpBatch(i16, 32-lane) → count.
            raw_int16_cnt_blk: {
                var i16cnt_ic_buf: [4]IntCmpCond = undefined;
                var i16cnt_ic_n: usize = 0;
                if (!extractAndIntConds(pred, &i16cnt_ic_buf, &i16cnt_ic_n, false)) break :raw_int16_cnt_blk;
                if (i16cnt_ic_n != 1) break :raw_int16_cnt_blk;
                const i16cnt_cond = i16cnt_ic_buf[0];
                const i16cnt_sm = ctx.source.schema();
                if (i16cnt_cond.col_idx >= i16cnt_sm.len) break :raw_int16_cnt_blk;
                const i16cnt_col = i16cnt_sm[i16cnt_cond.col_idx].name;
                if (i16cnt_cond.val < std.math.minInt(i16) or i16cnt_cond.val > std.math.maxInt(i16)) break :raw_int16_cnt_blk;
                if (i16cnt_cond.op == .in2 and (i16cnt_cond.val2 < std.math.minInt(i16) or i16cnt_cond.val2 > std.math.maxInt(i16))) break :raw_int16_cnt_blk;
                const i16cnt_raw = ctx.source.getRawInt16Col(i16cnt_col) orelse break :raw_int16_cnt_blk;
                if (i16cnt_raw.len < total_rows) break :raw_int16_cnt_blk;
                const rhs_i16c: i16 = @intCast(i16cnt_cond.val);
                const rhs2_i16c: i16 = @intCast(i16cnt_cond.val2);
                const I16CntCtx = struct {
                    raw: []const i16,
                    cond: IntCmpCond,
                    rhs_i16: i16,
                    rhs2_i16: i16,
                    accums: []AggAccum,
                    morsel_src: *parallel.MorselSource,
                    mask_buf: []i16,
                    tmp_buf: []i16,
                    fn work(self: *@This(), _: *parallel.MorselSource) void {
                        while (self.morsel_src.next()) |m| {
                            const n = m.end - m.start;
                            cmpBatchDispatch(i16, self.raw[m.start..m.end], self.cond.op, self.rhs_i16, self.rhs2_i16, self.mask_buf[0..n], self.tmp_buf[0..n]);
                            const cnt = simd_batch.countNonZeroI16(self.mask_buf[0..n]);
                            for (self.accums) |*a| {
                                a.count += cnt;
                            }
                        }
                    }
                };
                var i16cnt_morsels = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
                const i16cnt_pctxs = try alloc.alloc(I16CntCtx, n_threads);
                for (i16cnt_pctxs, 0..) |*pc, ti| {
                    pc.* = .{
                        .raw = i16cnt_raw,
                        .cond = i16cnt_cond,
                        .rhs_i16 = rhs_i16c,
                        .rhs2_i16 = rhs2_i16c,
                        .accums = thread_accums[ti],
                        .morsel_src = &i16cnt_morsels,
                        .mask_buf = try alloc.alloc(i16, parallel.default_morsel_size + 1),
                        .tmp_buf = try alloc.alloc(i16, parallel.default_morsel_size + 1),
                    };
                }
                try parallel.parallelFor(alloc, I16CntCtx, I16CntCtx.work, i16cnt_pctxs, &i16cnt_morsels);
                const i16cnt_merged = thread_accums[0];
                for (thread_accums[1..]) |ta| {
                    for (i16cnt_merged, ta, 0..) |*mv, t, ci| {
                        try mergeAccum(mv, t, aggs[ci], alloc);
                    }
                }
                const i16cnt_metas = try alloc.alloc(result.ColMeta, aggs.len);
                const i16cnt_row = try alloc.alloc(?Value, aggs.len);
                for (aggs, 0..) |item, ci| {
                    i16cnt_metas[ci] = .{ .name = item.alias, .col_type = item.out_type };
                    i16cnt_row[ci] = try finalizeAccum(i16cnt_merged[ci], item, alloc);
                }
                var i16cnt_rl = RowBuffer.init(i16cnt_metas);
                try i16cnt_rl.append(alloc, i16cnt_row);
                ctx.profile.raw_path_hits += 1;
                ctx.profile.recordReason("raw_i16_count");
                return i16cnt_rl;
            }
        }
    }

    // ── Raw int16 SUM fast path ───────────────────────────────────────────────
    // For queries like Q33: SUM(int16_col), SUM(int16_col+1), ..., SUM(int16_col+k)
    // Skip fetchRange entirely (avoids int16→i64 copy per morsel).
    // Access raw mmap'd i16 slice via getRawInt16Col, SIMD-sum each morsel.
    if (filter_pred == null) blk_i16: {
        // Detect pattern: all aggs are SUM(same_col) or SUM(same_col + k), same base int16 col.
        var base_col_idx: ?usize = null;
        for (aggs) |item| {
            const ac = switch (item.expr) {
                .agg_call => |a| a,
                else => break :blk_i16,
            };
            if (ac.kind != .sum) break :blk_i16;
            const arg = ac.arg orelse break :blk_i16;
            switch (arg) {
                .col_ref => |cr| {
                    if (base_col_idx == null) base_col_idx = cr.index else if (base_col_idx.? != cr.index) break :blk_i16;
                },
                .add => |bo| {
                    if (bo.left != .col_ref) break :blk_i16;
                    if (bo.right != .lit_i64) break :blk_i16;
                    const ci = bo.left.col_ref.index;
                    if (base_col_idx == null) base_col_idx = ci else if (base_col_idx.? != ci) break :blk_i16;
                },
                else => break :blk_i16,
            }
        }
        const col_idx = base_col_idx orelse break :blk_i16;
        // Get raw i16 mmap slice.
        const src_schema = ctx.source.schema();
        if (col_idx >= src_schema.len) break :blk_i16;
        const col_name = src_schema[col_idx].name;
        const raw_i16 = ctx.source.getRawInt16Col(col_name) orelse break :blk_i16;
        if (raw_i16.len < total_rows) break :blk_i16;

        const RawI16Ctx = struct {
            raw: []const i16,
            aggs: []const plan.ProjectItem,
            accums: []AggAccum,
            morsel_src: *parallel.MorselSource,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                while (self.morsel_src.next()) |m| {
                    const slice = self.raw[m.start..m.end];
                    const col_sum = simd.sumI16(slice);
                    const count: i64 = @intCast(m.end - m.start);
                    for (self.aggs, 0..) |item, ci| {
                        const ac = item.expr.agg_call;
                        const k: i64 = if (ac.arg) |arg| switch (arg) {
                            .add => |bo| bo.right.lit_i64,
                            else => 0,
                        } else 0;
                        self.accums[ci].i64_sum +%= col_sum + count * k;
                    }
                }
            }
        };

        var raw_i16_morsel = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
        const raw_i16_pctxs = try alloc.alloc(RawI16Ctx, n_threads);
        for (raw_i16_pctxs, 0..) |*pc, ti| {
            pc.* = .{
                .raw = raw_i16,
                .aggs = aggs,
                .accums = thread_accums[ti],
                .morsel_src = &raw_i16_morsel,
            };
        }

        try parallel.parallelFor(alloc, RawI16Ctx, RawI16Ctx.work, raw_i16_pctxs, &raw_i16_morsel);

        // Merge accumulators.
        const i16_merged = thread_accums[0];
        for (thread_accums[1..]) |ta| {
            for (i16_merged, ta, 0..) |*m, t, ci| {
                try mergeAccum(m, t, aggs[ci], alloc);
            }
        }

        const metas_i16 = try alloc.alloc(result.ColMeta, aggs.len);
        const out_row_i16 = try alloc.alloc(?Value, aggs.len);
        for (aggs, 0..) |item, ci| {
            metas_i16[ci] = .{ .name = item.alias, .col_type = item.out_type };
            out_row_i16[ci] = try finalizeAccum(i16_merged[ci], item, alloc);
        }
        var i16_rl = RowBuffer.init(metas_i16);
        try i16_rl.append(alloc, out_row_i16);
        ctx.profile.raw_path_hits += 1;
        ctx.profile.recordReason("raw_i16_sum");
        return i16_rl;
    } // end blk_i16

    // ── Raw integer scalar aggregate fast path ───────────────────────────────
    // Covers mixed COUNT/SUM/AVG over raw fixed-width integer columns without
    // fetchRange widening/materialization (e.g. Q3).
    if (filter_pred == null) blk_raw_scalar: {
        const Kind = enum { count, sum, avg };
        const Info = struct { kind: Kind, raw: ?RawColSlice = null };
        var infos_buf: [16]Info = undefined;
        if (aggs.len > infos_buf.len) break :blk_raw_scalar;
        const sm = ctx.source.schema();
        for (aggs, 0..) |item, ai| {
            const ac = switch (item.expr) {
                .agg_call => |a| a,
                else => break :blk_raw_scalar,
            };
            switch (ac.kind) {
                .count_star => infos_buf[ai] = .{ .kind = .count },
                .sum, .avg => {
                    if (ac.kind == .sum and item.out_type == .float64) break :blk_raw_scalar;
                    const arg = ac.arg orelse break :blk_raw_scalar;
                    if (arg != .col_ref) break :blk_raw_scalar;
                    const raw = RawColSlice.resolve(ctx.source, sm, arg.col_ref.index) orelse break :blk_raw_scalar;
                    infos_buf[ai] = .{ .kind = if (ac.kind == .sum) .sum else .avg, .raw = raw };
                },
                else => break :blk_raw_scalar,
            }
        }
        const infos = infos_buf[0..aggs.len];

        const RawScalarCtx = struct {
            infos: []const Info,
            accums: []AggAccum,
            morsel_src: *parallel.MorselSource,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                while (self.morsel_src.next()) |m| {
                    const start: usize = @intCast(m.start);
                    const end: usize = @intCast(m.end);
                    const cnt: u64 = @intCast(end - start);
                    for (self.infos, 0..) |info, ai| {
                        switch (info.kind) {
                            .count => self.accums[ai].count += cnt,
                            .sum => self.accums[ai].i64_sum +%= info.raw.?.sumI64Range(start, end),
                            .avg => {
                                self.accums[ai].f64_avg.sum += info.raw.?.sumF64Range(start, end);
                                self.accums[ai].f64_avg.count += cnt;
                            },
                        }
                    }
                }
            }
        };

        var raw_scalar_morsels = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
        const raw_scalar_ctxs = try alloc.alloc(RawScalarCtx, n_threads);
        for (raw_scalar_ctxs, 0..) |*pc, ti| {
            pc.* = .{
                .infos = infos,
                .accums = thread_accums[ti],
                .morsel_src = &raw_scalar_morsels,
            };
        }
        try parallel.parallelFor(alloc, RawScalarCtx, RawScalarCtx.work, raw_scalar_ctxs, &raw_scalar_morsels);

        const raw_scalar_merged = thread_accums[0];
        for (thread_accums[1..]) |ta| {
            for (raw_scalar_merged, ta, 0..) |*m, t, ci| {
                try mergeAccum(m, t, aggs[ci], alloc);
            }
        }
        const raw_scalar_metas = try alloc.alloc(result.ColMeta, aggs.len);
        const raw_scalar_row = try alloc.alloc(?Value, aggs.len);
        for (aggs, 0..) |item, ci| {
            raw_scalar_metas[ci] = .{ .name = item.alias, .col_type = item.out_type };
            raw_scalar_row[ci] = try finalizeAccum(raw_scalar_merged[ci], item, alloc);
        }
        var raw_scalar_rl = RowBuffer.init(raw_scalar_metas);
        try raw_scalar_rl.append(alloc, raw_scalar_row);
        ctx.profile.raw_path_hits += 1;
        ctx.profile.recordReason("raw_scalar_agg");
        return raw_scalar_rl;
    }

    // ── Raw int64 COUNT(DISTINCT) two-phase fast path ─────────────────────────
    // For queries like Q5: SELECT COUNT(DISTINCT UserID) FROM hits
    //
    // Phase 1 (parallel): scatter raw i64 values into N_CD_PARTS=64 partitions
    //   using low 6 bits of value as partition selector.
    // Phase 2 (parallel): each partition deduplicates independently using an
    //   AutoHashMap sized to partition cardinality → fits in L2 cache.
    // Avoids fetchRange, eliminates the serial O(N) key-set merge.
    blk_cdf: {
        if (aggs.len != 1) break :blk_cdf;
        const ac0 = switch (aggs[0].expr) {
            .agg_call => |a| a,
            else => break :blk_cdf,
        };
        if (ac0.kind != .count or !ac0.distinct) break :blk_cdf;
        const arg0 = ac0.arg orelse break :blk_cdf;
        if (arg0 != .col_ref) break :blk_cdf;
        if (filter_pred != null) break :blk_cdf;
        const cd_col_idx = arg0.col_ref.index;
        const cd_schema = ctx.source.schema();
        if (cd_col_idx >= cd_schema.len) break :blk_cdf;
        const cd_col_name = cd_schema[cd_col_idx].name;
        const cd_raw = ctx.source.getRawInt64Col(cd_col_name) orelse break :blk_cdf;
        if (cd_raw.len < total_rows) break :blk_cdf;

        const N_CD_PARTS: usize = 64;

        const CdScatterCtx = struct {
            raw_col: []const i64,
            parts: [N_CD_PARTS]std.ArrayListUnmanaged(u64),
            buf_arena: std.heap.ArenaAllocator,
            morsel_src: *parallel.MorselSource,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.doWork() catch |e| {
                    self.err = e;
                };
            }

            fn doWork(self: *@This()) !void {
                const ba = self.buf_arena.allocator();
                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |r| {
                        const v: u64 = @bitCast(self.raw_col[r]);
                        try self.parts[v & (N_CD_PARTS - 1)].append(ba, v);
                    }
                }
            }
        };

        var cd_scatter_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
        const cd_scatter_ctxs = try alloc.alloc(CdScatterCtx, n_threads);
        for (cd_scatter_ctxs) |*sc| {
            sc.* = .{
                .raw_col = cd_raw,
                .parts = undefined,
                .buf_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator),
                .morsel_src = &cd_scatter_src,
            };
            for (&sc.parts) |*p| p.* = std.ArrayListUnmanaged(u64).empty;
        }
        try parallel.parallelFor(alloc, CdScatterCtx, CdScatterCtx.work, cd_scatter_ctxs, &cd_scatter_src);
        for (cd_scatter_ctxs) |*sc| {
            if (sc.err) |e| return e;
        }
        defer for (cd_scatter_ctxs) |*sc| sc.buf_arena.deinit();

        // Phase 2: parallel per-partition deduplication.
        const cd_distinct_counts = try alloc.alloc(u64, N_CD_PARTS);
        @memset(cd_distinct_counts, 0);

        const CdAggCtx = struct {
            scatter_ctxs: []CdScatterCtx,
            distinct_counts: []u64,
            morsel_src: *parallel.MorselSource,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.doWork() catch |e| {
                    self.err = e;
                };
            }

            fn doWork(self: *@This()) !void {
                while (self.morsel_src.next()) |m| {
                    const p = m.start;
                    var part_total: usize = 0;
                    for (self.scatter_ctxs) |*sc| part_total += sc.parts[p].items.len;
                    if (part_total == 0) continue;

                    // Per-partition arena: freed at end of partition loop iteration.
                    var part_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
                    defer part_arena.deinit();

                    var hm = std.AutoHashMap(u64, void).init(part_arena.allocator());
                    // Pre-size: assume up to 50% distinct within partition.
                    try hm.ensureTotalCapacity(@intCast(part_total / 2 + 64));
                    for (self.scatter_ctxs) |*sc| {
                        for (sc.parts[p].items) |v| try hm.put(v, {});
                    }
                    self.distinct_counts[p] = hm.count();
                }
            }
        };

        var cd_agg_src = parallel.MorselSource.init(N_CD_PARTS, 1);
        const cd_agg_ctxs = try alloc.alloc(CdAggCtx, n_threads);
        for (cd_agg_ctxs) |*ac| {
            ac.* = .{
                .scatter_ctxs = cd_scatter_ctxs,
                .distinct_counts = cd_distinct_counts,
                .morsel_src = &cd_agg_src,
            };
        }
        try parallel.parallelFor(alloc, CdAggCtx, CdAggCtx.work, cd_agg_ctxs, &cd_agg_src);
        for (cd_agg_ctxs) |*ac| {
            if (ac.err) |e| return e;
        }

        var cd_total: u64 = 0;
        for (cd_distinct_counts) |c| cd_total += c;

        const cd_metas = try alloc.alloc(result.ColMeta, 1);
        cd_metas[0] = .{ .name = aggs[0].alias, .col_type = aggs[0].out_type };
        const cd_row = try alloc.alloc(?Value, 1);
        cd_row[0] = Value{ .uint64 = cd_total };
        var cd_rl = RowBuffer.init(cd_metas);
        try cd_rl.append(alloc, cd_row);
        ctx.profile.raw_path_hits += 1;
        ctx.profile.recordReason("raw_i64_count_distinct");
        return cd_rl;
    } // end blk_cdf

    const ParCtx = struct {
        source: SourceIface,
        filter_pred: ?plan.Expr,
        aggs: []const plan.ProjectItem,
        accums: []AggAccum,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        // Inline int conditions extracted from filter_pred (avoids FilterState.apply()).
        inline_ic: [16]IntCmpCond = undefined,
        inline_ic_n: usize = 0,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                // Fast path: pure int-condition filter — count matching rows inline
                // without allocating a mask or calling the generic FilterState evaluator.
                if (self.inline_ic_n > 0) {
                    const ics = self.inline_ic[0..self.inline_ic_n];
                    var count: u64 = 0;
                    for (0..c.num_rows) |r| {
                        var pass = true;
                        for (ics) |cond| {
                            if (cond.col_idx >= c.columns.len) {
                                pass = false;
                                break;
                            }
                            const col = c.columns[cond.col_idx];
                            if (col.isRowNull(r)) {
                                pass = false;
                                break;
                            }
                            const v: i64 = switch (col.data) {
                                .int64 => |a| a[r],
                                .uint64 => |a| @bitCast(a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                .bool_u8 => |a| @as(i64, a[r]),
                                else => {
                                    pass = false;
                                    break;
                                },
                            };
                            const ok: bool = switch (cond.op) {
                                .eq => v == cond.val,
                                .neq => v != cond.val,
                                .lt => v < cond.val,
                                .lte => v <= cond.val,
                                .gt => v > cond.val,
                                .gte => v >= cond.val,
                                .in2 => v == cond.val or v == cond.val2,
                            };
                            if (!ok) {
                                pass = false;
                                break;
                            }
                        }
                        if (pass) count += 1;
                    }
                    // Accumulate count directly.
                    for (self.accums, self.aggs) |*accum, item| {
                        const ac = item.expr.agg_call;
                        if (ac.kind == .count_star) {
                            accum.count += count;
                        } else {
                            // For non-count aggs fall through to generic path below.
                            // (This block won't be reached for Q2/Q8-style queries.)
                        }
                    }
                    chunk_arena.deinit();
                    continue;
                }

                // Apply filter if any.
                if (self.filter_pred) |pred| {
                    var fs = FilterState{ .predicate = pred };
                    var fake_ctx: QueryContext = undefined;
                    fake_ctx.arena = std.heap.ArenaAllocator.init(calloc);
                    try fs.apply(&c, &fake_ctx);
                }
                if (c.num_rows == 0) continue;
                try updateAccumsFromChunk(self.accums, self.aggs, &c, talloc, null);
                chunk_arena.deinit();
            }
        }
    };

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const pctxs = try alloc.alloc(ParCtx, n_threads);

    // Try to extract pure int conditions for the fast COUNT inline path (Q2/Q8 style).
    const all_count_star: bool = for (aggs) |item| {
        if (item.expr != .agg_call or item.expr.agg_call.kind != .count_star) break false;
    } else true;
    var pre_ic_buf: [16]IntCmpCond = undefined;
    var pre_ic_n: usize = 0;
    var use_inline_ic = false;
    if (all_count_star) {
        if (filter_pred) |fp| {
            const ok = extractAndIntConds(fp, &pre_ic_buf, &pre_ic_n, false);
            if (ok and pre_ic_n > 0) use_inline_ic = true;
        }
    }

    for (pctxs, 0..) |*pc, ti| {
        pc.* = .{
            .source = ctx.source,
            .filter_pred = if (use_inline_ic) null else filter_pred,
            .aggs = aggs,
            .accums = thread_accums[ti],
            .morsel_src = &morsel_src,
            .parent_alloc = alloc,
            .inline_ic_n = if (use_inline_ic) pre_ic_n else 0,
        };
        if (use_inline_ic) @memcpy(pc.inline_ic[0..pre_ic_n], pre_ic_buf[0..pre_ic_n]);
    }

    try parallel.parallelFor(alloc, ParCtx, ParCtx.work, pctxs, &morsel_src);

    // Check errors.
    for (pctxs) |pc| {
        if (pc.err) |e| return e;
    }

    // Merge all thread accumulators into thread_accums[0].
    const merged = thread_accums[0];
    for (thread_accums[1..]) |ta| {
        for (merged, ta, 0..) |*m, t, ci| {
            try mergeAccum(m, t, aggs[ci], alloc);
        }
    }

    return try finalizeScalarAgg(merged, aggs, alloc);
}

pub fn mergeAccum(dst: *AggAccum, src: AggAccum, item: plan.ProjectItem, alloc: std.mem.Allocator) !void {
    _ = item;
    switch (dst.*) {
        .count => dst.count += src.count,
        .i64_sum => dst.i64_sum +%= src.i64_sum,
        .u64_sum => dst.u64_sum +%= src.u64_sum,
        .f64_sum => dst.f64_sum += src.f64_sum,
        .f64_avg => {
            dst.f64_avg.sum += src.f64_avg.sum;
            dst.f64_avg.count += src.f64_avg.count;
        },
        .i64_min => dst.i64_min = @min(dst.i64_min, src.i64_min),
        .i64_max => dst.i64_max = @max(dst.i64_max, src.i64_max),
        .u64_min => dst.u64_min = @min(dst.u64_min, src.u64_min),
        .u64_max => dst.u64_max = @max(dst.u64_max, src.u64_max),
        .f64_min => dst.f64_min = @min(dst.f64_min, src.f64_min),
        .f64_max => dst.f64_max = @max(dst.f64_max, src.f64_max),
        .str_min => {
            if (src.str_min) |sv| {
                if (dst.str_min == null or std.mem.lessThan(u8, sv, dst.str_min.?)) {
                    dst.str_min = sv;
                }
            }
        },
        .str_max => {
            if (src.str_max) |sv| {
                if (dst.str_max == null or std.mem.lessThan(u8, dst.str_max.?, sv)) {
                    dst.str_max = sv;
                }
            }
        },
        .any_val => {
            if (dst.any_val == null) dst.any_val = src.any_val;
        },
        .uniq_strs => {
            var it = src.uniq_strs.keyIterator();
            while (it.next()) |k| {
                try dst.uniq_strs.put(alloc, try alloc.dupe(u8, k.*), {});
            }
        },
        .array_strs => {
            for (src.array_strs.items) |s| {
                try dst.array_strs.append(alloc, try alloc.dupe(u8, s));
            }
        },
        .distinct_u64 => {
            var it = src.distinct_u64.keyIterator();
            while (it.next()) |k| try dst.distinct_u64.put(std.heap.c_allocator, k.*, {});
        },
    }
}

pub fn updateAccumsFromChunk(
    accums: []AggAccum,
    aggs: []const plan.ProjectItem,
    c: *const DataChunk,
    alloc: std.mem.Allocator,
    ctx: ?*QueryContext,
) !void {
    // Track which aggs need a per-row fallback pass (one pass covers all of them).
    var needs_fallback = false;
    // Temp boolean array to mark which indices need fallback.
    const fb_mask = try alloc.alloc(bool, aggs.len);
    @memset(fb_mask, false);

    // Fast path: all aggs are SUM(same_col) or SUM(same_col + k) for int64 column —
    // compute SUM(col) + count*k in a single pass instead of one pass per agg.
    // Saves O(90) passes for Q30 (90× SUM(ResolutionWidth + k)).
    if (aggs.len > 1) blk: {
        var base_col_idx: ?usize = null;
        for (aggs) |item| {
            const ac = switch (item.expr) {
                .agg_call => |a| a,
                else => break :blk,
            };
            if (ac.kind != .sum) break :blk;
            const arg = ac.arg orelse break :blk;
            switch (arg) {
                .col_ref => |cr| {
                    if (base_col_idx == null) base_col_idx = cr.index else if (base_col_idx.? != cr.index) break :blk;
                },
                .add => |bo| {
                    const cr = switch (bo.left) {
                        .col_ref => |c2| c2,
                        else => break :blk,
                    };
                    _ = switch (bo.right) {
                        .lit_i64 => {},
                        else => break :blk,
                    };
                    if (base_col_idx == null) base_col_idx = cr.index else if (base_col_idx.? != cr.index) break :blk;
                },
                else => break :blk,
            }
        }
        const col_idx = base_col_idx orelse break :blk;
        const col = c.columns[col_idx];
        const vals = switch (col.data) {
            .int64 => |v| v,
            else => break :blk,
        };
        // Verify all accumulators are i64_sum.
        for (0..aggs.len) |ci| {
            if (accums[ci] != .i64_sum) break :blk;
        }
        // Single pass: accumulate col_sum and non_null_count.
        var col_sum: i64 = 0;
        var non_null_count: i64 = 0;
        if (chunk.allNonNull(col.null_mask)) {
            // Fast path: no nulls — use SIMD sum.
            col_sum = simd.sumI64(vals[0..c.num_rows]);
            non_null_count = @intCast(c.num_rows);
        } else {
            for (0..c.num_rows) |r| {
                if (!chunk.isNull(col.null_mask, r)) {
                    col_sum +%= vals[r];
                    non_null_count += 1;
                }
            }
        }
        // Update each accumulator analytically: SUM(col+k) = SUM(col) + count*k.
        for (aggs, 0..) |item, ci| {
            const k: i64 = switch (item.expr.agg_call.arg.?) {
                .col_ref => 0,
                .add => |bo| bo.right.lit_i64,
                else => 0,
            };
            accums[ci].i64_sum +%= col_sum + non_null_count * k;
        }
        return;
    }

    for (aggs, 0..) |item, ci| {
        const acc_ptr = &accums[ci];
        var handled = false;
        switch (item.expr) {
            .agg_call => |ac| {
                switch (ac.kind) {
                    .count_star => {
                        acc_ptr.count += c.num_rows;
                        handled = true;
                    },
                    .count => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    if (acc_ptr.* == .distinct_u64) {
                                        // Fast path for COUNT(DISTINCT col): insert raw values.
                                        switch (col.data) {
                                            .int64 => |vals| {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        try acc_ptr.distinct_u64.put(std.heap.c_allocator, @as(u64, @bitCast(vals[r])), {});
                                                }
                                                handled = true;
                                            },
                                            .uint64 => |vals| {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r))
                                                        try acc_ptr.distinct_u64.put(std.heap.c_allocator, vals[r], {});
                                                }
                                                handled = true;
                                            },
                                            .string => |vals| {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r)) {
                                                        const h = std.hash.Wyhash.hash(0, vals[r]);
                                                        try acc_ptr.distinct_u64.put(std.heap.c_allocator, h, {});
                                                    }
                                                }
                                                handled = true;
                                            },
                                            else => {},
                                        }
                                    } else {
                                        for (0..c.num_rows) |r| {
                                            if (!chunk.isNull(col.null_mask, r)) acc_ptr.count += 1;
                                        }
                                        handled = true;
                                    }
                                },
                                else => {},
                            }
                        } else {
                            acc_ptr.count += c.num_rows;
                            handled = true;
                        }
                    },
                    .sum => {
                        if (ac.arg) |arg| {
                            switch (arg) {
                                .col_ref => |cr| {
                                    const col = c.columns[cr.index];
                                    switch (col.data) {
                                        .int64 => |vals| {
                                            if (acc_ptr.* == .i64_sum) {
                                                acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .uint64 => |vals| {
                                            if (acc_ptr.* == .u64_sum) {
                                                acc_ptr.u64_sum +%= @bitCast(simd.sumU64(vals[0..c.num_rows]));
                                                handled = true;
                                            } else if (acc_ptr.* == .i64_sum) {
                                                acc_ptr.i64_sum +%= simd.sumU64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        .float64 => |vals| {
                                            if (acc_ptr.* == .f64_sum) {
                                                acc_ptr.f64_sum += simd.sumF64(vals[0..c.num_rows]);
                                                handled = true;
                                            }
                                        },
                                        else => {},
                                    }
                                },
                                // SUM(col + int_literal): vectorized sum of (val + k)
                                .add => |bo| {
                                    const cr_opt: ?plan.ColRef = switch (bo.left) {
                                        .col_ref => |c2| c2,
                                        else => null,
                                    };
                                    const k_opt: ?i64 = switch (bo.right) {
                                        .lit_i64 => |v| v,
                                        else => null,
                                    };
                                    if (cr_opt != null and k_opt != null) {
                                        const cr = cr_opt.?;
                                        const k = k_opt.?;
                                        const col = c.columns[cr.index];
                                        switch (col.data) {
                                            .int64 => |vals| {
                                                if (acc_ptr.* == .i64_sum) {
                                                    if (chunk.allNonNull(col.null_mask)) {
                                                        acc_ptr.i64_sum +%= simd.sumI64(vals[0..c.num_rows]) +% (@as(i64, @intCast(c.num_rows)) *% k);
                                                    } else {
                                                        for (0..c.num_rows) |r| {
                                                            if (!chunk.isNull(col.null_mask, r)) acc_ptr.i64_sum +%= vals[r] + k;
                                                        }
                                                    }
                                                    handled = true;
                                                }
                                            },
                                            else => {},
                                        }
                                    }
                                },
                                else => {},
                            }
                        }
                    },
                    .avg => {
                        // AVG accumulates into f64_avg (sum + count for correct finalization).
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .f64_avg) {
                                            // simd.sumI64 can overflow for large int64 values (e.g. UserID).
                                            // Accumulate directly as f64 to avoid overflow.
                                            if (chunk.allNonNull(col.null_mask)) {
                                                for (0..c.num_rows) |r| {
                                                    acc_ptr.f64_avg.sum += @floatFromInt(vals[r]);
                                                }
                                                acc_ptr.f64_avg.count += c.num_rows;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r)) {
                                                        acc_ptr.f64_avg.sum += @floatFromInt(vals[r]);
                                                        acc_ptr.f64_avg.count += 1;
                                                    }
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .f64_avg) {
                                            // Cannot use simd.sumU64: sum of large uint64 values overflows.
                                            // Accumulate directly as f64.
                                            if (chunk.allNonNull(col.null_mask)) {
                                                for (0..c.num_rows) |r| {
                                                    acc_ptr.f64_avg.sum += @floatFromInt(vals[r]);
                                                }
                                                acc_ptr.f64_avg.count += c.num_rows;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r)) {
                                                        acc_ptr.f64_avg.sum += @floatFromInt(vals[r]);
                                                        acc_ptr.f64_avg.count += 1;
                                                    }
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_avg) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                acc_ptr.f64_avg.sum += simd.sumF64(vals[0..c.num_rows]);
                                                acc_ptr.f64_avg.count += c.num_rows;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r)) {
                                                        acc_ptr.f64_avg.sum += vals[r];
                                                        acc_ptr.f64_avg.count += 1;
                                                    }
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .min => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minI64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.i64_min)
                                                        acc_ptr.i64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.u64_min)
                                                    acc_ptr.u64_min = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_min) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.minF64(vals[0..c.num_rows]);
                                                if (v < acc_ptr.f64_min) acc_ptr.f64_min = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] < acc_ptr.f64_min)
                                                        acc_ptr.f64_min = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_min) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v < acc_ptr.i64_min) acc_ptr.i64_min = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    .max => {
                        if (ac.arg) |arg| {
                            if (arg == .col_ref) {
                                const col = c.columns[arg.col_ref.index];
                                switch (col.data) {
                                    .int64 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxI64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.i64_max)
                                                        acc_ptr.i64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .uint64 => |vals| {
                                        if (acc_ptr.* == .u64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.u64_max)
                                                    acc_ptr.u64_max = vals[r];
                                            }
                                            handled = true;
                                        }
                                    },
                                    .float64 => |vals| {
                                        if (acc_ptr.* == .f64_max) {
                                            if (chunk.allNonNull(col.null_mask)) {
                                                const v = simd.maxF64(vals[0..c.num_rows]);
                                                if (v > acc_ptr.f64_max) acc_ptr.f64_max = v;
                                            } else {
                                                for (0..c.num_rows) |r| {
                                                    if (!chunk.isNull(col.null_mask, r) and vals[r] > acc_ptr.f64_max)
                                                        acc_ptr.f64_max = vals[r];
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    .date_u16 => |vals| {
                                        if (acc_ptr.* == .i64_max) {
                                            for (0..c.num_rows) |r| {
                                                if (!chunk.isNull(col.null_mask, r)) {
                                                    const v: i64 = vals[r];
                                                    if (v > acc_ptr.i64_max) acc_ptr.i64_max = v;
                                                }
                                            }
                                            handled = true;
                                        }
                                    },
                                    else => {},
                                }
                            }
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
        if (!handled) {
            fb_mask[ci] = true;
            needs_fallback = true;
        }
    }

    if (!needs_fallback) return;
    if (ctx) |qctx| {
        qctx.profile.fallback_rows += c.num_rows;
        qctx.profile.recordReason("scalar_agg_row_fallback");
    }

    // Collect referenced columns for all fallback aggs.
    const ref_mask2 = try alloc.alloc(bool, c.columns.len);
    @memset(ref_mask2, false);
    for (aggs, 0..) |item, ci| {
        if (fb_mask[ci]) collectColRefs(item.expr, ref_mask2);
    }

    // Single per-row pass for all fallback aggs.
    const row = try alloc.alloc(?Value, c.columns.len);
    @memset(row, null);
    for (0..c.num_rows) |r| {
        for (ref_mask2, 0..) |m, j| if (m) {
            const col = c.columns[j];
            row[j] = if (col.isRowNull(r)) null else col.data.get(r);
        };
        for (aggs, 0..) |item, ci| {
            if (!fb_mask[ci]) continue;
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }
}

pub fn extractFilter(node: *const plan.PhysicalNode) ?FilterState {
    return switch (node.*) {
        .filter => |f| .{ .predicate = f.predicate },
        .limit => |l| extractFilter(l.input),
        .project => |p| extractFilter(p.input),
        else => null,
    };
}

pub fn executeScalarAgg(inner: RowBuffer, aggs: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowBuffer {
    const accums = try initAggAccums(aggs, alloc);

    for (inner.rows.items) |row| {
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&accums[ci], v_opt, alloc);
        }
    }

    return finalizeScalarAgg(accums, aggs, alloc);
}

pub fn finalizeAccum(acc: AggAccum, item: plan.ProjectItem, alloc: std.mem.Allocator) !?Value {
    const ac_opt: ?*const plan.AggCall = switch (item.expr) {
        .agg_call => |ac| ac,
        else => null,
    };
    const base_val: ?Value = blk: {
        const sep = if (ac_opt) |ac| ac.sep else null;
        if (sep) |s| {
            const arr_val = try acc.toArrayValue(alloc);
            const elems = arr_val.array_string;
            if (elems.len == 0) break :blk Value{ .string = "" };
            var total: usize = 0;
            for (elems) |e| total += e.len;
            total += s.len * (elems.len - 1);
            const buf = try alloc.alloc(u8, total);
            var pos: usize = 0;
            for (elems, 0..) |e, idx| {
                if (idx > 0) {
                    @memcpy(buf[pos .. pos + s.len], s);
                    pos += s.len;
                }
                @memcpy(buf[pos .. pos + e.len], e);
                pos += e.len;
            }
            break :blk Value{ .string = buf };
        }
        break :blk acc.toValue() catch (try acc.toArrayValue(alloc));
    };
    if (ac_opt) |ac| {
        if (ac.post_expr) |post| {
            const row = [_]?Value{base_val};
            return kernels.evalExpr(post, &row, null, alloc);
        }
    }
    return base_val;
}

pub fn evalAggArg(expr: plan.Expr, row: []const ?Value, alloc: std.mem.Allocator) !?Value {
    return switch (expr) {
        .agg_call => |ac| if (ac.arg) |arg| blk: {
            // Inline fast paths for common single-arg function calls to avoid dispatch overhead.
            if (arg == .fn_call) {
                const fc = arg.fn_call;
                if (fc.args.len == 1 and fc.args[0] == .col_ref) {
                    const col_val = row[fc.args[0].col_ref.index] orelse break :blk null;
                    if (std.mem.eql(u8, fc.name, "length") or
                        std.mem.eql(u8, fc.name, "char_length") or
                        std.mem.eql(u8, fc.name, "len"))
                    {
                        const s = col_val.toStr() orelse break :blk null;
                        break :blk Value{ .int64 = @intCast(s.len) };
                    }
                }
            }
            break :blk kernels.evalExpr(arg, row, null, alloc);
        } else null,
        else => kernels.evalExpr(expr, row, null, alloc),
    };
}

pub fn buildAggResultMetas(
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    alloc: std.mem.Allocator,
) ![]result.ColMeta {
    const metas = try alloc.alloc(result.ColMeta, keys.len + aggs.len);
    for (keys, 0..) |item, i| {
        metas[i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    for (aggs, 0..) |item, i| {
        metas[keys.len + i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    return metas;
}

pub fn finalizeScalarAgg(
    accums: []AggAccum,
    aggs: []const plan.ProjectItem,
    alloc: std.mem.Allocator,
) !RowBuffer {
    const row = try alloc.alloc(?Value, aggs.len);
    for (aggs, 0..) |item, i| {
        row[i] = try finalizeAccum(accums[i], item, alloc);
        deinitAccum(&accums[i]);
    }
    var result_rows = RowBuffer.init(try buildAggResultMetas(&.{}, aggs, alloc));
    try result_rows.append(alloc, row);
    return result_rows;
}

pub fn extractLimit(node: *const plan.PhysicalNode) ?LimitState {
    return switch (node.*) {
        .limit => |l| .{ .limit = l.limit, .offset = l.offset, .emitted = 0 },
        .filter => |f| extractLimit(f.input),
        .project => |p| extractLimit(p.input),
        else => null,
    };
}

pub fn initAccumForAgg(item: plan.ProjectItem) AggAccum {
    return switch (item.expr) {
        .agg_call => |ac| switch (ac.kind) {
            .count_star => .{ .count = 0 },
            .count => if (ac.distinct) .{ .distinct_u64 = .{} } else .{ .count = 0 },
            .sum => if (item.out_type == .float64) .{ .f64_sum = 0.0 } else .{ .i64_sum = 0 },
            .avg => .{ .f64_avg = .{ .sum = 0.0, .count = 0 } },
            .min => if (item.out_type == .string) .{ .str_min = null } else .{ .i64_min = std.math.maxInt(i64) },
            .max => if (item.out_type == .string) .{ .str_max = null } else .{ .i64_max = std.math.minInt(i64) },
            .group_uniq_array => .{ .uniq_strs = .{} },
            .group_array => .{ .array_strs = .empty },
            .any => .{ .any_val = null },
        },
        else => .{ .count = 0 },
    };
}

pub fn deinitAccum(acc: *AggAccum) void {
    switch (acc.*) {
        .distinct_u64 => |*m| m.deinit(std.heap.c_allocator),
        else => {},
    }
}
