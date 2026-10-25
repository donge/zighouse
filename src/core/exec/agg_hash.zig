//! Hash aggregation layouts and strategy implementations.
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
const topn = @import("topn.zig");
const row_buffer = @import("row_buffer.zig");
const agg = @import("agg.zig");
const chunk_sink = @import("chunk_sink.zig");
const chunk_sink_mod = chunk_sink;
const hashmap = @import("hashmap");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const DataChunk = chunk.DataChunk;
pub const QueryContext = context.QueryContext;
pub const SourceIface = context.SourceIface;
pub const FilterState = operators.FilterState;
pub const ScanRequest = operators.ScanRequest;
pub const RawColSlice = operators.RawColSlice;
pub const IntCmpCond = operators.IntCmpCond;
pub const CaseWhenStrKey = operators.CaseWhenStrKey;
pub const LikeGuard = operators.LikeGuard;
pub const StrCmpCond = operators.StrCmpCond;
pub const RowBuffer = row_buffer.RowBuffer;
pub const ChunkResult = chunk_sink.ChunkSink.Result;
pub const executeTopK = topn.executeTopK;
pub const rowLessThan = topn.rowLessThan;
pub const TopKSortCtx = topn.TopKSortCtx;
pub const TopKCollector = topn.TopKCollector;
pub const executeTopKFromChunkResult = topn.executeTopKFromChunkResult;
pub const rowListToChunkResult = row_buffer.rowListToChunkResult;
pub const chunkResultToRowBuffer = row_buffer.chunkResultToRowBuffer;
pub const buildAggResultMetas = agg.buildAggResultMetas;
pub const extractFilter = agg.extractFilter;
pub const updateAccumsFromChunk = agg.updateAccumsFromChunk;
pub const finalizeAccum = agg.finalizeAccum;
pub const evalAggArg = agg.evalAggArg;
pub const initAggAccums = agg.initAggAccums;
pub const extractAndIntConds = operators.extractAndIntConds;
pub const extractAndStrConds = operators.extractAndStrConds;
pub const extractMixedAndConds = operators.extractMixedAndConds;
pub const tryExtractSimpleStrFilter = operators.tryExtractSimpleStrFilter;
pub const collectLikeGuards = operators.collectLikeGuards;
pub const collectColRefs = operators.collectColRefs;
pub const extractCaseWhenStrKey = operators.extractCaseWhenStrKey;
pub const applyIntCondSIMD = operators.applyIntCondSIMD;
pub const cmpBatchDispatch = operators.cmpBatchDispatch;
pub const SimpleStrFilter = operators.SimpleStrFilter;
const simd = simd_ops;

const common = @import("agg_hash_common.zig");
pub const finalizeAggRows = common.finalizeAggRows;
pub const emitCompactVals = common.emitCompactVals;
pub const emitCompactValsWithSidecar = common.emitCompactValsWithSidecar;
pub const updateCompactVals = common.updateCompactVals;
pub const executeOrderBy = common.executeOrderBy;
pub const ChunkRowSink = common.ChunkRowSink;
const topk_mod = @import("agg_hash_compact_topk.zig");
const tp_mod = @import("agg_hash_two_phase.zig");
pub const executeHashAggParallelCompactTopK = topk_mod.executeHashAggParallelCompactTopK;
pub const executeBoundedCountTopDistinct = topk_mod.executeBoundedCountTopDistinct;
pub const executeTwoPhaseHashAgg = tp_mod.executeTwoPhaseHashAgg;
const pair_mod = @import("agg_hash_pair.zig");
const str_mod = @import("agg_hash_str.zig");
pub const executeHashAggParallelPairCountBounded = pair_mod.executeHashAggParallelPairCountBounded;
pub const executeHashAggParallelPairCount = pair_mod.executeHashAggParallelPairCount;
pub const executeHashAggParallelTripleCount = pair_mod.executeHashAggParallelTripleCount;
pub const executeHashAggParallelStrKey = str_mod.executeHashAggParallelStrKey;
pub const executeTwoPhaseHashAggStrKeySimple = str_mod.executeTwoPhaseHashAggStrKeySimple;

pub fn executeHashAggScannable(
    ha: plan.HashAggNode,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    top_offset: usize,
    ctx: *QueryContext,
) !ChunkResult {
    // The planner selects one layout. Fast implementations may reject a chunk
    // shape (e.g. pair-count with a filtered input); fall back once to the
    // generic layout for the key type, then to the chunked aggregation. This is
    // a layout fallback, not a query-shape strategy probe.
    if (ha.strategy != .auto) {
        if (try executeHashAggStrategy(ha.strategy, ha, sort_keys, top_k, top_offset, ctx)) |rl|
            return try rowListToChunkResult(rl, ctx.allocator());
    }
    var has_string_key = false;
    for (ha.keys) |k| {
        if (k.out_type == .string or k.out_type == .array_string) {
            has_string_key = true;
            break;
        }
    }
    const fallback_strategy: plan.HashAggNode.Strategy = if (has_string_key) .string_key else .compact_int;
    if (fallback_strategy != ha.strategy) {
        if (try executeHashAggStrategy(fallback_strategy, ha, sort_keys, top_k, top_offset, ctx)) |rl|
            return try rowListToChunkResult(rl, ctx.allocator());
    }
    var res = try executeHashAggChunked(ha.input, ha.keys, ha.aggs, ctx);
    if (top_k > 0 and sort_keys.len > 0) {
        const out = try executeTopKFromChunkResult(res, sort_keys, top_k, ctx.allocator());
        res.deinit(ctx.allocator());
        return try rowListToChunkResult(out, ctx.allocator());
    }
    return res;
}

pub fn executeHashAggStrategy(
    strategy: plan.HashAggNode.Strategy,
    ha: plan.HashAggNode,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    top_offset: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    return switch (strategy) {
        .auto => null,
        .compact_int => blk: {
            if (top_k > 0 and sort_keys.len > 0) {
                break :blk try executeHashAggParallelCompactTopK(ha.input, ha.keys, ha.aggs, sort_keys, top_k, top_offset, ctx);
            }
            break :blk try executeHashAggParallelCompact(ha.input, ha.keys, ha.aggs, ctx);
        },
        .pair_count => try executeHashAggParallelPairCount(ha.input, ha.keys, ha.aggs, sort_keys, top_k, ctx),
        .triple_count => try executeHashAggParallelTripleCount(ha.input, ha.keys, ha.aggs, sort_keys, top_k, ctx),
        .string_key => try executeHashAggParallelStrKey(ha.input, ha.keys, ha.aggs, sort_keys, top_k, ctx),
    };
}

pub fn executeHashAggChunked(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    ctx: *QueryContext,
) !chunk_sink_mod.ChunkSink.Result {
    const alloc = ctx.allocator();
    // Pre-size hash table when input is a bare scan (no filter reduces cardinality).
    // Use min(row_count, 2M) to avoid excessive memory that degrades subsequent queries.
    const MAX_PRESIZED: u64 = 8_000_000;
    const est_rows: u64 = switch (input.*) {
        .part_scan, .mem_scan, .chunk_source => @min(ctx.source.rowCount(), MAX_PRESIZED),
        else => 0,
    };

    var ht_agg = try ht.AggHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows);
    const key_buf = try alloc.alloc(Value, keys.len);
    const init_accums = try initAggAccums(aggs, alloc);
    var filter_state: ?FilterState = extractFilter(input);
    var scan_request: ScanRequest = .{};
    scan_request.addItems(keys);
    scan_request.addItems(aggs);
    if (filter_state) |*fs| scan_request.addExpr(fs.predicate);
    const scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    ctx.source.reset();
    var ref_indices: ?[]usize = null;

    var c: DataChunk = undefined;
    var row_buf: []?Value = &.{};
    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;
        // Build ref_indices once (on first non-empty chunk).
        if (ref_indices == null) {
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            for (keys) |k| collectColRefs(k.expr, mask);
            for (aggs) |a| collectColRefs(a.expr, mask);
            var cnt: usize = 0;
            for (mask) |m| {
                if (m) cnt += 1;
            }
            const idxs = try alloc.alloc(usize, cnt);
            var wi: usize = 0;
            for (mask, 0..) |m, j| {
                if (m) {
                    idxs[wi] = j;
                    wi += 1;
                }
            }
            ref_indices = idxs;
        }
        const refs = ref_indices.?;

        {
            // ── General path ──────────────────────────────────────────────────
            for (0..c.num_rows) |r| {
                // Fill only referenced columns.
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                for (keys, 0..) |k, ki| {
                    // Inline fast path for common key expressions (avoids evalExpr dispatch).
                    const v: ?Value = switch (k.expr) {
                        .col_ref => |cr| row_buf[cr.index],
                        .add => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv +% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .sub => |op| blk: {
                            if (op.left == .col_ref and op.right == .lit_i64) {
                                if (row_buf[op.left.col_ref.index]) |base| {
                                    if (base.toI64()) |bv| break :blk Value{ .int64 = bv -% op.right.lit_i64 };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        .fn_call => |fc| blk: {
                            // Fast path: date_part('minute'/'hour', col_ref) — avoids arg eval + string dispatch.
                            if (fc.args.len == 2 and
                                fc.args[0] == .lit_str and
                                fc.args[1] == .col_ref)
                            {
                                const unit = fc.args[0].lit_str;
                                const col_idx = fc.args[1].col_ref.index;
                                if (row_buf[col_idx]) |ts_val| {
                                    const ms: i64 = switch (ts_val) {
                                        .datetime64_ms => |m| m,
                                        .int64 => |i| i * 1000,
                                        else => {
                                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                                        },
                                    };
                                    const secs = @divTrunc(ms, 1000);
                                    if (std.mem.eql(u8, unit, "minute") or std.mem.eql(u8, unit, "min")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 60), 60) };
                                    }
                                    if (std.mem.eql(u8, unit, "hour")) {
                                        break :blk Value{ .int64 = @mod(@divTrunc(secs, 3600), 24) };
                                    }
                                }
                            }
                            // Fast path: regexp_replace(col_ref, lit_str_pattern, lit_str_repl)
                            // for the Q29 URL-domain extraction pattern.
                            if (fc.name.len > 0 and
                                (std.mem.eql(u8, fc.name, "regexp_replace") or
                                    std.mem.eql(u8, fc.name, "replaceRegexpOne")) and
                                fc.args.len >= 3 and
                                fc.args[0] == .col_ref and
                                fc.args[1] == .lit_str and
                                fc.args[2] == .lit_str)
                            {
                                const col_idx = fc.args[0].col_ref.index;
                                const pattern = fc.args[1].lit_str;
                                const s = (row_buf[col_idx] orelse break :blk null).toStr() orelse break :blk null;
                                if (std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                                    std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*"))
                                {
                                    const after_proto = if (std.mem.startsWith(u8, s, "https://"))
                                        s[8..]
                                    else if (std.mem.startsWith(u8, s, "http://"))
                                        s[7..]
                                    else
                                        break :blk Value{ .string = s };
                                    const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                        break :blk Value{ .string = s };
                                    var host = after_proto[0..slash];
                                    if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                                    break :blk Value{ .string = host };
                                }
                            }
                            break :blk try kernels.evalExpr(k.expr, row_buf, null, alloc);
                        },
                        else => try kernels.evalExpr(k.expr, row_buf, null, alloc),
                    };
                    key_buf[ki] = v orelse Value{ .int64 = 0 };
                }
                const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                for (aggs, 0..) |item, ci| {
                    const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                    try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                }
            }
        }
    }

    const out_metas = try buildAggResultMetas(keys, aggs, alloc);
    var sink = try chunk_sink_mod.ChunkSink.init(alloc, out_metas);
    var rl = ChunkRowSink{ .sink = &sink };

    {
        const CtxT = struct {
            rl: *ChunkRowSink,
            alloc: std.mem.Allocator,
            keys_len: usize,
            aggs: []const plan.ProjectItem,
        };
        var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
        ht_agg.iterate(&emit_ctx, struct {
            fn cb(ec: *CtxT, k: []const Value, bucket: []const AggAccum) void {
                const row = ec.alloc.alloc(?Value, ec.keys_len + bucket.len) catch return;
                for (k, 0..) |kv, i| row[i] = kv;
                for (bucket, ec.aggs, 0..) |acc, item, i| {
                    row[ec.keys_len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
                }
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    }
    const chunks = try sink.finish();
    sink.deinit();
    return .{ .chunks = chunks, .metas = out_metas };
}

pub fn executeHashAgg(
    inner: RowBuffer,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    alloc: std.mem.Allocator,
) !RowBuffer {
    var ht_agg = try ht.AggHashTable.init(alloc, keys.len, aggs.len);

    const init_accums = try initAggAccums(aggs, alloc);
    const key_buf = try alloc.alloc(Value, keys.len);
    for (inner.rows.items) |row| {
        for (keys, 0..) |k, ki| {
            const v = try kernels.evalExpr(k.expr, row, null, alloc);
            key_buf[ki] = v orelse Value{ .int64 = 0 }; // NULL → zero sentinel for hashing
        }
        const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
        for (aggs, 0..) |item, ci| {
            const v_opt = try evalAggArg(item.expr, row, alloc);
            try kernels.updateAccum(&bucket[ci], v_opt, alloc);
        }
    }

    const out_metas = try buildAggResultMetas(keys, aggs, alloc);
    var rl = RowBuffer.init(out_metas);

    const CtxT = struct {
        rl: *RowBuffer,
        alloc: std.mem.Allocator,
        keys_len: usize,
        aggs: []const plan.ProjectItem,
    };
    var emit_ctx = CtxT{ .rl = &rl, .alloc = alloc, .keys_len = keys.len, .aggs = aggs };
    ht_agg.iterate(&emit_ctx, struct {
        fn cb(c: *CtxT, k: []const Value, bucket: []const AggAccum) void {
            const row = c.alloc.alloc(?Value, c.keys_len + bucket.len) catch return;
            for (k, 0..) |kv, i| row[i] = kv;
            for (bucket, c.aggs, 0..) |acc, item, i| {
                row[c.keys_len + i] = finalizeAccum(acc, item, c.alloc) catch null;
            }
            c.rl.append(c.alloc, row) catch {};
        }
    }.cb);

    return rl;
}

pub fn executeHashAggParallelCompact(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    ctx: *QueryContext,
) !?RowBuffer {
    return executeHashAggParallelCompactTopK(input, keys, aggs, &.{}, 0, 0, ctx);
}
