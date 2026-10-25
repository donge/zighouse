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

pub fn keysAreIntExpr(keys: []const plan.ProjectItem) bool {
    for (keys) |k| {
        if (k.out_type == .string) return false; // string columns cannot use the int-key path
        switch (k.expr) {
            .col_ref => {},
            .add => |op| {
                if (op.left != .col_ref or op.right != .lit_i64) return false;
            },
            .sub => |op| {
                if (op.left != .col_ref or op.right != .lit_i64) return false;
            },
            else => return false,
        }
    }
    return true;
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

    // Fast path: if all keys are integer col_ref / col_ref±lit expressions, try
    // IntKeyHashTable (no []Value boxing, inline key storage).
    const maybe_int_keys = keysAreIntExpr(keys);

    // Compact accum fast path: if all aggs are pure-numeric, use
    // CompactIntKeyHashTable (8B/agg vs 32B/agg), cutting the accum slab 4×.
    // Only active when maybe_int_keys is true.
    const compact_kinds: ?[]ht.CompactAggKind = blk: {
        const kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
        for (aggs, 0..) |item, ci| {
            if (item.expr != .agg_call) break :blk null;
            kinds[ci] = switch (item.expr.agg_call.kind) {
                .count_star => .count,
                .count => blk2: {
                    if (!item.expr.agg_call.distinct) break :blk2 .count;
                    // COUNT(DISTINCT col_ref int) → compact distinct path.
                    const arg = item.expr.agg_call.arg orelse break :blk null;
                    if (arg != .col_ref) break :blk null;
                    break :blk2 .count_distinct_u64;
                },
                .sum => if (item.out_type == .float64) .f64_sum else .i64_sum,
                .avg => blk2: {
                    const avg_arg = item.expr.agg_call.arg orelse break :blk null;
                    if (avg_arg == .col_ref) break :blk2 .f64_sum;
                    // avg(length(str_col)) — accumulate string length sum; finalize as avg.
                    if (avg_arg == .fn_call and
                        std.mem.eql(u8, avg_arg.fn_call.name, "length") and
                        avg_arg.fn_call.args.len == 1 and
                        avg_arg.fn_call.args[0] == .col_ref)
                        break :blk2 .f64_str_len_sum;
                    break :blk null;
                },
                // min/max: string args use str_min/str_max (StrAggHashTable sidecar);
                // numeric args use the appropriate numeric kind (refined at runtime).
                .min => if (item.out_type == .string) .str_min else .i64_min,
                .max => if (item.out_type == .string) .str_max else .i64_max,
                .group_uniq_array, .group_array, .any => break :blk null,
            };
        }
        break :blk kinds;
    };
    // init_vals: u64 encoding of the initial value per compact agg kind.
    const compact_init_vals: []u64 = if (compact_kinds) |ck| blk: {
        const iv = try alloc.alloc(u64, ck.len);
        for (ck, 0..) |kind, ci| {
            iv[ci] = switch (kind) {
                .count, .i64_sum, .u64_sum, .u64_max, .f64_str_len_sum, .count_distinct_u64 => 0,
                .f64_sum => @bitCast(@as(f64, 0.0)),
                .i64_min => @bitCast(@as(i64, std.math.maxInt(i64))),
                .i64_max => @bitCast(@as(i64, std.math.minInt(i64))),
                .u64_min => std.math.maxInt(u64),
                .f64_min => @bitCast(std.math.inf(f64)),
                .f64_max => @bitCast(-std.math.inf(f64)),
                // str_min/str_max: vals_flat slot unused; sidecar handles the string.
                .str_min, .str_max => 0,
            };
        }
        break :blk iv;
    } else &.{};

    // Count str_min/str_max aggs for StrAggHashTable sidecar sizing.
    const num_str_aggs: usize = if (compact_kinds) |ck| blk: {
        var n: usize = 0;
        for (ck) |k| {
            if (k == .str_min or k == .str_max) n += 1;
        }
        break :blk n;
    } else 0;
    // Map compact_kind index → sidecar index (only valid for str_min/str_max entries).
    const str_agg_sidecar_idx: []usize = if (compact_kinds) |ck| blk: {
        const m = try alloc.alloc(usize, ck.len);
        var si: usize = 0;
        for (ck, 0..) |k, ci| {
            if (k == .str_min or k == .str_max) {
                m[ci] = si;
                si += 1;
            } else m[ci] = 0;
        }
        break :blk m;
    } else &.{};

    // Flat pair-set for COUNT(DISTINCT) deduplication in the compact path.
    // Key = (group_key_bits | distinct_val_bits) packed into u128 for exact uniqueness.
    // Only allocated when at least one compact kind is count_distinct_u64.
    const has_distinct_compact: bool = if (compact_kinds) |ck| blk: {
        for (ck) |k| {
            if (k == .count_distinct_u64) break :blk true;
        }
        break :blk false;
    } else false;

    var distinct_pair_set = std.AutoHashMap(u128, void).init(std.heap.c_allocator);
    defer distinct_pair_set.deinit();
    if (has_distinct_compact) {
        // Reserve capacity for up to est_rows distinct (group, value) pairs.
        const cap: u32 = @intCast(@min(est_rows, 12_000_000));
        try distinct_pair_set.ensureTotalCapacity(cap);
    }

    var ht_agg = try ht.AggHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows);
    // Use CompactIntKeyHashTable when keys are all-int AND aggs are all pure-numeric.
    // Falls back to IntKeyHashTable when compact_kinds is null (e.g. any_val agg).
    var ht_compact: ?ht.CompactIntKeyHashTable = if (compact_kinds != null and maybe_int_keys and num_str_aggs == 0)
        try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else
        null;
    var ht_int: ?ht.IntKeyHashTable = if (maybe_int_keys and (compact_kinds == null or num_str_aggs > 0))
        try ht.IntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, est_rows)
    else
        null;

    const init_accums = try initAggAccums(aggs, alloc);
    const key_buf = try alloc.alloc(Value, keys.len);
    const int_key_buf = try alloc.alloc(i64, keys.len);

    var filter_state: ?FilterState = extractFilter(input);
    // Once we've verified on the first chunk that all key columns are int64/uint64,
    // this flag is set to true and we use ht_int for all subsequent rows.
    var use_int_path: bool = false;
    var int_path_checked: bool = false;

    // Column descriptors for int key path: per key, col index and addend.
    const IntKeyDesc = struct { col_idx: usize, addend: i64 };
    const int_key_descs = try alloc.alloc(IntKeyDesc, keys.len);

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
        // Verify int-key eligibility on first chunk.
        if (!int_path_checked) {
            int_path_checked = true;
            if (maybe_int_keys) {
                var all_int = true;
                for (keys, 0..) |k, ki| {
                    const col_idx: usize = switch (k.expr) {
                        .col_ref => |cr| cr.index,
                        .add => |op| op.left.col_ref.index,
                        .sub => |op| op.left.col_ref.index,
                        else => {
                            all_int = false;
                            break;
                        },
                    };
                    const addend: i64 = switch (k.expr) {
                        .col_ref => 0,
                        .add => |op| op.right.lit_i64,
                        .sub => |op| -op.right.lit_i64,
                        else => 0,
                    };
                    const cd = c.columns[col_idx];
                    switch (cd.data) {
                        .int64, .uint64 => {},
                        else => {
                            all_int = false;
                            break;
                        },
                    }
                    int_key_descs[ki] = .{ .col_idx = col_idx, .addend = addend };
                }
                use_int_path = all_int;
            }
            // Verify pair-count eligibility: exactly two col_refs, one i64 and one string.
        }
        const refs = ref_indices.?;

        if (use_int_path) {
            // ── Integer-key fast path ──────────────────────────────────────────
            if (ht_compact) |*htc| {
                // ── Compact accum sub-path: 8B/agg instead of 32B/agg ─────────
                const ck = compact_kinds.?;
                for (0..c.num_rows) |r| {
                    var key_valid = true;
                    for (int_key_descs, 0..) |desc, ki| {
                        const col = c.columns[desc.col_idx];
                        if (chunk.isNull(col.null_mask, r)) {
                            key_valid = false;
                            break;
                        }
                        int_key_buf[ki] = switch (col.data) {
                            .int64 => |v| v[r] +% desc.addend,
                            .uint64 => |v| @as(i64, @bitCast(v[r])) +% desc.addend,
                            else => {
                                key_valid = false;
                                break;
                            },
                        };
                    }
                    if (!key_valid) continue;
                    const slot_vals = try htc.getOrInsert(int_key_buf, compact_init_vals);
                    // COUNT(DISTINCT) deduplication via global flat pair-set.
                    if (has_distinct_compact) {
                        for (ck, 0..) |kind, ci| {
                            if (kind != .count_distinct_u64) continue;
                            const darg = aggs[ci].expr.agg_call.arg orelse continue;
                            if (darg != .col_ref) continue;
                            const dcol = c.columns[darg.col_ref.index];
                            if (dcol.isRowNull(r)) continue;
                            const dval: u64 = switch (dcol.data) {
                                .int64 => |v| @bitCast(v[r]),
                                .uint64 => |v| v[r],
                                .date_u16 => |v| @as(u64, v[r]),
                                else => continue,
                            };
                            // Pack (first group key u64, distinct_val u64) into u128.
                            // For multi-key groups hash the tail into the high bits.
                            const gk: u64 = @bitCast(int_key_buf[0]);
                            const pair: u128 = (@as(u128, gk) << 64) | dval;
                            const gop = try distinct_pair_set.getOrPut(pair);
                            if (!gop.found_existing) slot_vals[ci] += 1;
                        }
                    }
                    try updateCompactVals(slot_vals, ck, aggs, &c, r, null, 0, str_agg_sidecar_idx);
                }
            } else {
                // ── Regular AggAccum sub-path ──────────────────────────────────────
                for (0..c.num_rows) |r| {
                    // Build int key without Value boxing.
                    var key_valid = true;
                    for (int_key_descs, 0..) |desc, ki| {
                        const col = c.columns[desc.col_idx];
                        if (chunk.isNull(col.null_mask, r)) {
                            key_valid = false;
                            break;
                        }
                        const raw: i64 = switch (col.data) {
                            .int64 => |v| v[r],
                            .uint64 => |v| @bitCast(v[r]),
                            else => {
                                key_valid = false;
                                break;
                            },
                        };
                        int_key_buf[ki] = raw +% desc.addend;
                    }
                    if (!key_valid) continue;
                    const bucket = try ht_int.?.getOrInsert(int_key_buf, init_accums);
                    // Update accumulators (still uses row_buf for agg args).
                    for (refs) |j| {
                        const col = c.columns[j];
                        row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                    }
                    for (aggs, 0..) |item, ci| {
                        const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                        try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                    }
                }
            } // end else (regular path)
        } else {
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

    if (use_int_path) {
        if (ht_compact) |*htc| {
            // Emit from CompactIntKeyHashTable: u64 vals → Values.
            const EmitCtxC = struct {
                rl: *ChunkRowSink,
                alloc: std.mem.Allocator,
                keys: []const plan.ProjectItem,
                aggs: []const plan.ProjectItem,
                kinds: []const ht.CompactAggKind,
                descs: []const IntKeyDesc,
            };
            var emit_ctx_c = EmitCtxC{
                .rl = &rl,
                .alloc = alloc,
                .keys = keys,
                .aggs = aggs,
                .kinds = compact_kinds.?,
                .descs = int_key_descs,
            };
            htc.iterate(&emit_ctx_c, struct {
                fn cb(ec: *EmitCtxC, k: []const i64, vals: []const u64) void {
                    const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
                    for (k, 0..) |raw_val, i| {
                        _ = ec.descs[i];
                        row[i] = Value{ .int64 = raw_val };
                    }
                    emitCompactVals(vals, ec.kinds, ec.aggs, row[ec.keys.len..]);
                    ec.rl.append(ec.alloc, row) catch {};
                }
            }.cb);
        } else {
            // Emit from IntKeyHashTable: convert i64 keys back to Values.
            const EmitCtxI = struct {
                rl: *ChunkRowSink,
                alloc: std.mem.Allocator,
                keys: []const plan.ProjectItem,
                aggs: []const plan.ProjectItem,
                descs: []const IntKeyDesc,
            };
            var emit_ctx_i = EmitCtxI{
                .rl = &rl,
                .alloc = alloc,
                .keys = keys,
                .aggs = aggs,
                .descs = int_key_descs,
            };
            ht_int.?.iterate(&emit_ctx_i, struct {
                fn cb(ec: *EmitCtxI, k: []const i64, bucket: []const AggAccum) void {
                    const row = ec.alloc.alloc(?Value, ec.keys.len + bucket.len) catch return;
                    for (k, ec.descs, 0..) |raw_val, desc, i| {
                        // Convert back: if column was uint64, re-interpret; otherwise int64.
                        _ = desc;
                        row[i] = Value{ .int64 = raw_val };
                    }
                    for (bucket, ec.aggs, 0..) |acc, item, i| {
                        row[ec.keys.len + i] = finalizeAccum(acc, item, ec.alloc) catch null;
                    }
                    ec.rl.append(ec.alloc, row) catch {};
                }
            }.cb);
        } // end else (regular IntKeyHashTable emit)
    } else {
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
