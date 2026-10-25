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

    // Detect Q29-style regexp_replace(col_ref, lit_str_pattern, lit_str_repl) key.
    // Cache col_idx + whether it's the URL-domain pattern to avoid per-row checks.
    const RegexpReplaceKeyDesc = struct {
        col_idx: usize,
        is_url_domain: bool, // true = Q29 fast path
    };
    var regexp_replace_key_descs: ?[]RegexpReplaceKeyDesc = null;
    check_rr: {
        if (keys.len == 0) break :check_rr;
        // Quick pre-check: first key must be a fn_call named regexp_replace.
        if (keys[0].expr != .fn_call) break :check_rr;
        const fc0 = keys[0].expr.fn_call;
        if (!(std.mem.eql(u8, fc0.name, "regexp_replace") or
            std.mem.eql(u8, fc0.name, "replaceRegexpOne"))) break :check_rr;
        // All keys must be regexp_replace(col_ref, lit_str, lit_str).
        const descs_buf = try alloc.alloc(RegexpReplaceKeyDesc, keys.len);
        for (keys, 0..) |k, ki| {
            if (k.expr != .fn_call) break :check_rr;
            const fc = k.expr.fn_call;
            if (!(std.mem.eql(u8, fc.name, "regexp_replace") or
                std.mem.eql(u8, fc.name, "replaceRegexpOne")) or
                fc.args.len < 3 or
                fc.args[0] != .col_ref or
                fc.args[1] != .lit_str or
                fc.args[2] != .lit_str)
            {
                break :check_rr;
            }
            const pattern = fc.args[1].lit_str;
            const is_url = std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*$") or
                std.mem.eql(u8, pattern, "^https?://(?:www\\.)?([^/]+)/.*");
            descs_buf[ki] = .{ .col_idx = fc.args[0].col_ref.index, .is_url_domain = is_url };
        }
        regexp_replace_key_descs = descs_buf;
    }

    // StrAggHashTable for regexp_replace single-key path (e.g. Q29).
    // Initialized here if rr_descs is single-key and aggs are compact-numeric.
    const rr_can_use_str_agg = compact_kinds != null and
        regexp_replace_key_descs != null and
        regexp_replace_key_descs.?.len == 1;

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

    // StrAggHashTable fast path: exactly one col_ref key (others may be literals) + all-compact aggs.
    // Handles Q34 (GROUP BY URL), Q35 (GROUP BY 1, URL), Q22/Q23 (GROUP BY SearchPhrase + MIN/COUNT).
    // Also triggered for regexp_replace single-key path (e.g. Q29).
    const str_agg_col_idx: ?usize = blk: {
        if (maybe_int_keys) break :blk null;
        if (compact_kinds == null) break :blk null;
        // Allow exactly one col_ref key with optional literal keys.
        var col_ref_count: usize = 0;
        var col_ref_idx: usize = 0;
        for (keys) |k| {
            switch (k.expr) {
                .col_ref => |cr| {
                    col_ref_count += 1;
                    col_ref_idx = cr.index;
                },
                .lit_i64, .lit_str => {},
                else => break :blk null,
            }
        }
        if (col_ref_count != 1) break :blk null;
        break :blk col_ref_idx;
    };
    var ht_str_agg: ?ht.StrAggHashTable = if (str_agg_col_idx != null or rr_can_use_str_agg)
        try ht.StrAggHashTable.initWithCapacity(alloc, aggs.len, num_str_aggs, est_rows)
    else
        null;
    var use_str_agg_path: bool = false;
    // Set to true when regexp_replace key path routes to ht_str_agg (e.g. Q29).
    var rr_used_str_agg: bool = false;

    // PairCountHashTable fast path: exactly two col_ref keys (one i64, one string) + count(*).
    // Handles Q17/Q18 (GROUP BY UserID, SearchPhrase) and Q19 (3 keys — not handled here).
    const maybe_pair_count = blk: {
        if (aggs.len != 1) break :blk false;
        if (aggs[0].expr != .agg_call) break :blk false;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk false;
        var col_ref_count: usize = 0;
        for (keys) |k| {
            if (k.expr == .col_ref) col_ref_count += 1 else break :blk false;
        }
        break :blk col_ref_count == 2;
    };
    var ht_pair_count: ?ht.PairCountHashTable = null;
    var pair_i64_col_idx: usize = 0;
    var pair_str_col_idx: usize = 0;
    var use_pair_count_path: bool = false;

    // TripleCountHashTable fast path: (i64_col, date_part(unit, datetime_col), string_col) + count(*).
    // Handles Q19: GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase.
    const DatePartUnit = enum { minute, hour, day };
    const TripleDesc = struct {
        n0_col: usize, // first i64 col_ref index
        dp_col: usize, // col_ref index inside date_part(...)
        dp_unit: DatePartUnit,
        str_col: usize, // string col_ref index
        // Order of keys in output row: 0=n0, 1=dp, 2=str  or some permutation.
        key_order: [3]u8, // key_order[i] = which variable fills keys[i]
    };
    const maybe_triple_count: ?TripleDesc = blk: {
        if (aggs.len != 1) break :blk null;
        if (aggs[0].expr != .agg_call) break :blk null;
        if (aggs[0].expr.agg_call.kind != .count_star) break :blk null;
        if (keys.len != 3) break :blk null;
        // Find which key is the date_part fn_call and record the others.
        var dp_idx: ?usize = null;
        var dp_col: usize = 0;
        var dp_unit: DatePartUnit = .minute;
        var col_ref_indices: [2]usize = .{ 0, 0 };
        var cri: usize = 0;
        for (keys, 0..) |k, ki| {
            switch (k.expr) {
                .col_ref => {
                    if (cri >= 2) break :blk null;
                    col_ref_indices[cri] = ki;
                    cri += 1;
                },
                .fn_call => |fc| {
                    if (dp_idx != null) break :blk null; // two fn_calls
                    if (!(std.mem.eql(u8, fc.name, "date_part") or
                        std.mem.eql(u8, fc.name, "extract"))) break :blk null;
                    if (fc.args.len < 2) break :blk null;
                    if (fc.args[0] != .lit_str) break :blk null;
                    if (fc.args[1] != .col_ref) break :blk null;
                    const unit_str = fc.args[0].lit_str;
                    dp_unit = if (std.mem.eql(u8, unit_str, "minute") or std.mem.eql(u8, unit_str, "min"))
                        .minute
                    else if (std.mem.eql(u8, unit_str, "hour"))
                        .hour
                    else if (std.mem.eql(u8, unit_str, "day") or std.mem.eql(u8, unit_str, "dayofmonth"))
                        .day
                    else
                        break :blk null;
                    dp_col = fc.args[1].col_ref.index;
                    dp_idx = ki;
                },
                else => break :blk null,
            }
        }
        if (dp_idx == null or cri != 2) break :blk null;
        const key_order: [3]u8 = blk2: {
            var order: [3]u8 = .{ 0, 0, 0 };
            for (keys, 0..) |k, ki| {
                if (ki == dp_idx.?) order[ki] = 1 // date_part → n1
                else if (k.expr == .col_ref) order[ki] = if (ki == col_ref_indices[0]) 0 else 2;
            }
            break :blk2 order;
        };
        _ = key_order;
        break :blk TripleDesc{
            .n0_col = keys[col_ref_indices[0]].expr.col_ref.index,
            .dp_col = dp_col,
            .dp_unit = dp_unit,
            .str_col = keys[col_ref_indices[1]].expr.col_ref.index,
            .key_order = blk2: {
                var order: [3]u8 = .{ 0, 0, 0 };
                for (keys, 0..) |_, ki| {
                    if (ki == dp_idx.?) order[ki] = 1 else if (ki == col_ref_indices[0]) order[ki] = 0 else order[ki] = 2;
                }
                break :blk2 order;
            },
        };
    };
    var ht_triple_count: ?ht.TripleCountHashTable = null;
    var use_triple_count_path: bool = false;
    var triple_desc: TripleDesc = if (maybe_triple_count) |d| d else .{
        .n0_col = 0,
        .dp_col = 0,
        .dp_unit = .minute,
        .str_col = 0,
        .key_order = .{ 0, 1, 2 },
    };

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
            // Verify str-agg eligibility: string col_ref key + compact aggs.
            if (str_agg_col_idx) |col_idx| {
                if (col_idx < c.columns.len and c.columns[col_idx].data == .string) {
                    use_str_agg_path = true;
                }
            }
            // Verify pair-count eligibility: exactly two col_refs, one i64 and one string.
            if (maybe_pair_count and !use_int_path) {
                const c0 = keys[0].expr.col_ref.index;
                const c1 = keys[1].expr.col_ref.index;
                if (c0 < c.columns.len and c1 < c.columns.len) {
                    const d0 = c.columns[c0].data;
                    const d1 = c.columns[c1].data;
                    const ok0_i = (d0 == .int64 or d0 == .uint64) and d1 == .string;
                    const ok1_i = (d1 == .int64 or d1 == .uint64) and d0 == .string;
                    if (ok0_i) {
                        pair_i64_col_idx = c0;
                        pair_str_col_idx = c1;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    } else if (ok1_i) {
                        pair_i64_col_idx = c1;
                        pair_str_col_idx = c0;
                        ht_pair_count = try ht.PairCountHashTable.initWithCapacity(alloc, est_rows);
                        use_pair_count_path = true;
                    }
                }
            }
            // Verify triple-count eligibility: (i64, date_part_datetime, string) + count(*).
            if (maybe_triple_count != null and !use_int_path and !use_pair_count_path) {
                const td = maybe_triple_count.?;
                if (td.n0_col < c.columns.len and td.dp_col < c.columns.len and td.str_col < c.columns.len) {
                    const n0_ok = c.columns[td.n0_col].data == .int64 or c.columns[td.n0_col].data == .uint64;
                    const dp_ok = c.columns[td.dp_col].data == .datetime64_ms or c.columns[td.dp_col].data == .int64;
                    const str_ok = c.columns[td.str_col].data == .string;
                    if (n0_ok and dp_ok and str_ok) {
                        triple_desc = td;
                        ht_triple_count = try ht.TripleCountHashTable.initWithCapacity(alloc, est_rows);
                        use_triple_count_path = true;
                    }
                }
            }
        }
        const refs = ref_indices.?;

        if (use_pair_count_path) {
            // ── (i64, string) pair count(*) fast path ─────────────────────────
            const strs = c.columns[pair_str_col_idx].data.string;
            // Handle both int64 and uint64 key columns.
            switch (c.columns[pair_i64_col_idx].data) {
                .int64 => |ints| {
                    for (0..c.num_rows) |r| try ht_pair_count.?.increment(ints[r], strs[r]);
                },
                .uint64 => |ints| {
                    for (0..c.num_rows) |r| try ht_pair_count.?.increment(@bitCast(ints[r]), strs[r]);
                },
                else => unreachable,
            }
            continue;
        }

        if (use_triple_count_path) {
            // ── (i64, date_part, string) triple count(*) fast path ────────────
            const td = triple_desc;
            const n0_col = c.columns[td.n0_col];
            const dp_col = c.columns[td.dp_col];
            const strs = c.columns[td.str_col].data.string;
            for (0..c.num_rows) |r| {
                const n0: i64 = switch (n0_col.data) {
                    .int64 => |v| v[r],
                    .uint64 => |v| @bitCast(v[r]),
                    else => unreachable,
                };
                const ms: i64 = switch (dp_col.data) {
                    .datetime64_ms => |v| v[r],
                    .int64 => |v| v[r] * 1000,
                    else => unreachable,
                };
                const secs = @divTrunc(ms, 1000);
                const n1: i64 = switch (td.dp_unit) {
                    .minute => @mod(@divTrunc(secs, 60), 60),
                    .hour => @mod(@divTrunc(secs, 3600), 24),
                    .day => blk: {
                        const days = @divTrunc(ms, 86400 * 1000);
                        // Simple day-of-month: reuse date math from kernels.
                        const d = if (days >= 0) @as(u64, @intCast(days)) else 0;
                        // Gregorian calendar: days since epoch.
                        const n: u64 = d + 719468;
                        const era: u64 = @divTrunc(n, 146097);
                        const doe: u64 = n - era * 146097;
                        const yoe: u64 = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
                        const doy: u64 = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
                        const mp: u64 = @divTrunc(5 * doy + 2, 153);
                        break :blk @intCast(doy - @divTrunc(153 * mp + 2, 5) + 1);
                    },
                };
                try ht_triple_count.?.increment(n0, n1, strs[r]);
            }
            continue;
        }

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
        } else if (use_str_agg_path) {
            // ── String-key compact agg path (e.g. Q22/Q23 GROUP BY SearchPhrase) ──
            const col_idx = str_agg_col_idx.?;
            const ck = compact_kinds.?;
            const strs = c.columns[col_idx].data.string;
            for (0..c.num_rows) |r| {
                if (c.columns[col_idx].isRowNull(r)) continue;
                const s = strs[r];
                const res = try ht_str_agg.?.getOrInsert(s, compact_init_vals);
                // COUNT(DISTINCT) deduplication for string-key path.
                if (has_distinct_compact) {
                    const str_h: u64 = ht.StrAggHashTable.hashStr(s);
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
                        const pair: u128 = (@as(u128, str_h) << 64) | dval;
                        const gop = try distinct_pair_set.getOrPut(pair);
                        if (!gop.found_existing) res.vals[ci] += 1;
                    }
                }
                try updateCompactVals(res.vals, ck, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
            }
        } else if (regexp_replace_key_descs) |rr_descs| {
            // ── regexp_replace key fast path (e.g. Q29) ───────────────────────
            // Avoids per-row pattern string comparison in evalFnCall.
            const use_rr_str_agg = ht_str_agg != null and rr_descs.len == 1;
            if (use_rr_str_agg) rr_used_str_agg = true;
            const ck = compact_kinds;
            for (0..c.num_rows) |r| {
                for (refs) |j| {
                    const col = c.columns[j];
                    row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
                }
                var key_valid = true;
                var domain_str: []const u8 = "";
                for (rr_descs, 0..) |desc, ki| {
                    const s_opt = row_buf[desc.col_idx];
                    const s = if (s_opt) |v| (v.toStr() orelse null) else null;
                    const domain: ?Value = if (s) |str| d: {
                        if (desc.is_url_domain) {
                            const after_proto = if (std.mem.startsWith(u8, str, "https://"))
                                str[8..]
                            else if (std.mem.startsWith(u8, str, "http://"))
                                str[7..]
                            else
                                break :d Value{ .string = str };
                            const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse
                                break :d Value{ .string = str };
                            var host = after_proto[0..slash];
                            if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                            break :d Value{ .string = host };
                        }
                        break :d Value{ .string = str };
                    } else null;
                    if (domain == null) {
                        key_valid = false;
                        break;
                    }
                    key_buf[ki] = domain.?;
                    if (ki == 0) domain_str = domain.?.string;
                }
                if (!key_valid) continue;
                if (use_rr_str_agg) {
                    const res = try ht_str_agg.?.getOrInsert(domain_str, compact_init_vals);
                    try updateCompactVals(res.vals, ck.?, aggs, &c, r, &ht_str_agg.?, res.slot, str_agg_sidecar_idx);
                } else {
                    const bucket = try ht_agg.getOrInsert(key_buf, init_accums);
                    for (aggs, 0..) |item, ci| {
                        const v_opt = try evalAggArg(item.expr, row_buf, alloc);
                        try kernels.updateAccum(&bucket[ci], v_opt, alloc);
                    }
                }
            }
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

    if (use_pair_count_path) {
        // Emit from PairCountHashTable: restore key order (i64, str or str, i64).
        const k0_is_i64 = keys[0].expr.col_ref.index == pair_i64_col_idx;
        const EmitCtxP = struct {
            rl: *ChunkRowSink,
            alloc: std.mem.Allocator,
            k0_is_i64: bool,
        };
        var emit_ctx_p = EmitCtxP{ .rl = &rl, .alloc = alloc, .k0_is_i64 = k0_is_i64 };
        ht_pair_count.?.iterate(&emit_ctx_p, struct {
            fn cb(ec: *EmitCtxP, n: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 3) catch return;
                if (ec.k0_is_i64) {
                    row[0] = Value{ .int64 = n };
                    row[1] = Value{ .string = s };
                } else {
                    row[0] = Value{ .string = s };
                    row[1] = Value{ .int64 = n };
                }
                row[2] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_triple_count_path) {
        // Emit from TripleCountHashTable: restore key order per triple_desc.key_order.
        const td = triple_desc;
        const EmitCtxT = struct {
            rl: *ChunkRowSink,
            alloc: std.mem.Allocator,
            key_order: [3]u8,
        };
        var emit_ctx_t = EmitCtxT{ .rl = &rl, .alloc = alloc, .key_order = td.key_order };
        ht_triple_count.?.iterate(&emit_ctx_t, struct {
            fn cb(ec: *EmitCtxT, n0: i64, n1: i64, s: []const u8, count: u64) void {
                const row = ec.alloc.alloc(?Value, 4) catch return;
                for (ec.key_order, 0..) |kind, i| {
                    row[i] = switch (kind) {
                        0 => Value{ .int64 = n0 },
                        1 => Value{ .int64 = n1 },
                        else => Value{ .string = s },
                    };
                }
                row[3] = Value{ .uint64 = count };
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_str_agg_path or rr_used_str_agg) {
        // Emit from StrAggHashTable: string key + compact aggs → Values.
        // Handles literal keys alongside the col_ref key (e.g. Q35: GROUP BY 1, URL).
        // Also used when regexp_replace key path routed to ht_str_agg (Q29).
        const EmitCtxSA = struct {
            rl: *ChunkRowSink,
            alloc: std.mem.Allocator,
            aggs: []const plan.ProjectItem,
            keys: []const plan.ProjectItem,
            kinds: []const ht.CompactAggKind,
            str_ht: *ht.StrAggHashTable,
            sidecar_idx: []const usize,
        };
        var emit_ctx_sa = EmitCtxSA{
            .rl = &rl,
            .alloc = alloc,
            .aggs = aggs,
            .keys = keys,
            .kinds = compact_kinds.?,
            .str_ht = &ht_str_agg.?,
            .sidecar_idx = str_agg_sidecar_idx,
        };
        ht_str_agg.?.iterateWithSlot(&emit_ctx_sa, struct {
            fn cb(ec: *EmitCtxSA, s: []const u8, vals: []const u64, slot: usize) void {
                const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
                for (ec.keys, 0..) |k, i| {
                    row[i] = switch (k.expr) {
                        // col_ref and fn_call (e.g. regexp_replace) both map to the string key s.
                        .col_ref, .fn_call => Value{ .string = s },
                        .lit_i64 => |v| Value{ .int64 = v },
                        .lit_str => |v| Value{ .string = v },
                        else => Value{ .string = s },
                    };
                }
                emitCompactValsWithSidecar(vals, ec.kinds, ec.aggs, row[ec.keys.len..], ec.str_ht, slot, ec.sidecar_idx);
                ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    } else if (use_int_path) {
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

pub fn executeBoundedCountTopDistinct(
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    compact_kinds: []const ht.CompactAggKind,
    filter_pred: ?plan.Expr,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (filter_pred != null or keys.len != 1 or top_k == 0 or top_k > 255) return null;
    if (keys[0].expr != .col_ref or sort_keys.len == 0 or !sort_keys[0].desc) return null;

    const sort_col = sort_keys[0].col_idx;
    if (sort_col < keys.len or sort_col >= keys.len + aggs.len) return null;
    const sort_ai = sort_col - keys.len;
    const sort_kind = compact_kinds[sort_ai];
    if (sort_kind != .count) return null;

    var distinct_ai_opt: ?usize = null;
    var distinct_col_idx: usize = 0;
    for (aggs, compact_kinds, 0..) |ag, kind, ai| {
        switch (kind) {
            .count, .i64_sum, .f64_sum => {},
            .count_distinct_u64 => {
                if (distinct_ai_opt != null or ag.expr != .agg_call) return null;
                const arg = ag.expr.agg_call.arg orelse return null;
                if (arg != .col_ref) return null;
                distinct_ai_opt = ai;
                distinct_col_idx = arg.col_ref.index;
            },
            else => return null,
        }
    }
    const distinct_ai = distinct_ai_opt orelse return null;

    const alloc = ctx.allocator();
    const sm = ctx.source.schema();
    const key_idx = keys[0].expr.col_ref.index;
    const key_raw = RawColSlice.resolve(ctx.source, sm, key_idx) orelse return null;
    const distinct_raw = RawColSlice.resolve(ctx.source, sm, distinct_col_idx) orelse return null;
    const total_rows = std.math.cast(usize, ctx.source.rowCount()) orelse return null;

    var agg_raw: [16]RawColSlice = undefined;
    if (aggs.len > agg_raw.len) return null;
    for (aggs, compact_kinds, 0..) |ag, kind, ai| {
        switch (kind) {
            .count, .count_distinct_u64 => {},
            .i64_sum, .f64_sum => {
                if (ag.expr != .agg_call) return null;
                const arg = ag.expr.agg_call.arg orelse return null;
                if (arg != .col_ref) return null;
                agg_raw[ai] = RawColSlice.resolve(ctx.source, sm, arg.col_ref.index) orelse return null;
            },
            else => return null,
        }
    }

    var min_key: i64 = std.math.maxInt(i64);
    var max_key: i64 = std.math.minInt(i64);
    for (0..total_rows) |row| {
        const key = key_raw.getI64(row);
        min_key = @min(min_key, key);
        max_key = @max(max_key, key);
    }
    if (min_key > max_key) return null;
    const key_span_i = max_key -% min_key + 1;
    const key_span = std.math.cast(usize, key_span_i) orelse return null;
    if (key_span == 0 or key_span > 262_144) return null;

    const acc_i64 = try alloc.alloc(i64, aggs.len * key_span);
    const acc_f64 = try alloc.alloc(f64, aggs.len * key_span);
    const group_counts = try alloc.alloc(i64, key_span);
    @memset(acc_i64, 0);
    @memset(acc_f64, 0.0);
    @memset(group_counts, 0);

    for (0..total_rows) |row| {
        const key = key_raw.getI64(row);
        const idx: usize = @intCast(key - min_key);
        group_counts[idx] += 1;
        for (compact_kinds, 0..) |kind, ai| {
            const off = ai * key_span + idx;
            switch (kind) {
                .count => acc_i64[off] += 1,
                .i64_sum => acc_i64[off] +%= agg_raw[ai].getI64(row),
                .f64_sum => acc_f64[off] += @floatFromInt(agg_raw[ai].getI64(row)),
                .count_distinct_u64 => {},
                else => {},
            }
        }
    }

    const Candidate = struct { idx: usize, count: i64 };
    var candidates_buf: [255]Candidate = undefined;
    var cand_len: usize = 0;
    for (0..key_span) |idx| {
        const cnt = group_counts[idx];
        if (cnt == 0) continue;
        if (cand_len < top_k) {
            candidates_buf[cand_len] = .{ .idx = idx, .count = cnt };
            cand_len += 1;
        } else {
            var worst_i: usize = 0;
            for (candidates_buf[0..top_k], 0..) |c, i| {
                if (c.count < candidates_buf[worst_i].count) worst_i = i;
            }
            if (cnt > candidates_buf[worst_i].count) {
                candidates_buf[worst_i] = .{ .idx = idx, .count = cnt };
            }
        }
    }
    if (cand_len == 0) return null;

    const cand_map = try alloc.alloc(u8, key_span);
    @memset(cand_map, 255);
    for (candidates_buf[0..cand_len], 0..) |c, i| cand_map[c.idx] = @intCast(i);

    var distinct_sets = try alloc.alloc(hashmap.DistinctEpochSet, cand_len);
    for (distinct_sets) |*set| set.* = try hashmap.DistinctEpochSet.init(alloc, 1024);
    defer for (distinct_sets) |*set| set.deinit();

    for (0..total_rows) |row| {
        const idx: usize = @intCast(key_raw.getI64(row) - min_key);
        const ci = cand_map[idx];
        if (ci == 255) continue;
        const set = &distinct_sets[ci];
        if (set.needsGrow()) try set.growDouble();
        _ = set.insertNew(@bitCast(distinct_raw.getI64(row)));
    }
    for (candidates_buf[0..cand_len], 0..) |c, ci| {
        acc_i64[distinct_ai * key_span + c.idx] = @intCast(distinct_sets[ci].len);
    }

    const out_metas = try buildAggResultMetas(keys, aggs, alloc);
    var rl = RowBuffer.init(out_metas);
    for (candidates_buf[0..cand_len]) |c| {
        const row = try alloc.alloc(?Value, 1 + aggs.len);
        row[0] = .{ .int64 = min_key + @as(i64, @intCast(c.idx)) };
        for (compact_kinds, 0..) |kind, ai| {
            const off = ai * key_span + c.idx;
            row[1 + ai] = switch (kind) {
                .count, .i64_sum => .{ .int64 = acc_i64[off] },
                .f64_sum => blk: {
                    const sum = acc_f64[off];
                    if (aggs[ai].expr == .agg_call and aggs[ai].expr.agg_call.kind == .avg) {
                        const cnt = group_counts[c.idx];
                        if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                    }
                    break :blk Value{ .float64 = sum };
                },
                .count_distinct_u64 => .{ .uint64 = @intCast(acc_i64[off]) },
                else => .{ .int64 = 0 },
            };
        }
        try rl.append(alloc, row);
    }
    return try executeTopK(rl, sort_keys, top_k, alloc);
}

pub fn executeHashAggParallelCompactTopK(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    top_offset: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 500_000) return null; // not worth parallelizing small inputs

    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;

    const alloc = ctx.allocator();

    // Check: all keys must be int col_ref, lit_i64 constant, col ± lit_i64, or date_trunc fn_call.
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => {},
            .lit_i64 => {}, // constant key (e.g. GROUP BY 1)
            .add => |op| {
                if (op.left != .col_ref or op.right != .lit_i64) return null;
            },
            .sub => |op| {
                if (op.left != .col_ref or op.right != .lit_i64) return null;
            },
            .fn_call => |fc| {
                // Allow toStartOfMinute/toStartOfHour/toStartOfDay(col_ref) — date_trunc variants.
                const ok = (std.mem.eql(u8, fc.name, "toStartOfMinute") or
                    std.mem.eql(u8, fc.name, "toStartOfHour") or
                    std.mem.eql(u8, fc.name, "toStartOfDay")) and
                    fc.args.len == 1 and fc.args[0] == .col_ref;
                if (!ok) return null;
            },
            else => return null,
        }
    }

    // Check: all key columns must be integer (not string) type.
    // String keys must be handled by executeHashAggParallelStrKey instead.
    {
        const sm = ctx.source.schema();
        for (keys) |k| {
            const col_idx: usize = switch (k.expr) {
                .col_ref => |cr| cr.index,
                .add => |op| op.left.col_ref.index,
                .sub => |op| op.left.col_ref.index,
                .fn_call => |fc| fc.args[0].col_ref.index,
                else => continue,
            };
            if (col_idx < sm.len) {
                switch (sm[col_idx].col_type) {
                    .string, .array_string => return null,
                    else => {},
                }
            }
        }
    }

    // Check: all aggs must be compact (no str_min/str_max).
    const compact_kinds = try alloc.alloc(ht.CompactAggKind, aggs.len);
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) return null;
        compact_kinds[ci] = switch (item.expr.agg_call.kind) {
            .count_star => .count,
            .count => if (item.expr.agg_call.distinct) blk: {
                const arg = item.expr.agg_call.arg orelse return null;
                if (arg != .col_ref) return null;
                break :blk .count_distinct_u64;
            } else .count,
            .sum => if (item.out_type == .float64) .f64_sum else .i64_sum,
            .avg => blk_avg: {
                const avg_arg = item.expr.agg_call.arg orelse return null;
                if (avg_arg == .col_ref) break :blk_avg .f64_sum;
                // avg(length(str_col)) — accumulate string length sum; finalize as avg.
                if (avg_arg == .fn_call and
                    std.mem.eql(u8, avg_arg.fn_call.name, "length") and
                    avg_arg.fn_call.args.len == 1 and
                    avg_arg.fn_call.args[0] == .col_ref)
                    break :blk_avg .f64_str_len_sum;
                return null;
            },
            .min => if (item.out_type == .string) return null else .i64_min,
            .max => if (item.out_type == .string) return null else .i64_max,
            else => return null,
        };
    }

    // Extract filter predicate from input node.
    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter => |f| f.predicate,
        .project => |p| switch (p.input.*) {
            .filter => |f| f.predicate,
            else => null,
        },
        else => null,
    };

    // No-filter path now supports COUNT, i64_sum, f64_sum, and count_distinct_u64.
    // Only fall back for unusual agg kinds (i64_min/max, u64_*) without filter.
    if (filter_pred == null) {
        for (compact_kinds) |kind| {
            switch (kind) {
                .count, .i64_sum, .f64_sum, .count_distinct_u64 => {},
                else => return null,
            }
        }
    }

    var scan_request: ScanRequest = .{};
    scan_request.addItems(keys);
    scan_request.addItems(aggs);
    if (filter_pred) |fp| scan_request.addExpr(fp);
    const scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    // If the filter is a simple `col_ref != ''` (string non-empty check), tell the
    // source to decode that column as bool_u8 instead of fat string pointers.
    // This reduces write bandwidth by ~16× for the filter column (1B vs 16B per row).
    const str_nonempty_col_name: ?[]const u8 = if (filter_pred) |fp| blk: {
        if (fp == .neq) {
            const op = fp.neq;
            if (op.left == .col_ref and op.right == .lit_str and op.right.lit_str.len == 0) {
                const ci = op.left.col_ref.index;
                const sm2 = ctx.source.schema();
                if (ci < sm2.len) break :blk sm2[ci].name;
            }
        }
        break :blk null;
    } else null;
    if (str_nonempty_col_name) |col_name| {
        // Guard: do NOT optimize filter column as bool_u8 if it is also used for
        // f64_str_len_sum (which needs actual string lengths, not bool flags).
        var conflict = false;
        const sm3 = ctx.source.schema();
        for (aggs, compact_kinds) |ag, ck| {
            if (ck != .f64_str_len_sum) continue;
            const ac2 = ag.expr.agg_call;
            if (ac2.arg) |arg| {
                if (arg == .fn_call and arg.fn_call.args.len == 1 and
                    arg.fn_call.args[0] == .col_ref)
                {
                    const ci2 = arg.fn_call.args[0].col_ref.index;
                    if (ci2 < sm3.len and std.mem.eql(u8, sm3[ci2].name, col_name)) {
                        conflict = true;
                        break;
                    }
                }
            }
        }
        if (!conflict) ctx.source.setStringNonEmptyBool(col_name);
    }
    defer ctx.source.setStringNonEmptyBool(null);

    // Load columns before parallel scan.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    const compact_init_vals = try alloc.alloc(u64, aggs.len);
    for (compact_kinds, 0..) |kind, i| {
        compact_init_vals[i] = switch (kind) {
            .count, .i64_sum, .u64_sum, .f64_sum, .f64_str_len_sum, .count_distinct_u64 => 0,
            .i64_min => @as(u64, @bitCast(@as(i64, std.math.maxInt(i64)))),
            .i64_max => @as(u64, @bitCast(@as(i64, std.math.minInt(i64)))),
            .u64_min => std.math.maxInt(u64),
            .u64_max => 0,
            .f64_min => @bitCast(std.math.inf(f64)),
            .f64_max => @bitCast(-std.math.inf(f64)),
            .str_min, .str_max => 0,
        };
    }

    if (try executeBoundedCountTopDistinct(keys, aggs, compact_kinds, filter_pred, sort_keys, top_k, ctx)) |rl| return rl;

    // ── Two-phase partitioned aggregation (scatter + small-HT aggregate) ──────────
    // For large plain-col-ref-key queries (Q33, Q32), the per-thread HT
    // exceeds L3, causing ~15ms of DRAM stalls per scan.  Two-phase avoids this:
    // Phase1: scatter rows to 64 partition buffers (no HT → no random misses).
    // Phase2: aggregate each partition with a small (~15K entries) L2-fitting HT.
    // Also supports queries with a single col_ref eq/neq lit_str filter (e.g. Q32:
    // SearchPhrase <> '') — the filter is inlined in the scatter phase with no
    // per-row arena allocation.
    two_phase: {
        // Guard: 1–4 keys, each either plain col_ref or col_ref ± lit_i64.
        // 4-key queries (Q36) are included because ClientIP arithmetic has 1 true key,
        // so the cardinality is similar to 1-key and the 2-phase benefit outweighs scatter overhead.
        // 1-key non-COUNT(DISTINCT) queries (Q16: GROUP BY UserID) are now included:
        // UserID has ~4M distinct values → HT doesn't fit in L3; 2-phase gives cache-friendly local HTs.
        if (keys.len == 0 or keys.len > 4) break :two_phase;
        for (keys) |k| {
            switch (k.expr) {
                .col_ref => {},
                .sub => |op| {
                    if (op.left != .col_ref or op.right != .lit_i64) break :two_phase;
                },
                .add => |op| {
                    if (op.left != .col_ref or op.right != .lit_i64) break :two_phase;
                },
                else => break :two_phase,
            }
        }
        // Guard: skip two-phase when any agg is f64_str_len_sum (AVG(length(col))).
        // For these queries the scatter phase needs to load full string data, making
        // scatter write overhead exceed the DRAM-stall benefit for low-cardinality keys.
        // The parallel compact path handles f64_str_len_sum more efficiently.
        for (compact_kinds) |ck| {
            if (ck == .f64_str_len_sum) break :two_phase;
        }

        // Guard: skip two-phase for low-cardinality keys (int16/bool etc.) — their HTs
        // are L1-fitting already; scatter overhead exceeds the DRAM-stall benefit.
        {
            const src_schema = ctx.source.schema();
            for (keys) |k| {
                const ci: usize = switch (k.expr) {
                    .col_ref => |cr| cr.index,
                    .sub => |op| op.left.col_ref.index,
                    .add => |op| op.left.col_ref.index,
                    else => ~@as(usize, 0), // non-column expr — no narrow check
                };
                if (ci < src_schema.len and src_schema[ci].is_narrow_int) break :two_phase;
            }
        }
        // If there's a filter, try:
        //   (a) simple single-term col_ref eq/neq lit_str (e.g. Q32: SearchPhrase <> '')
        //   (b) pure int AND conditions (e.g. Q41: CounterID=62 AND TraficSourceID IN (-1,6) AND ...)
        // Fall back to regular parallel compact if neither applies.
        var str_filt: ?SimpleStrFilter = null;
        var int_filt: ?[]const IntCmpCond = null;
        if (filter_pred) |fp| {
            if (tryExtractSimpleStrFilter(fp)) |sf| {
                str_filt = sf;
            } else {
                var ic_buf: [16]IntCmpCond = undefined;
                var ic_n: usize = 0;
                const ic_complete = extractAndIntConds(fp, &ic_buf, &ic_n, false);
                if (ic_complete and ic_n > 0) {
                    int_filt = try alloc.dupe(IntCmpCond, ic_buf[0..ic_n]);
                } else {
                    break :two_phase;
                }
            }
        }
        if (try executeTwoPhaseHashAgg(
            keys,
            aggs,
            compact_kinds,
            compact_init_vals,
            total_rows,
            n_threads,
            alloc,
            ctx,
            sort_keys,
            top_k,
            str_filt,
            int_filt,
        )) |two_phase_rl| return two_phase_rl;
    }

    // ── Raw int16 histogram GROUP BY fast path ─────────────────────────────────
    // For COUNT(*) GROUP BY single narrow-int key with a filter only on that key.
    // e.g. Q8: SELECT AdvEngineID, COUNT(*) WHERE AdvEngineID <> 0 GROUP BY AdvEngineID
    // Builds a 65536-entry direct array (no hash, no probing) → L1-resident for low-cardinality.
    raw_i16_hist_blk: {
        if (keys.len != 1) break :raw_i16_hist_blk;
        for (compact_kinds) |k| {
            if (k != .count) break :raw_i16_hist_blk;
        }
        const hist_key_expr = keys[0].expr;
        if (hist_key_expr != .col_ref) break :raw_i16_hist_blk;
        const hist_ci = hist_key_expr.col_ref.index;
        const hist_sm = ctx.source.schema();
        if (hist_ci >= hist_sm.len or !hist_sm[hist_ci].is_narrow_int) break :raw_i16_hist_blk;
        const hist_col = hist_sm[hist_ci].name;
        const hist_raw = ctx.source.getRawInt16Col(hist_col) orelse break :raw_i16_hist_blk;
        if (hist_raw.len < total_rows) break :raw_i16_hist_blk;
        // Filter: must be pure AND int conditions all on the key column.
        var hist_ic_buf: [8]IntCmpCond = undefined;
        var hist_ic_n: usize = 0;
        if (filter_pred) |fp| {
            if (!extractAndIntConds(fp, &hist_ic_buf, &hist_ic_n, false)) break :raw_i16_hist_blk;
            if (hist_ic_n == 0) break :raw_i16_hist_blk;
            for (hist_ic_buf[0..hist_ic_n]) |fc| {
                if (fc.col_idx != hist_ci) break :raw_i16_hist_blk;
            }
        }
        // Build per-thread direct histograms (65536 × u64 = 512KB each).
        // For low-cardinality columns (e.g. AdvEngineID 0-255) only ~2KB are accessed → L1-resident.
        const HIST_SIZE: usize = 65536;
        const hist_per_thread = try alloc.alloc([]u64, n_threads);
        for (hist_per_thread) |*h| {
            h.* = try alloc.alloc(u64, HIST_SIZE);
            @memset(h.*, 0);
        }
        const HistBuildCtx = struct {
            raw: []const i16,
            hist: []u64,
            morsel_src: *parallel.MorselSource,
            // Inline filter conditions: applied per-element to skip non-qualifying rows
            // before updating the histogram.  For Q8 (AdvEngineID <> 0) this eliminates
            // ~90% of histogram writes (zeros), cutting wall time 4.7ms → ~1.5ms.
            conds: []const IntCmpCond,
            fn work(self: *@This(), _: *parallel.MorselSource) void {
                const conds = self.conds;
                while (self.morsel_src.next()) |m| {
                    if (conds.len == 0) {
                        // No filter: unconditional scatter-add (original fast path).
                        for (self.raw[m.start..m.end]) |v| {
                            self.hist[@as(usize, @as(u16, @bitCast(v)))] += 1;
                        }
                    } else {
                        // Filter-gated scatter-add: skip values that don't satisfy all
                        // conditions.  Branch predictor correctly predicts "skip" for
                        // majority of rows (e.g. AdvEngineID=0), eliminating most stores.
                        for (self.raw[m.start..m.end]) |v| {
                            const iv: i64 = v;
                            var pass = true;
                            for (conds) |cond| {
                                if (!switch (cond.op) {
                                    .eq => iv == cond.val,
                                    .neq => iv != cond.val,
                                    .lt => iv < cond.val,
                                    .lte => iv <= cond.val,
                                    .gt => iv > cond.val,
                                    .gte => iv >= cond.val,
                                    .in2 => iv == cond.val or iv == cond.val2,
                                }) {
                                    pass = false;
                                    break;
                                }
                            }
                            if (pass) self.hist[@as(usize, @as(u16, @bitCast(v)))] += 1;
                        }
                    }
                }
            }
        };
        var hist_morsels = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
        const hist_pctxs = try alloc.alloc(HistBuildCtx, n_threads);
        for (hist_pctxs, 0..) |*pc, ti| {
            pc.* = .{ .raw = hist_raw, .hist = hist_per_thread[ti], .morsel_src = &hist_morsels, .conds = hist_ic_buf[0..hist_ic_n] };
        }
        try parallel.parallelFor(alloc, HistBuildCtx, HistBuildCtx.work, hist_pctxs, &hist_morsels);
        // Merge thread histograms into hist_per_thread[0].
        const final_hist = hist_per_thread[0];
        for (hist_per_thread[1..]) |h| {
            for (0..HIST_SIZE) |i| {
                final_hist[i] += h[i];
            }
        }
        // Apply filter: zero entries that don't pass all conditions.
        if (filter_pred != null) {
            const fcs = hist_ic_buf[0..hist_ic_n];
            for (0..HIST_SIZE) |ui| {
                if (final_hist[ui] == 0) continue;
                const v: i64 = @as(i64, @as(i16, @bitCast(@as(u16, @intCast(ui)))));
                var pass = true;
                for (fcs) |fc| {
                    if (!pass) break;
                    pass = switch (fc.op) {
                        .eq => v == fc.val,
                        .neq => v != fc.val,
                        .lt => v < fc.val,
                        .lte => v <= fc.val,
                        .gt => v > fc.val,
                        .gte => v >= fc.val,
                        .in2 => v == fc.val or v == fc.val2,
                    };
                }
                if (!pass) final_hist[ui] = 0;
            }
        }
        // Build RowBuffer from non-zero histogram entries.
        const hist_out_metas = try buildAggResultMetas(keys, aggs, alloc);
        var hist_rl = RowBuffer.init(hist_out_metas);
        for (0..HIST_SIZE) |ui| {
            if (final_hist[ui] == 0) continue;
            const kv: i64 = @as(i64, @as(i16, @bitCast(@as(u16, @intCast(ui)))));
            const row = try alloc.alloc(?Value, keys.len + aggs.len);
            row[0] = .{ .int64 = kv };
            for (0..aggs.len) |ai| {
                row[keys.len + ai] = .{ .int64 = @intCast(final_hist[ui]) };
            }
            try hist_rl.append(alloc, row);
        }
        // Handle top_k with sorting when called from the top_k fusion path.
        if (sort_keys.len > 0 and top_k > 0) {
            const sorted = try executeOrderBy(hist_rl, sort_keys, alloc);
            const take = @min(sorted.rows.items.len, top_k);
            var trimmed_rl = RowBuffer.init(hist_out_metas);
            for (sorted.rows.items[0..take]) |row| try trimmed_rl.append(alloc, row);
            return trimmed_rl;
        }
        return hist_rl;
    }

    // COUNT(DISTINCT) is only supported via the two-phase path above.
    // If we reach here with count_distinct_u64, fall back to sequential.
    for (compact_kinds) |k| {
        if (k == .count_distinct_u64) return null;
    }

    const ParHashCtx = struct {
        source: SourceIface,
        filter_pred: ?plan.Expr,
        keys: []const plan.ProjectItem,
        aggs: []const plan.ProjectItem,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        local_ht: ht.CompactIntKeyHashTable,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();
            const key_buf = try talloc.alloc(i64, self.keys.len);

            // Extract filter conditions once before the morsel loop.
            // Hoisting avoids redundant Expr-tree traversal on every morsel.
            var pre_ic_buf: [16]IntCmpCond = undefined;
            var pre_ic_n: usize = 0;
            var pre_ic_complete: bool = false;
            if (self.filter_pred) |fp| {
                pre_ic_complete = extractAndIntConds(fp, &pre_ic_buf, &pre_ic_n, false);
            }
            const pre_conds = pre_ic_buf[0..pre_ic_n];

            // Allocate SIMD scratch buffers once per thread for the SIMD filter path.
            // Using i16 masks: 0 = filtered out, non-zero = pass.
            // Allocated when ANY int conditions were extracted (complete OR partial):
            // partial int conditions use SIMD pre-filter + evalExpr residual check.
            const simd_sz = parallel.default_morsel_size + 1;
            var simd_mask_buf: ?[]i16 = null;
            var simd_tmp_a_buf: ?[]i16 = null;
            var simd_tmp_b_buf: ?[]i16 = null;
            if (pre_ic_n > 0) {
                simd_mask_buf = try talloc.alloc(i16, simd_sz);
                simd_tmp_a_buf = try talloc.alloc(i16, simd_sz);
                simd_tmp_b_buf = try talloc.alloc(i16, simd_sz);
            }

            // ── Raw-slice fast path ───────────────────────────────────────────────
            // When all filter conditions are pure int AND all GROUP BY keys are
            // plain col_ref AND all aggregates are COUNT: bypass fetchRange entirely.
            // Reads narrow mmap'd columns directly, eliminating i16/i32→i64 widening
            // (~3-5× memory bandwidth savings).  For Q42 (5 filter + 2 key cols in
            // narrow int): saves ~340MB per query → expected speedup 12ms → ~2-3ms.
            raw_path: {
                if (!pre_ic_complete or pre_ic_n == 0) break :raw_path;
                for (self.keys) |k| {
                    if (k.expr != .col_ref) break :raw_path;
                }
                for (self.compact_kinds) |k| {
                    if (k != .count) break :raw_path;
                }
                const sch = self.source.schema();
                // Resolve each filter condition column to a raw typed slice.
                var rf: [16]RawColSlice = undefined;
                for (pre_conds, 0..) |cond, ci| {
                    const nm = if (cond.col_idx < sch.len) sch[cond.col_idx].name else break :raw_path;
                    if (self.source.getRawInt16Col(nm)) |s| {
                        rf[ci] = .{ .i16s = s };
                    } else if (self.source.getRawInt32Col(nm)) |s| {
                        rf[ci] = .{ .i32s = s };
                    } else if (self.source.getRawInt64Col(nm)) |s| {
                        rf[ci] = .{ .i64s = s };
                    } else break :raw_path;
                }
                // Resolve each GROUP BY key column to a raw typed slice.
                var rk: [8]RawColSlice = undefined;
                for (self.keys, 0..) |k, ki| {
                    const col_idx = k.expr.col_ref.index;
                    const nm = if (col_idx < sch.len) sch[col_idx].name else break :raw_path;
                    if (self.source.getRawInt16Col(nm)) |s| {
                        rk[ki] = .{ .i16s = s };
                    } else if (self.source.getRawInt32Col(nm)) |s| {
                        rk[ki] = .{ .i32s = s };
                    } else if (self.source.getRawInt64Col(nm)) |s| {
                        rk[ki] = .{ .i64s = s };
                    } else break :raw_path;
                }
                // All columns resolved — run morsel loop without fetchRange.
                const simd_mask = simd_mask_buf.?;
                const simd_tmp_a = simd_tmp_a_buf.?;
                const simd_tmp_b = simd_tmp_b_buf.?;

                // ── Condition reordering: sort rf[] + pre_ic_buf[] by selectivity ─────
                // i64 eq (e.g. URLHash exact-match) → i32 eq → i16 eq → range ops.
                // For Q42: URLHash has ~100 matches / 10M rows.  Moving it to index 0
                // means >99% of morsels are all-zero after the first SIMD pass and skip
                // the remaining 5 conditions entirely via the early-exit below.
                {
                    const condPri = struct {
                        fn get(sl: RawColSlice, c: IntCmpCond) u8 {
                            return switch (sl) {
                                .i64s => if (c.op == .eq) 0 else 3,
                                .i32s => if (c.op == .eq) 1 else 4,
                                .i16s => if (c.op == .eq) 2 else 5,
                            };
                        }
                    };
                    // Insertion sort (≤16 elements; virtually free at runtime).
                    var si: usize = 1;
                    while (si < pre_ic_n) : (si += 1) {
                        const pi = condPri.get(rf[si], pre_ic_buf[si]);
                        var sj = si;
                        while (sj > 0 and condPri.get(rf[sj - 1], pre_ic_buf[sj - 1]) > pi) : (sj -= 1) {
                            std.mem.swap(RawColSlice, &rf[sj - 1], &rf[sj]);
                            std.mem.swap(IntCmpCond, &pre_ic_buf[sj - 1], &pre_ic_buf[sj]);
                        }
                    }
                }

                const CHUNK = 32;
                while (self.morsel_src.next()) |m| {
                    const start = m.start;
                    const nr = m.end - m.start;
                    @memset(simd_mask[0..nr], 1);

                    // Apply highest-selectivity condition first (after reordering above).
                    rf[0].applyMaskSIMD(start, nr, pre_conds[0], simd_mask[0..nr], simd_tmp_a[0..nr], simd_tmp_b[0..nr]);

                    // ── Morsel-wide early-exit ──────────────────────────────────────
                    // After the first (most selective) condition, OR-reduce the full
                    // mask.  For Q42 with URLHash at position 0: ~99% of morsels have
                    // mask = all-zero and we skip 5 more SIMD passes + the row scan,
                    // saving ~4 ms per query.
                    if (pre_ic_n > 1) {
                        var any_pass = false;
                        var rr: usize = 0;
                        while (rr + CHUNK <= nr) : (rr += CHUNK) {
                            const v: @Vector(CHUNK, i16) = simd_mask[rr..][0..CHUNK].*;
                            if (@reduce(.Or, v) != 0) {
                                any_pass = true;
                                break;
                            }
                        }
                        if (!any_pass) {
                            while (rr < nr) : (rr += 1) {
                                if (simd_mask[rr] != 0) {
                                    any_pass = true;
                                    break;
                                }
                            }
                        }
                        if (!any_pass) continue; // entire morsel filtered → next morsel
                    }

                    // Apply remaining conditions (indices 1 .. pre_ic_n-1).
                    for (pre_conds[1..], 0..) |cond, rci| {
                        rf[1 + rci].applyMaskSIMD(start, nr, cond, simd_mask[0..nr], simd_tmp_a[0..nr], simd_tmp_b[0..nr]);
                    }

                    // ── Chunked mask scan: OR-reduce 32-lane chunks so all-zero
                    // blocks are skipped in ~5 cycles instead of 32 scalar checks.
                    // With ~1% pass rate, ~76% of 32-row chunks are all zero.
                    var r: usize = 0;
                    while (r + CHUNK <= nr) : (r += CHUNK) {
                        const v: @Vector(CHUNK, i16) = simd_mask[r..][0..CHUNK].*;
                        if (@reduce(.Or, v) == 0) continue;
                        for (r..r + CHUNK) |ri| {
                            if (simd_mask[ri] == 0) continue;
                            const row = start + ri;
                            for (0..self.keys.len) |ki| {
                                key_buf[ki] = rk[ki].getI64(row);
                            }
                            const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                            for (0..self.aggs.len) |ci2| {
                                slot_vals[ci2] += 1;
                            }
                        }
                    }
                    while (r < nr) : (r += 1) {
                        if (simd_mask[r] == 0) continue;
                        const row = start + r;
                        for (0..self.keys.len) |ki| {
                            key_buf[ki] = rk[ki].getI64(row);
                        }
                        const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                        for (0..self.aggs.len) |ci2| {
                            slot_vals[ci2] += 1;
                        }
                    }
                }
                return; // Raw path handled — skip fetchRange morsel loop.
            }

            // Pre-allocate a single reusable arena for per-morsel fetchRange data.
            // Resetting with retain_capacity reuses the existing buffer each iteration
            // instead of allocating ~7MB from talloc per morsel (which accumulates to
            // ~566MB/thread = ~2.3GB total, forcing OS to zero-fill fresh pages on each
            // query).  This is the root cause of Q42/Q41's 12ms wall time.
            var chunk_arena = std.heap.ArenaAllocator.init(talloc);
            defer chunk_arena.deinit();
            while (self.morsel_src.next()) |m| {
                _ = chunk_arena.reset(.retain_capacity);
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                const nr = c.num_rows;
                // Apply filter (non-compacting for parallel).
                if (self.filter_pred) |fp| {
                    var pass_mask = try calloc.alloc(bool, nr);
                    if (pre_ic_n > 0) {
                        // SIMD column-major fast path: process all rows per int condition.
                        // 8-16× faster than scalar row-major loop for pure-int predicates
                        // (e.g. Q42: 6 conditions × SIMD AVX2 → reduces filter time ~10×).
                        // When conditions are only partially int (pre_ic_complete=false),
                        // SIMD pre-filters to a small survivor set, then evalExpr runs only
                        // for survivors (e.g. Q38: CounterID=62 → ~1% survive, string check
                        // runs on 100K rows instead of 10M → ~100× faster).
                        const simd_mask = simd_mask_buf.?;
                        const simd_tmp_a = simd_tmp_a_buf.?;
                        const simd_tmp_b = simd_tmp_b_buf.?;
                        @memset(simd_mask[0..nr], 1);
                        for (pre_conds) |cond| {
                            _ = applyIntCondSIMD(&c, cond, nr, simd_mask[0..nr], simd_tmp_a[0..nr], simd_tmp_b[0..nr]);
                        }
                        if (pre_ic_complete) {
                            // All conditions were int — SIMD mask is the final filter result.
                            for (0..nr) |r| pass_mask[r] = (simd_mask[r] != 0);
                        } else {
                            // Partial int pre-filter: run full evalExpr only for SIMD survivors.
                            const ref_mask2 = try calloc.alloc(bool, @min(256, c.columns.len));
                            @memset(ref_mask2, false);
                            collectColRefs(fp, ref_mask2);
                            var ref_buf2 = try calloc.alloc(usize, c.columns.len);
                            var ref_n2: usize = 0;
                            for (ref_mask2, 0..) |m2, i| {
                                if (m2 and i < c.columns.len) {
                                    ref_buf2[ref_n2] = i;
                                    ref_n2 += 1;
                                }
                            }
                            const refs2 = ref_buf2[0..ref_n2];
                            const row2 = try calloc.alloc(?Value, c.columns.len);
                            @memset(row2, null);
                            for (0..nr) |r| {
                                if (simd_mask[r] == 0) {
                                    pass_mask[r] = false;
                                    continue;
                                }
                                for (refs2) |j| {
                                    const col = c.columns[j];
                                    row2[j] = if (col.isRowNull(r)) null else col.data.get(r);
                                }
                                const v = try kernels.evalExpr(fp, row2, null, calloc);
                                pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                            }
                        }
                    } else like_str_path: {
                        // No extractable int conditions — use fast string/general paths.
                        // Fast path: pure LIKE / NOT_LIKE col_ref lit_str.
                        switch (fp) {
                            .like, .not_like => |op| if (op.left == .col_ref and op.right == .lit_str) {
                                const col_idx2 = op.left.col_ref.index;
                                if (col_idx2 < c.columns.len and c.columns[col_idx2].data == .string) {
                                    const matcher = kernels.LikeMatcher.compile(op.right.lit_str, if (op.escape) |e| if (e.len > 0) e[0] else null else null);
                                    const negate = (fp == .not_like);
                                    const col2 = c.columns[col_idx2];
                                    for (0..c.num_rows) |r| {
                                        const s = if (col2.isRowNull(r)) "" else col2.data.string[r];
                                        pass_mask[r] = (matcher.match(s) != negate);
                                    }
                                    break :like_str_path;
                                }
                            },
                            // Fast path: col_ref != lit_str  (e.g. SearchPhrase <> '').
                            .neq => |op| if (op.left == .col_ref and op.right == .lit_str) {
                                const col_idx2 = op.left.col_ref.index;
                                const lit2 = op.right.lit_str;
                                if (col_idx2 < c.columns.len) {
                                    if (c.columns[col_idx2].data == .string) {
                                        const col2 = c.columns[col_idx2];
                                        for (0..c.num_rows) |r| {
                                            const s = if (col2.isRowNull(r)) "" else col2.data.string[r];
                                            pass_mask[r] = !std.mem.eql(u8, s, lit2);
                                        }
                                        break :like_str_path;
                                    } else if (c.columns[col_idx2].data == .bool_u8 and lit2.len == 0) {
                                        // Column decoded as bool_u8 (1=non-empty) via setStringNonEmptyBool.
                                        const col2 = c.columns[col_idx2];
                                        for (0..c.num_rows) |r| {
                                            pass_mask[r] = col2.data.bool_u8[r] != 0;
                                        }
                                        break :like_str_path;
                                    }
                                }
                            },
                            else => {},
                        }
                        // General evalExpr path.
                        const ref_mask = try calloc.alloc(bool, @min(256, c.columns.len));
                        @memset(ref_mask, false);
                        collectColRefs(fp, ref_mask);
                        var ref_buf = try calloc.alloc(usize, c.columns.len);
                        var ref_n: usize = 0;
                        for (ref_mask, 0..) |m2, i| {
                            if (m2 and i < c.columns.len) {
                                ref_buf[ref_n] = i;
                                ref_n += 1;
                            }
                        }
                        const refs = ref_buf[0..ref_n];
                        const row = try calloc.alloc(?Value, c.columns.len);
                        @memset(row, null);
                        for (0..c.num_rows) |r| {
                            for (refs) |j| {
                                const col = c.columns[j];
                                row[j] = if (col.isRowNull(r)) null else col.data.get(r);
                            }
                            const v = try kernels.evalExpr(fp, row, null, calloc);
                            pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                        }
                    }

                    // Process passing rows.
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        var key_valid = true;
                        for (self.keys, 0..) |k, ki| {
                            // Special case: fn_call date_trunc variants.
                            if (k.expr == .fn_call) {
                                const fc = k.expr.fn_call;
                                const col_idx2 = fc.args[0].col_ref.index;
                                if (col_idx2 >= c.columns.len) {
                                    key_valid = false;
                                    break;
                                }
                                const col = c.columns[col_idx2];
                                if (col.isRowNull(r)) {
                                    key_valid = false;
                                    break;
                                }
                                const secs: i64 = switch (col.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @as(i64, @bitCast(a[r])),
                                    else => {
                                        key_valid = false;
                                        break;
                                    },
                                };
                                const divisor_ms: i64 = if (std.mem.eql(u8, fc.name, "toStartOfMinute")) 60_000 else if (std.mem.eql(u8, fc.name, "toStartOfHour")) 3_600_000 else 86_400_000;
                                const ms = secs * 1000;
                                key_buf[ki] = @divTrunc(ms, divisor_ms) * divisor_ms;
                                continue;
                            }
                            // Constant key (lit_i64): value does not depend on row.
                            if (k.expr == .lit_i64) {
                                key_buf[ki] = k.expr.lit_i64;
                                continue;
                            }
                            const col_idx2: usize = switch (k.expr) {
                                .col_ref => |cr| cr.index,
                                .add => |op| op.left.col_ref.index,
                                .sub => |op| op.left.col_ref.index,
                                else => {
                                    key_valid = false;
                                    break;
                                },
                            };
                            const addend2: i64 = switch (k.expr) {
                                .col_ref => 0,
                                .add => |op| op.right.lit_i64,
                                .sub => |op| -op.right.lit_i64,
                                else => 0,
                            };
                            if (col_idx2 >= c.columns.len) {
                                key_valid = false;
                                break;
                            }
                            const col = c.columns[col_idx2];
                            if (col.isRowNull(r)) {
                                key_valid = false;
                                break;
                            }
                            const raw2: i64 = switch (col.data) {
                                .int64 => |a| a[r],
                                .uint64 => |a| @as(i64, @bitCast(a[r])),
                                .bool_u8 => |a| @as(i64, a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                else => {
                                    key_valid = false;
                                    break;
                                },
                            };
                            key_buf[ki] = raw2 +% addend2;
                        }
                        if (!key_valid) continue;
                        const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                        for (self.aggs, 0..) |item, ci| {
                            if (item.expr != .agg_call) continue;
                            const ac = item.expr.agg_call;
                            switch (self.compact_kinds[ci]) {
                                .count => slot_vals[ci] += 1,
                                .i64_sum => if (ac.arg) |arg| {
                                    if (arg == .col_ref) {
                                        const col = c.columns[arg.col_ref.index];
                                        if (!col.isRowNull(r)) switch (col.data) {
                                            .int64 => |v| {
                                                var s: i64 = @bitCast(slot_vals[ci]);
                                                s += v[r];
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            .uint64 => |v| {
                                                var s: i64 = @bitCast(slot_vals[ci]);
                                                s += @as(i64, @bitCast(v[r]));
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            else => {},
                                        };
                                    }
                                },
                                .f64_sum => if (ac.arg) |arg| {
                                    if (arg == .col_ref) {
                                        const col = c.columns[arg.col_ref.index];
                                        if (!col.isRowNull(r)) switch (col.data) {
                                            .int64 => |v| {
                                                var s: f64 = @bitCast(slot_vals[ci]);
                                                s += @floatFromInt(v[r]);
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            .uint64 => |v| {
                                                var s: f64 = @bitCast(slot_vals[ci]);
                                                s += @floatFromInt(v[r]);
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            .bool_u8 => |v| {
                                                var s: f64 = @bitCast(slot_vals[ci]);
                                                s += @floatFromInt(v[r]);
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            .float64 => |v| {
                                                var s: f64 = @bitCast(slot_vals[ci]);
                                                s += v[r];
                                                slot_vals[ci] = @bitCast(s);
                                            },
                                            else => {},
                                        };
                                    }
                                },
                                .f64_str_len_sum => if (ac.arg) |arg| {
                                    // AVG(length(col_ref)) — dig into fn_call arg to get string column.
                                    if (arg == .fn_call and arg.fn_call.args.len == 1 and
                                        arg.fn_call.args[0] == .col_ref)
                                    {
                                        const col_idx2 = arg.fn_call.args[0].col_ref.index;
                                        if (col_idx2 < c.columns.len) {
                                            const col = c.columns[col_idx2];
                                            if (!col.isRowNull(r)) switch (col.data) {
                                                .string => |v| {
                                                    var s: f64 = @bitCast(slot_vals[ci]);
                                                    s += @floatFromInt(v[r].len);
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                else => {},
                                            };
                                        }
                                    }
                                },
                                else => slot_vals[ci] += 1, // fallback: count
                            }
                        }
                    }
                } else {
                    // No filter.
                    // ── Fast path with ahead-prefetch for simple int col_ref keys ──────────
                    // Covers Q16 (1 key: UserID), Q33 (2 keys: WatchID+ClientIP), etc.
                    // For each row r, prefetch the HT cache line that row r+PDIST will need
                    // so that the L3/DRAM latency is hidden behind useful computation.
                    // PDIST=64: HT exceeds L3 on most queries → DRAM latency ~100ns,
                    // loop body ~1-2ns/row, so 64 rows gives ~64-128ns prefetch lead.
                    // PDIST: prefetch distance in rows.  DRAM latency ~100ns, loop body
                    // ~1-2ns/row, so 64 rows gives ~64-128ns prefetch lead.
                    const PDIST: usize = 64;
                    const fast_handled: bool = fast_nofilter: {
                        // All keys must be plain col_ref (no fn_call, lit_i64, arithmetic).
                        for (self.keys) |k| {
                            if (k.expr != .col_ref) break :fast_nofilter false;
                        }
                        if (self.keys.len == 1) {
                            const ci0 = self.keys[0].expr.col_ref.index;
                            if (ci0 >= c.columns.len) break :fast_nofilter false;
                            const col0 = c.columns[ci0];
                            for (0..c.num_rows) |r| {
                                // Prefetch HT entry for row r+PDIST.
                                if (r + PDIST < c.num_rows) {
                                    const fv: i64 = switch (col0.data) {
                                        .int64 => |a| a[r + PDIST],
                                        .uint64 => |a| @bitCast(a[r + PDIST]),
                                        .date_u16 => |a| @as(i64, a[r + PDIST]),
                                        .bool_u8 => |a| @as(i64, a[r + PDIST]),
                                        else => 0,
                                    };
                                    self.local_ht.prefetchForKey1(fv);
                                }
                                if (col0.isRowNull(r)) continue;
                                key_buf[0] = switch (col0.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8 => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                const slot_vals = try self.local_ht.getOrInsert(key_buf[0..1], self.compact_init_vals);
                                for (self.aggs, 0..) |item, ci| {
                                    if (item.expr != .agg_call) continue;
                                    const ac = item.expr.agg_call;
                                    switch (self.compact_kinds[ci]) {
                                        .count => slot_vals[ci] += 1,
                                        .i64_sum => if (ac.arg) |arg| {
                                            if (arg == .col_ref) {
                                                const acol = c.columns[arg.col_ref.index];
                                                if (!acol.isRowNull(r)) switch (acol.data) {
                                                    .int64 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += v[r];
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .uint64 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += @as(i64, @bitCast(v[r]));
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .bool_u8 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += @as(i64, v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    else => {},
                                                };
                                            }
                                        },
                                        .f64_sum => if (ac.arg) |arg| {
                                            if (arg == .col_ref) {
                                                const acol = c.columns[arg.col_ref.index];
                                                if (!acol.isRowNull(r)) switch (acol.data) {
                                                    .int64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .uint64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .bool_u8 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .float64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += v[r];
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    else => {},
                                                };
                                            }
                                        },
                                        else => slot_vals[ci] += 1,
                                    }
                                }
                            }
                            break :fast_nofilter true; // all rows processed
                        } else if (self.keys.len == 2) {
                            const ci0 = self.keys[0].expr.col_ref.index;
                            const ci1 = self.keys[1].expr.col_ref.index;
                            if (ci0 >= c.columns.len or ci1 >= c.columns.len) break :fast_nofilter false;
                            const col0 = c.columns[ci0];
                            const col1 = c.columns[ci1];
                            for (0..c.num_rows) |r| {
                                if (r + PDIST < c.num_rows) {
                                    const fv0: i64 = switch (col0.data) {
                                        .int64 => |a| a[r + PDIST],
                                        .uint64 => |a| @bitCast(a[r + PDIST]),
                                        .date_u16 => |a| @as(i64, a[r + PDIST]),
                                        .bool_u8 => |a| @as(i64, a[r + PDIST]),
                                        else => 0,
                                    };
                                    const fv1: i64 = switch (col1.data) {
                                        .int64 => |a| a[r + PDIST],
                                        .uint64 => |a| @bitCast(a[r + PDIST]),
                                        .date_u16 => |a| @as(i64, a[r + PDIST]),
                                        .bool_u8 => |a| @as(i64, a[r + PDIST]),
                                        else => 0,
                                    };
                                    self.local_ht.prefetchForKeys(fv0, fv1);
                                }
                                if (col0.isRowNull(r) or col1.isRowNull(r)) continue;
                                key_buf[0] = switch (col0.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8 => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                key_buf[1] = switch (col1.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @bitCast(a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    .bool_u8 => |a| @as(i64, a[r]),
                                    else => continue,
                                };
                                const slot_vals = try self.local_ht.getOrInsert(key_buf[0..2], self.compact_init_vals);
                                for (self.aggs, 0..) |item, ci| {
                                    if (item.expr != .agg_call) continue;
                                    const ac = item.expr.agg_call;
                                    switch (self.compact_kinds[ci]) {
                                        .count => slot_vals[ci] += 1,
                                        .i64_sum => if (ac.arg) |arg| {
                                            if (arg == .col_ref) {
                                                const acol = c.columns[arg.col_ref.index];
                                                if (!acol.isRowNull(r)) switch (acol.data) {
                                                    .int64 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += v[r];
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .uint64 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += @as(i64, @bitCast(v[r]));
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .bool_u8 => |v| {
                                                        var s: i64 = @bitCast(slot_vals[ci]);
                                                        s += @as(i64, v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    else => {},
                                                };
                                            }
                                        },
                                        .f64_sum => if (ac.arg) |arg| {
                                            if (arg == .col_ref) {
                                                const acol = c.columns[arg.col_ref.index];
                                                if (!acol.isRowNull(r)) switch (acol.data) {
                                                    .int64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .uint64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .bool_u8 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += @floatFromInt(v[r]);
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    .float64 => |v| {
                                                        var s: f64 = @bitCast(slot_vals[ci]);
                                                        s += v[r];
                                                        slot_vals[ci] = @bitCast(s);
                                                    },
                                                    else => {},
                                                };
                                            }
                                        },
                                        else => slot_vals[ci] += 1,
                                    }
                                }
                            }
                            break :fast_nofilter true; // all rows processed
                        }
                        break :fast_nofilter false; // keys.len > 2 or unhandled
                    };
                    // General no-filter loop (handles fn_call keys, lit_i64, arithmetic, 3+ keys).
                    if (!fast_handled) {
                        for (0..c.num_rows) |r| {
                            var key_valid = true;
                            for (self.keys, 0..) |k, ki| {
                                // Special case: fn_call date_trunc variants.
                                if (k.expr == .fn_call) {
                                    const fc = k.expr.fn_call;
                                    const col_idx3 = fc.args[0].col_ref.index;
                                    if (col_idx3 >= c.columns.len) {
                                        key_valid = false;
                                        break;
                                    }
                                    const col = c.columns[col_idx3];
                                    if (col.isRowNull(r)) {
                                        key_valid = false;
                                        break;
                                    }
                                    const secs: i64 = switch (col.data) {
                                        .int64 => |a| a[r],
                                        .uint64 => |a| @as(i64, @bitCast(a[r])),
                                        else => {
                                            key_valid = false;
                                            break;
                                        },
                                    };
                                    const divisor_ms: i64 = if (std.mem.eql(u8, fc.name, "toStartOfMinute")) 60_000 else if (std.mem.eql(u8, fc.name, "toStartOfHour")) 3_600_000 else 86_400_000;
                                    const ms = secs * 1000;
                                    key_buf[ki] = @divTrunc(ms, divisor_ms) * divisor_ms;
                                    continue;
                                }
                                // Constant key (lit_i64): value does not depend on row.
                                if (k.expr == .lit_i64) {
                                    key_buf[ki] = k.expr.lit_i64;
                                    continue;
                                }
                                const col_idx3: usize = switch (k.expr) {
                                    .col_ref => |cr| cr.index,
                                    .add => |op| op.left.col_ref.index,
                                    .sub => |op| op.left.col_ref.index,
                                    else => {
                                        key_valid = false;
                                        break;
                                    },
                                };
                                const addend3: i64 = switch (k.expr) {
                                    .col_ref => 0,
                                    .add => |op| op.right.lit_i64,
                                    .sub => |op| -op.right.lit_i64,
                                    else => 0,
                                };
                                if (col_idx3 >= c.columns.len) {
                                    key_valid = false;
                                    break;
                                }
                                const col = c.columns[col_idx3];
                                if (col.isRowNull(r)) {
                                    key_valid = false;
                                    break;
                                }
                                const raw3: i64 = switch (col.data) {
                                    .int64 => |a| a[r],
                                    .uint64 => |a| @as(i64, @bitCast(a[r])),
                                    .bool_u8 => |a| @as(i64, a[r]),
                                    .date_u16 => |a| @as(i64, a[r]),
                                    else => {
                                        key_valid = false;
                                        break;
                                    },
                                };
                                key_buf[ki] = raw3 +% addend3;
                            }
                            if (!key_valid) continue;
                            const slot_vals = try self.local_ht.getOrInsert(key_buf, self.compact_init_vals);
                            for (self.aggs, 0..) |item, ci| {
                                if (item.expr != .agg_call) continue;
                                const ac = item.expr.agg_call;
                                switch (self.compact_kinds[ci]) {
                                    .count => slot_vals[ci] += 1,
                                    .i64_sum => if (ac.arg) |arg| {
                                        if (arg == .col_ref) {
                                            const col = c.columns[arg.col_ref.index];
                                            if (!col.isRowNull(r)) switch (col.data) {
                                                .int64 => |v| {
                                                    var s: i64 = @bitCast(slot_vals[ci]);
                                                    s += v[r];
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                .uint64 => |v| {
                                                    var s: i64 = @bitCast(slot_vals[ci]);
                                                    s += @as(i64, @bitCast(v[r]));
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                .bool_u8 => |v| {
                                                    var s: i64 = @bitCast(slot_vals[ci]);
                                                    s += @as(i64, v[r]);
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                else => {},
                                            };
                                        }
                                    },
                                    .f64_sum => if (ac.arg) |arg| {
                                        if (arg == .col_ref) {
                                            const col = c.columns[arg.col_ref.index];
                                            if (!col.isRowNull(r)) switch (col.data) {
                                                .int64 => |v| {
                                                    var s: f64 = @bitCast(slot_vals[ci]);
                                                    s += @floatFromInt(v[r]);
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                .uint64 => |v| {
                                                    var s: f64 = @bitCast(slot_vals[ci]);
                                                    s += @floatFromInt(v[r]);
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                .bool_u8 => |v| {
                                                    var s: f64 = @bitCast(slot_vals[ci]);
                                                    s += @floatFromInt(v[r]);
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                .float64 => |v| {
                                                    var s: f64 = @bitCast(slot_vals[ci]);
                                                    s += v[r];
                                                    slot_vals[ci] = @bitCast(s);
                                                },
                                                else => {},
                                            };
                                        }
                                    },
                                    else => slot_vals[ci] += 1, // fallback: count
                                }
                            }
                        }
                    } // if (!fast_handled)
                }
            }
        }
    };

    // Q41/Q42 touch only 6-7 columns of the 105-column hits schema.
    var parallel_scan_request: ScanRequest = .{};
    parallel_scan_request.addItems(keys);
    parallel_scan_request.addItems(aggs);
    if (filter_pred) |fp| parallel_scan_request.addExpr(fp);
    const parallel_scan_scope = parallel_scan_request.apply(ctx.source, .any_reduction);
    defer parallel_scan_scope.deinit();

    // Allocate per-thread contexts.
    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);

    // For narrow int key columns (int8/int16) with a filter, cardinality is likely very low.
    // Start with a tiny HT (grows naturally to fit actual unique count) so it stays L1-resident.
    const all_narrow_keys: bool = blk: {
        if (filter_pred == null) break :blk false;
        const sm_narrow = ctx.source.schema();
        for (keys) |k| {
            if (k.expr != .col_ref) break :blk false;
            const ci = k.expr.col_ref.index;
            if (ci >= sm_narrow.len or !sm_narrow[ci].is_narrow_int) break :blk false;
        }
        break :blk true;
    };

    const pctxs = try alloc.alloc(ParHashCtx, n_threads);
    for (pctxs) |*pc| {
        pc.* = .{
            .source = ctx.source,
            .filter_pred = filter_pred,
            .keys = keys,
            .aggs = aggs,
            .compact_kinds = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .morsel_src = &morsel_src,
            .parent_alloc = alloc,
            .local_ht = try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len,
                // For narrow int keys (int8/int16) with a filter, actual cardinality is
                // typically very low (e.g. Q42: ~200 unique WindowClientWidth×Height pairs).
                // Start tiny (est_rows=0 → INITIAL_CAP=64); grows naturally to fit the actual
                // unique count and stays L1-resident. Avoids allocating a 524KB L3-resident HT.
                // For wide keys with filter: use conservative pre-size to avoid memset overhead.
                // Without filter: pre-size to avoid scan-phase doubling (Q33-style full scans).
                if (all_narrow_keys) 0 else if (filter_pred != null) @max(256, total_rows / n_threads / 32) else @max(256, total_rows / n_threads + 1)),
        };
    }

    try parallel.parallelFor(alloc, ParHashCtx, ParHashCtx.work, pctxs, &morsel_src);

    for (pctxs) |*pc| {
        if (pc.err) |e| return e;
    }

    // Compute total unique entries across all local HTs.
    var total_count: usize = 0;
    for (pctxs) |*pc| total_count += pc.local_ht.count;

    // Parallel partitioned merge: split the key space into `part_T` partitions.
    // Each thread handles one partition → its output HT is ~total/part_T entries.
    // part_T = n_threads: each thread handles exactly 1 partition, maximizing
    // parallelism for the merge phase (4 threads × 1 partition each).
    // Each partition master is ~total/4 entries; for Q33 (1M groups) this is
    // ~250K entries = 18MB per partition.  All 4 threads scan all local HTs
    // simultaneously (reading the same cache lines = shared DRAM bandwidth).
    const part_T: usize = blk: {
        var p: usize = 1;
        while (p * 2 <= n_threads) p <<= 1;
        break :blk p;
    };
    const part_mask: u64 = @as(u64, @intCast(part_T)) - 1;

    const part_masters = try alloc.alloc(ht.CompactIntKeyHashTable, part_T);
    {
        const cap_per_part: u64 = @max(64, @as(u64, @intCast(total_count)) / @as(u64, @intCast(part_T)) * 100 / 65 + 16);
        for (0..part_T) |t| {
            part_masters[t] = try ht.CompactIntKeyHashTable.initWithCapacity(alloc, keys.len, aggs.len, cap_per_part);
        }
    }

    const PMCtx = struct {
        pctxs: []ParHashCtx,
        part_masters: []ht.CompactIntKeyHashTable,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        part_mask: u64,
        err: ?anyerror = null,

        fn work(self: *@This(), src: *parallel.MorselSource) void {
            while (src.next()) |m| {
                for (m.start..m.end) |t| {
                    const part_id = @as(u64, @intCast(t));
                    for (self.pctxs) |*pc| {
                        pc.local_ht.mergeIntoPartitioned(
                            &self.part_masters[t],
                            self.compact_kinds,
                            self.compact_init_vals,
                            part_id,
                            self.part_mask,
                        ) catch |e| {
                            self.err = e;
                            return;
                        };
                    }
                }
            }
        }
    };

    const pm_ctxs = try alloc.alloc(PMCtx, n_threads);
    for (pm_ctxs) |*pm| pm.* = .{
        .pctxs = pctxs,
        .part_masters = part_masters,
        .compact_kinds = compact_kinds,
        .compact_init_vals = compact_init_vals,
        .part_mask = part_mask,
    };
    var pm_src = parallel.MorselSource.init(part_T, 1);
    try parallel.parallelFor(alloc, PMCtx, PMCtx.work, pm_ctxs, &pm_src);
    for (pm_ctxs) |*pm| {
        if (pm.err) |e| return e;
    }

    const part_hts = part_masters;

    // Emit result — same logic as in executeHashAggChunked compact emit path.

    // Precompute key output types so makeRow emits the correct Value union variant.
    const key_out_types_buf = try alloc.alloc(ColumnType, keys.len);
    for (keys, 0..) |k, i| key_out_types_buf[i] = k.out_type;
    const out_metas = try buildAggResultMetas(keys, aggs, alloc);

    if (top_offset > 0 and top_k > 0 and sort_keys.len > 0) {
        var merged_count: usize = 0;
        for (part_hts) |*ph| merged_count += ph.count;
        if (merged_count <= top_offset) return RowBuffer.init(out_metas);
    }

    var rl = RowBuffer.init(out_metas);
    const MCtx = struct {
        keys_n: usize,
        aggs_n: usize,
        compact_kinds: []const ht.CompactAggKind,
        aggs: []const plan.ProjectItem,
        rl: *RowBuffer,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,
        // Optional top-K heap (non-null → emit into heap instead of rl).
        heap: ?[][]?Value = null,
        heap_len: usize = 0,
        heap_k: usize = 0,
        sort_keys: []const plan.SortKey = &.{},
        key_out_types: []const ColumnType = &.{},
        // Cached raw sort-key value of heap[0] (the heap minimum for DESC, maximum for ASC).
        // Updated whenever the heap changes. Avoids double pointer dereference in hot path.
        heap_min_cached: i64 = std.math.minInt(i64),

        fn heapSiftDown(self: *@This(), i: usize) void {
            var cur = i;
            while (true) {
                var worst = cur;
                const l = cur * 2 + 1;
                const r = cur * 2 + 2;
                if (l < self.heap_len and rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[l])) worst = l;
                if (r < self.heap_len and rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[r])) worst = r;
                if (worst == cur) break;
                const tmp = self.heap.?[cur];
                self.heap.?[cur] = self.heap.?[worst];
                self.heap.?[worst] = tmp;
                cur = worst;
            }
        }

        fn heapSiftUp(self: *@This(), i: usize) void {
            var cur = i;
            while (cur > 0) {
                const parent = (cur - 1) / 2;
                if (rowLessThan(self.sort_keys, self.heap.?[parent], self.heap.?[cur])) {
                    const tmp = self.heap.?[cur];
                    self.heap.?[cur] = self.heap.?[parent];
                    self.heap.?[parent] = tmp;
                    cur = parent;
                } else break;
            }
        }

        // Update `heap_min_cached` from heap[0] after any heap structural change.
        fn updateHeapMinCache(self: *@This()) void {
            if (self.heap_len == 0 or self.sort_keys.len == 0) return;
            const heap = self.heap.?;
            const ci = self.sort_keys[0].col_idx;
            if (ci >= heap[0].len) return;
            self.heap_min_cached = if (heap[0][ci]) |v| switch (v) {
                .int64 => |x| x,
                .uint64 => |x| @bitCast(x),
                .float64 => |x| @as(i64, @bitCast(x)),
                .datetime64_ms => |x| x,
                .date_u16 => |x| @as(i64, x),
                else => std.math.minInt(i64),
            } else std.math.minInt(i64);
        }

        fn makeRow(self: *@This(), key_vals: []const i64, acc_vals: []const u64) ?[]?Value {
            const row = self.alloc.alloc(?Value, self.keys_n + self.aggs_n) catch return null;

            for (key_vals, 0..) |kv, i| {
                const out_type: ColumnType = if (i < self.key_out_types.len) self.key_out_types[i] else .int64;
                row[i] = switch (out_type) {
                    .datetime64_ms => .{ .datetime64_ms = kv },
                    .date_u16 => .{ .date_u16 = @intCast(kv) },
                    else => .{ .int64 = kv },
                };
            }
            for (self.compact_kinds, 0..) |kind, i| {
                row[self.keys_n + i] = switch (kind) {
                    .count => .{ .int64 = @intCast(acc_vals[i]) },
                    .i64_sum => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_sum => .{ .uint64 = acc_vals[i] },
                    .f64_sum, .f64_str_len_sum => blk: {
                        const sum: f64 = @bitCast(acc_vals[i]);
                        // If the agg is AVG, finalize by dividing sum by count.
                        if (i < self.aggs.len and self.aggs[i].expr == .agg_call and
                            self.aggs[i].expr.agg_call.kind == .avg)
                        {
                            for (self.compact_kinds, 0..) |ck, j| {
                                if (ck == .count) {
                                    const cnt = acc_vals[j];
                                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                                    break;
                                }
                            }
                        }
                        break :blk Value{ .float64 = sum };
                    },
                    .i64_min, .i64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_min, .u64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .f64_min, .f64_max => .{ .float64 = @bitCast(acc_vals[i]) },
                    .str_min, .str_max => .{ .int64 = 0 },
                    .count_distinct_u64 => .{ .uint64 = acc_vals[i] },
                };
            }
            return row;
        }

        fn cb(self: *@This(), key_vals: []const i64, acc_vals: []const u64) void {
            if (self.heap) |heap| {
                // Top-K heap path.
                // Ultra-fast pre-check using cached heap minimum: avoids 2 pointer
                // dereferences for the common case (1M entries, only 10 qualify).
                if (self.heap_len >= self.heap_k and self.sort_keys.len > 0) {
                    const sk = self.sort_keys[0];
                    const ci = sk.col_idx;
                    const new_raw: i64 = blk: {
                        if (ci < self.keys_n) break :blk key_vals[ci];
                        const ai = ci - self.keys_n;
                        if (ai < self.compact_kinds.len) {
                            break :blk switch (self.compact_kinds[ai]) {
                                .count => @intCast(acc_vals[ai]),
                                .i64_sum, .i64_min, .i64_max, .u64_min, .u64_max => @bitCast(acc_vals[ai]),
                                else => std.math.maxInt(i64),
                            };
                        }
                        break :blk std.math.maxInt(i64);
                    };
                    // Use cached heap_min (L1-resident scalar, no pointer chase).
                    const qualifies = if (sk.desc)
                        new_raw > self.heap_min_cached
                    else
                        new_raw < self.heap_min_cached;
                    if (!qualifies) return;
                }
                const row = self.makeRow(key_vals, acc_vals) orelse {
                    self.err = error.OutOfMemory;
                    return;
                };
                if (self.heap_len < self.heap_k) {
                    heap[self.heap_len] = row;
                    self.heap_len += 1;
                    self.heapSiftUp(self.heap_len - 1);
                    self.updateHeapMinCache();
                } else if (rowLessThan(self.sort_keys, row, heap[0])) {
                    heap[0] = row;
                    self.heapSiftDown(0);
                    self.updateHeapMinCache();
                }
            } else {
                const row = self.makeRow(key_vals, acc_vals) orelse {
                    self.err = error.OutOfMemory;
                    return;
                };
                self.rl.append(self.alloc, row) catch |e| {
                    self.err = e;
                };
            }
        }
    };
    const use_heap = top_k > 0 and sort_keys.len > 0;
    const heap_buf: ?[][]?Value = if (use_heap) try alloc.alloc([]?Value, top_k) else null;
    var emit_ctx = MCtx{
        .keys_n = keys.len,
        .aggs_n = aggs.len,
        .compact_kinds = compact_kinds,
        .aggs = aggs,
        .rl = &rl,
        .alloc = alloc,
        .heap = heap_buf,
        .heap_len = 0,
        .heap_k = top_k,
        .sort_keys = sort_keys,
        .key_out_types = key_out_types_buf,
    };
    // Emit from all partition HTs (each small → L3-friendly).
    for (part_hts) |*ph| {
        ph.iterate(&emit_ctx, MCtx.cb);
        if (emit_ctx.err) |e| return e;
    }

    if (use_heap) {
        // Sort the heap buffer and emit top-k rows in order.
        const heap_rows = emit_ctx.heap.?[0..emit_ctx.heap_len];
        // Sort descending (reverse of heap order = best first).
        std.sort.pdq([]?Value, heap_rows, TopKSortCtx{ .keys = sort_keys }, TopKSortCtx.lessThan);
        var result_rl = RowBuffer.init(out_metas);
        for (heap_rows) |row| try result_rl.append(alloc, row);
        return result_rl;
    }

    return rl;
}

pub fn executeTwoPhaseHashAgg(
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    compact_kinds: []const ht.CompactAggKind,
    compact_init_vals: []const u64,
    total_rows: u64,
    n_threads: usize,
    alloc: std.mem.Allocator,
    ctx: *QueryContext,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    str_filter: ?SimpleStrFilter,
    int_filter: ?[]const IntCmpCond,
) !?RowBuffer {
    const N_PARTS: usize = 128;
    const n_keys = keys.len;
    const n_aggs = aggs.len;
    var key_ci: [4]usize = @as([4]usize, @splat(0));
    var key_offs: [4]i64 = @as([4]i64, @splat(0));
    for (keys, 0..) |k, i| {
        switch (k.expr) {
            .col_ref => |cr| {
                key_ci[i] = cr.index;
                key_offs[i] = 0;
            },
            .sub => |op| {
                key_ci[i] = op.left.col_ref.index;
                key_offs[i] = -op.right.lit_i64;
            },
            .add => |op| {
                key_ci[i] = op.left.col_ref.index;
                key_offs[i] = op.right.lit_i64;
            },
            else => unreachable,
        }
    }

    // Same-column key collapsing: if all keys derive from the same source column
    // (e.g. Q36: GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3), we only
    // store one effective key value in scatter buffers and reconstruct the rest at
    // emit time. This halves scatter buffer size and uses a cheaper 1-key hash.
    var n_eff_keys: usize = n_keys;
    if (n_keys > 1) {
        var all_same = true;
        for (1..n_keys) |i| {
            if (key_ci[i] != key_ci[0]) {
                all_same = false;
                break;
            }
        }
        if (all_same) n_eff_keys = 1;
    }
    // count_only: sole agg is COUNT(*) — omit the count slot from scatter buffers
    // entirely.  Phase 1 writes [hash, k0, ...] only; Phase 2 does slot_vals[0]+=1.
    // This shrinks scatter bandwidth by 1 u64 per row (33% for 1-key queries like Q16).
    const count_only: bool = n_aggs == 1 and compact_kinds[0] == .count;
    const omit_first_count: bool = !count_only and n_aggs > 0 and compact_kinds[0] == .count;
    // row_stride = 1 (stored hash) + n_eff_keys + scatter agg slots.
    // COUNT(*) partials are implicit when they are the first aggregate.
    const scatter_aggs = if (count_only) @as(usize, 0) else n_aggs - @as(usize, if (omit_first_count) 1 else 0);
    const row_stride = 1 + n_eff_keys + scatter_aggs;

    // Pre-extract agg info: (col_idx or ~0 for no-arg, kind).
    const AggInfo = struct { col_idx: usize, kind: ht.CompactAggKind };
    const agg_infos = try alloc.alloc(AggInfo, n_aggs);
    for (aggs, compact_kinds, agg_infos) |ag, kind, *info| {
        const ac = ag.expr.agg_call;
        const col_idx: usize = blk: {
            if (ac.arg) |arg| {
                if (arg == .col_ref) break :blk arg.col_ref.index;
                // f64_str_len_sum: AVG(length(col_ref)) — dig into the fn_call arg.
                if (kind == .f64_str_len_sum and arg == .fn_call and
                    arg.fn_call.args.len == 1 and arg.fn_call.args[0] == .col_ref)
                    break :blk arg.fn_call.args[0].col_ref.index;
            }
            break :blk ~@as(usize, 0);
        };
        info.* = .{ .col_idx = col_idx, .kind = kind };
    }

    // ── Phase 1: parallel scatter ─────────────────────────────────────────────

    const ScatterCtx = struct {
        // Flat u64 scatter buffers per partition (stride=row_stride per record).
        // Each entry: [hash, k0, k1?, agg0, agg1, ...]
        bufs: [N_PARTS]std.ArrayListUnmanaged(u64),
        // Per-ctx arena backed by raw_c_allocator (malloc/free).
        // Using raw_c_allocator instead of page_allocator so that freed pages are
        // returned to the malloc pool and reused on the next query run without
        // triggering new page faults — avoids ~490ms of page-fault overhead in
        // hot benchmark runs where 490MB of scatter buffers are re-allocated each time.
        buf_arena: std.heap.ArenaAllocator,
        source: SourceIface,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        n_keys: usize,
        n_eff_keys: usize, // <= n_keys; =1 when all keys share one source column
        n_aggs: usize,
        row_stride: usize,
        key_ci: [4]usize,
        key_offs: [4]i64,
        agg_infos: []const AggInfo,
        compact_kinds: []const ht.CompactAggKind,
        str_filter: ?SimpleStrFilter,
        int_filter: ?[]const IntCmpCond,
        count_only: bool,
        omit_first_count: bool,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            const buf_alloc = self.buf_arena.allocator();

            var thread_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            // ── Raw-slice fast path for scatter phase ─────────────────────────────
            // When: pure int filter + count_only (no agg cols) + all key cols have
            // raw slices available.  Bypasses fetchRange for the scatter morsel loop,
            // applying SIMD filter on narrow mmap'd slices and scattering passing rows
            // directly.  For Q41 (5 filter + 2 key narrow cols): eliminates scalar
            // per-row filter loop (~50M ops) and i32→i64 widening (~250MB bandwidth).
            raw_scatter: {
                const ics: []const IntCmpCond = if (self.int_filter) |ic| ic else &.{};
                // Allow raw_scatter for count_only or for compact numeric aggs whose
                // inputs are backed by raw mmap slices. This keeps Q9/Q10 off the
                // fetchRange/DataChunk widening path while still feeding the existing
                // partitioned phase-2 aggregator.
                const sch = self.source.schema();
                const raw_sf_offsets: ?[]const u64 = blk: {
                    const sf = self.str_filter orelse break :blk null;
                    const nm = if (sf.col_idx < sch.len) sch[sf.col_idx].name else break :raw_scatter;
                    break :blk self.source.getRawStrOffsets(nm) orelse break :raw_scatter;
                };
                const raw_sf_bytes: ?[]const u8 = blk: {
                    const sf = self.str_filter orelse break :blk null;
                    if (sf.value.len == 0) break :blk null;
                    const nm = if (sf.col_idx < sch.len) sch[sf.col_idx].name else break :raw_scatter;
                    break :blk self.source.getRawStrBytes(nm) orelse break :raw_scatter;
                };
                // Resolve filter condition columns to raw slices.
                var rf: [16]RawColSlice = undefined;
                for (ics, 0..) |cond, ci| {
                    rf[ci] = RawColSlice.resolve(self.source, sch, cond.col_idx) orelse break :raw_scatter;
                }
                // Resolve key columns to raw slices.
                var rk: [4]RawColSlice = undefined;
                for (0..self.n_eff_keys) |ki| {
                    rk[ki] = RawColSlice.resolve(self.source, sch, self.key_ci[ki]) orelse break :raw_scatter;
                }
                var ra: [16]RawColSlice = undefined;
                if (!self.count_only) {
                    if (self.n_aggs > ra.len) break :raw_scatter;
                    for (self.agg_infos, 0..) |info, ai| {
                        switch (info.kind) {
                            .count => {},
                            .i64_sum, .u64_sum, .f64_sum, .count_distinct_u64 => {
                                ra[ai] = RawColSlice.resolve(self.source, sch, info.col_idx) orelse break :raw_scatter;
                            },
                            else => break :raw_scatter,
                        }
                    }
                }
                // No-filter fast path: scatter every row without mask allocation or SIMD.
                // Activated for queries like Q16 (GROUP BY int-key, no WHERE clause)
                // and Q9 (GROUP BY RegionID, COUNT(DISTINCT UserID)).
                if (ics.len == 0) {
                    var rec: [18]u64 = undefined;
                    while (self.morsel_src.next()) |m| {
                        for (m.start..m.end) |row| {
                            if (self.str_filter) |sf| {
                                if (!sf.passesRaw(raw_sf_offsets.?, raw_sf_bytes, row)) continue;
                            }
                            var kv: [4]i64 = undefined;
                            for (0..self.n_eff_keys) |ki|
                                kv[ki] = rk[ki].getI64(row) +% self.key_offs[ki];
                            const h: u64 = blk: {
                                if (self.n_eff_keys == 1) {
                                    var hh: u64 = @bitCast(kv[0]);
                                    hh ^= hh >> 33;
                                    hh *%= 0xff51afd7ed558ccd;
                                    hh ^= hh >> 33;
                                    hh *%= 0xc4ceb9fe1a85ec53;
                                    hh ^= hh >> 33;
                                    break :blk hh | (1 << 63);
                                }
                                const hk0: u64 = @bitCast(kv[0]);
                                const hk1: u64 = if (self.n_eff_keys >= 2) @bitCast(kv[1]) else 0;
                                var hh = hk0 *% 0x9e3779b97f4a7c15 ^ hk1 *% 0x6c62272e07bb0142;
                                if (self.n_eff_keys >= 3) hh ^= @as(u64, @bitCast(kv[2])) *% 0xd2a98b26625eee7b;
                                if (self.n_eff_keys >= 4) hh ^= @as(u64, @bitCast(kv[3])) *% 0xa0761d6478bd642f;
                                hh ^= hh >> 30;
                                hh *%= 0xbf58476d1ce4e5b9;
                                hh ^= hh >> 27;
                                hh *%= 0x94d049bb133111eb;
                                hh ^= hh >> 31;
                                break :blk hh | (1 << 63);
                            };
                            rec[0] = h;
                            for (0..self.n_eff_keys) |ki| rec[1 + ki] = @bitCast(kv[ki]);
                            if (!self.count_only) {
                                for (self.compact_kinds, 0..) |kind, ai| {
                                    if (self.omit_first_count and ai == 0) continue;
                                    const out_ai = ai - @as(usize, if (self.omit_first_count) 1 else 0);
                                    rec[1 + self.n_eff_keys + out_ai] = if (kind == .count) 1 else ra[ai].getAggPartial(row, kind);
                                }
                            }
                            try self.bufs[h & (N_PARTS - 1)].appendSlice(buf_alloc, rec[0..self.row_stride]);
                        }
                    }
                    return;
                }

                // Allocate per-thread SIMD scratch buffers (reused across morsels).
                const rs_sz = parallel.default_morsel_size + 1;
                const rs_mask = try talloc.alloc(i16, rs_sz);
                const rs_tmp_a = try talloc.alloc(i16, rs_sz);
                const rs_tmp_b = try talloc.alloc(i16, rs_sz);
                var rec: [18]u64 = undefined;
                while (self.morsel_src.next()) |m| {
                    const start = m.start;
                    const nr = m.end - m.start;
                    @memset(rs_mask[0..nr], 1);
                    for (ics, 0..) |cond, ci| {
                        rf[ci].applyMaskSIMD(start, nr, cond, rs_mask[0..nr], rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                        // Inter-condition early-exit: if the mask is already all-zero,
                        // remaining conditions cannot revive any row — skip them.
                        // Helps Q41 where CounterID=62 kills ~99% of rows upfront.
                        {
                            var still_live = false;
                            var ei: usize = 0;
                            while (ei + 32 <= nr) : (ei += 32) {
                                const v: @Vector(32, i16) = rs_mask[ei..][0..32].*;
                                if (@reduce(.Or, v) != 0) {
                                    still_live = true;
                                    break;
                                }
                            }
                            if (!still_live) while (ei < nr) : (ei += 1) {
                                if (rs_mask[ei] != 0) {
                                    still_live = true;
                                    break;
                                }
                            };
                            if (!still_live) break;
                        }
                    }
                    // Chunked mask scan: OR-reduce 32-lane chunks to skip all-zero
                    // blocks in ~5 cycles instead of 32 scalar checks.
                    const CHUNK = 32;
                    var r: usize = 0;
                    while (r + CHUNK <= nr) : (r += CHUNK) {
                        const v: @Vector(CHUNK, i16) = rs_mask[r..][0..CHUNK].*;
                        if (@reduce(.Or, v) == 0) continue;
                        for (r..r + CHUNK) |ri| {
                            if (rs_mask[ri] == 0) continue;
                            const row = start + ri;
                            if (self.str_filter) |sf| {
                                if (!sf.passesRaw(raw_sf_offsets.?, raw_sf_bytes, row)) continue;
                            }
                            var kv: [4]i64 = undefined;
                            for (0..self.n_eff_keys) |ki| {
                                kv[ki] = rk[ki].getI64(row) +% self.key_offs[ki];
                            }
                            const h: u64 = blk: {
                                if (self.n_eff_keys == 1) {
                                    var hh: u64 = @bitCast(kv[0]);
                                    hh ^= hh >> 33;
                                    hh *%= 0xff51afd7ed558ccd;
                                    hh ^= hh >> 33;
                                    hh *%= 0xc4ceb9fe1a85ec53;
                                    hh ^= hh >> 33;
                                    break :blk hh | (1 << 63);
                                }
                                const hk0: u64 = @bitCast(kv[0]);
                                const hk1: u64 = if (self.n_eff_keys >= 2) @bitCast(kv[1]) else 0;
                                var hh = hk0 *% 0x9e3779b97f4a7c15 ^ hk1 *% 0x6c62272e07bb0142;
                                if (self.n_eff_keys >= 3) hh ^= @as(u64, @bitCast(kv[2])) *% 0xd2a98b26625eee7b;
                                if (self.n_eff_keys >= 4) hh ^= @as(u64, @bitCast(kv[3])) *% 0xa0761d6478bd642f;
                                hh ^= hh >> 30;
                                hh *%= 0xbf58476d1ce4e5b9;
                                hh ^= hh >> 27;
                                hh *%= 0x94d049bb133111eb;
                                hh ^= hh >> 31;
                                break :blk hh | (1 << 63);
                            };
                            const part_id = h & (N_PARTS - 1);
                            rec[0] = h;
                            for (0..self.n_eff_keys) |ki| rec[1 + ki] = @bitCast(kv[ki]);
                            if (!self.count_only) {
                                for (self.compact_kinds, 0..) |kind, ai| {
                                    if (self.omit_first_count and ai == 0) continue;
                                    const out_ai = ai - @as(usize, if (self.omit_first_count) 1 else 0);
                                    rec[1 + self.n_eff_keys + out_ai] = if (kind == .count) 1 else ra[ai].getAggPartial(row, kind);
                                }
                            }
                            try self.bufs[part_id].appendSlice(buf_alloc, rec[0..self.row_stride]);
                        }
                    }
                    while (r < nr) : (r += 1) {
                        if (rs_mask[r] == 0) continue;
                        const row = start + r;
                        if (self.str_filter) |sf| {
                            if (!sf.passesRaw(raw_sf_offsets.?, raw_sf_bytes, row)) continue;
                        }
                        var kv: [4]i64 = undefined;
                        for (0..self.n_eff_keys) |ki| {
                            kv[ki] = rk[ki].getI64(row) +% self.key_offs[ki];
                        }
                        const h: u64 = blk: {
                            if (self.n_eff_keys == 1) {
                                var hh: u64 = @bitCast(kv[0]);
                                hh ^= hh >> 33;
                                hh *%= 0xff51afd7ed558ccd;
                                hh ^= hh >> 33;
                                hh *%= 0xc4ceb9fe1a85ec53;
                                hh ^= hh >> 33;
                                break :blk hh | (1 << 63);
                            }
                            const hk0: u64 = @bitCast(kv[0]);
                            const hk1: u64 = if (self.n_eff_keys >= 2) @bitCast(kv[1]) else 0;
                            var hh = hk0 *% 0x9e3779b97f4a7c15 ^ hk1 *% 0x6c62272e07bb0142;
                            if (self.n_eff_keys >= 3) hh ^= @as(u64, @bitCast(kv[2])) *% 0xd2a98b26625eee7b;
                            if (self.n_eff_keys >= 4) hh ^= @as(u64, @bitCast(kv[3])) *% 0xa0761d6478bd642f;
                            hh ^= hh >> 30;
                            hh *%= 0xbf58476d1ce4e5b9;
                            hh ^= hh >> 27;
                            hh *%= 0x94d049bb133111eb;
                            hh ^= hh >> 31;
                            break :blk hh | (1 << 63);
                        };
                        const part_id = h & (N_PARTS - 1);
                        rec[0] = h;
                        for (0..self.n_eff_keys) |ki| rec[1 + ki] = @bitCast(kv[ki]);
                        if (!self.count_only) {
                            for (self.compact_kinds, 0..) |kind, ai| {
                                if (self.omit_first_count and ai == 0) continue;
                                const out_ai = ai - @as(usize, if (self.omit_first_count) 1 else 0);
                                rec[1 + self.n_eff_keys + out_ai] = if (kind == .count) 1 else ra[ai].getAggPartial(row, kind);
                            }
                        }
                        try self.bufs[part_id].appendSlice(buf_alloc, rec[0..self.row_stride]);
                    }
                }
                return; // Raw scatter handled — skip fetchRange scatter loop.
            }

            // Reuse arena per morsel to avoid accumulating 566MB/thread of talloc growth.
            var chunk_arena = std.heap.ArenaAllocator.init(talloc);
            defer chunk_arena.deinit();
            while (self.morsel_src.next()) |m| {
                _ = chunk_arena.reset(.retain_capacity);
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, chunk_arena.allocator());

                const ci0 = self.key_ci[0];
                if (ci0 >= c.columns.len) continue;
                for (1..self.n_eff_keys) |ki| {
                    if (self.key_ci[ki] >= c.columns.len) continue;
                }

                // Resolve str_filter column once per chunk (null if col not present or wrong type).
                // Accept both .string (normal) and .bool_u8 (set by setStringNonEmptyBool).
                const sf_col: ?chunk.Column = if (self.str_filter) |sf|
                    if (sf.col_idx < c.columns.len and
                        (c.columns[sf.col_idx].data == .string or c.columns[sf.col_idx].data == .bool_u8))
                        c.columns[sf.col_idx]
                    else
                        null
                else
                    null;

                var rec: [18]u64 = undefined; // max row_stride = 18

                for (0..c.num_rows) |r| {
                    // Inline string pre-filter (e.g. SearchPhrase <> '').
                    if (self.str_filter) |sf| {
                        if (sf_col) |sfc| {
                            if (!sf.passes(sfc, r)) continue;
                        }
                    }
                    // Inline int pre-filter (e.g. Q41: CounterID=62 AND TraficSourceID IN (-1,6) AND ...).
                    if (self.int_filter) |ics| {
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
                                .bool_u8 => |a| @as(i64, a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                .datetime64_ms => |a| a[r],
                                else => {
                                    pass = false;
                                    break;
                                },
                            };
                            const ok = switch (cond.op) {
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
                        if (!pass) continue;
                    }

                    // Extract up to n_eff_keys integer values (with arithmetic offsets).
                    // When n_eff_keys < n_keys (same-column collapsing), we only store kv[0].
                    var kv: [4]i64 = undefined;
                    var any_null = false;
                    for (0..self.n_eff_keys) |ki| {
                        const col = c.columns[self.key_ci[ki]];
                        if (col.isRowNull(r)) {
                            any_null = true;
                            break;
                        }
                        const base: i64 = switch (col.data) {
                            .int64 => |a| a[r],
                            .uint64 => |a| @bitCast(a[r]),
                            .date_u16 => |a| @as(i64, a[r]),
                            .bool_u8 => |a| @as(i64, a[r]),
                            else => {
                                any_null = true;
                                break;
                            },
                        };
                        kv[ki] = base +% self.key_offs[ki];
                    }
                    if (any_null) continue;

                    // Compute hash over n_eff_keys values.
                    const h: u64 = blk: {
                        if (self.n_eff_keys == 1) {
                            var hh: u64 = @bitCast(kv[0]);
                            hh ^= hh >> 33;
                            hh *%= 0xff51afd7ed558ccd;
                            hh ^= hh >> 33;
                            hh *%= 0xc4ceb9fe1a85ec53;
                            hh ^= hh >> 33;
                            break :blk hh | (1 << 63);
                        }
                        const hk0: u64 = @bitCast(kv[0]);
                        const hk1: u64 = if (self.n_eff_keys >= 2) @bitCast(kv[1]) else 0;
                        var hh = hk0 *% 0x9e3779b97f4a7c15 ^ hk1 *% 0x6c62272e07bb0142;
                        if (self.n_eff_keys >= 3) hh ^= @as(u64, @bitCast(kv[2])) *% 0xd2a98b26625eee7b;
                        if (self.n_eff_keys >= 4) hh ^= @as(u64, @bitCast(kv[3])) *% 0xa0761d6478bd642f;
                        hh ^= hh >> 30;
                        hh *%= 0xbf58476d1ce4e5b9;
                        hh ^= hh >> 27;
                        hh *%= 0x94d049bb133111eb;
                        hh ^= hh >> 31;
                        break :blk hh | (1 << 63);
                    };

                    const part_id = h & (N_PARTS - 1);
                    rec[0] = h;
                    for (0..self.n_eff_keys) |ki| rec[1 + ki] = @bitCast(kv[ki]);
                    // Agg partial contributions — skipped in count_only mode (Phase 2 adds 1).
                    if (!self.count_only) {
                        for (self.agg_infos, 0..) |info, ai| {
                            if (self.omit_first_count and ai == 0) continue;
                            const out_ai = ai - @as(usize, if (self.omit_first_count) 1 else 0);
                            const base_off = 1 + self.n_eff_keys + out_ai;
                            switch (info.kind) {
                                .count => {
                                    rec[base_off] = 1;
                                },
                                .i64_sum => {
                                    if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                        rec[base_off] = 0;
                                    } else {
                                        const ac = c.columns[info.col_idx];
                                        if (ac.isRowNull(r)) {
                                            rec[base_off] = 0;
                                            continue;
                                        }
                                        rec[base_off] = switch (ac.data) {
                                            .int64 => |v| @bitCast(v[r]),
                                            .uint64 => |v| v[r],
                                            .bool_u8 => |v| @as(u64, v[r]),
                                            else => 0,
                                        };
                                    }
                                },
                                .f64_sum => {
                                    if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                        rec[base_off] = 0;
                                    } else {
                                        const ac = c.columns[info.col_idx];
                                        if (ac.isRowNull(r)) {
                                            rec[base_off] = 0;
                                            continue;
                                        }
                                        const fv: f64 = switch (ac.data) {
                                            .int64 => |v| @floatFromInt(v[r]),
                                            .uint64 => |v| @floatFromInt(v[r]),
                                            .bool_u8 => |v| @floatFromInt(v[r]),
                                            .float64 => |v| v[r],
                                            else => 0.0,
                                        };
                                        rec[base_off] = @bitCast(fv);
                                    }
                                },
                                .f64_str_len_sum => {
                                    // Accumulate string length as f64 for AVG(length(col)).
                                    if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                        rec[base_off] = @bitCast(@as(f64, 0.0));
                                    } else {
                                        const ac = c.columns[info.col_idx];
                                        if (ac.isRowNull(r)) {
                                            rec[base_off] = @bitCast(@as(f64, 0.0));
                                            continue;
                                        }
                                        const len_f64: f64 = switch (ac.data) {
                                            .string => |v| @floatFromInt(v[r].len),
                                            else => 0.0,
                                        };
                                        rec[base_off] = @bitCast(len_f64);
                                    }
                                },
                                else => {
                                    rec[base_off] = 0;
                                },
                                .count_distinct_u64 => {
                                    // Store the actual distinct column value so phase 2 can dedup.
                                    // Null rows are encoded as ~0 sentinel; phase 2 skips them.
                                    if (info.col_idx == ~@as(usize, 0) or info.col_idx >= c.columns.len) {
                                        rec[base_off] = ~@as(u64, 0);
                                    } else {
                                        const ac = c.columns[info.col_idx];
                                        rec[base_off] = if (ac.isRowNull(r)) ~@as(u64, 0) else switch (ac.data) {
                                            .int64 => |v| @bitCast(v[r]),
                                            .uint64 => |v| v[r],
                                            else => ~@as(u64, 0),
                                        };
                                    }
                                },
                            }
                        }
                    } // if (!count_only)

                    try self.bufs[part_id].appendSlice(buf_alloc, rec[0..self.row_stride]);
                }
            }
        }
    };

    var morsel_src1 = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);

    var scan_request: ScanRequest = .{};
    scan_request.addItems(keys);
    scan_request.addItems(aggs);
    if (int_filter) |ics| for (ics) |ic| scan_request.addIndex(ic.col_idx);
    if (str_filter) |sf| scan_request.addIndex(sf.col_idx);
    const scan_scope = scan_request.apply(ctx.source, .half);
    defer scan_scope.deinit();

    const scatter_ctxs = try alloc.alloc(ScatterCtx, n_threads);
    for (scatter_ctxs) |*sc| {
        sc.* = .{
            .bufs = @as([N_PARTS]std.ArrayListUnmanaged(u64), @splat(.empty)),
            .buf_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator),
            .source = ctx.source,
            .morsel_src = &morsel_src1,
            .parent_alloc = alloc,
            .n_keys = n_keys,
            .n_eff_keys = n_eff_keys,
            .n_aggs = n_aggs,
            .row_stride = row_stride,
            .key_ci = key_ci,
            .key_offs = key_offs,
            .agg_infos = agg_infos,
            .compact_kinds = compact_kinds,
            .str_filter = str_filter,
            .int_filter = int_filter,
            .count_only = count_only,
            .omit_first_count = omit_first_count,
        };
        // ── Pre-allocate scatter buffers to expected size ──────────────────────────
        // With N_PARTS=128 partitions and exponential doubling from 0, each buffer
        // undergoes ~log2(rows_per_part) doublings. Reserve by per-worker share
        // with slack instead of reserving a full-table partition in every worker.
        // Using c_allocator avoids new page faults on hot runs (memory recycled from
        // previous query run; same rationale as buf_arena using c_allocator).
        {
            const base_expected_rows = total_rows / @max(n_threads, 1) / N_PARTS;
            const expected_rows = if (str_filter != null)
                @max(@as(usize, 1), base_expected_rows / 8)
            else
                base_expected_rows;
            const reserve_mul: usize = if (int_filter != null) 1 else 4;
            const expected_per_part: usize = expected_rows * row_stride * reserve_mul + row_stride * 64;
            const ba = sc.buf_arena.allocator();
            for (&sc.bufs) |*b| {
                b.ensureTotalCapacity(ba, expected_per_part) catch {};
            }
        }
    }
    try parallel.parallelFor(alloc, ScatterCtx, ScatterCtx.work, scatter_ctxs, &morsel_src1);
    for (scatter_ctxs) |*sc| {
        if (sc.err) |e| return e;
    }
    // Note: scatter_ctxs[*].buf_arena must NOT be freed yet — Phase 2 reads from the bufs.

    if (count_only and n_eff_keys == 1 and top_k > 0 and sort_keys.len == 1 and sort_keys[0].desc and
        sort_keys[0].col_idx == n_keys and total_rows <= std.math.maxInt(u32))
    {
        const Candidate = struct { key: i64 = 0, count: u64 = 0 };
        const part_candidates = try alloc.alloc(Candidate, N_PARTS * top_k);
        @memset(part_candidates, .{});

        const FlatPartCtx = struct {
            scatter_ctxs: []ScatterCtx,
            out: []Candidate,
            row_stride: usize,
            k: usize,
            morsel_src: *parallel.MorselSource,
            alloc: std.mem.Allocator,
            err: ?anyerror = null,

            fn addTop(buf: []Candidate, c: Candidate) void {
                if (c.count == 0) return;
                var empty_i: ?usize = null;
                var worst_i: usize = 0;
                for (buf, 0..) |cur, i| {
                    if (cur.count == 0) {
                        empty_i = i;
                        break;
                    }
                    if (cur.count < buf[worst_i].count) worst_i = i;
                }
                if (empty_i) |i| {
                    buf[i] = c;
                } else if (c.count > buf[worst_i].count) {
                    buf[worst_i] = c;
                }
            }

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.runWork() catch |e| {
                    self.err = e;
                };
            }

            fn runWork(self: *@This()) !void {
                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |p| {
                        var total_p: usize = 0;
                        for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / self.row_stride;
                        if (total_p == 0) continue;

                        {
                            var table = try hashmap.HashU64Count.init(self.alloc, total_p);
                            defer table.deinit();
                            var empty_key_count: u64 = 0;
                            for (self.scatter_ctxs) |*sc| {
                                const buf = sc.bufs[p].items;
                                var i: usize = 0;
                                while (i < buf.len) : (i += self.row_stride) {
                                    const key_bits = buf[i + 1];
                                    if (key_bits == hashmap.empty_key) {
                                        empty_key_count += 1;
                                    } else {
                                        table.bump(key_bits);
                                    }
                                }
                            }

                            const out_slice = self.out[p * self.k .. p * self.k + self.k];
                            if (empty_key_count > 0) {
                                addTop(out_slice, .{ .key = @bitCast(hashmap.empty_key), .count = empty_key_count });
                            }
                            var it = table.iterator();
                            while (it.next()) |entry| {
                                addTop(out_slice, .{ .key = @bitCast(entry.key), .count = entry.value });
                            }
                        }
                    }
                }
            }
        };

        var flat_morsels = parallel.MorselSource.init(N_PARTS, 1);
        const flat_ctxs = try alloc.alloc(FlatPartCtx, n_threads);
        for (flat_ctxs) |*fc| {
            fc.* = .{
                .scatter_ctxs = scatter_ctxs,
                .out = part_candidates,
                .row_stride = row_stride,
                .k = top_k,
                .morsel_src = &flat_morsels,
                .alloc = std.heap.c_allocator,
            };
        }
        try parallel.parallelFor(alloc, FlatPartCtx, FlatPartCtx.work, flat_ctxs, &flat_morsels);
        for (flat_ctxs) |*fc| {
            if (fc.err) |e| return e;
        }
        for (scatter_ctxs) |*sc| sc.buf_arena.deinit();

        const out_metas = try buildAggResultMetas(keys, aggs, alloc);
        var rl = RowBuffer.init(out_metas);
        for (part_candidates) |c| {
            if (c.count == 0) continue;
            const row = try alloc.alloc(?Value, n_keys + n_aggs);
            const base_key = c.key -% key_offs[0];
            for (0..n_keys) |ki| {
                const kv = base_key +% key_offs[ki];
                row[ki] = switch (keys[ki].out_type) {
                    .uint64 => Value{ .uint64 = @bitCast(kv) },
                    .date_u16 => Value{ .date_u16 = @truncate(@as(u64, @bitCast(kv))) },
                    .bool_u8 => Value{ .bool_u8 = @truncate(@as(u64, @bitCast(kv))) },
                    else => Value{ .int64 = kv },
                };
            }
            row[n_keys] = .{ .uint64 = c.count };
            try rl.append(alloc, row);
        }
        return try executeTopK(rl, sort_keys, top_k, alloc);
    }

    dense_distinct: {
        if (count_only or n_eff_keys != 1 or n_aggs != 1 or compact_kinds[0] != .count_distinct_u64) break :dense_distinct;
        if (str_filter != null or int_filter != null) break :dense_distinct;
        if (top_k == 0 or sort_keys.len != 1 or !sort_keys[0].desc or sort_keys[0].col_idx != n_keys) break :dense_distinct;

        const sm = ctx.source.schema();
        const key_raw = RawColSlice.resolve(ctx.source, sm, key_ci[0]) orelse break :dense_distinct;
        var min_key: i64 = std.math.maxInt(i64);
        var max_key: i64 = std.math.minInt(i64);
        for (0..@as(usize, @intCast(total_rows))) |row| {
            const key = key_raw.getI64(row) +% key_offs[0];
            min_key = @min(min_key, key);
            max_key = @max(max_key, key);
        }
        if (min_key > max_key) break :dense_distinct;
        const span_i = max_key -% min_key + 1;
        const key_span = std.math.cast(usize, span_i) orelse break :dense_distinct;
        if (key_span == 0 or key_span > 1_000_000) break :dense_distinct;

        const counts = try alloc.alloc(u64, key_span);
        @memset(counts, 0);

        const DenseDistinctCtx = struct {
            scatter_ctxs: []ScatterCtx,
            counts: []u64,
            min_key: i64,
            row_stride: usize,
            morsel_src: *parallel.MorselSource,
            alloc: std.mem.Allocator,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.runWork() catch |e| {
                    self.err = e;
                };
            }

            fn runWork(self: *@This()) !void {
                const DISTINCT_PRIME: u64 = 0x9e3779b97f4a7c15;
                var distinct_set = try hashmap.DistinctEpochSet.init(self.alloc, 32);
                defer distinct_set.deinit();

                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |p| {
                        var total_p: usize = 0;
                        for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / self.row_stride;
                        if (total_p == 0) continue;

                        if (distinct_set.needsGrow()) try distinct_set.growDouble();
                        distinct_set.clearForNextPartition();

                        for (self.scatter_ctxs) |*sc| {
                            const buf = sc.bufs[p].items;
                            var i: usize = 0;
                            while (i < buf.len) : (i += self.row_stride) {
                                const h = buf[i];
                                const key: i64 = @bitCast(buf[i + 1]);
                                const dval = buf[i + 2];
                                if (dval == ~@as(u64, 0)) continue;
                                const pk = h ^ (dval *% DISTINCT_PRIME);
                                if (distinct_set.needsGrow()) try distinct_set.growDouble();
                                if (distinct_set.insertNew(pk)) {
                                    const idx: usize = @intCast(key -% self.min_key);
                                    self.counts[idx] += 1;
                                }
                            }
                        }
                    }
                }
            }
        };

        var dense_morsels = parallel.MorselSource.init(N_PARTS, 1);
        const dense_ctxs = try alloc.alloc(DenseDistinctCtx, n_threads);
        for (dense_ctxs) |*dc| {
            dc.* = .{
                .scatter_ctxs = scatter_ctxs,
                .counts = counts,
                .min_key = min_key,
                .row_stride = row_stride,
                .morsel_src = &dense_morsels,
                .alloc = alloc,
            };
        }
        try parallel.parallelFor(alloc, DenseDistinctCtx, DenseDistinctCtx.work, dense_ctxs, &dense_morsels);
        for (dense_ctxs) |*dc| {
            if (dc.err) |e| return e;
        }
        for (scatter_ctxs) |*sc| sc.buf_arena.deinit();

        const out_metas = try buildAggResultMetas(keys, aggs, alloc);
        var rl = RowBuffer.init(out_metas);
        for (counts, 0..) |count, idx| {
            if (count == 0) continue;
            const row = try alloc.alloc(?Value, n_keys + n_aggs);
            const key = min_key + @as(i64, @intCast(idx));
            row[0] = switch (keys[0].out_type) {
                .uint64 => Value{ .uint64 = @bitCast(key) },
                .date_u16 => Value{ .date_u16 = @truncate(@as(u64, @bitCast(key))) },
                .bool_u8 => Value{ .bool_u8 = @truncate(@as(u64, @bitCast(key))) },
                else => Value{ .int64 = key },
            };
            row[n_keys] = .{ .uint64 = count };
            try rl.append(alloc, row);
        }
        return try executeTopK(rl, sort_keys, top_k, alloc);
    }

    // ── Phase 2: parallel aggregate per partition ─────────────────────────────

    // Allocate output partition HTs (filled by Phase 2 workers).
    const part_hts = try alloc.alloc(ht.CompactIntKeyHashTable, N_PARTS);
    // Initialize all with minimal capacity (Phase 2 will resize as needed).
    for (part_hts) |*ph| {
        ph.* = try ht.CompactIntKeyHashTable.initWithCapacity(alloc, n_eff_keys, n_aggs, 0);
    }

    const AggCtx = struct {
        scatter_ctxs: []ScatterCtx,
        part_hts: []ht.CompactIntKeyHashTable,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        row_stride: usize,
        n_keys: usize,
        n_eff_keys: usize,
        n_aggs: usize,
        count_only: bool,
        omit_first_count: bool,
        morsel_src: *parallel.MorselSource,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            // Check whether any agg needs COUNT(DISTINCT) deduplication.
            var has_distinct: bool = false;
            for (self.compact_kinds) |k| {
                if (k == .count_distinct_u64) {
                    has_distinct = true;
                    break;
                }
            }
            // Per-partition hash set for COUNT(DISTINCT) deduplication.
            // Key = group_hash ^ (distinct_val *% prime) as a single u64.
            // Using u64 instead of u128 halves per-slot memory.
            // Collision probability: ~N^2/2^64 ≈ negligible for practical N.
            // DistinctEpochSet uses O(1) clearForNextPartition (epoch bump) instead
            // of O(capacity) memset, avoiding 256MB+ of clearing overhead across
            // 128 partitions when pre-allocated for worst-case row count.
            const DISTINCT_PRIME: u64 = 0x9e3779b97f4a7c15;
            var distinct_set_storage: hashmap.DistinctEpochSet = undefined;
            var distinct_set_inited = false;
            defer if (distinct_set_inited) distinct_set_storage.deinit();
            if (has_distinct) {
                // Start small (cap=64); grows lazily to actual distinct count.
                distinct_set_storage = try hashmap.DistinctEpochSet.init(self.alloc, 32);
                distinct_set_inited = true;
            }
            const distinct_set: *hashmap.DistinctEpochSet = &distinct_set_storage;

            while (self.morsel_src.next()) |m| {
                for (m.start..m.end) |p| {
                    // Count total rows for this partition across all scatter threads.
                    var total_p: usize = 0;
                    for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / self.row_stride;
                    if (total_p == 0) continue;

                    // Size small HT to fit this partition (should fit in L2 cache).
                    const ht_cap = @max(64, total_p * 100 / 65 + 16);
                    try self.part_hts[p].growTo(ht_cap);

                    // O(1) epoch-bump clear; grow if load factor ≥ 75% from previous partition.
                    if (has_distinct) {
                        if (distinct_set.needsGrow()) try distinct_set.growDouble();
                        distinct_set.clearForNextPartition();
                    }

                    // Aggregate all scatter records for partition p.
                    var key_buf: [4]i64 = undefined;
                    for (self.scatter_ctxs) |*sc| {
                        const buf = sc.bufs[p].items;
                        const rs = self.row_stride;
                        var i: usize = 0;
                        if (self.count_only) {
                            // Fast path: no partial slots → just increment count by 1 per record.
                            // PDIST=8 software prefetch: issue HT slot prefetch 8 records ahead
                            // so DRAM latency is hidden before getOrInsertH touches the slot.
                            const PDIST2: usize = 8;
                            while (i < buf.len) : (i += rs) {
                                if (i + PDIST2 * rs < buf.len)
                                    self.part_hts[p].prefetchH(buf[i + PDIST2 * rs]);
                                const h = buf[i];
                                for (0..self.n_eff_keys) |ki| key_buf[ki] = @bitCast(buf[i + 1 + ki]);
                                const slot_vals = try self.part_hts[p].getOrInsertH(key_buf[0..self.n_eff_keys], h, self.compact_init_vals);
                                slot_vals[0] += 1;
                            }
                        } else {
                            while (i < buf.len) : (i += rs) {
                                const h = buf[i];
                                for (0..self.n_eff_keys) |ki| key_buf[ki] = @bitCast(buf[i + 1 + ki]);
                                const partial = buf[i + 1 + self.n_eff_keys .. i + rs];
                                const slot_vals = try self.part_hts[p].getOrInsertH(key_buf[0..self.n_eff_keys], h, self.compact_init_vals);
                                for (self.compact_kinds, 0..) |kind, ci| {
                                    if (self.omit_first_count and ci == 0) {
                                        slot_vals[ci] += 1;
                                        continue;
                                    }
                                    const partial_ci = ci - @as(usize, if (self.omit_first_count) 1 else 0);
                                    const src = partial[partial_ci];
                                    switch (kind) {
                                        .count, .u64_sum => slot_vals[ci] += src,
                                        .i64_sum => {
                                            const a: i64 = @bitCast(slot_vals[ci]);
                                            const b: i64 = @bitCast(src);
                                            slot_vals[ci] = @bitCast(a + b);
                                        },
                                        .f64_sum => {
                                            const a: f64 = @bitCast(slot_vals[ci]);
                                            const b: f64 = @bitCast(src);
                                            slot_vals[ci] = @bitCast(a + b);
                                        },
                                        .f64_str_len_sum => {
                                            const a: f64 = @bitCast(slot_vals[ci]);
                                            const b: f64 = @bitCast(src);
                                            slot_vals[ci] = @bitCast(a + b);
                                        },
                                        .count_distinct_u64 => {
                                            // src = actual distinct column value (or ~0 for null).
                                            if (src == ~@as(u64, 0)) continue; // null → skip
                                            // Combine (group_hash, distinct_val) into one u64 key.
                                            const pk: u64 = h ^ (src *% DISTINCT_PRIME);
                                            if (distinct_set.needsGrow()) try distinct_set.growDouble();
                                            if (distinct_set.insertNew(pk)) slot_vals[ci] += 1;
                                        },
                                        else => slot_vals[ci] += src,
                                    }
                                }
                            }
                        } // else count_only
                    }
                }
            }
        }
    };

    var morsel_src2 = parallel.MorselSource.init(N_PARTS, 1);
    const agg_ctxs = try alloc.alloc(AggCtx, n_threads);
    for (agg_ctxs) |*ac| {
        ac.* = .{
            .scatter_ctxs = scatter_ctxs,
            .part_hts = part_hts,
            .compact_kinds = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .row_stride = row_stride,
            .n_keys = n_keys,
            .n_eff_keys = n_eff_keys,
            .n_aggs = n_aggs,
            .count_only = count_only,
            .omit_first_count = omit_first_count,
            .morsel_src = &morsel_src2,
            .alloc = alloc,
        };
    }
    try parallel.parallelFor(alloc, AggCtx, AggCtx.work, agg_ctxs, &morsel_src2);
    for (agg_ctxs) |*ac| {
        if (ac.err) |e| return e;
    }
    // Phase 2 done — release scatter buf memory (raw_c_allocator-backed → returned to malloc pool).
    for (scatter_ctxs) |*sc| sc.buf_arena.deinit();

    // ── Emit from partition HTs ────────────────────────────────────────────────

    const key_out_types_buf = try alloc.alloc(ColumnType, n_keys);
    for (keys, 0..) |k, i| key_out_types_buf[i] = k.out_type;
    const out_metas = try buildAggResultMetas(keys, aggs, alloc);

    var rl = RowBuffer.init(out_metas);
    const EmitCtx = struct {
        keys_n: usize,
        aggs_n: usize,
        n_eff_keys: usize, // =1 when same-column collapsing active
        key_offs_emit: [4]i64, // key_offs for reconstructing collapsed keys
        compact_kinds: []const ht.CompactAggKind,
        aggs: []const plan.ProjectItem,
        rl: *RowBuffer,
        alloc: std.mem.Allocator,
        key_out_types: []const ColumnType,
        heap: ?[][]?Value = null,
        heap_len: usize = 0,
        heap_k: usize = 0,
        sort_keys: []const plan.SortKey = &.{},
        heap_min_cached: i64 = std.math.minInt(i64),
        err: ?anyerror = null,

        fn heapSiftDown(self: *@This(), i: usize) void {
            var cur = i;
            while (true) {
                var worst = cur;
                const l = cur * 2 + 1;
                const r = cur * 2 + 2;
                if (l < self.heap_len and rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[l])) worst = l;
                if (r < self.heap_len and rowLessThan(self.sort_keys, self.heap.?[worst], self.heap.?[r])) worst = r;
                if (worst == cur) break;
                const tmp = self.heap.?[cur];
                self.heap.?[cur] = self.heap.?[worst];
                self.heap.?[worst] = tmp;
                cur = worst;
            }
        }
        fn heapSiftUp(self: *@This(), i: usize) void {
            var cur = i;
            while (cur > 0) {
                const parent = (cur - 1) / 2;
                if (rowLessThan(self.sort_keys, self.heap.?[parent], self.heap.?[cur])) {
                    const tmp = self.heap.?[cur];
                    self.heap.?[cur] = self.heap.?[parent];
                    self.heap.?[parent] = tmp;
                    cur = parent;
                } else break;
            }
        }
        fn updateHeapMinCache(self: *@This()) void {
            if (self.heap_len == 0 or self.sort_keys.len == 0) return;
            const heap = self.heap.?;
            const ci = self.sort_keys[0].col_idx;
            if (ci >= heap[0].len) return;
            self.heap_min_cached = if (heap[0][ci]) |v| switch (v) {
                .int64 => |x| x,
                .uint64 => |x| @bitCast(x),
                .float64 => |x| @as(i64, @bitCast(x)),
                else => std.math.minInt(i64),
            } else std.math.minInt(i64);
        }
        fn makeRow(self: *@This(), key_vals: []const i64, acc_vals: []const u64) ?[]?Value {
            const row = self.alloc.alloc(?Value, self.keys_n + self.aggs_n) catch return null;
            if (self.n_eff_keys < self.keys_n and key_vals.len > 0) {
                // Same-column key collapsing: reconstruct all keys from the single stored key.
                // stored eff key = raw_col +% key_offs_emit[0]; full_kv[i] = raw_col +% key_offs_emit[i]
                const eff_base = key_vals[0];
                for (0..self.keys_n) |i| {
                    const full_kv = eff_base -% self.key_offs_emit[0] +% self.key_offs_emit[i];
                    const out_type: ColumnType = if (i < self.key_out_types.len) self.key_out_types[i] else .int64;
                    row[i] = switch (out_type) {
                        .datetime64_ms => .{ .datetime64_ms = full_kv },
                        .date_u16 => .{ .date_u16 = @intCast(full_kv) },
                        else => .{ .int64 = full_kv },
                    };
                }
            } else {
                for (key_vals, 0..) |kv, i| {
                    const out_type: ColumnType = if (i < self.key_out_types.len) self.key_out_types[i] else .int64;
                    row[i] = switch (out_type) {
                        .datetime64_ms => .{ .datetime64_ms = kv },
                        .date_u16 => .{ .date_u16 = @intCast(kv) },
                        else => .{ .int64 = kv },
                    };
                }
            }
            for (self.compact_kinds, 0..) |kind, i| {
                row[self.keys_n + i] = switch (kind) {
                    .count => .{ .int64 = @intCast(acc_vals[i]) },
                    .i64_sum => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_sum => .{ .uint64 = acc_vals[i] },
                    .f64_sum, .f64_str_len_sum => blk: {
                        const sum: f64 = @bitCast(acc_vals[i]);
                        if (i < self.aggs.len and self.aggs[i].expr == .agg_call and
                            self.aggs[i].expr.agg_call.kind == .avg)
                        {
                            for (self.compact_kinds, 0..) |ck, j| {
                                if (ck == .count) {
                                    const cnt = acc_vals[j];
                                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                                    break;
                                }
                            }
                        }
                        break :blk Value{ .float64 = sum };
                    },
                    .i64_min, .i64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .u64_min, .u64_max => .{ .int64 = @bitCast(acc_vals[i]) },
                    .f64_min, .f64_max => .{ .float64 = @bitCast(acc_vals[i]) },
                    .str_min, .str_max => .{ .int64 = 0 },
                    .count_distinct_u64 => .{ .uint64 = acc_vals[i] },
                };
            }
            return row;
        }
        fn cb(self: *@This(), key_vals: []const i64, acc_vals: []const u64) void {
            if (self.heap) |heap| {
                if (self.heap_len >= self.heap_k and self.sort_keys.len > 0) {
                    const sk = self.sort_keys[0];
                    const ci = sk.col_idx;
                    const new_raw: i64 = blk: {
                        if (ci < self.keys_n) {
                            // Handle same-column collapsing: reconstruct key from eff_base.
                            if (self.n_eff_keys < self.keys_n and key_vals.len > 0) {
                                break :blk key_vals[0] -% self.key_offs_emit[0] +% self.key_offs_emit[ci];
                            }
                            break :blk if (ci < key_vals.len) key_vals[ci] else 0;
                        }
                        const ai = ci - self.keys_n;
                        if (ai < self.compact_kinds.len) {
                            break :blk switch (self.compact_kinds[ai]) {
                                .count => @intCast(acc_vals[ai]),
                                .i64_sum, .i64_min, .i64_max, .u64_min, .u64_max => @bitCast(acc_vals[ai]),
                                else => std.math.maxInt(i64),
                            };
                        }
                        break :blk std.math.maxInt(i64);
                    };
                    const qualifies = if (sk.desc) new_raw > self.heap_min_cached else new_raw < self.heap_min_cached;
                    if (!qualifies) return;
                }
                const row = self.makeRow(key_vals, acc_vals) orelse {
                    self.err = error.OutOfMemory;
                    return;
                };
                if (self.heap_len < self.heap_k) {
                    heap[self.heap_len] = row;
                    self.heap_len += 1;
                    self.heapSiftUp(self.heap_len - 1);
                    self.updateHeapMinCache();
                } else if (rowLessThan(self.sort_keys, row, heap[0])) {
                    heap[0] = row;
                    self.heapSiftDown(0);
                    self.updateHeapMinCache();
                }
            } else {
                const row = self.makeRow(key_vals, acc_vals) orelse {
                    self.err = error.OutOfMemory;
                    return;
                };
                self.rl.append(self.alloc, row) catch |e| {
                    self.err = e;
                };
            }
        }
    };

    const use_heap = top_k > 0 and sort_keys.len > 0;
    const heap_buf: ?[][]?Value = if (use_heap) try alloc.alloc([]?Value, top_k) else null;
    var emit_ctx = EmitCtx{
        .keys_n = n_keys,
        .aggs_n = n_aggs,
        .n_eff_keys = n_eff_keys,
        .key_offs_emit = key_offs,
        .compact_kinds = compact_kinds,
        .aggs = aggs,
        .rl = &rl,
        .alloc = alloc,
        .key_out_types = key_out_types_buf,
        .heap = heap_buf,
        .heap_len = 0,
        .heap_k = top_k,
        .sort_keys = sort_keys,
    };
    for (part_hts) |*ph| {
        ph.iterate(&emit_ctx, EmitCtx.cb);
        if (emit_ctx.err) |e| return e;
    }

    if (use_heap) {
        const heap_rows = emit_ctx.heap.?[0..emit_ctx.heap_len];
        std.sort.pdq([]?Value, heap_rows, TopKSortCtx{ .keys = sort_keys }, TopKSortCtx.lessThan);
        var result_rl = RowBuffer.init(out_metas);
        for (heap_rows) |row| try result_rl.append(alloc, row);
        return result_rl;
    }

    return rl;
}

pub const ChunkRowSink = struct {
    sink: *chunk_sink_mod.ChunkSink,
    fn append(self: *ChunkRowSink, _: std.mem.Allocator, row: []const ?Value) !void {
        try self.sink.append(row);
    }
};
