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

pub fn finalizeAggRows(
    rows: RowBuffer,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    alloc: std.mem.Allocator,
) !RowBuffer {
    if (sort_keys.len == 0) return rows;
    if (top_k > 0 and rows.rows.items.len > top_k) return executeTopK(rows, sort_keys, top_k, alloc);
    return executeOrderBy(rows, sort_keys, alloc);
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

pub fn emitCompactVals(
    vals: []const u64,
    kinds: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    out: []?Value,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max, .count_distinct_u64 => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum, .f64_str_len_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) {
                            cnt = cv;
                            break;
                        }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            // str_min/str_max: emitted via sidecar; return empty string as sentinel.
            .str_min, .str_max => Value{ .string = "" },
        };
    }
}

pub fn emitCompactValsWithSidecar(
    vals: []const u64,
    kinds: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    out: []?Value,
    str_ht: *const ht.StrAggHashTable,
    slot: usize,
    sidecar_idx: []const usize,
) void {
    for (vals, kinds, aggs, 0..) |v, kind, item, i| {
        out[i] = switch (kind) {
            .count, .u64_sum, .u64_min, .u64_max, .count_distinct_u64 => Value{ .uint64 = v },
            .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
            .f64_sum, .f64_str_len_sum => blk: {
                const sum: f64 = @bitCast(v);
                if (item.expr == .agg_call and item.expr.agg_call.kind == .avg) {
                    var cnt: u64 = 0;
                    for (vals, kinds) |cv, ck| {
                        if (ck == .count) {
                            cnt = cv;
                            break;
                        }
                    }
                    if (cnt > 0) break :blk Value{ .float64 = sum / @as(f64, @floatFromInt(cnt)) };
                }
                break :blk Value{ .float64 = sum };
            },
            .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
            .str_min, .str_max => blk: {
                const s = str_ht.getStrSidecar(slot, sidecar_idx[i]) orelse "";
                break :blk Value{ .string = s };
            },
        };
    }
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

pub fn executeHashAggParallelPairCountBounded(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    limit: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (limit == 0 or limit > 1024) return null;
    if (input.* != .part_scan and input.* != .mem_scan) return null;
    if (keys.len != 2 or aggs.len != 1) return null;
    if (aggs[0].expr != .agg_call or aggs[0].expr.agg_call.kind != .count_star) return null;
    for (keys) |k| if (k.expr != .col_ref) return null;

    const sm = ctx.source.schema();
    const ci0 = keys[0].expr.col_ref.index;
    const ci1 = keys[1].expr.col_ref.index;
    if (ci0 >= sm.len or ci1 >= sm.len) return null;
    const t0 = sm[ci0].col_type;
    const t1 = sm[ci1].col_type;
    const i64_ci: usize = blk: {
        if (t0 == .int64 and t1 == .string) break :blk ci0;
        if (t1 == .int64 and t0 == .string) break :blk ci1;
        return null;
    };
    const str_ci: usize = if (i64_ci == ci0) ci1 else ci0;
    const k0_is_i64 = (i64_ci == ci0);

    var scan_request: ScanRequest = .{};
    scan_request.addIndex(i64_ci);
    scan_request.addIndex(str_ci);
    var scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, ctx.allocator()) catch {};
    }
    const raw_i64 = ctx.source.getRawInt64Col(sm[i64_ci].name) orelse return null;
    const raw_offs = ctx.source.getRawStrOffsets(sm[str_ci].name) orelse return null;
    const raw_bytes = ctx.source.getRawStrBytes(sm[str_ci].name) orelse return null;
    const total_rows: usize = @intCast(ctx.source.rowCount());
    if (total_rows == 0 or raw_offs.len < total_rows + 1) return null;

    const alloc = ctx.allocator();
    const out_metas = try buildAggResultMetas(keys, aggs, alloc);

    // Choose up to K keys from a small prefix. This is valid because LIMIT
    // without ORDER BY may return any K groups.
    var key_table = try ht.PairCountHashTable.initWithCapacity(alloc, @as(u64, limit) * 4);
    const sample_n = @min(total_rows, 8192);
    for (0..sample_n) |row| {
        const lo: usize = @intCast(raw_offs[row]);
        const hi: usize = @intCast(raw_offs[row + 1]);
        if (hi > raw_bytes.len or lo > hi) continue;
        _ = key_table.incrementBounded(raw_i64[row], raw_bytes[lo..hi], limit);
    }
    if (key_table.count == 0) return RowBuffer.init(out_metas);
    key_table.resetCounts();

    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const local = try alloc.alloc(ht.PairCountHashTable, n_threads);
    for (local) |*t| t.* = try key_table.cloneForCounting(alloc);

    const Worker = struct {
        raw_i64: []const i64,
        raw_offs: []const u64,
        raw_bytes: []const u8,
        table: *ht.PairCountHashTable,
        err: ?anyerror = null,
        fn work(self: *@This(), morsel_src: *parallel.MorselSource) void {
            while (morsel_src.next()) |m| {
                for (m.start..m.end) |row| {
                    const lo: usize = @intCast(self.raw_offs[row]);
                    const hi: usize = @intCast(self.raw_offs[row + 1]);
                    if (hi > self.raw_bytes.len or lo > hi) continue;
                    _ = self.table.incrementExisting(self.raw_i64[row], self.raw_bytes[lo..hi]);
                }
            }
        }
    };
    const workers = try alloc.alloc(Worker, n_threads);
    for (workers, 0..) |*w, i| {
        w.* = .{
            .raw_i64 = raw_i64,
            .raw_offs = raw_offs,
            .raw_bytes = raw_bytes,
            .table = &local[i],
        };
    }
    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    try parallel.parallelFor(alloc, Worker, Worker.work, workers, &morsel_src);
    for (workers) |w| if (w.err) |e| return e;
    for (local) |*t| key_table.addCountsFrom(t);

    var rl = RowBuffer.init(out_metas);
    const Emit = struct {
        rl: *RowBuffer,
        alloc: std.mem.Allocator,
        k0_is_i64: bool,
        fn cb(self: *@This(), i64_key: i64, str_key: []const u8, count: u64) void {
            const row = self.alloc.alloc(?Value, 3) catch return;
            if (self.k0_is_i64) {
                row[0] = .{ .int64 = i64_key };
                row[1] = .{ .string = str_key };
            } else {
                row[0] = .{ .string = str_key };
                row[1] = .{ .int64 = i64_key };
            }
            row[2] = .{ .uint64 = count };
            self.rl.append(self.alloc, row) catch {};
        }
    };
    var emit = Emit{ .rl = &rl, .alloc = alloc, .k0_is_i64 = k0_is_i64 };
    key_table.iterate(&emit, Emit.cb);
    return rl;
}

pub fn executeHashAggParallelPairCount(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 1_000_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const alloc = ctx.allocator();

    // Only handle unfiltered scans (no WHERE clause support yet in this path).
    if (input.* != .part_scan and input.* != .mem_scan) return null;

    // LIMIT without ORDER BY: only K groups are needed. Choose the keys from a
    // tiny sample and count them in a parallel scan instead of building a table
    // over every distinct pair.
    if (top_k > 0 and sort_keys.len == 0) {
        if (try executeHashAggParallelPairCountBounded(input, keys, aggs, top_k, ctx)) |rl| return rl;
    }

    // Guard: exactly 2 col_ref keys, exactly 1 agg (count(*)).
    if (keys.len != 2) return null;
    if (aggs.len != 1) return null;
    if (aggs[0].expr != .agg_call) return null;
    if (aggs[0].expr.agg_call.kind != .count_star) return null;
    for (keys) |k| {
        if (k.expr != .col_ref) return null;
    }

    // Identify i64 and string key column indices from schema.
    const sm = ctx.source.schema();
    const ci0 = keys[0].expr.col_ref.index;
    const ci1 = keys[1].expr.col_ref.index;
    if (ci0 >= sm.len or ci1 >= sm.len) return null;
    const t0 = sm[ci0].col_type;
    const t1 = sm[ci1].col_type;

    const i64_ci: usize = blk: {
        if ((t0 == .int64 or t0 == .uint64) and t1 == .string) break :blk ci0;
        if ((t1 == .int64 or t1 == .uint64) and t0 == .string) break :blk ci1;
        return null;
    };
    const str_ci: usize = if (i64_ci == ci0) ci1 else ci0;
    const k0_is_i64 = (i64_ci == ci0);

    // Narrow-int check: skip for low-cardinality int columns.
    if (sm[i64_ci].is_narrow_int) return null;

    var scan_request: ScanRequest = .{};
    scan_request.addIndex(i64_ci);
    scan_request.addIndex(str_ci);
    var scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    // Preload columns.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    // Each worker scans a morsel range and builds a local HT.
    // After all workers finish, we merge the per-worker HTs serially.
    // This is the correct parallel hash-agg pattern: divide rows by morsel,
    // each worker aggregates its shard, then merge results by summing counts.
    const Worker = struct {
        source: SourceIface,
        morsel_src: *parallel.MorselSource, // shared; each morsel assigned to one worker
        parent_alloc: std.mem.Allocator,
        ht_alloc: std.mem.Allocator, // allocator for local_ht (c_allocator-backed)
        est_per_thread_: u64, // capacity hint for HT init (done in runWork)
        i64_ci: usize,
        str_ci: usize,
        local_ht: ht.PairCountHashTable = undefined,
        err: ?anyerror = null,
        // Raw mmap slices for skip-fetchRange fast path.
        // When both are set, we bypass fetchRange and its arena allocation entirely:
        // strings come from the mmap'd bytes (always valid), int64 from the raw slice.
        raw_i64: ?[]const i64 = null,
        raw_str_offsets: ?[]const u64 = null,
        raw_str_bytes: ?[]const u8 = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            // Initialize local HT IN the worker thread so all 8 @memset ops run in
            // parallel, eliminating the serial page-fault bottleneck in the main thread
            // (8 × 80 MB = 640 MB of zeroing previously done single-threaded).
            self.local_ht = try ht.PairCountHashTable.initWithCapacity(self.ht_alloc, self.est_per_thread_);

            // ── Fast path: all data available as raw mmap slices ─────────────────
            // Eliminates fetchRange overhead (~2.9 MB per morsel of arena allocations)
            // and the ~1.9 GB total query-arena accumulation across all morsels.
            // Strings stored in local_ht point into mmap'd bytes — always valid.
            // Software prefetch: for each row, issue prefetch for the HT bucket of
            // a row PDIST iterations ahead.  On M2 (DRAM latency ~100 ns, loop ~10 ns),
            // PDIST=16 keeps ~16 outstanding DRAM requests → hides most of the latency.
            if (self.raw_i64 != null and self.raw_str_offsets != null and self.raw_str_bytes != null) {
                const ri64 = self.raw_i64.?;
                const rso = self.raw_str_offsets.?;
                const rsb = self.raw_str_bytes.?;
                const PDIST: usize = 16;
                const mask = self.local_ht.capacity - 1;
                while (self.morsel_src.next()) |m| {
                    // Prime the prefetch pipeline: issue prefetches for the first PDIST rows.
                    const pre_end = @min(m.start + PDIST, m.end);
                    for (m.start..pre_end) |pfabs| {
                        const pfs = rsb[rso[pfabs]..rso[pfabs + 1]];
                        const pfh = ht.PairCountHashTable.tagHash(ht.PairCountHashTable.hashPair(ri64[pfabs], pfs));
                        @prefetch(&self.local_ht.slots[pfh & mask], .{ .rw = .read, .locality = 3, .cache = .data });
                    }
                    for (m.start..m.end) |abs| {
                        // Rolling prefetch: abs+PDIST is issued before processing abs.
                        if (abs + PDIST < m.end) {
                            const pfabs = abs + PDIST;
                            const pfs = rsb[rso[pfabs]..rso[pfabs + 1]];
                            const pfh = ht.PairCountHashTable.tagHash(ht.PairCountHashTable.hashPair(ri64[pfabs], pfs));
                            @prefetch(&self.local_ht.slots[pfh & mask], .{ .rw = .read, .locality = 3, .cache = .data });
                        }
                        const s = rsb[rso[abs]..rso[abs + 1]];
                        const h = ht.PairCountHashTable.tagHash(ht.PairCountHashTable.hashPair(ri64[abs], s));
                        try self.local_ht.incrementPrehashed(ri64[abs], s, h);
                    }
                }
                return;
            }

            // ── Fallback: fetchRange path (columns not available as raw slices) ──
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            while (self.morsel_src.next()) |m| {
                // Use talloc (thread-level arena) so strings in local_ht remain valid
                // for the entire thread lifetime (needed since local_ht stores raw slices).
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, talloc);

                if (self.i64_ci >= c.columns.len or self.str_ci >= c.columns.len) continue;
                const strs = c.columns[self.str_ci].data.string;

                switch (c.columns[self.i64_ci].data) {
                    .int64 => |ints| {
                        for (0..c.num_rows) |r| try self.local_ht.increment(ints[r], strs[r]);
                    },
                    .uint64 => |ints| {
                        for (0..c.num_rows) |r| try self.local_ht.increment(@bitCast(ints[r]), strs[r]);
                    },
                    else => {},
                }
            }
        }
    };

    // Estimate per-thread HT size: total / n_threads (each thread processes ~1/n_threads rows).
    const est_per_thread: u64 = @max(64, total_rows / @as(u64, n_threads));

    // Pre-fetch raw mmap slices for the fast skip-fetchRange path.
    // Requires the store to expose contiguous int64 + string mmap'd column data.
    // When both are available, Worker.runWork skips fetchRange entirely, eliminating
    // ~2.9 MB per morsel of arena allocations (~1.9 GB total across all threads/morsels).
    const raw_i64_pre = ctx.source.getRawInt64Col(sm[i64_ci].name);
    const raw_str_offsets_pre = ctx.source.getRawStrOffsets(sm[str_ci].name);
    const raw_str_bytes_pre = ctx.source.getRawStrBytes(sm[str_ci].name);

    // ── Two-phase scatter approach for Q17/Q18 ────────────────────────────────
    // Phase 1: scatter (hash, i64_key, str_start, str_len) records into N_P2=128
    //          partitions.  No per-row HT lookups → no cache thrashing.
    //          Each 24-byte record references string bytes by offset into raw_str_bytes.
    // Phase 2: aggregate per partition into a small (~15-30 K slot) HT.
    //          Small HT (~1-4 MB) fits in L2 cache → most lookups are L2 hits.
    //          Sequential scatter-buffer reads → hardware prefetch handles bandwidth.
    // This replaces the per-thread 160 MB PairCountHashTable (4 M slots × 40 bytes)
    // whose random accesses saturate DRAM even with software prefetch.
    if (raw_i64_pre != null and raw_str_offsets_pre != null and raw_str_bytes_pre != null) {
        const ri64 = raw_i64_pre.?;
        const rso = raw_str_offsets_pre.?;
        const rsb = raw_str_bytes_pre.?;

        const N_P2: usize = 128;
        // Scatter record: 24 bytes (3 × u64 layout for alignment convenience).
        // Fields: hash (u64), i64_key (as u64 bits), str_meta (str_start<<32 | str_len).
        const P2Rec = extern struct { hash: u64, i64_key_bits: u64, str_meta: u64 };

        const P2Scatter = struct {
            bufs: [N_P2]std.ArrayListUnmanaged(P2Rec),
            buf_arena: std.heap.ArenaAllocator,
            morsel_src: *parallel.MorselSource,
            ri64: []const i64,
            rso: []const u64,
            rsb: []const u8,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.runWork() catch |e| {
                    self.err = e;
                };
            }

            fn runWork(self: *@This()) !void {
                const ba = self.buf_arena.allocator();
                const PDIST3: usize = 24;
                while (self.morsel_src.next()) |m| {
                    // Prime prefetch pipeline for first PDIST3 str offsets.
                    for (m.start..@min(m.start + PDIST3, m.end)) |pf|
                        @prefetch(&self.rso[pf], .{ .rw = .read, .locality = 0, .cache = .data });
                    for (m.start..m.end) |row| {
                        if (row + PDIST3 < m.end)
                            @prefetch(&self.rso[row + PDIST3], .{ .rw = .read, .locality = 0, .cache = .data });
                        const iv = self.ri64[row];
                        const ss: u32 = @truncate(self.rso[row]);
                        const sl: u32 = @truncate(self.rso[row + 1] - self.rso[row]);
                        const s = self.rsb[ss .. ss + sl];
                        const h = ht.PairCountHashTable.tagHash(ht.PairCountHashTable.hashPair(iv, s));
                        const p = @as(usize, @truncate(h)) & (N_P2 - 1);
                        try self.bufs[p].append(ba, .{
                            .hash = h,
                            .i64_key_bits = @bitCast(iv),
                            .str_meta = (@as(u64, ss) << 32) | sl,
                        });
                    }
                }
            }
        };

        const sc2_ctxs = try alloc.alloc(P2Scatter, n_threads);
        for (sc2_ctxs) |*sc| {
            sc.* = .{
                .bufs = @as([N_P2]std.ArrayListUnmanaged(P2Rec), @splat(.empty)),
                .buf_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator),
                .morsel_src = undefined,
                .ri64 = ri64,
                .rso = rso,
                .rsb = rsb,
            };
            const ba = sc.buf_arena.allocator();
            const exp: usize = total_rows / @max(n_threads, 1) / N_P2 * 2 + 64;
            for (&sc.bufs) |*b| b.ensureTotalCapacity(ba, exp) catch {};
        }
        var ms2 = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
        for (sc2_ctxs) |*sc| sc.morsel_src = &ms2;
        try parallel.parallelFor(alloc, P2Scatter, P2Scatter.work, sc2_ctxs, &ms2);
        for (sc2_ctxs) |sc| {
            if (sc.err) |e| return e;
        }

        // Phase 2: aggregate per partition into a small HT.
        // Slot: 32 bytes (hash + i64_key + str_start + str_len + count + padding).
        const P2Slot = extern struct {
            hash: u64 = 0, // 0 = empty
            i64_key: i64 = 0,
            str_start: u32 = 0,
            str_len: u32 = 0,
            count: u64 = 0,
        };
        comptime std.debug.assert(@sizeOf(P2Slot) == 32);

        const P2AggCtx = struct {
            sc2_ctxs: []P2Scatter,
            part_slots: [][]P2Slot, // per-partition aggregated HT arrays
            part_caps: []usize, // per-partition allocated capacity (= ht_cap used)
            morsel_src: *parallel.MorselSource,
            rsb: []const u8,
            slot_arena: std.heap.ArenaAllocator, // per-worker; no sharing
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.doWork() catch |e| {
                    self.err = e;
                };
            }

            fn doWork(self: *@This()) !void {
                const sa = self.slot_arena.allocator();
                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |p| {
                        // Count total records for this partition.
                        var total_p: usize = 0;
                        for (self.sc2_ctxs) |*sc| total_p += sc.bufs[p].items.len;
                        if (total_p == 0) {
                            self.part_slots[p] = &.{};
                            continue;
                        }

                        // Size HT to fit partition (target load ≤ 65%).
                        const ht_cap = blk: {
                            var c: usize = 64;
                            const need = total_p * 100 / 65 + 2;
                            while (c < need) c <<= 1;
                            break :blk c;
                        };

                        // Allocate slot array from per-worker arena (thread-safe: no sharing).
                        const slots = try sa.alloc(P2Slot, ht_cap);
                        @memset(std.mem.sliceAsBytes(slots), 0); // zero hash = empty
                        self.part_slots[p] = slots;
                        self.part_caps[p] = ht_cap;
                        const mask = ht_cap - 1;

                        // Aggregate all scatter records for partition p.
                        // hashPair already encodes (i64_key ++ string_bytes) via Wyhash,
                        // so hash-only comparison is correct (collision prob ~1/2^64).
                        for (self.sc2_ctxs) |*sc| {
                            const items = sc.bufs[p].items;
                            const n = items.len;
                            const PDIST: usize = 8;
                            for (items, 0..) |rec, ri| {
                                // Prefetch slot for a future record.
                                if (ri + PDIST < n) {
                                    const fh = items[ri + PDIST].hash;
                                    const fi = @as(usize, @truncate(fh)) & mask;
                                    @prefetch(&slots[fi], .{ .rw = .read, .locality = 3, .cache = .data });
                                }
                                const h = rec.hash;
                                const iv: i64 = @bitCast(rec.i64_key_bits);
                                const ss: u32 = @truncate(rec.str_meta >> 32);
                                const sl: u32 = @truncate(rec.str_meta);
                                var slot_idx = @as(usize, @truncate(h)) & mask;
                                while (true) : (slot_idx = (slot_idx + 1) & mask) {
                                    const s = &slots[slot_idx];
                                    if (s.hash == 0) {
                                        s.* = .{ .hash = h, .i64_key = iv, .str_start = ss, .str_len = sl, .count = 1 };
                                        break;
                                    }
                                    if (s.hash == h) {
                                        // hashPair encodes both keys; no memcmp needed.
                                        s.count += 1;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        const metas = try alloc.alloc(result.ColMeta, 3);
        metas[0] = if (k0_is_i64) result.ColMeta{ .name = keys[0].alias, .col_type = .int64 } else result.ColMeta{ .name = keys[0].alias, .col_type = .string };
        metas[1] = if (k0_is_i64) result.ColMeta{ .name = keys[1].alias, .col_type = .string } else result.ColMeta{ .name = keys[1].alias, .col_type = .int64 };
        metas[2] = result.ColMeta{ .name = aggs[0].alias, .col_type = .uint64 };
        var rl2 = RowBuffer.init(metas);

        // Pre-allocate per-partition result arrays (all start empty; doWork fills them).
        const part_slots2 = try alloc.alloc([]P2Slot, N_P2);
        const part_caps2 = try alloc.alloc(usize, N_P2);
        for (part_slots2) |*ps| ps.* = &.{};
        @memset(part_caps2, 0);

        // Per-worker slot arenas (c_allocator backed, no page-fault penalty on hot runs).
        const slot_arenas2 = try alloc.alloc(std.heap.ArenaAllocator, n_threads);
        for (slot_arenas2) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);

        var ms2b = parallel.MorselSource.init(N_P2, 1);
        const agg2_ctxs = try alloc.alloc(P2AggCtx, n_threads);
        for (agg2_ctxs, 0..) |*ac, ti| {
            ac.* = .{
                .sc2_ctxs = sc2_ctxs,
                .part_slots = part_slots2,
                .part_caps = part_caps2,
                .morsel_src = &ms2b,
                .rsb = rsb,
                .slot_arena = slot_arenas2[ti],
            };
        }
        try parallel.parallelFor(alloc, P2AggCtx, P2AggCtx.work, agg2_ctxs, &ms2b);
        for (agg2_ctxs) |ac| {
            if (ac.err) |e| return e;
        }

        // Serial emit: collect results from all partition HTs into RowBuffer.
        // If this path is driven by LIMIT(hash_agg(...)) without ORDER BY, the
        // aggregate has already seen all rows, so we can materialize only N groups.
        const unordered_limit = top_k > 0 and sort_keys.len == 0;
        const count_top = top_k > 0 and sort_keys.len == 1 and sort_keys[0].desc and sort_keys[0].col_idx == 2;
        const P2Out = struct {
            i64_key: i64,
            str_start: u32,
            str_len: u32,
            count: u64,

            fn appendTo(self: @This(), out: *RowBuffer, allocator: std.mem.Allocator, bytes: []const u8, int_first: bool) !void {
                const str = bytes[self.str_start .. self.str_start + self.str_len];
                const row = try allocator.alloc(?Value, 3);
                if (int_first) {
                    row[0] = Value{ .int64 = self.i64_key };
                    row[1] = Value{ .string = str };
                } else {
                    row[0] = Value{ .string = str };
                    row[1] = Value{ .int64 = self.i64_key };
                }
                row[2] = Value{ .uint64 = self.count };
                try out.append(allocator, row);
            }
        };
        const top_buf: ?[]P2Out = if (count_top) try alloc.alloc(P2Out, top_k) else null;
        var top_len: usize = 0;
        var emitted_p2: usize = 0;
        p2_emit: for (0..N_P2) |p| {
            const slots = part_slots2[p];
            if (slots.len == 0) continue;
            for (slots[0..part_caps2[p]]) |s| {
                if (s.hash == 0) continue;
                if (top_buf) |tb| {
                    const cand: P2Out = .{ .i64_key = s.i64_key, .str_start = s.str_start, .str_len = s.str_len, .count = s.count };
                    if (top_len < tb.len) {
                        tb[top_len] = cand;
                        top_len += 1;
                    } else {
                        var min_i: usize = 0;
                        for (tb[1..], 1..) |v, i| {
                            if (v.count < tb[min_i].count) min_i = i;
                        }
                        if (cand.count > tb[min_i].count) tb[min_i] = cand;
                    }
                    continue;
                }
                try (P2Out{ .i64_key = s.i64_key, .str_start = s.str_start, .str_len = s.str_len, .count = s.count }).appendTo(&rl2, alloc, rsb, k0_is_i64);
                emitted_p2 += 1;
                if (unordered_limit and emitted_p2 >= top_k) break :p2_emit;
            }
        }
        if (top_buf) |tb| {
            const SortTop = struct {
                fn lessThan(_: void, a: P2Out, b: P2Out) bool {
                    return a.count > b.count;
                }
            };
            std.sort.pdq(P2Out, tb[0..top_len], {}, SortTop.lessThan);
            for (tb[0..top_len]) |s| {
                try s.appendTo(&rl2, alloc, rsb, k0_is_i64);
            }
        }

        // Free scatter and slot arenas now that collection is complete.
        for (sc2_ctxs) |*sc| sc.buf_arena.deinit();
        for (slot_arenas2) |*a| a.deinit();

        if (top_k > 0 and sort_keys.len > 0 and rl2.rows.items.len > top_k) {
            return try executeTopK(rl2, sort_keys, top_k, alloc);
        }
        return rl2;
    }

    // Each worker gets its own arena-backed HT.
    // Use c_allocator (malloc) instead of page_allocator to avoid page-fault overhead
    // on freshly allocated large pages during the merge phase.
    // NOTE: HT initialization (@memset of ~80 MB per worker) is done INSIDE runWork so
    // all 8 workers zero their HTs in parallel, not serially in the main thread.
    const ht_arenas = try alloc.alloc(std.heap.ArenaAllocator, n_threads);
    for (ht_arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer for (ht_arenas) |*a| a.deinit();

    const workers = try alloc.alloc(Worker, n_threads);
    for (workers, 0..) |*w, t| {
        w.* = .{
            .source = ctx.source,
            .morsel_src = undefined,
            .parent_alloc = alloc,
            .ht_alloc = ht_arenas[t].allocator(),
            .est_per_thread_ = est_per_thread,
            .i64_ci = i64_ci,
            .str_ci = str_ci,
            .err = null,
            .raw_i64 = raw_i64_pre,
            .raw_str_offsets = raw_str_offsets_pre,
            .raw_str_bytes = raw_str_bytes_pre,
        };
    }

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    for (workers) |*w| w.morsel_src = &morsel_src;
    try parallel.parallelFor(alloc, Worker, Worker.work, workers, &morsel_src);
    for (workers) |w| {
        if (w.err) |e| return e;
    }

    // ── Partitioned parallel merge ─────────────────────────────────────────────
    // N_PARTS_PAIR independent partitions: worker p processes all local_hts but only
    // accumulates entries where (hash & (N_PARTS_PAIR - 1)) == p.  Each worker builds
    // its own merged_hts[p] exclusively — no shared writes, no serial bottleneck.
    const N_PARTS_PAIR: usize = @min(n_threads, 8);
    var total_local_count: u64 = 0;
    for (workers) |w| total_local_count += w.local_ht.count;
    const est_per_part_pair: u64 = total_local_count / N_PARTS_PAIR + 64;
    const merge_arenas = try alloc.alloc(std.heap.ArenaAllocator, N_PARTS_PAIR);
    const merged_hts = try alloc.alloc(ht.PairCountHashTable, N_PARTS_PAIR);
    for (merge_arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer for (merge_arenas) |*a| a.deinit();
    for (merge_arenas, merged_hts) |*a, *h| {
        h.* = try ht.PairCountHashTable.initWithCapacity(a.allocator(), est_per_part_pair);
    }

    const PairMergeCtx = struct {
        workers_p: []Worker,
        merged_hts: []ht.PairCountHashTable,
        morsel_src: *parallel.MorselSource,
        n_parts: usize,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.doWork() catch |e| {
                self.err = e;
            };
        }

        fn doWork(self: *@This()) !void {
            while (self.morsel_src.next()) |m| {
                for (m.start..m.end) |p| {
                    for (self.workers_p) |*w| {
                        try self.merged_hts[p].mergeFromPart(&w.local_ht, p, self.n_parts);
                    }
                }
            }
        }
    };

    var pair_merge_morsel = parallel.MorselSource.init(N_PARTS_PAIR, 1);
    const pair_merge_ctxs = try alloc.alloc(PairMergeCtx, n_threads);
    for (pair_merge_ctxs) |*mc| {
        mc.* = .{
            .workers_p = workers,
            .merged_hts = merged_hts,
            .morsel_src = &pair_merge_morsel,
            .n_parts = N_PARTS_PAIR,
        };
    }
    try parallel.parallelFor(alloc, PairMergeCtx, PairMergeCtx.work, pair_merge_ctxs, &pair_merge_morsel);
    for (pair_merge_ctxs) |mc| {
        if (mc.err) |e| return e;
    }

    // ── Collect results ──────────────────────────────────────────────────────
    const metas = try alloc.alloc(result.ColMeta, 3);
    metas[0] = if (k0_is_i64) result.ColMeta{ .name = keys[0].alias, .col_type = .int64 } else result.ColMeta{ .name = keys[0].alias, .col_type = .string };
    metas[1] = if (k0_is_i64) result.ColMeta{ .name = keys[1].alias, .col_type = .string } else result.ColMeta{ .name = keys[1].alias, .col_type = .int64 };
    metas[2] = result.ColMeta{ .name = aggs[0].alias, .col_type = .uint64 };
    var rl = RowBuffer.init(metas);

    const EmitCtx = struct {
        rl: *RowBuffer,
        alloc: std.mem.Allocator,
        k0_is_i64: bool,
    };
    var emit_ctx = EmitCtx{ .rl = &rl, .alloc = alloc, .k0_is_i64 = k0_is_i64 };
    const emit_cb = struct {
        fn cb(ec: *EmitCtx, n: i64, s: []const u8, count: u64) void {
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
    }.cb;
    for (merged_hts) |*mht| mht.iterate(&emit_ctx, emit_cb);

    // Apply top_k if requested (Q17: ORDER BY count(*) DESC LIMIT 10).
    if (top_k > 0 and sort_keys.len > 0 and rl.rows.items.len > top_k) {
        return try executeTopK(rl, sort_keys, top_k, alloc);
    }
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

pub fn executeHashAggParallelTripleCount(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 1_000_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const alloc = ctx.allocator();

    // Only handle unfiltered scans (no WHERE clause support yet).
    if (input.* != .part_scan and input.* != .mem_scan) return null;

    // Guard: exactly 3 keys, exactly 1 agg (count(*)).
    if (keys.len != 3) return null;
    if (aggs.len != 1) return null;
    if (aggs[0].expr != .agg_call) return null;
    if (aggs[0].expr.agg_call.kind != .count_star) return null;

    const DatePartUnit = enum { minute, hour, day };
    const TripleParDesc = struct {
        n0_col: usize,
        dp_col: usize,
        dp_unit: DatePartUnit,
        str_col: usize,
        dp_scale: i64, // 1 for DateTime64(ms), 1000 for raw second timestamps
        key_order: [3]u8, // key_order[output_pos] = 0→n0, 1→dp, 2→str
    };

    // Detect the pattern: one col_ref(int), one fn_call(date_part/extract), one col_ref(string).
    const maybe_td: ?TripleParDesc = blk: {
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
                    if (dp_idx != null) break :blk null; // only one fn_call allowed
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
        const sm = ctx.source.schema();
        const ci0 = keys[col_ref_indices[0]].expr.col_ref.index;
        const ci1 = keys[col_ref_indices[1]].expr.col_ref.index;
        if (ci0 >= sm.len or ci1 >= sm.len or dp_col >= sm.len) break :blk null;
        const t0 = sm[ci0].col_type;
        const t1 = sm[ci1].col_type;
        // Identify which col_ref is int and which is string.
        var n0_ci: usize = undefined;
        var str_ci: usize = undefined;
        var n0_key_pos: usize = undefined;
        if ((t0 == .int64 or t0 == .uint64) and t1 == .string) {
            n0_ci = ci0;
            str_ci = ci1;
            n0_key_pos = col_ref_indices[0];
        } else if ((t1 == .int64 or t1 == .uint64) and t0 == .string) {
            n0_ci = ci1;
            str_ci = ci0;
            n0_key_pos = col_ref_indices[1];
        } else break :blk null;
        var order: [3]u8 = .{ 0, 0, 0 };
        for (0..3) |ki| {
            if (ki == dp_idx.?) order[ki] = 1 else if (ki == n0_key_pos) order[ki] = 0 else order[ki] = 2;
        }
        break :blk TripleParDesc{
            .n0_col = n0_ci,
            .dp_col = dp_col,
            .dp_unit = dp_unit,
            .str_col = str_ci,
            .dp_scale = if (sm[dp_col].col_type == .datetime64_ms) 1 else 1000,
            .key_order = order,
        };
    };

    const td = maybe_td orelse return null;
    const sm = ctx.source.schema();

    var scan_request: ScanRequest = .{};
    scan_request.addIndex(td.n0_col);
    scan_request.addIndex(td.dp_col);
    scan_request.addIndex(td.str_col);
    var scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    // Preload columns.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    // Number of hash partitions for the two-phase parallel merge.
    // 64 partitions × ~1M unique groups / 64 = ~16K entries each → HT fits in L3 cache.
    const N_TRIPLE_PARTS: usize = 64;

    const TripleParWorker = struct {
        source: SourceIface,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        scatter_alloc: std.mem.Allocator, // per-worker arena (c_allocator backed) for part_bufs
        td: TripleParDesc,
        raw_n0: ?[]const i64,
        raw_dp: ?[]const i64,
        raw_str_offsets: ?[]const u64,
        raw_str_bytes: ?[]const u8,
        local_ht: ht.TripleCountHashTable,
        part_bufs: []std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot), // len = N_TRIPLE_PARTS
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn datePart(unit: DatePartUnit, ms: i64) i64 {
            const secs = @divTrunc(ms, 1000);
            return switch (unit) {
                .minute => @mod(@divTrunc(secs, 60), 60),
                .hour => @mod(@divTrunc(secs, 3600), 24),
                .day => blk: {
                    const days = @divTrunc(ms, 86400 * 1000);
                    const d = if (days >= 0) @as(u64, @intCast(days)) else 0;
                    const n: u64 = d + 719468;
                    const era: u64 = @divTrunc(n, 146097);
                    const doe: u64 = n - era * 146097;
                    const yoe: u64 = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
                    const doy: u64 = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
                    const mp: u64 = @divTrunc(5 * doy + 2, 153);
                    break :blk @intCast(doy - @divTrunc(153 * mp + 2, 5) + 1);
                },
            };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            raw_scan: {
                const n0s = self.raw_n0 orelse break :raw_scan;
                const dps = self.raw_dp orelse break :raw_scan;
                const offs = self.raw_str_offsets orelse break :raw_scan;
                const bytes = self.raw_str_bytes orelse break :raw_scan;
                while (self.morsel_src.next()) |m| {
                    if (m.end > n0s.len or m.end > dps.len or m.end >= offs.len) break :raw_scan;
                    for (m.start..m.end) |row| {
                        const n0 = n0s[row];
                        const n1 = @This().datePart(self.td.dp_unit, dps[row] * self.td.dp_scale);
                        const lo: usize = @intCast(offs[row]);
                        const hi: usize = @intCast(offs[row + 1]);
                        if (hi > bytes.len or lo > hi) continue;
                        try self.local_ht.increment(n0, n1, bytes[lo..hi]);
                    }
                }
                break :raw_scan;
            }

            while (self.morsel_src.next()) |m| {
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, talloc);
                const desc = self.td;
                if (desc.n0_col >= c.columns.len or desc.dp_col >= c.columns.len or desc.str_col >= c.columns.len) continue;
                const n0_col_data = c.columns[desc.n0_col];
                const dp_col_data = c.columns[desc.dp_col];
                const strs = switch (c.columns[desc.str_col].data) {
                    .string => |s| s,
                    else => continue,
                };
                for (0..c.num_rows) |r| {
                    const n0: i64 = switch (n0_col_data.data) {
                        .int64 => |v| v[r],
                        .uint64 => |v| @bitCast(v[r]),
                        else => continue,
                    };
                    const ms: i64 = switch (dp_col_data.data) {
                        .datetime64_ms => |v| v[r],
                        .int64 => |v| v[r] * 1000,
                        else => continue,
                    };
                    const n1 = @This().datePart(desc.dp_unit, ms);
                    try self.local_ht.increment(n0, n1, strs[r]);
                }
            }
            // Scatter phase: distribute local_ht entries into per-worker partition buffers.
            // Each worker writes exclusively to its own part_bufs — no synchronization needed.
            const part_mask: u64 = N_TRIPLE_PARTS - 1;
            for (0..self.local_ht.capacity) |i| {
                if (self.local_ht.slots[i].hash == ht.TripleCountHashTable.EMPTY) continue;
                const s = self.local_ht.slots[i];
                const p: usize = @intCast(s.hash & part_mask);
                try self.part_bufs[p].append(self.scatter_alloc, s);
            }
        }
    };

    const est_per_thread: u64 = @max(64, total_rows / @as(u64, n_threads));
    const ht_arenas = try alloc.alloc(std.heap.ArenaAllocator, n_threads);
    for (ht_arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer for (ht_arenas) |*a| a.deinit();

    // Allocate per-worker partition scatter buffers (N_TRIPLE_PARTS lists each).
    // Backed by the per-thread arena (c_allocator) so each worker allocates independently.
    const per_worker_bufs = try alloc.alloc([]std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot), n_threads);
    for (per_worker_bufs, 0..) |*pb, t| {
        pb.* = try ht_arenas[t].allocator().alloc(std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot), N_TRIPLE_PARTS);
        for (pb.*) |*list| list.* = .empty;
    }

    const raw_n0_pre = ctx.source.getRawInt64Col(sm[td.n0_col].name);
    const raw_dp_pre = ctx.source.getRawInt64Col(sm[td.dp_col].name);
    const raw_str_offsets_pre = ctx.source.getRawStrOffsets(sm[td.str_col].name);
    const raw_str_bytes_pre = ctx.source.getRawStrBytes(sm[td.str_col].name);

    const workers = try alloc.alloc(TripleParWorker, n_threads);
    for (workers, 0..) |*w, t| {
        w.* = .{
            .source = ctx.source,
            .morsel_src = undefined,
            .parent_alloc = alloc,
            .scatter_alloc = ht_arenas[t].allocator(),
            .td = td,
            .raw_n0 = raw_n0_pre,
            .raw_dp = raw_dp_pre,
            .raw_str_offsets = raw_str_offsets_pre,
            .raw_str_bytes = raw_str_bytes_pre,
            .local_ht = try ht.TripleCountHashTable.initWithCapacity(ht_arenas[t].allocator(), est_per_thread),
            .part_bufs = per_worker_bufs[t],
            .err = null,
        };
    }

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    for (workers) |*w| w.morsel_src = &morsel_src;
    try parallel.parallelFor(alloc, TripleParWorker, TripleParWorker.work, workers, &morsel_src);
    for (workers) |w| {
        if (w.err) |e| return e;
    }

    // Two-phase parallel aggregate: scatter was done inline in each worker's runWork.
    // Now each of the N_TRIPLE_PARTS partitions is aggregated independently in parallel.
    // Each partition HT is ~(total_unique/N_TRIPLE_PARTS) entries ≈ fits in L3 cache.
    const part_arenas_triple = try alloc.alloc(std.heap.ArenaAllocator, N_TRIPLE_PARTS);
    for (part_arenas_triple) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer for (part_arenas_triple) |*a| a.deinit();
    const part_hts_triple = try alloc.alloc(ht.TripleCountHashTable, N_TRIPLE_PARTS);

    const TriplePartAggWorker = struct {
        per_worker_bufs: []const []std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot),
        n_scan_workers: usize,
        part_arenas: []std.heap.ArenaAllocator,
        part_hts: []ht.TripleCountHashTable,
        err: ?anyerror = null,

        fn work(self: *@This(), src: *parallel.MorselSource) void {
            while (src.next()) |m| {
                var pi = m.start;
                while (pi < m.end) : (pi += 1) {
                    self.processPartition(pi) catch |e| {
                        self.err = e;
                        return;
                    };
                }
            }
        }

        fn processPartition(self: *@This(), p: usize) !void {
            var total: usize = 0;
            for (0..self.n_scan_workers) |w| total += self.per_worker_bufs[w][p].items.len;
            self.part_hts[p] = try ht.TripleCountHashTable.initWithCapacity(
                self.part_arenas[p].allocator(),
                @max(64, total),
            );
            for (0..self.n_scan_workers) |w| {
                for (self.per_worker_bufs[w][p].items) |s| {
                    try self.part_hts[p].mergeFromSlot(s);
                }
            }
        }
    };

    const part_agg_workers = try alloc.alloc(TriplePartAggWorker, n_threads);
    for (part_agg_workers) |*aw| {
        aw.* = .{
            .per_worker_bufs = per_worker_bufs,
            .n_scan_workers = n_threads,
            .part_arenas = part_arenas_triple,
            .part_hts = part_hts_triple,
            .err = null,
        };
    }
    var part_agg_src = parallel.MorselSource.init(N_TRIPLE_PARTS, 1);
    try parallel.parallelFor(alloc, TriplePartAggWorker, TriplePartAggWorker.work, part_agg_workers, &part_agg_src);
    for (part_agg_workers) |aw| {
        if (aw.err) |e| return e;
    }

    const out_metas = try buildAggResultMetas(keys, aggs, alloc);
    var rl = RowBuffer.init(out_metas);
    var topk: ?TopKCollector = if (top_k > 0 and sort_keys.len > 0)
        try TopKCollector.init(alloc, sort_keys, top_k)
    else
        null;

    const EmitCtxTriple = struct {
        rl: *RowBuffer,
        topk: ?*TopKCollector,
        alloc: std.mem.Allocator,
        key_order: [3]u8,
    };
    var emit_ctx = EmitCtxTriple{
        .rl = &rl,
        .topk = if (topk) |*tk| tk else null,
        .alloc = alloc,
        .key_order = td.key_order,
    };
    for (0..N_TRIPLE_PARTS) |p| {
        part_hts_triple[p].iterate(&emit_ctx, struct {
            fn cb(ec: *EmitCtxTriple, n0: i64, n1: i64, s: []const u8, count: u64) void {
                if (ec.topk) |tk| {
                    if (tk.shouldSkipCount(count)) return;
                }
                const row = ec.alloc.alloc(?Value, 4) catch return;
                for (ec.key_order, 0..) |kind, i| {
                    row[i] = switch (kind) {
                        0 => Value{ .int64 = n0 },
                        1 => Value{ .int64 = n1 },
                        else => Value{ .string = s },
                    };
                }
                row[3] = Value{ .uint64 = count };
                if (ec.topk) |tk| tk.offer(row) else ec.rl.append(ec.alloc, row) catch {};
            }
        }.cb);
    }

    if (topk) |*tk| return try tk.finish(out_metas);
    return rl;
}

pub fn executeHashAggParallelStrKey(
    input: *const plan.PhysicalNode,
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    ctx: *QueryContext,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 500_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const alloc = ctx.allocator();

    // Require: all keys are plain col_ref, lit_i64 (constant), OR exactly one simple CASE WHEN str key;
    // exactly one col_ref must be a string column (all others int).
    // (Multi-key int+string queries like Q15/Q17/Q18 are handled via composite key.)
    if (keys.len == 0) return null;

    // Guard: for large unfiltered scans with multi-key (int+str), the parallel merge cost
    // dominates and causes high variance (millions of unique pairs exhaust L3).
    // Fall back to sequential executeHashAggChunked for better consistency.
    // Exception: if all "extra" keys are lit_i64 constants (e.g. Q35: GROUP BY 1, URL),
    // the hash-sidecar path reduces it to a single int key — no variance issue.
    const has_filter_str: bool = switch (input.*) {
        .filter => true,
        .project => |p| p.input.* == .filter,
        else => false,
    };
    {
        if (!has_filter_str and total_rows >= 3_000_000 and keys.len >= 2) {
            // Count non-constant keys. If only 1 non-constant key exists, hash-sidecar
            // can reduce it to a single int aggregation — allow it through.
            var non_const_count: usize = 0;
            for (keys) |k| {
                if (k.expr != .lit_i64) non_const_count += 1;
            }
            if (non_const_count > 2) return null;
        }
    }
    var cw_key: ?CaseWhenStrKey = null;
    var cw_key_pos: usize = 0;
    var rr_active: bool = false;
    var rr_col_idx: usize = 0;
    var rr_is_url_domain: bool = false;
    for (keys, 0..) |k, ki| {
        if (k.expr == .col_ref) continue;
        if (k.expr == .lit_i64) continue; // constant key (e.g. GROUP BY 1)
        if (k.expr == .case_when) {
            if (cw_key != null) return null; // at most one CASE WHEN
            const cw = extractCaseWhenStrKey(k.expr) orelse return null;
            cw_key = cw;
            cw_key_pos = ki;
            continue;
        }
        // Accept single fn_call(regexp_replace) key (e.g. Q29 domain extraction).
        if (k.expr == .fn_call and keys.len == 1) {
            const fc = k.expr.fn_call;
            if ((std.mem.eql(u8, fc.name, "regexp_replace") or
                std.mem.eql(u8, fc.name, "replaceRegexpOne")) and
                fc.args.len >= 3 and fc.args[0] == .col_ref and fc.args[1] == .lit_str)
            {
                rr_col_idx = fc.args[0].col_ref.index;
                const pat = fc.args[1].lit_str;
                rr_is_url_domain =
                    std.mem.eql(u8, pat, "^https?://(?:www\\.)?([^/]+)/.*$") or
                    std.mem.eql(u8, pat, "^https?://(?:www\\.)?([^/]+)/.*");
                rr_active = true;
                continue;
            }
        }
        return null;
    }
    const sm_pre = ctx.source.schema();
    var str_key_count: usize = 0;
    var str_key_col_idx: usize = 0;
    var str_key_pos: usize = 0; // position among keys array for the string key
    if (rr_active) {
        // regexp_replace key: treat the input column as the string key source.
        str_key_count = 1;
        str_key_col_idx = rr_col_idx;
        str_key_pos = 0;
    } else {
        for (keys, 0..) |k, ki| {
            if (k.expr != .col_ref) continue; // skip CASE WHEN keys
            const ci = k.expr.col_ref.index;
            const is_str = ci < sm_pre.len and (sm_pre[ci].col_type == .string or sm_pre[ci].col_type == .array_string);
            if (is_str) {
                str_key_count += 1;
                str_key_col_idx = ci;
                str_key_pos = ki;
            }
        }
    }
    // Must have exactly one col_ref string key (the primary string key).
    // If there's also a CASE WHEN, it becomes the secondary string component.
    if (str_key_count != 1) return null;
    const key_col_idx = str_key_col_idx;

    // Build compact_kinds; allow str_min/str_max and count_distinct_u64.
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
            .min => if (item.out_type == .string) .str_min else .i64_min,
            .max => if (item.out_type == .string) .str_max else .i64_max,
            else => return null,
        };
    }

    // Count str aggs and build sidecar_idx map.
    var num_str_aggs: usize = 0;
    const sidecar_idx = try alloc.alloc(usize, aggs.len);
    for (compact_kinds, 0..) |kind, ci| {
        if (kind == .str_min or kind == .str_max) {
            sidecar_idx[ci] = num_str_aggs;
            num_str_aggs += 1;
        } else {
            sidecar_idx[ci] = 0; // unused
        }
    }

    // Early-compute has_count_distinct so the two-phase call site can gate on it.
    const has_count_distinct: bool = blk_hcd: {
        for (compact_kinds) |k| {
            if (k == .count_distinct_u64) break :blk_hcd true;
        }
        break :blk_hcd false;
    };

    // Build init_vals.
    const compact_init_vals = try alloc.alloc(u64, aggs.len);
    for (compact_kinds, 0..) |kind, ci| {
        compact_init_vals[ci] = switch (kind) {
            .count, .i64_sum, .u64_sum, .u64_max, .str_min, .str_max, .f64_str_len_sum, .count_distinct_u64 => 0,
            .f64_sum => @bitCast(@as(f64, 0.0)),
            .i64_min => @bitCast(@as(i64, std.math.maxInt(i64))),
            .i64_max => @bitCast(@as(i64, std.math.minInt(i64))),
            .u64_min => std.math.maxInt(u64),
            .f64_min => @bitCast(std.math.inf(f64)),
            .f64_max => @bitCast(-std.math.inf(f64)),
        };
    }

    // Extract filter predicate.
    const filter_pred: ?plan.Expr = switch (input.*) {
        .filter => |f| f.predicate,
        .project => |p| switch (p.input.*) {
            .filter => |f| f.predicate,
            else => null,
        },
        else => null,
    };

    var parallel_scan_request: ScanRequest = .{};
    parallel_scan_request.addItems(keys);
    parallel_scan_request.addItems(aggs);
    if (filter_pred) |fp| parallel_scan_request.addExpr(fp);
    const parallel_scan_scope = parallel_scan_request.apply(ctx.source, .any_reduction);
    defer parallel_scan_scope.deinit();

    // Preload columns.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    // ── Two-phase scatter → small-HT path for high-cardinality pure-string GROUP BY ──
    // Conditions: no CASE WHEN, no regexp_replace, no str_min/str_max aggs, no COUNT DISTINCT,
    // all non-string GROUP BY keys are lit_i64 constants, filter is null or a single
    // col_ref eq/neq lit_str expression (e.g. SearchPhrase != '').
    // For Q34/Q35 (URL GROUP BY, 2.6M unique, no filter) and Q13 (SearchPhrase GROUP BY).
    if (cw_key == null and !rr_active and num_str_aggs == 0 and
        (!has_count_distinct or (aggs.len == 1 and compact_kinds[0] == .count_distinct_u64)) and
        total_rows >= 3_000_000)
    {
        // Check that every non-string key is either a lit_i64 constant or
        // exactly one col_ref int key (used as an int sidecar for composite keys).
        var all_const_int_keys_outer: bool = true;
        var sidecar_col_idx_outer: ?usize = null;
        var sidecar_key_pos_outer: ?usize = null;
        for (keys, 0..) |k, ki| {
            // The main string key is always fine.
            if (k.expr == .col_ref and k.expr.col_ref.index == key_col_idx) continue;
            // lit_i64 constants are always fine.
            if (k.expr == .lit_i64) continue;
            // Allow exactly one additional col_ref int key as a sidecar.
            if (k.expr == .col_ref) {
                if (sidecar_col_idx_outer == null) {
                    sidecar_col_idx_outer = k.expr.col_ref.index;
                    sidecar_key_pos_outer = ki;
                    continue;
                }
            }
            all_const_int_keys_outer = false;
            break;
        }
        if (all_const_int_keys_outer) {
            const fp_tp: ?plan.Expr = switch (input.*) {
                .filter => |f| f.predicate,
                .project => |p2| switch (p2.input.*) {
                    .filter => |f| f.predicate,
                    else => null,
                },
                else => null,
            };
            var sf_tp: ?SimpleStrFilter = null;
            var tp_filter_ok: bool = true;
            var tp_ic_buf: [16]IntCmpCond = undefined;
            var tp_ic_n: usize = 0;
            if (fp_tp) |pred| {
                sf_tp = tryExtractSimpleStrFilter(pred);
                if (sf_tp == null) {
                    // Try decomposing into int conditions + optional str condition.
                    var sc_buf: [4]StrCmpCond = undefined;
                    var sc_n: usize = 0;
                    const mixed_ok = extractMixedAndConds(pred, &tp_ic_buf, &tp_ic_n, &sc_buf, &sc_n);
                    if (mixed_ok and sc_n <= 1) {
                        if (sc_n == 1) {
                            sf_tp = .{ .col_idx = sc_buf[0].col_idx, .value = sc_buf[0].val, .is_neq = (sc_buf[0].op == .neq) };
                        }
                        // tp_filter_ok stays true; tp_ic_n may be > 0
                    } else {
                        tp_filter_ok = false;
                    }
                }
            }
            if (tp_filter_ok) {
                const tp_int_filter: ?[]const IntCmpCond = if (tp_ic_n > 0) tp_ic_buf[0..tp_ic_n] else null;
                if (try executeTwoPhaseHashAggStrKeySimple(
                    keys,
                    aggs,
                    sort_keys,
                    top_k,
                    ctx,
                    key_col_idx,
                    str_key_pos,
                    compact_kinds,
                    compact_init_vals,
                    sf_tp,
                    tp_int_filter,
                    sidecar_col_idx_outer,
                    sidecar_key_pos_outer,
                )) |tp_rl| return tp_rl;
            }
        }
    }

    const ParStrCtx = struct {
        source: SourceIface,
        filter_pred: ?plan.Expr,
        key_col_idx: usize,
        keys: []const plan.ProjectItem,
        str_key_pos: usize,
        aggs: []const plan.ProjectItem,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        sidecar_idx: []const usize,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        local_ht: ht.StrAggHashTable,
        has_count_distinct: bool = false,
        /// Pre-extracted COUNT(DISTINCT) agg column indices (at most 4).
        distinct_col_idx_buf: [4]usize = .{ 0, 0, 0, 0 },
        distinct_agg_ci_buf: [4]usize = .{ 0, 0, 0, 0 },
        distinct_n: usize = 0,
        err: ?anyerror = null,
        // Preextracted fast-path conditions (avoids pass_mask allocation + two-pass scan).
        inline_ic: [16]IntCmpCond = undefined,
        inline_ic_n: usize = 0,
        inline_sc: [8]StrCmpCond = undefined,
        inline_sc_n: usize = 0,
        use_inline_filter: bool = false,
        // Optional secondary CASE WHEN string key (e.g. Q40).
        cw_key: ?CaseWhenStrKey = null,
        cw_key_pos: usize = 0,
        // regexp_replace fast path (e.g. Q29): compute URL domain from rr_col_idx column.
        rr_active: bool = false,
        rr_col_idx: usize = 0,
        rr_is_url_domain: bool = false,
        /// Per-thread scatter buffers for COUNT(DISTINCT) dedup (only allocated when has_count_distinct).
        /// 64 partitions keyed by (group_hash & 63); each entry = (group_hash << 64) | distinct_val.
        /// Kept at end of struct so hot fields above stay in the first few cache lines.
        scatter_bufs: [64]std.ArrayListUnmanaged(u128) = undefined,
        /// Per-thread arena for composite key buffers (int_prefix + str_val bytes).
        /// These buffers are stored as borrowed pointers in local_ht.key_slots and must
        /// outlive both runWork AND the subsequent tree-merge + emit phase.
        /// Uses c_allocator (thread-safe) so threads can allocate independently.
        thread_key_arena: std.heap.ArenaAllocator = undefined,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.runWork() catch |e| {
                self.err = e;
            };
        }

        fn runWork(self: *@This()) !void {
            var thread_arena = std.heap.ArenaAllocator.init(self.parent_alloc);
            defer thread_arena.deinit();
            const talloc = thread_arena.allocator();

            // Build int_key_specs: each non-string, non-CASE-WHEN key is either a column ref or a constant.
            const IntKeySpec = struct {
                is_col: bool,
                col_idx: usize = 0, // valid if is_col
                const_val: u64 = 0, // valid if !is_col
            };
            var int_key_specs_buf: [16]IntKeySpec = undefined;
            var int_key_n: usize = 0;
            for (self.keys) |k| {
                switch (k.expr) {
                    .col_ref => |cr| {
                        if (cr.index == self.key_col_idx) continue; // skip the string key
                        if (int_key_n < 16) {
                            int_key_specs_buf[int_key_n] = .{ .is_col = true, .col_idx = cr.index };
                            int_key_n += 1;
                        }
                    },
                    .lit_i64 => |v| {
                        if (int_key_n < 16) {
                            int_key_specs_buf[int_key_n] = .{ .is_col = false, .const_val = @bitCast(v) };
                            int_key_n += 1;
                        }
                    },
                    else => {}, // skip CASE WHEN and other handled keys
                }
            }
            const int_key_specs = int_key_specs_buf[0..int_key_n];
            // If all int keys are constants (lit_i64), skip int prefix in the composite key.
            // They don't affect grouping cardinality; we reconstruct them at emit time.
            var all_const_int_keys = true;
            for (int_key_specs) |spec| {
                if (spec.is_col) {
                    all_const_int_keys = false;
                    break;
                }
            }
            const int_prefix_len: usize = if (all_const_int_keys) 0 else int_key_n * 8;

            while (self.morsel_src.next()) |m| {
                var chunk_arena = std.heap.ArenaAllocator.init(talloc);
                defer chunk_arena.deinit();
                const calloc = chunk_arena.allocator();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, calloc);

                // Build pass_mask from filter.
                var pass_mask: ?[]bool = null;
                if (self.filter_pred) |fp| {
                    const pm = try calloc.alloc(bool, c.num_rows);
                    @memset(pm, true);
                    // Fast int-cond path.
                    var ic_buf: [16]IntCmpCond = undefined;
                    var ic_n: usize = 0;
                    const ic_complete = extractAndIntConds(fp, &ic_buf, &ic_n, false);
                    if (ic_complete and ic_n > 0) {
                        const n_ic = c.num_rows;
                        const i16_mask = try calloc.alloc(i16, n_ic);
                        const i16_tmp_a = try calloc.alloc(i16, n_ic);
                        const i16_tmp_b = try calloc.alloc(i16, n_ic);
                        @memset(i16_mask, 1);
                        var simd_ok = true;
                        for (ic_buf[0..ic_n]) |cond| {
                            if (!applyIntCondSIMD(&c, cond, n_ic, i16_mask, i16_tmp_a, i16_tmp_b)) {
                                simd_ok = false;
                                break;
                            }
                        }
                        if (simd_ok) {
                            for (0..n_ic) |r| {
                                if (i16_mask[r] == 0) pm[r] = false;
                            }
                        } else {
                            for (0..c.num_rows) |r| {
                                for (ic_buf[0..ic_n]) |cond| {
                                    if (cond.col_idx >= c.columns.len) {
                                        pm[r] = false;
                                        break;
                                    }
                                    const col = c.columns[cond.col_idx];
                                    if (col.isRowNull(r)) {
                                        pm[r] = false;
                                        break;
                                    }
                                    const v: i64 = switch (col.data) {
                                        .int64 => |a| a[r],
                                        .uint64 => |a| @bitCast(a[r]),
                                        .bool_u8 => |a| @as(i64, a[r]),
                                        .date_u16 => |a| @as(i64, a[r]),
                                        .datetime64_ms => |a| a[r],
                                        else => {
                                            pm[r] = false;
                                            break;
                                        },
                                    };
                                    const pass = switch (cond.op) {
                                        .eq => v == cond.val,
                                        .neq => v != cond.val,
                                        .lt => v < cond.val,
                                        .lte => v <= cond.val,
                                        .gt => v > cond.val,
                                        .gte => v >= cond.val,
                                        .in2 => v == cond.val or v == cond.val2,
                                    };
                                    if (!pass) {
                                        pm[r] = false;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // Fast str-cond path: covers str_col != 'literal' (e.g. Q11 MobilePhoneModel <> '').
                        var sc_buf: [8]StrCmpCond = undefined;
                        var sc_n: usize = 0;
                        const sc_complete = extractAndStrConds(fp, &sc_buf, &sc_n, false);
                        if (sc_complete and sc_n > 0) {
                            for (0..c.num_rows) |r| {
                                for (sc_buf[0..sc_n]) |cond| {
                                    if (cond.col_idx >= c.columns.len) {
                                        pm[r] = false;
                                        break;
                                    }
                                    const col = c.columns[cond.col_idx];
                                    const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                        .string => |a| a[r],
                                        else => {
                                            pm[r] = false;
                                            break;
                                        },
                                    };
                                    const pass = switch (cond.op) {
                                        .eq => std.mem.eql(u8, s, cond.val),
                                        .neq => !std.mem.eql(u8, s, cond.val),
                                    };
                                    if (!pass) {
                                        pm[r] = false;
                                        break;
                                    }
                                }
                            }
                        } else {
                            // Mixed fast-path: handles AND of int + str comparisons without evalExpr.
                            // Covers e.g. Q37: CounterID=62 AND EventDate range AND URL<>'' etc.
                            var mic_buf: [16]IntCmpCond = undefined;
                            var mic_n: usize = 0;
                            var msc_buf: [8]StrCmpCond = undefined;
                            var msc_n: usize = 0;
                            const mixed_complete = extractMixedAndConds(fp, &mic_buf, &mic_n, &msc_buf, &msc_n);
                            if (mixed_complete and (mic_n > 0 or msc_n > 0)) {
                                // Phase 1: SIMD int conditions → build i16_mask.
                                const n_mx = c.num_rows;
                                const i16_mask = try calloc.alloc(i16, n_mx);
                                const i16_tmp_a = try calloc.alloc(i16, n_mx);
                                const i16_tmp_b = try calloc.alloc(i16, n_mx);
                                @memset(i16_mask, 1);
                                var simd_ok = true;
                                for (mic_buf[0..mic_n]) |cond| {
                                    if (!applyIntCondSIMD(&c, cond, n_mx, i16_mask, i16_tmp_a, i16_tmp_b)) {
                                        simd_ok = false;
                                        break;
                                    }
                                }
                                if (simd_ok) {
                                    // Write int results to pm.
                                    for (0..n_mx) |r| {
                                        if (i16_mask[r] == 0) pm[r] = false;
                                    }
                                    // Phase 2: scalar string conditions on surviving rows only.
                                    if (msc_n > 0) {
                                        str_loop: for (0..n_mx) |r| {
                                            if (!pm[r]) continue;
                                            for (msc_buf[0..msc_n]) |cond| {
                                                if (cond.col_idx >= c.columns.len) {
                                                    pm[r] = false;
                                                    continue :str_loop;
                                                }
                                                const col = c.columns[cond.col_idx];
                                                const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                                    .string => |a| a[r],
                                                    else => {
                                                        pm[r] = false;
                                                        continue :str_loop;
                                                    },
                                                };
                                                const pass = switch (cond.op) {
                                                    .eq => std.mem.eql(u8, s, cond.val),
                                                    .neq => !std.mem.eql(u8, s, cond.val),
                                                };
                                                if (!pass) {
                                                    pm[r] = false;
                                                    continue :str_loop;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    // Scalar fallback for mixed path (rare: unsupported column type).
                                    mixed_loop: for (0..n_mx) |r| {
                                        for (mic_buf[0..mic_n]) |cond| {
                                            if (cond.col_idx >= c.columns.len) {
                                                pm[r] = false;
                                                continue :mixed_loop;
                                            }
                                            const col = c.columns[cond.col_idx];
                                            if (col.isRowNull(r)) {
                                                pm[r] = false;
                                                continue :mixed_loop;
                                            }
                                            const v: i64 = switch (col.data) {
                                                .int64 => |a| a[r],
                                                .uint64 => |a| @bitCast(a[r]),
                                                .bool_u8 => |a| @as(i64, a[r]),
                                                .date_u16 => |a| @as(i64, a[r]),
                                                .datetime64_ms => |a| a[r],
                                                else => {
                                                    pm[r] = false;
                                                    continue :mixed_loop;
                                                },
                                            };
                                            const pass = switch (cond.op) {
                                                .eq => v == cond.val,
                                                .neq => v != cond.val,
                                                .lt => v < cond.val,
                                                .lte => v <= cond.val,
                                                .gt => v > cond.val,
                                                .gte => v >= cond.val,
                                                .in2 => v == cond.val or v == cond.val2,
                                            };
                                            if (!pass) {
                                                pm[r] = false;
                                                continue :mixed_loop;
                                            }
                                        }
                                        for (msc_buf[0..msc_n]) |cond| {
                                            if (cond.col_idx >= c.columns.len) {
                                                pm[r] = false;
                                                continue :mixed_loop;
                                            }
                                            const col = c.columns[cond.col_idx];
                                            const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                                .string => |a| a[r],
                                                else => {
                                                    pm[r] = false;
                                                    continue :mixed_loop;
                                                },
                                            };
                                            const pass = switch (cond.op) {
                                                .eq => std.mem.eql(u8, s, cond.val),
                                                .neq => !std.mem.eql(u8, s, cond.val),
                                            };
                                            if (!pass) {
                                                pm[r] = false;
                                                continue :mixed_loop;
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Partial int pre-filter: apply any int conditions fast before evalExpr.
                                // This short-circuits most rows (e.g. CounterID=62 filters out 99%).
                                var pic_buf: [16]IntCmpCond = undefined;
                                var pic_n: usize = 0;
                                _ = extractAndIntConds(fp, &pic_buf, &pic_n, true);

                                // Best-effort str cond pre-filter (applied before LIKE guard).
                                // For Q22/Q23: extracts SearchPhrase <> '' fast, reducing LIKE calls.
                                var psc_buf: [8]StrCmpCond = undefined;
                                var psc_n: usize = 0;
                                _ = extractAndStrConds(fp, &psc_buf, &psc_n, true);

                                // Build like guards for fast pre-filtering.
                                var guards_list = std.ArrayListUnmanaged(LikeGuard).empty;
                                collectLikeGuards(fp, &guards_list, calloc);
                                const guards = guards_list.items;

                                const ref_mask = try calloc.alloc(bool, @min(256, c.columns.len));
                                @memset(ref_mask, false);
                                collectColRefs(fp, ref_mask);
                                var ref_buf = try calloc.alloc(usize, c.columns.len);
                                var ref_n: usize = 0;
                                for (ref_mask, 0..) |m2, idx| {
                                    if (m2 and idx < c.columns.len) {
                                        ref_buf[ref_n] = idx;
                                        ref_n += 1;
                                    }
                                }
                                const refs = ref_buf[0..ref_n];
                                const row_v = try calloc.alloc(?Value, c.columns.len);
                                @memset(row_v, null);

                                row_loop: for (0..c.num_rows) |r| {
                                    // Partial int conditions pre-check (fast path).
                                    if (pic_n > 0) {
                                        for (pic_buf[0..pic_n]) |cond| {
                                            if (cond.col_idx >= c.columns.len) {
                                                pm[r] = false;
                                                continue :row_loop;
                                            }
                                            const col = c.columns[cond.col_idx];
                                            if (col.isRowNull(r)) {
                                                pm[r] = false;
                                                continue :row_loop;
                                            }
                                            const v: i64 = switch (col.data) {
                                                .int64 => |a| a[r],
                                                .uint64 => |a| @bitCast(a[r]),
                                                .bool_u8 => |a| @as(i64, a[r]),
                                                .date_u16 => |a| @as(i64, a[r]),
                                                .datetime64_ms => |a| a[r],
                                                else => {
                                                    pm[r] = false;
                                                    continue :row_loop;
                                                },
                                            };
                                            const pass = switch (cond.op) {
                                                .eq => v == cond.val,
                                                .neq => v != cond.val,
                                                .lt => v < cond.val,
                                                .lte => v <= cond.val,
                                                .gt => v > cond.val,
                                                .gte => v >= cond.val,
                                                .in2 => v == cond.val or v == cond.val2,
                                            };
                                            if (!pass) {
                                                pm[r] = false;
                                                continue :row_loop;
                                            }
                                        }
                                    }
                                    // Partial str cond pre-check (applied before LIKE to reduce expensive scan).
                                    if (psc_n > 0) {
                                        for (psc_buf[0..psc_n]) |cond| {
                                            if (cond.col_idx >= c.columns.len) {
                                                pm[r] = false;
                                                continue :row_loop;
                                            }
                                            const col = c.columns[cond.col_idx];
                                            const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                                .string => |a| a[r],
                                                else => continue,
                                            };
                                            const pass = switch (cond.op) {
                                                .eq => std.mem.eql(u8, s, cond.val),
                                                .neq => !std.mem.eql(u8, s, cond.val),
                                            };
                                            if (!pass) {
                                                pm[r] = false;
                                                continue :row_loop;
                                            }
                                        }
                                    }
                                    // LIKE guard pre-filter.
                                    for (guards) |lg| {
                                        if (lg.col_idx >= c.columns.len) {
                                            pm[r] = false;
                                            continue :row_loop;
                                        }
                                        const col = c.columns[lg.col_idx];
                                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                                        if (lg.matcher.match(s) == lg.negate) {
                                            pm[r] = false;
                                            continue :row_loop;
                                        }
                                    }
                                    for (refs) |j| {
                                        const col = c.columns[j];
                                        row_v[j] = if (col.isRowNull(r)) null else col.data.get(r);
                                    }
                                    const v = try kernels.evalExpr(fp, row_v, null, calloc);
                                    pm[r] = if (v) |val| val.bool_u8 != 0 else false;
                                }
                            } // end mixed_complete else
                        } // end sc_complete else
                    }
                    pass_mask = pm;
                }

                // Aggregate into local hash table.
                if (self.key_col_idx >= c.columns.len) continue;
                const key_col = c.columns[self.key_col_idx];
                if (key_col.data != .string) continue;
                const strs = key_col.data.string;

                // SIMD chunk-skip: pre-compute int-only inline filter mask.
                // For Q40 (4 conditions, ~1% pass rate) reduces agg iterations from
                // 10M to ~412K (~24x), skipping 99% of 32-row chunks via OR-reduce.
                var simd_inline_mask: ?[]i16 = null;
                if (self.use_inline_filter and self.inline_sc_n == 0 and self.inline_ic_n > 0) blk_sim: {
                    const _n = c.num_rows;
                    const _im = calloc.alloc(i16, _n) catch break :blk_sim;
                    const _ta = calloc.alloc(i16, _n) catch break :blk_sim;
                    const _tb = calloc.alloc(i16, _n) catch break :blk_sim;
                    @memset(_im, 1);
                    var _ok = true;
                    for (self.inline_ic[0..self.inline_ic_n]) |cond| {
                        if (!applyIntCondSIMD(&c, cond, _n, _im, _ta, _tb)) {
                            _ok = false;
                            break;
                        }
                    }
                    if (_ok) simd_inline_mask = _im;
                }
                // While replaces for: _r2 incremented before any continue so
                // continue :agg_loop always advances to the next row.
                var _r2: usize = 0;
                agg_loop: while (_r2 < c.num_rows) {
                    const r = _r2;
                    if (simd_inline_mask) |_sim| {
                        if (r & 31 == 0 and r + 32 <= c.num_rows) {
                            var _any: i16 = 0;
                            for (_sim[r .. r + 32]) |_v| _any |= _v;
                            if (_any == 0) {
                                _r2 += 32;
                                continue :agg_loop;
                            }
                        }
                    }
                    _r2 += 1;
                    // Inline fast filter: avoids separate pass_mask allocation+scan.
                    if (simd_inline_mask) |_sim| {
                        if (_sim[r] == 0) continue :agg_loop;
                    } else if (self.use_inline_filter) {
                        for (self.inline_ic[0..self.inline_ic_n]) |cond| {
                            if (cond.col_idx >= c.columns.len) continue :agg_loop;
                            const col = c.columns[cond.col_idx];
                            if (col.isRowNull(r)) continue :agg_loop;
                            const v: i64 = switch (col.data) {
                                .int64 => |a| a[r],
                                .uint64 => |a| @bitCast(a[r]),
                                .bool_u8 => |a| @as(i64, a[r]),
                                .date_u16 => |a| @as(i64, a[r]),
                                .datetime64_ms => |a| a[r],
                                else => continue :agg_loop,
                            };
                            const pass = switch (cond.op) {
                                .eq => v == cond.val,
                                .neq => v != cond.val,
                                .lt => v < cond.val,
                                .lte => v <= cond.val,
                                .gt => v > cond.val,
                                .gte => v >= cond.val,
                                .in2 => v == cond.val or v == cond.val2,
                            };
                            if (!pass) continue :agg_loop;
                        }
                        for (self.inline_sc[0..self.inline_sc_n]) |cond| {
                            if (cond.col_idx >= c.columns.len) continue :agg_loop;
                            const col = c.columns[cond.col_idx];
                            const s: []const u8 = if (col.isRowNull(r)) "" else switch (col.data) {
                                .string => |a| a[r],
                                else => continue :agg_loop,
                            };
                            const pass = switch (cond.op) {
                                .eq => std.mem.eql(u8, s, cond.val),
                                .neq => !std.mem.eql(u8, s, cond.val),
                            };
                            if (!pass) continue :agg_loop;
                        }
                    } else if (pass_mask) |pm| {
                        if (!pm[r]) continue :agg_loop;
                    }
                    if (key_col.isRowNull(r)) continue :agg_loop;
                    // When rr_active, extract URL domain from the raw string value.
                    const str_val: []const u8 = if (self.rr_active and self.rr_is_url_domain) blk: {
                        const raw = strs[r];
                        const after_proto = if (std.mem.startsWith(u8, raw, "https://"))
                            raw[8..]
                        else if (std.mem.startsWith(u8, raw, "http://"))
                            raw[7..]
                        else
                            break :blk raw;
                        const slash = std.mem.indexOfScalar(u8, after_proto, '/') orelse break :blk raw;
                        var host = after_proto[0..slash];
                        if (std.mem.startsWith(u8, host, "www.")) host = host[4..];
                        break :blk host;
                    } else strs[r];

                    // Evaluate optional CASE WHEN secondary string key.
                    const cw_str: []const u8 = if (self.cw_key) |*cwk| cwk.eval(&c, r) else "";

                    // Build composite key: [int_key_0:u64LE]...[cw_len:u16LE][cw_bytes][str_val_bytes]
                    // When no int keys and no CASE WHEN: use str_val directly (no alloc).
                    // When all int keys are constants: also use str_val directly (no prefix needed).
                    // When CASE WHEN present: encode as [int_prefix][cw_len:2B][cw_bytes][url_bytes].
                    // Composite key buffers are allocated from thread_key_arena (backed by
                    // c_allocator) instead of talloc.  talloc is freed when runWork returns,
                    // but the key pointers stored in local_ht must survive until after the
                    // tree-merge and emit phase.  thread_key_arena is freed by the caller
                    // (executeHashAggParallelStrKey) only after iterateWithSlot completes.
                    const composite_key: []const u8 = if (self.cw_key != null) blk: {
                        const total_len = int_prefix_len + 2 + cw_str.len + str_val.len;
                        const kbuf = try self.thread_key_arena.allocator().alloc(u8, total_len);
                        if (!all_const_int_keys) {
                            for (int_key_specs, 0..) |spec, ki| {
                                const ival: u64 = if (!spec.is_col) spec.const_val else blk2: {
                                    const col = c.columns[spec.col_idx];
                                    break :blk2 if (col.isRowNull(r)) 0 else switch (col.data) {
                                        .int64 => |a| @bitCast(a[r]),
                                        .uint64 => |a| a[r],
                                        .bool_u8 => |a| @as(u64, a[r]),
                                        .date_u16 => |a| @as(u64, a[r]),
                                        .datetime64_ms => |a| @bitCast(a[r]),
                                        else => 0,
                                    };
                                };
                                std.mem.writeInt(u64, kbuf[ki * 8 .. ki * 8 + 8][0..8], ival, .little);
                            }
                        }
                        std.mem.writeInt(u16, kbuf[int_prefix_len .. int_prefix_len + 2][0..2], @intCast(@min(cw_str.len, 65535)), .little);
                        @memcpy(kbuf[int_prefix_len + 2 .. int_prefix_len + 2 + cw_str.len], cw_str);
                        @memcpy(kbuf[int_prefix_len + 2 + cw_str.len ..], str_val);
                        break :blk kbuf;
                    } else if (int_key_n == 0 or all_const_int_keys) str_val else blk: {
                        const total_len = int_prefix_len + str_val.len;
                        const kbuf = try self.thread_key_arena.allocator().alloc(u8, total_len);
                        for (int_key_specs, 0..) |spec, ki| {
                            const ival: u64 = if (!spec.is_col) spec.const_val else blk2: {
                                const col = c.columns[spec.col_idx];
                                break :blk2 if (col.isRowNull(r)) 0 else switch (col.data) {
                                    .int64 => |a| @bitCast(a[r]),
                                    .uint64 => |a| a[r],
                                    .bool_u8 => |a| @as(u64, a[r]),
                                    .date_u16 => |a| @as(u64, a[r]),
                                    .datetime64_ms => |a| @bitCast(a[r]),
                                    else => 0,
                                };
                            };
                            std.mem.writeInt(u64, kbuf[ki * 8 .. ki * 8 + 8][0..8], ival, .little);
                        }
                        @memcpy(kbuf[int_prefix_len..], str_val);
                        break :blk kbuf;
                    };

                    const res = try self.local_ht.getOrInsert(composite_key, self.compact_init_vals);
                    try updateCompactVals(res.vals, self.compact_kinds, self.aggs, &c, r, &self.local_ht, res.slot, self.sidecar_idx);
                    // COUNT(DISTINCT) dedup: collect (group_hash, distinct_val) pairs.
                    if (self.has_count_distinct) {
                        const group_h = self.local_ht.tags[res.slot];
                        for (0..self.distinct_n) |di| {
                            const col_idx = self.distinct_col_idx_buf[di];
                            if (col_idx >= c.columns.len) continue;
                            const acol = c.columns[col_idx];
                            if (acol.isRowNull(r)) continue;
                            const dval: u64 = switch (acol.data) {
                                .int64 => |v| @bitCast(v[r]),
                                .uint64 => |v| v[r],
                                else => continue,
                            };
                            const pair: u128 = (@as(u128, group_h) << 64) | @as(u128, dval);
                            const bucket: usize = @as(u64, @truncate(group_h)) & 63;
                            try self.scatter_bufs[bucket].append(std.heap.c_allocator, pair);
                        }
                    }
                }
            }
        }
    };

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const pctxs = try alloc.alloc(ParStrCtx, n_threads);

    // Pre-extract inline filter conditions (mixed int+str AND predicates).
    // When complete, threads skip pass_mask allocation and check inline in the agg loop.
    var pre_inline_ic: [16]IntCmpCond = undefined;
    var pre_inline_ic_n: usize = 0;
    var pre_inline_sc: [8]StrCmpCond = undefined;
    var pre_inline_sc_n: usize = 0;
    var use_inline_filter = false;
    if (filter_pred) |fp| {
        if (extractMixedAndConds(fp, &pre_inline_ic, &pre_inline_ic_n, &pre_inline_sc, &pre_inline_sc_n)) {
            use_inline_filter = true;
        }
    }

    // Pre-extract COUNT(DISTINCT) agg column indices.
    // has_count_distinct was already computed above (before the two-phase call site).
    var distinct_col_idx_buf: [4]usize = undefined;
    var distinct_agg_ci_buf: [4]usize = undefined;
    var distinct_n: usize = 0;
    if (has_count_distinct) {
        for (aggs, 0..) |item, ci| {
            if (compact_kinds[ci] == .count_distinct_u64) {
                if (item.expr == .agg_call) {
                    const arg = item.expr.agg_call.arg orelse continue;
                    if (arg == .col_ref and distinct_n < 4) {
                        distinct_col_idx_buf[distinct_n] = arg.col_ref.index;
                        distinct_agg_ci_buf[distinct_n] = ci;
                        distinct_n += 1;
                    }
                }
            }
        }
    }

    for (pctxs) |*pc| {
        pc.* = .{
            .source = ctx.source,
            .filter_pred = if (use_inline_filter) null else filter_pred,
            .key_col_idx = key_col_idx,
            .keys = keys,
            .str_key_pos = str_key_pos,
            .aggs = aggs,
            .compact_kinds = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .sidecar_idx = sidecar_idx,
            .morsel_src = &morsel_src,
            .parent_alloc = alloc,
            // Pre-size local_ht to avoid repeated grow() calls.
            // Using total_rows / n_threads as upper bound (per-thread unique groups
            // ≤ per-thread rows). Capped at 262_144 to limit arena usage.
            .local_ht = try ht.StrAggHashTable.initWithCapacity(
                alloc,
                aggs.len,
                num_str_aggs,
                @min(total_rows / @as(u64, n_threads), 262_144),
            ),
            .has_count_distinct = has_count_distinct,
            .distinct_n = distinct_n,
            .use_inline_filter = use_inline_filter,
            .cw_key = cw_key,
            .cw_key_pos = cw_key_pos,
            .rr_active = rr_active,
            .rr_col_idx = rr_col_idx,
            .rr_is_url_domain = rr_is_url_domain,
        };
        // Zero-init scatter buffers (empty ArrayListUnmanaged = no allocation).
        for (&pc.scatter_bufs) |*b| b.* = std.ArrayListUnmanaged(u128).empty;
        // Each thread gets its own key arena (backed by c_allocator, which is thread-safe).
        // Composite key buffers (int_prefix + str bytes) live here until after emit.
        pc.thread_key_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
        if (has_count_distinct) {
            @memcpy(pc.distinct_col_idx_buf[0..distinct_n], distinct_col_idx_buf[0..distinct_n]);
            @memcpy(pc.distinct_agg_ci_buf[0..distinct_n], distinct_agg_ci_buf[0..distinct_n]);
        }
        if (use_inline_filter) {
            @memcpy(pc.inline_ic[0..pre_inline_ic_n], pre_inline_ic[0..pre_inline_ic_n]);
            pc.inline_ic_n = pre_inline_ic_n;
            @memcpy(pc.inline_sc[0..pre_inline_sc_n], pre_inline_sc[0..pre_inline_sc_n]);
            pc.inline_sc_n = pre_inline_sc_n;
        }
    }

    try parallel.parallelFor(alloc, ParStrCtx, ParStrCtx.work, pctxs, &morsel_src);
    for (pctxs) |*pc| {
        if (pc.err) |e| return e;
    }
    // Defer cleanup of per-thread key arenas.  Must happen AFTER iterateWithSlot (emit)
    // because composite key bytes stored in local_ht.key_slots are borrowed from these arenas.
    defer for (pctxs) |*pc| pc.thread_key_arena.deinit();

    // Allocate N_PARTS result hash tables in function scope so they're visible
    // to the COUNT(DISTINCT) reconciliation and emit sections below.
    const N_PARTS: usize = @min(n_threads, 8);
    const part_arenas = try alloc.alloc(std.heap.ArenaAllocator, N_PARTS);
    for (part_arenas) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    // part_arenas freed BEFORE thread_key_arenas (LIFO defer order) — safe because
    // freeing part_arenas only releases the HT arrays, not the key string bytes.
    defer for (part_arenas) |*a| a.deinit();
    const part_hts = try alloc.alloc(ht.StrAggHashTable, N_PARTS);

    // ── Partitioned parallel reduce (replaces tournament tree-merge) ──────────
    // N_PARTS independent partitions: worker p processes ALL pctxs' local_hts
    // but only accumulates entries where (tag % N_PARTS) == p. Each worker
    // builds its own part_hts[p] exclusively — no shared writes, no serial
    // bottleneck. Critical for high-cardinality groups (Q40: 354K, Q14: 500K).
    //
    // Cost: each worker scans n_threads × local_ht.capacity tags (sequential/hot)
    // and inserts ~(total_unique / N_PARTS) entries into its private result HT.
    // Wall-clock time ≈ O(total_unique / N_PARTS) vs tree-merge O(total_unique).
    {
        var total_local_count: u64 = 0;
        for (pctxs) |*pc| total_local_count += @as(u64, @intCast(pc.local_ht.count));
        const est_per_part: u64 = total_local_count / N_PARTS + 64;

        for (part_arenas, part_hts) |*a, *h| {
            h.* = try ht.StrAggHashTable.initWithCapacity(a.allocator(), aggs.len, num_str_aggs, est_per_part);
        }

        const PartReduceCtx = struct {
            pctxs_view: []const ParStrCtx,
            part_hts: []ht.StrAggHashTable,
            n_parts: usize,
            compact_kinds: []const ht.CompactAggKind,
            compact_init_vals: []const u64,
            morsel_src: *parallel.MorselSource,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.runWork() catch |e| {
                    self.err = e;
                };
            }
            fn runWork(self: *@This()) !void {
                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |p| {
                        for (self.pctxs_view) |*pc| {
                            try self.part_hts[p].mergeFromPart(
                                &pc.local_ht,
                                self.compact_kinds,
                                self.compact_init_vals,
                                p,
                                self.n_parts,
                            );
                        }
                    }
                }
            }
        };

        var part_morsel = parallel.MorselSource.init(N_PARTS, 1);
        const pr_ctxs = try alloc.alloc(PartReduceCtx, n_threads);
        for (pr_ctxs) |*c| {
            c.* = .{
                .pctxs_view = pctxs,
                .part_hts = part_hts,
                .n_parts = N_PARTS,
                .compact_kinds = compact_kinds,
                .compact_init_vals = compact_init_vals,
                .morsel_src = &part_morsel,
            };
        }
        try parallel.parallelFor(alloc, PartReduceCtx, PartReduceCtx.work, pr_ctxs, &part_morsel);
        for (pr_ctxs) |*c| {
            if (c.err) |e| return e;
        }
        // After partitioned reduce, part_hts[0..N_PARTS] collectively hold all merged entries.
    }

    // COUNT(DISTINCT) reconciliation via parallel scatter-sort-sweep.
    // Each scatter_bufs[p] holds pairs with (group_hash & 63) == p.
    // We sort each partition and sweep to count unique (group_h, dval) pairs per group.
    // Partitions are processed in parallel (one per thread) to amortise the O(N log N) sort cost.
    if (has_count_distinct and distinct_n > 0) {
        const SortSweepCtx = struct {
            pctxs_ptr: []ParStrCtx,
            local_cd_counts: std.AutoHashMap(u64, u64),
            morsel_src: *parallel.MorselSource,
            err: ?anyerror = null,

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.runWork() catch |e| {
                    self.err = e;
                };
            }
            fn runWork(self: *@This()) !void {
                var tmp_buf = std.ArrayListUnmanaged(u128).empty;
                defer tmp_buf.deinit(std.heap.c_allocator);
                var scratch_buf = std.ArrayListUnmanaged(u128).empty;
                defer scratch_buf.deinit(std.heap.c_allocator);
                while (self.morsel_src.next()) |m| {
                    for (m.start..m.end) |p| {
                        tmp_buf.clearRetainingCapacity();
                        for (self.pctxs_ptr) |*pc| {
                            try tmp_buf.appendSlice(std.heap.c_allocator, pc.scatter_bufs[p].items);
                        }
                        if (tmp_buf.items.len == 0) continue;
                        // Use radix sort instead of comparison sort: O(N) vs O(N log N).
                        // For ~140K u128 items per partition this is ~3× faster.
                        try scratch_buf.resize(std.heap.c_allocator, tmp_buf.items.len);
                        radixSortU128(tmp_buf.items, scratch_buf.items);
                        var prev: u128 = tmp_buf.items[0];
                        var prev_group_h: u64 = @truncate(prev >> 64);
                        var count: u64 = 1;
                        for (tmp_buf.items[1..]) |pair| {
                            if (pair == prev) continue;
                            const group_h: u64 = @truncate(pair >> 64);
                            if (group_h != prev_group_h) {
                                const gop = try self.local_cd_counts.getOrPutValue(prev_group_h, 0);
                                gop.value_ptr.* += count;
                                prev_group_h = group_h;
                                count = 0;
                            }
                            count += 1;
                            prev = pair;
                        }
                        const gop = try self.local_cd_counts.getOrPutValue(prev_group_h, 0);
                        gop.value_ptr.* += count;
                    }
                }
            }
        };

        var ss_morsel_src = parallel.MorselSource.init(64, 1);
        const ss_ctxs = try alloc.alloc(SortSweepCtx, n_threads);
        for (ss_ctxs) |*c| {
            c.* = .{
                .pctxs_ptr = pctxs,
                .local_cd_counts = std.AutoHashMap(u64, u64).init(std.heap.c_allocator),
                .morsel_src = &ss_morsel_src,
            };
        }

        try parallel.parallelFor(alloc, SortSweepCtx, SortSweepCtx.work, ss_ctxs, &ss_morsel_src);
        for (ss_ctxs) |*c| {
            if (c.err) |e| return e;
        }

        // Merge per-thread cd_counts into a single map (serial: small, O(unique groups)).
        var cd_counts = std.AutoHashMap(u64, u64).init(alloc);
        defer cd_counts.deinit();
        for (ss_ctxs) |*c| {
            var it = c.local_cd_counts.iterator();
            while (it.next()) |entry| {
                const gop = try cd_counts.getOrPutValue(entry.key_ptr.*, 0);
                gop.value_ptr.* += entry.value_ptr.*;
            }
            c.local_cd_counts.deinit();
        }

        // Free scatter buffers.
        for (pctxs) |*pc| {
            for (&pc.scatter_bufs) |*b| b.deinit(std.heap.c_allocator);
        }
        // Update COUNT(DISTINCT) slots in all partitioned result hash tables.
        for (part_hts) |*pht| {
            for (0..pht.capacity) |slot| {
                if (pht.tags[slot] == 0) continue; // EMPTY_TAG
                const group_h = pht.tags[slot];
                const count_val = cd_counts.get(group_h) orelse 0;
                for (0..distinct_n) |di| {
                    const ci = distinct_agg_ci_buf[di];
                    pht.vals_flat[slot * pht.num_aggs + ci] = count_val;
                }
            }
        }
    } else if (has_count_distinct) {
        // Clean up scatter buffers if distinct_n == 0 (shouldn't happen, but be safe).
        for (pctxs) |*pc| {
            for (&pc.scatter_bufs) |*b| b.deinit(std.heap.c_allocator);
        }
    }

    // Emit result.
    const out_metas = try buildAggResultMetas(keys, aggs, alloc);
    var rl = RowBuffer.init(out_metas);

    // Streaming top-K: for `ORDER BY count DESC LIMIT k`, keep only the best
    // K groups while emitting instead of materialising every group into RowBuffer.
    var topk: ?TopKCollector = null;
    var topk_sort_ai: ?usize = null;
    if (top_k > 0 and sort_keys.len == 1 and sort_keys[0].desc) {
        const sc = sort_keys[0].col_idx;
        if (sc >= keys.len and sc < keys.len + aggs.len and compact_kinds[sc - keys.len] == .count) {
            topk = try TopKCollector.init(alloc, sort_keys, top_k);
            topk_sort_ai = sc - keys.len;
        }
    }

    // Build emit_int_key_n for emit decoding (same order as in runWork: col_ref non-str + lit_i64).
    // When all int keys are constants, int_prefix_len2 = 0 (not encoded in composite key).
    var emit_int_key_n: usize = 0;
    var emit_all_const = true;
    for (keys) |k| {
        switch (k.expr) {
            .col_ref => |cr| {
                if (cr.index != key_col_idx) {
                    emit_int_key_n += 1;
                    emit_all_const = false;
                }
            },
            .lit_i64 => emit_int_key_n += 1,
            else => {},
        }
    }
    if (emit_int_key_n == 0) emit_all_const = true;
    const int_prefix_len2: usize = if (emit_all_const) 0 else emit_int_key_n * 8;

    const EmitCtx = struct {
        rl: *RowBuffer,
        topk: ?*TopKCollector,
        topk_sort_ai: ?usize,
        alloc: std.mem.Allocator,
        aggs: []const plan.ProjectItem,
        kinds: []const ht.CompactAggKind,
        str_ht: *ht.StrAggHashTable,
        sidecar_idx: []const usize,
        keys: []const plan.ProjectItem,
        str_key_pos: usize,
        cw_key_pos: usize,
        has_cw: bool,
        int_prefix: usize, // bytes
        all_const_ints: bool, // true when all int keys are lit_i64 constants (not in composite key)
        sm: []const result.ColMeta,
    };
    const sm_emit = ctx.source.schema();
    var emit_ctx = EmitCtx{
        .rl = &rl,
        .topk = if (topk) |*tk| tk else null,
        .topk_sort_ai = topk_sort_ai,
        .alloc = alloc,
        .aggs = aggs,
        .kinds = compact_kinds,
        .str_ht = &part_hts[0], // updated per partition in the emit loop below
        .sidecar_idx = sidecar_idx,
        .keys = keys,
        .str_key_pos = str_key_pos,
        .cw_key_pos = cw_key_pos,
        .has_cw = cw_key != null,
        .int_prefix = int_prefix_len2,
        .all_const_ints = emit_all_const,
        .sm = sm_emit,
    };
    const EmitCb = struct {
        fn cb(ec: *EmitCtx, composite: []const u8, vals: []const u64, slot: usize) void {
            if (ec.topk) |tk| {
                if (ec.topk_sort_ai) |ai| {
                    if (ai < vals.len and tk.shouldSkipCount(vals[ai])) return;
                }
            }
            const row = ec.alloc.alloc(?Value, ec.keys.len + vals.len) catch return;
            // Decode composite key into row slots.
            // Format (with CASE WHEN):   [int_prefix][cw_len:u16LE][cw_bytes][str_bytes]
            // Format (without CASE WHEN): [int_prefix][str_bytes]
            const cw_len: usize = if (ec.has_cw and composite.len >= ec.int_prefix + 2)
                @as(usize, std.mem.readInt(u16, composite[ec.int_prefix .. ec.int_prefix + 2][0..2], .little))
            else
                0;
            const cw_start: usize = ec.int_prefix + (if (ec.has_cw) @as(usize, 2) else 0);
            const str_start: usize = cw_start + cw_len;
            // Strings extracted from composite may point into thread_key_arena which
            // is freed by a defer AFTER return — dupe them into ec.alloc so the
            // returned RowBuffer owns its string values.
            const cw_str_raw: []const u8 = if (ec.has_cw and cw_start + cw_len <= composite.len)
                composite[cw_start .. cw_start + cw_len]
            else
                "";
            const str_val_raw: []const u8 = if (str_start <= composite.len)
                composite[str_start..]
            else
                "";
            const cw_str: []const u8 = ec.alloc.dupe(u8, cw_str_raw) catch cw_str_raw;
            const str_val: []const u8 = ec.alloc.dupe(u8, str_val_raw) catch str_val_raw;
            var int_ki: usize = 0;
            for (ec.keys, 0..) |k, ki| {
                if (ec.has_cw and ki == ec.cw_key_pos) {
                    row[ki] = Value{ .string = cw_str };
                    continue;
                }
                if (k.expr != .col_ref and k.expr != .lit_i64) {
                    // fn_call key (e.g. regexp_replace domain): emit the str_val stored in composite key.
                    if (ki == ec.str_key_pos) {
                        row[ki] = Value{ .string = str_val };
                        continue;
                    }
                    row[ki] = Value{ .int64 = 0 };
                    continue;
                }
                if (k.expr == .lit_i64) {
                    if (ec.all_const_ints) {
                        // Constant key — value is just the literal (not stored in composite key).
                        row[ki] = Value{ .int64 = k.expr.lit_i64 };
                    } else {
                        // Constant stored in int prefix of composite key.
                        const ival = std.mem.readInt(u64, composite[int_ki * 8 .. int_ki * 8 + 8][0..8], .little);
                        row[ki] = Value{ .int64 = @bitCast(ival) };
                        int_ki += 1;
                    }
                    continue;
                }
                const ci = k.expr.col_ref.index;
                if (ki == ec.str_key_pos) {
                    row[ki] = Value{ .string = str_val };
                } else {
                    const ival = std.mem.readInt(u64, composite[int_ki * 8 .. int_ki * 8 + 8][0..8], .little);
                    row[ki] = if (ci < ec.sm.len) switch (ec.sm[ci].col_type) {
                        .int64 => Value{ .int64 = @bitCast(ival) },
                        .uint64 => Value{ .uint64 = ival },
                        .date_u16 => Value{ .date_u16 = @truncate(ival) },
                        .bool_u8 => Value{ .bool_u8 = @truncate(ival) },
                        .datetime64_ms => Value{ .datetime64_ms = @bitCast(ival) },
                        else => Value{ .int64 = @bitCast(ival) },
                    } else Value{ .int64 = @bitCast(ival) };
                    int_ki += 1;
                }
            }
            emitCompactValsWithSidecar(vals, ec.kinds, ec.aggs, row[ec.keys.len..], ec.str_ht, slot, ec.sidecar_idx);
            if (ec.topk) |tk| tk.offer(row) else ec.rl.append(ec.alloc, row) catch {};
        }
    };
    // Emit from all N_PARTS result hash tables.
    for (part_hts) |*pht| {
        emit_ctx.str_ht = pht;
        pht.iterateWithSlot(&emit_ctx, EmitCb.cb);
    }

    if (topk) |*tk| return try tk.finish(out_metas);
    return try finalizeAggRows(rl, sort_keys, top_k, alloc);
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

pub fn executeTwoPhaseHashAggStrKeySimple(
    keys: []const plan.ProjectItem,
    aggs: []const plan.ProjectItem,
    sort_keys: []const plan.SortKey,
    top_k: usize,
    ctx: *QueryContext,
    key_col_idx: usize,
    str_key_pos: usize,
    compact_kinds: []const ht.CompactAggKind,
    compact_init_vals: []const u64,
    str_filter: ?SimpleStrFilter,
    int_filter: ?[]const IntCmpCond,
    /// Optional single integer sidecar key column index (e.g. SearchEngineID for Q15).
    /// When set, the sidecar value is XOR'd into the hash and stored in the scatter record.
    sidecar_col_idx: ?usize,
    /// Index within `keys` of the sidecar key (used at emit time).
    sidecar_key_pos: ?usize,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;

    const N_PARTS: usize = 64;
    const total_rows = ctx.source.rowCount();
    const n_threads = parallel.defaultThreads();
    const alloc = ctx.allocator();
    const n_aggs = aggs.len;
    // count_only: when the sole agg is COUNT(*), skip writing/reading the count
    // slot in scatter records (stride 4→3, 25 % less bandwidth for Q34/Q35).
    const count_only: bool = n_aggs == 1 and compact_kinds[0] == .count;
    // has_sidecar: an extra int key (e.g. SearchEngineID for Q15) is packed
    // into the scatter record at offset 3, shifting aggs by one slot.
    const has_sidecar: bool = sidecar_col_idx != null;
    // Scatter record layout: [hash:u64, str_ptr:u64, str_len:u64, (sidecar?:u64), agg0:u64, ...]
    // count_only skips the agg0 slot; has_sidecar adds 1 slot before aggs.
    const row_stride: usize = (if (count_only) 3 else 3 + n_aggs) + @as(usize, if (has_sidecar) 1 else 0);

    // COUNT DISTINCT: single agg is count_distinct_u64.
    // scatter record: [hash:u64, str_ptr:u64, str_len:u64, (sidecar?:u64), raw_dval:u64]
    // Phase 2 uses sort-sweep instead of HT-insert for exact deduplication.
    const is_count_distinct: bool = n_aggs == 1 and compact_kinds.len > 0 and
        compact_kinds[0] == .count_distinct_u64;

    // Per-agg: source column index and aggregation kind.
    const AggInfo = struct { col_idx: usize, kind: ht.CompactAggKind };
    const agg_infos = try alloc.alloc(AggInfo, n_aggs);
    for (aggs, compact_kinds, agg_infos) |ag, kind, *info| {
        const ac = ag.expr.agg_call;
        info.* = .{
            .col_idx = if (ac.arg != null and ac.arg.? == .col_ref)
                ac.arg.?.col_ref.index
            else
                ~@as(usize, 0),
            .kind = kind,
        };
    }

    // ── Phase 1: parallel scatter ─────────────────────────────────────────────
    const ScatterCtx = struct {
        bufs: [N_PARTS]std.ArrayListUnmanaged(u64),
        buf_arena: std.heap.ArenaAllocator,
        source: SourceIface,
        morsel_src: *parallel.MorselSource,
        parent_alloc: std.mem.Allocator,
        key_col_idx: usize,
        str_filter: ?SimpleStrFilter,
        int_filter: ?[]const IntCmpCond,
        sidecar_col_idx: ?usize,
        count_only: bool,
        n_aggs: usize,
        row_stride: usize,
        agg_infos: []const AggInfo,
        err: ?anyerror = null,
        // Raw int column data for fast SIMD filter (bypasses i16→i64 / i32→i64 widen in fetchRange).
        // int16 → 32-lane SIMD; int32 → 16-lane SIMD; vs 8-lane for widened i64.
        raw_ic_i16: [16]?[]const i16 = @as([16]?[]const i16, @splat(null)),
        raw_ic_i32: [16]?[]const i32 = @as([16]?[]const i32, @splat(null)),
        // Raw string data for key column: skip building 122 880-entry fat-pointer array in fetchRange.
        // When set, key_col in DataChunk is decoded as bool_u8 (1=non-empty) via setStringNonEmptyBool.
        raw_key_offsets: ?[]const u64 = null,
        raw_key_bytes: ?[]const u8 = null,
        // Raw integer slice for sidecar column (e.g. MobilePhone in Q12).
        // When non-null, allows skip_fetch even for composite (string + int) GROUP BY.
        raw_sidecar: ?RawColSlice = null,
        raw_distinct: ?RawColSlice = null,
        // When true, skip fetchRange entirely — all conditions covered by raw slices.
        // Only set when: all int conditions have raw i16/i32 slices, use_raw_key, count_only,
        // sidecar (if any) has raw int64 slice, and str_filter (if any) is on the key column.
        skip_fetch: bool = false,

        fn cmpI64(op: @TypeOf(@as(IntCmpCond, undefined).op), lhs: i64, rhs: i64, rhs2: i64) bool {
            return switch (op) {
                .eq => lhs == rhs,
                .neq => lhs != rhs,
                .lt => lhs < rhs,
                .lte => lhs <= rhs,
                .gt => lhs > rhs,
                .gte => lhs >= rhs,
                .in2 => lhs == rhs or lhs == rhs2,
            };
        }

        fn rawCondPass(self: *const @This(), cond: IntCmpCond, ci: usize, abs: usize) bool {
            if (self.raw_ic_i16[ci]) |raw16| {
                return cmpI64(cond.op, raw16[abs], cond.val, cond.val2);
            }
            if (self.raw_ic_i32[ci]) |raw32| {
                return cmpI64(cond.op, raw32[abs], cond.val, cond.val2);
            }
            return false;
        }

        fn appendRawRecord(self: *@This(), ba: std.mem.Allocator, abs: usize) !void {
            const key_off = self.raw_key_offsets.?;
            if (self.str_filter != null and key_off[abs + 1] == key_off[abs]) return;
            const key_bytes = self.raw_key_bytes.?;
            const s = key_bytes[key_off[abs]..key_off[abs + 1]];
            const h: u64 = blk: {
                const base_h = ht.StrAggHashTable.hashStr(s);
                if (self.raw_sidecar) |rsid| {
                    const sv: u64 = @bitCast(rsid.getI64(abs));
                    break :blk base_h ^ (sv *% 0x9e3779b97f4a7c15);
                }
                break :blk base_h;
            };
            const part_id = @as(usize, @truncate(h)) & (N_PARTS - 1);
            if (self.raw_distinct) |rd| {
                if (self.raw_sidecar) |rsid| {
                    var rec5: [5]u64 = .{ h, @intFromPtr(s.ptr), s.len, @bitCast(rsid.getI64(abs)), @bitCast(rd.getI64(abs)) };
                    try self.bufs[part_id].appendSlice(ba, &rec5);
                } else {
                    var rec4: [4]u64 = .{ h, @intFromPtr(s.ptr), s.len, @bitCast(rd.getI64(abs)) };
                    try self.bufs[part_id].appendSlice(ba, &rec4);
                }
            } else if (self.raw_sidecar) |rsid| {
                var rec4: [4]u64 = .{ h, @intFromPtr(s.ptr), s.len, @bitCast(rsid.getI64(abs)) };
                try self.bufs[part_id].appendSlice(ba, &rec4);
            } else {
                var rec3: [3]u64 = .{ h, @intFromPtr(s.ptr), s.len };
                try self.bufs[part_id].appendSlice(ba, &rec3);
            }
        }

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.doWork() catch |e| {
                self.err = e;
            };
        }

        fn doWork(self: *@This()) !void {
            const ba = self.buf_arena.allocator();
            var ta = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer ta.deinit();
            const tall = ta.allocator();
            // SIMD scratch buffers — allocated once per thread, reused across morsels.
            const rs_sz = parallel.default_morsel_size + 1;
            const rs_mask = try tall.alloc(i16, rs_sz);
            const rs_tmp_a = try tall.alloc(i16, rs_sz);
            const rs_tmp_b = try tall.alloc(i16, rs_sz);
            const rs_sel = try tall.alloc(u32, rs_sz);

            // Fast-path flags set from pre-fetched raw column data.
            const use_raw_key: bool = self.raw_key_offsets != null and self.raw_key_bytes != null;
            const n_raw_ic: usize = if (self.int_filter) |ics| ics.len else 0;

            // Fast path: skip fetchRange entirely when all conditions are satisfied by raw slices.
            // For Q38-class queries (count_only + all-raw int filter + raw string key): avoids
            // per-morsel allocation + 105-column fetchRange decode; pure SIMD filter + raw reads.
            if (self.skip_fetch) {
                while (self.morsel_src.next()) |m| {
                    const nr: usize = m.end - m.start;
                    if (self.int_filter) |ics| selective_blk: {
                        if (ics.len < 2) break :selective_blk;
                        var seed_i: ?usize = null;
                        for (ics, 0..) |cond, ci| {
                            if (cond.op == .eq and (self.raw_ic_i16[ci] != null or self.raw_ic_i32[ci] != null)) {
                                seed_i = ci;
                                break;
                            }
                        }
                        const si = seed_i orelse break :selective_blk;
                        const seed = ics[si];
                        if (self.raw_ic_i16[si]) |raw16| {
                            const rhs: i16 = @intCast(seed.val);
                            cmpBatchDispatch(i16, raw16[m.start .. m.start + nr], seed.op, rhs, 0, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                        } else if (self.raw_ic_i32[si]) |raw32| {
                            const rhs: i32 = @intCast(seed.val);
                            cmpBatchDispatch(i32, raw32[m.start .. m.start + nr], seed.op, rhs, 0, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                        } else break :selective_blk;
                        var sel_len: usize = 0;
                        for (rs_tmp_a[0..nr], 0..) |pass, ri| {
                            if (pass != 0) {
                                rs_sel[sel_len] = @intCast(ri);
                                sel_len += 1;
                            }
                        }
                        if (sel_len * 4 >= nr) break :selective_blk;

                        var out_len: usize = 0;
                        for (rs_sel[0..sel_len]) |ri_u32| {
                            const ri: usize = @intCast(ri_u32);
                            const abs = m.start + ri;
                            var ok = true;
                            for (ics, 0..) |cond, ci| {
                                if (ci == si) continue;
                                if (!self.rawCondPass(cond, ci, abs)) {
                                    ok = false;
                                    break;
                                }
                            }
                            if (!ok) continue;
                            rs_sel[out_len] = ri_u32;
                            out_len += 1;
                        }
                        for (rs_sel[0..out_len]) |ri_u32| {
                            try self.appendRawRecord(ba, m.start + @as(usize, @intCast(ri_u32)));
                        }
                        continue;
                    }
                    @memset(rs_mask[0..nr], 1);
                    if (self.int_filter) |ics| {
                        for (ics, 0..) |cond, ci| {
                            if (self.raw_ic_i16[ci]) |raw16| {
                                const rhs: i16 = @intCast(cond.val);
                                const rhs2: i16 = if (cond.op == .in2) @intCast(cond.val2) else 0;
                                cmpBatchDispatch(i16, raw16[m.start .. m.start + nr], cond.op, rhs, rhs2, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                                simd_batch.andMasks(rs_mask[0..nr], rs_tmp_a[0..nr], rs_mask[0..nr]);
                            } else if (self.raw_ic_i32[ci]) |raw32| {
                                const rhs: i32 = @intCast(cond.val);
                                const rhs2: i32 = if (cond.op == .in2) @intCast(cond.val2) else 0;
                                cmpBatchDispatch(i32, raw32[m.start .. m.start + nr], cond.op, rhs, rhs2, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                                simd_batch.andMasks(rs_mask[0..nr], rs_tmp_a[0..nr], rs_mask[0..nr]);
                            }
                        }
                    }
                    // Post-filter early-exit: skip morsel if all rows were filtered out.
                    // Mirrors the same pattern in executeTwoPhaseHashAggWithCW.
                    {
                        var any_pass = false;
                        var ri: usize = 0;
                        while (ri + 32 <= nr) : (ri += 32) {
                            const v: @Vector(32, i16) = rs_mask[ri..][0..32].*;
                            if (@reduce(.Or, v) != 0) {
                                any_pass = true;
                                break;
                            }
                        }
                        if (!any_pass) while (ri < nr) : (ri += 1) {
                            if (rs_mask[ri] != 0) {
                                any_pass = true;
                                break;
                            }
                        };
                        if (!any_pass) continue;
                    }
                    const CHUNK = 32;
                    var r: usize = 0;
                    while (r + CHUNK <= nr) : (r += CHUNK) {
                        const v: @Vector(CHUNK, i16) = rs_mask[r..][0..CHUNK].*;
                        if (@reduce(.Or, v) == 0) continue;
                        for (r..r + CHUNK) |ri| {
                            if (rs_mask[ri] == 0) continue;
                            try self.appendRawRecord(ba, m.start + ri);
                        }
                    }
                    while (r < nr) : (r += 1) {
                        if (rs_mask[r] == 0) continue;
                        try self.appendRawRecord(ba, m.start + r);
                    }
                }
                return;
            }

            // ── Standard path (fetchRange) ─────────────────────────────────────────
            while (self.morsel_src.next()) |m| {
                var ca = std.heap.ArenaAllocator.init(tall);
                defer ca.deinit();
                var c: DataChunk = undefined;
                try self.source.fetchRange(m.start, m.end - m.start, &c, ca.allocator());

                if (self.key_col_idx >= c.columns.len) continue;
                const key_col = c.columns[self.key_col_idx];
                // Accept .bool_u8 when key column was switched to non-empty bool via setStringNonEmptyBool
                // (raw_key path); otherwise require .string for the DataChunk fallback path.
                if (use_raw_key) {
                    if (key_col.data != .string and key_col.data != .bool_u8) continue;
                } else {
                    if (key_col.data != .string) continue;
                }
                const strs: [][]const u8 = if (key_col.data == .string) key_col.data.string else &.{};
                const nr = c.num_rows;

                // Resolve filter column once per chunk.
                const sf_col: ?chunk.Column = if (self.str_filter) |sf|
                    if (sf.col_idx < c.columns.len) c.columns[sf.col_idx] else null
                else
                    null;

                var rec: [64]u64 = undefined;

                // ── SIMD int pre-filter → i16 mask ──────────────────────────────────
                // For int16/int32 columns, use raw mmap'd slices (32-lane / 16-lane SIMD)
                // instead of the widened i64 DataChunk column (8-lane SIMD).  For Q38:
                // CounterID(int32)→16-lane, DontCountHits/IsRefresh(int16)→32-lane (vs 8-lane).
                // Then 32-lane OR-reduce chunk-skip skips all-zero 32-row blocks cheaply.
                const have_int_mask: bool = if (self.int_filter) |ics| blk: {
                    @memset(rs_mask[0..nr], 1);
                    for (ics, 0..) |cond, ci| {
                        var handled = false;
                        if (ci < n_raw_ic) {
                            if (self.raw_ic_i16[ci]) |raw16| {
                                if (cond.val >= std.math.minInt(i16) and cond.val <= std.math.maxInt(i16)) {
                                    const rhs: i16 = @intCast(cond.val);
                                    const rhs2: i16 = if (cond.op == .in2 and
                                        cond.val2 >= std.math.minInt(i16) and cond.val2 <= std.math.maxInt(i16))
                                        @intCast(cond.val2)
                                    else
                                        0;
                                    cmpBatchDispatch(i16, raw16[m.start .. m.start + nr], cond.op, rhs, rhs2, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                                    simd_batch.andMasks(rs_mask[0..nr], rs_tmp_a[0..nr], rs_mask[0..nr]);
                                    handled = true;
                                }
                            } else if (self.raw_ic_i32[ci]) |raw32| {
                                if (cond.val >= std.math.minInt(i32) and cond.val <= std.math.maxInt(i32)) {
                                    const rhs: i32 = @intCast(cond.val);
                                    const rhs2: i32 = if (cond.op == .in2 and
                                        cond.val2 >= std.math.minInt(i32) and cond.val2 <= std.math.maxInt(i32))
                                        @intCast(cond.val2)
                                    else
                                        0;
                                    cmpBatchDispatch(i32, raw32[m.start .. m.start + nr], cond.op, rhs, rhs2, rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                                    simd_batch.andMasks(rs_mask[0..nr], rs_tmp_a[0..nr], rs_mask[0..nr]);
                                    handled = true;
                                }
                            }
                        }
                        if (!handled) _ = applyIntCondSIMD(&c, cond, nr, rs_mask[0..nr], rs_tmp_a[0..nr], rs_tmp_b[0..nr]);
                    }
                    break :blk true;
                } else false;

                // ── Chunked scatter loop with optional OR-reduce skip ─────────────
                const CHUNK = 32;
                var r: usize = 0;
                while (r + CHUNK <= nr) : (r += CHUNK) {
                    if (have_int_mask) {
                        const v: @Vector(CHUNK, i16) = rs_mask[r..][0..CHUNK].*;
                        if (@reduce(.Or, v) == 0) continue;
                    }
                    for (r..r + CHUNK) |ri| {
                        if (have_int_mask and rs_mask[ri] == 0) continue;
                        if (self.str_filter) |sf| {
                            if (sf_col) |sfc| {
                                if (!sf.passes(sfc, ri)) continue;
                            }
                        }
                        if (!use_raw_key) {
                            if (key_col.isRowNull(ri)) continue;
                        }
                        const s: []const u8 = if (use_raw_key) blk_s: {
                            const a = m.start + ri;
                            break :blk_s self.raw_key_bytes.?[self.raw_key_offsets.?[a]..self.raw_key_offsets.?[a + 1]];
                        } else strs[ri];
                        var sidecar_val: i64 = 0;
                        const h: u64 = blk: {
                            const base_h = ht.StrAggHashTable.hashStr(s);
                            if (self.sidecar_col_idx) |sci| {
                                if (sci < c.columns.len) {
                                    const sc = c.columns[sci];
                                    if (!sc.isRowNull(ri)) {
                                        sidecar_val = switch (sc.data) {
                                            .int64 => |a| a[ri],
                                            .uint64 => |a| @bitCast(a[ri]),
                                            .bool_u8 => |a| @as(i64, a[ri]),
                                            .date_u16 => |a| @as(i64, a[ri]),
                                            else => 0,
                                        };
                                    }
                                }
                                const sv: u64 = @bitCast(sidecar_val);
                                break :blk base_h ^ (sv *% 0x9e3779b97f4a7c15);
                            }
                            break :blk base_h;
                        };
                        const part_id = @as(usize, @truncate(h)) & (N_PARTS - 1);
                        rec[0] = h;
                        rec[1] = @intFromPtr(s.ptr);
                        rec[2] = s.len;
                        if (self.sidecar_col_idx != null) rec[3] = @bitCast(sidecar_val);
                        if (!self.count_only) {
                            const agg_base: usize = if (self.sidecar_col_idx != null) 4 else 3;
                            for (self.agg_infos, 0..) |info, ai| {
                                const off = agg_base + ai;
                                switch (info.kind) {
                                    .count => {
                                        rec[off] = 1;
                                    },
                                    .i64_sum => {
                                        rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                            const ac = c.columns[info.col_idx];
                                            break :blk2 if (ac.isRowNull(ri)) 0 else switch (ac.data) {
                                                .int64 => |v| @bitCast(v[ri]),
                                                .uint64 => |v| v[ri],
                                                .bool_u8 => |v| @as(u64, v[ri]),
                                                else => 0,
                                            };
                                        };
                                    },
                                    .u64_sum => {
                                        rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                            const ac = c.columns[info.col_idx];
                                            break :blk2 if (ac.isRowNull(ri)) 0 else switch (ac.data) {
                                                .uint64 => |v| v[ri],
                                                .int64 => |v| @bitCast(v[ri]),
                                                .bool_u8 => |v| @as(u64, v[ri]),
                                                else => 0,
                                            };
                                        };
                                    },
                                    .f64_sum => {
                                        rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                            const ac = c.columns[info.col_idx];
                                            const fv: f64 = if (ac.isRowNull(ri)) 0.0 else switch (ac.data) {
                                                .int64 => |v| @floatFromInt(v[ri]),
                                                .uint64 => |v| @floatFromInt(v[ri]),
                                                .bool_u8 => |v| @floatFromInt(v[ri]),
                                                .float64 => |v| v[ri],
                                                else => 0.0,
                                            };
                                            break :blk2 @bitCast(fv);
                                        };
                                    },
                                    .count_distinct_u64 => {
                                        rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                            const ac = c.columns[info.col_idx];
                                            break :blk2 if (ac.isRowNull(ri)) 0 else switch (ac.data) {
                                                .int64 => |v| @bitCast(v[ri]),
                                                .uint64 => |v| v[ri],
                                                .bool_u8 => |v| @as(u64, v[ri]),
                                                .date_u16 => |v| @as(u64, v[ri]),
                                                else => 0,
                                            };
                                        };
                                    },
                                    else => {
                                        rec[off] = 0;
                                    },
                                }
                            }
                        }
                        try self.bufs[part_id].appendSlice(ba, rec[0..self.row_stride]);
                    }
                }
                // ── Tail: remaining rows after last full CHUNK ────────────────────
                while (r < nr) : (r += 1) {
                    if (have_int_mask and rs_mask[r] == 0) continue;
                    if (self.str_filter) |sf| {
                        if (sf_col) |sfc| {
                            if (!sf.passes(sfc, r)) continue;
                        }
                    }
                    if (!use_raw_key) {
                        if (key_col.isRowNull(r)) continue;
                    }
                    const s: []const u8 = if (use_raw_key) blk_s: {
                        const a = m.start + r;
                        break :blk_s self.raw_key_bytes.?[self.raw_key_offsets.?[a]..self.raw_key_offsets.?[a + 1]];
                    } else strs[r];
                    var sidecar_val: i64 = 0;
                    const h: u64 = blk: {
                        const base_h = ht.StrAggHashTable.hashStr(s);
                        if (self.sidecar_col_idx) |sci| {
                            if (sci < c.columns.len) {
                                const sc = c.columns[sci];
                                if (!sc.isRowNull(r)) {
                                    sidecar_val = switch (sc.data) {
                                        .int64 => |a| a[r],
                                        .uint64 => |a| @bitCast(a[r]),
                                        .bool_u8 => |a| @as(i64, a[r]),
                                        .date_u16 => |a| @as(i64, a[r]),
                                        else => 0,
                                    };
                                }
                            }
                            const sv: u64 = @bitCast(sidecar_val);
                            break :blk base_h ^ (sv *% 0x9e3779b97f4a7c15);
                        }
                        break :blk base_h;
                    };
                    const part_id = @as(usize, @truncate(h)) & (N_PARTS - 1);
                    rec[0] = h;
                    rec[1] = @intFromPtr(s.ptr);
                    rec[2] = s.len;
                    if (self.sidecar_col_idx != null) rec[3] = @bitCast(sidecar_val);
                    if (!self.count_only) {
                        const agg_base: usize = if (self.sidecar_col_idx != null) 4 else 3;
                        for (self.agg_infos, 0..) |info, ai| {
                            const off = agg_base + ai;
                            switch (info.kind) {
                                .count => {
                                    rec[off] = 1;
                                },
                                .i64_sum => {
                                    rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                        const ac = c.columns[info.col_idx];
                                        break :blk2 if (ac.isRowNull(r)) 0 else switch (ac.data) {
                                            .int64 => |v| @bitCast(v[r]),
                                            .uint64 => |v| v[r],
                                            .bool_u8 => |v| @as(u64, v[r]),
                                            else => 0,
                                        };
                                    };
                                },
                                .u64_sum => {
                                    rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                        const ac = c.columns[info.col_idx];
                                        break :blk2 if (ac.isRowNull(r)) 0 else switch (ac.data) {
                                            .uint64 => |v| v[r],
                                            .int64 => |v| @bitCast(v[r]),
                                            .bool_u8 => |v| @as(u64, v[r]),
                                            else => 0,
                                        };
                                    };
                                },
                                .f64_sum => {
                                    rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                        const ac = c.columns[info.col_idx];
                                        const fv: f64 = if (ac.isRowNull(r)) 0.0 else switch (ac.data) {
                                            .int64 => |v| @floatFromInt(v[r]),
                                            .uint64 => |v| @floatFromInt(v[r]),
                                            .bool_u8 => |v| @floatFromInt(v[r]),
                                            .float64 => |v| v[r],
                                            else => 0.0,
                                        };
                                        break :blk2 @bitCast(fv);
                                    };
                                },
                                .count_distinct_u64 => {
                                    rec[off] = if (info.col_idx >= c.columns.len) 0 else blk2: {
                                        const ac = c.columns[info.col_idx];
                                        break :blk2 if (ac.isRowNull(r)) 0 else switch (ac.data) {
                                            .int64 => |v| @bitCast(v[r]),
                                            .uint64 => |v| v[r],
                                            .bool_u8 => |v| @as(u64, v[r]),
                                            .date_u16 => |v| @as(u64, v[r]),
                                            else => 0,
                                        };
                                    };
                                },
                                else => {
                                    rec[off] = 0;
                                },
                            }
                        }
                    }
                    try self.bufs[part_id].appendSlice(ba, rec[0..self.row_stride]);
                }
            }
        }
    };

    // ── Pre-fetch raw column slices + column pruning for ScatterCtx ──────────
    // For int-filter columns that are stored as int16/int32 on disk, grab the
    // raw mmap'd slices so doWork can use 32-lane / 16-lane SIMD instead of the
    // 8-lane path that operates on i64-widened DataChunk columns.
    // For the key (string) column, grab offsets+bytes so doWork can skip the
    // per-morsel fat-pointer array allocation in fetchRange.
    const src_schema = ctx.source.schema();
    var raw_ic_i16_pre: [16]?[]const i16 = @as([16]?[]const i16, @splat(null));
    var raw_ic_i32_pre: [16]?[]const i32 = @as([16]?[]const i32, @splat(null));
    var n_raw_ic_covered: usize = 0; // count of int conditions covered by raw slices
    const n_ic_total: usize = if (int_filter) |ics| ics.len else 0;
    if (int_filter) |ics| {
        for (ics, 0..) |cond, ci| {
            if (ci >= 16) break;
            if (cond.col_idx >= src_schema.len) continue;
            const col_name = src_schema[cond.col_idx].name;
            if (ctx.source.getRawInt16Col(col_name)) |raw16| {
                raw_ic_i16_pre[ci] = raw16;
                n_raw_ic_covered += 1;
            } else if (ctx.source.getRawInt32Col(col_name)) |raw32| {
                raw_ic_i32_pre[ci] = raw32;
                n_raw_ic_covered += 1;
            }
        }
    }
    var raw_key_offsets_pre: ?[]const u64 = null;
    var raw_key_bytes_pre: ?[]const u8 = null;
    const key_col_name_pre: []const u8 = if (key_col_idx < src_schema.len) src_schema[key_col_idx].name else "";
    if (key_col_idx < src_schema.len) {
        raw_key_offsets_pre = ctx.source.getRawStrOffsets(key_col_name_pre);
        raw_key_bytes_pre = ctx.source.getRawStrBytes(key_col_name_pre);
        // Switch key column to bool_u8 decoding so fetchRange emits 1B/row
        // instead of building a 122 880-entry fat-pointer slice per morsel.
        // defer resets this after parallelFor completes.
        if (raw_key_offsets_pre != null and raw_key_bytes_pre != null) {
            ctx.source.setStringNonEmptyBool(key_col_name_pre);
        }
    }
    defer ctx.source.setStringNonEmptyBool(null);

    // Pre-fetch raw integer slice for sidecar column (e.g. MobilePhone for Q12).
    // When available, the sidecar value is read from mmap'd data in the skip_fetch path,
    // eliminating fetchRange even for composite (string + int64) GROUP BY queries.
    var raw_sidecar_pre: ?RawColSlice = null;
    if (sidecar_col_idx) |sci| {
        if (sci < src_schema.len) {
            raw_sidecar_pre = RawColSlice.resolve(ctx.source, src_schema, sci);
        }
    }
    const raw_distinct_pre: ?RawColSlice = if (is_count_distinct and agg_infos.len > 0 and agg_infos[0].col_idx < src_schema.len)
        RawColSlice.resolve(ctx.source, src_schema, agg_infos[0].col_idx)
    else
        null;

    // When ALL int conditions are covered by raw slices, the key is raw, it's count_only,
    // sidecar (if any) has a raw integer slice, and str_filter (if set) is on the key column —
    // we can skip fetchRange entirely and process morsels from mmap'd raw slices.
    const str_filter_is_key_col: bool = if (str_filter) |sf| sf.col_idx == key_col_idx else true;
    const can_skip_fetch: bool =
        n_raw_ic_covered == n_ic_total and // all int conditions have raw slices
        raw_key_offsets_pre != null and // string key available raw
        raw_key_bytes_pre != null and
        (count_only or (is_count_distinct and raw_distinct_pre != null)) and // no DataChunk agg columns to read
        (sidecar_col_idx == null or raw_sidecar_pre != null) and // sidecar ok if raw slice available
        str_filter_is_key_col; // str filter checked via key offsets

    var scatter_scan_request: ScanRequest = .{};
    if (!can_skip_fetch) {
        scatter_scan_request.addIndex(key_col_idx);
        if (str_filter) |sf| scatter_scan_request.addIndex(sf.col_idx);
        if (int_filter) |ics| for (ics) |cond| scatter_scan_request.addIndex(cond.col_idx);
        for (agg_infos) |info| {
            if (!count_only) scatter_scan_request.addIndex(info.col_idx);
        }
        if (sidecar_col_idx) |sci| scatter_scan_request.addIndex(sci);
    }
    const scatter_scan_scope = scatter_scan_request.apply(ctx.source, .half);
    defer scatter_scan_scope.deinit();

    var morsel_src1 = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const scatter_ctxs = try alloc.alloc(ScatterCtx, n_threads);
    for (scatter_ctxs) |*sc| {
        sc.* = .{
            .bufs = @as([N_PARTS]std.ArrayListUnmanaged(u64), @splat(.empty)),
            .buf_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator),
            .source = ctx.source,
            .morsel_src = &morsel_src1,
            .parent_alloc = alloc,
            .key_col_idx = key_col_idx,
            .str_filter = str_filter,
            .int_filter = int_filter,
            .sidecar_col_idx = sidecar_col_idx,
            .count_only = count_only,
            .n_aggs = n_aggs,
            .row_stride = row_stride,
            .agg_infos = agg_infos,
            .raw_ic_i16 = raw_ic_i16_pre,
            .raw_ic_i32 = raw_ic_i32_pre,
            .raw_key_offsets = raw_key_offsets_pre,
            .raw_key_bytes = raw_key_bytes_pre,
            .raw_sidecar = raw_sidecar_pre,
            .raw_distinct = raw_distinct_pre,
            .skip_fetch = can_skip_fetch,
        };
    }
    try parallel.parallelFor(alloc, ScatterCtx, ScatterCtx.work, scatter_ctxs, &morsel_src1);
    for (scatter_ctxs) |*sc| {
        if (sc.err) |e| return e;
    }

    // ── Phase 2: parallel aggregate, one HT per partition ────────────────────
    // Each partition gets its own c_allocator-backed arena so threads never
    // share an allocator (partition ownership is disjoint).
    const part_arenas = try alloc.alloc(std.heap.ArenaAllocator, N_PARTS);
    const part_hts = try alloc.alloc(ht.StrAggHashTable, N_PARTS);
    for (0..N_PARTS) |p| {
        part_arenas[p] = std.heap.ArenaAllocator.init(std.heap.c_allocator);
        part_hts[p] = try ht.StrAggHashTable.initWithCapacity(part_arenas[p].allocator(), n_aggs, 0, 0);
    }

    const AggCtx = struct {
        scatter_ctxs: []ScatterCtx,
        part_hts: []ht.StrAggHashTable,
        part_arenas: []std.heap.ArenaAllocator,
        compact_kinds: []const ht.CompactAggKind,
        compact_init_vals: []const u64,
        row_stride: usize,
        n_aggs: usize,
        count_only: bool,
        has_sidecar: bool,
        is_count_distinct: bool,
        morsel_src: *parallel.MorselSource,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.doWork() catch |e| {
                self.err = e;
            };
        }

        fn doWork(self: *@This()) !void {
            while (self.morsel_src.next()) |m| {
                for (m.start..m.end) |p| {
                    // ── COUNT DISTINCT: sort-sweep path ──────────────────────────────
                    // Scatter records: [h:u64, ptr:u64, len:u64, (sidecar?:u64), raw_dval:u64]
                    // Sort by (h primary, raw_dval secondary), sweep to count distinct
                    // dvals per h-group, insert counts into part_hts[p].
                    if (self.is_count_distinct) {
                        direct_distinct: {
                            const rs: usize = if (self.has_sidecar) 5 else 4;
                            var total_p: usize = 0;
                            for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / rs;
                            if (total_p == 0) break :direct_distinct;
                            if (total_p > 500_000) break :direct_distinct;

                            try self.part_hts[p].growTo(@max(64, total_p * 100 / 65 + 16));
                            var dset = try hashmap.DistinctEpochSet.init(std.heap.c_allocator, @max(64, total_p));
                            defer dset.deinit();
                            const ivs = self.compact_init_vals;
                            for (self.scatter_ctxs) |*sc| {
                                const buf = sc.bufs[p].items;
                                var i: usize = 0;
                                while (i < buf.len) : (i += rs) {
                                    const h = buf[i];
                                    const dval = buf[i + rs - 1];
                                    if (dset.needsGrow()) try dset.growDouble();
                                    const pair_key = h ^ (dval *% 0x9e3779b97f4a7c15);
                                    if (!dset.insertNew(pair_key)) continue;
                                    const ptr = buf[i + 1];
                                    const slen = buf[i + 2];
                                    const sidecar: i64 = if (self.has_sidecar) @bitCast(buf[i + 3]) else 0;
                                    const s: []const u8 = @as([*]const u8, @ptrFromInt(ptr))[0..slen];
                                    const res = try self.part_hts[p].getOrInsertHashOnly(s, h, ivs, sidecar);
                                    res.vals[0] += 1;
                                }
                            }
                            continue;
                        }
                        if (!self.has_sidecar) {
                            const CD_RS: usize = 4;
                            var total_p: usize = 0;
                            for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / CD_RS;
                            if (total_p == 0) continue;

                            var par = std.heap.ArenaAllocator.init(std.heap.c_allocator);
                            defer par.deinit();
                            const paa = par.allocator();

                            const SortRecNoSidecar = struct { h: u64, ptr: u64, len: u64, dval: u64 };
                            const recs = try paa.alloc(SortRecNoSidecar, total_p);
                            var ri: usize = 0;
                            for (self.scatter_ctxs) |*sc| {
                                const buf = sc.bufs[p].items;
                                var i: usize = 0;
                                while (i < buf.len) : (i += CD_RS) {
                                    recs[ri] = .{ .h = buf[i], .ptr = buf[i + 1], .len = buf[i + 2], .dval = buf[i + 3] };
                                    ri += 1;
                                }
                            }
                            const SortCtxNoSidecar = struct {
                                fn lt(_: @This(), a: SortRecNoSidecar, b: SortRecNoSidecar) bool {
                                    if (a.h != b.h) return a.h < b.h;
                                    return a.dval < b.dval;
                                }
                            };
                            std.sort.pdq(SortRecNoSidecar, recs[0..ri], SortCtxNoSidecar{}, SortCtxNoSidecar.lt);
                            try self.part_hts[p].growTo(@max(64, ri * 100 / 65 + 16));
                            const ivs = self.compact_init_vals;
                            var si: usize = 0;
                            while (si < ri) {
                                const h = recs[si].h;
                                const first_ptr = recs[si].ptr;
                                const first_len = recs[si].len;
                                var prev_dval: u64 = recs[si].dval;
                                var count: u64 = 1;
                                si += 1;
                                while (si < ri and recs[si].h == h) : (si += 1) {
                                    if (recs[si].dval != prev_dval) {
                                        count += 1;
                                        prev_dval = recs[si].dval;
                                    }
                                }
                                const s: []const u8 = @as([*]const u8, @ptrFromInt(first_ptr))[0..first_len];
                                const res = try self.part_hts[p].getOrInsertHashOnly(s, h, ivs, 0);
                                res.vals[0] += count;
                            }
                            continue;
                        }

                        const CD_RS: usize = 5;
                        const dval_off: usize = 4;
                        var total_p: usize = 0;
                        for (self.scatter_ctxs) |*sc| total_p += sc.bufs[p].items.len / CD_RS;
                        if (total_p == 0) continue;

                        var par = std.heap.ArenaAllocator.init(std.heap.c_allocator);
                        defer par.deinit();
                        const paa = par.allocator();

                        const SortRec = struct { h: u64, ptr: u64, len: u64, sidecar: i64, dval: u64 };
                        const recs = try paa.alloc(SortRec, total_p);

                        // Collect records from all scatter bufs for this partition.
                        var ri: usize = 0;
                        for (self.scatter_ctxs) |*sc| {
                            const buf = sc.bufs[p].items;
                            var i: usize = 0;
                            while (i < buf.len) : (i += CD_RS) {
                                recs[ri] = .{
                                    .h = buf[i],
                                    .ptr = buf[i + 1],
                                    .len = buf[i + 2],
                                    .sidecar = if (self.has_sidecar) @bitCast(buf[i + 3]) else 0,
                                    .dval = buf[i + dval_off],
                                };
                                ri += 1;
                            }
                        }

                        // Sort by (h primary, dval secondary).
                        const SortCtxCD = struct {
                            fn lt(_: @This(), a: SortRec, b: SortRec) bool {
                                if (a.h != b.h) return a.h < b.h;
                                return a.dval < b.dval;
                            }
                        };
                        std.sort.pdq(SortRec, recs[0..ri], SortCtxCD{}, SortCtxCD.lt);

                        // Pre-size part_hts[p] conservatively (ri is upper bound on unique groups).
                        const ht_cap_cd = @max(64, ri * 100 / 65 + 16);
                        try self.part_hts[p].growTo(ht_cap_cd);

                        const ivs = self.compact_init_vals;

                        // Sweep: count distinct dval per h-group, accumulate into part_hts[p].
                        var si: usize = 0;
                        while (si < ri) {
                            const h = recs[si].h;
                            const first_ptr = recs[si].ptr;
                            const first_len = recs[si].len;
                            const first_sidecar = recs[si].sidecar;
                            var prev_dval: u64 = recs[si].dval;
                            var count: u64 = 1;
                            si += 1;
                            while (si < ri and recs[si].h == h) : (si += 1) {
                                if (recs[si].dval != prev_dval) {
                                    count += 1;
                                    prev_dval = recs[si].dval;
                                }
                            }
                            const s: []const u8 = @as([*]const u8, @ptrFromInt(first_ptr))[0..first_len];
                            const res = try self.part_hts[p].getOrInsertHashOnly(s, h, ivs, first_sidecar);
                            res.vals[0] += count;
                        }
                        continue;
                    }

                    var total_p: usize = 0;
                    for (self.scatter_ctxs) |*sc|
                        total_p += sc.bufs[p].items.len / self.row_stride;
                    if (total_p == 0) continue;

                    const ht_cap = @max(64, total_p * 100 / 65 + 16);
                    try self.part_hts[p].growTo(ht_cap);

                    const rs = self.row_stride;
                    const ivs = self.compact_init_vals;
                    const ks = self.compact_kinds;
                    // Scatter record layout:  [h, str_ptr, str_len, (sidecar?), agg0, ...]
                    const agg_off: usize = if (self.has_sidecar) 4 else 3;
                    for (self.scatter_ctxs) |*sc| {
                        const buf = sc.bufs[p].items;
                        var i: usize = 0;
                        while (i < buf.len) : (i += rs) {
                            const h = buf[i];
                            const ptr = buf[i + 1];
                            const slen = buf[i + 2];
                            const s: []const u8 = @as([*]const u8, @ptrFromInt(ptr))[0..slen];
                            const sidecar: i64 = if (self.has_sidecar) @bitCast(buf[i + 3]) else 0;
                            // Hash-only comparison: skip std.mem.eql pointer-chase into the
                            // mmap'd file (e.g. URL.str.bin, 900 MB).  wyhash 64-bit collision
                            // probability ~n²/2⁶⁴ ≈ 3.6×10⁻⁷ for 2.6 M URLs — negligible.
                            const res = try self.part_hts[p].getOrInsertHashOnly(s, h, ivs, sidecar);
                            const sv = res.vals;
                            if (self.count_only) {
                                sv[0] += 1;
                            } else {
                                const partial = buf[i + agg_off .. i + rs];
                                for (ks, 0..) |kind, ci| {
                                    const src = partial[ci];
                                    switch (kind) {
                                        .count, .u64_sum => sv[ci] += src,
                                        .i64_sum => {
                                            const a: i64 = @bitCast(sv[ci]);
                                            const b: i64 = @bitCast(src);
                                            sv[ci] = @bitCast(a + b);
                                        },
                                        .f64_sum => {
                                            const a: f64 = @bitCast(sv[ci]);
                                            const b: f64 = @bitCast(src);
                                            sv[ci] = @bitCast(a + b);
                                        },
                                        .u64_min => {
                                            if (src < sv[ci]) sv[ci] = src;
                                        },
                                        .u64_max => {
                                            if (src > sv[ci]) sv[ci] = src;
                                        },
                                        .i64_min => {
                                            const a: i64 = @bitCast(sv[ci]);
                                            const b: i64 = @bitCast(src);
                                            if (b < a) sv[ci] = src;
                                        },
                                        .i64_max => {
                                            const a: i64 = @bitCast(sv[ci]);
                                            const b: i64 = @bitCast(src);
                                            if (b > a) sv[ci] = src;
                                        },
                                        .f64_min => {
                                            const a: f64 = @bitCast(sv[ci]);
                                            const b: f64 = @bitCast(src);
                                            if (b < a) sv[ci] = src;
                                        },
                                        .f64_max => {
                                            const a: f64 = @bitCast(sv[ci]);
                                            const b: f64 = @bitCast(src);
                                            if (b > a) sv[ci] = src;
                                        },
                                        else => sv[ci] += src,
                                    }
                                }
                            }
                        }
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
            .part_arenas = part_arenas,
            .compact_kinds = compact_kinds,
            .compact_init_vals = compact_init_vals,
            .row_stride = row_stride,
            .n_aggs = n_aggs,
            .count_only = count_only,
            .has_sidecar = has_sidecar,
            .is_count_distinct = is_count_distinct,
            .morsel_src = &morsel_src2,
        };
    }
    try parallel.parallelFor(alloc, AggCtx, AggCtx.work, agg_ctxs, &morsel_src2);
    for (agg_ctxs) |*ac| {
        if (ac.err) |e| return e;
    }

    // Release scatter buffers.
    for (scatter_ctxs) |*sc| sc.buf_arena.deinit();

    // ── Emit: iterate all partition HTs, build output RowBuffer ────────────────
    const out_metas = try buildAggResultMetas(keys, aggs, alloc);

    const use_heap = top_k > 0 and sort_keys.len > 0;

    const EmitCtx = struct {
        keys: []const plan.ProjectItem,
        aggs: []const plan.ProjectItem,
        kinds: []const ht.CompactAggKind,
        str_key_pos: usize,
        sidecar_key_pos: ?usize,
        current_ht: ?*const ht.StrAggHashTable,
        rl: RowBuffer,
        alloc: std.mem.Allocator,
        heap: std.ArrayListUnmanaged([]?Value),
        heap_k: usize,
        sort_keys: []const plan.SortKey,
        err: ?anyerror = null,

        fn rowWorse(sk: []const plan.SortKey, a: []?Value, b: []?Value) bool {
            // True when `a` should be evicted from the heap before `b`.
            // The heap is a max-heap of "worst" elements (root = eviction candidate).
            for (sk) |key| {
                const ai = if (key.col_idx < a.len) a[key.col_idx] else null;
                const bi = if (key.col_idx < b.len) b[key.col_idx] else null;
                const ord: std.math.Order = if (ai != null and bi != null) Value.order(ai.?, bi.?) else if (ai == null and bi == null) .eq else if (ai == null) .lt else .gt;
                if (ord == .eq) continue;
                // For ORDER BY x DESC LIMIT k: keep top-k largest → evict smallest.
                return if (key.desc) (ord == .lt) else (ord == .gt);
            }
            return false;
        }

        fn siftUp(self: *@This(), idx: usize) void {
            var i = idx;
            const h = self.heap.items;
            while (i > 0) {
                const p = (i - 1) / 2;
                if (rowWorse(self.sort_keys, h[i], h[p])) {
                    const tmp = h[i];
                    h[i] = h[p];
                    h[p] = tmp;
                    i = p;
                } else break;
            }
        }

        fn siftDown(self: *@This()) void {
            var i: usize = 0;
            const h = self.heap.items;
            while (true) {
                var worst = i;
                const l = i * 2 + 1;
                const r = i * 2 + 2;
                // Find the most-evictable (smallest count for DESC) among current and children.
                if (l < h.len and rowWorse(self.sort_keys, h[l], h[worst])) worst = l;
                if (r < h.len and rowWorse(self.sort_keys, h[r], h[worst])) worst = r;
                if (worst == i) break;
                const tmp = h[i];
                h[i] = h[worst];
                h[worst] = tmp;
                i = worst;
            }
        }

        fn emitRow(self: *@This(), key_str: []const u8, acc_vals: []const u64, _slot: usize) void {
            self.doEmit(key_str, acc_vals, _slot) catch |e| {
                self.err = e;
            };
        }

        fn doEmit(self: *@This(), key_str: []const u8, acc_vals: []const u64, slot: usize) !void {
            const nk = self.keys.len;

            // ── Fast TopK pre-check ────────────────────────────────────────────
            // When the heap is full, check whether this entry can beat the current
            // minimum BEFORE allocating the row or duplicating the key string.
            // For Q34/Q35 (GROUP BY URL ORDER BY count DESC LIMIT 10) this skips
            // ~2.56M alloc+dupe calls for entries that will never reach the top-10,
            // cutting emit time from ~200 ms to ~1 ms.
            if (self.heap_k > 0 and self.heap.items.len >= self.heap_k and self.sort_keys.len == 1) {
                const sk = self.sort_keys[0];
                if (sk.col_idx >= nk) {
                    const agg_idx = sk.col_idx - nk;
                    if (agg_idx < self.kinds.len and agg_idx < acc_vals.len) {
                        if (self.heap.items[0][sk.col_idx]) |hv| {
                            const new_v = acc_vals[agg_idx];
                            const worse: bool = switch (self.kinds[agg_idx]) {
                                .count, .u64_sum, .u64_max => if (sk.desc) new_v <= @as(u64, @bitCast(hv.int64)) else new_v >= @as(u64, @bitCast(hv.int64)),
                                .i64_sum, .i64_max => if (sk.desc) @as(i64, @bitCast(new_v)) <= hv.int64 else @as(i64, @bitCast(new_v)) >= hv.int64,
                                .u64_min => if (sk.desc) new_v >= @as(u64, @bitCast(hv.int64)) else new_v <= @as(u64, @bitCast(hv.int64)),
                                .i64_min => if (sk.desc) @as(i64, @bitCast(new_v)) >= hv.int64 else @as(i64, @bitCast(new_v)) <= hv.int64,
                                else => false,
                            };
                            if (worse) return;
                        }
                    }
                }
            }
            const row = try self.alloc.alloc(?Value, nk + self.aggs.len);

            for (self.keys, 0..) |k, ki| {
                row[ki] = if (ki == self.str_key_pos) blk: {
                    const s = try self.alloc.dupe(u8, key_str);
                    break :blk Value{ .string = s };
                } else if (self.sidecar_key_pos != null and ki == self.sidecar_key_pos.?) blk: {
                    // Read the int sidecar value stored in the HT key slot.
                    const int_val: i64 = if (self.current_ht) |cht|
                        cht.key_slots[slot].int_sidecar
                    else
                        0;
                    break :blk Value{ .int64 = int_val };
                } else switch (k.expr) {
                    .lit_i64 => |v| Value{ .int64 = v },
                    else => Value{ .int64 = 0 },
                };
            }

            for (self.kinds, 0..) |kind, ai| {
                const v = acc_vals[ai];
                row[nk + ai] = switch (kind) {
                    .count => Value{ .int64 = @intCast(v) },
                    .i64_sum, .i64_min, .i64_max => Value{ .int64 = @bitCast(v) },
                    .u64_sum, .u64_min, .u64_max => Value{ .uint64 = v },
                    .f64_sum, .f64_min, .f64_max => Value{ .float64 = @bitCast(v) },
                    else => Value{ .int64 = @bitCast(v) },
                };
            }

            if (self.heap_k > 0) {
                if (self.heap.items.len < self.heap_k) {
                    try self.heap.append(self.alloc, row);
                    self.siftUp(self.heap.items.len - 1);
                } else if (self.heap.items.len > 0 and
                    rowWorse(self.sort_keys, self.heap.items[0], row))
                {
                    self.heap.items[0] = row;
                    self.siftDown();
                }
            } else {
                try self.rl.append(self.alloc, row);
            }
        }
    };

    var ec = EmitCtx{
        .keys = keys,
        .aggs = aggs,
        .kinds = compact_kinds,
        .str_key_pos = str_key_pos,
        .sidecar_key_pos = sidecar_key_pos,
        .current_ht = null,
        .rl = RowBuffer.init(out_metas),
        .alloc = alloc,
        .heap = .empty,
        .heap_k = if (use_heap) top_k else 0,
        .sort_keys = sort_keys,
    };

    for (part_hts) |*ph| {
        ec.current_ht = ph;
        ph.iterateWithSlot(&ec, EmitCtx.emitRow);
        if (ec.err) |e| return e;
    }

    // Release partition HT memory.
    for (0..N_PARTS) |p| part_arenas[p].deinit();

    if (use_heap) {
        const items = ec.heap.items;
        std.sort.pdq([]?Value, items, TopKSortCtx{ .keys = sort_keys }, TopKSortCtx.lessThan);
        var tp_rl = RowBuffer.init(out_metas);
        for (items) |row| try tp_rl.append(alloc, row);
        return tp_rl;
    }

    return ec.rl;
}

pub fn executeOrderBy(inner: RowBuffer, keys: []const plan.SortKey, alloc: std.mem.Allocator) !RowBuffer {
    const rows_copy = try alloc.dupe([]?Value, inner.rows.items);
    std.sort.pdq([]?Value, rows_copy, TopKSortCtx{ .keys = keys }, TopKSortCtx.lessThan);
    var rl = RowBuffer.init(inner.metas);
    for (rows_copy) |row| try rl.append(alloc, row);
    return rl;
}

pub fn updateCompactVals(
    slot_vals: []u64,
    ck: []const ht.CompactAggKind,
    aggs: []const plan.ProjectItem,
    c: *const DataChunk,
    r: usize,
    str_ht: ?*ht.StrAggHashTable,
    slot: usize,
    sidecar_indices: []const usize,
) !void {
    for (aggs, 0..) |item, ci| {
        if (item.expr != .agg_call) continue;
        const ac = item.expr.agg_call;
        switch (ck[ci]) {
            .count => {
                if (ac.kind == .count_star) {
                    slot_vals[ci] += 1;
                } else if (ac.kind == .count) {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            if (!c.columns[arg.col_ref.index].isRowNull(r))
                                slot_vals[ci] += 1;
                        } else slot_vals[ci] += 1;
                    }
                }
            },
            .i64_sum => {
                if (ac.arg) |arg| {
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
                                s += v[r];
                                slot_vals[ci] = @bitCast(s);
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_sum => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                var s: f64 = @bitCast(slot_vals[ci]);
                                s += v[r];
                                slot_vals[ci] = @bitCast(s);
                            },
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
                            else => {},
                        };
                    }
                }
            },
            .i64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .int64 => |v| {
                                const cur: i64 = @bitCast(slot_vals[ci]);
                                if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .i64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .int64 => |v| {
                                const cur: i64 = @bitCast(slot_vals[ci]);
                                if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .u64_sum => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| slot_vals[ci] += v[r],
                            else => {},
                        };
                    }
                }
            },
            .u64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| {
                                if (v[r] < slot_vals[ci]) slot_vals[ci] = v[r];
                            },
                            else => {},
                        };
                    }
                }
            },
            .u64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .uint64 => |v| {
                                if (v[r] > slot_vals[ci]) slot_vals[ci] = v[r];
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_min => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                const cur: f64 = @bitCast(slot_vals[ci]);
                                if (v[r] < cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .f64_max => {
                if (ac.arg) |arg| {
                    if (arg == .col_ref) {
                        const col = c.columns[arg.col_ref.index];
                        if (!col.isRowNull(r)) switch (col.data) {
                            .float64 => |v| {
                                const cur: f64 = @bitCast(slot_vals[ci]);
                                if (v[r] > cur) slot_vals[ci] = @bitCast(v[r]);
                            },
                            else => {},
                        };
                    }
                }
            },
            .str_min => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            const col = c.columns[arg.col_ref.index];
                            if (!col.isRowNull(r)) switch (col.data) {
                                .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], true),
                                else => {},
                            };
                        }
                    }
                }
            },
            .str_max => {
                if (str_ht) |sht| {
                    if (ac.arg) |arg| {
                        if (arg == .col_ref) {
                            const col = c.columns[arg.col_ref.index];
                            if (!col.isRowNull(r)) switch (col.data) {
                                .string => |v| sht.updateStrSidecar(slot, sidecar_indices[ci], v[r], false),
                                else => {},
                            };
                        }
                    }
                }
            },
            .f64_str_len_sum => {
                if (ac.arg) |arg| {
                    // Resolve the string column index: either a direct col_ref (rare)
                    // or fn_call("length", col_ref) — the common AVG(length(col)) pattern.
                    const str_ci: usize = blk2: {
                        if (arg == .col_ref) break :blk2 arg.col_ref.index;
                        if (arg == .fn_call and
                            std.mem.eql(u8, arg.fn_call.name, "length") and
                            arg.fn_call.args.len == 1 and
                            arg.fn_call.args[0] == .col_ref)
                            break :blk2 arg.fn_call.args[0].col_ref.index;
                        break; // unsupported arg pattern — skip
                    };
                    const col = c.columns[str_ci];
                    if (!col.isRowNull(r)) switch (col.data) {
                        .string => |v| {
                            const cur: f64 = @bitCast(slot_vals[ci]);
                            slot_vals[ci] = @bitCast(cur + @as(f64, @floatFromInt(v[r].len)));
                        },
                        else => {},
                    };
                }
            },
            // count_distinct_u64: deduplication is handled by the global pair-set
            // in the caller (executeHashAggChunked compact path); no-op here.
            .count_distinct_u64 => {},
        }
    }
}

pub const ChunkRowSink = struct {
    sink: *chunk_sink_mod.ChunkSink,
    fn append(self: *ChunkRowSink, _: std.mem.Allocator, row: []const ?Value) !void {
        try self.sink.append(row);
    }
};

pub fn radixSortU128(items: []u128, scratch: []u128) void {
    if (items.len <= 1) return;
    var cnt: [256]usize = undefined;
    var src = items;
    var dst = scratch;
    comptime var pass: u7 = 0;
    inline while (pass < 16) : (pass += 1) {
        const shift: u7 = pass * 8;
        @memset(&cnt, 0);
        for (src) |v| cnt[@as(u8, @truncate(v >> shift))] += 1;
        // prefix sum (exclusive)
        var acc: usize = 0;
        for (&cnt) |*c| {
            const x = c.*;
            c.* = acc;
            acc += x;
        }
        // scatter
        for (src) |v| {
            const b = @as(u8, @truncate(v >> shift));
            dst[cnt[b]] = v;
            cnt[b] += 1;
        }
        // swap src/dst pointers
        const t = src;
        src = dst;
        dst = t;
    }
    // 16 passes (even): final result is in src, which points back to items.
}
