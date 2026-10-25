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
const hashmap = @import("hashmap");
const context = @import("context.zig");
const operators = @import("operators.zig");
const topn = @import("topn.zig");
const row_buffer = @import("row_buffer.zig");
const agg = @import("agg.zig");
const common = @import("agg_hash_common.zig");
pub const finalizeAggRows = common.finalizeAggRows;
pub const emitCompactValsWithSidecar = common.emitCompactValsWithSidecar;
pub const updateCompactVals = common.updateCompactVals;
const chunk_sink = @import("chunk_sink.zig");

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
pub const StrCmpCond = operators.StrCmpCond;
pub const CaseWhenStrKey = operators.CaseWhenStrKey;
pub const SimpleStrFilter = operators.SimpleStrFilter;
pub const RowBuffer = row_buffer.RowBuffer;
pub const executeTopK = topn.executeTopK;
pub const TopKCollector = topn.TopKCollector;
pub const TopKSortCtx = topn.TopKSortCtx;
pub const LikeGuard = operators.LikeGuard;
pub const rowLessThan = topn.rowLessThan;
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
const simd = simd_ops;

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
