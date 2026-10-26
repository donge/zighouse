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
const common = @import("agg_hash_common.zig");
const tp = @import("agg_hash_two_phase.zig");
pub const executeTwoPhaseHashAgg = tp.executeTwoPhaseHashAgg;
const hashmap = @import("hashmap");
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
pub const ChunkResult = chunk_sink.ChunkSink.Result;
pub const executeTopK = topn.executeTopK;
pub const rowLessThan = topn.rowLessThan;
pub const TopKSortCtx = topn.TopKSortCtx;
pub const TopKCollector = topn.TopKCollector;
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
pub const finalizeAggRows = common.finalizeAggRows;
pub const emitCompactVals = common.emitCompactVals;
pub const emitCompactValsWithSidecar = common.emitCompactValsWithSidecar;
pub const updateCompactVals = common.updateCompactVals;
pub const executeOrderBy = common.executeOrderBy;
pub const ChunkRowSink = common.ChunkRowSink;
const simd = simd_ops;

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

    var n_threads = parallel.defaultThreads();
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

    // Memory-bounded parallelism: 100M-row high-cardinality plans allocate
    // several GB per worker.  Keep those queries on fewer workers instead of
    // letting the OOM killer terminate them.
    if (total_rows > 50_000_000) {
        var has_distinct = false;
        for (compact_kinds) |k| if (k == .count_distinct_u64) {
            has_distinct = true;
            break;
        };
        if (has_distinct) {
            n_threads = @min(n_threads, 8);
        } else if (filter_pred != null) {
            n_threads = if (keys.len >= 2 and aggs.len > 1) @min(n_threads, 4) else @min(n_threads, 8);
        }
    }
    if (std.c.getenv("ZIGHOUSE_AGG_THREADS")) |raw| {
        const v = std.fmt.parseInt(usize, std.mem.span(raw), 10) catch 0;
        if (v > 0) n_threads = @min(v, 16);
    }

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
