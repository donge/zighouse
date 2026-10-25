/// Push-based Pipeline execution engine.
///
/// A Pipeline connects a Source, zero or more Transform operators, and a Sink.
/// Data flows in DataChunks from source → transforms → sink.
///
/// Pipeline breakers (HashAgg, OrderBy, HashJoin) are handled by splitting
/// execution into multiple pipeline segments. The breaker's build phase
/// consumes its input pipeline fully, then the probe/read phase feeds the
/// next pipeline segment.
///
/// Thread model: morsel-parallel for scalar_agg and hash_agg when the source
/// supports fetchRange. Falls back to single-threaded otherwise.
const std = @import("std");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const result = @import("../result.zig");
const plan = @import("plan.zig");
const kernels = @import("kernels.zig");
const ht = @import("hash_table.zig");
const hashmap = @import("hashmap");
const simd = @import("../simd_ops.zig");
const simd_batch = @import("../simd_batch.zig");
const parallel = @import("parallel");
const chunk_sink_mod = @import("chunk_sink.zig");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;
pub const ColumnType = types.ColumnType;
pub const DataChunk = chunk.DataChunk;
pub const ResultSet = result.ResultSet;
pub const ResultSink = result.ResultSink;

// ── QueryContext ──────────────────────────────────────────────────────────────

const profile = @import("profile.zig");
pub const ProfilePhase = profile.ProfilePhase;
pub const QueryProfile = profile.QueryProfile;
pub const profileNowNs = profile.profileNowNs;
pub const profileEnvEnabled = profile.profileEnvEnabled;
pub const emitUnsupportedProfile = profile.emitUnsupportedProfile;

const context = @import("context.zig");
pub const QueryContext = context.QueryContext;
pub const SourceIface = context.SourceIface;

// ── Operator state ────────────────────────────────────────────────────────────

/// Filter: evaluates predicate on each row, zeroes out non-matching rows.
/// Non-matching rows are compacted out — chunk.num_rows shrinks.
const operators = @import("operators.zig");
const LikeGuard = operators.LikeGuard;
const IntCmpCond = operators.IntCmpCond;
const StrCmpCond = operators.StrCmpCond;
const RawColSlice = operators.RawColSlice;
const CaseWhenStrKey = operators.CaseWhenStrKey;
const extractCaseWhenStrKey = operators.extractCaseWhenStrKey;
const cmpBatchDispatch = operators.cmpBatchDispatch;
const applyIntCondSIMD = operators.applyIntCondSIMD;
const extractAndIntConds = operators.extractAndIntConds;
const SimpleStrFilter = operators.SimpleStrFilter;
const tryExtractSimpleStrFilter = operators.tryExtractSimpleStrFilter;
const extractAndStrConds = operators.extractAndStrConds;
const extractMixedAndConds = operators.extractMixedAndConds;
const FilterState = operators.FilterState;
const collectLikeGuards = operators.collectLikeGuards;
const exprCanUseBatchSelection = operators.exprCanUseBatchSelection;
const collectColRefs = operators.collectColRefs;
const ScanPrunePolicy = operators.ScanPrunePolicy;
const ScanRequest = operators.ScanRequest;
const ScanScope = operators.ScanScope;
const copyRow = operators.copyRow;
const ProjectState = operators.ProjectState;
const allocProjectColumns = operators.allocProjectColumns;
const evalProjectRow = operators.evalProjectRow;
const allocColumnData = operators.allocColumnData;
const setColumnValue = operators.setColumnValue;
const setColumnZero = operators.setColumnZero;
const LimitState = operators.LimitState;
const PhysicalOperator = operators.PhysicalOperator;

const row_buffer = @import("row_buffer.zig");
pub const RowBuffer = row_buffer.RowBuffer;

fn cloneValueForRowBuffer(v: Value, alloc: std.mem.Allocator) !Value {
    return switch (v) {
        .string => |s| Value{ .string = try alloc.dupe(u8, s) },
        .array_string => |arr| blk: {
            const out = try alloc.alloc([]const u8, arr.len);
            for (arr, 0..) |s, i| out[i] = try alloc.dupe(u8, s);
            break :blk Value{ .array_string = out };
        },
        else => v,
    };
}

fn cloneMetas(metas: []const result.ColMeta, alloc: std.mem.Allocator) ![]result.ColMeta {
    const out = try alloc.alloc(result.ColMeta, metas.len);
    for (metas, 0..) |m, i| {
        out[i] = m;
        out[i].name = try alloc.dupe(u8, m.name);
        if (m.ch_type) |ct| out[i].ch_type = try alloc.dupe(u8, ct);
        if (m.hash_col_name) |hn| out[i].hash_col_name = try alloc.dupe(u8, hn);
    }
    return out;
}

fn cloneRowBuffer(src: RowBuffer, alloc: std.mem.Allocator) !RowBuffer {
    var rl = RowBuffer.init(try cloneMetas(src.metas, alloc));
    for (src.rows.items) |src_row| {
        const row = try alloc.alloc(?Value, src_row.len);
        for (src_row, 0..) |v_opt, i| {
            row[i] = if (v_opt) |v| try cloneValueForRowBuffer(v, alloc) else null;
        }
        try rl.append(alloc, row);
    }
    return rl;
}

const EqPred = struct { col_idx: usize, val: i64 };

/// Walk an AND-tree collecting all eq(col_ref, lit_i64) or eq(lit_i64, col_ref) leaves.
/// Returns the number of equalities written into `out` (up to `out.len`).
fn collectEqPredicates(expr: plan.Expr, schema_metas: []const result.ColMeta, out: []EqPred) usize {
    var n: usize = 0;
    switch (expr) {
        .@"and" => |op| {
            n += collectEqPredicates(op.left, schema_metas, out[n..]);
            n += collectEqPredicates(op.right, schema_metas, out[n..]);
        },
        .eq => |op| {
            if (n >= out.len) return n;
            const col_expr, const lit_expr = blk: {
                if (op.left == .col_ref and op.right == .lit_i64) break :blk .{ op.left, op.right };
                if (op.right == .col_ref and op.left == .lit_i64) break :blk .{ op.right, op.left };
                return n;
            };
            out[n] = .{ .col_idx = col_expr.col_ref.index, .val = lit_expr.lit_i64 };
            n += 1;
        },
        else => {},
    }
    return n;
}

/// If the plan has a filter containing equality predicates on a sort key column,
/// ask the source for an exact sorted-key range.
/// No-ops if the source doesn't support range pruning for the sorted key.
fn tryPushdownSortKeyRange(node: *const plan.PhysicalNode, ctx: *QueryContext) void {
    // Find the filter predicate (may be wrapped in project/limit/top_k).
    var predicate: ?plan.Expr = null;
    var cur = node;
    while (true) {
        switch (cur.*) {
            .filter => |f| {
                predicate = f.predicate;
                break;
            },
            .project => |p| {
                cur = p.input;
            },
            .limit => |l| {
                cur = l.input;
            },
            .top_k => |tk| {
                cur = tk.input;
            },
            .scalar_agg => |sa| {
                cur = sa.input;
            },
            .hash_agg => |ha| {
                cur = ha.input;
            },
            .order_by => |ob| {
                cur = ob.input;
            },
            else => return,
        }
    }
    const pred = predicate orelse return;

    // Get sort keys from the source.
    const sort_keys = ctx.source.getSortKeys();
    if (sort_keys.len == 0) return;

    // Get schema to map col_idx → name.
    const schema_metas = ctx.source.schema();

    // Collect equality predicates from the filter's AND tree.
    var eq_buf: [8]EqPred = undefined;
    const n_eq = collectEqPredicates(pred, schema_metas, &eq_buf);
    if (n_eq == 0) return;

    // Try each sort key (only the leading key is useful for a contiguous range).
    for (sort_keys[0..1]) |sk| {
        // Find an equality predicate matching this sort key.
        var match_val: ?i64 = null;
        for (eq_buf[0..n_eq]) |ep| {
            if (ep.col_idx < schema_metas.len and std.mem.eql(u8, schema_metas[ep.col_idx].name, sk)) {
                match_val = ep.val;
                break;
            }
        }
        const val = match_val orelse continue;

        if (ctx.source.findIntRange(sk, val)) |range| {
            if (ctx.profile.enabled) {
                ctx.profile.range_pushdowns += 1;
                ctx.profile.recordReason("sort_key_range_pushdown");
            }
            ctx.source.setRowRange(range.lo, range.hi);
            return;
        }
    }
}

/// Execute a PhysicalNode tree recursively, returning a ResultSet.
/// Handles all node types including pipeline breakers (HashAgg, ScalarAgg,
/// OrderBy, TopK, HashJoin).
pub fn executePlan(
    node: *const plan.PhysicalNode,
    ctx: *QueryContext,
) !ResultSet {
    errdefer {
        ctx.profile.execute_errors += 1;
        ctx.profile.recordReason("execute_error");
    }
    const result_alloc = ctx.resultAllocator();

    // ── Sort-key range pushdown ────────────────────────────────────────────
    // Restrict the scan range before executing so all paths (scannable and
    // breaker) benefit.  Resets to full range after execution.
    const total_rows = ctx.source.rowCount();
    tryPushdownSortKeyRange(node, ctx);
    defer ctx.source.setRowRange(0, total_rows);

    // Raw integer point lookup (e.g. Q20) bypasses the chunk/evalExpr path.
    if (node.* == .project) {
        if (try executeRawIntPointLookup(node.project.input, node.project.items, ctx)) |rl| {
            return rl.toResultSet(result_alloc);
        }
    }

    // ── Scannable path: stream chunks directly into ResultSink ─────────────
    if (isScannable(node)) {
        ctx.profile.scannable_path += 1;
        // For large tables: try parallel filter-project when the only operators
        // are project + pure-AND-int-filter. Reduces Q20-style point lookups
        // from ~12ms (sequential) to ~3ms (parallel scan, 4 threads).
        scan_par: {
            if (node.* != .project) break :scan_par;
            const p = node.project;
            // Peel inner project nodes until we reach a filter.
            var cur_inner = p.input;
            while (cur_inner.* == .project) cur_inner = cur_inner.project.input;
            if (cur_inner.* != .filter) break :scan_par;
            const filt_pred = cur_inner.filter.predicate;
            if (try executeFilterProjectParallel(p.items, filt_pred, ctx)) |par_rl| {
                return par_rl.toResultSet(result_alloc);
            }
        }
        var sink = ResultSink.init(result_alloc);
        try executeScannableToSink(node, ctx, &sink);
        var rs = try sink.finish();
        if (findProjectItems(node)) |items| {
            for (items, 0..) |item, i| {
                if (i >= rs.metas.len) break;
                rs.metas[i].ch_type = item.ch_type;
            }
        }
        return rs;
    }

    // ── Breaker path: existing RowBuffer → ResultSet (single copy) ───────────
    ctx.profile.breaker_path += 1;
    const res = try executeNode(node, ctx);
    return chunkResultToResultSet(res, ctx.allocator(), result_alloc);
}

fn findProjectItems(node: *const plan.PhysicalNode) ?[]const plan.ProjectItem {
    return switch (node.*) {
        .project => |p| p.items,
        .filter => |f| findProjectItems(f.input),
        .limit => |l| findProjectItems(l.input),
        .top_k => |tk| findProjectItems(tk.input),
        .order_by => |ob| findProjectItems(ob.input),
        else => null,
    };
}

/// Stream a scannable node (scan/filter/project/limit) directly to a ResultSink.
/// Avoids building a RowBuffer by operating on DataChunks throughout.
fn executeScannableToSink(
    node: *const plan.PhysicalNode,
    ctx: *QueryContext,
    sink: *ResultSink,
) !void {
    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: ?LimitState = null;

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit => |lim| {
                if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset };
                cur = lim.input;
            },
            .filter => |f| {
                if (filter_state == null) filter_state = .{ .predicate = f.predicate };
                cur = f.input;
            },
            .project => |p| {
                if (project_items == null) {
                    project_items = p.items;
                }
                cur = p.input;
            },
            else => break,
        }
    }

    ctx.source.reset();
    var c: DataChunk = undefined;
    var project_state: ?ProjectState = if (project_items) |items| .{ .items = items } else null;
    const use_selected_project =
        filter_state != null and
        project_state != null and
        lim_state == null and
        exprCanUseBatchSelection(filter_state.?.predicate);
    var selection_buf: ?[]u32 = null;
    var mask_buf: ?[]i16 = null;
    while (true) {
        const scan_started = ctx.profile.timestamp();
        const has_chunk = try ctx.source.nextChunk(&c, ctx);
        ctx.profile.addElapsed(.scan, scan_started);
        if (!has_chunk) break;

        if (use_selected_project) {
            const n = c.num_rows;
            if (selection_buf == null or selection_buf.?.len < n) selection_buf = try ctx.allocator().alloc(u32, n);
            if (mask_buf == null or mask_buf.?.len < n) mask_buf = try ctx.allocator().alloc(i16, n);

            const filter_started = ctx.profile.timestamp();
            const sel = try kernels.evalExprSelection(
                filter_state.?.predicate,
                c,
                selection_buf.?[0..n],
                mask_buf.?[0..n],
                ctx.allocator(),
            );
            ctx.profile.addElapsed(.filter, filter_started);
            if (sel.isEmpty()) continue;

            var out = DataChunk{
                .columns = &.{},
                .num_rows = 0,
                .arena = c.arena,
            };
            const project_started = ctx.profile.timestamp();
            try project_state.?.applySelected(c.selected(sel), &out, ctx);
            ctx.profile.addElapsed(.project, project_started);
            if (ctx.profile.enabled) ctx.profile.recordReason("selected_project");
            const output_started = ctx.profile.timestamp();
            try sink.consume(out);
            ctx.profile.addElapsed(.output, output_started);
            continue;
        }

        if (filter_state) |*fs| {
            const filter_started = ctx.profile.timestamp();
            try fs.apply(&c, ctx);
            ctx.profile.addElapsed(.filter, filter_started);
        }
        if (lim_state) |*ls| ls.apply(&c);
        if (c.num_rows == 0) {
            if (lim_state) |ls| if (ls.done()) break;
            continue;
        }
        if (project_state) |*project| {
            const project_started = ctx.profile.timestamp();
            try project.apply(&c, ctx);
            ctx.profile.addElapsed(.project, project_started);
        }
        const output_started = ctx.profile.timestamp();
        try sink.consume(c);
        ctx.profile.addElapsed(.output, output_started);
        if (lim_state) |ls| if (ls.done()) break;
    }
}
const ChunkResult = chunk_sink_mod.ChunkSink.Result;

fn projectChunkResult(res: ChunkResult, items: []const plan.ProjectItem, ctx: *QueryContext) !ChunkResult {
    var state = ProjectState{ .items = items };
    for (res.chunks) |*c| try state.apply(c, ctx);
    const alloc = ctx.allocator();
    const metas = try alloc.alloc(result.ColMeta, items.len);
    for (items, 0..) |item, i| {
        metas[i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    return .{ .chunks = res.chunks, .metas = metas };
}

fn limitChunkResult(res: ChunkResult, limit: u64, offset: u64, ctx: *QueryContext) !ChunkResult {
    const alloc = ctx.allocator();
    var sink = try chunk_sink_mod.ChunkSink.init(alloc, res.metas);
    defer sink.deinit();
    const row_buf = try alloc.alloc(?Value, res.metas.len);
    var skipped: u64 = 0;
    var emitted: u64 = 0;
    outer: for (res.chunks) |*c| {
        for (0..c.num_rows) |r| {
            if (skipped < offset) {
                skipped += 1;
                continue;
            }
            if (emitted >= limit) break :outer;
            c.fillRow(r, row_buf);
            try sink.append(row_buf);
            emitted += 1;
        }
    }
    return try sink.finishResult();
}

fn filterChunkResult(res: ChunkResult, predicate: plan.Expr, ctx: *QueryContext) !ChunkResult {
    var state = FilterState{ .predicate = predicate };
    for (res.chunks) |*c| try state.apply(c, ctx);
    return res;
}

fn chunkResultToResultSet(res: ChunkResult, query_alloc: std.mem.Allocator, result_alloc: std.mem.Allocator) !ResultSet {
    var sink = ResultSink.init(result_alloc);
    for (res.chunks) |c| try sink.consume(c);
    query_alloc.free(res.chunks);
    query_alloc.free(res.metas);
    return try sink.finish();
}

fn executeNode(node: *const plan.PhysicalNode, ctx: *QueryContext) !ChunkResult {
    const alloc = ctx.allocator();
    switch (node.*) {
        .chunk_source => |cs| {
            return try executeNode(cs.input, ctx);
        },
        .part_scan, .mem_scan => {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.scan, phase_started);
            const schema_metas = ctx.source.schema();
            var sink = try chunk_sink_mod.ChunkSink.init(alloc, schema_metas);
            defer sink.deinit();
            var c: DataChunk = undefined;
            while (try ctx.source.nextChunk(&c, ctx)) {
                try sink.consumeChunk(c);
            }
            return try sink.finishResult();
        },
        .filter => |f| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.filter, phase_started);
            const inner = try executeNode(f.input, ctx);
            return try filterChunkResult(inner, f.predicate, ctx);
        },
        .project => |p| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.project, phase_started);
            if (try executeRawIntPointLookup(p.input, p.items, ctx)) |rl| {
                return try rowListToChunkResult(rl, alloc);
            }
            if (!projectItemsContainArrayJoin(p.items) and isScannable(p.input)) {
                return try rowListToChunkResult(try executeLimitChunked(node, ctx), alloc);
            }
            if (p.input.* == .top_k) {
                const tk = p.input.top_k;
                if (isScannable(tk.input)) {
                    const remapped_keys = try alloc.dupe(plan.SortKey, tk.keys);
                    var all_remapped = true;
                    for (remapped_keys) |*rk| {
                        if (rk.col_idx < p.items.len) {
                            const expr = p.items[rk.col_idx].expr;
                            if (expr == .col_ref) {
                                rk.col_idx = expr.col_ref.index;
                            } else {
                                all_remapped = false;
                                break;
                            }
                        }
                    }
                    if (all_remapped) {
                        var proj_over_scan = plan.PhysicalNode{ .project = .{ .input = tk.input, .items = p.items } };
                        return try rowListToChunkResult(try executeTopKFromScannable(&proj_over_scan, remapped_keys, @intCast(tk.k), ctx), alloc);
                    }
                }
            }
            const inner = try executeNode(p.input, ctx);
            if (projectItemsContainArrayJoin(p.items)) {
                const rl = try chunkResultToRowBuffer(inner, alloc);
                return try rowListToChunkResult(try projectRowBuffer(rl, p.items, alloc), alloc);
            }
            return try projectChunkResult(inner, p.items, ctx);
        },
        .limit => |lim| {
            if (isScannable(node)) {
                return try rowListToChunkResult(try executeLimitChunked(node, ctx), alloc);
            }
            if (lim.offset == 0 and lim.input.* == .hash_agg and isScannable(lim.input.hash_agg.input)) {
                const ha = lim.input.hash_agg;
                const k = @as(usize, @intCast(lim.limit));
                if (try executeHashAggParallelPairCount(ha.input, ha.keys, ha.aggs, &.{}, k, ctx)) |rl| {
                    return try rowListToChunkResult(rl, alloc);
                }
            }
            const inner = try executeNode(lim.input, ctx);
            return try limitChunkResult(inner, lim.limit, lim.offset, ctx);
        },
        .scalar_agg => |sa| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.aggregate, phase_started);
            if (isScannable(sa.input)) {
                if (try executeScalarAggParallel(sa.input, sa.aggs, ctx)) |r| {
                    return try rowListToChunkResult(r, alloc);
                }
                return try rowListToChunkResult(try executeScalarAggChunked(sa.input, sa.aggs, ctx), alloc);
            }
            const inner = try executeNode(sa.input, ctx);
            const rl = try chunkResultToRowBuffer(inner, alloc);
            return try rowListToChunkResult(try executeScalarAgg(rl, sa.aggs, alloc), alloc);
        },
        .hash_agg => |ha| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.aggregate, phase_started);
            if (isScannable(ha.input)) {
                return try executeHashAggScannable(ha, &.{}, 0, 0, ctx);
            }
            const inner = try executeNode(ha.input, ctx);
            const rl = try chunkResultToRowBuffer(inner, alloc);
            return try rowListToChunkResult(try executeHashAgg(rl, ha.keys, ha.aggs, alloc), alloc);
        },
        .order_by => |ob| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.topk, phase_started);
            const inner = try executeNode(ob.input, ctx);
            const rl = try chunkResultToRowBuffer(inner, alloc);
            return try rowListToChunkResult(try executeOrderBy(rl, ob.keys, alloc), alloc);
        },
        .top_k => |tk| {
            const phase_started = ctx.profile.timestamp();
            defer ctx.profile.addElapsed(.topk, phase_started);
            const k = @as(usize, @intCast(tk.k));
            if (isScannable(tk.input)) {
                return try rowListToChunkResult(try executeTopKFromScannable(tk.input, tk.keys, k, ctx), alloc);
            }
            if (tk.input.* == .hash_agg and isScannable(tk.input.hash_agg.input)) {
                const ha = tk.input.hash_agg;
                return try executeHashAggScannable(ha, tk.keys, k, @intCast(tk.offset), ctx);
            }
            const inner = try executeNode(tk.input, ctx);
            const out = try executeTopKFromChunkResult(inner, tk.keys, k, alloc);
            return try rowListToChunkResult(out, alloc);
        },
        .hash_join => |hj| {
            const left = try executeNode(hj.left, ctx);
            const right = try executeNode(hj.right, ctx);
            const right_rl = try chunkResultToRowBuffer(right, alloc);
            if (hj.join_type == .inner or hj.join_type == .left) {
                return try executeHashJoinProbeChunks(left, right_rl, hj, alloc);
            }
            const left_rl = try chunkResultToRowBuffer(left, alloc);
            return try rowListToChunkResult(try executeHashJoin(left_rl, right_rl, hj, alloc), alloc);
        },
    }
}

const executeOrderBy = agg_hash.executeOrderBy;
const updateCompactVals = agg_hash.updateCompactVals;
const agg_hash = @import("agg_hash.zig");
const executeHashAggScannable = agg_hash.executeHashAggScannable;
const executeHashAgg = agg_hash.executeHashAgg;
const executeHashAggParallelPairCount = agg_hash.executeHashAggParallelPairCount;

const buildAggResultMetas = agg.buildAggResultMetas;
const finalizeScalarAgg = agg.finalizeScalarAgg;
const extractLimit = agg.extractLimit;
const initAccumForAgg = agg.initAccumForAgg;
const deinitAccum = agg.deinitAccum;
const rowListToChunkResult = row_buffer.rowListToChunkResult;
const chunkResultToRowBuffer = row_buffer.chunkResultToRowBuffer;
const executeTopKFromChunkResult = topn_mod.executeTopKFromChunkResult;
const agg = @import("agg.zig");
const initAggAccums = agg.initAggAccums;
const executeScalarAggChunked = agg.executeScalarAggChunked;
const collectRawEqIndices = agg.collectRawEqIndices;
const executeRawIntPointLookup = agg.executeRawIntPointLookup;
const executeRawMinMaxAgg = agg.executeRawMinMaxAgg;
const executeScalarAggParallel = agg.executeScalarAggParallel;
const mergeAccum = agg.mergeAccum;
const updateAccumsFromChunk = agg.updateAccumsFromChunk;
const extractFilter = agg.extractFilter;
const executeScalarAgg = agg.executeScalarAgg;
const finalizeAccum = agg.finalizeAccum;
const evalAggArg = agg.evalAggArg;

fn valueToBool(v: ?Value) bool {
    return if (v) |val| switch (val) {
        .bool_u8 => |b| b != 0,
        .int64 => |i| i != 0,
        .uint64 => |u| u != 0,
        .float64 => |f| f != 0.0,
        else => false,
    } else false;
}

// ── Project helper ────────────────────────────────────────────────────────────

fn projectRowBuffer(inner: RowBuffer, items: []const plan.ProjectItem, alloc: std.mem.Allocator) !RowBuffer {
    const new_metas = try alloc.alloc(result.ColMeta, items.len);
    for (items, 0..) |item, ci| {
        new_metas[ci] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
    }
    var rl = RowBuffer.init(new_metas);

    // Detect arrayJoin(expr) calls among the projection items.
    // Collect all indices; they will be expanded in lockstep (element i for all).
    // Other columns repeat their value for each element.
    var aj_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer aj_indices.deinit(alloc);
    for (items, 0..) |item, ci| {
        switch (item.expr) {
            .fn_call => |fc| if (std.mem.eql(u8, fc.name, "arrayJoin")) {
                try aj_indices.append(alloc, ci);
            },
            else => {},
        }
    }

    for (inner.rows.items) |row| {
        if (aj_indices.items.len > 0) {
            // Evaluate the first arrayJoin argument to determine the expansion count.
            const first_ai = aj_indices.items[0];
            const first_aj_expr = switch (items[first_ai].expr) {
                .fn_call => |fc| fc.args[0],
                else => unreachable,
            };
            const first_arr_val = try kernels.evalExpr(first_aj_expr, row, null, alloc);
            const first_elements: []const []const u8 = switch (first_arr_val orelse Value{ .array_string = &.{} }) {
                .array_string => |a| a,
                else => &.{},
            };
            // Evaluate all arrayJoin arrays upfront (for lockstep expansion).
            const aj_arrays = try alloc.alloc([]const []const u8, aj_indices.items.len);
            aj_arrays[0] = first_elements;
            for (aj_indices.items[1..], 1..) |ai, k| {
                const aj_expr = switch (items[ai].expr) {
                    .fn_call => |fc| fc.args[0],
                    else => unreachable,
                };
                const arr_val = try kernels.evalExpr(aj_expr, row, null, alloc);
                aj_arrays[k] = switch (arr_val orelse Value{ .array_string = &.{} }) {
                    .array_string => |a| a,
                    else => &.{},
                };
            }
            // Emit one output row per element (or one null row if empty)
            const n = if (first_elements.len > 0) first_elements.len else @as(usize, 1);
            for (0..n) |ei| {
                const new_row = try alloc.alloc(?Value, items.len);
                // Find which aj_index slot this column corresponds to (if any).
                for (items, 0..) |item, ci| {
                    var is_aj = false;
                    for (aj_indices.items, 0..) |ai, k| {
                        if (ci == ai) {
                            const elems = aj_arrays[k];
                            new_row[ci] = if (elems.len > ei)
                                Value{ .string = elems[ei] }
                            else
                                null;
                            is_aj = true;
                            break;
                        }
                    }
                    if (!is_aj) {
                        new_row[ci] = try kernels.evalExpr(item.expr, row, null, alloc);
                    }
                }
                try rl.append(alloc, new_row);
            }
        } else {
            const new_row = try alloc.alloc(?Value, items.len);
            for (items, 0..) |item, ci| {
                const v_opt = try kernels.evalExpr(item.expr, row, null, alloc);
                new_row[ci] = v_opt;
            }
            try rl.append(alloc, new_row);
        }
    }
    return rl;
}

// ── Chunked agg helpers ───────────────────────────────────────────────────────

fn exprContainsArrayJoin(expr: plan.Expr) bool {
    return switch (expr) {
        .fn_call => |fc| {
            if (std.ascii.eqlIgnoreCase(fc.name, "arrayJoin")) return true;
            for (fc.args) |arg| if (exprContainsArrayJoin(arg)) return true;
            return false;
        },
        .add, .sub, .mul, .div, .mod, .eq, .neq, .lt, .lte, .gt, .gte, .@"and", .@"or", .like, .not_like, .concat => |b| {
            return exprContainsArrayJoin(b.left) or exprContainsArrayJoin(b.right);
        },
        .not, .is_null, .is_not_null => |u| exprContainsArrayJoin(u.operand),
        .case_when => |cw| {
            for (cw.when) |e| if (exprContainsArrayJoin(e)) return true;
            for (cw.then) |e| if (exprContainsArrayJoin(e)) return true;
            if (cw.else_expr) |e| if (exprContainsArrayJoin(e)) return true;
            return false;
        },
        .agg_call => |a| {
            if (a.arg) |arg| if (exprContainsArrayJoin(arg)) return true;
            if (a.post_expr) |post| if (exprContainsArrayJoin(post)) return true;
            return false;
        },
        .cast => |c| exprContainsArrayJoin(c.expr),
        .dict_call => |dc| {
            for (dc.keys) |key| if (exprContainsArrayJoin(key)) return true;
            if (dc.default_expr) |def| if (exprContainsArrayJoin(def)) return true;
            return false;
        },
        .lambda => |l| exprContainsArrayJoin(l.body.*),
        else => false,
    };
}

fn projectItemsContainArrayJoin(items: []const plan.ProjectItem) bool {
    for (items) |item| if (exprContainsArrayJoin(item.expr)) return true;
    return false;
}

/// Returns true if node is a direct source (part_scan/mem_scan) or a
/// filter/project/limit over a direct source — i.e. no pipeline breakers.
fn isScannable(node: *const plan.PhysicalNode) bool {
    return switch (node.*) {
        .part_scan, .mem_scan => true,
        .filter => |f| isScannable(f.input),
        .project => |p| !projectItemsContainArrayJoin(p.items) and isScannable(p.input),
        .limit => |l| isScannable(l.input),
        else => false,
    };
}

// ── Parallel filter-project for large tables ─────────────────────────────────
//
// Handles:  project → filter(pure-AND-int-conds) → scannable
// Splits the row range into morsels and processes them in parallel across
// defaultThreads() workers.  Each thread writes matching rows to its own
// per-thread ArenaAllocator (backed by c_allocator), then they are merged
// into the query allocator after completion.
// Returns null if the path cannot be used.
fn executeFilterProjectParallel(
    proj_items: []const plan.ProjectItem,
    filter_pred: plan.Expr,
    ctx: *QueryContext,
) !?RowBuffer {
    if (!ctx.source.supportsRange()) return null;
    const total_rows = ctx.source.rowCount();
    if (total_rows < 1_000_000) return null;
    const n_threads = parallel.defaultThreads();
    if (n_threads <= 1) return null;
    const alloc = ctx.allocator();

    // Require a pure-AND integer condition so we know the filter is cheap and
    // won't touch string columns, preventing scanner contention.
    var ic_buf: [8]IntCmpCond = undefined;
    var ic_n: usize = 0;
    if (!extractAndIntConds(filter_pred, &ic_buf, &ic_n, false) or ic_n == 0) return null;

    // Column pruning: only load columns referenced by project and filter.
    var scan_request: ScanRequest = .{};
    scan_request.addItems(proj_items);
    scan_request.addExpr(filter_pred);
    const scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    // Preload mapped columns before parallel phase.
    {
        var dummy: DataChunk = undefined;
        ctx.source.fetchRange(0, 0, &dummy, alloc) catch {};
    }

    const out_metas = try alloc.alloc(result.ColMeta, proj_items.len);
    for (proj_items, 0..) |item, i|
        out_metas[i] = .{ .name = item.alias, .col_type = item.out_type };

    const ParFpCtx = struct {
        source: SourceIface,
        proj_items: []const plan.ProjectItem,
        filter_pred: plan.Expr,
        morsel_src: *parallel.MorselSource,
        profile_enabled: bool,
        // Per-thread output: allocated via thread-local c_allocator arena.
        // Rows are moved into the global arena after completion.
        row_arena: std.heap.ArenaAllocator,
        rows: std.ArrayListUnmanaged([]?Value),
        scan_ns: u64 = 0,
        filter_ns: u64 = 0,
        project_ns: u64 = 0,
        err: ?anyerror = null,

        fn work(self: *@This(), _: *parallel.MorselSource) void {
            self.doWork() catch |e| {
                self.err = e;
            };
        }

        fn doWork(self: *@This()) !void {
            const row_alloc = self.row_arena.allocator();
            var ta = std.heap.ArenaAllocator.init(std.heap.c_allocator);
            defer ta.deinit();
            const tall = ta.allocator();

            const selection_buf = try tall.alloc(u32, parallel.default_morsel_size);
            const mask_buf = try tall.alloc(i16, parallel.default_morsel_size);
            const row_buf = try tall.alloc(?Value, self.source.schema().len);
            @memset(row_buf, null);

            while (self.morsel_src.next()) |m| {
                var ca = std.heap.ArenaAllocator.init(tall);
                defer ca.deinit();
                var c: DataChunk = undefined;
                const scan_started = if (self.profile_enabled) profileNowNs() else 0;
                try self.source.fetchRange(m.start, m.end - m.start, &c, ca.allocator());
                if (self.profile_enabled) self.scan_ns +|= @intCast(@max(0, profileNowNs() - scan_started));
                if (c.num_rows == 0) continue;

                // Keep fetched columns immutable. GenericStoreBridge may expose native-width
                // columns as zero-copy slices over a read-only mmap; physically compacting
                // such a chunk writes into PROT_READ memory (the former Q20 crash).
                const filter_started = if (self.profile_enabled) profileNowNs() else 0;
                const selected = try kernels.evalExprSelection(
                    self.filter_pred,
                    c,
                    selection_buf[0..c.num_rows],
                    mask_buf[0..c.num_rows],
                    tall,
                );
                if (self.profile_enabled) self.filter_ns +|= @intCast(@max(0, profileNowNs() - filter_started));
                if (selected.isEmpty()) continue;

                // Project matching rows to output.
                const project_started = if (self.profile_enabled) profileNowNs() else 0;
                for (selected.slice()) |physical_row| {
                    for (c.columns, 0..) |col, j| {
                        if (col.pruned) continue;
                        row_buf[j] = if (col.isRowNull(physical_row)) null else col.data.get(physical_row);
                    }
                    const out_row = try row_alloc.alloc(?Value, self.proj_items.len);
                    for (self.proj_items, 0..) |item, i| {
                        out_row[i] = try kernels.evalExpr(item.expr, row_buf, null, row_alloc);
                    }
                    try self.rows.append(row_alloc, out_row);
                }
                if (self.profile_enabled) self.project_ns +|= @intCast(@max(0, profileNowNs() - project_started));
            }
        }
    };

    var morsel_src = parallel.MorselSource.init(total_rows, parallel.default_morsel_size);
    const par_ctxs = try alloc.alloc(ParFpCtx, n_threads);
    for (par_ctxs) |*pc| {
        pc.* = .{
            .source = ctx.source,
            .proj_items = proj_items,
            .filter_pred = filter_pred,
            .morsel_src = &morsel_src,
            .profile_enabled = ctx.profile.enabled,
            .row_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator),
            .rows = .empty,
        };
    }
    try parallel.parallelFor(alloc, ParFpCtx, ParFpCtx.work, par_ctxs, &morsel_src);

    // Merge per-thread row lists into global output; copy strings to query arena.
    var rl = RowBuffer.init(out_metas);
    for (par_ctxs) |*pc| {
        ctx.profile.scan_ns +|= pc.scan_ns;
        ctx.profile.filter_ns +|= pc.filter_ns;
        ctx.profile.project_ns +|= pc.project_ns;
        if (pc.err) |e| {
            for (par_ctxs) |*p2| p2.row_arena.deinit();
            return e;
        }
        for (pc.rows.items) |src_row| {
            const dst_row = try alloc.alloc(?Value, src_row.len);
            for (src_row, 0..) |v_opt, i| {
                dst_row[i] = if (v_opt) |v| switch (v) {
                    .string => |s| Value{ .string = try alloc.dupe(u8, s) },
                    else => v,
                } else null;
            }
            try rl.append(alloc, dst_row);
        }
        pc.row_arena.deinit();
    }
    return rl;
}

// ── Chunked limit helper ──────────────────────────────────────────────────────

/// Chunked streaming execution for limit/project/filter/scan patterns.
fn executeLimitChunked(node: *const plan.PhysicalNode, ctx: *QueryContext) !RowBuffer {
    const alloc = ctx.allocator();

    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: LimitState = .{ .limit = std.math.maxInt(u64), .offset = 0 };

    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit => |lim| {
                lim_state = .{ .limit = lim.limit, .offset = lim.offset };
                cur = lim.input;
            },
            .filter => |f| {
                if (filter_state == null) filter_state = .{ .predicate = f.predicate };
                cur = f.input;
            },
            .project => |p| {
                if (project_items == null) project_items = p.items;
                cur = p.input;
            },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);
    var rl = RowBuffer.init(out_metas);

    ctx.source.reset();
    var c: DataChunk = undefined;
    var skipped: u64 = 0;
    var emitted: u64 = 0;
    var row_ref_indices: ?[]usize = null;
    var row_buf: []?Value = &.{}; // allocated once on first chunk

    while (try ctx.source.nextChunk(&c, ctx)) {
        if (filter_state) |*fs| try fs.apply(&c, ctx);
        if (c.num_rows == 0) continue;

        if (row_ref_indices == null and c.columns.len > 0) {
            const mask = try alloc.alloc(bool, c.columns.len);
            @memset(mask, false);
            if (project_items) |items| {
                for (items) |item| collectColRefs(item.expr, mask);
            } else @memset(mask, true);
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
            row_ref_indices = idxs;
            // Allocate row_buf once (reused across all chunks).
            row_buf = try alloc.alloc(?Value, c.columns.len);
            @memset(row_buf, null);
        }
        const refs = row_ref_indices orelse &[_]usize{};

        for (0..c.num_rows) |r| {
            if (skipped < lim_state.offset) {
                skipped += 1;
                continue;
            }
            if (emitted >= lim_state.limit) break;
            for (refs) |j| {
                const col = c.columns[j];
                row_buf[j] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            const out_row: []?Value = if (project_items) |items| blk: {
                const out = try alloc.alloc(?Value, items.len);
                for (items, 0..) |item, i| out[i] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
                break :blk out;
            } else blk: {
                const out = try alloc.alloc(?Value, c.columns.len);
                for (c.columns, 0..) |col, j| out[j] = if (col.isRowNull(r)) null else col.data.get(r);
                break :blk out;
            };
            try rl.append(alloc, out_row);
            emitted += 1;
        }
        if (emitted >= lim_state.limit) break;
    }
    return rl;
}

// ── ScalarAgg helper ──────────────────────────────────────────────────────────

// ── HashAgg helper ────────────────────────────────────────────────────────────

// ── OrderBy helper ────────────────────────────────────────────────────────────

/// Heap-based top-K selection: O(n log k) time, O(k) extra memory.
/// Returns exactly min(k, n) rows in sorted order.
/// Total-order comparator shared by TopK/OrderBy and the streaming collector.
/// `lessThan(a, b)` means a should appear before b in the output.
const topn_mod = @import("topn.zig");
const rowLessThan = topn_mod.rowLessThan;
const TopKSortCtx = topn_mod.TopKSortCtx;
const TopKCollector = topn_mod.TopKCollector;
const TopNSink = topn_mod.TopNSink;
const HeapEntry = topn_mod.HeapEntry;
const lateLess = topn_mod.lateLess;
const LateSortCtx = topn_mod.LateSortCtx;
const LateTopNHeap = topn_mod.LateTopNHeap;
const executeTopK = topn_mod.executeTopK;

/// Stream a scannable node (scan/filter/project/limit) directly into a min-heap
/// of at most K rows, avoiding materialisation of all rows into a RowBuffer.
/// Late-materialization top-K: phase 1 scans only filter+sort columns using fetchRange
/// (so global row indices are stable), phase 2 fetches all columns for the K winners.
/// Returns null if unable to proceed (falls back to standard path).
fn executeTopKLateMat(
    schema_metas: []const result.ColMeta,
    out_metas: []const result.ColMeta,
    project_items: ?[]const plan.ProjectItem,
    filter_pred: plan.Expr,
    keys: []const plan.SortKey,
    k: usize,
    ctx: *QueryContext,
    alloc: std.mem.Allocator,
) !?RowBuffer {
    const total_rows = ctx.source.rowCount();
    if (total_rows == 0) return RowBuffer.init(@constCast(out_metas));

    // Phase 1: restrict source to scan cols, iterate fetchRange morsels.
    var scan_request: ScanRequest = .{};
    scan_request.addExpr(filter_pred);
    for (keys) |key| scan_request.addIndex(key.col_idx);
    const scan_scope = scan_request.apply(ctx.source, .any_reduction);
    defer scan_scope.deinit();

    var late = try LateTopNHeap.init(alloc, keys, k);
    const key_scratch = try alloc.alloc(?Value, keys.len);
    @memset(key_scratch, null);

    const morsel_size: usize = 65536;
    var pos: u64 = 0;
    var phase1_fs = FilterState{ .predicate = filter_pred };
    var chunk_arena = std.heap.ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();
    var fake_ctx: QueryContext = .{
        .parent_alloc = ctx.resultAllocator(),
        .arena = std.heap.ArenaAllocator.init(chunk_arena.allocator()),
        .source = ctx.source,
        .profile = .{},
    };

    // Pre-compile LikeGuards from filter predicate (once per query).
    // Used in phase 1 to fast-reject rows before evalExpr.
    var phase1_like_guards_list = std.ArrayListUnmanaged(LikeGuard).empty;
    collectLikeGuards(filter_pred, &phase1_like_guards_list, alloc);
    const phase1_like_guards = phase1_like_guards_list.items;

    // True when filter predicate is purely a single LIKE/NOT_LIKE col_ref — then
    // we can skip evalExpr entirely and use LikeMatcher.match directly.
    const phase1_pure_like: bool = switch (filter_pred) {
        .like, .not_like => phase1_like_guards.len == 1,
        else => false,
    };
    // True when filter is exactly `col_ref != ''` (non-empty string check).
    // Only the offset difference needs to be checked — no string bytes needed.
    const phase1_pure_neq_empty: bool = switch (filter_pred) {
        .neq => |op| op.left == .col_ref and op.right == .lit_str and op.right.lit_str.len == 0,
        else => false,
    };
    const neq_empty_col_idx: usize = if (phase1_pure_neq_empty) filter_pred.neq.left.col_ref.index else 0;

    // ── Parallel Phase 1 ─────────────────────────────────────────────────────
    // For large datasets with a pure LIKE filter (e.g. Q24) or a col != ''
    // non-empty check (e.g. Q25/Q27), parallelize Phase 1 across N threads each
    // maintaining a local heap of size k. Merge after: collect all N local heaps,
    // sort, keep top-k.
    const n_par_threads = parallel.defaultThreads();
    const use_parallel_phase1 = (phase1_pure_like or phase1_pure_neq_empty) and
        total_rows >= 200_000 and
        n_par_threads > 1 and
        ctx.source.supportsRange();

    if (use_parallel_phase1) {
        // For LIKE: extract matcher/negate from like_guards[0].
        // For neq_empty: no LIKE guards — use neq_empty_col_idx instead.
        var filt_col_name: []const u8 = "";
        var filt_col_idx_par: usize = 0;
        var matcher0: kernels.LikeMatcher = undefined;
        var negate0: bool = false;
        if (phase1_pure_like) {
            const lg0 = phase1_like_guards[0];
            filt_col_name = if (lg0.col_idx < schema_metas.len) schema_metas[lg0.col_idx].name else "";
            filt_col_idx_par = lg0.col_idx;
            matcher0 = lg0.matcher;
            negate0 = lg0.negate;
        } else { // phase1_pure_neq_empty
            filt_col_name = if (neq_empty_col_idx < schema_metas.len) schema_metas[neq_empty_col_idx].name else "";
            filt_col_idx_par = neq_empty_col_idx;
        }

        const ParPhase1Ctx = struct {
            source: SourceIface,
            keys: []const plan.SortKey,
            like_col_idx: usize,
            matcher: kernels.LikeMatcher,
            negate: bool,
            /// When true, filter is `col != ''` — only check offset[r+1] > offset[r].
            neq_empty: bool = false,
            k: usize,
            schema_len: usize,
            morsel_src: *parallel.MorselSource,
            parent_alloc: std.mem.Allocator,
            // Raw-byte fast path: if set, skip fetchRange in phase 1.
            raw_filter_offsets: ?[]const u64 = null,
            raw_filter_bytes: ?[]const u8 = null,
            raw_string_tiebreak: bool = false,
            /// Raw sort key i64 values (e.g. EventTime).
            raw_sortkey_i64: ?[]const i64 = null,
            raw_sortkey_col_idx: usize = 0,
            // Output (allocated per-ctx from parent_alloc after run).
            local_heap: []HeapEntry = &.{},
            local_len: usize = 0,
            err: ?anyerror = null,

            const LHeapEntry = struct { global_row: u64, key_vals: []?Value };

            fn work(self: *@This(), _: *parallel.MorselSource) void {
                self.run() catch |e| {
                    self.err = e;
                };
            }

            fn run(self: *@This()) !void {
                var arena = std.heap.ArenaAllocator.init(self.parent_alloc);
                defer arena.deinit();
                const talloc = arena.allocator();
                var local = try LateTopNHeap.init(talloc, self.keys, self.k);
                const local_key_scratch = try talloc.alloc(?Value, @max(1, self.keys.len));
                @memset(local_key_scratch, null);

                while (self.morsel_src.next()) |m| {
                    // ── Raw fast path: skip fetchRange, no per-row allocation ─
                    if (self.raw_filter_offsets) |ro| {
                        for (m.start..m.end) |r| {
                            // Filter check: either LIKE/NOT_LIKE or neq_empty (offset diff).
                            const passes: bool = if (self.neq_empty)
                                ro[r + 1] > ro[r] // non-empty string
                            else blk: {
                                const rb = self.raw_filter_bytes.?;
                                const lo: usize = @intCast(ro[r]);
                                const hi: usize = @intCast(ro[r + 1]);
                                break :blk self.matcher.match(rb[lo..hi]) != self.negate;
                            };
                            if (!passes) continue;
                            const global_row: u64 = @intCast(r);
                            // Store sort key inline — no per-row allocation.
                            const sk_val: i64 = if (self.raw_sortkey_i64) |rk| rk[r] else 0;
                            const sk_str: []const u8 = if (self.raw_string_tiebreak) blk: {
                                const rb = self.raw_filter_bytes.?;
                                const lo: usize = @intCast(ro[r]);
                                const hi: usize = @intCast(ro[r + 1]);
                                break :blk rb[lo..hi];
                            } else &.{};
                            local.offerRaw(global_row, sk_val, sk_str);
                        }
                        continue; // next morsel, raw path done
                    }
                    // ── Standard path: use fetchRange ─────────────────────────
                    var morsel_chunk_arena = std.heap.ArenaAllocator.init(talloc);
                    defer morsel_chunk_arena.deinit();
                    var c: DataChunk = undefined;
                    try self.source.fetchRange(@intCast(m.start), m.end - m.start, &c, morsel_chunk_arena.allocator());

                    if (self.like_col_idx >= c.columns.len) continue;
                    const filter_col = c.columns[self.like_col_idx];
                    // For LIKE: require .string; for neq_empty: accept .string or .bool_u8.
                    const is_string = filter_col.data == .string;
                    const is_bool_u8 = filter_col.data == .bool_u8;
                    if (!self.neq_empty and !is_string) continue;
                    if (self.neq_empty and !is_string and !is_bool_u8) continue;

                    for (0..c.num_rows) |r| {
                        const passes: bool = if (self.neq_empty)
                            (if (is_bool_u8) filter_col.data.bool_u8[r] != 0 else filter_col.data.string[r].len != 0)
                        else blk: {
                            const s = if (filter_col.isRowNull(r)) "" else filter_col.data.string[r];
                            break :blk self.matcher.match(s) != self.negate;
                        };
                        if (!passes) continue;
                        const global_row = m.start + r;

                        // Fill scratch (compact, size = keys.len).
                        for (self.keys, 0..) |key, ki| {
                            if (key.col_idx < c.columns.len) {
                                const col2 = &c.columns[key.col_idx];
                                local_key_scratch[ki] = if (col2.isRowNull(r)) null else col2.data.get(r);
                            } else {
                                local_key_scratch[ki] = null;
                            }
                        }

                        try local.offerKeys(@intCast(global_row), local_key_scratch);
                    }
                }

                self.local_heap = try local.copyTo(self.parent_alloc);
                self.local_len = local.len;
            }
        };

        var morsel_src = parallel.MorselSource.init(@intCast(total_rows), 65536);
        const pctxs = try alloc.alloc(ParPhase1Ctx, n_par_threads);

        // Try raw fast path: get filter column's raw offsets and bytes.
        // For neq_empty, bytes are only needed when the filter column is also a
        // string sort tie-breaker.
        const raw_filt_offsets = ctx.source.getRawStrOffsets(filt_col_name);
        const raw_second_key_is_filter_str =
            keys.len == 2 and keys[1].col_idx == filt_col_idx_par and filt_col_idx_par < schema_metas.len and
            schema_metas[filt_col_idx_par].col_type == .string;
        const need_raw_filter_bytes = !phase1_pure_neq_empty or raw_second_key_is_filter_str;
        const raw_filt_bytes = if (raw_filt_offsets != null and need_raw_filter_bytes)
            ctx.source.getRawStrBytes(filt_col_name)
        else
            null;
        // For LIKE: need both offsets + bytes; for neq_empty: only offsets needed.
        const use_raw_phase1 = raw_filt_offsets != null and
            (phase1_pure_neq_empty or raw_filt_bytes != null) and
            (!raw_second_key_is_filter_str or raw_filt_bytes != null);
        const raw_sk_i64: ?[]const i64 = if (use_raw_phase1 and keys.len >= 1 and keys[0].col_idx < schema_metas.len) blk: {
            const sk_name = schema_metas[keys[0].col_idx].name;
            if (ctx.source.vtable.getRawInt64Col) |f| {
                break :blk f(ctx.source.ptr, sk_name);
            }
            break :blk null;
        } else null;
        const raw_sk_col_idx: usize = if (keys.len >= 1) keys[0].col_idx else 0;
        // Raw path is only correct when we have sort key data for ALL sort keys.
        const raw_path_safe = keys.len == 0 or
            (keys.len == 1 and raw_sk_i64 != null) or
            (keys.len == 2 and raw_sk_i64 != null and raw_second_key_is_filter_str);

        for (pctxs) |*pc| {
            pc.* = .{
                .source = ctx.source,
                .keys = keys,
                .like_col_idx = filt_col_idx_par,
                .matcher = matcher0,
                .negate = negate0,
                .neq_empty = phase1_pure_neq_empty,
                .k = k,
                .schema_len = schema_metas.len,
                .morsel_src = &morsel_src,
                .parent_alloc = alloc,
                .raw_filter_offsets = if (use_raw_phase1 and raw_path_safe) raw_filt_offsets else null,
                .raw_filter_bytes = if (use_raw_phase1 and raw_path_safe and need_raw_filter_bytes) raw_filt_bytes else null,
                .raw_string_tiebreak = raw_second_key_is_filter_str,
                .raw_sortkey_i64 = raw_sk_i64,
                .raw_sortkey_col_idx = raw_sk_col_idx,
            };
        }
        try parallel.parallelFor(alloc, ParPhase1Ctx, ParPhase1Ctx.work, pctxs, &morsel_src);
        for (pctxs) |pc| {
            if (pc.err) |e| return e;
        }

        // Merge: collect all local heaps into one buffer, sort, take top-k.
        var total_candidates: usize = 0;
        for (pctxs) |pc| total_candidates += pc.local_len;
        const merged = try alloc.alloc(HeapEntry, total_candidates);
        var mi: usize = 0;
        for (pctxs) |pc| {
            @memcpy(merged[mi .. mi + pc.local_len], pc.local_heap[0..pc.local_len]);
            mi += pc.local_len;
        }
        std.sort.pdq(HeapEntry, merged, LateSortCtx{ .keys = keys }, LateSortCtx.lessThan);
        const take = @min(k, merged.len);

        // Phase 2: load columns for top-k rows and project to output schema.
        ctx.source.setNeededCols(null);
        var rl2 = RowBuffer.init(@constCast(out_metas));
        for (merged[0..take]) |entry| {
            var full_chunk: DataChunk = undefined;
            try ctx.source.fetchRange(entry.global_row, 1, &full_chunk, alloc);
            const full_row = try full_chunk.readRow(0, alloc);
            if (project_items) |items| {
                const proj_row = try alloc.alloc(?Value, items.len);
                for (items, 0..) |item, pi| {
                    proj_row[pi] = try kernels.evalExpr(item.expr, full_row, null, alloc);
                }
                try rl2.append(alloc, proj_row);
            } else {
                try rl2.append(alloc, full_row);
            }
        }
        return rl2;
    }

    while (pos < total_rows) {
        const n = @min(morsel_size, total_rows - pos);
        var c: DataChunk = undefined;
        try ctx.source.fetchRange(pos, n, &c, chunk_arena.allocator());

        // Apply filter; rows that pass keep their in-chunk index.
        // We need to know the original global index of each passing row.
        // Since we use fetchRange (not nextChunk), row i in chunk = global row pos+i.
        // BUT: the filter compacts rows in-place, losing original indices.
        // Solution: apply filter manually without compaction — just compute a pass mask.
        const pass_mask = try chunk_arena.allocator().alloc(bool, c.num_rows);
        @memset(pass_mask, true);

        // Evaluate filter per-row using a non-compacting approach.
        // We need ref_indices and row_buf from FilterState.
        if (phase1_fs.ref_indices == null) try phase1_fs.apply(&c, &fake_ctx);
        // Use int_conds if available for fast path.
        if (phase1_fs.int_conds) |conds| {
            if (conds.len > 0 and phase1_fs.int_conds_complete) {
                for (0..c.num_rows) |r| {
                    for (conds) |cond| {
                        if (cond.col_idx >= c.columns.len) {
                            pass_mask[r] = false;
                            break;
                        }
                        const col = c.columns[cond.col_idx];
                        if (col.isRowNull(r)) {
                            pass_mask[r] = false;
                            break;
                        }
                        const v: i64 = switch (col.data) {
                            .int64 => |a| a[r],
                            .uint64 => |a| @bitCast(a[r]),
                            .date_u16 => |a| @as(i64, a[r]),
                            .datetime64_ms => |a| a[r],
                            .bool_u8 => |a| @as(i64, a[r]),
                            else => {
                                pass_mask[r] = false;
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
                            pass_mask[r] = false;
                            break;
                        }
                    }
                }
            }
        } else {
            // General path: evalExpr per row.
            const ref = phase1_fs.ref_indices orelse &.{};
            const row = phase1_fs.row_buf orelse try chunk_arena.allocator().alloc(?Value, c.columns.len);
            for (0..c.num_rows) |r| {
                for (ref) |j| {
                    if (j < c.columns.len) {
                        const col = c.columns[j];
                        row[j] = if (col.isRowNull(r)) null else col.data.get(r);
                    }
                }
                const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
            }
        }

        // For LIKE-based filters, re-run with like_guards or general evalExpr.
        // Actually for Q24 the int_conds path won't fire (LIKE is not int).
        // We need the general evalExpr path to handle LIKE.
        // Let's use a combined approach: check int_conds (if any) then evalExpr for LIKE.
        // The above logic handles int-only conds. For LIKE we need the general path.
        // Redo with general evalExpr for non-int-complete predicates:
        if (phase1_fs.int_conds == null or !phase1_fs.int_conds_complete) {
            if (phase1_pure_like and phase1_like_guards.len == 1) {
                // Fast path: pure col_ref LIKE/NOT_LIKE lit_str — use pre-compiled LikeMatcher.
                const lg = phase1_like_guards[0];
                if (lg.col_idx < c.columns.len and c.columns[lg.col_idx].data == .string) {
                    const col = c.columns[lg.col_idx];
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        pass_mask[r] = lg.matcher.match(s) != lg.negate;
                    }
                } else {
                    // Column not string or out of range — fall back to evalExpr.
                    const ref = phase1_fs.ref_indices orelse &.{};
                    const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                    @memset(row, null);
                    for (0..c.num_rows) |r| {
                        if (!pass_mask[r]) continue;
                        for (ref) |j| {
                            if (j < c.columns.len) {
                                const col2 = c.columns[j];
                                row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                            }
                        }
                        const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                        pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                    }
                }
            } else if (phase1_like_guards.len > 0) {
                // Multi-guard path: pre-filter with like_guards, then evalExpr only for survivors.
                const ref = phase1_fs.ref_indices orelse &.{};
                const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                @memset(row, null);
                row_loop: for (0..c.num_rows) |r| {
                    if (!pass_mask[r]) continue;
                    // Check each LIKE guard with LikeMatcher (fast-reject).
                    for (phase1_like_guards) |lg| {
                        if (lg.col_idx >= c.columns.len) {
                            pass_mask[r] = false;
                            continue :row_loop;
                        }
                        const col = c.columns[lg.col_idx];
                        if (col.data != .string) continue;
                        const s = if (col.isRowNull(r)) "" else col.data.string[r];
                        if (lg.matcher.match(s) == lg.negate) {
                            pass_mask[r] = false;
                            continue :row_loop;
                        }
                    }
                    // Guards passed — evalExpr for full predicate.
                    for (ref) |j| {
                        if (j < c.columns.len) {
                            const col2 = c.columns[j];
                            row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                        }
                    }
                    const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                    pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                }
            } else {
                // General path: no LIKE guards — evalExpr per row.
                const ref = phase1_fs.ref_indices orelse &.{};
                const row = try chunk_arena.allocator().alloc(?Value, c.columns.len);
                @memset(row, null);
                for (0..c.num_rows) |r| {
                    if (!pass_mask[r]) continue;
                    for (ref) |j| {
                        if (j < c.columns.len) {
                            const col2 = c.columns[j];
                            row[j] = if (col2.isRowNull(r)) null else col2.data.get(r);
                        }
                    }
                    const v = try kernels.evalExpr(filter_pred, row, null, chunk_arena.allocator());
                    pass_mask[r] = if (v) |val| val.bool_u8 != 0 else false;
                }
            }
        }

        for (0..c.num_rows) |r| {
            if (!pass_mask[r]) continue;
            const global_row = pos + r;
            for (keys, 0..) |key, ki| {
                if (key.col_idx < c.columns.len) {
                    const col = &c.columns[key.col_idx];
                    key_scratch[ki] = if (col.isRowNull(r)) null else col.data.get(r);
                } else {
                    key_scratch[ki] = null;
                }
            }
            try late.offerKeys(global_row, key_scratch);
        }

        pos += n;
        _ = chunk_arena.reset(.retain_capacity);
        fake_ctx.arena = std.heap.ArenaAllocator.init(chunk_arena.allocator());
    }

    const winners = late.finish();

    // Phase 2: fetch all columns for the K winner rows.
    ctx.source.setNeededCols(null); // restore full decode
    var rl = RowBuffer.init(@constCast(out_metas));
    for (winners) |entry| {
        var full_chunk: DataChunk = undefined;
        try ctx.source.fetchRange(entry.global_row, 1, &full_chunk, alloc);
        const row = try full_chunk.readRow(0, alloc);
        if (project_items) |items| {
            const proj_row = try alloc.alloc(?Value, items.len);
            for (items, 0..) |item, pi| {
                proj_row[pi] = try kernels.evalExpr(item.expr, row, null, alloc);
            }
            try rl.append(alloc, proj_row);
        } else {
            try rl.append(alloc, row);
        }
    }
    return rl;
}

fn executeTopKFromScannable(
    node: *const plan.PhysicalNode,
    keys: []const plan.SortKey,
    k: usize,
    ctx: *QueryContext,
) !RowBuffer {
    const alloc = ctx.allocator();

    // Traverse to extract filter / project / limit wrappers.
    var filter_state: ?FilterState = null;
    var project_items: ?[]const plan.ProjectItem = null;
    var lim_state: ?LimitState = null;
    var cur = node;
    while (true) {
        switch (cur.*) {
            .limit => |lim| {
                if (lim_state == null) lim_state = .{ .limit = lim.limit, .offset = lim.offset };
                cur = lim.input;
            },
            .filter => |f| {
                if (filter_state == null) filter_state = .{ .predicate = f.predicate };
                cur = f.input;
            },
            .project => |p| {
                if (project_items == null) project_items = p.items;
                cur = p.input;
            },
            else => break,
        }
    }

    const schema_metas = ctx.source.schema();
    const out_metas: []result.ColMeta = if (project_items) |items| blk: {
        const m = try alloc.alloc(result.ColMeta, items.len);
        for (items, 0..) |item, i| m[i] = .{ .name = item.alias, .col_type = item.out_type, .ch_type = item.ch_type };
        break :blk m;
    } else try alloc.dupe(result.ColMeta, schema_metas);

    if (k == 0) return RowBuffer.init(out_metas);

    // Remap sort keys: if keys use output-column indices (e.g. from findOutputColIdx
    // in the planner) but we sort raw schema rows, map output index → schema col_ref index.
    // Only remap when project_items is present and all sort keys are simple col_refs.
    const effective_keys: []const plan.SortKey = if (project_items) |items| blk: {
        const remapped = try alloc.dupe(plan.SortKey, keys);
        var all_remapped = true;
        for (remapped) |*rk| {
            if (rk.col_idx < items.len) {
                const expr = items[rk.col_idx].expr;
                if (expr == .col_ref) {
                    // Only remap if the output index != schema index (avoids double-remap
                    // when keys already carry schema indices from tbl.findColumn).
                    // Heuristic: if col_idx < items.len AND items[col_idx].expr.col_ref.index
                    // != col_idx, it's an output-relative index.
                    rk.col_idx = expr.col_ref.index;
                } else {
                    all_remapped = false;
                    break;
                }
            }
        }
        if (all_remapped) break :blk remapped;
        break :blk keys; // fallback: use keys as-is
    } else keys;

    // ── Late-materialization path ─────────────────────────────────────────────
    // For SELECT * with a filter, scan with only filter+sort columns (phase 1)
    // to avoid decoding all 100+ columns per row. Track the global row indices
    // of the top-K winners. Then fetch only those K rows with all columns (phase 2).
    // Detect if this is effectively SELECT * (all project items are identity col_refs).
    const is_select_star = if (project_items) |items| blk: {
        if (items.len != schema_metas.len) break :blk false;
        var all_ident = true;
        for (items, 0..) |item, i| {
            if (item.expr != .col_ref or item.expr.col_ref.index != i) {
                all_ident = false;
                break;
            }
        }
        break :blk all_ident;
    } else true;

    // Use late-mat for SELECT * (all columns) OR narrow projections (few output cols).
    // For narrow: benefit is the same — phase 1 scans only filter+sort cols,
    // phase 2 fetches K rows with projected cols.
    const is_narrow_project = !is_select_star and
        project_items != null and
        project_items.?.len * 4 < schema_metas.len;
    const use_late_mat = (is_select_star or is_narrow_project) and
        filter_state != null and
        ctx.source.vtable.setNeededCols != null and
        ctx.source.supportsRange() and
        schema_metas.len > 8; // only worth it for wide schemas

    // For non-SELECT* with few needed cols, also apply column restriction during scan.
    // This covers Q25/Q26/Q27 style: SELECT col1 WHERE col2 <> '' ORDER BY col3.
    const narrow_scan_possible = !is_select_star and
        ctx.source.vtable.setNeededCols != null and
        filter_state != null;
    var scan_request: ScanRequest = .{};
    if (narrow_scan_possible) {
        if (filter_state) |*fs| scan_request.addExpr(fs.predicate);
        for (effective_keys) |key| scan_request.addIndex(key.col_idx);
        if (project_items) |items| scan_request.addItems(items);
    }
    const scan_scope = scan_request.apply(ctx.source, .any_reduction);

    if (use_late_mat) {
        const result_opt = try executeTopKLateMat(
            schema_metas,
            out_metas,
            project_items,
            filter_state.?.predicate,
            effective_keys,
            k,
            ctx,
            alloc,
        );
        if (result_opt) |rl| return rl;
        // If late_mat failed for any reason, fall through to standard path.
    }

    defer scan_scope.deinit();

    // Stream filtered rows straight into the bounded TopN sink. Only accepted
    // rows are materialised; rejected rows are compared on sort-key columns.
    var topn = try TopNSink.init(alloc, schema_metas, effective_keys, k);
    var chunk_arena = std.heap.ArenaAllocator.init(alloc);
    defer chunk_arena.deinit();

    var fs_mut = filter_state;
    var ls_mut = lim_state;
    const total_rows = ctx.source.rowCount();
    const morsel_sz: usize = 65536;

    if (narrow_scan_possible and ctx.source.supportsRange()) {
        var pos: u64 = 0;
        while (pos < total_rows) {
            const n = @min(morsel_sz, total_rows - pos);
            var c: DataChunk = undefined;
            _ = chunk_arena.reset(.retain_capacity);
            try ctx.source.fetchRange(pos, n, &c, chunk_arena.allocator());
            pos += n;
            if (fs_mut) |*fs| try fs.apply(&c, ctx);
            if (ls_mut) |*ls| ls.apply(&c);
            if (c.num_rows > 0) try topn.pushChunk(&c);
            if (ls_mut) |*ls| if (ls.done()) break;
        }
    } else {
        ctx.source.reset();
        var c: DataChunk = undefined;
        while (try ctx.source.nextChunk(&c, ctx)) {
            if (fs_mut) |*fs| try fs.apply(&c, ctx);
            if (ls_mut) |*ls| ls.apply(&c);
            if (c.num_rows > 0) try topn.pushChunk(&c);
            if (ls_mut) |*ls| if (ls.done()) break;
        }
    }

    const raw = try topn.finish();
    var rl = RowBuffer.init(out_metas);
    if (project_items) |items| {
        const row_buf = try alloc.alloc(?Value, schema_metas.len);
        for (raw.rows.items) |raw_row| {
            const proj_row = try alloc.alloc(?Value, items.len);
            @memcpy(row_buf[0..raw_row.len], raw_row);
            for (items, 0..) |item, ci| {
                proj_row[ci] = try kernels.evalExpr(item.expr, row_buf, null, alloc);
            }
            try rl.append(alloc, proj_row);
        }
    } else {
        for (raw.rows.items) |row| try rl.append(alloc, row);
    }
    return rl;
}

// ── HashJoin helper ───────────────────────────────────────────────────────────

/// Build the right side once, then probe the left side chunk-by-chunk.
/// Avoids materializing the left input, which is the common fact-table side.
fn executeHashJoinProbeChunks(
    left: ChunkResult,
    right_rl: RowBuffer,
    hj: plan.HashJoinNode,
    alloc: std.mem.Allocator,
) !ChunkResult {
    var jht = try ht.JoinHashTable.init(alloc);
    const key_buf = try alloc.alloc(Value, hj.equi_keys.len);
    for (right_rl.rows.items, 0..) |row, ri| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = (if (ek.right_col_idx < row.len) row[ek.right_col_idx] else null) orelse Value{ .int64 = 0 };
        }
        try jht.insert(key_buf, @intCast(ri));
    }

    const combined_metas = try alloc.alloc(result.ColMeta, left.metas.len + right_rl.metas.len);
    @memcpy(combined_metas[0..left.metas.len], left.metas);
    @memcpy(combined_metas[left.metas.len..], right_rl.metas);

    var sink = try chunk_sink_mod.ChunkSink.init(alloc, combined_metas);
    defer sink.deinit();
    const lrow = try alloc.alloc(?Value, left.metas.len);

    for (left.chunks) |*c| {
        for (0..c.num_rows) |r| {
            c.fillRow(r, lrow);
            for (hj.equi_keys, 0..) |ek, ki| {
                key_buf[ki] = (if (ek.left_col_idx < lrow.len) lrow[ek.left_col_idx] else null) orelse Value{ .int64 = 0 };
            }
            const matches = jht.probe(key_buf);
            if (matches.len == 0 and hj.join_type == .left) {
                const combined = try alloc.alloc(?Value, combined_metas.len);
                @memcpy(combined[0..lrow.len], lrow);
                for (combined[lrow.len..]) |*v| v.* = null;
                try sink.append(combined);
                continue;
            }
            for (matches) |ri| {
                const rrow = right_rl.rows.items[ri];
                const combined = try alloc.alloc(?Value, combined_metas.len);
                @memcpy(combined[0..lrow.len], lrow);
                @memcpy(combined[lrow.len..], rrow);
                try sink.append(combined);
            }
        }
    }
    return try sink.finishResult();
}

test "streaming join probe matches row join" {
    const alloc = std.testing.allocator;
    const lmetas = [_]result.ColMeta{
        .{ .name = "id", .col_type = .int64, .ch_type = "Int64" },
        .{ .name = "v", .col_type = .int64, .ch_type = "Int64" },
    };
    var lsink = try chunk_sink_mod.ChunkSink.init(alloc, &lmetas);
    try lsink.append(&.{ .{ .int64 = 1 }, .{ .int64 = 10 } });
    try lsink.append(&.{ .{ .int64 = 2 }, .{ .int64 = 20 } });
    try lsink.append(&.{ .{ .int64 = 3 }, .{ .int64 = 30 } });
    const left = try lsink.finishResult();

    var right = RowBuffer.init(&lmetas);
    try right.append(alloc, try alloc.dupe(?Value, &.{ .{ .int64 = 1 }, .{ .int64 = 100 } }));
    try right.append(alloc, try alloc.dupe(?Value, &.{ .{ .int64 = 3 }, .{ .int64 = 300 } }));
    try right.append(alloc, try alloc.dupe(?Value, &.{ .{ .int64 = 3 }, .{ .int64 = 301 } }));

    var dummy = plan.PhysicalNode{ .mem_scan = .{ .table = "x", .table_ref = 0 } };
    const eq = [_]plan.EquiKey{.{ .left_col_idx = 0, .right_col_idx = 0 }};
    const hj = plan.HashJoinNode{
        .left = &dummy,
        .right = &dummy,
        .join_type = .inner,
        .equi_keys = &eq,
        .filter = null,
    };
    const out = try executeHashJoinProbeChunks(left, right, hj, alloc);
    try std.testing.expectEqual(@as(usize, 3), out.chunks[0].num_rows);
    try std.testing.expectEqual(@as(i64, 1), out.chunks[0].columns[0].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 100), out.chunks[0].columns[3].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 3), out.chunks[0].columns[0].data.int64[1]);
    try std.testing.expectEqual(@as(i64, 300), out.chunks[0].columns[3].data.int64[1]);
    try std.testing.expectEqual(@as(i64, 3), out.chunks[0].columns[0].data.int64[2]);
    try std.testing.expectEqual(@as(i64, 301), out.chunks[0].columns[3].data.int64[2]);
}

fn executeHashJoin(
    left_rl: RowBuffer,
    right_rl: RowBuffer,
    hj: plan.HashJoinNode,
    alloc: std.mem.Allocator,
) !RowBuffer {
    var jht = try ht.JoinHashTable.init(alloc);
    const key_buf = try alloc.alloc(Value, hj.equi_keys.len);

    for (right_rl.rows.items, 0..) |row, ri| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = (if (ek.right_col_idx < row.len) row[ek.right_col_idx] else null) orelse Value{ .int64 = 0 };
        }
        try jht.insert(key_buf, @intCast(ri));
    }

    const combined_metas = try alloc.alloc(result.ColMeta, left_rl.metas.len + right_rl.metas.len);
    @memcpy(combined_metas[0..left_rl.metas.len], left_rl.metas);
    @memcpy(combined_metas[left_rl.metas.len..], right_rl.metas);

    var rl = RowBuffer.init(combined_metas);

    for (left_rl.rows.items) |lrow| {
        for (hj.equi_keys, 0..) |ek, ki| {
            key_buf[ki] = ((if (ek.left_col_idx < lrow.len) lrow[ek.left_col_idx] else null)) orelse Value{ .int64 = 0 };
        }
        const matches = jht.probe(key_buf);

        if (matches.len == 0 and hj.join_type == .left) {
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            for (combined[lrow.len..]) |*vv| vv.* = null;
            try rl.append(alloc, combined);
            continue;
        }

        for (matches) |ri| {
            const rrow = right_rl.rows.items[ri];
            const combined = try alloc.alloc(?Value, combined_metas.len);
            @memcpy(combined[0..lrow.len], lrow);
            @memcpy(combined[lrow.len .. lrow.len + rrow.len], rrow);
            if (hj.filter) |filt| {
                const keep_v = try kernels.evalExpr(filt, combined, null, alloc);
                if (!valueToBool(keep_v)) continue;
            }
            try rl.append(alloc, combined);
        }
    }

    return rl;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "LimitState basic" {
    var b = chunk.ChunkBuilder.init(std.testing.allocator, 5);
    defer b.chunk.deinit();
    const col = try b.addColumn("n", .int64);
    for (0..5) |i| col.data.int64[i] = @intCast(i);
    var c = b.finish();

    var lim = LimitState{ .limit = 3, .offset = 0 };
    lim.apply(&c);
    try std.testing.expectEqual(@as(usize, 3), c.num_rows);
}

// ── Mock source for executePlan tests ─────────────────────────────────────────

const MockSource = struct {
    chunk: DataChunk,
    done: bool = false,

    const metas_storage = [_]result.ColMeta{
        .{ .name = "n", .col_type = .int64 },
    };

    fn nextChunk(ptr: *anyopaque, out: *DataChunk, _ctx: *QueryContext) !bool {
        _ = _ctx;
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        if (self.done) return false;
        self.done = true;
        out.* = self.chunk;
        return true;
    }
    fn reset(ptr: *anyopaque) void {
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        self.done = false;
    }
    fn schema(_ptr: *anyopaque) []const result.ColMeta {
        _ = _ptr;
        return &metas_storage;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunk,
        .reset = reset,
        .schema = schema,
        .rowCount = struct {
            fn f(_: *anyopaque) u64 {
                return 0;
            }
        }.f,
    };

    fn iface(self: *MockSource) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

test "ScanRequest keeps more than 32 referenced columns" {
    const WideSource = struct {
        metas: [64]result.ColMeta = undefined,
        selected_count: ?usize = null,

        fn nextChunk(_: *anyopaque, _: *DataChunk, _: *QueryContext) !bool {
            return false;
        }

        fn reset(_: *anyopaque) void {}

        fn schema(ptr: *anyopaque) []const result.ColMeta {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return &self.metas;
        }

        fn rowCount(_: *anyopaque) u64 {
            return 0;
        }

        fn setNeededCols(ptr: *anyopaque, names: ?[]const []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.selected_count = if (names) |cols| cols.len else null;
        }

        const vtable = SourceIface.VTable{
            .nextChunk = nextChunk,
            .reset = reset,
            .schema = schema,
            .rowCount = rowCount,
            .setNeededCols = setNeededCols,
        };
    };

    var source: WideSource = .{};
    for (&source.metas) |*meta| meta.* = .{ .name = "c", .col_type = .int64 };

    var request: ScanRequest = .{};
    for (0..40) |i| request.addIndex(i);
    const scope = request.apply(.{ .ptr = &source, .vtable = &WideSource.vtable }, .any_reduction);
    try std.testing.expectEqual(@as(?usize, 40), source.selected_count);
    scope.deinit();
    try std.testing.expectEqual(@as(?usize, null), source.selected_count);
}

test "executePlan: scalar_agg count(*)" {
    const alloc = std.testing.allocator;

    // Build a chunk with 4 rows, column "n" Int64: [1, 2, 3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 4);
    const ci = try b.addColumn("n", .int64);
    for (0..4) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Plan: scalar_agg [ count(*) ]
    var agg_call = plan.AggCall{ .kind = .count_star, .arg = null, .distinct = false };
    const agg_items = [_]plan.ProjectItem{.{
        .expr = .{ .agg_call = &agg_call },
        .alias = "count()",
        .out_type = .uint64,
    }};
    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const agg_node = plan.PhysicalNode{ .scalar_agg = .{ .input = @constCast(&scan_node), .aggs = &agg_items } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&agg_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 1), rs.num_rows);
    try std.testing.expectEqual(@as(usize, 1), rs.metas.len);
    const v = rs.get(0, 0).?;
    try std.testing.expectEqual(Value{ .uint64 = 4 }, v);
}

test "executePlan: filter + limit" {
    const alloc = std.testing.allocator;

    // 5 rows: [1..5], keep n > 2, limit 2 → [3, 4]
    var b = chunk.ChunkBuilder.init(alloc, 5);
    const ci = try b.addColumn("n", .int64);
    for (0..5) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    // Filter: n > 2   (col_ref index=0 > lit_i64 2)
    var gt_binop = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 2 },
    };
    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const filter_node = plan.PhysicalNode{ .filter = .{ .input = @constCast(&scan_node), .predicate = .{ .gt = &gt_binop } } };
    const limit_node = plan.PhysicalNode{ .limit = .{ .input = @constCast(&filter_node), .limit = 2, .offset = 0 } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&limit_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(Value{ .int64 = 3 }, rs.get(0, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 4 }, rs.get(0, 1).?);
}

test "executePlan: batch filter + project" {
    const alloc = std.testing.allocator;

    var b = chunk.ChunkBuilder.init(alloc, 5);
    const ci = try b.addColumn("n", .int64);
    for (0..5) |i| b.chunk.columns[ci].data.int64[i] = @intCast(i + 1);
    const mock_chunk = b.finish();

    var src = MockSource{ .chunk = mock_chunk };

    var gt_binop = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 2 },
    };
    var add_binop = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 10 },
    };
    var project_items = [_]plan.ProjectItem{.{
        .expr = .{ .add = &add_binop },
        .alias = "n10",
        .out_type = .int64,
    }};

    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "t", .columns = &.{}, .filter = null } };
    const filter_node = plan.PhysicalNode{ .filter = .{ .input = @constCast(&scan_node), .predicate = .{ .gt = &gt_binop } } };
    const project_node = plan.PhysicalNode{ .project = .{ .input = @constCast(&filter_node), .items = project_items[0..] } };

    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var rs = try executePlan(&project_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 3), rs.num_rows);
    try std.testing.expectEqual(Value{ .int64 = 13 }, rs.get(0, 0).?);
    try std.testing.expectEqual(Value{ .int64 = 14 }, rs.get(0, 1).?);
    try std.testing.expectEqual(Value{ .int64 = 15 }, rs.get(0, 2).?);
}

test "executePlan: parallel filter project keeps zero-copy input immutable" {
    const alloc = std.testing.allocator;
    const row_count = 2_000_000;
    const target_row = 1000;

    const RangeSource = struct {
        values: []i64,

        const metas = [_]result.ColMeta{
            .{ .name = "UserID", .col_type = .int64 },
        };

        fn nextChunk(_: *anyopaque, _: *DataChunk, _: *QueryContext) !bool {
            return false;
        }

        fn reset(_: *anyopaque) void {}

        fn schema(_: *anyopaque) []const result.ColMeta {
            return &metas;
        }

        fn rowCount(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.values.len;
        }

        fn fetchRange(
            ptr: *anyopaque,
            start: u64,
            n: usize,
            out: *DataChunk,
            range_alloc: std.mem.Allocator,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const begin: usize = @intCast(start);
            const end = @min(begin + n, self.values.len);
            const actual = end - begin;
            const columns = try range_alloc.alloc(chunk.Column, 1);
            const null_mask = try range_alloc.alloc(u64, chunk.nullMaskWords(actual));
            @memset(null_mask, 0);
            columns[0] = .{
                .name = "UserID",
                .data = .{ .int64 = self.values[begin..end] },
                .null_mask = null_mask,
                .len = actual,
            };
            out.* = .{
                .columns = columns,
                .num_rows = actual,
                .arena = std.heap.ArenaAllocator.init(range_alloc),
            };
        }

        fn setNeededCols(_: *anyopaque, _: ?[]const []const u8) void {}

        const vtable = SourceIface.VTable{
            .nextChunk = nextChunk,
            .reset = reset,
            .schema = schema,
            .rowCount = rowCount,
            .fetchRange = fetchRange,
            .setNeededCols = setNeededCols,
        };

        fn iface(self: *@This()) SourceIface {
            return .{ .ptr = self, .vtable = &vtable };
        }
    };

    const values = try alloc.alloc(i64, row_count);
    defer alloc.free(values);
    for (values, 0..) |*value, i| value.* = @intCast(i);
    var source = RangeSource{ .values = values };

    var eq = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "UserID" } },
        .right = .{ .lit_i64 = target_row },
    };
    var items = [_]plan.ProjectItem{.{
        .expr = .{ .col_ref = .{ .index = 0, .name = "UserID" } },
        .alias = "UserID",
        .out_type = .int64,
    }};
    const scan_node = plan.PhysicalNode{ .part_scan = .{ .db = "db", .table = "hits", .columns = &.{}, .filter = null } };
    const filter_node = plan.PhysicalNode{ .filter = .{
        .input = @constCast(&scan_node),
        .predicate = .{ .eq = &eq },
    } };
    const project_node = plan.PhysicalNode{ .project = .{
        .input = @constCast(&filter_node),
        .items = &items,
    } };

    var ctx = QueryContext.init(alloc, source.iface());
    defer ctx.deinit();
    var rs = try executePlan(&project_node, &ctx);
    defer rs.deinit();

    try std.testing.expectEqual(@as(usize, 1), rs.num_rows);
    try std.testing.expectEqual(Value{ .int64 = target_row }, rs.get(0, 0).?);
    try std.testing.expectEqual(@as(i64, 0), values[0]);
}

test "ProjectState applySelected projects logical rows" {
    const alloc = std.testing.allocator;

    var b = chunk.ChunkBuilder.init(alloc, 4);
    defer b.chunk.deinit();
    const ci = try b.addColumn("n", .int64);
    for (0..4) |i| b.chunk.columns[ci].data.int64[i] = @intCast((i + 1) * 10);

    var add_op = plan.BinOp{
        .left = .{ .col_ref = .{ .index = 0, .name = "n" } },
        .right = .{ .lit_i64 = 1 },
    };
    var items = [_]plan.ProjectItem{.{
        .expr = .{ .add = &add_op },
        .alias = "n1",
        .out_type = .int64,
    }};
    var project = ProjectState{ .items = items[0..] };

    var sel_buf: [4]u32 = undefined;
    const mask = [_]i16{ 0, 1, 0, 1 };
    const sel = chunk.SelectionVector.fromI16Mask(&sel_buf, &mask);
    const selected = b.chunk.selected(sel);

    var src = MockSource{ .chunk = b.chunk };
    var ctx = QueryContext.init(alloc, src.iface());
    defer ctx.deinit();

    var out = chunk.DataChunk{
        .columns = &.{},
        .num_rows = 0,
        .arena = std.heap.ArenaAllocator.init(alloc),
    };
    defer out.deinit();

    try project.applySelected(selected, &out, &ctx);
    try std.testing.expectEqual(@as(usize, 2), out.num_rows);
    try std.testing.expectEqual(@as(usize, 1), out.columns.len);
    try std.testing.expectEqual(@as(i64, 21), out.columns[0].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 41), out.columns[0].data.int64[1]);
    try std.testing.expectEqual(@as(i64, 10), b.chunk.columns[ci].data.int64[0]);
}

// ── Two-phase scatter → aggregate ────────────────────────────────────────────
//
// Avoids per-thread HT exceeding L3 cache for high-cardinality GROUP BY.
//
// Phase 1 (parallel scatter):
//   Each thread scans its morsels and scatters (hash, k0[, k1], agg_partial...)
//   into N_PARTS=128 per-thread partition ArrayLists.  No HT touched — pure
//   sequential writes to hot (cache-resident) partition buffers.
//
// Phase 2 (parallel aggregate):
//   Thread t owns partitions [t*32 .. (t+1)*32).  For each partition p, it
//   collects all thread scatter bufs for p and aggregates with a small HT
//   (~1M/128 = 8K expected entries → fits in L2 cache).
//
// Conditions: 1-2 plain col_ref integer keys, count/i64_sum/f64_sum aggs.
// str_filter: optional single col_ref eq/neq lit_str pre-filter applied inline in scatter phase.

// ─────────────────────────────────────────────────────────────────────────────
// Two-phase scatter → aggregate for pure-string-key GROUP BY
// (Q34/Q35: URL GROUP BY 2.6M unique, Q13: SearchPhrase GROUP BY with ≠'' filter)
//
// Phase 1 (parallel scatter):
//   Each thread iterates its morsels and writes
//   [hash:u64, str_ptr:u64, str_len:u64, agg0:u64, ...] records
//   into N_PARTS=64 per-thread partition buffers (hot sequential writes).
//
// Phase 2 (parallel aggregate per partition):
//   Thread t picks partitions from morsel_src2.  For each partition p it
//   pre-sizes a private StrAggHashTable then sweeps all threads' scatter
//   buffers for p.  Working set per partition ≈ 40K entries → fits L3.
//
// Restrictions (caller already checked):
//   • Exactly one col_ref string key at key_col_idx / str_key_pos.
//   • All other GROUP BY keys are lit_i64 constants.
//   • No str_min/str_max aggs, no COUNT DISTINCT, no CASE-WHEN/regexp key.
//   • filter: null or a single SimpleStrFilter (col_ref eq/neq lit_str).
// ─────────────────────────────────────────────────────────────────────────────
