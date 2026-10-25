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
const hashmap = @import("hashmap");
const topk_mod = @import("agg_hash_compact_topk.zig");
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
            const reserve_mul: usize = if (total_rows > 20_000_000) 1 else 4;
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
                                const slot_vals = try self.part_hts[p].getOrInsertPreallocH(key_buf[0..self.n_eff_keys], h, self.compact_init_vals);
                                slot_vals[0] += 1;
                            }
                        } else {
                            while (i < buf.len) : (i += rs) {
                                const h = buf[i];
                                for (0..self.n_eff_keys) |ki| key_buf[ki] = @bitCast(buf[i + 1 + ki]);
                                const partial = buf[i + 1 + self.n_eff_keys .. i + rs];
                                const slot_vals = try self.part_hts[p].getOrInsertPreallocH(key_buf[0..self.n_eff_keys], h, self.compact_init_vals);
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
