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

            // Initialize the local HT only for the fetchRange fallback.  The
            // two-phase raw path below has its own partition tables.
            self.local_ht = try ht.PairCountHashTable.initWithCapacity(self.ht_alloc, self.est_per_thread_);

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
            const exp: usize = total_rows / @max(n_threads, 1) / N_P2 + 64;
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
    const n_parts: usize = blk: {
        var p: usize = 64;
        const target = total_rows / 160_000;
        while (p < target and p < 2048) p <<= 1;
        break :blk p;
    };

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
        part_bufs: []std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot), // len = n_parts
        n_parts: usize,
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
            const part_mask: u64 = self.n_parts - 1;
            const reserve_per_part = (self.local_ht.count / self.n_parts) * 3 / 2 + 32;
            for (self.part_bufs) |*b| b.ensureTotalCapacity(self.scatter_alloc, reserve_per_part) catch {};
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
        pb.* = try ht_arenas[t].allocator().alloc(std.ArrayListUnmanaged(ht.TripleCountHashTable.Slot), n_parts);
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
            .n_parts = n_parts,
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
    const part_arenas_triple = try alloc.alloc(std.heap.ArenaAllocator, n_parts);
    for (part_arenas_triple) |*a| a.* = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer for (part_arenas_triple) |*a| a.deinit();
    const part_hts_triple = try alloc.alloc(ht.TripleCountHashTable, n_parts);

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
    var part_agg_src = parallel.MorselSource.init(n_parts, 1);
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
    for (0..n_parts) |p| {
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
