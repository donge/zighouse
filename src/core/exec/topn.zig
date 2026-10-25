//! Top-N selection: total-order comparator, bounded collectors, and late heap.
const std = @import("std");
const plan = @import("plan.zig");
const result = @import("../result.zig");
const types = @import("../types.zig");
const chunk = @import("../chunk.zig");
const row_buffer = @import("row_buffer.zig");
const chunk_sink = @import("chunk_sink.zig");
const chunk_sink_mod = chunk_sink;

pub const Value = types.Value;
pub const DataChunk = chunk.DataChunk;
pub const RowBuffer = row_buffer.RowBuffer;
pub const ChunkResult = chunk_sink.ChunkSink.Result;

fn dupeOwnedValue(alloc: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array_string => |arr| blk: {
            const out = try alloc.alloc([]const u8, arr.len);
            for (arr, 0..) |s, i| out[i] = try alloc.dupe(u8, s);
            break :blk .{ .array_string = out };
        },
        else => v,
    };
}

fn materializeChunkRowOwned(c: *const DataChunk, row_idx: usize, alloc: std.mem.Allocator) ![]?Value {
    const row = try alloc.alloc(?Value, c.columns.len);
    for (c.columns, 0..) |col, i| {
        row[i] = if (col.isRowNull(row_idx)) null else try dupeOwnedValue(alloc, col.data.get(row_idx));
    }
    return row;
}

pub fn rowLessThan(keys: []const plan.SortKey, a: []?Value, b: []?Value) bool {
    for (keys) |key| {
        const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
        const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
        const ord: std.math.Order = if (av != null and bv != null)
            Value.order(av.?, bv.?)
        else if (av == null and bv == null)
            .eq
        else if (av == null)
            .lt
        else
            .gt;
        if (ord == .eq) continue;
        return if (key.desc) ord == .gt else ord == .lt;
    }
    // Stable tiebreaker: full lexicographic comparison on all row values.
    for (0..@min(a.len, b.len)) |ci| {
        const av2 = a[ci];
        const bv2 = b[ci];
        const ord2: std.math.Order = if (av2 != null and bv2 != null)
            Value.order(av2.?, bv2.?)
        else if (av2 == null and bv2 == null) .eq else if (av2 == null) .lt else .gt;
        if (ord2 == .eq) continue;
        return ord2 == .lt;
    }
    return a.len < b.len;
}

pub const TopKSortCtx = struct {
    keys: []const plan.SortKey,
    pub fn lessThan(self: @This(), a: []?Value, b: []?Value) bool {
        return rowLessThan(self.keys, a, b);
    }
};

/// Streaming top-K sink. Keeps only the best K rows in a worst-at-root heap,
/// so hash aggregation can avoid materialising every group into a RowBuffer.
pub const TopKCollector = struct {
    alloc: std.mem.Allocator,
    keys: []const plan.SortKey,
    k: usize,
    heap: [][]?Value,
    len: usize = 0,
    count_col: ?usize = null,

    pub fn init(alloc: std.mem.Allocator, keys: []const plan.SortKey, k: usize) !TopKCollector {
        var count_col: ?usize = null;
        if (keys.len == 1 and keys[0].desc) count_col = keys[0].col_idx;
        return .{
            .alloc = alloc,
            .keys = keys,
            .k = k,
            .heap = try alloc.alloc([]?Value, k),
            .count_col = count_col,
        };
    }

    /// Compare only the sort-key columns. Returns null when the keys are equal,
    /// in which case the caller must materialize the row for the full tiebreak.
    pub fn betterThanKeys(keys: []const plan.SortKey, a: []const ?Value, b: []const ?Value) ?bool {
        for (keys) |key| {
            const av: ?Value = if (key.col_idx < a.len) a[key.col_idx] else null;
            const bv: ?Value = if (key.col_idx < b.len) b[key.col_idx] else null;
            const ord: std.math.Order = if (av != null and bv != null)
                Value.order(av.?, bv.?)
            else if (av == null and bv == null) .eq else if (av == null) .lt else .gt;
            if (ord == .eq) continue;
            return if (key.desc) ord == .gt else ord == .lt;
        }
        return null;
    }

    /// Decide from key columns only whether a candidate can enter the heap.
    pub fn shouldOfferKeys(self: *const TopKCollector, key_row: []const ?Value) bool {
        if (self.k == 0) return false;
        if (self.len < self.k) return true;
        return betterThanKeys(self.keys, key_row, self.heap[0]) orelse true;
    }

    /// Fast reject for `ORDER BY count DESC LIMIT k`: if the candidate count is
    /// below the current worst, skip building/duplicating the row entirely.
    pub fn shouldSkipCount(self: *const TopKCollector, count: u64) bool {
        if (self.len < self.k) return false;
        const col = self.count_col orelse return false;
        const root = self.heap[0];
        if (col >= root.len) return false;
        const rv = root[col] orelse return false;
        const root_count: u64 = switch (rv) {
            .uint64 => |v| v,
            .int64 => |v| if (v < 0) 0 else @intCast(v),
            else => return false,
        };
        return count < root_count;
    }

    pub fn offer(self: *TopKCollector, row: []?Value) void {
        if (self.k == 0) return;
        if (self.len < self.k) {
            self.heap[self.len] = row;
            self.len += 1;
            var i = self.len - 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (rowLessThan(self.keys, self.heap[parent], self.heap[i])) {
                    const tmp = self.heap[i];
                    self.heap[i] = self.heap[parent];
                    self.heap[parent] = tmp;
                    i = parent;
                } else break;
            }
        } else if (rowLessThan(self.keys, row, self.heap[0])) {
            self.heap[0] = row;
            var i: usize = 0;
            while (true) {
                const l = 2 * i + 1;
                const r = 2 * i + 2;
                var smallest = i;
                if (l < self.len and rowLessThan(self.keys, self.heap[smallest], self.heap[l])) smallest = l;
                if (r < self.len and rowLessThan(self.keys, self.heap[smallest], self.heap[r])) smallest = r;
                if (smallest == i) break;
                const tmp = self.heap[i];
                self.heap[i] = self.heap[smallest];
                self.heap[smallest] = tmp;
                i = smallest;
            }
        }
    }

    pub fn finish(self: *TopKCollector, metas: []result.ColMeta) !RowBuffer {
        std.sort.pdq([]?Value, self.heap[0..self.len], TopKSortCtx{ .keys = self.keys }, TopKSortCtx.lessThan);
        var rl = RowBuffer.init(metas);
        for (self.heap[0..self.len]) |row| try rl.append(self.alloc, row);
        return rl;
    }
};

test "TopKCollector keeps best k by count desc" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const keys = [_]plan.SortKey{.{ .col_idx = 0, .desc = true, .nulls_first = false }};
    var c = try TopKCollector.init(a, &keys, 3);
    for ([_]u64{ 5, 1, 9, 3, 7 }) |v| {
        const row = try a.alloc(?Value, 1);
        row[0] = .{ .uint64 = v };
        c.offer(row);
    }
    const metas = [_]result.ColMeta{.{ .name = "c", .col_type = .uint64, .ch_type = "UInt64" }};
    const rl = try c.finish(&metas);
    try std.testing.expectEqual(@as(usize, 3), rl.rows.items.len);
    try std.testing.expectEqual(@as(u64, 9), rl.rows.items[0][0].?.uint64);
    try std.testing.expectEqual(@as(u64, 7), rl.rows.items[1][0].?.uint64);
    try std.testing.expectEqual(@as(u64, 5), rl.rows.items[2][0].?.uint64);
}

/// Chunk/row sink built on the bounded TopKCollector.  Producers can push
/// chunks without materialising every accepted row; rejected rows are compared
/// only through their sort-key columns.
pub const TopNSink = struct {
    alloc: std.mem.Allocator,
    collector: TopKCollector,
    metas: []result.ColMeta,
    key_buf: []?Value,

    pub fn init(
        alloc: std.mem.Allocator,
        metas: []const result.ColMeta,
        keys: []const plan.SortKey,
        k: usize,
    ) !TopNSink {
        const owned_metas = try alloc.dupe(result.ColMeta, metas);
        errdefer alloc.free(owned_metas);
        return .{
            .alloc = alloc,
            .collector = try TopKCollector.init(alloc, keys, k),
            .metas = owned_metas,
            .key_buf = try alloc.alloc(?Value, metas.len),
        };
    }

    pub fn pushRow(self: *TopNSink, row: []?Value) void {
        self.collector.offer(row);
    }

    pub fn pushChunk(self: *TopNSink, c: *const DataChunk) !void {
        @memset(self.key_buf, null);
        for (0..c.num_rows) |r| {
            for (self.collector.keys) |key| {
                if (key.col_idx >= c.columns.len) continue;
                const col = &c.columns[key.col_idx];
                self.key_buf[key.col_idx] = if (col.isRowNull(r)) null else col.data.get(r);
            }
            if (!self.collector.shouldOfferKeys(self.key_buf)) continue;
            self.collector.offer(try materializeChunkRowOwned(c, r, self.alloc));
        }
    }

    pub fn finish(self: *TopNSink) !RowBuffer {
        return self.collector.finish(self.metas);
    }
};

pub fn executeTopK(inner: RowBuffer, keys: []const plan.SortKey, k: usize, alloc: std.mem.Allocator) !RowBuffer {
    const rows = inner.rows.items;
    if (rows.len == 0 or k == 0) return RowBuffer.init(inner.metas);

    var sink = try TopNSink.init(alloc, inner.metas, keys, k);
    for (rows) |row| sink.pushRow(row);
    return sink.finish();
}

/// Entry used by the late-materialization top-N path. `key_vals` is compact
/// (one slot per sort key) and empty for the raw integer/string fast path.
pub const HeapEntry = struct {
    global_row: u64,
    key_vals: []?Value,
    sort_key_i64: i64 = 0,
    sort_key_str: []const u8 = &.{},
};

pub fn lateLess(keys: []const plan.SortKey, a: HeapEntry, b: HeapEntry) bool {
    if (a.key_vals.len == 0 and b.key_vals.len == 0) {
        const ord = std.math.order(a.sort_key_i64, b.sort_key_i64);
        if (ord != .eq) {
            const desc = keys.len > 0 and keys[0].desc;
            return if (desc) ord == .gt else ord == .lt;
        }
        if (keys.len > 1) {
            const s_ord = std.mem.order(u8, a.sort_key_str, b.sort_key_str);
            if (s_ord != .eq) return if (keys[1].desc) s_ord == .gt else s_ord == .lt;
        }
        return a.global_row < b.global_row;
    }
    for (keys, 0..) |key, ki| {
        const av = if (ki < a.key_vals.len) a.key_vals[ki] else null;
        const bv = if (ki < b.key_vals.len) b.key_vals[ki] else null;
        const ord: std.math.Order = if (av != null and bv != null)
            Value.order(av.?, bv.?)
        else if (av == null and bv == null) .eq else if (av == null) .lt else .gt;
        if (ord == .eq) continue;
        return if (key.desc) ord == .gt else ord == .lt;
    }
    return a.global_row < b.global_row;
}

pub const LateSortCtx = struct {
    keys: []const plan.SortKey,
    pub fn lessThan(self: @This(), a: HeapEntry, b: HeapEntry) bool {
        return lateLess(self.keys, a, b);
    }
};

/// Bounded worst-at-root heap over HeapEntry values. Used by both the
/// per-worker parallel phase and the sequential late-materialization scan.
pub const LateTopNHeap = struct {
    alloc: std.mem.Allocator,
    keys: []const plan.SortKey,
    k: usize,
    heap: []HeapEntry,
    len: usize = 0,

    pub fn init(alloc: std.mem.Allocator, keys: []const plan.SortKey, k: usize) !LateTopNHeap {
        return .{ .alloc = alloc, .keys = keys, .k = k, .heap = try alloc.alloc(HeapEntry, k) };
    }

    pub fn offer(self: *LateTopNHeap, entry: HeapEntry) void {
        if (self.k == 0) return;
        if (self.len < self.k) {
            self.heap[self.len] = entry;
            self.len += 1;
            var i = self.len - 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (lateLess(self.keys, self.heap[parent], self.heap[i])) {
                    const tmp = self.heap[i];
                    self.heap[i] = self.heap[parent];
                    self.heap[parent] = tmp;
                    i = parent;
                } else break;
            }
        } else if (lateLess(self.keys, entry, self.heap[0])) {
            self.heap[0] = entry;
            self.siftDown(0);
        }
    }

    pub fn offerKeys(self: *LateTopNHeap, global_row: u64, key_vals: []const ?Value) !void {
        const candidate = HeapEntry{ .global_row = global_row, .key_vals = @constCast(key_vals) };
        if (self.len >= self.k and !lateLess(self.keys, candidate, self.heap[0])) return;
        const owned = try self.alloc.alloc(?Value, self.keys.len);
        @memcpy(owned, key_vals[0..self.keys.len]);
        self.offer(.{ .global_row = global_row, .key_vals = owned });
    }

    pub fn offerRaw(self: *LateTopNHeap, global_row: u64, sort_i64: i64, sort_str: []const u8) void {
        self.offer(.{ .global_row = global_row, .key_vals = &.{}, .sort_key_i64 = sort_i64, .sort_key_str = sort_str });
    }

    pub fn siftDown(self: *LateTopNHeap, start: usize) void {
        var i = start;
        while (true) {
            const l = 2 * i + 1;
            const r = 2 * i + 2;
            var worst = i;
            if (l < self.len and lateLess(self.keys, self.heap[worst], self.heap[l])) worst = l;
            if (r < self.len and lateLess(self.keys, self.heap[worst], self.heap[r])) worst = r;
            if (worst == i) break;
            const tmp = self.heap[i];
            self.heap[i] = self.heap[worst];
            self.heap[worst] = tmp;
            i = worst;
        }
    }

    pub fn finish(self: *LateTopNHeap) []HeapEntry {
        std.sort.pdq(HeapEntry, self.heap[0..self.len], LateSortCtx{ .keys = self.keys }, LateSortCtx.lessThan);
        return self.heap[0..self.len];
    }

    pub fn copyTo(self: *LateTopNHeap, alloc: std.mem.Allocator) ![]HeapEntry {
        const out = try alloc.alloc(HeapEntry, self.len);
        for (out, 0..) |*dst, i| {
            dst.* = self.heap[i];
            if (self.heap[i].key_vals.len > 0) {
                const kv = try alloc.alloc(?Value, self.heap[i].key_vals.len);
                @memcpy(kv, self.heap[i].key_vals);
                dst.key_vals = kv;
            }
        }
        return out;
    }
};

pub fn executeTopKFromChunkResult(
    res: chunk_sink_mod.ChunkSink.Result,
    keys: []const plan.SortKey,
    k: usize,
    alloc: std.mem.Allocator,
) !RowBuffer {
    var sink = try TopNSink.init(alloc, res.metas, keys, k);
    for (res.chunks) |*c| try sink.pushChunk(c);
    return sink.finish();
}
