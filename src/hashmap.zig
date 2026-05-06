//! Open-addressing hash map specialized for u64 keys -> u32 counts.
//!
//! Designed for the ClickBench groupby workload: very large insertion volume
//! (up to ~100M), key cardinality typically 1k-50M, value is a per-group
//! counter. Uses linear probing on a power-of-two-sized table with an empty
//! sentinel of all-ones (any well-distributed packed key avoiding 0xFF...FF
//! works; the caller is responsible). For ClickBench we pack ids bit-shifted
//! into u64 — they never reach all-ones.
//!
//! Compared with `std.AutoHashMap(u64, u32)`:
//!   - No metadata array; one cache line per probe instead of two.
//!   - No separate growth threshold checks: caller pre-sizes via `init`.
//!   - Bumping a counter is one branch (sentinel check) + one store.

const std = @import("std");

pub const empty_key: u64 = 0xffff_ffff_ffff_ffff;

pub const HashU64Count = struct {
    keys: []u64,
    values: []u32,
    capacity: usize, // power of two
    mask: u64,
    len: usize,
    allocator: std.mem.Allocator,

    /// Initialise to hold at least `expected_keys` entries with load factor
    /// <= 0.5 (capacity = next power of two >= expected_keys * 2).
    pub fn init(allocator: std.mem.Allocator, expected_keys: usize) !HashU64Count {
        var cap: usize = 16;
        const target = std.math.mul(usize, expected_keys, 2) catch return error.OutOfMemory;
        while (cap < target) cap *= 2;
        const keys = try allocator.alloc(u64, cap);
        @memset(keys, empty_key);
        const values = try allocator.alloc(u32, cap);
        @memset(values, 0);
        return .{
            .keys = keys,
            .values = values,
            .capacity = cap,
            .mask = @as(u64, cap - 1),
            .len = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *HashU64Count) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
        self.* = undefined;
    }

    /// Hash function: a single multiply + xor (splittable PRNG style). Good
    /// avalanche for u64 keys constructed by bit-packing several smaller ids.
    inline fn hash(key: u64) u64 {
        var x = key ^ (key >> 30);
        x = x *% 0xbf58_476d_1ce4_e5b9;
        x ^= x >> 27;
        x = x *% 0x94d0_49bb_1331_11eb;
        x ^= x >> 31;
        return x;
    }

    /// Increment counter for `key`, inserting if absent. Caller must guarantee
    /// load factor stays <= ~0.85 (otherwise probing degrades).
    pub inline fn bump(self: *HashU64Count, key: u64) void {
        std.debug.assert(key != empty_key);
        var idx = hash(key) & self.mask;
        while (true) {
            const slot = self.keys[idx];
            if (slot == key) {
                self.values[idx] += 1;
                return;
            }
            if (slot == empty_key) {
                self.keys[idx] = key;
                self.values[idx] = 1;
                self.len += 1;
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }

    /// Batched bump with software prefetch pipelining. Processes `keys_in` in
    /// groups of `batch_size`: hashes all, prefetches all probe slots, then
    /// performs the actual lookup/insert. Intent is to overlap memory
    /// latency with hash computation (ARM `prfm` or x86 `prefetcht0`).
    ///
    /// `batch_size` is comptime so the inner array lives on the stack.
    pub fn bumpBatched(self: *HashU64Count, keys_in: []const u64, comptime batch_size: usize) void {
        var i: usize = 0;
        while (i + batch_size <= keys_in.len) : (i += batch_size) {
            var probe_idx: [batch_size]u64 = undefined;
            inline for (0..batch_size) |j| {
                const k = keys_in[i + j];
                probe_idx[j] = hash(k) & self.mask;
                @prefetch(&self.keys[probe_idx[j]], .{ .rw = .read, .locality = 0, .cache = .data });
            }
            inline for (0..batch_size) |j| {
                const k = keys_in[i + j];
                var idx = probe_idx[j];
                while (true) {
                    const slot = self.keys[idx];
                    if (slot == k) {
                        self.values[idx] += 1;
                        break;
                    }
                    if (slot == empty_key) {
                        self.keys[idx] = k;
                        self.values[idx] = 1;
                        self.len += 1;
                        break;
                    }
                    idx = (idx + 1) & self.mask;
                }
            }
        }
        while (i < keys_in.len) : (i += 1) self.bump(keys_in[i]);
    }

    /// Iterator over occupied (key, value) pairs.
    pub const Iterator = struct {
        map: *const HashU64Count,
        idx: usize,
        pub fn next(self: *Iterator) ?struct { key: u64, value: u32 } {
            while (self.idx < self.map.capacity) : (self.idx += 1) {
                if (self.map.keys[self.idx] != empty_key) {
                    const k = self.map.keys[self.idx];
                    const v = self.map.values[self.idx];
                    self.idx += 1;
                    return .{ .key = k, .value = v };
                }
            }
            return null;
        }
    };

    pub fn iterator(self: *const HashU64Count) Iterator {
        return .{ .map = self, .idx = 0 };
    }
};

/// Radix-partitioned hash table. Each insert is routed to one of
/// `partition_count` independent `HashU64Count` sub-tables based on the
/// top bits of the well-mixed hash. Designed for the parallel-aggregation
/// pattern (DuckDB-style):
///
///   1. Each worker thread owns its own `PartitionedHashU64Count` and
///      calls `bump()` on its row morsel.
///   2. After all workers finish, the merge phase walks one partition at
///      a time; partition P from each worker is merged into a single
///      output `HashU64Count` (no cross-partition contention because
///      keys with the same hash always fall in the same partition).
///
/// `partition_bits = 6` -> 64 partitions; chosen so that for a 9M-row
/// final output, each partition holds ~140k entries (~1 MB), which fits
/// comfortably in L2 on Apple Silicon (12-16 MB per cluster).
pub const partition_bits: u6 = 6;
pub const partition_count: usize = 1 << @as(u6, partition_bits);
const partition_shift: u6 = 58; // 64 - partition_bits
const partition_mask: u64 = partition_count - 1;

/// Map a key to its partition. Use the same hash function as the
/// underlying `HashU64Count` so that downstream merges can avoid
/// re-hashing if they want; we just take the top bits.
pub inline fn partitionFor(key: u64) usize {
    // Re-derive the hash using the same constants as HashU64Count.hash
    // to keep partition assignment consistent if the merge phase ever
    // wants to reuse the value.
    var x = key ^ (key >> 30);
    x = x *% 0xbf58_476d_1ce4_e5b9;
    x ^= x >> 27;
    x = x *% 0x94d0_49bb_1331_11eb;
    x ^= x >> 31;
    return @as(usize, @intCast(x >> partition_shift));
}

pub const PartitionedHashU64Count = struct {
    parts: [partition_count]HashU64Count,
    allocator: std.mem.Allocator,

    /// `expected_total_keys` is a global-cardinality estimate; we divide
    /// by `partition_count` and add 25% slack to absorb hash skew.
    pub fn init(allocator: std.mem.Allocator, expected_total_keys: usize) !PartitionedHashU64Count {
        const per_part = (expected_total_keys / partition_count) + (expected_total_keys / partition_count / 4) + 16;
        var self: PartitionedHashU64Count = .{
            .parts = undefined,
            .allocator = allocator,
        };
        var initialised: usize = 0;
        errdefer {
            for (self.parts[0..initialised]) |*p| p.deinit();
        }
        for (&self.parts) |*p| {
            p.* = try HashU64Count.init(allocator, per_part);
            initialised += 1;
        }
        return self;
    }

    pub fn deinit(self: *PartitionedHashU64Count) void {
        for (&self.parts) |*p| p.deinit();
        self.* = undefined;
    }

    pub inline fn bump(self: *PartitionedHashU64Count, key: u64) void {
        const p = partitionFor(key);
        self.parts[p].bump(key);
    }

    /// Total occupancy across all partitions.
    pub fn len(self: *const PartitionedHashU64Count) usize {
        var n: usize = 0;
        for (&self.parts) |*p| n += p.len;
        return n;
    }
};

/// Merge partition `p` from every per-thread `PartitionedHashU64Count`
/// into a single fresh `HashU64Count`. Caller owns the result.
///
/// `expected_p_keys` is a hint for the output table size; pass an
/// over-estimate (e.g. sum of `parts[p].len` across threads) to avoid
/// re-growth.
pub fn mergePartition(
    allocator: std.mem.Allocator,
    locals: []*PartitionedHashU64Count,
    p: usize,
    expected_p_keys: usize,
) !HashU64Count {
    var out = try HashU64Count.init(allocator, expected_p_keys);
    errdefer out.deinit();
    for (locals) |local| {
        const src = &local.parts[p];
        var i: usize = 0;
        while (i < src.capacity) : (i += 1) {
            const k = src.keys[i];
            if (k == empty_key) continue;
            const v = src.values[i];
            // Inline a "bumpBy" so we don't have to add it to the public
            // API just for merge: linear-probe insert with +v on hit.
            var idx = blk: {
                var x = k ^ (k >> 30);
                x = x *% 0xbf58_476d_1ce4_e5b9;
                x ^= x >> 27;
                x = x *% 0x94d0_49bb_1331_11eb;
                x ^= x >> 31;
                break :blk x & out.mask;
            };
            while (true) {
                const slot = out.keys[idx];
                if (slot == k) {
                    out.values[idx] += v;
                    break;
                }
                if (slot == empty_key) {
                    out.keys[idx] = k;
                    out.values[idx] = v;
                    out.len += 1;
                    break;
                }
                idx = (idx + 1) & out.mask;
            }
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// HashU64Tuple3Count: same open-addressing structure as HashU64Count but each
// slot stores three aggregates: count u32, sum_a u32, sum_b u64. Designed for
// queries like Q31/Q32: GROUP BY ... aggregating COUNT(*), SUM(IsRefresh),
// SUM(ResolutionWidth) (avg = sum_b / count). Keeping this as a separate type
// avoids paying the 16-byte slot cost in queries that only need a count.
// ---------------------------------------------------------------------------

pub const HashU64Tuple3Count = struct {
    keys: []u64,
    counts: []u32,
    sum_a: []u32,
    sum_b: []u64,
    capacity: usize,
    mask: u64,
    len: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, expected_keys: usize) !HashU64Tuple3Count {
        var cap: usize = 16;
        const target = std.math.mul(usize, expected_keys, 2) catch return error.OutOfMemory;
        while (cap < target) cap *= 2;
        const keys = try allocator.alloc(u64, cap);
        @memset(keys, empty_key);
        const counts = try allocator.alloc(u32, cap);
        @memset(counts, 0);
        const sum_a = try allocator.alloc(u32, cap);
        @memset(sum_a, 0);
        const sum_b = try allocator.alloc(u64, cap);
        @memset(sum_b, 0);
        return .{
            .keys = keys,
            .counts = counts,
            .sum_a = sum_a,
            .sum_b = sum_b,
            .capacity = cap,
            .mask = @as(u64, cap - 1),
            .len = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *HashU64Tuple3Count) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.counts);
        self.allocator.free(self.sum_a);
        self.allocator.free(self.sum_b);
        self.* = undefined;
    }

    inline fn hash(key: u64) u64 {
        var x = key ^ (key >> 30);
        x = x *% 0xbf58_476d_1ce4_e5b9;
        x ^= x >> 27;
        x = x *% 0x94d0_49bb_1331_11eb;
        x ^= x >> 31;
        return x;
    }

    pub inline fn add(self: *HashU64Tuple3Count, key: u64, a: u32, b: u32) void {
        std.debug.assert(key != empty_key);
        var idx = hash(key) & self.mask;
        while (true) {
            const slot = self.keys[idx];
            if (slot == key) {
                self.counts[idx] += 1;
                self.sum_a[idx] += a;
                self.sum_b[idx] += b;
                return;
            }
            if (slot == empty_key) {
                self.keys[idx] = key;
                self.counts[idx] = 1;
                self.sum_a[idx] = a;
                self.sum_b[idx] = b;
                self.len += 1;
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }
};

/// Partitioned variant of HashU64Tuple3Count. Mirrors PartitionedHashU64Count
/// but with three aggregates per slot.
pub const PartitionedHashU64Tuple3Count = struct {
    parts: [partition_count]HashU64Tuple3Count,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, expected_total_keys: usize) !PartitionedHashU64Tuple3Count {
        const per_part = (expected_total_keys / partition_count) + (expected_total_keys / partition_count / 4) + 16;
        var self: PartitionedHashU64Tuple3Count = .{ .parts = undefined, .allocator = allocator };
        var inited: usize = 0;
        errdefer for (self.parts[0..inited]) |*p| p.deinit();
        for (&self.parts) |*p| {
            p.* = try HashU64Tuple3Count.init(allocator, per_part);
            inited += 1;
        }
        return self;
    }

    pub fn deinit(self: *PartitionedHashU64Tuple3Count) void {
        for (&self.parts) |*p| p.deinit();
        self.* = undefined;
    }

    pub inline fn add(self: *PartitionedHashU64Tuple3Count, key: u64, a: u32, b: u32) void {
        const p = partitionFor(key);
        self.parts[p].add(key, a, b);
    }
};

/// Merge partition `p` from every per-thread `PartitionedHashU64Tuple3Count`
/// into a single fresh HashU64Tuple3Count. Caller owns the result.
pub fn mergeTuple3Partition(
    allocator: std.mem.Allocator,
    locals: []*PartitionedHashU64Tuple3Count,
    p: usize,
    expected_p_keys: usize,
) !HashU64Tuple3Count {
    var out = try HashU64Tuple3Count.init(allocator, expected_p_keys);
    errdefer out.deinit();
    for (locals) |local| {
        const src = &local.parts[p];
        var i: usize = 0;
        while (i < src.capacity) : (i += 1) {
            const k = src.keys[i];
            if (k == empty_key) continue;
            const c = src.counts[i];
            const sa = src.sum_a[i];
            const sb = src.sum_b[i];
            var idx = blk: {
                var x = k ^ (k >> 30);
                x = x *% 0xbf58_476d_1ce4_e5b9;
                x ^= x >> 27;
                x = x *% 0x94d0_49bb_1331_11eb;
                x ^= x >> 31;
                break :blk x & out.mask;
            };
            while (true) {
                const slot = out.keys[idx];
                if (slot == k) {
                    out.counts[idx] += c;
                    out.sum_a[idx] += sa;
                    out.sum_b[idx] += sb;
                    break;
                }
                if (slot == empty_key) {
                    out.keys[idx] = k;
                    out.counts[idx] = c;
                    out.sum_a[idx] = sa;
                    out.sum_b[idx] = sb;
                    out.len += 1;
                    break;
                }
                idx = (idx + 1) & out.mask;
            }
        }
    }
    return out;
}


test "HashU64Count basic" {
    const t = std.testing;
    var m = try HashU64Count.init(t.allocator, 1024);
    defer m.deinit();
    m.bump(42);
    m.bump(42);
    m.bump(7);
    try t.expectEqual(@as(usize, 2), m.len);
    var it = m.iterator();
    var seen42: u32 = 0;
    var seen7: u32 = 0;
    while (it.next()) |e| {
        if (e.key == 42) seen42 = e.value;
        if (e.key == 7) seen7 = e.value;
    }
    try t.expectEqual(@as(u32, 2), seen42);
    try t.expectEqual(@as(u32, 1), seen7);
}

test "PartitionedHashU64Count routes consistently" {
    // Same key always lands in the same partition.
    var i: u64 = 1;
    while (i < 10_000) : (i += 1) {
        const a = partitionFor(i);
        const b = partitionFor(i);
        try std.testing.expectEqual(a, b);
        try std.testing.expect(a < partition_count);
    }
}

test "PartitionedHashU64Count distributes uniformly enough" {
    // 64-bin chi-squared sanity: 64k random keys should spread roughly
    // 1k per bin. Allow generous slack.
    var counts: [partition_count]u32 = .{0} ** partition_count;
    var i: u64 = 1;
    while (i <= 64_000) : (i += 1) {
        counts[partitionFor(i)] += 1;
    }
    var min_c: u32 = std.math.maxInt(u32);
    var max_c: u32 = 0;
    for (counts) |c| {
        if (c < min_c) min_c = c;
        if (c > max_c) max_c = c;
    }
    // Expected ~1000; very loose envelope.
    try std.testing.expect(min_c >= 700);
    try std.testing.expect(max_c <= 1300);
}

test "PartitionedHashU64Count + mergePartition" {
    const t = std.testing;
    var a = try PartitionedHashU64Count.init(t.allocator, 1024);
    defer a.deinit();
    var b = try PartitionedHashU64Count.init(t.allocator, 1024);
    defer b.deinit();

    var k: u64 = 1;
    while (k <= 500) : (k += 1) {
        a.bump(k);
        a.bump(k);
        b.bump(k);
    }
    // a contributes 2, b contributes 1 -> merged value should be 3 per key.

    const locals = try t.allocator.alloc(*PartitionedHashU64Count, 2);
    defer t.allocator.free(locals);
    locals[0] = &a;
    locals[1] = &b;

    var seen: u32 = 0;
    var p: usize = 0;
    while (p < partition_count) : (p += 1) {
        const expected_p = a.parts[p].len + b.parts[p].len;
        if (expected_p == 0) continue;
        var merged = try mergePartition(t.allocator, locals, p, expected_p);
        defer merged.deinit();
        var it = merged.iterator();
        while (it.next()) |e| {
            try t.expectEqual(@as(u32, 3), e.value);
            try t.expect(e.key >= 1 and e.key <= 500);
            seen += 1;
        }
    }
    try t.expectEqual(@as(u32, 500), seen);
}

test "HashU64Tuple3Count basic add and merge" {
    const t = std.testing;
    var a = try PartitionedHashU64Tuple3Count.init(t.allocator, 1024);
    defer a.deinit();
    var b = try PartitionedHashU64Tuple3Count.init(t.allocator, 1024);
    defer b.deinit();
    // a: key=42 -> count=2 sum_a=10 sum_b=300
    a.add(42, 4, 100);
    a.add(42, 6, 200);
    // b: key=42 -> count=1 sum_a=3 sum_b=50, key=7 -> count=1 sum_a=1 sum_b=2
    b.add(42, 3, 50);
    b.add(7, 1, 2);

    const locals = try t.allocator.alloc(*PartitionedHashU64Tuple3Count, 2);
    defer t.allocator.free(locals);
    locals[0] = &a;
    locals[1] = &b;

    var p: usize = 0;
    var seen42 = false;
    var seen7 = false;
    while (p < partition_count) : (p += 1) {
        const exp = a.parts[p].len + b.parts[p].len;
        if (exp == 0) continue;
        var merged = try mergeTuple3Partition(t.allocator, locals, p, exp);
        defer merged.deinit();
        var i: usize = 0;
        while (i < merged.capacity) : (i += 1) {
            if (merged.keys[i] == empty_key) continue;
            if (merged.keys[i] == 42) {
                seen42 = true;
                try t.expectEqual(@as(u32, 3), merged.counts[i]);
                try t.expectEqual(@as(u32, 13), merged.sum_a[i]);
                try t.expectEqual(@as(u64, 350), merged.sum_b[i]);
            } else if (merged.keys[i] == 7) {
                seen7 = true;
                try t.expectEqual(@as(u32, 1), merged.counts[i]);
                try t.expectEqual(@as(u32, 1), merged.sum_a[i]);
                try t.expectEqual(@as(u64, 2), merged.sum_b[i]);
            }
        }
    }
    try t.expect(seen42);
    try t.expect(seen7);
}
