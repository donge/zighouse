/// Hash table primitives shared by HashAgg and HashJoin operators.
///
/// Design: open-addressing hash table with linear probing.
/// Key: []Value (composite GROUP BY key or join key).
/// Value: flexible — HashAgg stores []AggAccum, HashJoin stores row indices.
///
/// All memory is allocated from a provided ArenaAllocator so the entire
/// table can be freed in O(1) when the query finishes.
const std = @import("std");
const types = @import("../types.zig");

pub const Value = types.Value;
pub const AggAccum = types.AggAccum;

// ── Key hashing ───────────────────────────────────────────────────────────────

/// Hash a composite key (slice of Values) using a single Wyhash pass.
/// All key components are fed into one hasher for better throughput.
pub fn hashKey(key: []const Value) u64 {
    var h = std.hash.Wyhash.init(0);
    for (key) |v| {
        const tag: u8 = @intFromEnum(@as(types.ColumnType, v));
        h.update(&[1]u8{tag});
        switch (v) {
            .int64         => |x| h.update(std.mem.asBytes(&x)),
            .uint64        => |x| h.update(std.mem.asBytes(&x)),
            .float64       => |x| { const n: f64 = if (x == 0.0) 0.0 else x; h.update(std.mem.asBytes(&n)); },
            .date_u16      => |x| h.update(std.mem.asBytes(&x)),
            .datetime64_ms => |x| h.update(std.mem.asBytes(&x)),
            .bool_u8       => |x| h.update(std.mem.asBytes(&x)),
            .string        => |x| h.update(x),
            .array_string  => |arr| { for (arr) |s| h.update(s); },
        }
    }
    return h.final();
}

/// Compare two composite keys for equality.
pub fn eqlKey(a: []const Value, b: []const Value) bool {
    if (a.len != b.len) return false;
    for (a, b) |av, bv| {
        if (!Value.eql(av, bv)) return false;
    }
    return true;
}

// ── AggHashTable ─────────────────────────────────────────────────────────────

/// Open-addressing hash table mapping composite Value keys to aggregate
/// accumulators. Used by HashAggOp.
///
/// Layout: parallel arrays `keys`, `accums`, `occupied`.
/// Probe: linear.
pub const AggHashTable = struct {
    const LOAD_FACTOR = 0.70;
    const INITIAL_CAP = 64;

    keys:     [][]Value,          // each entry: slice of num_key_cols Values
    accums:   [][]AggAccum,       // each entry: slice of num_agg_cols AggAccums
    occupied: []bool,
    hashes:   []u64,              // cached hash per slot — avoids key comparison on probe miss
    capacity: usize,
    count:    usize,
    num_keys: usize,
    num_aggs: usize,
    arena:    std.mem.Allocator,

    pub fn init(
        arena: std.mem.Allocator,
        num_keys: usize,
        num_aggs: usize,
    ) !AggHashTable {
        return initWithCapacity(arena, num_keys, num_aggs, 0);
    }

    /// Like init but pre-sizes the table for `est_rows` rows (0 = use default).
    pub fn initWithCapacity(
        arena: std.mem.Allocator,
        num_keys: usize,
        num_aggs: usize,
        est_rows: u64,
    ) !AggHashTable {
        const cap = if (est_rows > 0)
            // Round up to next power of 2 at 70% load factor.
            nextPow2(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else
            INITIAL_CAP;
        const keys     = try arena.alloc([]Value,    cap);
        const accums   = try arena.alloc([]AggAccum, cap);
        const occupied = try arena.alloc(bool,       cap);
        const hashes   = try arena.alloc(u64,        cap);
        @memset(occupied, false);
        return .{
            .keys     = keys,
            .accums   = accums,
            .occupied = occupied,
            .hashes   = hashes,
            .capacity = cap,
            .count    = 0,
            .num_keys = num_keys,
            .num_aggs = num_aggs,
            .arena    = arena,
        };
    }

    fn nextPow2(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    /// Look up a key. Returns a pointer to the accumulators for that key,
    /// initialising them with `init_accums` if the key is new.
    pub fn getOrInsert(
        self: *AggHashTable,
        key: []const Value,
        init_accums: []const AggAccum,
    ) ![]AggAccum {
        // Grow if over load factor (capacity is always a power of 2).
        if (self.count + 1 > (self.capacity * 7) / 10) {
            try self.grow();
        }

        const mask = self.capacity - 1;
        const h    = hashKey(key);
        var  slot  = h & mask;

        while (true) : (slot = (slot + 1) & mask) {
            if (!self.occupied[slot]) {
                // New entry
                const k = try self.arena.dupe(Value, key);
                const a = try self.arena.dupe(AggAccum, init_accums);
                self.keys[slot]     = k;
                self.accums[slot]   = a;
                self.hashes[slot]   = h;
                self.occupied[slot] = true;
                self.count += 1;
                return self.accums[slot];
            }
            // Fast hash check before full key comparison.
            if (self.hashes[slot] == h and eqlKey(self.keys[slot], key)) {
                return self.accums[slot];
            }
        }
    }

    fn grow(self: *AggHashTable) !void {
        const new_cap  = self.capacity * 2;
        const new_mask = new_cap - 1;
        const new_keys     = try self.arena.alloc([]Value,    new_cap);
        const new_accums   = try self.arena.alloc([]AggAccum, new_cap);
        const new_occupied = try self.arena.alloc(bool,       new_cap);
        const new_hashes   = try self.arena.alloc(u64,        new_cap);
        @memset(new_occupied, false);

        for (0..self.capacity) |i| {
            if (!self.occupied[i]) continue;
            const h   = self.hashes[i];
            var  slot = h & new_mask;
            while (new_occupied[slot]) : (slot = (slot + 1) & new_mask) {}
            new_keys[slot]     = self.keys[i];
            new_accums[slot]   = self.accums[i];
            new_hashes[slot]   = h;
            new_occupied[slot] = true;
        }

        self.keys     = new_keys;
        self.accums   = new_accums;
        self.occupied = new_occupied;
        self.hashes   = new_hashes;
        self.capacity = new_cap;
    }

    /// Iterate over all occupied entries. Caller provides a callback:
    ///   fn(key: []const Value, accums: []const AggAccum) void
    pub fn iterate(self: *const AggHashTable, ctx: anytype, comptime cb: fn (@TypeOf(ctx), []const Value, []const AggAccum) void) void {
        for (0..self.capacity) |i| {
            if (self.occupied[i]) cb(ctx, self.keys[i], self.accums[i]);
        }
    }
};

// ── JoinHashTable ─────────────────────────────────────────────────────────────

/// Hash table used for the build side of a hash join.
/// Maps composite key → list of row indices in the build-side DataChunk.
pub const JoinHashTable = struct {
    const INITIAL_CAP  = 64;
    const LOAD_FACTOR  = 0.70;

    /// Each bucket holds a list of row indices (for multi-match support).
    buckets:  []std.ArrayListUnmanaged(u32),
    keys:     [][]Value,
    occupied: []bool,
    capacity: usize,
    count:    usize,
    arena:    std.mem.Allocator,

    pub fn init(arena: std.mem.Allocator) !JoinHashTable {
        const cap      = INITIAL_CAP;
        const buckets  = try arena.alloc(std.ArrayListUnmanaged(u32), cap);
        const keys     = try arena.alloc([]Value, cap);
        const occupied = try arena.alloc(bool,    cap);
        @memset(occupied, false);
        for (buckets) |*b| b.* = .empty;
        return .{
            .buckets  = buckets,
            .keys     = keys,
            .occupied = occupied,
            .capacity = cap,
            .count    = 0,
            .arena    = arena,
        };
    }

    /// Insert a build-side row index for the given key.
    pub fn insert(self: *JoinHashTable, key: []const Value, row_idx: u32) !void {
        // Grow if over load factor.
        if (self.count + 1 > @as(usize, @intFromFloat(@as(f64, @floatFromInt(self.capacity)) * LOAD_FACTOR))) {
            try self.grow();
        }
        const h   = hashKey(key);
        var  slot = h % self.capacity;
        while (true) : (slot = (slot + 1) % self.capacity) {
            if (!self.occupied[slot]) {
                self.keys[slot]     = try self.arena.dupe(Value, key);
                self.occupied[slot] = true;
                self.buckets[slot]  = .empty;
                try self.buckets[slot].append(self.arena, row_idx);
                self.count += 1;
                return;
            }
            if (eqlKey(self.keys[slot], key)) {
                try self.buckets[slot].append(self.arena, row_idx);
                return;
            }
        }
    }

    fn grow(self: *JoinHashTable) !void {
        const new_cap      = self.capacity * 2;
        const new_buckets  = try self.arena.alloc(std.ArrayListUnmanaged(u32), new_cap);
        const new_keys     = try self.arena.alloc([]Value, new_cap);
        const new_occupied = try self.arena.alloc(bool,    new_cap);
        @memset(new_occupied, false);
        for (new_buckets) |*b| b.* = .empty;

        for (0..self.capacity) |i| {
            if (!self.occupied[i]) continue;
            const h   = hashKey(self.keys[i]);
            var  slot = h % new_cap;
            while (new_occupied[slot]) : (slot = (slot + 1) % new_cap) {}
            new_keys[slot]     = self.keys[i];
            new_buckets[slot]  = self.buckets[i];
            new_occupied[slot] = true;
        }

        self.buckets  = new_buckets;
        self.keys     = new_keys;
        self.occupied = new_occupied;
        self.capacity = new_cap;
    }

    /// Look up all row indices for a given probe key.
    /// Returns an empty slice if not found.
    pub fn probe(self: *const JoinHashTable, key: []const Value) []const u32 {
        const h   = hashKey(key);
        var  slot = h % self.capacity;
        var  iters: usize = 0;
        while (iters < self.capacity) : ({
            slot = (slot + 1) % self.capacity;
            iters += 1;
        }) {
            if (!self.occupied[slot]) return &.{};
            if (eqlKey(self.keys[slot], key)) return self.buckets[slot].items;
        }
        return &.{};
    }
};

// ── IntKeyHashTable ───────────────────────────────────────────────────────────

/// Specialized open-addressing hash table for integer (i64) composite keys.
/// Keys are stored inline as flat i64 arrays, avoiding []Value boxing.
/// 10-30% faster than AggHashTable for all-integer GROUP BY (e.g. Q33, Q16, Q36).
pub const IntKeyHashTable = struct {
    const LOAD_FACTOR = 0.70;
    const INITIAL_CAP = 64;

    /// Flat storage: slot i's key is keys_flat[i*num_keys .. (i+1)*num_keys].
    keys_flat: []i64,
    accums:    [][]AggAccum,
    occupied:  []bool,
    hashes:    []u64,
    capacity:  usize,
    count:     usize,
    num_keys:  usize,
    num_aggs:  usize,
    arena:     std.mem.Allocator,

    pub fn initWithCapacity(
        arena: std.mem.Allocator,
        num_keys: usize,
        num_aggs: usize,
        est_rows: u64,
    ) !IntKeyHashTable {
        const cap = if (est_rows > 0)
            nextPow2I(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else INITIAL_CAP;
        const keys_flat = try arena.alloc(i64,       cap * num_keys);
        const accums    = try arena.alloc([]AggAccum, cap);
        const occupied  = try arena.alloc(bool,       cap);
        const hashes    = try arena.alloc(u64,        cap);
        @memset(occupied, false);
        return .{
            .keys_flat = keys_flat,
            .accums    = accums,
            .occupied  = occupied,
            .hashes    = hashes,
            .capacity  = cap,
            .count     = 0,
            .num_keys  = num_keys,
            .num_aggs  = num_aggs,
            .arena     = arena,
        };
    }

    fn nextPow2I(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    fn hashI64s(keys: []const i64) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.sliceAsBytes(keys));
        return h.final();
    }

    pub fn getOrInsert(
        self: *IntKeyHashTable,
        key: []const i64,
        init_accums: []const AggAccum,
    ) ![]AggAccum {
        if (self.count + 1 > (self.capacity * 7) / 10) {
            try self.grow();
        }
        const mask = self.capacity - 1;
        const h    = hashI64s(key);
        var   slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            if (!self.occupied[slot]) {
                const base = slot * self.num_keys;
                @memcpy(self.keys_flat[base .. base + self.num_keys], key);
                const a = try self.arena.dupe(AggAccum, init_accums);
                self.accums[slot]   = a;
                self.hashes[slot]   = h;
                self.occupied[slot] = true;
                self.count += 1;
                return self.accums[slot];
            }
            if (self.hashes[slot] == h) {
                const base = slot * self.num_keys;
                if (std.mem.eql(i64, self.keys_flat[base .. base + self.num_keys], key)) {
                    return self.accums[slot];
                }
            }
        }
    }

    fn grow(self: *IntKeyHashTable) !void {
        const new_cap      = self.capacity * 2;
        const new_mask     = new_cap - 1;
        const new_keys     = try self.arena.alloc(i64,       new_cap * self.num_keys);
        const new_accums   = try self.arena.alloc([]AggAccum, new_cap);
        const new_occupied = try self.arena.alloc(bool,       new_cap);
        const new_hashes   = try self.arena.alloc(u64,        new_cap);
        @memset(new_occupied, false);

        for (0..self.capacity) |i| {
            if (!self.occupied[i]) continue;
            const h    = self.hashes[i];
            var   slot = h & new_mask;
            while (new_occupied[slot]) : (slot = (slot + 1) & new_mask) {}
            const src_base  = i * self.num_keys;
            const dst_base  = slot * self.num_keys;
            @memcpy(new_keys[dst_base .. dst_base + self.num_keys], self.keys_flat[src_base .. src_base + self.num_keys]);
            new_accums[slot]   = self.accums[i];
            new_hashes[slot]   = h;
            new_occupied[slot] = true;
        }

        self.keys_flat = new_keys;
        self.accums    = new_accums;
        self.occupied  = new_occupied;
        self.hashes    = new_hashes;
        self.capacity  = new_cap;
    }

    /// Iterate over all occupied entries, calling cb with (key_slice, accums_slice).
    pub fn iterate(self: *const IntKeyHashTable, ctx: anytype, comptime cb: fn (@TypeOf(ctx), []const i64, []const AggAccum) void) void {
        for (0..self.capacity) |i| {
            if (self.occupied[i]) {
                const base = i * self.num_keys;
                cb(ctx, self.keys_flat[base .. base + self.num_keys], self.accums[i]);
            }
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "AggHashTable insert and lookup" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var tbl = try AggHashTable.init(alloc, 1, 1);

    const key = [_]Value{.{ .string = "foo" }};
    const init_accums = [_]AggAccum{.{ .count = 0 }};

    const a1 = try tbl.getOrInsert(&key, &init_accums);
    a1[0].count += 1;

    const a2 = try tbl.getOrInsert(&key, &init_accums);
    try std.testing.expectEqual(@as(u64, 1), a2[0].count);
}

test "JoinHashTable insert and probe" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var tbl = try JoinHashTable.init(alloc);

    const key = [_]Value{.{ .int64 = 42 }};
    try tbl.insert(&key, 0);
    try tbl.insert(&key, 7);

    const rows = tbl.probe(&key);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
}
