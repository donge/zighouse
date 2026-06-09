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

// ── PairCountHashTable ────────────────────────────────────────────────────────

/// Specialized hash table for (i64, string) composite GROUP BY with count(*).
/// Flat layout, no per-slot arena alloc.  Used by Q17/Q18.
/// Uses hash==0 as empty sentinel (no separate occupied[] array).
pub const PairCountHashTable = struct {
    const EMPTY: u64 = 0;
    const Slot = struct {
        hash:  u64,
        i64_key: i64,
        str_key: []const u8,
        count:   u64,
    };

    slots:    []Slot,
    capacity: usize,
    count:    usize,
    arena:    std.mem.Allocator,

    pub fn initWithCapacity(arena: std.mem.Allocator, est_rows: u64) !PairCountHashTable {
        const cap = if (est_rows > 0)
            nextPow2Pair(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else
            64;
        const slots = try arena.alloc(Slot, cap);
        @memset(std.mem.sliceAsBytes(slots), 0); // zero hash = empty
        return .{ .slots = slots, .capacity = cap, .count = 0, .arena = arena };
    }

    fn nextPow2Pair(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    pub fn tagHash(h: u64) u64 { return if (h == 0) 1 else h; }

    pub fn increment(self: *PairCountHashTable, i64_key: i64, str_key: []const u8) !void {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();

        const mask = self.capacity - 1;
        const h = tagHash(hashPair(i64_key, str_key));
        var slot = h & mask;

        while (true) : (slot = (slot + 1) & mask) {
            if (self.slots[slot].hash == EMPTY) {
                self.slots[slot] = .{ .hash = h, .i64_key = i64_key, .str_key = str_key, .count = 1 };
                self.count += 1;
                return;
            }
            if (self.slots[slot].hash == h and
                self.slots[slot].i64_key == i64_key and
                std.mem.eql(u8, self.slots[slot].str_key, str_key))
            {
                self.slots[slot].count += 1;
                return;
            }
        }
    }

    fn grow(self: *PairCountHashTable) !void {
        const new_cap  = self.capacity * 2;
        const new_mask = new_cap - 1;
        const new_slots = try self.arena.alloc(Slot, new_cap);
        @memset(std.mem.sliceAsBytes(new_slots), 0);
        for (0..self.capacity) |i| {
            if (self.slots[i].hash == EMPTY) continue;
            var sl = self.slots[i].hash & new_mask;
            while (new_slots[sl].hash != EMPTY) : (sl = (sl + 1) & new_mask) {}
            new_slots[sl] = self.slots[i];
        }
        self.slots    = new_slots;
        self.capacity = new_cap;
    }

    pub fn iterate(self: *const PairCountHashTable, ctx: anytype, comptime cb: fn (@TypeOf(ctx), i64, []const u8, u64) void) void {
        for (0..self.capacity) |i| {
            if (self.slots[i].hash != EMPTY) cb(ctx, self.slots[i].i64_key, self.slots[i].str_key, self.slots[i].count);
        }
    }

    /// Like mergeFrom but only merges entries where (slot.hash & (n_parts - 1)) == part_id.
    /// Requires n_parts to be a power of 2.
    pub fn mergeFromPart(self: *PairCountHashTable, other: *const PairCountHashTable, part_id: usize, n_parts: usize) !void {
        for (0..other.capacity) |i| {
            const s = other.slots[i];
            if (s.hash == EMPTY) continue;
            if (s.hash & (n_parts - 1) != part_id) continue;
            if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
            const mask = self.capacity - 1;
            var sl = s.hash & mask;
            while (true) : (sl = (sl + 1) & mask) {
                if (self.slots[sl].hash == EMPTY) {
                    self.slots[sl] = s;
                    self.count += 1;
                    break;
                }
                if (self.slots[sl].hash == s.hash and
                    self.slots[sl].i64_key == s.i64_key and
                    std.mem.eql(u8, self.slots[sl].str_key, s.str_key))
                {
                    self.slots[sl].count += s.count;
                    break;
                }
            }
        }
    }

    pub fn hashPair(n: i64, s: []const u8) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.asBytes(&n));
        h.update(s);
        return h.final();
    }

    /// Increment with a pre-computed tagged hash (avoids re-hashing in prefetch pipeline).
    pub fn incrementPrehashed(self: *PairCountHashTable, i64_key: i64, str_key: []const u8, h: u64) !void {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
        const mask = self.capacity - 1;
        var slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            if (self.slots[slot].hash == EMPTY) {
                self.slots[slot] = .{ .hash = h, .i64_key = i64_key, .str_key = str_key, .count = 1 };
                self.count += 1;
                return;
            }
            if (self.slots[slot].hash == h and
                self.slots[slot].i64_key == i64_key and
                std.mem.eql(u8, self.slots[slot].str_key, str_key))
            {
                self.slots[slot].count += 1;
                return;
            }
        }
    }
};

// ── TripleCountHashTable ───────────────────────────────────────────────────────

/// Specialized hash table for (i64, i64, string) GROUP BY with count(*).
/// Used for Q19: GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase.
/// Uses hash==0 as empty sentinel (no separate occupied[] array).
pub const TripleCountHashTable = struct {
    pub const EMPTY: u64 = 0;
    pub const Slot = struct {
        hash:    u64,
        n0:      i64,
        n1:      i64,
        str_key: []const u8,
        count:   u64,
    };

    slots:    []Slot,
    capacity: usize,
    count:    usize,
    arena:    std.mem.Allocator,

    pub fn initWithCapacity(arena: std.mem.Allocator, est_rows: u64) !TripleCountHashTable {
        const cap = if (est_rows > 0)
            nextPow2Triple(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else
            64;
        const slots = try arena.alloc(Slot, cap);
        @memset(std.mem.sliceAsBytes(slots), 0); // zero hash = empty
        return .{ .slots = slots, .capacity = cap, .count = 0, .arena = arena };
    }

    fn nextPow2Triple(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    fn tagHash(h: u64) u64 { return if (h == 0) 1 else h; }

    pub fn increment(self: *TripleCountHashTable, n0: i64, n1: i64, str_key: []const u8) !void {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();

        const mask = self.capacity - 1;
        const h = tagHash(hashTriple(n0, n1, str_key));
        var slot = h & mask;

        while (true) : (slot = (slot + 1) & mask) {
            if (self.slots[slot].hash == EMPTY) {
                self.slots[slot] = .{ .hash = h, .n0 = n0, .n1 = n1, .str_key = str_key, .count = 1 };
                self.count += 1;
                return;
            }
            if (self.slots[slot].hash == h and
                self.slots[slot].n0 == n0 and
                self.slots[slot].n1 == n1 and
                std.mem.eql(u8, self.slots[slot].str_key, str_key))
            {
                self.slots[slot].count += 1;
                return;
            }
        }
    }

    fn grow(self: *TripleCountHashTable) !void {
        const new_cap  = self.capacity * 2;
        const new_mask = new_cap - 1;
        const new_slots = try self.arena.alloc(Slot, new_cap);
        @memset(std.mem.sliceAsBytes(new_slots), 0);
        for (0..self.capacity) |i| {
            if (self.slots[i].hash == EMPTY) continue;
            var sl = self.slots[i].hash & new_mask;
            while (new_slots[sl].hash != EMPTY) : (sl = (sl + 1) & new_mask) {}
            new_slots[sl] = self.slots[i];
        }
        self.slots    = new_slots;
        self.capacity = new_cap;
    }

    pub fn iterate(self: *const TripleCountHashTable, ctx: anytype, comptime cb: fn (@TypeOf(ctx), i64, i64, []const u8, u64) void) void {
        for (0..self.capacity) |i| {
            if (self.slots[i].hash != EMPTY) cb(ctx, self.slots[i].n0, self.slots[i].n1, self.slots[i].str_key, self.slots[i].count);
        }
    }

    pub fn mergeFromSlot(self: *TripleCountHashTable, s: Slot) !void {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
        const mask = self.capacity - 1;
        var sl = s.hash & mask;
        while (true) : (sl = (sl + 1) & mask) {
            if (self.slots[sl].hash == EMPTY) {
                self.slots[sl] = s;
                self.count += 1;
                return;
            }
            if (self.slots[sl].hash == s.hash and
                self.slots[sl].n0 == s.n0 and
                self.slots[sl].n1 == s.n1 and
                std.mem.eql(u8, self.slots[sl].str_key, s.str_key))
            {
                self.slots[sl].count += s.count;
                return;
            }
        }
    }

    fn hashTriple(n0: i64, n1: i64, s: []const u8) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.asBytes(&n0));
        h.update(std.mem.asBytes(&n1));
        h.update(s);
        return h.final();
    }
};

// ── IntKeyHashTable ───────────────────────────────────────────────────────────

/// Specialized open-addressing hash table for integer (i64) composite keys.
/// Keys are stored inline as flat i64 arrays, avoiding []Value boxing.
/// 10-30% faster than AggHashTable for all-integer GROUP BY (e.g. Q33, Q16, Q36).
///
/// Accumulators are stored inline (flat array, no pointer per slot) to eliminate
/// per-insert arena allocation and pointer indirection on every update.
/// Layout: slot i → keys_flat[i*num_keys..(i+1)*num_keys], accums_flat[i*num_aggs..(i+1)*num_aggs].
///
/// `tags` merges occupied+hash into one u64 (0 = empty, else = hash value with bit 63 forced to 1).
/// This eliminates a separate `occupied[]` array access on every probe, reducing L3 misses.
pub const IntKeyHashTable = struct {
    const LOAD_FACTOR = 0.70;
    const INITIAL_CAP = 64;
    /// Tag sentinel: 0 = empty.  Stored hash = raw_hash | (1 << 63) to avoid 0 collision.
    const EMPTY_TAG: u64 = 0;

    /// Flat key storage: slot i's key is keys_flat[i*num_keys .. (i+1)*num_keys].
    keys_flat:  []i64,
    /// Flat accum storage: slot i's accums are accums_flat[i*num_aggs .. (i+1)*num_aggs].
    accums_flat: []AggAccum,
    /// tags[i] = 0 (empty) or (hash | TAG_SET_BIT).  Replaces separate occupied+hashes arrays.
    tags:       []u64,
    capacity:   usize,
    count:      usize,
    num_keys:   usize,
    num_aggs:   usize,
    arena:      std.mem.Allocator,

    pub fn initWithCapacity(
        arena: std.mem.Allocator,
        num_keys: usize,
        num_aggs: usize,
        est_rows: u64,
    ) !IntKeyHashTable {
        const cap = if (est_rows > 0)
            nextPow2I(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else INITIAL_CAP;
        const keys_flat   = try arena.alloc(i64,      cap * num_keys);
        const accums_flat = try arena.alloc(AggAccum,  cap * num_aggs);
        const tags        = try arena.alloc(u64,       cap);
        @memset(tags, EMPTY_TAG);
        return .{
            .keys_flat   = keys_flat,
            .accums_flat = accums_flat,
            .tags        = tags,
            .capacity    = cap,
            .count       = 0,
            .num_keys    = num_keys,
            .num_aggs    = num_aggs,
            .arena       = arena,
        };
    }

    fn nextPow2I(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    fn hashI64s(keys: []const i64) u64 {
        // For 1-2 keys use a faster multiply-xor-shift mix (avoids Wyhash overhead).
        if (keys.len == 1) {
            var h: u64 = @bitCast(keys[0]);
            h ^= h >> 33;
            h *%= 0xff51afd7ed558ccd;
            h ^= h >> 33;
            h *%= 0xc4ceb9fe1a85ec53;
            h ^= h >> 33;
            return h | (1 << 63); // ensure non-zero
        }
        if (keys.len == 2) {
            const k0: u64 = @bitCast(keys[0]);
            const k1: u64 = @bitCast(keys[1]);
            var h: u64 = k0 *% 0x9e3779b97f4a7c15 ^ k1 *% 0x6c62272e07bb0142;
            h ^= h >> 30;
            h *%= 0xbf58476d1ce4e5b9;
            h ^= h >> 27;
            h *%= 0x94d049bb133111eb;
            h ^= h >> 31;
            return h | (1 << 63);
        }
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.sliceAsBytes(keys));
        return h.final() | (1 << 63);
    }

    /// Returns a slice into accums_flat for slot. On new insert, copies init_accums.
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
            const tag = self.tags[slot];
            if (tag == EMPTY_TAG) {
                // Empty slot: insert here.
                const kbase = slot * self.num_keys;
                @memcpy(self.keys_flat[kbase .. kbase + self.num_keys], key);
                const abase = slot * self.num_aggs;
                @memcpy(self.accums_flat[abase .. abase + self.num_aggs], init_accums);
                self.tags[slot] = h;
                self.count += 1;
                return self.accums_flat[abase .. abase + self.num_aggs];
            }
            if (tag == h) {
                const kbase = slot * self.num_keys;
                if (std.mem.eql(i64, self.keys_flat[kbase .. kbase + self.num_keys], key)) {
                    const abase = slot * self.num_aggs;
                    return self.accums_flat[abase .. abase + self.num_aggs];
                }
            }
        }
    }

    fn grow(self: *IntKeyHashTable) !void {
        const new_cap  = self.capacity * 2;
        const new_mask = new_cap - 1;
        const new_keys   = try self.arena.alloc(i64,      new_cap * self.num_keys);
        const new_accums = try self.arena.alloc(AggAccum,  new_cap * self.num_aggs);
        const new_tags   = try self.arena.alloc(u64,       new_cap);
        @memset(new_tags, EMPTY_TAG);

        for (0..self.capacity) |i| {
            if (self.tags[i] == EMPTY_TAG) continue;
            const h    = self.tags[i];
            var   slot = h & new_mask;
            while (new_tags[slot] != EMPTY_TAG) : (slot = (slot + 1) & new_mask) {}
            const src_kbase = i * self.num_keys;
            const dst_kbase = slot * self.num_keys;
            @memcpy(new_keys[dst_kbase .. dst_kbase + self.num_keys],
                    self.keys_flat[src_kbase .. src_kbase + self.num_keys]);
            const src_abase = i * self.num_aggs;
            const dst_abase = slot * self.num_aggs;
            @memcpy(new_accums[dst_abase .. dst_abase + self.num_aggs],
                    self.accums_flat[src_abase .. src_abase + self.num_aggs]);
            new_tags[slot] = h;
        }

        self.keys_flat   = new_keys;
        self.accums_flat = new_accums;
        self.tags        = new_tags;
        self.capacity    = new_cap;
    }

    /// Iterate over all occupied entries, calling cb with (key_slice, accums_slice).
    pub fn iterate(self: *const IntKeyHashTable, ctx: anytype, comptime cb: fn (@TypeOf(ctx), []const i64, []const AggAccum) void) void {
        for (0..self.capacity) |i| {
            if (self.tags[i] != EMPTY_TAG) {
                const kbase = i * self.num_keys;
                const abase = i * self.num_aggs;
                cb(ctx, self.keys_flat[kbase .. kbase + self.num_keys],
                        self.accums_flat[abase .. abase + self.num_aggs]);
            }
        }
    }
};

// ── StrAggHashTable ───────────────────────────────────────────────────────────

/// Specialized hash table for single-string GROUP BY keys with numeric aggregates.
/// Key is stored as a borrowed []const u8 pointer (no arena.dupe needed when the
/// source string is in stable memory — store mmap or non-freed chunk arenas).
/// Accumulators are stored as u64 (8B each, same as CompactIntKeyHashTable).
///
/// Layout:
///   key_slots[slot] = { hash: u64, str: []const u8 }  — 24B per slot
///   vals_flat[slot*num_aggs .. (slot+1)*num_aggs]      — 8B per agg per slot
///   tags[slot] = 0 (empty) | (hash | bit63)
///
/// String sidecar (for str_min / str_max aggs):
///   str_sidecar[slot*num_str_aggs + str_idx]  — nullable []const u8 per slot
pub const StrAggHashTable = struct {
    const EMPTY_TAG: u64 = 0;
    const INITIAL_CAP = 64;

    const KeySlot = struct {
        str:          []const u8,
        int_sidecar:  i64 = 0,  // optional integer co-key (e.g. SearchEngineID in Q15)
    };

    key_slots:    []KeySlot,
    vals_flat:    []u64,
    tags:         []u64,
    /// Sidecar for string-valued aggs (str_min / str_max).
    /// Indexed as [slot * num_str_aggs + sidecar_idx].  null = unset (initial state).
    str_sidecar:  []?[]const u8,
    capacity:     usize,
    count:        usize,
    num_aggs:     usize,
    num_str_aggs: usize,
    arena:        std.mem.Allocator,

    pub fn initWithCapacity(
        arena:        std.mem.Allocator,
        num_aggs:     usize,
        num_str_aggs: usize,
        est_rows:     u64,
    ) !StrAggHashTable {
        const cap = if (est_rows > 0)
            nextPow2(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else INITIAL_CAP;
        const key_slots   = try arena.alloc(KeySlot, cap);
        const vals_flat   = try arena.alloc(u64, cap * num_aggs);
        const tags        = try arena.alloc(u64, cap);
        const str_sidecar = if (num_str_aggs > 0)
            try arena.alloc(?[]const u8, cap * num_str_aggs)
        else
            try arena.alloc(?[]const u8, 0);
        @memset(tags, EMPTY_TAG);
        if (num_str_aggs > 0) @memset(str_sidecar, null);
        return .{
            .key_slots    = key_slots,
            .vals_flat    = vals_flat,
            .tags         = tags,
            .str_sidecar  = str_sidecar,
            .capacity     = cap,
            .count        = 0,
            .num_aggs     = num_aggs,
            .num_str_aggs = num_str_aggs,
            .arena        = arena,
        };
    }

    fn nextPow2(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    pub fn hashStr(s: []const u8) u64 {
        return std.hash.Wyhash.hash(0, s) | (1 << 63);
    }

    pub const InsertResult = struct {
        vals: []u64,
        slot: usize,
    };

    /// Returns a slice into vals_flat and the slot index for the given string key,
    /// inserting with init_vals if new. The str pointer is borrowed (not duped).
    pub fn getOrInsert(
        self:      *StrAggHashTable,
        s:         []const u8,
        init_vals: []const u64,
    ) !InsertResult {
        return self.getOrInsertH(s, hashStr(s), init_vals);
    }

    /// Same as getOrInsert but takes a precomputed hash (avoids recomputation during merge).
    pub fn getOrInsertH(
        self:      *StrAggHashTable,
        s:         []const u8,
        h:         u64,
        init_vals: []const u64,
    ) !InsertResult {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
        const mask = self.capacity - 1;
        var   slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            const tag = self.tags[slot];
            if (tag == EMPTY_TAG) {
                self.key_slots[slot] = .{ .str = s };
                const vb = slot * self.num_aggs;
                @memcpy(self.vals_flat[vb .. vb + self.num_aggs], init_vals);
                self.tags[slot] = h;
                self.count += 1;
                // Zero-init sidecar entries for this slot.
                if (self.num_str_aggs > 0) {
                    const sb = slot * self.num_str_aggs;
                    @memset(self.str_sidecar[sb .. sb + self.num_str_aggs], null);
                }
                return .{ .vals = self.vals_flat[vb .. vb + self.num_aggs], .slot = slot };
            }
            if (tag == h and std.mem.eql(u8, self.key_slots[slot].str, s)) {
                const vb = slot * self.num_aggs;
                return .{ .vals = self.vals_flat[vb .. vb + self.num_aggs], .slot = slot };
            }
        }
    }

    /// Hash-only variant: treats equal 64-bit hash as equal key (no full string comparison).
    /// Safe for high-cardinality string keys where wyhash collision probability is negligible
    /// (~n²/2⁶⁴). Eliminates the random mmap pointer-chase in Phase 2 of two-phase string
    /// aggregation, cutting DRAM stall time for large string columns (e.g. URL: 900 MB).
    /// On insert the string slice is still stored in key_slots for use during emit.
    pub fn getOrInsertHashOnly(
        self:        *StrAggHashTable,
        s:           []const u8,
        h:           u64,
        init_vals:   []const u64,
        int_sidecar: i64,
    ) !InsertResult {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
        const mask = self.capacity - 1;
        var   slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            const tag = self.tags[slot];
            if (tag == EMPTY_TAG) {
                self.key_slots[slot] = .{ .str = s, .int_sidecar = int_sidecar };
                const vb = slot * self.num_aggs;
                @memcpy(self.vals_flat[vb .. vb + self.num_aggs], init_vals);
                self.tags[slot] = h;
                self.count += 1;
                if (self.num_str_aggs > 0) {
                    const sb = slot * self.num_str_aggs;
                    @memset(self.str_sidecar[sb .. sb + self.num_str_aggs], null);
                }
                return .{ .vals = self.vals_flat[vb .. vb + self.num_aggs], .slot = slot };
            }
            if (tag == h) { // hash match = key match (no string comparison)
                const vb = slot * self.num_aggs;
                return .{ .vals = self.vals_flat[vb .. vb + self.num_aggs], .slot = slot };
            }
        }
    }

    /// Probe-only variant: returns slot index if h matches an existing tag, null if absent.
    /// PRECONDITION: caller must ensure count + 1 <= capacity * 7 / 10 (via growTo) before
    /// calling so that a linear probe is guaranteed to terminate at an EMPTY_TAG.
    pub fn probeHashOnly(self: *const StrAggHashTable, h: u64) ?usize {
        const mask = self.capacity - 1;
        var slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            const tag = self.tags[slot];
            if (tag == EMPTY_TAG) return null;
            if (tag == h) return slot;
        }
    }

    /// Insert-only variant for use after probeHashOnly returned null.
    /// PRECONDITION: caller already called growTo(capacity * 2) if needed, then
    /// called probeHashOnly and confirmed the key is absent.
    /// Finds the first EMPTY_TAG slot and inserts s with init_vals. Does NOT grow.
    pub fn insertHashOnly(
        self:      *StrAggHashTable,
        s:         []const u8,
        h:         u64,
        init_vals: []const u64,
    ) InsertResult {
        const mask = self.capacity - 1;
        var slot = h & mask;
        while (true) : (slot = (slot + 1) & mask) {
            if (self.tags[slot] == EMPTY_TAG) {
                self.key_slots[slot] = .{ .str = s };
                const vb = slot * self.num_aggs;
                @memcpy(self.vals_flat[vb .. vb + self.num_aggs], init_vals);
                self.tags[slot] = h;
                self.count += 1;
                if (self.num_str_aggs > 0) {
                    const sb = slot * self.num_str_aggs;
                    @memset(self.str_sidecar[sb .. sb + self.num_str_aggs], null);
                }
                return .{ .vals = self.vals_flat[vb .. vb + self.num_aggs], .slot = slot };
            }
        }
    }

    /// Update a string sidecar entry (str_min or str_max) at the given slot.
    /// For str_min: replaces if new value is lexicographically smaller.
    /// For str_max: replaces if new value is lexicographically larger.
    /// kind must be .str_min or .str_max.
    pub fn updateStrSidecar(
        self:      *StrAggHashTable,
        slot:      usize,
        sidecar_i: usize,
        s:         []const u8,
        comptime is_min: bool,
    ) void {
        const idx = slot * self.num_str_aggs + sidecar_i;
        if (self.str_sidecar[idx]) |cur| {
            if (is_min) {
                if (std.mem.lessThan(u8, s, cur)) self.str_sidecar[idx] = s;
            } else {
                if (std.mem.lessThan(u8, cur, s)) self.str_sidecar[idx] = s;
            }
        } else {
            self.str_sidecar[idx] = s;
        }
    }

    /// Return the string sidecar value for a given slot and sidecar index (null = unset).
    pub fn getStrSidecar(self: *const StrAggHashTable, slot: usize, sidecar_i: usize) ?[]const u8 {
        return self.str_sidecar[slot * self.num_str_aggs + sidecar_i];
    }

    fn grow(self: *StrAggHashTable) !void {
        const new_cap  = self.capacity * 2;
        const new_mask = new_cap - 1;
        const new_keys = try self.arena.alloc(KeySlot, new_cap);
        const new_vals = try self.arena.alloc(u64, new_cap * self.num_aggs);
        const new_tags = try self.arena.alloc(u64, new_cap);
        const new_sc   = if (self.num_str_aggs > 0)
            try self.arena.alloc(?[]const u8, new_cap * self.num_str_aggs)
        else
            try self.arena.alloc(?[]const u8, 0);
        @memset(new_tags, EMPTY_TAG);
        if (self.num_str_aggs > 0) @memset(new_sc, null);
        for (0..self.capacity) |i| {
            if (self.tags[i] == EMPTY_TAG) continue;
            const h    = self.tags[i];
            var   slot = h & new_mask;
            while (new_tags[slot] != EMPTY_TAG) : (slot = (slot + 1) & new_mask) {}
            new_keys[slot] = self.key_slots[i];
            const src_vb = i * self.num_aggs;
            const dst_vb = slot * self.num_aggs;
            @memcpy(new_vals[dst_vb .. dst_vb + self.num_aggs],
                    self.vals_flat[src_vb .. src_vb + self.num_aggs]);
            new_tags[slot] = h;
            if (self.num_str_aggs > 0) {
                const src_sb = i * self.num_str_aggs;
                const dst_sb = slot * self.num_str_aggs;
                @memcpy(new_sc[dst_sb .. dst_sb + self.num_str_aggs],
                        self.str_sidecar[src_sb .. src_sb + self.num_str_aggs]);
            }
        }
        self.key_slots   = new_keys;
        self.vals_flat   = new_vals;
        self.tags        = new_tags;
        self.str_sidecar = new_sc;
         self.capacity    = new_cap;
     }

     /// Grow the HT to at least `new_cap` capacity before insertions to avoid
     /// incremental doubling overhead.  No-op if already large enough.
     /// Old arrays are leaked into `self.arena` (typically a query-lifetime arena).
     pub fn growTo(self: *StrAggHashTable, new_cap: usize) !void {
         if (new_cap <= self.capacity) return;
         const actual_cap = nextPow2(new_cap);
         const new_mask   = actual_cap - 1;
         const new_keys   = try self.arena.alloc(KeySlot, actual_cap);
         const new_vals   = try self.arena.alloc(u64, actual_cap * self.num_aggs);
         const new_tags   = try self.arena.alloc(u64, actual_cap);
         const new_sc     = if (self.num_str_aggs > 0)
             try self.arena.alloc(?[]const u8, actual_cap * self.num_str_aggs)
         else
             try self.arena.alloc(?[]const u8, 0);
         @memset(new_tags, EMPTY_TAG);
         if (self.num_str_aggs > 0) @memset(new_sc, null);
         for (0..self.capacity) |i| {
             if (self.tags[i] == EMPTY_TAG) continue;
             const h    = self.tags[i];
             var   slot = h & new_mask;
             while (new_tags[slot] != EMPTY_TAG) : (slot = (slot + 1) & new_mask) {}
             new_keys[slot] = self.key_slots[i];
             const src_vb = i          * self.num_aggs;
             const dst_vb = slot       * self.num_aggs;
             @memcpy(new_vals[dst_vb .. dst_vb + self.num_aggs],
                     self.vals_flat[src_vb .. src_vb + self.num_aggs]);
             new_tags[slot] = h;
             if (self.num_str_aggs > 0) {
                 const src_sb = i    * self.num_str_aggs;
                 const dst_sb = slot * self.num_str_aggs;
                 @memcpy(new_sc[dst_sb .. dst_sb + self.num_str_aggs],
                         self.str_sidecar[src_sb .. src_sb + self.num_str_aggs]);
             }
         }
         self.key_slots   = new_keys;
         self.vals_flat   = new_vals;
         self.tags        = new_tags;
         self.str_sidecar = new_sc;
         self.capacity    = actual_cap;
     }

     pub fn iterate(
        self: *const StrAggHashTable,
        ctx:  anytype,
        comptime cb: fn (@TypeOf(ctx), []const u8, []const u64) void,
    ) void {
        for (0..self.capacity) |i| {
            if (self.tags[i] != EMPTY_TAG) {
                const vb = i * self.num_aggs;
                cb(ctx, self.key_slots[i].str,
                        self.vals_flat[vb .. vb + self.num_aggs]);
            }
        }
    }

    /// Like iterate but also provides the slot index for sidecar access.
    pub fn iterateWithSlot(
        self: *const StrAggHashTable,
        ctx:  anytype,
        comptime cb: fn (@TypeOf(ctx), []const u8, []const u64, usize) void,
    ) void {
        for (0..self.capacity) |i| {
            if (self.tags[i] != EMPTY_TAG) {
                const vb = i * self.num_aggs;
                cb(ctx, self.key_slots[i].str,
                        self.vals_flat[vb .. vb + self.num_aggs], i);
            }
        }
    }

    /// Like mergeFrom but only merges entries where (tags[i] % n_parts) == part_id.
    /// Used by the partitioned parallel reduce path to avoid serial tree-merge.
    pub fn mergeFromPart(
        self:      *StrAggHashTable,
        other:     *const StrAggHashTable,
        kinds:     []const CompactAggKind,
        init_vals: []const u64,
        part_id:   usize,
        n_parts:   usize,
    ) !void {
        for (0..other.capacity) |i| {
            if (other.tags[i] == EMPTY_TAG) continue;
            if (other.tags[i] % n_parts != part_id) continue;
            const key = other.key_slots[i].str;
            const res = try self.getOrInsertH(key, other.tags[i], init_vals);
            const src_vb = i * other.num_aggs;
            for (kinds, 0..) |kind, ci| {
                const src = other.vals_flat[src_vb + ci];
                switch (kind) {
                    .count, .i64_sum, .u64_sum, .count_distinct_u64 => res.vals[ci] += src,
                    .f64_sum, .f64_str_len_sum => {
                        const a: f64 = @bitCast(res.vals[ci]);
                        const b: f64 = @bitCast(src);
                        res.vals[ci] = @bitCast(a + b);
                    },
                    .i64_min => if (@as(i64, @bitCast(src)) < @as(i64, @bitCast(res.vals[ci]))) { res.vals[ci] = src; },
                    .i64_max => if (@as(i64, @bitCast(src)) > @as(i64, @bitCast(res.vals[ci]))) { res.vals[ci] = src; },
                    .u64_min => if (src < res.vals[ci]) { res.vals[ci] = src; },
                    .u64_max => if (src > res.vals[ci]) { res.vals[ci] = src; },
                    .f64_min => {
                        const a: f64 = @bitCast(res.vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b < a) res.vals[ci] = src;
                    },
                    .f64_max => {
                        const a: f64 = @bitCast(res.vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b > a) res.vals[ci] = src;
                    },
                    .str_min, .str_max => {},
                }
            }
            if (other.num_str_aggs > 0 and self.num_str_aggs > 0) {
                const src_sb = i * other.num_str_aggs;
                for (0..@min(self.num_str_aggs, other.num_str_aggs)) |si| {
                    const src_s = other.str_sidecar[src_sb + si] orelse continue;
                    var sc_i: usize = 0;
                    for (kinds) |kind| {
                        if (kind == .str_min) {
                            if (sc_i == si) { self.updateStrSidecar(res.slot, si, src_s, true); break; }
                            sc_i += 1;
                        } else if (kind == .str_max) {
                            if (sc_i == si) { self.updateStrSidecar(res.slot, si, src_s, false); break; }
                            sc_i += 1;
                        }
                    }
                }
            }
        }
    }
};

// ── CompactIntKeyHashTable ────────────────────────────────────────────────────

/// Agg kind for compact (8-byte) accumulator storage.
/// Covers all numeric aggregates and string min/max (via StrAggHashTable sidecar).
pub const CompactAggKind = enum {
    count,    // u64 ++
    i64_sum,  // i64 +=
    u64_sum,  // u64 +=
    f64_sum,  // f64 += (also used for AVG, with col_ref arg)
    f64_str_len_sum, // f64 += (string length): AVG(length(col_ref))
    i64_min,  // i64 = min(cur, arg)
    i64_max,  // i64 = max(cur, arg)
    u64_min,  // u64 = min(cur, arg)
    u64_max,  // u64 = max(cur, arg)
    f64_min,  // f64 = min(cur, arg)
    f64_max,  // f64 = max(cur, arg)
    /// String min: stored in StrAggHashTable.str_sidecar, not in vals_flat.
    /// vals_flat slot is unused (set to 0).
    str_min,
    /// String max: same as str_min but uses max comparison.
    str_max,
    /// COUNT(DISTINCT int_col): slot stores the running distinct count (u64).
    /// Deduplication is handled by a global flat pair-set in the caller;
    /// merge ops treat this identically to .count (summing counts is correct
    /// only in the sequential path where the pair-set guarantees uniqueness).
    count_distinct_u64,
};

/// Like IntKeyHashTable but stores accumulators as raw u64 (8B) instead of
/// AggAccum (32B union), reducing the accum slab size by 4×.
///
/// This matters for high-cardinality GROUP BY with numeric aggs (e.g. Q33:
/// 10M unique (WatchID, ClientIP) pairs × 3 aggs = 32B×3=96B vs 8B×3=24B per slot;
/// working set drops from ~1.7 GB to ~0.5 GB, reducing L3 miss rate).
///
/// Only applicable when all aggs are pure numeric (no str_min, uniq_strs, any_val).
/// The caller must provide a `kinds` slice describing each agg's update semantics
/// and matching `init_vals` (u64-pun of the initial value).
pub const CompactIntKeyHashTable = struct {
    const EMPTY_TAG: u64 = 0;
    const INITIAL_CAP = 64;

    /// Full AoS layout: entries[slot * entry_size] = tag (0=empty, else hash|bit63),
    /// entries[slot * entry_size + 1 .. + 1 + num_keys] = keys (as u64, bitcast from i64),
    /// entries[slot * entry_size + 1 + num_keys .. + entry_size] = vals.
    ///
    /// For Q33 (2 keys, 3 aggs): entry_size=6, 48B per slot ≤ 1 cache line (64B).
    /// A single DRAM miss fetches tag + keys + vals, so key compare AND value
    /// update after a tag hit are both "free" (already in the same cache line).
    /// This halves DRAM misses compared to SoA (tag[], keys[], vals[] separate).
    entries:    []u64,   // length = capacity * entry_size
    capacity:   usize,
    count:      usize,
    num_keys:   usize,
    num_aggs:   usize,
    entry_size: usize,   // = num_keys + num_aggs + 1  (cached)
    arena:      std.mem.Allocator,

    pub fn initWithCapacity(
        arena:    std.mem.Allocator,
        num_keys: usize,
        num_aggs: usize,
        est_rows: u64,
    ) !CompactIntKeyHashTable {
        const cap = if (est_rows > 0)
            nextPow2I(@as(usize, @intCast(@min(est_rows * 100 / 70 + 1, std.math.maxInt(u32)))))
        else INITIAL_CAP;
        const es      = num_keys + num_aggs + 1;
        const entries = try arena.alloc(u64, cap * es);
        // Zero entire entries so every tag starts as EMPTY_TAG (0).
        @memset(entries, 0);
        return .{
            .entries    = entries,
            .capacity   = cap,
            .count      = 0,
            .num_keys   = num_keys,
            .num_aggs   = num_aggs,
            .entry_size = es,
            .arena      = arena,
        };
    }

    fn nextPow2I(n: usize) usize {
        if (n <= 1) return 1;
        var p: usize = 1;
        while (p < n) p <<= 1;
        return p;
    }

    pub fn hashI64s(keys: []const i64) u64 {
        if (keys.len == 1) {
            var h: u64 = @bitCast(keys[0]);
            h ^= h >> 33; h *%= 0xff51afd7ed558ccd;
            h ^= h >> 33; h *%= 0xc4ceb9fe1a85ec53;
            h ^= h >> 33;
            return h | (1 << 63);
        }
        if (keys.len == 2) {
            const k0: u64 = @bitCast(keys[0]);
            const k1: u64 = @bitCast(keys[1]);
            var h: u64 = k0 *% 0x9e3779b97f4a7c15 ^ k1 *% 0x6c62272e07bb0142;
            h ^= h >> 30; h *%= 0xbf58476d1ce4e5b9;
            h ^= h >> 27; h *%= 0x94d049bb133111eb;
            h ^= h >> 31;
            return h | (1 << 63);
        }
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.sliceAsBytes(keys));
        return h.final() | (1 << 63);
    }

    /// Returns a pointer to the vals portion of the given key's slot,
    /// inserting with `init_vals` if not present.
    pub fn getOrInsert(
        self: *CompactIntKeyHashTable,
        key:       []const i64,
        init_vals: []const u64,
    ) ![]u64 {
        return self.getOrInsertH(key, hashI64s(key), init_vals);
    }

    /// Prefetch the expected HT cache line for `key` without probing.
    /// Call this PREFETCH_DIST iterations before getOrInsert to hide L3/DRAM latency.
    pub inline fn prefetchForKeys(self: *const CompactIntKeyHashTable, key0: i64, key1: i64) void {
        const k0: u64 = @bitCast(key0);
        const k1: u64 = @bitCast(key1);
        var h: u64 = k0 *% 0x9e3779b97f4a7c15 ^ k1 *% 0x6c62272e07bb0142;
        h ^= h >> 30; h *%= 0xbf58476d1ce4e5b9;
        h ^= h >> 27; h *%= 0x94d049bb133111eb;
        h ^= h >> 31;
        h |= (1 << 63);
        const slot = h & (self.capacity - 1);
        @prefetch(&self.entries[slot * self.entry_size], .{ .rw = .read, .locality = 1, .cache = .data });
    }

    /// Prefetch using a precomputed hash (for Phase 2 loops that already have the hash).
    pub inline fn prefetchH(self: *const CompactIntKeyHashTable, h: u64) void {
        const slot = h & (self.capacity - 1);
        @prefetch(&self.entries[slot * self.entry_size], .{ .rw = .read, .locality = 1, .cache = .data });
    }

    /// Prefetch variant for single key (Q16, Q36, etc.).
    pub inline fn prefetchForKey1(self: *const CompactIntKeyHashTable, key0: i64) void {
        var h: u64 = @bitCast(key0);
        h ^= h >> 33; h *%= 0xff51afd7ed558ccd;
        h ^= h >> 33; h *%= 0xc4ceb9fe1a85ec53;
        h ^= h >> 33;
        h |= (1 << 63);
        const slot = h & (self.capacity - 1);
        @prefetch(&self.entries[slot * self.entry_size], .{ .rw = .read, .locality = 1, .cache = .data });
    }

    /// Same as getOrInsert but takes precomputed hash (avoids rehashing during merge).
    pub fn getOrInsertH(
        self: *CompactIntKeyHashTable,
        key:       []const i64,
        h:         u64,
        init_vals: []const u64,
    ) ![]u64 {
        if (self.count + 1 > (self.capacity * 7) / 10) try self.grow();
        const mask = self.capacity - 1;
        const es   = self.entry_size;
        var   slot = h & mask;
        // Prefetch the next-in-line slot preemptively to hide collision chain latency.
        @prefetch(&self.entries[((slot +% 1) & mask) * es], .{ .rw = .read, .locality = 1, .cache = .data });
        while (true) : (slot = (slot + 1) & mask) {
            const base = slot * es;
            const tag  = self.entries[base];
            if (tag == EMPTY_TAG) {
                // Insert: write tag, keys, init vals — all contiguous in one cache line.
                self.entries[base] = h;
                const kb = base + 1;
                for (key, 0..) |k, i| self.entries[kb + i] = @bitCast(k);
                const vb = base + 1 + self.num_keys;
                @memcpy(self.entries[vb .. vb + self.num_aggs], init_vals);
                self.count += 1;
                return self.entries[vb .. vb + self.num_aggs];
            }
            if (tag == h) {
                // Potential match: compare keys (same cache line as tag).
                const kb = base + 1;
                var match = true;
                for (key, 0..) |k, i| {
                    if (self.entries[kb + i] != @as(u64, @bitCast(k))) {
                        match = false; break;
                    }
                }
                if (match) {
                    const vb = base + 1 + self.num_keys;
                    return self.entries[vb .. vb + self.num_aggs];
                }
            }
        }
    }

    fn grow(self: *CompactIntKeyHashTable) !void {
        try self.growTo(self.capacity * 2);
    }

    /// Grow (or pre-size) to at least `new_cap` slots. No-op if already large enough.
    pub fn growTo(self: *CompactIntKeyHashTable, new_cap: usize) !void {
        if (new_cap <= self.capacity) return;
        const actual_cap = nextPow2I(new_cap);
        const new_mask   = actual_cap - 1;
        const es         = self.entry_size;
        const new_entries = try self.arena.alloc(u64, actual_cap * es);
        @memset(new_entries, 0);
        for (0..self.capacity) |i| {
            const src_base = i * es;
            const h = self.entries[src_base];
            if (h == EMPTY_TAG) continue;
            var slot = h & new_mask;
            while (new_entries[slot * es] != EMPTY_TAG) : (slot = (slot + 1) & new_mask) {}
            const dst_base = slot * es;
            @memcpy(new_entries[dst_base .. dst_base + es],
                    self.entries[src_base .. src_base + es]);
        }
        self.entries  = new_entries;
        self.capacity = actual_cap;
    }

     /// Look up an existing entry by hash value only (no key comparison).
     /// Returns a mutable slice of the agg vals for the first slot whose tag equals `h`,
     /// or null if none is found (empty slot reached without a match).
     ///
     /// Safety: only call this after all groups have been inserted, and only when
     /// hash collisions within the same partition are negligible (64-bit hash,
     /// <10K groups per partition → collision prob ≈ 0).
     pub fn lookupByHash(self: *const CompactIntKeyHashTable, h: u64) ?[]u64 {
         const mask = self.capacity - 1;
         const es   = self.entry_size;
         var   slot = h & mask;
         while (true) : (slot = (slot + 1) & mask) {
             const base = slot * es;
             const tag  = self.entries[base];
             if (tag == EMPTY_TAG) return null;
             if (tag == h) {
                 const vb = base + 1 + self.num_keys;
                 return self.entries[vb .. vb + self.num_aggs];
             }
         }
     }

     pub fn iterate(
         self: *const CompactIntKeyHashTable,
         ctx:  anytype,
         comptime cb: fn (@TypeOf(ctx), []const i64, []const u64) void,
     ) void {
        const es = self.entry_size;
        var key_buf: [16]i64 = undefined;
        for (0..self.capacity) |i| {
            const base = i * es;
            if (self.entries[base] == EMPTY_TAG) continue;
            const kb = base + 1;
            for (0..self.num_keys) |ki| key_buf[ki] = @bitCast(self.entries[kb + ki]);
            const vb = base + 1 + self.num_keys;
            cb(ctx, key_buf[0..self.num_keys],
                    self.entries[vb .. vb + self.num_aggs]);
        }
    }

    /// Merge all entries from `self` into `master`, combining accumulators by kind.
    /// Uses precomputed hashes from entries to avoid rehashing.
    /// Uses software prefetch to hide DRAM latency during random master HT probes.
    pub fn mergeInto(
        self:      *const CompactIntKeyHashTable,
        master:    *CompactIntKeyHashTable,
        kinds:     []const CompactAggKind,
        init_vals: []const u64,
    ) !void {
        const pf_dist: usize = 32;
        const es      = self.entry_size;
        const m_es    = master.entry_size;
        const mask    = master.capacity - 1;
        var key_buf: [16]i64 = undefined;
        for (0..self.capacity) |i| {
            // Prefetch master entry for a future source entry.
            if (i + pf_dist < self.capacity) {
                const h_pf = self.entries[(i + pf_dist) * es];
                if (h_pf != EMPTY_TAG) {
                    @prefetch(master.entries.ptr + (h_pf & mask) * m_es,
                              .{ .rw = .read, .locality = 1, .cache = .data });
                }
            }
            const base = i * es;
            const h    = self.entries[base];
            if (h == EMPTY_TAG) continue;
            const kb = base + 1;
            for (0..self.num_keys) |ki| key_buf[ki] = @bitCast(self.entries[kb + ki]);
            const vb = base + 1 + self.num_keys;
            const local_vals = self.entries[vb .. vb + self.num_aggs];
            const master_vals = try master.getOrInsertH(key_buf[0..self.num_keys], h, init_vals);
            for (kinds, 0..) |kind, ci| {
                const src = local_vals[ci];
                switch (kind) {
                    .count, .u64_sum, .count_distinct_u64 => master_vals[ci] += src,
                    .i64_sum => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(a + b);
                    },
                    .f64_sum, .f64_str_len_sum => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        master_vals[ci] = @bitCast(a + b);
                    },
                    .i64_min => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(@min(a, b));
                    },
                    .i64_max => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(@max(a, b));
                    },
                    .u64_min => master_vals[ci] = @min(master_vals[ci], src),
                    .u64_max => master_vals[ci] = @max(master_vals[ci], src),
                    .f64_min => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b < a) master_vals[ci] = src;
                    },
                    .f64_max => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b > a) master_vals[ci] = src;
                    },
                    .str_min, .str_max => {},
                }
            }
        }
    }

    /// Like mergeInto, but only processes entries where `(h & part_mask) == part_id`.
    /// Used by partition-parallel merge to keep each partition HT L3-sized.
    pub fn mergeIntoPartitioned(
        self:      *const CompactIntKeyHashTable,
        master:    *CompactIntKeyHashTable,
        kinds:     []const CompactAggKind,
        init_vals: []const u64,
        part_id:   u64,
        part_mask: u64,
    ) !void {
        const es = self.entry_size;
        var key_buf: [16]i64 = undefined;
        for (0..self.capacity) |i| {
            const base = i * es;
            const h    = self.entries[base];
            if (h == EMPTY_TAG) continue;
            if (h & part_mask != part_id) continue;
            const kb = base + 1;
            for (0..self.num_keys) |ki| key_buf[ki] = @bitCast(self.entries[kb + ki]);
            const vb = base + 1 + self.num_keys;
            const local_vals = self.entries[vb .. vb + self.num_aggs];
            const master_vals = try master.getOrInsertH(key_buf[0..self.num_keys], h, init_vals);
            for (kinds, 0..) |kind, ci| {
                const src = local_vals[ci];
                switch (kind) {
                    .count, .u64_sum, .count_distinct_u64 => master_vals[ci] += src,
                    .i64_sum => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(a + b);
                    },
                    .f64_sum, .f64_str_len_sum => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        master_vals[ci] = @bitCast(a + b);
                    },
                    .i64_min => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(@min(a, b));
                    },
                    .i64_max => {
                        const a: i64 = @bitCast(master_vals[ci]);
                        const b: i64 = @bitCast(src);
                        master_vals[ci] = @bitCast(@max(a, b));
                    },
                    .u64_min => master_vals[ci] = @min(master_vals[ci], src),
                    .u64_max => master_vals[ci] = @max(master_vals[ci], src),
                    .f64_min => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b < a) master_vals[ci] = src;
                    },
                    .f64_max => {
                        const a: f64 = @bitCast(master_vals[ci]);
                        const b: f64 = @bitCast(src);
                        if (b > a) master_vals[ci] = src;
                    },
                    .str_min, .str_max => {},
                }
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
