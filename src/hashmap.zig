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

    /// Insert key as a set member.  Returns true if this is the first time
    /// `key` was seen (newly inserted), false if it was already present.
    /// Used for COUNT(DISTINCT) deduplication — 3-5x faster than AutoHashMap
    /// because of flat arrays, inline probing, and no allocation overhead.
    /// Caller must ensure load factor stays <= ~0.85 (same as `bump`).
    /// The extremely rare case of key == empty_key is remapped to empty_key^1
    /// (probability 1/2^64; negligible false-"already-seen" risk).
    pub inline fn bumpNew(self: *HashU64Count, key_in: u64) bool {
        const key: u64 = if (key_in == empty_key) key_in ^ 1 else key_in;
        var idx = hash(key) & self.mask;
        while (true) {
            const slot = self.keys[idx];
            if (slot == key) return false; // already present
            if (slot == empty_key) {
                self.keys[idx] = key;
                self.values[idx] = 1;
                self.len += 1;
                return true; // newly inserted
            }
            idx = (idx + 1) & self.mask;
        }
    }

    /// Reset the hash set in-place, keeping the allocated memory.
    /// O(capacity) — amortised O(1) per insert when used across partitions.
    pub fn clearAndReset(self: *HashU64Count) void {
        @memset(self.keys, empty_key);
        self.len = 0;
        // values[] don't need clearing — they're only read when keys[] != empty_key.
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

/// Flat hash set for COUNT(DISTINCT) deduplication with O(1) per-partition
/// clear via epoch stamps instead of memset.
///
/// Each slot holds a (epoch: u32, key: u64) pair.  A slot is "occupied in the
/// current epoch" iff epochs[slot] == current_epoch; any other epoch value
/// means the slot is treated as empty.  On clearForNextPartition() we just
/// increment current_epoch — no memset of keys or epochs needed.
///
/// Correctness of linear probing with epoch-based empties:
///   A key K is stored at slot s where hash(K) ≤ s (mod cap), and every slot
///   between hash(K) and s carries an entry that was inserted in this epoch
///   AFTER the "empty" slots in that range were claimed.  When we see an
///   epoch-mismatch slot we treat it as empty and stop — keys from the current
///   epoch cannot appear beyond such a slot.
///
/// Growth: starts small (initial_cap=32 → actual cap=64), doubles on demand.
/// Typical usage: 1-8 doublings during the first partition, then stable.
/// clearForNextPartition resets len to 0 and bumps epoch in O(1).
pub const DistinctEpochSet = struct {
    keys: []u64,
    epochs: []u32,
    capacity: usize,
    mask: u64,
    len: usize,
    current_epoch: u32,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, initial_cap: usize) !DistinctEpochSet {
        var cap: usize = 16;
        const target = if (initial_cap < std.math.maxInt(usize) / 2) initial_cap * 2 else initial_cap;
        while (cap < target) cap <<= 1;
        const keys = try allocator.alloc(u64, cap);
        const epochs = try allocator.alloc(u32, cap);
        @memset(epochs, 0); // epoch=0 means never touched; current starts at 1
        return .{
            .keys = keys,
            .epochs = epochs,
            .capacity = cap,
            .mask = @as(u64, cap - 1),
            .len = 0,
            .current_epoch = 1,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *DistinctEpochSet) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.epochs);
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

    /// Insert key.  Returns true if newly seen in current epoch, false if duplicate.
    /// Caller must ensure load factor < 75% before calling (check needsGrow first).
    pub inline fn insertNew(self: *DistinctEpochSet, key_in: u64) bool {
        const epoch = self.current_epoch;
        var idx = hash(key_in) & self.mask;
        while (true) {
            const e = self.epochs[idx];
            if (e == epoch) {
                if (self.keys[idx] == key_in) return false; // already present
                idx = (idx + 1) & self.mask;
                continue;
            }
            // Empty slot (previous epoch) — insert here.
            self.keys[idx] = key_in;
            self.epochs[idx] = epoch;
            self.len += 1;
            return true;
        }
    }

    /// True when load factor ≥ 75% and a growDouble() is needed before the next insert.
    pub inline fn needsGrow(self: *const DistinctEpochSet) bool {
        return self.len * 4 >= self.capacity * 3;
    }

    /// Double capacity, rehashing all current-epoch entries.
    pub fn growDouble(self: *DistinctEpochSet) !void {
        const new_cap = self.capacity * 2;
        const new_keys = try self.allocator.alloc(u64, new_cap);
        const new_epochs = try self.allocator.alloc(u32, new_cap);
        @memset(new_epochs, 0);
        const new_mask = @as(u64, new_cap - 1);
        const epoch = self.current_epoch;
        for (0..self.capacity) |i| {
            if (self.epochs[i] != epoch) continue;
            var idx = hash(self.keys[i]) & new_mask;
            while (new_epochs[idx] != 0) idx = (idx + 1) & new_mask;
            new_keys[idx] = self.keys[i];
            new_epochs[idx] = epoch;
        }
        self.allocator.free(self.keys);
        self.allocator.free(self.epochs);
        self.keys = new_keys;
        self.epochs = new_epochs;
        self.capacity = new_cap;
        self.mask = new_mask;
    }

    /// O(1) clear: increment epoch so all existing slots appear empty.
    /// On u32 overflow (after 2^32 clears) fall back to a full @memset.
    pub fn clearForNextPartition(self: *DistinctEpochSet) void {
        self.len = 0;
        self.current_epoch +%= 1;
        if (self.current_epoch == 0) {
            self.current_epoch = 1;
            @memset(self.epochs, 0);
        }
    }
};

test "DistinctEpochSet basic" {
    const t = std.testing;
    var s = try DistinctEpochSet.init(t.allocator, 8);
    defer s.deinit();

    try t.expect(s.insertNew(42));  // new
    try t.expect(!s.insertNew(42)); // dup
    try t.expect(s.insertNew(7));   // new
    try t.expect(!s.insertNew(7));  // dup
    try t.expectEqual(@as(usize, 2), s.len);

    s.clearForNextPartition();
    try t.expectEqual(@as(usize, 0), s.len);
    try t.expect(s.insertNew(42));  // new again after clear
    try t.expect(s.insertNew(7));   // new again after clear
    try t.expectEqual(@as(usize, 2), s.len);
}

test "DistinctEpochSet grow" {
    const t = std.testing;
    var s = try DistinctEpochSet.init(t.allocator, 4);
    defer s.deinit();

    var i: u64 = 1;
    while (i <= 200) : (i += 1) {
        if (s.needsGrow()) try s.growDouble();
        _ = s.insertNew(i);
    }
    try t.expectEqual(@as(usize, 200), s.len);

    s.clearForNextPartition();
    i = 1;
    while (i <= 200) : (i += 1) {
        try t.expect(s.insertNew(i));   // all new after clear
    }
    i = 1;
    while (i <= 200) : (i += 1) {
        try t.expect(!s.insertNew(i));  // all dups now
    }
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

