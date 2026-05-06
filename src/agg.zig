const std = @import("std");

const empty_key = std.math.minInt(i64);

pub const I64CountTable = struct {
    keys: []i64,
    counts: []u32,
    occupied: []u32,
    len: usize = 0,
    mask: usize,
    max_load: usize,

    pub fn init(allocator: std.mem.Allocator, expected: usize) !I64CountTable {
        var capacity: usize = 1;
        const target = expected * 10 / 7 + 1;
        while (capacity < target) capacity <<= 1;
        capacity = @max(capacity, 1024);

        const keys = try allocator.alloc(i64, capacity);
        errdefer allocator.free(keys);
        const counts = try allocator.alloc(u32, capacity);
        errdefer allocator.free(counts);
        const occupied = try allocator.alloc(u32, capacity);
        errdefer allocator.free(occupied);
        @memset(keys, empty_key);
        @memset(counts, 0);
        return .{
            .keys = keys,
            .counts = counts,
            .occupied = occupied,
            .mask = capacity - 1,
            .max_load = capacity * 7 / 10,
        };
    }

    pub fn deinit(self: I64CountTable, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.counts);
        allocator.free(self.occupied);
    }

    pub fn add(self: *I64CountTable, allocator: std.mem.Allocator, key: i64) !void {
        if (key == empty_key) return error.InvalidKey;
        while (true) {
            var index = hashI64(key) & self.mask;
            while (true) {
                const existing = self.keys[index];
                if (existing == key) {
                    self.counts[index] += 1;
                    return;
                }
                if (existing == empty_key) {
                    if (self.len + 1 > self.max_load) {
                        try self.grow(allocator);
                        break;
                    }
                    self.keys[index] = key;
                    self.counts[index] = 1;
                    self.occupied[self.len] = @intCast(index);
                    self.len += 1;
                    return;
                }
                index = (index + 1) & self.mask;
            }
        }
    }

    pub fn addWithHash(self: *I64CountTable, allocator: std.mem.Allocator, key: i64, hash: usize) !void {
        if (key == empty_key) return error.InvalidKey;
        while (true) {
            var index = hash & self.mask;
            while (true) {
                const existing = self.keys[index];
                if (existing == key) {
                    self.counts[index] += 1;
                    return;
                }
                if (existing == empty_key) {
                    if (self.len + 1 > self.max_load) {
                        try self.grow(allocator);
                        break;
                    }
                    self.keys[index] = key;
                    self.counts[index] = 1;
                    self.occupied[self.len] = @intCast(index);
                    self.len += 1;
                    return;
                }
                index = (index + 1) & self.mask;
            }
        }
    }

    fn grow(self: *I64CountTable, allocator: std.mem.Allocator) !void {
        const old_keys = self.keys;
        const old_counts = self.counts;
        const old_occupied = self.occupied;
        const old_len = self.len;

        const new_capacity = old_keys.len * 2;
        const new_keys = try allocator.alloc(i64, new_capacity);
        errdefer allocator.free(new_keys);
        const new_counts = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_counts);
        const new_occupied = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_occupied);
        @memset(new_keys, empty_key);
        @memset(new_counts, 0);

        self.keys = new_keys;
        self.counts = new_counts;
        self.occupied = new_occupied;
        self.len = 0;
        self.mask = new_capacity - 1;
        self.max_load = new_capacity * 7 / 10;

        for (old_occupied[0..old_len]) |old_index| {
            self.insertExisting(old_keys[old_index], old_counts[old_index]);
        }

        allocator.free(old_keys);
        allocator.free(old_counts);
        allocator.free(old_occupied);
    }

    fn insertExisting(self: *I64CountTable, key: i64, count: u32) void {
        var index = hashI64(key) & self.mask;
        while (true) {
            if (self.keys[index] == empty_key) {
                self.keys[index] = key;
                self.counts[index] = count;
                self.occupied[self.len] = @intCast(index);
                self.len += 1;
                return;
            }
            index = (index + 1) & self.mask;
        }
    }
};

pub const UserCount = struct {
    user_id: i64,
    count: u32,
};

pub fn insertTop10(top: *[10]UserCount, top_len: *usize, row: UserCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and before(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn before(a: UserCount, b: UserCount) bool {
    if (a.count == b.count) return a.user_id < b.user_id;
    return a.count > b.count;
}

pub fn hashI64(value: i64) usize {
    var x: u64 = @bitCast(value);
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return @intCast(x);
}

test "i64 count table aggregates" {
    const allocator = std.testing.allocator;
    var table = try I64CountTable.init(allocator, 4);
    defer table.deinit(allocator);
    try table.add(allocator, 1);
    try table.add(allocator, 2);
    try table.add(allocator, 1);
    try std.testing.expectEqual(@as(usize, 2), table.len);
}

const empty_u64_key: u64 = std.math.maxInt(u64);

/// Open-addressing u64 -> u32 count map. Sentinel `maxInt(u64)` marks empty
/// slots; verify your hash domain never produces that value before adopting.
/// 70% load factor; doubles on overflow.
pub const U64CountTable = struct {
    keys: []u64,
    counts: []u32,
    occupied: []u32,
    len: usize = 0,
    mask: usize,
    max_load: usize,

    pub fn init(allocator: std.mem.Allocator, expected: usize) !U64CountTable {
        var capacity: usize = 1;
        const target = expected * 10 / 7 + 1;
        while (capacity < target) capacity <<= 1;
        capacity = @max(capacity, 1024);

        const keys = try allocator.alloc(u64, capacity);
        errdefer allocator.free(keys);
        const counts = try allocator.alloc(u32, capacity);
        errdefer allocator.free(counts);
        const occupied = try allocator.alloc(u32, capacity);
        errdefer allocator.free(occupied);
        @memset(keys, empty_u64_key);
        @memset(counts, 0);
        return .{
            .keys = keys,
            .counts = counts,
            .occupied = occupied,
            .mask = capacity - 1,
            .max_load = capacity * 7 / 10,
        };
    }

    pub fn deinit(self: U64CountTable, allocator: std.mem.Allocator) void {
        allocator.free(self.keys);
        allocator.free(self.counts);
        allocator.free(self.occupied);
    }

    pub fn add(self: *U64CountTable, allocator: std.mem.Allocator, key: u64) !void {
        if (key == empty_u64_key) return error.InvalidKey;
        while (true) {
            // Hash already produced; mix once more to spread bits across mask.
            var index: usize = @intCast(key *% 0x9e3779b97f4a7c15);
            index &= self.mask;
            while (true) {
                const existing = self.keys[index];
                if (existing == key) {
                    self.counts[index] += 1;
                    return;
                }
                if (existing == empty_u64_key) {
                    if (self.len + 1 > self.max_load) {
                        try self.grow(allocator);
                        break;
                    }
                    self.keys[index] = key;
                    self.counts[index] = 1;
                    self.occupied[self.len] = @intCast(index);
                    self.len += 1;
                    return;
                }
                index = (index + 1) & self.mask;
            }
        }
    }

    fn grow(self: *U64CountTable, allocator: std.mem.Allocator) !void {
        const old_keys = self.keys;
        const old_counts = self.counts;
        const old_occupied = self.occupied;
        const old_len = self.len;

        const new_capacity = old_keys.len * 2;
        const new_keys = try allocator.alloc(u64, new_capacity);
        errdefer allocator.free(new_keys);
        const new_counts = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_counts);
        const new_occupied = try allocator.alloc(u32, new_capacity);
        errdefer allocator.free(new_occupied);
        @memset(new_keys, empty_u64_key);
        @memset(new_counts, 0);

        self.keys = new_keys;
        self.counts = new_counts;
        self.occupied = new_occupied;
        self.len = 0;
        self.mask = new_capacity - 1;
        self.max_load = new_capacity * 7 / 10;

        for (old_occupied[0..old_len]) |old_index| {
            self.insertExisting(old_keys[old_index], old_counts[old_index]);
        }

        allocator.free(old_keys);
        allocator.free(old_counts);
        allocator.free(old_occupied);
    }

    fn insertExisting(self: *U64CountTable, key: u64, count: u32) void {
        var index: usize = @intCast(key *% 0x9e3779b97f4a7c15);
        index &= self.mask;
        while (true) {
            if (self.keys[index] == empty_u64_key) {
                self.keys[index] = key;
                self.counts[index] = count;
                self.occupied[self.len] = @intCast(index);
                self.len += 1;
                return;
            }
            index = (index + 1) & self.mask;
        }
    }
};

pub const HashCount = struct {
    hash: u64,
    count: u32,
};

/// Maintain top-k by count desc, hash asc tiebreak. k is small (usually 10);
/// linear insertion is faster than a heap for k<=32.
pub fn insertTopK(top: []HashCount, top_len: *usize, k: usize, row: HashCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and beforeHash(top[pos], row)) : (pos += 1) {}
    if (pos >= k) return;
    if (top_len.* < k) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

fn beforeHash(a: HashCount, b: HashCount) bool {
    if (a.count == b.count) return a.hash < b.hash;
    return a.count > b.count;
}

test "u64 count table aggregates" {
    const allocator = std.testing.allocator;
    var table = try U64CountTable.init(allocator, 4);
    defer table.deinit(allocator);
    try table.add(allocator, 7);
    try table.add(allocator, 9);
    try table.add(allocator, 7);
    try table.add(allocator, 7);
    try std.testing.expectEqual(@as(usize, 2), table.len);
    // Find slot for key=7 and confirm count=3.
    var seen_seven: bool = false;
    var seen_nine: bool = false;
    for (table.occupied[0..table.len]) |idx| {
        if (table.keys[idx] == 7) {
            try std.testing.expectEqual(@as(u32, 3), table.counts[idx]);
            seen_seven = true;
        } else if (table.keys[idx] == 9) {
            try std.testing.expectEqual(@as(u32, 1), table.counts[idx]);
            seen_nine = true;
        }
    }
    try std.testing.expect(seen_seven and seen_nine);
}

test "insertTopK keeps best by count desc, hash asc" {
    var top: [3]HashCount = undefined;
    var n: usize = 0;
    insertTopK(&top, &n, 3, .{ .hash = 1, .count = 5 });
    insertTopK(&top, &n, 3, .{ .hash = 2, .count = 10 });
    insertTopK(&top, &n, 3, .{ .hash = 3, .count = 5 });
    insertTopK(&top, &n, 3, .{ .hash = 4, .count = 10 });
    insertTopK(&top, &n, 3, .{ .hash = 5, .count = 1 });
    try std.testing.expectEqual(@as(usize, 3), n);
    try std.testing.expectEqual(@as(u64, 2), top[0].hash);
    try std.testing.expectEqual(@as(u64, 4), top[1].hash);
    try std.testing.expectEqual(@as(u64, 1), top[2].hash);
}
