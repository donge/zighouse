/// Part scanner: enumerates ClickHouse MergeTree part directories under
///   <data_dir>/<db>/<table>/parts/
///
/// Part naming convention: all_{min_seq}_{max_seq}_{level}
///   - Fresh INSERT parts: all_N_N_0  (min==max, level==0)
///   - Merged parts:       all_3_7_1  (covers seq 3..7, merge level 1)
///
/// Sorting: by min_seq ascending (numeric, not lexicographic).
/// Overlap filtering: if all_3_7_1 exists, constituent parts
///   all_3_3_0..all_7_7_0 are filtered out.
/// Directories with prefix "tmp_" are always ignored (in-progress writes).
///
/// Usage:
///   var parts = try part_scanner.scan(allocator, io, data_dir, db, table_name);
///   defer parts.deinit();
///   for (parts.dirs()) |dir| { ... }           // []const []const u8
///   for (parts.metas()) |m| { _ = m.min_seq; } // []const PartMeta

const std = @import("std");

/// Parsed metadata for a single part directory.
pub const PartMeta = struct {
    min_seq:   u64,
    max_seq:   u64,
    level:     u32,
    /// Full path to the part directory (owned by PartList).
    full_path: []const u8,
};

/// Parse "all_{min}_{max}_{level}" → fields.  Returns null on mismatch.
pub fn parseName(name: []const u8) ?struct { min_seq: u64, max_seq: u64, level: u32 } {
    if (!std.mem.startsWith(u8, name, "all_")) return null;
    var rest = name[4..]; // skip "all_"

    const us1 = std.mem.indexOfScalar(u8, rest, '_') orelse return null;
    const min_seq = std.fmt.parseInt(u64, rest[0..us1], 10) catch return null;
    rest = rest[us1 + 1 ..];

    const us2 = std.mem.indexOfScalar(u8, rest, '_') orelse return null;
    const max_seq = std.fmt.parseInt(u64, rest[0..us2], 10) catch return null;
    rest = rest[us2 + 1 ..];

    const level = std.fmt.parseInt(u32, rest, 10) catch return null;
    return .{ .min_seq = min_seq, .max_seq = max_seq, .level = level };
}

pub const PartList = struct {
    allocator: std.mem.Allocator,
    /// Sorted, overlap-filtered part metadata.  Owns full_path strings.
    _metas: std.ArrayList(PartMeta),
    /// Parallel slice of full_path pointers into _metas, kept in sync.
    /// Provides zero-allocation dirs() return.
    _dirs: std.ArrayList([]const u8),

    pub fn deinit(self: *PartList) void {
        for (self._metas.items) |m| self.allocator.free(m.full_path);
        self._metas.deinit(self.allocator);
        self._dirs.deinit(self.allocator);
    }

    /// Sorted, overlap-filtered part directory paths.
    /// Slice is valid until PartList.deinit().
    pub fn dirs(self: *const PartList) []const []const u8 {
        return self._dirs.items;
    }

    /// Sorted, overlap-filtered part metadata.
    /// Slice is valid until PartList.deinit().
    pub fn metas(self: *const PartList) []const PartMeta {
        return self._metas.items;
    }
};

/// Scan <data_dir>/<db>/<table>/parts/ and return all valid part directories.
/// - Ignores entries not matching "all_{min}_{max}_{level}" or starting with "tmp_".
/// - Sorts by min_seq ascending (numeric); ties broken by level descending.
/// - Filters out parts whose [min,max] range is fully covered by a
///   higher-level merged part.
pub fn scan(
    allocator: std.mem.Allocator,
    io:        std.Io,
    data_dir:  []const u8,
    db:        []const u8,
    table_name: []const u8,
) !PartList {
    var result = PartList{
        .allocator = allocator,
        ._metas    = .empty,
        ._dirs     = .empty,
    };
    errdefer result.deinit();

    const parts_path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/parts", .{ data_dir, db, table_name });
    defer allocator.free(parts_path);

    var parts_dir = std.Io.Dir.cwd().openDir(io, parts_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return result,
        else => return err,
    };
    defer parts_dir.close(io);

    // ── Collect valid entries ─────────────────────────────────────────────────
    var iter = parts_dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .directory) continue;
        if (std.mem.startsWith(u8, entry.name, "tmp_")) continue; // in-progress write

        const parsed = parseName(entry.name) orelse continue;

        const full_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ parts_path, entry.name });
        errdefer allocator.free(full_path);

        try result._metas.append(allocator, .{
            .min_seq   = parsed.min_seq,
            .max_seq   = parsed.max_seq,
            .level     = parsed.level,
            .full_path = full_path,
        });
    }

    // ── Sort by min_seq asc; same min_seq → level desc ────────────────────────
    std.mem.sort(PartMeta, result._metas.items, {}, struct {
        fn lt(_: void, a: PartMeta, b: PartMeta) bool {
            if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
            return a.level > b.level;
        }
    }.lt);

    // ── Overlap filter ────────────────────────────────────────────────────────
    // O(n²) — expected n < a few hundred.
    // A part A is covered if there exists part B with:
    //   B.level > A.level  AND  B.min_seq <= A.min_seq  AND  B.max_seq >= A.max_seq
    const n = result._metas.items.len;
    var keep = try allocator.alloc(bool, n);
    defer allocator.free(keep);
    for (keep) |*k| k.* = true;

    for (0..n) |i| {
        if (!keep[i]) continue;
        const a = result._metas.items[i];
        for (0..n) |j| {
            if (i == j or !keep[j]) continue;
            const b = result._metas.items[j];
            if (b.level > a.level and b.min_seq <= a.min_seq and b.max_seq >= a.max_seq) {
                keep[i] = false;
                break;
            }
        }
    }

    // Compact _metas in place, free covered entries.
    var write: usize = 0;
    for (0..n) |i| {
        if (keep[i]) {
            result._metas.items[write] = result._metas.items[i];
            write += 1;
        } else {
            allocator.free(result._metas.items[i].full_path);
        }
    }
    result._metas.shrinkRetainingCapacity(write);

    // ── Build parallel _dirs slice ────────────────────────────────────────────
    try result._dirs.ensureTotalCapacity(allocator, result._metas.items.len);
    for (result._metas.items) |m| {
        result._dirs.appendAssumeCapacity(m.full_path);
    }

    return result;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parseName: basic cases" {
    const t = std.testing;

    const r1 = parseName("all_1_1_0").?;
    try t.expectEqual(@as(u64, 1), r1.min_seq);
    try t.expectEqual(@as(u64, 1), r1.max_seq);
    try t.expectEqual(@as(u32, 0), r1.level);

    const r2 = parseName("all_3_7_1").?;
    try t.expectEqual(@as(u64, 3), r2.min_seq);
    try t.expectEqual(@as(u64, 7), r2.max_seq);
    try t.expectEqual(@as(u32, 1), r2.level);

    try t.expect(parseName("tmp_abc")  == null);
    try t.expect(parseName("all_")     == null);
    try t.expect(parseName("other")    == null);
    try t.expect(parseName("all_1_1")  == null); // missing level
}

test "scan: empty when parts dir missing" {
    var parts = try scan(std.testing.allocator, std.testing.io, "/nonexistent", "db", "t");
    defer parts.deinit();
    try std.testing.expectEqual(@as(usize, 0), parts.dirs().len);
    try std.testing.expectEqual(@as(usize, 0), parts.metas().len);
}

test "numeric sort: all_10 comes after all_2" {
    var arr = [_]PartMeta{
        .{ .min_seq = 10, .max_seq = 10, .level = 0, .full_path = "" },
        .{ .min_seq = 2,  .max_seq = 2,  .level = 0, .full_path = "" },
        .{ .min_seq = 1,  .max_seq = 1,  .level = 0, .full_path = "" },
    };
    std.mem.sort(PartMeta, &arr, {}, struct {
        fn lt(_: void, a: PartMeta, b: PartMeta) bool {
            if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
            return a.level > b.level;
        }
    }.lt);
    try std.testing.expectEqual(@as(u64, 1),  arr[0].min_seq);
    try std.testing.expectEqual(@as(u64, 2),  arr[1].min_seq);
    try std.testing.expectEqual(@as(u64, 10), arr[2].min_seq);
}

test "overlap filter: all_3_7_1 hides level-0 parts 3..7" {
    const metas_arr = [_]PartMeta{
        .{ .min_seq = 1, .max_seq = 1, .level = 0, .full_path = "" },
        .{ .min_seq = 2, .max_seq = 2, .level = 0, .full_path = "" },
        .{ .min_seq = 3, .max_seq = 3, .level = 0, .full_path = "" },
        .{ .min_seq = 4, .max_seq = 4, .level = 0, .full_path = "" },
        .{ .min_seq = 5, .max_seq = 5, .level = 0, .full_path = "" },
        .{ .min_seq = 6, .max_seq = 6, .level = 0, .full_path = "" },
        .{ .min_seq = 7, .max_seq = 7, .level = 0, .full_path = "" },
        .{ .min_seq = 3, .max_seq = 7, .level = 1, .full_path = "" }, // merged
        .{ .min_seq = 8, .max_seq = 8, .level = 0, .full_path = "" },
    };
    const n = metas_arr.len;
    var keep = [_]bool{true} ** 9;
    for (0..n) |i| {
        const a = metas_arr[i];
        for (0..n) |j| {
            if (i == j or !keep[j]) continue;
            const b = metas_arr[j];
            if (b.level > a.level and b.min_seq <= a.min_seq and b.max_seq >= a.max_seq) {
                keep[i] = false;
                break;
            }
        }
    }
    var survived: usize = 0;
    var has_merged = false;
    for (0..n) |i| {
        if (!keep[i]) continue;
        survived += 1;
        const m = metas_arr[i];
        if (m.min_seq == 3 and m.max_seq == 7) has_merged = true;
        if (m.level == 0) {
            // level-0 seqs 3..7 must NOT survive
            try std.testing.expect(m.min_seq < 3 or m.min_seq > 7);
        }
    }
    // Survivors: all_1, all_2, all_3_7_1, all_8 = 4
    try std.testing.expectEqual(@as(usize, 4), survived);
    try std.testing.expect(has_merged);
}
