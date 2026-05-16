/// Part scanner: enumerates ClickHouse MergeTree part directories under
///   <data_dir>/<db>/<table>/parts/
///
/// Returns all directories matching the pattern all_<N>_<N>_0 sorted by N.
///
/// Usage:
///   var parts = try part_scanner.scan(allocator, io, data_dir, db, table_name);
///   defer parts.deinit();
///   for (parts.dirs()) |dir| { ... }

const std = @import("std");

pub const PartList = struct {
    allocator: std.mem.Allocator,
    /// Sorted part directory paths (absolute or relative, owned).
    _dirs: std.ArrayList([]const u8),

    pub fn deinit(self: *PartList) void {
        for (self._dirs.items) |d| self.allocator.free(d);
        self._dirs.deinit(self.allocator);
    }

    pub fn dirs(self: *const PartList) []const []const u8 {
        return self._dirs.items;
    }
};

/// Scan <data_dir>/<db>/<table>/parts/ and return all valid part directories
/// (directories whose name starts with "all_").
/// Returns an empty list if the parts directory doesn't exist.
pub fn scan(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    db: []const u8,
    table_name: []const u8,
) !PartList {
    var result = PartList{
        .allocator = allocator,
        ._dirs = .empty,
    };
    errdefer result.deinit();

    const parts_path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/parts", .{ data_dir, db, table_name });
    defer allocator.free(parts_path);

    var parts_dir = std.Io.Dir.cwd().openDir(io, parts_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return result,
        else => return err,
    };
    defer parts_dir.close(io);

    var iter = parts_dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .directory) continue;
        if (!std.mem.startsWith(u8, entry.name, "all_")) continue;

        const full_path = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}",
            .{ parts_path, entry.name },
        );
        errdefer allocator.free(full_path);
        try result._dirs.append(allocator, full_path);
    }

    // Sort by part name for deterministic ordering.
    std.mem.sort([]const u8, result._dirs.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            // Compare just the base names (last component).
            const a_base = std.fs.path.basename(a);
            const b_base = std.fs.path.basename(b);
            return std.mem.lessThan(u8, a_base, b_base);
        }
    }.lt);

    return result;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "scan: empty when parts dir missing" {
    var parts = try scan(std.testing.allocator, std.testing.io, "/nonexistent", "db", "t");
    defer parts.deinit();
    try std.testing.expectEqual(@as(usize, 0), parts.dirs().len);
}
