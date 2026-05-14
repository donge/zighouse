/// Catalog: runtime table registry.
///
/// Holds an ordered list of TableEntry values.  Each entry carries the
/// table schema (schema.Table), an optional on-disk store directory, and
/// a StoreLayout tag that tells query and import code which physical
/// layout the store uses.
///
/// Two layouts coexist:
///   .clickbench_hot  — legacy ZigHouse ClickBench hot-column layout
///                      (hot_*.bin files written by importClickBenchParquet*)
///   .generic_part    — new ClickHouse-style part layout
///                      (<store>/<table>/parts/all_1_1_0/<col>.bin + columns.txt)
///
/// The ClickBench `hits` table is registered as a builtin entry with layout
/// .clickbench_hot so existing specialized/generic paths continue to work
/// unchanged.

const std = @import("std");
const schema = @import("schema.zig");
const clickbench_schema = @import("clickbench/schema.zig");

pub const StoreLayout = enum {
    /// Legacy ZigHouse ClickBench hot-column files (hot_*.bin, *.id, *.dict.tsv …)
    clickbench_hot,
    /// New ClickHouse-style part directory (<col>.bin + columns.txt + count.txt)
    generic_part,
};

pub const TableEntry = struct {
    table: schema.Table,
    /// Directory where this table's store lives.  null for in-memory / not-yet-imported tables.
    store_dir: ?[]const u8,
    layout: StoreLayout,
};

pub const Catalog = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayListUnmanaged(TableEntry),

    pub fn init(allocator: std.mem.Allocator) Catalog {
        return .{
            .allocator = allocator,
            .entries = .empty,
        };
    }

    pub fn deinit(self: *Catalog) void {
        for (self.entries.items) |entry| {
            if (entry.store_dir) |d| self.allocator.free(d);
        }
        self.entries.deinit(self.allocator);
    }

    /// Register a table.  Duplicates (same name, case-insensitive) are replaced.
    pub fn register(self: *Catalog, table: schema.Table, store_dir: ?[]const u8, layout: StoreLayout) !void {
        const dir_copy = if (store_dir) |d| try self.allocator.dupe(u8, d) else null;
        errdefer if (dir_copy) |d| self.allocator.free(d);

        // Replace existing entry with the same name.
        for (self.entries.items) |*entry| {
            if (schema.asciiEqlIgnoreCase(entry.table.name, table.name)) {
                if (entry.store_dir) |old| self.allocator.free(old);
                entry.table = table;
                entry.store_dir = dir_copy;
                entry.layout = layout;
                return;
            }
        }
        try self.entries.append(self.allocator, .{ .table = table, .store_dir = dir_copy, .layout = layout });
    }

    /// Look up a table by name (case-insensitive).  Returns null if not found.
    pub fn find(self: *const Catalog, name: []const u8) ?*const TableEntry {
        for (self.entries.items) |*entry| {
            if (schema.asciiEqlIgnoreCase(entry.table.name, name)) return entry;
        }
        return null;
    }

    /// Register the built-in ClickBench `hits` table with the given store
    /// directory.  If store_dir is null the table is registered without a
    /// backing store (schema-only; queries against it will fail at execution
    /// time unless a parquet path is provided separately).
    pub fn registerHits(self: *Catalog, store_dir: ?[]const u8) !void {
        try self.register(clickbench_schema.hits, store_dir, .clickbench_hot);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "catalog register and find" {
    const allocator = std.testing.allocator;
    var cat = Catalog.init(allocator);
    defer cat.deinit();

    const col = schema.Column{ .name = "id", .ty = .int64 };
    const table = schema.Table{ .name = "orders", .columns = &[_]schema.Column{col} };

    try cat.register(table, "/tmp/store", .generic_part);
    const entry = cat.find("orders") orelse return error.TestExpectedEntry;
    try std.testing.expectEqualStrings("orders", entry.table.name);
    try std.testing.expectEqualStrings("/tmp/store", entry.store_dir.?);
    try std.testing.expect(entry.layout == .generic_part);
}

test "catalog find is case insensitive" {
    const allocator = std.testing.allocator;
    var cat = Catalog.init(allocator);
    defer cat.deinit();

    const col = schema.Column{ .name = "x", .ty = .int32 };
    const table = schema.Table{ .name = "Hits", .columns = &[_]schema.Column{col} };

    try cat.register(table, null, .clickbench_hot);
    try std.testing.expect(cat.find("hits") != null);
    try std.testing.expect(cat.find("HITS") != null);
    try std.testing.expect(cat.find("unknown") == null);
}

test "catalog replace entry on duplicate name" {
    const allocator = std.testing.allocator;
    var cat = Catalog.init(allocator);
    defer cat.deinit();

    const col = schema.Column{ .name = "x", .ty = .int32 };
    const table1 = schema.Table{ .name = "foo", .columns = &[_]schema.Column{col} };
    const table2 = schema.Table{ .name = "foo", .columns = &[_]schema.Column{col} };

    try cat.register(table1, "/a", .generic_part);
    try cat.register(table2, "/b", .generic_part);
    try std.testing.expectEqual(@as(usize, 1), cat.entries.items.len);
    try std.testing.expectEqualStrings("/b", cat.find("foo").?.store_dir.?);
}

test "catalog registerHits populates hits schema" {
    const allocator = std.testing.allocator;
    var cat = Catalog.init(allocator);
    defer cat.deinit();

    try cat.registerHits(null);
    const entry = cat.find("hits") orelse return error.TestExpectedHits;
    try std.testing.expectEqualStrings("hits", entry.table.name);
    try std.testing.expect(entry.table.columns.len > 0);
    try std.testing.expect(entry.layout == .clickbench_hot);
}
