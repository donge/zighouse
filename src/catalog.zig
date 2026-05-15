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
const schema = @import("schema");
const schema_infer = @import("schema_infer.zig");
const clickbench_schema = schema.clickbench;

/// Name of the per-table catalog manifest file stored inside the part dir.
/// Format:
///   table=<name>\n
///   parquet=<absolute_or_relative_path>\n
///   part_format=<generic|ch_mergetree>\n
pub const catalog_manifest_name = "catalog.zig-house";

/// Part storage format tag.
pub const PartFormat = enum {
    /// ZigHouse generic store: raw LE binary, no compression, no marks.
    generic,
    /// ClickHouse MergeTree-compatible: LZ4 compressed .bin, .mrk2, checksums.txt.
    ch_mergetree,
};

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
    part_format: PartFormat = .generic,
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
        try self.registerFmt(table, store_dir, layout, .generic);
    }

    /// Register a table with explicit part format.
    pub fn registerFmt(self: *Catalog, table: schema.Table, store_dir: ?[]const u8, layout: StoreLayout, part_format: PartFormat) !void {
        const dir_copy = if (store_dir) |d| try self.allocator.dupe(u8, d) else null;
        errdefer if (dir_copy) |d| self.allocator.free(d);

        // Replace existing entry with the same name.
        for (self.entries.items) |*entry| {
            if (schema.asciiEqlIgnoreCase(entry.table.name, table.name)) {
                if (entry.store_dir) |old| self.allocator.free(old);
                entry.table = table;
                entry.store_dir = dir_copy;
                entry.layout = layout;
                entry.part_format = part_format;
                return;
            }
        }
        try self.entries.append(self.allocator, .{ .table = table, .store_dir = dir_copy, .layout = layout, .part_format = part_format });
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

    /// Write a catalog manifest for a generic_part table into
    ///   <store_dir>/<table_name>/parts/all_1_1_0/catalog.zig-house
    /// The manifest records the table name, original parquet source path, and
    /// part format so that `restoreFromStore` can reconstruct the catalog entry.
    pub fn writeManifest(
        io: std.Io,
        allocator: std.mem.Allocator,
        store_dir: []const u8,
        table_name: []const u8,
        parquet_path: []const u8,
        part_format: PartFormat,
    ) !void {
        const manifest_path = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/parts/all_1_1_0/{s}",
            .{ store_dir, table_name, catalog_manifest_name },
        );
        defer allocator.free(manifest_path);

        const fmt_str: []const u8 = switch (part_format) {
            .generic => "generic",
            .ch_mergetree => "ch_mergetree",
        };
        const content = try std.fmt.allocPrint(
            allocator,
            "table={s}\nparquet={s}\npart_format={s}\n",
            .{ table_name, parquet_path, fmt_str },
        );
        defer allocator.free(content);

        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = manifest_path, .data = content });
    }

    /// Scan `store_dir` for generic_part catalog manifests and register each
    /// table found.  For each <store_dir>/<table>/parts/all_1_1_0/catalog.zig-house
    /// the schema is re-inferred from the recorded parquet path and the table is
    /// registered with layout .generic_part.
    ///
    /// Errors opening or parsing individual manifests are silently skipped so
    /// that a partially-imported store doesn't prevent other tables from loading.
    pub fn restoreFromStore(
        self: *Catalog,
        allocator: std.mem.Allocator,
        io: std.Io,
        store_dir: []const u8,
    ) !void {
        var dir = std.Io.Dir.cwd().openDir(io, store_dir, .{ .iterate = true }) catch return;
        defer dir.close(io);

        var iter = dir.iterate(io);
        while (try iter.next()) |entry| {
            if (entry.kind != .directory) continue;
            const table_name = entry.name;

            const manifest_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}/parts/all_1_1_0/{s}",
                .{ store_dir, table_name, catalog_manifest_name },
            );
            defer allocator.free(manifest_path);

            const content = std.Io.Dir.cwd().readFileAlloc(
                io,
                manifest_path,
                allocator,
                .limited(4096),
            ) catch continue;
            defer allocator.free(content);

            const parquet_path = parseManifestField(content, "parquet") orelse continue;

            const fmt_str = parseManifestField(content, "part_format") orelse "generic";
            const part_format: PartFormat = if (std.mem.eql(u8, fmt_str, "ch_mergetree"))
                .ch_mergetree
            else
                .generic;

            var inferred = schema_infer.inferSchema(allocator, io, parquet_path, table_name) catch continue;
            defer inferred.deinit();

            self.registerFmt(inferred.table, store_dir, .generic_part, part_format) catch continue;
        }
    }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse a `key=value\n` line from a manifest.  Returns the value slice (pointing
/// into `content`) for the first line whose key matches, or null.
fn parseManifestField(content: []const u8, key: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        if (!std.mem.eql(u8, line[0..eq], key)) continue;
        return line[eq + 1 ..];
    }
    return null;
}

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
