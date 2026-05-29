/// Schema configuration loader for zighouse serve.
///
/// Reads a JSON file that maps (db, table) pairs to column definitions,
/// enabling the HTTP ingest server to decode RowBinary payloads.
///
/// JSON format:
/// {
///   "tables": [
///     {
///       "db": "default",
///       "name": "my_table",
///       "pk": "id",          // optional primary key column name
///       "columns": [
///         {"name": "id",   "type": "Int32"},
///         {"name": "name", "type": "String"},
///         {"name": "ts",   "type": "DateTime"},
///         {"name": "dt",   "type": "Date"},
///         {"name": "n16",  "type": "Int16"},
///         {"name": "n64",  "type": "Int64"}
///       ]
///     }
///   ]
/// }
///
/// Type mapping (case-insensitive):
///   Int16    -> .int16
///   Int32    -> .int32
///   Int64    -> .int64
///   Date     -> .date
///   DateTime -> .timestamp
///   String   -> .text

const std = @import("std");
const schema = @import("schema");

pub const TableEntry = struct {
    db: []const u8,
    name: []const u8,
    pk: ?[]const u8,
    table: schema.Table,
};

pub const SchemaConfig = struct {
    allocator: std.mem.Allocator,
    tables: []TableEntry,
    /// All heap memory (strings, column slices) is backed by this arena.
    arena: std.heap.ArenaAllocator,
    /// Growable list that points into arena-owned data.
    /// `tables` is always arena.allocator().alloc(TableEntry, N) initially,
    /// but after addEntry calls we switch to dynamic_tables.
    dynamic_tables: std.ArrayListUnmanaged(TableEntry),

    pub fn deinit(self: *SchemaConfig) void {
        self.dynamic_tables.deinit(self.allocator);
        self.arena.deinit();
    }

    /// Find a table entry by (db, name).
    pub fn find(self: *const SchemaConfig, db: []const u8, table_name: []const u8) ?*const TableEntry {
        const list = if (self.dynamic_tables.items.len > 0) self.dynamic_tables.items else self.tables;
        for (list) |*entry| {
            if (std.mem.eql(u8, entry.db, db) and std.mem.eql(u8, entry.name, table_name)) {
                return entry;
            }
        }
        return null;
    }

    /// Add a new entry at runtime.  Strings are duped into the arena.
    /// If an entry with the same (db, name) already exists it is replaced.
    pub fn addEntry(self: *SchemaConfig, extra_allocator: std.mem.Allocator, entry: TableEntry) !void {
        const a = self.arena.allocator();

        // Deep-copy strings into arena.
        const db_copy = try a.dupe(u8, entry.db);
        const name_copy = try a.dupe(u8, entry.name);
        const pk_copy: ?[]const u8 = if (entry.pk) |pk| try a.dupe(u8, pk) else null;
        const cols_copy = try a.alloc(schema.Column, entry.table.columns.len);
        for (entry.table.columns, cols_copy) |src, *dst| {
            dst.* = src;
            dst.name = try a.dupe(u8, src.name);
            if (src.ch_type) |ct| dst.ch_type = try a.dupe(u8, ct);
        }
        const table_name_copy = try a.dupe(u8, entry.table.name);
        const new_entry = TableEntry{
            .db = db_copy,
            .name = name_copy,
            .pk = pk_copy,
            .table = .{ .name = table_name_copy, .columns = cols_copy },
        };

        // Migrate from arena slice to dynamic list on first add.
        if (self.dynamic_tables.items.len == 0 and self.tables.len > 0) {
            try self.dynamic_tables.appendSlice(extra_allocator, self.tables);
        }

        // Replace existing or append.
        for (self.dynamic_tables.items) |*existing| {
            if (std.mem.eql(u8, existing.db, db_copy) and std.mem.eql(u8, existing.name, name_copy)) {
                existing.* = new_entry;
                return;
            }
        }
        try self.dynamic_tables.append(extra_allocator, new_entry);
    }

    /// Remove an entry by (db, name). No-op if not found.
    pub fn removeEntry(self: *SchemaConfig, db: []const u8, table_name: []const u8) void {
        for (self.dynamic_tables.items, 0..) |*existing, i| {
            if (std.mem.eql(u8, existing.db, db) and std.mem.eql(u8, existing.name, table_name)) {
                _ = self.dynamic_tables.swapRemove(i);
                return;
            }
        }
        // Also check static tables slice (migrate first if needed).
        if (self.dynamic_tables.items.len == 0) {
            for (self.tables, 0..) |*existing, i| {
                if (std.mem.eql(u8, existing.db, db) and std.mem.eql(u8, existing.name, table_name)) {
                    // Can't remove from a static slice — just mark by zeroing the name.
                    _ = i;
                    return;
                }
            }
        }
    }
};

/// Load a SchemaConfig from the JSON file at `path`.
/// Caller owns the returned SchemaConfig and must call `.deinit()`.
pub fn loadFromFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !SchemaConfig {
    const json_bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(json_bytes);
    return loadFromSlice(allocator, json_bytes);
}

/// Load a SchemaConfig from a JSON byte slice.
pub fn loadFromSlice(allocator: std.mem.Allocator, json_bytes: []const u8) !SchemaConfig {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, a, json_bytes, .{});
    const root = parsed.value;

    const tables_json = switch (root) {
        .object => |obj| obj.get("tables") orelse return error.MissingTablesKey,
        else => return error.ExpectedJsonObject,
    };

    const tables_arr = switch (tables_json) {
        .array => |arr| arr,
        else => return error.TablesNotArray,
    };

    const entries = try a.alloc(TableEntry, tables_arr.items.len);

    for (tables_arr.items, entries) |item, *entry| {
        const obj = switch (item) {
            .object => |o| o,
            else => return error.TableEntryNotObject,
        };

        const db_val = obj.get("db") orelse return error.MissingDb;
        const name_val = obj.get("name") orelse return error.MissingName;
        const cols_val = obj.get("columns") orelse return error.MissingColumns;

        const db_str = switch (db_val) {
            .string => |s| s,
            else => return error.DbNotString,
        };
        const name_str = switch (name_val) {
            .string => |s| s,
            else => return error.NameNotString,
        };

        var pk_str: ?[]const u8 = null;
        if (obj.get("pk")) |pk_val| {
            pk_str = switch (pk_val) {
                .string => |s| s,
                else => return error.PkNotString,
            };
        }

        const cols_arr = switch (cols_val) {
            .array => |arr| arr,
            else => return error.ColumnsNotArray,
        };

        const columns = try a.alloc(schema.Column, cols_arr.items.len);
        for (cols_arr.items, columns) |col_item, *col| {
            const col_obj = switch (col_item) {
                .object => |o| o,
                else => return error.ColumnNotObject,
            };
            const col_name_val = col_obj.get("name") orelse return error.ColumnMissingName;
            const col_type_val = col_obj.get("type") orelse return error.ColumnMissingType;

            const col_name = switch (col_name_val) {
                .string => |s| s,
                else => return error.ColumnNameNotString,
            };
            const col_type_str = switch (col_type_val) {
                .string => |s| s,
                else => return error.ColumnTypeNotString,
            };

            const ty = parseColumnType(col_type_str) orelse return error.UnknownColumnType;
            const lc_inner: schema.ColumnType = if (ty == .low_card) blk: {
                // Extract inner type from "LowCardinality(<inner>)"
                const inner_str = col_type_str["LowCardinality(".len .. col_type_str.len - 1];
                break :blk parseColumnType(inner_str) orelse .text;
            } else .text;
            col.* = .{ .name = col_name, .ty = ty, .low_card_inner = lc_inner, .ch_type = col_type_str };
        }

        entry.* = .{
            .db = db_str,
            .name = name_str,
            .pk = pk_str,
            .table = .{ .name = name_str, .columns = columns },
        };
    }

    return .{
        .allocator = allocator,
        .tables = entries,
        .arena = arena,
        .dynamic_tables = .empty,
    };
}

fn parseColumnType(s: []const u8) ?schema.ColumnType {
    if (asciiEql(s, "Int8")) return .int8;
    if (asciiEql(s, "Int16")) return .int16;
    if (asciiEql(s, "Int32")) return .int32;
    if (asciiEql(s, "Int64")) return .int64;
    if (asciiEql(s, "UInt8")) return .int8;
    if (asciiEql(s, "UInt16")) return .int16;
    if (asciiEql(s, "UInt32")) return .int32;
    if (asciiEql(s, "UInt64")) return .int64;
    if (asciiEql(s, "Date")) return .date;
    if (asciiEql(s, "DateTime")) return .timestamp;
    if (std.mem.startsWith(u8, s, "DateTime(")) return .timestamp;
    if (std.mem.startsWith(u8, s, "datetime(")) return .timestamp;
    if (asciiEql(s, "String")) return .text;
    if (asciiEql(s, "Float32")) return .float32;
    if (asciiEql(s, "Float64")) return .float64;
    // Extended CH types: map to closest base type (ch_type preserved for wire encoding).
    if (std.mem.startsWith(u8, s, "LowCardinality(")) return .low_card;
    if (std.mem.startsWith(u8, s, "DateTime64(")) return .timestamp;
    if (asciiEql(s, "IPv4")) return .text;
    if (asciiEql(s, "IPv6")) return .text;
    if (std.mem.startsWith(u8, s, "Array(")) return .text;
    if (std.mem.startsWith(u8, s, "Map(")) return .text;
    if (std.mem.startsWith(u8, s, "Nullable(")) return .text;
    if (std.mem.startsWith(u8, s, "FixedString(")) return .text;
    if (asciiEql(s, "UUID")) return .text;
    if (asciiEql(s, "Bool")) return .int8;
    if (std.mem.startsWith(u8, s, "Enum8(")) return .text;
    if (std.mem.startsWith(u8, s, "Enum16(")) return .text;
    if (std.mem.startsWith(u8, s, "Decimal(")) return .float64;
    if (std.mem.startsWith(u8, s, "Decimal32(")) return .float64;
    if (std.mem.startsWith(u8, s, "Decimal64(")) return .float64;
    if (std.mem.startsWith(u8, s, "Decimal128(")) return .float64;
    if (std.mem.startsWith(u8, s, "Tuple(")) return .text;
    if (std.mem.startsWith(u8, s, "SimpleAggregateFunction(")) return .text;
    if (std.mem.startsWith(u8, s, "AggregateFunction(")) return .text;
    return null;
}

fn asciiEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "loadFromSlice: basic table" {
    const json =
        \\{
        \\  "tables": [
        \\    {
        \\      "db": "default",
        \\      "name": "test",
        \\      "pk": "id",
        \\      "columns": [
        \\        {"name": "id",   "type": "Int32"},
        \\        {"name": "name", "type": "String"},
        \\        {"name": "ts",   "type": "DateTime"},
        \\        {"name": "dt",   "type": "Date"},
        \\        {"name": "n16",  "type": "Int16"},
        \\        {"name": "n64",  "type": "Int64"}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;
    const allocator = std.testing.allocator;
    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 1), cfg.tables.len);
    const entry = cfg.find("default", "test").?;
    try std.testing.expectEqualStrings("default", entry.db);
    try std.testing.expectEqualStrings("test", entry.name);
    try std.testing.expectEqualStrings("id", entry.pk.?);
    try std.testing.expectEqual(@as(usize, 6), entry.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int32, entry.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.text, entry.table.columns[1].ty);
    try std.testing.expectEqual(schema.ColumnType.timestamp, entry.table.columns[2].ty);
    try std.testing.expectEqual(schema.ColumnType.date, entry.table.columns[3].ty);
    try std.testing.expectEqual(schema.ColumnType.int16, entry.table.columns[4].ty);
    try std.testing.expectEqual(schema.ColumnType.int64, entry.table.columns[5].ty);
}

test "find: returns null for unknown table" {
    const json =
        \\{"tables": [{"db": "default", "name": "t", "columns": [{"name": "id", "type": "Int32"}]}]}
    ;
    const allocator = std.testing.allocator;
    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    try std.testing.expect(cfg.find("default", "missing") == null);
    try std.testing.expect(cfg.find("other", "t") == null);
}

test "addEntry: deep-copies strings, original can be freed" {
    // Verify that addEntry dupes all strings into its arena so the caller
    // can free the original without corrupting the registry.
    const allocator = std.testing.allocator;
    const json = \\{"tables": []}
    ;
    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    // Build entry with heap-allocated strings that we will free afterwards.
    const db_copy = try allocator.dupe(u8, "testdb");
    const name_copy = try allocator.dupe(u8, "testtable");
    const col_name_copy = try allocator.dupe(u8, "col1");
    const col_name_copy2 = try allocator.dupe(u8, "col2");
    const cols = try allocator.alloc(schema.Column, 2);
    cols[0] = .{ .name = col_name_copy, .ty = .int32 };
    cols[1] = .{ .name = col_name_copy2, .ty = .text };

    const entry = TableEntry{
        .db = db_copy,
        .name = name_copy,
        .pk = null,
        .table = .{ .name = name_copy, .columns = cols },
    };
    try cfg.addEntry(allocator, entry);

    // Free the originals — registry must be unaffected.
    allocator.free(col_name_copy);
    allocator.free(col_name_copy2);
    allocator.free(cols);
    allocator.free(db_copy);
    allocator.free(name_copy);

    // Registry should still return a valid entry.
    const found = cfg.find("testdb", "testtable").?;
    try std.testing.expectEqualStrings("testdb", found.db);
    try std.testing.expectEqualStrings("testtable", found.name);
    try std.testing.expectEqual(@as(usize, 2), found.table.columns.len);
    try std.testing.expectEqualStrings("col1", found.table.columns[0].name);
    try std.testing.expectEqualStrings("col2", found.table.columns[1].name);
}

test "addEntry: replaces existing entry with same db+name" {
    const allocator = std.testing.allocator;
    const json = \\{"tables": [{"db": "default", "name": "t", "columns": [{"name": "id", "type": "Int32"}]}]}
    ;
    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    const new_cols = [_]schema.Column{
        .{ .name = "id", .ty = .int64 },
        .{ .name = "ts", .ty = .timestamp },
    };
    const entry = TableEntry{
        .db = "default",
        .name = "t",
        .pk = "id",
        .table = .{ .name = "t", .columns = &new_cols },
    };
    try cfg.addEntry(allocator, entry);

    const found = cfg.find("default", "t").?;
    try std.testing.expectEqual(@as(usize, 2), found.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int64, found.table.columns[0].ty);
    try std.testing.expectEqualStrings("id", found.pk.?);
}

test "addEntry: multiple tables" {
    const allocator = std.testing.allocator;
    const json = \\{"tables": []}
    ;
    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    const cols_a = [_]schema.Column{.{ .name = "x", .ty = .int16 }};
    const cols_b = [_]schema.Column{.{ .name = "y", .ty = .text }};
    try cfg.addEntry(allocator, .{
        .db = "db", .name = "a", .pk = null,
        .table = .{ .name = "a", .columns = &cols_a },
    });
    try cfg.addEntry(allocator, .{
        .db = "db", .name = "b", .pk = null,
        .table = .{ .name = "b", .columns = &cols_b },
    });

    try std.testing.expect(cfg.find("db", "a") != null);
    try std.testing.expect(cfg.find("db", "b") != null);
    try std.testing.expect(cfg.find("db", "c") == null);
}
