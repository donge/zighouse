/// Per-table schema persistence.
///
/// Writes/reads a schema.json file at:
///   <data_dir>/<db>/<table>/schema.json
///
/// Format is the single-table subset of schema_config.zig's JSON:
/// {
///   "db": "default",
///   "name": "my_table",
///   "pk": "id",                      // optional
///   "sort_keys": ["CounterID"],       // optional — columns stored in sorted order
///   "columns": [
///     {"name": "id",   "type": "Int32"},
///     {"name": "name", "type": "String"}
///   ]
/// }
///
/// Usage:
///   try schema_persist.save(io, allocator, data_dir, db, table_entry);
///   const entry = try schema_persist.load(allocator, io, data_dir, db, table_name);

const std = @import("std");
const schema = @import("schema");
const schema_config = @import("schema_config");
const ddl_parser = @import("ddl_parser");

const LoadedKey = struct {
    db: []const u8,
    table: []const u8,
};

fn hasLoaded(keys: []const LoadedKey, db: []const u8, table: []const u8) bool {
    for (keys) |k| {
        if (std.mem.eql(u8, k.db, db) and std.mem.eql(u8, k.table, table)) return true;
    }
    return false;
}

// ── Shared serialiser ─────────────────────────────────────────────────────────

fn appendTableJson(
    buf:       *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    db:        []const u8,
    entry:     *const schema_config.TableEntry,
) !void {
    try buf.appendSlice(allocator, "{\n  \"db\": \"");
    try writeJsonString(buf, allocator, db);
    try buf.appendSlice(allocator, "\",\n  \"name\": \"");
    try writeJsonString(buf, allocator, entry.name);
    try buf.appendSlice(allocator, "\"");

    if (entry.pk) |pk| {
        try buf.appendSlice(allocator, ",\n  \"pk\": \"");
        try writeJsonString(buf, allocator, pk);
        try buf.appendSlice(allocator, "\"");
    }

    if (entry.table.sort_keys.len > 0) {
        try buf.appendSlice(allocator, ",\n  \"sort_keys\": [");
        for (entry.table.sort_keys, 0..) |sk, i| {
            try buf.append(allocator, '"');
            try writeJsonString(buf, allocator, sk);
            try buf.append(allocator, '"');
            if (i + 1 < entry.table.sort_keys.len) try buf.append(allocator, ',');
        }
        try buf.append(allocator, ']');
    }

    try buf.appendSlice(allocator, ",\n  \"columns\": [\n");
    for (entry.table.columns, 0..) |col, i| {
        try buf.appendSlice(allocator, "    {\"name\": \"");
        try writeJsonString(buf, allocator, col.name);
        try buf.appendSlice(allocator, "\", \"type\": \"");
        if (col.ch_type) |ct| {
            try writeJsonString(buf, allocator, ct);
        } else {
            try buf.appendSlice(allocator, columnTypeName(col.ty));
        }
        try buf.appendSlice(allocator, "\"}");
        if (i + 1 < entry.table.columns.len) try buf.append(allocator, ',');
        try buf.append(allocator, '\n');
    }
    try buf.appendSlice(allocator, "  ]\n}\n");
}

/// Write schema.json for a table.
/// Creates intermediate directories if needed.
pub fn save(
    io: std.Io,
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    db: []const u8,
    entry: *const schema_config.TableEntry,
) !void {
    const dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ data_dir, db, entry.name });
    defer allocator.free(dir_path);
    try std.Io.Dir.cwd().createDirPath(io, dir_path);
    const file_path = try std.fmt.allocPrint(allocator, "{s}/schema.json", .{dir_path});
    defer allocator.free(file_path);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try appendTableJson(&buf, allocator, db, entry);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = file_path, .data = buf.items });
}

/// Write schema.json directly into `table_dir` (i.e. `<table_dir>/schema.json`).
pub fn saveToDir(
    io: std.Io,
    allocator: std.mem.Allocator,
    table_dir: []const u8,
    db: []const u8,
    entry: *const schema_config.TableEntry,
) !void {
    try std.Io.Dir.cwd().createDirPath(io, table_dir);
    const file_path = try std.fmt.allocPrint(allocator, "{s}/schema.json", .{table_dir});
    defer allocator.free(file_path);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try appendTableJson(&buf, allocator, db, entry);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = file_path, .data = buf.items });
}

/// Load schema.json for a single table.
/// Returns null if the file doesn't exist.
/// Caller owns the returned TableEntry via the returned SchemaConfig (call .deinit()).
pub fn load(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    db: []const u8,
    table_name: []const u8,
) !?schema_config.SchemaConfig {
    const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/schema.json", .{ data_dir, db, table_name });
    defer allocator.free(file_path);

    const json_bytes = std.Io.Dir.cwd().readFileAlloc(io, file_path, allocator, .limited(256 * 1024)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(json_bytes);

    // Wrap single-table JSON in the array format expected by schema_config.
    const wrapped = try std.fmt.allocPrint(allocator, "{{\"tables\":[{s}]}}", .{json_bytes});
    defer allocator.free(wrapped);

    return try schema_config.loadFromSlice(allocator, wrapped);
}

/// Scan data_dir for all db/table pairs that have a schema.json and load them
/// into a combined SchemaConfig.
/// data_dir layout: data_dir/<db>/<table>/schema.json
pub fn loadAll(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
) !schema_config.SchemaConfig {
    // Collect all JSON fragments.
    var fragments: std.ArrayList([]const u8) = .empty;
    var loaded: std.ArrayList(LoadedKey) = .empty;
    defer {
        for (fragments.items) |f| allocator.free(f);
        fragments.deinit(allocator);
        for (loaded.items) |k| {
            allocator.free(k.db);
            allocator.free(k.table);
        }
        loaded.deinit(allocator);
    }

    var data_dir_handle = std.Io.Dir.cwd().openDir(io, data_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => {
            // Empty config if data_dir doesn't exist yet.
            return schema_config.loadFromSlice(allocator, "{\"tables\":[]}");
        },
        else => return err,
    };
    defer data_dir_handle.close(io);

    var db_iter = data_dir_handle.iterate();
    while (try db_iter.next(io)) |db_entry| {
        if (db_entry.kind != .directory) continue;
        const db = db_entry.name;

        const db_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ data_dir, db });
        defer allocator.free(db_path);

        var db_dir = std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true }) catch continue;
        defer db_dir.close(io);

        var table_iter = db_dir.iterate();
        while (try table_iter.next(io)) |tbl_entry| {
            if (tbl_entry.kind != .directory) continue;
            const table_name = tbl_entry.name;

            const schema_path = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/schema.json", .{ data_dir, db, table_name });
            defer allocator.free(schema_path);

            const json_bytes = std.Io.Dir.cwd().readFileAlloc(io, schema_path, allocator, .limited(256 * 1024)) catch continue;
            // Trim whitespace for clean embedding.
            const trimmed = std.mem.trim(u8, json_bytes, " \t\r\n");
            const fragment = try allocator.dupe(u8, trimmed);
            allocator.free(json_bytes);
            try fragments.append(allocator, fragment);
            try loaded.append(allocator, .{
                .db = try allocator.dupe(u8, db),
                .table = try allocator.dupe(u8, table_name),
            });
        }
    }

    try loadClickHouseMetadataSql(allocator, io, data_dir, &fragments, &loaded);

    if (fragments.items.len == 0) {
        return schema_config.loadFromSlice(allocator, "{\"tables\":[]}");
    }

    // Build combined JSON: {"tables":[...]}
    var combined: std.ArrayList(u8) = .empty;
    defer combined.deinit(allocator);
    try combined.appendSlice(allocator, "{\"tables\":[");
    for (fragments.items, 0..) |f, i| {
        try combined.appendSlice(allocator, f);
        if (i + 1 < fragments.items.len) try combined.append(allocator, ',');
    }
    try combined.appendSlice(allocator, "]}");

    return schema_config.loadFromSlice(allocator, combined.items);
}

fn loadClickHouseMetadataSql(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    fragments: *std.ArrayList([]const u8),
    loaded: *std.ArrayList(LoadedKey),
) !void {
    const meta_root = try std.fmt.allocPrint(allocator, "{s}/metadata", .{data_dir});
    defer allocator.free(meta_root);

    var meta_dir = std.Io.Dir.cwd().openDir(io, meta_root, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return,
        else => return err,
    };
    defer meta_dir.close(io);

    var db_iter = meta_dir.iterate();
    while (try db_iter.next(io)) |db_entry| {
        if (db_entry.kind != .directory) continue;
        const db = db_entry.name;
        const db_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ meta_root, db });
        defer allocator.free(db_path);

        var db_dir = std.Io.Dir.cwd().openDir(io, db_path, .{ .iterate = true }) catch continue;
        defer db_dir.close(io);

        var table_iter = db_dir.iterate();
        while (try table_iter.next(io)) |sql_entry| {
            if (sql_entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, sql_entry.name, ".sql")) continue;
            const table_name = sql_entry.name[0 .. sql_entry.name.len - ".sql".len];
            if (hasLoaded(loaded.items, db, table_name)) continue;

            const sql_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ db_path, sql_entry.name });
            defer allocator.free(sql_path);
            const sql = std.Io.Dir.cwd().readFileAlloc(io, sql_path, allocator, .limited(1024 * 1024)) catch continue;
            defer allocator.free(sql);

            var parsed = ddl_parser.parse(allocator, sql) catch continue;
            defer parsed.deinit();

            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(allocator);
            try appendTableJson(&buf, allocator, db, &parsed.entry);
            try fragments.append(allocator, try allocator.dupe(u8, std.mem.trim(u8, buf.items, " \t\r\n")));
            try loaded.append(allocator, .{
                .db = try allocator.dupe(u8, db),
                .table = try allocator.dupe(u8, table_name),
            });
        }
    }
}

fn columnTypeName(ty: schema.ColumnType) []const u8 {
    return switch (ty) {
        .int8 => "Int8",
        .int16 => "Int16",
        .int32 => "Int32",
        .int64 => "Int64",
        .date => "Date",
        .timestamp => "DateTime",
        .text, .char, .low_card => "String",
        .float32 => "Float32",
        .float64 => "Float64",
    };
}

fn writeJsonString(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, c),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "columnTypeName round-trips through schema_config" {
    const allocator = std.testing.allocator;

    const table = schema.Table{
        .name = "t",
        .columns = &.{
            .{ .name = "a", .ty = .int16 },
            .{ .name = "b", .ty = .int32 },
            .{ .name = "c", .ty = .int64 },
            .{ .name = "d", .ty = .date },
            .{ .name = "e", .ty = .timestamp },
            .{ .name = "f", .ty = .text },
        },
    };
    const entry = schema_config.TableEntry{
        .db = "default",
        .name = "t",
        .pk = null,
        .table = table,
    };

    // Serialise manually and parse back.
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"db\":\"default\",\"name\":\"t\",\"columns\":[");
    for (table.columns, 0..) |col, i| {
        const col_json = try std.fmt.allocPrint(allocator, "{{\"name\":\"{s}\",\"type\":\"{s}\"}}", .{ col.name, columnTypeName(col.ty) });
        defer allocator.free(col_json);
        try buf.appendSlice(allocator, col_json);
        if (i + 1 < table.columns.len) try buf.append(allocator, ',');
    }
    try buf.appendSlice(allocator, "]}");

    const wrapped = try std.fmt.allocPrint(allocator, "{{\"tables\":[{s}]}}", .{buf.items});
    defer allocator.free(wrapped);

    var cfg = try schema_config.loadFromSlice(allocator, wrapped);
    defer cfg.deinit();

    const found = cfg.find("default", "t").?;
    try std.testing.expectEqual(@as(usize, 6), found.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int16, found.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.timestamp, found.table.columns[4].ty);
    try std.testing.expectEqual(schema.ColumnType.text, found.table.columns[5].ty);

    _ = entry; // suppress unused warning
}

test "loadAll: loads ClickHouse metadata sql after schema json" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/zig_test_schema_metadata_sql";
    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, root) catch {};
    }
    defer {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, root) catch {};
    }

    const json_dir = root ++ "/vprobe/json_first";
    try std.Io.Dir.cwd().createDirPath(io, json_dir);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = json_dir ++ "/schema.json",
        .data =
            \\{"db":"vprobe","name":"json_first","columns":[{"name":"id","type":"Int32"}],"sort_keys":["id"]}
        ,
    });

    const meta_dir = root ++ "/metadata/vprobe";
    try std.Io.Dir.cwd().createDirPath(io, meta_dir);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = meta_dir ++ "/json_first.sql",
        .data =
            \\CREATE TABLE vprobe.json_first
            \\(
            \\  `id` UInt64,
            \\  `ignored` String
            \\)
            \\ENGINE = MergeTree
            \\ORDER BY (id)
        ,
    });
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = meta_dir ++ "/from_metadata.sql",
        .data =
            \\CREATE TABLE vprobe.from_metadata
            \\(
            \\  `event_type` LowCardinality(String),
            \\  `ts` DateTime64(3),
            \\  `features` Map(String, Float64)
            \\)
            \\ENGINE = ReplacingMergeTree(ts)
            \\PARTITION BY toYYYYMM(ts)
            \\ORDER BY (event_type, ts)
            \\SETTINGS index_granularity = 8192
        ,
    });

    var cfg = try loadAll(allocator, io, root);
    defer cfg.deinit();

    const json_first = cfg.find("vprobe", "json_first").?;
    try std.testing.expectEqual(@as(usize, 1), json_first.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int32, json_first.table.columns[0].ty);

    const meta = cfg.find("vprobe", "from_metadata").?;
    try std.testing.expectEqual(@as(usize, 3), meta.table.columns.len);
    try std.testing.expectEqualStrings("LowCardinality(String)", meta.table.columns[0].ch_type.?);
    try std.testing.expectEqualStrings("DateTime64(3)", meta.table.columns[1].ch_type.?);
    try std.testing.expectEqualStrings("Map(String, Float64)", meta.table.columns[2].ch_type.?);
    try std.testing.expectEqual(@as(usize, 2), meta.table.sort_keys.len);
    try std.testing.expectEqualStrings("event_type", meta.table.sort_keys[0]);
    try std.testing.expectEqualStrings("ts", meta.table.sort_keys[1]);
}
