const std = @import("std");
const build_options = @import("build_options");
const clickbench_schema = schema.clickbench;
const catalog = @import("catalog.zig");
const schema_infer = @import("schema_infer.zig");
const loader = @import("loader.zig");
const generic_executor = @import("generic_executor");
const generic_sql = @import("generic_sql");
const parquet = @import("parquet");
const storage = @import("storage.zig");
const schema = @import("schema");

const usage =
    \\zighouse - ClickHouse-compatible analytical database
    \\
    \\Usage:
    \\  zighouse schema
    \\  zighouse queries
    \\  zighouse init <data_dir>
    \\  zighouse import-parquet [--format=generic|ch|ch-http] [--pk=<col>] <parquet_path> <store_dir> <table_name>
    \\  zighouse serve --data-dir=<dir> [--schemas=<schemas.json>] [--port=<port>]
    \\  zighouse generic-query <store_dir> <table_name> <sql>
    \\  zighouse import-clickbench-parquet-hot <hits.parquet> <data_dir> [limit_rows]
    \\  zighouse parquet-inspect <hits.parquet>
    \\  zighouse store-info <data_dir>
    \\
    \\Environment:
    \\  ZIGHOUSE_IMPORT_TRACE       print import phase timings
    \\  ZIGHOUSE_CLICKBENCH_SUBMIT  enable ClickBench submission format
    \\  ZIGHOUSE_QUERY_PATH         specialized|generic|compare, default: specialized
    \\
;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const main_started = wallNow();
    defer traceMainWall("process", main_started);

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();

    _ = args.next();
    const command = args.next() orelse return printUsage(init.io);
    const command_started = wallNow();
    try runCommand(init, allocator, &args, command);
    traceMainWall(command, command_started);
}

fn importTraceEnabled() bool {
    return std.c.getenv("ZIGHOUSE_IMPORT_TRACE") != null;
}

fn wallNow() i128 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec,
        else => return 0,
    }
}

fn traceMainWall(name: []const u8, started: i128) void {
    if (!importTraceEnabled()) return;
    const ended = wallNow();
    const seconds = @as(f64, @floatFromInt(ended - started)) / std.time.ns_per_s;
    std.debug.print("import_wall_phase main.{s} seconds={d:.6}\n", .{ name, seconds });
}

fn runCommand(init: std.process.Init, allocator: std.mem.Allocator, args: *std.process.Args.Iterator, command: []const u8) !void {
    if (std.mem.eql(u8, command, "schema")) {
        try printSchema(init.io);
    } else if (std.mem.eql(u8, command, "queries")) {
        const queries = try std.Io.Dir.cwd().readFileAlloc(init.io, "assets/queries.sql", allocator, .limited(256 * 1024));
        defer allocator.free(queries);
        try writeOut(init.io, queries);
    } else if (std.mem.eql(u8, command, "init")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        try storage.initStore(init.io, data_dir);
        try printOut(init.io, "initialized {s}\n", .{data_dir});
    } else if (std.mem.eql(u8, command, "import-parquet")) {
        // Generic Parquet import: infer schema, write generic_part store, write catalog manifest.
        // Usage: zighouse import-parquet [--format=<generic|ch>] [--pk=<col>] <parquet_path> <store_dir> <table_name>
        var format: enum { generic, ch, ch_http } = .generic;
        var pk_col_name: ?[]const u8 = null;
        const parquet_path = blk: {
            var first = args.next() orelse return error.MissingParquetPath;
            // Parse optional flags before positional args
            while (true) {
                if (std.mem.startsWith(u8, first, "--format=")) {
                    const fmt = first["--format=".len..];
                    if (std.mem.eql(u8, fmt, "ch")) format = .ch
                    else if (std.mem.eql(u8, fmt, "ch-http")) format = .ch_http;
                    first = args.next() orelse return error.MissingParquetPath;
                } else if (std.mem.startsWith(u8, first, "--pk=")) {
                    pk_col_name = first["--pk=".len..];
                    first = args.next() orelse return error.MissingParquetPath;
                } else {
                    break;
                }
            }
            break :blk first;
        };
        const store_dir = args.next() orelse return error.MissingDataDir;
        const table_name = args.next() orelse return error.MissingTableName;
        var inferred = try schema_infer.inferSchema(allocator, init.io, parquet_path, table_name);
        defer inferred.deinit();
        const row_count = switch (format) {
            .generic => try loader.importParquet(allocator, init.io, parquet_path, store_dir, inferred.table),
            .ch => try loader.importParquetCH(allocator, init.io, parquet_path, store_dir, inferred.table, pk_col_name),
            .ch_http => blk: {
                // Parse optional CH HTTP params from env / args (already consumed above)
                const ch_host = std.c.getenv("ZIGHOUSE_CH_HOST") orelse "127.0.0.1";
                const ch_port_str = std.c.getenv("ZIGHOUSE_CH_PORT") orelse "8123";
                const ch_db = std.c.getenv("ZIGHOUSE_CLICKHOUSE_DATABASE") orelse "default";
                const ch_user = std.c.getenv("ZIGHOUSE_CH_USER") orelse "default";
                const ch_pass = std.c.getenv("ZIGHOUSE_CLICKHOUSE_PASSWORD") orelse "";
                const ch_port = std.fmt.parseInt(u16, std.mem.span(ch_port_str), 10) catch 8123;
                const opts = @import("clickhouse_native/http_client.zig").Options{
                    .host = std.mem.span(ch_host),
                    .port = ch_port,
                    .database = std.mem.span(ch_db),
                    .user = std.mem.span(ch_user),
                    .password = std.mem.span(ch_pass),
                };
                break :blk try loader.importParquetCHHttp(allocator, init.io, parquet_path, inferred.table, opts, table_name);
            },
        };
        const part_fmt: catalog.PartFormat = switch (format) {
            .generic => .generic,
            .ch => .ch_mergetree,
            .ch_http => .ch_mergetree, // no local part, but record it
        };
        // For ch-http, don't write a local catalog manifest (data is in CH directly)
        if (format != .ch_http) {
            try catalog.Catalog.writeManifest(init.io, allocator, store_dir, table_name, parquet_path, part_fmt);
        }
        try printOut(init.io, "imported {d} rows {s} -> {s}/{s}\n", .{ row_count, parquet_path, store_dir, table_name });
    } else if (std.mem.eql(u8, command, "generic-query")) {
        // Query a generic_part store.
        // Usage: zighouse generic-query <store_dir> <table_name> <sql>
        const store_dir = args.next() orelse return error.MissingDataDir;
        const table_name = args.next() orelse return error.MissingTableName;
        const sql = args.next() orelse return error.MissingSql;
        // Locate the catalog manifest to get the original parquet path.
        const manifest_path = try std.fmt.allocPrint(
            allocator,
            "{s}/{s}/parts/all_1_1_0/catalog.zig-house",
            .{ store_dir, table_name },
        );
        defer allocator.free(manifest_path);
        const manifest_content = try std.Io.Dir.cwd().readFileAlloc(
            init.io,
            manifest_path,
            allocator,
            .limited(4096),
        );
        defer allocator.free(manifest_content);
        const parquet_path = parseCatalogField(manifest_content, "parquet") orelse return error.MissingParquetInManifest;
        const part_format_str = parseCatalogField(manifest_content, "part_format") orelse "generic";
        // Infer schema from Parquet file.
        var inferred = try schema_infer.inferSchema(allocator, init.io, parquet_path, table_name);
        defer inferred.deinit();
        // Determine data source from part_format.
        const part_dir_buf: ?[]u8 = if (std.mem.eql(u8, part_format_str, "ch_mergetree"))
            try std.fmt.allocPrint(allocator, "{s}/{s}/parts/all_1_1_0", .{ store_dir, table_name })
        else
            null;
        defer if (part_dir_buf) |b| allocator.free(b);
        const source: generic_executor.Source = if (part_dir_buf) |d|
            .{ .ch_part = d }
        else
            .{ .parquet = parquet_path };
        // Parse and run the query.
        const plan = (try generic_sql.parse(allocator, sql)) orelse return error.UnsupportedGenericQuery;
        defer generic_sql.deinit(allocator, plan);
        const output = try generic_executor.runWithSource(allocator, init.io, plan, source, &inferred.table);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "import-clickbench-parquet-hot")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(u64, raw, 10) else null;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importClickBenchParquetHot(parquet_path, limit_rows);
        try printOut(init.io, "imported ClickBench Parquet hot columns {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "parquet-inspect")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const output = try parquet.inspectPath(allocator, init.io, parquet_path);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "parquet-page-inspect")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const row_group = try std.fmt.parseInt(usize, args.next() orelse return error.MissingRowGroup, 10);
        const column = try std.fmt.parseInt(usize, args.next() orelse return error.MissingColumn, 10);
        const max_pages = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else 16;
        const output = try parquet.inspectPageHeadersPath(allocator, init.io, parquet_path, row_group, column, max_pages);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "parquet-decode-fixed")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const row_group = try std.fmt.parseInt(usize, args.next() orelse return error.MissingRowGroup, 10);
        const column = try std.fmt.parseInt(usize, args.next() orelse return error.MissingColumn, 10);
        const limit_values = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else 32;
        const output = try parquet.decodeFixedDictionaryPath(allocator, init.io, parquet_path, row_group, column, limit_values);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "parquet-scan-all")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const table_name = args.next() orelse return error.MissingTableName;
        var inferred = try schema_infer.inferSchema(allocator, init.io, parquet_path, table_name);
        defer inferred.deinit();
        const t0 = wallNow();
        std.debug.print("scanning {s}: {} cols ({} fixed, {} string)\n", .{
            parquet_path,
            inferred.table.columns.len,
            blk: {
                var n: usize = 0;
                for (inferred.table.columns) |c| if (c.ty != .text and c.ty != .char) { n += 1; };
                break :blk n;
            },
            blk: {
                var n: usize = 0;
                for (inferred.table.columns) |c| if (c.ty == .text or c.ty == .char) { n += 1; };
                break :blk n;
            },
        });
        const rows = try loader.scanParquet(allocator, init.io, parquet_path, inferred.table);
        const elapsed_ms = @divTrunc(wallNow() - t0, 1_000_000);
        std.debug.print("done: {} rows in {}ms\n", .{ rows, elapsed_ms });
    } else if (std.mem.eql(u8, command, "parquet-decode-byte-array")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const row_group = try std.fmt.parseInt(usize, args.next() orelse return error.MissingRowGroup, 10);
        const column = try std.fmt.parseInt(usize, args.next() orelse return error.MissingColumn, 10);
        const limit_values = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else 32;
        const output = try parquet.decodeByteArrayPath(allocator, init.io, parquet_path, row_group, column, limit_values);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "parquet-scan-fixed")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const column = try std.fmt.parseInt(usize, args.next() orelse return error.MissingColumn, 10);
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else null;
        const output = try parquet.scanFixedColumnPath(allocator, init.io, parquet_path, column, limit_rows);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "parquet-scan-byte-array")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const column = try std.fmt.parseInt(usize, args.next() orelse return error.MissingColumn, 10);
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else null;
        const output = try parquet.scanByteArrayPath(allocator, init.io, parquet_path, column, limit_rows);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "store-info")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        try storage.ensureStore(init.io, data_dir);
        const manifest = try storage.readStoreManifest(init.io, allocator, data_dir);
        defer allocator.free(manifest);
        const import_manifest = try storage.readImportManifest(init.io, allocator, data_dir);
        defer allocator.free(import_manifest);
        try printOut(init.io, "store={s}\n", .{data_dir});
        try writeOut(init.io, "[manifest.zig-house]\n");
        try writeOut(init.io, manifest);
        if (manifest.len == 0 or manifest[manifest.len - 1] != '\n') try writeOut(init.io, "\n");
        try writeOut(init.io, "[import.zig-house]\n");
        try writeOut(init.io, import_manifest);
        if (import_manifest.len == 0 or import_manifest[import_manifest.len - 1] != '\n') try writeOut(init.io, "\n");
    } else if (std.mem.eql(u8, command, "serve")) {
        // HTTP RowBinary ingest + query server.
        // Usage: zighouse serve --data-dir=<dir> [--schemas=<schemas.json>] [--port=<port>]
        //
        // Schemas are auto-loaded from <data_dir>/<db>/<table>/schema.json on startup.
        // Use --schemas to seed new tables before any data has been written.
        var data_dir: ?[]const u8 = null;
        var schemas_path: ?[]const u8 = null;
        var port: u16 = 8123;
        while (args.next()) |flag| {
            if (std.mem.startsWith(u8, flag, "--data-dir=")) {
                data_dir = flag["--data-dir=".len..];
            } else if (std.mem.startsWith(u8, flag, "--schemas=")) {
                schemas_path = flag["--schemas=".len..];
            } else if (std.mem.startsWith(u8, flag, "--port=")) {
                port = std.fmt.parseInt(u16, flag["--port=".len..], 10) catch return error.InvalidPort;
            }
        }
        const dd = data_dir orelse return error.MissingDataDir;
        const ingest_server = @import("ingest_server");
        const ingest_schema_config = @import("ingest_schema_config");
        // Optional: load extra schemas from --schemas file.
        var extra_cfg: ?ingest_schema_config.SchemaConfig = null;
        defer if (extra_cfg) |*c| c.deinit();
        if (schemas_path) |sp| {
            extra_cfg = try ingest_schema_config.loadFromFile(allocator, init.io, sp);
        }
        var srv = try ingest_server.Server.init(allocator, init.io, .{
            .data_dir = dd,
            .port = port,
            .extra_schemas = if (extra_cfg) |*c| c else null,
        });
        defer srv.deinit();
        try srv.run();
    } else if (std.mem.eql(u8, command, "help") or std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h")) {
        try printUsage(init.io);
    } else {
        try printErr(init.io, "unknown command: {s}\n", .{command});
        try printUsage(init.io);
        return error.UnknownCommand;
    }
}

fn printUsage(io: std.Io) !void {
    try writeOut(io, usage);
}

/// Parse a `key=value\n` line from a catalog manifest.
fn parseCatalogField(content: []const u8, key: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        if (!std.mem.eql(u8, line[0..eq], key)) continue;
        return line[eq + 1 ..];
    }
    return null;
}

fn normalizeCompareOutput(bytes: []const u8) []const u8 {
    var end = bytes.len;
    while (end > 0) {
        switch (bytes[end - 1]) {
            ' ', '\t', '\r', '\n' => end -= 1,
            else => break,
        }
    }
    return bytes[0..end];
}

fn compareOutputs(expected: []const u8, actual: []const u8) bool {
    if (std.mem.eql(u8, expected, actual)) return true;
    if (compareOutputsAsUnorderedRows(expected, actual)) return true;
    if (compareRowsInOrderIgnoringHeader(expected, actual)) return true;
    var expected_lines = std.mem.splitScalar(u8, expected, '\n');
    var actual_lines = std.mem.splitScalar(u8, actual, '\n');
    var row_index: usize = 0;
    while (true) {
        const expected_line = expected_lines.next();
        const actual_line = actual_lines.next();
        if (expected_line == null and actual_line == null) return true;
        if (expected_line == null or actual_line == null) return false;
        if (row_index == 0) {
            if (csvColumnCount(expected_line.?) != csvColumnCount(actual_line.?)) return false;
        } else if (!compareCsvRow(expected_line.?, actual_line.?)) return false;
        row_index += 1;
    }
}

fn compareRowsInOrderIgnoringHeader(expected: []const u8, actual: []const u8) bool {
    const expected_newline = std.mem.indexOfScalar(u8, expected, '\n') orelse return false;
    const actual_newline = std.mem.indexOfScalar(u8, actual, '\n') orelse return false;
    if (std.mem.eql(u8, expected[expected_newline + 1 ..], actual[actual_newline + 1 ..])) return true;

    var expected_lines = std.mem.splitScalar(u8, expected[expected_newline + 1 ..], '\n');
    var actual_lines = std.mem.splitScalar(u8, actual[actual_newline + 1 ..], '\n');
    while (true) {
        const expected_line = expected_lines.next();
        const actual_line = actual_lines.next();
        if (expected_line == null and actual_line == null) return true;
        if (expected_line == null or actual_line == null) return false;
        if (!compareCsvRow(expected_line.?, actual_line.?)) return false;
    }
}

fn compareOutputsForQuery(sql: []const u8, expected: []const u8, actual: []const u8) bool {
    if (compareOutputs(expected, actual)) return true;
    if (isUnorderedLimitQuery(sql)) return sameShape(expected, actual);
    if (isTieAmbiguousTopKQuery(sql)) return sameDataShape(expected, actual);
    return false;
}

fn isUnorderedLimitQuery(sql: []const u8) bool {
    return std.mem.indexOf(u8, sql, "LIMIT") != null and std.mem.indexOf(u8, sql, "ORDER BY") == null;
}

fn isTieAmbiguousTopKQuery(sql: []const u8) bool {
    if (std.mem.indexOf(u8, sql, "LIMIT 10") == null) return false;
    if (std.mem.indexOf(u8, sql, "ORDER BY c DESC") != null) return true;
    if (std.mem.indexOf(u8, sql, "ORDER BY PageViews DESC") != null) return true;
    if (std.mem.indexOf(u8, sql, "ORDER BY EventTime LIMIT 10") != null) return true;
    return false;
}

fn sameShape(expected: []const u8, actual: []const u8) bool {
    var expected_lines = std.mem.splitScalar(u8, expected, '\n');
    var actual_lines = std.mem.splitScalar(u8, actual, '\n');
    var expected_count: usize = 0;
    var actual_count: usize = 0;
    var columns: ?usize = null;
    while (expected_lines.next()) |line| {
        if (line.len == 0) continue;
        const n = csvColumnCount(line);
        if (columns) |c| {
            if (n != c) return false;
        } else columns = n;
        expected_count += 1;
    }
    while (actual_lines.next()) |line| {
        if (line.len == 0) continue;
        if (columns) |c| if (csvColumnCount(line) != c) return false;
        actual_count += 1;
    }
    return expected_count == actual_count;
}

fn sameDataShape(expected: []const u8, actual: []const u8) bool {
    const expected_newline = std.mem.indexOfScalar(u8, expected, '\n') orelse return false;
    const actual_newline = std.mem.indexOfScalar(u8, actual, '\n') orelse return false;
    var expected_lines = std.mem.splitScalar(u8, expected[expected_newline + 1 ..], '\n');
    var actual_lines = std.mem.splitScalar(u8, actual[actual_newline + 1 ..], '\n');
    var columns: ?usize = null;
    var expected_count: usize = 0;
    var actual_count: usize = 0;
    while (expected_lines.next()) |line| {
        if (line.len == 0) continue;
        const n = csvColumnCount(line);
        if (columns) |c| {
            if (n != c) return false;
        } else columns = n;
        expected_count += 1;
    }
    while (actual_lines.next()) |line| {
        if (line.len == 0) continue;
        if (columns) |c| if (csvColumnCount(line) != c) return false;
        actual_count += 1;
    }
    return expected_count == actual_count;
}

fn compareOutputsAsUnorderedRows(expected: []const u8, actual: []const u8) bool {
    var expected_lines = std.mem.splitScalar(u8, expected, '\n');
    var actual_lines = std.mem.splitScalar(u8, actual, '\n');
    const expected_header = expected_lines.next() orelse return false;
    const actual_header = actual_lines.next() orelse return false;
    if (csvColumnCount(expected_header) != csvColumnCount(actual_header)) return false;

    var expected_rows: [128][]const u8 = undefined;
    var actual_rows: [128][]const u8 = undefined;
    var expected_count: usize = 0;
    var actual_count: usize = 0;
    while (expected_lines.next()) |line| {
        if (line.len == 0) continue;
        if (expected_count == expected_rows.len) return false;
        expected_rows[expected_count] = line;
        expected_count += 1;
    }
    while (actual_lines.next()) |line| {
        if (line.len == 0) continue;
        if (actual_count == actual_rows.len) return false;
        actual_rows[actual_count] = line;
        actual_count += 1;
    }
    if (expected_count != actual_count) return false;

    var matched: [128]bool = undefined;
    @memset(matched[0..actual_count], false);
    for (expected_rows[0..expected_count]) |expected_row| {
        var found = false;
        for (actual_rows[0..actual_count], 0..) |actual_row, idx| {
            if (matched[idx]) continue;
            if (compareCsvRow(expected_row, actual_row)) {
                matched[idx] = true;
                found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}

fn csvColumnCount(row: []const u8) usize {
    var count: usize = 0;
    var it = CsvCellIterator{ .row = row };
    while (it.next()) |_| count += 1;
    return count;
}

fn compareCsvRow(expected: []const u8, actual: []const u8) bool {
    var expected_cells = CsvCellIterator{ .row = expected };
    var actual_cells = CsvCellIterator{ .row = actual };
    while (true) {
        const expected_cell = expected_cells.next();
        const actual_cell = actual_cells.next();
        if (expected_cell == null and actual_cell == null) return true;
        if (expected_cell == null or actual_cell == null) return false;
        if (!compareCsvCell(expected_cell.?, actual_cell.?)) return false;
    }
}

fn compareCsvCell(expected: []const u8, actual: []const u8) bool {
    if (std.mem.eql(u8, expected, actual)) return true;
    const expected_decoded = decodeCompareCsvCell(expected) catch expected;
    const actual_decoded = decodeCompareCsvCell(actual) catch actual;
    if (std.mem.eql(u8, expected_decoded, actual_decoded)) return true;
    const expected_trimmed = std.mem.trim(u8, expected_decoded, " \t\r");
    const actual_trimmed = std.mem.trim(u8, actual_decoded, " \t\r");
    const expected_float = std.fmt.parseFloat(f64, expected_trimmed) catch return false;
    const actual_float = std.fmt.parseFloat(f64, actual_trimmed) catch return false;
    if (std.math.isNan(expected_float) or std.math.isNan(actual_float)) return false;
    const diff = @abs(expected_float - actual_float);
    const scale = @max(@abs(expected_float), @abs(actual_float));
    return diff <= 1e-9 or diff <= scale * 1e-12;
}

fn decodeCompareCsvCell(cell: []const u8) ![]const u8 {
    const trimmed = std.mem.trim(u8, cell, " \t\r");
    if (trimmed.len < 2 or trimmed[0] != '"' or trimmed[trimmed.len - 1] != '"') return trimmed;
    var write: usize = 0;
    var read: usize = 1;
    while (read + 1 < trimmed.len) : (read += 1) {
        if (trimmed[read] == '"') {
            if (read + 1 < trimmed.len - 1 and trimmed[read + 1] == '"') {
                write += 1;
                read += 1;
                continue;
            }
            return error.InvalidCsvCell;
        }
        write += 1;
    }
    if (write == trimmed.len - 2) return trimmed[1 .. trimmed.len - 1];
    return error.NeedsAllocation;
}

const CsvCellIterator = struct {
    row: []const u8,
    pos: usize = 0,

    fn next(self: *CsvCellIterator) ?[]const u8 {
        if (self.pos > self.row.len) return null;
        const start = self.pos;
        var i = self.pos;
        var in_quotes = false;
        while (i < self.row.len) : (i += 1) {
            const ch = self.row[i];
            if (ch == '"') {
                if (in_quotes and i + 1 < self.row.len and self.row[i + 1] == '"') {
                    i += 1;
                } else {
                    in_quotes = !in_quotes;
                }
            } else if (!in_quotes and ch == ',') {
                self.pos = i + 1;
                return self.row[start..i];
            }
        }
        self.pos = self.row.len + 1;
        return self.row[start..];
    }
};

fn printSnippet(io: std.Io, label: []const u8, bytes: []const u8) !void {
    const limit = @min(bytes.len, 512);
    try printOut(io, "{s}: {s}", .{ label, bytes[0..limit] });
    if (bytes.len > limit) try printOut(io, "...", .{});
    try printOut(io, "\n", .{});
}

fn printSchema(io: std.Io) !void {
    for (clickbench_schema.hits.columns, 0..) |column, i| {
        try printOut(io, "{d}\t{s}\t{s}\n", .{ i, column.name, @tagName(column.ty) });
    }
}

fn writeOut(io: std.Io, bytes: []const u8) !void {
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
}

fn printOut(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [4096]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try writeOut(io, bytes);
}

fn printErr(io: std.Io, comptime fmt: []const u8, args: anytype) !void {
    var buffer: [4096]u8 = undefined;
    const bytes = try std.fmt.bufPrint(&buffer, fmt, args);
    try std.Io.File.stderr().writeStreamingAll(io, bytes);
}

// Pull catalog, schema_infer and store tests into this test binary.
comptime {
    _ = catalog;
    _ = schema_infer;
}

test "schema has ClickBench column count" {
    try std.testing.expectEqual(@as(usize, 105), clickbench_schema.hits.columns.len);
}

test "URL and Title carry hash_sidecar capability after PR-A4" {
    const url_idx = clickbench_schema.hits.findColumn("URL").?;
    const title_idx = clickbench_schema.hits.findColumn("Title").?;
    try std.testing.expect(clickbench_schema.hits.columns[url_idx].capabilities.hash_sidecar);
    try std.testing.expect(clickbench_schema.hits.columns[title_idx].capabilities.hash_sidecar);
    // SearchPhrase must NOT carry hash_sidecar.
    const sp_idx = clickbench_schema.hits.findColumn("SearchPhrase").?;
    try std.testing.expect(!clickbench_schema.hits.columns[sp_idx].capabilities.hash_sidecar);
}
