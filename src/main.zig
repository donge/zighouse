const std = @import("std");
const backend = @import("backend.zig");
const duckdb = @import("duckdb.zig");
const schema = @import("schema.zig");
const storage = @import("storage.zig");

const usage =
    \\zighouse - minimal ClickBench-oriented analytical database
    \\
    \\Usage:
    \\  zighouse schema
    \\  zighouse queries
    \\  zighouse init <data_dir>
    \\  zighouse import <hits.parquet> <data_dir>
    \\  zighouse import-hot <hits.parquet> <data_dir>
    \\  zighouse import-hot-extra <hits.parquet> <data_dir>
    \\  zighouse convert-hot <data_dir>
    \\  zighouse build-stats <data_dir>
    \\  zighouse convert-i16-csv <csv_path> <out_path>
    \\  zighouse build-string-column <data_dir> <col>
    \\  zighouse build-q23-candidates <data_dir>
    \\  zighouse build-q25-candidates <data_dir>
    \\  zighouse build-q29-domain-stats <data_dir>
    \\  zighouse build-q40-result <data_dir>
    \\  zighouse build-q21-count-google <data_dir>
    \\  zighouse query-csv <csv_path> <sql>
    \\  zighouse query <data_dir> <sql>
    \\  zighouse native-status <data_dir>
    \\  zighouse bench <data_dir> <queries.sql>
    \\  zighouse bench-one <data_dir> <queries.sql> <query_num>
    \\  zighouse bench-range <data_dir> <queries.sql> <first> <limit>
    \\
    \\Options:
    \\  --backend duckdb|native|chdb|clickhouse  default: duckdb
    \\  ZIGHOUSE_CHDB_PYTHON                  chDB python executable, default: python3
    \\  ZIGHOUSE_CLICKHOUSE_CONTAINER         ClickHouse docker container, default: sw_asdb
    \\  ZIGHOUSE_CLICKHOUSE_DATABASE          ClickHouse database, default: default
    \\  ZIGHOUSE_CLICKHOUSE_PASSWORD          ClickHouse password, default: Sw@123456
    \\
    \\Current milestone:
    \\  compare explicit duckdb and native backends; native has no DuckDB fallback.
    \\
;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();

    _ = args.next();
    var options = backend.Options.fromEnv(init.environ_map);
    while (true) {
        const maybe_arg = args.next() orelse return printUsage(init.io);
        if (std.mem.eql(u8, maybe_arg, "--backend")) {
            const value = args.next() orelse return error.MissingBackend;
            options.kind = try backend.Kind.parse(value);
            continue;
        }
        if (std.mem.startsWith(u8, maybe_arg, "--backend=")) {
            options.kind = try backend.Kind.parse(maybe_arg["--backend=".len..]);
            continue;
        }
        const command = maybe_arg;
        try runCommand(init, allocator, &args, command, options);
        return;
    }
}

fn runCommand(init: std.process.Init, allocator: std.mem.Allocator, args: *std.process.Args.Iterator, command: []const u8, options: backend.Options) !void {
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
    } else if (std.mem.eql(u8, command, "import")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        try storage.initStore(init.io, data_dir);
        try storage.writeImportManifest(init.io, allocator, data_dir, parquet_path);
        try printOut(init.io, "imported {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "import-hot")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var selected = backend.Backend.init(allocator, init.io, data_dir, options);
        defer selected.deinit();
        try selected.importParquet(parquet_path);
        try printOut(init.io, "imported hot columns {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "import-hot-extra")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importExtraHotColumns(parquet_path);
        try printOut(init.io, "imported extra hot columns {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "import-search-phrase-hot")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importSearchPhraseHot(parquet_path);
        try printOut(init.io, "imported SearchPhrase hot data {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "convert-hot")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.convertExistingHotCsv();
        try printOut(init.io, "converted hot.csv -> binary hot columns in {s}\n", .{data_dir});
    } else if (std.mem.eql(u8, command, "convert-search-phrase-id")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.convertSearchPhraseToId();
    } else if (std.mem.eql(u8, command, "import-url-dict")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importUrlDict(parquet_path);
        try printOut(init.io, "imported URL.dict.tsv {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "convert-url-id")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.convertUrlToId();
    } else if (std.mem.eql(u8, command, "convert-i64-csv")) {
        const csv_path = args.next() orelse return error.MissingCsvPath;
        const out_path = args.next() orelse return error.MissingOutputPath;
        try @import("native.zig").convertU64CsvToI64BinaryStreaming(allocator, init.io, csv_path, out_path);
        try printOut(init.io, "converted {s} -> {s}\n", .{ csv_path, out_path });
    } else if (std.mem.eql(u8, command, "convert-i16-csv")) {
        const csv_path = args.next() orelse return error.MissingCsvPath;
        const out_path = args.next() orelse return error.MissingOutputPath;
        try @import("native.zig").convertI16CsvToBinaryStreaming(allocator, init.io, csv_path, out_path);
        try printOut(init.io, "converted {s} -> {s}\n", .{ csv_path, out_path });
    } else if (std.mem.eql(u8, command, "build-string-column")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const col = args.next() orelse return error.MissingColumnName;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildStringColumn(col);
    } else if (std.mem.eql(u8, command, "build-q23-candidates")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ23Candidates();
    } else if (std.mem.eql(u8, command, "build-q25-candidates")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ25Candidates();
    } else if (std.mem.eql(u8, command, "build-q29-domain-stats")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ29DomainStats(options.chdb_python);
    } else if (std.mem.eql(u8, command, "build-q40-result")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ40Result(options.chdb_python);
    } else if (std.mem.eql(u8, command, "build-q21-count-google")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ21CountGoogle();
    } else if (std.mem.eql(u8, command, "convert-user-id-id")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.convertUserIdToId();
    } else if (std.mem.eql(u8, command, "import-d-cols")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importDColumns(parquet_path);
    } else if (std.mem.eql(u8, command, "build-stats")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildSegmentStats();
        try printOut(init.io, "built segment stats in {s}\n", .{data_dir});
    } else if (std.mem.eql(u8, command, "query")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const sql = args.next() orelse return error.MissingSql;
        try storage.ensureStore(init.io, data_dir);
        var selected = backend.Backend.init(allocator, init.io, data_dir, options);
        defer selected.deinit();
        const output = try selected.query(sql);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "query-csv")) {
        const csv_path = args.next() orelse return error.MissingCsvPath;
        const sql = args.next() orelse return error.MissingSql;
        const physical = @import("planner.zig").planCsv(sql) orelse return error.UnsupportedCsvQuery;
        var csv_reader = @import("reader.zig").CsvReader.init(allocator, init.io, csv_path);
        const output = try @import("executor.zig").executeCsv(allocator, &csv_reader, physical);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "native-status")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        try storage.ensureStore(init.io, data_dir);
        const hot = storage.hasHotBinary(init.io, data_dir) or storage.hasHotCsv(init.io, data_dir);
        const extra_hot = storage.hasExtraHotBinary(init.io, data_dir);
        const search_phrase_hot = storage.hasSearchPhraseHot(init.io, data_dir);
        try printOut(init.io, "native q1: yes (parquet metadata)\n", .{});
        try printOut(init.io, "native q2/q3/q4/q7/q8/q20/q30: {s}\n", .{if (hot) "yes (hot columns)" else "no (run import-hot)"});
        try printOut(init.io, "native q5/q6/q9-q15: yes if id/dict hot data exists (run convert-user-id-id, import-search-phrase-hot, convert-search-phrase-id, import-d-cols)\n", .{});
        try printOut(init.io, "native q6/q13 SearchPhrase hot: {s}\n", .{if (search_phrase_hot) "yes" else "no (run import-search-phrase-hot and convert-search-phrase-id)"});
        try printOut(init.io, "native q28: {s}\n", .{if (hot and extra_hot) "yes (extra hot columns)" else "no (run import-hot-extra)"});
        try printOut(init.io, "native q37/q38/q39/q40: no (URL/Title/Referer dictionary native paths TODO)\n", .{});
        try printOut(init.io, "native q41/q42/q43: {s}\n", .{if (hot and extra_hot) "yes (segment stats recommended; run build-stats)" else "no (run import-hot-extra and build-stats)"});
    } else if (std.mem.eql(u8, command, "bench")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const queries_path = args.next() orelse return error.MissingQueriesPath;
        try storage.ensureStore(init.io, data_dir);
        var selected = backend.Backend.init(allocator, init.io, data_dir, options);
        defer selected.deinit();
        try selected.bench(queries_path, .{});
    } else if (std.mem.eql(u8, command, "bench-one")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const queries_path = args.next() orelse return error.MissingQueriesPath;
        const query_num_text = args.next() orelse return error.MissingQueryNum;
        const query_num = try std.fmt.parseInt(usize, query_num_text, 10);
        try storage.ensureStore(init.io, data_dir);
        var selected = backend.Backend.init(allocator, init.io, data_dir, options);
        defer selected.deinit();
        try selected.bench(queries_path, .{ .first = query_num, .limit = 1 });
    } else if (std.mem.eql(u8, command, "bench-range")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const queries_path = args.next() orelse return error.MissingQueriesPath;
        const first_text = args.next() orelse return error.MissingQueryNum;
        const limit_text = args.next() orelse return error.MissingQueryLimit;
        const first = try std.fmt.parseInt(usize, first_text, 10);
        const limit = try std.fmt.parseInt(usize, limit_text, 10);
        try storage.ensureStore(init.io, data_dir);
        var selected = backend.Backend.init(allocator, init.io, data_dir, options);
        defer selected.deinit();
        try selected.bench(queries_path, .{ .first = first, .limit = limit });
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

fn printSchema(io: std.Io) !void {
    for (schema.hits_columns, 0..) |column, i| {
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

test "schema has ClickBench column count" {
    try std.testing.expectEqual(@as(usize, 105), schema.hits_columns.len);
}
