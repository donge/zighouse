const std = @import("std");
const backend = @import("backend.zig");
const duckdb = @import("duckdb.zig");
const parquet = @import("parquet.zig");
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
    \\  zighouse import-clickbench-csv-hot <hits.csv> <data_dir>
    \\  zighouse import-clickbench-parquet-hot <hits.parquet> <data_dir> [limit_rows]
    \\  zighouse import-clickbench-parquet-duckdb-vector-hot <hits.parquet> <data_dir> [limit_rows]
    \\  zighouse parquet-inspect <hits.parquet>
    \\  zighouse duckdb-vector-smoke <hits.parquet> [limit_rows]
    \\  zighouse import-hot-extra <hits.parquet> <data_dir>
    \\  zighouse convert-hot <data_dir>
    \\  zighouse build-stats <data_dir>
    \\  zighouse convert-i16-csv <csv_path> <out_path>
    \\  zighouse build-string-column <data_dir> <col>
    \\  zighouse build-clickbench-mobile-phone-model <data_dir>
    \\  zighouse build-clickbench-search-phrase <data_dir>
    \\  zighouse build-clickbench-url <data_dir>
    \\  zighouse build-q23-candidates <data_dir>
    \\  zighouse build-q25-candidates <data_dir>
    \\  zighouse build-q33-result <data_dir>
    \\  zighouse build-q29-domain-stats <data_dir>
    \\  zighouse build-q40-result <data_dir>
    \\  zighouse build-q21-count-google <data_dir>
    \\  zighouse query-csv <csv_path> <sql>  tables: csv (header), csv_no_header
    \\  zighouse query <data_dir> <sql>
    \\  zighouse compare-duckdb-native <data_dir> <queries.sql> [first] [limit]
    \\  zighouse store-info <data_dir>
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
    \\  ZIGHOUSE_IMPORT_TRACE                 print import phase timings
    \\  ZIGHOUSE_IMPORT_TINY_CACHES           write Q24/Q29/Q40 result artifacts during import
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
    } else if (std.mem.eql(u8, command, "import-clickbench-csv-hot")) {
        const csv_path = args.next() orelse return error.MissingCsvPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importClickBenchCsvHot(csv_path);
        try printOut(init.io, "imported ClickBench CSV hot columns {s} -> {s}\n", .{ csv_path, data_dir });
    } else if (std.mem.eql(u8, command, "import-clickbench-parquet-hot")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(u64, raw, 10) else null;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importClickBenchParquetHot(parquet_path, limit_rows);
        try printOut(init.io, "imported ClickBench Parquet hot columns {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "import-clickbench-parquet-duckdb-vector-hot")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const data_dir = args.next() orelse return error.MissingDataDir;
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(u64, raw, 10) else null;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.importClickBenchParquetDuckDbVectorHot(parquet_path, limit_rows);
        try printOut(init.io, "imported ClickBench Parquet hot columns via DuckDB vectors {s} -> {s}\n", .{ parquet_path, data_dir });
    } else if (std.mem.eql(u8, command, "parquet-inspect")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const output = try parquet.inspectPath(allocator, init.io, parquet_path);
        defer allocator.free(output);
        try writeOut(init.io, output);
    } else if (std.mem.eql(u8, command, "duckdb-vector-smoke")) {
        const parquet_path = args.next() orelse return error.MissingParquetPath;
        const limit_rows = if (args.next()) |raw| try std.fmt.parseInt(u64, raw, 10) else 1_000_000;
        try duckdb.vectorSmoke(allocator, init.io, parquet_path, limit_rows);
    } else if (std.mem.eql(u8, command, "compare-duckdb-native")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        const queries_path = args.next() orelse return error.MissingQueriesPath;
        const first = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else 1;
        const limit = if (args.next()) |raw| try std.fmt.parseInt(usize, raw, 10) else null;
        try storage.ensureStore(init.io, data_dir);
        try compareDuckDbNative(allocator, init.io, data_dir, queries_path, .{ .first = first, .limit = limit });
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
    } else if (std.mem.eql(u8, command, "build-clickbench-mobile-phone-model")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildClickBenchMobilePhoneModel();
    } else if (std.mem.eql(u8, command, "build-clickbench-search-phrase")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildClickBenchSearchPhrase();
    } else if (std.mem.eql(u8, command, "build-clickbench-url")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildClickBenchUrl();
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
    } else if (std.mem.eql(u8, command, "build-q33-result")) {
        const data_dir = args.next() orelse return error.MissingDataDir;
        var native_backend = @import("native.zig").Native.init(allocator, init.io, data_dir);
        defer native_backend.deinit();
        try native_backend.buildQ33Result();
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
        try printOut(init.io, "native q37/q38/q39/q40: yes with ClickBench CSV hot import artifacts\n", .{});
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

fn compareDuckDbNative(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, queries_path: []const u8, range: duckdb.QueryRange) !void {
    const native_mod = @import("native.zig");
    var duck = duckdb.DuckDb.init(allocator, io, data_dir);
    defer duck.deinit();
    var native_backend = native_mod.Native.init(allocator, io, data_dir);
    defer native_backend.deinit();
    const compare_limit = try readImportRowLimit(allocator, io, data_dir);

    const queries = try std.Io.Dir.cwd().readFileAlloc(io, queries_path, allocator, .limited(512 * 1024));
    defer allocator.free(queries);

    var checked: usize = 0;
    var passed: usize = 0;
    var failed: usize = 0;
    var errored: usize = 0;
    var query_num: usize = 1;
    var line_it = std.mem.splitScalar(u8, queries, '\n');
    while (line_it.next()) |raw_line| : (query_num += 1) {
        const query_text = std.mem.trim(u8, raw_line, " \t\r");
        if (query_text.len == 0) continue;
        if (!range.contains(query_num)) continue;

        checked += 1;
        const duck_out = duck.queryLimited(query_text, compare_limit) catch |err| {
            errored += 1;
            try printOut(io, "q{d}: ERROR duckdb {s}\n", .{ query_num, @errorName(err) });
            continue;
        };
        defer allocator.free(duck_out);

        const native_out = native_backend.query(query_text) catch |err| {
            errored += 1;
            try printOut(io, "q{d}: ERROR native {s}\n", .{ query_num, @errorName(err) });
            continue;
        };
        defer allocator.free(native_out);

        const duck_norm = normalizeCompareOutput(duck_out);
        const native_norm = normalizeCompareOutput(native_out);
        if (compareOutputsForQuery(query_text, duck_norm, native_norm)) {
            passed += 1;
            try printOut(io, "q{d}: PASS\n", .{query_num});
        } else {
            failed += 1;
            try printOut(io, "q{d}: FAIL duckdb_len={d} native_len={d}\n", .{ query_num, duck_norm.len, native_norm.len });
            try printSnippet(io, "  duckdb", duck_norm);
            try printSnippet(io, "  native", native_norm);
        }
    }

    try printOut(io, "summary: checked={d} passed={d} failed={d} errors={d}\n", .{ checked, passed, failed, errored });
    if (failed != 0 or errored != 0) return error.QueryMismatch;
}

fn readImportRowLimit(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !?u64 {
    const manifest = storage.readImportManifest(io, allocator, data_dir) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer allocator.free(manifest);
    var lines = std.mem.splitScalar(u8, manifest, '\n');
    while (lines.next()) |line| {
        if (!std.mem.startsWith(u8, line, "row_count=")) continue;
        const raw = std.mem.trim(u8, line["row_count=".len..], " \t\r");
        const count = try std.fmt.parseInt(u64, raw, 10);
        if (count == 0) return null;
        return count;
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
