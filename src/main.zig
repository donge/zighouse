const std = @import("std");
const catalog = @import("catalog.zig");
const schema_infer = @import("schema_infer.zig");
const schema_persist = @import("ingest_schema_persist");
const schema_config = @import("ingest_schema_config");
const compactor = @import("compactor");
const mv_persist = @import("mv_persist");
const loader = @import("loader.zig");
const generic_sql = @import("generic_sql");
const parquet = @import("parquet");
const storage = @import("storage.zig");
const schema = @import("schema");
const generic_store = @import("generic_store");

const usage =
    \\zighouse - ClickHouse-compatible analytical database
    \\
    \\Usage:
    \\  zighouse serve [<data_dir>] [--port=<port>]
    \\  zighouse import <parquet> [--db=<db>] [--table=<t>] [--pk=<col>]
    \\  zighouse import-parquet [--format=generic|ch|ch-compact] [--pk=<col>] <parquet> <store> <table>
    \\  zighouse bench [--query=<N>] [--from=<N>] [--limit=<N>]
    \\  zighouse compact [--once] [--interval=<secs>]
    \\  zighouse query <sql>
    \\  zighouse inspect <parquet>
    \\  zighouse info [<data_dir>]
    \\  zighouse --help / --version
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
    if (std.mem.eql(u8, command, "import") or std.mem.eql(u8, command, "import-parquet")) {
        const legacy_import_parquet = std.mem.eql(u8, command, "import-parquet");
        // Generic Parquet import: infer schema, write generic_part store, write catalog manifest.
        // Usage: zighouse import <parquet> [--db=<db>] [--table=<t>] [--pk=<col>]
        var format: enum { generic, ch, ch_compact, ch_http } = .generic;
        var pk_col_name: ?[]const u8 = null;
        var db_name: []const u8 = "default";
        var bare_table: []const u8 = "hits";
        var store_dir: ?[]const u8 = null;
        const parquet_path = blk: {
            var first = args.next() orelse return error.MissingParquetPath;
            while (true) {
                if (std.mem.startsWith(u8, first, "--format=")) {
                    const fmt = first["--format=".len..];
                    if (std.mem.eql(u8, fmt, "ch")) format = .ch else if (std.mem.eql(u8, fmt, "ch-compact")) format = .ch_compact else if (std.mem.eql(u8, fmt, "ch-http")) format = .ch_http;
                    first = args.next() orelse return error.MissingParquetPath;
                } else if (std.mem.startsWith(u8, first, "--pk=")) {
                    pk_col_name = first["--pk=".len..];
                    first = args.next() orelse return error.MissingParquetPath;
                } else if (std.mem.startsWith(u8, first, "--db=")) {
                    db_name = first["--db=".len..];
                    first = args.next() orelse return error.MissingParquetPath;
                } else if (std.mem.startsWith(u8, first, "--table=")) {
                    bare_table = first["--table=".len..];
                    first = args.next() orelse return error.MissingParquetPath;
                } else {
                    break;
                }
            }
            break :blk first;
        };
        if (legacy_import_parquet) {
            store_dir = args.next() orelse return error.MissingStoreDir;
            bare_table = args.next() orelse return error.MissingTableName;
        }
        const store_path = store_dir orelse parquet_path;
        const table_name = bare_table;
        var inferred = try schema_infer.inferSchema(allocator, init.io, parquet_path, table_name);
        defer inferred.deinit();
        const row_count = switch (format) {
            .generic => try loader.importParquet(allocator, init.io, parquet_path, store_path, inferred.table),
            .ch => try loader.importParquetCH(allocator, init.io, parquet_path, store_path, inferred.table, pk_col_name),
            .ch_compact => try loader.importParquetCompact(allocator, init.io, parquet_path, store_path, inferred.table),
            .ch_http => blk: {
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
            .ch_compact => .ch_mergetree,
            .ch_http => .ch_mergetree,
        };
        if (format != .ch_http) {
            try catalog.Catalog.writeManifest(init.io, allocator, store_path, table_name, parquet_path, part_fmt);
        }
        const sort_keys: []const []const u8 = &.{};
        var table_with_keys = inferred.table;
        table_with_keys.sort_keys = sort_keys;

        const entry = schema_config.TableEntry{
            .db = db_name,
            .name = bare_table,
            .pk = pk_col_name,
            .table = table_with_keys,
        };
        const table_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ store_path, bare_table });
        defer allocator.free(table_dir);
        try schema_persist.saveToDir(init.io, allocator, table_dir, db_name, &entry);
        try printOut(init.io, "imported {d} rows {s} -> {s}/{s}\n", .{ row_count, parquet_path, store_path, table_name });
    } else if (std.mem.eql(u8, command, "inspect")) {
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
                for (inferred.table.columns) |c| if (c.ty != .text and c.ty != .char) {
                    n += 1;
                };
                break :blk n;
            },
            blk: {
                var n: usize = 0;
                for (inferred.table.columns) |c| if (c.ty == .text or c.ty == .char) {
                    n += 1;
                };
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
    } else if (std.mem.eql(u8, command, "info")) {
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
    } else if (std.mem.eql(u8, command, "bench")) {
        const duckdb = @import("query_bench.zig");
        const QueryRange = duckdb.QueryRange;
        const ir_planner = @import("ir_planner");
        const core = @import("core");
        const gsb = @import("generic_store_bridge");

        var store_dir: ?[]const u8 = null;
        var table_name: ?[]const u8 = null;
        var queries_path: ?[]const u8 = null;
        var bench_query: ?usize = null;
        var bench_from: ?usize = null;
        var bench_limit: ?usize = null;

        while (args.next()) |arg| {
            if (std.mem.startsWith(u8, arg, "--query=")) {
                bench_query = std.fmt.parseInt(usize, arg["--query=".len..], 10) catch null;
            } else if (std.mem.startsWith(u8, arg, "--from=")) {
                bench_from = std.fmt.parseInt(usize, arg["--from=".len..], 10) catch null;
            } else if (std.mem.startsWith(u8, arg, "--limit=")) {
                bench_limit = std.fmt.parseInt(usize, arg["--limit=".len..], 10) catch null;
            } else if (std.mem.startsWith(u8, arg, "--store=")) {
                store_dir = arg["--store=".len..];
            } else if (table_name == null) {
                table_name = arg;
            } else if (queries_path == null) {
                queries_path = arg;
            }
        }

        const bench_range: QueryRange = blk: {
            if (bench_query) |n| break :blk .{ .first = n, .limit = 1 };
            if (bench_from) |f| break :blk .{ .first = f, .limit = bench_limit orelse 43 };
            break :blk .{};
        };

        const sd = store_dir orelse return error.MissingStoreDir;
        const tn = table_name orelse return error.MissingTableName;
        const qp = queries_path orelse return error.MissingQueriesPath;

        const columns_txt = try std.fmt.allocPrint(allocator, "{s}/{s}/parts/all_1_1_0/columns.txt", .{ sd, tn });
        defer allocator.free(columns_txt);
        var inferred_schema = try schema_infer.loadSchemaFromColumnsTxt(allocator, init.io, columns_txt, tn);
        defer inferred_schema.deinit();
        const table: schema.Table = inferred_schema.table;

        const part_dir = try std.fmt.allocPrint(allocator, "{s}/{s}/parts/all_1_1_0", .{ sd, tn });
        defer allocator.free(part_dir);

        const IrRunner = struct {
            alloc: std.mem.Allocator,
            io: std.Io,
            part_dir: []const u8,
            table: schema.Table,
            bridge: *gsb.GenericStoreBridge,

            pub fn runQuery(self: *const @This(), query_text: []const u8) !?[]u8 {
                const maybe_gplan = generic_sql.parse(self.alloc, query_text) catch |err| {
                    std.log.err("bench parse error: {}: {s}", .{ err, query_text });
                    return null;
                };
                const gplan = maybe_gplan orelse {
                    std.log.err("bench parse null: {s}", .{query_text});
                    return null;
                };
                defer generic_sql.deinit(self.alloc, gplan);

                var arena = std.heap.ArenaAllocator.init(self.alloc);
                errdefer arena.deinit();
                var pctx = ir_planner.PlannerCtx.init(arena.allocator(), self.table);
                const node = ir_planner.plan_query(&pctx, gplan) catch |err| {
                    std.log.err("bench plan error: {}: {s}", .{ err, query_text });
                    arena.deinit();
                    return null;
                };
                if (node == null) {
                    std.log.err("bench plan null: {s}", .{query_text});
                    arena.deinit();
                    return null;
                }

                const pruned_cols = ir_planner.findPrunedCols(node.?);
                self.bridge.resetForNewQuery(pruned_cols);

                var qctx = core.exec.pipeline.QueryContext.init(self.alloc, self.bridge.source());
                defer qctx.deinit();
                const t_exec_start = std.Io.Clock.Timestamp.now(self.io, .awake);
                var rs = core.exec.pipeline.executePlan(node.?, &qctx) catch |err| {
                    std.log.err("bench exec error: {}: {s}", .{ err, query_text });
                    arena.deinit();
                    return null;
                };
                const t_exec_ns = @as(u64, @intCast(t_exec_start.durationTo(std.Io.Clock.Timestamp.now(self.io, .awake)).raw.nanoseconds));
                defer rs.deinit();
                arena.deinit();

                std.log.info("exec_ms={d} rows={d} sql={s}", .{ t_exec_ns / std.time.ns_per_ms, rs.num_rows, query_text[0..@min(80, query_text.len)] });

                const out = try std.fmt.allocPrint(self.alloc, "rows={d} exec_ms={d}\n", .{ rs.num_rows, t_exec_ns / std.time.ns_per_ms });
                return out;
            }
        };

        var shared_bridge = try gsb.GenericStoreBridge.init(
            allocator,
            init.io,
            part_dir,
            table,
            &.{},
        );
        defer shared_bridge.deinit();

        const runner = IrRunner{
            .alloc = allocator,
            .io = init.io,
            .part_dir = part_dir,
            .table = table,
            .bridge = &shared_bridge,
        };
        try duckdb.benchWithRunner(allocator, init.io, qp, bench_range, &runner);
    } else if (std.mem.eql(u8, command, "query")) {
        const ir_planner2 = @import("ir_planner");
        const core2 = @import("core");
        const gsb2 = @import("generic_store_bridge");
        const serializer = @import("serializer");

        const store_dir2 = args.next() orelse return error.MissingStoreDir;
        const table_name2 = args.next() orelse return error.MissingTableName;
        const query_text2 = args.next() orelse return error.MissingQuery;

        const columns_txt2 = try std.fmt.allocPrint(allocator, "{s}/{s}/parts/all_1_1_0/columns.txt", .{ store_dir2, table_name2 });
        defer allocator.free(columns_txt2);
        var inferred_schema2 = try schema_infer.loadSchemaFromColumnsTxt(allocator, init.io, columns_txt2, table_name2);
        defer inferred_schema2.deinit();
        const table2: schema.Table = inferred_schema2.table;

        const part_dir2 = try std.fmt.allocPrint(allocator, "{s}/{s}/parts/all_1_1_0", .{ store_dir2, table_name2 });
        defer allocator.free(part_dir2);

        const gplan2 = (try generic_sql.parse(allocator, query_text2)) orelse return error.ParseFailed;
        defer generic_sql.deinit(allocator, gplan2);

        var arena2 = std.heap.ArenaAllocator.init(allocator);
        defer arena2.deinit();
        var pctx2 = ir_planner2.PlannerCtx.init(arena2.allocator(), table2);
        const node2 = (try ir_planner2.plan_query(&pctx2, gplan2)) orelse return error.PlanFailed;

        const pruned2 = ir_planner2.findPrunedCols(node2);
        var bridge2 = try gsb2.GenericStoreBridge.init(allocator, init.io, part_dir2, table2, pruned2);
        defer bridge2.deinit();

        var qctx2 = core2.exec.pipeline.QueryContext.init(allocator, bridge2.source());
        defer qctx2.deinit();
        var rs2 = try core2.exec.pipeline.executePlan(node2, &qctx2);
        defer rs2.deinit();

        const csv2 = try serializer.toCsvOffset(allocator, rs2, gplan2.offset orelse 0);
        defer allocator.free(csv2);
        try writeOut(init.io, csv2);
    } else if (std.mem.eql(u8, command, "serve")) {
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
            } else if (data_dir == null and !std.mem.startsWith(u8, flag, "--")) {
                data_dir = flag;
            }
        }
        const dd = data_dir orelse return error.MissingDataDir;
        const ingest_server = @import("ingest_server");
        const ingest_schema_config = @import("ingest_schema_config");
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
    } else if (std.mem.eql(u8, command, "compact")) {
        var cfg = compactor.Config{ .data_dir = "" };
        while (args.next()) |arg| {
            if (std.mem.startsWith(u8, arg, "--data-dir=")) {
                cfg.data_dir = arg["--data-dir=".len..];
            } else if (std.mem.startsWith(u8, arg, "--interval=")) {
                cfg.interval_s = std.fmt.parseInt(u64, arg["--interval=".len..], 10) catch cfg.interval_s;
            } else if (std.mem.startsWith(u8, arg, "--min-parts=")) {
                cfg.min_parts_to_merge = std.fmt.parseInt(usize, arg["--min-parts=".len..], 10) catch cfg.min_parts_to_merge;
            } else if (std.mem.startsWith(u8, arg, "--max-parts=")) {
                cfg.max_parts_per_merge = std.fmt.parseInt(usize, arg["--max-parts=".len..], 10) catch cfg.max_parts_per_merge;
            } else if (std.mem.startsWith(u8, arg, "--max-rows=")) {
                cfg.max_rows_per_merge = std.fmt.parseInt(u64, arg["--max-rows=".len..], 10) catch cfg.max_rows_per_merge;
            } else if (std.mem.eql(u8, arg, "--once")) {
                cfg.once = true;
            } else if (std.mem.startsWith(u8, arg, "--codec=")) {
                const codec_str = arg["--codec=".len..];
                if (std.mem.eql(u8, codec_str, "zstd")) {
                    cfg.codec = 0x90;
                } else {
                    cfg.codec = 0x82;
                }
            }
        }
        if (cfg.data_dir.len == 0) return error.MissingDataDir;
        const mat_views = mv_persist.loadAll(allocator, init.io, cfg.data_dir) catch &.{};
        defer {
            for (mat_views) |*mv| {
                var m = mv.*;
                m.deinit();
            }
            allocator.free(mat_views);
        }
        cfg.mat_views = mat_views;
        try compactor.run(allocator, init.io, cfg);
    } else if (std.mem.eql(u8, command, "--version") or std.mem.eql(u8, command, "-v")) {
        try writeOut(init.io, "zighouse v1.0.2\n");
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

comptime {
    _ = catalog;
    _ = schema_infer;
}
