/// HTTP ingest + query server for zighouse.
///
/// Listens on a TCP port.  Three request types are handled:
///
/// CREATE TABLE (DDL):
///   POST /?query=CREATE+TABLE+[db.]table+(...)ENGINE=MergeTree+ORDER+BY+col
///   → Parses DDL, registers schema in memory, persists schema.json.
///   → Idempotent: if table already exists the request is a no-op (200 OK).
///
/// INSERT RowBinary (schema must exist):
///   POST /?query=INSERT+INTO+<db>.<table>+FORMAT+RowBinary
///   Body: raw RowBinary bytes
///   → Decodes using known schema, writes a new MergeTree wide part.
///
/// INSERT RowBinaryWithNamesAndTypes (schema inferred from payload):
///   POST /?query=INSERT+INTO+<db>.<table>+FORMAT+RowBinaryWithNamesAndTypes
///   Body: RowBinaryWithNamesAndTypes header + rows
///   → Extracts schema from body header.
///   → If table unknown: registers schema + persists schema.json automatically.
///   → If table known: validates incoming schema matches stored schema.
///   → Writes a new MergeTree wide part.
///
/// SELECT (query):
///   GET  /?query=SELECT+...+FROM+<db>.<table>+...
///   POST /?query=SELECT+...  (body ignored)
///   → Scans all parts under data_dir/db/table/parts/, runs generic_executor,
///     returns TSV (tab-separated) result.
///
/// Schema auto-load:
///   On startup the server scans data_dir for <db>/<table>/schema.json files
///   and loads them all.  Additional schemas can be injected via --schemas at startup.
///
/// On success: 200 OK.
/// On error:   400/500 with plain-text error message.
const std = @import("std");
const schema = @import("schema");
const schema_config = @import("schema_config");
const schema_persist = @import("schema_persist");
const part_scanner = @import("part_scanner");
const row_binary_decoder = @import("row_binary_decoder");
const part_writer_session = @import("part_writer_session");
const generic_sql = @import("generic_sql");
const ddl_parser = @import("ddl_parser");
const mv_parse = @import("mv_parse");
const mv_persist = @import("mv_persist");
const native_block = @import("native_block");
const serializer = @import("serializer");
const core = @import("core");
const ir_planner = @import("ir_planner");
const part_scan_bridge = @import("part_scan_bridge");
const tcp_server = @import("tcp_server");

/// Server configuration.
pub const Config = struct {
    data_dir: []const u8,
    port: u16 = 8123,
    /// Optional extra schemas to seed on startup (in addition to auto-loaded ones).
    extra_schemas: ?*const schema_config.SchemaConfig = null,
};

/// When `port` is set, TCP listens on that port and HTTP listens on `port + 1`.
/// Default: TCP=9000, HTTP=8123 (legacy behavior when port=8123).
fn tcpPort(cfg: Config) u16 {
    if (cfg.port == 8123) return 9000; // legacy default
    return cfg.port;
}
fn httpPort(cfg: Config) u16 {
    if (cfg.port == 8123) return 8123;
    return cfg.port + 1;
}

pub const Server = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    /// Live schema registry (heap-allocated, mutable at runtime).
    schemas: schema_config.SchemaConfig,
    /// Monotonically increasing part sequence number (per-process, not per-table).
    /// Atomic so both the HTTP and TCP servers can increment it safely.
    seq: std.atomic.Value(u64),
    /// In-memory view registry: view_name → SELECT SQL (owned strings).
    views: std.StringHashMap([]const u8),
    /// In-memory function registry: fn_name → lambda text "(params) -> body" (owned strings).
    functions: std.StringHashMap([]const u8),
    /// In-memory materialized view registry: "db.mv_name" → MatViewEntry (owned).
    mat_views: std.StringHashMap(mv_parse.MatViewEntry),

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: Config) !Server {
        // Load all persisted schemas from disk.
        var schemas = try schema_persist.loadAll(allocator, io, config.data_dir);
        errdefer schemas.deinit();

        // Merge in any extra schemas passed at startup.
        if (config.extra_schemas) |extra| {
            for (extra.tables) |*entry| {
                // Only add if not already present (disk takes precedence).
                if (schemas.find(entry.db, entry.name) == null) {
                    try schemas.addEntry(allocator, entry.*);
                }
            }
        }

        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .schemas = schemas,
            .seq = std.atomic.Value(u64).init(scanMaxPartSeq(io, config.data_dir) + 1),
            .views = std.StringHashMap([]const u8).init(allocator),
            .functions = std.StringHashMap([]const u8).init(allocator),
            .mat_views = blk: {
                var mv_map = std.StringHashMap(mv_parse.MatViewEntry).init(allocator);
                const loaded = mv_persist.loadAll(allocator, io, config.data_dir) catch &.{};
                for (loaded) |entry| {
                    const key = std.fmt.allocPrint(allocator, "{s}.{s}", .{ entry.db, entry.mv_name }) catch continue;
                    mv_map.put(key, entry) catch {
                        allocator.free(key);
                        continue;
                    };
                }
                allocator.free(loaded);
                break :blk mv_map;
            },
        };
    }

    pub fn deinit(self: *Server) void {
        self.schemas.deinit();
        var it = self.views.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.views.deinit();
        var fit = self.functions.iterator();
        while (fit.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.functions.deinit();
        var mv_it = self.mat_views.iterator();
        while (mv_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var mv = entry.value_ptr.*;
            mv.deinit();
        }
        self.mat_views.deinit();
    }

    /// Block and serve requests until an error or signal.
    pub fn run(self: *Server) !void {
        // Spawn TCP server (port 9000) in a background thread.
        const tcp_thread = std.Thread.spawn(.{}, tcpServerThread, .{self}) catch |err| blk: {
            std.debug.print("tcp: failed to start Native TCP server: {s}\n", .{@errorName(err)});
            break :blk null;
        };
        _ = tcp_thread; // detached; runs until process exit

        const net = std.Io.net;
        const address = try net.IpAddress.parseIp4("127.0.0.1", httpPort(self.config));
        var listener = try address.listen(self.io, .{});
        defer listener.deinit(self.io);

        std.debug.print("zighouse serve listening on 127.0.0.1:{d}\n", .{httpPort(self.config)});

        while (true) {
            const stream = try listener.accept(self.io);
            self.handleConnection(stream) catch |err| {
                std.debug.print("connection error: {s}\n", .{@errorName(err)});
            };
        }
    }

    fn tcpServerThread(self: *Server) void {
        var ctx = tcp_server.ServerCtx{
            .allocator = self.allocator,
            .io = self.io,
            .data_dir = self.config.data_dir,
            .schemas = &self.schemas,
            .seq = &self.seq,
        };
        tcp_server.listenAndServe(&ctx, tcpPort(self.config)) catch |err| {
            std.debug.print("tcp: server exited: {s}\n", .{@errorName(err)});
        };
    }

    // ── Per-connection handling ────────────────────────────────────────────────

    fn handleConnection(self: *Server, stream: std.Io.net.Stream) !void {
        defer stream.close(self.io);

        var read_buf: [64 * 1024]u8 = undefined;
        var write_buf: [64 * 1024]u8 = undefined;

        var net_reader = stream.reader(self.io, &read_buf);
        var net_writer = stream.writer(self.io, &write_buf);

        var http_server = std.http.Server.init(&net_reader.interface, &net_writer.interface);

        while (true) {
            var request = http_server.receiveHead() catch |err| switch (err) {
                error.HttpConnectionClosing => return,
                else => return err,
            };

            self.handleRequest(&request, &net_writer.interface) catch |err| {
                std.debug.print("request error: {s}\n", .{@errorName(err)});
                return err;
            };

            if (!request.head.keep_alive) return;
        }
    }

    fn handleRequest(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer) !void {
        const target = request.head.target;

        // Handle /ping — health check endpoint (GET or POST, no body needed).
        if (std.mem.startsWith(u8, target, "/ping")) {
            try sendResponse(request, out, .ok, "Ok.\n");
            return;
        }

        // Extract the `query` parameter from the URL.
        // clickhouse-go HTTP mode sends the SQL in the POST body (no ?query= param).
        var decoded_buf: [64 * 1024]u8 = undefined;

        if (extractQueryParam(target, "query")) |query_param| {
            // Classic ?query=... path (curl, our scripts).
            const decoded = urlDecode(query_param, &decoded_buf) catch {
                try sendResponse(request, out, .bad_request, "Failed to decode query parameter\n");
                return;
            };
            const trimmed_raw = std.mem.trim(u8, decoded, " \t\r\n");
            // Substitute $N parameters from URL query string params.
            const after_params = try substituteParams(self.allocator, target, trimmed_raw);
            defer self.allocator.free(after_params);
            // When default_format is explicitly specified (e.g. by the CH test harness),
            // suppress the column-name header line; otherwise include it for HTTP API clients.
            const has_explicit_fmt = extractQueryParam(target, "default_format") != null;
            try self.dispatchSqlWithHeader(request, out, after_params, !has_explicit_fmt);
        } else {
            // clickhouse-go sends SQL in POST body.
            // The body may contain: just the SQL (DDL/SELECT), or SQL\ndata (INSERT).
            // We read the full body and split at first newline if it's an INSERT.
            var body_buf: [4096]u8 = undefined;
            request.head.expect = null; // suppress Expect: 100-continue handling
            const body_reader = request.readerExpectNone(&body_buf);
            const max_body = 256 * 1024 * 1024;
            const body = try body_reader.allocRemaining(self.allocator, .limited(max_body));
            defer self.allocator.free(body);

            // Split SQL from optional data payload only for INSERT with FORMAT.
            // clickhouse-go sends INSERT as: "INSERT INTO ... FORMAT ...\n<binary data>"
            // INSERT with VALUES uses the entire body as SQL (multi-line allowed).
            // For SELECT/CREATE the entire body is SQL (may contain newlines).
            const trimmed_check = std.mem.trim(u8, body, " \t\r\n");
            const is_insert = asciiStartsWith(trimmed_check, "INSERT");
            // Only split at first newline if there's a FORMAT keyword on the first line.
            const first_nl = std.mem.indexOfScalar(u8, body, '\n');
            const has_format = if (first_nl) |nl_pos| blk: {
                const first_line = body[0..nl_pos];
                break :blk std.mem.indexOf(u8, first_line, "FORMAT") != null;
            } else false;
            const nl = if (is_insert and has_format) first_nl else null;
            const sql_part = if (nl) |n| body[0..n] else body;
            const data_part = if (nl) |n| body[n + 1 ..] else body[body.len..];

            const trimmed_raw = std.mem.trim(u8, sql_part, " \t\r\n");
            if (trimmed_raw.len == 0) {
                try sendResponse(request, out, .bad_request, "Empty query\n");
                return;
            }
            // Substitute $N parameters from URL query string params.
            const after_params = try substituteParams(self.allocator, target, trimmed_raw);
            defer self.allocator.free(after_params);
            // Strip trailing FORMAT <name> clause (added by clickhouse-go) — only for non-INSERT.
            // INSERT handlers use parseInsertTarget which needs the FORMAT keyword.
            const trimmed = if (asciiStartsWith(after_params, "INSERT"))
                after_params
            else
                stripFormatClause(after_params);
            try self.dispatchSqlWithData(request, out, trimmed, data_part);
        }
    }

    fn dispatchSqlWithHeader(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, trimmed: []const u8, with_header: bool) !void {
        if (asciiStartsWith(trimmed, "INSERT")) {
            try self.handleInsert(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "SELECT") or asciiStartsWith(trimmed, "WITH")) {
            try self.handleSelectNoDrainEx(request, out, trimmed, true, !with_header);
        } else if (asciiStartsWith(trimmed, "CREATE")) {
            try self.handleCreate(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "TRUNCATE")) {
            try self.handleTruncate(request, out, trimmed);
            try sendResponse(request, out, .ok, "");
        } else if (asciiStartsWith(trimmed, "DROP") or
            asciiStartsWith(trimmed, "SYSTEM") or
            asciiStartsWith(trimmed, "ALTER") or
            asciiStartsWith(trimmed, "SET"))
        {
            // DROP TABLE: actually remove schema and data.
            if (asciiStartsWith(trimmed, "DROP TABLE") or
                std.ascii.startsWithIgnoreCase(trimmed, "DROP TABLE"))
            {
                try self.handleDropTable(trimmed);
            } else if (asciiStartsWith(trimmed, "ALTER TABLE")) {
                try self.handleAlterTable(trimmed);
            }
            // Other DDL (SYSTEM, SET, DROP PARTITION, etc.) — no-op
            try sendResponse(request, out, .ok, "");
        } else if (asciiStartsWith(trimmed, "START") or
            asciiStartsWith(trimmed, "COMMIT") or
            asciiStartsWith(trimmed, "ROLLBACK") or
            asciiStartsWith(trimmed, "GRANT") or
            asciiStartsWith(trimmed, "REVOKE"))
        {
            // Transaction and privilege statements — no-op (non-transactional).
            try sendResponse(request, out, .ok, "");
        } else if (asciiStartsWith(trimmed, "DELETE") or
            asciiStartsWith(trimmed, "UPDATE"))
        {
            // DELETE / UPDATE — no-op (not implemented in generic store path).
            try sendResponse(request, out, .ok, "");
        } else {
            try sendResponse(request, out, .bad_request, "Only CREATE TABLE, INSERT and SELECT are supported\n");
        }
    }

    /// Like dispatchSql but for body-SQL mode where data_part holds INSERT payload.
    fn dispatchSqlWithData(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, trimmed: []const u8, data_part: []const u8) !void {
        if (asciiStartsWith(trimmed, "INSERT")) {
            try self.handleInsertBodyData(request, out, trimmed, data_part);
        } else if (asciiStartsWith(trimmed, "SELECT") or asciiStartsWith(trimmed, "WITH")) {
            try self.handleSelectSimple(request, out, trimmed, false);
        } else if (asciiStartsWith(trimmed, "CREATE")) {
            try self.handleCreateSimple(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "DESCRIBE")) {
            try self.handleDescribeSimple(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "TRUNCATE")) {
            try self.handleTruncate(request, out, trimmed);
            try self.sendEmptyNativeBlock(request, out);
        } else {
            // Other DDL/admin commands (SYSTEM, DROP, ALTER, SET, etc.) — no-op
            try self.sendEmptyNativeBlock(request, out);
        }
    }

    // ── CREATE TABLE handler ───────────────────────────────────────────────────

    fn handleCreate(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Drain body (DDL has no body).
        var body_buf: [64]u8 = undefined;
        _ = request.readerExpectNone(&body_buf);
        return self.handleCreateCore(request, out, sql, false);
    }

    /// Shared CREATE logic.
    /// native_path=false → respond with HTTP 200 + empty body (TSV/HTTP path).
    /// native_path=true  → respond with empty Native block (TCP path).
    fn handleCreateCore(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8, native_path: bool) !void {
        // Inline helper: send success response appropriate for the protocol.
        const respondOk = struct {
            fn f(srv: *Server, req: *std.http.Server.Request, o: *std.Io.Writer, native: bool) !void {
                if (native) try srv.sendEmptyNativeBlock(req, o) else try sendResponse(req, o, .ok, "");
            }
        }.f;

        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it.next(); // CREATE
        const second = it.next() orelse "";
        const third = it.next() orelse "";

        // CREATE DATABASE — no-op.
        if (std.ascii.eqlIgnoreCase(second, "DATABASE")) {
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE DICTIONARY — no-op (dictionary support is not implemented yet).
        if (std.ascii.eqlIgnoreCase(second, "DICTIONARY")) {
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE VIEW [OR REPLACE] — store view definition.
        if (std.ascii.eqlIgnoreCase(second, "VIEW") or
            (std.ascii.eqlIgnoreCase(second, "OR") and std.ascii.eqlIgnoreCase(third, "REPLACE") and blk: {
                var it3 = std.mem.tokenizeAny(u8, sql, " \t\r\n");
                _ = it3.next(); // CREATE
                _ = it3.next(); // OR
                _ = it3.next(); // REPLACE
                const fourth = it3.next() orelse "";
                break :blk std.ascii.eqlIgnoreCase(fourth, "VIEW");
            }))
        {
            // Parse: CREATE [OR REPLACE] VIEW [db.]name AS <select>
            const view_kw_pos = std.ascii.indexOfIgnoreCase(sql, "VIEW ") orelse {
                try respondOk(self, request, out, native_path);
                return;
            };
            const after_view = sql[view_kw_pos + 5 ..];
            const as_pos = std.ascii.indexOfIgnoreCase(after_view, " AS ") orelse {
                try respondOk(self, request, out, native_path);
                return;
            };
            const view_full_name = std.mem.trim(u8, after_view[0..as_pos], " \t\r\n");
            const select_sql = std.mem.trim(u8, after_view[as_pos + 4 ..], " \t\r\n");
            const view_name = if (std.mem.indexOfScalar(u8, view_full_name, '.')) |dot_pos|
                view_full_name[dot_pos + 1 ..]
            else
                view_full_name;
            const key_short = try self.allocator.dupe(u8, view_name);
            errdefer self.allocator.free(key_short);
            const val = try self.allocator.dupe(u8, select_sql);
            errdefer self.allocator.free(val);
            if (self.views.getEntry(key_short)) |existing| {
                self.allocator.free(existing.value_ptr.*);
            }
            try self.views.put(key_short, val);
            if (std.mem.indexOfScalar(u8, view_full_name, '.') != null) {
                const key_full = try self.allocator.dupe(u8, view_full_name);
                errdefer self.allocator.free(key_full);
                const val2 = try self.allocator.dupe(u8, select_sql);
                errdefer self.allocator.free(val2);
                if (self.views.getEntry(key_full)) |existing2| {
                    self.allocator.free(existing2.value_ptr.*);
                }
                try self.views.put(key_full, val2);
            }
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE [OR REPLACE] FUNCTION — store function definition.
        if (std.ascii.eqlIgnoreCase(second, "FUNCTION") or
            (std.ascii.eqlIgnoreCase(second, "OR") and std.ascii.eqlIgnoreCase(third, "REPLACE")))
        {
            const fn_kw_pos = std.ascii.indexOfIgnoreCase(sql, "FUNCTION ") orelse {
                try respondOk(self, request, out, native_path);
                return;
            };
            const after_fn = sql[fn_kw_pos + 9 ..];
            var tok_it = std.mem.tokenizeAny(u8, after_fn, " \t\r\n");
            const fn_name_tok = tok_it.next() orelse {
                try respondOk(self, request, out, native_path);
                return;
            };
            const as_pos2 = std.ascii.indexOfIgnoreCase(after_fn, " AS ") orelse {
                try respondOk(self, request, out, native_path);
                return;
            };
            const lambda_body = std.mem.trim(u8, after_fn[as_pos2 + 4 ..], " \t\r\n");
            const fn_key = try self.allocator.dupe(u8, fn_name_tok);
            errdefer self.allocator.free(fn_key);
            const fn_val = try self.allocator.dupe(u8, lambda_body);
            errdefer self.allocator.free(fn_val);
            if (self.functions.getEntry(fn_key)) |existing| {
                self.allocator.free(existing.value_ptr.*);
            }
            try self.functions.put(fn_key, fn_val);
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE MATERIALIZED VIEW — parse and persist.
        if (std.ascii.eqlIgnoreCase(second, "MATERIALIZED") and std.ascii.eqlIgnoreCase(third, "VIEW")) {
            var parsed_mv = mv_parse.parse(self.allocator, sql) catch |err| {
                if (native_path) {
                    try self.sendEmptyNativeBlock(request, out);
                } else {
                    const msg = try std.fmt.allocPrint(self.allocator, "MV parse error: {s}\n", .{@errorName(err)});
                    defer self.allocator.free(msg);
                    try sendResponse(request, out, .bad_request, msg);
                }
                return;
            };
            // Idempotent: if MV already exists, succeed without overwriting.
            const mv_key = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ parsed_mv.db, parsed_mv.mv_name });
            if (self.mat_views.contains(mv_key)) {
                self.allocator.free(mv_key);
                parsed_mv.deinit();
            } else {
                mv_persist.save(self.io, self.allocator, self.config.data_dir, &parsed_mv) catch |e| {
                    std.debug.print("mv_persist.save warning: {s}\n", .{@errorName(e)});
                };
                try self.mat_views.put(mv_key, parsed_mv);
            }
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE SCHEMA — no-op (schemas are implicit in zighouse).
        if (std.ascii.eqlIgnoreCase(second, "SCHEMA")) {
            try respondOk(self, request, out, native_path);
            return;
        }
        // CREATE ROLE / DOMAIN / TYPE / SEQUENCE / COLLATION / CHARACTER SET / TRANSLATION — no-op.
        if (std.ascii.eqlIgnoreCase(second, "ROLE") or
            std.ascii.eqlIgnoreCase(second, "DOMAIN") or
            std.ascii.eqlIgnoreCase(second, "TYPE") or
            std.ascii.eqlIgnoreCase(second, "SEQUENCE") or
            std.ascii.eqlIgnoreCase(second, "COLLATION") or
            (std.ascii.eqlIgnoreCase(second, "CHARACTER") and std.ascii.eqlIgnoreCase(third, "SET")) or
            std.ascii.eqlIgnoreCase(second, "TRANSLATION"))
        {
            try respondOk(self, request, out, native_path);
            return;
        }

        var parsed = ddl_parser.parse(self.allocator, sql) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "DDL parse error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer parsed.deinit();

        // Idempotent: if table already exists, succeed without overwriting.
        if (self.schemas.find(parsed.entry.db, parsed.entry.name) == null) {
            try self.schemas.addEntry(self.allocator, parsed.entry);
            const stored = self.schemas.find(parsed.entry.db, parsed.entry.name).?;
            self.tryPersistSchema(stored.db, stored);
        }

        try respondOk(self, request, out, native_path);
    }

    // ── INSERT handler ─────────────────────────────────────────────────────────

    fn handleInsert(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // VALUES INSERT: INSERT INTO table VALUES (...)
        if (std.ascii.indexOfIgnoreCase(sql, " VALUES") != null) {
            try self.handleInsertValues(request, out, sql);
            return;
        }

        // INSERT INTO table SELECT ...: detect SELECT keyword after table name
        if (std.ascii.indexOfIgnoreCase(sql, " SELECT ") != null or
            std.ascii.indexOfIgnoreCase(sql, "\nSELECT ") != null or
            std.ascii.endsWithIgnoreCase(sql, " SELECT *"))
        {
            try self.handleInsertSelect(request, out, sql);
            return;
        }

        // Detect format: RowBinary vs RowBinaryWithNamesAndTypes
        const insert_info = parseInsertTarget(sql) orelse {
            try sendResponse(request, out, .bad_request, "Expected: INSERT INTO <db>.<table> FORMAT RowBinary[WithNamesAndTypes]\n");
            return;
        };

        // Read body — use allocRemaining for correct chunked TE handling.
        var body_buf: [256]u8 = undefined;
        const body_reader = request.readerExpectNone(&body_buf);
        const max_body: usize = 256 * 1024 * 1024;
        const body = try body_reader.allocRemaining(self.allocator, .limited(max_body));
        defer self.allocator.free(body);

        if (body.len == 0) {
            try sendResponse(request, out, .ok, "");
            return;
        }

        if (insert_info.native_fmt) {
            try self.handleInsertNative(request, out, insert_info.db_table, body);
        } else if (insert_info.with_names_and_types) {
            try self.handleInsertWithHeader(request, out, insert_info.db_table, body);
        } else if (insert_info.csv_fmt) {
            try self.handleInsertJSONEachRow(request, out, insert_info.db_table, body);
        } else if (insert_info.json_each_row_fmt) {
            try self.handleInsertJSONEachRow(request, out, insert_info.db_table, body);
        } else {
            try self.handleInsertRowBinary(request, out, insert_info.db_table, body);
        }
    }

    fn handleInsertRowBinary(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        // Schema must already exist.
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            const msg = try std.fmt.allocPrint(self.allocator, "Unknown table '{s}.{s}': use CREATE TABLE or RowBinaryWithNamesAndTypes first\n", .{ db_table.db, db_table.table });
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };

        var dec = try row_binary_decoder.RowBinaryDecoder.init(self.allocator, entry.table);
        defer dec.deinit();
        const n_rows = dec.decode(body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "RowBinary decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };

        if (n_rows == 0) {
            try sendResponse(request, out, .ok, "");
            return;
        }

        try self.writePart(db_table, entry, dec.columns);
        try sendResponse(request, out, .ok, "");
    }

    fn handleInsertNative(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        // Decode Native Block body (columnar format with metadata).
        var decoded = row_binary_decoder.decodeNativeBlock(self.allocator, body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "Native Block decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (decoded.table.columns.len == 0) {
            try self.sendEmptyNativeBlock(request, out);
            return;
        }

        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            try self.writePart(db_table, existing, decoded.decoder.columns);
        } else {
            // Auto-register schema
            const new_entry = schema_config.TableEntry{
                .db = db_table.db,
                .table = .{ .name = db_table.table, .columns = decoded.table.columns },
                .name = db_table.table,
                .pk = null,
            };
            try self.schemas.addEntry(self.allocator, new_entry);
            const stored = self.schemas.find(db_table.db, db_table.table).?;
            self.tryPersistSchema(stored.db, stored);
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        try self.sendEmptyNativeBlock(request, out);
    }

    fn handleInsertWithHeader(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        // Decode header + rows from body.
        var decoded = row_binary_decoder.decodeWithHeader(self.allocator, body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "RowBinaryWithNamesAndTypes decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        // If table exists: validate schema compatibility.
        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            if (!schemasCompatible(existing.table, decoded.table)) {
                try sendResponse(request, out, .bad_request, "Schema mismatch: incoming columns don't match registered schema\n");
                return;
            }
            // Use existing schema (pk info etc.) for the write.
            try self.writePart(db_table, existing, decoded.decoder.columns);
        } else {
            // Auto-register schema from header.
            const new_entry = schema_config.TableEntry{
                .db = db_table.db,
                .table = .{ .name = db_table.table, .columns = decoded.table.columns },
                .name = db_table.table,
                .pk = null,
            };
            try self.schemas.addEntry(self.allocator, new_entry);
            const stored = self.schemas.find(db_table.db, db_table.table).?;
            // Persist schema.json
            self.tryPersistSchema(stored.db, stored);
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        try sendResponse(request, out, .ok, "");
    }

    /// INSERT FORMAT CSV — comma-separated values, one row per line.
    /// Requires table schema to be pre-registered (no auto-inference).
    /// INSERT FORMAT CSV / JSONEachRow — one JSON object per line.
    fn handleInsertJSONEachRow(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            try sendResponse(request, out, .bad_request, "Unknown table; use CREATE TABLE first\n");
            return;
        };
        const cols = try row_binary_decoder.ColumnBuffer.initAll(self.allocator, entry.table);
        defer row_binary_decoder.ColumnBuffer.deinitAll(self.allocator, cols);

        var line_it = std.mem.splitScalar(u8, body, '\n');
        while (line_it.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (trimmed.len == 0 or trimmed[0] != '{') continue;
            for (entry.table.columns, cols) |col, *buf| {
                const val = extractJsonField(trimmed, col.name) orelse "";
                try appendParsedField(self.allocator, col, val, buf);
            }
        }
        try self.writePart(db_table, entry, cols);
        try sendResponse(request, out, .ok, "");
    }

    /// Write one part to disk and increment seq.
    fn writePart(
        self: *Server,
        db_table: DbTable,
        entry: *const schema_config.TableEntry,
        columns: []row_binary_decoder.ColumnBuffer,
    ) !void {
        const seq = self.seq.fetchAdd(1, .monotonic);

        var sess = try part_writer_session.CompactPartWriterSession.open(
            self.allocator,
            self.io,
            self.config.data_dir,
            db_table.db,
            db_table.table,
            entry.table,
            seq,
            0x82, // METHOD_LZ4
        );
        defer sess.deinit();

        // Reorder decoded columns to match the registered schema column order.
        // The client may send columns in a different order than the schema.
        const schema_cols = entry.table.columns;
        const reordered = try self.allocator.alloc(row_binary_decoder.ColumnBuffer, schema_cols.len);
        defer self.allocator.free(reordered);

        // Build a null-initialized empty ColumnBuffer for missing columns.
        const empty_col = row_binary_decoder.ColumnBuffer.initEmpty;
        for (schema_cols, reordered) |sc, *out_col| {
            out_col.* = empty_col(sc);
            for (columns) |*inc| {
                if (std.ascii.eqlIgnoreCase(inc.col.name, sc.name)) {
                    out_col.* = inc.*;
                    break;
                }
            }
        }

        try sess.writeColumns(reordered);
        try sess.finish();
    }

    /// Parse "TRUNCATE TABLE [db.]table" and delete all parts under that table.
    /// Silently succeeds if the table or its parts directory does not exist.
    fn handleTruncate(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        _ = request;
        _ = out;
        // Parse: TRUNCATE [TABLE] [db.]table
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n;");
        _ = it.next(); // "TRUNCATE"
        const maybe_table_kw = it.next() orelse return;
        const tbl_token = if (std.ascii.eqlIgnoreCase(maybe_table_kw, "TABLE")) (it.next() orelse return) else maybe_table_kw;
        // Strip trailing semicolons
        const tbl_name = std.mem.trimEnd(u8, tbl_token, ";");
        const dbt = splitDbTable(tbl_name);

        const parts_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}/parts",
            .{ self.config.data_dir, dbt.db, dbt.table },
        );
        defer self.allocator.free(parts_path);

        const cwd = std.Io.Dir.cwd();
        var parts_dir = cwd.openDir(self.io, parts_path, .{ .iterate = true }) catch return; // not found → ok
        defer parts_dir.close(self.io);

        // Collect part directory names to delete (can't delete while iterating).
        var to_delete: std.ArrayListUnmanaged([]u8) = .empty;
        defer {
            for (to_delete.items) |p| self.allocator.free(p);
            to_delete.deinit(self.allocator);
        }

        var it2 = parts_dir.iterate();
        while (try it2.next(self.io)) |entry| {
            if (entry.kind != .directory) continue;
            const full = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ parts_path, entry.name });
            try to_delete.append(self.allocator, full);
        }

        for (to_delete.items) |part_path| {
            cwd.deleteTree(self.io, part_path) catch {}; // best-effort
        }
    }

    /// DROP TABLE [IF EXISTS] [db.]table — remove schema and all data parts.
    fn handleDropTable(self: *Server, sql: []const u8) !void {
        // Parse: DROP TABLE [IF EXISTS] [db.]table
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n;");
        _ = it.next(); // "DROP"
        _ = it.next(); // "TABLE"
        const next_tok = it.next() orelse return;
        const tbl_token: []const u8 = if (std.ascii.eqlIgnoreCase(next_tok, "IF")) blk: {
            _ = it.next(); // "EXISTS"
            break :blk it.next() orelse return;
        } else next_tok;
        const tbl_name = std.mem.trimEnd(u8, tbl_token, ";");
        const dbt = splitDbTable(tbl_name);

        // Remove schema entry.
        self.schemas.removeEntry(dbt.db, dbt.table);

        // Delete schema.json.
        const schema_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}/schema.json",
            .{ self.config.data_dir, dbt.db, dbt.table },
        );
        defer self.allocator.free(schema_path);
        const cwd = std.Io.Dir.cwd();
        cwd.deleteFile(self.io, schema_path) catch {};

        // Delete all parts (entire table directory).
        const table_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}",
            .{ self.config.data_dir, dbt.db, dbt.table },
        );
        defer self.allocator.free(table_path);
        cwd.deleteTree(self.io, table_path) catch {};
    }

    /// ALTER TABLE [db.]table ADD [COLUMN] col Type
    /// ALTER TABLE [db.]table DROP [COLUMN] col
    fn handleAlterTable(self: *Server, sql: []const u8) !void {
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it.next(); // ALTER
        _ = it.next(); // TABLE
        const tbl_tok = it.next() orelse return;
        const tbl = std.mem.trim(u8, tbl_tok, ";");

        var db: []const u8 = "default";
        var table_name: []const u8 = tbl;
        if (std.mem.indexOfScalar(u8, tbl, '.')) |dot| {
            db = tbl[0..dot];
            table_name = tbl[dot + 1 ..];
        }

        const existing = self.schemas.find(db, table_name) orelse return;
        const action_tok = it.next() orelse return;

        if (std.ascii.eqlIgnoreCase(action_tok, "ADD")) {
            const maybe_col = it.next() orelse return;
            const col_name_raw = if (std.ascii.eqlIgnoreCase(maybe_col, "COLUMN"))
                it.next() orelse return
            else
                maybe_col;
            const col_name = std.mem.trim(u8, col_name_raw, "`\"");
            const type_tok = it.next() orelse "String";
            const ch_type = std.mem.trim(u8, type_tok, " \t;");

            const col_ty = ddl_parser.parseColumnTypePublic(ch_type) orelse .text;

            const old_cols = existing.table.columns;
            const new_cols = try self.allocator.alloc(schema.Column, old_cols.len + 1);
            defer self.allocator.free(new_cols);
            @memcpy(new_cols[0..old_cols.len], old_cols);
            new_cols[old_cols.len] = .{ .name = col_name, .ty = col_ty, .ch_type = ch_type };

            var updated = existing.*;
            updated.table.columns = new_cols;
            try self.schemas.addEntry(self.allocator, updated);
        } else if (std.ascii.eqlIgnoreCase(action_tok, "DROP")) {
            const maybe_col = it.next() orelse return;
            const col_name_raw = if (std.ascii.eqlIgnoreCase(maybe_col, "COLUMN"))
                it.next() orelse return
            else
                maybe_col;
            const col_name = std.mem.trim(u8, col_name_raw, "`\";");

            const old_cols = existing.table.columns;
            var new_cols = try self.allocator.alloc(schema.Column, old_cols.len);
            defer self.allocator.free(new_cols);
            var n: usize = 0;
            for (old_cols) |col| {
                if (!std.ascii.eqlIgnoreCase(col.name, col_name)) {
                    new_cols[n] = col;
                    n += 1;
                }
            }

            var updated = existing.*;
            updated.table.columns = new_cols[0..n];
            try self.schemas.addEntry(self.allocator, updated);
        }

        if (self.schemas.find(db, table_name)) |stored| {
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.log.warn("http: ALTER TABLE schema_persist.save: {s}", .{@errorName(err)});
            };
        }
    }

    /// Returns empty slice if no part_scan found or scan reads all columns.
    fn findPrunedCols(node: *ir_planner.PhysicalNode) []const []const u8 {
        return ir_planner.findPrunedCols(node);
    }

    /// Try to execute `gplan` via the IR planner + pipeline.
    /// Returns an owned ResultSet on success (caller must call rs.deinit()), or null
    /// if the plan shape is not yet supported by the IR planner.
    fn tryIrExecute(
        self: *Server,
        gplan: generic_sql.Plan,
        table: *const schema.Table,
        part_dirs: []const []const u8,
        orig_sql: []const u8,
    ) !?serializer.ResultSet {
        // ── 1. Build PlannerCtx & translate ──────────────────────────────────
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        var pctx = ir_planner.PlannerCtx.init(alloc, table.*);
        pctx.user_functions = &self.functions;
        const node = ir_planner.plan_query(&pctx, gplan) catch |err| {
            std.log.warn("ir_planner error: {}", .{err});
            arena.deinit();
            return null;
        };
        if (node == null) {
            std.log.warn("IR unsupported: {s}", .{orig_sql});
            arena.deinit();
            return null;
        }

        // ── 2. Build SourceIface via PartScanBridge ───────────────────────────
        const pruned_cols: []const []const u8 = findPrunedCols(node.?);
        var bridge = part_scan_bridge.PartScanBridge.init(
            self.allocator,
            self.io,
            table.*,
            part_dirs,
            pruned_cols,
        ) catch |err| {
            std.log.warn("part_scan_bridge init error: {}", .{err});
            arena.deinit();
            return null;
        };
        defer bridge.deinit();

        // ── 3. Execute plan through pipeline ─────────────────────────────────
        var qctx = core.exec.pipeline.QueryContext.init(self.allocator, bridge.source());
        qctx.setProfileLabel(orig_sql);
        defer qctx.deinit();

        const rs = core.exec.pipeline.executePlan(node.?, &qctx) catch |err| {
            std.log.warn("pipeline executePlan error: {}", .{err});
            arena.deinit();
            return null;
        };
        arena.deinit(); // free planner IR allocations
        return rs; // caller owns ResultSet; must call rs.deinit()
    }

    /// Serialize a ResultSet to HTTP response (JSON, TSV, or Native Block).
    fn serializeResultSet(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        rs: *serializer.ResultSet,
        want_json: bool,
        want_tsv: bool,
        skip_header: bool,
    ) !void {
        if (want_json) {
            const csv_out = try serializer.toCsv(self.allocator, rs.*);
            defer self.allocator.free(csv_out);
            const json_out = try csvToJson(self.allocator, csv_out, null);
            defer self.allocator.free(json_out);
            try sendResponse(request, out, .ok, json_out);
        } else if (want_tsv) {
            const csv_out = try serializer.toCsv(self.allocator, rs.*);
            defer self.allocator.free(csv_out);
            const tsv = try csvToTsv(self.allocator, csv_out, skip_header);
            defer self.allocator.free(tsv);
            try sendResponse(request, out, .ok, tsv);
        } else {
            const nb = try serializer.toNativeBlock(self.allocator, rs.*);
            defer self.allocator.free(nb);
            try sendNativeBlock(self.allocator, request, out, nb);
        }
    }

    fn serializeCsvResponse(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        csv: []const u8,
        want_json: bool,
        want_tsv: bool,
        skip_header: bool,
    ) !void {
        if (want_json) {
            const json_out = try csvToJson(self.allocator, csv, null);
            defer self.allocator.free(json_out);
            try sendResponse(request, out, .ok, json_out);
        } else if (want_tsv) {
            const tsv = try csvToTsv(self.allocator, csv, skip_header);
            defer self.allocator.free(tsv);
            try sendResponse(request, out, .ok, tsv);
        } else {
            try self.sendEmptyNativeBlock(request, out);
        }
    }

    /// DESCRIBE TABLE handler for body-SQL mode.
    /// clickhouse-go sends "DESCRIBE TABLE db.table" to discover column types before INSERT batch.
    fn handleDescribeSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Parse: DESCRIBE TABLE [db.]table
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it.next(); // DESCRIBE
        _ = it.next(); // TABLE
        const tbl_name = it.next() orelse {
            try self.sendEmptyNativeBlock(request, out);
            return;
        };
        const db_table = splitDbTable(tbl_name);
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            try self.sendEmptyNativeBlock(request, out);
            return;
        };

        // Build describe rows from schema
        var rows = try self.allocator.alloc(native_block.DescribeRow, entry.table.columns.len);
        defer self.allocator.free(rows);
        for (entry.table.columns, 0..) |col, i| {
            rows[i] = .{
                .name = col.name,
                .type_name = col.ch_type orelse schemaTypeToChType(col.ty),
            };
        }

        const bytes = try native_block.encodeDescribeTable(self.allocator, rows);
        defer self.allocator.free(bytes);
        try sendNativeBlock(self.allocator, request, out, bytes);
    }

    /// SELECT handler for body-SQL mode.
    /// Handles clickhouse-go handshake and generic SELECTs.
    fn handleSelectSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8, want_tsv: bool) !void {
        // clickhouse-go handshake: SELECT displayName(), version(), revision(), timezone()
        // Must return a Native Block (clickhouse-go uses default_format=Native).
        if (std.mem.indexOf(u8, sql, "displayName()") != null or
            std.mem.indexOf(u8, sql, "version()") != null)
        {
            const cols = [_]native_block.Col{
                .{ .name = "displayName()", .kind = .string, .str_val = "ZigHouse" },
                .{ .name = "version()", .kind = .string, .str_val = "24.8.0" },
                .{ .name = "revision()", .kind = .uint32, .u32_val = 54460 },
                .{ .name = "timezone()", .kind = .string, .str_val = "UTC" },
            };
            const bytes = try native_block.encodeOneRow(self.allocator, &cols);
            defer self.allocator.free(bytes);
            try sendNativeBlock(self.allocator, request, out, bytes);
            return;
        }
        // SELECT 1 → return 1 (TSV) or native block.
        if (std.mem.eql(u8, sql, "SELECT 1") or std.mem.eql(u8, sql, "select 1")) {
            if (want_tsv) {
                try sendResponse(request, out, .ok, "1\n");
                return;
            }
            const cols = [_]native_block.Col{
                .{ .name = "1", .kind = .int64, .i64_val = 1 },
            };
            const bytes = try native_block.encodeOneRow(self.allocator, &cols);
            defer self.allocator.free(bytes);
            try sendNativeBlock(self.allocator, request, out, bytes);
            return;
        }
        // Generic SELECT: route through normal path (body already consumed).
        try self.handleSelectNoDrainEx(request, out, sql, want_tsv, true);
    }

    fn handleSelectNoDrainEx(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8, want_tsv: bool, skip_header: bool) !void {
        // Detect and strip FORMAT JSON — switch to JSON output mode.
        const want_json = std.ascii.indexOfIgnoreCase(sql, "FORMAT JSON") != null or
            std.ascii.indexOfIgnoreCase(sql, "FORMAT JSONCompact") != null or
            std.ascii.indexOfIgnoreCase(sql, "FORMAT JSONEachRow") != null;
        const sql_clean_input: []const u8 = if (want_json) stripFormatClause(sql) else sql;

        // Fast path: SELECT 1
        if (std.mem.eql(u8, sql_clean_input, "SELECT 1") or std.mem.eql(u8, sql_clean_input, "select 1")) {
            if (want_json) {
                try sendResponse(request, out, .ok, "{\"meta\":[{\"name\":\"1\",\"type\":\"UInt8\"}],\"data\":[{\"1\":1}],\"rows\":1,\"statistics\":{\"elapsed\":0.001,\"rows_read\":1,\"bytes_read\":0}}");
            } else if (want_tsv) {
                try sendResponse(request, out, .ok, if (!skip_header) "1\n1\n" else "1\n");
            } else {
                const cols = [_]native_block.Col{
                    .{ .name = "1", .kind = .int64, .i64_val = 1 },
                };
                const bytes = try native_block.encodeOneRow(self.allocator, &cols);
                defer self.allocator.free(bytes);
                try sendNativeBlock(self.allocator, request, out, bytes);
            }
            return;
        }

        // Fast path: system.disks — return one stub row (large free space, no cleanup triggered)
        if (std.ascii.indexOfIgnoreCase(sql_clean_input, "system.disks") != null) {
            const data_dir = self.config.data_dir;
            if (want_json) {
                // ClickHouse FORMAT JSON with fields tequila expects
                const json = try std.fmt.allocPrint(self.allocator,
                    \\{{"meta":[{{"name":"name","type":"String"}},{{"name":"path","type":"String"}},{{"name":"free_space","type":"UInt64"}},{{"name":"total_space","type":"UInt64"}},{{"name":"keep_free_space","type":"UInt64"}},{{"name":"type","type":"String"}}],"data":[{{"name":"default","path":"{s}","free_space":1000000000000,"total_space":2000000000000,"keep_free_space":0,"type":"local"}}],"rows":1,"statistics":{{"elapsed":0.001,"rows_read":1,"bytes_read":0}}}}
                , .{data_dir});
                defer self.allocator.free(json);
                try sendResponse(request, out, .ok, json);
            } else {
                // Always return the full row so any column projection in the SQL still gets valid data.
                const body = try std.fmt.allocPrint(self.allocator, "name\tpath\tfree_space\ttotal_space\tkeep_free_space\ttype\ndefault\t{s}\t1000000000000\t2000000000000\t0\tlocal\n", .{data_dir});
                defer self.allocator.free(body);
                const body_no_hdr = try std.fmt.allocPrint(self.allocator, "default\t{s}\t1000000000000\t2000000000000\t0\tlocal\n", .{data_dir});
                defer self.allocator.free(body_no_hdr);
                try sendResponse(request, out, .ok, if (skip_header) body_no_hdr else body);
            }
            return;
        }

        // Fast path: system.parts — return empty result set (no partitions to drop)
        if (std.ascii.indexOfIgnoreCase(sql_clean_input, "system.parts") != null) {
            if (want_json) {
                try sendResponse(request, out, .ok,
                    \\{"meta":[{"name":"table","type":"String"},{"name":"partition","type":"String"},{"name":"data_compressed_bytes","type":"UInt64"},{"name":"disk_name","type":"String"}],"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}
                );
            } else if (want_tsv) {
                try sendResponse(request, out, .ok, if (skip_header) "" else "table\tpartition\tdata_compressed_bytes\tdisk_name\n");
            } else {
                try self.sendEmptyNativeBlock(request, out);
            }
            return;
        }

        // Fast path: system.tables — return empty (no tables visible externally)
        if (std.ascii.indexOfIgnoreCase(sql_clean_input, "system.tables") != null) {
            if (want_json) {
                try sendResponse(request, out, .ok,
                    \\{"meta":[{"name":"engine_full","type":"String"}],"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}
                );
            } else if (want_tsv) {
                try sendResponse(request, out, .ok, "");
            } else {
                try self.sendEmptyNativeBlock(request, out);
            }
            return;
        }

        // Fast path: system.columns — return empty (GORM AutoMigrate compatibility)
        if (std.ascii.indexOfIgnoreCase(sql_clean_input, "system.columns") != null) {
            if (want_json) {
                try sendResponse(request, out, .ok,
                    \\{"meta":[{"name":"name","type":"String"},{"name":"type","type":"String"}],"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}
                );
            } else if (want_tsv) {
                try sendResponse(request, out, .ok, "");
            } else {
                try self.sendEmptyNativeBlock(request, out);
            }
            return;
        }

        // Fast path: system.one — return single stub row
        if (std.ascii.indexOfIgnoreCase(sql_clean_input, "system.one") != null) {
            if (want_json) {
                try sendResponse(request, out, .ok,
                    \\{"meta":[{"name":"1","type":"UInt8"}],"data":[{"1":1}],"rows":1,"statistics":{"elapsed":0.001,"rows_read":1,"bytes_read":0}}
                );
            } else if (want_tsv) {
                try sendResponse(request, out, .ok, if (skip_header) "1\n" else "1\n1\n");
            } else {
                const cols = [_]native_block.Col{
                    .{ .name = "1", .kind = .uint32, .u32_val = 1 },
                };
                const bytes = try native_block.encodeOneRow(self.allocator, &cols);
                defer self.allocator.free(bytes);
                try sendNativeBlock(self.allocator, request, out, bytes);
            }
            return;
        }

        // Strip FINAL modifier. removeFinal returns the original slice when no FINAL
        // is present (no allocation), and a heap-allocated copy when FINAL was stripped.
        const sql_after_final = try removeFinal(self.allocator, sql_clean_input);
        const final_was_stripped = sql_after_final.ptr != sql_clean_input.ptr;
        errdefer if (final_was_stripped) self.allocator.free(sql_after_final);
        const sql_clean: []const u8 = blk_sc: {
            if (final_was_stripped and std.ascii.indexOfIgnoreCase(sql_after_final, "ORDER BY") == null) {
                const s = try std.fmt.allocPrint(self.allocator, "{s} ORDER BY version DESC", .{sql_after_final});
                self.allocator.free(sql_after_final);
                break :blk_sc s;
            }
            break :blk_sc sql_after_final;
        };
        defer if (final_was_stripped) self.allocator.free(sql_clean);

        // Parse SQL into a Plan.
        const plan = (try generic_sql.parse(self.allocator, sql_clean)) orelse {
            try sendResponse(request, out, .bad_request, "Cannot parse SELECT query\n");
            return;
        };
        defer generic_sql.deinit(self.allocator, plan);

        // View resolution: if the FROM table is a registered view, rewrite SQL to
        // use a subquery from the view's underlying SELECT.
        {
            const tbl_full = plan.table;
            const tbl_short = if (std.mem.indexOfScalar(u8, tbl_full, '.')) |dot| tbl_full[dot + 1 ..] else tbl_full;
            const view_sql_opt = self.views.get(tbl_full) orelse self.views.get(tbl_short);
            if (view_sql_opt) |view_sql| {
                // Rewrite: replace "FROM <view_name>" with "FROM (<view_sql>)" subquery.
                // Find and replace the table name in sql_clean.
                const from_full = try std.fmt.allocPrint(self.allocator, "FROM {s}", .{tbl_full});
                defer self.allocator.free(from_full);
                const from_short = try std.fmt.allocPrint(self.allocator, "FROM {s}", .{tbl_short});
                defer self.allocator.free(from_short);
                const from_sub = try std.fmt.allocPrint(self.allocator, "FROM ({s})", .{view_sql});
                defer self.allocator.free(from_sub);
                const needle = if (std.ascii.indexOfIgnoreCase(sql_clean, from_full) != null) from_full else from_short;
                if (std.ascii.indexOfIgnoreCase(sql_clean, needle)) |pos| {
                    const rewritten = try std.fmt.allocPrint(self.allocator, "{s}{s}{s}", .{
                        sql_clean[0..pos], from_sub, sql_clean[pos + needle.len ..],
                    });
                    defer self.allocator.free(rewritten);
                    return self.handleSelectNoDrainEx(request, out, rewritten, want_tsv, skip_header);
                }
            }
        }

        // Subquery / CTE: resolve table from inner plan when subquery_source is set.
        const db_table = if (plan.subquery_source) |sq|
            splitDbTable(sq.table)
        else
            splitDbTable(plan.table);

        // UNION ALL: execute each plan separately, concatenate CSV rows, return.
        if (plan.union_other) |uo| {
            const left_entry = self.schemas.find(db_table.db, db_table.table) orelse {
                if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
                return;
            };
            var left_parts = try part_scanner.scan(self.allocator, self.io, self.config.data_dir, db_table.db, db_table.table);
            defer left_parts.deinit();
            var left_rs = try self.tryIrExecute(plan, &left_entry.table, left_parts.dirs(), sql_clean);
            defer if (left_rs) |*l| l.deinit();
            const right_db_table = splitDbTable(uo.table);
            const right_entry = self.schemas.find(right_db_table.db, right_db_table.table) orelse {
                if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
                return;
            };
            var right_parts = try part_scanner.scan(self.allocator, self.io, self.config.data_dir, right_db_table.db, right_db_table.table);
            defer right_parts.deinit();
            var right_rs = try self.tryIrExecute(uo.*, &right_entry.table, right_parts.dirs(), sql_clean);
            defer if (right_rs) |*r| r.deinit();
            if (left_rs) |*l| {
                if (right_rs) |*r| {
                    const csv_left = try serializer.toCsv(self.allocator, l.*);
                    defer self.allocator.free(csv_left);
                    const csv_right = try serializer.toCsv(self.allocator, r.*);
                    defer self.allocator.free(csv_right);
                    // Merge: left CSV (with header) + right CSV rows (skip header)
                    const nl = std.mem.indexOfScalar(u8, csv_right, '\n');
                    const csv_merged = if (nl) |n|
                        try std.mem.concat(self.allocator, u8, &.{ csv_left, csv_right[n + 1 ..] })
                    else
                        try self.allocator.dupe(u8, csv_left);
                    defer self.allocator.free(csv_merged);
                    try self.serializeCsvResponse(request, out, csv_merged, want_json, want_tsv, skip_header);
                } else {
                    try self.serializeResultSet(request, out, l, want_json, want_tsv, skip_header);
                }
            } else if (right_rs) |*r| {
                try self.serializeResultSet(request, out, r, want_json, want_tsv, skip_header);
            } else {
                if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
            }
            return;
        }

        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            std.log.warn("HTTP SELECT unknown table: {s}.{s} sql={s}", .{ db_table.db, db_table.table, sql_clean });
            core.exec.pipeline.emitUnsupportedProfile(sql_clean, "unknown_table");
            if (strictUnsupportedSelect()) {
                try sendResponse(request, out, .not_implemented, "unknown table\n");
                return;
            }
            if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
            return;
        };

        var parts = try part_scanner.scan(
            self.allocator,
            self.io,
            self.config.data_dir,
            db_table.db,
            db_table.table,
        );
        defer parts.deinit();

        if (parts.dirs().len == 0) {
            if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
            return;
        }

        // ── IR execution path ─────────────────────────────────────────────────
        if (try self.tryIrExecute(plan, &entry.table, parts.dirs(), sql_clean)) |rs| {
            var owned_rs = rs;
            defer owned_rs.deinit();

            // FINAL deduplication: for ReplacingMergeTree tables, keep only the
            // first row per primary key (highest version, since ORDER BY version DESC
            // was injected by the FINAL handler).
            if (final_was_stripped and entry.pk != null) {
                if (try dedupResultSetByPk(self.allocator, &owned_rs, entry.pk.?, entry.table)) |deduped| {
                    owned_rs.deinit();
                    owned_rs = deduped;
                }
            }
            try self.serializeResultSet(request, out, &owned_rs, want_json, want_tsv, skip_header);
            return;
        }

        // IR path returned null — unsupported query shape, return empty.
        core.exec.pipeline.emitUnsupportedProfile(sql_clean, "unsupported_select");
        if (strictUnsupportedSelect()) {
            try sendResponse(request, out, .not_implemented, "unsupported SELECT\n");
            return;
        }
        if (want_tsv) try sendResponse(request, out, .ok, "") else try self.sendEmptyNativeBlock(request, out);
    }

    fn handleCreateSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        return self.handleCreateCore(request, out, sql, true);
    }

    /// INSERT handler for body-SQL mode.
    /// clickhouse-go sends: "INSERT INTO db.table FORMAT RowBinaryWithNamesAndTypes\n<binary data>"
    fn handleInsertBodyData(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8, data: []const u8) !void {
        const insert_info = parseInsertTarget(sql) orelse {
            try sendResponse(request, out, .bad_request, "Expected: INSERT INTO <db>.<table> FORMAT RowBinary[WithNamesAndTypes]\n");
            return;
        };

        // VALUES INSERT is entirely in the SQL string — data part is empty; handle first.
        if (insert_info.values_fmt) {
            try self.handleInsertValues(request, out, sql);
            return;
        }

        if (data.len == 0) {
            try self.sendEmptyNativeBlock(request, out);
            return;
        }

        if (insert_info.native_fmt) {
            try self.handleInsertNativeData(request, out, insert_info.db_table, data);
        } else if (insert_info.with_names_and_types) {
            try self.handleInsertWithHeaderData(request, out, insert_info.db_table, data);
        } else {
            try self.handleInsertRowBinaryData(request, out, insert_info.db_table, data);
        }
    }

    /// SQL VALUES INSERT: INSERT INTO db.table (cols) VALUES (v1, v2, ...) [, (...), ...]
    /// Parses the VALUES clause and writes each row as a part.
    fn handleInsertValues(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        sql: []const u8,
    ) !void {
        // Parse: INSERT INTO [db.]table [(col1,...)] VALUES (v1,...) [,(v2,...)]
        var it = SqlValuesParser{ .src = sql, .pos = 0 };
        it.skipWs();
        it.consumeKeyword("INSERT") catch {
            try sendResponse(request, out, .bad_request, "VALUES INSERT parse error\n");
            return;
        };
        it.skipWs();
        it.consumeKeyword("INTO") catch {
            try sendResponse(request, out, .bad_request, "VALUES INSERT parse error\n");
            return;
        };
        it.skipWs();
        const table_tok = it.nextToken() orelse {
            try sendResponse(request, out, .bad_request, "VALUES INSERT: missing table\n");
            return;
        };
        var db_name: []const u8 = "default";
        var table_name: []const u8 = table_tok;
        if (std.mem.indexOfScalar(u8, table_tok, '.')) |dot| {
            db_name = table_tok[0..dot];
            table_name = table_tok[dot + 1 ..];
        }

        // Optional column list
        it.skipWs();
        var col_indices: ?[]usize = null;
        defer if (col_indices) |ci| self.allocator.free(ci);

        if (it.peekChar() == '(') {
            it.pos += 1;
            var col_name_list: std.ArrayListUnmanaged([]const u8) = .empty;
            defer col_name_list.deinit(self.allocator);
            while (true) {
                it.skipWs();
                if (it.peekChar() == ')') {
                    it.pos += 1;
                    break;
                }
                if (it.peekChar() == ',') {
                    it.pos += 1;
                    continue;
                }
                const cn = it.nextToken() orelse break;
                try col_name_list.append(self.allocator, cn);
            }
            // Resolve column names to indices in entry schema
            const entry = self.schemas.find(db_name, table_name) orelse {
                try sendResponse(request, out, .bad_request, "VALUES INSERT: unknown table\n");
                return;
            };
            var indices = try self.allocator.alloc(usize, col_name_list.items.len);
            for (col_name_list.items, 0..) |cn, i| {
                var found = false;
                for (entry.table.columns, 0..) |col, j| {
                    if (std.mem.eql(u8, col.name, cn)) {
                        indices[i] = j;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    self.allocator.free(indices);
                    try sendResponse(request, out, .bad_request, "VALUES INSERT: unknown column\n");
                    return;
                }
            }
            col_indices = indices;
        }

        it.skipWs();
        it.consumeKeyword("VALUES") catch {
            try sendResponse(request, out, .bad_request, "VALUES INSERT: missing VALUES\n");
            return;
        };

        const entry = self.schemas.find(db_name, table_name) orelse {
            try sendResponse(request, out, .bad_request, "VALUES INSERT: unknown table\n");
            return;
        };
        const n_cols = if (col_indices) |ci| ci.len else entry.table.columns.len;

        // Allocate one ColumnBuffer per schema column; accumulate ALL rows before writePart.
        const col_bufs = try self.allocator.alloc(row_binary_decoder.ColumnBuffer, entry.table.columns.len);
        for (col_bufs, entry.table.columns) |*buf, col| {
            buf.* = .{ .col = col, .fixed_vals = .empty, .str_vals = .empty, .str_bytes = .empty, .null_flags = .empty };
        }
        defer {
            for (col_bufs) |*buf| buf.deinit(self.allocator);
            self.allocator.free(col_bufs);
        }

        var row_count: usize = 0;

        // Parse all (v1, v2, ...) tuples into col_bufs.
        while (true) {
            it.skipWs();
            if (it.pos >= it.src.len) break;
            if (it.peekChar() == ',') {
                it.pos += 1;
                continue;
            }
            if (it.peekChar() == ';') break;
            if (it.peekChar() != '(') break;
            it.pos += 1; // consume '('

            // Track which schema columns were set in this row.
            const set_flags = try self.allocator.alloc(bool, entry.table.columns.len);
            defer self.allocator.free(set_flags);
            @memset(set_flags, false);

            var val_count: usize = 0;
            while (val_count < n_cols) {
                it.skipWs();
                if (it.peekChar() == ')') break;
                if (it.peekChar() == ',') {
                    it.pos += 1;
                    continue;
                }
                const val_str = try it.parseValue(self.allocator);
                defer self.allocator.free(val_str);

                const col_idx = if (col_indices) |ci| ci[val_count] else val_count;
                if (col_idx < col_bufs.len) {
                    try appendParsedField(self.allocator, entry.table.columns[col_idx], val_str, &col_bufs[col_idx]);
                    set_flags[col_idx] = true;
                }
                val_count += 1;
            }
            it.skipWs();
            if (it.peekChar() == ')') it.pos += 1;

            // Fill any unset columns with zero/empty for this row.
            for (col_bufs, set_flags) |*buf, was_set| {
                if (was_set) continue;
                switch (buf.col.ty) {
                    .text, .char, .low_card => try buf.str_vals.append(self.allocator, buf.str_bytes.items[0..0]),
                    else => try buf.fixed_vals.append(self.allocator, 0),
                }
            }
            row_count += 1;
        }

        if (row_count > 0) {
            const db_table = DbTable{ .db = db_name, .table = table_name };
            try self.writePart(db_table, entry, col_bufs);
        }

        // Return empty text response (not native block) for HTTP VALUES INSERT.
        // This keeps the response non-binary so test harnesses work correctly.
        try sendResponse(request, out, .ok, "");
    }

    /// INSERT INTO table SELECT ... — execute SELECT, write rows to target table.
    fn handleInsertSelect(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        sql: []const u8,
    ) !void {
        // INSERT SELECT is not supported in the IR-only execution path.
        _ = self;
        _ = sql;
        try sendResponse(request, out, .bad_request, "INSERT SELECT not supported\n");
    }

    fn handleInsertNativeData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        data: []const u8,
    ) !void {
        var decoded = row_binary_decoder.decodeNativeBlock(self.allocator, data) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "Native Block decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (decoded.table.columns.len == 0) {
            try self.sendEmptyNativeBlock(request, out);
            return;
        }

        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            try self.writePart(db_table, existing, decoded.decoder.columns);
        } else {
            const new_entry = schema_config.TableEntry{
                .db = db_table.db,
                .table = .{ .name = db_table.table, .columns = decoded.table.columns },
                .name = db_table.table,
                .pk = null,
            };
            try self.schemas.addEntry(self.allocator, new_entry);
            const stored = self.schemas.find(db_table.db, db_table.table).?;
            self.tryPersistSchema(stored.db, stored);
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        try self.sendEmptyNativeBlock(request, out);
    }

    fn handleInsertWithHeaderData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        var decoded = row_binary_decoder.decodeWithHeader(self.allocator, body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "RowBinaryWithNamesAndTypes decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            if (!schemasCompatible(existing.table, decoded.table)) {
                try sendResponse(request, out, .bad_request, "Schema mismatch: incoming columns don't match registered schema\n");
                return;
            }
            try self.writePart(db_table, existing, decoded.decoder.columns);
        } else {
            const new_entry = schema_config.TableEntry{
                .db = db_table.db,
                .table = .{ .name = db_table.table, .columns = decoded.table.columns },
                .name = db_table.table,
                .pk = null,
            };
            try self.schemas.addEntry(self.allocator, new_entry);
            const stored = self.schemas.find(db_table.db, db_table.table).?;
            self.tryPersistSchema(stored.db, stored);
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        try self.sendEmptyNativeBlock(request, out);
    }

    fn handleInsertRowBinaryData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            const msg = try std.fmt.allocPrint(self.allocator, "Unknown table '{s}.{s}': use CREATE TABLE or RowBinaryWithNamesAndTypes first\n", .{ db_table.db, db_table.table });
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };

        var dec = try row_binary_decoder.RowBinaryDecoder.init(self.allocator, entry.table);
        defer dec.deinit();
        const n_rows = dec.decode(body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "RowBinary decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };

        if (n_rows == 0) {
            try self.sendEmptyNativeBlock(request, out);
            return;
        }

        try self.writePart(db_table, entry, dec.columns);
        try self.sendEmptyNativeBlock(request, out);
    }

    /// Send an empty Native-protocol block and flush.
    fn sendEmptyNativeBlock(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer) !void {
        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }

    /// Persist schema to disk, logging any error as a warning.
    fn tryPersistSchema(self: *Server, db: []const u8, stored: *const schema_config.TableEntry) void {
        schema_persist.save(self.io, self.allocator, self.config.data_dir, db, stored) catch |err| {
            std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
        };
    }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Scan all existing part directories under data_dir to find the highest seq number.
/// Part directories are named "all_{seq}_{seq}_0". Returns the max seq found, or 0
/// if none exist. Used at startup so new parts never overwrite existing ones after restart.
fn scanMaxPartSeq(io: std.Io, data_dir: []const u8) u64 {
    var max_seq: u64 = 0;
    const cwd = std.Io.Dir.cwd();
    var dir = cwd.openDir(io, data_dir, .{ .iterate = true }) catch return 0;
    defer dir.close(io);
    var db_it = dir.iterate();
    while (db_it.next(io) catch null) |db_entry| {
        if (db_entry.kind != .directory) continue;
        var db_dir = dir.openDir(io, db_entry.name, .{ .iterate = true }) catch continue;
        defer db_dir.close(io);
        var tbl_it = db_dir.iterate();
        while (tbl_it.next(io) catch null) |tbl_entry| {
            if (tbl_entry.kind != .directory) continue;
            var tbl_dir = db_dir.openDir(io, tbl_entry.name, .{ .iterate = true }) catch continue;
            defer tbl_dir.close(io);
            var parts_dir = tbl_dir.openDir(io, "parts", .{ .iterate = true }) catch continue;
            defer parts_dir.close(io);
            var part_it = parts_dir.iterate();
            while (part_it.next(io) catch null) |part_entry| {
                if (part_entry.kind != .directory) continue;
                const seq = parsePartSeq(part_entry.name) orelse continue;
                if (seq > max_seq) max_seq = seq;
            }
        }
    }
    return max_seq;
}

/// Parse a text field value and append to a ColumnBuffer according to the column type.
/// Used by CSV, JSONEachRow, and VALUES handlers.
/// Parse an array literal string like "[1,2,3]" or "['a','b']" into element strings.
/// Returns owned slice of owned element strings (caller frees all).
fn parseArrayLiteralElements(allocator: std.mem.Allocator, lit: []const u8) ![][]const u8 {
    const s = std.mem.trim(u8, lit, " \t");
    var elems: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (elems.items) |e| allocator.free(e);
        elems.deinit(allocator);
    }
    if (s.len < 2 or s[0] != '[') return elems.toOwnedSlice(allocator);
    var p: usize = 1; // skip '['
    while (p < s.len and s[p] != ']') {
        // skip whitespace and commas
        while (p < s.len and (s[p] == ' ' or s[p] == '\t' or s[p] == ',')) p += 1;
        if (p >= s.len or s[p] == ']') break;
        if (s[p] == '\'') {
            // quoted string element
            p += 1;
            var ebuf: std.ArrayListUnmanaged(u8) = .empty;
            while (p < s.len) {
                if (s[p] == '\'') {
                    p += 1;
                    if (p < s.len and s[p] == '\'') {
                        try ebuf.append(allocator, '\'');
                        p += 1;
                    } else break;
                } else {
                    try ebuf.append(allocator, s[p]);
                    p += 1;
                }
            }
            try elems.append(allocator, try ebuf.toOwnedSlice(allocator));
        } else if (s[p] == '[') {
            // nested array: capture balanced brackets
            const start = p;
            var depth: usize = 0;
            while (p < s.len) {
                if (s[p] == '[') depth += 1 else if (s[p] == ']') {
                    depth -= 1;
                    if (depth == 0) {
                        p += 1;
                        break;
                    }
                }
                p += 1;
            }
            try elems.append(allocator, try allocator.dupe(u8, s[start..p]));
        } else {
            // number or bare token
            const start = p;
            while (p < s.len and s[p] != ',' and s[p] != ']' and s[p] != ' ' and s[p] != '\t') p += 1;
            try elems.append(allocator, try allocator.dupe(u8, s[start..p]));
        }
    }
    return elems.toOwnedSlice(allocator);
}

fn appendParsedField(
    allocator: std.mem.Allocator,
    col: schema.Column,
    field: []const u8,
    buf: *row_binary_decoder.ColumnBuffer,
) !void {
    // Array(*) columns: parse [elem,...] literal into binary blob
    if (col.ch_type) |ct| {
        if (std.mem.startsWith(u8, ct, "Array(")) {
            const elem_type = ct[6 .. ct.len - 1]; // strip "Array(" and ")"
            const elems = try parseArrayLiteralElements(allocator, field);
            defer {
                for (elems) |e| allocator.free(e);
                allocator.free(elems);
            }
            // Encode elements into binary blob
            var blob: std.ArrayListUnmanaged(u8) = .empty;
            defer blob.deinit(allocator);
            const fix_w = row_binary_decoder.chTypeFixedWidth(elem_type);
            for (elems) |e| {
                if (fix_w) |w| {
                    const v = std.fmt.parseInt(i64, std.mem.trim(u8, e, " \t"), 10) catch 0;
                    var tmp = [_]u8{0} ** 8;
                    std.mem.writeInt(i64, &tmp, v, .little);
                    try blob.appendSlice(allocator, tmp[0..w]);
                } else {
                    // variable-length string: varint(len) + content
                    const slen = e.len;
                    var vbuf: [10]u8 = undefined;
                    var vi: usize = 0;
                    var rem = slen;
                    while (true) {
                        vbuf[vi] = @intCast(rem & 0x7F);
                        rem >>= 7;
                        if (rem == 0) {
                            vi += 1;
                            break;
                        }
                        vbuf[vi] |= 0x80;
                        vi += 1;
                    }
                    try blob.appendSlice(allocator, vbuf[0..vi]);
                    try blob.appendSlice(allocator, e);
                }
            }
            const start = buf.str_bytes.items.len;
            try buf.str_bytes.appendSlice(allocator, blob.items);
            try buf.str_vals.append(allocator, buf.str_bytes.items[start..]);
            return;
        }
    }
    switch (col.ty) {
        .text, .char, .low_card => {
            const start = buf.str_bytes.items.len;
            try buf.str_bytes.appendSlice(allocator, field);
            const end = buf.str_bytes.items.len;
            try buf.str_vals.append(allocator, buf.str_bytes.items[start..end]);
        },
        .float32, .float64 => {
            const v = std.fmt.parseFloat(f64, field) catch 0.0;
            const bits: u64 = @bitCast(v);
            try buf.fixed_vals.append(allocator, @bitCast(bits));
        },
        .date => {
            // Accept 'YYYY-MM-DD' or integer days.
            const v: i64 = parseDateLiteral(field) orelse
                std.fmt.parseInt(i64, field, 10) catch 0;
            try buf.fixed_vals.append(allocator, v);
        },
        .timestamp => {
            // Accept 'YYYY-MM-DD HH:MM:SS' or integer unix seconds.
            const v: i64 = parseDateTimeLiteral(field) orelse
                std.fmt.parseInt(i64, field, 10) catch 0;
            try buf.fixed_vals.append(allocator, v);
        },
        else => {
            const v = std.fmt.parseInt(i64, field, 10) catch 0;
            try buf.fixed_vals.append(allocator, v);
        },
    }
}

/// Parse 'YYYY-MM-DD' → days since 1970-01-01 (UInt16 stored as i64).
/// Returns null if the string is not in the expected format.
fn parseDateLiteral(s: []const u8) ?i64 {
    const str = std.mem.trim(u8, s, " \t'\"");
    if (str.len < 10) return null;
    const year = std.fmt.parseInt(u32, str[0..4], 10) catch return null;
    if (str[4] != '-') return null;
    const month = std.fmt.parseInt(u32, str[5..7], 10) catch return null;
    if (str[7] != '-') return null;
    const day = std.fmt.parseInt(u32, str[8..10], 10) catch return null;
    return @intCast(ymdToEpochDays(year, month, day));
}

/// Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' → unix seconds.
fn parseDateTimeLiteral(s: []const u8) ?i64 {
    const str = std.mem.trim(u8, s, " \t'\"");
    if (str.len < 10) return null;
    const days = parseDateLiteral(str) orelse return null;
    var secs: i64 = days * 86400;
    if (str.len >= 19 and (str[10] == ' ' or str[10] == 'T')) {
        const hh = std.fmt.parseInt(i64, str[11..13], 10) catch 0;
        const mm = std.fmt.parseInt(i64, str[14..16], 10) catch 0;
        const ss = std.fmt.parseInt(i64, str[17..19], 10) catch 0;
        secs += hh * 3600 + mm * 60 + ss;
    }
    return secs * 1000; // datetime64_ms expects milliseconds
}

/// Gregorian calendar: year/month/day → days since 1970-01-01.
fn ymdToEpochDays(year: u32, month: u32, day: u32) i32 {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    var y: i32 = @intCast(year);
    const m: i32 = @intCast(month);
    const d: i32 = @intCast(day);
    if (m <= 2) y -= 1;
    const era: i32 = @divFloor(y, 400);
    const yoe: i32 = y - era * 400;
    const doy: i32 = @divFloor(153 * (m + (if (m > 2) @as(i32, -3) else 9)) + 2, 5) + d - 1;
    const doe: i32 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

/// Extract the string value of a named field from a JSON object literal.
/// Returns raw value (unquoted for strings, as-is for numbers/booleans), or null if not found.
/// This is a minimal extractor — handles simple flat JSON objects only.
fn extractJsonField(json: []const u8, key: []const u8) ?[]const u8 {
    // Search for `"key":` pattern
    var pos: usize = 0;
    while (pos < json.len) {
        const quote = std.mem.indexOfScalarPos(u8, json, pos, '"') orelse break;
        const end_key = std.mem.indexOfScalarPos(u8, json, quote + 1, '"') orelse break;
        const found_key = json[quote + 1 .. end_key];
        pos = end_key + 1;
        // Skip whitespace and colon
        while (pos < json.len and (json[pos] == ' ' or json[pos] == '\t')) pos += 1;
        if (pos >= json.len or json[pos] != ':') continue;
        pos += 1;
        while (pos < json.len and (json[pos] == ' ' or json[pos] == '\t')) pos += 1;
        if (pos >= json.len) break;
        if (!std.mem.eql(u8, found_key, key)) {
            // Skip this value to advance pos
            if (json[pos] == '"') {
                pos += 1;
                while (pos < json.len and json[pos] != '"') {
                    if (json[pos] == '\\') pos += 1;
                    pos += 1;
                }
                pos += 1; // closing quote
            } else {
                while (pos < json.len and json[pos] != ',' and json[pos] != '}') pos += 1;
            }
            continue;
        }
        // Found the key — extract value
        if (json[pos] == '"') {
            // String value — return contents without quotes
            const vstart = pos + 1;
            var vend = vstart;
            while (vend < json.len and json[vend] != '"') {
                if (json[vend] == '\\') vend += 1;
                vend += 1;
            }
            return json[vstart..vend];
        } else {
            // Number / bool / null — return raw token
            const vstart = pos;
            var vend = pos;
            while (vend < json.len and json[vend] != ',' and json[vend] != '}' and json[vend] != ' ') vend += 1;
            return json[vstart..vend];
        }
    }
    return null;
}

/// Convert a CSV string (comma-separated, first row = header) to TabSeparated.
/// Handles quoted fields; strips surrounding quotes. Output has no quoting.
/// If skip_header is true, the first (header) line of the CSV is omitted.
fn csvToTsv(allocator: std.mem.Allocator, csv: []const u8, skip_header: bool) ![]u8 {
    // If skip_header, advance past the first CSV line.
    var src = csv;
    if (skip_header) {
        if (std.mem.indexOfScalar(u8, src, '\n')) |nl| {
            src = src[nl + 1 ..];
        } else {
            return allocator.dupe(u8, "");
        }
    }
    var out = std.ArrayListUnmanaged(u8){ .items = &.{}, .capacity = 0 };
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < src.len) {
        // Parse one field.
        if (i < src.len and src[i] == '"') {
            // Quoted field.
            i += 1;
            while (i < src.len) {
                if (src[i] == '"') {
                    i += 1;
                    if (i < src.len and src[i] == '"') {
                        try out.append(allocator, '"');
                        i += 1;
                    } else break;
                } else {
                    try out.append(allocator, src[i]);
                    i += 1;
                }
            }
        } else if (i < src.len and src[i] == 0x01) {
            // Array sentinel field: \x01 + elements joined by \x0c → [elem1,elem2,...]
            i += 1; // skip sentinel
            const field_start = i;
            while (i < src.len and src[i] != ',' and src[i] != '\n' and src[i] != '\r') i += 1;
            const field = src[field_start..i];
            try out.append(allocator, '[');
            var first_elem = true;
            var elem_it = std.mem.splitScalar(u8, field, '\x0c');
            while (elem_it.next()) |elem| {
                if (!first_elem) try out.append(allocator, ',');
                first_elem = false;
                // Numeric elements: emit as-is; string elements: single-quoted
                const is_numeric = for (elem) |c| {
                    if (c != '-' and (c < '0' or c > '9') and c != '.') break false;
                } else true;
                if (is_numeric or elem.len == 0) {
                    try out.appendSlice(allocator, elem);
                } else {
                    try out.append(allocator, '\'');
                    for (elem) |c| {
                        if (c == '\'') try out.append(allocator, '\\');
                        try out.append(allocator, c);
                    }
                    try out.append(allocator, '\'');
                }
            }
            try out.append(allocator, ']');
        } else {
            // Unquoted field: read until comma or newline.
            while (i < src.len and src[i] != ',' and src[i] != '\n' and src[i] != '\r') {
                try out.append(allocator, src[i]);
                i += 1;
            }
        }
        // After field: decide separator.
        if (i >= src.len) {
            // EOF — done.
        } else if (src[i] == ',') {
            try out.append(allocator, '\t');
            i += 1;
        } else if (src[i] == '\r') {
            i += 1;
            if (i < src.len and src[i] == '\n') i += 1;
            try out.append(allocator, '\n');
        } else if (src[i] == '\n') {
            i += 1;
            try out.append(allocator, '\n');
        }
    }
    return out.toOwnedSlice(allocator);
}

/// Convert a CSV byte slice (produced by generic_executor) to ClickHouse FORMAT JSON.
/// Output: {"meta":[{"name":"col","type":"String"}],"data":[{"col":"val",...}],"rows":N,"statistics":{"elapsed":0.001,"rows_read":N,"bytes_read":0}}
/// col_types: parallel slice of ch_type strings for each column (or null to default to "String").
fn csvToJson(allocator: std.mem.Allocator, csv: []const u8, col_types: ?[]const ?[]const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8){ .items = &.{}, .capacity = 0 };
    errdefer out.deinit(allocator);

    // Parse header line to get column names.
    const hdr_end = std.mem.indexOfScalar(u8, csv, '\n') orelse csv.len;
    const hdr_line = csv[0..hdr_end];
    var col_names = std.ArrayListUnmanaged([]const u8){ .items = &.{}, .capacity = 0 };
    defer col_names.deinit(allocator);
    {
        var pos: usize = 0;
        while (pos <= hdr_line.len) {
            const end = std.mem.indexOfScalarPos(u8, hdr_line, pos, ',') orelse hdr_line.len;
            var name = std.mem.trim(u8, hdr_line[pos..end], " \t\r");
            // Strip type sentinels (\x03U8:, \x02D:, \x01A:, \x05F:)
            if (name.len > 4 and name[0] == 0x03 and name[1] == 'U' and name[2] == '8' and name[3] == ':')
                name = name[4..]
            else if (name.len > 3 and name[0] == 0x02 and name[1] == 'D' and name[2] == ':')
                name = name[3..]
            else if (name.len > 3 and name[0] == 0x01 and name[1] == 'A' and name[2] == ':')
                name = name[3..]
            else if (name.len > 3 and name[0] == 0x05 and name[1] == 'F' and name[2] == ':')
                name = name[3..];
            try col_names.append(allocator, name);
            if (end >= hdr_line.len) break;
            pos = end + 1;
        }
    }
    const ncols = col_names.items.len;

    // meta
    try out.appendSlice(allocator, "{\"meta\":[");
    for (col_names.items, 0..) |name, ci| {
        if (ci > 0) try out.append(allocator, ',');
        const ct: []const u8 = if (col_types) |cts| (if (ci < cts.len) (cts[ci] orelse "String") else "String") else "String";
        try out.appendSlice(allocator, "{\"name\":\"");
        try jsonEscapeAppend(allocator, &out, name);
        try out.appendSlice(allocator, "\",\"type\":\"");
        try jsonEscapeAppend(allocator, &out, ct);
        try out.appendSlice(allocator, "\"}");
    }
    try out.appendSlice(allocator, "],\"data\":[");

    // data rows
    var row_count: u64 = 0;
    var src = if (hdr_end < csv.len) csv[hdr_end + 1 ..] else csv[csv.len..];
    var first_row = true;
    while (src.len > 0) {
        // Find end of row
        const row_end = std.mem.indexOfScalar(u8, src, '\n') orelse src.len;
        const row_line = std.mem.trimEnd(u8, src[0..row_end], "\r");
        src = if (row_end < src.len) src[row_end + 1 ..] else src[src.len..];
        if (row_line.len == 0) continue;

        if (!first_row) try out.append(allocator, ',');
        first_row = false;
        try out.append(allocator, '{');

        // Parse CSV fields
        var ci: usize = 0;
        var pos: usize = 0;
        while (ci < ncols and pos <= row_line.len) {
            if (ci > 0) try out.append(allocator, ',');
            try out.append(allocator, '"');
            try jsonEscapeAppend(allocator, &out, col_names.items[ci]);
            try out.appendSlice(allocator, "\":");

            // Extract field value
            var field: []const u8 = "";
            if (pos < row_line.len and row_line[pos] == '"') {
                // Quoted field — unescape
                pos += 1;
                var fbuf = std.ArrayListUnmanaged(u8){ .items = &.{}, .capacity = 0 };
                defer fbuf.deinit(allocator);
                while (pos < row_line.len) {
                    if (row_line[pos] == '"') {
                        pos += 1;
                        if (pos < row_line.len and row_line[pos] == '"') {
                            try fbuf.append(allocator, '"');
                            pos += 1;
                        } else break;
                    } else {
                        try fbuf.append(allocator, row_line[pos]);
                        pos += 1;
                    }
                }
                try out.append(allocator, '"');
                try jsonEscapeAppend(allocator, &out, fbuf.items);
                try out.append(allocator, '"');
            } else if (pos < row_line.len and row_line[pos] == 0x01) {
                // Array sentinel
                pos += 1;
                const fend = std.mem.indexOfScalarPos(u8, row_line, pos, ',') orelse row_line.len;
                field = row_line[pos..fend];
                pos = fend;
                // Emit as JSON array
                try out.append(allocator, '[');
                var first_e = true;
                var eit = std.mem.splitScalar(u8, field, '\x0c');
                while (eit.next()) |elem| {
                    if (!first_e) try out.append(allocator, ',');
                    first_e = false;
                    const is_num = for (elem) |c| {
                        if (c != '-' and (c < '0' or c > '9') and c != '.') break false;
                    } else true;
                    if (is_num or elem.len == 0) {
                        if (elem.len == 0) try out.appendSlice(allocator, "null") else try out.appendSlice(allocator, elem);
                    } else {
                        try out.append(allocator, '"');
                        try jsonEscapeAppend(allocator, &out, elem);
                        try out.append(allocator, '"');
                    }
                }
                try out.append(allocator, ']');
            } else {
                // Unquoted field
                const fend = std.mem.indexOfScalarPos(u8, row_line, pos, ',') orelse row_line.len;
                field = row_line[pos..fend];
                pos = fend;
                // Determine if numeric
                const is_num = field.len > 0 and for (field) |c| {
                    if (c != '-' and (c < '0' or c > '9') and c != '.' and c != 'e' and c != 'E' and c != '+') break false;
                } else true;
                if (is_num) {
                    try out.appendSlice(allocator, field);
                } else {
                    try out.append(allocator, '"');
                    try jsonEscapeAppend(allocator, &out, field);
                    try out.append(allocator, '"');
                }
            }

            // Skip comma separator
            if (pos < row_line.len and row_line[pos] == ',') pos += 1;
            ci += 1;
        }
        // Fill missing columns with null
        while (ci < ncols) : (ci += 1) {
            try out.appendSlice(allocator, ",\"");
            try jsonEscapeAppend(allocator, &out, col_names.items[ci]);
            try out.appendSlice(allocator, "\":null");
        }
        try out.append(allocator, '}');
        row_count += 1;
    }

    const suffix = try std.fmt.allocPrint(allocator, "],\"rows\":{d},\"statistics\":{{\"elapsed\":0.001,\"rows_read\":{d},\"bytes_read\":0}}}}", .{ row_count, row_count });
    defer allocator.free(suffix);
    try out.appendSlice(allocator, suffix);
    return out.toOwnedSlice(allocator);
}

/// JSON-escape a string slice, appending to an ArrayListUnmanaged(u8).
fn jsonEscapeAppend(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try out.appendSlice(allocator, "\\\""),
            '\\' => try out.appendSlice(allocator, "\\\\"),
            '\n' => try out.appendSlice(allocator, "\\n"),
            '\r' => try out.appendSlice(allocator, "\\r"),
            '\t' => try out.appendSlice(allocator, "\\t"),
            0x00...0x08, 0x0b...0x0c, 0x0e...0x1f => {
                const esc = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{c});
                defer allocator.free(esc);
                try out.appendSlice(allocator, esc);
            },
            else => try out.append(allocator, c),
        }
    }
}

/// Parse the seq number from a part directory name "all_{seq}_{seq}_0".
fn parsePartSeq(name: []const u8) ?u64 {
    // Expected format: "all_N_N_0"
    if (!std.mem.startsWith(u8, name, "all_")) return null;
    const after_all = name[4..];
    const underscore = std.mem.indexOfScalar(u8, after_all, '_') orelse return null;
    return std.fmt.parseInt(u64, after_all[0..underscore], 10) catch null;
}

fn sendResponse(request: *std.http.Server.Request, out: *std.Io.Writer, status: std.http.Status, body: []const u8) !void {
    try request.respond(body, .{
        .status = status,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
            .{ .name = "connection", .value = "close" },
        },
    });
    try out.flush();
}

fn sendNativeBlock(allocator: std.mem.Allocator, request: *std.http.Server.Request, out: *std.Io.Writer, body: []const u8) !void {
    try request.respond(body, .{
        .status = .ok,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/octet-stream" },
            .{ .name = "x-clickhouse-format", .value = "Native" },
            .{ .name = "connection", .value = "close" },
        },
    });
    _ = allocator;
    try out.flush();
}

/// Substitute positional parameters ($1, $2, …) in `sql` using `param_N=value`
/// entries from the URL `target`.  Returns the substituted SQL (heap-allocated;
/// caller owns it) or the original slice if there are no parameters to replace.
/// Parameter values are URL-decoded and SQL-quoted:
///   - Looks like a number (int or float)  → inserted verbatim
///   - Otherwise                            → wrapped in single quotes (') with
///     internal single-quotes escaped as ''
fn substituteParams(allocator: std.mem.Allocator, target: []const u8, sql: []const u8) ![]u8 {
    // Quick check: does the SQL even contain a '$'?
    if (std.mem.indexOfScalar(u8, sql, '$') == null) return allocator.dupe(u8, sql);

    // Collect param_N=value entries.  Support up to 16 params.
    const max_params = 16;
    var param_vals: [max_params]?[]u8 = [_]?[]u8{null} ** max_params;
    defer {
        for (param_vals) |pv| if (pv) |v| allocator.free(v);
    }

    const q_start = std.mem.indexOfScalar(u8, target, '?') orelse target.len;
    var rest = target[q_start..];
    // strip leading '?'
    if (rest.len > 0 and rest[0] == '?') rest = rest[1..];
    while (rest.len > 0) {
        const amp = std.mem.indexOfScalar(u8, rest, '&') orelse rest.len;
        const kv = rest[0..amp];
        rest = if (amp < rest.len) rest[amp + 1 ..] else "";
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse continue;
        const key = kv[0..eq];
        const raw_val = kv[eq + 1 ..];
        // Check prefix "param_" (case-sensitive per ClickHouse spec)
        if (!std.mem.startsWith(u8, key, "param_")) continue;
        const num_str = key["param_".len..];
        const idx = std.fmt.parseInt(usize, num_str, 10) catch continue;
        if (idx < 1 or idx > max_params) continue;
        // URL-decode value
        const decode_buf = try allocator.alloc(u8, raw_val.len);
        defer allocator.free(decode_buf);
        const decoded = urlDecode(raw_val, decode_buf) catch raw_val;
        // Store SQL-quoted value
        const quoted = try sqlQuoteParam(allocator, decoded);
        param_vals[idx - 1] = quoted;
    }

    // Replace each $N in sql with the corresponding value.
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);
    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '$') {
            // Parse digits after '$'
            var j = i + 1;
            while (j < sql.len and std.ascii.isDigit(sql[j])) : (j += 1) {}
            if (j > i + 1) {
                const n = std.fmt.parseInt(usize, sql[i + 1 .. j], 10) catch {
                    try result.append(allocator, sql[i]);
                    i += 1;
                    continue;
                };
                if (n >= 1 and n <= max_params) {
                    if (param_vals[n - 1]) |val| {
                        try result.appendSlice(allocator, val);
                        i = j;
                        continue;
                    }
                }
            }
        }
        try result.append(allocator, sql[i]);
        i += 1;
    }
    return result.toOwnedSlice(allocator);
}

/// SQL-quote a parameter value: numbers pass through verbatim; strings get
/// wrapped in single quotes with internal single-quotes escaped as ''.
fn sqlQuoteParam(allocator: std.mem.Allocator, val: []const u8) ![]u8 {
    // If it looks like an integer or float, pass verbatim.
    if (isNumericLiteral(val)) return allocator.dupe(u8, val);
    // Otherwise quote as string.
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    try buf.append(allocator, '\'');
    for (val) |c| {
        if (c == '\'') try buf.append(allocator, '\''); // escape ''
        try buf.append(allocator, c);
    }
    try buf.append(allocator, '\'');
    return buf.toOwnedSlice(allocator);
}

fn isNumericLiteral(s: []const u8) bool {
    if (s.len == 0) return false;
    var i: usize = 0;
    if (s[0] == '-') i = 1;
    if (i >= s.len) return false;
    var has_dot = false;
    while (i < s.len) : (i += 1) {
        if (s[i] == '.') {
            if (has_dot) return false;
            has_dot = true;
        } else if (!std.ascii.isDigit(s[i])) {
            return false;
        }
    }
    return true;
}

/// Strip trailing FORMAT <name> clause added by clickhouse-go (e.g. "FORMAT Native").
/// Returns a slice into the original string (no allocation).
fn stripFormatClause(sql: []const u8) []const u8 {
    // Trim trailing whitespace
    var end: usize = sql.len;
    while (end > 0 and (sql[end - 1] == ' ' or sql[end - 1] == '\t' or sql[end - 1] == '\r' or sql[end - 1] == '\n')) end -= 1;
    const trimmed = sql[0..end];
    // Walk backwards: last word (format name), then "FORMAT"
    var i = end;
    while (i > 0 and (std.ascii.isAlphanumeric(sql[i - 1]) or sql[i - 1] == '_')) i -= 1;
    if (i == end) return sql; // no word
    // skip spaces
    var j = i;
    while (j > 0 and (sql[j - 1] == ' ' or sql[j - 1] == '\t')) j -= 1;
    // check "FORMAT" keyword
    const kw = "FORMAT";
    if (j >= kw.len and std.ascii.eqlIgnoreCase(trimmed[j - kw.len .. j], kw)) {
        const before = j - kw.len;
        if (before == 0 or !std.ascii.isAlphanumeric(sql[before - 1])) {
            var k = before;
            while (k > 0 and (sql[k - 1] == ' ' or sql[k - 1] == '\t' or sql[k - 1] == '\r' or sql[k - 1] == '\n')) k -= 1;
            return sql[0..k];
        }
    }
    return sql;
}

/// Remove FINAL keyword from SQL (used by ReplacingMergeTree; no-op in ZigHouse).
/// Returns a heap-allocated copy with FINAL removed (caller owns it), or a dupe
/// of the original if FINAL was not present.
fn removeFinal(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    // Case-insensitive search for word-boundary FINAL.
    // When no FINAL is found, returns the original slice to avoid an allocation.
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);
    var i: usize = 0;
    var stripped: bool = false;
    while (i < sql.len) {
        const final_kw = "FINAL";
        if (i + final_kw.len <= sql.len and
            std.ascii.eqlIgnoreCase(sql[i .. i + final_kw.len], final_kw))
        {
            const before_ok = i == 0 or !std.ascii.isAlphanumeric(sql[i - 1]) and sql[i - 1] != '_';
            const after_pos = i + final_kw.len;
            const after_ok = after_pos >= sql.len or (!std.ascii.isAlphanumeric(sql[after_pos]) and sql[after_pos] != '_');
            if (before_ok and after_ok) {
                stripped = true;
                i = after_pos;
                if (result.items.len > 0 and result.items[result.items.len - 1] == ' ') {
                    result.items.len -= 1;
                }
                continue;
            }
        }
        try result.append(allocator, sql[i]);
        i += 1;
    }
    if (!stripped) {
        result.deinit(allocator);
        return @constCast(sql);
    }
    return result.toOwnedSlice(allocator);
}

/// Deduplicate a ResultSet by primary key. For ReplacingMergeTree FINAL queries:
/// ORDER BY version DESC was injected, so the first row per pk has the highest version.
fn dedupResultSetByPk(
    allocator: std.mem.Allocator,
    rs: *const core.ResultSet,
    pk_name: []const u8,
    table: schema.Table,
) !?core.ResultSet {
    _ = table;
    var pk_idx: ?usize = null;
    for (rs.metas, 0..) |meta, i| {
        if (std.mem.eql(u8, meta.name, pk_name)) {
            pk_idx = i;
            break;
        }
    }
    const pki = pk_idx orelse return null;

    const num_rows = rs.num_rows;
    const num_cols = rs.numCols();

    var seen = std.AutoHashMap(u64, void).init(allocator);
    defer seen.deinit();

    var unique_count: usize = 0;
    for (0..num_rows) |r| {
        if (core.chunk.isNull(rs.columns[pki].null_mask, r)) continue;
        const hash = switch (rs.columns[pki].data) {
            .int64 => |vals| @as(u64, @bitCast(vals[r])),
            .uint64 => |vals| vals[r],
            .float64 => |vals| @as(u64, @bitCast(vals[r])),
            .string => |vals| std.hash.Wyhash.hash(0, vals[r]),
            .date_u16 => |vals| @as(u64, vals[r]),
            .datetime64_ms => |vals| @as(u64, @bitCast(vals[r])),
            .bool_u8 => |vals| @as(u64, vals[r]),
            else => @as(u64, r),
        };
        if (seen.contains(hash)) continue;
        try seen.put(hash, {});
        unique_count += 1;
    }

    if (unique_count == num_rows) return null;

    var arena = std.heap.ArenaAllocator.init(allocator);
    const ra = arena.allocator();

    const out_metas = try ra.dupe(core.result.ColMeta, rs.metas);
    const out_cols = try ra.alloc(core.chunk.Column, num_cols);
    for (out_cols, out_metas) |*col, meta| {
        col.* = .{
            .name = meta.name,
            .data = switch (meta.col_type) {
                .bool_u8 => .{ .bool_u8 = try ra.alloc(u8, unique_count) },
                .int64 => .{ .int64 = try ra.alloc(i64, unique_count) },
                .uint64 => .{ .uint64 = try ra.alloc(u64, unique_count) },
                .float64 => .{ .float64 = try ra.alloc(f64, unique_count) },
                .date_u16 => .{ .date_u16 = try ra.alloc(u16, unique_count) },
                .datetime64_ms => .{ .datetime64_ms = try ra.alloc(i64, unique_count) },
                .string => .{ .string = try ra.alloc([]const u8, unique_count) },
                .array_string => .{ .array_string = try ra.alloc([][]const u8, unique_count) },
            },
            .null_mask = try ra.alloc(u64, core.chunk.nullMaskWords(unique_count)),
            .len = unique_count,
        };
        @memset(col.null_mask, 0);
    }

    var seen2 = std.AutoHashMap(u64, void).init(allocator);
    defer seen2.deinit();
    var out_row: usize = 0;
    for (0..num_rows) |r| {
        if (core.chunk.isNull(rs.columns[pki].null_mask, r)) continue;
        const hash = switch (rs.columns[pki].data) {
            .int64 => |vals| @as(u64, @bitCast(vals[r])),
            .uint64 => |vals| vals[r],
            .float64 => |vals| @as(u64, @bitCast(vals[r])),
            .string => |vals| std.hash.Wyhash.hash(0, vals[r]),
            .date_u16 => |vals| @as(u64, vals[r]),
            .datetime64_ms => |vals| @as(u64, @bitCast(vals[r])),
            .bool_u8 => |vals| @as(u64, vals[r]),
            else => @as(u64, r),
        };
        if (seen2.contains(hash)) continue;
        try seen2.put(hash, {});

        for (out_cols, 0..) |*oc, ci| {
            const cs = rs.columns[ci];
            switch (oc.data) {
                .bool_u8 => oc.data.bool_u8[out_row] = cs.data.bool_u8[r],
                .int64 => oc.data.int64[out_row] = cs.data.int64[r],
                .uint64 => oc.data.uint64[out_row] = cs.data.uint64[r],
                .float64 => oc.data.float64[out_row] = cs.data.float64[r],
                .date_u16 => oc.data.date_u16[out_row] = cs.data.date_u16[r],
                .datetime64_ms => oc.data.datetime64_ms[out_row] = cs.data.datetime64_ms[r],
                .string => oc.data.string[out_row] = cs.data.string[r],
                .array_string => oc.data.array_string[out_row] = cs.data.array_string[r],
            }
        }
        out_row += 1;
    }

    return core.ResultSet{
        .metas = out_metas,
        .columns = out_cols,
        .num_rows = unique_count,
        .arena = arena,
    };
}

/// Extract a URL query parameter value from a path like `/?query=...&foo=bar`.
fn extractQueryParam(target: []const u8, param: []const u8) ?[]const u8 {
    const q_start = std.mem.indexOfScalar(u8, target, '?') orelse return null;
    var rest = target[q_start + 1 ..];
    while (rest.len > 0) {
        const amp = std.mem.indexOfScalar(u8, rest, '&') orelse rest.len;
        const kv = rest[0..amp];
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse {
            rest = if (amp < rest.len) rest[amp + 1 ..] else "";
            continue;
        };
        if (std.mem.eql(u8, kv[0..eq], param)) return kv[eq + 1 ..];
        rest = if (amp < rest.len) rest[amp + 1 ..] else "";
    }
    return null;
}

/// Simple URL decoder: %XX and + → space.
fn urlDecode(input: []const u8, buf: []u8) ![]const u8 {
    var out: usize = 0;
    var i: usize = 0;
    while (i < input.len) {
        if (out >= buf.len) return error.BufferTooSmall;
        if (input[i] == '+') {
            buf[out] = ' ';
            out += 1;
            i += 1;
        } else if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexDigit(input[i + 1]) orelse return error.InvalidPercent;
            const lo = hexDigit(input[i + 2]) orelse return error.InvalidPercent;
            buf[out] = (hi << 4) | lo;
            out += 1;
            i += 3;
        } else {
            buf[out] = input[i];
            out += 1;
            i += 1;
        }
    }
    return buf[0..out];
}

fn hexDigit(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

const DbTable = struct { db: []const u8, table: []const u8 };
const InsertInfo = struct {
    db_table: DbTable,
    with_names_and_types: bool,
    native_fmt: bool = false,
    values_fmt: bool = false,
    csv_fmt: bool = false,
    json_each_row_fmt: bool = false,
};

/// Split "db.table" → {db, table}.  If no dot, db = "default".
fn splitDbTable(name: []const u8) DbTable {
    if (std.mem.indexOfScalar(u8, name, '.')) |dot| {
        return .{ .db = name[0..dot], .table = name[dot + 1 ..] };
    }
    return .{ .db = "default", .table = name };
}

fn strictUnsupportedSelect() bool {
    const raw = std.c.getenv("ZIGHOUSE_STRICT_UNSUPPORTED") orelse return false;
    const val = std.mem.span(raw);
    return !(val.len == 0 or
        std.mem.eql(u8, val, "0") or
        std.ascii.eqlIgnoreCase(val, "false") or
        std.ascii.eqlIgnoreCase(val, "off"));
}

/// Parse "INSERT INTO [db.]table [(col1, col2, ...)] FORMAT RowBinary[WithNamesAndTypes|Native]"
fn parseInsertTarget(q: []const u8) ?InsertInfo {
    var it = std.mem.tokenizeAny(u8, q, " \t\r\n");
    const t0 = it.next() orelse return null;
    const t1 = it.next() orelse return null;
    const t2 = it.next() orelse return null;
    if (!std.ascii.eqlIgnoreCase(t0, "INSERT")) return null;
    if (!std.ascii.eqlIgnoreCase(t1, "INTO")) return null;
    // t2 is the table name (possibly db.table)
    const db_table = splitDbTable(t2);
    // Skip optional column list in parentheses: (col1, col2, ...)
    // We scan remaining tokens until we find FORMAT
    var found_format = false;
    var fmt: []const u8 = "";
    while (it.next()) |tok| {
        if (std.ascii.eqlIgnoreCase(tok, "FORMAT")) {
            fmt = it.next() orelse return null;
            found_format = true;
            break;
        }
        // Otherwise skip (column list tokens, commas, etc.)
    }
    if (!found_format) {
        // No FORMAT keyword — check for VALUES clause (SQL-syntax INSERT).
        // INSERT INTO db.table [(cols...)] VALUES (v1, v2, ...)
        // Signal this by returning a special InsertInfo with values_fmt = true.
        return .{
            .db_table = db_table,
            .with_names_and_types = false,
            .native_fmt = false,
            .values_fmt = true,
        };
    }
    const with_header = std.ascii.eqlIgnoreCase(fmt, "RowBinaryWithNamesAndTypes");
    const native_fmt = std.ascii.eqlIgnoreCase(fmt, "Native");
    const csv_fmt = std.ascii.eqlIgnoreCase(fmt, "CSV") or std.ascii.eqlIgnoreCase(fmt, "CSVWithNames");
    const json_fmt = std.ascii.eqlIgnoreCase(fmt, "JSONEachRow") or std.ascii.eqlIgnoreCase(fmt, "NDJSON");
    if (!with_header and !std.ascii.eqlIgnoreCase(fmt, "RowBinary") and !native_fmt and !csv_fmt and !json_fmt) return null;
    return .{
        .db_table = db_table,
        .with_names_and_types = with_header,
        .native_fmt = native_fmt,
        .csv_fmt = csv_fmt,
        .json_each_row_fmt = json_fmt,
    };
}

/// Check that incoming columns are compatible with stored table schema.
/// Compatible means same number of columns, same names and types in order.
fn schemasCompatible(stored: schema.Table, incoming: schema.Table) bool {
    if (stored.columns.len != incoming.columns.len) return false;
    for (stored.columns, incoming.columns) |s, i| {
        if (!std.mem.eql(u8, s.name, i.name)) return false;
        if (s.ty != i.ty) return false;
    }
    return true;
}

fn schemaTypeToChType(ty: schema.ColumnType) []const u8 {
    return switch (ty) {
        .int8 => "UInt8", // UInt8 and Int8 both map to .int8; prefer UInt8 for output
        .int16 => "Int16",
        .int32 => "Int32",
        .int64 => "Int64",
        .float32 => "Float32",
        .float64 => "Float64",
        .text => "String",
        .char => "String",
        .low_card => "String",
        .date => "Date",
        .timestamp => "DateTime64(3)",
    };
}

/// Parser for SQL VALUES INSERT statements.
const SqlValuesParser = struct {
    src: []const u8,
    pos: usize,

    fn skipWs(self: *SqlValuesParser) void {
        while (self.pos < self.src.len and isValuesWs(self.src[self.pos])) self.pos += 1;
    }

    fn peekChar(self: *SqlValuesParser) u8 {
        if (self.pos >= self.src.len) return 0;
        return self.src[self.pos];
    }

    fn consumeKeyword(self: *SqlValuesParser, kw: []const u8) !void {
        self.skipWs();
        if (self.pos + kw.len > self.src.len) return error.ExpectedKeyword;
        if (!std.ascii.eqlIgnoreCase(self.src[self.pos .. self.pos + kw.len], kw)) return error.ExpectedKeyword;
        self.pos += kw.len;
    }

    fn nextToken(self: *SqlValuesParser) ?[]const u8 {
        self.skipWs();
        if (self.pos >= self.src.len) return null;
        const start = self.pos;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            if (isValuesWs(c) or c == ',' or c == '(' or c == ')' or c == ';') break;
            self.pos += 1;
        }
        if (self.pos == start) return null;
        return self.src[start..self.pos];
    }

    fn parseValue(self: *SqlValuesParser, allocator: std.mem.Allocator) ![]const u8 {
        self.skipWs();
        if (self.pos >= self.src.len) return error.UnexpectedEnd;
        const c = self.src[self.pos];
        if (c == '\'') {
            self.pos += 1;
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            errdefer buf.deinit(allocator);
            while (self.pos < self.src.len) {
                const ch = self.src[self.pos];
                if (ch == '\'') {
                    self.pos += 1;
                    if (self.pos < self.src.len and self.src[self.pos] == '\'') {
                        try buf.append(allocator, '\'');
                        self.pos += 1;
                    } else break;
                } else {
                    try buf.append(allocator, ch);
                    self.pos += 1;
                }
            }
            return try buf.toOwnedSlice(allocator);
        }
        // NULL → empty
        if (self.pos + 4 <= self.src.len and std.ascii.eqlIgnoreCase(self.src[self.pos .. self.pos + 4], "NULL")) {
            self.pos += 4;
            return try allocator.dupe(u8, "");
        }
        // Array literal: [elem, elem, ...]
        if (c == '[') {
            const start = self.pos;
            var depth: usize = 0;
            while (self.pos < self.src.len) {
                const ch = self.src[self.pos];
                if (ch == '[') depth += 1 else if (ch == ']') {
                    depth -= 1;
                    if (depth == 0) {
                        self.pos += 1;
                        break;
                    }
                }
                self.pos += 1;
            }
            return try allocator.dupe(u8, self.src[start..self.pos]);
        }
        // Number or bare token
        const start = self.pos;
        while (self.pos < self.src.len) {
            const ch = self.src[self.pos];
            if (isValuesWs(ch) or ch == ',' or ch == ')') break;
            self.pos += 1;
        }
        return try allocator.dupe(u8, self.src[start..self.pos]);
    }
};

fn isValuesWs(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn asciiStartsWith(s: []const u8, prefix: []const u8) bool {
    if (s.len < prefix.len) return false;
    return std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "extractQueryParam: basic" {
    try std.testing.expectEqualStrings(
        "INSERT+INTO+default.t+FORMAT+RowBinary",
        extractQueryParam("/?query=INSERT+INTO+default.t+FORMAT+RowBinary", "query").?,
    );
}

test "extractQueryParam: multiple params" {
    try std.testing.expectEqualStrings("bar", extractQueryParam("/?foo=bar&baz=qux", "foo").?);
    try std.testing.expectEqualStrings("qux", extractQueryParam("/?foo=bar&baz=qux", "baz").?);
    try std.testing.expect(extractQueryParam("/?foo=bar", "missing") == null);
}

test "urlDecode: plus and percent" {
    var buf: [128]u8 = undefined;
    const out = try urlDecode("INSERT+INTO+default.t+FORMAT+RowBinary", &buf);
    try std.testing.expectEqualStrings("INSERT INTO default.t FORMAT RowBinary", out);
}

test "urlDecode: %20" {
    var buf: [64]u8 = undefined;
    const out = try urlDecode("hello%20world", &buf);
    try std.testing.expectEqualStrings("hello world", out);
}

test "parseInsertTarget: db.table" {
    const r = parseInsertTarget("INSERT INTO default.my_table FORMAT RowBinary").?;
    try std.testing.expectEqualStrings("default", r.db_table.db);
    try std.testing.expectEqualStrings("my_table", r.db_table.table);
    try std.testing.expect(!r.with_names_and_types);
}

test "parseInsertTarget: bare table defaults to default db" {
    const r = parseInsertTarget("INSERT INTO my_table FORMAT RowBinary").?;
    try std.testing.expectEqualStrings("default", r.db_table.db);
    try std.testing.expectEqualStrings("my_table", r.db_table.table);
}

test "parseInsertTarget: wrong format returns null" {
    try std.testing.expect(parseInsertTarget("SELECT 1") == null);
    // CSV and JSONEachRow are now supported; truly unsupported formats still return null.
    try std.testing.expect(parseInsertTarget("INSERT INTO t FORMAT Parquet") == null);
    try std.testing.expect(parseInsertTarget("INSERT INTO t FORMAT Arrow") == null);
}

test "splitDbTable: db.table" {
    const r = splitDbTable("default.hits");
    try std.testing.expectEqualStrings("default", r.db);
    try std.testing.expectEqualStrings("hits", r.table);
}

test "splitDbTable: bare table" {
    const r = splitDbTable("hits");
    try std.testing.expectEqualStrings("default", r.db);
    try std.testing.expectEqualStrings("hits", r.table);
}

test "parseInsertTarget: RowBinaryWithNamesAndTypes" {
    const r = parseInsertTarget("INSERT INTO default.my_table FORMAT RowBinaryWithNamesAndTypes").?;
    try std.testing.expectEqualStrings("default", r.db_table.db);
    try std.testing.expectEqualStrings("my_table", r.db_table.table);
    try std.testing.expect(r.with_names_and_types);
}

test "parseInsertTarget: RowBinary sets with_names_and_types=false" {
    const r = parseInsertTarget("INSERT INTO t FORMAT RowBinary").?;
    try std.testing.expect(!r.with_names_and_types);
}

test "parseInsertTarget: case-insensitive keywords" {
    const r = parseInsertTarget("insert into db.t format RowBinary").?;
    try std.testing.expectEqualStrings("db", r.db_table.db);
    try std.testing.expectEqualStrings("t", r.db_table.table);
    try std.testing.expect(!r.with_names_and_types);
}

test "schemasCompatible: identical schemas" {
    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "name", .ty = .text },
    };
    const t = schema.Table{ .name = "t", .columns = &cols };
    try std.testing.expect(schemasCompatible(t, t));
}

test "schemasCompatible: different column count" {
    const cols1 = [_]schema.Column{.{ .name = "id", .ty = .int32 }};
    const cols2 = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "name", .ty = .text },
    };
    const t1 = schema.Table{ .name = "t", .columns = &cols1 };
    const t2 = schema.Table{ .name = "t", .columns = &cols2 };
    try std.testing.expect(!schemasCompatible(t1, t2));
}

test "schemasCompatible: different column name" {
    const cols1 = [_]schema.Column{.{ .name = "id", .ty = .int32 }};
    const cols2 = [_]schema.Column{.{ .name = "uid", .ty = .int32 }};
    const t1 = schema.Table{ .name = "t", .columns = &cols1 };
    const t2 = schema.Table{ .name = "t", .columns = &cols2 };
    try std.testing.expect(!schemasCompatible(t1, t2));
}

test "schemasCompatible: different column type" {
    const cols1 = [_]schema.Column{.{ .name = "id", .ty = .int32 }};
    const cols2 = [_]schema.Column{.{ .name = "id", .ty = .int64 }};
    const t1 = schema.Table{ .name = "t", .columns = &cols1 };
    const t2 = schema.Table{ .name = "t", .columns = &cols2 };
    try std.testing.expect(!schemasCompatible(t1, t2));
}
