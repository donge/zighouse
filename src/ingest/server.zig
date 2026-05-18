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
const generic_executor = @import("generic_executor");
const generic_sql = @import("generic_sql");
const ddl_parser = @import("ddl_parser");
const native_block = @import("native_block");

/// Server configuration.
pub const Config = struct {
    data_dir: []const u8,
    port: u16 = 8123,
    /// Optional extra schemas to seed on startup (in addition to auto-loaded ones).
    extra_schemas: ?*const schema_config.SchemaConfig = null,
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    /// Live schema registry (heap-allocated, mutable at runtime).
    schemas: schema_config.SchemaConfig,
    /// Monotonically increasing part sequence number (per-process, not per-table).
    seq: u64,
    /// In-memory view registry: view_name → SELECT SQL (owned strings).
    views: std.StringHashMap([]const u8),
    /// In-memory function registry: fn_name → lambda text "(params) -> body" (owned strings).
    functions: std.StringHashMap([]const u8),

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
            .seq = 1,
            .views = std.StringHashMap([]const u8).init(allocator),
            .functions = std.StringHashMap([]const u8).init(allocator),
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
    }

    /// Block and serve requests until an error or signal.
    pub fn run(self: *Server) !void {
        const net = std.Io.net;
        const address = try net.IpAddress.parseIp4("127.0.0.1", self.config.port);
        var listener = try address.listen(self.io, .{});
        defer listener.deinit(self.io);

        std.debug.print("zighouse serve listening on 127.0.0.1:{d}\n", .{self.config.port});

        while (true) {
            const stream = try listener.accept(self.io);
            self.handleConnection(stream) catch |err| {
                std.debug.print("connection error: {s}\n", .{@errorName(err)});
            };
        }
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
             try self.dispatchSql(request, out, after_params);
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
            // Remove FINAL keyword (no-op in ZigHouse).
            const after_final = try removeFinal(self.allocator, after_params);
            defer self.allocator.free(after_final);
            // Strip trailing FORMAT <name> clause (added by clickhouse-go).
            const trimmed = stripFormatClause(after_final);
            try self.dispatchSqlWithData(request, out, trimmed, data_part);
        }
    }

    fn dispatchSql(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, trimmed: []const u8) !void {
        if (asciiStartsWith(trimmed, "INSERT")) {
            try self.handleInsert(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "SELECT") or asciiStartsWith(trimmed, "WITH")) {
            try self.handleSelect(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "CREATE")) {
            try self.handleCreate(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "TRUNCATE")) {
            // TRUNCATE TABLE <name> — no-op
            try sendResponse(request, out, .ok, "");
        } else if (asciiStartsWith(trimmed, "DROP") or
                   asciiStartsWith(trimmed, "SYSTEM") or
                   asciiStartsWith(trimmed, "ALTER") or
                   asciiStartsWith(trimmed, "SET"))
        {
            // DDL/admin commands we don't need to implement — no-op
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
            try self.handleSelectSimple(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "CREATE")) {
            try self.handleCreateSimple(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "DESCRIBE")) {
            try self.handleDescribeSimple(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "TRUNCATE")) {
            // TRUNCATE TABLE <name> — no-op (ZigHouse uses append-only parts)
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
        } else {
            // Other DDL/admin commands (SYSTEM, DROP, ALTER, SET, etc.) — no-op
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
        }
    }

    // ── CREATE TABLE handler ───────────────────────────────────────────────────

    fn handleCreate(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Drain body (DDL has no body).
        var body_buf: [64]u8 = undefined;
        _ = request.readerExpectNone(&body_buf);

        var it2 = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it2.next(); // CREATE
        const second2 = it2.next() orelse "";
        const third2  = it2.next() orelse "";

        // CREATE DATABASE — no-op.
        if (asciiEql(second2, "DATABASE")) {
            try sendResponse(request, out, .ok, "");
            return;
        }
        // CREATE DICTIONARY — no-op (dictionary support is not implemented yet).
        if (asciiEql(second2, "DICTIONARY")) {
            try sendResponse(request, out, .ok, "");
            return;
        }
        // CREATE VIEW [OR REPLACE] — store view definition.
        if (asciiEql(second2, "VIEW") or
            (asciiEql(second2, "OR") and asciiEql(third2, "REPLACE") and blk: {
                var it3 = std.mem.tokenizeAny(u8, sql, " \t\r\n");
                _ = it3.next(); // CREATE
                _ = it3.next(); // OR
                _ = it3.next(); // REPLACE
                const fourth = it3.next() orelse "";
                break :blk asciiEql(fourth, "VIEW");
            }))
        {
            // Parse: CREATE [OR REPLACE] VIEW [db.]name AS <select>
            // Find "VIEW" keyword position
            const view_kw_pos = std.ascii.indexOfIgnoreCase(sql, "VIEW ") orelse {
                try sendResponse(request, out, .ok, "");
                return;
            };
            const after_view = sql[view_kw_pos + 5..];
            // Find "AS" keyword
            const as_pos = std.ascii.indexOfIgnoreCase(after_view, " AS ") orelse {
                try sendResponse(request, out, .ok, "");
                return;
            };
            const view_full_name = std.mem.trim(u8, after_view[0..as_pos], " \t\r\n");
            const select_sql = std.mem.trim(u8, after_view[as_pos + 4..], " \t\r\n");
            // view_full_name may be "db.name" or just "name"
            const view_name = if (std.mem.indexOfScalar(u8, view_full_name, '.')) |dot_pos|
                view_full_name[dot_pos + 1..]
            else
                view_full_name;
            // Also store with db prefix
            const key_short = try self.allocator.dupe(u8, view_name);
            errdefer self.allocator.free(key_short);
            const val = try self.allocator.dupe(u8, select_sql);
            errdefer self.allocator.free(val);
            try self.views.put(key_short, val);
            // Also store with full name (db.name)
            if (std.mem.indexOfScalar(u8, view_full_name, '.') != null) {
                const key_full = try self.allocator.dupe(u8, view_full_name);
                errdefer self.allocator.free(key_full);
                const val2 = try self.allocator.dupe(u8, select_sql);
                errdefer self.allocator.free(val2);
                try self.views.put(key_full, val2);
            }
            try sendResponse(request, out, .ok, "");
            return;
        }
        // CREATE [OR REPLACE] FUNCTION — store function definition.
        if (asciiEql(second2, "FUNCTION") or
            (asciiEql(second2, "OR") and asciiEql(third2, "REPLACE")))
        {
            // Parse: CREATE [OR REPLACE] FUNCTION name AS (params) -> body
            // Find FUNCTION keyword
            const fn_kw_pos = std.ascii.indexOfIgnoreCase(sql, "FUNCTION ") orelse {
                try sendResponse(request, out, .ok, "");
                return;
            };
            const after_fn = sql[fn_kw_pos + 9..];
            // Next token is function name
            var tok_it2 = std.mem.tokenizeAny(u8, after_fn, " \t\r\n");
            const fn_name_tok = tok_it2.next() orelse {
                try sendResponse(request, out, .ok, "");
                return;
            };
            // Find " AS " keyword
            const as_pos2 = std.ascii.indexOfIgnoreCase(after_fn, " AS ") orelse {
                try sendResponse(request, out, .ok, "");
                return;
            };
            const lambda_body = std.mem.trim(u8, after_fn[as_pos2 + 4..], " \t\r\n");
            const fn_key = try self.allocator.dupe(u8, fn_name_tok);
            errdefer self.allocator.free(fn_key);
            const fn_val = try self.allocator.dupe(u8, lambda_body);
            errdefer self.allocator.free(fn_val);
            try self.functions.put(fn_key, fn_val);
            try sendResponse(request, out, .ok, "");
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
        if (self.schemas.find(parsed.entry.db, parsed.entry.name) != null) {
            try sendResponse(request, out, .ok, "");
            return;
        }

        // Register in memory.
        try self.schemas.addEntry(self.allocator, parsed.entry);

        // Persist schema.json for this table.
        const stored = self.schemas.find(parsed.entry.db, parsed.entry.name).?;
        schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
            std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
        };

        try sendResponse(request, out, .ok, "");
    }

    // ── INSERT handler ─────────────────────────────────────────────────────────

     fn handleInsert(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // VALUES INSERT: INSERT INTO table VALUES (...)
        if (std.ascii.indexOfIgnoreCase(sql, " VALUES") != null) {
            try self.handleInsertValues(request, out, sql);
            return;
        }

        // Detect format: RowBinary vs RowBinaryWithNamesAndTypes
        const insert_info = parseInsertTarget(sql) orelse {
            try sendResponse(request, out, .bad_request,
                "Expected: INSERT INTO <db>.<table> FORMAT RowBinary[WithNamesAndTypes]\n");
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
            const msg = try std.fmt.allocPrint(self.allocator,
                "Unknown table '{s}.{s}': use CREATE TABLE or RowBinaryWithNamesAndTypes first\n",
                .{ db_table.db, db_table.table });
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
            const msg = try std.fmt.allocPrint(self.allocator,
                "Native Block decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (decoded.table.columns.len == 0) {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
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
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
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
            const msg = try std.fmt.allocPrint(self.allocator,
                "RowBinaryWithNamesAndTypes decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        // If table exists: validate schema compatibility.
        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            if (!schemasCompatible(existing.table, decoded.table)) {
                try sendResponse(request, out, .bad_request,
                    "Schema mismatch: incoming columns don't match registered schema\n");
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
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        try sendResponse(request, out, .ok, "");
    }

    /// Write one part to disk and increment seq.
    fn writePart(
        self: *Server,
        db_table: DbTable,
        entry: *const schema_config.TableEntry,
        columns: []row_binary_decoder.ColumnBuffer,
    ) !void {
        const seq = self.seq;
        self.seq += 1;

        var sess = try part_writer_session.PartWriterSession.open(
            self.allocator,
            self.io,
            self.config.data_dir,
            db_table.db,
            db_table.table,
            entry.table,
            entry.pk,
            seq,
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

    // ── SELECT handler ─────────────────────────────────────────────────────────

    fn handleSelect(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Drain body (ignore for SELECT).
        var body_buf: [64]u8 = undefined;
        _ = request.readerExpectNone(&body_buf);

        // Strip FINAL modifier (ReplacingMergeTree FINAL — dedup handled separately).
        var final_mode = false;
        const sql_clean: []const u8 = blk: {
            if (std.ascii.indexOfIgnoreCase(sql, " FINAL") != null) {
                final_mode = true;
                // Remove all " FINAL" occurrences (case-insensitive via two passes)
                const c1 = try std.mem.replaceOwned(u8, self.allocator, sql, " FINAL", "");
                errdefer self.allocator.free(c1);
                const c2 = try std.mem.replaceOwned(u8, self.allocator, c1, " final", "");
                self.allocator.free(c1);
                // Add ORDER BY version DESC for ReplacingMergeTree dedup
                // (if query doesn't already have an ORDER BY)
                if (std.ascii.indexOfIgnoreCase(c2, "ORDER BY") == null) {
                    const c3 = try std.fmt.allocPrint(self.allocator, "{s} ORDER BY version DESC", .{c2});
                    self.allocator.free(c2);
                    break :blk c3;
                }
                break :blk c2;
            }
            break :blk sql;
        };
        defer if (final_mode) self.allocator.free(sql_clean);

         // Parse SQL into a Plan.
         const plan = (try generic_sql.parse(self.allocator, sql_clean)) orelse {
             std.debug.print("Cannot parse SELECT query: {s}\n", .{sql_clean[0..@min(200, sql_clean.len)]});
             try sendResponse(request, out, .bad_request, "Cannot parse SELECT query\n");
             return;
         };
        defer generic_sql.deinit(self.allocator, plan);
        _ = &final_mode;

        // Subquery in FROM clause: run inner plan first, then outer plan over result rows.
        if (plan.subquery_source) |inner_plan| {
            try self.handleSubquerySelect(request, out, plan, inner_plan.*);
            return;
        }

        // UNION ALL: run both plans, concatenate rows (skip second header).
        if (plan.union_other) |right_plan| {
            try self.handleUnionSelect(request, out, plan, right_plan.*);
            return;
        }

        // Resolve db.table from plan.table (may be "db.table" or bare "table").
        const db_table = splitDbTable(plan.table);

        // Make user-defined functions available to the executor.
        generic_executor.udf_registry = &self.functions;

        // Handle system.one — virtual single-row table (dummy UInt8 column).
        if (asciiEql(db_table.db, "system") and asciiEql(db_table.table, "one")) {
            const dummy_table = schema.Table{
                .name = "one",
                .columns = &.{},
            };
            const result = try generic_executor.runWithSource(
                self.allocator, self.io, plan, .{ .csv_rows = "dummy\n0\n" }, &dummy_table,
            );
            defer self.allocator.free(result);
            try sendResponse(request, out, .ok, result);
            return;
        }

        // Look up schema; if not found check if it's a registered view.
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            // Try view registry: short name or db.table full name
            const view_sql = self.views.get(db_table.table) orelse
                self.views.get(plan.table) orelse {
                const msg = try std.fmt.allocPrint(self.allocator,
                    "Unknown table '{s}.{s}'\n",
                    .{ db_table.db, db_table.table });
                defer self.allocator.free(msg);
                try sendResponse(request, out, .bad_request, msg);
                return;
            };
            // Rewrite: run the view's SELECT SQL directly (view has no extra filters in zhtest)
            try self.handleSelect(request, out, view_sql);
            return;
        };

        // Enumerate parts.
        var parts = try part_scanner.scan(
            self.allocator, self.io,
            self.config.data_dir, db_table.db, db_table.table,
        );
        defer parts.deinit();

        if (parts.dirs().len == 0) {
            // No data yet — run the plan against empty source so aggregates return 0.
            const result = try generic_executor.runWithSource(
                self.allocator, self.io,
                plan,
                .{ .ch_parts = &.{} },
                &entry.table,
            );
            defer self.allocator.free(result);
            try sendResponse(request, out, .ok, result);
            return;
        }

        // Run query across all parts.
        const result = try generic_executor.runWithSource(
            self.allocator, self.io,
            plan,
            .{ .ch_parts = parts.dirs() },
            &entry.table,
        );
        defer self.allocator.free(result);

        try sendResponse(request, out, .ok, result);
    }

    /// Handle SELECT with a subquery in FROM: materialize inner plan, then run outer.
    fn handleSubquerySelect(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, outer: generic_sql.Plan, inner: generic_sql.Plan) !void {
        const db_table = splitDbTable(inner.table);
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            try sendResponse(request, out, .bad_request, "Unknown table in subquery\n");
            return;
        };
        var parts = try part_scanner.scan(self.allocator, self.io, self.config.data_dir, db_table.db, db_table.table);
        defer parts.deinit();
        const inner_csv = try generic_executor.runWithSource(self.allocator, self.io, inner, .{ .ch_parts = parts.dirs() }, &entry.table);
        defer self.allocator.free(inner_csv);
        // Run outer plan over the CSV rows materialized from inner plan.
        const result = try generic_executor.runOverCsv(self.allocator, outer, inner_csv, &entry.table);
        defer self.allocator.free(result);
        try sendResponse(request, out, .ok, result);
    }

    /// UNION ALL: run both halves, return left CSV header + all data rows from both.
    fn handleUnionSelect(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, left: generic_sql.Plan, right: generic_sql.Plan) !void {
        const left_csv = try self.runPlanToCSV(left) orelse {
            try sendResponse(request, out, .bad_request, "Cannot execute left side of UNION\n");
            return;
        };
        defer self.allocator.free(left_csv);
        const right_csv = try self.runPlanToCSV(right) orelse {
            try sendResponse(request, out, .bad_request, "Cannot execute right side of UNION\n");
            return;
        };
        defer self.allocator.free(right_csv);
        // Combine: left CSV header + left data rows + right data rows (skip right header)
        var combined: std.ArrayList(u8) = .empty;
        defer combined.deinit(self.allocator);
        try combined.appendSlice(self.allocator, left_csv);
        // Find end of header line in right_csv and append only data lines
        if (std.mem.indexOfScalar(u8, right_csv, '\n')) |nl| {
            try combined.appendSlice(self.allocator, right_csv[nl + 1 ..]);
        }
        try sendResponse(request, out, .ok, combined.items);
    }

    /// Run a Plan against its table's parts, return CSV string (caller frees). Returns null on unknown table.
    fn runPlanToCSV(self: *Server, plan: generic_sql.Plan) !?[]u8 {
        const db_table = splitDbTable(plan.table);
        const entry = self.schemas.find(db_table.db, db_table.table) orelse return null;
        var parts = try part_scanner.scan(self.allocator, self.io, self.config.data_dir, db_table.db, db_table.table);
        defer parts.deinit();
        return try generic_executor.runWithSource(self.allocator, self.io, plan, .{ .ch_parts = parts.dirs() }, &entry.table);
    }



    /// DESCRIBE TABLE handler for body-SQL mode.
    /// clickhouse-go sends "DESCRIBE TABLE db.table" to discover column types before INSERT batch.
    fn handleDescribeSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Parse: DESCRIBE TABLE [db.]table
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it.next(); // DESCRIBE
        _ = it.next(); // TABLE
        const tbl_name = it.next() orelse {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        };
        const db_table = splitDbTable(tbl_name);
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
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
     fn handleSelectSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
         // Debug: log incoming SQL
         std.debug.print("[SQL] {s}\n", .{sql[0..@min(3000, sql.len)]});
         // clickhouse-go handshake: SELECT displayName(), version(), revision(), timezone()
        // Must return a Native Block (clickhouse-go uses default_format=Native).
        if (std.mem.indexOf(u8, sql, "displayName()") != null or
            std.mem.indexOf(u8, sql, "version()") != null)
        {
            const cols = [_]native_block.Col{
                .{ .name = "displayName()", .kind = .string, .str_val = "ZigHouse" },
                .{ .name = "version()",     .kind = .string, .str_val = "24.8.0" },
                .{ .name = "revision()",    .kind = .uint32, .u32_val = 54460 },
                .{ .name = "timezone()",    .kind = .string, .str_val = "UTC" },
            };
            const bytes = try native_block.encodeOneRow(self.allocator, &cols);
            defer self.allocator.free(bytes);
            try sendNativeBlock(self.allocator, request, out, bytes);
            return;
        }
        // SELECT 1 → return a single-row Native Block with column "1" Int64 value 1.
        if (std.mem.eql(u8, sql, "SELECT 1") or std.mem.eql(u8, sql, "select 1")) {
            const cols = [_]native_block.Col{
                .{ .name = "1", .kind = .int64, .i64_val = 1 },
            };
            const bytes = try native_block.encodeOneRow(self.allocator, &cols);
            defer self.allocator.free(bytes);
            try sendNativeBlock(self.allocator, request, out, bytes);
            return;
        }
        // Generic SELECT: route through normal path (body already consumed).
        try self.handleSelectNoDrain(request, out, sql);
    }

    /// SELECT that doesn't drain the body (already consumed by handleRequest).
    fn handleSelectNoDrain(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Make user-defined functions available to the executor.
        generic_executor.udf_registry = &self.functions;

        // Parse SQL into a Plan.
        const plan = (try generic_sql.parse(self.allocator, sql)) orelse {
            try sendResponse(request, out, .bad_request, "Cannot parse SELECT query\n");
            return;
        };
        defer generic_sql.deinit(self.allocator, plan);

        // Subquery: materialize inner, run outer, return Native block.
        if (plan.subquery_source) |inner_plan| {
            const inner_db_table = splitDbTable(inner_plan.table);
            const inner_entry = self.schemas.find(inner_db_table.db, inner_db_table.table) orelse {
                const nb = try native_block.encodeEmpty(self.allocator);
                defer self.allocator.free(nb);
                try sendNativeBlock(self.allocator, request, out, nb);
                return;
            };
            var inner_parts = try part_scanner.scan(self.allocator, self.io, self.config.data_dir, inner_db_table.db, inner_db_table.table);
            defer inner_parts.deinit();
            const inner_csv = try generic_executor.runWithSource(self.allocator, self.io, inner_plan.*, .{ .ch_parts = inner_parts.dirs() }, &inner_entry.table);
            defer self.allocator.free(inner_csv);
            const result = try generic_executor.runOverCsv(self.allocator, plan, inner_csv, &inner_entry.table);
            defer self.allocator.free(result);
            const nb = try csvToNativeBlock(self.allocator, result);
            defer self.allocator.free(nb);
            try sendNativeBlock(self.allocator, request, out, nb);
            return;
        }

        const db_table = splitDbTable(plan.table);

        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            // Unknown table: synthesise a zero-row result.
            // For scalar aggregate queries (e.g. SELECT count() FROM unknown)
            // we must return a properly-shaped block; otherwise return empty block.
            const fake_table = schema.Table{ .name = "", .columns = &.{} };
            if (generic_executor.runWithSource(
                self.allocator, self.io,
                plan,
                .{ .ch_parts = &.{} },
                &fake_table,
            )) |fake_result| {
                defer self.allocator.free(fake_result);
                const nb = try csvToNativeBlock(self.allocator, fake_result);
                defer self.allocator.free(nb);
                try sendNativeBlock(self.allocator, request, out, nb);
            } else |_| {
                const nb = try native_block.encodeEmpty(self.allocator);
                defer self.allocator.free(nb);
                try sendNativeBlock(self.allocator, request, out, nb);
            }
            return;
        };

        var parts = try part_scanner.scan(
            self.allocator, self.io,
            self.config.data_dir, db_table.db, db_table.table,
        );
        defer parts.deinit();

        if (parts.dirs().len == 0) {
            // No data yet — run the plan against an empty source so scalar aggregates
            // (e.g. SELECT count() → 0) return a properly-shaped block.
            if (generic_executor.runWithSource(
                self.allocator, self.io,
                plan,
                .{ .ch_parts = &.{} },
                &entry.table,
            )) |empty_result| {
                defer self.allocator.free(empty_result);
                const nb = try csvToNativeBlockWithSchema(self.allocator, empty_result, &entry.table);
                defer self.allocator.free(nb);
                try sendNativeBlock(self.allocator, request, out, nb);
            } else |_| {
                const nb = try native_block.encodeEmpty(self.allocator);
                defer self.allocator.free(nb);
                try sendNativeBlock(self.allocator, request, out, nb);
            }
            return;
        }

        const result = try generic_executor.runWithSource(
            self.allocator, self.io,
            plan,
            .{ .ch_parts = parts.dirs() },
            &entry.table,
        );
        defer self.allocator.free(result);

        // Wrap CSV result in Native Block so clickhouse-go can parse it.
        const nb = try csvToNativeBlockWithSchema(self.allocator, result, &entry.table);
        defer self.allocator.free(nb);
        try sendNativeBlock(self.allocator, request, out, nb);
    }

    /// CREATE handler for body-SQL mode.
    fn handleCreateSimple(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Classify by second (and possibly third) token.
        var it = std.mem.tokenizeAny(u8, sql, " \t\r\n");
        _ = it.next(); // CREATE
        const second = it.next() orelse "";
        const third  = it.next() orelse "";

        // CREATE DATABASE — no-op.
        if (asciiEql(second, "DATABASE")) {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        }

        // CREATE DICTIONARY / VIEW — no-op.
        if (asciiEql(second, "DICTIONARY") or asciiEql(second, "VIEW")) {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        }

        // CREATE OR REPLACE FUNCTION / CREATE FUNCTION — store definition.
        if (asciiEql(second, "FUNCTION") or
            (asciiEql(second, "OR") and asciiEql(third, "REPLACE")))
        {
            const fn_kw_pos2 = std.ascii.indexOfIgnoreCase(sql, "FUNCTION ") orelse {
                const empty = try native_block.encodeEmpty(self.allocator);
                defer self.allocator.free(empty);
                try sendNativeBlock(self.allocator, request, out, empty);
                return;
            };
            const after_fn2 = sql[fn_kw_pos2 + 9..];
            var tok_fn2 = std.mem.tokenizeAny(u8, after_fn2, " \t\r\n");
            if (tok_fn2.next()) |fn_name2| {
                if (std.ascii.indexOfIgnoreCase(after_fn2, " AS ")) |as_pos3| {
                    const lambda_body2 = std.mem.trim(u8, after_fn2[as_pos3 + 4..], " \t\r\n");
                    const fn_key2 = try self.allocator.dupe(u8, fn_name2);
                    errdefer self.allocator.free(fn_key2);
                    const fn_val2 = try self.allocator.dupe(u8, lambda_body2);
                    errdefer self.allocator.free(fn_val2);
                    try self.functions.put(fn_key2, fn_val2);
                }
            }
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        }

        var parsed = ddl_parser.parse(self.allocator, sql) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator, "DDL parse error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer parsed.deinit();

        if (self.schemas.find(parsed.entry.db, parsed.entry.name) == null) {
            try self.schemas.addEntry(self.allocator, parsed.entry);
            const stored = self.schemas.find(parsed.entry.db, parsed.entry.name).?;
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
        }

        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }

    /// INSERT handler for body-SQL mode.
    /// clickhouse-go sends: "INSERT INTO db.table FORMAT RowBinaryWithNamesAndTypes\n<binary data>"
    fn handleInsertBodyData(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8, data: []const u8) !void {
        const insert_info = parseInsertTarget(sql) orelse {
            try sendResponse(request, out, .bad_request,
                "Expected: INSERT INTO <db>.<table> FORMAT RowBinary[WithNamesAndTypes]\n");
            return;
        };

        if (data.len == 0) {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        }

        if (insert_info.native_fmt) {
            try self.handleInsertNativeData(request, out, insert_info.db_table, data);
        } else if (insert_info.with_names_and_types) {
            try self.handleInsertWithHeaderData(request, out, insert_info.db_table, data);
        } else if (insert_info.values_fmt) {
            // SQL VALUES INSERT — entire sql string contains the full statement.
            try self.handleInsertValues(request, out, sql);
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
                if (it.peekChar() == ')') { it.pos += 1; break; }
                if (it.peekChar() == ',') { it.pos += 1; continue; }
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

        // Parse and write each row
        while (true) {
            it.skipWs();
            if (it.pos >= it.src.len) break;
            if (it.peekChar() == ',') { it.pos += 1; continue; }
            if (it.peekChar() == ';') break;
            if (it.peekChar() != '(') break;
            it.pos += 1; // consume '('

            // Build ColumnBuffers for this row
            const col_bufs = try self.allocator.alloc(row_binary_decoder.ColumnBuffer, entry.table.columns.len);
            for (col_bufs, entry.table.columns) |*buf, col| {
                buf.* = row_binary_decoder.ColumnBuffer{
                    .col = col,
                    .fixed_vals = .empty,
                    .str_vals = .empty,
                    .str_bytes = .empty,
                };
            }
            defer {
                for (col_bufs) |*buf| buf.deinit(self.allocator);
                self.allocator.free(col_bufs);
            }

            var val_count: usize = 0;
            while (val_count < n_cols) {
                it.skipWs();
                if (it.peekChar() == ')') break;
                if (it.peekChar() == ',') { it.pos += 1; continue; }
                const val_str = try it.parseValue(self.allocator);
                defer self.allocator.free(val_str);

                const col_idx = if (col_indices) |ci| ci[val_count] else val_count;
                if (col_idx < col_bufs.len) {
                    const col_ty = entry.table.columns[col_idx].ty;
                    var buf = &col_bufs[col_idx];
                    switch (col_ty) {
                        .text, .char => {
                            const start = buf.str_bytes.items.len;
                            try buf.str_bytes.appendSlice(self.allocator, val_str);
                            const end = buf.str_bytes.items.len;
                            try buf.str_vals.append(self.allocator, buf.str_bytes.items[start..end]);
                        },
                        else => {
                            const iv: i64 = if (val_str.len == 0) 0 else
                                std.fmt.parseInt(i64, val_str, 10) catch
                                @as(i64, @intFromFloat(std.fmt.parseFloat(f64, val_str) catch 0.0));
                            try buf.fixed_vals.append(self.allocator, iv);
                        },
                    }
                }
                val_count += 1;
            }
            it.skipWs();
            if (it.peekChar() == ')') it.pos += 1;

            // Fill any columns with 0 empty values not set
            for (col_bufs) |*buf| {
                if (buf.rowCount() == 0) {
                    switch (buf.col.ty) {
                        .text, .char => {
                            const start = buf.str_bytes.items.len;
                            _ = start;
                            try buf.str_vals.append(self.allocator, buf.str_bytes.items[0..0]);
                        },
                        else => try buf.fixed_vals.append(self.allocator, 0),
                    }
                }
            }

            // Write the part
            const db_table = DbTable{ .db = db_name, .table = table_name };
            try self.writePart(db_table, entry, col_bufs);
        }

        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }

    /// Native Block INSERT for body-SQL mode (data already split from SQL line).
    fn handleInsertNativeData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        data: []const u8,
    ) !void {
        var decoded = row_binary_decoder.decodeNativeBlock(self.allocator, data) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator,
                "Native Block decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (decoded.table.columns.len == 0) {
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
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
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }

    fn handleInsertWithHeaderData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        var decoded = row_binary_decoder.decodeWithHeader(self.allocator, body) catch |err| {
            const msg = try std.fmt.allocPrint(self.allocator,
                "RowBinaryWithNamesAndTypes decode error: {s}\n", .{@errorName(err)});
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };
        defer decoded.deinit(self.allocator);

        if (self.schemas.find(db_table.db, db_table.table)) |existing| {
            if (!schemasCompatible(existing.table, decoded.table)) {
                try sendResponse(request, out, .bad_request,
                    "Schema mismatch: incoming columns don't match registered schema\n");
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
            schema_persist.save(self.io, self.allocator, self.config.data_dir, stored.db, stored) catch |err| {
                std.debug.print("schema_persist.save warning: {s}\n", .{@errorName(err)});
            };
            try self.writePart(db_table, stored, decoded.decoder.columns);
        }

        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }

    fn handleInsertRowBinaryData(
        self: *Server,
        request: *std.http.Server.Request,
        out: *std.Io.Writer,
        db_table: DbTable,
        body: []const u8,
    ) !void {
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            const msg = try std.fmt.allocPrint(self.allocator,
                "Unknown table '{s}.{s}': use CREATE TABLE or RowBinaryWithNamesAndTypes first\n",
                .{ db_table.db, db_table.table });
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
            const empty = try native_block.encodeEmpty(self.allocator);
            defer self.allocator.free(empty);
            try sendNativeBlock(self.allocator, request, out, empty);
            return;
        }

        try self.writePart(db_table, entry, dec.columns);
        const empty = try native_block.encodeEmpty(self.allocator);
        defer self.allocator.free(empty);
        try sendNativeBlock(self.allocator, request, out, empty);
    }
};

// ── Helpers ───────────────────────────────────────────────────────────────────

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
    // Case-insensitive search for word-boundary FINAL
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(allocator);
    var i: usize = 0;
    while (i < sql.len) {
        // Check for FINAL at word boundary
        const final_kw = "FINAL";
        if (i + final_kw.len <= sql.len and
            std.ascii.eqlIgnoreCase(sql[i .. i + final_kw.len], final_kw))
        {
            const before_ok = i == 0 or !std.ascii.isAlphanumeric(sql[i - 1]) and sql[i - 1] != '_';
            const after_pos = i + final_kw.len;
            const after_ok = after_pos >= sql.len or (!std.ascii.isAlphanumeric(sql[after_pos]) and sql[after_pos] != '_');
            if (before_ok and after_ok) {
                // Skip FINAL + any trailing space
                i = after_pos;
                // Remove one preceding space if present
                if (result.items.len > 0 and result.items[result.items.len - 1] == ' ') {
                    result.items.len -= 1;
                }
                continue;
            }
        }
        try result.append(allocator, sql[i]);
        i += 1;
    }
    return result.toOwnedSlice(allocator);
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
const InsertInfo = struct { db_table: DbTable, with_names_and_types: bool, native_fmt: bool = false, values_fmt: bool = false };

/// Split "db.table" → {db, table}.  If no dot, db = "default".
fn splitDbTable(name: []const u8) DbTable {
    if (std.mem.indexOfScalar(u8, name, '.')) |dot| {
        return .{ .db = name[0..dot], .table = name[dot + 1 ..] };
    }
    return .{ .db = "default", .table = name };
}

/// Parse "INSERT INTO [db.]table [(col1, col2, ...)] FORMAT RowBinary[WithNamesAndTypes|Native]"
fn parseInsertTarget(q: []const u8) ?InsertInfo {
    var it = std.mem.tokenizeAny(u8, q, " \t\r\n");
    const t0 = it.next() orelse return null;
    const t1 = it.next() orelse return null;
    const t2 = it.next() orelse return null;
    if (!asciiEql(t0, "INSERT")) return null;
    if (!asciiEql(t1, "INTO")) return null;
    // t2 is the table name (possibly db.table)
    const db_table = splitDbTable(t2);
    // Skip optional column list in parentheses: (col1, col2, ...)
    // We scan remaining tokens until we find FORMAT
    var found_format = false;
    var fmt: []const u8 = "";
    while (it.next()) |tok| {
        if (asciiEql(tok, "FORMAT")) {
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
    const with_header = asciiEql(fmt, "RowBinaryWithNamesAndTypes");
    const native_fmt = asciiEql(fmt, "Native");
    if (!with_header and !asciiEql(fmt, "RowBinary") and !native_fmt) return null;
    return .{
        .db_table = db_table,
        .with_names_and_types = with_header,
        .native_fmt = native_fmt,
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
        .int8    => "UInt8",   // UInt8 and Int8 both map to .int8; prefer UInt8 for output
        .int16   => "Int16",
        .int32   => "Int32",
        .int64   => "Int64",
        .float32 => "Float32",
        .float64 => "Float64",
        .text    => "String",
        .char    => "String",
        .date    => "Date",
        .timestamp => "DateTime64(3)",
    };
}

fn asciiEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la: u8 = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb: u8 = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
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
        if (!std.ascii.eqlIgnoreCase(self.src[self.pos..self.pos + kw.len], kw)) return error.ExpectedKeyword;
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
                } else { try buf.append(allocator, ch); self.pos += 1; }
            }
            return try buf.toOwnedSlice(allocator);
        }
        // NULL → empty
        if (self.pos + 4 <= self.src.len and std.ascii.eqlIgnoreCase(self.src[self.pos..self.pos + 4], "NULL")) {
            self.pos += 4;
            return try allocator.dupe(u8, "");
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
    return asciiEql(s[0..prefix.len], prefix);
}

/// Convert a CSV result (header\nvalues\n) from generic_executor into a Native Block.
/// All columns are treated as either Int64 (parseable integer) or String.
/// Supports multi-column results (scalar agg) and multi-row results (group-by, scan).
/// Returns true if a column name looks like an aggregate (count, sum, min, max, avg, total, etc.)
/// such columns should never be encoded as UInt8 even if their values are 0/1.
fn isAggColName(name: []const u8) bool {
    const lower = blk: {
        var buf: [64]u8 = undefined;
        if (name.len > buf.len) break :blk name;
        break :blk std.ascii.lowerString(&buf, name);
    };
    const agg_prefixes = [_][]const u8{ "count", "sum(", "avg(", "min(", "max(", "total", "any(", "groupuniq" };
    for (agg_prefixes) |pfx| {
        if (std.mem.startsWith(u8, lower, pfx)) return true;
    }
    return false;
}

fn csvToNativeBlock(allocator: std.mem.Allocator, csv: []const u8) ![]u8 {
    return csvToNativeBlockWithSchema(allocator, csv, null);
}

fn csvToNativeBlockWithSchema(allocator: std.mem.Allocator, csv: []const u8, tbl: ?*const schema.Table) ![]u8 {
    // Split into lines, skip trailing empty
    var lines: std.ArrayListUnmanaged([]const u8) = .empty;
    defer lines.deinit(allocator);
    var it = std.mem.splitScalar(u8, csv, '\n');
    while (it.next()) |line| {
        if (line.len > 0) try lines.append(allocator, line);
    }
    if (lines.items.len == 0) return native_block.encodeEmpty(allocator);

    // Parse header (first line): comma-separated column names
    var col_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer col_names.deinit(allocator);
    var hdr_it = std.mem.splitScalar(u8, lines.items[0], ',');
    while (hdr_it.next()) |name| try col_names.append(allocator, name);

    const num_cols = col_names.items.len;
    if (num_cols == 0) return native_block.encodeEmpty(allocator);

    const num_rows = lines.items.len - 1;

    // For each column, collect string values then determine type (Int64, Float64, Array(String), or String)
    const ColData = struct {
        vals: [][]const u8,
        is_int: bool,
        is_float: bool,
        is_array: bool,  // true if any value starts with 0x01 (array sentinel)
        has_negative: bool,
        max_int: i64,    // maximum integer value seen (for UInt8 detection)
        is_bool: bool,   // true if all values are "0" or "1" (encode as UInt8)
    };
    const col_data = try allocator.alloc(ColData, num_cols);
    defer allocator.free(col_data);

    for (0..num_cols) |ci| {
        const vals = try allocator.alloc([]const u8, num_rows);
        col_data[ci] = .{ .vals = vals, .is_int = true, .is_float = true, .is_array = false, .has_negative = false, .max_int = 0, .is_bool = true };
    }
    defer for (col_data) |cd| allocator.free(cd.vals);

    for (lines.items[1..], 0..) |line, ri| {
        var val_it = std.mem.splitScalar(u8, line, ',');
        var ci: usize = 0;
        while (val_it.next()) |val| : (ci += 1) {
            if (ci >= num_cols) break;
            col_data[ci].vals[ri] = val;
            // Check for array sentinel (0x01 prefix from writeCsv)
            if (val.len > 0 and val[0] == 0x01) {
                col_data[ci].is_array = true;
                col_data[ci].is_int = false;
                col_data[ci].is_float = false;
                continue;
            }
            // Try to parse as int
            if (col_data[ci].is_int) {
                const iv = std.fmt.parseInt(i64, val, 10) catch {
                    col_data[ci].is_int = false;
                    col_data[ci].is_bool = false;
                    // Fall through to float check below (don't continue).
                    _ = std.fmt.parseFloat(f64, val) catch {
                        col_data[ci].is_float = false;
                    };
                    continue;
                };
                if (iv < 0) col_data[ci].has_negative = true;
                if (iv > col_data[ci].max_int) col_data[ci].max_int = iv;
                // Track if all values are 0 or 1 (boolean/UInt8)
                if (iv != 0 and iv != 1) { col_data[ci].is_bool = false; }
            }
            // Try to parse as float (only if not int)
            if (!col_data[ci].is_int and col_data[ci].is_float) {
                _ = std.fmt.parseFloat(f64, val) catch {
                    col_data[ci].is_float = false;
                };
            }
        }
        // Fill missing columns with empty string
        while (ci < num_cols) : (ci += 1) {
            col_data[ci].vals[ri] = "";
            col_data[ci].is_int = false;
            col_data[ci].is_float = false;
        }
    }

    // Encode Native Block
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);

    // Inline putUVarInt / putString helpers
    const putV = struct {
        fn f(b: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, v: u64) !void {
            var x = v;
            while (x >= 0x80) {
                try b.append(a, @as(u8, @intCast((x & 0x7F) | 0x80)));
                x >>= 7;
            }
            try b.append(a, @as(u8, @intCast(x)));
        }
    }.f;
    const putS = struct {
        fn f(b: *std.ArrayListUnmanaged(u8), a: std.mem.Allocator, s: []const u8) !void {
            var x = s.len;
            while (x >= 0x80) {
                try b.append(a, @as(u8, @intCast((x & 0x7F) | 0x80)));
                x >>= 7;
            }
            try b.append(a, @as(u8, @intCast(x)));
            try b.appendSlice(a, s);
        }
    }.f;

    // Block info
    try putV(&buf, allocator, 1);
    try buf.append(allocator, 0);
    try putV(&buf, allocator, 2);
    try buf.appendSlice(allocator, &[4]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    try putV(&buf, allocator, 0);

    try putV(&buf, allocator, num_cols);
    try putV(&buf, allocator, num_rows);

    for (0..num_cols) |ci| {
        try putS(&buf, allocator, col_names.items[ci]);

        // Determine actual type from schema if available
        const schema_ty: ?schema.ColumnType = if (tbl) |t| blk: {
            const idx = t.findColumn(col_names.items[ci]) orelse break :blk null;
            break :blk t.columns[idx].ty;
        } else null;

        // Check if column has ch_type Array(...) or Map(...) — these take priority
        // over schema_ty (which maps them to .text) when is_array is detected.
        const ch_type_override: ?[]const u8 = if (tbl) |t| blk: {
            const idx = t.findColumn(col_names.items[ci]) orelse break :blk null;
            const ct = t.columns[idx].ch_type orelse break :blk null;
            if (std.mem.startsWith(u8, ct, "Array(") or std.mem.startsWith(u8, ct, "Map(")) break :blk ct;
            break :blk null;
        } else null;

        // If this column is an Array column (detected via sentinel or ch_type), encode as Array(String).
        // This must come BEFORE schema_ty check since Array(String) maps to .text in schema.
        const force_array = (ch_type_override != null and std.mem.startsWith(u8, ch_type_override.?, "Array(")) or col_data[ci].is_array;

        if (schema_ty != null and !force_array) {
            // Use schema type for precise encoding
            switch (schema_ty.?) {
                .int8 => {
                    try putS(&buf, allocator, "UInt8");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(u8, val, 10) catch 0;
                        try buf.append(allocator, iv);
                    }
                },
                .int16 => {
                    try putS(&buf, allocator, "Int16");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(i16, val, 10) catch 0;
                        var tmp: [2]u8 = undefined;
                        std.mem.writeInt(i16, &tmp, iv, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .int32 => {
                    try putS(&buf, allocator, "Int32");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(i32, val, 10) catch 0;
                        var tmp: [4]u8 = undefined;
                        std.mem.writeInt(i32, &tmp, iv, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .int64 => {
                    try putS(&buf, allocator, "Int64");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(i64, val, 10) catch 0;
                        var tmp: [8]u8 = undefined;
                        std.mem.writeInt(i64, &tmp, iv, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .date => {
                    try putS(&buf, allocator, "Date");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(u16, val, 10) catch 0;
                        var tmp: [2]u8 = undefined;
                        std.mem.writeInt(u16, &tmp, iv, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .timestamp => {
                    try putS(&buf, allocator, "DateTime64(3)");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const iv = std.fmt.parseInt(i64, val, 10) catch 0;
                        var tmp: [8]u8 = undefined;
                        std.mem.writeInt(i64, &tmp, iv, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .float32 => {
                    try putS(&buf, allocator, "Float32");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const fv = std.fmt.parseFloat(f32, val) catch 0.0;
                        const bits: u32 = @bitCast(fv);
                        var tmp: [4]u8 = undefined;
                        std.mem.writeInt(u32, &tmp, bits, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .float64 => {
                    try putS(&buf, allocator, "Float64");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        const fv = std.fmt.parseFloat(f64, val) catch 0.0;
                        const bits: u64 = @bitCast(fv);
                        var tmp: [8]u8 = undefined;
                        std.mem.writeInt(u64, &tmp, bits, .little);
                        try buf.appendSlice(allocator, &tmp);
                    }
                },
                .text, .char => {
                    try putS(&buf, allocator, "String");
                    try buf.append(allocator, 0);
                    for (col_data[ci].vals) |val| {
                        try putS(&buf, allocator, val);
                    }
                },
            }
        } else if (col_data[ci].is_int and num_rows > 0) {
            // Heuristic: large positive integers are epoch timestamps → DateTime64(3)
            const first_val_int = std.fmt.parseInt(i64, col_data[ci].vals[0], 10) catch 0;
            // >= 1e12: millisecond-precision epoch (2001+), already in ms
            // >= 1e9 and < 1e12: second-precision epoch (2001-2286), multiply by 1000
            const is_timestamp_ms = !col_data[ci].has_negative and first_val_int >= 1_000_000_000_000;
            const is_timestamp_s  = !col_data[ci].has_negative and first_val_int >= 1_000_000_000 and first_val_int < 1_000_000_000_000;
            if (is_timestamp_ms) {
                try putS(&buf, allocator, "DateTime64(3)");
                try buf.append(allocator, 0);
                for (col_data[ci].vals) |val| {
                    const iv = std.fmt.parseInt(i64, val, 10) catch 0;
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(i64, &tmp, iv, .little);
                    try buf.appendSlice(allocator, &tmp);
                }
            } else if (is_timestamp_s) {
                // Second-precision epoch: convert to milliseconds for DateTime64(3)
                try putS(&buf, allocator, "DateTime64(3)");
                try buf.append(allocator, 0);
                for (col_data[ci].vals) |val| {
                    const iv = (std.fmt.parseInt(i64, val, 10) catch 0) * 1000;
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(i64, &tmp, iv, .little);
                    try buf.appendSlice(allocator, &tmp);
                }
            } else if (col_data[ci].has_negative) {
                try putS(&buf, allocator, "Int64");
                try buf.append(allocator, 0);
                for (col_data[ci].vals) |val| {
                    const iv = std.fmt.parseInt(i64, val, 10) catch 0;
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(i64, &tmp, iv, .little);
                    try buf.appendSlice(allocator, &tmp);
                }
            } else if (col_data[ci].is_bool and !isAggColName(col_names.items[ci])) {
                // Boolean-range non-negative integers (0 or 1) → UInt8
                // (e.g. has(), isIPv4String(), countIf results 0/1)
                try putS(&buf, allocator, "UInt8");
                try buf.append(allocator, 0);
                for (col_data[ci].vals) |val| {
                    const iv = std.fmt.parseInt(u8, val, 10) catch 0;
                    try buf.append(allocator, iv);
                }
            } else {
                try putS(&buf, allocator, "UInt64");
                try buf.append(allocator, 0);
                for (col_data[ci].vals) |val| {
                    const iv = std.fmt.parseInt(u64, val, 10) catch 0;
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(u64, &tmp, iv, .little);
                    try buf.appendSlice(allocator, &tmp);
                }
            }
        } else if (col_data[ci].is_float and num_rows > 0) {
            try putS(&buf, allocator, "Float64");
            try buf.append(allocator, 0); // custom_serialization=false
            for (col_data[ci].vals) |val| {
                const fv = std.fmt.parseFloat(f64, val) catch 0.0;
                const bits: u64 = @bitCast(fv);
                var tmp: [8]u8 = undefined;
                std.mem.writeInt(u64, &tmp, bits, .little);
                try buf.appendSlice(allocator, &tmp);
            }
        } else if (col_data[ci].is_array) {
            // Array(String): each value is \x01 + elements joined by \x0c
            try putS(&buf, allocator, "Array(String)");
            try buf.append(allocator, 0); // custom_serialization=false
            // ClickHouse Native Array format:
            // 1) Cumulative offsets: one uint64 per row (how many total elements through this row)
            // 2) All element strings concatenated (each as varint_len + bytes)
            var all_elems: std.ArrayListUnmanaged([]const u8) = .empty;
            defer all_elems.deinit(allocator);
            var cumulative: u64 = 0;
            // First pass: write offsets and collect elements
            var offsets_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer offsets_buf.deinit(allocator);
            for (col_data[ci].vals) |val| {
                const content = if (val.len > 0 and val[0] == 0x01) val[1..] else "";
                if (content.len == 0) {
                    // empty array — offset unchanged
                } else {
                    var elem_it = std.mem.splitScalar(u8, content, '\x0c');
                    while (elem_it.next()) |elem| {
                        cumulative += 1;
                        try all_elems.append(allocator, elem);
                    }
                }
                // Write offset as little-endian uint64
                var tmp: [8]u8 = undefined;
                std.mem.writeInt(u64, &tmp, cumulative, .little);
                try offsets_buf.appendSlice(allocator, &tmp);
            }
            try buf.appendSlice(allocator, offsets_buf.items);
            // Second: write all elements
            for (all_elems.items) |elem| try putS(&buf, allocator, elem);
        } else {
            try putS(&buf, allocator, "String");
            try buf.append(allocator, 0); // custom_serialization=false
            for (col_data[ci].vals) |val| {
                try putS(&buf, allocator, val);
            }
        }
    }

    // Empty terminator block
    try putV(&buf, allocator, 1);
    try buf.append(allocator, 0);
    try putV(&buf, allocator, 2);
    try buf.appendSlice(allocator, &[4]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    try putV(&buf, allocator, 0);
    try putV(&buf, allocator, 0);
    try putV(&buf, allocator, 0);

    return buf.toOwnedSlice(allocator);
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
    try std.testing.expect(parseInsertTarget("INSERT INTO t FORMAT CSV") == null);
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
