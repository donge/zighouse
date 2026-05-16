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
        };
    }

    pub fn deinit(self: *Server) void {
        self.schemas.deinit();
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

        // Extract the `query` parameter from the URL.
        const query_param = extractQueryParam(target, "query") orelse {
            try sendResponse(request, out, .bad_request, "Missing 'query' parameter\n");
            return;
        };

        // URL-decode.
        var decoded_buf: [16 * 1024]u8 = undefined;
        const decoded_query = urlDecode(query_param, &decoded_buf) catch {
            try sendResponse(request, out, .bad_request, "Failed to decode query parameter\n");
            return;
        };

        // Route by SQL verb.
        const trimmed = std.mem.trim(u8, decoded_query, " \t\r\n");
        if (asciiStartsWith(trimmed, "INSERT")) {
            try self.handleInsert(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "SELECT")) {
            try self.handleSelect(request, out, trimmed);
        } else if (asciiStartsWith(trimmed, "CREATE")) {
            try self.handleCreate(request, out, trimmed);
        } else {
            try sendResponse(request, out, .bad_request, "Only CREATE TABLE, INSERT and SELECT are supported\n");
        }
    }

    // ── CREATE TABLE handler ───────────────────────────────────────────────────

    fn handleCreate(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Drain body (DDL has no body).
        var body_buf: [64]u8 = undefined;
        _ = request.readerExpectNone(&body_buf);

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
        // Detect format: RowBinary vs RowBinaryWithNamesAndTypes
        const insert_info = parseInsertTarget(sql) orelse {
            try sendResponse(request, out, .bad_request,
                "Expected: INSERT INTO <db>.<table> FORMAT RowBinary[WithNamesAndTypes]\n");
            return;
        };

        // Read body.
        var body_buf: [256]u8 = undefined;
        const body_reader = request.readerExpectNone(&body_buf);
        const max_body = 256 * 1024 * 1024;
        const body_len: usize = if (request.head.content_length) |cl|
            @min(@as(usize, cl), max_body)
        else
            max_body;
        const body = try body_reader.readAlloc(self.allocator, body_len);
        defer self.allocator.free(body);

        if (body.len == 0) {
            try sendResponse(request, out, .ok, "");
            return;
        }

        if (insert_info.with_names_and_types) {
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

        try sess.writeColumns(columns);
        try sess.finish();
    }

    // ── SELECT handler ─────────────────────────────────────────────────────────

    fn handleSelect(self: *Server, request: *std.http.Server.Request, out: *std.Io.Writer, sql: []const u8) !void {
        // Drain body (ignore for SELECT).
        var body_buf: [64]u8 = undefined;
        _ = request.readerExpectNone(&body_buf);

        // Parse SQL into a Plan.
        const plan = (try generic_sql.parse(self.allocator, sql)) orelse {
            try sendResponse(request, out, .bad_request, "Cannot parse SELECT query\n");
            return;
        };
        defer generic_sql.deinit(self.allocator, plan);

        // Resolve db.table from plan.table (may be "db.table" or bare "table").
        const db_table = splitDbTable(plan.table);

        // Look up schema.
        const entry = self.schemas.find(db_table.db, db_table.table) orelse {
            const msg = try std.fmt.allocPrint(self.allocator,
                "Unknown table '{s}.{s}'\n",
                .{ db_table.db, db_table.table });
            defer self.allocator.free(msg);
            try sendResponse(request, out, .bad_request, msg);
            return;
        };

        // Enumerate parts.
        var parts = try part_scanner.scan(
            self.allocator, self.io,
            self.config.data_dir, db_table.db, db_table.table,
        );
        defer parts.deinit();

        if (parts.dirs().len == 0) {
            // No data yet — return empty result (just header row).
            try sendResponse(request, out, .ok, "");
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
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn sendResponse(request: *std.http.Server.Request, out: *std.Io.Writer, status: std.http.Status, body: []const u8) !void {
    try request.respond(body, .{
        .status = status,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "text/plain; charset=utf-8" },
        },
    });
    try out.flush();
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
const InsertInfo = struct { db_table: DbTable, with_names_and_types: bool };

/// Split "db.table" → {db, table}.  If no dot, db = "default".
fn splitDbTable(name: []const u8) DbTable {
    if (std.mem.indexOfScalar(u8, name, '.')) |dot| {
        return .{ .db = name[0..dot], .table = name[dot + 1 ..] };
    }
    return .{ .db = "default", .table = name };
}

/// Parse "INSERT INTO [db.]table FORMAT RowBinary[WithNamesAndTypes]"
fn parseInsertTarget(q: []const u8) ?InsertInfo {
    var tokens: [8][]const u8 = undefined;
    var n: usize = 0;
    var it = std.mem.tokenizeAny(u8, q, " \t\r\n");
    while (it.next()) |tok| {
        if (n >= tokens.len) break;
        tokens[n] = tok;
        n += 1;
    }
    if (n < 5) return null;
    if (!asciiEql(tokens[0], "INSERT")) return null;
    if (!asciiEql(tokens[1], "INTO")) return null;
    if (!asciiEql(tokens[n - 2], "FORMAT")) return null;
    const fmt = tokens[n - 1];
    const with_header = asciiEql(fmt, "RowBinaryWithNamesAndTypes");
    if (!with_header and !asciiEql(fmt, "RowBinary")) return null;
    return .{
        .db_table = splitDbTable(tokens[2]),
        .with_names_and_types = with_header,
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

fn asciiEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la: u8 = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb: u8 = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
}

fn asciiStartsWith(s: []const u8, prefix: []const u8) bool {
    if (s.len < prefix.len) return false;
    return asciiEql(s[0..prefix.len], prefix);
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
