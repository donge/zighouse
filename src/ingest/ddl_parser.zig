/// Minimal CREATE TABLE DDL parser for zighouse serve.
///
/// Supported syntax (case-insensitive keywords):
///
///   CREATE TABLE [IF NOT EXISTS] [db.]table (
///     col_name TypeName [, ...]
///   ) ENGINE = MergeTree [ORDER BY col] [PRIMARY KEY col]
///
/// Supported Phase-1 types (case-insensitive):
///   Int16, Int32, Int64, Date, DateTime, String
///   UInt16  → .date  (ClickHouse Date is stored as UInt16 days)
///   UInt32  → .timestamp
///
/// Returns a ParseResult with the table entry, or an error.
///
/// Usage:
///   const result = try ddl_parser.parse(allocator, sql);
///   defer result.deinit(allocator);
///   // result.entry contains db, name, pk, table

const std = @import("std");
const schema = @import("schema");
const schema_config = @import("schema_config");

pub const ParseResult = struct {
    entry: schema_config.TableEntry,
    /// Columns slice is owned here (freed by deinit).
    columns: []schema.Column,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ParseResult) void {
        for (self.columns) |col| self.allocator.free(col.name);
        self.allocator.free(self.columns);
        self.allocator.free(self.entry.db);
        self.allocator.free(self.entry.name);
        self.allocator.free(self.entry.table.name);
        if (self.entry.pk) |pk| self.allocator.free(pk);
    }
};

/// Parse a CREATE TABLE statement.
/// Caller must call result.deinit(allocator) when done.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !ParseResult {
    var tok = Tokenizer.init(sql);

    // CREATE
    try expectKeyword(&tok, "CREATE");
    // TABLE
    try expectKeyword(&tok, "TABLE");
    // IF NOT EXISTS (optional)
    if (tok.peekKeyword("IF")) {
        tok.skip();
        try expectKeyword(&tok, "NOT");
        try expectKeyword(&tok, "EXISTS");
    }

    // [db.]table
    const first_name = tok.next() orelse return error.MissingTableName;
    var db: []const u8 = "default";
    var table_name: []const u8 = first_name;
    if (tok.peekChar('.')) {
        tok.skip(); // consume '.'
        db = first_name;
        table_name = tok.next() orelse return error.MissingTableName;
    }
    const db_owned = try allocator.dupe(u8, db);
    errdefer allocator.free(db_owned);
    const table_name_owned = try allocator.dupe(u8, table_name);
    errdefer allocator.free(table_name_owned);
    const table_name_for_table = try allocator.dupe(u8, table_name);
    errdefer allocator.free(table_name_for_table);

    // '('
    try expectChar(&tok, '(');

    // Column definitions: name Type [, name Type ...]
    var cols: std.ArrayListUnmanaged(schema.Column) = .empty;
    errdefer {
        for (cols.items) |col| allocator.free(col.name);
        cols.deinit(allocator);
    }

    while (true) {
        // Skip commas between columns
        while (tok.peekChar(',')) tok.skip();
        // Check for closing ')'
        if (tok.peekChar(')')) break;

        const col_name_raw = tok.next() orelse return error.UnexpectedEndInColumnList;
        // Could be ')' as a token string — handle edge case
        if (col_name_raw.len == 1 and col_name_raw[0] == ')') break;
        // Skip "PRIMARY KEY (...)" inside column list (ClickHouse syntax variant)
        if (asciiEql(col_name_raw, "PRIMARY")) {
            try expectKeyword(&tok, "KEY");
            _ = tok.next(); // consume key column
            continue;
        }

        const col_type_raw = tok.next() orelse return error.MissingColumnType;
        const col_ty = parseColumnType(col_type_raw) orelse {
            // Handle Nullable(T), LowCardinality(T) etc. by trying inner type
            const inner = extractInnerType(col_type_raw);
            _ = parseColumnType(inner) orelse return error.UnsupportedColumnType;
            // Use inner type
            const col_name_owned = try allocator.dupe(u8, col_name_raw);
            try cols.append(allocator, .{ .name = col_name_owned, .ty = parseColumnType(inner).? });
            continue;
        };

        const col_name_owned = try allocator.dupe(u8, col_name_raw);
        try cols.append(allocator, .{ .name = col_name_owned, .ty = col_ty });
    }

    // ')' already consumed by peekChar/break — consume it
    if (tok.peekChar(')')) tok.skip();

    if (cols.items.len == 0) return error.NoColumnsFound;

    // ENGINE = ... (optional for us — we ignore the engine name)
    // ORDER BY col — first column becomes pk
    var pk: ?[]const u8 = null;
    errdefer if (pk) |p| allocator.free(p);

    while (tok.next()) |kw| {
        if (asciiEql(kw, "ORDER")) {
            const by = tok.next() orelse break;
            if (!asciiEql(by, "BY")) break;
            // ORDER BY can be (col1, col2) or just col1
            if (tok.peekChar('(')) {
                tok.skip(); // consume '('
                const first_col = tok.next() orelse break;
                pk = try allocator.dupe(u8, first_col);
                // drain rest of tuple
                while (tok.next()) |t| {
                    if (t.len == 1 and t[0] == ')') break;
                }
            } else {
                const order_col = tok.next() orelse break;
                pk = try allocator.dupe(u8, order_col);
            }
        } else if (asciiEql(kw, "PRIMARY")) {
            const key_kw = tok.next() orelse break;
            if (!asciiEql(key_kw, "KEY")) break;
            if (tok.peekChar('(')) {
                tok.skip();
                const first_col = tok.next() orelse break;
                if (pk == null) pk = try allocator.dupe(u8, first_col);
                while (tok.next()) |t| {
                    if (t.len == 1 and t[0] == ')') break;
                }
            } else {
                const pk_col = tok.next() orelse break;
                if (pk == null) pk = try allocator.dupe(u8, pk_col);
            }
        }
        // ignore ENGINE, SETTINGS, PARTITION BY etc.
    }

    const columns_slice = try cols.toOwnedSlice(allocator);

    return .{
        .entry = .{
            .db = db_owned,
            .name = table_name_owned,
            .pk = pk,
            .table = .{ .name = table_name_for_table, .columns = columns_slice },
        },
        .columns = columns_slice,
        .allocator = allocator,
    };
}

// ── Type mapping ──────────────────────────────────────────────────────────────

fn parseColumnType(s: []const u8) ?schema.ColumnType {
    if (asciiEql(s, "Int16")) return .int16;
    if (asciiEql(s, "Int32")) return .int32;
    if (asciiEql(s, "Int64")) return .int64;
    if (asciiEql(s, "UInt16")) return .date;       // Date stored as UInt16
    if (asciiEql(s, "UInt32")) return .timestamp;  // DateTime stored as UInt32
    if (asciiEql(s, "Date")) return .date;
    if (asciiEql(s, "Date32")) return .date;
    if (asciiEql(s, "DateTime")) return .timestamp;
    if (asciiEql(s, "DateTime64")) return .timestamp;
    if (asciiEql(s, "String")) return .text;
    if (asciiEql(s, "FixedString")) return .text;
    return null;
}

/// Extract inner type from Nullable(T) / LowCardinality(T) / FixedString(N).
fn extractInnerType(s: []const u8) []const u8 {
    const lparen = std.mem.indexOfScalar(u8, s, '(') orelse return s;
    const rparen = std.mem.lastIndexOfScalar(u8, s, ')') orelse return s;
    if (rparen > lparen) return s[lparen + 1 .. rparen];
    return s;
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

/// Simple whitespace+punctuation tokenizer.
/// Punctuation characters ( ) , . ; are returned as single-char tokens.
const Tokenizer = struct {
    src: []const u8,
    pos: usize,

    fn init(src: []const u8) Tokenizer {
        return .{ .src = src, .pos = 0 };
    }

    fn skipWs(self: *Tokenizer) void {
        while (self.pos < self.src.len) {
            switch (self.src[self.pos]) {
                ' ', '\t', '\r', '\n' => self.pos += 1,
                '-' => {
                    // -- line comment
                    if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '-') {
                        while (self.pos < self.src.len and self.src[self.pos] != '\n') self.pos += 1;
                    } else break;
                },
                else => break,
            }
        }
    }

    /// Return next token or null at EOF.
    fn next(self: *Tokenizer) ?[]const u8 {
        self.skipWs();
        if (self.pos >= self.src.len) return null;
        const c = self.src[self.pos];
        // Punctuation: single-char tokens
        switch (c) {
            '(', ')', ',', '.', ';', '=' => {
                const tok = self.src[self.pos .. self.pos + 1];
                self.pos += 1;
                return tok;
            },
            // Backtick / double-quote quoted identifier
            '`', '"' => {
                const quote = c;
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.src.len and self.src[self.pos] != quote) self.pos += 1;
                const tok = self.src[start..self.pos];
                if (self.pos < self.src.len) self.pos += 1; // closing quote
                return tok;
            },
            else => {
                const start = self.pos;
                while (self.pos < self.src.len) {
                    const d = self.src[self.pos];
                    switch (d) {
                        ' ', '\t', '\r', '\n', '(', ')', ',', '.', ';', '=' => break,
                        else => self.pos += 1,
                    }
                }
                return self.src[start..self.pos];
            },
        }
    }

    fn peekChar(self: *Tokenizer, c: u8) bool {
        var tmp = self.*;
        const tok = tmp.next() orelse return false;
        return tok.len == 1 and tok[0] == c;
    }

    fn peekKeyword(self: *Tokenizer, kw: []const u8) bool {
        var tmp = self.*;
        const tok = tmp.next() orelse return false;
        return asciiEql(tok, kw);
    }

    fn skip(self: *Tokenizer) void {
        _ = self.next();
    }
};

fn expectKeyword(tok: *Tokenizer, kw: []const u8) !void {
    const t = tok.next() orelse return error.UnexpectedEndOfInput;
    if (!asciiEql(t, kw)) return error.UnexpectedToken;
}

fn expectChar(tok: *Tokenizer, c: u8) !void {
    const t = tok.next() orelse return error.UnexpectedEndOfInput;
    if (t.len != 1 or t[0] != c) return error.UnexpectedToken;
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

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parse: basic CREATE TABLE" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE TABLE default.events (
        \\  id Int32,
        \\  name String,
        \\  ts DateTime
        \\) ENGINE = MergeTree ORDER BY id
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();

    try std.testing.expectEqualStrings("default", result.entry.db);
    try std.testing.expectEqualStrings("events", result.entry.name);
    try std.testing.expectEqualStrings("id", result.entry.pk.?);
    try std.testing.expectEqual(@as(usize, 3), result.entry.table.columns.len);
    try std.testing.expectEqualStrings("id", result.entry.table.columns[0].name);
    try std.testing.expectEqual(schema.ColumnType.int32, result.entry.table.columns[0].ty);
    try std.testing.expectEqualStrings("name", result.entry.table.columns[1].name);
    try std.testing.expectEqual(schema.ColumnType.text, result.entry.table.columns[1].ty);
    try std.testing.expectEqualStrings("ts", result.entry.table.columns[2].name);
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.entry.table.columns[2].ty);
}

test "parse: bare table name defaults to 'default' db" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("default", result.entry.db);
    try std.testing.expectEqualStrings("t", result.entry.name);
    try std.testing.expectEqual(schema.ColumnType.int64, result.entry.table.columns[0].ty);
}

test "parse: IF NOT EXISTS" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE IF NOT EXISTS db.t (x Int16) ENGINE = MergeTree ORDER BY x";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("db", result.entry.db);
    try std.testing.expectEqualStrings("t", result.entry.name);
}

test "parse: all phase-1 types" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE TABLE t (
        \\  a Int16, b Int32, c Int64,
        \\  d Date, e DateTime, f String
        \\) ENGINE = MergeTree ORDER BY a
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.int16,    result.entry.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.int32,    result.entry.table.columns[1].ty);
    try std.testing.expectEqual(schema.ColumnType.int64,    result.entry.table.columns[2].ty);
    try std.testing.expectEqual(schema.ColumnType.date,     result.entry.table.columns[3].ty);
    try std.testing.expectEqual(schema.ColumnType.timestamp,result.entry.table.columns[4].ty);
    try std.testing.expectEqual(schema.ColumnType.text,     result.entry.table.columns[5].ty);
}

test "parse: ORDER BY tuple" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (id Int32, ts DateTime) ENGINE = MergeTree ORDER BY (id, ts)";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("id", result.entry.pk.?);
}

test "parse: unsupported type returns error" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (x Float64) ENGINE = MergeTree";
    try std.testing.expectError(error.UnsupportedColumnType, parse(allocator, sql));
}
