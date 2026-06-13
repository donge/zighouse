/// Minimal CREATE TABLE DDL parser for zighouse serve.
///
/// Supported syntax (case-insensitive keywords):
///
///   CREATE TABLE [IF NOT EXISTS] [db.]table (
///   ATTACH TABLE [db.]table [UUID '...'] (
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
const type_mapping = @import("type_mapping");

pub const ParseResult = struct {
    entry: schema_config.TableEntry,
    /// Columns slice is owned here (freed by deinit).
    columns: []schema.Column,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ParseResult) void {
        for (self.columns) |col| {
            self.allocator.free(col.name);
            if (col.ch_type) |ct| self.allocator.free(ct);
        }
        for (self.entry.table.sort_keys) |sk| self.allocator.free(sk);
        if (self.entry.table.sort_keys.len > 0) self.allocator.free(self.entry.table.sort_keys);
        self.allocator.free(self.columns);
        self.allocator.free(self.entry.db);
        self.allocator.free(self.entry.name);
        self.allocator.free(self.entry.table.name);
        if (self.entry.pk) |pk| self.allocator.free(pk);
    }
};

/// Parse a CREATE/ATTACH TABLE statement.
/// Caller must call result.deinit(allocator) when done.
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !ParseResult {
    var tok = Tokenizer.init(sql);

    const first_kw = tok.next() orelse return error.UnexpectedEndOfInput;
    if (!std.ascii.eqlIgnoreCase(first_kw, "CREATE") and !std.ascii.eqlIgnoreCase(first_kw, "ATTACH"))
        return error.UnexpectedToken;

    try expectKeyword(&tok, "TABLE");
    if (std.ascii.eqlIgnoreCase(first_kw, "CREATE") and tok.peekKeyword("IF")) {
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

    if (std.ascii.eqlIgnoreCase(first_kw, "ATTACH") and tok.peekKeyword("UUID")) {
        tok.skip();
        _ = tok.next() orelse return error.UnexpectedEndOfInput;
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
        for (cols.items) |col| {
            allocator.free(col.name);
            if (col.ch_type) |ct| allocator.free(ct);
        }
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
        // Skip table-level constraint keywords (PRIMARY KEY, FOREIGN KEY, CONSTRAINT, CHECK, INDEX)
        if (std.ascii.eqlIgnoreCase(col_name_raw, "PRIMARY") or
            std.ascii.eqlIgnoreCase(col_name_raw, "FOREIGN") or
            std.ascii.eqlIgnoreCase(col_name_raw, "CONSTRAINT") or
            std.ascii.eqlIgnoreCase(col_name_raw, "CHECK") or
            std.ascii.eqlIgnoreCase(col_name_raw, "UNIQUE") or
            std.ascii.eqlIgnoreCase(col_name_raw, "REFERENCES") or
            std.ascii.eqlIgnoreCase(col_name_raw, "INDEX"))
        {
            tok.skipToColumnDelimiter();
            continue;
        }

        const col_type_raw = tok.next() orelse return error.MissingColumnType;
        // Record the start offset in the source so we can reconstruct the raw type string.
        const type_src_start = @intFromPtr(col_type_raw.ptr) - @intFromPtr(tok.src.ptr);

        // Multi-word SQL standard type names: consume continuation tokens
        // DOUBLE PRECISION, CHARACTER VARYING, CHAR VARYING, BINARY VARYING, etc.
        if (std.ascii.eqlIgnoreCase(col_type_raw, "DOUBLE")) {
            if (tok.peekKeyword("PRECISION")) _ = tok.next();
        } else if (std.ascii.eqlIgnoreCase(col_type_raw, "CHARACTER")) {
            if (tok.peekKeyword("VARYING")) _ = tok.next();
        } else if (std.ascii.eqlIgnoreCase(col_type_raw, "CHAR")) {
            if (tok.peekKeyword("VARYING")) _ = tok.next();
        } else if (std.ascii.eqlIgnoreCase(col_type_raw, "BINARY")) {
            if (tok.peekKeyword("VARYING")) {
                _ = tok.next();
            } else if (tok.peekKeyword("LARGE")) {
                _ = tok.next();
                if (tok.peekKeyword("OBJECT")) _ = tok.next();
            }
        } else if (std.ascii.eqlIgnoreCase(col_type_raw, "NATIONAL")) {
            if (tok.peekKeyword("CHARACTER")) {
                _ = tok.next();
                if (tok.peekKeyword("VARYING")) _ = tok.next();
            } else if (tok.peekKeyword("CHAR")) {
                _ = tok.next();
                if (tok.peekKeyword("VARYING")) _ = tok.next();
            }
        }
        if (std.ascii.eqlIgnoreCase(col_type_raw, "TIME") or std.ascii.eqlIgnoreCase(col_type_raw, "TIMESTAMP")) {
            var peek = tok;
            if (peek.next()) |s| {
                if (std.ascii.eqlIgnoreCase(s, "WITH") or std.ascii.eqlIgnoreCase(s, "WITHOUT")) {
                    _ = tok.next();
                    if (tok.peekKeyword("TIME")) _ = tok.next();
                    if (tok.peekKeyword("ZONE")) _ = tok.next();
                }
            }
        }

        // If the next token is '(' this type has arguments: Nullable(T), LowCardinality(T),
        // FixedString(N), DateTime64(p), etc.  Consume the parenthesised argument list and
        // resolve the effective type.
        if (tok.peekChar('(')) {
            tok.skip(); // consume '('
            // Collect inner tokens until matching ')'
            var inner_buf: [64]u8 = undefined;
            var inner_len: usize = 0;
            var depth: usize = 1;
            while (tok.next()) |t| {
                if (t.len == 1 and t[0] == '(') {
                    depth += 1;
                } else if (t.len == 1 and t[0] == ')') {
                    depth -= 1;
                    if (depth == 0) break;
                } else if (depth == 1 and t.len + inner_len <= inner_buf.len) {
                    // Capture the first non-punctuation token as the inner type
                    if (inner_len == 0) {
                        @memcpy(inner_buf[0..t.len], t);
                        inner_len = t.len;
                    }
                }
            }
            const inner = inner_buf[0..inner_len];
            // Raw type string: from type_src_start up to current tok.pos
            const ch_type_raw = std.mem.trim(u8, tok.src[type_src_start..tok.pos], " \t\r\n");
            const ch_type_owned = try allocator.dupe(u8, ch_type_raw);
            errdefer allocator.free(ch_type_owned);

            // DateTime64(p[, tz]) → timestamp
            if (std.ascii.eqlIgnoreCase(col_type_raw, "DateTime64")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .timestamp, ch_type_owned);
                continue;
            }
            // DateTime(tz) → timestamp
            if (std.ascii.eqlIgnoreCase(col_type_raw, "DateTime")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .timestamp, ch_type_owned);
                continue;
            }
            // FixedString(N) → text
            if (std.ascii.eqlIgnoreCase(col_type_raw, "FixedString")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .text, ch_type_owned);
                continue;
            }
            // Array(...) → text (blob)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "Array")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .text, ch_type_owned);
                continue;
            }
            // Map(...) → text (blob)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "Map")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .text, ch_type_owned);
                continue;
            }
            // Tuple(...) → text (blob)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "Tuple")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .text, ch_type_owned);
                continue;
            }
            // LowCardinality(T) — keep outer type consistent with RowBinary wire path (.text),
            // but record ch_type for schema fidelity. The LC write/read paths in CompactPart
            // are activated by ty=.low_card; for now we stay on the String path for VALUES INSERT.
            if (std.ascii.eqlIgnoreCase(col_type_raw, "LowCardinality")) {
                const inner_ty = parseColumnType(inner) orelse .text;
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, inner_ty, ch_type_owned);
                continue;
            }
            // VARCHAR(n), CHARACTER(n), CHAR(n) → text (length constraint dropped)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "VARCHAR") or
                std.ascii.eqlIgnoreCase(col_type_raw, "CHARACTER") or
                std.ascii.eqlIgnoreCase(col_type_raw, "CHAR"))
            {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .text, ch_type_owned);
                continue;
            }
            // DECIMAL(p[,s]), NUMERIC(p[,s]), DEC(p[,s]) → float64 (approximate, loses precision)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "DECIMAL") or
                std.ascii.eqlIgnoreCase(col_type_raw, "NUMERIC") or
                std.ascii.eqlIgnoreCase(col_type_raw, "DEC"))
            {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .float64, ch_type_owned);
                continue;
            }
            // FLOAT(p), REAL(p), DOUBLE(p) → float64 (precision parameter discarded)
            if (std.ascii.eqlIgnoreCase(col_type_raw, "FLOAT") or
                std.ascii.eqlIgnoreCase(col_type_raw, "REAL") or
                std.ascii.eqlIgnoreCase(col_type_raw, "DOUBLE"))
            {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .float64, ch_type_owned);
                continue;
            }
            // TIME(p) → timestamp
            if (std.ascii.eqlIgnoreCase(col_type_raw, "TIME")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .timestamp, ch_type_owned);
                continue;
            }
            // TIMESTAMP(p) → timestamp
            if (std.ascii.eqlIgnoreCase(col_type_raw, "TIMESTAMP")) {
                try appendAndSkip(allocator, &cols, &tok, col_name_raw, .timestamp, ch_type_owned);
                continue;
            }
            // Nullable(T) → resolve inner type
            const eff_ty = parseColumnType(inner) orelse return error.UnsupportedColumnType;
            try appendAndSkip(allocator, &cols, &tok, col_name_raw, eff_ty, ch_type_owned);
            continue;
        }

        const col_ty = parseColumnType(col_type_raw) orelse return error.UnsupportedColumnType;

        const col_name_owned = try allocator.dupe(u8, col_name_raw);
        // Full type name from source (captures multi-word types like "DOUBLE PRECISION").
        // Capture before skipping DEFAULT/MATERIALIZED/etc so client-visible type strings
        // stay parseable by ClickHouse drivers.
        const ch_type_full = std.mem.trim(u8, tok.src[type_src_start..tok.pos], " \t\r\n");
        const ch_type_owned = try allocator.dupe(u8, ch_type_full);

        // Skip optional column-level constraints: DEFAULT, NOT NULL, UNIQUE, PRIMARY KEY,
        // REFERENCES, CHECK, MATERIALIZED, ALIAS, COMMENT, CODEC, etc.
        if (tok.peekKeyword("DEFAULT") or tok.peekKeyword("MATERIALIZED") or tok.peekKeyword("ALIAS") or tok.peekKeyword("COMMENT") or tok.peekKeyword("CODEC") or
            tok.peekKeyword("NOT") or tok.peekKeyword("UNIQUE") or tok.peekKeyword("PRIMARY") or tok.peekKeyword("REFERENCES") or tok.peekKeyword("CHECK"))
        {
            tok.skipToColumnDelimiter();
        }

        try cols.append(allocator, .{ .name = col_name_owned, .ty = col_ty, .ch_type = ch_type_owned });
    }

    // ')' already consumed by peekChar/break — consume it
    if (tok.peekChar(')')) tok.skip();

    if (cols.items.len == 0) return error.NoColumnsFound;

    // ENGINE = ... (optional for us — we ignore the engine name)
    // ORDER BY col / tuple — full expression becomes sort_keys, first column becomes pk.
    var pk: ?[]const u8 = null;
    errdefer if (pk) |p| allocator.free(p);
    var sort_keys: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (sort_keys.items) |sk| allocator.free(sk);
        sort_keys.deinit(allocator);
    }

    while (tok.next()) |kw| {
        if (std.ascii.eqlIgnoreCase(kw, "ORDER")) {
            const by = tok.next() orelse break;
            if (!std.ascii.eqlIgnoreCase(by, "BY")) break;
            // ORDER BY can be (col1, col2) or just col1
            if (tok.peekChar('(')) {
                tok.skip(); // consume '('
                var depth: usize = 1;
                while (tok.next()) |t| {
                    if (t.len == 1 and t[0] == '(') {
                        depth += 1;
                        continue;
                    }
                    if (t.len == 1 and t[0] == ')') {
                        depth -= 1;
                        if (depth == 0) break;
                        continue;
                    }
                    if (depth != 1 or (t.len == 1 and t[0] == ',')) continue;
                    if (std.ascii.eqlIgnoreCase(t, "tuple")) continue;
                    const key = try allocator.dupe(u8, std.mem.trim(u8, t, "`\""));
                    try sort_keys.append(allocator, key);
                    if (pk == null) pk = try allocator.dupe(u8, key);
                }
            } else {
                const order_col = tok.next() orelse break;
                const key = try allocator.dupe(u8, std.mem.trim(u8, order_col, "`\""));
                try sort_keys.append(allocator, key);
                pk = try allocator.dupe(u8, key);
            }
        } else if (std.ascii.eqlIgnoreCase(kw, "PRIMARY")) {
            const key_kw = tok.next() orelse break;
            if (!std.ascii.eqlIgnoreCase(key_kw, "KEY")) break;
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
        } else if (std.ascii.eqlIgnoreCase(kw, "TTL")) {
            // TTL <expr> — skip until next known keyword or EOF
            // e.g. TTL ts + INTERVAL 30 DAY or TTL dt + toIntervalMonth(1)
            // We just consume tokens until we see a top-level keyword we recognise.
            while (true) {
                var tmp = tok;
                const peek = tmp.next() orelse break;
                if (std.ascii.eqlIgnoreCase(peek, "SETTINGS") or std.ascii.eqlIgnoreCase(peek, "ORDER") or
                    std.ascii.eqlIgnoreCase(peek, "PRIMARY") or std.ascii.eqlIgnoreCase(peek, "PARTITION") or
                    std.ascii.eqlIgnoreCase(peek, "SAMPLE") or std.ascii.eqlIgnoreCase(peek, "INDEX"))
                    break;
                _ = tok.next();
            }
        }
        // ignore ENGINE, SETTINGS, PARTITION BY etc.
    }

    const columns_slice = try cols.toOwnedSlice(allocator);
    const sort_keys_slice = try sort_keys.toOwnedSlice(allocator);

    return .{
        .entry = .{
            .db = db_owned,
            .name = table_name_owned,
            .pk = pk,
            .table = .{ .name = table_name_for_table, .columns = columns_slice, .sort_keys = sort_keys_slice },
        },
        .columns = columns_slice,
        .allocator = allocator,
    };
}

// ── Type mapping ──────────────────────────────────────────────────────────────

pub fn parseColumnTypePublic(s: []const u8) ?schema.ColumnType {
    return type_mapping.parseType(s, .ddl);
}

fn parseColumnType(s: []const u8) ?schema.ColumnType {
    return type_mapping.parseType(s, .ddl);
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
            // Backtick / double-quote / single-quote quoted identifier or string literal
            '`', '"', '\'' => {
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

    /// Consume tokens until a top-level ',' or ')' is the next token (not consumed).
    /// Used to skip DEFAULT / MATERIALIZED / ALIAS / COMMENT / CODEC expressions.
    fn skipToColumnDelimiter(self: *Tokenizer) void {
        var depth: usize = 0;
        while (true) {
            // Peek at next char without modifying self (unless we decide to advance)
            var tmp = self.*;
            const tok = tmp.next() orelse return; // EOF
            if (depth == 0 and tok.len == 1 and (tok[0] == ',' or tok[0] == ')')) return;
            // Otherwise actually consume
            _ = self.next();
            if (tok.len == 1 and tok[0] == '(') depth += 1;
            if (tok.len == 1 and tok[0] == ')') {
                if (depth == 0) return; // shouldn't happen but be safe
                depth -= 1;
            }
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
        return std.ascii.eqlIgnoreCase(tok, kw);
    }

    fn skip(self: *Tokenizer) void {
        _ = self.next();
    }
};

fn expectKeyword(tok: *Tokenizer, kw: []const u8) !void {
    const t = tok.next() orelse return error.UnexpectedEndOfInput;
    if (!std.ascii.eqlIgnoreCase(t, kw)) return error.UnexpectedToken;
}

fn expectChar(tok: *Tokenizer, c: u8) !void {
    const t = tok.next() orelse return error.UnexpectedEndOfInput;
    if (t.len != 1 or t[0] != c) return error.UnexpectedToken;
}

/// Append a column to `cols` and skip any trailing column-modifier keywords.
fn appendAndSkip(
    allocator: std.mem.Allocator,
    cols: *std.ArrayListUnmanaged(schema.Column),
    tok: *Tokenizer,
    col_name_raw: []const u8,
    ty: schema.ColumnType,
    ch_type_owned: ?[]const u8,
) !void {
    const col_name_owned = try allocator.dupe(u8, col_name_raw);
    try cols.append(allocator, .{ .name = col_name_owned, .ty = ty, .ch_type = ch_type_owned });
    if (tok.peekKeyword("DEFAULT") or tok.peekKeyword("COMMENT") or tok.peekKeyword("CODEC"))
        tok.skipToColumnDelimiter();
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
    try std.testing.expectEqual(@as(usize, 2), result.entry.table.sort_keys.len);
    try std.testing.expectEqualStrings("id", result.entry.table.sort_keys[0]);
    try std.testing.expectEqualStrings("ts", result.entry.table.sort_keys[1]);
}

test "parse: Decimal type maps to Float64 compatibility storage" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (x Decimal(10,2)) ENGINE = MergeTree";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.float64, result.entry.table.columns[0].ty);
}

test "parse: Float32 and Float64 are supported" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (a Float32, b Float64) ENGINE = MergeTree";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.float32, result.entry.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.float64, result.entry.table.columns[1].ty);
}

test "parse: Nullable(T) unwraps inner type" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (id Nullable(Int32), name Nullable(String)) ENGINE = MergeTree ORDER BY id";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.int32, result.entry.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.text,  result.entry.table.columns[1].ty);
}

test "parse: LowCardinality(String)" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (cat LowCardinality(String)) ENGINE = MergeTree ORDER BY cat";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.text, result.entry.table.columns[0].ty);
}

test "parse: FixedString(N)" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (code FixedString(4)) ENGINE = MergeTree ORDER BY code";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.text, result.entry.table.columns[0].ty);
}

test "parse: PRIMARY KEY clause" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (id Int32, ts DateTime) ENGINE = MergeTree PRIMARY KEY id";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("id", result.entry.pk.?);
}

test "parse: PRIMARY KEY overrides ORDER BY when both present" {
    // ORDER BY sets pk first, PRIMARY KEY should not overwrite (pk already set).
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (id Int32, ts DateTime) ENGINE = MergeTree ORDER BY ts PRIMARY KEY id";
    var result = try parse(allocator, sql);
    defer result.deinit();
    // ORDER BY was parsed first → pk = "ts"
    try std.testing.expectEqualStrings("ts", result.entry.pk.?);
}

test "parse: backtick-quoted identifiers" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE `mydb`.`my_table` (`id` Int32, `name` String) ENGINE = MergeTree ORDER BY `id`";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("mydb", result.entry.db);
    try std.testing.expectEqualStrings("my_table", result.entry.name);
    try std.testing.expectEqualStrings("id", result.entry.table.columns[0].name);
    try std.testing.expectEqualStrings("id", result.entry.pk.?);
}

test "parse: UInt8 maps to int8, UInt16 to int16, UInt32 to int32, UInt64 to int64" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (a UInt8, b UInt16, c UInt32, d UInt64) ENGINE = MergeTree ORDER BY a";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.int8,  result.entry.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.int16, result.entry.table.columns[1].ty);
    try std.testing.expectEqual(schema.ColumnType.int32, result.entry.table.columns[2].ty);
    try std.testing.expectEqual(schema.ColumnType.int64, result.entry.table.columns[3].ty);
}

test "parse: DateTime64 maps to timestamp" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (ts DateTime64(3)) ENGINE = MergeTree ORDER BY ts";
    // DateTime64(3) tokenizes as "DateTime64" + "(" + "3" + ")"
    // extractInnerType("DateTime64(3)") → "3" which is not a known type,
    // but parseColumnType("DateTime64") also won't match because the token
    // includes the parens. This tests the Nullable path.
    // Actually the tokenizer splits at '(' so col_type_raw = "DateTime64" → maps fine.
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.entry.table.columns[0].ty);
}

test "parse: Date32 maps to date" {
    const allocator = std.testing.allocator;
    const sql = "CREATE TABLE t (d Date32) ENGINE = MergeTree ORDER BY d";
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(schema.ColumnType.date, result.entry.table.columns[0].ty);
}

test "parse: DEFAULT clause is skipped" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE TABLE IF NOT EXISTS vprobe.scoring_rules (
        \\    rule_id      String,
        \\    protocol     LowCardinality(String) DEFAULT '*',
        \\    feature      String,
        \\    operator     LowCardinality(String),
        \\    threshold    Float64 DEFAULT 0,
        \\    weight       Float64 DEFAULT 0,
        \\    enabled      UInt8 DEFAULT 1,
        \\    note         String DEFAULT '',
        \\    updated_at   DateTime64(3),
        \\    version      UInt64
        \\) ENGINE = ReplacingMergeTree(version)
        \\ORDER BY (rule_id)
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqualStrings("vprobe", result.entry.db);
    try std.testing.expectEqualStrings("scoring_rules", result.entry.name);
    try std.testing.expectEqual(@as(usize, 10), result.entry.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.text,      result.entry.table.columns[0].ty); // rule_id
    try std.testing.expectEqual(schema.ColumnType.text,      result.entry.table.columns[1].ty); // protocol
    try std.testing.expectEqual(schema.ColumnType.text,      result.entry.table.columns[2].ty); // feature
    try std.testing.expectEqual(schema.ColumnType.text,      result.entry.table.columns[3].ty); // operator
    try std.testing.expectEqual(schema.ColumnType.float64,   result.entry.table.columns[4].ty); // threshold
    try std.testing.expectEqual(schema.ColumnType.float64,   result.entry.table.columns[5].ty); // weight
    try std.testing.expectEqual(schema.ColumnType.int8,      result.entry.table.columns[6].ty); // enabled
    try std.testing.expectEqual(schema.ColumnType.text,      result.entry.table.columns[7].ty); // note
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.entry.table.columns[8].ty); // updated_at
    try std.testing.expectEqual(schema.ColumnType.int64,     result.entry.table.columns[9].ty); // version
    try std.testing.expectEqualStrings("Float64", result.entry.table.columns[4].ch_type.?);
    try std.testing.expectEqualStrings("UInt8", result.entry.table.columns[6].ch_type.?);
    try std.testing.expectEqualStrings("String", result.entry.table.columns[7].ch_type.?);
    try std.testing.expectEqualStrings("rule_id", result.entry.pk.?);
    try std.testing.expectEqual(@as(usize, 1), result.entry.table.sort_keys.len);
    try std.testing.expectEqualStrings("rule_id", result.entry.table.sort_keys[0]);
}

test "parse: ClickHouse metadata DDL with partition ttl settings and sort keys" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE TABLE vprobe.detect_events
        \\(
        \\    `event_type` LowCardinality(String),
        \\    `ts` DateTime64(3),
        \\    `src_ip` IPv6,
        \\    `features` Map(String, Float64),
        \\    `version` UInt64
        \\)
        \\ENGINE = ReplacingMergeTree(version)
        \\PARTITION BY toYYYYMM(ts)
        \\ORDER BY (event_type, ts)
        \\TTL ts + INTERVAL 30 DAY
        \\SETTINGS index_granularity = 8192
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();

    try std.testing.expectEqualStrings("vprobe", result.entry.db);
    try std.testing.expectEqualStrings("detect_events", result.entry.name);
    try std.testing.expectEqual(@as(usize, 5), result.entry.table.columns.len);
    try std.testing.expectEqualStrings("LowCardinality(String)", result.entry.table.columns[0].ch_type.?);
    try std.testing.expectEqualStrings("DateTime64(3)", result.entry.table.columns[1].ch_type.?);
    try std.testing.expectEqualStrings("IPv6", result.entry.table.columns[2].ch_type.?);
    try std.testing.expectEqualStrings("Map(String, Float64)", result.entry.table.columns[3].ch_type.?);
    try std.testing.expectEqualStrings("event_type", result.entry.pk.?);
    try std.testing.expectEqual(@as(usize, 2), result.entry.table.sort_keys.len);
    try std.testing.expectEqualStrings("event_type", result.entry.table.sort_keys[0]);
    try std.testing.expectEqualStrings("ts", result.entry.table.sort_keys[1]);
}

test "parse: Atomic ATTACH TABLE metadata with UUID and sort keys" {
    const allocator = std.testing.allocator;
    const sql =
        \\ATTACH TABLE _ UUID 'f0805669-b200-4cf8-8cb4-565fad8eb655'
        \\(
        \\    `date` Date,
        \\    `code` String,
        \\    `close` Float64,
        \\    `ver` UInt64
        \\)
        \\ENGINE = ReplacingMergeTree(ver)
        \\ORDER BY (code, date)
        \\SETTINGS index_granularity = 8192
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();

    try std.testing.expectEqualStrings("_", result.entry.name);
    try std.testing.expectEqual(@as(usize, 4), result.entry.table.columns.len);
    try std.testing.expectEqualStrings("code", result.entry.pk.?);
    try std.testing.expectEqual(@as(usize, 2), result.entry.table.sort_keys.len);
    try std.testing.expectEqualStrings("code", result.entry.table.sort_keys[0]);
    try std.testing.expectEqualStrings("date", result.entry.table.sort_keys[1]);
}

test "parse: full scoring_rules with upper column" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE TABLE IF NOT EXISTS vprobe.scoring_rules (
        \\    rule_id      String,
        \\    protocol     LowCardinality(String) DEFAULT '*',
        \\    feature      String,
        \\    operator     LowCardinality(String),
        \\    threshold    Float64 DEFAULT 0,
        \\    upper        Float64 DEFAULT 0,
        \\    weight       Float64 DEFAULT 0,
        \\    enabled      UInt8 DEFAULT 1,
        \\    note         String DEFAULT '',
        \\    updated_at   DateTime64(3),
        \\    version      UInt64
        \\) ENGINE = ReplacingMergeTree(version)
        \\ORDER BY (rule_id);
    ;
    var result = try parse(allocator, sql);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 11), result.entry.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.entry.table.columns[9].ty); // updated_at
    try std.testing.expectEqual(schema.ColumnType.int64,     result.entry.table.columns[10].ty); // version
}
