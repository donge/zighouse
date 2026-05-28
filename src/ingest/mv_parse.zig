/// Materialized view DDL parser for zighouse.
///
/// Supported syntax (case-insensitive keywords):
///
///   CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]mv_name
///   TO [db.]target_table
///   AS SELECT ...
///
/// The SELECT SQL is stored verbatim (with the original FROM source_table).
/// The source table is extracted from the FROM clause.

const std = @import("std");

pub const MatViewEntry = struct {
    /// Name of the materialized view itself.
    mv_name: []const u8,
    /// Database the MV belongs to (default "default").
    db: []const u8,
    /// Source table database.
    source_db: []const u8,
    /// Source table name (extracted from SELECT … FROM <source>).
    source_table: []const u8,
    /// Target table database (from TO clause).
    target_db: []const u8,
    /// Target table name (from TO clause).
    target_table: []const u8,
    /// The SELECT SQL to execute (FROM references source_table).
    select_sql: []const u8,
    /// The full original CREATE … SQL (used for persistence).
    raw_sql: []const u8,

    allocator: std.mem.Allocator,

    pub fn deinit(self: *MatViewEntry) void {
        self.allocator.free(self.mv_name);
        self.allocator.free(self.db);
        self.allocator.free(self.source_db);
        self.allocator.free(self.source_table);
        self.allocator.free(self.target_db);
        self.allocator.free(self.target_table);
        self.allocator.free(self.select_sql);
        self.allocator.free(self.raw_sql);
    }
};

/// Parse a CREATE MATERIALIZED VIEW statement.
/// Caller owns the returned MatViewEntry and must call entry.deinit().
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) !MatViewEntry {
    var tok = Tokenizer.init(sql);

    try expectKeyword(&tok, "CREATE");
    try expectKeyword(&tok, "MATERIALIZED");
    try expectKeyword(&tok, "VIEW");

    // IF NOT EXISTS (optional — we just skip it, callers enforce idempotence)
    if (tok.peekKeyword("IF")) {
        tok.skip(); // IF
        try expectKeyword(&tok, "NOT");
        try expectKeyword(&tok, "EXISTS");
    }

    // [db.]mv_name
    const first = tok.next() orelse return error.MissingViewName;
    var mv_db: []const u8 = "default";
    var mv_name: []const u8 = first;
    if (tok.peekChar('.')) {
        tok.skip(); // consume '.'
        mv_db = first;
        mv_name = tok.next() orelse return error.MissingViewName;
    }
    const mv_name_owned = try allocator.dupe(u8, mv_name);
    errdefer allocator.free(mv_name_owned);
    const mv_db_owned = try allocator.dupe(u8, mv_db);
    errdefer allocator.free(mv_db_owned);

    // TO [db.]target_table
    try expectKeyword(&tok, "TO");
    const t_first = tok.next() orelse return error.MissingToTable;
    var tgt_db: []const u8 = "default";
    var tgt_table: []const u8 = t_first;
    if (tok.peekChar('.')) {
        tok.skip();
        tgt_db = t_first;
        tgt_table = tok.next() orelse return error.MissingToTable;
    }
    const tgt_db_owned = try allocator.dupe(u8, tgt_db);
    errdefer allocator.free(tgt_db_owned);
    const tgt_table_owned = try allocator.dupe(u8, tgt_table);
    errdefer allocator.free(tgt_table_owned);

    // AS
    try expectKeyword(&tok, "AS");

    // Everything from here on is the SELECT SQL.
    // tok.pos is just after the 'AS' token.
    tok.skipWsPublic();
    const select_start = tok.pos;
    // Trim trailing whitespace and semicolons from SELECT SQL.
    var sel_end = sql[select_start..].len;
    while (sel_end > 0) {
        const ch = sql[select_start + sel_end - 1];
        if (ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n' or ch == ';') {
            sel_end -= 1;
        } else break;
    }
    const select_sql_raw = sql[select_start .. select_start + sel_end];
    if (select_sql_raw.len == 0) return error.MissingSelectSql;
    const select_sql_owned = try allocator.dupe(u8, select_sql_raw);
    errdefer allocator.free(select_sql_owned);

    // Extract source table from SELECT … FROM <source>
    // Simple approach: find "FROM" keyword in select_sql, grab the next identifier.
    var src_db: []const u8 = "default";
    var src_table: []const u8 = "";
    if (std.ascii.indexOfIgnoreCase(select_sql_raw, "FROM ")) |from_pos| {
        const after_from = std.mem.trim(u8, select_sql_raw[from_pos + 5 ..], " \t\r\n");
        var src_tok = Tokenizer.init(after_from);
        const s_first = src_tok.next() orelse "";
        if (s_first.len > 0) {
            if (src_tok.peekChar('.')) {
                src_tok.skip();
                src_db = s_first;
                src_table = src_tok.next() orelse s_first;
            } else {
                src_table = s_first;
            }
        }
    }
    if (src_table.len == 0) return error.CannotExtractSourceTable;
    const src_db_owned = try allocator.dupe(u8, src_db);
    errdefer allocator.free(src_db_owned);
    const src_table_owned = try allocator.dupe(u8, src_table);
    errdefer allocator.free(src_table_owned);

    // Trim trailing whitespace/semicolons from raw SQL.
    var raw_end = sql.len;
    while (raw_end > 0) {
        const ch = sql[raw_end - 1];
        if (ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n' or ch == ';') {
            raw_end -= 1;
        } else break;
    }
    const raw_sql_owned = try allocator.dupe(u8, sql[0..raw_end]);
    errdefer allocator.free(raw_sql_owned);

    return .{
        .mv_name      = mv_name_owned,
        .db           = mv_db_owned,
        .source_db    = src_db_owned,
        .source_table = src_table_owned,
        .target_db    = tgt_db_owned,
        .target_table = tgt_table_owned,
        .select_sql   = select_sql_owned,
        .raw_sql      = raw_sql_owned,
        .allocator    = allocator,
    };
}

// ── Tokenizer (same rules as ddl_parser.zig) ─────────────────────────────────

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
                    if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '-') {
                        while (self.pos < self.src.len and self.src[self.pos] != '\n') self.pos += 1;
                    } else break;
                },
                else => break,
            }
        }
    }

    // Public alias used by parse() to advance tok.pos to the SELECT start.
    fn skipWsPublic(self: *Tokenizer) void {
        self.skipWs();
    }

    fn next(self: *Tokenizer) ?[]const u8 {
        self.skipWs();
        if (self.pos >= self.src.len) return null;
        const c = self.src[self.pos];
        switch (c) {
            '(', ')', ',', '.', ';', '=' => {
                const t = self.src[self.pos .. self.pos + 1];
                self.pos += 1;
                return t;
            },
            '`', '"' => {
                const quote = c;
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.src.len and self.src[self.pos] != quote) self.pos += 1;
                const t = self.src[start..self.pos];
                if (self.pos < self.src.len) self.pos += 1;
                return t;
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
        const t = tmp.next() orelse return false;
        return t.len == 1 and t[0] == c;
    }

    fn peekKeyword(self: *Tokenizer, kw: []const u8) bool {
        var tmp = self.*;
        const t = tmp.next() orelse return false;
        return asciiEql(t, kw);
    }

    fn skip(self: *Tokenizer) void {
        _ = self.next();
    }
};

fn expectKeyword(tok: *Tokenizer, kw: []const u8) !void {
    const t = tok.next() orelse return error.UnexpectedEndOfInput;
    if (!asciiEql(t, kw)) return error.UnexpectedToken;
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

test "parse: basic materialized view" {
    const allocator = std.testing.allocator;
    const sql =
        \\CREATE MATERIALIZED VIEW default.zh_mv TO default.zh_mv_dst
        \\AS SELECT id, val * 2 AS val_doubled FROM zh_mv_src
    ;
    var entry = try parse(allocator, sql);
    defer entry.deinit();

    try std.testing.expectEqualStrings("zh_mv", entry.mv_name);
    try std.testing.expectEqualStrings("default", entry.db);
    try std.testing.expectEqualStrings("zh_mv_dst", entry.target_table);
    try std.testing.expectEqualStrings("default", entry.target_db);
    try std.testing.expectEqualStrings("zh_mv_src", entry.source_table);
    try std.testing.expectEqualStrings("default", entry.source_db);
}

test "parse: bare mv name, no db prefix" {
    const allocator = std.testing.allocator;
    const sql = "CREATE MATERIALIZED VIEW my_mv TO dst_tbl AS SELECT x FROM src_tbl";
    var entry = try parse(allocator, sql);
    defer entry.deinit();

    try std.testing.expectEqualStrings("my_mv", entry.mv_name);
    try std.testing.expectEqualStrings("default", entry.db);
    try std.testing.expectEqualStrings("dst_tbl", entry.target_table);
    try std.testing.expectEqualStrings("src_tbl", entry.source_table);
}

test "parse: IF NOT EXISTS" {
    const allocator = std.testing.allocator;
    const sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 TO t2 AS SELECT a FROM t1";
    var entry = try parse(allocator, sql);
    defer entry.deinit();
    try std.testing.expectEqualStrings("mv1", entry.mv_name);
    try std.testing.expectEqualStrings("t1", entry.source_table);
    try std.testing.expectEqualStrings("t2", entry.target_table);
}
