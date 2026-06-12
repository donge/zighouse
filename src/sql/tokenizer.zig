/// SQL tokenizer for the zighouse native SQL parser.
///
/// Produces typed tokens from a SQL string. The tokenizer is zero-copy:
/// all string slices point into the original input.
///
/// Recognised token kinds:
///   - Keywords (case-insensitive): SELECT, FROM, WHERE, GROUP, HAVING,
///     ORDER, BY, LIMIT, OFFSET, UNION, ALL, DISTINCT, AS, WITH, CASE,
///     WHEN, THEN, ELSE, END, IN, NOT, LIKE, BETWEEN, AND, OR, IS, NULL,
///     TRUE, FALSE, IF, EXISTS, OVER, PARTITION, ROWS, RANGE, UNBOUNDED,
///     PRECEDING, FOLLOWING, CURRENT, ROW, INTERVAL
///   - Identifiers / unquoted names
///   - String literals (single-quoted, with '' escape)
///   - Integer literals
///   - Float literals
///   - Operators: = <> != < <= > >= + - * / % || ->
///   - Punctuation: ( ) , . ; [ ] :
///   - EOF

const std = @import("std");

pub const TokenKind = enum {
    // Value tokens
    ident,        // unquoted identifier or keyword
    keyword,      // recognised SQL keyword (still carries the text)
    string,       // single-quoted string literal; value has quotes stripped
    integer,      // decimal integer literal
    float,        // decimal float literal
    // Operators
    eq,           // =
    neq,          // <> or !=
    lt,           // <
    lte,          // <=
    gt,           // >
    gte,          // >=
    plus,         // +
    minus,        // -
    star,         // *
    slash,        // /
    percent,      // %
    concat,       // ||
    arrow,        // ->
    // Punctuation
    lparen,       // (
    rparen,       // )
    comma,        // ,
    dot,          // .
    semicolon,    // ;
    lbracket,     // [
    rbracket,     // ]
    colon,        // :
    // Special
    eof,
};

pub const Token = struct {
    kind: TokenKind,
    /// Slice into the original SQL string.
    text: []const u8,
};

// Keywords recognised by the tokenizer (lower-cased for comparison).
const KEYWORDS = [_][]const u8{
    "select", "from", "where", "group", "having", "order", "by",
    "limit", "offset", "union", "all", "distinct", "as", "with",
    "case", "when", "then", "else", "end", "in", "not", "like",
    "between", "and", "or", "is", "null", "true", "false",
    "if", "exists", "over", "partition", "rows", "range",
    "unbounded", "preceding", "following", "current", "row",
    "interval", "join", "inner", "left", "outer", "right", "cross",
    "on", "using", "create", "table", "insert", "into", "values",
    "update", "set", "delete", "drop", "alter", "add", "column",
    "primary", "key", "engine", "mergetree", "asc", "desc",
    "prewhere",
    "substring", "position", "trim", "leading", "trailing", "both",
    "escape", "for",
};

fn isKeyword(s: []const u8) bool {
    var buf: [64]u8 = undefined;
    if (s.len > buf.len) return false;
    const lower = std.ascii.lowerString(buf[0..s.len], s);
    for (KEYWORDS) |kw| {
        if (std.mem.eql(u8, lower, kw)) return true;
    }
    return false;
}

pub const Tokenizer = struct {
    src: []const u8,
    pos: usize,
    /// Peek buffer: one token of lookahead.
    peeked: ?Token,

    pub fn init(src: []const u8) Tokenizer {
        return .{ .src = src, .pos = 0, .peeked = null };
    }

    /// Skip whitespace and SQL comments (-- line comments, /* block comments */).
    fn skipWs(self: *Tokenizer) void {
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                ' ', '\t', '\r', '\n' => self.pos += 1,
                '-' => {
                    if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '-') {
                        // Line comment
                        while (self.pos < self.src.len and self.src[self.pos] != '\n')
                            self.pos += 1;
                    } else break;
                },
                '/' => {
                    if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '*') {
                        // Block comment
                        self.pos += 2;
                        while (self.pos + 1 < self.src.len) {
                            if (self.src[self.pos] == '*' and self.src[self.pos + 1] == '/') {
                                self.pos += 2;
                                break;
                            }
                            self.pos += 1;
                        }
                    } else break;
                },
                else => break,
            }
        }
    }

    /// Read the next token from the stream without consuming it.
    pub fn peek(self: *Tokenizer) Token {
        if (self.peeked) |t| return t;
        const t = self.readNext();
        self.peeked = t;
        return t;
    }

    /// Consume and return the next token.
    pub fn next(self: *Tokenizer) Token {
        if (self.peeked) |t| {
            self.peeked = null;
            return t;
        }
        return self.readNext();
    }

    /// Consume if the next token matches `kind`. Returns true if consumed.
    pub fn eatIf(self: *Tokenizer, kind: TokenKind) bool {
        if (self.peek().kind == kind) {
            _ = self.next();
            return true;
        }
        return false;
    }

    /// Consume if the next token is a keyword matching `kw` (case-insensitive).
    pub fn eatKeyword(self: *Tokenizer, kw: []const u8) bool {
        const t = self.peek();
        if (t.kind != .keyword and t.kind != .ident) return false;
        if (!std.ascii.eqlIgnoreCase(t.text, kw)) return false;
        _ = self.next();
        return true;
    }

    /// Return true if next token is a keyword equal to `kw` (case-insensitive), without consuming.
    pub fn peekKeyword(self: *Tokenizer, kw: []const u8) bool {
        const t = self.peek();
        if (t.kind != .keyword and t.kind != .ident) return false;
        return std.ascii.eqlIgnoreCase(t.text, kw);
    }

    /// Return true if next token is an ident/keyword with text equal to `name` (case-insensitive).
    pub fn peekIdent(self: *Tokenizer, name: []const u8) bool {
        const t = self.peek();
        if (t.kind != .keyword and t.kind != .ident) return false;
        return std.ascii.eqlIgnoreCase(t.text, name);
    }

    fn readNext(self: *Tokenizer) Token {
        self.skipWs();
        if (self.pos >= self.src.len) return .{ .kind = .eof, .text = "" };

        const c = self.src[self.pos];

        // Single-quoted string literal
        if (c == '\'') {
            return self.readString();
        }

        // Backtick or double-quote quoted identifier
        if (c == '`' or c == '"') {
            return self.readQuotedIdent(c);
        }

        // Numeric literal
        if (c >= '0' and c <= '9') {
            return self.readNumber();
        }

        // Identifier or keyword (starts with letter or _)
        if ((c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_') {
            return self.readIdent();
        }

        // Operators and punctuation
        self.pos += 1;
        switch (c) {
            '(' => return .{ .kind = .lparen,    .text = self.src[self.pos-1..self.pos] },
            ')' => return .{ .kind = .rparen,    .text = self.src[self.pos-1..self.pos] },
            ',' => return .{ .kind = .comma,     .text = self.src[self.pos-1..self.pos] },
            '.' => return .{ .kind = .dot,       .text = self.src[self.pos-1..self.pos] },
            ';' => return .{ .kind = .semicolon, .text = self.src[self.pos-1..self.pos] },
            '[' => return .{ .kind = .lbracket,  .text = self.src[self.pos-1..self.pos] },
            ']' => return .{ .kind = .rbracket,  .text = self.src[self.pos-1..self.pos] },
            ':' => return .{ .kind = .colon,     .text = self.src[self.pos-1..self.pos] },
            '+' => return .{ .kind = .plus,      .text = self.src[self.pos-1..self.pos] },
            '*' => return .{ .kind = .star,      .text = self.src[self.pos-1..self.pos] },
            '%' => return .{ .kind = .percent,   .text = self.src[self.pos-1..self.pos] },
            '/' => return .{ .kind = .slash,     .text = self.src[self.pos-1..self.pos] },
            '-' => {
                // Could be -> arrow
                if (self.pos < self.src.len and self.src[self.pos] == '>') {
                    self.pos += 1;
                    return .{ .kind = .arrow, .text = self.src[self.pos-2..self.pos] };
                }
                return .{ .kind = .minus, .text = self.src[self.pos-1..self.pos] };
            },
            '=' => return .{ .kind = .eq,  .text = self.src[self.pos-1..self.pos] },
            '!' => {
                if (self.pos < self.src.len and self.src[self.pos] == '=') {
                    self.pos += 1;
                    return .{ .kind = .neq, .text = self.src[self.pos-2..self.pos] };
                }
                // '!' alone: treat as ident so we don't silently drop it
                return .{ .kind = .ident, .text = self.src[self.pos-1..self.pos] };
            },
            '<' => {
                if (self.pos < self.src.len and self.src[self.pos] == '=') {
                    self.pos += 1;
                    return .{ .kind = .lte, .text = self.src[self.pos-2..self.pos] };
                }
                if (self.pos < self.src.len and self.src[self.pos] == '>') {
                    self.pos += 1;
                    return .{ .kind = .neq, .text = self.src[self.pos-2..self.pos] };
                }
                return .{ .kind = .lt,  .text = self.src[self.pos-1..self.pos] };
            },
            '>' => {
                if (self.pos < self.src.len and self.src[self.pos] == '=') {
                    self.pos += 1;
                    return .{ .kind = .gte, .text = self.src[self.pos-2..self.pos] };
                }
                return .{ .kind = .gt,  .text = self.src[self.pos-1..self.pos] };
            },
            '|' => {
                if (self.pos < self.src.len and self.src[self.pos] == '|') {
                    self.pos += 1;
                    return .{ .kind = .concat, .text = self.src[self.pos-2..self.pos] };
                }
                // Single '|': treat as ident
                return .{ .kind = .ident, .text = self.src[self.pos-1..self.pos] };
            },
            else => {
                // Unknown character — return as ident so the parser can produce a better error
                return .{ .kind = .ident, .text = self.src[self.pos-1..self.pos] };
            },
        }
    }

    fn readString(self: *Tokenizer) Token {
        const start = self.pos; // points at opening '
        self.pos += 1; // skip opening '
        while (self.pos < self.src.len) {
            if (self.src[self.pos] == '\'') {
                self.pos += 1;
                // '' escape: two consecutive quotes → literal single quote
                if (self.pos < self.src.len and self.src[self.pos] == '\'') {
                    self.pos += 1;
                    continue;
                }
                break; // closing quote
            }
            // Backslash escape (MySQL-style, sometimes present after ch_compat rewrite)
            if (self.src[self.pos] == '\\' and self.pos + 1 < self.src.len) {
                self.pos += 2;
                continue;
            }
            self.pos += 1;
        }
        // text includes surrounding quotes so the parser can distinguish '' content
        return .{ .kind = .string, .text = self.src[start..self.pos] };
    }

    fn readQuotedIdent(self: *Tokenizer, quote: u8) Token {
        self.pos += 1; // skip opening quote
        const start = self.pos;
        while (self.pos < self.src.len and self.src[self.pos] != quote) self.pos += 1;
        const text = self.src[start..self.pos];
        if (self.pos < self.src.len) self.pos += 1; // closing quote
        return .{ .kind = .ident, .text = text };
    }

    fn readNumber(self: *Tokenizer) Token {
        const start = self.pos;
        var is_float = false;
        while (self.pos < self.src.len) {
            const d = self.src[self.pos];
            if (d >= '0' and d <= '9') {
                self.pos += 1;
            } else if (d == '.' and !is_float) {
                // Look ahead: if next char after '.' is a digit, it's a float
                if (self.pos + 1 < self.src.len and self.src[self.pos + 1] >= '0' and self.src[self.pos + 1] <= '9') {
                    is_float = true;
                    self.pos += 1;
                } else {
                    break;
                }
            } else if ((d == 'e' or d == 'E') and !is_float) {
                is_float = true;
                self.pos += 1;
                if (self.pos < self.src.len and (self.src[self.pos] == '+' or self.src[self.pos] == '-'))
                    self.pos += 1;
            } else {
                break;
            }
        }
        const kind: TokenKind = if (is_float) .float else .integer;
        return .{ .kind = kind, .text = self.src[start..self.pos] };
    }

    fn readIdent(self: *Tokenizer) Token {
        const start = self.pos;
        while (self.pos < self.src.len) {
            const d = self.src[self.pos];
            if ((d >= 'a' and d <= 'z') or (d >= 'A' and d <= 'Z') or
                (d >= '0' and d <= '9') or d == '_') {
                self.pos += 1;
            } else break;
        }
        const text = self.src[start..self.pos];
        const kind: TokenKind = if (isKeyword(text)) .keyword else .ident;
        return .{ .kind = kind, .text = text };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "tokenizer: basic SELECT" {
    var tok = Tokenizer.init("SELECT a, b FROM t WHERE x = 1");
    const expected_kinds = [_]TokenKind{
        .keyword, .ident, .comma, .ident, .keyword, .ident, .keyword, .ident, .eq, .integer, .eof,
    };
    const expected_texts = [_][]const u8{
        "SELECT", "a", ",", "b", "FROM", "t", "WHERE", "x", "=", "1", "",
    };
    for (expected_kinds, 0..) |kind, i| {
        const t = tok.next();
        try std.testing.expectEqual(kind, t.kind);
        try std.testing.expectEqualStrings(expected_texts[i], t.text);
    }
}

test "tokenizer: string with escape" {
    var tok = Tokenizer.init("'hello''world'");
    const t = tok.next();
    try std.testing.expectEqual(TokenKind.string, t.kind);
    try std.testing.expectEqualStrings("'hello''world'", t.text);
}

test "tokenizer: operators" {
    var tok = Tokenizer.init("<= >= <> != -> ||");
    try std.testing.expectEqual(TokenKind.lte,    tok.next().kind);
    try std.testing.expectEqual(TokenKind.gte,    tok.next().kind);
    try std.testing.expectEqual(TokenKind.neq,    tok.next().kind);
    try std.testing.expectEqual(TokenKind.neq,    tok.next().kind);
    try std.testing.expectEqual(TokenKind.arrow,  tok.next().kind);
    try std.testing.expectEqual(TokenKind.concat, tok.next().kind);
    try std.testing.expectEqual(TokenKind.eof,    tok.next().kind);
}

test "tokenizer: float" {
    var tok = Tokenizer.init("3.14 1e5");
    try std.testing.expectEqual(TokenKind.float,   tok.next().kind);
    try std.testing.expectEqual(TokenKind.float,   tok.next().kind);
    try std.testing.expectEqual(TokenKind.eof,     tok.next().kind);
}

test "tokenizer: line comment skipped" {
    var tok = Tokenizer.init("a -- this is a comment\nb");
    try std.testing.expectEqualStrings("a", tok.next().text);
    try std.testing.expectEqualStrings("b", tok.next().text);
    try std.testing.expectEqual(TokenKind.eof, tok.next().kind);
}

test "tokenizer: keywords" {
    var tok = Tokenizer.init("select FROM WHERE group by");
    try std.testing.expectEqual(TokenKind.keyword, tok.next().kind);
    try std.testing.expectEqual(TokenKind.keyword, tok.next().kind);
    try std.testing.expectEqual(TokenKind.keyword, tok.next().kind);
    try std.testing.expectEqual(TokenKind.keyword, tok.next().kind);
    try std.testing.expectEqual(TokenKind.keyword, tok.next().kind);
}

test "tokenizer: backtick quoted ident" {
    var tok = Tokenizer.init("`my table`");
    const t = tok.next();
    try std.testing.expectEqual(TokenKind.ident, t.kind);
    try std.testing.expectEqualStrings("my table", t.text);
}

test "tokenizer: peek does not consume" {
    var tok = Tokenizer.init("a b");
    try std.testing.expectEqualStrings("a", tok.peek().text);
    try std.testing.expectEqualStrings("a", tok.peek().text);
    try std.testing.expectEqualStrings("a", tok.next().text);
    try std.testing.expectEqualStrings("b", tok.next().text);
}
