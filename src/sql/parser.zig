/// Recursive descent SQL parser for zighouse.
///
/// Entry point: parse(allocator, sql) returns ?*ast.Stmt
///
/// The parser handles the SQL subset exercised by zighouse integration tests:
///   - SELECT [DISTINCT] projections FROM ... WHERE ... GROUP BY ... HAVING ...
///     ORDER BY ... LIMIT ... OFFSET ...
///   - UNION ALL
///   - WITH cte AS (SELECT ...) SELECT ...
///   - Subqueries: FROM (SELECT ...) AS alias, col IN (SELECT ...)
///   - Expressions: literals, functions (including aggregate), CASE WHEN,
///     IN lists, NOT IN, BETWEEN, IS NULL, subscript arr[idx], CAST,
///     lambda (x -> expr), binary ops with correct precedence.
///
/// Functions like splitByChar, has, hasAll, hasAny, concat, substring,
/// JSONExtractString, etc. are parsed as generic FuncExpr nodes; the
/// plan_builder then renders them as raw text for the runtime evaluator.
const std = @import("std");
const ast = @import("ast.zig");
const tok_mod = @import("tokenizer.zig");
const Tokenizer = tok_mod.Tokenizer;
const Token = tok_mod.Token;
const TokenKind = tok_mod.TokenKind;

pub const ParseError = error{
    OutOfMemory,
    UnexpectedToken,
    UnexpectedEof,
    UnsupportedSyntax,
};

/// Parse a SQL string. Returns null if the SQL is empty or cannot be parsed.
/// Caller owns all allocated memory (arena allocator recommended).
pub fn parse(allocator: std.mem.Allocator, sql: []const u8) ?*ast.Stmt {
    var parser = Parser.init(allocator, sql);
    return parser.parseStmt() catch null;
}

const Parser = struct {
    allocator: std.mem.Allocator,
    tok: Tokenizer,

    fn init(allocator: std.mem.Allocator, sql: []const u8) Parser {
        return .{ .allocator = allocator, .tok = Tokenizer.init(sql) };
    }

    fn alloc(self: *Parser, comptime T: type) ParseError!*T {
        return self.allocator.create(T) catch error.OutOfMemory;
    }

    fn dupeSlice(self: *Parser, comptime T: type, s: []const T) ParseError![]T {
        return self.allocator.dupe(T, s) catch error.OutOfMemory;
    }

    // ── Statement ─────────────────────────────────────────────────────────────

    fn parseStmt(self: *Parser) ParseError!*ast.Stmt {
        // WITH clause
        var ctes: []ast.Cte = &.{};
        if (self.tok.eatKeyword("WITH")) {
            ctes = try self.parseCtes();
        }

        const left = try self.parseSelectStmt(ctes);

        // UNION ALL / UNION DISTINCT
        if (self.tok.peekKeyword("UNION")) {
            _ = self.tok.next(); // consume UNION
            const all = self.tok.eatKeyword("ALL");
            if (!all) _ = self.tok.eatKeyword("DISTINCT"); // optional DISTINCT

            const right = try self.parseStmt();
            const node = try self.alloc(ast.Stmt);
            node.* = .{ .union_all = .{
                .left = left,
                .right = right,
                .all = all,
            } };
            return node;
        }

        return left;
    }

    fn parseCtes(self: *Parser) ParseError![]ast.Cte {
        var list = std.ArrayListUnmanaged(ast.Cte).empty;
        while (true) {
            const name_tok = self.tok.next();
            if (name_tok.kind == .eof) return error.UnexpectedEof;
            const name = name_tok.text;
            if (!self.tok.eatKeyword("AS")) return error.UnexpectedToken;
            if (self.tok.peek().kind != .lparen) return error.UnexpectedToken;
            _ = self.tok.next(); // (
            const cte_stmt = try self.parseStmt();
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next(); // )
            try list.append(self.allocator, .{ .name = name, .stmt = cte_stmt });
            if (!self.tok.eatIf(.comma)) break;
        }
        return list.toOwnedSlice(self.allocator) catch error.OutOfMemory;
    }

    fn parseSelectStmt(self: *Parser, ctes: []ast.Cte) ParseError!*ast.Stmt {
        if (!self.tok.eatKeyword("SELECT")) return error.UnexpectedToken;

        const distinct = self.tok.eatKeyword("DISTINCT");

        const projections = try self.parseProjections();

        // FROM clause
        var from: ?ast.FromClause = null;
        if (self.tok.eatKeyword("FROM")) {
            from = try self.parseFromWithJoins();
        }

        var array_join: []ast.ArrayJoinItem = &.{};
        if (self.tok.eatKeyword("ARRAY")) {
            if (!self.tok.eatKeyword("JOIN")) return error.UnexpectedToken;
            array_join = try self.parseArrayJoinItems();
        }

        // WHERE / PREWHERE (ClickHouse extension — treated identically; either order)
        var where: ?*ast.Expr = null;
        for (0..2) |_| {
            const is_where = self.tok.eatKeyword("WHERE");
            const is_prewhere = if (!is_where) self.tok.eatKeyword("PREWHERE") else false;
            if (!is_where and !is_prewhere) break;
            const e = try self.parseExpr();
            if (where) |w| {
                const binop = try self.alloc(ast.BinopExpr);
                binop.* = .{ .op = .and_, .left = w.*, .right = e };
                const merged = try self.alloc(ast.Expr);
                merged.* = .{ .binop = binop };
                where = merged;
            } else {
                where = try self.alloc(ast.Expr);
                where.?.* = e;
            }
        }

        // GROUP BY
        var group_by: []ast.Expr = &.{};
        if (self.tok.eatKeyword("GROUP")) {
            if (!self.tok.eatKeyword("BY")) return error.UnexpectedToken;
            group_by = try self.parseExprList();
        }

        // HAVING
        var having: ?*ast.Expr = null;
        if (self.tok.eatKeyword("HAVING")) {
            const e = try self.parseExpr();
            having = try self.alloc(ast.Expr);
            having.?.* = e;
        }

        // ORDER BY
        var order_by: []ast.OrderItem = &.{};
        if (self.tok.eatKeyword("ORDER")) {
            if (!self.tok.eatKeyword("BY")) return error.UnexpectedToken;
            order_by = try self.parseOrderBy();
        }

        // LIMIT
        var limit: ?i64 = null;
        if (self.tok.eatKeyword("LIMIT")) {
            const t = self.tok.next();
            if (t.kind != .integer) return error.UnexpectedToken;
            limit = std.fmt.parseInt(i64, t.text, 10) catch return error.UnexpectedToken;
        }

        // OFFSET
        var offset: ?i64 = null;
        if (self.tok.eatKeyword("OFFSET")) {
            const t = self.tok.next();
            if (t.kind != .integer) return error.UnexpectedToken;
            offset = std.fmt.parseInt(i64, t.text, 10) catch return error.UnexpectedToken;
        }

        // Optional trailing semicolon
        _ = self.tok.eatIf(.semicolon);

        const node = try self.alloc(ast.Stmt);
        node.* = .{ .select = .{
            .distinct = distinct,
            .projections = projections,
            .from = from,
            .array_join = array_join,
            .where = where,
            .group_by = group_by,
            .having = having,
            .order_by = order_by,
            .limit = limit,
            .offset = offset,
            .ctes = ctes,
        } };
        return node;
    }

    // ── Projections ───────────────────────────────────────────────────────────

    fn parseProjections(self: *Parser) ParseError![]ast.Projection {
        var list = std.ArrayListUnmanaged(ast.Projection).empty;
        while (true) {
            const expr = try self.parseExpr();
            const alias = try self.parseOptionalAlias();
            try list.append(self.allocator, .{ .expr = expr, .alias = alias });
            if (!self.tok.eatIf(.comma)) break;
        }
        return list.toOwnedSlice(self.allocator) catch error.OutOfMemory;
    }

    fn parseOptionalAlias(self: *Parser) ParseError!?[]const u8 {
        // AS alias
        if (self.tok.eatKeyword("AS")) {
            const t = self.tok.next();
            return t.text;
        }
        // Implicit alias: next token is an ident/keyword that is not a clause keyword
        const t = self.tok.peek();
        if ((t.kind == .ident or t.kind == .keyword) and !isClauseStart(t.text)) {
            _ = self.tok.next();
            return t.text;
        }
        return null;
    }

    fn parseArrayJoinItems(self: *Parser) ParseError![]ast.ArrayJoinItem {
        var list = std.ArrayListUnmanaged(ast.ArrayJoinItem).empty;
        while (true) {
            const expr = try self.parseExpr();
            const alias = try self.parseOptionalAlias();
            try list.append(self.allocator, .{ .expr = expr, .alias = alias });
            if (!self.tok.eatIf(.comma)) break;
        }
        return list.toOwnedSlice(self.allocator) catch error.OutOfMemory;
    }

    fn isClauseStart(kw: []const u8) bool {
        const clauses = [_][]const u8{
            "from",  "where",  "group", "having", "order", "limit", "offset",
            "union", "select", "with",  "join",   "array", "inner", "left",
            "right", "outer",  "cross", "on",     "using",
        };
        for (clauses) |c| {
            if (std.ascii.eqlIgnoreCase(kw, c)) return true;
        }
        return false;
    }

    // ── FROM clause with JOIN ─────────────────────────────────────────────────

    fn parseFromWithJoins(self: *Parser) ParseError!ast.FromClause {
        var left = try self.parseFrom();

        // Loop to handle left-associative chained JOINs.
        while (true) {
            const t = self.tok.peek();
            if (t.kind != .keyword) break;

            var kind: ast.JoinKind = undefined;

            if (std.ascii.eqlIgnoreCase(t.text, "JOIN")) {
                kind = .inner;
                _ = self.tok.next();
            } else if (std.ascii.eqlIgnoreCase(t.text, "INNER")) {
                _ = self.tok.next();
                if (!self.tok.eatKeyword("JOIN")) return error.UnexpectedToken;
                kind = .inner;
            } else if (std.ascii.eqlIgnoreCase(t.text, "LEFT")) {
                _ = self.tok.next();
                _ = self.tok.eatKeyword("OUTER"); // optional OUTER
                if (!self.tok.eatKeyword("JOIN")) return error.UnexpectedToken;
                kind = .left;
            } else if (std.ascii.eqlIgnoreCase(t.text, "RIGHT")) {
                _ = self.tok.next();
                _ = self.tok.eatKeyword("OUTER");
                if (!self.tok.eatKeyword("JOIN")) return error.UnexpectedToken;
                kind = .right;
            } else if (std.ascii.eqlIgnoreCase(t.text, "FULL")) {
                _ = self.tok.next();
                _ = self.tok.eatKeyword("OUTER");
                if (!self.tok.eatKeyword("JOIN")) return error.UnexpectedToken;
                kind = .full;
            } else {
                break;
            }

            const right = try self.parseFrom();
            if (!self.tok.eatKeyword("ON")) return error.UnexpectedToken;
            const on_expr = try self.parseExpr();

            const left_ptr = try self.alloc(ast.FromClause);
            left_ptr.* = left;
            const right_ptr = try self.alloc(ast.FromClause);
            right_ptr.* = right;
            const on_ptr = try self.alloc(ast.Expr);
            on_ptr.* = on_expr;

            left = .{ .join = .{
                .kind = kind,
                .left = left_ptr,
                .right = right_ptr,
                .on = on_ptr,
            } };
        }

        return left;
    }

    // ── FROM clause ───────────────────────────────────────────────────────────

    fn parseFrom(self: *Parser) ParseError!ast.FromClause {
        const t = self.tok.peek();

        // Subquery: FROM (SELECT ...)
        if (t.kind == .lparen) {
            _ = self.tok.next(); // (
            const sub = try self.parseStmt();
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next(); // )
            const alias = try self.parseOptionalAlias();
            return .{ .subquery = .{ .stmt = sub, .alias = alias } };
        }

        // Table name (possibly qualified: db.table) or table function
        if (t.kind == .ident or t.kind == .keyword) {
            const name = self.tok.next().text;

            // Check for qualified name: db.table
            var db: ?[]const u8 = null;
            var table_name: []const u8 = name;
            if (self.tok.peek().kind == .dot) {
                _ = self.tok.next(); // .
                const second = self.tok.next();
                db = name;
                table_name = second.text;
                // Could be db.table.func or system.numbers etc
                // Check for another dot (three-part name) — skip
                if (self.tok.peek().kind == .dot) {
                    _ = self.tok.next();
                    const third = self.tok.next();
                    _ = third; // ignore third part for now
                }
            }

            // Table function: name(args)
            if (self.tok.peek().kind == .lparen) {
                _ = self.tok.next(); // (
                var args = std.ArrayListUnmanaged(ast.Expr).empty;
                if (self.tok.peek().kind != .rparen) {
                    while (true) {
                        try args.append(self.allocator, try self.parseExpr());
                        if (!self.tok.eatIf(.comma)) break;
                    }
                }
                _ = self.tok.next(); // )
                const alias = try self.parseOptionalAlias();
                const args_owned = try args.toOwnedSlice(self.allocator);

                // numbers(N) → special FromClause
                if (std.ascii.eqlIgnoreCase(table_name, "numbers") and args_owned.len == 1) {
                    if (args_owned[0] == .int) {
                        return .{ .numbers = args_owned[0].int };
                    }
                }
                return .{ .table_func = .{ .name = table_name, .args = args_owned, .alias = alias } };
            }

            // system.numbers → special
            if (db != null and std.ascii.eqlIgnoreCase(db.?, "system") and
                std.ascii.eqlIgnoreCase(table_name, "numbers"))
            {
                return .{ .numbers = -1 }; // unbounded; limit applied separately
            }

            const alias = try self.parseOptionalAlias();
            return .{ .table = .{ .db = db, .name = table_name, .alias = alias } };
        }

        return error.UnexpectedToken;
    }

    // ── ORDER BY ─────────────────────────────────────────────────────────────

    fn parseOrderBy(self: *Parser) ParseError![]ast.OrderItem {
        var list = std.ArrayListUnmanaged(ast.OrderItem).empty;
        while (true) {
            const expr = try self.parseExpr();
            const desc = self.tok.eatKeyword("DESC");
            if (!desc) _ = self.tok.eatKeyword("ASC");
            try list.append(self.allocator, .{ .expr = expr, .desc = desc });
            if (!self.tok.eatIf(.comma)) break;
        }
        return list.toOwnedSlice(self.allocator) catch error.OutOfMemory;
    }

    // ── Expression parsing (Pratt-style) ──────────────────────────────────────

    fn parseExpr(self: *Parser) ParseError!ast.Expr {
        return self.parsePrec(0);
    }

    fn parseExprList(self: *Parser) ParseError![]ast.Expr {
        var list = std.ArrayListUnmanaged(ast.Expr).empty;
        while (true) {
            try list.append(self.allocator, try self.parseExpr());
            if (!self.tok.eatIf(.comma)) break;
        }
        return list.toOwnedSlice(self.allocator) catch error.OutOfMemory;
    }

    // Precedence levels (higher = tighter binding):
    //   1: OR
    //   2: AND
    //   3: NOT (unary)  — handled in parsePrimary
    //   4: IS NULL, BETWEEN, LIKE, IN (postfix predicates)
    //   5: comparison: = <> < <= > >=
    //   6: + - (additive)   || (concat)
    //   7: * / % (multiplicative)
    //   8: unary minus     — handled in parsePrimary
    //   9: postfix: [subscript]

    fn parsePrec(self: *Parser, min_prec: u8) ParseError!ast.Expr {
        var left = try self.parseUnary();

        while (true) {
            const t = self.tok.peek();
            const prec = self.infixPrec(t) orelse break;
            if (prec < min_prec) break;

            // Postfix predicates: IS NULL/NOT NULL, BETWEEN, LIKE, IN
            if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "IS")) {
                _ = self.tok.next(); // IS
                const is_not = self.tok.eatKeyword("NOT");
                if (!self.tok.eatKeyword("NULL")) return error.UnexpectedToken;
                const e = try self.alloc(ast.IsNullExpr);
                e.* = .{ .val = left, .is_not = is_not };
                left = .{ .is_null = e };
                continue;
            }

            if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "NOT")) {
                _ = self.tok.next(); // NOT
                if (self.tok.eatKeyword("IN")) {
                    left = try self.parseInRhs(left, true);
                    continue;
                }
                if (self.tok.eatKeyword("LIKE")) {
                    const pattern = try self.parsePrec(prec + 1);
                    var escape: ?[]const u8 = null;
                    if (self.tok.eatKeyword("ESCAPE")) {
                        const esc_tok = self.tok.peek();
                        if (esc_tok.kind == .string) {
                            _ = self.tok.next();
                            const raw = esc_tok.text;
                            const inner = if (raw.len >= 2) raw[1 .. raw.len - 1] else raw;
                            if (inner.len > 0) escape = inner;
                        }
                    }
                    const e = try self.alloc(ast.BinopExpr);
                    e.* = .{ .op = .not_like, .left = left, .right = pattern, .escape = escape };
                    left = .{ .binop = e };
                    continue;
                }
                if (self.tok.eatKeyword("BETWEEN")) {
                    left = try self.parseBetweenRhs(left, true);
                    continue;
                }
                return error.UnexpectedToken;
            }

            if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "IN")) {
                _ = self.tok.next(); // IN
                left = try self.parseInRhs(left, false);
                continue;
            }

            if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "LIKE")) {
                _ = self.tok.next(); // LIKE
                const pattern = try self.parsePrec(prec + 1);
                var escape: ?[]const u8 = null;
                if (self.tok.eatKeyword("ESCAPE")) {
                    const esc_tok = self.tok.peek();
                    if (esc_tok.kind == .string) {
                        _ = self.tok.next();
                        const raw = esc_tok.text;
                        const inner = if (raw.len >= 2) raw[1 .. raw.len - 1] else raw;
                        if (inner.len > 0) escape = inner;
                    }
                }
                const e = try self.alloc(ast.BinopExpr);
                e.* = .{ .op = .like, .left = left, .right = pattern, .escape = escape };
                left = .{ .binop = e };
                continue;
            }

            if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "BETWEEN")) {
                _ = self.tok.next(); // BETWEEN
                left = try self.parseBetweenRhs(left, false);
                continue;
            }

            // Subscript: expr[index]
            if (t.kind == .lbracket) {
                _ = self.tok.next(); // [
                const idx = try self.parseExpr();
                if (self.tok.peek().kind != .rbracket) return error.UnexpectedToken;
                _ = self.tok.next(); // ]
                const e = try self.alloc(ast.SubscriptExpr);
                e.* = .{ .base = left, .index = idx };
                left = .{ .subscript = e };
                continue;
            }

            // Binary infix
            const op = self.tokenToBinOp(t) orelse break;
            _ = self.tok.next(); // consume operator
            const right = try self.parsePrec(prec + 1);
            const e = try self.alloc(ast.BinopExpr);
            e.* = .{ .op = op, .left = left, .right = right };
            left = .{ .binop = e };
        }

        return left;
    }

    fn infixPrec(self: *Parser, t: Token) ?u8 {
        _ = self;
        switch (t.kind) {
            .keyword => {
                if (std.ascii.eqlIgnoreCase(t.text, "OR")) return 1;
                if (std.ascii.eqlIgnoreCase(t.text, "AND")) return 2;
                if (std.ascii.eqlIgnoreCase(t.text, "IS")) return 4;
                if (std.ascii.eqlIgnoreCase(t.text, "NOT")) return 4; // NOT IN / NOT LIKE / NOT BETWEEN
                if (std.ascii.eqlIgnoreCase(t.text, "IN")) return 4;
                if (std.ascii.eqlIgnoreCase(t.text, "LIKE")) return 4;
                if (std.ascii.eqlIgnoreCase(t.text, "BETWEEN")) return 4;
                return null;
            },
            .eq, .neq, .lt, .lte, .gt, .gte => return 5,
            .plus, .minus => return 6,
            .concat => return 6,
            .star, .slash, .percent => return 7,
            .lbracket => return 9, // subscript
            else => return null,
        }
    }

    fn tokenToBinOp(self: *Parser, t: Token) ?ast.BinOp {
        _ = self;
        switch (t.kind) {
            .eq => return .eq,
            .neq => return .neq,
            .lt => return .lt,
            .lte => return .lte,
            .gt => return .gt,
            .gte => return .gte,
            .plus => return .add,
            .minus => return .sub,
            .star => return .mul,
            .slash => return .div,
            .percent => return .mod,
            .concat => return .concat,
            .keyword => {
                if (std.ascii.eqlIgnoreCase(t.text, "AND")) return .and_;
                if (std.ascii.eqlIgnoreCase(t.text, "OR")) return .or_;
                return null;
            },
            else => return null,
        }
    }

    fn parseUnary(self: *Parser) ParseError!ast.Expr {
        const t = self.tok.peek();
        // Unary NOT
        if (t.kind == .keyword and std.ascii.eqlIgnoreCase(t.text, "NOT")) {
            _ = self.tok.next();
            const inner = try self.parseUnary();
            const e = try self.alloc(ast.Expr);
            e.* = inner;
            return .{ .not = e };
        }
        // Unary minus
        if (t.kind == .minus) {
            _ = self.tok.next();
            const inner = try self.parseUnary();
            // Fold: -(integer literal) → negative int
            if (inner == .int) return .{ .int = -inner.int };
            if (inner == .float) return .{ .float = -inner.float };
            const e = try self.alloc(ast.Expr);
            e.* = inner;
            return .{ .neg = e };
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) ParseError!ast.Expr {
        const t = self.tok.peek();

        // Parenthesised expression or subquery
        if (t.kind == .lparen) {
            _ = self.tok.next();
            // Is it a subquery?
            if (self.tok.peekKeyword("SELECT") or self.tok.peekKeyword("WITH")) {
                const sub = try self.parseStmt();
                if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
                _ = self.tok.next();
                return .{ .subquery = sub };
            }
            const inner = try self.parseExpr();
            // Could be a tuple (expr, expr, ...) — we just return the first for now
            // but skip any remaining comma-separated exprs
            while (self.tok.eatIf(.comma)) {
                _ = try self.parseExpr(); // discard additional tuple elements
            }
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next();
            return inner;
        }

        // Array literal: [expr, ...]
        if (t.kind == .lbracket) {
            _ = self.tok.next();
            var items = std.ArrayListUnmanaged(ast.Expr).empty;
            if (self.tok.peek().kind != .rbracket) {
                while (true) {
                    try items.append(self.allocator, try self.parseExpr());
                    if (!self.tok.eatIf(.comma)) break;
                }
            }
            if (self.tok.peek().kind != .rbracket) return error.UnexpectedToken;
            _ = self.tok.next();
            const owned = try items.toOwnedSlice(self.allocator);
            return .{ .array = owned };
        }

        // Integer literal
        if (t.kind == .integer) {
            _ = self.tok.next();
            const v = std.fmt.parseInt(i64, t.text, 10) catch {
                // May be too large for i64 — try u64
                const uv = std.fmt.parseInt(u64, t.text, 10) catch return error.UnexpectedToken;
                return .{ .uint = uv };
            };
            return .{ .int = v };
        }

        // Float literal
        if (t.kind == .float) {
            _ = self.tok.next();
            const v = std.fmt.parseFloat(f64, t.text) catch return error.UnexpectedToken;
            return .{ .float = v };
        }

        // String literal
        if (t.kind == .string) {
            _ = self.tok.next();
            const raw = t.text; // includes surrounding quotes
            const inner = if (raw.len >= 2) raw[1 .. raw.len - 1] else raw;
            return .{ .str = inner };
        }

        // * (star)
        if (t.kind == .star) {
            _ = self.tok.next();
            return .{ .star = {} };
        }

        // Keywords that are literals or start special expressions
        if (t.kind == .keyword or t.kind == .ident) {
            // NULL
            if (std.ascii.eqlIgnoreCase(t.text, "NULL")) {
                _ = self.tok.next();
                return .{ .null_lit = {} };
            }
            // TRUE / FALSE
            if (std.ascii.eqlIgnoreCase(t.text, "TRUE")) {
                _ = self.tok.next();
                return .{ .bool_lit = true };
            }
            if (std.ascii.eqlIgnoreCase(t.text, "FALSE")) {
                _ = self.tok.next();
                return .{ .bool_lit = false };
            }
            // CASE WHEN ...
            if (std.ascii.eqlIgnoreCase(t.text, "CASE")) {
                _ = self.tok.next();
                return self.parseCaseExpr();
            }
            // CAST(expr AS type)
            if (std.ascii.eqlIgnoreCase(t.text, "CAST")) {
                _ = self.tok.next();
                return self.parseCastExpr();
            }
            // NOT (unary, when used in primary position like NOT EXISTS)
            if (std.ascii.eqlIgnoreCase(t.text, "NOT")) {
                _ = self.tok.next();
                const inner = try self.parseUnary();
                const e = try self.alloc(ast.Expr);
                e.* = inner;
                return .{ .not = e };
            }

            // Identifier — might be a function call or column reference
            const name = self.tok.next().text;

            // SQL-standard special function syntax: SUBSTRING, POSITION, TRIM
            if (std.ascii.eqlIgnoreCase(name, "substring") or
                std.ascii.eqlIgnoreCase(name, "position") or
                std.ascii.eqlIgnoreCase(name, "trim"))
            {
                if (self.tok.peek().kind != .lparen) return error.UnexpectedToken;
                _ = self.tok.next(); // (
                return self.parseSpecialFunc(name);
            }

            // Check for lambda params: single ident followed by -> (without parens)
            if (self.tok.peek().kind == .arrow) {
                _ = self.tok.next(); // ->
                const body = try self.parseExpr();
                const params = try self.allocator.alloc([]const u8, 1);
                params[0] = name;
                const lam = try self.alloc(ast.LambdaExpr);
                lam.* = .{ .params = params, .body = body };
                return .{ .lambda = lam };
            }

            // Function call: name(...)
            if (self.tok.peek().kind == .lparen) {
                _ = self.tok.next(); // (
                var fn_distinct = false;
                if (self.tok.eatKeyword("DISTINCT")) {
                    fn_distinct = true;
                } else {
                    _ = self.tok.eatKeyword("ALL");
                }

                var args = std.ArrayListUnmanaged(ast.Expr).empty;
                if (self.tok.peek().kind != .rparen) {
                    while (true) {
                        // Check for lambda with multiple params: (x, y) -> expr
                        // This is tricky — the first arg might look like a col ref.
                        // We try to parse as expr and check for lambda arrow after ')'.
                        const arg = try self.parseExpr();
                        try args.append(self.allocator, arg);
                        if (!self.tok.eatIf(.comma)) break;
                    }
                }
                if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
                _ = self.tok.next(); // )

                // Check for lambda: if all args are col refs and next is ->, it's a lambda
                if (self.tok.peek().kind == .arrow) {
                    _ = self.tok.next(); // ->
                    const body = try self.parseExpr();
                    var params = try self.allocator.alloc([]const u8, args.items.len);
                    for (args.items, 0..) |a, i| {
                        params[i] = if (a == .col) a.col else "x";
                    }
                    const lam = try self.alloc(ast.LambdaExpr);
                    lam.* = .{ .params = params, .body = body };
                    return .{ .lambda = lam };
                }

                // Lowercase function name for canonical comparison
                const fn_name_lower = blk: {
                    const duped = self.allocator.dupe(u8, name) catch return error.OutOfMemory;
                    for (duped) |*c| c.* = std.ascii.toLower(c.*);
                    break :blk duped;
                };

                // COUNT(*): special case
                if (std.mem.eql(u8, fn_name_lower, "count") and args.items.len == 1 and args.items[0] == .star) {
                    args.items[0] = .{ .star = {} };
                }

                const args_owned = try args.toOwnedSlice(self.allocator);
                return .{ .func = .{
                    .name = fn_name_lower,
                    .args = args_owned,
                    .distinct = fn_distinct,
                } };
            }

            // Qualified name: db.table.col or table.col
            // We consume additional .name parts and concatenate them.
            if (self.tok.peek().kind == .dot) {
                var parts: std.ArrayListUnmanaged(u8) = .empty;
                defer parts.deinit(self.allocator);
                try parts.appendSlice(self.allocator, name);
                while (self.tok.peek().kind == .dot) {
                    _ = self.tok.next(); // .
                    const part = self.tok.peek();
                    if (part.kind == .ident or part.kind == .keyword or part.kind == .star) {
                        _ = self.tok.next();
                        try parts.append(self.allocator, '.');
                        try parts.appendSlice(self.allocator, part.text);
                    } else break;
                }
                const dup = self.allocator.dupe(u8, parts.items) catch return error.OutOfMemory;
                return .{ .col = dup };
            }

            // Plain column reference
            return .{ .col = name };
        }

        return error.UnexpectedToken;
    }

    fn parseInRhs(self: *Parser, lhs: ast.Expr, negated: bool) ParseError!ast.Expr {
        const t = self.tok.peek();
        if (t.kind == .lparen) {
            _ = self.tok.next(); // (
            // Is it a subquery?
            if (self.tok.peekKeyword("SELECT") or self.tok.peekKeyword("WITH")) {
                const sub = try self.parseStmt();
                if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
                _ = self.tok.next();
                const e = try self.alloc(ast.InSubqExpr);
                e.* = .{ .lhs = lhs, .query = sub, .negated = negated };
                return .{ .in_subq = e };
            }
            // Comma-separated list
            var items = std.ArrayListUnmanaged(ast.Expr).empty;
            if (self.tok.peek().kind != .rparen) {
                while (true) {
                    try items.append(self.allocator, try self.parseExpr());
                    if (!self.tok.eatIf(.comma)) break;
                }
            }
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next();
            const e = try self.alloc(ast.InListExpr);
            e.* = .{ .lhs = lhs, .items = try items.toOwnedSlice(self.allocator), .negated = negated };
            return .{ .in_list = e };
        }
        if (t.kind == .lbracket) {
            // IN [...] (ClickHouse array syntax, after normalizeInBrackets may not be needed)
            _ = self.tok.next();
            var items = std.ArrayListUnmanaged(ast.Expr).empty;
            if (self.tok.peek().kind != .rbracket) {
                while (true) {
                    try items.append(self.allocator, try self.parseExpr());
                    if (!self.tok.eatIf(.comma)) break;
                }
            }
            if (self.tok.peek().kind != .rbracket) return error.UnexpectedToken;
            _ = self.tok.next();
            const e = try self.alloc(ast.InListExpr);
            e.* = .{ .lhs = lhs, .items = try items.toOwnedSlice(self.allocator), .negated = negated };
            return .{ .in_list = e };
        }
        return error.UnexpectedToken;
    }

    fn parseBetweenRhs(self: *Parser, val: ast.Expr, negated: bool) ParseError!ast.Expr {
        const lo = try self.parsePrec(6); // above AND
        if (!self.tok.eatKeyword("AND")) return error.UnexpectedToken;
        const hi = try self.parsePrec(6);
        const e = try self.alloc(ast.BetweenExpr);
        e.* = .{ .val = val, .lo = lo, .hi = hi, .negated = negated };
        return .{ .between = e };
    }

    fn parseCaseExpr(self: *Parser) ParseError!ast.Expr {
        // CASE has already been consumed
        // Searched CASE: CASE WHEN cond THEN val [WHEN ...] [ELSE val] END
        // Simple CASE:   CASE expr WHEN val THEN val ... END
        var input: ?*ast.Expr = null;
        if (!self.tok.peekKeyword("WHEN")) {
            const e = try self.parseExpr();
            input = try self.alloc(ast.Expr);
            input.?.* = e;
        }

        var whens = std.ArrayListUnmanaged(ast.WhenClause).empty;
        while (self.tok.eatKeyword("WHEN")) {
            const cond = try self.parseExpr();
            if (!self.tok.eatKeyword("THEN")) return error.UnexpectedToken;
            const then = try self.parseExpr();
            try whens.append(self.allocator, .{ .cond = cond, .then = then });
        }

        var else_: ?*ast.Expr = null;
        if (self.tok.eatKeyword("ELSE")) {
            const e = try self.parseExpr();
            else_ = try self.alloc(ast.Expr);
            else_.?.* = e;
        }

        if (!self.tok.eatKeyword("END")) return error.UnexpectedToken;

        const ce = try self.alloc(ast.CaseExpr);
        ce.* = .{
            .input = input,
            .whens = try whens.toOwnedSlice(self.allocator),
            .else_ = else_,
        };
        return .{ .case_when = ce };
    }

    fn parseCastExpr(self: *Parser) ParseError!ast.Expr {
        // CAST has already been consumed
        if (self.tok.peek().kind != .lparen) return error.UnexpectedToken;
        _ = self.tok.next(); // (
        const val = try self.parseExpr();
        // Support both CAST(expr AS type) and CAST(expr, 'type')
        const use_as = self.tok.eatKeyword("AS");
        if (!use_as and !self.tok.eatIf(.comma)) return error.UnexpectedToken;
        // Type name may be multi-word or nested: UNSIGNED INT, Array(String), etc.
        var type_parts: std.ArrayListUnmanaged(u8) = .empty;
        defer type_parts.deinit(self.allocator);
        var depth: usize = 0;
        while (self.tok.peek().kind != .eof) {
            if (self.tok.peek().kind == .rparen and depth == 0) break;
            const tp = self.tok.next();
            if (tp.kind == .lparen) depth += 1;
            if (tp.kind == .rparen) {
                if (depth == 0) break;
                depth -= 1;
            }
            const needs_space = type_parts.items.len > 0 and
                tp.kind != .lparen and tp.kind != .rparen and
                type_parts.items[type_parts.items.len - 1] != '(';
            if (needs_space) try type_parts.append(self.allocator, ' ');
            // Strip surrounding single quotes from string literal tokens
            const raw = tp.text;
            const clean = if (raw.len >= 2 and raw[0] == '\'' and raw[raw.len - 1] == '\'')
                raw[1 .. raw.len - 1]
            else
                raw;
            try type_parts.appendSlice(self.allocator, clean);
        }
        if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
        _ = self.tok.next(); // )
        const type_name = try self.allocator.dupe(u8, type_parts.items);
        const ce = try self.alloc(ast.CastExpr);
        ce.* = .{ .val = val, .type_name = type_name };
        return .{ .cast = ce };
    }

    /// Parse SQL-standard special function syntax:
    ///   SUBSTRING(str FROM start [FOR len])
    ///   POSITION(needle IN haystack)
    ///   TRIM([[LEADING|TRAILING|BOTH] [char] FROM] str)
    fn parseSpecialFunc(self: *Parser, name: []const u8) ParseError!ast.Expr {
        if (std.ascii.eqlIgnoreCase(name, "substring")) return self.parseSubstringFunc();
        if (std.ascii.eqlIgnoreCase(name, "position")) return self.parsePositionFunc();
        if (std.ascii.eqlIgnoreCase(name, "trim")) return self.parseTrimFunc();
        return error.UnexpectedToken;
    }

    fn parseSubstringFunc(self: *Parser) ParseError!ast.Expr {
        // SQL standard form: SUBSTRING(str FROM start [FOR len] [USING ...])
        // Functional form: substring(str, start, len) — detected by comma after first expr
        const first = try self.parseExpr();
        // Check for functional form (comma-separated args)
        if (self.tok.eatIf(.comma)) {
            var args = std.ArrayListUnmanaged(ast.Expr).empty;
            try args.append(self.allocator, first);
            while (true) {
                try args.append(self.allocator, try self.parseExpr());
                if (!self.tok.eatIf(.comma)) break;
            }
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next(); // )
            const fn_name = try self.allocator.dupe(u8, "substring");
            return .{ .func = .{ .name = fn_name, .args = try args.toOwnedSlice(self.allocator) } };
        }
        // Standard FROM/FOR form
        var start_expr: ?ast.Expr = null;
        var len_expr: ?ast.Expr = null;
        if (self.tok.eatKeyword("FROM")) {
            start_expr = try self.parseExpr();
        }
        if (self.tok.eatKeyword("FOR")) {
            len_expr = try self.parseExpr();
        }
        // Skip optional USING CHARACTERS / USING OCTETS
        if (self.tok.eatKeyword("USING")) {
            _ = self.tok.next(); // CHARACTERS or OCTETS
        }
        if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
        _ = self.tok.next(); // )
        var args = std.ArrayListUnmanaged(ast.Expr).empty;
        try args.append(self.allocator, first);
        if (start_expr) |s| try args.append(self.allocator, s);
        if (len_expr) |l| try args.append(self.allocator, l);
        const args_slice = try args.toOwnedSlice(self.allocator);
        const fn_name = try self.allocator.dupe(u8, "substring");
        return .{ .func = .{ .name = fn_name, .args = args_slice } };
    }

    fn parsePositionFunc(self: *Parser) ParseError!ast.Expr {
        // Functional form: position(haystack, needle) — comma-separated args
        const first = try self.parseExpr();
        if (self.tok.eatIf(.comma)) {
            var args = std.ArrayListUnmanaged(ast.Expr).empty;
            try args.append(self.allocator, first);
            while (true) {
                try args.append(self.allocator, try self.parseExpr());
                if (!self.tok.eatIf(.comma)) break;
            }
            if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
            _ = self.tok.next(); // )
            const fn_name = try self.allocator.dupe(u8, "position");
            return .{ .func = .{ .name = fn_name, .args = try args.toOwnedSlice(self.allocator) } };
        }
        // SQL standard form: POSITION(needle IN haystack)
        if (!self.tok.eatKeyword("IN")) return error.UnexpectedToken;
        const haystack = try self.parseExpr();
        // Skip optional USING CHARACTERS / USING OCTETS
        if (self.tok.eatKeyword("USING")) {
            _ = self.tok.next(); // CHARACTERS or OCTETS
        }
        if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
        _ = self.tok.next(); // )
        var args = std.ArrayListUnmanaged(ast.Expr).empty;
        try args.append(self.allocator, haystack);
        try args.append(self.allocator, first); // needle (swap: haystack first for kernel)
        const args_slice = try args.toOwnedSlice(self.allocator);
        const fn_name = try self.allocator.dupe(u8, "position");
        return .{ .func = .{ .name = fn_name, .args = args_slice } };
    }

    fn parseTrimFunc(self: *Parser) ParseError!ast.Expr {
        const spec: enum { default, both, leading, trailing } = if (self.tok.eatKeyword("LEADING"))
            .leading
        else if (self.tok.eatKeyword("TRAILING"))
            .trailing
        else if (self.tok.eatKeyword("BOTH"))
            .both
        else
            .default;
        // Functional form: trim(str) — no LEADING/TRAILING/BOTH, and no FROM after first expr
        if (spec == .default) {
            var peek = self.tok;
            const ft = peek.next(); // peek at first potential type arg
            if (!std.ascii.eqlIgnoreCase(ft.text, "FROM") and !std.ascii.eqlIgnoreCase(ft.text, "LEADING") and
                !std.ascii.eqlIgnoreCase(ft.text, "TRAILING") and !std.ascii.eqlIgnoreCase(ft.text, "BOTH"))
            {
                // Functional form: trim(expr) or trim(expr, expr, ...)
                const str_expr = try self.parseExpr();
                if (self.tok.peek().kind == .rparen) {
                    _ = self.tok.next();
                    const fn_name = try self.allocator.dupe(u8, "trim");
                    const args = try self.allocator.alloc(ast.Expr, 1);
                    args[0] = str_expr;
                    return .{ .func = .{ .name = fn_name, .args = args } };
                }
                if (self.tok.eatIf(.comma)) {
                    var args = std.ArrayListUnmanaged(ast.Expr).empty;
                    try args.append(self.allocator, str_expr);
                    while (true) {
                        try args.append(self.allocator, try self.parseExpr());
                        if (!self.tok.eatIf(.comma)) break;
                    }
                    if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
                    _ = self.tok.next();
                    const fn_name = try self.allocator.dupe(u8, "trim");
                    return .{ .func = .{ .name = fn_name, .args = try args.toOwnedSlice(self.allocator) } };
                }
            }
        }
        // Skip optional trim character string (when next token is a string followed by FROM)
        var peek2 = self.tok;
        if (!self.tok.peekKeyword("FROM") and self.tok.peek().kind != .rparen and self.tok.peek().kind == .string) {
            _ = peek2.next(); // peek at potential trim char
            if (peek2.peekKeyword("FROM")) {
                _ = self.tok.next(); // consume the trim char string
            }
        }
        _ = self.tok.eatKeyword("FROM");
        const str_expr = try self.parseExpr();
        if (self.tok.peek().kind != .rparen) return error.UnexpectedToken;
        _ = self.tok.next(); // )
        const fn_name = switch (spec) {
            .leading => try self.allocator.dupe(u8, "trimLeft"),
            .trailing => try self.allocator.dupe(u8, "trimRight"),
            .both, .default => try self.allocator.dupe(u8, "trim"),
        };
        var args = std.ArrayListUnmanaged(ast.Expr).empty;
        try args.append(self.allocator, str_expr);
        const args_slice = try args.toOwnedSlice(self.allocator);
        return .{ .func = .{ .name = fn_name, .args = args_slice } };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parser: simple SELECT" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT a, b FROM t WHERE x = 1");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expectEqual(@as(usize, 2), sel.projections.len);
    try std.testing.expect(sel.from != null);
    try std.testing.expect(sel.where != null);
    // cleanup: use arena in real use; here just check no crash
}

test "parser: function call" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT has(['a','b','c'], 'b') as r");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expectEqual(@as(usize, 1), sel.projections.len);
    const proj = sel.projections[0];
    try std.testing.expectEqualStrings("r", proj.alias.?);
    try std.testing.expect(proj.expr == .func);
    try std.testing.expectEqualStrings("has", proj.expr.func.name);
}

test "parser: IN list (square brackets)" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT 1 as x WHERE 'api' in ['api','res','file']");
    try std.testing.expect(stmt != null);
    const where = stmt.?.select.where.?.*;
    try std.testing.expect(where == .in_list);
    try std.testing.expectEqual(false, where.in_list.negated);
    try std.testing.expectEqual(@as(usize, 3), where.in_list.items.len);
}

test "parser: NOT IN subquery" {
    const allocator = std.testing.allocator;
    const sql = "SELECT 'x' as v WHERE 'x' not in (select content from rule_filter_dict where content!='' and length(host)=0 and risk='test')";
    const stmt = parse(allocator, sql);
    try std.testing.expect(stmt != null);
    const where = stmt.?.select.where.?.*;
    try std.testing.expect(where == .in_subq);
    try std.testing.expectEqual(true, where.in_subq.negated);
}

test "parser: UNION ALL" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    try std.testing.expect(stmt != null);
    try std.testing.expect(stmt.?.* == .union_all);
}

test "parser: CAST supports AS and comma type forms" {
    const allocator = std.testing.allocator;

    const standard = parse(allocator, "SELECT CAST(a AS Int64) FROM t");
    try std.testing.expect(standard != null);
    try std.testing.expect(standard.?.select.projections[0].expr == .cast);
    try std.testing.expectEqualStrings("Int64", standard.?.select.projections[0].expr.cast.type_name);

    const clickhouse = parse(allocator, "SELECT CAST(a, 'Int64') FROM t");
    try std.testing.expect(clickhouse != null);
    try std.testing.expect(clickhouse.?.select.projections[0].expr == .cast);
    try std.testing.expectEqualStrings("Int64", clickhouse.?.select.projections[0].expr.cast.type_name);

    const array_cast = parse(allocator, "SELECT CAST([], 'Array(String)'), CAST([] AS Array(String)) FROM t");
    try std.testing.expect(array_cast != null);
    try std.testing.expectEqualStrings("Array(String)", array_cast.?.select.projections[0].expr.cast.type_name);
    try std.testing.expectEqualStrings("Array(String)", array_cast.?.select.projections[1].expr.cast.type_name);
}

test "parser: GROUP BY ORDER BY LIMIT" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT COUNT(*) as n FROM t GROUP BY x ORDER BY n DESC LIMIT 10");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expectEqual(@as(usize, 1), sel.group_by.len);
    try std.testing.expectEqual(@as(usize, 1), sel.order_by.len);
    try std.testing.expectEqual(true, sel.order_by[0].desc);
    try std.testing.expectEqual(@as(?i64, 10), sel.limit);
}

test "parser: ARRAY JOIN clause with lockstep aliases" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT fk, fv FROM events ARRAY JOIN mapKeys(features) AS fk, mapValues(features) AS fv GROUP BY fk, fv");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expectEqual(@as(usize, 2), sel.array_join.len);
    try std.testing.expectEqualStrings("fk", sel.array_join[0].alias.?);
    try std.testing.expectEqualStrings("fv", sel.array_join[1].alias.?);
    try std.testing.expect(sel.array_join[0].expr == .func);
    try std.testing.expectEqualStrings("mapkeys", sel.array_join[0].expr.func.name);
    try std.testing.expectEqual(@as(usize, 2), sel.group_by.len);
}

test "parser: CASE WHEN" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END as s FROM t");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expect(sel.projections[0].expr == .case_when);
}

test "parser: lambda (x -> expr)" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT arrayMap(x -> x * 2, arr) FROM t");
    try std.testing.expect(stmt != null);
    const args = stmt.?.select.projections[0].expr.func.args;
    try std.testing.expectEqual(@as(usize, 2), args.len);
    try std.testing.expect(args[0] == .lambda);
}

test "parser: subquery FROM" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT * FROM (SELECT a, b FROM t WHERE x > 0) AS sub");
    try std.testing.expect(stmt != null);
    const from = stmt.?.select.from.?;
    try std.testing.expect(from == .subquery);
}

test "parser: BETWEEN" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT a FROM t WHERE x BETWEEN 1 AND 10");
    try std.testing.expect(stmt != null);
    const where = stmt.?.select.where.?.*;
    try std.testing.expect(where == .between);
}

test "parser: IS NULL / IS NOT NULL" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT a FROM t WHERE x IS NOT NULL");
    try std.testing.expect(stmt != null);
    const where = stmt.?.select.where.?.*;
    try std.testing.expect(where == .is_null);
    try std.testing.expectEqual(true, where.is_null.is_not);
}

test "parser: compute query no FROM" {
    const allocator = std.testing.allocator;
    const stmt = parse(allocator, "SELECT splitByChar('/', '/api/users') as parts");
    try std.testing.expect(stmt != null);
    const sel = stmt.?.select;
    try std.testing.expect(sel.from == null);
    try std.testing.expectEqualStrings("parts", sel.projections[0].alias.?);
}
