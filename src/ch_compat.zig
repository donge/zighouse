/// ch_compat.zig — ClickHouse→DuckDB grammar rewrites (grammar-level blockers only).
/// Only rewrites what DuckDB's parser cannot accept. Functions that DuckDB can parse
/// as unknown FUNCTION nodes (uniqExact, arrayFilter, has, splitByChar, …) are left
/// unchanged and handled by the executor at runtime.
const std = @import("std");

// ── Rewrite rule table ────────────────────────────────────────────────────────
// Each rule describes one CH construct that DuckDB's parser cannot accept.
//
// kind = .cast       → CAST(<args> AS <param>)
// kind = .date_trunc → date_trunc('<param>', <args>)
// kind = .rename     → <param><args>)   (param already includes the opening '(')
// kind = .multiif    → handled by rwMultiIf (structural, cannot be a data row)

const RewriteKind = enum { cast, date_trunc, rename, multiif };

const RewriteRule = struct {
    name:  []const u8,
    kind:  RewriteKind,
    param: []const u8 = "",
};

const rules = [_]RewriteRule{
    // multiIf — structural rewrite, param unused (rwMultiIf handles it)
    .{ .name = "multiIf",          .kind = .multiif                           },

    // CH type-cast functions → CAST(x AS T)
    // DuckDB does not recognise these names at the parser level.
    // toString(x) — renamed to ch_tostring to avoid DuckDB binding CAST(x AS VARCHAR) when x is TIMESTAMP
    // .{ .name = "toString",         .kind = .cast,       .param = "AS VARCHAR"   },
    .{ .name = "toDate",           .kind = .cast,       .param = "AS DATE"      },
    .{ .name = "toDateTime",       .kind = .cast,       .param = "AS TIMESTAMP" },
    .{ .name = "toUInt8",          .kind = .cast,       .param = "AS UTINYINT"  },
    .{ .name = "toUInt16",         .kind = .cast,       .param = "AS USMALLINT" },
    .{ .name = "toUInt32",         .kind = .cast,       .param = "AS UINTEGER"  },
    .{ .name = "toUInt64",         .kind = .cast,       .param = "AS UBIGINT"   },
    .{ .name = "toInt8",           .kind = .cast,       .param = "AS TINYINT"   },
    .{ .name = "toInt16",          .kind = .cast,       .param = "AS SMALLINT"  },
    .{ .name = "toInt32",          .kind = .cast,       .param = "AS INTEGER"   },
    .{ .name = "toInt64",          .kind = .cast,       .param = "AS BIGINT"    },
    .{ .name = "toFloat32",        .kind = .cast,       .param = "AS FLOAT"     },
    .{ .name = "toFloat64",        .kind = .cast,       .param = "AS DOUBLE"    },

    // CH time-truncation functions → date_trunc('unit', x)
    // DuckDB does not recognise toStartOf* names.
    .{ .name = "toStartOfMinute",  .kind = .date_trunc, .param = "minute"       },
    .{ .name = "toStartOfHour",    .kind = .date_trunc, .param = "hour"         },
    .{ .name = "toStartOfDay",     .kind = .date_trunc, .param = "day"          },
    .{ .name = "toStartOfWeek",    .kind = .date_trunc, .param = "week"         },
    .{ .name = "toStartOfMonth",   .kind = .date_trunc, .param = "month"        },
    .{ .name = "toStartOfQuarter", .kind = .date_trunc, .param = "quarter"      },
    .{ .name = "toStartOfYear",    .kind = .date_trunc, .param = "year"         },

    // Renames — DuckDB treats these as reserved keywords or misparses them.
    .{ .name = "any",                  .kind = .rename,     .param = "any_value("        },
    .{ .name = "lowerUTF8",            .kind = .rename,     .param = "lower("            },
    .{ .name = "upperUTF8",            .kind = .rename,     .param = "upper("            },
    // position(hay, ndl) → strpos(hay, ndl): DuckDB's `position` uses SQL syntax (needle IN hay)
    .{ .name = "position",             .kind = .rename,     .param = "strpos("           },
    .{ .name = "positionCaseInsensitive", .kind = .rename,  .param = "strpos("           },
    // arrayStringConcat(arr, sep) → array_to_string(arr, sep)
    // DuckDB's parser rejects arrayStringConcat when the first arg is an array literal.
    .{ .name = "arrayStringConcat",    .kind = .rename,     .param = "array_to_string("  },
    // materialize(x) is a CH no-op that forces materialization; treat as identity.
    .{ .name = "materialize",          .kind = .rename,     .param = "("                 },
    // toString(x) — rename to avoid DuckDB's built-in toString binding TIMESTAMP args
    .{ .name = "toString",             .kind = .rename,     .param = "ch_tostring("      },
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Rewrite ARRAY JOIN syntax into arrayJoin() function calls in the SELECT list.
///
/// Input:  SELECT a, b FROM t ARRAY JOIN expr1 AS al1, expr2 AS al2 WHERE cond
/// Output: SELECT a, b, arrayJoin(expr1) AS al1, arrayJoin(expr2) AS al2 FROM t WHERE cond
///
/// Aliases that appear in the SELECT list are left as-is (they refer to the ARRAY JOIN result).
/// Only the first ARRAY JOIN item is used for row expansion; others are kept as columns.
/// Check if a string is exactly "system.one" (case-insensitive).
fn isSystemOne(s: []const u8) bool {
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, s, " \t\r\n"), "system.one");
}

/// Try to extract count and start from "range(N)", "range(0, N)", or "range(start, N)" expressions.
/// Returns .{count_expr, start_expr} where count_expr is the number of elements and start_expr is the start value.
/// For range(N): count="N", start=null
/// For range(start, end): count is computed as integer string if both are integer literals.
fn extractRangeArgs(allocator: std.mem.Allocator, expr: []const u8) !?struct { count: []const u8, start: ?[]const u8 } {
    const e = std.mem.trim(u8, expr, " \t\r\n");
    if (!std.ascii.startsWithIgnoreCase(e, "range(")) return null;
    if (e[e.len - 1] != ')') return null;
    const inner = std.mem.trim(u8, e[6 .. e.len - 1], " \t\r\n");
    if (std.mem.indexOf(u8, inner, ",")) |ci| {
        const start_s = std.mem.trim(u8, inner[0..ci], " \t\r\n");
        const end_s = std.mem.trim(u8, inner[ci + 1 ..], " \t\r\n");
        // range(start, end): count = end - start
        if (std.mem.eql(u8, start_s, "0")) {
            return .{ .count = try allocator.dupe(u8, end_s), .start = null };
        }
        // Require both to be integer literals so we can emit "numbers(count)"
        const start_int = std.fmt.parseInt(i64, start_s, 10) catch return null;
        const end_int = std.fmt.parseInt(i64, end_s, 10) catch return null;
        if (end_int <= start_int) return null;
        const count_text = try std.fmt.allocPrint(allocator, "{d}", .{end_int - start_int});
        return .{ .count = count_text, .start = try allocator.dupe(u8, start_s) };
    }
    return .{ .count = try allocator.dupe(u8, inner), .start = null };
}

/// Replace all word-boundary occurrences of `old` with `new` in `src`.
fn replaceTokenAll(allocator: std.mem.Allocator, src: []const u8, old: []const u8, new: []const u8) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(allocator);
    var i: usize = 0;
    while (i < src.len) {
        if (i + old.len <= src.len and std.ascii.eqlIgnoreCase(src[i .. i + old.len], old)) {
            const before_ok = i == 0 or !isIdent(src[i - 1]);
            const after_ok = i + old.len >= src.len or !isIdent(src[i + old.len]);
            if (before_ok and after_ok) {
                try buf.appendSlice(allocator, new);
                i += old.len;
                continue;
            }
        }
        try buf.append(allocator, src[i]);
        i += 1;
    }
    return buf.toOwnedSlice(allocator);
}

fn rewriteArrayJoin(allocator: std.mem.Allocator, sql: []const u8) !?[]u8 {
    // Find " ARRAY JOIN " position
    const aj_pos = std.ascii.indexOfIgnoreCase(sql, " ARRAY JOIN ") orelse return null;
    // Find the FROM keyword position (must come before ARRAY JOIN)
    const from_pos = std.ascii.indexOfIgnoreCase(sql, " FROM ") orelse return null;
    if (from_pos >= aj_pos) return null;

    // Split the SQL into parts:
    // pre_select = everything up to (but not including) " FROM "
    // table_part = from " FROM " up to " ARRAY JOIN "
    // aj_clause  = after " ARRAY JOIN " until WHERE/LIMIT/ORDER/GROUP or end
    const pre_select = sql[0..from_pos];
    const after_from = sql[from_pos..aj_pos]; // includes " FROM tablename"
    const aj_clause_start = aj_pos + 12; // len(" ARRAY JOIN ") == 12
    const aj_rest = sql[aj_clause_start..];

    // ── Fast path ──────────────────────────────────────────────────────────────
    // Pattern: SELECT ... FROM system.one ARRAY JOIN range(N) AS alias [suffix]
    // Rewrite to: SELECT ...(alias→number)... FROM numbers(N) [suffix]
    // This allows DuckDB to handle aggregation natively over N rows.
    {
        const from_table = std.mem.trim(u8, after_from[6..], " \t\r\n"); // strip " FROM "
        if (isSystemOne(from_table)) {
            // Find suffix (WHERE/ORDER/GROUP/LIMIT/HAVING)
            var aj_end2: usize = aj_rest.len;
            for ([_][]const u8{ " WHERE ", " ORDER ", " GROUP ", " LIMIT ", " HAVING " }) |kw| {
                if (std.ascii.indexOfIgnoreCase(aj_rest, kw)) |p| {
                    if (p < aj_end2) aj_end2 = p;
                }
            }
            const aj_clause2 = aj_rest[0..aj_end2];
            const suffix2 = aj_rest[aj_end2..];

            // Parse ARRAY JOIN items — require all are range(N) AS alias
            var items2: std.ArrayListUnmanaged(struct { alias: []const u8, count: []const u8, offset: ?[]const u8 }) = .empty;
            defer items2.deinit(allocator);
            var valid = true;
            {
                var depth: usize = 0;
                var start: usize = 0;
                for (aj_clause2, 0..) |ch, ci| {
                    if (ch == '(' or ch == '[') depth += 1;
                    if (ch == ')' or ch == ']') { if (depth > 0) depth -= 1; }
                    if (ch == ',' and depth == 0) {
                        const item = std.mem.trim(u8, aj_clause2[start..ci], " \t\r\n");
                        const as_p = std.ascii.indexOfIgnoreCase(item, " AS ") orelse { valid = false; break; };
                        const expr = std.mem.trim(u8, item[0..as_p], " \t\r\n");
                        const alias = std.mem.trim(u8, item[as_p + 4..], " \t\r\n");
                        const rargs = (try extractRangeArgs(allocator, expr)) orelse { valid = false; break; };
                        try items2.append(allocator, .{ .alias = alias, .count = rargs.count, .offset = rargs.start });
                        start = ci + 1;
                    }
                }
                if (valid) {
                    const last_item = std.mem.trim(u8, aj_clause2[if (items2.items.len == 0) 0 else blk: {
                        // find last comma pos
                        var d2: usize = 0;
                        var lc: usize = 0;
                        for (aj_clause2, 0..) |ch, ci| {
                            if (ch == '(' or ch == '[') d2 += 1;
                            if (ch == ')' or ch == ']') { if (d2 > 0) d2 -= 1; }
                            if (ch == ',' and d2 == 0) lc = ci + 1;
                        }
                        break :blk lc;
                    }..], " \t\r\n");
                    const as_p2 = std.ascii.indexOfIgnoreCase(last_item, " AS ");
                    if (as_p2 == null) {
                        valid = false;
                    } else {
                        const expr2 = std.mem.trim(u8, last_item[0..as_p2.?], " \t\r\n");
                        const alias2 = std.mem.trim(u8, last_item[as_p2.? + 4..], " \t\r\n");
                        const rargs2 = try extractRangeArgs(allocator, expr2);
                        if (rargs2 == null) valid = false
                        else try items2.append(allocator, .{ .alias = alias2, .count = rargs2.?.count, .offset = rargs2.?.start });
                    }
                }
            }

            if (valid and items2.items.len == 1) {
                // Single range alias — rewrite to numbers(count)
                // with alias replaced by (number + offset) if offset != null, else just "number"
                const alias = items2.items[0].alias;
                const count_tok = items2.items[0].count;
                const offset_tok = items2.items[0].offset;

                // Replace alias → "number" (or "(number + offset)") in the SELECT list
                const pre_select_trimmed = std.mem.trim(u8, pre_select, " \t\r\n");
                const replacement: []const u8 = if (offset_tok) |off|
                    try std.fmt.allocPrint(allocator, "(number + {s})", .{off})
                else
                    try allocator.dupe(u8, "number");
                defer allocator.free(replacement);
                const replaced_select = try replaceTokenAll(allocator, pre_select_trimmed, alias, replacement);
                defer allocator.free(replaced_select);

                var out2: std.ArrayListUnmanaged(u8) = .empty;
                errdefer out2.deinit(allocator);
                try out2.appendSlice(allocator, replaced_select);
                try out2.appendSlice(allocator, " FROM numbers(");
                try out2.appendSlice(allocator, count_tok);
                try out2.append(allocator, ')');
                try out2.appendSlice(allocator, suffix2);
                // Free allocator-owned count_tok and offset_tok
                allocator.free(count_tok);
                if (offset_tok) |off| allocator.free(off);
                return try out2.toOwnedSlice(allocator);
            }
            // Free any allocated items before falling through
            for (items2.items) |it| {
                allocator.free(it.count);
                if (it.offset) |off| allocator.free(off);
            }
        }
    }
    // ── End fast path ──────────────────────────────────────────────────────────

    // Find end of ARRAY JOIN clause: WHERE, ORDER, GROUP, LIMIT, or end of string
    var aj_end: usize = aj_rest.len;
    for ([_][]const u8{ " WHERE ", " ORDER ", " GROUP ", " LIMIT ", " HAVING " }) |kw| {
        if (std.ascii.indexOfIgnoreCase(aj_rest, kw)) |p| {
            if (p < aj_end) aj_end = p;
        }
    }
    const aj_clause = aj_rest[0..aj_end];
    const suffix = aj_rest[aj_end..]; // WHERE ... or empty

    // Parse ARRAY JOIN items: "expr1 AS alias1, expr2 AS alias2"
    // Split by top-level comma
    var items: std.ArrayListUnmanaged([]const u8) = .empty;
    defer items.deinit(allocator);
    {
        var depth: usize = 0;
        var start: usize = 0;
        for (aj_clause, 0..) |ch, ci| {
            if (ch == '(' or ch == '[') depth += 1;
            if (ch == ')' or ch == ']') { if (depth > 0) depth -= 1; }
            if (ch == ',' and depth == 0) {
                try items.append(allocator, std.mem.trim(u8, aj_clause[start..ci], " \t\r\n"));
                start = ci + 1;
            }
        }
        try items.append(allocator, std.mem.trim(u8, aj_clause[start..], " \t\r\n"));
    }

    // Build mapping: alias → arrayJoin(expr)
    const AliasMap = struct { alias: []const u8, expr: []const u8 };
    var alias_map: std.ArrayListUnmanaged(AliasMap) = .empty;
    defer alias_map.deinit(allocator);

    for (items.items) |item| {
        if (std.ascii.indexOfIgnoreCase(item, " AS ")) |as_pos| {
            const expr = std.mem.trim(u8, item[0..as_pos], " \t\r\n");
            const alias = std.mem.trim(u8, item[as_pos + 4..], " \t\r\n");
            try alias_map.append(allocator, .{ .alias = alias, .expr = expr });
        } else {
            // No AS: the alias is the same as the expression (e.g. "ARRAY JOIN arr" means arr AS arr)
            const expr = std.mem.trim(u8, item, " \t\r\n");
            if (expr.len > 0) try alias_map.append(allocator, .{ .alias = expr, .expr = expr });
        }
    }

    // Parse original SELECT list: "SELECT col1, col2" → ["col1", "col2"]
    const select_kw = "SELECT ";
    const pre_select_trimmed = std.mem.trim(u8, pre_select, " \t\r\n");
    if (!std.ascii.startsWithIgnoreCase(pre_select_trimmed, select_kw)) return null;
    const after_select = pre_select_trimmed[select_kw.len..];

    // Split SELECT columns by top-level comma
    var sel_cols: std.ArrayListUnmanaged([]const u8) = .empty;
    defer sel_cols.deinit(allocator);
    {
        var depth2: usize = 0;
        var start2: usize = 0;
        for (after_select, 0..) |ch, ci| {
            if (ch == '(' or ch == '[') depth2 += 1;
            if (ch == ')' or ch == ']') { if (depth2 > 0) depth2 -= 1; }
            if (ch == ',' and depth2 == 0) {
                try sel_cols.append(allocator, std.mem.trim(u8, after_select[start2..ci], " \t\r\n"));
                start2 = ci + 1;
            }
        }
        try sel_cols.append(allocator, std.mem.trim(u8, after_select[start2..], " \t\r\n"));
    }

    // Build final SELECT list.
    // For each SELECT column, check if it is (or starts with) an ARRAY JOIN alias.
    // Cases:
    //   "alias"          → replace with "arrayJoin(expr) AS alias"
    //   "alias AS other" → replace with "arrayJoin(expr) AS other"
    //   "fn(alias, ...)" or anything containing the alias inside an expression
    //                    → rewrite alias token inside the expression (token replacement)
    // Columns that are not aliases are emitted unchanged.
    // After processing SELECT columns, we do NOT append extra arrayJoin expressions;
    // instead all arrayJoin substitution happens inline.
    // Track which aliases were used as top-level SELECT columns.
    var alias_used = try allocator.alloc(bool, alias_map.items.len);
    defer allocator.free(alias_used);
    @memset(alias_used, false);

    var new_select: std.ArrayListUnmanaged(u8) = .empty;
    defer new_select.deinit(allocator);
    try new_select.appendSlice(allocator, "SELECT ");
    var first_col = true;

    for (sel_cols.items) |scol| {
        if (!first_col) try new_select.appendSlice(allocator, ", ");
        first_col = false;

        // Check if scol is exactly an alias or "alias AS other"
        var replaced = false;
        for (alias_map.items, 0..) |am, ai| {
            // Case 1: exact match "alias"
            if (std.ascii.eqlIgnoreCase(scol, am.alias)) {
                try new_select.appendSlice(allocator, "arrayJoin(");
                try new_select.appendSlice(allocator, am.expr);
                try new_select.appendSlice(allocator, ") AS ");
                try new_select.appendSlice(allocator, am.alias);
                alias_used[ai] = true;
                replaced = true;
                break;
            }
            // Case 2: "alias AS other" — replace leading alias with arrayJoin(expr)
            if (scol.len > am.alias.len + 4) {
                const prefix = scol[0..am.alias.len];
                const rest = scol[am.alias.len..];
                if (std.ascii.eqlIgnoreCase(prefix, am.alias) and
                    std.ascii.startsWithIgnoreCase(rest, " AS ")) {
                    try new_select.appendSlice(allocator, "arrayJoin(");
                    try new_select.appendSlice(allocator, am.expr);
                    try new_select.append(allocator, ')');
                    try new_select.appendSlice(allocator, rest); // " AS other"
                    alias_used[ai] = true;
                    replaced = true;
                    break;
                }
            }
        }
        if (!replaced) {
            // The column may contain ARRAY JOIN aliases inside expressions
            // (e.g. "avg(fv) AS avg_val" where "fv" is an alias).
            // Rewrite each such alias token to "__aj__<alias>" so the planner
            // can find the hidden arrayJoin expansion column.
            var rewritten: std.ArrayListUnmanaged(u8) = .empty;
            defer rewritten.deinit(allocator);
            var ri: usize = 0;
            while (ri < scol.len) {
                var matched_alias = false;
                for (alias_map.items, 0..) |am, ai| {
                    if (alias_used[ai]) continue; // top-level aliases already handled
                    const al = am.alias;
                    if (ri + al.len > scol.len) continue;
                    if (!std.ascii.eqlIgnoreCase(scol[ri .. ri + al.len], al)) continue;
                    // Check word boundaries
                    const before_ok = ri == 0 or !isIdent(scol[ri - 1]);
                    const after_ok = ri + al.len >= scol.len or !isIdent(scol[ri + al.len]);
                    if (!before_ok or !after_ok) continue;
                    try rewritten.appendSlice(allocator, "__aj__");
                    try rewritten.appendSlice(allocator, al);
                    ri += al.len;
                    matched_alias = true;
                    break;
                }
                if (!matched_alias) {
                    try rewritten.append(allocator, scol[ri]);
                    ri += 1;
                }
            }
            if (rewritten.items.len > 0) {
                try new_select.appendSlice(allocator, rewritten.items);
            } else {
                try new_select.appendSlice(allocator, scol);
            }
        }
    }

    // Append arrayJoin(expr) AS alias for any ARRAY JOIN alias that did NOT appear
    // as a top-level SELECT column. This makes the alias available to aggregate
    // expressions like avg(fv) that reference it inside a function argument.
    // Prefix with "__aj__" to mark as hidden (not emitted in output).
    for (alias_map.items, 0..) |am, ai| {
        if (!alias_used[ai]) {
            try new_select.appendSlice(allocator, ", arrayJoin(");
            try new_select.appendSlice(allocator, am.expr);
            try new_select.appendSlice(allocator, ") AS __aj__");
            try new_select.appendSlice(allocator, am.alias);
        }
    }

    // alias_map was consumed above by inline substitution in the SELECT list.

    // Rebuild: new_select + after_from + suffix
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, new_select.items);
    try out.appendSlice(allocator, after_from);
    try out.appendSlice(allocator, suffix);
    return try out.toOwnedSlice(allocator);
}

/// Rewrite CH grammar so DuckDB can parse it.
/// Returns an owned slice (caller frees), or null if the query is unsupported
/// (currently: ARRAY JOIN).
/// Collapse runs of whitespace (space, tab, CR, LF) into a single space and trim ends.
/// Always returns a freshly allocated slice.
fn normalizeWhitespace(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql.len);
    errdefer buf.deinit(allocator);
    var in_ws = true; // start true to trim leading whitespace
    for (sql) |c| {
        const ws = c == ' ' or c == '\t' or c == '\r' or c == '\n';
        if (ws) {
            if (!in_ws) try buf.append(allocator, ' ');
            in_ws = true;
        } else {
            try buf.append(allocator, c);
            in_ws = false;
        }
    }
    // Trim trailing space if any.
    if (buf.items.len > 0 and buf.items[buf.items.len - 1] == ' ')
        buf.items.len -= 1;
    return buf.toOwnedSlice(allocator);
}

/// Rewrite ClickHouse "expr AS alias" let-binding inside function arguments.
/// Example: `abs(number - 10 as x) = (x < 0 ? -x : x)`
///       → `abs((number - 10)) = ((number - 10) < 0 ? -(number - 10) : (number - 10))`
/// Only handles the simple case where a single `AS ident` appears at the top level
/// of a function's argument list and `ident` appears elsewhere in the expression.
fn rewriteFuncArgAlias(allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
    // Quick scan: if no " as " (case-insensitive), nothing to do.
    if (std.ascii.indexOfIgnoreCase(sql, " as ") == null) return sql;

    // We scan for patterns: <funcname>(<expr> AS <ident>)
    // where <expr> contains no top-level comma (single-arg function).
    var out: std.ArrayListUnmanaged(u8) = .empty;
    try out.ensureTotalCapacity(allocator, sql.len * 2);
    errdefer out.deinit(allocator);

    var pos: usize = 0;
    while (pos < sql.len) {
        // Find " as " (case-insensitive) scanning forward
        const as_pos = blk: {
            var p = pos;
            while (p + 4 <= sql.len) : (p += 1) {
                if (std.ascii.startsWithIgnoreCase(sql[p..], " as ") and (p == 0 or isIdent(sql[p - 1]) or sql[p-1] == ')')) break :blk p;
            }
            break :blk null;
        } orelse {
            try out.appendSlice(allocator, sql[pos..]);
            break;
        };

        // Check if the character after " as " starts an identifier
        const alias_start = as_pos + 4;
        if (alias_start >= sql.len or !isIdentStart(sql[alias_start])) {
            try out.appendSlice(allocator, sql[pos..alias_start]);
            pos = alias_start;
            continue;
        }
        // Find end of alias identifier
        var alias_end = alias_start;
        while (alias_end < sql.len and isIdent(sql[alias_end])) alias_end += 1;
        const alias = sql[alias_start..alias_end];

        // Now backtrack from as_pos to find the opening '(' of the function call
        // that contains this AS expression. Walk backwards tracking depth.
        const open_paren = blk: {
            var depth: usize = 0;
            var p = as_pos;
            while (p > pos) {
                p -= 1;
                if (sql[p] == ')') depth += 1
                else if (sql[p] == '(') {
                    if (depth == 0) break :blk p;
                    depth -= 1;
                }
            }
            break :blk null;
        } orelse {
            // No matching open paren, copy up to alias_end and continue
            try out.appendSlice(allocator, sql[pos..alias_end]);
            pos = alias_end;
            continue;
        };

        // The expression inside is sql[open_paren+1..as_pos]
        const inner_expr = std.mem.trim(u8, sql[open_paren + 1..as_pos], " \t");

        // Ensure the '(' is from a function call (char before it must be ident char)
        // and that inner_expr is not a subquery (no SELECT keyword).
        if (open_paren == 0 or !isIdent(sql[open_paren - 1]) or
            std.ascii.indexOfIgnoreCase(inner_expr, "SELECT") != null)
        {
            try out.appendSlice(allocator, sql[pos..alias_end]);
            pos = alias_end;
            continue;
        }

        // After alias_end: expect ')' closing the function call
        if (alias_end >= sql.len or sql[alias_end] != ')') {
            try out.appendSlice(allocator, sql[pos..alias_end]);
            pos = alias_end;
            continue;
        }
        const close_paren = alias_end; // the ')' that closes the function call

        // Copy everything up to (but not including) the function's open paren
        try out.appendSlice(allocator, sql[pos..open_paren]);
        // Rewrite: funcname((inner_expr))
        try out.append(allocator, '(');
        try out.append(allocator, '(');
        try out.appendSlice(allocator, inner_expr);
        try out.append(allocator, ')');
        try out.append(allocator, ')');

        // Now scan the rest of the sql (after close_paren+1), replacing alias with (inner_expr)
        // but only until end of current expression context (tricky — just do the full remainder)
        const rest_start = close_paren + 1;
        const rest = sql[rest_start..];
        // Replace whole-word occurrences of alias in rest
        var rp: usize = 0;
        while (rp < rest.len) {
            // Check for alias at word boundary
            if (rp + alias.len <= rest.len and
                std.mem.eql(u8, rest[rp..rp + alias.len], alias) and
                (rp == 0 or !isIdent(rest[rp - 1])) and
                (rp + alias.len >= rest.len or !isIdent(rest[rp + alias.len])))
            {
                try out.append(allocator, '(');
                try out.appendSlice(allocator, inner_expr);
                try out.append(allocator, ')');
                rp += alias.len;
            } else {
                try out.append(allocator, rest[rp]);
                rp += 1;
            }
        }
        pos = sql.len; // done
        break;
    }

    const result = try out.toOwnedSlice(allocator);
    if (std.mem.eql(u8, result, sql)) {
        allocator.free(result);
        return sql;
    }
    return result;
}

fn isIdentStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_';
}

/// Rewrite CH ternary `(cond ? then_expr : else_expr)` → `if(cond, then_expr, else_expr)`.
/// Only handles ternaries enclosed in parentheses to avoid ambiguity.
fn rewriteTernary(allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
    if (std.mem.indexOf(u8, sql, "?") == null) return sql;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql.len + 16);
    errdefer buf.deinit(allocator);

    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        // Skip string literals
        if (ch == '\'') {
            try buf.append(allocator, ch);
            i += 1;
            while (i < sql.len and sql[i] != '\'') { try buf.append(allocator, sql[i]); i += 1; }
            if (i < sql.len) { try buf.append(allocator, sql[i]); i += 1; }
            continue;
        }
        if (ch != '(') {
            try buf.append(allocator, ch);
            i += 1;
            continue;
        }
        // Found '(' — check if the content is `cond ? then : else`
        // Find the matching ')'
        var close: usize = 0;
        {
            var depth: usize = 0;
            var j = i;
            while (j < sql.len) : (j += 1) {
                if (sql[j] == '(') depth += 1
                else if (sql[j] == ')') {
                    depth -= 1;
                    if (depth == 0) { close = j; break; }
                }
            }
        }
        if (close == 0) { try buf.append(allocator, ch); i += 1; continue; }

        const inner = sql[i + 1..close];
        // Find '?' at depth 0 inside inner, skipping string literals
        const q_pos = blk: {
            var depth: usize = 0;
            var idx: usize = 0;
            while (idx < inner.len) : (idx += 1) {
                const c = inner[idx];
                if (c == '\'') {
                    // Skip string literal
                    idx += 1;
                    while (idx < inner.len and inner[idx] != '\'') idx += 1;
                    // idx now points at closing '\'' or end; loop increment will advance past it
                    continue;
                }
                if (c == '(' or c == '[') depth += 1
                else if (c == ')' or c == ']') { if (depth > 0) depth -= 1; }
                else if (c == '?' and depth == 0) break :blk idx;
            }
            break :blk null;
        } orelse {
            // No ternary — recursively rewrite the inner content
            try buf.append(allocator, '(');
            const inner_rw = try rewriteTernary(allocator, inner);
            defer if (inner_rw.ptr != inner.ptr) allocator.free(inner_rw);
            try buf.appendSlice(allocator, inner_rw);
            try buf.append(allocator, ')');
            i = close + 1;
            continue;
        };

        const cond = std.mem.trim(u8, inner[0..q_pos], " \t");
        // Find ':' at depth 0 after '?', skipping string literals
        const colon_pos = blk: {
            var depth: usize = 0;
            var j = q_pos + 1;
            while (j < inner.len) : (j += 1) {
                const c = inner[j];
                if (c == '\'') {
                    j += 1;
                    while (j < inner.len and inner[j] != '\'') j += 1;
                    continue;
                }
                if (c == '(' or c == '[') depth += 1
                else if (c == ')' or c == ']') { if (depth > 0) depth -= 1; }
                else if (c == ':' and depth == 0) break :blk j;
            }
            break :blk null;
        } orelse {
            try buf.append(allocator, '(');
            try buf.appendSlice(allocator, inner);
            try buf.append(allocator, ')');
            i = close + 1;
            continue;
        };

        const then_expr = std.mem.trim(u8, inner[q_pos + 1..colon_pos], " \t");
        const else_expr = std.mem.trim(u8, inner[colon_pos + 1..], " \t");

        // Recursively rewrite each part
        const cond_rw = try rewriteTernary(allocator, cond);
        defer if (cond_rw.ptr != cond.ptr) allocator.free(cond_rw);
        const then_rw = try rewriteTernary(allocator, then_expr);
        defer if (then_rw.ptr != then_expr.ptr) allocator.free(then_rw);
        const else_rw = try rewriteTernary(allocator, else_expr);
        defer if (else_rw.ptr != else_expr.ptr) allocator.free(else_rw);

        try buf.appendSlice(allocator, "if(");
        try buf.appendSlice(allocator, cond_rw);
        try buf.append(allocator, ',');
        try buf.appendSlice(allocator, then_rw);
        try buf.append(allocator, ',');
        try buf.appendSlice(allocator, else_rw);
        try buf.append(allocator, ')');
        i = close + 1;
    }

    const result = try buf.toOwnedSlice(allocator);
    if (std.mem.eql(u8, result, sql)) {
        allocator.free(result);
        return sql;
    }
    return result;
}

pub fn rewrite(allocator: std.mem.Allocator, sql: []const u8) !?[]u8 {
    // Normalize whitespace: collapse runs of \t\r\n and multiple spaces into a
    // single space so that keyword searches like " ARRAY JOIN " work regardless
    // of indentation or newlines in the incoming SQL.
    const norm_raw = try normalizeWhitespace(allocator, sql);
    defer allocator.free(norm_raw);

    // Strip trailing ClickHouse SETTINGS clause: "SETTINGS key = val, ..."
    // This clause is not valid SQL and DuckDB cannot parse it.
    const norm = blk: {
        const settings_kw = " SETTINGS ";
        if (std.ascii.indexOfIgnoreCase(norm_raw, settings_kw)) |pos| {
            // Only strip if SETTINGS is not inside parentheses (i.e. top-level).
            var depth: usize = 0;
            var in_str: bool = false;
            var top_pos: ?usize = null;
            var j: usize = 0;
            while (j < norm_raw.len) : (j += 1) {
                const c = norm_raw[j];
                if (in_str) {
                    if (c == '\'') in_str = false;
                    continue;
                }
                if (c == '\'') { in_str = true; continue; }
                if (c == '(') { depth += 1; continue; }
                if (c == ')') { if (depth > 0) depth -= 1; continue; }
                if (depth == 0 and j >= pos and
                    std.ascii.startsWithIgnoreCase(norm_raw[j..], settings_kw[1..]))
                {
                    top_pos = j;
                    break;
                }
            }
            if (top_pos) |tp| {
                const trimmed = std.mem.trimEnd(u8, norm_raw[0..tp], " \t");
                break :blk try allocator.dupe(u8, trimmed);
            }
        }
        break :blk try allocator.dupe(u8, norm_raw);
    };
    defer allocator.free(norm);

    // Rewrite hex literals 0x... → decimal, outside of string literals.
    const after_hex = blk: {
        if (std.ascii.indexOfIgnoreCase(norm, "0x") == null and
            std.ascii.indexOfIgnoreCase(norm, "0X") == null) break :blk try allocator.dupe(u8, norm);
        var out: std.ArrayListUnmanaged(u8) = .empty;
        var i: usize = 0;
        var in_str: bool = false;
        while (i < norm.len) {
            const c = norm[i];
            if (in_str) {
                try out.append(allocator, c);
                if (c == '\'') in_str = false;
                i += 1;
                continue;
            }
            if (c == '\'') { in_str = true; try out.append(allocator, c); i += 1; continue; }
            // Detect 0x or 0X prefix
            if (c == '0' and i + 1 < norm.len and (norm[i+1] == 'x' or norm[i+1] == 'X')) {
                // Scan hex digits (and maybe 'p' for float hex like 0x1p4)
                var j = i + 2;
                while (j < norm.len and (std.ascii.isHex(norm[j]) or norm[j] == 'p' or norm[j] == 'P' or norm[j] == '+' or norm[j] == '-')) j += 1;
                const hex_str = norm[i+2..j];
                // If it contains 'p'/'P', it's a hex float like 0x1p4 — convert to decimal float
                const has_p = std.ascii.indexOfIgnoreCase(hex_str, "p") != null;
                if (!has_p and hex_str.len > 0) {
                    // Pure hex integer: parse and emit decimal
                    if (std.fmt.parseInt(u128, hex_str, 16) catch null) |v| {
                        if (v > std.math.maxInt(u64)) {
                            // Too large for UBIGINT — emit as float
                            const fv: f64 = @floatFromInt(v);
                            const s = try std.fmt.allocPrint(allocator, "{d}", .{fv});
                            defer allocator.free(s);
                            try out.appendSlice(allocator, s);
                        } else if (v > std.math.maxInt(i64)) {
                            // Emit as UBIGINT cast so DuckDB can handle it
                            const s = try std.fmt.allocPrint(allocator, "CAST({d} AS UBIGINT)", .{v});
                            defer allocator.free(s);
                            try out.appendSlice(allocator, s);
                        } else {
                            const s = try std.fmt.allocPrint(allocator, "{d}", .{v});
                            defer allocator.free(s);
                            try out.appendSlice(allocator, s);
                        }
                        i = j;
                        continue;
                    }
                    // Too large for u128: emit as float
                    if (std.fmt.parseFloat(f64, norm[i..j]) catch null) |fv| {
                        const s = try std.fmt.allocPrint(allocator, "{d}", .{fv});
                        defer allocator.free(s);
                        try out.appendSlice(allocator, s);
                        i = j;
                        continue;
                    }
                }
                if (has_p) {
                    // Hex float like 0x1p4 or 0x1P1023: convert via float parse
                    if (std.fmt.parseFloat(f64, norm[i..j]) catch null) |fv| {
                        const s = try std.fmt.allocPrint(allocator, "{d}", .{fv});
                        defer allocator.free(s);
                        try out.appendSlice(allocator, s);
                        i = j;
                        continue;
                    }
                }
                // Fallthrough: pass through unchanged
                try out.appendSlice(allocator, norm[i..j]);
                i = j;
                continue;
            }
            try out.append(allocator, c);
            i += 1;
        }
        break :blk try out.toOwnedSlice(allocator);
    };
    defer allocator.free(after_hex);

    // Rewrite count() → count(*): ClickHouse allows bare count() but DuckDB requires count(*).
    const after_count: []const u8 = blk: {
        if (std.ascii.indexOfIgnoreCase(after_hex, "count()") != null) {
            const r1 = try std.mem.replaceOwned(u8, allocator, after_hex, "count()", "count(*)");
            errdefer allocator.free(r1);
            const r2 = try std.mem.replaceOwned(u8, allocator, r1, "COUNT()", "count(*)");
            allocator.free(r1);
            break :blk r2;
        }
        break :blk try allocator.dupe(u8, after_hex);
    };
    defer allocator.free(after_count);

    // If ARRAY JOIN is present, rewrite it first, then fall through to apply
    // function-rename rules (e.g. toDateTime→CAST) on the result.
    var after_aj: ?[]u8 = null;
    if (std.ascii.indexOfIgnoreCase(after_count, "ARRAY JOIN") != null) {
        after_aj = try rewriteArrayJoin(allocator, after_count) orelse return null;
    }
    // base points to either the ARRAY JOIN-rewritten SQL or the normalized SQL.
    // We dupe after_aj immediately so we can free it while base2 lives on.
    const base: []const u8 = if (after_aj) |s| s else after_count;

    // Pre-pass: rewrite CAST(expr, 'TypeName') → CAST(expr AS TypeName).
    // When after_aj is set we dupe base so sql2 and after_aj never alias,
    // preventing double-free in the defers below.
    const base2: []const u8 = if (after_aj != null) try allocator.dupe(u8, base) else base;
    if (after_aj) |s| allocator.free(s);  // safe to free now; base2 is a fresh copy
    const sql2 = try rewriteCastStringType(allocator, base2);
    defer if (sql2.ptr != base2.ptr) allocator.free(sql2);
    defer if (base2.ptr != after_count.ptr) allocator.free(base2);

    // Rewrite "expr AS alias" inside function arguments (CH let-binding syntax).
    // Pattern: funcname(lhs AS varname) used-in-outer-expr
    // → funcname((lhs)) with varname replaced by (lhs) in outer expression.
    const sql2b = try rewriteFuncArgAlias(allocator, sql2);
    defer if (sql2b.ptr != sql2.ptr) allocator.free(sql2b);

    // Rewrite CH ternary `cond ? then : else` → `if(cond, then, else)`
    const sql2c = try rewriteTernary(allocator, sql2b);
    defer if (sql2c.ptr != sql2b.ptr) allocator.free(sql2c);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql2c.len + 64);
    errdefer buf.deinit(allocator);

    var i: usize = 0;
    while (i < sql2c.len) {
        // Only attempt rewrites at word boundaries.
        const wb = i == 0 or !isIdent(sql2c[i - 1]);
        if (wb) {
            const matched = inline for (rules) |rule| {
                if (matchFn(sql2c, i, rule.name)) |r| {
                    switch (rule.kind) {
                        .multiif    => { i = try rwMultiIf(allocator, &buf, sql2c, r);              break true; },
                        .cast       => { i = try rwCast(allocator, &buf, sql2c, r, rule.param);     break true; },
                        .date_trunc => { i = try rwTrunc(allocator, &buf, sql2c, r, rule.param);    break true; },
                        .rename     => { try buf.appendSlice(allocator, rule.param); i = r;       break true; },
                    }
                }
            } else false;
            if (matched) continue;
        }
        try buf.append(allocator, sql2c[i]);
        i += 1;
    }
    return @as(?[]u8, try buf.toOwnedSlice(allocator));
}

/// Rewrite CAST(expr, 'TypeName') → CAST(expr AS TypeName) (ClickHouse alternate syntax).
fn rewriteCastStringType(allocator: std.mem.Allocator, sql: []const u8) ![]const u8 {
    if (std.ascii.indexOfIgnoreCase(sql, "CAST(") == null and
        std.ascii.indexOfIgnoreCase(sql, "CAST (") == null) return sql;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql.len + 16);
    errdefer buf.deinit(allocator);
    var i: usize = 0;
    while (i < sql.len) {
        const wb = i == 0 or !isIdent(sql[i - 1]);
        if (wb) {
            if (matchFn(sql, i, "CAST")) |after_paren| {
            // Find the closing paren
            const close = findClose(sql, after_paren) orelse {
                try buf.appendSlice(allocator, sql[i..]);
                break;
            };
            const inner = sql[after_paren..close];
            // Split args — if exactly 2 args and second is a string literal, rewrite
            const args = try splitArgs(allocator, inner, 0, inner.len);
            defer allocator.free(args);
            if (args.len == 2) {
                const type_arg = std.mem.trim(u8, args[1], " \t\r\n");
                if (type_arg.len >= 2 and type_arg[0] == '\'' and type_arg[type_arg.len - 1] == '\'') {
                    const type_name = type_arg[1..type_arg.len - 1];
                    const duckdb_type = chTypeToDuckdb(type_name);
                    try buf.appendSlice(allocator, "CAST(");
                    try buf.appendSlice(allocator, std.mem.trim(u8, args[0], " \t\r\n"));
                    try buf.appendSlice(allocator, " AS ");
                    try buf.appendSlice(allocator, duckdb_type);
                    try buf.append(allocator, ')');
                    i = close + 1;
                    continue;
                }
            }
            // No rewrite — copy CAST( as-is
            try buf.appendSlice(allocator, "CAST(");
            i = after_paren;
            continue;
            }
        }
        try buf.append(allocator, sql[i]);
        i += 1;
    }
    return buf.toOwnedSlice(allocator);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn isIdent(ch: u8) bool { return std.ascii.isAlphanumeric(ch) or ch == '_'; }

/// Match `name(` at sql[i] (word-boundary already checked by caller).
/// Returns the index after '(', or null.
fn matchFn(sql: []const u8, i: usize, name: []const u8) ?usize {
    if (i + name.len > sql.len) return null;
    if (!std.ascii.eqlIgnoreCase(sql[i .. i + name.len], name)) return null;
    var j = i + name.len;
    while (j < sql.len and sql[j] == ' ') j += 1;
    if (j >= sql.len or sql[j] != '(') return null;
    return j + 1;
}

/// Find matching ')' starting at `start` (depth already 1). Returns index of ')'.
fn findClose(sql: []const u8, start: usize) ?usize {
    var depth: usize = 1;
    var k = start;
    while (k < sql.len) : (k += 1) {
        switch (sql[k]) {
            '(' => depth += 1,
            ')' => { depth -= 1; if (depth == 0) return k; },
            '\'' => { k += 1; while (k < sql.len and sql[k] != '\'') k += 1; },
            else => {},
        }
    }
    return null;
}

/// Split top-level comma-separated args within sql[start..close].
fn splitArgs(alloc: std.mem.Allocator, sql: []const u8, start: usize, close: usize) ![][]const u8 {
    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    var depth: usize = 0;
    var s = start;
    var k = start;
    while (k < close) : (k += 1) {
        switch (sql[k]) {
            '(', '[' => depth += 1,
            ')', ']' => depth -= 1,
            '\'' => { k += 1; while (k < close and sql[k] != '\'') k += 1; },
            ',' => if (depth == 0) {
                try args.append(alloc, std.mem.trim(u8, sql[s..k], " \t\r\n"));
                s = k + 1;
            },
            else => {},
        }
    }
    try args.append(alloc, std.mem.trim(u8, sql[s..close], " \t\r\n"));
    return args.toOwnedSlice(alloc);
}

// ── Structural rewriters ──────────────────────────────────────────────────────

/// multiIf(c1,v1,...,else) → CASE WHEN c1 THEN v1 … ELSE else END
fn rwMultiIf(alloc: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), sql: []const u8, start: usize) !usize {
    const close = findClose(sql, start) orelse { try buf.appendSlice(alloc, "multiIf("); return start; };
    const args = try splitArgs(alloc, sql, start, close);
    defer alloc.free(args);
    try buf.appendSlice(alloc, "CASE");
    var k: usize = 0;
    while (k + 1 < args.len) : (k += 2) {
        try buf.appendSlice(alloc, " WHEN "); try buf.appendSlice(alloc, args[k]);
        try buf.appendSlice(alloc, " THEN "); try buf.appendSlice(alloc, args[k + 1]);
    }
    if (k < args.len) { try buf.appendSlice(alloc, " ELSE "); try buf.appendSlice(alloc, args[k]); }
    try buf.appendSlice(alloc, " END");
    return close + 1;
}

/// toString(x)→CAST(x AS VARCHAR), toDate(x)→CAST(x AS DATE), etc.
fn rwCast(alloc: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), sql: []const u8, start: usize, suffix: []const u8) !usize {
    const close = findClose(sql, start) orelse { try buf.appendSlice(alloc, "CAST("); return start; };
    try buf.appendSlice(alloc, "CAST(");
    try buf.appendSlice(alloc, sql[start..close]);
    try buf.append(alloc, ' ');
    try buf.appendSlice(alloc, suffix);
    try buf.append(alloc, ')');
    return close + 1;
}

/// toStartOfHour(x)→date_trunc('hour', x), etc.
fn rwTrunc(alloc: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), sql: []const u8, start: usize, unit: []const u8) !usize {
    const close = findClose(sql, start) orelse { try buf.appendSlice(alloc, "date_trunc("); return start; };
    try buf.appendSlice(alloc, "date_trunc('");
    try buf.appendSlice(alloc, unit);
    try buf.appendSlice(alloc, "', ");
    try buf.appendSlice(alloc, sql[start..close]);
    try buf.append(alloc, ')');
    return close + 1;
}

/// Map ClickHouse type names to DuckDB equivalents for CAST.
fn chTypeToDuckdb(ch_type: []const u8) []const u8 {
    const t = std.mem.trim(u8, ch_type, " \t\r\n");
    if (std.ascii.startsWithIgnoreCase(t, "Array(")) return "VARCHAR[]";
    if (std.ascii.eqlIgnoreCase(t, "String")) return "VARCHAR";
    if (std.ascii.eqlIgnoreCase(t, "UInt8"))  return "UTINYINT";
    if (std.ascii.eqlIgnoreCase(t, "UInt16")) return "USMALLINT";
    if (std.ascii.eqlIgnoreCase(t, "UInt32")) return "UINTEGER";
    if (std.ascii.eqlIgnoreCase(t, "UInt64")) return "UBIGINT";
    if (std.ascii.eqlIgnoreCase(t, "Int8"))   return "TINYINT";
    if (std.ascii.eqlIgnoreCase(t, "Int16"))  return "SMALLINT";
    if (std.ascii.eqlIgnoreCase(t, "Int32"))  return "INTEGER";
    if (std.ascii.eqlIgnoreCase(t, "Int64"))  return "BIGINT";
    if (std.ascii.eqlIgnoreCase(t, "Float32")) return "FLOAT";
    if (std.ascii.eqlIgnoreCase(t, "Float64")) return "DOUBLE";
    return t; // fallthrough: pass as-is
}
