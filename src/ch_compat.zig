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
    .{ .name = "toString",         .kind = .cast,       .param = "AS VARCHAR"   },
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
    // arrayStringConcat(arr, sep) → array_to_string(arr, sep)
    // DuckDB's parser rejects arrayStringConcat when the first arg is an array literal.
    .{ .name = "arrayStringConcat",    .kind = .rename,     .param = "array_to_string("  },
};

// ── Public entry point ────────────────────────────────────────────────────────

/// Rewrite ARRAY JOIN syntax into arrayJoin() function calls in the SELECT list.
///
/// Input:  SELECT a, b FROM t ARRAY JOIN expr1 AS al1, expr2 AS al2 WHERE cond
/// Output: SELECT a, b, arrayJoin(expr1) AS al1, arrayJoin(expr2) AS al2 FROM t WHERE cond
///
/// Aliases that appear in the SELECT list are left as-is (they refer to the ARRAY JOIN result).
/// Only the first ARRAY JOIN item is used for row expansion; others are kept as columns.
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
        const as_pos = std.ascii.indexOfIgnoreCase(item, " AS ") orelse continue;
        const expr = std.mem.trim(u8, item[0..as_pos], " \t\r\n");
        const alias = std.mem.trim(u8, item[as_pos + 4..], " \t\r\n");
        try alias_map.append(allocator, .{ .alias = alias, .expr = expr });
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
            // Emit the column as-is; aliases inside expressions (e.g. avg(fv))
            // will be resolved via extra arrayJoin columns appended below.
            try new_select.appendSlice(allocator, scol);
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

pub fn rewrite(allocator: std.mem.Allocator, sql: []const u8) !?[]u8 {
    // Normalize whitespace: collapse runs of \t\r\n and multiple spaces into a
    // single space so that keyword searches like " ARRAY JOIN " work regardless
    // of indentation or newlines in the incoming SQL.
    const norm = try normalizeWhitespace(allocator, sql);
    defer allocator.free(norm);

    // If ARRAY JOIN is present, rewrite it first, then fall through to apply
    // function-rename rules (e.g. toDateTime→CAST) on the result.
    var after_aj: ?[]u8 = null;
    if (std.ascii.indexOfIgnoreCase(norm, "ARRAY JOIN") != null) {
        after_aj = try rewriteArrayJoin(allocator, norm) orelse return null;
    }
    // base points to either the ARRAY JOIN-rewritten SQL or the normalized SQL.
    // We dupe after_aj immediately so we can free it while base2 lives on.
    const base: []const u8 = if (after_aj) |s| s else norm;

    // Pre-pass: rewrite CAST(expr, 'TypeName') → CAST(expr AS TypeName).
    // When after_aj is set we dupe base so sql2 and after_aj never alias,
    // preventing double-free in the defers below.
    const base2: []const u8 = if (after_aj != null) try allocator.dupe(u8, base) else base;
    if (after_aj) |s| allocator.free(s);  // safe to free now; base2 is a fresh copy
    const sql2 = try rewriteCastStringType(allocator, base2);
    defer if (sql2.ptr != base2.ptr) allocator.free(sql2);
    defer if (base2.ptr != norm.ptr) allocator.free(base2);

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql2.len + 64);
    errdefer buf.deinit(allocator);

    var i: usize = 0;
    while (i < sql2.len) {
        // Only attempt rewrites at word boundaries.
        const wb = i == 0 or !isIdent(sql2[i - 1]);
        if (wb) {
            const matched = inline for (rules) |rule| {
                if (matchFn(sql2, i, rule.name)) |r| {
                    switch (rule.kind) {
                        .multiif    => { i = try rwMultiIf(allocator, &buf, sql2, r);              break true; },
                        .cast       => { i = try rwCast(allocator, &buf, sql2, r, rule.param);     break true; },
                        .date_trunc => { i = try rwTrunc(allocator, &buf, sql2, r, rule.param);    break true; },
                        .rename     => { try buf.appendSlice(allocator, rule.param); i = r;       break true; },
                    }
                }
            } else false;
            if (matched) continue;
        }
        try buf.append(allocator, sql2[i]);
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
