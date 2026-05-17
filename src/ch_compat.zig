/// ch_compat.zig — ClickHouse→DuckDB grammar rewrites (grammar-level blockers only).
/// Unknown CH function names (uniqExact, dictHas, …) parse fine in DuckDB as-is.
const std = @import("std");

/// Rewrite CH grammar so DuckDB can parse. Returns owned slice or null (ARRAY JOIN).
pub fn rewrite(allocator: std.mem.Allocator, sql: []const u8) !?[]u8 {
    if (std.ascii.indexOfIgnoreCase(sql, "ARRAY JOIN") != null) return null;
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    try buf.ensureTotalCapacity(allocator, sql.len + 64);
    errdefer buf.deinit(allocator);
    var i: usize = 0;
    while (i < sql.len) {
        const wb = i == 0 or !isIdent(sql[i - 1]);
        if (wb) {
            if (matchFn(sql, i, "multiIf"))        |r| { i = try rwMultiIf(allocator, &buf, sql, r); continue; }
            if (matchFn(sql, i, "toString"))        |r| { i = try rwCast(allocator, &buf, sql, r, "AS VARCHAR"); continue; }
            if (matchFn(sql, i, "toDate"))          |r| { i = try rwCast(allocator, &buf, sql, r, "AS DATE"); continue; }
            if (matchFn(sql, i, "toStartOfMinute")) |r| { i = try rwTrunc(allocator, &buf, sql, r, "minute"); continue; }
            if (matchFn(sql, i, "toStartOfHour"))   |r| { i = try rwTrunc(allocator, &buf, sql, r, "hour"); continue; }
            if (matchFn(sql, i, "toStartOfDay"))    |r| { i = try rwTrunc(allocator, &buf, sql, r, "day"); continue; }
            if (matchFn(sql, i, "any"))             |r| { try buf.appendSlice(allocator, "any_value("); i = r; continue; }
        }
        try buf.append(allocator, sql[i]);
        i += 1;
    }
    return @as(?[]u8, try buf.toOwnedSlice(allocator));
}

fn isIdent(ch: u8) bool { return std.ascii.isAlphanumeric(ch) or ch == '_'; }

/// Match `name(` at sql[i]. Returns index after '(', or null.
fn matchFn(sql: []const u8, i: usize, name: []const u8) ?usize {
    if (i + name.len > sql.len) return null;
    if (!std.ascii.eqlIgnoreCase(sql[i .. i + name.len], name)) return null;
    var j = i + name.len;
    while (j < sql.len and sql[j] == ' ') j += 1;
    if (j >= sql.len or sql[j] != '(') return null;
    return j + 1;
}

/// Find matching ')' starting at `start` (depth=1). Returns index of ')'.
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

/// Split top-level args (inside already-found start..close range).
fn splitArgs(alloc: std.mem.Allocator, sql: []const u8, start: usize, close: usize) ![][]const u8 {
    var args: std.ArrayListUnmanaged([]const u8) = .empty;
    var depth: usize = 0;
    var s = start;
    var k = start;
    while (k < close) : (k += 1) {
        switch (sql[k]) {
            '(','[' => depth += 1,
            ')',']' => depth -= 1,
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

/// toString(x)→CAST(x AS VARCHAR), toDate(x)→CAST(x AS DATE)
fn rwCast(alloc: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), sql: []const u8, start: usize, suffix: []const u8) !usize {
    const close = findClose(sql, start) orelse { try buf.appendSlice(alloc, "toString("); return start; };
    try buf.appendSlice(alloc, "CAST("); try buf.appendSlice(alloc, sql[start..close]);
    try buf.append(alloc, ' '); try buf.appendSlice(alloc, suffix); try buf.append(alloc, ')');
    return close + 1;
}

/// toStartOfHour(x)→date_trunc('hour', x)
fn rwTrunc(alloc: std.mem.Allocator, buf: *std.ArrayListUnmanaged(u8), sql: []const u8, start: usize, unit: []const u8) !usize {
    const close = findClose(sql, start) orelse { try buf.appendSlice(alloc, "toStartOf("); return start; };
    try buf.appendSlice(alloc, "date_trunc('"); try buf.appendSlice(alloc, unit);
    try buf.appendSlice(alloc, "', "); try buf.appendSlice(alloc, sql[start..close]); try buf.append(alloc, ')');
    return close + 1;
}
