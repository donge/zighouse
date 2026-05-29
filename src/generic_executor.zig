/// Generic SQL executor: evaluates a generic_sql.Plan by streaming rows
/// directly from a Parquet file.  This is the "slow path" fallback that
/// handles any SQL the specialized hot-column executors cannot.
///
/// Design goals:
///   - Zero DuckDB dependency at execution time (parser is DuckDB; executor is not)
///   - Single-pass over the Parquet file where possible
///   - Correct output for all 43 ClickBench query forms plus arbitrary
///     SELECT/WHERE/GROUP BY/ORDER BY/LIMIT/OFFSET on hits columns
///   - No performance requirement for this path (correctness only)
///
/// Supported plan shapes (what generic_sql.Plan can express):
///   1. Scalar aggregate  : SELECT count(*)/sum(col)/avg(col)/min(col)/max(col) [WHERE …]
///   2. count(distinct)   : SELECT COUNT(DISTINCT col) [WHERE …]
///   3. Group-by + agg    : SELECT col, count(*) … GROUP BY col [ORDER BY … LIMIT n]
///   4. Filtered select   : SELECT col FROM hits WHERE col op val [ORDER BY … LIMIT n]
///   5. Point lookup      : SELECT col FROM hits WHERE col = val
///   6. Multi-agg scalar  : SELECT sum(a), count(*), avg(b) … (no group by)
///
/// Columns are read from the Parquet file using parquet.streamFixedColumnsTypedPath
/// (fixed-width integers / dates / timestamps) and parquet.streamByteArrayColumnPath
/// (variable-length strings).  String columns are materialised into an arena on
/// the first pass and then referenced by index for filter and projection.

const std = @import("std");
const generic_sql = @import("generic_sql");
const parquet = @import("parquet");
const clickbench_schema = schema.clickbench;
const schema = @import("schema");
const build_options = @import("build_options");
const ch_part = @import("ch_part");
const csv_mod = @import("csv");

extern fn erf(x: f64) f64;
extern fn erfc(x: f64) f64;
fn erf_c(x: f64) f64 { return erf(x); }
fn erfc_c(x: f64) f64 { return erfc(x); }

// ── User-defined functions (set by server before calling executor) ────────────

/// Thread-local registry of user-defined functions: name → lambda text "(params) -> body".
/// Set by the server before each query execution.
pub var udf_registry: ?*std.StringHashMap([]const u8) = null;

// ── Data source ───────────────────────────────────────────────────────────────

/// Describes where the executor reads row data from.
pub const Source = union(enum) {
    /// Stream rows directly from a Parquet file (ZigDB path).
    parquet: []const u8,
    /// Read from a CH MergeTree part directory (ZigHouse path).
    /// The string is the part directory path (e.g. `<store>/<table>/parts/all_1_1_0`).
    ch_part: []const u8,
    /// Read from multiple CH MergeTree part directories (multi-part ZigHouse path).
    /// Rows are streamed sequentially across all parts.
    ch_parts: []const []const u8,
    /// Materialized CSV from a subquery (header line + data lines).
    csv_rows: []const u8,
};

// ── Public entry points ───────────────────────────────────────────────────────

/// Execute `plan` against a data `source` and return the result as a
/// CSV-formatted string (header row + data rows, comma-separated).
/// `table` is used to resolve column names to column indices and types.
/// Returns `error.UnsupportedGenericQuery` when the plan shape is not handled.
pub fn runWithSource(
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    source: Source,
    table: *const schema.Table,
) anyerror![]u8 {
    // Expand SELECT * (STAR projection with column==null) to all schema columns.
    var expanded_projs: ?[]generic_sql.Expr = null;
    defer if (expanded_projs) |ep| allocator.free(ep);
    var effective_plan = plan;
    for (plan.projections) |p| {
        if (p.func == .column_ref and p.column == null and table.columns.len > 0) {
            // Replace the single STAR with all columns
            const new_projs = try allocator.alloc(generic_sql.Expr, table.columns.len);
            for (table.columns, 0..) |col, ci| {
                new_projs[ci] = .{ .func = .column_ref, .column = col.name };
            }
            expanded_projs = new_projs;
            effective_plan.projections = new_projs;
            break;
        }
    }
    const exec = Executor{
        .allocator = allocator,
        .io = io,
        .plan = effective_plan,
        .source = source,
        .table = table,
    };

    const has_group = plan.group_by != null;
    const all_agg = allAggregates(plan.projections);
    const any_agg = anyAggregate(plan.projections);

    // Run as scalar aggregation when there is no GROUP BY and at least one
    // aggregate function.  column_ref projections (e.g. `if(total>0, avg(x), 0)`)
    // are evaluated post-aggregation against the assembled aggregate RowCtx.
    if (!has_group and (all_agg or any_agg)) return exec.runScalarAgg();
    if (has_group) return exec.runGroupBy();
    return exec.runScan();
}

/// Run `plan` against a materialized CSV (from a subquery) and return CSV result.
pub fn runOverCsv(
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    csv: []const u8,
    table: *const schema.Table,
) anyerror![]u8 {
    return runWithSource(allocator, undefined, plan, .{ .csv_rows = csv }, table);
}

/// Backward-compatible entry point: execute `plan` against a Parquet file.
pub fn run(
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    parquet_path: []const u8,
    table: *const schema.Table,
) anyerror![]u8 {
    return runWithSource(allocator, io, plan, .{ .parquet = parquet_path }, table);
}

// ── Column descriptor ─────────────────────────────────────────────────────────

const ColKind = enum { fixed_i16, fixed_i32, fixed_i64, fixed_date, fixed_timestamp, fixed_f32, fixed_f64, string, array_string };

const ColDesc = struct {
    name: []const u8,    // original column name (case as in schema)
    index: usize,        // Parquet column index
    kind: ColKind,
};

fn lookupColumn(tbl: *const schema.Table, name: []const u8) ?ColDesc {
    // Handle computed / derived column names used in plan projections
    // e.g. "extract(minute from EventTime)" handled elsewhere; skip
    const idx = tbl.findColumn(name) orelse return null;
    const col = tbl.columns[idx];
    const kind: ColKind = switch (col.ty) {
        .int8, .int16 => .fixed_i16,
        .int32 => .fixed_i32,
        .int64 => .fixed_i64,
        .date   => .fixed_date,
        .timestamp => .fixed_timestamp,
        .float32 => .fixed_f32,
        .float64 => .fixed_f64,
        .text, .char, .low_card => blk: {
            // Check if this is an Array(String) column
            if (col.ch_type) |ct| {
                if (std.ascii.startsWithIgnoreCase(ct, "Array(")) break :blk .array_string;
            }
            break :blk .string;
        },
    };
    return ColDesc{ .name = col.name, .index = idx, .kind = kind };
}

// ── Value type ────────────────────────────────────────────────────────────────

/// Runtime value for a single cell.
const Value = union(enum) {
    i64: i64,
    f64: f64,
    /// Days since 1970-01-01 (ClickHouse Date / UInt16).
    date: u16,
    /// UInt8 boolean result from dictHas/has/etc — must encode as UInt8, not UInt64.
    uint8: u8,
    /// Slice into arena; valid for the lifetime of the Executor.run call.
    str: []const u8,
    /// Heap-allocated string owned by this Value; caller must free with page_allocator.
    str_owned: []u8,
    /// Array of Values; elements are page_allocator-owned or arena slices.
    array: []Value,
    null_val,

    fn isNull(self: Value) bool {
        return self == .null_val;
    }

    fn toI64(self: Value) ?i64 {
        return switch (self) {
            .i64   => |v| v,
            .f64   => |v| @intFromFloat(v),
            .date  => |v| @as(i64, v),
            .uint8 => |v| @as(i64, v),
            else   => null,
        };
    }

    fn toF64(self: Value) ?f64 {
        return switch (self) {
            .i64   => |v| @floatFromInt(v),
            .f64   => |v| v,
            .date  => |v| @as(f64, @floatFromInt(v)),
            .uint8 => |v| @as(f64, @floatFromInt(v)),
            else   => null,
        };
    }

    fn toStr(self: Value) ?[]const u8 {
        return switch (self) {
            .str      => |s| s,
            .str_owned => |s| s,
            else => null,
        };
    }

    fn order(a: Value, b: Value) std.math.Order {
        switch (a) {
            .i64 => |av| switch (b) {
                .i64   => |bv| return std.math.order(av, bv),
                .f64   => |bv| return std.math.order(@as(f64, @floatFromInt(av)), bv),
                .date  => |bv| return std.math.order(av, @as(i64, bv)),
                .uint8 => |bv| return std.math.order(av, @as(i64, bv)),
                else   => return .lt,
            },
            .f64 => |av| switch (b) {
                .i64   => |bv| return std.math.order(av, @as(f64, @floatFromInt(bv))),
                .f64   => |bv| return std.math.order(av, bv),
                .date  => |bv| return std.math.order(av, @as(f64, @floatFromInt(bv))),
                .uint8 => |bv| return std.math.order(av, @as(f64, @floatFromInt(bv))),
                else   => return .lt,
            },
            .date => |av| switch (b) {
                .date  => |bv| return std.math.order(av, bv),
                .i64   => |bv| return std.math.order(@as(i64, av), bv),
                .f64   => |bv| return std.math.order(@as(f64, @floatFromInt(av)), bv),
                .uint8 => |bv| return std.math.order(@as(i64, av), @as(i64, bv)),
                else   => return .lt,
            },
            .uint8 => |av| switch (b) {
                .uint8 => |bv| return std.math.order(av, bv),
                .i64   => |bv| return std.math.order(@as(i64, av), bv),
                .f64   => |bv| return std.math.order(@as(f64, @floatFromInt(av)), bv),
                .date  => |bv| return std.math.order(@as(i64, av), @as(i64, bv)),
                else   => return .lt,
            },
            .str, .str_owned => {
                const av = a.toStr().?;
                switch (b) {
                    .str, .str_owned => return std.mem.order(u8, av, b.toStr().?),
                    else => return .gt,
                }
            },
            .array => |av| switch (b) {
                .array => |bv| {
                    const len = @min(av.len, bv.len);
                    for (0..len) |i| {
                        const o = Value.order(av[i], bv[i]);
                        if (o != .eq) return o;
                    }
                    return std.math.order(av.len, bv.len);
                },
                else => return .gt,
            },
            .null_val => return if (b == .null_val) .eq else .lt,
        }
    }

    fn eql(a: Value, b: Value) bool {
        return order(a, b) == .eq;
    }

    /// Write in CSV-compatible format.
    fn writeCsv(self: Value, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
        switch (self) {
            .i64 => |v| try out.print(allocator, "{d}", .{v}),
            .f64 => |v| {
                // Always output with a decimal point so csvToNativeBlock can detect
                // this as a Float64 column (not Int64/UInt64).
                if (v == @trunc(v) and @abs(v) < 1e15) {
                    try out.print(allocator, "{d}.0", .{@as(i64, @intFromFloat(v))});
                } else {
                    try out.print(allocator, "{d}", .{v});
                }
            },
            .str      => |s| {
                // CSV-quote strings that contain commas, quotes, or newlines
                const needs_quote = std.mem.indexOfAny(u8, s, ",\"\n\r") != null;
                if (needs_quote) {
                    try out.append(allocator, '"');
                    for (s) |ch| {
                        if (ch == '"') try out.append(allocator, '"'); // escape double-quote
                        try out.append(allocator, ch);
                    }
                    try out.append(allocator, '"');
                } else {
                    try out.appendSlice(allocator, s);
                }
            },
            .str_owned => |s| {
                const needs_quote = std.mem.indexOfAny(u8, s, ",\"\n\r") != null;
                if (needs_quote) {
                    try out.append(allocator, '"');
                    for (s) |ch| {
                        if (ch == '"') try out.append(allocator, '"');
                        try out.append(allocator, ch);
                    }
                    try out.append(allocator, '"');
                } else {
                    try out.appendSlice(allocator, s);
                }
            },
            .array    => |arr| {
                // Render array as \x01 sentinel + \x0c-separated elements.
                // Use raw (unquoted) string values inside the blob — the \x0c separator
                // avoids commas so no CSV quoting is needed here.
                try out.append(allocator, 0x01);
                for (arr, 0..) |elem, i| {
                    if (i > 0) try out.append(allocator, '\x0c');
                    // Write element as raw string (no CSV quoting inside the blob)
                    switch (elem) {
                        .str      => |s| try out.appendSlice(allocator, s),
                        .str_owned=> |s| try out.appendSlice(allocator, s),
                        .i64      => |v| try out.print(allocator, "{d}", .{v}),
                        .f64      => |v| try out.print(allocator, "{d}", .{v}),
                        else      => try elem.writeCsv(out, allocator),
                    }
                }
            },
            .null_val => {},
            // Date: format as YYYY-MM-DD (ClickHouse TabSeparated format).
            // Header sentinel \x02D: is still added by writeExprHeader for native-block encoding.
            .date => |d| {
                const ymd = epochDaysToYmd(d);
                // year is i32 but always positive for dates after 1970; cast to u32 avoids '+' prefix.
                const y: u32 = @intCast(@max(0, ymd.year));
                try out.print(allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ y, ymd.month, ymd.day });
            },
            // UInt8 bool: emit as plain integer — header sentinel \x03U8: tells native-block to encode as UInt8
            .uint8 => |v| try out.print(allocator, "{d}", .{v}),
        }
    }
};

/// Convert days-since-1970-01-01 to a Gregorian (year, month, day) triple.
/// Algorithm: http://howardhinnant.github.io/date_algorithms.html  civil_from_days
fn epochDaysToYmd(days: i32) struct { year: i32, month: u32, day: u32 } {
    const z: i32 = days + 719468;
    const era: i32 = @divFloor(z, 146097);
    const doe: u32 = @intCast(z - era * 146097);
    const yoe: u32 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const y: i32   = @as(i32, @intCast(yoe)) + era * 400;
    const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp: u32  = (5 * doy + 2) / 153;
    const d: u32   = doy - (153 * mp + 2) / 5 + 1;
    const m: u32   = if (mp < 10) mp + 3 else mp - 9;
    return .{ .year = y + @as(i32, if (m <= 2) 1 else 0), .month = m, .day = d };
}

/// Format Unix seconds as "YYYY-MM-DD HH:MM:SS" (UTC).
fn secsToDatetimeStr(secs: i64) ![]u8 {
    const s_of_day: u32 = @intCast(@mod(secs, 86400));
    const days: i32 = @intCast(@divFloor(secs, 86400));
    const ymd = epochDaysToYmd(days);
    const h = s_of_day / 3600;
    const m = (s_of_day % 3600) / 60;
    const s = s_of_day % 60;
    const y: u32 = @intCast(@max(0, ymd.year));
    return std.fmt.allocPrint(std.heap.page_allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}",
        .{ y, ymd.month, ymd.day, h, m, s });
}


// ── Predicate evaluation ──────────────────────────────────────────────────────

/// Evaluate a `WhereNode` predicate tree against a row.
/// Returns true if the row passes the predicate.
fn evalWhereNode(node: *const generic_sql.WhereNode, row: *const RowCtx) bool {
    switch (node.*) {
        .cmp_int => |c| {
            const v = row.get(c.col) orelse evalTextExpr(c.col, row) orelse return false;
            const iv = v.toI64() orelse return false;
            return switch (c.op) {
                .eq => iv == c.val,
                .ne => iv != c.val,
                .lt => iv <  c.val,
                .le => iv <= c.val,
                .gt => iv >  c.val,
                .ge => iv >= c.val,
            };
        },
        .cmp_str => |c| {
            const v = row.get(c.col) orelse evalTextExpr(c.col, row) orelse return false;
            const sv = v.toStr() orelse {
                // numeric column compared to string literal (e.g. date as epoch vs '2013-07-01')
                // Try parsing the string as a date-epoch integer
                if (c.op == .eq or c.op == .ne or c.op == .lt or c.op == .le or c.op == .gt or c.op == .ge) {
                    if (parseDateStr(c.val)) |epoch| {
                        const iv = v.toI64() orelse return false;
                        return switch (c.op) {
                            .eq => iv == epoch,
                            .ne => iv != epoch,
                            .lt => iv <  epoch,
                            .le => iv <= epoch,
                            .gt => iv >  epoch,
                            .ge => iv >= epoch,
                        };
                    }
                }
                return false;
            };
            return switch (c.op) {
                .eq => std.mem.eql(u8, sv, c.val),
                .ne => !std.mem.eql(u8, sv, c.val),
                .lt => std.mem.order(u8, sv, c.val) == .lt,
                .le => std.mem.order(u8, sv, c.val) != .gt,
                .gt => std.mem.order(u8, sv, c.val) == .gt,
                .ge => std.mem.order(u8, sv, c.val) != .lt,
            };
        },
        .like => |l| {
            const v = row.get(l.col) orelse return false;
            const sv = v.toStr() orelse return false;
            const matched = likeMatch(sv, l.pattern, l.op == .ilike);
            return switch (l.op) {
                .like, .ilike => matched,
                .not_like => !matched,
            };
        },
        .is_null => |col| {
            const v = row.get(col) orelse return true; // missing → null
            return v == .null_val;
        },
        .is_not_null => |col| {
            const v = row.get(col) orelse return false;
            return v != .null_val;
        },
        .and_ => |children| {
            for (children) |ch| if (!evalWhereNode(ch, row)) return false;
            return true;
        },
        .or_ => |children| {
            for (children) |ch| if (evalWhereNode(ch, row)) return true;
            return false;
        },
    }
}

/// SQL LIKE pattern matching: '%' matches any sequence, '_' matches one char.
/// case_insensitive=true for ILIKE.
fn likeMatch(str: []const u8, pattern: []const u8, case_insensitive: bool) bool {
    if (pattern.len == 0) return str.len == 0;
    if (pattern[0] == '%') {
        // Try matching the rest of the pattern at each position in str
        if (likeMatch(str, pattern[1..], case_insensitive)) return true;
        if (str.len == 0) return false;
        return likeMatch(str[1..], pattern, case_insensitive);
    }
    if (str.len == 0) return false;
    if (pattern[0] == '_') {
        return likeMatch(str[1..], pattern[1..], case_insensitive);
    }
    const match = if (case_insensitive)
        std.ascii.toLower(str[0]) == std.ascii.toLower(pattern[0])
    else
        str[0] == pattern[0];
    if (!match) return false;
    return likeMatch(str[1..], pattern[1..], case_insensitive);
}

/// Parse a 'YYYY-MM-DD' date string into days-since-epoch (DuckDB DATE epoch).
/// Returns null if the string is not a recognisable date.
/// Parse a date/datetime string into a Unix millisecond timestamp (UTC).
/// Accepts:
///   'YYYY-MM-DD'           → start of that day, 00:00:00.000 UTC (ms)
///   'YYYY-MM-DD HH:MM:SS'  → that exact second, .000 UTC (ms)
/// Returns null if the string does not match either format.
fn parseDateStr(s: []const u8) ?i64 {
    if (s.len < 10 or s[4] != '-' or s[7] != '-') return null;
    const y = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    const mo = std.fmt.parseInt(u8,  s[5..7], 10) catch return null;
    const d  = std.fmt.parseInt(u8,  s[8..10], 10) catch return null;
    const day_epoch = dateToDays(y, mo, d); // days since 1970-01-01
    var h:  i64 = 0;
    var mi: i64 = 0;
    var sec: i64 = 0;
    if (s.len >= 19 and s[10] == ' ' and s[13] == ':' and s[16] == ':') {
        h   = std.fmt.parseInt(i64, s[11..13], 10) catch 0;
        mi  = std.fmt.parseInt(i64, s[14..16], 10) catch 0;
        sec = std.fmt.parseInt(i64, s[17..19], 10) catch 0;
    }
    const total_s = day_epoch * 86400 + h * 3600 + mi * 60 + sec;
    return total_s * 1000; // milliseconds
}

fn dateToDays(year: i32, month: u8, day: u8) i64 {
    // Zeller-style: count days from 1970-01-01
    var y: i64 = year;
    var m: i64 = month;
    if (m <= 2) { y -= 1; m += 12; }
    const a = @divFloor(y, 100);
    const b = 2 - a + @divFloor(a, 4);
    const jdn = @as(i64, @intFromFloat(@floor(365.25 * @as(f64, @floatFromInt(y + 4716))))) +
                @as(i64, @intFromFloat(@floor(30.6001 * @as(f64, @floatFromInt(m + 1))))) +
                @as(i64, day) + b - 1524;
    // Julian Day Number for 1970-01-01 is 2440588
    return jdn - 2440588;
}

/// Evaluate `Filter` against a row represented as a name→Value lookup.
/// Used as fallback when where_expr is not available.
/// Evaluate the plan's WHERE predicate against a row.
/// Prefers where_expr (full Expr tree) over where_text fallback.
fn evalPlanFilter(plan: generic_sql.Plan, row: *const RowCtx) bool {
    if (plan.where_expr) |we| return evalWhereNode(we, row);
    // Fall back to text-based WHERE evaluation for complex predicates
    if (plan.where_text) |wt| return evalTextBoolExpr(wt, row);
    return true; // no filter: row passes
}

// ── Row context: a lightweight name→value map backed by parallel slices ──────

const RowCtx = struct {
    names: []const []const u8,
    values: []const Value,
    /// Optional parent row for lambda scopes (lambda param shadows parent columns).
    parent: ?*const RowCtx = null,
    /// Optional table schema for Map value-type dispatch.
    table: ?*const schema.Table = null,

    fn get(self: *const RowCtx, name: []const u8) ?Value {
        // Strip table-alias qualifier (e.g. "sys_num.number" → "number")
        const bare_name = if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| name[dot + 1 ..] else name;
        // Fast path: direct column name match in this scope
        for (self.names, self.values) |n, v| {
            if (std.ascii.eqlIgnoreCase(n, bare_name)) {
                // If this is a raw Array blob (stored as .str), decode it to .array
                // so that array functions (has, arrayFilter, etc.) can operate on it.
                if (v == .str or v == .str_owned) {
                    if (self.table) |tbl| {
                        if (tbl.findColumn(bare_name)) |ci| {
                            if (tbl.columns[ci].ch_type) |ct| {
                                if (std.mem.startsWith(u8, ct, "Array(")) {
                                    return decodeArrayBlob(ct, v.toStr().?);
                                }
                            }
                        }
                    }
                }
                return v;
            }
        }
        // Chain to parent scope (lambda bindings)
        if (self.parent) |p| return p.get(name);
        // Slow path: Map subscript access like data['key'] or features['key']
        if (parseMapSubscript(name)) |sub| {
            for (self.names, self.values) |n, v| {
                if (std.ascii.eqlIgnoreCase(n, sub.col)) {
                    const blob = v.toStr() orelse return Value{ .str = "" };
                    // Determine value type from schema if available
                    var val_ch_type: []const u8 = "String";
                    if (self.table) |tbl| {
                        if (tbl.findColumn(sub.col)) |ci| {
                            if (tbl.columns[ci].ch_type) |ct| {
                                val_ch_type = extractMapValueType(ct);
                            }
                        }
                    }
                    return lookupMapBlobTyped(blob, sub.key, val_ch_type);
                }
            }
        }
        return null;
    }
};

/// Parsed result of `col['key']` subscript expression.
const MapSubscript = struct { col: []const u8, key: []const u8 };

/// Parse `expr` as `col['key']` or `col["key"]`.
/// Returns null if not a subscript expression.
fn parseMapSubscript(expr: []const u8) ?MapSubscript {
    const open = std.mem.indexOfScalar(u8, expr, '[') orelse return null;
    const close = std.mem.lastIndexOfScalar(u8, expr, ']') orelse return null;
    if (close <= open + 2) return null;
    const col = std.mem.trim(u8, expr[0..open], " \t");
    if (col.len == 0) return null;
    // Ensure col is a simple identifier (no parens, commas, spaces — not a function call)
    for (col) |c| {
        if (c == '(' or c == ')' or c == ',' or c == ' ' or c == '\t') return null;
    }
    var inner = std.mem.trim(u8, expr[open + 1 .. close], " \t");
    // Strip surrounding quotes
    if (inner.len >= 2 and ((inner[0] == '\'' and inner[inner.len - 1] == '\'') or
                             (inner[0] == '"' and inner[inner.len - 1] == '"'))) {
        inner = inner[1 .. inner.len - 1];
    }
    return .{ .col = col, .key = inner };
}

/// Look up `key` in a raw ClickHouse Map(String,String) binary blob.
/// The blob format (as stored by consumeNativeTextRows for a single row) is:
///   N * (varUInt(klen) + kbytes) followed by N * (varUInt(vlen) + vbytes)
/// But since we don't have count N stored in the blob, we store it as:
///   key_data_bytes + value_data_bytes concatenated back-to-back.
/// We find the boundary by scanning keys first and counting, then scanning values.
///
/// Actually, ZigHouse stores the raw key bytes + raw value bytes for the row's
/// pairs concatenated.  The format stored by consumeNativeTextRows (line 747-749) is:
///   data[k_row_start..kp]  (all keys varUInt-prefixed)
///   data[v_row_start..vp]  (all values varUInt-prefixed)
/// We scan keys counting N, then scan values to find the Nth one.
fn lookupMapBlob(blob: []const u8, key: []const u8) ?[]const u8 {
    // Blob format (written by decodeRowBinaryArrayOrMap):
    // varint N | N × (varint_len + key_bytes) | N × (varint_len + value_bytes)
    if (blob.len == 0) return null;
    const count, const cb = readVarUIntSlice(blob) orelse return null;
    var kp: usize = cb;
    var match_idx: ?usize = null;

    for (0..count) |i| {
        const len, const lb = readVarUIntSlice(blob[kp..]) orelse return null;
        if (kp + lb + len > blob.len) return null;
        const k = blob[kp + lb .. kp + lb + len];
        if (match_idx == null and std.mem.eql(u8, k, key)) {
            match_idx = i;
        }
        kp += lb + len;
    }
    if (match_idx == null) return null;
    // kp now points to start of values section
    var vp: usize = kp;
    for (0..count) |i| {
        const vlen, const vlb = readVarUIntSlice(blob[vp..]) orelse return null;
        if (vp + vlb + vlen > blob.len) return null;
        if (i == match_idx.?) {
            return blob[vp + vlb .. vp + vlb + vlen];
        }
        vp += vlb + vlen;
    }
    return null;
}

/// Extract the value type string from "Map(K, V)" → "V". Returns "String" on failure.
fn extractMapValueType(ch_type: []const u8) []const u8 {
    if (!std.mem.startsWith(u8, ch_type, "Map(")) return "String";
    const inner = ch_type[4 .. ch_type.len - 1]; // strip "Map(" and ")"
    var depth: usize = 0;
    for (inner, 0..) |c, i| {
        if (c == '(') depth += 1
        else if (c == ')') depth -= 1
        else if (c == ',' and depth == 0) {
            return std.mem.trim(u8, inner[i+1..], " ");
        }
    }
    return "String";
}

/// Fixed byte widths for CH numeric value types in Map blobs.
fn mapValueFixedWidth(vtype: []const u8) ?usize {
    if (std.mem.eql(u8, vtype, "Float32") or std.mem.eql(u8, vtype, "Int32") or std.mem.eql(u8, vtype, "UInt32")) return 4;
    if (std.mem.eql(u8, vtype, "Float64") or std.mem.eql(u8, vtype, "Int64") or std.mem.eql(u8, vtype, "UInt64")) return 8;
    if (std.mem.eql(u8, vtype, "Int16") or std.mem.eql(u8, vtype, "UInt16")) return 2;
    if (std.mem.eql(u8, vtype, "Int8") or std.mem.eql(u8, vtype, "UInt8")) return 1;
    return null;
}

/// Lookup a key in a Map blob, returning the correct Value type.
/// Blob format: varint N | N×varint_key_bytes | N×value_bytes
/// Value bytes format depends on val_ch_type (String → varint-prefixed, numeric → fixed-width).
fn lookupMapBlobTyped(blob: []const u8, key: []const u8, val_ch_type: []const u8) Value {
    if (blob.len == 0) return Value{ .str = "" };
    const count, const cb = readVarUIntSlice(blob) orelse return Value{ .str = "" };
    var kp: usize = cb;
    var match_idx: ?usize = null;

    for (0..count) |i| {
        const len, const lb = readVarUIntSlice(blob[kp..]) orelse return Value{ .str = "" };
        if (kp + lb + len > blob.len) return Value{ .str = "" };
        const k = blob[kp + lb .. kp + lb + len];
        if (match_idx == null and std.mem.eql(u8, k, key)) match_idx = i;
        kp += lb + len;
    }
    if (match_idx == null) return Value{ .str = "" };

    // Values section
    const fix_w = mapValueFixedWidth(val_ch_type);
    var vp: usize = kp;
    for (0..count) |i| {
        if (fix_w) |w| {
            if (vp + w > blob.len) return Value{ .str = "" };
            if (i == match_idx.?) {
                const raw = blob[vp..vp+w];
                if (w == 8) {
                    const bits = std.mem.readInt(u64, raw[0..8], .little);
                    return Value{ .f64 = @bitCast(bits) };
                } else if (w == 4) {
                    const bits = std.mem.readInt(u32, raw[0..4], .little);
                    return Value{ .f64 = @as(f64, @floatCast(@as(f32, @bitCast(bits)))) };
                }
                // 1 or 2 byte integers
                var ibuf = [_]u8{0} ** 8;
                @memcpy(ibuf[0..w], raw[0..w]);
                return Value{ .i64 = @intCast(std.mem.readInt(u64, &ibuf, .little)) };
            }
            vp += w;
        } else {
            // String (varint-prefixed)
            const vlen, const vlb = readVarUIntSlice(blob[vp..]) orelse return Value{ .str = "" };
            if (vp + vlb + vlen > blob.len) return Value{ .str = "" };
            if (i == match_idx.?) return Value{ .str = blob[vp + vlb .. vp + vlb + vlen] };
            vp += vlb + vlen;
        }
    }
    return Value{ .str = "" };
}

/// Decode a raw Array(String) or Array(T) blob (stored by row_binary_decoder)
/// into a Value{ .array = [...] }.  Allocations use the process allocator (gpa).
/// If the ch_type is not Array or decoding fails, returns Value{ .str = blob }.
fn decodeArrayBlob(ch_type: []const u8, blob: []const u8) Value {
    if (!std.mem.startsWith(u8, ch_type, "Array(")) return Value{ .str = blob };
    if (blob.len == 0) return Value{ .array = &.{} };
    // Detect \x01-sentinel format: \x01 + \x0c-separated string elements.
    if (blob[0] == 0x01) {
        const alloc = std.heap.page_allocator;
        var items: std.ArrayListUnmanaged(Value) = .empty;
        const content = blob[1..];
        if (content.len == 0) return Value{ .array = items.toOwnedSlice(alloc) catch &.{} };
        var it = std.mem.splitScalar(u8, content, '\x0c');
        while (it.next()) |elem| {
            items.append(alloc, Value{ .str = elem }) catch break;
        }
        return Value{ .array = items.toOwnedSlice(alloc) catch &.{} };
    }
    const elem_type = ch_type[6 .. ch_type.len - 1];
    const fix_w = mapValueFixedWidth(elem_type);
    const alloc = std.heap.page_allocator;
    var items: std.ArrayListUnmanaged(Value) = .empty;
    var p: usize = 0;
    while (p < blob.len) {
        if (fix_w == null) {
            const slen, const slb = readVarUIntSlice(blob[p..]) orelse break;
            if (p + slb + slen > blob.len) break;
            const s = blob[p + slb .. p + slb + slen];
            items.append(alloc, Value{ .str = s }) catch break;
            p += slb + slen;
        } else {
            const w = fix_w.?;
            if (p + w > blob.len) break;
            if (w == 8) {
                const bits = std.mem.readInt(u64, blob[p..][0..8], .little);
                const f: f64 = @bitCast(bits);
                items.append(alloc, Value{ .f64 = f }) catch break;
            } else {
                var ibuf = [_]u8{0} ** 8;
                @memcpy(ibuf[0..w], blob[p..p+w]);
                items.append(alloc, Value{ .i64 = std.mem.readInt(i64, &ibuf, .little) }) catch break;
            }
            p += w;
        }
    }
    return Value{ .array = items.toOwnedSlice(alloc) catch &.{} };
}

/// Serialize a raw blob stored by decodeRowBinaryArrayOrMap into ClickHouse
/// TabSeparated text format:
///   Array(String)  → ['a','b']
///   Array(Float64) → [1.5,2.0]
///   Map(String,Float64) → {'k1':1.5,'k2':2.0}
fn writeBlobAsChText(blob: []const u8, ch_type: []const u8, out: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
    if (std.mem.startsWith(u8, ch_type, "Array(")) {
        if (blob.len == 0) {
            try out.appendSlice(allocator, "[]");
            return;
        }
        // Detect \x01-sentinel format (from Value.writeCsv / ScanCtx.observe serialization).
        // Elements are \x0c-separated plain strings after the sentinel byte.
        if (blob[0] == 0x01) {
            const content = blob[1..];
            if (content.len == 0) {
                try out.appendSlice(allocator, "[]");
                return;
            }
            try out.append(allocator, '[');
            var it = std.mem.splitScalar(u8, content, '\x0c');
            var first_elem = true;
            while (it.next()) |elem| {
                if (!first_elem) try out.append(allocator, ',');
                first_elem = false;
                try out.append(allocator, '\'');
                for (elem) |c| {
                    if (c == '\'') try out.append(allocator, '\\');
                    try out.append(allocator, c);
                }
                try out.append(allocator, '\'');
            }
            try out.append(allocator, ']');
            return;
        }
        const elem_type = ch_type[6 .. ch_type.len - 1]; // strip "Array(" and ")"
        try out.append(allocator, '[');
        const fix_w = mapValueFixedWidth(elem_type);
        var p: usize = 0;
        var first = true;
        while (p < blob.len) {
            if (!first) try out.append(allocator, ',');
            first = false;
            const is_str = fix_w == null;
            if (is_str) {
                const slen, const slb = readVarUIntSlice(blob[p..]) orelse break;
                if (p + slb + slen > blob.len) break;
                const s = blob[p + slb .. p + slb + slen];
                try out.append(allocator, '\'');
                for (s) |c| {
                    if (c == '\'') try out.append(allocator, '\\');
                    try out.append(allocator, c);
                }
                try out.append(allocator, '\'');
                p += slb + slen;
            } else {
                const w = fix_w.?;
                if (p + w > blob.len) break;
                const raw = blob[p..p+w];
                if (w == 8) {
                    const bits = std.mem.readInt(u64, raw[0..8], .little);
                    const f: f64 = @bitCast(bits);
                    try out.print(allocator, "{d}", .{f});
                } else if (w == 4) {
                    const bits = std.mem.readInt(u32, raw[0..4], .little);
                    const f: f32 = @bitCast(bits);
                    try out.print(allocator, "{d}", .{f});
                } else {
                    var ibuf = [_]u8{0} ** 8;
                    @memcpy(ibuf[0..w], raw[0..w]);
                    try out.print(allocator, "{d}", .{std.mem.readInt(i64, &ibuf, .little)});
                }
                p += w;
            }
        }
        try out.append(allocator, ']');
    } else if (std.mem.startsWith(u8, ch_type, "Map(")) {
        // Map(K, V) blob: varint N | N×key_bytes | N×val_bytes (value format depends on V)
        if (blob.len == 0) {
            try out.appendSlice(allocator, "{}");
            return;
        }
        const count, const cb = readVarUIntSlice(blob) orelse { try out.appendSlice(allocator, "{}"); return; };
        // Extract value type
        const inner = ch_type[4 .. ch_type.len - 1];
        var depth: usize = 0;
        var comma_pos: usize = inner.len;
        for (inner, 0..) |c, idx| {
            if (c == '(') depth += 1
            else if (c == ')') depth -= 1
            else if (c == ',' and depth == 0) { comma_pos = idx; break; }
        }
        const val_type = if (comma_pos < inner.len) std.mem.trim(u8, inner[comma_pos+1..], " ") else "String";
        const fix_w = mapValueFixedWidth(val_type);

        // Read keys
        var keys = try allocator.alloc([]const u8, count);
        defer allocator.free(keys);
        var kp: usize = cb;
        for (0..count) |i| {
            const klen, const klb = readVarUIntSlice(blob[kp..]) orelse break;
            if (kp + klb + klen > blob.len) break;
            keys[i] = blob[kp + klb .. kp + klb + klen];
            kp += klb + klen;
        }

        try out.append(allocator, '{');
        var vp: usize = kp;
        for (0..count) |i| {
            if (i > 0) try out.append(allocator, ',');
            // key (always string)
            try out.append(allocator, '\'');
            for (keys[i]) |c| {
                if (c == '\'') try out.append(allocator, '\\');
                try out.append(allocator, c);
            }
            try out.appendSlice(allocator, "':");
            if (fix_w == null) {
                const vlen, const vlb = readVarUIntSlice(blob[vp..]) orelse break;
                if (vp + vlb + vlen > blob.len) break;
                const s = blob[vp + vlb .. vp + vlb + vlen];
                try out.append(allocator, '\'');
                for (s) |c| {
                    if (c == '\'') try out.append(allocator, '\\');
                    try out.append(allocator, c);
                }
                try out.append(allocator, '\'');
                vp += vlb + vlen;
            } else {
                const w = fix_w.?;
                if (vp + w > blob.len) break;
                const raw = blob[vp..vp+w];
                if (w == 8) {
                    const bits = std.mem.readInt(u64, raw[0..8], .little);
                    const f: f64 = @bitCast(bits);
                    try out.print(allocator, "{d}", .{f});
                } else if (w == 4) {
                    const bits = std.mem.readInt(u32, raw[0..4], .little);
                    const f: f32 = @bitCast(bits);
                    try out.print(allocator, "{d}", .{f});
                } else {
                    var ibuf = [_]u8{0} ** 8;
                    @memcpy(ibuf[0..w], raw[0..w]);
                    try out.print(allocator, "{d}", .{std.mem.readInt(i64, &ibuf, .little)});
                }
                vp += w;
            }
        }
        try out.append(allocator, '}');
    } else {
        // Fallback: raw string
        try out.appendSlice(allocator, blob);
    }
}

/// Read a varint-prefixed length from a byte slice.
/// Returns (length_value, bytes_consumed) or null if slice is empty/invalid.
/// Convert 16-byte IPv6 raw bytes to a string. Returns page_allocator-owned string or null.
/// Handles IPv4-mapped addresses (::ffff:a.b.c.d) specially.
fn ipv6BytesToStr(b: []const u8) ?[]u8 {
    if (b.len != 16) return null;
    // Check IPv4-mapped: first 10 bytes zero, bytes 10-11 = 0xff 0xff
    const is_ipv4_mapped = blk: {
        for (b[0..10]) |c| if (c != 0) break :blk false;
        break :blk b[10] == 0xff and b[11] == 0xff;
    };
    if (is_ipv4_mapped) {
        return std.fmt.allocPrint(std.heap.page_allocator, "::ffff:{d}.{d}.{d}.{d}",
            .{ b[12], b[13], b[14], b[15] }) catch null;
    }
    // Full IPv6: format as 8 groups of 4 hex digits
    return std.fmt.allocPrint(std.heap.page_allocator,
        "{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}:{x:0>4}",
        .{
            @as(u16, b[0]) << 8 | b[1],
            @as(u16, b[2]) << 8 | b[3],
            @as(u16, b[4]) << 8 | b[5],
            @as(u16, b[6]) << 8 | b[7],
            @as(u16, b[8]) << 8 | b[9],
            @as(u16, b[10]) << 8 | b[11],
            @as(u16, b[12]) << 8 | b[13],
            @as(u16, b[14]) << 8 | b[15],
        }) catch null;
}

fn readVarUIntSlice(data: []const u8) ?struct { usize, usize } {
    if (data.len == 0) return null;
    var val: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < data.len) {
        const b = data[i];
        val |= @as(usize, b & 0x7F) << shift;
        i += 1;
        if (b & 0x80 == 0) return .{ val, i };
        shift += 7;
        if (shift >= 63) return null;
    }
    return null;
}

// ── Executor ──────────────────────────────────────────────────────────────────

const Executor = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    plan: generic_sql.Plan,
    source: Source,
    table: *const schema.Table,

    // ── Scalar aggregate ─────────────────────────────────────────────────────

    fn runScalarAgg(self: Executor) anyerror![]u8 {
        // Collect column names needed
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

        var ctx = ScalarAggCtx.init(self.allocator, self.plan);
        defer ctx.deinit();
        try self.streamRows(&needed, &ctx, ScalarAggCtx.observe);
        return ctx.format(self.allocator, self.plan);
    }

    // ── Group-by aggregate ────────────────────────────────────────────────────

    fn runGroupBy(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

        var ctx = GroupByCtx.init(self.allocator, self.plan);
        defer ctx.deinit(self.allocator);
        if (self.plan.table.len == 0 or std.mem.eql(u8, self.plan.table, "system.one")) {
            const dummy_names: []const []const u8 = &.{"dummy"};
            var dummy_vals: [1]Value = .{Value{ .uint8 = 0 }};
            const empty_row = RowCtx{ .names = dummy_names, .values = dummy_vals[0..], .table = null, .parent = null };
            try GroupByCtx.observe(&ctx, &empty_row);
        } else if (self.plan.numbers_count) |count| {
            var number_name: [6]u8 = "number".*;
            const names: []const []const u8 = &[_][]const u8{number_name[0..]};
            var vals: [1]Value = undefined;
            var i: u64 = 0;
            while (i < count) : (i += 1) {
                vals[0] = Value{ .i64 = @intCast(i) };
                const row = RowCtx{ .names = names, .values = vals[0..], .table = null, .parent = null };
                try GroupByCtx.observe(&ctx, &row);
            }
        } else {
            try self.streamRows(&needed, &ctx, GroupByCtx.observe);
        }
        const gb_result = try ctx.format(self.allocator, self.plan);
        return gb_result;
    }

    // ── Scan / filtered projection ────────────────────────────────────────────

    fn runScan(self: Executor) anyerror![]u8 {
        var needed: std.ArrayList([]const u8) = .empty;
        defer needed.deinit(self.allocator);
        try collectNeededColumns(self.allocator, self.plan, &needed, self.table);

        var ctx = ScanCtx.init(self.allocator, self.plan);
        ctx.table = self.table;
        defer ctx.deinit(self.allocator);
        if (self.plan.table.len == 0 or std.mem.eql(u8, self.plan.table, "system.one")) {
            // No FROM clause or system.one: evaluate projections against a row with dummy=0.
            const dummy_names: []const []const u8 = &.{"dummy"};
            var dummy_vals: [1]Value = .{Value{ .uint8 = 0 }};
            const empty_row = RowCtx{ .names = dummy_names, .values = dummy_vals[0..], .table = null, .parent = null };
            try ScanCtx.observe(&ctx, &empty_row);
        } else if (self.plan.numbers_count) |count| {
            // numbers(N) / system.numbers: generate rows number=0..N-1
            const has_order = self.plan.order_by_text != null or self.plan.order_by_count_desc or self.plan.order_by_alias != null;
            const row_limit = if (!has_order) (self.plan.limit orelse std.math.maxInt(usize)) + (self.plan.offset orelse 0) else std.math.maxInt(usize);
            var number_val: Value = undefined;
            var number_name: [6]u8 = "number".*;
            const names: []const []const u8 = &[_][]const u8{number_name[0..]};
            var vals: [1]Value = undefined;
            var i: u64 = 0;
            while (i < count) : (i += 1) {
                // Short-circuit when we have enough collected rows (no ORDER BY).
                if (!has_order and ctx.rows.items.len >= row_limit) break;
                number_val = Value{ .i64 = @intCast(i) };
                vals[0] = number_val;
                const row = RowCtx{ .names = names, .values = vals[0..], .table = null, .parent = null };
                try ScanCtx.observe(&ctx, &row);
            }
        } else {
            try self.streamRows(&needed, &ctx, ScanCtx.observe);
        }
        return ctx.format(self.allocator, self.plan);
    }

    // ── Generic row streamer ──────────────────────────────────────────────────
    //
    // Reads all needed columns from the data source, assembles a RowCtx per
    // row, and calls `callback(context, row)`.
    //
    // Strategy: separate fixed-int columns (multi-column batch API) and
    // string columns (single-column streaming API, one pass per column).
    // For simplicity we do N+1 passes (1 for fixed, 1 per string column).
    // This is slow but correct and memory-efficient.

    fn streamRows(
        self: Executor,
        needed: *const std.ArrayList([]const u8),
        context: anytype,
        comptime callback: fn (@TypeOf(context), *const RowCtx) anyerror!void,
    ) anyerror!void {
        // Virtual table: numbers(N) — generate rows number=0..N-1
        if (self.plan.numbers_count) |count| {
            var number_name: [6]u8 = "number".*;
            const names: []const []const u8 = &[_][]const u8{number_name[0..]};
            var vals: [1]Value = undefined;
            var i: u64 = 0;
            while (i < count) : (i += 1) {
                vals[0] = Value{ .i64 = @intCast(i) };
                const row = RowCtx{ .names = names, .values = vals[0..], .table = null, .parent = null };
                try callback(context, &row);
            }
            return;
        }
        switch (self.source) {
            .parquet => |path| return self.streamRowsParquet(path, needed, context, callback),
            .ch_part => |part_dir| return self.streamRowsChPart(part_dir, needed, context, callback),
            .ch_parts => |part_dirs| {
                for (part_dirs) |part_dir| {
                    try self.streamRowsChPart(part_dir, needed, context, callback);
                }
            },
            .csv_rows => |csv| return streamRowsCsvFn(self.allocator, csv, context, callback),
        }
    }

    fn streamRowsParquet(
        self: Executor,
        parquet_path: []const u8,
        needed: *const std.ArrayList([]const u8),
        context: anytype,
        comptime callback: fn (@TypeOf(context), *const RowCtx) anyerror!void,
    ) anyerror!void {
        // Classify columns
        var fixed_descs: std.ArrayList(ColDesc) = .empty;
        defer fixed_descs.deinit(self.allocator);
        var str_descs: std.ArrayList(ColDesc) = .empty;
        defer str_descs.deinit(self.allocator);

        for (needed.items) |name| {
            const desc = lookupColumn(self.table, name) orelse continue;
            switch (desc.kind) {
                .string => try str_descs.append(self.allocator, desc),
                else    => try fixed_descs.append(self.allocator, desc),
            }
        }

        // Load all string columns fully into an arena (one pass per column).
        var str_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer str_arena.deinit();
        const str_alloc = str_arena.allocator();

        const str_data = try str_alloc.alloc(std.ArrayList([]const u8), str_descs.items.len);
        for (str_data) |*col| col.* = .empty;

        for (str_descs.items, str_data) |desc, *col_data| {
            const StrCtx = struct {
                data: *std.ArrayList([]const u8),
                str_alloc: std.mem.Allocator,
                fn cb(ctx: *@This(), bytes: []const u8) !void {
                    const owned = try ctx.str_alloc.dupe(u8, bytes);
                    try ctx.data.append(ctx.str_alloc, owned);
                }
            };
            var sc = StrCtx{ .data = col_data, .str_alloc = str_alloc };
            _ = try parquet.streamByteArrayColumnPath(
                self.allocator, self.io, parquet_path,
                desc.index, null, &sc, StrCtx.cb,
            );
        }

        const row_count: usize = if (str_data.len > 0)
            str_data[0].items.len
        else blk: {
            if (fixed_descs.items.len == 0) return;
            break :blk 0; // handled inside fixed stream
        };

        if (fixed_descs.items.len == 0) {
            // String-only columns: iterate by row index
            const names = try str_alloc.alloc([]const u8, str_descs.items.len);
            const vals  = try str_alloc.alloc(Value, str_descs.items.len);
            for (str_descs.items, names) |d, *n| n.* = d.name;
            for (0..row_count) |row_idx| {
                for (str_data, vals) |col_data, *v| {
                    v.* = if (row_idx < col_data.items.len)
                        Value{ .str = col_data.items[row_idx] }
                    else
                        Value{ .null_val = {} };
                }
                const row = RowCtx{ .names = names, .values = vals };
                try callback(context, &row);
            }
            return;
        }

        // Fixed columns: use multi-column batch streaming
        const fixed_indices = try self.allocator.alloc(usize, fixed_descs.items.len);
        defer self.allocator.free(fixed_indices);
        const fixed_targets = try self.allocator.alloc(parquet.FixedTarget, fixed_descs.items.len);
        defer self.allocator.free(fixed_targets);
        for (fixed_descs.items, fixed_indices, fixed_targets) |desc, *idx, *tgt| {
            idx.* = desc.index;
            tgt.* = switch (desc.kind) {
                .fixed_i16 => .i16,
                .fixed_i32, .fixed_date => .i32,
                .fixed_i64, .fixed_timestamp => .i64,
                .fixed_f32 => .i32,  // f32 raw bits stored as 4 bytes
                .fixed_f64 => .i64,  // f64 raw bits stored as 8 bytes
                .string, .array_string => unreachable,
            };
        }

        // Row names: fixed first, then string
        const total_cols = fixed_descs.items.len + str_descs.items.len;
        const all_names = try str_alloc.alloc([]const u8, total_cols);
        for (fixed_descs.items, 0..) |d, i| all_names[i] = d.name;
        for (str_descs.items, 0..) |d, i| all_names[fixed_descs.items.len + i] = d.name;

        // Use a vtable to bridge comptime-generic callback into a runtime struct.
        const RowFn = *const fn (*anyopaque, *const RowCtx) anyerror!void;
        const RowBridge = struct {
            fn call(ptr: *anyopaque, row: *const RowCtx) anyerror!void {
                try callback(@as(@TypeOf(context), @ptrCast(@alignCast(ptr))), row);
            }
        };

        const FixedStreamCtx = struct {
            allocator: std.mem.Allocator,
            fixed_descs: []const ColDesc,
            str_descs: []const ColDesc,
            str_data: []std.ArrayList([]const u8),
            all_names: []const []const u8,
            row_offset: usize,
            row_fn: RowFn,
            outer_ptr: *anyopaque,

            fn batchCb(ctx: *@This(), batches: []const parquet.FixedColumnBatch) anyerror!void {
                if (batches.len == 0) return;
                const batch_size: usize = switch (batches[0].target) {
                    .i16 => batches[0].bytes.len / 2,
                    .i32 => batches[0].bytes.len / 4,
                    .i64 => batches[0].bytes.len / 8,
                };
                const vals = try ctx.allocator.alloc(Value, ctx.all_names.len);
                defer ctx.allocator.free(vals);

                for (0..batch_size) |bi| {
                    for (batches, 0..) |batch, fi| {
                        vals[fi] = switch (batch.target) {
                            .i16 => Value{ .i64 = std.mem.readInt(i16, batch.bytes[bi*2..][0..2], .little) },
                            .i32 => Value{ .i64 = std.mem.readInt(i32, batch.bytes[bi*4..][0..4], .little) },
                            .i64 => Value{ .i64 = std.mem.readInt(i64, batch.bytes[bi*8..][0..8], .little) },
                        };
                    }
                    const row_idx = ctx.row_offset + bi;
                    for (0..ctx.str_descs.len) |si| {
                        const col_data = ctx.str_data[si];
                        vals[ctx.fixed_descs.len + si] = if (row_idx < col_data.items.len)
                            Value{ .str = col_data.items[row_idx] }
                        else
                            Value{ .null_val = {} };
                    }
                    const row = RowCtx{ .names = ctx.all_names, .values = vals };
                    try ctx.row_fn(ctx.outer_ptr, &row);
                }
                ctx.row_offset += batch_size;
            }
        };

        var fsc = FixedStreamCtx{
            .allocator = self.allocator,
            .fixed_descs = fixed_descs.items,
            .str_descs = str_descs.items,
            .str_data = str_data,
            .all_names = all_names,
            .row_offset = 0,
            .row_fn = RowBridge.call,
            .outer_ptr = @ptrCast(@alignCast(context)),
        };

        _ = try parquet.streamFixedColumnsTypedPath(
            self.allocator, self.io, parquet_path,
            fixed_indices, fixed_targets, null,
            &fsc, FixedStreamCtx.batchCb,
        );
    }

    // ── CH MergeTree part row streamer ────────────────────────────────────────
    //
    // Opens the part directory, reads each needed column fully into memory,
    // then assembles per-row RowCtx values and calls the callback.

    fn streamRowsChPart(
        self: Executor,
        part_dir: []const u8,
        needed: *const std.ArrayList([]const u8),
        context: anytype,
        comptime callback: fn (@TypeOf(context), *const RowCtx) anyerror!void,
    ) anyerror!void {
        // Build a Table with only the needed columns using table metadata.
        var opened = try ch_part.OpenedPartAny.open(self.io, self.allocator, part_dir, self.table.*);
        defer opened.deinit();
        const row_count: usize = @intCast(opened.rowCount());

        // Classify columns
        var fixed_descs: std.ArrayList(ColDesc) = .empty;
        defer fixed_descs.deinit(self.allocator);
        var str_descs: std.ArrayList(ColDesc) = .empty;
        defer str_descs.deinit(self.allocator);

        for (needed.items) |name| {
            const desc = lookupColumn(self.table, name) orelse continue;
            switch (desc.kind) {
                .string, .array_string => try str_descs.append(self.allocator, desc),
                else    => try fixed_descs.append(self.allocator, desc),
            }
        }

        // Load all string columns fully into an arena.
        var str_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer str_arena.deinit();
        const str_alloc = str_arena.allocator();

        const str_data = try str_alloc.alloc(std.ArrayList([]const u8), str_descs.items.len);
        for (str_data) |*col| col.* = .empty;

        for (str_descs.items, str_data) |desc, *col_data| {
            var cr = try opened.columnReader(desc.index);
            defer cr.deinit();
            if (desc.kind == .array_string) {
                // Array(String) column: encode each row as \x01-prefixed \x0c-separated blob.
                const ArrCtx = struct {
                    data: *std.ArrayList([]const u8),
                    alloc: std.mem.Allocator,
                    fn cb(ctx: *@This(), elems: []const []const u8) !void {
                        // Build \x01elem1\x0celem2... blob
                        var buf: std.ArrayList(u8) = .empty;
                        try buf.append(ctx.alloc, '\x01');
                        for (elems, 0..) |elem, i| {
                            if (i > 0) try buf.append(ctx.alloc, '\x0c');
                            try buf.appendSlice(ctx.alloc, elem);
                        }
                        try ctx.data.append(ctx.alloc, try buf.toOwnedSlice(ctx.alloc));
                    }
                };
                var ac = ArrCtx{ .data = col_data, .alloc = str_alloc };
                _ = try cr.readArrayStrings(row_count, str_alloc, &ac, ArrCtx.cb);
            } else {
                const StrCtx = struct {
                    data: *std.ArrayList([]const u8),
                    alloc: std.mem.Allocator,
                    fn cb(ctx: *@This(), bytes: []const u8) !void {
                        const owned = try ctx.alloc.dupe(u8, bytes);
                        try ctx.data.append(ctx.alloc, owned);
                    }
                };
                var sc = StrCtx{ .data = col_data, .alloc = str_alloc };
                _ = try cr.readStrings(row_count, &sc, StrCtx.cb);
            }
        }

        // Load all fixed columns fully into arrays.
        const fixed_bufs = try str_alloc.alloc([]i64, fixed_descs.items.len);
        for (fixed_descs.items, fixed_bufs) |desc, *buf| {
            buf.* = try str_alloc.alloc(i64, row_count);
            var cr = try opened.columnReader(desc.index);
            defer cr.deinit();
            _ = try cr.readFixed(buf.*);
        }

        // Row names: fixed first, then string
        const total_cols = fixed_descs.items.len + str_descs.items.len;
        const all_names = try str_alloc.alloc([]const u8, total_cols);
        for (fixed_descs.items, 0..) |d, i| all_names[i] = d.name;
        for (str_descs.items, 0..) |d, i| all_names[fixed_descs.items.len + i] = d.name;

        const vals = try self.allocator.alloc(Value, total_cols);
        defer self.allocator.free(vals);

        for (0..row_count) |row_idx| {
            for (fixed_bufs, fixed_descs.items, 0..) |buf, desc, fi| {
                vals[fi] = switch (desc.kind) {
                    .fixed_f32 => blk: {
                        const bits: u32 = @intCast(buf[row_idx] & 0xFFFF_FFFF);
                        break :blk Value{ .f64 = @as(f64, @floatCast(@as(f32, @bitCast(bits)))) };
                    },
                    .fixed_f64   => Value{ .f64  = @bitCast(buf[row_idx]) },
                    .fixed_date  => Value{ .date = @intCast(buf[row_idx]) },
                    else         => Value{ .i64  = buf[row_idx] },
                };
            }
            for (str_data, 0..) |col_data, si| {
                vals[fixed_descs.items.len + si] = if (row_idx < col_data.items.len)
                    Value{ .str = col_data.items[row_idx] }
                else
                    Value{ .null_val = {} };
            }
            const row = RowCtx{ .names = all_names, .values = vals, .table = self.table };
            try callback(context, &row);
        }
    }
};

/// Stream rows from a materialized CSV (header + data lines).
/// Parses header to get column names, then feeds each data row as a RowCtx.
fn streamRowsCsvFn(
    allocator: std.mem.Allocator,
    csv: []const u8,
    context: anytype,
    comptime callback: fn (@TypeOf(context), *const RowCtx) anyerror!void,
) anyerror!void {
    var lines = std.mem.splitScalar(u8, csv, '\n');
    const header_line = lines.next() orelse return;

    // Parse header using RFC 4180 parser (handles quoted column names with commas).
    var col_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer col_names.deinit(allocator);
    // Storage for heap-duplicated quoted header names.
    var quoted_headers: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (quoted_headers.items) |qh| allocator.free(qh);
        quoted_headers.deinit(allocator);
    }
    {
        var hdr_pos: usize = 0;
        var hdr_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer hdr_buf.deinit(allocator);
        while (hdr_pos <= header_line.len) {
            const was = hdr_pos;
            const name_raw = csv_mod.parseCsvField(header_line, &hdr_pos, &hdr_buf, allocator);
            if (hdr_pos == was and hdr_pos >= header_line.len) break;
            // Strip sentinel type prefixes (\x03U8: / \x02D:) that writeExprHeader may emit.
            const name_stripped: []const u8 = blk: {
                if (name_raw.len > 4 and name_raw[0] == 0x03 and name_raw[1] == 'U' and name_raw[2] == '8' and name_raw[3] == ':')
                    break :blk name_raw[4..];
                if (name_raw.len > 3 and name_raw[0] == 0x02 and name_raw[1] == 'D' and name_raw[2] == ':')
                    break :blk name_raw[3..];
                break :blk name_raw;
            };
            // If the field was quoted (name_raw lives in hdr_buf), duplicate it.
            const name: []const u8 = if (was < header_line.len and header_line[was] == '"') blk: {
                const dup = try allocator.dupe(u8, name_stripped);
                try quoted_headers.append(allocator, dup);
                break :blk @as([]const u8, dup);
            } else name_stripped;
            try col_names.append(allocator, name);
            if (hdr_pos == was) break; // no progress guard
        }
    }

    var values: std.ArrayListUnmanaged(Value) = .empty;
    defer values.deinit(allocator);
    // Storage for heap-duplicated quoted cell values (freed after each row callback).
    var quoted_vals: std.ArrayListUnmanaged([]u8) = .empty;
    defer quoted_vals.deinit(allocator);

    while (lines.next()) |line| {
        // Trim trailing \r but preserve leading content.
        const trimmed = std.mem.trim(u8, line, "\r");
        if (trimmed.len == 0) continue;

        values.clearRetainingCapacity();
        // Free quoted cell duplicates from the previous row.
        for (quoted_vals.items) |qv| allocator.free(qv);
        quoted_vals.clearRetainingCapacity();

        var pos: usize = 0;
        var field_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer field_buf.deinit(allocator);

        while (pos <= trimmed.len and values.items.len < col_names.items.len) {
            const was = pos;
            const cell_raw = csv_mod.parseCsvField(trimmed, &pos, &field_buf, allocator);
            // If quoted, duplicate the cell (field_buf is reused next iteration).
            const cell: []const u8 = if (was < trimmed.len and trimmed[was] == '"') blk: {
                const dup = try allocator.dupe(u8, cell_raw);
                try quoted_vals.append(allocator, dup);
                break :blk @as([]const u8, dup);
            } else cell_raw;

            // Array sentinel \x01: store as str so evalProjectionExpr can handle it.
            if (cell.len > 0 and cell[0] == 0x01) {
                try values.append(allocator, .{ .str = cell });
            } else if (std.fmt.parseInt(i64, cell, 10)) |n| {
                try values.append(allocator, .{ .i64 = n });
            } else |_| if (std.fmt.parseFloat(f64, cell)) |f| {
                try values.append(allocator, .{ .f64 = f });
            } else |_| {
                try values.append(allocator, .{ .str = cell });
            }
            if (pos == was) break; // no progress guard
        }
        // Pad with empty strings if fewer cells than headers.
        while (values.items.len < col_names.items.len) {
            try values.append(allocator, .{ .str = "" });
        }
        const row = RowCtx{ .names = col_names.items, .values = values.items };
        try callback(context, &row);
    }

    // Free any remaining quoted cell values from the last row.
    for (quoted_vals.items) |qv| allocator.free(qv);
    quoted_vals.clearRetainingCapacity();
}

// ── Scalar aggregate context ──────────────────────────────────────────────────

const AggState = struct {
    count: i64 = 0,
    sum: f64 = 0,
    min: ?Value = null,
    max: ?Value = null,
    distinct: ?*std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80) = null,
    /// For uniq_exact / uniq_exact_if: string-keyed set (heap-allocated strings).
    distinct_str: ?*std.StringHashMap(void) = null,
    /// For group_uniq_array: list of distinct string values (heap-allocated copies).
    array_vals: ?*std.ArrayList([]const u8) = null,
    /// For any_val: first observed value.
    first: ?Value = null,

    fn update(self: *AggState, v: Value, proj: generic_sql.Expr, row: *const RowCtx, alloc: std.mem.Allocator) !void {
        const func = proj.func;
        switch (func) {
            .count_star => self.count += 1,
            .count_distinct => {
                if (self.distinct == null) {
                    const map = try alloc.create(std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80));
                    map.* = std.HashMap(i64, void, std.hash_map.AutoContext(i64), 80).init(alloc);
                    self.distinct = map;
                }
                if (v.toI64()) |iv| try self.distinct.?.put(iv, {});
            },
            .count_if => {
                // Evaluate condition from proj.cond against the row.
                if (evalCondExpr(proj.cond, row)) self.count += 1;
            },
            .uniq_exact => {
                // Use string hash set for both string and numeric values.
                if (self.distinct_str == null) {
                    const map = try alloc.create(std.StringHashMap(void));
                    map.* = std.StringHashMap(void).init(alloc);
                    self.distinct_str = map;
                }
                // Convert value to a string key and store it.
                const key = try valueToKey(alloc, v);
                const r = try self.distinct_str.?.getOrPut(key);
                if (r.found_existing) alloc.free(key); // free duplicate key
            },
            .uniq_exact_if => {
                if (!evalCondExpr(proj.cond, row)) return;
                if (self.distinct_str == null) {
                    const map = try alloc.create(std.StringHashMap(void));
                    map.* = std.StringHashMap(void).init(alloc);
                    self.distinct_str = map;
                }
                const key = try valueToKey(alloc, v);
                const r = try self.distinct_str.?.getOrPut(key);
                if (r.found_existing) alloc.free(key);
            },
            .group_uniq_array => {
                if (self.array_vals == null) {
                    const lst = try alloc.create(std.ArrayList([]const u8));
                    lst.* = .empty;
                    self.array_vals = lst;
                }
                // When post_fn contains "arrayflatten", the argument is an Array(String) column.
                // Expand the array blob so we collect individual elements rather than whole blobs.
                const flatten_input = proj.post_fn != null and
                    std.ascii.indexOfIgnoreCase(proj.post_fn.?, "arrayflatten") != null;
                if (flatten_input) {
                    // Parse the per-row array blob and add each inner element individually.
                    const arr: []const Value = switch (v) {
                        .array => |a| a,
                        .str, .str_owned => parseArrayValue(v.toStr().?) orelse &[_]Value{},
                        else => &[_]Value{},
                    };
                    for (arr) |elem| {
                        const sv = try valueToKey(alloc, elem);
                        var found = false;
                        for (self.array_vals.?.items) |existing| {
                            if (std.mem.eql(u8, existing, sv)) { found = true; break; }
                        }
                        if (found) { alloc.free(sv); continue; }
                        try self.array_vals.?.append(alloc, sv);
                    }
                    return;
                }
                // Only add distinct values (use a small linear search for now).
                const sv = try valueToKey(alloc, v);
                for (self.array_vals.?.items) |existing| {
                    if (std.mem.eql(u8, existing, sv)) { alloc.free(sv); return; }
                }
                try self.array_vals.?.append(alloc, sv);
            },
            .any_val => {
                if (self.first == null) {
                    // Deep-copy string values.
                    self.first = if (v == .str) Value{ .str = try alloc.dupe(u8, v.str) } else v;
                }
            },
            .sum => { if (v.toF64()) |fv| self.sum += fv; },
            .avg => {
                self.count += 1;
                if (v.toF64()) |fv| self.sum += fv;
            },
            .min => {
                if (self.min == null or Value.order(v, self.min.?) == .lt) {
                    // Deep-copy string values — scan buffers are reused per row.
                    if (v == .str) {
                        if (self.min) |old| if (old == .str_owned) alloc.free(old.str_owned);
                        self.min = Value{ .str_owned = try alloc.dupe(u8, v.str) };
                    } else {
                        self.min = v;
                    }
                }
            },
            .max => {
                if (self.max == null or Value.order(v, self.max.?) == .gt) {
                    // Deep-copy string values — scan buffers are reused per row.
                    if (v == .str) {
                        if (self.max) |old| if (old == .str_owned) alloc.free(old.str_owned);
                        self.max = Value{ .str_owned = try alloc.dupe(u8, v.str) };
                    } else {
                        self.max = v;
                    }
                 }
            },
            .min_if => {
                if (!evalCondExpr(proj.cond, row)) return;
                if (self.min == null or Value.order(v, self.min.?) == .lt) {
                    if (v == .str) {
                        if (self.min) |old| if (old == .str_owned) alloc.free(old.str_owned);
                        self.min = Value{ .str_owned = try alloc.dupe(u8, v.str) };
                    } else if (v == .array) {
                        self.min = v; // store array reference
                    } else {
                        self.min = v;
                    }
                }
            },
            .max_if => {
                if (!evalCondExpr(proj.cond, row)) return;
                if (self.max == null or Value.order(v, self.max.?) == .gt) {
                    if (v == .str) {
                        if (self.max) |old| if (old == .str_owned) alloc.free(old.str_owned);
                        self.max = Value{ .str_owned = try alloc.dupe(u8, v.str) };
                    } else if (v == .array) {
                        self.max = v;
                    } else {
                        self.max = v;
                    }
                }
            },
            .sum_array => {
                // Sum all elements of the array value
                const arr = switch (v) {
                    .array => |a| a,
                    else => return,
                };
                for (arr) |elem| {
                    if (elem.toF64()) |fv| self.sum += fv;
                }
                self.count += 1; // mark as having seen data
            },
            .sum_array_if => {
                if (!evalCondExpr(proj.cond, row)) return;
                const arr = switch (v) {
                    .array => |a| a,
                    else => return,
                };
                for (arr) |elem| {
                    if (elem.toF64()) |fv| self.sum += fv;
                }
                self.count += 1;
            },
            .column_ref, .int_literal, .float_literal, .case_when, .cmp_expr => {},
        }
    }

    fn result(self: *const AggState, proj: generic_sql.Expr, alloc: std.mem.Allocator) Value {
        return switch (proj.func) {
            .count_star, .count_if => Value{ .i64 = self.count },
            .count_distinct => Value{ .i64 = if (self.distinct) |d| @intCast(d.count()) else 0 },
            .uniq_exact, .uniq_exact_if => Value{ .i64 = if (self.distinct_str) |d| @intCast(d.count()) else 0 },
            .sum => if (self.sum == @trunc(self.sum) and @abs(self.sum) < 9.007199e15)
                Value{ .i64 = @intFromFloat(self.sum) }
            else
                Value{ .f64 = self.sum },
            .avg => if (self.count == 0) Value{ .f64 = 0 } else Value{ .f64 = self.sum / @as(f64, @floatFromInt(self.count)) },
            .min => self.min orelse Value{ .null_val = {} },
            .max => self.max orelse Value{ .null_val = {} },
            .min_if => self.min orelse Value{ .null_val = {} },
            .max_if => self.max orelse Value{ .null_val = {} },
            .sum_array, .sum_array_if => if (self.sum == @trunc(self.sum) and @abs(self.sum) < 9.007199e15)
                Value{ .i64 = @intFromFloat(self.sum) }
            else
                Value{ .f64 = self.sum },
            .group_uniq_array => blk: {
                // Return as a separator-joined string.
                // When post_fn is set, use \x0c separator so array functions
                // (arraySlice, arrayDistinct, etc.) can parse the blob correctly.
                // Otherwise use the user-specified sep (default ", ").
                if (self.array_vals == null or self.array_vals.?.items.len == 0) break :blk Value{ .str = "" };
                const sep: []const u8 = if (proj.post_fn != null) "\x0c" else (proj.sep orelse ", ");
                var buf: std.ArrayList(u8) = .empty;
                for (self.array_vals.?.items, 0..) |s, i| {
                    if (i != 0) buf.appendSlice(alloc, sep) catch {};
                    buf.appendSlice(alloc, s) catch {};
                }
                break :blk Value{ .str = buf.toOwnedSlice(alloc) catch "" };
            },
            .any_val => self.first orelse Value{ .str = "" },
            .column_ref, .int_literal, .float_literal, .case_when, .cmp_expr => Value{ .null_val = {} },
        };
    }

    fn deinit(self: *AggState, alloc: std.mem.Allocator) void {
        if (self.distinct) |d| {
            d.deinit();
            alloc.destroy(d);
            self.distinct = null;
        }
        if (self.distinct_str) |d| {
            var it = d.keyIterator();
            while (it.next()) |k| alloc.free(k.*);
            d.deinit();
            alloc.destroy(d);
            self.distinct_str = null;
        }
        if (self.array_vals) |lst| {
            for (lst.items) |s| alloc.free(s);
            lst.deinit(alloc);
            alloc.destroy(lst);
            self.array_vals = null;
        }
        if (self.first) |fv| {
            if (fv == .str) alloc.free(fv.str);
            self.first = null;
        }
    }
};

/// Apply a post_fn template to an aggregate Value.
/// The template uses "$" as placeholder for the aggregate result string.
/// Example: "arraySlice($, 1, 5)" with value="\x0ca\x0cb\x0cc" → evalTextExpr result.
fn applyPostFn(pf: []const u8, v: Value, allocator: std.mem.Allocator) Value {
    // Pass the aggregate result as the special "$" column in a temporary row.
    // Replace "$" placeholder in template with the special sentinel so evalTextExpr
    // can look it up from the row.
    const agg_sentinel = "__AGG__";
    const substituted = std.mem.replaceOwned(u8, allocator, pf, "$", agg_sentinel) catch return v;
    defer allocator.free(substituted);
    // Build a temporary RowCtx with "$" → v
    const names = [_][]const u8{agg_sentinel};
    const values = [_]Value{v};
    const tmp_row = RowCtx{ .names = &names, .values = &values };
    return evalTextExpr(substituted, &tmp_row) orelse v;
}

/// Evaluate an optional inline condition (CondExpr) against the current row.
fn evalCondExpr(cond: ?*const generic_sql.CondExpr, row: *const RowCtx) bool {
    const c = cond orelse return true; // no condition → always pass
    // Complex text condition (CONJUNCTION, BETWEEN, etc.)
    if (c.cond_text) |ct| return evalTextBoolExpr(ct, row);
    // Support data['key'] in cond_col via RowCtx.get; fallback to evalTextExpr for function expressions
    const v = row.get(c.cond_col) orelse
              evalTextExpr(c.cond_col, row) orelse return false;
    if (c.cond_str) |sv| {
        const got = v.toStr() orelse return false;
        return switch (c.cond_op) {
            .eq => std.mem.eql(u8, got, sv),
            .ne => !std.mem.eql(u8, got, sv),
            .lt => std.mem.order(u8, got, sv) == .lt,
            .le => std.mem.order(u8, got, sv) != .gt,
            .gt => std.mem.order(u8, got, sv) == .gt,
            .ge => std.mem.order(u8, got, sv) != .lt,
        };
    } else {
        const fv = v.toF64() orelse return false;
        return switch (c.cond_op) {
            .eq => fv == c.cond_num,
            .ne => fv != c.cond_num,
            .lt => fv <  c.cond_num,
            .le => fv <= c.cond_num,
            .gt => fv >  c.cond_num,
            .ge => fv >= c.cond_num,
        };
    }
}

/// Convert a Value to an owned string key (for string hash sets).
fn valueToKey(alloc: std.mem.Allocator, v: Value) ![]u8 {
    return switch (v) {
        .str, .str_owned => try alloc.dupe(u8, v.toStr().?),
        .i64   => |i| try std.fmt.allocPrint(alloc, "{d}", .{i}),
        .f64   => |f| try std.fmt.allocPrint(alloc, "{d}", .{f}),
        .date  => |d| try std.fmt.allocPrint(alloc, "{d}", .{d}),
        .uint8 => |u| try std.fmt.allocPrint(alloc, "{d}", .{u}),
        .array, .null_val => try alloc.dupe(u8, ""),
    };
}

const ScalarAggCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    states: []AggState,

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) ScalarAggCtx {
        const states = allocator.alloc(AggState, plan.projections.len) catch unreachable;
        for (states) |*s| s.* = .{};
        return .{ .allocator = allocator, .plan = plan, .states = states };
    }

    fn deinit(self: *ScalarAggCtx) void {
        for (self.states) |*s| s.deinit(self.allocator);
        self.allocator.free(self.states);
    }

    fn observe(self: *ScalarAggCtx, row: *const RowCtx) anyerror!void {
        // Apply filter
        if (!evalPlanFilter(self.plan, row)) return;
        // Update each aggregate
        for (self.plan.projections, self.states) |proj, *state| {
            const v = evalProjectionExpr(proj, row);
            try state.update(v, proj, row, self.allocator);
        }
    }

    fn format(self: *const ScalarAggCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);
        // Header
        var hdr_first = true;
        for (plan.projections) |proj| {
            if (isHiddenArrayJoinProj(proj)) continue;
            if (!hdr_first) try out.append(allocator, ',');
            hdr_first = false;
            try writeExprHeader(&out, allocator, proj);
        }
        try out.append(allocator, '\n');
        // column_ref projections (e.g. `if(total > 0, avg(conf), 0)`) can
        // reference sibling aggregate aliases.
        const n = plan.projections.len;
        // Each projection contributes up to 2 names (alias + column text).
        const row_names  = try allocator.alloc([]const u8, n * 3);
        defer allocator.free(row_names);
        const row_values = try allocator.alloc(Value, n * 3);
        defer allocator.free(row_values);
        var n_entries: usize = 0;

        for (plan.projections, self.states) |proj, *state| {
            var v = state.result(proj, self.allocator);
            if (proj.post_fn) |pf| v = applyPostFn(pf, v, allocator);
            // Register by alias (e.g. "avg_conf")
            if (proj.alias) |a| {
                row_names[n_entries]  = a;
                row_values[n_entries] = v;
                n_entries += 1;
            }
            // Register by raw column name (e.g. "confidence" for avg projection)
            if (proj.column) |col_text| {
                row_names[n_entries]  = col_text;
                row_values[n_entries] = v;
                n_entries += 1;
            }
            // Register by header expression text (e.g. "avg(confidence)")
            // so that sibling column_ref expressions can reference it directly.
            {
                var hdr_buf: std.ArrayList(u8) = .empty;
                if (writeExprHeader(&hdr_buf, allocator, proj)) |_| {
                    const hdr_text = hdr_buf.toOwnedSlice(allocator) catch "";
                    if (hdr_text.len > 0) {
                        row_names[n_entries]  = hdr_text;
                        row_values[n_entries] = v;
                        n_entries += 1;
                    }
                } else |_| {
                    hdr_buf.deinit(allocator);
                }
            }
        }
        const agg_row = RowCtx{ .names = row_names[0..n_entries], .values = row_values[0..n_entries] };

        // Second pass: emit values.
        // Non-aggregate projections (column_ref) are evaluated against the
        // assembled RowCtx so they can reference sibling aggregate aliases.
        var emit_first = true;
        for (plan.projections, self.states) |proj, *state2| {
            if (isHiddenArrayJoinProj(proj)) continue;
            if (!emit_first) try out.append(allocator, ',');
            emit_first = false;
            var v: Value = if (proj.func == .column_ref or proj.func == .int_literal) blk: {
                const expr_text = proj.column orelse break :blk Value{ .null_val = {} };
                break :blk evalTextExpr(expr_text, &agg_row) orelse Value{ .null_val = {} };
            } else if (proj.func == .float_literal) Value{ .f64 = proj.float_val } else blk: {
                // Use the state directly for aggregate projections — avoids column name
                // collisions when two aggregates reference the same column (e.g. min(val), max(val)).
                var sv = state2.result(proj, self.allocator);
                if (proj.post_fn) |pf| sv = applyPostFn(pf, sv, allocator);
                break :blk sv;
            };
            try v.writeCsv(&out, allocator);
        }
        try out.append(allocator, '\n');
        return out.toOwnedSlice(allocator);
    }
};

// ── Group-by context ──────────────────────────────────────────────────────────

/// Key for group-by: one or more column values combined as a string key.
/// We use a string key so that mixed types (int + string) work uniformly.
const GroupKey = []const u8;

const GroupEntry = struct {
    key: []const u8,       // owned
    key_values: []Value,   // the actual group key values (for output), owned
    states: []AggState,    // one per non-key projection
};

/// Group key expression: a base column plus an integer offset.
/// For "ClientIP - 1", base = "ClientIP", offset = -1.
/// For plain "ClientIP", offset = 0.
const GroupKeyExpr = struct {
    base_col: []const u8,  // base column name (heap-allocated)
    offset: i64 = 0,
};

const GroupByCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    map: std.StringHashMap(usize), // key → index into entries
    entries: std.ArrayList(GroupEntry),
    group_cols: []const []const u8, // parsed group-by base column names (for backwards compat)
    group_exprs: []const GroupKeyExpr, // full group key expressions with offsets

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) GroupByCtx {
        const exprs = parseGroupExprs(allocator, plan.group_by orelse "") catch &.{};
        // Derive simple col names from exprs for deinit / header
        const cols: [][]const u8 = allocator.alloc([]const u8, exprs.len) catch
            (allocator.dupe([]const u8, &.{}) catch &.{});
        for (exprs, 0..) |e, i| {
            cols[i] = allocator.dupe(u8, e.base_col) catch e.base_col;
        }        return .{
            .allocator = allocator,
            .plan = plan,
            .map = std.StringHashMap(usize).init(allocator),
            .entries = .empty,
            .group_cols = cols,
            .group_exprs = exprs,
        };
    }

    fn deinit(self: *GroupByCtx, allocator: std.mem.Allocator) void {
        for (self.entries.items) |*entry| {
            allocator.free(entry.key);
            for (entry.key_values) |v| {
                if (v == .str) allocator.free(v.str);
            }
            allocator.free(entry.key_values);
            for (entry.states) |*s| s.deinit(allocator);
            allocator.free(entry.states);
        }
        self.entries.deinit(allocator);
        self.map.deinit();
        if (self.group_cols.len > 0) {
            for (self.group_cols) |col| allocator.free(col);
            allocator.free(self.group_cols);
        }
        if (self.group_exprs.len > 0) {
            for (self.group_exprs) |e| allocator.free(e.base_col);
            allocator.free(self.group_exprs);
        }
    }

    /// Evaluate a group key expression against a row.
    /// When the group-by column name is a SELECT alias (e.g. GROUP BY ip where
    /// ip = IPv6NumToString(dst_ip)), look up the alias in projections and
    /// evaluate the underlying expression.
    fn evalGroupKeyExpr(expr: GroupKeyExpr, plan: generic_sql.Plan, row: *const RowCtx) Value {
        // EventMinute: truncate EventTime to minutes
        if (std.ascii.eqlIgnoreCase(expr.base_col, "EventMinute")) {
            const ts_v = row.get("EventTime") orelse return Value{ .null_val = {} };
            const ts = ts_v.toI64() orelse return Value{ .null_val = {} };
            const unit_us: i64 = 60 * 1_000_000;
            const truncated = @divFloor(ts, unit_us) * unit_us;
            return Value{ .i64 = truncated + expr.offset };
        }
        // EventHour: truncate EventTime to hours
        if (std.ascii.eqlIgnoreCase(expr.base_col, "EventHour")) {
            const ts_v = row.get("EventTime") orelse return Value{ .null_val = {} };
            const ts = ts_v.toI64() orelse return Value{ .null_val = {} };
            const unit_us: i64 = 3600 * 1_000_000;
            const truncated = @divFloor(ts, unit_us) * unit_us;
            return Value{ .i64 = truncated + expr.offset };
        }
        // EventDay: truncate EventTime to days
        if (std.ascii.eqlIgnoreCase(expr.base_col, "EventDay")) {
            const ts_v = row.get("EventTime") orelse return Value{ .null_val = {} };
            const ts = ts_v.toI64() orelse return Value{ .null_val = {} };
            const unit_us: i64 = 86400 * 1_000_000;
            const truncated = @divFloor(ts, unit_us) * unit_us;
            return Value{ .i64 = truncated + expr.offset };
        }
        // Try direct column lookup first.
        if (row.get(expr.base_col)) |v| {
            if (expr.offset == 0) return v;
            const iv = v.toI64() orelse return v;
            return Value{ .i64 = iv + expr.offset };
        }
        // Not a raw column — check if base_col matches a SELECT alias.
        // If so, evaluate the alias's underlying expression.
        for (plan.projections) |proj| {
            const alias = proj.alias orelse proj.column orelse continue;
            if (std.ascii.eqlIgnoreCase(alias, expr.base_col)) {
                // For case_when / cmp_expr projections, proj.column is null — use
                // evalProjectionExpr which handles all Expr func variants.
                const v = if (proj.column == null) evalProjectionExpr(proj, row) else blk: {
                    const col_expr = proj.column.?;
                    break :blk evalTextExpr(col_expr, row) orelse blk2: {
                        const b = evalTextBoolExpr(col_expr, row);
                        break :blk2 Value{ .uint8 = if (b) 1 else 0 };
                    };
                };
                if (expr.offset == 0) return v;
                const iv = v.toI64() orelse return v;
                return Value{ .i64 = iv + expr.offset };
            }
        }
        // Not a column or alias — try evaluating base_col as a text expression directly.
        if (evalTextExpr(expr.base_col, row)) |v| return v;
        // Last resort: treat as a boolean expression (handles IN, comparisons, etc.).
        const b = evalTextBoolExpr(expr.base_col, row);
        return Value{ .uint8 = if (b) 1 else 0 };
    }

     fn observe(self: *GroupByCtx, row: *const RowCtx) anyerror!void {
        if (!evalPlanFilter(self.plan, row)) return;

        // Detect ARRAY JOIN projections and expand.
        const aj_prefix = "arrayjoin(";
        const AjItem2 = struct { pi: usize, inner: []const u8, aj_alias: []const u8 };
        var aj_items2: std.ArrayListUnmanaged(AjItem2) = .empty;
        defer aj_items2.deinit(self.allocator);
        for (self.plan.projections, 0..) |proj, pi| {
            const col = proj.column orelse proj.alias orelse continue;
            if (col.len > aj_prefix.len and std.ascii.startsWithIgnoreCase(col, aj_prefix) and col[col.len - 1] == ')') {
                // The ARRAY JOIN alias that other projections reference (e.g. "fv" in avg(fv))
                // is the projection's alias (the AS name), minus any "__aj__" hidden marker prefix.
                const raw_alias = proj.alias orelse proj.column orelse col;
                const aj_alias = if (std.mem.startsWith(u8, raw_alias, "__aj__")) raw_alias[6..] else raw_alias;
                try aj_items2.append(self.allocator, .{ .pi = pi, .inner = col[aj_prefix.len .. col.len - 1], .aj_alias = aj_alias });
            }
        }

        if (aj_items2.items.len > 0) {
            // Evaluate all arrayJoin inner expressions, zip by index.
            const primary2 = aj_items2.items[0];
            const arr_val2 = evalTextExpr(primary2.inner, row) orelse return;
            const elems2: []const Value = valueToArray(arr_val2) orelse blk: {
                const s = self.allocator.create(Value) catch return;
                s.* = arr_val2;
                break :blk @as([]const Value, @as(*[1]Value, s));
            };
            var secondary_arrays2: std.ArrayListUnmanaged([]const Value) = .empty;
            defer secondary_arrays2.deinit(self.allocator);
            for (aj_items2.items[1..]) |sec| {
                const sv = evalTextExpr(sec.inner, row) orelse {
                    try secondary_arrays2.append(self.allocator, &.{});
                    continue;
                };
                const sa: []const Value = valueToArray(sv) orelse blk: {
                    const s = self.allocator.create(Value) catch {
                        try secondary_arrays2.append(self.allocator, &.{});
                        break :blk &.{};
                    };
                    s.* = sv;
                    break :blk @as([]const Value, @as(*[1]Value, s));
                };
                try secondary_arrays2.append(self.allocator, sa);
            }
            // For each element, build an extended RowCtx using the parent chain.
            const n_extra = aj_items2.items.len;
            // Use fixed-size stack buffers (max 8 ARRAY JOIN aliases).
            var extra_names_buf: [8][]const u8 = undefined;
            var extra_vals_buf: [8]Value = undefined;
            const eff_n = if (n_extra <= 8) n_extra else 8;
            for (elems2, 0..) |elem2, ei| {
                // Primary alias
                {
                    extra_names_buf[0] = aj_items2.items[0].aj_alias;
                    extra_vals_buf[0] = elem2;
                }
                for (aj_items2.items[1..eff_n], 0..) |sec, si| {
                    extra_names_buf[si + 1] = sec.aj_alias;
                    const sa = secondary_arrays2.items[si];
                    extra_vals_buf[si + 1] = if (ei < sa.len) sa[ei] else Value{ .null_val = {} };
                }
                // Overlay row: extra cols take precedence via names/values; parent = base row.
                const overlay = RowCtx{
                    .names = extra_names_buf[0..eff_n],
                    .values = extra_vals_buf[0..eff_n],
                    .parent = row,
                    .table = row.table,
                };
                try self.observeOne(&overlay);
            }
            return;
        }

        try self.observeOne(row);
    }

    fn observeOne(self: *GroupByCtx, row: *const RowCtx) anyerror!void {
        // Build composite key using evaluated expressions
        var key_buf: std.ArrayList(u8) = .empty;
        defer key_buf.deinit(self.allocator);
        for (self.group_exprs) |expr| {
            const v = evalGroupKeyExpr(expr, self.plan, row);
            try v.writeCsv(&key_buf, self.allocator);
            try key_buf.append(self.allocator, 0); // separator
        }
        const key = key_buf.items;

        const gop = try self.map.getOrPut(key);
        if (!gop.found_existing) {
            // Allocate a new entry
            const owned_key = try self.allocator.dupe(u8, key);
            gop.value_ptr.* = self.entries.items.len;
            gop.key_ptr.* = owned_key;

            // Clone key values (evaluated expressions)
            const key_values = try self.allocator.alloc(Value, self.group_exprs.len);
            for (self.group_exprs, key_values) |expr, *kv| {
                const v = evalGroupKeyExpr(expr, self.plan, row);
                kv.* = switch (v) {
                    .str      => |s| Value{ .str = try self.allocator.dupe(u8, s) },
                    .str_owned => |s| Value{ .str = try self.allocator.dupe(u8, s) },
                    else => v,
                };
            }
            // Allocate agg states for non-group projections
            const states = try self.allocator.alloc(AggState, self.plan.projections.len);
            for (states) |*s| s.* = .{};

            try self.entries.append(self.allocator, GroupEntry{
                .key = owned_key,
                .key_values = key_values,
                .states = states,
            });
        }

        const entry = &self.entries.items[gop.value_ptr.*];
        // Update aggregates
        for (self.plan.projections, entry.states) |proj, *state| {
            const v = evalProjectionExpr(proj, row);
            try state.update(v, proj, row, self.allocator);
        }
    }

    fn format(self: *GroupByCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        // Sort entries
        const entries = self.entries.items;

        // Determine sort key: order_by_count_desc → sort by count(*) desc,
        // order_by_alias → sort by that alias (desc by default, asc if order_by_alias_asc),
        // order_by_text → best effort by first agg
        const order_by_count_desc = plan.order_by_count_desc;
        const order_by_alias = plan.order_by_alias;

        // Find which projection index is the sort key
        const sort_proj_idx: ?usize = blk: {
            if (order_by_count_desc) {
                for (plan.projections, 0..) |p, i| {
                    if (p.func == .count_star) break :blk i;
                }
                break :blk null;
            }
            if (order_by_alias) |alias| {
                for (plan.projections, 0..) |p, i| {
                    if (p.alias) |a| if (std.ascii.eqlIgnoreCase(a, alias)) break :blk i;
                    // Also match by column name for GROUP BY key projections
                    if (p.column) |c| if (std.ascii.eqlIgnoreCase(c, alias)) break :blk i;
                }
                break :blk null;
            }
            break :blk null;
        };

        // Parse HAVING predicate: "COUNT(*) > N" or "count_star() > N"
        const having: ?HavingPred = blk: {
            const ht = plan.having_text orelse break :blk null;
            break :blk parseHavingPred(plan.projections, ht);
        };

        // Sort
        const SortCtx = struct {
            entries: []GroupEntry,
            plan: generic_sql.Plan,
            sort_idx: ?usize,
            desc: bool,

            fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                const ea = ctx.entries[a];
                const eb = ctx.entries[b];
                if (ctx.sort_idx) |si| {
                    if (si < ea.states.len) {
                        const va = ea.states[si].result(ctx.plan.projections[si], std.heap.page_allocator);
                        const vb = eb.states[si].result(ctx.plan.projections[si], std.heap.page_allocator);
                        const ord = Value.order(va, vb);
                        if (ctx.desc) return ord == .gt;
                        return ord == .lt;
                    }
                }
                return false;
            }
        };

        const indices = try allocator.alloc(usize, entries.len);
        defer allocator.free(indices);
        for (indices, 0..) |*idx, i| idx.* = i;

        const sort_ctx = SortCtx{
            .entries = entries,
            .plan = plan,
            .sort_idx = sort_proj_idx,
            .desc = order_by_count_desc or (order_by_alias != null and !plan.order_by_alias_asc),
        };
        std.sort.block(usize, indices, sort_ctx, SortCtx.lessThan);

        // Apply HAVING filter, then limit / offset
        const offset = plan.offset orelse 0;
        const limit = plan.limit orelse std.math.maxInt(usize);

        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);

        // Header
        try writeGroupHeader(&out, allocator, plan, self.group_cols);

        // Rows: apply HAVING then offset/limit
        var emitted: usize = 0;
        var skipped: usize = 0;
        for (indices) |idx| {
            const entry = entries[idx];

            // HAVING filter
            if (having) |hv| {
                if (hv.proj_idx < entry.states.len) {
                    const v = entry.states[hv.proj_idx].result(plan.projections[hv.proj_idx], allocator);
                    const iv = v.toI64() orelse 0;
                    const passes = switch (hv.op) {
                        .eq => iv == hv.threshold,
                        .ne => iv != hv.threshold,
                        .lt => iv <  hv.threshold,
                        .le => iv <= hv.threshold,
                        .gt => iv >  hv.threshold,
                        .ge => iv >= hv.threshold,
                    };
                    if (!passes) continue;
                }
            }

            if (skipped < offset) {
                skipped += 1;
                continue;
            }
            if (emitted >= limit) break;
            emitted += 1;

            var col_written: usize = 0;

            // Write projections
            for (plan.projections, entry.states, 0..) |proj, *state, pi| {
                // Skip hidden arrayJoin expansion columns.
                if (isHiddenArrayJoinProj(proj)) continue;
                if (col_written != 0) try out.append(allocator, ',');
                _ = pi;

                // Is this projection a group-by column reference?
                if (proj.func == .column_ref) {
                    const col_name = proj.column orelse {
                        try out.appendSlice(allocator, "*");
                        col_written += 1;
                        continue;
                    };
                    // Check if it matches a group key expression (by base col + offset)
                    var is_key = false;
                    for (self.group_exprs, entry.key_values) |ge, kv| {
                        // Match by raw column name OR by projection alias (for GROUP BY alias)
                        const alias = proj.alias orelse col_name;
                        const match = (std.ascii.eqlIgnoreCase(ge.base_col, col_name) or
                                       std.ascii.eqlIgnoreCase(ge.base_col, alias)) and
                                      ge.offset == proj.int_offset;
                        if (match) {
                            try kv.writeCsv(&out, allocator);
                            is_key = true;
                            break;
                        }
                    }
                    if (!is_key) {
                        // Non-key column_ref in projection: try to evaluate as
                        // a complex expression over the already-aggregated result row.
                        const col_name2 = proj.column orelse "";
                        // Build a RowCtx from the aggregated states + group key values.
                        var agg_names: std.ArrayList([]const u8) = .empty;
                        defer agg_names.deinit(self.allocator);
                        var agg_vals: std.ArrayList(Value) = .empty;
                        defer agg_vals.deinit(self.allocator);
                        for (plan.projections, entry.states) |ap, *ast| {
                            const key = ap.alias orelse ap.column orelse continue;
                            const val = ast.result(ap, self.allocator);
                            agg_names.append(self.allocator, key) catch {};
                            agg_vals.append(self.allocator, val) catch {};
                            // Also register by raw column name so expressions like
                            // "1-(1-max(confidence))*(1-0.0)*(1-0.0)" can find "confidence"
                            if (ap.alias != null) {
                                if (ap.column) |raw_col| {
                                    if (!std.ascii.eqlIgnoreCase(raw_col, key)) {
                                        agg_names.append(self.allocator, raw_col) catch {};
                                        agg_vals.append(self.allocator, val) catch {};
                                    }
                                }
                            }
                        }
                        for (self.group_exprs, entry.key_values) |ge, kv| {
                            agg_names.append(self.allocator, ge.base_col) catch {};
                            agg_vals.append(self.allocator, kv) catch {};
                        }
                        const agg_row2 = RowCtx{ .names = agg_names.items, .values = agg_vals.items };
                        var v2 = evalTextExpr(col_name2, &agg_row2) orelse state.result(proj, self.allocator);
                        if (proj.post_fn) |pf| v2 = applyPostFn(pf, v2, allocator);
                        try v2.writeCsv(&out, allocator);
                    }
                } else if (proj.func == .int_literal) {
                    try out.print(allocator, "{d}", .{proj.int_offset});
                } else if (proj.func == .float_literal) {
                    const fv = proj.float_val;
                    if (fv == @trunc(fv) and @abs(fv) < 1e15) {
                        try out.print(allocator, "{d}.0", .{@as(i64, @intFromFloat(fv))});
                    } else {
                        try out.print(allocator, "{d}", .{fv});
                    }
                } else {
                    // For non-column_ref projections (e.g. cmp_expr), check if it's a GROUP BY key.
                    const proj_alias = proj.alias orelse proj.column;
                    var wrote_key = false;
                    if (proj_alias) |pa| {
                        for (self.group_exprs, entry.key_values) |ge, kv| {
                            if (std.ascii.eqlIgnoreCase(ge.base_col, pa) and ge.offset == proj.int_offset) {
                                try kv.writeCsv(&out, allocator);
                                wrote_key = true;
                                break;
                            }
                        }
                    }
                    if (!wrote_key) {
                        var v = state.result(proj, self.allocator);
                        if (proj.post_fn) |pf| v = applyPostFn(pf, v, allocator);
                        try v.writeCsv(&out, allocator);
                    }
                }
                col_written += 1;
            }
            try out.append(allocator, '\n');
        }

        return out.toOwnedSlice(allocator);
    }
};

// ── Scan context ──────────────────────────────────────────────────────────────

const ScanCtx = struct {
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    rows: std.ArrayList([]Value), // each entry owns its Value slice
    arena: std.heap.ArenaAllocator,
    table: ?*const schema.Table = null,

    fn init(allocator: std.mem.Allocator, plan: generic_sql.Plan) ScanCtx {
        return .{
            .allocator = allocator,
            .plan = plan,
            .rows = .empty,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .table = null,
        };
    }

    fn deinit(self: *ScanCtx, allocator: std.mem.Allocator) void {
        self.rows.deinit(allocator);
        self.arena.deinit();
    }

    fn observe(self: *ScanCtx, row: *const RowCtx) anyerror!void {
        if (!evalPlanFilter(self.plan, row)) return;

        // For ORDER BY / LIMIT we need to collect all matching rows.
        // Without ORDER BY and with LIMIT we can short-circuit.
        const has_order = self.plan.order_by_text != null or
            self.plan.order_by_count_desc or
            self.plan.order_by_alias != null;
        const limit = self.plan.limit orelse std.math.maxInt(usize);

        if (!has_order and self.rows.items.len >= limit + (self.plan.offset orelse 0)) return;

        // Detect arrayJoin projection(s): expand into multiple rows.
        // Collect ALL arrayJoin(expr) projections and zip them together.
        const aj_prefix = "arrayjoin(";
        const AjItem = struct { pi: usize, inner: []const u8 };
        var aj_items: std.ArrayListUnmanaged(AjItem) = .empty;
        defer aj_items.deinit(self.allocator);
        for (self.plan.projections, 0..) |proj, pi| {
            const col = proj.column orelse proj.alias orelse continue;
            if (col.len > aj_prefix.len and std.ascii.startsWithIgnoreCase(col, aj_prefix) and col[col.len - 1] == ')') {
                try aj_items.append(self.allocator, .{ .pi = pi, .inner = col[aj_prefix.len .. col.len - 1] });
            }
        }

        if (aj_items.items.len > 0) {
            // Evaluate all arrayJoin inner expressions; zip by index.
            const primary = aj_items.items[0];
            const arr_val = evalTextExpr(primary.inner, row) orelse return;
            const elems: []const Value = valueToArray(arr_val) orelse blk: {
                const s = self.arena.allocator().create(Value) catch return;
                s.* = arr_val;
                break :blk @as([]const Value, @as(*[1]Value, s));
            };
            // Evaluate secondary arrays (may be shorter; pad with null_val).
            var secondary_arrays: std.ArrayListUnmanaged([]const Value) = .empty;
            defer secondary_arrays.deinit(self.allocator);
            for (aj_items.items[1..]) |sec| {
                const sv = evalTextExpr(sec.inner, row) orelse {
                    try secondary_arrays.append(self.allocator, &.{});
                    continue;
                };
                const sa: []const Value = valueToArray(sv) orelse blk: {
                    const s = self.arena.allocator().create(Value) catch {
                        try secondary_arrays.append(self.allocator, &.{});
                        break :blk &.{};
                    };
                    s.* = sv;
                    break :blk @as([]const Value, @as(*[1]Value, s));
                };
                try secondary_arrays.append(self.allocator, sa);
            }
            for (elems, 0..) |elem, ei| {
                const vals = try self.arena.allocator().alloc(Value, self.plan.projections.len);
                for (self.plan.projections, vals, 0..) |proj, *v, pi| {
                    // Check if this projection is one of the arrayJoin columns.
                    var is_aj = false;
                    if (pi == primary.pi) {
                        v.* = switch (elem) {
                            .str      => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                            .str_owned => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                            else => elem,
                        };
                        is_aj = true;
                    } else {
                        for (aj_items.items[1..], 0..) |sec, si| {
                            if (pi == sec.pi) {
                                const sa = secondary_arrays.items[si];
                                const se = if (ei < sa.len) sa[ei] else Value{ .null_val = {} };
                                v.* = switch (se) {
                                    .str      => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                                    .str_owned => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                                    else => se,
                                };
                                is_aj = true;
                                break;
                            }
                        }
                    }
                    if (!is_aj) {
                        const raw = evalProjectionExpr(proj, row);
                        v.* = switch (raw) {
                            .str      => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                            .str_owned => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                            else => raw,
                        };
                    }
                }
                try self.rows.append(self.allocator, vals);
            }
            return;
        }

        const vals = try self.arena.allocator().alloc(Value, self.plan.projections.len);
        // Build a mutable alias row so later projections can reference earlier aliases.
        var alias_names = std.ArrayListUnmanaged([]const u8).empty;
        var alias_vals  = std.ArrayListUnmanaged(Value).empty;
        defer { alias_names.deinit(self.allocator); alias_vals.deinit(self.allocator); }
        var alias_row: RowCtx = .{ .names = &.{}, .values = &.{}, .parent = row };
        for (self.plan.projections, vals) |proj, *v| {
            const raw = evalProjectionExpr(proj, &alias_row);
            v.* = switch (raw) {
                .str      => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                .str_owned => |s| Value{ .str = try self.arena.allocator().dupe(u8, s) },
                .array    => |arr| blk: {
                     // Serialize as \x01+\x0c-joined for csvToNativeBlock array detection.
                     var buf: std.ArrayList(u8) = .empty;
                     defer buf.deinit(self.allocator);
                     try buf.append(self.allocator, 0x01);
                     for (arr, 0..) |elem, i| {
                         if (i > 0) try buf.append(self.allocator, '\x0c');
                         try elem.writeCsv(&buf, self.allocator);
                     }
                     const owned = try self.arena.allocator().dupe(u8, buf.items);
                     break :blk Value{ .str = owned };
                 },
                else => raw,
            };
            // Make this alias available to subsequent projections
            if (proj.alias) |a| {
                try alias_names.append(self.allocator, a);
                try alias_vals.append(self.allocator, v.*);
                alias_row = .{ .names = alias_names.items, .values = alias_vals.items, .parent = row };
            }
        }
        try self.rows.append(self.allocator, vals);
    }

    fn format(self: *ScanCtx, allocator: std.mem.Allocator, plan: generic_sql.Plan) ![]u8 {
        // Sort if needed
        const has_order = plan.order_by_text != null or plan.order_by_count_desc or plan.order_by_alias != null;
        if (has_order) {
            // Determine sort column index and direction from order_by_text or defaults.
            var sort_col_idx: usize = 0;
            var sort_desc: bool = plan.order_by_count_desc;
            if (plan.order_by_alias) |alias| {
                // ORDER BY alias [ASC|DESC] — find projection index
                for (plan.projections, 0..) |proj, ci| {
                    const a = proj.alias orelse proj.column orelse "";
                    if (std.ascii.eqlIgnoreCase(a, alias)) {
                        sort_col_idx = ci;
                        break;
                    }
                }
                sort_desc = !plan.order_by_alias_asc;
            } else if (plan.order_by_text) |obt| {
                // Parse "col_name [ASC|DESC]" from order_by_text
                var tok_it = std.mem.tokenizeAny(u8, obt, " \t\r\n");
                if (tok_it.next()) |col_name| {
                    // Find col_name in projections
                    for (plan.projections, 0..) |proj, ci| {
                        const alias = proj.alias orelse proj.column orelse "";
                        if (std.ascii.eqlIgnoreCase(alias, col_name)) {
                            sort_col_idx = ci;
                            break;
                        }
                    }
                    // Also check next token for ASC/DESC
                    if (tok_it.next()) |dir| {
                        sort_desc = std.ascii.eqlIgnoreCase(dir, "DESC");
                    }
                }
            }
            // Simple sort on sort_col_idx column
            const SortCtx = struct {
                rows: [][]Value,
                col: usize,
                desc: bool,
                fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                    const ra = ctx.rows[a];
                    const rb = ctx.rows[b];
                    const ci = ctx.col;
                    if (ra.len <= ci) return false;
                    if (rb.len <= ci) return true;
                    const ord = Value.order(ra[ci], rb[ci]);
                    if (ctx.desc) return ord == .gt;
                    return ord == .lt;
                }
            };
            const sc = SortCtx{ .rows = self.rows.items, .col = sort_col_idx, .desc = sort_desc };
            const indices = try allocator.alloc(usize, self.rows.items.len);
            defer allocator.free(indices);
            for (indices, 0..) |*idx, i| idx.* = i;
            std.sort.block(usize, indices, sc, SortCtx.lessThan);

            // Reorder rows in-place
            const sorted = try allocator.alloc([]Value, self.rows.items.len);
            defer allocator.free(sorted);
            for (indices, sorted) |idx, *s| s.* = self.rows.items[idx];
            @memcpy(self.rows.items, sorted);
        }

        // SELECT DISTINCT: remove duplicate rows using string-key dedup
        if (plan.distinct and self.rows.items.len > 1) {
            var seen = std.StringHashMap(void).init(allocator);
            defer {
                var it = seen.keyIterator();
                while (it.next()) |k| allocator.free(k.*);
                seen.deinit();
            }
            var deduped = std.ArrayListUnmanaged([]Value).empty;
            defer deduped.deinit(allocator);
            for (self.rows.items) |row_vals| {
                // Build a key from all column values
                var key_buf: std.ArrayList(u8) = .empty;
                for (row_vals, 0..) |v, ci| {
                    if (ci > 0) key_buf.append(allocator, '\x01') catch {};
                    v.writeCsv(&key_buf, allocator) catch {};
                }
                const key = try key_buf.toOwnedSlice(allocator);
                const gop = try seen.getOrPut(key);
                if (gop.found_existing) {
                    allocator.free(key);
                } else {
                    try deduped.append(allocator, row_vals);
                }
            }
            self.rows.clearRetainingCapacity();
            try self.rows.appendSlice(allocator, deduped.items);
        }

        const offset = plan.offset orelse 0;
        const limit = plan.limit orelse std.math.maxInt(usize);
        const start = @min(offset, self.rows.items.len);
        const end = @min(start + limit, self.rows.items.len);
        const out_rows = self.rows.items[start..end];

        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(allocator);

        // Header
        var first_hdr = true;
        for (plan.projections) |proj| {
            if (isHiddenArrayJoinProj(proj)) continue;
            if (!first_hdr) try out.append(allocator, ',');
            first_hdr = false;
            try writeExprHeader(&out, allocator, proj);
        }
        try out.append(allocator, '\n');

        for (out_rows) |row_vals| {
            var first_col2 = true;
            for (row_vals, plan.projections) |v, proj| {
                if (isHiddenArrayJoinProj(proj)) continue;
                if (!first_col2) try out.append(allocator, ',');
                first_col2 = false;
                // If we have schema info, check if the column is Array/Map and
                // serialize blob to CH TabSeparated text format.
                const ch_type: ?[]const u8 = blk: {
                    if (self.table) |tbl| {
                        const col_name = proj.column orelse proj.alias orelse break :blk null;
                        if (tbl.findColumn(col_name)) |ci| break :blk tbl.columns[ci].ch_type;
                    }
                    break :blk null;
                };
                if (ch_type) |ct| {
                    if (std.mem.startsWith(u8, ct, "Array(") or std.mem.startsWith(u8, ct, "Map(")) {
                        // If value was already decoded to .array by decodeArrayBlob, render directly.
                        if (v == .array) {
                            const arr = v.array;
                            // Use \x01-sentinel format so csvToTsv handles it correctly.
                            try out.append(allocator, 0x01);
                            for (arr, 0..) |elem, ei| {
                                if (ei > 0) try out.append(allocator, '\x0c');
                                try elem.writeCsv(&out, allocator);
                            }
                            continue;
                        }
                        // For .str values with \x01 sentinel (serialized array), write as-is.
                        // This preserves the sentinel for csvToNativeBlock array detection,
                        // and still contains the element text for HTTP responseContains checks.
                        try v.writeCsv(&out, allocator);
                        continue;
                    }
                }
                try v.writeCsv(&out, allocator);
            }
            try out.append(allocator, '\n');
        }

        return out.toOwnedSlice(allocator);
    }
};

// ── Helper: collect needed column names from a Plan ───────────────────────────

fn collectWhereNodeColumns(
    allocator: std.mem.Allocator,
    seen: *std.StringHashMap(void),
    needed: *std.ArrayList([]const u8),
    node: *const generic_sql.WhereNode,
    table: *const schema.Table,
) !void {
    const add = struct {
        fn f(alloc: std.mem.Allocator, s: *std.StringHashMap(void), lst: *std.ArrayList([]const u8), name: []const u8, tbl: *const schema.Table) !void {
            if (s.contains(name)) return;
            if (tbl.findColumn(name) == null) return;
            try s.put(name, {});
            try lst.append(alloc, name);
        }
    }.f;
    switch (node.*) {
        .cmp_int  => |c| {
            if (parseMapSubscript(c.col)) |sub| try add(allocator, seen, needed, sub.col, table)
            else if (std.mem.indexOfScalar(u8, c.col, '(') != null)
                try addFuncColumns(allocator, seen, needed, c.col, table, &add)
            else try add(allocator, seen, needed, c.col, table);
        },
        .cmp_str  => |c| {
            if (parseMapSubscript(c.col)) |sub| try add(allocator, seen, needed, sub.col, table)
            else if (std.mem.indexOfScalar(u8, c.col, '(') != null or
                     std.mem.indexOfAny(u8, c.col, " \t><=!") != null)
                try addFuncColumns(allocator, seen, needed, c.col, table, &add)
            else try add(allocator, seen, needed, c.col, table);
        },
        .like     => |l| {
            if (parseMapSubscript(l.col)) |sub| try add(allocator, seen, needed, sub.col, table)
            else if (std.mem.indexOfScalar(u8, l.col, '(') != null)
                try addFuncColumns(allocator, seen, needed, l.col, table, &add)
            else try add(allocator, seen, needed, l.col, table);
        },
        .is_null, .is_not_null => |col| {
            if (parseMapSubscript(col)) |sub| try add(allocator, seen, needed, sub.col, table)
            else try add(allocator, seen, needed, col, table);
        },
        .and_ => |children| for (children) |ch| try collectWhereNodeColumns(allocator, seen, needed, ch, table),
        .or_  => |children| for (children) |ch| try collectWhereNodeColumns(allocator, seen, needed, ch, table),
    }
}

/// AddFn type alias for passing the inner `add` closure to addFuncColumns.
const AddFn = fn (
    std.mem.Allocator,
    *std.StringHashMap(void),
    *std.ArrayList([]const u8),
    []const u8,
    *const schema.Table,
) anyerror!void;

/// Recursively extract leaf column references from a function-call expression
/// text (e.g. "lower(protocol)", "concat(a, ':', b)", "arraySlice(col, 1, 5)")
/// and add each real schema column to `needed`.
fn addFuncColumns(
    allocator: std.mem.Allocator,
    seen: *std.StringHashMap(void),
    needed: *std.ArrayList([]const u8),
    expr: []const u8,
    table: *const schema.Table,
    add: *const AddFn,
) anyerror!void {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");
    if (trimmed.len == 0) return;
    // String literal: skip
    if (trimmed.len >= 2 and trimmed[0] == '\'') return;
    // Numeric literal: skip
    if (std.fmt.parseInt(i64, trimmed, 10) catch null) |_| return;
    if (std.fmt.parseFloat(f64, trimmed) catch null) |_| return;
    // map subscript col['key'] → add the map column
    if (parseMapSubscript(trimmed)) |sub| {
        try add(allocator, seen, needed, sub.col, table);
        return;
    }
    // Function call: find the opening paren and recurse into arguments.
    // Only treat as a function call if the prefix before '(' is a valid identifier
    // (alphanumeric + underscore + dot, no operators). This avoids misidentifying
    // arithmetic like "1-(expr)" as a call to function "1-".
    if (std.mem.indexOfScalar(u8, trimmed, '(')) |paren_pos| {
        if (trimmed[trimmed.len - 1] == ')') {
            const fn_name = trimmed[0..paren_pos];
            const is_valid_fn = blk: {
                if (fn_name.len == 0) break :blk false;
                for (fn_name) |c| {
                    if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '.') break :blk false;
                }
                break :blk true;
            };
            if (is_valid_fn) {
                const inner = trimmed[paren_pos + 1 .. trimmed.len - 1];
                const args = splitTopLevelArgs(inner) catch return;
                for (args.items[0..args.len]) |arg| {
                    const a = std.mem.trim(u8, arg, " \t\r\n");
                    if (a.len == 0) continue;
                    // Skip lambda arguments (e.g. "x -> expr") — lambda vars are not schema cols.
                    // Only skip if " -> " appears at the TOP LEVEL (not nested inside parens/brackets).
                    const is_top_level_lambda = blk2: {
                        var d: usize = 0;
                        var j: usize = 0;
                        while (j + 4 <= a.len) : (j += 1) {
                            if (a[j] == '(' or a[j] == '[') { d += 1; continue; }
                            if (a[j] == ')' or a[j] == ']') { if (d > 0) d -= 1; continue; }
                            if (d == 0 and std.mem.startsWith(u8, a[j..], " -> ")) break :blk2 true;
                        }
                        break :blk2 false;
                    };
                    if (is_top_level_lambda) continue;
                    try addFuncColumns(allocator, seen, needed, a, table, add);
                }
                return;
            }
        }
    }
    // Leaf node: attempt to add as a schema column name.
    // But first: if the expression contains spaces/operators (comparison, arithmetic, CASE WHEN, etc.),
    // scan all word tokens and try each as a schema column.
    if (std.mem.indexOfAny(u8, trimmed, " \t><=!+-*/()[]") != null) {
        // Tokenize by splitting on non-identifier characters and try each word
        var i: usize = 0;
        while (i < trimmed.len) {
            // skip non-identifier chars
            while (i < trimmed.len and !std.ascii.isAlphanumeric(trimmed[i]) and trimmed[i] != '_') i += 1;
            if (i >= trimmed.len) break;
            const start = i;
            while (i < trimmed.len and (std.ascii.isAlphanumeric(trimmed[i]) or trimmed[i] == '_')) i += 1;
            const token = trimmed[start..i];
            if (token.len == 0) continue;
            // Skip SQL keywords
            const sql_kws = [_][]const u8{ "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN" };
            var is_kw = false;
            for (sql_kws) |kw| {
                if (std.ascii.eqlIgnoreCase(token, kw)) { is_kw = true; break; }
            }
            if (is_kw) continue;
            // Skip string literals (tokens starting with ')  — already excluded by isAlphanumeric
            // Skip numeric literals
            if (std.fmt.parseInt(i64, token, 10) catch null) |_| continue;
            if (std.fmt.parseFloat(f64, token) catch null) |_| continue;
            // Try as schema column
            try add(allocator, seen, needed, token, table);
        }
        return;
    }
    try add(allocator, seen, needed, trimmed, table);
}

fn collectNeededColumns(
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    needed: *std.ArrayList([]const u8),
    table: *const schema.Table,
) !void {
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();

    const add = struct {
        fn f(alloc: std.mem.Allocator, s: *std.StringHashMap(void), lst: *std.ArrayList([]const u8), name: []const u8, tbl: *const schema.Table) !void {
            if (s.contains(name)) return;
            if (tbl.findColumn(name) == null) return; // skip unknown (computed) columns
            const owned = try alloc.dupe(u8, name);
            try s.put(owned, {});
            try lst.append(alloc, owned);
        }
    }.f;

    for (plan.projections) |proj| {
        if (proj.column) |col| {
            // Handle "length(<actual_col>)" — collect the inner column
            if (parseLengthCall(col)) |inner| {
                try add(allocator, &seen, needed, inner, table);
            } else if (parseMapSubscript(col)) |sub| {
                // data['key'] → need the Map column (e.g. "data")
                try add(allocator, &seen, needed, sub.col, table);
            } else if (std.mem.indexOfScalar(u8, col, '(') != null or
                       std.mem.indexOfAny(u8, col, " \t><=!") != null) {
                // Function expression or complex expression (CASE WHEN, comparisons, etc.):
                // recursively extract leaf column references
                try addFuncColumns(allocator, &seen, needed, col, table, &add);
            } else {
                try add(allocator, &seen, needed, col, table);
            }
        }
        // Also collect columns from CASE WHEN expressions
        if (proj.case_when_data) |cwd| {
            for (cwd.when_texts) |wt| {
                try addFuncColumns(allocator, &seen, needed, wt, table, &add);
            }
            for (cwd.then_texts) |tt| {
                try addFuncColumns(allocator, &seen, needed, tt, table, &add);
            }
            if (cwd.else_text) |et| {
                try addFuncColumns(allocator, &seen, needed, et, table, &add);
            }
        }
        // Also collect columns from cond expressions (countIf/uniqExactIf)
        if (proj.cond) |cond| {
            if (cond.cond_text) |ct| {
                // Parse column refs from the text condition so they get scanned.
                try addFuncColumns(allocator, &seen, needed, ct, table, &add);
            } else if (parseMapSubscript(cond.cond_col)) |sub| {
                try add(allocator, &seen, needed, sub.col, table);
            } else if (cond.cond_col.len > 0) {
                try add(allocator, &seen, needed, cond.cond_col, table);
            }
        }
    }
    // Parse group-by columns using parseGroupExprs (handles arithmetic + date_trunc)
    if (plan.group_by) |gb| {
        const exprs = try parseGroupExprs(allocator, gb);
        defer {
            for (exprs) |e| allocator.free(e.base_col);
            allocator.free(exprs);
        }
        for (exprs) |e| {
            // EventMinute/EventHour/EventDay are derived from EventTime
            const real_col = if (std.ascii.eqlIgnoreCase(e.base_col, "EventMinute") or
                std.ascii.eqlIgnoreCase(e.base_col, "EventHour") or
                std.ascii.eqlIgnoreCase(e.base_col, "EventDay")) "EventTime" else e.base_col;
            if (std.mem.indexOfScalar(u8, real_col, '(') != null or
                std.mem.indexOfAny(u8, real_col, " \t><=!") != null)
            {
                // Complex expression: extract leaf column references
                try addFuncColumns(allocator, &seen, needed, real_col, table, &add);
            } else {
                try add(allocator, &seen, needed, real_col, table);
            }
        }
    }
    // Also collect columns referenced by where_expr so filter evaluation works.
    if (plan.where_expr) |we| try collectWhereNodeColumns(allocator, &seen, needed, we, table);
    // Also collect columns from where_text (when where_expr was not parseable).
    if (plan.where_expr == null) {
        if (plan.where_text) |wt| {
            try addFuncColumns(allocator, &seen, needed, wt, table, &add);
        }
    }
    // Ensure at least one column is present so streamRows can count rows.
    if (needed.items.len == 0) {
        // Fall back to first column in schema if "CounterID" is not available.
        if (table.columns.len == 0) return; // empty/fake table: no columns to collect
        const fallback = if (table.findColumn("CounterID") != null) "CounterID" else table.columns[0].name;
        try add(allocator, &seen, needed, fallback, table);
    }
}

// ── Helper: parse HAVING predicate ───────────────────────────────────────────
//
// Supports: "COUNT(*) > N", "count_star() > N", "c > N" (alias), etc.
// Returns null if the HAVING text cannot be parsed.

const HavingPred = struct { proj_idx: usize, op: generic_sql.CmpOp, threshold: i64 };

fn parseHavingPred(projections: []const generic_sql.Expr, having_text: []const u8) ?HavingPred {
    const ops = [_]struct { text: []const u8, op: generic_sql.CmpOp }{
        .{ .text = ">=", .op = .ge },
        .{ .text = "<=", .op = .le },
        .{ .text = "<>", .op = .ne },
        .{ .text = ">",  .op = .gt },
        .{ .text = "<",  .op = .lt },
        .{ .text = "=",  .op = .eq },
    };
    for (ops) |candidate| {
        const pos = std.mem.indexOf(u8, having_text, candidate.text) orelse continue;
        const lhs = std.mem.trim(u8, having_text[0..pos], " \t\r\n");
        const rhs = std.mem.trim(u8, having_text[pos + candidate.text.len ..], " \t\r\n");
        const threshold = std.fmt.parseInt(i64, rhs, 10) catch continue;
        // Match lhs to a projection: count_star name variants, alias, or column
        for (projections, 0..) |p, i| {
            if (std.ascii.eqlIgnoreCase(lhs, "COUNT(*)") or
                std.ascii.eqlIgnoreCase(lhs, "count_star()"))
            {
                if (p.func == .count_star) return .{ .proj_idx = i, .op = candidate.op, .threshold = threshold };
            }
            if (p.alias) |a| if (std.ascii.eqlIgnoreCase(lhs, a)) return .{ .proj_idx = i, .op = candidate.op, .threshold = threshold };
        }
    }
    return null;
}

// ── Helper: parse group-by column list ───────────────────────────────────────

fn parseGroupCols(allocator: std.mem.Allocator, group_by: []const u8) ![][]const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |c| allocator.free(c);
        result.deinit(allocator);
    }
    // Split on commas at depth 0 (ignore commas inside parentheses)
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= group_by.len) : (i += 1) {
        const c = if (i < group_by.len) group_by[i] else ',';
        switch (c) {
            '(' => depth += 1,
            ')' => { if (depth > 0) depth -= 1; },
            ',' => if (depth == 0) {
                const part = std.mem.trim(u8, group_by[start..i], " \t\r\n");
                start = i + 1;
                if (part.len == 0) continue;
                // Skip numeric position references like "1", "2"
                if (std.fmt.parseInt(usize, part, 10) catch null) |_| continue;
                // Map date_trunc(unit, EventTime) → EventMinute/EventHour/EventDay
                if (dateTruncDerivedCol(part)) |derived| {
                    try result.append(allocator, try allocator.dupe(u8, derived));
                    continue;
                }
                // Handle arithmetic expressions like "ClientIP - 1": use the base column name
                const base = extractBaseColumnName(part);
                try result.append(allocator, try allocator.dupe(u8, base));
            },
            else => {},
        }
    }
    return result.toOwnedSlice(allocator);
}

/// Returns the derived column name if `part` is a date_trunc(unit, EventTime) expression,
/// otherwise null. Supported units: 'minute' → EventMinute, 'hour' → EventHour, 'day' → EventDay.
fn dateTruncDerivedCol(part: []const u8) ?[]const u8 {
    if (part.len < 7) return null;
    if (!std.ascii.startsWithIgnoreCase(part, "date_trunc")) return null;
    const open = std.mem.indexOfScalar(u8, part, '(') orelse return null;
    const close = std.mem.lastIndexOfScalar(u8, part, ')') orelse return null;
    if (close <= open) return null;
    const inner = std.mem.trim(u8, part[open + 1 .. close], " \t\r\n");
    const comma = std.mem.indexOfScalar(u8, inner, ',') orelse return null;
    const unit = std.mem.trim(u8, inner[0..comma], " \t\r\n");
    const source = std.mem.trim(u8, inner[comma + 1 ..], " \t\r\n");
    if (!std.ascii.eqlIgnoreCase(source, "EventTime")) return null;
    if (std.mem.eql(u8, unit, "'minute'")) return "EventMinute";
    if (std.mem.eql(u8, unit, "'hour'")) return "EventHour";
    if (std.mem.eql(u8, unit, "'day'")) return "EventDay";
    return null;
}

/// Returns true if `part` is a date_trunc('minute', EventTime) expression.
fn isDateTruncMinutePart(part: []const u8) bool {
    return std.mem.eql(u8, dateTruncDerivedCol(part) orelse return false, "EventMinute");
}

/// Extract the base column name from an expression like "ClientIP - 1" → "ClientIP".
/// If no operator is found, returns the full expression (assumed to be a plain identifier).
fn extractBaseColumnName(expr: []const u8) []const u8 {
    // Look for arithmetic operators: only minus for now (covers q36)
    for ([_]u8{ '-', '+' }) |op| {
        if (std.mem.indexOfScalar(u8, expr, op)) |pos| {
            const base = std.mem.trim(u8, expr[0..pos], " \t\r\n");
            if (base.len > 0) return base;
        }
    }
    return expr;
}

/// Parse a group-by expression string like "ClientIP - 1" into a GroupKeyExpr.
fn parseGroupKeyExpr(allocator: std.mem.Allocator, part: []const u8) !GroupKeyExpr {
    // date_trunc(unit, EventTime) → EventMinute/EventHour/EventDay with offset 0
    if (dateTruncDerivedCol(part)) |derived| {
        return .{ .base_col = try allocator.dupe(u8, derived), .offset = 0 };
    }
    // Look for subtraction
    if (std.mem.indexOfScalar(u8, part, '-')) |pos| {
        const base = std.mem.trim(u8, part[0..pos], " \t\r\n");
        const rest = std.mem.trim(u8, part[pos + 1 ..], " \t\r\n");
        if (base.len > 0) {
            if (std.fmt.parseInt(i64, rest, 10) catch null) |off| {
                return .{ .base_col = try allocator.dupe(u8, base), .offset = -off };
            }
        }
    }
    // Look for addition
    if (std.mem.indexOfScalar(u8, part, '+')) |pos| {
        const base = std.mem.trim(u8, part[0..pos], " \t\r\n");
        const rest = std.mem.trim(u8, part[pos + 1 ..], " \t\r\n");
        if (base.len > 0) {
            if (std.fmt.parseInt(i64, rest, 10) catch null) |off| {
                return .{ .base_col = try allocator.dupe(u8, base), .offset = off };
            }
        }
    }
    return .{ .base_col = try allocator.dupe(u8, part), .offset = 0 };
}

/// Parse group-by string into GroupKeyExpr slice (handles depth-0 comma splitting).
fn parseGroupExprs(allocator: std.mem.Allocator, group_by: []const u8) ![]GroupKeyExpr {
    var result: std.ArrayList(GroupKeyExpr) = .empty;
    errdefer {
        for (result.items) |e| allocator.free(e.base_col);
        result.deinit(allocator);
    }
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= group_by.len) : (i += 1) {
        const c = if (i < group_by.len) group_by[i] else ',';
        switch (c) {
            '(' => depth += 1,
            ')' => { if (depth > 0) depth -= 1; },
            ',' => if (depth == 0) {
                const part = std.mem.trim(u8, group_by[start..i], " \t\r\n");
                start = i + 1;
                if (part.len == 0) continue;
                // Skip numeric position references
                if (std.fmt.parseInt(usize, part, 10) catch null) |_| continue;
                try result.append(allocator, try parseGroupKeyExpr(allocator, part));
            },
            else => {},
        }
    }
    return result.toOwnedSlice(allocator);
}

// ── Helper: evaluate a projection expression against a row ────────────────────

fn evalProjectionExpr(proj: generic_sql.Expr, row: *const RowCtx) Value {
    switch (proj.func) {
        .column_ref => {
            const col = proj.column orelse return Value{ .null_val = {} };
            // Try direct column lookup (also handles data['key'] via RowCtx.get)
            const base_v: Value = if (row.get(col)) |v| v else blk: {
                // Fall back to text expression evaluator for complex expressions
                break :blk evalTextExpr(col, row) orelse Value{ .str = "" };
            };
            // Apply int_offset (from e.g. "col - 1" or "col + N" parsed as column_ref)
            if (proj.int_offset != 0) {
                const base_f = base_v.toF64() orelse return base_v;
                const res = base_f + @as(f64, @floatFromInt(proj.int_offset));
                if (res == @floor(res) and @abs(res) < 9.007199e15)
                    return Value{ .i64 = @intFromFloat(res) };
                return Value{ .f64 = res };
            }
            return base_v;
        },
        .int_literal => return Value{ .i64 = proj.int_offset },
        .float_literal => return Value{ .f64 = proj.float_val },
        .count_star, .count_if  => return Value{ .i64 = 1 }, // counted by AggState
        .count_distinct, .sum, .avg, .min, .max,
        .min_if, .max_if, .sum_array, .sum_array_if,
        .uniq_exact, .uniq_exact_if,
        .group_uniq_array, .any_val => {
            const col = proj.column orelse return Value{ .null_val = {} };
            // Handle "length(<actual_col>)" — compute string length instead of column value
            if (parseLengthCall(col)) |inner_col| {
                const v = row.get(inner_col) orelse return Value{ .null_val = {} };
                return switch (v) {
                    .str, .str_owned => Value{ .i64 = @intCast(v.toStr().?.len) },
                    else => Value{ .null_val = {} },
                };
            }
            // Try Map subscript / direct lookup
            if (row.get(col)) |v| return v;
            return evalTextExpr(col, row) orelse Value{ .null_val = {} };
        },
        .case_when => {
            // Evaluate CASE WHEN … THEN … ELSE … END using text evaluators.
            const cwd = proj.case_when_data orelse return Value{ .null_val = {} };
            for (cwd.when_texts, cwd.then_texts) |when_t, then_t| {
                if (evalTextBoolExpr(when_t, row)) {
                    return evalTextExpr(then_t, row) orelse Value{ .null_val = {} };
                }
            }
            if (cwd.else_text) |et| {
                return evalTextExpr(et, row) orelse Value{ .null_val = {} };
            }
            return Value{ .null_val = {} };
        },
        .cmp_expr => {
            // Comparison/boolean expression used as a value: returns uint8 0 or 1.
            const text = proj.column orelse return Value{ .uint8 = 0 };
            return Value{ .uint8 = if (evalTextBoolExpr(text, row)) 1 else 0 };
        },
    }
}

// ── comptime function dispatch table ─────────────────────────────────────────
//
// Each entry maps a CH function name (lowercase) to an EvalKind.
// evalTextExpr uses `inline for` over this table so all prefix strings and
// their lengths are computed at compile time — no hand-written byte offsets.
//
// Functions that DuckDB cannot parse (multiIf, toStartOf*, toString, …) are
// handled in ch_compat.zig before reaching this layer.
// Functions below are CH originals that DuckDB parses fine as unknown FUNCTION
// nodes; the executor is responsible for evaluating them at runtime.

const EvalKind = enum {
    // Conditional
    cond_if,           // if(cond, then, else)
    // String → scalar
    str_lower,         // lower(s), lowerUTF8 already rewritten to lower
    str_upper,         // upper(s)
    str_concat,        // concat(a, b, ...)
    str_substring,     // substring(s, pos [, len]) — 1-based
    str_starts_with,   // startsWith(s, prefix)
    str_position_ci,   // positionCaseInsensitive(haystack, needle)
    str_position,      // position(hay, ndl) / strpos(hay, ndl)
    str_length,        // length(s or array) — byte len or element count
    not_empty,         // notEmpty(x) → 1 if non-empty array/string, else 0
    is_empty,          // empty(x) → 1 if empty array/string, else 0
    reinterpret_as_str, // reinterpretAsString(n) → raw little-endian bytes as string
    // Numeric
    num_floor,         // floor(x)
    num_round,         // round(x)
    num_abs,           // abs(x)
    num_greatest,      // greatest(a, b, ...)
    num_least,         // least(a, b, ...)
    // Date
    date_yyyymmdd,     // toYYYYMMDD(x) → integer
    date_trunc,        // date_trunc('unit', col) → truncated timestamp string
    date_year,         // toYear(x) / year(x) → integer year
    date_month,        // toMonth(x) / month(x) → integer month (1-12)
    date_day,          // toDayOfMonth(x) / day(x) → integer day (1-31)
    date_hour,         // toHour(x) → integer hour (0-23)
    date_minute,       // toMinute(x) → integer minute (0-59)
    date_second,       // toSecond(x) → integer second (0-59)
    // IP
    ip_bool,           // isIPv4String / isIPv6String → 0 or 1
    ip_to_num,         // IPv4StringToNumOrDefault(s) → uint32
    // Cast
    cast_expr,         // CAST(expr AS type)
    str_tostring,      // toString(x) → string representation
    fn_to_datetime,    // toDateTime(x) → unix seconds i64
    fn_array_element,  // ch_array_element(arr, idx) → arr[idx] 1-based, 0/empty on OOB
    fn_now,            // now() → current Unix timestamp as DateTime (seconds)
    fn_to_days,        // to_days(n) → n * 86400 (DuckDB's INTERVAL n DAY translation)
    fn_today,          // today() → current date as Date
    fn_yesterday,      // yesterday() → yesterday as Date
    empty_arr_str,     // emptyArrayString() → []
    empty_arr_int,     // emptyArrayUInt8/16/32/64() → []
    // Array construction
    arr_make,          // list_value(a, b, ...) / array(a, b, ...) → Value.array
    arr_split_char,    // splitByChar(delim, str) → array
    arr_split_str,     // splitByString(delim, str) → array
    // Array predicates
    arr_has,           // has(arr, val)
    arr_has_any,       // hasAny(arr, [v1,v2,...])
    arr_has_all,       // hasAll(arr, [v1,v2,...])
    arr_index_of,      // indexOf(arr, val) → 1-based
    // Array transforms
    arr_filter,        // arrayFilter(x -> cond, arr)
    arr_map,           // arrayMap(x -> expr, arr)
    arr_exists,        // arrayExists(x -> cond, arr)
    arr_flatten,       // arrayFlatten(arr)
    arr_distinct,      // arrayDistinct(arr)
    arr_sum,           // arraySum(arr)
    arr_concat,        // arrayConcat(a, b)
    arr_max,           // arrayMax(arr)
    arr_min,           // arrayMin(arr)
    arr_slice,         // arraySlice(arr, off, len)
    arr_str_join,      // arrayStringConcat(arr [, sep])
    arr_enumerate,     // arrayEnumerate(arr) → [1,2,...,len]
    arr_enumerate_uniq, // arrayEnumerateUniq(arr) → [1,1,...] within equal-value runs
    // Map
    map_keys,          // mapKeys(m) → array of keys
    map_values,        // mapValues(m) → array of values
    // Dict stubs
     stub_zero,         // integer functions that stub to 0 (e.g. IPv6StringToNumOrDefault)
     stub_bool_zero,    // bool functions that stub to UInt8 0 (e.g. dictHas)
     stub_float_zero,   // float functions that stub to 0.0 (e.g. risk_score)
     stub_empty_str,    // dictGet → ""
    stub_null,         // dictGetOrNull → null
    stub_default_arg4, // dictGetOrDefault → 4th arg
    // IP conversion
    ipv6_num_to_str,   // IPv6NumToString(bytes) → "x.x.x.x" or "::ffff:..." string
    // Scalar passthrough (max in non-aggregate context)
    scalar_passthru,   // max(expr) → evalTextExpr(inner)
    // Type introspection
    type_name,         // toTypeName(x) → CH type name string
    // Integer division
    int_div_or_zero,   // intDivOrZero(a, b) → trunc(a/b) or 0 if b=0
    // String helpers
    append_trailing_char, // appendTrailingCharIfAbsent(s, c) → s with c appended if not already last
    // FixedString
    fixed_string,         // toFixedString(s, n) → s padded/truncated to n bytes (null-pad)
    // Math functions (f64 → f64)
    math_sqrt,   // sqrt(x)
    math_trunc,
    math_cbrt,   // cbrt(x)
    math_exp,    // exp(x)
    math_exp2,   // exp2(x)
    math_exp10,  // exp10(x)
    math_log,    // log(x) / ln(x)
    math_log2,   // log2(x)
    math_log10,  // log10(x)
    math_sin,    // sin(x)
    math_cos,    // cos(x)
    math_tan,    // tan(x)
    math_asin,   // asin(x)
    math_acos,   // acos(x)
    math_atan,   // atan(x)
    math_atan2,  // atan2(y, x)
    math_pow,    // pow(x, y) / power(x, y)
    math_lgamma, // lgamma(x)
    math_tgamma, // tgamma(x)
    math_erf,    // erf(x)
    math_erfc,   // erfc(x)
    math_is_nan, // isNaN(x)
    math_is_inf, // isInfinite(x) / isInf(x)
    math_is_finite, // isFinite(x)
    math_pi,        // pi()
    math_e,         // e()
    // Hyperbolic
    math_sinh,  math_cosh,  math_tanh,
    math_asinh, math_acosh, math_atanh,
    // Other math
    math_log1p, // log1p(x)
    math_hypot, // hypot(x,y)
    // Range/sequence function
    fn_range,   // range(n) or range(start, end) → array [0..n-1]
    // JSON extraction
    json_extract_str,  // simpleJSONExtractString(json, key) / JSONExtractString(json, key)
    json_extract_int,  // simpleJSONExtractInt / JSONExtractInt
    json_extract_float, // simpleJSONExtractFloat / JSONExtractFloat
    json_extract_raw,  // simpleJSONExtractRaw / JSONExtractRaw
    json_extract_bool, // simpleJSONExtractBool / JSONExtractBool
    // Regex
    fn_match,          // match(s, pattern) → 1 if matched, 0 if not
    fn_extract,        // extract(s, pattern) → first capture group or full match
    fn_replace_regexp, // replaceRegexpAll(s, pattern, replacement) → s with all matches replaced
    fn_replace_one,    // replaceOne(s, from, to) → replace first occurrence
    fn_replace_all,    // replaceAll(s, from, to) → replace all occurrences
    // Date formatting
    fn_format_datetime, // formatDateTime(ts, fmt [, tz]) → formatted string
    // String
    fn_trim,           // trim(s) / trimLeft(s) / trimRight(s) / trimBoth(s)
    fn_trim_left,
    fn_trim_right,
    fn_to_string_base,  // toStringCutToZero → just toString
    // Type casts
    fn_to_int32,   // toInt32(x) / toInt32OrZero(x)
    fn_to_uint32,  // toUInt32(x)
    fn_to_int64,   // toInt64(x)
    fn_to_uint64,  // toUInt64(x)
    fn_to_float32, // toFloat32(x)
    fn_to_float64, // toFloat64(x)
    // Misc
    fn_if_null,    // ifNull(x, default) → x if not null else default
    fn_coalesce,   // coalesce(a, b, ...) → first non-null
    fn_to_nullable, // toNullable(x) → x (passthrough)
    fn_nullable_or_default, // nullableOrDefault(x) → x or 0/""
    fn_ends_with,  // endsWith(s, suffix)
};

const FuncEval = struct {
    name: []const u8,  // lowercase CH function name (no trailing '(')
    kind: EvalKind,
};

const func_evals = [_]FuncEval{
    .{ .name = "if",                          .kind = .cond_if          },
    .{ .name = "lower",                       .kind = .str_lower        },
    .{ .name = "upper",                       .kind = .str_upper        },
    .{ .name = "concat",                      .kind = .str_concat       },
    .{ .name = "substring",                   .kind = .str_substring    },
    .{ .name = "substr",                      .kind = .str_substring    },
    .{ .name = "startswith",                  .kind = .str_starts_with  },
    .{ .name = "positioncaseinsensitive",      .kind = .str_position_ci  },
    .{ .name = "position",                     .kind = .str_position     },
    .{ .name = "strpos",                       .kind = .str_position     },
    .{ .name = "locate",                       .kind = .str_position     },
    .{ .name = "length",                      .kind = .str_length       },
    .{ .name = "notEmpty",                    .kind = .not_empty        },
    .{ .name = "notempty",                    .kind = .not_empty        },
    .{ .name = "empty",                       .kind = .is_empty         },
    .{ .name = "reinterpretAsString",         .kind = .reinterpret_as_str },
    .{ .name = "reinterpretasstring",         .kind = .reinterpret_as_str },
    .{ .name = "floor",                       .kind = .num_floor        },
    .{ .name = "round",                       .kind = .num_round        },
    .{ .name = "abs",                         .kind = .num_abs          },
    .{ .name = "greatest",                    .kind = .num_greatest     },
    .{ .name = "least",                       .kind = .num_least        },
    .{ .name = "toyyyymmdd",                  .kind = .date_yyyymmdd    },
    .{ .name = "date_trunc",                  .kind = .date_trunc       },
    .{ .name = "toyear",                      .kind = .date_year        },
    .{ .name = "year",                        .kind = .date_year        },
    .{ .name = "tomonth",                     .kind = .date_month       },
    .{ .name = "month",                       .kind = .date_month       },
    .{ .name = "todayofmonth",                .kind = .date_day         },
    .{ .name = "day",                         .kind = .date_day         },
    .{ .name = "tohour",                      .kind = .date_hour        },
    .{ .name = "hour",                        .kind = .date_hour        },
    .{ .name = "tominute",                    .kind = .date_minute      },
    .{ .name = "minute",                      .kind = .date_minute      },
    .{ .name = "tosecond",                    .kind = .date_second      },
    .{ .name = "second",                      .kind = .date_second      },
    .{ .name = "isipv4string",                .kind = .ip_bool          },
    .{ .name = "isipv6string",                .kind = .ip_bool          },
    .{ .name = "ipv4stringtonumordefault",     .kind = .ip_to_num        },
     .{ .name = "ipv6stringtonumordefault",     .kind = .stub_zero        },
     .{ .name = "risk_score",                  .kind = .stub_float_zero  },
    .{ .name = "cast",                        .kind = .cast_expr        },
    .{ .name = "tostring",                    .kind = .str_tostring     },
    .{ .name = "ch_tostring",                 .kind = .str_tostring     },
    .{ .name = "todatetime",                  .kind = .fn_to_datetime   },
    .{ .name = "todatetime64",                .kind = .fn_to_datetime   },
    .{ .name = "tounixtimestamp",             .kind = .fn_to_datetime   },
    .{ .name = "ch_array_element",            .kind = .fn_array_element },
    .{ .name = "now",                         .kind = .fn_now           },
    .{ .name = "to_days",                     .kind = .fn_to_days       },
    .{ .name = "today",                       .kind = .fn_today         },
    .{ .name = "yesterday",                   .kind = .fn_yesterday     },
    .{ .name = "emptyarraystring",            .kind = .empty_arr_str    },
    .{ .name = "emptyarrayuint8",             .kind = .empty_arr_int    },
    .{ .name = "emptyarrayuint16",            .kind = .empty_arr_int    },
    .{ .name = "emptyarrayuint32",            .kind = .empty_arr_int    },
    .{ .name = "emptyarrayuint64",            .kind = .empty_arr_int    },
    .{ .name = "emptyarrayint8",              .kind = .empty_arr_int    },
    .{ .name = "emptyarrayint16",             .kind = .empty_arr_int    },
    .{ .name = "emptyarrayint32",             .kind = .empty_arr_int    },
    .{ .name = "emptyarrayint64",             .kind = .empty_arr_int    },
    .{ .name = "emptyarrayfloat32",           .kind = .empty_arr_int    },
    .{ .name = "emptyarrayfloat64",           .kind = .empty_arr_int    },
    .{ .name = "list_value",                  .kind = .arr_make         },
    .{ .name = "array",                       .kind = .arr_make         },
    .{ .name = "splitbychar",                 .kind = .arr_split_char   },
    .{ .name = "splitbystring",               .kind = .arr_split_str    },
    .{ .name = "has",                         .kind = .arr_has          },
    .{ .name = "hasany",                      .kind = .arr_has_any      },
    .{ .name = "hasall",                      .kind = .arr_has_all      },
    .{ .name = "indexof",                     .kind = .arr_index_of     },
    .{ .name = "arrayfilter",                 .kind = .arr_filter       },
    .{ .name = "arraymap",                    .kind = .arr_map          },
    .{ .name = "arrayexists",                 .kind = .arr_exists       },
    .{ .name = "arrayflatten",                .kind = .arr_flatten      },
    .{ .name = "arraydistinct",               .kind = .arr_distinct     },
    .{ .name = "arraysum",                    .kind = .arr_sum          },
    .{ .name = "arrayconcat",                 .kind = .arr_concat       },
    .{ .name = "arraymax",                    .kind = .arr_max          },
    .{ .name = "arraymin",                    .kind = .arr_min          },
    .{ .name = "arrayslice",                  .kind = .arr_slice        },
     .{ .name = "arraystringconcat",           .kind = .arr_str_join     },
     .{ .name = "array_to_string",            .kind = .arr_str_join     },
    .{ .name = "arrayenumerate",              .kind = .arr_enumerate    },
    .{ .name = "arrayenumerateuniq",          .kind = .arr_enumerate_uniq },
    .{ .name = "mapkeys",                     .kind = .map_keys         },
    .{ .name = "map_keys",                    .kind = .map_keys         },
    .{ .name = "mapvalues",                   .kind = .map_values       },
    .{ .name = "map_values",                  .kind = .map_values       },
    .{ .name = "dicthas",                     .kind = .stub_bool_zero   },
    .{ .name = "dictget",                     .kind = .stub_empty_str   },
    .{ .name = "dictgetornull",               .kind = .stub_null        },
    .{ .name = "dictgetordefault",            .kind = .stub_default_arg4},
    .{ .name = "ipv6numtostring",             .kind = .ipv6_num_to_str  },
    .{ .name = "max",                         .kind = .scalar_passthru  },
    .{ .name = "totypename",                  .kind = .type_name        },
    .{ .name = "intdivorzero",                .kind = .int_div_or_zero  },
    .{ .name = "appendtrailingcharifabsent",  .kind = .append_trailing_char },
    .{ .name = "tofixedstring",               .kind = .fixed_string         },
    // Math functions
    .{ .name = "sqrt",    .kind = .math_sqrt  },
    .{ .name = "trunc",   .kind = .math_trunc },
    .{ .name = "cbrt",    .kind = .math_cbrt  },
    .{ .name = "exp",     .kind = .math_exp   },
    .{ .name = "exp2",    .kind = .math_exp2  },
    .{ .name = "exp10",   .kind = .math_exp10 },
    .{ .name = "log",     .kind = .math_log   },
    .{ .name = "ln",      .kind = .math_log   },
    .{ .name = "log2",    .kind = .math_log2  },
    .{ .name = "log10",   .kind = .math_log10 },
    .{ .name = "sin",     .kind = .math_sin   },
    .{ .name = "cos",     .kind = .math_cos   },
    .{ .name = "tan",     .kind = .math_tan   },
    .{ .name = "asin",    .kind = .math_asin  },
    .{ .name = "acos",    .kind = .math_acos  },
    .{ .name = "atan",    .kind = .math_atan  },
    .{ .name = "atan2",   .kind = .math_atan2 },
    .{ .name = "pow",     .kind = .math_pow   },
    .{ .name = "power",   .kind = .math_pow   },
    .{ .name = "lgamma",  .kind = .math_lgamma },
    .{ .name = "tgamma",  .kind = .math_tgamma },
    .{ .name = "erf",     .kind = .math_erf   },
    .{ .name = "erfc",    .kind = .math_erfc  },
    .{ .name = "isnan",   .kind = .math_is_nan },
    .{ .name = "isinfinite", .kind = .math_is_inf },
    .{ .name = "isinf",   .kind = .math_is_inf },
    .{ .name = "isfinite", .kind = .math_is_finite },
    .{ .name = "pi",       .kind = .math_pi      },
    .{ .name = "e",        .kind = .math_e       },
    .{ .name = "sinh",     .kind = .math_sinh    },
    .{ .name = "cosh",     .kind = .math_cosh    },
    .{ .name = "tanh",     .kind = .math_tanh    },
    .{ .name = "asinh",    .kind = .math_asinh   },
    .{ .name = "acosh",    .kind = .math_acosh   },
    .{ .name = "atanh",    .kind = .math_atanh   },
    .{ .name = "log1p",    .kind = .math_log1p   },
    .{ .name = "hypot",    .kind = .math_hypot   },
    .{ .name = "range",    .kind = .fn_range     },
    .{ .name = "generate_series", .kind = .fn_range },
    // JSON extraction
    .{ .name = "simplejsonextractstring",  .kind = .json_extract_str   },
    .{ .name = "jsonextractstring",        .kind = .json_extract_str   },
    .{ .name = "simplejsonextractint",     .kind = .json_extract_int   },
    .{ .name = "jsonextractint",           .kind = .json_extract_int   },
    .{ .name = "simplejsonextractfloat",   .kind = .json_extract_float },
    .{ .name = "jsonextractfloat",         .kind = .json_extract_float },
    .{ .name = "simplejsonextractraw",     .kind = .json_extract_raw   },
    .{ .name = "jsonextractraw",           .kind = .json_extract_raw   },
    .{ .name = "simplejsonextractbool",    .kind = .json_extract_bool  },
    .{ .name = "jsonextractbool",          .kind = .json_extract_bool  },
    // Regex
    .{ .name = "match",                    .kind = .fn_match           },
    .{ .name = "extract",                  .kind = .fn_extract         },
    .{ .name = "replaceregexpall",         .kind = .fn_replace_regexp  },
    .{ .name = "replaceregexpone",         .kind = .fn_replace_regexp  },
    .{ .name = "replaceone",               .kind = .fn_replace_one     },
    .{ .name = "replaceall",               .kind = .fn_replace_all     },
    // Date formatting
    .{ .name = "formatdatetime",           .kind = .fn_format_datetime },
    .{ .name = "dateformat",               .kind = .fn_format_datetime },
    // String trim
    .{ .name = "trim",                     .kind = .fn_trim            },
    .{ .name = "trimboth",                 .kind = .fn_trim            },
    .{ .name = "trimleft",                 .kind = .fn_trim_left       },
    .{ .name = "ltrim",                    .kind = .fn_trim_left       },
    .{ .name = "trimright",                .kind = .fn_trim_right      },
    .{ .name = "rtrim",                    .kind = .fn_trim_right      },
    .{ .name = "tostringcuttozero",        .kind = .str_tostring       },
    // Type casts
    .{ .name = "toint8",                   .kind = .fn_to_int32        },
    .{ .name = "toint16",                  .kind = .fn_to_int32        },
    .{ .name = "toint32",                  .kind = .fn_to_int32        },
    .{ .name = "toint8orzero",             .kind = .fn_to_int32        },
    .{ .name = "toint16orzero",            .kind = .fn_to_int32        },
    .{ .name = "toint32orzero",            .kind = .fn_to_int32        },
    .{ .name = "toint64",                  .kind = .fn_to_int64        },
    .{ .name = "toint64orzero",            .kind = .fn_to_int64        },
    .{ .name = "touint8",                  .kind = .fn_to_uint32       },
    .{ .name = "touint16",                 .kind = .fn_to_uint32       },
    .{ .name = "touint32",                 .kind = .fn_to_uint32       },
    .{ .name = "touint8orzero",            .kind = .fn_to_uint32       },
    .{ .name = "touint16orzero",           .kind = .fn_to_uint32       },
    .{ .name = "touint32orzero",           .kind = .fn_to_uint32       },
    .{ .name = "touint64",                 .kind = .fn_to_uint64       },
    .{ .name = "touint64orzero",           .kind = .fn_to_uint64       },
    .{ .name = "tofloat32",                .kind = .fn_to_float32      },
    .{ .name = "tofloat32orzero",          .kind = .fn_to_float32      },
    .{ .name = "tofloat64",                .kind = .fn_to_float64      },
    .{ .name = "tofloat64orzero",          .kind = .fn_to_float64      },
    .{ .name = "todouble",                 .kind = .fn_to_float64      },
    // Null handling
    .{ .name = "ifnull",                   .kind = .fn_if_null         },
    .{ .name = "coalesce",                 .kind = .fn_coalesce        },
    .{ .name = "tonullable",               .kind = .fn_to_nullable     },
    .{ .name = "assumenotnull",            .kind = .fn_to_nullable     },
    // endsWith
    .{ .name = "endswith",                 .kind = .fn_ends_with       },
};

// ── Helper: resolve a Value from an array stored as \f-separated string ───────

/// Parse a \x0c-separated string into a []Value (page_allocator-owned).
/// Handles both "a\x0cb" and "\x0ca\x0cb" (leading \x0c = element prefix) formats.
fn parseArrayValue(s: []const u8) ?[]Value {
    // Handle \x0c-separated format (from sentinel serialization: \x01 elem \x0c elem ...)
    if (std.mem.indexOfScalar(u8, s, '\x0c') != null) {
        // strip optional leading \x01 sentinel byte
        const data = if (s.len > 0 and s[0] == '\x01') s[1..] else s;
        var list: std.ArrayListUnmanaged(Value) = .empty;
        var it = std.mem.splitScalar(u8, data, '\x0c');
        while (it.next()) |elem| {
            if (elem.len == 0 and list.items.len == 0) continue;
            list.append(std.heap.page_allocator, Value{ .str = elem }) catch return null;
        }
        return list.toOwnedSlice(std.heap.page_allocator) catch null;
    }
    // Handle \x01-prefixed single-element array blob (no \x0c separator).
    // A bare \x01 sentinel prefix with no \x0c means a 1-element array.
    if (s.len > 0 and s[0] == '\x01') {
        const elem = s[1..];
        const out = std.heap.page_allocator.alloc(Value, 1) catch return null;
        out[0] = Value{ .str = elem };
        return out;
    }
    // Handle ClickHouse text format: [elem1,elem2,...] or ['str1','str2']
    const trimmed = std.mem.trim(u8, s, " \t");
    if (trimmed.len >= 2 and trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']') {
        const content = trimmed[1 .. trimmed.len - 1];
        if (content.len == 0) return &.{}; // empty array
        var list: std.ArrayListUnmanaged(Value) = .empty;
        var p: usize = 0;
        while (p < content.len) {
            // skip whitespace and commas
            while (p < content.len and (content[p] == ' ' or content[p] == ',' or content[p] == '\t')) p += 1;
            if (p >= content.len) break;
            if (content[p] == '\'') {
                // quoted string
                p += 1;
                const start = p;
                while (p < content.len and content[p] != '\'') p += 1;
                const elem = content[start..p];
                if (p < content.len) p += 1; // skip closing '
                list.append(std.heap.page_allocator, Value{ .str = elem }) catch return null;
            } else {
                // number or bare token
                const start = p;
                while (p < content.len and content[p] != ',' and content[p] != ']' and content[p] != ' ') p += 1;
                const elem_str = content[start..p];
                if (elem_str.len == 0) continue;
                if (std.fmt.parseInt(i64, elem_str, 10)) |iv| {
                    list.append(std.heap.page_allocator, Value{ .i64 = iv }) catch return null;
                } else |_| {
                    if (std.fmt.parseFloat(f64, elem_str)) |fv| {
                        list.append(std.heap.page_allocator, Value{ .f64 = fv }) catch return null;
                    } else |_| {
                        list.append(std.heap.page_allocator, Value{ .str = elem_str }) catch return null;
                    }
                }
            }
        }
        return list.toOwnedSlice(std.heap.page_allocator) catch null;
    }
    return null;
}

/// Get the array elements from a Value (either .array or parse from .str/.str_owned).
fn valueToArray(v: Value) ?[]Value {
    return switch (v) {
        .array     => |a| a,
        .str, .str_owned => parseArrayValue(v.toStr().?),
        else => null,
    };
}

/// Evaluate lambda body: bind `param_name` → `elem` in a pseudo-row, eval `body`.
/// Lambda text form is: "param -> body_expr" (produced by exprToText).
fn evalLambdaBody(lambda_text: []const u8, elem: Value, row: *const RowCtx) ?Value {
    const arrow = std.mem.indexOf(u8, lambda_text, " -> ") orelse return null;
    const body = std.mem.trim(u8, lambda_text[arrow + 4 ..], " \t\r\n");
    const param = std.mem.trim(u8, lambda_text[0..arrow], " \t\r\n");
    // Build a temporary row that adds param → elem on top of existing row
    var extra_names = [_][]const u8{param};
    var extra_vals  = [_]Value{elem};
    const inner_row = RowCtx{
        .names  = &extra_names,
        .values = &extra_vals,
        .parent = row,
    };
    return evalTextExpr(body, &inner_row);
}

fn evalLambdaBool(lambda_text: []const u8, elem: Value, row: *const RowCtx) bool {
    const arrow = std.mem.indexOf(u8, lambda_text, " -> ") orelse return false;
    const body = std.mem.trim(u8, lambda_text[arrow + 4 ..], " \t\r\n");
    const param = std.mem.trim(u8, lambda_text[0..arrow], " \t\r\n");
    var extra_names = [_][]const u8{param};
    var extra_vals  = [_]Value{elem};
    const inner_row = RowCtx{
        .names  = &extra_names,
        .values = &extra_vals,
        .parent = row,
    };
    // If body is a comparison/boolean expression, use bool evaluator.
    // Detect by presence of comparison operators or logical keywords.
    const has_cmp = std.mem.indexOf(u8, body, " != ") != null or
                    std.mem.indexOf(u8, body, " <> ") != null or
                    std.mem.indexOf(u8, body, " = ")  != null or
                    std.mem.indexOf(u8, body, " < ")  != null or
                    std.mem.indexOf(u8, body, " > ")  != null or
                    std.mem.indexOf(u8, body, " >= ") != null or
                    std.mem.indexOf(u8, body, " <= ") != null;
    if (has_cmp) return evalTextBoolExpr(body, &inner_row);
    const v = evalTextExpr(body, &inner_row) orelse return false;
    return switch (v) {
        .i64   => |i| i != 0,
        .f64   => |f| f != 0,
        .date  => |d| d != 0,
        .uint8 => |u| u != 0,
        .str, .str_owned => v.toStr().?.len > 0,
        .array => |a| a.len > 0,
        .null_val => false,
    };
}

// ── IPv4 numeric conversion ───────────────────────────────────────────────────

fn ipv4ToNum(s: []const u8) i64 {
    var parts: [4]u8 = .{0, 0, 0, 0};
    var pi: u8 = 0;
    var acc: u16 = 0;
    var has_digits = false;
    for (s) |c| {
        if (c == '.') {
            if (pi >= 3 or !has_digits) return 0;
            parts[pi] = @intCast(acc);
            pi += 1;
            acc = 0;
            has_digits = false;
        } else if (c >= '0' and c <= '9') {
            acc = acc * 10 + (c - '0');
            if (acc > 255) return 0;
            has_digits = true;
        } else return 0;
    }
    if (pi != 3 or !has_digits) return 0;
    parts[3] = @intCast(acc);
    return (@as(i64, parts[0]) << 24) | (@as(i64, parts[1]) << 16) |
           (@as(i64, parts[2]) << 8)  |  @as(i64, parts[3]);
}

// ── evalTextExpr ─────────────────────────────────────────────────────────────

/// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" to Unix seconds. Returns null on failure.
fn datetimeStrToSecs(s: []const u8) ?f64 {
    if (s.len >= 19 and s[4] == '-' and s[7] == '-' and s[10] == ' ' and s[13] == ':' and s[16] == ':') {
        const y  = std.fmt.parseInt(i32, s[0..4],   10) catch return null;
        const mo = std.fmt.parseInt(u8,  s[5..7],   10) catch return null;
        const dy = std.fmt.parseInt(u8,  s[8..10],  10) catch return null;
        const hh = std.fmt.parseInt(u8,  s[11..13], 10) catch return null;
        const mm = std.fmt.parseInt(u8,  s[14..16], 10) catch return null;
        const ss = std.fmt.parseInt(u8,  s[17..19], 10) catch return null;
        const days = dateToDays(y, mo, dy);
        return @as(f64, @floatFromInt(days)) * 86400.0 + @as(f64, @floatFromInt(hh)) * 3600.0 + @as(f64, @floatFromInt(mm)) * 60.0 + @as(f64, @floatFromInt(ss));
    }
    if (s.len >= 10 and s[4] == '-' and s[7] == '-') {
        const y  = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
        const mo = std.fmt.parseInt(u8,  s[5..7], 10) catch return null;
        const dy = std.fmt.parseInt(u8,  s[8..10], 10) catch return null;
        return @as(f64, @floatFromInt(dateToDays(y, mo, dy))) * 86400.0;
    }
    return null;
}

/// Infer the ClickHouse type name string for an expression, using AST-level
/// knowledge before falling back to value evaluation.
fn inferChTypeName(expr: []const u8, row: *const RowCtx) []const u8 {
    const t = std.mem.trim(u8, expr, " \t\r\n");
    // Strip outer parens
    if (t.len >= 2 and t[0] == '(' and t[t.len - 1] == ')') {
        var depth: i32 = 0;
        var balanced = true;
        for (t[0 .. t.len - 1], 0..) |c, i| {
            if (c == '(') depth += 1 else if (c == ')') depth -= 1;
            if (i > 0 and depth == 0) { balanced = false; break; }
        }
        if (balanced) return inferChTypeName(t[1 .. t.len - 1], row);
    }
    // Detect binary arithmetic: look for top-level +/-
    if (findTopLevelOp(t, "+-")) |op_pos| {
        const op = t[op_pos];
        const lhs = std.mem.trim(u8, t[0..op_pos], " \t\r\n");
        const rhs = std.mem.trim(u8, t[op_pos + 1 ..], " \t\r\n");
        const lt = inferChTypeName(lhs, row);
        const rt = inferChTypeName(rhs, row);
        // DateTime ± Int → DateTime; DateTime - DateTime → Int32
        if (std.mem.startsWith(u8, lt, "DateTime") and std.mem.startsWith(u8, rt, "DateTime"))
            return "Int32";
        if (std.mem.startsWith(u8, lt, "DateTime") or std.mem.startsWith(u8, rt, "DateTime"))
            return "DateTime";
        // Date ± Int → Date; Date - Date → Int32
        if (std.mem.eql(u8, lt, "Date") and std.mem.eql(u8, rt, "Date"))
            return "Int32";
        if (std.mem.eql(u8, lt, "Date") or std.mem.eql(u8, rt, "Date"))
            return "Date";
        _ = op;
        return "Int64";
    }
    // Detect function call: name(...)
    if (std.mem.indexOfScalar(u8, t, '(')) |paren| {
        if (paren > 0 and t[t.len - 1] == ')') {
            const fname_raw = t[0..paren];
            var fname_buf: [64]u8 = undefined;
            const fname = std.ascii.lowerString(fname_buf[0..@min(fname_buf.len, fname_raw.len)], fname_raw);
            if (std.mem.eql(u8, fname, "now"))        return "DateTime";
            if (std.mem.eql(u8, fname, "today"))      return "Date";
            if (std.mem.eql(u8, fname, "yesterday"))  return "Date";
            if (std.mem.eql(u8, fname, "todatetime")) return "DateTime";
            if (std.mem.eql(u8, fname, "todate"))     return "Date";
            if (std.mem.startsWith(u8, fname, "todatetime")) return "DateTime";
            if (std.mem.startsWith(u8, fname, "todate")) return "Date";
            if (std.mem.eql(u8, fname, "totypename")) return "String";
            if (std.mem.eql(u8, fname, "typename"))   return "String";
        }
    }
    // Float literal
    if (std.fmt.parseFloat(f64, t) catch null) |_| {
        if (std.mem.indexOfScalar(u8, t, '.') != null) return "Float64";
    }
    // Fall back to evaluated value
    const v = evalTextExpr(t, row) orelse {
        // If the expression looks like a plain identifier (no parens/operators/quotes)
        // it could be a same-SELECT alias not yet in the row. Default to Int64.
        const is_plain_ident = for (t) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '_') break false;
        } else true;
        if (is_plain_ident and t.len > 0) return "Int64";
        return "Null";
    };
    return switch (v) {
        .i64      => "Int64",
        .uint8    => "UInt8",
        .f64      => "Float64",
        .date     => "Date",
        .str, .str_owned => "String",
        .array    => "Array(String)",
        .null_val => "Null",
    };
}

/// Find the position of a top-level (depth=0) operator char in ops string.
/// Returns null if not found at top level.
fn findTopLevelOp(expr: []const u8, ops: []const u8) ?usize {
    var depth: i32 = 0;
    var i: usize = expr.len;
    while (i > 0) {
        i -= 1;
        const c = expr[i];
        if (c == ')') { depth += 1; continue; }
        if (c == '(') { depth -= 1; continue; }
        if (depth == 0 and i > 0) {
            for (ops) |op| {
                if (c == op) return i;
            }
        }
    }
    return null;
}

/// Evaluate a text-encoded expression against a row.
/// Returns null if the expression cannot be evaluated (unknown column or function).
fn evalTextExpr(expr: []const u8, row: *const RowCtx) ?Value {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");
    if (trimmed.len == 0) return Value{ .str = "" };

    // ── literals ────────────────────────────────────────────────────
    if (trimmed.len >= 2 and trimmed[0] == '\'' and trimmed[trimmed.len - 1] == '\'')
        return Value{ .str = trimmed[1 .. trimmed.len - 1] };
    if (std.fmt.parseInt(i64, trimmed, 10) catch null) |iv|
        return Value{ .i64 = iv };
    if (std.fmt.parseFloat(f64, trimmed) catch null) |fv|
        return Value{ .f64 = fv };
    // ── array literal: ['a', 'b', ...] or [1, 2, ...] ──────────────
    if (trimmed.len >= 2 and trimmed[0] == '[' and trimmed[trimmed.len - 1] == ']') {
        const contents = std.mem.trim(u8, trimmed[1 .. trimmed.len - 1], " \t\r\n");
        if (contents.len == 0) return Value{ .array = &.{} };  // empty array []
        const split_res = splitTopLevelArgs(contents) catch return Value{ .array = &.{} };
        var list: std.ArrayListUnmanaged(Value) = .empty;
        for (split_res.items[0..split_res.len]) |item| {
            const t = std.mem.trim(u8, item, " \t\r\n");
            const v = evalTextExpr(t, row) orelse continue;
            list.append(std.heap.page_allocator, v) catch {};
        }
        return Value{ .array = list.toOwnedSlice(std.heap.page_allocator) catch return null };
    }

    // ── outer parentheses: strip and re-evaluate ────────────────────
    if (trimmed.len >= 2 and trimmed[0] == '(' and trimmed[trimmed.len - 1] == ')') {
        // Only strip if the outer parens truly wrap the whole expression
        var depth: i32 = 0;
        var balanced = true;
        for (trimmed[0..trimmed.len - 1], 0..) |c, ci| {
            if (c == '(') depth += 1 else if (c == ')') {
                depth -= 1;
                if (depth == 0 and ci < trimmed.len - 1) { balanced = false; break; }
            }
        }
        if (balanced) return evalTextExpr(trimmed[1 .. trimmed.len - 1], row);
    }

    // ── logical OR / AND as value expression ───────────────────────────
    if (findTopLevelKeyword(trimmed, "OR")) |pos| {
        const lv = evalTextExpr(trimmed[0..pos], row) orelse Value{ .i64 = 0 };
        const rv = evalTextExpr(trimmed[pos + 2 ..], row) orelse Value{ .i64 = 0 };
        const lb = switch (lv) { .i64 => |v| v != 0, .f64 => |v| v != 0.0, .uint8 => |v| v != 0, .str, .str_owned => |v| v.len > 0, else => false };
        const rb = switch (rv) { .i64 => |v| v != 0, .f64 => |v| v != 0.0, .uint8 => |v| v != 0, .str, .str_owned => |v| v.len > 0, else => false };
        const bit: u8 = if (lb or rb) 1 else 0;
        // Preserve UInt8 type only if both sides are UInt8 (pure bool OR)
        const both_u8 = (std.meta.activeTag(lv) == .uint8) and (std.meta.activeTag(rv) == .uint8);
        if (both_u8) return Value{ .uint8 = bit };
        return Value{ .i64 = bit };
    }
    if (findTopLevelKeyword(trimmed, "AND")) |pos| {
        const lv = evalTextExpr(trimmed[0..pos], row) orelse Value{ .i64 = 0 };
        const rv = evalTextExpr(trimmed[pos + 3 ..], row) orelse Value{ .i64 = 0 };
        const lb = switch (lv) { .i64 => |v| v != 0, .f64 => |v| v != 0.0, .uint8 => |v| v != 0, .str, .str_owned => |v| v.len > 0, else => false };
        const rb = switch (rv) { .i64 => |v| v != 0, .f64 => |v| v != 0.0, .uint8 => |v| v != 0, .str, .str_owned => |v| v.len > 0, else => false };
        const bit: u8 = if (lb and rb) 1 else 0;
        const both_u8 = (std.meta.activeTag(lv) == .uint8) and (std.meta.activeTag(rv) == .uint8);
        if (both_u8) return Value{ .uint8 = bit };
        return Value{ .i64 = bit };
    }

     // ── comparison operators: =, !=, <>, <=, >=, <, > (top-level) ──
    // Lower precedence than arithmetic, higher than AND/OR.
    {
        const cmp_ops = [_][]const u8{ "!=", "<>", "<=", ">=", "=", "<", ">" };
        var found_op_s: ?[]const u8 = null;
        var found_pos_c: usize = 0;
        scan_cmp: {
            var depth: usize = 0;
            var case_depth: usize = 0; // track CASE...END blocks (depth-0 but opaque)
            var ci: usize = trimmed.len;
            while (ci > 0) {
                ci -= 1;
                const ch = trimmed[ci];
                if (ch == ')' or ch == ']') depth += 1;
                if (ch == '(' or ch == '[') { if (depth > 0) depth -= 1; }
                if (depth != 0) continue;
                // Track CASE/END at top level of parens (right-to-left: END encountered before CASE)
                if (ci + 3 <= trimmed.len and std.ascii.startsWithIgnoreCase(trimmed[ci..], "END") and
                    (ci + 3 >= trimmed.len or !(std.ascii.isAlphanumeric(trimmed[ci + 3]) or trimmed[ci+3] == '_')) and
                    (ci == 0 or !(std.ascii.isAlphanumeric(trimmed[ci - 1]) or trimmed[ci-1] == '_')))
                    { case_depth += 1; }
                if (ci + 4 <= trimmed.len and std.ascii.startsWithIgnoreCase(trimmed[ci..], "CASE") and
                    (ci + 4 >= trimmed.len or !(std.ascii.isAlphanumeric(trimmed[ci + 4]) or trimmed[ci+4] == '_')) and
                    (ci == 0 or !(std.ascii.isAlphanumeric(trimmed[ci - 1]) or trimmed[ci-1] == '_')))
                    { if (case_depth > 0) case_depth -= 1; }
                if (case_depth != 0) continue;
                for (cmp_ops) |op| {
                    if (ci + op.len > trimmed.len) continue;
                    if (!std.mem.eql(u8, trimmed[ci..ci + op.len], op)) continue;
                    found_op_s = op;
                    found_pos_c = ci;
                    break :scan_cmp;
                }
            }
        }
        if (found_op_s) |op_s| {
            const lhs_s = std.mem.trim(u8, trimmed[0..found_pos_c], " \t\r\n");
            const rhs_s = std.mem.trim(u8, trimmed[found_pos_c + op_s.len..], " \t\r\n");
            if (lhs_s.len > 0 and rhs_s.len > 0) {
                if (evalTextExpr(lhs_s, row)) |lv| {
                    if (evalTextExpr(rhs_s, row)) |rv| {
                        const result: bool = blk: {
                            if (std.mem.eql(u8, op_s, "=")) break :blk Value.eql(lv, rv);
                            if (std.mem.eql(u8, op_s, "!=") or std.mem.eql(u8, op_s, "<>")) break :blk !Value.eql(lv, rv);
                            const ord = Value.order(lv, rv);
                            if (std.mem.eql(u8, op_s, "<")) break :blk ord == .lt;
                            if (std.mem.eql(u8, op_s, ">")) break :blk ord == .gt;
                            if (std.mem.eql(u8, op_s, "<=")) break :blk ord != .gt;
                            if (std.mem.eql(u8, op_s, ">=")) break :blk ord != .lt;
                            break :blk false;
                        };
                        return Value{ .uint8 = if (result) 1 else 0 };
                    }
                }
            }
        }
    }

    // ── CASE WHEN … END (keyword-prefix, not a function call) ──────
    if (std.ascii.startsWithIgnoreCase(trimmed, "CASE "))
        return evalCaseWhen(trimmed, row);

    // ── comptime function dispatch ───────────────────────────────────
    // All function entries must end with ')'; we check this once here.
    if (trimmed[trimmed.len - 1] == ')') {
        inline for (func_evals) |entry| {
            // comptime: build prefix = lowercase_name ++ "("
            const prefix = entry.name ++ "(";
            if (trimmed.len > prefix.len and
                std.ascii.startsWithIgnoreCase(trimmed, prefix))
            {
                // Verify the '(' at prefix.len-1 is matched by the final ')'.
                // Otherwise this is "fn(args) op more" and should go to arithmetic.
                const is_whole_call = blk: {
                    var depth_check: usize = 0;
                    for (trimmed[prefix.len - 1..], 0..) |ch, ci| {
                        if (ch == '(') depth_check += 1
                        else if (ch == ')') {
                            if (depth_check > 0) depth_check -= 1;
                            if (depth_check == 0) {
                                // Check if this ')' is the last character
                                break :blk ci == trimmed.len - prefix.len;
                            }
                        }
                    }
                    break :blk false;
                };
                if (is_whole_call) {
                    const inner = trimmed[prefix.len .. trimmed.len - 1];
                    return evalFunc(entry.kind, entry.name, inner, row);
                }
            }
        }
        // ── user-defined functions (registered via CREATE FUNCTION) ──
        if (udf_registry) |udfs| {
            // Extract function name from call: everything before "("
            const paren_pos = std.mem.indexOfScalar(u8, trimmed, '(') orelse trimmed.len;
            const call_name = trimmed[0..paren_pos];
            const call_args_str = trimmed[paren_pos + 1 .. trimmed.len - 1];
            if (udfs.get(call_name)) |lambda| {
                // lambda is "(params) -> body"
                if (lambda.len > 0 and lambda[0] == '(') {
                    const close_paren = std.mem.indexOfScalar(u8, lambda, ')') orelse lambda.len;
                    const params_str = lambda[1..close_paren];
                    const arrow_pos = std.mem.indexOf(u8, lambda[close_paren..], "->") orelse 0;
                    const body = std.mem.trim(u8, lambda[close_paren + arrow_pos + 2..], " \t\r\n");
                    // Bind params → evaluated args in a child RowCtx
                    var param_names_buf: [16][]const u8 = undefined;
                    var param_vals_buf: [16]Value = undefined;
                    var param_count: usize = 0;
                    var param_it = std.mem.splitScalar(u8, params_str, ',');
                    var arg_it = std.mem.splitScalar(u8, call_args_str, ',');
                    while (param_it.next()) |param_raw| {
                        const arg_raw = arg_it.next() orelse break;
                        const param = std.mem.trim(u8, param_raw, " \t\r\n");
                        const arg_expr = std.mem.trim(u8, arg_raw, " \t\r\n");
                        const arg_val = evalTextExpr(arg_expr, row) orelse Value{ .str = "" };
                        if (param_count < param_names_buf.len) {
                            param_names_buf[param_count] = param;
                            param_vals_buf[param_count] = arg_val;
                            param_count += 1;
                        }
                    }
                    const inner_row = RowCtx{
                        .names = param_names_buf[0..param_count],
                        .values = param_vals_buf[0..param_count],
                        .parent = row,
                        .table = row.table,
                    };
                    return evalTextExpr(body, &inner_row);
                }
            }
        }
    }

    // ── arithmetic: expr op expr (top-level only) ───────────────────
    // Scan for +, -, *, / at top level (not inside parens/brackets).
    {
        var depth: usize = 0;
        var i: usize = trimmed.len;
        // Scan right-to-left for lower-precedence operators first (+/-), then (*//)
        // to approximate standard precedence.
        var found_op: ?u8 = null;
        var found_pos: usize = 0;
        // First pass: find + or - at depth 0 (right-to-left to get correct associativity)
        depth = 0;
        i = trimmed.len;
        while (i > 0) {
            i -= 1;
            const ch = trimmed[i];
            if (ch == ')' or ch == ']') depth += 1;
            if (ch == '(' or ch == '[') { if (depth > 0) depth -= 1; }
            if (depth == 0 and (ch == '+' or ch == '-') and i > 0) {
                // Skip if this is the exponent sign in scientific notation (e.g. 1.0e-9)
                const prev = trimmed[i - 1];
                if (prev == 'e' or prev == 'E') continue;
                found_op = ch;
                found_pos = i;
                break;
            }
        }
        // Second pass (if no +/-): find * or /
        if (found_op == null) {
            depth = 0;
            i = trimmed.len;
            while (i > 0) {
                i -= 1;
                const ch = trimmed[i];
                if (ch == ')' or ch == ']') depth += 1;
                if (ch == '(' or ch == '[') { if (depth > 0) depth -= 1; }
                if (depth == 0 and (ch == '*' or ch == '/' or ch == '%') and i > 0) {
                    found_op = ch;
                    found_pos = i;
                    break;
                }
            }
        }
        if (found_op) |op| {
            const lhs_str = std.mem.trim(u8, trimmed[0..found_pos], " \t\r\n");
            const rhs_str = std.mem.trim(u8, trimmed[found_pos + 1..], " \t\r\n");
            if (lhs_str.len > 0 and rhs_str.len > 0) {
                if (evalTextExpr(lhs_str, row)) |lv| {
                    if (evalTextExpr(rhs_str, row)) |rv| {
                        // DateTime string arithmetic: parse "YYYY-MM-DD HH:MM:SS" as Unix seconds
                        const la = lv.toF64() orelse datetimeStrToSecs(lv.toStr() orelse return null) orelse return null;
                        const ra = rv.toF64() orelse datetimeStrToSecs(rv.toStr() orelse return null) orelse return null;
                        const res: f64 = switch (op) {
                            '+' => la + ra,
                            '-' => la - ra,
                            '*' => la * ra,
                            '/' => if (ra == 0.0) 0.0 else la / ra,
                            '%' => if (ra == 0.0) 0.0 else @mod(la, ra),
                            else => unreachable,
                        };
                        // Return as i64 if result is whole number
                        if (res == @floor(res) and res >= @as(f64, @floatFromInt(std.math.minInt(i64))) and res <= @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                            return Value{ .i64 = @intFromFloat(res) };
                        }
                        return Value{ .f64 = res };
                    }
                }
            }
        }
        // Unary minus: "-expr" with no +/- found at i>0
        if (trimmed.len > 1 and trimmed[0] == '-') {
            const inner_str = std.mem.trim(u8, trimmed[1..], " \t\r\n");
            if (evalTextExpr(inner_str, row)) |rv| {
                const ra = rv.toF64() orelse return null;
                const res = -ra;
                if (res == @floor(res) and res >= @as(f64, @floatFromInt(std.math.minInt(i64))) and res <= @as(f64, @floatFromInt(std.math.maxInt(i64)))) {
                    return Value{ .i64 = @intFromFloat(res) };
                }
                return Value{ .f64 = res };
            }
        }
    }

    // ── SQL aggregate functions used in projection expressions ──────
    // e.g. max(confidence) inside 1-(1-max(confidence))*... should resolve
    // to the column value (or alias) in the current row context.
    if (trimmed[trimmed.len - 1] == ')') {
        const sql_aggs = [_][]const u8{ "max(", "min(", "sum(", "avg(", "any(", "count(" };
        inline for (sql_aggs) |prefix| {
            if (std.ascii.startsWithIgnoreCase(trimmed, prefix)) {
                const col_inner = std.mem.trim(u8, trimmed[prefix.len .. trimmed.len - 1], " \t\r\n");
                if (row.get(col_inner)) |v| return v;
                // Also try the full expression as key (e.g. "avg(confidence)" registered by header)
                if (row.get(trimmed)) |v| return v;
                break;
            }
        }
    }

    // ── fallback: direct column lookup ──────────────────────────────
    return row.get(trimmed);
}

/// Dispatch to the concrete implementation for each EvalKind.
fn evalFunc(kind: EvalKind, name: []const u8, inner: []const u8, row: *const RowCtx) ?Value {
    switch (kind) {
        // ── conditional ────────────────────────────────────────────
        .cond_if => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len != 3) return null;
            const cond = std.mem.trim(u8, args.items[0], " \t\r\n");
            const then = std.mem.trim(u8, args.items[1], " \t\r\n");
            const els  = std.mem.trim(u8, args.items[2], " \t\r\n");
            const then_val = evalTextExpr(then, row);
            const else_val = evalTextExpr(els,  row);
            const chosen = if (evalTextBoolExpr(cond, row)) then_val else else_val;
            // Coerce: if either branch is f64, return f64 to keep type consistent.
            if (then_val != null and else_val != null) {
                const tv = then_val.?;
                const ev = else_val.?;
                const either_float = (tv == .f64) or (ev == .f64);
                if (either_float) {
                    if (chosen) |cv| {
                        return switch (cv) {
                            .i64 => |v| Value{ .f64 = @floatFromInt(v) },
                            else => cv,
                        };
                    }
                }
            }
            return chosen;
        },
        // ── string functions ───────────────────────────────────────
        .str_lower => {
            const v = evalTextExpr(inner, row) orelse return null;
            const sv = v.toStr() orelse return v;
            const r = std.ascii.allocLowerString(std.heap.page_allocator, sv) catch return v;
            return Value{ .str_owned = r };
        },
        .str_upper => {
            const v = evalTextExpr(inner, row) orelse return null;
            const sv = v.toStr() orelse return v;
            const r = std.ascii.allocUpperString(std.heap.page_allocator, sv) catch return v;
            return Value{ .str_owned = r };
        },
        .str_concat => {
            const args = splitTopLevelArgs(inner) catch return Value{ .str = "" };
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            defer buf.deinit(std.heap.page_allocator);
            for (args.items[0..args.len]) |arg| {
                const a = std.mem.trim(u8, arg, " \t\r\n");
                const v = evalTextExpr(a, row) orelse continue;
                buf.appendSlice(std.heap.page_allocator, v.toStr() orelse continue) catch {};
            }
            const r = buf.toOwnedSlice(std.heap.page_allocator) catch return Value{ .str = "" };
            return Value{ .str_owned = r };
        },
        .str_substring => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const sv = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            const pos_raw = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toI64() orelse return null;
            const pos = pos_raw - 1; // CH 1-based → 0-based
            if (pos < 0 or @as(usize, @intCast(pos)) >= sv.len) return Value{ .str = "" };
            const start: usize = @intCast(pos);
            if (args.len >= 3) {
                const len_v = (evalTextExpr(std.mem.trim(u8, args.items[2], " \t\r\n"), row) orelse return null).toI64() orelse return null;
                if (len_v <= 0) return Value{ .str = "" };
                return Value{ .str = sv[start..@min(start + @as(usize, @intCast(len_v)), sv.len)] };
            }
            return Value{ .str = sv[start..] };
        },
        .str_starts_with => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const sv  = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            const pfx = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            return Value{ .i64 = if (std.mem.startsWith(u8, sv, pfx)) 1 else 0 };
        },
        .str_position_ci => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const hay = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            const ndl = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            if (std.ascii.indexOfIgnoreCase(hay, ndl)) |pos| return Value{ .i64 = @intCast(pos + 1) };
            return Value{ .i64 = 0 };
        },
        .str_position => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const hay = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            const ndl = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toStr() orelse return Value{ .i64 = 0 };
            if (std.mem.indexOf(u8, hay, ndl)) |pos| return Value{ .i64 = @intCast(pos + 1) };
            return Value{ .i64 = 0 };
        },
        .str_length => {
            const v = evalTextExpr(inner, row) orelse return Value{ .i64 = 0 };
            switch (v) {
                .array => |a| return Value{ .i64 = @intCast(a.len) },
                .str, .str_owned => {
                    const sv = v.toStr().?;
                    if (std.mem.indexOfScalar(u8, sv, '\x0c') != null) {
                        // \x0c-delimited array: each element is preceded by \x0c
                        // so element count == number of \x0c characters
                        var count: usize = 0;
                        for (sv) |ch| if (ch == '\x0c') { count += 1; };
                        return Value{ .i64 = @intCast(count) };
                    }
                    return Value{ .i64 = @intCast(sv.len) };
                },
                else => return Value{ .i64 = 0 },
            }
        },
        .not_empty => {
            const v = evalTextExpr(inner, row) orelse return Value{ .uint8 = 0 };
            return switch (v) {
                .array     => |a| Value{ .uint8 = if (a.len > 0) 1 else 0 },
                .str, .str_owned => Value{ .uint8 = if ((v.toStr().?.len) > 0) 1 else 0 },
                .null_val  => Value{ .uint8 = 0 },
                else       => Value{ .uint8 = 1 },
            };
        },
        .is_empty => {
            const v = evalTextExpr(inner, row) orelse return Value{ .uint8 = 1 };
            return switch (v) {
                .array     => |a| Value{ .uint8 = if (a.len == 0) 1 else 0 },
                .str, .str_owned => Value{ .uint8 = if ((v.toStr().?.len) == 0) 1 else 0 },
                .null_val  => Value{ .uint8 = 1 },
                else       => Value{ .uint8 = 0 },
            };
        },
        .reinterpret_as_str => {
            const v = evalTextExpr(inner, row) orelse return null;
            // Convert integer to little-endian bytes, strip trailing NUL bytes, return as string.
            const n: u64 = switch (v) {
                .i64   => |i| @bitCast(i),
                .uint8 => |u| @intCast(u),
                .f64   => |f| @bitCast(@as(u64, @bitCast(f))),
                else   => return null,
            };
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, n, .little);
            // Trim trailing NUL bytes
            var end: usize = 8;
            while (end > 0 and buf[end - 1] == 0) end -= 1;
            const alloc_tmp = std.heap.page_allocator;
            const s = alloc_tmp.dupe(u8, buf[0..end]) catch return null;
            return Value{ .str_owned = s };
        },
        // ── numeric functions ──────────────────────────────────────
        .num_floor => {
            const v = evalTextExpr(inner, row) orelse return null;
            return Value{ .i64 = @intFromFloat(@floor(v.toF64() orelse return null)) };
        },
        .num_round => {
            const v = evalTextExpr(inner, row) orelse return null;
            return Value{ .i64 = @intFromFloat(@round(v.toF64() orelse return null)) };
        },
        .num_abs => {
            const v = evalTextExpr(inner, row) orelse return null;
            return switch (v) {
                .i64 => |i| Value{ .i64 = @intCast(@abs(i)) },
                .f64 => |f| Value{ .f64 = @abs(f) },
                else => null,
            };
        },
        .num_greatest => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            var best: f64 = -std.math.inf(f64);
            for (args.items[0..args.len]) |arg| {
                const fv = (evalTextExpr(std.mem.trim(u8, arg, " \t\r\n"), row) orelse continue).toF64() orelse continue;
                if (fv > best) best = fv;
            }
            if (best == -std.math.inf(f64)) return null;
            return Value{ .f64 = best };
        },
        .num_least => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            var best: f64 = std.math.inf(f64);
            for (args.items[0..args.len]) |arg| {
                const fv = (evalTextExpr(std.mem.trim(u8, arg, " \t\r\n"), row) orelse continue).toF64() orelse continue;
                if (fv < best) best = fv;
            }
            if (best == std.math.inf(f64)) return null;
            return Value{ .f64 = best };
        },
        // ── date functions ─────────────────────────────────────────
        .date_yyyymmdd => {
            const v = evalTextExpr(inner, row) orelse return null;
            switch (v) {
                .i64 => |ts| {
                    const ts_s: u64 = @intCast(if (ts > 1_000_000_000_000) @divFloor(ts, 1000) else if (ts > 0) ts else 0);
                    const epoch_s = std.time.epoch.EpochSeconds{ .secs = ts_s };
                    const epoch_day = epoch_s.getEpochDay();
                    const yad = epoch_day.calculateYearDay();
                    const mad = yad.calculateMonthDay();
                    return Value{ .i64 = @as(i64, yad.year) * 10000 +
                        @as(i64, @intFromEnum(mad.month)) * 100 +
                        @as(i64, mad.day_index + 1) };
                },
                else => {
                    const sv = v.toStr() orelse return null;
                    if (sv.len >= 10 and sv[4] == '-' and sv[7] == '-') {
                        var buf: [8]u8 = undefined;
                        @memcpy(buf[0..4], sv[0..4]);
                        @memcpy(buf[4..6], sv[5..7]);
                        @memcpy(buf[6..8], sv[8..10]);
                        return Value{ .i64 = std.fmt.parseInt(i64, &buf, 10) catch return null };
                    }
                    return null;
                },
            }
        },
         .date_trunc => {
            // inner = "'unit', col_expr"
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const unit_raw = std.mem.trim(u8, args.items[0], " \t'\"`");
            const col_expr = std.mem.trim(u8, args.items[1], " \t\r\n");
            const v = evalTextExpr(col_expr, row) orelse return null;
            // Normalize to epoch seconds
            const ts_s: i64 = switch (v) {
                .i64 => |ts| if (ts > 1_000_000_000_000) @divFloor(ts, 1000) else ts,
                .f64 => |ts| @intFromFloat(if (ts > 1_000_000_000_000.0) ts / 1000.0 else ts),
                .str, .str_owned => blk: {
                    const sv = v.toStr() orelse break :blk 0;
                    // Try numeric parse first
                    if (std.fmt.parseInt(i64, sv, 10) catch null) |iv|
                        break :blk if (iv > 1_000_000_000_000) @divFloor(iv, 1000) else iv;
                    // Try datetime string "YYYY-MM-DD HH:MM:SS"
                    if (sv.len >= 10) {
                        const yr = std.fmt.parseInt(u32, sv[0..4], 10) catch break :blk 0;
                        const mo = std.fmt.parseInt(u32, sv[5..7], 10) catch break :blk 0;
                        const dy = std.fmt.parseInt(u32, sv[8..10], 10) catch break :blk 0;
                        var hh: u32 = 0; var mm: u32 = 0; var ss: u32 = 0;
                        if (sv.len >= 13) hh = std.fmt.parseInt(u32, sv[11..13], 10) catch 0;
                        if (sv.len >= 16) mm = std.fmt.parseInt(u32, sv[14..16], 10) catch 0;
                        if (sv.len >= 19) ss = std.fmt.parseInt(u32, sv[17..19], 10) catch 0;
                        // Approximate days from epoch (not leap-aware, close enough)
                        const days_per_month = [_]u32{31,28,31,30,31,30,31,31,30,31,30,31};
                        var days: u64 = (yr - 1970) * 365 + (yr - 1969) / 4;
                        for (days_per_month[0..mo-1]) |d| days += d;
                        days += dy - 1;
                        const epoch = @as(i64, @intCast(days * 86400)) + @as(i64, hh * 3600 + mm * 60 + ss);
                        break :blk epoch;
                    }
                    break :blk 0;
                },
                else => 0,
            };
            // Truncate to the requested unit (in seconds)
            const truncated_s: i64 = if (std.ascii.eqlIgnoreCase(unit_raw, "minute"))
                @divFloor(ts_s, 60) * 60
            else if (std.ascii.eqlIgnoreCase(unit_raw, "hour"))
                @divFloor(ts_s, 3600) * 3600
            else if (std.ascii.eqlIgnoreCase(unit_raw, "day"))
                @divFloor(ts_s, 86400) * 86400
            else if (std.ascii.eqlIgnoreCase(unit_raw, "month")) blk: {
                // Truncate to start of month — convert to yyyy/mm, back to epoch
                const ts_u: u64 = @intCast(@max(ts_s, 0));
                const epoch_s2 = std.time.epoch.EpochSeconds{ .secs = ts_u };
                const epoch_day2 = epoch_s2.getEpochDay();
                const yad2 = epoch_day2.calculateYearDay();
                const mad2 = yad2.calculateMonthDay();
                const days_per_month2 = [_]u32{31,28,31,30,31,30,31,31,30,31,30,31};
                var days2: u64 = (@as(u64, yad2.year) - 1970) * 365 + (@as(u64, yad2.year) - 1969) / 4;
                const mo2 = @intFromEnum(mad2.month);
                for (days_per_month2[0..mo2]) |d| days2 += d;
                break :blk @as(i64, @intCast(days2 * 86400));
            }
            else if (std.ascii.eqlIgnoreCase(unit_raw, "week"))
                @divFloor(ts_s, 604800) * 604800
            else if (std.ascii.eqlIgnoreCase(unit_raw, "year")) blk: {
                const ts_u2: u64 = @intCast(@max(ts_s, 0));
                const epoch_s3 = std.time.epoch.EpochSeconds{ .secs = ts_u2 };
                const epoch_day3 = epoch_s3.getEpochDay();
                const yad3 = epoch_day3.calculateYearDay();
                const days3: u64 = (@as(u64, yad3.year) - 1970) * 365 + (@as(u64, yad3.year) - 1969) / 4;
                break :blk @as(i64, @intCast(days3 * 86400));
            }
            else
                @divFloor(ts_s, 86400) * 86400;
            // Return epoch milliseconds (so native block encoder treats as DateTime64(3))
            return Value{ .i64 = truncated_s * 1000 };
        },
        // ── date extraction ────────────────────────────────────────
        .date_year, .date_month, .date_day, .date_hour, .date_minute, .date_second => {
            const v = evalTextExpr(inner, row) orelse return null;
            // Parse to epoch seconds
            const ts_s: i64 = switch (v) {
                .i64 => |ts| if (ts > 1_000_000_000_000) @divFloor(ts, 1000) else ts,
                .f64 => |ts| @intFromFloat(if (ts > 1_000_000_000_000.0) ts / 1000.0 else ts),
                .str, .str_owned => blk: {
                    const sv = v.toStr() orelse break :blk 0;
                    if (std.fmt.parseInt(i64, sv, 10) catch null) |iv|
                        break :blk if (iv > 1_000_000_000_000) @divFloor(iv, 1000) else iv;
                    if (sv.len >= 10) {
                        const yr = std.fmt.parseInt(i64, sv[0..4], 10) catch break :blk 0;
                        const mo = std.fmt.parseInt(i64, sv[5..7], 10) catch break :blk 0;
                        const dy = std.fmt.parseInt(i64, sv[8..10], 10) catch break :blk 0;
                        var hh: i64 = 0; var mm: i64 = 0; var ss: i64 = 0;
                        if (sv.len >= 13) hh = std.fmt.parseInt(i64, sv[11..13], 10) catch 0;
                        if (sv.len >= 16) mm = std.fmt.parseInt(i64, sv[14..16], 10) catch 0;
                        if (sv.len >= 19) ss = std.fmt.parseInt(i64, sv[17..19], 10) catch 0;
                        const dpm = [_]i64{31,28,31,30,31,30,31,31,30,31,30,31};
                        var days: i64 = (yr - 1970) * 365 + @divFloor(yr - 1969, 4);
                        for (dpm[0..@intCast(mo - 1)]) |d| days += d;
                        days += dy - 1;
                        break :blk days * 86400 + hh * 3600 + mm * 60 + ss;
                    }
                    break :blk 0;
                },
                else => 0,
            };
            const ts_u: u64 = @intCast(@max(ts_s, 0));
            const epoch_s4 = std.time.epoch.EpochSeconds{ .secs = ts_u };
            const epoch_day4 = epoch_s4.getEpochDay();
            const yad4 = epoch_day4.calculateYearDay();
            const mad4 = yad4.calculateMonthDay();
            return switch (kind) {
                .date_year   => Value{ .i64 = @as(i64, yad4.year) },
                .date_month  => Value{ .i64 = @as(i64, @intFromEnum(mad4.month)) },
                .date_day    => Value{ .i64 = @as(i64, mad4.day_index) + 1 },
                .date_hour   => Value{ .i64 = @mod(@divFloor(ts_s, 3600), 24) },
                .date_minute => Value{ .i64 = @mod(@divFloor(ts_s, 60), 60) },
                .date_second => Value{ .i64 = @mod(ts_s, 60) },
                else => unreachable,
            };
        },
        .ip_bool => {
            const v = evalTextExpr(inner, row) orelse return Value{ .i64 = 0 };
            const sv = v.toStr() orelse return Value{ .i64 = 0 };
            const ok = if (std.ascii.eqlIgnoreCase(name, "isipv4string")) isIPv4String(sv)
                       else isIPv6String(sv);
            return Value{ .i64 = if (ok) 1 else 0 };
        },
        .ip_to_num => {
            const v = evalTextExpr(inner, row) orelse return Value{ .i64 = 0 };
            return Value{ .i64 = ipv4ToNum(v.toStr() orelse return Value{ .i64 = 0 }) };
        },
        // ── CAST ───────────────────────────────────────────────────
        .fn_now => {
            var ts: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(.REALTIME, &ts);
            return Value{ .i64 = ts.sec };
        },
        .fn_to_days => {
            // DuckDB translates `INTERVAL N DAY` to `to_days(N)`.
            // Return N*86400 so that `now() - to_days(1)` = yesterday in Unix seconds.
            const v = evalTextExpr(inner, row) orelse return null;
            const n = v.toF64() orelse return null;
            return Value{ .i64 = @intFromFloat(n * 86400.0) };
        },
        .fn_today => {
            var ts: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(.REALTIME, &ts);
            // days since epoch
            return Value{ .date = @intCast(@divFloor(ts.sec, 86400)) };
        },
        .fn_yesterday => {
            var ts: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(.REALTIME, &ts);
            return Value{ .date = @intCast(@divFloor(ts.sec, 86400) - 1) };
        },
        .empty_arr_str, .empty_arr_int => {
            return Value{ .array = &.{} };
        },
        .fn_to_datetime => {
            const v = evalTextExpr(inner, row) orelse return null;
            switch (v) {
                .i64, .uint8 => return v, // already seconds
                .f64 => |f| return Value{ .i64 = @intFromFloat(f) },
                .str, .str_owned => {
                    const s = v.toStr().?;
                    if (datetimeStrToSecs(s)) |secs| return Value{ .i64 = @intFromFloat(secs) };
                    if (std.fmt.parseInt(i64, s, 10) catch null) |n| return Value{ .i64 = n };
                    return null;
                },
                else => return null,
            }
        },
        .fn_array_element => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const arr_val = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null;
            const idx_val = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null;
            // Decode alias blob (from ScanCtx serialization: \x01 + \x0c-separated elements)
            const arr: []const Value = switch (arr_val) {
                .array => |a| a,
                .str, .str_owned => blk: {
                    const s = arr_val.toStr().?;
                    if (s.len > 0 and s[0] == 0x01) {
                        // \x01-sentinel format: decode \x0c-separated elements
                        const content = s[1..];
                        const alloc_tmp = std.heap.page_allocator;
                        var items: std.ArrayListUnmanaged(Value) = .empty;
                        if (content.len > 0) {
                            var it = std.mem.splitScalar(u8, content, '\x0c');
                            while (it.next()) |elem| {
                                const trimmed_elem = std.mem.trim(u8, elem, " \t\r\n");
                                if (std.fmt.parseInt(i64, trimmed_elem, 10)) |iv| {
                                    items.append(alloc_tmp, Value{ .i64 = iv }) catch break;
                                } else |_| {
                                    if (std.fmt.parseFloat(f64, trimmed_elem)) |fv| {
                                        items.append(alloc_tmp, Value{ .f64 = fv }) catch break;
                                    } else |_| {
                                        items.append(alloc_tmp, Value{ .str = trimmed_elem }) catch break;
                                    }
                                }
                            }
                        }
                        break :blk items.toOwnedSlice(alloc_tmp) catch &.{};
                    }
                    return null;
                },
                else => return null,
            };
            const idx_raw: i64 = switch (idx_val) {
                .i64 => |n| n,
                .uint8 => |n| @intCast(n),
                .f64 => |f| @intFromFloat(f),
                else => return null,
            };
            // ClickHouse 1-based; negative counts from end; 0 or OOB → default (0 or empty)
            const len: i64 = @intCast(arr.len);
            // Determine default value: empty string for string arrays, 0 for numeric
            const str_default = arr.len > 0 and (arr[0] == .str or arr[0] == .str_owned);
            const oob_default: Value = if (str_default) Value{ .str = "" } else Value{ .i64 = 0 };
            const real_idx: i64 = if (idx_raw > 0) idx_raw - 1
                                  else if (idx_raw < 0) len + idx_raw
                                  else return oob_default;
            if (real_idx < 0 or real_idx >= len) return oob_default;
            return arr[@intCast(real_idx)];
        },
        .str_tostring => {
            const v = evalTextExpr(inner, row) orelse return Value{ .str = "" };
            // If argument is a datetime expression, format result as datetime string
            const inner_trim = std.mem.trim(u8, inner, " \t");
            const is_datetime_arg = std.ascii.startsWithIgnoreCase(inner_trim, "todatetime(") or
                std.ascii.startsWithIgnoreCase(inner_trim, "toDateTime64(") or
                (std.ascii.startsWithIgnoreCase(inner_trim, "CAST(") and
                 std.ascii.endsWithIgnoreCase(inner_trim, "AS TIMESTAMP)"));
            if (is_datetime_arg) {
                const secs: i64 = switch (v) {
                    .i64 => |n| n,
                    .uint8 => |u| @intCast(u),
                    else => blk: {
                        if (v.toF64()) |f| break :blk @intFromFloat(f);
                        break :blk 0;
                    },
                };
                return Value{ .str_owned = secsToDatetimeStr(secs) catch return Value{ .str = "" } };
            }
            switch (v) {
                .str, .str_owned => {
                    const s = v.toStr().?;
                    if (s.len == 16) {
                        if (ipv6BytesToStr(s)) |ip_str| return Value{ .str_owned = ip_str };
                    }
                    return v;
                },
                .i64 => |n| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{n}) catch return null },
                .f64 => |f| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{f}) catch return null },
                .uint8 => |u| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{u}) catch return null },
                .date => |d| {
                    const ymd = epochDaysToYmd(d);
                    const y: u32 = @intCast(@max(0, ymd.year));
                    return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ y, ymd.month, ymd.day }) catch return null };
                },
                else => return Value{ .str = "" },
            }
        },
        .cast_expr => {
            // Support both CAST(expr AS type) and CAST(expr, 'type') / CAST(expr, type)
            // Determine expr_part and type_part depending on syntax used.
            // Must find the LAST top-level " AS " (outside nested parens) so that
            // CAST(trunc(CAST(1 AS DOUBLE)) AS INTEGER) splits correctly.
            var cast_buf = [2][]const u8{ inner, "" };
            const as_pos: ?usize = blk: {
                var depth2: usize = 0;
                var j: usize = inner.len;
                while (j > 0) {
                    j -= 1;
                    const c = inner[j];
                    if (c == ')' or c == ']') depth2 += 1
                    else if (c == '(' or c == '[') { if (depth2 > 0) depth2 -= 1; }
                    else if (depth2 == 0 and j + 4 <= inner.len and
                             std.ascii.eqlIgnoreCase(inner[j..j+4], " AS ")) {
                        break :blk j;
                    }
                }
                break :blk null;
            };
            if (as_pos) |ap| {
                cast_buf[0] = std.mem.trim(u8, inner[0..ap], " \t\r\n");
                cast_buf[1] = std.mem.trim(u8, inner[ap + 4 ..], " \t\r\n");
            } else {
                const cast_args = splitTopLevelArgs(inner) catch return null;
                if (cast_args.len < 2) return null;
                cast_buf[0] = std.mem.trim(u8, cast_args.items[0], " \t\r\n");
                var tp = std.mem.trim(u8, cast_args.items[1], " \t\r\n");
                if (tp.len >= 2 and tp[0] == '\'' and tp[tp.len - 1] == '\'') tp = tp[1 .. tp.len - 1];
                cast_buf[1] = tp;
            }
            const expr_part = cast_buf[0];
            const type_part = cast_buf[1];
            // Empty array literal "[]" → Value.array &.{} before general eval
            const v = if (std.mem.eql(u8, std.mem.trim(u8, expr_part, " \t"), "[]"))
                Value{ .array = &.{} }
            else
                evalTextExpr(expr_part, row) orelse return null;
            if (std.ascii.eqlIgnoreCase(type_part, "VARCHAR") or
                std.ascii.eqlIgnoreCase(type_part, "STRING"))
            {
                switch (v) {
                    .str, .str_owned => {
                        const s = v.toStr().?;
                        if (s.len == 16) {
                            if (ipv6BytesToStr(s)) |ip_str| return Value{ .str_owned = ip_str };
                        }
                        return v;
                    },
                    .i64 => |n| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{n}) catch return null },
                    .f64 => |f| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{f}) catch return null },
                    .uint8 => |u| return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{u}) catch return null },
                    .date => |d| {
                        const ymd = epochDaysToYmd(d);
                        const y: u32 = @intCast(@max(0, ymd.year));
                        return Value{ .str_owned = std.fmt.allocPrint(std.heap.page_allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ y, ymd.month, ymd.day }) catch return null };
                    },
                    else => return Value{ .str = "" },
                }
            }
            if (std.ascii.eqlIgnoreCase(type_part, "DATE")) {
                // Handle both string timestamps and integer Unix timestamps
                // Return Value.date (days since 1970) so native block encodes as Date (UInt16).
                switch (v) {
                    .date => return v, // already a date
                    .i64 => |ts| {
                        const ts_s: u64 = @intCast(if (ts > 1_000_000_000_000) @divFloor(ts, 1000) else if (ts > 0) ts else 0);
                        const days: u16 = @intCast(@min(65535, ts_s / 86400));
                        return Value{ .date = days };
                    },
                    else => {
                        // Try string "YYYY-MM-DD"
                        const s = v.toStr() orelse return null;
                        if (s.len >= 10 and s[4] == '-' and s[7] == '-') {
                            const y  = std.fmt.parseInt(i32, s[0..4], 10) catch null;
                            const mo = std.fmt.parseInt(u8,  s[5..7], 10) catch null;
                            const dy = std.fmt.parseInt(u8,  s[8..10], 10) catch null;
                            if (y != null and mo != null and dy != null) {
                                const d = dateToDays(y.?, mo.?, dy.?);
                                return Value{ .date = @intCast(@max(0, @min(65535, d))) };
                            }
                        }
                        return Value{ .str = s };
                    },
                }
            }
            // For array/list types, preserve as Value.array so writeCsv emits the \x01 sentinel
            if (std.ascii.startsWithIgnoreCase(type_part, "VARCHAR[]") or
                std.ascii.eqlIgnoreCase(type_part, "LIST") or
                std.ascii.startsWithIgnoreCase(type_part, "Array("))
            {
                switch (v) {
                    .array => return v,
                    else => {},
                }
            }
            // TIMESTAMP / DATETIME: parse string "YYYY-MM-DD HH:MM:SS" and return as i64 seconds
            if (std.ascii.eqlIgnoreCase(type_part, "TIMESTAMP") or
                std.ascii.eqlIgnoreCase(type_part, "DATETIME"))
            {
                switch (v) {
                    .i64 => return v, // integer → DateTime (keep as i64 seconds for arithmetic)
                    .str, .str_owned => {
                        // "YYYY-MM-DD HH:MM:SS" → parse to i64 Unix seconds
                        const s = v.toStr().?;
                        if (datetimeStrToSecs(s)) |secs| return Value{ .i64 = @intFromFloat(secs) };
                        return v; // pass through if unparseable
                    },
                    else => return v,
                }
            }
            return v;
        },
        .arr_make => {
            // list_value(a, b, ...) / array(a, b, ...) → Value.array
            const args = splitTopLevelArgs(inner) catch return Value{ .array = &.{} };
            var list: std.ArrayListUnmanaged(Value) = .empty;
            for (args.items[0..args.len]) |item| {
                const t = std.mem.trim(u8, item, " \t\r\n");
                const v = evalTextExpr(t, row) orelse continue;
                list.append(std.heap.page_allocator, v) catch {};
            }
            return Value{ .array = list.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_split_char, .arr_split_str => {
            // splitByChar(delim, str) / splitByString(delim, str)
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const delim = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            const s     = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            var list: std.ArrayListUnmanaged(Value) = .empty;
            var it = std.mem.splitSequence(u8, s, delim);
            while (it.next()) |part|
                list.append(std.heap.page_allocator, Value{ .str = part }) catch {};
            return Value{ .array = list.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        // ── Array predicates ───────────────────────────────────────
        .arr_has => {
            const args = splitTopLevelArgs(inner) catch return Value{ .uint8 = 0 };
            if (args.len < 2) return Value{ .uint8 = 0 };
            const arr_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .uint8 = 0 };
            const val_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .uint8 = 0 };
            const arr = valueToArray(arr_v) orelse return Value{ .uint8 = 0 };
            for (arr) |elem| if (Value.eql(elem, val_v)) return Value{ .uint8 = 1 };
            return Value{ .uint8 = 0 };
        },
        .arr_has_any => {
            const args = splitTopLevelArgs(inner) catch return Value{ .uint8 = 0 };
            if (args.len < 2) return Value{ .uint8 = 0 };
            const arr_v  = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .uint8 = 0 };
            const vals_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .uint8 = 0 };
            const arr  = valueToArray(arr_v)  orelse return Value{ .uint8 = 0 };
            const vals = valueToArray(vals_v) orelse return Value{ .uint8 = 0 };
            for (vals) |needle| for (arr) |elem| if (Value.eql(elem, needle)) return Value{ .uint8 = 1 };
            return Value{ .uint8 = 0 };
        },
        .arr_has_all => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const arr_v  = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const vals_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const arr  = valueToArray(arr_v)  orelse return Value{ .i64 = 0 };
            const vals = valueToArray(vals_v) orelse return Value{ .i64 = 0 };
            for (vals) |needle| {
                var found = false;
                for (arr) |elem| if (Value.eql(elem, needle)) { found = true; break; };
                if (!found) return Value{ .i64 = 0 };
            }
            return Value{ .i64 = 1 };
        },
        .arr_index_of => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const arr_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const val_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const arr = valueToArray(arr_v) orelse return Value{ .i64 = 0 };
            for (arr, 0..) |elem, i| if (Value.eql(elem, val_v)) return Value{ .i64 = @intCast(i + 1) };
            return Value{ .i64 = 0 };
        },
        // ── Array transforms ───────────────────────────────────────
        .arr_filter => {
            // arrayFilter(x -> cond, arr)
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const lambda = std.mem.trim(u8, args.items[0], " \t\r\n");
            const arr_expr = std.mem.trim(u8, args.items[1], " \t\r\n");
            const arr_v  = evalTextExpr(arr_expr, row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (arr) |elem| if (evalLambdaBool(lambda, elem, row)) out.append(std.heap.page_allocator, elem) catch {};
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_map => {
            // arrayMap(x -> expr, arr)
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const lambda = std.mem.trim(u8, args.items[0], " \t\r\n");
            const arr_expr2 = std.mem.trim(u8, args.items[1], " \t\r\n");
            const arr_v  = evalTextExpr(arr_expr2, row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (arr) |elem| {
                const mapped = evalLambdaBody(lambda, elem, row) orelse Value{ .null_val = {} };
                out.append(std.heap.page_allocator, mapped) catch {};
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_exists => {
            // arrayExists(x -> cond, arr) → 1 if any element satisfies cond
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len < 2) return Value{ .i64 = 0 };
            const lambda = std.mem.trim(u8, args.items[0], " \t\r\n");
            const arr_v  = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const arr = valueToArray(arr_v) orelse return Value{ .i64 = 0 };
            for (arr) |elem| if (evalLambdaBool(lambda, elem, row)) return Value{ .i64 = 1 };
            return Value{ .i64 = 0 };
        },
        .arr_flatten => {
            const arr_v = evalTextExpr(inner, row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (arr) |elem| {
                switch (elem) {
                    .array => |inner_arr| for (inner_arr) |e| out.append(std.heap.page_allocator, e) catch {},
                    .str, .str_owned => {
                        // nested \f-separated sub-array stored as string
                        const sub = valueToArray(elem) orelse { out.append(std.heap.page_allocator, elem) catch {}; continue; };
                        for (sub) |e| out.append(std.heap.page_allocator, e) catch {};
                    },
                    else => out.append(std.heap.page_allocator, elem) catch {},
                }
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_sum => {
            const arr_v = evalTextExpr(inner, row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            var total: f64 = 0;
            for (arr) |elem| total += elem.toF64() orelse 0;
            if (total == @floor(total) and @abs(total) < 9.007199e15)
                return Value{ .i64 = @intFromFloat(total) };
            return Value{ .f64 = total };
        },
        .arr_distinct => {
            const arr_v = evalTextExpr(inner, row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            outer: for (arr) |elem| {
                for (out.items) |existing| if (Value.eql(existing, elem)) continue :outer;
                out.append(std.heap.page_allocator, elem) catch {};
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_max => {
            const v = evalTextExpr(inner, row) orelse return null;
            const arr = valueToArray(v) orelse return null;
            if (arr.len == 0) return Value{ .null_val = {} };
            var best = arr[0];
            for (arr[1..]) |e| {
                const ef = e.toF64() orelse continue;
                const bf = best.toF64() orelse continue;
                if (ef > bf) best = e;
            }
            return best;
        },
        .arr_min => {
            const v = evalTextExpr(inner, row) orelse return null;
            const arr = valueToArray(v) orelse return null;
            if (arr.len == 0) return Value{ .null_val = {} };
            var best = arr[0];
            for (arr[1..]) |e| {
                const ef = e.toF64() orelse continue;
                const bf = best.toF64() orelse continue;
                if (ef < bf) best = e;
            }
            return best;
        },
        .arr_concat => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (args.items[0..args.len]) |arg| {
                const v = evalTextExpr(std.mem.trim(u8, arg, " \t\r\n"), row) orelse continue;
                const sub = valueToArray(v) orelse { out.append(std.heap.page_allocator, v) catch {}; continue; };
                for (sub) |e| out.append(std.heap.page_allocator, e) catch {};
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .arr_slice => {
            // arraySlice(arr, offset, length) — 1-based offset, length count
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const arr_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null;
            const arr = valueToArray(arr_v) orelse return null;
            const off_raw = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toI64() orelse return null;
            const off: usize = if (off_raw <= 0) 0 else @intCast(off_raw - 1);
            if (off >= arr.len) return Value{ .array = &.{} };
            const rest = arr[off..];
            if (args.len >= 3) {
                const len_raw = (evalTextExpr(std.mem.trim(u8, args.items[2], " \t\r\n"), row) orelse return null).toI64() orelse return null;
                const take: usize = if (len_raw <= 0) 0 else @min(@as(usize, @intCast(len_raw)), rest.len);
                return Value{ .array = rest[0..take] };
            }
            return Value{ .array = rest };
        },
         .arr_str_join => {
            // arrayStringConcat(arr [, sep])
            const args = splitTopLevelArgs(inner) catch return Value{ .str = "" };
            if (args.len < 1) return Value{ .str = "" };
            const arr_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const arr = valueToArray(arr_v) orelse return Value{ .str = "" };
            const sep: []const u8 = if (args.len >= 2) blk: {
                const sv = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse break :blk "";
                break :blk sv.toStr() orelse "";
            } else "";
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            for (arr, 0..) |elem, i| {
                if (i > 0) buf.appendSlice(std.heap.page_allocator, sep) catch {};
                buf.appendSlice(std.heap.page_allocator, elem.toStr() orelse "") catch {};
            }
            return Value{ .str_owned = buf.toOwnedSlice(std.heap.page_allocator) catch return Value{ .str = "" } };
        },
        .arr_enumerate => {
            // arrayEnumerate(arr) → [1, 2, ..., len(arr)]
            const arr_v = evalTextExpr(inner, row) orelse return Value{ .array = &.{} };
            const arr = valueToArray(arr_v) orelse return Value{ .array = &.{} };
            const out = std.heap.page_allocator.alloc(Value, arr.len) catch return Value{ .array = &.{} };
            for (0..arr.len) |i| out[i] = Value{ .i64 = @intCast(i + 1) };
            return Value{ .array = out };
        },
        .arr_enumerate_uniq => {
            // arrayEnumerateUniq(arr) → for each element, position within run of equal values
            const args = splitTopLevelArgs(inner) catch return Value{ .array = &.{} };
            if (args.len == 0) return Value{ .array = &.{} };
            const arr_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .array = &.{} };
            const arr = valueToArray(arr_v) orelse return Value{ .array = &.{} };
            const out = std.heap.page_allocator.alloc(Value, arr.len) catch return Value{ .array = &.{} };
            // For single array: count per unique value
            var counts = std.ArrayListUnmanaged(struct { v: Value, c: i64 }).empty;
            for (arr) |elem| {
                var found = false;
                for (counts.items) |*entry| {
                    if (Value.eql(entry.v, elem)) { entry.c += 1; found = true; break; }
                }
                if (!found) counts.append(std.heap.page_allocator, .{ .v = elem, .c = 1 }) catch {};
            }
            // Reset and recount for output
            for (counts.items) |*entry| entry.c = 0;
            for (arr, 0..) |elem, i| {
                for (counts.items) |*entry| {
                    if (Value.eql(entry.v, elem)) { entry.c += 1; out[i] = Value{ .i64 = entry.c }; break; }
                }
            }
            counts.deinit(std.heap.page_allocator);
            return Value{ .array = out };
        },
        // ── Map functions ─────────────────────────────────────────
        .map_keys => {
            const v = evalTextExpr(inner, row) orelse return null;
            const blob = v.toStr() orelse return null;
            // Map blob: varint N | N×(varint_len+key_bytes) | N×value_bytes
            if (blob.len == 0) return Value{ .array = &.{} };
            const count, const cb = readVarUIntSlice(blob) orelse return null;
            var kp: usize = cb;
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (0..count) |_| {
                const klen, const klb = readVarUIntSlice(blob[kp..]) orelse break;
                if (kp + klb + klen > blob.len) break;
                out.append(std.heap.page_allocator, Value{ .str = blob[kp + klb .. kp + klb + klen] }) catch {};
                kp += klb + klen;
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        .map_values => {
            const v = evalTextExpr(inner, row) orelse return null;
            const blob = v.toStr() orelse return null;
            if (blob.len == 0) return Value{ .array = &.{} };
            const count, const cb = readVarUIntSlice(blob) orelse return null;
            var kp: usize = cb;
            // Skip all keys first
            for (0..count) |_| {
                const klen, const klb = readVarUIntSlice(blob[kp..]) orelse return null;
                kp += klb + klen;
            }
            // Detect if values are fixed-width: remaining bytes == count * 8 (Float64)
            const remaining_total = blob.len - kp;
            const is_f64 = (count > 0 and remaining_total == count * 8);
            var out: std.ArrayListUnmanaged(Value) = .empty;
            for (0..count) |_| {
                if (kp >= blob.len) break;
                if (is_f64) {
                    if (kp + 8 > blob.len) break;
                    const bits = std.mem.readInt(u64, blob[kp..][0..8], .little);
                    out.append(std.heap.page_allocator, Value{ .f64 = @bitCast(bits) }) catch {};
                    kp += 8;
                } else {
                    const vlen, const vlb = readVarUIntSlice(blob[kp..]) orelse break;
                    if (kp + vlb + vlen > blob.len) break;
                    out.append(std.heap.page_allocator, Value{ .str = blob[kp + vlb .. kp + vlb + vlen] }) catch {};
                    kp += vlb + vlen;
                }
            }
            return Value{ .array = out.toOwnedSlice(std.heap.page_allocator) catch return null };
        },
        // ── Dict stubs ─────────────────────────────────────────────
        .stub_zero       => return Value{ .i64 = 0 },
        .stub_float_zero => return Value{ .f64 = 0.0 },
        .stub_bool_zero  => return Value{ .uint8 = 0 },
        .stub_empty_str  => return Value{ .str = "" },
        .stub_null       => return Value{ .null_val = {} },
        .stub_default_arg4 => {
            const args = splitTopLevelArgs(inner) catch return Value{ .str = "" };
            if (args.len >= 4)
                return evalTextExpr(std.mem.trim(u8, args.items[3], " \t\r\n"), row) orelse Value{ .str = "" };
            return Value{ .str = "" };
        },
        // ── IPv6NumToString(bytes) → IP string ─────────────────────
        .ipv6_num_to_str => {
            const v = evalTextExpr(inner, row) orelse return Value{ .str = "" };
            const s = v.toStr() orelse return Value{ .str = "" };
            if (s.len == 16) {
                if (ipv6BytesToStr(s)) |ip_str| return Value{ .str_owned = ip_str };
            }
            // Already a string IP (e.g. from CSV round-trip) — pass through.
            return Value{ .str = s };
        },
        // ── Scalar passthrough ─────────────────────────────────────
        .scalar_passthru => return evalTextExpr(inner, row) orelse Value{ .null_val = {} },
        .type_name => {
            // toTypeName(x) → CH type name as string
            // Infer CH type from expression AST first, fall back to value tag.
            const tn = inferChTypeName(inner, row);
            return Value{ .str = tn };
        },
        .int_div_or_zero => {
            const args = splitTopLevelArgs(inner) catch return Value{ .i64 = 0 };
            if (args.len != 2) return Value{ .i64 = 0 };
            const a = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toF64() orelse return Value{ .i64 = 0 };
            const b = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 }).toF64() orelse return Value{ .i64 = 0 };
            if (b == 0.0) return Value{ .i64 = 0 };
            return Value{ .i64 = @intFromFloat(@trunc(a / b)) };
        },
        .append_trailing_char => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len != 2) return null;
            const sv = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            const cv = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            if (cv.len == 0) return Value{ .str = sv };
            const c = cv[0];
            // Empty string: return as-is (ClickHouse: don't append to empty strings)
            if (sv.len == 0) return Value{ .str = sv };
            if (sv[sv.len - 1] == c) return Value{ .str = sv };
            const result = std.heap.page_allocator.alloc(u8, sv.len + 1) catch return null;
            @memcpy(result[0..sv.len], sv);
            result[sv.len] = c;
            return Value{ .str_owned = result };
        },
        .fixed_string => {
            // toFixedString(s, n): pad/truncate s to exactly n bytes with \x00 padding
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 1) return null;
            const sv = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toStr() orelse return null;
            if (args.len < 2) return Value{ .str = sv };
            const n_val = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = sv };
            const n: usize = @intCast(@max(0, n_val.toI64() orelse return Value{ .str = sv }));
            if (sv.len == n) return Value{ .str = sv };
            const result = std.heap.page_allocator.alloc(u8, n) catch return null;
            const copy_len = @min(sv.len, n);
            @memcpy(result[0..copy_len], sv[0..copy_len]);
            if (n > sv.len) @memset(result[sv.len..], 0);
            return Value{ .str_owned = result };
        },
        // Math functions
        .math_sqrt, .math_cbrt, .math_trunc, .math_exp, .math_exp2, .math_exp10,
        .math_log, .math_log2, .math_log10,
        .math_sin, .math_cos, .math_tan,
        .math_asin, .math_acos, .math_atan,
        .math_lgamma, .math_tgamma, .math_erf, .math_erfc => {
            const x = (evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return null).toF64() orelse return null;
            const result: f64 = switch (kind) {
                .math_sqrt   => @sqrt(x),
                .math_cbrt   => std.math.cbrt(x),
                .math_trunc  => @trunc(x),
                .math_exp    => @exp(x),
                .math_exp2   => @exp2(x),
                .math_exp10  => std.math.pow(f64, 10.0, x),
                .math_log    => @log(x),
                .math_log2   => @log2(x),
                .math_log10  => @log10(x),
                .math_sin    => @sin(x),
                .math_cos    => @cos(x),
                .math_tan    => @tan(x),
                .math_asin   => std.math.asin(x),
                .math_acos   => std.math.acos(x),
                .math_atan   => std.math.atan(x),
                .math_lgamma => blk: {
                    // lgamma is undefined (NaN) at negative integers; at 0 it's +inf.
                    if (x < 0 and x == @floor(x)) break :blk std.math.nan(f64);
                    if (x == 0) break :blk std.math.inf(f64);
                    break :blk std.math.lgamma(f64, x);
                },
                .math_tgamma => std.math.gamma(f64, x),
                .math_erf    => erf_c(x),
                .math_erfc   => erfc_c(x),
                else         => unreachable,
            };
            return Value{ .f64 = result };
        },
        .math_pow, .math_atan2 => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len != 2) return null;
            const a = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toF64() orelse return null;
            const b = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toF64() orelse return null;
            const result: f64 = if (kind == .math_pow) std.math.pow(f64, a, b) else std.math.atan2(a, b);
            return Value{ .f64 = result };
        },
        .math_is_nan => {
            const x = (evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return null).toF64() orelse return null;
            return Value{ .i64 = if (std.math.isNan(x)) 1 else 0 };
        },
        .math_is_inf => {
            const x = (evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return null).toF64() orelse return null;
            return Value{ .i64 = if (std.math.isInf(x)) 1 else 0 };
        },
        .math_is_finite => {
            const x = (evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return null).toF64() orelse return null;
            return Value{ .i64 = if (std.math.isFinite(x)) 1 else 0 };
        },
        .math_pi => return Value{ .f64 = std.math.pi },
        .math_e  => return Value{ .f64 = std.math.e  },
        .math_sinh, .math_cosh, .math_tanh, .math_asinh, .math_acosh, .math_atanh, .math_log1p => {
            const x = (evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return null).toF64() orelse return null;
            const result: f64 = switch (kind) {
                .math_sinh  => std.math.sinh(x),
                .math_cosh  => std.math.cosh(x),
                .math_tanh  => std.math.tanh(x),
                .math_asinh => std.math.asinh(x),
                .math_acosh => std.math.acosh(x),
                .math_atanh => std.math.atanh(x),
                .math_log1p => std.math.log1p(x),
                else        => unreachable,
            };
            return Value{ .f64 = result };
        },
        .math_hypot => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len != 2) return null;
            const a = (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toF64() orelse return null;
            const b = (evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return null).toF64() orelse return null;
            return Value{ .f64 = std.math.hypot(a, b) };
        },
        .fn_range => {
            // range(n): [0, 1, ..., n-1]  or  range(start, end): [start, ..., end-1]
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len == 0) return Value{ .array = &.{} };
            const start: i64 = if (args.len >= 2)
                (evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return null).toI64() orelse return null
            else 0;
            const end_idx: usize = if (args.len >= 2) 1 else 0;
            const n_val = evalTextExpr(std.mem.trim(u8, args.items[end_idx], " \t\r\n"), row) orelse return null;
            const end_val = n_val.toI64() orelse return null;
            const count: usize = @intCast(@max(0, end_val - start));
            const elems = std.heap.page_allocator.alloc(Value, count) catch return null;
            for (elems, 0..) |*e, i| e.* = Value{ .i64 = start + @as(i64, @intCast(i)) };
            return Value{ .array = elems };
        },

        // ── JSON extraction ────────────────────────────────────────
        .json_extract_str, .json_extract_int, .json_extract_float,
        .json_extract_raw, .json_extract_bool => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return Value{ .str = "" };
            const json_val = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const json_str = json_val.toStr() orelse return Value{ .str = "" };
            // Collect key path (args[1..])
            for (args.items[1..]) |arg| {
                const key_raw = std.mem.trim(u8, arg, " \t\r\n");
                // Strip surrounding quotes if present
                const key = stripQuotes(key_raw);
                // Find "key": in json
                const extracted = jsonExtract(json_str, key) orelse return Value{ .str = "" };
                return switch (kind) {
                    .json_extract_int   => Value{ .i64 = std.fmt.parseInt(i64, extracted, 10) catch 0 },
                    .json_extract_float => Value{ .f64 = std.fmt.parseFloat(f64, extracted) catch 0.0 },
                    .json_extract_bool  => Value{ .i64 = if (std.mem.eql(u8, extracted, "true")) 1 else 0 },
                    else                => Value{ .str = extracted },
                };
            }
            return Value{ .str = "" };
        },

        // ── Regex ─────────────────────────────────────────────────
        .fn_match => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return Value{ .i64 = 0 };
            const haystack_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const haystack = haystack_v.toStr() orelse return Value{ .i64 = 0 };
            const pat_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const pat = pat_v.toStr() orelse return Value{ .i64 = 0 };
            const matched = regexpMatch(haystack, pat);
            return Value{ .i64 = if (matched) 1 else 0 };
        },
        .fn_extract => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return Value{ .str = "" };
            const haystack_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const haystack = haystack_v.toStr() orelse return Value{ .str = "" };
            const pat_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = "" };
            const pat = pat_v.toStr() orelse return Value{ .str = "" };
            const result = regexpExtract(haystack, pat) orelse return Value{ .str = "" };
            return Value{ .str = result };
        },
        .fn_replace_regexp => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 3) return Value{ .str = inner };
            const s_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            const pat_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = s };
            const pat = pat_v.toStr() orelse return Value{ .str = s };
            const repl_v = evalTextExpr(std.mem.trim(u8, args.items[2], " \t\r\n"), row) orelse return Value{ .str = s };
            const repl = repl_v.toStr() orelse return Value{ .str = s };
            const result = regexpReplaceAll(s, pat, repl) orelse return Value{ .str = s };
            return Value{ .str = result };
        },
        .fn_replace_one => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 3) return Value{ .str = "" };
            const s_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            const from_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = s };
            const from = from_v.toStr() orelse return Value{ .str = s };
            const to_v = evalTextExpr(std.mem.trim(u8, args.items[2], " \t\r\n"), row) orelse return Value{ .str = s };
            const to = to_v.toStr() orelse return Value{ .str = s };
            if (std.mem.indexOf(u8, s, from)) |pos| {
                const result = std.heap.page_allocator.alloc(u8, s.len - from.len + to.len) catch return Value{ .str = s };
                @memcpy(result[0..pos], s[0..pos]);
                @memcpy(result[pos..pos + to.len], to);
                @memcpy(result[pos + to.len..], s[pos + from.len..]);
                return Value{ .str = result };
            }
            return Value{ .str = s };
        },
        .fn_replace_all => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 3) return Value{ .str = "" };
            const s_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            const from_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = s };
            const from = from_v.toStr() orelse return Value{ .str = s };
            const to_v = evalTextExpr(std.mem.trim(u8, args.items[2], " \t\r\n"), row) orelse return Value{ .str = s };
            const to = to_v.toStr() orelse return Value{ .str = s };
            if (from.len == 0) return Value{ .str = s };
            // Count occurrences
            var count: usize = 0;
            var i: usize = 0;
            while (i + from.len <= s.len) {
                if (std.mem.eql(u8, s[i..i + from.len], from)) { count += 1; i += from.len; }
                else i += 1;
            }
            if (count == 0) return Value{ .str = s };
            const actual_len = s.len - count * from.len + count * to.len;
            const result = std.heap.page_allocator.alloc(u8, actual_len) catch return Value{ .str = s };
            var ri: usize = 0;
            i = 0;
            while (i < s.len) {
                if (i + from.len <= s.len and std.mem.eql(u8, s[i..i + from.len], from)) {
                    @memcpy(result[ri..ri + to.len], to);
                    ri += to.len;
                    i += from.len;
                } else {
                    result[ri] = s[i];
                    ri += 1;
                    i += 1;
                }
            }
            return Value{ .str = result[0..ri] };
        },

        // ── formatDateTime ────────────────────────────────────────
        .fn_format_datetime => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return Value{ .str = "" };
            const ts_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .str = "" };
            const ts = ts_v.toI64() orelse return Value{ .str = "" };
            const fmt_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .str = "" };
            const fmt = fmt_v.toStr() orelse return Value{ .str = "" };
            // tz arg (args[2]) ignored — always UTC for now
            const result = formatDateTimeStr(ts, fmt) orelse return Value{ .str = "" };
            return Value{ .str = result };
        },

        // ── String trim ──────────────────────────────────────────
        .fn_trim => {
            const s_v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            return Value{ .str = std.mem.trim(u8, s, " \t\r\n") };
        },
        .fn_trim_left => {
            const s_v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            return Value{ .str = std.mem.trimStart(u8, s, " \t\r\n") };
        },
        .fn_trim_right => {
            const s_v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .str = "" };
            const s = s_v.toStr() orelse return Value{ .str = "" };
            return Value{ .str = std.mem.trimEnd(u8, s, " \t\r\n") };
        },

        // ── Type casts ────────────────────────────────────────────
        .fn_to_int32 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            return Value{ .i64 = v.toI64() orelse 0 };
        },
        .fn_to_int64 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            return Value{ .i64 = v.toI64() orelse 0 };
        },
        .fn_to_uint32 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const iv = v.toI64() orelse 0;
            return Value{ .i64 = @as(i64, @intCast(@as(u32, @truncate(@as(u64, @bitCast(iv)))))) };
        },
        .fn_to_uint64 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            return Value{ .i64 = v.toI64() orelse 0 };
        },
        .fn_to_float32 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .f64 = 0.0 };
            return Value{ .f64 = v.toF64() orelse 0.0 };
        },
        .fn_to_float64 => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row) orelse return Value{ .f64 = 0.0 };
            return Value{ .f64 = v.toF64() orelse 0.0 };
        },

        // ── Null handling ─────────────────────────────────────────
        .fn_if_null => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return null;
            const v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row);
            if (v != null and v.? != .null_val) return v;
            return evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row);
        },
        .fn_coalesce => {
            const args = splitTopLevelArgs(inner) catch return null;
            for (args.items) |arg| {
                const v = evalTextExpr(std.mem.trim(u8, arg, " \t\r\n"), row);
                if (v != null and v.? != .null_val) return v;
            }
            return Value.null_val;
        },
        .fn_to_nullable => {
            return evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row);
        },
        .fn_nullable_or_default => {
            const v = evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row);
            if (v != null and v.? != .null_val) return v;
            return Value{ .i64 = 0 };
        },

        // ── endsWith ─────────────────────────────────────────────
        .fn_ends_with => {
            const args = splitTopLevelArgs(inner) catch return null;
            if (args.len < 2) return Value{ .i64 = 0 };
            const s_v = evalTextExpr(std.mem.trim(u8, args.items[0], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const s = s_v.toStr() orelse return Value{ .i64 = 0 };
            const suf_v = evalTextExpr(std.mem.trim(u8, args.items[1], " \t\r\n"), row) orelse return Value{ .i64 = 0 };
            const suf = suf_v.toStr() orelse return Value{ .i64 = 0 };
            return Value{ .i64 = if (std.mem.endsWith(u8, s, suf)) 1 else 0 };
        },

        .fn_to_string_base => {
            return evalTextExpr(std.mem.trim(u8, inner, " \t\r\n"), row);
        },
    }
}

/// Strip surrounding single or double quotes from a string literal.
fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2) {
        if ((s[0] == '\'' and s[s.len - 1] == '\'') or
            (s[0] == '"'  and s[s.len - 1] == '"'))
            return s[1..s.len - 1];
    }
    return s;
}

/// Simple JSON key extractor: finds "key": <value> and returns the raw value
/// (string without quotes, number, bool literal).
/// Handles nested objects by returning their raw text.
fn jsonExtract(json: []const u8, key: []const u8) ?[]const u8 {
    // Build search token: "key":
    var needle_buf: [256]u8 = undefined;
    if (key.len + 4 > needle_buf.len) return null;
    needle_buf[0] = '"';
    @memcpy(needle_buf[1..1 + key.len], key);
    needle_buf[1 + key.len] = '"';
    needle_buf[2 + key.len] = ':';
    const needle = needle_buf[0..3 + key.len];
    var i: usize = 0;
    while (i + needle.len <= json.len) : (i += 1) {
        if (!std.mem.eql(u8, json[i..i + needle.len], needle)) continue;
        var pos = i + needle.len;
        // skip whitespace
        while (pos < json.len and (json[pos] == ' ' or json[pos] == '\t' or json[pos] == '\n')) pos += 1;
        if (pos >= json.len) return null;
        if (json[pos] == '"') {
            // string value
            pos += 1;
            const start = pos;
            while (pos < json.len) {
                if (json[pos] == '\\') { pos += 2; continue; }
                if (json[pos] == '"') break;
                pos += 1;
            }
            return json[start..pos];
        } else if (json[pos] == '{' or json[pos] == '[') {
            // nested — return raw text
            const start = pos;
            var depth: usize = 1;
            pos += 1;
            while (pos < json.len and depth > 0) {
                if (json[pos] == '{' or json[pos] == '[') depth += 1
                else if (json[pos] == '}' or json[pos] == ']') depth -= 1
                else if (json[pos] == '"') {
                    pos += 1;
                    while (pos < json.len) {
                        if (json[pos] == '\\') { pos += 2; continue; }
                        if (json[pos] == '"') break;
                        pos += 1;
                    }
                }
                pos += 1;
            }
            return json[start..pos];
        } else {
            // number, bool, null
            const start = pos;
            while (pos < json.len and json[pos] != ',' and json[pos] != '}' and json[pos] != ']' and json[pos] != ' ') pos += 1;
            return json[start..pos];
        }
    }
    return null;
}

/// Minimal regex match using simple pattern:
/// Supports: . * + ? ^ $ [] [^] literal chars, \d \w \s
/// Falls back to std.mem.containsAtLeast for unsupported patterns.
fn regexpMatch(haystack: []const u8, pattern: []const u8) bool {
    // Use simple anchoring check
    const pat = pattern;
    if (pat.len == 0) return true;
    // Try each position
    var start: usize = 0;
    const anchored_start = pat.len > 0 and pat[0] == '^';
    const search_start: usize = if (anchored_start) 1 else 0;
    if (anchored_start) {
        return regexpMatchAt(haystack, 0, pat[search_start..]);
    }
    while (start <= haystack.len) : (start += 1) {
        if (regexpMatchAt(haystack, start, pat)) return true;
    }
    return false;
}

/// Try to match pattern starting at haystack[pos]
fn regexpMatchAt(haystack: []const u8, start: usize, pattern: []const u8) bool {
    var hi = start;
    var pi: usize = 0;
    while (pi < pattern.len) {
        const anchored_end = pi == pattern.len - 1 and pattern[pi] == '$';
        if (anchored_end) return hi == haystack.len;

        // get current pattern char
        const pc = pattern[pi];

        // check for quantifier after current atom
        const has_star  = pi + 1 < pattern.len and pattern[pi + 1] == '*';
        const has_plus  = pi + 1 < pattern.len and pattern[pi + 1] == '+';
        const has_opt   = pi + 1 < pattern.len and pattern[pi + 1] == '?';
        const quantified = has_star or has_plus or has_opt;

        if (pc == '.') {
            if (quantified) {
                const min_count: usize = if (has_plus) 1 else 0;
                const max_advance = haystack.len - hi;
                // greedy: try from max down
                var k: usize = max_advance;
                while (true) {
                    if (k >= min_count) {
                        if (regexpMatchAt(haystack, hi + k, pattern[pi + 2..])) return true;
                    }
                    if (k == 0) break;
                    k -= 1;
                }
                return false;
            }
            if (hi >= haystack.len) return false;
            hi += 1;
            pi += 1;
        } else if (pc == '[') {
            // find closing ]
            var end_bracket = pi + 1;
            if (end_bracket < pattern.len and pattern[end_bracket] == '^') end_bracket += 1;
            while (end_bracket < pattern.len and pattern[end_bracket] != ']') end_bracket += 1;
            const char_class = pattern[pi..end_bracket + 1];
            const quant_idx = end_bracket + 1;
            const qc = if (quant_idx < pattern.len) pattern[quant_idx] else 0;
            const q_star = qc == '*'; const q_plus = qc == '+';
            if (q_star or q_plus) {
                const min_c: usize = if (q_plus) 1 else 0;
                var k: usize = 0;
                while (hi + k <= haystack.len and (k == 0 or matchCharClass(haystack[hi + k - 1], char_class))) : (k += 1) {}
                var back = k;
                while (true) {
                    if (back >= min_c) {
                        if (regexpMatchAt(haystack, hi + back, pattern[quant_idx + 1..])) return true;
                    }
                    if (back == 0) break;
                    back -= 1;
                }
                return false;
            }
            if (hi >= haystack.len) return false;
            if (!matchCharClass(haystack[hi], char_class)) return false;
            hi += 1;
            pi = end_bracket + 1;
        } else {
            // literal or escape
            const literal: u8 = if (pc == '\\' and pi + 1 < pattern.len) blk: {
                pi += 1;
                break :blk pattern[pi];
            } else pc;
            if (quantified) {
                const min_count: usize = if (has_plus) 1 else if (has_opt) 0 else 0;
                const max_count: usize = if (has_opt) 1 else haystack.len - hi + 1;
                var k: usize = 0;
                while (k < max_count and hi + k < haystack.len and haystack[hi + k] == literal) : (k += 1) {}
                var back = k;
                while (true) {
                    if (back >= min_count) {
                        if (regexpMatchAt(haystack, hi + back, pattern[pi + 2..])) return true;
                    }
                    if (back == 0) break;
                    back -= 1;
                }
                return false;
            }
            if (hi >= haystack.len or haystack[hi] != literal) return false;
            hi += 1;
            pi += 1;
        }
    }
    return true;
}

fn matchCharClass(c: u8, class: []const u8) bool {
    if (class.len < 2) return false;
    var i: usize = 1;
    const negate = class[1] == '^';
    if (negate) i = 2;
    var matched = false;
    while (i < class.len and class[i] != ']') {
        if (i + 2 < class.len and class[i + 1] == '-' and class[i + 2] != ']') {
            if (c >= class[i] and c <= class[i + 2]) matched = true;
            i += 3;
        } else {
            if (c == class[i]) matched = true;
            i += 1;
        }
    }
    return if (negate) !matched else matched;
}

/// Extract first match of pattern from haystack (returns full match if no capture groups).
fn regexpExtract(haystack: []const u8, pattern: []const u8) ?[]const u8 {
    var start: usize = 0;
    while (start <= haystack.len) : (start += 1) {
        if (regexpMatchAt(haystack, start, pattern)) {
            // find end of match
            var end_pos = start + 1;
            while (end_pos <= haystack.len) : (end_pos += 1) {
                if (!regexpMatchAt(haystack, start, pattern)) break;
                // check if longer match possible
                if (end_pos >= haystack.len) break;
            }
            return haystack[start..end_pos];
        }
    }
    return null;
}

/// Replace all occurrences of pattern with replacement in haystack.
fn regexpReplaceAll(haystack: []const u8, pattern: []const u8, replacement: []const u8) ?[]const u8 {
    var result: std.ArrayListUnmanaged(u8) = .empty;
    var i: usize = 0;
    while (i <= haystack.len) {
        if (i < haystack.len and regexpMatch(haystack[i..], pattern)) {
            // find match length
            var mlen: usize = 1;
            while (mlen <= haystack.len - i) : (mlen += 1) {
                if (!regexpMatchAt(haystack, i, pattern)) break;
            }
            result.appendSlice(std.heap.page_allocator, replacement) catch return null;
            i += mlen;
        } else {
            if (i < haystack.len) result.append(std.heap.page_allocator, haystack[i]) catch return null;
            i += 1;
        }
    }
    return result.toOwnedSlice(std.heap.page_allocator) catch null;
}

/// Format a unix timestamp using ClickHouse-style format string (%Y %m %d %H %i %S etc.)
fn formatDateTimeStr(ts: i64, fmt: []const u8) ?[]const u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @intCast(@max(0, ts)) };
    const epoch_day = epoch_seconds.getEpochDay();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();

    var buf: [64]u8 = undefined;
    var out: [128]u8 = undefined;
    var out_len: usize = 0;

    var i: usize = 0;
    while (i < fmt.len) {
        if (fmt[i] == '%' and i + 1 < fmt.len) {
            i += 1;
            const written: []const u8 = switch (fmt[i]) {
                'Y' => std.fmt.bufPrint(&buf, "{d:0>4}", .{year_day.year}) catch return null,
                'y' => std.fmt.bufPrint(&buf, "{d:0>2}", .{year_day.year % 100}) catch return null,
                'm' => std.fmt.bufPrint(&buf, "{d:0>2}", .{@intFromEnum(month_day.month)}) catch return null,
                'd' => std.fmt.bufPrint(&buf, "{d:0>2}", .{month_day.day_index + 1}) catch return null,
                'H' => std.fmt.bufPrint(&buf, "{d:0>2}", .{day_seconds.getHoursIntoDay()}) catch return null,
                'i' => std.fmt.bufPrint(&buf, "{d:0>2}", .{day_seconds.getMinutesIntoHour()}) catch return null,
                'S' => std.fmt.bufPrint(&buf, "{d:0>2}", .{day_seconds.getSecondsIntoMinute()}) catch return null,
                'M' => blk: {
                    const month_names = [_][]const u8{ "January","February","March","April","May","June",
                        "July","August","September","October","November","December" };
                    const mi = @intFromEnum(month_day.month) - 1;
                    break :blk if (mi < 12) month_names[mi] else "Unknown";
                },
                'e' => std.fmt.bufPrint(&buf, "{d}", .{month_day.day_index + 1}) catch return null,
                'j' => std.fmt.bufPrint(&buf, "{d:0>3}", .{year_day.day + 1}) catch return null,
                'n' => std.fmt.bufPrint(&buf, "{d}", .{@intFromEnum(month_day.month)}) catch return null,
                '%' => "%",
                else => blk: {
                    buf[0] = '%'; buf[1] = fmt[i];
                    break :blk buf[0..2];
                },
            };
            if (out_len + written.len > out.len) return null;
            @memcpy(out[out_len..out_len + written.len], written);
            out_len += written.len;
        } else {
            if (out_len >= out.len) return null;
            out[out_len] = fmt[i];
            out_len += 1;
        }
        i += 1;
    }
    const heap = std.heap.page_allocator.alloc(u8, out_len) catch return null;
    @memcpy(heap, out[0..out_len]);
    return heap;
}

/// Evaluate CASE WHEN c1 THEN v1 … [ELSE vN] END
fn evalCaseWhen(trimmed: []const u8, row: *const RowCtx) ?Value {
    // Simple token scanner: find WHEN/THEN/ELSE/END at depth 0
    var pos: usize = 5; // skip "CASE "
    while (pos < trimmed.len) {
        // skip whitespace
        while (pos < trimmed.len and (trimmed[pos] == ' ' or trimmed[pos] == '\t')) pos += 1;
        if (pos >= trimmed.len) break;
        // check keyword
        if (std.ascii.startsWithIgnoreCase(trimmed[pos..], "WHEN ")) {
            pos += 5;
            // Extract WHEN expression up to " THEN "
            const when_start = pos;
            var depth: usize = 0;
            while (pos < trimmed.len) {
                if (trimmed[pos] == '(') depth += 1
                else if (trimmed[pos] == ')') { if (depth > 0) depth -= 1; }
                else if (trimmed[pos] == '\'') {
                    pos += 1;
                    while (pos < trimmed.len and trimmed[pos] != '\'') pos += 1;
                } else if (depth == 0 and std.ascii.startsWithIgnoreCase(trimmed[pos..], " THEN ")) break;
                if (pos < trimmed.len) pos += 1;
            }
            const when_expr = std.mem.trim(u8, trimmed[when_start..pos], " \t");
            pos += 6; // skip " THEN "
            // Extract THEN expression up to next " WHEN " / " ELSE " / " END"
            const then_start = pos;
            depth = 0;
            while (pos < trimmed.len) {
                if (trimmed[pos] == '(') depth += 1
                else if (trimmed[pos] == ')') { if (depth > 0) depth -= 1; }
                else if (trimmed[pos] == '\'') {
                    pos += 1;
                    while (pos < trimmed.len and trimmed[pos] != '\'') pos += 1;
                } else if (depth == 0 and (
                    std.ascii.startsWithIgnoreCase(trimmed[pos..], " WHEN ") or
                    std.ascii.startsWithIgnoreCase(trimmed[pos..], " ELSE ") or
                    std.ascii.startsWithIgnoreCase(trimmed[pos..], " END"))) break;
                if (pos < trimmed.len) pos += 1;
            }
            const then_expr = std.mem.trim(u8, trimmed[then_start..pos], " \t");
            if (evalTextBoolExpr(when_expr, row))
                return evalTextExpr(then_expr, row);
            // Save first THEN expr text for type coercion of the ELSE branch.
            const first_then_expr = if (pos == trimmed.len) then_expr else then_expr;
            _ = first_then_expr;
            // else continue to next WHEN/ELSE
        } else if (std.ascii.startsWithIgnoreCase(trimmed[pos..], "ELSE ")) {
            const else_expr = std.mem.trim(u8, trimmed[pos + 5 ..], " \t");
            // strip trailing END
            const end_pos = std.ascii.indexOfIgnoreCase(else_expr, " END") orelse else_expr.len;
            const else_val = evalTextExpr(else_expr[0..end_pos], row);
            // If ELSE is an integer literal but any THEN branch looks like a float aggregate
            // (e.g. avg(...)), coerce to Float64 so Go driver sees consistent type.
            if (else_val) |v| switch (v) {
                .i64 => |iv| {
                    // Scan back through the CASE expression for "avg(" or "sum(" or similar
                    // float-producing aggregate to determine the expected return type.
                    if (std.mem.indexOf(u8, trimmed, "avg(") != null or
                        std.mem.indexOf(u8, trimmed, "sum(") != null or
                        std.mem.indexOf(u8, trimmed, "AVG(") != null or
                        std.mem.indexOf(u8, trimmed, "SUM(") != null)
                    {
                        return Value{ .f64 = @floatFromInt(iv) };
                    }
                    return v;
                },
                else => return v,
            };
            return else_val;
        } else if (std.ascii.startsWithIgnoreCase(trimmed[pos..], "END")) {
            break;
        } else {
            pos += 1;
        }
    }
    return Value{ .null_val = {} };
}

/// Evaluate a text-encoded boolean condition (for if() conditions and WHERE).
fn evalTextBoolExpr(expr: []const u8, row: *const RowCtx) bool {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");

    // IS NOT NULL / IS NULL
    if (std.ascii.indexOfIgnoreCase(trimmed, " IS NOT NULL")) |pos| {
        const col = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const v = row.get(col) orelse return false; // missing → null
        return v != .null_val;
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " IS NULL")) |pos| {
        const col = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const v = row.get(col) orelse return true;
        return v == .null_val;
    }

    // AND: split on top-level AND
    if (findTopLevelKeyword(trimmed, "AND")) |pos| {
        const left = evalTextBoolExpr(trimmed[0..pos], row);
        if (!left) return false;
        return evalTextBoolExpr(trimmed[pos + 3 ..], row);
    }
    // OR: split on top-level OR
    if (findTopLevelKeyword(trimmed, "OR")) |pos| {
        const left = evalTextBoolExpr(trimmed[0..pos], row);
        if (left) return true;
        return evalTextBoolExpr(trimmed[pos + 2 ..], row);
    }

    // isIPv4String / isIPv6String / dictHas / arrayExists:
    // These are evaluated by evalTextExpr (which returns 0 or 1), then truthy-checked below.

    // NOT IN / IN set membership
    if (std.ascii.indexOfIgnoreCase(trimmed, " NOT IN (")) |pos| {
        const lhs_raw = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const list_start = pos + 9; // len(" NOT IN (")
        const list_end = std.mem.lastIndexOfScalar(u8, trimmed, ')') orelse trimmed.len;
        const lv = evalTextExpr(lhs_raw, row) orelse Value{ .null_val = {} };
        var it = std.mem.splitScalar(u8, trimmed[list_start..list_end], ',');
        while (it.next()) |item| {
            const rv = evalTextExpr(std.mem.trim(u8, item, " \t\r\n"), row) orelse continue;
            if (Value.order(lv, rv) == .eq) return false;
        }
        return true;
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " IN (")) |pos| {
        const lhs_raw = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const list_start = pos + 5; // len(" IN (")
        const list_end = std.mem.lastIndexOfScalar(u8, trimmed, ')') orelse trimmed.len;
        const lv = evalTextExpr(lhs_raw, row) orelse Value{ .null_val = {} };
        var it = std.mem.splitScalar(u8, trimmed[list_start..list_end], ',');
        while (it.next()) |item| {
            const rv = evalTextExpr(std.mem.trim(u8, item, " \t\r\n"), row) orelse continue;
            if (Value.order(lv, rv) == .eq) return true;
        }
        return false;
    }

    // LIKE / NOT LIKE / ILIKE
    if (std.ascii.indexOfIgnoreCase(trimmed, " NOT LIKE ")) |pos| {
        const lhs = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const rhs = std.mem.trim(u8, trimmed[pos + 10 ..], " \t\r\n");
        const lv = (evalTextExpr(lhs, row) orelse return false).toStr() orelse return false;
        const pattern_raw = std.mem.trim(u8, rhs, "'");
        return !likeMatch(lv, pattern_raw, false);
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " ILIKE ")) |pos| {
        const lhs = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const rhs = std.mem.trim(u8, trimmed[pos + 7 ..], " \t\r\n");
        const lv = (evalTextExpr(lhs, row) orelse return false).toStr() orelse return false;
        const pattern_raw = std.mem.trim(u8, rhs, "'");
        return likeMatch(lv, pattern_raw, true);
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " LIKE ")) |pos| {
        const lhs = std.mem.trim(u8, trimmed[0..pos], " \t\r\n");
        const rhs = std.mem.trim(u8, trimmed[pos + 6 ..], " \t\r\n");
        const lv = (evalTextExpr(lhs, row) orelse return false).toStr() orelse return false;
        const pattern_raw = std.mem.trim(u8, rhs, "'");
        return likeMatch(lv, pattern_raw, false);
    }

    // col op val comparisons — depth-aware scan to avoid matching inside parens or lambdas (->)
    const ops = [_]struct { text: []const u8, op: generic_sql.CmpOp }{
        .{ .text = "<>", .op = .ne },
        .{ .text = ">=", .op = .ge },
        .{ .text = "<=", .op = .le },
        .{ .text = "!=", .op = .ne },
        .{ .text = "=",  .op = .eq },
        .{ .text = ">",  .op = .gt },
        .{ .text = "<",  .op = .lt },
    };
    // Find leftmost top-level comparison operator (depth-aware).
    // At each position, prefer longer operators (priority order in ops ensures this).
    var best_pos: usize = std.math.maxInt(usize);
    var best_op_idx: usize = ops.len;
    {
        var depth: usize = 0;
        for (trimmed, 0..) |ch, ci| {
            if (ch == '(' or ch == '[') { depth += 1; continue; }
            if (ch == ')' or ch == ']') { if (depth > 0) depth -= 1; continue; }
            if (depth != 0) continue;
            // Try each op in priority order; at a given position, take the first
            // (highest priority = longest) match.  Only update best if ci < best_pos.
            if (ci >= best_pos) continue;
            for (ops, 0..) |candidate, oi| {
                if (ci + candidate.text.len > trimmed.len) continue;
                if (!std.mem.eql(u8, trimmed[ci..ci + candidate.text.len], candidate.text)) continue;
                // Skip if this '>' is part of "->" lambda arrow
                if (candidate.text[0] == '>' and ci > 0 and trimmed[ci - 1] == '-') continue;
                best_pos = ci;
                best_op_idx = oi;
                break; // highest-priority match at this position
            }
        }
    }
    if (best_op_idx < ops.len) {
        const candidate = ops[best_op_idx];
        const lhs_raw = std.mem.trim(u8, trimmed[0..best_pos], " \t\r\n");
        const rhs_raw = std.mem.trim(u8, trimmed[best_pos + candidate.text.len ..], " \t\r\n");
        const lv = evalTextExpr(lhs_raw, row) orelse Value{ .null_val = {} };
        const rv = evalTextExpr(rhs_raw, row) orelse Value{ .null_val = {} };
        const ord = Value.order(lv, rv);
        return switch (candidate.op) {
            .eq => ord == .eq,
            .ne => ord != .eq,
            .lt => ord == .lt,
            .le => ord != .gt,
            .gt => ord == .gt,
            .ge => ord != .lt,
        };
    }

    // Truthy check: non-empty string or non-zero number
    if (evalTextExpr(trimmed, row)) |v| {
        return switch (v) {
            .i64       => |i| i != 0,
            .f64       => |f| f != 0,
            .date      => |d| d != 0,
            .uint8     => |u| u != 0,
            .str, .str_owned => v.toStr().?.len > 0,
            .array     => |a| a.len > 0,
            .null_val  => false,
        };
    }
    return false;
}

/// Find a top-level (depth=0) keyword in an expression.
/// Returns the position of the keyword or null.
fn findTopLevelKeyword(expr: []const u8, kw: []const u8) ?usize {
    var depth: usize = 0;
    var i: usize = 0;
    while (i + kw.len <= expr.len) : (i += 1) {
        switch (expr[i]) {
            '(' => { depth += 1; continue; },
            ')' => { if (depth > 0) depth -= 1; continue; },
            '\'' => {
                // Skip string literal
                i += 1;
                while (i < expr.len and expr[i] != '\'') i += 1;
                continue;
            },
            else => {},
        }
        if (depth != 0) continue;
        if (!std.ascii.eqlIgnoreCase(expr[i .. i + kw.len], kw)) continue;
        const before_ok = i == 0 or !std.ascii.isAlphanumeric(expr[i - 1]);
        const after_pos = i + kw.len;
        const after_ok = after_pos >= expr.len or !std.ascii.isAlphanumeric(expr[after_pos]);
        if (before_ok and after_ok) return i;
    }
    return null;
}

/// Split a string by top-level (depth=0) commas.
/// Returns a fixed-size buffer of slices (max 8 args).
const SplitResult = struct {
    items: [8][]const u8 = undefined,
    len: usize = 0,

    fn deinit(_: *SplitResult) void {}
};

fn splitTopLevelArgs(expr: []const u8) !SplitResult {
    var result: SplitResult = .{};
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= expr.len) : (i += 1) {
        const c = if (i < expr.len) expr[i] else ',';
        switch (c) {
            '(', '[' => depth += 1,
            ')', ']' => { if (depth > 0) depth -= 1; },
            '\'' => {
                i += 1;
                while (i < expr.len and expr[i] != '\'') i += 1;
            },
            ',' => if (depth == 0) {
                if (result.len < result.items.len) {
                    result.items[result.len] = expr[start..i];
                    result.len += 1;
                }
                start = i + 1;
            },
            else => {},
        }
    }
    return result;
}

/// Check if a string looks like an IPv4 address (e.g. "1.2.3.4").
fn isIPv4String(s: []const u8) bool {
    var parts: u8 = 0;
    var start: usize = 0;
    for (s, 0..) |c, i| {
        if (c == '.') {
            const part = s[start..i];
            const n = std.fmt.parseInt(u16, part, 10) catch return false;
            if (n > 255) return false;
            parts += 1;
            start = i + 1;
        }
    }
    if (parts != 3) return false;
    const last = std.fmt.parseInt(u16, s[start..], 10) catch return false;
    return last <= 255;
}

/// Check if a string looks like an IPv6 address (contains ':').
fn isIPv6String(s: []const u8) bool {
    if (s.len < 2) return false;
    var colon_count: u8 = 0;
    for (s) |c| {
        if (c == ':') colon_count += 1;
    }
    return colon_count >= 2;
}

/// If expr is of the form "length(<col>)" (case-insensitive), returns the inner col name.
fn parseLengthCall(expr: []const u8) ?[]const u8 {
    const prefix = "length(";
    if (!std.ascii.startsWithIgnoreCase(expr, prefix)) return null;
    if (expr[expr.len - 1] != ')') return null;
    return expr[prefix.len .. expr.len - 1];
}

// ── Helper: write expression header label ────────────────────────────────────

fn writeExprHeader(out: *std.ArrayList(u8), allocator: std.mem.Allocator, proj: generic_sql.Expr) !void {
    // Helper to write a column name, quoting it if it contains commas or quotes.
    const writeColName = struct {
        fn write(o: *std.ArrayList(u8), a: std.mem.Allocator, name: []const u8) !void {
            if (std.mem.indexOfAny(u8, name, ",\"\n\r") != null) {
                try o.append(a, '"');
                for (name) |c| {
                    if (c == '"') try o.append(a, '"');
                    try o.append(a, c);
                }
                try o.append(a, '"');
            } else {
                try o.appendSlice(a, name);
            }
        }
    }.write;
    // Determine type prefix for native-block encoding hint, based on the raw expression.
    // \x03U8: → UInt8 (bool function),  \x02D: → Date.
    // These prefixes are stripped by csvToNativeBlock before writing column names.
    const col_expr = proj.column orelse "";
    const col_lower = blk: {
        var buf: [256]u8 = undefined;
        if (col_expr.len > buf.len) break :blk col_expr;
        break :blk std.ascii.lowerString(&buf, col_expr);
    };
    const is_bool_expr = blk: {
        // Aggregate functions (max/min/sum/avg/count) always return numeric types
        // that the driver scans as uint64/float64 — never UInt8.
        switch (proj.func) {
            .max, .min, .sum, .avg, .count_star, .count_distinct, .count_if,
            .uniq_exact, .uniq_exact_if, .group_uniq_array, .any_val => break :blk false,
            else => {},
        }
        // Only mark as bool if the top-level expression IS a pure bool function call —
        // i.e. no top-level OR/AND operators (which would produce a mixed-type result
        // that must be UInt64, not UInt8).
        if (findTopLevelKeyword(col_expr, "OR") != null) break :blk false;
        if (findTopLevelKeyword(col_expr, "AND") != null) break :blk false;
        break :blk std.mem.startsWith(u8, col_lower, "has(") or
            std.mem.startsWith(u8, col_lower, "hasany(") or
            std.mem.startsWith(u8, col_lower, "hasall(") or
            std.mem.startsWith(u8, col_lower, "dictha") or
            std.mem.startsWith(u8, col_lower, "nothas(") or
            std.mem.startsWith(u8, col_lower, "match(") or
            std.mem.startsWith(u8, col_lower, "startswith(") or
            std.mem.startsWith(u8, col_lower, "endswith(") or
            std.mem.startsWith(u8, col_lower, "isipv4string(") or
            std.mem.startsWith(u8, col_lower, "isipv6string(") or
            std.mem.startsWith(u8, col_lower, "notempty(") or
            std.mem.startsWith(u8, col_lower, "empty(") or
            std.mem.startsWith(u8, col_lower, "like ") or
            std.mem.startsWith(u8, col_lower, "simplejsonextractbool(") or
            std.mem.startsWith(u8, col_lower, "jsonextractbool(");
    };
    const is_date_expr = std.mem.startsWith(u8, col_lower, "cast(") and
        (std.ascii.indexOfIgnoreCase(col_expr, " AS DATE") != null or
         std.ascii.indexOfIgnoreCase(col_expr, " AS date") != null) or
        std.mem.startsWith(u8, col_lower, "todate(");
    const type_prefix: []const u8 = if (is_bool_expr) "\x03U8:" else if (is_date_expr) "\x02D:" else "";
    if (proj.alias) |a| {
        if (type_prefix.len > 0) try out.appendSlice(allocator, type_prefix);
        try writeColName(out, allocator, a);
        return;
    }
    switch (proj.func) {
        .column_ref => {
            const col = proj.column orelse "*";
            if (proj.int_offset == 0) {
                if (type_prefix.len > 0) try out.appendSlice(allocator, type_prefix);
                try writeColName(out, allocator, col);
            } else if (proj.int_offset > 0) {
                try out.print(allocator, "{s} + {d}", .{ col, proj.int_offset });
            } else {
                try out.print(allocator, "{s} - {d}", .{ col, -proj.int_offset });
            }
        },
        .int_literal => try out.print(allocator, "{d}", .{proj.int_offset}),
        .float_literal => try out.print(allocator, "{d}", .{proj.float_val}),
        .count_star => try out.appendSlice(allocator, "count_star()"),
        .count_distinct => try out.print(allocator, "count(DISTINCT {s})", .{proj.column orelse ""}),
        .sum => if (proj.int_offset == 0)
            try out.print(allocator, "sum({s})", .{proj.column orelse ""})
        else
            try out.print(allocator, "sum(({s} + {d}))", .{ proj.column orelse "", proj.int_offset }),
        .avg => try out.print(allocator, "avg({s})", .{proj.column orelse ""}),
        .min => try out.print(allocator, "min({s})", .{proj.column orelse ""}),
        .max => try out.print(allocator, "max({s})", .{proj.column orelse ""}),
        .count_if => try out.print(allocator, "countIf(...)", .{}),
        .uniq_exact => try out.print(allocator, "uniqExact({s})", .{proj.column orelse ""}),
        .uniq_exact_if => try out.print(allocator, "uniqExactIf({s},...)", .{proj.column orelse ""}),
        .group_uniq_array => try out.print(allocator, "groupUniqArray({s})", .{proj.column orelse ""}),
        .any_val => try out.print(allocator, "any({s})", .{proj.column orelse ""}),
        .case_when => try out.appendSlice(allocator, proj.alias orelse "case_when"),
        .cmp_expr => try out.appendSlice(allocator, proj.alias orelse proj.column orelse "cmp_expr"),
        .min_if => try out.print(allocator, "minIf({s},...)", .{proj.column orelse ""}),
        .max_if => try out.print(allocator, "maxIf({s},...)", .{proj.column orelse ""}),
        .sum_array => try out.print(allocator, "sumArray({s})", .{proj.column orelse ""}),
        .sum_array_if => try out.print(allocator, "sumArrayIf({s},...)", .{proj.column orelse ""}),
    }
}

// ── Helper: write group-by header ────────────────────────────────────────────

fn writeGroupHeader(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    plan: generic_sql.Plan,
    group_cols: []const []const u8,
) !void {
    _ = group_cols;
    var first = true;
    for (plan.projections) |proj| {
        // Skip hidden arrayJoin expansion columns (added internally for ARRAY JOIN support).
        if (isHiddenArrayJoinProj(proj)) continue;
        if (!first) try out.append(allocator, ',');
        first = false;
        try writeExprHeader(out, allocator, proj);
    }
    try out.append(allocator, '\n');
}

/// Returns true for projections that were auto-appended by rewriteArrayJoin for
/// non-top-level ARRAY JOIN aliases (e.g. "arrayJoin(mapValues(features)) AS __aj__fv").
/// These provide values for aggregate expressions but should not appear in output.
fn isHiddenArrayJoinProj(proj: generic_sql.Expr) bool {
    const alias = proj.alias orelse return false;
    return std.mem.startsWith(u8, alias, "__aj__") or std.mem.startsWith(u8, alias, "__ha__");
}

// ── Helper: detect all-aggregate plan ────────────────────────────────────────

fn allAggregates(projections: []const generic_sql.Expr) bool {
    for (projections) |p| {
        switch (p.func) {
            .count_star, .count_distinct, .count_if,
            .sum, .avg, .min, .max,
            .min_if, .max_if, .sum_array, .sum_array_if,
            .uniq_exact, .uniq_exact_if,
            .group_uniq_array, .any_val => {},
            .column_ref, .int_literal, .float_literal, .case_when, .cmp_expr => return false,
        }
    }
    return projections.len > 0;
}

fn anyAggregate(projections: []const generic_sql.Expr) bool {
    for (projections) |p| {
        switch (p.func) {
            .count_star, .count_distinct, .count_if,
            .sum, .avg, .min, .max,
            .min_if, .max_if, .sum_array, .sum_array_if,
            .uniq_exact, .uniq_exact_if,
            .group_uniq_array, .any_val => return true,
            .column_ref, .int_literal, .float_literal, .case_when, .cmp_expr => {},
        }
    }
    return false;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// Smoke tests against data/fixture_hits.parquet (1 row):
//   WatchID=..., CounterID=62, Age=30, EventDate=15887, ...
// Run with: zig build test

const fixture_parquet = build_options.fixture_parquet_path;

fn runQuery(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    const plan = (try generic_sql.parse(allocator, sql)) orelse
        return error.ParseFailed;
    defer generic_sql.deinit(allocator, plan);
    return run(allocator, std.testing.io, plan, fixture_parquet, &clickbench_schema.hits);
}

test "smoke: count(*)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT count(*) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("count_star()\n1\n", out);
}

test "smoke: sum(Age)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT sum(Age) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("sum(Age)\n30\n", out);
}

test "smoke: avg(Age)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT avg(Age) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("avg(Age)\n30\n", out);
}

test "smoke: min and max EventDate" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT min(EventDate), max(EventDate) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("min(EventDate),max(EventDate)\n15887,15887\n", out);
}

test "smoke: count(DISTINCT CounterID)" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator, "SELECT count(DISTINCT CounterID) FROM hits");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("count(DISTINCT CounterID)\n1\n", out);
}

test "smoke: GROUP BY CounterID" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID, count(*) AS c FROM hits GROUP BY CounterID ORDER BY c DESC LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID,c\n62,1\n", out);
}

test "smoke: WHERE Age > 0 scan" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE no match returns header only" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 999 LIMIT 5");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}

test "smoke: sum with WHERE filter" {
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT sum(Age) FROM hits WHERE Age > 0");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("sum(Age)\n30\n", out);
}

test "allAggregates" {
    const std2 = std;
    _ = std2;
    const plan_agg = [_]generic_sql.Expr{
        .{ .func = .count_star },
        .{ .func = .sum, .column = "x" },
    };
    try std.testing.expect(allAggregates(&plan_agg));

    const plan_mixed = [_]generic_sql.Expr{
        .{ .func = .column_ref, .column = "x" },
        .{ .func = .count_star },
    };
    try std.testing.expect(!allAggregates(&plan_mixed));
}

test "parseGroupCols" {
    const allocator = std.testing.allocator;
    const cols = try parseGroupCols(allocator, "RegionID, CounterID");
    defer {
        for (cols) |c| allocator.free(c);
        allocator.free(cols);
    }
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("RegionID", cols[0]);
    try std.testing.expectEqualStrings("CounterID", cols[1]);
}

test "value order" {
    try std.testing.expect(Value.order(.{ .i64 = 1 }, .{ .i64 = 2 }) == .lt);
    try std.testing.expect(Value.order(.{ .i64 = 2 }, .{ .i64 = 1 }) == .gt);
    try std.testing.expect(Value.order(.{ .i64 = 1 }, .{ .i64 = 1 }) == .eq);
    try std.testing.expect(Value.order(.{ .str = "a" }, .{ .str = "b" }) == .lt);
}

test "likeMatch: basic percent wildcard" {
    try std.testing.expect(likeMatch("hello world", "hello%", false));
    try std.testing.expect(likeMatch("hello world", "%world", false));
    try std.testing.expect(likeMatch("hello world", "%lo wo%", false));
    try std.testing.expect(!likeMatch("hello world", "hello", false));
}

test "likeMatch: underscore wildcard" {
    try std.testing.expect(likeMatch("abc", "a_c", false));
    try std.testing.expect(!likeMatch("ac", "a_c", false));
    try std.testing.expect(likeMatch("aXc", "a_c", false));
}

test "likeMatch: case insensitive ILIKE" {
    try std.testing.expect(likeMatch("Hello World", "hello%", true));
    try std.testing.expect(likeMatch("GOOGLE", "%oog%", true));
    try std.testing.expect(!likeMatch("GOOGLE", "%oog%", false));
}

test "likeMatch: empty pattern and string" {
    try std.testing.expect(likeMatch("", "%", false));
    try std.testing.expect(likeMatch("", "", false));
    try std.testing.expect(!likeMatch("a", "", false));
}

test "smoke: WHERE AND multi-condition match" {
    // Age=30 AND CounterID=62 — fixture row should match
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 AND CounterID = 62");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE AND multi-condition no match" {
    // Age=30 AND CounterID=999 — no row matches
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE Age > 0 AND CounterID = 999");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}

test "smoke: WHERE date string comparison" {
    // EventDate=15887 corresponds to 2013-07-03; row should pass >= '2013-07-01'
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE EventDate >= '2013-07-01' AND EventDate < '2013-07-10'");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n62\n", out);
}

test "smoke: WHERE date string no match" {
    // EventDate=15887 should not pass > '2013-12-31'
    const allocator = std.testing.allocator;
    const out = try runQuery(allocator,
        "SELECT CounterID FROM hits WHERE EventDate > '2013-12-31'");
    defer allocator.free(out);
    try std.testing.expectEqualStrings("CounterID\n", out);
}
