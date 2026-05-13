//! Late-materialize execution path for ClickBench string-top queries.
//!
//! These operators run a hash-keyed top-K aggregation over the hot
//! sidecar columns (URLHash / TitleHash / RefererHash) and only resolve
//! the winning hashes back to their full string payloads at the end via
//! a parquet (or DuckDB) lookup. The shared cache, parquet resolver and
//! tiny URL-top sort helpers live here so they can be reused by Q21,
//! Q34, Q37, Q38, Q39, Q40 and the Q22/Q23 SearchPhrase variants.
//!
//! Phase B3a: Q21/Q34 entry points fully owned by this module. Q37/Q38/
//! Q39/Q40/Q22/Q23 entry points still live in src/native.zig but consume
//! the helpers exported from here.

const std = @import("std");
const build_options = @import("build_options");
const duckdb = if (build_options.duckdb) @import("../duckdb.zig") else @import("../duckdb_stub.zig");
const io_map = @import("../io_map.zig");
const native_group = @import("../exec/group.zig");
const parquet = @import("../parquet.zig");
const storage = @import("../storage.zig");

pub const UrlHashCount = native_group.UrlHashCount;
pub const UrlTopCache = native_group.UrlTopCache;

// ----------------------------------------------------------------------------
// HashStringCache: hash -> bytes blob. One instance per hash column kind
// (URL / Title / Referer) so independent queries amortise resolver work
// across runs.
// ----------------------------------------------------------------------------

pub const HashStringCache = struct {
    allocator: std.mem.Allocator,
    map: std.AutoHashMap(i64, Span),
    blob: std.ArrayList(u8),

    const Span = struct { start: usize, end: usize };

    pub fn init(allocator: std.mem.Allocator) HashStringCache {
        return .{ .allocator = allocator, .map = std.AutoHashMap(i64, Span).init(allocator), .blob = .empty };
    }

    pub fn deinit(self: *HashStringCache) void {
        self.map.deinit();
        self.blob.deinit(self.allocator);
    }

    pub fn get(self: *const HashStringCache, hash: i64) ?[]const u8 {
        const span = self.map.get(hash) orelse return null;
        return self.blob.items[span.start..span.end];
    }

    pub fn put(self: *HashStringCache, hash: i64, value: []const u8) !void {
        if (self.map.contains(hash)) return;
        const start = self.blob.items.len;
        try self.blob.appendSlice(self.allocator, value);
        try self.map.put(hash, .{ .start = start, .end = self.blob.items.len });
    }
};

pub const HashStringKind = enum { url, title, referer };

// ----------------------------------------------------------------------------
// URL-hash top-10 sort helpers (count DESC, hash ASC tiebreak).
// ----------------------------------------------------------------------------

pub fn urlHashBefore(a: UrlHashCount, b: UrlHashCount) bool {
    if (a.count == b.count) return a.url_hash < b.url_hash;
    return a.count > b.count;
}

pub fn insertUrlHashTop10(top: *[10]UrlHashCount, top_len: *usize, row: UrlHashCount) void {
    var pos: usize = 0;
    while (pos < top_len.* and urlHashBefore(top[pos], row)) : (pos += 1) {}
    if (pos >= 10) return;
    if (top_len.* < 10) top_len.* += 1;
    var i = top_len.* - 1;
    while (i > pos) : (i -= 1) top[i] = top[i - 1];
    top[pos] = row;
}

pub fn q39UrlHashHeapLess(_: void, a: UrlHashCount, b: UrlHashCount) std.math.Order {
    const a_worse = if (a.count != b.count) a.count < b.count else a.url_hash > b.url_hash;
    const b_worse = if (a.count != b.count) b.count < a.count else b.url_hash > a.url_hash;
    if (a_worse) return .lt;
    if (b_worse) return .gt;
    return .eq;
}

// ----------------------------------------------------------------------------
// CSV emission helper for resolved (URL/Title hash, count) rows.
// ----------------------------------------------------------------------------

pub fn formatHashCountRows(allocator: std.mem.Allocator, cache: *HashStringCache, rows: []const UrlHashCount, header: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, header);
    try out.append(allocator, '\n');
    for (rows) |row| {
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

// ----------------------------------------------------------------------------
// Parquet hash resolver. Falls back to DuckDB only for .title (no stored
// hash sidecar) and only outside submit mode.
// ----------------------------------------------------------------------------

pub fn resolveUrlHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .url);
}

pub fn resolveTitleHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .title);
}

pub fn resolveRefererHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount) !void {
    try resolveHashesFromParquet(allocator, io, data_dir, cache, rows, .referer);
}

pub fn resolveHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, rows: []const UrlHashCount, kind: HashStringKind) !void {
    var missing: std.ArrayList(i64) = .empty;
    defer missing.deinit(allocator);
    for (rows) |row| {
        if (cache.get(row.url_hash) == null) try missing.append(allocator, row.url_hash);
    }
    if (missing.items.len == 0) return;
    if (kind == .url or kind == .referer) {
        try resolveStoredHashesFromParquet(allocator, io, data_dir, cache, missing.items, kind);
        return;
    }
    if (submitMode() or !build_options.duckdb) return error.UnsupportedNativeQuery;

    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const parquet_literal = try duckdb.sqlStringLiteral(allocator, parquet_path);
    defer allocator.free(parquet_literal);

    var hash_list: std.ArrayList(u8) = .empty;
    defer hash_list.deinit(allocator);
    for (missing.items, 0..) |hash, i| {
        if (i != 0) try hash_list.appendSlice(allocator, ",");
        try hash_list.print(allocator, "{d}", .{hash});
    }

    const limit_filter = if (try importRowLimit(allocator, io, data_dir)) |limit_rows|
        try std.fmt.allocPrint(allocator, " AND file_row_number < {d}", .{limit_rows})
    else
        try allocator.dupe(u8, "");
    defer allocator.free(limit_filter);

    const sql = switch (kind) {
        .url => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT URLHash AS h, hex(URL) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE URLHash IN ({s}){s}
            \\GROUP BY URLHash, URL
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
        .title => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT CAST(hash(CAST(Title AS VARCHAR)) & 9223372036854775807 AS BIGINT) AS h, hex(Title) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE CAST(hash(CAST(Title AS VARCHAR)) & 9223372036854775807 AS BIGINT) IN ({s}){s}
            \\GROUP BY h, Title
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
        .referer => try std.fmt.allocPrint(allocator,
            \\COPY (
            \\SELECT RefererHash AS h, hex(Referer) AS x
            \\FROM read_parquet({s}, binary_as_string=True, file_row_number=True)
            \\WHERE RefererHash IN ({s}){s}
            \\GROUP BY RefererHash, Referer
            \\) TO STDOUT (FORMAT csv, HEADER true);
        , .{ parquet_literal, hash_list.items, limit_filter }),
    };
    defer allocator.free(sql);

    var ddb = duckdb.DuckDb.init(allocator, io, data_dir);
    defer ddb.deinit();
    const raw = try ddb.runRawSql(sql);
    defer allocator.free(raw);
    try parseHashHexRowsIntoCache(allocator, cache, raw);
}

const NativeHashResolveContext = struct {
    allocator: std.mem.Allocator,
    cache: *HashStringCache,
    targets: std.AutoHashMap(i64, void),
    hashes: []const i64,
    row_index: usize = 0,

    fn observe(self: *NativeHashResolveContext, value: []const u8) !void {
        defer self.row_index += 1;
        if (self.row_index >= self.hashes.len) return error.CorruptHotColumns;
        const hash = self.hashes[self.row_index];
        if (!self.targets.contains(hash)) return;
        if (self.cache.get(hash) != null) return;
        try self.cache.put(hash, value);
    }
};

fn resolveStoredHashesFromParquet(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8, cache: *HashStringCache, missing: []const i64, kind: HashStringKind) !void {
    var targets = std.AutoHashMap(i64, void).init(allocator);
    defer targets.deinit();
    try targets.ensureTotalCapacity(@intCast(missing.len));
    for (missing) |hash| try targets.put(hash, {});

    const hash_file = switch (kind) {
        .url => storage.hot_url_hash_name,
        .referer => storage.hot_referer_hash_name,
        .title => return error.UnsupportedNativeQuery,
    };
    const parquet_column: usize = switch (kind) {
        .url => 13,
        .referer => 14,
        .title => return error.UnsupportedNativeQuery,
    };
    const hash_path = try storage.hotColumnPath(allocator, data_dir, hash_file);
    defer allocator.free(hash_path);
    const hash_col = try io_map.mapColumn(i64, io, hash_path);
    defer hash_col.mapping.unmap();
    const parquet_path = try storage.readImportSource(io, allocator, data_dir);
    defer allocator.free(parquet_path);
    const limit_rows = try importRowLimit(allocator, io, data_dir);
    const limit_usize: ?usize = if (limit_rows) |n| @intCast(n) else null;
    if (limit_usize) |n| if (n != hash_col.values.len) return error.CorruptHotColumns;
    var ctx = NativeHashResolveContext{ .allocator = allocator, .cache = cache, .targets = targets, .hashes = hash_col.values };
    const scanned = try parquet.streamByteArrayColumnPath(allocator, io, parquet_path, parquet_column, limit_usize, &ctx, NativeHashResolveContext.observe);
    if (scanned != hash_col.values.len) return error.CorruptHotColumns;
    for (missing) |hash| if (cache.get(hash) == null) return error.CorruptHotColumns;
}

fn parseHashHexRowsIntoCache(allocator: std.mem.Allocator, cache: *HashStringCache, raw: []const u8) !void {
    var lines = std.mem.splitScalar(u8, raw, '\n');
    _ = lines.next();
    var decoded: std.ArrayList(u8) = .empty;
    defer decoded.deinit(allocator);
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, "\r");
        if (line.len == 0) continue;
        const comma = std.mem.indexOfScalar(u8, line, ',') orelse continue;
        const hash = try std.fmt.parseInt(i64, line[0..comma], 10);
        const hex = line[comma + 1 ..];
        try decoded.resize(allocator, hex.len / 2);
        _ = try std.fmt.hexToBytes(decoded.items, hex);
        try cache.put(hash, decoded.items);
    }
}

// ----------------------------------------------------------------------------
// Q21 / Q34 entry points: full URL top-10 by PageViews, optional leading
// constant column for SELECT 1, URL, count(*) variants. `hot` only needs
// to expose `.url_hash: []const i64`.
// ----------------------------------------------------------------------------

pub fn formatUrlCountTopHashLateMaterialize(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    hot: anytype,
    cache: *HashStringCache,
    comptime include_constant: bool,
) ![]u8 {
    const top = try native_group.collectUrlHashTop(allocator, hot.url_hash);
    try resolveUrlHashesFromParquet(allocator, io, data_dir, cache, top.rows[0..top.len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, if (include_constant) "1,URL,c\n" else "URL,c\n");
    for (top.rows[0..top.len]) |row| {
        if (include_constant) try out.appendSlice(allocator, "1,");
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

/// Same as formatUrlCountTopHashLateMaterialize but caches the URL-hash
/// top-10 across calls (Q34 invokes this 10x with the same predicate).
pub fn formatUrlCountTopHashLateMaterializeCached(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    hot: anytype,
    cache: *HashStringCache,
    top_cache: *?UrlTopCache,
    comptime include_constant: bool,
) ![]u8 {
    if (top_cache.* == null) {
        top_cache.* = try native_group.collectUrlHashTop(allocator, hot.url_hash);
    }
    const top = top_cache.*.?;
    try resolveUrlHashesFromParquet(allocator, io, data_dir, cache, top.rows[0..top.len]);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, if (include_constant) "1,URL,c\n" else "URL,c\n");
    for (top.rows[0..top.len]) |row| {
        if (include_constant) try out.appendSlice(allocator, "1,");
        try writeCsvField(allocator, &out, cache.get(row.url_hash) orelse return error.CorruptHotColumns);
        try out.print(allocator, ",{d}\n", .{row.count});
    }
    return out.toOwnedSlice(allocator);
}

// ----------------------------------------------------------------------------
// Local utility copies. `writeCsvField` must match src/native.zig:8244 byte
// for byte (DuckDB-compatible quoting). `submitMode`/`importRowLimit` mirror
// src/native.zig so the resolver can be self-contained.
// ----------------------------------------------------------------------------

fn submitMode() bool {
    return std.c.getenv("ZIGHOUSE_CLICKBENCH_SUBMIT") != null;
}

fn importRowLimit(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !?u64 {
    const info = storage.readImportInfo(io, allocator, data_dir) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer info.deinit(allocator);
    return info.rowLimit();
}

fn writeCsvField(allocator: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    var needs_quote = false;
    for (s) |b| {
        if (b == ',' or b == '"' or b == '\n' or b == '\r' or b >= 0x80) {
            needs_quote = true;
            break;
        }
    }
    if (!needs_quote) {
        try out.appendSlice(allocator, s);
    } else {
        try out.append(allocator, '"');
        for (s) |b| {
            if (b == '"') try out.append(allocator, '"');
            try out.append(allocator, b);
        }
        try out.append(allocator, '"');
    }
}
