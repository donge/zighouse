/// ResultSet → ClickHouse Native block serializer.
/// Also provides CSV → ResultSet conversion for the bridge path
/// (while generic_executor still outputs CSV).
///
/// Design:
///   - `toNativeBlock(alloc, rs)`: ResultSet → []u8 Native block (type-exact, no guessing)
///   - `csvToResultSet(alloc, csv, schema)`: CSV []u8 → ResultSet (uses schema for types)
///
/// Type information flows:
///   generic_executor → CSV (with \x03U8: / \x02D: sentinels) → csvToResultSet → ResultSet
///   → toNativeBlock → []u8
///
/// This eliminates `colNameTypeHint` (column-name-based type guessing) because
/// csvToResultSet derives types from the schema, not the column name.
const std    = @import("std");
const core   = @import("core");
const schema = @import("schema");
const csv_mod = @import("csv");

pub const ResultSet  = core.ResultSet;
pub const ColMeta    = core.ColMeta;
pub const ColumnType = core.ColumnType;

// ── Low-level wire helpers ────────────────────────────────────────────────────

fn putUVarInt(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, v: u64) !void {
    var x = v;
    while (x >= 0x80) {
        try buf.append(alloc, @as(u8, @intCast((x & 0x7F) | 0x80)));
        x >>= 7;
    }
    try buf.append(alloc, @as(u8, @intCast(x)));
}

fn putString(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, s: []const u8) !void {
    try putUVarInt(buf, alloc, s.len);
    try buf.appendSlice(alloc, s);
}

fn putBlockInfo(buf: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator) !void {
    try putUVarInt(buf, alloc, 1);
    try buf.append(alloc, 0);
    try putUVarInt(buf, alloc, 2);
    try buf.appendSlice(alloc, &[4]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    try putUVarInt(buf, alloc, 0);
}

// ── CSV → ResultSet ───────────────────────────────────────────────────────────

/// Convert a CSV byte slice (produced by generic_executor) into a ResultSet.
///
/// `tbl` provides schema-level type information so types are exact.
/// When `tbl` is null, types are inferred from sentinels and value content.
///
/// The returned ResultSet must be freed with rs.deinit().
pub fn csvToResultSet(
    alloc: std.mem.Allocator,
    csv: []const u8,
    tbl: ?*const schema.Table,
) !ResultSet {
    var sink = core.ResultSink.init(alloc);
    errdefer {
        var s2 = sink;
        _ = s2.finish() catch {};
    }

    // Split lines
    var lines: std.ArrayListUnmanaged([]const u8) = .empty;
    defer lines.deinit(alloc);
    var it = std.mem.splitScalar(u8, csv, '\n');
    while (it.next()) |line| {
        if (line.len > 0) try lines.append(alloc, line);
    }
    if (lines.items.len == 0) return sink.finish();

    // Parse header
    var col_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer col_names.deinit(alloc);
    var header_owned: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (header_owned.items) |h| alloc.free(h);
        header_owned.deinit(alloc);
    }
    {
        const hdr = lines.items[0];
        if (std.mem.indexOfScalar(u8, hdr, '"') == null) {
            var hi = std.mem.splitScalar(u8, hdr, ',');
            while (hi.next()) |name| try col_names.append(alloc, name);
        } else {
            var pos: usize = 0;
            var field_buf: std.ArrayListUnmanaged(u8) = .empty;
            defer field_buf.deinit(alloc);
            while (pos < hdr.len) {
                const was = pos;
                const raw = csv_mod.parseCsvField(hdr, &pos, &field_buf, alloc);
                if (pos == was) break;
                const name: []const u8 = if (was < hdr.len and hdr[was] == '"') blk: {
                    const dup = try alloc.dupe(u8, raw);
                    try header_owned.append(alloc, dup);
                    break :blk @as([]const u8, dup);
                } else raw;
                try col_names.append(alloc, name);
            }
        }
    }

    const num_cols = col_names.items.len;
    if (num_cols == 0) return sink.finish();

    // Strip type-hint sentinels: \x03U8: → bool_u8,  \x02D: → date_u16
    const ColSentinel = enum { none, uint8, date };
    const sentinels = try alloc.alloc(ColSentinel, num_cols);
    defer alloc.free(sentinels);
    for (col_names.items, 0..) |raw, i| {
        if (raw.len > 4 and raw[0] == 0x03 and raw[1] == 'U' and raw[2] == '8' and raw[3] == ':') {
            sentinels[i] = .uint8;
            col_names.items[i] = raw[4..];
        } else if (raw.len > 3 and raw[0] == 0x02 and raw[1] == 'D' and raw[2] == ':') {
            sentinels[i] = .date;
            col_names.items[i] = raw[3..];
        } else {
            sentinels[i] = .none;
        }
    }

    const num_rows = lines.items.len - 1;

    // Collect raw string values per column
    const cell_data = try alloc.alloc([][]const u8, num_cols);
    defer alloc.free(cell_data);
    for (0..num_cols) |ci| {
        cell_data[ci] = try alloc.alloc([]const u8, num_rows);
    }
    defer for (cell_data) |cd| alloc.free(cd);

    // Also track per-column value flags for type inference fallback
    const is_int   = try alloc.alloc(bool, num_cols);
    defer alloc.free(is_int);
    const is_float = try alloc.alloc(bool, num_cols);
    defer alloc.free(is_float);
    const is_array = try alloc.alloc(bool, num_cols);
    defer alloc.free(is_array);
    const has_neg  = try alloc.alloc(bool, num_cols);
    defer alloc.free(has_neg);
    const was_quoted = try alloc.alloc(bool, num_cols); // true if any value in column was CSV-quoted
    defer alloc.free(was_quoted);
    @memset(is_int,   true);
    @memset(is_float, true);
    @memset(is_array, false);
    @memset(has_neg,  false);
    @memset(was_quoted, false);

    // Quoted field storage (owned strings for quoted cells)
    var cell_owned: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (cell_owned.items) |s| alloc.free(s);
        cell_owned.deinit(alloc);
    }

    for (lines.items[1..], 0..) |line, ri| {
        var field_buf: std.ArrayListUnmanaged(u8) = .empty;
        defer field_buf.deinit(alloc);
        var pos: usize = 0;
        var ci: usize = 0;
        while (pos <= line.len and ci < num_cols) : (ci += 1) {
            const was = pos;
            const raw = csv_mod.parseCsvField(line, &pos, &field_buf, alloc);
            const val: []const u8 = if (was < line.len and line[was] == '"') blk: {
                const dup = try alloc.dupe(u8, raw);
                try cell_owned.append(alloc, dup);
                was_quoted[ci] = true; // mark column as string
                break :blk @as([]const u8, dup);
            } else raw;
            cell_data[ci][ri] = val;
            if (val.len > 0 and val[0] == 0x01) {
                is_array[ci] = true; is_int[ci] = false; is_float[ci] = false;
                continue;
            }
            // If this cell was CSV-quoted, the column is a string — skip numeric inference.
            if (was_quoted[ci]) {
                is_int[ci] = false; is_float[ci] = false;
                continue;
            }
            if (is_int[ci]) {
                const iv = std.fmt.parseInt(i64, val, 10) catch {
                    is_int[ci] = false;
                    _ = std.fmt.parseFloat(f64, val) catch { is_float[ci] = false; };
                    continue;
                };
                if (iv < 0) has_neg[ci] = true;
            }
            if (!is_int[ci] and is_float[ci]) {
                _ = std.fmt.parseFloat(f64, val) catch { is_float[ci] = false; };
            }
        }
        while (ci < num_cols) : (ci += 1) {
            cell_data[ci][ri] = "";
            is_int[ci] = false; is_float[ci] = false;
        }
    }

    // Determine column type for each column
    const col_types = try alloc.alloc(core.ColumnType, num_cols);
    defer alloc.free(col_types);
    // ch_type override (e.g. "UInt16") for narrow unsigned integer columns.
    const ch_type_overrides = try alloc.alloc(?[]const u8, num_cols);
    defer alloc.free(ch_type_overrides);
    @memset(ch_type_overrides, null);
    for (0..num_cols) |ci| {
        if (sentinels[ci] == .uint8) {
            col_types[ci] = .bool_u8;
            continue;
        }
        if (sentinels[ci] == .date) {
            col_types[ci] = .date_u16;
            continue;
        }
        // Schema-based type (most accurate)
        if (tbl) |t| {
            if (t.findColumn(col_names.items[ci])) |idx| {
                const col = t.columns[idx];
                const ch = col.ch_type;
                col_types[ci] = schemaToCore(col.ty, ch);
                // Record narrow unsigned wire type.
                if (col_types[ci] == .uint64) {
                    if (ch) |ct| {
                        if (std.mem.eql(u8, ct, "UInt16") or std.mem.eql(u8, ct, "UInt32")) {
                            ch_type_overrides[ci] = ct;
                        }
                    }
                }
                continue;
            }
        }
        // Heuristic fallback (only when no schema)
        if (is_array[ci]) {
            col_types[ci] = .array_string;
        } else if (is_int[ci]) {
            // Timestamp heuristic: large non-negative ints
            const fv = if (num_rows > 0) std.fmt.parseInt(i64, cell_data[ci][0], 10) catch 0 else 0;
            if (!has_neg[ci] and fv >= 1_000_000_000_000) {
                col_types[ci] = .datetime64_ms;
            } else if (!has_neg[ci] and fv >= 1_000_000_000 and fv < 4_000_000_000) {
                // Unix timestamp seconds (e.g. toUnixTimestamp result) → UInt32
                col_types[ci] = .uint64;
                ch_type_overrides[ci] = "UInt32";
            } else if (has_neg[ci]) {
                col_types[ci] = .int64;
            } else if (tbl == null) {
                // No schema hint (pure heuristic): use uint64 for non-negative ints.
                col_types[ci] = .uint64;
            } else {
                // Empty schema (computed query with fake_table): default to Int64
                // so that Go *int64 scans work for expressions like length(), arraySize(), etc.
                col_types[ci] = .int64;
            }
        } else if (is_float[ci]) {
            col_types[ci] = .float64;
        } else {
            col_types[ci] = .string;
        }
    }

    // Build a single DataChunk
    if (num_rows == 0) {
        // Zero-row result: return a ResultSet with schema but no rows.
        // Build an empty chunk so sink records the column metadata.
        var b = core.ChunkBuilder.init(alloc, 0);
        errdefer b.chunk.deinit();
        for (0..num_cols) |ci| {
            _ = try b.addColumn(col_names.items[ci], col_types[ci]);
        }
        try sink.consume(b.finish());
        return sink.finish();
    }

    var b = core.ChunkBuilder.init(alloc, num_rows);
    errdefer b.chunk.deinit();
    for (0..num_cols) |ci| {
        const idx = try b.addColumn(col_names.items[ci], col_types[ci]);
        const col = &b.chunk.columns[idx];
        switch (col_types[ci]) {
            .bool_u8 => {
                for (0..num_rows) |r| {
                    col.data.bool_u8[r] = std.fmt.parseInt(u8, cell_data[ci][r], 10) catch 0;
                }
            },
            .int64 => {
                for (0..num_rows) |r| {
                    col.data.int64[r] = std.fmt.parseInt(i64, cell_data[ci][r], 10) catch 0;
                }
            },
            .uint64 => {
                for (0..num_rows) |r| {
                    col.data.uint64[r] = std.fmt.parseInt(u64, cell_data[ci][r], 10) catch 0;
                }
            },
            .float64 => {
                for (0..num_rows) |r| {
                    col.data.float64[r] = std.fmt.parseFloat(f64, cell_data[ci][r]) catch 0.0;
                }
            },
            .date_u16 => {
                for (0..num_rows) |r| {
                    col.data.date_u16[r] = std.fmt.parseInt(u16, cell_data[ci][r], 10) catch 0;
                }
            },
            .datetime64_ms => {
                const fv0 = if (num_rows > 0) std.fmt.parseInt(i64, cell_data[ci][0], 10) catch 0 else 0;
                const scale_s: bool = !has_neg[ci] and fv0 >= 1_000_000_000 and fv0 < 1_000_000_000_000;
                for (0..num_rows) |r| {
                    const iv = std.fmt.parseInt(i64, cell_data[ci][r], 10) catch 0;
                    col.data.datetime64_ms[r] = if (scale_s) iv * 1000 else iv;
                }
            },
            .string => {
                const ra = b.chunk.arena.allocator();
                for (0..num_rows) |r| {
                    col.data.string[r] = try ra.dupe(u8, cell_data[ci][r]);
                }
            },
            .array_string => {
                const ra = b.chunk.arena.allocator();
                for (0..num_rows) |r| {
                    const val = cell_data[ci][r];
                    // Values encoded as \x01 + elements joined by \x0c
                    if (val.len > 0 and val[0] == 0x01) {
                        const content = val[1..];
                        if (content.len == 0) {
                            col.data.array_string[r] = &.{};
                        } else {
                            var elem_list: std.ArrayListUnmanaged([]const u8) = .empty;
                            var elem_it = std.mem.splitScalar(u8, content, '\x0c');
                            while (elem_it.next()) |elem| {
                                try elem_list.append(ra, try ra.dupe(u8, elem));
                            }
                            col.data.array_string[r] = try elem_list.toOwnedSlice(ra);
                        }
                    } else {
                        col.data.array_string[r] = &.{};
                    }
                }
            },
        }
    }
    try sink.consume(b.finish());
    var rs = try sink.finish();
    // Apply ch_type overrides to metas (for narrow unsigned wire encoding).
    for (0..rs.metas.len) |ci| {
        if (ch_type_overrides[ci]) |ov| rs.metas[ci].ch_type = ov;
    }
    return rs;
}

/// Map schema.ColumnType + optional raw ch_type string to core.ColumnType.
fn schemaToCore(ty: schema.ColumnType, ch_type: ?[]const u8) core.ColumnType {
    // ch_type overrides for Array/Map → array_string
    if (ch_type) |ct| {
        if (std.mem.startsWith(u8, ct, "Array(") or std.mem.startsWith(u8, ct, "Map(")) {
            return .array_string;
        }
    }
    return switch (ty) {
        .int8  => .bool_u8,
        .int16 => blk: {
            // UInt16 via ch_type prefix 'U'
            if (ch_type) |ct| if (std.mem.startsWith(u8, ct, "U")) break :blk .uint64;
            break :blk .int64;
        },
        .int32 => blk: {
            // UInt32 via ch_type prefix 'U'
            if (ch_type) |ct| if (std.mem.startsWith(u8, ct, "U")) break :blk .uint64;
            break :blk .int64;
        },
        .int64 => blk: {
            if (ch_type) |ct| if (std.mem.startsWith(u8, ct, "U")) break :blk .uint64;
            break :blk .int64;
        },
        .date      => .date_u16,
        .timestamp => .datetime64_ms,
        .float32   => .float64, // upcast to float64
        .float64   => .float64,
        .text, .char, .low_card => .string,
    };
}

// ── ResultSet → CSV ───────────────────────────────────────────────────────────

/// Serialise `rs` into a CSV byte sequence compatible with generic_executor output.
///
/// Format:
///   - Header row: comma-separated column names, with \x03U8: prefix for bool_u8
///     and \x02D: prefix for date_u16 (consumed by csvToResultSet / streamRowsCsvFn).
///   - Data rows: comma-separated values, strings are RFC-4180 quoted when needed.
///
/// The caller owns the returned slice and must free it with `alloc`.
pub fn toCsv(alloc: std.mem.Allocator, rs: ResultSet) ![]u8 {
    return toCsvOffset(alloc, rs, 0);
}

pub fn toCsvOffset(alloc: std.mem.Allocator, rs: ResultSet, row_start: usize) ![]u8 {
    const num_rows = rs.num_rows;

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);

    // ── Header ────────────────────────────────────────────────────────────────
    for (rs.metas, 0..) |meta, ci| {
        if (ci > 0) try buf.append(alloc, ',');
        // Type-hint sentinels so streamRowsCsvFn / csvToResultSet knows the type.
        switch (meta.col_type) {
            .bool_u8   => try buf.appendSlice(alloc, "\x03U8:"),
            .date_u16  => try buf.appendSlice(alloc, "\x02D:"),
            else       => {},
        }
        // Quote column name if it contains commas, quotes, or newlines.
        const needs_quote = std.mem.indexOfAny(u8, meta.name, ",\"\n\r") != null;
        if (needs_quote) {
            try buf.append(alloc, '"');
            for (meta.name) |ch| {
                if (ch == '"') try buf.append(alloc, '"');
                try buf.append(alloc, ch);
            }
            try buf.append(alloc, '"');
        } else {
            try buf.appendSlice(alloc, meta.name);
        }
    }
    try buf.append(alloc, '\n');

    // ── Data rows ─────────────────────────────────────────────────────────────
    const actual_start = if (row_start > num_rows) num_rows else row_start;
    for (actual_start..num_rows) |r| {
        for (rs.metas, rs.columns, 0..) |meta, col, ci| {
            if (ci > 0) try buf.append(alloc, ',');
            const is_null = core.chunk.isNull(col.null_mask, r);
            if (is_null) {
                // Null → empty field
                continue;
            }
            switch (meta.col_type) {
                .bool_u8 => try buf.print(alloc, "{d}", .{col.data.bool_u8[r]}),
                .int64   => try buf.print(alloc, "{d}", .{col.data.int64[r]}),
                .uint64  => {
                    // Check for narrow wire type via ch_type override.
                    const wire = meta.ch_type orelse "UInt64";
                    if (std.mem.eql(u8, wire, "UInt16")) {
                        try buf.print(alloc, "{d}", .{@as(u16, @truncate(col.data.uint64[r]))});
                    } else if (std.mem.eql(u8, wire, "UInt32")) {
                        try buf.print(alloc, "{d}", .{@as(u32, @truncate(col.data.uint64[r]))});
                    } else {
                        try buf.print(alloc, "{d}", .{col.data.uint64[r]});
                    }
                },
                .float64 => {
                    const v = col.data.float64[r];
                    // Emit decimal point so downstream can distinguish Float64 from Int.
                    if (v == @trunc(v) and @abs(v) < 1e15) {
                        try buf.print(alloc, "{d}.0", .{@as(i64, @intFromFloat(v))});
                    } else {
                        try buf.print(alloc, "{d}", .{v});
                    }
                },
                .date_u16 => {
                    const ymd = core.exec.kernels.daysToYMD(col.data.date_u16[r]);
                    try buf.print(alloc, "{d:0>4}-{d:0>2}-{d:0>2}", .{ ymd[0], ymd[1], ymd[2] });
                },
                .datetime64_ms => try buf.print(alloc, "{d}", .{col.data.datetime64_ms[r]}),
                .string => {
                    const s = col.data.string[r];
                    const needs_quote = std.mem.indexOfAny(u8, s, ",\"\n\r") != null;
                    if (needs_quote) {
                        try buf.append(alloc, '"');
                        for (s) |ch| {
                            if (ch == '"') try buf.append(alloc, '"');
                            try buf.append(alloc, ch);
                        }
                        try buf.append(alloc, '"');
                    } else {
                        try buf.appendSlice(alloc, s);
                    }
                },
                .array_string => {
                    // Render as \x01 sentinel + \x0c-separated elements (same as generic_executor).
                    const arr = col.data.array_string[r];
                    try buf.append(alloc, 0x01);
                    for (arr, 0..) |s, i| {
                        if (i > 0) try buf.append(alloc, '\x0c');
                        try buf.appendSlice(alloc, s);
                    }
                },
            }
        }
        try buf.append(alloc, '\n');
    }

    return buf.toOwnedSlice(alloc);
}

// ── ResultSet → Native block ──────────────────────────────────────────────────

/// Serialise `rs` into a ClickHouse Native block byte sequence.
/// The caller owns the returned slice and must free it with `alloc`.
pub fn toNativeBlock(alloc: std.mem.Allocator, rs: ResultSet) ![]u8 {
    const num_cols = rs.metas.len;
    const num_rows = rs.num_rows;

    if (num_cols == 0) {
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        errdefer buf.deinit(alloc);
        try putBlockInfo(&buf, alloc);
        try putUVarInt(&buf, alloc, 0);
        try putUVarInt(&buf, alloc, 0);
        return buf.toOwnedSlice(alloc);
    }

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);

    try putBlockInfo(&buf, alloc);
    try putUVarInt(&buf, alloc, num_cols);
    try putUVarInt(&buf, alloc, num_rows);

    for (rs.metas, rs.columns) |meta, col| {
        try putString(&buf, alloc, meta.name);

        switch (meta.col_type) {
            .bool_u8 => {
                try putString(&buf, alloc, "UInt8");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: u8 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.bool_u8[r];
                    try buf.append(alloc, v);
                }
            },
            .int64 => {
                try putString(&buf, alloc, "Int64");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: i64 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.int64[r];
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(i64, &tmp, v, .little);
                    try buf.appendSlice(alloc, &tmp);
                }
            },
            .uint64 => {
                // Check for narrower wire types (UInt16, UInt32) via ch_type override.
                const wire_type = meta.ch_type orelse "UInt64";
                if (std.mem.eql(u8, wire_type, "UInt16")) {
                    try putString(&buf, alloc, "UInt16");
                    try buf.append(alloc, 0);
                    for (0..num_rows) |r| {
                        const v: u64 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.uint64[r];
                        var tmp: [2]u8 = undefined;
                        std.mem.writeInt(u16, &tmp, @truncate(v), .little);
                        try buf.appendSlice(alloc, &tmp);
                    }
                } else if (std.mem.eql(u8, wire_type, "UInt32")) {
                    try putString(&buf, alloc, "UInt32");
                    try buf.append(alloc, 0);
                    for (0..num_rows) |r| {
                        const v: u64 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.uint64[r];
                        var tmp: [4]u8 = undefined;
                        std.mem.writeInt(u32, &tmp, @truncate(v), .little);
                        try buf.appendSlice(alloc, &tmp);
                    }
                } else {
                    try putString(&buf, alloc, "UInt64");
                    try buf.append(alloc, 0);
                    for (0..num_rows) |r| {
                        const v: u64 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.uint64[r];
                        var tmp: [8]u8 = undefined;
                        std.mem.writeInt(u64, &tmp, v, .little);
                        try buf.appendSlice(alloc, &tmp);
                    }
                }
            },
            .float64 => {
                try putString(&buf, alloc, "Float64");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: f64 = if (core.chunk.isNull(col.null_mask, r)) 0.0 else col.data.float64[r];
                    const bits: u64 = @bitCast(v);
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(u64, &tmp, bits, .little);
                    try buf.appendSlice(alloc, &tmp);
                }
            },
            .date_u16 => {
                try putString(&buf, alloc, "Date");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: u16 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.date_u16[r];
                    var tmp: [2]u8 = undefined;
                    std.mem.writeInt(u16, &tmp, v, .little);
                    try buf.appendSlice(alloc, &tmp);
                }
            },
            .datetime64_ms => {
                try putString(&buf, alloc, "DateTime64(3)");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: i64 = if (core.chunk.isNull(col.null_mask, r)) 0 else col.data.datetime64_ms[r];
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(i64, &tmp, v, .little);
                    try buf.appendSlice(alloc, &tmp);
                }
            },
            .string => {
                try putString(&buf, alloc, "String");
                try buf.append(alloc, 0);
                for (0..num_rows) |r| {
                    const v: []const u8 = if (core.chunk.isNull(col.null_mask, r)) "" else col.data.string[r];
                    try putString(&buf, alloc, v);
                }
            },
            .array_string => {
                try putString(&buf, alloc, "Array(String)");
                try buf.append(alloc, 0);
                var cumulative: u64 = 0;
                var offsets_buf: std.ArrayListUnmanaged(u8) = .empty;
                defer offsets_buf.deinit(alloc);
                var elems_buf: std.ArrayListUnmanaged(u8) = .empty;
                defer elems_buf.deinit(alloc);

                for (0..num_rows) |r| {
                    const arr: [][]const u8 = if (core.chunk.isNull(col.null_mask, r))
                        &.{}
                    else
                        col.data.array_string[r];
                    cumulative += arr.len;
                    var tmp: [8]u8 = undefined;
                    std.mem.writeInt(u64, &tmp, cumulative, .little);
                    try offsets_buf.appendSlice(alloc, &tmp);
                    for (arr) |elem| try putString(&elems_buf, alloc, elem);
                }
                try buf.appendSlice(alloc, offsets_buf.items);
                try buf.appendSlice(alloc, elems_buf.items);
            },
        }
    }

    return buf.toOwnedSlice(alloc);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "toNativeBlock empty ResultSet" {
    const alloc = std.testing.allocator;
    var sink = core.ResultSink.init(alloc);
    var rs = try sink.finish();
    defer rs.deinit();
    const nb = try toNativeBlock(alloc, rs);
    defer alloc.free(nb);
    try std.testing.expect(nb.len >= 8); // block info (8 bytes) + num_cols + num_rows
}

test "toNativeBlock uint64 column" {
    const alloc = std.testing.allocator;
    var sink = core.ResultSink.init(alloc);

    var b = core.ChunkBuilder.init(alloc, 2);
    const ci = try b.addColumn("cnt", .uint64);
    b.chunk.columns[ci].data.uint64[0] = 42;
    b.chunk.columns[ci].data.uint64[1] = 99;
    try sink.consume(b.finish());

    var rs = try sink.finish();
    defer rs.deinit();
    const nb = try toNativeBlock(alloc, rs);
    defer alloc.free(nb);
    try std.testing.expect(std.mem.indexOf(u8, nb, "UInt64") != null);
    try std.testing.expect(std.mem.indexOf(u8, nb, "cnt") != null);
}

test "csvToResultSet basic int and string" {
    const alloc = std.testing.allocator;
    const csv = "count,name\n42,foo\n99,bar\n";
    var rs = try csvToResultSet(alloc, csv, null);
    defer rs.deinit();
    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(@as(usize, 2), rs.numCols());
    // "count" col: both positive ints → uint64
    try std.testing.expectEqual(core.ColumnType.uint64, rs.metas[0].col_type);
    try std.testing.expectEqual(@as(?core.Value, .{ .uint64 = 42 }), rs.get(0, 0));
    // "name" col: strings
    try std.testing.expectEqual(core.ColumnType.string, rs.metas[1].col_type);
    try std.testing.expectEqualStrings("foo", rs.get(1, 0).?.string);
}

test "csvToResultSet uint8 sentinel" {
    const alloc = std.testing.allocator;
    const csv = "\x03U8:is_hit\n1\n0\n";
    var rs = try csvToResultSet(alloc, csv, null);
    defer rs.deinit();
    try std.testing.expectEqual(@as(usize, 2), rs.num_rows);
    try std.testing.expectEqual(core.ColumnType.bool_u8, rs.metas[0].col_type);
    try std.testing.expectEqual(@as(?core.Value, .{ .bool_u8 = 1 }), rs.get(0, 0));
}

test "csvToResultSet empty csv" {
    const alloc = std.testing.allocator;
    var rs = try csvToResultSet(alloc, "", null);
    defer rs.deinit();
    try std.testing.expectEqual(@as(usize, 0), rs.num_rows);
}
