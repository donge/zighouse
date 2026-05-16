/// ClickHouse RowBinary / RowBinaryWithNamesAndTypes decoder.
///
/// RowBinary format (per row, in schema column order):
///   Int16      — 2 bytes LE signed
///   Int32      — 4 bytes LE signed
///   Int64      — 8 bytes LE signed
///   Date       — 2 bytes LE UInt16 (days since 1970-01-01)
///   DateTime   — 4 bytes LE UInt32 (unix seconds)
///   String     — varUInt(len) + raw bytes
///
/// RowBinaryWithNamesAndTypes prefix (before the rows):
///   varUInt(num_columns)
///   for each column: varUInt(name_len) + name_bytes
///   for each column: varUInt(type_len) + type_bytes
///
/// Usage (RowBinary — schema must be known ahead of time):
///   var dec = try RowBinaryDecoder.init(allocator, schema_table);
///   defer dec.deinit();
///   const n = try dec.decode(raw_bytes);
///
/// Usage (RowBinaryWithNamesAndTypes — schema extracted from payload):
///   const result = try decodeWithHeader(allocator, raw_bytes);
///   defer result.deinit(allocator);
///   // result.table  — inferred schema.Table (name = "")
///   // result.decoder.columns — decoded column buffers

const std = @import("std");
const schema = @import("schema");

pub const MAX_STRING_LEN: usize = 128 * 1024 * 1024;

/// Per-column accumulation buffer.
pub const ColumnBuffer = struct {
    col: schema.Column,
    /// Fixed-width columns: i64 values (all fixed types widened to i64 internally).
    fixed_vals: std.ArrayListUnmanaged(i64),
    /// String columns: slices into str_bytes (no separate allocation per value).
    str_vals: std.ArrayListUnmanaged([]const u8),
    /// Backing store for string bytes.
    str_bytes: std.ArrayListUnmanaged(u8),

    fn init(col: schema.Column) ColumnBuffer {
        return .{
            .col = col,
            .fixed_vals = .empty,
            .str_vals = .empty,
            .str_bytes = .empty,
        };
    }

    /// Return an empty ColumnBuffer for the given column (no data).
    pub fn initEmpty(col: schema.Column) ColumnBuffer {
        return init(col);
    }

    pub fn deinit(self: *ColumnBuffer, allocator: std.mem.Allocator) void {
        self.fixed_vals.deinit(allocator);
        self.str_vals.deinit(allocator);
        self.str_bytes.deinit(allocator);
    }

    pub fn rowCount(self: *const ColumnBuffer) usize {
        return switch (self.col.ty) {
            .text, .char => self.str_vals.items.len,
            else => self.fixed_vals.items.len,
        };
    }
};

/// Decodes a complete RowBinary buffer into per-column buffers.
/// All bytes must be available at once (no streaming mid-row).
pub const RowBinaryDecoder = struct {
    allocator: std.mem.Allocator,
    table: schema.Table,
    columns: []ColumnBuffer,

    pub fn init(allocator: std.mem.Allocator, table: schema.Table) !RowBinaryDecoder {
        const columns = try allocator.alloc(ColumnBuffer, table.columns.len);
        for (table.columns, columns) |col, *buf| {
            buf.* = ColumnBuffer.init(col);
        }
        return .{
            .allocator = allocator,
            .table = table,
            .columns = columns,
        };
    }

    pub fn deinit(self: *RowBinaryDecoder) void {
        for (self.columns) |*col| col.deinit(self.allocator);
        self.allocator.free(self.columns);
    }

    /// Reset buffers for reuse without re-allocating.
    pub fn reset(self: *RowBinaryDecoder) void {
        for (self.columns) |*col| {
            col.fixed_vals.items.len = 0;
            col.str_vals.items.len = 0;
            col.str_bytes.items.len = 0;
        }
    }

    /// Decode a complete RowBinary payload.
    /// Appends decoded values into self.columns.
    /// Returns the number of rows decoded.
    pub fn decode(self: *RowBinaryDecoder, data: []const u8) !usize {
        var pos: usize = 0;
        var rows: usize = 0;

        while (pos < data.len) {
            for (self.table.columns, self.columns) |col, *buf| {
                switch (col.ty) {
                    .int8 => {
                        if (pos + 1 > data.len) return error.UnexpectedEndOfData;
                        const v = @as(i8, @bitCast(data[pos]));
                        try buf.fixed_vals.append(self.allocator, @as(i64, v));
                        pos += 1;
                    },
                    .int16 => {
                        if (pos + 2 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i16, data[pos..][0..2], .little);
                        try buf.fixed_vals.append(self.allocator, @as(i64, v));
                        pos += 2;
                    },
                    .int32, .date => {
                        if (pos + 4 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i32, data[pos..][0..4], .little);
                        try buf.fixed_vals.append(self.allocator, @as(i64, v));
                        pos += 4;
                    },
                    .int64, .timestamp => {
                        if (pos + 8 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i64, data[pos..][0..8], .little);
                        try buf.fixed_vals.append(self.allocator, v);
                        pos += 8;
                    },
                    // Float32: 4 bytes IEEE 754. Store raw bits sign-extended to i64.
                    .float32 => {
                        if (pos + 4 > data.len) return error.UnexpectedEndOfData;
                        const bits = std.mem.readInt(u32, data[pos..][0..4], .little);
                        try buf.fixed_vals.append(self.allocator, @as(i64, bits));
                        pos += 4;
                    },
                    // Float64: 8 bytes IEEE 754. Store raw bits as i64.
                    .float64 => {
                        if (pos + 8 > data.len) return error.UnexpectedEndOfData;
                        const bits = std.mem.readInt(u64, data[pos..][0..8], .little);
                        try buf.fixed_vals.append(self.allocator, @bitCast(bits));
                        pos += 8;
                    },
                    .text, .char => {
                        const len, const var_bytes = readVarUInt(data[pos..]) orelse
                            return error.UnexpectedEndOfData;
                        pos += var_bytes;
                        if (len > MAX_STRING_LEN) return error.StringTooLong;
                        if (pos + len > data.len) return error.UnexpectedEndOfData;
                        const start = buf.str_bytes.items.len;
                        try buf.str_bytes.appendSlice(self.allocator, data[pos..][0..len]);
                        try buf.str_vals.append(self.allocator, buf.str_bytes.items[start..]);
                        pos += len;
                    },
                }
            }
            rows += 1;
        }

        return rows;
    }
};

/// Read a ClickHouse varUInt from `buf`.
/// Returns .{value, bytes_consumed} or null if buffer is too short.
pub fn readVarUInt(buf: []const u8) ?struct { usize, usize } {
    var result: usize = 0;
    var shift: u6 = 0;
    var i: usize = 0;
    while (i < buf.len and i < 9) : (i += 1) {
        const b = buf[i];
        result |= @as(usize, b & 0x7F) << shift;
        shift += 7;
        if (b & 0x80 == 0) return .{ result, i + 1 };
    }
    return null;
}

// ── RowBinaryWithNamesAndTypes ─────────────────────────────────────────────────

pub const WithHeaderResult = struct {
    table: schema.Table,
    decoder: RowBinaryDecoder,

    pub fn deinit(self: *WithHeaderResult, allocator: std.mem.Allocator) void {
        for (self.table.columns) |col| {
            allocator.free(col.name);
            if (col.ch_type) |ct| allocator.free(ct);
        }
        allocator.free(self.table.columns);
        self.decoder.deinit();
    }
};

/// Parse a RowBinaryWithNamesAndTypes payload.
/// Reads the header (column names + types) then decodes the row data.
/// The returned table name is empty string ""; caller should set it.
/// Caller must call result.deinit(allocator).
pub fn decodeWithHeader(allocator: std.mem.Allocator, data: []const u8) !WithHeaderResult {
    var pos: usize = 0;

    // num_columns
    const num_cols, const nc_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
    pos += nc_bytes;
    if (num_cols == 0) return error.NoColumnsInHeader;

    // Read column names
    const col_names = try allocator.alloc([]u8, num_cols);
    var names_read: usize = 0;
    errdefer {
        for (col_names[0..names_read]) |n| allocator.free(n);
        allocator.free(col_names);
    }
    for (col_names) |*name| {
        const len, const lb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += lb;
        if (pos + len > data.len) return error.UnexpectedEndOfData;
        name.* = try allocator.dupe(u8, data[pos .. pos + len]);
        names_read += 1;
        pos += len;
    }

    // Read column types → schema.ColumnType
    const col_types = try allocator.alloc(schema.ColumnType, num_cols);
    defer allocator.free(col_types);

    for (col_types) |*ty| {
        const len, const lb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += lb;
        if (pos + len > data.len) return error.UnexpectedEndOfData;
        const type_str = data[pos .. pos + len];
        pos += len;
        ty.* = parseChType(type_str) orelse return error.UnsupportedColumnType;
    }

    // Build schema.Table columns
    const columns = try allocator.alloc(schema.Column, num_cols);
    errdefer allocator.free(columns);
    for (columns, col_names, col_types) |*col, name, ty| {
        col.* = .{ .name = name, .ty = ty };
    }
    allocator.free(col_names); // slice itself (names owned by columns now)

    const table = schema.Table{ .name = "", .columns = columns };

    // Decode row data (remainder of buffer)
    var dec = try RowBinaryDecoder.init(allocator, table);
    errdefer dec.deinit();
    _ = try dec.decode(data[pos..]);

    return .{ .table = table, .decoder = dec };
}

// ── Native Block decoder ──────────────────────────────────────────────────────

/// Parse a ClickHouse Native Block INSERT payload (FORMAT Native).
/// Wire format (clickhouse-go HTTP INSERT body — NO BlockInfo prefix):
///   uvarint(num_cols) uvarint(num_rows)
///   For each column:
///     string(name) string(type_name)
///     [NO custom_serialization byte — encodeRevision=0]
///     [LowCardinality columns: uint64(state_prefix=1) before data block]
///     <column data: num_rows values, columnar layout>
///
///   LowCardinality data block layout:
///     uint64(flags = updateAll | key_type)   flags: 0x600 | key_type
///     int64(dict_count) + dict_count strings
///     int64(key_count) + key_count uint8/16/32/64 indices
///
/// Note: clickhouse-go HTTP INSERT sends the block WITHOUT BlockInfo header.
/// The BlockInfo is only present in responses (server→client).
/// WriteStatePrefix IS written even at encodeRevision=0 for CustomSerialization columns.
///
/// Returns a WithHeaderResult (same shape as decodeWithHeader).
/// Caller must call result.deinit(allocator).
pub fn decodeNativeBlock(allocator: std.mem.Allocator, data: []const u8) !WithHeaderResult {
    var pos: usize = 0;

    // num_columns, num_rows
    const num_cols, const nc_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
    pos += nc_bytes;
    const num_rows, const nr_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
    pos += nr_bytes;

    if (num_cols == 0 or num_rows == 0) {
        // Empty block — return empty result
        const empty_cols = try allocator.alloc(schema.Column, 0);
        const table = schema.Table{ .name = "", .columns = empty_cols };
        const dec_cols = try allocator.alloc(ColumnBuffer, 0);
        return .{ .table = table, .decoder = .{ .allocator = allocator, .table = table, .columns = dec_cols } };
    }

    // Read column metadata + data
    const columns = try allocator.alloc(schema.Column, num_cols);
    var cols_inited: usize = 0;
    errdefer {
        for (columns[0..cols_inited]) |col| {
            allocator.free(col.name);
            if (col.ch_type) |ct| allocator.free(ct);
        }
        allocator.free(columns);
    }

    const col_bufs = try allocator.alloc(ColumnBuffer, num_cols);
    var bufs_inited: usize = 0;
    errdefer {
        for (col_bufs[0..bufs_inited]) |*cb| cb.deinit(allocator);
        allocator.free(col_bufs);
    }

    for (0..num_cols) |ci| {
        // Column name
        const name_len, const nl_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += nl_bytes;
        if (pos + name_len > data.len) return error.UnexpectedEndOfData;
        const name_owned = try allocator.dupe(u8, data[pos .. pos + name_len]);
        pos += name_len;
        var name_transferred = false;
        errdefer if (!name_transferred) allocator.free(name_owned);

        // Column type name
        const type_len, const tl_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        pos += tl_bytes;
        if (pos + type_len > data.len) return error.UnexpectedEndOfData;
        const type_str = data[pos .. pos + type_len];
        pos += type_len;

        const is_low_cardinality = chTypeStartsWith(type_str, "LowCardinality(");
        const col_ty = parseChType(type_str) orelse return error.UnsupportedColumnType;

        // NOTE: clickhouse-go HTTP INSERT uses encodeRevision=0, which means
        // the custom_serialization byte is NOT written (revision 0 < DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION=54454).
        // However, WriteStatePrefix IS written for CustomSerialization columns (LowCardinality)
        // regardless of revision. So we must read the state prefix uint64 for LowCardinality.

        if (is_low_cardinality) {
            // Read state prefix: uint64 = 1 (sharedDictionariesWithAdditionalKeys)
            if (pos + 8 > data.len) return error.UnexpectedEndOfData;
            pos += 8; // skip the state prefix value (we don't validate it)
        }

        // Dupe the original CH type string so we can persist it in the schema.
        const ch_type_owned = try allocator.dupe(u8, type_str);
        errdefer allocator.free(ch_type_owned);

        columns[ci] = .{ .name = name_owned, .ty = col_ty, .ch_type = ch_type_owned };
        name_transferred = true;
        cols_inited += 1;

        col_bufs[ci] = ColumnBuffer.init(columns[ci]);
        bufs_inited += 1;

        if (is_low_cardinality) {
            // LowCardinality block layout (from LowCardinality.Encode):
            //   uint64  flags = updateAll | key_type  (updateAll = 0x600, key_type = 0/1/2/3)
            //   int64   dict_count
            //   dict_count strings (varUInt len + bytes each)
            //   int64   key_count  (= num_rows)
            //   key_count indices (uint8/16/32/64 depending on key_type)
            if (pos + 8 > data.len) return error.UnexpectedEndOfData;
            const flags = std.mem.readInt(u64, data[pos..][0..8], .little);
            pos += 8;
            const key_type: u2 = @truncate(flags & 0xFF);

            // dict_count
            if (pos + 8 > data.len) return error.UnexpectedEndOfData;
            const dict_count = std.mem.readInt(i64, data[pos..][0..8], .little);
            pos += 8;

            // Build dictionary: array of strings indexed by key value
            var dict = try allocator.alloc([]const u8, @intCast(dict_count));
            defer allocator.free(dict);

            for (0..@intCast(dict_count)) |di| {
                const slen, const sl_bytes = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
                pos += sl_bytes;
                if (slen > MAX_STRING_LEN) return error.StringTooLong;
                if (pos + slen > data.len) return error.UnexpectedEndOfData;
                dict[di] = data[pos .. pos + slen];
                pos += slen;
            }

            // key_count
            if (pos + 8 > data.len) return error.UnexpectedEndOfData;
            const key_count = std.mem.readInt(i64, data[pos..][0..8], .little);
            pos += 8;

            // Decode indices and expand to strings.
            // We collect (start, len) offsets into str_bytes first, then fix up
            // str_vals pointers AFTER all appends to avoid realloc-invalidation.
            const key_size: usize = switch (key_type) {
                0 => 1, // UInt8
                1 => 2, // UInt16
                2 => 4, // UInt32
                3 => 8, // UInt64
            };
            // Pre-reserve str_bytes to prevent reallocation after slices are taken.
            var total_str_bytes: usize = 0;
            {
                var scan_pos = pos;
                for (0..@intCast(key_count)) |_| {
                    if (scan_pos + key_size > data.len) break;
                    const k_idx: usize = switch (key_type) {
                        0 => @as(usize, data[scan_pos]),
                        1 => @as(usize, std.mem.readInt(u16, data[scan_pos..][0..2], .little)),
                        2 => @as(usize, std.mem.readInt(u32, data[scan_pos..][0..4], .little)),
                        3 => @as(usize, std.mem.readInt(u64, data[scan_pos..][0..8], .little)),
                    };
                    scan_pos += key_size;
                    const sv = if (k_idx < dict.len) dict[k_idx] else "";
                    total_str_bytes += sv.len;
                }
            }
            try col_bufs[ci].str_bytes.ensureTotalCapacity(
                allocator,
                col_bufs[ci].str_bytes.items.len + total_str_bytes,
            );
            for (0..@intCast(key_count)) |_| {
                if (pos + key_size > data.len) return error.UnexpectedEndOfData;
                const idx: usize = switch (key_type) {
                    0 => @as(usize, data[pos]),
                    1 => @as(usize, std.mem.readInt(u16, data[pos..][0..2], .little)),
                    2 => @as(usize, std.mem.readInt(u32, data[pos..][0..4], .little)),
                    3 => @as(usize, std.mem.readInt(u64, data[pos..][0..8], .little)),
                };
                pos += key_size;
                const str_val = if (idx < dict.len) dict[idx] else "";
                const start = col_bufs[ci].str_bytes.items.len;
                // appendSlice will NOT reallocate because we pre-reserved capacity above.
                try col_bufs[ci].str_bytes.appendSlice(allocator, str_val);
                try col_bufs[ci].str_vals.append(allocator, col_bufs[ci].str_bytes.items[start..]);
            }
        } else {
            // Decode num_rows values for this column (columnar layout).
            // String columns are handled separately to allow pre-reserving capacity.
            if (col_ty == .text or col_ty == .char) {
                try consumeNativeTextRows(allocator, type_str, data, &pos, num_rows, &col_bufs[ci]);
            } else {
            for (0..num_rows) |_| {
                switch (col_ty) {
                    .int8 => {
                        if (pos + 1 > data.len) return error.UnexpectedEndOfData;
                        const v = @as(i8, @bitCast(data[pos]));
                        try col_bufs[ci].fixed_vals.append(allocator, @as(i64, v));
                        pos += 1;
                    },
                    .int16 => {
                        if (pos + 2 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i16, data[pos..][0..2], .little);
                        try col_bufs[ci].fixed_vals.append(allocator, @as(i64, v));
                        pos += 2;
                    },
                    .int32, .date => {
                        if (pos + 4 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i32, data[pos..][0..4], .little);
                        try col_bufs[ci].fixed_vals.append(allocator, @as(i64, v));
                        pos += 4;
                    },
                    .int64, .timestamp => {
                        if (pos + 8 > data.len) return error.UnexpectedEndOfData;
                        const v = std.mem.readInt(i64, data[pos..][0..8], .little);
                        try col_bufs[ci].fixed_vals.append(allocator, v);
                        pos += 8;
                    },
                    .float32 => {
                        if (pos + 4 > data.len) return error.UnexpectedEndOfData;
                        const bits = std.mem.readInt(u32, data[pos..][0..4], .little);
                        try col_bufs[ci].fixed_vals.append(allocator, @as(i64, bits));
                        pos += 4;
                    },
                    .float64 => {
                        if (pos + 8 > data.len) return error.UnexpectedEndOfData;
                        const bits = std.mem.readInt(u64, data[pos..][0..8], .little);
                        try col_bufs[ci].fixed_vals.append(allocator, @bitCast(bits));
                        pos += 8;
                    },
                    .text, .char => unreachable, // handled above
                }
            }
            } // end else fixed
        }
    }

    const table = schema.Table{ .name = "", .columns = columns };
    return .{
        .table = table,
        .decoder = .{ .allocator = allocator, .table = table, .columns = col_bufs },
    };
}

/// Map a ClickHouse type string to our schema.ColumnType.
/// Handles Nullable(T) and LowCardinality(T) wrappers.
fn parseChType(s: []const u8) ?schema.ColumnType {
    if (chTypeEql(s, "Int8")) return .int8;
    if (chTypeEql(s, "Int16")) return .int16;
    if (chTypeEql(s, "Int32")) return .int32;
    if (chTypeEql(s, "Int64")) return .int64;
    if (chTypeEql(s, "UInt8"))  return .int8;
    if (chTypeEql(s, "UInt16")) return .int16;
    if (chTypeEql(s, "UInt32")) return .int32;
    if (chTypeEql(s, "UInt64")) return .int64;
    if (chTypeEql(s, "Date")) return .date;
    if (chTypeEql(s, "Date32")) return .date;
    if (chTypeEql(s, "DateTime")) return .timestamp;
    if (chTypeEql(s, "String")) return .text;
    if (chTypeEql(s, "FixedString")) return .text;
    if (chTypeEql(s, "IPv4")) return .text;
    if (chTypeEql(s, "IPv6")) return .text;
    if (chTypeEql(s, "Float32")) return .float32;
    if (chTypeEql(s, "Float64")) return .float64;
    // Nullable(T) / LowCardinality(T)
    if (chTypeStartsWith(s, "Nullable(") or chTypeStartsWith(s, "LowCardinality(")) {
        const inner = extractInner(s);
        return parseChType(inner);
    }
    // Array(...) / Map(...) → text blob
    if (chTypeStartsWith(s, "Array(") or chTypeStartsWith(s, "Map(")) return .text;
    // DateTime64(precision) / DateTime64(precision, tz)
    if (chTypeStartsWith(s, "DateTime64")) return .timestamp;
    // FixedString(N)
    if (chTypeStartsWith(s, "FixedString(")) return .text;
    return null;
}

fn chTypeEql(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    return std.mem.eql(u8, a, b);
}

fn chTypeStartsWith(s: []const u8, prefix: []const u8) bool {
    return std.mem.startsWith(u8, s, prefix);
}

fn extractInner(s: []const u8) []const u8 {
    const lp = std.mem.indexOfScalar(u8, s, '(') orelse return s;
    const rp = std.mem.lastIndexOfScalar(u8, s, ')') orelse return s;
    if (rp > lp) return s[lp + 1 .. rp];
    return s;
}

/// Return the fixed byte width for a CH type that maps to .text, or null if variable-length.
/// Used by consumeNativeTextRows to decide how to consume each row.
fn nativeTextFixedWidth(type_str: []const u8) ?usize {
    if (chTypeEql(type_str, "IPv4")) return 4;
    if (chTypeEql(type_str, "IPv6")) return 16;
    // Numeric fixed-width types (used as Map values, etc.)
    if (chTypeEql(type_str, "UInt8")  or chTypeEql(type_str, "Int8"))  return 1;
    if (chTypeEql(type_str, "UInt16") or chTypeEql(type_str, "Int16")) return 2;
    if (chTypeEql(type_str, "UInt32") or chTypeEql(type_str, "Int32")  or
        chTypeEql(type_str, "Float32") or chTypeEql(type_str, "Date") or
        chTypeEql(type_str, "Date32")) return 4;
    if (chTypeEql(type_str, "UInt64") or chTypeEql(type_str, "Int64")  or
        chTypeEql(type_str, "Float64") or chTypeEql(type_str, "DateTime") or
        chTypeStartsWith(type_str, "DateTime64")) return 8;
    if (chTypeStartsWith(type_str, "FixedString(")) {
        // e.g. "FixedString(36)"
        const inner = extractInner(type_str);
        return std.fmt.parseInt(usize, inner, 10) catch null;
    }
    return null; // variable-length: String, Array(...), Map(...)
}

/// Skip one Native-block value for a .text column, returning the raw bytes as a blob.
/// For fixed-size types (IPv4=4, IPv6=16, FixedString(N)=N): read N raw bytes.
/// For String: read varUInt(len) + len bytes.
/// For Array(T): read varUInt(count) elements recursively.
/// For Map(K,V): read varUInt(count) key+value pairs recursively.
/// The raw bytes (including any length prefixes) are returned as a slice into `data`.
fn measureNativeValue(type_str: []const u8, data: []const u8, pos: usize) !usize {
    if (nativeTextFixedWidth(type_str)) |w| {
        if (pos + w > data.len) return error.UnexpectedEndOfData;
        return w;
    }
    // String
    if (chTypeEql(type_str, "String") or
        chTypeStartsWith(type_str, "Nullable(") or
        chTypeEql(type_str, "UUID"))
    {
        const len, const lb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        if (pos + lb + len > data.len) return error.UnexpectedEndOfData;
        return lb + len;
    }
    // Array(T)
    if (chTypeStartsWith(type_str, "Array(")) {
        const inner = extractInner(type_str);
        const count, const cb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        var off = pos + cb;
        for (0..count) |_| {
            const sz = try measureNativeValue(inner, data, off);
            off += sz;
        }
        return off - pos;
    }
    // Map(K, V)
    if (chTypeStartsWith(type_str, "Map(")) {
        // Find the key type and value type by splitting on first comma at depth 0
        const inner = extractInner(type_str);
        var ktype: []const u8 = inner;
        var vtype: []const u8 = "";
        {
            var depth: usize = 0;
            for (inner, 0..) |c, i| {
                if (c == '(') depth += 1
                else if (c == ')') depth -= 1
                else if (c == ',' and depth == 0) {
                    ktype = std.mem.trim(u8, inner[0..i], " ");
                    vtype = std.mem.trim(u8, inner[i+1..], " ");
                    break;
                }
            }
        }
        const count, const cb = readVarUInt(data[pos..]) orelse return error.UnexpectedEndOfData;
        var off = pos + cb;
        for (0..count) |_| {
            off += try measureNativeValue(ktype, data, off);
            off += try measureNativeValue(vtype, data, off);
        }
        return off - pos;
    }
    // Tuple / other: unsupported, return error
    return error.UnsupportedColumnType;
}

/// Decode `num_rows` values from a Native Block for a .text-mapped column.
/// Stores each row's raw bytes as a blob in col_buf.str_bytes / str_vals.
/// For plain String: stores only the content bytes (no length prefix).
/// For Array(T) columns in Native format: offsets[N]uint64 + flat element data.
/// For Map(K,V) columns in Native format: offsets[N]uint64 + flat keys + flat values.
/// For IPv4/IPv6/FixedString: stores fixed-width raw bytes.
fn consumeNativeTextRows(
    allocator: std.mem.Allocator,
    type_str: []const u8,
    data: []const u8,
    pos: *usize,
    num_rows: usize,
    col_buf: *ColumnBuffer,
) !void {
    // Plain String / UUID: read varUInt(len) + content; store ONLY content.
    const is_plain_string = chTypeEql(type_str, "String") or chTypeEql(type_str, "UUID");
    if (is_plain_string) {
        {
            var scan = pos.*;
            var total: usize = 0;
            for (0..num_rows) |_| {
                const l, const lb = readVarUInt(data[scan..]) orelse break;
                scan += lb + l;
                total += l;
            }
            try col_buf.str_bytes.ensureTotalCapacity(
                allocator, col_buf.str_bytes.items.len + total,
            );
        }
        for (0..num_rows) |_| {
            const len, const var_bytes = readVarUInt(data[pos.*..]) orelse
                return error.UnexpectedEndOfData;
            pos.* += var_bytes;
            if (len > MAX_STRING_LEN) return error.StringTooLong;
            if (pos.* + len > data.len) return error.UnexpectedEndOfData;
            const start = col_buf.str_bytes.items.len;
            try col_buf.str_bytes.appendSlice(allocator, data[pos.*..][0..len]);
            try col_buf.str_vals.append(allocator, col_buf.str_bytes.items[start..]);
            pos.* += len;
        }
        return;
    }

    // Array(T) columns in ClickHouse Native format use offset-based encoding:
    //   uint64[num_rows] cumulative end-offsets into element array
    //   element data (all rows concatenated)
    if (chTypeStartsWith(type_str, "Array(")) {
        const inner = extractInner(type_str);
        if (pos.* + num_rows * 8 > data.len) return error.UnexpectedEndOfData;
        const offsets_start = pos.*;
        pos.* += num_rows * 8;
        // Measure element positions using offsets
        var ep = pos.*; // start of element data
        var prev_off: u64 = 0;
        for (0..num_rows) |i| {
            const off = std.mem.readInt(u64, data[offsets_start + i * 8..][0..8], .little);
            const count = off - prev_off;
            const row_start = ep;
            for (0..count) |_| ep += try measureNativeValue(inner, data, ep);
            const start = col_buf.str_bytes.items.len;
            try col_buf.str_bytes.appendSlice(allocator, data[row_start..ep]);
            try col_buf.str_vals.append(allocator, col_buf.str_bytes.items[start..]);
            prev_off = off;
        }
        pos.* = ep;
        return;
    }

    // Map(K,V) columns in ClickHouse Native format:
    //   uint64[num_rows] cumulative end-offsets into pair array
    //   key data (all rows concatenated)
    //   value data (all rows concatenated)
    if (chTypeStartsWith(type_str, "Map(")) {
        const inner = extractInner(type_str);
        var ktype: []const u8 = inner;
        var vtype: []const u8 = "";
        {
            var depth: usize = 0;
            for (inner, 0..) |c, ii| {
                if (c == '(') depth += 1
                else if (c == ')') depth -= 1
                else if (c == ',' and depth == 0) {
                    ktype = std.mem.trim(u8, inner[0..ii], " ");
                    vtype = std.mem.trim(u8, inner[ii+1..], " ");
                    break;
                }
            }
        }
        if (pos.* + num_rows * 8 > data.len) return error.UnexpectedEndOfData;
        const offsets_start = pos.*;
        pos.* += num_rows * 8;
        const total_pairs: u64 = if (num_rows > 0)
            std.mem.readInt(u64, data[offsets_start + (num_rows - 1) * 8..][0..8], .little)
        else 0;
        // Advance through all keys
        const keys_start = pos.*;
        for (0..total_pairs) |_| pos.* += try measureNativeValue(ktype, data, pos.*);
        const vals_start = pos.*;
        for (0..total_pairs) |_| pos.* += try measureNativeValue(vtype, data, pos.*);
        const vals_end = pos.*;
        // Re-traverse per row to build per-row blobs
        var kp = keys_start;
        var vp = vals_start;
        var prev_off2: u64 = 0;
        for (0..num_rows) |i| {
            const off = std.mem.readInt(u64, data[offsets_start + i * 8..][0..8], .little);
            const count = off - prev_off2;
            const k_row_start = kp;
            const v_row_start = vp;
            for (0..count) |_| {
                kp += try measureNativeValue(ktype, data, kp);
                vp += try measureNativeValue(vtype, data, vp);
            }
            const start = col_buf.str_bytes.items.len;
            try col_buf.str_bytes.appendSlice(allocator, data[k_row_start..kp]);
            try col_buf.str_bytes.appendSlice(allocator, data[v_row_start..vp]);
            try col_buf.str_vals.append(allocator, col_buf.str_bytes.items[start..]);
            prev_off2 = off;
        }
        pos.* = vals_end;
        return;
    }

    // Fixed-width types (IPv4=4, IPv6=16, FixedString(N)=N): raw blobs.
    var total: usize = 0;
    {
        var scan = pos.*;
        for (0..num_rows) |_| {
            const sz = measureNativeValue(type_str, data, scan) catch break;
            scan += sz;
            total += sz;
        }
    }
    try col_buf.str_bytes.ensureTotalCapacity(allocator, col_buf.str_bytes.items.len + total);

    for (0..num_rows) |_| {
        const sz = try measureNativeValue(type_str, data, pos.*);
        const start = col_buf.str_bytes.items.len;
        try col_buf.str_bytes.appendSlice(allocator, data[pos.*..][0..sz]);
        try col_buf.str_vals.append(allocator, col_buf.str_bytes.items[start..]);
        pos.* += sz;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "decode: two rows Int32 + String" {
    const allocator = std.testing.allocator;

    const table = schema.Table{
        .name = "t",
        .columns = &.{
            .{ .name = "id", .ty = .int32 },
            .{ .name = "name", .ty = .text },
        },
    };

    // Build RowBinary manually: row1=(42,"hello"), row2=(100,"world")
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    // row1: id=42
    std.mem.writeInt(i32, buf[pos..][0..4], 42, .little);
    pos += 4;
    // row1: name="hello" (varUInt 5 + bytes)
    buf[pos] = 5; pos += 1;
    @memcpy(buf[pos..][0..5], "hello"); pos += 5;
    // row2: id=100
    std.mem.writeInt(i32, buf[pos..][0..4], 100, .little);
    pos += 4;
    // row2: name="world"
    buf[pos] = 5; pos += 1;
    @memcpy(buf[pos..][0..5], "world"); pos += 5;
    const data = buf[0..pos];

    var dec = try RowBinaryDecoder.init(allocator, table);
    defer dec.deinit();

    const rows = try dec.decode(data);
    try std.testing.expectEqual(@as(usize, 2), rows);
    try std.testing.expectEqual(@as(i64, 42), dec.columns[0].fixed_vals.items[0]);
    try std.testing.expectEqual(@as(i64, 100), dec.columns[0].fixed_vals.items[1]);
    try std.testing.expectEqualSlices(u8, "hello", dec.columns[1].str_vals.items[0]);
    try std.testing.expectEqualSlices(u8, "world", dec.columns[1].str_vals.items[1]);
}

test "readVarUInt: single byte" {
    const r = readVarUInt(&.{0x05}).?;
    try std.testing.expectEqual(@as(usize, 5), r.@"0");
    try std.testing.expectEqual(@as(usize, 1), r.@"1");
}

test "readVarUInt: two bytes (300)" {
    const r = readVarUInt(&.{ 0xAC, 0x02 }).?;
    try std.testing.expectEqual(@as(usize, 300), r.@"0");
    try std.testing.expectEqual(@as(usize, 2), r.@"1");
}

test "decodeWithHeader: Int32 + String two rows" {
    const allocator = std.testing.allocator;

    // Build RowBinaryWithNamesAndTypes payload manually.
    var buf: [256]u8 = undefined;
    var pos: usize = 0;

    // num_columns = 2
    buf[pos] = 2; pos += 1;
    // names: "id", "name" (all names first)
    buf[pos] = 2; pos += 1; @memcpy(buf[pos..][0..2], "id"); pos += 2;
    buf[pos] = 4; pos += 1; @memcpy(buf[pos..][0..4], "name"); pos += 4;
    // types: "Int32", "String" (all types after all names)
    buf[pos] = 5; pos += 1; @memcpy(buf[pos..][0..5], "Int32"); pos += 5;
    buf[pos] = 6; pos += 1; @memcpy(buf[pos..][0..6], "String"); pos += 6;
    // row1: id=7, name="alice"
    std.mem.writeInt(i32, buf[pos..][0..4], 7, .little); pos += 4;
    buf[pos] = 5; pos += 1; @memcpy(buf[pos..][0..5], "alice"); pos += 5;
    // row2: id=8, name="bob"
    std.mem.writeInt(i32, buf[pos..][0..4], 8, .little); pos += 4;
    buf[pos] = 3; pos += 1; @memcpy(buf[pos..][0..3], "bob"); pos += 3;

    var result = try decodeWithHeader(allocator, buf[0..pos]);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.table.columns.len);
    try std.testing.expectEqualStrings("id",   result.table.columns[0].name);
    try std.testing.expectEqual(schema.ColumnType.int32, result.table.columns[0].ty);
    try std.testing.expectEqualStrings("name", result.table.columns[1].name);
    try std.testing.expectEqual(schema.ColumnType.text,  result.table.columns[1].ty);
    try std.testing.expectEqual(@as(usize, 2), result.decoder.columns[0].fixed_vals.items.len);
    try std.testing.expectEqual(@as(i64, 7), result.decoder.columns[0].fixed_vals.items[0]);
    try std.testing.expectEqual(@as(i64, 8), result.decoder.columns[0].fixed_vals.items[1]);
    try std.testing.expectEqualSlices(u8, "alice", result.decoder.columns[1].str_vals.items[0]);
    try std.testing.expectEqualSlices(u8, "bob",   result.decoder.columns[1].str_vals.items[1]);
}

test "decodeWithHeader: truncated header triggers errdefer (no leak)" {
    // Provide only the num_cols byte — no names follow.
    // The errdefer path must clean up without double-free or leak.
    const allocator = std.testing.allocator;
    const data = [_]u8{0x02}; // num_cols=2, then EOF
    try std.testing.expectError(error.UnexpectedEndOfData, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: partial name triggers errdefer (no leak)" {
    // num_cols=1, name_len=5 but only 3 bytes of name provided.
    const allocator = std.testing.allocator;
    const data = [_]u8{ 0x01, 0x05, 'a', 'b', 'c' };
    try std.testing.expectError(error.UnexpectedEndOfData, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: unsupported type triggers errdefer (no leak)" {
    // num_cols=1, name="x", type="Decimal(10,2)" (unsupported)
    const allocator = std.testing.allocator;
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 1; pos += 1;           // num_cols=1
    buf[pos] = 1; pos += 1; buf[pos] = 'x'; pos += 1; // name="x"
    const unsupported = "Decimal(10,2)";
    buf[pos] = unsupported.len; pos += 1;
    @memcpy(buf[pos..][0..unsupported.len], unsupported); pos += unsupported.len;
    try std.testing.expectError(error.UnsupportedColumnType, decodeWithHeader(allocator, buf[0..pos]));
}

test "decodeWithHeader: zero columns returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{0x00}; // num_cols=0
    try std.testing.expectError(error.NoColumnsInHeader, decodeWithHeader(allocator, &data));
}

test "decodeWithHeader: all Phase-1 types" {
    // Build payload: num_cols=5, names+types for all fixed types, then one row.
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;
    var pos: usize = 0;

    buf[pos] = 5; pos += 1; // num_cols
    // names (all first)
    const names = [_][]const u8{ "i16", "i32", "i64", "d", "ts" };
    for (names) |n| {
        buf[pos] = @intCast(n.len); pos += 1;
        @memcpy(buf[pos..][0..n.len], n); pos += n.len;
    }
    // types
    const types = [_][]const u8{ "Int16", "Int32", "Int64", "Date", "DateTime" };
    for (types) |t| {
        buf[pos] = @intCast(t.len); pos += 1;
        @memcpy(buf[pos..][0..t.len], t); pos += t.len;
    }
    // one row: i16=1, i32=2, i64=3, d=4 (UInt16 days), ts=5 (UInt32 secs)
    std.mem.writeInt(i16, buf[pos..][0..2], 1, .little); pos += 2;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .little); pos += 4;
    std.mem.writeInt(i64, buf[pos..][0..8], 3, .little); pos += 8;
    std.mem.writeInt(i32, buf[pos..][0..4], 4, .little); pos += 4; // Date stored as i32 in RowBinary
    std.mem.writeInt(i64, buf[pos..][0..8], 5, .little); pos += 8; // DateTime stored as i64

    var result = try decodeWithHeader(allocator, buf[0..pos]);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), result.table.columns.len);
    try std.testing.expectEqual(schema.ColumnType.int16,     result.table.columns[0].ty);
    try std.testing.expectEqual(schema.ColumnType.int32,     result.table.columns[1].ty);
    try std.testing.expectEqual(schema.ColumnType.int64,     result.table.columns[2].ty);
    try std.testing.expectEqual(schema.ColumnType.date,      result.table.columns[3].ty);
    try std.testing.expectEqual(schema.ColumnType.timestamp, result.table.columns[4].ty);
}
