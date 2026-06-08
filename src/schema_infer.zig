/// Schema inference from Parquet file metadata.
///
/// Reads the Parquet footer and derives a schema.Table whose columns reflect
/// the physical types present in the file.  The inferred schema uses only the
/// types that generic_executor and the generic store layout can handle:
///
///   Parquet INT32 (DATE)                   → .date   fixed
///   Parquet INT64 (TIMESTAMP_MILLIS/MICROS) → .timestamp fixed
///   Parquet INT32 (INT_8 / INT_16)          → .int16  fixed  (narrow)
///   Parquet INT32 (other / none)            → .int32  fixed
///   Parquet INT64 (other / none)            → .int64  fixed
///   Parquet BYTE_ARRAY (UTF8 / none)        → .text   lazy_text
///   Parquet BOOLEAN                         → .int16  fixed  (0/1)
///   Parquet FLOAT                           → .int32  fixed  (bit-cast, lossy)
///   Parquet DOUBLE                          → .int64  fixed  (bit-cast, lossy)
///   Parquet INT96                           → .timestamp fixed (96-bit ns, truncated to 64)
///   Parquet FIXED_LEN_BYTE_ARRAY            → .text   lazy_text
///
/// The table name is set from the `name` argument passed to inferSchema.
/// Column names are taken from the Parquet schema elements.
/// Nested / repeated columns (num_children != null) are skipped.
///
/// The returned Table's column slice and all names are allocated from
/// `allocator`.  Free with `freeInferredSchema`.

const std = @import("std");
const parquet = @import("parquet");
const schema = @import("schema");

// Parquet physical type constants (Thrift enum order)
const PT_BOOLEAN: i32 = 0;
const PT_INT32: i32 = 1;
const PT_INT64: i32 = 2;
const PT_INT96: i32 = 3;
const PT_FLOAT: i32 = 4;
const PT_DOUBLE: i32 = 5;
const PT_BYTE_ARRAY: i32 = 6;
const PT_FIXED_LEN_BYTE_ARRAY: i32 = 7;

// Parquet converted type constants (ConvertedType Thrift enum)
// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
const CT_UTF8: i32 = 0;
// 1 = MAP, 2 = MAP_KEY_VALUE, 3 = LIST, 4 = ENUM, 5 = DECIMAL
const CT_DATE: i32 = 6;
const CT_TIME_MILLIS: i32 = 7;
const CT_TIME_MICROS: i32 = 8;
const CT_TIMESTAMP_MILLIS: i32 = 9;
const CT_TIMESTAMP_MICROS: i32 = 10;
const CT_UINT_8: i32 = 11;
const CT_UINT_16: i32 = 12;
// 13 = UINT_32, 14 = UINT_64
const CT_INT_8: i32 = 15;
const CT_INT_16: i32 = 16;
const CT_INT_32: i32 = 17;
const CT_INT_64: i32 = 18;

/// Result of schema inference.  Free with `freeInferredSchema`.
pub const InferredSchema = struct {
    table: schema.Table,
    /// Backing allocator used for all allocations in this struct.
    allocator: std.mem.Allocator,
    /// Owned slice of Column values (table.columns points into this).
    columns: []schema.Column,
    /// Owned slice of column name strings.
    names: [][]u8,

    pub fn deinit(self: *InferredSchema) void {
        for (self.names) |n| self.allocator.free(n);
        self.allocator.free(self.names);
        self.allocator.free(self.columns);
    }
};

/// Infer a schema.Table from the Parquet file at `parquet_path`.
/// The table is named `table_name`.
/// Returns an InferredSchema whose lifetime is managed by the caller.
pub fn inferSchema(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    table_name: []const u8,
) !InferredSchema {
    var loaded = try parquet.readMetadataPath(allocator, io, parquet_path);
    defer loaded.deinit();

    const elements = loaded.meta.schema;

    // Count leaf (data) columns: skip index 0 (message root) and group nodes.
    var leaf_count: usize = 0;
    for (elements[1..]) |el| {
        if (el.num_children != null) continue; // group node
        leaf_count += 1;
    }

    const columns = try allocator.alloc(schema.Column, leaf_count);
    errdefer allocator.free(columns);

    const names = try allocator.alloc([]u8, leaf_count);
    errdefer allocator.free(names);
    var names_init: usize = 0;
    errdefer for (names[0..names_init]) |n| allocator.free(n);

    var col_idx: usize = 0;
    // Track Parquet column index (leaf columns only, same order as row groups).
    var parquet_col: usize = 0;
    for (elements[1..]) |el| {
        if (el.num_children != null) {
            // Group node: doesn't map to a column chunk, skip without incrementing parquet_col.
            // Actually in flat schemas all non-root elements are leaves; nested schemas
            // have group nodes with children.  We conservatively skip group nodes here.
            continue;
        }
        defer parquet_col += 1;

        const pt = el.type_ orelse PT_BYTE_ARRAY;
        const ct = el.converted_type;

        const name_copy = try allocator.dupe(u8, el.name);
        names[names_init] = name_copy;
        names_init += 1;

        columns[col_idx] = inferColumn(name_copy, parquet_col, pt, ct);
        col_idx += 1;
    }

    return .{
        .table = .{ .name = table_name, .columns = columns },
        .allocator = allocator,
        .columns = columns,
        .names = names,
    };
}

fn inferColumn(name: []const u8, parquet_idx: usize, pt: i32, ct: ?i32) schema.Column {
    _ = parquet_idx; // will be used by loader for column ordering

    const col_type: schema.ColumnType = blk: {
        if (pt == PT_INT32) {
            if (ct) |c| switch (c) {
                CT_DATE, CT_UINT_16 => break :blk .date,
                CT_INT_8, CT_INT_16, CT_UINT_8 => break :blk .int16,
                CT_INT_32 => break :blk .int32,
                else => {},
            };
            break :blk .int32;
        }
        if (pt == PT_INT64) {
            if (ct) |c| switch (c) {
                CT_TIMESTAMP_MILLIS, CT_TIMESTAMP_MICROS,
                CT_TIME_MILLIS, CT_TIME_MICROS => break :blk .timestamp,
                CT_INT_64 => break :blk .int64,
                else => {},
            };
            break :blk .int64;
        }
        if (pt == PT_INT96) break :blk .timestamp;
        if (pt == PT_BOOLEAN) break :blk .int16;
        if (pt == PT_FLOAT) break :blk .int32;   // bit-cast; caller beware
        if (pt == PT_DOUBLE) break :blk .int64;  // bit-cast; caller beware
        // BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, unknown → text
        break :blk .text;
    };

    const physical: schema.PhysicalColumn = switch (col_type) {
        .text, .char => .{ .lazy_text = .{ .source_column = name } },
        else => .{ .fixed = .{ .path_name = name, .ty = col_type } },
    };

    return .{
        .name = name,
        .ty = col_type,
        .storage = switch (col_type) {
            .text, .char => .lazy_source,
            else => .fixed_eager,
        },
        .physical = physical,
    };
}

// ── Schema loading from columns.txt ──────────────────────────────────────────

/// Parse a ColumnType from a string as written by ColumnBinWriter / generic_store.
/// Returns null for unrecognised type strings.
fn parseColType(s: []const u8) ?schema.ColumnType {
    if (std.mem.eql(u8, s, "int8"))      return .int8;
    if (std.mem.eql(u8, s, "int16"))     return .int16;
    if (std.mem.eql(u8, s, "int32"))     return .int32;
    if (std.mem.eql(u8, s, "int64"))     return .int64;
    if (std.mem.eql(u8, s, "date"))      return .date;
    if (std.mem.eql(u8, s, "timestamp")) return .timestamp;
    if (std.mem.eql(u8, s, "float32"))   return .float32;
    if (std.mem.eql(u8, s, "float64"))   return .float64;
    if (std.mem.eql(u8, s, "text"))      return .text;
    if (std.mem.eql(u8, s, "char"))      return .char;
    if (std.mem.eql(u8, s, "low_card"))  return .low_card;
    return null;
}

/// Load a schema.Table from a `columns.txt` file produced by the generic store.
/// Format: one `<name>\t<type>\n` line per column.
/// Returns an InferredSchema; caller frees with `result.deinit()`.
pub fn loadSchemaFromColumnsTxt(
    allocator: std.mem.Allocator,
    io: std.Io,
    columns_txt_path: []const u8,
    table_name: []const u8,
) !InferredSchema {
    var file = if (std.fs.path.isAbsolute(columns_txt_path))
        try std.Io.Dir.openFileAbsolute(io, columns_txt_path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, columns_txt_path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    const buf = try allocator.alloc(u8, stat.size);
    defer allocator.free(buf);
    _ = try file.readPositionalAll(io, buf, 0);

    // Count lines to pre-allocate.
    var line_count: usize = 0;
    var it = std.mem.splitScalar(u8, buf, '\n');
    while (it.next()) |line| {
        if (line.len > 0) line_count += 1;
    }

    const columns = try allocator.alloc(schema.Column, line_count);
    errdefer allocator.free(columns);
    const names = try allocator.alloc([]u8, line_count);
    errdefer allocator.free(names);
    var n_init: usize = 0;
    errdefer for (names[0..n_init]) |nm| allocator.free(nm);

    var ci: usize = 0;
    var it2 = std.mem.splitScalar(u8, buf, '\n');
    while (it2.next()) |line| {
        if (line.len == 0) continue;
        const tab = std.mem.indexOfScalar(u8, line, '\t') orelse continue;
        const col_name = line[0..tab];
        const type_str = std.mem.trimEnd(u8, line[tab + 1 ..], "\r \n");
        const col_type = parseColType(type_str) orelse .text;

        const name_copy = try allocator.dupe(u8, col_name);
        names[n_init] = name_copy;
        n_init += 1;

        const physical: schema.PhysicalColumn = switch (col_type) {
            .text, .char => .{ .lazy_text = .{ .source_column = name_copy } },
            else => .{ .fixed = .{ .path_name = name_copy, .ty = col_type } },
        };
        columns[ci] = .{
            .name = name_copy,
            .ty = col_type,
            .storage = switch (col_type) {
                .text, .char => .lazy_source,
                else => .fixed_eager,
            },
            .physical = physical,
        };
        ci += 1;
    }

    // Trim to actual count (in case of blank lines).
    const final_columns = columns[0..ci];
    const final_names   = names[0..n_init];

    // Wire up hash-sidecar: for any string column "X" where "XHash" (Int64/UInt64)
    // also exists in the schema, set X.physical.lazy_text.hash_column = "XHash".
    // This enables executeHashAggParallelStrKeyViaHash for GROUP BY X queries
    // (aggregates by integer hash instead of string, then late-materializes strings).
    for (final_columns) |*col| {
        if (col.physical != .lazy_text) continue;
        for (final_columns) |other| {
            if (other.ty != .int64) continue;
            const expected_len = col.name.len + 4; // "Hash" = 4 chars
            if (other.name.len != expected_len) continue;
            if (!std.mem.eql(u8, other.name[0..col.name.len], col.name)) continue;
            if (!std.mem.eql(u8, other.name[col.name.len..], "Hash")) continue;
            // other.name is "XHash" — wire it as the hash sidecar.
            // other.name lifetime = InferredSchema lifetime (owned by names[]).
            col.physical.lazy_text.hash_column = other.name;
            break;
        }
    }

    return .{
        .table     = .{ .name = table_name, .columns = final_columns },
        .allocator = allocator,
        .columns   = columns,
        .names     = final_names,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "inferColumn: INT32 without converted type → int32 fixed" {
    const col = inferColumn("x", 0, PT_INT32, null);
    try std.testing.expectEqual(schema.ColumnType.int32, col.ty);
    try std.testing.expect(col.physical == .fixed);
}

test "inferColumn: INT32 DATE → date fixed" {
    const col = inferColumn("d", 0, PT_INT32, CT_DATE);
    try std.testing.expectEqual(schema.ColumnType.date, col.ty);
    try std.testing.expect(col.physical == .fixed);
}

test "inferColumn: INT64 TIMESTAMP_MILLIS → timestamp fixed" {
    const col = inferColumn("ts", 0, PT_INT64, CT_TIMESTAMP_MILLIS);
    try std.testing.expectEqual(schema.ColumnType.timestamp, col.ty);
    try std.testing.expect(col.physical == .fixed);
}

test "inferColumn: INT32 INT_16 → int16 fixed" {
    const col = inferColumn("s", 0, PT_INT32, CT_INT_16);
    try std.testing.expectEqual(schema.ColumnType.int16, col.ty);
}

test "inferColumn: BYTE_ARRAY → text lazy_text" {
    const col = inferColumn("url", 0, PT_BYTE_ARRAY, null);
    try std.testing.expectEqual(schema.ColumnType.text, col.ty);
    try std.testing.expect(col.physical == .lazy_text);
}

test "inferColumn: BYTE_ARRAY UTF8 → text lazy_text" {
    const col = inferColumn("s", 0, PT_BYTE_ARRAY, CT_UTF8);
    try std.testing.expectEqual(schema.ColumnType.text, col.ty);
    try std.testing.expect(col.physical == .lazy_text);
}

test "inferColumn: BOOLEAN → int16 fixed" {
    const col = inferColumn("flag", 0, PT_BOOLEAN, null);
    try std.testing.expectEqual(schema.ColumnType.int16, col.ty);
    try std.testing.expect(col.physical == .fixed);
}
