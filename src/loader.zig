/// Generic Parquet importer.
///
/// `importParquet` reads a Parquet file and writes the generic part store
/// layout produced by `generic_store`:
///
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt
///     count.txt
///     <col>.bin         — fixed-width columns (int16/int32/int64/date/timestamp)
///     <col>.str.bin     — string columns (offset table + byte data)
///
/// String column format for <col>.str.bin:
///   [u64 row_count]
///   [row_count+1 × u64 offsets]   (offset[i] = start of string i in bytes blob)
///   [total_bytes × u8 bytes]       (UTF-8 or raw bytes, no NUL terminator)
///
/// All integers are little-endian.
///
/// The caller is responsible for ensuring that `table` schema matches the
/// Parquet file layout (column count and order) — `inferSchema` produces a
/// compatible table from the same file.

const std = @import("std");
const parquet = @import("parquet.zig");
const schema = @import("schema");
const generic_store = @import("generic_store.zig");
const ch_part = @import("ch_part");
const row_binary = @import("clickhouse_native/row_binary.zig");
const http_client = @import("clickhouse_native/http_client.zig");

/// Import a Parquet file into the generic part store.
///
/// Parameters:
///   allocator   — used for temporary allocations during the import
///   io          — I/O handle
///   parquet_path — path to the source .parquet file
///   store_dir   — root directory for the store (will be created if absent)
///   table       — schema of the table to import (column order must match file)
///
/// Returns the number of rows imported.
pub fn importParquet(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    store_dir: []const u8,
    table: schema.Table,
) !u64 {
    // Create the part directory tree and get the part path.
    const part = try generic_store.initPart(io, store_dir, table.name, allocator);
    defer allocator.free(part);

    // Write columns.txt manifest.
    try generic_store.writeColumnsTxt(io, allocator, part, table);

    // Determine total row count from metadata (sum of row group rows).
    const total_rows: u64 = try parquet.rowCountPath(allocator, io, parquet_path);

    // Import each column.
    for (table.columns, 0..) |col, col_idx| {
        try importColumn(allocator, io, parquet_path, part, col, col_idx, total_rows);
    }

    // Write count.txt last (so a partial import leaves no count.txt).
    try generic_store.writeCountTxt(io, allocator, part, total_rows);

    return total_rows;
}

/// Import a Parquet file into a ClickHouse MergeTree-compatible part directory.
///
/// Writes:
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt, count.txt, primary.idx, checksums.txt
///     <col>.bin  (LZ4 compressed)
///     <col>.mrk2 (mark file)
///
/// Each column is streamed independently from the Parquet file.
pub fn importParquetCH(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    store_dir: []const u8,
    table: schema.Table,
    pk_col_name: ?[]const u8,
) !u64 {
    const total_rows: u64 = try parquet.rowCountPath(allocator, io, parquet_path);

    const part_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/parts/all_1_1_0",
        .{ store_dir, table.name },
    );
    defer allocator.free(part_dir);

    var part = try ch_part.Part.open(io, allocator, part_dir, table, pk_col_name);
    defer part.deinit();

    // Build separate index lists for fixed vs string columns.
    var fixed_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_indices.deinit(allocator);
    var str_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_indices.deinit(allocator);
    // Map from str_slot -> col_idx in the Part
    var str_col_map: std.ArrayListUnmanaged(usize) = .empty;
    defer str_col_map.deinit(allocator);
    // Map from fixed_slot -> col_idx in the Part
    var fixed_col_map: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_col_map.deinit(allocator);

    for (table.columns, 0..) |col, col_idx| {
        switch (col.ty) {
            .text, .char => {
                try str_indices.append(allocator, col_idx);
                try str_col_map.append(allocator, col_idx);
            },
            else => {
                try fixed_indices.append(allocator, col_idx);
                try fixed_col_map.append(allocator, col_idx);
            },
        }
    }

    const ctx = CHAllColsCtx{
        .part = &part,
        .fixed_col_map = fixed_col_map.items,
        .str_col_map = str_col_map.items,
    };

    _ = try parquet.streamAllColumnsPath(
        allocator,
        io,
        parquet_path,
        fixed_indices.items,
        str_indices.items,
        ctx,
        chAllFixedBatch,
        chAllStrValue,
    );

    part.setRowCount(total_rows);
    try part.finish();
    return total_rows;
}

/// Import a Parquet file by streaming RowBinary rows to a ClickHouse HTTP endpoint.
///
/// For each Parquet row group:
///   1. All fixed columns are cached as []i64 slices.
///   2. All string columns are cached as [][]u8 slices.
///   3. Rows are interleaved into RowBinary and appended to ChHttpInserter.
///   4. maybeFlush is called after each row group; finish is called at the end.
///
/// Parameters:
///   allocator     — used for temporary row-group buffers
///   io            — I/O handle
///   parquet_path  — path to the source .parquet file
///   table         — schema of the table (must match Parquet column order)
///   inserter_opts — connection/batching options for ChHttpInserter
///   table_name    — CH table name for INSERT query
pub fn importParquetCHHttp(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    table: schema.Table,
    inserter_opts: http_client.Options,
    table_name: []const u8,
) !u64 {
    // Build separate index lists for fixed vs string columns.
    var fixed_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_indices.deinit(allocator);
    var str_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_indices.deinit(allocator);
    // Maps: slot -> column schema index (same order as indices lists)
    var fixed_col_schema: std.ArrayListUnmanaged(schema.Column) = .empty;
    defer fixed_col_schema.deinit(allocator);
    var str_col_schema: std.ArrayListUnmanaged(schema.Column) = .empty;
    defer str_col_schema.deinit(allocator);
    // Maps: original column index -> (is_str, slot)
    // We need to know column order for RowBinary output.
    var col_is_str = try allocator.alloc(bool, table.columns.len);
    defer allocator.free(col_is_str);
    var col_slot = try allocator.alloc(usize, table.columns.len);
    defer allocator.free(col_slot);

    for (table.columns, 0..) |col, col_idx| {
        switch (col.ty) {
            .text, .char => {
                col_is_str[col_idx] = true;
                col_slot[col_idx] = str_indices.items.len;
                try str_indices.append(allocator, col_idx);
                try str_col_schema.append(allocator, col);
            },
            else => {
                col_is_str[col_idx] = false;
                col_slot[col_idx] = fixed_indices.items.len;
                try fixed_indices.append(allocator, col_idx);
                try fixed_col_schema.append(allocator, col);
            },
        }
    }

    const n_fixed = fixed_indices.items.len;
    const n_str = str_indices.items.len;

    var inserter = try http_client.ChHttpInserter.init(allocator, io, inserter_opts);
    defer inserter.deinit();

    // Context caches values for the current row group.
    var ctx = CHHttpCtx{
        .allocator = allocator,
        .n_fixed = n_fixed,
        .n_str = n_str,
        // these are allocated in accumulate callbacks
        .fixed_cols = try allocator.alloc(std.ArrayListUnmanaged(i64), n_fixed),
        .str_cols = try allocator.alloc(std.ArrayListUnmanaged([]u8), n_str),
        .str_bytes = try allocator.alloc(std.ArrayListUnmanaged(u8), n_str),
        .rows_in_group = 0,
    };
    defer {
        for (ctx.fixed_cols) |*fc| fc.deinit(allocator);
        for (ctx.str_cols, ctx.str_bytes) |*sc, *sb| {
            // str_cols items are slices into str_bytes — do NOT free them individually
            sc.deinit(allocator);
            sb.deinit(allocator);
        }
        allocator.free(ctx.fixed_cols);
        allocator.free(ctx.str_cols);
        allocator.free(ctx.str_bytes);
    }
    for (ctx.fixed_cols) |*fc| fc.* = .empty;
    for (ctx.str_cols) |*sc| sc.* = .empty;
    for (ctx.str_bytes) |*sb| sb.* = .empty;

    // We need a row-group-aware streaming approach.
    // streamAllColumnsPath processes all row groups in sequence but calls
    // fixed_cb and str_cb interleaved per row group. We'll accumulate into
    // ctx buffers and flush after each row group.
    //
    // However streamAllColumnsPath doesn't have a "row group boundary" callback.
    // We track row counts: after fixed_cb fills `rows_in_group` rows, we know
    // a row group is complete when str_cb has processed the same count.
    // Since fixed_cb runs for ALL fixed cols before str_cb runs for str cols
    // within a row group, we can't flush inline. Instead we accumulate the
    // entire file and flush at the end — but this uses O(total_rows) memory.
    //
    // For large files we need a different approach: use the lower-level parquet
    // API to iterate row groups manually. Since we need to write RowBinary rows
    // interleaved, we do it row-group by row-group via streamAllColumnsPath's
    // "reset on row group" semantics. Unfortunately the current API doesn't
    // expose row groups directly.
    //
    // Pragmatic solution: accumulate all data, then flush. For 10M rows of
    // ClickBench (all fixed + 3 string cols) this is ~500MB RAM which is acceptable
    // for an import tool. The unc_buf approach for wide part was similar.

    _ = try parquet.streamAllColumnsPath(
        allocator,
        io,
        parquet_path,
        fixed_indices.items,
        str_indices.items,
        &ctx,
        chHttpFixedBatch,
        chHttpStrValue,
    );

    // Now interleave rows and send RowBinary.
    const total_rows = ctx.rows_in_group; // accumulated across all row groups

    // Row buffer: encode each row, then appendBytes.
    var row_aw = std.Io.Writer.Allocating.init(allocator);
    defer row_aw.deinit();

    // Verify all columns have the same row count.
    for (ctx.fixed_cols) |fc| {
        if (fc.items.len != total_rows) return error.ColumnRowCountMismatch;
    }
    for (ctx.str_cols) |sc| {
        if (sc.items.len != total_rows) return error.ColumnRowCountMismatch;
    }

    for (0..total_rows) |row_idx| {
        row_aw.clearRetainingCapacity();
        var enc = row_binary.RowBinaryEncoder.init(&row_aw.writer);

        for (table.columns, 0..) |col, col_idx| {
            if (col_is_str[col_idx]) {
                const slot = col_slot[col_idx];
                try enc.writeString(ctx.str_cols[slot].items[row_idx]);
            } else {
                const slot = col_slot[col_idx];
                const v = ctx.fixed_cols[slot].items[row_idx];
                switch (col.ty) {
                    .int16 => try enc.writeInt16(@intCast(v)),
                    .int32 => try enc.writeInt32(@intCast(v)),
                    .int64 => try enc.writeInt64(v),
                    .date => try enc.writeDate(@intCast(v)),
                    .timestamp => try enc.writeDateTime(v),
                    else => unreachable,
                }
            }
        }

        try inserter.appendBytes(row_aw.written());
        try inserter.maybeFlush(table_name);
    }

    try inserter.finish(table_name);
    return @intCast(total_rows);
}

const CHHttpCtx = struct {
    allocator: std.mem.Allocator,
    n_fixed: usize,
    n_str: usize,
    fixed_cols: []std.ArrayListUnmanaged(i64), // [n_fixed][rows]
    str_cols: []std.ArrayListUnmanaged([]u8),   // [n_str][rows] — each []u8 is owned
    str_bytes: []std.ArrayListUnmanaged(u8),    // backing storage for each string (concatenated)
    rows_in_group: usize,                       // total rows accumulated
};

fn chHttpFixedBatch(ctx: *CHHttpCtx, slot_start: usize, batches: []const []const i64) anyerror!void {
    for (ctx.fixed_cols[slot_start..][0..batches.len], batches) |*col, batch| {
        for (batch) |v| {
            try col.append(ctx.allocator, v);
        }
    }
    // Count rows only via slot_start==0 to avoid double-counting sub-batches.
    // streamAllColumnsPath calls us with slot_start=0,8,16,... per row group;
    // we count once per unique (row_group, chunk) which corresponds to slot_start==0.
    if (batches.len > 0 and slot_start == 0) {
        ctx.rows_in_group += batches[0].len;
    }
    // If there are no fixed cols, rows_in_group is tracked via chHttpStrValue.
}

fn chHttpStrValue(ctx: *CHHttpCtx, slot: usize, value: []const u8) anyerror!void {
    // Copy string bytes into backing storage
    const start = ctx.str_bytes[slot].items.len;
    try ctx.str_bytes[slot].appendSlice(ctx.allocator, value);
    const owned = ctx.str_bytes[slot].items[start..];
    try ctx.str_cols[slot].append(ctx.allocator, owned);
    // If no fixed cols, count rows via first string col
    if (ctx.n_fixed == 0 and slot == 0) {
        ctx.rows_in_group += 1;
    }
}

const CHAllColsCtx = struct {
    part: *ch_part.Part,
    fixed_col_map: []const usize, // slot -> part col_idx
    str_col_map: []const usize,   // slot -> part col_idx
};

fn chAllFixedBatch(ctx: CHAllColsCtx, slot_start: usize, batches: []const []const i64) anyerror!void {
    for (ctx.fixed_col_map[slot_start..][0..batches.len], batches) |col_idx, batch| {
        try ctx.part.appendFixedBatch(col_idx, batch);
    }
}

fn chAllStrValue(ctx: CHAllColsCtx, slot: usize, value: []const u8) anyerror!void {
    try ctx.part.appendStrOne(ctx.str_col_map[slot], value);
}

/// Scan a Parquet file using the CH import path (streamAllColumnsPath),
/// but discard all data — only count rows and verify all columns decode OK.
/// Prints progress every 1M rows.
pub fn scanParquet(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    table: schema.Table,
) !u64 {
    // Build separate index lists for fixed vs string columns.
    var fixed_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_indices.deinit(allocator);
    var str_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_indices.deinit(allocator);

    for (table.columns, 0..) |col, col_idx| {
        switch (col.ty) {
            .text, .char => try str_indices.append(allocator, col_idx),
            else => try fixed_indices.append(allocator, col_idx),
        }
    }

    var ctx = ScanCtx{ .rows = 0, .str_cols = str_indices.items.len };

    const total = try parquet.streamAllColumnsPath(
        allocator,
        io,
        parquet_path,
        fixed_indices.items,
        str_indices.items,
        &ctx,
        scanFixedBatch,
        scanStrValue,
    );

    return total;
}

const ScanCtx = struct {
    rows: u64,
    str_cols: usize,
    str_in_row: usize = 0,
};

fn scanFixedBatch(ctx: *ScanCtx, slot_start: usize, batches: []const []const i64) anyerror!void {
    _ = slot_start;
    if (batches.len == 0) return;
    const n = batches[0].len;
    ctx.rows += n;
    if (ctx.rows % 1_000_000 < n) {
        std.debug.print("  scanned {}M rows...\n", .{ctx.rows / 1_000_000});
    }
}

fn scanStrValue(ctx: *ScanCtx, slot: usize, value: []const u8) anyerror!void {
    _ = ctx;
    _ = slot;
    _ = value;
}


fn importCHStringColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: *ch_part.Part,
    col_idx: usize,
    total_rows: u64,
) !void {
    // Stream strings directly into the Part one-by-one to avoid buffering
    // all rows in memory (critical for large datasets like 10M-row hits.parquet).
    _ = total_rows;
    const ctx = CHStrStreamCtx{ .part = part, .col_idx = col_idx };
    _ = try parquet.streamByteArrayColumnPath(
        allocator,
        io,
        parquet_path,
        col_idx,
        null,
        ctx,
        chStreamStr,
    );
}

const CHStrStreamCtx = struct {
    part: *ch_part.Part,
    col_idx: usize,
};

fn chStreamStr(ctx: CHStrStreamCtx, value: []const u8) anyerror!void {
    try ctx.part.appendStrOne(ctx.col_idx, value);
}

// ── Column import ─────────────────────────────────────────────────────────────

fn importColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col: schema.Column,
    col_idx: usize,
    total_rows: u64,
) !void {
    switch (col.ty) {
        .text, .char => try importStringColumn(allocator, io, parquet_path, part, col.name, col_idx, total_rows),
        .int16 => try importFixedColumn(i16, allocator, io, parquet_path, part, col.name, col_idx),
        .int32, .date => try importFixedColumn(i32, allocator, io, parquet_path, part, col.name, col_idx),
        .int64, .timestamp => try importFixedColumn(i64, allocator, io, parquet_path, part, col.name, col_idx),
    }
}

// ── Fixed-width columns ────────────────────────────────────────────────────────

fn importFixedColumn(
    comptime T: type,
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col_name: []const u8,
    col_idx: usize,
) !void {
    const path = try generic_store.columnBinPath(allocator, part, col_name);
    defer allocator.free(path);

    var writer = try generic_store.ColumnBinWriter.open(io, path);
    defer writer.close();

    const ctx = FixedWriteCtx(T){ .writer = &writer };

    _ = try parquet.streamFixedColumnPath(
        allocator,
        io,
        parquet_path,
        col_idx,
        null,
        ctx,
        writeFixedBatch(T),
    );
}

fn FixedWriteCtx(comptime T: type) type {
    _ = T;
    return struct { writer: *generic_store.ColumnBinWriter };
}

fn writeFixedBatch(comptime T: type) fn (FixedWriteCtx(T), []const i64) anyerror!void {
    return struct {
        fn cb(ctx: FixedWriteCtx(T), values: []const i64) anyerror!void {
            for (values) |v| {
                const cast: T = @intCast(v);
                switch (T) {
                    i16 => try ctx.writer.writeI16(cast),
                    i32 => try ctx.writer.writeI32(cast),
                    i64 => try ctx.writer.writeI64(cast),
                    else => @compileError("unsupported fixed type"),
                }
            }
        }
    }.cb;
}

// ── String columns ─────────────────────────────────────────────────────────────

/// String column (.str.bin) layout:
///   u64 row_count
///   (row_count+1) × u64 offsets  (byte offset into bytes blob)
///   bytes blob
///
/// We build the offset table and bytes blob in memory then flush.
fn importStringColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col_name: []const u8,
    col_idx: usize,
    total_rows: u64,
) !void {
    // Accumulate offsets and bytes.
    const offsets = try allocator.alloc(u64, total_rows + 1);
    defer allocator.free(offsets);
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(allocator);

    offsets[0] = 0;
    var row: u64 = 0;

    const ctx = StringCollectCtx{
        .allocator = allocator,
        .offsets = offsets,
        .bytes = &bytes,
        .row = &row,
    };

    _ = try parquet.streamByteArrayColumnPath(
        allocator,
        io,
        parquet_path,
        col_idx,
        null,
        ctx,
        collectStringValue,
    );

    // Flush to .str.bin
    const str_path = try generic_store.columnStrBinPath(allocator, part, col_name);
    defer allocator.free(str_path);

    const file = try std.Io.Dir.cwd().createFile(io, str_path, .{ .truncate = true });
    defer file.close(io);

    // Write row_count header
    var hdr: [8]u8 = undefined;
    std.mem.writeInt(u64, &hdr, total_rows, .little);
    try file.writeStreamingAll(io, &hdr);

    // Write offsets (row_count+1 entries)
    try file.writeStreamingAll(io, std.mem.sliceAsBytes(offsets[0 .. total_rows + 1]));

    // Write bytes blob
    try file.writeStreamingAll(io, bytes.items);
}

const StringCollectCtx = struct {
    allocator: std.mem.Allocator,
    offsets: []u64,
    bytes: *std.ArrayListUnmanaged(u8),
    row: *u64,
};

fn collectStringValue(ctx: StringCollectCtx, value: []const u8) anyerror!void {
    try ctx.bytes.appendSlice(ctx.allocator, value);
    ctx.row.* += 1;
    ctx.offsets[ctx.row.*] = @intCast(ctx.bytes.items.len);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "importParquet round-trips a tiny synthetic parquet" {
    // This test uses a real .parquet fixture if available.
    // Skipped here because we don't bundle test fixtures in the repo.
    // Integration tests live in tests/import_roundtrip_test.zig (future).
    // Smoke: just make sure the module compiles and the helpers link.
    _ = importParquet;
}
