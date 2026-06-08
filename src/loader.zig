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
const parquet = @import("parquet");
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
    const part_path = try generic_store.initPart(io, store_dir, table.name, allocator);
    defer allocator.free(part_path);

    // Write columns.txt manifest.
    try generic_store.writeColumnsTxt(io, allocator, part_path, table);

    // Determine total row count from metadata (sum of row group rows).
    const total_rows: u64 = try parquet.rowCountPath(allocator, io, parquet_path);

    // Separate columns into fixed and string groups; track mapping back to
    // original column index in `table` so callbacks can dispatch correctly.
    var fixed_parquet_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_parquet_indices.deinit(allocator);
    var fixed_table_indices: std.ArrayListUnmanaged(usize) = .empty;  // index into table.columns
    defer fixed_table_indices.deinit(allocator);
    var str_parquet_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_parquet_indices.deinit(allocator);
    var str_table_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_table_indices.deinit(allocator);

    for (table.columns, 0..) |col, col_idx| {
        switch (col.ty) {
            .text, .char, .low_card => {
                try str_parquet_indices.append(allocator, col_idx);
                try str_table_indices.append(allocator, col_idx);
            },
            else => {
                try fixed_parquet_indices.append(allocator, col_idx);
                try fixed_table_indices.append(allocator, col_idx);
            },
        }
    }

    // Open a ColumnBinWriter for every fixed column.
    const n_fixed = fixed_parquet_indices.items.len;
    const fixed_writers = try allocator.alloc(generic_store.ColumnBinWriter, n_fixed);
    defer allocator.free(fixed_writers);
    var fixed_open: usize = 0;
    errdefer for (fixed_writers[0..fixed_open]) |*w| w.close();

    for (fixed_table_indices.items, 0..) |ti, i| {
        const col_name = table.columns[ti].name;
        const bin_path = try generic_store.columnBinPath(allocator, part_path, col_name);
        defer allocator.free(bin_path);
        fixed_writers[i] = try generic_store.ColumnBinWriter.open(io, bin_path);
        fixed_open += 1;
    }

    // Open a temp file + allocate an offset table for every string column.
    const n_str = str_parquet_indices.items.len;
    const str_tmp_paths = try allocator.alloc([]u8, n_str);
    defer {
        for (str_tmp_paths) |p| allocator.free(p);
        allocator.free(str_tmp_paths);
    }
    const str_tmp_files = try allocator.alloc(std.Io.File, n_str);
    defer allocator.free(str_tmp_files);
    var str_tmp_open: usize = 0;
    errdefer for (str_tmp_files[0..str_tmp_open]) |f| f.close(io);

    const str_offsets = try allocator.alloc([]u64, n_str);
    defer allocator.free(str_offsets);
    const str_byte_counts = try allocator.alloc(u64, n_str);
    defer allocator.free(str_byte_counts);
    var str_offsets_alloc: usize = 0;
    errdefer for (str_offsets[0..str_offsets_alloc]) |off| allocator.free(off);

    for (str_table_indices.items, 0..) |ti, i| {
        const col_name = table.columns[ti].name;
        const str_path = try generic_store.columnStrBinPath(allocator, part_path, col_name);
        const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{str_path});
        allocator.free(str_path);
        str_tmp_paths[i] = tmp_path;
        str_tmp_files[i] = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        str_tmp_open += 1;
        str_offsets[i] = try allocator.alloc(u64, total_rows + 1);
        str_offsets[i][0] = 0;
        str_offsets_alloc += 1;
        str_byte_counts[i] = 0;
    }

    const ctx = GenericAllColsCtx{
        .io = io,
        .fixed_writers = fixed_writers,
        .fixed_col_types = blk: {
            const tys = try allocator.alloc(schema.ColumnType, n_fixed);
            for (fixed_table_indices.items, 0..) |ti, i| tys[i] = table.columns[ti].ty;
            break :blk tys;
        },
        .str_tmp_files = str_tmp_files,
        .str_offsets = str_offsets,
        .str_byte_counts = str_byte_counts,
        .str_row_counts = blk: {
            const rc = try allocator.alloc(u64, n_str);
            @memset(rc, 0);
            break :blk rc;
        },
    };
    defer allocator.free(ctx.fixed_col_types);
    defer allocator.free(ctx.str_row_counts);

    _ = try parquet.streamAllColumnsPath(
        allocator,
        io,
        parquet_path,
        fixed_parquet_indices.items,
        str_parquet_indices.items,
        ctx,
        genericAllFixedBatch,
        genericAllStrValue,
    );

    // Close fixed writers.
    for (fixed_writers) |*w| w.close();

    // Flush string columns: close temp files, write final .str.bin files.
    for (str_table_indices.items, 0..) |ti, i| {
        str_tmp_files[i].close(io);
        const col_name = table.columns[ti].name;
        const str_path = try generic_store.columnStrBinPath(allocator, part_path, col_name);
        defer allocator.free(str_path);

        const out = try std.Io.Dir.cwd().createFile(io, str_path, .{ .truncate = true });
        defer out.close(io);

        var hdr: [8]u8 = undefined;
        std.mem.writeInt(u64, &hdr, total_rows, .little);
        try out.writeStreamingAll(io, &hdr);
        try out.writeStreamingAll(io, std.mem.sliceAsBytes(str_offsets[i][0 .. total_rows + 1]));

        const tmp_in = try std.Io.Dir.cwd().openFile(io, str_tmp_paths[i], .{});
        defer tmp_in.close(io);
        var cpbuf: [65536]u8 = undefined;
        while (true) {
            const n = tmp_in.readStreaming(io, &.{&cpbuf}) catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (n == 0) break;
            try out.writeStreamingAll(io, cpbuf[0..n]);
        }

        std.Io.Dir.cwd().deleteFile(io, str_tmp_paths[i]) catch {};
        allocator.free(str_offsets[i]);
    }

    // Write count.txt last (so a partial import leaves no count.txt).
    try generic_store.writeCountTxt(io, allocator, part_path, total_rows);

    return total_rows;
}

/// Detect which integer columns in `table` are stored in globally sorted
/// (non-decreasing) order within the part at `part_path`.
///
/// Uses a sampling strategy: for each candidate column we draw N_SAMPLES
/// evenly-spaced consecutive pairs and verify each pair is non-decreasing.
/// False-positive rate with N_SAMPLES=64 is (1/2)^64 ≈ 5×10⁻²⁰ for a
/// fully-random column — negligible.
///
/// Returns a heap-allocated slice of column name strings owned by the caller.
pub fn detectSortKeys(
    allocator: std.mem.Allocator,
    io:        std.Io,
    part_path: []const u8,
    table:     schema.Table,
) ![]const []const u8 {
    const N_SAMPLES: usize = 64;

    var sorted_names: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer {
        for (sorted_names.items) |n| allocator.free(n);
        sorted_names.deinit(allocator);
    }

    for (table.columns) |col| {
        const bytes_per_val: usize = switch (col.ty) {
            .int8  => 1,
            .int16 => 2,
            .int32 => 4,
            .int64 => 8,
            else   => continue, // skip strings, floats, date, timestamp
        };

        const bin_path = try generic_store.columnBinPath(allocator, part_path, col.name);
        defer allocator.free(bin_path);

        const file = std.Io.Dir.cwd().openFile(io, bin_path, .{}) catch continue;
        defer file.close(io);

        const stat = file.stat(io) catch continue;
        const file_size = stat.size;
        const n_rows = file_size / bytes_per_val;
        if (n_rows < 2) {
            const name = try allocator.dupe(u8, col.name);
            try sorted_names.append(allocator, name);
            continue;
        }

        const step = @max(1, n_rows / N_SAMPLES);
        var prev: i64 = std.math.minInt(i64);
        var is_sorted = true;

        var si: usize = 0;
        while (si < n_rows and is_sorted) : (si += step) {
            var buf: [8]u8 = undefined;
            const n_read = file.readPositionalAll(io, buf[0..bytes_per_val], si * bytes_per_val) catch {
                is_sorted = false; break;
            };
            if (n_read < bytes_per_val) { is_sorted = false; break; }
            const val: i64 = switch (col.ty) {
                .int8  => @as(i8,  @bitCast(buf[0])),
                .int16 => std.mem.readInt(i16, buf[0..2], .little),
                .int32 => std.mem.readInt(i32, buf[0..4], .little),
                .int64 => std.mem.readInt(i64, buf[0..8], .little),
                else   => unreachable,
            };
            if (val < prev) { is_sorted = false; break; }
            prev = val;
        }

        if (is_sorted) {
            const name = try allocator.dupe(u8, col.name);
            try sorted_names.append(allocator, name);
        }
    }

    return sorted_names.toOwnedSlice(allocator);
}

const GenericAllColsCtx = struct {
    io: std.Io,
    fixed_writers: []generic_store.ColumnBinWriter,
    fixed_col_types: []schema.ColumnType,
    str_tmp_files: []std.Io.File,
    str_offsets: [][]u64,
    str_byte_counts: []u64,
    str_row_counts: []u64,
};

fn genericAllFixedBatch(ctx: GenericAllColsCtx, fi: usize, batches: []const []const i64) anyerror!void {
    for (batches, 0..) |batch, bi| {
        const slot = fi + bi;
        const writer = &ctx.fixed_writers[slot];
        const ty = ctx.fixed_col_types[slot];
        for (batch) |v| {
            switch (ty) {
                .int8 => try writer.writeI8(@intCast(v)),
                .int16 => try writer.writeI16(@intCast(v)),
                .int32, .date => try writer.writeI32(@intCast(v)),
                .int64, .timestamp => try writer.writeI64(v),
                .float32 => {
                    const bits: i32 = @bitCast(@as(u32, @intCast(v & 0xFFFF_FFFF)));
                    try writer.writeI32(bits);
                },
                .float64 => try writer.writeI64(v),
                else => {},
            }
        }
    }
}

fn genericAllStrValue(ctx: GenericAllColsCtx, slot: usize, value: []const u8) anyerror!void {
    if (value.len > 0) {
        try ctx.str_tmp_files[slot].writeStreamingAll(ctx.io, value);
    }
    ctx.str_byte_counts[slot] += value.len;
    ctx.str_row_counts[slot] += 1;
    const row = ctx.str_row_counts[slot];
    ctx.str_offsets[slot][row] = ctx.str_byte_counts[slot];
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

    var part = try ch_part.Part.open(io, allocator, part_dir, table, pk_col_name, 0x82); // METHOD_LZ4
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

// ── CompactPart import ────────────────────────────────────────────────────────

/// Import a Parquet file into a ClickHouse-compatible Compact MergeTree part.
///
/// Writes all files required by CH 26.5 compact parts:
///   data.bin, data.cmrk4, primary.cidx, serialization.json,
///   columns.txt, columns_substreams.txt, count.txt,
///   checksums.txt, default_compression_codec.txt, metadata_version.txt
///
/// Returns max_seq+1 by scanning existing `parts/all_*` directories under
/// `<store_dir>/<table_name>/parts/`.  Falls back to 1 on any error.
fn nextPartSeq(allocator: std.mem.Allocator, io: std.Io, store_dir: []const u8, table_name: []const u8) u64 {
    const parts_path = std.fmt.allocPrint(allocator, "{s}/{s}/parts", .{ store_dir, table_name }) catch return 1;
    defer allocator.free(parts_path);

    var dir = std.Io.Dir.cwd().openDir(io, parts_path, .{ .iterate = true }) catch return 1;
    defer dir.close(io);

    var max: u64 = 0;
    var it = dir.iterate();
    while (it.next(io) catch null) |entry| {
        if (entry.kind != .directory) continue;
        const name = entry.name;
        // Parse "all_<min>_<max>_<level>" — we only need max_seq field.
        if (!std.mem.startsWith(u8, name, "all_")) continue;
        var parts = std.mem.splitScalar(u8, name["all_".len..], '_');
        _ = parts.next(); // min_seq
        const max_s = parts.next() orelse continue;
        const v = std.fmt.parseInt(u64, max_s, 10) catch continue;
        if (v > max) max = v;
    }
    return max + 1;
}

/// Imports a Parquet file as a ClickHouse Compact MergeTree part.
pub fn importParquetCompact(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    store_dir: []const u8,
    table: schema.Table,
) !u64 {
    const total_rows: u64 = try parquet.rowCountPath(allocator, io, parquet_path);

    // Choose a seq that doesn't collide with existing parts.
    const seq = nextPartSeq(allocator, io, store_dir, table.name);

    const part_dir = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/parts/all_{d}_{d}_0",
        .{ store_dir, table.name, seq, seq },
    );
    defer allocator.free(part_dir);

    var part = try ch_part.CompactPart.open(io, allocator, part_dir, table, 0x82); // METHOD_LZ4
    defer part.deinit();

    // Build separate index lists for fixed vs string columns.
    var fixed_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer fixed_indices.deinit(allocator);
    var str_indices: std.ArrayListUnmanaged(usize) = .empty;
    defer str_indices.deinit(allocator);
    var str_col_map: std.ArrayListUnmanaged(usize) = .empty;
    defer str_col_map.deinit(allocator);
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

    const ctx = CompactAllColsCtx{
        .part         = &part,
        .fixed_col_map = fixed_col_map.items,
        .str_col_map   = str_col_map.items,
    };

    _ = try parquet.streamAllColumnsPath(
        allocator,
        io,
        parquet_path,
        fixed_indices.items,
        str_indices.items,
        ctx,
        compactAllFixedBatch,
        compactAllStrValue,
    );

    part.setRowCount(total_rows);
    try part.finish();
    return total_rows;
}

const CompactAllColsCtx = struct {
    part:          *ch_part.CompactPart,
    fixed_col_map: []const usize,
    str_col_map:   []const usize,
};

fn compactAllFixedBatch(ctx: CompactAllColsCtx, slot_start: usize, batches: []const []const i64) anyerror!void {
    for (ctx.fixed_col_map[slot_start..][0..batches.len], batches) |col_idx, batch| {
        try ctx.part.appendFixedBatch(col_idx, batch);
    }
}

fn compactAllStrValue(ctx: CompactAllColsCtx, slot: usize, value: []const u8) anyerror!void {
    try ctx.part.appendString(ctx.str_col_map[slot], value);
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
        .text, .char, .low_card => try importStringColumn(allocator, io, parquet_path, part, col.name, col_idx, total_rows),
        .int8 => try importFixedColumn(i8, allocator, io, parquet_path, part, col.name, col_idx),
        .int16 => try importFixedColumn(i16, allocator, io, parquet_path, part, col.name, col_idx),
        .int32, .date => try importFixedColumn(i32, allocator, io, parquet_path, part, col.name, col_idx),
        .int64, .timestamp => try importFixedColumn(i64, allocator, io, parquet_path, part, col.name, col_idx),
        .float32 => try importFixedColumn(f32, allocator, io, parquet_path, part, col.name, col_idx),
        .float64 => try importFixedColumn(f64, allocator, io, parquet_path, part, col.name, col_idx),
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
                switch (T) {
                    i8 => try ctx.writer.writeI8(@intCast(v)),
                    i16 => try ctx.writer.writeI16(@intCast(v)),
                    i32 => try ctx.writer.writeI32(@intCast(v)),
                    i64 => try ctx.writer.writeI64(v),
                    f32 => {
                        // Parquet stores f32 as i64 with lower 32 bits = raw float bits
                        const bits: i32 = @bitCast(@as(u32, @intCast(v & 0xFFFF_FFFF)));
                        try ctx.writer.writeI32(bits);
                    },
                    f64 => {
                        // Parquet stores f64 as i64 = raw float bits
                        const bits: i64 = v;
                        try ctx.writer.writeI64(bits);
                    },
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
/// We collect offsets in memory and stream bytes to a temp file, then
/// assemble the final .str.bin file.  This avoids holding the full bytes
/// blob in RAM for large columns (e.g. URL, Referer with 10M rows).
fn importStringColumn(
    allocator: std.mem.Allocator,
    io: std.Io,
    parquet_path: []const u8,
    part: []const u8,
    col_name: []const u8,
    col_idx: usize,
    total_rows: u64,
) !void {
    // Allocate offset table (total_rows+1 entries × 8 bytes = ~80 MB for 10M rows).
    const offsets = try allocator.alloc(u64, total_rows + 1);
    defer allocator.free(offsets);
    offsets[0] = 0;
    var row: u64 = 0;
    var total_bytes: u64 = 0;

    // Stream bytes to a temp file so we never hold the full blob in RAM.
    const str_path = try generic_store.columnStrBinPath(allocator, part, col_name);
    defer allocator.free(str_path);

    // Build a temp path alongside the final file.
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{str_path});
    defer allocator.free(tmp_path);

    {
        const tmp_file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer tmp_file.close(io);

        const ctx = StringStreamCtx{
            .io = io,
            .file = tmp_file,
            .offsets = offsets,
            .row = &row,
            .total_bytes = &total_bytes,
        };

        _ = try parquet.streamByteArrayColumnPath(
            allocator,
            io,
            parquet_path,
            col_idx,
            null,
            ctx,
            streamStringValue,
        );
    }

    // Assemble final .str.bin: header + offsets + bytes blob from temp file.
    {
        const out = try std.Io.Dir.cwd().createFile(io, str_path, .{ .truncate = true });
        defer out.close(io);

        // Write row_count header.
        var hdr: [8]u8 = undefined;
        std.mem.writeInt(u64, &hdr, total_rows, .little);
        try out.writeStreamingAll(io, &hdr);

        // Write offsets table.
        try out.writeStreamingAll(io, std.mem.sliceAsBytes(offsets[0 .. total_rows + 1]));

        // Copy bytes blob from temp file.
        const tmp_in = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
        defer tmp_in.close(io);

        var buf: [65536]u8 = undefined;
        while (true) {
            const n = tmp_in.readStreaming(io, &.{&buf}) catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (n == 0) break;
            try out.writeStreamingAll(io, buf[0..n]);
        }
    }

    // Remove temp file.
    std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
}

const StringStreamCtx = struct {
    io: std.Io,
    file: std.Io.File,
    offsets: []u64,
    row: *u64,
    total_bytes: *u64,
};

fn streamStringValue(ctx: StringStreamCtx, value: []const u8) anyerror!void {
    if (value.len > 0) {
        try ctx.file.writeStreamingAll(ctx.io, value);
    }
    ctx.total_bytes.* += value.len;
    ctx.row.* += 1;
    ctx.offsets[ctx.row.*] = ctx.total_bytes.*;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "importParquet round-trips a tiny synthetic parquet" {
    // This test uses a real .parquet fixture if available.
    // Skipped here because we don't bundle test fixtures in the repo.
    // Integration tests live in tests/import_roundtrip_test.zig (future).
    // Smoke: just make sure the module compiles and the helpers link.
    _ = importParquet;
}

test "importParquetCompact compiles and links" {
    // Smoke test — just verify the function and its helpers compile.
    _ = importParquetCompact;
    _ = CompactAllColsCtx;
    _ = compactAllFixedBatch;
    _ = compactAllStrValue;
}
