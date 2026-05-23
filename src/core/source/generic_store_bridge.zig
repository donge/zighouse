/// generic_store_bridge.zig
///
/// Bridges the generic_store part format (src/generic_store.zig) into the
/// SourceIface / PartReaderVTable expected by the core IR pipeline.
///
/// The generic_store format lives at:
///   <store_dir>/<table_name>/parts/all_1_1_0/
///     columns.txt   — "name\ttype_tag\n" lines
///     count.txt     — row count as decimal string
///     <col>.bin     — fixed-width little-endian column data (int16/int32/int64/date/timestamp)
///     <col>.str.bin — variable-length string column:
///                       [u64 row_count]
///                       [(row_count+1) × u64 offsets]
///                       [bytes blob]
///
/// Usage:
///
///   var bridge = try GenericStoreBridge.init(
///       allocator, io, part_dir, table, pruned_columns,
///   );
///   defer bridge.deinit();
///
///   var qctx = pipeline.QueryContext.init(allocator, bridge.source());
///   defer qctx.deinit();
///
///   const rs = try pipeline.executePlan(node, &qctx);

const std      = @import("std");
const schema   = @import("schema");
const core     = @import("core");
const chunk    = core.chunk;
const result   = core.result;
const pipeline = core.exec.pipeline;
const generic_store = @import("../../generic_store.zig");

pub const DataChunk    = chunk.DataChunk;
pub const ColumnType   = chunk.ColumnType;
pub const SourceIface  = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;
pub const ColMeta      = result.ColMeta;

// ── schema.ColumnType → core.ColumnType ──────────────────────────────────────

fn toCoreColType(ty: schema.ColumnType) ColumnType {
    return switch (ty) {
        .int8, .int16, .int32, .int64 => .int64,
        .float32, .float64            => .float64,
        .date                         => .date_u16,
        .timestamp                    => .datetime64_ms,
        .text, .char                  => .string,
    };
}

// ── Mapped column data ────────────────────────────────────────────────────────

/// Holds the mmap'd data for a fixed-width column.
const MappedFixedCol = struct {
    ptr:  []align(std.heap.page_size_min) u8,
    bytes: []const u8,   // same memory, untyped

    fn deinit(self: MappedFixedCol) void {
        std.posix.munmap(self.ptr);
    }
};

/// Holds the mmap'd data for a string column.
const MappedStringCol = struct {
    ptr:      []align(std.heap.page_size_min) u8,
    row_count: u64,
    offsets:  []const u64,   // row_count+1 entries
    bytes:    []const u8,    // byte blob

    fn deinit(self: MappedStringCol) void {
        std.posix.munmap(self.ptr);
    }

    fn str(self: MappedStringCol, row: u64) []const u8 {
        const start = self.offsets[row];
        const end   = self.offsets[row + 1];
        return self.bytes[start..end];
    }
};

const ColData = union(enum) {
    fixed: MappedFixedCol,
    string: MappedStringCol,
    none,  // pruned / skipped
};

// ── ScanState ─────────────────────────────────────────────────────────────────

const ScanState = struct {
    alloc:        std.mem.Allocator,
    io:           std.Io,
    part_dir:     []const u8,       // path to the all_1_1_0/ directory (borrowed)
    table:        schema.Table,
    row_count:    u64,
    rows_read:    u64,
    col_data:     []ColData,        // one per table column; loaded lazily
    metas:        []ColMeta,
    needed_cols:  ?[]const bool,    // null = read all
    /// Pre-allocated zero buffers reused for pruned columns (avoid per-chunk alloc).
    zero_i64:     []i64,
    zero_f64:     []f64,
    zero_u16:     []u16,
    zero_u8:      []u8,
    zero_u64:     []u64,
    zero_str:     [][]const u8,
    zero_astr:    [][][]const u8,
    zero_nmask:   []u64,
    loaded:       bool,             // have we opened the columns yet?

    fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        part_dir: []const u8,
        table: schema.Table,
        pruned_columns: []const []const u8,
    ) !ScanState {
        // Read row count.
        const row_count = try generic_store.readCountTxt(io, alloc, part_dir);

        // Build ColMeta array.
        const metas = try alloc.alloc(ColMeta, table.columns.len);
        for (table.columns, 0..) |col, i| {
            metas[i] = .{
                .name     = col.name,
                .col_type = toCoreColType(col.ty),
            };
        }

        // Build needed_cols mask.
        const needed_cols: ?[]bool = if (pruned_columns.len == 0) null else blk: {
            const mask = try alloc.alloc(bool, table.columns.len);
            @memset(mask, false);
            for (pruned_columns) |name| {
                for (table.columns, 0..) |col, i| {
                    if (std.mem.eql(u8, col.name, name)) { mask[i] = true; break; }
                }
            }
            break :blk mask;
        };

        const col_data = try alloc.alloc(ColData, table.columns.len);
        @memset(col_data, .none);

        // Pre-allocate zero buffers (CHUNK_SIZE elements) for pruned columns.
        const zero_i64   = try alloc.alloc(i64, chunk.CHUNK_SIZE);  @memset(zero_i64,   0);
        const zero_f64   = try alloc.alloc(f64, chunk.CHUNK_SIZE);  @memset(zero_f64,   0.0);
        const zero_u16   = try alloc.alloc(u16, chunk.CHUNK_SIZE);  @memset(zero_u16,   0);
        const zero_u8    = try alloc.alloc(u8,  chunk.CHUNK_SIZE);  @memset(zero_u8,    0);
        const zero_u64   = try alloc.alloc(u64, chunk.CHUNK_SIZE);  @memset(zero_u64,   0);
        const zero_str   = try alloc.alloc([]const u8, chunk.CHUNK_SIZE);  @memset(zero_str,   "");
        const zero_astr  = try alloc.alloc([][]const u8, chunk.CHUNK_SIZE);  @memset(zero_astr,  &.{});
        const zero_nmask = try alloc.alloc(u64, chunk.nullMaskWords(chunk.CHUNK_SIZE));
        @memset(zero_nmask, 0);

        return .{
            .alloc       = alloc,
            .io          = io,
            .part_dir    = part_dir,
            .table       = table,
            .row_count   = row_count,
            .rows_read   = 0,
            .col_data    = col_data,
            .metas       = metas,
            .needed_cols = needed_cols,
            .loaded      = false,
            .zero_i64    = zero_i64,
            .zero_f64    = zero_f64,
            .zero_u16    = zero_u16,
            .zero_u8     = zero_u8,
            .zero_u64    = zero_u64,
            .zero_str    = zero_str,
            .zero_astr   = zero_astr,
            .zero_nmask  = zero_nmask,
        };
    }

    fn loadColumns(self: *ScanState) !void {
        if (self.loaded) return;
        self.loaded = true;

        for (self.table.columns, 0..) |col, ci| {
            // Skip pruned columns.
            if (self.needed_cols) |nc| {
                if (!nc[ci]) continue;
            }

            if (col.ty == .text or col.ty == .char) {
                // String column: mmap .str.bin
                const path = try generic_store.columnStrBinPath(self.alloc, self.part_dir, col.name);
                defer self.alloc.free(path);
                const file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
                    // If file doesn't exist yet (empty store), leave as .none
                    if (err == error.FileNotFound) continue;
                    return err;
                };
                defer file.close(self.io);
                const stat = try file.stat(self.io);
                if (stat.size < 8) continue; // empty / partial
                const ptr = try std.posix.mmap(null, stat.size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
                const row_count = std.mem.readInt(u64, ptr[0..8], .little);
                const offsets_bytes = (row_count + 1) * 8;
                const offsets = std.mem.bytesAsSlice(u64, ptr[8..][0..offsets_bytes]);
                const bytes   = ptr[8 + offsets_bytes .. stat.size];
                self.col_data[ci] = .{ .string = .{
                    .ptr       = ptr,
                    .row_count = row_count,
                    .offsets   = offsets,
                    .bytes     = bytes,
                }};
            } else {
                // Fixed-width column: mmap .bin
                const path = try generic_store.columnBinPath(self.alloc, self.part_dir, col.name);
                defer self.alloc.free(path);
                const file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
                    if (err == error.FileNotFound) continue;
                    return err;
                };
                defer file.close(self.io);
                const stat = try file.stat(self.io);
                if (stat.size == 0) continue;
                const ptr = try std.posix.mmap(null, stat.size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
                self.col_data[ci] = .{ .fixed = .{ .ptr = ptr, .bytes = ptr[0..stat.size] }};
            }
        }
    }

    fn deinit(self: *ScanState) void {
        for (self.col_data) |cd| {
            switch (cd) {
                .fixed  => |m| m.deinit(),
                .string => |m| m.deinit(),
                .none   => {},
            }
        }
        self.alloc.free(self.col_data);
        self.alloc.free(self.metas);
        if (self.needed_cols) |nc| self.alloc.free(nc);
        self.alloc.free(self.zero_i64);
        self.alloc.free(self.zero_f64);
        self.alloc.free(self.zero_u16);
        self.alloc.free(self.zero_u8);
        self.alloc.free(self.zero_u64);
        self.alloc.free(self.zero_str);
        self.alloc.free(self.zero_astr);
        self.alloc.free(self.zero_nmask);
    }

    fn reset(self: *ScanState) void {
        self.rows_read = 0;
    }

    fn nextChunk(self: *ScanState, out: *DataChunk, ctx: *QueryContext) !bool {
        try self.loadColumns();

        if (self.rows_read >= self.row_count) return false;

        const remaining = self.row_count - self.rows_read;
        const n = @min(remaining, chunk.CHUNK_SIZE);

        const arena_alloc = ctx.allocator();
        out.* = .{
            .columns  = try arena_alloc.alloc(chunk.Column, self.table.columns.len),
            .num_rows = n,
            .arena    = std.heap.ArenaAllocator.init(arena_alloc),
        };
        const chunk_alloc = out.arena.allocator();
        const null_words  = chunk.nullMaskWords(n);
        const base = self.rows_read;

        // Shared zero null_mask for all pruned/non-nullable columns.
        // The pre-allocated buffer covers CHUNK_SIZE rows; trim to null_words.
        const zero_nm = self.zero_nmask[0..null_words];

        for (self.table.columns, 0..) |col, ci| {
            const core_ty   = toCoreColType(col.ty);
            const is_pruned = self.col_data[ci] == .none;

            var null_mask: []u64 = zero_nm;
            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    if (is_pruned) break :blk if (core_ty == .int64)
                        .{ .int64 = self.zero_i64[0..n] }
                    else
                        .{ .datetime64_ms = self.zero_i64[0..n] };
                    const buf = try chunk_alloc.alloc(i64, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const bytes = self.col_data[ci].fixed.bytes;
                    const width: usize = switch (col.ty) {
                        .int8  => 1,
                        .int16 => 2,
                        .int32, .date => 4,
                        else   => 8,
                    };
                    for (0..n) |i| {
                        const off = (base + i) * width;
                        buf[i] = switch (width) {
                            1 => @as(i8, @bitCast(bytes[off])),
                            2 => std.mem.readInt(i16, bytes[off..][0..2], .little),
                            4 => std.mem.readInt(i32, bytes[off..][0..4], .little),
                            else => std.mem.readInt(i64, bytes[off..][0..8], .little),
                        };
                    }
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    if (is_pruned) break :blk .{ .float64 = self.zero_f64[0..n] };
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const bytes = self.col_data[ci].fixed.bytes;
                    const width: usize = if (col.ty == .float32) 4 else 8;
                    for (0..n) |i| {
                        const off = (base + i) * width;
                        if (width == 4) {
                            const iv = std.mem.readInt(i32, bytes[off..][0..4], .little);
                            fbuf[i] = @floatCast(@as(f32, @bitCast(iv)));
                        } else {
                            const iv = std.mem.readInt(i64, bytes[off..][0..8], .little);
                            fbuf[i] = @bitCast(iv);
                        }
                    }
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    if (is_pruned) break :blk .{ .date_u16 = self.zero_u16[0..n] };
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const bytes = self.col_data[ci].fixed.bytes;
                    for (0..n) |i| {
                        const off = (base + i) * 4;
                        const iv = std.mem.readInt(i32, bytes[off..][0..4], .little);
                        ubuf[i] = @truncate(@as(u32, @bitCast(iv)));
                    }
                    break :blk .{ .date_u16 = ubuf };
                },
                .string => blk: {
                    if (is_pruned) break :blk .{ .string = self.zero_str[0..n] };
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const sc = &self.col_data[ci].string;
                    for (0..n) |i| {
                        sbuf[i] = sc.str(base + i);
                    }
                    break :blk .{ .string = sbuf };
                },
                .bool_u8 => blk: {
                    if (is_pruned) break :blk .{ .bool_u8 = self.zero_u8[0..n] };
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    @memset(bbuf, 0);
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    if (is_pruned) break :blk .{ .uint64 = self.zero_u64[0..n] };
                    const ubuf = try chunk_alloc.alloc(u64, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const bytes = self.col_data[ci].fixed.bytes;
                    for (0..n) |i| {
                        const off = (base + i) * 8;
                        ubuf[i] = std.mem.readInt(u64, bytes[off..][0..8], .little);
                    }
                    break :blk .{ .uint64 = ubuf };
                },
                .array_string => blk: {
                    if (is_pruned) break :blk .{ .array_string = self.zero_astr[0..n] };
                    const sbuf = try chunk_alloc.alloc([][]const u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    @memset(sbuf, &.{});
                    break :blk .{ .array_string = sbuf };
                },
            };

            out.columns[ci] = .{
                .name      = col.name,
                .data      = col_data,
                .null_mask = null_mask,
                .len       = n,
            };
        }

        self.rows_read += n;
        return true;
    }
};

// ── GenericStoreBridge — public API ───────────────────────────────────────────

pub const GenericStoreBridge = struct {
    state: *ScanState,
    alloc: std.mem.Allocator,

    /// `part_dir` must remain valid for the lifetime of the bridge.
    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        part_dir: []const u8,
        table: schema.Table,
        pruned_columns: []const []const u8,
    ) !GenericStoreBridge {
        const s = try alloc.create(ScanState);
        s.* = try ScanState.init(alloc, io, part_dir, table, pruned_columns);
        return .{ .state = s, .alloc = alloc };
    }

    pub fn deinit(self: *GenericStoreBridge) void {
        self.state.deinit();
        self.alloc.destroy(self.state);
    }

    pub fn source(self: *GenericStoreBridge) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) !bool {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        return self.state.nextChunk(out, ctx);
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        self.state.reset();
    }

    fn schemaFn(ptr: *anyopaque) []const ColMeta {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        return self.state.metas;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset     = resetFn,
        .schema    = schemaFn,
    };
};
