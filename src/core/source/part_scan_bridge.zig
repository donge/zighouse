/// part_scan_bridge.zig
///
/// Bridges ZigHouse's ClickHouse-format part reader (src/clickhouse_format/part.zig +
/// src/ingest/part_scanner.zig) into the SourceIface / PartReaderVTable expected
/// by the core pipeline.
///
/// Usage (from server.zig):
///
///   var bridge = try PartScanBridge.init(
///       allocator, io, data_dir, db, table_name, schema_table, part_dirs,
///   );
///   defer bridge.deinit();
///
///   var qctx = pipeline.QueryContext.init(allocator, bridge.source());
///   defer qctx.deinit();
///
///   const rs = try pipeline.executePlan(node, &qctx);
///
const std     = @import("std");
const schema  = @import("schema");
const core    = @import("core");
const chunk   = core.chunk;
const result  = core.result;
const pipeline = core.exec.pipeline;
const part_mod = @import("part");       // clickhouse_format/part.zig

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

/// Like toCoreColType, but also checks ch_type for Array(String) types.
/// Map columns still use .string (custom blob format for Map(String,String)).
fn toCoreColTypeFull(col: schema.Column) ColumnType {
    if (col.ch_type) |ch| {
        if (std.mem.startsWith(u8, ch, "Array("))
            return .array_string;
    }
    return toCoreColType(col.ty);
}

// ── ScanState — per-query state across all parts ──────────────────────────────

/// Internal scan state: walks through a list of part directories, reading
/// one chunk per call to nextChunk().
const ScanState = struct {
    alloc:       std.mem.Allocator,
    io:          std.Io,
    table:       schema.Table,
    part_dirs:   []const []const u8,
    part_idx:    usize,                      // which part we're on
    opened:      ?part_mod.OpenedPart,       // currently-open part
    rows_read:   u64,                        // rows read from current part
    col_readers: []?part_mod.ColumnReader,   // one per table column; null if unopened
    metas:       []ColMeta,                  // schema metas (allocated once)

    fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        table: schema.Table,
        part_dirs: []const []const u8,
    ) !ScanState {
        const metas = try alloc.alloc(ColMeta, table.columns.len);
        for (table.columns, 0..) |col, i| {
            metas[i] = .{
                .name    = col.name,
                .col_type = toCoreColTypeFull(col),
            };
        }
        const col_readers = try alloc.alloc(?part_mod.ColumnReader, table.columns.len);
        @memset(col_readers, null);
        return .{
            .alloc       = alloc,
            .io          = io,
            .table       = table,
            .part_dirs   = part_dirs,
            .part_idx    = 0,
            .opened      = null,
            .rows_read   = 0,
            .col_readers = col_readers,
            .metas       = metas,
        };
    }

    fn deinit(self: *ScanState) void {
        self.closeCurrentPart();
        self.alloc.free(self.col_readers);
        self.alloc.free(self.metas);
    }

    fn closeCurrentPart(self: *ScanState) void {
        for (self.col_readers) |*mr| {
            if (mr.*) |*cr| { cr.deinit(); mr.* = null; }
        }
        if (self.opened) |*op| { op.deinit(); self.opened = null; }
        self.rows_read = 0;
    }

    /// Open the next part in the list. Returns false if no more parts.
    fn openNextPart(self: *ScanState) !bool {
        self.closeCurrentPart();
        if (self.part_idx >= self.part_dirs.len) return false;
        const dir = self.part_dirs[self.part_idx];
        self.part_idx += 1;
        self.opened = try part_mod.OpenedPart.open(self.io, self.alloc, dir, self.table);
        for (self.table.columns, 0..) |_, i| {
            self.col_readers[i] = try self.opened.?.columnReader(i);
        }
        return true;
    }

    /// Read up to CHUNK_SIZE rows into a DataChunk. Returns false when all parts exhausted.
    fn nextChunk(self: *ScanState, out: *DataChunk, ctx: *QueryContext) !bool {
        // Advance to a part with remaining rows.
        while (true) {
            if (self.opened) |*op| {
                if (self.rows_read < op.row_count) break;
            }
            const ok = try self.openNextPart();
            if (!ok) return false;
        }
        const op = &self.opened.?;
        const remaining = op.row_count - self.rows_read;
        const n = @min(remaining, chunk.CHUNK_SIZE);

        const arena_alloc = ctx.allocator();
        // Build chunk
        out.* = .{
            .columns  = try arena_alloc.alloc(chunk.Column, self.table.columns.len),
            .num_rows = n,
            .arena    = std.heap.ArenaAllocator.init(arena_alloc),
        };
        const chunk_alloc = out.arena.allocator();
        const null_words  = chunk.nullMaskWords(n);

        for (self.table.columns, 0..) |col, ci| {
            const null_mask = try chunk_alloc.alloc(u64, null_words);
            @memset(null_mask, 0);
            const core_ty = toCoreColTypeFull(col);
            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    const buf = try chunk_alloc.alloc(i64, n);
                    const actual = try self.col_readers[ci].?.readFixed(buf);
                    // Zero out any unread tail (shouldn't happen in well-formed parts)
                    if (actual < n) @memset(buf[actual..], 0);
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    const ibuf = try chunk_alloc.alloc(i64, n);
                    _ = try self.col_readers[ci].?.readFixed(ibuf);
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    for (ibuf, 0..) |iv, i| fbuf[i] = @bitCast(iv);
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    const ibuf = try chunk_alloc.alloc(i64, n);
                    _ = try self.col_readers[ci].?.readFixed(ibuf);
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    for (ibuf, 0..) |iv, i| ubuf[i] = @intCast(@as(u16, @truncate(@as(u64, @bitCast(iv)))));
                    break :blk .{ .date_u16 = ubuf };
                },
                .bool_u8 => blk: {
                    const ibuf = try chunk_alloc.alloc(i64, n);
                    _ = try self.col_readers[ci].?.readFixed(ibuf);
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    for (ibuf, 0..) |iv, i| bbuf[i] = @intCast(@as(u8, @truncate(@as(u64, @bitCast(iv)))));
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    const ibuf = try chunk_alloc.alloc(i64, n);
                    _ = try self.col_readers[ci].?.readFixed(ibuf);
                    const ubuf = try chunk_alloc.alloc(u64, n);
                    for (ibuf, 0..) |iv, i| ubuf[i] = @bitCast(iv);
                    break :blk .{ .uint64 = ubuf };
                },
                .string => blk: {
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    const Ctx = struct {
                        buf:   [][]const u8,
                        idx:   usize,
                        alloc: std.mem.Allocator,
                    };
                    var sctx = Ctx{ .buf = sbuf, .idx = 0, .alloc = chunk_alloc };
                    _ = try self.col_readers[ci].?.readStrings(n, &sctx,
                        struct {
                            fn cb(c: *Ctx, str: []const u8) !void {
                                c.buf[c.idx] = try c.alloc.dupe(u8, str);
                                c.idx += 1;
                            }
                        }.cb,
                    );
                    break :blk .{ .string = sbuf };
                },
                .array_string => blk: {
                    const sbuf = try chunk_alloc.alloc([][]const u8, n);
                    // Use readArrayStrings to decode Array(String) columns from parts.
                    if (self.col_readers[ci]) |*cr| {
                        const Ctx2 = struct {
                            buf:   [][][]const u8,
                            idx:   usize,
                        };
                        var ctx2 = Ctx2{ .buf = sbuf, .idx = 0 };
                        _ = cr.readArrayStrings(n, chunk_alloc, &ctx2,
                            struct {
                                fn cb(c: *Ctx2, arr: [][]const u8) !void {
                                    c.buf[c.idx] = arr;
                                    c.idx += 1;
                                }
                            }.cb,
                        ) catch |err| {
                            // If readArrayStrings fails (wrong format), fall back to empty
                            std.log.warn("readArrayStrings error at row {}: {}", .{ctx2.idx, err});
                            @memset(sbuf[ctx2.idx..], &.{});
                        };
                        // Fill any unread rows with empty
                        while (ctx2.idx < n) : (ctx2.idx += 1) sbuf[ctx2.idx] = &.{};
                    } else {
                        @memset(sbuf, &.{});
                    }
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

    fn reset(self: *ScanState) void {
        self.closeCurrentPart();
        self.part_idx = 0;
    }
};

// ── PartScanBridge — public API ───────────────────────────────────────────────

pub const PartScanBridge = struct {
    state: *ScanState,
    alloc: std.mem.Allocator,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        table: schema.Table,
        part_dirs: []const []const u8,
    ) !PartScanBridge {
        const s = try alloc.create(ScanState);
        s.* = try ScanState.init(alloc, io, table, part_dirs);
        return .{ .state = s, .alloc = alloc };
    }

    pub fn deinit(self: *PartScanBridge) void {
        self.state.deinit();
        self.alloc.destroy(self.state);
    }

    pub fn source(self: *PartScanBridge) SourceIface {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn nextChunkFn(ptr: *anyopaque, out: *DataChunk, ctx: *QueryContext) !bool {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.nextChunk(out, ctx);
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        self.state.reset();
    }

    fn schemaFn(ptr: *anyopaque) []const ColMeta {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.metas;
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset     = resetFn,
        .schema    = schemaFn,
    };
};
