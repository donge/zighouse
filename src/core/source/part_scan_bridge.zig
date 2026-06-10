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

const RawStringCol = struct {
    offsets: []u64,
    bytes: []u8,

    fn deinit(self: RawStringCol, alloc: std.mem.Allocator) void {
        alloc.free(self.offsets);
        alloc.free(self.bytes);
    }
};

const RawColData = union(enum) {
    none,
    i16s: []i16,
    i32s: []i32,
    i64s: []i64,
    string: RawStringCol,

    fn deinit(self: RawColData, alloc: std.mem.Allocator) void {
        switch (self) {
            .none => {},
            .i16s => |s| alloc.free(s),
            .i32s => |s| alloc.free(s),
            .i64s => |s| alloc.free(s),
            .string => |s| s.deinit(alloc),
        }
    }
};

// ── schema.ColumnType → core.ColumnType ──────────────────────────────────────

fn toCoreColType(ty: schema.ColumnType) ColumnType {
    return switch (ty) {
        .int8, .int16, .int32, .int64 => .int64,
        .float32, .float64            => .float64,
        .date                         => .date_u16,
        .timestamp                    => .datetime64_ms,
        .text, .char, .low_card          => .string,
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
    part_idx:    usize,                          // which part we're on
    opened:      ?part_mod.OpenedPartAny,        // currently-open part (wide or compact)
    rows_read:   u64,                            // rows read from current part
    row_count:   u64,                            // total rows across all parts
    col_readers: []?part_mod.ColumnReader,       // one per table column; null if unopened
    raw_cols:    []RawColData,                   // lazy all-part raw views
    raw_lock:    std.atomic.Value(u32),
    metas:       []ColMeta,                      // schema metas (allocated once)
    /// If non-null, only columns marked `true` are read from disk.
    /// Length == table.columns.len.  null means read all.
    needed_cols: ?[]bool,
    /// Runtime override used by late-materialized physical strategies.
    override_needed: [256]bool,
    override_active: bool,
    /// String columns decoded as bool_u8 for `col <> ''` predicates.
    nonempty_bool: [256]bool,

    fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        table: schema.Table,
        part_dirs: []const []const u8,
        /// Optional list of column names to read. Empty slice = read all.
        pruned_columns: []const []const u8,
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
        const raw_cols = try alloc.alloc(RawColData, table.columns.len);
        @memset(raw_cols, .none);

        var total_rows: u64 = 0;
        for (part_dirs) |dir| {
            total_rows += try readPartCount(alloc, io, dir);
        }

        // Build needed_cols mask if pruned list is provided.
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

        return .{
            .alloc       = alloc,
            .io          = io,
            .table       = table,
            .part_dirs   = part_dirs,
            .part_idx    = 0,
            .opened      = null,
            .rows_read   = 0,
            .row_count   = total_rows,
            .col_readers = col_readers,
            .raw_cols    = raw_cols,
            .raw_lock    = .init(0),
            .metas       = metas,
            .needed_cols = needed_cols,
            .override_needed = [_]bool{false} ** 256,
            .override_active = false,
            .nonempty_bool = [_]bool{false} ** 256,
        };
    }

    fn deinit(self: *ScanState) void {
        self.closeCurrentPart();
        for (self.raw_cols) |rc| rc.deinit(self.alloc);
        self.alloc.free(self.raw_cols);
        self.alloc.free(self.col_readers);
        self.alloc.free(self.metas);
        if (self.needed_cols) |nc| self.alloc.free(nc);
    }

    fn closeCurrentPart(self: *ScanState) void {
        for (self.col_readers) |*mr| {
            if (mr.*) |*cr| { cr.deinit(); mr.* = null; }
        }
        if (self.opened) |*op| { op.deinit(); self.opened = null; }
        self.rows_read = 0;
    }

    inline fn isNeeded(self: *const ScanState, ci: usize) bool {
        if (self.override_active) {
            return ci < 256 and self.override_needed[ci];
        }
        if (self.needed_cols) |nc| return ci < nc.len and nc[ci];
        return true;
    }

    fn colIndex(self: *const ScanState, col_name: []const u8) ?usize {
        for (self.table.columns, 0..) |col, ci| {
            if (std.mem.eql(u8, col.name, col_name)) return ci;
        }
        return null;
    }

    fn lockRaw(self: *ScanState) void {
        while (self.raw_lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    fn unlockRaw(self: *ScanState) void {
        self.raw_lock.store(0, .release);
    }

    fn readFixedInto(self: *ScanState, ci: usize, out: anytype) !void {
        const Slice = @TypeOf(out);
        const Elem = @typeInfo(Slice).pointer.child;
        var tmp_buf: [8192]i64 = undefined;
        var pos: usize = 0;
        for (self.part_dirs) |dir| {
            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReader(ci);
            defer cr.deinit();
            while (pos < out.len) {
                const want = @min(tmp_buf.len, out.len - pos);
                const got = try cr.readFixed(tmp_buf[0..want]);
                if (got == 0) break;
                for (tmp_buf[0..got], 0..) |v, i| {
                    out[pos + i] = if (Elem == i64) v else @intCast(v);
                }
                pos += got;
            }
        }
    }

    fn ensureRawFixed(self: *ScanState, ci: usize) !void {
        if (self.raw_cols[ci] != .none) return;
        self.lockRaw();
        defer self.unlockRaw();
        if (self.raw_cols[ci] != .none) return;
        const col = self.table.columns[ci];
        const total: usize = @intCast(self.row_count);

        switch (col.ty) {
            .int8, .float32, .float64 => {
                const out = try self.alloc.alloc(i64, total);
                errdefer self.alloc.free(out);
                try self.readFixedInto(ci, out);
                self.raw_cols[ci] = .{ .i64s = out };
            },
            .int16 => {
                const out = try self.alloc.alloc(i16, total);
                errdefer self.alloc.free(out);
                try self.readFixedInto(ci, out);
                self.raw_cols[ci] = .{ .i16s = out };
            },
            .int32, .date => {
                const out = try self.alloc.alloc(i32, total);
                errdefer self.alloc.free(out);
                try self.readFixedInto(ci, out);
                self.raw_cols[ci] = .{ .i32s = out };
            },
            .int64, .timestamp => {
                const out = try self.alloc.alloc(i64, total);
                errdefer self.alloc.free(out);
                try self.readFixedInto(ci, out);
                self.raw_cols[ci] = .{ .i64s = out };
            },
            else => {},
        }
    }

    fn ensureRawString(self: *ScanState, ci: usize) !void {
        if (self.raw_cols[ci] != .none) return;
        self.lockRaw();
        defer self.unlockRaw();
        if (self.raw_cols[ci] != .none) return;
        const col = self.table.columns[ci];
        switch (col.ty) {
            .text, .char, .low_card => {},
            else => return,
        }

        const total: usize = @intCast(self.row_count);
        const offsets = try self.alloc.alloc(u64, total + 1);
        errdefer self.alloc.free(offsets);
        offsets[0] = 0;
        var bytes: std.ArrayListUnmanaged(u8) = .empty;
        errdefer bytes.deinit(self.alloc);

        const Ctx = struct {
            alloc: std.mem.Allocator,
            offsets: []u64,
            row: usize,
            bytes: *std.ArrayListUnmanaged(u8),
        };

        var ctx = Ctx{
            .alloc = self.alloc,
            .offsets = offsets,
            .row = 0,
            .bytes = &bytes,
        };
        for (self.part_dirs) |dir| {
            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReader(ci);
            defer cr.deinit();
            while (ctx.row < total) {
                const remaining = total - ctx.row;
                const got = try cr.readStrings(@min(remaining, chunk.CHUNK_SIZE), &ctx,
                    struct {
                        fn cb(c: *Ctx, str: []const u8) !void {
                            try c.bytes.appendSlice(c.alloc, str);
                            c.row += 1;
                            c.offsets[c.row] = c.bytes.items.len;
                        }
                    }.cb,
                );
                if (got == 0) break;
            }
        }

        self.raw_cols[ci] = .{ .string = .{
            .offsets = offsets,
            .bytes = try bytes.toOwnedSlice(self.alloc),
        }};
    }

    fn readArrayRange(self: *ScanState, ci: usize, start: u64, n: usize, out: [][][]const u8, alloc: std.mem.Allocator) !void {
        var global_base: u64 = 0;
        var dst: usize = 0;
        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (start >= global_base + part_rows) continue;
            if (start + n <= global_base) break;

            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReader(ci);
            defer cr.deinit();

            const local_start: usize = if (start > global_base) @intCast(start - global_base) else 0;
            if (local_start > 0) _ = try cr.skipArrayStrings(local_start);
            const available: usize = @intCast(part_rows - local_start);
            const want = @min(n - dst, available);
            const Ctx = struct {
                out: [][][]const u8,
                idx: usize,
            };
            var ctx = Ctx{ .out = out, .idx = dst };
            _ = try cr.readArrayStrings(want, alloc, &ctx,
                struct {
                    fn cb(c: *Ctx, arr: [][]const u8) !void {
                        c.out[c.idx] = arr;
                        c.idx += 1;
                    }
                }.cb,
            );
            dst = ctx.idx;
            if (dst >= n) break;
        }
        while (dst < n) : (dst += 1) out[dst] = &.{};
    }

    fn fillRange(self: *ScanState, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        out.* = .{
            .columns = undefined,
            .num_rows = n,
            .arena = std.heap.ArenaAllocator.init(alloc),
        };
        const chunk_alloc = out.arena.allocator();
        out.columns = try chunk_alloc.alloc(chunk.Column, self.table.columns.len);
        const null_words = chunk.nullMaskWords(n);
        const end: usize = @intCast(start + n);
        const base: usize = @intCast(start);

        for (self.table.columns, 0..) |col, ci| {
            const null_mask = try chunk_alloc.alloc(u64, null_words);
            @memset(null_mask, 0);
            const core_ty = toCoreColTypeFull(col);
            const is_pruned = !self.isNeeded(ci);

            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    const buf = try chunk_alloc.alloc(i64, n);
                    if (is_pruned) {
                        @memset(buf, 0);
                    } else {
                        try self.ensureRawFixed(ci);
                        switch (self.raw_cols[ci]) {
                            .i16s => |s| {
                                for (s[base..end], 0..) |v, i| buf[i] = v;
                            },
                            .i32s => |s| {
                                for (s[base..end], 0..) |v, i| buf[i] = v;
                            },
                            .i64s => |s| {
                                @memcpy(buf, s[base..end]);
                            },
                            else => {
                                @memset(buf, 0);
                            },
                        }
                    }
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    if (is_pruned) {
                        @memset(fbuf, 0.0);
                    } else {
                        try self.ensureRawFixed(ci);
                        switch (self.raw_cols[ci]) {
                            .i64s => |s| {
                                if (col.ty == .float32) {
                                    for (s[base..end], 0..) |iv, i| {
                                        fbuf[i] = @floatCast(@as(f32, @bitCast(@as(u32, @truncate(@as(u64, @bitCast(iv)))))));
                                    }
                                } else {
                                    for (s[base..end], 0..) |iv, i| fbuf[i] = @bitCast(iv);
                                }
                            },
                            else => {
                                @memset(fbuf, 0.0);
                            },
                        }
                    }
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        try self.ensureRawFixed(ci);
                        switch (self.raw_cols[ci]) {
                            .i32s => |s| {
                                for (s[base..end], 0..) |v, i| ubuf[i] = @truncate(@as(u32, @bitCast(v)));
                            },
                            else => {
                                @memset(ubuf, 0);
                            },
                        }
                    }
                    break :blk .{ .date_u16 = ubuf };
                },
                .bool_u8 => blk: {
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    if (is_pruned) {
                        @memset(bbuf, 0);
                    } else {
                        try self.ensureRawFixed(ci);
                        switch (self.raw_cols[ci]) {
                            .i64s => |s| {
                                for (s[base..end], 0..) |v, i| bbuf[i] = @intCast(@as(u8, @truncate(@as(u64, @bitCast(v)))));
                            },
                            else => {
                                @memset(bbuf, 0);
                            },
                        }
                    }
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    const ubuf = try chunk_alloc.alloc(u64, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        try self.ensureRawFixed(ci);
                        switch (self.raw_cols[ci]) {
                            .i64s => |s| {
                                for (s[base..end], 0..) |v, i| ubuf[i] = @bitCast(v);
                            },
                            else => {
                                @memset(ubuf, 0);
                            },
                        }
                    }
                    break :blk .{ .uint64 = ubuf };
                },
                .string => blk: {
                    if (ci < 256 and self.nonempty_bool[ci]) {
                        const bbuf = try chunk_alloc.alloc(u8, n);
                        if (is_pruned) {
                            @memset(bbuf, 0);
                        } else {
                            try self.ensureRawString(ci);
                            switch (self.raw_cols[ci]) {
                                .string => |s| {
                                    for (0..n) |i| {
                                        bbuf[i] = if (s.offsets[base + i + 1] > s.offsets[base + i]) 1 else 0;
                                    }
                                },
                                else => {
                                    @memset(bbuf, 0);
                                },
                            }
                        }
                        break :blk .{ .bool_u8 = bbuf };
                    }
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    if (is_pruned) {
                        @memset(sbuf, "");
                    } else {
                        try self.ensureRawString(ci);
                        switch (self.raw_cols[ci]) {
                            .string => |s| {
                                for (0..n) |i| {
                                    const lo: usize = @intCast(s.offsets[base + i]);
                                    const hi: usize = @intCast(s.offsets[base + i + 1]);
                                    sbuf[i] = s.bytes[lo..hi];
                                }
                            },
                            else => {
                                @memset(sbuf, "");
                            },
                        }
                    }
                    break :blk .{ .string = sbuf };
                },
                .array_string => blk: {
                    const abuf = try chunk_alloc.alloc([][]const u8, n);
                    if (is_pruned) {
                        @memset(abuf, &.{});
                    } else {
                        try self.readArrayRange(ci, start, n, abuf, chunk_alloc);
                    }
                    break :blk .{ .array_string = abuf };
                },
            };

            out.columns[ci] = .{
                .name = col.name,
                .data = col_data,
                .null_mask = null_mask,
                .len = n,
                .pruned = is_pruned,
            };
        }
    }

    /// Open the next part in the list. Returns false if no more parts.
    fn openNextPart(self: *ScanState) !bool {
        self.closeCurrentPart();
        if (self.part_idx >= self.part_dirs.len) return false;
        const dir = self.part_dirs[self.part_idx];
        self.part_idx += 1;
        self.opened = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
        for (self.table.columns, 0..) |_, i| {
            // Skip columns not in the needed set (column pruning).
            if (!self.isNeeded(i)) continue;
            self.col_readers[i] = try self.opened.?.columnReader(i);
        }
        return true;
    }

    /// Read up to CHUNK_SIZE rows into a DataChunk. Returns false when all parts exhausted.
    fn nextChunk(self: *ScanState, out: *DataChunk, ctx: *QueryContext) !bool {
        // Advance to a part with remaining rows.
        while (true) {
            if (self.opened) |*op| {
                if (self.rows_read < op.rowCount()) break;
            }
            const ok = try self.openNextPart();
            if (!ok) return false;
        }
        const op = &self.opened.?;
        const remaining = op.rowCount() - self.rows_read;
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
            // Pruned column: produce zero/empty data without reading from disk.
            const is_pruned = self.col_readers[ci] == null and !self.isNeeded(ci);
            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    const buf = try chunk_alloc.alloc(i64, n);
                    if (is_pruned) {
                        @memset(buf, 0);
                    } else {
                        const actual = try self.col_readers[ci].?.readFixed(buf);
                        if (actual < n) @memset(buf[actual..], 0);
                    }
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    if (is_pruned) {
                        @memset(fbuf, 0.0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        _ = try self.col_readers[ci].?.readFixed(ibuf);
                        for (ibuf, 0..) |iv, i| fbuf[i] = @bitCast(iv);
                    }
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        _ = try self.col_readers[ci].?.readFixed(ibuf);
                        for (ibuf, 0..) |iv, i| ubuf[i] = @intCast(@as(u16, @truncate(@as(u64, @bitCast(iv)))));
                    }
                    break :blk .{ .date_u16 = ubuf };
                },
                .bool_u8 => blk: {
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    if (is_pruned) {
                        @memset(bbuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        _ = try self.col_readers[ci].?.readFixed(ibuf);
                        for (ibuf, 0..) |iv, i| bbuf[i] = @intCast(@as(u8, @truncate(@as(u64, @bitCast(iv)))));
                    }
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    const ubuf = try chunk_alloc.alloc(u64, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        _ = try self.col_readers[ci].?.readFixed(ibuf);
                        for (ibuf, 0..) |iv, i| ubuf[i] = @bitCast(iv);
                    }
                    break :blk .{ .uint64 = ubuf };
                },
                .string => blk: {
                    if (ci < 256 and self.nonempty_bool[ci]) {
                        const bbuf = try chunk_alloc.alloc(u8, n);
                        if (is_pruned) {
                            @memset(bbuf, 0);
                        } else {
                            const Ctx = struct {
                                buf: []u8,
                                idx: usize,
                            };
                            var sctx = Ctx{ .buf = bbuf, .idx = 0 };
                            const actual = try self.col_readers[ci].?.readStrings(n, &sctx,
                                struct {
                                    fn cb(c: *Ctx, str: []const u8) !void {
                                        c.buf[c.idx] = if (str.len != 0) 1 else 0;
                                        c.idx += 1;
                                    }
                                }.cb,
                            );
                            if (actual < n) @memset(bbuf[actual..], 0);
                        }
                        break :blk .{ .bool_u8 = bbuf };
                    }
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    if (is_pruned) {
                        @memset(sbuf, "");
                    } else {
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
                    }
                    break :blk .{ .string = sbuf };
                },
                .array_string => blk: {
                    const sbuf = try chunk_alloc.alloc([][]const u8, n);
                    if (is_pruned) {
                        @memset(sbuf, &.{});
                    } else {
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
                    } // end else (not pruned)
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

fn readPartCount(alloc: std.mem.Allocator, io: std.Io, part_dir: []const u8) !u64 {
    const path = try std.fmt.allocPrint(alloc, "{s}/count.txt", .{part_dir});
    defer alloc.free(path);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(128));
    defer alloc.free(raw);
    return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

// ── PartScanBridge — public API ───────────────────────────────────────────────

pub const PartScanBridge = struct {
    state: *ScanState,
    alloc: std.mem.Allocator,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        table: schema.Table,
        part_dirs: []const []const u8,
        pruned_columns: []const []const u8,
    ) !PartScanBridge {
        const s = try alloc.create(ScanState);
        s.* = try ScanState.init(alloc, io, table, part_dirs, pruned_columns);
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

    fn fetchRangeFn(ptr: *anyopaque, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        try self.state.fillRange(start, n, out, alloc);
    }

    fn resetFn(ptr: *anyopaque) void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        self.state.reset();
    }

    fn schemaFn(ptr: *anyopaque) []const ColMeta {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.metas;
    }

    fn rowCountFn(ptr: *anyopaque) u64 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.row_count;
    }

    fn setNeededColsFn(ptr: *anyopaque, col_names: ?[]const []const u8) void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        if (col_names == null) {
            s.override_active = false;
            return;
        }
        @memset(&s.override_needed, false);
        for (col_names.?) |name| {
            for (s.table.columns, 0..) |col, ci| {
                if (ci >= 256) break;
                if (std.mem.eql(u8, col.name, name)) {
                    s.override_needed[ci] = true;
                    break;
                }
            }
        }
        s.override_active = true;
    }

    fn setStringNonEmptyBoolFn(ptr: *anyopaque, col_name: ?[]const u8) void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        if (col_name == null) {
            @memset(&s.nonempty_bool, false);
            return;
        }
        for (s.table.columns, 0..) |col, ci| {
            if (ci >= 256) break;
            if (std.mem.eql(u8, col.name, col_name.?)) {
                s.nonempty_bool[ci] = true;
                break;
            }
        }
    }

    fn getSortKeysFn(ptr: *anyopaque) []const []const u8 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.table.sort_keys;
    }

    fn getRawInt16ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i16 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const ci = self.state.colIndex(col_name) orelse return null;
        if (self.state.table.columns[ci].ty != .int16) return null;
        self.state.ensureRawFixed(ci) catch return null;
        return switch (self.state.raw_cols[ci]) {
            .i16s => |s| s,
            else => null,
        };
    }

    fn getRawInt32ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i32 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const ci = self.state.colIndex(col_name) orelse return null;
        const ty = self.state.table.columns[ci].ty;
        if (ty != .int32 and ty != .date) return null;
        self.state.ensureRawFixed(ci) catch return null;
        return switch (self.state.raw_cols[ci]) {
            .i32s => |s| s,
            else => null,
        };
    }

    fn getRawInt64ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i64 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const ci = self.state.colIndex(col_name) orelse return null;
        const ty = self.state.table.columns[ci].ty;
        if (ty != .int64 and ty != .timestamp) return null;
        self.state.ensureRawFixed(ci) catch return null;
        return switch (self.state.raw_cols[ci]) {
            .i64s => |s| s,
            else => null,
        };
    }

    fn getRawStrOffsetsFn(ptr: *anyopaque, col_name: []const u8) ?[]const u64 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const ci = self.state.colIndex(col_name) orelse return null;
        const ty = self.state.table.columns[ci].ty;
        if (ty != .text and ty != .char and ty != .low_card) return null;
        self.state.ensureRawString(ci) catch return null;
        return switch (self.state.raw_cols[ci]) {
            .string => |s| s.offsets,
            else => null,
        };
    }

    fn getRawStrBytesFn(ptr: *anyopaque, col_name: []const u8) ?[]const u8 {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        const ci = self.state.colIndex(col_name) orelse return null;
        const ty = self.state.table.columns[ci].ty;
        if (ty != .text and ty != .char and ty != .low_card) return null;
        self.state.ensureRawString(ci) catch return null;
        return switch (self.state.raw_cols[ci]) {
            .string => |s| s.bytes,
            else => null,
        };
    }

    const vtable = SourceIface.VTable{
        .nextChunk             = nextChunkFn,
        .reset                 = resetFn,
        .schema                = schemaFn,
        .rowCount              = rowCountFn,
        .fetchRange            = fetchRangeFn,
        .setNeededCols         = setNeededColsFn,
        .setStringNonEmptyBool = setStringNonEmptyBoolFn,
        .getRawInt16Col        = getRawInt16ColFn,
        .getRawInt32Col        = getRawInt32ColFn,
        .getRawInt64Col        = getRawInt64ColFn,
        .getRawStrOffsets      = getRawStrOffsetsFn,
        .getRawStrBytes        = getRawStrBytesFn,
        .getSortKeys           = getSortKeysFn,
    };
};

test "PartScanBridge exposes raw views for compact parts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_part_scan_bridge_raw";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
    }

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "ts", .ty = .int64 },
        .{ .name = "name", .ty = .text },
    };
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "raw_bridge", .columns = &cols, .sort_keys = &sort_keys };

    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 1, 2, 3 };
        const ts = [_]i64{ 10, 20, 30 };
        try cp.appendFixedBatch(0, &ids);
        try cp.appendFixedBatch(1, &ts);
        try cp.appendString(2, "aa");
        try cp.appendString(2, "");
        try cp.appendString(2, "bbb");
        try cp.finish();
    }

    var bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{part_dir}, &.{});
    defer bridge.deinit();
    const source = bridge.source();

    try std.testing.expectEqual(@as(u64, 3), source.rowCount());
    try std.testing.expectEqualStrings("id", source.getSortKeys()[0]);

    const ids_raw = source.getRawInt32Col("id") orelse return error.TestExpectedRawInt32;
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3 }, ids_raw);
    const ts_raw = source.getRawInt64Col("ts") orelse return error.TestExpectedRawInt64;
    try std.testing.expectEqualSlices(i64, &[_]i64{ 10, 20, 30 }, ts_raw);

    const offsets = source.getRawStrOffsets("name") orelse return error.TestExpectedRawStrOffsets;
    const bytes = source.getRawStrBytes("name") orelse return error.TestExpectedRawStrBytes;
    try std.testing.expectEqualSlices(u64, &[_]u64{ 0, 2, 2, 5 }, offsets);
    try std.testing.expectEqualStrings("aabbb", bytes);

    source.setStringNonEmptyBool("name");
    defer source.setStringNonEmptyBool(null);
    var qctx = QueryContext.init(allocator, source);
    defer qctx.deinit();
    var out: DataChunk = undefined;
    try std.testing.expect(try source.nextChunk(&out, &qctx));
    defer out.deinit();
    try std.testing.expectEqual(chunk.ColumnData.bool_u8, std.meta.activeTag(out.columns[2].data));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 0, 1 }, out.columns[2].data.bool_u8);

    source.setStringNonEmptyBool(null);
    try std.testing.expect(source.supportsRange());
    var range_out: DataChunk = undefined;
    try source.fetchRange(1, 2, &range_out, allocator);
    defer range_out.deinit();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, range_out.columns[0].data.int64);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 20, 30 }, range_out.columns[1].data.int64);
    try std.testing.expectEqualStrings("", range_out.columns[2].data.string[0]);
    try std.testing.expectEqualStrings("bbb", range_out.columns[2].data.string[1]);

    const only_id = [_][]const u8{"id"};
    source.setNeededCols(&only_id);
    defer source.setNeededCols(null);
    var pruned_out: DataChunk = undefined;
    try source.fetchRange(0, 1, &pruned_out, allocator);
    defer pruned_out.deinit();
    try std.testing.expect(!pruned_out.columns[0].pruned);
    try std.testing.expect(pruned_out.columns[1].pruned);
    try std.testing.expect(pruned_out.columns[2].pruned);
    try std.testing.expectEqual(@as(i64, 1), pruned_out.columns[0].data.int64[0]);
    try std.testing.expectEqual(@as(i64, 0), pruned_out.columns[1].data.int64[0]);
}
