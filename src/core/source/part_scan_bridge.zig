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
const generic_store = @import("generic_store");
const generic_store_bridge = @import("generic_store_bridge");
const part_mod = @import("part");       // clickhouse_format/part.zig

pub const DataChunk    = chunk.DataChunk;
pub const ColumnType   = chunk.ColumnType;
pub const SourceIface  = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;
pub const ColMeta      = result.ColMeta;
const plan = core.exec.plan;

const RAW_MATERIALIZE_MAX_COMPACT_BYTES: u64 = 256 * 1024 * 1024;
const RAW_MATERIALIZE_MAX_COMPACT_ROWS: u64 = 2_000_000;

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
    raw_materialize_allowed: bool,
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
    scan_lo: u64,
    scan_hi: u64,

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
        var raw_materialize_allowed = true;
        for (part_dirs) |dir| {
            const part_rows = try readPartCount(alloc, io, dir);
            total_rows += part_rows;
            if (part_rows > RAW_MATERIALIZE_MAX_COMPACT_ROWS or try isLargeCompactPart(alloc, io, dir, part_rows)) {
                raw_materialize_allowed = false;
            }
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
            .raw_materialize_allowed = raw_materialize_allowed,
            .raw_lock    = .init(0),
            .metas       = metas,
            .needed_cols = needed_cols,
            .override_needed = [_]bool{false} ** 256,
            .override_active = false,
            .nonempty_bool = [_]bool{false} ** 256,
            .scan_lo = 0,
            .scan_hi = std.math.maxInt(u64),
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

    inline fn effectiveLo(self: *const ScanState) u64 {
        return @min(self.scan_lo, self.row_count);
    }

    inline fn effectiveHi(self: *const ScanState) u64 {
        return @min(self.scan_hi, self.row_count);
    }

    inline fn effectiveCount(self: *const ScanState) u64 {
        const lo = self.effectiveLo();
        const hi = self.effectiveHi();
        return if (hi > lo) hi - lo else 0;
    }

    inline fn rangeActive(self: *const ScanState) bool {
        return self.effectiveLo() != 0 or self.effectiveHi() != self.row_count;
    }

    fn colIndex(self: *const ScanState, col_name: []const u8) ?usize {
        for (self.table.columns, 0..) |col, ci| {
            if (std.mem.eql(u8, col.name, col_name)) return ci;
        }
        return null;
    }

    fn lowerBoundI64(values: []const i64, target: i64) usize {
        var lo: usize = 0;
        var hi: usize = values.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (values[mid] < target) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn upperBoundI64(values: []const i64, target: i64) usize {
        var lo: usize = 0;
        var hi: usize = values.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (values[mid] <= target) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn findIntRange(self: *ScanState, col_name: []const u8, value: i64) !?SourceIface.RowRange {
        if (self.table.sort_keys.len == 0 or !std.mem.eql(u8, self.table.sort_keys[0], col_name)) return null;
        const ci = self.colIndex(col_name) orelse return null;
        const col = self.table.columns[ci];
        if (col.ty != .int16 and col.ty != .int32 and col.ty != .int64 and col.ty != .date and col.ty != .timestamp) return null;

        var global_base: u64 = 0;
        var range_lo: ?u64 = null;
        var range_hi: u64 = 0;

        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (part_rows == 0) continue;

            const pk_values = part_mod.readCompactPrimaryFixedValues(self.io, self.alloc, dir, col.ty) catch return null;
            defer self.alloc.free(pk_values);
            const expected_granules: usize = @intCast((part_rows + part_mod.GRANULE_SIZE - 1) / part_mod.GRANULE_SIZE);
            if (pk_values.len != expected_granules) return null;

            const lb = lowerBoundI64(pk_values, value);
            const start_granule = if (lb == 0) 0 else lb - 1;
            var end_granule = upperBoundI64(pk_values, value);
            if (end_granule <= start_granule) end_granule = start_granule + 1;
            end_granule = @min(end_granule, pk_values.len);

            const local_start = @as(u64, @intCast(start_granule)) * part_mod.GRANULE_SIZE;
            const local_end = @min(part_rows, @as(u64, @intCast(end_granule)) * part_mod.GRANULE_SIZE);
            if (local_end <= local_start) continue;

            const n: usize = @intCast(local_end - local_start);
            const vals = try self.alloc.alloc(i64, n);
            defer self.alloc.free(vals);
            try self.readFixedRange(ci, global_base + local_start, n, vals);

            const exact_lo = lowerBoundI64(vals, value);
            const exact_hi = upperBoundI64(vals, value);
            if (exact_hi > exact_lo) {
                const global_lo = global_base + local_start + exact_lo;
                const global_hi = global_base + local_start + exact_hi;
                if (range_lo == null or global_lo < range_lo.?) range_lo = global_lo;
                if (global_hi > range_hi) range_hi = global_hi;
            }
        }

        return if (range_lo) |lo|
            .{ .lo = lo, .hi = range_hi }
        else
            .{ .lo = 0, .hi = 0 };
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
        if (!self.raw_materialize_allowed) return;
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
        if (!self.raw_materialize_allowed) return;
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

    fn readFixedRange(self: *ScanState, ci: usize, start: u64, n: usize, out: []i64) !void {
        var global_base: u64 = 0;
        var dst: usize = 0;
        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (start >= global_base + part_rows) continue;
            if (start + n <= global_base) break;

            const local_start: u64 = if (start > global_base) start - global_base else 0;
            const available: usize = @intCast(part_rows - local_start);
            const want = @min(n - dst, available);

            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReaderRange(ci, local_start, want);
            defer cr.deinit();
            const got = try cr.readFixed(out[dst .. dst + want]);
            if (got < want) @memset(out[dst + got .. dst + want], 0);
            dst += want;
            if (dst >= n) break;
        }
        if (dst < n) @memset(out[dst..n], 0);
    }

    fn readFixedFromCurrentPart(self: *ScanState, op: *part_mod.OpenedPartAny, use_range_reader: bool, ci: usize, n: u64, out: []i64) !usize {
        if (use_range_reader) {
            var cr = try op.columnReaderRange(ci, self.rows_read, @intCast(n));
            defer cr.deinit();
            return cr.readFixed(out);
        }
        return self.col_readers[ci].?.readFixed(out);
    }

    fn readStringRange(self: *ScanState, ci: usize, start: u64, n: usize, out: [][]const u8, alloc: std.mem.Allocator) !void {
        const Ctx = struct {
            out: [][]const u8,
            idx: usize,
            alloc: std.mem.Allocator,
        };
        var global_base: u64 = 0;
        var dst: usize = 0;
        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (start >= global_base + part_rows) continue;
            if (start + n <= global_base) break;

            const local_start: u64 = if (start > global_base) start - global_base else 0;
            const available: usize = @intCast(part_rows - local_start);
            const want = @min(n - dst, available);

            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReaderRange(ci, local_start, want);
            defer cr.deinit();

            var ctx = Ctx{ .out = out, .idx = dst, .alloc = alloc };
            _ = try cr.readStrings(want, &ctx, struct {
                fn cb(c: *Ctx, str: []const u8) !void {
                    c.out[c.idx] = try c.alloc.dupe(u8, str);
                    c.idx += 1;
                }
            }.cb);
            dst = ctx.idx;
            if (dst >= n) break;
        }
        while (dst < n) : (dst += 1) out[dst] = "";
    }

    fn readStringNonEmptyRange(self: *ScanState, ci: usize, start: u64, n: usize, out: []u8) !void {
        const Ctx = struct {
            out: []u8,
            idx: usize,
        };
        var global_base: u64 = 0;
        var dst: usize = 0;
        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (start >= global_base + part_rows) continue;
            if (start + n <= global_base) break;

            const local_start: u64 = if (start > global_base) start - global_base else 0;
            const available: usize = @intCast(part_rows - local_start);
            const want = @min(n - dst, available);

            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReaderRange(ci, local_start, want);
            defer cr.deinit();

            var ctx = Ctx{ .out = out, .idx = dst };
            _ = try cr.readStrings(want, &ctx, struct {
                fn cb(c: *Ctx, str: []const u8) !void {
                    c.out[c.idx] = if (str.len != 0) 1 else 0;
                    c.idx += 1;
                }
            }.cb);
            dst = ctx.idx;
            if (dst >= n) break;
        }
        if (dst < n) @memset(out[dst..n], 0);
    }

    fn readArrayRange(self: *ScanState, ci: usize, start: u64, n: usize, out: [][][]const u8, alloc: std.mem.Allocator) !void {
        var global_base: u64 = 0;
        var dst: usize = 0;
        for (self.part_dirs) |dir| {
            const part_rows = try readPartCount(self.alloc, self.io, dir);
            defer global_base += part_rows;
            if (start >= global_base + part_rows) continue;
            if (start + n <= global_base) break;

            const local_start: u64 = if (start > global_base) start - global_base else 0;
            const available: usize = @intCast(part_rows - local_start);
            const want = @min(n - dst, available);

            var op = try part_mod.OpenedPartAny.open(self.io, self.alloc, dir, self.table);
            defer op.deinit();
            var cr = try op.columnReaderRange(ci, local_start, want);
            defer cr.deinit();

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
                        try self.readFixedRange(ci, start, n, buf);
                    }
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    if (is_pruned) {
                        @memset(fbuf, 0.0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        try self.readFixedRange(ci, start, n, ibuf);
                        if (col.ty == .float32) {
                            for (ibuf, 0..) |iv, i| {
                                fbuf[i] = @floatCast(@as(f32, @bitCast(@as(u32, @truncate(@as(u64, @bitCast(iv)))))));
                            }
                        } else {
                            for (ibuf, 0..) |iv, i| fbuf[i] = @bitCast(iv);
                        }
                    }
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        try self.readFixedRange(ci, start, n, ibuf);
                        for (ibuf, 0..) |v, i| ubuf[i] = @intCast(v);
                    }
                    break :blk .{ .date_u16 = ubuf };
                },
                .bool_u8 => blk: {
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    if (is_pruned) {
                        @memset(bbuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        try self.readFixedRange(ci, start, n, ibuf);
                        for (ibuf, 0..) |v, i| bbuf[i] = @intCast(@as(u8, @truncate(@as(u64, @bitCast(v)))));
                    }
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    const ubuf = try chunk_alloc.alloc(u64, n);
                    if (is_pruned) {
                        @memset(ubuf, 0);
                    } else {
                        const ibuf = try chunk_alloc.alloc(i64, n);
                        try self.readFixedRange(ci, start, n, ibuf);
                        for (ibuf, 0..) |v, i| ubuf[i] = @bitCast(v);
                    }
                    break :blk .{ .uint64 = ubuf };
                },
                .string => blk: {
                    if (ci < 256 and self.nonempty_bool[ci]) {
                        const bbuf = try chunk_alloc.alloc(u8, n);
                        if (is_pruned) {
                            @memset(bbuf, 0);
                        } else {
                            try self.readStringNonEmptyRange(ci, start, n, bbuf);
                        }
                        break :blk .{ .bool_u8 = bbuf };
                    }
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    if (is_pruned) {
                        @memset(sbuf, "");
                    } else {
                        try self.readStringRange(ci, start, n, sbuf, chunk_alloc);
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
        const is_compact = switch (self.opened.?) {
            .compact => true,
            .wide => true,
        };
        if (is_compact) return true;
        for (self.table.columns, 0..) |_, i| {
            // Skip columns not in the needed set (column pruning).
            if (!self.isNeeded(i)) continue;
            self.col_readers[i] = try self.opened.?.columnReader(i);
        }
        return true;
    }

    /// Read up to CHUNK_SIZE rows into a DataChunk. Returns false when all parts exhausted.
    fn nextChunk(self: *ScanState, out: *DataChunk, ctx: *QueryContext) !bool {
        if (self.rangeActive()) {
            const remaining = self.effectiveCount() -| self.rows_read;
            if (remaining == 0) return false;
            const n: usize = @intCast(@min(remaining, chunk.CHUNK_SIZE));
            try self.fillRange(self.effectiveLo() + self.rows_read, n, out, ctx.allocator());
            self.rows_read += n;
            return true;
        }

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
        const use_range_readers = switch (op.*) {
            .compact => true,
            .wide => true,
        };

        for (self.table.columns, 0..) |col, ci| {
            const null_mask = try chunk_alloc.alloc(u64, null_words);
            @memset(null_mask, 0);
            const core_ty = toCoreColTypeFull(col);
            // Pruned column: produce zero/empty data without reading from disk.
            const is_pruned = !self.isNeeded(ci);
            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    const buf = try chunk_alloc.alloc(i64, n);
                    if (is_pruned) {
                        @memset(buf, 0);
                    } else {
                        const actual = try self.readFixedFromCurrentPart(op, use_range_readers, ci, n, buf);
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
                        _ = try self.readFixedFromCurrentPart(op, use_range_readers, ci, n, ibuf);
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
                        _ = try self.readFixedFromCurrentPart(op, use_range_readers, ci, n, ibuf);
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
                        _ = try self.readFixedFromCurrentPart(op, use_range_readers, ci, n, ibuf);
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
                        _ = try self.readFixedFromCurrentPart(op, use_range_readers, ci, n, ibuf);
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
                            var tmp_cr: ?part_mod.ColumnReader = null;
                            defer if (tmp_cr) |*cr| cr.deinit();
                            const cr = if (use_range_readers) cr_blk: {
                                tmp_cr = try op.columnReaderRange(ci, self.rows_read, n);
                                break :cr_blk &tmp_cr.?;
                            } else &self.col_readers[ci].?;
                            const actual = try cr.readStrings(n, &sctx,
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
                        var tmp_cr: ?part_mod.ColumnReader = null;
                        defer if (tmp_cr) |*cr| cr.deinit();
                        const cr = if (use_range_readers) cr_blk: {
                            tmp_cr = try op.columnReaderRange(ci, self.rows_read, n);
                            break :cr_blk &tmp_cr.?;
                        } else &self.col_readers[ci].?;
                        _ = try cr.readStrings(n, &sctx,
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
                        var tmp_cr: ?part_mod.ColumnReader = null;
                        defer if (tmp_cr) |*cr| cr.deinit();
                        const cr = if (use_range_readers) cr_blk: {
                            tmp_cr = try op.columnReaderRange(ci, self.rows_read, n);
                            break :cr_blk &tmp_cr.?;
                        } else &self.col_readers[ci].?;
                    {
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
        self.rows_read = 0;
    }
};

fn readPartCount(alloc: std.mem.Allocator, io: std.Io, part_dir: []const u8) !u64 {
    const path = try std.fmt.allocPrint(alloc, "{s}/count.txt", .{part_dir});
    defer alloc.free(path);
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(128));
    defer alloc.free(raw);
    return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

fn isLargeCompactPart(alloc: std.mem.Allocator, io: std.Io, part_dir: []const u8, row_count: u64) !bool {
    const cmrk_path = try std.fmt.allocPrint(alloc, "{s}/data.cmrk4", .{part_dir});
    defer alloc.free(cmrk_path);
    var cmrk = std.Io.Dir.cwd().openFile(io, cmrk_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    cmrk.close(io);

    if (row_count > RAW_MATERIALIZE_MAX_COMPACT_ROWS) return true;

    const data_path = try std.fmt.allocPrint(alloc, "{s}/data.bin", .{part_dir});
    defer alloc.free(data_path);
    var data_file = try std.Io.Dir.cwd().openFile(io, data_path, .{});
    defer data_file.close(io);
    const stat = try data_file.stat(io);
    return stat.size > RAW_MATERIALIZE_MAX_COMPACT_BYTES;
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
        try self.state.fillRange(self.state.effectiveLo() + start, n, out, alloc);
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
        return self.state.effectiveCount();
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
                    s.nonempty_bool[ci] = false;
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

    fn setRowRangeFn(ptr: *anyopaque, lo: u64, hi: u64) void {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        self.state.scan_lo = lo;
        self.state.scan_hi = hi;
        self.state.reset();
    }

    fn findIntRangeFn(ptr: *anyopaque, col_name: []const u8, value: i64) ?SourceIface.RowRange {
        const self: *PartScanBridge = @ptrCast(@alignCast(ptr));
        return self.state.findIntRange(col_name, value) catch null;
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
        .setRowRange           = setRowRangeFn,
        .getSortKeys           = getSortKeysFn,
        .findIntRange          = findIntRangeFn,
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

    source.setNeededCols(null);
    source.setRowRange(1, 3);
    defer source.setRowRange(0, 3);
    try std.testing.expectEqual(@as(u64, 2), source.rowCount());

    var ranged_fetch: DataChunk = undefined;
    try source.fetchRange(0, 2, &ranged_fetch, allocator);
    defer ranged_fetch.deinit();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ranged_fetch.columns[0].data.int64);
    try std.testing.expectEqualStrings("", ranged_fetch.columns[2].data.string[0]);
    try std.testing.expectEqualStrings("bbb", ranged_fetch.columns[2].data.string[1]);

    source.reset();
    var ranged_ctx = QueryContext.init(allocator, source);
    defer ranged_ctx.deinit();
    var ranged_next: DataChunk = undefined;
    try std.testing.expect(try source.nextChunk(&ranged_next, &ranged_ctx));
    defer ranged_next.deinit();
    try std.testing.expectEqual(@as(usize, 2), ranged_next.num_rows);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3 }, ranged_next.columns[0].data.int64);
}

test "PartScanBridge disables raw materialization for large compact parts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_part_scan_bridge_large_raw_guard";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
    }

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "name", .ty = .text },
    };
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "large_raw_guard", .columns = &cols, .sort_keys = &sort_keys };

    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 1, 2, 3 };
        try cp.appendFixedBatch(0, &ids);
        try cp.appendString(1, "a");
        try cp.appendString(1, "bb");
        try cp.appendString(1, "ccc");
        try cp.finish();
    }

    const data_path = try std.fmt.allocPrint(allocator, "{s}/data.bin", .{part_dir});
    defer allocator.free(data_path);
    var data_file = try std.Io.Dir.cwd().openFile(io, data_path, .{ .mode = .read_write });
    defer data_file.close(io);
    try data_file.writePositionalAll(io, &[_]u8{0}, RAW_MATERIALIZE_MAX_COMPACT_BYTES + 1);

    var bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{part_dir}, &.{});
    defer bridge.deinit();
    const source = bridge.source();

    try std.testing.expectEqual(@as(u64, 3), source.rowCount());
    try std.testing.expect(source.getRawInt32Col("id") == null);
    try std.testing.expect(source.getRawStrOffsets("name") == null);
    try std.testing.expect(source.getRawStrBytes("name") == null);
}

test "PartScanBridge finds compact int sort-key range via primary index" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir = "/tmp/zig_test_part_scan_bridge_primary_range";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir) catch {};
    }

    const cols = [_]schema.Column{.{ .name = "id", .ty = .int32 }};
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "primary_range", .columns = &cols, .sort_keys = &sort_keys };

    const n_rows = part_mod.GRANULE_SIZE * 2 + 2048;
    const ids = try allocator.alloc(i64, n_rows);
    defer allocator.free(ids);
    for (ids, 0..) |*id, i| {
        id.* = if (i < 5000) 1 else if (i < 12000) 42 else 99;
    }

    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir, table, 0x82);
        defer cp.deinit();
        try cp.appendFixedBatch(0, ids);
        try cp.finish();
    }

    var bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{part_dir}, &.{});
    defer bridge.deinit();
    const source = bridge.source();

    const hit = source.findIntRange("id", 42) orelse return error.TestExpectedRange;
    try std.testing.expectEqual(@as(u64, 5000), hit.lo);
    try std.testing.expectEqual(@as(u64, 12000), hit.hi);

    const miss = source.findIntRange("id", 7) orelse return error.TestExpectedEmptyRange;
    try std.testing.expectEqual(miss.lo, miss.hi);
}

test "PartScanBridge finds compact int sort-key range across parts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir_1 = "/tmp/zig_test_part_scan_bridge_primary_range_part_1";
    const part_dir_2 = "/tmp/zig_test_part_scan_bridge_primary_range_part_2";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir_1) catch {};
        cwd.deleteTree(io, part_dir_2) catch {};
    }

    const cols = [_]schema.Column{.{ .name = "id", .ty = .int32 }};
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "primary_range_multi", .columns = &cols, .sort_keys = &sort_keys };

    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir_1, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 1, 42, 42 };
        try cp.appendFixedBatch(0, &ids);
        try cp.finish();
    }
    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir_2, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 42, 99 };
        try cp.appendFixedBatch(0, &ids);
        try cp.finish();
    }

    var bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{ part_dir_1, part_dir_2 }, &.{});
    defer bridge.deinit();
    const source = bridge.source();

    const hit = source.findIntRange("id", 42) orelse return error.TestExpectedRange;
    try std.testing.expectEqual(@as(u64, 1), hit.lo);
    try std.testing.expectEqual(@as(u64, 4), hit.hi);

    const miss = source.findIntRange("id", 7) orelse return error.TestExpectedEmptyRange;
    try std.testing.expectEqual(miss.lo, miss.hi);
}

test "PartScanBridge combines raw views across compact parts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const part_dir_1 = "/tmp/zig_test_part_scan_bridge_multi_1";
    const part_dir_2 = "/tmp/zig_test_part_scan_bridge_multi_2";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, part_dir_1) catch {};
        cwd.deleteTree(io, part_dir_2) catch {};
    }

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "name", .ty = .text },
    };
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "multi_bridge", .columns = &cols, .sort_keys = &sort_keys };

    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir_1, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 1, 2 };
        try cp.appendFixedBatch(0, &ids);
        try cp.appendString(1, "a");
        try cp.appendString(1, "bb");
        try cp.finish();
    }
    {
        var cp = try part_mod.CompactPart.open(io, allocator, part_dir_2, table, 0x82);
        defer cp.deinit();
        const ids = [_]i64{ 3, 4 };
        try cp.appendFixedBatch(0, &ids);
        try cp.appendString(1, "ccc");
        try cp.appendString(1, "");
        try cp.finish();
    }

    var bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{ part_dir_1, part_dir_2 }, &.{});
    defer bridge.deinit();
    const source = bridge.source();

    try std.testing.expectEqual(@as(u64, 4), source.rowCount());
    const ids_raw = source.getRawInt32Col("id") orelse return error.TestExpectedRawInt32;
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4 }, ids_raw);

    const offsets = source.getRawStrOffsets("name") orelse return error.TestExpectedRawStrOffsets;
    const bytes = source.getRawStrBytes("name") orelse return error.TestExpectedRawStrBytes;
    try std.testing.expectEqualSlices(u64, &[_]u64{ 0, 1, 3, 6, 6 }, offsets);
    try std.testing.expectEqualStrings("abbccc", bytes);

    var range_out: DataChunk = undefined;
    try source.fetchRange(1, 3, &range_out, allocator);
    defer range_out.deinit();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 2, 3, 4 }, range_out.columns[0].data.int64);
    try std.testing.expectEqualStrings("bb", range_out.columns[1].data.string[0]);
    try std.testing.expectEqualStrings("ccc", range_out.columns[1].data.string[1]);
    try std.testing.expectEqualStrings("", range_out.columns[1].data.string[2]);

    source.setRowRange(2, 4);
    defer source.setRowRange(0, 4);
    try std.testing.expectEqual(@as(u64, 2), source.rowCount());

    var ranged_fetch: DataChunk = undefined;
    try source.fetchRange(0, 2, &ranged_fetch, allocator);
    defer ranged_fetch.deinit();
    try std.testing.expectEqualSlices(i64, &[_]i64{ 3, 4 }, ranged_fetch.columns[0].data.int64);
    try std.testing.expectEqualStrings("ccc", ranged_fetch.columns[1].data.string[0]);
    try std.testing.expectEqualStrings("", ranged_fetch.columns[1].data.string[1]);
}

const ConsistencyFixture = struct {
    ids: []const i32,
    user_ids: []const i64,
    categories: []const []const u8,
    scores: []const i32,
};

fn writeBytes(io: std.Io, alloc: std.mem.Allocator, path: []const u8, bytes: []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const dir = std.fs.path.dirname(path) orelse ".";
    try cwd.createDirPath(io, dir);
    try cwd.writeFile(io, .{ .sub_path = path, .data = bytes });
    _ = alloc;
}

fn writeGenericInt32(io: std.Io, alloc: std.mem.Allocator, part_dir: []const u8, name: []const u8, values: []const i32) !void {
    var bytes = try alloc.alloc(u8, values.len * 4);
    defer alloc.free(bytes);
    for (values, 0..) |v, i| std.mem.writeInt(i32, bytes[i * 4 ..][0..4], v, .little);
    const path = try generic_store.columnBinPath(alloc, part_dir, name);
    defer alloc.free(path);
    try writeBytes(io, alloc, path, bytes);
}

fn writeGenericInt64(io: std.Io, alloc: std.mem.Allocator, part_dir: []const u8, name: []const u8, values: []const i64) !void {
    var bytes = try alloc.alloc(u8, values.len * 8);
    defer alloc.free(bytes);
    for (values, 0..) |v, i| std.mem.writeInt(i64, bytes[i * 8 ..][0..8], v, .little);
    const path = try generic_store.columnBinPath(alloc, part_dir, name);
    defer alloc.free(path);
    try writeBytes(io, alloc, path, bytes);
}

fn writeGenericString(io: std.Io, alloc: std.mem.Allocator, part_dir: []const u8, name: []const u8, values: []const []const u8) !void {
    var total_bytes: usize = 0;
    for (values) |v| total_bytes += v.len;
    const header_len = 8 + (values.len + 1) * 8;
    var bytes = try alloc.alloc(u8, header_len + total_bytes);
    defer alloc.free(bytes);
    std.mem.writeInt(u64, bytes[0..8], values.len, .little);
    var offset: u64 = 0;
    std.mem.writeInt(u64, bytes[8..16], offset, .little);
    var pos = header_len;
    for (values, 0..) |v, i| {
        @memcpy(bytes[pos..][0..v.len], v);
        pos += v.len;
        offset += v.len;
        const off_pos = 8 + (i + 1) * 8;
        std.mem.writeInt(u64, bytes[off_pos..][0..8], offset, .little);
    }
    const path = try generic_store.columnStrBinPath(alloc, part_dir, name);
    defer alloc.free(path);
    try writeBytes(io, alloc, path, bytes);
}

fn writeGenericFixture(io: std.Io, alloc: std.mem.Allocator, store_dir: []const u8, table: schema.Table, fixture: ConsistencyFixture) ![]u8 {
    const part_dir = try generic_store.initPart(io, store_dir, table.name, alloc);
    try generic_store.writeColumnsTxt(io, alloc, part_dir, table);
    try generic_store.writeCountTxt(io, alloc, part_dir, fixture.ids.len);
    try writeGenericInt32(io, alloc, part_dir, "id", fixture.ids);
    try writeGenericInt64(io, alloc, part_dir, "user_id", fixture.user_ids);
    try writeGenericString(io, alloc, part_dir, "category", fixture.categories);
    try writeGenericInt32(io, alloc, part_dir, "score", fixture.scores);
    return part_dir;
}

fn writeCompactPart(
    io: std.Io,
    alloc: std.mem.Allocator,
    part_dir: []const u8,
    table: schema.Table,
    fixture: ConsistencyFixture,
    start: usize,
    end: usize,
) !void {
    var cp = try part_mod.CompactPart.open(io, alloc, part_dir, table, 0x82);
    defer cp.deinit();
    var tmp_i64 = std.ArrayListUnmanaged(i64).empty;
    defer tmp_i64.deinit(alloc);

    tmp_i64.clearRetainingCapacity();
    for (fixture.ids[start..end]) |v| try tmp_i64.append(alloc, v);
    try cp.appendFixedBatch(0, tmp_i64.items);

    tmp_i64.clearRetainingCapacity();
    for (fixture.user_ids[start..end]) |v| try tmp_i64.append(alloc, v);
    try cp.appendFixedBatch(1, tmp_i64.items);

    for (fixture.categories[start..end]) |v| try cp.appendString(2, v);

    tmp_i64.clearRetainingCapacity();
    for (fixture.scores[start..end]) |v| try tmp_i64.append(alloc, v);
    try cp.appendFixedBatch(3, tmp_i64.items);

    try cp.finish();
}

fn valueEqual(a: core.Value, b: core.Value) bool {
    if (@as(ColumnType, a) != @as(ColumnType, b)) return false;
    return switch (a) {
        .bool_u8 => |v| v == b.bool_u8,
        .int64 => |v| v == b.int64,
        .uint64 => |v| v == b.uint64,
        .float64 => |v| v == b.float64,
        .date_u16 => |v| v == b.date_u16,
        .datetime64_ms => |v| v == b.datetime64_ms,
        .string => |v| std.mem.eql(u8, v, b.string),
        .array_string => |v| blk: {
            if (v.len != b.array_string.len) break :blk false;
            for (v, b.array_string) |lhs, rhs| {
                if (!std.mem.eql(u8, lhs, rhs)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn expectSameResult(generic: result.ResultSet, compact: result.ResultSet) !void {
    try std.testing.expectEqual(generic.metas.len, compact.metas.len);
    try std.testing.expectEqual(generic.num_rows, compact.num_rows);
    for (generic.metas, compact.metas) |gm, cm| {
        try std.testing.expectEqualStrings(gm.name, cm.name);
        try std.testing.expectEqual(gm.col_type, cm.col_type);
    }
    for (0..generic.num_rows) |r| {
        for (0..generic.metas.len) |ci| {
            const gv = generic.get(ci, r);
            const cv = compact.get(ci, r);
            try std.testing.expectEqual(gv != null, cv != null);
            if (gv) |v| try std.testing.expect(valueEqual(v, cv.?));
        }
    }
}

fn runOnSource(alloc: std.mem.Allocator, source: SourceIface, node: *const plan.PhysicalNode) !result.ResultSet {
    var qctx = QueryContext.init(alloc, source);
    defer qctx.deinit();
    return pipeline.executePlan(node, &qctx);
}

fn compareGenericAndCompact(
    alloc: std.mem.Allocator,
    generic_source: SourceIface,
    compact_source: SourceIface,
    node: *const plan.PhysicalNode,
) !void {
    var grs = try runOnSource(alloc, generic_source, node);
    defer grs.deinit();
    var crs = try runOnSource(alloc, compact_source, node);
    defer crs.deinit();
    try expectSameResult(grs, crs);
}

fn colRef(index: usize, name: []const u8) plan.Expr {
    return .{ .col_ref = .{ .index = index, .name = name } };
}

fn countStarExpr(agg: *plan.AggCall) plan.Expr {
    agg.* = .{ .kind = .count_star, .arg = null, .distinct = false };
    return .{ .agg_call = agg };
}

fn countDistinctExpr(agg: *plan.AggCall, arg: *plan.Expr) plan.Expr {
    agg.* = .{ .kind = .count, .arg = arg.*, .distinct = true };
    return .{ .agg_call = agg };
}

test "generic and compact sources produce consistent pipeline results" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const generic_store_dir = "/tmp/zig_test_generic_compact_consistency_generic";
    const compact_part_dir_1 = "/tmp/zig_test_generic_compact_consistency_part_1";
    const compact_part_dir_2 = "/tmp/zig_test_generic_compact_consistency_part_2";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, generic_store_dir) catch {};
        cwd.deleteTree(io, compact_part_dir_1) catch {};
        cwd.deleteTree(io, compact_part_dir_2) catch {};
    }

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "user_id", .ty = .int64 },
        .{ .name = "category", .ty = .text },
        .{ .name = "score", .ty = .int32 },
    };
    const sort_keys = [_][]const u8{"id"};
    const table = schema.Table{ .name = "events_consistency", .columns = &cols, .sort_keys = &sort_keys };

    const ids = [_]i32{ 1, 2, 3, 4, 5, 6 };
    const user_ids = [_]i64{ 10, 20, 10, 30, 20, 20 };
    const categories = [_][]const u8{ "alpha", "", "beta", "alpha", "gamma", "beta" };
    const scores = [_]i32{ 5, 7, 4, 9, 3, 8 };
    const fixture = ConsistencyFixture{
        .ids = &ids,
        .user_ids = &user_ids,
        .categories = &categories,
        .scores = &scores,
    };

    const generic_part_dir = try writeGenericFixture(io, allocator, generic_store_dir, table, fixture);
    defer allocator.free(generic_part_dir);
    try writeCompactPart(io, allocator, compact_part_dir_1, table, fixture, 0, 3);
    try writeCompactPart(io, allocator, compact_part_dir_2, table, fixture, 3, 6);

    var generic_bridge = try generic_store_bridge.GenericStoreBridge.init(allocator, io, generic_part_dir, table, &.{});
    defer generic_bridge.deinit();
    try generic_bridge.preload();
    var compact_bridge = try PartScanBridge.init(allocator, io, table, &[_][]const u8{ compact_part_dir_1, compact_part_dir_2 }, &.{});
    defer compact_bridge.deinit();
    const generic_source = generic_bridge.source();
    const compact_source = compact_bridge.source();

    var scan_cols = [_][]const u8{};
    var scan = plan.PhysicalNode{ .part_scan = .{
        .db = "default",
        .table = table.name,
        .columns = scan_cols[0..],
        .filter = null,
    }};

    var count_call: plan.AggCall = undefined;
    var count_aggs = [_]plan.ProjectItem{.{
        .expr = countStarExpr(&count_call),
        .alias = "count()",
        .out_type = .uint64,
    }};
    var count_node = plan.PhysicalNode{ .scalar_agg = .{ .input = &scan, .aggs = count_aggs[0..] } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &count_node);

    var gt_op = plan.BinOp{ .left = colRef(3, "score"), .right = .{ .lit_i64 = 6 } };
    var filter = plan.PhysicalNode{ .filter = .{ .input = &scan, .predicate = .{ .gt = &gt_op } } };
    var id_project_items = [_]plan.ProjectItem{.{
        .expr = colRef(0, "id"),
        .alias = "id",
        .out_type = .int64,
    }};
    var project_id = plan.PhysicalNode{ .project = .{ .input = &filter, .items = id_project_items[0..] } };
    var id_sort_keys = [_]plan.SortKey{.{ .col_idx = 0, .desc = false, .nulls_first = false }};
    var order_ids = plan.PhysicalNode{ .order_by = .{ .input = &project_id, .keys = id_sort_keys[0..] } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &order_ids);

    var neq_empty_op = plan.BinOp{ .left = colRef(2, "category"), .right = .{ .lit_str = "" } };
    var nonempty_filter = plan.PhysicalNode{ .filter = .{ .input = &scan, .predicate = .{ .neq = &neq_empty_op } } };
    var nonempty_count = plan.PhysicalNode{ .scalar_agg = .{ .input = &nonempty_filter, .aggs = count_aggs[0..] } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &nonempty_count);

    var group_keys = [_]plan.ProjectItem{.{
        .expr = colRef(1, "user_id"),
        .alias = "user_id",
        .out_type = .int64,
    }};
    var grouped_count = plan.PhysicalNode{ .hash_agg = .{
        .input = &scan,
        .keys = group_keys[0..],
        .aggs = count_aggs[0..],
        .strategy = .single_int_count_topk,
    }};
    var count_sort_keys = [_]plan.SortKey{.{ .col_idx = 1, .desc = true, .nulls_first = false }};
    var grouped_topk = plan.PhysicalNode{ .top_k = .{ .input = &grouped_count, .keys = count_sort_keys[0..], .k = 2 } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &grouped_topk);

    var distinct_arg = colRef(1, "user_id");
    var distinct_call: plan.AggCall = undefined;
    var distinct_aggs = [_]plan.ProjectItem{.{
        .expr = countDistinctExpr(&distinct_call, &distinct_arg),
        .alias = "uniq",
        .out_type = .uint64,
    }};
    var distinct_count = plan.PhysicalNode{ .scalar_agg = .{ .input = &scan, .aggs = distinct_aggs[0..] } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &distinct_count);

    var ordered_ids_limit = plan.PhysicalNode{ .limit = .{ .input = &order_ids, .limit = 2, .offset = 2 } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &ordered_ids_limit);

    var eq_id_op = plan.BinOp{ .left = colRef(0, "id"), .right = .{ .lit_i64 = 4 } };
    var id_eq_filter = plan.PhysicalNode{ .filter = .{ .input = &scan, .predicate = .{ .eq = &eq_id_op } } };
    var id_eq_project = plan.PhysicalNode{ .project = .{ .input = &id_eq_filter, .items = id_project_items[0..] } };
    try compareGenericAndCompact(allocator, generic_source, compact_source, &id_eq_project);
}
