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
extern fn LZ4_decompress_safe(source: [*]const u8, dest: [*]u8, compressed_size: c_int, max_decompressed_size: c_int) c_int;

const std = @import("std");
const schema = @import("schema");
const core = @import("core");
const chunk = core.chunk;
const result = core.result;
const pipeline = core.exec.pipeline;
const generic_store = @import("generic_store");
const scan_common = @import("scan_common");

pub const DataChunk = chunk.DataChunk;
pub const ColumnType = chunk.ColumnType;
pub const SourceIface = pipeline.SourceIface;
pub const QueryContext = pipeline.QueryContext;
pub const ColMeta = result.ColMeta;

fn elapsedNs(started_ns: i128) u64 {
    if (started_ns == 0) return 0;
    const elapsed = pipeline.profileNowNs() - started_ns;
    return if (elapsed > 0) @intCast(elapsed) else 0;
}

// ── Mapped column data ────────────────────────────────────────────────────────

const MappedFixedCol = struct {
    mapped: generic_store.v2.MappedColumn,
    full_bytes: ?[]const u8 = null,
    full_borrowed: bool = false,

    fn deinit(self: MappedFixedCol, allocator: std.mem.Allocator) void {
        if (self.full_bytes) |bytes| {
            if (!self.full_borrowed) allocator.free(bytes);
        }
        self.mapped.deinit(allocator);
    }
};

/// Holds the mmap'd data for a string column.
const MappedStringCol = struct {
    ptr: []align(std.heap.page_size_min) u8,
    row_count: u64,
    offsets: []const u64, // row_count+1 entries
    bytes: []const u8, // byte blob

    fn deinit(self: MappedStringCol) void {
        std.posix.munmap(self.ptr);
    }

    fn str(self: MappedStringCol, row: u64) []const u8 {
        const start = self.offsets[row];
        const end = self.offsets[row + 1];
        return self.bytes[start..end];
    }
};

const STR_BLOCK_MAGIC = "ZHSTRB1\x00";

const MappedBlockStringCol = struct {
    ptr: []align(std.heap.page_size_min) u8,
    row_count: u64,
    block_rows: u32,
    block_count: u32,
    dir_offset: u64,

    const Entry = struct {
        row_start: u64,
        data_offset: u64,
        comp_size: u32,
        raw_size: u32,
        codec: u8,
    };

    fn deinit(self: MappedBlockStringCol) void {
        std.posix.munmap(self.ptr);
    }

    fn entry(self: MappedBlockStringCol, i: usize) Entry {
        const off: usize = @intCast(self.dir_offset + @as(u64, i) * 32);
        const e = self.ptr[off..][0..32];
        return .{
            .row_start = std.mem.readInt(u64, e[0..8], .little),
            .data_offset = std.mem.readInt(u64, e[8..16], .little),
            .comp_size = std.mem.readInt(u32, e[16..20], .little),
            .raw_size = std.mem.readInt(u32, e[20..24], .little),
            .codec = e[24],
        };
    }

    fn decode(self: MappedBlockStringCol, e: Entry, alloc: std.mem.Allocator) ![]const u8 {
        const src = self.ptr[@intCast(e.data_offset)..@intCast(e.data_offset + e.comp_size)];
        if (e.codec == 0) return src;
        if (e.codec != 1) return error.InvalidStringBlock;
        const out = try alloc.alloc(u8, e.raw_size);
        const n = LZ4_decompress_safe(src.ptr, out.ptr, @intCast(src.len), @intCast(out.len));
        if (n < 0 or @as(usize, @intCast(n)) != out.len) return error.CorruptStringBlock;
        return out;
    }

    fn readRange(self: MappedBlockStringCol, start: u64, count: usize, alloc: std.mem.Allocator, out: [][]const u8) !void {
        var produced: usize = 0;
        var row = start;
        while (produced < count) {
            const bi: usize = @intCast(row / self.block_rows);
            if (bi >= self.block_count) return error.InvalidStringBlock;
            const e = self.entry(bi);
            const raw = try self.decode(e, alloc);
            const rows_in: usize = @intCast(@min(@as(u64, self.block_rows), self.row_count - e.row_start));
            if (raw.len < rows_in * 4) return error.InvalidStringBlock;
            const lens = @as([*]const u32, @ptrCast(@alignCast(raw.ptr)))[0..rows_in];
            var rr: usize = @intCast(row - e.row_start);
            var byte_pos: usize = rows_in * 4;
            var j: usize = 0;
            while (j < rr) : (j += 1) byte_pos += lens[j];
            while (rr < rows_in and produced < count) : (rr += 1) {
                const len = lens[rr];
                out[produced] = raw[byte_pos .. byte_pos + len];
                byte_pos += len;
                produced += 1;
            }
            row = e.row_start + rows_in;
        }
    }
};

const ColData = union(enum) {
    fixed: MappedFixedCol,
    string: MappedStringCol,
    block_string: MappedBlockStringCol,
    none, // pruned / skipped
};

// ── ScanState ─────────────────────────────────────────────────────────────────

const ScanState = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    part_dir: []const u8, // path to the all_1_1_0/ directory (borrowed)
    table: schema.Table,
    row_count: u64,
    rows_read: u64,
    col_data: []ColData, // one per table column; loaded lazily
    metas: []ColMeta,
    needed_cols: ?[]bool, // null = read all
    /// Temporary column restriction set by setNeededCols() for late materialization.
    /// When active, overrides `needed_cols` for the duration of the scan phase.
    override_needed: [256]bool,
    override_active: bool,
    /// Pre-allocated zero buffers reused for pruned columns (avoid per-chunk alloc).
    zero_i64: []i64,
    zero_f64: []f64,
    zero_u16: []u16,
    zero_u8: []u8,
    zero_u64: []u64,
    zero_str: [][]const u8,
    zero_astr: [][][]const u8,
    zero_nmask: []u64,
    loaded: bool, // have we opened the columns yet?
    /// Spinlock for loadColumns() — used when parallel fetchRange() calls arrive
    /// before an explicit preload(). 0 = unlocked, 1 = locked.
    load_lock: std.atomic.Value(u8),
    /// Columns to decode as bool_u8 (1=non-empty, 0=empty) instead of full slices.
    nonempty_bool: [256]bool,
    /// Row-range restriction: only rows in [scan_lo, scan_hi) are visible.
    /// scan_hi is clamped to row_count at access time.
    /// Defaults to [0, maxInt(u64)] = full scan.
    scan_lo: u64,
    scan_hi: u64,
    profile_open_ns: u64,
    profile_mmap_ns: u64,
    profile_bytes_mapped: u64,
    profile_bytes_read: std.atomic.Value(u64),

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
            const hash_col_name: ?[]const u8 = switch (col.physical) {
                .lowcard_text => |lc| lc.hash_column,
                .lazy_text => |lt| lt.hash_column,
                else => null,
            };
            metas[i] = .{
                .name = col.name,
                .col_type = scan_common.toCoreColType(col.ty),
                .is_narrow_int = (col.ty == .int8 or col.ty == .int16),
                .hash_col_name = hash_col_name,
            };
        }

        const needed_cols = try scan_common.buildNeededMask(alloc, table.columns, pruned_columns);

        const col_data = try alloc.alloc(ColData, table.columns.len);
        @memset(col_data, .none);

        // Pre-allocate zero buffers large enough for morsel-sized fetchRange calls.
        // default_morsel_size = 122_880; use 131_072 (next power of 2) as the upper bound
        // so that `use_prealloc` is always true for normal morsel fetches, avoiding
        // expensive per-morsel zero-buffer allocation inside fetchRange.
        const ZERO_BUF_SIZE: usize = 131_072;
        const zero_i64 = try alloc.alloc(i64, ZERO_BUF_SIZE);
        @memset(zero_i64, 0);
        const zero_f64 = try alloc.alloc(f64, ZERO_BUF_SIZE);
        @memset(zero_f64, 0.0);
        const zero_u16 = try alloc.alloc(u16, ZERO_BUF_SIZE);
        @memset(zero_u16, 0);
        const zero_u8 = try alloc.alloc(u8, ZERO_BUF_SIZE);
        @memset(zero_u8, 0);
        const zero_u64 = try alloc.alloc(u64, ZERO_BUF_SIZE);
        @memset(zero_u64, 0);
        const zero_str = try alloc.alloc([]const u8, ZERO_BUF_SIZE);
        @memset(zero_str, "");
        const zero_astr = try alloc.alloc([][]const u8, ZERO_BUF_SIZE);
        @memset(zero_astr, &.{});
        const zero_nmask = try alloc.alloc(u64, chunk.nullMaskWords(ZERO_BUF_SIZE));
        @memset(zero_nmask, 0);

        return .{
            .alloc = alloc,
            .io = io,
            .part_dir = part_dir,
            .table = table,
            .row_count = row_count,
            .rows_read = 0,
            .col_data = col_data,
            .metas = metas,
            .needed_cols = needed_cols,
            .override_needed = @as([256]bool, @splat(false)),
            .override_active = false,
            .loaded = false,
            .load_lock = .init(0),
            .nonempty_bool = @as([256]bool, @splat(false)),
            .zero_i64 = zero_i64,
            .zero_f64 = zero_f64,
            .zero_u16 = zero_u16,
            .zero_u8 = zero_u8,
            .zero_u64 = zero_u64,
            .zero_str = zero_str,
            .zero_astr = zero_astr,
            .zero_nmask = zero_nmask,
            .scan_lo = 0,
            .scan_hi = std.math.maxInt(u64),
            .profile_open_ns = 0,
            .profile_mmap_ns = 0,
            .profile_bytes_mapped = 0,
            .profile_bytes_read = .init(0),
        };
    }

    /// Returns true if column ci should be decoded (not pruned).
    inline fn isNeeded(self: *const ScanState, ci: usize) bool {
        return scan_common.isNeeded(self.needed_cols, self.override_active, &self.override_needed, ci);
    }

    fn loadColumns(self: *ScanState) !void {
        if (@atomicLoad(bool, &self.loaded, .acquire)) return;
        // Acquire spinlock (CAS 0→1).
        while (self.load_lock.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
        defer {
            self.load_lock.store(0, .release);
        }
        // Re-check under lock in case another thread raced here.
        if (@atomicLoad(bool, &self.loaded, .acquire)) return;

        const profile_enabled = pipeline.profileEnvEnabled();
        for (self.table.columns, 0..) |col, ci| {
            // Skip columns that were already mmap'd by a previous loadColumns call.
            // This allows incremental loading: columns stay warm once loaded.
            if (self.col_data[ci] != .none) continue;

            // Skip columns not needed by either the static pruning mask OR the current
            // runtime override (e.g. URLHash sidecar requested via setNeededCols).
            // We load the UNION so that all subsequent fetchRange calls (including
            // late-materialize scans that run after the override is cleared) have
            // access to every column they need.
            const in_needed = if (self.needed_cols) |nc| (ci < nc.len and nc[ci]) else true;
            const in_override = self.override_active and ci < 256 and self.override_needed[ci];
            if (!in_needed and !in_override) continue;

            if (col.ty == .text or col.ty == .char or col.ty == .low_card) {
                // String column: mmap .str.bin
                const path = try generic_store.columnStrBinPath(self.alloc, self.part_dir, col.name);
                defer self.alloc.free(path);
                const open_started = if (profile_enabled) pipeline.profileNowNs() else 0;
                const file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
                    if (profile_enabled) self.profile_open_ns +|= elapsedNs(open_started);
                    // If file doesn't exist yet (empty store), leave as .none
                    if (err == error.FileNotFound) continue;
                    return err;
                };
                defer file.close(self.io);
                const stat = try file.stat(self.io);
                if (profile_enabled) self.profile_open_ns +|= elapsedNs(open_started);
                if (stat.size < 8) continue; // empty / partial
                const mmap_started = if (profile_enabled) pipeline.profileNowNs() else 0;
                const ptr = try std.posix.mmap(null, stat.size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, file.handle, 0);
                if (profile_enabled) {
                    self.profile_mmap_ns +|= elapsedNs(mmap_started);
                    self.profile_bytes_mapped +|= stat.size;
                }
                // Hint: we will scan sequentially; OS should use aggressive read-ahead.
                std.posix.madvise(ptr.ptr, stat.size, std.posix.MADV.SEQUENTIAL) catch {};
                if (stat.size >= 32 and std.mem.eql(u8, ptr[0..8], STR_BLOCK_MAGIC)) {
                    const row_count = std.mem.readInt(u64, ptr[8..16], .little);
                    const block_rows = std.mem.readInt(u32, ptr[16..20], .little);
                    const block_count = std.mem.readInt(u32, ptr[20..24], .little);
                    const dir_offset = std.mem.readInt(u64, ptr[24..32], .little);
                    if (block_rows == 0 or dir_offset + @as(u64, block_count) * 32 > stat.size)
                        return error.InvalidStringBlock;
                    self.col_data[ci] = .{ .block_string = .{
                        .ptr = ptr,
                        .row_count = row_count,
                        .block_rows = block_rows,
                        .block_count = block_count,
                        .dir_offset = dir_offset,
                    } };
                } else {
                    const row_count = std.mem.readInt(u64, ptr[0..8], .little);
                    const offsets_bytes = (row_count + 1) * 8;
                    if (8 + offsets_bytes > stat.size) return error.InvalidStringColumn;
                    const offsets = std.mem.bytesAsSlice(u64, ptr[8..][0..offsets_bytes]);
                    const bytes = ptr[8 + offsets_bytes .. stat.size];
                    self.col_data[ci] = .{ .string = .{
                        .ptr = ptr,
                        .row_count = row_count,
                        .offsets = offsets,
                        .bytes = bytes,
                    } };
                }
            } else {
                // Fixed-width columns are always Store V2 block files.
                const path = try generic_store.columnBinPath(self.alloc, self.part_dir, col.name);
                defer self.alloc.free(path);
                const open_started = if (profile_enabled) pipeline.profileNowNs() else 0;
                const file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
                    if (profile_enabled) self.profile_open_ns +|= elapsedNs(open_started);
                    if (err == error.FileNotFound) continue;
                    return err;
                };
                defer file.close(self.io);
                const stat = try file.stat(self.io);
                if (profile_enabled) self.profile_open_ns +|= elapsedNs(open_started);
                if (stat.size == 0) continue;
                const mmap_started = if (profile_enabled) pipeline.profileNowNs() else 0;
                const mapped = try generic_store.v2.MappedColumn.open(
                    self.alloc,
                    self.io,
                    path,
                    col.ty,
                    self.row_count,
                );
                std.posix.madvise(mapped.ptr.ptr, mapped.ptr.len, std.posix.MADV.SEQUENTIAL) catch {};
                if (profile_enabled) {
                    self.profile_mmap_ns +|= elapsedNs(mmap_started);
                    self.profile_bytes_mapped +|= stat.size;
                }
                self.col_data[ci] = .{ .fixed = .{ .mapped = mapped } };
            }
        }
        @atomicStore(bool, &self.loaded, true, .release);
    }

    fn deinit(self: *ScanState) void {
        for (self.col_data) |cd| {
            switch (cd) {
                .fixed => |m| m.deinit(self.alloc),
                .string => |m| m.deinit(),
                .block_string => |m| m.deinit(),
                .none => {},
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
        // Note: scan_lo/scan_hi are intentionally NOT reset here.
        // The caller is responsible for restoring them via setRowRange(0, maxInt(u64)).
    }

    inline fn effectiveLo(self: *const ScanState) u64 {
        return scan_common.effectiveLo(self.scan_lo, self.row_count);
    }
    inline fn effectiveHi(self: *const ScanState) u64 {
        return scan_common.effectiveHi(self.scan_hi, self.row_count);
    }
    inline fn effectiveCount(self: *const ScanState) u64 {
        return scan_common.effectiveCount(self.scan_lo, self.scan_hi, self.row_count);
    }

    const FixedRange = struct {
        bytes: []const u8,
        row_base: u64,
    };

    fn fixedRange(
        self: *ScanState,
        ci: usize,
        start: u64,
        n: usize,
        allocator: std.mem.Allocator,
    ) !FixedRange {
        return switch (self.col_data[ci]) {
            .fixed => |m| blk: {
                _ = self.profile_bytes_read.fetchAdd(m.mapped.compressedBytesForRange(start, n), .monotonic);
                const view = try m.mapped.decodeRangeView(allocator, start, n);
                break :blk .{
                    .bytes = view.bytes,
                    .row_base = 0,
                };
            },
            else => error.InvalidColumnData,
        };
    }

    fn rawFixedBytes(self: *ScanState, ci: usize) ?[]const u8 {
        if (self.col_data[ci] != .fixed) return null;
        const m = &self.col_data[ci].fixed;
        if (m.full_bytes == null) {
            const view = m.mapped.decodeRangeView(self.alloc, 0, @intCast(self.row_count)) catch return null;
            m.full_bytes = view.bytes;
            m.full_borrowed = !view.owned;
            _ = self.profile_bytes_read.fetchAdd(
                m.mapped.compressedBytesForRange(0, @intCast(self.row_count)),
                .monotonic,
            );
        }
        return m.full_bytes.?;
    }

    /// Thread-safe: reads rows [start, start+n) for the currently-loaded columns.
    /// `start` is relative to scan_lo (0 = first visible row).
    /// Caller must have called loadColumns() already.
    /// Uses caller-supplied `alloc` so multiple threads can allocate independently.
    fn fetchRange(self: *ScanState, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        // Translate relative start to absolute row index.
        const abs_start = self.effectiveLo() + start;
        const null_words = chunk.nullMaskWords(n);
        out.* = .{
            .columns = undefined,
            .num_rows = n,
            .arena = std.heap.ArenaAllocator.init(alloc),
        };
        errdefer out.arena.deinit();
        const chunk_alloc = out.arena.allocator();
        // For pruned columns in fetchRange, we may need larger zero buffers than
        // the pre-allocated ones; now sized to 131_072 to cover morsel-sized fetches.
        // Use the pre-alloc'd ones when n fits, else allocate from chunk_alloc.
        const use_prealloc = n <= self.zero_i64.len;
        const zero_nm = if (use_prealloc) self.zero_nmask[0..null_words] else blk: {
            const z = try chunk_alloc.alloc(u64, null_words);
            @memset(z, 0);
            break :blk z;
        };

        out.columns = try chunk_alloc.alloc(chunk.Column, self.table.columns.len);

        // Helper: get or allocate a zero buffer of the given type for pruned cols.
        // When n > 131_072 (very rare) the pre-alloc'd buffers are too small; allocate fresh.
        const ZeroBufs = struct {
            i64_buf: ?[]i64 = null,
            f64_buf: ?[]f64 = null,
            u16_buf: ?[]u16 = null,
            u8_buf: ?[]u8 = null,
            u64_buf: ?[]u64 = null,
            str_buf: ?[][]const u8 = null,
            astr_buf: ?[][][]const u8 = null,
        };
        var zb = ZeroBufs{};

        for (self.table.columns, 0..) |col, ci| {
            const core_ty = scan_common.toCoreColType(col.ty);
            const is_pruned = self.col_data[ci] == .none or !self.isNeeded(ci);

            // Lazily allocate zero buffers from chunk_alloc when n > CHUNK_SIZE.
            const zero_i64_slice: []i64 = if (use_prealloc) self.zero_i64[0..n] else blk: {
                if (zb.i64_buf == null) {
                    const z = try chunk_alloc.alloc(i64, n);
                    @memset(z, 0);
                    zb.i64_buf = z;
                }
                break :blk zb.i64_buf.?;
            };
            const zero_f64_slice: []f64 = if (use_prealloc) self.zero_f64[0..n] else blk: {
                if (zb.f64_buf == null) {
                    const z = try chunk_alloc.alloc(f64, n);
                    @memset(z, 0.0);
                    zb.f64_buf = z;
                }
                break :blk zb.f64_buf.?;
            };
            const zero_u16_slice: []u16 = if (use_prealloc) self.zero_u16[0..n] else blk: {
                if (zb.u16_buf == null) {
                    const z = try chunk_alloc.alloc(u16, n);
                    @memset(z, 0);
                    zb.u16_buf = z;
                }
                break :blk zb.u16_buf.?;
            };
            const zero_u8_slice: []u8 = if (use_prealloc) self.zero_u8[0..n] else blk: {
                if (zb.u8_buf == null) {
                    const z = try chunk_alloc.alloc(u8, n);
                    @memset(z, 0);
                    zb.u8_buf = z;
                }
                break :blk zb.u8_buf.?;
            };
            const zero_u64_slice: []u64 = if (use_prealloc) self.zero_u64[0..n] else blk: {
                if (zb.u64_buf == null) {
                    const z = try chunk_alloc.alloc(u64, n);
                    @memset(z, 0);
                    zb.u64_buf = z;
                }
                break :blk zb.u64_buf.?;
            };
            const zero_str_slice: [][]const u8 = if (use_prealloc) self.zero_str[0..n] else blk: {
                if (zb.str_buf == null) {
                    const z = try chunk_alloc.alloc([]const u8, n);
                    @memset(z, "");
                    zb.str_buf = z;
                }
                break :blk zb.str_buf.?;
            };
            const zero_astr_slice: [][][]const u8 = if (use_prealloc) self.zero_astr[0..n] else blk: {
                if (zb.astr_buf == null) {
                    const z = try chunk_alloc.alloc([][]const u8, n);
                    @memset(z, &.{});
                    zb.astr_buf = z;
                }
                break :blk zb.astr_buf.?;
            };
            _ = zero_astr_slice;

            var null_mask: []u64 = zero_nm;
            const col_data: chunk.ColumnData = switch (core_ty) {
                .int64, .datetime64_ms => blk: {
                    if (is_pruned) break :blk if (core_ty == .int64)
                        .{ .int64 = zero_i64_slice }
                    else
                        .{ .datetime64_ms = zero_i64_slice };
                    const fixed = try self.fixedRange(ci, abs_start, n, chunk_alloc);
                    const bytes = fixed.bytes;
                    const base = fixed.row_base;
                    const width: usize = switch (col.ty) {
                        .int8 => 1,
                        .int16 => 2,
                        .int32, .date => 4,
                        else => 8,
                    };
                    // Zero-copy fast path for native-width int64/datetime columns:
                    // The on-disk format is 8-byte little-endian, identical to i64 in memory
                    // on little-endian targets (x86, ARM).  Return a direct mmap'd slice
                    // — avoids a 983KB allocation + copy per morsel (~5ms over full scan).
                    if (width == 8) {
                        const src: [*]i64 = @ptrCast(@alignCast(@constCast(bytes.ptr)));
                        const slice = src[base .. base + n];
                        break :blk if (core_ty == .int64) .{ .int64 = slice } else .{ .datetime64_ms = slice };
                    }
                    const buf = try chunk_alloc.alloc(i64, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    // Fast path for int16: direct pointer cast → auto-vectorized by compiler.
                    // This avoids the readInt overhead (scalar byte-by-byte) and lets
                    // ReleaseFast emit SIMD widening instructions (≈8x faster than scalar).
                    if (width == 2) {
                        const src: [*]const i16 = @ptrCast(@alignCast(bytes.ptr));
                        for (0..n) |i| {
                            buf[i] = src[base + i];
                        }
                    } else if (width == 1) {
                        // int8: direct pointer cast → auto-vectorized sign-extend i8→i64.
                        const src: [*]const i8 = @ptrCast(bytes.ptr);
                        for (0..n) |i| {
                            buf[i] = src[base + i];
                        }
                    } else if (width == 4) {
                        // int32: direct pointer cast → auto-vectorized sign-extend i32→i64.
                        const src: [*]const i32 = @ptrCast(@alignCast(bytes.ptr));
                        for (0..n) |i| {
                            buf[i] = src[base + i];
                        }
                    } else {
                        for (0..n) |i| {
                            const off = (base + i) * width;
                            buf[i] = switch (width) {
                                1 => @as(i8, @bitCast(bytes[off])),
                                4 => std.mem.readInt(i32, bytes[off..][0..4], .little),
                                else => std.mem.readInt(i64, bytes[off..][0..8], .little),
                            };
                        }
                    } // end else (non-int16)
                    break :blk if (core_ty == .int64) .{ .int64 = buf } else .{ .datetime64_ms = buf };
                },
                .float64 => blk: {
                    if (is_pruned) break :blk .{ .float64 = zero_f64_slice };
                    const fixed = try self.fixedRange(ci, abs_start, n, chunk_alloc);
                    const bytes = fixed.bytes;
                    const base = fixed.row_base;
                    const width: usize = if (col.ty == .float32) 4 else 8;
                    if (width == 8) {
                        // Zero-copy: mmap'd f64 data is already native IEEE-754 on little-endian.
                        const src: [*]f64 = @ptrCast(@alignCast(@constCast(bytes.ptr)));
                        break :blk .{ .float64 = src[base .. base + n] };
                    }
                    const fbuf = try chunk_alloc.alloc(f64, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    for (0..n) |i| {
                        const off = (base + i) * width;
                        const iv = std.mem.readInt(i32, bytes[off..][0..4], .little);
                        fbuf[i] = @floatCast(@as(f32, @bitCast(iv)));
                    }
                    break :blk .{ .float64 = fbuf };
                },
                .date_u16 => blk: {
                    if (is_pruned) break :blk .{ .date_u16 = zero_u16_slice };
                    const ubuf = try chunk_alloc.alloc(u16, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    const fixed = try self.fixedRange(ci, abs_start, n, chunk_alloc);
                    const bytes = fixed.bytes;
                    // Fast path: date stored as int32 (4 bytes LE); truncate lower 16 bits.
                    // Direct pointer cast → auto-vectorized truncation i32→u16.
                    const src: [*]const i32 = @ptrCast(@alignCast(bytes.ptr));
                    const base = fixed.row_base;
                    for (0..n) |i| {
                        ubuf[i] = @truncate(@as(u32, @bitCast(src[base + i])));
                    }
                    break :blk .{ .date_u16 = ubuf };
                },
                .string => blk: {
                    if (is_pruned) break :blk .{ .string = zero_str_slice };
                    // Fast path: column only needed for non-empty check — decode as bool_u8
                    // (1=non-empty, 0=empty). Avoids building 16-byte fat pointers per row.
                    const sbuf = try chunk_alloc.alloc([]const u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    switch (self.col_data[ci]) {
                        .string => |*sc| for (0..n) |i| {
                            sbuf[i] = sc.str(abs_start + i);
                        },
                        .block_string => |*sc| try sc.readRange(abs_start, n, chunk_alloc, sbuf),
                        else => @memset(sbuf, ""),
                    }
                    if (ci < 256 and self.nonempty_bool[ci]) {
                        const bbuf = try chunk_alloc.alloc(u8, n);
                        for (0..n) |i| bbuf[i] = if (sbuf[i].len > 0) 1 else 0;
                        break :blk .{ .bool_u8 = bbuf };
                    }
                    break :blk .{ .string = sbuf };
                },
                .bool_u8 => blk: {
                    if (is_pruned) break :blk .{ .bool_u8 = zero_u8_slice };
                    const bbuf = try chunk_alloc.alloc(u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    @memset(bbuf, 0);
                    break :blk .{ .bool_u8 = bbuf };
                },
                .uint64 => blk: {
                    if (is_pruned) break :blk .{ .uint64 = zero_u64_slice };
                    const fixed = try self.fixedRange(ci, abs_start, n, chunk_alloc);
                    const bytes = fixed.bytes;
                    // Zero-copy: mmap'd u64 data is already native little-endian on ARM/x86.
                    const src: [*]u64 = @ptrCast(@alignCast(@constCast(bytes.ptr)));
                    break :blk .{ .uint64 = src[fixed.row_base .. fixed.row_base + n] };
                },
                .array_string => blk: {
                    if (is_pruned) break :blk .{ .array_string = self.zero_astr[0..@min(n, self.zero_astr.len)] };
                    const sbuf = try chunk_alloc.alloc([][]const u8, n);
                    null_mask = try chunk_alloc.alloc(u64, null_words);
                    @memset(null_mask, 0);
                    @memset(sbuf, &.{});
                    break :blk .{ .array_string = sbuf };
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

    fn nextChunk(self: *ScanState, out: *DataChunk, ctx: *QueryContext) !bool {
        try self.loadColumns();

        const eff_hi = self.effectiveHi();
        const eff_lo = self.effectiveLo();
        if (self.rows_read >= eff_hi - eff_lo) return false;

        const remaining = (eff_hi - eff_lo) - self.rows_read;
        const n: usize = @intCast(@min(remaining, chunk.CHUNK_SIZE));

        const arena_alloc = ctx.allocator();
        if (@atomicLoad(bool, &self.loaded, .monotonic)) {
            try self.fetchRange(self.rows_read, n, out, arena_alloc);
            self.rows_read += n;
            return true;
        }
        return error.SourceNotLoaded;
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

    /// Pre-load all column mmaps before parallel execution.
    /// Must be called from the main thread before any fetchRange calls.
    pub fn preload(self: *GenericStoreBridge) !void {
        try self.state.loadColumns();
    }

    /// Prepare the bridge for the next query in a benchmark run.
    /// Updates the needed-column set and clears the `loaded` flag so that
    /// `loadColumns` will run again — but will skip columns already mmap'd
    /// from previous queries.  This keeps column pages warm across queries.
    pub fn resetForNewQuery(self: *GenericStoreBridge, pruned_cols: []const []const u8) void {
        const s = self.state;
        scan_common.resetNeededMask(s.alloc, &s.needed_cols, s.table.columns, pruned_cols);
        // Clear the `loaded` flag so loadColumns runs again for any new columns.
        @atomicStore(bool, &s.loaded, false, .release);
        // Reset per-query pipeline state.
        s.override_active = false;
        @memset(&s.nonempty_bool, false);
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

    fn rowCountFn(ptr: *anyopaque) u64 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        return self.state.effectiveCount();
    }

    fn fetchRangeFn(ptr: *anyopaque, start: u64, n: usize, out: *DataChunk, alloc: std.mem.Allocator) !void {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        // Ensure columns are loaded (idempotent — safe to call from multiple threads
        // because by the time fetchRange is called, loadColumns() has already run
        // in the first nextChunk() call or we call it here explicitly).
        try self.state.loadColumns();
        try self.state.fetchRange(start, n, out, alloc);
    }

    fn setNeededColsFn(ptr: *anyopaque, col_names: ?[]const []const u8) void {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        scan_common.setNeededOverride(&s.override_needed, &s.override_active, s.table.columns, col_names);
        if (col_names == null) return;
        // Reset loaded so the next loadColumns call picks up the newly-needed columns.
        // loadColumns skips columns already mmap'd (col_data != .none), so this is
        // incremental: only truly new columns are opened.
        @atomicStore(bool, &s.loaded, false, .release);
    }

    fn setStringNonEmptyBoolFn(ptr: *anyopaque, col_name: ?[]const u8) void {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        scan_common.setNonEmptyBool(&s.nonempty_bool, s.table.columns, col_name);
    }

    fn getRawInt16ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i16 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null;
        for (s.table.columns, 0..) |col, ci| {
            if (!std.mem.eql(u8, col.name, col_name)) continue;
            if (col.ty != .int16) return null; // only int16 (not int8)
            if (ci >= s.col_data.len) return null;
            const bytes = s.rawFixedBytes(ci) orelse return null;
            return @as([*]const i16, @ptrCast(@alignCast(bytes.ptr)))[0 .. bytes.len / 2];
        }
        return null;
    }

    fn getRawInt64ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i64 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null; // ensure columns are mmap'd
        for (s.table.columns, 0..) |col, ci| {
            if (!std.mem.eql(u8, col.name, col_name)) continue;
            if (col.ty != .int64 and col.ty != .timestamp) return null;
            if (ci >= s.col_data.len) return null;
            const bytes = s.rawFixedBytes(ci) orelse return null;
            return @as([*]const i64, @ptrCast(@alignCast(bytes.ptr)))[0 .. bytes.len / 8];
        }
        return null;
    }

    fn getRawStrOffsetsFn(ptr: *anyopaque, col_name: []const u8) ?[]const u64 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null; // ensure columns are mmap'd
        for (s.table.columns, 0..) |col, ci| {
            if (!std.mem.eql(u8, col.name, col_name)) continue;
            if (col.ty != .text and col.ty != .char and col.ty != .low_card) return null;
            if (ci >= s.col_data.len) return null;
            if (s.col_data[ci] != .string) return null;
            return s.col_data[ci].string.offsets;
        }
        return null;
    }

    fn getRawStrBytesFn(ptr: *anyopaque, col_name: []const u8) ?[]const u8 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null;
        for (s.table.columns, 0..) |col, ci| {
            if (!std.mem.eql(u8, col.name, col_name)) continue;
            if (col.ty != .text and col.ty != .char and col.ty != .low_card) return null;
            if (ci >= s.col_data.len) return null;
            if (s.col_data[ci] != .string) return null;
            return s.col_data[ci].string.bytes;
        }
        return null;
    }

    fn getRawInt32ColFn(ptr: *anyopaque, col_name: []const u8) ?[]const i32 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null;
        for (s.table.columns, 0..) |col, ci| {
            if (!std.mem.eql(u8, col.name, col_name)) continue;
            if (col.ty != .int32 and col.ty != .date) return null;
            if (ci >= s.col_data.len) return null;
            const bytes = s.rawFixedBytes(ci) orelse return null;
            return @as([*]const i32, @ptrCast(@alignCast(bytes.ptr)))[0 .. bytes.len / 4];
        }
        return null;
    }

    fn lowerBoundScalar(comptime T: type, values: []const T, target: T) usize {
        var lo: usize = 0;
        var hi: usize = values.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (values[mid] < target) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn upperBoundScalar(comptime T: type, values: []const T, target: T) usize {
        var lo: usize = 0;
        var hi: usize = values.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (values[mid] <= target) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    fn rangeFromSorted(comptime T: type, values: []const T, target: T) SourceIface.RowRange {
        return .{
            .lo = lowerBoundScalar(T, values, target),
            .hi = upperBoundScalar(T, values, target),
        };
    }

    fn findIntRangeFn(ptr: *anyopaque, col_name: []const u8, value: i64) ?SourceIface.RowRange {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        if (s.table.sort_keys.len == 0 or !std.mem.eql(u8, s.table.sort_keys[0], col_name)) return null;
        if (getRawInt16ColFn(ptr, col_name)) |col_slice| {
            if (value < std.math.minInt(i16) or value > std.math.maxInt(i16)) return .{ .lo = 0, .hi = 0 };
            return rangeFromSorted(i16, col_slice, @intCast(value));
        }
        if (getRawInt32ColFn(ptr, col_name)) |col_slice| {
            if (value < std.math.minInt(i32) or value > std.math.maxInt(i32)) return .{ .lo = 0, .hi = 0 };
            return rangeFromSorted(i32, col_slice, @intCast(value));
        }
        if (getRawInt64ColFn(ptr, col_name)) |col_slice| {
            return rangeFromSorted(i64, col_slice, value);
        }
        return null;
    }

    fn setRowRangeFn(ptr: *anyopaque, lo: u64, hi: u64) void {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        self.state.scan_lo = lo;
        self.state.scan_hi = hi;
    }

    fn getSortKeysFn(ptr: *anyopaque) []const []const u8 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        return self.state.table.sort_keys;
    }

    fn getIntMinMaxFn(ptr: *anyopaque, col_name: []const u8) ?[2]i64 {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        const s = self.state;
        s.loadColumns() catch return null;
        const ci = blk: {
            for (s.table.columns, 0..) |col, i| {
                if (std.mem.eql(u8, col.name, col_name)) break :blk i;
            }
            return null;
        };
        switch (s.table.columns[ci].ty) {
            .int8, .int16, .int32, .int64, .date, .timestamp => {},
            else => return null,
        }
        return switch (s.col_data[ci]) {
            .fixed => |m| blk: {
                if (m.mapped.row_count == 0) return null;
                var lo: i64 = std.math.maxInt(i64);
                var hi: i64 = std.math.minInt(i64);
                for (m.mapped.entries) |entry| {
                    if (entry.stats_kind != .signed) return null;
                    lo = @min(lo, @as(i64, @bitCast(entry.min_bits)));
                    hi = @max(hi, @as(i64, @bitCast(entry.max_bits)));
                }
                break :blk .{ lo, hi };
            },
            else => null,
        };
    }

    fn profileStatsFn(ptr: *anyopaque) SourceIface.ProfileStats {
        const self: *GenericStoreBridge = @ptrCast(@alignCast(ptr));
        return .{
            .open_ns = self.state.profile_open_ns,
            .mmap_ns = self.state.profile_mmap_ns,
            .bytes_read = self.state.profile_bytes_read.load(.monotonic),
            .bytes_mapped = self.state.profile_bytes_mapped,
        };
    }

    const vtable = SourceIface.VTable{
        .nextChunk = nextChunkFn,
        .reset = resetFn,
        .schema = schemaFn,
        .rowCount = rowCountFn,
        .fetchRange = fetchRangeFn,
        .setNeededCols = setNeededColsFn,
        .setStringNonEmptyBool = setStringNonEmptyBoolFn,
        .getRawInt16Col = getRawInt16ColFn,
        .getRawInt64Col = getRawInt64ColFn,
        .getRawStrOffsets = getRawStrOffsetsFn,
        .getRawStrBytes = getRawStrBytesFn,
        .getRawInt32Col = getRawInt32ColFn,
        .getIntMinMax = getIntMinMaxFn,
        .setRowRange = setRowRangeFn,
        .getSortKeys = getSortKeysFn,
        .findIntRange = findIntRangeFn,
        .profileStats = profileStatsFn,
    };
};

fn writeGenericFixedForTest(
    comptime T: type,
    io: std.Io,
    alloc: std.mem.Allocator,
    part_dir: []const u8,
    name: []const u8,
    values: []const T,
) !void {
    const path = try generic_store.columnBinPath(alloc, part_dir, name);
    defer alloc.free(path);
    const ty: schema.ColumnType = switch (T) {
        i16 => .int16,
        i32 => .int32,
        i64 => .int64,
        else => @compileError("unsupported test column type"),
    };
    var writer = try generic_store.v2.Writer.open(alloc, io, path, ty, values.len, 2);
    defer writer.deinit();
    for (values) |v| switch (T) {
        i16 => try writer.writeI16(v),
        i32 => try writer.writeI32(v),
        i64 => try writer.writeI64(v),
        else => @compileError("unsupported test column type"),
    };
    try writer.finish();
}

test "GenericStoreBridge findIntRange supports sorted int widths" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/zig_test_generic_find_range";

    {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, root) catch {};
    }
    defer {
        var cwd = std.Io.Dir.cwd();
        cwd.deleteTree(io, root) catch {};
    }

    const Case = struct {
        fn run(
            comptime T: type,
            table_name: []const u8,
            col_ty: schema.ColumnType,
            values: []const T,
            target: i64,
            expected_lo: u64,
            expected_hi: u64,
        ) !void {
            const cols = [_]schema.Column{.{ .name = "id", .ty = col_ty }};
            const sort_keys = [_][]const u8{"id"};
            const table = schema.Table{ .name = table_name, .columns = &cols, .sort_keys = &sort_keys };
            const part_dir = try generic_store.initPart(io, root, table_name, allocator);
            defer allocator.free(part_dir);
            try generic_store.writeColumnsTxt(io, allocator, part_dir, table);
            try generic_store.writeCountTxt(io, allocator, part_dir, values.len);
            try writeGenericFixedForTest(T, io, allocator, part_dir, "id", values);

            var bridge = try GenericStoreBridge.init(allocator, io, part_dir, table, &.{});
            defer bridge.deinit();
            const range = bridge.source().findIntRange("id", target) orelse return error.TestExpectedRange;
            try std.testing.expectEqual(expected_lo, range.lo);
            try std.testing.expectEqual(expected_hi, range.hi);
        }
    }.run;

    const vals16 = [_]i16{ 1, 2, 2, 9 };
    try Case(i16, "t16", .int16, &vals16, 2, 1, 3);

    const vals32 = [_]i32{ 3, 5, 5, 5, 8 };
    try Case(i32, "t32", .int32, &vals32, 5, 1, 4);

    const vals64 = [_]i64{ 7, 7, 11, 99 };
    try Case(i64, "t64", .int64, &vals64, 7, 0, 2);
}

test "GenericStoreBridge reads Store V2 fixed columns across blocks" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const root = "/tmp/zig_test_generic_store_v2_bridge";
    std.Io.Dir.cwd().deleteTree(io, root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io, root) catch {};

    const cols = [_]schema.Column{
        .{ .name = "id", .ty = .int32 },
        .{ .name = "score", .ty = .float64 },
    };
    const table = schema.Table{ .name = "events", .columns = &cols };
    const part_dir = try generic_store.initPart(io, root, table.name, allocator);
    defer allocator.free(part_dir);
    try generic_store.writeColumnsTxt(io, allocator, part_dir, table);
    try generic_store.writeCountTxt(io, allocator, part_dir, 10);

    const id_path = try generic_store.columnBinPath(allocator, part_dir, "id");
    defer allocator.free(id_path);
    var id_writer = try generic_store.v2.Writer.open(allocator, io, id_path, .int32, 10, 4);
    defer id_writer.deinit();
    for (0..10) |i| try id_writer.writeI32(@intCast(i * 2));
    try id_writer.finish();

    const score_path = try generic_store.columnBinPath(allocator, part_dir, "score");
    defer allocator.free(score_path);
    var score_writer = try generic_store.v2.Writer.open(allocator, io, score_path, .float64, 10, 4);
    defer score_writer.deinit();
    for (0..10) |i| try score_writer.writeF64(@as(f64, @floatFromInt(i)) + 0.5);
    try score_writer.finish();

    var bridge = try GenericStoreBridge.init(allocator, io, part_dir, table, &.{});
    defer bridge.deinit();
    var fetched: DataChunk = undefined;
    try bridge.source().fetchRange(3, 5, &fetched, allocator);
    defer fetched.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 6, 8, 10, 12, 14 }, fetched.columns[0].data.int64);
    try std.testing.expectEqualSlices(f64, &.{ 3.5, 4.5, 5.5, 6.5, 7.5 }, fetched.columns[1].data.float64);

    var qctx = QueryContext.init(allocator, bridge.source());
    defer qctx.deinit();
    var sequential: DataChunk = undefined;
    try std.testing.expect(try bridge.source().nextChunk(&sequential, &qctx));
    defer sequential.deinit();
    try std.testing.expectEqual(@as(i64, 0), sequential.columns[0].data.int64[0]);
}
